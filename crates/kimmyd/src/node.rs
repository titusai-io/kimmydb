//! Node lifecycle: startup, serving, and graceful shutdown.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use kimmy_auth::{JwkSet, OidcVerifier, TokenIssuer, UserStore};
use kimmy_storage::{Engine, RetentionPolicy};
use tracing::{debug, info, warn};

use crate::config::{AuthConfig, Config, OidcConfig};
use crate::lifecycle;

/// Filename of the redb database inside the data directory.
const DATABASE_FILE: &str = "kimmy.redb";

/// Signing key used when authentication is disabled.
///
/// Named rather than inline so it is greppable: with auth off no token is ever
/// verified, and this value must never sign one that is. See `run`.
const THROWAWAY_SIGNING_KEY: &str = "insecure-no-auth-unused-signing-key";

/// The key tokens are signed with, or a refusal.
///
/// Split out of `run` so the decision can be tested without starting a server:
/// the case that matters is the one where the node must *not* start, and a test
/// that reached it through `run` would otherwise have to be a test that waits
/// for a server to fail to be born.
///
/// `Config::validate` already refuses auth-on-with-no-secret, so the error here
/// is unreachable through the CLI. It is a refusal rather than a `debug_assert`
/// because assertions are compiled out of the release build that ships, and
/// this is the line where a constant from the source would become a signing
/// key — anyone could then forge a token.
fn signing_key(auth: &AuthConfig) -> Result<String> {
    match &auth.jwt_secret {
        Some(secret) => Ok(secret.clone()),
        // With auth off no token is ever verified, so a throwaway key is
        // correct rather than a placeholder that might be mistaken for a secret.
        None if auth.insecure_no_auth => Ok(THROWAWAY_SIGNING_KEY.to_string()),
        None => anyhow::bail!(
            "authentication is enabled but no auth.jwt_secret is configured; refusing to sign \
             tokens with a constant compiled into the binary. Set KIMMY_JWT_SECRET."
        ),
    }
}

/// Say, once at startup and once when it is due, that a previous signing
/// secret is configured and when it can go.
///
/// The window (ADR-101) is meant to close: a previous secret verifies tokens
/// for as long as it is configured, and the moment it stops being useful is one
/// token lifetime after the rotation — every token it signed has expired by
/// then. This node cannot know when the rotation happened, only when *it*
/// started with the previous secret in place, so it counts from its own start.
/// That is exact for the common case (the rotation is the restart that brought
/// the new pair in) and conservative otherwise: a node restarted later in the
/// window warns later, never earlier. Nothing is persisted across restarts; the
/// reminder exists to be noticed, not to be relied on.
///
/// Two lines, deliberately: an `info` naming the deadline, so the operator who
/// just performed the rotation has a time to write down, and one `warn` when it
/// passes. Not repeated — a log that nags on a timer is a log that gets
/// filtered, and the summary line already says `jwt_previous_secret=set` on
/// every start.
fn remind_to_remove_previous_secret(ttl_secs: u64) {
    info!(
        remove_after_secs = ttl_secs,
        "a previous JWT signing secret is configured; every token it signed will have expired \
         one token lifetime from now, so remove KIMMY_JWT_PREVIOUS_SECRET after that"
    );
    // UNSUPERVISED: a reminder that warns once and stops. Its ending is the point, and a
    // panic in it must not stop a node that is otherwise serving.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(ttl_secs)).await;
        warn!(
            configured_for_secs = ttl_secs,
            "the previous JWT signing secret has outlived every token it signed; it still \
             verifies tokens while it is set, so remove KIMMY_JWT_PREVIOUS_SECRET and restart"
        );
    });
}

/// How often the certificate files are checked for a change.
///
/// A constant rather than configuration: a renewal lands weeks before expiry,
/// so a minute is far inside any window that matters, and SIGHUP already covers
/// "now". See ADR-049.
const CERT_POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Start the node, serve until told to stop, and say how it ended.
///
/// Both ways out are logged and both leave the exit marker `lifecycle`
/// reads on the next start (ADR-147): the marker's absence is what tells
/// that start the process was ended by something else. The error path logs
/// at `INFO` with the error's text — `main` prints it to stderr as well, so
/// the level is for naming the exit in the log, not for paging; the exit
/// status already says it failed.
pub async fn run(config: Config) -> Result<()> {
    let data_dir = config.storage.data_dir.clone();
    let outcome = start_and_serve(config).await;
    match &outcome {
        Ok(()) => {
            lifecycle::record_exit(&data_dir, lifecycle::Exit::Shutdown);
            info!("shutdown complete");
        }
        Err(e) => {
            lifecycle::record_exit(&data_dir, lifecycle::Exit::Error);
            info!(error = format!("{e:#}"), "exiting on an error");
        }
    }
    outcome
}

/// How long the embedding worker waits before retrying a storage error, and the
/// ceiling that wait doubles up to (ADR-184).
///
/// Short enough that a transient failure costs a few seconds of embedding, long
/// enough that a permanent one does not spin. The ceiling matters more than the
/// floor: a worker retrying for ever at a two-minute cadence leaves
/// `kimmy_task_retries_total` rising slowly, which is what the age series in the
/// follow-up reads as "no progress".
const EMBEDDING_RETRY_FIRST: Duration = Duration::from_secs(1);
const EMBEDDING_RETRY_MAX: Duration = Duration::from_secs(120);

async fn start_and_serve(config: Config) -> Result<()> {
    std::fs::create_dir_all(&config.storage.data_dir).with_context(|| {
        format!("creating data directory {}", config.storage.data_dir.display())
    })?;

    // Node identity lives inside the database file rather than beside it, so
    // that copying or restoring the file carries the identity with it. That
    // matters because the id is the tiebreak half of every write's stamp — a
    // node that forgets it becomes a stranger to its own prior writes.
    let path = config.storage.data_dir.join(DATABASE_FILE);
    // Read before the engine opens, because opening creates the database
    // file, and a database with no marker beside it is what an unclean exit
    // looks like (ADR-147). Announced after the banner below.
    let previous = lifecycle::previous_run(&config.storage.data_dir, &path);
    let engine = Arc::new(
        Engine::open_with_cache(&path, Some(config.storage.cache_bytes as usize))
            .with_context(|| format!("opening database {}", path.display()))?,
    );
    engine.set_multi_chunk_docs(config.storage.multi_chunk_docs);
    // Validation already refused anything else; the fallback is only so a
    // future class name cannot silently mean "durable".
    let class = kimmy_storage::DurabilityClass::parse(&config.storage.durability)
        .unwrap_or(kimmy_storage::DurabilityClass::Durable);
    engine.set_durability(class, Duration::from_millis(config.storage.commit_coalesce_ms));

    // Version and commit together, because during a rolling upgrade or an
    // incident the question is "which build is this exactly", and a version
    // number alone does not answer it between releases.
    info!(
        node = %engine.node_id(),
        version = kimmy_core::build::VERSION,
        commit = kimmy_core::build::COMMIT,
        "starting kimmyd"
    );
    info!("{}", config.summary());

    // After the banner, so the line an operator is sent to look for sits
    // under the identity of the run that is reporting it.
    lifecycle::announce(&config.storage.data_dir, &previous);

    // Before the first task is spawned: a supervised death needs somewhere to
    // record itself, and a panic anywhere needs to reach the structured log
    // rather than bare stderr (ADR-184).
    if !crate::supervision::install(config.storage.data_dir.clone()) {
        anyhow::bail!("the supervision hooks were installed twice; this is a programming error");
    }
    // Announced at the signal, before anything drains, so that a supervised
    // task ending during the drain is a stop rather than a death.
    let shutdown = kimmy_task::Shutdown::new();

    // A test switch that stops a background task on purpose. It is in the
    // shipped binary so that the tests drive the binary that ships, so every
    // start where it is set says so — it must not be able to sit on unnoticed in
    // a deployment.
    if let Some(what) = kimmy_task::test_kill_requested() {
        warn!(
            KIMMY_TEST_KILL_TASK = %what,
            "a test switch is set that will stop a background task on purpose, and this node \
             will then exit; unset KIMMY_TEST_KILL_TASK outside a test"
        );
    }

    if config.auth.insecure_no_auth {
        warn!("authentication is DISABLED; every request runs with full privileges");
    } else {
        bootstrap_users(&engine, &config)?;
    }

    let secret = signing_key(&config.auth)?;
    // The previous secret only means anything while tokens are verified; with
    // auth off nothing is, and validation did not look at it either.
    let previous = if config.auth.insecure_no_auth {
        None
    } else {
        config.auth.jwt_previous_secret.as_deref()
    };
    let tokens = TokenIssuer::with_previous(&secret, previous, config.auth.token_ttl_secs)
        .context("configuring the token issuer")?;
    if tokens.has_previous_secret() {
        remind_to_remove_previous_secret(config.auth.token_ttl_secs);
    }

    // With auth off there is no login to brute-force and every request is a
    // superuser anyway, so a limiter would only be an obstacle to the local
    // development the flag exists for.
    let limits = if config.auth.insecure_no_auth {
        kimmy_api::RateLimits::disabled()
    } else {
        config.server.rate_limit.build()
    };

    // Set before anything can be authorized, so no decision escapes the mode
    // an operator configured.
    let audit = kimmy_api::AuditMode::parse(&config.audit.mode)
        .map_err(|e| anyhow::anyhow!("audit.{e}"))?;
    kimmy_api::audit::set_mode(audit);
    info!(mode = audit.name(), "audit logging");

    // Beside the audit mode, and process-global for the same reason: it is a
    // property of the deployment rather than of a request, and threading it
    // through every extractor would put a configuration parameter in the
    // signature of code that has no other reason to know about configuration.
    // Set before anything can be served, so no span escapes carrying a name
    // the operator did not ask to publish (ADR-068).
    kimmy_api::telemetry::set_include_names(config.telemetry.include_names);

    let egress = kimmy_api::egress::EgressPolicy::new(
        kimmy_api::egress::WEBHOOKS,
        config.webhooks.allowed_hosts.clone(),
    );
    // What an embedding provider may be handed and where it may be sent
    // (ADR-115). One policy, built once, held by the API for configure time
    // and searches and by the worker for documents, so the two cannot
    // disagree about a configuration. Validation already refused a policy
    // that could not be built; this cannot fail on a configuration that
    // passed it.
    let providers = config.vector.provider_policy().context("building the provider policy")?;
    if providers.locked() || !providers.profiles().is_empty() {
        info!(
            endpoints_locked = providers.locked(),
            profiles = providers.profiles().len(),
            "embedding provider policy"
        );
    }
    let state = kimmy_api::state_with_policies(
        Arc::clone(&engine),
        tokens,
        config.auth.insecure_no_auth,
        limits,
        egress,
        providers.clone(),
    )
    .context("building the API state")?;

    // Where a local token may be minted from (ADR-100). Validation already
    // refused an unknown name and `disabled` without a provider; this is the
    // line that makes the mode true, said out loud whenever it is not the
    // default, because the failure it produces — a 403 or 404 from login — is
    // one an operator will otherwise go looking for in the wrong place.
    let local_login = config.auth.local.login_mode()?;
    state.set_local_login(local_login);
    match local_login {
        kimmy_api::LocalLogin::Always => {}
        kimmy_api::LocalLogin::LoopbackOnly => info!(
            "local login answers loopback connections only; a token already issued keeps \
             working, and a reverse proxy on this host will look like loopback"
        ),
        kimmy_api::LocalLogin::Disabled => warn!(
            "local login is DISABLED; only the identity provider can authenticate a caller, \
             and a token already issued keeps working until it expires"
        ),
    }

    // How much memory resident HNSW graphs may take between them. Set on the
    // built state rather than passed into the constructor every test shares:
    // like the audit mode above it is a property of the deployment, and the
    // cache's default is the same value the config's default carries.
    state.vectors.set_max_bytes(config.vector.index_cache.max_bytes);

    // The OTLP counters, reading the same atomics `/metrics` renders. Here
    // rather than in `logging::init` because the counters live in this state
    // and this state needs a database, which does not exist when the
    // subscriber is installed. With no collector configured the global meter
    // is a no-op and this registers nothing (ADR-070).
    crate::logging::TelemetryGuard::bridge_metrics(&state);

    // MCP shares the state rather than being handed its own, so an agent tool
    // and the REST route beside it reach the same engine through the same
    // authorization check. See kimmy-mcp's crate documentation.
    //
    // Merged through `router_with` rather than onto the finished router: a
    // router merged afterwards sits outside the layer that counts, times,
    // traces and challenges, which is how `/mcp` came to answer 401 with no
    // `WWW-Authenticate` and to be missing from `/metrics` entirely.
    let mcp = config.server.mcp.then(|| {
        kimmy_mcp::mcp_router(Arc::clone(&state), config.server.mcp_allowed_hosts.clone())
    });
    let serving_mcp = mcp.is_some();
    // The request deadline and body ceiling ride in with the router rather
    // than the state: they are parameters of the middleware stack, fixed when
    // the table is built (ADR-099).
    let app =
        kimmy_api::router_with_limits(Arc::clone(&state), mcp, config.server.request_limits());
    if serving_mcp {
        info!("serving MCP at /mcp");
    }

    // Published so `/v1/topology` can tell a client where this node is. The
    // record replicates like any other document, which is what lets one node
    // answer for the whole cluster; liveness comes from SWIM separately.
    if let Some(endpoint) = kimmy_api::topology::advertised_endpoint(
        config.server.advertise.as_deref(),
        config.server.bind,
        config.server.tls.is_enabled(),
    ) && let Err(e) = kimmy_api::topology::register(&state, &endpoint)
    {
        // Not fatal: a node that cannot publish its address still serves. The
        // cost is that clients are not told about it, which is worth a loud
        // line rather than a refusal to start.
        warn!(error = %e.message, "could not register this node in the client topology");
    }

    // Federation, if an external provider is configured. The verifier is
    // installed *before* anything is served, with no keys in it yet; the keys
    // arrive from the provider behind the node.
    //
    // Deliberately not awaited. A briefly unreachable identity provider must
    // not stop a database from restarting — during an incident that is exactly
    // when both are being restarted — so the fetch retries in the background
    // and local users keep working the whole time. `kimmyd check-config` is
    // where an operator gets the live answer.
    let jwks_handle = match config.auth.oidc.settings() {
        None => None,
        Some(settings) => {
            let verifier = OidcVerifier::new(settings).context("configuring the OIDC verifier")?;
            info!(
                issuer = verifier.issuer(),
                audience = verifier.settings().audience,
                roles_claim = verifier.settings().roles_claim,
                mappings = verifier.settings().role_mappings.len(),
                "federating with an external identity provider"
            );
            // Whether this node can name itself to an authorization server, said
            // once at startup rather than left to be inferred from a 404. An
            // operator who set an opaque audience and expected RFC 9728 to
            // appear has no other way to find out why it did not, and the two
            // configurations are otherwise indistinguishable from the outside.
            match verifier.settings().resource_identifier() {
                Some(resource) => info!(
                    resource,
                    path = kimmy_auth::PROTECTED_RESOURCE_METADATA_PATH,
                    "publishing protected resource metadata"
                ),
                None => info!(
                    audience = verifier.settings().audience,
                    "the audience is not an https URI, so no protected resource metadata is \
                     published and clients cannot ask this node's provider for a token scoped \
                     to it; set auth.oidc.audience to the public URL clients reach this node at \
                     to enable it"
                ),
            }
            let federation = kimmy_api::Federation::new(verifier);
            state.set_federation(Arc::clone(&federation));
            // Built here, not inside the task, and for the same reason as the
            // webhook client and the cluster's TLS: it is a builder failure, not
            // a network one, so it cannot be retried and will not come right.
            // Inside the task it used to warn once and return, leaving every
            // federated token refused for the life of the process while the node
            // went on serving -- and under supervision that return is a death,
            // so the same misconfiguration became a restart loop instead. Built
            // before anything is spawned, it fails the start, which is what a
            // node that cannot do a duty it was configured for should do.
            //
            // Only when OIDC is configured: this arm is that condition.
            let http = jwks_client().context("building the HTTP client for OIDC key refresh")?;
            Some(spawn_jwks_refresher(
                federation,
                Duration::from_secs(config.auth.oidc.refresh_interval_secs),
                Arc::clone(&state),
                shutdown.clone(),
                http,
            ))
        }
    };

    let gc_handle = spawn_collector(Arc::clone(&engine), &config, shutdown.clone());
    let cluster =
        spawn_cluster(Arc::clone(&engine), Arc::clone(&state), &config, shutdown.clone()).await?;

    // The routes see the live member set only once the cluster is up, which is
    // after the router was built — hence a late hand-off rather than a
    // constructor argument.
    if let Some(members) = cluster.members.clone() {
        state.set_members(members);
    }

    // The webhook dispatcher is an ordinary oplog consumer, like the embedding
    // worker below. It derives which subscriptions it owns from the same live
    // member set SWIM maintains, so a node dying hands its subscriptions to a
    // survivor without anything being elected. See ADR-045.
    let webhook_handle = {
        let state = Arc::clone(&state);
        let egress = kimmy_api::egress::EgressPolicy::new(
            kimmy_api::egress::WEBHOOKS,
            config.webhooks.allowed_hosts.clone(),
        );
        let members = cluster.members.clone();
        let limits = kimmy_api::dispatch::Limits {
            max_concurrent_deliveries: config.webhooks.max_concurrent_deliveries,
            max_payload_bytes: config.webhooks.max_payload_bytes,
            // A cadence rather than a policy, so it stays out of the config
            // file — see `Limits::DEFAULT_PROGRESS_HEARTBEAT`.
            ..Default::default()
        };
        // This node's own id, which exists whether or not clustering does —
        // so ownership needs no placeholder address for the single-node case.
        // With no member set, the union is just `me` and one node owns
        // everything (ADR-051).
        let me = engine.node_id();
        // Built here, not inside the task: a client that will not build is a
        // startup failure, because there is no configuration that turns webhook
        // delivery off and a subscription can be created at runtime (ADR-184).
        let client =
            kimmy_api::dispatch::client(&egress).context("building the webhook delivery client")?;
        kimmy_task::supervise("webhook_dispatcher", shutdown.clone(), async move {
            kimmy_api::dispatch::run(state, egress, me, members, limits, client).await;
        })
    };

    // The runtime-stall probe behind `kimmy_runtime_stall_seconds`: a timer
    // that should fire every 250 ms and records how late it was. It runs on
    // the same workers as everything else, so a worker blocked on the storage
    // lock or an fsync shows up here as a number before it shows up as a
    // peer's 5 s handshake timeout or a member marked down.
    let stall_probe = {
        let state = Arc::clone(&state);
        kimmy_task::supervise("stall_probe", shutdown.clone(), async move {
            let period = std::time::Duration::from_millis(250);
            loop {
                let t = std::time::Instant::now();
                tokio::time::sleep(period).await;
                state.metrics.record_runtime_stall(t.elapsed().saturating_sub(period));
            }
        })
    };

    // TTL expiry. Ownership is rendezvous-hashed per collection through the
    // same live member set the dispatcher uses, so one node expires a given
    // collection and one expired document produces one delete cluster-wide
    // rather than one per node.
    let expiry_handle = {
        let state = Arc::clone(&state);
        let members = cluster.members.clone();
        let me = engine.node_id();
        let interval = match config.storage.ttl_interval_secs {
            0 => None,
            secs => Some(Duration::from_secs(secs)),
        };
        match interval {
            None => {
                warn!(
                    "TTL expiry is disabled; documents with an expiry policy will not be removed"
                );
                None
            }
            Some(interval) => {
                Some(kimmy_task::supervise("ttl_expiry", shutdown.clone(), async move {
                    kimmy_api::expiry::run(state, me, members, interval).await;
                }))
            }
        }
    };

    // Keeps each node's view of "is this token still good" honest. Another
    // ordinary oplog consumer, which is also what makes revoking on one node
    // take effect on every node: a replicated write to `__users` publishes on
    // the node that applied it (ADR-052).
    let sessions_handle = kimmy_task::supervise(
        "session_invalidator",
        shutdown.clone(),
        kimmy_api::sessions::invalidator(&engine, state.sessions.clone()),
    );

    // The same shape, one cache over. Dropping a collection forgets its vector
    // index on the member that took the request; a drop that arrives by
    // replication is applied by the sync path, which runs no route, so without
    // this consumer that member keeps the graph resident and its snapshot on
    // disk for a collection that no longer exists.
    let vector_index_handle = kimmy_task::supervise(
        "vector_index_invalidator",
        shutdown.clone(),
        kimmy_api::vectors::invalidator(&state),
    );

    // Snapshots left by a drop this node was not running for are reached by
    // neither the routes nor the consumer: nothing opens a snapshot directory
    // until something asks for that collection, and nothing asks for one that
    // no longer exists. Swept once, here, before any graph can be building.
    // That ordering is what makes removing the `<id>.build` staging
    // directories safe: the listener has not bound and the consumer above only
    // forgets, so no build is writing one, and anything staged was left by a
    // build the previous process did not finish. The listener is the only
    // thing this orders against: the embedding worker below never reads or
    // writes a snapshot, and the sweep is disk hygiene, not a correctness
    // step — the load boundary's `created` check is what refuses a previous
    // incarnation's graph, swept or not. Kept on the critical path on purpose:
    // a readdir and a `meta.json` parse per live vector collection, against a
    // restart that already takes seconds, buys the one arrangement with no
    // race.
    match kimmy_storage::blocking(|| {
        engine
            .live_collections()
            .map(|live| state.vectors.sweep_snapshots(&live, kimmy_vector::Staging::Remove))
    }) {
        Ok(removed) if removed > 0 => {
            info!(
                removed,
                "removed HNSW snapshots this node holds no collection for, or that a previous \
                 collection of the same name left behind, and staging directories of interrupted \
                 builds"
            );
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "could not sweep stale HNSW snapshots"),
    }

    // The embedding worker is an ordinary change-stream subscriber, so it runs
    // alongside the server rather than inside the write path. A write returns
    // as soon as its oplog entry is durable; embedding catches up behind it.
    //
    // Disabled by `[vector] worker_enabled = false` or
    // --disable-vector-worker: this node then consumes embeddings by
    // replication instead of producing provider calls, which is how a
    // deployment pins embedding work to designated members.
    let worker_handle = if config.vector.worker_enabled {
        // Ownership over the same rendezvous function as webhooks and expiry:
        // per collection, derived from the live member set, no agreement
        // needed. Backfill scans and deferred re-checks run only on the owner,
        // which is what keeps a replicated `ConfigureVectors` entry from being
        // a full-corpus provider bill on every member at once.
        let worker_me = engine.node_id();
        let worker_members = cluster.members.clone();
        let worker_counters = Arc::new(kimmy_vector::WorkerCounters::default());
        state.metrics.set_vector_counters(Arc::clone(&worker_counters));
        let batching = config.vector.batch.settings();
        Some(kimmy_task::supervise("embedding_worker", shutdown.clone(), {
            let engine = Arc::clone(&engine);
            let retrying = shutdown.clone();
            async move {
                let mut worker = kimmy_vector::EmbeddingWorker::new(engine);
                worker.set_batching(batching);
                worker.set_policy(providers);
                worker.set_owner_check(Box::new(move |key| match &worker_members {
                    // No clustering: the candidate set is just this node,
                    // which owns everything.
                    Some(members) => {
                        kimmy_api::ownership::owns(key, worker_me, &members.node_ids())
                    }
                    None => true,
                }));
                worker.set_counters(worker_counters);
                // Every error `run` returns is a storage error, which is
                // transient: it used to return here, and node.rs logged
                // "embedding worker stopped" while embedding stayed stopped
                // until the next restart (ADR-184). Retried in place instead,
                // and `kimmy_task_retries_total{task="embedding_worker"}`
                // counts the attempts so a permanent failure is visible as a
                // rising count rather than as silence.
                let mut retry = kimmy_task::Retry::new(
                    "embedding_worker",
                    EMBEDDING_RETRY_FIRST,
                    EMBEDDING_RETRY_MAX,
                );
                // The loop lives in `Retry::forever` rather than here. Written
                // out at this call site first, and the whole workspace suite
                // passed with its `Err` arm returning instead of retrying —
                // so the one rule this is about had no test anywhere. Returning
                // from `forever` is a death, which is what should happen if
                // this worker ever finishes.
                retry.forever(&retrying, &mut worker, |w| Box::pin(w.run())).await;
            }
        }))
    } else {
        warn!(
            "embedding worker is disabled; collections with server-side providers will not embed \
             on this node — vectors arrive by replication from workers elsewhere"
        );
        None
    };

    // Loaded before binding, so a bad certificate is a startup failure rather
    // than a handshake error for whoever connects first.
    let tls = load_tls(&config).await?;

    // Only with TLS configured: with it off there is no file to watch, so
    // there is no task to run (ADR-049).
    let cert_reloader = match (&tls, config.server.tls.pair()) {
        (Some(tls), Some((cert, key))) => Some(spawn_cert_reloader(
            tls.clone(),
            cert.to_path_buf(),
            key.to_path_buf(),
            Arc::clone(&state),
            shutdown.clone(),
        )),
        _ => None,
    };

    let listener = tokio::net::TcpListener::bind(config.server.bind)
        .await
        .with_context(|| format!("binding {}", config.server.bind))?;
    let local = listener.local_addr().unwrap_or(config.server.bind);

    if tls.is_some() {
        info!(
            bind = %local,
            poll_secs = CERT_POLL_INTERVAL.as_secs(),
            "serving HTTPS, WebSocket over TLS, and MCP; SIGHUP or a changed file reloads the certificate"
        );
    } else {
        info!(bind = %local, "serving HTTP and WebSocket");
        if !is_loopback(&local) && !config.auth.insecure_no_auth {
            // Not fatal — terminating TLS at a proxy or a service mesh is a
            // legitimate deployment, and refusing to start would break every
            // one of them. But it is worth saying out loud, because the failure
            // it warns about is silent: nothing about a working request reveals
            // that the token authorising it crossed the wire in the clear.
            warn!(
                bind = %local,
                "serving plaintext HTTP on a non-loopback address; tokens and passwords cross \
                 the wire unencrypted. Set server.tls.cert_file and server.tls.key_file, or \
                 terminate TLS at a proxy in front of this node"
            );
        }
    }

    // From here the node is serving, which is the earliest the test switch may
    // act: armed later than startup so it can never turn a start into a crash
    // loop, and can never be mistaken for a startup failure.
    kimmy_task::arm_test_kills();
    let served = serve(listener, app, tls, shutdown.clone()).await;
    // Before the aborts below, and before returning an error: from here on a
    // supervised task ending is a stop, not a death. `serve` has already
    // announced it on the signal path; this covers the path where serving
    // itself failed, where no signal ever arrived.
    shutdown.begin();
    served.context("serving")?;

    // Nothing to drain: it holds no state beyond the mtimes it last saw, and
    // the certificate in use is already in the acceptor.
    if let Some(handle) = cert_reloader {
        handle.abort();
    }
    // Likewise the key refresher: an aborted fetch installs nothing, and the
    // key set already in the verifier is the one that was serving.
    if let Some(handle) = jwks_handle {
        handle.abort();
    }
    // Holds only a cache, which the next start rebuilds by reading.
    sessions_handle.abort();
    // Likewise, and it is holding nothing when it is between entries.
    vector_index_handle.abort();
    stall_probe.abort();
    // The worker holds no locks and its position is durable, so aborting is
    // safe: whatever it had not finished is re-delivered on the next start.
    // `None` when the worker is disabled — nothing to abort.
    if let Some(handle) = worker_handle {
        handle.abort();
    }
    // Likewise the collector: a pass is a transaction, so an aborted one either
    // committed or did not, and the next start simply finds the same garbage.
    if let Some(handle) = gc_handle {
        handle.abort();
    }
    // And expiry: each delete is its own commit, so an aborted pass leaves a
    // prefix of the batch removed and the rest still due. The next pass finds
    // them, which is the same property that lets a bounded pass drain a
    // backlog over several ticks.
    if let Some(handle) = expiry_handle {
        handle.abort();
    }
    // And replication: anti-entropy is idempotent and resumes from version
    // vectors, so an interrupted round costs nothing but a repeat.
    for handle in cluster.tasks {
        handle.abort();
    }
    // The dispatcher records its progress only after an endpoint accepts, so
    // an aborted delivery is redelivered rather than lost.
    webhook_handle.abort();

    // `run` writes the exit marker and says "shutdown complete", after this
    // returns, so the last line of the log is the last thing done.
    Ok(())
}

/// How long in-flight requests get to finish once shutdown begins.
///
/// Only reached when a request is still running; an idle server stops
/// immediately, which is what keeps `docker stop` returning in milliseconds.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Read the certificate and key, if TLS is configured.
///
/// Returns `None` when neither is set. The configuration layer has already
/// refused the half-configured case, so "one of the two" cannot reach here.
async fn load_tls(config: &Config) -> Result<Option<RustlsConfig>> {
    let Some((cert, key)) = config.server.tls.pair() else {
        return Ok(None);
    };

    // rustls needs a process-wide crypto provider. Installed explicitly rather
    // than left to the crate-features fallback so the choice is visible in the
    // code: `ring` is already in the build via `reqwest`, whereas the
    // `aws-lc-rs` default would add CMake and a full C build for a second
    // implementation of the same primitives. See ADR-039.
    //
    // An error means one is already installed, which is the desired end state,
    // so it is not a failure.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let tls = RustlsConfig::from_pem_file(cert, key).await.with_context(|| {
        format!("loading the TLS certificate {} and key {}", cert.display(), key.display())
    })?;
    info!(cert = %cert.display(), "TLS enabled");
    Ok(Some(tls))
}

/// Watch the certificate files and swap them in without a restart.
///
/// Two triggers, one reload (ADR-049). SIGHUP is what an operator on systemd or
/// bare metal reaches for; the poll is what works where a certificate is
/// rotated *by* something rather than by someone, such as cert-manager
/// rewriting a mounted Secret, since there is no convenient way to signal PID 1
/// of a pod. Neither covers both deployments alone.
///
/// The swap itself costs nothing: `RustlsConfig` is the live handle the running
/// acceptor reads per handshake, so connections already in flight finish under
/// the certificate they negotiated with and the next handshake gets the new one.
fn spawn_cert_reloader(
    tls: RustlsConfig,
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
    metrics: kimmy_api::SharedState,
    shutdown: kimmy_task::Shutdown,
) -> tokio::task::JoinHandle<()> {
    kimmy_task::supervise("cert_reloader", shutdown.clone(), async move {
        // The baseline is taken now, so the first tick compares against what
        // was actually loaded rather than reloading once for no reason.
        let mut seen = stamps(&cert, &key).await;

        let mut hangup = Hangup::install();

        let mut ticker = tokio::time::interval(CERT_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // `interval` fires immediately; that first tick is the one we just took
        // the baseline for.
        ticker.tick().await;

        loop {
            let forced = tokio::select! {
                _ = ticker.tick() => false,
                _ = hangup.recv() => true,
            };

            let current = stamps(&cert, &key).await;
            if should_reload(forced, current, seen) {
                if forced {
                    info!("SIGHUP received, reloading the TLS certificate");
                }
                reload(&tls, &cert, &key, &metrics.metrics).await;
            }
            // Recorded whether or not the reload succeeded, and whether or not
            // one happened: a file that cannot be parsed must not be retried
            // every minute until it changes again, or one bad rotation would
            // fill the log forever. (When no reload happened, `current` already
            // equals `seen`.)
            seen = current;
        }
    })
}

/// SIGHUP, where there is such a thing.
///
/// Wrapped so the reload loop has one shape on every platform. Where the signal
/// is unavailable — a non-unix target, or a handler that would not install —
/// `recv` simply never completes and the poll carries the feature alone.
struct Hangup {
    #[cfg(unix)]
    signal: Option<tokio::signal::unix::Signal>,
}

impl Hangup {
    fn install() -> Self {
        #[cfg(unix)]
        {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
                Ok(signal) => Self { signal: Some(signal) },
                Err(e) => {
                    // Degrade rather than give up the whole feature: the poll
                    // is still worth running without a signal.
                    warn!(
                        error = %e,
                        "could not install the SIGHUP handler; certificate reload is poll-only"
                    );
                    Self { signal: None }
                }
            }
        }
        #[cfg(not(unix))]
        Self {}
    }

    async fn recv(&mut self) {
        #[cfg(unix)]
        match self.signal.as_mut() {
            Some(signal) => {
                signal.recv().await;
            }
            None => std::future::pending().await,
        }
        #[cfg(not(unix))]
        std::future::pending().await
    }
}

/// Modification times of a certificate and its key, absent when either could
/// not be read.
type Stamps = Option<(std::time::SystemTime, std::time::SystemTime)>;

/// Whether this wake-up should re-read the certificate.
///
/// An explicit SIGHUP reloads whatever is on disk, whether or not anything
/// looks different — an operator who has just replaced a file is not asking a
/// question. The timer only acts on a change, or every node would re-parse its
/// certificate once a minute forever.
///
/// Extracted from the loop so it can be tested: the loop itself runs until the
/// process ends, and the interesting part is this decision.
fn should_reload(forced: bool, current: Stamps, seen: Stamps) -> bool {
    forced || current != seen
}

/// Modification times of both files, or `None` if either could not be read.
///
/// A rotation can momentarily replace a file, so a failed stat is not an error
/// here — it compares unequal to whatever was seen last, and the reload that
/// follows reports the real problem if there is one.
async fn stamps(cert: &std::path::Path, key: &std::path::Path) -> Stamps {
    let cert = tokio::fs::metadata(cert).await.ok()?.modified().ok()?;
    let key = tokio::fs::metadata(key).await.ok()?.modified().ok()?;
    Some((cert, key))
}

/// Swap in the certificate on disk, or keep the one already serving.
///
/// A failure here must not take the node down: unlike startup, where a bad
/// certificate is fatal because there is nothing to fall back to, a serving
/// node has something that works. `reload_from_pem_file` parses the pair before
/// it stores it, so a bad or half-written pair leaves the live configuration
/// untouched — which is also what absorbs the window between writing a new
/// certificate and writing its key. See ADR-039 and ADR-049.
async fn reload(
    tls: &RustlsConfig,
    cert: &std::path::Path,
    key: &std::path::Path,
    metrics: &kimmy_api::Metrics,
) {
    match tls.reload_from_pem_file(cert, key).await {
        Ok(()) => {
            metrics.record_tls_reload(true);
            info!(cert = %cert.display(), "TLS certificate reloaded");
        }
        Err(e) => {
            metrics.record_tls_reload(false);
            warn!(
                error = %e,
                cert = %cert.display(),
                key = %key.display(),
                "could not reload the TLS certificate; keeping the one currently in use"
            );
        }
    }
}

/// How long a discovery or JWKS request may take before it is a failure.
///
/// Short on purpose. The task retries, so a slow provider costs a retry rather
/// than a task parked on a socket — and an unbounded fetch here would be a
/// background task that silently stops refreshing keys forever.
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// How soon to try again after a failed fetch.
///
/// Faster than the configured interval, because the state being recovered from
/// is different: the interval keeps a working key set current, this is a node
/// that has not reached its provider at all — possibly since it started, with
/// every federated caller refused until it does.
const JWKS_RETRY_INTERVAL: Duration = Duration::from_secs(30);

/// Keep the identity provider's signing keys current, forever.
///
/// The same shape as the certificate reloader (ADR-049) and for the same
/// reason: key material rotates *by* something rather than by someone, so there
/// is nobody to signal. Two triggers, one refetch — the interval, and a nudge
/// raised when a token names a key id this node has never seen, which is what
/// makes a rotation between two ticks cost one request rather than a whole
/// interval of refusals. The nudge is rate-limited inside `Federation`, because
/// a key id is attacker-controlled.
///
/// The swap costs nothing to a request in flight: the verifier it is already
/// holding keeps working against the key set it was cloned with.
fn spawn_jwks_refresher(
    federation: Arc<kimmy_api::Federation>,
    interval: Duration,
    state: kimmy_api::SharedState,
    shutdown: kimmy_task::Shutdown,
    http: reqwest::Client,
) -> tokio::task::JoinHandle<()> {
    kimmy_task::supervise("jwks_refresher", shutdown.clone(), async move {
        let issuer = federation.issuer();

        loop {
            let wait = match fetch_jwks(&http, &issuer).await {
                Ok(keys) => {
                    let count = keys.keys.len();
                    federation.install_keys(keys);
                    state.metrics.record_jwks_refresh(true);
                    debug!(issuer, keys = count, "refreshed the identity provider's signing keys");
                    interval
                }
                Err(e) => {
                    state.metrics.record_jwks_refresh(false);
                    // Loud, because nothing about a working request reveals
                    // it: the node keeps verifying against the keys it already
                    // holds until the provider rotates, and then refuses every
                    // federated caller at once.
                    warn!(
                        issuer,
                        error = %e,
                        keys = federation.key_count(),
                        "could not refresh the identity provider's signing keys; keeping the \
                         key set already in use"
                    );
                    JWKS_RETRY_INTERVAL.min(interval)
                }
            };

            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = federation.refresh_requested() => {
                    debug!(issuer, "a token named an unknown signing key; re-fetching early");
                }
            }
        }
    })
}

/// The HTTP client used for discovery and JWKS.
///
/// Its own, not the one webhooks use: the timeouts differ, and an identity
/// provider is not an endpoint a caller registered.
pub(crate) fn jwks_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(JWKS_FETCH_TIMEOUT)
        .build()
        .context("building the HTTP client for OIDC discovery")
}

/// Where a provider publishes its discovery document.
///
/// Built by concatenation rather than by URL joining, because that is what
/// RFC 8414 says the location is: the well-known path appended to the issuer,
/// which for an issuer with a path component is *not* what joining produces.
pub(crate) fn discovery_url(issuer: &str) -> String {
    format!("{}/.well-known/openid-configuration", issuer.trim_end_matches('/'))
}

/// The key-set URI a discovery document names, once the document is shown to
/// belong to the issuer it was fetched for.
///
/// **Two checks, and neither is ceremony.**
///
/// The `issuer` member must equal the issuer that was asked for. OpenID
/// Connect Discovery §4.3 and RFC 8414 §3.3 both make this a MUST, and it is
/// the step that binds a document to the identity the node actually trusts:
/// without it, anything that can answer for the well-known path — a redirect
/// followed silently, a stale CDN entry, a hijacked DNS record — chooses the
/// signing keys this node will accept tokens against, which is the whole trust
/// root. The verifier still matches `iss` on every token, so a substituted
/// document cannot by itself authorize anybody; what it can do is point the
/// refresher at a key set an attacker holds the private half of, and then the
/// `iss` match is satisfied too.
///
/// `jwks_uri` must be https, for the reason the issuer itself must be
/// (`OidcConfig::validate`): the key set *is* the trust root, and one fetched
/// over plaintext can be replaced in transit by anyone on the path.
///
/// Pure and separate from the fetch so both can be tested without a network:
/// the failures worth pinning are all about the document's contents.
fn jwks_uri_from<'a>(document: &'a serde_json::Value, issuer: &str, url: &str) -> Result<&'a str> {
    let named = document
        .get("issuer")
        .and_then(|v| v.as_str())
        .with_context(|| format!("the discovery document at {url} names no issuer"))?;
    // Byte-for-byte, exactly as `iss` is matched on a token. RFC 8414 §3.3
    // says the comparison is on the literal string, and a document naming a
    // near-miss is the case this is here to catch.
    if named != issuer {
        anyhow::bail!(
            "the discovery document at {url} says its issuer is {named:?}, not {issuer:?}. \
             A provider's metadata must name the issuer it was fetched for (OpenID Connect \
             Discovery §4.3, RFC 8414 §3.3); a document that does not is either the wrong \
             provider or one substituted on the way here, and its jwks_uri decides which \
             signing keys this node trusts. Check auth.oidc.issuer against what the provider \
             publishes."
        );
    }

    let jwks_uri = document
        .get("jwks_uri")
        .and_then(|v| v.as_str())
        .with_context(|| format!("the discovery document at {url} names no jwks_uri"))?;
    if !is_secure_url(jwks_uri) {
        anyhow::bail!(
            "the discovery document at {url} names a jwks_uri of {jwks_uri:?}, which is not \
             https. The key set is what every federated token is verified against, so fetching \
             it over plaintext would let anyone on the network path choose the keys this node \
             trusts. Only a loopback address is exempt."
        );
    }
    Ok(jwks_uri)
}

/// Whether a URL is one credentials or keys may safely travel over.
///
/// https, or plain http to **loopback**. The exemption is the same one
/// RFC 8252 §7.3 makes for native applications and browsers make for secure
/// contexts, and it rests on the same fact: there is no network path to be on
/// between a process and itself, so the attack the https requirement exists to
/// stop cannot happen. Without it a stub or a locally-run provider would be
/// untestable and undevelopable against, which is a good way to have the check
/// removed later by somebody who only sees it getting in the way.
///
/// The host is parsed rather than matched as a prefix, deliberately.
/// `http://127.0.0.1.attacker.example` starts with a loopback address and is
/// not one, and `http://127.0.0.1@attacker.example` is userinfo — the host is
/// what follows the `@`. Both are refused.
fn is_secure_url(url: &str) -> bool {
    if url.starts_with("https://") {
        return true;
    }
    let Some(rest) = url.strip_prefix("http://") else { return false };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    // Everything before an `@` is userinfo, so a host that appears there is
    // not the host being reached.
    let authority = authority.rsplit('@').next().unwrap_or("");
    let host = match authority.strip_prefix('[') {
        // An IPv6 literal is bracketed, and the port comes after the bracket.
        Some(bracketed) => bracketed.split(']').next().unwrap_or(""),
        None => authority.split(':').next().unwrap_or(""),
    };
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        // Not resolved: a name that resolves to loopback elsewhere is not
        // treated as loopback, because this cannot know that and guessing
        // would make the rule depend on DNS.
        Err(_) => host.eq_ignore_ascii_case("localhost"),
    }
}

/// Fetch the provider's current signing keys, through discovery.
///
/// Discovery every time rather than the JWKS URI being remembered: a provider
/// is allowed to move it, and one extra request every few minutes is not worth
/// a cache that can go stale in a way nothing would report.
#[tracing::instrument(name = "oidc.jwks_refresh", skip_all, fields(
    issuer = %issuer,
    otel.kind = "client",
    keys = tracing::field::Empty,
))]
pub(crate) async fn fetch_jwks(http: &reqwest::Client, issuer: &str) -> Result<JwkSet> {
    // The issuer is on the span deliberately, unlike a database name: it is an
    // operator's own identity provider, it is already in every log line this
    // task writes, and without it a refresh failure in a trace says only that
    // *something* could not be reached. The failure this span exists for is
    // the quiet one — a node that keeps verifying perfectly against the keys
    // it holds until the provider rotates, and then refuses every federated
    // caller at once (ADR-064).
    let url = discovery_url(issuer);
    let document: serde_json::Value = http
        .get(&url)
        .send()
        .await
        .with_context(|| format!("fetching the discovery document from {url}"))?
        .error_for_status()
        .with_context(|| format!("fetching the discovery document from {url}"))?
        .json()
        .await
        .with_context(|| format!("parsing the discovery document from {url}"))?;

    let jwks_uri = jwks_uri_from(&document, issuer, &url)?;

    let keys: JwkSet = http
        .get(jwks_uri)
        .send()
        .await
        .with_context(|| format!("fetching the key set from {jwks_uri}"))?
        .error_for_status()
        .with_context(|| format!("fetching the key set from {jwks_uri}"))?
        .json()
        .await
        .with_context(|| format!("parsing the key set from {jwks_uri}"))?;

    // An empty set is treated as a failed fetch, not as a successful one that
    // happens to trust nothing. Installing it would replace a working key set
    // with one that refuses every token — a provider mid-deploy must not be
    // able to lock this node out.
    if keys.keys.is_empty() {
        anyhow::bail!("the key set at {jwks_uri} is empty");
    }
    tracing::Span::current().record("keys", keys.keys.len() as i64);
    Ok(keys)
}

/// Reach the identity provider once and report what happened.
///
/// What `check-config` runs. The server deliberately does *not* do this at
/// startup — it retries in the background instead — so this is the only place
/// an operator gets a straight answer about whether the provider is reachable
/// and its keys are usable, at a moment when they are watching.
pub async fn probe_oidc(oidc: &OidcConfig) -> Result<usize> {
    let issuer = oidc.issuer.as_deref().context("no issuer configured")?;
    let keys = fetch_jwks(&jwks_client()?, issuer).await?;
    Ok(keys.keys.len())
}

/// Serve the router, with or without TLS.
///
/// Both paths use `into_make_service_with_connect_info`, which is not optional:
/// it is what puts the peer address in the request extensions, and without it
/// every caller shares one rate-limit bucket — silently, since requests still
/// succeed. Both paths also shut down gracefully, because a node that drops
/// in-flight requests on SIGTERM makes every rolling restart a source of
/// client-visible errors.
async fn serve(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    tls: Option<RustlsConfig>,
    shutdown: kimmy_task::Shutdown,
) -> Result<()> {
    let service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();

    let Some(tls) = tls else {
        axum::serve(listener, service).with_graceful_shutdown(announced(shutdown.clone())).await?;
        return Ok(());
    };

    // `axum::serve` has no TLS, so the TLS path runs on axum-server. It takes a
    // std listener, which lets the bind stay where it was — a port already in
    // use is still a startup error rather than a warning in a log nobody reads.
    let std_listener = listener.into_std().context("converting the listener")?;
    // Must stay non-blocking. `into_std` preserves the flag tokio set, and
    // axum-server re-registers the socket with the runtime — handing it a
    // blocking one panics at the first connection, not at startup.
    std_listener.set_nonblocking(true).context("configuring the listener")?;

    let handle = axum_server::Handle::new();
    // UNSUPERVISED: the shutdown watcher, whose return *is* shutdown. It must be free to
    // finish during the drain, which is exactly what a supervisor would cut short.
    tokio::spawn({
        let handle = handle.clone();
        let shutdown = shutdown.clone();
        async move {
            announced(shutdown).await;
            handle.graceful_shutdown(Some(DRAIN_TIMEOUT));
        }
    });

    axum_server::from_tcp_rustls(std_listener, tls)
        .context("preparing the TLS listener")?
        .handle(handle)
        .serve(service)
        .await?;
    Ok(())
}

fn is_loopback(addr: &std::net::SocketAddr) -> bool {
    addr.ip().is_loopback()
}

/// Start the replication listener and peer loop, unless clustering is off.
///
/// Binding happens here rather than inside the task so that a port already in
/// use is a startup failure with a clear message, not a warning in a log nobody
/// reads while the node silently never replicates.
/// The cluster tasks, plus the live member set the webhook dispatcher derives
/// ownership from.
///
/// This no longer carries an advertised address: ownership hashes node ids, and
/// this node's id exists whether or not clustering does (ADR-051).
struct Cluster {
    tasks: Vec<tokio::task::JoinHandle<()>>,
    members: Option<kimmy_cluster::Members>,
}

async fn spawn_cluster(
    engine: Arc<Engine>,
    state: kimmy_api::SharedState,
    config: &Config,
    shutdown: kimmy_task::Shutdown,
) -> Result<Cluster> {
    if !config.cluster.enabled {
        return Ok(Cluster { tasks: Vec::new(), members: None });
    }

    let secret = config.cluster.cluster_secret.clone().context(
        "cluster.enabled is set with no cluster_secret (validation should have caught this)",
    )?;

    let listener = tokio::net::TcpListener::bind(config.cluster.bind)
        .await
        .with_context(|| format!("binding the cluster listener on {}", config.cluster.bind))?;
    let local = listener.local_addr().unwrap_or(config.cluster.bind);

    // A schema change a peer pushes here (ADR-140) goes through the same
    // batch application a pulled one does, and lands on the same counter when
    // this node cannot apply it: the member's own metric must not depend on
    // which way the change arrived.
    let on_pushed: kimmy_cluster::PushHook = Arc::new({
        let state = Arc::clone(&state);
        move |outcome: &kimmy_storage::SyncOutcome| {
            state.metrics.record_ddl_refused(outcome.ddl_refused as u64);
            state.metrics.record_ddl_declined(outcome.ddl_declined as u64);
            state
                .metrics
                .record_entries_skipped(outcome.unknown_collection as u64, outcome.deferred as u64);
        }
    });
    // Built before the node commits to serving. `serve_with` used to build it
    // and return on failure, which made a fatal condition fatal to that task
    // only: the node went on serving while no peer could pull from it. With
    // clustering enabled the encryption is always on and has no switch
    // (ADR-040), so a TLS that will not start is a startup failure (ADR-184).
    let cluster_tls = Arc::new(
        kimmy_cluster::tls::ClusterTls::new()
            .map_err(|e| anyhow::anyhow!("{e}"))
            .context("starting cluster TLS, which cluster.enabled requires")?,
    );
    let serving = kimmy_task::supervise(
        "replication_server",
        shutdown.clone(),
        kimmy_cluster::serve_with(
            Arc::clone(&engine),
            listener,
            secret.clone(),
            Some(on_pushed),
            cluster_tls,
        ),
    );

    // SWIM shares the port with replication: UDP for probes and membership,
    // TCP for oplog transfer. Bound here for the same reason as the listener —
    // a port already in use should fail at startup, not in a log line.
    let mut cluster_tasks = vec![serving];
    let mut members = None;
    let mut announce = None;

    if config.cluster.membership {
        let socket = tokio::net::UdpSocket::bind(config.cluster.bind)
            .await
            .with_context(|| format!("binding the membership socket on {}", config.cluster.bind))?;

        let live = kimmy_cluster::Members::default();
        let (tx, feed) = kimmy_cluster::SeedFeed::channel();
        // The same secret the replication handshake uses: membership is
        // authenticated too, so an unauthenticated node cannot join the member
        // set that webhook ownership is computed over (ADR-053).
        cluster_tasks.push(kimmy_task::supervise(
            "membership",
            shutdown.clone(),
            kimmy_cluster::membership::run(
                socket,
                advertised(local),
                engine.node_id(),
                secret.clone(),
                live.clone(),
                feed,
                shutdown.clone(),
            ),
        ));
        members = Some(live);
        announce = Some(tx);
    } else {
        warn!("SWIM membership is disabled; peers come from discovery alone");
    }

    // A schema change confirms itself on every live member before its request
    // answers (ADR-140). The member set is SWIM's, so this needs membership;
    // with it off the node answers as it always did, saying nothing about
    // its peers.
    match (&members, config.cluster.ddl_confirm_timeout_secs) {
        (Some(live), secs) if secs > 0 => {
            state.set_ddl_confirmer(ddl_confirmer(
                Arc::clone(&engine),
                secret.clone(),
                live.clone(),
                Duration::from_secs(secs),
            ));
        }
        (Some(_), _) => {
            info!("schema-change confirmation is off (cluster.ddl_confirm_timeout_secs = 0)")
        }
        (None, _) => {
            warn!(
                "schema-change confirmation needs membership; a createIndex will not wait for peers"
            )
        }
    }

    let replicating = kimmy_task::supervise(
        "replication",
        shutdown.clone(),
        kimmy_cluster::replicate(
            engine,
            kimmy_cluster::ReplicationConfig {
                seeds: config.cluster.seeds.clone(),
                secret,
                local,
                sync_interval: Duration::from_secs(config.cluster.sync_interval_secs),
                discovery_interval: Duration::from_secs(config.cluster.discovery_interval_secs),
                fanout: config.cluster.fanout,
                announce,
                // Cloned: the replication loop and the webhook dispatcher both
                // read the same live set, and `Members` is a shared handle.
                members: members.clone(),
                tombstone_retention: Duration::from_secs(config.storage.tombstone_retention_secs),
                // A stale rejoiner is a fact about a peer's vector, which only the
                // loop sees; the API keeps the record for `/v1/topology` (ADR-085).
                on_peer_staleness: Some(std::sync::Arc::new({
                    let state = state.clone();
                    move |node, behind_ms| state.report_peer_staleness(node, behind_ms)
                })),
                // What the loop saw that lag cannot say: rounds that failed,
                // peers backed off, schema changes refused. Pushed after every
                // tick, reached peers or not, because a tick in which every
                // round failed is the one that leaves the lag gauge at its last
                // value and the cluster looking healthy (ADR-123). Two of its
                // fields say whether the divergence check ran at all, so that
                // gauge's 0 can be told apart from silence (ADR-135).
                //
                // Handed over whole rather than unpacked into arguments here:
                // this closure is the only caller of `record_sync_round` and no
                // test covers it, so a pair of same-typed positional arguments
                // transposed on this line would compile, pass every gate, and
                // report one series under another's name until somebody read a
                // dashboard closely. There is nothing here to get in the wrong
                // order (ADR-135).
                on_round: Some(std::sync::Arc::new({
                    let state = state.clone();
                    move |report: kimmy_cluster::RoundReport| {
                        state.metrics.record_sync_round(&report);
                    }
                })),
                // The replication loop is the only place a peer's version vector
                // exists, so lag is pushed from there into the gauge (ADR-046).
                on_lag: Some(std::sync::Arc::new(move |ms| {
                    state.metrics.set_replication_lag_ms(ms);
                })),
            },
        ),
    );
    cluster_tasks.push(replicating);

    info!(
        bind = %local,
        seeds = config.cluster.seeds.len(),
        membership = config.cluster.membership,
        "clustering enabled"
    );
    Ok(Cluster { tasks: cluster_tasks, members })
}

/// How this node confirms a schema change on its live members (ADR-140).
///
/// Hands every member, at once, the window it lacks from this node ending in
/// the entry (ADR-143), and waits for each, bounded by `deadline` per member,
/// so the request waits about as long as the slowest member takes rather
/// than the sum. A member the window cannot reach — too far behind — is
/// named pending with the reason. A member that answers with a refusal is
/// named as such — it counted the refusal itself, on its own
/// `kimmy_sync_ddl_refused_total` — and one that does not answer is named
/// pending with the reason. Anti-entropy still carries the change to both,
/// as it always did; what the push adds is the response meaning what a
/// client reads it to mean.
fn ddl_confirmer(
    engine: Arc<Engine>,
    secret: String,
    members: kimmy_cluster::Members,
    deadline: Duration,
) -> kimmy_api::DdlConfirmer {
    Arc::new(move |entry: kimmy_core::OplogEntry| {
        let engine = Arc::clone(&engine);
        let secret = secret.clone();
        let members = members.clone();
        Box::pin(async move {
            let mut pushes = tokio::task::JoinSet::new();
            for (addr, node) in members.entries() {
                let engine = Arc::clone(&engine);
                let secret = secret.clone();
                let entry = entry.clone();
                // UNSUPERVISED: one push per peer in a JoinSet this round awaits. It is
                // this round's work, not background work, and the round reports it.
                pushes.spawn(async move {
                    let pushed = tokio::time::timeout(
                        deadline,
                        kimmy_cluster::push_entry(&engine, addr, &secret, &entry),
                    )
                    .await;
                    let result = match pushed {
                        Ok(Ok(pushed)) => Ok(pushed),
                        Ok(Err(e)) => Err(e.to_string()),
                        Err(_) => Err(format!("no answer within {deadline:?}")),
                    };
                    (addr, node, result)
                });
            }
            let mut found = kimmy_api::DdlConfirmation::default();
            while let Some(joined) = pushes.join_next().await {
                let Ok((addr, node, result)) = joined else { continue };
                match result {
                    Ok(kimmy_cluster::PushOutcome { unreached: Some(reason), .. }) => {
                        info!(
                            peer = %addr,
                            node = %node,
                            %reason,
                            "a member was not pushed a schema change; anti-entropy will carry it"
                        );
                        found.pending.push((node, reason));
                    }
                    Ok(kimmy_cluster::PushOutcome { outcome, .. })
                        if outcome.ddl_refused > 0
                            || outcome.unknown_collection > 0
                            || outcome.ddl_declined > 0 =>
                    {
                        warn!(
                            peer = %addr,
                            node = %node,
                            refused = outcome.ddl_refused,
                            unknown_collection = outcome.unknown_collection,
                            declined = outcome.ddl_declined,
                            "a member could not apply a schema change pushed to it"
                        );
                        found.refused.push(node);
                    }
                    Ok(_) => found.confirmed.push(node),
                    Err(reason) => {
                        warn!(
                            peer = %addr,
                            node = %node,
                            %reason,
                            "a member did not confirm a schema change; anti-entropy will carry it"
                        );
                        found.pending.push((node, reason));
                    }
                }
            }
            // Deterministic order, whatever order the members answered in.
            found.confirmed.sort();
            found.refused.sort();
            found.pending.sort();
            found
        })
    })
}

/// The address peers should use to reach this node.
///
/// A wildcard bind is a listening instruction, not an identity: announcing
/// `0.0.0.0` would tell the cluster to probe an address that routes nowhere.
/// Falling back to loopback keeps a single-host cluster working, and a real
/// deployment binds an address it can be reached on.
fn advertised(bind: std::net::SocketAddr) -> std::net::SocketAddr {
    if bind.ip().is_unspecified() {
        let mut resolved = bind;
        resolved.set_ip(std::net::Ipv4Addr::LOCALHOST.into());
        warn!(%bind, advertised = %resolved, "cluster bind is a wildcard; advertising loopback");
        resolved
    } else {
        bind
    }
}

/// Start the retention collector, unless it is disabled.
fn spawn_collector(
    engine: Arc<Engine>,
    config: &Config,
    shutdown: kimmy_task::Shutdown,
) -> Option<tokio::task::JoinHandle<()>> {
    if config.storage.gc_interval_secs == 0 {
        warn!("retention collection is disabled; the oplog and tombstones will grow without bound");
        return None;
    }

    let interval = Duration::from_secs(config.storage.gc_interval_secs);
    let policy = RetentionPolicy::new(
        config.storage.oplog_retention_secs,
        config.storage.tombstone_retention_secs,
    );

    Some(kimmy_task::supervise("retention_collector", shutdown.clone(), async move {
        // A sleep of the whole interval *after* each pass, not a ticker. A
        // pass that overruns the interval must not be followed by the next
        // one at once: a ticker's default catches up on every missed tick
        // immediately, which ran passes back to back on a member whose pass
        // took longer than the interval and left its writer held almost
        // continuously (ADR-151) — and its `Delay` behaviour still fires the
        // one tick that was already due the moment the late pass ends, so a
        // pass that always overruns would still chain. Sleeping after the
        // pass is the only schedule under which the writer is guaranteed a
        // full interval free between passes. It also means nothing collects
        // during start-up, while the node is still opening for business.
        loop {
            tokio::time::sleep(interval).await;
            let started = std::time::Instant::now();
            // A pass reads whole tables; on a cold cache that is minutes of
            // disk, and a worker thread must not be held for it.
            let result = kimmy_storage::blocking(|| engine.collect_garbage(policy));
            let elapsed = started.elapsed();
            match result {
                // A failed pass is not fatal — the garbage is still there and
                // the next tick will find it — so it is logged and retried
                // rather than taking the node down.
                Err(e) => warn!(error = %e, "retention pass failed"),
                Ok(outcome) if outcome.is_empty() => {}
                Ok(outcome) => info!(
                    oplog = outcome.oplog_removed,
                    tombstones = outcome.tombstones_removed,
                    dropped_rows = outcome.dropped_rows_removed,
                    elapsed_ms = elapsed.as_millis() as u64,
                    "collected expired records"
                ),
            }
            if elapsed >= interval {
                warn!(
                    elapsed_secs = elapsed.as_secs(),
                    interval_secs = interval.as_secs(),
                    "a retention pass took longer than storage.gc_interval_secs; the next \
                     runs a full interval after this one finished"
                );
            }
        }
    }))
}

/// Create the bootstrap superuser on first start.
fn bootstrap_users(engine: &Engine, config: &Config) -> Result<()> {
    let store = UserStore::open(engine).context("opening the user store")?;
    let password = config
        .auth
        .root_password
        .as_deref()
        .context("no root password configured (validation should have caught this)")?;

    if store.bootstrap_root(engine, &config.auth.root_user, password)? {
        info!(user = %config.auth.root_user, "bootstrapped the superuser");
    }
    Ok(())
}

/// Wait for the shutdown signal, then announce it **before** returning.
///
/// The announcement has to happen before anything drains, because every
/// supervised task reads it to tell a stop from a death. Putting it here rather
/// than after `serve` returns makes the ordering structural: there is no path
/// from the signal to a drained server that skips it.
async fn announced(shutdown: kimmy_task::Shutdown) {
    shutdown_signal().await;
    shutdown.begin();
}

/// Resolve on SIGINT or SIGTERM.
///
/// SIGTERM matters most — it is what Docker and Kubernetes send, and ignoring it
/// means a hard kill after the grace period.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                warn!(error = %e, "could not install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("shutdown signal received, draining");
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::extract::ConnectInfo;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[test]
    fn node_identity_is_stable_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DATABASE_FILE);

        let first = Engine::open(&path).unwrap().node_id();
        let second = Engine::open(&path).unwrap().node_id();
        assert_eq!(first, second, "identity must survive a restart");
    }

    #[test]
    fn auth_on_with_no_secret_refuses_rather_than_signing_with_the_constant() {
        // The second half of the defence. `Config::validate` refuses this
        // combination at the CLI; this is the line where the constant would
        // otherwise become a signing key, and a forged token follows from that.
        let mut auth = AuthConfig {
            root_password: Some("hunter2".into()),
            jwt_secret: None,
            insecure_no_auth: false,
            ..Default::default()
        };

        let err = signing_key(&auth).unwrap_err().to_string();
        assert!(err.contains("jwt_secret"), "unhelpful error: {err}");

        // With auth off the throwaway key is correct: nothing verifies a token.
        auth.insecure_no_auth = true;
        assert_eq!(signing_key(&auth).unwrap(), THROWAWAY_SIGNING_KEY);

        // A configured secret always wins, so the throwaway can never displace
        // an operator's key.
        auth.jwt_secret = Some("a-signing-key-of-adequate-length".into());
        assert_eq!(signing_key(&auth).unwrap(), "a-signing-key-of-adequate-length");
        auth.insecure_no_auth = false;
        assert_eq!(signing_key(&auth).unwrap(), "a-signing-key-of-adequate-length");
    }

    #[test]
    fn the_throwaway_key_satisfies_the_token_issuer() {
        // It is only ever used with auth off, but `TokenIssuer::new` rejects a
        // short secret — so shortening this constant would stop an auth-off
        // node from starting at all.
        TokenIssuer::new(THROWAWAY_SIGNING_KEY, 3600).expect("throwaway key must be accepted");
    }

    /// A self-signed certificate for `localhost`, written to a temp directory.
    ///
    /// Generated per run rather than checked in: a private key in the
    /// repository trips secret scanners and eventually expires, and neither
    /// problem is worth inheriting for a fixture this cheap to build.
    fn self_signed() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf, Vec<u8>) {
        let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");
        std::fs::write(&cert_path, issued.cert.pem()).unwrap();
        std::fs::write(&key_path, issued.signing_key.serialize_pem()).unwrap();
        let der = issued.cert.der().to_vec();
        (dir, cert_path, key_path, der)
    }

    /// Another self-signed certificate for `localhost`, written over the paths
    /// an existing one occupies.
    ///
    /// A *different* certificate, deliberately: the only way to prove a reload
    /// swapped anything is a client that trusts the new one and not the old.
    fn overwrite_with_a_new_cert(
        cert_path: &std::path::Path,
        key_path: &std::path::Path,
    ) -> Vec<u8> {
        let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        std::fs::write(cert_path, issued.cert.pem()).unwrap();
        std::fs::write(key_path, issued.signing_key.serialize_pem()).unwrap();
        issued.cert.der().to_vec()
    }

    /// Whether a handshake against `addr` succeeds while trusting only `der`.
    async fn handshake_trusting(addr: SocketAddr, der: Vec<u8>) -> bool {
        let Ok(stream) = tokio::net::TcpStream::connect(addr).await else {
            return false;
        };
        let name =
            tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap().to_owned();
        client_config(der).connect(name, stream).await.is_ok()
    }

    #[tokio::test]
    async fn a_reload_swaps_the_certificate_the_next_handshake_gets() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (_dir, cert, key, first_der) = self_signed();

        let tls = RustlsConfig::from_pem_file(&cert, &key).await.unwrap();
        let addr = serve_echoing_peer(Some(tls.clone())).await;
        let metrics = kimmy_api::Metrics::default();

        assert!(
            handshake_trusting(addr, first_der.clone()).await,
            "the certificate the server started with must serve"
        );

        let second_der = overwrite_with_a_new_cert(&cert, &key);
        reload(&tls, &cert, &key, &metrics).await;

        assert!(
            handshake_trusting(addr, second_der).await,
            "a new handshake must get the reloaded certificate, with no restart and no rebind"
        );
        assert!(
            !handshake_trusting(addr, first_der).await,
            "and the replaced certificate must be gone, or the reload only appeared to work"
        );
        assert!(
            metrics.render().contains("kimmy_tls_reloads_total{outcome=\"ok\"} 1"),
            "a successful reload is counted"
        );
    }

    #[tokio::test]
    async fn an_unreadable_certificate_leaves_the_one_in_use_serving() {
        // The hazard this whole feature introduces. At startup a bad
        // certificate is fatal because there is nothing to fall back to; at
        // reload there is, and taking the node down instead would turn a
        // botched rotation into an outage. See ADR-039 and ADR-049.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (_dir, cert, key, der) = self_signed();

        let tls = RustlsConfig::from_pem_file(&cert, &key).await.unwrap();
        let addr = serve_echoing_peer(Some(tls.clone())).await;
        let metrics = kimmy_api::Metrics::default();

        std::fs::write(&cert, b"-----BEGIN CERTIFICATE-----\nnot a certificate\n").unwrap();
        reload(&tls, &cert, &key, &metrics).await;

        assert!(
            handshake_trusting(addr, der).await,
            "a node serving a good certificate must keep serving it when a bad one appears"
        );
        assert!(
            metrics.render().contains("kimmy_tls_reloads_total{outcome=\"failed\"} 1"),
            "and the failure must be visible, because its consequence arrives weeks later"
        );
    }

    #[tokio::test]
    async fn a_half_rotated_pair_is_refused_rather_than_half_applied() {
        // Replacing a certificate and its key is two writes, and between them
        // the pair does not match. The window is absorbed, not closed: the
        // mismatch fails to parse and the old pair keeps serving until the
        // next attempt finds both halves.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (_dir, cert, key, der) = self_signed();

        let tls = RustlsConfig::from_pem_file(&cert, &key).await.unwrap();
        let addr = serve_echoing_peer(Some(tls.clone())).await;
        let metrics = kimmy_api::Metrics::default();

        // A new certificate, but still the old key.
        let orphan = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        std::fs::write(&cert, orphan.cert.pem()).unwrap();

        reload(&tls, &cert, &key, &metrics).await;
        assert!(
            handshake_trusting(addr, der).await,
            "a mismatched pair must not be adopted; the previous one keeps serving"
        );

        // Now the key lands, and the pair is whole.
        std::fs::write(&key, orphan.signing_key.serialize_pem()).unwrap();
        reload(&tls, &cert, &key, &metrics).await;
        assert!(
            handshake_trusting(addr, orphan.cert.der().to_vec()).await,
            "once both halves are on disk the retry must succeed"
        );
    }

    #[tokio::test]
    async fn the_poll_notices_a_rewritten_file_and_ignores_an_untouched_one() {
        let (_dir, cert, key, _) = self_signed();

        let before = stamps(&cert, &key).await;
        assert!(before.is_some(), "both files exist, so both stat");
        assert_eq!(before, stamps(&cert, &key).await, "an untouched pair must not look changed");

        // Filesystem timestamps are coarse enough that an immediate rewrite can
        // land in the same tick, so the mtime is set explicitly rather than
        // raced for.
        overwrite_with_a_new_cert(&cert, &key);
        let later = std::time::SystemTime::now() + Duration::from_secs(10);
        std::fs::File::options().write(true).open(&cert).unwrap().set_modified(later).unwrap();

        assert_ne!(before, stamps(&cert, &key).await, "a rewritten certificate must look changed");
    }

    #[test]
    fn a_signal_always_reloads_and_the_timer_only_reloads_on_a_change() {
        let t = |secs: u64| std::time::UNIX_EPOCH + Duration::from_secs(secs);
        let a: Stamps = Some((t(1), t(2)));
        let b: Stamps = Some((t(9), t(2)));

        // The timer: a change, and only a change.
        assert!(should_reload(false, b, a), "a moved mtime must reload");
        assert!(!should_reload(false, a, a), "an unchanged pair must not");
        // A file that vanished mid-rotation reads as changed, not as nothing.
        assert!(should_reload(false, None, a));
        assert!(should_reload(false, a, None));

        // SIGHUP: unconditional. An operator who has just replaced a file is
        // not asking whether it looks different.
        assert!(should_reload(true, a, a), "a signal must reload regardless");
        assert!(should_reload(true, None, None));
    }

    #[tokio::test]
    async fn a_missing_file_reads_as_changed_rather_than_as_an_error() {
        // A rotation can momentarily unlink a file. That must not panic, and it
        // must not read as "unchanged" — the reload that follows is what
        // reports the real problem, if there is one.
        let (_dir, cert, key, _) = self_signed();
        let present = stamps(&cert, &key).await;

        std::fs::remove_file(&cert).unwrap();
        assert_eq!(stamps(&cert, &key).await, None);
        assert_ne!(stamps(&cert, &key).await, present);
    }

    /// Serve a router that reports the caller's address, and return where it is.
    async fn serve_echoing_peer(tls: Option<RustlsConfig>) -> SocketAddr {
        async fn peer(ConnectInfo(addr): ConnectInfo<SocketAddr>) -> String {
            addr.to_string()
        }

        let app = axum::Router::new().route("/peer", axum::routing::get(peer));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // Nothing shuts this down: the test drops it when it is finished.
            let _ = serve(listener, app, tls, kimmy_task::Shutdown::new()).await;
        });
        addr
    }

    /// A TLS client that trusts exactly the certificate it is given.
    fn client_config(trusted: Vec<u8>) -> tokio_rustls::TlsConnector {
        let mut roots = tokio_rustls::rustls::RootCertStore::empty();
        roots.add(trusted.into()).unwrap();
        let config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
    }

    #[tokio::test]
    async fn tls_serves_requests_and_still_reports_the_caller() {
        // Two properties in one request, because the second fails silently.
        // Encryption announces itself when it breaks — a handshake error is
        // loud. Losing `ConnectInfo` does not: requests keep succeeding, and
        // the only symptom is that every caller shares one rate-limit bucket,
        // which nothing in a response would reveal.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (_dir, cert, key, der) = self_signed();

        let tls = RustlsConfig::from_pem_file(&cert, &key).await.expect("load the test cert");
        let addr = serve_echoing_peer(Some(tls)).await;

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let server_name =
            tokio_rustls::rustls::pki_types::ServerName::try_from("localhost").unwrap().to_owned();
        let mut tls_stream = client_config(der)
            .connect(server_name, stream)
            .await
            .expect("the handshake must succeed against the certificate the server was given");

        tls_stream
            .write_all(b"GET /peer HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut raw = Vec::new();
        tls_stream.read_to_end(&mut raw).await.unwrap();
        let response = String::from_utf8_lossy(&raw);

        assert!(response.starts_with("HTTP/1.1 200"), "expected 200, got: {response}");
        let body = response.rsplit("\r\n\r\n").next().unwrap_or_default();
        assert!(
            body.starts_with("127.0.0.1:"),
            "the peer address must survive the TLS serving stack, or every caller shares one \
             rate-limit bucket; handler saw {body:?}"
        );
    }

    #[tokio::test]
    async fn a_plaintext_client_is_refused_by_a_tls_listener() {
        // Otherwise a misconfigured client could silently fall back to sending
        // credentials in the clear against a port believed to be encrypted.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (_dir, cert, key, _der) = self_signed();

        let tls = RustlsConfig::from_pem_file(&cert, &key).await.unwrap();
        let addr = serve_echoing_peer(Some(tls)).await;

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /peer HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut raw = Vec::new();
        let _ = stream.read_to_end(&mut raw).await;

        assert!(
            !String::from_utf8_lossy(&raw).contains("200"),
            "a plaintext request must not be answered by a TLS listener, got {} bytes",
            raw.len()
        );
    }

    #[tokio::test]
    async fn the_plaintext_path_still_reports_the_caller() {
        // The same invariant on the other branch: adding TLS must not quietly
        // change how an unencrypted node sees its callers.
        let addr = serve_echoing_peer(None).await;

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /peer HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.unwrap();
        let response = String::from_utf8_lossy(&raw);

        assert!(response.starts_with("HTTP/1.1 200"), "expected 200, got: {response}");
        let body = response.rsplit("\r\n\r\n").next().unwrap_or_default();
        assert!(body.starts_with("127.0.0.1:"), "handler saw {body:?}");
    }

    #[test]
    fn discovery_is_the_well_known_path_appended_to_the_issuer() {
        // Appended, not URL-joined. RFC 8414 says the path goes on the end of
        // the issuer, and for an issuer that already has a path — which is how
        // Keycloak and Entra ID name realms and tenants — joining would drop
        // it and look for the document at the host root instead.
        assert_eq!(
            discovery_url("https://auth.example.com"),
            "https://auth.example.com/.well-known/openid-configuration"
        );
        assert_eq!(
            discovery_url("https://auth.example.com/realms/kimmy"),
            "https://auth.example.com/realms/kimmy/.well-known/openid-configuration"
        );
        // A trailing slash must not double one, or the provider answers 404.
        assert_eq!(
            discovery_url("https://auth.example.com/"),
            "https://auth.example.com/.well-known/openid-configuration"
        );
    }

    #[test]
    fn a_discovery_document_must_name_the_issuer_it_was_fetched_for() {
        // OpenID Connect Discovery §4.3 / RFC 8414 §3.3. This is what binds
        // the document to the identity being trusted: its jwks_uri decides
        // which signing keys this node accepts tokens against, so anything
        // able to answer for the well-known path would otherwise choose the
        // trust root outright.
        let url = "https://auth.example.com/.well-known/openid-configuration";
        let good = serde_json::json!({
            "issuer": "https://auth.example.com",
            "jwks_uri": "https://auth.example.com/keys",
        });
        assert_eq!(
            jwks_uri_from(&good, "https://auth.example.com", url).unwrap(),
            "https://auth.example.com/keys"
        );

        // A near miss is the case worth pinning: matched byte for byte,
        // exactly as `iss` is matched on a token.
        for named in ["https://auth.example.com/", "https://evil.example.com", "AUTH.EXAMPLE.COM"] {
            let mut document = good.clone();
            document["issuer"] = serde_json::json!(named);
            let err = jwks_uri_from(&document, "https://auth.example.com", url)
                .expect_err(&format!("{named:?} is not the issuer"))
                .to_string();
            assert!(err.contains(named), "the error must name what it found: {err}");
        }

        let mut anonymous = good.clone();
        anonymous.as_object_mut().unwrap().remove("issuer");
        let err = jwks_uri_from(&anonymous, "https://auth.example.com", url).unwrap_err();
        assert!(err.to_string().contains("no issuer"), "unhelpful error: {err}");
    }

    #[test]
    fn a_plaintext_key_set_uri_is_refused_unless_it_is_loopback() {
        // The key set is the trust root. Fetched over plaintext, anyone on the
        // path replaces it and mints tokens this node accepts — and note the
        // jwks_uri need not share a host with the issuer, so an https issuer
        // does not imply an https key set.
        let url = "https://auth.example.com/.well-known/openid-configuration";
        let document = |jwks_uri: &str| serde_json::json!({ "issuer": "https://auth.example.com", "jwks_uri": jwks_uri });

        let err = jwks_uri_from(
            &document("http://auth.example.com/keys"),
            "https://auth.example.com",
            url,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("not https"), "unhelpful error: {err}");

        // Loopback is exempt, which is what keeps a locally-run provider
        // usable — and what the stub in these tests relies on.
        assert!(
            jwks_uri_from(&document("http://127.0.0.1:8080/keys"), "https://auth.example.com", url)
                .is_ok()
        );
    }

    #[test]
    fn only_a_real_loopback_host_is_exempt_from_https() {
        // Prefix-matching the host would accept all three of the first group:
        // a subdomain that merely starts with the address, userinfo hiding the
        // real host after an `@`, and a name nothing here can resolve.
        for url in [
            "http://127.0.0.1.attacker.example/keys",
            "http://127.0.0.1@attacker.example/keys",
            "http://auth.internal/keys",
            "ftp://127.0.0.1/keys",
        ] {
            assert!(!is_secure_url(url), "{url} must not count as secure");
        }
        for url in [
            "https://auth.example.com/keys",
            "http://127.0.0.1/keys",
            "http://127.0.0.53:8080/keys",
            "http://localhost:9000/keys",
            "http://LOCALHOST/keys",
            "http://[::1]:8080/keys",
        ] {
            assert!(is_secure_url(url), "{url} must count as secure");
        }
    }

    /// A stub identity provider: discovery pointing at a key set, both served
    /// once over plain HTTP.
    ///
    /// A stub rather than a live provider on purpose — a test that needs an IdP
    /// reachable is a test that fails for reasons that have nothing to do with
    /// this code.
    async fn stub_idp(jwks_body: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer = format!("http://{addr}");
        let discovery = format!(r#"{{"issuer":"{issuer}","jwks_uri":"{issuer}/keys"}}"#);

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else { return };
                let mut buf = [0u8; 2048];
                let Ok(read) = socket.read(&mut buf).await else { continue };
                let request = String::from_utf8_lossy(&buf[..read]).into_owned();
                let body = if request.contains("/keys") { jwks_body } else { discovery.as_str() };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        issuer
    }

    /// One RSA public key, in the shape a provider publishes it.
    const STUB_JWKS: &str = concat!(
        r#"{"keys":[{"kty":"RSA","alg":"RS256","use":"sig","kid":"stub-key-1","#,
        r#""n":"skSt8G8fV8ZteU7D70PGq2y5Q_dtHnxjsnGfr054ja_rNuLyQxmniWViaLYLNujpZeHR32dx"#,
        r#"Fivh_7sZO9RbsgW_umwV1HCO5EEDMIITwoERFyGxMOeWR8m8ow6KDHI4O0J1Y6maNqU6oBCtyhgy"#,
        r#"-6G91O6q8c2ZzPD7iwSgy1s9uaA88H4w4_sUJ0KIJXO6Kpgo_Qdtm-RHeKG4V_LYWevCfSP9hOUL"#,
        r#"rEI2X9jIi_P1S4OcGj4ieTtF56TA_lYPVw5lBHtNORyliGq6s0knhOzx5DMlsrWBhv7cD8XgHpK0"#,
        r#"pU0TVkrUDf_KcCLd3H5uyw7k3RzaK670bmwF1bpskQ","e":"AQAB"}]}"#,
    );

    #[tokio::test]
    async fn the_key_set_is_fetched_through_the_discovery_document() {
        // The whole path an operator depends on: issuer to discovery to
        // jwks_uri to keys. A provider is free to move the key set, which is
        // why the jwks_uri is read every time rather than remembered.
        let issuer = stub_idp(STUB_JWKS).await;
        let keys = fetch_jwks(&jwks_client().unwrap(), &issuer).await.unwrap();

        assert_eq!(keys.keys.len(), 1);
        assert_eq!(keys.keys[0].common.key_id.as_deref(), Some("stub-key-1"));
        assert!(keys.find("stub-key-1").is_some(), "the key must be findable by its id");
    }

    #[tokio::test]
    async fn an_empty_key_set_is_a_failed_fetch_rather_than_a_successful_one() {
        // Installing it would replace a working key set with one that refuses
        // every token, so a provider mid-deploy could lock this node out.
        let issuer = stub_idp(r#"{"keys":[]}"#).await;
        let err = fetch_jwks(&jwks_client().unwrap(), &issuer).await.unwrap_err().to_string();

        assert!(err.contains("empty"), "unhelpful error: {err}");
    }

    #[tokio::test]
    async fn a_discovery_document_with_no_jwks_uri_is_reported_as_such() {
        // The mistake this catches is pointing `issuer` at something that
        // answers JSON but is not an identity provider.
        //
        // The document names the issuer, so this reaches the jwks_uri check
        // rather than stopping at the issuer binding — the two failures are
        // different mistakes and must not be reported as each other.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let body = format!(r#"{{"issuer":"{issuer}"}}"#);
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else { return };
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });

        let err = fetch_jwks(&jwks_client().unwrap(), &issuer).await.unwrap_err().to_string();
        assert!(err.contains("jwks_uri"), "unhelpful error: {err}");
    }

    #[tokio::test]
    async fn a_substituted_discovery_document_is_refused_over_the_wire() {
        // The unit test above pins the rule; this pins that `fetch_jwks`
        // actually applies it, rather than the check living somewhere the
        // fetch path never reaches.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        // Answers for the issuer that was asked for, but names another.
        let body =
            r#"{"issuer":"https://evil.example.com","jwks_uri":"https://evil.example.com/keys"}"#;
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else { return };
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });

        let err = fetch_jwks(&jwks_client().unwrap(), &issuer).await.unwrap_err().to_string();
        assert!(err.contains("evil.example.com"), "unhelpful error: {err}");
        assert!(err.contains(&issuer), "the error must name what was asked for: {err}");
    }

    #[tokio::test]
    async fn an_unreachable_provider_leaves_the_key_set_already_in_use() {
        // The property that lets a node restart while its identity provider is
        // down: the fetch fails, nothing is installed, and the node serves.
        let verifier = OidcVerifier::new(kimmy_auth::OidcSettings {
            allow_federated_admin: false,
            issuer: "http://127.0.0.1:1".into(),
            audience: "kimmydb".into(),
            roles_claim: "roles".into(),
            role_mappings: Vec::new(),
            require_at_jwt: false,
            subject_claim: None,
        })
        .unwrap();
        let federation = kimmy_api::Federation::new(verifier);
        federation.install_keys(serde_json::from_str(STUB_JWKS).unwrap());

        assert!(fetch_jwks(&jwks_client().unwrap(), "http://127.0.0.1:1").await.is_err());
        assert_eq!(federation.key_count(), 1, "a failed fetch must install nothing");
    }
}
