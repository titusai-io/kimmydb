//! Node lifecycle: startup, serving, and graceful shutdown.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use kimmy_auth::{TokenIssuer, UserStore};
use kimmy_storage::{Engine, RetentionPolicy};
use tracing::{info, warn};

use crate::config::Config;

/// Filename of the redb database inside the data directory.
const DATABASE_FILE: &str = "kimmy.redb";

/// How often the certificate files are checked for a change.
///
/// A constant rather than configuration: a renewal lands weeks before expiry,
/// so a minute is far inside any window that matters, and SIGHUP already covers
/// "now". See ADR-049.
const CERT_POLL_INTERVAL: Duration = Duration::from_secs(60);

pub async fn run(config: Config) -> Result<()> {
    std::fs::create_dir_all(&config.storage.data_dir).with_context(|| {
        format!("creating data directory {}", config.storage.data_dir.display())
    })?;

    // Node identity lives inside the database file rather than beside it, so
    // that copying or restoring the file carries the identity with it. That
    // matters because the id is the tiebreak half of every write's stamp — a
    // node that forgets it becomes a stranger to its own prior writes.
    let path = config.storage.data_dir.join(DATABASE_FILE);
    let engine = Arc::new(
        Engine::open(&path).with_context(|| format!("opening database {}", path.display()))?,
    );

    info!(node = %engine.node_id(), version = env!("CARGO_PKG_VERSION"), "starting kimmyd");
    info!("{}", config.summary());

    if config.auth.insecure_no_auth {
        warn!("authentication is DISABLED; every request runs with full privileges");
    } else {
        bootstrap_users(&engine, &config)?;
    }

    // With auth off, no token is ever verified, so a throwaway signing key is
    // correct rather than a placeholder that might be mistaken for a secret.
    // With auth on the fallback must never be reached: it is a constant that
    // ships in the source, so signing a real token with it would let anyone
    // forge one. `Config::validate` guarantees a configured secret whenever
    // auth is on; the assertion pins that guarantee to this line so a future
    // change to the validation cannot silently re-open the hole.
    debug_assert!(
        config.auth.insecure_no_auth || config.auth.jwt_secret.is_some(),
        "auth is enabled but no jwt_secret is set; Config::validate should have refused this"
    );
    let secret = config
        .auth
        .jwt_secret
        .clone()
        .unwrap_or_else(|| "insecure-no-auth-unused-signing-key".to_string());
    let tokens = TokenIssuer::new(&secret, config.auth.token_ttl_secs)
        .context("configuring the token issuer")?;

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

    let egress = kimmy_api::egress::EgressPolicy::new(config.webhooks.allowed_hosts.clone());
    let state = kimmy_api::state_with_egress(
        Arc::clone(&engine),
        tokens,
        config.auth.insecure_no_auth,
        limits,
        egress,
    )
    .context("building the API state")?;

    // MCP shares the state rather than being handed its own, so an agent tool
    // and the REST route beside it reach the same engine through the same
    // authorization check. See kimmy-mcp's crate documentation.
    let mut app = kimmy_api::router(Arc::clone(&state));
    if config.server.mcp {
        app = app.merge(kimmy_mcp::mcp_router(
            Arc::clone(&state),
            config.server.mcp_allowed_hosts.clone(),
        ));
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

    let gc_handle = spawn_collector(Arc::clone(&engine), &config);
    let cluster = spawn_cluster(Arc::clone(&engine), Arc::clone(&state), &config).await?;

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
        let egress = kimmy_api::egress::EgressPolicy::new(config.webhooks.allowed_hosts.clone());
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
        tokio::spawn(async move {
            kimmy_api::dispatch::run(state, egress, me, members, limits).await;
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
            Some(interval) => Some(tokio::spawn(async move {
                kimmy_api::expiry::run(state, me, members, interval).await;
            })),
        }
    };

    // Keeps each node's view of "is this token still good" honest. Another
    // ordinary oplog consumer, which is also what makes revoking on one node
    // take effect on every node: a replicated write to `__users` publishes on
    // the node that applied it (ADR-052).
    let sessions_handle =
        tokio::spawn(kimmy_api::sessions::invalidator(&engine, state.sessions.clone()));

    // The embedding worker is an ordinary change-stream subscriber, so it runs
    // alongside the server rather than inside the write path. A write returns
    // as soon as its oplog entry is durable; embedding catches up behind it.
    let worker_handle = tokio::spawn({
        let engine = Arc::clone(&engine);
        async move {
            let mut worker = kimmy_vector::EmbeddingWorker::new(engine);
            if let Err(e) = worker.run().await {
                warn!(error = %e, "embedding worker stopped");
            }
        }
    });

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

    serve(listener, app, tls).await.context("serving")?;

    // Nothing to drain: it holds no state beyond the mtimes it last saw, and
    // the certificate in use is already in the acceptor.
    if let Some(handle) = cert_reloader {
        handle.abort();
    }
    // Holds only a cache, which the next start rebuilds by reading.
    sessions_handle.abort();
    // The worker holds no locks and its position is durable, so aborting is
    // safe: whatever it had not finished is re-delivered on the next start.
    worker_handle.abort();
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

    info!("shutdown complete");
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
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
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
) -> Result<()> {
    let service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();

    let Some(tls) = tls else {
        axum::serve(listener, service).with_graceful_shutdown(shutdown_signal()).await?;
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
    tokio::spawn({
        let handle = handle.clone();
        async move {
            shutdown_signal().await;
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

    let serving = tokio::spawn(kimmy_cluster::serve(Arc::clone(&engine), listener, secret.clone()));

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
        cluster_tasks.push(tokio::spawn(kimmy_cluster::membership::run(
            socket,
            advertised(local),
            engine.node_id(),
            secret.clone(),
            live.clone(),
            feed,
        )));
        members = Some(live);
        announce = Some(tx);
    } else {
        warn!("SWIM membership is disabled; peers come from discovery alone");
    }

    let replicating = tokio::spawn(kimmy_cluster::replicate(
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
            // The replication loop is the only place a peer's version vector
            // exists, so lag is pushed from there into the gauge (ADR-046).
            on_lag: Some(std::sync::Arc::new(move |secs| {
                state.metrics.set_replication_lag_secs(secs);
            })),
        },
    ));
    cluster_tasks.push(replicating);

    info!(
        bind = %local,
        seeds = config.cluster.seeds.len(),
        membership = config.cluster.membership,
        "clustering enabled"
    );
    Ok(Cluster { tasks: cluster_tasks, members })
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
fn spawn_collector(engine: Arc<Engine>, config: &Config) -> Option<tokio::task::JoinHandle<()>> {
    if config.storage.gc_interval_secs == 0 {
        warn!("retention collection is disabled; the oplog and tombstones will grow without bound");
        return None;
    }

    let interval = Duration::from_secs(config.storage.gc_interval_secs);
    let policy = RetentionPolicy::new(
        config.storage.oplog_retention_secs,
        config.storage.tombstone_retention_secs,
    );

    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately, which would collect during startup
        // while the node is still opening for business. Skip it.
        ticker.tick().await;

        loop {
            ticker.tick().await;
            match engine.collect_garbage(policy) {
                // A failed pass is not fatal — the garbage is still there and
                // the next tick will find it — so it is logged and retried
                // rather than taking the node down.
                Err(e) => warn!(error = %e, "retention pass failed"),
                Ok(outcome) if outcome.is_empty() => {}
                Ok(outcome) => info!(
                    oplog = outcome.oplog_removed,
                    tombstones = outcome.tombstones_removed,
                    "collected expired records"
                ),
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

/// Resolve on SIGINT or SIGTERM. SIGTERM matters most — it is what Docker and
/// Kubernetes send, and ignoring it means a hard kill after the grace period.
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
            let _ = serve(listener, app, tls).await;
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
}
