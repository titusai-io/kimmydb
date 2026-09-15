//! Tracing subscriber setup, and the OpenTelemetry exporters behind it.
//!
//! # One registry, two consumers
//!
//! The `tracing` events this process emits go to a formatting layer as they
//! always have, and — when a collector is configured — to a second layer that
//! converts spans into OTLP. That is what lets `kimmy-storage`,
//! `kimmy-cluster` and `kimmy-vector` be instrumented with the plain `tracing`
//! they already had and no new dependency at all.
//!
//! # `kimmy::audit` never reaches the collector
//!
//! The audit target is filtered out of the OTLP layer and only that layer, so
//! an operator's `RUST_LOG` routing of it keeps working untouched. Audit
//! records carry principal names *and* collection names — exactly what
//! `telemetry.include_names` exists to gate — and a collector is a different
//! destination from a log file even when the same person runs both. See
//! ADR-068, and `the_otel_layer_refuses_audit_records_and_accepts_everything_else`
//! below, which is what stops this from being a promise nobody checks.
//!
//! # Why the exporter is blocking
//!
//! [`init`] runs **before** the tokio runtime is built — see `main.rs`, where
//! configuration is resolved and logging installed before a runtime exists so
//! that a misconfiguration is a one-line error rather than a panic in a worker
//! thread. An async exporter constructed there would have no reactor, so the
//! OTLP builder is fed the blocking `reqwest` client, which owns its own
//! threads (ADR-069).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_semantic_conventions::attribute as semconv;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::prelude::*;

use crate::config::{LogConfig, LogFormat, TelemetryConfig};

/// Keeps the exporters alive, and flushes them on the way out.
///
/// Held by `main` across `node::run` and dropped after it returns. A batch
/// processor buffers, so a process that exits without this drops whatever it
/// had not yet sent — which is precisely the last few seconds before a crash,
/// the part anybody debugging actually wants.
///
/// Empty when no collector is configured, which is the ordinary case: the guard
/// still exists so the call site has one shape.
#[must_use = "dropping the guard immediately shuts the exporters down again"]
pub struct TelemetryGuard {
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
}

impl TelemetryGuard {
    /// A guard for a node that exports nothing.
    fn inert() -> Self {
        Self { tracer: None, meter: None }
    }

    /// Report every process counter to the collector, from the atomics
    /// `/metrics` already renders.
    ///
    /// **Bridged, not duplicated** (ADR-070). Each instrument is *observable*:
    /// its callback reads [`kimmy_api::metrics::Metrics::snapshot`] at export
    /// time, so there is exactly one source of truth per counter and no way
    /// for a Prometheus dashboard and a trace backend to disagree about how
    /// many requests a node served.
    ///
    /// Called from `node::run` rather than from [`init`], because the counters
    /// live in the API state and the API state needs a database — which does
    /// not exist yet when the subscriber is installed. With no meter provider
    /// installed the global meter is a no-op and this registers nothing.
    pub fn bridge_metrics(state: &kimmy_api::SharedState) {
        let meter = opentelemetry::global::meter("kimmydb");

        // Weak, so the callbacks the global meter provider holds forever do
        // not keep the engine alive past shutdown. A dead weak reference simply
        // observes nothing, which is the honest reading of "the node is gone".
        let weak = Arc::downgrade(state);
        let snapshot = move || {
            let state = weak.upgrade()?;
            // The engine's readings, taken fresh for this export exactly as
            // the `/metrics` handler takes them for a scrape (ADR-142). A
            // reading that fails observes nothing this export rather than
            // zeros: a counter reported as 0 and then its true value is a
            // reset a collector will believe.
            match state.storage_readings() {
                Ok(readings) => Some(state.metrics.snapshot_with(&readings)),
                Err(e) => {
                    tracing::debug!(error = ?e, "metrics bridge: engine readings unavailable this export");
                    None
                }
            }
        };

        // `$description` is an `expr` rather than a `literal` so that a
        // family of instruments can build one description from a shared
        // stem with `concat!`; the name stays a literal, because the guard
        // that compares this file against `/metrics` reads those literals.
        macro_rules! observe {
            ($build:ident, $name:literal, $unit:literal, $description:expr, $field:ident) => {
                observe!($build, $name, $unit, $description, |s| s.$field)
            };
            // The same, for a series that is not a bare field of the snapshot
            // -- `embed_provider_errors{kind}` reads out of a fixed array, and
            // an index is not an `ident`. A series the macro cannot express is
            // a reason to extend the macro, not a reason to leave the series
            // off the bridge.
            ($build:ident, $name:literal, $unit:literal, $description:expr, |$s:ident| $value:expr) => {{
                let snapshot = snapshot.clone();
                let _ = meter
                    .$build($name)
                    .with_unit($unit)
                    .with_description($description)
                    .with_callback(move |observer| {
                        if let Some($s) = snapshot() {
                            observer.observe($value, &[]);
                        }
                    })
                    .build();
            }};
        }

        // The writer-hold histogram's two rows for one holder (ADR-159).
        // Two instruments per holder rather than one instrument with a
        // `holder` attribute, because that is how every other labelled
        // series on this bridge is carried and a dashboard reading both
        // surfaces should not have to learn a second convention. The
        // holder's slot is a constant here, so the array index the value
        // comes from and the name it is published under cannot drift apart:
        // both are written on the same line.
        macro_rules! held_by {
            ($seconds:literal, $holds:literal, $holder:expr, $what:literal) => {{
                observe!(
                    f64_observable_counter,
                    $seconds,
                    "s",
                    concat!("Seconds the storage writer was held by ", $what, "."),
                    |s| s.write_lock_held_us[$holder.slot()] as f64 / 1e6
                );
                observe!(
                    u64_observable_counter,
                    $holds,
                    "{hold}",
                    concat!("Times the storage writer was held by ", $what, "."),
                    |s| s.write_lock_holds[$holder.slot()]
                );
            }};
        }

        // The same names `/metrics` uses, minus the `_total` suffix Prometheus
        // adds to a counter: an OTLP counter called `kimmy_requests_total`
        // becomes `kimmy_requests_total_total` the moment a collector exports
        // it back to Prometheus.
        //
        // The engine's block first, as the scrape renders it (ADR-142). The
        // two gauges below cost a metadata scan per export, as they cost one
        // per scrape.
        observe!(
            u64_observable_gauge,
            "kimmy.databases",
            "{database}",
            "Number of databases.",
            databases
        );
        observe!(
            u64_observable_gauge,
            "kimmy.collections",
            "{collection}",
            "Number of collections across all databases.",
            collections
        );
        observe!(
            u64_observable_counter,
            "kimmy.unique_violations",
            "{violation}",
            "Unique constraints broken by merging replicated writes.",
            unique_violations
        );
        observe!(
            u64_observable_counter,
            "kimmy.commits",
            "{commit}",
            "Durable write transactions committed by the storage engine.",
            commits
        );
        observe!(
            u64_observable_counter,
            "kimmy.fsyncs",
            "{fsync}",
            "Times the disk was asked to make something durable: one per commit under durable, one per shared flush under coalesced.",
            fsyncs
        );
        observe!(
            u64_observable_counter,
            "kimmy.commits.grouped",
            "{commit}",
            "Commits made durable by a shared flush rather than their own fsync.",
            commits_grouped
        );
        // The wait for the single writer (ADR-151): the histogram itself is a
        // synchronous instrument and stays off the bridge with the latency
        // one (see `NOT_BRIDGED`); its two summaries are observable.
        observe!(
            u64_observable_counter,
            "kimmy.write_lock.wait_timeouts",
            "{write}",
            "Writes that gave up waiting for the storage writer inside server.request_timeout_secs; nothing was written and the client was told to retry.",
            write_lock_wait_timeouts
        );
        observe!(
            f64_observable_gauge,
            "kimmy.write_lock.held_seconds.max",
            "s",
            "The longest any one transaction has held the storage writer since start.",
            |s| s.write_lock_held_max_us as f64 / 1e6
        );
        // What held it, and for how long in total (ADR-159). The buckets of
        // `kimmy_write_lock_held_seconds` stay off the bridge with the other
        // two histograms (see `NOT_BRIDGED`); the attribution itself does
        // not, because it is the reading an operator acts on and a
        // collector is where the alerting lives. One pair per holder, in
        // `WriterHolder::ALL` order.
        use kimmy_storage::WriterHolder;
        held_by!(
            "kimmy.write_lock.held_seconds.write",
            "kimmy.write_lock.holds.write",
            WriterHolder::Write,
            "one document written by a client"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.bulk",
            "kimmy.write_lock.holds.bulk",
            WriterHolder::Bulk,
            "many documents in one transaction: a bulk insert, a chunk of a multi-document update, a scoped batch"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.ddl",
            "kimmy.write_lock.holds.ddl",
            WriterHolder::Ddl,
            "a schema change writing metadata alone"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.index_build",
            "kimmy.write_lock.holds.index_build",
            WriterHolder::IndexBuild,
            "an index build, which files every document of the collection in the transaction that creates it"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.drop",
            "kimmy.write_lock.holds.drop",
            WriterHolder::Drop,
            "the destructive half of a drop: one chunk of a collection's purge, or an index drop, which is still one transaction"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.replication",
            "kimmy.write_lock.holds.replication",
            WriterHolder::Replication,
            "applying a peer's entries"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.repair",
            "kimmy.write_lock.holds.repair",
            WriterHolder::Repair,
            "applying a page of a peer's snapshot to repair a divergence"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.retention",
            "kimmy.write_lock.holds.retention",
            WriterHolder::Retention,
            "the retention pass removing what its scans found"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.expiry",
            "kimmy.write_lock.holds.expiry",
            WriterHolder::Expiry,
            "a TTL index's delete"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.embedding",
            "kimmy.write_lock.holds.embedding",
            WriterHolder::Embedding,
            "the embedding worker writing vectors or checkpointing its position"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.durability",
            "kimmy.write_lock.holds.durability",
            WriterHolder::Durability,
            "the shared fsync of the coalescing barrier"
        );
        held_by!(
            "kimmy.write_lock.held_seconds.rewind",
            "kimmy.write_lock.holds.rewind",
            WriterHolder::Rewind,
            "a rewind to a point in time, which runs only in a process that never serves"
        );
        observe!(
            u64_observable_gauge,
            "kimmy.storage.bytes",
            "By",
            "Size of the database file on disk.",
            storage_bytes
        );
        observe!(
            u64_observable_gauge,
            "kimmy.vector.index_cache.bytes",
            "By",
            "Estimated bytes of HNSW graphs held in memory across vector collections.",
            vector_index_cache_bytes
        );
        // The process as the kernel sees it (ADR-147): the figure a container
        // limit is enforced against, which neither byte gauge above is. Zero
        // on a platform without /proc, as on `/metrics`.
        observe!(
            u64_observable_gauge,
            "kimmy.process.resident.bytes",
            "By",
            "Resident memory of this process as the kernel reports it; 0 where /proc is not available.",
            process_resident_bytes
        );
        observe!(
            u64_observable_gauge,
            "kimmy.process.resident.peak.bytes",
            "By",
            "The most resident memory this process has had at any moment since it started; 0 where /proc is not available.",
            process_resident_peak_bytes
        );
        observe!(
            u64_observable_gauge,
            "kimmy.up",
            "1",
            "Always 1; presence indicates the node is serving.",
            |_s| 1
        );
        observe!(u64_observable_gauge, "kimmy.uptime", "s", "Seconds serving.", uptime_secs);
        observe!(
            u64_observable_counter,
            "kimmy.requests",
            "{request}",
            "HTTP requests handled.",
            requests
        );
        observe!(
            u64_observable_counter,
            "kimmy.responses.2xx",
            "{response}",
            "HTTP responses with a 2xx status.",
            responses_2xx
        );
        observe!(
            u64_observable_counter,
            "kimmy.responses.4xx",
            "{response}",
            "HTTP responses with a 4xx status.",
            responses_4xx
        );
        observe!(
            u64_observable_counter,
            "kimmy.responses.5xx",
            "{response}",
            "HTTP responses with a 5xx status.",
            responses_5xx
        );
        observe!(
            u64_observable_counter,
            "kimmy.authz.denied",
            "{decision}",
            "Operations refused by RBAC.",
            authz_denied
        );
        observe!(
            u64_observable_counter,
            "kimmy.auth.failures",
            "{attempt}",
            "Rejected credentials and tokens.",
            auth_failures
        );
        observe!(
            u64_observable_counter,
            "kimmy.rate_limited",
            "{request}",
            "Requests refused by a rate limit.",
            rate_limited
        );
        observe!(
            u64_observable_counter,
            "kimmy.rate_limited.principal",
            "{request}",
            "Authenticated requests refused by the per-principal rate limit.",
            rate_limited_principal
        );
        observe!(u64_observable_counter, "kimmy.backups", "{backup}", "Backups served.", backups);
        // `kimmy_backup_duration_seconds` is a histogram and its buckets stay
        // on `/metrics`, as the other histograms' do (see `NOT_BRIDGED`); its
        // sum is observable and is carried here, and `kimmy.backups` above is
        // its count, so a collector has the mean (ADR-170).
        observe!(
            f64_observable_counter,
            "kimmy.backup.duration_seconds",
            "s",
            "Seconds spent producing backups: the walk of the whole store and its spill to disk. Divide by kimmy.backups for the mean.",
            |s| s.backup_duration_sum_us as f64 / 1e6
        );
        observe!(
            u64_observable_counter,
            "kimmy.ttl.expired",
            "{document}",
            "Documents deleted by a TTL index.",
            ttl_expired
        );
        observe!(
            u64_observable_counter,
            "kimmy.index.unkeyed",
            "{document}",
            "Documents stored under an index that could not key them, rechecked on every scan of that index instead.",
            index_unkeyed
        );
        observe!(
            u64_observable_counter,
            "kimmy.ttl.skipped",
            "{document}",
            "Expiry candidates refused because the document was refreshed first.",
            ttl_skipped
        );
        observe!(
            u64_observable_counter,
            "kimmy.webhook.delivered",
            "{batch}",
            "Webhook batches an endpoint accepted.",
            webhook_delivered
        );
        observe!(
            u64_observable_counter,
            "kimmy.webhook.failed",
            "{batch}",
            "Webhook batches an endpoint refused or did not answer.",
            webhook_failed
        );
        observe!(
            u64_observable_counter,
            "kimmy.webhook.events",
            "{event}",
            "Change events pushed to endpoints.",
            webhook_events
        );
        observe!(
            u64_observable_gauge,
            "kimmy.webhook.subscriptions.active",
            "{subscription}",
            "Active subscriptions, as this node sees the registry.",
            webhook_active
        );
        observe!(
            u64_observable_gauge,
            "kimmy.webhook.subscriptions.invalidated",
            "{subscription}",
            "Invalidated subscriptions, as this node sees the registry.",
            webhook_invalidated
        );
        observe!(
            u64_observable_gauge,
            "kimmy.webhook.backlog",
            "s",
            "Age of the oldest undelivered event, across subscriptions this node owns.",
            webhook_backlog_secs
        );
        observe!(
            u64_observable_gauge,
            "kimmy.cluster.members",
            "{node}",
            "Peers this node's SWIM membership considers alive.",
            cluster_members
        );
        observe!(
            u64_observable_gauge,
            "kimmy.replication.lag",
            "s",
            "Seconds since the newest peer entry applied locally where a peer holds newer, worst peer in the last round.",
            replication_lag_secs
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.failures",
            "{round}",
            "Anti-entropy rounds against a peer that failed, any cause.",
            sync_failures
        );
        observe!(
            u64_observable_gauge,
            "kimmy.sync.peers_backing_off",
            "{peer}",
            "Peers this node is currently backing off from after failed rounds.",
            sync_peers_backing_off
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.ddl_refused",
            "{change}",
            "Replicated schema changes this node could not apply to its own data and skipped.",
            sync_ddl_refused
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.ddl_declined",
            "{change}",
            "Replicated index drops this node declined as older than the index standing under the name here, and had not already recorded; a re-served replay is not counted.",
            sync_ddl_declined
        );
        observe!(
            u64_observable_counter,
            "kimmy.tls.reloads.ok",
            "{reload}",
            "Certificate reloads that succeeded.",
            tls_reloads_ok
        );
        observe!(
            u64_observable_counter,
            "kimmy.tls.reloads.failed",
            "{reload}",
            "Certificate reloads that failed, leaving the one in use serving.",
            tls_reloads_failed
        );
        observe!(
            u64_observable_counter,
            "kimmy.jwks.refresh.ok",
            "{refresh}",
            "OIDC signing-key refreshes that succeeded.",
            jwks_refresh_ok
        );
        observe!(
            u64_observable_counter,
            "kimmy.jwks.refresh.failed",
            "{refresh}",
            "OIDC signing-key refreshes that failed, leaving the key set in use verifying.",
            jwks_refresh_failed
        );

        // The divergence pair (ADR-133, ADR-135). The gauge alone is not
        // readable: a `0` is *checked and the peers agree* or *not checked at
        // all*, and the two counters are what tell those apart, so a collector
        // that received one without the others would be in the state the
        // operations guide tells an operator not to reason from. The labelled
        // Prometheus series becomes two instrument names, which is how every
        // other labelled series here is bridged.
        observe!(
            u64_observable_gauge,
            "kimmy.sync.divergent_collections",
            "{collection}",
            "Collections the cross-member divergence check currently has confirmed as divergent.",
            sync_divergent_collections
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.divergence_checks.ran",
            "{contact}",
            "Peer contacts whose round ran the cross-member divergence check.",
            sync_divergence_checks
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.divergence_checks.skipped",
            "{contact}",
            "Rounds that did not run the check: the pull was truncated by the batch cap, or the round failed.",
            sync_divergence_skips
        );
        // The count half's own pair, and the age of the gauge's reading
        // (ADR-145). Same argument as the pair above, one level further
        // down: `ran` rising says the check ran, and says nothing about
        // whether the half that compares a document count ever did against
        // a peer permanently behind; and a member whose rounds all fail
        // moves neither counter while the gauge holds its last value, which
        // the age is the one series to say. The age is computed when the
        // snapshot is taken, from the instant the loop last reported
        // (ADR-154), so this export and a `/metrics` scrape at the same
        // moment read the same number, and both keep rising through a tick
        // of the loop that never ends.
        observe!(
            u64_observable_counter,
            "kimmy.sync.divergence_count_probes.compared",
            "{contact}",
            "Checked peer contacts in which the count half of the divergence check compared the probed collection's document count against the peer's.",
            sync_divergence_count_compared
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.divergence_count_probes.deferred",
            "{contact}",
            "Checked peer contacts in which the count half was deferred because one member was behind the other and still catching up.",
            sync_divergence_count_deferred
        );
        observe!(
            u64_observable_gauge,
            "kimmy.sync.divergence_check_age",
            "s",
            "Seconds since the last peer contact in which the cross-member divergence check ran, computed at export; 0 before the first.",
            sync_divergence_check_age_secs
        );
        // What a batch left rather than took, and the repairs that follow
        // (ADR-148): the series that move for a hole the lag gauge reads 0
        // through.
        observe!(
            u64_observable_counter,
            "kimmy.sync.entries_skipped.unknown_collection",
            "{batch}",
            "Sync batches stopped at an entry for a collection this node does not hold; re-served every round until a snapshot brings the collection.",
            sync_entries_skipped_unknown_collection
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.entries_skipped.beyond_advertised",
            "{entry}",
            "Replicated entries left for a later window because they sat above the vector the peer advertised before serving it.",
            sync_entries_skipped_beyond_advertised
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.repair_rounds",
            "{round}",
            "Sync rounds spent repairing against a peer: re-serving its oplog from below this node's position, or pulling its snapshot.",
            sync_repair_rounds
        );

        // The embedding worker. Every one of these reads 0 on a node where the
        // worker is disabled, which is the distinction an operator is looking
        // for, and none of them reached a collector at all until now.
        observe!(
            u64_observable_counter,
            "kimmy.embed.documents",
            "{document}",
            "Documents embedded by this node's worker.",
            embed_documents_embedded
        );
        observe!(
            u64_observable_counter,
            "kimmy.embed.chunks",
            "{chunk}",
            "Chunks embedded by this node's worker.",
            embed_chunks_embedded
        );
        observe!(
            u64_observable_counter,
            "kimmy.embed.deferred",
            "{document}",
            "Documents whose embedding was deferred to a later pass.",
            embed_deferred
        );
        observe!(
            u64_observable_counter,
            "kimmy.embed.skipped_not_owned",
            "{document}",
            "Documents skipped because another member owns their embedding.",
            embed_skipped_not_owned
        );
        observe!(
            u64_observable_counter,
            "kimmy.embed.failures",
            "{document}",
            "Embedding attempts that failed.",
            embed_failures
        );
        observe!(
            u64_observable_counter,
            "kimmy.embed.provider.requests",
            "{request}",
            "Calls to an upstream embedding provider that were answered.",
            embed_provider_requests
        );
        observe!(
            u64_observable_counter,
            "kimmy.embed.provider.tokens",
            "{token}",
            "Input tokens billed by an upstream embedding provider.",
            embed_provider_tokens
        );

        // Provider calls that failed before a response, by what failed. Four
        // instrument names for the four `kind` values, in the order the
        // snapshot's array holds them.
        observe!(
            u64_observable_counter,
            "kimmy.embed.provider.errors.connect",
            "{error}",
            "Provider calls that failed to connect: DNS, TCP or TLS.",
            |s| s.embed_transport[0]
        );
        observe!(
            u64_observable_counter,
            "kimmy.embed.provider.errors.timeout",
            "{error}",
            "Provider calls that timed out before a response.",
            |s| s.embed_transport[1]
        );
        observe!(
            u64_observable_counter,
            "kimmy.embed.provider.errors.reset",
            "{error}",
            "Provider calls where the far side closed an open connection.",
            |s| s.embed_transport[2]
        );
        observe!(
            u64_observable_counter,
            "kimmy.embed.provider.errors.other",
            "{error}",
            "Provider calls that failed before a response for any other reason.",
            |s| s.embed_transport[3]
        );

        // Worst scheduling delay since this bridge last reported one. A gauge
        // in microseconds, bridged in its own unit rather than converted,
        // because the interesting values are well under a second and rounding
        // to seconds would report every one of them as 0.
        //
        // The one instrument here that does NOT go through `observe!`, because
        // it is the one series whose read *clears* what it read. `/metrics`
        // takes its own high-water mark on every scrape; this takes a second
        // mark fed by the same `fetch_max`. Reading through `snapshot()` like
        // everything else would mean never clearing it — and on a deployment
        // whose telemetry only leaves through a collector, nothing else ever
        // would either, so the gauge would climb to the worst stall ever seen
        // and stay there for the life of the process. A latched gauge cannot
        // answer the one question it exists for, which is whether a worker
        // thread is blocked *now*.
        let stall = {
            let weak = Arc::downgrade(state);
            move || weak.upgrade().map(|s| s.metrics.take_runtime_stall_otlp_us())
        };
        let _ = meter
            .u64_observable_gauge("kimmy.runtime.stall")
            .with_unit("us")
            .with_description(
                "Worst runtime scheduling delay observed since this bridge last reported one.",
            )
            .with_callback(move |observer| {
                if let Some(us) = stall() {
                    observer.observe(us, &[]);
                }
            })
            .build();
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // Errors are logged rather than propagated: this runs on the way out,
        // there is nothing left to abort, and a failed flush must not turn a
        // clean shutdown into a non-zero exit.
        if let Some(tracer) = self.tracer.take()
            && let Err(e) = tracer.shutdown()
        {
            tracing::warn!(error = %e, "could not flush buffered spans to the collector");
        }
        if let Some(meter) = self.meter.take()
            && let Err(e) = meter.shutdown()
        {
            tracing::warn!(error = %e, "could not flush buffered metrics to the collector");
        }
    }
}

/// Install the global tracing subscriber, and the exporters if any.
///
/// `RUST_LOG` takes precedence over the configured level when set, since that
/// is what an operator will reach for when debugging a running container.
///
/// `telemetry` is `Some` only when the process is going to serve. `check-config`
/// and `restore` pass `None`: neither serves, both exit in under a second, and
/// starting an exporter for them would mean a config check opens a connection
/// to production's collector.
pub fn init(cfg: &LogConfig, telemetry: Option<&TelemetryConfig>) -> Result<TelemetryGuard> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&cfg.level))
        .with_context(|| format!("invalid log filter {:?}", cfg.level))?;

    let registry = tracing_subscriber::registry().with(filter);

    // Boxed, because the two formats are different types and the OTel layer
    // below has to be built against *one* subscriber type. Building it inside
    // each arm instead would mean writing the exporter setup twice, and the
    // half that would drift is the audit filter.
    let fmt = match cfg.format {
        LogFormat::Pretty => tracing_subscriber::fmt::layer().with_target(true).boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer().json().with_target(true).boxed(),
    };

    let Some(telemetry) = telemetry.filter(|t| t.is_configured()) else {
        registry.with(fmt).init();
        return Ok(TelemetryGuard::inert());
    };

    let resource = resource(telemetry);
    let tracer_provider = tracer_provider(telemetry, resource.clone())?;
    let meter_provider = meter_provider(telemetry, resource)?;

    // Without this, `kimmy-api`'s extract and inject calls are silent no-ops:
    // the global propagator defaults to one that reads and writes nothing, so
    // every request would start its own trace and no webhook would carry one.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let otel = tracing_opentelemetry::layer()
        .with_tracer(tracer_provider.tracer("kimmydb"))
        // ADR-068, and attached to this layer alone: the log keeps receiving
        // everything, and an operator's `RUST_LOG` routing is untouched.
        .with_filter(filter_fn(exported));

    registry.with(fmt).with(otel).init();

    Ok(TelemetryGuard { tracer: Some(tracer_provider), meter: Some(meter_provider) })
}

/// Whether a callsite may reach the collector: **spans only, never the audit
/// target**.
///
/// # Spans, not events, and this is the load-bearing half
///
/// `tracing-opentelemetry` turns every log event that happens inside a span
/// into a **span event**, carrying that event's own fields with it. Nothing in
/// this codebase writes log lines with telemetry in mind, and they are full of
/// exactly what ADR-068 gates: `kimmy_storage` logs `db` and `collection` on
/// every DDL line, the dispatcher logs a webhook `url`, `kimmy-auth` logs
/// `user`. Found by reading what a live collector actually received — with the
/// audit filter alone in place, a `create_collection` span arrived with
/// `collection: "orders"` hanging off it as an event field, while
/// `include_names` was off and every span attribute was correctly empty.
///
/// Gating on names one field at a time is not a fix: it is an audit of every
/// `info!` in the workspace, redone whenever anyone adds one. Spans are the
/// surface this feature designed for privacy — bounded, reviewed, and named
/// from route templates — and events are the surface nobody designed for it, so
/// events do not go. Logs stay logs.
///
/// # The audit target, separately and unconditionally
///
/// Audit records are events, so the span rule already excludes them. Naming
/// them anyway is deliberate: the exclusion is a promise about *identities*
/// rather than a consequence of how one layer happens to be filtered, and if
/// auditing ever grew a span this must still refuse it.
fn exported(metadata: &tracing::Metadata<'_>) -> bool {
    metadata.is_span() && metadata.target() != kimmy_api::telemetry::AUDIT_TARGET
}

/// What every span and metric from this node is attributed to.
///
/// Version *and* commit, because the startup line at `node.rs` pairs them for
/// exactly this reason: during a rolling upgrade or an incident the question is
/// "which build is this exactly", and a version number alone does not answer it
/// between releases. A trace is read at precisely that moment.
fn resource(cfg: &TelemetryConfig) -> Resource {
    Resource::builder()
        .with_service_name(cfg.service_name.clone())
        .with_attributes([
            KeyValue::new(semconv::SERVICE_VERSION, kimmy_core::build::VERSION),
            KeyValue::new("service.commit", kimmy_core::build::COMMIT),
        ])
        .build()
}

/// `http/protobuf` or `http/json`, already validated.
fn protocol(cfg: &TelemetryConfig) -> Protocol {
    match cfg.protocol.as_str() {
        "http/json" => Protocol::HttpJson,
        // `Config::validate` refuses anything else, so this is the protobuf
        // arm rather than a fallback that could quietly change an operator's
        // encoding.
        _ => Protocol::HttpBinary,
    }
}

/// The signal's own URL.
///
/// The configured endpoint is a **base**, matching what
/// `OTEL_EXPORTER_OTLP_ENDPOINT` means everywhere else, so an operator can
/// paste the value their other services use. The exporter does *not* append the
/// path when the endpoint is set programmatically — it takes the URI verbatim —
/// so a base handed straight through would POST to the collector's root and be
/// answered with 404 forever.
fn signal_url(endpoint: &str, path: &str) -> String {
    format!("{}/{}", endpoint.trim_end_matches('/'), path)
}

fn tracer_provider(cfg: &TelemetryConfig, resource: Resource) -> Result<SdkTracerProvider> {
    let endpoint = cfg.endpoint.as_deref().context("no telemetry endpoint configured")?;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(signal_url(endpoint, "v1/traces"))
        .with_protocol(protocol(cfg))
        .with_timeout(Duration::from_secs(cfg.export_timeout_secs))
        .build()
        .context("building the OTLP span exporter")?;

    Ok(SdkTracerProvider::builder()
        // Batched, never simple. A simple processor exports inside the span's
        // own `end`, which would put a network round trip to the collector on
        // the request path — and make an unreachable collector into request
        // latency, which is the one thing tracing must never cost.
        .with_batch_exporter(exporter)
        // Parent-based, so a request arriving with a sampled `traceparent` is
        // recorded whatever the local ratio says. A ratio applied
        // independently per node produces traces with holes in them, which is
        // worse than fewer whole ones.
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(cfg.sample_ratio))))
        .with_resource(resource)
        .build())
}

fn meter_provider(cfg: &TelemetryConfig, resource: Resource) -> Result<SdkMeterProvider> {
    let endpoint = cfg.endpoint.as_deref().context("no telemetry endpoint configured")?;

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(signal_url(endpoint, "v1/metrics"))
        .with_protocol(protocol(cfg))
        .with_timeout(Duration::from_secs(cfg.export_timeout_secs))
        .build()
        .context("building the OTLP metric exporter")?;

    Ok(SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter).build())
        .with_resource(resource)
        .build())
}

/// Every `/metrics` series that is deliberately **not** on the OTLP bridge,
/// with the reason. Read by `every_metrics_series_reaches_the_bridge`, which
/// fails on a series that is neither bridged nor named here.
///
/// The list exists because the bridge drifted twelve series behind `/metrics`
/// without anyone deciding that it should — including the divergence pair,
/// which is the one an operations guide tells an operator to alert on. Nothing
/// compared the two surfaces, so nothing objected. OTLP support is a standing
/// requirement (ADR-070), so the default is bridged and an exception has to be
/// written down here to compile.
#[cfg(test)]
const NOT_BRIDGED: &[(&str, &str)] = &[
    (
        "kimmy_request_duration_seconds",
        "A histogram. Every instrument on this bridge is observable (async): a \
         callback reads the latest snapshot when the collector asks. OpenTelemetry \
         has no observable histogram — a histogram is recorded synchronously, at \
         the point each observation happens — so bridging this one means \
         instrumenting the request path rather than adding a callback here, which \
         is a different change with its own design (bucket boundaries against the \
         Prometheus ones, and what the collector should export back). Deliberately \
         left for that change.",
    ),
    (
        "kimmy_write_lock_wait_seconds",
        "A histogram, for the same reason as the latency one, and to be bridged by \
         the same change: it is recorded synchronously where a transaction takes \
         the writer (ADR-151). Its two summaries — the writes that gave up waiting, \
         and the longest hold — are observable and are on the bridge.",
    ),
    (
        "kimmy_backup_duration_seconds",
        "A histogram, and OpenTelemetry has no observable histogram, so its buckets \
         stay on /metrics for the same reason as the two above. Its sum is bridged \
         as kimmy.backup.duration_seconds and its count is kimmy.backups, so a \
         collector still has how many backups ran and how long they took in total \
         (ADR-170).",
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_metrics_series_reaches_the_bridge() {
        // The guard the bridge did not have. `/metrics` is the surface an
        // operator scrapes directly and OTLP is the one a collector reads, and
        // a series on the first and not the second is invisible to everything
        // downstream of the collector — which is where the alerting lives.
        //
        // Matching is by name, and it has to tolerate the convention this
        // module already uses: a labelled Prometheus series becomes one
        // instrument per label value (`kimmy_responses_total{class}` is
        // `kimmy.responses.2xx`, `.4xx`, `.5xx`), so a series counts as
        // bridged when some instrument name starts with its stem.
        // `include_str!` rather than reading the path at runtime: it resolves
        // at compile time, so the test does not depend on the working
        // directory a `cargo test` invocation happens to have. Every
        // instrument name is a `"kimmy.…"` literal, wherever on the line it
        // sits — some `observe!` calls are one-liners.
        let source = include_str!("logging.rs");
        let mut bridged: Vec<String> = Vec::new();
        for (i, _) in source.match_indices("\"kimmy.") {
            let rest = &source[i + 1..];
            let Some(end) = rest.find('"') else { continue };
            bridged.push(format!(
                "kimmy_{}",
                rest[..end].trim_start_matches("kimmy.").replace('.', "_")
            ));
        }
        bridged.sort();
        bridged.dedup();
        assert!(
            bridged.len() > 30,
            "found {} instruments; the scrape of this file broke",
            bridged.len()
        );

        // Where the OTLP name is not a prefix transform of the Prometheus one.
        // Both of these predate the guard and are bridged correctly; the names
        // simply do not line up textually, and renaming a published instrument
        // to please a test would be the wrong way round.
        let aliases = [("kimmy_webhook_deliveries", "kimmy_webhook_delivered")];

        // The whole page, engine block included: the nine series that block
        // holds were on `/metrics` and off the bridge for as long as the
        // guard rendered the process counters alone (ADR-142).
        let rendered = kimmy_api::metrics::Metrics::default()
            .render_with(&kimmy_api::metrics::StorageReadings::default());

        // An exception has to carry a reason, or the list becomes the place a
        // series goes to stop being asked about — which is the failure this
        // whole test exists to prevent, rebuilt one level up.
        for (name, why) in NOT_BRIDGED {
            assert!(
                why.trim().len() > 40,
                "`{name}` is in NOT_BRIDGED with no real reason. An exception that does not say \
                 why is indistinguishable from an oversight, which is how the twelve got there"
            );
            // And it has to name a series that still exists. An exception that
            // outlives the series it excused is a permission nobody granted.
            assert!(
                rendered.lines().any(|l| l
                    .strip_prefix("# TYPE ")
                    .is_some_and(|r| { r.split_whitespace().next() == Some(name) })),
                "NOT_BRIDGED names `{name}`, which /metrics no longer exposes. Remove the entry"
            );
        }

        let mut unbridged = Vec::new();
        for line in rendered.lines() {
            let Some(rest) = line.strip_prefix("# TYPE ") else { continue };
            let series = rest.split_whitespace().next().expect("a series name");
            let stem = series.strip_suffix("_total").unwrap_or(series);
            // `_seconds` is a Prometheus unit suffix; the bridge carries the
            // unit in the instrument's own `with_unit`, so the names differ.
            let stem = stem.strip_suffix("_seconds").unwrap_or(stem);
            let stem = aliases.iter().find(|(from, _)| *from == stem).map_or(stem, |(_, to)| *to);
            if bridged.iter().any(|b| b == stem || b.starts_with(&format!("{stem}_"))) {
                continue;
            }
            if NOT_BRIDGED.iter().any(|(name, _)| *name == series) {
                continue;
            }
            unbridged.push(series.to_string());
        }
        assert!(
            unbridged.is_empty(),
            "these /metrics series reach no OTLP instrument: {unbridged:?}. Add an `observe!` \
             for each in `install`, or, if one genuinely cannot be bridged, add it to \
             NOT_BRIDGED with the reason. OTLP is a standing requirement, so silence is not \
             the default"
        );
    }

    /// A `Metadata` for a callsite that does not exist, so the filter can be
    /// asked about targets and kinds no test would otherwise produce.
    ///
    /// `tracing::Metadata::new` is public and this constructs nothing that is
    /// ever registered or emitted — it is an argument to a pure predicate.
    fn meta(target: &'static str, kind: tracing::metadata::Kind) -> tracing::Metadata<'static> {
        tracing::Metadata::new(
            "callsite",
            target,
            tracing::Level::INFO,
            None,
            None,
            None,
            tracing::field::FieldSet::new(&[], tracing::callsite::Identifier(&TEST_CALLSITE)),
            kind,
        )
    }

    struct TestCallsite;
    impl tracing::callsite::Callsite for TestCallsite {
        fn set_interest(&self, _: tracing::subscriber::Interest) {}
        fn metadata(&self) -> &tracing::Metadata<'_> {
            unreachable!("never registered; only its identifier is used")
        }
    }
    static TEST_CALLSITE: TestCallsite = TestCallsite;

    #[test]
    fn only_spans_are_exported_and_never_the_audit_target() {
        // ADR-068's promise, as a check rather than a sentence, in both
        // directions — a filter that excluded too much would silently stop
        // exporting the spans the feature exists for.
        let span = |target| exported(&meta(target, tracing::metadata::Kind::SPAN));
        let event = |target| exported(&meta(target, tracing::metadata::Kind::EVENT));

        // Spans go, from every crate that has them.
        for target in [
            "kimmy_api::exec",
            "kimmy_api::routes",
            "kimmy_api::dispatch",
            "kimmy_storage::engine",
            "kimmy_cluster::peers",
            "kimmy_vector::worker",
            "kimmyd::node",
        ] {
            assert!(span(target), "{target} spans must be exported");
        }

        // **Events never do.** `tracing-opentelemetry` would otherwise attach
        // each one to its enclosing span *with its own fields*, and this
        // codebase's log lines carry `db`, `collection`, `user` and webhook
        // `url`s — the exact things `include_names` gates. Caught against a
        // live collector: a `create_collection` span arrived carrying
        // `collection: "orders"` as an event field while every span attribute
        // was correctly empty.
        for target in ["kimmy_storage::engine", "kimmy_api::dispatch", "kimmy_auth::users"] {
            assert!(!event(target), "{target} events must not be exported");
        }

        // And the audit target is refused as both, unconditionally. The span
        // rule already covers today's records — they are events — but the
        // promise is about identities, not about how one layer is filtered.
        assert!(!event(kimmy_api::telemetry::AUDIT_TARGET));
        assert!(!span(kimmy_api::telemetry::AUDIT_TARGET));

        // Near misses, because a `starts_with` or a `contains` would catch
        // these too and quietly stop exporting real spans.
        for target in ["kimmy::auditor", "kimmy_audit", "audit"] {
            assert!(span(target), "{target} must still be exported");
        }
    }

    #[test]
    fn the_signal_path_is_appended_to_the_base_exactly_once() {
        // The exporter takes a programmatic endpoint verbatim, so getting this
        // wrong is a node that POSTs to the collector's root and is answered
        // 404 forever — while serving perfectly, which is why it would not be
        // noticed.
        assert_eq!(
            signal_url("http://otel-collector:4318", "v1/traces"),
            "http://otel-collector:4318/v1/traces"
        );
        // A trailing slash must not double one.
        assert_eq!(
            signal_url("http://otel-collector:4318/", "v1/metrics"),
            "http://otel-collector:4318/v1/metrics"
        );
        // A collector behind a path prefix keeps it, for the same reason
        // OIDC discovery is appended rather than URL-joined.
        assert_eq!(signal_url("http://gateway/otlp", "v1/traces"), "http://gateway/otlp/v1/traces");
    }

    #[test]
    fn the_default_configuration_installs_no_exporter() {
        // Telemetry off is the default, and "off" has to mean no exporter and
        // no connection rather than an exporter pointed at nothing. Checked
        // through the guard rather than through `init`, which installs a
        // process-global subscriber and so can only run once per test binary.
        let cfg = TelemetryConfig::default();
        assert!(!cfg.is_configured());

        let guard = TelemetryGuard::inert();
        assert!(guard.tracer.is_none(), "no span exporter");
        assert!(guard.meter.is_none(), "no metric exporter");
    }

    #[test]
    fn the_resource_carries_the_version_and_the_commit_this_binary_was_built_from() {
        // Paired for the same reason the startup log pairs them: during a
        // rolling upgrade the question is which build this exactly is, and a
        // version number alone does not answer it between releases.
        let cfg = TelemetryConfig { service_name: "kimmydb-test".into(), ..Default::default() };
        let resource = resource(&cfg);

        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str(semconv::SERVICE_NAME)),
            Some(opentelemetry::Value::from("kimmydb-test"))
        );
        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str(semconv::SERVICE_VERSION)),
            Some(opentelemetry::Value::from(kimmy_core::build::VERSION))
        );
        assert_eq!(
            resource.get(&opentelemetry::Key::from_static_str("service.commit")),
            Some(opentelemetry::Value::from(kimmy_core::build::COMMIT))
        );
    }

    #[test]
    fn the_configured_protocol_decides_the_encoding() {
        let mut cfg = TelemetryConfig::default();
        assert!(matches!(protocol(&cfg), Protocol::HttpBinary));
        cfg.protocol = "http/json".into();
        assert!(matches!(protocol(&cfg), Protocol::HttpJson));
    }
}
