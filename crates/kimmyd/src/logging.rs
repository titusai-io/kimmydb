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
        // What each holder's holds were made of (ADR-176): every column of
        // `kimmy_write_lock_held_{component,phase}_seconds_total`,
        // `_io_bytes_total`, `_write_estimated_seconds_total` and
        // `_overcounted_total`, one instrument per holder and label value, as
        // the holders above are carried. Built in a loop, because twelve
        // holders by twelve columns is a table rather than a list; the table is
        // `hold_instrument_table`, which a test holds to the `/metrics` labels
        // and, through this same call, to every holder.
        register_hold_instruments(&meter, snapshot.clone());
        observe!(
            u64_observable_counter,
            "kimmy.write_lock.held_cpu_unmeasured",
            "{hold}",
            "Holds of the storage writer whose thread CPU time could not be read, and so are not in the cpu and off_cpu components.",
            |s| s.write_lock_hold.cpu_unmeasured
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
        // One instrument per supervised task rather than one carrying a `task`
        // attribute, for the reason the holder family above gives: that is how
        // every labelled series on this bridge is carried, and a dashboard
        // reading both surfaces should not have to learn a second convention.
        //
        // The name and the task it reads are written on the same line, so they
        // cannot drift apart; that they match `kimmy_task::TASKS` is held by
        // `every_supervised_name_is_in_the_task_list_and_every_entry_is_used`,
        // which reads this file too -- otherwise the bridge could fall behind
        // the task list without anything saying so.
        macro_rules! task_retries {
            ($name:literal, $task:literal) => {{
                let _ = meter
                    .u64_observable_counter($name)
                    .with_unit("{retry}")
                    .with_description(concat!(
                        "Times the ",
                        $task,
                        " task retried its work in place after a transient failure. A count that \
                         keeps rising while its work does not progress is a task retrying \
                         something permanent."
                    ))
                    .with_callback(move |observer| {
                        let n = kimmy_task::retries()
                            .into_iter()
                            .find(|(task, _)| *task == $task)
                            .map_or(0, |(_, n)| n);
                        observer.observe(n, &[]);
                    })
                    .build();
            }};
        }
        task_retries!("kimmy.task.retries.cert_reloader", "cert_reloader");
        task_retries!("kimmy.task.retries.embedding_worker", "embedding_worker");
        task_retries!("kimmy.task.retries.jwks_refresher", "jwks_refresher");
        task_retries!("kimmy.task.retries.membership", "membership");
        task_retries!("kimmy.task.retries.membership_announce", "membership_announce");
        task_retries!("kimmy.task.retries.membership_inbound", "membership_inbound");
        task_retries!("kimmy.task.retries.membership_timer", "membership_timer");
        task_retries!("kimmy.task.retries.replication", "replication");
        task_retries!("kimmy.task.retries.replication_server", "replication_server");
        task_retries!("kimmy.task.retries.retention_collector", "retention_collector");
        task_retries!("kimmy.task.retries.session_invalidator", "session_invalidator");
        task_retries!("kimmy.task.retries.stall_probe", "stall_probe");
        task_retries!("kimmy.task.retries.ttl_expiry", "ttl_expiry");
        task_retries!("kimmy.task.retries.vector_index_invalidator", "vector_index_invalidator");
        task_retries!("kimmy.task.retries.webhook_dispatcher", "webhook_dispatcher");

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
            "kimmy.index.undecidable",
            "{document}",
            "Documents an index holds because its partial filter could not decide them; every \
             scan re-checks them (ADR-185).",
            index_undecidable
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
            "kimmy.ttl.skipped_filter",
            "{document}",
            "Expiry candidates not deleted because the TTL index's partial filter, evaluated as find evaluates it, no longer selected the document.",
            ttl_skipped_filter
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
        // To the millisecond since ADR-175, so a floating-point gauge where it
        // was an integer one: whole seconds could not read an effect of a few.
        observe!(
            f64_observable_gauge,
            "kimmy.replication.lag",
            "s",
            "Seconds since the newest peer entry applied locally where a peer holds newer, worst peer in the last round, to the millisecond. Reads 0 once a round's pull reached the vector the peer advertised, including while entries written since wait for the next round.",
            |s| s.replication_lag_ms as f64 / 1e3
        );
        // Where sync pulls spend their time (ADR-175). The two histograms'
        // buckets stay on `/metrics` (see `NOT_BRIDGED`); each phase's sum and
        // the pull count are observable, and are what a collector divides for
        // the mean, one instrument per phase as ADR-159 carries the holders.
        // The phases share one count: every pull observes all three.
        observe!(
            f64_observable_counter,
            "kimmy.sync.pull_seconds.serve",
            "s",
            "Seconds sync pulls spent from asking a peer for a window to holding it: the peer's walk of its oplog and the wire.",
            |s| s.sync_pulls.serve.sum_us as f64 / 1e6
        );
        observe!(
            f64_observable_counter,
            "kimmy.sync.pull_seconds.wait",
            "s",
            "Seconds sync pulls spent applying their window waiting for this node's single writer.",
            |s| s.sync_pulls.wait.sum_us as f64 / 1e6
        );
        observe!(
            f64_observable_counter,
            "kimmy.sync.pull_seconds.apply",
            "s",
            "Seconds sync pulls spent applying their window, less the wait for the writer: the entries' work, the commits and their fsync.",
            |s| s.sync_pulls.apply.sum_us as f64 / 1e6
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.pulls",
            "{pull}",
            "Sync pulls of a peer's oplog: the count of each kimmy.sync.pull_seconds phase.",
            |s| s.sync_pulls.serve.count
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.pulled_entries",
            "{entry}",
            "Entries sync pulls carried from peers. Against kimmy.sync.pull_seconds.apply, the cost of applying one.",
            |s| s.sync_pulls.entries
        );
        observe!(
            f64_observable_counter,
            "kimmy.sync.entry_wait_seconds",
            "s",
            "Seconds the oldest entry each sync pull carried that this node lacked had waited when the pull arrived. Divide by kimmy.sync.entry_waits for the mean.",
            |s| s.sync_pulls.entry_wait.sum_us as f64 / 1e6
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.entry_waits",
            "{pull}",
            "Sync pulls whose oldest lacked entry's wait was observed in kimmy.sync.entry_wait_seconds.",
            |s| s.sync_pulls.entry_wait.count
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.entry_wait_ahead",
            "{pull}",
            "Sync pulls whose oldest lacked entry was stamped ahead of this node's clock, so its wait could not be observed: clock skew between members.",
            |s| s.sync_pulls.entry_wait_ahead
        );
        // How each contact ended, one instrument per label value, in
        // `ContactEnd::ALL` order.
        use kimmy_cluster::ContactEnd;
        observe!(
            u64_observable_counter,
            "kimmy.sync.contacts.caught_up",
            "{contact}",
            "Sync contacts whose last pull did not come back truncated.",
            |s| s.sync_pulls.contacts[ContactEnd::CaughtUp.slot()]
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.contacts.budget",
            "{contact}",
            "Sync contacts that ended truncated because the next pull would not fit in the tick: a backlog carried into the next tick.",
            |s| s.sync_pulls.contacts[ContactEnd::Budget.slot()]
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.contacts.ceiling",
            "{contact}",
            "Sync contacts that ended truncated with time left, at the most pulls one contact may make.",
            |s| s.sync_pulls.contacts[ContactEnd::Ceiling.slot()]
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.contacts.failed",
            "{contact}",
            "Sync contacts ended by a pull that failed.",
            |s| s.sync_pulls.contacts[ContactEnd::Failed.slot()]
        );
        // What serving peers' windows cost this node (ADR-176). The walk
        // histogram's buckets stay on `/metrics` (see `NOT_BRIDGED`); its sum
        // is here and its count is the windows served.
        observe!(
            u64_observable_counter,
            "kimmy.sync.served_windows",
            "{window}",
            "Windows of this node's oplog walked for pulling peers; also the count of kimmy.sync.serve_walk_seconds.",
            |s| s.sync_serve.windows
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.served_entries",
            "{entry}",
            "Entries the windows this node served to peers carried.",
            |s| s.sync_serve.entries
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.serve_passed_entries",
            "{entry}",
            "Entries the walks behind served windows examined and did not serve, mostly because the peer already held them.",
            |s| s.sync_serve.passed
        );
        observe!(
            f64_observable_counter,
            "kimmy.sync.serve_walk_seconds",
            "s",
            "Seconds this node spent walking its oplog for windows served to peers, the wire not included.",
            |s| s.sync_serve.walk_sum_us as f64 / 1e6
        );
        observe!(
            f64_observable_counter,
            "kimmy.sync.serve_walk_read_seconds",
            "s",
            "Seconds the walks behind served windows spent reading pages of the storage file its cache did not hold.",
            |s| s.sync_serve.read_ns as f64 / 1e9
        );
        observe!(
            u64_observable_counter,
            "kimmy.sync.serve_walk_read_bytes",
            "By",
            "Bytes the walks behind served windows read from the storage file.",
            |s| s.sync_serve.read_bytes
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
            "kimmy.sync.ddl_relogged",
            "{change}",
            "Schema changes a snapshot restore appended to this node's oplog so that it can serve them onward.",
            sync_ddl_relogged
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
        // The release that beyond_advertised cannot tell from the race it has
        // always counted (ADR-169's addendum).
        observe!(
            u64_observable_counter,
            "kimmy.sync.held_marks_released",
            "{entry}",
            "Entries held as state that arrived in a sync window contiguous from this node's position and were released; the release path itself, which beyond_advertised cannot tell from the ordinary race.",
            sync_held_marks_released
        );
        // Whether marks are held at all, which the counter above moves only
        // when one goes (ADR-160, ADR-172).
        observe!(
            u64_observable_gauge,
            "kimmy.sync.held_marks",
            "{entry}",
            "Entries this node holds as state rather than history, waiting to arrive in a sync window contiguous from its position; read at export.",
            sync_held_marks
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
            "kimmy.embed.skipped_no_shadow",
            "{document}",
            "Documents and scans skipped because a collection configured for vectors has no shadow collection here.",
            embed_skipped_no_shadow
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
        "kimmy_sync_pull_seconds",
        "A histogram, and OpenTelemetry has no observable histogram, so its buckets \
         stay on /metrics for the same reason as the ones above. Each phase's sum is \
         bridged as kimmy.sync.pull_seconds.serve, .wait and .apply, and the pull \
         count they share as kimmy.sync.pulls, so a collector still has where the \
         pulls' time went (ADR-175).",
    ),
    (
        "kimmy_sync_entry_wait_seconds",
        "A histogram, for the same reason. Its sum is bridged as \
         kimmy.sync.entry_wait_seconds and its count as kimmy.sync.entry_waits, and \
         the pulls it could not observe as kimmy.sync.entry_wait_ahead (ADR-175).",
    ),
    (
        "kimmy_sync_serve_walk_seconds",
        "A histogram, for the same reason as the ones above. Its sum is bridged as \
         kimmy.sync.serve_walk_seconds and its count is kimmy.sync.served_windows \
         (ADR-176).",
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

/// Register every instrument of the hold decomposition (ADR-176), and say how
/// many were built: the whole of `hold_instrument_table`, which a test holds
/// to twelve per holder by calling this very function.
fn register_hold_instruments<F>(meter: &opentelemetry::metrics::Meter, snapshot: F) -> usize
where
    F: Fn() -> Option<kimmy_api::metrics::MetricsSnapshot> + Clone + Send + Sync + 'static,
{
    let mut built = 0;
    for (holder, name, unit, description, read) in hold_instrument_table() {
        let row = holder.slot();
        let snapshot = snapshot.clone();
        if unit == "s" {
            let _ = meter
                .f64_observable_counter(name)
                .with_unit(unit)
                .with_description(description)
                .with_callback(move |observer| {
                    if let Some(s) = snapshot() {
                        observer.observe(read(&s.write_lock_hold, row) as f64 / 1e9, &[]);
                    }
                })
                .build();
        } else {
            let _ = meter
                .u64_observable_counter(name)
                .with_unit(unit)
                .with_description(description)
                .with_callback(move |observer| {
                    if let Some(s) = snapshot() {
                        observer.observe(read(&s.write_lock_hold, row), &[]);
                    }
                })
                .build();
        }
        built += 1;
    }
    built
}

/// Every holder's instruments, in `WriterHolder::ALL` order.
fn hold_instrument_table()
-> Vec<(kimmy_storage::WriterHolder, String, &'static str, String, HoldColumn)> {
    kimmy_storage::WriterHolder::ALL
        .into_iter()
        .flat_map(|holder| {
            hold_instruments(holder)
                .into_iter()
                .map(move |(name, unit, description, read)| (holder, name, unit, description, read))
        })
        .collect()
}

/// What reads one column of a holder's row of the hold decomposition.
type HoldColumn = fn(&kimmy_storage::HoldDecomposition, usize) -> u64;

/// The bridge's instruments for one holder's row of the hold decomposition
/// (ADR-176): name, unit, description, and what reads the value. Seconds are
/// read in nanoseconds and published as seconds; the rest are published as
/// read.
fn hold_instruments(
    holder: kimmy_storage::WriterHolder,
) -> Vec<(String, &'static str, String, HoldColumn)> {
    use kimmy_storage::{HoldComponent, HoldPhase};
    let label = holder.label();
    let mut out: Vec<(String, &'static str, String, HoldColumn)> = Vec::new();
    for component in HoldComponent::ALL {
        let read: HoldColumn = match component {
            HoldComponent::Read => |d, row| d.component_ns[row][HoldComponent::Read.slot()],
            HoldComponent::Write => |d, row| d.component_ns[row][HoldComponent::Write.slot()],
            HoldComponent::Sync => |d, row| d.component_ns[row][HoldComponent::Sync.slot()],
            HoldComponent::Cpu => |d, row| d.component_ns[row][HoldComponent::Cpu.slot()],
            HoldComponent::OffCpu => |d, row| d.component_ns[row][HoldComponent::OffCpu.slot()],
        };
        out.push((
            format!("kimmy.write_lock.held_component.{label}.{}", component.label()),
            "s",
            format!(
                "Seconds holds of the storage writer by {label} spent as {}: see kimmy_write_lock_held_component_seconds_total.",
                component.label()
            ),
            read,
        ));
    }
    for phase in HoldPhase::ALL {
        let read: HoldColumn = match phase {
            HoldPhase::Work => |d, row| d.phase_ns[row][HoldPhase::Work.slot()],
            HoldPhase::Counts => |d, row| d.phase_ns[row][HoldPhase::Counts.slot()],
            HoldPhase::Commit => |d, row| d.phase_ns[row][HoldPhase::Commit.slot()],
        };
        out.push((
            format!("kimmy.write_lock.held_phase.{label}.{}", phase.label()),
            "s",
            format!(
                "Seconds holds of the storage writer by {label} spent in their {} phase: see kimmy_write_lock_held_phase_seconds_total.",
                phase.label()
            ),
            read,
        ));
    }
    out.push((
        format!("kimmy.write_lock.held_io_bytes.{label}.read"),
        "By",
        format!("Bytes holds of the storage writer by {label} read from the storage file."),
        |d, row| d.read_bytes[row],
    ));
    out.push((
        format!("kimmy.write_lock.held_io_bytes.{label}.write"),
        "By",
        format!("Bytes holds of the storage writer by {label} wrote to the storage file."),
        |d, row| d.write_bytes[row],
    ));
    out.push((
        format!("kimmy.write_lock.held_write_estimated.{label}"),
        "s",
        format!(
            "Seconds of page writes in holds by {label} whose CPU time was estimated: the most cpu and off_cpu can be misattributed by."
        ),
        |d, row| d.write_estimated_ns[row],
    ));
    out.push((
        format!("kimmy.write_lock.held_overcounted.{label}"),
        "{hold}",
        format!("Holds of the storage writer by {label} whose measured components came to more than the hold."),
        |d, row| d.overcounted[row],
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_column_of_the_hold_decomposition_reaches_the_bridge_under_its_own_label() {
        // The coverage test below matches a series by its stem, and these
        // instruments are named in a loop, so it would pass with a column
        // missing or two columns read from the same slot. Each instrument is
        // held here to the `/metrics` sample of the same holder and label,
        // read from a decomposition whose every value differs (ADR-176).
        let d = kimmy_storage::HoldDecomposition {
            component_ns: std::array::from_fn(|h| {
                std::array::from_fn(|c| (h * 100 + c + 1) as u64)
            }),
            phase_ns: std::array::from_fn(|h| std::array::from_fn(|p| (h * 100 + p + 11) as u64)),
            read_bytes: std::array::from_fn(|h| (h * 100 + 21) as u64),
            write_bytes: std::array::from_fn(|h| (h * 100 + 22) as u64),
            write_estimated_ns: std::array::from_fn(|h| (h * 100 + 23) as u64),
            overcounted: std::array::from_fn(|h| (h * 100 + 24) as u64),
            cpu_unmeasured: 0,
        };
        let mut names = std::collections::HashSet::new();
        for holder in kimmy_storage::WriterHolder::ALL {
            let row = holder.slot();
            let instruments = hold_instruments(holder);
            assert_eq!(instruments.len(), 12, "five components, three phases, two io, two more");
            let mut values = std::collections::HashSet::new();
            for (name, _, _, read) in &instruments {
                assert!(names.insert(name.clone()), "{name} is published twice");
                assert!(values.insert(read(&d, row)), "{name} reads a column another reads");
                let parts: Vec<&str> = name.split('.').collect();
                assert_eq!(parts[3], holder.label(), "{name} is not named for its holder");
                let expected = match parts[2] {
                    "held_component" => {
                        let c = kimmy_storage::HoldComponent::ALL
                            .into_iter()
                            .find(|c| c.label() == parts[4])
                            .unwrap();
                        d.component_ns[row][c.slot()]
                    }
                    "held_phase" => {
                        let p = kimmy_storage::HoldPhase::ALL
                            .into_iter()
                            .find(|p| p.label() == parts[4])
                            .unwrap();
                        d.phase_ns[row][p.slot()]
                    }
                    "held_io_bytes" if parts[4] == "read" => d.read_bytes[row],
                    "held_io_bytes" => d.write_bytes[row],
                    "held_write_estimated" => d.write_estimated_ns[row],
                    "held_overcounted" => d.overcounted[row],
                    other => panic!("unexpected instrument family {other}"),
                };
                assert_eq!(read(&d, row), expected, "{name} reads the wrong column");
            }
        }
        // The serve series, named one by one for the same reason ADR-175's
        // are: their stems overlap, so one would satisfy the match for all.
        let source = include_str!("logging.rs");
        let source = &source[..source.find("#[cfg(test)]\nmod tests").expect("the tests")];
        // And the whole table is registered, every holder of it: counted by
        // the function the bridge calls, against a meter that exports nothing.
        let meter = opentelemetry::global::meter("hold-instruments-test");
        assert_eq!(
            register_hold_instruments(&meter, || None),
            kimmy_storage::WriterHolder::COUNT * 12,
            "twelve instruments for each of the twelve holders"
        );
        for holder in kimmy_storage::WriterHolder::ALL {
            assert_eq!(
                hold_instrument_table().iter().filter(|(h, ..)| *h == holder).count(),
                12,
                "{holder:?} is missing from the table"
            );
        }
        let bridge = &source[source.find("pub fn bridge_metrics").expect("the bridge")
            ..source
                .find("/// Register every instrument of the hold decomposition")
                .expect("the helper")];
        assert_eq!(
            bridge.matches("register_hold_instruments(&meter, snapshot.clone());").count(),
            1,
            "the bridge does not register the hold decomposition"
        );
        // ADR-159's per-holder hold series, named one by one: the coverage
        // test's stem for `kimmy_write_lock_held_seconds` is
        // `kimmy_write_lock_held`, which every instrument above extends, so
        // it would pass with all twenty-four of these gone.
        for holder in kimmy_storage::WriterHolder::ALL {
            for name in [
                format!("kimmy.write_lock.held_seconds.{}", holder.label()),
                format!("kimmy.write_lock.holds.{}", holder.label()),
            ] {
                assert_eq!(
                    source.matches(&format!("\"{name}\",")).count(),
                    1,
                    "`{name}` is not an instrument on the bridge"
                );
            }
        }
        for name in [
            "kimmy.write_lock.held_cpu_unmeasured",
            "kimmy.sync.served_windows",
            "kimmy.sync.served_entries",
            "kimmy.sync.serve_passed_entries",
            "kimmy.sync.serve_walk_seconds",
            "kimmy.sync.serve_walk_read_seconds",
            "kimmy.sync.serve_walk_read_bytes",
        ] {
            assert_eq!(
                source.matches(&format!("\"{name}\",")).count(),
                1,
                "`{name}` is not an instrument on the bridge"
            );
        }
    }

    #[test]
    fn the_sync_pull_instruments_the_stem_match_cannot_see_reach_the_bridge() {
        // The coverage test below matches a series by the stem of its name,
        // and both ADR-175 histograms are in `NOT_BRIDGED` for their buckets,
        // so it would pass with their sums and counts gone from the bridge.
        // Those summaries are the only form of either histogram a collector
        // sees, so they are named here one by one.
        // Everything above the first test-only item: the instruments, and not
        // this list of their names.
        let source = include_str!("logging.rs");
        let source = &source[..source.find("#[cfg(test)]").expect("a test-only item")];
        for name in [
            "kimmy.sync.pull_seconds.serve",
            "kimmy.sync.pull_seconds.wait",
            "kimmy.sync.pull_seconds.apply",
            "kimmy.sync.pulls",
            "kimmy.sync.entry_wait_seconds",
            "kimmy.sync.entry_waits",
            // Labelled counters are matched by stem too, so any one of the
            // four satisfies it for all of them.
            "kimmy.sync.contacts.caught_up",
            "kimmy.sync.contacts.budget",
            "kimmy.sync.contacts.ceiling",
            "kimmy.sync.contacts.failed",
        ] {
            assert_eq!(
                source.matches(&format!("\"{name}\",")).count(),
                1,
                "`{name}` is not an instrument on the bridge"
            );
        }
    }

    #[test]
    fn no_two_bridge_instruments_read_the_same_field() {
        // The bridge is a list of `observe!(kind, "kimmy.x.y", unit, help,
        // field)` calls, and **nothing checked which field each one reads**.
        // `kimmy.index.undecidable` reading `index_unkeyed` would publish one
        // figure under two names, on the surface alerting is built from, and
        // every existing test would pass: the coverage test below matches names
        // only, and the snapshot that might have caught it renders
        // `StorageReadings::default()`, where every field is zero.
        //
        // **Distinctness rather than name agreement**, because agreement is not
        // the rule: `kimmy.webhook.subscriptions.active` reads `webhook_active`
        // and four others abbreviate likewise, all correctly. Requiring the name
        // to match would need a list of those five — the literal list beside a
        // derivation that this round has removed three times. Two instruments
        // reading one field is the actual defect, and it needs no list.
        //
        // Only calls whose last argument is a bare field are judged; several
        // legitimately compute, and a closure is a departure rather than a typo.
        let source = include_str!("logging.rs");
        let mut by_field: std::collections::BTreeMap<String, Vec<String>> = Default::default();
        for (at, _) in source.match_indices("observe!(") {
            let Some(end) = source[at..].find(");") else { continue };
            let call = &source[at..at + end];
            let Some(name_start) = call.find("\"kimmy.") else { continue };
            let after = &call[name_start + 1..];
            let Some(name_end) = after.find('"') else { continue };
            let name = after[..name_end].to_string();
            let last = call.rsplit(',').next().unwrap_or_default().trim().trim_end_matches(',');
            let bare = !last.is_empty()
                && last.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if bare {
                by_field.entry(last.to_string()).or_default().push(name);
            }
        }
        assert!(
            by_field.len() > 20,
            "premise: bare-field instruments were found ({})",
            by_field.len()
        );
        let shared: Vec<String> = by_field
            .iter()
            .filter(|(_, names)| names.len() > 1)
            .map(|(field, names)| format!("{field} is read by {}", names.join(" and ")))
            .collect();
        assert!(
            shared.is_empty(),
            "these bridge instruments read one field under more than one name, so a figure is \
             published as something it is not:\n  {}",
            shared.join("\n  ")
        );
    }

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
