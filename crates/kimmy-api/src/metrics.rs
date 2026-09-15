//! Process counters behind `/metrics`.
//!
//! Plain atomics rather than a metrics framework. The endpoint renders
//! Prometheus text directly, the set of series is small and fixed, and a
//! registry would add a dependency and an abstraction to hold nine numbers.
//!
//! # What is deliberately not here
//!
//! **Per-collection series.** `/metrics` is unauthenticated, and a series per
//! collection would put the schema on it. The endpoint has always reported
//! counts rather than names for that reason.
//!
//! The two absences ADR-043 recorded are now filled, each on the terms that
//! kept it out. The latency histogram's buckets were **measured**, not
//! guessed — end-to-end against a release build, conditions in ADR-046 — and
//! replication lag is **pushed here by the replication loop**, which is the
//! only place a peer's version vector exists.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Histogram bucket upper bounds, in microseconds.
///
/// Chosen from measurement, not preference (ADR-046): end-to-end against a
/// release build, point reads (`GET /docs/{id}`) run p50 ≈ 250 µs / p99 under
/// 1 ms, filtered finds ≈ 1.4–2.6 ms, single-document inserts p50 ≈ 6 ms —
/// one durable commit each — and a 10k-document aggregation 10–43 ms. The
/// buckets bracket those clusters with headroom on both ends; the wide top
/// bucket exists so a stall shows as a shape change rather than vanishing
/// into `+Inf`.
const LATENCY_BUCKETS_US: [u64; 12] =
    [100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 1_000_000, 10_000_000];

/// Every counter as a plain value, for a reader that is not the renderer.
///
/// A value struct rather than an accessor per counter: the OTLP bridge wants
/// all of them, once, and twenty-five getters would be twenty-five things to
/// forget when a counter is added. See [`Metrics::snapshot`].
/// What the storage engine, the vector cache and the kernel report at the
/// moment a reader asks: the block `/metrics` renders ahead of the process
/// counters, and the one the OTLP bridge reads beside them (ADR-142).
///
/// Read by the caller, which has the engine, and handed in: this type holds
/// no database handle, and the two readers — the `/metrics` handler and the
/// bridge's export callback — each take a fresh reading so neither reports a
/// window the other measured. Every field is a level or a monotonic count,
/// so a reading taken at read time is exact.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageReadings {
    pub databases: u64,
    pub collections: u64,
    pub unique_violations: u64,
    pub commits: u64,
    pub fsyncs: u64,
    pub commits_grouped: u64,
    pub storage_bytes: u64,
    pub vector_index_cache_bytes: u64,
    /// This process's resident memory and its high-water mark, as the
    /// kernel reports them ([`ProcessMemory`], ADR-147). The figure a
    /// container memory limit is enforced against, which neither of the two
    /// byte gauges above is.
    pub process_resident_bytes: u64,
    pub process_resident_peak_bytes: u64,
    /// Documents filed under an index's unkeyed run since start
    /// (`Engine::unkeyed_writes`, ADR-139).
    pub index_unkeyed: u64,
    /// How long writes waited for the single writer, the writes that gave
    /// up waiting inside their budget, and the longest any one transaction
    /// held it (ADR-151). The wait is the part of a write's latency that
    /// is not its own work, and nothing else on this page separates the two.
    pub writer_wait: kimmy_storage::WriterWaitSnapshot,
    pub writer_wait_timeouts: u64,
    pub writer_hold_max_us: u64,
    /// How long the writer was held, split by what held it (ADR-159). The
    /// maximum above says how bad the worst hold was and nothing about what
    /// caused it; this is the reading an operator acts on.
    pub writer_hold: kimmy_storage::WriterHoldSnapshot,
    /// Entries held as state that a sync window released, since start
    /// (`Engine::held_marks_released`, ADR-169's addendum).
    pub held_marks_released: u64,
}

/// The process's resident memory, read from the kernel.
///
/// `VmRSS` and `VmHWM` from `/proc/self/status`, which is what the project's
/// own allocator measurements sample (docs/benchmarks.md) and what a cgroup
/// memory limit is enforced against. Parsed with the standard library alone:
/// two lines of a text file are not worth a crate, and the release binary
/// is a static musl build where every dependency is a build to audit.
///
/// Zero where there is no `/proc` — a macOS build, or a mount namespace
/// without one. Zero rather than absent, so the series is always on the
/// page and a dashboard built against it does not go blank on a platform;
/// the HELP text says what a zero means.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProcessMemory {
    /// Bytes resident right now (`VmRSS`).
    pub resident_bytes: u64,
    /// The most bytes that were resident at any moment of this process's
    /// life (`VmHWM`). What a container limit was hit *by*, after the fact,
    /// when the current figure has already come back down.
    pub peak_resident_bytes: u64,
}

impl ProcessMemory {
    /// Read the kernel's figures for this process, fresh.
    ///
    /// One small file read; `/proc` is not a disk, so this costs what a
    /// scrape can afford. A file that cannot be read reads as zeros, which
    /// is what the HELP text promises for a platform without it.
    pub fn read() -> Self {
        #[cfg(target_os = "linux")]
        {
            std::fs::read_to_string("/proc/self/status")
                .map(|s| Self::parse(&s))
                .unwrap_or_default()
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::default()
        }
    }

    /// Pull the two fields out of the text of `/proc/self/status`.
    ///
    /// The kernel prints both in kB whatever the value, as
    /// `VmRSS:\t  123456 kB`. A line that is missing or does not parse leaves
    /// its field at zero rather than failing the reading: the other field is
    /// still worth having.
    pub fn parse(status: &str) -> Self {
        let field = |name: &str| -> u64 {
            status
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|kb| kb.parse::<u64>().ok())
                .map_or(0, |kb| kb.saturating_mul(1024))
        };
        Self { resident_bytes: field("VmRSS:"), peak_resident_bytes: field("VmHWM:") }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// The engine's block, as read for this snapshot (ADR-142).
    pub databases: u64,
    pub collections: u64,
    pub unique_violations: u64,
    pub commits: u64,
    pub fsyncs: u64,
    pub commits_grouped: u64,
    pub storage_bytes: u64,
    pub vector_index_cache_bytes: u64,
    /// Resident memory and its high-water mark, from the same reading
    /// (ADR-147).
    pub process_resident_bytes: u64,
    pub process_resident_peak_bytes: u64,
    pub uptime_secs: u64,
    pub requests: u64,
    pub responses_2xx: u64,
    pub responses_4xx: u64,
    pub responses_5xx: u64,
    pub authz_denied: u64,
    pub auth_failures: u64,
    pub rate_limited: u64,
    /// The subset of `rate_limited` refused by the per-principal limit
    /// (ADR-099); the rest were login attempts.
    pub rate_limited_principal: u64,
    pub backups: u64,
    pub ttl_expired: u64,
    pub ttl_skipped: u64,
    /// Documents filed under an index's unkeyed run — stored, but with no
    /// key the index could derive, so every scan of that index rechecks
    /// them (ADR-139). One of the engine's readings.
    pub index_unkeyed: u64,
    /// Writes that gave up waiting for the writer, and the longest hold in
    /// microseconds (ADR-151); the wait histogram itself is not bridged.
    pub write_lock_wait_timeouts: u64,
    pub write_lock_held_max_us: u64,
    /// Holds of the writer, and microseconds it was held for, by each
    /// [`kimmy_storage::WriterHolder`] in `WriterHolder::ALL` order
    /// (ADR-159). The histogram's buckets are on `/metrics` alone; these
    /// two rows are what the bridge carries, one instrument per holder.
    pub write_lock_holds: [u64; kimmy_storage::WriterHolder::COUNT],
    pub write_lock_held_us: [u64; kimmy_storage::WriterHolder::COUNT],
    pub webhook_delivered: u64,
    pub webhook_failed: u64,
    pub webhook_events: u64,
    pub webhook_active: u64,
    pub webhook_invalidated: u64,
    pub webhook_backlog_secs: u64,
    pub cluster_members: u64,
    pub replication_lag_secs: u64,
    /// Anti-entropy rounds that failed, peers currently backed off, and
    /// replicated schema changes skipped (ADR-123). The failure signals the
    /// lag gauge cannot carry: a failed round reports no lag.
    pub sync_failures: u64,
    pub sync_peers_backing_off: u64,
    pub sync_ddl_refused: u64,
    /// Replicated index drops declined as older than the index standing
    /// here (ADR-141).
    pub sync_ddl_declined: u64,
    /// Collections the cross-member divergence check currently has confirmed
    /// (ADR-133): held by a peer and not here, or held by both with
    /// disagreeing document counts, seen on two ticks running. Moves for a
    /// divergence none of the three counters above can express, because
    /// nothing about it fails a round.
    pub sync_divergent_collections: u64,
    /// Peer contacts whose round ran the cross-member divergence check, and
    /// contacts whose round skipped it because the pull was truncated by the
    /// batch cap (ADR-135). What makes `sync_divergent_collections` reading 0
    /// mean *checked and agreed* rather than *not checked*.
    pub sync_divergence_checks: u64,
    pub sync_divergence_skips: u64,
    /// Checked contacts in which the count half of the check compared a
    /// document count, and checked contacts in which it was deferred
    /// because the peer was behind and still advancing (ADR-145). What
    /// says whether the half that catches a lost run of documents has
    /// looked at anything.
    pub sync_divergence_count_compared: u64,
    pub sync_divergence_count_deferred: u64,
    /// Seconds since the last contact whose round ran the check, computed
    /// when this snapshot was taken from the instant the loop last
    /// reported; 0 before the first (ADR-145, ADR-154). How old the gauge's
    /// reading is, on a member whose rounds have stopped completing — or
    /// whose loop has stopped ticking, which is why it is computed here and
    /// not carried from the loop's last tick.
    pub sync_divergence_check_age_secs: u64,
    /// Batches a sync round stopped at an entry for a collection this node
    /// does not hold, and entries a round left for a later window because
    /// they sat above the vector the peer had advertised (ADR-148). Two
    /// reasons on one series: the first is a hole being held open until a
    /// snapshot closes it, the second ordinary and rare.
    pub sync_entries_skipped_unknown_collection: u64,
    pub sync_entries_skipped_beyond_advertised: u64,
    /// Entries held as state that a sync window released (ADR-169's
    /// addendum): the release path itself, which `beyond_advertised` cannot
    /// tell from the ordinary race. An engine reading, not a round report.
    pub sync_held_marks_released: u64,
    /// Rounds spent repairing against a peer (ADR-148): re-serving its
    /// oplog from below this node's position or pulling its snapshot, on
    /// the strength of a confirmed divergence or a stopped batch.
    pub sync_repair_rounds: u64,
    /// Worst runtime scheduling delay since the last scrape, microseconds.
    pub runtime_stall_us: u64,
    pub tls_reloads_ok: u64,
    pub tls_reloads_failed: u64,
    pub jwks_refresh_ok: u64,
    pub jwks_refresh_failed: u64,
    /// Embedding-worker counters, all zero when the worker is disabled on
    /// this node — which is exactly the distinction an operator needs.
    pub embed_documents_embedded: u64,
    pub embed_chunks_embedded: u64,
    pub embed_deferred: u64,
    pub embed_skipped_not_owned: u64,
    pub embed_failures: u64,
    /// Transport failures among `embed_failures`, in the order connect,
    /// timeout, reset, other.
    pub embed_transport: [u64; 4],
    /// Provider calls answered and input tokens billed, process-wide.
    pub embed_provider_requests: u64,
    pub embed_provider_tokens: u64,
    /// Requests observed by the latency histogram — health and metrics routes
    /// excluded, so this is smaller than `requests` on any real node.
    pub latency_count: u64,
    pub latency_sum_us: u64,
}

/// Counters for one running server.
pub struct Metrics {
    started: Instant,
    latency_buckets: [AtomicU64; LATENCY_BUCKETS_US.len()],
    latency_sum_us: AtomicU64,
    latency_count: AtomicU64,
    replication_lag_secs: AtomicU64,
    /// Pushed by the replication loop after every sync tick (ADR-123): two
    /// counters and a level. What a wedged round looks like from outside,
    /// which the lag gauge — set only by a round that succeeded — cannot
    /// show.
    sync_failures: AtomicU64,
    sync_peers_backing_off: AtomicU64,
    sync_ddl_refused: AtomicU64,
    sync_ddl_declined: AtomicU64,
    sync_divergent_collections: AtomicU64,
    /// The gauge above says how many collections disagree; these two say
    /// whether anything looked (ADR-135). Counters, unlike the gauge beside
    /// them, because "has the check run recently" is a question about a
    /// window of time and a level cannot answer it.
    sync_divergence_checks: AtomicU64,
    sync_divergence_skips: AtomicU64,
    /// The count half's own pair (ADR-145): `ran` above says the check ran,
    /// these say whether the half that compares a document count did, or
    /// was held back for a peer still catching up. Counters, like the pair
    /// above and for the same reason.
    sync_divergence_count_compared: AtomicU64,
    sync_divergence_count_deferred: AtomicU64,
    /// When the loop last ran the check against any peer, as the loop
    /// reported it at its last tick; `None` before the first check
    /// (ADR-145, ADR-154). What `kimmy_sync_divergence_check_age_seconds`
    /// is computed from *at the moment it is read*, so the age keeps
    /// rising on a member whose rounds all fail — where the two counters
    /// above stop and the gauge holds its last value — and equally on a
    /// member whose loop has stopped completing ticks and pushes nothing at
    /// all. Under ADR-145 this held the age the loop computed at the end of
    /// its tick, and a loop stuck behind the single writer for an hour left
    /// it reading the same number on every scrape: the one series meant to
    /// say the gauge was stale was frozen with it.
    ///
    /// A mutex rather than an atomic because it holds an `Instant`, which
    /// has no lock-free encoding that is not a fiction about some epoch;
    /// it is taken for one copy on a tick and one on a scrape, never held.
    sync_divergence_last_check: parking_lot::Mutex<Option<Instant>>,
    sync_entries_skipped_unknown_collection: AtomicU64,
    sync_entries_skipped_beyond_advertised: AtomicU64,
    sync_repair_rounds: AtomicU64,
    /// The worst scheduling delay the runtime probe saw since the last
    /// scrape, in microseconds. A worker that blocks on a storage commit
    /// shows up here before it shows up as a peer's handshake timeout.
    ///
    /// **One high-water mark per reader**, because reading one clears it.
    /// `/metrics` and the OTLP bridge are two independent consumers on two
    /// unrelated schedules: with a single mark, whichever read first would take
    /// the value and leave the other reporting a window it did not measure, and
    /// on a deployment that only ever reads through a collector nothing would
    /// clear the `/metrics` mark at all. Every other series here is a counter or
    /// a level, where a plain load serves every reader; this is the only
    /// take-on-read series, so it is the only one that needs a mark each.
    runtime_stall_us: AtomicU64,
    /// The OTLP bridge's own copy of [`Self::runtime_stall_us`]. Fed by the
    /// same `fetch_max`, cleared by the bridge's own read.
    runtime_stall_otlp_us: AtomicU64,
    requests: AtomicU64,
    responses_2xx: AtomicU64,
    responses_4xx: AtomicU64,
    responses_5xx: AtomicU64,
    authz_denied: AtomicU64,
    auth_failures: AtomicU64,
    rate_limited: AtomicU64,
    /// Bumped by the `Auth` extractor rather than derived from the status,
    /// unlike `rate_limited`: a 429 no longer has exactly one source, and the
    /// question this series answers is *which* limit an operator is hitting.
    rate_limited_principal: AtomicU64,
    backups: AtomicU64,
    webhook_delivered: AtomicU64,
    webhook_failed: AtomicU64,
    webhook_events: AtomicU64,
    webhook_active: AtomicU64,
    webhook_invalidated: AtomicU64,
    webhook_backlog_secs: AtomicU64,
    cluster_members: AtomicU64,
    tls_reloads_ok: AtomicU64,
    tls_reloads_failed: AtomicU64,
    jwks_refresh_ok: AtomicU64,
    jwks_refresh_failed: AtomicU64,
    ttl_expired: AtomicU64,
    ttl_skipped: AtomicU64,
    /// Set once at startup when the embedding worker runs. `None` — the
    /// renderer then reports zeros — means this node has
    /// `[vector] worker_enabled = false`, which an operator must be able to
    /// distinguish from "worker enabled but idle".
    vector_counters: OnceLock<std::sync::Arc<kimmy_vector::WorkerCounters>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            replication_lag_secs: AtomicU64::new(0),
            sync_failures: AtomicU64::new(0),
            sync_peers_backing_off: AtomicU64::new(0),
            sync_ddl_refused: AtomicU64::new(0),
            sync_ddl_declined: AtomicU64::new(0),
            sync_divergent_collections: AtomicU64::new(0),
            sync_divergence_checks: AtomicU64::new(0),
            sync_divergence_skips: AtomicU64::new(0),
            sync_divergence_count_compared: AtomicU64::new(0),
            sync_divergence_count_deferred: AtomicU64::new(0),
            sync_divergence_last_check: parking_lot::Mutex::new(None),
            sync_entries_skipped_unknown_collection: AtomicU64::new(0),
            sync_entries_skipped_beyond_advertised: AtomicU64::new(0),
            sync_repair_rounds: AtomicU64::new(0),
            runtime_stall_us: AtomicU64::new(0),
            runtime_stall_otlp_us: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            responses_2xx: AtomicU64::new(0),
            responses_4xx: AtomicU64::new(0),
            responses_5xx: AtomicU64::new(0),
            authz_denied: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            rate_limited: AtomicU64::new(0),
            rate_limited_principal: AtomicU64::new(0),
            backups: AtomicU64::new(0),
            webhook_delivered: AtomicU64::new(0),
            webhook_failed: AtomicU64::new(0),
            webhook_events: AtomicU64::new(0),
            ttl_expired: AtomicU64::new(0),
            ttl_skipped: AtomicU64::new(0),
            webhook_active: AtomicU64::new(0),
            webhook_invalidated: AtomicU64::new(0),
            webhook_backlog_secs: AtomicU64::new(0),
            cluster_members: AtomicU64::new(0),
            tls_reloads_ok: AtomicU64::new(0),
            tls_reloads_failed: AtomicU64::new(0),
            jwks_refresh_ok: AtomicU64::new(0),
            jwks_refresh_failed: AtomicU64::new(0),
            vector_counters: OnceLock::new(),
        }
    }
}

impl Metrics {
    /// Share the worker's counters with this renderer. Called once at node
    /// startup, before the worker task is spawned; later calls are ignored,
    /// because a renderer that switched handles mid-flight would report two
    /// partial series where one total was meant.
    pub fn set_vector_counters(&self, counters: std::sync::Arc<kimmy_vector::WorkerCounters>) {
        let _ = self.vector_counters.set(counters);
    }
    /// Count one finished request.
    ///
    /// The three specific counters are derived from the status rather than
    /// incremented where the refusal happens, because each of those statuses
    /// has exactly one source: 401 from token or credential rejection, 403 from
    /// `ApiError::forbidden` (RBAC and nothing else), 429 from a rate
    /// limiter. Deriving them here keeps the counting in one place instead of
    /// threading a metrics handle into the authorization path — and a counter
    /// that lives beside the check is a counter someone forgets to bump when
    /// they add a route. The one exception is
    /// [`Metrics::record_principal_rate_limited`], which splits the 429s by
    /// *which* limiter refused, a fact the status does not carry.
    pub fn record_request(&self, status: u16) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        match status {
            200..=299 => &self.responses_2xx,
            400..=499 => &self.responses_4xx,
            500..=599 => &self.responses_5xx,
            // 1xx and 3xx are counted in the total and nowhere else; neither is
            // a success or a failure worth its own series here.
            _ => return,
        }
        .fetch_add(1, Ordering::Relaxed);

        match status {
            401 => self.auth_failures.fetch_add(1, Ordering::Relaxed),
            403 => self.authz_denied.fetch_add(1, Ordering::Relaxed),
            429 => self.rate_limited.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    /// One authenticated request refused by the per-principal limit
    /// (ADR-099).
    ///
    /// The response is a 429, so `record_request` counts it in
    /// `rate_limited_total` as well; this series is the part of that total an
    /// operator tuning `server.rate_limit.per_principal` is looking for, kept
    /// apart from the login limiter's refusals, which mean something else.
    pub fn record_principal_rate_limited(&self) {
        self.rate_limited_principal.fetch_add(1, Ordering::Relaxed);
    }

    /// One delivery attempt, and how many events it carried.
    ///
    /// Batches, not events, are counted as the outcome: a retried batch is one
    /// failure, and counting per event would make one dead endpoint look like
    /// thousands of separate problems.
    pub fn record_webhook_delivery(&self, succeeded: bool, events: usize) {
        if succeeded {
            self.webhook_delivered.fetch_add(1, Ordering::Relaxed);
            self.webhook_events.fetch_add(events as u64, Ordering::Relaxed);
        } else {
            self.webhook_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The webhook gauges, set by the dispatcher at the end of each pass.
    ///
    /// Set rather than accumulated: all three describe a state at an instant,
    /// and the dispatcher already computes them while walking the registry. The
    /// alternative — recomputing on every `/metrics` scrape — would re-read the
    /// progress collection once per subscription for a number the dispatcher
    /// had in hand two seconds earlier.
    ///
    /// `backlog_secs` covers only subscriptions **this node owns**. A node that
    /// has stood down must not report a backlog it is not the one working
    /// through, or every node in a cluster would alert for the same lag.
    pub fn set_webhook_gauges(&self, active: u64, invalidated: u64, backlog_secs: u64) {
        self.webhook_active.store(active, Ordering::Relaxed);
        self.webhook_invalidated.store(invalidated, Ordering::Relaxed);
        self.webhook_backlog_secs.store(backlog_secs, Ordering::Relaxed);
    }

    /// Record one request's end-to-end latency.
    ///
    /// Non-cumulative per bucket; the render accumulates, because Prometheus
    /// buckets are cumulative on the wire but a store-time increment of every
    /// bucket ≥ the observation would be N writes for one sample.
    pub fn record_latency(&self, elapsed: std::time::Duration) {
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        let slot = LATENCY_BUCKETS_US.iter().position(|&upper| micros <= upper);
        if let Some(slot) = slot {
            self.latency_buckets[slot].fetch_add(1, Ordering::Relaxed);
        }
        // Above every bound: only `+Inf` (derived from the count) sees it.
        self.latency_sum_us.fetch_add(micros, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    /// How far behind in time this node is: seconds since the newest entry
    /// it has applied from an origin a peer holds newer entries of, worst
    /// origin over the peers reached in the last round.
    ///
    /// Pushed by the replication loop after each round, because that is the
    /// only place a peer's version vector exists — the reason ADR-043 left
    /// this out rather than guessing. Zero when caught up; grows with the
    /// clock while a backlog drains, which the span of the missing history
    /// did not (ADR-122).
    pub fn set_replication_lag_secs(&self, secs: u64) {
        self.replication_lag_secs.store(secs, Ordering::Relaxed);
    }

    /// One sync tick of the replication loop: how many rounds failed, how
    /// many peers are now being backed off, how many replicated schema
    /// changes were refused and skipped (ADR-123), and how many collections
    /// the cross-member divergence check currently has confirmed (ADR-133).
    ///
    /// Pushed after every tick, reached peers or not — unlike the lag gauge,
    /// which a tick that reached nobody leaves alone. The first and third
    /// counters accumulate; the backoff figure and the divergence count are
    /// each a level and replace the last one. A rising failure count against
    /// a lag gauge that reads 0 is one shape of a replication wedge that
    /// reads as healthy everywhere else; a divergent-collections count above
    /// 0 against the *same* healthy reading is another, and it is the one
    /// none of the other three can express, because nothing about it fails a
    /// round.
    ///
    /// `divergence_checks` and `divergence_skips` qualify the fourth rather
    /// than adding a fifth signal (ADR-135): a divergent count of 0 is
    /// evidence of agreement only while checks are still being made, and the
    /// skip count is why they are not. Both accumulate. Pushed on this same
    /// call rather than through a second one from the same hook, so a tick
    /// cannot land half of what it saw.
    ///
    /// `divergence_count_compared` and `divergence_count_deferred` qualify
    /// the check one level further (ADR-145): whether the half that
    /// compares a document count ran, or was held back for a peer still
    /// catching up. Both accumulate. `divergence_last_check` is a level
    /// and replaces the last one: *when* the divergent count was last
    /// re-examined, from which every read of the age series subtracts its
    /// own clock (ADR-154) — the number that keeps moving when a member's
    /// rounds stop completing and every counter here stops with them, and
    /// when the loop itself stops and this method is not called again.
    /// `None` — no check has ever run — renders as an age of `0`, beside a
    /// `ran` count that also reads `0`.
    ///
    /// **The report is taken whole, not field by field.** Six of its numbers
    /// now reach `/metrics`, all of them `usize`, four of them counting
    /// different things about the same tick — and the only call site is a
    /// closure in `kimmyd::spawn_cluster` that no test covers, so a pair of
    /// positional arguments swapped there would compile, pass every gate, and
    /// silently report one series' value under another's name for the life of
    /// the release. Passing the struct removes the argument order from the
    /// problem entirely: the fields are named at the point they are read, and
    /// a field added to [`kimmy_cluster::RoundReport`] cannot quietly take
    /// another's place. The `usize` to `u64` widening happens here for the
    /// same reason — six casts at a call site are six more chances to write
    /// the wrong one.
    pub fn record_sync_round(&self, round: &kimmy_cluster::RoundReport) {
        self.sync_failures.fetch_add(round.failed as u64, Ordering::Relaxed);
        self.sync_peers_backing_off.store(round.backing_off as u64, Ordering::Relaxed);
        self.sync_ddl_refused.fetch_add(round.ddl_refused as u64, Ordering::Relaxed);
        self.sync_ddl_declined.fetch_add(round.ddl_declined as u64, Ordering::Relaxed);
        self.sync_divergent_collections
            .store(round.divergent_collections as u64, Ordering::Relaxed);
        self.sync_divergence_checks.fetch_add(round.divergence_checks as u64, Ordering::Relaxed);
        self.sync_divergence_skips.fetch_add(round.divergence_skips as u64, Ordering::Relaxed);
        self.sync_divergence_count_compared
            .fetch_add(round.divergence_count_compared as u64, Ordering::Relaxed);
        self.sync_divergence_count_deferred
            .fetch_add(round.divergence_count_deferred as u64, Ordering::Relaxed);
        *self.sync_divergence_last_check.lock() = round.divergence_last_check;
        self.record_entries_skipped(
            round.entries_skipped_unknown_collection as u64,
            round.entries_skipped_beyond_advertised as u64,
        );
        self.sync_repair_rounds.fetch_add(round.repair_rounds as u64, Ordering::Relaxed);
    }

    /// Count what a batch left rather than took (ADR-148), on the series a
    /// pulled and a pushed batch share, for the reason
    /// [`Self::record_ddl_refused`] gives: `unknown_collection` batches
    /// stopped at a collection this node lacks, `beyond_advertised` entries
    /// left for a later window.
    pub fn record_entries_skipped(&self, unknown_collection: u64, beyond_advertised: u64) {
        self.sync_entries_skipped_unknown_collection
            .fetch_add(unknown_collection, Ordering::Relaxed);
        self.sync_entries_skipped_beyond_advertised.fetch_add(beyond_advertised, Ordering::Relaxed);
    }

    /// Count schema changes a peer pushed to this node that it could not
    /// apply and skipped (ADR-140), on the series a pulled refusal lands on:
    /// the member's own counter must not depend on which way the change
    /// arrived.
    pub fn record_ddl_refused(&self, n: u64) {
        self.sync_ddl_refused.fetch_add(n, Ordering::Relaxed);
    }

    /// Count index drops a peer pushed to this node that it declined as older
    /// than the index it holds (ADR-141), on the series a pulled decline
    /// lands on, for the reason [`Self::record_ddl_refused`] gives.
    pub fn record_ddl_declined(&self, n: u64) {
        self.sync_ddl_declined.fetch_add(n, Ordering::Relaxed);
    }

    /// How many peers this node's SWIM instance currently considers alive.
    ///
    /// The observable the cluster harness asserts gossip *formed* with —
    /// replication converging is not proof, because discovery alone can
    /// deliver convergence while gossip silently never forms, which is
    /// exactly what the shipped compose file once did.
    pub fn set_cluster_members(&self, n: u64) {
        self.cluster_members.store(n, Ordering::Relaxed);
    }

    pub fn record_backup(&self) {
        self.backups.fetch_add(1, Ordering::Relaxed);
    }

    /// One collection's expiry pass.
    ///
    /// `expired` is what makes the ownership choice checkable: expiry is owned
    /// by one node per collection so a document produces **one** delete
    /// cluster-wide, and summing this counter across a cluster is how that
    /// stops being an assertion nobody measures. `skipped` counts candidates
    /// refused because the document was refreshed between the scan and the
    /// write — a steady rise there means TTLs are being reset as fast as the
    /// pass finds them.
    pub fn record_expiry(&self, expired: u64, skipped: u64) {
        self.ttl_expired.fetch_add(expired, Ordering::Relaxed);
        self.ttl_skipped.fetch_add(skipped, Ordering::Relaxed);
    }

    /// Count one certificate reload attempt.
    ///
    /// A failed reload is the quiet failure this metric exists for: the node
    /// keeps serving the certificate it already had, perfectly, until that one
    /// expires and every client drops at once. Nothing about a working request
    /// reveals that the renewal never took (ADR-049).
    pub fn record_tls_reload(&self, succeeded: bool) {
        let counter = if succeeded { &self.tls_reloads_ok } else { &self.tls_reloads_failed };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one attempt to refresh the identity provider's signing keys.
    ///
    /// The same quiet failure as a certificate reload, one step worse: a node
    /// that cannot reach its provider keeps verifying tokens perfectly against
    /// the keys it already has, until the provider rotates and every federated
    /// caller is refused at once. Nothing about a working request reveals that
    /// the refresh has been failing for a day (ADR-064).
    pub fn record_jwks_refresh(&self, succeeded: bool) {
        let counter = if succeeded { &self.jwks_refresh_ok } else { &self.jwks_refresh_failed };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// `kimmy_sync_divergence_check_age_seconds` as of `now`: seconds since
    /// the instant the replication loop last reported a check, 0 before the
    /// first (ADR-145, ADR-154).
    ///
    /// Computed here, at the read, and not stored: a stored age is as old
    /// as the tick that stored it, and the member this was written for had
    /// a tick that did not end for an hour. `now` is a parameter so a test
    /// can read the age ninety seconds after a check without waiting ninety
    /// seconds; every caller outside a test passes `Instant::now()`.
    fn sync_divergence_check_age_secs_at(&self, now: Instant) -> u64 {
        self.sync_divergence_last_check
            .lock()
            .map_or(0, |at| now.saturating_duration_since(at).as_secs())
    }

    /// Record how late the runtime probe woke up. Keeps the maximum until the
    /// next scrape reads it, so a one-off stall between scrapes is not lost.
    ///
    /// Fed to both marks: each reader gets the worst stall since *its own* last
    /// read, and neither can consume the other's.
    pub fn record_runtime_stall(&self, late: std::time::Duration) {
        let us = u64::try_from(late.as_micros()).unwrap_or(u64::MAX);
        self.runtime_stall_us.fetch_max(us, Ordering::Relaxed);
        self.runtime_stall_otlp_us.fetch_max(us, Ordering::Relaxed);
    }

    /// The worst runtime stall since the last call, in seconds.
    fn take_runtime_stall_secs(&self) -> f64 {
        self.runtime_stall_us.swap(0, Ordering::Relaxed) as f64 / 1_000_000.0
    }

    /// The worst runtime stall since the OTLP bridge last asked, in
    /// microseconds.
    ///
    /// Separate from [`Self::take_runtime_stall_secs`] because both clear on
    /// read and the two surfaces are read on unrelated schedules. In
    /// microseconds rather than seconds because the instrument carries `us` as
    /// its unit: the interesting values are well under a second, and rounding
    /// them to seconds would report every one as 0.
    ///
    /// **Not part of [`Self::snapshot`]**, which is a plain read every other
    /// bridged instrument shares and which must stay non-destructive — a
    /// clearing read hidden inside it would silently break every other caller.
    pub fn take_runtime_stall_otlp_us(&self) -> u64 {
        self.runtime_stall_otlp_us.swap(0, Ordering::Relaxed)
    }

    fn get(&self, counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// Every counter, read once, as plain numbers.
    ///
    /// The read surface the OTLP bridge uses (ADR-070). Counters are
    /// **bridged, not duplicated**: an observable instrument's callback reads
    /// this and reports it, so `/metrics` and a collector are two renderings of
    /// one set of atomics rather than two sets that can drift. A second set is
    /// the failure this avoids — a Prometheus dashboard and a trace backend
    /// disagreeing about how many requests a node served, with nothing to say
    /// which is right.
    ///
    /// Not a consistent snapshot, and it does not need to be: each field is a
    /// relaxed load, so a value may be one increment behind a sibling. That is
    /// already true of `render`, and of any counter read without a lock.
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.snapshot_with(&StorageReadings::default())
    }

    /// [`Self::snapshot`], with the engine's readings filled in — the form
    /// the OTLP bridge uses, so every series `/metrics` renders has a field
    /// here (ADR-142). The reading-free form above is for a caller with no
    /// engine in hand, which is a test.
    pub fn snapshot_with(&self, readings: &StorageReadings) -> MetricsSnapshot {
        self.snapshot_with_at(readings, Instant::now())
    }

    /// [`Self::snapshot_with`], with the age of the divergence check's
    /// reading measured against `now` rather than the clock (ADR-154). For
    /// a test; nothing else has a reason to read the age as of any moment
    /// but this one.
    pub fn snapshot_with_at(&self, readings: &StorageReadings, now: Instant) -> MetricsSnapshot {
        MetricsSnapshot {
            databases: readings.databases,
            collections: readings.collections,
            unique_violations: readings.unique_violations,
            commits: readings.commits,
            fsyncs: readings.fsyncs,
            commits_grouped: readings.commits_grouped,
            storage_bytes: readings.storage_bytes,
            vector_index_cache_bytes: readings.vector_index_cache_bytes,
            process_resident_bytes: readings.process_resident_bytes,
            process_resident_peak_bytes: readings.process_resident_peak_bytes,
            write_lock_wait_timeouts: readings.writer_wait_timeouts,
            write_lock_held_max_us: readings.writer_hold_max_us,
            write_lock_holds: readings.writer_hold.count,
            write_lock_held_us: readings.writer_hold.sum_us,
            uptime_secs: self.uptime_secs(),
            requests: self.get(&self.requests),
            responses_2xx: self.get(&self.responses_2xx),
            responses_4xx: self.get(&self.responses_4xx),
            responses_5xx: self.get(&self.responses_5xx),
            authz_denied: self.get(&self.authz_denied),
            auth_failures: self.get(&self.auth_failures),
            rate_limited: self.get(&self.rate_limited),
            rate_limited_principal: self.get(&self.rate_limited_principal),
            backups: self.get(&self.backups),
            ttl_expired: self.get(&self.ttl_expired),
            ttl_skipped: self.get(&self.ttl_skipped),
            index_unkeyed: readings.index_unkeyed,
            webhook_delivered: self.get(&self.webhook_delivered),
            webhook_failed: self.get(&self.webhook_failed),
            webhook_events: self.get(&self.webhook_events),
            webhook_active: self.get(&self.webhook_active),
            webhook_invalidated: self.get(&self.webhook_invalidated),
            webhook_backlog_secs: self.get(&self.webhook_backlog_secs),
            cluster_members: self.get(&self.cluster_members),
            replication_lag_secs: self.get(&self.replication_lag_secs),
            sync_failures: self.get(&self.sync_failures),
            sync_peers_backing_off: self.get(&self.sync_peers_backing_off),
            sync_ddl_refused: self.get(&self.sync_ddl_refused),
            sync_ddl_declined: self.get(&self.sync_ddl_declined),
            sync_divergent_collections: self.get(&self.sync_divergent_collections),
            sync_divergence_checks: self.get(&self.sync_divergence_checks),
            sync_divergence_skips: self.get(&self.sync_divergence_skips),
            sync_divergence_count_compared: self.get(&self.sync_divergence_count_compared),
            sync_divergence_count_deferred: self.get(&self.sync_divergence_count_deferred),
            sync_divergence_check_age_secs: self.sync_divergence_check_age_secs_at(now),
            sync_entries_skipped_unknown_collection: self
                .get(&self.sync_entries_skipped_unknown_collection),
            sync_entries_skipped_beyond_advertised: self
                .get(&self.sync_entries_skipped_beyond_advertised),
            sync_held_marks_released: readings.held_marks_released,
            sync_repair_rounds: self.get(&self.sync_repair_rounds),
            runtime_stall_us: self.get(&self.runtime_stall_us),
            tls_reloads_ok: self.get(&self.tls_reloads_ok),
            tls_reloads_failed: self.get(&self.tls_reloads_failed),
            jwks_refresh_ok: self.get(&self.jwks_refresh_ok),
            jwks_refresh_failed: self.get(&self.jwks_refresh_failed),
            latency_count: self.get(&self.latency_count),
            latency_sum_us: self.get(&self.latency_sum_us),
            embed_documents_embedded: self
                .vector_counters
                .get()
                .map_or(0, |c| c.documents_embedded.load(Ordering::Relaxed)),
            embed_chunks_embedded: self
                .vector_counters
                .get()
                .map_or(0, |c| c.chunks_embedded.load(Ordering::Relaxed)),
            embed_deferred: self
                .vector_counters
                .get()
                .map_or(0, |c| c.deferred.load(Ordering::Relaxed)),
            embed_skipped_not_owned: self
                .vector_counters
                .get()
                .map_or(0, |c| c.skipped_not_owned.load(Ordering::Relaxed)),
            embed_failures: self
                .vector_counters
                .get()
                .map_or(0, |c| c.failures.load(Ordering::Relaxed)),
            embed_provider_requests: kimmy_vector::provider_totals().0,
            embed_provider_tokens: kimmy_vector::provider_totals().1,
            embed_transport: self.vector_counters.get().map_or([0; 4], |c| {
                kimmy_vector::TransportKind::ALL.map(|k| c.transport_failures(k))
            }),
        }
    }

    /// Render the page in Prometheus text format, with no engine readings —
    /// every engine series at zero. For a caller with no engine in hand,
    /// which is a test; the `/metrics` handler uses [`Self::render_with`].
    pub fn render(&self) -> String {
        self.render_with(&StorageReadings::default())
    }

    /// [`Self::render`], with the age of the divergence check's reading
    /// measured against `now` rather than the clock (ADR-154). For a test.
    pub fn render_at(&self, now: Instant) -> String {
        self.render_with_at(&StorageReadings::default(), now)
    }

    /// Render the whole `/metrics` page: the engine's readings first, then
    /// the process counters, in the order a scrape has always seen them.
    ///
    /// The engine block used to be a second format string in the route
    /// handler, ahead of this one, which is how nine series came to be on
    /// `/metrics` and not on the OTLP bridge with nothing to object: the
    /// guard that compares the two surfaces reads this render, and those
    /// series were not in it (ADR-142). The readings are handed in rather
    /// than read here, so this type still holds no database handle.
    pub fn render_with(&self, readings: &StorageReadings) -> String {
        self.render_with_at(readings, Instant::now())
    }

    /// [`Self::render_with`], with the age of the divergence check's
    /// reading measured against `now` rather than the clock (ADR-154). The
    /// one series on the page that is a subtraction against the moment of
    /// the read rather than a load of something stored — so it is the one
    /// a test has to be able to read at a moment of its choosing. Uptime
    /// reads the process clock as it always has; it has no test that needs
    /// otherwise.
    pub fn render_with_at(&self, readings: &StorageReadings, now: Instant) -> String {
        // Read once: the worker's atomics move as it runs, and a render that
        // straddled an increment would show mismatched document/chunk pairs.
        let vc = self.vector_counters.get();
        let (embed_docs, embed_chunks, embed_deferred, embed_not_owned, embed_failures, transport) =
            match vc {
                Some(c) => (
                    c.documents_embedded.load(Ordering::Relaxed),
                    c.chunks_embedded.load(Ordering::Relaxed),
                    c.deferred.load(Ordering::Relaxed),
                    c.skipped_not_owned.load(Ordering::Relaxed),
                    c.failures.load(Ordering::Relaxed),
                    kimmy_vector::TransportKind::ALL.map(|k| c.transport_failures(k)),
                ),
                None => (0, 0, 0, 0, 0, [0; 4]),
            };
        let [t_connect, t_timeout, t_reset, t_other] = transport;
        let mut out = format!(
            "# HELP kimmy_databases Number of databases.\n\
             # TYPE kimmy_databases gauge\n\
             kimmy_databases {databases}\n\
             # HELP kimmy_collections Number of collections across all databases.\n\
             # TYPE kimmy_collections gauge\n\
             kimmy_collections {collections}\n\
             # HELP kimmy_unique_violations Unique constraints broken by merging replicated writes.\n\
             # TYPE kimmy_unique_violations counter\n\
             kimmy_unique_violations {violations}\n\
             # HELP kimmy_commits Durable write transactions committed by the storage engine.\n\
             # TYPE kimmy_commits counter\n\
             kimmy_commits {commits}\n\
             # HELP kimmy_fsyncs Times the disk was asked to make something durable: one per commit under durable, one per shared flush under coalesced.\n\
             # TYPE kimmy_fsyncs counter\n\
             kimmy_fsyncs {fsyncs}\n\
             # HELP kimmy_commits_grouped_total Commits made durable by a shared flush rather than their own fsync.\n\
             # TYPE kimmy_commits_grouped_total counter\n\
             kimmy_commits_grouped_total {grouped}\n\
             {writer_wait}\
             # HELP kimmy_write_lock_wait_timeouts_total Writes that gave up waiting for the storage writer inside server.request_timeout_secs; nothing was written and the client was told to retry.\n\
             # TYPE kimmy_write_lock_wait_timeouts_total counter\n\
             kimmy_write_lock_wait_timeouts_total {writer_wait_timeouts}\n\
             # HELP kimmy_write_lock_held_seconds_max The longest any one transaction has held the storage writer since start. Every other write on the node waited behind it.\n\
             # TYPE kimmy_write_lock_held_seconds_max gauge\n\
             kimmy_write_lock_held_seconds_max {writer_hold_max}\n\
             {writer_hold}\
             # HELP kimmy_storage_bytes Size of the database file on disk.\n\
             # TYPE kimmy_storage_bytes gauge\n\
             kimmy_storage_bytes {storage}\n\
             # HELP kimmy_vector_index_cache_bytes Estimated bytes of HNSW graphs held in memory across vector collections. Bounded by vector.index_cache.max_bytes; a graph larger than the whole budget is held anyway.\n\
             # TYPE kimmy_vector_index_cache_bytes gauge\n\
             kimmy_vector_index_cache_bytes {index_cache}\n\
             # HELP kimmy_process_resident_bytes Resident memory of this process as the kernel reports it (VmRSS in /proc/self/status) - the figure a container memory limit is enforced against. Holds storage.cache_bytes, the HNSW graphs, and whatever heap the allocator keeps for reuse after a burst, which is why it does not follow the two byte gauges above down. 0 where /proc is not available.\n\
             # TYPE kimmy_process_resident_bytes gauge\n\
             kimmy_process_resident_bytes {resident}\n\
             # HELP kimmy_process_resident_peak_bytes The most resident memory this process has had at any moment since it started (VmHWM) - what a container limit was reached by, readable after the current figure has come back down. 0 where /proc is not available.\n\
             # TYPE kimmy_process_resident_peak_bytes gauge\n\
             kimmy_process_resident_peak_bytes {resident_peak}\n\
             # HELP kimmy_up Always 1; presence indicates the node is serving.\n\
             # TYPE kimmy_up gauge\n\
             kimmy_up 1\n\
             # HELP kimmy_uptime_seconds Seconds since this process started serving.\n\
             # TYPE kimmy_uptime_seconds gauge\n\
             kimmy_uptime_seconds {uptime}\n\
             # HELP kimmy_runtime_stall_seconds Worst delay a 250 ms timer on the async runtime saw since the last scrape. Above a few tens of milliseconds, something blocked a worker thread - the storage lock or an fsync - and peers may have marked this node down.\n\
             # TYPE kimmy_runtime_stall_seconds gauge\n\
             kimmy_runtime_stall_seconds {stall}\n\
             # HELP kimmy_requests_total HTTP requests handled.\n\
             # TYPE kimmy_requests_total counter\n\
             kimmy_requests_total {requests}\n\
             # HELP kimmy_responses_total HTTP responses by status class.\n\
             # TYPE kimmy_responses_total counter\n\
             kimmy_responses_total{{class=\"2xx\"}} {ok}\n\
             kimmy_responses_total{{class=\"4xx\"}} {client}\n\
             kimmy_responses_total{{class=\"5xx\"}} {server}\n\
             # HELP kimmy_authz_denied_total Operations refused by RBAC.\n\
             # TYPE kimmy_authz_denied_total counter\n\
             kimmy_authz_denied_total {denied}\n\
             # HELP kimmy_auth_failures_total Rejected credentials and tokens.\n\
             # TYPE kimmy_auth_failures_total counter\n\
             kimmy_auth_failures_total {auth}\n\
             # HELP kimmy_rate_limited_total Requests refused by a rate limit.\n\
             # TYPE kimmy_rate_limited_total counter\n\
             kimmy_rate_limited_total {limited}\n\
             # HELP kimmy_rate_limited_principal_total Authenticated requests refused by the per-principal rate limit. Also counted in kimmy_rate_limited_total; the difference is the login limiter.\n\
             # TYPE kimmy_rate_limited_principal_total counter\n\
             kimmy_rate_limited_principal_total {limited_principal}\n\
             # HELP kimmy_backups_total Backups served.\n\
             # TYPE kimmy_backups_total counter\n\
             kimmy_backups_total {backups}\n\
             # HELP kimmy_ttl_expired_total Documents deleted by a TTL index.\n\
             # TYPE kimmy_ttl_expired_total counter\n\
             kimmy_ttl_expired_total {ttl_expired}\n\
             # HELP kimmy_ttl_skipped_total Expiry candidates refused because the document was refreshed before the delete.\n\
             # TYPE kimmy_ttl_skipped_total counter\n\
             kimmy_ttl_skipped_total {ttl_skipped}\n\
             # HELP kimmy_index_unkeyed_total Documents stored under an index that could not key them - arrays at two of a compound index's paths, more than 1000 keys, or a Decimal128 - and are rechecked on every scan of that index instead. Each one is logged at warning naming the index and the document; the index listing reports how many stand under each index.\n\
             # TYPE kimmy_index_unkeyed_total counter\n\
             kimmy_index_unkeyed_total {index_unkeyed}\n\
             # HELP kimmy_webhook_deliveries_total Webhook delivery attempts by outcome.\n\
             # TYPE kimmy_webhook_deliveries_total counter\n\
             kimmy_webhook_deliveries_total{{outcome=\"delivered\"}} {wh_ok}\n\
             kimmy_webhook_deliveries_total{{outcome=\"failed\"}} {wh_fail}\n\
             # HELP kimmy_webhook_events_total Change events pushed to endpoints.\n\
             # TYPE kimmy_webhook_events_total counter\n\
             kimmy_webhook_events_total {wh_events}\n\
             # HELP kimmy_webhook_subscriptions Registered subscriptions, as this node sees the registry.\n\
             # TYPE kimmy_webhook_subscriptions gauge\n\
             kimmy_webhook_subscriptions{{state=\"active\"}} {wh_active}\n\
             kimmy_webhook_subscriptions{{state=\"invalidated\"}} {wh_invalid}\n\
             # HELP kimmy_webhook_backlog_seconds Age of the oldest undelivered event, across subscriptions this node owns.\n\
             # TYPE kimmy_webhook_backlog_seconds gauge\n\
             kimmy_webhook_backlog_seconds {wh_backlog}\n\
             # HELP kimmy_cluster_members Peers this node's SWIM membership currently considers alive. 0 with clustering off.\n\
             # TYPE kimmy_cluster_members gauge\n\
             kimmy_cluster_members {cluster}\n\
             # HELP kimmy_replication_lag_seconds Seconds since the newest peer entry applied locally where a peer holds newer, max over peers in the last sync round. 0 when caught up or clustering is off.\n\
             # TYPE kimmy_replication_lag_seconds gauge\n\
             kimmy_replication_lag_seconds {lag}\n\
             # HELP kimmy_sync_failures_total Anti-entropy rounds against a peer that failed, any cause: unreachable, refused, or a batch this node could not apply. Rising while kimmy_replication_lag_seconds sits at 0 is a wedged peer, not a healthy one - a failed round reports no lag.\n\
             # TYPE kimmy_sync_failures_total counter\n\
             kimmy_sync_failures_total {sync_failures}\n\
             # HELP kimmy_sync_peers_backing_off Peers this node is currently backing off from after failed rounds. 0 when every peer answered its last round or clustering is off.\n\
             # TYPE kimmy_sync_peers_backing_off gauge\n\
             kimmy_sync_peers_backing_off {sync_backing_off}\n\
             # HELP kimmy_sync_ddl_refused_total Replicated schema changes this node could not apply to its own data and skipped - an index its peers hold and it does not. Each one is logged at warning with the reason.\n\
             # TYPE kimmy_sync_ddl_refused_total counter\n\
             kimmy_sync_ddl_refused_total {sync_ddl_refused}\n\
             # HELP kimmy_sync_ddl_declined_total Replicated index drops this node declined as older than the index standing under the name here, and had not already recorded. A drop applied when it was current leaves a tombstone, so a re-served window carrying it past the recreation it preceded is a replay and is not counted. What is counted is a drop this member has never seen - a member whose clock ran ahead when it created the index, which is now the only member still holding it; drop it directly on that member.\n\
             # TYPE kimmy_sync_ddl_declined_total counter\n\
             kimmy_sync_ddl_declined_total {sync_ddl_declined}\n\
             # HELP kimmy_sync_divergent_collections Collections a periodic cross-member check currently finds disagreeing with a peer - held there and not here, or held by both with a different document count - confirmed on two checks running. 0 on a converged cluster. Moves for a divergence that leaves every other sync series reading healthy, because nothing about it fails a round.\n\
             # TYPE kimmy_sync_divergent_collections gauge\n\
             kimmy_sync_divergent_collections {sync_divergent}\n\
             # HELP kimmy_sync_divergence_checks_total Contacts with a peer in which the cross-member divergence check above ran, and rounds that did not run it - completed with the pull truncated by the batch cap, or failed. kimmy_sync_divergent_collections reading 0 is evidence that the peers agree only while ran is rising; ran flat while skipped rises means nothing looked, which a bare 0 cannot say. A round that failed is counted in kimmy_sync_failures_total and here as skipped, so ran plus skipped is every round attempted.\n\
             # TYPE kimmy_sync_divergence_checks_total counter\n\
             kimmy_sync_divergence_checks_total{{outcome=\"ran\"}} {sync_div_ran}\n\
             kimmy_sync_divergence_checks_total{{outcome=\"skipped\"}} {sync_div_skipped}\n\
             # HELP kimmy_sync_divergence_count_probes_total Checked contacts in which the document-count half of the check compared the probed collection's count against the peer's, and checked contacts in which it was deferred because one member was behind the other and still catching up. compared flat while ran rises means no document count has been compared against any peer, whatever the gauge reads. A member that is behind but whose position has not moved for {frozen} consecutive checked contacts is compared regardless, so a member whose replication has stopped is not deferred for as long as it stays stopped.\n\
             # TYPE kimmy_sync_divergence_count_probes_total counter\n\
             kimmy_sync_divergence_count_probes_total{{outcome=\"compared\"}} {sync_div_compared}\n\
             kimmy_sync_divergence_count_probes_total{{outcome=\"deferred\"}} {sync_div_deferred}\n\
             # HELP kimmy_sync_divergence_check_age_seconds Seconds since the last contact, with any peer, in which the cross-member divergence check ran, computed when this page is read. 0 before the first such contact, when ran is also 0. Above a few multiples of cluster.sync_interval_secs, kimmy_sync_divergent_collections is holding a value nothing has re-examined, whether the rounds are failing or the loop itself is stuck - look at kimmy_sync_failures_total, kimmy_sync_peers_backing_off and kimmy_write_lock_wait_seconds.\n\
             # TYPE kimmy_sync_divergence_check_age_seconds gauge\n\
             kimmy_sync_divergence_check_age_seconds {sync_div_age}\n\
             # HELP kimmy_sync_entries_skipped_total Replicated entries a sync round left rather than took. unknown_collection: batches stopped at an entry for a collection this node has no record of - neither holding it nor a tombstone for it - because its creation was witnessed here without being applied, or has aged out of the peer's oplog; one per stopped batch, the window is re-served from the same place every round, and the round plans a snapshot from the peer to bring the collection. A collection dropped here is history instead and stops nothing. beyond_advertised: entries above the vector the peer advertised before serving the window, left for the next round, which asks for them from the right position; ordinary and rare on a busy cluster. A hole of either kind reads 0 on kimmy_replication_lag_seconds; this and kimmy_sync_divergent_collections are what move.\n\
             # TYPE kimmy_sync_entries_skipped_total counter\n\
             kimmy_sync_entries_skipped_total{{reason=\"unknown_collection\"}} {sync_skipped_unknown}\n\
             kimmy_sync_entries_skipped_total{{reason=\"beyond_advertised\"}} {sync_skipped_beyond}\n\
             # HELP kimmy_sync_held_marks_released_total Entries this node held as state - written by a snapshot page, a carried delete or a scoped repair, above the vector it advertises - that arrived in a sync window served contiguously from its position and were released: the mark removed and both vectors raised over the entry. One per entry, counted when the batch commits. The release path itself: the beyond_advertised reason of kimmy_sync_entries_skipped_total rises on a peer while these entries are held and also for the ordinary race, and only this tells the two apart.\n\
             # TYPE kimmy_sync_held_marks_released_total counter\n\
             kimmy_sync_held_marks_released_total {sync_held_released}\n\
             # HELP kimmy_sync_repair_rounds_total Sync rounds spent repairing against a peer: re-serving its oplog from the divergent collection's creation, or pulling its snapshot, after the divergence check confirmed a collection against it or a batch stopped at a collection this node lacks. Rising is a repair under way; it stops when the repair reaches the peer's tail.\n\
             # TYPE kimmy_sync_repair_rounds_total counter\n\
             kimmy_sync_repair_rounds_total {sync_repair_rounds}\n\
             # HELP kimmy_tls_reloads_total Certificate reload attempts by outcome. A failed reload leaves the certificate already in use serving.\n\
             # TYPE kimmy_tls_reloads_total counter\n\
             kimmy_tls_reloads_total{{outcome=\"ok\"}} {tls_ok}\n\
             kimmy_tls_reloads_total{{outcome=\"failed\"}} {tls_fail}\n\
             # HELP kimmy_jwks_refresh_total Attempts to refresh the OIDC provider's signing keys, by outcome. A failed refresh leaves the key set already in use verifying.\n\
             # TYPE kimmy_jwks_refresh_total counter\n\
             kimmy_jwks_refresh_total{{outcome=\"ok\"}} {jwks_ok}\n\
             kimmy_jwks_refresh_total{{outcome=\"failed\"}} {jwks_fail}\n\
             # HELP kimmy_embed_documents_total Documents whose vectors this node wrote.\n\
             # TYPE kimmy_embed_documents_total counter\n\
             kimmy_embed_documents_total {embed_docs}\n\
             # HELP kimmy_embed_chunks_total Provider inputs embedded - the closest proxy for provider spend.\n\
             # TYPE kimmy_embed_chunks_total counter\n\
             kimmy_embed_chunks_total {embed_chunks}\n\
             # HELP kimmy_embed_deferred_total Foreign-written documents held for a later re-check.\n\
             # TYPE kimmy_embed_deferred_total counter\n\
             kimmy_embed_deferred_total {embed_deferred}\n\
             # HELP kimmy_embed_skipped_not_owned_total Documents dropped un-embedded because another node owns embedding - the duplicate provider calls this counts replacing is the 3x amplification measured in the August 2026 load test.\n\
             # TYPE kimmy_embed_skipped_not_owned_total counter\n\
             kimmy_embed_skipped_not_owned_total {embed_not_owned}\n\
             # HELP kimmy_embed_failures_total Failed provider calls, including each retry. Climbing while embed_documents stays flat is a provider outage.\n\
             # TYPE kimmy_embed_failures_total counter\n\
             kimmy_embed_failures_total {embed_failures}\n\
             # HELP kimmy_embed_provider_errors_total Provider calls that failed before a response, by what failed: connect (DNS, TCP, TLS), timeout, reset (the far side closed an open connection), other.\n\
             # TYPE kimmy_embed_provider_errors_total counter\n\
             kimmy_embed_provider_errors_total{{kind=\"connect\"}} {t_connect}\n\
             kimmy_embed_provider_errors_total{{kind=\"timeout\"}} {t_timeout}\n\
             kimmy_embed_provider_errors_total{{kind=\"reset\"}} {t_reset}\n\
             kimmy_embed_provider_errors_total{{kind=\"other\"}} {t_other}\n\
             # HELP kimmy_embed_provider_requests_total Embedding provider calls answered, documents and search queries alike - compare with the provider's own request count.\n\
             # TYPE kimmy_embed_provider_requests_total counter\n\
             kimmy_embed_provider_requests_total {p_requests}\n\
             # HELP kimmy_embed_provider_tokens_total Input tokens the embedding provider reported billing for - the number a metered provider's invoice is made of. Zero for providers that report none.\n\
             # TYPE kimmy_embed_provider_tokens_total counter\n\
             kimmy_embed_provider_tokens_total {p_tokens}\n",
            databases = readings.databases,
            collections = readings.collections,
            violations = readings.unique_violations,
            commits = readings.commits,
            fsyncs = readings.fsyncs,
            grouped = readings.commits_grouped,
            writer_wait = render_writer_wait(&readings.writer_wait),
            writer_hold = render_writer_hold(&readings.writer_hold),
            writer_wait_timeouts = readings.writer_wait_timeouts,
            writer_hold_max = readings.writer_hold_max_us as f64 / 1e6,
            storage = readings.storage_bytes,
            index_cache = readings.vector_index_cache_bytes,
            resident = readings.process_resident_bytes,
            resident_peak = readings.process_resident_peak_bytes,
            p_requests = kimmy_vector::provider_totals().0,
            p_tokens = kimmy_vector::provider_totals().1,
            uptime = self.uptime_secs(),
            stall = self.take_runtime_stall_secs(),
            requests = self.get(&self.requests),
            ok = self.get(&self.responses_2xx),
            client = self.get(&self.responses_4xx),
            server = self.get(&self.responses_5xx),
            denied = self.get(&self.authz_denied),
            auth = self.get(&self.auth_failures),
            limited = self.get(&self.rate_limited),
            limited_principal = self.get(&self.rate_limited_principal),
            backups = self.get(&self.backups),
            wh_ok = self.get(&self.webhook_delivered),
            wh_fail = self.get(&self.webhook_failed),
            wh_events = self.get(&self.webhook_events),
            ttl_expired = self.get(&self.ttl_expired),
            ttl_skipped = self.get(&self.ttl_skipped),
            index_unkeyed = readings.index_unkeyed,
            wh_active = self.get(&self.webhook_active),
            wh_invalid = self.get(&self.webhook_invalidated),
            wh_backlog = self.get(&self.webhook_backlog_secs),
            cluster = self.get(&self.cluster_members),
            lag = self.get(&self.replication_lag_secs),
            sync_failures = self.get(&self.sync_failures),
            sync_backing_off = self.get(&self.sync_peers_backing_off),
            sync_ddl_refused = self.get(&self.sync_ddl_refused),
            sync_ddl_declined = self.get(&self.sync_ddl_declined),
            sync_divergent = self.get(&self.sync_divergent_collections),
            sync_div_ran = self.get(&self.sync_divergence_checks),
            sync_div_skipped = self.get(&self.sync_divergence_skips),
            sync_div_compared = self.get(&self.sync_divergence_count_compared),
            sync_div_deferred = self.get(&self.sync_divergence_count_deferred),
            sync_div_age = self.sync_divergence_check_age_secs_at(now),
            sync_skipped_unknown = self.get(&self.sync_entries_skipped_unknown_collection),
            sync_skipped_beyond = self.get(&self.sync_entries_skipped_beyond_advertised),
            sync_held_released = readings.held_marks_released,
            sync_repair_rounds = self.get(&self.sync_repair_rounds),
            frozen = kimmy_cluster::FROZEN_CONTACTS,
            tls_ok = self.get(&self.tls_reloads_ok),
            tls_fail = self.get(&self.tls_reloads_failed),
            embed_docs = embed_docs,
            embed_chunks = embed_chunks,
            embed_deferred = embed_deferred,
            embed_not_owned = embed_not_owned,
            embed_failures = embed_failures,
            jwks_ok = self.get(&self.jwks_refresh_ok),
            jwks_fail = self.get(&self.jwks_refresh_failed),
        );
        self.render_latency(&mut out);
        out
    }

    /// The latency histogram, in Prometheus's cumulative-bucket form.
    ///
    /// Buckets are stored non-cumulative and summed here, and the `le` labels
    /// are the microsecond bounds converted to seconds — `f64` prints `0.0001`
    /// and `10` exactly for every bound in the table, so the labels stay
    /// stable strings rather than formatting artifacts.
    fn render_latency(&self, out: &mut String) {
        use std::fmt::Write;

        out.push_str(
            "# HELP kimmy_request_duration_seconds End-to-end request latency. Health and \
             metrics routes are excluded, so scrapes do not crowd the buckets the real \
             traffic lands in.\n\
             # TYPE kimmy_request_duration_seconds histogram\n",
        );
        let mut cumulative = 0u64;
        for (slot, upper) in LATENCY_BUCKETS_US.iter().enumerate() {
            cumulative += self.get(&self.latency_buckets[slot]);
            let le = *upper as f64 / 1e6;
            let _ =
                writeln!(out, "kimmy_request_duration_seconds_bucket{{le=\"{le}\"}} {cumulative}");
        }
        let count = self.get(&self.latency_count);
        let sum = self.get(&self.latency_sum_us) as f64 / 1e6;
        let _ = writeln!(out, "kimmy_request_duration_seconds_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(out, "kimmy_request_duration_seconds_sum {sum}");
        let _ = writeln!(out, "kimmy_request_duration_seconds_count {count}");
    }
}

/// The writer-wait histogram, in Prometheus's cumulative-bucket form
/// (ADR-151); the same shape as `render_latency`, from the engine's buckets.
fn render_writer_wait(wait: &kimmy_storage::WriterWaitSnapshot) -> String {
    use std::fmt::Write;

    let mut out = String::from(
        "# HELP kimmy_write_lock_wait_seconds How long a write transaction waited for the storage writer before it could begin. The part of a write's latency that is not its own work: a bulk, a repair or a retention pass holding the writer shows here on every other write.\n\
         # TYPE kimmy_write_lock_wait_seconds histogram\n",
    );
    let mut cumulative = 0u64;
    for (slot, upper) in kimmy_storage::WRITER_WAIT_BUCKETS_US.iter().enumerate() {
        cumulative += wait.buckets[slot];
        let le = *upper as f64 / 1e6;
        let _ = writeln!(out, "kimmy_write_lock_wait_seconds_bucket{{le=\"{le}\"}} {cumulative}");
    }
    let sum = wait.sum_us as f64 / 1e6;
    let _ = writeln!(out, "kimmy_write_lock_wait_seconds_bucket{{le=\"+Inf\"}} {}", wait.count);
    let _ = writeln!(out, "kimmy_write_lock_wait_seconds_sum {sum}");
    let _ = writeln!(out, "kimmy_write_lock_wait_seconds_count {}", wait.count);
    out
}

/// The writer-hold histogram, one row per holder (ADR-159).
///
/// The same cumulative-bucket shape as `render_writer_wait`, carrying the
/// `holder` label. Every holder is rendered whether or not it has held the
/// writer yet, for the reason the whole page renders counters at zero: a
/// dashboard split by holder must not gain a series the first time a
/// retention pass removes something.
fn render_writer_hold(hold: &kimmy_storage::WriterHoldSnapshot) -> String {
    use std::fmt::Write;

    let mut out = String::from(
        "# HELP kimmy_write_lock_held_seconds How long a transaction held the storage writer, by what held it. Every other write on the node waited behind the hold, so this is the cause kimmy_write_lock_wait_seconds is the effect of.\n\
         # TYPE kimmy_write_lock_held_seconds histogram\n",
    );
    for holder in kimmy_storage::WriterHolder::ALL {
        let row = holder.slot();
        let label = holder.label();
        let mut cumulative = 0u64;
        for (slot, upper) in kimmy_storage::WRITER_HOLD_BUCKETS_US.iter().enumerate() {
            cumulative += hold.buckets[row][slot];
            let le = *upper as f64 / 1e6;
            let _ = writeln!(
                out,
                "kimmy_write_lock_held_seconds_bucket{{holder=\"{label}\",le=\"{le}\"}} {cumulative}"
            );
        }
        let count = hold.count[row];
        let sum = hold.sum_us[row] as f64 / 1e6;
        let _ = writeln!(
            out,
            "kimmy_write_lock_held_seconds_bucket{{holder=\"{label}\",le=\"+Inf\"}} {count}"
        );
        let _ = writeln!(out, "kimmy_write_lock_held_seconds_sum{{holder=\"{label}\"}} {sum}");
        let _ = writeln!(out, "kimmy_write_lock_held_seconds_count{{holder=\"{label}\"}} {count}");
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// One instance with every counter at a value nothing else has.
    ///
    /// Distinct on purpose: with several counters sharing a value, a render
    /// that printed the wrong one would still match.
    ///
    /// `now` is the moment the render or snapshot under test is taken at:
    /// the divergence check's age is computed at the read, from an instant
    /// the loop reported (ADR-154), so the instant recorded here is placed
    /// relative to it.
    fn every_counter_distinct(now: Instant) -> Metrics {
        let m = Metrics::default();
        // 2 × 2xx, 3 × 4xx (one each of 401/403/429), 4 × 5xx, and one 304
        // that lands in the total alone: ten requests, no two classes equal.
        for _ in 0..2 {
            m.record_request(200);
        }
        m.record_request(401);
        m.record_request(403);
        m.record_request(429);
        for _ in 0..4 {
            m.record_request(500);
        }
        m.record_request(304);

        // Five refusals by the per-principal limit. Not paired with five 429s
        // above on purpose: the two series are recorded from different places,
        // and a render that printed one for the other must not match.
        for _ in 0..5 {
            m.record_principal_rate_limited();
        }

        m.record_backup();
        m.record_expiry(11, 12);
        m.record_webhook_delivery(true, 13);
        m.record_webhook_delivery(true, 14);
        m.record_webhook_delivery(false, 0);
        m.set_webhook_gauges(15, 16, 17);
        m.set_cluster_members(18);
        m.set_replication_lag_secs(19);
        // Two ticks: the counters accumulate, the backoff level, the
        // divergence count and the check's instant are each replaced. A
        // render that printed the first tick's level, or a level that
        // accumulated, would not match: the age is 71 at `now` only if the
        // second tick's instant is the one kept.
        // Every field written out rather than `..Default::default()`: a
        // field added to the report must break this and be given a distinct
        // value here, not default silently into the golden below.
        m.record_sync_round(&kimmy_cluster::RoundReport {
            failed: 20,
            backing_off: 99,
            ddl_refused: 21,
            ddl_declined: 28,
            divergent_collections: 12,
            divergence_checks: 30,
            divergence_skips: 33,
            divergence_count_compared: 61,
            divergence_count_deferred: 64,
            divergence_last_check: Some(now - Duration::from_secs(70)),
            entries_skipped_unknown_collection: 72,
            entries_skipped_beyond_advertised: 74,
            repair_rounds: 76,
        });
        m.record_sync_round(&kimmy_cluster::RoundReport {
            failed: 3,
            backing_off: 24,
            ddl_refused: 4,
            ddl_declined: 8,
            divergent_collections: 5,
            divergence_checks: 2,
            divergence_skips: 1,
            divergence_count_compared: 2,
            divergence_count_deferred: 3,
            divergence_last_check: Some(now - Duration::from_secs(71)),
            entries_skipped_unknown_collection: 1,
            entries_skipped_beyond_advertised: 1,
            repair_rounds: 1,
        });
        for _ in 0..20 {
            m.record_tls_reload(true);
        }
        m.record_tls_reload(false);
        for _ in 0..22 {
            m.record_jwks_refresh(true);
        }
        m.record_jwks_refresh(false);

        // One observation in three different buckets, so the cumulative sum is
        // visible in the golden text rather than being three copies of 1.
        m.record_latency(Duration::from_micros(90));
        m.record_latency(Duration::from_micros(400));
        m.record_latency(Duration::from_millis(30));
        m
    }

    /// The hold histogram with every holder's row distinct from every
    /// other's, so a row rendered under the wrong label cannot match the
    /// golden (ADR-159).
    ///
    /// Each holder lands `slot + 1` holds in the bucket its own slot picks,
    /// one more in the top bucket, and one hold above every bound — so the
    /// cumulative sum, the `+Inf` overflow and the `holder` label are each
    /// visible in the golden text rather than inferred from it.
    fn distinct_hold() -> kimmy_storage::WriterHoldSnapshot {
        let top = kimmy_storage::WRITER_HOLD_BUCKETS_US.len() - 1;
        let mut hold = kimmy_storage::WriterHoldSnapshot::default();
        for holder in kimmy_storage::WriterHolder::ALL {
            let row = holder.slot();
            hold.buckets[row][row % (top + 1)] += row as u64 + 1;
            hold.buckets[row][top] += 1;
            hold.count[row] = row as u64 + 3;
            hold.sum_us[row] = (row as u64 + 1) * 1_500_000;
        }
        hold
    }

    /// The engine's readings, every one distinct from every counter above
    /// and from each other, so a reading rendered under another's name
    /// cannot match the golden.
    fn distinct_readings() -> StorageReadings {
        StorageReadings {
            databases: 41,
            collections: 42,
            unique_violations: 43,
            commits: 44,
            fsyncs: 45,
            commits_grouped: 46,
            storage_bytes: 47,
            vector_index_cache_bytes: 48,
            process_resident_bytes: 49,
            process_resident_peak_bytes: 50,
            index_unkeyed: 26,
            writer_wait: kimmy_storage::WriterWaitSnapshot {
                buckets: [1, 2, 0, 0, 3, 0, 0, 1],
                count: 8,
                sum_us: 6_500_000,
            },
            writer_wait_timeouts: 51,
            writer_hold_max_us: 52_500_000,
            held_marks_released: 53,
            // One holder per row, none of them equal, so a row rendered
            // under another holder's label cannot match the golden. The
            // counts are the buckets' sum, as a real snapshot's are.
            writer_hold: distinct_hold(),
        }
    }

    /// **Production clusters scrape this endpoint. Any diff is a
    /// regression** — a renamed series is a dashboard that goes blank and an
    /// alert that stops firing, and neither announces itself.
    ///
    /// A whole-string comparison rather than a set of `contains` assertions,
    /// because the failure this guards against is the one `contains` cannot
    /// see: a series *added*, a HELP line reworded, a blank line appearing
    /// between two samples. `render` is fully deterministic in a test —
    /// `uptime_secs` is 0 on a fresh instance, and the one other series that
    /// is a subtraction against a clock, the divergence check's age, is
    /// read at an instant this test chooses (ADR-154) — so there is no
    /// reason to check it loosely.
    ///
    /// If this fails because you meant to change the output, read the diff as
    /// the release note it is: every line here is something a scrape config or
    /// a dashboard may name.
    #[test]
    fn the_render_is_byte_for_byte_what_a_scrape_receives() {
        let expected = "\
# HELP kimmy_databases Number of databases.
# TYPE kimmy_databases gauge
kimmy_databases 41
# HELP kimmy_collections Number of collections across all databases.
# TYPE kimmy_collections gauge
kimmy_collections 42
# HELP kimmy_unique_violations Unique constraints broken by merging replicated writes.
# TYPE kimmy_unique_violations counter
kimmy_unique_violations 43
# HELP kimmy_commits Durable write transactions committed by the storage engine.
# TYPE kimmy_commits counter
kimmy_commits 44
# HELP kimmy_fsyncs Times the disk was asked to make something durable: one per commit under durable, one per shared flush under coalesced.
# TYPE kimmy_fsyncs counter
kimmy_fsyncs 45
# HELP kimmy_commits_grouped_total Commits made durable by a shared flush rather than their own fsync.
# TYPE kimmy_commits_grouped_total counter
kimmy_commits_grouped_total 46
# HELP kimmy_write_lock_wait_seconds How long a write transaction waited for the storage writer before it could begin. The part of a write's latency that is not its own work: a bulk, a repair or a retention pass holding the writer shows here on every other write.
# TYPE kimmy_write_lock_wait_seconds histogram
kimmy_write_lock_wait_seconds_bucket{le=\"0.001\"} 1
kimmy_write_lock_wait_seconds_bucket{le=\"0.005\"} 3
kimmy_write_lock_wait_seconds_bucket{le=\"0.025\"} 3
kimmy_write_lock_wait_seconds_bucket{le=\"0.1\"} 3
kimmy_write_lock_wait_seconds_bucket{le=\"0.5\"} 6
kimmy_write_lock_wait_seconds_bucket{le=\"1\"} 6
kimmy_write_lock_wait_seconds_bucket{le=\"5\"} 6
kimmy_write_lock_wait_seconds_bucket{le=\"30\"} 7
kimmy_write_lock_wait_seconds_bucket{le=\"+Inf\"} 8
kimmy_write_lock_wait_seconds_sum 6.5
kimmy_write_lock_wait_seconds_count 8
# HELP kimmy_write_lock_wait_timeouts_total Writes that gave up waiting for the storage writer inside server.request_timeout_secs; nothing was written and the client was told to retry.
# TYPE kimmy_write_lock_wait_timeouts_total counter
kimmy_write_lock_wait_timeouts_total 51
# HELP kimmy_write_lock_held_seconds_max The longest any one transaction has held the storage writer since start. Every other write on the node waited behind it.
# TYPE kimmy_write_lock_held_seconds_max gauge
kimmy_write_lock_held_seconds_max 52.5
# HELP kimmy_write_lock_held_seconds How long a transaction held the storage writer, by what held it. Every other write on the node waited behind the hold, so this is the cause kimmy_write_lock_wait_seconds is the effect of.
# TYPE kimmy_write_lock_held_seconds histogram
kimmy_write_lock_held_seconds_bucket{holder=\"write\",le=\"0.001\"} 1
kimmy_write_lock_held_seconds_bucket{holder=\"write\",le=\"0.01\"} 1
kimmy_write_lock_held_seconds_bucket{holder=\"write\",le=\"0.1\"} 1
kimmy_write_lock_held_seconds_bucket{holder=\"write\",le=\"1\"} 1
kimmy_write_lock_held_seconds_bucket{holder=\"write\",le=\"5\"} 1
kimmy_write_lock_held_seconds_bucket{holder=\"write\",le=\"30\"} 1
kimmy_write_lock_held_seconds_bucket{holder=\"write\",le=\"300\"} 2
kimmy_write_lock_held_seconds_bucket{holder=\"write\",le=\"+Inf\"} 3
kimmy_write_lock_held_seconds_sum{holder=\"write\"} 1.5
kimmy_write_lock_held_seconds_count{holder=\"write\"} 3
kimmy_write_lock_held_seconds_bucket{holder=\"bulk\",le=\"0.001\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"bulk\",le=\"0.01\"} 2
kimmy_write_lock_held_seconds_bucket{holder=\"bulk\",le=\"0.1\"} 2
kimmy_write_lock_held_seconds_bucket{holder=\"bulk\",le=\"1\"} 2
kimmy_write_lock_held_seconds_bucket{holder=\"bulk\",le=\"5\"} 2
kimmy_write_lock_held_seconds_bucket{holder=\"bulk\",le=\"30\"} 2
kimmy_write_lock_held_seconds_bucket{holder=\"bulk\",le=\"300\"} 3
kimmy_write_lock_held_seconds_bucket{holder=\"bulk\",le=\"+Inf\"} 4
kimmy_write_lock_held_seconds_sum{holder=\"bulk\"} 3
kimmy_write_lock_held_seconds_count{holder=\"bulk\"} 4
kimmy_write_lock_held_seconds_bucket{holder=\"ddl\",le=\"0.001\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"ddl\",le=\"0.01\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"ddl\",le=\"0.1\"} 3
kimmy_write_lock_held_seconds_bucket{holder=\"ddl\",le=\"1\"} 3
kimmy_write_lock_held_seconds_bucket{holder=\"ddl\",le=\"5\"} 3
kimmy_write_lock_held_seconds_bucket{holder=\"ddl\",le=\"30\"} 3
kimmy_write_lock_held_seconds_bucket{holder=\"ddl\",le=\"300\"} 4
kimmy_write_lock_held_seconds_bucket{holder=\"ddl\",le=\"+Inf\"} 5
kimmy_write_lock_held_seconds_sum{holder=\"ddl\"} 4.5
kimmy_write_lock_held_seconds_count{holder=\"ddl\"} 5
kimmy_write_lock_held_seconds_bucket{holder=\"index_build\",le=\"0.001\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"index_build\",le=\"0.01\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"index_build\",le=\"0.1\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"index_build\",le=\"1\"} 4
kimmy_write_lock_held_seconds_bucket{holder=\"index_build\",le=\"5\"} 4
kimmy_write_lock_held_seconds_bucket{holder=\"index_build\",le=\"30\"} 4
kimmy_write_lock_held_seconds_bucket{holder=\"index_build\",le=\"300\"} 5
kimmy_write_lock_held_seconds_bucket{holder=\"index_build\",le=\"+Inf\"} 6
kimmy_write_lock_held_seconds_sum{holder=\"index_build\"} 6
kimmy_write_lock_held_seconds_count{holder=\"index_build\"} 6
kimmy_write_lock_held_seconds_bucket{holder=\"drop\",le=\"0.001\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"drop\",le=\"0.01\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"drop\",le=\"0.1\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"drop\",le=\"1\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"drop\",le=\"5\"} 5
kimmy_write_lock_held_seconds_bucket{holder=\"drop\",le=\"30\"} 5
kimmy_write_lock_held_seconds_bucket{holder=\"drop\",le=\"300\"} 6
kimmy_write_lock_held_seconds_bucket{holder=\"drop\",le=\"+Inf\"} 7
kimmy_write_lock_held_seconds_sum{holder=\"drop\"} 7.5
kimmy_write_lock_held_seconds_count{holder=\"drop\"} 7
kimmy_write_lock_held_seconds_bucket{holder=\"replication\",le=\"0.001\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"replication\",le=\"0.01\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"replication\",le=\"0.1\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"replication\",le=\"1\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"replication\",le=\"5\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"replication\",le=\"30\"} 6
kimmy_write_lock_held_seconds_bucket{holder=\"replication\",le=\"300\"} 7
kimmy_write_lock_held_seconds_bucket{holder=\"replication\",le=\"+Inf\"} 8
kimmy_write_lock_held_seconds_sum{holder=\"replication\"} 9
kimmy_write_lock_held_seconds_count{holder=\"replication\"} 8
kimmy_write_lock_held_seconds_bucket{holder=\"repair\",le=\"0.001\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"repair\",le=\"0.01\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"repair\",le=\"0.1\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"repair\",le=\"1\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"repair\",le=\"5\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"repair\",le=\"30\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"repair\",le=\"300\"} 8
kimmy_write_lock_held_seconds_bucket{holder=\"repair\",le=\"+Inf\"} 9
kimmy_write_lock_held_seconds_sum{holder=\"repair\"} 10.5
kimmy_write_lock_held_seconds_count{holder=\"repair\"} 9
kimmy_write_lock_held_seconds_bucket{holder=\"retention\",le=\"0.001\"} 8
kimmy_write_lock_held_seconds_bucket{holder=\"retention\",le=\"0.01\"} 8
kimmy_write_lock_held_seconds_bucket{holder=\"retention\",le=\"0.1\"} 8
kimmy_write_lock_held_seconds_bucket{holder=\"retention\",le=\"1\"} 8
kimmy_write_lock_held_seconds_bucket{holder=\"retention\",le=\"5\"} 8
kimmy_write_lock_held_seconds_bucket{holder=\"retention\",le=\"30\"} 8
kimmy_write_lock_held_seconds_bucket{holder=\"retention\",le=\"300\"} 9
kimmy_write_lock_held_seconds_bucket{holder=\"retention\",le=\"+Inf\"} 10
kimmy_write_lock_held_seconds_sum{holder=\"retention\"} 12
kimmy_write_lock_held_seconds_count{holder=\"retention\"} 10
kimmy_write_lock_held_seconds_bucket{holder=\"expiry\",le=\"0.001\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"expiry\",le=\"0.01\"} 9
kimmy_write_lock_held_seconds_bucket{holder=\"expiry\",le=\"0.1\"} 9
kimmy_write_lock_held_seconds_bucket{holder=\"expiry\",le=\"1\"} 9
kimmy_write_lock_held_seconds_bucket{holder=\"expiry\",le=\"5\"} 9
kimmy_write_lock_held_seconds_bucket{holder=\"expiry\",le=\"30\"} 9
kimmy_write_lock_held_seconds_bucket{holder=\"expiry\",le=\"300\"} 10
kimmy_write_lock_held_seconds_bucket{holder=\"expiry\",le=\"+Inf\"} 11
kimmy_write_lock_held_seconds_sum{holder=\"expiry\"} 13.5
kimmy_write_lock_held_seconds_count{holder=\"expiry\"} 11
kimmy_write_lock_held_seconds_bucket{holder=\"embedding\",le=\"0.001\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"embedding\",le=\"0.01\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"embedding\",le=\"0.1\"} 10
kimmy_write_lock_held_seconds_bucket{holder=\"embedding\",le=\"1\"} 10
kimmy_write_lock_held_seconds_bucket{holder=\"embedding\",le=\"5\"} 10
kimmy_write_lock_held_seconds_bucket{holder=\"embedding\",le=\"30\"} 10
kimmy_write_lock_held_seconds_bucket{holder=\"embedding\",le=\"300\"} 11
kimmy_write_lock_held_seconds_bucket{holder=\"embedding\",le=\"+Inf\"} 12
kimmy_write_lock_held_seconds_sum{holder=\"embedding\"} 15
kimmy_write_lock_held_seconds_count{holder=\"embedding\"} 12
kimmy_write_lock_held_seconds_bucket{holder=\"durability\",le=\"0.001\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"durability\",le=\"0.01\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"durability\",le=\"0.1\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"durability\",le=\"1\"} 11
kimmy_write_lock_held_seconds_bucket{holder=\"durability\",le=\"5\"} 11
kimmy_write_lock_held_seconds_bucket{holder=\"durability\",le=\"30\"} 11
kimmy_write_lock_held_seconds_bucket{holder=\"durability\",le=\"300\"} 12
kimmy_write_lock_held_seconds_bucket{holder=\"durability\",le=\"+Inf\"} 13
kimmy_write_lock_held_seconds_sum{holder=\"durability\"} 16.5
kimmy_write_lock_held_seconds_count{holder=\"durability\"} 13
kimmy_write_lock_held_seconds_bucket{holder=\"rewind\",le=\"0.001\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"rewind\",le=\"0.01\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"rewind\",le=\"0.1\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"rewind\",le=\"1\"} 0
kimmy_write_lock_held_seconds_bucket{holder=\"rewind\",le=\"5\"} 12
kimmy_write_lock_held_seconds_bucket{holder=\"rewind\",le=\"30\"} 12
kimmy_write_lock_held_seconds_bucket{holder=\"rewind\",le=\"300\"} 13
kimmy_write_lock_held_seconds_bucket{holder=\"rewind\",le=\"+Inf\"} 14
kimmy_write_lock_held_seconds_sum{holder=\"rewind\"} 18
kimmy_write_lock_held_seconds_count{holder=\"rewind\"} 14
# HELP kimmy_storage_bytes Size of the database file on disk.
# TYPE kimmy_storage_bytes gauge
kimmy_storage_bytes 47
# HELP kimmy_vector_index_cache_bytes Estimated bytes of HNSW graphs held in memory across vector collections. Bounded by vector.index_cache.max_bytes; a graph larger than the whole budget is held anyway.
# TYPE kimmy_vector_index_cache_bytes gauge
kimmy_vector_index_cache_bytes 48
# HELP kimmy_process_resident_bytes Resident memory of this process as the kernel reports it (VmRSS in /proc/self/status) - the figure a container memory limit is enforced against. Holds storage.cache_bytes, the HNSW graphs, and whatever heap the allocator keeps for reuse after a burst, which is why it does not follow the two byte gauges above down. 0 where /proc is not available.
# TYPE kimmy_process_resident_bytes gauge
kimmy_process_resident_bytes 49
# HELP kimmy_process_resident_peak_bytes The most resident memory this process has had at any moment since it started (VmHWM) - what a container limit was reached by, readable after the current figure has come back down. 0 where /proc is not available.
# TYPE kimmy_process_resident_peak_bytes gauge
kimmy_process_resident_peak_bytes 50
# HELP kimmy_up Always 1; presence indicates the node is serving.
# TYPE kimmy_up gauge
kimmy_up 1
# HELP kimmy_uptime_seconds Seconds since this process started serving.
# TYPE kimmy_uptime_seconds gauge
kimmy_uptime_seconds 0
# HELP kimmy_runtime_stall_seconds Worst delay a 250 ms timer on the async runtime saw since the last scrape. Above a few tens of milliseconds, something blocked a worker thread - the storage lock or an fsync - and peers may have marked this node down.
# TYPE kimmy_runtime_stall_seconds gauge
kimmy_runtime_stall_seconds 0
# HELP kimmy_requests_total HTTP requests handled.
# TYPE kimmy_requests_total counter
kimmy_requests_total 10
# HELP kimmy_responses_total HTTP responses by status class.
# TYPE kimmy_responses_total counter
kimmy_responses_total{class=\"2xx\"} 2
kimmy_responses_total{class=\"4xx\"} 3
kimmy_responses_total{class=\"5xx\"} 4
# HELP kimmy_authz_denied_total Operations refused by RBAC.
# TYPE kimmy_authz_denied_total counter
kimmy_authz_denied_total 1
# HELP kimmy_auth_failures_total Rejected credentials and tokens.
# TYPE kimmy_auth_failures_total counter
kimmy_auth_failures_total 1
# HELP kimmy_rate_limited_total Requests refused by a rate limit.
# TYPE kimmy_rate_limited_total counter
kimmy_rate_limited_total 1
# HELP kimmy_rate_limited_principal_total Authenticated requests refused by the per-principal rate limit. Also counted in kimmy_rate_limited_total; the difference is the login limiter.
# TYPE kimmy_rate_limited_principal_total counter
kimmy_rate_limited_principal_total 5
# HELP kimmy_backups_total Backups served.
# TYPE kimmy_backups_total counter
kimmy_backups_total 1
# HELP kimmy_ttl_expired_total Documents deleted by a TTL index.
# TYPE kimmy_ttl_expired_total counter
kimmy_ttl_expired_total 11
# HELP kimmy_ttl_skipped_total Expiry candidates refused because the document was refreshed before the delete.
# TYPE kimmy_ttl_skipped_total counter
kimmy_ttl_skipped_total 12
# HELP kimmy_index_unkeyed_total Documents stored under an index that could not key them - arrays at two of a compound index's paths, more than 1000 keys, or a Decimal128 - and are rechecked on every scan of that index instead. Each one is logged at warning naming the index and the document; the index listing reports how many stand under each index.
# TYPE kimmy_index_unkeyed_total counter
kimmy_index_unkeyed_total 26
# HELP kimmy_webhook_deliveries_total Webhook delivery attempts by outcome.
# TYPE kimmy_webhook_deliveries_total counter
kimmy_webhook_deliveries_total{outcome=\"delivered\"} 2
kimmy_webhook_deliveries_total{outcome=\"failed\"} 1
# HELP kimmy_webhook_events_total Change events pushed to endpoints.
# TYPE kimmy_webhook_events_total counter
kimmy_webhook_events_total 27
# HELP kimmy_webhook_subscriptions Registered subscriptions, as this node sees the registry.
# TYPE kimmy_webhook_subscriptions gauge
kimmy_webhook_subscriptions{state=\"active\"} 15
kimmy_webhook_subscriptions{state=\"invalidated\"} 16
# HELP kimmy_webhook_backlog_seconds Age of the oldest undelivered event, across subscriptions this node owns.
# TYPE kimmy_webhook_backlog_seconds gauge
kimmy_webhook_backlog_seconds 17
# HELP kimmy_cluster_members Peers this node's SWIM membership currently considers alive. 0 with clustering off.
# TYPE kimmy_cluster_members gauge
kimmy_cluster_members 18
# HELP kimmy_replication_lag_seconds Seconds since the newest peer entry applied locally where a peer holds newer, max over peers in the last sync round. 0 when caught up or clustering is off.
# TYPE kimmy_replication_lag_seconds gauge
kimmy_replication_lag_seconds 19
# HELP kimmy_sync_failures_total Anti-entropy rounds against a peer that failed, any cause: unreachable, refused, or a batch this node could not apply. Rising while kimmy_replication_lag_seconds sits at 0 is a wedged peer, not a healthy one - a failed round reports no lag.
# TYPE kimmy_sync_failures_total counter
kimmy_sync_failures_total 23
# HELP kimmy_sync_peers_backing_off Peers this node is currently backing off from after failed rounds. 0 when every peer answered its last round or clustering is off.
# TYPE kimmy_sync_peers_backing_off gauge
kimmy_sync_peers_backing_off 24
# HELP kimmy_sync_ddl_refused_total Replicated schema changes this node could not apply to its own data and skipped - an index its peers hold and it does not. Each one is logged at warning with the reason.
# TYPE kimmy_sync_ddl_refused_total counter
kimmy_sync_ddl_refused_total 25
# HELP kimmy_sync_ddl_declined_total Replicated index drops this node declined as older than the index standing under the name here, and had not already recorded. A drop applied when it was current leaves a tombstone, so a re-served window carrying it past the recreation it preceded is a replay and is not counted. What is counted is a drop this member has never seen - a member whose clock ran ahead when it created the index, which is now the only member still holding it; drop it directly on that member.
# TYPE kimmy_sync_ddl_declined_total counter
kimmy_sync_ddl_declined_total 36
# HELP kimmy_sync_divergent_collections Collections a periodic cross-member check currently finds disagreeing with a peer - held there and not here, or held by both with a different document count - confirmed on two checks running. 0 on a converged cluster. Moves for a divergence that leaves every other sync series reading healthy, because nothing about it fails a round.
# TYPE kimmy_sync_divergent_collections gauge
kimmy_sync_divergent_collections 5
# HELP kimmy_sync_divergence_checks_total Contacts with a peer in which the cross-member divergence check above ran, and rounds that did not run it - completed with the pull truncated by the batch cap, or failed. kimmy_sync_divergent_collections reading 0 is evidence that the peers agree only while ran is rising; ran flat while skipped rises means nothing looked, which a bare 0 cannot say. A round that failed is counted in kimmy_sync_failures_total and here as skipped, so ran plus skipped is every round attempted.
# TYPE kimmy_sync_divergence_checks_total counter
kimmy_sync_divergence_checks_total{outcome=\"ran\"} 32
kimmy_sync_divergence_checks_total{outcome=\"skipped\"} 34
# HELP kimmy_sync_divergence_count_probes_total Checked contacts in which the document-count half of the check compared the probed collection's count against the peer's, and checked contacts in which it was deferred because one member was behind the other and still catching up. compared flat while ran rises means no document count has been compared against any peer, whatever the gauge reads. A member that is behind but whose position has not moved for 3 consecutive checked contacts is compared regardless, so a member whose replication has stopped is not deferred for as long as it stays stopped.
# TYPE kimmy_sync_divergence_count_probes_total counter
kimmy_sync_divergence_count_probes_total{outcome=\"compared\"} 63
kimmy_sync_divergence_count_probes_total{outcome=\"deferred\"} 67
# HELP kimmy_sync_divergence_check_age_seconds Seconds since the last contact, with any peer, in which the cross-member divergence check ran, computed when this page is read. 0 before the first such contact, when ran is also 0. Above a few multiples of cluster.sync_interval_secs, kimmy_sync_divergent_collections is holding a value nothing has re-examined, whether the rounds are failing or the loop itself is stuck - look at kimmy_sync_failures_total, kimmy_sync_peers_backing_off and kimmy_write_lock_wait_seconds.
# TYPE kimmy_sync_divergence_check_age_seconds gauge
kimmy_sync_divergence_check_age_seconds 71
# HELP kimmy_sync_entries_skipped_total Replicated entries a sync round left rather than took. unknown_collection: batches stopped at an entry for a collection this node has no record of - neither holding it nor a tombstone for it - because its creation was witnessed here without being applied, or has aged out of the peer's oplog; one per stopped batch, the window is re-served from the same place every round, and the round plans a snapshot from the peer to bring the collection. A collection dropped here is history instead and stops nothing. beyond_advertised: entries above the vector the peer advertised before serving the window, left for the next round, which asks for them from the right position; ordinary and rare on a busy cluster. A hole of either kind reads 0 on kimmy_replication_lag_seconds; this and kimmy_sync_divergent_collections are what move.
# TYPE kimmy_sync_entries_skipped_total counter
kimmy_sync_entries_skipped_total{reason=\"unknown_collection\"} 73
kimmy_sync_entries_skipped_total{reason=\"beyond_advertised\"} 75
# HELP kimmy_sync_held_marks_released_total Entries this node held as state - written by a snapshot page, a carried delete or a scoped repair, above the vector it advertises - that arrived in a sync window served contiguously from its position and were released: the mark removed and both vectors raised over the entry. One per entry, counted when the batch commits. The release path itself: the beyond_advertised reason of kimmy_sync_entries_skipped_total rises on a peer while these entries are held and also for the ordinary race, and only this tells the two apart.
# TYPE kimmy_sync_held_marks_released_total counter
kimmy_sync_held_marks_released_total 53
# HELP kimmy_sync_repair_rounds_total Sync rounds spent repairing against a peer: re-serving its oplog from the divergent collection's creation, or pulling its snapshot, after the divergence check confirmed a collection against it or a batch stopped at a collection this node lacks. Rising is a repair under way; it stops when the repair reaches the peer's tail.
# TYPE kimmy_sync_repair_rounds_total counter
kimmy_sync_repair_rounds_total 77
# HELP kimmy_tls_reloads_total Certificate reload attempts by outcome. A failed reload leaves the certificate already in use serving.
# TYPE kimmy_tls_reloads_total counter
kimmy_tls_reloads_total{outcome=\"ok\"} 20
kimmy_tls_reloads_total{outcome=\"failed\"} 1
# HELP kimmy_jwks_refresh_total Attempts to refresh the OIDC provider's signing keys, by outcome. A failed refresh leaves the key set already in use verifying.
# TYPE kimmy_jwks_refresh_total counter
kimmy_jwks_refresh_total{outcome=\"ok\"} 22
kimmy_jwks_refresh_total{outcome=\"failed\"} 1
# HELP kimmy_embed_documents_total Documents whose vectors this node wrote.
# TYPE kimmy_embed_documents_total counter
kimmy_embed_documents_total 0
# HELP kimmy_embed_chunks_total Provider inputs embedded - the closest proxy for provider spend.
# TYPE kimmy_embed_chunks_total counter
kimmy_embed_chunks_total 0
# HELP kimmy_embed_deferred_total Foreign-written documents held for a later re-check.
# TYPE kimmy_embed_deferred_total counter
kimmy_embed_deferred_total 0
# HELP kimmy_embed_skipped_not_owned_total Documents dropped un-embedded because another node owns embedding - the duplicate provider calls this counts replacing is the 3x amplification measured in the August 2026 load test.
# TYPE kimmy_embed_skipped_not_owned_total counter
kimmy_embed_skipped_not_owned_total 0
# HELP kimmy_embed_failures_total Failed provider calls, including each retry. Climbing while embed_documents stays flat is a provider outage.
# TYPE kimmy_embed_failures_total counter
kimmy_embed_failures_total 0
# HELP kimmy_embed_provider_errors_total Provider calls that failed before a response, by what failed: connect (DNS, TCP, TLS), timeout, reset (the far side closed an open connection), other.
# TYPE kimmy_embed_provider_errors_total counter
kimmy_embed_provider_errors_total{kind=\"connect\"} 0
kimmy_embed_provider_errors_total{kind=\"timeout\"} 0
kimmy_embed_provider_errors_total{kind=\"reset\"} 0
kimmy_embed_provider_errors_total{kind=\"other\"} 0
# HELP kimmy_embed_provider_requests_total Embedding provider calls answered, documents and search queries alike - compare with the provider's own request count.
# TYPE kimmy_embed_provider_requests_total counter
kimmy_embed_provider_requests_total 0
# HELP kimmy_embed_provider_tokens_total Input tokens the embedding provider reported billing for - the number a metered provider's invoice is made of. Zero for providers that report none.
# TYPE kimmy_embed_provider_tokens_total counter
kimmy_embed_provider_tokens_total 0
# HELP kimmy_request_duration_seconds End-to-end request latency. Health and metrics routes are excluded, so scrapes do not crowd the buckets the real traffic lands in.
# TYPE kimmy_request_duration_seconds histogram
kimmy_request_duration_seconds_bucket{le=\"0.0001\"} 1
kimmy_request_duration_seconds_bucket{le=\"0.00025\"} 1
kimmy_request_duration_seconds_bucket{le=\"0.0005\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.001\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.0025\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.005\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.01\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.025\"} 2
kimmy_request_duration_seconds_bucket{le=\"0.05\"} 3
kimmy_request_duration_seconds_bucket{le=\"0.1\"} 3
kimmy_request_duration_seconds_bucket{le=\"1\"} 3
kimmy_request_duration_seconds_bucket{le=\"10\"} 3
kimmy_request_duration_seconds_bucket{le=\"+Inf\"} 3
kimmy_request_duration_seconds_sum 0.03049
kimmy_request_duration_seconds_count 3
";

        // The read is taken at a moment placed ahead of the clock, so the
        // instants the helper records relative to it are safely after any
        // epoch `Instant` might count from.
        let now = Instant::now() + Duration::from_secs(100);
        assert_eq!(every_counter_distinct(now).render_with_at(&distinct_readings(), now), expected);
    }

    #[test]
    fn the_snapshot_reads_the_same_atomics_the_render_does() {
        // The bridge's whole claim (ADR-070) is that there is one source of
        // truth per counter. A snapshot that drifted from the render would be
        // the duplication this was written to avoid, arrived at by accident —
        // so every field is checked against the text a scrape would see.
        let now = Instant::now() + Duration::from_secs(100);
        let m = every_counter_distinct(now);
        let readings = distinct_readings();
        // Both at one instant: the check's age is computed at the read
        // (ADR-154), and two reads a second boundary apart would disagree
        // by one without either being wrong.
        let s = m.snapshot_with_at(&readings, now);
        let out = m.render_with_at(&readings, now);

        let expect = |line: &str| {
            assert!(out.contains(line), "the render disagrees with the snapshot: {line}\n{out}")
        };
        expect(&format!("kimmy_databases {}\n", s.databases));
        expect(&format!("kimmy_collections {}\n", s.collections));
        expect(&format!("kimmy_unique_violations {}\n", s.unique_violations));
        expect(&format!("kimmy_commits {}\n", s.commits));
        expect(&format!("kimmy_fsyncs {}\n", s.fsyncs));
        expect(&format!("kimmy_commits_grouped_total {}\n", s.commits_grouped));
        expect(&format!("kimmy_storage_bytes {}\n", s.storage_bytes));
        expect(&format!("kimmy_vector_index_cache_bytes {}\n", s.vector_index_cache_bytes));
        expect(&format!("kimmy_process_resident_bytes {}\n", s.process_resident_bytes));
        expect(&format!("kimmy_process_resident_peak_bytes {}\n", s.process_resident_peak_bytes));
        expect(&format!("kimmy_uptime_seconds {}\n", s.uptime_secs));
        expect(&format!("kimmy_requests_total {}\n", s.requests));
        expect(&format!("kimmy_responses_total{{class=\"2xx\"}} {}\n", s.responses_2xx));
        expect(&format!("kimmy_responses_total{{class=\"4xx\"}} {}\n", s.responses_4xx));
        expect(&format!("kimmy_responses_total{{class=\"5xx\"}} {}\n", s.responses_5xx));
        expect(&format!("kimmy_authz_denied_total {}\n", s.authz_denied));
        expect(&format!("kimmy_auth_failures_total {}\n", s.auth_failures));
        expect(&format!("kimmy_rate_limited_total {}\n", s.rate_limited));
        expect(&format!("kimmy_rate_limited_principal_total {}\n", s.rate_limited_principal));
        expect(&format!("kimmy_backups_total {}\n", s.backups));
        expect(&format!("kimmy_ttl_expired_total {}\n", s.ttl_expired));
        expect(&format!("kimmy_ttl_skipped_total {}\n", s.ttl_skipped));
        expect(&format!("kimmy_index_unkeyed_total {}\n", s.index_unkeyed));
        expect(&format!(
            "kimmy_webhook_deliveries_total{{outcome=\"delivered\"}} {}\n",
            s.webhook_delivered
        ));
        expect(&format!(
            "kimmy_webhook_deliveries_total{{outcome=\"failed\"}} {}\n",
            s.webhook_failed
        ));
        expect(&format!("kimmy_webhook_events_total {}\n", s.webhook_events));
        expect(&format!("kimmy_webhook_subscriptions{{state=\"active\"}} {}\n", s.webhook_active));
        expect(&format!(
            "kimmy_webhook_subscriptions{{state=\"invalidated\"}} {}\n",
            s.webhook_invalidated
        ));
        expect(&format!("kimmy_webhook_backlog_seconds {}\n", s.webhook_backlog_secs));
        expect(&format!("kimmy_cluster_members {}\n", s.cluster_members));
        expect(&format!("kimmy_replication_lag_seconds {}\n", s.replication_lag_secs));
        expect(&format!("kimmy_sync_failures_total {}\n", s.sync_failures));
        expect(&format!("kimmy_sync_peers_backing_off {}\n", s.sync_peers_backing_off));
        expect(&format!("kimmy_sync_ddl_refused_total {}\n", s.sync_ddl_refused));
        expect(&format!("kimmy_sync_ddl_declined_total {}\n", s.sync_ddl_declined));
        expect(&format!("kimmy_sync_divergent_collections {}\n", s.sync_divergent_collections));
        expect(&format!(
            "kimmy_sync_divergence_checks_total{{outcome=\"ran\"}} {}\n",
            s.sync_divergence_checks
        ));
        expect(&format!(
            "kimmy_sync_divergence_checks_total{{outcome=\"skipped\"}} {}\n",
            s.sync_divergence_skips
        ));
        expect(&format!(
            "kimmy_sync_divergence_count_probes_total{{outcome=\"compared\"}} {}\n",
            s.sync_divergence_count_compared
        ));
        expect(&format!(
            "kimmy_sync_divergence_count_probes_total{{outcome=\"deferred\"}} {}\n",
            s.sync_divergence_count_deferred
        ));
        expect(&format!(
            "kimmy_sync_divergence_check_age_seconds {}\n",
            s.sync_divergence_check_age_secs
        ));
        expect(&format!(
            "kimmy_sync_entries_skipped_total{{reason=\"unknown_collection\"}} {}\n",
            s.sync_entries_skipped_unknown_collection
        ));
        expect(&format!(
            "kimmy_sync_entries_skipped_total{{reason=\"beyond_advertised\"}} {}\n",
            s.sync_entries_skipped_beyond_advertised
        ));
        expect(&format!("kimmy_sync_held_marks_released_total {}\n", s.sync_held_marks_released));
        expect(&format!("kimmy_sync_repair_rounds_total {}\n", s.sync_repair_rounds));
        expect(&format!("kimmy_tls_reloads_total{{outcome=\"ok\"}} {}\n", s.tls_reloads_ok));
        expect(&format!(
            "kimmy_tls_reloads_total{{outcome=\"failed\"}} {}\n",
            s.tls_reloads_failed
        ));
        expect(&format!("kimmy_jwks_refresh_total{{outcome=\"ok\"}} {}\n", s.jwks_refresh_ok));
        expect(&format!(
            "kimmy_jwks_refresh_total{{outcome=\"failed\"}} {}\n",
            s.jwks_refresh_failed
        ));
        expect(&format!("kimmy_embed_documents_total {}\n", s.embed_documents_embedded));
        expect(&format!("kimmy_embed_chunks_total {}\n", s.embed_chunks_embedded));
        expect(&format!("kimmy_embed_deferred_total {}\n", s.embed_deferred));
        expect(&format!("kimmy_embed_skipped_not_owned_total {}\n", s.embed_skipped_not_owned));
        expect(&format!("kimmy_embed_failures_total {}\n", s.embed_failures));
        expect(&format!("kimmy_embed_provider_requests_total {}\n", s.embed_provider_requests));
        expect(&format!("kimmy_embed_provider_tokens_total {}\n", s.embed_provider_tokens));
        for (kind, n) in ["connect", "timeout", "reset", "other"].iter().zip(s.embed_transport) {
            expect(&format!("kimmy_embed_provider_errors_total{{kind=\"{kind}\"}} {n}\n"));
        }
        expect(&format!("kimmy_request_duration_seconds_count {}\n", s.latency_count));

        // Not a rendered series of its own — the histogram prints it in seconds
        // — but the bridge reports microseconds, so the conversion is the thing
        // that can silently be wrong.
        assert_eq!(s.latency_sum_us, 90 + 400 + 30_000);
    }

    #[test]
    fn statuses_land_in_the_right_class() {
        let m = Metrics::default();
        for status in [200, 201, 204] {
            m.record_request(status);
        }
        for status in [400, 403, 429] {
            m.record_request(status);
        }
        m.record_request(500);
        m.record_request(304);

        let out = m.render();
        assert!(out.contains("kimmy_requests_total 8"), "{out}");
        assert!(out.contains("class=\"2xx\"} 3"), "{out}");
        assert!(out.contains("class=\"4xx\"} 3"), "{out}");
        assert!(out.contains("class=\"5xx\"} 1"), "{out}");
    }

    #[test]
    fn the_render_is_parseable_prometheus_text() {
        // Every series needs its HELP and TYPE, and every sample line must be
        // `name value`. A scrape failing on a malformed line loses the whole
        // endpoint, not just the bad series.
        let m = Metrics::default();
        m.record_request(403);
        let out = m.render();

        let mut samples = 0;
        for line in out.lines() {
            if line.starts_with('#') {
                assert!(
                    line.starts_with("# HELP ") || line.starts_with("# TYPE "),
                    "unexpected comment: {line}"
                );
                continue;
            }
            let value = line.rsplit(' ').next().expect("a value");
            // f64 rather than u64: the histogram's `_sum` is in seconds.
            assert!(value.parse::<f64>().is_ok(), "not a numeric sample: {line}");
            samples += 1;
        }
        // 56 scalar sample lines plus three histograms: the latency one's 12
        // buckets, +Inf, sum and count; the writer wait's 8 buckets, +Inf,
        // sum and count (ADR-151); and the writer hold's 7 buckets, +Inf,
        // sum and count for each of the twelve holders (ADR-159).
        assert_eq!(
            samples,
            90 + 10 * kimmy_storage::WriterHolder::COUNT,
            "expected one sample per series: {out}"
        );
    }

    #[test]
    fn latency_buckets_are_cumulative_and_the_sum_is_in_seconds() {
        let m = Metrics::default();
        m.record_latency(Duration::from_micros(200)); // ≤ 250µs
        m.record_latency(Duration::from_micros(200));
        m.record_latency(Duration::from_millis(3)); // ≤ 5ms
        let out = m.render();

        // Cumulative: the 250µs bucket holds 2, everything from 5ms up holds
        // all 3 — a non-cumulative render would break every Prometheus
        // quantile function silently.
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"0.00025\"} 2"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"0.001\"} 2"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"0.005\"} 3"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"+Inf\"} 3"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_count 3"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_sum 0.0034"), "{out}");
    }

    #[test]
    fn an_observation_above_every_bound_reaches_only_inf() {
        let m = Metrics::default();
        m.record_latency(Duration::from_secs(60));
        let out = m.render();
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"10\"} 0"), "{out}");
        assert!(out.contains("kimmy_request_duration_seconds_bucket{le=\"+Inf\"} 1"), "{out}");
    }

    #[test]
    fn the_specific_counters_track_their_statuses() {
        let m = Metrics::default();
        m.record_request(401);
        m.record_request(403);
        m.record_request(403);
        m.record_request(429);

        let out = m.render();
        assert!(out.contains("kimmy_auth_failures_total 1"), "{out}");
        assert!(out.contains("kimmy_authz_denied_total 2"), "{out}");
        assert!(out.contains("kimmy_rate_limited_total 1"), "{out}");
        assert!(out.contains("class=\"4xx\"} 4"), "all four are client errors too: {out}");
    }

    #[test]
    fn counters_start_at_zero_rather_than_being_absent() {
        // A counter that only appears after its first event makes a dashboard
        // show "no data" instead of "nothing has gone wrong yet".
        let out = Metrics::default().render();
        assert!(out.contains("kimmy_authz_denied_total 0"), "{out}");
        assert!(out.contains("kimmy_rate_limited_total 0"), "{out}");
        assert!(out.contains("kimmy_rate_limited_principal_total 0"), "{out}");
    }

    #[test]
    fn a_principal_refusal_is_its_own_series_and_part_of_the_total() {
        // The two are recorded from different places — the total from the
        // status, the split from the extractor — so the relationship an
        // operator relies on (split ≤ total) is one the two call sites have
        // to keep, and this is what holds them to it.
        let m = Metrics::default();
        m.record_principal_rate_limited();
        m.record_request(429);
        m.record_request(429);

        let out = m.render();
        assert!(out.contains("kimmy_rate_limited_total 2"), "{out}");
        assert!(out.contains("kimmy_rate_limited_principal_total 1"), "{out}");
    }

    #[test]
    fn each_runtime_stall_reader_clears_only_its_own_high_water_mark() {
        // The stall gauge is the only series here whose read *clears* what it
        // read, and it has two readers on unrelated schedules: the `/metrics`
        // render and the OTLP bridge. With one mark between them, whichever
        // read first would take the value and leave the other reporting a
        // window it never measured — and on a deployment whose telemetry only
        // leaves through a collector, nothing would ever clear the `/metrics`
        // mark, so the bridged gauge would latch at the worst stall ever seen.
        //
        // Both directions are asserted, because a single mark passes a test
        // that only ever reads one surface.
        let m = Metrics::default();
        m.record_runtime_stall(std::time::Duration::from_micros(1_500));

        // The bridge reads first. It gets the stall, and clears only its own.
        assert_eq!(m.take_runtime_stall_otlp_us(), 1_500);
        assert_eq!(m.take_runtime_stall_otlp_us(), 0, "the bridge's own mark did not clear");
        assert!(
            m.render().contains("kimmy_runtime_stall_seconds 0.0015"),
            "the bridge's read consumed the value /metrics had not yet reported"
        );
        assert!(
            m.render().contains("kimmy_runtime_stall_seconds 0\n"),
            "the /metrics mark did not clear on its own read"
        );

        // And the other way round: /metrics reads first.
        m.record_runtime_stall(std::time::Duration::from_micros(2_500));
        assert!(m.render().contains("kimmy_runtime_stall_seconds 0.0025"));
        assert_eq!(
            m.take_runtime_stall_otlp_us(),
            2_500,
            "a /metrics scrape consumed the stall the bridge had not yet reported"
        );

        // The mark is a maximum, not a last-value, on each surface
        // independently.
        m.record_runtime_stall(std::time::Duration::from_micros(9_000));
        m.record_runtime_stall(std::time::Duration::from_micros(400));
        assert_eq!(m.take_runtime_stall_otlp_us(), 9_000);
    }

    #[test]
    fn the_pushed_gauges_render_what_was_pushed() {
        // Each of these is *set* from somewhere else — the replication loop,
        // the dispatcher, the certificate reloader — and every existing
        // assertion about them checks a value a broken setter would also
        // produce: the cluster harness waits for replication lag to reach
        // **zero**, which is exactly what a setter that does nothing reports.
        // A non-zero value is the only one that distinguishes the two.
        let m = Metrics::default();
        let now = Instant::now() + Duration::from_secs(100);
        m.set_replication_lag_secs(7);
        m.set_cluster_members(2);
        m.record_sync_round(&kimmy_cluster::RoundReport {
            failed: 1,
            backing_off: 1,
            ddl_refused: 0,
            ddl_declined: 0,
            divergent_collections: 0,
            divergence_checks: 4,
            divergence_skips: 0,
            divergence_count_compared: 2,
            divergence_count_deferred: 1,
            divergence_last_check: Some(now - Duration::from_secs(30)),
            entries_skipped_unknown_collection: 0,
            entries_skipped_beyond_advertised: 0,
            repair_rounds: 0,
        });
        m.record_sync_round(&kimmy_cluster::RoundReport {
            failed: 2,
            backing_off: 0,
            ddl_refused: 3,
            ddl_declined: 0,
            divergent_collections: 6,
            divergence_checks: 0,
            divergence_skips: 5,
            divergence_count_compared: 0,
            divergence_count_deferred: 4,
            divergence_last_check: Some(now - Duration::from_secs(8)),
            entries_skipped_unknown_collection: 0,
            entries_skipped_beyond_advertised: 0,
            repair_rounds: 0,
        });
        m.record_tls_reload(true);
        m.record_tls_reload(false);
        m.record_tls_reload(false);
        m.record_jwks_refresh(true);
        m.record_jwks_refresh(true);
        m.record_jwks_refresh(false);

        let out = m.render_at(now);
        assert!(out.contains("kimmy_replication_lag_seconds 7"), "{out}");
        assert!(out.contains("kimmy_cluster_members 2"), "{out}");
        // The failure signals are pushed per tick: counters accumulate across
        // ticks, the backoff level is the latest tick's (ADR-123).
        assert!(out.contains("kimmy_sync_failures_total 3"), "{out}");
        assert!(out.contains("kimmy_sync_peers_backing_off 0"), "{out}");
        assert!(out.contains("kimmy_sync_ddl_refused_total 3"), "{out}");
        assert!(
            out.contains("kimmy_sync_divergent_collections 6"),
            "a level, not accumulated: {out}"
        );
        // And the two counters that say whether that level was arrived at by
        // looking (ADR-135). Counters, so they accumulate across the ticks
        // the gauge above replaces: four contacts checked, five skipped.
        assert!(
            out.contains("kimmy_sync_divergence_checks_total{outcome=\"ran\"} 4"),
            "a counter, not a level: {out}"
        );
        assert!(
            out.contains("kimmy_sync_divergence_checks_total{outcome=\"skipped\"} 5"),
            "a counter, not a level: {out}"
        );
        // The count half's pair accumulates too, and the age is computed
        // at the read from the latest tick's instant (ADR-145, ADR-154): a
        // check thirty seconds before the read at the first tick, eight
        // before it at the second, and the gauge says eight.
        assert!(
            out.contains("kimmy_sync_divergence_count_probes_total{outcome=\"compared\"} 2"),
            "a counter, not a level: {out}"
        );
        assert!(
            out.contains("kimmy_sync_divergence_count_probes_total{outcome=\"deferred\"} 5"),
            "a counter, not a level: {out}"
        );
        assert!(
            out.contains("kimmy_sync_divergence_check_age_seconds 8"),
            "a level, not accumulated: {out}"
        );
        assert!(out.contains("kimmy_tls_reloads_total{outcome=\"ok\"} 1"), "{out}");
        assert!(out.contains("kimmy_tls_reloads_total{outcome=\"failed\"} 2"), "{out}");
        // A node that stops being able to reach its identity provider keeps
        // serving until the provider rotates, so the failed count is the only
        // warning there is (ADR-064).
        assert!(out.contains("kimmy_jwks_refresh_total{outcome=\"ok\"} 2"), "{out}");
        assert!(out.contains("kimmy_jwks_refresh_total{outcome=\"failed\"} 1"), "{out}");
    }

    /// A loop that has never run the check reports no age, and the gauge
    /// renders `0` for it — beside a `ran` counter that also reads `0`,
    /// which is what tells "never checked" from "checked just now"
    /// (ADR-145, answering ADR-135's objection to an age gauge).
    #[test]
    fn an_age_the_loop_has_not_got_renders_as_zero() {
        let m = Metrics::default();
        m.record_sync_round(&kimmy_cluster::RoundReport {
            failed: 1,
            backing_off: 1,
            ddl_refused: 0,
            ddl_declined: 0,
            divergent_collections: 0,
            divergence_checks: 0,
            divergence_skips: 1,
            divergence_count_compared: 0,
            divergence_count_deferred: 0,
            divergence_last_check: None,
            entries_skipped_unknown_collection: 0,
            entries_skipped_beyond_advertised: 0,
            repair_rounds: 0,
        });
        let out = m.render();
        assert!(out.contains("kimmy_sync_divergence_check_age_seconds 0\n"), "{out}");
        assert!(out.contains("kimmy_sync_divergence_checks_total{outcome=\"ran\"} 0\n"), "{out}");
        // And it stays 0 however long the loop goes without a check: there
        // is no instant to age. "Never" is not "long ago" (ADR-135's
        // objection, ADR-145's answer), and computing the age at the read
        // (ADR-154) must not turn one into the other.
        let out = m.render_at(Instant::now() + Duration::from_secs(3_600));
        assert!(out.contains("kimmy_sync_divergence_check_age_seconds 0\n"), "{out}");
    }

    /// The age is computed when it is read, not when the loop pushed it
    /// (ADR-154). Under ADR-145 the loop computed the age at the end of its
    /// tick and this type stored the number, so a loop whose tick did not
    /// end — stuck behind the single writer for over an hour, in the round
    /// that found this — left the age reading the same number on every
    /// scrape: the series that exists to say the gauge is stale was frozen
    /// with the gauge. Now the loop reports the instant of the check and
    /// every render and snapshot subtracts it from its own clock, so the
    /// age rises through a stuck tick exactly as it rises through a run of
    /// failed rounds, and a scrape needs nothing from the loop to read it.
    ///
    /// The stuck loop is simulated by what it is: a check is recorded, and
    /// `record_sync_round` is never called again.
    #[test]
    fn the_check_age_is_computed_when_it_is_read_so_a_stuck_loop_cannot_freeze_it() {
        let m = Metrics::default();
        let checked = Instant::now();
        let tick = |at: Option<Instant>| kimmy_cluster::RoundReport {
            failed: 0,
            backing_off: 0,
            ddl_refused: 0,
            ddl_declined: 0,
            divergent_collections: 0,
            divergence_checks: usize::from(at == Some(checked)),
            divergence_skips: 0,
            divergence_count_compared: 0,
            divergence_count_deferred: 0,
            divergence_last_check: at,
            entries_skipped_unknown_collection: 0,
            entries_skipped_beyond_advertised: 0,
            repair_rounds: 0,
        };
        let age_in = |out: &str| -> u64 {
            out.lines()
                .find_map(|l| l.strip_prefix("kimmy_sync_divergence_check_age_seconds "))
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("no age line: {out}"))
        };

        // The tick that ran the check reports its instant, and a read at
        // that instant says the reading is fresh.
        m.record_sync_round(&tick(Some(checked)));
        assert_eq!(age_in(&m.render_at(checked)), 0, "just checked");

        // The loop's next tick starts and never ends. Nothing is pushed for
        // ninety seconds, then for an hour; the age is what a scrape at each
        // moment computes, and it rises. Under ADR-145 every one of these
        // reads would have said 0.
        let ninety = checked + Duration::from_secs(90);
        assert_eq!(age_in(&m.render_at(ninety)), 90, "the render computes the age at the read");
        assert_eq!(
            m.snapshot_with_at(&StorageReadings::default(), ninety).sync_divergence_check_age_secs,
            90,
            "the snapshot the OTLP bridge exports carries the same computed age"
        );
        let hour = checked + Duration::from_secs(3_600);
        assert_eq!(age_in(&m.render_at(hour)), 3_600, "and keeps rising while nothing is pushed");

        // A tick that completed without a check — every round failed, say —
        // carries the old instant, and the age goes on rising from it.
        m.record_sync_round(&tick(Some(checked)));
        let later = hour + Duration::from_secs(5);
        assert_eq!(age_in(&m.render_at(later)), 3_605, "a tick without a check resets nothing");

        // A check resets it, and the reset is the loop's to make: a fresh
        // instant, and the age is measured from there.
        let rechecked = later;
        m.record_sync_round(&tick(Some(rechecked)));
        assert_eq!(age_in(&m.render_at(rechecked)), 0, "a check resets the age");
        assert_eq!(age_in(&m.render_at(rechecked + Duration::from_secs(12))), 12);
    }
    #[test]
    fn resident_memory_is_read_from_the_two_status_lines_in_bytes() {
        // The shape the kernel prints: a tab after the colon, right-aligned
        // digits, the unit always kB. Order and surrounding lines are the
        // kernel's business, so the fixture carries neighbours.
        let status = "Name:\tkimmyd\nVmPeak:\t 2233104 kB\nVmSize:\t 2101972 kB\n\
                      VmHWM:\t 2097152 kB\nVmRSS:\t  696320 kB\nRssAnon:\t  690000 kB\n\
                      Threads:\t9\n";
        let m = ProcessMemory::parse(status);
        assert_eq!(m.resident_bytes, 696_320 * 1024);
        assert_eq!(m.peak_resident_bytes, 2_097_152 * 1024);

        // `VmSize` and `VmPeak` share a prefix character run with nothing
        // here, but `RssAnon` and `VmRSS` must not be confused: the match is
        // on the field name at the start of the line, not on a substring.
        let m = ProcessMemory::parse("RssAnon:\t 1 kB\nVmRSSx:\t 2 kB\n");
        assert_eq!(m, ProcessMemory::default());
    }

    #[test]
    fn a_missing_or_malformed_line_reads_as_zero_and_leaves_the_other_field() {
        // A kernel that stopped printing one of the two, or a line that is
        // not a number, must not take the other reading with it. Zero is
        // what the HELP text promises for "not available".
        let m = ProcessMemory::parse("VmRSS:\t 12 kB\n");
        assert_eq!(m, ProcessMemory { resident_bytes: 12 * 1024, peak_resident_bytes: 0 });
        let m = ProcessMemory::parse("VmRSS:\t lots kB\nVmHWM:\t 3 kB\n");
        assert_eq!(m, ProcessMemory { resident_bytes: 0, peak_resident_bytes: 3 * 1024 });
        assert_eq!(ProcessMemory::parse(""), ProcessMemory::default());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_live_reading_is_non_zero_on_linux_and_the_peak_is_at_least_the_current() {
        // The claim the two series make: on the target the release binary
        // ships for, the numbers are the kernel's and not the placeholder.
        let m = ProcessMemory::read();
        assert!(m.resident_bytes > 0, "{m:?}");
        assert!(m.peak_resident_bytes >= m.resident_bytes, "{m:?}");
    }
}
