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

/// Upper bounds of `kimmy_backup_duration_seconds`, in microseconds (ADR-170).
///
/// A backup reads the whole store, so its duration follows the store's size and
/// whether the file is in page cache, not the request: measured from 15 s for
/// an 800 MB backup to 1,903 s for a 4.29 GB one on a cold cache. The bounds
/// run from a second to the hour, which brackets both with room either side;
/// anything longer lands in `+Inf`, which is what an operator should be looking
/// at anyway.
pub const BACKUP_BUCKETS_US: [u64; 10] = [
    1_000_000,
    5_000_000,
    15_000_000,
    30_000_000,
    60_000_000,
    120_000_000,
    300_000_000,
    600_000_000,
    1_800_000_000,
    3_600_000_000,
];

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
    pub index_undecidable: u64,
    /// Schema changes a snapshot restore appended to the oplog since start
    /// (`Engine::ddl_relogged`, ADR-180). A reading rather than a counter
    /// here: the engine counts each once its commit lands, which a round's
    /// report, returned only when the whole page succeeds, could not.
    pub sync_ddl_relogged: u64,
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
    /// What those holds were made of, per holder (ADR-176): the reading that
    /// says *which part* of a hold moved when the hold did.
    pub writer_hold_decomposition: kimmy_storage::HoldDecomposition,
    /// What serving peers' windows has cost this node (ADR-176).
    pub serve: kimmy_storage::ServeSnapshot,
    /// Entries held as state that a sync window released, since start
    /// (`Engine::held_marks_released`, ADR-169's addendum).
    pub held_marks_released: u64,
    /// Entries held as state right now (`Engine::held_marks`, ADR-160).
    pub held_marks: u64,
    /// Registered webhook subscriptions by state, counted from the registry
    /// at the read (ADR-187). They were set by the dispatcher at the end of
    /// each pass, so a dispatcher that had stopped left them at whatever it
    /// last saw. Counting a small system collection costs one walk; a read
    /// that fails fails the scrape, as every other reading here does, instead
    /// of reporting none.
    pub webhook_active: u64,
    pub webhook_invalidated: u64,
    /// Registry records that do not decode as a document: counted, rather
    /// than failing the scrape, so one bad record is visible without taking
    /// the page down.
    pub webhook_unreadable: u64,
    /// Peers this node's SWIM membership currently holds, counted from the
    /// member set at the read (ADR-187); 0 with clustering off, by
    /// construction. It was written by the webhook dispatcher's loop, which
    /// had nothing to do with membership and could stop writing it.
    pub cluster_members: u64,
}

/// The background writers behind the page's measured gauges, each with a
/// progress age (`kimmy_task_progress_age_seconds{task}`, ADR-187), by the name
/// each is supervised under.
///
/// A gauge that can be read at scrape is read at scrape and needs no age. What
/// is left here are values only a background pass can know: the replication
/// loop's lag, backoff and divergence figures, the stall probe's lateness, the
/// dispatcher's backlog, and the embedding worker's work. Every name is a
/// `kimmy_task::TASKS` entry, which `every_progress_writer_is_a_supervised_task`
/// holds.
pub const PROGRESS_WRITERS: [&str; 5] =
    ["drop_purger", "embedding_worker", "replication", "stall_probe", "webhook_dispatcher"];

/// Where `writer` sits in [`PROGRESS_WRITERS`], and so in every array ordered
/// by it. Read by name, so that a writer added to the list cannot move a
/// reader onto its neighbour's age.
pub fn progress_slot(writer: &str) -> usize {
    PROGRESS_WRITERS
        .iter()
        .position(|w| *w == writer)
        .unwrap_or_else(|| panic!("{writer} is not a progress writer"))
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
    /// Expiry candidates declined because the index's partial filter no
    /// longer selected the document (ADR-181).
    pub ttl_skipped_filter: u64,
    /// Documents filed under an index's unkeyed run — stored, but with no
    /// key the index could derive, so every scan of that index rechecks
    /// them (ADR-139). One of the engine's readings.
    pub index_unkeyed: u64,
    pub index_undecidable: u64,
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
    /// What the holds were made of, and what serving peers cost (ADR-176).
    /// Counters only, so the whole of both is on the bridge.
    pub write_lock_hold: kimmy_storage::HoldDecomposition,
    pub sync_serve: kimmy_storage::ServeSnapshot,
    pub webhook_delivered: u64,
    pub webhook_failed: u64,
    pub webhook_events: u64,
    pub webhook_active: u64,
    pub webhook_invalidated: u64,
    pub webhook_unreadable: u64,
    pub webhook_backlog_secs: u64,
    pub cluster_members: u64,
    /// Seconds since each of [`PROGRESS_WRITERS`] last made progress, in that
    /// order, or since the process started before its first (ADR-187). `None`
    /// for a writer this node does not run, which has no row.
    pub task_progress_age_secs: [Option<u64>; PROGRESS_WRITERS.len()],
    /// Milliseconds, since ADR-175; rendered and bridged as seconds.
    pub replication_lag_ms: u64,
    /// Anti-entropy rounds that failed, peers currently backed off, and
    /// replicated schema changes skipped (ADR-123). The failure signals the
    /// lag gauge cannot carry: a failed round reports no lag.
    pub sync_failures: u64,
    pub sync_peers_backing_off: u64,
    pub sync_ddl_refused: u64,
    /// Replicated index drops declined as older than the index standing
    /// here (ADR-141).
    pub sync_ddl_declined: u64,
    /// Replicated schema changes applied here, by the way they arrived: a
    /// pulled window, or a window a peer pushed to confirm a change
    /// (ADR-140). Every entry the apply took as applied, not one whose entry
    /// this node already held as sent — see
    /// [`Metrics::record_ddl_applied_push`].
    pub sync_ddl_applied_pull: u64,
    pub sync_ddl_applied_push: u64,
    /// Replicated schema changes a window carried that this node already
    /// held, entry and all, by the same two ways: not applied again, and
    /// their append committed nothing (a kind's own writes are unchanged).
    /// The overlap of windows, kept visible.
    pub sync_ddl_held_pull: u64,
    pub sync_ddl_held_push: u64,
    /// Schema-change confirmations on each member, by how each ended, in
    /// `ConfirmOutcome::ALL` order (ADR-191); and the windows pushed for
    /// them, whose ratio to the confirmations is the coalescing.
    pub ddl_confirmations: [u64; kimmy_cluster::ConfirmOutcome::COUNT],
    pub ddl_confirm_pushes: u64,
    /// Schema changes a snapshot restore re-logged so that this node can
    /// serve them onward (ADR-180). One of the engine's readings.
    pub sync_ddl_relogged: u64,
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
    /// reported; since the process started before the first (ADR-145,
    /// ADR-154, ADR-187). How old the gauge's
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
    pub sync_entries_skipped_purge_pending: u64,
    /// Entries held as state that a sync window released (ADR-169's
    /// addendum): the release path itself, which `beyond_advertised` cannot
    /// tell from the ordinary race. An engine reading, not a round report.
    pub sync_held_marks_released: u64,
    /// Entries held as state right now (ADR-160): an engine reading.
    pub sync_held_marks: u64,
    /// Rounds spent repairing against a peer (ADR-148): re-serving its
    /// oplog from below this node's position or pulling its snapshot, on
    /// the strength of a confirmed divergence or a stopped batch.
    pub sync_repair_rounds: u64,
    /// Where sync pulls spent their time, how long what they carried had
    /// waited, and how contacts ended (ADR-175): the loop's report, summed
    /// since start. The histograms' buckets are on `/metrics` alone; their
    /// sums and counts, and the counters, are what the bridge carries.
    pub sync_pulls: kimmy_cluster::PullReport,
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
    pub embed_skipped_no_shadow: u64,
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
    /// Microseconds spent producing backups (ADR-170); `backups` is the
    /// matching count. The histogram's buckets are on `/metrics` alone.
    pub backup_duration_sum_us: u64,
}

/// Counters for one running server.
pub struct Metrics {
    started: Instant,
    latency_buckets: [AtomicU64; LATENCY_BUCKETS_US.len()],
    latency_sum_us: AtomicU64,
    latency_count: AtomicU64,
    /// `kimmy_backup_duration_seconds`, non-cumulative like the latency
    /// buckets; its count is `backups`.
    backup_buckets: [AtomicU64; BACKUP_BUCKETS_US.len()],
    backup_sum_us: AtomicU64,
    /// Milliseconds (ADR-175).
    replication_lag_ms: AtomicU64,
    /// Every sync tick's [`kimmy_cluster::PullReport`], summed (ADR-175). A
    /// mutex over the loop's own shape rather than an atomic per bucket: some
    /// fifty numbers arrive together once a tick and are read together once
    /// a scrape, and neither holds it for longer than a copy.
    sync_pulls: parking_lot::Mutex<kimmy_cluster::PullReport>,
    /// Pushed by the replication loop after every sync tick (ADR-123): two
    /// counters and a level. What a wedged round looks like from outside,
    /// which the lag gauge — set only by a round that succeeded — cannot
    /// show.
    sync_failures: AtomicU64,
    sync_peers_backing_off: AtomicU64,
    sync_ddl_refused: AtomicU64,
    sync_ddl_declined: AtomicU64,
    sync_ddl_applied_pull: AtomicU64,
    sync_ddl_applied_push: AtomicU64,
    sync_ddl_held_pull: AtomicU64,
    sync_ddl_held_push: AtomicU64,
    ddl_confirmations: [AtomicU64; kimmy_cluster::ConfirmOutcome::COUNT],
    ddl_confirm_pushes: AtomicU64,
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
    sync_entries_skipped_purge_pending: AtomicU64,
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
    webhook_backlog_secs: AtomicU64,
    /// When the stall probe last woke, when the dispatcher last completed the
    /// pass that sets the backlog, and when a replication round last
    /// completed, as the loop's round report carried it; `None` before the
    /// first (ADR-187). The embedding worker's instant is in its own
    /// counters, so it is not kept here.
    stall_probe_progress: parking_lot::Mutex<Option<Instant>>,
    dispatcher_progress: parking_lot::Mutex<Option<Instant>>,
    replication_progress: parking_lot::Mutex<Option<Instant>>,
    /// The writers this node runs, fixed once startup has spawned them
    /// ([`Self::fix_progress_writers`]); every one of [`PROGRESS_WRITERS`]
    /// until then, so a render with no startup behind it — a test — has
    /// every row.
    progress_writers: OnceLock<Vec<&'static str>>,
    tls_reloads_ok: AtomicU64,
    tls_reloads_failed: AtomicU64,
    jwks_refresh_ok: AtomicU64,
    jwks_refresh_failed: AtomicU64,
    ttl_expired: AtomicU64,
    ttl_skipped: AtomicU64,
    ttl_skipped_filter: AtomicU64,
    /// Set once at startup when the embedding worker runs. `None` — the
    /// renderer then reports zeros — means this node has
    /// `[vector] worker_enabled = false`, which an operator must be able to
    /// distinguish from "worker enabled but idle".
    vector_counters: OnceLock<std::sync::Arc<kimmy_vector::WorkerCounters>>,
    /// The drop purger's, for its progress age (ADR-189).
    purge_counters: OnceLock<std::sync::Arc<kimmy_storage::PurgeCounters>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            backup_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            backup_sum_us: AtomicU64::new(0),
            replication_lag_ms: AtomicU64::new(0),
            sync_pulls: parking_lot::Mutex::new(kimmy_cluster::PullReport::default()),
            sync_failures: AtomicU64::new(0),
            sync_peers_backing_off: AtomicU64::new(0),
            sync_ddl_refused: AtomicU64::new(0),
            sync_ddl_declined: AtomicU64::new(0),
            sync_ddl_applied_pull: AtomicU64::new(0),
            sync_ddl_applied_push: AtomicU64::new(0),
            sync_ddl_held_pull: AtomicU64::new(0),
            sync_ddl_held_push: AtomicU64::new(0),
            ddl_confirmations: std::array::from_fn(|_| AtomicU64::new(0)),
            ddl_confirm_pushes: AtomicU64::new(0),
            sync_divergent_collections: AtomicU64::new(0),
            sync_divergence_checks: AtomicU64::new(0),
            sync_divergence_skips: AtomicU64::new(0),
            sync_divergence_count_compared: AtomicU64::new(0),
            sync_divergence_count_deferred: AtomicU64::new(0),
            sync_divergence_last_check: parking_lot::Mutex::new(None),
            sync_entries_skipped_unknown_collection: AtomicU64::new(0),
            sync_entries_skipped_beyond_advertised: AtomicU64::new(0),
            sync_entries_skipped_purge_pending: AtomicU64::new(0),
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
            ttl_skipped_filter: AtomicU64::new(0),
            webhook_backlog_secs: AtomicU64::new(0),
            stall_probe_progress: parking_lot::Mutex::new(None),
            dispatcher_progress: parking_lot::Mutex::new(None),
            replication_progress: parking_lot::Mutex::new(None),
            progress_writers: OnceLock::new(),
            tls_reloads_ok: AtomicU64::new(0),
            tls_reloads_failed: AtomicU64::new(0),
            jwks_refresh_ok: AtomicU64::new(0),
            jwks_refresh_failed: AtomicU64::new(0),
            vector_counters: OnceLock::new(),
            purge_counters: OnceLock::new(),
        }
    }
}

impl Metrics {
    /// The drop purger's counters, read for its progress age (ADR-189).
    pub fn set_purge_counters(&self, counters: std::sync::Arc<kimmy_storage::PurgeCounters>) {
        let _ = self.purge_counters.set(counters);
    }

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

    /// The webhook backlog, set by the dispatcher once per pass, and the
    /// dispatcher's progress with it (ADR-187).
    ///
    /// Set rather than accumulated: it describes a state at an instant, and
    /// the dispatcher computes it while walking the registry. Recomputing it
    /// on every `/metrics` scrape would re-read the progress collection once
    /// per subscription for a number the dispatcher had in hand two seconds
    /// earlier. That cost is why this one stays with its writer while the
    /// subscription counts are read at scrape: an age says how old it is.
    ///
    /// Covers only subscriptions **this node owns**. A node that has stood
    /// down must not report a backlog it is not the one working through, or
    /// every node in a cluster would alert for the same lag.
    ///
    /// **Called only by a pass that read the registry.** A pass whose read
    /// failed has no backlog to report, and writing 0 there would be a
    /// healthy value after a failed read.
    pub fn set_webhook_backlog(&self, backlog_secs: u64) {
        self.webhook_backlog_secs.store(backlog_secs, Ordering::Relaxed);
        *self.dispatcher_progress.lock() = Some(Instant::now());
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
    ///
    /// Milliseconds, and rendered to the millisecond (ADR-175). It was whole
    /// seconds, truncated, which cannot read an effect of a few seconds.
    pub fn set_replication_lag_ms(&self, ms: u64) {
        self.replication_lag_ms.store(ms, Ordering::Relaxed);
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
        self.sync_ddl_applied_pull.fetch_add(round.ddl_applied as u64, Ordering::Relaxed);
        self.sync_ddl_held_pull.fetch_add(round.ddl_held as u64, Ordering::Relaxed);
        self.sync_divergent_collections
            .store(round.divergent_collections as u64, Ordering::Relaxed);
        self.sync_divergence_checks.fetch_add(round.divergence_checks as u64, Ordering::Relaxed);
        self.sync_divergence_skips.fetch_add(round.divergence_skips as u64, Ordering::Relaxed);
        self.sync_divergence_count_compared
            .fetch_add(round.divergence_count_compared as u64, Ordering::Relaxed);
        self.sync_divergence_count_deferred
            .fetch_add(round.divergence_count_deferred as u64, Ordering::Relaxed);
        *self.sync_divergence_last_check.lock() = round.divergence_last_check;
        *self.replication_progress.lock() = round.last_completed_round;
        self.record_entries_skipped(
            round.entries_skipped_unknown_collection as u64,
            round.entries_skipped_beyond_advertised as u64,
            round.entries_skipped_purge_pending as u64,
        );
        self.sync_repair_rounds.fetch_add(round.repair_rounds as u64, Ordering::Relaxed);
        self.sync_pulls.lock().add(&round.pulls);
    }

    /// Count what a batch left rather than took (ADR-148), on the series a
    /// pulled and a pushed batch share, for the reason
    /// [`Self::record_ddl_refused`] gives: `unknown_collection` batches
    /// stopped at a collection this node lacks, `beyond_advertised` entries
    /// left for a later window, `purge_pending` batches stopped at a creation
    /// waiting for the drop purger (ADR-189).
    pub fn record_entries_skipped(
        &self,
        unknown_collection: u64,
        beyond_advertised: u64,
        purge_pending: u64,
    ) {
        self.sync_entries_skipped_unknown_collection
            .fetch_add(unknown_collection, Ordering::Relaxed);
        self.sync_entries_skipped_beyond_advertised.fetch_add(beyond_advertised, Ordering::Relaxed);
        self.sync_entries_skipped_purge_pending.fetch_add(purge_pending, Ordering::Relaxed);
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

    /// Everything a window a peer pushed to this node did that its metrics
    /// count (ADR-140): what it refused, declined and skipped, on the series
    /// a pulled window's land on, and what it applied and what it already
    /// held, under `via="push"`.
    /// One call per pushed window, from the node's push hook.
    pub fn record_pushed(&self, outcome: &kimmy_storage::SyncOutcome) {
        self.record_ddl_refused(outcome.ddl_refused as u64);
        self.record_ddl_declined(outcome.ddl_declined as u64);
        self.record_ddl_applied_push(outcome.ddl as u64);
        self.record_ddl_held_push(outcome.ddl_held as u64);
        self.record_entries_skipped(
            outcome.unknown_collection as u64,
            outcome.deferred as u64,
            outcome.purge_pending as u64,
        );
    }

    /// One schema-change confirmation on one member, by how it ended
    /// (ADR-191). Called once per member per change, from the confirmer.
    pub fn record_ddl_confirmation(&self, outcome: kimmy_cluster::ConfirmOutcome) {
        self.ddl_confirmations[outcome.slot()].fetch_add(1, Ordering::Relaxed);
    }

    /// One window pushed to confirm schema changes (ADR-191).
    pub fn record_ddl_confirm_push(&self) {
        self.ddl_confirm_pushes.fetch_add(1, Ordering::Relaxed);
    }

    /// Count schema changes a peer pushed to this node that it applied
    /// (ADR-140), beside the pulled ones on the same series under
    /// `via="push"`. Unlike the refusal, which way the change arrived is the
    /// point here: a push that confirms one change carries the whole window
    /// the member lacks (ADR-143), and the pushed count is where the work a
    /// burst costs shows. A change whose entry this node already held as sent
    /// is not applied again and is not counted here, but under
    /// [`Self::record_ddl_held_push`].
    pub fn record_ddl_applied_push(&self, n: u64) {
        self.sync_ddl_applied_push.fetch_add(n, Ordering::Relaxed);
    }

    /// Count schema changes a peer pushed to this node that it already held,
    /// entry and all, under `via="push"`: not applied again, and its append
    /// committed nothing.
    pub fn record_ddl_held_push(&self, n: u64) {
        self.sync_ddl_held_push.fetch_add(n, Ordering::Relaxed);
    }

    /// One backup produced, and how long the walk and the spill took
    /// (ADR-170). Recorded when the backup exists, before it is sent.
    pub fn record_backup(&self, elapsed: std::time::Duration) {
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        if let Some(slot) = BACKUP_BUCKETS_US.iter().position(|&upper| micros <= upper) {
            self.backup_buckets[slot].fetch_add(1, Ordering::Relaxed);
        }
        self.backup_sum_us.fetch_add(micros, Ordering::Relaxed);
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
    pub fn record_expiry(&self, expired: u64, skipped: u64, skipped_filter: u64) {
        self.ttl_expired.fetch_add(expired, Ordering::Relaxed);
        self.ttl_skipped.fetch_add(skipped, Ordering::Relaxed);
        self.ttl_skipped_filter.fetch_add(skipped_filter, Ordering::Relaxed);
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
    /// the instant the replication loop last reported a check, and since the
    /// process started before the first (ADR-145, ADR-154, ADR-187).
    ///
    /// Computed here, at the read, and not stored: a stored age is as old
    /// as the tick that stored it, and the member this was written for had
    /// a tick that did not end for an hour. `now` is a parameter so a test
    /// can read the age ninety seconds after a check without waiting ninety
    /// seconds; every caller outside a test passes `Instant::now()`.
    ///
    /// **Never 0 for "not yet".** It read 0 before the first check, which is
    /// also what a check a moment ago reads, so a member that had never
    /// checked anything reported the freshest possible reading (ADR-187).
    ///
    /// **0 on a node that runs no replication loop**, which is a node with
    /// clustering off: there is nothing to check against, and an age that
    /// climbed from the start there would fire the documented alert on every
    /// standalone node and never clear. Such a node has no replication row
    /// either, so this is the one reading of the pair it keeps.
    fn sync_divergence_check_age_secs_at(&self, now: Instant) -> u64 {
        if !self.runs("replication") {
            return 0;
        }
        self.age_at(*self.sync_divergence_last_check.lock(), now)
    }

    /// Whether this node runs `writer`: every writer, until startup has fixed
    /// the set ([`Self::fix_progress_writers`]).
    fn runs(&self, writer: &str) -> bool {
        self.progress_writers.get().is_none_or(|ws| ws.contains(&writer))
    }

    /// Seconds from `last` to `now`, or from the process start when there is
    /// no `last` yet: the one rule every age on this page follows (ADR-187).
    fn age_at(&self, last: Option<Instant>, now: Instant) -> u64 {
        now.saturating_duration_since(last.unwrap_or(self.started)).as_secs()
    }

    /// Fix which of [`PROGRESS_WRITERS`] this node runs, from the tasks
    /// startup supervised. Called once, after every background task is
    /// spawned and before the HTTP listener binds, so the label set is the
    /// same on every scrape; a second call is ignored.
    ///
    /// A writer this node does not start has no row, rather than a row that
    /// climbs for ever or one that reads 0: replication on a node with
    /// clustering off, or the embedding worker where it is disabled.
    pub fn fix_progress_writers(&self, started: &[&'static str]) {
        let writers = PROGRESS_WRITERS.iter().copied().filter(|w| started.contains(w)).collect();
        let _ = self.progress_writers.set(writers);
    }

    /// Each writer's progress age as of `now`, in [`PROGRESS_WRITERS`] order,
    /// `None` for one this node does not run.
    fn task_progress_ages_at(&self, now: Instant) -> [Option<u64>; PROGRESS_WRITERS.len()] {
        PROGRESS_WRITERS.map(|writer| {
            let last = match writer {
                "drop_purger" => self.purge_counters.get().and_then(|c| c.last_progress()),
                "embedding_worker" => self.vector_counters.get().and_then(|c| c.last_progress()),
                "replication" => *self.replication_progress.lock(),
                "stall_probe" => *self.stall_probe_progress.lock(),
                "webhook_dispatcher" => *self.dispatcher_progress.lock(),
                _ => unreachable!("every progress writer has a source"),
            };
            self.runs(writer).then(|| self.age_at(last, now))
        })
    }

    /// Record how late the runtime probe woke up. Keeps the maximum until the
    /// next scrape reads it, so a one-off stall between scrapes is not lost.
    /// Each wake is the probe's progress (ADR-187): a probe that has stopped
    /// waking reads 0 here for ever, and only its age says so.
    ///
    /// Fed to both marks: each reader gets the worst stall since *its own* last
    /// read, and neither can consume the other's.
    pub fn record_runtime_stall(&self, late: std::time::Duration) {
        let us = u64::try_from(late.as_micros()).unwrap_or(u64::MAX);
        self.runtime_stall_us.fetch_max(us, Ordering::Relaxed);
        self.runtime_stall_otlp_us.fetch_max(us, Ordering::Relaxed);
        *self.stall_probe_progress.lock() = Some(Instant::now());
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
            write_lock_hold: readings.writer_hold_decomposition,
            sync_serve: readings.serve,
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
            backup_duration_sum_us: self.get(&self.backup_sum_us),
            ttl_expired: self.get(&self.ttl_expired),
            ttl_skipped: self.get(&self.ttl_skipped),
            ttl_skipped_filter: self.get(&self.ttl_skipped_filter),
            index_unkeyed: readings.index_unkeyed,
            index_undecidable: readings.index_undecidable,
            webhook_delivered: self.get(&self.webhook_delivered),
            webhook_failed: self.get(&self.webhook_failed),
            webhook_events: self.get(&self.webhook_events),
            webhook_active: readings.webhook_active,
            webhook_invalidated: readings.webhook_invalidated,
            webhook_unreadable: readings.webhook_unreadable,
            webhook_backlog_secs: self.get(&self.webhook_backlog_secs),
            cluster_members: readings.cluster_members,
            task_progress_age_secs: self.task_progress_ages_at(now),
            replication_lag_ms: self.get(&self.replication_lag_ms),
            sync_failures: self.get(&self.sync_failures),
            sync_peers_backing_off: self.get(&self.sync_peers_backing_off),
            sync_ddl_refused: self.get(&self.sync_ddl_refused),
            sync_ddl_declined: self.get(&self.sync_ddl_declined),
            sync_ddl_applied_pull: self.get(&self.sync_ddl_applied_pull),
            sync_ddl_applied_push: self.get(&self.sync_ddl_applied_push),
            sync_ddl_held_pull: self.get(&self.sync_ddl_held_pull),
            sync_ddl_held_push: self.get(&self.sync_ddl_held_push),
            ddl_confirmations: std::array::from_fn(|slot| self.get(&self.ddl_confirmations[slot])),
            ddl_confirm_pushes: self.get(&self.ddl_confirm_pushes),
            sync_ddl_relogged: readings.sync_ddl_relogged,
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
            sync_entries_skipped_purge_pending: self.get(&self.sync_entries_skipped_purge_pending),
            sync_held_marks_released: readings.held_marks_released,
            sync_held_marks: readings.held_marks,
            sync_repair_rounds: self.get(&self.sync_repair_rounds),
            sync_pulls: *self.sync_pulls.lock(),
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
            embed_skipped_no_shadow: self
                .vector_counters
                .get()
                .map_or(0, |c| c.skipped_no_shadow.load(Ordering::Relaxed)),
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
        // One line per declared task, always, including the ones at 0: the rule
        // for this page is that no series is conditional, so the label set comes
        // from the declared task list rather than from what has happened to
        // retry (`kimmy_task::TASKS`).
        // One line per outcome, always: the label set is the enum's
        // (ADR-191), not what has happened.
        let ddl_confirmations = kimmy_cluster::ConfirmOutcome::ALL
            .iter()
            .map(|outcome| {
                format!(
                    "kimmy_ddl_confirmations_total{{outcome=\"{}\"}} {}\n",
                    outcome.label(),
                    self.get(&self.ddl_confirmations[outcome.slot()])
                )
            })
            .collect::<String>();
        let task_retries = kimmy_task::retries()
            .into_iter()
            .map(|(task, n)| format!("kimmy_task_retries_total{{task=\"{task}\"}} {n}\n"))
            .collect::<String>();
        // One line per writer this node runs, fixed at startup: a row that
        // appeared later would be a series a dashboard could lose (ADR-187).
        let task_progress = PROGRESS_WRITERS
            .iter()
            .zip(self.task_progress_ages_at(now))
            .filter_map(|(task, age)| {
                age.map(|age| format!("kimmy_task_progress_age_seconds{{task=\"{task}\"}} {age}\n"))
            })
            .collect::<String>();

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
        let embed_no_shadow = vc.map_or(0, |c| c.skipped_no_shadow.load(Ordering::Relaxed));
        let [t_connect, t_timeout, t_reset, t_other] = transport;
        // Copied once, so the counters and the histograms below are the same
        // ticks' worth.
        let pulls = *self.sync_pulls.lock();
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
             {hold_decomposition}\
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
             # HELP kimmy_task_retries_total Times a supervised background task retried its work in place after a transient failure, by task. A task whose count rises while nothing else changes is retrying for ever: alive, and doing no work. For the embedding worker, read it beside kimmy_task_progress_age_seconds, which rises through a retry that never succeeds.\n\
             # TYPE kimmy_task_retries_total counter\n\
             {task_retries}\
             # HELP kimmy_task_progress_age_seconds Seconds since a background writer last completed its work, computed when this page is read: a completed replication round, a stall-probe wake, a dispatcher pass, an embedding flush or idle turn. Since the process started before the first, never 0. Alert on this, and read the gauges a writer sets only while its age is fresh: a dead, stuck or retrying writer leaves them at their last value. A writer this node does not run has no row.\n\
             # TYPE kimmy_task_progress_age_seconds gauge\n\
             {task_progress}\
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
             # HELP kimmy_ttl_skipped_filter_total Expiry candidates a TTL index held that its partial filter, evaluated as find evaluates it, did not select when the delete re-read the document, and were not deleted. A document moved out of the filter while the pass ran, or one the index should never have held. Should fall to near zero once partial-index membership agrees with the filter; until then each one is a document expiry used to delete.\n\
             # TYPE kimmy_ttl_skipped_filter_total counter\n\
             kimmy_ttl_skipped_filter_total {ttl_skipped_filter}\n\
             # HELP kimmy_index_unkeyed_total Documents stored under an index that could not key them - arrays at two of a compound index's paths, more than 1000 keys, or a Decimal128 - and are rechecked on every scan of that index instead. Each one is logged at warning naming the index and the document; the index listing reports how many stand under each index as `unkeyed`. This counts that reason only; a document held because a partial filter could not decide it is kimmy_index_undecidable_total.\n\
             # TYPE kimmy_index_unkeyed_total counter\n\
             kimmy_index_unkeyed_total {index_unkeyed}\n\
             # HELP kimmy_index_undecidable_total Documents an index holds because its partial filter could not decide them: a Decimal128 at a filtered path, which the canonical order ranks equal to every number, so the filter's answer is not an answer. The index holds them and every scan re-checks them, which is what stops a partial index missing documents find returns. Expected, not a fault - the separate kimmy_index_unkeyed_total counts documents an index could not key, which is one.\n\
             # TYPE kimmy_index_undecidable_total counter\n\
             kimmy_index_undecidable_total {index_undecidable}\n\
             # HELP kimmy_webhook_deliveries_total Webhook delivery attempts by outcome.\n\
             # TYPE kimmy_webhook_deliveries_total counter\n\
             kimmy_webhook_deliveries_total{{outcome=\"delivered\"}} {wh_ok}\n\
             kimmy_webhook_deliveries_total{{outcome=\"failed\"}} {wh_fail}\n\
             # HELP kimmy_webhook_events_total Change events pushed to endpoints.\n\
             # TYPE kimmy_webhook_events_total counter\n\
             kimmy_webhook_events_total {wh_events}\n\
             # HELP kimmy_webhook_subscriptions Registered subscriptions by state, counted from this node's registry when this page is read.\n\
             # TYPE kimmy_webhook_subscriptions gauge\n\
             kimmy_webhook_subscriptions{{state=\"active\"}} {wh_active}\n\
             kimmy_webhook_subscriptions{{state=\"invalidated\"}} {wh_invalid}\n\
             kimmy_webhook_subscriptions{{state=\"unreadable\"}} {wh_unreadable}\n\
             # HELP kimmy_webhook_backlog_seconds Age of the oldest undelivered event, across subscriptions this node owns, as of the dispatcher's last pass; kimmy_task_progress_age_seconds says how old that is.\n\
             # TYPE kimmy_webhook_backlog_seconds gauge\n\
             kimmy_webhook_backlog_seconds {wh_backlog}\n\
             # HELP kimmy_cluster_members Peers this node's SWIM membership currently considers alive, counted when this page is read. 0 with clustering off.\n\
             # TYPE kimmy_cluster_members gauge\n\
             kimmy_cluster_members {cluster}\n\
             # HELP kimmy_replication_lag_seconds Seconds since the newest peer entry applied locally where a peer holds newer, max over peers in the last sync round, to the millisecond. Measured after each round against the vector the peer advertised when its pull opened, so it reads 0 once a round's pull reached that vector - including while entries the peer wrote since wait up to cluster.sync_interval_secs for the next round. Non-zero means a round ended with a pull still truncated: a backlog deeper than a tick could drain. 0 when clustering is off.\n\
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
             # HELP kimmy_sync_ddl_applied_total Replicated schema changes this node applied, by how they arrived: pull, a window this node pulled from a peer; push, a window a peer pushed to confirm a change it made (ADR-140). Counted per entry applied, not per entry received: a refused, declined or skipped entry is not counted here, and neither is a replayed drop this node had already recorded or a change for a collection dropped here, which no outcome series counts. Nor is a change whose entry this node already held as sent, which is not applied again and whose append commits nothing (a kind's own writes are unchanged); it is counted in kimmy_sync_ddl_held_total. A burst of N index changes on one member should read about N on each other member, summed over both labels; compare the increase over a burst, not the total.\n\
             # TYPE kimmy_sync_ddl_applied_total counter\n\
             kimmy_sync_ddl_applied_total{{via=\"pull\"}} {sync_ddl_applied_pull}\n\
             kimmy_sync_ddl_applied_total{{via=\"push\"}} {sync_ddl_applied_push}\n\
             # HELP kimmy_sync_ddl_held_total Replicated schema changes a window carried that this node already held, entry and all, by how they arrived: pull or push, as for kimmy_sync_ddl_applied_total. Not applied again: the append of the entry commits nothing, though a kind's own writes are unchanged (a drop of an index already gone still records its tombstone). Windows overlap by design - a pull that read this node's position before a push landed, a third member relaying what the origin pushed - and this is how much; for an index create it costs a read, not a commit.\n\
             # TYPE kimmy_sync_ddl_held_total counter\n\
             kimmy_sync_ddl_held_total{{via=\"pull\"}} {sync_ddl_held_pull}\n\
             kimmy_sync_ddl_held_total{{via=\"push\"}} {sync_ddl_held_push}\n\
             # HELP kimmy_ddl_confirmations_total Schema-change confirmations on a member, one per member per index create or drop this node made (ADR-140), by how each ended (ADR-191). confirmed: the member took the change and did not refuse it. refused: it could not apply it, or declined a drop older than the index it holds. The rest are pending, and anti-entropy carries the change: timeout, the request's deadline passed first; failed, the push errored or timed out; unreached, the member is more than a batch behind or below the retention horizon; purging, it is still purging a drop of the name; stopped_unknown, its batch stopped earlier at a collection it lacks; other_member, a different node answered at the address; task_ended, the push task panicked or was aborted; backoff, the member did not answer the last push and is not pushed to for a while; unattributable, the member runs a version whose answer does not name changes; cancelled, the request went away before an answer, with its client.\n\
             # TYPE kimmy_ddl_confirmations_total counter\n\
             {ddl_confirmations}\
             # HELP kimmy_ddl_confirm_pushes_total Windows pushed to members to confirm schema changes (ADR-191). At most one is in flight per member, and each carries everything queued for it, so in a burst this rises far slower than kimmy_ddl_confirmations_total.\n\
             # TYPE kimmy_ddl_confirm_pushes_total counter\n\
             kimmy_ddl_confirm_pushes_total {ddl_confirm_pushes}\n\
             # HELP kimmy_sync_ddl_relogged_total Schema changes a snapshot restore appended to this node's oplog so that it can serve them onward. Not an error: 0 on a member that never caught up by snapshot, and one per index definition a snapshot restored where it did not already hold the entry.\n\
             # TYPE kimmy_sync_ddl_relogged_total counter\n\
             kimmy_sync_ddl_relogged_total {sync_ddl_relogged}\n\
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
             # HELP kimmy_sync_divergence_check_age_seconds Seconds since the last contact, with any peer, in which the cross-member divergence check ran, computed when this page is read. Before the first such contact, seconds since the process started, never 0; 0 on a node without clustering, so alert on it only where kimmy_task_progress_age_seconds has a replication row. Above a few multiples of cluster.sync_interval_secs, kimmy_sync_divergent_collections is holding a value nothing has re-examined, whether the rounds are failing or the loop itself is stuck - look at kimmy_sync_failures_total, kimmy_sync_peers_backing_off and kimmy_write_lock_wait_seconds.\n\
             # TYPE kimmy_sync_divergence_check_age_seconds gauge\n\
             kimmy_sync_divergence_check_age_seconds {sync_div_age}\n\
             # HELP kimmy_sync_entries_skipped_total Replicated entries a sync round left rather than took. unknown_collection: batches stopped at an entry for a collection this node has no record of - neither holding it nor a tombstone for it - because its creation was witnessed here without being applied, or has aged out of the peer's oplog; one per stopped batch, the window is re-served from the same place every round, and the round plans a snapshot from the peer to bring the collection. A collection dropped here is history instead and stops nothing. beyond_advertised: entries above the vector the peer advertised before serving the window, left for the next round, which asks for them from the right position; ordinary and rare on a busy cluster. A hole of either kind reads 0 on kimmy_replication_lag_seconds; this and kimmy_sync_divergent_collections are what move. purge_pending: batches stopped at a replicated creation, and snapshot pages that would create a collection, of a name whose earlier collection this node's drop purger is still removing (ADR-189); nothing is missing, so no snapshot is planned and a repair waiting on one is not abandoned, the window or page is asked for again from the same place until the purge is done, and nothing stamped after the creation is taken meanwhile, so kimmy_sync_divergence_check_age_seconds rises by design.\n\
             # TYPE kimmy_sync_entries_skipped_total counter\n\
             kimmy_sync_entries_skipped_total{{reason=\"unknown_collection\"}} {sync_skipped_unknown}\n\
             kimmy_sync_entries_skipped_total{{reason=\"beyond_advertised\"}} {sync_skipped_beyond}\n\
             kimmy_sync_entries_skipped_total{{reason=\"purge_pending\"}} {sync_skipped_purge}\n\
             # HELP kimmy_sync_held_marks_released_total Entries this node held as state - written by a snapshot page, a carried delete or a scoped repair, above the vector it advertises - that arrived in a sync window served contiguously from its position and were released: the mark removed and both vectors raised over the entry. One per entry, counted when the batch commits. The release path itself: the beyond_advertised reason of kimmy_sync_entries_skipped_total rises on a peer while these entries are held and also for the ordinary race, and only this tells the two apart.\n\
             # TYPE kimmy_sync_held_marks_released_total counter\n\
             kimmy_sync_held_marks_released_total {sync_held_released}\n\
             # HELP kimmy_sync_held_marks Entries this node holds as state rather than history - written by a snapshot page, a carried delete or a scoped repair above the vector it advertises - and still waiting to arrive in a sync window contiguous from its position, which releases them. A gauge, read at scrape. Non-zero is not a fault: a member caught up by snapshot holds entries until they arrive as history. Non-zero and not falling over many rounds is a member whose peers are not serving those entries, and each pull names them to its peers as spans until they are.\n\
             # TYPE kimmy_sync_held_marks gauge\n\
             kimmy_sync_held_marks {sync_held_marks}\n\
             # HELP kimmy_sync_repair_rounds_total Sync rounds spent repairing against a peer: re-serving its oplog from the divergent collection's creation, or pulling its snapshot, after the divergence check confirmed a collection against it or a batch stopped at a collection this node lacks. Rising is a repair under way; it stops when the repair reaches the peer's tail.\n\
             # TYPE kimmy_sync_repair_rounds_total counter\n\
             kimmy_sync_repair_rounds_total {sync_repair_rounds}\n\
             # HELP kimmy_sync_pulled_entries_total Entries sync pulls carried from peers, whatever became of each - applied, superseded, a schema change or left for a later window. Divide the growth of kimmy_sync_pull_seconds_sum{{phase=\"apply\"}} by the growth of this for what applying one entry costs, whatever size the batches were.\n\
             # TYPE kimmy_sync_pulled_entries_total counter\n\
             kimmy_sync_pulled_entries_total {sync_pulled_entries}\n\
             # HELP kimmy_sync_entry_wait_ahead_total Sync pulls whose oldest entry this node lacked carried a timestamp later than this node's clock read when the batch arrived, so its wait could not be taken and is not in kimmy_sync_entry_wait_seconds. A peer's clock, or one its stamps witnessed, runs ahead of this node's; rising steadily is clock skew between members, and the wait histogram is under-reading by the pulls counted here.\n\
             # TYPE kimmy_sync_entry_wait_ahead_total counter\n\
             kimmy_sync_entry_wait_ahead_total {sync_entry_wait_ahead}\n\
             # HELP kimmy_sync_contacts_total Contacts with a peer in a sync tick, by how they ended. caught_up: the last pull did not come back truncated, so nothing more could be pulled at once. budget: a pull came back truncated and the next would not have fitted in what was left of cluster.sync_interval_secs, so a backlog was carried into the next tick. ceiling: truncated with time left, after the most pulls one contact may make. failed: a pull failed. budget rising is a backlog outliving a tick; kimmy_sync_pull_seconds says whether the tick's time went to the peer serving, to waiting for this node's writer, or to applying.\n\
             # TYPE kimmy_sync_contacts_total counter\n\
             kimmy_sync_contacts_total{{ended=\"caught_up\"}} {sync_ended_caught_up}\n\
             kimmy_sync_contacts_total{{ended=\"budget\"}} {sync_ended_budget}\n\
             kimmy_sync_contacts_total{{ended=\"ceiling\"}} {sync_ended_ceiling}\n\
             kimmy_sync_contacts_total{{ended=\"failed\"}} {sync_ended_failed}\n\
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
             # HELP kimmy_embed_skipped_no_shadow_total Documents and scans skipped because a collection is configured for vectors and its shadow collection is not on this node. Should read 0; rising means a configuration without the collection its vectors are stored in.\n\
             # TYPE kimmy_embed_skipped_no_shadow_total counter\n\
             kimmy_embed_skipped_no_shadow_total {embed_no_shadow}\n\
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
            hold_decomposition = render_hold_decomposition(&readings.writer_hold_decomposition),
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
            ttl_skipped_filter = self.get(&self.ttl_skipped_filter),
            index_unkeyed = readings.index_unkeyed,
            index_undecidable = readings.index_undecidable,
            wh_active = readings.webhook_active,
            wh_invalid = readings.webhook_invalidated,
            wh_unreadable = readings.webhook_unreadable,
            wh_backlog = self.get(&self.webhook_backlog_secs),
            cluster = readings.cluster_members,
            lag = self.get(&self.replication_lag_ms) as f64 / 1e3,
            sync_failures = self.get(&self.sync_failures),
            sync_backing_off = self.get(&self.sync_peers_backing_off),
            sync_ddl_refused = self.get(&self.sync_ddl_refused),
            sync_ddl_declined = self.get(&self.sync_ddl_declined),
            sync_ddl_applied_pull = self.get(&self.sync_ddl_applied_pull),
            sync_ddl_applied_push = self.get(&self.sync_ddl_applied_push),
            sync_ddl_held_pull = self.get(&self.sync_ddl_held_pull),
            sync_ddl_held_push = self.get(&self.sync_ddl_held_push),
            ddl_confirm_pushes = self.get(&self.ddl_confirm_pushes),
            sync_ddl_relogged = readings.sync_ddl_relogged,
            sync_divergent = self.get(&self.sync_divergent_collections),
            sync_div_ran = self.get(&self.sync_divergence_checks),
            sync_div_skipped = self.get(&self.sync_divergence_skips),
            sync_div_compared = self.get(&self.sync_divergence_count_compared),
            sync_div_deferred = self.get(&self.sync_divergence_count_deferred),
            sync_div_age = self.sync_divergence_check_age_secs_at(now),
            sync_skipped_unknown = self.get(&self.sync_entries_skipped_unknown_collection),
            sync_skipped_beyond = self.get(&self.sync_entries_skipped_beyond_advertised),
            sync_skipped_purge = self.get(&self.sync_entries_skipped_purge_pending),
            sync_held_released = readings.held_marks_released,
            sync_held_marks = readings.held_marks,
            sync_repair_rounds = self.get(&self.sync_repair_rounds),
            sync_pulled_entries = pulls.entries,
            sync_entry_wait_ahead = pulls.entry_wait_ahead,
            sync_ended_caught_up = pulls.contacts[kimmy_cluster::ContactEnd::CaughtUp.slot()],
            sync_ended_budget = pulls.contacts[kimmy_cluster::ContactEnd::Budget.slot()],
            sync_ended_ceiling = pulls.contacts[kimmy_cluster::ContactEnd::Ceiling.slot()],
            sync_ended_failed = pulls.contacts[kimmy_cluster::ContactEnd::Failed.slot()],
            frozen = kimmy_cluster::FROZEN_CONTACTS,
            tls_ok = self.get(&self.tls_reloads_ok),
            tls_fail = self.get(&self.tls_reloads_failed),
            embed_docs = embed_docs,
            embed_chunks = embed_chunks,
            embed_deferred = embed_deferred,
            embed_not_owned = embed_not_owned,
            embed_no_shadow = embed_no_shadow,
            embed_failures = embed_failures,
            jwks_ok = self.get(&self.jwks_refresh_ok),
            jwks_fail = self.get(&self.jwks_refresh_failed),
        );
        self.render_latency(&mut out);
        self.render_backup_duration(&mut out);
        render_sync_pulls(&mut out, &pulls);
        render_sync_serve(&mut out, &readings.serve);
        out
    }

    /// The backup-duration histogram (ADR-170), in the latency histogram's
    /// shape. Its count is the backup counter, read once here so the two
    /// cannot disagree within a scrape.
    fn render_backup_duration(&self, out: &mut String) {
        use std::fmt::Write;

        out.push_str(
            "# HELP kimmy_backup_duration_seconds How long a backup took to produce: the walk of the whole store and its spill to disk, before any of it was sent. Follows the store's size and whether the file is in page cache.\n\
             # TYPE kimmy_backup_duration_seconds histogram\n",
        );
        let mut cumulative = 0u64;
        for (slot, upper) in BACKUP_BUCKETS_US.iter().enumerate() {
            cumulative += self.get(&self.backup_buckets[slot]);
            let le = *upper as f64 / 1e6;
            let _ =
                writeln!(out, "kimmy_backup_duration_seconds_bucket{{le=\"{le}\"}} {cumulative}");
        }
        let count = self.get(&self.backups);
        let sum = self.get(&self.backup_sum_us) as f64 / 1e6;
        let _ = writeln!(out, "kimmy_backup_duration_seconds_bucket{{le=\"+Inf\"}} {count}");
        let _ = writeln!(out, "kimmy_backup_duration_seconds_sum {sum}");
        let _ = writeln!(out, "kimmy_backup_duration_seconds_count {count}");
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

/// The two sync-pull histograms (ADR-175), in Prometheus's cumulative-bucket
/// form. Every phase is rendered whether or not a pull has run, for the reason
/// every holder of the writer-hold histogram is: a dashboard split by phase
/// must not gain a series the first time a peer answers.
fn render_sync_pulls(out: &mut String, pulls: &kimmy_cluster::PullReport) {
    use std::fmt::Write;

    out.push_str(
        "# HELP kimmy_sync_pull_seconds Where a sync pull's time went, by phase, one observation per pull of a peer's oplog. serve: from asking the peer for a window to holding it - the peer walking its oplog and the wire. wait: applying the window, waiting for this node's single writer behind whatever else was writing. apply: applying the window less that wait - every entry's work, the commits and their fsync. The three add up to the pull, and each is a different fix. Under storage.durability = coalesced, the wait for the shared flush's window and for the flush another committer leads is in apply, so there apply includes queueing behind other committers. A snapshot page is not a pull and is not observed here.\n\
         # TYPE kimmy_sync_pull_seconds histogram\n",
    );
    for (phase, histogram) in
        [("serve", &pulls.serve), ("wait", &pulls.wait), ("apply", &pulls.apply)]
    {
        let mut cumulative = 0u64;
        for (slot, upper) in kimmy_cluster::PULL_BUCKETS_US.iter().enumerate() {
            cumulative += histogram.buckets[slot];
            let le = *upper as f64 / 1e6;
            let _ = writeln!(
                out,
                "kimmy_sync_pull_seconds_bucket{{phase=\"{phase}\",le=\"{le}\"}} {cumulative}"
            );
        }
        let count = histogram.count;
        let sum = histogram.sum_us as f64 / 1e6;
        let _ = writeln!(
            out,
            "kimmy_sync_pull_seconds_bucket{{phase=\"{phase}\",le=\"+Inf\"}} {count}"
        );
        let _ = writeln!(out, "kimmy_sync_pull_seconds_sum{{phase=\"{phase}\"}} {sum}");
        let _ = writeln!(out, "kimmy_sync_pull_seconds_count{{phase=\"{phase}\"}} {count}");
    }

    out.push_str(
        "# HELP kimmy_sync_entry_wait_seconds How long the oldest entry a sync pull carried that this node lacked had waited when the pull arrived: from its origin's timestamp to this node's clock. The time an entry spends before any pull takes it - waiting for the next tick, or behind a backlog - which kimmy_sync_pull_seconds does not see. Crosses member clocks, so skew shifts it; a wait that would be negative is counted in kimmy_sync_entry_wait_ahead_total instead.\n\
         # TYPE kimmy_sync_entry_wait_seconds histogram\n",
    );
    let wait = &pulls.entry_wait;
    let mut cumulative = 0u64;
    for (slot, upper) in kimmy_cluster::ENTRY_WAIT_BUCKETS_US.iter().enumerate() {
        cumulative += wait.buckets[slot];
        let le = *upper as f64 / 1e6;
        let _ = writeln!(out, "kimmy_sync_entry_wait_seconds_bucket{{le=\"{le}\"}} {cumulative}");
    }
    let sum = wait.sum_us as f64 / 1e6;
    let _ = writeln!(out, "kimmy_sync_entry_wait_seconds_bucket{{le=\"+Inf\"}} {}", wait.count);
    let _ = writeln!(out, "kimmy_sync_entry_wait_seconds_sum {sum}");
    let _ = writeln!(out, "kimmy_sync_entry_wait_seconds_count {}", wait.count);
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

/// What the writer's holds were made of, per holder (ADR-176): five
/// components of what the holding thread was doing, three phases of the
/// transaction, the bytes it read and wrote, the bound on the component
/// split's error, and the holds whose measured parts came to more than the
/// hold. Every holder is rendered whether or not it has held the writer, as
/// the hold histogram's are.
fn render_hold_decomposition(d: &kimmy_storage::HoldDecomposition) -> String {
    use kimmy_storage::{HoldComponent, HoldPhase, WriterHolder};
    use std::fmt::Write;

    let seconds = |ns: u64| ns as f64 / 1e9;
    let mut out = String::from(
        "# HELP kimmy_write_lock_held_component_seconds_total Seconds holds of the storage writer spent, by holder and by what the holding thread was doing. read, write, sync: inside the storage file's page reads, page writes and fsyncs. cpu: on the CPU outside those - B-tree work over cached pages, encoding, index keys, bookkeeping. off_cpu: the rest - off the CPU outside any file call, which is scheduler delay or a wait on a lock inside the storage engine. The five add up to kimmy_write_lock_held_seconds_sum. off_cpu is a residual, so anything the other four fail to capture lands there too; read it beside kimmy_write_lock_held_write_estimated_seconds_total. cpu and off_cpu leave out holds counted in kimmy_write_lock_held_cpu_unmeasured_total.\n\
         # TYPE kimmy_write_lock_held_component_seconds_total counter\n",
    );
    for holder in WriterHolder::ALL {
        for component in HoldComponent::ALL {
            let _ = writeln!(
                out,
                "kimmy_write_lock_held_component_seconds_total{{holder=\"{}\",component=\"{}\"}} {}",
                holder.label(),
                component.label(),
                seconds(d.component_ns[holder.slot()][component.slot()])
            );
        }
    }
    out.push_str(
        "# HELP kimmy_write_lock_held_phase_seconds_total Seconds holds of the storage writer spent, by holder and by where in the transaction. work: from taking the writer to asking to commit, the whole hold of one that aborted. counts: writing the collections' live document counts, once per transaction. commit: the storage engine's commit, its page writes and fsync, to letting go. The three add up to kimmy_write_lock_held_seconds_sum.\n\
         # TYPE kimmy_write_lock_held_phase_seconds_total counter\n",
    );
    for holder in WriterHolder::ALL {
        for phase in HoldPhase::ALL {
            let _ = writeln!(
                out,
                "kimmy_write_lock_held_phase_seconds_total{{holder=\"{}\",phase=\"{}\"}} {}",
                holder.label(),
                phase.label(),
                seconds(d.phase_ns[holder.slot()][phase.slot()])
            );
        }
    }
    out.push_str(
        "# HELP kimmy_write_lock_held_io_bytes_total Bytes holds of the storage writer read from and wrote to the storage file, by holder. Beside the read and write components: more bytes is more pages, and the same bytes in more seconds is slower pages.\n\
         # TYPE kimmy_write_lock_held_io_bytes_total counter\n",
    );
    for holder in WriterHolder::ALL {
        for (io, bytes) in [("read", d.read_bytes), ("write", d.write_bytes)] {
            let _ = writeln!(
                out,
                "kimmy_write_lock_held_io_bytes_total{{holder=\"{}\",io=\"{io}\"}} {}",
                holder.label(),
                bytes[holder.slot()]
            );
        }
    }
    out.push_str(
        "# HELP kimmy_write_lock_held_write_estimated_seconds_total Seconds of page writes, inside holds of the storage writer, whose CPU time was estimated from a sample rather than read. The most by which cpu and off_cpu in kimmy_write_lock_held_component_seconds_total can be misattributed between each other, in either direction; 0 for a hold of 32 page writes or fewer, which is measured exactly.\n\
         # TYPE kimmy_write_lock_held_write_estimated_seconds_total counter\n",
    );
    for holder in WriterHolder::ALL {
        let _ = writeln!(
            out,
            "kimmy_write_lock_held_write_estimated_seconds_total{{holder=\"{}\"}} {}",
            holder.label(),
            seconds(d.write_estimated_ns[holder.slot()])
        );
    }
    out.push_str(
        "# HELP kimmy_write_lock_held_overcounted_total Holds of the storage writer whose measured components came to more than the hold, past the clocks' tolerance: something was counted twice. Should read 0; it cannot see a component that was missed, which lands in off_cpu instead.\n\
         # TYPE kimmy_write_lock_held_overcounted_total counter\n",
    );
    for holder in WriterHolder::ALL {
        let _ = writeln!(
            out,
            "kimmy_write_lock_held_overcounted_total{{holder=\"{}\"}} {}",
            holder.label(),
            d.overcounted[holder.slot()]
        );
    }
    let _ = write!(
        out,
        "# HELP kimmy_write_lock_held_cpu_unmeasured_total Holds of the storage writer, of any holder, whose thread CPU time could not be read, so they are not in the cpu and off_cpu components. Rises on every hold on a platform without a per-thread CPU clock; 0 on Linux and macOS.\n\
         # TYPE kimmy_write_lock_held_cpu_unmeasured_total counter\n\
         kimmy_write_lock_held_cpu_unmeasured_total {}\n",
        d.cpu_unmeasured
    );
    out
}

/// What serving peers' windows has cost this node (ADR-176).
fn render_sync_serve(out: &mut String, serve: &kimmy_storage::ServeSnapshot) {
    use std::fmt::Write;

    let _ = write!(
        out,
        "# HELP kimmy_sync_served_windows_total Windows of this node's oplog walked for peers that pulled from it, including one too large for a frame that the peer is asked to take in fewer entries.\n\
         # TYPE kimmy_sync_served_windows_total counter\n\
         kimmy_sync_served_windows_total {}\n\
         # HELP kimmy_sync_served_entries_total Entries the windows this node served to peers carried.\n\
         # TYPE kimmy_sync_served_entries_total counter\n\
         kimmy_sync_served_entries_total {}\n\
         # HELP kimmy_sync_serve_passed_entries_total Entries the walks behind served windows examined and did not serve, mostly because the pulling peer already held them. A walk costs what it examines: this plus kimmy_sync_served_entries_total.\n\
         # TYPE kimmy_sync_serve_passed_entries_total counter\n\
         kimmy_sync_serve_passed_entries_total {}\n\
         # HELP kimmy_sync_serve_walk_read_seconds_total Seconds the walks behind served windows spent reading pages of the storage file its cache did not hold: the serving load on this node's disk.\n\
         # TYPE kimmy_sync_serve_walk_read_seconds_total counter\n\
         kimmy_sync_serve_walk_read_seconds_total {}\n\
         # HELP kimmy_sync_serve_walk_read_bytes_total Bytes the walks behind served windows read from the storage file.\n\
         # TYPE kimmy_sync_serve_walk_read_bytes_total counter\n\
         kimmy_sync_serve_walk_read_bytes_total {}\n\
         # HELP kimmy_sync_serve_walk_seconds How long this node took to walk its oplog for one window served to a peer, the wire not included.\n\
         # TYPE kimmy_sync_serve_walk_seconds histogram\n",
        serve.windows,
        serve.entries,
        serve.passed,
        serve.read_ns as f64 / 1e9,
        serve.read_bytes,
    );
    let mut cumulative = 0u64;
    for (slot, upper) in kimmy_storage::SERVE_WALK_BUCKETS_US.iter().enumerate() {
        cumulative += serve.walk_buckets[slot];
        let le = *upper as f64 / 1e6;
        let _ = writeln!(out, "kimmy_sync_serve_walk_seconds_bucket{{le=\"{le}\"}} {cumulative}");
    }
    let _ = writeln!(out, "kimmy_sync_serve_walk_seconds_bucket{{le=\"+Inf\"}} {}", serve.windows);
    let _ = writeln!(out, "kimmy_sync_serve_walk_seconds_sum {}", serve.walk_sum_us as f64 / 1e6);
    let _ = writeln!(out, "kimmy_sync_serve_walk_seconds_count {}", serve.windows);
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
    /// A tick's pull report: one pull observed at `[serve, wait, apply]`
    /// milliseconds carrying `entries`, its oldest lacked entry having waited
    /// `waited` milliseconds, `ahead` pulls that could not observe one, and
    /// contacts ended in `ContactEnd::ALL` order.
    fn pulls_observed(
        [serve, wait, apply]: [u64; 3],
        entries: usize,
        waited: Option<u64>,
        ahead: u64,
        contacts: [u64; kimmy_cluster::ContactEnd::COUNT],
    ) -> kimmy_cluster::PullReport {
        let mut report = kimmy_cluster::PullReport::default();
        report.pulled(&kimmy_storage::PullTiming {
            serve: Duration::from_millis(serve),
            wait: Duration::from_millis(wait),
            apply: Duration::from_millis(apply),
            entries,
            oldest_lacked: waited
                .map(|ms| kimmy_storage::EntryWait::Waited(Duration::from_millis(ms))),
        });
        report.entry_wait_ahead += ahead;
        report.contacts = contacts;
        report
    }

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

        m.record_backup(Duration::from_secs(42));
        m.record_expiry(11, 12, 93);
        m.record_webhook_delivery(true, 13);
        m.record_webhook_delivery(true, 14);
        m.record_webhook_delivery(false, 0);
        // The subscription counts (15, 16) and the member count (18) are
        // readings now (ADR-187), in `distinct_readings`.
        m.set_webhook_backlog(17);
        // Not a whole number of seconds: the gauge renders milliseconds
        // (ADR-175), and a render that truncated would print 19.
        m.set_replication_lag_ms(19_250);
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
            ddl_applied: 83,
            ddl_held: 43,
            ddl_refused: 21,
            ddl_declined: 28,
            divergent_collections: 12,
            divergence_checks: 30,
            divergence_skips: 33,
            divergence_count_compared: 61,
            divergence_count_deferred: 64,
            divergence_last_check: Some(now - Duration::from_secs(70)),
            last_completed_round: Some(now - Duration::from_secs(80)),
            entries_skipped_unknown_collection: 72,
            entries_skipped_beyond_advertised: 74,
            entries_skipped_purge_pending: 77,
            repair_rounds: 76,
            pulls: pulls_observed([3, 40, 700], 1_024, Some(1_500), 115, [101, 102, 103, 104]),
        });
        m.record_sync_round(&kimmy_cluster::RoundReport {
            failed: 3,
            backing_off: 24,
            ddl_applied: 6,
            ddl_held: 5,
            ddl_refused: 4,
            ddl_declined: 8,
            divergent_collections: 5,
            divergence_checks: 2,
            divergence_skips: 1,
            divergence_count_compared: 2,
            divergence_count_deferred: 3,
            divergence_last_check: Some(now - Duration::from_secs(71)),
            last_completed_round: Some(now - Duration::from_secs(81)),
            entries_skipped_unknown_collection: 1,
            entries_skipped_beyond_advertised: 1,
            entries_skipped_purge_pending: 1,
            repair_rounds: 1,
            // A wait of 45 s: past the old 10 s top, so the golden reads the
            // buckets a long wait for the writer lands in (ADR-175).
            pulls: pulls_observed([8, 45_000, 3_000], 81, Some(45_000), 2, [10, 10, 10, 10]),
        });
        // The pushed applies, on the series the pulled ones share: a push
        // recorded under `via="pull"` would read 186 there.
        m.record_ddl_applied_push(97);
        m.record_ddl_held_push(58);
        // A distinct count per outcome, so a line under the wrong label
        // cannot match: 120 for the first, one more for each after it.
        for outcome in kimmy_cluster::ConfirmOutcome::ALL {
            for _ in 0..(120 + outcome.slot()) {
                m.record_ddl_confirmation(outcome);
            }
        }
        for _ in 0..137 {
            m.record_ddl_confirm_push();
        }
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
        // The stall probe's and the dispatcher's instants are taken by the
        // calls above when they are made, so they are placed here instead,
        // each at an age of its own (ADR-187). The embedding worker has no
        // counters on this `Metrics`, so its row reads the time since the
        // process started: 99, the whole of `now`'s 100 s less the moments
        // since `Metrics::default()`.
        *m.stall_probe_progress.lock() = Some(now - Duration::from_secs(83));
        *m.dispatcher_progress.lock() = Some(now - Duration::from_secs(85));
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
            // 37 rather than a small number: the golden is asserted
            // byte-for-byte, and a value shared with another series (4 was also
            // `kimmy_responses_total{class="5xx"}`) lets an assertion that names
            // this one match the wrong line.
            index_undecidable: 37,
            sync_ddl_relogged: 91,
            webhook_active: 15,
            webhook_invalidated: 16,
            webhook_unreadable: 38,
            cluster_members: 18,
            writer_wait: kimmy_storage::WriterWaitSnapshot {
                buckets: [1, 2, 0, 0, 3, 0, 0, 1],
                count: 8,
                sum_us: 6_500_000,
            },
            writer_wait_timeouts: 51,
            writer_hold_max_us: 52_500_000,
            held_marks_released: 53,
            held_marks: 54,
            // One holder per row, none of them equal, so a row rendered
            // under another holder's label cannot match the golden. The
            // counts are the buckets' sum, as a real snapshot's are.
            writer_hold: distinct_hold(),
            writer_hold_decomposition: distinct_decomposition(),
            serve: kimmy_storage::ServeSnapshot {
                windows: 1_201,
                entries: 1_202,
                passed: 1_203,
                walk_buckets: [3, 0, 1_190, 0, 0, 0, 0, 0, 0, 0, 0, 7],
                walk_sum_us: 2_500_000,
                read_ns: 3_300_000_000,
                read_bytes: 1_204,
            },
        }
    }

    /// What the holds were made of, with every holder's, component's and
    /// phase's value distinct from every other, so a value rendered under
    /// another's labels cannot match the golden (ADR-176).
    fn distinct_decomposition() -> kimmy_storage::HoldDecomposition {
        let ms = |n: usize| n as u64 * 1_000_000;
        kimmy_storage::HoldDecomposition {
            component_ns: std::array::from_fn(|h| {
                std::array::from_fn(|c| ms((h + 1) * 100 + c + 1))
            }),
            phase_ns: std::array::from_fn(|h| std::array::from_fn(|p| ms((h + 1) * 100 + p + 11))),
            read_bytes: std::array::from_fn(|h| (h as u64 + 1) * 1_000 + 1),
            write_bytes: std::array::from_fn(|h| (h as u64 + 1) * 1_000 + 2),
            write_estimated_ns: std::array::from_fn(|h| ms((h + 1) * 100 + 21)),
            overcounted: std::array::from_fn(|h| h as u64 + 61),
            cpu_unmeasured: 99,
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
# HELP kimmy_write_lock_held_component_seconds_total Seconds holds of the storage writer spent, by holder and by what the holding thread was doing. read, write, sync: inside the storage file's page reads, page writes and fsyncs. cpu: on the CPU outside those - B-tree work over cached pages, encoding, index keys, bookkeeping. off_cpu: the rest - off the CPU outside any file call, which is scheduler delay or a wait on a lock inside the storage engine. The five add up to kimmy_write_lock_held_seconds_sum. off_cpu is a residual, so anything the other four fail to capture lands there too; read it beside kimmy_write_lock_held_write_estimated_seconds_total. cpu and off_cpu leave out holds counted in kimmy_write_lock_held_cpu_unmeasured_total.
# TYPE kimmy_write_lock_held_component_seconds_total counter
kimmy_write_lock_held_component_seconds_total{holder=\"write\",component=\"read\"} 0.101
kimmy_write_lock_held_component_seconds_total{holder=\"write\",component=\"write\"} 0.102
kimmy_write_lock_held_component_seconds_total{holder=\"write\",component=\"sync\"} 0.103
kimmy_write_lock_held_component_seconds_total{holder=\"write\",component=\"cpu\"} 0.104
kimmy_write_lock_held_component_seconds_total{holder=\"write\",component=\"off_cpu\"} 0.105
kimmy_write_lock_held_component_seconds_total{holder=\"bulk\",component=\"read\"} 0.201
kimmy_write_lock_held_component_seconds_total{holder=\"bulk\",component=\"write\"} 0.202
kimmy_write_lock_held_component_seconds_total{holder=\"bulk\",component=\"sync\"} 0.203
kimmy_write_lock_held_component_seconds_total{holder=\"bulk\",component=\"cpu\"} 0.204
kimmy_write_lock_held_component_seconds_total{holder=\"bulk\",component=\"off_cpu\"} 0.205
kimmy_write_lock_held_component_seconds_total{holder=\"ddl\",component=\"read\"} 0.301
kimmy_write_lock_held_component_seconds_total{holder=\"ddl\",component=\"write\"} 0.302
kimmy_write_lock_held_component_seconds_total{holder=\"ddl\",component=\"sync\"} 0.303
kimmy_write_lock_held_component_seconds_total{holder=\"ddl\",component=\"cpu\"} 0.304
kimmy_write_lock_held_component_seconds_total{holder=\"ddl\",component=\"off_cpu\"} 0.305
kimmy_write_lock_held_component_seconds_total{holder=\"index_build\",component=\"read\"} 0.401
kimmy_write_lock_held_component_seconds_total{holder=\"index_build\",component=\"write\"} 0.402
kimmy_write_lock_held_component_seconds_total{holder=\"index_build\",component=\"sync\"} 0.403
kimmy_write_lock_held_component_seconds_total{holder=\"index_build\",component=\"cpu\"} 0.404
kimmy_write_lock_held_component_seconds_total{holder=\"index_build\",component=\"off_cpu\"} 0.405
kimmy_write_lock_held_component_seconds_total{holder=\"drop\",component=\"read\"} 0.501
kimmy_write_lock_held_component_seconds_total{holder=\"drop\",component=\"write\"} 0.502
kimmy_write_lock_held_component_seconds_total{holder=\"drop\",component=\"sync\"} 0.503
kimmy_write_lock_held_component_seconds_total{holder=\"drop\",component=\"cpu\"} 0.504
kimmy_write_lock_held_component_seconds_total{holder=\"drop\",component=\"off_cpu\"} 0.505
kimmy_write_lock_held_component_seconds_total{holder=\"replication\",component=\"read\"} 0.601
kimmy_write_lock_held_component_seconds_total{holder=\"replication\",component=\"write\"} 0.602
kimmy_write_lock_held_component_seconds_total{holder=\"replication\",component=\"sync\"} 0.603
kimmy_write_lock_held_component_seconds_total{holder=\"replication\",component=\"cpu\"} 0.604
kimmy_write_lock_held_component_seconds_total{holder=\"replication\",component=\"off_cpu\"} 0.605
kimmy_write_lock_held_component_seconds_total{holder=\"repair\",component=\"read\"} 0.701
kimmy_write_lock_held_component_seconds_total{holder=\"repair\",component=\"write\"} 0.702
kimmy_write_lock_held_component_seconds_total{holder=\"repair\",component=\"sync\"} 0.703
kimmy_write_lock_held_component_seconds_total{holder=\"repair\",component=\"cpu\"} 0.704
kimmy_write_lock_held_component_seconds_total{holder=\"repair\",component=\"off_cpu\"} 0.705
kimmy_write_lock_held_component_seconds_total{holder=\"retention\",component=\"read\"} 0.801
kimmy_write_lock_held_component_seconds_total{holder=\"retention\",component=\"write\"} 0.802
kimmy_write_lock_held_component_seconds_total{holder=\"retention\",component=\"sync\"} 0.803
kimmy_write_lock_held_component_seconds_total{holder=\"retention\",component=\"cpu\"} 0.804
kimmy_write_lock_held_component_seconds_total{holder=\"retention\",component=\"off_cpu\"} 0.805
kimmy_write_lock_held_component_seconds_total{holder=\"expiry\",component=\"read\"} 0.901
kimmy_write_lock_held_component_seconds_total{holder=\"expiry\",component=\"write\"} 0.902
kimmy_write_lock_held_component_seconds_total{holder=\"expiry\",component=\"sync\"} 0.903
kimmy_write_lock_held_component_seconds_total{holder=\"expiry\",component=\"cpu\"} 0.904
kimmy_write_lock_held_component_seconds_total{holder=\"expiry\",component=\"off_cpu\"} 0.905
kimmy_write_lock_held_component_seconds_total{holder=\"embedding\",component=\"read\"} 1.001
kimmy_write_lock_held_component_seconds_total{holder=\"embedding\",component=\"write\"} 1.002
kimmy_write_lock_held_component_seconds_total{holder=\"embedding\",component=\"sync\"} 1.003
kimmy_write_lock_held_component_seconds_total{holder=\"embedding\",component=\"cpu\"} 1.004
kimmy_write_lock_held_component_seconds_total{holder=\"embedding\",component=\"off_cpu\"} 1.005
kimmy_write_lock_held_component_seconds_total{holder=\"durability\",component=\"read\"} 1.101
kimmy_write_lock_held_component_seconds_total{holder=\"durability\",component=\"write\"} 1.102
kimmy_write_lock_held_component_seconds_total{holder=\"durability\",component=\"sync\"} 1.103
kimmy_write_lock_held_component_seconds_total{holder=\"durability\",component=\"cpu\"} 1.104
kimmy_write_lock_held_component_seconds_total{holder=\"durability\",component=\"off_cpu\"} 1.105
kimmy_write_lock_held_component_seconds_total{holder=\"rewind\",component=\"read\"} 1.201
kimmy_write_lock_held_component_seconds_total{holder=\"rewind\",component=\"write\"} 1.202
kimmy_write_lock_held_component_seconds_total{holder=\"rewind\",component=\"sync\"} 1.203
kimmy_write_lock_held_component_seconds_total{holder=\"rewind\",component=\"cpu\"} 1.204
kimmy_write_lock_held_component_seconds_total{holder=\"rewind\",component=\"off_cpu\"} 1.205
# HELP kimmy_write_lock_held_phase_seconds_total Seconds holds of the storage writer spent, by holder and by where in the transaction. work: from taking the writer to asking to commit, the whole hold of one that aborted. counts: writing the collections' live document counts, once per transaction. commit: the storage engine's commit, its page writes and fsync, to letting go. The three add up to kimmy_write_lock_held_seconds_sum.
# TYPE kimmy_write_lock_held_phase_seconds_total counter
kimmy_write_lock_held_phase_seconds_total{holder=\"write\",phase=\"work\"} 0.111
kimmy_write_lock_held_phase_seconds_total{holder=\"write\",phase=\"counts\"} 0.112
kimmy_write_lock_held_phase_seconds_total{holder=\"write\",phase=\"commit\"} 0.113
kimmy_write_lock_held_phase_seconds_total{holder=\"bulk\",phase=\"work\"} 0.211
kimmy_write_lock_held_phase_seconds_total{holder=\"bulk\",phase=\"counts\"} 0.212
kimmy_write_lock_held_phase_seconds_total{holder=\"bulk\",phase=\"commit\"} 0.213
kimmy_write_lock_held_phase_seconds_total{holder=\"ddl\",phase=\"work\"} 0.311
kimmy_write_lock_held_phase_seconds_total{holder=\"ddl\",phase=\"counts\"} 0.312
kimmy_write_lock_held_phase_seconds_total{holder=\"ddl\",phase=\"commit\"} 0.313
kimmy_write_lock_held_phase_seconds_total{holder=\"index_build\",phase=\"work\"} 0.411
kimmy_write_lock_held_phase_seconds_total{holder=\"index_build\",phase=\"counts\"} 0.412
kimmy_write_lock_held_phase_seconds_total{holder=\"index_build\",phase=\"commit\"} 0.413
kimmy_write_lock_held_phase_seconds_total{holder=\"drop\",phase=\"work\"} 0.511
kimmy_write_lock_held_phase_seconds_total{holder=\"drop\",phase=\"counts\"} 0.512
kimmy_write_lock_held_phase_seconds_total{holder=\"drop\",phase=\"commit\"} 0.513
kimmy_write_lock_held_phase_seconds_total{holder=\"replication\",phase=\"work\"} 0.611
kimmy_write_lock_held_phase_seconds_total{holder=\"replication\",phase=\"counts\"} 0.612
kimmy_write_lock_held_phase_seconds_total{holder=\"replication\",phase=\"commit\"} 0.613
kimmy_write_lock_held_phase_seconds_total{holder=\"repair\",phase=\"work\"} 0.711
kimmy_write_lock_held_phase_seconds_total{holder=\"repair\",phase=\"counts\"} 0.712
kimmy_write_lock_held_phase_seconds_total{holder=\"repair\",phase=\"commit\"} 0.713
kimmy_write_lock_held_phase_seconds_total{holder=\"retention\",phase=\"work\"} 0.811
kimmy_write_lock_held_phase_seconds_total{holder=\"retention\",phase=\"counts\"} 0.812
kimmy_write_lock_held_phase_seconds_total{holder=\"retention\",phase=\"commit\"} 0.813
kimmy_write_lock_held_phase_seconds_total{holder=\"expiry\",phase=\"work\"} 0.911
kimmy_write_lock_held_phase_seconds_total{holder=\"expiry\",phase=\"counts\"} 0.912
kimmy_write_lock_held_phase_seconds_total{holder=\"expiry\",phase=\"commit\"} 0.913
kimmy_write_lock_held_phase_seconds_total{holder=\"embedding\",phase=\"work\"} 1.011
kimmy_write_lock_held_phase_seconds_total{holder=\"embedding\",phase=\"counts\"} 1.012
kimmy_write_lock_held_phase_seconds_total{holder=\"embedding\",phase=\"commit\"} 1.013
kimmy_write_lock_held_phase_seconds_total{holder=\"durability\",phase=\"work\"} 1.111
kimmy_write_lock_held_phase_seconds_total{holder=\"durability\",phase=\"counts\"} 1.112
kimmy_write_lock_held_phase_seconds_total{holder=\"durability\",phase=\"commit\"} 1.113
kimmy_write_lock_held_phase_seconds_total{holder=\"rewind\",phase=\"work\"} 1.211
kimmy_write_lock_held_phase_seconds_total{holder=\"rewind\",phase=\"counts\"} 1.212
kimmy_write_lock_held_phase_seconds_total{holder=\"rewind\",phase=\"commit\"} 1.213
# HELP kimmy_write_lock_held_io_bytes_total Bytes holds of the storage writer read from and wrote to the storage file, by holder. Beside the read and write components: more bytes is more pages, and the same bytes in more seconds is slower pages.
# TYPE kimmy_write_lock_held_io_bytes_total counter
kimmy_write_lock_held_io_bytes_total{holder=\"write\",io=\"read\"} 1001
kimmy_write_lock_held_io_bytes_total{holder=\"write\",io=\"write\"} 1002
kimmy_write_lock_held_io_bytes_total{holder=\"bulk\",io=\"read\"} 2001
kimmy_write_lock_held_io_bytes_total{holder=\"bulk\",io=\"write\"} 2002
kimmy_write_lock_held_io_bytes_total{holder=\"ddl\",io=\"read\"} 3001
kimmy_write_lock_held_io_bytes_total{holder=\"ddl\",io=\"write\"} 3002
kimmy_write_lock_held_io_bytes_total{holder=\"index_build\",io=\"read\"} 4001
kimmy_write_lock_held_io_bytes_total{holder=\"index_build\",io=\"write\"} 4002
kimmy_write_lock_held_io_bytes_total{holder=\"drop\",io=\"read\"} 5001
kimmy_write_lock_held_io_bytes_total{holder=\"drop\",io=\"write\"} 5002
kimmy_write_lock_held_io_bytes_total{holder=\"replication\",io=\"read\"} 6001
kimmy_write_lock_held_io_bytes_total{holder=\"replication\",io=\"write\"} 6002
kimmy_write_lock_held_io_bytes_total{holder=\"repair\",io=\"read\"} 7001
kimmy_write_lock_held_io_bytes_total{holder=\"repair\",io=\"write\"} 7002
kimmy_write_lock_held_io_bytes_total{holder=\"retention\",io=\"read\"} 8001
kimmy_write_lock_held_io_bytes_total{holder=\"retention\",io=\"write\"} 8002
kimmy_write_lock_held_io_bytes_total{holder=\"expiry\",io=\"read\"} 9001
kimmy_write_lock_held_io_bytes_total{holder=\"expiry\",io=\"write\"} 9002
kimmy_write_lock_held_io_bytes_total{holder=\"embedding\",io=\"read\"} 10001
kimmy_write_lock_held_io_bytes_total{holder=\"embedding\",io=\"write\"} 10002
kimmy_write_lock_held_io_bytes_total{holder=\"durability\",io=\"read\"} 11001
kimmy_write_lock_held_io_bytes_total{holder=\"durability\",io=\"write\"} 11002
kimmy_write_lock_held_io_bytes_total{holder=\"rewind\",io=\"read\"} 12001
kimmy_write_lock_held_io_bytes_total{holder=\"rewind\",io=\"write\"} 12002
# HELP kimmy_write_lock_held_write_estimated_seconds_total Seconds of page writes, inside holds of the storage writer, whose CPU time was estimated from a sample rather than read. The most by which cpu and off_cpu in kimmy_write_lock_held_component_seconds_total can be misattributed between each other, in either direction; 0 for a hold of 32 page writes or fewer, which is measured exactly.
# TYPE kimmy_write_lock_held_write_estimated_seconds_total counter
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"write\"} 0.121
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"bulk\"} 0.221
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"ddl\"} 0.321
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"index_build\"} 0.421
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"drop\"} 0.521
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"replication\"} 0.621
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"repair\"} 0.721
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"retention\"} 0.821
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"expiry\"} 0.921
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"embedding\"} 1.021
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"durability\"} 1.121
kimmy_write_lock_held_write_estimated_seconds_total{holder=\"rewind\"} 1.221
# HELP kimmy_write_lock_held_overcounted_total Holds of the storage writer whose measured components came to more than the hold, past the clocks' tolerance: something was counted twice. Should read 0; it cannot see a component that was missed, which lands in off_cpu instead.
# TYPE kimmy_write_lock_held_overcounted_total counter
kimmy_write_lock_held_overcounted_total{holder=\"write\"} 61
kimmy_write_lock_held_overcounted_total{holder=\"bulk\"} 62
kimmy_write_lock_held_overcounted_total{holder=\"ddl\"} 63
kimmy_write_lock_held_overcounted_total{holder=\"index_build\"} 64
kimmy_write_lock_held_overcounted_total{holder=\"drop\"} 65
kimmy_write_lock_held_overcounted_total{holder=\"replication\"} 66
kimmy_write_lock_held_overcounted_total{holder=\"repair\"} 67
kimmy_write_lock_held_overcounted_total{holder=\"retention\"} 68
kimmy_write_lock_held_overcounted_total{holder=\"expiry\"} 69
kimmy_write_lock_held_overcounted_total{holder=\"embedding\"} 70
kimmy_write_lock_held_overcounted_total{holder=\"durability\"} 71
kimmy_write_lock_held_overcounted_total{holder=\"rewind\"} 72
# HELP kimmy_write_lock_held_cpu_unmeasured_total Holds of the storage writer, of any holder, whose thread CPU time could not be read, so they are not in the cpu and off_cpu components. Rises on every hold on a platform without a per-thread CPU clock; 0 on Linux and macOS.
# TYPE kimmy_write_lock_held_cpu_unmeasured_total counter
kimmy_write_lock_held_cpu_unmeasured_total 99
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
# HELP kimmy_task_retries_total Times a supervised background task retried its work in place after a transient failure, by task. A task whose count rises while nothing else changes is retrying for ever: alive, and doing no work. For the embedding worker, read it beside kimmy_task_progress_age_seconds, which rises through a retry that never succeeds.
# TYPE kimmy_task_retries_total counter
kimmy_task_retries_total{task=\"cert_reloader\"} 0
kimmy_task_retries_total{task=\"drop_purger\"} 0
kimmy_task_retries_total{task=\"embedding_worker\"} 0
kimmy_task_retries_total{task=\"jwks_refresher\"} 0
kimmy_task_retries_total{task=\"membership\"} 0
kimmy_task_retries_total{task=\"membership_announce\"} 0
kimmy_task_retries_total{task=\"membership_inbound\"} 0
kimmy_task_retries_total{task=\"membership_timer\"} 0
kimmy_task_retries_total{task=\"replication\"} 0
kimmy_task_retries_total{task=\"replication_server\"} 0
kimmy_task_retries_total{task=\"retention_collector\"} 0
kimmy_task_retries_total{task=\"session_invalidator\"} 0
kimmy_task_retries_total{task=\"stall_probe\"} 0
kimmy_task_retries_total{task=\"ttl_expiry\"} 0
kimmy_task_retries_total{task=\"vector_index_invalidator\"} 0
kimmy_task_retries_total{task=\"webhook_dispatcher\"} 0
# HELP kimmy_task_progress_age_seconds Seconds since a background writer last completed its work, computed when this page is read: a completed replication round, a stall-probe wake, a dispatcher pass, an embedding flush or idle turn. Since the process started before the first, never 0. Alert on this, and read the gauges a writer sets only while its age is fresh: a dead, stuck or retrying writer leaves them at their last value. A writer this node does not run has no row.
# TYPE kimmy_task_progress_age_seconds gauge
kimmy_task_progress_age_seconds{task=\"drop_purger\"} 99
kimmy_task_progress_age_seconds{task=\"embedding_worker\"} 99
kimmy_task_progress_age_seconds{task=\"replication\"} 81
kimmy_task_progress_age_seconds{task=\"stall_probe\"} 83
kimmy_task_progress_age_seconds{task=\"webhook_dispatcher\"} 85
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
# HELP kimmy_ttl_skipped_filter_total Expiry candidates a TTL index held that its partial filter, evaluated as find evaluates it, did not select when the delete re-read the document, and were not deleted. A document moved out of the filter while the pass ran, or one the index should never have held. Should fall to near zero once partial-index membership agrees with the filter; until then each one is a document expiry used to delete.
# TYPE kimmy_ttl_skipped_filter_total counter
kimmy_ttl_skipped_filter_total 93
# HELP kimmy_index_unkeyed_total Documents stored under an index that could not key them - arrays at two of a compound index's paths, more than 1000 keys, or a Decimal128 - and are rechecked on every scan of that index instead. Each one is logged at warning naming the index and the document; the index listing reports how many stand under each index as `unkeyed`. This counts that reason only; a document held because a partial filter could not decide it is kimmy_index_undecidable_total.
# TYPE kimmy_index_unkeyed_total counter
kimmy_index_unkeyed_total 26
# HELP kimmy_index_undecidable_total Documents an index holds because its partial filter could not decide them: a Decimal128 at a filtered path, which the canonical order ranks equal to every number, so the filter's answer is not an answer. The index holds them and every scan re-checks them, which is what stops a partial index missing documents find returns. Expected, not a fault - the separate kimmy_index_unkeyed_total counts documents an index could not key, which is one.
# TYPE kimmy_index_undecidable_total counter
kimmy_index_undecidable_total 37
# HELP kimmy_webhook_deliveries_total Webhook delivery attempts by outcome.
# TYPE kimmy_webhook_deliveries_total counter
kimmy_webhook_deliveries_total{outcome=\"delivered\"} 2
kimmy_webhook_deliveries_total{outcome=\"failed\"} 1
# HELP kimmy_webhook_events_total Change events pushed to endpoints.
# TYPE kimmy_webhook_events_total counter
kimmy_webhook_events_total 27
# HELP kimmy_webhook_subscriptions Registered subscriptions by state, counted from this node's registry when this page is read.
# TYPE kimmy_webhook_subscriptions gauge
kimmy_webhook_subscriptions{state=\"active\"} 15
kimmy_webhook_subscriptions{state=\"invalidated\"} 16
kimmy_webhook_subscriptions{state=\"unreadable\"} 38
# HELP kimmy_webhook_backlog_seconds Age of the oldest undelivered event, across subscriptions this node owns, as of the dispatcher's last pass; kimmy_task_progress_age_seconds says how old that is.
# TYPE kimmy_webhook_backlog_seconds gauge
kimmy_webhook_backlog_seconds 17
# HELP kimmy_cluster_members Peers this node's SWIM membership currently considers alive, counted when this page is read. 0 with clustering off.
# TYPE kimmy_cluster_members gauge
kimmy_cluster_members 18
# HELP kimmy_replication_lag_seconds Seconds since the newest peer entry applied locally where a peer holds newer, max over peers in the last sync round, to the millisecond. Measured after each round against the vector the peer advertised when its pull opened, so it reads 0 once a round's pull reached that vector - including while entries the peer wrote since wait up to cluster.sync_interval_secs for the next round. Non-zero means a round ended with a pull still truncated: a backlog deeper than a tick could drain. 0 when clustering is off.
# TYPE kimmy_replication_lag_seconds gauge
kimmy_replication_lag_seconds 19.25
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
# HELP kimmy_sync_ddl_applied_total Replicated schema changes this node applied, by how they arrived: pull, a window this node pulled from a peer; push, a window a peer pushed to confirm a change it made (ADR-140). Counted per entry applied, not per entry received: a refused, declined or skipped entry is not counted here, and neither is a replayed drop this node had already recorded or a change for a collection dropped here, which no outcome series counts. Nor is a change whose entry this node already held as sent, which is not applied again and whose append commits nothing (a kind's own writes are unchanged); it is counted in kimmy_sync_ddl_held_total. A burst of N index changes on one member should read about N on each other member, summed over both labels; compare the increase over a burst, not the total.
# TYPE kimmy_sync_ddl_applied_total counter
kimmy_sync_ddl_applied_total{via=\"pull\"} 89
kimmy_sync_ddl_applied_total{via=\"push\"} 97
# HELP kimmy_sync_ddl_held_total Replicated schema changes a window carried that this node already held, entry and all, by how they arrived: pull or push, as for kimmy_sync_ddl_applied_total. Not applied again: the append of the entry commits nothing, though a kind's own writes are unchanged (a drop of an index already gone still records its tombstone). Windows overlap by design - a pull that read this node's position before a push landed, a third member relaying what the origin pushed - and this is how much; for an index create it costs a read, not a commit.
# TYPE kimmy_sync_ddl_held_total counter
kimmy_sync_ddl_held_total{via=\"pull\"} 48
kimmy_sync_ddl_held_total{via=\"push\"} 58
# HELP kimmy_ddl_confirmations_total Schema-change confirmations on a member, one per member per index create or drop this node made (ADR-140), by how each ended (ADR-191). confirmed: the member took the change and did not refuse it. refused: it could not apply it, or declined a drop older than the index it holds. The rest are pending, and anti-entropy carries the change: timeout, the request's deadline passed first; failed, the push errored or timed out; unreached, the member is more than a batch behind or below the retention horizon; purging, it is still purging a drop of the name; stopped_unknown, its batch stopped earlier at a collection it lacks; other_member, a different node answered at the address; task_ended, the push task panicked or was aborted; backoff, the member did not answer the last push and is not pushed to for a while; unattributable, the member runs a version whose answer does not name changes; cancelled, the request went away before an answer, with its client.
# TYPE kimmy_ddl_confirmations_total counter
kimmy_ddl_confirmations_total{outcome=\"confirmed\"} 120
kimmy_ddl_confirmations_total{outcome=\"refused\"} 121
kimmy_ddl_confirmations_total{outcome=\"timeout\"} 122
kimmy_ddl_confirmations_total{outcome=\"failed\"} 123
kimmy_ddl_confirmations_total{outcome=\"unreached\"} 124
kimmy_ddl_confirmations_total{outcome=\"purging\"} 125
kimmy_ddl_confirmations_total{outcome=\"stopped_unknown\"} 126
kimmy_ddl_confirmations_total{outcome=\"other_member\"} 127
kimmy_ddl_confirmations_total{outcome=\"task_ended\"} 128
kimmy_ddl_confirmations_total{outcome=\"backoff\"} 129
kimmy_ddl_confirmations_total{outcome=\"unattributable\"} 130
kimmy_ddl_confirmations_total{outcome=\"cancelled\"} 131
# HELP kimmy_ddl_confirm_pushes_total Windows pushed to members to confirm schema changes (ADR-191). At most one is in flight per member, and each carries everything queued for it, so in a burst this rises far slower than kimmy_ddl_confirmations_total.
# TYPE kimmy_ddl_confirm_pushes_total counter
kimmy_ddl_confirm_pushes_total 137
# HELP kimmy_sync_ddl_relogged_total Schema changes a snapshot restore appended to this node's oplog so that it can serve them onward. Not an error: 0 on a member that never caught up by snapshot, and one per index definition a snapshot restored where it did not already hold the entry.
# TYPE kimmy_sync_ddl_relogged_total counter
kimmy_sync_ddl_relogged_total 91
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
# HELP kimmy_sync_divergence_check_age_seconds Seconds since the last contact, with any peer, in which the cross-member divergence check ran, computed when this page is read. Before the first such contact, seconds since the process started, never 0; 0 on a node without clustering, so alert on it only where kimmy_task_progress_age_seconds has a replication row. Above a few multiples of cluster.sync_interval_secs, kimmy_sync_divergent_collections is holding a value nothing has re-examined, whether the rounds are failing or the loop itself is stuck - look at kimmy_sync_failures_total, kimmy_sync_peers_backing_off and kimmy_write_lock_wait_seconds.
# TYPE kimmy_sync_divergence_check_age_seconds gauge
kimmy_sync_divergence_check_age_seconds 71
# HELP kimmy_sync_entries_skipped_total Replicated entries a sync round left rather than took. unknown_collection: batches stopped at an entry for a collection this node has no record of - neither holding it nor a tombstone for it - because its creation was witnessed here without being applied, or has aged out of the peer's oplog; one per stopped batch, the window is re-served from the same place every round, and the round plans a snapshot from the peer to bring the collection. A collection dropped here is history instead and stops nothing. beyond_advertised: entries above the vector the peer advertised before serving the window, left for the next round, which asks for them from the right position; ordinary and rare on a busy cluster. A hole of either kind reads 0 on kimmy_replication_lag_seconds; this and kimmy_sync_divergent_collections are what move. purge_pending: batches stopped at a replicated creation, and snapshot pages that would create a collection, of a name whose earlier collection this node's drop purger is still removing (ADR-189); nothing is missing, so no snapshot is planned and a repair waiting on one is not abandoned, the window or page is asked for again from the same place until the purge is done, and nothing stamped after the creation is taken meanwhile, so kimmy_sync_divergence_check_age_seconds rises by design.
# TYPE kimmy_sync_entries_skipped_total counter
kimmy_sync_entries_skipped_total{reason=\"unknown_collection\"} 73
kimmy_sync_entries_skipped_total{reason=\"beyond_advertised\"} 75
kimmy_sync_entries_skipped_total{reason=\"purge_pending\"} 78
# HELP kimmy_sync_held_marks_released_total Entries this node held as state - written by a snapshot page, a carried delete or a scoped repair, above the vector it advertises - that arrived in a sync window served contiguously from its position and were released: the mark removed and both vectors raised over the entry. One per entry, counted when the batch commits. The release path itself: the beyond_advertised reason of kimmy_sync_entries_skipped_total rises on a peer while these entries are held and also for the ordinary race, and only this tells the two apart.
# TYPE kimmy_sync_held_marks_released_total counter
kimmy_sync_held_marks_released_total 53
# HELP kimmy_sync_held_marks Entries this node holds as state rather than history - written by a snapshot page, a carried delete or a scoped repair above the vector it advertises - and still waiting to arrive in a sync window contiguous from its position, which releases them. A gauge, read at scrape. Non-zero is not a fault: a member caught up by snapshot holds entries until they arrive as history. Non-zero and not falling over many rounds is a member whose peers are not serving those entries, and each pull names them to its peers as spans until they are.
# TYPE kimmy_sync_held_marks gauge
kimmy_sync_held_marks 54
# HELP kimmy_sync_repair_rounds_total Sync rounds spent repairing against a peer: re-serving its oplog from the divergent collection's creation, or pulling its snapshot, after the divergence check confirmed a collection against it or a batch stopped at a collection this node lacks. Rising is a repair under way; it stops when the repair reaches the peer's tail.
# TYPE kimmy_sync_repair_rounds_total counter
kimmy_sync_repair_rounds_total 77
# HELP kimmy_sync_pulled_entries_total Entries sync pulls carried from peers, whatever became of each - applied, superseded, a schema change or left for a later window. Divide the growth of kimmy_sync_pull_seconds_sum{phase=\"apply\"} by the growth of this for what applying one entry costs, whatever size the batches were.
# TYPE kimmy_sync_pulled_entries_total counter
kimmy_sync_pulled_entries_total 1105
# HELP kimmy_sync_entry_wait_ahead_total Sync pulls whose oldest entry this node lacked carried a timestamp later than this node's clock read when the batch arrived, so its wait could not be taken and is not in kimmy_sync_entry_wait_seconds. A peer's clock, or one its stamps witnessed, runs ahead of this node's; rising steadily is clock skew between members, and the wait histogram is under-reading by the pulls counted here.
# TYPE kimmy_sync_entry_wait_ahead_total counter
kimmy_sync_entry_wait_ahead_total 117
# HELP kimmy_sync_contacts_total Contacts with a peer in a sync tick, by how they ended. caught_up: the last pull did not come back truncated, so nothing more could be pulled at once. budget: a pull came back truncated and the next would not have fitted in what was left of cluster.sync_interval_secs, so a backlog was carried into the next tick. ceiling: truncated with time left, after the most pulls one contact may make. failed: a pull failed. budget rising is a backlog outliving a tick; kimmy_sync_pull_seconds says whether the tick's time went to the peer serving, to waiting for this node's writer, or to applying.
# TYPE kimmy_sync_contacts_total counter
kimmy_sync_contacts_total{ended=\"caught_up\"} 111
kimmy_sync_contacts_total{ended=\"budget\"} 112
kimmy_sync_contacts_total{ended=\"ceiling\"} 113
kimmy_sync_contacts_total{ended=\"failed\"} 114
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
# HELP kimmy_embed_skipped_no_shadow_total Documents and scans skipped because a collection is configured for vectors and its shadow collection is not on this node. Should read 0; rising means a configuration without the collection its vectors are stored in.
# TYPE kimmy_embed_skipped_no_shadow_total counter
kimmy_embed_skipped_no_shadow_total 0
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
# HELP kimmy_backup_duration_seconds How long a backup took to produce: the walk of the whole store and its spill to disk, before any of it was sent. Follows the store's size and whether the file is in page cache.
# TYPE kimmy_backup_duration_seconds histogram
kimmy_backup_duration_seconds_bucket{le=\"1\"} 0
kimmy_backup_duration_seconds_bucket{le=\"5\"} 0
kimmy_backup_duration_seconds_bucket{le=\"15\"} 0
kimmy_backup_duration_seconds_bucket{le=\"30\"} 0
kimmy_backup_duration_seconds_bucket{le=\"60\"} 1
kimmy_backup_duration_seconds_bucket{le=\"120\"} 1
kimmy_backup_duration_seconds_bucket{le=\"300\"} 1
kimmy_backup_duration_seconds_bucket{le=\"600\"} 1
kimmy_backup_duration_seconds_bucket{le=\"1800\"} 1
kimmy_backup_duration_seconds_bucket{le=\"3600\"} 1
kimmy_backup_duration_seconds_bucket{le=\"+Inf\"} 1
kimmy_backup_duration_seconds_sum 42
kimmy_backup_duration_seconds_count 1
# HELP kimmy_sync_pull_seconds Where a sync pull's time went, by phase, one observation per pull of a peer's oplog. serve: from asking the peer for a window to holding it - the peer walking its oplog and the wire. wait: applying the window, waiting for this node's single writer behind whatever else was writing. apply: applying the window less that wait - every entry's work, the commits and their fsync. The three add up to the pull, and each is a different fix. Under storage.durability = coalesced, the wait for the shared flush's window and for the flush another committer leads is in apply, so there apply includes queueing behind other committers. A snapshot page is not a pull and is not observed here.
# TYPE kimmy_sync_pull_seconds histogram
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"0.001\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"0.005\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"0.01\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"0.025\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"0.05\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"0.1\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"0.25\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"0.5\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"1\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"2.5\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"5\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"10\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"30\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"60\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"300\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"900\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"+Inf\"} 2
kimmy_sync_pull_seconds_sum{phase=\"serve\"} 0.011
kimmy_sync_pull_seconds_count{phase=\"serve\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"0.001\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"0.005\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"0.01\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"0.025\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"0.05\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"0.1\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"0.25\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"0.5\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"1\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"2.5\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"5\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"10\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"30\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"60\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"300\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"900\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"+Inf\"} 2
kimmy_sync_pull_seconds_sum{phase=\"wait\"} 45.04
kimmy_sync_pull_seconds_count{phase=\"wait\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"0.001\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"0.005\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"0.01\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"0.025\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"0.05\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"0.1\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"0.25\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"0.5\"} 0
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"1\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"2.5\"} 1
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"5\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"10\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"30\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"60\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"300\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"900\"} 2
kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"+Inf\"} 2
kimmy_sync_pull_seconds_sum{phase=\"apply\"} 3.7
kimmy_sync_pull_seconds_count{phase=\"apply\"} 2
# HELP kimmy_sync_entry_wait_seconds How long the oldest entry a sync pull carried that this node lacked had waited when the pull arrived: from its origin's timestamp to this node's clock. The time an entry spends before any pull takes it - waiting for the next tick, or behind a backlog - which kimmy_sync_pull_seconds does not see. Crosses member clocks, so skew shifts it; a wait that would be negative is counted in kimmy_sync_entry_wait_ahead_total instead.
# TYPE kimmy_sync_entry_wait_seconds histogram
kimmy_sync_entry_wait_seconds_bucket{le=\"0.1\"} 0
kimmy_sync_entry_wait_seconds_bucket{le=\"0.25\"} 0
kimmy_sync_entry_wait_seconds_bucket{le=\"0.5\"} 0
kimmy_sync_entry_wait_seconds_bucket{le=\"1\"} 0
kimmy_sync_entry_wait_seconds_bucket{le=\"2\"} 1
kimmy_sync_entry_wait_seconds_bucket{le=\"5\"} 1
kimmy_sync_entry_wait_seconds_bucket{le=\"10\"} 1
kimmy_sync_entry_wait_seconds_bucket{le=\"30\"} 1
kimmy_sync_entry_wait_seconds_bucket{le=\"60\"} 2
kimmy_sync_entry_wait_seconds_bucket{le=\"300\"} 2
kimmy_sync_entry_wait_seconds_bucket{le=\"3600\"} 2
kimmy_sync_entry_wait_seconds_bucket{le=\"+Inf\"} 2
kimmy_sync_entry_wait_seconds_sum 46.5
kimmy_sync_entry_wait_seconds_count 2
# HELP kimmy_sync_served_windows_total Windows of this node's oplog walked for peers that pulled from it, including one too large for a frame that the peer is asked to take in fewer entries.
# TYPE kimmy_sync_served_windows_total counter
kimmy_sync_served_windows_total 1201
# HELP kimmy_sync_served_entries_total Entries the windows this node served to peers carried.
# TYPE kimmy_sync_served_entries_total counter
kimmy_sync_served_entries_total 1202
# HELP kimmy_sync_serve_passed_entries_total Entries the walks behind served windows examined and did not serve, mostly because the pulling peer already held them. A walk costs what it examines: this plus kimmy_sync_served_entries_total.
# TYPE kimmy_sync_serve_passed_entries_total counter
kimmy_sync_serve_passed_entries_total 1203
# HELP kimmy_sync_serve_walk_read_seconds_total Seconds the walks behind served windows spent reading pages of the storage file its cache did not hold: the serving load on this node's disk.
# TYPE kimmy_sync_serve_walk_read_seconds_total counter
kimmy_sync_serve_walk_read_seconds_total 3.3
# HELP kimmy_sync_serve_walk_read_bytes_total Bytes the walks behind served windows read from the storage file.
# TYPE kimmy_sync_serve_walk_read_bytes_total counter
kimmy_sync_serve_walk_read_bytes_total 1204
# HELP kimmy_sync_serve_walk_seconds How long this node took to walk its oplog for one window served to a peer, the wire not included.
# TYPE kimmy_sync_serve_walk_seconds histogram
kimmy_sync_serve_walk_seconds_bucket{le=\"0.0001\"} 3
kimmy_sync_serve_walk_seconds_bucket{le=\"0.001\"} 3
kimmy_sync_serve_walk_seconds_bucket{le=\"0.005\"} 1193
kimmy_sync_serve_walk_seconds_bucket{le=\"0.01\"} 1193
kimmy_sync_serve_walk_seconds_bucket{le=\"0.025\"} 1193
kimmy_sync_serve_walk_seconds_bucket{le=\"0.05\"} 1193
kimmy_sync_serve_walk_seconds_bucket{le=\"0.1\"} 1193
kimmy_sync_serve_walk_seconds_bucket{le=\"0.25\"} 1193
kimmy_sync_serve_walk_seconds_bucket{le=\"0.5\"} 1193
kimmy_sync_serve_walk_seconds_bucket{le=\"1\"} 1193
kimmy_sync_serve_walk_seconds_bucket{le=\"5\"} 1193
kimmy_sync_serve_walk_seconds_bucket{le=\"30\"} 1200
kimmy_sync_serve_walk_seconds_bucket{le=\"+Inf\"} 1201
kimmy_sync_serve_walk_seconds_sum 2.5
kimmy_sync_serve_walk_seconds_count 1201
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
        expect(&format!("kimmy_ttl_skipped_filter_total {}\n", s.ttl_skipped_filter));
        expect(&format!("kimmy_index_unkeyed_total {}\n", s.index_unkeyed));
        expect(&format!("kimmy_index_undecidable_total {}\n", s.index_undecidable));
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
        expect(&format!("kimmy_replication_lag_seconds {}\n", s.replication_lag_ms as f64 / 1e3));
        expect(&format!("kimmy_sync_failures_total {}\n", s.sync_failures));
        expect(&format!("kimmy_sync_peers_backing_off {}\n", s.sync_peers_backing_off));
        expect(&format!("kimmy_sync_ddl_refused_total {}\n", s.sync_ddl_refused));
        expect(&format!("kimmy_sync_ddl_declined_total {}\n", s.sync_ddl_declined));
        expect(&format!(
            "kimmy_sync_ddl_applied_total{{via=\"pull\"}} {}\n",
            s.sync_ddl_applied_pull
        ));
        expect(&format!(
            "kimmy_sync_ddl_applied_total{{via=\"push\"}} {}\n",
            s.sync_ddl_applied_push
        ));
        expect(&format!("kimmy_sync_ddl_held_total{{via=\"pull\"}} {}\n", s.sync_ddl_held_pull));
        expect(&format!("kimmy_sync_ddl_held_total{{via=\"push\"}} {}\n", s.sync_ddl_held_push));
        for outcome in kimmy_cluster::ConfirmOutcome::ALL {
            expect(&format!(
                "kimmy_ddl_confirmations_total{{outcome=\"{}\"}} {}\n",
                outcome.label(),
                s.ddl_confirmations[outcome.slot()]
            ));
        }
        expect(&format!("kimmy_ddl_confirm_pushes_total {}\n", s.ddl_confirm_pushes));
        expect(&format!("kimmy_sync_ddl_relogged_total {}\n", s.sync_ddl_relogged));
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
        expect(&format!(
            "kimmy_sync_entries_skipped_total{{reason=\"purge_pending\"}} {}\n",
            s.sync_entries_skipped_purge_pending
        ));
        expect(&format!("kimmy_sync_held_marks_released_total {}\n", s.sync_held_marks_released));
        expect(&format!("kimmy_sync_held_marks {}\n", s.sync_held_marks));
        expect(&format!("kimmy_sync_repair_rounds_total {}\n", s.sync_repair_rounds));
        expect(&format!("kimmy_sync_pulled_entries_total {}\n", s.sync_pulls.entries));
        expect(&format!("kimmy_sync_entry_wait_ahead_total {}\n", s.sync_pulls.entry_wait_ahead));
        for end in kimmy_cluster::ContactEnd::ALL {
            expect(&format!(
                "kimmy_sync_contacts_total{{ended=\"{}\"}} {}\n",
                end.label(),
                s.sync_pulls.contacts[end.slot()]
            ));
        }
        for (phase, h) in [
            ("serve", s.sync_pulls.serve),
            ("wait", s.sync_pulls.wait),
            ("apply", s.sync_pulls.apply),
        ] {
            expect(&format!(
                "kimmy_sync_pull_seconds_sum{{phase=\"{phase}\"}} {}\n",
                h.sum_us as f64 / 1e6
            ));
            expect(&format!("kimmy_sync_pull_seconds_count{{phase=\"{phase}\"}} {}\n", h.count));
        }
        expect(&format!(
            "kimmy_sync_entry_wait_seconds_sum {}\n",
            s.sync_pulls.entry_wait.sum_us as f64 / 1e6
        ));
        expect(&format!("kimmy_sync_entry_wait_seconds_count {}\n", s.sync_pulls.entry_wait.count));
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
        expect(&format!("kimmy_embed_skipped_no_shadow_total {}\n", s.embed_skipped_no_shadow));
        expect(&format!("kimmy_embed_failures_total {}\n", s.embed_failures));
        expect(&format!("kimmy_embed_provider_requests_total {}\n", s.embed_provider_requests));
        expect(&format!("kimmy_embed_provider_tokens_total {}\n", s.embed_provider_tokens));
        for (kind, n) in ["connect", "timeout", "reset", "other"].iter().zip(s.embed_transport) {
            expect(&format!("kimmy_embed_provider_errors_total{{kind=\"{kind}\"}} {n}\n"));
        }
        expect(&format!("kimmy_request_duration_seconds_count {}\n", s.latency_count));
        expect(&format!(
            "kimmy_backup_duration_seconds_sum {}\n",
            s.backup_duration_sum_us as f64 / 1e6
        ));

        for holder in kimmy_storage::WriterHolder::ALL {
            let (row, label) = (holder.slot(), holder.label());
            let d = &s.write_lock_hold;
            for c in kimmy_storage::HoldComponent::ALL {
                expect(&format!(
                    "kimmy_write_lock_held_component_seconds_total{{holder=\"{label}\",component=\"{}\"}} {}\n",
                    c.label(),
                    d.component_ns[row][c.slot()] as f64 / 1e9
                ));
            }
            for p in kimmy_storage::HoldPhase::ALL {
                expect(&format!(
                    "kimmy_write_lock_held_phase_seconds_total{{holder=\"{label}\",phase=\"{}\"}} {}\n",
                    p.label(),
                    d.phase_ns[row][p.slot()] as f64 / 1e9
                ));
            }
            expect(&format!(
                "kimmy_write_lock_held_io_bytes_total{{holder=\"{label}\",io=\"read\"}} {}\n",
                d.read_bytes[row]
            ));
            expect(&format!(
                "kimmy_write_lock_held_io_bytes_total{{holder=\"{label}\",io=\"write\"}} {}\n",
                d.write_bytes[row]
            ));
            expect(&format!(
                "kimmy_write_lock_held_write_estimated_seconds_total{{holder=\"{label}\"}} {}\n",
                d.write_estimated_ns[row] as f64 / 1e9
            ));
            expect(&format!(
                "kimmy_write_lock_held_overcounted_total{{holder=\"{label}\"}} {}\n",
                d.overcounted[row]
            ));
        }
        expect(&format!(
            "kimmy_write_lock_held_cpu_unmeasured_total {}\n",
            s.write_lock_hold.cpu_unmeasured
        ));
        expect(&format!("kimmy_sync_served_windows_total {}\n", s.sync_serve.windows));
        expect(&format!("kimmy_sync_served_entries_total {}\n", s.sync_serve.entries));
        expect(&format!("kimmy_sync_serve_passed_entries_total {}\n", s.sync_serve.passed));
        expect(&format!(
            "kimmy_sync_serve_walk_read_seconds_total {}\n",
            s.sync_serve.read_ns as f64 / 1e9
        ));
        expect(&format!("kimmy_sync_serve_walk_read_bytes_total {}\n", s.sync_serve.read_bytes));
        expect(&format!(
            "kimmy_sync_serve_walk_seconds_sum {}\n",
            s.sync_serve.walk_sum_us as f64 / 1e6
        ));
        expect(&format!("kimmy_sync_serve_walk_seconds_count {}\n", s.sync_serve.windows));

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
    fn the_lag_gauge_reads_to_the_millisecond() {
        // ADR-175: the loop divided milliseconds by a thousand into an integer
        // before this ever saw them, so every reading was a whole second,
        // truncated, and an effect of a few seconds could not be read.
        let m = Metrics::default();
        m.set_replication_lag_ms(6_384);
        let out = m.render();
        assert!(out.contains("kimmy_replication_lag_seconds 6.384\n"), "{out}");
        assert_eq!(m.snapshot().replication_lag_ms, 6_384, "the bridge reads the same value");
    }

    #[test]
    fn each_ticks_pulls_land_in_their_own_phase_label_and_bucket() {
        // Three phases and four contact ends share one shape, so a phase or
        // a label rendered from its neighbour's row compiles and reads
        // plausibly. Every value here differs from every other, and two
        // ticks must add rather than replace.
        let m = Metrics::default();
        let tick = |serve, wait, apply, contacts| kimmy_cluster::RoundReport {
            pulls: pulls_observed([serve, wait, apply], 10, Some(700), 0, contacts),
            ..Default::default()
        };
        m.record_sync_round(&tick(2, 30, 400, [1, 2, 3, 4]));
        m.record_sync_round(&tick(2, 30, 400, [10, 20, 30, 40]));

        let out = m.render();
        for line in [
            "kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"0.005\"} 2\n",
            "kimmy_sync_pull_seconds_bucket{phase=\"serve\",le=\"0.001\"} 0\n",
            "kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"0.025\"} 0\n",
            "kimmy_sync_pull_seconds_bucket{phase=\"wait\",le=\"0.05\"} 2\n",
            "kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"0.25\"} 0\n",
            "kimmy_sync_pull_seconds_bucket{phase=\"apply\",le=\"0.5\"} 2\n",
            "kimmy_sync_pull_seconds_sum{phase=\"serve\"} 0.004\n",
            "kimmy_sync_pull_seconds_sum{phase=\"wait\"} 0.06\n",
            "kimmy_sync_pull_seconds_sum{phase=\"apply\"} 0.8\n",
            "kimmy_sync_pull_seconds_count{phase=\"apply\"} 2\n",
            "kimmy_sync_pulled_entries_total 20\n",
            "kimmy_sync_entry_wait_seconds_bucket{le=\"0.5\"} 0\n",
            "kimmy_sync_entry_wait_seconds_bucket{le=\"1\"} 2\n",
            "kimmy_sync_entry_wait_seconds_sum 1.4\n",
            "kimmy_sync_contacts_total{ended=\"caught_up\"} 11\n",
            "kimmy_sync_contacts_total{ended=\"budget\"} 22\n",
            "kimmy_sync_contacts_total{ended=\"ceiling\"} 33\n",
            "kimmy_sync_contacts_total{ended=\"failed\"} 44\n",
        ] {
            assert!(out.contains(line), "missing {line:?} in:\n{out}");
        }
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
        // 57 scalar sample lines plus three histograms: the latency one's 12
        // buckets, +Inf, sum and count; the writer wait's 8 buckets, +Inf,
        // sum and count (ADR-151); and the writer hold's 7 buckets, +Inf,
        // sum and count for each of the twelve holders (ADR-159); and the
        // backup duration's 10 buckets, +Inf, sum and count (ADR-170). Since
        // ADR-175, six more scalars (pulled entries, clock-ahead pulls, four
        // contact ends), the pull histogram's 16 buckets, +Inf, sum and count
        // for each of three phases, and the entry wait's 11 buckets, +Inf,
        // sum and count. Since ADR-176, for each of the twelve holders five
        // components, three phases, two byte counts, the estimate's bound and
        // the over-count; one scalar for unmeasured CPU; five serve scalars;
        // and the serve walk's 12 buckets, +Inf, sum and count. Since
        // ADR-178, one scalar for embedding skipped for want of a shadow;
        // since ADR-180, one for schema changes a snapshot restore re-logged;
        // since ADR-181, one for expiry declined by the partial filter;
        // since ADR-184, one retry counter per supervised task; and since
        // ADR-185, one for documents an index holds because its partial
        // filter could not decide them.
        assert_eq!(
            samples,
            107 + 6
                + 3 * 19
                + 14
                + 10 * kimmy_storage::WriterHolder::COUNT
                + 12 * kimmy_storage::WriterHolder::COUNT
                + 1
                + 5
                + 15
                // ADR-185: one for documents an index holds because its
                // partial filter could not decide them.
                + 1
                // Since ADR-184, one per supervised task: the label set is
                // `kimmy_task::TASKS`, so this counts the tasks rather than a
                // number written twice.
                + kimmy_task::TASKS.len()
                // Since ADR-187, one progress age per background writer, with
                // no startup behind this render to leave any out.
                + PROGRESS_WRITERS.len()
                // And a third subscription state, for records that do not decode.
                + 1
                // ADR-189: batches stopped at a creation waiting for the drop
                // purger, a third reason for a skipped entry.
                + 1
                // Replicated schema changes applied, pulled and pushed.
                + 2
                // And those already held, pulled and pushed.
                + 2
                // Schema-change confirmations by outcome, and the pushes
                // made for them (ADR-191).
                + kimmy_cluster::ConfirmOutcome::COUNT
                + 1,
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
    fn a_pushed_window_counts_what_it_applied_as_pushed_and_the_rest_where_a_pull_would() {
        // Applied, not received: the refusal, the decline and the skips are
        // on their own series and not under `via="push"`, and nothing lands
        // under `via="pull"`.
        let m = Metrics::default();
        m.record_pushed(&kimmy_storage::SyncOutcome {
            ddl: 2,
            ddl_held: 3,
            ddl_refused: 1,
            ddl_declined: 1,
            unknown_collection: 1,
            deferred: 1,
            purge_pending: 1,
            ..Default::default()
        });
        let s = m.snapshot();
        assert_eq!(s.sync_ddl_applied_push, 2);
        assert_eq!(s.sync_ddl_applied_pull, 0);
        assert_eq!((s.sync_ddl_held_push, s.sync_ddl_held_pull), (3, 0));
        assert_eq!((s.sync_ddl_refused, s.sync_ddl_declined), (1, 1));
        assert_eq!(
            (
                s.sync_entries_skipped_unknown_collection,
                s.sync_entries_skipped_beyond_advertised,
                s.sync_entries_skipped_purge_pending
            ),
            (1, 1, 1)
        );
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
        m.set_replication_lag_ms(7_000);
        m.record_sync_round(&kimmy_cluster::RoundReport {
            failed: 1,
            backing_off: 1,
            ddl_applied: 0,
            ddl_held: 0,
            ddl_refused: 0,
            ddl_declined: 0,
            divergent_collections: 0,
            divergence_checks: 4,
            divergence_skips: 0,
            divergence_count_compared: 2,
            divergence_count_deferred: 1,
            divergence_last_check: Some(now - Duration::from_secs(30)),
            last_completed_round: None,
            entries_skipped_unknown_collection: 0,
            entries_skipped_beyond_advertised: 0,
            entries_skipped_purge_pending: 0,
            repair_rounds: 0,
            pulls: kimmy_cluster::PullReport::default(),
        });
        m.record_sync_round(&kimmy_cluster::RoundReport {
            failed: 2,
            backing_off: 0,
            ddl_applied: 7,
            ddl_held: 0,
            ddl_refused: 3,
            ddl_declined: 0,
            divergent_collections: 6,
            divergence_checks: 0,
            divergence_skips: 5,
            divergence_count_compared: 0,
            divergence_count_deferred: 4,
            divergence_last_check: Some(now - Duration::from_secs(8)),
            last_completed_round: None,
            entries_skipped_unknown_collection: 0,
            entries_skipped_beyond_advertised: 0,
            entries_skipped_purge_pending: 0,
            repair_rounds: 0,
            pulls: kimmy_cluster::PullReport::default(),
        });
        m.record_ddl_applied_push(11);
        m.record_tls_reload(true);
        m.record_tls_reload(false);
        m.record_tls_reload(false);
        m.record_jwks_refresh(true);
        m.record_jwks_refresh(true);
        m.record_jwks_refresh(false);

        let out = m.render_at(now);
        assert!(out.contains("kimmy_replication_lag_seconds 7"), "{out}");
        // The failure signals are pushed per tick: counters accumulate across
        // ticks, the backoff level is the latest tick's (ADR-123).
        assert!(out.contains("kimmy_sync_failures_total 3"), "{out}");
        assert!(out.contains("kimmy_sync_peers_backing_off 0"), "{out}");
        assert!(out.contains("kimmy_sync_ddl_refused_total 3"), "{out}");
        assert!(out.contains("kimmy_sync_ddl_applied_total{via=\"pull\"} 7"), "{out}");
        assert!(out.contains("kimmy_sync_ddl_applied_total{via=\"push\"} 11"), "{out}");
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

    /// **An age before the first completion is the time since the process
    /// started, never 0** (ADR-187). This is the load-bearing detail: 0 is
    /// what a writer that finished a moment ago reads, so a member that had
    /// never checked anything reported the freshest possible reading, and
    /// read it most confidently when it knew least. It is also the
    /// "simplification" most likely to be made again.
    #[test]
    fn an_age_before_the_first_completion_is_the_time_since_start_never_zero() {
        let m = Metrics::default();
        // A tick in which every round failed: it reports, and nothing in it
        // completed or checked.
        m.record_sync_round(&kimmy_cluster::RoundReport {
            failed: 1,
            divergence_skips: 1,
            ..Default::default()
        });
        let later = m.started + Duration::from_secs(90);
        let out = m.render_at(later);
        assert!(out.contains("kimmy_sync_divergence_check_age_seconds 90\n"), "{out}");
        for writer in PROGRESS_WRITERS {
            let row = format!("kimmy_task_progress_age_seconds{{task=\"{writer}\"}} 90\n");
            assert!(out.contains(&row), "{writer}: {out}");
        }
        let snapshot = m.snapshot_with_at(&StorageReadings::default(), later);
        assert_eq!(snapshot.sync_divergence_check_age_secs, 90);
        assert_eq!(snapshot.task_progress_age_secs, [Some(90); PROGRESS_WRITERS.len()]);
    }

    /// Each writer's age resets when that writer completes, and only that
    /// writer's: a completion recorded under the wrong name would leave a dead
    /// writer reading fresh.
    #[test]
    fn each_writer_resets_its_own_age_and_no_other() {
        let m = Metrics::default();
        let later = m.started + Duration::from_secs(90);
        let ages = |m: &Metrics| m.snapshot_with_at(&StorageReadings::default(), later);
        m.record_sync_round(&kimmy_cluster::RoundReport {
            last_completed_round: Some(later - Duration::from_secs(4)),
            ..Default::default()
        });
        assert_eq!(
            ages(&m).task_progress_age_secs,
            [Some(90), Some(90), Some(4), Some(90), Some(90)],
            "only replication's, in {PROGRESS_WRITERS:?}"
        );
        // Stamped with the clock, which is 90 s short of `later`.
        m.record_runtime_stall(Duration::ZERO);
        m.set_webhook_backlog(0);
        let got = ages(&m).task_progress_age_secs;
        let slot = |writer| got[progress_slot(writer)];
        assert_eq!(slot("drop_purger"), Some(90), "the drop purger made no progress: {got:?}");
        assert_eq!(slot("embedding_worker"), Some(90), "nor the embedding worker: {got:?}");
        assert_eq!(slot("replication"), Some(4), "{got:?}");
        assert!(slot("stall_probe").is_some_and(|a| a < 90), "the stall probe woke: {got:?}");
        assert!(
            slot("webhook_dispatcher").is_some_and(|a| a < 90),
            "the dispatcher completed a pass: {got:?}"
        );
    }

    /// A writer with nothing new to report keeps ageing: two reads, no writes
    /// between them, two different ages. An age stored at the write, rather
    /// than subtracted at the read, is frozen with the gauges it qualifies.
    #[test]
    fn an_age_rises_between_reads_when_nothing_is_written() {
        let m = Metrics::default();
        let at = m.started + Duration::from_secs(10);
        m.record_sync_round(&kimmy_cluster::RoundReport {
            last_completed_round: Some(at),
            ..Default::default()
        });
        let age = |now| {
            m.snapshot_with_at(&StorageReadings::default(), now).task_progress_age_secs
                [progress_slot("replication")]
        };
        assert_eq!(age(at + Duration::from_secs(5)), Some(5));
        assert_eq!(age(at + Duration::from_secs(500)), Some(500));
    }

    /// Only the writers this node runs have a row, fixed once (ADR-187): no
    /// clustering means no replication loop, and a row for it would climb for
    /// ever or read 0, and both are lies.
    #[test]
    fn a_writer_this_node_does_not_run_has_no_row() {
        let m = Metrics::default();
        m.fix_progress_writers(&["stall_probe", "session_invalidator", "webhook_dispatcher"]);
        // A second call, as a restart of the fixing code would make, changes
        // nothing: the label set is the same on every scrape.
        m.fix_progress_writers(&PROGRESS_WRITERS);
        let out = m.render();
        let rows: Vec<&str> =
            out.lines().filter(|l| l.starts_with("kimmy_task_progress_age_seconds{")).collect();
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert!(rows[0].starts_with("kimmy_task_progress_age_seconds{task=\"stall_probe\"}"));
        assert!(
            rows[1].starts_with("kimmy_task_progress_age_seconds{task=\"webhook_dispatcher\"}")
        );
        let later = m.started + Duration::from_secs(90);
        let snapshot = m.snapshot_with_at(&StorageReadings::default(), later);
        for writer in ["drop_purger", "embedding_worker", "replication"] {
            assert_eq!(snapshot.task_progress_age_secs[progress_slot(writer)], None, "{writer}");
        }
        // And with no replication loop there is no divergence check to age: a
        // standalone node's age reads 0, not the time since it started, or
        // the documented alert would fire on it for ever.
        assert_eq!(snapshot.sync_divergence_check_age_secs, 0);
    }

    #[test]
    fn every_progress_writer_is_a_supervised_task() {
        for writer in PROGRESS_WRITERS {
            assert!(kimmy_task::TASKS.contains(&writer), "{writer} is supervised under no name");
        }
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
            ddl_applied: 0,
            ddl_held: 0,
            ddl_refused: 0,
            ddl_declined: 0,
            divergent_collections: 0,
            divergence_checks: usize::from(at == Some(checked)),
            divergence_skips: 0,
            divergence_count_compared: 0,
            divergence_count_deferred: 0,
            divergence_last_check: at,
            last_completed_round: None,
            entries_skipped_unknown_collection: 0,
            entries_skipped_beyond_advertised: 0,
            entries_skipped_purge_pending: 0,
            repair_rounds: 0,
            pulls: kimmy_cluster::PullReport::default(),
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
