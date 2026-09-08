//! The storage engine.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kimmy_core::{
    CollectionId, Error as CoreError, Hlc, HlcClock, NodeId, OpKind, OplogEntry, Stamp, vector_meta,
};
use parking_lot::{Condvar, Mutex};
use redb::{Database, ReadableDatabase, ReadableTable};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::codec;
use crate::error::{Result, StorageError};
use crate::meta::{CollectionMeta, DatabaseMeta};
use crate::tables;

/// How many change events are buffered per subscriber before it is considered
/// too slow. A lagging subscriber is told to resubscribe rather than being
/// allowed to stall writers.
const EVENT_BUFFER: usize = 1024;

/// Whether [`Engine::collections`] reports a vector shadow collection that
/// stands beside its parent in the same database.
///
/// `Hidden` is the ADR-138 rule: a shadow whose parent is present is the
/// owning member's lifecycle lag, not a divergence, so the cross-member
/// existence check leaves it out; an orphaned shadow — parent gone — is
/// residue and stays in under either value. `Included` hides nothing. The
/// ids `Hidden` removes are exactly the ones a vector index is keyed by, so
/// anything reconciling the index cache must ask for `Included`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PairedShadows {
    /// Leave out a shadow whose parent is present (ADR-138).
    Hidden,
    /// Report every collection there is.
    Included,
}

pub struct Engine {
    db: Database,
    node_id: NodeId,
    /// Guards the HLC. Every write takes this briefly to mint a stamp, so it
    /// must never be held across a redb commit.
    clock: Mutex<HlcClock>,
    events: broadcast::Sender<Arc<OplogEntry>>,
    /// Bumped whenever a collection's vectors change.
    ///
    /// An in-memory vector index is built from a snapshot and cannot see later
    /// writes. Counting vectors to detect that would be O(n) per query, and a
    /// count misses the case where one document is deleted and another added.
    /// A counter is exact and free.
    vector_generations: Mutex<std::collections::HashMap<CollectionId, u64>>,
    /// Where the database file lives, kept so that size can be reported
    /// without the caller having to remember what it opened.
    path: std::path::PathBuf,
    /// Unique constraints broken by merging replicated writes, since start.
    ///
    /// A counter rather than only a log line so the condition is visible on the
    /// metrics endpoint without anyone having to be watching a stream when it
    /// happens — see [ADR-020](../../../docs/decisions.md).
    unique_violations: std::sync::atomic::AtomicU64,
    /// Documents filed under an index's unkeyed run, since start: stored,
    /// but with no key the index could derive for them, so every scan of that
    /// index rechecks them (ADR-139). Local writes, replicated writes and
    /// backfills all count here, because all three file them; a client sees
    /// the standing number per index as `unkeyed` on the listing, and this is
    /// the rate, for the metrics endpoint.
    unkeyed_writes: std::sync::atomic::AtomicU64,
    /// Durable write transactions committed, since start.
    ///
    /// redb has a single writer and every commit is an fsync, so the number of
    /// commits a piece of work costs is the thing that decides what a write is
    /// worth — not how much of it is CPU. Counting them makes "an insert is one
    /// commit" a property of a running node rather than a claim in a comment,
    /// which is what M11 task 1 needed: the daemon was paying two commits per
    /// insert where a bare engine paid one, and nothing said so.
    commits: std::sync::atomic::AtomicU64,
    /// Documents a `multi: true` filtered write commits per transaction
    /// (ADR-086). Set from configuration at startup; the storage default
    /// stands for a bare engine.
    multi_chunk_docs: std::sync::atomic::AtomicUsize,
    /// How a commit becomes durable (ADR-088). `None` is the default class,
    /// `durable`: every commit fsyncs before it returns. `Some` is
    /// `coalesced`: commits skip their own fsync and wait at a shared barrier
    /// that fsyncs once per window.
    coalescer: Mutex<Option<Coalescer>>,
    /// Wakes committers waiting at the barrier; beside the mutex rather than
    /// inside it so a wait can re-acquire the lock it released.
    coalesce_woken: Condvar,
    /// Commits that reached the disk with their own fsync, or the shared
    /// fsync of a barrier flush — the number of times the disk was asked to
    /// make something durable.
    fsyncs: std::sync::atomic::AtomicU64,
    /// Commits that skipped their own fsync and were made durable by a
    /// barrier flush shared with others (ADR-088).
    grouped_commits: std::sync::atomic::AtomicU64,
    /// The queue for the single writer (ADR-151). Taken before redb's own
    /// writer lock, held for the life of the [`WriteTxn`], and released when
    /// the transaction commits, aborts or is dropped. Two things redb's lock
    /// does not give: a wait a caller can bound, and eventual fairness — a
    /// path that takes and releases the writer in a tight loop is made to
    /// hand it over rather than winning every time against a thread that
    /// was woken and had to race for it.
    writer_gate: parking_lot::Mutex<()>,
    /// How long callers waited for the writer, as a histogram over
    /// [`WRITER_WAIT_BUCKETS_US`]; `count` and `sum` beside it.
    writer_wait_buckets: [std::sync::atomic::AtomicU64; WRITER_WAIT_BUCKETS_US.len()],
    writer_wait_count: std::sync::atomic::AtomicU64,
    writer_wait_sum_us: std::sync::atomic::AtomicU64,
    /// Writes that gave up waiting for the writer inside their budget.
    writer_wait_timeouts: std::sync::atomic::AtomicU64,
    /// The longest any one transaction has held the writer, since start.
    writer_hold_max_us: std::sync::atomic::AtomicU64,
    /// Where the retention pass's tombstone scan resumes next pass
    /// (ADR-151): the last document key it visited, or `None` to start from
    /// the top. The scan visits a bounded number of documents per pass.
    gc_scan_cursor: parking_lot::Mutex<Option<(u64, Vec<u8>)>>,
}

/// Upper bounds of the writer-wait histogram, in microseconds.
///
/// One millisecond is what a wait costs when the writer is free and another
/// commit's fsync is finishing; the top bucket is the request timeout's
/// default. Everything between is where a client write sits while a long
/// transaction — a bulk, a repair, a retention pass — holds the writer.
pub const WRITER_WAIT_BUCKETS_US: [u64; 8] =
    [1_000, 5_000, 25_000, 100_000, 500_000, 1_000_000, 5_000_000, 30_000_000];

/// A transaction that held the writer longer than this is logged at WARN,
/// with the span it was opened under, when it lets go. Five seconds is
/// more than a bulk of ten thousand documents costs and a small fraction of
/// the request timeout a client write is waiting under.
pub const WRITER_HOLD_WARN: std::time::Duration = std::time::Duration::from_secs(5);

tokio::task_local! {
    /// The longest the current task is prepared to wait for the writer.
    static WRITE_WAIT_BUDGET: std::time::Duration;
}

/// Run `f` with every write it opens bounded to `budget` of waiting for the
/// writer (ADR-151).
///
/// A write that cannot take the writer inside the budget fails with
/// [`StorageError::WriterBusy`] rather than blocking, having written nothing.
/// The request path sets this to `server.request_timeout_secs`, so a client
/// sees the documented `503 timeout` instead of a hang: the timeout
/// middleware cannot abandon a handler that is blocked inside
/// [`blocking`], because the future never yields while it waits. Background
/// work — replication, retention, TTL, the embedding worker — sets no budget
/// and waits as long as it takes.
pub async fn with_write_wait_budget<F: std::future::Future>(
    budget: std::time::Duration,
    f: F,
) -> F::Output {
    WRITE_WAIT_BUDGET.scope(budget, f).await
}

/// The budget the current task set, if any.
fn write_wait_budget() -> Option<std::time::Duration> {
    WRITE_WAIT_BUDGET.try_with(|budget| *budget).ok()
}

/// An exclusive hold of the writer; see [`Engine::hold_writer`].
pub struct WriterHold<'a> {
    _gate: parking_lot::MutexGuard<'a, ()>,
    engine: &'a Engine,
    held_from: std::time::Instant,
}

impl Drop for WriterHold<'_> {
    fn drop(&mut self) {
        self.engine.record_writer_hold(self.held_from.elapsed(), "hold_writer");
    }
}

/// The writer-wait histogram, as a scrape reads it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriterWaitSnapshot {
    /// Waits in each bucket of [`WRITER_WAIT_BUCKETS_US`], **not**
    /// cumulative.
    pub buckets: [u64; WRITER_WAIT_BUCKETS_US.len()],
    pub count: u64,
    pub sum_us: u64,
}

/// How a commit becomes durable (ADR-088).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityClass {
    /// Every commit fsyncs before it returns. The default, and what every
    /// release before 0.12 did.
    Durable,
    /// A commit is written without its own fsync and then **waits** for the
    /// next shared fsync, which runs once per `commit_coalesce_ms` window and
    /// covers every commit that arrived during it. Durable when the call
    /// returns, exactly as `Durable`; what changes is that N concurrent
    /// writers pay one fsync rather than N.
    Coalesced,
}

impl DurabilityClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Coalesced => "coalesced",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "durable" => Some(Self::Durable),
            "coalesced" => Some(Self::Coalesced),
            _ => None,
        }
    }
}

/// Run a blocking storage step without holding a tokio worker hostage.
///
/// redb has a single writer: `begin_write` waits for whoever holds the lock,
/// and `commit` ends in an fsync. Both are fine on a thread of their own and
/// ruinous on an async worker — a handful of concurrent writers pin every
/// worker of the runtime, and nothing else that needs one runs: peers' TLS
/// handshakes time out, `/metrics` hangs, SWIM probes go unanswered and the
/// member is marked down by its peers. Measured on a three-member cluster on
/// 2026-08-28 with bulk inserts spread across members: 5 s handshake timeouts
/// on every pair, a 10 s `/metrics` stall, membership flapping for 30 s.
///
/// `block_in_place` tells the runtime this worker is about to block, so it
/// moves the worker's queued tasks elsewhere and carries on; the blocking
/// step then runs inline with no thread hop and no `Send` bound. Off a
/// multi-thread runtime — the CLI, a `current_thread` test, a plain thread —
/// there is nothing to yield to and the closure simply runs.
pub fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// The shared-fsync barrier behind [`DurabilityClass::Coalesced`].
///
/// No background thread and no handle to the engine: the committers
/// themselves run it. The first committer to arrive after a flush becomes
/// the *leader*, sleeps one window so others can join, then performs one
/// durable commit and wakes everyone whose commit it covered. Later arrivals
/// during the window are *followers* and only wait. A committer always
/// waits for a flush that started after its own commit, which is what makes
/// "durable when the call returns" hold.
struct Coalescer {
    window: std::time::Duration,
    /// Commits so far that are waiting on, or have had, a flush.
    requested: u64,
    /// Commits covered by the last completed flush.
    flushed: u64,
    leader_running: bool,
}

impl Coalescer {
    fn new(window: std::time::Duration) -> Self {
        Self { window, requested: 0, flushed: 0, leader_running: false }
    }
}

/// A write transaction that counts itself when it commits, and makes itself
/// durable the way the engine's durability class says (ADR-088).
///
/// Aborts are not counted, deliberately: an abort does not fsync, and the
/// question this exists to answer is how many times a write path reaches the
/// disk. Derefs to the redb transaction, so `open_table` and the rest are
/// unchanged at the call sites.
pub(crate) struct WriteTxn<'a> {
    /// `None` only once `commit` or `abort` has taken it; `Drop` handles the
    /// transaction that was neither, which redb aborts.
    txn: Option<redb::WriteTransaction>,
    engine: &'a Engine,
    /// Whether this transaction was opened without its own fsync and must
    /// wait at the barrier after committing.
    coalesced: bool,
    /// The place in the writer queue (ADR-151). `None` once released, which
    /// `commit` does before waiting at the coalescing barrier — the flush
    /// leader takes the gate for its own transaction.
    gate: Option<parking_lot::MutexGuard<'a, ()>>,
    /// When the writer was taken, for the hold measurement.
    held_from: std::time::Instant,
    /// The span this transaction was opened under — `replace`, `bulk`,
    /// `cluster.sync`, `storage.retention` — so a long hold names its cause.
    holder: &'static str,
}

impl WriteTxn<'_> {
    /// Let go of the writer and record how long it was held.
    fn release(&mut self) {
        if self.gate.take().is_some() {
            self.engine.record_writer_hold(self.held_from.elapsed(), self.holder);
        }
    }

    pub(crate) fn commit(mut self) -> std::result::Result<(), redb::CommitError> {
        // The one span in this crate, at the one place a write reaches the
        // disk. `commits_are_counted_at_one_chokepoint` already proves this is
        // the only such place, so the span inherits that proof: redb has a
        // single writer and every commit is an fsync, which makes this the
        // segment of a trace that shows what a write *cost* rather than how
        // much of it was CPU.
        //
        // Plain `tracing`, with no OpenTelemetry dependency in this crate. The
        // binary's `tracing-opentelemetry` layer converts it if an operator
        // configured a collector, and if none is configured this is the same
        // disabled-span check every other `tracing` call site already pays.
        let _span = tracing::info_span!("storage.commit").entered();
        let txn = self.txn.take().expect("a transaction is taken once");
        let engine = self.engine;
        let coalesced = self.coalesced;
        // The fsync (or the wait at the barrier) is the blocking part; see
        // [`blocking`] for why it must not happen on an async worker.
        blocking(move || {
            txn.commit()?;
            engine.commits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // The writer is free from here: what follows is the barrier,
            // whose leader opens a transaction of its own (ADR-088).
            self.release();
            if coalesced {
                engine.grouped_commits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                engine.wait_for_flush()?;
            } else {
                engine.fsyncs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(())
        })
    }

    pub(crate) fn abort(mut self) -> std::result::Result<(), redb::StorageError> {
        let txn = self.txn.take().expect("a transaction is taken once");
        let result = txn.abort();
        self.release();
        result
    }
}

impl Drop for WriteTxn<'_> {
    fn drop(&mut self) {
        // A transaction dropped on an error path: redb aborts it, and the
        // writer is let go after that, never before.
        drop(self.txn.take());
        self.release();
    }
}

impl std::ops::Deref for WriteTxn<'_> {
    type Target = redb::WriteTransaction;

    fn deref(&self) -> &Self::Target {
        self.txn.as_ref().expect("a transaction is taken only by commit or abort")
    }
}

impl Engine {
    /// Open or create the database at `path`.
    ///
    /// Node identity lives in the database file rather than beside it, so that
    /// copying or restoring the file carries the identity with it. Identity
    /// must survive restarts: it is the tiebreak half of every write's stamp.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_cache(path, None)
    }

    /// Open with a bound on redb's page cache.
    ///
    /// The cache is where a node's resident memory goes: redb keeps up to
    /// this many bytes of pages and evicts only for room, never on a timer,
    /// so after a burst of reads a node sits at whatever the burst filled —
    /// measured on a three-member cluster at 460–590 MiB per member with
    /// every collection dropped and nothing to do. `None` is redb's own
    /// default (1 GiB). The daemon sets this from `storage.cache_bytes`.
    pub fn open_with_cache(path: &Path, cache_bytes: Option<usize>) -> Result<Self> {
        let db = match cache_bytes {
            Some(bytes) => Database::builder().set_cache_size(bytes).create(path)?,
            None => Database::create(path)?,
        };

        // Ensure every table exists up front so that read transactions never
        // have to handle a missing table.
        let txn = db.begin_write()?;
        {
            let _ = txn.open_table(tables::META)?;
            let _ = txn.open_table(tables::DATABASES)?;
            let _ = txn.open_table(tables::COLLECTIONS)?;
            let _ = txn.open_table(tables::DOCS)?;
            let _ = txn.open_table(tables::INDEX_ENTRIES)?;
            let _ = txn.open_table(tables::OPLOG)?;
            let _ = txn.open_table(tables::OPLOG_ARRIVAL)?;
            let _ = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;
            let _ = txn.open_table(tables::OPLOG_VERSIONS)?;
            let _ = txn.open_table(tables::OPLOG_WITNESSED)?;
            let _ = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            let _ = txn.open_table(tables::INDEXES_DROPPED)?;
            let _ = txn.open_table(tables::OPLOG_COLLECTED)?;
        }
        txn.commit()?;

        // Before anything reads a collection id: schema 1 allocated them from a
        // counter, schema 2 derives them from the name.
        crate::migrate::run(&db)?;

        // The arrival index is derived from the oplog, so a database written
        // before it existed — or by a build that did not maintain it — is
        // repaired rather than refused. That is why adding it needed no format
        // version bump: there is no state here that the oplog does not already
        // determine.
        Self::rebuild_arrival_index_if_stale(&db)?;
        Self::rebuild_version_vector_if_stale(&db)?;
        Self::seed_collected_if_untracked(&db)?;

        let node_id = Self::load_or_create_node_id(&db)?;
        let resumed = Self::last_oplog_hlc(&db)?;

        if resumed != Hlc::ZERO {
            debug!(hlc = %resumed, "resumed logical clock from the oplog tail");
        }

        let (events, _) = broadcast::channel(EVENT_BUFFER);

        info!(node = %node_id, path = %path.display(), "storage engine open");

        Ok(Self {
            db,
            node_id,
            clock: Mutex::new(HlcClock::resuming_from(resumed)),
            events,
            vector_generations: Mutex::new(Default::default()),
            path: path.to_path_buf(),
            unique_violations: std::sync::atomic::AtomicU64::new(0),
            unkeyed_writes: std::sync::atomic::AtomicU64::new(0),
            commits: std::sync::atomic::AtomicU64::new(0),
            multi_chunk_docs: std::sync::atomic::AtomicUsize::new(
                crate::modify::DEFAULT_MULTI_CHUNK_DOCS,
            ),
            coalescer: Mutex::new(None),
            coalesce_woken: Condvar::new(),
            fsyncs: std::sync::atomic::AtomicU64::new(0),
            grouped_commits: std::sync::atomic::AtomicU64::new(0),
            writer_gate: parking_lot::Mutex::new(()),
            writer_wait_buckets: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            writer_wait_count: std::sync::atomic::AtomicU64::new(0),
            writer_wait_sum_us: std::sync::atomic::AtomicU64::new(0),
            writer_wait_timeouts: std::sync::atomic::AtomicU64::new(0),
            writer_hold_max_us: std::sync::atomic::AtomicU64::new(0),
            gc_scan_cursor: parking_lot::Mutex::new(None),
        })
    }

    /// How long callers have waited for the writer, since start (ADR-151).
    pub fn writer_wait(&self) -> WriterWaitSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        WriterWaitSnapshot {
            buckets: std::array::from_fn(|slot| self.writer_wait_buckets[slot].load(Relaxed)),
            count: self.writer_wait_count.load(Relaxed),
            sum_us: self.writer_wait_sum_us.load(Relaxed),
        }
    }

    /// Writes that gave up waiting for the writer inside their budget, since
    /// start (ADR-151).
    pub fn writer_wait_timeouts(&self) -> u64 {
        self.writer_wait_timeouts.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The longest any one transaction has held the writer, since start.
    pub fn writer_hold_max(&self) -> std::time::Duration {
        std::time::Duration::from_micros(
            self.writer_hold_max_us.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Take the writer and hold it until the guard is dropped (ADR-151).
    ///
    /// Every write on this engine waits behind the hold, exactly as behind
    /// a transaction; a caller with a budget gives up inside it. For a test
    /// that needs the writer busy, and for nothing on a request path.
    pub fn hold_writer(&self) -> WriterHold<'_> {
        let gate = blocking(|| self.writer_gate.lock());
        WriterHold { _gate: gate, engine: self, held_from: std::time::Instant::now() }
    }

    fn record_writer_wait(&self, waited: std::time::Duration) {
        use std::sync::atomic::Ordering::Relaxed;
        let us = u64::try_from(waited.as_micros()).unwrap_or(u64::MAX);
        if let Some(slot) = WRITER_WAIT_BUCKETS_US.iter().position(|upper| us <= *upper) {
            self.writer_wait_buckets[slot].fetch_add(1, Relaxed);
        }
        self.writer_wait_count.fetch_add(1, Relaxed);
        self.writer_wait_sum_us.fetch_add(us, Relaxed);
    }

    fn record_writer_hold(&self, held: std::time::Duration, holder: &'static str) {
        let us = u64::try_from(held.as_micros()).unwrap_or(u64::MAX);
        self.writer_hold_max_us.fetch_max(us, std::sync::atomic::Ordering::Relaxed);
        if held >= WRITER_HOLD_WARN {
            warn!(
                held_ms = held.as_millis() as u64,
                holder,
                "a transaction held the single writer for longer than {} s; every other \
                 write on this node waited behind it",
                WRITER_HOLD_WARN.as_secs()
            );
        }
    }

    /// Where the retention pass's tombstone scan resumes (ADR-151).
    pub(crate) fn gc_scan_cursor(&self) -> Option<(u64, Vec<u8>)> {
        self.gc_scan_cursor.lock().clone()
    }

    pub(crate) fn set_gc_scan_cursor(&self, cursor: Option<(u64, Vec<u8>)>) {
        *self.gc_scan_cursor.lock() = cursor;
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Size of the database file on disk, or zero if it cannot be read.
    ///
    /// Zero rather than an error: this exists for a metrics endpoint, and a
    /// gauge that fails the whole scrape because one `stat` did is worse than a
    /// gauge that reads zero.
    pub fn storage_bytes(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    /// How many times this collection's vectors have changed, whether the
    /// change was written here or arrived by replication.
    ///
    /// Resets to zero on restart, which is correct: an in-memory index does
    /// not survive one either.
    pub fn vector_generation(&self, collection: CollectionId) -> u64 {
        self.vector_generations.lock().get(&collection).copied().unwrap_or(0)
    }

    /// How many merged writes have broken a unique constraint since start.
    pub fn unique_violations(&self) -> u64 {
        self.unique_violations.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many documents have been filed unkeyed under an index since start.
    pub fn unkeyed_writes(&self) -> u64 {
        self.unkeyed_writes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many durable write transactions have committed since start.
    ///
    /// One per unit of work is the expectation. Anything that turns one
    /// client-visible write into two commits doubles what that write costs,
    /// and this is where that shows up.
    pub fn commits(&self) -> u64 {
        self.commits.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many documents a `multi: true` filtered write commits per
    /// transaction (ADR-086).
    pub fn multi_chunk_docs(&self) -> usize {
        self.multi_chunk_docs.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set the chunk size. Clamped to `1..=MAX_CANDIDATES`: zero would never
    /// advance, and more than the cap would hold the writer for longer than
    /// `find_and_modify` is allowed to.
    pub fn set_multi_chunk_docs(&self, docs: usize) {
        let docs = docs.clamp(1, crate::modify::MAX_CANDIDATES);
        self.multi_chunk_docs.store(docs, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn count_unique_violation(&self) {
        self.unique_violations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn count_unkeyed(&self, n: u64) {
        self.unkeyed_writes.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record that a collection's vectors have changed.
    ///
    /// Called after the change is committed, never before, by every path that
    /// writes a shadow collection: `put_vectors` and `delete_vectors` for a
    /// local write, and `report_remote_write` for one that arrived by
    /// replication. Bumping before the commit would let a build read the new
    /// generation against the old data and be served as fresh for it.
    pub(crate) fn bump_vector_generation(&self, collection: CollectionId) {
        *self.vector_generations.lock().entry(collection).or_insert(0) += 1;
    }

    /// Subscribe to the live change feed.
    ///
    /// This is only half of a change stream: on its own it starts at "now" and
    /// misses anything already written. [`crate::watch`] combines it with an
    /// oplog replay to deliver a gap-free sequence.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<OplogEntry>> {
        self.events.subscribe()
    }

    fn load_or_create_node_id(db: &Database) -> Result<NodeId> {
        let txn = db.begin_write()?;
        let id = {
            let mut meta = txn.open_table(tables::META)?;

            // Read guards borrow the table, so copy out what we need before
            // any insert.
            let stored_node =
                meta.get(tables::META_NODE_ID)?.map(|v| <[u8; 16]>::try_from(v.value()));

            match stored_node {
                Some(bytes) => NodeId::from_bytes(
                    bytes.map_err(|_| StorageError::Corrupt("node id is not 16 bytes".into()))?,
                ),
                None => {
                    let id = NodeId::generate();
                    meta.insert(tables::META_NODE_ID, id.to_bytes().as_slice())?;
                    info!(node = %id, "generated a new node identity");
                    id
                }
            }
        };
        txn.commit()?;
        Ok(id)
    }

    /// Rebuild the arrival index if it does not cover the oplog exactly.
    ///
    /// Cheap to check — two counts — and only pays the rebuild when something
    /// is actually wrong: a database written before the index existed, or one
    /// an older build appended to after this one had created it. Comparing
    /// counts rather than contents is enough because the index is only ever
    /// written alongside an oplog append and collected alongside an oplog
    /// removal, so a length mismatch is the only way it can diverge.
    ///
    /// Existing history is ordered by stamp, which is correct: everything
    /// written before this index existed was locally originated, and for local
    /// writes arrival order *is* stamp order.
    fn rebuild_arrival_index_if_stale(db: &Database) -> Result<()> {
        {
            let txn = db.begin_read()?;
            let oplog = txn.open_table(tables::OPLOG)?;
            let arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
            if oplog.iter()?.count() == arrival.iter()?.count() {
                return Ok(());
            }
        }

        let txn = db.begin_write()?;
        let rebuilt = {
            let oplog = txn.open_table(tables::OPLOG)?;
            let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
            let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;
            arrival.retain(|_, _| false)?;
            by_stamp.retain(|_, _| false)?;

            let mut seq = 0u64;
            for row in oplog.iter()? {
                let (key, _) = row?;
                arrival.insert(seq, key.value())?;
                by_stamp.insert(key.value(), seq)?;
                seq += 1;
            }
            seq
        };
        txn.commit()?;

        if rebuilt > 0 {
            info!(entries = rebuilt, "rebuilt the oplog arrival index");
        }
        Ok(())
    }

    /// Raise the version vector to cover everything in the oplog.
    ///
    /// **Only ever raises.** The vector was derived state when the oplog was
    /// the sole way to gain coverage; a snapshot transfer grants coverage of
    /// entries this node will never hold, so the oplog is now a *lower bound*
    /// on what has been seen rather than the whole truth. Recomputing from it
    /// would silently undo a completed snapshot and send the node back to
    /// asking for history it has already been given another way.
    ///
    /// What it still does is repair a vector that has fallen behind: a database
    /// written before the vector existed, or one an older build appended to.
    pub(crate) fn rebuild_version_vector_if_stale(db: &Database) -> Result<()> {
        let mut actual = kimmy_core::VersionVector::new();
        {
            let txn = db.begin_read()?;
            let oplog = txn.open_table(tables::OPLOG)?;
            for row in oplog.iter()? {
                let (key, _) = row?;
                actual.observe(codec::decode_oplog_key(key.value())?);
            }
        }

        let mut stored = Self::read_versions(db, tables::OPLOG_VERSIONS)?;
        let before = stored.clone();
        stored.merge(&actual);

        // A database written before the witnessed vector existed has an empty
        // one. Seeding it from the servable vector is the safe lower bound:
        // the next sync round re-fetches once, witnesses what it processes,
        // and goes quiet (ADR-054).
        let mut witnessed = Self::read_versions(db, tables::OPLOG_WITNESSED)?;
        let witnessed_before = witnessed.clone();
        witnessed.merge(&stored);

        if stored == before && witnessed == witnessed_before {
            return Ok(());
        }

        let txn = db.begin_write()?;
        {
            let mut versions = txn.open_table(tables::OPLOG_VERSIONS)?;
            for (node, hlc) in stored.iter() {
                versions.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }
            let mut seen = txn.open_table(tables::OPLOG_WITNESSED)?;
            for (node, hlc) in witnessed.iter() {
                seen.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }
        }
        txn.commit()?;

        info!(nodes = stored.len(), "raised the version vector to cover the oplog");
        Ok(())
    }

    /// Replace the version vector with exactly what the oplog now covers.
    ///
    /// **The only legitimate lowering.** `rebuild_version_vector_if_stale`
    /// merges, so it can only raise: the vector is authoritative, and
    /// recomputing it during normal operation would discard coverage a snapshot
    /// granted that the oplog never held.
    ///
    /// A point-in-time rewind is the exception, because it *removed* oplog
    /// entries on purpose. Leaving the vector high afterwards would have the
    /// node claim history it no longer holds, and no peer would ever send that
    /// range again — the node would be permanently missing writes and would
    /// look caught up.
    pub(crate) fn reset_version_vector_to_oplog(db: &Database) -> Result<()> {
        let mut actual = kimmy_core::VersionVector::new();
        {
            let txn = db.begin_read()?;
            let oplog = txn.open_table(tables::OPLOG)?;
            for row in oplog.iter()? {
                let (key, _) = row?;
                actual.observe(codec::decode_oplog_key(key.value())?);
            }
        }

        let txn = db.begin_write()?;
        {
            // **Both** vectors. If witnessed stayed high, the node would
            // believe it had already seen the entries the rewind removed and
            // would never ask for them again — permanently missing writes
            // while looking caught up, which is the very failure this function
            // exists to prevent (ADR-054).
            let mut seen = txn.open_table(tables::OPLOG_WITNESSED)?;
            seen.retain(|_, _| false)?;
            for (node, hlc) in actual.iter() {
                seen.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }

            let mut versions = txn.open_table(tables::OPLOG_VERSIONS)?;
            // Cleared rather than overwritten: a node that appears in the old
            // vector but no longer in the oplog has to disappear entirely, and
            // inserting over the top would leave its stale entry behind.
            versions.retain(|_, _| false)?;
            for (node, hlc) in actual.iter() {
                versions.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }
        }
        txn.commit()?;

        info!(nodes = actual.len(), "reset the version vector to the rewound oplog");
        Ok(())
    }

    fn read_versions(
        db: &Database,
        table: redb::TableDefinition<&'static [u8], &'static [u8]>,
    ) -> Result<kimmy_core::VersionVector> {
        let txn = db.begin_read()?;
        let versions = txn.open_table(table)?;
        let mut out = kimmy_core::VersionVector::new();
        for row in versions.iter()? {
            let (node, hlc) = row?;
            out.insert(decode_node(node.value())?, decode_hlc(hlc.value())?);
        }
        Ok(out)
    }

    /// When this collection was dropped, if a tombstone still records it.
    ///
    /// `None` means either that it was never dropped or that the tombstone has
    /// been collected — the two are indistinguishable, which is exactly why
    /// `tombstone_retention_secs` must exceed the longest partition you intend
    /// to survive.
    pub fn collection_dropped_at(&self, id: CollectionId) -> Result<Option<Stamp>> {
        let txn = self.db.begin_read()?;
        let dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        match dropped.get(id.0)? {
            Some(raw) => Ok(Some(codec::decode_oplog_key(raw.value())?)),
            None => Ok(None),
        }
    }

    /// Record that a collection was dropped at `stamp`, if that is newer.
    pub(crate) fn record_collection_drop(&self, id: CollectionId, stamp: Stamp) -> Result<()> {
        let txn = self.begin_write()?;
        {
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            let newer = match dropped.get(id.0)? {
                Some(existing) => stamp > codec::decode_oplog_key(existing.value())?,
                None => true,
            };
            if newer {
                dropped.insert(id.0, codec::oplog_key(&stamp).as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// When an index on this collection was dropped, if a tombstone still
    /// records it.
    ///
    /// `None` means never dropped or already collected, indistinguishably, as
    /// for [`Self::collection_dropped_at`]. `index_id` is the id derived from
    /// the index name, which is what both the create and the drop entry can
    /// compute (ADR-123).
    pub fn index_dropped_at(
        &self,
        collection: CollectionId,
        index_id: u32,
    ) -> Result<Option<Stamp>> {
        let txn = self.db.begin_read()?;
        let dropped = txn.open_table(tables::INDEXES_DROPPED)?;
        match dropped.get((collection.0, index_id))? {
            Some(raw) => Ok(Some(codec::decode_oplog_key(raw.value())?)),
            None => Ok(None),
        }
    }

    /// Record that an index was dropped at `stamp`, if that is newer, in the
    /// caller's transaction.
    ///
    /// In-transaction because a local drop records its tombstone in the same
    /// transaction that removes the entries and mints the drop entry: there
    /// must be no instant in which the index is gone with no record that it
    /// was dropped, exactly as `drop_collection_inner` keeps for collections.
    pub(crate) fn record_index_drop_in_txn(
        txn: &redb::WriteTransaction,
        collection: CollectionId,
        index_id: u32,
        stamp: Stamp,
    ) -> Result<()> {
        let mut dropped = txn.open_table(tables::INDEXES_DROPPED)?;
        let newer = match dropped.get((collection.0, index_id))? {
            Some(existing) => stamp > codec::decode_oplog_key(existing.value())?,
            None => true,
        };
        if newer {
            dropped.insert((collection.0, index_id), codec::oplog_key(&stamp).as_slice())?;
        }
        Ok(())
    }

    /// [`Self::record_index_drop_in_txn`] in a transaction of its own, for the
    /// replicated path when there is nothing else to write: a drop for an
    /// index this node does not hold, or for a collection it no longer has.
    pub(crate) fn record_index_drop(
        &self,
        collection: CollectionId,
        index_id: u32,
        stamp: Stamp,
    ) -> Result<()> {
        let txn = self.begin_write()?;
        Self::record_index_drop_in_txn(&txn, collection, index_id, stamp)?;
        txn.commit()?;
        Ok(())
    }

    /// Record coverage granted by a snapshot.
    ///
    /// Merged rather than replaced, so writes this node made that the sender
    /// never saw are not claimed to be forgotten.
    pub fn absorb_version_vector(&self, granted: &kimmy_core::VersionVector) -> Result<()> {
        let mut current = Self::read_versions(&self.db, tables::OPLOG_VERSIONS)?;
        current.merge(granted);

        // Both: a snapshot hands over state, which is the strongest form of
        // having processed everything behind it. Raising only the servable
        // vector would leave the node still asking for the history the
        // snapshot replaced.
        let mut witnessed = Self::read_versions(&self.db, tables::OPLOG_WITNESSED)?;
        witnessed.merge(&current);

        let txn = self.begin_write()?;
        {
            let mut seen = txn.open_table(tables::OPLOG_WITNESSED)?;
            for (node, hlc) in witnessed.iter() {
                seen.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }
            let mut versions = txn.open_table(tables::OPLOG_VERSIONS)?;
            for (node, hlc) in current.iter() {
                versions.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// The highest `Hlc` retention has removed from the oplog.
    ///
    /// [`Hlc::ZERO`] when nothing has ever been collected, which reads
    /// naturally: every peer is at or above it, so every peer can be served
    /// incrementally.
    pub fn oplog_collected_through(&self) -> Result<Hlc> {
        Self::read_collected_through(&self.db)
    }

    fn read_collected_through(db: &Database) -> Result<Hlc> {
        let txn = db.begin_read()?;
        let meta = txn.open_table(tables::META)?;
        match meta.get(tables::META_OPLOG_COLLECTED_THROUGH)? {
            Some(raw) => Ok(codec::decode_oplog_key(raw.value())?.hlc),
            None => Ok(Hlc::ZERO),
        }
    }

    /// Per origin, the highest `Hlc` retention has removed from the oplog.
    ///
    /// [`Self::oplog_collected_through`] split by origin, and the record a
    /// peer's coverage is compared against to decide whether it lacks
    /// anything this node can no longer serve: `theirs.get(origin)` below
    /// this vector's entry for that origin means at least one collected
    /// entry is missing there; at or above it, nothing of that origin has
    /// been removed that the peer does not hold. An origin absent from the
    /// vector has had nothing collected — which reads as [`Hlc::ZERO`], the
    /// same way an empty version vector does.
    pub fn oplog_collected(&self) -> Result<kimmy_core::VersionVector> {
        Self::read_versions(&self.db, tables::OPLOG_COLLECTED)
    }

    /// Give every held origin the coarse horizon when the per-origin record
    /// cannot vouch for what was collected.
    ///
    /// The per-origin table is written by the same retention pass that moves
    /// the coarse horizon, so on a database only this build has collected
    /// from, the highest entry in the table *is* the horizon. A horizon above
    /// the table — a database an earlier build collected from, or one an
    /// earlier build ran against between two runs of this one — means
    /// entries were removed that the table never saw, from origins it cannot
    /// name. Every origin the node holds is raised to the horizon then, which
    /// is exactly the coarse answer the horizon alone gave: a peer whose
    /// coverage of any origin sits below it is told it is beyond it. Above
    /// that point the record is exact, so the seeding costs nothing that was
    /// not already being paid, and the cost ends once every origin has
    /// written again and been collected once more.
    ///
    /// Only raises, like every other movement of a version table. Origins
    /// the node learns of later need no seeding: nothing of theirs was held
    /// here to collect before they were known.
    pub(crate) fn seed_collected_if_untracked(db: &Database) -> Result<()> {
        let horizon = Self::read_collected_through(db)?;
        if horizon == Hlc::ZERO {
            return Ok(());
        }
        let collected = Self::read_versions(db, tables::OPLOG_COLLECTED)?;
        if collected.iter().map(|(_, hlc)| hlc).max().is_some_and(|max| max >= horizon) {
            return Ok(());
        }

        let held = Self::read_versions(db, tables::OPLOG_VERSIONS)?;
        let txn = db.begin_write()?;
        let mut seeded = 0usize;
        {
            let mut table = txn.open_table(tables::OPLOG_COLLECTED)?;
            for (node, _) in held.iter() {
                if collected.get(node) < horizon {
                    table.insert(node.to_bytes().as_slice(), horizon.to_bytes().as_slice())?;
                    seeded += 1;
                }
            }
        }
        txn.commit()?;

        if seeded > 0 {
            info!(
                origins = seeded,
                horizon = %horizon,
                "seeded the per-origin retention record from the coarse horizon"
            );
        }
        Ok(())
    }

    /// The oldest stamp still in the oplog, if any.
    ///
    /// A peer asking from a point below this cannot be served incrementally:
    /// the history it is missing has been collected.
    pub fn oldest_retained(&self) -> Result<Option<Stamp>> {
        let txn = self.db.begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        match oplog.first()? {
            Some((key, _)) => Ok(Some(codec::decode_oplog_key(key.value())?)),
            None => Ok(None),
        }
    }

    /// What this node holds, summarized per originating node.
    ///
    /// The half of anti-entropy a peer needs in order to work out what to send.
    pub fn version_vector(&self) -> Result<kimmy_core::VersionVector> {
        Self::read_versions(&self.db, tables::OPLOG_VERSIONS)
    }

    /// What this node has **processed**, per origin — appended or not.
    ///
    /// This is what "am I behind a peer" and "how far behind" must be asked
    /// against. [`Self::version_vector`] answers a different question — what
    /// this node can *serve* — and is what a peer receives. Conflating them
    /// made every cluster re-request the same entries forever (ADR-054).
    pub fn witnessed_vector(&self) -> Result<kimmy_core::VersionVector> {
        Self::read_versions(&self.db, tables::OPLOG_WITNESSED)
    }

    /// Raise the witnessed vector to cover a whole batch, in one transaction.
    ///
    /// Only ever raises, like every other movement of a version vector: a
    /// lowering would send the node back to asking for history it has already
    /// processed.
    pub fn absorb_witnessed(&self, seen: &kimmy_core::VersionVector) -> Result<()> {
        if seen.is_empty() {
            return Ok(());
        }
        let txn = self.begin_write()?;
        Self::absorb_witnessed_in_txn(&txn, seen)?;
        txn.commit()?;
        Ok(())
    }

    /// [`Self::absorb_witnessed`] inside a transaction the caller owns.
    ///
    /// A sync batch raises its witnessed vector in the same transaction that
    /// holds the batch's last run of documents, so a DDL-free batch is one
    /// commit rather than one plus one (ADR-119). Same rule as the wrapper:
    /// an origin only ever moves up.
    pub(crate) fn absorb_witnessed_in_txn(
        txn: &redb::WriteTransaction,
        seen: &kimmy_core::VersionVector,
    ) -> Result<()> {
        for (node, hlc) in seen.iter() {
            raise_version(txn, tables::OPLOG_WITNESSED, &Stamp::new(hlc, node))?;
        }
        Ok(())
    }

    /// Record that a stamp has been processed, whatever came of it.
    ///
    /// Durable, unlike [`Self::witness`], which only nudges the in-memory
    /// clock: a hole that survived a restart would restart the re-sync loop
    /// with it.
    pub fn witness_processed(&self, stamp: &Stamp) -> Result<()> {
        self.witness(stamp);
        let txn = self.begin_write()?;
        raise_version(&txn, tables::OPLOG_WITNESSED, stamp)?;
        txn.commit()?;
        Ok(())
    }

    /// The highest stamp in the oplog, used to resume the logical clock.
    ///
    /// Without this, a restart would mint stamps below ones already written,
    /// and a document updated after the restart could lose to its own older
    /// version under last-writer-wins.
    fn last_oplog_hlc(db: &Database) -> Result<Hlc> {
        let txn = db.begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        match oplog.last()? {
            Some((key, _)) => Ok(codec::decode_oplog_key(key.value())?.hlc),
            None => Ok(Hlc::ZERO),
        }
    }

    /// Mint the next stamp for a local write.
    pub(crate) fn next_stamp(&self) -> Stamp {
        let hlc = self.clock.lock().tick(physical_now_ms());
        Stamp::new(hlc, self.node_id)
    }

    /// Fold a stamp observed from a peer into the local clock.
    pub(crate) fn witness(&self, stamp: &Stamp) {
        self.clock.lock().witness(stamp.hlc);
    }

    pub(crate) fn db(&self) -> &Database {
        &self.db
    }

    /// Begin a counted write transaction.
    ///
    /// Every write an open engine performs goes through here rather than
    /// through [`Database::begin_write`] directly, so that [`Engine::commits`]
    /// counts the engine's durable commits rather than the paths somebody
    /// remembered to instrument — `commits_are_counted_at_one_chokepoint`
    /// fails if a new one appears. The three commits that legitimately do not
    /// pass through here all happen where there is no `Engine` yet to count
    /// them: opening the database, migrating it, and restoring a backup into a
    /// fresh file.
    pub(crate) fn begin_write(&self) -> Result<WriteTxn<'_>> {
        let holder = tracing::Span::current().metadata().map_or("none", |m| m.name());
        let budget = write_wait_budget();
        let waited_from = std::time::Instant::now();
        // Waiting for the writer is the other blocking step. The queue is
        // this engine's gate (ADR-151), which a caller can wait at for a
        // bounded time; redb's own lock behind it is then uncontended.
        let gate = blocking(|| match budget {
            Some(budget) => self.writer_gate.try_lock_for(budget),
            None => Some(self.writer_gate.lock()),
        });
        let waited = waited_from.elapsed();
        self.record_writer_wait(waited);
        let Some(gate) = gate else {
            self.writer_wait_timeouts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(
                waited_ms = waited.as_millis() as u64,
                caller = holder,
                "a write gave up waiting for the single writer; another transaction held it \
                 for the whole wait"
            );
            return Err(StorageError::WriterBusy { waited });
        };
        let mut txn = blocking(|| self.db.begin_write())?;
        let coalesced = self.coalescer.lock().is_some();
        if coalesced {
            // No persistent savepoints exist in this engine, so the one
            // reason redb refuses a reduced durability cannot apply.
            txn.set_durability(redb::Durability::None)
                .expect("no persistent savepoint was touched in a fresh transaction");
        }
        Ok(WriteTxn {
            txn: Some(txn),
            engine: self,
            coalesced,
            gate: Some(gate),
            held_from: std::time::Instant::now(),
            holder,
        })
    }

    /// The durability class this engine commits under (ADR-088).
    pub fn durability(&self) -> DurabilityClass {
        if self.coalescer.lock().is_some() {
            DurabilityClass::Coalesced
        } else {
            DurabilityClass::Durable
        }
    }

    /// Choose how commits become durable. `window` is the coalescing window
    /// and is ignored for `Durable`. Set once at startup; switching while
    /// writes are in flight is safe (each transaction reads the class when it
    /// opens) but pointless.
    pub fn set_durability(&self, class: DurabilityClass, window: std::time::Duration) {
        let mut coalescer = self.coalescer.lock();
        *coalescer = match class {
            DurabilityClass::Durable => None,
            DurabilityClass::Coalesced => Some(Coalescer::new(window)),
        };
    }

    /// Times the disk was asked to make something durable: one per commit
    /// under `durable`, one per barrier flush under `coalesced`.
    pub fn fsyncs(&self) -> u64 {
        self.fsyncs.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Commits made durable by a shared barrier flush rather than their own
    /// fsync (ADR-088). Zero under `durable`.
    pub fn grouped_commits(&self) -> u64 {
        self.grouped_commits.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Wait until a barrier flush that started after this commit has landed,
    /// running the flush if nobody else is.
    fn wait_for_flush(&self) -> std::result::Result<(), redb::CommitError> {
        let window;
        let my_ticket;
        {
            let mut guard = self.coalescer.lock();
            let Some(c) = guard.as_mut() else {
                // The class changed to `durable` between open and commit:
                // nothing will flush for us, so flush ourselves.
                drop(guard);
                return self.flush_now();
            };
            c.requested += 1;
            my_ticket = c.requested;
            window = c.window;
            if c.leader_running {
                // A leader is collecting. Its flush starts after its window,
                // which is after now, so it covers this ticket; if the class
                // is switched off meanwhile the wait ends with nothing left
                // to wait for.
                while guard.as_ref().is_some_and(|c| c.flushed < my_ticket) {
                    self.coalesce_woken.wait(&mut guard);
                }
                return Ok(());
            }
            c.leader_running = true;
        }

        // Leader: give the window to whoever is about to commit, then flush.
        std::thread::sleep(window);
        let result = self.flush_now();
        let mut guard = self.coalescer.lock();
        if let Some(c) = guard.as_mut() {
            if result.is_ok() {
                c.flushed = c.requested;
            }
            c.leader_running = false;
        }
        self.coalesce_woken.notify_all();
        result
    }

    /// One durable commit that also carries every earlier non-durable one to
    /// the disk. Writes a marker so the transaction is never empty — an
    /// empty commit is one redb could reasonably skip, and the point here is
    /// the fsync.
    fn flush_now(&self) -> std::result::Result<(), redb::CommitError> {
        // The leader's own transaction queues like any other (ADR-151); the
        // committer it flushes for released the gate before it began to wait.
        let _gate = blocking(|| self.writer_gate.lock());
        let mut txn = blocking(|| self.db.begin_write()).map_err(|e| {
            redb::CommitError::Storage(redb::StorageError::Io(std::io::Error::other(e.to_string())))
        })?;
        txn.set_durability(redb::Durability::Immediate).expect("Immediate is always permitted");
        {
            let mut meta = txn.open_table(tables::META).map_err(|e| {
                redb::CommitError::Storage(redb::StorageError::Io(std::io::Error::other(
                    e.to_string(),
                )))
            })?;
            let n = self.fsyncs.load(std::sync::atomic::Ordering::Relaxed);
            meta.insert("durability_flush", n.to_be_bytes().as_slice()).map_err(|e| {
                redb::CommitError::Storage(redb::StorageError::Io(std::io::Error::other(
                    e.to_string(),
                )))
            })?;
        }
        txn.commit()?;
        self.fsyncs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Publish committed events to live subscribers.
    ///
    /// Called only *after* a successful commit. Publishing before the commit
    /// would let a subscriber observe a change that then rolled back.
    pub(crate) fn publish(&self, entries: Vec<OplogEntry>) {
        for entry in entries {
            // An error here just means nobody is listening.
            let _ = self.events.send(Arc::new(entry));
        }
    }

    // -----------------------------------------------------------------------
    // Databases
    // -----------------------------------------------------------------------

    pub fn create_database(&self, name: &str) -> Result<DatabaseMeta> {
        CoreError::validate_name(name)?;
        let txn = self.begin_write()?;
        // Minted under the writer, as every stamp is (ADR-148).
        let stamp = self.next_stamp();
        let meta = DatabaseMeta { name: name.to_string(), created: stamp.hlc };
        {
            let mut dbs = txn.open_table(tables::DATABASES)?;
            if dbs.get(name)?.is_some() {
                // Creating an existing database is a no-op, matching Mongo's
                // implicit-creation feel rather than erroring.
                let existing = dbs.get(name)?.expect("checked above");
                let parsed: DatabaseMeta = serde_json::from_slice(existing.value())?;
                drop(existing);
                drop(dbs);
                txn.abort()?;
                return Ok(parsed);
            }
            dbs.insert(name, serde_json::to_vec(&meta)?.as_slice())?;
        }
        txn.commit()?;
        Ok(meta)
    }

    pub fn list_databases(&self) -> Result<Vec<DatabaseMeta>> {
        let txn = self.db.begin_read()?;
        let dbs = txn.open_table(tables::DATABASES)?;
        let mut out = Vec::new();
        for entry in dbs.iter()? {
            let (_, value) = entry?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    pub fn database_exists(&self, name: &str) -> Result<bool> {
        let txn = self.db.begin_read()?;
        let dbs = txn.open_table(tables::DATABASES)?;
        Ok(dbs.get(name)?.is_some())
    }

    /// Drop a database and every collection in it.
    ///
    /// Each drop is its own replicated entry, and the last one removes the
    /// database row on every member. A vector shadow goes with its parent in
    /// the parent's transaction; a shadow with no parent (ADR-138) is dropped
    /// on its own afterwards, so nothing in the database survives the answer.
    pub fn drop_database(&self, name: &str) -> Result<bool> {
        // Answer "did it exist" up front: dropping the last collection removes
        // the row, so the removal below finds nothing on the common path.
        let existed = self.database_exists(name)?;
        let collections = self.list_collections(name)?;
        for collection in &collections {
            // Shadows go with their parents, in the parent's transaction.
            if vector_meta::is_shadow(&collection.name) {
                continue;
            }
            self.drop_collection(name, &collection.name)?;
        }

        // What the parent loop leaves behind is a shadow with no parent to
        // take it: the residue ADR-138 describes, where a parent's drop was
        // applied before the shadow's create arrived. Skipped, it outlives
        // the database row and the `true` this answers, and the next create
        // in the database brings it back with its chunks. Listed again rather
        // than remembered from the first pass, so only what is still here is
        // named; `drop_collection_inner` takes a shadow name directly and
        // mints its own `DropCollection` entry for it, which is how a peer
        // holding the same orphan learns to drop its copy.
        for orphan in self.list_collections(name)? {
            self.drop_collection(name, &orphan.name)?;
        }

        // A database with no collections (the row exists, nothing else) still
        // has a row to remove.
        let txn = self.begin_write()?;
        {
            let mut dbs = txn.open_table(tables::DATABASES)?;
            dbs.remove(name)?;
        }
        txn.commit()?;
        Ok(existed)
    }

    // -----------------------------------------------------------------------
    // Collections
    // -----------------------------------------------------------------------

    pub fn create_collection(&self, db: &str, name: &str) -> Result<CollectionMeta> {
        CoreError::validate_name(db)?;
        CoreError::validate_name(name)?;
        self.create_collection_unchecked(db, name)
    }

    /// Create a collection whose name would fail user-facing validation.
    ///
    /// The `__` prefix is reserved precisely so that users cannot create
    /// collections that collide with internal ones — which means the internal
    /// ones have to be created through a path that skips that check.
    pub fn create_system_collection(&self, db: &str, name: &str) -> Result<CollectionMeta> {
        match self.get_collection(db, name) {
            Ok(existing) => Ok(existing),
            Err(StorageError::Core(CoreError::CollectionNotFound { .. })) => {
                self.create_collection_unchecked(db, name)
            }
            Err(e) => Err(e),
        }
    }

    fn create_collection_unchecked(&self, db: &str, name: &str) -> Result<CollectionMeta> {
        self.create_collection_inner(db, name, true, None)
    }

    /// `log = false` when applying a replicated creation. See
    /// `create_index_inner` for why a replicated change must not mint an entry.
    pub(crate) fn create_collection_inner(
        &self,
        db: &str,
        name: &str,
        log: bool,
        origin: Option<Hlc>,
    ) -> Result<CollectionMeta> {
        let txn = self.begin_write()?;
        // Minted *after* the writer is held, never before (ADR-148). A stamp
        // minted while another transaction holds the writer sorts below the
        // entries that transaction commits first, and a peer that reads this
        // node's vector and window in that interval witnesses past the stamp
        // without ever being served the entry it will belong to. Under the
        // writer, stamp order is commit order: the oplog this node serves is
        // contiguous for its own origin, which is what makes its advertised
        // vector a promise a peer can trust.
        let stamp = self.next_stamp();

        let meta = {
            let mut collections = txn.open_table(tables::COLLECTIONS)?;
            if collections.get((db, name))?.is_some() {
                drop(collections);
                txn.abort()?;
                return Err(CoreError::CollectionExists {
                    db: db.to_string(),
                    collection: name.to_string(),
                }
                .into());
            }

            // Derived, not allocated: every node computes the same id for the
            // same collection, so a replicated oplog entry addresses the same
            // collection everywhere. See `CollectionId::derive`.
            let id = CollectionId::derive(db, name);

            // If this creation follows a drop of the same id — a recreate —
            // the drop's stamp becomes the new incarnation's floor: replicated
            // entries stamped at or before it belong to the previous life and
            // must not enter the replacement, however their stamps sort
            // against the drop itself. A creation with no tombstone behind it
            // carries no floor: two nodes deriving the same id independently
            // is normal convergence, not reincarnation, and flooring there
            // would make whichever node created second silently discard the
            // first one's documents.
            let incarnation_floor = self.collection_dropped_at(id)?.map(|stamp| stamp.hlc);

            // The derivation is a 64-bit hash, so a collision is possible in
            // principle. Checked rather than trusted, because the failure would
            // be two unrelated collections quietly sharing storage — refusing
            // to create the second one is recoverable, merging them is not.
            let mut collision = None;
            for existing in collections.iter()? {
                let (key, value) = existing?;
                let other: CollectionMeta = serde_json::from_slice(value.value())?;
                if other.id == id {
                    let (other_db, other_name) = key.value();
                    collision = Some(format!("{other_db}.{other_name}"));
                    break;
                }
            }
            if let Some(other) = collision {
                drop(collections);
                txn.abort()?;
                return Err(StorageError::Corrupt(format!(
                    "collection id for {db}.{name} collides with {other}; rename one of them"
                )));
            }

            // `created` is the stamp of the create that produced this
            // incarnation *at its origin* — for a replicated create, the
            // entry's stamp rather than this node's clock at apply time. A
            // replayed drop is judged against it, and a local clock would
            // misjudge a legitimate drop stamped just before a late apply.
            let meta =
                CollectionMeta::new(id, db, name, origin.unwrap_or(stamp.hlc), incarnation_floor);
            collections.insert((db, name), serde_json::to_vec(&meta)?.as_slice())?;
            meta
        };

        // Databases are created implicitly by their first collection.
        {
            let mut dbs = txn.open_table(tables::DATABASES)?;
            if dbs.get(db)?.is_none() {
                let db_meta = DatabaseMeta { name: db.to_string(), created: stamp.hlc };
                dbs.insert(db, serde_json::to_vec(&db_meta)?.as_slice())?;
            }
        }

        let logged = if log {
            let entry = ddl_entry(
                stamp,
                OpKind::CreateCollection,
                meta.id,
                &kimmy_core::CollectionRef::new(db, name),
            )?;
            append_oplog(&txn, &entry)?;
            Some(entry)
        } else {
            None
        };

        txn.commit()?;
        if let Some(entry) = logged {
            self.publish(vec![entry]);
        }

        info!(db, collection = name, id = %meta.id, "created collection");
        Ok(meta)
    }

    pub fn get_collection(&self, db: &str, name: &str) -> Result<CollectionMeta> {
        let txn = self.db.begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        match collections.get((db, name))? {
            Some(v) => Ok(serde_json::from_slice(v.value())?),
            None => Err(CoreError::CollectionNotFound {
                db: db.to_string(),
                collection: name.to_string(),
            }
            .into()),
        }
    }

    pub fn list_collections(&self, db: &str) -> Result<Vec<CollectionMeta>> {
        let txn = self.db.begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        let mut out = Vec::new();
        // The database name leads the key, so one collection's entries form a
        // contiguous range.
        for entry in collections.range((db, "")..=(db, "\u{10FFFF}"))? {
            let (_, value) = entry?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Every collection this node holds, across every database, shadow
    /// collections included — one read transaction over the whole
    /// `collections` table, in key order (database, then name).
    ///
    /// The one catalogue walk. [`Self::all_collection_ids`],
    /// [`Self::live_collections`] and [`Self::collection_by_id`] are each a
    /// view over this rather than their own `list_databases` ×
    /// `list_collections` loop, which was a read transaction per database and
    /// had been written four times over. Metadata only: a JSON parse per
    /// collection and never a document, so it costs the same whether the
    /// collections are empty or full.
    pub fn all_collections(&self) -> Result<Vec<CollectionMeta>> {
        let txn = self.db.begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        let mut out = Vec::new();
        for entry in collections.iter()? {
            let (_, value) = entry?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// [`Self::all_collections`] with the ADR-138 rule applied, or not, as
    /// `paired_shadows` says. The **only** place the rule is written: the two
    /// public views that differ on it, [`Self::all_collection_ids`] and
    /// [`Self::live_collections`], differ by this one argument and nothing
    /// else.
    pub(crate) fn collections(&self, paired_shadows: PairedShadows) -> Result<Vec<CollectionMeta>> {
        let mut all = self.all_collections()?;
        if paired_shadows == PairedShadows::Hidden {
            // The parent is looked for by name within the same database, which
            // is where the shadow's own name is derived from.
            let hidden: std::collections::HashSet<CollectionId> = {
                let present: std::collections::HashSet<(&str, &str)> =
                    all.iter().map(|c| (c.db.as_str(), c.name.as_str())).collect();
                all.iter()
                    .filter(|c| {
                        kimmy_core::vector_meta::base_name(&c.name)
                            .is_some_and(|base| present.contains(&(c.db.as_str(), base)))
                    })
                    .map(|c| c.id)
                    .collect()
            };
            all.retain(|c| !hidden.contains(&c.id));
        }
        Ok(all)
    }

    /// Drop a collection along with all its documents and index entries.
    pub fn drop_collection(&self, db: &str, name: &str) -> Result<bool> {
        self.drop_collection_inner(db, name, None)
    }

    /// `replicated` carries the originating stamp when applying a peer's drop.
    ///
    /// It decides two things at once, and they have to agree: whether to log an
    /// entry of our own (a replicated change must not — see
    /// `create_index_inner`), and **which stamp the tombstone records**. Using a
    /// fresh local stamp for a replicated drop would put the tombstone ahead of
    /// a recreation that legitimately followed it, making the name permanently
    /// unusable on that node.
    pub(crate) fn drop_collection_inner(
        &self,
        db: &str,
        name: &str,
        replicated: Option<Stamp>,
    ) -> Result<bool> {
        let meta = match self.get_collection(db, name) {
            Ok(m) => m,
            Err(StorageError::Core(CoreError::CollectionNotFound { .. })) => return Ok(false),
            Err(e) => return Err(e),
        };

        // A vector-enabled collection keeps its vectors in a shadow collection,
        // which is an ordinary collection with its own id and so is not carried
        // away by removing this one. Left behind, its chunks outlive the
        // documents they describe — and because the shadow's name is derived
        // from this one's, a collection later created with the same name adopts
        // them. They are searchable: a document from the dropped collection
        // came back from `vector_search` scoring 1.0, above the new
        // collection's own documents, with an `_id` that resolves to nothing.
        //
        // Removed in the same transaction rather than by a second call, so
        // there is no instant in which the parent is gone and its vectors are
        // still answering queries.
        let shadow = (!vector_meta::is_shadow(name))
            .then(|| self.get_collection(db, &vector_meta::shadow_name(name)).ok())
            .flatten();

        let log = replicated.is_none();
        let txn = self.begin_write()?;
        // Under the writer, as `create_collection_inner` mints (ADR-148).
        let stamp = replicated.unwrap_or_else(|| self.next_stamp());
        let database_emptied = {
            let mut collections = txn.open_table(tables::COLLECTIONS)?;
            collections.remove((db, name))?;
            if let Some(shadow) = &shadow {
                collections.remove((db, shadow.name.as_str()))?;
            }
            collections.range((db, "")..=(db, "\u{10FFFF}"))?.next().is_none()
        };
        // A database exists while it has collections — creation is implicit
        // in the first one, so removal is implicit in the last. Decided here,
        // inside the drop, because drops replicate and database rows do not:
        // every member applies the same last drop and reaches the same
        // answer, where a separate "drop database" step would leave the empty
        // name listed on every peer that never heard it.
        if database_emptied {
            let mut dbs = txn.open_table(tables::DATABASES)?;
            dbs.remove(db)?;
        }
        {
            // Range-retain rather than collecting keys: a large collection
            // should not have to fit its key set in memory to be dropped.
            let mut docs = txn.open_table(tables::DOCS)?;
            docs.retain_in(doc_range(meta.id), |_, _| false)?;
            if let Some(shadow) = &shadow {
                docs.retain_in(doc_range(shadow.id), |_, _| false)?;
            }
        }
        {
            let mut indexes = txn.open_table(tables::INDEX_ENTRIES)?;
            indexes.retain_in(index_range(meta.id), |_, _| false)?;
            if let Some(shadow) = &shadow {
                indexes.retain_in(index_range(shadow.id), |_, _| false)?;
            }
        }
        {
            // Same transaction as the removal, so there is no instant in which
            // the collection is gone with no record that it was dropped.
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            // The shadow needs its own tombstone for the same reason the parent
            // does: without one, a peer still replaying pre-drop vector writes
            // would recreate it and repopulate the chunks this just removed.
            let ids = [Some(meta.id), shadow.as_ref().map(|s| s.id)];
            for id in ids.into_iter().flatten() {
                let newer = match dropped.get(id.0)? {
                    Some(existing) => stamp > codec::decode_oplog_key(existing.value())?,
                    None => true,
                };
                if newer {
                    dropped.insert(id.0, codec::oplog_key(&stamp).as_slice())?;
                }
            }
        }

        let logged = if log {
            let entry = ddl_entry(
                stamp,
                OpKind::DropCollection,
                meta.id,
                &kimmy_core::CollectionRef::new(db, name),
            )?;
            append_oplog(&txn, &entry)?;
            Some(entry)
        } else {
            None
        };

        txn.commit()?;
        if let Some(entry) = logged {
            self.publish(vec![entry]);
        }

        if let Some(shadow) = &shadow {
            info!(db, collection = name, shadow = %shadow.name, "dropped collection and its vectors");
        } else {
            info!(db, collection = name, "dropped collection");
        }
        Ok(true)
    }

    /// Persist a modified collection definition (used when adding an index).
    pub(crate) fn put_collection_meta(
        txn: &redb::WriteTransaction,
        meta: &CollectionMeta,
    ) -> Result<()> {
        let mut collections = txn.open_table(tables::COLLECTIONS)?;
        collections
            .insert((meta.db.as_str(), meta.name.as_str()), serde_json::to_vec(meta)?.as_slice())?;
        Ok(())
    }
}

/// Build a DDL oplog entry with a BSON-encoded payload.
///
/// Every schema change names its target by db and collection *name*, not only
/// by the entry's collection id: ids are derived from names by a hash, and a
/// hash cannot be inverted, so a peer meeting a collection for the first time
/// could not otherwise learn what to call it.
pub(crate) fn ddl_entry<T: serde::Serialize>(
    stamp: Stamp,
    kind: OpKind,
    collection: CollectionId,
    payload: &T,
) -> Result<OplogEntry> {
    Ok(OplogEntry {
        stamp,
        kind,
        collection,
        doc_id: None,
        body: Some(bson::serialize_to_vec(payload)?),
    })
}

fn decode_hlc(bytes: &[u8]) -> Result<Hlc> {
    let fixed: [u8; kimmy_core::HLC_ENCODED_LEN] = bytes
        .try_into()
        .map_err(|_| StorageError::Corrupt("version vector entry is not an Hlc".into()))?;
    Ok(Hlc::from_bytes(fixed))
}

fn decode_node(bytes: &[u8]) -> Result<NodeId> {
    let fixed: [u8; 16] = bytes
        .try_into()
        .map_err(|_| StorageError::Corrupt("version vector key is not a node id".into()))?;
    Ok(NodeId::from_bytes(fixed))
}

/// Append one oplog entry inside an existing transaction.
///
/// Always called in the same transaction as the change it describes, so the log
/// and the data can never disagree — there is no window in which a document is
/// updated but unlogged, or logged but not applied.
/// Raise one origin's entry in a version table, never lowering it.
///
/// Shared by both vectors, so they cannot drift in how they compare — and so a
/// vector can only ever move forward, which is the invariant that keeps a
/// rebuild from granting coverage the oplog never held.
pub(crate) fn raise_version(
    txn: &redb::WriteTransaction,
    table: redb::TableDefinition<&'static [u8], &'static [u8]>,
    stamp: &Stamp,
) -> Result<()> {
    let mut versions = txn.open_table(table)?;
    let node = stamp.node.to_bytes();
    let higher = match versions.get(node.as_slice())? {
        Some(current) => stamp.hlc > decode_hlc(current.value())?,
        None => true,
    };
    if higher {
        versions.insert(node.as_slice(), stamp.hlc.to_bytes().as_slice())?;
    }
    Ok(())
}

impl Engine {
    /// The entry this node holds under `stamp`, if any.
    ///
    /// A point lookup, for a caller that minted an entry a moment ago and
    /// wants to hand it to a peer directly — a schema change confirming
    /// itself on every live member (ADR-140) — rather than wait for
    /// anti-entropy to carry it.
    pub fn oplog_entry(&self, stamp: &Stamp) -> Result<Option<OplogEntry>> {
        let txn = self.db().begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        let key = codec::oplog_key(stamp);
        match oplog.get(key.as_slice())? {
            Some(raw) => Ok(Some(codec::decode_oplog_entry(raw.value())?)),
            None => Ok(None),
        }
    }
}

pub(crate) fn append_oplog(txn: &redb::WriteTransaction, entry: &OplogEntry) -> Result<()> {
    let key = codec::oplog_key(&entry.stamp);
    let mut oplog = txn.open_table(tables::OPLOG)?;
    let existed =
        oplog.insert(key.as_slice(), codec::encode_oplog_entry(entry).as_slice())?.is_some();

    // Re-appending an entry we already hold must not give it a second arrival
    // position. Peers resend overlapping ranges routinely, and a duplicate
    // arrival entry would deliver the same change twice to every stream.
    if existed {
        return Ok(());
    }

    // Same transaction as the entry, so the vector can never claim coverage of
    // something that was rolled back — a peer would then never be sent it.
    //
    // Both vectors: appending is also the strongest form of having seen it, so
    // witnessed stays at or above servable by construction (ADR-054).
    raise_version(txn, tables::OPLOG_VERSIONS, &entry.stamp)?;
    raise_version(txn, tables::OPLOG_WITNESSED, &entry.stamp)?;

    let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
    let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;

    // The counter lives in the index rather than in `meta` so that it cannot
    // drift from the thing it counts: rebuilding the index also rebuilds the
    // counter, and there is no third place for them to disagree.
    let next = arrival.last()?.map_or(0, |(seq, _)| seq.value() + 1);
    arrival.insert(next, key.as_slice())?;
    by_stamp.insert(key.as_slice(), next)?;
    Ok(())
}

/// Key range covering every document in a collection.
/// [`doc_range`], starting strictly after `after` when one is given.
///
/// Exclusive on purpose: a cursor names the last row already delivered, so
/// including it would hand the caller a duplicate at every page boundary.
pub(crate) fn doc_range_after(
    id: CollectionId,
    after: Option<&[u8]>,
) -> impl std::ops::RangeBounds<(u64, &[u8])> {
    use std::ops::Bound;
    let start = match after {
        Some(key) => Bound::Excluded((id.0, key)),
        None => Bound::Included((id.0, [].as_slice())),
    };
    let end = match id.0.checked_add(1) {
        Some(next) => Bound::Excluded((next, [].as_slice())),
        None => Bound::Unbounded,
    };
    (start, end)
}

pub(crate) fn doc_range(id: CollectionId) -> impl std::ops::RangeBounds<(u64, &'static [u8])> {
    use std::ops::Bound;
    let start = Bound::Included((id.0, [].as_slice()));
    let end = match id.0.checked_add(1) {
        Some(next) => Bound::Excluded((next, [].as_slice())),
        None => Bound::Unbounded,
    };
    (start, end)
}

/// Key range covering every index entry in a collection.
pub(crate) fn index_range(
    id: CollectionId,
) -> impl std::ops::RangeBounds<(u64, u32, &'static [u8], &'static [u8])> {
    use std::ops::Bound;
    let start = Bound::Included((id.0, 0u32, [].as_slice(), [].as_slice()));
    let end = match id.0.checked_add(1) {
        Some(next) => Bound::Excluded((next, 0u32, [].as_slice(), [].as_slice())),
        None => Bound::Unbounded,
    };
    (start, end)
}

/// Milliseconds since the Unix epoch.
///
/// The only place the storage layer reads the wall clock. `kimmy-core` takes
/// physical time as a parameter precisely so that this stays isolated and the
/// clock logic remains deterministically testable.
/// Milliseconds since the Unix epoch.
///
/// Public so that tests elsewhere in the workspace can express "far in the
/// future" against the same clock retention uses.
pub fn physical_now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    #[test]
    fn dropping_the_last_collection_removes_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let engine = super::Engine::open(&dir.path().join("k.redb")).unwrap();
        engine.create_collection("shop", "orders").unwrap();
        engine.create_collection("shop", "items").unwrap();
        let names = |e: &super::Engine| -> Vec<String> {
            e.list_databases().unwrap().into_iter().map(|d| d.name).collect()
        };
        assert!(names(&engine).contains(&"shop".to_string()));

        engine.drop_collection("shop", "orders").unwrap();
        assert!(names(&engine).contains(&"shop".to_string()), "one collection left");

        engine.drop_collection("shop", "items").unwrap();
        assert!(
            !names(&engine).contains(&"shop".to_string()),
            "the last drop removes the database"
        );
        assert!(!engine.database_exists("shop").unwrap());

        // And it comes back with its next collection, as any database does.
        engine.create_collection("shop", "again").unwrap();
        assert!(names(&engine).contains(&"shop".to_string()));
    }

    #[test]
    fn drop_database_drops_every_collection_and_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let engine = super::Engine::open(&dir.path().join("k.redb")).unwrap();
        engine.create_collection("shop", "orders").unwrap();
        engine.create_collection("shop", "items").unwrap();
        assert!(engine.drop_database("shop").unwrap());
        assert!(engine.list_collections("shop").unwrap().is_empty());
        assert!(!engine.database_exists("shop").unwrap());
        assert!(!engine.drop_database("shop").unwrap(), "already gone");
    }

    #[test]
    fn drop_database_drops_an_orphan_shadow_that_has_no_parent_to_take_it() {
        // A shadow beside its parent goes in the parent's transaction. An
        // orphan (ADR-138: the parent's drop applied before the shadow's
        // create arrived) has no parent to go with, so unless the drop names
        // it directly it survives the database it was in — still listed,
        // resurrected by the next create in the database, and adopted by a
        // vector-enabled collection recreated under its parent's name.
        let (engine, _dir) = engine();
        let orphan =
            engine.create_system_collection("shop", &vector_meta::shadow_name("docs")).unwrap();
        let config = kimmy_core::vector_meta::VectorConfig {
            fields: vec!["body".into()],
            provider: kimmy_core::ProviderConfig::Byo {},
            dim: 3,
            metric: Default::default(),
            document_prefix: None,
            query_prefix: None,
            chunk: Default::default(),
        };
        engine.create_collection("shop", "notes").unwrap();
        engine.configure_vectors("shop", "notes", config).unwrap();
        assert_eq!(engine.list_collections("shop").unwrap().len(), 3, "orphan, notes, its shadow");

        assert!(engine.drop_database("shop").unwrap());

        assert!(engine.list_collections("shop").unwrap().is_empty(), "the orphan survived");
        assert!(!engine.database_exists("shop").unwrap());
        // Its own replicated entry, naming the shadow: a peer holding the same
        // orphan drops it through the ordinary sync arm.
        let entries = engine.entries_for_peer(Hlc::ZERO, 100).unwrap().entries;
        assert!(
            entries.iter().any(|e| e.kind == OpKind::DropCollection && e.collection == orphan.id),
            "no DropCollection entry names the orphan shadow"
        );
        assert!(!engine.drop_database("shop").unwrap(), "already gone");
    }

    #[test]
    fn opens_with_a_bounded_cache() {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            super::Engine::open_with_cache(&dir.path().join("k.redb"), Some(8 << 20)).unwrap();
        let coll = engine.create_collection("app", "docs").unwrap();
        engine.insert(&coll, bson::doc! { "_id": 1 }).unwrap();
        assert_eq!(engine.count(&coll).unwrap(), 1);
    }

    use super::*;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        (engine, dir)
    }

    /// A counter is only worth reading if nothing can write behind its back.
    ///
    /// `Engine::commits` is a claim about the whole engine, and the way that
    /// claim goes quietly false is somebody reaching for `self.db()` in a new
    /// write path — which compiles, works, and undercounts. So the invariant is
    /// checked against the source rather than trusted: every `begin_write` in
    /// this crate is either `Engine::begin_write` or one of the three places
    /// that legitimately has no engine to count against.
    #[test]
    fn commits_are_counted_at_one_chokepoint() {
        // Where a commit happens before or outside an open `Engine`, and so
        // cannot be counted by one. Each is a whole-file exemption because each
        // file's entire job is one of these.
        const NO_ENGINE_YET: [&str; 3] = [
            "migrate.rs", // runs on the raw database during `Engine::open`
            "backup.rs",  // restores into a fresh file that no engine has opened
            "engine.rs",  // `Engine::open` itself, plus the chokepoint
        ];

        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = Vec::new();

        for entry in std::fs::read_dir(&src).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if path.extension().is_none_or(|e| e != "rs") || NO_ENGINE_YET.contains(&name.as_str())
            {
                continue;
            }

            let body = std::fs::read_to_string(&path).unwrap();
            for (n, line) in body.lines().enumerate() {
                // Tests reach for a raw database on purpose — to prove what an
                // engine does when the file underneath it was written by
                // something else.
                let raw = line.contains(".begin_write()") && !line.contains("self.begin_write()");
                if raw && !line.trim_start().starts_with("let txn = db.begin_write().unwrap()") {
                    offenders.push(format!("{name}:{}: {}", n + 1, line.trim()));
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "these write transactions bypass `Engine::begin_write`, so `Engine::commits` \
             undercounts them and every conclusion drawn from it is wrong:\n  {}",
            offenders.join("\n  ")
        );
    }

    /// The same invariant checked against the source, because the way it
    /// goes quietly false is a new write path minting its stamp where the
    /// value is convenient rather than where the writer is held (ADR-148).
    ///
    /// The shape, not the spacing: every function in this crate that both
    /// mints a stamp and opens a write transaction of its own must mint
    /// *after* it opens one. A function that takes a transaction its caller
    /// owns is exempt — the caller is already holding the writer, which is
    /// the whole point — and so is a function that never opens one, whose
    /// stamp goes nowhere near an entry. Whole files are scanned, including
    /// subdirectories, and each file is read only as far as its test module:
    /// a test may mint a stamp to build a fixture and open a transaction
    /// afterwards to check what it did, which is not this defect.
    #[test]
    fn no_write_path_mints_a_stamp_before_it_takes_the_writer() {
        /// Every `.rs` file under `dir`, at any depth.
        fn sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    sources(&path, out);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    out.push(path);
                }
            }
        }

        /// Each function in `body`, as (line number, signature, its text).
        ///
        /// A function runs from its `fn` line to the first line that closes
        /// it at the same indentation, which is what `rustfmt` guarantees
        /// and this crate is formatted by.
        fn functions(body: &str) -> Vec<(usize, String, String)> {
            let lines: Vec<&str> = body.lines().collect();
            let mut out = Vec::new();
            for (n, line) in lines.iter().enumerate() {
                let indent = line.len() - line.trim_start().len();
                let head = line.trim_start();
                let is_fn = [
                    "fn ",
                    "pub fn ",
                    "pub(crate) fn ",
                    "pub(super) fn ",
                    "pub unsafe fn ",
                    "unsafe fn ",
                    "async fn ",
                    "pub async fn ",
                    "pub(crate) async fn ",
                    "pub(super) async fn ",
                ]
                .iter()
                .any(|prefix| head.starts_with(prefix));
                if !is_fn {
                    continue;
                }
                let closing = format!("{}}}", " ".repeat(indent));
                let end = lines[n..]
                    .iter()
                    .position(|l| *l == closing)
                    .map_or(lines.len(), |offset| n + offset);
                // The signature may wrap over several lines; it ends at the
                // line holding the opening brace.
                let signature_end = lines[n..=end]
                    .iter()
                    .position(|l| l.trim_end().ends_with('{'))
                    .map_or(0, |offset| n + offset);
                out.push((n + 1, lines[n..=signature_end].join(" "), lines[n..=end].join("\n")));
            }
            out
        }

        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut paths = Vec::new();
        sources(&src, &mut paths);
        assert!(paths.len() > 10, "the scan of this crate's sources broke: {paths:?}");
        let mut offenders = Vec::new();

        for path in paths {
            let name = path.strip_prefix(&src).unwrap().to_string_lossy().to_string();
            let whole = std::fs::read_to_string(&path).unwrap();
            // Tests build fixtures; the invariant is about write paths. Cut
            // at the test *module*, not at any `#[cfg(test)]` — an earlier
            // one on a helper would hide every write path below it.
            let body = match whole.find("#[cfg(test)]\nmod tests") {
                Some(at) => &whole[..at],
                None => &whole[..],
            };
            for (line, signature, text) in functions(body) {
                // A *write* transaction the caller owns means the caller
                // holds the writer already, which is where those paths
                // mint. Named by type rather than by the parameter's name:
                // a read transaction called `txn` is not a writer, and
                // must not buy a write path an exemption.
                if signature.contains("txn: &WriteTxn")
                    || signature.contains("txn: &redb::WriteTransaction")
                {
                    continue;
                }
                let Some(mint) = text.find("next_stamp()") else { continue };
                let Some(writer) = text.find("begin_write()") else { continue };
                if mint < writer {
                    offenders.push(format!("{name}:{line}: {}", signature.trim()));
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "these paths mint a stamp before opening the transaction that will carry its \
             entry, so an entry can be committed below one a peer has already been served — \
             the hole ADR-148 closes. Mint after `begin_write`, or inside the caller's \
             transaction:\n  {}",
            offenders.join("\n  ")
        );
    }

    #[test]
    fn node_identity_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let first = Engine::open(&path).unwrap().node_id();
        let second = Engine::open(&path).unwrap().node_id();
        assert_eq!(first, second, "identity must live with the data");
    }

    /// The origin-side half of ADR-148's invariant: a stamp is minted only
    /// while this node holds the writer, so stamp order is commit order for
    /// its own origin and its advertised vector never names a stamp the
    /// committed oplog lacks below it.
    ///
    /// Before this, a schema change minted its stamp and *then* waited for
    /// the writer. Under a bulk load that wait is long, and every insert
    /// that committed meanwhile carried a higher stamp — so the change
    /// landed below entries a peer had already been served, behind a
    /// position that peer never asks about again. Here one thread holds
    /// the writer while another creates a collection; the creation must
    /// commit after the inserts and sort after them too.
    #[test]
    fn a_stamp_is_minted_only_under_the_writer() {
        use std::sync::mpsc::channel;

        let (engine, _dir) = engine();
        let engine = Arc::new(engine);
        let docs = engine.create_collection("app", "docs").unwrap();

        let (held_tx, held_rx) = channel();
        let (go_tx, go_rx) = channel();
        let writer = {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                let txn = engine.begin_write().unwrap();
                held_tx.send(()).unwrap();
                go_rx.recv().unwrap();
                let mut last = Hlc::ZERO;
                for i in 0..3 {
                    let (_, entry) =
                        engine.insert_in_txn(&txn, &docs, bson::doc! { "_id": i }).unwrap();
                    last = entry.stamp.hlc;
                }
                txn.commit().unwrap();
                last
            })
        };
        held_rx.recv().unwrap();

        // Asked for while the writer is held: it blocks until the inserts
        // commit, and must mint nothing until then.
        let creating = {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || engine.create_collection("app", "late").unwrap())
        };
        std::thread::sleep(std::time::Duration::from_millis(100));
        go_tx.send(()).unwrap();
        let last_insert = writer.join().unwrap();
        let late = creating.join().unwrap();

        assert!(
            late.created > last_insert,
            "the creation committed after the inserts and must sort after them; \
             {:?} is below {last_insert:?}, a stamp minted before the writer was held",
            late.created
        );
        // The same fact as the oplog states it: arrival order is stamp
        // order for this node's own entries.
        let arrived: Vec<_> =
            engine.read_arrival_from(0, 100).unwrap().iter().map(|e| e.stamp).collect();
        let mut sorted = arrived.clone();
        sorted.sort();
        assert_eq!(arrived, sorted, "an entry arrived below one already committed");
    }

    /// ADR-148 inside a scope, in the shape of
    /// `a_stamp_is_minted_only_under_the_writer`: every stamp a scope mints
    /// is minted after the writer is taken, so an entry committed while a
    /// scope waited for the writer sorts below everything the scope writes.
    /// Here one thread holds the writer and appends inserts under it while
    /// another opens a scope; the scope's entries must commit after those
    /// inserts and sort after them too.
    #[test]
    fn a_stamp_inside_a_scope_is_minted_under_the_writer() {
        use std::sync::mpsc::channel;

        let (engine, _dir) = engine();
        let engine = Arc::new(engine);
        let coll = engine.create_collection("app", "docs").unwrap();
        engine.insert(&coll, bson::doc! { "_id": "kept", "v": "before" }).unwrap();

        let (held_tx, held_rx) = channel();
        let (go_tx, go_rx) = channel();
        let holder = {
            let engine = Arc::clone(&engine);
            let coll = coll.clone();
            std::thread::spawn(move || {
                let txn = engine.begin_write().unwrap();
                held_tx.send(()).unwrap();
                go_rx.recv().unwrap();
                let mut last = Hlc::ZERO;
                for n in 0..3 {
                    let (_, entry) =
                        engine.insert_in_txn(&txn, &coll, bson::doc! { "_id": n }).unwrap();
                    last = entry.stamp.hlc;
                }
                txn.commit().unwrap();
                last
            })
        };
        held_rx.recv().unwrap();

        // Asked for while the writer is held: the scope blocks until the
        // inserts commit, and must mint nothing until then.
        let scoped = {
            let engine = Arc::clone(&engine);
            let coll = coll.clone();
            std::thread::spawn(move || {
                let mut rx = engine.subscribe();
                engine
                    .write_batch(|scope| {
                        scope.replace(
                            &coll,
                            &kimmy_core::DocId::String("kept".into()),
                            bson::doc! { "v": "after" },
                            false,
                        )?;
                        scope.replace(
                            &coll,
                            &kimmy_core::DocId::String("new".into()),
                            bson::doc! { "v": "after" },
                            true,
                        )?;
                        scope.delete(&coll, &kimmy_core::DocId::String("kept".into()))?;
                        Ok(())
                    })
                    .unwrap();
                let mut stamps = Vec::new();
                while let Ok(entry) = rx.try_recv() {
                    stamps.push(entry.stamp.hlc);
                }
                stamps
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(100));
        go_tx.send(()).unwrap();
        let last_insert = holder.join().unwrap();
        let stamps = scoped.join().unwrap();

        assert_eq!(stamps.len(), 3, "the scope published its three entries");
        for stamp in &stamps {
            assert!(
                *stamp > last_insert,
                "the scope committed after the inserts and must sort after them; {stamp:?} is \
                 below {last_insert:?}, a stamp minted before the writer was held"
            );
        }
        // The same fact as the oplog states it: arrival order is stamp
        // order for this node's own entries.
        let arrived: Vec<_> =
            engine.read_arrival_from(0, 100).unwrap().iter().map(|e| e.stamp).collect();
        let mut sorted = arrived.clone();
        sorted.sort();
        assert_eq!(arrived, sorted, "an entry arrived below one already committed");
    }

    #[test]
    fn stamps_are_strictly_increasing() {
        let (engine, _dir) = engine();
        let mut previous = Stamp::new(Hlc::ZERO, engine.node_id());
        for _ in 0..1000 {
            let next = engine.next_stamp();
            assert!(next > previous, "{next:?} did not exceed {previous:?}");
            previous = next;
        }
    }

    #[test]
    fn the_clock_resumes_above_the_oplog_tail_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let last = {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("app", "orders").unwrap();
            engine.clock.lock().last()
        };

        // A restart must not mint stamps below what is already on disk, or a
        // document rewritten after the restart could lose to its own older
        // version under last-writer-wins.
        let reopened = Engine::open(&path).unwrap();
        assert!(reopened.next_stamp().hlc > last);
    }

    #[test]
    fn creating_a_collection_creates_its_database() {
        let (engine, _dir) = engine();
        engine.create_collection("app", "orders").unwrap();
        assert!(engine.database_exists("app").unwrap());
        assert_eq!(engine.list_databases().unwrap().len(), 1);
    }

    #[test]
    fn duplicate_collections_are_rejected() {
        let (engine, _dir) = engine();
        engine.create_collection("app", "orders").unwrap();
        assert!(matches!(
            engine.create_collection("app", "orders"),
            Err(StorageError::Core(CoreError::CollectionExists { .. }))
        ));
    }

    #[test]
    fn distinct_collections_get_distinct_ids() {
        let (engine, _dir) = engine();
        let a = engine.create_collection("app", "a").unwrap().id;
        let b = engine.create_collection("app", "b").unwrap().id;
        assert_ne!(a, b);
    }

    #[test]
    fn recreating_a_collection_reuses_its_id_but_not_its_data() {
        // Ids are derived from the name, so a recreated collection necessarily
        // gets the same id — "same name means same id on every node" and
        // "recreating yields a fresh id" cannot both hold.
        //
        // That makes purging on drop load-bearing rather than merely tidy: a
        // surviving document or index entry would be inherited by the new
        // collection.
        let (engine, _dir) = engine();
        let coll = engine.create_collection("app", "a").unwrap();
        engine.insert(&coll, bson::doc! { "_id": 1, "v": "old" }).unwrap();

        engine.drop_collection("app", "a").unwrap();
        let recreated = engine.create_collection("app", "a").unwrap();

        assert_eq!(recreated.id, coll.id, "a derived id is stable across drop and recreate");
        assert_eq!(engine.count(&recreated).unwrap(), 0, "the dropped data must not be inherited");
        assert!(engine.get(&recreated, &kimmy_core::DocId::Int64(1)).unwrap().is_none());
    }

    #[test]
    fn two_nodes_agree_on_a_collection_id_whatever_the_creation_order() {
        // The reason ids are derived at all. A counter makes this depend on
        // creation order, so a replicated write would land in whichever
        // collection happened to hold that number locally.
        let a_dir = tempfile::tempdir().unwrap();
        let b_dir = tempfile::tempdir().unwrap();
        let a = Engine::open(&a_dir.path().join("kimmy.redb")).unwrap();
        let b = Engine::open(&b_dir.path().join("kimmy.redb")).unwrap();

        let a_orders = a.create_collection("shop", "orders").unwrap().id;
        a.create_collection("shop", "customers").unwrap();

        // Deliberately the opposite order on the second node.
        b.create_collection("shop", "customers").unwrap();
        let b_orders = b.create_collection("shop", "orders").unwrap().id;

        assert_eq!(a_orders, b_orders);
    }

    #[test]
    fn collection_ids_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let first = Engine::open(&path).unwrap().create_collection("app", "a").unwrap().id;
        let second = Engine::open(&path).unwrap().create_collection("app", "b").unwrap().id;
        assert_ne!(first, second, "the id counter must be persistent");
    }

    #[test]
    fn listing_collections_is_scoped_to_one_database() {
        let (engine, _dir) = engine();
        engine.create_collection("app", "orders").unwrap();
        engine.create_collection("app", "users").unwrap();
        engine.create_collection("other", "orders").unwrap();

        let mut names: Vec<_> =
            engine.list_collections("app").unwrap().into_iter().map(|c| c.name).collect();
        names.sort();
        assert_eq!(names, ["orders", "users"]);
        assert_eq!(engine.list_collections("other").unwrap().len(), 1);
        assert!(engine.list_collections("missing").unwrap().is_empty());
    }

    #[test]
    fn getting_a_missing_collection_is_an_error() {
        let (engine, _dir) = engine();
        assert!(matches!(
            engine.get_collection("app", "nope"),
            Err(StorageError::Core(CoreError::CollectionNotFound { .. }))
        ));
    }

    #[test]
    fn dropping_a_missing_collection_reports_false_rather_than_erroring() {
        let (engine, _dir) = engine();
        assert!(!engine.drop_collection("app", "nope").unwrap());
    }

    #[test]
    fn dropping_a_database_removes_its_collections() {
        let (engine, _dir) = engine();
        engine.create_collection("app", "a").unwrap();
        engine.create_collection("app", "b").unwrap();
        engine.create_collection("keep", "c").unwrap();

        assert!(engine.drop_database("app").unwrap());
        assert!(engine.list_collections("app").unwrap().is_empty());
        assert!(!engine.database_exists("app").unwrap());
        // An unrelated database must be untouched.
        assert_eq!(engine.list_collections("keep").unwrap().len(), 1);
    }

    #[test]
    fn invalid_names_are_rejected() {
        let (engine, _dir) = engine();
        assert!(engine.create_collection("app", "__system").is_err());
        assert!(engine.create_collection("app", "with/slash").is_err());
        assert!(engine.create_collection("", "x").is_err());
    }

    #[test]
    fn collection_operations_are_logged_to_the_oplog() {
        let (engine, _dir) = engine();
        let mut rx = engine.subscribe();
        let meta = engine.create_collection("app", "orders").unwrap();

        let event = rx.try_recv().expect("a create should publish an event");
        assert_eq!(event.kind, OpKind::CreateCollection);
        assert_eq!(event.collection, meta.id);
    }

    #[test]
    fn a_mismatched_format_version_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let _ = Engine::open(&path).unwrap();
        }

        // Simulate a data directory written by an incompatible build.
        {
            let db = Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut meta = txn.open_table(tables::META).unwrap();
                meta.insert(tables::META_FORMAT_VERSION, [99u8].as_slice()).unwrap();
            }
            txn.commit().unwrap();
        }

        assert!(
            matches!(Engine::open(&path), Err(StorageError::UnsupportedFormat { found: 99, .. })),
            "opening must refuse rather than misread the records"
        );
    }

    #[test]
    fn coalesced_commits_share_fsyncs_and_are_readable_after_reopen() {
        // ADR-088: N concurrent writers, N commits, fewer than N fsyncs —
        // and every document is on disk when the engine is reopened.
        use bson::doc;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let engine = Arc::new(Engine::open(&path).unwrap());
        engine.set_durability(DurabilityClass::Coalesced, std::time::Duration::from_millis(5));
        assert_eq!(engine.durability(), DurabilityClass::Coalesced);
        let coll = engine.create_collection("app", "c").unwrap();
        let fsyncs_before = engine.fsyncs();
        let commits_before = engine.commits();
        let grouped_before = engine.grouped_commits();

        let mut handles = Vec::new();
        for w in 0..8i64 {
            let engine = Arc::clone(&engine);
            let coll = coll.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..20i64 {
                    engine.insert(&coll, doc! {"_id": w * 100 + i, "w": w}).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let commits = engine.commits() - commits_before;
        let fsyncs = engine.fsyncs() - fsyncs_before;
        assert_eq!(commits, 160, "one commit per insert, still");
        assert_eq!(
            engine.grouped_commits() - grouped_before,
            160,
            "every one of them went through the barrier"
        );
        assert!(
            fsyncs < commits,
            "the barrier must have grouped some: {fsyncs} fsyncs for {commits} commits"
        );
        assert!(fsyncs >= 1);

        drop(engine);
        let reopened = Engine::open(&path).unwrap();
        let coll = reopened.get_collection("app", "c").unwrap();
        assert_eq!(reopened.count(&coll).unwrap(), 160, "durable when the call returned");
    }

    #[test]
    fn durable_is_the_default_and_pays_one_fsync_per_commit() {
        use bson::doc;
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        assert_eq!(engine.durability(), DurabilityClass::Durable);
        let coll = engine.create_collection("app", "c").unwrap();
        let (c0, f0) = (engine.commits(), engine.fsyncs());
        for i in 0..5i64 {
            engine.insert(&coll, doc! {"_id": i}).unwrap();
        }
        assert_eq!(engine.commits() - c0, 5);
        assert_eq!(engine.fsyncs() - f0, 5, "under durable every commit is its own fsync");
        assert_eq!(engine.grouped_commits(), 0);

        assert_eq!(DurabilityClass::parse("coalesced"), Some(DurabilityClass::Coalesced));
        assert_eq!(DurabilityClass::parse("fast"), None, "there is no such class");
        assert_eq!(DurabilityClass::Durable.as_str(), "durable");
    }

    /// The bounded wait (ADR-151): a caller with a budget gives up inside
    /// it and writes nothing; one without waits for however long the writer
    /// is held. Both are on a runtime, because the budget is a task-local
    /// the request path sets and the engine reads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_with_a_budget_gives_up_while_the_writer_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let meta = engine.create_collection("db", "c").unwrap();

        let waits_before = engine.writer_wait().count;
        let hold = engine.hold_writer();
        let budget = std::time::Duration::from_millis(100);
        let refused = {
            let engine = Arc::clone(&engine);
            let meta = meta.clone();
            tokio::spawn(with_write_wait_budget(budget, async move {
                engine.insert(&meta, bson::doc! { "n": 1 })
            }))
            .await
            .unwrap()
        };
        assert!(
            matches!(refused, Err(StorageError::WriterBusy { waited }) if waited >= budget),
            "expected WriterBusy after the budget, got {refused:?}"
        );
        assert_eq!(engine.writer_wait_timeouts(), 1);
        assert_eq!(engine.writer_wait().count, waits_before + 1, "a refused wait is still a wait");
        assert_eq!(engine.count(&meta).unwrap(), 0, "nothing was written");

        // Released after a while: a caller with no budget outlasts it.
        let release = {
            let engine = Arc::clone(&engine);
            let meta = meta.clone();
            tokio::spawn(async move { engine.insert(&meta, bson::doc! { "n": 2 }) })
        };
        std::thread::sleep(std::time::Duration::from_millis(150));
        drop(hold);
        release.await.unwrap().expect("a write with no budget waits out the hold");
        assert_eq!(engine.count(&meta).unwrap(), 1);
        assert_eq!(engine.writer_wait_timeouts(), 1, "the unbounded wait did not time out");
        assert!(
            engine.writer_hold_max() >= std::time::Duration::from_millis(150),
            "the hold was measured: {:?}",
            engine.writer_hold_max()
        );
    }

    /// A budget bounds the wait, not the transaction: a write that gets the
    /// writer inside its budget proceeds however long its own work takes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_budget_only_bounds_the_wait() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let meta = engine.create_collection("db", "c").unwrap();
        let written =
            tokio::spawn(with_write_wait_budget(std::time::Duration::from_millis(1), async move {
                engine.insert(&meta, bson::doc! { "n": 1 })
            }))
            .await
            .unwrap();
        assert!(written.is_ok(), "{written:?}");
    }
}
