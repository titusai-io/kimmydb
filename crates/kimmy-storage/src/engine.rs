//! The storage engine.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kimmy_core::{
    CollectionId, Error as CoreError, Hlc, HlcClock, NodeId, OpKind, OplogEntry, Stamp, vector_meta,
};
use parking_lot::{Condvar, Mutex};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata};
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
    /// Entries held as state that a window released, since start (ADR-169's
    /// addendum). Added only after the run that released them commits.
    held_marks_released: std::sync::atomic::AtomicU64,
    /// The longest any one transaction has held the writer, since start.
    writer_hold_max_us: std::sync::atomic::AtomicU64,
    /// How long the writer was held, as a histogram over
    /// [`WRITER_HOLD_BUCKETS_US`] **per holder** (ADR-159); `count` and
    /// `sum` beside it, also per holder. The maximum above says how bad the
    /// worst hold was; this says what was holding it.
    writer_hold_buckets:
        [[std::sync::atomic::AtomicU64; WRITER_HOLD_BUCKETS_US.len()]; WriterHolder::COUNT],
    writer_hold_count: [std::sync::atomic::AtomicU64; WriterHolder::COUNT],
    writer_hold_sum_us: [std::sync::atomic::AtomicU64; WriterHolder::COUNT],
    /// What the holds above were made of, per holder (ADR-176).
    hold_counters: crate::hold_meter::HoldCounters,
    /// What serving peers' windows cost this node (ADR-176).
    serve_counters: crate::hold_meter::ServeCounters,
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

/// Rows a drop removes per write transaction while it clears what a collection
/// held (ADR-158).
///
/// Each chunk is one commit and the writer is released between chunks, so a
/// drop of any size holds it for one chunk's removal at a time. A thousand,
/// which is what the retention pass removes per commit and what a `multi: true`
/// write commits at a time (ADR-151, ADR-086): the same bound on the same
/// writer, and one number an operator can hold in their head beats three that
/// each need their own explanation. Rows rather than bytes, because the purge
/// removes by key and never materialises a document, so a byte bound would mean
/// reading values the work does not otherwise need.
pub const DROP_PURGE_CHUNK: usize = 1_000;

/// A transaction that held the writer longer than this is logged at WARN,
/// naming its [`WriterHolder`], when it lets go. Five seconds is more than a
/// bulk of ten thousand documents costs and a small fraction of the request
/// timeout a client write is waiting under.
pub const WRITER_HOLD_WARN: std::time::Duration = std::time::Duration::from_secs(5);

/// Upper bounds of the writer-hold histogram, in microseconds (ADR-159).
///
/// A hold is not a wait, and the two do not want the same table. One
/// millisecond is a transaction that wrote nothing and aborted, which costs
/// no fsync; ten is an ordinary durable commit, measured at ~3.4 ms. A
/// hundred milliseconds and a second are where a batch sits — a bulk, a
/// sync run, a chunk of a retention removal. Five seconds is
/// [`WRITER_HOLD_WARN`], so the bucket and the log line agree and a count
/// can be reconciled against the lines. Thirty is the request timeout's
/// default: above it every client write that queued behind the hold has
/// already been refused. Five minutes separates a bad hold from a write
/// outage — the retention passes ADR-151 measured ran ten to twelve.
pub const WRITER_HOLD_BUCKETS_US: [u64; 7] =
    [1_000, 10_000, 100_000, 1_000_000, 5_000_000, 30_000_000, 300_000_000];

/// What kind of work a transaction holding the single writer is doing
/// (ADR-159).
///
/// Every path that takes the writer names one where it opens its
/// transaction, so the hold histogram can say *what* held it and not only
/// for how long. The set is closed and small because it is a metric label,
/// and it names **the work rather than who asked for it**: a collection
/// drop costs the same hold whether a client issued it or a peer's entry
/// carried it, and it is the drop an operator is hunting. The two holders
/// that exist only because of replication — a batch of a peer's entries and
/// a page of its snapshot — are named for that, because there is nothing
/// else they are.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WriterHolder {
    /// One document, from a client: an insert, a replace, an update, a
    /// delete, a find-and-modify.
    Write,
    /// Many documents in one transaction: a bulk insert, a chunk of a
    /// multi-document update, a scoped batch.
    Bulk,
    /// A schema change that writes metadata alone — a database or
    /// collection created, a vector configuration settled, a tombstone
    /// recorded. Costs the same whatever the collection holds.
    Ddl,
    /// Creating an index: every document of the collection is read and
    /// filed under the new definition in the transaction that creates it.
    IndexBuild,
    /// The destructive half of a drop: one chunk of a collection's purge
    /// (ADR-158), or an index drop, which is still one transaction. **Not**
    /// the burial that precedes a collection's purge — that writes metadata
    /// alone and is [`WriterHolder::Ddl`]. Also what a creation pays when it
    /// finishes the residue of an earlier drop, and what the sweep at
    /// [`Engine::open`] pays: the holder names the work, not who asked.
    Drop,
    /// Applying a peer's entries — one run of an anti-entropy batch
    /// (ADR-119), or the entry a replicated schema change is recorded from.
    Replication,
    /// Applying a page of a peer's snapshot, repairing a divergence
    /// (ADR-152).
    Repair,
    /// The retention pass, removing what its scans already found (ADR-151).
    /// Never the scans themselves, which take no writer at all.
    Retention,
    /// A TTL index's delete.
    Expiry,
    /// The embedding worker: vectors written, its position checkpointed.
    Embedding,
    /// The shared fsync of the coalescing barrier (ADR-088) — the one hold
    /// that is an fsync and nothing else, and the one that was invisible
    /// before this histogram, because it takes the gate without opening a
    /// counted transaction.
    Durability,
    /// Rewinding the database to a point in time. Reads zero on any node
    /// that serves: it runs only under `kimmyd restore --until`, which
    /// exits before anything can scrape it. Here so that the set is the
    /// whole set of what takes the writer, rather than the part of it a
    /// scrape happens to see.
    Rewind,
}

impl WriterHolder {
    /// Every holder, in the order the histogram renders them, which is the
    /// order they are declared in.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Write,
        Self::Bulk,
        Self::Ddl,
        Self::IndexBuild,
        Self::Drop,
        Self::Replication,
        Self::Repair,
        Self::Retention,
        Self::Expiry,
        Self::Embedding,
        Self::Durability,
        Self::Rewind,
    ];

    /// How many there are; the width of every per-holder array.
    pub const COUNT: usize = 12;

    /// The word a metric label and a log line name this holder by.
    ///
    /// What an operator already reads in the logs and the operations guide,
    /// never the name of a function: `retention` is the pass ADR-151
    /// describes, `repair` is the round `kimmy_sync_repair_rounds_total`
    /// counts.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::Bulk => "bulk",
            Self::Ddl => "ddl",
            Self::IndexBuild => "index_build",
            Self::Drop => "drop",
            Self::Replication => "replication",
            Self::Repair => "repair",
            Self::Retention => "retention",
            Self::Expiry => "expiry",
            Self::Embedding => "embedding",
            Self::Durability => "durability",
            Self::Rewind => "rewind",
        }
    }

    /// Its row in the per-holder arrays; `ALL[h.slot()] == h`.
    pub const fn slot(self) -> usize {
        self as usize
    }
}

/// The writer-hold histogram, as a scrape reads it (ADR-159).
///
/// One row per [`WriterHolder`], in [`WriterHolder::ALL`] order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriterHoldSnapshot {
    /// Holds in each bucket of [`WRITER_HOLD_BUCKETS_US`], **not**
    /// cumulative, per holder.
    pub buckets: [[u64; WRITER_HOLD_BUCKETS_US.len()]; WriterHolder::COUNT],
    /// Holds per holder, which is the histogram's `+Inf` bucket.
    pub count: [u64; WriterHolder::COUNT],
    /// Microseconds the writer was held by each holder, since start.
    pub sum_us: [u64; WriterHolder::COUNT],
}

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
    /// `None` once let go, which happens *before* the hold is recorded —
    /// the order [`WriteTxn::release`] takes and states its reason for.
    gate: Option<parking_lot::MutexGuard<'a, ()>>,
    engine: &'a Engine,
    held_from: std::time::Instant,
    holder: WriterHolder,
    /// What the hold is made of (ADR-176), metered from `held_from`.
    meter: Option<crate::hold_meter::Scope>,
    /// When the hold's commit began, if it made one: the end of its `work`
    /// phase. A hold that commits nothing is all `work`.
    commit_from: Option<std::time::Instant>,
}

impl WriterHold<'_> {
    fn new<'a>(
        engine: &'a Engine,
        gate: parking_lot::MutexGuard<'a, ()>,
        holder: WriterHolder,
    ) -> WriterHold<'a> {
        WriterHold {
            gate: Some(gate),
            engine,
            held_from: std::time::Instant::now(),
            holder,
            meter: Some(crate::hold_meter::Scope::hold()),
            commit_from: None,
        }
    }
}

impl Drop for WriterHold<'_> {
    fn drop(&mut self) {
        // Let go first, then record, as `WriteTxn::release` does. What
        // follows the release is a handful of atomics and, past
        // `WRITER_HOLD_WARN`, a log call; small, but it is not work the
        // next writer in the queue should be waiting through, and two
        // paths that release the same gate should not do it in two orders.
        if self.gate.take().is_some() {
            let released = std::time::Instant::now();
            let from = self.held_from;
            let commit_from = self.commit_from.unwrap_or(released);
            self.engine.record_writer_hold(
                released - from,
                self.holder,
                self.meter.take(),
                [commit_from - from, std::time::Duration::ZERO, released - commit_from],
            );
        }
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

thread_local! {
    /// What [`metered_writer_wait`] has gathered on this thread so far, or
    /// `None` outside one.
    static WRITER_WAIT_METER: std::cell::Cell<Option<std::time::Duration>> =
        const { std::cell::Cell::new(None) };
}

/// Run `f` and say how long it spent waiting for the single writer, across
/// every time it took it (ADR-175).
///
/// The engine-wide wait histogram (ADR-151) cannot answer that for one
/// caller: every writer on the node lands in it, so a replicated batch that
/// queued behind a client's bulk and a client's bulk that queued behind the
/// batch are the same observation. A thread-local rather than a field on the
/// transaction, because a batch takes the writer from more places than one —
/// each run, each schema change it records, and the shared flush a coalesced
/// commit waits on — and every one of them waits at the same gate. The storage
/// work under `f` is synchronous, and [`blocking`] runs its closure on the
/// thread it was called from, so every wait `f` makes happens on this thread.
///
/// Nested calls each see their own waits, and the outer one sees the inner
/// one's too.
pub fn metered_writer_wait<T>(f: impl FnOnce() -> T) -> (T, std::time::Duration) {
    /// Puts the enclosing meter back, with this one's waits added, however
    /// `f` ends — a panic unwinding through it included, so a thread that
    /// survives the panic does not go on metering into a scope that is gone.
    struct Restore(Option<std::time::Duration>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let inner = WRITER_WAIT_METER.with(|m| m.get()).unwrap_or_default();
            WRITER_WAIT_METER.with(|m| m.set(self.0.map(|outer| outer + inner)));
        }
    }

    let restore = Restore(WRITER_WAIT_METER.with(|m| m.replace(Some(std::time::Duration::ZERO))));
    let value = f();
    let waited = WRITER_WAIT_METER.with(|m| m.get()).unwrap_or_default();
    drop(restore);
    (value, waited)
}

/// Add a wait for the writer to the meter this thread is running under, if
/// any.
fn meter_writer_wait(waited: std::time::Duration) {
    WRITER_WAIT_METER.with(|m| {
        if let Some(sofar) = m.get() {
            m.set(Some(sofar + waited));
        }
    });
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
    /// What the hold is made of (ADR-176), metered from `held_from` to the
    /// release.
    meter: Option<crate::hold_meter::Scope>,
    /// Where the hold's phases end (ADR-176): the commit asked for, and the
    /// live counts flushed. Unset for a transaction that never committed,
    /// whose whole hold is `work`.
    commit_from: Option<std::time::Instant>,
    counted_at: Option<std::time::Instant>,
    /// What this transaction is doing, declared by the path that opened it
    /// (ADR-159), so a long hold names its cause and the hold histogram can
    /// be split by it.
    holder: WriterHolder,
    /// What this transaction owes the live-count tables, gathered as it writes
    /// and written once by [`Self::commit`] before the inner commit (ADR-174).
    ///
    /// Here rather than on each write path because the commit is the one point
    /// every path passes through, which is what keeps the count and the mark
    /// in the same transaction as the records that moved them — without every
    /// path having to remember. Behind a mutex because the write paths hold a
    /// shared reference to the transaction, not an exclusive one; it is never
    /// contended, since the writer gate means one thread owns this.
    live_counts: parking_lot::Mutex<crate::live_count::Pending>,
}

impl WriteTxn<'_> {
    /// Let go of the writer and record how long it was held.
    fn release(&mut self) {
        if self.gate.take().is_some() {
            let released = std::time::Instant::now();
            let from = self.held_from;
            let commit_from = self.commit_from.unwrap_or(released);
            let counted_at = self.counted_at.unwrap_or(commit_from);
            self.engine.record_writer_hold(
                released - from,
                self.holder,
                self.meter.take(),
                [commit_from - from, counted_at - commit_from, released - counted_at],
            );
        }
    }

    /// What this transaction owes the live-count tables, for the write paths
    /// to add to as they go.
    pub(crate) fn live_counts(&self) -> &parking_lot::Mutex<crate::live_count::Pending> {
        &self.live_counts
    }

    pub(crate) fn commit(mut self) -> Result<()> {
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
        #[cfg(test)]
        crate::hold_meter::test_hooks::at_phase(crate::hold_meter::Phase::Work);
        self.commit_from = Some(std::time::Instant::now());
        #[cfg(test)]
        crate::hold_meter::test_hooks::at_phase(crate::hold_meter::Phase::Counts);
        let txn = self.txn.take().expect("a transaction is taken once");
        // The live counts and their mark, once for the whole transaction and
        // inside it (ADR-174's addendum). Before the inner commit, so this is
        // the same commit as the records that moved them and a failure here
        // fails the write rather than leaving a landed batch miscounted.
        crate::live_count::flush(&txn, &self.live_counts.lock())?;
        self.counted_at = Some(std::time::Instant::now());
        #[cfg(test)]
        crate::hold_meter::test_hooks::at_phase(crate::hold_meter::Phase::Commit);
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
        // Through a backend that meters what it is asked for, so a hold can
        // say how much of it was the disk (ADR-176). Opened exactly as
        // `Builder::create` opens it.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let backend =
            crate::hold_meter::MeteredBackend::new(redb::backends::FileBackend::new(file)?);
        let mut builder = Database::builder();
        if let Some(bytes) = cache_bytes {
            builder.set_cache_size(bytes);
        }
        let db = builder.create_with_backend(backend)?;

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
            let _ = txn.open_table(tables::OPLOG_HELD)?;
            let _ = txn.open_table(tables::LIVE_COUNTS)?;
            let _ = txn.open_table(tables::LIVE_COUNTS_THROUGH)?;
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
        // After the arrival index, whose end the counts' mark is compared
        // against: a rebuilt index renumbers positions, and the counts are
        // rebuilt with it (ADR-174).
        {
            let started = std::time::Instant::now();
            let txn = db.begin_write()?;
            match crate::live_count::rebuild_if_stale(&txn)? {
                Some(rebuilt) => {
                    txn.commit()?;
                    // Before the node serves anything: this function has not
                    // returned. On a large store with a cold page cache the
                    // walk runs at the disk's speed.
                    info!(
                        collections = rebuilt.collections,
                        records = rebuilt.records,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        previous_mark = ?rebuilt.previous_mark,
                        arrival = rebuilt.arrival,
                        "rebuilt the live document counts before serving"
                    );
                }
                None => txn.abort()?,
            }
        }

        // A Cargo.toml comment is the only thing standing between the benchmark
        // control and a person, which is not enough. Such a build keeps no live
        // counts: nothing here is corrupted on disk — the mark is never written,
        // so a later ordinary start rebuilds — but *this* process serves an
        // empty count table, and the divergence check reads that as a
        // divergence on every contact.
        #[cfg(feature = "bench-no-live-counts")]
        tracing::error!(
            "this build was compiled with `bench-no-live-counts`: it does not keep the live \
             document counts, and the cross-member divergence check will report differences \
             that do not exist. It is a benchmark control and must never serve traffic."
        );

        let node_id = Self::load_or_create_node_id(&db)?;
        let resumed = Self::last_oplog_hlc(&db)?;

        if resumed != Hlc::ZERO {
            debug!(hlc = %resumed, "resumed logical clock from the oplog tail");
        }

        let (events, _) = broadcast::channel(EVENT_BUFFER);

        info!(node = %node_id, path = %path.display(), "storage engine open");

        let engine = Self {
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
            held_marks_released: std::sync::atomic::AtomicU64::new(0),
            writer_hold_max_us: std::sync::atomic::AtomicU64::new(0),
            writer_hold_buckets: std::array::from_fn(|_| {
                std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0))
            }),
            writer_hold_count: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            writer_hold_sum_us: std::array::from_fn(|_| std::sync::atomic::AtomicU64::new(0)),
            hold_counters: Default::default(),
            serve_counters: Default::default(),
            gc_scan_cursor: parking_lot::Mutex::new(None),
        };

        // Here rather than beside the rebuilds above, because it is the one
        // repair that needs an engine: it takes the writer, a chunk at a time,
        // through the same path a drop does. Ahead of any retention pass on
        // this process, which is the ordering it needs — see its own
        // documentation.
        engine.resume_interrupted_drops()?;

        Ok(engine)
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

    /// Entries this node held as state (ADR-160) that arrived in a window
    /// contiguous from its position and were released, since start (ADR-169's
    /// addendum). One per entry: a mark is released once, and a re-delivery
    /// of an entry already released finds no mark. Counted when the run that
    /// released them commits, so a rolled-back release is not counted.
    pub fn held_marks_released(&self) -> u64 {
        self.held_marks_released.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Add `n` committed releases to [`Self::held_marks_released`].
    pub(crate) fn count_held_marks_released(&self, n: u64) {
        self.held_marks_released.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// The longest any one transaction has held the writer, since start.
    pub fn writer_hold_max(&self) -> std::time::Duration {
        std::time::Duration::from_micros(
            self.writer_hold_max_us.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// What every hold since start was made of, per holder (ADR-176).
    pub fn writer_hold_decomposition(&self) -> crate::hold_meter::HoldDecomposition {
        self.hold_counters.snapshot()
    }

    /// What serving peers' windows has cost this node, since start (ADR-176).
    pub fn serve_cost(&self) -> crate::hold_meter::ServeSnapshot {
        self.serve_counters.snapshot()
    }

    pub(crate) fn serve_counters(&self) -> &crate::hold_meter::ServeCounters {
        &self.serve_counters
    }

    /// How long each holder has held the writer, since start (ADR-159).
    pub fn writer_hold(&self) -> WriterHoldSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        WriterHoldSnapshot {
            buckets: std::array::from_fn(|holder| {
                std::array::from_fn(|slot| self.writer_hold_buckets[holder][slot].load(Relaxed))
            }),
            count: std::array::from_fn(|holder| self.writer_hold_count[holder].load(Relaxed)),
            sum_us: std::array::from_fn(|holder| self.writer_hold_sum_us[holder].load(Relaxed)),
        }
    }

    /// Take the writer as `holder` and hold it until the guard is dropped
    /// (ADR-151).
    ///
    /// Every write on this engine waits behind the hold, exactly as behind
    /// a transaction; a caller with a budget gives up inside it. For a test
    /// that needs the writer busy, and for nothing on a request path — so
    /// the holder it is given is the one whose hold it is standing in for.
    pub fn hold_writer(&self, holder: WriterHolder) -> WriterHold<'_> {
        let gate = blocking(|| self.writer_gate.lock());
        WriterHold::new(self, gate, holder)
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

    /// Record a hold that has just been let go: its length and holder
    /// (ADR-159), and what it was made of (ADR-176) — `meter` is the scope
    /// that metered it, finished here, after the release, and `phases` its
    /// `work`, `counts` and `commit` spans.
    fn record_writer_hold(
        &self,
        held: std::time::Duration,
        holder: WriterHolder,
        meter: Option<crate::hold_meter::Scope>,
        phases: [std::time::Duration; crate::hold_meter::Phase::COUNT],
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        if let Some(meter) = meter {
            let (metered, cpu) = meter.finish();
            let hold = crate::hold_meter::decompose(&metered, held, cpu);
            self.hold_counters.record(holder, &hold, phases);
        }
        let us = u64::try_from(held.as_micros()).unwrap_or(u64::MAX);
        self.writer_hold_max_us.fetch_max(us, Relaxed);
        let row = holder.slot();
        if let Some(slot) = WRITER_HOLD_BUCKETS_US.iter().position(|upper| us <= *upper) {
            self.writer_hold_buckets[row][slot].fetch_add(1, Relaxed);
        }
        self.writer_hold_count[row].fetch_add(1, Relaxed);
        self.writer_hold_sum_us[row].fetch_add(us, Relaxed);
        if held >= WRITER_HOLD_WARN {
            warn!(
                held_ms = held.as_millis() as u64,
                holder = holder.label(),
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
            // `len()` is the count redb keeps in each table's root header,
            // read without visiting a page of the table. This used to be
            // `iter().count()` on both, which walked the whole oplog and the
            // whole index through the page cache on every open — at a 4 GiB
            // file, the oplog twice over before the node served anything
            // (ADR-153's investigation; the third walk, the version vector's,
            // is recorded there as the one that remains).
            if oplog.len()? == arrival.len()? {
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

    /// Raise the version vector to cover every oplog entry this node appended
    /// in position — everything in the oplog except the entries it holds as
    /// state (ADR-160).
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
            // The entries this node appended as STATE rather than as history,
            // which it must not raise its position over: it holds the document
            // but cannot serve a contiguous window containing it. ADR-160.
            //
            // `?`, not `.ok()`. `redb::TableError` has seven variants and only
            // one of them is "no such table": the rest include `Storage(Io)`
            // and `Storage(Corrupted)`, and swallowing those would turn a
            // database reporting damage into "nothing is held" -- silently
            // restoring the pre-ADR-160 behaviour on precisely the node least
            // entitled to it. The table is created eagerly in `open_with_cache`
            // above, before this runs, so a database from an older build or a
            // restored backup finds it here EMPTY rather than missing, which
            // produces the same "count every entry" answer without hiding
            // anything.
            let held = txn.open_table(tables::OPLOG_HELD)?;
            // Hoisted: after one retention pass the table exists on every
            // node, including ones that have never taken a snapshot, so
            // without this every open would pay a lookup per oplog row on the
            // walk ADR-153 measured as the dominant cost of opening a large
            // database. Empty is the overwhelmingly common case.
            let any_held = !held.is_empty()?;
            for row in oplog.iter()? {
                let (key, _) = row?;
                if any_held && held.get(key.value())?.is_some() {
                    continue;
                }
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

        info!(
            nodes = stored.len(),
            "raised the version vector to cover the oplog entries appended in position"
        );
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
            // The same exclusion the open-time rebuild makes, for the same
            // reason and more sharply: this REPLACES the vectors rather than
            // merging into them, so counting a held entry here does not merely
            // fail to withhold a raise -- it writes the claim. A rewind taken
            // while a snapshot is in flight, or one that was interrupted and
            // not resumed, would otherwise leave the node asserting it can
            // serve a contiguous window containing documents it took out of
            // stamp order: the defect ADR-160 closes, entered by another door.
            let held = txn.open_table(tables::OPLOG_HELD)?;
            let any_held = !held.is_empty()?;
            for row in oplog.iter()? {
                let (key, _) = row?;
                if any_held && held.get(key.value())?.is_some() {
                    continue;
                }
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
        Self::read_versions_in(&txn, table)
    }

    /// [`Self::read_versions`] inside a read transaction the caller holds.
    pub(crate) fn read_versions_in(
        txn: &redb::ReadTransaction,
        table: redb::TableDefinition<&'static [u8], &'static [u8]>,
    ) -> Result<kimmy_core::VersionVector> {
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

    /// Every collection tombstone this node holds.
    ///
    /// Bounded by `tombstone_retention_secs`: retention collects a tombstone
    /// on the same window it collects the oplog, so this is the drops recent
    /// enough that a peer might still be behind them, not every drop ever
    /// made. That bound is what makes it affordable to put on a snapshot page.
    pub fn collections_dropped(&self) -> Result<Vec<(CollectionId, Stamp)>> {
        let txn = self.db.begin_read()?;
        let dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        let mut out = Vec::new();
        for row in dropped.iter()? {
            let (id, raw) = row?;
            out.push((CollectionId(id.value()), codec::decode_oplog_key(raw.value())?));
        }
        Ok(out)
    }

    /// Record that a collection was dropped at `stamp`, if that is newer.
    pub(crate) fn record_collection_drop(&self, id: CollectionId, stamp: Stamp) -> Result<()> {
        // Read first, outside the writer. A tombstone this node already holds
        // at or above `stamp` is the common case on the route ADR-162 added --
        // a whole-database snapshot replays the sender's whole tombstone list
        // on every page -- and taking the single writer to decide that nothing
        // needs writing costs the receiver an fsync per tombstone per page.
        // ADR-152's rule is that a page a member already holds must not cost
        // one, and this is inside that promise.
        if let Some(existing) = self.collection_dropped_at(id)?
            && stamp <= existing
        {
            return Ok(());
        }
        let txn = self.begin_write(WriterHolder::Ddl)?;
        let wrote = {
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            // Re-read under the writer: the check above is outside it, so
            // another writer may have recorded a newer one since.
            let newer = match dropped.get(id.0)? {
                Some(existing) => stamp > codec::decode_oplog_key(existing.value())?,
                None => true,
            };
            if newer {
                dropped.insert(id.0, codec::oplog_key(&stamp).as_slice())?;
            }
            newer
        };
        // Nothing written, nothing committed -- the same rule the snapshot
        // page itself follows.
        if wrote {
            txn.commit()?;
        } else {
            txn.abort()?;
        }
        Ok(())
    }

    /// Record a batch of peers' collection tombstones in one transaction.
    ///
    /// The per-tombstone [`Self::record_collection_drop`] is right for the one
    /// drop a scoped page or a replicated entry carries. A whole-database
    /// snapshot page carries the sender's whole list (ADR-162), and the first
    /// page of a catch-up has every one of them to record: taking the writer
    /// and fsyncing per tombstone makes that hundreds of thousands of commits,
    /// each one taking the writer away from live traffic.
    ///
    /// Callers pass only tombstones they have already established are newer
    /// than what is held; this re-establishes it under the writer, as the
    /// single version does, because that check was made outside it.
    pub(crate) fn record_collection_drops(&self, stamps: &[(CollectionId, Stamp)]) -> Result<()> {
        if stamps.is_empty() {
            return Ok(());
        }
        let txn = self.begin_write(WriterHolder::Ddl)?;
        let wrote = {
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            let mut wrote = false;
            for (id, stamp) in stamps {
                let newer = match dropped.get(id.0)? {
                    Some(existing) => *stamp > codec::decode_oplog_key(existing.value())?,
                    None => true,
                };
                if newer {
                    dropped.insert(id.0, codec::oplog_key(stamp).as_slice())?;
                    wrote = true;
                }
            }
            wrote
        };
        // Nothing written, nothing committed -- the rule ADR-152 sets and
        // `record_collection_drop` follows one tombstone at a time.
        if wrote {
            txn.commit()?;
        } else {
            txn.abort()?;
        }
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
        let txn = self.begin_write(WriterHolder::Ddl)?;
        Self::record_index_drop_in_txn(&txn, collection, index_id, stamp)?;
        txn.commit()?;
        Ok(())
    }

    /// Record coverage granted by a snapshot.
    ///
    /// Merged rather than replaced, so writes this node made that the sender
    /// never saw are not claimed to be forgotten. A transaction of its own;
    /// a snapshot's final page records it inside the page's transaction
    /// through [`Self::absorb_version_vector_in_txn`] instead (ADR-152).
    pub fn absorb_version_vector(&self, granted: &kimmy_core::VersionVector) -> Result<()> {
        let txn = self.begin_write(WriterHolder::Replication)?;
        Self::absorb_version_vector_in_txn(&txn, granted)?;
        txn.commit()?;
        Ok(())
    }

    /// [`Self::absorb_version_vector`] inside a transaction the caller owns.
    ///
    /// Both vectors: a snapshot hands over state, which is the strongest
    /// form of having processed everything behind it. Raising only the
    /// servable vector would leave the node still asking for the history the
    /// snapshot replaced. Each origin only ever moves up, as every movement
    /// of a version vector does; the witnessed vector stands at or above the
    /// servable one by construction (ADR-054), so raising both by `granted`
    /// is the merge the first form of this computed by reading the tables.
    /// Returns whether anything moved, so a caller can let a transaction go
    /// rather than commit one that wrote nothing.
    pub(crate) fn absorb_version_vector_in_txn(
        txn: &redb::WriteTransaction,
        granted: &kimmy_core::VersionVector,
    ) -> Result<bool> {
        let mut raised = false;
        for (node, hlc) in granted.iter() {
            let stamp = Stamp::new(hlc, node);
            raised |= raise_version(txn, tables::OPLOG_VERSIONS, &stamp)?;
            raised |= raise_version(txn, tables::OPLOG_WITNESSED, &stamp)?;
        }
        raised |= Self::release_held_under(txn, granted)?;
        Ok(raised)
    }

    /// Marks naming an oplog entry this node no longer holds (ADR-160).
    ///
    /// The invariant every remover of an oplog row has to keep: retention only
    /// removes a mark alongside the entry it names, so a mark left behind by
    /// any OTHER remover can never be collected afterwards. Zero, always --
    /// an orphan here is the one growth path nothing bounds.
    pub(crate) fn held_orphans(&self) -> Result<usize> {
        let txn = self.db.begin_read()?;
        let held = txn.open_table(tables::OPLOG_HELD)?;
        let oplog = txn.open_table(tables::OPLOG)?;
        let mut orphans = 0;
        for row in held.iter()? {
            let (key, _) = row?;
            if oplog.get(key.value())?.is_none() {
                orphans += 1;
            }
        }
        Ok(orphans)
    }

    /// Record where a snapshot pull with `peer` stands, in the transaction the
    /// page is already writing. ADR-161.
    ///
    /// `peer` is a key and nothing else here: the bytes are written and read
    /// back and never interpreted, the same way `OPLOG_VERSIONS` is keyed by a
    /// node id. What a peer *is* stays in the cluster layer.
    /// A scoped pull does not displace a whole-database one, and neither
    /// displaces a row belonging to the other's scope on the way out — see
    /// [`Self::stored_snapshot_scope`] for why the row is one per peer and
    /// what decides which pull owns it.
    pub(crate) fn persist_snapshot_progress_in_txn(
        txn: &redb::WriteTransaction,
        peer: NodeId,
        progress: &crate::snapshot::SnapshotProgress,
    ) -> Result<()> {
        let mut table = txn.open_table(tables::SNAPSHOT_PROGRESS)?;
        if progress.scope().is_some() && Self::stored_snapshot_scope(&table, peer)? == Some(None) {
            // A repair of one collection does not cost a whole-database pull
            // its place. Both are in flight against this peer, the row holds
            // one, and the one worth thousands of pages outranks the one worth
            // a few.
            return Ok(());
        }
        let encoded = serde_json::to_vec(progress)?;
        table.insert(peer.to_bytes().as_slice(), encoded.as_slice())?;
        Ok(())
    }

    /// The scope of the pull recorded for `peer`: `None` for no row at all,
    /// `Some(None)` for a whole-database pull, `Some(Some(id))` for a repair
    /// of one collection.
    ///
    /// # Why the row is one per peer
    ///
    /// Because the thing that reads it is: `PeerStalls` holds one
    /// `SnapshotProgress` per peer and `resume_snapshots` inserts by peer, so
    /// a second row for the same peer could only be resolved arbitrarily.
    /// The persisted row mirrors that model rather than inventing a second
    /// one.
    ///
    /// What it must not do is let either pull silently take the other's place.
    /// The in-memory half already knew this — `PeerStalls::snapshot_forgotten`
    /// removes a pull only when the scope matches — and the persisted half did
    /// not, so a scoped repair completing against a peer cleared a
    /// whole-database pull's cursor and the next start began again at page
    /// one. ADR-165.
    ///
    /// An undecodable row reads as no row, so it is overwritten and removed
    /// freely: it is a resume hint, the worst a lost one costs is a snapshot
    /// that starts again, and `snapshots_to_resume` already drops what it
    /// cannot read.
    fn stored_snapshot_scope(
        table: &impl redb::ReadableTable<&'static [u8], &'static [u8]>,
        peer: NodeId,
    ) -> Result<Option<Option<CollectionId>>> {
        let Some(raw) = table.get(peer.to_bytes().as_slice())? else { return Ok(None) };
        let stored: std::result::Result<crate::snapshot::SnapshotProgress, _> =
            serde_json::from_slice(raw.value());
        Ok(stored.ok().map(|progress| progress.scope()))
    }

    /// Forget the snapshot pull with `peer` **scoped to `scope`**. -> whether
    /// there was one to forget.
    ///
    /// The answer is what tells the caller whether anything was written, which
    /// is what decides whether the transaction commits at all -- a completed
    /// snapshot that never persisted a cursor must not cost an fsync to clear
    /// a row that is not there. A row belonging to a pull of a different scope
    /// is not one to forget, and reads the same way: nothing written.
    pub(crate) fn forget_snapshot_progress_in_txn(
        txn: &redb::WriteTransaction,
        peer: NodeId,
        scope: Option<CollectionId>,
    ) -> Result<bool> {
        let mut table = txn.open_table(tables::SNAPSHOT_PROGRESS)?;
        match Self::stored_snapshot_scope(&table, peer)? {
            // No row, or one this pull does not own.
            None => return Ok(false),
            Some(stored) if stored != scope => return Ok(false),
            Some(_) => {}
        }
        Ok(table.remove(peer.to_bytes().as_slice())?.is_some())
    }

    /// Whether a snapshot pull with `peer` scoped to `scope` is recorded,
    /// without opening a write transaction. Used to decide whether clearing
    /// one is worth a transaction of its own.
    pub(crate) fn snapshot_progress_recorded(
        &self,
        peer: NodeId,
        scope: Option<CollectionId>,
    ) -> Result<bool> {
        let txn = self.db.begin_read()?;
        let Ok(table) = txn.open_table(tables::SNAPSHOT_PROGRESS) else { return Ok(false) };
        Ok(Self::stored_snapshot_scope(&table, peer)? == Some(scope))
    }

    /// Clear a recorded snapshot pull with `peer` scoped to `scope`, in a
    /// transaction of its own. Only for the one case that has no page
    /// transaction to ride in: a final page that wrote nothing.
    pub(crate) fn forget_snapshot_progress(
        &self,
        peer: NodeId,
        scope: Option<CollectionId>,
    ) -> Result<()> {
        let txn = self.begin_write(WriterHolder::Repair)?;
        let existed = Self::forget_snapshot_progress_in_txn(&txn, peer, scope)?;
        if existed {
            txn.commit()?;
        } else {
            txn.abort()?;
        }
        Ok(())
    }

    /// Every snapshot pull this node was part-way through when it stopped, by
    /// peer. Read once at startup; the cluster layer decides what to do with
    /// them.
    ///
    /// A record that cannot be decoded is dropped with a warning rather than
    /// failing the open. It is an optimisation -- the worst a lost record
    /// costs is a snapshot that starts again -- and a node that will not start
    /// because it cannot read a resume hint is a worse failure than the one
    /// the hint avoids.
    pub fn snapshots_to_resume(&self) -> Result<Vec<(NodeId, crate::snapshot::SnapshotProgress)>> {
        let txn = self.db.begin_read()?;
        let Ok(table) = txn.open_table(tables::SNAPSHOT_PROGRESS) else { return Ok(Vec::new()) };
        let mut out = Vec::new();
        for row in table.iter()? {
            let (key, value) = row?;
            let Ok(bytes) = <[u8; 16]>::try_from(key.value()) else {
                warn!("a recorded snapshot pull has an unreadable peer key; ignored");
                continue;
            };
            let peer = NodeId::from_bytes(bytes);
            match serde_json::from_slice(value.value()) {
                Ok(progress) => out.push((peer, progress)),
                Err(e) => {
                    warn!(%peer, error = %e, "a recorded snapshot pull could not be read; it will start again")
                }
            }
        }
        Ok(out)
    }

    /// How many entries this node holds as state rather than as history
    /// (ADR-160). Not zero on a settled node in general -- see the note on
    /// `OPLOG_HELD` for the two cases that leave marks behind a completed
    /// transfer.
    pub(crate) fn held_len(&self) -> Result<usize> {
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(tables::OPLOG_HELD)?.iter()?.count())
    }

    /// How many entries this node holds as state (ADR-160), for the
    /// `kimmy_sync_held_marks` gauge.
    ///
    /// The count redb keeps in the table's root, read without visiting a row,
    /// so a scrape costs the same however many marks a member holds. Not
    /// split by origin: that would be a walk of the table on every scrape.
    pub fn held_marks(&self) -> Result<u64> {
        let txn = self.db.begin_read()?;
        Ok(txn.open_table(tables::OPLOG_HELD)?.len()?)
    }

    /// Drop the state marks (ADR-160) on every entry `granted` now covers.
    /// -> whether anything was released.
    ///
    /// A snapshot document is held out of the position because the node cannot
    /// serve a contiguous window containing it. The grant is precisely the
    /// statement that it now can: the sender served this node a vector it
    /// stands behind, and every stamp at or below it is covered however the
    /// document arrived. So the mark has done its job and must go, in the same
    /// transaction that adopts the coverage.
    ///
    /// It matters more than it looks, and not for the obvious reason: a stale
    /// mark cannot lower a vector that already covers the stamp, because the
    /// open-time rebuild merges and never lowers. What it can do is withhold a
    /// raise in the one case the rebuild exists for — a stored vector lost or
    /// disagreeing with the oplog (ADR-054's repair). A node repaired in that
    /// state with marks still on covered entries would come back under-claiming
    /// what it can serve, permanently. Releasing them here is what keeps the
    /// rebuild able to re-derive the coverage it is there to re-derive.
    ///
    /// The table is NOT bounded by "a snapshot in flight" -- see the note on
    /// `OPLOG_HELD` -- so this removes in place rather than collecting the
    /// covered keys first. Materialising them would put an allocation
    /// proportional to a repaired collection inside the final page's write
    /// transaction, holding the single writer while it built.
    fn release_held_under(
        txn: &redb::WriteTransaction,
        granted: &kimmy_core::VersionVector,
    ) -> Result<bool> {
        let mut released = false;
        txn.open_table(tables::OPLOG_HELD)?.retain(|key, ()| {
            let Ok(stamp) = codec::decode_oplog_key(key) else {
                // A key this build cannot read is kept. Dropping it would
                // silently raise the position over whatever it named.
                return true;
            };
            // `get` answers `Hlc::ZERO` for an origin the grant does not
            // mention, so such an entry is covered only if its own stamp is
            // ZERO — which no real entry's is, since `HlcClock::tick` never
            // mints one. An unmentioned origin therefore keeps its marks,
            // which is the conservative half.
            let covered = stamp.hlc <= granted.get(stamp.node);
            released |= covered;
            !covered
        })?;
        Ok(released)
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

    /// This node's side of a divergence count probe (ADR-168): the witnessed
    /// vector, and the live count of collection `id`, read in **one** read
    /// transaction.
    ///
    /// The gate judges the count against the vector, and it is sound only
    /// while the vector cannot name an entry the count missed. Read as two
    /// calls, that held because the caller read the vector first, an order
    /// kept by a comment. Read from one snapshot, the two describe the same
    /// state and there is no order to keep. The count is `None` when this node
    /// holds no collection under `id`, as [`Self::count_by_id`] answers.
    ///
    /// A walk of the collection's records, so a caller on the async runtime
    /// runs it under [`blocking`] (ADR-153).
    pub fn count_probe_reading(
        &self,
        id: CollectionId,
    ) -> Result<(kimmy_core::VersionVector, Option<u64>)> {
        let txn = self.db.begin_read()?;
        let witnessed = Self::read_versions_in(&txn, tables::OPLOG_WITNESSED)?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        let mut held = false;
        for row in collections.iter()? {
            let (_, value) = row?;
            let meta: CollectionMeta = serde_json::from_slice(value.value())?;
            if meta.id == id {
                held = true;
                break;
            }
        }
        if !held {
            return Ok((witnessed, None));
        }
        Ok((witnessed, Some(crate::live_count::live_count(&txn, id)?)))
    }

    /// The live documents of collection `id`, counted in `txn` by walking each
    /// record's header (`codec::doc_record_is_live`).
    ///
    /// What the kept count (ADR-174) is checked against. The probe no longer
    /// walks: it reads `live_count::live_count`.
    #[cfg(test)]
    pub(crate) fn live_count_in(txn: &redb::ReadTransaction, id: CollectionId) -> Result<u64> {
        let docs = txn.open_table(tables::DOCS)?;
        let mut count = 0u64;
        for row in docs.range(doc_range(id))? {
            let (_, value) = row?;
            if codec::doc_record_is_live(value.value())? {
                count += 1;
            }
        }
        Ok(count)
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
        let txn = self.begin_write(WriterHolder::Replication)?;
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
        let txn = self.begin_write(WriterHolder::Replication)?;
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

    /// Begin a counted write transaction, held as `holder`.
    ///
    /// Every write an open engine performs goes through here rather than
    /// through [`Database::begin_write`] directly, so that [`Engine::commits`]
    /// counts the engine's durable commits rather than the paths somebody
    /// remembered to instrument — `commits_are_counted_at_one_chokepoint`
    /// fails if a new one appears. The three commits that legitimately do not
    /// pass through here all happen where there is no `Engine` yet to count
    /// them: opening the database, migrating it, and restoring a backup into a
    /// fresh file.
    ///
    /// `holder` is a parameter rather than something read from the ambient
    /// span, so that a new write path cannot be added without naming what it
    /// is: the attribution is a compile error to omit, which is the only way
    /// it stays complete (ADR-159).
    pub(crate) fn begin_write(&self, holder: WriterHolder) -> Result<WriteTxn<'_>> {
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
        meter_writer_wait(waited);
        let Some(gate) = gate else {
            self.writer_wait_timeouts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            warn!(
                waited_ms = waited.as_millis() as u64,
                caller = holder.label(),
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
            meter: Some(crate::hold_meter::Scope::hold()),
            commit_from: None,
            counted_at: None,
            holder,
            live_counts: Default::default(),
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
        // The gate is held for exactly as long as it always was — to the end
        // of this function — and is now wrapped so that the hold is recorded
        // (ADR-159). This is the one path that takes the writer without
        // opening a counted transaction, which is why it was the one hold
        // nothing measured at all: not mislabelled, absent.
        let waited_from = std::time::Instant::now();
        let gate = blocking(|| self.writer_gate.lock());
        meter_writer_wait(waited_from.elapsed());
        let mut gate = WriterHold::new(self, gate, WriterHolder::Durability);
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
        gate.commit_from = Some(std::time::Instant::now());
        txn.commit()?;
        self.fsyncs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        drop(gate);
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
        let txn = self.begin_write(WriterHolder::Ddl)?;
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
        let txn = self.begin_write(WriterHolder::Ddl)?;
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
                #[cfg(test)]
                crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::SystemCreate);
                match self.create_collection_unchecked(db, name) {
                    // Checked outside the writer, so two callers can both find
                    // it absent: two applies of one `ConfigureVectors` creating
                    // its shadow, or two requests setting up the same system
                    // collection. The second finds it here, and wants what it
                    // would have had a moment earlier: the collection.
                    Err(StorageError::Core(CoreError::CollectionExists { .. })) => {
                        #[cfg(test)]
                        crate::sync::race_hooks::absorbed(
                            crate::sync::race_hooks::Race::SystemCreate,
                        );
                        // Buried again before this read, it is gone for a
                        // reason this caller did not see: fail with that, not
                        // with `CollectionNotFound`, which a replicated
                        // `ConfigureVectors` reads as its *parent* missing.
                        self.get_collection(db, name).map_err(|e| match e {
                            StorageError::Core(CoreError::CollectionNotFound { .. }) => {
                                StorageError::Transaction(format!(
                                    "{db}.{name} was created and dropped again while this node \
                                     was creating it; retry"
                                ))
                            }
                            other => other,
                        })
                    }
                    created => created,
                }
            }
            Err(e) => Err(e),
        }
    }

    fn create_collection_unchecked(&self, db: &str, name: &str) -> Result<CollectionMeta> {
        Ok(self
            .create_collection_inner(db, name, true, None, &|_| false)?
            .expect("a creation that judges nothing history always creates"))
    }

    /// `log = false` when applying a replicated creation. See
    /// `create_index_inner` for why a replicated change must not mint an entry.
    ///
    /// `history` judges the collection's tombstone, if it has one, **under the
    /// writer**: true means this creation is of a life that drop ended, and
    /// nothing is created (`None`). A caller that arrives with a creation from
    /// elsewhere checks its tombstone before calling, to answer cheaply, but
    /// that check is not under the writer: the drop can land between it and
    /// here, and a creation older than it then resurrected the collection the
    /// drop had just removed (ADR-148). A local creation passes `|_| false`.
    pub(crate) fn create_collection_inner(
        &self,
        db: &str,
        name: &str,
        log: bool,
        origin: Option<Hlc>,
        history: &dyn Fn(Stamp) -> bool,
    ) -> Result<Option<CollectionMeta>> {
        // Derived, not allocated: every node computes the same id for the
        // same collection, so a replicated oplog entry addresses the same
        // collection everywhere. See `CollectionId::derive`.
        let id = CollectionId::derive(db, name);

        // A drop is chunked (ADR-158), so a creation of the same name can land
        // between two of its chunks — and the name derives the same id, so
        // whatever that drop has not reached yet would be inherited by the new
        // incarnation: documents of a life that has ended, answering queries
        // under ids this collection never wrote, and index entries pointing at
        // them. Finished here, before the definition that would stand over
        // them exists. Ordinarily there is nothing to finish and this is two
        // seeks against an empty range; where there is, it is the same bounded
        // chunks the drop was making, so it does not hold the writer either.
        //
        // Those chunks are held as `drop` and not as `ddl` (ADR-159): the
        // holder names the work, and finishing somebody else's drop is drop
        // work whoever happens to be doing it. A creation that pays for one
        // says so on the page.
        self.purge_dropped_collection(id)?;

        let txn = self.begin_write(WriterHolder::Ddl)?;
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

            // If this creation follows a drop of the same id — a recreate —
            // the drop's stamp becomes the new incarnation's floor: replicated
            // entries stamped at or before it belong to the previous life and
            // must not enter the replacement, however their stamps sort
            // against the drop itself. A creation with no tombstone behind it
            // carries no floor: two nodes deriving the same id independently
            // is normal convergence, not reincarnation, and flooring there
            // would make whichever node created second silently discard the
            // first one's documents.
            let dropped = self.collection_dropped_at(id)?;
            if dropped.is_some_and(history) {
                drop(collections);
                txn.abort()?;
                return Ok(None);
            }
            let incarnation_floor = dropped.map(|stamp| stamp.hlc);

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
        Ok(Some(meta))
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
    ///
    /// Two stages, and the order between them is the decision (ADR-158). The
    /// definition, the tombstone and the entry go in one short transaction;
    /// what the collection *held* is removed after it, a chunk per commit,
    /// with the writer released between chunks. So a drop of any size holds
    /// the single writer for one chunk at a time, and from the first commit
    /// the collection is gone to everything that asks about it.
    pub(crate) fn drop_collection_inner(
        &self,
        db: &str,
        name: &str,
        replicated: Option<Stamp>,
    ) -> Result<bool> {
        let Some(buried) = self.bury_collection(db, name, replicated)? else {
            return Ok(false);
        };
        for id in buried {
            self.purge_what_the_drop_left(id, db, name)?;
        }
        Ok(true)
    }

    /// [`Self::purge_dropped_collection`], tolerating the one failure that is
    /// not the drop's to report.
    ///
    /// Once the collection is buried the drop **has happened**: the definition
    /// is gone, the tombstone is durable, and a local drop's entry is
    /// published. What is left is a removal this node owes itself and nothing
    /// can see. So a chunk that gives up waiting for the writer inside a
    /// caller's budget (ADR-151) must not be reported as a failed drop — it is
    /// not one, and a client told to retry would be answered `dropped: false`
    /// by the retry while the rows stayed exactly where they are. The next
    /// retention pass finishes it ([`Self::finish_owed_drops`]), as the next
    /// start would; a collection created under the name before then finishes
    /// it first. The tombstone that identifies the residue is not collected
    /// while rows remain under it, so the pass that finds it is never late.
    fn purge_what_the_drop_left(&self, id: CollectionId, db: &str, name: &str) -> Result<()> {
        match self.purge_dropped_collection(id) {
            Ok(_) => Ok(()),
            Err(StorageError::WriterBusy { waited }) => {
                warn!(
                    db,
                    collection = name,
                    waited_ms = waited.as_millis() as u64,
                    "a chunk of this drop gave up waiting for the single writer; the collection \
                     is dropped and what it held is removed by the next retention pass"
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// The first transaction of a drop: the definition goes, the tombstone is
    /// recorded, the entry is minted, and the database row goes with the last
    /// collection in it. `None` if there was no such collection; otherwise the
    /// ids whose documents and index entries are left to
    /// [`Self::purge_dropped_collection`].
    ///
    /// **The tombstone is recorded here, before the first chunk of the purge,
    /// and that ordering is what makes chunking safe against ADR-155.** From
    /// this commit the collection is gone to everything that asks: a client's
    /// query reads it as absent rather than as intact and short of documents,
    /// the existence half of the divergence check subtracts it, a snapshot
    /// page offering to recreate it is refused, and a peer's write addressed
    /// to it is history. A drop that removed the documents first would spend
    /// the whole of its length in the one state that re-seeds it — a
    /// collection this node still holds, missing most of what a peer holds,
    /// with no tombstone to say why — which is a count divergence to the
    /// check, a repair to anti-entropy, and the collection back from the peer
    /// that has not applied the drop yet.
    ///
    /// `pub(crate)` for the tests in the modules that own ADR-155's guards:
    /// stopping a drop after this commit is the state a drop is in for the
    /// whole of its purge, and the state a restart leaves behind, and those
    /// guards have to be exercised in it rather than only after a drop that
    /// finished.
    pub(crate) fn bury_collection(
        &self,
        db: &str,
        name: &str,
        replicated: Option<Stamp>,
    ) -> Result<Option<Vec<CollectionId>>> {
        // Nothing to bury, answered without taking the writer. Only that: what
        // is buried is read again under the writer below.
        match self.get_collection(db, name) {
            Ok(_) => {}
            Err(StorageError::Core(CoreError::CollectionNotFound { .. })) => return Ok(None),
            Err(e) => return Err(e),
        }
        #[cfg(test)]
        crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::Burial);

        // A vector-enabled collection keeps its vectors in a shadow collection,
        // which is an ordinary collection with its own id and so is not carried
        // away by removing this one. Left behind, its chunks outlive the
        // documents they describe — and because the shadow's name is derived
        // from this one's, a collection later created with the same name adopts
        // them. They are searchable: a document from the dropped collection
        // came back from `vector_search` scoring 1.0, above the new
        // collection's own documents, with an `_id` that resolves to nothing.
        //
        // Buried in the same transaction rather than by a second call, so there
        // is no instant in which the parent is gone and its vectors are still
        // answering queries. Its chunks are then purged with the parent's
        // documents, in the same chunks and under the same guard: on the
        // measurement this change was made for, the shadow was the slower half.

        let log = replicated.is_none();
        // `ddl` and not `drop`, and the split is the point of the pairing
        // (ADR-159): the holder names what the transaction writes, and this
        // one writes metadata — a definition removed, a tombstone, an entry —
        // in time that does not grow with the collection. The destructive
        // half is `purge_chunk`, and it is `drop`. Labelling the burial as a
        // drop would put an O(1) transaction in the row an operator reads to
        // find out what is churning through the writer.
        let txn = self.begin_write(WriterHolder::Ddl)?;
        // What is buried is what stands under the name **now**, read under the
        // writer, and not what the read above found. Between the two, another
        // burial of the name can land, or a burial and a recreation with
        // writes into the new incarnation: burying by name alone then removed
        // the recreation and its documents, on an apply that returned `Ok`
        // and witnessed the window that carried them. So a replicated drop is
        // held here to the incarnation rule its caller applies before it
        // (`aims_at_a_previous_incarnation`, ADR-148): aimed at a life that is
        // gone, it buries nothing, and the caller records its tombstone as it
        // does for any drop of a previous life. A local drop mints its stamp
        // below, ahead of everything, and takes whatever stands.
        let standing = {
            let collections = txn.open_table(tables::COLLECTIONS)?;
            let read = |n: &str| -> Result<Option<CollectionMeta>> {
                collections
                    .get((db, n))?
                    .map(|v| serde_json::from_slice(v.value()).map_err(Into::into))
                    .transpose()
            };
            match read(name)? {
                Some(current)
                    if replicated.is_none_or(|stamp| {
                        !crate::sync::aims_at_a_previous_incarnation(&current, stamp.hlc)
                    }) =>
                {
                    let shadow = if vector_meta::is_shadow(name) {
                        None
                    } else {
                        read(&vector_meta::shadow_name(name))?
                    };
                    Some((current, shadow))
                }
                _ => None,
            }
        };
        let Some((meta, shadow)) = standing else {
            txn.abort()?;
            debug!(
                db,
                collection = name,
                "the collection a drop read was buried, or buried and recreated, before the drop \
                 took the writer; nothing of what stands now is the drop's to bury"
            );
            #[cfg(test)]
            crate::sync::race_hooks::absorbed(crate::sync::race_hooks::Race::Burial);
            return Ok(None);
        };
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
        let buried = {
            // Same transaction as the definition, so there is no instant in
            // which the collection is gone with no record that it was dropped
            // — the state a peer re-seeds it from.
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            // The shadow needs its own tombstone for the same reason the parent
            // does: without one, a peer still replaying pre-drop vector writes
            // would recreate it and repopulate the chunks the purge is about to
            // remove.
            let ids: Vec<CollectionId> =
                [Some(meta.id), shadow.as_ref().map(|s| s.id)].into_iter().flatten().collect();
            for id in &ids {
                let newer = match dropped.get(id.0)? {
                    Some(existing) => stamp > codec::decode_oplog_key(existing.value())?,
                    None => true,
                };
                if newer {
                    dropped.insert(id.0, codec::oplog_key(&stamp).as_slice())?;
                }
            }
            ids
        };

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
        Ok(Some(buried))
    }

    /// Remove what a dropped collection held, a chunk per commit, with the
    /// writer released between chunks (ADR-158). Returns how many rows went.
    ///
    /// The ranges are looked at under a read transaction and the writer is
    /// taken only when there is something in them, so a drop of an empty
    /// collection — and a sweep with nothing to finish — costs no transaction
    /// at all, which is the rule the retention pass follows over the same
    /// writer (ADR-151).
    ///
    /// Called by the drop itself, by [`Self::create_collection_inner`] before
    /// a name that derives this id can stand over what is left, and by
    /// [`Self::resume_interrupted_drops`] at open. All three want the same
    /// thing and none of them may assume the others got there first.
    pub(crate) fn purge_dropped_collection(&self, id: CollectionId) -> Result<usize> {
        let mut removed = 0usize;
        loop {
            if self.collection_range_is_empty(id)? {
                break;
            }
            let chunk = self.purge_chunk(id)?;
            removed += chunk;
            // Short of a full chunk means the ranges are exhausted, or that
            // the guard below turned the chunk away; both are the end of it.
            if chunk < DROP_PURGE_CHUNK {
                break;
            }
        }
        Ok(removed)
    }

    /// Whether anything is filed under a collection id at all.
    pub(crate) fn collection_range_is_empty(&self, id: CollectionId) -> Result<bool> {
        let txn = self.db.begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        if docs.range(doc_range(id))?.next().is_some() {
            return Ok(false);
        }
        let indexes = txn.open_table(tables::INDEX_ENTRIES)?;
        Ok(indexes.range(index_range(id))?.next().is_none())
    }

    /// One chunk of [`Self::purge_dropped_collection`]: at most
    /// [`DROP_PURGE_CHUNK`] rows in one transaction, documents first and index
    /// entries with whatever of the chunk they leave.
    ///
    /// Keys are read from the front of the range and then removed, both inside
    /// the transaction — the read is bounded by the chunk, not by the size of
    /// the collection, so it is not the walk under the writer ADR-151 forbids.
    fn purge_chunk(&self, id: CollectionId) -> Result<usize> {
        // The whole of a drop's destructive work is here, so this is the
        // transaction `drop` counts (ADR-159) — once per chunk now rather
        // than once per drop, which is what the count of that row is for:
        // a `drop` count climbing in thousands beside a flat `ddl` is a large
        // purge in progress, whoever started it. The same chunks are run by
        // `create_collection_inner` and by the sweep at open, and they are
        // `drop` there too: the holder names the work, not who asked.
        let txn = self.begin_write(WriterHolder::Drop)?;
        let removed = {
            // A collection standing under this id means the name was created
            // again since the drop — the id is derived from the name, so a
            // recreation lands on it — and what is filed under it now belongs
            // to the new incarnation. Asked in the same transaction that does
            // the removing, so the two cannot interleave: redb has one writer,
            // and a creation either commits before this chunk sees it or after
            // this chunk has finished.
            //
            // A backstop rather than the primary mechanism: the creation
            // drains the range itself before it writes its definition, so in
            // a real race the purge's loop usually ends at its own
            // emptiness check and never reaches here. What is left for this
            // is the narrow ordering where the dropper's last chunk was full,
            // and the creation then drains, creates and writes before the
            // dropper looks again — narrow, reachable, and what the
            // sequential test pins.
            let collections = txn.open_table(tables::COLLECTIONS)?;
            if collection_stands_under(&collections, id)? {
                0
            } else {
                drop(collections);
                let mut removed = 0usize;
                {
                    let mut docs = txn.open_table(tables::DOCS)?;
                    let keys: Vec<Vec<u8>> = {
                        let mut keys = Vec::new();
                        for row in docs.range(doc_range(id))?.take(DROP_PURGE_CHUNK) {
                            let (key, _) = row?;
                            keys.push(key.value().1.to_vec());
                        }
                        keys
                    };
                    for key in &keys {
                        crate::live_count::remove_record(&txn, &mut docs, id.0, key)?;
                    }
                    removed += keys.len();
                }
                if removed < DROP_PURGE_CHUNK {
                    let mut indexes = txn.open_table(tables::INDEX_ENTRIES)?;
                    let keys: Vec<(u32, Vec<u8>, Vec<u8>)> = {
                        let mut keys = Vec::new();
                        for row in indexes.range(index_range(id))?.take(DROP_PURGE_CHUNK - removed)
                        {
                            let (key, _) = row?;
                            let (_, index, value, doc) = key.value();
                            keys.push((index, value.to_vec(), doc.to_vec()));
                        }
                        keys
                    };
                    for (index, value, doc) in &keys {
                        indexes.remove((id.0, *index, value.as_slice(), doc.as_slice()))?;
                    }
                    removed += keys.len();
                }
                removed
            }
        };
        // Nothing removed, nothing to commit: the collection came back, or a
        // concurrent purge of the same id got there first.
        if removed == 0 {
            txn.abort()?;
        } else {
            txn.commit()?;
        }
        Ok(removed)
    }

    /// Finish a drop a restart interrupted (ADR-158).
    ///
    /// A drop records the definition's removal and the tombstone in its first
    /// transaction and removes what the collection held after it, so a process
    /// that stops in between leaves rows under an id nothing resolves. They are
    /// invisible to every reader and to every peer — the tombstone saw to that
    /// before the first chunk — but a collection created again under the same
    /// name derives the same id and would stand over them. There is nothing to
    /// replay: the drop is durable and has already replicated, so what is left
    /// is the removal.
    ///
    /// It is finished **here**, in `open`, to be ahead of the retention pass:
    /// the residue is identified by a tombstone with no collection over it,
    /// and `gc::collect_dropped_collections` removes that tombstone past
    /// `storage.tombstone_retention_secs`, so a collector that ran first would
    /// take the only marker this reads. Running inside `open` puts this ahead
    /// of any collector on this process by construction. Being ahead of a
    /// *creation* is not the reason — `create_collection_inner` purges the
    /// derived id unconditionally, which covers that case whenever it happens.
    fn resume_interrupted_drops(&self) -> Result<()> {
        let owed = self.drops_left_unfinished()?;
        // An ordinary start finds every drop finished and says nothing.
        if owed.is_empty() {
            return Ok(());
        }

        // Said **before** the work and not only after it. This runs on the
        // way to opening, so a member restarted part-way through a large
        // drop finishes it before it serves anything — up to the length of
        // what is left of that drop. Each chunk is short, so the
        // writer-hold `WARN` never fires either, and a purge that speaks
        // only when it ends is indistinguishable from a start that has hung.
        // The row count is what lets an operator size the wait.
        for (id, rows) in &owed {
            info!(
                collection = %id,
                rows,
                "a collection drop was interrupted; finishing it before this node opens"
            );
        }

        let mut rows = 0usize;
        for (id, _) in &owed {
            rows += self.purge_dropped_collection(*id)?;
        }
        info!(
            collections = owed.len(),
            rows, "finished the collection drops that a restart interrupted"
        );
        Ok(())
    }

    /// Finish, while the node runs, a drop whose purge was left owed (ADR-158's
    /// addendum): the retention pass's half of [`Self::resume_interrupted_drops`].
    ///
    /// A purge is left owed when one of its chunks gives up waiting for the
    /// writer inside a caller's budget ([`Self::purge_what_the_drop_left`]),
    /// which happens under exactly the sustained load a member can then stay
    /// up through for days. Found the way `open` finds it — a tombstone with no
    /// collection over it and rows beneath — and removed with the same chunked
    /// purge, so the pass holds the writer one chunk at a time as every other
    /// part of it does (ADR-151). Run before the pass collects collection
    /// tombstones, and those are not collected while rows remain under them,
    /// so the marker this reads outlasts the residue it marks.
    ///
    /// A chunk that gives up here too leaves the rest for the next pass, which
    /// finds it by the same marker. A pass with nothing owed reads the dropped
    /// table and says nothing.
    pub(crate) fn finish_owed_drops(&self) -> Result<usize> {
        let owed = self.drops_left_unfinished()?;
        if owed.is_empty() {
            return Ok(0);
        }
        for (id, rows) in &owed {
            info!(
                collection = %id,
                rows,
                "a collection drop left rows behind; the retention pass is removing them"
            );
        }

        let mut rows = 0usize;
        for (id, _) in &owed {
            match self.purge_dropped_collection(*id) {
                Ok(removed) => rows += removed,
                Err(StorageError::WriterBusy { waited }) => {
                    warn!(
                        collection = %id,
                        rows,
                        waited_ms = waited.as_millis() as u64,
                        "removing what a collection drop left gave up waiting for the single \
                         writer; the next retention pass carries on"
                    );
                    return Ok(rows);
                }
                Err(e) => return Err(e),
            }
        }
        info!(collections = owed.len(), rows, "removed what the collection drops had left behind");
        Ok(rows)
    }

    /// Every collection id a drop left rows under, with how many: a tombstone
    /// with no collection standing over it, and something still filed beneath.
    ///
    /// Counted rather than merely detected, because the number is the only
    /// thing that tells an operator how long the start is about to take. One
    /// read transaction, and it walks only the ranges that are about to be
    /// removed anyway; an id with nothing under it costs the two seeks that
    /// find that out, which is what every start pays and nothing more.
    fn drops_left_unfinished(&self) -> Result<Vec<(CollectionId, usize)>> {
        let live: std::collections::HashSet<CollectionId> =
            self.all_collections()?.into_iter().map(|c| c.id).collect();
        // One row per dropped collection, so the table is walked whole, as
        // the retention pass walks it.
        let txn = self.db.begin_read()?;
        let dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        let docs = txn.open_table(tables::DOCS)?;
        let indexes = txn.open_table(tables::INDEX_ENTRIES)?;
        let mut owed = Vec::new();
        for row in dropped.iter()? {
            let (key, _) = row?;
            let id = CollectionId(key.value());
            if live.contains(&id) {
                continue;
            }
            let mut rows = 0usize;
            for row in docs.range(doc_range(id))? {
                row?;
                rows += 1;
            }
            for row in indexes.range(index_range(id))? {
                row?;
                rows += 1;
            }
            if rows > 0 {
                owed.push((id, rows));
            }
        }
        Ok(owed)
    }

    /// Persist a modified collection definition (used when adding an index).
    /// Whether the definition standing under `read`'s name in `txn` is still
    /// exactly `read`.
    ///
    /// For a change that decides from a definition read before it took the
    /// writer and then writes that definition back under it. The read is not
    /// under the writer, so another change to the collection can commit in
    /// between, and writing the earlier copy back erased it: of two index
    /// creates only one definition survived, the other's entries orphaned.
    /// Checked once the writer is held, a changed definition sends the change
    /// back to decide again from what stands; nothing can change it after
    /// that. [`crate::index::mark_multikey`] re-reads through the transaction
    /// for the same reason.
    pub(crate) fn definition_is(
        txn: &redb::WriteTransaction,
        read: &CollectionMeta,
    ) -> Result<bool> {
        let collections = txn.open_table(tables::COLLECTIONS)?;
        Ok(match collections.get((read.db.as_str(), read.name.as_str()))? {
            Some(standing) => serde_json::from_slice::<CollectionMeta>(standing.value())? == *read,
            None => false,
        })
    }

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
) -> Result<bool> {
    let mut versions = txn.open_table(table)?;
    let node = stamp.node.to_bytes();
    let higher = match versions.get(node.as_slice())? {
        Some(current) => stamp.hlc > decode_hlc(current.value())?,
        None => true,
    };
    if higher {
        versions.insert(node.as_slice(), stamp.hlc.to_bytes().as_slice())?;
    }
    Ok(higher)
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

/// Whether appending an entry moves this node's version vectors to its stamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Position {
    /// The entry is the next thing this node has seen of its origin — a
    /// local write, or a replicated window served contiguously from this
    /// node's own position — so both vectors move to it: appending is the
    /// strongest form of having seen it (ADR-054).
    Raise,
    /// The entry is a snapshot document (ADR-152): state, arriving in key
    /// order rather than stamp order, from a snapshot that may still be
    /// running. The coverage a snapshot grants is recorded once, when it
    /// completes, and is the vector served with its first page; moving the
    /// vectors per document would carry the position to whatever stamp
    /// arrived last, over an entry for a document the sender wrote behind
    /// the cursor and the snapshot never carried — the hole ADR-148 forbids,
    /// which the first form of the restore opened on every page.
    Hold,
    /// [`Self::Raise`], and the caller vouches that the entry arrived in a
    /// window served **contiguously from this node's own position** — a pulled
    /// sync window or a push, both through `Engine::apply_peer_batch` (ADR-143,
    /// ADR-148). Only this variant releases a held mark on an entry this node
    /// already holds (ADR-169): servable means "can serve a contiguous window
    /// containing this", and an entry physically in the oplog that arrived in
    /// such a window has that property whether or not it was appended now. A
    /// plain `Raise` — a local write, a hand-applied `apply_remote`, a batch
    /// with no window behind it — says nothing about contiguity and releases
    /// nothing.
    InWindow,
}

pub(crate) fn append_oplog(txn: &WriteTxn<'_>, entry: &OplogEntry) -> Result<()> {
    append_oplog_at(txn, entry, Position::Raise).map(|_| ())
}

/// [`append_oplog`], with the caller saying whether the vectors move.
/// -> whether a held mark was released on an entry this node already held
/// ([`release_held_in_position`]), for the caller to count once it commits.
pub(crate) fn append_oplog_at(
    txn: &WriteTxn<'_>,
    entry: &OplogEntry,
    position: Position,
) -> Result<bool> {
    let key = codec::oplog_key(&entry.stamp);
    let mut oplog = txn.open_table(tables::OPLOG)?;
    let existed =
        oplog.insert(key.as_slice(), codec::encode_oplog_entry(entry).as_slice())?.is_some();

    // Re-appending an entry we already hold must not give it a second arrival
    // position. Peers resend overlapping ranges routinely, and a duplicate
    // arrival entry would deliver the same change twice to every stream.
    //
    // It must still release a held mark when it arrives in position: that is
    // the entry reaching this node as history after all (ADR-160, ADR-169).
    if existed {
        if position == Position::InWindow {
            // The oplog table is still open on `txn`, and redb refuses a second
            // open of it; the release reads the same table. Without this the
            // release failed the whole batch, and every later round with that
            // peer failed the same way.
            drop(oplog);
            return release_held_in_position(txn, &entry.stamp);
        }
        return Ok(false);
    }

    // Same transaction as the entry, so the vector can never claim coverage of
    // something that was rolled back — a peer would then never be sent it.
    //
    // Both vectors: appending is also the strongest form of having seen it, so
    // witnessed stays at or above servable by construction (ADR-054). Not for
    // a snapshot document, whose coverage is granted once at the end — see
    // [`Position::Hold`].
    if matches!(position, Position::Raise | Position::InWindow) {
        raise_version(txn, tables::OPLOG_VERSIONS, &entry.stamp)?;
        raise_version(txn, tables::OPLOG_WITNESSED, &entry.stamp)?;
        // An entry appended in POSITION is not state, so any mark on its key
        // goes -- whoever left it. A key can carry a stale mark without the
        // entry: `rewind` removes oplog rows directly, and a mark it left
        // behind would otherwise make the re-delivered entry invisible to
        // every later `Engine::open` while the vectors it just raised said
        // otherwise. Symmetric with the `Hold` arm below rather than a special
        // case, which is what stops the two drifting. ADR-160.
        txn.open_table(tables::OPLOG_HELD)?.remove(key.as_slice())?;
    } else {
        // Marked in the entry's own transaction, for the same reason the raise
        // is: a mark that outlived a rolled-back entry would hold the position
        // down over something this node does not hold, and one that was lost
        // while the entry committed would let `Engine::open` raise over it.
        // ADR-160.
        txn.open_table(tables::OPLOG_HELD)?.insert(key.as_slice(), ())?;
    }

    let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
    let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;

    // The counter lives in the index rather than in `meta` so that it cannot
    // drift from the thing it counts: rebuilding the index also rebuilds the
    // counter, and there is no third place for them to disagree.
    let next = arrival.last()?.map_or(0, |(seq, _)| seq.value() + 1);
    arrival.insert(next, key.as_slice())?;
    by_stamp.insert(key.as_slice(), next)?;
    // Every document write appends, so this is where a build that keeps the
    // live counts says so; a mark that no longer matches at open means one that
    // does not wrote since (ADR-174). Read here, where `arrival` and `oplog`
    // are already open, and written once at the commit: writing it per entry
    // rewrote the same single key once for every entry in a batch, while
    // computing it at the commit instead would have to open these two tables
    // again on every transaction, single-document writes included.
    #[cfg(not(feature = "bench-no-live-counts"))]
    {
        let mark = crate::live_count::mark_of(&arrival, &oplog)?;
        txn.live_counts().lock().appended(mark);
    }
    Ok(false)
}

/// Release the held mark (ADR-160) on the entry stored under `stamp`, because
/// that entry has just arrived in position, and raise both vectors to it.
/// -> whether a mark was released.
///
/// Called only for [`Position::InWindow`]: the caller vouches the entry
/// arrived in a window contiguous from this node's position. Reached two
/// ways: from `apply_remote_in_txn` when the entry is superseded at its key --
/// the ordinary case, since a held entry's record is usually at its stamp -- and
/// from [`append_oplog_at`]'s existing-key branch when it wins instead, which
/// happens once retention has collected a held delete's tombstone but kept the
/// entry as the oplog's newest. ADR-160 named "an append of that key under
/// `Position::Raise`" as a release, which the existing-key return never
/// performed, and a test asserted supersession must not release; ADR-169
/// overturns that. So a snapshot document or a carried delete stayed held, above the
/// servable vector, however it later reached this node — the mechanism behind
/// ADR-167's held-delete residual and both routes of ADR-168's limitation.
///
/// Only an entry this node actually holds is raised over: a mark can outlive
/// its entry (`rewind` removes oplog rows directly), and raising a vector over
/// an entry the oplog does not hold would claim a window this node cannot
/// serve. Such a mark is removed and nothing is raised. No arrival is added and
/// nothing is published: the entry already has its arrival position and its
/// change-stream event, both from when it was applied under `Hold`.
pub(crate) fn release_held_in_position(
    txn: &redb::WriteTransaction,
    stamp: &Stamp,
) -> Result<bool> {
    let key = codec::oplog_key(stamp);
    if txn.open_table(tables::OPLOG_HELD)?.remove(key.as_slice())?.is_none() {
        return Ok(false);
    }
    if txn.open_table(tables::OPLOG)?.get(key.as_slice())?.is_none() {
        return Ok(false);
    }
    raise_version(txn, tables::OPLOG_VERSIONS, stamp)?;
    raise_version(txn, tables::OPLOG_WITNESSED, stamp)?;
    Ok(true)
}

/// Whether a live collection stands under `id`.
///
/// The question [`Engine::collection_by_id`] answers, asked against a table the
/// caller already has open so that a purge chunk can ask it inside the
/// transaction that removes the rows — which is what makes a recreation between
/// two chunks safe rather than merely unlikely. A catalogue walk with a JSON
/// parse per collection, metadata only and never a document, stopping at the
/// first match.
fn collection_stands_under(
    collections: &impl ReadableTable<(&'static str, &'static str), &'static [u8]>,
    id: CollectionId,
) -> Result<bool> {
    for entry in collections.iter()? {
        let (_, value) = entry?;
        let meta: CollectionMeta = serde_json::from_slice(value.value())?;
        if meta.id == id {
            return Ok(true);
        }
    }
    Ok(false)
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
    fn a_meter_a_panic_unwinds_through_is_put_back() {
        // A thread that survives a panic inside a metered call must not go
        // on adding its waits to a scope that no longer exists (ADR-175).
        let outcome = std::panic::catch_unwind(|| {
            super::metered_writer_wait(|| panic!("inside the metered call"))
        });
        assert!(outcome.is_err());
        assert_eq!(
            super::WRITER_WAIT_METER.with(|m| m.get()),
            None,
            "the thread is metering nothing once the call is gone"
        );
    }

    /// A file backend that counts the bytes redb asks it for.
    ///
    /// What an open *reads* is not observable from outside otherwise: redb's
    /// cache statistics are behind a feature this crate does not enable, and
    /// timing a walk is a flaky proxy for it. The backend is where every page
    /// miss ends, so its count is the walk.
    #[derive(Debug)]
    struct CountingBackend {
        inner: redb::backends::FileBackend,
        read: std::sync::Arc<std::sync::atomic::AtomicU64>,
    }

    impl redb::StorageBackend for CountingBackend {
        fn len(&self) -> std::result::Result<u64, std::io::Error> {
            self.inner.len()
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> std::result::Result<(), std::io::Error> {
            self.read.fetch_add(out.len() as u64, std::sync::atomic::Ordering::Relaxed);
            self.inner.read(offset, out)
        }
        fn set_len(&self, len: u64) -> std::result::Result<(), std::io::Error> {
            self.inner.set_len(len)
        }
        fn sync_data(&self) -> std::result::Result<(), std::io::Error> {
            self.inner.sync_data()
        }
        fn write(&self, offset: u64, data: &[u8]) -> std::result::Result<(), std::io::Error> {
            self.inner.write(offset, data)
        }
    }

    /// The arrival-index staleness check reads two table headers, not two
    /// tables (ADR-153's investigation).
    ///
    /// It compared the lengths by iterating both tables to the end, which on
    /// every open walked the whole oplog and the whole index through the page
    /// cache before the node served anything. Sixteen thousand kilobyte
    /// documents make an oplog of over 16 MiB; the check is run on a database
    /// opened with a 1 MiB cache, so a walk cannot be hidden in it, and the
    /// bytes the backend was asked for are asserted to be a small fraction of
    /// the table rather than the table.
    #[test]
    fn the_arrival_index_check_reads_headers_not_tables() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("k.redb");
        {
            let engine = super::Engine::open(&path).unwrap();
            let coll = engine.create_collection("shop", "orders").unwrap();
            let filler = "x".repeat(1_000);
            for _ in 0..16 {
                let docs =
                    (0..1_000).map(|i| bson::doc! { "n": i, "body": filler.clone() }).collect();
                engine.insert_many(&coll, docs).unwrap();
            }
        }
        let oplog_bytes = std::fs::metadata(&path).unwrap().len();
        assert!(oplog_bytes > 16 << 20, "the fixture must be larger than the cache by far");

        let read = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let file = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let backend = CountingBackend {
            inner: redb::backends::FileBackend::new(file).unwrap(),
            read: std::sync::Arc::clone(&read),
        };
        let db = Database::builder().set_cache_size(1 << 20).create_with_backend(backend).unwrap();
        // The open itself reads what it reads; only the check is measured.
        read.store(0, std::sync::atomic::Ordering::Relaxed);

        super::Engine::rebuild_arrival_index_if_stale(&db).unwrap();

        let checked = read.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            checked < 256 << 10,
            "the staleness check read {checked} bytes of a {oplog_bytes}-byte database: it \
             walked the tables rather than reading their headers"
        );
    }

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
                // something else. Empty parentheses are what makes a call
                // raw: `Engine::begin_write` takes the holder its hold is
                // measured under (ADR-159), so a call with nothing in them
                // is redb's.
                let raw = line.contains(".begin_write()");
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
                let Some(writer) = text.find("begin_write(") else { continue };
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
                let txn = engine.begin_write(WriterHolder::Bulk).unwrap();
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
                let txn = engine.begin_write(WriterHolder::Bulk).unwrap();
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
                    .write_batch(WriterHolder::Bulk, |scope| {
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

    /// Documents and index entries filed under a collection id, whether or
    /// not a collection stands over them.
    fn rows_under(engine: &Engine, id: CollectionId) -> (usize, usize) {
        let txn = engine.db().begin_read().unwrap();
        let docs = txn.open_table(tables::DOCS).unwrap();
        let indexes = txn.open_table(tables::INDEX_ENTRIES).unwrap();
        (
            docs.range(doc_range(id)).unwrap().count(),
            indexes.range(index_range(id)).unwrap().count(),
        )
    }

    /// A collection larger than one chunk, with an index and a vector shadow
    /// beside it: what a drop has to clear.
    fn a_collection_of_more_than_one_chunk(engine: &Engine) -> (CollectionMeta, CollectionMeta) {
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine
            .create_index(
                "shop",
                "orders",
                vec![kimmy_core::IndexField::ascending("v")],
                false,
                None,
            )
            .unwrap();
        let shadow =
            engine.create_system_collection("shop", &vector_meta::shadow_name("orders")).unwrap();
        let rows = DROP_PURGE_CHUNK * 2 + 7;
        engine
            .insert_many(
                &coll,
                (0..rows).map(|i| bson::doc! { "_id": i as i64, "v": i as i64 }).collect(),
            )
            .unwrap();
        engine
            .insert_many(&shadow, (0..rows).map(|i| bson::doc! { "_id": i as i64 }).collect())
            .unwrap();
        (coll, shadow)
    }

    /// A drop clears everything the collection held however many chunks that
    /// takes — documents, index entries, and the vector shadow, which is the
    /// slower half of the drop this change was made for. A chunked drop that
    /// stopped short would leave the residue a collection recreated under the
    /// same name inherits, since the name derives the id.
    #[test]
    fn a_drop_of_more_than_one_chunk_leaves_nothing_behind_including_its_shadow() {
        let (engine, _dir) = engine();
        let (coll, shadow) = a_collection_of_more_than_one_chunk(&engine);
        assert!(rows_under(&engine, coll.id).1 > DROP_PURGE_CHUNK, "an index worth clearing");

        assert!(engine.drop_collection("shop", "orders").unwrap());

        assert!(engine.get_collection("shop", "orders").is_err());
        assert!(engine.get_collection("shop", &shadow.name).is_err(), "the shadow went too");
        for id in [coll.id, shadow.id] {
            assert_eq!(rows_under(&engine, id), (0, 0), "rows left under {id}");
            assert!(engine.collection_dropped_at(id).unwrap().is_some(), "no tombstone for {id}");
        }
    }

    /// **The ordering the whole change turns on.** The tombstone and the
    /// removal of the definition are the drop's *first* commit, before a
    /// single document goes: from that instant this node answers every
    /// question about the collection the way ADR-155 expects, for the whole
    /// length of the purge. Recorded at the end instead, a drop would spend
    /// its length as a collection this node still holds and a peer holds more
    /// of — a count divergence, then a repair, then the collection back.
    #[test]
    fn a_drop_records_its_tombstone_before_it_removes_the_first_document() {
        let (engine, _dir) = engine();
        let (coll, shadow) = a_collection_of_more_than_one_chunk(&engine);
        let held = rows_under(&engine, coll.id);
        let before = engine.commits();

        let buried = engine.bury_collection("shop", "orders", None).unwrap().expect("dropped");

        assert_eq!(engine.commits() - before, 1, "the first stage is one commit");
        assert_eq!(buried, vec![coll.id, shadow.id], "the shadow is buried with its parent");
        for id in [coll.id, shadow.id] {
            assert!(
                engine.collection_dropped_at(id).unwrap().is_some(),
                "no tombstone for {id} before the first chunk"
            );
        }
        assert!(engine.get_collection("shop", "orders").is_err(), "gone, not thinned out");
        assert!(engine.list_collections("shop").unwrap().is_empty());
        assert_eq!(engine.all_collection_ids().unwrap(), Default::default());
        assert_eq!(rows_under(&engine, coll.id), held, "and not one document removed yet");
    }

    /// The purge is a commit per chunk, and the writer is released between
    /// chunks (ADR-151's shape, ADR-158's drop): a collection of any size
    /// costs as many short transactions as it takes, never one long one, so
    /// the client writes queued behind it wait a chunk rather than a drop.
    #[test]
    fn the_writer_is_released_between_the_chunks_of_a_drop() {
        let (engine, _dir) = engine();
        let (coll, shadow) = a_collection_of_more_than_one_chunk(&engine);
        // The chunk budget is spent per collection id — documents first, then
        // whatever of the chunk the index entries can have — so each of the
        // two ids ends in a part-full chunk of its own.
        let chunks: usize = [coll.id, shadow.id]
            .into_iter()
            .map(|id| {
                let (docs, indexes) = rows_under(&engine, id);
                (docs + indexes).div_ceil(DROP_PURGE_CHUNK)
            })
            .sum();
        assert!(chunks > 4, "a drop worth chunking: {chunks} chunks");
        let before = engine.commits();
        let waits = engine.writer_wait().count;

        assert!(engine.drop_collection("shop", "orders").unwrap());

        let commits = (engine.commits() - before) as usize;
        assert_eq!(
            commits,
            chunks + 1,
            "the burial's commit and one per chunk of {DROP_PURGE_CHUNK}"
        );
        assert_eq!(
            engine.writer_wait().count - waits,
            commits as u64,
            "each chunk queued for the writer on its own, so each let go of it"
        );
    }

    /// A drop interrupted between chunks is a drop, not a half-collection:
    /// the definition and the tombstone were durable before the first chunk,
    /// so what a restart finds is rows under an id nothing resolves. The next
    /// start finishes the removal, which is all that is left of the drop —
    /// there is nothing to replay, the drop has already replicated.
    #[test]
    fn a_drop_interrupted_between_chunks_is_finished_by_the_next_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (coll, shadow) = {
            let engine = Engine::open(&path).unwrap();
            let (coll, shadow) = a_collection_of_more_than_one_chunk(&engine);
            engine.bury_collection("shop", "orders", None).unwrap().expect("dropped");
            // One chunk, then the process stops.
            assert_eq!(engine.purge_chunk(coll.id).unwrap(), DROP_PURGE_CHUNK);
            assert!(engine.get_collection("shop", "orders").is_err(), "gone, not half-held");
            assert!(rows_under(&engine, coll.id).0 > 0, "the fixture must leave rows behind");

            // What the next start names before it does the work, which is
            // all an operator has to tell a long start from a hung one: both
            // ids, and how many rows each still owes.
            let owed: std::collections::BTreeMap<CollectionId, usize> =
                engine.drops_left_unfinished().unwrap().into_iter().collect();
            for id in [coll.id, shadow.id] {
                let (docs, indexes) = rows_under(&engine, id);
                assert_eq!(owed.get(&id), Some(&(docs + indexes)), "what is owed under {id}");
            }
            assert_eq!(owed.len(), 2, "and nothing else: {owed:?}");
            (coll, shadow)
        };

        let engine = Engine::open(&path).unwrap();

        for id in [coll.id, shadow.id] {
            assert_eq!(rows_under(&engine, id), (0, 0), "rows left under {id} after a restart");
            assert!(engine.collection_dropped_at(id).unwrap().is_some(), "the tombstone survived");
        }
        assert!(engine.get_collection("shop", "orders").is_err(), "and the drop still stands");
    }

    /// A name created again between two chunks of its own drop keeps what it
    /// writes. The id is derived from the name, so the purge is working in
    /// the range the new collection now stands over; the check that no
    /// collection stands under the id is made in the transaction that does
    /// the removing, so a creation either lands before a chunk sees it or
    /// after that chunk has finished.
    #[test]
    fn a_collection_created_again_between_two_chunks_of_its_drop_keeps_its_documents() {
        let (engine, _dir) = engine();
        let (coll, _shadow) = a_collection_of_more_than_one_chunk(&engine);
        engine.bury_collection("shop", "orders", None).unwrap().expect("dropped");
        assert_eq!(engine.purge_chunk(coll.id).unwrap(), DROP_PURGE_CHUNK, "one chunk gone");

        // The recreation clears what the drop had not reached, so the new
        // incarnation starts empty rather than over a previous life's rows.
        let again = engine.create_collection("shop", "orders").unwrap();
        assert_eq!(again.id, coll.id, "a derived id is stable across drop and recreate");
        assert_eq!(rows_under(&engine, again.id), (0, 0), "nothing inherited");
        engine.insert(&again, bson::doc! { "_id": 1, "v": 1 }).unwrap();

        // What is left of the drop now runs on a live collection and must
        // take nothing.
        assert_eq!(engine.purge_dropped_collection(coll.id).unwrap(), 0);

        assert_eq!(engine.count(&again).unwrap(), 1, "the new incarnation's own document");
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
        let hold = engine.hold_writer(WriterHolder::Bulk);
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

    /// A drop is done when it is buried, and the purge behind it is a
    /// removal this node owes itself: a chunk that gives up waiting for the
    /// writer inside the caller's budget (ADR-151) leaves the drop standing
    /// and the rows for the next retention pass or the next start, rather than
    /// reporting a failed drop whose retry would answer `dropped: false` over
    /// rows still on disk. This one pins the start.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_chunk_that_cannot_take_the_writer_leaves_the_drop_standing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let coll = {
            let engine = Arc::new(Engine::open(&path).unwrap());
            let coll = engine.create_collection("shop", "orders").unwrap();
            for i in 0..5i64 {
                engine.insert(&coll, bson::doc! { "_id": i }).unwrap();
            }
            engine.bury_collection("shop", "orders", None).unwrap().expect("dropped");

            let hold = engine.hold_writer(WriterHolder::Bulk);
            let budget = std::time::Duration::from_millis(100);
            let owed = {
                let engine = Arc::clone(&engine);
                tokio::spawn(with_write_wait_budget(budget, async move {
                    engine.purge_what_the_drop_left(coll.id, "shop", "orders")
                }))
                .await
                .unwrap()
            };
            drop(hold);

            assert!(owed.is_ok(), "the drop must not be reported as failed: {owed:?}");
            assert_eq!(engine.writer_wait_timeouts(), 1, "a chunk did give up");
            assert!(engine.get_collection("shop", "orders").is_err(), "the drop stands");
            assert!(engine.collection_dropped_at(coll.id).unwrap().is_some(), "and its tombstone");
            assert_eq!(rows_under(&engine, coll.id).0, 5, "with the rows still owed");
            coll
        };

        assert_eq!(rows_under(&Engine::open(&path).unwrap(), coll.id), (0, 0), "the next start");
    }

    /// The probe reading counts what `count_by_id` counts — live documents, a
    /// tombstone not among them — and hands back the witnessed vector beside
    /// it, both from one snapshot (ADR-168's limitation). An id this node does
    /// not hold is `None`, not zero.
    #[test]
    fn the_count_probe_reading_counts_what_count_by_id_counts() {
        let (engine, _dir) = engine();
        let coll = engine.create_collection("app", "c").unwrap();
        for i in 0..5 {
            engine.insert(&coll, bson::doc! { "_id": format!("d{i}") }).unwrap();
        }
        assert!(engine.delete(&coll, &kimmy_core::DocId::String("d1".into())).unwrap());

        let (vector, count) = engine.count_probe_reading(coll.id).unwrap();
        assert_eq!(count, Some(4), "a tombstone is not a document");
        assert_eq!(count, engine.count_by_id(coll.id).unwrap());
        assert_eq!(vector, engine.witnessed_vector().unwrap());
        assert_eq!(engine.count_probe_reading(CollectionId(0x5eed)).unwrap().1, None);
    }

    /// The same owed purge on a node that stays up: the next retention pass
    /// finishes it, without a restart (ADR-158's addendum). More than a chunk
    /// is owed, under the collection and its vector shadow both, and the
    /// tombstones stay — they are not due, and they mark the drop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_drop_left_owed_by_a_busy_writer_is_finished_by_the_next_retention_pass() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let (coll, shadow) = a_collection_of_more_than_one_chunk(&engine);
        engine.bury_collection("shop", "orders", None).unwrap().expect("dropped");

        let hold = engine.hold_writer(WriterHolder::Bulk);
        let budget = std::time::Duration::from_millis(100);
        let owed = {
            let engine = Arc::clone(&engine);
            tokio::spawn(with_write_wait_budget(budget, async move {
                engine.purge_what_the_drop_left(coll.id, "shop", "orders")
            }))
            .await
            .unwrap()
        };
        drop(hold);
        assert!(owed.is_ok(), "the drop must not be reported as failed: {owed:?}");
        assert_eq!(engine.writer_wait_timeouts(), 1, "a chunk did give up");
        let rows: usize = [coll.id, shadow.id]
            .into_iter()
            .map(|id| {
                let (docs, indexes) = rows_under(&engine, id);
                docs + indexes
            })
            .sum();
        assert!(rows > DROP_PURGE_CHUNK, "more than a chunk owed: {rows}");

        let policy = crate::RetentionPolicy::new(24 * 60 * 60, 24 * 60 * 60);
        let outcome = engine.collect_garbage(policy).unwrap();

        assert_eq!(outcome.dropped_rows_removed, rows, "{outcome:?}");
        for id in [coll.id, shadow.id] {
            assert_eq!(rows_under(&engine, id), (0, 0), "rows left under {id} after the pass");
            assert!(engine.collection_dropped_at(id).unwrap().is_some(), "the tombstone stays");
        }
        assert!(engine.get_collection("shop", "orders").is_err(), "the drop still stands");
        assert_eq!(
            engine.collect_garbage(policy).unwrap().dropped_rows_removed,
            0,
            "and the pass after owes nothing"
        );
    }

    /// The holder set is a metric label, so its shape is load-bearing: a
    /// row per holder, in declaration order, and a word per row that a
    /// Prometheus label can carry and an operator can read.
    #[test]
    fn every_holder_has_its_own_row_and_its_own_word() {
        let mut seen = std::collections::BTreeSet::new();
        for (row, holder) in WriterHolder::ALL.iter().enumerate() {
            assert_eq!(holder.slot(), row, "{holder:?} indexes a row that is not its own");
            let label = holder.label();
            assert!(
                !label.is_empty()
                    && label.chars().all(|c| c.is_ascii_lowercase() || c == '_')
                    && !label.starts_with('_'),
                "{label:?} is not a label a scrape can carry"
            );
            assert!(seen.insert(label), "two holders share the word {label:?}");
        }
        assert_eq!(WriterHolder::ALL.len(), WriterHolder::COUNT);
    }

    /// A label nothing fills is worse than no label: the documentation
    /// promises a dimension an operator can split on, and the split comes
    /// back empty for ever with nothing to say the path was never named.
    ///
    /// So every holder has to be named by some path in this crate that
    /// takes the writer, checked against the source the way
    /// `commits_are_counted_at_one_chokepoint` checks its own invariant —
    /// each file read only as far as its test module, since a test may
    /// take the writer under any holder it likes to build a fixture.
    #[test]
    fn every_holder_is_named_by_a_path_that_takes_the_writer() {
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

        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut paths = Vec::new();
        sources(&src, &mut paths);
        assert!(paths.len() > 10, "the scan of this crate's sources broke: {paths:?}");

        let mut named = String::new();
        for path in paths {
            let whole = std::fs::read_to_string(&path).unwrap();
            let body = match whole.find("#[cfg(test)]\nmod tests") {
                Some(at) => &whole[..at],
                None => &whole[..],
            };
            named.push_str(body);
        }

        let unnamed: Vec<&str> = WriterHolder::ALL
            .iter()
            .filter(|holder| !named.contains(&format!("WriterHolder::{holder:?}")))
            .map(|holder| holder.label())
            .collect();
        assert!(
            unnamed.is_empty(),
            "these holders are on `/metrics` and in the operations guide, and no write path \
             names one: {unnamed:?}. Either a path lost its holder or the set has a word in it \
             that describes nothing"
        );
    }

    /// The bucket the operations guide tells an operator to alert on is the
    /// threshold the `WARN` fires at, so a hold counted above that bound has
    /// a log line naming the same holder. Two constants, one promise.
    #[test]
    fn the_warned_hold_is_a_bucket_boundary() {
        let warn_us = u64::try_from(WRITER_HOLD_WARN.as_micros()).unwrap();
        assert!(
            WRITER_HOLD_BUCKETS_US.contains(&warn_us),
            "{WRITER_HOLD_WARN:?} is not a bound of {WRITER_HOLD_BUCKETS_US:?}, so the bucket \
             the guide names and the line the log writes no longer agree"
        );
    }

    /// The measurement ADR-159 exists for: a hold lands in its own holder's
    /// row and in no other, with the ADR-151 maximum still moving beside it.
    #[test]
    fn a_hold_is_recorded_against_what_held_the_writer() {
        let (engine, _dir) = engine();
        let coll = engine.create_collection("app", "docs").unwrap();

        let before = engine.writer_hold();
        engine.insert(&coll, bson::doc! { "_id": 1i64 }).unwrap();
        let after = engine.writer_hold();

        let write = WriterHolder::Write.slot();
        assert_eq!(
            after.count[write],
            before.count[write] + 1,
            "one document from a client is one hold under `write`"
        );
        assert!(after.sum_us[write] >= before.sum_us[write], "the hold was timed");
        for holder in WriterHolder::ALL {
            if holder == WriterHolder::Write {
                continue;
            }
            assert_eq!(
                after.count[holder.slot()],
                before.count[holder.slot()],
                "an insert moved {}'s row",
                holder.label()
            );
        }

        // Two holders on one engine stay apart, which is the whole point:
        // a drop and a client write were one number before this.
        let drops = WriterHolder::Drop.slot();
        engine.drop_collection("app", "docs").unwrap();
        let dropped = engine.writer_hold();
        assert!(dropped.count[drops] >= 1, "the drop is its own holder");
        assert_eq!(dropped.count[write], after.count[write], "and it is not a client's write");

        // ADR-151's since-start maximum is unchanged by any of this.
        assert!(engine.writer_hold_max() > std::time::Duration::ZERO);
    }

    /// ADR-158 split a collection drop into a burial and a purge, and
    /// ADR-159's rule splits their attribution with them: the burial writes
    /// metadata and is `ddl`, every chunk of the purge is `drop`.
    ///
    /// The two are checked apart rather than together because folding them
    /// would be the easy mistake and an invisible one — the burial is the
    /// transaction whose cost does *not* grow with the collection, and
    /// counting it as a drop would put an O(1) hold in the row an operator
    /// reads to find out what is churning through the writer. Chunk counts
    /// are the assertion for the same reason: after chunking, what names a
    /// large drop is a `drop` count in the thousands beside a flat `ddl`.
    #[test]
    fn a_chunked_drop_buries_under_ddl_and_purges_under_drop() {
        let (engine, _dir) = engine();
        let (coll, _shadow) = a_collection_of_more_than_one_chunk(&engine);

        let ddl = WriterHolder::Ddl.slot();
        let drops = WriterHolder::Drop.slot();
        let before = engine.writer_hold();

        engine.drop_collection("shop", "orders").unwrap();
        let after = engine.writer_hold();

        assert_eq!(
            after.count[ddl] - before.count[ddl],
            1,
            "the burial is one metadata transaction, and the only one"
        );
        // Two collections of `DROP_PURGE_CHUNK * 2 + 7` rows, the shadow
        // among them, plus the index entries under the parent: more chunks
        // than either collection has on its own, and every one of them here.
        assert!(
            after.count[drops] - before.count[drops] >= 4,
            "each chunk of the purge is its own hold: {} chunks",
            after.count[drops] - before.count[drops]
        );
        assert_eq!(rows_under(&engine, coll.id), (0, 0), "the fixture really did purge");
    }

    /// The sweep that finishes an interrupted drop runs inside
    /// `Engine::open`, on an engine that is fully constructed by then, so its
    /// chunks are attributed like any other purge — and are already on the
    /// first scrape of a member that has served nothing yet.
    ///
    /// That is the reading an operator wants from a member whose start took a
    /// minute, and it is the only series that offers it. It also puts the
    /// line through `open` where `commits_are_counted_at_one_chokepoint`
    /// puts it: the migrations and index rebuilds above write on the raw
    /// database and stay outside this accounting, the sweep takes the gate
    /// and is inside it.
    #[test]
    fn a_drop_finished_at_the_next_start_is_attributed_to_that_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let coll = {
            let engine = Engine::open(&path).unwrap();
            let (coll, _shadow) = a_collection_of_more_than_one_chunk(&engine);
            engine.bury_collection("shop", "orders", None).unwrap().expect("dropped");
            assert_eq!(engine.purge_chunk(coll.id).unwrap(), DROP_PURGE_CHUNK);
            assert!(rows_under(&engine, coll.id).0 > 0, "the fixture must leave rows owed");
            coll
        };

        let restarted = Engine::open(&path).unwrap();
        let hold = restarted.writer_hold();
        assert!(
            hold.count[WriterHolder::Drop.slot()] >= 1,
            "the sweep's chunks are on the engine it was returned with"
        );
        assert_eq!(
            hold.count[WriterHolder::Ddl.slot()],
            0,
            "and the burial was another process's; this start buried nothing"
        );
        assert_eq!(rows_under(&restarted, coll.id), (0, 0), "the sweep did finish it");
    }

    /// The barrier's own flush holds the writer without opening a counted
    /// transaction, so it was the one hold nothing measured at all
    /// (ADR-159). Under `coalesced` it is a holder like any other.
    #[test]
    fn the_barrier_flush_is_a_holder_rather_than_an_unmeasured_hold() {
        use bson::doc;
        let (engine, _dir) = engine();
        engine.set_durability(DurabilityClass::Coalesced, std::time::Duration::from_millis(5));
        let coll = engine.create_collection("app", "c").unwrap();

        let durability = WriterHolder::Durability.slot();
        let before = engine.writer_hold().count[durability];
        for i in 0..5i64 {
            engine.insert(&coll, doc! {"_id": i}).unwrap();
        }
        assert!(
            engine.writer_hold().count[durability] > before,
            "a shared flush held the writer and said so"
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
