//! The drop purger: what a collection drop held is removed here, after the
//! drop has answered (ADR-189).
//!
//! A drop is its burial ([`Engine::bury_collection`]): one short transaction
//! removes the definition, records the tombstone and mints the entry, and from
//! that commit the collection is gone to every reader and every peer
//! (ADR-158). What it held is left under an id nothing resolves, and this
//! module removes it, a chunk per commit, on one supervised task. Before, the
//! drop's caller removed it: an HTTP `DELETE` answered only when the last row
//! went, and a replicated drop ran the whole removal inside the replication
//! round, so a member pulled nothing, for any database, for as long as it took
//! — about 120 s for 400,000 documents.
//!
//! **Nothing else removes those rows while the node runs.** A creation of the
//! same name used to finish the removal itself, which only moved the wait to
//! the creation; it is refused while rows remain
//! ([`crate::StorageError::CollectionPurging`]) and asks for this id to go next. The
//! retention pass used to finish an owed drop too, and could join one in
//! progress; it now asks this purger to look instead.
//!
//! **The queue is a hint, not a record.** The burial is durable before its ids
//! are handed over, so a crash anywhere leaves ADR-158's state — rows under an
//! id with no collection standing over it — which [`Engine::owed_purges`] finds
//! again: at the purger's start, every [`OWED_CHECK`], and whenever the
//! retention pass asks. The tombstone that marks such rows is not collected
//! while they remain (gc.rs), so the marker outlasts them.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_core::CollectionId;
use redb::{ReadableDatabase, ReadableTable};
use tracing::info;

use crate::engine::{DROP_PURGE_CHUNK, Engine};
use crate::error::Result;
use crate::tables;

/// How often the purger wakes with nothing handed to it: the idle turn that
/// counts as progress when nothing is owed (ADR-187).
pub const IDLE_TICK: Duration = Duration::from_secs(5);

/// How often the purger looks for owed rows it was not handed: a drop from
/// before a restart, or rows whose tombstone an older collector removed.
pub const OWED_CHECK: Duration = Duration::from_secs(60);

/// The purger's shared state, one per engine.
#[derive(Default)]
pub(crate) struct Purges {
    /// Ids to purge, in order. An id is in it at most once.
    queue: parking_lot::Mutex<VecDeque<CollectionId>>,
    /// Set when something moved an id to the front, so the purger yields the
    /// one it is on at the next chunk boundary.
    jumped: std::sync::atomic::AtomicBool,
    /// Set when the retention pass asks for an owed check.
    pub(crate) owed_check_asked: std::sync::atomic::AtomicBool,
    wake: tokio::sync::Notify,
    counters: Arc<PurgeCounters>,
    /// When the purger last looked for owed rows. Here rather than in the
    /// purger's loop, so a restart by `Retry` after a failure does not look
    /// again at once and call that progress.
    last_owed_check: parking_lot::Mutex<Option<Instant>>,
    /// Set by a chunk that failed, cleared by the next that commits. While it
    /// is set nothing marks progress: retrying is not progress (ADR-187), and
    /// a purger failing every chunk must read as old.
    failing: std::sync::atomic::AtomicBool,
    /// A test's hold on this engine's purge chunks.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) gate: PurgeGate,
}

/// What `/metrics` reads of the purger.
#[derive(Default)]
pub struct PurgeCounters {
    /// When the purger last made progress: a chunk committed, an owed check
    /// finished, or an idle turn with nothing queued and nothing owed.
    last_progress: parking_lot::Mutex<Option<Instant>>,
    /// Set while a purge chunk waits for the single writer, from just before
    /// `begin_write` until it returns: the wait alone, not the chunk's own
    /// work or its commit.
    at_writer: std::sync::atomic::AtomicBool,
}

impl PurgeCounters {
    /// When the purger last made progress; `None` before it first did.
    ///
    /// **While it waits for the writer, now.** A purger queued behind another
    /// holder — an index build files a whole collection in one transaction,
    /// and replicated DDL waits with no budget — is not stuck, and an age that
    /// climbed through that wait would fire the alert on a healthy node. A
    /// writer that is never released is reported by the writer's own series
    /// (`kimmy_write_lock_wait_seconds`, `kimmy_write_lock_held_seconds`), not
    /// by this one. Only the wait: a chunk that has the writer and never
    /// commits reads old, as it should.
    pub fn last_progress(&self) -> Option<Instant> {
        if self.at_writer.load(std::sync::atomic::Ordering::Relaxed) {
            return Some(Instant::now());
        }
        *self.last_progress.lock()
    }

    fn progressed(&self) {
        *self.last_progress.lock() = Some(Instant::now());
    }

    pub(crate) fn waiting_for_writer(&self, waiting: bool) {
        self.at_writer.store(waiting, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Engine {
    /// The purger's counters, for `/metrics`.
    pub fn purge_counters(&self) -> Arc<PurgeCounters> {
        Arc::clone(&self.purges.counters)
    }

    /// Hand what a burial left to the purger, after the burial committed.
    pub(crate) fn hand_over_purges(&self, ids: &[CollectionId]) {
        {
            let mut queue = self.purges.queue.lock();
            for id in ids {
                if !queue.contains(id) {
                    queue.push_back(*id);
                }
            }
        }
        self.purges.wake.notify_one();
    }

    /// Move `id` to the front of the purge queue, adding it if it is absent.
    ///
    /// Called wherever a creation meets rows under the id it derives. Adding
    /// is the point as much as moving: rows whose tombstone an older collector
    /// removed are found by no owed check, and would block the name for good
    /// if only a queued id could be moved.
    pub fn prioritise_purge(&self, id: CollectionId) {
        {
            let mut queue = self.purges.queue.lock();
            queue.retain(|queued| *queued != id);
            queue.push_front(id);
        }
        self.purges.jumped.store(true, std::sync::atomic::Ordering::Relaxed);
        self.purges.wake.notify_one();
    }

    /// Ask the purger for an owed check. The retention pass calls this where
    /// it used to finish owed drops itself.
    pub fn ask_for_owed_check(&self) {
        self.purges.owed_check_asked.store(true, std::sync::atomic::Ordering::Relaxed);
        self.purges.wake.notify_one();
    }

    /// Whether a creation under `id` must wait for the purger: no collection
    /// stands under the id, and rows are filed under it.
    ///
    /// A collection standing under it is not a pending purge: its rows are its
    /// own, and a creation of that name is refused as a conflict.
    pub fn purge_pending(&self, id: CollectionId) -> Result<bool> {
        let txn = self.db().begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        if crate::engine::collection_stands_under(&collections, id)? {
            return Ok(false);
        }
        drop(collections);
        drop(txn);
        Ok(!self.collection_range_is_empty(id)?)
    }

    /// Every id a drop left rows under: a tombstone with no collection
    /// standing over it, and something still filed beneath. Found, not
    /// counted: two seeks per dropped id, one read transaction.
    pub fn owed_purges(&self) -> Result<Vec<CollectionId>> {
        let live: std::collections::HashSet<CollectionId> =
            self.all_collections()?.into_iter().map(|c| c.id).collect();
        let txn = self.db().begin_read()?;
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
            if docs.range(crate::engine::doc_range(id))?.next().is_some()
                || indexes.range(crate::engine::index_range(id))?.next().is_some()
            {
                owed.push(id);
            }
        }
        Ok(owed)
    }

    /// Run the drop purger until it fails; it never returns `Ok`.
    ///
    /// Driven by `kimmy_task::Retry::forever` under supervision in the daemon,
    /// and directly by tests. An error re-queues the id it was on, at the
    /// back, and is returned for the retry to count and back off; nothing
    /// about a failure is progress. Stopped by being dropped, which happens at
    /// an `await`: a chunk runs to its commit under `blocking`, so a stop
    /// lands between chunks, where the rows left are ADR-158's state.
    pub async fn run_drop_purger(self: Arc<Self>) -> Result<()> {
        loop {
            // Registered before the queue is read, so a hand-over between the
            // read and the wait still wakes it.
            let woken = self.purges.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();

            let asked =
                self.purges.owed_check_asked.swap(false, std::sync::atomic::Ordering::Relaxed);
            let mut owed_found = false;
            let due =
                self.purges.last_owed_check.lock().is_none_or(|at| at.elapsed() >= OWED_CHECK);
            if asked || due {
                let owed = crate::blocking(|| self.owed_purges())?;
                let mut queue = self.purges.queue.lock();
                for id in owed {
                    owed_found = true;
                    if !queue.contains(&id) {
                        info!(
                            collection = %id,
                            "a collection drop left rows behind; the drop purger is removing them"
                        );
                        queue.push_back(id);
                    }
                }
                drop(queue);
                *self.purges.last_owed_check.lock() = Some(Instant::now());
                self.mark_progress();
            }

            let mut purged_any = false;
            while let Some(id) = self.next_purge() {
                purged_any = true;
                self.purge_one(id).await?;
            }

            // An idle turn is progress only when there is truly nothing to do.
            if !purged_any && !owed_found && self.purges.queue.lock().is_empty() {
                self.mark_progress();
            }
            tokio::select! {
                () = &mut woken => {}
                () = tokio::time::sleep(IDLE_TICK) => {}
            }
        }
    }

    /// Progress, unless the last chunk failed: nothing the purger does while
    /// it is failing counts until a chunk commits again.
    fn mark_progress(&self) {
        if !self.purges.failing.load(std::sync::atomic::Ordering::Relaxed) {
            self.purges.counters.progressed();
        }
    }

    fn next_purge(&self) -> Option<CollectionId> {
        self.purges.jumped.store(false, std::sync::atomic::Ordering::Relaxed);
        self.purges.queue.lock().pop_front()
    }

    /// Purge `id` a chunk at a time until its ranges are empty, a collection
    /// stands over it again, or another id is moved ahead of it.
    async fn purge_one(&self, id: CollectionId) -> Result<()> {
        let started = Instant::now();
        let mut removed = 0usize;
        loop {
            // On entering the chunk, so a purger queued at the writer starts
            // from a fresh mark (the wait itself is held by `at_writer`).
            self.mark_progress();
            let chunk = crate::blocking(|| self.purge_chunk(id));
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(e) => {
                    self.purges.failing.store(true, std::sync::atomic::Ordering::Relaxed);
                    let mut queue = self.purges.queue.lock();
                    if !queue.contains(&id) {
                        queue.push_back(id);
                    }
                    return Err(e);
                }
            };
            self.purges.failing.store(false, std::sync::atomic::Ordering::Relaxed);
            self.purges.counters.progressed();
            removed += chunk;
            // Short of a full chunk: the ranges are exhausted, or a collection
            // stands under the id again and the chunk's guard removed nothing.
            if chunk < DROP_PURGE_CHUNK {
                break;
            }
            // Between chunks: where a stop lands, and where a creation that
            // is waiting on another id gets it next.
            tokio::task::yield_now().await;
            if self.purges.jumped.load(std::sync::atomic::Ordering::Relaxed) {
                let mut queue = self.purges.queue.lock();
                if !queue.contains(&id) {
                    let at = queue.len().min(1);
                    queue.insert(at, id);
                }
                return Ok(());
            }
        }
        if removed > 0 {
            info!(
                collection = %id,
                rows = removed,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "removed what a dropped collection held"
            );
        }
        Ok(())
    }

    /// Log each owed id at start, without purging: the purger finishes them
    /// once the node serves (ADR-158's second addendum).
    pub(crate) fn announce_owed_purges(&self) -> Result<()> {
        for id in self.owed_purges()? {
            info!(
                collection = %id,
                "a collection drop was interrupted; the drop purger finishes it once this node \
                 is serving"
            );
        }
        Ok(())
    }
}

/// A test's hold on one engine's purge chunks: open, closed, or letting a
/// number of chunks through. Per engine, so a peer's own drop in the same test
/// is not held, and tests running in parallel do not share it.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default)]
pub(crate) struct PurgeGate {
    state: parking_lot::Mutex<GateState>,
    changed: parking_lot::Condvar,
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
enum GateState {
    #[default]
    Open,
    /// This many more chunks may pass; zero is closed.
    Allow(usize),
    /// The next chunk fails with an injected error, then the gate is open.
    FailNext,
    /// Every chunk fails with an injected error until the gate is set again.
    FailAlways,
}

/// How long a chunk waits at a closed gate before the test is declared broken.
#[cfg(any(test, feature = "test-hooks"))]
const GATE_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(any(test, feature = "test-hooks"))]
impl PurgeGate {
    fn set(&self, state: GateState) {
        *self.state.lock() = state;
        self.changed.notify_all();
    }

    /// Wait until a chunk may pass. Panics if a test never opens it, so a
    /// mistake fails the test rather than hanging it.
    pub(crate) fn pass(&self) -> Result<()> {
        let mut state = self.state.lock();
        let deadline = Instant::now() + GATE_TIMEOUT;
        loop {
            match *state {
                GateState::Open => return Ok(()),
                GateState::FailNext => {
                    *state = GateState::Open;
                    return Err(crate::sync::count_hooks::injected());
                }
                GateState::FailAlways => return Err(crate::sync::count_hooks::injected()),
                GateState::Allow(n) if n > 0 => {
                    *state = GateState::Allow(n - 1);
                    self.changed.notify_all();
                    return Ok(());
                }
                GateState::Allow(_) => {
                    if self.changed.wait_until(&mut state, deadline).timed_out() {
                        panic!(
                            "the purge gate on this engine was never opened: a chunk waited {} s",
                            GATE_TIMEOUT.as_secs()
                        );
                    }
                }
            }
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Engine {
    /// Hold every purge chunk on this engine until opened.
    pub fn close_purge_gate(&self) {
        self.purges.gate.set(GateState::Allow(0));
    }

    /// Let purge chunks on this engine through.
    pub fn open_purge_gate(&self) {
        self.purges.gate.set(GateState::Open);
    }

    /// Let `n` more chunks through, then hold the rest.
    pub fn allow_purge_chunks(&self, n: usize) {
        self.purges.gate.set(GateState::Allow(n));
    }

    /// Fail the next chunk with an injected error, then open.
    pub fn fail_next_purge_chunk(&self) {
        self.purges.gate.set(GateState::FailNext);
    }

    /// Fail every chunk with an injected error until the gate is set again.
    pub fn fail_every_purge_chunk(&self) {
        self.purges.gate.set(GateState::FailAlways);
    }

    /// Do on this thread what the drop purger would, to the end: every
    /// queued id, then every owed one. For a test in which the purge having
    /// finished is a premise rather than the subject. Returns the rows removed.
    pub fn finish_purges_now(&self) -> Result<usize> {
        let mut ids: Vec<CollectionId> = self.purges.queue.lock().drain(..).collect();
        for id in self.owed_purges()? {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        let mut rows = 0;
        for id in ids {
            rows += self.purge_dropped_collection(id)?;
        }
        Ok(rows)
    }

    /// The ids queued for the purger, in order.
    pub fn queued_purges(&self) -> Vec<CollectionId> {
        self.purges.queue.lock().iter().copied().collect()
    }

    /// Rows filed under `id`, documents and index entries.
    pub fn rows_under(&self, id: CollectionId) -> Result<usize> {
        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        let indexes = txn.open_table(tables::INDEX_ENTRIES)?;
        let mut rows = 0;
        for row in docs.range(crate::engine::doc_range(id))? {
            row?;
            rows += 1;
        }
        for row in indexes.range(crate::engine::index_range(id))? {
            row?;
            rows += 1;
        }
        Ok(rows)
    }

    /// Remove `db.name`'s definition and its tombstone and leave its rows: the
    /// state an older collector left when it collected a tombstone whose drop
    /// still owed rows (ADR-158's residual, before its addendum).
    pub fn forget_collection_leaving_rows(&self, db: &str, name: &str) -> Result<CollectionId> {
        let meta = self.get_collection(db, name)?;
        let txn = self.begin_write(crate::engine::WriterHolder::Ddl)?;
        {
            let mut collections = txn.open_table(tables::COLLECTIONS)?;
            collections.remove((db, name))?;
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            dropped.remove(meta.id.0)?;
        }
        txn.commit()?;
        Ok(meta.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::DROP_PURGE_CHUNK;

    fn open(dir: &tempfile::TempDir) -> Arc<Engine> {
        Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap())
    }

    /// A collection of more than two chunks of documents.
    fn a_large_collection(engine: &Engine, name: &str) -> CollectionId {
        let coll = engine.create_collection("shop", name).unwrap();
        let rows = DROP_PURGE_CHUNK * 2 + 7;
        engine
            .insert_many(&coll, (0..rows).map(|i| bson::doc! { "_id": i as i64 }).collect())
            .unwrap();
        coll.id
    }

    /// Poll `done` every 10 ms for up to ten seconds; a condition that never
    /// holds fails the test rather than hanging it.
    async fn eventually(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(Instant::now() < deadline, "{what} did not happen within ten seconds");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_purger_removes_what_a_burial_handed_it() {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(&dir);
        let id = a_large_collection(&engine, "orders");
        engine.drop_collection("shop", "orders").unwrap();
        assert!(engine.rows_under(id).unwrap() > DROP_PURGE_CHUNK, "the drop left its rows");

        let purger = tokio::spawn(Arc::clone(&engine).run_drop_purger());
        eventually("the purge", || engine.rows_under(id).unwrap() == 0).await;
        assert!(engine.queued_purges().is_empty());
        assert!(engine.collection_dropped_at(id).unwrap().is_some(), "the tombstone stays");
        purger.abort();
    }

    /// ADR-187's age for the purger, with the writer wait apart from it (the
    /// second ruling's N4). Another holder has the single writer, as an index
    /// build does for a whole collection: the purger queued behind it reads
    /// fresh for as long as the hold lasts, and each committed chunk marks
    /// progress once the writer is free.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_purgers_age_holds_while_another_holder_has_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(&dir);
        let id = a_large_collection(&engine, "orders");
        let counters = engine.purge_counters();
        engine.drop_collection("shop", "orders").unwrap();

        let (held, is_held) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let holding = Arc::clone(&engine);
        let holder = std::thread::spawn(move || {
            let _hold = holding.hold_writer(crate::engine::WriterHolder::Bulk);
            held.send(()).unwrap();
            let _ = released.recv_timeout(Duration::from_secs(20));
        });
        is_held.recv().unwrap();

        let purger = tokio::spawn(Arc::clone(&engine).run_drop_purger());
        eventually("the purger waiting for the writer", || {
            counters.at_writer.load(std::sync::atomic::Ordering::Relaxed)
        })
        .await;
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let age = counters.last_progress().map(|at| at.elapsed());
        assert!(
            age.is_some_and(|age| age < Duration::from_millis(50)),
            "queued behind another holder reads fresh, not {age:?}"
        );
        assert!(engine.rows_under(id).unwrap() > 0, "and it removed nothing while it waited");

        let before = *counters.last_progress.lock();
        release.send(()).unwrap();
        holder.join().unwrap();
        eventually("the purge", || engine.rows_under(id).unwrap() == 0).await;
        assert!(*counters.last_progress.lock() > before, "the chunks marked progress");
        purger.abort();
    }

    /// A purger that fails every chunk reads old: neither its restarts by
    /// `Retry`, nor the owed check, nor entering the writer again count as
    /// progress while the last chunk failed (the delta review's M-b).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_purger_failing_every_chunk_shows_a_rising_age() {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(&dir);
        a_large_collection(&engine, "orders");
        engine.drop_collection("shop", "orders").unwrap();
        engine.fail_every_purge_chunk();
        let counters = engine.purge_counters();

        let first = Arc::clone(&engine).run_drop_purger().await;
        assert!(first.is_err(), "{first:?}");
        let stuck_at = *counters.last_progress.lock();
        for restart in 0..3 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let again = Arc::clone(&engine).run_drop_purger().await;
            assert!(again.is_err(), "restart {restart}: {again:?}");
            assert_eq!(
                *counters.last_progress.lock(),
                stuck_at,
                "restart {restart}: a failing purger made no progress"
            );
        }
        let age = counters.last_progress().map(|at| at.elapsed()).unwrap();
        assert!(age >= Duration::from_millis(150), "the age rose: {age:?}");

        engine.open_purge_gate();
        let purger = tokio::spawn(Arc::clone(&engine).run_drop_purger());
        eventually("the first commit's mark", || *counters.last_progress.lock() > stuck_at).await;
        purger.abort();
    }

    /// A failed chunk is not progress and loses nothing: its id goes back on
    /// the queue and the error is returned, for `Retry` to count and back off
    /// (the first review's M2). The purge then finishes on the next run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_chunk_puts_its_id_back_and_returns_the_error() {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(&dir);
        let id = a_large_collection(&engine, "orders");
        engine.drop_collection("shop", "orders").unwrap();
        let rows = engine.rows_under(id).unwrap();

        engine.fail_next_purge_chunk();
        let failed =
            tokio::time::timeout(Duration::from_secs(10), Arc::clone(&engine).run_drop_purger())
                .await
                .expect("a failed chunk returns at once");
        assert!(failed.is_err(), "{failed:?}");
        assert_eq!(engine.queued_purges(), vec![id], "the id is back on the queue");
        assert_eq!(engine.rows_under(id).unwrap(), rows, "and nothing was removed");

        let purger = tokio::spawn(Arc::clone(&engine).run_drop_purger());
        eventually("the purge on the next run", || engine.rows_under(id).unwrap() == 0).await;
        purger.abort();
    }

    /// Rows with no tombstone over them (ADR-158's residual before its
    /// addendum) are found by no owed check; a creation that meets them adds
    /// the id, and the purger removes them (the second ruling's N1).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_purger_removes_rows_with_no_tombstone_once_a_creation_asks() {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(&dir);
        a_large_collection(&engine, "orders");
        let id = engine.forget_collection_leaving_rows("shop", "orders").unwrap();
        let purger = tokio::spawn(Arc::clone(&engine).run_drop_purger());

        assert!(matches!(
            engine.create_collection("shop", "orders"),
            Err(crate::StorageError::CollectionPurging { .. })
        ));
        eventually("the purge", || engine.rows_under(id).unwrap() == 0).await;
        engine.create_collection("shop", "orders").unwrap();
        purger.abort();
    }

    /// Stopped mid-purge, the purger leaves ADR-158's state, which a
    /// restarted engine does not finish in `open` (the first review's M3) and
    /// its purger does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_purge_stopped_part_way_is_left_to_the_restarted_purger_and_not_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let id = {
            let engine = open(&dir);
            let id = a_large_collection(&engine, "orders");
            engine.allow_purge_chunks(1);
            engine.drop_collection("shop", "orders").unwrap();
            let rows = engine.rows_under(id).unwrap();
            let purger = tokio::spawn(Arc::clone(&engine).run_drop_purger());
            eventually("one chunk", || engine.rows_under(id).unwrap() == rows - DROP_PURGE_CHUNK)
                .await;
            // The stop lands at the purger's next `await`. The chunk parked
            // at the gate is synchronous and cannot be interrupted, so the
            // gate is opened after the abort: that chunk commits, and the
            // task stops at the yield after it, with rows still owed.
            purger.abort();
            engine.open_purge_gate();
            let _ = purger.await;
            assert!(engine.rows_under(id).unwrap() > 0, "the stop left rows owed");
            id
        };

        let engine = open(&dir);
        let owed = engine.rows_under(id).unwrap();
        assert!(owed > 0, "open removed nothing");
        let purger = tokio::spawn(Arc::clone(&engine).run_drop_purger());
        eventually("the purge after the restart", || engine.rows_under(id).unwrap() == 0).await;
        purger.abort();
    }
}
