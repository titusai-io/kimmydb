//! Walks of the store that end at the node's stop.
//!
//! A walk holds the engine for as long as it reads, and the node's stop can
//! close the store only once nothing holds it (ADR-147's 0.40.1 addendum). A
//! walk serving a peer's pull held a stop up for as long as the oplog it read,
//! so the store was left for the next start to repair. Every walk that reads
//! more than a bounded number of rows is therefore opened through
//! [`open_walk_table`], whose iterators check the stop **before every row they
//! yield**, including rows a caller passes over, and yield
//! [`StorageError::Stopping`] once it has come. The check is a few atomic
//! loads: on a cold disk a row can be a seek, so a check every so many
//! rows could leave a stop waiting seconds.
//!
//! **Two scopes.** A [`WalkScope::Background`] walk serves no client: a pull
//! served to a peer, a retention pass, a backfill listing. It stops at the
//! signal. A [`WalkScope::Request`] walk answers a client, who is owed an
//! answer while the drain lets requests finish; it stops at the drain's
//! deadline, or when the storage has failed, which is when
//! [`Engine::is_stopping`] turns.
//!
//! **A write transaction's walks never stop.** It holds the single writer,
//! so `close_writes` bounds it: the stop waits for it up to its cap and
//! refuses a clean exit past that (ADR-192). Its tables are opened with
//! [`WalkStop::in_write`], and each place that does is listed in the guard.
//!
//! The guard in `tests/walks_stop.rs` holds the storage crate to this: every
//! function that iterates a table is listed as a stop-aware walk or as a
//! bounded read with its reason, and a listed walk opens its tables here.

use std::borrow::Borrow;
use std::ops::RangeBounds;

use redb::{AccessGuard, Key, ReadableTable, TableDefinition, Value};

use crate::engine::Engine;
use crate::error::{Result, StopReason, StorageError};

/// When a walk stops: see the module docs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalkScope {
    /// Work that serves no client stops at the node's stop signal.
    Background,
    /// A client's request stops at the drain's deadline, or when the storage
    /// has failed.
    Request,
}

impl Engine {
    /// Why a walk in `scope` must stop now, if it must.
    pub fn walk_stop(&self, scope: WalkScope) -> Option<StopReason> {
        if let Some(reason) = self.stop_reason() {
            return Some(reason);
        }
        (scope == WalkScope::Background && self.walks_stopping()).then_some(StopReason::Shutdown)
    }

    /// `Err(Stopping)` when a walk in `scope` must stop. For a loop that does
    /// not read the store row by row, such as building a vector graph from
    /// rows already read, which checks it on each step.
    pub fn check_walk(&self, scope: WalkScope) -> Result<()> {
        match self.walk_stop(scope) {
            Some(reason) => Err(StorageError::Stopping(reason)),
            None => Ok(()),
        }
    }

    pub(crate) fn walk(&self, scope: WalkScope) -> WalkStop<'_> {
        WalkStop { stop: Some((self, scope)) }
    }
}

/// A walk's engine and scope, for the tables it opens.
#[derive(Clone, Copy)]
pub(crate) struct WalkStop<'e> {
    /// `None` for a walk inside a write transaction: see [`WalkStop::in_write`].
    stop: Option<(&'e Engine, WalkScope)>,
}

impl WalkStop<'static> {
    /// A walk inside a write transaction, which does not stop. The write
    /// holds the single writer, so `close_writes` bounds it: the stop waits
    /// for it up to its cap, and refuses a clean exit past that (ADR-192).
    /// Stopping it instead would turn a write the drain let finish into a
    /// refusal, and a replicated batch into one applied again.
    pub(crate) fn in_write() -> Self {
        WalkStop { stop: None }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Engine {
    /// Stop the walks, as the signal does, at the `rows`-th row any
    /// stop-aware walk of this engine checks from now on. For a test that
    /// needs the stop to arrive in the middle of a sequence of walks, where
    /// no other hook reaches.
    pub fn stop_walks_after_rows(&self, rows: u64) {
        self.stop_after_rows.store(rows, std::sync::atomic::Ordering::SeqCst);
    }

    /// [`Self::stop_walks_after_rows`] for the drain's deadline: at the
    /// `rows`-th row, [`Engine::set_stopping`], which ends a request's walks.
    pub fn set_stopping_after_rows(&self, rows: u64) {
        self.stopping_after_rows.store(rows, std::sync::atomic::Ordering::SeqCst);
    }
}

impl WalkStop<'_> {
    /// Count a row towards [`Engine::stop_walks_after_rows`].
    #[cfg(any(test, feature = "test-hooks"))]
    fn count_row(&self) {
        use std::sync::atomic::{AtomicU64, Ordering};
        // `fetch_update` is `try_update` from Rust 1.99, which the 1.90
        // minimum does not have; the old name stays until the minimum moves.
        #[allow(deprecated)]
        let reaches = |left: &AtomicU64| {
            left.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)) == Ok(1)
        };
        if let Some((engine, _)) = self.stop {
            if reaches(&engine.stop_after_rows) {
                engine.stop_walks();
            }
            if reaches(&engine.stopping_after_rows) {
                engine.set_stopping();
            }
        }
    }

    pub(crate) fn check(&self) -> Result<()> {
        match self.stop {
            Some((engine, scope)) => engine.check_walk(scope),
            None => Ok(()),
        }
    }
}

/// A table whose `range` and `iter` stop at the node's stop. Everything else
/// a table does, a point read or a write, passes through unchanged.
pub(crate) struct WalkTable<'e, T> {
    table: T,
    walk: WalkStop<'e>,
}

/// The key and value types of a redb table, so [`WalkTable::range`] takes
/// the same turbofish redb's does (`range::<KR>`).
pub(crate) trait TableTypes {
    type K: Key + 'static;
    type V: Value + 'static;
}

impl<K: Key + 'static, V: Value + 'static> TableTypes for redb::ReadOnlyTable<K, V> {
    type K = K;
    type V = V;
}

impl<K: Key + 'static, V: Value + 'static> TableTypes for redb::Table<'_, K, V> {
    type K = K;
    type V = V;
}

impl<'e, T> WalkTable<'e, T> {
    /// The table's rows in `range`, checking the stop before each.
    pub(crate) fn range<'a, KR>(
        &self,
        range: impl RangeBounds<KR> + 'a,
    ) -> Result<Rows<'e, redb::Range<'_, T::K, T::V>>>
    where
        T: TableTypes + ReadableTable<T::K, T::V>,
        KR: Borrow<<T::K as Value>::SelfType<'a>> + 'a,
    {
        self.walk.check()?;
        Ok(Rows { rows: self.table.range(range)?, walk: self.walk, stopped: false })
    }

    /// Every row of the table, checking the stop before each.
    pub(crate) fn iter(&self) -> Result<Rows<'e, redb::Range<'_, T::K, T::V>>>
    where
        T: TableTypes + ReadableTable<T::K, T::V>,
    {
        self.walk.check()?;
        Ok(Rows { rows: self.table.iter()?, walk: self.walk, stopped: false })
    }
}

/// A row of a walk table's `T`, as redb gives one end of it.
type EndRow<'a, T> = (AccessGuard<'a, <T as TableTypes>::K>, AccessGuard<'a, <T as TableTypes>::V>);

/// The bounded reads a walk table allows beside its walks: one key, or one
/// end. There is no `Deref` to the table: a walk table has no way to reach
/// redb's own `range` or `iter`, which do not check the stop.
impl<T> WalkTable<'_, T>
where
    T: TableTypes + ReadableTable<T::K, T::V>,
{
    pub(crate) fn get<'a>(
        &self,
        key: impl Borrow<<T::K as Value>::SelfType<'a>>,
    ) -> Result<Option<AccessGuard<'_, T::V>>> {
        Ok(self.table.get(key)?)
    }

    /// How many rows the table holds, from the count in its header: no page
    /// of the table is read.
    pub(crate) fn len(&self) -> Result<u64> {
        Ok(self.table.len()?)
    }

    pub(crate) fn first(&self) -> Result<Option<EndRow<'_, T>>> {
        Ok(self.table.first()?)
    }

    pub(crate) fn last(&self) -> Result<Option<EndRow<'_, T>>> {
        Ok(self.table.last()?)
    }
}

/// A write transaction's walk table also writes, a key at a time.
impl<'t, K: Key + 'static, V: Value + 'static> WalkTable<'_, redb::Table<'t, K, V>> {
    pub(crate) fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> Result<Option<AccessGuard<'_, V>>> {
        Ok(self.table.insert(key, value)?)
    }

    pub(crate) fn remove<'k>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
    ) -> Result<Option<AccessGuard<'_, V>>> {
        Ok(self.table.remove(key)?)
    }
}

/// `KIMMY_TEST_WALK_ROW_MS`: how long each row of a stop-aware walk takes,
/// in milliseconds, after its check; 0, the default, for no delay. So that a
/// test of a real node can make a walk long enough to be running at the stop,
/// as a cold walk of a large oplog is. In the shipped binary, like the other
/// test switches; the daemon sets it only once it is serving, and says so.
static TEST_ROW_DELAY_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Set [`TEST_ROW_DELAY_MS`]: see there.
pub fn set_test_walk_row_delay(delay: std::time::Duration) {
    TEST_ROW_DELAY_MS.store(delay.as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
}

fn test_row_delay() {
    let ms = TEST_ROW_DELAY_MS.load(std::sync::atomic::Ordering::Relaxed);
    if ms > 0 {
        std::thread::sleep(std::time::Duration::from_millis(ms));
    }
}

/// A table's rows, ending with one `Err(Stopping)` once the stop has come.
pub(crate) struct Rows<'e, I> {
    rows: I,
    walk: WalkStop<'e>,
    stopped: bool,
}

impl<I> Rows<'_, I> {
    fn stop(&mut self) -> Option<StorageError> {
        if self.stopped {
            return None;
        }
        #[cfg(any(test, feature = "test-hooks"))]
        self.walk.count_row();
        let stop = self.walk.check().err();
        self.stopped = stop.is_some();
        stop
    }
}

impl<'a, K, V, I> Iterator for Rows<'_, I>
where
    K: Key + 'static,
    V: Value + 'static,
    I: Iterator<
        Item = std::result::Result<(AccessGuard<'a, K>, AccessGuard<'a, V>), redb::StorageError>,
    >,
{
    type Item = Result<(AccessGuard<'a, K>, AccessGuard<'a, V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped {
            return None;
        }
        if let Some(stop) = self.stop() {
            return Some(Err(stop));
        }
        test_row_delay();
        self.rows.next().map(|row| row.map_err(StorageError::from))
    }
}

impl<'a, K, V, I> DoubleEndedIterator for Rows<'_, I>
where
    K: Key + 'static,
    V: Value + 'static,
    I: DoubleEndedIterator<
        Item = std::result::Result<(AccessGuard<'a, K>, AccessGuard<'a, V>), redb::StorageError>,
    >,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.stopped {
            return None;
        }
        if let Some(stop) = self.stop() {
            return Some(Err(stop));
        }
        test_row_delay();
        self.rows.next_back().map(|row| row.map_err(StorageError::from))
    }
}

/// Open `table` in a read transaction for a walk that stops.
pub(crate) fn open_walk_table<'e, K: Key + 'static, V: Value + 'static>(
    txn: &redb::ReadTransaction,
    table: TableDefinition<K, V>,
    walk: WalkStop<'e>,
) -> Result<WalkTable<'e, redb::ReadOnlyTable<K, V>>> {
    Ok(WalkTable { table: txn.open_table(table)?, walk })
}

/// [`open_walk_table`], or `None` for a table this store has never written,
/// which a fresh node has not for some.
pub(crate) fn open_walk_table_if_exists<'e, K: Key + 'static, V: Value + 'static>(
    txn: &redb::ReadTransaction,
    table: TableDefinition<K, V>,
    walk: WalkStop<'e>,
) -> Result<Option<WalkTable<'e, redb::ReadOnlyTable<K, V>>>> {
    match txn.open_table(table) {
        Ok(table) => Ok(Some(WalkTable { table, walk })),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Open `table` in a write transaction for a walk that stops. A walk that
/// stops inside a write transaction returns its error before the commit, so
/// the transaction is dropped and nothing of it is written.
pub(crate) fn open_walk_table_in<'t, 'e, K: Key + 'static, V: Value + 'static>(
    txn: &'t redb::WriteTransaction,
    table: TableDefinition<K, V>,
    walk: WalkStop<'e>,
) -> Result<WalkTable<'e, redb::Table<'t, K, V>>> {
    Ok(WalkTable { table: txn.open_table(table)?, walk })
}

/// Test support: a walk stopped at each row it checks.
#[cfg(test)]
pub(crate) mod every_row {
    use super::*;

    /// What `f` answers, and how many rows the stop-aware walks of `engine`
    /// check while it runs.
    pub(crate) fn rows_checked<T>(engine: &Engine, f: impl FnOnce() -> T) -> (T, u64) {
        use std::sync::atomic::Ordering;
        engine.lift_stops_for_test();
        engine.stop_after_rows.store(u64::MAX, Ordering::SeqCst);
        let out = f();
        let left = engine.stop_after_rows.swap(0, Ordering::SeqCst);
        (out, u64::MAX - left)
    }

    /// Run `walk` once unstopped, to count the rows it checks; then stop it,
    /// in `scope`'s way, at each of those rows, first to last, and hold it to
    /// answering `Stopping` every time. The rows it returns.
    ///
    /// A walk that `writes` gets a new `fixture` for each stop, since a
    /// finished write changes what the next run reads; the rest share one.
    /// The count is exact, not a bound: a run checks the same rows each time,
    /// so every stop is reached, and a stop that was not says so.
    pub(crate) fn assert_stops_at_every_row<F, T: std::fmt::Debug>(
        name: &str,
        scope: WalkScope,
        writes: bool,
        fixture: impl Fn() -> F,
        engine_of: impl Fn(&F) -> &Engine,
        walk: impl Fn(&F) -> Result<T>,
    ) -> u64 {
        let counting = fixture();
        let (answer, rows) = rows_checked(engine_of(&counting), || walk(&counting));
        assert!(answer.is_ok(), "{name} ({scope:?}) unstopped: {answer:?}");
        for row in 1..=rows {
            let own;
            let f = match writes {
                true => {
                    own = fixture();
                    &own
                }
                false => &counting,
            };
            let engine = engine_of(f);
            engine.lift_stops_for_test();
            match scope {
                WalkScope::Background => engine.stop_walks_after_rows(row),
                WalkScope::Request => engine.set_stopping_after_rows(row),
            }
            let answer = walk(f);
            let stopped = match scope {
                WalkScope::Background => engine.walks_stopping(),
                WalkScope::Request => engine.is_stopping(),
            };
            assert!(stopped, "{name} ({scope:?}) never reached row {row} of {rows}: {answer:?}");
            assert!(
                matches!(answer, Err(StorageError::Stopping(_))),
                "{name} ({scope:?}) stopped at row {row} of {rows} and answered {answer:?}"
            );
            engine.lift_stops_for_test();
        }
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StopReason;
    use bson::doc;
    use kimmy_core::Hlc;
    use redb::ReadableDatabase;

    fn engine_with(docs: i32) -> (Engine, crate::CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("shop", "orders").unwrap();
        for n in 0..docs {
            engine.insert(&coll, doc! { "_id": n }).unwrap();
        }
        (engine, coll, dir)
    }

    /// A background walk ends at the signal, on the next row it would read,
    /// with the stop's reason. A request's walk goes on through the drain,
    /// and ends at its deadline.
    #[test]
    fn a_walk_ends_at_its_scopes_stop_on_the_next_row() {
        let (engine, coll, _dir) = engine_with(10);
        let mut seen = 0;
        let stopped = engine.for_each_doc(&coll, WalkScope::Background, |_, _| {
            seen += 1;
            if seen == 3 {
                engine.stop_walks();
            }
            Ok(true)
        });
        assert!(
            matches!(stopped, Err(StorageError::Stopping(StopReason::Shutdown))),
            "{stopped:?}"
        );
        assert_eq!(seen, 3, "the row after the signal is not read");

        let mut seen = 0;
        engine
            .for_each_doc(&coll, WalkScope::Request, |_, _| {
                seen += 1;
                Ok(true)
            })
            .unwrap();
        assert_eq!(seen, 10, "a request's walk is not ended by the signal");

        let mut seen = 0;
        let stopped = engine.for_each_doc(&coll, WalkScope::Request, |_, _| {
            seen += 1;
            if seen == 4 {
                engine.set_stopping();
            }
            Ok(true)
        });
        assert!(
            matches!(stopped, Err(StorageError::Stopping(StopReason::DrainDeadline))),
            "{stopped:?}"
        );
        assert_eq!(seen, 4);
    }

    /// The stop is checked before every row, the rows a walk passes over
    /// included: a window served to a peer skips what the peer holds, and on
    /// a cold disk each skipped row can be a seek.
    #[test]
    fn a_walk_passing_over_rows_still_stops_on_each() {
        let (engine, _coll, _dir) = engine_with(10);
        let skipped = std::cell::Cell::new(0);
        let stopped = engine.read_oplog_from_skipping(
            Hlc::ZERO,
            usize::MAX,
            WalkScope::Background,
            |_| {
                skipped.set(skipped.get() + 1);
                if skipped.get() == 2 {
                    engine.stop_walks();
                }
                true
            },
            |_| true,
        );
        assert!(matches!(stopped, Err(StorageError::Stopping(_))), "{stopped:?}");
        assert_eq!(skipped.get(), 2, "no row is passed over after the signal");
    }

    /// A walk inside a write does not stop: `close_writes` bounds it.
    #[test]
    fn a_walk_inside_a_write_does_not_stop() {
        let (engine, coll, _dir) = engine_with(3);
        engine.stop_walks();
        engine.set_stopping();
        let db = engine.db();
        let txn = db.begin_write().unwrap();
        let docs = open_walk_table_in(&txn, crate::tables::DOCS, WalkStop::in_write()).unwrap();
        let rows = docs.range(crate::engine::doc_range(coll.id)).unwrap().count();
        assert_eq!(rows, 3);
    }

    /// Both ends of a walk stop, and a stopped walk yields its error once.
    #[test]
    fn a_stopped_walk_yields_its_error_once_from_either_end() {
        let (engine, coll, _dir) = engine_with(3);
        let txn = engine.db().begin_read().unwrap();
        let docs =
            open_walk_table(&txn, crate::tables::DOCS, engine.walk(WalkScope::Background)).unwrap();
        let mut rows = docs.range(crate::engine::doc_range(coll.id)).unwrap();
        assert!(rows.next_back().unwrap().is_ok());
        engine.stop_walks();
        assert!(matches!(rows.next_back(), Some(Err(StorageError::Stopping(_)))));
        assert!(rows.next().is_none(), "one error, then the end");
    }

    /// Every public walk that stops, stopped at each row it checks, first to
    /// last: each answers `Stopping`, rather than a partial answer. A walk
    /// that is stop-aware in its tables but ended some other way on the stop,
    /// or swallowed the error and answered what it had read so far, fails
    /// here whatever the source looks like. Every row, not a sample: a call
    /// made of several walks, such as retention's oplog and then its
    /// tombstones, or a candidate scan's sentinel seeks and then its runs,
    /// is stopped inside each of them.
    #[test]
    fn every_stop_aware_walk_answers_stopping_at_every_row_it_reads() {
        type Fixture = (Engine, crate::CollectionMeta, tempfile::TempDir);
        type Walk = fn(&Engine, &crate::CollectionMeta) -> Result<()>;
        // A peer's marks, as a snapshot leaves them, so the marks' walk has
        // rows to be stopped among.
        const HELD_ORIGIN: kimmy_core::NodeId = kimmy_core::NodeId::from_bytes([7; 16]);
        // Documents, a unique index over them, a compound one that cannot key
        // the documents with two arrays and a partial one that cannot decide
        // those with a Decimal128 (ADR-185), deleted documents for
        // retention's tombstones, and the marks.
        let fixture = || -> Fixture {
            let (engine, _, dir) = engine_with(10);
            let orders = engine.get_collection("shop", "orders").unwrap();
            for n in 10..13 {
                engine.insert(&orders, doc! { "_id": n, "a": [1, 2], "b": [3, 4] }).unwrap();
            }
            for n in 13..16 {
                let k = bson::Bson::Decimal128("1".parse().unwrap());
                engine.insert(&orders, doc! { "_id": n, "k": k }).unwrap();
            }
            for n in [1, 4, 7] {
                assert!(engine.delete(&orders, &kimmy_core::DocId::Int64(n)).unwrap());
            }
            // A standing unique violation, for the violations walks.
            let field = |path: &str| crate::IndexField { path: path.into(), descending: false };
            engine
                .create_index_with(
                    "shop",
                    "orders",
                    vec![field("u")],
                    true,
                    Default::default(),
                    Some("by_u".into()),
                    None,
                    Some(doc! { "u": { "$gt": 0 } }),
                )
                .unwrap();
            let orders = engine.get_collection("shop", "orders").unwrap();
            engine.insert(&orders, doc! { "_id": 100, "u": 1 }).unwrap();
            let clash = kimmy_core::OplogEntry {
                stamp: kimmy_core::Stamp::new(
                    Hlc::new(u64::MAX >> 20, 0),
                    kimmy_core::NodeId::from_bytes([9; 16]),
                ),
                kind: kimmy_core::OpKind::Insert,
                collection: orders.id,
                doc_id: Some(kimmy_core::DocId::Int64(101)),
                body: Some(bson::serialize_to_vec(&doc! { "_id": 101, "u": 1 }).unwrap()),
            };
            assert!(engine.apply_remote(&orders, &clash).unwrap());
            let field = |path: &str| crate::IndexField { path: path.into(), descending: false };
            engine
                .create_index("shop", "orders", vec![field("_id")], true, Some("by_n".into()))
                .unwrap();
            engine
                .create_index(
                    "shop",
                    "orders",
                    vec![field("a"), field("b")],
                    false,
                    Some("ab".into()),
                )
                .unwrap();
            // A partial filter a Decimal128 leaves undecided (ADR-185).
            engine
                .create_index_with(
                    "shop",
                    "orders",
                    vec![field("_id")],
                    false,
                    Default::default(),
                    Some("partial".into()),
                    None,
                    Some(doc! { "k": { "$gt": 5 } }),
                )
                .unwrap();
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            {
                let mut held = txn.open_table(crate::tables::OPLOG_HELD).unwrap();
                for wall_ms in 1..=10 {
                    let stamp = kimmy_core::Stamp::new(Hlc::new(wall_ms, 0), HELD_ORIGIN);
                    held.insert(crate::codec::oplog_key(&stamp).as_slice(), ()).unwrap();
                }
            }
            txn.commit().unwrap();
            let coll = engine.get_collection("shop", "orders").unwrap();
            (engine, coll, dir)
        };
        // Each order of a candidate scan, by the plan the planner makes.
        fn candidates(
            e: &Engine,
            c: &crate::CollectionMeta,
            filter: bson::Document,
            order: crate::CandidateOrder<'_>,
        ) -> Result<()> {
            let filter = kimmy_query::filter::parse(&filter).unwrap();
            let plan = kimmy_query::plan::choose(&filter, &c.indexes).expect("an index plan");
            let scan = crate::IndexScan {
                index_id: plan.index_id,
                ranges: &plan.ranges,
                both_bounds: plan.both_bounds,
                exact: plan.exact,
            };
            e.visit_index_candidates(c, &scan, order, WalkScope::Request, |_, _, _| Ok(true))
                .map(drop)
        }
        let background: &[(&str, Walk)] = &[
            // covers: engine::Engine::warm_purge_chunk
            ("warm_purge_chunk", |e, c| e.warm_purge_chunk(c.id)),
            // covers: violations_table::Engine::violations_backfill_scan
            ("violations_backfill_step", |e, _| {
                e.violations().clear_ready_for_test();
                let budget = crate::watch::ExamineBudget {
                    time: std::time::Duration::from_secs(3_600),
                    rows: u64::MAX,
                };
                e.violations_backfill_step(budget).map(drop)
            }),
            // covers: watch::Engine::read_oplog_linear_in
            ("serve_entries_to_peer", |e, _| {
                e.serve_entries_to_peer(Hlc::ZERO, 100, None, &[], None).map(drop)
            }),
            // covers: watch::Engine::read_oplog_by_arrival_keys_in
            ("serve_entries_to_peer_holding", |e, _| {
                // A requester that holds everything the oplog has, so the walk
                // takes the keys of the arrival index and skips every one.
                let held = [(HELD_ORIGIN, Hlc::new(u64::MAX, 0))].into_iter().collect();
                e.serve_entries_to_peer(Hlc::ZERO, 100, Some(&held), &[], None).map(drop)
            }),
            // covers: watch::Engine::read_oplog_linear_in
            ("entries_for_peer", |e, _| {
                e.entries_for_peer(Hlc::ZERO, 100, WalkScope::Background).map(drop)
            }),
            // covers: sync::Engine::held_marks_covered_by
            ("held_marks_covered_by", |e, _| {
                let witnessed = [(HELD_ORIGIN, Hlc::new(u64::MAX, 0))].into_iter().collect();
                e.held_marks_covered_by(&witnessed).map(drop)
            }),
            // covers: snapshot::Engine::snapshot_documents
            ("snapshot_page", |e, _| e.snapshot_page(None, None).map(drop)),
            // covers: gc::Engine::collect_oplog, gc::Engine::collect_tombstones_within
            ("collect_garbage", |e, _| {
                e.collect_garbage(crate::RetentionPolicy::new(0, 0)).map(drop)
            }),
            // covers: index::Engine::index_keyed_entries_after
            ("index_keyed_entries_after", |e, c| {
                let index = c.index("by_n").unwrap();
                e.index_keyed_entries_after(c, index.id, &[], &[0xFF; 8], None, 100).map(drop)
            }),
            // covers: docs::Engine::for_each_record_after
            ("for_each_doc", |e, c| e.for_each_doc(c, WalkScope::Background, |_, _| Ok(true))),
        ];
        let request: &[(&str, Walk)] = &[
            // covers: backup::Engine::backup_to
            ("backup_to", |e, _| e.backup_to(&mut Vec::new(), WalkScope::Request).map(drop)),
            // covers: docs::Engine::for_each_record_after
            ("for_each_doc", |e, c| e.for_each_doc(c, WalkScope::Request, |_, _| Ok(true))),
            // covers: docs::Engine::for_each_record_after
            ("for_each_record_after", |e, c| {
                e.for_each_record_after(c, None, WalkScope::Request, |_, _, _| Ok(true))
            }),
            // covers: docs::Engine::for_each_doc_or_undecodable
            ("for_each_doc_or_undecodable", |e, c| {
                e.for_each_doc_or_undecodable(c, WalkScope::Request, |_, _| Ok(true))
            }),
            // covers: docs::Engine::for_each_record_after
            ("count", |e, c| e.count(c, WalkScope::Request).map(drop)),
            // covers: docs::Engine::live_unique_violations_from_table
            ("live_unique_violations", |e, c| {
                assert!(e.violations_table_ready());
                e.live_unique_violations(c, None, WalkScope::Request).map(drop)
            }),
            // covers: docs::Engine::live_unique_violations_from_oplog
            ("live_unique_violations (oplog)", |e, c| {
                e.violations().clear_ready_for_test();
                e.live_unique_violations(c, None, WalkScope::Request).map(drop)
            }),
            // covers: watch::Engine::read_oplog_linear_in
            ("read_oplog_from", |e, _| {
                e.read_oplog_from(Hlc::ZERO, 100, WalkScope::Request).map(drop)
            }),
            // covers: index::scan_table
            ("index_candidates", |e, c| {
                let index = c.index("by_n").unwrap();
                e.index_candidates(c, index.id, &[], &[0xFF; 8]).map(drop)
            }),
            // covers: index::holders_of
            ("unkeyed_count", |e, c| e.unkeyed_count(c, c.index("ab").unwrap().id).map(drop)),
            // covers: index::holders_of
            ("undecidable_count", |e, c| {
                e.undecidable_count(c, c.index("partial").unwrap().id).map(drop)
            }),
            // The candidate walks: in index order, one exact run, exact runs
            // merged, and an inexact range put in key order.
            // covers: index::has_entries_at, index::Walk::in_index_order
            ("in_index_order", |e, c| {
                candidates(e, c, doc! { "_id": { "$gte": 2 } }, crate::CandidateOrder::Any)
            }),
            // covers: index::has_entries_at, index::Walk::one_run
            ("one_run", |e, c| {
                let by_id = crate::CandidateOrder::ById { after: None, want: None };
                candidates(e, c, doc! { "_id": 3 }, by_id)
            }),
            // covers: index::has_entries_at, index::Walk::merged_runs
            ("merged_runs", |e, c| {
                let by_id = crate::CandidateOrder::ById { after: None, want: None };
                candidates(e, c, doc! { "_id": { "$in": [2, 5, 8] } }, by_id)
            }),
            // covers: index::has_entries_at, index::Walk::in_key_order
            ("in_key_order", |e, c| {
                let by_id = crate::CandidateOrder::ById { after: None, want: None };
                candidates(e, c, doc! { "_id": { "$gte": 2, "$lte": 9 } }, by_id)
            }),
            // covers: index::Engine::create_index_inner
            ("a client's index build", |e, _| {
                let field = crate::IndexField { path: "m".into(), descending: false };
                e.create_index("shop", "orders", vec![field], false, None).map(drop)
            }),
        ];
        // The walks that write when they finish, so each stop needs a store
        // the last run did not change.
        let writes = ["collect_garbage", "a client's index build", "violations_backfill_step"];
        for (scope, walks) in [(WalkScope::Background, background), (WalkScope::Request, request)] {
            for (name, walk) in walks {
                let rows = every_row::assert_stops_at_every_row(
                    name,
                    scope,
                    writes.contains(name),
                    fixture,
                    |f| &f.0,
                    |f| walk(&f.0, &f.1),
                );
                assert!(rows > 1, "{name} ({scope:?}) read {rows} rows: nothing to stop among");
            }
        }
    }

    /// A backup that stops writes no `END`, so what it wrote is refused as
    /// truncated, never restored as a whole store.
    #[test]
    fn a_backup_that_stops_is_never_a_whole_one() {
        let (engine, _coll, dir) = engine_with(50);
        struct StopAfter<'e> {
            out: Vec<u8>,
            engine: &'e Engine,
        }
        impl std::io::Write for StopAfter<'_> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.out.extend_from_slice(buf);
                if self.out.len() > 200 {
                    self.engine.set_stopping();
                }
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut out = StopAfter { out: Vec::new(), engine: &engine };
        let stopped = engine.backup_to(&mut out, WalkScope::Request);
        assert!(
            matches!(stopped, Err(StorageError::Stopping(StopReason::DrainDeadline))),
            "{stopped:?}"
        );
        let restored = crate::backup::restore(&dir.path().join("restored.redb"), &mut &out.out[..]);
        assert!(restored.is_err(), "a stopped backup restored: {restored:?}");
    }
}
