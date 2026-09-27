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

    /// Every public walk that stops, called once its scope's stop has come:
    /// each answers `Stopping`, rather than a partial answer. A walk that
    /// is stop-aware in its tables but ended some other way on the stop, or
    /// swallowed the error, fails here whatever the source looks like.
    #[test]
    fn every_stop_aware_walk_answers_stopping_once_its_stop_has_come() {
        type Walk = fn(&Engine, &crate::CollectionMeta) -> Result<()>;
        // A peer's marks, as a snapshot leaves them, so the marks' walk has
        // rows to be stopped among.
        const HELD_ORIGIN: kimmy_core::NodeId = kimmy_core::NodeId::from_bytes([7; 16]);
        let unique = |engine: &Engine| {
            let field = crate::IndexField { path: "_id".into(), descending: false };
            engine.create_index("shop", "orders", vec![field], true, Some("by_n".into())).unwrap();
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
        };
        let background: &[(&str, Walk)] = &[
            ("serve_entries_to_peer", |e, _| {
                e.serve_entries_to_peer(Hlc::ZERO, 100, None, &[]).map(drop)
            }),
            ("entries_for_peer", |e, _| {
                e.entries_for_peer(Hlc::ZERO, 100, WalkScope::Background).map(drop)
            }),
            ("held_marks_covered_by", |e, _| {
                let witnessed = [(HELD_ORIGIN, Hlc::new(u64::MAX, 0))].into_iter().collect();
                e.held_marks_covered_by(&witnessed).map(drop)
            }),
            ("snapshot_page", |e, _| e.snapshot_page(None, None).map(drop)),
            ("collect_garbage", |e, _| {
                e.collect_garbage(crate::RetentionPolicy::new(0, 0)).map(drop)
            }),
            ("index_keyed_entries_after", |e, c| {
                let index = c.index("by_n").unwrap();
                e.index_keyed_entries_after(c, index.id, &[], &[0xFF; 8], None, 100).map(drop)
            }),
            ("for_each_doc", |e, c| e.for_each_doc(c, WalkScope::Background, |_, _| Ok(true))),
        ];
        let request: &[(&str, Walk)] = &[
            ("backup_to", |e, _| e.backup_to(&mut Vec::new(), WalkScope::Request).map(drop)),
            ("for_each_doc", |e, c| e.for_each_doc(c, WalkScope::Request, |_, _| Ok(true))),
            ("for_each_record_after", |e, c| {
                e.for_each_record_after(c, None, WalkScope::Request, |_, _, _| Ok(true))
            }),
            ("for_each_doc_or_undecodable", |e, c| {
                e.for_each_doc_or_undecodable(c, WalkScope::Request, |_, _| Ok(true))
            }),
            ("count", |e, c| e.count(c, WalkScope::Request).map(drop)),
            ("live_unique_violations", |e, c| {
                e.live_unique_violations(c, None, WalkScope::Request).map(drop)
            }),
            ("read_oplog_from", |e, _| {
                e.read_oplog_from(Hlc::ZERO, 100, WalkScope::Request).map(drop)
            }),
            ("index_candidates", |e, c| {
                let index = c.index("by_n").unwrap();
                e.index_candidates(c, index.id, &[], &[0xFF; 8]).map(drop)
            }),
            ("unkeyed_count", |e, c| e.unkeyed_count(c, c.index("by_n").unwrap().id).map(drop)),
            ("undecidable_count", |e, c| {
                e.undecidable_count(c, c.index("by_n").unwrap().id).map(drop)
            }),
            ("a client's index build", |e, _| {
                let field = crate::IndexField { path: "m".into(), descending: false };
                e.create_index("shop", "orders", vec![field], false, None).map(drop)
            }),
        ];
        for (scope, walks) in [(WalkScope::Background, background), (WalkScope::Request, request)] {
            for (name, walk) in walks {
                // Before the walk begins, and then part of the way through
                // it: a walk that ends its loop on the stop and answers what
                // it had is a short answer taken for a whole one, and only a
                // stop in the middle shows it.
                let mut midway = 0;
                for rows in [0, 1, 2, 3, 5, 8] {
                    let (engine, _coll, _dir) = engine_with(10);
                    unique(&engine);
                    let coll = engine.get_collection("shop", "orders").unwrap();
                    let stopped = || match scope {
                        WalkScope::Background => engine.walks_stopping(),
                        WalkScope::Request => engine.is_stopping(),
                    };
                    match (scope, rows) {
                        (WalkScope::Background, 0) => engine.stop_walks(),
                        (WalkScope::Request, 0) => engine.set_stopping(),
                        (WalkScope::Background, n) => engine.stop_walks_after_rows(n),
                        (WalkScope::Request, n) => engine.set_stopping_after_rows(n),
                    }
                    let answer = walk(&engine, &coll);
                    if !stopped() {
                        // The walk read fewer rows than that, and finished.
                        assert!(answer.is_ok(), "{name} ({scope:?}) at row {rows}: {answer:?}");
                        continue;
                    }
                    midway += usize::from(rows > 0);
                    assert!(
                        matches!(answer, Err(StorageError::Stopping(_))),
                        "{name} ({scope:?}) stopped at row {rows} and answered {answer:?}"
                    );
                }
                // Every walk here reads rows: each is stopped in the middle at
                // least once.
                assert!(midway > 0, "{name} ({scope:?}) was never stopped mid-walk");
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
