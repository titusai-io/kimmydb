//! Each collection's live document count, kept rather than walked (ADR-174).
//!
//! The divergence check's count half compares one collection's live count
//! across two members on every contact (ADR-133). Walking the collection for it
//! read every page of that collection on both members, every contact, however
//! little the record header was read for. This table holds the count instead,
//! moved in the same transaction as every write to `DOCS`.
//!
//! **One way in.** Every write to a document record goes through
//! [`put_record`] or [`remove_record`], which read the record's liveness before
//! and after from its header and move the count by the difference. A source
//! guard in the tests fails on any other write to `DOCS`, because a count one
//! path forgot drifts silently, and the check then reports a divergence that is
//! not there or misses one that is.
//!
//! **Once per transaction, not once per write in it.** Those two record the
//! difference in [`Pending`], which the transaction carries, and
//! `WriteTxn::commit` writes through [`flush`] before the inner commit. A
//! replicated batch is one transaction of up to 1,024 entries (ADR-119), and
//! moving the row per entry opened [`tables::LIVE_COUNTS`] 1,024 times, wrote
//! the single key of [`tables::LIVE_COUNTS_THROUGH`] 1,024 times, and paid an
//! extra `oplog.last()` seek for each — inside one commit, so it cost landing
//! time and no commits, which is how it hid. Flushing at the commit keeps both
//! in the same transaction as the entries, so a write that aborts after its
//! record still takes the count back with it.
//!
//! **Rebuilt when it may be stale.** `Engine::open` walks every record's header
//! (`live_walk`, which rebuilds the digests with it) and rewrites the table unless [`tables::LIVE_COUNTS_THROUGH`] still matches
//! the store: the arrival index's next position and the oplog's newest key
//! ([`mark_of`]). The mark is written by any transaction that appended, at its
//! commit, by a build that keeps the count, and carried forward by a rewind and
//! by retention, which remove rows without appending. So it is missing on a
//! database no such build has opened and on one restored from a backup (which
//! carries neither table), and it stops matching once a build that does not
//! keep the count wrote a document — every document write appends to the oplog.

use std::cell::Cell;
use std::collections::BTreeMap;

use redb::{ReadableTable, Table, WriteTransaction};

use crate::codec;
use crate::engine::WriteTxn;
use crate::error::Result;
use crate::tables;

/// The `DOCS` table, open for writing.
pub(crate) type Docs<'txn> = Table<'txn, (u64, &'static [u8]), &'static [u8]>;

/// The one key of [`tables::LIVE_COUNTS_THROUGH`].
pub(crate) const THROUGH: &str = "arrival";

thread_local! {
    /// Opens of [`tables::LIVE_COUNTS`] or [`tables::LIVE_COUNTS_THROUGH`] for
    /// writing, on this thread.
    ///
    /// Instrumentation, and it earns its keep: the cost this module was changed
    /// for is invisible to a commit counter and to any assertion about the
    /// counts themselves, because per-entry work inside one transaction
    /// produces exactly the right numbers, slowly. redb offers no table-open
    /// counter, so this is the only place that can say how often the two tables
    /// are reached for, and
    /// `the_count_and_the_mark_are_written_once_for_a_replicated_batch` reads
    /// it.
    ///
    /// **Per thread, not per process.** The test binary runs its tests in
    /// parallel in one process, and a shared counter measures whatever else
    /// was writing at the time — which makes the guard fail on an innocent
    /// change and pass on a guilty one. Every open a transaction makes happens
    /// on the thread driving it: the flush runs in the committing thread,
    /// before `blocking` is reached, and the rebuild runs in the thread that
    /// opens the engine.
    ///
    /// The read path ([`live_count`]) opens on a read transaction and is
    /// deliberately not counted: what is bounded here is the write path.
    static TABLE_OPENS: Cell<u64> = const { Cell::new(0) };
}

/// Opens made on this thread, for the test that bounds them.
pub(crate) fn table_opens() -> u64 {
    TABLE_OPENS.with(Cell::get)
}

fn counted() {
    TABLE_OPENS.with(|opens| opens.set(opens.get() + 1));
}

/// [`tables::LIVE_COUNTS`], open for writing and counted.
fn open_counts(txn: &WriteTransaction) -> Result<Table<'_, u64, u64>> {
    counted();
    Ok(txn.open_table(tables::LIVE_COUNTS)?)
}

/// [`tables::LIVE_COUNTS_THROUGH`], open for writing and counted.
fn open_through(txn: &WriteTransaction) -> Result<Table<'_, &'static str, &'static [u8]>> {
    counted();
    Ok(txn.open_table(tables::LIVE_COUNTS_THROUGH)?)
}

/// What a transaction owes the two tables, gathered as it writes and written
/// once by [`flush`] when it commits.
///
/// Carried by `WriteTxn` rather than threaded through every write path,
/// because the commit is the one point all of them pass through — the same
/// argument that makes `WriteTxn::commit` the commit counter's chokepoint, and
/// what keeps "the count and the mark land in the same commit as the entries"
/// true by construction rather than by everyone remembering.
#[derive(Default)]
pub(crate) struct Pending {
    /// Per collection, live records gained less lost in this transaction.
    /// Signed and accumulated, so a document written and deleted again inside
    /// one transaction nets to nothing and moves no row.
    deltas: BTreeMap<u64, i64>,
    /// Where the counts are kept through, as the last append in this
    /// transaction left the store, or `None` if it appended nothing.
    ///
    /// The bytes rather than a flag, and read at the append rather than at the
    /// commit. [`mark_of`] needs the arrival index and the oplog, and
    /// `append_oplog_at` holds both open already — recomputing it at the commit
    /// would mean opening those two tables again, which is two opens added to
    /// *every* transaction including a single-document write. Each append
    /// overwrites this, so what is stored is the last one, which is the state
    /// the transaction ends in.
    mark: Option<Vec<u8>>,
    /// Per collection, what the digest row owes (B3 half 1): written by
    /// [`crate::live_digest::flush`] in the same commit, beside the counts.
    digests: BTreeMap<u64, crate::live_digest::Delta>,
}

impl Pending {
    /// The control's no-op (`--cfg kimmy_bench_no_live_counts`).
    #[cfg(kimmy_bench_no_live_counts)]
    fn moved(&mut self, _collection: u64, _was_live: bool, _is_live: bool) {}

    /// The control's no-op (`--cfg kimmy_bench_no_live_counts`).
    #[cfg(kimmy_bench_no_live_counts)]
    pub(crate) fn appended(&mut self, _mark: Vec<u8>) {}

    /// Move `collection`'s count by what one write did to liveness there.
    #[cfg(not(kimmy_bench_no_live_counts))]
    fn moved(&mut self, collection: u64, was_live: bool, is_live: bool) {
        let delta = match (was_live, is_live) {
            (false, true) => 1,
            (true, false) => -1,
            // Liveness unchanged: a replace over a live document, or a
            // tombstone over a tombstone.
            _ => return,
        };
        *self.deltas.entry(collection).or_default() += delta;
    }

    /// One write to `key` of `collection`: the record that was there and the
    /// one now, each as its stamp and whether it is live.
    ///
    /// `key` is the stored key, and the digest moves by the terms of it: every
    /// write to `DOCS` passes through here, `put_record`, `put_bare_tombstone` and
    /// `remove_record` (the drop purger's) alike.
    #[cfg(not(kimmy_bench_no_live_counts))]
    fn wrote(
        &mut self,
        collection: u64,
        key: &[u8],
        before: Option<(kimmy_core::Stamp, bool)>,
        after: Option<(kimmy_core::Stamp, bool)>,
    ) {
        self.digests.entry(collection).or_default().wrote(key, before, after);
    }

    /// The control's no-op (`--cfg kimmy_bench_no_live_counts`).
    #[cfg(kimmy_bench_no_live_counts)]
    fn wrote(
        &mut self,
        _collection: u64,
        _key: &[u8],
        _before: Option<(kimmy_core::Stamp, bool)>,
        _after: Option<(kimmy_core::Stamp, bool)>,
    ) {
    }

    /// The write just recorded for `collection` was a tombstone no oplog entry
    /// stands behind (ADR-228).
    #[cfg(not(kimmy_bench_no_live_counts))]
    fn bare(&mut self, collection: u64, stamp: &kimmy_core::Stamp) {
        self.digests.entry(collection).or_default().bare(stamp);
    }

    /// The control's no-op (`--cfg kimmy_bench_no_live_counts`).
    #[cfg(kimmy_bench_no_live_counts)]
    fn bare(&mut self, _collection: u64, _stamp: &kimmy_core::Stamp) {}

    /// The last chunk of a drop purge of `collection`: its digest row goes
    /// with the flush: the ranges are exhausted, so no record is left under it.
    pub(crate) fn forget_digest(&mut self, collection: u64) {
        self.digests.entry(collection).or_default().forget_row();
    }

    /// Record where an append left the store, for [`flush`] to write once.
    #[cfg(not(kimmy_bench_no_live_counts))]
    pub(crate) fn appended(&mut self, mark: Vec<u8>) {
        self.mark = Some(mark);
    }
}

/// Write the encoded `record` at `key` of `collection`, and move the
/// collection's live count by what the write did to liveness there.
///
/// The move is recorded on the transaction and written by [`flush`] at its
/// commit, so a run of writes costs one row and one table open between them
/// rather than one each.
pub(crate) fn put_record(
    txn: &WriteTxn<'_>,
    docs: &mut Docs<'_>,
    collection: u64,
    key: &[u8],
    record: &[u8],
) -> Result<()> {
    let before = match docs.insert((collection, key), record)? {
        Some(previous) => Some(codec::doc_record_header(previous.value())?),
        None => None,
    };
    let after = codec::doc_record_header(record)?;
    let mut pending = txn.live_counts().lock();
    pending.moved(collection, before.is_some_and(|(_, live)| live), after.1);
    pending.wrote(collection, key, before, Some(after));
    Ok(())
}

/// [`put_record`] for a tombstone written with no oplog entry behind it
/// (ADR-228): the digest row's `bare_max` rises with it.
pub(crate) fn put_bare_tombstone(
    txn: &WriteTxn<'_>,
    docs: &mut Docs<'_>,
    collection: u64,
    key: &[u8],
    record: &[u8],
) -> Result<()> {
    put_record(txn, docs, collection, key, record)?;
    let (stamp, _) = codec::doc_record_header(record)?;
    txn.live_counts().lock().bare(collection, &stamp);
    Ok(())
}

/// Remove the record at `key` of `collection`, and move the collection's live
/// count down if it was live. Whether a record was there.
pub(crate) fn remove_record(
    txn: &WriteTxn<'_>,
    docs: &mut Docs<'_>,
    collection: u64,
    key: &[u8],
) -> Result<bool> {
    let before = match docs.remove((collection, key))? {
        Some(previous) => codec::doc_record_header(previous.value())?,
        None => return Ok(false),
    };
    let mut pending = txn.live_counts().lock();
    pending.moved(collection, before.1, false);
    pending.wrote(collection, key, Some(before), None);
    Ok(true)
}

/// Write what `pending` gathered, in the transaction that gathered it and
/// before that transaction commits.
///
/// Each table is opened at most once however much the transaction held, so a
/// 1,024-entry replicated batch pays what a one-entry one pays. A collection
/// whose moves cancelled out is not written at all.
///
/// Called by `WriteTxn::commit` and nowhere else. The three commits that run
/// before an engine exists (`Engine::open`, the migration, a restore) write
/// through `live_walk` or not at all.
pub(crate) fn flush(txn: &WriteTransaction, pending: &Pending) -> Result<()> {
    // The control writes nothing at all (`--cfg kimmy_bench_no_live_counts`).
    if cfg!(kimmy_bench_no_live_counts) {
        let _ = (txn, pending);
        return Ok(());
    }
    let moved: Vec<(u64, i64)> =
        pending.deltas.iter().filter(|(_, delta)| **delta != 0).map(|(id, d)| (*id, *d)).collect();
    if !moved.is_empty() {
        let mut counts = open_counts(txn)?;
        for (collection, delta) in moved {
            let current = counts.get(collection)?.map(|n| n.value()).unwrap_or(0);
            // Saturating rather than failing the write: a count already wrong
            // is repaired at the next open, and refusing a delete for it would
            // turn a wrong number into lost availability.
            let next = match delta > 0 {
                true => current.saturating_add(delta.unsigned_abs()),
                false => current.saturating_sub(delta.unsigned_abs()),
            };
            match next {
                0 => {
                    counts.remove(collection)?;
                }
                n => {
                    counts.insert(collection, n)?;
                }
            }
        }
    }
    crate::live_digest::flush(txn, &pending.digests)?;
    if let Some(mark) = &pending.mark {
        // Already read, by the last append, from the tables it had open. This
        // writes the one row and opens nothing else.
        mark_through(txn, mark)?;
        crate::live_digest::mark_through(txn, mark)?;
    }
    Ok(())
}

/// The kept live count of collection `id`, read in `txn`.
pub(crate) fn live_count(txn: &redb::ReadTransaction, id: kimmy_core::CollectionId) -> Result<u64> {
    let counts = txn.open_table(tables::LIVE_COUNTS)?;
    Ok(counts.get(id.0)?.map(|n| n.value()).unwrap_or(0))
}

/// Where the store stands, as the mark records it: the arrival index's next
/// position, then the oplog's newest key if it has one.
///
/// **Both, because either alone comes back to a value it held.** The arrival
/// end moves down when a rewind or retention removes rows, and an older build
/// appending as many entries as were removed brings it back to exactly the
/// mark. The oplog's newest key moves on with that append, and a 0.29.x rewind
/// — which removes oplog rows and leaves the arrival index alone — moves it
/// back. A store written behind a build that keeps the counts matches neither.
pub(crate) fn mark_of(
    arrival: &impl ReadableTable<u64, &'static [u8]>,
    oplog: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<Vec<u8>> {
    let next = arrival.last()?.map_or(0, |(seq, _)| seq.value() + 1);
    let mut mark = next.to_be_bytes().to_vec();
    if let Some((key, _)) = oplog.last()? {
        mark.extend_from_slice(key.value());
    }
    Ok(mark)
}

/// Whether the kept counts match the store, read without writing: the test
/// [`crate::live_walk::run`] makes before it walks.
///
/// For a reader that runs before `Engine::open` has rebuilt them — the schema
/// 4 migration's announcement (ADR-183) — and must not trust a table a restore
/// left empty.
pub(crate) fn counts_are_current(txn: &redb::ReadTransaction) -> Result<bool> {
    let mark = mark_of(&txn.open_table(tables::OPLOG_ARRIVAL)?, &txn.open_table(tables::OPLOG)?)?;
    let through = txn.open_table(tables::LIVE_COUNTS_THROUGH)?;
    Ok(through.get(THROUGH)?.is_some_and(|m| m.value() == mark.as_slice()))
}

/// Record, in the transaction of the write that moved it, that the counts are
/// kept through `mark` ([`mark_of`], read after the write).
pub(crate) fn mark_through(txn: &WriteTransaction, mark: &[u8]) -> Result<()> {
    open_through(txn)?.insert(THROUGH, mark)?;
    Ok(())
}

/// Carry the mark from `before` to `after` across a removal that appends
/// nothing — a rewind, or retention — **only if it matched `before`**.
///
/// A mark that already did not match says an older build wrote behind the
/// counts, and moving it to the new state would hide that from the next open.
pub(crate) fn carry_mark(txn: &WriteTxn<'_>, before: &[u8], after: &[u8]) -> Result<()> {
    // A transaction that appended would have its own mark waiting in [`Pending`],
    // and [`flush`] writes that *after* this runs — silently overwriting the
    // refusal below, moving a stale mark to a fresh one, and letting the next
    // `Engine::open` skip a rebuild it needed. No path both appends and carries
    // today (a rewind and a retention pass remove rows without appending), which
    // is what makes this an assertion rather than a branch.
    //
    // **It is order-sensitive, and catches only one order.** This fires for
    // append-then-carry. Carry-then-append passes it silently, because the
    // append's mark reaches [`Pending`] after the check has run. That is benign
    // as things stand — the flush then writes the mark the append read, which
    // is the correct one either way — so this is not a guarantee in both
    // directions, and `the_carry_guard_is_blind_to_carry_then_append` pins the
    // blind side to keep it a known gap rather than an assumed one.
    debug_assert!(
        txn.live_counts().lock().mark.is_none(),
        "a transaction that appended is also carrying the mark; the flush would overwrite the \
         carry and a stale mark would be moved forward"
    );
    let mut table = open_through(txn)?;
    let current = table.get(THROUGH)?.is_some_and(|stored| stored.value() == before);
    if current {
        table.insert(THROUGH, after)?;
    }
    drop(table);
    // The digest rows' own mark, under the same rule: a mark that did not
    // match says a build that does not keep them wrote since, and moving it
    // forward would hide that from the next open.
    crate::live_digest::carry_mark(txn, before, after)?;
    Ok(())
}

/// The arrival position a mark records, for the log.
pub(crate) fn decode_arrival(mark: &[u8]) -> u64 {
    mark.get(..8).and_then(|b| b.try_into().ok()).map_or(0, u64::from_be_bytes)
}

/// Checks the tests of this module and of `live_walk` share.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;

    use redb::{ReadableDatabase, ReadableTable};

    use crate::Engine;
    use crate::codec;
    use crate::tables;

    /// Every kept count equals a walk of the records' headers, and no count is
    /// kept for a collection with no live record.
    pub(crate) fn assert_counts_exact(engine: &Engine, after: &str) {
        let db = engine.db();
        let txn = db.begin_read().unwrap();
        let kept: BTreeMap<u64, u64> = txn
            .open_table(tables::LIVE_COUNTS)
            .unwrap()
            .iter()
            .unwrap()
            .map(|row| {
                let (id, n) = row.unwrap();
                (id.value(), n.value())
            })
            .collect();
        let mut walked: BTreeMap<u64, u64> = BTreeMap::new();
        for row in txn.open_table(tables::DOCS).unwrap().iter().unwrap() {
            let (key, value) = row.unwrap();
            if codec::doc_record_is_live(value.value()).unwrap() {
                *walked.entry(key.value().0).or_default() += 1;
            }
        }
        assert_eq!(kept, walked, "the kept counts disagree with the records after {after}");
        drop(txn);
        assert_digests_exact(engine, after);
    }

    /// Every kept digest row equals a walk of the records' headers: the same
    /// digest. The highest stamps are only ever raised, so the kept `maxw` is
    /// never below the walk's, and the kept `bare_max` never exceeds what the
    /// walk would set (a rebuilt row sets it to the highest tombstone stamp per
    /// origin, the most it can be).
    pub(crate) fn assert_digests_exact(engine: &Engine, after: &str) {
        let db = engine.db();
        let txn = db.begin_read().unwrap();
        let mut walked: BTreeMap<u64, crate::live_digest::DigestRow> = BTreeMap::new();
        for row in txn.open_table(tables::DOCS).unwrap().iter().unwrap() {
            let (key, value) = row.unwrap();
            let (collection, doc_key) = key.value();
            let (stamp, live) = codec::doc_record_header(value.value()).unwrap();
            crate::live_digest::fold_walked(&mut walked, collection, doc_key, &stamp, live);
        }
        let rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
        let mut kept: BTreeMap<u64, crate::live_digest::DigestRow> = BTreeMap::new();
        for row in rows.iter().unwrap() {
            let (id, raw) = row.unwrap();
            kept.insert(id.value(), crate::live_digest::DigestRow::decode(raw.value()).unwrap());
        }
        for (id, walk) in &walked {
            let row =
                kept.get(id).unwrap_or_else(|| panic!("no digest row for {id} after {after}"));
            assert_eq!(row.digest, walk.digest, "digest of {id} after {after}");
            // Never lowered, so a kept `maxw` may stand above the walk's (a
            // replaced record's origin keeps its stamp), never below it.
            for (node, hlc) in &walk.maxw {
                assert!(
                    row.maxw.get(node).is_some_and(|k| k >= hlc),
                    "maxw of {id} is below the walk's for {node:?} after {after}"
                );
            }
            for (node, hlc) in &row.bare_max {
                assert!(
                    walk.bare_max.get(node).is_some_and(|w| w >= hlc),
                    "bare_max of {id} exceeds the walk's after {after}"
                );
            }
        }
        for (id, row) in &kept {
            if !walked.contains_key(id) {
                assert_eq!(row.digest, 0, "digest of the emptied {id} after {after}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use bson::{Document, doc};
    use kimmy_core::{DocId, Hlc, NodeId, OpKind, OplogEntry, Stamp};
    use redb::ReadableDatabase;

    use super::test_support::{assert_counts_exact, assert_digests_exact};
    use super::*;
    use crate::meta::CollectionMeta;
    use crate::{Candidates, Engine, ModifySpec};

    fn engine() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("app", "docs").unwrap();
        (engine, coll, dir)
    }

    /// Set `v` on the document `_id: id`, or remove it.
    struct Modify {
        id: &'static str,
        remove: bool,
    }

    impl ModifySpec for Modify {
        fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
            Ok(doc.get_str("_id").is_ok_and(|id| id == self.id))
        }
        fn compare(&self, _: &Document, _: &Document) -> Ordering {
            Ordering::Equal
        }
        fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
            if self.remove {
                return Ok(None);
            }
            let mut next = doc.clone();
            next.insert("v", 99);
            Ok(Some(next))
        }
        fn upsert(&self) -> Option<std::result::Result<Document, String>> {
            None
        }
    }

    fn remote(coll: &CollectionMeta, id: &str, wall_ms: u64, body: Option<Document>) -> OplogEntry {
        OplogEntry {
            stamp: Stamp::new(Hlc::new(wall_ms, 0), NodeId::generate()),
            kind: if body.is_some() { OpKind::Insert } else { OpKind::Delete },
            collection: coll.id,
            doc_id: Some(DocId::String(id.into())),
            body: body.map(|d| bson::serialize_to_vec(&d).unwrap()),
        }
    }

    fn id(s: &str) -> DocId {
        DocId::String(s.into())
    }

    #[test]
    fn the_kept_counts_match_the_records_after_every_kind_of_write() {
        let (a, coll, _da) = engine();
        let check = |what: &str| assert_counts_exact(&a, what);

        for x in ["x1", "x2", "x3"] {
            a.insert(&coll, doc! { "_id": x, "v": 1 }).unwrap();
        }
        check("an insert");
        a.delete(&coll, &id("x1")).unwrap();
        check("a delete");
        assert!(!a.delete(&coll, &id("absent")).unwrap());
        check("a delete of nothing");
        a.insert(&coll, doc! { "_id": "x1", "v": 2 }).unwrap();
        check("an insert over a tombstone");
        a.replace(&coll, &id("x2"), doc! { "_id": "x2", "v": 3 }, false).unwrap();
        check("a replace");
        a.replace(&coll, &id("y"), doc! { "_id": "y" }, true).unwrap();
        check("an upsert");
        a.find_and_modify(&coll, &Candidates::Scan, &Modify { id: "x3", remove: false }).unwrap();
        check("a find-and-modify");
        a.find_and_modify(&coll, &Candidates::Scan, &Modify { id: "x3", remove: true }).unwrap();
        check("a find-and-modify that removes");

        let later = crate::physical_now_ms() + 10_000;
        a.apply_remote(&coll, &remote(&coll, "r1", later, Some(doc! { "_id": "r1" }))).unwrap();
        check("a replicated insert");
        a.apply_remote(&coll, &remote(&coll, "x2", later, None)).unwrap();
        check("a replicated delete");
        assert!(!a.apply_remote(&coll, &remote(&coll, "x1", 1, None)).unwrap());
        check("a superseded replicated delete");

        // A member built from A's snapshot keeps its own counts.
        let dir = tempfile::tempdir().unwrap();
        let b = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let mut progress = crate::SnapshotProgress::whole_database();
        while !progress.is_complete() {
            let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
            b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        }
        assert_counts_exact(&b, "a snapshot");

        // Again, over documents the member already holds: since the first, A
        // replaced one, deleted one and wrote a new one.
        a.replace(&coll, &id("y"), doc! { "_id": "y", "v": 5 }, false).unwrap();
        a.delete(&coll, &id("r1")).unwrap();
        a.insert(&coll, doc! { "_id": "z" }).unwrap();
        let snapshot = |into: &Engine, mut progress: crate::SnapshotProgress| {
            while !progress.is_complete() {
                let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
                into.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
            }
        };
        snapshot(&b, crate::SnapshotProgress::whole_database());
        assert_counts_exact(&b, "a second snapshot over documents the member holds");

        // A scoped repair grants no coverage, so what it writes stays held as
        // state; the second lands over that held copy.
        let dir_c = tempfile::tempdir().unwrap();
        let c = Engine::open(&dir_c.path().join("kimmy.redb")).unwrap();
        snapshot(&c, crate::SnapshotProgress::of_collection(coll.id));
        assert_counts_exact(&c, "a scoped repair");
        a.replace(&coll, &id("y"), doc! { "_id": "y", "v": 6 }, false).unwrap();
        a.delete(&coll, &id("z")).unwrap();
        snapshot(&c, crate::SnapshotProgress::of_collection(coll.id));
        assert_counts_exact(&c, "a scoped repair over a held copy");

        let until = a.version_vector().unwrap().iter().map(|(_, h)| h).max().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        a.insert(&coll, doc! { "_id": "undone" }).unwrap();
        a.replace(&coll, &id("y"), doc! { "_id": "y", "v": 4 }, false).unwrap();
        // A delete after the target, so the rewind brings a document back to
        // life as well as putting one back to an earlier value.
        a.delete(&coll, &id("x1")).unwrap();
        a.rewind_to(until).unwrap();
        check("a rewind");

        a.collect_garbage_at(
            crate::physical_now_ms() + 1_000_000_000,
            crate::RetentionPolicy::new(24 * 60 * 60, 0),
        )
        .unwrap();
        check("retention removing tombstones");

        a.drop_collection("app", "docs").unwrap();
        check("a drop");
        // The drop's purge finished: this test is about what comes after it.
        a.finish_purges_now().unwrap();
        let recreated = a.create_collection("app", "docs").unwrap();
        a.insert(&recreated, doc! { "_id": "again" }).unwrap();
        check("a recreate");

        // A replicated batch: one transaction holding 1,024 entries (ADR-119),
        // which is the shape whose per-entry count work this narrowed to once
        // per transaction. Last, so it perturbs none of the sequences above.
        let batch: Vec<OplogEntry> = (0..1_024u64)
            .map(|i| {
                let id = format!("b{i}");
                remote(&recreated, &id, later + 1_000 + i, Some(doc! { "_id": id.as_str() }))
            })
            .collect();
        a.apply_batch(&batch).unwrap();
        check("a 1,024-entry replicated batch");
    }

    /// A single-document write reaches for the two count tables twice: once
    /// for the count, once for the mark.
    ///
    /// The batch guard below says the per-entry work is gone. This one says the
    /// per-transaction work did not multiply while it went, which is the trap
    /// in moving work to the commit: a single-document write is one entry in
    /// one transaction, so it has no loop to amortise anything over and
    /// whatever the commit does lands on it whole.
    ///
    /// **What this cannot see.** It counts opens of the two count tables only,
    /// so a commit that re-opened the arrival index and the oplog in order to
    /// recompute the mark would still read two here — and that is a real
    /// regression, two further opens on every transaction on the node. What
    /// holds it is reading the mark at the append, where both tables are
    /// already open, and the `grown/insert_one` benchmark, which prices the
    /// whole transaction rather than counting one part of it.
    #[test]
    fn a_single_document_write_reaches_for_the_count_tables_twice() {
        let (engine, coll, _dir) = engine();
        let before = table_opens();
        engine.insert(&coll, doc! { "_id": "one" }).unwrap();
        let opened = table_opens() - before;
        assert_eq!(
            opened, 2,
            "a single-document write opened the live-count tables {opened} times; it is one \
             open for the count and one for the mark"
        );
    }

    /// The count and the mark are written once for the whole transaction, not
    /// once for every entry in it.
    ///
    /// The shape ADR-174 named and deferred. A replicated batch is one run and
    /// one transaction (ADR-119), so moving the count row and rewriting the
    /// mark's single key per entry repeated both 1,024 times inside one commit
    /// — which no commit or fsync counter can see, and which leaves the counts
    /// themselves perfectly correct. The only thing that can hold it is how
    /// often the two tables are reached for, which is what [`table_opens`]
    /// answers.
    #[test]
    fn the_count_and_the_mark_are_written_once_for_a_replicated_batch() {
        let (engine, coll, _dir) = engine();
        let later = crate::physical_now_ms() + 10_000;
        let entries: Vec<OplogEntry> = (0..1_024u64)
            .map(|i| {
                let id = format!("batch-{i}");
                remote(&coll, &id, later + i, Some(doc! { "_id": id.as_str() }))
            })
            .collect();

        let before = table_opens();
        let commits = engine.commits();
        engine.apply_batch(&entries).unwrap();
        let opened = table_opens() - before;

        // The counts are right, which they were before this too: the point is
        // what they cost, not what they say.
        assert_counts_exact(&engine, "a 1,024-entry replicated batch");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(1_024));
        // The batch is one run and one transaction (ADR-119). Asserted, so
        // that a batch which started committing per entry would be read as the
        // regression it is rather than as this guard passing for a new reason.
        assert_eq!(
            engine.commits() - commits,
            1,
            "a replicated batch is one run and one commit; this one took more"
        );
        assert_eq!(
            opened, 2,
            "a 1,024-entry batch reached for the live-count tables {opened} times. The count \
             and the mark move once per transaction, so it is two: one open of LIVE_COUNTS and \
             one of LIVE_COUNTS_THROUGH. Per entry it was 2,048."
        );
    }

    #[test]
    fn a_write_that_aborts_after_its_record_leaves_the_count_unchanged() {
        // The count moves in the transaction that writes the record, so a write
        // that fails after writing it — here a unique violation found while
        // maintaining the index, after the record is in — takes the count back
        // with it. A count kept in a transaction of its own would stay moved.
        let (engine, coll, _dir) = engine();
        engine
            .create_index(
                "app",
                "docs",
                vec![crate::meta::IndexField { path: "email".into(), descending: false }],
                true,
                None,
            )
            .unwrap();
        let coll = engine.get_collection("app", "docs").unwrap_or(coll);
        engine.insert(&coll, doc! { "_id": "a", "email": "same@x" }).unwrap();
        assert!(engine.insert(&coll, doc! { "_id": "b", "email": "same@x" }).is_err());
        assert_counts_exact(&engine, "an aborted insert");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(1));
    }

    #[test]
    fn a_missing_or_wrong_count_is_rebuilt_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let coll_id = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            for i in 0..4i64 {
                engine.insert(&coll, doc! { "_id": i }).unwrap();
            }
            engine.delete(&coll, &DocId::Int64(0)).unwrap();
            // What a restore or a first start looks like: no mark, and a count
            // that is simply wrong.
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_COUNTS).unwrap().insert(coll.id.0, 1_000).unwrap();
            txn.open_table(tables::LIVE_COUNTS_THROUGH).unwrap().remove(THROUGH).unwrap();
            txn.commit().unwrap();
            coll.id
        };
        let engine = Engine::open(&path).unwrap();
        assert_counts_exact(&engine, "an open with no mark");
        assert_eq!(engine.count_by_id(coll_id).unwrap(), Some(3));
    }

    /// Write `name` into `coll` the way a build that does not keep the counts
    /// does: the record, its oplog entry and both halves of the arrival index,
    /// and neither the count nor the mark.
    fn write_as_an_older_build(engine: &Engine, coll: &CollectionMeta, name: &str, live: bool) {
        let stamp = engine.next_stamp();
        let body = bson::serialize_to_vec(&doc! { "_id": name }).unwrap();
        let record = codec::encode_doc_record(&match live {
            true => kimmy_core::DocRecord::live(stamp, body.clone()),
            false => kimmy_core::DocRecord::tombstone(stamp),
        });
        let key = crate::docs::doc_key(&id(name)).unwrap();
        let db = engine.db();
        let txn = db.begin_write().unwrap();
        txn.open_table(tables::DOCS)
            .unwrap()
            .insert((coll.id.0, key.as_slice()), record.as_slice())
            .unwrap();
        {
            // Its entry, as any document write appends one: the oplog row
            // and both halves of the arrival index, so the index still
            // covers the oplog and is not rebuilt.
            let entry = OplogEntry {
                stamp,
                kind: if live { OpKind::Insert } else { OpKind::Delete },
                collection: coll.id,
                doc_id: Some(id(name)),
                body: live.then_some(body),
            };
            let oplog_key = codec::oplog_key(&stamp);
            txn.open_table(tables::OPLOG)
                .unwrap()
                .insert(oplog_key.as_slice(), codec::encode_oplog_entry(&entry).as_slice())
                .unwrap();
            let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL).unwrap();
            let next = arrival.last().unwrap().map_or(0, |(seq, _)| seq.value() + 1);
            arrival.insert(next, oplog_key.as_slice()).unwrap();
            txn.open_table(tables::OPLOG_ARRIVAL_SEQ)
                .unwrap()
                .insert(oplog_key.as_slice(), next)
                .unwrap();
        }
        txn.commit().unwrap();
    }

    #[test]
    fn a_rewind_leaves_the_counts_trusted_at_the_next_open() {
        // A rewind removes arrival rows and appends nothing. The mark moves
        // with it, so the next start does not walk the store to rebuild counts
        // that are exact. Proven as the clean-start test proves it: a count
        // altered behind the mark's back survives only a start that did not
        // walk.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let coll_id = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            engine.insert(&coll, doc! { "_id": "kept" }).unwrap();
            let until = engine.version_vector().unwrap().iter().map(|(_, h)| h).max().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            engine.insert(&coll, doc! { "_id": "undone" }).unwrap();
            engine.rewind_to(until).unwrap();
            assert_counts_exact(&engine, "a rewind");
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_COUNTS).unwrap().insert(coll.id.0, 7).unwrap();
            txn.commit().unwrap();
            coll.id
        };
        let engine = Engine::open(&path).unwrap();
        assert_eq!(
            engine.count_by_id(coll_id).unwrap(),
            Some(7),
            "the start after a rewind walked the store to rebuild the counts"
        );
    }

    #[test]
    fn a_build_that_writes_as_many_entries_as_a_rewind_removed_is_caught_at_open() {
        // The sequence a mark of the arrival end alone cannot see: the rewind
        // takes the end down by one, and an older build's one append brings it
        // back to exactly the old mark.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let coll_id = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            engine.insert(&coll, doc! { "_id": "kept" }).unwrap();
            let until = engine.version_vector().unwrap().iter().map(|(_, h)| h).max().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            engine.insert(&coll, doc! { "_id": "undone" }).unwrap();
            engine.rewind_to(until).unwrap();
            write_as_an_older_build(&engine, &coll, "older", true);
            coll.id
        };
        let engine = Engine::open(&path).unwrap();
        assert_counts_exact(&engine, "an open after an older build wrote behind a rewind");
        assert_eq!(engine.count_by_id(coll_id).unwrap(), Some(2));
    }

    #[test]
    fn an_older_build_rewinding_then_writing_is_caught_at_open() {
        // A 0.29.x rewind removes oplog rows and leaves the arrival index, so
        // the next start rebuilds the index, renumbered, to fewer positions.
        // One older write later, the renumbered end is the old mark again; the
        // oplog's newest key is not, which is why the mark carries both.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let coll_id = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            engine.insert(&coll, doc! { "_id": "kept" }).unwrap();
            engine.insert(&coll, doc! { "_id": "undone" }).unwrap();
            let undone = engine.read_arrival_from(0, 100).unwrap().pop().unwrap().stamp;
            // The older rewind: its entry out of the oplog, its document
            // tombstoned, the arrival index and the count left as they were.
            let key = crate::docs::doc_key(&id("undone")).unwrap();
            let tombstone =
                codec::encode_doc_record(&kimmy_core::DocRecord::tombstone(engine.next_stamp()));
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::OPLOG)
                .unwrap()
                .remove(codec::oplog_key(&undone).as_slice())
                .unwrap();
            txn.open_table(tables::DOCS)
                .unwrap()
                .insert((coll.id.0, key.as_slice()), tombstone.as_slice())
                .unwrap();
            txn.commit().unwrap();
            // Then an older delete, of the document still live.
            write_as_an_older_build(&engine, &coll, "kept", false);
            coll.id
        };
        let engine = Engine::open(&path).unwrap();
        assert_counts_exact(&engine, "an open after an older build rewound and wrote");
        assert_eq!(engine.count_by_id(coll_id).unwrap(), Some(0));
    }

    #[test]
    fn a_bare_tombstone_raises_bare_max_and_a_live_write_does_not() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "live" }).unwrap();
        let kept = |engine: &Engine| {
            let db = engine.db();
            let txn = db.begin_read().unwrap();
            let rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
            crate::live_digest::kept_row(&rows, coll.id.0).unwrap().expect("a row")
        };
        assert!(kept(&engine).bare_max.is_empty(), "a live write is no bare tombstone");

        // A tombstone with no entry behind it, as a snapshot page leaves one.
        let stamp = Stamp::new(Hlc::new(crate::physical_now_ms() + 50_000, 0), NodeId::generate());
        let key = crate::docs::doc_key(&id("gone")).unwrap();
        {
            let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            put_bare_tombstone(
                &txn,
                &mut docs,
                coll.id.0,
                &key,
                &codec::encode_doc_record(&kimmy_core::DocRecord::tombstone(stamp)),
            )
            .unwrap();
            drop(docs);
            txn.commit().unwrap();
        }
        let row = kept(&engine);
        assert_eq!(row.bare_max.get(&stamp.node), Some(&stamp.hlc));
        assert_eq!(
            row.maxw.get(&stamp.node),
            Some(&stamp.hlc),
            "a tombstone's stamp is a record stamp"
        );
        assert_digests_exact(&engine, "a bare tombstone");

        // Removing a tombstone (retention) does not lower either.
        let later = Stamp::new(Hlc::new(stamp.hlc.wall_ms + 1, 0), stamp.node);
        {
            let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            put_record(
                &txn,
                &mut docs,
                coll.id.0,
                &key,
                &codec::encode_doc_record(&kimmy_core::DocRecord::tombstone(later)),
            )
            .unwrap();
            assert!(remove_record(&txn, &mut docs, coll.id.0, &key).unwrap());
            drop(docs);
            txn.commit().unwrap();
        }
        let row = kept(&engine);
        assert_eq!(row.maxw.get(&stamp.node), Some(&later.hlc), "maxw never falls");
    }

    #[test]
    fn a_collection_created_again_under_the_same_id_starts_with_no_digest_row() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "a" }).unwrap();
        engine.delete(&coll, &id("a")).unwrap();
        let row_of = |engine: &Engine| {
            let db = engine.db();
            let txn = db.begin_read().unwrap();
            let rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
            crate::live_digest::kept_row(&rows, coll.id.0).unwrap()
        };
        assert!(row_of(&engine).is_some_and(|r| !r.maxw.is_empty()));
        // Dropped, its records removed, and created again under the same name.
        engine.drop_collection("app", "docs").unwrap();
        engine.purge_dropped_collection(coll.id).unwrap();
        let again = engine.create_collection("app", "docs").unwrap();
        assert_eq!(again.id, coll.id, "the id derives from the name");
        assert_eq!(row_of(&engine), None, "the old incarnation's stamps are gone");
    }

    #[test]
    fn a_stale_digest_mark_alone_rebuilds_both_and_a_matching_pair_does_not_walk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (coll_id, truth) = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            engine.insert(&coll, doc! { "_id": "one" }).unwrap();
            engine.insert(&coll, doc! { "_id": "two" }).unwrap();
            let db = engine.db();
            let txn = db.begin_read().unwrap();
            let rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
            let truth = crate::live_digest::kept_row(&rows, coll.id.0).unwrap().unwrap();
            (coll.id, truth)
        };
        // Both marks match: a row altered behind them survives the start,
        // which could not be if the start had walked.
        {
            let engine = Engine::open(&path).unwrap();
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            let mut forged = truth.clone();
            forged.digest ^= 0xdead;
            txn.open_table(tables::LIVE_DIGESTS)
                .unwrap()
                .insert(coll_id.0, forged.encode().as_slice())
                .unwrap();
            txn.commit().unwrap();
        }
        {
            let engine = Engine::open(&path).unwrap();
            let db = engine.db();
            let txn = db.begin_read().unwrap();
            let rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
            let kept = crate::live_digest::kept_row(&rows, coll_id.0).unwrap().unwrap();
            assert_eq!(
                kept.digest,
                truth.digest ^ 0xdead,
                "a start with both marks matching walked"
            );
        }
        // The digest mark alone missing: the counts are exact, and a walk
        // happens all the same, for the digests.
        {
            let engine = Engine::open(&path).unwrap();
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_DIGESTS_THROUGH)
                .unwrap()
                .remove(crate::live_digest::THROUGH)
                .unwrap()
                .expect("premise: a mark");
            txn.commit().unwrap();
        }
        let engine = Engine::open(&path).unwrap();
        assert_counts_exact(&engine, "an open with only the digest mark missing");
        let db = engine.db();
        let txn = db.begin_read().unwrap();
        let rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
        assert_eq!(crate::live_digest::kept_row(&rows, coll_id.0).unwrap().unwrap(), truth);
    }

    fn marks_of(engine: &Engine) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let db = engine.db();
        let txn = db.begin_read().unwrap();
        let counts = txn
            .open_table(tables::LIVE_COUNTS_THROUGH)
            .unwrap()
            .get(THROUGH)
            .unwrap()
            .map(|m| m.value().to_vec());
        let digests = txn
            .open_table(tables::LIVE_DIGESTS_THROUGH)
            .unwrap()
            .get(crate::live_digest::THROUGH)
            .unwrap()
            .map(|m| m.value().to_vec());
        (counts, digests)
    }

    /// The format number is part of the mark: the digest mark is the number,
    /// then exactly the counts' mark.
    #[test]
    fn the_digest_mark_is_the_format_number_then_the_counts_mark() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "one" }).unwrap();
        let (counts, digests) = marks_of(&engine);
        let (counts, digests) = (counts.expect("a counts mark"), digests.expect("a digest mark"));
        assert_eq!(digests[0], crate::live_digest::FORMAT);
        assert_eq!(&digests[1..], counts.as_slice());
    }

    /// `carry_mark` moves the digest mark only if it matched `before`: one
    /// that did not says a build that does not keep the digests wrote since,
    /// and moving it forward would hide that from the next open.
    #[test]
    fn the_digest_mark_is_carried_only_if_it_matched_before() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "one" }).unwrap();
        let (before, _) = marks_of(&engine);
        let before = before.unwrap();
        let carry = |engine: &Engine| {
            let txn = engine.begin_write(crate::engine::WriterHolder::Rewind).unwrap();
            carry_mark(&txn, &before, b"after").unwrap();
            txn.commit().unwrap();
        };
        let set_digest_mark = |engine: &Engine, mark: Option<&[u8]>| {
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            {
                let mut through = txn.open_table(tables::LIVE_DIGESTS_THROUGH).unwrap();
                match mark {
                    Some(mark) => {
                        through.insert(crate::live_digest::THROUGH, mark).unwrap();
                    }
                    None => {
                        through.remove(crate::live_digest::THROUGH).unwrap();
                    }
                }
            }
            txn.commit().unwrap();
        };

        // A digest mark that is not the counts' mark: left alone.
        let forged = crate::live_digest::mark_bytes(b"written behind our back");
        set_digest_mark(&engine, Some(&forged));
        carry(&engine);
        assert_eq!(marks_of(&engine).1, Some(forged), "a mark that did not match moved");
        // Absent: stays absent.
        set_digest_mark(&engine, None);
        let (counts_now, _) = marks_of(&engine);
        assert_eq!(counts_now.as_deref(), Some(&b"after"[..]), "the counts mark carried");
        // Matching: moves.
        let (engine, coll, _dir) = self::engine();
        engine.insert(&coll, doc! { "_id": "one" }).unwrap();
        let (before_2, matching) = marks_of(&engine);
        assert_eq!(matching, Some(crate::live_digest::mark_bytes(&before_2.clone().unwrap())));
        {
            let txn = engine.begin_write(crate::engine::WriterHolder::Rewind).unwrap();
            carry_mark(&txn, &before_2.unwrap(), b"after").unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(marks_of(&engine).1, Some(crate::live_digest::mark_bytes(b"after")));
        // And absent stays absent, on a mark that was absent from the start.
        set_digest_mark(&engine, None);
        let (counts_before, _) = marks_of(&engine);
        {
            let txn = engine.begin_write(crate::engine::WriterHolder::Rewind).unwrap();
            carry_mark(&txn, &counts_before.unwrap(), b"later").unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(marks_of(&engine).1, None, "a missing digest mark was made");
    }

    #[test]
    fn a_digest_mark_from_another_format_is_not_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            engine.insert(&coll, doc! { "_id": "one" }).unwrap();
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            let mut through = txn.open_table(tables::LIVE_DIGESTS_THROUGH).unwrap();
            let mut mark = through
                .get(crate::live_digest::THROUGH)
                .unwrap()
                .map(|m| m.value().to_vec())
                .expect("premise: a mark");
            mark[0] = crate::live_digest::FORMAT + 1;
            through.insert(crate::live_digest::THROUGH, mark.as_slice()).unwrap();
            drop(through);
            txn.open_table(tables::LIVE_DIGESTS).unwrap().retain(|_, _| false).unwrap();
            txn.commit().unwrap();
        }
        let engine = Engine::open(&path).unwrap();
        assert_digests_exact(&engine, "an open under a mark of another digest format");
    }

    #[test]
    fn an_unreadable_digest_row_does_not_fail_a_write_and_is_started_afresh() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "one" }).unwrap();
        {
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_DIGESTS)
                .unwrap()
                .insert(coll.id.0, &[9u8, 9, 9][..])
                .unwrap();
            txn.commit().unwrap();
        }
        engine.insert(&coll, doc! { "_id": "two" }).expect("a derived row must not refuse a write");
        let db = engine.db();
        let txn = db.begin_read().unwrap();
        let rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
        assert!(
            crate::live_digest::kept_row(&rows, coll.id.0).unwrap().is_some(),
            "the row was replaced by one that reads"
        );
    }

    #[test]
    fn retention_carries_both_marks_when_it_moves_the_arrival_end() {
        // Retention removes the oplog row an entry arrived last as when its stamp
        // is old and a later one of its origin stands, so the arrival index's end
        // moves and the mark has to move with it, for both tables (B3 design 2.2).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (coll_id, forged_digest) = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            engine.insert(&coll, doc! { "_id": "kept" }).unwrap();
            let origin = NodeId::generate();
            let entry = |wall_ms: u64, name: &str| OplogEntry {
                stamp: Stamp::new(Hlc::new(wall_ms, 0), origin),
                kind: OpKind::Insert,
                collection: coll.id,
                doc_id: Some(DocId::String(name.into())),
                body: Some(bson::serialize_to_vec(&doc! { "_id": name }).unwrap()),
            };
            // The newer arrives first, so the older arrives last, and is what
            // retention removes (the newer is its origin's newest, and stays).
            engine.apply_remote(&coll, &entry(2, "newer")).unwrap();
            engine.apply_remote(&coll, &entry(1, "older")).unwrap();
            let before = {
                let db = engine.db();
                let txn = db.begin_read().unwrap();
                mark_of(
                    &txn.open_table(tables::OPLOG_ARRIVAL).unwrap(),
                    &txn.open_table(tables::OPLOG).unwrap(),
                )
                .unwrap()
            };
            std::thread::sleep(std::time::Duration::from_millis(5));
            engine.collect_garbage(crate::RetentionPolicy::new(0, 3_600)).unwrap();
            let after = {
                let db = engine.db();
                let txn = db.begin_read().unwrap();
                mark_of(
                    &txn.open_table(tables::OPLOG_ARRIVAL).unwrap(),
                    &txn.open_table(tables::OPLOG).unwrap(),
                )
                .unwrap()
            };
            assert_ne!(before, after, "the premise: retention moved the arrival end");
            assert_digests_exact(&engine, "retention");
            // Forge a count and a digest behind the marks.
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_COUNTS).unwrap().insert(coll.id.0, 77).unwrap();
            let mut rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
            let mut row = crate::live_digest::kept_row(&rows, coll.id.0).unwrap().unwrap();
            row.digest ^= 0x1234;
            rows.insert(coll.id.0, row.encode().as_slice()).unwrap();
            drop(rows);
            txn.commit().unwrap();
            (coll.id, row)
        };
        let engine = Engine::open(&path).unwrap();
        assert_eq!(
            engine.count_by_id(coll_id).unwrap(),
            Some(77),
            "the counts' mark was carried, so the start did not walk"
        );
        let db = engine.db();
        let txn = db.begin_read().unwrap();
        let rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
        assert_eq!(
            crate::live_digest::kept_row(&rows, coll_id.0).unwrap().unwrap(),
            forged_digest,
            "the digest mark was carried too"
        );
    }

    #[test]
    fn retention_and_a_rewind_carry_the_digest_mark_with_the_counts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (coll_id, forged) = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            engine.insert(&coll, doc! { "_id": "kept" }).unwrap();
            let until = engine.version_vector().unwrap().iter().map(|(_, h)| h).max().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
            engine.insert(&coll, doc! { "_id": "undone" }).unwrap();
            engine.rewind_to(until).unwrap();
            assert_digests_exact(&engine, "a rewind");
            // A row altered behind the marks survives a start only if the
            // start did not walk, which is only so if the rewind carried the
            // digest mark as it carried the counts'.
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            let mut rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
            let mut row = crate::live_digest::kept_row(&rows, coll.id.0).unwrap().unwrap();
            row.digest ^= 0xbeef;
            rows.insert(coll.id.0, row.encode().as_slice()).unwrap();
            drop(rows);
            txn.commit().unwrap();
            (coll.id, row)
        };
        let engine = Engine::open(&path).unwrap();
        let db = engine.db();
        let txn = db.begin_read().unwrap();
        let rows = txn.open_table(tables::LIVE_DIGESTS).unwrap();
        assert_eq!(
            crate::live_digest::kept_row(&rows, coll_id.0).unwrap().unwrap(),
            forged,
            "the start after a rewind walked the store to rebuild digests that were exact"
        );
    }

    /// Every `DOCS` row, key and value, as bytes.
    fn docs_rows(path: &std::path::Path) -> Vec<(u64, Vec<u8>, Vec<u8>)> {
        let db = redb::Database::open(path).unwrap();
        let txn = db.begin_read().unwrap();
        txn.open_table(tables::DOCS)
            .unwrap()
            .iter()
            .unwrap()
            .map(|row| {
                let (key, value) = row.unwrap();
                let (collection, doc_key) = key.value();
                (collection, doc_key.to_vec(), value.value().to_vec())
            })
            .collect()
    }

    /// B3 design 8.9: the resume rule rests on an open never changing a
    /// document record before it commits the counts mark, so that a missing
    /// counts mark means no earlier build has served since a walk began.
    /// The premise is read from the code (`DOCS` has one open-time writer,
    /// `move_documents`, and only on a schema-1 store) and kept true here: an
    /// open of a schema-current store that finds the mark missing, the counts
    /// wrong, the arrival index's stamp half short and the vector unverified
    /// repairs every one of them, leaves each `DOCS` row byte for byte as it
    /// found it, and commits the mark.
    #[test]
    fn an_open_that_finds_every_derived_table_stale_leaves_the_records_alone_and_commits_the_mark()
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (orders, notes) = {
            let engine = Engine::open(&path).unwrap();
            let orders = engine.create_collection("app", "orders").unwrap();
            let notes = engine.create_collection("app", "notes").unwrap();
            for i in 0..6i64 {
                engine.insert(&orders, doc! { "_id": i, "n": i }).unwrap();
            }
            engine.insert(&notes, doc! { "_id": "a" }).unwrap();
            engine.delete(&orders, &DocId::Int64(2)).unwrap();
            engine.delete(&orders, &DocId::Int64(5)).unwrap();
            // What a first start of a build that keeps the counts finds, and
            // what the open's other repairs find: the mark gone, a count
            // wrong, one row of the arrival index's stamp half lost, the
            // vector's verification record gone.
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_COUNTS).unwrap().insert(orders.id.0, 1_000).unwrap();
            txn.open_table(tables::LIVE_COUNTS_THROUGH).unwrap().remove(THROUGH).unwrap();
            {
                let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
                let first = by_stamp.first().unwrap().map(|(k, _)| k.value().to_vec()).unwrap();
                by_stamp.remove(first.as_slice()).unwrap();
            }
            {
                let mut verified = txn.open_table(tables::VECTOR_VERIFIED).unwrap();
                verified.retain(|_, _| false).unwrap();
            }
            txn.commit().unwrap();
            (orders.id, notes.id)
        };
        let before = docs_rows(&path);
        assert!(!before.is_empty());

        let engine = crate::verified::test_support::forcing(|| Engine::open(&path).unwrap());
        assert_counts_exact(&engine, "an open that found every derived table stale");
        assert_eq!(engine.count_by_id(orders).unwrap(), Some(4));
        assert_eq!(engine.count_by_id(notes).unwrap(), Some(1));
        let db = engine.db();
        let txn = db.begin_read().unwrap();
        assert!(
            counts_are_current(&txn).unwrap(),
            "the open committed the counts mark before it returned"
        );
        drop(txn);
        drop(engine);

        assert_eq!(
            docs_rows(&path),
            before,
            "an open changed a document record ahead of the counts mark"
        );
    }

    #[test]
    fn a_clean_start_trusts_the_counts_and_does_not_walk() {
        // The mark is what spares an ordinary start the walk. Proven the only
        // way a skipped rebuild can be seen: a count altered behind the mark's
        // back survives the restart, which it could not if the start had
        // walked the records.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let coll_id = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            engine.insert(&coll, doc! { "_id": "one" }).unwrap();
            let db = engine.db();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_COUNTS).unwrap().insert(coll.id.0, 7).unwrap();
            txn.commit().unwrap();
            coll.id
        };
        let engine = Engine::open(&path).unwrap();
        assert_eq!(
            engine.count_by_id(coll_id).unwrap(),
            Some(7),
            "a start with the mark at the arrival index's end rebuilt anyway"
        );
    }

    #[test]
    fn a_write_by_a_build_that_does_not_keep_the_counts_is_caught_at_open() {
        // An older build writes a document and appends its entry, and moves
        // neither the count nor the mark. The mark then trails the arrival
        // index, and the next open rebuilds.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let coll_id = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            engine.insert(&coll, doc! { "_id": "kept" }).unwrap();
            write_as_an_older_build(&engine, &coll, "older", true);
            coll.id
        };
        let engine = Engine::open(&path).unwrap();
        assert_counts_exact(&engine, "an open after an older build wrote");
        assert_eq!(engine.count_by_id(coll_id).unwrap(), Some(2));
    }

    /// Every write to `DOCS` goes through this module, or a count drifts.
    #[test]
    fn every_document_write_moves_the_count() {
        // Exempt: `migrate.rs` stamps the version of a fresh store during
        // `Engine::open`; `backup.rs` restores into a file no engine has opened,
        // which the first open rebuilds; `faults.rs` stores test-only records that
        // are not documents, for readers that must survive them; and this module.
        const EXEMPT: [&str; 4] = ["live_count.rs", "migrate.rs", "backup.rs", "faults.rs"];
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = Vec::new();
        for entry in std::fs::read_dir(&src).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if path.extension().is_none_or(|e| e != "rs") || EXEMPT.contains(&name.as_str()) {
                continue;
            }
            let body = std::fs::read_to_string(&path).unwrap();
            // Whatever name the table is opened under: `let mut <name> =
            // ...open_table(...tables::DOCS)`, so a write through a variable
            // not called `docs` is caught as well (B3 design 2.2).
            let mut names = vec!["docs".to_string()];
            for line in body.lines() {
                if line.contains("open_table")
                    && line.contains("tables::DOCS")
                    && let Some(rest) = line.trim_start().strip_prefix("let mut ")
                    && let Some(name) = rest.split([' ', ':']).next()
                    && !names.iter().any(|n| n == name)
                {
                    names.push(name.to_string());
                }
            }
            for (n, line) in body.lines().enumerate() {
                // `docs.retain` matches `retain_in` too. The rest are every
                // other method redb's `Table` offers that removes or replaces a
                // row.
                if names.iter().any(|name| {
                    [".insert(", ".remove(", ".retain", ".pop_", ".drain", ".extract"]
                        .iter()
                        .any(|w| line.contains(&format!("{name}{w}")))
                }) {
                    offenders.push(format!("{name}:{}: {}", n + 1, line.trim()));
                    continue;
                }
                if [
                    "docs.insert(",
                    "docs.remove(",
                    "docs.retain",
                    "docs.pop_",
                    "docs.drain",
                    "docs.extract",
                ]
                .iter()
                .any(|w| line.contains(w))
                {
                    offenders.push(format!("{name}:{}: {}", n + 1, line.trim()));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "these write DOCS without moving the live count, which then drifts:\n  {}",
            offenders.join("\n  ")
        );
    }

    /// A name dropped and created again derives the same id, so creating a
    /// collection resets its digest row (B3 design 2.1): the hook is a line in
    /// the creation path, and a refactor that drops it is caught here.
    #[test]
    fn creating_a_collection_resets_its_digest_row_in_the_creation_path() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine.rs");
        let body = std::fs::read_to_string(src).unwrap();
        let insert = body
            .find("collections.insert((db, name), serde_json::to_vec(&meta)?.as_slice())?;")
            .expect("the creation path inserts the collection's row");
        let after = &body[insert..insert + 600];
        assert!(
            after.contains("crate::live_digest::reset(txn, id)?;"),
            "the creation path must reset the digest row right after it inserts the collection"
        );
    }

    fn live_record(engine: &Engine, name: &str) -> Vec<u8> {
        let body = bson::serialize_to_vec(&doc! { "_id": name }).unwrap();
        codec::encode_doc_record(&kimmy_core::DocRecord::live(engine.next_stamp(), body))
    }

    fn tomb_record(engine: &Engine) -> Vec<u8> {
        codec::encode_doc_record(&kimmy_core::DocRecord::tombstone(engine.next_stamp()))
    }

    fn write_live(engine: &Engine, txn: &crate::engine::WriteTxn<'_>, coll: u64, names: &[&str]) {
        let mut docs = txn.open_table(tables::DOCS).unwrap();
        for name in names {
            let key = crate::docs::doc_key(&id(name)).unwrap();
            let rec = live_record(engine, name);
            put_record(txn, &mut docs, coll, &key, &rec).unwrap();
        }
    }

    /// A panic part-way through a transaction loses the deltas with the
    /// records, and leaves the writer free for the next write.
    #[test]
    fn a_panic_mid_transaction_loses_the_deltas_with_the_records() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "kept" }).unwrap();
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(1));

        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
            write_live(&engine, &txn, coll.id.0, &["p0", "p1", "p2", "p3"]);
            panic!("probe: unwinding with four pending deltas");
        }));
        assert!(caught.is_err(), "the probe must actually panic");

        assert_counts_exact(&engine, "a panic mid-transaction");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(1));
        // The writer is not left held and no delta survived into the next one.
        engine.insert(&coll, doc! { "_id": "after" }).unwrap();
        assert_counts_exact(&engine, "a write after a panic");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(2));
    }

    /// A transaction dropped rather than committed or aborted loses its
    /// deltas, as redb loses the records it held.
    #[test]
    fn a_dropped_transaction_loses_its_deltas() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "kept" }).unwrap();
        {
            let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
            write_live(&engine, &txn, coll.id.0, &["d0", "d1"]);
            drop(txn);
        }
        assert_counts_exact(&engine, "a dropped transaction");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(1));
        engine.insert(&coll, doc! { "_id": "after" }).unwrap();
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(2));
    }

    /// The same for an explicit abort: the deltas go with the records.
    #[test]
    fn an_explicit_abort_loses_its_deltas() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "kept" }).unwrap();
        {
            let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
            write_live(&engine, &txn, coll.id.0, &["a0", "a1", "a2"]);
            txn.abort().unwrap();
        }
        assert_counts_exact(&engine, "an explicit abort");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(1));
    }

    /// A transaction whose deltas cancel out must not touch the row at all.
    ///
    /// Checked as a value as well as an open count: the count is set wrong
    /// first, so "left alone" is visible as the wrong value surviving.
    #[test]
    fn a_net_zero_transaction_touches_no_count_row() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "a" }).unwrap();
        engine.insert(&coll, doc! { "_id": "b" }).unwrap();
        // A count deliberately wrong, so "left alone" is visible as a value
        // and not only as an open count.
        {
            let t = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
            t.open_table(tables::LIVE_COUNTS).unwrap().insert(coll.id.0, 9).unwrap();
            t.commit().unwrap();
        }
        let before = table_opens();
        {
            let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
            {
                let mut docs = txn.open_table(tables::DOCS).unwrap();
                let key = crate::docs::doc_key(&id("z")).unwrap();
                let live = live_record(&engine, "z");
                put_record(&txn, &mut docs, coll.id.0, &key, &live).unwrap();
                let tomb = tomb_record(&engine);
                put_record(&txn, &mut docs, coll.id.0, &key, &tomb).unwrap();
            }
            txn.commit().unwrap();
        }
        assert_eq!(
            table_opens() - before,
            0,
            "a transaction whose deltas cancelled, and which appended nothing, still opened a \
             count table"
        );
        assert_eq!(
            engine.count_by_id(coll.id).unwrap(),
            Some(9),
            "a net-zero transaction rewrote a row it should have left alone"
        );
    }

    /// Two collections moved in one transaction land on their own rows, in
    /// opposite directions.
    #[test]
    fn deltas_are_keyed_per_collection_within_one_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let a = engine.create_collection("app", "a").unwrap();
        let b = engine.create_collection("app", "b").unwrap();
        engine.insert(&b, doc! { "_id": "b1" }).unwrap();
        engine.insert(&b, doc! { "_id": "b2" }).unwrap();
        {
            let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
            write_live(&engine, &txn, a.id.0, &["a1", "a2", "a3"]);
            {
                let mut docs = txn.open_table(tables::DOCS).unwrap();
                let key = crate::docs::doc_key(&id("b1")).unwrap();
                let tomb = tomb_record(&engine);
                put_record(&txn, &mut docs, b.id.0, &key, &tomb).unwrap();
            }
            txn.commit().unwrap();
        }
        assert_counts_exact(&engine, "two collections moved in one transaction");
        assert_eq!(engine.count_by_id(a.id).unwrap(), Some(3));
        assert_eq!(engine.count_by_id(b.id).unwrap(), Some(1));
    }

    /// A schema change mid-batch ends the run and starts another, and the
    /// counts survive the split.
    ///
    /// The one case where a batch is more than one transaction (ADR-119), so
    /// the accumulator of each run has to land on its own.
    #[test]
    fn a_batch_with_a_ddl_entry_mid_run_keeps_the_counts() {
        let (a, docs_a, _da) = engine();
        let dir = tempfile::tempdir().unwrap();
        let b = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        b.create_collection("app", "docs").unwrap();

        for n in 0..3i64 {
            a.insert(&docs_a, doc! { "_id": n }).unwrap();
        }
        a.delete(&docs_a, &DocId::Int64(0)).unwrap();
        let items = a.create_collection("app", "items").unwrap();
        for n in 0..3i64 {
            a.insert(&items, doc! { "_id": n }).unwrap();
        }
        a.delete(&items, &DocId::Int64(1)).unwrap();

        let entries: Vec<OplogEntry> = a
            .entries_for_peer(Hlc::ZERO, 4_096, crate::WalkScope::Background)
            .unwrap()
            .entries
            .into_iter()
            .filter(|e| e.kind.is_document() || e.collection == items.id)
            .collect();
        assert!(
            entries.iter().any(|e| e.kind.is_ddl()),
            "the probe needs a schema change in the batch"
        );
        let outcome = b.apply_batch(&entries).unwrap();
        assert_eq!(outcome.ddl, 1, "{outcome:?}");

        assert_counts_exact(&b, "a batch with a schema change mid-run");
        assert_eq!(b.count_by_id(docs_a.id).unwrap(), Some(2));
        assert_eq!(b.count_by_id(items.id).unwrap(), Some(2));
    }

    /// The local bulk paths keep the counts: `insert_many`, a `write_batch` of
    /// deletes and upserts, a closure that fails, and a bulk insert that
    /// collides part-way.
    ///
    /// The verifier walks the single-document paths; these are the two that
    /// gather many deltas before one commit, and the two whose failure arms
    /// abort a transaction with an accumulator already populated.
    #[test]
    fn insert_many_and_write_batch_keep_the_counts() {
        let (engine, coll, _dir) = engine();
        engine
            .insert_many(&coll, (0..50).map(|i| doc! { "_id": format!("m{i}") }).collect())
            .unwrap();
        assert_counts_exact(&engine, "insert_many");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(50));

        engine
            .write_batch(crate::engine::WriterHolder::Bulk, |scope| {
                for i in 0..10 {
                    scope.delete(&coll, &id(&format!("m{i}")))?;
                }
                for i in 50..60 {
                    let name = format!("m{i}");
                    scope.replace(&coll, &id(&name), doc! { "_id": name.as_str() }, true)?;
                }
                Ok(())
            })
            .unwrap();
        assert_counts_exact(&engine, "a write_batch of deletes and upserts");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(50));

        let failed = engine.write_batch(crate::engine::WriterHolder::Bulk, |scope| {
            scope.replace(&coll, &id("m99"), doc! { "_id": "m99" }, true)?;
            Err::<(), _>(crate::error::StorageError::Database("probe".into()))
        });
        assert!(failed.is_err());
        assert_counts_exact(&engine, "a write_batch aborted by its closure");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(50));

        // A bulk insert that collides mid-batch: aborted after some records
        // are already in the transaction, with their deltas gathered.
        let collide = engine.insert_many(
            &coll,
            vec![doc! { "_id": "n1" }, doc! { "_id": "n2" }, doc! { "_id": "m20" }],
        );
        assert!(collide.is_err());
        assert_counts_exact(&engine, "a bulk insert aborted mid-batch");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(50));
    }

    /// A drop purge takes the count down across its chunks and leaves no row
    /// behind.
    ///
    /// The purge is the one count-moving path that runs in many transactions
    /// rather than one, a chunk at a time (ADR-158), so what is checked is
    /// that every chunk's accumulator lands and none leaves a residue: the
    /// dropped collection ends with no row at all, not a row reading zero.
    #[test]
    fn a_drop_purge_moves_the_count_down_in_chunks() {
        let (engine, coll, _dir) = engine();
        for i in 0..300 {
            engine.insert(&coll, doc! { "_id": format!("d{i}") }).unwrap();
        }
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(300));
        engine.drop_collection("app", "docs").unwrap();
        engine.finish_purges_now().unwrap();
        assert_counts_exact(&engine, "a drop purge");
        let db = engine.db();
        let r = db.begin_read().unwrap();
        assert_eq!(
            r.open_table(tables::LIVE_COUNTS).unwrap().get(coll.id.0).unwrap().map(|v| v.value()),
            None,
            "the purged collection kept a count row"
        );
    }

    /// The `carry_mark` guard fires when one transaction both appends and
    /// carries.
    ///
    /// A `debug_assert` compiles out of a release build, so this is gated to
    /// the profile that has one to catch — otherwise `cargo test --release`
    /// would fail on a guard that is simply not there.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "a transaction that appended is also carrying the mark")]
    fn the_carry_guard_fires_on_a_transaction_that_appended() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "one" }).unwrap();
        let txn = engine.begin_write(crate::engine::WriterHolder::Rewind).unwrap();
        let entry = OplogEntry {
            stamp: engine.next_stamp(),
            kind: OpKind::Insert,
            collection: coll.id,
            doc_id: Some(id("two")),
            body: Some(bson::serialize_to_vec(&doc! { "_id": "two" }).unwrap()),
        };
        crate::engine::append_oplog(&txn, &entry).unwrap();
        carry_mark(&txn, b"before", b"after").unwrap();
    }

    /// The same guard is blind to the other order: carry first, then append,
    /// and the commit's flush writes over the carry.
    ///
    /// Pinned rather than fixed. The flush writes the mark the append read,
    /// which is the right one either way, so the blind side is benign — but it
    /// is a gap, and a test is what keeps it a known one.
    #[test]
    fn the_carry_guard_is_blind_to_carry_then_append() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": "one" }).unwrap();
        // The mark as it stands, and a deliberately stale "before" so the
        // carry refuses -- which is the case the assert exists to protect.
        let stored_before = {
            let db = engine.db();
            let r = db.begin_read().unwrap();
            let t = r.open_table(tables::LIVE_COUNTS_THROUGH).unwrap();
            t.get(THROUGH).unwrap().unwrap().value().to_vec()
        };
        let txn = engine.begin_write(crate::engine::WriterHolder::Rewind).unwrap();
        // The carry MATCHES, so it takes -- and then the append's flush lands
        // on top of it at the commit.
        carry_mark(&txn, &stored_before, b"a-forged-mark").unwrap();
        let entry = OplogEntry {
            stamp: engine.next_stamp(),
            kind: OpKind::Insert,
            collection: coll.id,
            doc_id: Some(id("two")),
            body: Some(bson::serialize_to_vec(&doc! { "_id": "two" }).unwrap()),
        };
        crate::engine::append_oplog(&txn, &entry).unwrap();
        // No assertion fired, and the flush wrote the append's fresh mark.
        txn.commit().unwrap();
        let db = engine.db();
        let r = db.begin_read().unwrap();
        let stored =
            r.open_table(tables::LIVE_COUNTS_THROUGH).unwrap().get(THROUGH).unwrap().unwrap();
        assert_ne!(stored.value(), b"a-forged-mark", "the carry survived the flush");
    }

    /// A batch in which every entry is superseded writes nothing, and reaches
    /// for no count table beyond the one the run's own commit takes.
    #[test]
    fn a_wholly_superseded_batch_touches_no_count_table() {
        let (engine, coll, _dir) = engine();
        let later = crate::physical_now_ms() + 10_000;
        let entries: Vec<OplogEntry> = (0..64u64)
            .map(|i| {
                let name = format!("s{i}");
                remote(&coll, &name, later + i, Some(doc! { "_id": name.as_str() }))
            })
            .collect();
        engine.apply_batch(&entries).unwrap();
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(64));

        // The same batch again: every entry is now superseded by its own
        // equal stamp, so nothing is written.
        let before = table_opens();
        let outcome = engine.apply_batch(&entries).unwrap();
        assert_eq!(outcome.applied, 0, "{outcome:?}");
        let opened = table_opens() - before;
        assert_counts_exact(&engine, "a wholly superseded batch");
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(64));
        assert!(opened <= 2, "a wholly superseded batch opened the count tables {opened} times");
    }
}
