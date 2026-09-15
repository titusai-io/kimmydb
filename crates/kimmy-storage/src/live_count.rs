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
//! **Rebuilt when it may be stale.** `Engine::open` walks every record's header
//! and rewrites the table unless [`tables::LIVE_COUNTS_THROUGH`] names the
//! arrival index's next position. The mark is written with every oplog append
//! by a build that keeps the count, so it is missing on a database no such
//! build has opened and on one restored from a backup (which carries neither
//! table), and it trails the index after a build that does not keep the count
//! wrote a document — every document write appends to the oplog.

use std::collections::BTreeMap;

use redb::{Database, ReadableTable, Table, WriteTransaction};
use tracing::{info, warn};

use crate::codec;
use crate::error::Result;
use crate::tables;

/// The `DOCS` table, open for writing.
pub(crate) type Docs<'txn> = Table<'txn, (u64, &'static [u8]), &'static [u8]>;

/// The one key of [`tables::LIVE_COUNTS_THROUGH`].
pub(crate) const THROUGH: &str = "arrival";

/// Write the encoded `record` at `key` of `collection`, and move the
/// collection's live count by what the write did to liveness there.
pub(crate) fn put_record(
    txn: &WriteTransaction,
    docs: &mut Docs<'_>,
    collection: u64,
    key: &[u8],
    record: &[u8],
) -> Result<()> {
    let was_live = match docs.insert((collection, key), record)? {
        Some(previous) => codec::doc_record_is_live(previous.value())?,
        None => false,
    };
    adjust(txn, collection, was_live, codec::doc_record_is_live(record)?)
}

/// Remove the record at `key` of `collection`, and move the collection's live
/// count down if it was live. Whether a record was there.
pub(crate) fn remove_record(
    txn: &WriteTransaction,
    docs: &mut Docs<'_>,
    collection: u64,
    key: &[u8],
) -> Result<bool> {
    let was_live = match docs.remove((collection, key))? {
        Some(previous) => codec::doc_record_is_live(previous.value())?,
        None => return Ok(false),
    };
    adjust(txn, collection, was_live, false)?;
    Ok(true)
}

fn adjust(txn: &WriteTransaction, collection: u64, was_live: bool, is_live: bool) -> Result<()> {
    if was_live == is_live {
        return Ok(());
    }
    let mut counts = txn.open_table(tables::LIVE_COUNTS)?;
    let current = counts.get(collection)?.map(|n| n.value()).unwrap_or(0);
    let next = match is_live {
        true => current + 1,
        // Saturating rather than failing the write: a count already wrong is
        // repaired at the next open, and refusing a delete for it would turn a
        // wrong number into lost availability.
        false => current.saturating_sub(1),
    };
    match next {
        0 => {
            counts.remove(collection)?;
        }
        n => {
            counts.insert(collection, n)?;
        }
    }
    Ok(())
}

/// The kept live count of collection `id`, read in `txn`.
pub(crate) fn live_count(txn: &redb::ReadTransaction, id: kimmy_core::CollectionId) -> Result<u64> {
    let counts = txn.open_table(tables::LIVE_COUNTS)?;
    Ok(counts.get(id.0)?.map(|n| n.value()).unwrap_or(0))
}

/// Record, in the transaction that appended it, that the counts are kept
/// through arrival position `next` (the position after the entry appended).
pub(crate) fn mark_through(txn: &WriteTransaction, next: u64) -> Result<()> {
    txn.open_table(tables::LIVE_COUNTS_THROUGH)?.insert(THROUGH, next)?;
    Ok(())
}

/// Rebuild every count from the records' headers, unless the mark says the
/// counts were kept through the arrival index's current end.
///
/// One transaction, and a walk of the whole of `DOCS`, paid only when the mark
/// is missing or behind: the first start of a build that keeps the count, a
/// restore, or a start after an older build wrote.
pub(crate) fn rebuild_if_stale(db: &Database) -> Result<()> {
    let started = std::time::Instant::now();
    let txn = db.begin_write()?;
    let next = txn.open_table(tables::OPLOG_ARRIVAL)?.last()?.map_or(0, |(seq, _)| seq.value() + 1);
    let through = txn.open_table(tables::LIVE_COUNTS_THROUGH)?.get(THROUGH)?.map(|n| n.value());
    if through == Some(next) {
        return Ok(());
    }

    let mut counts: BTreeMap<u64, u64> = BTreeMap::new();
    let mut records = 0usize;
    {
        let docs = txn.open_table(tables::DOCS)?;
        for row in docs.iter()? {
            let (key, value) = row?;
            records += 1;
            match codec::doc_record_is_live(value.value()) {
                Ok(true) => *counts.entry(key.value().0).or_default() += 1,
                Ok(false) => {}
                // Not counted, as no reader can return it either; refusing to
                // open over one unreadable record would be worse than a count
                // one short.
                Err(e) => warn!(error = %e, "unreadable document record not counted"),
            }
        }
    }
    {
        let mut table = txn.open_table(tables::LIVE_COUNTS)?;
        table.retain(|_, _| false)?;
        for (id, n) in &counts {
            table.insert(*id, *n)?;
        }
    }
    mark_through(&txn, next)?;
    txn.commit()?;
    // Before the node serves anything: `Engine::open` has not returned. On a
    // large store with a cold page cache the walk runs at the disk's speed.
    info!(
        collections = counts.len(),
        records,
        elapsed_ms = started.elapsed().as_millis() as u64,
        previous_mark = ?through,
        arrival = next,
        "rebuilt the live document counts before serving"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use bson::{Document, doc};
    use kimmy_core::{DocId, Hlc, NodeId, OpKind, OplogEntry, Stamp};
    use redb::ReadableDatabase;

    use super::*;
    use crate::meta::CollectionMeta;
    use crate::{Candidates, Engine, ModifySpec};

    /// Every kept count equals a walk of the records' headers, and no count is
    /// kept for a collection with no live record.
    fn assert_counts_exact(engine: &Engine, after: &str) {
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
    }

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
        fn matches(&self, doc: &Document) -> bool {
            doc.get_str("_id").is_ok_and(|id| id == self.id)
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
        let recreated = a.create_collection("app", "docs").unwrap();
        a.insert(&recreated, doc! { "_id": "again" }).unwrap();
        check("a recreate");
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
            let stamp = engine.next_stamp();
            let record = codec::encode_doc_record(&kimmy_core::DocRecord::live(
                stamp,
                bson::serialize_to_vec(&doc! { "_id": "older" }).unwrap(),
            ));
            let key = crate::docs::doc_key(&id("older")).unwrap();
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
                    kind: OpKind::Insert,
                    collection: coll.id,
                    doc_id: Some(id("older")),
                    body: Some(bson::serialize_to_vec(&doc! { "_id": "older" }).unwrap()),
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
            coll.id
        };
        let engine = Engine::open(&path).unwrap();
        assert_counts_exact(&engine, "an open after an older build wrote");
        assert_eq!(engine.count_by_id(coll_id).unwrap(), Some(2));
    }

    /// Every write to `DOCS` goes through this module, or a count drifts.
    #[test]
    fn every_document_write_moves_the_count() {
        // Exempt: `migrate.rs` moves rows between ids during `Engine::open`,
        // before the counts are rebuilt; `backup.rs` restores into a file no
        // engine has opened, which the first open rebuilds; and this module.
        const EXEMPT: [&str; 3] = ["live_count.rs", "migrate.rs", "backup.rs"];
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = Vec::new();
        for entry in std::fs::read_dir(&src).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if path.extension().is_none_or(|e| e != "rs") || EXEMPT.contains(&name.as_str()) {
                continue;
            }
            let body = std::fs::read_to_string(&path).unwrap();
            for (n, line) in body.lines().enumerate() {
                if ["docs.insert(", "docs.remove(", "docs.retain"].iter().any(|w| line.contains(w))
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
}
