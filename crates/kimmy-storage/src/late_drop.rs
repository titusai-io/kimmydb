//! A drop that reaches a newer life of its collection (ADR-231).
//!
//! A collection's id derives from its name, so two members can create the same
//! name independently, and a drop made on one of them can reach the other after
//! that member has made a life of its own. [`aims_at_a_previous_incarnation`]
//! judges such a drop to be aimed at a life already over here and ignores it.
//! That was right for the collection and wrong for what the other life had put
//! in it: a document of the life before, stamped at or before the drop, that
//! arrived **before** the drop was known (entries arrive in stamp order, so the
//! document precedes the drop that destroys it) was applied into the newer
//! life, and nothing removed it. The member that made the drop had removed it
//! with the drop; the other kept it for good, the counts differed, and the
//! count half's repair ran again every few seconds without converging.
//!
//! The rule here is ADR-081's incarnation floor, extended to the late drop. A
//! collection whose `created` is above the drop and whose floor is below it
//! **owes a sweep**: it removes every record stamped at or before the drop, as
//! a replicated delete at the drop's stamp (no new entry: every member does the
//! same on learning of the drop), indexes and vector rows with them, in chunks
//! that look for a stop between two, and then **sets its floor at the drop**,
//! which is also what says the sweep was done. A record at or before the drop
//! in a life created after it can only be a straggler of the life before:
//! every write this member mints is above its `created`, and the creation was
//! stamped by a clock that had witnessed what it replicated.
//!
//! The floor is set last, so a sweep that was cut short owes again, to the
//! entry that comes round once more and to the start's reconcile
//! ([`Engine::reconcile_late_drops`]), which also heals a store that holds the
//! stragglers of a drop applied before this rule existed.

use kimmy_core::{Hlc, Stamp};
use redb::ReadableTable;
use tracing::{debug, info};

use crate::codec;
use crate::engine::{Engine, WriterHolder, doc_range_after};
use crate::error::Result;
use crate::meta::CollectionMeta;
use crate::tables;

/// Records examined in one chunk, and removed at most: one transaction holds
/// the writer for no more than this.
const SWEEP_CHUNK: usize = 1_000;

/// Whether a collection standing here owes a sweep for a drop stamped
/// `dropped`: its life began after the drop (`created` above it) and its floor
/// has not been taken up to it.
pub(crate) fn owes_a_sweep(standing: &CollectionMeta, dropped: Hlc) -> bool {
    dropped < standing.created && standing.incarnation_floor.is_none_or(|floor| floor < dropped)
}

impl Engine {
    /// Sweep the stragglers of the life before `dropped` out of the collection
    /// `db.name`, if it owes the sweep, and take its floor up to the drop.
    /// Returns how many records it removed.
    ///
    /// Called with the drop's tombstone already recorded, so a record that
    /// arrives while the sweep runs is refused as history, and the floor is
    /// written when the sweep has finished. A stop ends it between two chunks
    /// (`Stopping`); the chunks that landed stand and the next call finds the
    /// rest.
    pub(crate) fn sweep_late_drop(&self, db: &str, name: &str, dropped: Stamp) -> Result<usize> {
        let mut removed = 0usize;
        let mut after: Option<Vec<u8>> = None;
        let mut started = false;
        loop {
            self.check_walk(crate::WalkScope::Background)?;
            let txn = self.begin_write(WriterHolder::Drop)?;
            let Some(standing) = read_collection(&txn, db, name)? else {
                txn.abort()?;
                return Ok(removed);
            };
            if !owes_a_sweep(&standing, dropped.hlc) {
                txn.abort()?;
                return Ok(removed);
            }
            if !started {
                started = true;
                debug!(
                    db,
                    collection = name,
                    stamp = ?dropped,
                    "a drop reached a newer life of the collection; removing what it holds of the \
                     life before"
                );
            }
            let (chunk, last, exhausted) =
                self.remove_stragglers(&txn, &standing, dropped, after)?;
            txn.commit()?;
            if kimmy_core::vector_meta::is_shadow(&standing.name) && chunk > 0 {
                self.bump_vector_generation(standing.id);
            }
            removed += chunk;
            after = last;
            if exhausted {
                break;
            }
        }
        // The floor, last: it says the sweep is done. Re-read under the writer,
        // since the collection may have been dropped or taken to a later floor
        // in between.
        let txn = self.begin_write(WriterHolder::Ddl)?;
        match read_collection(&txn, db, name)? {
            Some(mut standing) if owes_a_sweep(&standing, dropped.hlc) => {
                standing.incarnation_floor = Some(dropped.hlc);
                Engine::put_collection_meta(&txn, &standing)?;
                txn.commit()?;
            }
            _ => txn.abort()?,
        }
        if removed > 0 {
            info!(
                db,
                collection = name,
                records = removed,
                "removed what a collection held of the life before a drop that reached it late"
            );
        }
        Ok(removed)
    }

    /// One chunk: examine the collection's records after `after`, and replace
    /// each live one stamped at or before the drop with a tombstone at the
    /// drop's stamp. Returns how many it removed, the last key examined, and
    /// whether the collection has no more.
    ///
    /// A tombstone with no oplog entry behind it, as a snapshot's carried
    /// delete is (ADR-228): the count, the digest (and its `bare_max`) and the
    /// index entries move in this transaction. A stream that saw the
    /// straggler's insert is not sent a delete: there is no entry to send.
    fn remove_stragglers(
        &self,
        txn: &crate::engine::WriteTxn<'_>,
        standing: &CollectionMeta,
        dropped: Stamp,
        after: Option<Vec<u8>>,
    ) -> Result<(usize, Option<Vec<u8>>, bool)> {
        // Bounded by SWEEP_CHUNK rows examined; the stop is looked for between
        // chunks (walks_stop: a bounded read).
        let mut found: Vec<(Vec<u8>, Option<bson::Document>)> = Vec::new();
        let mut examined = 0usize;
        let mut last = after.clone();
        let mut exhausted = true;
        {
            let docs = txn.open_table(tables::DOCS)?;
            for row in docs.range(doc_range_after(standing.id, after.as_deref()))? {
                if examined == SWEEP_CHUNK {
                    exhausted = false;
                    break;
                }
                let (key, value) = row?;
                examined += 1;
                last = Some(key.value().1.to_vec());
                let (stamp, live) = codec::doc_record_header(value.value())?;
                if live && stamp.hlc <= dropped.hlc {
                    let record = codec::decode_doc_record(value.value())?;
                    found.push((key.value().1.to_vec(), record.document()?));
                }
            }
        }
        let tombstone = codec::encode_doc_record(&kimmy_core::DocRecord::tombstone(dropped));
        let mut docs = txn.open_table(tables::DOCS)?;
        for (key, _) in &found {
            crate::live_count::put_bare_tombstone(txn, &mut docs, standing.id.0, key, &tombstone)?;
        }
        drop(docs);
        for (key, previous) in &found {
            // A removal has no document to key and so no violation to report.
            crate::index::maintain_remote(self, txn, standing, previous.as_ref(), None, key)?;
        }
        Ok((found.len(), last, exhausted))
    }

    /// A drop was judged aimed at a life of `current` that has already ended
    /// ([`crate::sync::aims_at_a_previous_incarnation`]): sweep what the newer
    /// life holds of the one before, in the collection and in its vector shadow.
    pub(crate) fn sweep_after_an_ignored_drop(
        &self,
        current: &CollectionMeta,
        dropped: Stamp,
    ) -> Result<()> {
        self.sweep_late_drop(&current.db, &current.name, dropped)?;
        if !kimmy_core::vector_meta::is_shadow(&current.name) {
            let shadow = kimmy_core::vector_meta::shadow_name(&current.name);
            if self.get_collection(&current.db, &shadow).is_ok() {
                self.sweep_late_drop(&current.db, &shadow, dropped)?;
            }
        }
        Ok(())
    }

    /// At a start: sweep every collection that owes one for a tombstone this
    /// member holds, so a store that was left with stragglers (a drop applied
    /// before the sweep existed, or a stop in the middle of one) is healed
    /// before it serves. The floor each sweep sets is the marker that it was
    /// done, so a store with nothing owed pays one pass over the collection
    /// list. A stop ends it between two chunks and the next start continues.
    pub fn reconcile_late_drops(&self) -> Result<usize> {
        use crate::open_progress::{OpenPhase, open_stop_requested, set_open_phase};

        let tombstones: std::collections::HashMap<_, _> =
            self.collections_dropped()?.into_iter().collect();
        let owed: Vec<(CollectionMeta, Stamp)> = self
            .all_collections()?
            .into_iter()
            .filter_map(|standing| {
                let dropped = *tombstones.get(&standing.id)?;
                owes_a_sweep(&standing, dropped.hlc).then_some((standing, dropped))
            })
            .collect();
        if owed.is_empty() {
            return Ok(0);
        }
        let before = crate::open_progress::open_snapshot().phase;
        crate::open_progress::set_open_phase_of(OpenPhase::Reconciling, owed.len() as u64);
        let mut removed = 0usize;
        let result = (|| {
            for (n, (standing, dropped)) in owed.iter().enumerate() {
                if open_stop_requested() {
                    return Err(crate::error::StorageError::Stopping(crate::StopReason::Shutdown));
                }
                crate::open_progress::set_open_done(n as u64);
                removed += self.sweep_late_drop(&standing.db, &standing.name, *dropped)?;
            }
            Ok(())
        })();
        set_open_phase(before);
        result?;
        info!(
            collections = owed.len(),
            records = removed,
            "swept what newer lives held of the lives before their drops, before serving"
        );
        Ok(removed)
    }
}

/// The collection `db.name` as `txn` sees it.
fn read_collection(
    txn: &redb::WriteTransaction,
    db: &str,
    name: &str,
) -> Result<Option<CollectionMeta>> {
    let collections = txn.open_table(tables::COLLECTIONS)?;
    match collections.get((db, name))? {
        Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::{DocId, Hlc, NodeId, Stamp};

    use super::*;
    use crate::live_count::test_support::assert_counts_exact;
    use crate::{Engine, WalkScope};

    const BATCH: usize = 1_024;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
    }

    fn pull(into: &Engine, from: &Engine) {
        let mine = into.version_vector().unwrap();
        let theirs = from.version_vector().unwrap();
        if let Some(start) = mine.behind(&theirs) {
            let entries =
                from.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
            into.apply_batch(&entries).unwrap();
        }
    }

    fn sync(a: &Engine, b: &Engine) {
        pull(a, b);
        pull(b, a);
    }

    fn quiesce(engines: &[&Engine]) {
        for _ in 0..4 {
            for (i, x) in engines.iter().enumerate() {
                for y in &engines[i + 1..] {
                    sync(x, y);
                }
            }
            for e in engines {
                e.finish_purges_now().unwrap();
            }
        }
    }

    fn docs_of(e: &Engine, db: &str, name: &str, ids: &[i64]) -> Vec<i64> {
        let coll = e.get_collection(db, name).unwrap();
        ids.iter().copied().filter(|n| e.get(&coll, &DocId::Int64(*n)).unwrap().is_some()).collect()
    }

    /// A creates and drops; B, which knew nothing, creates the name afterwards.
    /// Entries arrive in stamp order, so A's document reaches B before A's
    /// drop does. The drop is aimed at a life that was over here (it predates
    /// B's creation) and is ignored: and the document of the life before stays
    /// in B's.
    fn the_race() -> (Engine, Engine, tempfile::TempDir, tempfile::TempDir) {
        let (a, da) = engine();
        let (b, db) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64, "from": "a" }).unwrap();
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        let on_b = b.create_collection("shop", "orders").unwrap();
        assert!(on_b.created > on_a.created, "premise: B's creation is the later");
        b.insert(&on_b, doc! { "_id": 2i64, "from": "b" }).unwrap();
        (a, b, da, db)
    }

    /// The reproducer of the finding: both members end with the same documents,
    /// and B's floor stands at A's drop, which is also what says the sweep was
    /// done.
    #[test]
    fn a_drop_that_reaches_a_newer_life_removes_what_that_life_took_in_of_the_one_before() {
        let (a, b, _da, _db) = the_race();
        quiesce(&[&a, &b]);
        assert_eq!(docs_of(&a, "shop", "orders", &[1, 2]), vec![2]);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2], "the straggler is gone");
        for e in [&a, &b] {
            assert_eq!(e.count_by_id(CollectionId::derive("shop", "orders")).unwrap(), Some(1));
            assert_counts_exact(e, "a late drop");
        }
        let dropped = *b
            .collection_tombstones()
            .unwrap()
            .get(&CollectionId::derive("shop", "orders"))
            .unwrap();
        let floor = b.get_collection("shop", "orders").unwrap().incarnation_floor;
        assert_eq!(floor, Some(dropped), "the floor is taken up to the drop");
    }

    use kimmy_core::CollectionId;

    /// Nothing is swept for a life the drop did not end: a document of the
    /// newer life stays, and so does one that arrives after, and a second
    /// delivery of the drop finds nothing owed.
    #[test]
    fn the_sweep_keeps_what_the_newer_life_wrote_and_is_done_once() {
        let (a, b, _da, _db) = the_race();
        quiesce(&[&a, &b]);
        let on_b = b.get_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 5i64 }).unwrap();
        quiesce(&[&a, &b]);
        assert_eq!(docs_of(&a, "shop", "orders", &[1, 2, 5]), vec![2, 5]);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2, 5]), vec![2, 5]);
        let id = CollectionId::derive("shop", "orders");
        let dropped = *b.collection_tombstones().unwrap().get(&id).unwrap();
        let dropped = Stamp::new(dropped, a.node_id());
        assert!(!owes_a_sweep(&b.get_collection("shop", "orders").unwrap(), dropped.hlc));
        assert_eq!(
            b.sweep_late_drop("shop", "orders", dropped).unwrap(),
            0,
            "nothing is owed twice"
        );
    }

    /// A member D that still holds the old life writes Z after the drop, then
    /// receives it. The drop destroys the life it was aimed at, Z with it, as it
    /// always has; A and B, whose lives are newer, keep Z (it is stamped after
    /// the drop), so D lacks a document the others hold. That is a different
    /// divergence from this rule's (a write to a life the drop destroyed, kept
    /// elsewhere) and is recorded as its own finding; what this test pins is
    /// that the document of the life before is gone from all three.
    #[test]
    fn a_member_still_holding_the_old_life_does_not_keep_the_document_of_it_after_the_drop() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let (d, _dd) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
        sync(&a, &d);
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        let on_d = d.get_collection("shop", "orders").unwrap();
        d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 2i64 }).unwrap();
        quiesce(&[&a, &b, &d]);
        for (who, e) in [("a", &a), ("b", &b), ("d", &d)] {
            assert!(
                !docs_of(e, "shop", "orders", &[1, 2, 3]).contains(&1),
                "{who} kept document 1"
            );
        }
        assert_eq!(
            docs_of(&a, "shop", "orders", &[1, 2, 3]),
            docs_of(&b, "shop", "orders", &[1, 2, 3]),
            "A and B agree"
        );
    }

    /// A database drop: a collection created after it stands, and what it took
    /// in of the life before, before it learned of the drop, goes.
    #[test]
    fn a_database_drop_removes_what_a_survivor_took_in_of_the_life_before() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let on_a = a.create_collection("shop", "orders").unwrap();
        a.insert(&on_a, doc! { "_id": 1i64, "from": "a" }).unwrap();
        a.drop_database("shop").unwrap();
        a.finish_purges_now().unwrap();
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 2i64, "from": "b" }).unwrap();
        quiesce(&[&a, &b]);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2], "the survivor keeps its own");
        assert_eq!(docs_of(&a, "shop", "orders", &[1, 2]), vec![2]);
        assert_counts_exact(&b, "a database drop that reached a survivor late");
    }

    /// A store left with the stragglers (a drop applied before the sweep
    /// existed) is healed by the reconcile, once.
    #[test]
    fn the_reconcile_heals_a_store_built_with_the_bug() {
        let (b, _db) = engine();
        let id = CollectionId::derive("shop", "orders");
        let on_b = b.create_collection("shop", "orders").unwrap();
        b.insert(&on_b, doc! { "_id": 2i64 }).unwrap();
        // What the old rule left: a document of the life before, below a drop
        // this member holds, in a life created after it, and no floor.
        let node = NodeId::from_bytes([7; 16]);
        let dropped = Stamp::new(Hlc::new(on_b.created.wall_ms - 10, 0), node);
        let straggler = Stamp::new(Hlc::new(on_b.created.wall_ms - 20, 0), node);
        {
            let key = crate::docs::doc_key(&DocId::Int64(1)).unwrap();
            let body = bson::serialize_to_vec(&doc! { "_id": 1i64 }).unwrap();
            let record = kimmy_core::DocRecord::live(straggler, body);
            let txn = b.begin_write(WriterHolder::Replication).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            crate::live_count::put_record(
                &txn,
                &mut docs,
                id.0,
                &key,
                &codec::encode_doc_record(&record),
            )
            .unwrap();
            drop(docs);
            txn.commit().unwrap();
        }
        b.record_collection_drop(id, dropped).unwrap();
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![1, 2], "premise");
        assert!(owes_a_sweep(&b.get_collection("shop", "orders").unwrap(), dropped.hlc));

        assert_eq!(b.reconcile_late_drops().unwrap(), 1);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2]);
        assert_eq!(
            b.get_collection("shop", "orders").unwrap().incarnation_floor,
            Some(dropped.hlc)
        );
        assert_counts_exact(&b, "the reconcile");
        assert_eq!(b.reconcile_late_drops().unwrap(), 0, "the floor says it was done");
    }

    /// More stragglers than a chunk are swept across chunks, a stop between two
    /// leaves the floor unset, and the next call finishes.
    #[test]
    fn a_sweep_of_more_than_a_chunk_stops_between_chunks_and_finishes_on_the_next_call() {
        let (b, _db) = engine();
        let id = CollectionId::derive("shop", "orders");
        let on_b = b.create_collection("shop", "orders").unwrap();
        let node = NodeId::from_bytes([7; 16]);
        let dropped = Stamp::new(Hlc::new(on_b.created.wall_ms - 10, 0), node);
        let straggler = Stamp::new(Hlc::new(on_b.created.wall_ms - 20, 0), node);
        let n = SWEEP_CHUNK * 2 + 17;
        {
            let txn = b.begin_write(WriterHolder::Replication).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            for i in 0..n as i64 {
                let key = crate::docs::doc_key(&DocId::Int64(i)).unwrap();
                let body = bson::serialize_to_vec(&doc! { "_id": i }).unwrap();
                let record = kimmy_core::DocRecord::live(straggler, body);
                crate::live_count::put_record(
                    &txn,
                    &mut docs,
                    id.0,
                    &key,
                    &codec::encode_doc_record(&record),
                )
                .unwrap();
            }
            drop(docs);
            txn.commit().unwrap();
        }
        b.record_collection_drop(id, dropped).unwrap();
        assert_eq!(b.count_by_id(id).unwrap(), Some(n as u64));

        // A stop is asked for: nothing is swept, nothing is claimed done.
        b.stop_walks();
        assert!(matches!(
            b.sweep_late_drop("shop", "orders", dropped),
            Err(crate::StorageError::Stopping(_))
        ));
        assert_eq!(b.get_collection("shop", "orders").unwrap().incarnation_floor, None);
        b.lift_stops_for_test();

        assert_eq!(b.sweep_late_drop("shop", "orders", dropped).unwrap(), n);
        assert_eq!(b.count_by_id(id).unwrap(), Some(0));
        assert_eq!(
            b.get_collection("shop", "orders").unwrap().incarnation_floor,
            Some(dropped.hlc)
        );
        assert_counts_exact(&b, "a sweep across chunks");
    }

    /// The tie the floor exists for: a record stamped at the drop's own `Hlc`.
    #[test]
    fn a_record_stamped_at_the_drops_own_hlc_is_swept_and_one_after_it_is_kept() {
        let (b, _db) = engine();
        let id = CollectionId::derive("shop", "orders");
        let on_b = b.create_collection("shop", "orders").unwrap();
        let node = NodeId::from_bytes([7; 16]);
        let dropped = Stamp::new(Hlc::new(on_b.created.wall_ms - 10, 0), node);
        let put = |n: i64, stamp: Stamp| {
            let key = crate::docs::doc_key(&DocId::Int64(n)).unwrap();
            let body = bson::serialize_to_vec(&doc! { "_id": n }).unwrap();
            let txn = b.begin_write(WriterHolder::Replication).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            crate::live_count::put_record(
                &txn,
                &mut docs,
                id.0,
                &key,
                &codec::encode_doc_record(&kimmy_core::DocRecord::live(stamp, body)),
            )
            .unwrap();
            drop(docs);
            txn.commit().unwrap();
        };
        put(1, Stamp::new(dropped.hlc, NodeId::from_bytes([9; 16])));
        put(2, Stamp::new(Hlc::new(dropped.hlc.wall_ms + 1, 0), node));
        b.record_collection_drop(id, dropped).unwrap();
        assert_eq!(b.sweep_late_drop("shop", "orders", dropped).unwrap(), 1);
        assert_eq!(docs_of(&b, "shop", "orders", &[1, 2]), vec![2]);
    }
}
