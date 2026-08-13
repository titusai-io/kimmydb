//! Atomic find-modify-return, inside one write transaction.
//!
//! # Why the match happens here rather than in a read pass
//!
//! `update` and `delete` collect their targets in a *read* transaction and then
//! write them one at a time. That is fine when the answer is "change everything
//! matching", and wrong when the answer is "claim exactly one" — two callers
//! scanning concurrently both see the same pending job, and both claim it.
//!
//! redb has a single writer, so a match found *inside* the write transaction
//! cannot be taken by anyone else between the match and the commit. That makes
//! this atomic by construction, with no retry loop, no ABA window, and no
//! possibility of reporting "nothing matched" while something did.
//!
//! The cost is that the writer is held for the match as well as the commit. An
//! indexed filter adds microseconds; an unindexed one adds the whole collection
//! scan, and that blocks every other write on the node. [`MAX_CANDIDATES`] is
//! the bound that keeps the worst case a refusal rather than a stall.
//!
//! # Why the query language is not in this crate
//!
//! `kimmy-query` is a **dev-only** dependency here, deliberately — the engine
//! does storage, not semantics. So filtering, ordering and update operators
//! arrive as [`ModifySpec`], a set of pure functions over documents that the
//! caller supplies and this module calls *inside* the transaction. It is the
//! same shape as the guard [`crate::Engine::delete_guarded`] takes, one step
//! further: there the caller decides "still eligible?", here it also decides
//! "which one?" and "changed how?".

use std::cmp::Ordering;

use bson::Document;
use kimmy_core::{DocId, DocRecord, OpKind, OplogEntry};
use redb::ReadableTable;

use crate::docs::extract_id;
use crate::engine::{append_oplog, doc_range};
use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;
use crate::{Engine, codec, index, tables};

/// How many matching documents may be held inside the write transaction.
///
/// Sorting to choose one means materialising every match, and that happens
/// while the single writer is held. A full scan of 10,000 documents is ~8 ms
/// ([Benchmarks](../../../docs/benchmarks.md)) and `find`'s own `MAX_LIMIT` is
/// 10,000 for exactly that reason, so the same ceiling applies here — but as a
/// **refusal** rather than a truncation, because silently choosing from a
/// prefix of the matches would return the wrong document with no way to tell.
pub const MAX_CANDIDATES: usize = 10_000;

/// Where to look for candidate documents, in the engine's own terms.
///
/// The caller plans; this scans. Encoded byte ranges rather than a query type
/// keeps the crate boundary intact.
#[derive(Clone, Debug)]
pub enum Candidates {
    /// Every live document in the collection.
    Scan,
    /// The union of one index's key ranges, both bounds inclusive.
    Index {
        index_id: u32,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
        /// Whether the ranges intersect *both* ends, which is only sound while
        /// the index is not multikey. Re-checked inside this transaction; a
        /// multikey index falls back to a collection scan rather than silently
        /// losing documents — the same rule the read path follows, and the
        /// reason it must be re-read in the scanning snapshot.
        both_bounds: bool,
    },
}

/// What the caller decides, as pure functions over documents.
pub trait ModifySpec {
    /// Whether this document is a candidate.
    fn matches(&self, doc: &Document) -> bool;

    /// Order for choosing among matches; the first after sorting wins.
    ///
    /// Returning `Ordering::Equal` for everything leaves the scan's own order,
    /// which is `_id` order for an index scan and storage order otherwise.
    fn compare(&self, a: &Document, b: &Document) -> Ordering;

    /// The new document, or `None` to remove it.
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String>;

    /// The document to insert when nothing matched, if this is an upsert.
    fn upsert(&self) -> Option<std::result::Result<Document, String>>;
}

/// What happened, and the images either side of it.
#[derive(Clone, Debug, Default)]
pub struct ModifyOutcome {
    /// The document as it was before, when something matched.
    pub before: Option<Document>,
    /// The document as it is now, absent when it was removed.
    pub after: Option<Document>,
    pub matched: bool,
    pub upserted: Option<DocId>,
}

impl Engine {
    /// Find one document, change it, and return it — atomically.
    pub fn find_and_modify(
        &self,
        coll: &CollectionMeta,
        candidates: &Candidates,
        spec: &dyn ModifySpec,
    ) -> Result<ModifyOutcome> {
        let txn = self.db().begin_write()?;

        let chosen = match self.choose(&txn, coll, candidates, spec) {
            Ok(chosen) => chosen,
            Err(e) => {
                txn.abort()?;
                return Err(e);
            }
        };

        let Some(before) = chosen else {
            // Nothing matched. An upsert inserts; anything else is a no-op,
            // and a no-op must not mint an oplog entry or publish an event.
            let Some(doc) = spec.upsert() else {
                txn.abort()?;
                return Ok(ModifyOutcome::default());
            };
            let doc = match doc {
                Ok(doc) => doc,
                Err(e) => {
                    txn.abort()?;
                    return Err(StorageError::Core(kimmy_core::Error::InvalidQuery(e)));
                }
            };
            let (id, entry) = match self.insert_in_txn(&txn, coll, doc) {
                Ok(pair) => pair,
                Err(e) => {
                    txn.abort()?;
                    return Err(e);
                }
            };
            let inserted: Document = bson::deserialize_from_slice(
                entry.body.as_deref().expect("an insert carries its body"),
            )?;
            txn.commit()?;
            self.publish(vec![entry]);
            return Ok(ModifyOutcome {
                before: None,
                after: Some(inserted),
                matched: false,
                upserted: Some(id),
            });
        };

        let id = extract_id(&before)?;
        let next = match spec.apply(&before) {
            Ok(next) => next,
            Err(e) => {
                txn.abort()?;
                return Err(StorageError::Core(kimmy_core::Error::InvalidQuery(e)));
            }
        };

        let result = self.write_chosen(&txn, coll, &id, &before, next.clone());
        let entry = match result {
            Ok(entry) => entry,
            Err(e) => {
                txn.abort()?;
                return Err(e);
            }
        };

        txn.commit()?;
        self.publish(vec![entry]);

        Ok(ModifyOutcome { before: Some(before), after: next, matched: true, upserted: None })
    }

    /// Collect matches inside the transaction and pick the first after sorting.
    fn choose(
        &self,
        txn: &redb::WriteTransaction,
        coll: &CollectionMeta,
        candidates: &Candidates,
        spec: &dyn ModifySpec,
    ) -> Result<Option<Document>> {
        let mut matches: Vec<Document> = Vec::new();

        let consider = |doc: Document, matches: &mut Vec<Document>| -> Result<()> {
            if !spec.matches(&doc) {
                return Ok(());
            }
            matches.push(doc);
            if matches.len() > MAX_CANDIDATES {
                // Refused, not truncated: choosing from a prefix would return
                // a document that is not the one the sort asked for, and no
                // caller could tell.
                return Err(StorageError::Core(kimmy_core::Error::InvalidQuery(format!(
                    "find_and_modify matched more than {MAX_CANDIDATES} documents; \
                     narrow the filter, or add an index and a tighter one"
                ))));
            }
            Ok(())
        };

        let docs = txn.open_table(tables::DOCS)?;
        match candidates {
            Candidates::Index { index_id, ranges, both_bounds } => {
                // The multikey flag is re-read here, in the transaction that
                // scans — a `false` from the caller's earlier read proves
                // nothing about this snapshot.
                let sound = !both_bounds || !self.index_is_multikey(txn, coll, *index_id)?;
                if sound {
                    let mut seen: std::collections::BTreeSet<Vec<u8>> = Default::default();
                    for (lower, upper) in ranges {
                        for key in
                            index::scan_range_in_write(txn, coll.id, *index_id, lower, Some(upper))?
                        {
                            // A `$in` union can offer one document twice.
                            if !seen.insert(key.clone()) {
                                continue;
                            }
                            let Some(raw) = docs.get((coll.id.0, key.as_slice()))? else {
                                continue;
                            };
                            let record = codec::decode_doc_record(raw.value())?;
                            if record.deleted {
                                continue;
                            }
                            consider(bson::deserialize_from_slice(&record.body)?, &mut matches)?;
                        }
                    }
                } else {
                    scan_all(&docs, coll, &mut |doc| consider(doc, &mut matches))?;
                }
            }
            Candidates::Scan => scan_all(&docs, coll, &mut |doc| consider(doc, &mut matches))?,
        }

        if matches.is_empty() {
            return Ok(None);
        }
        // `sort_by` rather than picking a minimum: the comparator is the
        // caller's whole sort specification, and a stable sort keeps the
        // scan's order for documents the sort does not separate.
        matches.sort_by(|a, b| spec.compare(a, b));
        Ok(Some(matches.swap_remove(0)))
    }

    fn index_is_multikey(
        &self,
        txn: &redb::WriteTransaction,
        coll: &CollectionMeta,
        index_id: u32,
    ) -> Result<bool> {
        let collections = txn.open_table(tables::COLLECTIONS)?;
        let Some(raw) = collections.get((coll.db.as_str(), coll.name.as_str()))? else {
            return Ok(false);
        };
        let fresh: CollectionMeta = serde_json::from_slice(raw.value())?;
        Ok(fresh.index_by_id(index_id).is_none_or(|i| i.multikey))
    }

    /// Write the chosen document's new state and return its oplog entry.
    fn write_chosen(
        &self,
        txn: &redb::WriteTransaction,
        coll: &CollectionMeta,
        id: &DocId,
        before: &Document,
        next: Option<Document>,
    ) -> Result<OplogEntry> {
        let key = crate::docs::doc_key(id)?;
        let stamp = self.next_stamp();

        let (record, body, kind) = match &next {
            Some(doc) => {
                let body = bson::serialize_to_vec(doc)?;
                (DocRecord::live(stamp, body.clone()), Some(body), OpKind::Replace)
            }
            // A tombstone, exactly as an ordinary delete leaves — so a removal
            // through this route replicates and streams like any other delete.
            None => (DocRecord::tombstone(stamp), None, OpKind::Delete),
        };

        {
            let mut docs = txn.open_table(tables::DOCS)?;
            docs.insert((coll.id.0, key.as_slice()), codec::encode_doc_record(&record).as_slice())?;
        }

        let newly_multikey = index::maintain(txn, coll, Some(before), next.as_ref(), &key)?;
        index::mark_multikey(txn, &coll.db, &coll.name, &newly_multikey)?;

        let entry = OplogEntry { stamp, kind, collection: coll.id, doc_id: Some(id.clone()), body };
        append_oplog(txn, &entry)?;
        Ok(entry)
    }
}

/// Every live document in the collection, inside the caller's transaction.
fn scan_all(
    docs: &impl ReadableTable<(u64, &'static [u8]), &'static [u8]>,
    coll: &CollectionMeta,
    f: &mut impl FnMut(Document) -> Result<()>,
) -> Result<()> {
    for entry in docs.range(doc_range(coll.id))? {
        let (_, value) = entry?;
        let record = codec::decode_doc_record(value.value())?;
        if record.deleted {
            continue;
        }
        f(bson::deserialize_from_slice(&record.body)?)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    /// A spec built from closures, so each test states only what it varies.
    struct TestSpec<M, C, A> {
        matches: M,
        compare: C,
        apply: A,
        upsert: Option<Document>,
    }

    impl<M, C, A> ModifySpec for TestSpec<M, C, A>
    where
        M: Fn(&Document) -> bool,
        C: Fn(&Document, &Document) -> Ordering,
        A: Fn(&Document) -> std::result::Result<Option<Document>, String>,
    {
        fn matches(&self, doc: &Document) -> bool {
            (self.matches)(doc)
        }
        fn compare(&self, a: &Document, b: &Document) -> Ordering {
            (self.compare)(a, b)
        }
        fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
            (self.apply)(doc)
        }
        fn upsert(&self) -> Option<std::result::Result<Document, String>> {
            self.upsert.clone().map(Ok)
        }
    }

    fn engine() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let e = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let c = e.create_collection("app", "jobs").unwrap();
        (e, c, dir)
    }

    fn i64_of(doc: &Document, key: &str) -> i64 {
        doc.get_i64(key).or_else(|_| doc.get_i32(key).map(i64::from)).unwrap()
    }

    /// Claim the lowest-`created` pending job, marking it claimed.
    fn claim() -> impl ModifySpec {
        TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |a: &Document, b: &Document| i64_of(a, "created").cmp(&i64_of(b, "created")),
            apply: |d: &Document| {
                let mut next = d.clone();
                next.insert("status", "claimed");
                Ok(Some(next))
            },
            upsert: None,
        }
    }

    fn seed(engine: &Engine, coll: &CollectionMeta) {
        for (id, created, status) in
            [(1i64, 30i64, "pending"), (2, 10, "pending"), (3, 20, "done"), (4, 20, "pending")]
        {
            engine.insert(coll, doc! {"_id": id, "created": created, "status": status}).unwrap();
        }
    }

    #[test]
    fn the_sort_decides_which_match_is_taken() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let out = engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
        assert!(out.matched);
        // _id 2 has the lowest `created` among pending; _id 3 is not pending.
        assert_eq!(out.before.as_ref().unwrap().get_i64("_id").unwrap(), 2);
        assert_eq!(out.before.unwrap().get_str("status").unwrap(), "pending");
        assert_eq!(out.after.unwrap().get_str("status").unwrap(), "claimed");
    }

    #[test]
    fn a_claim_is_visible_immediately_and_the_next_takes_another() {
        // Draining a queue must never hand out the same job twice.
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let mut claimed = Vec::new();
        for _ in 0..3 {
            let out = engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
            assert!(out.matched);
            claimed.push(out.before.unwrap().get_i64("_id").unwrap());
        }
        claimed.sort();
        assert_eq!(claimed, vec![1, 2, 4], "each pending job claimed exactly once");

        // Nothing pending left.
        let out = engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
        assert!(!out.matched);
        assert!(out.before.is_none());
    }

    #[test]
    fn no_match_writes_nothing_and_publishes_nothing() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! {"_id": 1, "status": "done"}).unwrap();

        let mut rx = engine.subscribe();
        let out = engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
        assert!(!out.matched);
        assert!(rx.try_recv().is_err(), "a no-op must not publish an event");
    }

    #[test]
    fn remove_leaves_a_tombstone_and_an_ordinary_delete_entry() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let remove = TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |a: &Document, b: &Document| i64_of(a, "created").cmp(&i64_of(b, "created")),
            apply: |_: &Document| Ok(None),
            upsert: None,
        };

        let mut rx = engine.subscribe();
        let out = engine.find_and_modify(&coll, &Candidates::Scan, &remove).unwrap();
        assert!(out.matched);
        assert_eq!(out.before.unwrap().get_i64("_id").unwrap(), 2);
        assert!(out.after.is_none(), "there is no document after a removal");

        let event = rx.try_recv().unwrap();
        assert_eq!(event.kind, OpKind::Delete);
        assert!(engine.get(&coll, &DocId::Int64(2)).unwrap().is_none());
    }

    #[test]
    fn upsert_inserts_when_nothing_matched() {
        let (engine, coll, _dir) = engine();

        let spec = TestSpec {
            matches: |_: &Document| false,
            compare: |_: &Document, _: &Document| Ordering::Equal,
            apply: |d: &Document| Ok(Some(d.clone())),
            upsert: Some(doc! {"_id": 99, "status": "pending"}),
        };

        let mut rx = engine.subscribe();
        let out = engine.find_and_modify(&coll, &Candidates::Scan, &spec).unwrap();
        assert!(!out.matched, "an upsert did not match; it created");
        assert_eq!(out.upserted, Some(DocId::Int64(99)));
        assert_eq!(out.after.unwrap().get_str("status").unwrap(), "pending");

        // A created document is an insert to a change-stream subscriber.
        assert_eq!(rx.try_recv().unwrap().kind, OpKind::Insert);
        assert!(engine.get(&coll, &DocId::Int64(99)).unwrap().is_some());
    }

    #[test]
    fn upsert_does_not_fire_when_something_matched() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let spec = TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |a: &Document, b: &Document| i64_of(a, "created").cmp(&i64_of(b, "created")),
            apply: |d: &Document| {
                let mut next = d.clone();
                next.insert("status", "claimed");
                Ok(Some(next))
            },
            upsert: Some(doc! {"_id": 99}),
        };

        let out = engine.find_and_modify(&coll, &Candidates::Scan, &spec).unwrap();
        assert!(out.matched);
        assert_eq!(out.upserted, None);
        assert!(engine.get(&coll, &DocId::Int64(99)).unwrap().is_none());
    }

    #[test]
    fn an_index_plan_and_a_scan_agree() {
        let (engine, _coll, _dir) = engine();
        engine
            .create_index(
                "app",
                "jobs",
                vec![kimmy_core::IndexField::ascending("status")],
                false,
                Some("status_1".into()),
            )
            .unwrap();
        let coll = engine.get_collection("app", "jobs").unwrap();
        seed(&engine, &coll);

        let index = coll.index("status_1").unwrap();
        let probe = kimmy_core::keyenc::encode_compound_ordered(&[(
            bson::Bson::String("pending".into()),
            false,
        )])
        .unwrap();
        let candidates = Candidates::Index {
            index_id: index.id,
            ranges: vec![(probe.clone(), probe)],
            both_bounds: false,
        };

        let out = engine.find_and_modify(&coll, &candidates, &claim()).unwrap();
        assert!(out.matched);
        // The same answer the scan gives: lowest `created` among pending.
        assert_eq!(out.before.unwrap().get_i64("_id").unwrap(), 2);
    }

    #[test]
    fn an_index_entry_is_maintained_through_the_change() {
        // The modified document must leave the index consistent, or a later
        // claim would find a candidate whose document no longer matches.
        let (engine, _coll, _dir) = engine();
        engine
            .create_index(
                "app",
                "jobs",
                vec![kimmy_core::IndexField::ascending("status")],
                false,
                Some("status_1".into()),
            )
            .unwrap();
        let coll = engine.get_collection("app", "jobs").unwrap();
        seed(&engine, &coll);

        let index = coll.index("status_1").unwrap();
        let probe = kimmy_core::keyenc::encode_compound_ordered(&[(
            bson::Bson::String("pending".into()),
            false,
        )])
        .unwrap();
        let candidates = Candidates::Index {
            index_id: index.id,
            ranges: vec![(probe.clone(), probe.clone())],
            both_bounds: false,
        };

        // Claim all three pending jobs through the index.
        for _ in 0..3 {
            assert!(engine.find_and_modify(&coll, &candidates, &claim()).unwrap().matched);
        }
        // The index must now offer no `pending` candidates at all.
        let out = engine.find_and_modify(&coll, &candidates, &claim()).unwrap();
        assert!(!out.matched, "stale index entries survived the modification");
    }

    #[test]
    fn too_many_matches_is_refused_rather_than_truncated() {
        // Choosing from a prefix would return a document the sort did not
        // pick, and no caller could tell it happened.
        let (engine, coll, _dir) = engine();
        // One transaction: 10,001 separate commits would make this test a
        // minute long on its own, which is how a suite stops being run.
        let batch: Vec<Document> = (0..=MAX_CANDIDATES as i64)
            .map(|id| doc! {"_id": id, "created": id, "status": "pending"})
            .collect();
        engine.insert_many(&coll, batch).unwrap();

        let err = engine.find_and_modify(&coll, &Candidates::Scan, &claim());
        assert!(err.is_err(), "over the cap must refuse");
        // And nothing was written by the attempt.
        assert_eq!(
            engine.get(&coll, &DocId::Int64(0)).unwrap().unwrap().get_str("status").unwrap(),
            "pending"
        );
    }

    #[test]
    fn a_failing_apply_aborts_and_leaves_the_document_alone() {
        let (engine, coll, _dir) = engine();
        seed(&engine, &coll);

        let spec = TestSpec {
            matches: |d: &Document| d.get_str("status").map(|s| s == "pending").unwrap_or(false),
            compare: |a: &Document, b: &Document| i64_of(a, "created").cmp(&i64_of(b, "created")),
            apply: |_: &Document| Err("nope".to_string()),
            upsert: None,
        };

        let mut rx = engine.subscribe();
        assert!(engine.find_and_modify(&coll, &Candidates::Scan, &spec).is_err());
        assert_eq!(
            engine.get(&coll, &DocId::Int64(2)).unwrap().unwrap().get_str("status").unwrap(),
            "pending"
        );
        assert!(rx.try_recv().is_err(), "a failed modify must publish nothing");
    }

    #[test]
    fn concurrent_claims_never_hand_out_the_same_job_twice() {
        // The reason the match lives inside the write transaction. Eight
        // threads race for four jobs: every claim must be distinct, and
        // exactly four must succeed.
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let coll = engine.create_collection("app", "jobs").unwrap();
        for id in 0..4i64 {
            engine.insert(&coll, doc! {"_id": id, "created": id, "status": "pending"}).unwrap();
        }

        let winners = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let engine = Arc::clone(&engine);
            let coll = coll.clone();
            let winners = Arc::clone(&winners);
            handles.push(std::thread::spawn(move || {
                let out = engine.find_and_modify(&coll, &Candidates::Scan, &claim()).unwrap();
                if out.matched {
                    let id = out.before.unwrap().get_i64("_id").unwrap();
                    winners.lock().unwrap().push(id);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut claimed = winners.lock().unwrap().clone();
        claimed.sort();
        let distinct: std::collections::BTreeSet<i64> = claimed.iter().copied().collect();
        assert_eq!(
            claimed.len(),
            distinct.len(),
            "a job was claimed twice: {claimed:?} — the match is not atomic"
        );
        assert_eq!(claimed, vec![0, 1, 2, 3], "every job claimed exactly once");
    }
}
