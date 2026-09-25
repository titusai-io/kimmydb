//! TTL expiry: deleting documents a TTL index says are past their time.
//!
//! # Why this reads an index rather than scanning
//!
//! An expiry pass runs on an interval, forever, over every collection that has
//! a policy. A collection scan costs ~0.8 µs per *document present*
//! ([Benchmarks](../../../docs/benchmarks.md)), so at ten million documents
//! that is ~8 s of storage work per pass whether or not anything expired. A
//! range scan over the TTL index costs ~1.66 µs per *candidate returned*, and
//! the candidates are exactly the expired documents. The index is both the
//! policy and the mechanism, which is what keeps a background task
//! proportional to the work there is rather than to the data there is.
//!
//! # What is deliberately not here
//!
//! **Any notion of which node should run this.** Expiry is owned by one node
//! per collection so that one document produces one delete rather than N, but
//! that decision is rendezvous hashing over cluster membership, and this crate
//! does not know a cluster exists — the same boundary that keeps `$lookup` out
//! of `kimmy-query`. The caller decides *whether* to run a pass; this decides
//! *what* a pass removes.
//!
//! # Non-dates are ignored, and get that for free
//!
//! `keyenc` orders by type tag first, so every `DateTime` entry in an index is
//! contiguous and every non-date sorts outside it. Bounding the scan by two
//! encoded dates therefore skips a document whose indexed field holds a string
//! without needing to look at it — which is also MongoDB's behaviour.

use std::collections::HashSet;

use bson::{Bson, Document};
use kimmy_core::{IndexMeta, keyenc, path};
use tracing::warn;

use crate::Engine;
use crate::error::Result;
use crate::meta::CollectionMeta;

/// How many documents one pass may remove from one collection.
///
/// Each delete is its own durable commit (~3.4 ms), so an unbounded pass over
/// a large expiry backlog would hold the single redb writer for minutes and
/// starve foreground writes. Bounding it means a backlog drains over several
/// passes instead of in one stall; the pass reports whether it hit the bound
/// so the caller can say so.
pub const MAX_EXPIRED_PER_PASS: usize = 1_000;

/// Where a TTL index's expiry scan resumes: the last `(index key, document
/// key)` a pass examined (ADR-181).
pub(crate) type ExpiryCursor = (Vec<u8>, Vec<u8>);

/// What one collection's pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExpiryOutcome {
    /// Documents actually removed.
    pub deleted: u64,
    /// Candidates the scan offered that were no longer eligible when the
    /// delete transaction re-read them — a document whose date moved on
    /// between the scan and the write.
    pub skipped: u64,
    /// Candidates the index held that its partial filter did not select when
    /// the delete transaction re-read them, the filter evaluated as `find`
    /// evaluates it (ADR-181). Not deleted: a document moved out of the filter
    /// while the pass ran, or one the index held that the filter never
    /// selected.
    pub skipped_filter: u64,
    /// Whether the pass stopped at [`MAX_EXPIRED_PER_PASS`] with more of its
    /// cycle to do. The next pass resumes after the last entry this one
    /// examined; a pass that reaches the end of the expired range ends the
    /// cycle, and the one after it starts again from the front.
    pub truncated: bool,
}

impl Engine {
    /// Remove documents that `index` says have expired, as of `now_ms`.
    ///
    /// Returns `ExpiryOutcome::default()` for an index with no policy, so a
    /// caller may pass every index without filtering first.
    pub fn expire_documents(
        &self,
        coll: &CollectionMeta,
        index: &IndexMeta,
        now_ms: u64,
    ) -> Result<ExpiryOutcome> {
        let (Some(secs), Some(field)) = (index.expire_after_secs, index.ttl_path()) else {
            return Ok(ExpiryOutcome::default());
        };

        // Saturating: a policy longer than the time since the epoch expires
        // nothing, rather than wrapping into a cutoff in the far future and
        // deleting the collection.
        let cutoff_ms = (now_ms as i64).saturating_sub(secs.saturating_mul(1_000));

        // Parsed once for the pass. Membership in the index is not taken to
        // mean the filter selects a document: it can be stale by the delete,
        // and the index can hold what the filter never selected (ADR-181).
        // A filter this build cannot parse skips the index for the pass,
        // deleting nothing, with one line naming it: nothing it holds can be
        // checked against the filter, and the index's other collections'
        // expiry goes on.
        let filter = match index.partial().transpose() {
            Ok(filter) => filter,
            Err(e) => {
                warn!(
                    db = %coll.db,
                    collection = %coll.name,
                    index = %index.name,
                    error = %e,
                    "TTL index skipped this pass: its partial filter does not parse, so nothing \
                     it holds can be checked against the filter, and nothing is deleted"
                );
                return Ok(ExpiryOutcome::default());
            }
        };

        // From where the last pass stopped, not from the front: the guard
        // below can decline a candidate and leave its entry in the index, and
        // a scan that started from the front every pass read the same declined
        // entries first, every pass, and never reached what was behind them
        // (ADR-181). Resuming, a declined entry is examined once a cycle.
        let (lower, upper) = expired_range(index, cutoff_ms)?;
        let at = (coll.id.0, index.id);
        let resume = self.expiry_cursor(at);
        let mut candidates = self.index_keyed_entries_after(
            coll,
            index.id,
            &lower,
            &upper,
            resume.as_ref(),
            MAX_EXPIRED_PER_PASS + 1,
        )?;
        let truncated = candidates.len() > MAX_EXPIRED_PER_PASS;
        candidates.truncate(MAX_EXPIRED_PER_PASS);
        // Set before the deletes, so that one that errors cannot pin the scan
        // to itself either: the next pass goes on past it.
        self.set_expiry_cursor(at, if truncated { candidates.last().cloned() } else { None });
        #[cfg(test)]
        hooks::between_scan_and_delete();

        let mut outcome = ExpiryOutcome { truncated, ..Default::default() };
        // A document an index keys more than once is one candidate.
        let mut seen = HashSet::new();
        for (_, key) in candidates {
            if !seen.insert(key.clone()) {
                continue;
            }
            // The scan hands back encoded document keys, and `keyenc` is
            // one-way, so the id comes from the document itself.
            let Some(doc) = self.get_by_encoded_key(coll, &key)? else {
                continue;
            };
            let Some(id) = doc.get("_id").and_then(|v| kimmy_core::DocId::try_from_bson(v).ok())
            else {
                continue;
            };

            // The guard re-reads inside the write transaction. Between the
            // scan and here, something may have pushed the date forward, or
            // moved the document out of the index's filter: both halves of
            // what made it a candidate are checked on what stands now.
            let outside_filter = std::cell::Cell::new(false);
            let removed = self.delete_guarded(coll, &id, |current| {
                if !is_expired(current, field, cutoff_ms) {
                    return false;
                }
                if filter.as_ref().is_some_and(|filter| !filter.selects(current)) {
                    outside_filter.set(true);
                    return false;
                }
                true
            })?;
            if removed {
                outcome.deleted += 1;
            } else if outside_filter.get() {
                outcome.skipped_filter += 1;
            } else {
                outcome.skipped += 1;
            }
        }
        Ok(outcome)
    }
}

/// The index keys whose date is at or before `cutoff_ms`, as `(lower,
/// upper)`, both inclusive.
///
/// Encoded exactly as the index encodes its keys, direction included: a
/// descending field inverts the ordering, so the two bounds swap. The upper
/// bound is inclusive, which is what makes a document whose date lands exactly
/// on the cutoff expire rather than waiting a pass. Keyed entries only: a
/// document the index could not key holds no date it can be expired by, and
/// reading it would make every pass reconsider it and count it as skipped
/// (ADR-139). The unkeyed run sorts below either bound.
fn expired_range(index: &IndexMeta, cutoff_ms: i64) -> Result<(Vec<u8>, Vec<u8>)> {
    let descending = index.fields.first().is_some_and(|f| f.descending);
    let oldest = date_key(i64::MIN, descending)?;
    let cutoff = date_key(cutoff_ms, descending)?;
    Ok(if descending { (cutoff, oldest) } else { (oldest, cutoff) })
}

/// One index key holding a single date, encoded as the index stores it.
fn date_key(millis: i64, descending: bool) -> Result<Vec<u8>> {
    let value = Bson::DateTime(bson::DateTime::from_millis(millis));
    Ok(keyenc::encode_compound_ordered(&[(value, descending)])?)
}

/// Whether `doc`'s date at `field` is at or before the cutoff.
///
/// A missing field, or one holding something that is not a date, is **not**
/// expired: a TTL index ignores documents it cannot date, which is what stops
/// a policy added to a heterogeneous collection from deleting everything that
/// happens not to carry the field.
fn is_expired(doc: &Document, field: &str, cutoff_ms: i64) -> bool {
    path::resolve(doc, field)
        .into_iter()
        .next()
        .and_then(|v| match v {
            Bson::DateTime(dt) => Some(dt.timestamp_millis()),
            _ => None,
        })
        .is_some_and(|millis| millis <= cutoff_ms)
}

/// A test-only point between an expiry pass's candidate scan and its deletes,
/// where a concurrent write lands in production. Per thread, and absent from
/// every build that ships.
#[cfg(test)]
pub(crate) mod hooks {
    use std::cell::RefCell;

    thread_local! {
        static BETWEEN: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }

    /// Run `write` once, in the next pass on this thread, after its scan.
    pub(crate) fn after_the_next_scan(write: impl FnOnce() + 'static) {
        BETWEEN.with(|b| *b.borrow_mut() = Some(Box::new(write)));
    }

    pub(super) fn between_scan_and_delete() {
        if let Some(write) = BETWEEN.with(|b| b.borrow_mut().take()) {
            write();
        }
    }
}

/// TTL indexes on a collection, in definition order.
pub fn ttl_indexes(coll: &CollectionMeta) -> impl Iterator<Item = &IndexMeta> {
    coll.indexes.iter().filter(|i| i.is_ttl())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use kimmy_core::IndexField;

    fn dt(millis: i64) -> Bson {
        Bson::DateTime(bson::DateTime::from_millis(millis))
    }

    fn engine() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("app", "sessions").unwrap();
        (engine, coll, dir)
    }

    /// Create a TTL index and return the refreshed collection metadata.
    fn with_ttl(engine: &Engine, secs: i64) -> (CollectionMeta, IndexMeta) {
        let index = engine
            .create_index_with(
                "app",
                "sessions",
                vec![IndexField::ascending("seen")],
                false,
                Default::default(),
                Some("ttl_seen".into()),
                Some(secs),
                None,
            )
            .unwrap();
        (engine.get_collection("app", "sessions").unwrap(), index)
    }

    #[test]
    fn an_expired_document_is_removed_and_a_fresh_one_is_not() {
        let (engine, _, _dir) = engine();
        let (coll, index) = with_ttl(&engine, 60);

        engine.insert(&coll, doc! {"_id": 1, "seen": dt(0)}).unwrap();
        engine.insert(&coll, doc! {"_id": 2, "seen": dt(100_000)}).unwrap();

        // now = 100_000 ms, ttl = 60 s, so the cutoff is 40_000.
        let out = engine.expire_documents(&coll, &index, 100_000).unwrap();
        assert_eq!(out.deleted, 1);
        assert_eq!(out.skipped, 0);

        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(1)).unwrap().is_none());
        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(2)).unwrap().is_some());
    }

    #[test]
    fn a_document_exactly_on_the_cutoff_expires() {
        let (engine, _, _dir) = engine();
        let (coll, index) = with_ttl(&engine, 60);
        engine.insert(&coll, doc! {"_id": 1, "seen": dt(40_000)}).unwrap();

        let out = engine.expire_documents(&coll, &index, 100_000).unwrap();
        assert_eq!(out.deleted, 1, "the bound is inclusive");
    }

    #[test]
    fn a_non_date_field_is_ignored_rather_than_expired() {
        // The failure this prevents: adding a TTL policy to a collection where
        // some documents carry a string in that field must not delete them.
        let (engine, _, _dir) = engine();
        let (coll, index) = with_ttl(&engine, 0);

        engine.insert(&coll, doc! {"_id": 1, "seen": "not a date"}).unwrap();
        engine.insert(&coll, doc! {"_id": 2, "seen": 12345i64}).unwrap();
        engine.insert(&coll, doc! {"_id": 3}).unwrap();

        let out = engine.expire_documents(&coll, &index, 10_000_000).unwrap();
        assert_eq!(out.deleted, 0);
        for id in 1..=3 {
            assert!(engine.get(&coll, &kimmy_core::DocId::Int64(id)).unwrap().is_some());
        }
    }

    #[test]
    fn a_document_refreshed_before_the_pass_is_not_a_candidate() {
        // Refreshed before the pass runs, the document's date is outside the
        // range the scan reads, so it is never offered and the delete's guard
        // never sees it. This test used to be the date guard's only one and
        // could not fail without it (ADR-181);
        // `a_document_refreshed_after_the_scan_is_not_deleted` is the guard's.
        let (engine, _, _dir) = engine();
        let (coll, index) = with_ttl(&engine, 60);
        engine.insert(&coll, doc! {"_id": 1, "seen": dt(0)}).unwrap();

        // Refresh the session before the pass runs.
        engine
            .replace(
                &coll,
                &kimmy_core::DocId::Int64(1),
                doc! {"_id": 1, "seen": dt(100_000)},
                false,
            )
            .unwrap();

        let out = engine.expire_documents(&coll, &index, 100_000).unwrap();
        assert_eq!(out.deleted, 0, "a refreshed document must survive its old candidacy");
        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(1)).unwrap().is_some());
    }

    #[test]
    fn an_index_without_a_policy_expires_nothing() {
        let (engine, _, _dir) = engine();
        let index = engine
            .create_index("app", "sessions", vec![IndexField::ascending("seen")], false, None)
            .unwrap();
        let coll = engine.get_collection("app", "sessions").unwrap();
        engine.insert(&coll, doc! {"_id": 1, "seen": dt(0)}).unwrap();

        let out = engine.expire_documents(&coll, &index, u64::MAX).unwrap();
        assert_eq!(out, ExpiryOutcome::default());
        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(1)).unwrap().is_some());
    }

    #[test]
    fn a_descending_ttl_index_scans_the_right_way_round() {
        // A descending field inverts the key encoding, so the bounds swap. Get
        // this wrong and the pass either expires nothing or expires the newest
        // documents instead of the oldest.
        let (engine, _, _dir) = engine();
        let index = engine
            .create_index_with(
                "app",
                "sessions",
                vec![IndexField::descending("seen")],
                false,
                Default::default(),
                Some("ttl_desc".into()),
                Some(60),
                None,
            )
            .unwrap();
        let coll = engine.get_collection("app", "sessions").unwrap();

        engine.insert(&coll, doc! {"_id": 1, "seen": dt(0)}).unwrap();
        engine.insert(&coll, doc! {"_id": 2, "seen": dt(100_000)}).unwrap();

        let out = engine.expire_documents(&coll, &index, 100_000).unwrap();
        assert_eq!(out.deleted, 1);
        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(1)).unwrap().is_none());
        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(2)).unwrap().is_some());
    }

    #[test]
    fn a_huge_policy_expires_nothing_rather_than_wrapping() {
        // saturating_mul: i64::MAX seconds in milliseconds overflows, and a
        // wrapped cutoff would land in the future and delete the collection.
        let (engine, _, _dir) = engine();
        let (coll, index) = with_ttl(&engine, i64::MAX);
        engine.insert(&coll, doc! {"_id": 1, "seen": dt(0)}).unwrap();

        let out = engine.expire_documents(&coll, &index, 100_000).unwrap();
        assert_eq!(out.deleted, 0);
        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(1)).unwrap().is_some());
    }

    #[test]
    fn expiry_appends_an_ordinary_delete_so_it_replicates() {
        // Decision three: an expiry is indistinguishable from a user delete on
        // the wire, so nothing about replication or change streams needs a new
        // case. This pins that it really is OpKind::Delete.
        let (engine, _, _dir) = engine();
        let (coll, index) = with_ttl(&engine, 60);
        engine.insert(&coll, doc! {"_id": 1, "seen": dt(0)}).unwrap();

        let mut rx = engine.subscribe();
        engine.expire_documents(&coll, &index, 100_000).unwrap();

        let event = rx.try_recv().expect("expiry publishes an event");
        assert_eq!(event.kind, kimmy_core::OpKind::Delete);
        assert_eq!(event.doc_id, Some(kimmy_core::DocId::Int64(1)));
    }

    #[test]
    fn a_pass_is_bounded_and_says_so() {
        let (engine, _, _dir) = engine();
        let (coll, index) = with_ttl(&engine, 0);
        for id in 0..(MAX_EXPIRED_PER_PASS as i64 + 5) {
            engine.insert(&coll, doc! {"_id": id, "seen": dt(0)}).unwrap();
        }

        let out = engine.expire_documents(&coll, &index, 1_000_000).unwrap();
        assert_eq!(out.deleted, MAX_EXPIRED_PER_PASS as u64);
        assert!(out.truncated, "the caller has to be able to tell there is more");

        // The remainder drains on the next pass rather than being forgotten.
        let out = engine.expire_documents(&coll, &index, 1_000_000).unwrap();
        assert_eq!(out.deleted, 5);
        assert!(!out.truncated);
    }

    #[test]
    fn ttl_indexes_selects_only_those_with_a_policy() {
        let (engine, _, _dir) = engine();
        engine
            .create_index("app", "sessions", vec![IndexField::ascending("other")], false, None)
            .unwrap();
        with_ttl(&engine, 60);
        let coll = engine.get_collection("app", "sessions").unwrap();

        let names: Vec<&str> = ttl_indexes(&coll).map(|i| i.name.as_str()).collect();
        assert_eq!(names, vec!["ttl_seen"]);
    }

    /// A shared engine with `app.sessions`, for a test whose hook writes to it
    /// from inside a pass.
    fn shared() -> (std::sync::Arc<Engine>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "sessions").unwrap();
        (std::sync::Arc::new(engine), dir)
    }

    /// A TTL index on `seen`, 60 s, with `filter` as its partial filter.
    fn with_filtered_ttl(engine: &Engine, filter: Document) -> (CollectionMeta, IndexMeta) {
        let index = engine
            .create_index_with(
                "app",
                "sessions",
                vec![IndexField::ascending("seen")],
                false,
                Default::default(),
                Some("ttl_seen".into()),
                Some(60),
                Some(filter),
            )
            .unwrap();
        (engine.get_collection("app", "sessions").unwrap(), index)
    }

    #[test]
    fn a_document_moved_out_of_the_filter_after_the_scan_is_not_deleted() {
        // The race: the scan offers the document, a client reopens it -- out
        // of the filter, date unchanged -- and the delete comes after. The
        // index's maintenance takes it out of the index in that write, but
        // the candidate list was read before it (ADR-181).
        let (engine, _dir) = shared();
        let (coll, index) = with_filtered_ttl(&engine, doc! {"state": "done"});
        engine.insert(&coll, doc! {"_id": 1, "state": "done", "seen": dt(0)}).unwrap();

        let (writer, at) = (engine.clone(), coll.clone());
        hooks::after_the_next_scan(move || {
            writer
                .replace(
                    &at,
                    &kimmy_core::DocId::Int64(1),
                    doc! {"_id": 1, "state": "open", "seen": dt(0)},
                    false,
                )
                .unwrap();
        });
        let out = engine.expire_documents(&coll, &index, 100_000).unwrap();

        assert_eq!(out.skipped_filter, 1, "offered by the scan, declined by the filter: {out:?}");
        assert_eq!((out.deleted, out.skipped), (0, 0), "{out:?}");
        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(1)).unwrap().is_some());
    }

    #[test]
    fn a_document_refreshed_after_the_scan_is_not_deleted() {
        // The heartbeat, landing where it lands in production: after the scan
        // and before the delete. The document stays in the filter, so only
        // the date can decline it -- which is what tells the date check and
        // the filter check apart.
        let (engine, _dir) = shared();
        let (coll, index) = with_filtered_ttl(&engine, doc! {"state": "done"});
        engine.insert(&coll, doc! {"_id": 1, "state": "done", "seen": dt(0)}).unwrap();

        let (writer, at) = (engine.clone(), coll.clone());
        hooks::after_the_next_scan(move || {
            writer
                .replace(
                    &at,
                    &kimmy_core::DocId::Int64(1),
                    doc! {"_id": 1, "state": "done", "seen": dt(100_000)},
                    false,
                )
                .unwrap();
        });
        let out = engine.expire_documents(&coll, &index, 100_000).unwrap();

        assert_eq!(out.skipped, 1, "offered by the scan, declined by the date: {out:?}");
        assert_eq!((out.deleted, out.skipped_filter), (0, 0), "{out:?}");
        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(1)).unwrap().is_some());
    }

    /// Entries in `index` for the documents of `coll` that `declined` names,
    /// keyed by their date exactly as the index keys them, and returning how
    /// many were written: the state a partial index built before ADR-183 held.
    ///
    /// Constructed, rather than obtained by inserting documents, because
    /// ADR-183 removed the route the old fixture took. Membership is now
    /// `find`'s, so a document the filter declines is given no entry, and no
    /// sequence of writes produces an index holding what its filter does not
    /// select. What the pass does when it *meets* such an entry is what this
    /// fixture is for, and until that release every partial index over a
    /// mixed-type field held them.
    fn hold_the_entries_a_pre_adr_183_index_held(
        engine: &Engine,
        coll: &CollectionMeta,
        index: &IndexMeta,
        declined: &dyn Fn(&Document) -> bool,
    ) -> usize {
        use redb::ReadableTable;

        let db = engine.db();
        let txn = db.begin_write().unwrap();
        let mut held = 0;
        {
            let mut entries = txn.open_table(crate::tables::INDEX_ENTRIES).unwrap();
            let docs = txn.open_table(crate::tables::DOCS).unwrap();
            for row in docs.range(crate::engine::doc_range(coll.id)).unwrap() {
                let (key, value) = row.unwrap();
                let doc = crate::codec::decode_doc_record(value.value())
                    .unwrap()
                    .document()
                    .unwrap()
                    .unwrap();
                if !declined(&doc) {
                    continue;
                }
                let at = path::resolve(&doc, "seen")
                    .into_iter()
                    .next()
                    .and_then(|v| match v {
                        Bson::DateTime(dt) => Some(dt.timestamp_millis()),
                        _ => None,
                    })
                    .expect("the fixture dates every document at seen");
                let k = date_key(at, false).unwrap();
                entries.insert((coll.id.0, index.id, k.as_slice(), key.value().1), ()).unwrap();
                held += 1;
            }
        }
        txn.commit().unwrap();
        held
    }

    #[test]
    fn declined_candidates_do_not_keep_a_pass_from_what_is_behind_them() {
        // The review's shape (ADR-181). More entries than one pass examines,
        // which the index holds and `find` does not select, all expired, and
        // one genuinely expired document dated after them. When every pass
        // started from the front, each declined the same entries first and none
        // reached the last one: TTL for the index made no progress again.
        let (engine, _, _dir) = engine();
        let (coll, index) = with_filtered_ttl(&engine, doc! {"size": {"$gt": 5}});
        let stale = MAX_EXPIRED_PER_PASS as i64;
        let mut docs: Vec<Document> =
            (0..stale).map(|i| doc! {"_id": i, "size": "large", "seen": dt(i)}).collect();
        docs.push(doc! {"_id": stale, "size": 10, "seen": dt(stale + 1_000)});
        engine.insert_many(&coll, docs).unwrap();
        // Written in, not indexed in: `{size: {$gt: 5}}` does not select a
        // string, so under ADR-183 the index holds none of them of its own
        // accord. This is the third premise ADR-183 retired.
        let held = hold_the_entries_a_pre_adr_183_index_held(&engine, &coll, &index, &|doc| {
            doc.get_str("size").is_ok()
        });
        assert_eq!(
            held, stale as usize,
            "premise: the index holds every document the filter declines"
        );
        let now = 10_000_000;

        let first = engine.expire_documents(&coll, &index, now).unwrap();
        assert_eq!(
            (first.deleted, first.skipped_filter, first.truncated),
            (0, stale as u64, true),
            "premise: the first pass is all declines, and says there is more: {first:?}"
        );

        // Within ceil(1,001 / 1,000) = 2 passes, and without examining the
        // declined entries again inside the cycle.
        let second = engine.expire_documents(&coll, &index, now).unwrap();
        assert_eq!((second.deleted, second.skipped_filter), (1, 0), "{second:?}");
        assert!(!second.truncated, "the cycle ended: {second:?}");
        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(stale)).unwrap().is_none());

        // A new cycle starts from the front: a declined entry is examined
        // once a cycle, not never.
        let third = engine.expire_documents(&coll, &index, now).unwrap();
        assert_eq!((third.deleted, third.skipped_filter), (0, stale as u64), "{third:?}");
    }

    /// A TTL index on `seen`, 60 s, over 1,001 expired documents dated by
    /// `_id`, after a pass that stopped at its budget: `_id` 1,000 is left,
    /// and the cursor is at `_id` 999.
    fn after_a_truncated_pass(engine: &Engine) -> (CollectionMeta, IndexMeta) {
        let (coll, index) = with_ttl(engine, 60);
        let docs = (0..=MAX_EXPIRED_PER_PASS as i64).map(|i| doc! {"_id": i, "seen": dt(i)});
        engine.insert_many(&coll, docs.collect()).unwrap();
        let out = engine.expire_documents(&coll, &index, 10_000_000).unwrap();
        assert!(out.truncated, "premise: the pass stops at its budget: {out:?}");
        assert!(engine.expiry_cursor((coll.id.0, index.id)).is_some(), "premise: with a cursor");
        (coll, index)
    }

    #[test]
    fn a_collection_dropped_and_recreated_expires_from_the_front() {
        // Ids are derived from names, so a recreation lands on the cursor the
        // dropped collection's last pass left, unless the drop forgets it. Its
        // first pass then started after a position in an index that no longer
        // exists, and deleted nothing dated below it (ADR-181).
        let (engine, _, _dir) = engine();
        let (dropped, dropped_index) = after_a_truncated_pass(&engine);
        assert!(engine.drop_collection("app", "sessions").unwrap());
        assert!(
            engine.expiry_cursor((dropped.id.0, dropped_index.id)).is_none(),
            "the drop forgets where the collection's scan stopped"
        );

        // The drop's purge finished: this test is about what comes after it.
        engine.finish_purges_now().unwrap();
        engine.create_collection("app", "sessions").unwrap();
        let (coll, index) = with_ttl(&engine, 60);
        assert_eq!(
            (coll.id, index.id),
            (dropped.id, dropped_index.id),
            "premise: the recreation lands on the same ids"
        );
        // Dated below where the dropped collection's pass stopped.
        engine.insert(&coll, doc! {"_id": 0, "seen": dt(0)}).unwrap();

        let out = engine.expire_documents(&coll, &index, 10_000_000).unwrap();
        assert_eq!(out.deleted, 1, "on the recreation's first pass: {out:?}");
    }

    #[test]
    fn an_index_dropped_and_recreated_expires_from_the_front() {
        let (engine, _, _dir) = engine();
        let (coll, dropped) = after_a_truncated_pass(&engine);
        assert!(engine.drop_index("app", "sessions", "ttl_seen").unwrap());
        assert!(
            engine.expiry_cursor((coll.id.0, dropped.id)).is_none(),
            "the drop forgets where the index's scan stopped"
        );

        let (coll, index) = with_ttl(&engine, 60);
        assert_eq!(index.id, dropped.id, "premise: the recreation lands on the same id");
        // Dated below where the dropped index's pass stopped; `_id` 1,000 is
        // above it.
        engine.insert(&coll, doc! {"_id": -1, "seen": dt(0)}).unwrap();

        let out = engine.expire_documents(&coll, &index, 10_000_000).unwrap();
        assert_eq!(out.deleted, 2, "on the recreation's first pass: {out:?}");
    }

    #[test]
    fn an_index_superseded_under_its_name_expires_from_the_front() {
        // A peer's definition of the name, created later, replaces the one
        // here without a drop (ADR-132), under the same derived id.
        let (engine, _, _dir) = engine();
        let (_, held) = after_a_truncated_pass(&engine);
        let later = kimmy_core::Stamp::new(
            kimmy_core::Hlc::new(u64::MAX / 2, 0),
            kimmy_core::NodeId::from_bytes([9; 16]),
        );
        engine
            .create_index_inner(
                "app",
                "sessions",
                vec![IndexField::ascending("seen")],
                false,
                Default::default(),
                Some("ttl_seen".into()),
                Some(120),
                None,
                crate::index::CreateOrigin::Replicated { created: Some(later), logged: None },
                &|_, _| false,
            )
            .unwrap();
        let coll = engine.get_collection("app", "sessions").unwrap();
        let index = coll.index("ttl_seen").unwrap().clone();
        assert_eq!(
            (index.id, index.expire_after_secs),
            (held.id, Some(120)),
            "premise: the peer's definition stands, under the same id"
        );
        assert!(
            engine.expiry_cursor((coll.id.0, index.id)).is_none(),
            "building it forgets where the superseded index's scan stopped"
        );

        engine.insert(&coll, doc! {"_id": -1, "seen": dt(0)}).unwrap();
        let out = engine.expire_documents(&coll, &index, 10_000_000).unwrap();
        assert_eq!(out.deleted, 2, "on the new definition's first pass: {out:?}");
    }

    #[test]
    fn a_filter_this_build_cannot_parse_skips_its_index_and_deletes_nothing() {
        // A filter an earlier build accepted and this one refuses: a
        // `Decimal128` operand, from before parsing refused it, handed to the
        // pass as a definition that build stored would be.
        let (engine, _, _dir) = engine();
        let (coll, mut index) = with_filtered_ttl(&engine, doc! {"size": {"$gt": 5}});
        engine.insert(&coll, doc! {"_id": 1, "size": 10, "seen": dt(0)}).unwrap();
        index.partial_filter = Some(doc! {"size": {"$gt": bson::Decimal128::from_bytes([0; 16])}});
        assert!(index.partial().unwrap().is_err(), "premise: this build refuses the filter");

        let out = engine.expire_documents(&coll, &index, 10_000_000).unwrap();

        assert_eq!(out, ExpiryOutcome::default(), "skipped, not failed");
        assert!(engine.get(&coll, &kimmy_core::DocId::Int64(1)).unwrap().is_some());
    }

    #[test]
    fn a_range_filter_expires_only_what_find_selects() {
        // `{size: {$gt: 5}}` as `find` reads it selects a number above five.
        // Membership used to compare across type brackets, so the index also
        // held a string, a document and a boolean, and expiry deleted all
        // four; ADR-181 made the delete re-check the filter. Since ADR-183 the
        // index holds only what the filter selects, so the other three are
        // never candidates at all.
        let (engine, _, _dir) = engine();
        let filter = doc! {"size": {"$gt": 5}};
        let (coll, index) = with_filtered_ttl(&engine, filter.clone());
        let sizes =
            [Bson::Int32(10), Bson::Int32(3), "large".into(), doc! {"w": 1}.into(), true.into()];
        for (id, size) in sizes.iter().enumerate() {
            engine
                .insert(&coll, doc! {"_id": id as i64, "size": size.clone(), "seen": dt(0)})
                .unwrap();
        }
        let held = crate::index::scan_range(
            engine.db(),
            coll.id,
            index.id,
            &[],
            None,
            crate::index::Unkeyed::Include,
        )
        .unwrap();
        assert_eq!(held.len(), 1, "the index holds only the number above five");

        let out = engine.expire_documents(&coll, &index, 100_000).unwrap();

        assert_eq!((out.deleted, out.skipped_filter), (1, 0), "{out:?}");
        let left: Vec<i64> = (0..5)
            .filter(|id| engine.get(&coll, &kimmy_core::DocId::Int64(*id)).unwrap().is_some())
            .collect();
        assert_eq!(left, [1, 2, 3, 4], "only size 10 expires");
    }
}
