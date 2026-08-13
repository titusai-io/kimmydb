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

use bson::{Bson, Document};
use kimmy_core::{IndexMeta, keyenc, path};

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

/// What one collection's pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExpiryOutcome {
    /// Documents actually removed.
    pub deleted: u64,
    /// Candidates the scan offered that were no longer eligible when the
    /// delete transaction re-read them — a document whose date moved on
    /// between the scan and the write.
    pub skipped: u64,
    /// Whether the pass stopped at [`MAX_EXPIRED_PER_PASS`] with more to do.
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

        let candidates = self.expired_candidates(coll, index, cutoff_ms)?;
        let truncated = candidates.len() > MAX_EXPIRED_PER_PASS;

        let mut outcome = ExpiryOutcome { truncated, ..Default::default() };
        for key in candidates.into_iter().take(MAX_EXPIRED_PER_PASS) {
            // The scan hands back encoded document keys, and `keyenc` is
            // one-way, so the id comes from the document itself.
            let Some(doc) = self.get_by_encoded_key(coll, &key)? else {
                continue;
            };
            let Some(id) = doc.get("_id").and_then(|v| kimmy_core::DocId::try_from_bson(v).ok())
            else {
                continue;
            };

            // The guard re-reads inside the write transaction: between the
            // scan and here, something may have pushed the date forward.
            let removed =
                self.delete_guarded(coll, &id, |current| is_expired(current, field, cutoff_ms))?;
            if removed {
                outcome.deleted += 1;
            } else {
                outcome.skipped += 1;
            }
        }
        Ok(outcome)
    }

    /// Encoded document keys whose indexed date is at or before `cutoff_ms`.
    fn expired_candidates(
        &self,
        coll: &CollectionMeta,
        index: &IndexMeta,
        cutoff_ms: i64,
    ) -> Result<Vec<Vec<u8>>> {
        // Encoded exactly as the index encodes its keys, direction included —
        // a descending field inverts the ordering, so the two bounds swap.
        let descending = index.fields.first().is_some_and(|f| f.descending);
        let oldest = date_key(i64::MIN, descending)?;
        let cutoff = date_key(cutoff_ms, descending)?;
        let (lower, upper) = if descending { (cutoff, oldest) } else { (oldest, cutoff) };

        // The upper bound is inclusive, which is what makes a document whose
        // date lands exactly on the cutoff expire rather than waiting a pass.
        self.index_candidates(coll, index.id, &lower, &upper)
    }
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
    fn a_document_whose_date_moved_on_is_skipped_not_deleted() {
        // The heartbeat case, and the reason the guard runs inside the write
        // transaction. The scan is taken against an old cutoff, the document
        // is then refreshed, and the delete must decline.
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
}
