//! Anti-entropy: deciding what to send a peer, and applying what one sends.
//!
//! Deliberately transport-free. Everything here works between two `Engine`
//! values in one process, which is how it is tested — convergence is a property
//! of the merge rules, not of the network, and mixing the two would make
//! failures ambiguous. The gossip transport calls into this; it does not
//! reimplement it.
//!
//! ```text
//!   A                                    B
//!   │  version_vector() ────────────────▶│
//!   │                                    │  behind(theirs) -> Some(from)
//!   │◀──────────── entries_for_peer(from)│
//!   │  apply_batch(entries)              │
//! ```
//!
//! Both directions run the same exchange, which is why one round converges both
//! ways rather than only pushing.

use kimmy_core::{Hlc, OpKind, OplogEntry};
use tracing::{debug, warn};

use crate::engine::Engine;
use crate::error::Result;

/// What applying a batch of replicated entries did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncOutcome {
    /// Entries that won and changed a document.
    pub applied: usize,
    /// Entries already covered by an equal or newer local version. Expected,
    /// not an error: peers resend overlapping ranges by design.
    pub superseded: usize,
    /// Entries for a collection this node does not have.
    ///
    /// **Currently unavoidable**, and the reason M4 is not finished: collection
    /// creation is logged with no payload naming it, so a peer cannot create
    /// the collection an entry refers to. Counted rather than silently dropped
    /// so the gap is visible instead of looking like convergence.
    pub unknown_collection: usize,
}

impl SyncOutcome {
    pub fn total(&self) -> usize {
        self.applied + self.superseded + self.unknown_collection
    }
}

impl Engine {
    /// Entries at or after `from`, for a peer that asked to catch up.
    ///
    /// Stamp order, not arrival order: a peer's question is "what do you hold
    /// after this logical time", which is about origin stamps. Change streams
    /// ask a different question and use the arrival index instead.
    ///
    /// **Unique-violation entries are excluded.** They record what *this* node
    /// observed when it merged, and every node observes the same collision
    /// independently — shipping them would report one violation once per node.
    /// See [ADR-029](../../../docs/decisions.md).
    pub fn entries_for_peer(&self, from: Hlc, limit: usize) -> Result<Vec<OplogEntry>> {
        Ok(self
            .read_oplog_from(from, limit)?
            .into_iter()
            .filter(|entry| entry.kind != OpKind::UniqueViolation)
            .collect())
    }

    /// Merge a batch of entries received from a peer.
    ///
    /// Each entry goes through `apply_remote`, so last-writer-wins decides
    /// per document and re-delivery is harmless. Ordering within the batch does
    /// not matter — that is the point of LWW, and it is what lets a peer send
    /// a range without coordinating.
    pub fn apply_batch(&self, entries: &[OplogEntry]) -> Result<SyncOutcome> {
        let mut outcome = SyncOutcome::default();

        for entry in entries {
            // A node's own observation of a broken constraint is not a fact
            // about the data; refuse it even if a peer sends one.
            if entry.kind == OpKind::UniqueViolation {
                continue;
            }

            let Some(collection) = self.collection_by_id(entry.collection)? else {
                outcome.unknown_collection += 1;
                continue;
            };

            if self.apply_remote(&collection, entry)? {
                outcome.applied += 1;
            } else {
                outcome.superseded += 1;
            }
        }

        if outcome.unknown_collection > 0 {
            warn!(
                entries = outcome.unknown_collection,
                "skipped replicated entries for collections this node does not have; \
                 collection replication is not implemented yet"
            );
        }
        debug!(
            applied = outcome.applied,
            superseded = outcome.superseded,
            "merged a batch from a peer"
        );
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use kimmy_core::DocId;

    const BATCH: usize = 1024;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        (engine, dir)
    }

    /// One direction of an anti-entropy round: pull into `into` from `from`.
    fn pull(into: &Engine, from: &Engine) -> SyncOutcome {
        let mine = into.version_vector().unwrap();
        let theirs = from.version_vector().unwrap();
        match mine.behind(&theirs) {
            Some(start) => {
                let entries = from.entries_for_peer(start, BATCH).unwrap();
                into.apply_batch(&entries).unwrap()
            }
            None => SyncOutcome::default(),
        }
    }

    /// A full round, both directions, as two gossiping peers would run it.
    fn sync(a: &Engine, b: &Engine) {
        pull(a, b);
        pull(b, a);
    }

    #[test]
    fn a_fresh_engine_has_an_empty_vector() {
        let (engine, _dir) = engine();
        assert!(engine.version_vector().unwrap().is_empty());
    }

    #[test]
    fn writing_advances_this_nodes_entry() {
        let (engine, _dir) = engine();
        let coll = engine.create_collection("db", "c").unwrap();
        engine.insert(&coll, doc! { "_id": 1 }).unwrap();

        let vector = engine.version_vector().unwrap();
        assert_eq!(vector.len(), 1);
        assert!(vector.get(engine.node_id()) > Hlc::ZERO);
    }

    #[test]
    fn the_vector_is_rebuilt_when_it_disagrees_with_the_oplog() {
        // Derived state, so a database written before it existed is repaired
        // rather than refused.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let (node, expected) = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("db", "c").unwrap();
            engine.insert(&coll, doc! { "_id": 1 }).unwrap();
            (engine.node_id(), engine.version_vector().unwrap())
        };

        {
            let db = redb::Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut versions = txn.open_table(crate::tables::OPLOG_VERSIONS).unwrap();
                versions.retain(|_, _| false).unwrap();
            }
            txn.commit().unwrap();
        }

        let reopened = Engine::open(&path).unwrap();
        assert_eq!(reopened.version_vector().unwrap(), expected);
        assert!(reopened.version_vector().unwrap().get(node) > Hlc::ZERO);
    }

    #[test]
    fn two_engines_converge_after_one_round() {
        let (a, dir_a) = engine();
        let (b, dir_b) = engine();
        // Same names, so both derive the same collection id — the property that
        // makes any of this work at all.
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();

        a.insert(&ca, doc! { "_id": "from-a", "v": 1 }).unwrap();
        b.insert(&cb, doc! { "_id": "from-b", "v": 2 }).unwrap();

        sync(&a, &b);

        for (engine, coll) in [(&a, &ca), (&b, &cb)] {
            assert!(engine.get(coll, &DocId::String("from-a".into())).unwrap().is_some());
            assert!(engine.get(coll, &DocId::String("from-b".into())).unwrap().is_some());
        }
        drop((dir_a, dir_b));
    }

    #[test]
    fn a_second_round_transfers_nothing() {
        // Convergence has to be stable, or gossip would ship the same entries
        // forever.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        b.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();

        sync(&a, &b);
        let second = pull(&b, &a);

        assert_eq!(second, SyncOutcome::default(), "a converged pair must exchange nothing");
    }

    #[test]
    fn conflicting_writes_converge_to_the_same_document() {
        // Both nodes write the same _id concurrently. LWW decides, and both
        // sides must land on the same winner.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();

        a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "who": "a" }, true).unwrap();
        b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "who": "b" }, true).unwrap();

        sync(&a, &b);
        sync(&a, &b);

        let from_a = a.get(&ca, &DocId::Int64(1)).unwrap().unwrap();
        let from_b = b.get(&cb, &DocId::Int64(1)).unwrap().unwrap();
        assert_eq!(from_a, from_b, "both nodes must agree on the winner");
    }

    #[test]
    fn a_deletion_replicates_as_a_deletion() {
        // The tombstone is what stops the delete being undone by a peer that
        // still holds the document.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();

        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        sync(&a, &b);
        assert!(b.get(&cb, &DocId::Int64(1)).unwrap().is_some());

        a.delete(&ca, &DocId::Int64(1)).unwrap();
        sync(&a, &b);

        assert!(b.get(&cb, &DocId::Int64(1)).unwrap().is_none(), "the delete must replicate");
    }

    #[test]
    fn unique_violation_entries_are_never_sent() {
        // They are this node's observation of a collision, and every node makes
        // the same observation when it merges. Sending them would report one
        // violation once per node.
        let (a, _da) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "local", "email": "clash@x" }).unwrap();

        let entry = OplogEntry {
            stamp: kimmy_core::Stamp::new(Hlc::new(9_000, 0), kimmy_core::NodeId::generate()),
            kind: OpKind::Insert,
            collection: ca.id,
            doc_id: Some(DocId::String("remote".into())),
            body: Some(
                bson::serialize_to_vec(&doc! { "_id": "remote", "email": "clash@x" }).unwrap(),
            ),
        };
        a.apply_remote(&ca, &entry).unwrap();
        assert_eq!(a.unique_violations(), 1, "the collision must have been recorded");

        let outgoing = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap();
        assert!(
            outgoing.iter().all(|e| e.kind != OpKind::UniqueViolation),
            "a violation entry must not be replicated"
        );
        assert!(!outgoing.is_empty(), "ordinary entries must still be sent");
    }

    #[test]
    fn entries_for_an_unknown_collection_are_counted_not_dropped() {
        // Collection creation carries no payload naming it, so a peer cannot
        // create the collection an entry refers to. Until that is fixed this
        // must be visible rather than looking like successful convergence.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();

        // `b` never created the collection.
        let outcome = pull(&b, &a);

        assert!(outcome.unknown_collection > 0, "the gap must be counted: {outcome:?}");
        assert_eq!(outcome.applied, 0);
    }

    #[test]
    fn a_node_joining_late_receives_the_whole_history() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        for i in 0..50i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }

        sync(&a, &b);

        assert_eq!(b.count(&cb).unwrap(), 50, "an empty peer must catch up from zero");
    }

    #[test]
    fn three_nodes_converge_through_a_middle_peer() {
        // A and C never talk directly. Convergence has to be transitive, or a
        // partially connected cluster silently diverges.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let (c, _dc) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        let cc = c.create_collection("shop", "orders").unwrap();

        a.insert(&ca, doc! { "_id": "a" }).unwrap();
        c.insert(&cc, doc! { "_id": "c" }).unwrap();

        sync(&a, &b);
        sync(&b, &c);
        sync(&a, &b);

        for (engine, coll) in [(&a, &ca), (&b, &cb), (&c, &cc)] {
            assert!(engine.get(coll, &DocId::String("a".into())).unwrap().is_some());
            assert!(engine.get(coll, &DocId::String("c".into())).unwrap().is_some());
        }
    }

    #[test]
    fn a_node_behind_on_two_peers_at_different_points_receives_both() {
        // The case the simpler convergence tests miss, and the reason the
        // request threshold is the *lowest* deficient point rather than any
        // other: C is behind on both A and B, and what it is missing from B is
        // stamped EARLIER than what it already holds from A.
        //
        // Starting the request at C's position for A would skip B's older entry
        // entirely — silently, since nothing else would notice.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let (c, _dc) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        let cc = c.create_collection("shop", "orders").unwrap();

        // Oldest write in the cluster, and C never hears of it directly.
        b.insert(&cb, doc! { "_id": "b-oldest" }).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));

        // C catches up with A, so its vector holds a *non-zero* mark for A that
        // is later than B's write.
        a.insert(&ca, doc! { "_id": "a-first" }).unwrap();
        sync(&a, &c);
        std::thread::sleep(std::time::Duration::from_millis(5));

        // A writes again, so C is behind on A as well — but only slightly.
        a.insert(&ca, doc! { "_id": "a-second" }).unwrap();

        // B learns everything, so a single peer can serve both histories.
        sync(&a, &b);
        pull(&c, &b);

        for id in ["b-oldest", "a-first", "a-second"] {
            assert!(
                c.get(&cc, &DocId::String(id.into())).unwrap().is_some(),
                "{id} was skipped; the request started later than the earliest gap"
            );
        }
    }

    fn field(path: &str) -> crate::meta::IndexField {
        crate::meta::IndexField { path: path.into(), descending: false }
    }
}
