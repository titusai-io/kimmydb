//! The clock bound covers every replicated stamp, a `document` entry included: no remote stamp is witnessed unless it has
//! passed the bound, so an honest member's logical clock never leads its own physical clock by more than
//! `max_clock_skew_secs` through replication.
//!
//! Before this, an entry of a `document` collection was applied as before whatever its stamp, and applying one witnessed
//! its stamp: `HlcClock::witness` has no clamp, so one entry a day ahead dragged the receiver's logical clock a day
//! forward, every later stamp the receiver minted sat at or above it, and every other honest member refused the
//! receiver's `field` writes and schema changes until due. System collections are `document` collections, so the entry
//! reached every member.

use bson::doc;
use kimmy_core::{DocId, Hlc, IndexField, MergePolicy, NodeId, OpKind, OplogEntry, Stamp};

use crate::{ClockRefusal, Engine, WalkScope, physical_now_ms};

const SKEW_MS: u64 = 300_000;
const A_DAY_MS: u64 = 86_400_000;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// Two honest members that share `shop.notes` (a `document` collection) and `shop.orders` (a `field` collection), and an
/// entry of `shop.notes` that a third member, whose clock is a day ahead, wrote.
struct Cluster {
    a: Engine,
    b: Engine,
    /// The creations, as both honest members hold them.
    creation_len: usize,
    far_entry: OplogEntry,
    _dirs: [tempfile::TempDir; 3],
}

fn cluster() -> Cluster {
    let (a, da) = engine();
    let (b, db) = engine();
    let (x, dx) = engine();
    for e in [&a, &x] {
        e.create_collection_with_policy("shop", "notes", MergePolicy::Document, None).unwrap();
    }
    a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let creation = everything(&a);
    b.apply_batch(&creation).unwrap();

    let notes_x = x.get_collection("shop", "notes").unwrap();
    x.insert(&notes_x, doc! { "_id": 1, "text": "from a clock a day ahead" }).unwrap();
    let mut far_entry = everything(&x).pop().expect("the insert");
    // The third member's clock is a day ahead: its stamp is, and the document is honest.
    far_entry.stamp = Stamp::new(
        Hlc::new(physical_now_ms() + A_DAY_MS, 0),
        NodeId::from_bytes([9; 16]),
        far_entry.stamp.epoch,
    );
    Cluster { a, b, creation_len: creation.len(), far_entry, _dirs: [da, db, dx] }
}

#[test]
fn a_far_future_document_entry_is_refused_by_name_and_not_witnessed() {
    let c = cluster();
    for member in [&c.a, &c.b] {
        let outcome = member.apply_batch(std::slice::from_ref(&c.far_entry)).unwrap();
        let stop = outcome
            .clock_refused_at
            .as_ref()
            .expect("a document entry a day ahead is refused by name: stamp");
        assert_eq!(stop.reason, ClockRefusal::Stamp);
        assert_eq!((outcome.applied, outcome.clock_refused), (0, 1), "{outcome:?}");
        assert_eq!(
            member.witnessed_vector().unwrap().get(c.far_entry.stamp.origin()),
            Hlc::ZERO,
            "not witnessed: the vector did not move"
        );
    }
}

#[test]
fn a_far_future_document_entry_does_not_drag_an_honest_members_clock_past_the_bound() {
    let c = cluster();
    for member in [&c.a, &c.b] {
        member.apply_batch(std::slice::from_ref(&c.far_entry)).unwrap();
        let lead = member.clock_last().wall_ms.saturating_sub(physical_now_ms());
        assert!(
            lead <= SKEW_MS,
            "an honest member's logical clock leads its physical clock by {lead} ms, more than the bound"
        );
    }
}

#[test]
fn a_field_write_and_a_schema_change_between_two_honest_members_replicate_after_a_far_future_document_entry()
 {
    let c = cluster();
    for member in [&c.a, &c.b] {
        member.apply_batch(std::slice::from_ref(&c.far_entry)).unwrap();
    }
    let orders_a = c.a.get_collection("shop", "orders").unwrap();
    c.a.insert(&orders_a, doc! { "_id": 7, "n": 7 }).unwrap();
    c.a.create_index("shop", "orders", vec![IndexField::ascending("n")], false, None).unwrap();
    let after: Vec<OplogEntry> = everything(&c.a).into_iter().skip(c.creation_len).collect();
    assert!(
        after.len() >= 2,
        "premise: the write and the index creation are in the oplog: {after:?}"
    );

    let outcome = c.b.apply_batch(&after).unwrap();

    assert_eq!(outcome.clock_refused, 0, "B refused A's stamps as far-future: {outcome:?}");
    let orders_b = c.b.get_collection("shop", "orders").unwrap();
    assert!(c.b.get(&orders_b, &DocId::Int64(7)).unwrap().is_some(), "the field write replicated");
    assert_eq!(c.b.list_indexes("shop", "orders").unwrap().len(), 1, "the createIndex replicated");
}

/// An entry that names no document is a collection-level operation: it merges nothing, and it used to be witnessed
/// whatever its stamp.
#[test]
fn a_far_future_entry_with_no_document_is_refused_and_not_witnessed() {
    let c = cluster();
    let mut entry = c.far_entry.clone();
    entry.doc_id = None;
    entry.body = None;
    for member in [&c.a, &c.b] {
        let outcome = member.apply_batch(std::slice::from_ref(&entry)).unwrap();
        assert_eq!((outcome.applied, outcome.clock_refused), (0, 1), "{outcome:?}");
        assert_eq!(member.witnessed_vector().unwrap().get(entry.stamp.origin()), Hlc::ZERO);
        let lead = member.clock_last().wall_ms.saturating_sub(physical_now_ms());
        assert!(lead <= SKEW_MS, "the logical clock followed the stamp: {lead} ms ahead");
    }
}

/// A loss record addressed to a `document` collection is ignored, and used to be witnessed before the bound was read.
#[test]
fn a_far_future_loss_record_for_a_document_collection_is_refused_and_not_witnessed() {
    let c = cluster();
    let mut entry = c.far_entry.clone();
    entry.kind = OpKind::LossRecord;
    for member in [&c.a, &c.b] {
        let outcome = member.apply_batch(std::slice::from_ref(&entry)).unwrap();
        assert_eq!((outcome.applied, outcome.clock_refused), (0, 1), "{outcome:?}");
        let lead = member.clock_last().wall_ms.saturating_sub(physical_now_ms());
        assert!(lead <= SKEW_MS, "the logical clock followed the stamp: {lead} ms ahead");
    }
}

/// The single-entry form (`apply_remote`, the snapshot restore's and the tests') of an entry with no document.
#[test]
fn the_single_entry_form_refuses_a_far_future_entry_with_no_document() {
    let c = cluster();
    let mut entry = c.far_entry.clone();
    entry.doc_id = None;
    entry.body = None;
    let notes = c.a.get_collection("shop", "notes").unwrap();
    let err = c.a.apply_remote(&notes, &entry).expect_err("refused by name");
    assert!(matches!(err, crate::StorageError::ClockRefused(_)), "{err:?}");
    let lead = c.a.clock_last().wall_ms.saturating_sub(physical_now_ms());
    assert!(lead <= SKEW_MS, "the logical clock followed the stamp: {lead} ms ahead");
}
