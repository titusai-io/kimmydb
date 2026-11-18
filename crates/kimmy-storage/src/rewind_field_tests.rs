//! The rewind of a `field` collection: a document's state at the target is the fold of the states its entries
//! carry (design q7-step3-rewind-design.md), not the post-image of one entry, so concurrent writes of different
//! fields are both put back; a document whose missing history may reach the target is refused, never guessed.

use std::time::Duration;

use bson::doc;
use kimmy_core::{DocId, Hlc, MergePolicy, OpKind, OplogEntry};

use crate::{CollectionMeta, Engine, RetentionPolicy, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(Duration::from_millis(20));
}

fn entries(engine: &Engine) -> Vec<OplogEntry> {
    engine.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn newest(engine: &Engine) -> Hlc {
    entries(engine).iter().map(|entry| entry.stamp.hlc).max().unwrap()
}

fn field_collection(engine: &Engine) -> CollectionMeta {
    engine.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap()
}

/// A document as its fields by name: the order of a merged document's fields is the merge state's, not the test's.
type Fields = Option<std::collections::BTreeMap<String, bson::Bson>>;

fn fields(document: Option<bson::Document>) -> Fields {
    document.map(|document| document.into_iter().collect())
}

fn read(engine: &Engine, coll: &CollectionMeta, id: i64) -> Fields {
    fields(engine.get(coll, &DocId::Int64(id)).unwrap())
}

fn want(document: bson::Document) -> Fields {
    fields(Some(document))
}

/// A takes B's entries and B takes A's: both then hold every write.
fn exchange(a: &Engine, b: &Engine) {
    a.apply_batch(&entries(b)).unwrap();
    b.apply_batch(&entries(a)).unwrap();
}

#[test]
fn a_rewind_puts_back_both_fields_two_members_wrote_concurrently() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    later();
    b.apply_batch(&entries(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1 }, false).unwrap();
    b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "y": 2 }, false).unwrap();
    later();
    exchange(&a, &b);
    assert_eq!(read(&a, &ca, 1), want(doc! { "_id": 1, "x": 1, "y": 2 }), "premise: both fields");
    let target = newest(&a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 9, "y": 2 }, false).unwrap();

    let outcome = a.rewind_to(target).unwrap();

    assert_eq!(outcome.reverted, 1);
    assert_eq!(
        read(&a, &ca, 1),
        want(doc! { "_id": 1, "x": 1, "y": 2 }),
        "the state at the target is the join of both members' writes, not one entry's image"
    );
}

#[test]
fn a_rewind_removes_a_field_document_created_after_the_target_and_leaves_no_tombstone() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let target = newest(&a);
    later();
    a.insert(&ca, doc! { "_id": 2 }).unwrap();

    let outcome = a.rewind_to(target).unwrap();

    assert_eq!(outcome.removed, 1);
    assert_eq!(read(&a, &ca, 2), None);
    assert_eq!(
        crate::docs::record_stamp(&a, &ca, &DocId::Int64(2)).unwrap(),
        None,
        "no row, and no tombstone to outrank a write of it a peer holds"
    );
    assert!(read(&a, &ca, 1).is_some(), "a document that did not change is left alone");
}

#[test]
fn a_rewind_puts_back_a_field_document_a_later_delete_removed() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    let target = newest(&a);
    later();
    assert!(a.delete(&ca, &DocId::Int64(1)).unwrap());

    a.rewind_to(target).unwrap();

    assert_eq!(read(&a, &ca, 1), want(doc! { "_id": 1, "x": 1 }));
}

#[test]
fn a_document_deleted_at_the_target_stays_deleted_and_keeps_its_state() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    later();
    assert!(a.delete(&ca, &DocId::Int64(1)).unwrap());
    let target = newest(&a);
    later();
    a.insert(&ca, doc! { "_id": 1, "again": true }).unwrap();

    a.rewind_to(target).unwrap();

    assert_eq!(read(&a, &ca, 1), None, "deleted at the target");
    assert!(
        crate::docs::record_stamp(&a, &ca, &DocId::Int64(1)).unwrap().is_some(),
        "a tombstone that still carries the state, so the delete's context is kept"
    );
}

/// A inserts document 1, B edits it having seen A's creation, and A's own insert is then collected on A: the write
/// at the target that A no longer holds as an entry is covered by B's later state, which carries A's whole state.
#[test]
fn a_collected_write_that_a_retained_entry_covers_is_no_reason_to_refuse() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca = field_collection(&a);
    let first = a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    let inserted = entries(&a).into_iter().find(|e| e.kind == OpKind::Insert).unwrap();
    later();
    b.apply_batch(&entries(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    b.replace(&cb, &first, doc! { "_id": 1, "x": 1, "y": 2 }, false).unwrap();
    later();
    a.apply_batch(&entries(&b)).unwrap();
    a.collect_garbage_at(inserted.stamp.hlc.wall_ms + 10, RetentionPolicy::new(0, u64::MAX))
        .unwrap();
    assert!(
        !a.missing_through(&ca, &first).unwrap().is_empty(),
        "premise: A's insert was collected and is recorded"
    );
    let target = newest(&a);
    later();
    a.replace(&ca, &first, doc! { "_id": 1, "x": 9, "y": 2 }, false).unwrap();

    a.rewind_to(target).unwrap();

    assert_eq!(
        read(&a, &ca, 1),
        want(doc! { "_id": 1, "x": 1, "y": 2 }),
        "restored from B's state, which carries A's insert"
    );
}

#[test]
fn a_document_whose_write_at_the_target_was_collected_is_refused_and_nothing_is_written() {
    let (a, _da) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    let inserted = entries(&a).into_iter().find(|e| e.kind == OpKind::Insert).unwrap();
    later();
    a.insert(&ca, doc! { "_id": 2 }).unwrap();
    a.collect_garbage_at(inserted.stamp.hlc.wall_ms + 10, RetentionPolicy::new(0, u64::MAX))
        .unwrap();
    let target = newest(&a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 9 }, false).unwrap();
    let before = entries(&a);

    let err = a.rewind_to(target).expect_err("the value at the target is not recoverable");

    assert!(err.to_string().contains("not in this member's oplog"), "{err}");
    assert_eq!(read(&a, &ca, 1), want(doc! { "_id": 1, "x": 9 }), "nothing was written");
    assert_eq!(entries(&a), before, "and no entry was discarded");
}

#[test]
fn a_document_held_only_as_a_state_with_writes_above_the_target_is_refused() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca = field_collection(&a);
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let page = a.snapshot_page(None, None).unwrap();
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();

    let err = b.rewind_to(Hlc::new(1, 0)).expect_err("nothing here can undo a state");

    assert!(err.to_string().contains("only as a snapshot page's state"), "{err}");
    assert!(read(&b, &cb, 1).is_some(), "nothing was written");
}
