//! A member appends an entry of its own, once, for each loss record that enters its state, by whatever route (V6M2),
//! behind the emission window, in the transaction that stores the row.

use bson::doc;
use kimmy_core::merge_state::{
    LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, WriterId,
};
use kimmy_core::{DocId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp};

use crate::{CollectionMeta, Engine, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn orders() -> kimmy_core::CollectionId {
    kimmy_core::CollectionId::derive("shop", "orders")
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(20));
}

/// The records `e` appended an entry of its own for, by key.
fn own_records(e: &Engine) -> Vec<LossKey> {
    everything(e)
        .into_iter()
        .filter(|entry| entry.kind == OpKind::LossRecord && entry.stamp.node == e.node_id())
        .map(|entry| MergeState::decode_loss_record(entry.body.as_deref().unwrap()).unwrap().0)
        .collect()
}

fn stored_state(from: &Engine) -> MergeState {
    let page = from.snapshot_page(None, Some(orders())).unwrap();
    crate::field_write::load(page.documents[0].meta.as_deref()).unwrap()
}

/// Two members that wrote the one field concurrently: `a` has taken `b`'s write and so detected the clash; `b` has not.
fn a_clash() -> (Engine, Engine, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let id = a.insert(&ca, doc! { "_id": 1, "x": 0 }).unwrap();
    later();
    b.apply_batch(&everything(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    a.replace(&ca, &id, doc! { "_id": 1, "x": 1 }, false).unwrap();
    b.replace(&cb, &id, doc! { "_id": 1, "x": 2 }, false).unwrap();
    later();
    a.apply_batch(&everything(&b)).unwrap();
    (a, b, ca, [da, db])
}

#[test]
fn a_member_that_detects_a_clash_appends_one_entry_of_its_own_for_it() {
    let (a, b, _ca, _d) = a_clash();

    let own = own_records(&a);

    assert_eq!(own.len(), 1, "one entry of its own, for the one record it detected: {own:?}");
    assert_eq!(own[0].kind, LossKind::Clash);
    assert_eq!(
        stored_state(&a).losses.keys().collect::<Vec<_>>(),
        vec![&own[0]],
        "and the record is in its state"
    );
    assert!(own_records(&b).is_empty(), "b has detected nothing yet");
}

#[test]
fn a_record_that_reaches_a_member_by_another_route_is_not_given_a_second_entry() {
    let (a, b, _ca, _d) = a_clash();

    // b takes a's whole window: a's write (b detects the clash by the join) and a's own loss entry (the same record).
    b.apply_batch(&everything(&a)).unwrap();
    assert_eq!(
        own_records(&b).len(),
        1,
        "b has one entry of its own: its join detected it, and the received entry is known"
    );
    // And again, every entry once more, in both directions.
    b.apply_batch(&everything(&a)).unwrap();
    a.apply_batch(&everything(&b)).unwrap();
    a.apply_batch(&everything(&b)).unwrap();

    assert_eq!(own_records(&b).len(), 1, "a repeat is a no-op");
    assert_eq!(own_records(&a).len(), 1, "for the member that detected it too");
}

#[test]
fn a_record_that_arrives_in_a_loss_entry_is_given_an_entry_of_its_own() {
    // The record enters by `receive_loss`: the document has a row and its state lacks the record.
    let (a, e, ca, _d) = pair_with_a_row();
    let entry = a_loss_entry(&a, &ca, None);

    e.apply_batch(std::slice::from_ref(&entry)).unwrap();

    assert_eq!(own_records(&e).len(), 1, "an entry of e's own, besides the one it kept");
    assert_eq!(
        everything(&e).iter().filter(|x| x.kind == OpKind::LossRecord).count(),
        2,
        "the one it received and the one it appended"
    );
}

#[test]
fn a_record_that_a_local_write_folds_in_is_given_an_entry_of_its_own() {
    let (a, b, ca, _d) = pair();
    let entry = a_loss_entry(&a, &ca, None);
    b.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert!(own_records(&b).is_empty(), "premise: parked, so not in a state yet");

    b.insert(&b.get_collection("shop", "orders").unwrap(), doc! { "_id": 1, "x": 5 }).unwrap();

    assert_eq!(
        own_records(&b).len(),
        1,
        "the fold put it into the state, and an entry of b's own followed"
    );
}

#[test]
fn a_record_that_a_snapshot_page_carries_is_given_an_entry_of_its_own() {
    let (a, _b, _ca, _d) = a_clash();
    let (d, _dd) = engine();
    let page = a.snapshot_page(None, None).unwrap();

    d.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

    assert_eq!(
        own_records(&d).len(),
        1,
        "the page document is state, and the loss entry is the member's own"
    );
}

#[test]
fn a_record_that_reaches_a_member_inside_a_snapshot_carried_tombstone_is_given_an_entry_of_its_own()
{
    // The document is deleted after the clash: what the page carries for it is a tombstone, whose state keeps the
    // record, and the receiving member holds the document nowhere else. The tombstone keeps the `_id` (C4c), so
    // the member can name the document in an entry of its own.
    let (a, _b, ca, _d) = a_clash();
    a.delete(&ca, &DocId::Int64(1)).unwrap();
    let page = a.snapshot_page(None, None).unwrap();
    assert!(
        page.deleted_documents.iter().any(|t| t.id == Some(DocId::Int64(1))),
        "premise: the page carries the tombstone, with its id: {:?}",
        page.deleted_documents
    );
    assert!(page.documents.is_empty(), "premise: and no live document");
    let (d, _dd) = engine();

    d.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

    let own = own_records(&d);
    assert_eq!(
        own.len(),
        1,
        "the record entered d inside a tombstone and got an entry of d's own: {own:?}"
    );
    assert_eq!(
        everything(&d).iter().find(|x| x.kind == OpKind::LossRecord).and_then(|x| x.doc_id.clone()),
        Some(DocId::Int64(1)),
        "which names the document"
    );
}

#[test]
fn a_record_outside_the_emission_window_is_in_the_state_and_gets_no_entry() {
    let (a, da) = engine();
    let (e, de) = engine();
    let _keep = (da, de);
    // A window of ten milliseconds, fixed at the collection's creation.
    a.set_default_join_retention(std::time::Duration::from_millis(10));
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    e.apply_batch(&everything(&a)).unwrap();
    let loser_wall = stored_state(&e).lives.keys().next().unwrap().hlc.wall_ms;
    std::thread::sleep(std::time::Duration::from_millis(100));
    let entry = a_loss_entry(&a, &ca, Some(loser_wall));

    e.apply_batch(std::slice::from_ref(&entry)).unwrap();

    assert_eq!(
        stored_state(&e).losses.len(),
        1,
        "the record entered the state: the state's horizon does not know the clock"
    );
    assert!(
        own_records(&e).is_empty(),
        "but it is a hundred milliseconds old against a window of ten: no entry"
    );
}

#[test]
fn the_emission_window_closes_r_after_the_losers_own_wall_time() {
    use crate::docs::within_the_emission_window as inside;
    assert!(inside(Hlc::new(1_000, 0), 500, 1_000), "now at the loser");
    assert!(inside(Hlc::new(1_000, 0), 500, 1_500), "now at the last moment");
    assert!(!inside(Hlc::new(1_000, 0), 500, 1_501), "one past");
}

// ---- fixtures --------------------------------------------------------------------------------------------

fn pair() -> (Engine, Engine, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    (a, b, ca, [da, db])
}

fn pair_with_a_row() -> (Engine, Engine, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, e, ca, dirs) = pair();
    a.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    e.apply_batch(&everything(&a)).unwrap();
    (a, e, ca, dirs)
}

/// The loss entry `a` would append for a record about the document `1`, whose loser was stamped at `loser_wall` (now).
fn a_loss_entry(a: &Engine, coll: &CollectionMeta, loser_wall: Option<u64>) -> OplogEntry {
    let writer = WriterId { node: a.node_id(), epoch: a.epoch() };
    let loser =
        MStamp { hlc: Hlc::new(loser_wall.unwrap_or_else(crate::physical_now_ms), 0), writer };
    let key = LossKey::field(LossKind::Clash, Path::flat("x").unwrap(), loser);
    OplogEntry {
        stamp: Stamp::new(Hlc::new(crate::physical_now_ms() + 1, 0), a.node_id(), a.epoch()),
        kind: OpKind::LossRecord,
        collection: coll.id,
        doc_id: Some(DocId::Int64(1)),
        body: Some(MergeState::encode_loss_record(&key, &LossValue::Value(Value::Int64(7)))),
        meta: None,
    }
}
