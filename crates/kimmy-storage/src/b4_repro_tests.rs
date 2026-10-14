//! B4 reproductions (the remaining snapshot edge cases), written red first on
//! main `feb275e2` (0.49.0), engine level, no fixes. Each test asserts the
//! **correct** end state, so a test that fails here is a defect that
//! reproduces and the same test going green is the acceptance criterion of its
//! fix. Nothing in this file is meant to be merged red: a fix PR takes its
//! tests from here.

use bson::{Bson, doc};
use kimmy_core::{DocId, Hlc, IndexField, NodeId, OpKind, Stamp};

use crate::{Engine, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

/// A whole-database snapshot from `from` into `into`, page by page, as the
/// transport applies it.
fn transfer(into: &Engine, from: &Engine) {
    let mut progress = SnapshotProgress::whole_database();
    let mut waited = false;
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        let outcome = into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
        if outcome.purge_pending.is_some() {
            assert!(!waited, "a purge that never finishes");
            waited = true;
            into.finish_purges_now().unwrap();
            continue;
        }
        waited = false;
    }
}

/// Everything `from` has to serve, applied to `into` as one batch.
fn pull(into: &Engine, from: &Engine) -> crate::SyncOutcome {
    let entries = from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap()
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

// -- (1) A differing collection incarnation is not reconciled, on either path ------------

/// The sender dropped and recreated `shop.orders`; the receiver holds the life
/// before. Returns `(sender, receiver, the sender's new life)` with the sender's
/// drop tombstone gone, as it is once `tombstone_retention_secs` has passed.
fn receiver_holds_the_previous_life()
-> (Engine, Engine, kimmy_core::CollectionId, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    a.insert(&first, doc! { "_id": 1 }).unwrap();
    transfer(&b, &a);
    assert!(
        b.get(&b.get_collection("shop", "orders").unwrap(), &DocId::Int64(1)).unwrap().is_some()
    );
    later();
    a.drop_collection("shop", "orders").unwrap();
    a.finish_purges_now().unwrap();
    later();
    let second = a.create_collection("shop", "orders").unwrap();
    a.insert(&second, doc! { "_id": 2 }).unwrap();
    assert_ne!(first.created, second.created, "premise: two lives");
    (a, b, second.id, [da, db])
}

fn tombstone_aged_out(sender: &Engine, id: kimmy_core::CollectionId) {
    let txn = sender.begin_write(crate::WriterHolder::Ddl).unwrap();
    txn.open_table(crate::tables::COLLECTIONS_DROPPED).unwrap().remove(id.0).unwrap();
    txn.commit().unwrap();
    assert!(sender.collection_dropped_at(id).unwrap().is_none(), "premise: the headstone is gone");
}

#[test]
fn b4_1a_a_snapshot_without_the_tombstone_still_replaces_the_previous_life() {
    let (a, b, id, _dirs) = receiver_holds_the_previous_life();
    tombstone_aged_out(&a, id);

    transfer(&b, &a);

    let held = b.get_collection("shop", "orders").unwrap();
    let want = a.get_collection("shop", "orders").unwrap();
    assert_eq!(held.created, want.created, "the receiver keeps the life that ended");
    assert!(
        b.get(&held, &DocId::Int64(1)).unwrap().is_none(),
        "one collection made of two lives: the previous life's document is still here"
    );
    assert!(b.get(&held, &DocId::Int64(2)).unwrap().is_some());
}

#[test]
fn b4_1b_the_entries_path_does_not_replace_the_previous_life_either() {
    // The same decision written a second time. The drop's own entry is withheld,
    // as it is when retention has collected it, so the create is the only thing
    // that says the life changed.
    let (a, b, id, _dirs) = receiver_holds_the_previous_life();
    let entries = a.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    let newest_create = entries
        .iter()
        .filter(|e| e.kind == OpKind::CreateCollection && e.collection == id)
        .max_by_key(|e| e.stamp)
        .expect("the sender's second creation")
        .clone();
    b.apply_batch(std::slice::from_ref(&newest_create)).unwrap();

    let held = b.get_collection("shop", "orders").unwrap();
    let want = a.get_collection("shop", "orders").unwrap();
    assert_eq!(held.created, want.created, "the receiver keeps the life that ended");
}

#[test]
fn b4_1c_two_members_that_create_one_name_without_a_drop_agree_on_when() {
    // The case with no tombstone to carry (the page's case 3). Not a claim that
    // a winner exists to be chosen: only that, once everything is exchanged, the
    // members do not keep a different `created` each for good.
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    later();
    b.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    pull(&a, &b);
    transfer(&b, &a);
    transfer(&a, &b);
    assert_eq!(
        a.get_collection("shop", "orders").unwrap().created,
        b.get_collection("shop", "orders").unwrap().created,
        "each member keeps its own creation"
    );
}

// -- (2) A snapshot restores a floor and does not move the receiver's clock --------------

/// `a` runs an hour ahead and drops and recreates `shop.orders` (empty); `b`
/// takes it by snapshot, and so restores the sender's incarnation floor.
fn receiver_behind_a_restored_floor() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let ahead =
        Stamp::new(Hlc::new(crate::physical_now_ms() + 3_600_000, 0), NodeId::from_bytes([9; 16]));
    a.witness_processed(&ahead).unwrap();
    a.create_collection("shop", "orders").unwrap();
    a.drop_collection("shop", "orders").unwrap();
    a.create_collection("shop", "orders").unwrap();
    transfer(&b, &a);
    assert!(b.get_collection("shop", "orders").unwrap().incarnation_floor.is_some(), "premise");
    (a, b, [da, db])
}

#[test]
fn b4_2a_a_receiver_mints_above_the_floor_it_restored() {
    let (_a, b, _dirs) = receiver_behind_a_restored_floor();
    let cb = b.get_collection("shop", "orders").unwrap();
    let floor = cb.incarnation_floor.unwrap();
    let id = b.insert(&cb, doc! { "_id": 1 }).unwrap();
    let (stamp, _) = b.get_stamped(&cb, &id).unwrap().expect("the receiver holds its own write");
    assert!(
        stamp.hlc > floor,
        "the receiver minted {stamp:?} below the floor {floor:?} it restored from the sender"
    );
}

#[test]
fn b4_2b_the_sender_keeps_the_receivers_acknowledged_write() {
    let (a, b, _dirs) = receiver_behind_a_restored_floor();
    let cb = b.get_collection("shop", "orders").unwrap();
    b.insert(&cb, doc! { "_id": 1 }).unwrap();

    pull(&a, &b);

    let ca = a.get_collection("shop", "orders").unwrap();
    assert!(
        a.get(&ca, &DocId::Int64(1)).unwrap().is_some(),
        "the sender judged the receiver's acknowledged write history and dropped it"
    );
}

// -- (3) An index dedups a Decimal128 against a number -----------------------------------

fn decimal(text: &str) -> Bson {
    Bson::Decimal128(text.parse().unwrap())
}

/// The index over `z.b` and the keys one document contributes to it.
fn zb_keys(document: bson::Document) -> crate::index::DocumentKeys {
    let (e, _d) = engine();
    e.create_collection("shop", "orders").unwrap();
    e.create_index("shop", "orders", vec![IndexField::ascending("z.b")], false, Some("zb".into()))
        .unwrap();
    let meta = e.get_collection("shop", "orders").unwrap();
    crate::index::document_keys(meta.index("zb").unwrap(), &document).unwrap()
}

#[test]
fn b4_3a_a_lone_decimal_at_a_dotted_path_is_unkeyed() {
    // The shape that already works: pinned so a fix keeps it.
    let keys = zb_keys(doc! { "_id": 1, "z": [{ "b": decimal("1") }] });
    assert!(matches!(keys, crate::index::DocumentKeys::Unkeyed { .. }), "{keys:?}");
}

#[test]
fn b4_3b_a_number_then_a_decimal_at_a_dotted_path_is_unkeyed() {
    let keys = zb_keys(doc! { "_id": 2, "z": [{ "b": 1 }, { "b": decimal("1") }] });
    assert!(matches!(keys, crate::index::DocumentKeys::Unkeyed { .. }), "filed keyed: {keys:?}");
}

#[test]
fn b4_3c_a_decimal_then_a_number_at_a_dotted_path_is_unkeyed() {
    let keys = zb_keys(doc! { "_id": 3, "z": [{ "b": decimal("2") }, { "b": 1 }] });
    assert!(matches!(keys, crate::index::DocumentKeys::Unkeyed { .. }), "filed keyed: {keys:?}");
}

#[test]
fn b4_3d_the_index_reports_what_it_holds_unkeyed() {
    // The operator's view: `unkeyed` on the index listing.
    let (e, _d) = engine();
    let coll = e.create_collection("shop", "orders").unwrap();
    let idx = e
        .create_index(
            "shop",
            "orders",
            vec![IndexField::ascending("z.b")],
            false,
            Some("zb".into()),
        )
        .unwrap();
    e.insert(&coll, doc! { "_id": 1, "z": [{ "b": decimal("1") }] }).unwrap();
    e.insert(&coll, doc! { "_id": 2, "z": [{ "b": 1 }, { "b": decimal("1") }] }).unwrap();
    e.insert(&coll, doc! { "_id": 3, "z": [{ "b": decimal("2") }, { "b": 1 }] }).unwrap();
    let coll = e.get_collection("shop", "orders").unwrap();
    assert_eq!(e.unkeyed_count(&coll, idx.id).unwrap(), 3);
}

#[test]
fn b4_3e_the_same_at_a_plain_array_path_stays_unkeyed() {
    // The shape the page says is right only by accident (the whole-array key
    // is pushed and fails to encode): pinned so a fix does not break it.
    let (e, _d) = engine();
    let coll = e.create_collection("shop", "orders").unwrap();
    let idx = e
        .create_index("shop", "orders", vec![IndexField::ascending("a")], false, Some("a".into()))
        .unwrap();
    e.insert(&coll, doc! { "_id": 1, "a": [1, decimal("1")] }).unwrap();
    let coll = e.get_collection("shop", "orders").unwrap();
    assert_eq!(e.unkeyed_count(&coll, idx.id).unwrap(), 1);
}

// -- (4) Two origins change one client-supplied vector configuration concurrently -------

fn byo(dim: usize) -> kimmy_core::VectorConfig {
    kimmy_core::VectorConfig {
        fields: vec!["body".into()],
        provider: kimmy_core::ProviderConfig::Byo {},
        dim,
        metric: kimmy_core::Metric::Cosine,
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

/// `a` and `b` both hold width 8. Then `a` turns it off (stamp `s_off`), `b`
/// configures width 8 again (`s_8 > s_off`), and `a` configures width 16
/// (`s_16 > s_8`): one origin's width-8 at a stamp the receiver holds, the other's
/// off below it and a width above it.
fn concurrent_width_change() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    a.create_collection("shop", "orders").unwrap();
    a.configure_vectors("shop", "orders", byo(8)).unwrap();
    let (b, db) = engine();
    pull(&b, &a);
    assert!(b.get_collection("shop", "orders").unwrap().vector.is_some(), "premise");
    later();
    assert!(a.disable_vectors("shop", "orders", false).unwrap());
    later();
    b.configure_vectors("shop", "orders", byo(8)).unwrap();
    later();
    a.configure_vectors("shop", "orders", byo(16)).unwrap();
    let (off, held, wide) = (
        a.get_collection("shop", "orders").unwrap().vector_changed.unwrap(),
        b.get_collection("shop", "orders").unwrap().vector_changed.unwrap(),
        a.get_collection("shop", "orders").unwrap().vector_changed.unwrap(),
    );
    assert!(
        held < wide && off != held,
        "premise: the receiver's stamp sits between the two of `a`"
    );
    (a, b, [da, db])
}

#[test]
fn b4_4_the_entries_path_and_a_page_end_in_the_same_width() {
    // The page: a winner by stamp, applied.
    let (a, via_page, _dirs) = concurrent_width_change();
    transfer(&via_page, &a);
    let page_width = via_page.get_collection("shop", "orders").unwrap().vector.map(|v| v.dim);

    // The entries: the off loses on its stamp, then the width meets the held one.
    let (a, via_entries, _dirs) = concurrent_width_change();
    let outcome = pull(&via_entries, &a);
    let entries_width = via_entries.get_collection("shop", "orders").unwrap().vector.map(|v| v.dim);

    assert_eq!(
        entries_width, page_width,
        "the same history, two end states: entries {entries_width:?} (refused {}) against page {page_width:?}",
        outcome.ddl_refused
    );
}
