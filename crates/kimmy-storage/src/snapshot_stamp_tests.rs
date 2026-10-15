//! What a snapshot page stores with its sender's stamp, and what it does with
//! the receiver's own (B4 items 2 and 5).
//!
//! **A restore moves the clock.** A collection's creation and drop tombstone,
//! the floor restored from them, an index's creation and drop: the receiver
//! holds and serves each, so its clock must be past each, as it is for every
//! other stamp it accepts. A receiver whose clock ran behind its sender's minted
//! a write below the floor it had restored, and every peer judged it history.
//! Only a stamp the sender's own coverage names is folded in (ADR-163).
//!
//! **A restore honours the receiver's own index tombstone.** The replicated
//! `CreateIndex` does; the page's loop did not, so a member that dropped an index
//! rebuilt it from a peer that had not heard of the drop.

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, IndexField, NodeId, Stamp};

use crate::{Engine, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn transfer_under(into: &Engine, from: &Engine, mut progress: SnapshotProgress) {
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

fn transfer(into: &Engine, from: &Engine) {
    transfer_under(into, from, SnapshotProgress::whole_database());
}

fn pull(into: &Engine, from: &Engine) {
    let entries = from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn field(path: &str) -> Vec<IndexField> {
    vec![IndexField::ascending(path)]
}

/// An engine whose clock runs an hour ahead of this host's, as a peer's can.
fn engine_an_hour_ahead() -> (Engine, tempfile::TempDir, Hlc) {
    let (a, dir) = engine();
    let ahead =
        Stamp::new(Hlc::new(crate::physical_now_ms() + 3_600_000, 0), NodeId::from_bytes([9; 16]));
    a.witness_processed(&ahead).unwrap();
    (a, dir, ahead.hlc)
}

/// The receiver's clock is behind `ahead` before the page, and not after.
fn assert_clock_moved_past(receiver: &Engine, stamp: Hlc, what: &str) {
    assert!(
        receiver.clock_last() >= stamp,
        "{what}: the receiver's clock {:?} is behind the {stamp:?} it stored",
        receiver.clock_last()
    );
}

fn assert_clock_behind(receiver: &Engine, ahead: Hlc) {
    assert!(receiver.clock_last() < ahead, "premise: the receiver is behind the sender");
}

// -- A restore moves the clock -----------------------------------------------------------

/// `a` runs an hour ahead and drops and recreates `shop.orders` (empty); `b`
/// takes it by snapshot, and so restores the sender's incarnation floor.
fn receiver_behind_a_restored_floor() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da, _) = engine_an_hour_ahead();
    let (b, db) = engine();
    a.create_collection("shop", "orders").unwrap();
    a.drop_collection("shop", "orders").unwrap();
    a.create_collection("shop", "orders").unwrap();
    transfer(&b, &a);
    assert!(b.get_collection("shop", "orders").unwrap().incarnation_floor.is_some(), "premise");
    (a, b, [da, db])
}

#[test]
fn a_receiver_mints_above_the_floor_it_restored() {
    let (_a, b, _dirs) = receiver_behind_a_restored_floor();
    let cb = b.get_collection("shop", "orders").unwrap();
    let floor = cb.incarnation_floor.unwrap();
    let id = b.insert(&cb, doc! { "_id": 1 }).unwrap();
    let (stamp, _) = b.get_stamped(&cb, &id).unwrap().expect("the receiver holds its own write");
    assert!(
        stamp.hlc > floor,
        "minted {stamp:?} below the floor {floor:?} restored from the sender"
    );
}

#[test]
fn the_sender_keeps_the_receivers_acknowledged_write() {
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

#[test]
fn a_restored_creation_moves_the_clock() {
    let (a, _da, ahead) = engine_an_hour_ahead();
    let (b, _db) = engine();
    let created = a.create_collection("shop", "orders").unwrap().created;
    assert_clock_behind(&b, ahead);
    transfer(&b, &a);
    assert_clock_moved_past(&b, created, "the collection's creation");
}

#[test]
fn a_restored_index_creation_moves_the_clock() {
    let (a, _da, _) = engine_an_hour_ahead();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    // The collection first, so the index's stamp is the only thing past it.
    transfer(&b, &a);
    // Local writes on the sender first, so the index's stamp is past anything the
    // receiver's own bookkeeping can tick to.
    let orders = a.get_collection("shop", "orders").unwrap();
    for i in 0..8 {
        a.insert(&orders, doc! { "_id": i }).unwrap();
    }
    a.create_index("shop", "orders", field("x"), false, Some("x".into())).unwrap();
    let created = a.get_collection("shop", "orders").unwrap().index("x").unwrap().created.unwrap();
    // Drop the collection's own stamp from the question: the receiver's clock is
    // wound back to the host's, which is behind the index.
    assert!(b.clock_last() < created.hlc, "premise: {:?} {:?}", b.clock_last(), created.hlc);
    transfer(&b, &a);
    assert_clock_moved_past(&b, created.hlc, "the index's creation");
}

#[test]
fn a_carried_collection_drop_moves_the_clock_though_the_receiver_never_held_it() {
    let (a, _da, ahead) = engine_an_hour_ahead();
    let (b, _db) = engine();
    a.create_collection("shop", "gone").unwrap();
    a.drop_collection("shop", "gone").unwrap();
    let id = CollectionId::derive("shop", "gone");
    let dropped = a.collection_dropped_at(id).unwrap().expect("the sender's tombstone");
    assert_clock_behind(&b, ahead);
    transfer(&b, &a);
    assert!(b.collection_dropped_at(id).unwrap().is_some(), "premise: the tombstone is recorded");
    assert_clock_moved_past(&b, dropped.hlc, "the carried drop");
}

#[test]
fn a_carried_collection_drop_of_a_held_collection_moves_the_clock() {
    let (a, _da, ahead) = engine_an_hour_ahead();
    let (b, _db) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    a.insert(&first, doc! { "_id": 1 }).unwrap();
    // The receiver holds the life that is about to be dropped.
    let entries = a.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    b.apply_batch(&entries).unwrap();
    a.drop_collection("shop", "orders").unwrap();
    let dropped = a.collection_dropped_at(first.id).unwrap().unwrap();
    assert!(b.clock_last() < dropped.hlc, "premise: {:?} {dropped:?} {ahead:?}", b.clock_last());
    transfer(&b, &a);
    assert_clock_moved_past(&b, dropped.hlc, "the carried drop of a held collection");
}

#[test]
fn a_carried_index_drop_moves_the_clock() {
    let (a, _da, _) = engine_an_hour_ahead();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    a.create_index("shop", "orders", field("x"), false, Some("x".into())).unwrap();
    transfer(&b, &a);
    let orders = a.get_collection("shop", "orders").unwrap();
    for i in 0..8 {
        a.insert(&orders, doc! { "_id": i }).unwrap();
    }
    a.drop_index("shop", "orders", "x").unwrap();
    let id = CollectionId::derive("shop", "orders");
    let dropped = a
        .indexes_dropped(Some(id))
        .unwrap()
        .into_iter()
        .map(|(_, _, stamp)| stamp)
        .max()
        .expect("the sender's index tombstone");
    assert!(b.clock_last() < dropped.hlc, "premise");
    transfer(&b, &a);
    assert_clock_moved_past(&b, dropped.hlc, "the carried index drop");
}

#[test]
fn a_scoped_pages_drop_moves_the_clock() {
    let (a, _da, ahead) = engine_an_hour_ahead();
    let (b, _db) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    // The receiver holds it, so the scoped page's `dropped` is applied to it.
    let entries = a.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    b.apply_batch(&entries).unwrap();
    a.drop_collection("shop", "orders").unwrap();
    let dropped = a.collection_dropped_at(first.id).unwrap().unwrap();
    assert!(b.clock_last() < dropped.hlc, "premise: {ahead:?}");
    transfer_under(&b, &a, SnapshotProgress::of_collection(first.id));
    assert_clock_moved_past(&b, dropped.hlc, "the scoped page's drop");
}

#[test]
fn a_scoped_drop_or_a_creation_the_senders_coverage_does_not_name_moves_nothing() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let orders = a.create_collection("shop", "orders").unwrap();
    transfer(&b, &a);
    let far = Hlc::new(crate::physical_now_ms() + 86_400_000, 0);
    let mut page = a.snapshot_page(None, Some(orders.id)).unwrap();
    page.dropped = Some(Stamp::new(far, a.node_id()));
    for state in &mut page.collections {
        state.created = Some(far);
    }
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(orders.id), &page)
        .unwrap();
    assert!(b.clock_last() < far, "the clock followed a time nothing vouched for");
}

#[test]
fn a_stamp_the_senders_coverage_does_not_name_moves_nothing() {
    // ADR-163: the stamp comes off the wire. A drop the page carries that its own
    // `versions` do not name is ignored and must not drag the clock to it.
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.create_collection("shop", "orders").unwrap();
    transfer(&b, &a);
    let far = Stamp::new(Hlc::new(crate::physical_now_ms() + 86_400_000, 0), a.node_id());
    let mut page = a.snapshot_page(None, None).unwrap();
    page.dropped_collections.push((CollectionId::derive("shop", "other"), far));
    page.dropped_indexes.push((CollectionId::derive("shop", "orders"), 7, far));
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(b.clock_last() < far.hlc, "the clock followed a stamp nothing vouched for");
}

/// A local drop is minted above the life it buries, whatever the clock says (ADR-236). A
/// creation a page stored without witnessing it (a scoped page from a relay whose own
/// coverage does not name it, ADR-233's trade) stands above this member's clock, and a
/// drop minted at the clock was judged, on every member holding the life, as aimed at one
/// that ended before it: the acknowledged drop was undone everywhere.
#[test]
fn a_local_drop_is_minted_above_a_creation_the_clock_never_witnessed() {
    let (a, _da, _) = engine_an_hour_ahead();
    let (relay, _dr) = engine();
    let (c, _dc) = engine();
    let orders = a.create_collection("shop", "orders").unwrap();
    transfer_under(&relay, &a, SnapshotProgress::of_collection(orders.id));
    transfer_under(&c, &relay, SnapshotProgress::of_collection(orders.id));
    assert!(c.clock_last() < orders.created, "premise: stored and not witnessed");

    c.drop_collection("shop", "orders").unwrap();
    let dropped = c.collection_dropped_at(orders.id).unwrap().expect("the drop's tombstone");
    assert!(dropped.hlc > orders.created, "the drop {dropped:?} is below the life it buried");
    pull(&a, &c);
    assert!(a.get_collection("shop", "orders").is_err(), "the drop reaches the life it buried");
}

// -- A restore honours the receiver's own index tombstone --------------------------------

/// `a` and `b` hold `shop.orders.x`; then `b` drops it and `a` has not heard.
fn b_dropped_what_a_still_serves(
    unique: bool,
    expire: Option<i64>,
) -> (Engine, Engine, CollectionId, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let orders = a.create_collection("shop", "orders").unwrap();
    a.create_index_with(
        "shop",
        "orders",
        field("x"),
        unique,
        kimmy_core::Enforcement::Local,
        Some("x".into()),
        expire,
        None,
    )
    .unwrap();
    pull(&b, &a);
    assert!(b.get_collection("shop", "orders").unwrap().index("x").is_some(), "premise");
    later();
    assert!(b.drop_index("shop", "orders", "x").unwrap());
    (a, b, orders.id, [da, db])
}

fn holds_x(e: &Engine) -> bool {
    e.get_collection("shop", "orders").unwrap().index("x").is_some()
}

#[test]
fn a_whole_database_page_does_not_rebuild_an_index_the_receiver_dropped() {
    let (a, b, _id, _dirs) = b_dropped_what_a_still_serves(false, None);
    transfer(&b, &a);
    assert!(!holds_x(&b), "the receiver rebuilt an index it had dropped");
}

#[test]
fn a_scoped_page_does_not_rebuild_an_index_the_receiver_dropped() {
    let (a, b, id, _dirs) = b_dropped_what_a_still_serves(false, None);
    transfer_under(&b, &a, SnapshotProgress::of_collection(id));
    assert!(!holds_x(&b), "the receiver rebuilt an index it had dropped");
}

#[test]
fn a_unique_index_the_receiver_dropped_is_not_rebuilt_and_enforces_nothing() {
    let (a, b, id, _dirs) = b_dropped_what_a_still_serves(true, None);
    transfer_under(&b, &a, SnapshotProgress::of_collection(id));
    assert!(!holds_x(&b));
    let cb = b.get_collection("shop", "orders").unwrap();
    b.insert(&cb, doc! { "_id": 1, "x": 5 }).unwrap();
    b.insert(&cb, doc! { "_id": 2, "x": 5 }).expect("no unique index here to refuse the second");
}

#[test]
fn a_ttl_index_the_receiver_dropped_is_not_rebuilt() {
    let (a, b, id, _dirs) = b_dropped_what_a_still_serves(false, Some(60));
    transfer_under(&b, &a, SnapshotProgress::of_collection(id));
    assert!(
        !holds_x(&b),
        "the receiver rebuilt a TTL index it had dropped and would expire with it"
    );
}

#[test]
fn a_definition_made_after_the_receivers_drop_is_still_restored() {
    let (a, b, id, _dirs) = b_dropped_what_a_still_serves(false, None);
    // `a` hears of the drop, then makes the index again: a later definition.
    pull(&a, &b);
    assert!(!holds_x(&a), "premise: the drop reached the sender");
    later();
    a.create_index("shop", "orders", field("x"), false, Some("x".into())).unwrap();
    transfer_under(&b, &a, SnapshotProgress::of_collection(id));
    assert!(holds_x(&b), "a definition newer than the tombstone is not history");
}

#[test]
fn a_definition_stamped_exactly_at_the_tombstone_is_restored_as_the_entries_route_does() {
    // The replicated route judges `stamp < dropped_at`; so does the page.
    let (a, _da) = engine();
    let (b, _db) = engine();
    let orders = a.create_collection("shop", "orders").unwrap();
    a.create_index("shop", "orders", field("x"), false, Some("x".into())).unwrap();
    let created = a.get_collection("shop", "orders").unwrap().index("x").unwrap().created.unwrap();
    // The receiver holds the collection and a tombstone for `x` at that very stamp.
    let creation: Vec<_> = a
        .entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.kind == kimmy_core::OpKind::CreateCollection)
        .collect();
    b.apply_batch(&creation).unwrap();
    b.record_index_drop(orders.id, kimmy_core::IndexMeta::derive_id("x"), created).unwrap();
    assert!(!holds_x(&b), "premise");
    transfer_under(&b, &a, SnapshotProgress::of_collection(orders.id));
    assert!(holds_x(&b));
}

// -- An index with no creation stamp ------------------------------------------------------

#[test]
fn an_unstamped_definition_is_not_rebuilt_over_the_receivers_tombstone() {
    // A sender that predates the stamp. Building it over the tombstone is the
    // defect; a real recreation arrives by the entries route with a stamp.
    let (a, b, id, _dirs) = b_dropped_what_a_still_serves(false, None);
    let mut page = a.snapshot_page(None, Some(id)).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            index.created = None;
        }
    }
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(id), &page).unwrap();
    assert!(!holds_x(&b), "an unstamped definition was rebuilt over a tombstone");
}

#[test]
fn an_unstamped_definition_with_no_tombstone_is_restored_as_before() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let orders = a.create_collection("shop", "orders").unwrap();
    a.create_index("shop", "orders", field("x"), false, Some("x".into())).unwrap();
    let creation: Vec<_> = a
        .entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.kind == kimmy_core::OpKind::CreateCollection)
        .collect();
    b.apply_batch(&creation).unwrap();
    let mut page = a.snapshot_page(None, Some(orders.id)).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            index.created = None;
        }
    }
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(orders.id), &page)
        .unwrap();
    assert!(holds_x(&b));
}

// -- A restart after a restore ------------------------------------------------------------

/// The same store, closed and opened again: what a restart is.
fn restarted(e: Engine, dir: &tempfile::TempDir) -> Engine {
    drop(e);
    Engine::open(&dir.path().join("kimmy.redb")).unwrap()
}

#[test]
fn the_clock_resumes_past_a_collection_tombstone_a_scoped_restore_recorded() {
    // A scoped snapshot grants no coverage, so the vectors that would name the
    // sender's drop stamp are not raised; the tombstone has no oplog entry here.
    // Only the metadata at open can carry the clock past it.
    let (a, _da, _) = engine_an_hour_ahead();
    let (b, db) = engine();
    let first = a.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    a.drop_collection("shop", "orders").unwrap();
    let dropped = a.collection_dropped_at(first.id).unwrap().unwrap();
    transfer_under(&b, &a, SnapshotProgress::of_collection(first.id));
    assert!(b.collection_dropped_at(first.id).unwrap().is_some(), "premise");
    let b = restarted(b, &db);
    assert_clock_moved_past(&b, dropped.hlc, "after a restart, the scoped restore's drop");
}

#[test]
fn the_clock_resumes_past_an_index_tombstone_a_scoped_restore_recorded() {
    let (a, _da, _) = engine_an_hour_ahead();
    let (b, db) = engine();
    let orders = a.create_collection("shop", "orders").unwrap();
    a.create_index("shop", "orders", field("x"), false, Some("x".into())).unwrap();
    pull(&b, &a);
    let held = a.get_collection("shop", "orders").unwrap().index("x").unwrap().created.unwrap();
    for i in 0..8 {
        a.insert(&orders, doc! { "_id": i }).unwrap();
    }
    a.drop_index("shop", "orders", "x").unwrap();
    let dropped = a
        .indexes_dropped(Some(orders.id))
        .unwrap()
        .into_iter()
        .map(|(_, _, stamp)| stamp)
        .max()
        .unwrap();
    assert!(dropped.hlc > held.hlc);
    transfer_under(&b, &a, SnapshotProgress::of_collection(orders.id));
    let b = restarted(b, &db);
    assert_clock_moved_past(&b, dropped.hlc, "after a restart, the scoped restore's index drop");
}
