//! Step 3, item (3.4a): a state of a `field` document is cumulative, so one stamped above a collection's
//! incarnation floor can still carry the cells of the life before the drop. They are history whatever
//! the stamp of the state says: only the cells stamped above the floor are taken, on every route a
//! state arrives by, and a state with nothing left is superseded and leaves no hole.

use bson::doc;
use kimmy_core::{DocId, Hlc, MergePolicy, OplogEntry};

use crate::{Engine, SnapshotProgress};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap().entries
}

/// A holds X = {a: 1} in a field collection; B takes it. A then drops the collection and creates it
/// again, so A stands over a floor and holds nothing of X; B, which has not heard of the drop, writes
/// X.b above the floor. `write` is B's entry of that write: stamped above the floor, and carrying in
/// its state the cell of X.a, which is below it.
struct Dropped {
    a: Engine,
    b: Engine,
    x: DocId,
    floor: Hlc,
    write: OplogEntry,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

fn dropped() -> Dropped {
    let (a, da) = engine();
    let (b, db) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let x = a.insert(&ca, doc! { "_id": 1, "a": 1 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    assert!(b.get(&cb, &x).unwrap().is_some(), "premise: B holds X");
    later();
    assert!(a.drop_collection("shop", "orders").unwrap());
    a.finish_purges_now().unwrap();
    later();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let floor = ca.incarnation_floor.expect("premise: the collection stands over a floor");
    assert!(a.get(&ca, &x).unwrap().is_none(), "premise: A holds nothing of X");
    later();
    b.replace(&cb, &x, doc! { "_id": 1, "a": 1, "b": 2 }, false).unwrap();
    let write = everything(&b).last().unwrap().clone();
    assert!(write.stamp.hlc > floor, "premise: B's write is stamped above the floor");
    let state = crate::field_write::load(write.meta.as_deref()).unwrap();
    assert!(
        state.regs.values().any(|r| r.stamp.hlc <= floor),
        "premise: the state of the write carries a cell below the floor"
    );
    Dropped { a, b, x, floor, write, _dirs: (da, db) }
}

const MESSAGE: &str =
    "the cells of the life before the drop do not come back with a write after it";

#[test]
fn an_entry_stamped_above_the_floor_brings_none_of_the_cells_below_it() {
    let d = dropped();
    d.a.apply_batch(std::slice::from_ref(&d.write)).unwrap();
    let ca = d.a.get_collection("shop", "orders").unwrap();
    assert_eq!(d.a.get(&ca, &d.x).unwrap(), Some(doc! { "_id": 1, "b": 2 }), "{MESSAGE}");
}

#[test]
fn a_page_document_stamped_above_the_floor_brings_none_of_the_cells_below_it() {
    let d = dropped();
    let ca = d.a.get_collection("shop", "orders").unwrap();
    let mut page = d.b.snapshot_page(None, Some(ca.id)).unwrap();
    assert!(!page.documents.is_empty(), "premise: the page carries X");
    // B's definition is of the life the drop ended; A's own stands, and is what the page lands in.
    page.collections.clear();
    let outcome =
        d.a.apply_snapshot_page(d.b.node_id(), &mut SnapshotProgress::of_collection(ca.id), &page)
            .unwrap();
    assert_eq!(outcome.applied, 1, "{outcome:?}");
    assert_eq!(d.a.get(&ca, &d.x).unwrap(), Some(doc! { "_id": 1, "b": 2 }), "{MESSAGE}");
}

#[test]
fn a_state_with_nothing_above_the_floor_is_superseded_and_leaves_no_hole() {
    let d = dropped();
    let ca = d.a.get_collection("shop", "orders").unwrap();
    // B's first entry for X is the life before the drop whole: at or below the floor. Held as a page
    // document is, it reaches the one place every state passes, past the pre-checks by stamp.
    let old = everything(&d.b)
        .into_iter()
        .find(|e| e.kind.is_document() && e.stamp.hlc <= d.floor)
        .expect("premise: B holds an entry of the life before the drop");
    let held_before = d.a.state_held().unwrap();
    d.a.apply_held_for_test(&ca, &old).unwrap();
    assert_eq!(d.a.get(&ca, &d.x).unwrap(), None, "{MESSAGE}");
    assert_eq!(
        d.a.state_held().unwrap(),
        held_before,
        "a state that is all history leaves no hole"
    );
}

/// The hole row is the observable of the early return: a state held as state records what it holds of every
/// other writer as a hole in what this member can serve. A state that is all history holds nothing of the
/// life now, so it records none. The state must come from a writer other than the applying member, whose own
/// writes are never holes.
#[test]
fn a_state_of_another_writer_that_is_all_history_records_no_hole() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let x = a.insert(&ca, doc! { "_id": 1, "a": 1 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    later();
    b.replace(&cb, &x, doc! { "_id": 1, "a": 1, "c": 3 }, false).unwrap();
    let old = everything(&b).last().unwrap().clone();
    assert_eq!(old.stamp.node, b.node_id(), "premise: the entry is B's own write");
    later();
    assert!(a.drop_collection("shop", "orders").unwrap());
    a.finish_purges_now().unwrap();
    later();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let floor = ca.incarnation_floor.expect("premise: the collection stands over a floor");
    assert!(old.stamp.hlc <= floor, "premise: B's write is at or below the floor");
    let held_before = a.state_held().unwrap();
    a.apply_held_for_test(&ca, &old).unwrap();
    assert_eq!(a.get(&ca, &x).unwrap(), None, "{MESSAGE}");
    assert_eq!(
        a.state_held().unwrap(),
        held_before,
        "a state that is all history, of another writer, leaves no hole"
    );
}

/// A member that stands in a life the drop reached, while the sweep it owes is not done: the collection has
/// no floor yet, only the drop's tombstone. A holds X = {a}; B takes it; M holds the collection and nothing
/// of X, and holds a tombstone for the drop at X's stamp (the tie goes to the drop) under `id`; B writes
/// X.b above it. B's state carries the cell of X.a, which is below the drop.
fn under_a_tombstone(
    tombstone_id: impl Fn(kimmy_core::CollectionId) -> kimmy_core::CollectionId,
) -> bool {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (m, _dm) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let creation = everything(&a);
    let x = a.insert(&ca, doc! { "_id": 1, "a": 1 }).unwrap();
    let insert = everything(&a).into_iter().find(|e| e.kind == kimmy_core::OpKind::Insert).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    m.apply_batch(&creation).unwrap();
    let cm = m.get_collection("shop", "orders").unwrap();
    assert!(cm.incarnation_floor.is_none(), "premise: M's collection has no floor yet");
    let dropped = kimmy_core::Stamp::new(insert.stamp.hlc, a.node_id(), a.epoch());
    m.record_collection_drop(tombstone_id(cm.id), dropped).unwrap();
    later();
    let cb = b.get_collection("shop", "orders").unwrap();
    b.replace(&cb, &x, doc! { "_id": 1, "a": 1, "b": 2 }, false).unwrap();
    let write = everything(&b).last().unwrap().clone();
    assert!(write.stamp.hlc > dropped.hlc, "premise: B's write is stamped above the drop");
    m.apply_batch(std::slice::from_ref(&write)).unwrap();
    m.get(&cm, &x).unwrap() == Some(doc! { "_id": 1, "b": 2 })
}

#[test]
fn a_state_arriving_under_the_collections_drop_tombstone_before_the_floor_is_set_is_held_to_it() {
    assert!(under_a_tombstone(|id| id), "{MESSAGE}: the tombstone of the collection");
}

#[test]
fn a_state_arriving_under_the_databases_drop_tombstone_before_the_floor_is_set_is_held_to_it() {
    assert!(
        under_a_tombstone(|_| kimmy_core::database_drop_id("shop")),
        "{MESSAGE}: the tombstone of the database"
    );
}

/// What thinning the held state drops is written back even where the join moves nothing. M holds X = {a, b}
/// whole, then learns the drop (a tombstone at X's insert, with no floor yet), and the entry that brought
/// b arrives again: all it holds is in M's state already, so the join moves nothing, but the cell of X.a is
/// of the life before the drop and goes from what M holds.
#[test]
fn the_held_state_of_a_document_loses_the_life_before_the_drop_though_the_join_moves_nothing() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (m, _dm) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let x = a.insert(&ca, doc! { "_id": 1, "a": 1 }).unwrap();
    let insert = everything(&a).into_iter().find(|e| e.kind == kimmy_core::OpKind::Insert).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    later();
    let cb = b.get_collection("shop", "orders").unwrap();
    b.replace(&cb, &x, doc! { "_id": 1, "a": 1, "b": 2 }, false).unwrap();
    let write = everything(&b).last().unwrap().clone();
    m.apply_batch(&everything(&b)).unwrap();
    let cm = m.get_collection("shop", "orders").unwrap();
    assert_eq!(
        m.get(&cm, &x).unwrap(),
        Some(doc! { "_id": 1, "a": 1, "b": 2 }),
        "premise: M holds X whole"
    );
    let dropped = kimmy_core::Stamp::new(insert.stamp.hlc, a.node_id(), a.epoch());
    m.record_collection_drop(cm.id, dropped).unwrap();
    assert!(write.stamp.hlc > dropped.hlc, "premise: the write that brought b is after the drop");
    m.apply_held_for_test(&cm, &write).unwrap();
    assert_eq!(
        m.get(&cm, &x).unwrap(),
        Some(doc! { "_id": 1, "b": 2 }),
        "{MESSAGE}: what thinning the held state drops is written back"
    );
}

/// A and C insert the same document concurrently, each in a life of its own, and the older of the two is held
/// by M. D saw only the other and writes m after the drop. M knows the drop, which stands at both inserts, as a
/// tombstone. Held to the floor on both sides, the life M holds does not compete, and D's write is not lost to it.
#[test]
fn a_write_after_the_floor_is_not_lost_to_a_life_the_member_holds_from_before_it() {
    let (a, _da) = engine();
    let (c, _dc) = engine();
    let (d, _dd) = engine();
    let (m, _dm) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let cc = c.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let x = a.insert(&ca, doc! { "_id": 1 }).unwrap();
    c.insert(&cc, doc! { "_id": 1 }).unwrap();
    let insert_of = |e: &Engine| {
        e.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background)
            .unwrap()
            .entries
            .into_iter()
            .find(|e| e.kind == kimmy_core::OpKind::Insert)
            .unwrap()
    };
    let (ia, ic) = (insert_of(&a), insert_of(&c));
    // The life that wins the first-insert rule is the newer; M holds it, and D wrote into the other.
    let (hi, lo, lo_engine) = if ic.stamp > ia.stamp { (&ic, &ia, &a) } else { (&ia, &ic, &c) };
    d.apply_batch(
        &lo_engine
            .entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background)
            .unwrap()
            .entries,
    )
    .unwrap();
    later();
    let cd = d.get_collection("shop", "orders").unwrap();
    d.replace(&cd, &x, doc! { "_id": 1, "m": 7 }, false).unwrap();
    let write = d
        .entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .find(|e| e.kind == kimmy_core::OpKind::Replace)
        .unwrap();
    let dropped = Hlc::max(ia.stamp.hlc, ic.stamp.hlc);
    assert!(write.stamp.hlc > dropped, "premise: D's write is after the drop");

    m.apply_batch(
        &a.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background)
            .unwrap()
            .entries
            .into_iter()
            .filter(|e| e.kind != kimmy_core::OpKind::Insert)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let cm = m.get_collection("shop", "orders").unwrap();
    assert!(m.apply_remote(&cm, hi).unwrap(), "premise: M holds the newer life");
    assert!(m.get(&cm, &x).unwrap().is_some(), "premise: M holds the document");
    let _ = lo;
    m.record_collection_drop(cm.id, kimmy_core::Stamp::new(dropped, m.node_id(), m.epoch()))
        .unwrap();
    m.apply_batch(std::slice::from_ref(&write)).unwrap();
    let held = m.get(&cm, &x).unwrap().expect("the document stands");
    assert!(
        held.contains_key("m"),
        "{MESSAGE}: the write after the drop was not lost to the older life"
    );
}
