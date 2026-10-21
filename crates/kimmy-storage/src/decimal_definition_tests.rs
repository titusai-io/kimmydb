//! A `Decimal128` `_id` and a partial index with a `Decimal128` bound travel like any
//! other: a replicated entry, a restored or repaired snapshot page and a converge row
//! apply them, and the definition is built where it lands (ADR-238).

use bson::{Bson, Document, doc};
use kimmy_core::{CollectionId, Hlc};

use crate::{CollectionMeta, Engine, IndexField, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn dec(text: &str) -> Bson {
    Bson::Decimal128(text.parse().unwrap())
}

fn orders() -> CollectionId {
    CollectionId::derive("shop", "orders")
}

fn coll(e: &Engine) -> CollectionMeta {
    e.get_collection("shop", "orders").unwrap()
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn pull(into: &Engine, from: &Engine) {
    let entries = from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

/// Two members that have replicated everything so far.
fn pair() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    a.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    (a, b, [da, db])
}

fn bounded(e: &Engine, filter: Document) -> crate::Result<crate::IndexMeta> {
    e.create_index_with(
        "shop",
        "orders",
        vec![IndexField::ascending("email")],
        false,
        crate::Enforcement::Local,
        Some("positive".into()),
        None,
        Some(filter),
    )
}

fn names(e: &Engine) -> Vec<String> {
    e.list_indexes("shop", "orders").unwrap().into_iter().map(|i| i.name).collect()
}

#[test]
fn a_replicated_decimal_id_lands_on_its_own_document() {
    let (a, b, _d) = pair();
    let id = a.insert(&coll(&a), doc! { "_id": dec("1.5"), "v": 1 }).unwrap();
    pull(&b, &a);
    assert!(b.get(&coll(&b), &id).unwrap().is_some());
    assert_eq!(b.count(&coll(&b), WalkScope::Request).unwrap(), 1);
}

#[test]
fn a_replicated_decimal_bound_definition_is_applied() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 1, "email": "x", "balance": dec("10") }).unwrap();
    bounded(&a, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    pull(&b, &a);
    assert!(names(&b).contains(&"positive".to_string()), "the peer's definition is built");
}

#[test]
fn a_snapshots_decimal_bound_definition_is_applied() {
    let (a, b, _d) = pair();
    bounded(&a, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    let mut progress = SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    }
    assert!(names(&b).contains(&"positive".to_string()), "a snapshot's definition is built");
}

#[test]
fn a_repaired_decimal_bound_definition_is_built() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 1, "email": "x", "balance": dec("10") }).unwrap();
    pull(&b, &a);
    later();
    bounded(&a, doc! { "balance": { "$gt": dec("0") } }).unwrap();
    let mut progress = SnapshotProgress::of_collection_definitions(orders(), 1_000_000);
    let page = a.snapshot_page(None, progress.scope()).unwrap();
    let done = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert_eq!(done.definitions.built, 1, "{done:?}");
    assert!(names(&b).contains(&"positive".to_string()), "the repair built it");
}

#[test]
fn a_converge_row_of_a_decimal_id_is_applied_over_a_held_row() {
    let (a, b, _d) = pair();
    let c = coll(&a);
    let id = a.insert(&c, doc! { "_id": dec("1.5"), "v": 1 }).unwrap();
    pull(&b, &a);
    later();
    a.replace(&c, &id, doc! { "v": 2 }, false).unwrap();
    let mut progress =
        SnapshotProgress::of_collection_converging(orders(), kimmy_core::VersionVector::new());
    let mut applied = 0;
    while !progress.is_complete() {
        let page = a.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        applied += b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap().applied;
    }
    assert_eq!(applied, 1, "the newer row was applied");
    assert_eq!(b.get(&coll(&b), &id).unwrap().unwrap().get_i32("v").unwrap(), 2);
}
