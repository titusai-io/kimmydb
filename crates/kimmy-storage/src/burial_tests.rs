//! The bound-out burial (ADR-272, decision 5; Q6.4 item 1): when a binding takes effect on a member, the physical collections it
//! leaves behind are buried IN THE SAME TRANSACTION, unconditionally (never `drop_keeping_later_writes`), and only when the new
//! physical is alive on this member: every unforeseen race becomes "the swap did not happen here yet", never "the name lost its
//! data". A burial made by the register is every member's own judgement of the same events: it mints no drop entry.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, WriterHolder, physical_now_ms};

const DB: &str = "app";
const STAGE: &str = "orders.__stg.a1";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().unwrap();
    let mut outcome = SyncOutcome::default();
    b.apply_peer_window_into(
        &theirs,
        Some(&mine),
        entries,
        PeerWindow {
            scanned_to: entries.last().map_or(Hlc::ZERO, |e| e.stamp.hlc),
            exhausted: true,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )
    .unwrap();
    outcome
}

fn bind_of(a: &Engine, physical: &str, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: physical.into(),
            })
            .unwrap(),
        ),
        meta: None,
    }
}

/// A made a stage (with the creation entry B takes) and the binding of it; B holds `orders` with a document, and the stage's creation.
struct Case {
    b: Engine,
    creation: OplogEntry,
    bind: OplogEntry,
    orders: CollectionId,
    _da: tempfile::TempDir,
    _db: tempfile::TempDir,
}

fn case() -> Case {
    let (a, da) = engine();
    let (b, db) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    let creation = everything(&a)
        .into_iter()
        .find(|x| {
            x.kind == OpKind::CreateCollection && x.collection == CollectionId::derive(DB, STAGE)
        })
        .unwrap();
    let bind = bind_of(&a, STAGE, creation.stamp.hlc.wall_ms + 1);
    let orders = b.create_collection(DB, "orders").unwrap();
    b.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    b.apply_batch(std::slice::from_ref(&creation)).unwrap();
    Case { b, creation, bind, orders: orders.id, _da: da, _db: db }
}

fn is_buried(b: &Engine, id: CollectionId) -> bool {
    b.collection_dropped_at(id).unwrap().is_some()
}

#[test]
fn a_binding_in_a_window_buries_the_old_physical_in_the_same_transaction_and_the_name_resolves_to_the_stage()
 {
    let c = case();
    window(&c.b, std::slice::from_ref(&c.bind));
    assert!(
        c.b.get_collection(DB, "orders").is_err(),
        "the old physical is gone from the catalogue"
    );
    assert_eq!(
        c.b.collection_dropped_at(c.orders).unwrap(),
        Some(c.bind.stamp),
        "buried by the binding that bound it out, at its stamp"
    );
    let resolved = c.b.resolve(DB, "orders").unwrap();
    assert_eq!((resolved.physical.as_str(), resolved.visible), (STAGE, Some(c.bind.stamp)));
}

#[test]
fn the_old_physical_is_never_buried_unless_the_new_one_is_alive_here() {
    // The register knows the stage (a creation row without a collection) and a binding of it takes effect, but no collection stands
    // under the stage on this member: nothing is buried and the name keeps resolving to what it did.
    let (a, _da) = engine();
    let (b, _db) = engine();
    let orders = b.create_collection(DB, "orders").unwrap();
    let ghost = "orders.__stg.ghost";
    let created = Stamp::new(Hlc::new(physical_now_ms(), 0), a.node_id(), a.epoch());
    let txn = b.begin_write(WriterHolder::Ddl).unwrap();
    crate::register::write_stage_row(
        &txn,
        DB,
        ghost,
        &kimmy_core::StageMeta {
            created,
            info: kimmy_core::StageInfo { deadline: Hlc::new(physical_now_ms() + 60_000, 0) },
        },
    )
    .unwrap();
    txn.commit().unwrap();
    let bind = bind_of(&a, ghost, created.hlc.wall_ms + 1);
    window(&b, std::slice::from_ref(&bind));
    assert!(
        !is_buried(&b, orders.id),
        "the new physical is not alive here: the swap did not happen here yet"
    );
    assert_eq!(b.resolve(DB, "orders").unwrap().meta.id, orders.id);
}

#[test]
fn a_parked_binding_buries_in_the_transaction_that_releases_it() {
    let c = case();
    c.b.apply_batch(std::slice::from_ref(&c.bind)).unwrap(); // no window behind it: information
    assert!(!is_buried(&c.b, c.orders), "premise: information buries nothing");
    window(&c.b, std::slice::from_ref(&c.bind)); // arrives in position: released, in effect
    assert!(is_buried(&c.b, c.orders), "released: in effect and buried in the same transaction");
}

#[test]
fn a_grant_that_covers_a_parked_binding_buries_in_the_same_transaction() {
    let c = case();
    c.b.apply_batch(std::slice::from_ref(&c.bind)).unwrap();
    assert!(!is_buried(&c.b, c.orders), "premise: information buries nothing");
    let mut granted = VersionVector::default();
    granted.observe(c.bind.stamp);
    c.b.absorb_version_vector(&granted).unwrap();
    assert!(is_buried(&c.b, c.orders), "the grant took effect and buried");
}

#[test]
fn a_burial_made_by_the_register_mints_no_entry_and_hands_the_rows_to_the_purger() {
    let c = case();
    let before = everything(&c.b).len();
    window(&c.b, std::slice::from_ref(&c.bind));
    let drops: Vec<_> =
        everything(&c.b).into_iter().filter(|e| e.kind == OpKind::DropCollection).collect();
    assert!(drops.is_empty(), "every member derives the burial from the same events: {drops:?}");
    assert_eq!(everything(&c.b).len(), before + 1, "only the binding's own entry was appended");
    assert!(c.b.purge_pending(c.orders).unwrap(), "the old physical's rows are owed to the purger");
    assert!(
        c.b.queued_purges().contains(&c.orders),
        "and handed to it: the burial was finished after its commit, not only recorded"
    );
}

#[test]
fn a_late_write_to_the_buried_physical_is_history_and_does_not_bring_it_back() {
    // No zombie (decision 5): nothing keeps a physical named exactly like the logical name alive after the flip.
    let c = case();
    window(&c.b, std::slice::from_ref(&c.bind));
    let (a2, _d2) = engine();
    a2.create_collection(DB, "orders").unwrap();
    let late_coll = a2.get_collection(DB, "orders").unwrap();
    a2.insert(&late_coll, doc! { "_id": 2, "v": "late" }).unwrap();
    let late = everything(&a2).into_iter().last().unwrap();
    c.b.apply_batch(std::slice::from_ref(&late)).unwrap();
    assert!(
        c.b.get_collection(DB, "orders").is_err(),
        "the old physical does not come back for a late write"
    );
    assert_eq!(c.b.resolve(DB, "orders").unwrap().physical, STAGE);
}

#[test]
fn a_name_that_was_never_swapped_buries_nothing_when_an_unrelated_binding_arrives() {
    let c = case();
    let other = c.b.create_collection(DB, "items").unwrap();
    window(&c.b, std::slice::from_ref(&c.bind));
    assert!(!is_buried(&c.b, other.id), "only the name the binding addresses is judged");
}
