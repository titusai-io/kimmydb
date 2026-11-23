//! Pins for the routes a drop of a swapped name can take that need no change of their own (ADR-272, decisions 5 and 6; Q6.4 item
//! 1b-2, routes R5, R7 and R8). Each is judged by the register's facts: the flip buries the identity at the binding's stamp and
//! leaves the bound stage serving; a drop of the name buries the stage. A route that read the identity's burial as "the name is
//! missing", or exempted a stage from a database's drop, would lose the name or keep a zombie.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, CollectionRef, Hlc, MergePolicy, OpKind, OplogEntry, Stamp,
    VersionVector,
};

use crate::divergence::{LocalState, PeerAnswer, compare};
use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

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

fn absent(e: &Engine) -> bool {
    matches!(
        e.resolve(DB, "orders"),
        Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound { .. }))
    )
}

struct World {
    a: Engine,
    creation: OplogEntry,
    _dirs: Vec<tempfile::TempDir>,
}

/// The stage's creation, minted by one author `a`.
fn world() -> World {
    let (a, da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    let creation = everything(&a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, STAGE)
        })
        .unwrap();
    World { a, creation, _dirs: vec![da] }
}

fn life_wall(m: &Engine) -> u64 {
    m.get_collection(DB, "orders").unwrap().created.wall_ms
}

/// The binding of the stage, stamped at `wall_ms`.
fn bind_at(w: &World, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), w.a.node_id(), w.a.epoch()),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: STAGE.into(),
            })
            .unwrap(),
        ),
        meta: None,
    }
}

/// A member that holds `orders` and the stage's creation, and has NOT heard of the binding.
fn unflipped(w: &World) -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    (m, d)
}

/// A member that has made the binding visible, and the binding: stamped above the creation, above the member's own life and
/// above `floor` (the lives of other members), so that no life is created above the tombstone the flip leaves. No stamp here
/// depends on how long the test has been running.
fn flipped(w: &World, floor: u64) -> (Engine, tempfile::TempDir, OplogEntry) {
    let (m, d) = unflipped(w);
    let wall = [life_wall(&m), w.creation.stamp.hlc.wall_ms, floor, physical_now_ms()]
        .into_iter()
        .max()
        .unwrap()
        + 1;
    let bind = bind_at(w, wall);
    window(&m, std::slice::from_ref(&bind));
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, STAGE, "premise: the flip is visible");
    (m, d, bind)
}

fn drop_database_entry(by: &Engine, wall_ms: u64) -> OplogEntry {
    crate::engine::ddl_entry(
        Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        OpKind::DropCollection,
        kimmy_core::database_drop_id(DB),
        &CollectionRef::new(DB, kimmy_core::DATABASE_DROP_NAME),
    )
    .unwrap()
}

// ---- R5: a database's drop (ADR-229) covers the collections created at or below it, the stage among them ----

#[test]
fn a_database_drop_above_the_stage_buries_the_bound_stage_and_the_name_is_absent() {
    let w = world();
    let (m, _d, bind) = flipped(&w, 0);
    let (x, _dx) = engine();
    // above the binding, the stage's creation and everything the member wrote
    let wall = physical_now_ms().max(bind.stamp.hlc.wall_ms) + 10;
    window(&m, &[drop_database_entry(&x, wall)]);
    assert!(m.get_collection(DB, STAGE).is_err(), "the bound stage is buried with the database");
    assert!(absent(&m), "and the name resolves to nothing");
}

#[test]
fn a_database_drop_below_the_stage_leaves_the_stage_it_was_born_after() {
    let w = world();
    let (m, _d, _bind) = flipped(&w, 0);
    let (x, _dx) = engine();
    let drop = drop_database_entry(&x, w.creation.stamp.hlc.wall_ms - 1);
    window(&m, std::slice::from_ref(&drop));
    assert!(m.get_collection(DB, STAGE).is_ok(), "a stage created after the drop stands");
    assert_eq!(
        m.resolve(DB, "orders").unwrap().physical,
        STAGE,
        "and the name still answers from it"
    );
    assert_eq!(
        m.collection_dropped_at(CollectionId::derive(DB, STAGE)).unwrap(),
        Some(drop.stamp),
        "with its own tombstone at the drop, as every collection the sweep spares"
    );
}

// ---- R7: the existence half of the divergence check ----

fn local_state(e: &Engine) -> LocalState {
    LocalState {
        collections: e.all_collection_ids().unwrap(),
        probe: None,
        dropped: e.collection_tombstones().unwrap(),
    }
}

fn answer_of(e: &Engine) -> PeerAnswer {
    PeerAnswer {
        collections: e.all_collection_ids().unwrap(),
        probe_count: None,
        incarnations: e.collection_incarnations().unwrap(),
    }
}

#[test]
fn the_identity_a_flip_buried_is_not_a_missing_collection_to_the_existence_half() {
    // Two members at different points of one swap. The flipped one has buried `orders` (its tombstone is the binding's stamp);
    // the other still holds it, at an incarnation below that tombstone. The existence half must not report it, or the repair it
    // plans pulls `orders` back from the member that has not flipped, and the burial is undone by the machinery meant to
    // converge it.
    let w = world();
    let (behind, _d2) = unflipped(&w);
    let (flipped_member, _d1, _bind) = flipped(&w, life_wall(&behind));
    let findings = compare(&local_state(&flipped_member), &answer_of(&behind));
    assert!(findings.existence.is_empty(), "nothing is missing: {:?}", findings.existence);
    // and the other way: a member holding the identity does not report the stage it has not got a binding for as missing either
    let findings = compare(&local_state(&behind), &answer_of(&flipped_member));
    assert!(
        !findings.existence.contains(&CollectionId::derive(DB, "orders")),
        "the peer does not hold the identity: nothing to find"
    );
}

// ---- R8: a restart, and the count half's replay ----

#[test]
fn a_restart_after_a_flip_sweeps_nothing_and_the_name_stays_answered_by_the_stage() {
    let w = world();
    let (m, _d, _bind) = flipped(&w, 0);
    assert_eq!(
        m.reconcile_late_drops().unwrap(),
        0,
        "no live collection owes a sweep for a burial of another physical"
    );
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, STAGE, "the flip stands");
}

#[test]
fn a_restart_after_a_drop_of_the_name_sweeps_nothing_and_the_name_stays_absent() {
    let w = world();
    let (m, _d, _bind) = flipped(&w, 0);
    assert!(m.drop_collection(DB, "orders").unwrap());
    assert_eq!(m.reconcile_late_drops().unwrap(), 0);
    assert!(absent(&m), "the name stays dropped");
}
