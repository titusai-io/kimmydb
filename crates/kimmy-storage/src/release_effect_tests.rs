//! Build item (a) (ADR-272, decision 3): a release takes effect in the same transaction. The mark of a parked binding is
//! released by a window (rule B's tests) and by a grant (a snapshot's last page, a rebuilt vector); on every route the binding
//! takes effect in the transaction that releases its mark, so no member ever serves coverage of a binding it does not act on.

use kimmy_core::{
    CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};

use crate::{Engine, WalkScope, WriterHolder, physical_now_ms};

const DB: &str = "app";
const STAGE: &str = "orders.__stg.a1";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

struct Case {
    creation: OplogEntry,
    bind: OplogEntry,
    /// A second stage of the same name, made by ANOTHER engine, and a binding of it by that engine, stamped above the first binding.
    creation2: OplogEntry,
    bind2: OplogEntry,
    _dirs: [tempfile::TempDir; 2],
}

const STAGE2: &str = "orders.__stg.a2";

fn creation_of(a: &Engine, physical: &str) -> OplogEntry {
    everything(a)
        .into_iter()
        .find(|x| {
            x.kind == OpKind::CreateCollection && x.collection == CollectionId::derive(DB, physical)
        })
        .unwrap()
}

fn bind_of(a: &Engine, wall_ms: u64, physical: &str) -> OplogEntry {
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

fn case() -> Case {
    let (a, da) = engine();
    let (a2, da2) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    a2.create_stage(DB, STAGE2, MergePolicy::Document, deadline).unwrap();
    let creation = creation_of(&a, STAGE);
    let creation2 = creation_of(&a2, STAGE2);
    let above = creation.stamp.hlc.wall_ms.max(creation2.stamp.hlc.wall_ms);
    let bind = bind_of(&a, above + 1, STAGE);
    let bind2 = bind_of(&a2, above + 2, STAGE2);
    Case { creation, bind, creation2, bind2, _dirs: [da, da2] }
}

/// A member that holds the creation and has the binding as information: known, not in effect, its entry held.
fn parked_member(c: &Case) -> (Engine, tempfile::TempDir) {
    let (b, db) = engine();
    b.apply_batch(std::slice::from_ref(&c.creation)).unwrap();
    b.apply_batch(std::slice::from_ref(&c.bind)).unwrap();
    assert_eq!((in_effect(&b), parked(&b), b.held_marks().unwrap()), (0, 1, 1));
    (b, db)
}

/// A member that holds both creations and has both bindings as information.
fn parked_two(c: &Case) -> (Engine, tempfile::TempDir) {
    let (b, db) = engine();
    b.apply_batch(std::slice::from_ref(&c.creation)).unwrap();
    b.apply_batch(std::slice::from_ref(&c.creation2)).unwrap();
    b.apply_batch(std::slice::from_ref(&c.bind)).unwrap();
    b.apply_batch(std::slice::from_ref(&c.bind2)).unwrap();
    assert_eq!((in_effect(&b), parked(&b), b.held_marks().unwrap()), (0, 2, 2));
    (b, db)
}

fn in_effect(b: &Engine) -> usize {
    let txn = b.begin_write(WriterHolder::Ddl).unwrap();
    let n = crate::register::timeline(&txn, DB, "orders").unwrap().events().len();
    txn.abort().unwrap();
    n
}

fn parked(b: &Engine) -> usize {
    let txn = b.begin_write(WriterHolder::Ddl).unwrap();
    let n = crate::register::parked(&txn, DB, "orders").unwrap().events().len();
    txn.abort().unwrap();
    n
}

fn grant_over(stamp: Stamp) -> VersionVector {
    let mut granted = VersionVector::default();
    granted.observe(stamp);
    granted
}

#[test]
fn a_grant_that_covers_a_parked_binding_puts_it_in_effect_in_the_same_transaction() {
    let c = case();
    let (b, _db) = parked_member(&c);
    b.absorb_version_vector(&grant_over(c.bind.stamp)).unwrap();
    assert_eq!(b.held_marks().unwrap(), 0, "the grant released the mark");
    assert_eq!(
        in_effect(&b),
        1,
        "and the binding is in effect: coverage and effect arrive together"
    );
    assert_eq!(parked(&b), 0, "no longer information");
}

/// Two bindings are parked; the grant reaches the first and stops short of the second. Coverage alone decides.
///
/// **A stand-in of two engines.** A replay stops behind the first event of an origin it parks, so no member holds two parked events of
/// one origin; the second stage and its binding are another engine's. On a real member two parked events can only be of one node
/// under two epochs: two engines differ in the node as well, which this test does not tell apart.
#[test]
fn a_grant_puts_in_effect_only_the_binding_it_covers() {
    let c = case();
    let (b, _db) = parked_two(&c);
    // the grant names both origins: the first up to its binding, the second up to one millisecond short of its own
    let mut granted = grant_over(c.bind.stamp);
    granted.observe(Stamp::new(
        Hlc::new(c.bind2.stamp.hlc.wall_ms - 1, 0),
        c.bind2.stamp.node,
        c.bind2.stamp.epoch,
    ));
    b.absorb_version_vector(&granted).unwrap();
    assert_eq!(in_effect(&b), 1, "only the binding the grant covers is in effect");
    assert_eq!(parked(&b), 1, "the one above the grant is still information");
    assert_eq!(b.held_marks().unwrap(), 1, "and still held");
}

#[test]
fn a_grant_that_does_not_reach_a_parked_binding_leaves_it_information() {
    let c = case();
    let (b, _db) = parked_member(&c);
    let below = Stamp::new(
        Hlc::new(c.bind.stamp.hlc.wall_ms - 1, 0),
        c.bind.stamp.node,
        c.bind.stamp.epoch,
    );
    b.absorb_version_vector(&grant_over(below)).unwrap();
    assert_eq!(b.held_marks().unwrap(), 1, "still held");
    assert_eq!((in_effect(&b), parked(&b)), (0, 1), "still information");
}

#[test]
fn no_member_serves_coverage_of_a_binding_it_does_not_act_on() {
    // The invariant a snapshot read between a release and its effect would break: no information row is at or below the
    // servable vector. Checked after the grant, the route that releases without a window.
    let c = case();
    let (b, _db) = parked_member(&c);
    let serves =
        |b: &Engine| b.version_vector().unwrap().get(c.bind.stamp.origin()) >= c.bind.stamp.hlc;
    assert!(!serves(&b) || parked(&b) == 0, "before the grant: not covered");
    b.absorb_version_vector(&grant_over(c.bind.stamp)).unwrap();
    assert!(serves(&b), "the grant covers the binding");
    assert_eq!(
        parked(&b),
        0,
        "so nothing at or below the servable vector is still only information"
    );
}

#[test]
fn a_release_that_left_a_binding_as_information_is_caught_by_the_check() {
    // FIX-4: a DDL-kind register event released in position is in effect in the transaction that releases it. Releasing the
    // mark alone is the defect; the check names it.
    let c = case();
    let (b, _db) = parked_member(&c);
    let txn = b.begin_write(WriterHolder::Replication).unwrap();
    assert!(crate::engine::release_held_in_position(&txn, &c.bind.stamp).unwrap());
    let caught = crate::register::check_released_took_effect(&txn, &c.bind.stamp);
    txn.abort().unwrap();
    match caught {
        Err(crate::StorageError::Corrupt(why)) => {
            assert!(why.contains("still only information"), "{why}")
        }
        other => panic!("the check must fire on a released binding still parked: {other:?}"),
    }
}

#[test]
fn the_check_passes_when_the_release_took_effect() {
    let c = case();
    let (b, _db) = parked_member(&c);
    let txn = b.begin_write(WriterHolder::Replication).unwrap();
    assert!(crate::engine::release_held_in_position(&txn, &c.bind.stamp).unwrap());
    crate::register::release(&txn, DB, "orders", &c.bind.stamp).unwrap();
    crate::register::check_released_took_effect(&txn, &c.bind.stamp).unwrap();
    txn.abort().unwrap();
}
