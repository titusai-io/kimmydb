//! The drop of a once-swapped name (ADR-272, decision 6; Q6.4 item 1b-1): a DROP addressed to the logical name is a LogicalDrop
//! register event, put in effect through the one door under rule B (in a window; information otherwise) and settled in the same
//! transaction, so a drop stamped above the flip buries the BOUND physical and one stamped below it does not. Judged by the old
//! rule (the identity is already buried, so the drop finds no collection of that name) the bound successor survives the drop on a
//! member that made the binding visible and the identity is dropped on one that did not: the zombie of decision 5. A name is
//! "once swapped" here if it has register events or a stage of it is held, and a first event seeds the register from the
//! tombstone of a drop the member already applied, so a drop that arrived before the binding still counts.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, CollectionRef, Hlc, MergePolicy, OpKind, OplogEntry, Stamp,
    VersionVector,
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

fn bind_of(by: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
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

/// A drop of the logical name `orders`, as the member `by` would mint it at `wall_ms`.
fn drop_of(by: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::DropCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(bson::serialize_to_vec(&CollectionRef::new(DB, "orders")).unwrap()),
        meta: None,
    }
}

struct World {
    /// The author of the stage and its binding.
    a: Engine,
    /// The author of the drop.
    x: Engine,
    creation: OplogEntry,
    bind: OplogEntry,
    _dirs: Vec<tempfile::TempDir>,
}

fn world() -> World {
    let (a, da) = engine();
    let (x, dx) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    let creation = everything(&a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, STAGE)
        })
        .unwrap();
    let bind = bind_of(&a, creation.stamp.hlc.wall_ms + 1);
    World { a, x, creation, bind, _dirs: vec![da, dx] }
}

/// The wall time a member's `orders` was created at.
fn life_wall(m: &Engine) -> u64 {
    m.get_collection(DB, "orders").unwrap().created.wall_ms
}

/// The wall time of a drop aimed at the lives that stand on `members`: above every life's creation, above everything written to
/// them (the clock is read AFTER the members were built, so no write they hold is stamped above it, which is the case a life
/// keeps its writes against, ADR-231) and above the binding. The incarnation rule cannot turn it away and no write of the
/// members can outlive it, however long the test has been running.
fn wall_above(w: &World, members: &[&Engine]) -> u64 {
    members
        .iter()
        .map(|m| life_wall(m))
        .chain([w.bind.stamp.hlc.wall_ms, physical_now_ms()])
        .max()
        .unwrap_or_default()
        + 10
}

/// A drop of the name stamped just above the binding, for a member that has the register's events (the incarnation rule is not
/// asked of such a name, so the clock cannot matter).
fn drop_above_the_flip(w: &World) -> OplogEntry {
    drop_of(&w.x, w.bind.stamp.hlc.wall_ms + 10)
}

/// A member holding `orders` (with a document) and the stage's creation: the binding is not known to it yet.
fn member(w: &World) -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    (m, d)
}

fn buried_at(e: &Engine, id: CollectionId) -> Option<Stamp> {
    e.collection_dropped_at(id).unwrap()
}

fn register_events(e: &Engine) -> Vec<Stamp> {
    let txn = e.begin_write(WriterHolder::Ddl).unwrap();
    let events = crate::register::timeline(&txn, DB, "orders")
        .unwrap()
        .events()
        .iter()
        .map(|(s, _)| *s)
        .collect();
    txn.abort().unwrap();
    events
}

fn absent(e: &Engine) -> bool {
    matches!(
        e.resolve(DB, "orders"),
        Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound { .. }))
    )
}

/// A member that has made the binding visible (the identity buried, the stage serving).
fn flipped(w: &World) -> (Engine, tempfile::TempDir) {
    let (b, d) = member(w);
    window(&b, std::slice::from_ref(&w.bind));
    assert_eq!(b.resolve(DB, "orders").unwrap().physical, STAGE, "premise: the flip is visible");
    (b, d)
}

#[test]
fn a_local_drop_of_a_swapped_name_drops_the_bound_physical_and_mints_one_entry() {
    let w = world();
    let (b, _d) = flipped(&w);
    let stage_id = CollectionId::derive(DB, STAGE);
    assert!(
        b.drop_collection(DB, "orders").unwrap(),
        "the drop found the name: it is the bound physical's"
    );
    let drops: Vec<_> =
        everything(&b).into_iter().filter(|e| e.kind == OpKind::DropCollection).collect();
    assert_eq!(drops.len(), 1, "one drop entry, addressed to the logical name: {drops:?}");
    assert_eq!(drops[0].collection, CollectionId::derive(DB, "orders"));
    assert!(absent(&b), "the name resolves to nothing");
    assert_eq!(
        buried_at(&b, stage_id),
        Some(drops[0].stamp),
        "the stage is buried at the drop's stamp"
    );
    assert!(b.get_collection(DB, STAGE).is_err(), "the bound physical is gone, not only unnamed");
}

#[test]
fn a_replicated_drop_above_the_flip_in_a_window_buries_the_bound_physical() {
    let w = world();
    let (b, _d) = flipped(&w);
    let drop = drop_above_the_flip(&w);
    window(&b, std::slice::from_ref(&drop));
    assert!(absent(&b), "the name is dropped");
    assert_eq!(
        buried_at(&b, CollectionId::derive(DB, STAGE)),
        Some(drop.stamp),
        "the bound physical, at the drop"
    );
    assert!(register_events(&b).contains(&drop.stamp), "the drop is a register event of the name");
}

#[test]
fn a_replicated_drop_that_arrives_ahead_of_its_position_is_information_until_a_window_releases_it()
{
    let w = world();
    let (b, _d) = flipped(&w);
    let drop = drop_above_the_flip(&w);
    b.apply_batch(std::slice::from_ref(&drop)).unwrap(); // no window behind it
    assert!(!absent(&b), "known, not in effect: the stage still serves");
    assert_eq!(buried_at(&b, CollectionId::derive(DB, STAGE)), None);
    window(&b, std::slice::from_ref(&drop));
    assert!(absent(&b), "released by a window: in effect and settled in the same transaction");
}

#[test]
fn a_replicated_drop_stamped_below_the_flip_leaves_the_bound_physical() {
    let w = world();
    let (b, _d) = flipped(&w);
    // below the binding: the stage was born after the drop, so the drop cannot reach it
    let below = drop_of(&w.x, w.bind.stamp.hlc.wall_ms - 1);
    window(&b, std::slice::from_ref(&below));
    assert_eq!(b.resolve(DB, "orders").unwrap().physical, STAGE, "the flip stands");
    assert_eq!(buried_at(&b, CollectionId::derive(DB, STAGE)), None);
}

#[test]
fn a_member_that_made_the_binding_visible_and_one_that_did_not_end_alike_after_the_same_drop() {
    // THE ZOMBIE TEST. B: the binding first, then the drop. C: the drop FIRST (the identity is still its collection, the stage
    // held and unbound), then the binding. Same entries, two orders: one state.
    let w = world();
    let (b, _db) = member(&w);
    let (c, _dc) = member(&w);
    // minted after both lives exist, above both: C's incarnation rule cannot see a drop of a previous life
    let drop = drop_of(&w.x, wall_above(&w, &[&b, &c]));
    window(&b, std::slice::from_ref(&w.bind));
    window(&b, std::slice::from_ref(&drop));
    window(&c, std::slice::from_ref(&drop));
    window(&c, std::slice::from_ref(&w.bind));
    for (name, m) in [("B", &b), ("C", &c)] {
        assert!(absent(m), "{name}: the name is dropped");
        assert!(
            m.get_collection(DB, "orders").is_err(),
            "{name}: no zombie named like the logical name"
        );
        assert!(m.get_collection(DB, STAGE).is_err(), "{name}: the bound physical is gone too");
        assert_eq!(
            buried_at(m, CollectionId::derive(DB, STAGE)),
            Some(drop.stamp),
            "{name}: the stage's tombstone"
        );
    }
    let mut eb = register_events(&b);
    let mut ec = register_events(&c);
    eb.sort();
    ec.sort();
    assert_eq!(eb, ec, "the same register events on both");
    assert_eq!(eb.len(), 2, "the binding and the drop");
}

/// A member holding only `orders` (with a document): no stage, no event.
fn member_with_orders() -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    (m, d)
}

/// The member applies the drop BEFORE it knows of any swap (no stage, no event: the old rule takes the identity and leaves a
/// tombstone), and then meets the stage's creation.
fn drop_first_then_the_creation(m: &Engine, w: &World, drop: &OplogEntry) {
    window(m, std::slice::from_ref(drop));
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the old rule dropped the identity");
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
}

#[test]
fn a_drop_applied_before_any_swap_was_known_still_buries_a_binding_stamped_below_it() {
    let w = world();
    let (m, _d) = member_with_orders();
    let drop = drop_of(&w.x, wall_above(&w, &[&m]));
    drop_first_then_the_creation(&m, &w, &drop);
    window(&m, std::slice::from_ref(&w.bind));
    assert!(absent(&m), "the name is dropped, as on the member that knew of the swap");
    assert_eq!(
        buried_at(&m, CollectionId::derive(DB, STAGE)),
        Some(drop.stamp),
        "the bound physical is buried at the drop"
    );
    let mut events = register_events(&m);
    events.sort();
    assert_eq!(
        events,
        vec![w.bind.stamp, drop.stamp],
        "the same register events as every other member"
    );
}

#[test]
fn a_binding_stamped_above_a_drop_applied_before_any_swap_was_known_is_not_touched_by_it() {
    let w = world();
    let (m, _d) = member_with_orders();
    // the drop is aimed at the life that stands (just above its creation); the binding is stamped above the drop
    let drop = drop_of(&w.x, wall_above(&w, &[&m]));
    let bind = bind_of(&w.a, drop.stamp.hlc.wall_ms + 10);
    drop_first_then_the_creation(&m, &w, &drop);
    window(&m, std::slice::from_ref(&bind));
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, STAGE, "the swap after the drop stands");
    assert_eq!(buried_at(&m, CollectionId::derive(DB, STAGE)), None);
}

#[test]
fn a_drop_aimed_at_a_life_that_ended_before_the_identity_is_the_old_rules_even_with_a_stage_held() {
    // No event of the name, a stage of it held, the identity created ABOVE the drop: the incarnation rule (ADR-148), not the
    // register's. The identity stands.
    let w = world();
    let (m, _d) = member(&w);
    let old_drop = drop_of(&w.x, 1_000);
    window(&m, std::slice::from_ref(&old_drop));
    assert!(
        m.get_collection(DB, "orders").is_ok(),
        "a drop of a previous life leaves the life that stands"
    );
    assert!(register_events(&m).is_empty(), "and is no register event");
}

#[test]
fn a_grant_that_covers_a_parked_drop_puts_it_in_effect_and_buries_in_the_same_transaction() {
    let w = world();
    let (b, _d) = flipped(&w);
    let drop = drop_above_the_flip(&w);
    b.apply_batch(std::slice::from_ref(&drop)).unwrap(); // ahead of its position: information
    assert!(!absent(&b), "premise: not in effect");
    let mut granted = VersionVector::default();
    granted.observe(drop.stamp);
    b.absorb_version_vector(&granted).unwrap();
    assert!(absent(&b), "the grant released the drop and it buried the bound physical with it");
    assert_eq!(buried_at(&b, CollectionId::derive(DB, STAGE)), Some(drop.stamp));
}
