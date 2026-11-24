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

/// The register events of `orders` known ahead of their position (information: not in effect).
fn parked_events(e: &Engine) -> Vec<Stamp> {
    let txn = e.begin_write(WriterHolder::Ddl).unwrap();
    let events = crate::register::parked(&txn, DB, "orders")
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

// -- A drop that kept later writes before the swap was known (ADR-272 decision 6, 2026-11-23) --------------------------------------------
//
// `seed_from_the_tombstone` returns early when the identity stands. A member that applied the drop as a plain drop and KEPT a write
// stamped after it (the survivors path re-dates the identity, ADR-236) holds the drop as the identity's tombstone and a standing
// identity; the binding stamped below that drop, which arrives afterwards, must be buried by the drop as on a member that knew of the
// swap when the drop arrived.

/// The creation of `orders` and one write to it stamped ABOVE the creation by a few milliseconds, both made on a third member (the
/// same two entries for every member under test).
fn a_life_and_a_later_write() -> (Vec<OplogEntry>, tempfile::TempDir) {
    let (s, d) = engine();
    let orders = s.create_collection(DB, "orders").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    s.insert(&orders, doc! { "_id": 1, "v": "after the drop" }).unwrap();
    (everything(&s), d)
}

#[test]
fn a_drop_that_kept_later_writes_before_the_swap_was_known_ends_as_on_a_member_that_knew() {
    let w = world();
    let (life, _ds) = a_life_and_a_later_write();
    let created = life
        .iter()
        .find(|e| e.kind == OpKind::CreateCollection)
        .expect("the life's creation")
        .stamp
        .hlc
        .wall_ms;
    // The drop is stamped above the life's creation and above the binding, and below the write: the write survives it.
    let drop = drop_of(&w.x, created.max(w.bind.stamp.hlc.wall_ms) + 1);
    let write = life.iter().find(|e| e.kind == OpKind::Insert).expect("the write");
    assert!(write.stamp.hlc > drop.stamp.hlc, "premise: the write is stamped after the drop");
    assert!(w.bind.stamp.hlc < drop.stamp.hlc, "premise: the binding is stamped below the drop");

    // K knew of the swap when the drop arrived: the stage's creation, the binding (flip), then the drop.
    let (k, _dk) = engine();
    window(&k, &life);
    k.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    window(&k, std::slice::from_ref(&w.bind));
    window(&k, std::slice::from_ref(&drop));

    // M applies the drop as a plain drop FIRST (no swap known), keeps the write, and then meets the creation and the binding.
    let (m, _dm) = engine();
    window(&m, &life);
    window(&m, std::slice::from_ref(&drop));
    assert!(
        m.get_collection(DB, "orders").is_ok(),
        "premise: the plain drop kept the identity, which holds the write stamped after the drop"
    );
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    window(&m, std::slice::from_ref(&w.bind));

    let mut ke = register_events(&k);
    let mut me = register_events(&m);
    ke.sort();
    me.sort();
    assert_eq!(me, ke, "the same register events as the member that knew");
    assert_eq!(absent(&m), absent(&k), "the name resolves the same way on both members");
    assert!(absent(&k), "the member that knew: the name is dropped");
    assert_eq!(
        buried_at(&k, CollectionId::derive(DB, STAGE)),
        Some(drop.stamp),
        "the member that knew buried the bound physical at the drop"
    );
    assert_eq!(
        buried_at(&m, CollectionId::derive(DB, STAGE)),
        buried_at(&k, CollectionId::derive(DB, STAGE)),
        "the bound physical is buried at the same stamp on both"
    );
}

// -- The widened seed: what a standing identity is seeded for, and what it is not (ADR-272 decision 6, 2026-11-23) ---------------------
//
// A past drop of a name is a logical drop only if a stage of the name was created below it. A tombstone is also what an ordinary
// re-create leaves behind and what a database's drop leaves on a life it spared, so the seed buries a standing identity only when it
// is the remains that very drop re-dated and the swap began below the drop.

/// A drop of the whole database `app` as the member `by` would mint it at `wall_ms`.
fn a_database_drop(by: &Engine, wall_ms: u64) -> OplogEntry {
    crate::engine::ddl_entry(
        Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        OpKind::DropCollection,
        kimmy_core::database_drop_id(DB),
        &CollectionRef::new(DB, "__dropped_database"),
    )
    .unwrap()
}

/// The wall time of a drop stamped above the life's creation and above the binding, and below the write a life of
/// [`a_life_and_a_later_write`] keeps.
fn above_the_creation_and_the_binding(w: &World, life: &[OplogEntry]) -> u64 {
    let created = life
        .iter()
        .find(|e| e.kind == OpKind::CreateCollection)
        .expect("the life's creation")
        .stamp
        .hlc
        .wall_ms;
    created.max(w.bind.stamp.hlc.wall_ms) + 1
}

/// How many documents the identity `orders` holds.
fn documents_kept(e: &Engine) -> u64 {
    let orders = e.get_collection(DB, "orders").unwrap();
    e.count(&orders, WalkScope::Background).unwrap()
}

fn serves_the_identity(e: &Engine) -> bool {
    e.resolve(DB, "orders").is_ok_and(|served| served.physical == "orders")
}

/// A member that has applied the drop of a life it re-created: the life made, dropped (buried: nothing was written after the drop),
/// and made again by a member that did not know of any swap, with one document in it. The next thing it meets is a swap.
fn recreated_after(
    drop_of_the_first: impl FnOnce(&Engine, u64) -> OplogEntry,
) -> (Engine, tempfile::TempDir, OplogEntry) {
    let (m, d) = engine();
    let first = m.create_collection(DB, "orders").unwrap();
    let (x, _dx) = engine();
    let drop = drop_of_the_first(&x, first.created.wall_ms + 1);
    window(&m, std::slice::from_ref(&drop));
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "premise: the drop buried the life it was aimed at"
    );
    std::thread::sleep(std::time::Duration::from_millis(20));
    let again = m.create_collection(DB, "orders").unwrap();
    m.insert(&again, doc! { "_id": 1, "v": "made after the drop" }).unwrap();
    (m, d, drop)
}

fn a_swap_that_began_after_a_re_create_does_not_bury_the_new_life(
    drop_of_the_first: impl FnOnce(&Engine, u64) -> OplogEntry,
) {
    let (m, _d, drop) = recreated_after(drop_of_the_first);
    let orders = CollectionId::derive(DB, "orders");
    assert!(
        buried_at(&m, orders).is_some(),
        "premise: the drop left its tombstone, which the re-create does not remove"
    );
    let before = m.get_collection(DB, "orders").unwrap();
    assert!(
        !before.is_redated(),
        "premise: the life is one made after the drop, not remains it re-dated"
    );
    // the swap begins now: its stage is created above the life that stands
    let w = world();
    assert!(
        w.creation.stamp.hlc > m.get_collection(DB, "orders").unwrap().created,
        "premise: the swap began after the life that stands was made"
    );
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    assert!(register_events(&m).is_empty(), "no event: the stage's creation is no drop");
    assert!(serves_the_identity(&m), "the name serves the life made after the drop");
    assert_eq!(documents_kept(&m), 1, "with its document");
    let after = m.get_collection(DB, "orders").unwrap();
    assert_eq!(after.created, before.created, "the life is the one that was made ({drop:?})");
    assert!(!after.is_redated(), "and nothing re-dated or buried it");
    // and the binding, when it comes, is the swap: the stage serves and the identity is bound out at the binding
    window(&m, std::slice::from_ref(&w.bind));
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, STAGE, "the swap after the drop stands");
    assert_eq!(buried_at(&m, orders), Some(w.bind.stamp), "the identity is buried at the binding");
}

#[test]
fn a_swap_that_began_after_an_ordinary_re_create_does_not_bury_the_new_life() {
    a_swap_that_began_after_a_re_create_does_not_bury_the_new_life(drop_of);
}

#[test]
fn a_binding_met_after_a_re_create_over_a_database_drop_seeds_no_drop_over_the_new_life() {
    // The swap began FIRST: the stage and its binding are stamped below the database's drop. M knows no swap: it applies the database's
    // drop, then makes the name again (the life stands over the database's tombstone, which the creation takes for its own), and then
    // meets the stage's creation (history under the database's drop, its row written) and the binding. The tombstone the new life
    // carries is the database's, not a drop of the life: the seed must not read it as one and bury what the drop did not end.
    //
    // Pins the rule before C3. With C3 and P2 (Chris, 2026-11-23: lost and reported) the seed buries this life and the register holds
    // the drop and the binding: Q65-model-rows C3-6, database form. This test, and mutant M2 of the seed's list, change there.
    let w = world();
    let (x, _dx) = engine();
    let drop = a_database_drop(&x, w.bind.stamp.hlc.wall_ms + 5);
    assert!(
        w.creation.stamp < drop.stamp && w.bind.stamp < drop.stamp,
        "premise: the swap began below the drop"
    );
    let (m, _dm) = engine();
    window(&m, std::slice::from_ref(&drop));
    std::thread::sleep(std::time::Duration::from_millis(20));
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "made after the database's drop" }).unwrap();
    assert!(
        buried_at(&m, CollectionId::derive(DB, "orders")).is_some(),
        "premise: the life stands over a tombstone"
    );
    assert!(
        orders.created > drop.stamp.hlc,
        "premise: the life was made after the database's drop"
    );
    assert!(
        !orders.is_redated(),
        "premise: it is a life made after the drop, not remains the drop re-dated"
    );
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    // the binding, known ahead of its position: it brings the seed if there is one, and the seed settles at once
    m.apply_batch(std::slice::from_ref(&w.bind)).unwrap();
    assert_eq!(
        parked_events(&m),
        vec![w.bind.stamp],
        "premise: the binding is known ahead of its position"
    );
    assert!(
        register_events(&m).is_empty(),
        "no drop of the name was seeded over a life made after the database's drop"
    );
    assert!(serves_the_identity(&m), "the life made after the database's drop still serves");
    assert_eq!(documents_kept(&m), 1, "with its document");
    // and when the binding's turn comes it is the one event of the name
    window(&m, std::slice::from_ref(&w.bind));
    assert_eq!(register_events(&m), vec![w.bind.stamp], "the binding alone");
    // the state in which this member and the one that knew agree today, and the one that changes with C3
    assert!(serves_the_identity(&m), "the life made after the database's drop still serves");
    assert_eq!(documents_kept(&m), 1, "with its document");
}

#[test]
fn a_swap_that_began_after_a_re_create_over_a_database_drop_does_not_bury_the_new_life() {
    a_swap_that_began_after_a_re_create_does_not_bury_the_new_life(a_database_drop);
}

#[test]
fn a_swap_that_began_after_a_drop_that_kept_later_writes_leaves_the_remains() {
    // The remains a drop re-dated: clause 1 holds. The swap began above the drop, so clause 2 does not, and the remains stand.
    let (life, _ds) = a_life_and_a_later_write();
    let created =
        life.iter().find(|e| e.kind == OpKind::CreateCollection).unwrap().stamp.hlc.wall_ms;
    let (x, _dx) = engine();
    let drop = drop_of(&x, created + 1);
    let write = life.iter().find(|e| e.kind == OpKind::Insert).unwrap();
    assert!(write.stamp.hlc > drop.stamp.hlc, "premise: the write is stamped after the drop");
    let w = world(); // after the write: the stage is created above the drop
    assert!(w.creation.stamp > drop.stamp, "premise: the swap began after the drop");
    let (m, _dm) = engine();
    window(&m, &life);
    window(&m, std::slice::from_ref(&drop));
    assert!(
        m.get_collection(DB, "orders").is_ok(),
        "premise: the drop kept the write and the identity"
    );
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    assert!(register_events(&m).is_empty(), "no seed: the swap did not begin below the drop");
    assert!(serves_the_identity(&m), "the name still serves the remains");
    assert_eq!(documents_kept(&m), 1, "with the write the drop kept");
}

/// Entries of one world, applied in the order a member that knew (K) and one that did not (M) meet them.
fn knew_and_did_not(
    w: &World,
    life: &[OplogEntry],
    drops: &[OplogEntry],
) -> ((Engine, tempfile::TempDir), (Engine, tempfile::TempDir)) {
    // K knew of the swap when the drops arrived: the stage's creation, the binding (flip), then the drops.
    let k = engine();
    window(&k.0, life);
    k.0.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    window(&k.0, std::slice::from_ref(&w.bind));
    for drop in drops {
        window(&k.0, std::slice::from_ref(drop));
    }
    // M applies the drops FIRST (no swap known), keeps the write, and then meets the creation and the binding.
    let m = engine();
    window(&m.0, life);
    for drop in drops {
        window(&m.0, std::slice::from_ref(drop));
    }
    assert!(
        m.0.get_collection(DB, "orders").is_ok(),
        "premise: the drops kept the identity, which holds the write stamped after them"
    );
    m.0.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    window(&m.0, std::slice::from_ref(&w.bind));
    (k, m)
}

#[test]
fn a_drop_that_kept_later_writes_is_seeded_by_the_stage_creation_alone() {
    // T1 with no binding: the stage's creation is the first thing the register learns, and its stamp (below the drop) is what the seed
    // asks of the standing identity. K knew of the swap when the drop arrived; M applied the drop first and kept the write.
    let w = world();
    let (life, _ds) = a_life_and_a_later_write();
    let drop = drop_of(&w.x, above_the_creation_and_the_binding(&w, &life));
    let write = life.iter().find(|e| e.kind == OpKind::Insert).expect("the write");
    assert!(write.stamp.hlc > drop.stamp.hlc, "premise: the write is stamped after the drop");
    assert!(w.creation.stamp < drop.stamp, "premise: the swap began below the drop");

    let (k, _dk) = engine();
    window(&k, &life);
    k.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    window(&k, std::slice::from_ref(&drop));

    let (m, _dm) = engine();
    window(&m, &life);
    window(&m, std::slice::from_ref(&drop));
    assert!(
        m.get_collection(DB, "orders").is_ok(),
        "premise: the drop kept the identity and its write"
    );
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();

    let mut ke = register_events(&k);
    let mut me = register_events(&m);
    ke.sort();
    me.sort();
    assert_eq!(ke, vec![drop.stamp], "premise: the member that knew holds the drop alone");
    assert_eq!(me, ke, "the same register events as the member that knew");
    assert!(absent(&k) && absent(&m), "the name is dropped on both");
    assert!(
        k.get_collection(DB, "orders").is_err() && m.get_collection(DB, "orders").is_err(),
        "the identity is gone on both"
    );
}

#[test]
fn a_database_drop_that_kept_later_writes_before_the_swap_was_known_ends_as_on_a_member_that_knew()
{
    let w = world();
    let (life, _ds) = a_life_and_a_later_write();
    let drop = a_database_drop(&w.x, above_the_creation_and_the_binding(&w, &life));
    let write = life.iter().find(|e| e.kind == OpKind::Insert).expect("the write");
    assert!(write.stamp.hlc > drop.stamp.hlc, "premise: the write is stamped after the drop");
    let ((k, _dk), (m, _dm)) = knew_and_did_not(&w, &life, std::slice::from_ref(&drop));
    assert!(absent(&k), "the member that knew: the name is dropped");
    assert_eq!(absent(&m), absent(&k), "the name resolves the same way on both members");
    assert!(
        k.get_collection(DB, "orders").is_err() && m.get_collection(DB, "orders").is_err(),
        "the identity is gone on both"
    );
    // (the register events differ until the database's drop is a logical drop of the swapped names: Q6.5, C3)
}

#[test]
fn a_collection_drop_then_a_database_drop_that_each_kept_later_writes_end_as_on_a_member_that_knew()
{
    let w = world();
    let (life, _ds) = a_life_and_a_later_write();
    let first = above_the_creation_and_the_binding(&w, &life);
    let t = drop_of(&w.x, first);
    let d = a_database_drop(&w.x, first + 1);
    let write = life.iter().find(|e| e.kind == OpKind::Insert).expect("the write");
    assert!(write.stamp.hlc > d.stamp.hlc, "premise: the write is stamped after both drops");
    let ((k, _dk), (m, _dm)) = knew_and_did_not(&w, &life, &[t, d]);
    assert!(absent(&k), "the member that knew: the name is dropped");
    assert!(
        absent(&m),
        "the name is dropped on the member that did not know, as on the one that did"
    );
}

#[test]
fn a_seed_over_remains_settles_when_the_first_event_is_only_known_ahead_of_its_position() {
    // The binding is the first event of the name, known ahead of its position (information): the seed is what puts the drop in effect,
    // and the remains it ends are buried in that transaction, not at the next settle.
    let w = world();
    let (life, _ds) = a_life_and_a_later_write();
    let drop = a_database_drop(&w.x, above_the_creation_and_the_binding(&w, &life));
    let (m, _dm) = engine();
    window(&m, &life);
    window(&m, std::slice::from_ref(&drop));
    assert!(m.get_collection(DB, "orders").is_ok(), "premise: the remains stand");
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap(); // history under the database's drop, its row written
    m.apply_batch(std::slice::from_ref(&w.bind)).unwrap(); // no window behind it: known ahead of its position
    assert_eq!(
        parked_events(&m),
        vec![w.bind.stamp],
        "premise: the binding is known ahead of its position"
    );
    assert_eq!(
        register_events(&m),
        vec![drop.stamp],
        "the seeded drop is in effect, the binding is only known"
    );
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "the remains are buried at once, not at the next settle"
    );
}

// -- A drop below a stage's creation is a plain drop (P1 and P3) ------------------------------------------------------------

#[test]
fn a_drop_stamped_below_a_held_stages_creation_is_the_same_plain_drop_whichever_arrives_first() {
    // c0 creates `orders`, T drops it, W is written to it after T (it survives the drop), and then a swap's stage is created above W.
    // J meets the stage's creation first and the drop late; M meets the drop first. The drop never concerned a stage created after it.
    let (life, _ds) = a_life_and_a_later_write();
    let created =
        life.iter().find(|e| e.kind == OpKind::CreateCollection).unwrap().stamp.hlc.wall_ms;
    let (x, _dx) = engine();
    let drop = drop_of(&x, created + 1);
    let write = life.iter().find(|e| e.kind == OpKind::Insert).unwrap();
    assert!(write.stamp.hlc > drop.stamp.hlc, "premise: the write is stamped after the drop");
    let w = world(); // after the write
    assert!(
        w.creation.stamp > write.stamp,
        "premise: the stage is created above the write and the drop"
    );

    let (j, _dj) = engine();
    window(&j, &life);
    j.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    window(&j, std::slice::from_ref(&drop));

    let (m, _dm) = engine();
    window(&m, &life);
    window(&m, std::slice::from_ref(&drop));
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();

    for (name, e) in [("J (the stage first)", &j), ("M (the drop first)", &m)] {
        assert!(register_events(e).is_empty(), "{name}: the drop is no register event");
        assert!(serves_the_identity(e), "{name}: the name serves the identity the drop spared");
        assert_eq!(documents_kept(e), 1, "{name}: with the write that survived the drop");
    }
}

#[test]
fn an_older_drop_that_arrives_while_only_a_binding_is_known_ahead_of_its_position_is_a_plain_drop()
{
    // The binding is known ahead of its position and nothing of the register is in effect: a drop stamped below the binding's stage
    // is the plain drop it is on every member that never heard of the swap, and the identity it spared stands until the binding's turn.
    let (life, _ds) = a_life_and_a_later_write();
    let created =
        life.iter().find(|e| e.kind == OpKind::CreateCollection).unwrap().stamp.hlc.wall_ms;
    let (x, _dx) = engine();
    let drop = drop_of(&x, created + 1);
    let w = world(); // after the write: the stage is created above the drop
    assert!(
        w.creation.stamp.hlc.wall_ms > drop.stamp.hlc.wall_ms,
        "premise: the stage is created above the drop"
    );
    let (m, _dm) = engine();
    window(&m, &life);
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    m.apply_batch(std::slice::from_ref(&w.bind)).unwrap(); // known ahead of its position
    assert_eq!(
        parked_events(&m),
        vec![w.bind.stamp],
        "premise: the binding is known ahead of its position"
    );
    window(&m, std::slice::from_ref(&drop));
    assert!(register_events(&m).is_empty(), "no event in effect: the drop is not a logical drop");
    assert!(serves_the_identity(&m), "the name serves the identity the drop spared");
    assert_eq!(documents_kept(&m), 1, "with the write that survived the drop");
    window(&m, std::slice::from_ref(&w.bind));
    assert_eq!(
        m.resolve(DB, "orders").unwrap().physical,
        STAGE,
        "the binding, when its turn comes, is the swap"
    );
}

#[test]
fn a_drop_aimed_at_a_life_that_ended_before_the_identity_stands_even_with_a_stage_created_below_it()
{
    // The twin of `a_drop_aimed_at_a_life_that_ended_before_the_identity_is_the_old_rules_even_with_a_stage_held`, whose drop is far
    // below the stage: there the stage clause fails and the test passes whatever the incarnation rule does. Here the stage is created
    // BELOW the drop, so the stage clause holds, and the identity is made above the drop: only the incarnation rule keeps it.
    //
    // Pins the rule before P2. With P2 (Chris, 2026-11-23: lost and reported) the member that did not know of the swap and made the
    // name again ends with the life buried and the register holding the drop: Q65-model-rows, C3's set. This drop becomes logical there.
    let w = world();
    let (x, _dx) = engine();
    let drop = drop_of(&x, w.creation.stamp.hlc.wall_ms + 1);
    let (m, _dm) = engine();
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap(); // the stage is held
    std::thread::sleep(std::time::Duration::from_millis(20));
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "made after the drop" }).unwrap();
    assert!(orders.created > drop.stamp.hlc, "premise: the identity is made above the drop");
    assert!(w.creation.stamp < drop.stamp, "premise: the stage is created below the drop");
    window(&m, std::slice::from_ref(&drop));
    assert!(
        register_events(&m).is_empty(),
        "a drop of a life that ended before this one is no register event"
    );
    assert!(serves_the_identity(&m), "the life that stands is not what the drop ended");
    assert_eq!(documents_kept(&m), 1, "with its document");
}

// -- The three answers of the stage test that are kept on purpose ---------------------------------------------------------------------------

#[test]
fn a_held_stage_whose_creation_row_is_gone_counts_as_older_than_the_drop() {
    // A restore leaves a member without `REGISTER_STAGES` (a backup does not carry it): the member cannot say the stage is not older than
    // the drop, and takes the drop as it always did, as a logical drop, though the stage is created above it (P1's case, on a row-less stage).
    let (life, _ds) = a_life_and_a_later_write();
    let created =
        life.iter().find(|e| e.kind == OpKind::CreateCollection).unwrap().stamp.hlc.wall_ms;
    let (x, _dx) = engine();
    let drop = drop_of(&x, created + 1);
    let w = world(); // the stage is created above the drop
    assert!(w.creation.stamp > drop.stamp, "premise: the stage is created above the drop");
    let (j, _dj) = engine();
    window(&j, &life);
    j.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    crate::register::remove_stage_row_for_test(&j, CollectionId::derive(DB, STAGE));
    window(&j, std::slice::from_ref(&drop));
    assert_eq!(
        register_events(&j),
        vec![drop.stamp],
        "a stage with no row counts as older: the drop is logical"
    );
}

#[test]
fn a_drop_below_the_stage_is_logical_when_a_drop_known_ahead_of_its_position_is_all_that_is_in_the_way()
 {
    // The stage is held and a later drop of the name is known ahead of its position (a parked LogicalDrop): an event of the swap's own
    // history counts as older whatever its stamp, so a drop stamped BELOW the stage, which no stage clause would take, is logical too.
    let (m, _dm) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let w = world();
    let (x, _dx) = engine();
    let below = drop_of(&x, orders.created.wall_ms + 1);
    let ahead = drop_of(&x, w.creation.stamp.hlc.wall_ms + 10);
    assert!(
        below.stamp.hlc < w.creation.stamp.hlc && w.creation.stamp.hlc < ahead.stamp.hlc,
        "premise: one drop below the stage, one above it"
    );
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap(); // the stage is held
    m.apply_batch(std::slice::from_ref(&ahead)).unwrap(); // known ahead of its position
    assert_eq!(
        parked_events(&m),
        vec![ahead.stamp],
        "premise: the later drop is known ahead of its position"
    );
    window(&m, std::slice::from_ref(&below));
    assert!(
        register_events(&m).contains(&below.stamp),
        "the drop below the stage is logical: a parked drop counts as the swap's history"
    );
}

#[test]
fn a_drop_of_a_name_whose_identity_is_absent_and_whose_stage_is_held_is_a_logical_drop() {
    // The identity is gone (or was never made here) and a stage of the name is held: the drop is judged against the register as it stands,
    // which is what the identity-absent seed does, and no life is left for it to bury.
    let w = world();
    let (x, _dx) = engine();
    let drop = drop_of(&x, w.creation.stamp.hlc.wall_ms + 1);
    let (m, _dm) = engine();
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    assert!(m.get_collection(DB, "orders").is_err(), "premise: no identity on this member");
    window(&m, std::slice::from_ref(&drop));
    assert_eq!(register_events(&m), vec![drop.stamp], "the drop is a register event of the name");
}

#[test]
fn a_drop_below_a_held_stage_of_a_name_whose_identity_is_absent_is_a_logical_drop_whatever_the_stages_stamp()
 {
    // The twin of the test above, with the stage created ABOVE the drop: the stage clause is false, and only the identity's absence
    // answers yes. The first event of the name is judged against the drop as it stands (the identity-absent seed does the same).
    let w = world();
    let (x, _dx) = engine();
    let drop = drop_of(&x, w.creation.stamp.hlc.wall_ms - 1);
    assert!(drop.stamp < w.creation.stamp, "premise: the stage is created above the drop");
    let (m, _dm) = engine();
    m.apply_batch(std::slice::from_ref(&w.creation)).unwrap();
    assert!(m.get_collection(DB, "orders").is_err(), "premise: no identity on this member");
    window(&m, std::slice::from_ref(&drop));
    assert_eq!(register_events(&m), vec![drop.stamp], "the drop is a register event of the name");
}
