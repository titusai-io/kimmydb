//! The recreate of a swapped name (ADR-272, decision 6; Q6.4 item 1c): a `CreateCollection` of a name that has register events is
//! a register event, a `Create`, through the one door under rule B and settled in the same transaction. The recreate makes a NEW
//! physical collection named by its own event (`orders.__rc.<stamp>`), born at that stamp; the name answers from it once the
//! event is visible. Judged by the old path (the identity's name is free again, so the create makes `orders` itself) the name is
//! dropped in the register and the collection the client just made is invisible, and on another member the order of arrival
//! decides which of the two answers.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionCreate, CollectionId, CollectionRef, Hlc, MergePolicy, OpKind,
    OplogEntry, Stamp, VersionVector,
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

fn absent(e: &Engine) -> bool {
    matches!(
        e.resolve(DB, "orders"),
        Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound { .. }))
    )
}

fn register_events(e: &Engine) -> Vec<Stamp> {
    let txn = e.begin_write(WriterHolder::Ddl).unwrap();
    let mut events: Vec<Stamp> = crate::register::timeline(&txn, DB, "orders")
        .unwrap()
        .events()
        .iter()
        .map(|(s, _)| *s)
        .collect();
    txn.abort().unwrap();
    events.sort();
    events
}

/// A member holding `orders` with a document.
fn with_orders() -> (Engine, tempfile::TempDir) {
    let (m, d) = with_empty_orders();
    let orders = m.get_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    (m, d)
}

/// A member holding `orders` and nothing in it, so that no rows stand between the name and its recreation.
fn with_empty_orders() -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    m.create_collection(DB, "orders").unwrap();
    (m, d)
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

/// The creation of the recreate physical named by the event stamped at `wall_ms` of `by`: what a member's `createCollection` mints.
fn recreate_of(by: &Engine, wall_ms: u64) -> OplogEntry {
    let stamp = Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch());
    let physical = kimmy_core::recreation_name("orders", &stamp);
    crate::engine::ddl_entry(
        stamp,
        OpKind::CreateCollection,
        CollectionId::derive(DB, &physical),
        &CollectionCreate {
            db: DB.into(),
            name: physical,
            merge_policy: MergePolicy::Document,
            validator: None,
            join_retention_ms: None,
            loss_limits: None,
            stage: None,
        },
    )
    .unwrap()
}

/// The author of a swap, its drop and the recreate, through the product's own paths: the stage and its creation, the binding
/// applied in a window, the local drop of the name and the local create of it. Every member that is to receive the entries was
/// built BEFORE this (so no stamp of the author sits below the life of a member), and the author's clock is moved past all of them.
struct Author {
    a: Engine,
    stage_creation: OplogEntry,
    bind: OplogEntry,
    drop: OplogEntry,
    recreate: OplogEntry,
    _dir: tempfile::TempDir,
}

fn author(members: &[&Engine]) -> Author {
    author_of(members, with_orders())
}

fn author_of(members: &[&Engine], own: (Engine, tempfile::TempDir)) -> Author {
    let (a, dir) = own;
    for m in members.iter().copied().chain([&a]) {
        let life = m.get_collection(DB, "orders").unwrap().created;
        a.advance_clock_past(&Stamp::new(life, m.node_id(), m.epoch()));
    }
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    let stage_creation = everything(&a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, STAGE)
        })
        .unwrap();
    let bind = bind_of(&a, stage_creation.stamp.hlc.wall_ms + 1);
    window(&a, std::slice::from_ref(&bind));
    assert!(a.drop_collection(DB, "orders").unwrap(), "premise: the name was dropped");
    assert!(absent(&a), "premise: the name is absent");
    a.create_collection(DB, "orders")
        .expect("a create of a swapped name that was dropped is a recreate, and succeeds");
    let entries = everything(&a);
    let drop = entries.iter().find(|e| e.kind == OpKind::DropCollection).cloned().unwrap();
    let recreate = entries
        .iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection
                && e.collection != CollectionId::derive(DB, STAGE)
                && e.stamp > drop.stamp
        })
        .cloned()
        .expect("premise: the create of the name minted an entry above the drop");
    Author { a, stage_creation, bind, drop, recreate, _dir: dir }
}

fn physical_of(entry: &OplogEntry) -> String {
    let body: CollectionCreate =
        bson::deserialize_from_slice(entry.body.as_ref().unwrap()).unwrap();
    body.name
}

/// `n` members built first, then the author, then the stage's creation delivered to each.
fn members_then_author(n: usize) -> (Author, Vec<(Engine, tempfile::TempDir)>) {
    let members: Vec<_> = (0..n).map(|_| with_orders()).collect();
    let refs: Vec<&Engine> = members.iter().map(|(m, _)| m).collect();
    let author = author(&refs);
    for (m, _) in &members {
        m.apply_batch(std::slice::from_ref(&author.stage_creation)).unwrap();
    }
    (author, members)
}

#[test]
fn a_create_of_a_dropped_swapped_name_is_a_recreate_the_name_answers_from() {
    let w = author(&[]);
    let a = &w.a;
    let resolved = a.resolve(DB, "orders").unwrap();
    assert!(
        kimmy_core::is_recreation_name(&resolved.physical),
        "the name answers from a recreation's physical, not from a collection named like it: {}",
        resolved.physical
    );
    assert_eq!(resolved.physical, physical_of(&w.recreate), "named by its own event");
    assert!(
        a.get_collection(DB, "orders").is_err(),
        "no collection is named like the logical name"
    );
    let id = a.insert(&resolved.meta, doc! { "_id": 7, "v": "new" }).unwrap();
    let again = a.resolve(DB, "orders").unwrap();
    assert!(
        a.get(&again.meta, &id).unwrap().is_some(),
        "a write to the recreated name is read back through the name"
    );
    assert_eq!(
        register_events(a),
        vec![w.bind.stamp, w.drop.stamp, w.recreate.stamp],
        "the binding, the drop and the recreate: three register events"
    );
    assert!(a.get_collection(DB, STAGE).is_err(), "the stage the drop buried is gone");
}

#[test]
fn the_recreate_of_a_name_whose_identity_holds_no_rows_is_visible_too() {
    // With nothing left of the identity to purge, the old path makes `orders` itself again; the register still says the name was
    // dropped, and the collection the client just made answers to nothing.
    let w = author_of(&[], with_empty_orders());
    let resolved = w.a.resolve(DB, "orders").expect("the name answers after its recreate");
    assert!(kimmy_core::is_recreation_name(&resolved.physical), "{}", resolved.physical);
    assert!(
        w.a.get_collection(DB, "orders").is_err(),
        "and no collection is named like the logical name"
    );
}

#[test]
fn a_create_of_a_name_that_answers_is_refused_as_it_always_was() {
    let w = author(&[]);
    // the recreated name answers: a second create is a conflict, not a second physical
    let again = w.a.create_collection(DB, "orders");
    assert!(
        matches!(again, Err(crate::StorageError::Core(kimmy_core::Error::CollectionExists { .. }))),
        "a create of a name that answers is a conflict: {again:?}"
    );
    assert_eq!(register_events(&w.a).len(), 3, "and no event was made");
}

#[test]
fn members_that_receive_the_swap_the_drop_and_the_recreate_in_any_order_end_alike() {
    let (w, members) = members_then_author(3);
    let (b, c, d) = (&members[0].0, &members[1].0, &members[2].0);
    // B: the binding, the drop, the recreate
    window(b, std::slice::from_ref(&w.bind));
    window(b, std::slice::from_ref(&w.drop));
    window(b, std::slice::from_ref(&w.recreate));
    // C: the drop FIRST (the stage held, no event yet), then the binding, then the recreate
    window(c, std::slice::from_ref(&w.drop));
    window(c, std::slice::from_ref(&w.bind));
    window(c, std::slice::from_ref(&w.recreate));
    // D: the recreate ahead of its position (no window behind it), then the rest in position
    d.apply_batch(std::slice::from_ref(&w.recreate)).unwrap();
    assert_eq!(
        d.resolve(DB, "orders").unwrap().physical,
        "orders",
        "known, not in effect: the name still answers from what it served"
    );
    window(d, std::slice::from_ref(&w.bind));
    window(d, std::slice::from_ref(&w.drop));
    window(d, std::slice::from_ref(&w.recreate));
    let want = physical_of(&w.recreate);
    for (name, m) in [("B", b), ("C", c), ("D", d)] {
        let resolved = m.resolve(DB, "orders");
        assert!(resolved.is_ok(), "{name}: the name answers: {resolved:?}");
        assert_eq!(resolved.unwrap().physical, want, "{name}: from the recreate's physical");
        assert!(
            m.get_collection(DB, "orders").is_err(),
            "{name}: no collection named like the logical name"
        );
        assert!(m.get_collection(DB, STAGE).is_err(), "{name}: the stage is gone");
        assert_eq!(
            register_events(m),
            vec![w.bind.stamp, w.drop.stamp, w.recreate.stamp],
            "{name}: the same three register events"
        );
    }
}

#[test]
fn a_recreate_stamped_below_the_drop_is_history_in_either_order() {
    // The recreate at C0 < D. Whether it arrives before or after the drop, the drop stands and the name is absent: the recreate
    // physical, born at C0, is buried by the drop above it.
    let (w, members) = members_then_author(2);
    let (b, c) = (&members[0].0, &members[1].0);
    let (x, _dx) = engine();
    let base = w.bind.stamp.hlc.wall_ms;
    let below = recreate_of(&x, base + 1);
    let drop = drop_of(&x, base + 10);
    // B: the recreate before the drop
    window(b, std::slice::from_ref(&w.bind));
    window(b, std::slice::from_ref(&below));
    assert_eq!(
        b.resolve(DB, "orders").unwrap().physical,
        physical_of(&below),
        "premise: before the drop the recreate is the newest event and answers"
    );
    window(b, std::slice::from_ref(&drop));
    // C: the drop before the recreate
    window(c, std::slice::from_ref(&w.bind));
    window(c, std::slice::from_ref(&drop));
    window(c, std::slice::from_ref(&below));
    for (name, m) in [("B", b), ("C", c)] {
        assert!(absent(m), "{name}: the drop stands");
        assert!(
            m.get_collection(DB, &physical_of(&below)).is_err(),
            "{name}: the recreate it buries is gone"
        );
        assert!(m.get_collection(DB, STAGE).is_err(), "{name}: so is the stage");
        assert_eq!(
            register_events(m),
            vec![w.bind.stamp, below.stamp, drop.stamp],
            "{name}: the same three events"
        );
    }
}

#[test]
fn a_grant_that_covers_a_parked_recreate_puts_it_in_effect_and_settles_in_the_same_transaction() {
    let (w, members) = members_then_author(1);
    let b = &members[0].0;
    window(b, std::slice::from_ref(&w.bind));
    window(b, std::slice::from_ref(&w.drop));
    assert!(absent(b), "premise: the name was dropped");
    b.apply_batch(std::slice::from_ref(&w.recreate)).unwrap(); // ahead of its position: information
    assert!(absent(b), "premise: not in effect");
    let mut granted = VersionVector::default();
    granted.observe(w.recreate.stamp);
    b.absorb_version_vector(&granted).unwrap();
    assert_eq!(
        b.resolve(DB, "orders").unwrap().physical,
        physical_of(&w.recreate),
        "the grant released the recreate and the name answers from it"
    );
}

// ---- Item 1d: what a client can list is the names it uses ----

fn listed(e: &Engine) -> Vec<String> {
    let mut names: Vec<String> =
        e.list_logical_collections(DB).unwrap().into_iter().map(|c| c.name).collect();
    names.sort();
    names
}

#[test]
fn a_name_bound_to_a_stage_is_listed_under_its_own_name_and_the_stage_is_not() {
    // Before the flip: the identity stands, a stage of it is held and is not listed.
    let (m, _d) = with_orders();
    let (a, _da) = engine();
    a.create_stage(DB, STAGE, MergePolicy::Document, Hlc::new(physical_now_ms() + 60_000, 0))
        .unwrap();
    let creation = everything(&a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, STAGE)
        })
        .unwrap();
    m.apply_batch(std::slice::from_ref(&creation)).unwrap();
    assert_eq!(listed(&m), vec!["orders".to_string()], "premise: the stage is held and hidden");
    // After the flip the identity is buried and the stage serves: the name is listed, the stage is not.
    let wall = physical_now_ms().max(creation.stamp.hlc.wall_ms) + 1;
    let bind = bind_of(&a, wall);
    window(&m, std::slice::from_ref(&bind));
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, STAGE, "premise: the flip is visible");
    assert_eq!(
        listed(&m),
        vec!["orders".to_string()],
        "the name that answers is listed, under its own name"
    );
    // After the drop of the name: nothing.
    assert!(m.drop_collection(DB, "orders").unwrap());
    assert!(listed(&m).is_empty(), "a dropped name is not listed: {:?}", listed(&m));
}

#[test]
fn a_recreated_name_is_listed_under_its_own_name_and_its_physical_is_not() {
    let w = author(&[]);
    assert_eq!(listed(&w.a), vec!["orders".to_string()], "{:?}", listed(&w.a));
}

#[test]
fn a_definition_addressed_to_a_swapped_name_finds_nothing_until_q6_5() {
    // A PIN of today's behaviour, not of the intended one. Decision 6 says an index, a validator, a vector configuration and a TTL
    // address the LOGICAL name and are stamped registers joined across the physicals of the chain; that is Q6.5 ("definition
    // registers"). Until then these entry points look the name up with `get_collection`, which finds a physical by its own name,
    // so after a flip (the identity buried, the stage serving) they answer CollectionNotFound. Nothing is lost and nothing
    // diverges. Q6.5's failing-first commit flips this test to "the definition lands on the physical the name answers from".
    let (m, _d) = with_orders();
    let (a, _da) = engine();
    a.create_stage(DB, STAGE, MergePolicy::Document, Hlc::new(physical_now_ms() + 60_000, 0))
        .unwrap();
    let creation = everything(&a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, STAGE)
        })
        .unwrap();
    m.apply_batch(std::slice::from_ref(&creation)).unwrap();
    window(
        &m,
        std::slice::from_ref(&bind_of(&a, physical_now_ms().max(creation.stamp.hlc.wall_ms) + 1)),
    );
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, STAGE, "premise: the flip is visible");
    let created =
        m.create_index(DB, "orders", vec![kimmy_core::IndexField::ascending("v")], false, None);
    assert!(
        matches!(
            created,
            Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound { .. }))
        ),
        "an index on a swapped name: {created:?}"
    );
    let validator = m.set_validator(DB, "orders", None);
    assert!(
        matches!(
            validator,
            Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound { .. }))
        ),
        "a validator on a swapped name: {validator:?}"
    );
}
