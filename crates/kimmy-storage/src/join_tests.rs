//! The join (ADR-272, decision 6, rule 3; Q6.5-2, series B1): when a physical becomes visible, by a binding or by a recreation, the
//! definitions of every physical that settle buries as bound out (the identity, or a physical with its own binding or recreation in effect)
//! are merged into it, in the transaction of the flip, with their ORIGINAL stamps: an index, its tombstone (re-keyed under the new
//! physical's id) and the validator. Rows a, a2, b, c and e2 of `Q65-model-rows.md`, and the recreation that becomes visible before the
//! older drop arrives (the order the model's random grid refuted a binding-only join with).
//!
//! The fixture is the swap of `definition_door_tests`, here with documents in the stage (the stage's own rows are what a join builds
//! over) and a member that has not heard of the swap (`x`), whose definitions reach the others as entries addressed to the logical name.
//! A stage's own definitions are written by hand through the entries Q6.7's creation will carry: a replicated `CreateIndex` addressed to
//! the stage, which builds over the stage's rows as a creation does.

use std::sync::Arc;

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, Enforcement, Hlc, IndexCreate, IndexDrop, IndexField,
    MergePolicy, OpKind, OplogEntry, Stamp, ValidatorSpec, VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
/// A stage that is never bound: its name sorts before the bound one's.
const UNBOUND: &str = "orders.__stg.a0";
const BOUND: &str = "orders.__stg.a1";

fn engine() -> (Arc<Engine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    (Arc::new(Engine::open(&dir.path().join("kimmy.redb")).expect("an engine opens")), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .expect("the oplog is readable")
        .entries
}

fn entries_of(e: &Engine, kind: OpKind) -> Vec<OplogEntry> {
    everything(e).into_iter().filter(|entry| entry.kind == kind).collect()
}

fn try_window(b: &Engine, entries: &[OplogEntry]) -> crate::Result<SyncOutcome> {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector()?;
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
    )?;
    Ok(outcome)
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    try_window(b, entries).expect("the window applies")
}

fn bind_of(by: &Engine, wall_ms: u64, physical: &str) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: physical.into(),
            })
            .expect("the binding encodes"),
        ),
        meta: None,
    }
}

/// The author of the swap: both stages and the binding of `BOUND`, not yet delivered to anyone.
struct World {
    creations: Vec<OplogEntry>,
    bind: OplogEntry,
    /// A later binding, of the stage `UNBOUND`: the second flip of row b.
    bind_second: OplogEntry,
    _author: (Engine, tempfile::TempDir),
}

fn world() -> World {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let author = Engine::open(&dir.path().join("kimmy.redb")).expect("the author's engine opens");
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    author
        .create_stage(DB, UNBOUND, MergePolicy::Document, deadline)
        .expect("the unbound stage is created");
    author
        .create_stage(DB, BOUND, MergePolicy::Document, deadline)
        .expect("the bound stage is created");
    let creations: Vec<OplogEntry> = everything(&author)
        .into_iter()
        .filter(|e| e.kind == OpKind::CreateCollection)
        .filter(|e| {
            e.collection == CollectionId::derive(DB, UNBOUND)
                || e.collection == CollectionId::derive(DB, BOUND)
        })
        .collect();
    assert_eq!(creations.len(), 2, "premise: both stages' creations");
    let wall = creations
        .iter()
        .map(|e| e.stamp.hlc.wall_ms)
        .max()
        .expect("two creations")
        .max(physical_now_ms())
        + 10;
    let bind = bind_of(&author, wall, BOUND);
    let bind_second = bind_of(&author, wall + 10, UNBOUND);
    World { creations, bind, bind_second, _author: (author, dir) }
}

/// A member holding `orders` with two documents and both stages' creations; the stage `BOUND` holds three documents of its own (the
/// output of the job). The binding is not known to it.
fn member(w: &World) -> (Arc<Engine>, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").expect("the identity is created");
    m.insert(&orders, doc! { "_id": 1, "v": 1, "t": 1 }).expect("the identity takes a document");
    m.insert(&orders, doc! { "_id": 2, "v": 2, "t": 2 }).expect("the identity takes a document");
    m.apply_batch(&w.creations).expect("the stages' creations apply");
    let stage = m.get_collection(DB, BOUND).expect("premise: the bound stage is held");
    for id in 10..13 {
        m.insert(&stage, doc! { "_id": id, "v": id, "t": id })
            .expect("the stage takes a row of the job's output");
    }
    (m, d)
}

/// How the binding reaches the member.
#[derive(Clone, Copy, Debug)]
enum Route {
    /// In a window.
    Window,
    /// Known ahead of its position, then released by a window.
    ParkedThenWindow,
    /// Known ahead of its position, then released by a snapshot grant.
    ParkedThenGrant,
}

const ROUTES: [Route; 3] = [Route::Window, Route::ParkedThenWindow, Route::ParkedThenGrant];

fn granted(stamp: Stamp) -> VersionVector {
    let mut vector = VersionVector::default();
    vector.observe(stamp);
    vector
}

/// The first half of a route: what happens when the binding is learned.
fn learn(m: &Engine, bind: &OplogEntry, route: Route) {
    match route {
        Route::Window => {
            window(m, std::slice::from_ref(bind));
        }
        Route::ParkedThenWindow | Route::ParkedThenGrant => {
            m.apply_batch(std::slice::from_ref(bind))
                .expect("the binding is stored ahead of its position");
        }
    }
}

/// The second half: what makes it take effect, for a parked binding.
fn release(m: &Engine, bind: &OplogEntry, route: Route) {
    match route {
        Route::Window => {}
        Route::ParkedThenWindow => {
            window(m, std::slice::from_ref(bind));
        }
        Route::ParkedThenGrant => {
            m.absorb_version_vector(&granted(bind.stamp)).expect("the grant is absorbed")
        }
    }
}

fn flip(m: &Engine, bind: &OplogEntry, route: Route) {
    learn(m, bind, route);
    release(m, bind, route);
    assert_eq!(
        m.resolve(DB, "orders").expect("the name resolves").physical,
        BOUND,
        "premise: the flip is visible ({route:?})"
    );
}

fn field(path: &str) -> Vec<IndexField> {
    vec![IndexField::ascending(path)]
}

fn validator(tag: &str) -> ValidatorSpec {
    ValidatorSpec {
        schema: doc! { "bsonType": "object", "title": tag },
        action: Default::default(),
        level: Default::default(),
    }
}

/// The creation of an index on a stage as Q6.7's creation entry will carry it: a replicated `CreateIndex` addressed to the stage, minted by
/// `by` (which holds the stage too) at `wall_ms`; it builds over the stage's rows as any replicated creation does.
fn stage_index_entry(by: &Engine, stage: &str, name: &str, path: &str, wall_ms: u64) -> OplogEntry {
    let index = kimmy_core::IndexMeta {
        id: kimmy_core::IndexMeta::derive_id(name),
        name: name.into(),
        fields: field(path),
        unique: false,
        enforcement: Enforcement::Local,
        multikey: false,
        expire_after_secs: None,
        partial_filter: None,
        created: None,
    };
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::CreateIndex,
        collection: CollectionId::derive(DB, stage),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&IndexCreate { db: DB.into(), collection: stage.into(), index })
                .expect("the creation body encodes"),
        ),
        meta: None,
    }
}

fn drop_index_entry(by: &Engine, name: &str, wall_ms: u64, node_of: Option<&Engine>) -> OplogEntry {
    let from = node_of.unwrap_or(by);
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), from.node_id(), from.epoch()),
        kind: OpKind::DropIndex,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&IndexDrop {
                db: DB.into(),
                collection: "orders".into(),
                index: name.into(),
            })
            .expect("the drop body encodes"),
        ),
        meta: None,
    }
}

/// How many entries the index `name` holds under the physical `physical` (0 when it has no such index).
fn entries(m: &Engine, physical: &str, name: &str) -> usize {
    let coll = m.get_collection(DB, physical).expect("the physical is held");
    let Some(index) = coll.index(name) else { return 0 };
    crate::index::scan_range(
        m,
        WalkScope::Request,
        coll.id,
        index.id,
        &[],
        None,
        crate::index::Unkeyed::Exclude,
    )
    .expect("the index is readable")
    .len()
}

fn tombstone(m: &Engine, physical: &str, name: &str) -> Option<Stamp> {
    let coll = m.get_collection(DB, physical).expect("the physical is held");
    m.index_dropped_at(coll.id, kimmy_core::IndexMeta::derive_id(name))
        .expect("the tombstone is readable")
}

// -- Row a: a definition before and after the flip -----------------------------------------------------------------------------------------

#[test]
fn an_index_and_a_validator_made_on_the_old_physical_are_on_the_new_one_with_their_stamps_by_every_route()
 {
    for route in ROUTES {
        let w = world();
        let (m, _d) = member(&w);
        let held = m
            .create_index(DB, "orders", field("v"), false, None)
            .expect("premise: the identity gets an index");
        let spec = validator("one");
        let set = m
            .set_validator(DB, "orders", Some(spec.clone()))
            .expect("premise: the identity gets a validator");
        let stage = m.get_collection(DB, BOUND).expect("the stage is held");
        assert!(
            stage.indexes.is_empty() && stage.validator.is_none(),
            "premise: the stage holds no definition before the flip ({route:?})"
        );
        flip(&m, &w.bind, route);
        let on = m.get_collection(DB, BOUND).expect("the bound stage is held");
        let carried = on.index("v_1").unwrap_or_else(|| {
            panic!("{route:?}: the index of the old physical is on the new one")
        });
        assert_eq!(
            carried.created, held.created,
            "{route:?}: with its ORIGINAL stamp, not the flip's"
        );
        assert_eq!(
            entries(&m, BOUND, "v_1"),
            3,
            "{route:?}: built over the stage's three rows, not copied from the identity's two"
        );
        assert_eq!(on.validator.as_ref(), Some(&spec), "{route:?}: the validator is carried");
        assert_eq!(
            on.validator_changed, set.validator_changed,
            "{route:?}: with its original stamp"
        );
    }
}

#[test]
fn a_definition_that_arrives_while_the_binding_is_parked_lands_on_the_old_physical_and_the_release_carries_it()
 {
    let w = world();
    for route in [Route::ParkedThenWindow, Route::ParkedThenGrant] {
        let (m, _d) = member(&w);
        learn(&m, &w.bind, route);
        assert_eq!(
            m.resolve(DB, "orders").expect("the name resolves").physical,
            "orders",
            "premise: the binding is parked"
        );
        let made = m
            .create_index(DB, "orders", field("t"), false, None)
            .expect("the door finds the identity while the binding is parked");
        m.set_validator(DB, "orders", Some(validator("parked")))
            .expect("the validator door finds the identity too");
        release(&m, &w.bind, route);
        let on = m.get_collection(DB, BOUND).expect("the bound stage is held");
        let carried = on.index("t_1").unwrap_or_else(|| {
            panic!("{route:?}: the release carries the index made while it was parked")
        });
        assert_eq!(carried.created, made.created, "{route:?}: with its original stamp");
        assert_eq!(
            on.validator.as_ref(),
            Some(&validator("parked")),
            "{route:?}: and the validator"
        );
    }
}

#[test]
fn a_definition_stamped_below_the_flip_that_arrives_after_it_lands_on_the_new_physical() {
    // Row a, M2: the member that made the flip first meets entries of a member that had not heard of it, stamped below the flip.
    let (x, _dx) = engine();
    x.create_collection(DB, "orders")
        .expect("the member that never heard of the swap holds the name");
    x.create_index(DB, "orders", field("t"), false, None).expect("it makes an index");
    x.set_validator(DB, "orders", Some(validator("x"))).expect("and a validator");
    std::thread::sleep(std::time::Duration::from_millis(15));
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    let theirs: Vec<OplogEntry> = everything(&x)
        .into_iter()
        .filter(|e| matches!(e.kind, OpKind::CreateIndex | OpKind::ConfigureValidator))
        .collect();
    assert_eq!(theirs.len(), 2, "premise: an index and a validator");
    assert!(
        theirs.iter().all(|e| e.stamp < w.bind.stamp),
        "premise: both are stamped below the flip"
    );
    window(&m, &theirs);
    let on = m.get_collection(DB, BOUND).expect("the bound stage is held");
    assert!(
        on.index("t_1").is_some(),
        "the index stamped below the flip, met after it, is on the physical the name answers from"
    );
    assert_eq!(on.validator.as_ref(), Some(&validator("x")), "and so is the validator");
}

// -- Row a2: a drop during the job of an index the stage was created with ----------------------------------------------------------------

#[test]
fn an_index_the_stage_was_built_with_is_removed_by_the_join_when_the_identity_dropped_it_and_a_surviving_index_is_carried()
 {
    for route in ROUTES {
        let w = world();
        let (m, _d) = member(&w);
        // the stage was created with `v_1` (by hand: the entry Q6.7's creation will carry), over its three rows
        let (author, _da) = engine();
        author.apply_batch(&w.creations).expect("the author holds the stages");
        let early =
            w.creations.iter().map(|e| e.stamp.hlc.wall_ms).max().expect("two creations") + 1;
        window(&m, &[stage_index_entry(&author, BOUND, "v_1", "v", early)]);
        assert_eq!(
            entries(&m, BOUND, "v_1"),
            3,
            "premise: the stage's own index holds its three rows"
        );
        // the identity holds `v_1` too, dropped during the job, and a second index that survives
        m.create_index(DB, "orders", field("v"), false, None).expect("the identity gets v_1");
        let survivor = m
            .create_index(DB, "orders", field("t"), false, None)
            .expect("the identity gets a second index");
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(
            m.drop_index(DB, "orders", "v_1").expect("the drop runs"),
            "premise: the identity drops v_1"
        );
        flip(&m, &w.bind, route);
        let on = m.get_collection(DB, BOUND).expect("the bound stage is held");
        assert!(
            on.index("v_1").is_none(),
            "{route:?}: the stage's index was dropped during the job, so the join removes it"
        );
        assert_eq!(entries(&m, BOUND, "v_1"), 0, "{route:?}: and its entries go with it");
        assert!(
            tombstone(&m, BOUND, "v_1").is_some(),
            "{route:?}: the tombstone is under the stage's id"
        );
        assert_eq!(
            on.index("t_1").map(|i| i.created),
            Some(survivor.created),
            "{route:?}: the index that was never dropped is carried, with its stamp"
        );
        assert_eq!(entries(&m, BOUND, "t_1"), 3, "{route:?}: and built over the stage's rows");
    }
}

// -- Row b: a definition applied to a stage that then loses ----------------------------------------------------------------------------------

#[test]
fn a_definition_applied_to_a_stage_while_it_is_visible_is_on_the_next_stage_after_a_second_flip() {
    let w = world();
    let (m, _d) = member(&w);
    let second = m.get_collection(DB, UNBOUND).expect("premise: the second stage is held");
    for id in 20..22 {
        m.insert(&second, doc! { "_id": id, "v": id, "t": id })
            .expect("the second stage takes a row of the job's output");
    }
    flip(&m, &w.bind, Route::Window);
    let made = m
        .create_index(DB, "orders", field("t"), false, None)
        .expect("the door finds the first stage while it is visible");
    assert_eq!(
        entries(&m, BOUND, "t_1"),
        3,
        "premise: the definition is on the visible stage, built over its three rows"
    );
    let builds = m.index_builds_for_tests();
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(
        m.resolve(DB, "orders").expect("the name resolves").physical,
        UNBOUND,
        "premise: the second binding is visible"
    );
    assert!(m.get_collection(DB, BOUND).is_err(), "premise: the first stage lost and was buried");
    let on = m.get_collection(DB, UNBOUND).expect("the second stage is held");
    assert_eq!(
        on.index("t_1").map(|i| i.created),
        Some(made.created),
        "the definition made on the stage that then lost is on the stage that won, with its stamp"
    );
    assert_eq!(entries(&m, UNBOUND, "t_1"), 2, "built over the second stage's two rows");
    assert_eq!(m.index_builds_for_tests(), builds + 1, "one build for the one definition");
}

// -- Two definitions of one index name, and a tombstone older than the creation that stands -------------------------------------------------

#[test]
fn two_differing_definitions_of_one_index_name_go_to_the_later_creation_stamp_whichever_physical_holds_it()
 {
    // One physical holds `dup` on a field, the other holds `dup` on another: the later creation stamp wins (ADR-132's rule, the one a
    // replicated create is judged by), absolutely, in both directions and with the entries built over the stage's rows.
    for stage_is_later in [false, true] {
        let w = world();
        let (m, _d) = member(&w);
        let (author, _da) = engine();
        author.apply_batch(&w.creations).expect("the author holds the stages");
        let early =
            w.creations.iter().map(|e| e.stamp.hlc.wall_ms).max().expect("two creations") + 1;
        std::thread::sleep(std::time::Duration::from_millis(5));
        let on_identity = m
            .create_index(DB, "orders", field("v"), false, Some("dup".into()))
            .expect("the identity gets dup on v");
        let identity_stamp = on_identity.created.expect("a stamped index");
        let stage_wall = if stage_is_later { identity_stamp.hlc.wall_ms + 20 } else { early };
        let on_stage = stage_index_entry(&author, BOUND, "dup", "t", stage_wall);
        window(&m, std::slice::from_ref(&on_stage));
        assert_eq!(
            on_stage.stamp > identity_stamp,
            stage_is_later,
            "premise: the stage's definition is the later one exactly when stage_is_later"
        );
        flip(&m, &w.bind, Route::Window);
        let on = m.get_collection(DB, BOUND).expect("the bound stage is held");
        let standing = on.index("dup").unwrap_or_else(|| {
            panic!("stage_is_later={stage_is_later}: dup stands on the new physical")
        });
        let (path, stamp) =
            if stage_is_later { ("t", on_stage.stamp) } else { ("v", identity_stamp) };
        assert_eq!(
            standing.fields.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
            vec![path],
            "stage_is_later={stage_is_later}: the later creation's definition wins, absolutely"
        );
        assert_eq!(
            standing.created,
            Some(stamp),
            "stage_is_later={stage_is_later}: with the winner's creation stamp"
        );
        assert_eq!(
            entries(&m, BOUND, "dup"),
            3,
            "stage_is_later={stage_is_later}: built over the stage's rows under the winner's definition"
        );
    }
}

#[test]
fn a_tombstone_older_than_the_creation_that_stands_on_the_stage_does_not_end_it_and_the_older_creation_is_still_history()
 {
    // The identity held an index made by a peer (stamp a) and dropped it (stamp b). The stage was created with its own creation of the same
    // index, stamped c above b: it stands. The tombstone is carried under the stage's id (a re-served creation at a is history there).
    let (x, _dx) = engine();
    x.create_collection(DB, "orders").expect("the member that made the index holds the name");
    x.create_index(DB, "orders", field("t"), false, None).expect("it makes t_1");
    let old = entries_of(&x, OpKind::CreateIndex);
    assert_eq!(old.len(), 1, "premise: the creation at a");
    std::thread::sleep(std::time::Duration::from_millis(15));
    let w = world();
    let (m, _d) = member(&w);
    window(&m, &old);
    assert!(
        m.get_collection(DB, "orders").expect("held").index("t_1").is_some(),
        "premise: the identity holds t_1"
    );
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(
        m.drop_index(DB, "orders", "t_1").expect("the drop runs"),
        "premise: the identity drops it at b"
    );
    let dropped_at =
        tombstone(&m, "orders", "t_1").expect("premise: the tombstone under the identity");
    let (author, _da) = engine();
    author.apply_batch(&w.creations).expect("the author holds the stages");
    let on_stage = stage_index_entry(&author, BOUND, "t_1", "t", dropped_at.hlc.wall_ms + 5);
    window(&m, std::slice::from_ref(&on_stage));
    assert!(
        on_stage.stamp > dropped_at,
        "premise: the stage's creation is stamped above the tombstone"
    );
    assert_eq!(entries(&m, BOUND, "t_1"), 3, "premise: the stage's own t_1 holds its three rows");
    flip(&m, &w.bind, Route::Window);
    let on = m.get_collection(DB, BOUND).expect("the bound stage is held");
    assert_eq!(
        on.index("t_1").map(|i| i.created),
        Some(Some(on_stage.stamp)),
        "the creation stamped above the older tombstone stands, with its own stamp"
    );
    assert_eq!(
        tombstone(&m, BOUND, "t_1"),
        Some(dropped_at),
        "the older tombstone is carried under the stage's id, below the creation"
    );
    window(&m, &old);
    assert_eq!(
        m.get_collection(DB, BOUND).expect("held").index("t_1").map(|i| i.created),
        Some(Some(on_stage.stamp)),
        "the creation at a, served again, is history under the stage and replaces nothing"
    );
}

// -- Row c: a drop of an index, then the older creation served again --------------------------------------------------------------------

#[test]
fn a_drop_of_an_index_before_the_flip_leaves_its_tombstone_under_the_new_physical_and_the_older_creation_is_history_there()
 {
    for route in [Route::Window, Route::ParkedThenWindow] {
        let (x, _dx) = engine();
        x.create_collection(DB, "orders").expect("the member that made the index holds the name");
        x.create_index(DB, "orders", field("v"), false, None).expect("it makes v_1");
        let creation = entries_of(&x, OpKind::CreateIndex);
        assert_eq!(creation.len(), 1, "premise: the creation the oplog can serve again");
        std::thread::sleep(std::time::Duration::from_millis(15));
        let w = world();
        let (m, _d) = member(&w);
        window(&m, &creation);
        assert!(
            m.get_collection(DB, "orders").expect("the identity is held").index("v_1").is_some(),
            "premise: the identity holds v_1"
        );
        m.create_index(DB, "orders", field("t"), false, None)
            .expect("a second index, which survives");
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(
            m.drop_index(DB, "orders", "v_1").expect("the drop runs"),
            "premise: v_1 is dropped before the flip"
        );
        let dropped_at =
            tombstone(&m, "orders", "v_1").expect("premise: the tombstone is under the identity");
        flip(&m, &w.bind, route);
        assert_eq!(
            tombstone(&m, BOUND, "v_1"),
            Some(dropped_at),
            "{route:?}: the tombstone is under the new physical's id, at the drop's stamp"
        );
        assert!(
            m.get_collection(DB, BOUND).expect("held").index("t_1").is_some(),
            "{route:?}: the surviving index is carried"
        );
        let before = m.index_builds_for_tests();
        window(&m, &creation);
        assert!(
            m.get_collection(DB, BOUND).expect("held").index("v_1").is_none(),
            "{route:?}: the older creation served again is history: not rebuilt"
        );
        assert_eq!(m.index_builds_for_tests(), before, "{route:?}: and no build ran for it");
    }
}

// -- Row e2: a create and a drop of one index name at one hlc, by two origins ------------------------------------------------------------

#[test]
fn a_create_and_a_drop_at_one_hlc_by_two_origins_end_the_same_on_the_apply_and_the_join_in_both_node_orders()
 {
    // The higher full stamp wins, on every member and in every order, the same on the apply and on the join (ADR-132's comparison).
    let (p, _dp) = engine();
    let (q, _dq) = engine();
    let (lo, hi) = if p.node_id() < q.node_id() { (&p, &q) } else { (&q, &p) };
    for create_is_higher in [true, false] {
        let (creator, dropper) = if create_is_higher { (hi, lo) } else { (lo, hi) };
        let wall = physical_now_ms() + 30;
        let create = {
            let mut e = stage_index_entry(creator, "orders", "i5", "v", wall);
            e.collection = CollectionId::derive(DB, "orders");
            e
        };
        let drop = drop_index_entry(dropper, "i5", wall, None);
        for order in [
            "create then flip then drop",
            "drop then create then flip",
            "create then drop then flip",
        ] {
            let w = world();
            let (m, _d) = member(&w);
            let mut late_flip = w.bind.clone();
            late_flip.stamp =
                Stamp::new(Hlc::new(wall + 20, 0), w.bind.stamp.node, w.bind.stamp.epoch);
            match order {
                "create then flip then drop" => {
                    window(&m, std::slice::from_ref(&create));
                    window(&m, std::slice::from_ref(&late_flip));
                    window(&m, std::slice::from_ref(&drop));
                }
                "drop then create then flip" => {
                    window(&m, std::slice::from_ref(&drop));
                    window(&m, std::slice::from_ref(&create));
                    window(&m, std::slice::from_ref(&late_flip));
                }
                _ => {
                    window(&m, std::slice::from_ref(&create));
                    window(&m, std::slice::from_ref(&drop));
                    window(&m, std::slice::from_ref(&late_flip));
                }
            }
            let on = m.get_collection(DB, BOUND).expect("the bound stage is held");
            let present = on.index("i5").is_some();
            assert_eq!(
                present, create_is_higher,
                "{order}, create_is_higher={create_is_higher}: the higher full stamp wins, absolutely (present={present})"
            );
            if !create_is_higher {
                assert_eq!(
                    tombstone(&m, BOUND, "i5"),
                    Some(drop.stamp),
                    "{order}: the drop's tombstone is under the stage's id"
                );
            }
        }
    }
}

// -- The recreation that becomes visible before the older drop arrives --------------------------------------------------------------------

#[test]
fn a_recreation_made_visible_before_the_older_drop_arrives_holds_what_the_buried_physical_held() {
    // Model refinement (REPORT12): the visible event names a physical N whether it is a binding or a recreation. Order: the definition
    // above the drop (it lands on the bound physical), the recreation (which buries it), then the older drop. The order drop, definition,
    // recreation is the post-drop row's (B2, row d).
    let w = world();
    let (m, _d) = member(&w);
    window(&m, std::slice::from_ref(&w.bind));
    let (a, _da) = engine();
    a.create_collection(DB, "orders").expect("the author holds the name");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let drop_wall = w.bind.stamp.hlc.wall_ms + 10;
    let drop = OplogEntry {
        stamp: Stamp::new(Hlc::new(drop_wall, 0), a.node_id(), a.epoch()),
        kind: OpKind::DropCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders"))
                .expect("the drop encodes"),
        ),
        meta: None,
    };
    let def = stage_index_entry(&a, "orders", "i2", "v", drop_wall + 5);
    let rc_stamp = Stamp::new(Hlc::new(drop_wall + 10, 0), a.node_id(), a.epoch());
    let physical = kimmy_core::recreation_name("orders", &rc_stamp);
    let recreate = crate::engine::ddl_entry(
        rc_stamp,
        OpKind::CreateCollection,
        CollectionId::derive(DB, &physical),
        &kimmy_core::CollectionCreate {
            db: DB.into(),
            name: physical.clone(),
            merge_policy: MergePolicy::Document,
            validator: None,
            join_retention_ms: None,
            loss_limits: None,
            stage: None,
        },
    )
    .expect("the recreation's creation encodes");
    window(&m, std::slice::from_ref(&def));
    assert!(
        m.get_collection(DB, BOUND).expect("held").index("i2").is_some(),
        "premise: the definition landed on the bound physical"
    );
    window(&m, std::slice::from_ref(&recreate));
    window(&m, std::slice::from_ref(&drop));
    let resolved = m.resolve(DB, "orders").expect("the name answers from the recreation");
    assert_eq!(resolved.physical, physical, "premise: the recreation is visible");
    assert!(
        m.get_collection(DB, &physical).expect("the recreation is held").index("i2").is_some(),
        "the recreation holds the definition the bound physical held when the recreation buried it"
    );
}

// -- Violations and an aborted flip ---------------------------------------------------------------------------------------------------------

#[test]
fn a_unique_index_made_during_the_job_is_built_in_the_flip_and_the_violations_are_reported_once() {
    let w = world();
    let (m, _d) = member(&w);
    let stage = m.get_collection(DB, BOUND).expect("the stage is held");
    m.insert(&stage, doc! { "_id": 20, "v": 10, "t": 0 })
        .expect("premise: a second document of the output with the same v as _id 10");
    m.create_index(DB, "orders", field("v"), true, None)
        .expect("premise: the identity holds a unique index over its distinct rows");
    let before = m.unique_violations();
    flip(&m, &w.bind, Route::Window);
    let on = m.get_collection(DB, BOUND).expect("the bound stage is held");
    assert!(
        on.index("v_1").is_some_and(|i| i.unique),
        "the unique index stands on the stage although its rows violate it"
    );
    assert_eq!(
        m.unique_violations() - before,
        1,
        "exactly one violation reported, for the one shared key"
    );
    let live = m
        .live_unique_violations(&on, None, WalkScope::Request)
        .expect("the violations are readable");
    assert_eq!(live.len(), 1, "the violations route names it: {live:?}");
}

#[test]
fn a_flip_that_fails_after_its_join_joins_nothing_and_the_redelivery_joins_once() {
    let w = world();
    let (m, _d) = member(&w);
    m.create_index(DB, "orders", field("v"), false, None)
        .expect("premise: the identity gets an index");
    let builds = m.index_builds_for_tests();
    let passes = crate::sync::race_hooks::settle_passes();
    crate::sync::race_hooks::fail_after_the_next_settle();
    let attempt = try_window(&m, std::slice::from_ref(&w.bind));
    assert!(attempt.is_err(), "premise: the transaction failed: {attempt:?}");
    assert_eq!(
        crate::sync::race_hooks::settle_passes(),
        passes + 1,
        "premise: the pass ran before it failed"
    );
    assert_eq!(
        m.index_builds_for_tests(),
        builds + 1,
        "premise: the join built the index in the aborted attempt"
    );
    assert_eq!(
        m.resolve(DB, "orders").expect("the name resolves").physical,
        "orders",
        "premise: nothing of the flip was committed"
    );
    assert!(
        m.get_collection(DB, BOUND).expect("held").index("v_1").is_none(),
        "nothing of the join is on the new physical"
    );
    window(&m, std::slice::from_ref(&w.bind));
    assert!(
        m.get_collection(DB, BOUND).expect("held").index("v_1").is_some(),
        "the redelivery joined"
    );
    assert_eq!(m.index_builds_for_tests(), builds + 2, "and built it once more, once");
}

// -- What does not join ------------------------------------------------------------------------------------------------------------------------

#[test]
fn a_local_drop_of_a_swapped_name_joins_nothing() {
    let w = world();
    let (m, _d) = member(&w);
    m.create_index(DB, "orders", field("v"), false, None)
        .expect("premise: the identity gets an index");
    flip(&m, &w.bind, Route::Window);
    let builds = m.index_builds_for_tests();
    assert!(
        m.drop_collection(DB, "orders").expect("the local drop runs"),
        "the drop found the name"
    );
    assert_eq!(
        m.index_builds_for_tests(),
        builds,
        "a drop makes no physical visible: no join, no build"
    );
}

#[test]
fn a_stage_that_never_took_effect_is_never_joined_and_builds_nothing_on_the_visible_physical() {
    // Row b's "never join a stage whose binding never took effect": an unbound stage holding a definition by hand, doomed (deadline below
    // the binding), is buried with the flip and contributes nothing: its index is not on the visible physical and no build ran for it.
    let (a, _da) = engine();
    let now = physical_now_ms();
    a.create_stage(DB, UNBOUND, MergePolicy::Document, Hlc::new(now + 5_000, 0))
        .expect("a stage with a short deadline");
    a.create_stage(DB, BOUND, MergePolicy::Document, Hlc::new(now + 120_000, 0))
        .expect("the stage that is bound");
    let creations: Vec<OplogEntry> = entries_of(&a, OpKind::CreateCollection);
    let bind = bind_of(&a, now + 20_000, BOUND);
    let (m, _d) = engine();
    m.create_collection(DB, "orders").expect("the identity");
    m.apply_batch(&creations).expect("the stages' creations apply");
    let early = creations.iter().map(|e| e.stamp.hlc.wall_ms).max().expect("two creations") + 1;
    let doomed_def = stage_index_entry(&a, UNBOUND, "d_1", "t", early);
    m.apply_batch(std::slice::from_ref(&doomed_def))
        .expect("the doomed stage's own definition applies by hand");
    assert!(
        m.get_collection(DB, UNBOUND).expect("held").index("d_1").is_some(),
        "premise: the unbound stage holds a definition"
    );
    let builds = m.index_builds_for_tests();
    window(&m, std::slice::from_ref(&bind));
    assert!(
        m.get_collection(DB, UNBOUND).is_err(),
        "premise: the doomed stage was buried with the binding"
    );
    assert!(
        m.get_collection(DB, BOUND).expect("held").index("d_1").is_none(),
        "its definition is not on the visible physical"
    );
    assert_eq!(
        m.index_builds_for_tests(),
        builds,
        "and no build ran for it (the identity held no index to join either)"
    );
}
