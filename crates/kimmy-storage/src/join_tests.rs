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
        // far enough above the real clock that no run can build a member after the author's stamps: a member's identity is created at the
        // real clock of the test, and a drop stamped below it is aimed at an earlier life of the identity. 30 s is more than any build of a
        // member takes under load, and it is safe: it is far below the stage deadlines (60 s from the same clock, so the binding is valid)
        // and below the clock-skew bound (300 s), so no entry is refused as ahead
        + 30_000;
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

/// How many entry rows the index `name` still has under `physical`, whether or not the physical still holds the index's definition: the
/// rows themselves, read by the id the name derives. `entries` above answers 0 for an index that is gone, so it cannot say that the rows
/// went with the definition; this can.
fn index_rows(m: &Engine, physical: &str, name: &str) -> usize {
    let coll = m.get_collection(DB, physical).expect("the physical is held");
    crate::index::scan_range(
        m,
        WalkScope::Request,
        coll.id,
        kimmy_core::IndexMeta::derive_id(name),
        &[],
        None,
        crate::index::Unkeyed::Include,
    )
    .expect("the index rows are readable")
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
        assert_eq!(index_rows(&m, BOUND, "v_1"), 0, "{route:?}: and its entries go with it");
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

// =====================================================================================================================================
// The post-drop row (Q6.5-2, B2): the definitions of a swapped name stamped above its newest logical drop, kept while the name is absent
// =====================================================================================================================================
//
// A logical drop in effect makes the name absent. What was made to the name above the drop (by members that had not heard of it, or after
// it) must not be lost with the physicals the drop buries, and must reach the next physical that becomes visible: the drop's burial writes
// the row from the physicals it buries, a definition that arrives while the name is absent merges into it, a later drop floors it, and the
// next physical that becomes visible takes it through the join and deletes it in the same transaction. Rows c and d of the model.
//
// Stamps are explicit, from the author of the swap: W is the first binding, `early` W+2, `old` W+3, the drop d at W+5, `late` W+7 and its
// validator W+8, the second binding W+10. Everything stamped above d is the new life's; everything at or below it is the dropped life's.

fn at(w: &World, offset: u64) -> u64 {
    w.bind.stamp.hlc.wall_ms + offset
}

fn author(w: &World) -> &Engine {
    &w._author.0
}

fn logical_drop(w: &World, offset: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(at(w, offset), 0), author(w).node_id(), author(w).epoch()),
        kind: OpKind::DropCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders"))
                .expect("the drop encodes"),
        ),
        meta: None,
    }
}

/// An index made to the logical name `orders` by the author at `W + offset`.
fn definition(w: &World, name: &str, path: &str, offset: u64) -> OplogEntry {
    stage_index_entry(author(w), "orders", name, path, at(w, offset))
}

fn validator_entry(w: &World, tag: &str, offset: u64) -> OplogEntry {
    crate::engine::ddl_entry(
        Stamp::new(Hlc::new(at(w, offset), 0), author(w).node_id(), author(w).epoch()),
        OpKind::ConfigureValidator,
        CollectionId::derive(DB, "orders"),
        &kimmy_core::ValidatorSet {
            db: DB.into(),
            collection: "orders".into(),
            validator: Some(validator(tag)),
        },
    )
    .expect("the validator entry encodes")
}

fn the_row(m: &Engine) -> crate::register::PostRow {
    crate::register::post_row_for_tests(m, DB, "orders")
        .expect("the row is readable")
        .expect("the post-drop row exists")
}

fn no_row(m: &Engine) -> bool {
    crate::register::post_row_for_tests(m, DB, "orders").expect("the row is readable").is_none()
}

fn names(row: &crate::register::PostRow) -> Vec<String> {
    let mut names: Vec<String> = row.indexes.iter().map(|i| i.name.clone()).collect();
    names.sort();
    names
}

fn absent(m: &Engine) -> bool {
    matches!(
        m.resolve(DB, "orders"),
        Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound { .. }))
    )
}

/// A member that made the flip, then took the definitions of a member that had not heard of the drop: `early` (below d), `late` and its
/// validator (above d), all landing on the bound stage, which is the physical the name answers from.
fn flipped_with_definitions(w: &World) -> (Arc<Engine>, tempfile::TempDir) {
    let (m, d) = member(w);
    flip(&m, &w.bind, Route::Window);
    window(
        &m,
        &[
            definition(w, "early_1", "t", 2),
            definition(w, "late_1", "v", 7),
            validator_entry(w, "late", 8),
        ],
    );
    let on = m.get_collection(DB, BOUND).expect("the bound stage is held");
    assert!(
        on.index("early_1").is_some() && on.index("late_1").is_some(),
        "premise: both definitions landed on the bound stage"
    );
    (m, d)
}

#[test]
fn a_logical_drop_writes_the_definitions_above_it_into_the_row_and_not_the_ones_at_or_below_it_in_every_order()
 {
    // Row d, M1: the definitions are on the bound physical when the drop takes effect. M4: the same with the drop known ahead of its
    // position and released by a window. The positive assertion comes first: what is above d is in the row, with its stamps.
    for parked in [false, true] {
        let w = world();
        let (m, _d) = flipped_with_definitions(&w);
        let drop = logical_drop(&w, 5);
        if parked {
            m.apply_batch(std::slice::from_ref(&drop))
                .expect("the drop is stored ahead of its position");
            assert!(
                !absent(&m),
                "premise: the parked drop is not in effect, the name still answers"
            );
            assert!(no_row(&m), "premise: no row until the drop takes effect");
        }
        window(&m, std::slice::from_ref(&drop));
        assert!(absent(&m), "premise: the drop is in effect, parked={parked}");
        let row = the_row(&m);
        assert_eq!(
            names(&row),
            vec!["late_1".to_string()],
            "parked={parked}: only the index stamped above the drop is in the row"
        );
        let late = definition(&w, "late_1", "v", 7);
        assert_eq!(
            row.indexes[0].created,
            Some(late.stamp),
            "parked={parked}: with its original stamp"
        );
        assert_eq!(
            row.validator,
            Some(validator("late")),
            "parked={parked}: and the validator stamped above the drop"
        );
        assert_eq!(
            row.validator_changed,
            Some(validator_entry(&w, "late", 8).stamp),
            "parked={parked}: with its stamp"
        );
    }
}

#[test]
fn a_definition_that_arrives_while_the_name_is_absent_merges_into_the_row_and_one_at_or_below_the_drop_is_history()
 {
    // Row d, M2: the drop first, the definitions after, in an order of their own. Nothing stops the batch, and delivering one again (as an
    // interrupted apply is delivered again) leaves the row exactly as after one delivery.
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    assert!(absent(&m), "premise: the name is absent");
    let entries = [
        definition(&w, "late_1", "v", 7),
        definition(&w, "old_1", "t", 3),
        validator_entry(&w, "late", 8),
    ];
    let outcome = window(&m, &entries);
    assert_eq!(
        outcome.unknown_collection, 0,
        "the batch does not stop at a definition for an absent swapped name: {outcome:?}"
    );
    assert_eq!(outcome.ddl, 3, "all three were taken: {outcome:?}");
    let row = the_row(&m);
    assert_eq!(
        names(&row),
        vec!["late_1".to_string()],
        "the one above the drop is in the row, the one below it is history"
    );
    assert_eq!(row.validator, Some(validator("late")), "and so is the validator");
    window(&m, &entries);
    assert_eq!(the_row(&m), row, "delivered again, the row ends exactly as after one delivery");
}

#[test]
fn a_swapped_name_whose_newest_drop_is_only_parked_still_answers_and_a_definition_lands_on_the_physical_it_answers_from()
 {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    m.apply_batch(std::slice::from_ref(&logical_drop(&w, 5)))
        .expect("the drop is stored ahead of its position");
    assert!(!absent(&m), "premise: the name answers while the drop is parked");
    window(&m, &[definition(&w, "late_1", "v", 7)]);
    assert!(
        m.get_collection(DB, BOUND).expect("held").index("late_1").is_some(),
        "the definition is on the visible physical"
    );
    assert!(no_row(&m), "and no row was written: the name is not absent");
}

#[test]
fn a_definition_for_a_collection_nobody_swapped_still_stops_the_batch_as_an_unknown_collection() {
    let w = world();
    let (m, _d) = member(&w);
    let ghost = stage_index_entry(author(&w), "ghost", "g_1", "t", at(&w, 7));
    let outcome = window(&m, std::slice::from_ref(&ghost));
    assert_eq!(
        outcome.unknown_collection, 1,
        "a plain unknown collection is today's stop: {outcome:?}"
    );
}

#[test]
fn the_next_physical_that_becomes_visible_takes_the_row_and_deletes_it_and_a_second_drop_and_recreate_inherit_nothing()
 {
    let w = world();
    let (m, _d) = flipped_with_definitions(&w);
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    let row = the_row(&m);
    assert_eq!(
        names(&row),
        vec!["late_1".to_string()],
        "premise: the row exists with known content"
    );
    m.create_collection(DB, "orders").expect("the create of a dropped swapped name is a recreate");
    let recreation =
        m.resolve(DB, "orders").expect("the name answers from the recreation").physical;
    assert!(
        kimmy_core::is_recreation_name(&recreation),
        "premise: it is a recreation: {recreation}"
    );
    let on = m.get_collection(DB, &recreation).expect("the recreation is held");
    assert_eq!(
        on.indexes.iter().map(|i| i.name.as_str()).collect::<Vec<_>>(),
        vec!["late_1"],
        "the first recreate inherits exactly the row's content"
    );
    assert_eq!(on.validator, Some(validator("late")), "validator included");
    assert!(no_row(&m), "the row is gone in the same transaction");
    assert!(m.drop_collection(DB, "orders").expect("the local drop runs"), "the second drop");
    m.create_collection(DB, "orders").expect("the second recreate");
    let second = m.resolve(DB, "orders").expect("the name answers").physical;
    assert_ne!(second, recreation, "premise: a new physical");
    let on = m.get_collection(DB, &second).expect("held");
    assert!(
        on.indexes.is_empty() && on.validator.is_none(),
        "the second recreate inherits nothing: the row was deleted"
    );
}

#[test]
fn a_stage_created_below_the_drop_and_bound_above_it_takes_the_row_and_its_own_copy_only_above_the_drop()
 {
    let w = world();
    let (m, _d) = flipped_with_definitions(&w);
    let second = m.get_collection(DB, UNBOUND).expect("premise: the second stage is held");
    for id in 20..22 {
        m.insert(&second, doc! { "_id": id, "v": id, "t": id })
            .expect("the second stage takes a row of the job's output");
    }
    // the second stage was created WITH an index, stamped below the drop (by hand, as the creation entry will carry it)
    let (author_copy, _dc) = engine();
    author_copy.apply_batch(&w.creations).expect("the author holds the stages");
    window(&m, &[stage_index_entry(&author_copy, UNBOUND, "copy_old_1", "t", at(&w, 1))]);
    assert!(
        m.get_collection(DB, UNBOUND).expect("held").index("copy_old_1").is_some(),
        "premise: the stage's own copy holds it"
    );
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    assert_eq!(
        names(&the_row(&m)),
        vec!["late_1".to_string()],
        "premise: the row holds the definition above the drop"
    );
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(
        m.resolve(DB, "orders").expect("the name resolves").physical,
        UNBOUND,
        "premise: the stage bound above the drop is visible"
    );
    let on = m.get_collection(DB, UNBOUND).expect("the second stage is held");
    assert!(on.index("late_1").is_some(), "it takes the row's definition");
    assert_eq!(entries(&m, UNBOUND, "late_1"), 2, "built over its own two rows");
    assert!(
        on.index("copy_old_1").is_none(),
        "its own copy keeps only what is above the drop: the one below it is the dropped life's"
    );
    assert!(no_row(&m), "and the row is gone");
}

#[test]
fn a_later_drop_removes_from_the_row_everything_at_or_below_it() {
    let w = world();
    let (m, _d) = flipped_with_definitions(&w);
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    assert_eq!(
        names(&the_row(&m)),
        vec!["late_1".to_string()],
        "premise: the row holds late_1, stamped W+7"
    );
    window(&m, std::slice::from_ref(&logical_drop(&w, 9)));
    assert!(absent(&m), "premise: still absent");
    let row = crate::register::post_row_for_tests(&m, DB, "orders").expect("readable");
    assert!(
        row.is_none_or(|r| r.indexes.is_empty() && r.validator.is_none()),
        "the second drop, above both, removed what the first row held"
    );
}

#[test]
fn a_later_drop_in_the_hlc_of_a_definition_the_row_holds_removes_it_from_a_node_above_and_from_one_below()
 {
    for above in [true, false] {
        let w = world();
        let (m, _d) = flipped_with_definitions(&w);
        window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
        assert_eq!(
            names(&the_row(&m)),
            vec!["late_1".to_string()],
            "premise: the row holds late_1, stamped W+7"
        );
        let late = definition(&w, "late_1", "v", 7);
        let (other, _od) = author_sorting(author(&w), above);
        let tie = OplogEntry {
            stamp: Stamp::new(Hlc::new(at(&w, 7), 0), other.node_id(), other.epoch()),
            kind: OpKind::DropCollection,
            collection: CollectionId::derive(DB, "orders"),
            doc_id: None,
            body: Some(
                bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders"))
                    .expect("the drop encodes"),
            ),
            meta: None,
        };
        assert_eq!(tie.stamp.hlc, late.stamp.hlc, "premise: the same hlc");
        assert_eq!(tie.stamp > late.stamp, above, "premise: the node sorts as the run says");
        window(&m, std::slice::from_ref(&tie));
        assert!(absent(&m), "premise: still absent");
        let row = crate::register::post_row_for_tests(&m, DB, "orders").expect("readable");
        assert!(
            row.is_none_or(|r| !names(&r).contains(&"late_1".to_string())),
            "a definition in a later drop's own hlc is the drop's, and is removed from the row (node above: {above})"
        );
    }
}

#[test]
fn a_pass_that_fails_after_writing_the_row_leaves_no_row_and_the_redelivery_writes_it_once() {
    let w = world();
    let (m, _d) = flipped_with_definitions(&w);
    let drop = logical_drop(&w, 5);
    crate::sync::race_hooks::fail_after_the_next_settle();
    let attempt = try_window(&m, std::slice::from_ref(&drop));
    assert!(attempt.is_err(), "premise: the transaction failed: {attempt:?}");
    assert!(!absent(&m), "premise: the drop was not committed");
    assert!(no_row(&m), "a transaction that aborts leaves no row");
    window(&m, std::slice::from_ref(&drop));
    assert_eq!(names(&the_row(&m)), vec!["late_1".to_string()], "the redelivery writes it, once");
}

#[test]
fn a_recreation_in_effect_while_the_older_drop_is_parked_carries_the_dropped_lifes_definitions_until_the_drop_is_released()
 {
    // The drop is known and not in effect, so the join runs with no floor and carries what the bound stage held, `old_1` (stamped at or
    // below the drop) included; a parked drop is not a floor. When the drop is released, the re-floor (Q6.5-3) removes it from the
    // recreation.
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    window(&m, &[definition(&w, "old_1", "t", 3)]);
    m.apply_batch(std::slice::from_ref(&logical_drop(&w, 5)))
        .expect("the drop is stored ahead of its position");
    let rc_stamp = Stamp::new(Hlc::new(at(&w, 9), 0), author(&w).node_id(), author(&w).epoch());
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
    window(&m, std::slice::from_ref(&recreate));
    assert_eq!(
        m.resolve(DB, "orders").expect("the name resolves").physical,
        physical,
        "premise: the recreation is visible"
    );
    assert!(
        m.get_collection(DB, &physical).expect("held").index("old_1").is_some(),
        "no floor is in effect (the drop is parked), so the join carries the dropped life's definition"
    );
    // Q6.5-3: when the drop takes effect below the recreation, the re-floor removes what is at or below it from the recreation
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    assert!(
        m.get_collection(DB, &physical).expect("held").index("old_1").is_none(),
        "the drop, released below the recreation, removes the dropped life's definition from it"
    );
}

#[test]
fn a_fresh_engine_has_the_post_drop_table_and_no_row() {
    let (m, _d) = engine();
    assert!(
        crate::register::post_row_for_tests(&m, DB, "orders")
            .expect("the table exists from the open: a read of it never meets a missing table")
            .is_none(),
        "no row on a store that has seen no drop"
    );
}

// -- The re-floor of a standing physical (Q6.5-3; ADR-272, decision 6) ------------------------------------------------------------------
//
// A drop at `d` that takes effect BELOW a physical that is visible (a recreation made visible before the older drop arrived, or a stage
// bound above the drop whose drop arrives after the flip) removes from it every definition stamped at or below `d`: the index with its
// entries, and the validator. The physical was born above the drop, so nothing buries it. The re-floor writes index tombstones under the
// standing physical at max(d, created), as a peer's drop of each index at the drop's stamp would have done. It is NOT an incarnation floor:
// documents are judged by nothing here, and a document stamped at or below `d` is still applied afterwards.

fn drop_below(w: &World, minus: u64) -> OplogEntry {
    let mut drop = logical_drop(w, 0);
    drop.stamp = Stamp::new(
        Hlc::new(w.bind.stamp.hlc.wall_ms - minus, 0),
        author(w).node_id(),
        author(w).epoch(),
    );
    drop
}

/// A recreation of `orders` by the author at `W + offset`, with the validator it was made with.
fn recreation(w: &World, offset: u64, validator: Option<ValidatorSpec>) -> (OplogEntry, String) {
    let stamp = Stamp::new(Hlc::new(at(w, offset), 0), author(w).node_id(), author(w).epoch());
    let physical = kimmy_core::recreation_name("orders", &stamp);
    let entry = crate::engine::ddl_entry(
        stamp,
        OpKind::CreateCollection,
        CollectionId::derive(DB, &physical),
        &kimmy_core::CollectionCreate {
            db: DB.into(),
            name: physical.clone(),
            merge_policy: MergePolicy::Document,
            validator,
            join_retention_ms: None,
            loss_limits: None,
            stage: None,
        },
    )
    .expect("the recreation's creation encodes");
    (entry, physical)
}

/// A document written by the author to `physical` at `wall_ms`, as an entry.
fn doc_entry(w: &World, physical: &str, id: i64, wall_ms: u64) -> OplogEntry {
    let scratch = author(w)
        .get_collection(DB, "scratch")
        .or_else(|_| author(w).create_collection(DB, "scratch"))
        .expect("the author's scratch collection");
    author(w)
        .insert(&scratch, doc! { "_id": id, "v": id, "t": id })
        .expect("the author writes a document");
    let mut entry = entries_of(author(w), OpKind::Insert)
        .into_iter()
        .rev()
        .find(|e| e.collection == scratch.id && e.doc_id == Some(kimmy_core::DocId::Int64(id)))
        .expect("the insert is in the author's oplog");
    entry.collection = CollectionId::derive(DB, physical);
    entry.stamp = Stamp::new(Hlc::new(wall_ms, 0), author(w).node_id(), author(w).epoch());
    entry
}

fn holds(m: &Engine, physical: &str, name: &str) -> bool {
    m.get_collection(DB, physical).is_ok_and(|c| c.index(name).is_some())
}

fn validator_of(m: &Engine, physical: &str) -> Option<String> {
    m.get_collection(DB, physical)
        .expect("the physical is held")
        .validator
        .map(|v| v.schema.get_str("title").unwrap_or_default().to_string())
}

fn has_doc(m: &Engine, physical: &str, id: i64) -> bool {
    let coll = m.get_collection(DB, physical).expect("the physical is held");
    m.get(&coll, &kimmy_core::DocId::Int64(id)).expect("the physical reads").is_some()
}

fn served(m: &Engine) -> String {
    m.resolve(DB, "orders").expect("the name resolves").physical
}

/// A member that made the flip and holds, on the bound stage, `old_1` (at `W + 3`, below the drop at `W + 5`), a validator (`W + 4`) and
/// `new_1` (`W + 7`, above it), each index with entries.
fn flipped_with_old_and_new(w: &World) -> (Arc<Engine>, tempfile::TempDir) {
    let (m, d) = member(w);
    flip(&m, &w.bind, Route::Window);
    window(
        &m,
        &[
            definition(w, "old_1", "t", 3),
            validator_entry(w, "old", 4),
            definition(w, "new_1", "v", 7),
        ],
    );
    assert!(entries(&m, BOUND, "old_1") > 0 && entries(&m, BOUND, "new_1") > 0, "premise: entries");
    assert_eq!(validator_of(&m, BOUND).as_deref(), Some("old"), "premise: the validator");
    (m, d)
}

/// A member whose recreation is visible while the drop at `W + 5` is not in effect: unknown (route `Window`) or parked.
fn recreation_over_a_drop_not_yet_in_effect(
    w: &World,
    route: Route,
) -> (Arc<Engine>, tempfile::TempDir, String, OplogEntry) {
    let (m, d) = flipped_with_old_and_new(w);
    let drop = logical_drop(w, 5);
    if !matches!(route, Route::Window) {
        learn(&m, &drop, route);
    }
    let (recreate, physical) = recreation(w, 9, None);
    window(&m, std::slice::from_ref(&recreate));
    assert_eq!(served(&m), physical, "premise: the recreation is visible");
    // a recreation starts with no documents, so the joined indexes have no entries until it takes some
    window(&m, &[doc_entry(w, &physical, 50, at(w, 10)), doc_entry(w, &physical, 51, at(w, 10))]);
    assert!(
        entries(&m, &physical, "old_1") > 0
            && validator_of(&m, &physical).as_deref() == Some("old"),
        "premise: with no floor in effect the recreation carries the dropped life's definitions, with entries"
    );
    (m, d, physical, drop)
}

fn take_effect(m: &Engine, drop: &OplogEntry, route: Route) {
    match route {
        Route::Window => {
            window(m, std::slice::from_ref(drop));
        }
        _ => release(m, drop, route),
    }
}

#[test]
fn a_drop_that_takes_effect_below_a_recreation_removes_what_is_at_or_below_it_from_the_recreation_by_every_route()
 {
    for route in ROUTES {
        let w = world();
        let (m, _d, physical, drop) = recreation_over_a_drop_not_yet_in_effect(&w, route);
        take_effect(&m, &drop, route);
        assert_eq!(served(&m), physical, "the recreation still serves ({route:?})");
        assert!(
            !holds(&m, &physical, "old_1"),
            "the index at or below the drop is gone ({route:?})"
        );
        assert_eq!(index_rows(&m, &physical, "old_1"), 0, "and its entries ({route:?})");
        assert_eq!(validator_of(&m, &physical), None, "and the validator ({route:?})");
        assert!(
            entries(&m, &physical, "new_1") > 0,
            "an index made above the drop stays, with its entries ({route:?})"
        );
        assert_eq!(
            tombstone(&m, &physical, "old_1"),
            Some(drop.stamp),
            "the re-floor leaves the index's tombstone under the standing physical, at max(d, created) ({route:?})"
        );
    }
}

#[test]
fn a_drop_that_takes_effect_below_a_bound_stage_removes_the_copied_definitions_and_is_not_an_incarnation_floor()
 {
    for route in ROUTES {
        let w = world();
        let (m, _d) = member(&w);
        let wall = w.bind.stamp.hlc.wall_ms;
        // the stage was created below the drop and carries the target's definitions (by hand, as Q6.7's creation entry will), and rows
        // written by explicit entries on both sides of the drop at `wall - 5`
        window(
            &m,
            &[
                stage_index_entry(author(&w), BOUND, "copied", "t", wall - 8),
                crate::engine::ddl_entry(
                    Stamp::new(Hlc::new(wall - 7, 0), author(&w).node_id(), author(&w).epoch()),
                    OpKind::ConfigureValidator,
                    CollectionId::derive(DB, BOUND),
                    &kimmy_core::ValidatorSet {
                        db: DB.into(),
                        collection: BOUND.into(),
                        validator: Some(validator("copied")),
                    },
                )
                .expect("the stage's validator encodes"),
                doc_entry(&w, BOUND, 30, wall - 6),
                doc_entry(&w, BOUND, 31, wall + 7),
            ],
        );
        flip(&m, &w.bind, Route::Window);
        assert!(entries(&m, BOUND, "copied") > 0, "premise: the copied index has entries");
        assert_eq!(validator_of(&m, BOUND).as_deref(), Some("copied"), "premise: the validator");
        assert!(
            has_doc(&m, BOUND, 30) && has_doc(&m, BOUND, 31),
            "premise: a row at or below the drop and one above it"
        );
        let before: Vec<bool> =
            [10, 11, 12, 30, 31].iter().map(|id| has_doc(&m, BOUND, *id)).collect();

        let drop = drop_below(&w, 5);
        learn(&m, &drop, route);
        release(&m, &drop, route);
        assert_eq!(served(&m), BOUND, "the stage still serves ({route:?})");
        assert!(!holds(&m, BOUND, "copied"), "the copied index is gone ({route:?})");
        assert_eq!(index_rows(&m, BOUND, "copied"), 0, "and its entries ({route:?})");
        assert_eq!(validator_of(&m, BOUND), None, "and the validator ({route:?})");
        let after: Vec<bool> =
            [10, 11, 12, 30, 31].iter().map(|id| has_doc(&m, BOUND, *id)).collect();
        assert_eq!(before, after, "no document is removed ({route:?})");
        // the drop is at `wall - 5`: one document below it and one in its own hlc, each delivered after the re-floor
        window(&m, &[doc_entry(&w, BOUND, 32, wall - 6), doc_entry(&w, BOUND, 33, wall - 5)]);
        assert!(
            has_doc(&m, BOUND, 32) && has_doc(&m, BOUND, 33),
            "a document stamped at or below the drop, delivered after the re-floor, is applied: the re-floor is not an incarnation floor ({route:?})"
        );
    }
}

#[test]
fn a_flip_then_a_drop_and_a_drop_then_a_flip_end_with_the_same_definitions() {
    let w = world();
    let wall = w.bind.stamp.hlc.wall_ms;
    let by_hand = |m: &Engine| {
        window(
            m,
            &[
                stage_index_entry(author(&w), BOUND, "copied", "t", wall - 8),
                stage_index_entry(author(&w), BOUND, "kept", "v", wall + 7),
            ],
        );
        assert!(
            entries(m, BOUND, "copied") > 0 && entries(m, BOUND, "kept") > 0,
            "premise: both are on the stage"
        );
    };
    let drop = drop_below(&w, 5);
    // the flip first, then the drop
    let (flip_first, _d1) = member(&w);
    by_hand(&flip_first);
    flip(&flip_first, &w.bind, Route::Window);
    assert!(
        holds(&flip_first, BOUND, "copied"),
        "premise: the definition to be removed existed before the drop"
    );
    window(&flip_first, std::slice::from_ref(&drop));
    // the drop first, then the flip
    let (drop_first, _d2) = member(&w);
    by_hand(&drop_first);
    window(&drop_first, std::slice::from_ref(&drop));
    flip(&drop_first, &w.bind, Route::Window);
    for (m, order) in [(&flip_first, "flip then drop"), (&drop_first, "drop then flip")] {
        assert!(
            !holds(m, BOUND, "copied"),
            "the definition at or below the drop is gone ({order})"
        );
        assert!(
            entries(m, BOUND, "kept") > 0,
            "the definition above it stands, with entries ({order})"
        );
        assert_eq!(
            m.get_collection(DB, BOUND).expect("held").indexes.len(),
            1,
            "and it is the only one ({order})"
        );
    }
}

/// An author whose node sorts above (`above`) or below the node of `reference`'s author.
fn author_sorting(reference: &Engine, above: bool) -> (Engine, tempfile::TempDir) {
    loop {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let candidate = Engine::open(&dir.path().join("kimmy.redb")).expect("an engine opens");
        if (candidate.node_id() > reference.node_id()) == above {
            return (candidate, dir);
        }
    }
}

// Every floor is an `Hlc` at every site (the row's floor and merge, the join's `above`, `floor_definitions_in_txn`), so these runs pin an
// hlc-only comparison: the node-above and node-below runs both end the same way, and are killed together by `>=` for `>` and `<` for `<=`.
// A comparison on the full stamp would be a code change and has no sed mutant.
#[test]
fn a_definition_in_the_drops_own_hlc_is_the_drops_from_a_node_above_it_and_from_one_below_it_whenever_it_arrives()
 {
    for above in [true, false] {
        for before_the_drop in [true, false] {
            let w = world();
            let (m, _d) = member(&w);
            flip(&m, &w.bind, Route::Window);
            let drop = logical_drop(&w, 5);
            let (other, _od) = author_sorting(author(&w), above);
            let tie = stage_index_entry(&other, "orders", "tie_1", "t", drop.stamp.hlc.wall_ms);
            assert_eq!(tie.stamp.hlc, drop.stamp.hlc, "premise: the same hlc");
            assert_eq!(tie.stamp > drop.stamp, above, "premise: the node sorts as the run says");
            let later = stage_index_entry(&other, "orders", "after_1", "v", at(&w, 7));
            let run = format!("node above: {above}, before the drop: {before_the_drop}");
            if before_the_drop {
                let outcome = window(&m, &[tie.clone(), later.clone()]);
                assert!(!outcome.stopped(), "the entries are judged ({run})");
                window(&m, std::slice::from_ref(&drop));
            } else {
                window(&m, std::slice::from_ref(&drop));
                let outcome = window(&m, &[tie.clone(), later.clone()]);
                assert!(!outcome.stopped(), "the entries are judged, not a stop ({run})");
            }
            let (recreate, physical) = recreation(&w, 9, None);
            window(&m, std::slice::from_ref(&recreate));
            assert_eq!(served(&m), physical, "premise: the recreation is visible ({run})");
            assert!(
                !holds(&m, &physical, "tie_1"),
                "a definition in the drop's own hlc is the drop's ({run})"
            );
            assert!(
                holds(&m, &physical, "after_1"),
                "and a later entry of the same origin in the same window was applied ({run})"
            );
        }
    }
}

#[test]
fn a_second_cycle_of_drop_and_recreate_inherits_nothing_from_before_the_second_drop() {
    let w = world();
    let (m, _d) = flipped_with_old_and_new(&w);
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    m.finish_purges_now().expect("the purge finishes");
    m.create_collection(DB, "orders").expect("the first recreate");
    let first = served(&m);
    assert!(
        holds(&m, &first, "new_1") && !holds(&m, &first, "old_1"),
        "premise: the first recreation"
    );
    m.create_index(DB, "orders", vec![IndexField::ascending("t")], false, Some("cycle_2".into()))
        .expect("a definition is made on the first recreation");
    assert!(holds(&m, &first, "cycle_2"), "premise: it exists before the second drop");
    assert!(m.drop_collection(DB, "orders").expect("the second drop"), "premise: the second drop");
    assert!(
        crate::register::post_row_for_tests(&m, DB, "orders").expect("reads").is_none_or(|row| {
            !row.indexes.iter().any(|i| i.name == "old_1" || i.name == "new_1")
        }),
        "premise: the row holds nothing of the first life"
    );
    m.finish_purges_now().expect("the purge finishes");
    m.create_collection(DB, "orders").expect("the second recreate");
    let second = served(&m);
    assert_ne!(second, first, "premise: a new physical");
    let held = m.get_collection(DB, &second).expect("held");
    let names: Vec<&str> = held.indexes.iter().map(|i| i.name.as_str()).collect();
    assert!(
        !names.contains(&"old_1") && !names.contains(&"new_1"),
        "nothing of the first life: {names:?}"
    );
}

#[test]
fn a_recreation_made_with_a_validator_keeps_its_own_over_an_older_one_in_the_row() {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    window(&m, &[validator_entry(&w, "row_validator", 8)]);
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    let row = the_row(&m);
    assert_eq!(
        row.validator.as_ref().and_then(|v| v.schema.get_str("title").ok()),
        Some("row_validator"),
        "premise: the row holds the older validator"
    );
    let (recreate, physical) = recreation(&w, 12, Some(validator("own")));
    window(&m, std::slice::from_ref(&recreate));
    assert_eq!(served(&m), physical, "premise: the recreation is visible");
    assert_eq!(
        validator_of(&m, &physical).as_deref(),
        Some("own"),
        "the creation's own validator stands over the row's"
    );
}

#[test]
fn a_drop_that_is_only_parked_is_not_a_floor_and_the_re_floor_waits_for_its_release() {
    let w = world();
    let (m, _d, physical, drop) =
        recreation_over_a_drop_not_yet_in_effect(&w, Route::ParkedThenWindow);
    // parked: a definition below it that arrives is applied to the visible recreation
    window(&m, &[definition(&w, "old_2", "v", 2)]);
    assert!(
        entries(&m, &physical, "old_2") > 0 && entries(&m, &physical, "old_1") > 0,
        "a parked drop is not a floor: what is below it is applied and stays until it is released"
    );
    window(&m, std::slice::from_ref(&drop));
    assert!(
        !holds(&m, &physical, "old_1") && !holds(&m, &physical, "old_2"),
        "gone after the release"
    );
}

#[test]
fn a_local_recreate_while_a_second_drop_is_only_parked_keeps_what_the_post_drop_row_holds_above_the_drop_in_effect()
 {
    // The local doors run the pass with no event that has just taken effect (engine.rs:5730, 5789), so they reach the re-floor's branch with
    // a drop that is known and not in effect. The state that reaches it: the name is absent under a logical drop D1 in effect (a local
    // create is refused as existing while the name answers), and a SECOND drop D2, above what the post-drop row holds, is only parked. The
    // recreation takes the row; a parked drop is no floor, so what the row holds above D1 stays.
    let w = world();
    let (m, _d) = flipped_with_old_and_new(&w);
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    assert!(absent(&m), "premise: the name is absent under the drop in effect");
    assert_eq!(
        names(&the_row(&m)),
        vec!["new_1".to_string()],
        "premise: the row holds only what is above the drop in effect"
    );
    let second = logical_drop(&w, 9);
    // an older parked drop cannot kill C3-8: the second drop is stamped above new_1's creation (W + 7), the definition the row holds, or the
    // widened re-floor would remove nothing from it
    assert!(
        second.stamp.hlc > Hlc::new(at(&w, 7), 0),
        "premise: the parked drop is above what the row holds (an older parked drop cannot kill C3-8)"
    );
    learn(&m, &second, Route::ParkedThenWindow);
    {
        use redb::ReadableDatabase;
        let txn = m.db().begin_read().expect("a read");
        assert_eq!(
            crate::register::parked_read(&txn, DB, "orders")
                .expect("the register reads")
                .events()
                .len(),
            1,
            "premise: the second drop is known and not in effect"
        );
    }
    assert!(absent(&m), "premise: the name is still absent: a parked drop changes nothing");
    m.create_collection(DB, "orders")
        .expect("the create of a name absent under a drop is a recreate");
    let local = served(&m);
    assert!(kimmy_core::is_recreation_name(&local), "premise: a recreation serves: {local}");
    let on_local = m.get_collection(DB, &local).expect("the recreation is held");
    m.insert(&on_local, doc! { "_id": 60, "v": 60, "t": 60 })
        .expect("the recreation takes a document");
    assert!(
        entries(&m, &local, "new_1") > 0,
        "a parked drop is not a floor: the definition above the drop in effect stays on the recreation, with entries"
    );
    // THIS test pins the order "the recreation is ABOVE the parked drop": a local create is newer than everything the member knows, a
    // parked drop included (it is stamped above the clock and the newest event of the name), so the second drop, when it takes effect, is
    // BELOW the recreation and re-floors it instead of ending it. (The order "below" cannot be minted by a local door.) The assertion
    // before the release is the one that kills C3-8.
    assert!(
        on_local.created > second.stamp.hlc,
        "premise: the local door minted the recreation above the witnessed parked drop"
    );
    window(&m, std::slice::from_ref(&second));
    assert_eq!(served(&m), local, "the second drop is below the recreation: it still serves");
    assert!(
        !holds(&m, &local, "new_1"),
        "the released drop re-floors the recreation: new_1, at or below it, is gone"
    );
    assert_eq!(index_rows(&m, &local, "new_1"), 0, "and its entries with it");
}

// Half one ("an aborted drop re-floors nothing") is a STRUCTURAL guard and cannot be mutated: the removal runs inside the window's own
// WriteTxn, so a re-floor made in the pass is made before the commit and an abort rolls it back. The pass reaches the removal only through
// `floor_definitions_in_txn(txn: &WriteTxn, ..)` (join.rs:340), which takes only that transaction and opens none of its own, and the pass is
// called only inside a commit's transaction (sync.rs:3583 and 3664, engine.rs:4029, 5730 and 5789). A mutant that survived an abort would
// need a second, independently committed write path, and there is none. Half two ("the redelivery re-floors") is the red.
#[test]
fn a_drop_released_in_a_transaction_that_fails_re_floors_nothing_and_the_redelivery_re_floors_once()
{
    let w = world();
    let (m, _d, physical, drop) =
        recreation_over_a_drop_not_yet_in_effect(&w, Route::ParkedThenWindow);
    let passes = crate::sync::race_hooks::settle_passes();
    crate::sync::race_hooks::fail_after_the_next_settle();
    let attempt = try_window(&m, std::slice::from_ref(&drop));
    assert!(attempt.is_err(), "premise: the transaction failed: {attempt:?}");
    assert_eq!(crate::sync::race_hooks::settle_passes(), passes + 1, "premise: the pass ran");
    assert!(
        entries(&m, &physical, "old_1") > 0 && validator_of(&m, &physical).is_some(),
        "an aborted drop re-floors nothing"
    );
    window(&m, std::slice::from_ref(&drop));
    assert!(
        !holds(&m, &physical, "old_1") && validator_of(&m, &physical).is_none(),
        "the redelivery re-floors"
    );
    assert!(entries(&m, &physical, "new_1") > 0, "and keeps what is above the drop");
}

#[test]
fn an_older_index_delivered_again_after_the_re_floor_is_history_where_it_re_floored() {
    let w = world();
    let (m, _d, physical, drop) =
        recreation_over_a_drop_not_yet_in_effect(&w, Route::ParkedThenWindow);
    window(&m, std::slice::from_ref(&drop));
    assert!(!holds(&m, &physical, "old_1"), "premise: re-floored");
    let outcome = window(&m, &[definition(&w, "old_1", "t", 3), definition(&w, "later_1", "t", 8)]);
    assert!(!outcome.stopped(), "the redelivery reached the arm and the window did not stop");
    assert!(holds(&m, &physical, "later_1"), "a later entry of the same window was applied");
    assert!(
        !holds(&m, &physical, "old_1"),
        "the older create is history on the member that re-floored"
    );
    assert_eq!(
        tombstone(&m, &physical, "old_1"),
        Some(drop.stamp),
        "by the tombstone under the standing physical, at max(d, created)"
    );
}

#[test]
fn a_recreate_inherits_nothing_at_or_below_the_drop_on_the_local_door_and_on_a_replica() {
    let w = world();
    let (m, _d) = flipped_with_old_and_new(&w);
    let drop = logical_drop(&w, 5);
    window(&m, std::slice::from_ref(&drop));
    assert_eq!(
        names(&the_row(&m)),
        vec!["new_1".to_string()],
        "premise: the row holds only what is above the drop"
    );
    m.create_collection(DB, "orders").expect("the create of a dropped swapped name is a recreate");
    let local = served(&m);
    let on_local = m.get_collection(DB, &local).expect("the recreation is held");
    m.insert(&on_local, doc! { "_id": 60, "v": 60, "t": 60 })
        .expect("the recreation takes a document");
    assert!(kimmy_core::is_recreation_name(&local), "premise: a recreation: {local}");
    // a replica that meets the same events and the recreation as an entry
    let (replica, _dr) = flipped_with_old_and_new(&w);
    window(&replica, std::slice::from_ref(&drop));
    let creation = everything(&m)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, &local)
        })
        .expect("the local recreation minted its creation entry");
    window(&replica, std::slice::from_ref(&creation));
    let written = everything(&m)
        .into_iter()
        .find(|e| e.kind == OpKind::Insert && e.collection == CollectionId::derive(DB, &local))
        .expect("the local insert minted its entry");
    window(&replica, std::slice::from_ref(&written));
    for (member, door) in [(&m, "local door"), (&replica, "replica")] {
        assert_eq!(served(member), local, "the recreation serves ({door})");
        assert!(
            entries(member, &local, "new_1") > 0,
            "the index above the drop is on it, with entries ({door})"
        );
        assert!(!holds(member, &local, "old_1"), "the index at or below the drop is not ({door})");
        assert_eq!(validator_of(member, &local), None, "nor the validator ({door})");
    }
}

#[test]
fn two_recreations_do_not_merge_their_documents_and_the_losers_definitions_join_the_winner_in_every_order()
 {
    // R1 and R2 are made at once by two members; every member ends with R2 visible and R1 bound out. A document written to R1 and an index
    // made on R1 above the drop: the document is not merged into R2 and the index is
    let orders: [(&str, [usize; 4]); 3] = [
        ("R1, the document and the index, then R2", [0, 2, 3, 1]),
        ("R2, then R1, then the document and the index", [1, 0, 2, 3]),
        ("R1, R2, then the document and the index", [0, 1, 2, 3]),
    ];
    for (name, order) in orders {
        let w = world();
        let (m, _d) = member(&w);
        flip(&m, &w.bind, Route::Window);
        window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
        let (r1, p1) = recreation(&w, 9, None);
        let (r2, p2) = recreation(&w, 10, None);
        let doc = doc_entry(&w, &p1, 40, at(&w, 11));
        let index = definition(&w, "r1_idx", "t", 12);
        let items = [r1, r2, doc, index];
        for i in order {
            window(&m, std::slice::from_ref(&items[i]));
        }
        assert_eq!(served(&m), p2, "R2 serves ({name})");
        assert!(!has_doc(&m, &p2, 40), "the document written to R1 is not in R2 ({name})");
        assert!(holds(&m, &p2, "r1_idx"), "the index made on R1 above the drop is on R2 ({name})");
    }
}

#[test]
fn a_definition_in_the_drops_own_hlc_from_a_node_above_it_is_removed_by_the_re_floor_and_its_tombstone_sorts_above_the_drop()
 {
    // the index sorts above the drop's stamp (the same hlc, a higher node), so its tombstone is at its own stamp and not at the drop's
    let w = world();
    let (m, _d) = flipped_with_old_and_new(&w);
    let drop = logical_drop(&w, 5);
    let (other, _od) = author_sorting(author(&w), true);
    let tie = stage_index_entry(&other, "orders", "tie_2", "t", drop.stamp.hlc.wall_ms);
    assert!(
        tie.stamp > drop.stamp && tie.stamp.hlc == drop.stamp.hlc,
        "premise: the same hlc, above the drop's stamp"
    );
    window(&m, std::slice::from_ref(&tie));
    m.apply_batch(std::slice::from_ref(&drop)).expect("the drop is stored ahead");
    let (recreate, physical) = recreation(&w, 9, None);
    window(&m, std::slice::from_ref(&recreate));
    window(&m, &[doc_entry(&w, &physical, 50, at(&w, 10))]);
    assert!(
        entries(&m, &physical, "tie_2") > 0,
        "premise: the recreation carries it, with entries"
    );
    release(&m, &drop, Route::ParkedThenWindow);
    assert!(
        !holds(&m, &physical, "tie_2"),
        "an index in the drop's own hlc is the drop's, from whichever node"
    );
    assert_eq!(
        tombstone(&m, &physical, "tie_2"),
        Some(tie.stamp),
        "the tombstone is at the index's own stamp where that sorts above the drop's"
    );
}

#[test]
fn a_definition_entry_addressed_to_a_physical_the_member_never_held_stops_the_window_as_an_unknown_collection_today()
 {
    // PINS TODAY'S BEHAVIOUR. No route mints such an entry: a local index, drop and validator change are addressed to the LOGICAL name
    // (rule 1: index.rs:1601, 1921, 2078, validator.rs:235), a recreation's creation carries its validator in the creation entry itself, a
    // snapshot page carries collection state and no definition entries, and the vector doors refuse a name with a register event, so a
    // vector entry is never addressed to a swapped name's physical (vectors.rs:389, 571 name `meta.id` of a name that has no event).
    // Only an entry made by hand reaches it, as Q6.7's stage definitions would if they were separate entries; Q6.7 carries the
    // stage's definitions in the stage's creation entry (Q67-prebuild unit 1), which the member holds. A stop on an unknown collection
    // plans a snapshot, which never brings a buried physical: if Q6.7 ever sends such an entry the arm must treat it as history.
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    let (r1, p1) = recreation(&w, 9, None);
    let (r2, p2) = recreation(&w, 10, None);
    window(&m, std::slice::from_ref(&r2));
    window(&m, std::slice::from_ref(&r1));
    assert!(m.get_collection(DB, &p1).is_err(), "premise: the losing recreation is not held here");
    assert_eq!(served(&m), p2, "premise: the winner serves");
    let by_hand = stage_index_entry(author(&w), &p1, "r1_idx", "t", at(&w, 12));
    let outcome = window(&m, std::slice::from_ref(&by_hand));
    assert!(
        outcome.stopped() && outcome.unknown_collection == 1,
        "an entry addressed to a physical this member never held is an unknown collection and stops the window (today)"
    );
}

// -- The arms' floor (ADR-272, decision 6, rule 4; Q6.5-4) --------------------------------------------------------------------------------
//
// A physical that became visible has no tombstone of its own and no incarnation floor, so what an arm judged a definition by (the standing
// collection's own floors and tombstones) says nothing of the drop that ended the life it replaced. The one rule every route judges a
// definition by (`definition_of_an_ended_life`) reads the newest logical drop in effect for the name as well, on the `hlc`, a tie being the
// drop's. And what an arm remembers of a definition (an index's tombstone, a refusal's hint) is keyed by the physical it judged it against.

/// A member whose recreation is visible over the logical drop at `W + 5` that is in effect, with nothing delivered to the recreation: it
/// holds no incarnation floor, no definition floor and no tombstone of its own.
fn recreation_visible_over_the_drop_in_effect(
    w: &World,
) -> (Arc<Engine>, tempfile::TempDir, String, OplogEntry) {
    let (m, d) = member(w);
    flip(&m, &w.bind, Route::Window);
    let drop = logical_drop(w, 5);
    window(&m, std::slice::from_ref(&drop));
    let (recreate, physical) = recreation(w, 9, None);
    window(&m, std::slice::from_ref(&recreate));
    assert_eq!(served(&m), physical, "premise: the recreation is visible");
    let standing = m.get_collection(DB, &physical).expect("premise: the recreation is held");
    assert!(
        standing.incarnation_floor.is_none() && standing.definition_floor.is_none(),
        "premise: the recreation has no floor of its own"
    );
    assert_eq!(
        m.collection_dropped_at(standing.id).expect("reads"),
        None,
        "premise: the recreation's id holds no tombstone"
    );
    (m, d, physical, drop)
}

#[test]
fn a_definition_at_or_below_the_drop_arriving_after_the_recreation_is_visible_is_history_by_the_registers_floor_alone()
 {
    // the runs moved from Q6.5-3's test 5 (an equal `hlc` from a node above the drop's and from one below it) and test 6 (below the drop).
    // A failure is COLLECTED per run and asserted at the end, so a run that fails does not hide the runs after it
    let mut failures: Vec<String> = Vec::new();
    for (run, tie, above) in
        [("tie, node above", true, true), ("tie, node below", true, false), ("below", false, true)]
    {
        let w = world();
        let (m, _d, physical, drop) = recreation_visible_over_the_drop_in_effect(&w);
        let (other, _od) = author_sorting(author(&w), above);
        let old = if tie {
            stage_index_entry(&other, "orders", "old_1", "t", drop.stamp.hlc.wall_ms)
        } else {
            stage_index_entry(&other, "orders", "old_1", "t", at(&w, 3))
        };
        if tie {
            assert_eq!(old.stamp.hlc, drop.stamp.hlc, "premise: the same hlc ({run})");
            assert_eq!(old.stamp > drop.stamp, above, "premise: the node sorts as the run says");
        }
        let later = stage_index_entry(&other, "orders", "after_1", "v", at(&w, 11));
        let outcome = window(&m, &[old.clone(), later.clone()]);
        if outcome.stopped() {
            failures.push(format!("the entries are judged, not a stop ({run})"));
        }
        if holds(&m, &physical, "old_1") {
            failures.push(format!("old_1 is on the recreation ({run})"));
        }
        if !holds(&m, &physical, "after_1") {
            failures.push(format!("the later entry of the same window was not applied ({run})"));
        }
    }
    assert!(
        failures.is_empty(),
        "a definition at or below the drop is the dropped life's, on the recreation that has no floor of its own, and a later entry of the same window was applied: {failures:?}"
    );
}

#[test]
fn an_older_index_delivered_again_is_history_on_a_member_that_floored_at_the_join_and_never_held_it()
 {
    // Expected RED before the arm reads the register. The drop is met first, then the flip: the member never held `old_1` and wrote no
    // tombstone for it, the identity was made at the real clock and has no incarnation floor, and the flip assigns the new physical no
    // definition floor here, so nothing but the register's floor refuses the older create. If this is green at the tests commit, something
    // else refused it, and that is to be found and named, not assumed.
    let w = world();
    let (m, _d) = member(&w);
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(served(&m), UNBOUND, "premise: the flip over the drop in effect");
    assert_eq!(tombstone(&m, UNBOUND, "old_1"), None, "premise: no tombstone for the index");
    assert!(!holds(&m, UNBOUND, "old_1"), "premise: the member never held it");
    let outcome = window(&m, &[definition(&w, "old_1", "t", 3), definition(&w, "later_1", "t", 8)]);
    assert!(!outcome.stopped(), "the redelivery reached the arm and the window did not stop");
    assert!(holds(&m, UNBOUND, "later_1"), "a later entry of the same window was applied");
    assert!(
        !holds(&m, UNBOUND, "old_1"),
        "the older create is history on the member that floored at the join"
    );
}

#[test]
fn a_drop_of_an_index_older_than_the_one_on_the_new_physical_is_remembered_under_the_new_physical_and_not_under_the_identity()
 {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    window(&m, std::slice::from_ref(&definition(&w, "i_1", "t", 7)));
    assert!(holds(&m, BOUND, "i_1"), "premise: the index is on the new physical");
    let id = kimmy_core::IndexMeta::derive_id("i_1");
    let identity = CollectionId::derive(DB, "orders");
    assert_eq!(
        m.index_dropped_at(identity, id).expect("reads"),
        None,
        "premise: nothing under the identity"
    );
    assert_eq!(tombstone(&m, BOUND, "i_1"), None, "premise: nothing under the new physical");
    // a drop stamped below the index's creation, from a member that had not seen it: declined, and remembered
    let stale = drop_index_entry(author(&w), "i_1", at(&w, 6), None);
    let outcome = window(&m, std::slice::from_ref(&stale));
    assert!(!outcome.stopped(), "the drop is judged, not a stop");
    assert_eq!(outcome.ddl_declined, 1, "premise: the drop was declined and counted, as news");
    assert!(holds(&m, BOUND, "i_1"), "the drop is older than the index it names: declined");
    assert_eq!(
        tombstone(&m, BOUND, "i_1"),
        Some(stale.stamp),
        "remembered under the physical that holds the index, at the drop's stamp"
    );
    assert_eq!(
        m.index_dropped_at(identity, id).expect("reads"),
        None,
        "and none is written under the identity's id, which the burial never clears"
    );
    // served again, the drop is one this member has recorded: not counted a second time (a re-served window is not news)
    let again = window(&m, std::slice::from_ref(&stale));
    assert_eq!(
        again.ddl_declined, 0,
        "a drop recorded under the physical is a replay, and not counted"
    );
}

#[test]
fn a_refused_index_leaves_its_hint_under_the_new_physical_and_the_burial_clears_it() {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    let identity = CollectionId::derive(DB, "orders");
    let bound = CollectionId::derive(DB, BOUND);
    // an index this build cannot apply: a time-to-live over two fields
    let refused = {
        let index = kimmy_core::IndexMeta {
            id: kimmy_core::IndexMeta::derive_id("ttl_2"),
            name: "ttl_2".into(),
            fields: vec![IndexField::ascending("a"), IndexField::ascending("b")],
            unique: false,
            enforcement: Enforcement::Local,
            multikey: false,
            expire_after_secs: Some(60),
            partial_filter: None,
            created: None,
        };
        OplogEntry {
            stamp: Stamp::new(Hlc::new(at(&w, 7), 0), author(&w).node_id(), author(&w).epoch()),
            kind: OpKind::CreateIndex,
            collection: identity,
            doc_id: None,
            body: Some(
                bson::serialize_to_vec(&IndexCreate {
                    db: DB.into(),
                    collection: "orders".into(),
                    index,
                })
                .expect("the creation body encodes"),
            ),
            meta: None,
        }
    };
    assert!(
        m.refusal_hints(identity).expect("reads").is_empty()
            && m.refusal_hints(bound).expect("reads").is_empty(),
        "premise: no hint under either id"
    );
    let outcome = window(&m, std::slice::from_ref(&refused));
    assert_eq!(outcome.ddl_refused, 1, "premise: the member refused the index");
    assert_eq!(
        m.refusal_hints(bound).expect("reads").len(),
        1,
        "the hint is under the physical that refused it"
    );
    assert!(
        m.refusal_hints(identity).expect("reads").is_empty(),
        "and none is under the identity's id, which nothing clears"
    );
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(served(&m), UNBOUND, "premise: the second flip buries the first physical");
    assert!(
        m.refusal_hints(bound).expect("reads").is_empty(),
        "the burial clears the hint with the physical"
    );
}

// -- The same floor on the other routes that judge a definition, and the tombstone of a drop that is applied -------------------------------

/// Apply the pages of a snapshot of `orders` from `sender` to `m`, with a BOUND: a route that refuses every page whole may never advance its
/// progress, and a loop with no bound hangs the queue instead of failing on a designed message.
fn apply_the_pages(m: &Engine, sender: &Engine) {
    let mut progress = crate::SnapshotProgress::of_collection(CollectionId::derive(DB, "orders"));
    for _ in 0..50 {
        if progress.is_complete() {
            return;
        }
        let page = sender
            .snapshot_page(progress.after().cloned(), progress.scope())
            .expect("the sender's page");
        m.apply_snapshot_page(sender.node_id(), &mut progress, &page).expect("the page applies");
    }
    assert!(
        progress.is_complete(),
        "the page route did not advance: 50 pages and the pull is not complete"
    );
}

/// A sender that holds `orders` with an index made at its own clock, which is below every stamp of the author of the swap (the author's
/// stamps are `world()`'s margin above the real clock): the dropped life's definition, as a page names it.
fn a_sender_with_an_old_index(name: &str) -> (Arc<Engine>, tempfile::TempDir) {
    let (a, d) = engine();
    a.create_collection(DB, "orders").expect("the sender's collection");
    a.create_index(DB, "orders", field("t"), false, Some(name.into())).expect("the sender's index");
    (a, d)
}

#[test]
fn a_snapshot_page_naming_an_index_at_or_below_the_drop_does_not_restore_it_into_the_recreation() {
    let w = world();
    let (m, _d, physical, drop) = recreation_visible_over_the_drop_in_effect(&w);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    assert!(
        a.get_collection(DB, "orders")
            .expect("the sender holds it")
            .index("by_t")
            .and_then(|i| i.created)
            .is_some_and(|created| created.hlc < drop.stamp.hlc),
        "premise: the index is the dropped life's"
    );
    apply_the_pages(&m, &a);
    assert!(
        !holds(&m, &physical, "by_t"),
        "a page's definition of the dropped life is not restored into the physical that stands"
    );
}

#[test]
fn a_drop_of_an_index_after_the_flip_leaves_its_tombstone_under_the_new_physical_and_the_older_creation_is_history_there()
 {
    // PINS Q6.5-1's door: the drop is applied by the target it resolved, so the tombstone was already keyed by the physical's id
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    window(&m, std::slice::from_ref(&definition(&w, "i_1", "t", 7)));
    assert!(
        entries(&m, BOUND, "i_1") > 0,
        "premise: the index is on the new physical with entries"
    );
    let id = kimmy_core::IndexMeta::derive_id("i_1");
    let identity = CollectionId::derive(DB, "orders");
    assert!(
        tombstone(&m, BOUND, "i_1").is_none()
            && m.index_dropped_at(identity, id).expect("reads").is_none(),
        "premise: no tombstone under either id"
    );
    let drop = drop_index_entry(author(&w), "i_1", at(&w, 8), None);
    window(&m, std::slice::from_ref(&drop));
    assert!(!holds(&m, BOUND, "i_1"), "the index is dropped");
    assert_eq!(
        tombstone(&m, BOUND, "i_1"),
        Some(drop.stamp),
        "the tombstone is under the new physical's id, at the drop's stamp"
    );
    assert_eq!(
        m.index_dropped_at(identity, id).expect("reads"),
        None,
        "and none is under the identity's id"
    );
    let outcome = window(&m, &[definition(&w, "i_1", "t", 6), definition(&w, "later_1", "v", 9)]);
    assert!(!outcome.stopped(), "the older creation delivered again is judged, not a stop");
    assert!(holds(&m, BOUND, "later_1"), "a later entry of the same window was applied");
    assert!(
        !holds(&m, BOUND, "i_1"),
        "the older creation is history by the tombstone, and the index is still absent"
    );
}

// Controls for the one site of an index drop that keeps the entry's id (the collection is gone, so no physical stands to name another): an
// ordinary absent name records the tombstone under the name's own id and is an unknown collection, as it always was; a swapped absent name
// records none, because the drop goes to the name's post-drop row.
#[test]
fn an_index_drop_for_an_absent_name_nobody_swapped_records_its_tombstone_under_the_entrys_id() {
    let w = world();
    let (m, _d) = engine();
    let drop = drop_index_entry(author(&w), "i_1", at(&w, 3), None);
    let outcome = window(&m, std::slice::from_ref(&drop));
    assert_eq!(outcome.unknown_collection, 1, "an unknown collection, as it always was");
    assert_eq!(
        m.index_dropped_at(
            CollectionId::derive(DB, "orders"),
            kimmy_core::IndexMeta::derive_id("i_1")
        )
        .expect("reads"),
        Some(drop.stamp),
        "the tombstone is recorded under the name's own id"
    );
}

#[test]
fn an_index_drop_for_a_swapped_name_that_is_absent_goes_to_the_row_and_records_no_tombstone() {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    assert!(absent(&m), "premise: the name is absent under the drop in effect");
    let drop = drop_index_entry(author(&w), "i_1", at(&w, 7), None);
    let outcome = window(&m, std::slice::from_ref(&drop));
    assert!(!outcome.stopped() && outcome.unknown_collection == 0, "not an unknown collection");
    assert_eq!(
        m.index_dropped_at(
            CollectionId::derive(DB, "orders"),
            kimmy_core::IndexMeta::derive_id("i_1")
        )
        .expect("reads"),
        None,
        "no tombstone is recorded under the identity's id: the drop is in the post-drop row"
    );
    assert!(!no_row(&m), "the post-drop row holds it");
}

#[test]
fn a_definition_at_or_below_the_drop_arriving_after_the_bound_stage_took_it_is_history_on_the_stage()
 {
    // the standing physical is a STAGE: the name is found from the stage's own name. The drop is stamped BELOW the binding (W - 5), so the
    // stage stays visible over it (a drop above the flip would bury the bound stage).
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    window(&m, std::slice::from_ref(&drop_below(&w, 5)));
    assert_eq!(served(&m), BOUND, "premise: the bound stage still serves, over a drop below it");
    let wall = w.bind.stamp.hlc.wall_ms;
    let old = stage_index_entry(author(&w), "orders", "old_1", "t", wall - 6);
    let later = stage_index_entry(author(&w), "orders", "later_1", "v", wall - 3);
    let outcome = window(&m, &[old, later]);
    assert!(!outcome.stopped(), "the entries are judged, not a stop");
    assert!(
        !holds(&m, BOUND, "old_1"),
        "a definition at or below the drop in effect is the dropped life's on the stage"
    );
    assert!(holds(&m, BOUND, "later_1"), "and a later entry of the same window was applied");
}

#[test]
fn a_definition_between_two_logical_drops_in_effect_is_history_by_the_newer() {
    // both drops are below the binding (W - 8 and W - 5), so the stage serves over both; the definition is above the older and at or below
    // the newer
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    window(&m, std::slice::from_ref(&drop_below(&w, 8)));
    window(&m, std::slice::from_ref(&drop_below(&w, 5)));
    assert_eq!(served(&m), BOUND, "premise: the bound stage still serves over both drops");
    assert_eq!(logical_drops_in_register(&m), 2, "premise: both drops are in effect");
    let wall = w.bind.stamp.hlc.wall_ms;
    let between = stage_index_entry(author(&w), "orders", "between_1", "t", wall - 6);
    let after = stage_index_entry(author(&w), "orders", "after_1", "v", wall - 3);
    let outcome = window(&m, &[between, after]);
    assert!(!outcome.stopped(), "the entries are judged, not a stop");
    assert!(
        !holds(&m, BOUND, "between_1"),
        "a definition above the older drop and below the newer is the dropped life's: judged by the newest drop in effect"
    );
    assert!(holds(&m, BOUND, "after_1"), "and a later entry of the same window was applied");
}

/// How many logical drops of `orders` are in effect on the member.
fn logical_drops_in_register(m: &Engine) -> usize {
    let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
    let drops = crate::register::timeline(&txn, DB, "orders")
        .expect("the register reads")
        .events()
        .iter()
        .filter(|(_, event)| event.physical().is_none())
        .count();
    txn.abort().expect("the read ends");
    drops
}

// -- The repair and snapshot routes find definitions through the register (ADR-272, decision 6; Q6.5-4) -----------------------------------
//
// Both routes read the collection a definition belongs to by its EXACT name (`get_collection(db, name)`), which on a member that swapped the
// name is the buried identity, or nothing: the repair counted every definition gone, and the snapshot route created the identity again or
// judged the definition against it. They resolve the name through the register as the arms do (`definition_target`), judge a definition by
// the one rule, and for a name absent under a drop in effect send it the way the arms send it: history at or below the drop, otherwise
// into the post-drop row.

/// A member whose bound stage serves over a drop stamped far below the binding and a little below the real clock, so a sender made now is
/// ABOVE the drop: what such a sender offers is the new life's.
fn flipped_over_a_drop_below_a_sender(w: &World) -> (Arc<Engine>, tempfile::TempDir, OplogEntry) {
    let (m, d) = member(w);
    flip(&m, &w.bind, Route::Window);
    let drop = drop_below(w, 31_000);
    window(&m, std::slice::from_ref(&drop));
    assert_eq!(served(&m), BOUND, "premise: the bound stage still serves over the drop");
    (m, d, drop)
}

fn the_page_of_a_repair(m: &Engine, sender: &Engine) -> crate::SnapshotApplied {
    let mut progress = crate::SnapshotProgress::of_collection_definitions(
        CollectionId::derive(DB, "orders"),
        1_000_000,
    );
    let page = sender.snapshot_page(None, progress.scope()).expect("the sender's page");
    m.apply_snapshot_page(sender.node_id(), &mut progress, &page).expect("the repair page applies")
}

#[test]
fn a_repair_page_naming_an_index_above_the_drop_that_the_receiver_lacks_builds_it_on_the_visible_stage()
 {
    let w = world();
    let (m, _d, drop) = flipped_over_a_drop_below_a_sender(&w);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    let created = a
        .get_collection(DB, "orders")
        .expect("the sender holds it")
        .index("by_t")
        .and_then(|i| i.created)
        .expect("the sender's index carries its stamp");
    assert!(
        created.hlc > drop.stamp.hlc,
        "premise: the sender's index is above the drop: the new life's"
    );
    assert!(!holds(&m, BOUND, "by_t"), "premise: the receiver lacks it");
    let done = the_page_of_a_repair(&m, &a);
    assert_eq!(
        done.definitions.built, 1,
        "the repair finds the visible stage through the name and builds the index there ({:?})",
        done.definitions
    );
    assert!(holds(&m, BOUND, "by_t"), "the index is on the visible stage");
}

#[test]
fn a_repair_page_naming_an_index_the_visible_stage_has_a_tombstone_for_counts_it_tombstoned() {
    let w = world();
    let (m, _d, _drop) = flipped_over_a_drop_below_a_sender(&w);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    let created = a
        .get_collection(DB, "orders")
        .expect("the sender holds it")
        .index("by_t")
        .and_then(|i| i.created)
        .expect("the sender's index carries its stamp");
    // this member dropped the index after the sender made it: the tombstone is under the physical that stands, never the identity's
    let at = Stamp::new(
        Hlc::new(created.hlc.wall_ms + 5_000, 0),
        author(&w).node_id(),
        author(&w).epoch(),
    );
    let bound = m.get_collection(DB, BOUND).expect("the stage is held").id;
    m.record_index_drop(bound, kimmy_core::IndexMeta::derive_id("by_t"), at)
        .expect("the tombstone is recorded");
    let done = the_page_of_a_repair(&m, &a);
    assert_eq!(
        done.definitions.tombstoned, 1,
        "the tombstone under the standing physical's id is read ({:?})",
        done.definitions
    );
    assert!(!holds(&m, BOUND, "by_t"), "and nothing is built");
}

#[test]
fn a_repair_page_for_a_swapped_name_that_is_absent_counts_a_definition_at_or_below_the_drop_history_and_never_gone()
 {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    window(&m, std::slice::from_ref(&logical_drop(&w, 5)));
    assert!(absent(&m), "premise: the name is absent under the drop in effect");
    let (a, _da) = a_sender_with_an_old_index("by_t");
    let created = a
        .get_collection(DB, "orders")
        .expect("the sender holds it")
        .index("by_t")
        .and_then(|i| i.created)
        .expect("the sender's index carries its stamp");
    assert!(
        created.hlc <= logical_drop(&w, 5).stamp.hlc,
        "premise: the sender's index is at or below the drop: the dropped life's"
    );
    let done = the_page_of_a_repair(&m, &a);
    assert_eq!(
        (done.definitions.history, done.definitions.gone),
        (1, 0),
        "an absent swapped name judges the definition as the arms do, and is never counted gone ({:?})",
        done.definitions
    );
}

#[test]
fn a_snapshot_page_of_a_life_newer_than_the_identitys_tombstone_creates_no_identity_beside_a_swapped_name_and_its_index_lands_on_the_visible_stage()
 {
    // The identity's rows are still filed under its id (no purger runs in a unit test): the page is not held up by them (the purge gate
    // skips a swapped name), and it creates no identity.
    a_newer_life_page_creates_no_identity_and_its_index_lands_on_the_stage(false);
}

#[test]
fn the_same_page_when_the_identitys_rows_are_already_purged_creates_no_identity_either() {
    // With nothing left to wait for, the page would make `orders` again were it not for the swapped branch: the assertion that no identity
    // is made is reached here (with the rows pending, the old create branch stalls on the purge before it can make one).
    a_newer_life_page_creates_no_identity_and_its_index_lands_on_the_stage(true);
}

fn a_newer_life_page_creates_no_identity_and_its_index_lands_on_the_stage(purged: bool) {
    // AUDIT: `restore_collection` created the page's collection whenever the exact name was absent, so a swapped name whose identity is buried
    // got an ordinary `orders` again (a sender whose life is newer than the tombstone the burial left), and the index was judged against that.
    // The tombstone is set by hand: a burial leaves it at the binding's stamp, which is above any sender a test can build.
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    {
        let low = Stamp::new(Hlc::new(1, 0), author(&w).node_id(), author(&w).epoch());
        let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
        txn.open_table(crate::tables::COLLECTIONS_DROPPED)
            .expect("the table opens")
            .insert(CollectionId::derive(DB, "orders").0, crate::codec::oplog_key(&low).as_slice())
            .expect("the tombstone is lowered");
        txn.commit().expect("the commit lands");
    }
    assert!(
        a.get_collection(DB, "orders").expect("the sender holds it").index("by_t").is_some(),
        "premise: the page offers the index"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the identity is buried here");
    let identity = CollectionId::derive(DB, "orders");
    if purged {
        m.purge_dropped_collection(identity).expect("the identity's purge runs");
        assert!(
            !m.purge_pending(identity).expect("reads"),
            "premise: nothing of the identity's is left to purge"
        );
    } else {
        assert!(
            m.purge_pending(identity).expect("reads"),
            "premise: the identity's rows are still pending a purge"
        );
    }
    apply_the_pages(&m, &a);
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "no ordinary `orders` is created beside the swapped name"
    );
    assert!(
        holds(&m, BOUND, "by_t"),
        "the index the page offers lands on the visible stage, through the name"
    );
}

/// The index the sender's page is made to name: one this build refuses (an empty partial filter has no meaning), stamped as the sender's
/// own index is, so it is above the floor and the drop and reaches the build.
fn an_index_this_build_refuses(sender: &Engine) -> kimmy_core::IndexMeta {
    let mut index = sender
        .get_collection(DB, "orders")
        .expect("the sender holds it")
        .index("by_t")
        .expect("the sender holds the index")
        .clone();
    index.name = "pf_empty".into();
    index.id = kimmy_core::IndexMeta::derive_id("pf_empty");
    index.partial_filter = Some(bson::doc! {});
    index
}

#[test]
fn a_snapshot_page_offering_an_index_the_visible_stage_refuses_leaves_its_hint_under_the_physical_that_stands_and_the_burial_clears_it()
 {
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    let identity = CollectionId::derive(DB, "orders");
    let bound = CollectionId::derive(DB, BOUND);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    let mut progress = crate::SnapshotProgress::of_collection(identity);
    let mut page = a.snapshot_page(None, progress.scope()).expect("the sender's page");
    assert_eq!(page.collections.len(), 1, "premise: the page carries the collection");
    page.collections[0].indexes = vec![an_index_this_build_refuses(&a)];
    assert!(
        m.refusal_hints(identity).expect("reads").is_empty()
            && m.refusal_hints(bound).expect("reads").is_empty(),
        "premise: no hint under either id"
    );
    let done = m.apply_snapshot_page(a.node_id(), &mut progress, &page).expect("the page applies");
    assert_eq!(done.ddl_refused, 1, "premise: the member refused the index");
    assert_eq!(
        m.refusal_hints(bound).expect("reads").len(),
        1,
        "the snapshot route's hint is under the physical that stands"
    );
    assert!(
        m.refusal_hints(identity).expect("reads").is_empty(),
        "and none is under the identity's id, which nothing clears"
    );
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(served(&m), UNBOUND, "premise: the second flip buries the first physical");
    assert!(
        m.refusal_hints(bound).expect("reads").is_empty(),
        "the burial clears the snapshot route's hint with the physical"
    );
}

#[test]
fn a_repair_page_offering_an_index_the_visible_stage_refuses_leaves_its_hint_under_the_physical_that_stands()
 {
    let w = world();
    let (m, _d, drop) = flipped_over_a_drop_below_a_sender(&w);
    let identity = CollectionId::derive(DB, "orders");
    let bound = CollectionId::derive(DB, BOUND);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    let refused = an_index_this_build_refuses(&a);
    assert!(
        refused.created.is_some_and(|created| created.hlc > drop.stamp.hlc),
        "premise: the index is above the drop, so it reaches the build"
    );
    let mut progress = crate::SnapshotProgress::of_collection_definitions(identity, 1_000_000);
    let mut page = a.snapshot_page(None, progress.scope()).expect("the sender's page");
    assert_eq!(page.collections.len(), 1, "premise: the page carries the collection");
    page.collections[0].indexes = vec![refused];
    assert!(
        m.refusal_hints(identity).expect("reads").is_empty()
            && m.refusal_hints(bound).expect("reads").is_empty(),
        "premise: no hint under either id"
    );
    let done =
        m.apply_snapshot_page(a.node_id(), &mut progress, &page).expect("the repair page applies");
    assert_eq!(
        done.definitions.refused, 1,
        "premise: the repair counted the index refused ({:?})",
        done.definitions
    );
    assert_eq!(
        m.refusal_hints(bound).expect("reads").len(),
        1,
        "the repair route's hint is under the physical that stands"
    );
    assert!(
        m.refusal_hints(identity).expect("reads").is_empty(),
        "and none is under the identity's id, which nothing clears"
    );
}

#[test]
fn the_definition_repair_reads_no_collection_by_its_exact_name() {
    // a source guard in the manner of `resolve_door.rs`: the repair's reads go through the register's door
    let source = include_str!("definition_repair.rs");
    assert!(
        !source.contains("self.get_collection(&state.db, &state.name)"),
        "definition_repair.rs finds a collection by `get_collection(db, name)`: use `definition_target`"
    );
}

#[test]
fn a_definition_repair_page_naming_an_index_at_or_below_the_drop_counts_it_history_on_the_recreation()
 {
    let w = world();
    let (m, _d, physical, drop) = recreation_visible_over_the_drop_in_effect(&w);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    let created = a
        .get_collection(DB, "orders")
        .expect("the sender holds it")
        .index("by_t")
        .expect("the sender holds the index")
        .created
        .expect("the index carries its stamp");
    assert!(created.hlc < drop.stamp.hlc, "premise: the index is the dropped life's");
    let mut progress = crate::SnapshotProgress::of_collection_definitions(
        CollectionId::derive(DB, "orders"),
        1_000_000,
    );
    let page = a.snapshot_page(None, progress.scope()).expect("the sender's page");
    let done =
        m.apply_snapshot_page(a.node_id(), &mut progress, &page).expect("the repair page applies");
    assert_eq!(
        done.definitions.history, 1,
        "the repair judges the definition by the one rule: history on the recreation ({:?})",
        done.definitions
    );
    assert!(!holds(&m, &physical, "by_t"), "and nothing is built on the recreation");
}

/// A member that holds the stages and an identity, with a logical drop of the name in effect stamped at the real clock: the name is absent,
/// and a sender made a little later is ABOVE the drop, so what it offers is the new life's.
fn absent_under_a_drop_below_a_sender(w: &World) -> (Arc<Engine>, tempfile::TempDir, OplogEntry) {
    let (m, d) = member(w);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let drop = OplogEntry {
        stamp: Stamp::new(Hlc::new(physical_now_ms(), 0), author(w).node_id(), author(w).epoch()),
        kind: OpKind::DropCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders"))
                .expect("the drop encodes"),
        ),
        meta: None,
    };
    window(&m, std::slice::from_ref(&drop));
    assert!(absent(&m), "premise: the name is absent under the drop in effect");
    std::thread::sleep(std::time::Duration::from_millis(5));
    (m, d, drop)
}

#[test]
fn a_repair_page_naming_an_index_above_the_drop_for_an_absent_swapped_name_is_written_to_the_row_and_built_when_the_next_physical_is_visible()
 {
    let w = world();
    let (m, _d, drop) = absent_under_a_drop_below_a_sender(&w);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    let created = a
        .get_collection(DB, "orders")
        .expect("the sender holds it")
        .index("by_t")
        .and_then(|i| i.created)
        .expect("the sender's index carries its stamp");
    assert!(created.hlc > drop.stamp.hlc, "premise: the sender's index is above the drop");
    let done = the_page_of_a_repair(&m, &a);
    assert_eq!(
        (done.definitions.to_row, done.definitions.gone, done.definitions.built),
        (1, 0, 0),
        "an absent swapped name sends it to the post-drop row: not gone, and not built ({:?})",
        done.definitions
    );
    assert_eq!(names(&the_row(&m)), vec!["by_t".to_string()], "the row holds it");
    m.create_collection(DB, "orders").expect("the name is made again");
    assert!(holds(&m, &served(&m), "by_t"), "the next physical that becomes visible has it");
}

#[test]
fn a_snapshot_page_naming_an_index_above_the_drop_for_an_absent_swapped_name_is_written_to_the_row()
{
    let w = world();
    let (m, _d, drop) = absent_under_a_drop_below_a_sender(&w);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    assert!(
        a.get_collection(DB, "orders")
            .expect("the sender holds it")
            .index("by_t")
            .and_then(|i| i.created)
            .is_some_and(|created| created.hlc > drop.stamp.hlc),
        "premise: the page offers an index above the drop"
    );
    apply_the_pages(&m, &a);
    assert_eq!(names(&the_row(&m)), vec!["by_t".to_string()], "the row holds it");
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "and no identity is made beside the swapped name"
    );
}

#[test]
fn a_snapshot_page_of_an_older_sender_is_judged_per_definition_and_not_refused_whole_by_the_natural_tombstone()
 {
    // The commonest page: a sender whose identity predates the flip, against the tombstone the burial left (the binding's stamp). It used to
    // be refused whole as an older life; the swapped name judges each definition, as the arms do (a behaviour change, intended): the old
    // identity's index lands on the stage, no ordinary `orders` is made, and the page's documents are history.
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    let sender = a.get_collection(DB, "orders").expect("the sender holds it");
    a.insert(&sender, doc! { "_id": 901, "t": 1 }).expect("the sender holds a document");
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the identity is buried here");
    assert!(
        m.collection_dropped_at(CollectionId::derive(DB, "orders"))
            .expect("reads")
            .is_some_and(|tombstone| sender.created < tombstone.hlc),
        "premise: the sender's life is older than the tombstone the burial left"
    );
    apply_the_pages(&m, &a);
    assert!(holds(&m, BOUND, "by_t"), "the old identity's index lands on the stage");
    assert!(m.get_collection(DB, "orders").is_err(), "no ordinary `orders` is made");
    assert!(!has_doc(&m, BOUND, 901), "the page's documents are history: they are not the stage's");
}

#[test]
fn a_snapshot_page_of_a_swapped_name_older_than_its_databases_drop_is_refused_and_leaves_the_tombstone_a_later_entry_needs()
 {
    // ADR-229: the early branch for a swapped name must keep the database's refusal the create branch makes, so that the entries which follow
    // are history and not a stop
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    assert!(m.drop_database(DB).expect("the database drops"), "premise: the database is dropped");
    {
        use redb::ReadableDatabase;
        let txn = m.db().begin_read().expect("a read");
        assert!(
            crate::register::is_swapped(&txn, DB, "orders").expect("the register reads"),
            "premise: the name is still swapped after the drop (its register rows survive the burial), or the page takes the ordinary create branch"
        );
    }
    let dropped = m
        .database_dropped_at(DB)
        .expect("reads")
        .expect("premise: the database holds its tombstone");
    let id = CollectionId::derive(DB, "orders");
    assert_ne!(
        m.collection_dropped_at(id).expect("reads"),
        Some(dropped),
        "premise: the name's own tombstone is not yet at the database's drop"
    );
    apply_the_pages(&m, &a);
    assert_eq!(
        m.collection_dropped_at(id).expect("reads"),
        Some(dropped),
        "the refusal leaves the collection's tombstone at the database's drop"
    );
}

#[test]
fn a_repair_page_for_a_swapped_name_whose_visible_stage_holds_more_than_the_gate_is_counted_too_large_and_builds_nothing()
 {
    // the live count is read under the physical that stands: the identity's id has no rows, so a count read there would pass the gate
    let w = world();
    let (m, _d, _drop) = flipped_over_a_drop_below_a_sender(&w);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    assert!(
        m.count_by_id(m.get_collection(DB, BOUND).expect("the stage is held").id)
            .expect("the count reads")
            .is_some_and(|live| live > 1),
        "premise: the visible stage holds more documents than the gate"
    );
    let mut progress =
        crate::SnapshotProgress::of_collection_definitions(CollectionId::derive(DB, "orders"), 1);
    let page = a.snapshot_page(None, progress.scope()).expect("the sender's page");
    let done =
        m.apply_snapshot_page(a.node_id(), &mut progress, &page).expect("the repair page applies");
    assert_eq!(
        done.definitions.too_large, 1,
        "the visible stage's live count is over the gate ({:?})",
        done.definitions
    );
    assert!(!holds(&m, BOUND, "by_t"), "and nothing is built");
}

#[test]
fn a_snapshot_page_naming_a_vector_configuration_for_a_swapped_name_is_discarded_and_counted() {
    // a page is a delivery (ruled): a vector configuration it names for a swapped name is discarded, warned and counted in
    // `kimmy_out_vector_config_discarded_total`, as an entry's is, and none stands on the visible stage
    let w = world();
    let (m, _d) = member(&w);
    flip(&m, &w.bind, Route::Window);
    let (a, _da) = a_sender_with_an_old_index("by_t");
    a.configure_vectors(
        DB,
        "orders",
        kimmy_core::VectorConfig {
            fields: vec!["body".into()],
            provider: kimmy_core::ProviderConfig::Byo {},
            dim: 8,
            metric: Default::default(),
            document_prefix: None,
            query_prefix: None,
            chunk: Default::default(),
        },
    )
    .expect("the sender configures vectors");
    assert!(
        a.get_collection(DB, "orders").expect("the sender holds it").vector.is_some(),
        "premise: the page offers a vector configuration"
    );
    let before = m.out_vector_config_discarded();
    apply_the_pages(&m, &a);
    assert_eq!(
        m.out_vector_config_discarded(),
        before + 1,
        "the page's vector configuration for a swapped name is counted, once"
    );
    assert!(
        m.get_collection(DB, BOUND).expect("the stage is held").vector.is_none(),
        "and none stands on the visible stage"
    );
}
