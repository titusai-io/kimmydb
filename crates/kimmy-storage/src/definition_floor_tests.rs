//! The definition floor (ADR-272, decision 6; Q6.5-2, series B4): a physical that becomes visible by a binding or a recreation has no
//! incarnation floor and no tombstone of its own, so a definition of an earlier life of the name, served again after the flip, was history on
//! the old physical and not on the new one: the index was built on the member that received the entry after the flip and on no other. The
//! join carries the boundary: the new physical takes the greatest of the old physical's incarnation floor, the old physical's own definition
//! floor and the `hlc` of the tombstone the old physical's id held BEFORE its burial (which moves that tombstone to the binding's stamp),
//! and the one rule a definition is judged by (`definition_of_an_ended_life`) reads it as a third clause. It judges definitions only,
//! never documents: a stage's rows are the job's output.
//!
//! The fixture: an author who made `orders` with an index and a document, dropped it at `T` and made it again, then two stages and the
//! entries of the swap; members that hold the second life (whose incarnation floor is `T`) and are handed the entries in the order each
//! test needs.

use std::sync::Arc;

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, CollectionRef, DocId, Enforcement, Hlc, IndexCreate, IndexField,
    IndexMeta, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
const FIRST: &str = "orders.__stg.s1";
const SECOND: &str = "orders.__stg.s2";
const INDEX: &str = "t_idx";

fn engine() -> (Arc<Engine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    (Arc::new(Engine::open(&dir.path().join("kimmy.redb")).expect("an engine opens")), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .expect("the oplog is readable")
        .entries
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().expect("the member's vector reads");
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
    .expect("the window applies");
    outcome
}

fn orders_id() -> CollectionId {
    CollectionId::derive(DB, "orders")
}

fn bind_of(a: &Engine, wall_ms: u64, physical: &str) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::BindCollection,
        collection: orders_id(),
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

fn drop_of(a: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::DropCollection,
        collection: orders_id(),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionRef::new(DB, "orders")).expect("the drop encodes"),
        ),
        meta: None,
    }
}

/// What the author made, as entries.
struct World {
    /// The creation of the first life, the drop, the creation of the second life (the prefix a member holds the second life from).
    life: Vec<OplogEntry>,
    /// The creation of both stages.
    stages: Vec<OplogEntry>,
    /// The index of the FIRST life, stamped below `T`.
    first_life_index: OplogEntry,
    /// A document of the first life, stamped below `T`, addressed to the first life's collection.
    first_life_doc: OplogEntry,
    /// The hlc of the drop at `T`.
    t: Hlc,
    bind_first: OplogEntry,
    bind_second: OplogEntry,
    /// A logical drop of the name, stamped above the stages' creation and below the second binding.
    drop_between: OplogEntry,
    _author: (Engine, tempfile::TempDir),
    /// The author of the stages when they are made apart from the life (so the life's author never held them).
    _stage_author: Option<(Engine, tempfile::TempDir)>,
}

fn world() -> World {
    world_with(false)
}

/// A world whose stages are created BELOW the drop at `T` (before the first life is made), as when the swap began before the drop.
fn world_below() -> World {
    world_with(true)
}

fn world_with(stages_first: bool) -> World {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let a = Engine::open(&dir.path().join("kimmy.redb")).expect("the author's engine opens");
    let stage_author = stages_first.then(|| {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let s =
            Engine::open(&dir.path().join("kimmy.redb")).expect("the stage author's engine opens");
        (s, dir)
    });
    let make_stages = |a: &Engine| {
        let now = physical_now_ms();
        a.create_stage(DB, FIRST, MergePolicy::Document, Hlc::new(now + 120_000, 0))
            .expect("the first stage is created");
        a.create_stage(DB, SECOND, MergePolicy::Document, Hlc::new(now + 121_000, 0))
            .expect("the second stage is created");
    };
    if let Some((s, _)) = &stage_author {
        // the stages are made by another writer, before the life: they are stamped below the drop, and the life's author never held them
        make_stages(s);
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let first = a.create_collection(DB, "orders").expect("the first life is created");
    a.create_index(DB, "orders", vec![IndexField::ascending("t")], false, Some(INDEX.into()))
        .expect("the first life takes an index");
    a.insert(&first, doc! { "_id": 77, "t": 1 }).expect("the first life takes a document");
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(
        a.drop_collection(DB, "orders").expect("the drop lands"),
        "premise: a life was dropped"
    );
    // the rows of the first life are purged before the name can be made again
    a.finish_purges_now().expect("the purge of the first life finishes");
    std::thread::sleep(std::time::Duration::from_millis(5));
    a.create_collection(DB, "orders").expect("the second life is created");
    if !stages_first {
        make_stages(&a);
    }
    let now = physical_now_ms();
    let mut all = everything(&a);
    if let Some((s, _)) = &stage_author {
        all.extend(everything(s));
    }
    let of = |kind: OpKind, id: CollectionId| -> Vec<OplogEntry> {
        all.iter().filter(|e| e.kind == kind && e.collection == id).cloned().collect()
    };
    let creations = of(OpKind::CreateCollection, orders_id());
    assert_eq!(creations.len(), 2, "premise: two lives were created");
    let drop = of(OpKind::DropCollection, orders_id());
    assert_eq!(drop.len(), 1, "premise: one drop");
    let t = drop[0].stamp.hlc;
    let first_life_index = of(OpKind::CreateIndex, orders_id()).remove(0);
    let first_life_doc = of(OpKind::Insert, orders_id()).remove(0);
    assert!(
        first_life_index.stamp.hlc < t && first_life_doc.stamp.hlc < t,
        "premise: the index and the document are of the life the drop ended"
    );
    let mut stages = of(OpKind::CreateCollection, CollectionId::derive(DB, FIRST));
    stages.extend(of(OpKind::CreateCollection, CollectionId::derive(DB, SECOND)));
    assert_eq!(stages.len(), 2, "premise: both stages' creations");
    let life = vec![creations[0].clone(), drop[0].clone(), creations[1].clone()];
    let wall = all.iter().map(|e| e.stamp.hlc.wall_ms).max().expect("entries").max(now) + 10;
    let bind_first = bind_of(&a, wall, FIRST);
    let drop_between = drop_of(&a, wall + 10);
    let bind_second = bind_of(&a, wall + 20, SECOND);
    World {
        life,
        stages,
        first_life_index,
        first_life_doc,
        t,
        bind_first,
        bind_second,
        drop_between,
        _author: (a, dir),
        _stage_author: stage_author,
    }
}

/// A member holding the second life of `orders` (its incarnation floor is `T`) and both stages.
fn member(w: &World) -> (Arc<Engine>, tempfile::TempDir) {
    let (m, d) = engine();
    m.apply_batch(&w.life).expect("the lives apply");
    m.apply_batch(&w.stages).expect("the stages apply");
    let held = m.get_collection(DB, "orders").expect("premise: the second life is held");
    assert_eq!(held.incarnation_floor, Some(w.t), "premise: the identity's incarnation floor is T");
    (m, d)
}

fn index_on(m: &Engine, physical: &str) -> bool {
    m.get_collection(DB, physical).expect("the physical is held").index(INDEX).is_some()
}

fn floor_of(m: &Engine, physical: &str) -> Option<Hlc> {
    m.get_collection(DB, physical).expect("the physical is held").definition_floor
}

fn served(m: &Engine) -> String {
    m.resolve(DB, "orders").expect("the name resolves").physical
}

// -- Test 17: the definition floor -------------------------------------------------------------------------------------------------------

#[test]
fn a_definition_of_the_first_life_is_history_on_the_physical_a_flip_made_visible_and_a_document_below_the_floor_is_still_applied()
 {
    let w = world();
    // two members that differ in what they heard before the flip: one has had the entry (and refused it), the other has not
    let (heard, _d1) = member(&w);
    let (unheard, _d2) = member(&w);
    window(&heard, std::slice::from_ref(&w.first_life_index));
    assert!(
        !index_on(&heard, "orders"),
        "premise: the entry is history on the identity, by its incarnation floor"
    );
    assert_eq!(
        floor_of(&heard, FIRST),
        None,
        "premise: the stage has no definition floor before the flip"
    );

    for m in [&heard, &unheard] {
        window(m, std::slice::from_ref(&w.bind_first));
        assert_eq!(served(m), FIRST, "premise: the flip");
        assert_eq!(
            floor_of(m, FIRST),
            Some(w.t),
            "the stage takes the floor of the life it replaces: its value, `T`"
        );
        let outcome = window(m, std::slice::from_ref(&w.first_life_index));
        assert!(!outcome.stopped(), "the entry is judged, not a stop");
        assert!(
            !index_on(m, FIRST),
            "the first life's index, served again after the flip, is history on the stage as it was on the identity"
        );
    }
    // the floor judges definitions and never documents: a document stamped below it, addressed to the stage, is applied
    let stage_id = CollectionId::derive(DB, FIRST);
    let late = OplogEntry { collection: stage_id, ..w.first_life_doc.clone() };
    window(&unheard, std::slice::from_ref(&late));
    let stage = unheard.get_collection(DB, FIRST).expect("the stage is held");
    assert!(
        unheard.get(&stage, &DocId::Int64(77)).expect("the stage reads").is_some(),
        "a document at or below the floor is applied to the stage: the floor is not an incarnation floor"
    );
}

#[test]
fn the_floor_is_carried_from_a_physical_that_had_taken_it_to_the_next_flip() {
    let w = world();
    let (m, _d) = member(&w);
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(floor_of(&m, FIRST), Some(w.t), "premise: the first flip gave the stage the floor");
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(served(&m), SECOND, "premise: the second flip");
    assert!(m.get_collection(DB, FIRST).is_err(), "premise: the first stage is buried");
    assert_eq!(
        floor_of(&m, SECOND),
        Some(w.t),
        "the second stage takes the first one's own definition floor"
    );
    window(&m, std::slice::from_ref(&w.first_life_index));
    assert!(!index_on(&m, SECOND), "and the first life's index is history on it");
}

#[test]
fn the_floor_includes_the_tombstone_the_old_id_held_before_its_burial_and_not_the_bindings_stamp() {
    // the identity has no incarnation floor and holds a tombstone of its own id: a drop that this life survived
    // Is there a product route that leaves a tombstone on a standing identity with NO incarnation floor? The one I found by reading: a replicated
    // drop aimed at a previous incarnation is ignored and its tombstone is still recorded (sync.rs:2884-2892, `record_collection_drop`),
    // without a floor, on a life created above it. The tombstone it leaves is below the life's `created`, so the term can only return a value
    // under the life's own creation: this test sets a tombstone above `created` by hand, which that route does not leave. The term is therefore
    // close to DEFENSIVE (nothing in a product state needs it to decide); it stays a live mutant on this test.
    let w = world();
    let (m, _d) = engine();
    m.create_collection(DB, "orders").expect("the identity is created");
    let tombstone =
        Stamp::new(Hlc::new(w.bind_first.stamp.hlc.wall_ms - 5, 0), m.node_id(), m.epoch());
    m.record_collection_drops(&[(orders_id(), tombstone)]).expect("the tombstone is recorded");
    m.apply_batch(&w.stages).expect("the stages apply");
    assert_eq!(
        m.get_collection(DB, "orders").expect("held").incarnation_floor,
        None,
        "premise: no incarnation floor"
    );
    assert_eq!(
        m.collection_dropped_at(orders_id()).expect("reads"),
        Some(tombstone),
        "premise: the id holds a tombstone"
    );
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(served(&m), FIRST, "premise: the flip");
    assert_eq!(
        floor_of(&m, FIRST),
        Some(tombstone.hlc),
        "the stage takes the tombstone the identity held, read before the burial moved it to the binding's stamp"
    );
}

#[test]
fn the_floor_reaches_the_next_physical_through_the_post_drop_row() {
    let w = world();
    let (m, _d) = member(&w);
    // a logical drop of the name ends the identity (its floor goes into the name's row), and a stage bound above it takes the row
    m.apply_batch(std::slice::from_ref(&w.bind_second)).expect("the binding is stored ahead");
    window(&m, std::slice::from_ref(&w.drop_between));
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the drop buried the identity");
    let row = crate::register::post_row_for_tests(&m, DB, "orders").expect("the row reads");
    assert!(row.is_some(), "the drop wrote a row for the name, though it holds no definition");
    assert_eq!(
        row.map(|row| row.definition_floor),
        Some(Some(w.t)),
        "the drop wrote the name's row, carrying the floor of what it buried"
    );
    let mut granted = VersionVector::default();
    granted.observe(w.bind_second.stamp);
    m.absorb_version_vector(&granted).expect("the grant is absorbed");
    assert_eq!(served(&m), SECOND, "premise: the binding is visible");
    assert_eq!(
        floor_of(&m, SECOND),
        Some(w.t),
        "the physical that becomes visible takes the floor from the row"
    );
    assert!(
        crate::register::post_row_for_tests(&m, DB, "orders").expect("reads").is_none(),
        "premise: the row is gone"
    );
}

#[test]
fn a_flip_over_an_identity_that_never_had_a_floor_leaves_the_stage_with_none() {
    // control: nothing to carry, nothing invented
    let w = world();
    let (m, _d) = engine();
    m.create_collection(DB, "orders").expect("the identity is created");
    m.apply_batch(&w.stages).expect("the stages apply");
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(served(&m), FIRST, "premise: the flip");
    assert_eq!(floor_of(&m, FIRST), None, "a floor is carried, never made up");
}

/// The creation of the index `INDEX` on a stage as Q6.7's creation entry will carry it: a replicated `CreateIndex` addressed to the stage,
/// stamped at `wall_ms` by the author `by`.
fn stage_index_entry(by: &Engine, stage: &str, wall_ms: u64) -> OplogEntry {
    let index = IndexMeta {
        id: IndexMeta::derive_id(INDEX),
        name: INDEX.into(),
        fields: vec![IndexField::ascending("t")],
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

#[test]
fn the_join_carries_no_definition_at_or_below_the_floor_of_the_physical_it_leaves() {
    // the first stage holds, by hand, an index stamped below `T` (nothing has given it a floor yet); the first flip gives it the floor and
    // floors its own copy at it, so nothing at or below the floor is left for the second flip to carry (the join's own floor, which would
    // refuse it, is a second line of defence that no product state reaches while the flip floors the new physical itself)
    let w = world();
    let (m, _d) = member(&w);
    let author = &w._author.0;
    let below = stage_index_entry(author, FIRST, w.t.wall_ms.saturating_sub(1));
    assert!(below.stamp.hlc < w.t, "premise: the index is stamped below T");
    window(&m, std::slice::from_ref(&below));
    assert!(index_on(&m, FIRST), "premise: the stage holds the index, built over its rows");
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(floor_of(&m, FIRST), Some(w.t), "premise: the first flip gave the stage the floor");
    assert!(
        !index_on(&m, FIRST),
        "the flip floored the stage's own copy at the floor it was assigned: the definition below T is gone"
    );
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(served(&m), SECOND, "premise: the second flip");
    assert!(
        !index_on(&m, SECOND),
        "a definition at or below the floor of the physical that is left is not carried into the next one"
    );
}

#[test]
fn the_incarnation_floor_alone_is_a_source_of_the_floor() {
    // no tombstone of the identity's id and no definition floor: only the incarnation floor, above anything else, can be the value
    let w = world();
    let (m, _d) = engine();
    let mut meta = m.create_collection(DB, "orders").expect("the identity is created");
    let floor = Hlc::new(w.bind_first.stamp.hlc.wall_ms - 5, 0);
    meta.incarnation_floor = Some(floor);
    {
        let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
        Engine::put_collection_meta(&txn, &meta).expect("the metadata is written");
        txn.commit().expect("the commit lands");
    }
    m.apply_batch(&w.stages).expect("the stages apply");
    assert_eq!(m.collection_dropped_at(orders_id()).expect("reads"), None, "premise: no tombstone");
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(served(&m), FIRST, "premise: the flip");
    assert_eq!(floor_of(&m, FIRST), Some(floor), "the incarnation floor is the stage's floor");
}

fn put_meta(m: &Engine, meta: &crate::meta::CollectionMeta) {
    let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
    Engine::put_collection_meta(&txn, meta).expect("the metadata is written");
    txn.commit().expect("the commit lands");
}

#[test]
fn the_floor_is_the_greatest_of_the_three_sources_when_the_greatest_is_neither_the_first_nor_the_last()
 {
    // the old physical's incarnation floor, its own definition floor and the tombstone of its id are three different values, the
    // greatest in the middle: no first, last or least of them is the answer
    let w = world();
    let (m, _d) = member(&w);
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(served(&m), FIRST, "premise: the first flip");
    let base = w.t.wall_ms;
    let (low, mid, high) = (Hlc::new(base + 1, 0), Hlc::new(base + 2, 0), Hlc::new(base + 3, 0));
    let mut first = m.get_collection(DB, FIRST).expect("the first stage is held");
    first.incarnation_floor = Some(mid);
    first.definition_floor = Some(high);
    put_meta(&m, &first);
    let tombstone = Stamp::new(low, m.node_id(), m.epoch());
    m.record_collection_drops(&[(first.id, tombstone)]).expect("the tombstone is recorded");
    assert_eq!(
        m.collection_dropped_at(first.id).expect("reads"),
        Some(tombstone),
        "premise: the old id holds a tombstone below both floors"
    );
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(served(&m), SECOND, "premise: the second flip");
    assert_eq!(
        floor_of(&m, SECOND),
        Some(high),
        "the floor is the greatest of the three, and that is the old physical's own definition floor, the value between the other two"
    );
}

#[test]
fn a_definition_between_the_floor_and_a_later_drop_that_the_standing_physical_holds_is_not_carried_by_the_next_flip()
 {
    // the drop `d` is above the first life's floor `T`; the first stage was made visible above `d` and took the floor `T`; a definition
    // stamped between `T` and `d` reached it afterwards (nothing refuses it there yet), and the join into the second stage floors at `d`
    let w = world();
    let (m, _d) = member(&w);
    let wall = w.bind_first.stamp.hlc.wall_ms;
    let drop = drop_of(&w._author.0, wall - 5);
    let between = stage_index_entry(&w._author.0, "orders", wall - 7);
    assert!(
        between.stamp.hlc > w.t && between.stamp.hlc < drop.stamp.hlc,
        "premise: the definition is stamped between the floor and the drop"
    );
    window(&m, std::slice::from_ref(&drop));
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(served(&m), FIRST, "premise: the first stage is visible above the drop");
    assert_eq!(floor_of(&m, FIRST), Some(w.t), "premise: it took the floor T");
    window(&m, std::slice::from_ref(&between));
    assert!(
        index_on(&m, FIRST),
        "premise: the definition between T and the drop is on the first stage"
    );
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(served(&m), SECOND, "premise: the second flip");
    assert!(
        !index_on(&m, SECOND),
        "the join floors at the later of the drop and the floor of the physical it leaves: the drop"
    );
}

#[test]
fn the_join_floors_at_the_greater_of_the_drop_and_the_floor_of_the_physical_it_leaves_when_that_floor_is_above_the_drop()
 {
    // The join's floor matters on its own only where the left physical's floor `T2` is ABOVE the newest logical drop `d` in effect: the
    // flip's own flooring of the new physical at `d` (the step after the join) removes everything at or below `d`, so below `T2 <= d`
    // the join's choice decides nothing. A real member reaches `T2 > d` when the floor came from an ordinary drop of the identity at `T2`,
    // made before it heard of any stage, while the logical drop `d` (below `T2`) came from a member that had: here the first stage holds a
    // definition stamped between `d` and `T2`, written before the floor reached it (it is applied, as it would be on that member), and the
    // floor is then raised by hand to `T2`, which is that state. The definition must not be carried by the next flip.
    // (A drop at `T2` arriving AFTER a flip would itself be a logical drop and become the newest drop in effect, so this is observable only
    // because `T2` comes from the identity's floor before the flip. Since the flip floors the new physical at the greater of the drop and its
    // own floor after the join, the join's own floor is a second line of defence here and is kept as one.)
    let w = world();
    let (m, _d) = member(&w);
    let wall = w.bind_first.stamp.hlc.wall_ms;
    let drop = drop_of(&w._author.0, wall - 5);
    let between = stage_index_entry(&w._author.0, "orders", wall - 3);
    assert!(between.stamp.hlc > drop.stamp.hlc, "premise: the definition is above the drop");
    window(&m, std::slice::from_ref(&drop));
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(served(&m), FIRST, "premise: the first stage is visible above the drop");
    window(&m, std::slice::from_ref(&between));
    assert!(
        index_on(&m, FIRST),
        "premise: the definition between the drop and T2 is on the first stage"
    );
    let t2 = Hlc::new(wall - 2, 0);
    assert!(
        t2 > drop.stamp.hlc && t2 >= between.stamp.hlc,
        "premise: T2 is above the drop and the definition"
    );
    let mut first = m.get_collection(DB, FIRST).expect("the first stage is held");
    first.definition_floor = Some(t2);
    put_meta(&m, &first);
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(served(&m), SECOND, "premise: the second flip");
    assert!(
        !index_on(&m, SECOND),
        "the join floors at the greater of the drop and the held floor: a definition between the drop and the held floor is not carried"
    );
}

/// An index on `physical`, named `name` on `path`, as Q6.7's creation entry will carry it: a replicated `CreateIndex` addressed to the
/// physical, stamped at `wall_ms` by the author `by`.
fn named_index_entry(
    by: &Engine,
    physical: &str,
    name: &str,
    path: &str,
    wall_ms: u64,
) -> OplogEntry {
    let index = IndexMeta {
        id: IndexMeta::derive_id(name),
        name: name.into(),
        fields: vec![IndexField::ascending(path)],
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
        collection: CollectionId::derive(DB, physical),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&IndexCreate {
                db: DB.into(),
                collection: physical.into(),
                index,
            })
            .expect("the creation body encodes"),
        ),
        meta: None,
    }
}

/// A document written by the author to `physical` at `wall_ms`, as an entry.
fn doc_on(w: &World, physical: &str, id: i64, wall_ms: u64) -> OplogEntry {
    let a = &w._author.0;
    let scratch = a
        .get_collection(DB, "scratch")
        .or_else(|_| a.create_collection(DB, "scratch"))
        .expect("the author's scratch collection");
    // written once: a test that builds several members from one world asks for the same document again
    let written = |a: &Engine| {
        everything(a).into_iter().rev().find(|e| {
            e.kind == OpKind::Insert
                && e.collection == scratch.id
                && e.doc_id == Some(DocId::Int64(id))
        })
    };
    if written(a).is_none() {
        a.insert(&scratch, bson::doc! { "_id": id, "t": id, "v": id })
            .expect("the author writes a document");
    }
    let mut entry = written(a).expect("the insert is in the author's oplog");
    entry.collection = CollectionId::derive(DB, physical);
    entry.stamp = Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch());
    entry
}

/// How many entries the index `name` holds on `physical` (0 when it has no such index).
fn entries_of_index(m: &Engine, physical: &str, name: &str) -> usize {
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

/// The first stage's own copy, by hand: three documents and two indexes, `x` at `x_wall` and `kept` above everything.
fn first_stage_with_a_copy(m: &Engine, w: &World, x_wall: u64) {
    let wall = w.bind_first.stamp.hlc.wall_ms;
    window(
        m,
        &[
            doc_on(w, FIRST, 201, wall - 8),
            doc_on(w, FIRST, 202, wall - 8),
            doc_on(w, FIRST, 203, wall - 8),
            named_index_entry(&w._author.0, FIRST, INDEX, "t", x_wall),
            named_index_entry(&w._author.0, FIRST, "kept", "v", wall + 7),
        ],
    );
    assert!(entries_of_index(m, FIRST, INDEX) > 0, "premise: x is on the first stage with entries");
    assert!(
        entries_of_index(m, FIRST, "kept") > 0,
        "premise: the index above the floor is on it with entries"
    );
}

#[test]
fn a_flip_floors_the_new_physicals_own_copy_at_the_floor_it_was_assigned_even_with_no_drop_in_effect()
 {
    // The identity's own floor `T2` (set before anything else, as an ordinary drop of the identity at `T2` leaves it). The first stage
    // carries a copy of the target's definitions, by hand (as Q6.7's creation entry will): `x` stamped in `T2`'s own hlc, and `kept`
    // above it. The flip assigns the stage the floor, and the stage itself must not keep what its floor calls history.
    let w = world();
    let (m, _d) = member(&w);
    let wall = w.bind_first.stamp.hlc.wall_ms;
    let t2 = Hlc::new(wall - 2, 0);
    let mut identity = m.get_collection(DB, "orders").expect("the identity is held");
    identity.incarnation_floor = Some(t2);
    put_meta(&m, &identity);
    first_stage_with_a_copy(&m, &w, t2.wall_ms);
    assert_eq!(floor_of(&m, FIRST), None, "premise: the stage has no floor before the flip");
    assert_eq!(logical_drops_in_effect(&m), 0, "premise: no logical drop is in effect");
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(served(&m), FIRST, "premise: the flip");
    assert_eq!(floor_of(&m, FIRST), Some(t2), "premise: the flip assigned the stage the floor T2");
    assert!(
        !index_on(&m, FIRST),
        "the flip floors the new physical's own copy at the floor it was assigned: x, at T2's own hlc, is gone"
    );
    assert_eq!(entries_of_index(&m, FIRST, INDEX), 0, "and so are its entries");
    assert!(
        entries_of_index(&m, FIRST, "kept") > 0,
        "an index above the floor stays, with its entries"
    );
}

#[test]
fn two_members_that_meet_the_floor_before_the_flip_and_after_it_end_with_the_same_definitions() {
    // Member 1 knows the floor `T2` (the identity's own) before the flip; member 2 meets the flip first and the drop at `T2` after it, as a
    // logical drop below the visible stage. Each is asserted absolutely: no x, and the index above `T2` is present with entries.
    for floor_first in [true, false] {
        let w = world();
        let (m, _d) = member(&w);
        let wall = w.bind_first.stamp.hlc.wall_ms;
        let t2 = Hlc::new(wall - 2, 0);
        let order = if floor_first {
            "the floor, then the flip"
        } else {
            "the flip, then the drop at the floor"
        };
        if floor_first {
            let mut identity = m.get_collection(DB, "orders").expect("the identity is held");
            identity.incarnation_floor = Some(t2);
            put_meta(&m, &identity);
        }
        first_stage_with_a_copy(&m, &w, t2.wall_ms);
        window(&m, std::slice::from_ref(&w.bind_first));
        assert_eq!(served(&m), FIRST, "premise: the flip ({order})");
        if !floor_first {
            assert!(index_on(&m, FIRST), "premise: x is still there before the drop ({order})");
            window(&m, std::slice::from_ref(&drop_of(&w._author.0, t2.wall_ms)));
            assert_eq!(logical_drops_in_effect(&m), 1, "premise: the drop took effect ({order})");
        }
        assert!(!index_on(&m, FIRST), "no x ({order})");
        assert!(
            entries_of_index(&m, FIRST, "kept") > 0,
            "the index above the floor is present, with entries ({order})"
        );
    }
}

#[test]
fn a_flip_over_a_drop_in_effect_floors_the_stages_copy_at_the_drop_and_not_at_the_lesser_floor() {
    // The identity's floor `T` (from the life it replaced) is below the drop `d` in effect: a copy stamped between them is the dropped life's
    let w = world();
    let (m, _d) = member(&w);
    let wall = w.bind_first.stamp.hlc.wall_ms;
    window(
        &m,
        &[
            doc_on(&w, FIRST, 201, wall - 8),
            doc_on(&w, FIRST, 202, wall - 8),
            named_index_entry(&w._author.0, FIRST, INDEX, "t", wall - 7),
        ],
    );
    let drop = drop_of(&w._author.0, wall - 5);
    assert!(
        w.t < drop.stamp.hlc && Hlc::new(wall - 7, 0) > w.t,
        "premise: the copy is between T and the drop"
    );
    assert!(
        entries_of_index(&m, FIRST, INDEX) > 0,
        "premise: the copy is on the stage with entries"
    );
    window(&m, std::slice::from_ref(&drop));
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(served(&m), FIRST, "premise: the flip over the drop in effect");
    assert_eq!(
        floor_of(&m, FIRST),
        Some(w.t),
        "premise: the stage took the floor T, below the drop"
    );
    assert!(
        !index_on(&m, FIRST),
        "the copy between the floor and the drop is the dropped life's: the flip floors at the drop"
    );
}

fn logical_drops_in_effect(m: &Engine) -> usize {
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

// -- The floor before the flip, through product routes (the shapes A and B) ----------------------------------------------------------------
//
// The two-member test above sets the identity's floor by hand on a member that already holds the stages. These reach the same floor the way a
// member does: built WITHOUT the stages, so the drop at `T` is an ordinary drop (no stage is held, so `drop_is_a_logical_drop` says no), and
// then the stage, its copy and the binding arrive. The copy is written by hand: no product route gives a stage a copy before Q6.7. They run
// against today's seed (register.rs:698); the F3 fix will change how the drop is judged while the name's only events are parked, and so
// the setup of these tests, which is a known future change and not a defect of theirs.

#[derive(Clone, Copy, Debug)]
enum Shape {
    /// The drop at `T`, then the name made again (the incarnation floor is `T`).
    A,
    /// The drop at `T` meets a write stamped above it: the life is re-dated, kept, and the id holds the drop's tombstone.
    B,
}

fn stage_copy_and_flip(m: &Engine, w: &World) {
    m.apply_batch(&w.stages).expect("the stages apply");
    first_stage_with_a_copy(m, w, w.t.wall_ms);
    window(m, std::slice::from_ref(&w.bind_first));
    assert_eq!(served(m), FIRST, "premise: the flip");
}

fn the_end_state(m: &Engine, what: &str) {
    assert!(!index_on(m, FIRST), "no x ({what})");
    assert_eq!(entries_of_index(m, FIRST, INDEX), 0, "and no entries of it ({what})");
    assert!(
        entries_of_index(m, FIRST, "kept") > 0,
        "the index above the floor is present, with entries ({what})"
    );
}

/// What the register holds for the name: how many logical drops are in effect.
fn shapes_end_as_the_flip_first_member(w: &World, below: bool) {
    // the flip-first member: the name made here, the stages, the copy, the binding, and the drop at `T` after the flip (a logical drop,
    // since an event of the name is in effect)
    let (flip_first, _d0) = engine();
    flip_first.create_collection(DB, "orders").expect("the name is made");
    stage_copy_and_flip(&flip_first, w);
    assert!(
        index_on(&flip_first, FIRST),
        "premise: x is still on the stage before the drop arrives"
    );
    window(&flip_first, std::slice::from_ref(&w.life[1]));
    assert_eq!(
        logical_drops_in_effect(&flip_first),
        1,
        "premise: the drop took effect below the stage"
    );
    the_end_state(&flip_first, "the flip, then the drop");

    for shape in [Shape::A, Shape::B] {
        let (m, _d) = engine();
        match shape {
            Shape::A => {
                m.apply_batch(&w.life).expect("the first life, the drop and the second life apply");
                assert_eq!(
                    m.get_collection(DB, "orders").expect("held").incarnation_floor,
                    Some(w.t),
                    "premise: the ordinary drop left the identity's incarnation floor at T"
                );
            }
            Shape::B => {
                window(&m, std::slice::from_ref(&w.life[0]));
                let stray = doc_on(w, "orders", 300, w.t.wall_ms + 1);
                assert!(
                    stray.stamp.hlc > w.t,
                    "premise: the stray write is stamped above the drop"
                );
                window(&m, std::slice::from_ref(&stray));
                window(&m, std::slice::from_ref(&w.life[1]));
                let held =
                    m.get_collection(DB, "orders").expect("premise: the life is kept, re-dated");
                // The survivors path re-dates the life and the sweep it starts gives it the floor `T` (late_drop.rs:317, after the re-dating
                // at 744-775), together with the tombstone at `T`: B closes through Held's incarnation term exactly as A does. Under the old
                // flip rule it diverged all the same (no logical drop in effect, so nothing was floored). The tombstone-only term has its own
                // test (the identity with a tombstone and no floor, set by hand).
                assert_eq!(
                    held.incarnation_floor,
                    Some(w.t),
                    "premise: the re-dated life carries the floor T, set by the sweep ({shape:?})"
                );
                assert_eq!(
                    m.collection_dropped_at(orders_id()).expect("reads").map(|s| s.hlc),
                    Some(w.t),
                    "premise: the id holds the drop's tombstone at T"
                );
            }
        }
        m.apply_batch(&w.stages).expect("the stages apply");
        let before = logical_drops_in_effect(&m);
        first_stage_with_a_copy(&m, w, w.t.wall_ms);
        window(&m, std::slice::from_ref(&w.bind_first));
        assert_eq!(served(&m), FIRST, "premise: the flip ({shape:?})");
        let after = logical_drops_in_effect(&m);
        if !below {
            // the stages are made above the drop by a writer that had not received it: no seed fires, in either shape, so a seed that fires
            // wrongly (or a drop judged logical) is seen here
            assert_eq!(
                after, 0,
                "the register's logical drops after the flip ({shape:?}, stages above the drop; before the flip: {before})"
            );
        }
        if below {
            // The stages were created below the drop: the swap began before it. What the register holds records which mechanism closes
            // the shape. B: the seed writes the logical drop at `T` (the drop leaves the life it re-dated and the swap began below it); A: a
            // plain recreation, so there is no seed and the end state is the new flip floor's alone.
            let want = if matches!(shape, Shape::B) { 1 } else { 0 };
            assert_eq!(
                after, want,
                "the register's logical drops after the flip ({shape:?}, stages below the drop; before the flip: {before})"
            );
        }
        assert_eq!(
            floor_of(&m, FIRST),
            Some(w.t),
            "premise: the flip assigned the stage the floor T ({shape:?})"
        );
        the_end_state(
            &m,
            &format!(
                "{shape:?}: the drop, then the stages and the flip (stages below the drop: {below})"
            ),
        );
    }
}

#[test]
fn a_member_that_met_the_ordinary_drop_before_the_stages_ends_as_one_that_met_the_flip_first() {
    // the stages are created ABOVE the drop, by a writer that had not received it: its copy still holds x
    shapes_end_as_the_flip_first_member(&world(), false);
}

#[test]
fn the_same_with_the_stages_created_below_the_drop_records_which_mechanism_closes_each_shape() {
    // B: the swap began before the drop, the life is kept, so the seed writes the logical drop (register.rs:723); A: a plain recreation, no
    // seed, and x is absent only because the flip floors the new physical itself
    shapes_end_as_the_flip_first_member(&world_below(), true);
}
