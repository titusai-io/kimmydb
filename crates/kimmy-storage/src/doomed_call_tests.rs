//! Burial by a later event, at the storage call (ADR-272, decision 5; Q6.4 item 6). `Timeline::doomed` is the pure rule: a stage
//! with NO binding in effect on this member, whose effective deadline lies STRICTLY below the visible event's stamp, can never be
//! the visible physical (every valid binding of it is stamped at or below the deadline, so in every order of arrival it is older
//! than that event), and is buried with it. Here the rule is called in the one pass that decides visibility (`settle_register_in_txn`)
//! and after a stage's creation that arrives behind the event; a valid binding of a doomed stage that arrives later still takes
//! effect as an event and is never visible. (A staging write addressed to a doomed stage, appended and served and storing nothing,
//! is the next commit: it needs the record of which stages were buried as doomed.)

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
const S1: &str = "orders.__stg.s1";
const S2: &str = "orders.__stg.s2";

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

fn creation_of(a: &Engine, physical: &str) -> OplogEntry {
    everything(a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, physical)
        })
        .unwrap()
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

/// The author of two stages: S1 with a short deadline, S2 with a long one, and the binding of S2 stamped above S1's deadline.
struct World {
    a: Engine,
    s1: OplogEntry,
    s2: OplogEntry,
    /// S2's binding: valid (at or below S2's deadline), and strictly above S1's.
    bind_s2: OplogEntry,
    /// S1's own binding: stamped just above S1's creation, so at or below its deadline: a valid binding.
    bind_s1: OplogEntry,
    _dir: tempfile::TempDir,
}

fn world() -> World {
    let (a, dir) = engine();
    let now = physical_now_ms();
    a.create_stage(DB, S1, MergePolicy::Document, Hlc::new(now + 500, 0)).unwrap();
    a.create_stage(DB, S2, MergePolicy::Document, Hlc::new(now + 120_000, 0)).unwrap();
    let (s1, s2) = (creation_of(&a, S1), creation_of(&a, S2));
    let bind_s2 = bind_of(&a, S2, now + 20_000);
    let bind_s1 = bind_of(&a, S1, s1.stamp.hlc.wall_ms + 1);
    World { a, s1, s2, bind_s2, bind_s1, _dir: dir }
}

/// A member holding `orders` with a document.
fn with_orders() -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").unwrap();
    m.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    (m, d)
}

fn buried_at(e: &Engine, physical: &str) -> Option<Stamp> {
    e.collection_dropped_at(CollectionId::derive(DB, physical)).unwrap()
}

#[test]
fn a_stage_with_no_binding_whose_deadline_is_below_the_visible_event_is_buried_with_it() {
    let w = world();
    let (m, _d) = with_orders();
    m.apply_batch(&[w.s1.clone(), w.s2.clone()]).unwrap();
    assert!(m.get_collection(DB, S1).is_ok(), "premise: S1 is held");
    window(&m, std::slice::from_ref(&w.bind_s2)); // S2's binding takes effect: strictly above S1's deadline
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, S2, "premise: S2 is visible");
    assert!(
        m.get_collection(DB, S1).is_err(),
        "S1 can never be visible: it is buried with the binding that made S2 so"
    );
    assert_eq!(buried_at(&m, S1), Some(w.bind_s2.stamp), "stamped by the visible event");
}

#[test]
fn a_stage_whose_deadline_is_not_below_the_visible_event_is_left() {
    // S1's deadline above the binding: a binding of S1 stamped between could still arrive and be valid
    let (a, _da) = engine();
    let now = physical_now_ms();
    a.create_stage(DB, S1, MergePolicy::Document, Hlc::new(now + 60_000, 0)).unwrap();
    a.create_stage(DB, S2, MergePolicy::Document, Hlc::new(now + 120_000, 0)).unwrap();
    let (s1, s2) = (creation_of(&a, S1), creation_of(&a, S2));
    let bind_s2 = bind_of(&a, S2, now + 20_000);
    let (m, _d) = with_orders();
    m.apply_batch(&[s1, s2]).unwrap();
    window(&m, std::slice::from_ref(&bind_s2));
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, S2, "premise");
    assert!(m.get_collection(DB, S1).is_ok(), "S1's deadline is above the event: it is not doomed");
}

#[test]
fn an_event_known_ahead_of_its_position_dooms_nothing() {
    let w = world();
    let (m, _d) = with_orders();
    m.apply_batch(&[w.s1.clone(), w.s2.clone()]).unwrap();
    m.apply_batch(std::slice::from_ref(&w.bind_s2)).unwrap(); // information only: no window behind it
    assert!(
        m.get_collection(DB, S1).is_ok(),
        "an event that is not in effect is not a visible event"
    );
}

#[test]
fn a_valid_binding_of_a_doomed_stage_that_arrives_later_takes_effect_and_is_never_visible() {
    let w = world();
    let (m, _d) = with_orders();
    m.apply_batch(&[w.s1.clone(), w.s2.clone()]).unwrap();
    window(&m, std::slice::from_ref(&w.bind_s2));
    assert!(m.get_collection(DB, S1).is_err(), "premise: S1 is doomed");
    window(&m, std::slice::from_ref(&w.bind_s1)); // S1's own binding, at or below its deadline, arrives late
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, S2, "the newer event stays visible");
    assert!(m.get_collection(DB, S1).is_err(), "and the doomed stage is not brought back");
    let events = {
        let txn = m.begin_write(crate::WriterHolder::Ddl).unwrap();
        let n = crate::register::timeline(&txn, DB, "orders").unwrap().events().len();
        txn.abort().unwrap();
        n
    };
    assert_eq!(events, 2, "the late binding took effect as an event (it makes writes to S1 late)");
}

#[test]
fn a_stage_created_behind_an_event_that_is_already_above_its_deadline_is_doomed_at_once() {
    // The member learns S2's binding (and S2) first; S1's creation arrives after, in a later window.
    let w = world();
    let (m, _d) = with_orders();
    m.apply_batch(std::slice::from_ref(&w.s2)).unwrap();
    window(&m, std::slice::from_ref(&w.bind_s2));
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, S2, "premise");
    window(&m, std::slice::from_ref(&w.s1)); // the creation of a stage that is already doomed
    assert!(
        m.get_collection(DB, S1).is_err(),
        "a stage this member knows is alive or buried by its own test: never left standing"
    );
    assert_eq!(buried_at(&m, S1), Some(w.bind_s2.stamp));
}
