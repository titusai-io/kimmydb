//! Burial by a later event, at the storage call (ADR-272, decision 5; Q6.4 item 6). `Timeline::doomed` is the pure rule: a stage
//! with NO binding in effect on this member, whose effective deadline lies STRICTLY below the visible event's stamp, can never be
//! the visible physical (every valid binding of it is stamped at or below the deadline, so in every order of arrival it is older
//! than that event), and is buried with it. Here the rule is called in the one pass that decides visibility (`settle_register_in_txn`)
//! and after a stage's creation that arrives behind the event; a valid binding of a doomed stage that arrives later still takes
//! effect as an event and is never visible. (A staging write addressed to a stage this member holds no collection of, appended and
//! served and storing nothing, is item 6b, below: the trigger is the stage's creation row in the register and no collection alive.)

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
    a.create_stage(DB, S1, MergePolicy::Document, Hlc::new(now + 5_000, 0)).unwrap();
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

// -- Item 6b: a staging write addressed to a stage this member holds no collection of (decision 4, rule S widened; D-3) --------------------------------

/// The author's staged row into S1, written before S1's deadline (a stamp below any binding S1 could have).
fn staged_row(w: &World) -> OplogEntry {
    let s1_meta = w.a.get_collection(DB, S1).unwrap();
    w.a.insert(&s1_meta, doc! { "_id": 9, "v": "staged" }).unwrap();
    everything(&w.a)
        .into_iter()
        .find(|e| e.kind == OpKind::Insert && e.collection == CollectionId::derive(DB, S1))
        .expect("the staged row's entry")
}

/// A member holding `orders` that has buried S1 as doomed: it took both creations and S2's binding in effect.
fn member_that_doomed_s1(w: &World) -> (Engine, tempfile::TempDir) {
    let (m, d) = with_orders();
    m.apply_batch(&[w.s1.clone(), w.s2.clone()]).unwrap();
    window(&m, std::slice::from_ref(&w.bind_s2));
    assert!(m.get_collection(DB, S1).is_err(), "premise: S1 is doomed");
    (m, d)
}

fn held_entries_at(e: &Engine, stamp: &Stamp) -> usize {
    everything(e).iter().filter(|x| x.stamp == *stamp).count()
}

#[test]
fn a_staging_write_to_a_doomed_stage_is_served_onward_and_stores_nothing() {
    let w = world();
    let row = staged_row(&w);
    let (m, _d) = member_that_doomed_s1(&w);
    window(&m, std::slice::from_ref(&row)); // the staging write arrives after the burial
    assert_eq!(
        m.rows_under(CollectionId::derive(DB, S1)).unwrap(),
        0,
        "nothing is stored for a doomed stage"
    );
    assert_eq!(
        held_entries_at(&m, &row.stamp),
        1,
        "and the write is appended and served onward, so a member behind this one still learns of it"
    );
}

#[test]
fn the_same_staging_write_arriving_again_leaves_one_entry_in_the_oplog() {
    let w = world();
    let row = staged_row(&w);
    let (m, _d) = member_that_doomed_s1(&w);
    window(&m, std::slice::from_ref(&row));
    window(&m, std::slice::from_ref(&row));
    assert_eq!(
        held_entries_at(&m, &row.stamp),
        1,
        "a window sent again appends nothing a second time: the entry is one in the oplog"
    );
}

#[test]
fn a_write_of_another_origin_to_a_doomed_stage_is_history_and_is_not_relayed() {
    let w = world();
    let (m, _d) = member_that_doomed_s1(&w);
    // another member wrote into a collection of S1's id (it made its own stage of that name): not the stage's author
    let (c, _dc) = engine();
    c.create_stage(DB, S1, MergePolicy::Document, Hlc::new(physical_now_ms() + 500, 0)).unwrap();
    let c_meta = c.get_collection(DB, S1).unwrap();
    c.insert(&c_meta, doc! { "_id": 9, "v": "not the author's" }).unwrap();
    let theirs = everything(&c)
        .into_iter()
        .find(|e| e.kind == OpKind::Insert && e.collection == CollectionId::derive(DB, S1))
        .unwrap();
    window(&m, std::slice::from_ref(&theirs));
    assert_eq!(
        held_entries_at(&m, &theirs.stamp),
        0,
        "only the stage's author's rows are staging rows (the trigger is the creation's origin, as for rule S)"
    );
}

#[test]
fn a_doomed_stage_is_remembered_across_a_restart() {
    let w = world();
    let row = staged_row(&w);
    let (m, d) = member_that_doomed_s1(&w);
    assert!(m.close().is_ok(), "the engine closed");
    let reopened = Engine::open(&d.path().join("kimmy.redb")).unwrap();
    window(&reopened, std::slice::from_ref(&row));
    assert_eq!(
        held_entries_at(&reopened, &row.stamp),
        1,
        "a member that restarted still knows S1 was buried as doomed, and serves the staging write onward"
    );
}

/// `row`, as another write of the same author's, stamped at `wall_ms` (its own `_id`).
fn row_at(w: &World, row: &OplogEntry, wall_ms: u64, id: i32) -> OplogEntry {
    let mut other = row.clone();
    other.stamp = Stamp::new(Hlc::new(wall_ms, 0), w.a.node_id(), w.a.epoch());
    other.doc_id = Some(kimmy_core::DocId::Int64(i64::from(id)));
    other
}

#[test]
fn the_relay_honours_the_binding_a_write_below_it_is_a_staging_row_and_one_above_it_is_history() {
    let w = world();
    let row = staged_row(&w);
    let binding_wall = row.stamp.hlc.wall_ms + 5;
    let late_binding = bind_of(&w.a, S1, binding_wall); // valid: at or below S1's deadline
    let above = row_at(&w, &row, binding_wall + 5, 10);
    let (m, _d) = member_that_doomed_s1(&w);
    window(&m, std::slice::from_ref(&late_binding)); // a valid binding of a doomed stage takes effect as an event
    window(&m, std::slice::from_ref(&row));
    window(&m, std::slice::from_ref(&above));
    assert_eq!(
        held_entries_at(&m, &row.stamp),
        1,
        "a write below the stage's binding is a staging row: relayed"
    );
    assert_eq!(
        held_entries_at(&m, &above.stamp),
        0,
        "a write above the stage's binding is a late write of that binding, not a staging row: history here, as before"
    );
}

#[test]
fn a_staging_write_with_a_far_future_stamp_is_refused_before_it_is_relayed() {
    let w = world();
    let row = staged_row(&w);
    let far = row_at(&w, &row, physical_now_ms() + 10 * 3_600_000, 11);
    let (m, _d) = member_that_doomed_s1(&w);
    let outcome = window(&m, std::slice::from_ref(&far));
    assert_eq!(
        outcome.clock_refused, 1,
        "the stamp is past the bound U1 holds every replicated entry to: {outcome:?}"
    );
    assert_eq!(
        held_entries_at(&m, &far.stamp),
        0,
        "a relayed write is bounded like every other: nothing is appended or witnessed on a stamp past the bound"
    );
}

// -- Item 6b, the trigger is the creation row and no collection alive: no cause is read --------------------------------------------

#[test]
fn a_stage_buried_by_the_loser_rule_serves_a_staging_write_onward_as_well() {
    let w = world();
    let row = staged_row(&w);
    let binding_wall = row.stamp.hlc.wall_ms + 5;
    let bind_s1 = bind_of(&w.a, S1, binding_wall); // S1's own valid binding, above the staged row
    let (m, _d) = with_orders();
    m.apply_batch(&[w.s1.clone(), w.s2.clone()]).unwrap();
    window(&m, std::slice::from_ref(&bind_s1));
    window(&m, std::slice::from_ref(&w.bind_s2)); // S2's binding is above S1's: S1 is buried by the loser rule, not by a later event alone
    assert!(m.get_collection(DB, S1).is_err(), "premise: S1 is buried");
    window(&m, std::slice::from_ref(&row));
    assert_eq!(
        held_entries_at(&m, &row.stamp),
        1,
        "a staging write below its stage's binding is served onward wherever the stage is not alive here, whatever buried it"
    );
}

#[test]
fn a_staging_write_held_as_state_is_released_when_it_arrives_in_position_after_the_burial() {
    // The stage's deadline is its creation's own HLC, and the event that buries it is ANOTHER origin's drop of the name: an event of the stage's
    // own origin would raise the version vector of that origin above the held row, and the end-of-window sweep of superseded marks would drop
    // the mark before the entry arrives (review of items 6 and 6b, F2)
    let (a, _da) = engine();
    a.create_stage(DB, S1, MergePolicy::Document, Hlc::new(physical_now_ms(), 0)).unwrap();
    let creation = creation_of(&a, S1);
    let stage = a.get_collection(DB, S1).unwrap();
    a.insert(&stage, doc! { "_id": 9, "v": "staged" }).unwrap();
    let row = everything(&a)
        .into_iter()
        .find(|e| e.kind == OpKind::Insert && e.collection == CollectionId::derive(DB, S1))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let (z, _dz) = engine();
    z.create_collection(DB, "orders").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(z.drop_collection(DB, "orders").unwrap(), "premise: Z dropped the name");
    let (m, _dm) = engine();
    window(&m, std::slice::from_ref(&creation));
    // the row reached this member first as a snapshot document would: held state, in the oplog with a mark, S1 still alive
    m.apply_held_for_test(&m.get_collection(DB, S1).unwrap(), &row).unwrap();
    window(&m, &everything(&z)); // the drop of the name, stamped above S1's deadline: S1 is buried
    assert!(m.get_collection(DB, S1).is_err(), "premise: S1 is buried");
    let before = m.held_marks_released();
    window(&m, std::slice::from_ref(&row)); // and now it arrives in position
    assert_eq!(
        m.held_marks_released(),
        before + 1,
        "an entry held as state that arrives in a contiguous window has its mark released, wherever it is addressed"
    );
}

#[test]
fn a_staging_row_for_a_creation_a_database_drop_refused_is_served_onward_once() {
    // A starts a replacing `$out`, B drops the database, C hears the drop first (the race of the stage-drop tests): the creation is
    // refused as history here, its row is kept, and the author's staging row for it is still served onward, once
    let (a, _da) = engine();
    let (d, _dd) = engine();
    d.create_collection(DB, "orders").unwrap();
    a.create_stage(DB, S1, MergePolicy::Document, Hlc::new(physical_now_ms() + 60_000, 0)).unwrap();
    let creation = creation_of(&a, S1);
    let stage = a.get_collection(DB, S1).unwrap();
    a.insert(&stage, doc! { "_id": 9, "v": "staged" }).unwrap();
    let row = everything(&a)
        .into_iter()
        .find(|e| e.kind == OpKind::Insert && e.collection == CollectionId::derive(DB, S1))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(d.drop_database(DB).unwrap(), "premise: the database is dropped");
    let (m, _dm) = engine();
    window(&m, &everything(&d));
    window(&m, &[creation, row.clone()]);
    assert!(m.get_collection(DB, S1).is_err(), "premise: the creation was refused as history");
    assert_eq!(
        held_entries_at(&m, &row.stamp),
        1,
        "the author's staging row for a creation the database's drop refused is appended and served onward, once"
    );
}

// -- F1 of the review of items 6 and 6b: the creation of a stage that arrives after the drop of its name ---------------------------------

#[test]
fn a_stage_is_buried_on_both_members_whichever_of_its_creation_and_the_drop_of_its_name_arrives_first()
 {
    // W creates the stage S1 of `orders` (the deadline is its creation's own HLC) and vanishes. Z drops `orders` after the deadline.
    // X meets the creation and then the drop; Y meets the drop first (an ordinary drop: the identity's tombstone) and then the creation.
    let (w, _dw) = engine();
    w.create_stage(DB, S1, MergePolicy::Document, Hlc::new(physical_now_ms(), 0)).unwrap();
    let creation = creation_of(&w, S1);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let (z, _dz) = engine();
    z.create_collection(DB, "orders").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(z.drop_collection(DB, "orders").unwrap(), "premise: Z dropped the name");
    let from_z = everything(&z);
    let (x, _dx) = engine();
    window(&x, std::slice::from_ref(&creation));
    window(&x, &from_z);
    let (y, _dy) = engine();
    window(&y, &from_z);
    assert!(
        y.collection_dropped_at(CollectionId::derive(DB, "orders")).unwrap().is_some(),
        "premise: Y holds the drop as the identity's tombstone"
    );
    window(&y, std::slice::from_ref(&creation));
    assert!(
        x.get_collection(DB, S1).is_err(),
        "X, which met the creation first, buried the stage when the drop arrived"
    );
    assert!(
        y.get_collection(DB, S1).is_err(),
        "Y, which met the drop first, buries the stage as soon as its creation arrives: nothing later repairs a stage whose writer is gone"
    );
}
