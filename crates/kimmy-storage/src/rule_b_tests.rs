//! Rule B for a binding (ADR-272, decision 3): a register event takes effect only in position. In a window it takes effect in
//! the transaction that appends its entry; with no window behind it it is information (known, not in effect, its entry held out
//! of position) until it arrives in one, when it takes effect in the transaction that releases the hold; a binding of a stage
//! whose creation is not held stops the window and waits.

use kimmy_core::{
    CollectionBind, CollectionId, Epoch, Hlc, MergePolicy, OpKind, OplogEntry, RegisterEvent, Stamp,
};

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

/// The stage's creation (made by A, with a deadline a minute away) and a binding of it that A could have made, the first
/// stamped at the deadline's side of safe: one millisecond after the creation.
struct Case {
    a: Engine,
    creation: OplogEntry,
    bind: OplogEntry,
    deadline: Hlc,
    _da: tempfile::TempDir,
}

fn case() -> Case {
    let (a, da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    let creation = everything(&a)
        .into_iter()
        .find(|x| {
            x.kind == OpKind::CreateCollection && x.collection == CollectionId::derive(DB, STAGE)
        })
        .unwrap();
    let bind = bind_at(&a, Hlc::new(creation.stamp.hlc.wall_ms + 1, 0));
    Case { a, creation, bind, deadline, _da: da }
}

fn bind_at(a: &Engine, hlc: Hlc) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(hlc, a.node_id(), a.epoch()),
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

/// `entries` as one window served contiguously from the receiver's own position.
fn window(b: &Engine, from: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    let mut theirs = from.version_vector().unwrap();
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

fn in_effect(b: &Engine) -> usize {
    let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
    let n = crate::register::timeline(&txn, DB, "orders").unwrap().events().len();
    txn.abort().unwrap();
    n
}

fn parked(b: &Engine) -> usize {
    let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
    let n = crate::register::parked(&txn, DB, "orders").unwrap().events().len();
    txn.abort().unwrap();
    n
}

fn holds(b: &Engine, entry: &OplogEntry) -> bool {
    everything(b).iter().any(|x| x.stamp == entry.stamp && x.kind == entry.kind)
}

#[test]
fn a_binding_in_a_window_takes_effect_in_the_transaction_that_appends_it() {
    let c = case();
    let (b, _db) = engine();
    let outcome = window(&b, &c.a, &[c.creation.clone(), c.bind.clone()]);
    assert_eq!(outcome.unknown_collection, 0, "{outcome:?}");
    assert_eq!(in_effect(&b), 1, "the binding is in effect");
    assert_eq!(parked(&b), 0);
    assert!(holds(&b, &c.bind), "and its entry is appended, served onward");
    assert_eq!(b.held_marks().unwrap(), 0, "in position: no hold");
    assert_eq!(b.witnessed_vector().unwrap().get(c.bind.stamp.origin()), c.bind.stamp.hlc);
}

#[test]
fn a_binding_with_no_window_behind_it_is_information_and_its_entry_is_held_out_of_position() {
    let c = case();
    let (b, _db) = engine();
    b.apply_batch(std::slice::from_ref(&c.creation)).unwrap();
    b.apply_batch(std::slice::from_ref(&c.bind)).unwrap();
    assert_eq!(in_effect(&b), 0, "not in effect: it has not arrived in position");
    assert_eq!(parked(&b), 1, "but known");
    assert!(holds(&b, &c.bind), "the entry is appended");
    assert_eq!(b.held_marks().unwrap(), 1, "and held out of position: it grants no coverage");
    assert!(
        b.witnessed_vector().unwrap().get(c.bind.stamp.origin()) < c.bind.stamp.hlc,
        "the witnessed vector did not move over it"
    );
}

#[test]
fn a_parked_binding_takes_effect_in_the_transaction_that_releases_its_hold() {
    let c = case();
    let (b, _db) = engine();
    b.apply_batch(std::slice::from_ref(&c.creation)).unwrap();
    b.apply_batch(std::slice::from_ref(&c.bind)).unwrap();
    assert_eq!((in_effect(&b), parked(&b), b.held_marks().unwrap()), (0, 1, 1));
    // the same entry now arrives in a window
    window(&b, &c.a, std::slice::from_ref(&c.bind));
    assert_eq!(in_effect(&b), 1, "released: in effect");
    assert_eq!(parked(&b), 0, "no longer information");
    assert_eq!(b.held_marks().unwrap(), 0, "the hold is gone");
    assert_eq!(b.witnessed_vector().unwrap().get(c.bind.stamp.origin()), c.bind.stamp.hlc);
}

#[test]
fn a_binding_above_its_stages_deadline_is_history_appended_and_served_and_never_in_effect() {
    let c = case();
    let (b, _db) = engine();
    let late = bind_at(&c.a, Hlc::new(c.deadline.wall_ms + 1, 0));
    window(&b, &c.a, &[c.creation.clone(), late.clone()]);
    assert_eq!(in_effect(&b), 0, "an invalid binding never takes effect");
    assert_eq!(parked(&b), 0);
    assert!(holds(&b, &late), "it is appended and served onward all the same");
    assert!(b.witnessed_vector().unwrap().get(late.stamp.origin()) >= late.stamp.hlc);
}

#[test]
fn a_binding_of_a_stage_whose_creation_is_not_held_stops_the_window_and_waits() {
    let c = case();
    let (b, _db) = engine();
    let outcome = window(&b, &c.a, std::slice::from_ref(&c.bind));
    assert_eq!(outcome.unknown_collection, 1, "{outcome:?}");
    assert_eq!(
        outcome.unknown.as_ref().and_then(|u| u.name.as_deref()),
        Some(format!("{DB}.{STAGE}").as_str()),
        "it names the stage it waits for"
    );
    assert!(!holds(&b, &c.bind), "nothing was appended");
    assert!(
        b.witnessed_vector().unwrap().get(c.bind.stamp.origin()) < c.bind.stamp.hlc,
        "nor witnessed: the vector stops short of it"
    );
    assert_eq!(in_effect(&b) + parked(&b), 0);
    let waiting = b.register_waiting_for_stage();
    assert_eq!(waiting.iter().map(|(_, n)| n).sum::<u64>(), 1, "{waiting:?}: counted, by origin");
    // the creation arrives: the next window carries it first, and the binding is taken in the same round
    let outcome = window(&b, &c.a, &[c.creation.clone(), c.bind.clone()]);
    assert_eq!(outcome.unknown_collection, 0, "{outcome:?}");
    assert_eq!(in_effect(&b), 1, "the wait is over");
    assert!(holds(&b, &c.bind));
}

#[test]
fn a_stages_own_creation_is_never_parked() {
    // the creation relies on invariant (ii) alone (decision 3): taken with no window behind it, it makes the collection
    let c = case();
    let (b, _db) = engine();
    b.apply_batch(std::slice::from_ref(&c.creation)).unwrap();
    assert!(b.get_collection(DB, STAGE).is_ok());
    assert_eq!(b.held_marks().unwrap(), 0);
    let _ = Epoch::ZERO;
    let _ = RegisterEvent::LogicalDrop;
}
