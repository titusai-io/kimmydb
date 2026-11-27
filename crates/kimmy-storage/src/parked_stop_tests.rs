//! The parked stop (ADR-148, amended; ADR-272, decision 3): on an apply with no window, nothing of an origin is taken past an
//! event of that origin the member holds parked.
//!
//! **The worlds.** A, the author, makes everything by its own operations, but the binding, which no product code writes yet and is
//! built by hand. M takes what comes before the event in a window, as a member that holds it all in position; then it is given the
//! event and what follows with NO window, as a member's replay of its own lost entries gives them. For the two vectors of A's
//! origin this is the state of a creator restored from a backup that lacks the event. The replay itself, and the transport, are
//! kimmy-cluster's tests.

use std::collections::BTreeMap;

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, RegisterEvent, Stamp,
    ValidatorSpec, VersionVector, database_drop_id,
};

use redb::ReadableDatabase;

use crate::register::{Carried, carried_rows};
use crate::{
    Engine, MarkedRange, PeerWindow, SnapshotProgress, StorageError, SyncOutcome, WalkScope,
    physical_now_ms,
};

const DB: &str = "app";
const STAGE: &str = "orders.__stg.a1";
const STAGE2: &str = "orders.__stg.a2";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// `entries` to `to` as one window that carries them, from a sender whose vector is theirs.
fn window(to: &Engine, entries: &[OplogEntry]) {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = to.witnessed_vector().unwrap();
    let mut outcome = SyncOutcome::default();
    to.apply_peer_window_into(
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
    assert!(!outcome.stopped(), "premise: the window is taken whole: {outcome:?}");
    to.finish_purges_now().unwrap();
}

/// The one entry `act` appended to `a`'s oplog of the kind `kind` for the collection `of`.
fn made(a: &Engine, kind: OpKind, of: CollectionId, act: impl FnOnce(&Engine)) -> OplogEntry {
    let before: Vec<Stamp> = everything(a).iter().map(|e| e.stamp).collect();
    act(a);
    let mut new: Vec<OplogEntry> = everything(a)
        .into_iter()
        .filter(|e| !before.contains(&e.stamp) && e.kind == kind && e.collection == of)
        .collect();
    assert_eq!(new.len(), 1, "premise: one {kind:?}: {new:?}");
    new.remove(0)
}

fn of(name: &str) -> CollectionId {
    CollectionId::derive(DB, name)
}

fn insert(a: &Engine, name: &str, n: i64) -> OplogEntry {
    let coll = a.get_collection(DB, name).unwrap();
    made(a, OpKind::Insert, of(name), |a| {
        a.insert(&coll, doc! { "_id": n }).unwrap();
    })
}

// -- The three parking worlds ------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub(crate) enum Parks {
    Binding,
    Drop,
    Recreation,
    /// The recreation, made with a validator. Not one of [`WORLDS`]: one row's.
    ValidatedRecreation,
}

const WORLDS: [Parks; 3] = [Parks::Binding, Parks::Drop, Parks::Recreation];

/// A holds `orders` and `items`, a stage bound over `orders`, and, as far as the world goes, the drop of the name and its
/// recreation.
struct Author {
    a: Engine,
    bind: OplogEntry,
    /// A write into `items` made before the stage, which M is not given with the rest: an older entry of the origin.
    older: OplogEntry,
    _dir: tempfile::TempDir,
}

fn author() -> Author {
    let (a, dir) = engine();
    a.create_collection(DB, "orders").unwrap();
    a.create_collection(DB, "items").unwrap();
    // two plain collections for the rows that need one: made here, below every event, so that M takes them in position
    a.create_collection(DB, "plain").unwrap();
    a.create_collection(DB, "temp").unwrap();
    let older = insert(&a, "items", 100);
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    let creation = made(&a, OpKind::CreateCollection, of(STAGE), |a| {
        a.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    });
    let bind = OplogEntry {
        stamp: Stamp::new(Hlc::new(creation.stamp.hlc.wall_ms + 1, 0), a.node_id(), a.epoch()),
        kind: OpKind::BindCollection,
        collection: of("orders"),
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
    };
    Author { a, bind, older, _dir: dir }
}

impl Author {
    /// A takes its own binding, as a window that carries it alone, and its clock passes it.
    fn takes_its_binding(&self) {
        window(&self.a, std::slice::from_ref(&self.bind));
        self.a.advance_clock_past(&self.bind.stamp);
    }

    /// What A holds stamped below `event`, but the older write kept back: what M takes in position.
    fn before(&self, event: &OplogEntry) -> Vec<OplogEntry> {
        everything(&self.a)
            .into_iter()
            .filter(|e| e.stamp < event.stamp && e.stamp != self.older.stamp)
            .collect()
    }
}

/// One world, ready to park: the event, what M holds below it, and the physical of the recreation where there is one.
pub(crate) struct World {
    author: Author,
    pub(crate) event: OplogEntry,
    drop: Option<OplogEntry>,
    recreation: Option<String>,
    prefix: Vec<OplogEntry>,
    /// A's later write, made after the event: but in a world of [`world_writing`], of a collection the event does not concern.
    pub(crate) write: OplogEntry,
    pub(crate) m: Engine,
    _dm: tempfile::TempDir,
}

pub(crate) fn world(parks: Parks) -> World {
    world_writing(parks, |a| insert(&a.a, "items", 1))
}

/// [`world`], whose later writes are what `later` makes A do: it answers the first of them.
fn world_writing(parks: Parks, later: impl FnOnce(&Author) -> OplogEntry) -> World {
    let author = author();
    author.takes_its_binding();
    let drop_it = |a: &Engine| {
        made(a, OpKind::DropCollection, of("orders"), |a| {
            assert!(a.drop_collection(DB, "orders").unwrap());
        })
    };
    let (event, drop, recreation) = match parks {
        Parks::Binding => (author.bind.clone(), None, None),
        Parks::Drop => {
            let dropped = drop_it(&author.a);
            (dropped.clone(), Some(dropped), None)
        }
        Parks::Recreation | Parks::ValidatedRecreation => {
            let dropped = drop_it(&author.a);
            let before: Vec<Stamp> = everything(&author.a).iter().map(|e| e.stamp).collect();
            if matches!(parks, Parks::ValidatedRecreation) {
                let spec = ValidatorSpec {
                    schema: doc! { "bsonType": "object" },
                    action: Default::default(),
                    level: Default::default(),
                };
                author.a.create_collection_with_validator(DB, "orders", spec).unwrap();
            } else {
                author.a.create_collection(DB, "orders").unwrap();
            }
            let recreated = everything(&author.a)
                .into_iter()
                .find(|e| !before.contains(&e.stamp) && e.kind == OpKind::CreateCollection)
                .expect("premise: the creation is an entry");
            let physical = author.a.resolve(DB, "orders").unwrap().physical;
            assert!(kimmy_core::is_recreation_name(&physical), "premise: {physical}");
            (recreated, Some(dropped), Some(physical))
        }
    };
    let prefix = author.before(&event);
    let write = later(&author);
    assert!(write.stamp > event.stamp, "premise: the write is above the event");
    let (m, dm) = engine();
    window(&m, &prefix);
    World { author, event, drop, recreation, prefix, write, m, _dm: dm }
}

impl World {
    /// The stamp of the last entry of A's origin M holds below the event: the literal both vectors must keep.
    fn below(&self) -> Hlc {
        self.prefix.last().expect("a prefix").stamp.hlc
    }
}

/// The two vectors of `origin_of`'s origin on `e`: served, then witnessed.
fn vectors(e: &Engine, origin_of: &OplogEntry) -> (Hlc, Hlc) {
    (
        e.version_vector().unwrap().get(origin_of.stamp.origin()),
        e.witnessed_vector().unwrap().get(origin_of.stamp.origin()),
    )
}

fn register_of(e: &Engine) -> (Vec<(Stamp, RegisterEvent)>, Vec<Stamp>) {
    let txn = e.db().begin_read().unwrap();
    let in_effect = crate::register::timeline_read(&txn, DB, "orders").unwrap();
    let parked = crate::register::parked_read(&txn, DB, "orders").unwrap();
    (in_effect.events().to_vec(), parked.events().iter().map(|(s, _)| *s).collect())
}

/// **The invariant** (ADR-148, amended): no event held parked is at or below either vector for its origin. Answers how many
/// parked rows it looked at: a caller at a parked step requires more than none, or the check has checked nothing. It holds where
/// what was lost is of an epoch the member no longer writes under; and it cannot see a member that joined from a broken one, which
/// holds no parked row.
fn no_parked_row_at_or_below_a_vector(e: &Engine) -> usize {
    let (served, witnessed) = (e.version_vector().unwrap(), e.witnessed_vector().unwrap());
    let txn = e.db().begin_read().unwrap();
    let rows = carried_rows(&txn, Carried::Info).unwrap();
    for (key, _) in &rows {
        let at = crate::codec::decode_oplog_key(&key[8..]).unwrap();
        for (which, vector) in [("served", &served), ("witnessed", &witnessed)] {
            assert!(
                at.hlc > vector.get(at.origin()),
                "a parked row at or below the {which} vector: {at:?}, the vector {:?}",
                vector.get(at.origin())
            );
        }
    }
    rows.len()
}

/// What a member answers of `orders`, beside literal values: there is no register function in this file.
#[derive(Debug, PartialEq, Eq)]
struct Said {
    serves: Option<String>,
    identity_held: bool,
    register: Vec<(Stamp, RegisterEvent)>,
    superseded_identity: Option<Stamp>,
    superseded_stage: Option<Stamp>,
    held: Vec<String>,
}

fn said(e: &Engine) -> Said {
    let serves = match e.resolve(DB, "orders") {
        Ok(resolved) => Some(resolved.physical),
        Err(StorageError::Core(kimmy_core::Error::CollectionNotFound { .. })) => None,
        Err(e) => panic!("resolve: {e}"),
    };
    let held = e
        .all_collections()
        .unwrap()
        .into_iter()
        .filter(|c| c.db == DB && c.name.starts_with("orders."))
        .map(|c| c.name)
        .collect();
    let txn = e.db().begin_read().unwrap();
    let timeline = crate::register::timeline_read(&txn, DB, "orders").unwrap();
    Said {
        serves,
        identity_held: e.get_collection(DB, "orders").is_ok(),
        register: timeline.events().to_vec(),
        superseded_identity: timeline.superseded_at("orders"),
        superseded_stage: timeline.superseded_at(STAGE),
        held,
    }
}

/// The literal answer of a member that has the world's event in effect.
fn wanted(w: &World) -> Said {
    let bind = w.author.bind.stamp;
    let mut register = vec![(bind, RegisterEvent::Bind { physical: STAGE.into() })];
    if let Some(dropped) = &w.drop {
        register.push((dropped.stamp, RegisterEvent::LogicalDrop));
    }
    let (serves, held) = match (&w.recreation, &w.drop) {
        (Some(physical), _) => {
            register.push((w.event.stamp, RegisterEvent::Create { physical: physical.clone() }));
            (Some(physical.clone()), vec![physical.clone()])
        }
        (None, Some(_)) => (None, Vec::new()),
        (None, None) => (Some(STAGE.to_string()), vec![STAGE.to_string()]),
    };
    Said {
        serves,
        identity_held: false,
        register,
        superseded_identity: Some(bind),
        superseded_stage: w.drop.as_ref().map(|dropped| dropped.stamp),
        held,
    }
}

/// Pull `from`'s entries into `to` until a window is exhausted or `to` asks for nothing.
fn pull(to: &Engine, from: &Engine) {
    for _ in 0..32 {
        let theirs = from.version_vector().unwrap();
        let mine = to.witnessed_vector().unwrap();
        let marked: Vec<MarkedRange> = to
            .held_marks_covered_by(&mine)
            .unwrap()
            .into_iter()
            .filter_map(|(span, _)| {
                let through = span.through.min(theirs.get(span.origin));
                (through >= span.from).then_some(MarkedRange { through, ..span })
            })
            .collect();
        let start = mine.behind(&theirs).or_else(|| {
            (!marked.is_empty()).then(|| theirs.iter().map(|(_, h)| h).max().unwrap_or(Hlc::ZERO))
        });
        let Some(start) = start else { return };
        let next = from
            .entries_for_peer_marked(start, 1024, Some(&mine), &marked, WalkScope::Background)
            .unwrap();
        let mut outcome = SyncOutcome::default();
        to.apply_peer_window_into(
            &theirs,
            Some(&mine),
            &next.entries,
            PeerWindow {
                scanned_to: next.scanned_to,
                exhausted: next.exhausted,
                passed_through: None,
                asked_partial: false,
            },
            &mut outcome,
        )
        .unwrap();
        to.finish_purges_now().unwrap();
        assert!(!outcome.stopped(), "premise: the pull is not stopped: {outcome:?}");
        if next.exhausted {
            return;
        }
    }
    panic!("a pull that never ended");
}

/// `to` takes the whole database from `source` as a snapshot.
fn take_a_snapshot(to: &Engine, source: &Engine) {
    let mut progress = SnapshotProgress::whole_database();
    let mut pages = 0;
    while !progress.is_complete() {
        let page = source.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        let applied = to.apply_snapshot_page(source.node_id(), &mut progress, &page).unwrap();
        to.finish_purges_now().unwrap();
        pages += 1;
        assert!(pages < 64 && applied.clock_refused.is_none(), "a snapshot that does not end");
    }
}

/// A member that joins from `source` by a whole snapshot.
fn joiner(source: &Engine) -> (Engine, tempfile::TempDir) {
    let (j, dir) = engine();
    take_a_snapshot(&j, source);
    (j, dir)
}

/// A backup of `of`, restored and opened.
fn restored_copy(of: &Engine) -> (Engine, tempfile::TempDir) {
    let mut bytes = Vec::new();
    of.backup_to(&mut bytes, WalkScope::Background).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    crate::backup::restore(&path, &mut bytes.as_slice()).unwrap();
    (Engine::open(&path).unwrap(), dir)
}

// -- P1 to P3: the stop, the member that joins from it, and the release --------------------------

/// M is given the event and the later write with no window. The event is parked; the write is left, and both vectors stay at the
/// last entry below the event.
fn stops_at_the_write(parks: Parks) {
    stops_in(&world(parks), parks);
}

/// [`stops_at_the_write`], in a world the caller built.
fn stops_in(w: &World, parks: Parks) {
    let outcome = w.m.apply_batch(&[w.event.clone(), w.write.clone()]).unwrap();
    let (served, witnessed) = vectors(&w.m, &w.event);
    assert_eq!(served, w.below(), "{parks:?}: the served vector");
    assert_eq!(witnessed, w.below(), "{parks:?}: the witnessed vector");
    assert!(!w.m.has_oplog_entry(&w.write.stamp).unwrap(), "{parks:?}: the write is not taken");
    assert!(outcome.stopped(), "{parks:?}: {outcome:?}");
    assert_eq!(outcome.stopped_at(), Some(w.write.stamp), "{parks:?}: {outcome:?}");
    assert_eq!(outcome.parked_stop, 1, "{parks:?}: {outcome:?}");
    assert_eq!(outcome.parked_stop_at, Some(w.write.stamp), "{parks:?}: {outcome:?}");
    assert_eq!(register_of(&w.m).1, vec![w.event.stamp], "{parks:?}: one parked, the event");
    assert!(no_parked_row_at_or_below_a_vector(&w.m) > 0, "{parks:?}: nothing was checked");
}

#[test]
fn a_replay_stops_behind_a_parked_binding() {
    stops_at_the_write(Parks::Binding);
}

#[test]
fn a_replay_stops_behind_a_parked_drop() {
    stops_at_the_write(Parks::Drop);
}

#[test]
fn a_replay_stops_behind_a_parked_recreation() {
    stops_at_the_write(Parks::Recreation);
}

/// A member that joins from M takes no coverage past the event, gets it by its own pulls, and answers what A answers. M's backup
/// restores, parked as M is.
fn a_joiner_from_it_gets_the_event(parks: Parks) {
    let w = world(parks);
    w.m.apply_batch(&[w.event.clone(), w.write.clone()]).unwrap();
    let (j, _dj) = joiner(&w.m);
    let (served, witnessed) = vectors(&j, &w.event);
    assert!(
        served < w.event.stamp.hlc && witnessed < w.event.stamp.hlc,
        "{parks:?}: joined, its vectors are below the event: {served:?}, {witnessed:?}"
    );
    assert!(!j.has_oplog_entry(&w.event.stamp).unwrap(), "{parks:?}: and it lacks the event");
    pull(&j, &w.author.a);
    assert!(j.has_oplog_entry(&w.event.stamp).unwrap(), "{parks:?}: its pull brings the event");
    let want = wanted(&w);
    assert_eq!(said(&j), want, "{parks:?}: the joiner");
    assert_eq!(said(&w.author.a), want, "{parks:?}: the author");

    let (restored, _dr) = restored_copy(&w.m);
    assert_eq!(register_of(&restored).1, register_of(&w.m).1, "{parks:?}: restored, parked");
    assert_eq!(vectors(&restored, &w.event), vectors(&w.m, &w.event), "{parks:?}: its vectors");
}

#[test]
fn a_member_that_joins_behind_a_parked_binding_gets_it() {
    a_joiner_from_it_gets_the_event(Parks::Binding);
}

#[test]
fn a_member_that_joins_behind_a_parked_drop_gets_it() {
    a_joiner_from_it_gets_the_event(Parks::Drop);
}

#[test]
fn a_member_that_joins_behind_a_parked_recreation_gets_it() {
    a_joiner_from_it_gets_the_event(Parks::Recreation);
}

/// ONE ordinary window that asks no marked range brings M the event and the write it left, and releases the event with effect.
fn one_window_releases(parks: Parks) {
    let w = world(parks);
    w.m.apply_batch(&[w.event.clone(), w.write.clone()]).unwrap();
    the_window_releases(&w, parks);
}

/// The second half of [`one_window_releases`]: M, with the event parked and its vector below it, takes one ordinary window.
fn the_window_releases(w: &World, parks: Parks) {
    let theirs = w.author.a.version_vector().unwrap();
    let mine = w.m.witnessed_vector().unwrap();
    let start = mine.behind(&theirs).expect("M is behind, so an ordinary pull asks");
    assert!(
        w.m.held_marks_covered_by(&mine).unwrap().is_empty(),
        "{parks:?}: and no held mark is covered, so the pull asks no marked range"
    );
    let next = w
        .author
        .a
        .entries_for_peer_marked(start, 1024, Some(&mine), &[], WalkScope::Background)
        .unwrap();
    assert_eq!(
        next.entries.iter().map(|e| e.stamp).collect::<Vec<_>>(),
        vec![w.event.stamp, w.write.stamp],
        "{parks:?}: the window carries the event and the write"
    );
    window(&w.m, &next.entries);
    assert_eq!(register_of(&w.m).1, Vec::<Stamp>::new(), "{parks:?}: nothing parked");
    assert_eq!(vectors(&w.m, &w.event), (w.write.stamp.hlc, w.write.stamp.hlc), "{parks:?}");
    assert_eq!(said(&w.m), wanted(w), "{parks:?}");
}

/// The control: the stop is not of windows. With the event parked ALONE, one window carries it and what follows past it, whole.
#[test]
fn a_window_goes_past_a_parked_event_and_releases_it() {
    for parks in WORLDS {
        let w = world(parks);
        w.m.apply_batch(std::slice::from_ref(&w.event)).unwrap();
        assert_eq!(register_of(&w.m).1, vec![w.event.stamp], "{parks:?}: premise: parked");
        the_window_releases(&w, parks);
    }
}

#[test]
fn one_window_releases_a_parked_binding_and_brings_what_was_left() {
    one_window_releases(Parks::Binding);
}

#[test]
fn one_window_releases_a_parked_drop_and_brings_what_was_left() {
    one_window_releases(Parks::Drop);
}

#[test]
fn one_window_releases_a_parked_recreation_and_brings_what_was_left() {
    one_window_releases(Parks::Recreation);
}

// -- B: what the stop does not stop --------------------------------------------------------------

/// The binding world with the binding parked.
fn parked_binding() -> World {
    let w = world(Parks::Binding);
    w.m.apply_batch(std::slice::from_ref(&w.event)).unwrap();
    assert_eq!(register_of(&w.m).1, vec![w.event.stamp], "premise: parked");
    w
}

#[test]
fn another_origins_entry_above_a_parked_event_is_taken() {
    let w = parked_binding();
    let (x, _dx) = engine();
    x.advance_clock_past(&w.event.stamp);
    let others = made(&x, OpKind::CreateCollection, of("others"), |x| {
        x.create_collection(DB, "others").unwrap();
    });
    assert!(others.stamp > w.event.stamp, "premise: above the parked event");
    let outcome = w.m.apply_batch(std::slice::from_ref(&others)).unwrap();
    assert!(!outcome.stopped(), "{outcome:?}");
    assert!(w.m.get_collection(DB, "others").is_ok(), "the creation is applied");
    assert_eq!(vectors(&w.m, &others), (others.stamp.hlc, others.stamp.hlc));
}

#[test]
fn the_same_nodes_entry_under_another_epoch_is_taken() {
    let w = parked_binding();
    let (x, _dx) = engine();
    assert_ne!(x.epoch(), w.author.a.epoch(), "premise: another epoch");
    let mut other = w.write.clone();
    other.stamp = Stamp::new(w.write.stamp.hlc, w.author.a.node_id(), x.epoch());
    let outcome = w.m.apply_batch(std::slice::from_ref(&other)).unwrap();
    assert!(!outcome.stopped(), "{outcome:?}");
    assert!(w.m.has_oplog_entry(&other.stamp).unwrap(), "it is taken");
}

#[test]
fn an_older_entry_of_the_parked_events_origin_is_taken() {
    let w = parked_binding();
    let older = &w.author.older;
    assert!(older.stamp < w.event.stamp, "premise: older than the parked event");
    assert!(!w.m.has_oplog_entry(&older.stamp).unwrap(), "premise: M lacks it");
    let outcome = w.m.apply_batch(std::slice::from_ref(older)).unwrap();
    assert!(!outcome.stopped(), "{outcome:?}");
    assert!(w.m.has_oplog_entry(&older.stamp).unwrap(), "it is taken");
}

/// The parked entry itself, given again with no window, stops nothing and is NOT witnessed: witnessing it would raise the vector to
/// the very stamp held parked, and an unmarked window built from that vector would carry what follows past the event.
#[test]
fn a_parked_entry_given_again_with_no_window_is_not_witnessed_and_stops_nothing() {
    for parks in WORLDS {
        let w = world(parks);
        w.m.apply_batch(std::slice::from_ref(&w.event)).unwrap();
        let before = vectors(&w.m, &w.event);
        assert_eq!(before, (w.below(), w.below()), "{parks:?}: premise: parked, not covered");
        let outcome = w.m.apply_batch(std::slice::from_ref(&w.event)).unwrap();
        assert!(!outcome.stopped(), "{parks:?}: {outcome:?}");
        assert_eq!(vectors(&w.m, &w.event), before, "{parks:?}: neither vector moves");
        assert!(no_parked_row_at_or_below_a_vector(&w.m) > 0, "{parks:?}: nothing was checked");
    }
}

/// The pass-over is of the parked entry, however its apply answers. A recreation that carries a validator, given again, applies
/// that validator to the physical already held before the register's step is reached: refused there, the entry answers refused and
/// not held, and it is still not witnessed.
#[test]
fn a_parked_recreation_refused_before_the_registers_step_is_not_witnessed() {
    use std::sync::atomic::Ordering::SeqCst;
    let w = world(Parks::ValidatedRecreation);
    w.m.apply_batch(std::slice::from_ref(&w.event)).unwrap();
    assert_eq!(register_of(&w.m).1, vec![w.event.stamp], "premise: parked");
    let before = vectors(&w.m, &w.event);
    assert_eq!(before, (w.below(), w.below()), "premise: parked, not covered");
    w.m.refuse_the_next_adopted_validator.store(true, SeqCst);
    let outcome = w.m.apply_batch(std::slice::from_ref(&w.event)).unwrap();
    assert!(!w.m.refuse_the_next_adopted_validator.load(SeqCst), "premise: the seam fired");
    assert_eq!(outcome.ddl_refused, 1, "premise: answered refused: {outcome:?}");
    assert!(!outcome.stopped(), "{outcome:?}");
    assert_eq!(vectors(&w.m, &w.event), before, "neither vector moves");
    assert!(no_parked_row_at_or_below_a_vector(&w.m) > 0, "nothing was checked");
}

/// The control of the two reads that could outlive a park: the event parked ALONE, released by one window, and then a later entry
/// of its origin given with no window is taken.
#[test]
fn after_the_release_a_later_entry_given_with_no_window_is_taken() {
    let w = parked_binding();
    pull(&w.m, &w.author.a);
    assert_eq!(register_of(&w.m).1, Vec::<Stamp>::new(), "premise: released");
    let later = insert(&w.author.a, "items", 2);
    let outcome = w.m.apply_batch(std::slice::from_ref(&later)).unwrap();
    assert!(!outcome.stopped(), "{outcome:?}");
    assert!(w.m.has_oplog_entry(&later.stamp).unwrap(), "it is taken");
    assert_eq!(vectors(&w.m, &later), (later.stamp.hlc, later.stamp.hlc));
}

// -- RACE: the same park by another batch, between this one's look and its write ------------------

/// Nothing is parked when M's batch of the binding and the later write begins. Between the batch's finding the binding not held and
/// its taking the writer, another batch parks it. The batch must still leave the write: what it learned about parked events before
/// the race is stale, and the answer it gets for the binding is "held" and not "parked".
#[test]
fn a_binding_parked_by_another_batch_under_this_one_still_stops_what_follows() {
    use crate::sync::race_hooks::{self, Race};
    let w = world(Parks::Binding);
    let (below, bind, write) = (w.below(), w.event.clone(), w.write.clone());
    assert_eq!(register_of(&w.m).1, Vec::<Stamp>::new(), "premise: nothing parked");
    let m = std::sync::Arc::new(w.m);
    let competing = (std::sync::Arc::clone(&m), bind.clone());
    let (outer, competitor) = race_hooks::race(
        Race::DdlAppend,
        move || competing.0.apply_batch(std::slice::from_ref(&competing.1)).unwrap(),
        || m.apply_batch(&[bind.clone(), write.clone()]).unwrap(),
    );
    race_hooks::assert_absorbed(Race::DdlAppend);
    assert_eq!(register_of(&m).1, vec![bind.stamp], "one parked row, the binding: {competitor:?}");
    assert!(!m.has_oplog_entry(&write.stamp).unwrap(), "the write is left: {outer:?}");
    assert_eq!(outer.stopped_at(), Some(write.stamp), "{outer:?}");
    assert_eq!(vectors(&m, &bind), (below, below), "both vectors stay below the binding");
    assert!(no_parked_row_at_or_below_a_vector(&m) > 0, "nothing was checked");
}

// -- S: left means no effect, kind by kind, each with the effect shown where nothing is parked ------

/// What an S row prepares: A's later entry, the member that takes it, and what the entry would change on it.
struct Later {
    entry: OplogEntry,
    w: World,
    /// The facts of the member the entry changes where nothing stops it.
    facts: fn(&Engine) -> BTreeMap<&'static str, String>,
    /// The entry changes nothing but the witnessed vector where nothing stops it.
    only_witnessed: bool,
}

/// One S row. First the CONTROL: with nothing parked, the same entry on a member made the same way HAS its effect. Then, with the
/// binding parked, the entry is left: not in the oplog, no effect, the witnessed vector unmoved. Every failure is collected, so that
/// the effect is seen to fail and not only the first thing that does.
fn left_with_no_effect(row: &str, prepare: fn() -> Later) {
    let mut failures = Vec::new();
    let facts_and_witnessed =
        |later: &Later| ((later.facts)(&later.w.m), vectors(&later.w.m, &later.entry).1);

    let control = prepare();
    let before = facts_and_witnessed(&control);
    control.w.m.apply_batch(std::slice::from_ref(&control.entry)).unwrap();
    control.w.m.finish_purges_now().unwrap();
    let after = facts_and_witnessed(&control);
    if control.only_witnessed {
        if !(after.1 > before.1 && after.0 == before.0) {
            failures
                .push(format!("CONTROL: only the witnessed vector rises: {before:?} {after:?}"));
        }
    } else if after.0 == before.0 {
        failures
            .push(format!("CONTROL: with nothing parked the entry changes nothing: {before:?}"));
    }

    let later = prepare();
    later.w.m.apply_batch(std::slice::from_ref(&later.w.event)).unwrap();
    assert_eq!(register_of(&later.w.m).1, vec![later.w.event.stamp], "premise: parked");
    assert!(later.entry.stamp > later.w.event.stamp, "premise: above the parked event");
    let before = facts_and_witnessed(&later);
    let outcome = later.w.m.apply_batch(std::slice::from_ref(&later.entry)).unwrap();
    later.w.m.finish_purges_now().unwrap();
    let after = facts_and_witnessed(&later);
    if outcome.stopped_at() != Some(later.entry.stamp) {
        failures.push(format!("LEFT: the batch does not stop at the entry: {outcome:?}"));
    }
    if later.w.m.has_oplog_entry(&later.entry.stamp).unwrap() {
        failures.push("LEFT: the entry is in the oplog".to_string());
    }
    if after.0 != before.0 {
        failures.push(format!("EFFECT: {:?} became {:?}", before.0, after.0));
    }
    if after.1 != before.1 {
        failures.push(format!("EFFECT: witnessed {:?} became {:?}", before.1, after.1));
    }
    assert!(failures.is_empty(), "{row}:\n{}", failures.join("\n"));
}

fn held(e: &Engine, name: &str) -> String {
    match e.get_collection(DB, name) {
        Ok(meta) => {
            format!("held, {} document(s)", e.count(&meta, WalkScope::Background).unwrap())
        }
        Err(_) => "not held".to_string(),
    }
}

/// The binding world with nothing parked yet, whose later entry is whatever `then` makes A, and any other member, do.
fn later_of(
    facts: fn(&Engine) -> BTreeMap<&'static str, String>,
    only_witnessed: bool,
    then: impl FnOnce(&World) -> OplogEntry,
) -> Later {
    let w = world(Parks::Binding);
    let entry = then(&w);
    Later { entry, w, facts, only_witnessed }
}

#[test]
fn a_document_behind_a_parked_event_is_not_written() {
    left_with_no_effect("S-doc", || {
        later_of(|e| BTreeMap::from([("items", held(e, "items"))]), false, |w| w.write.clone())
    });
}

#[test]
fn a_stage_creation_behind_a_parked_event_makes_nothing() {
    left_with_no_effect("S-stage", || {
        later_of(
            |e| {
                let txn = e.db().begin_read().unwrap();
                let owned = crate::register::owns_id(&txn, of(STAGE2)).unwrap();
                BTreeMap::from([("stage", held(e, STAGE2)), ("stage row", owned.to_string())])
            },
            false,
            |w| {
                let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
                made(&w.author.a, OpKind::CreateCollection, of(STAGE2), |a| {
                    a.create_stage(DB, STAGE2, MergePolicy::Document, deadline).unwrap();
                })
            },
        )
    });
}

#[test]
fn a_database_drop_behind_a_parked_event_drops_nothing() {
    left_with_no_effect("S-dbdrop", || {
        later_of(
            |e| {
                BTreeMap::from([
                    ("database tombstone", format!("{:?}", e.database_dropped_at(DB).unwrap())),
                    ("items", held(e, "items")),
                ])
            },
            false,
            |w| {
                made(&w.author.a, OpKind::DropCollection, database_drop_id(DB), |a| {
                    a.drop_database(DB).unwrap();
                })
            },
        )
    });
}

#[test]
fn a_collection_drop_behind_a_parked_event_buries_nothing() {
    left_with_no_effect("S-drop", || {
        later_of(
            |e| BTreeMap::from([("items", held(e, "items"))]),
            false,
            |w| {
                made(&w.author.a, OpKind::DropCollection, of("items"), |a| {
                    assert!(a.drop_collection(DB, "items").unwrap());
                })
            },
        )
    });
}

/// Another member drops `name`, which it holds from `creation`, above `above`; answers the drop's entry.
fn dropped_elsewhere(creation: &OplogEntry, name: &str, above: &Stamp) -> OplogEntry {
    let (z, _dz) = engine();
    window(&z, std::slice::from_ref(creation));
    z.advance_clock_past(above);
    made(&z, OpKind::DropCollection, of(name), |z| {
        assert!(z.drop_collection(DB, name).unwrap());
    })
}

/// The binding world, and A's creation of the plain collection `name`, which M took in position with the rest.
fn world_with_a_plain_collection(name: &str) -> (World, OplogEntry) {
    let w = world(Parks::Binding);
    let creation = w
        .prefix
        .iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == of(name))
        .expect("premise: M took the creation")
        .clone();
    (w, creation)
}

#[test]
fn a_write_above_the_floor_of_a_gone_collection_behind_a_parked_event_remakes_nothing() {
    left_with_no_effect("S-remake", || {
        let (w, creation) = world_with_a_plain_collection("plain");
        let dropped = dropped_elsewhere(&creation, "plain", &creation.stamp);
        window(&w.m, std::slice::from_ref(&dropped));
        assert!(w.m.get_collection(DB, "plain").is_err(), "premise: M holds the drop");
        w.author.a.advance_clock_past(&dropped.stamp);
        let entry = insert(&w.author.a, "plain", 2);
        Later {
            entry,
            w,
            facts: |e| BTreeMap::from([("plain", held(e, "plain"))]),
            only_witnessed: false,
        }
    });
}

#[test]
fn a_document_history_answers_behind_a_parked_event_is_not_witnessed() {
    left_with_no_effect("S-history", || {
        let (w, creation) = world_with_a_plain_collection("temp");
        let entry = insert(&w.author.a, "temp", 3);
        let dropped = dropped_elsewhere(&creation, "temp", &entry.stamp);
        window(&w.m, std::slice::from_ref(&dropped));
        Later {
            entry,
            w,
            facts: |e| BTreeMap::from([("temp", held(e, "temp"))]),
            only_witnessed: true,
        }
    });
}

/// A member's own report of a broken constraint is taken at the top of the apply with nothing appended, and is a real entry of its
/// oplog, so a real lost one.
#[test]
fn a_local_observation_behind_a_parked_event_is_not_witnessed() {
    left_with_no_effect("S-local", || {
        later_of(
            |_| BTreeMap::new(),
            true,
            |w| OplogEntry {
                stamp: Stamp::new(w.write.stamp.hlc, w.author.a.node_id(), w.author.a.epoch()),
                kind: OpKind::UniqueViolation,
                collection: of("items"),
                doc_id: None,
                body: None,
                meta: None,
            },
        )
    });
}

// -- P5 to P7, E: what else is left, and where the batch ends -------------------------------------

fn stage2_creation(a: &Engine) -> OplogEntry {
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    made(a, OpKind::CreateCollection, of(STAGE2), |a| {
        a.create_stage(DB, STAGE2, MergePolicy::Document, deadline).unwrap();
    })
}

/// What follows the parked event is itself of a kind only this release makes: a second stage's creation, in one batch with it.
#[test]
fn a_second_stage_creation_in_the_parking_batch_is_left() {
    let w = world_writing(Parks::Binding, |a| stage2_creation(&a.a));
    stops_in(&w, Parks::Binding);
    assert_eq!(held(&w.m, STAGE2), "not held");
    let txn = w.m.db().begin_read().unwrap();
    assert!(!crate::register::owns_id(&txn, of(STAGE2)).unwrap(), "no stage row");
}

/// The stop is read from the store: a later batch, after the member is closed and opened again, stops alike, and so does the same
/// batch given once more.
#[test]
fn after_a_reopen_a_later_batch_still_stops_and_so_does_the_same_batch_again() {
    let w = world(Parks::Binding);
    w.m.apply_batch(std::slice::from_ref(&w.event)).unwrap();
    let below = w.below();
    let World { m, _dm, event, write, .. } = w;
    drop(m);
    let m = Engine::open(&_dm.path().join("kimmy.redb")).unwrap();
    for time in ["first", "again"] {
        let outcome = m.apply_batch(std::slice::from_ref(&write)).unwrap();
        assert_eq!(outcome.stopped_at(), Some(write.stamp), "{time}: {outcome:?}");
        assert!(!m.has_oplog_entry(&write.stamp).unwrap(), "{time}: not taken");
        assert_eq!(vectors(&m, &event), (below, below), "{time}");
        assert!(no_parked_row_at_or_below_a_vector(&m) > 0, "{time}: nothing was checked");
    }
}

/// M takes, in a window, another member's later write of the document the world's write makes: the world's write then loses.
fn the_write_is_already_beaten(w: &World) {
    let (z, _dz) = engine();
    let items = w
        .prefix
        .iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == of("items"))
        .expect("premise: M took the creation");
    window(&z, std::slice::from_ref(items));
    z.advance_clock_past(&w.write.stamp);
    let newer = insert(&z, "items", 1);
    assert_eq!(newer.doc_id, w.write.doc_id, "premise: the same document");
    assert!(newer.stamp > w.write.stamp, "premise: newer");
    window(&w.m, std::slice::from_ref(&newer));
}

/// An entry that appends nothing would still raise the witnessed vector, which covers the event as well as an appended one does.
#[test]
fn a_write_that_would_append_nothing_does_not_raise_witnessed_past_a_parked_event() {
    let control = world(Parks::Binding);
    the_write_is_already_beaten(&control);
    assert!(
        vectors(&control.m, &control.write).1 < control.write.stamp.hlc,
        "premise: not yet witnessed"
    );
    control.m.apply_batch(std::slice::from_ref(&control.write)).unwrap();
    assert!(
        !control.m.has_oplog_entry(&control.write.stamp).unwrap(),
        "premise: with nothing parked it appends nothing"
    );
    assert_eq!(
        vectors(&control.m, &control.write).1,
        control.write.stamp.hlc,
        "premise: and witnessed rises past it"
    );

    let w = world(Parks::Binding);
    the_write_is_already_beaten(&w);
    w.m.apply_batch(std::slice::from_ref(&w.event)).unwrap();
    let outcome = w.m.apply_batch(std::slice::from_ref(&w.write)).unwrap();
    assert_eq!(vectors(&w.m, &w.event).1, w.below(), "witnessed stays below: {outcome:?}");
    assert_eq!(outcome.stopped_at(), Some(w.write.stamp), "{outcome:?}");
    assert!(no_parked_row_at_or_below_a_vector(&w.m) > 0, "nothing was checked");
}

/// The batch ENDS at the entry left: what follows it is not looked at, another origin's entry included.
#[test]
fn the_batch_ends_at_the_entry_left() {
    let w = world(Parks::Binding);
    let (x, _dx) = engine();
    x.advance_clock_past(&w.write.stamp);
    let others = made(&x, OpKind::CreateCollection, of("others"), |x| {
        x.create_collection(DB, "others").unwrap();
    });
    let batch = [w.event.clone(), w.write.clone(), others.clone()];
    let outcome = w.m.apply_batch(&batch).unwrap();
    assert!(!w.m.has_oplog_entry(&others.stamp).unwrap(), "the third is not taken: {outcome:?}");
    assert!(w.m.get_collection(DB, "others").is_err(), "and has no effect");
    assert_eq!(vectors(&w.m, &others), (Hlc::ZERO, Hlc::ZERO), "nor is it witnessed");
    assert_eq!(outcome.stopped_at(), Some(w.write.stamp), "{outcome:?}");
}

// -- T3, G, N, R: three lost, the grant, what is newly served, and the refusal while stalled ---------

/// Three events lost together. Only the first parks; one window releases all three, each settled by itself: the buried stage's
/// tombstone is at the DROP's stamp, where one pass that saw drop and recreation together would give the recreation's.
#[test]
fn of_three_lost_events_the_first_parks_and_one_window_releases_them_in_order() {
    let w = world(Parks::Recreation);
    let dropped = w.drop.clone().expect("the world's drop");
    let bind = w.author.bind.clone();
    let prefix = w.author.before(&bind);
    let below = prefix.last().expect("a prefix").stamp.hlc;
    let (m, _dm) = engine();
    window(&m, &prefix);
    let outcome = m.apply_batch(&[bind.clone(), dropped.clone(), w.event.clone()]).unwrap();
    assert_eq!(register_of(&m).1, vec![bind.stamp], "one parked, the first: {outcome:?}");
    assert!(!m.has_oplog_entry(&dropped.stamp).unwrap(), "the drop is left");
    assert!(!m.has_oplog_entry(&w.event.stamp).unwrap(), "and the recreation");
    assert_eq!(outcome.stopped_at(), Some(dropped.stamp), "{outcome:?}");
    assert_eq!(vectors(&m, &bind), (below, below));
    assert!(no_parked_row_at_or_below_a_vector(&m) > 0, "nothing was checked");

    let theirs = w.author.a.version_vector().unwrap();
    let mine = m.witnessed_vector().unwrap();
    let start = mine.behind(&theirs).expect("M is behind");
    let next = w
        .author
        .a
        .entries_for_peer_marked(start, 1024, Some(&mine), &[], WalkScope::Background)
        .unwrap();
    let events = [bind.stamp, dropped.stamp, w.event.stamp];
    assert_eq!(
        next.entries.iter().map(|e| e.stamp).filter(|s| events.contains(s)).collect::<Vec<_>>(),
        events,
        "one window carries all three"
    );
    window(&m, &next.entries);
    assert_eq!(register_of(&m).1, Vec::<Stamp>::new(), "nothing parked");
    assert_eq!(said(&m), wanted(&w));
    assert_eq!(m.collection_dropped_at(of(STAGE)).unwrap(), Some(dropped.stamp), "in order");
}

/// A stalled member whose catch-up is a whole snapshot: the grant releases the event, and what was left arrives as state.
#[test]
fn a_snapshot_grant_releases_a_stalled_member_and_covers_what_was_left() {
    let w = world(Parks::Binding);
    let outcome = w.m.apply_batch(&[w.event.clone(), w.write.clone()]).unwrap();
    assert_eq!(outcome.stopped_at(), Some(w.write.stamp), "premise: stalled: {outcome:?}");
    take_a_snapshot(&w.m, &w.author.a);
    assert_eq!(register_of(&w.m).1, Vec::<Stamp>::new(), "nothing parked");
    assert_eq!(said(&w.m), wanted(&w));
    let items = w.m.get_collection(DB, "items").unwrap();
    let id = w.write.doc_id.as_ref().expect("a document");
    assert!(w.m.get(&items, id).unwrap().is_some(), "what was left is held");
    let (served, witnessed) = vectors(&w.m, &w.write);
    assert!(
        served >= w.write.stamp.hlc && witnessed >= w.write.stamp.hlc,
        "and covered: {served:?}, {witnessed:?}"
    );
    let (restored, _dr) = restored_copy(&w.m);
    assert_eq!(said(&restored), said(&w.m), "restored");
    assert_eq!(vectors(&restored, &w.write), vectors(&w.m, &w.write), "restored: its vectors");
}

/// Two documents into the physical the name is served from once the event is in effect, and the delete of one; answers the first.
fn into_what_is_served(a: &Author) -> OplogEntry {
    let served = a.a.resolve(DB, "orders").unwrap().physical;
    assert_ne!(served, "orders", "premise: the event serves another physical");
    let coll = a.a.get_collection(DB, &served).unwrap();
    let mut first_id = None;
    let first = made(&a.a, OpKind::Insert, coll.id, |a| {
        first_id = Some(a.insert(&coll, doc! { "_id": 1 }).unwrap());
    });
    a.a.insert(&coll, doc! { "_id": 2 }).unwrap();
    assert!(a.a.delete(&coll, &first_id.expect("inserted")).unwrap());
    first
}

/// Everything `e` stores of one physical: its documents, live and deleted, and its names and options rows.
#[derive(Debug, PartialEq)]
struct Physical {
    docs: Vec<(Vec<u8>, Stamp, bool, Vec<u8>)>,
    rows: Vec<(u8, Vec<u8>, Vec<u8>)>,
}

fn physical(e: &Engine, id: CollectionId) -> Physical {
    let txn = e.db().begin_read().unwrap();
    let table = txn.open_table(crate::tables::DOCS).unwrap();
    let from: (u64, &[u8]) = (id.0, &[]);
    let to: (u64, &[u8]) = (id.0 + 1, &[]);
    let docs = table
        .range(from..to)
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            let record = crate::codec::decode_doc_record(value.value()).unwrap();
            (key.value().1.to_vec(), record.stamp, record.deleted, record.body)
        })
        .collect();
    Physical { docs, rows: crate::register::rows_of(&txn, id).unwrap() }
}

/// The later writes go into what the parked event makes served: left while it is parked, and applied, into the physical just made
/// visible, by the one window that releases it. A, M and a member that joins from M then store the same of that physical.
fn writes_into_what_is_newly_served(parks: Parks) {
    let w = world_writing(parks, into_what_is_served);
    let writes: Vec<OplogEntry> =
        everything(&w.author.a).into_iter().filter(|e| e.stamp >= w.write.stamp).collect();
    assert_eq!(writes.len(), 3, "premise: two inserts and a delete: {writes:?}");
    let batch: Vec<OplogEntry> = std::iter::once(w.event.clone()).chain(writes.clone()).collect();
    let outcome = w.m.apply_batch(&batch).unwrap();
    assert_eq!(outcome.stopped_at(), Some(w.write.stamp), "{parks:?}: {outcome:?}");
    assert_eq!(outcome.parked_stop, 1, "{parks:?}: {outcome:?}");
    for write in &writes {
        assert!(!w.m.has_oplog_entry(&write.stamp).unwrap(), "{parks:?}: left: {write:?}");
    }
    assert_eq!(vectors(&w.m, &w.event), (w.below(), w.below()), "{parks:?}");
    assert!(no_parked_row_at_or_below_a_vector(&w.m) > 0, "{parks:?}: nothing was checked");

    let theirs = w.author.a.version_vector().unwrap();
    let mine = w.m.witnessed_vector().unwrap();
    let start = mine.behind(&theirs).expect("M is behind");
    let next = w
        .author
        .a
        .entries_for_peer_marked(start, 1024, Some(&mine), &[], WalkScope::Background)
        .unwrap();
    assert_eq!(
        next.entries.iter().map(|e| e.stamp).collect::<Vec<_>>(),
        batch.iter().map(|e| e.stamp).collect::<Vec<_>>(),
        "{parks:?}: one window carries the event and the writes"
    );
    window(&w.m, &next.entries);
    assert_eq!(register_of(&w.m).1, Vec::<Stamp>::new(), "{parks:?}: nothing parked");
    assert_eq!(said(&w.m), wanted(&w), "{parks:?}");

    let (j, _dj) = joiner(&w.m);
    let id = writes[0].collection;
    let holder = physical(&w.author.a, id);
    assert!(
        holder.docs.iter().any(|d| d.2) && holder.docs.iter().any(|d| !d.2),
        "premise: a tombstone and a live row: {holder:?}"
    );
    assert_eq!(physical(&w.m, id), holder, "{parks:?}: M");
    assert_eq!(physical(&j, id), holder, "{parks:?}: the joiner");
}

#[test]
fn writes_into_a_newly_bound_stage_arrive_with_its_release() {
    writes_into_what_is_newly_served(Parks::Binding);
}

#[test]
fn writes_into_a_newly_served_recreation_arrive_with_its_release() {
    writes_into_what_is_newly_served(Parks::Recreation);
}

/// While M is stalled it refuses a write to the name, or, where the parked event is a recreation, has no such name. A local write of
/// M's on another collection, of M's own origin, moves nothing the stop holds still.
#[test]
fn while_stalled_a_write_is_refused_and_a_local_write_changes_nothing_of_the_stall() {
    for parks in WORLDS {
        let w = world(parks);
        w.m.apply_batch(&[w.event.clone(), w.write.clone()]).unwrap();
        let answer = w.m.resolve_for_write(DB, "orders");
        match parks {
            Parks::Binding | Parks::Drop => assert!(
                matches!(&answer, Err(StorageError::NameSuperseded { parked: true, .. })),
                "{parks:?}: {answer:?}"
            ),
            _ => assert!(
                matches!(
                    &answer,
                    Err(StorageError::Core(kimmy_core::Error::CollectionNotFound { .. }))
                ),
                "{parks:?}: {answer:?}"
            ),
        }
        let items = w.m.get_collection(DB, "items").unwrap();
        w.m.insert(&items, doc! { "_id": 500 }).unwrap();
        assert_eq!(vectors(&w.m, &w.event), (w.below(), w.below()), "{parks:?}");
        assert!(no_parked_row_at_or_below_a_vector(&w.m) > 0, "{parks:?}: nothing was checked");
        let (j, _dj) = joiner(&w.m);
        let (served, witnessed) = vectors(&j, &w.event);
        assert!(
            served < w.event.stamp.hlc && witnessed < w.event.stamp.hlc,
            "{parks:?}: joined: {served:?}, {witnessed:?}"
        );
        pull(&j, &w.author.a);
        assert_eq!(said(&j), wanted(&w), "{parks:?}: the joiner");
    }
}

// -- AR: the one-entry door with no window ------------------------------------------------------

#[test]
fn apply_remote_leaves_what_stands_behind_a_parked_event() {
    let items = |e: &Engine| e.get_collection(DB, "items").unwrap();
    let control = world(Parks::Binding);
    assert!(
        control.m.apply_remote(&items(&control.m), &control.write).unwrap(),
        "CONTROL: with nothing parked the entry is applied"
    );

    let w = parked_binding();
    let coll = items(&w.m);
    let before = vectors(&w.m, &w.event);
    assert!(!w.m.apply_remote(&coll, &w.write).unwrap(), "the later write is left");
    assert!(!w.m.has_oplog_entry(&w.write.stamp).unwrap(), "not in the oplog");
    let id = w.write.doc_id.as_ref().expect("a document");
    assert!(w.m.get(&coll, id).unwrap().is_none(), "not written");
    assert_eq!(vectors(&w.m, &w.event), before, "neither vector moves");
    assert!(no_parked_row_at_or_below_a_vector(&w.m) > 0, "nothing was checked");

    let (x, _dx) = engine();
    let creation = w
        .prefix
        .iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == of("items"))
        .expect("premise: M took the creation");
    window(&x, std::slice::from_ref(creation));
    x.advance_clock_past(&w.event.stamp);
    let others = insert(&x, "items", 7);
    assert!(w.m.apply_remote(&coll, &others).unwrap(), "another origin's later entry is applied");
    assert!(w.m.apply_remote(&coll, &w.author.older).unwrap(), "an older entry of the origin is");

    // An entry with no document does nothing but move the clock: seen in the member's next stamp.
    let ahead = OplogEntry {
        stamp: Stamp::new(
            Hlc::new(physical_now_ms() + 5_000, 0),
            w.author.a.node_id(),
            w.author.a.epoch(),
        ),
        kind: OpKind::UniqueViolation,
        collection: of("items"),
        doc_id: None,
        body: None,
        meta: None,
    };
    assert!(!w.m.apply_remote(&coll, &ahead).unwrap());
    let next = insert(&w.m, "items", 600);
    assert!(next.stamp < ahead.stamp, "the clock did not move: {next:?}");
}

// -- H: the invariant helper's own row ------------------------------------------------------------

#[test]
fn the_invariant_helper_fails_at_the_parked_stamp_and_above_it() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let w = parked_binding();
    let at = w.event.stamp;
    let witnessed_to = |hlc: Hlc| {
        let mut seen = VersionVector::default();
        seen.observe(Stamp::new(hlc, at.node, at.epoch));
        w.m.absorb_witnessed(&seen).unwrap();
        assert_eq!(vectors(&w.m, &w.event).1, hlc, "premise: witnessed raised to it");
        catch_unwind(AssertUnwindSafe(|| no_parked_row_at_or_below_a_vector(&w.m)))
    };
    let just_below = Hlc::new(at.hlc.wall_ms - 1, u16::MAX);
    assert_eq!(witnessed_to(just_below).ok(), Some(1), "just below: it passes, one row looked at");
    assert!(witnessed_to(at.hlc).is_err(), "at exactly the parked stamp it fails");
    assert!(witnessed_to(Hlc::new(at.hlc.wall_ms, 1)).is_err(), "and above it");
}

// -- W2: two parked rows ---------------------------------------------------------------------------

/// A second author, C: a binding over `name`, made as A's is, and a later write of its own.
struct Second {
    prefix: Vec<OplogEntry>,
    bind: OplogEntry,
    write: OplogEntry,
    _dir: tempfile::TempDir,
}

fn second_author(name: &str) -> Second {
    let (c, dir) = engine();
    c.create_collection(DB, name).unwrap();
    c.create_collection(DB, "cfiles").unwrap();
    let stage = format!("{name}.__stg.c1");
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    let creation = made(&c, OpKind::CreateCollection, of(&stage), |c| {
        c.create_stage(DB, &stage, MergePolicy::Document, deadline).unwrap();
    });
    let bind = OplogEntry {
        stamp: Stamp::new(Hlc::new(creation.stamp.hlc.wall_ms + 1, 0), c.node_id(), c.epoch()),
        kind: OpKind::BindCollection,
        collection: of(name),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: name.into(),
                physical: stage.clone(),
            })
            .unwrap(),
        ),
        meta: None,
    };
    let prefix = everything(&c);
    assert!(prefix.iter().all(|e| e.stamp < bind.stamp), "premise: all below the binding");
    c.advance_clock_past(&bind.stamp);
    let write = insert(&c, "cfiles", 1);
    Second { prefix, bind, write, _dir: dir }
}

/// Two names, two origins, one event parked for each. The later entry is of the origin whose parked row sorts SECOND: a read that
/// answers from the first row only would take it.
fn the_second_row_stops_its_origin(a_sorts_second: bool) {
    let name = (0..)
        .map(|n| format!("second{n}"))
        .find(|name| (of(name).0 < of("orders").0) == a_sorts_second)
        .expect("a name on each side");
    let w = world(Parks::Binding);
    let c = second_author(&name);
    window(&w.m, &c.prefix);
    w.m.apply_batch(std::slice::from_ref(&w.event)).unwrap();
    w.m.apply_batch(std::slice::from_ref(&c.bind)).unwrap();
    {
        let txn = w.m.db().begin_read().unwrap();
        let rows = carried_rows(&txn, Carried::Info).unwrap();
        assert_eq!(rows.len(), 2, "premise: one parked for each");
        let second = if a_sorts_second { of("orders") } else { of(&name) };
        assert!(rows[1].0.starts_with(&second.0.to_be_bytes()), "premise: the order of the rows");
    }
    let later = if a_sorts_second { w.write.clone() } else { c.write.clone() };
    let outcome = w.m.apply_batch(std::slice::from_ref(&later)).unwrap();
    assert_eq!(outcome.stopped_at(), Some(later.stamp), "{outcome:?}");
    assert!(!w.m.has_oplog_entry(&later.stamp).unwrap(), "left");
    assert_eq!(no_parked_row_at_or_below_a_vector(&w.m), 2);
}

#[test]
fn with_two_parked_the_row_that_sorts_second_stops_its_origin_the_authors() {
    the_second_row_stops_its_origin(true);
}

#[test]
fn with_two_parked_the_row_that_sorts_second_stops_its_origin_the_other_one() {
    the_second_row_stops_its_origin(false);
}
