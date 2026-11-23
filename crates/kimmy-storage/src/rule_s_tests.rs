//! Rule S with the S-bind-origin trigger (ADR-272, decision 6): a write to a stage that the stage's creator made, stamped below
//! the stage's known valid binding (or with none known), and that arrives in position is APPENDED and served onward even when
//! a held version supersedes it, with no change to the document. Nothing wider: an ordinary write of another origin to the
//! same collection is witnessed away as it always was, and nothing is appended under Hold (a snapshot document is state).
//!
//! Why: a sender that witnessed a staged row away leaves a hole in the window a peer asks it for, and the row is the only
//! copy the bound stage's rows have (DF-1); appending it keeps the stage's rows servable until the stage is bound or buried.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, DocId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
const STAGE: &str = "orders.__stg.a1";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// `entries` as one window served contiguously from the receiver's own position.
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

fn held(b: &Engine, entry: &OplogEntry) -> bool {
    b.oplog_entry(&entry.stamp).unwrap().is_some()
}

fn bind_at(a: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
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

/// A stage made by `a` under `policy`, and what a receiver needs of it: the creation and the document `_id: 1` inserted.
struct Staged {
    a: Engine,
    _da: tempfile::TempDir,
    creation: Vec<OplogEntry>,
    insert: OplogEntry,
}

fn staged(policy: MergePolicy) -> Staged {
    let (a, da) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE, policy, deadline).unwrap();
    let creation = everything(&a);
    let coll_a = a.get_collection(DB, STAGE).unwrap();
    later();
    a.insert(&coll_a, doc! { "_id": 1 }).unwrap();
    let insert = everything(&a).into_iter().last().unwrap();
    Staged { a, _da: da, creation, insert }
}

/// A's next write to the document: `field` set to `value`. Returns the entry.
fn write(s: &Staged, field: &str, value: i32) -> OplogEntry {
    later();
    let coll = s.a.get_collection(DB, STAGE).unwrap();
    s.a.replace(&coll, &DocId::Int64(1), doc! { "_id": 1, field: value }, false).unwrap();
    everything(&s.a).into_iter().last().unwrap()
}

/// A member that holds the stage and the first insert.
fn receiver(s: &Staged) -> (Engine, tempfile::TempDir) {
    let (b, db) = engine();
    b.apply_batch(&s.creation).unwrap();
    window(&b, std::slice::from_ref(&s.insert));
    (b, db)
}

#[test]
fn a_staged_row_the_creator_wrote_is_appended_though_a_held_version_supersedes_it_on_document() {
    let s = staged(MergePolicy::Document);
    let e1 = write(&s, "v", 1);
    let e2 = write(&s, "v", 2);
    let (b, _db) = receiver(&s);
    window(&b, std::slice::from_ref(&e2));
    let outcome = window(&b, std::slice::from_ref(&e1));
    let coll = b.get_collection(DB, STAGE).unwrap();
    assert_eq!(
        b.get(&coll, &DocId::Int64(1)).unwrap(),
        Some(doc! { "_id": 1, "v": 2 }),
        "the document is the newer write's: the superseded row changed nothing ({outcome:?})"
    );
    assert!(held(&b, &e1), "the superseded staged row is appended: this member can serve it");
    assert!(
        everything(&b).iter().any(|e| e.stamp == e1.stamp),
        "and it is in the window a peer is served"
    );
}

#[test]
fn a_staged_row_the_creator_wrote_is_appended_though_its_state_is_already_held_on_field() {
    // Green before rule S as well (see the characterization below): it pins the half of the rule that field already has.
    // A field member that holds the row's state (a snapshot's page, under Hold: no entry, no mark) and then meets the entry in
    // a window: the state already covers it, the join moves nothing, and the entry used to be witnessed away, so this member
    // held the row and could not serve it onward.
    let s = staged(MergePolicy::Field);
    let e1 = write(&s, "a", 1);
    let (b, _db) = receiver(&s);
    let coll = b.get_collection(DB, STAGE).unwrap();
    b.apply_held_for_test(&coll, &e1).unwrap();
    assert!(!held(&b, &e1), "premise: a state has no entry");
    window(&b, std::slice::from_ref(&e1));
    assert_eq!(
        b.get(&coll, &DocId::Int64(1)).unwrap(),
        Some(doc! { "_id": 1, "a": 1 }),
        "the document is unchanged"
    );
    assert!(held(&b, &e1), "the staged row is appended on a field collection");
}

/// Another member's ordinary writes to the physical: `m` took the creation and the insert and wrote twice; `b` holds the
/// second as an entry (a document collection) or the first as state (a field one) and then meets the first.
fn other_origin_case(
    policy: MergePolicy,
) -> (Engine, tempfile::TempDir, OplogEntry, tempfile::TempDir) {
    let s = staged(policy);
    let (m, dm) = receiver(&s);
    let coll_m = m.get_collection(DB, STAGE).unwrap();
    later();
    m.replace(&coll_m, &DocId::Int64(1), doc! { "_id": 1, "a": 5 }, false).unwrap();
    let first = everything(&m).into_iter().last().unwrap();
    later();
    m.replace(&coll_m, &DocId::Int64(1), doc! { "_id": 1, "a": 6 }, false).unwrap();
    let second = everything(&m).into_iter().last().unwrap();
    let (b, db) = receiver(&s);
    if policy == MergePolicy::Field {
        // The shape that moves nothing on a field collection: the state of the row is held, then its entry arrives.
        let coll = b.get_collection(DB, STAGE).unwrap();
        b.apply_held_for_test(&coll, &first).unwrap();
    } else {
        window(&b, std::slice::from_ref(&second));
    }
    window(&b, std::slice::from_ref(&first));
    (b, db, first, dm)
}

#[test]
fn a_write_of_another_origin_superseded_by_a_held_version_is_not_appended_on_document() {
    let (b, _db, theirs, _dm) = other_origin_case(MergePolicy::Document);
    assert!(
        !held(&b, &theirs),
        "rule S is the creator's rows only: another origin's is witnessed away"
    );
}

#[test]
fn on_field_every_entry_in_position_is_already_kept_so_rule_s_adds_nothing() {
    // A characterization, green before rule S: `field_entry_kept` appends an in-position entry this member does not hold
    // whatever its origin ("always keep"), so a field collection has no witnessed-away staged row for rule S to rescue, and the
    // creator's origin is not what decides there. Pinned so that a change to either half is seen.
    let (b, _db, theirs, _dm) = other_origin_case(MergePolicy::Field);
    assert!(held(&b, &theirs), "a field collection keeps the entry of any origin");
}

#[test]
fn a_staged_row_is_never_appended_under_hold() {
    let s = staged(MergePolicy::Document);
    let e1 = write(&s, "v", 1);
    let e2 = write(&s, "v", 2);
    let (b, _db) = receiver(&s);
    window(&b, std::slice::from_ref(&e2));
    let coll = b.get_collection(DB, STAGE).unwrap();
    b.apply_held_for_test(&coll, &e1).unwrap();
    assert!(!held(&b, &e1), "a snapshot document is state, never history: not appended");
}

#[test]
fn a_staged_row_above_the_known_binding_is_not_appended() {
    let s = staged(MergePolicy::Document);
    let e1 = write(&s, "v", 1);
    let e2 = write(&s, "v", 2);
    let (b, _db) = engine();
    b.apply_batch(&s.creation).unwrap();
    // A binding stamped just after the creation: the staged rows above it are late writes, not rule S's.
    let bind = bind_at(&s.a, s.creation.last().unwrap().stamp.hlc.wall_ms + 1);
    window(&b, std::slice::from_ref(&bind));
    window(&b, std::slice::from_ref(&s.insert));
    window(&b, std::slice::from_ref(&e2));
    window(&b, std::slice::from_ref(&e1));
    assert!(!held(&b, &e1), "above the stage's binding: not the staging rule's to append");
}

#[test]
fn a_staged_row_below_the_known_binding_is_appended() {
    let s = staged(MergePolicy::Document);
    let e1 = write(&s, "v", 1);
    let e2 = write(&s, "v", 2);
    let (b, _db) = engine();
    b.apply_batch(&s.creation).unwrap();
    window(&b, std::slice::from_ref(&s.insert));
    window(&b, std::slice::from_ref(&e2));
    let bind = bind_at(&s.a, e2.stamp.hlc.wall_ms + 10);
    window(&b, std::slice::from_ref(&bind));
    window(&b, std::slice::from_ref(&e1));
    assert!(held(&b, &e1), "below the binding: appended and served");
}

#[test]
fn an_ordinary_collection_of_the_same_origin_is_untouched() {
    // The same shape on a collection that is not a stage: the superseded write is witnessed away, as ever.
    let (a, _da) = engine();
    a.create_collection(DB, "plain").unwrap();
    let creation = everything(&a);
    let coll_a = a.get_collection(DB, "plain").unwrap();
    later();
    a.insert(&coll_a, doc! { "_id": 1 }).unwrap();
    let insert = everything(&a).into_iter().last().unwrap();
    later();
    a.replace(&coll_a, &DocId::Int64(1), doc! { "_id": 1, "v": 1 }, false).unwrap();
    let e1 = everything(&a).into_iter().last().unwrap();
    later();
    a.replace(&coll_a, &DocId::Int64(1), doc! { "_id": 1, "v": 2 }, false).unwrap();
    let e2 = everything(&a).into_iter().last().unwrap();
    let (b, _db) = engine();
    b.apply_batch(&creation).unwrap();
    window(&b, std::slice::from_ref(&insert));
    window(&b, std::slice::from_ref(&e2));
    window(&b, std::slice::from_ref(&e1));
    assert!(!held(&b, &e1), "not a stage: no rule S");
}
