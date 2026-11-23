//! K1: a swap's stage is created by an ordinary `CreateCollection` (ADR-272).
//!
//! The design assumed it: the stage's creation carries the deadline and the target's merge policy, and needs
//! no kind of its own. This file is the check. A member makes a stage and what it logs is one `CreateCollection`
//! entry; a member that takes that entry by the ordinary window makes the same hidden collection, under the id
//! derived from its physical name, with the policy and the deadline the creation carried; and an ordinary
//! creation is what it always was.
//!
//! Public API only.

use kimmy_core::{CollectionCreate, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry};
use kimmy_storage::{Engine, PeerWindow, SyncOutcome, WalkScope};

const STAGE: &str =
    "orders.__stg.0aaa0000000000000000000000000000000000000000000000000000000000000000000000000000";

fn engine(dir: &tempfile::TempDir, name: &str) -> Engine {
    Engine::open(&dir.path().join(name)).unwrap()
}

/// Every entry `from` holds, as a peer asking from nothing would be served them.
fn everything(from: &Engine) -> Vec<OplogEntry> {
    let theirs = from.version_vector().unwrap();
    let nothing = kimmy_core::VersionVector::new();
    let start = nothing.behind(&theirs).expect("the member holds entries");
    from.entries_for_peer_holding(start, 4096, Some(&nothing), WalkScope::Background)
        .unwrap()
        .entries
}

fn pull_all(to: &Engine, from: &Engine) {
    let theirs = from.version_vector().unwrap();
    let mine = to.witnessed_vector().unwrap();
    let Some(start) = mine.behind(&theirs) else { return };
    let window =
        from.entries_for_peer_holding(start, 1024, Some(&mine), WalkScope::Background).unwrap();
    let mut outcome = SyncOutcome::default();
    to.apply_peer_window_into(
        &theirs,
        Some(&mine),
        &window.entries,
        PeerWindow {
            scanned_to: window.scanned_to,
            exhausted: window.exhausted,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )
    .unwrap();
}

fn creation_of(entries: &[OplogEntry], collection: CollectionId) -> CollectionCreate {
    let creations: Vec<_> = entries
        .iter()
        .filter(|e| e.kind == OpKind::CreateCollection && e.collection == collection)
        .collect();
    assert_eq!(creations.len(), 1, "one creation entry for the stage: {creations:?}");
    bson::deserialize_from_slice(creations[0].body.as_ref().unwrap()).unwrap()
}

#[test]
fn a_stage_is_logged_as_one_creation_that_carries_its_deadline_and_the_targets_policy() {
    let dir = tempfile::tempdir().unwrap();
    let a = engine(&dir, "a.redb");
    let deadline = Hlc::new(kimmy_storage::physical_now_ms() + 30_000, 0);

    let meta = a.create_stage("app", STAGE, MergePolicy::Field, deadline).unwrap();
    assert_eq!(
        meta.id,
        CollectionId::derive("app", STAGE),
        "the id is derived from the physical name"
    );
    assert_eq!(meta.merge_policy, MergePolicy::Field, "the stage inherits the target's policy");
    assert_eq!(meta.stage.map(|s| s.deadline), Some(deadline));
    assert!(
        meta.join_retention_ms.is_some() && meta.loss_limits.is_some(),
        "a field stage holds R and K"
    );

    let entries = everything(&a);
    assert!(
        entries.iter().all(|e| e.kind == OpKind::CreateCollection),
        "the only entries are creations (the database's and the stage's): {entries:?}"
    );
    let creation = creation_of(&entries, meta.id);
    assert_eq!((creation.db.as_str(), creation.name.as_str()), ("app", STAGE));
    assert_eq!(creation.merge_policy, MergePolicy::Field);
    assert_eq!(creation.stage.map(|s| s.deadline), Some(deadline));
}

#[test]
fn a_member_that_takes_the_creation_makes_the_same_hidden_collection() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (engine(&dir, "a.redb"), engine(&dir, "b.redb"));
    let deadline = Hlc::new(kimmy_storage::physical_now_ms() + 30_000, 7);
    let made = a.create_stage("app", STAGE, MergePolicy::Document, deadline).unwrap();

    pull_all(&b, &a);
    let held = b.get_collection("app", STAGE).expect("the creation took effect on the peer");
    assert_eq!(
        held.id, made.id,
        "the same id: the invariant meta.id == derive(db, physical name) holds"
    );
    assert_eq!(held.merge_policy, MergePolicy::Document);
    assert_eq!(
        held.stage.map(|s| s.deadline),
        Some(deadline),
        "the deadline travelled with the creation"
    );
}

#[test]
fn an_ordinary_creation_carries_no_stage_and_its_bytes_are_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let a = engine(&dir, "a.redb");
    let ordinary = a.create_collection("app", "orders").unwrap();
    assert_eq!(ordinary.stage, None);
    let entries = everything(&a);
    let body = entries
        .iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == ordinary.id)
        .and_then(|e| e.body.as_ref())
        .unwrap();
    let doc: bson::Document = bson::deserialize_from_slice(body).unwrap();
    assert!(!doc.contains_key("stage"), "an ordinary creation has no `stage` key: {doc:?}");
}

#[test]
fn only_a_stages_name_can_be_made_a_stage() {
    let dir = tempfile::tempdir().unwrap();
    let a = engine(&dir, "a.redb");
    assert!(a.create_stage("app", "orders", MergePolicy::Document, Hlc::new(1, 0)).is_err());
    assert!(a.get_collection("app", "orders").is_err(), "nothing was made");
}
