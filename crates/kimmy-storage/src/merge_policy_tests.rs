//! The merge policy of a collection (Q7 Step 2): chosen at creation, fixed, carried by the
//! creation entry and the snapshot definition, `document` for system and shadow collections.

use kimmy_core::{CollectionCreate, Hlc, MergePolicy, OpKind};

use crate::WalkScope;

use crate::Engine;
use crate::snapshot::CollectionState;

fn open() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

#[test]
fn the_helper_creates_document_and_the_explicit_form_the_policy_asked() {
    let (e, _d) = open();
    assert_eq!(e.create_collection("app", "plain").unwrap().merge_policy, MergePolicy::Document);
    let f = e.create_collection_with_policy("app", "wide", MergePolicy::Field, None).unwrap();
    assert_eq!(f.merge_policy, MergePolicy::Field);
    // stored, not just returned
    assert_eq!(e.get_collection("app", "wide").unwrap().merge_policy, MergePolicy::Field);
    assert_eq!(e.get_collection("app", "plain").unwrap().merge_policy, MergePolicy::Document);
}

#[test]
fn system_and_shadow_collections_are_document_whatever_is_asked() {
    let (e, _d) = open();
    e.create_collection_with_policy("app", "orders", MergePolicy::Field, None).unwrap();
    let sys = e.create_system_collection("app", "__jobs").unwrap();
    assert_eq!(sys.merge_policy, MergePolicy::Document);
    let shadow = e
        .create_collection_with_policy("app", "orders.__vectors", MergePolicy::Field, None)
        .unwrap();
    assert_eq!(shadow.merge_policy, MergePolicy::Document);
    assert_eq!(e.get_collection("app", "orders").unwrap().merge_policy, MergePolicy::Field);
}

#[test]
fn a_collection_exists_under_one_policy_and_a_second_creation_does_not_change_it() {
    let (e, _d) = open();
    e.create_collection_with_policy("app", "c", MergePolicy::Field, None).unwrap();
    assert!(e.create_collection_with_policy("app", "c", MergePolicy::Document, None).is_err());
    assert_eq!(e.get_collection("app", "c").unwrap().merge_policy, MergePolicy::Field);
}

#[test]
fn the_creation_entry_and_the_snapshot_definition_carry_the_policy() {
    let (e, _d) = open();
    e.create_collection_with_policy("app", "wide", MergePolicy::Field, None).unwrap();
    let entry = e
        .read_oplog_from(Hlc::ZERO, 100, WalkScope::Request)
        .unwrap()
        .into_iter()
        .find(|x| x.kind == OpKind::CreateCollection)
        .expect("a creation entry");
    let create: CollectionCreate =
        bson::deserialize_from_slice(entry.body.as_ref().unwrap()).unwrap();
    assert_eq!(create.merge_policy, MergePolicy::Field);

    let state = CollectionState::from(e.get_collection("app", "wide").unwrap());
    assert_eq!(state.merge_policy, MergePolicy::Field);
    let doc = bson::serialize_to_document(&state).unwrap();
    assert_eq!(doc.get_str("merge_policy").unwrap(), "field");
    let mut without = doc.clone();
    without.remove("merge_policy");
    assert!(bson::deserialize_from_document::<CollectionState>(without).is_err());
}

#[test]
fn a_replicated_creation_creates_under_the_policy_it_carries() {
    let (a, _da) = open();
    let (b, _db) = open();
    a.create_collection_with_policy("app", "wide", MergePolicy::Field, None).unwrap();
    a.create_collection("app", "plain").unwrap();
    let entries = a.read_oplog_from(Hlc::ZERO, 100, WalkScope::Request).unwrap();
    b.apply_batch(&entries).unwrap();
    assert_eq!(b.get_collection("app", "wide").unwrap().merge_policy, MergePolicy::Field);
    assert_eq!(b.get_collection("app", "plain").unwrap().merge_policy, MergePolicy::Document);
}
