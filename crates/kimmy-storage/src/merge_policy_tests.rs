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

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn creation_entries(e: &Engine) -> Vec<kimmy_core::OplogEntry> {
    e.read_oplog_from(Hlc::ZERO, 100, WalkScope::Request).unwrap()
}

fn policy_of(e: &Engine) -> MergePolicy {
    e.get_collection("app", "c").unwrap().merge_policy
}

/// Two members create one name at the same time under different policies. The policy follows the
/// creation with the later stamp, on every member, whichever order the entries arrive in.
fn converges_on_the_later_creation(first: MergePolicy, second: MergePolicy) {
    let (x, _dx) = open();
    let (y, _dy) = open();
    x.create_collection_with_policy("app", "c", first, None).unwrap();
    later();
    y.create_collection_with_policy("app", "c", second, None).unwrap();
    let (ex, ey) = (creation_entries(&x), creation_entries(&y));
    // each applies the other's entries
    y.apply_batch(&ex).unwrap();
    x.apply_batch(&ey).unwrap();
    assert_eq!(policy_of(&x), second, "the earlier creator takes the later creation's policy");
    assert_eq!(policy_of(&y), second, "the later creator keeps its own");
    // a member that has neither: both orders
    for (one, two) in [(&ex, &ey), (&ey, &ex)] {
        let (z, _dz) = open();
        z.apply_batch(one).unwrap();
        z.apply_batch(two).unwrap();
        assert_eq!(policy_of(&z), second);
    }
}

#[test]
fn concurrent_creations_under_different_policies_converge_on_the_later_one() {
    converges_on_the_later_creation(MergePolicy::Field, MergePolicy::Document);
    converges_on_the_later_creation(MergePolicy::Document, MergePolicy::Field);
}

/// The same through a snapshot page: a member that holds its own creation takes the policy of
/// the later creation the page carries, and a member holding the later one keeps it.
#[test]
fn a_snapshot_page_carries_the_policy_of_the_winning_creation() {
    use crate::SnapshotProgress;
    let pull = |from: &Engine, into: &Engine| {
        let mut progress =
            SnapshotProgress::of_collection(kimmy_core::CollectionId::derive("app", "c"));
        while !progress.is_complete() {
            let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
            into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
        }
    };
    for (first, second) in
        [(MergePolicy::Field, MergePolicy::Document), (MergePolicy::Document, MergePolicy::Field)]
    {
        let (x, _dx) = open();
        let (y, _dy) = open();
        x.create_collection_with_policy("app", "c", first, None).unwrap();
        later();
        y.create_collection_with_policy("app", "c", second, None).unwrap();
        pull(&y, &x);
        assert_eq!(policy_of(&x), second, "adopted from the page");
        pull(&x, &y);
        assert_eq!(policy_of(&y), second, "kept against the page of the earlier creation");
        // and a member that took the policy by a page serves it onward
        let (w, _dw) = open();
        pull(&x, &w);
        assert_eq!(policy_of(&w), second);
    }
}
