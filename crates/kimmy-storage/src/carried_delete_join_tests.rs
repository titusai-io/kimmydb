//! Step 3, item (3.2): the delete a snapshot page carries for a document of a `field` collection is
//! joined, with the state its tombstone keeps, into what the receiver holds. The join, not the stamp,
//! decides what the delete kills, and a receiver that held nothing keeps the delete's context.

use bson::doc;
use kimmy_core::{Hlc, MergePolicy, OplogEntry};

use crate::{Engine, SnapshotProgress, StorageError};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap().entries
}

/// The entries of `from` that `into` has not applied yet, by stamp.
fn entries_after(from: &Engine, last: &OplogEntry) -> Vec<OplogEntry> {
    everything(from).into_iter().filter(|e| e.stamp > last.stamp).collect()
}

/// A donor and a member that both hold document X of a field collection (A's insert, applied by M),
/// and then: A deletes X; M, which has not seen the delete, writes X.b above the delete's stamp.
struct Diverged {
    a: Engine,
    m: Engine,
    x: kimmy_core::DocId,
    delete: OplogEntry,
    write: OplogEntry,
    creation: Vec<OplogEntry>,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

fn diverged() -> Diverged {
    let (a, da) = engine();
    let (m, dm) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let creation = everything(&a);
    m.apply_batch(&creation).unwrap();
    later();
    let x = a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let insert = entries_after(&a, creation.last().unwrap());
    m.apply_batch(&insert).unwrap();
    let cm = m.get_collection("shop", "orders").unwrap();
    assert!(m.get(&cm, &x).unwrap().is_some(), "premise: M holds X");
    later();
    a.delete(&ca, &x).unwrap();
    let delete = everything(&a).last().unwrap().clone();
    later();
    m.replace(&cm, &x, doc! { "_id": 1, "b": 2 }, false).unwrap();
    let write = everything(&m).last().unwrap().clone();
    assert!(write.stamp > delete.stamp, "premise: M's write is stamped above A's delete");
    Diverged { a, m, x, delete, write, creation, _dirs: (da, dm) }
}

fn take_the_page(into: &Engine, from: &Engine) -> Result<crate::SnapshotApplied, StorageError> {
    let page = from.snapshot_page(None, None).unwrap();
    assert!(!page.deleted_documents.is_empty(), "premise: the page carries the delete");
    into.apply_snapshot_page(from.node_id(), &mut SnapshotProgress::whole_database(), &page)
}

/// The delete of a page is joined, not compared by stamp: M's write is stamped above the delete, but it
/// is a write into the life the delete killed, which the delete's context does not name.
#[test]
fn a_carried_delete_kills_the_life_that_did_not_see_it_over_a_page() {
    let d = diverged();
    take_the_page(&d.m, &d.a).unwrap();
    let cm = d.m.get_collection("shop", "orders").unwrap();
    assert_eq!(
        d.m.get(&cm, &d.x).unwrap(),
        None,
        "the delete kills the life that did not see it, below the record's stamp"
    );
}

/// A member that holds nothing of X takes the delete by page and keeps its context: a write into the
/// life the delete killed, arriving after, does not bring X back.
#[test]
fn a_carried_delete_with_nothing_held_keeps_its_context() {
    let d = diverged();
    let (b, _db) = engine();
    b.apply_batch(&d.creation).unwrap();
    take_the_page(&b, &d.a).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    assert_eq!(b.get(&cb, &d.x).unwrap(), None, "premise: X is deleted on B");
    b.apply_batch(std::slice::from_ref(&d.write))
        .expect("the write meets a tombstone that kept the delete's context");
    assert_eq!(b.get(&cb, &d.x).unwrap(), None, "a write into the killed life is not a new life");
}

/// A tombstone of a field collection with no state is not a valid row.
#[test]
fn a_carried_delete_of_a_field_collection_with_no_state_is_refused_by_name() {
    let d = diverged();
    let mut page = d.a.snapshot_page(None, None).unwrap();
    assert!(!page.deleted_documents.is_empty(), "premise: the page carries the delete");
    for tombstone in &mut page.deleted_documents {
        tombstone.meta = None;
    }
    let result =
        d.m.apply_snapshot_page(d.a.node_id(), &mut SnapshotProgress::whole_database(), &page);
    match result {
        Err(StorageError::Corrupt(why)) => {
            assert!(why.contains("carries no state"), "{why}");
        }
        other => {
            panic!("a field tombstone with no state is refused by name as corrupt, not {other:?}")
        }
    }
}

/// The delete is state, not history: it writes no oplog entry for the delete and leaves no held mark.
#[test]
fn a_carried_delete_writes_no_oplog_entry_and_leaves_no_held_mark() {
    let d = diverged();
    take_the_page(&d.m, &d.a).unwrap();
    assert!(
        d.m.oplog_entry(&d.delete.stamp).unwrap().is_none(),
        "no entry for the delete {:?}: state, not history",
        d.delete.stamp
    );
    assert_eq!(d.m.held_len().unwrap(), 0, "no held mark");
}
