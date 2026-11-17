//! Step 3, item (3): a snapshot page document of a `field` collection is applied as state only: the
//! merged state joins, the indexes and the digest move, and the document has no oplog entry on the
//! member that took it by snapshot.

use bson::doc;
use kimmy_core::{DocId, Hlc, MergePolicy};

use crate::{Engine, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

/// A whole-database snapshot of `from`, applied to `into`, page by page.
fn take_a_snapshot(into: &Engine, from: &Engine) {
    let mut progress = SnapshotProgress::whole_database();
    let mut after = None;
    for _ in 0..50 {
        let page = from.snapshot_page(after.take(), None).unwrap();
        into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
        after = page.next.clone();
        if after.is_none() {
            return;
        }
    }
    panic!("the snapshot did not end");
}

/// A member with nothing takes a whole-database snapshot of a member that holds a field document
/// written in two steps. It reads the merged document with the donor's version identity and digest,
/// and holds no entry for the document's writes: it was handed state, not history.
#[test]
fn a_member_that_takes_a_field_document_by_snapshot_holds_its_state_and_no_entries() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let x = a.insert(&ca, doc! { "_id": 1 }).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    a.replace(&ca, &x, doc! { "_id": 1, "a": 1 }, false).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    a.replace(&ca, &x, doc! { "_id": 1, "a": 1, "b": 2 }, false).unwrap();
    let document_entries: Vec<_> = a
        .entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.kind.is_document())
        .collect();
    assert_eq!(document_entries.len(), 3, "premise: an insert and two replaces");

    take_a_snapshot(&b, &a);

    let cb = b.get_collection("shop", "orders").unwrap();
    let (vb, db) = b.get_versioned(&cb, &x).unwrap().expect("B holds the document by snapshot");
    let (va, _) = a.get_versioned(&ca, &x).unwrap().unwrap();
    assert_eq!(db, doc! { "_id": 1, "a": 1, "b": 2 });
    assert_eq!(vb, va, "the donor's version identity: the stamp and the hash of the merged state");
    assert_eq!(
        b.digest_row(ca.id).unwrap().expect("B's digest row").digest,
        a.digest_row(ca.id).unwrap().expect("A's digest row").digest,
        "the digest folds the same state"
    );
    for e in &document_entries {
        assert!(
            b.oplog_entry(&e.stamp).unwrap().is_none(),
            "no entry for {:?}: state, not history",
            e.stamp
        );
    }
    assert_eq!(
        b.held_len().unwrap(),
        0,
        "no held mark for X's stamp {:?}: state, not history (the one document is the only thing B took)",
        document_entries.last().map(|e| e.stamp)
    );
    assert_eq!(b.field_replication_refusals(), 0, "nothing was refused");
    let _ = DocId::Int64(1);
}
