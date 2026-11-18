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
    let _ = DocId::Int64(1);
}

/// A page document of a `field` collection that carries no merge state has nothing to join: a malformed page,
/// refused as corrupt and named, not as a capability that does not exist.
#[test]
fn a_page_document_of_a_field_collection_with_no_merge_state_is_refused_as_corrupt() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let mut page = a.snapshot_page(None, None).unwrap();
    assert!(!page.documents.is_empty(), "premise: the page carries the document");
    for document in &mut page.documents {
        document.meta = None;
    }
    let refused = b
        .apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .expect_err("a page document with no merge state is refused");
    assert!(
        matches!(
            &refused,
            crate::StorageError::Corrupt(text)
                if text.contains("a snapshot document of a collection with merge policy field carries no merge state")
        ),
        "refused as corrupt, by the page document's own name (a record's load says the same of a record, not of a \
         page document): {refused:?}"
    );
}

/// The same snapshot taken in several pages. A snapshot grants its coverage, and with it releases
/// any held mark the pages left, only when its LAST page applies, so a mark written for a document
/// by an earlier page would sit there for as long as the snapshot is in flight (or for good, if it
/// is interrupted) and be gone by the time a one-page snapshot has been read back. The rule is
/// therefore asserted after EVERY page: no held mark and no oplog entry for the documents applied
/// so far, which are state, not history.
#[test]
fn a_snapshot_of_several_pages_leaves_no_held_mark_and_no_entry_after_any_page() {
    let (a, _da) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    for i in 0..6i64 {
        a.insert(&ca, doc! { "_id": i, "pad": "x".repeat(200) }).unwrap();
    }
    // The size of a document's page row is measured, not guessed: the smallest budget (in steps)
    // whose snapshot completes, takes at least three pages, and carries documents before its last.
    let (bytes, pages) = (0..64usize)
        .find_map(|step| {
            let bytes = 500 + step * 250;
            a.set_snapshot_page_bytes_for_test(Some(bytes));
            let mut pages = Vec::new();
            let mut after = None;
            loop {
                let page = a.snapshot_page(after.take(), None).ok()?;
                after = page.next.clone();
                pages.push(page);
                if after.is_none() {
                    break;
                }
                if pages.len() > 50 {
                    return None;
                }
            }
            let early_rows = pages[..pages.len() - 1].iter().any(|p| !p.documents.is_empty());
            (pages.len() >= 3 && early_rows).then_some((bytes, pages))
        })
        .expect("a page budget that splits six documents into three pages or more");

    let (b, _db) = engine();
    let mut progress = SnapshotProgress::whole_database();
    let mut applied = 0usize;
    let mut documents_before_the_end = false;
    for (n, page) in pages.iter().enumerate() {
        b.apply_snapshot_page(a.node_id(), &mut progress, page).unwrap();
        applied += page.documents.len();
        documents_before_the_end |= page.next.is_some() && !page.documents.is_empty();
        assert_eq!(
            b.held_len().unwrap(),
            0,
            "no held mark for the {applied} documents applied so far, after page {n} of {} \
             ({bytes} B pages): state, not history",
            pages.len()
        );
        let entries = b
            .entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
            .unwrap()
            .entries
            .into_iter()
            .filter(|e| e.kind.is_document())
            .count();
        assert_eq!(
            entries, 0,
            "no entry for the {applied} documents applied so far, after page {n}: state, not history"
        );
        let held = b
            .get_collection("shop", "orders")
            .ok()
            .map(|c| usize::try_from(b.count(&c, WalkScope::Request).unwrap()).unwrap());
        assert_eq!(held.unwrap_or(0), applied, "the documents of the pages so far are held");
    }
    assert!(progress.is_complete(), "the snapshot ended");
    assert!(
        documents_before_the_end,
        "premise: a page before the last carried documents, so a one-page run cannot pass"
    );
    assert_eq!(applied, 6, "premise: every document travelled");
    let cb = b.get_collection("shop", "orders").unwrap();
    for i in 0..6i64 {
        let id = DocId::Int64(i);
        let (vb, db) = b.get_versioned(&cb, &id).unwrap().expect("B holds the document");
        let (va, da) = a.get_versioned(&ca, &id).unwrap().unwrap();
        assert_eq!(db, da, "the same document");
        assert_eq!(vb, va, "the donor's version identity");
    }
}
