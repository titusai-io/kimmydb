//! Step 3, package 3.0 (U1): a snapshot page that carries a document of a `field` collection whose clock is
//! past the bound stops the snapshot at that page, never skips the document. The page is one transaction, so
//! it is rolled back whole; nothing on it is applied, the snapshot does not move and no coverage is granted;
//! the answer is `Ok` (the peer is not at fault), and the same page applies once the document is due.

use bson::doc;
use kimmy_core::{DocId, MergePolicy};

use crate::{ClockRefusal, Engine, SnapshotProgress, WalkScope};

const SKEW_MS: i64 = 300_000;

/// The page with the creation of each collection it defines made long ago. The receiver's clock is moved back so
/// that the documents are past the bound, and a creation made just now would be past it too: a schema stamp past the
/// bound stops a page before its documents are read (the frame check), and these tests are about the documents.
fn with_honest_definitions(mut page: crate::SnapshotPage) -> crate::SnapshotPage {
    for state in &mut page.collections {
        let long_ago = kimmy_core::Hlc::new(crate::physical_now_ms() - 3_600_000, 0);
        state.created = long_ago;
        state.merge_policy_at = state.merge_policy_at.map(|_| long_ago);
    }
    page
}

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

#[test]
fn a_far_future_state_by_snapshot_is_refused_and_the_page_rolls_back() {
    let (a, _da) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    for i in 0..3i64 {
        a.insert(&ca, doc! { "_id": i, "n": i }).unwrap();
    }
    let page = with_honest_definitions(a.snapshot_page(None, None).unwrap());
    assert_eq!(page.documents.len(), 3, "premise: the page carries the three documents");

    let (b, _db) = engine();
    b.set_clock_offset_for_tests(-(SKEW_MS + 10_000));
    let mut progress = SnapshotProgress::whole_database();
    let applied = b
        .apply_snapshot_page(a.node_id(), &mut progress, &page)
        .expect("state refused: vv: the page was answered with an error, not Ok(clock_refused)");

    let stop = applied.clock_refused.as_ref().expect("state refused: vv, but nothing was refused");
    assert_eq!(stop.reason, ClockRefusal::Vv, "state refused: vv");
    assert!(!progress.is_complete(), "the snapshot did not complete: a stop, not a skip");
    let cb = b.get_collection("shop", "orders").expect("the definitions are applied first");
    assert_eq!(
        b.count(&cb, WalkScope::Request).unwrap(),
        0,
        "the page rolled back whole: B holds none of its documents"
    );
    assert_eq!(b.held_len().unwrap(), 0, "and no held mark");
    assert_eq!(b.clock_refusals().len(), 1, "counted once, by origin");

    // Due: the same page, under the same progress, applies.
    b.set_clock_offset_for_tests(0);
    let applied = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert!(applied.clock_refused.is_none(), "{applied:?}");
    assert_eq!(b.count(&cb, WalkScope::Request).unwrap(), 3, "applied once due");
    assert!(progress.is_complete());
}

/// A carried delete past the bound is not taken, and comes by entries once it is due.
///
/// It is no longer a stop of the page with the state's reason (`Vv`), as a far-future page document is: the carried
/// delete is judged by the sender's coverage before the bound (`apply_carried_document_deletes`), and the page's
/// coverage is clamped to the bound on arrival (the frame check), so a delete past the bound is above the coverage
/// and is ignored, with a warning. U1's refusal of it by the state's version vector is therefore not reachable
/// through a page; it stays covered for an entry (`clock_bound_entry_tests`) and for a page document
/// (`a_far_future_state_by_snapshot_is_refused_and_the_page_rolls_back`). The outcome is as safe as the stop was:
/// nothing is applied or recorded, and the coverage the page grants stops below the delete, so the delete is served
/// again by entries, where it is refused until due.
#[test]
fn a_far_future_carried_delete_of_a_field_document_is_not_taken_and_comes_once_due() {
    let (a, _da) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let x = a.insert(&ca, doc! { "_id": 1 }).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(3));
    a.delete(&ca, &x).unwrap();
    let page = with_honest_definitions(a.snapshot_page(None, None).unwrap());
    assert!(!page.deleted_documents.is_empty(), "premise: the page carries the delete");
    let deleted_at = a.version_vector().unwrap().get(a.origin());

    let (b, _db) = engine();
    b.set_clock_offset_for_tests(-(SKEW_MS + 10_000));
    let mut progress = SnapshotProgress::whole_database();
    let applied = b
        .apply_snapshot_page(a.node_id(), &mut progress, &page)
        .expect("the page is answered, with the delete left");

    assert!(applied.clock_refused.is_none(), "not a stop of the page: {applied:?}");
    let cb = b.get_collection("shop", "orders").unwrap();
    assert_eq!(
        crate::docs::record_stamp(&b, &cb, &DocId::Int64(1)).unwrap(),
        None,
        "nothing was recorded for the document"
    );
    assert!(
        b.witnessed_vector().unwrap().get(a.origin()) < deleted_at,
        "the coverage stops at the clamp, below the delete, so the entry comes again"
    );

    b.set_clock_offset_for_tests(0);
    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    assert!(
        crate::docs::record_stamp(&b, &cb, &DocId::Int64(1)).unwrap().is_some(),
        "once due, the same page applies the delete"
    );
}

/// The audit of every route that writes a document record (and so a tombstone or a merge state) is in
/// `q7-step3-u1-plan.md`; this is its tripwire. A file of production code that builds a `DocRecord` is a route
/// a stamp or a state can enter by, and U1 holds only the routes it names to the bound. A new one fails here
/// until it is placed in the table (through the choke point, or
/// shown to carry no remote stamp or state) and added below.
#[test]
fn every_file_that_writes_a_document_record_is_a_route_the_audit_names() {
    let known: std::collections::BTreeSet<&str> = [
        "docs.rs", // local writes, the document policy's apply, and the field apply behind the choke point
        "late_drop.rs", // a drop's bare tombstones, stamped by the (collection-level) drop entry
        "modify.rs", // a local update's write
        "rewind.rs", // a local rewind of this member's own records
        "snapshot.rs", // the bare tombstone (document policy only: a field tombstone is joined behind the choke point)
    ]
    .into_iter()
    .collect();
    let mut found = std::collections::BTreeSet::new();
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if !name.ends_with(".rs") || name.ends_with("_tests.rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let production = text.split("#[cfg(test)]\nmod ").next().unwrap();
        if production.contains("DocRecord::tombstone(") || production.contains("DocRecord::live(") {
            found.insert(name);
        }
    }
    let found: std::collections::BTreeSet<&str> = found.iter().map(String::as_str).collect();
    assert_eq!(
        found, known,
        "a file that writes a document record is not in the audit of U1's routes: place it in \
         q7-step3-u1-plan.md and here"
    );
}
