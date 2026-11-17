//! Step 3, item (3.2): a delete a snapshot page carries for a document of a `field` collection travels with
//! the state the tombstone keeps (the delete and its context), so a receiver joins it instead of comparing
//! stamps. A tombstone of any other policy is only a stamp.

use bson::{Document, doc};
use kimmy_core::MergePolicy;

use crate::{Engine, SnapshotPage};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn wide(i: i64) -> Document {
    let mut d = doc! { "_id": i };
    for f in 0..24 {
        d.insert(format!("field_{f}"), i * 100 + f);
    }
    d
}

#[test]
fn a_tombstone_of_a_field_document_travels_with_the_state_it_keeps() {
    let (a, _da) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let x = a.insert(&ca, doc! { "_id": 1, "a": 1 }).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(3));
    a.delete(&ca, &x).unwrap();
    let page = a.snapshot_page(None, None).unwrap();
    assert_eq!(page.deleted_documents.len(), 1, "premise: the page carries the delete");
    let meta =
        page.deleted_documents[0].meta.as_deref().expect("a field tombstone carries its state");
    assert!(!meta.is_empty());
    assert!(
        crate::field_write::load(Some(meta)).is_ok(),
        "the carried state is a merge state a receiver can join"
    );
}

#[test]
fn a_tombstone_of_a_document_policy_collection_is_only_a_stamp() {
    let (a, _da) = engine();
    let ca = a.create_collection_with_policy("shop", "plain", MergePolicy::Document, None).unwrap();
    let x = a.insert(&ca, doc! { "_id": 1, "a": 1 }).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(3));
    a.delete(&ca, &x).unwrap();
    let page = a.snapshot_page(None, None).unwrap();
    assert_eq!(page.deleted_documents.len(), 1, "premise: the page carries the delete");
    assert_eq!(page.deleted_documents[0].meta, None);
}

/// What a page's tombstones cost its room, the state they carry included.
fn tombstone_bytes(page: &SnapshotPage) -> usize {
    page.deleted_documents.iter().map(tombstone_bytes_of).sum()
}

fn tombstone_bytes_of(t: &crate::SnapshotTombstone) -> usize {
    t.key.len() + t.meta.as_ref().map_or(0, Vec::len) + crate::snapshot::TOMBSTONE_OVERHEAD
}

/// A page is cut by what its rows cost, and a tombstone's state is part of what it costs: a page of
/// tombstones that carry their state stays inside its room, and every tombstone still arrives.
#[test]
fn the_state_a_tombstone_carries_counts_against_the_room_of_its_page() {
    let (a, _da) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    for i in 0..4i64 {
        let id = a.insert(&ca, wide(i)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        a.delete(&ca, &id).unwrap();
    }
    // The cost of each tombstone as the page counts it, read off pages of the default room.
    let mut costs = Vec::new();
    let mut after = None;
    loop {
        let page = a.snapshot_page(after.take(), Some(ca.id)).unwrap();
        costs.extend(page.deleted_documents.iter().map(tombstone_bytes_of));
        after = page.next.clone();
        if after.is_none() {
            break;
        }
    }
    assert_eq!(costs.len(), 4, "premise: four tombstones");
    let (least, most) = (*costs.iter().min().unwrap(), *costs.iter().max().unwrap());
    assert!(
        least > 400 && 2 * least > most + 50,
        "premise: the state is most of the cost: {costs:?}"
    );

    // Room for any one tombstone with its cursor, not for two.
    let room = most + crate::snapshot::cursor_reserve(16, 0) + 50;
    a.set_snapshot_room_for_test(Some(room));
    let mut seen = 0usize;
    let mut pages = 0usize;
    let mut after = None;
    loop {
        let page = a.snapshot_page(after.take(), Some(ca.id)).unwrap();
        assert!(
            tombstone_bytes(&page) <= room,
            "a page holds {} B of tombstones in {room}",
            tombstone_bytes(&page)
        );
        seen += page.deleted_documents.len();
        pages += 1;
        after = page.next.clone();
        if after.is_none() || pages > 20 {
            break;
        }
    }
    assert_eq!(seen, 4, "every tombstone arrives, once");
    assert!(pages >= 4, "one tombstone to a page here: {pages} pages");
}
