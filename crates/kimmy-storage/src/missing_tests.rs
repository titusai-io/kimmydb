//! The missing-through vector of a `field` document (`missing.rs`): raised where an entry stops being held
//! (retention) or a state is installed as state (a page), removed with the tombstone and with the collection, and
//! carried by a backup.

use bson::doc;
use kimmy_core::{DocId, Hlc, MergePolicy, OpKind, OplogEntry};
use redb::{ReadableDatabase, ReadableTableMetadata};

use crate::{Engine, RetentionPolicy, SnapshotProgress, WalkScope, physical_now_ms, tables};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn entries(engine: &Engine) -> Vec<OplogEntry> {
    engine.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn rows(engine: &Engine) -> u64 {
    let txn = engine.db().begin_read().unwrap();
    txn.open_table(tables::MISSING_THROUGH).unwrap().len().unwrap()
}

/// Three documents, the first two collected by a pass that keeps the newest entry (the third's).
fn three_documents(policy: MergePolicy) -> (Engine, tempfile::TempDir, crate::CollectionMeta) {
    let (a, dir) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", policy, None).unwrap();
    for id in 1..=3i64 {
        a.insert(&ca, doc! { "_id": id }).unwrap();
    }
    (a, dir, ca)
}

fn collect_entries(engine: &Engine) {
    engine
        .collect_garbage_at(physical_now_ms() + 1_000, RetentionPolicy::new(0, u64::MAX))
        .unwrap();
}

fn insert_entry(engine: &Engine, id: i64) -> OplogEntry {
    entries(engine)
        .into_iter()
        .find(|entry| entry.kind == OpKind::Insert && entry.doc_id == Some(DocId::Int64(id)))
        .expect("the insert")
}

#[test]
fn collecting_an_entry_of_a_field_document_records_what_was_collected() {
    let (a, _da, ca) = three_documents(MergePolicy::Field);
    let first = insert_entry(&a, 1);
    let third = insert_entry(&a, 3);

    collect_entries(&a);

    assert_eq!(
        a.missing_through(&ca, &DocId::Int64(1)).unwrap().get(first.stamp.origin()),
        first.stamp.hlc,
        "the collected write of the writer, by its stamp"
    );
    assert!(
        a.missing_through(&ca, &DocId::Int64(3)).unwrap().is_empty(),
        "the newest entry is never collected: {third:?}"
    );
    assert_eq!(rows(&a), 2, "one row for each of the two documents whose entry went");
}

#[test]
fn collecting_an_entry_of_a_document_collection_records_nothing() {
    let (a, _da, ca) = three_documents(MergePolicy::Document);

    collect_entries(&a);

    assert!(a.missing_through(&ca, &DocId::Int64(1)).unwrap().is_empty());
    assert_eq!(rows(&a), 0, "a document collection's entries carry no state to record");
}

#[test]
fn a_row_holds_the_newest_collected_stamp_of_each_writer() {
    let (a, _da, ca) = three_documents(MergePolicy::Field);
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "n": 2 }, false).unwrap();
    a.insert(&ca, doc! { "_id": 4 }).unwrap();
    let newest_write = entries(&a)
        .into_iter()
        .filter(|entry| entry.doc_id == Some(DocId::Int64(1)))
        .map(|entry| entry.stamp.hlc)
        .max()
        .unwrap();

    collect_entries(&a);

    assert_eq!(
        a.missing_through(&ca, &DocId::Int64(1)).unwrap().get(a.origin()),
        newest_write,
        "the later of the document's two collected writes"
    );
}

#[test]
fn a_state_installed_as_state_records_every_write_it_has_seen() {
    let (a, _da, ca) = three_documents(MergePolicy::Field);
    let (b, _db) = engine();
    let page = a.snapshot_page(None, None).unwrap();

    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

    let cb = b.get_collection("shop", "orders").unwrap();
    let held = crate::docs::record_stamp(&a, &ca, &DocId::Int64(2)).unwrap().expect("A's record");
    assert_eq!(
        b.missing_through(&cb, &DocId::Int64(2)).unwrap().get(a.origin()),
        held.hlc,
        "B holds A's write to the document as state alone"
    );
}

#[test]
fn an_entry_applied_as_an_entry_records_nothing() {
    let (a, _da, _ca) = three_documents(MergePolicy::Field);
    let (b, _db) = engine();

    b.apply_batch(&entries(&a)).unwrap();

    assert_eq!(rows(&b), 0, "B holds every entry, so nothing of it is missing");
}

#[test]
fn collecting_the_tombstone_removes_the_row() {
    let (a, _da, ca) = three_documents(MergePolicy::Field);
    a.delete(&ca, &DocId::Int64(1)).unwrap();
    a.insert(&ca, doc! { "_id": 4 }).unwrap();
    collect_entries(&a);
    assert!(!a.missing_through(&ca, &DocId::Int64(1)).unwrap().is_empty(), "premise: a row");

    a.collect_garbage_at(physical_now_ms() + 1_000, RetentionPolicy::new(0, 0)).unwrap();

    assert!(a.get(&ca, &DocId::Int64(1)).unwrap().is_none());
    assert!(
        a.missing_through(&ca, &DocId::Int64(1)).unwrap().is_empty(),
        "the document is gone for good, and so is what was recorded of it"
    );
}

#[test]
fn dropping_the_collection_removes_its_rows() {
    let (a, _da, _ca) = three_documents(MergePolicy::Field);
    collect_entries(&a);
    assert_eq!(rows(&a), 2, "premise");

    a.drop_collection("shop", "orders").unwrap();
    a.finish_purges_now().unwrap();

    assert_eq!(rows(&a), 0, "the purger removed them with the documents");
}

#[test]
fn a_backup_carries_the_rows() {
    let (a, _da, ca) = three_documents(MergePolicy::Field);
    collect_entries(&a);
    let mut backup = Vec::new();
    a.backup_to(&mut backup, WalkScope::Request).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");

    crate::backup::restore(&path, &mut backup.as_slice()).unwrap();
    let restored = Engine::open(&path).unwrap();

    let cr = restored.get_collection("shop", "orders").unwrap();
    assert_eq!(
        restored.missing_through(&cr, &DocId::Int64(1)).unwrap(),
        a.missing_through(&ca, &DocId::Int64(1)).unwrap(),
        "a restored member does not forget what its oplog had collected"
    );
    assert_eq!(rows(&restored), 2);
}
