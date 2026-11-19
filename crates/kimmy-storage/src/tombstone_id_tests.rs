//! A tombstone of a `field` collection keeps the `_id` of its document, and exactly that: the merge state keeps none
//! (the join drops the cells of a dead life), and a member that is to name the document in an entry of its own must be
//! able to, whichever route its state came by.

use bson::doc;
use kimmy_core::{DocId, DocRecord, Hlc, MergePolicy, OplogEntry};
use redb::ReadableDatabase;

use crate::{CollectionMeta, Engine, SnapshotProgress, WalkScope, tables};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(20));
}

fn field_pair() -> (Engine, Engine, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let coll = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    (a, b, coll, [da, db])
}

/// The stored record of document `id`.
fn stored(e: &Engine, coll: &CollectionMeta, id: &DocId) -> DocRecord {
    let key = crate::docs::doc_key(id).unwrap();
    let txn = e.db().begin_read().unwrap();
    let docs = txn.open_table(tables::DOCS).unwrap();
    let raw = docs.get((coll.id.0, key.as_slice())).unwrap().expect("a row").value().to_vec();
    crate::codec::decode_doc_record_with_meta(&raw).unwrap()
}

/// What the tombstone body is: exactly `{_id: id}`.
fn is_exactly_the_id(record: &DocRecord, id: &DocId) -> bool {
    let body: bson::Document = bson::deserialize_from_slice(&record.body).unwrap_or_default();
    record.deleted && body.len() == 1 && record.tombstone_id().as_ref() == Some(id)
}

fn the_page_id(from: &Engine, coll: &CollectionMeta) -> Option<DocId> {
    let page = from.snapshot_page(None, Some(coll.id)).unwrap();
    assert_eq!(page.deleted_documents.len(), 1, "premise: one tombstone on the page");
    page.deleted_documents[0].id.clone()
}

#[test]
fn a_local_delete_of_a_field_document_leaves_a_tombstone_that_keeps_exactly_the_id() {
    let (a, _b, coll, _d) = field_pair();
    let id = a.insert(&coll, doc! { "_id": 1, "x": 1, "y": "keep me out" }).unwrap();

    assert!(a.delete(&coll, &id).unwrap());

    assert!(
        is_exactly_the_id(&stored(&a, &coll, &id), &id),
        "the body is {{_id}} and nothing of the document"
    );
}

#[test]
fn a_replicated_delete_leaves_the_same_tombstone() {
    let (a, b, coll, _d) = field_pair();
    let id = a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    later();
    a.delete(&coll, &id).unwrap();

    b.apply_batch(&everything(&a)).unwrap();

    let cb = b.get_collection("shop", "orders").unwrap();
    assert!(is_exactly_the_id(&stored(&b, &cb, &id), &id), "the apply of a delete keeps the id");
}

#[test]
fn a_tombstone_of_a_document_collection_keeps_an_empty_body() {
    let (a, _da) = engine();
    let plain =
        a.create_collection_with_policy("shop", "plain", MergePolicy::Document, None).unwrap();
    let id = a.insert(&plain, doc! { "_id": 1, "x": 1 }).unwrap();

    a.delete(&plain, &id).unwrap();

    let record = stored(&a, &plain, &id);
    assert!(
        record.deleted && record.body.is_empty(),
        "a document collection's tombstone is as it always was"
    );
    assert_eq!(record.tombstone_id(), None);
}

#[test]
fn a_tombstone_keeps_its_id_across_a_snapshot_page_to_a_member_that_never_held_the_document() {
    let (a, _b, coll, _d) = field_pair();
    let id = a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    later();
    a.delete(&coll, &id).unwrap();
    assert_eq!(
        the_page_id(&a, &coll),
        Some(id.clone()),
        "the sender fills the page row from the stored body"
    );
    let (c, _dc) = engine();
    let page = a.snapshot_page(None, None).unwrap();

    c.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

    let cc = c.get_collection("shop", "orders").unwrap();
    assert!(
        is_exactly_the_id(&stored(&c, &cc, &id), &id),
        "the receiver's stored tombstone keeps it"
    );
    assert_eq!(the_page_id(&c, &cc), Some(id), "and serves it onward");
}

#[test]
fn a_tombstone_keeps_its_id_across_a_backup_and_a_restore() {
    let (a, _b, coll, _d) = field_pair();
    let id = a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    later();
    a.delete(&coll, &id).unwrap();
    let mut backup = Vec::new();
    a.backup_to(&mut backup, WalkScope::Request).unwrap();

    let dest = tempfile::tempdir().unwrap();
    let path = dest.path().join("restored.redb");
    crate::backup::restore(&path, &mut backup.as_slice()).unwrap();
    let restored = Engine::open(&path).unwrap();

    let rc = restored.get_collection("shop", "orders").unwrap();
    assert!(is_exactly_the_id(&stored(&restored, &rc, &id), &id), "the record is copied whole");
}

#[test]
fn a_rewind_that_deletes_a_live_document_leaves_a_tombstone_of_exactly_the_id() {
    let (a, _b, coll, _d) = field_pair();
    let id = a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    later();
    a.delete(&coll, &id).unwrap();
    later();
    let target = everything(&a).iter().map(|e| e.stamp.hlc).max().unwrap();
    later();
    a.insert(&coll, doc! { "_id": 1, "x": 2, "y": "a live document, with a body" }).unwrap();
    assert!(!stored(&a, &coll, &id).deleted, "premise: the document is live again");

    a.rewind_to(target).unwrap();

    let record = stored(&a, &coll, &id);
    assert!(
        is_exactly_the_id(&record, &id),
        "the document the rewind deleted left a tombstone of {{_id}} and no body"
    );
}

#[test]
fn a_loss_entry_applied_to_a_tombstone_row_keeps_the_tombstone_body() {
    use kimmy_core::merge_state::{
        LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, WriterId,
    };
    let (a, b, coll, _d) = field_pair();
    let id = a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    later();
    a.delete(&coll, &id).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    let writer = WriterId { node: a.node_id(), epoch: a.epoch() };
    let key = LossKey::field(
        LossKind::DeleteWon,
        Path::flat("x").unwrap(),
        MStamp { hlc: Hlc::new(crate::physical_now_ms(), 0), writer },
    );
    let entry = OplogEntry {
        stamp: kimmy_core::Stamp::new(
            Hlc::new(crate::physical_now_ms() + 1, 0),
            a.node_id(),
            a.epoch(),
        ),
        kind: kimmy_core::OpKind::LossRecord,
        collection: coll.id,
        doc_id: Some(id.clone()),
        body: Some(MergeState::encode_loss_record(&key, &LossValue::Value(Value::Int64(1)))),
        meta: None,
    };

    b.apply_batch(std::slice::from_ref(&entry)).unwrap();

    let record = stored(&b, &cb, &id);
    assert!(
        is_exactly_the_id(&record, &id),
        "the row the record was applied to is still a tombstone of {{_id}}"
    );
    let state = crate::field_write::load(record.meta.as_deref()).unwrap();
    assert!(state.losses.contains_key(&key), "premise: the record is in the tombstone's state");
}

/// A spec that matches every document and removes it.
struct Remove;

impl crate::ModifySpec for Remove {
    fn matches(&self, _: &bson::Document) -> kimmy_core::Result<bool> {
        Ok(true)
    }
    fn compare(&self, _: &bson::Document, _: &bson::Document) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
    fn apply(&self, _: &bson::Document) -> Result<Option<bson::Document>, String> {
        Ok(None)
    }
    fn upsert(&self) -> Option<Result<bson::Document, String>> {
        None
    }
}

#[test]
fn a_removal_through_a_filtered_write_leaves_a_tombstone_that_keeps_exactly_the_id() {
    let (a, _b, coll, _d) = field_pair();
    let id = a.insert(&coll, doc! { "_id": 1, "x": 1, "y": "keep me out" }).unwrap();

    let out = a.modify_where(&coll, &crate::Candidates::Scan, &Remove, None).unwrap();

    assert_eq!((out.matched, out.modified), (1, 1), "premise");
    assert!(
        is_exactly_the_id(&stored(&a, &coll, &id), &id),
        "the removal's tombstone is {{_id}} and nothing of the document"
    );
}
