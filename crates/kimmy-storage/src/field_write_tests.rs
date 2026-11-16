//! The local writes of a `field` collection (Q7 Step 2, W4): the record carries the merge state,
//! the oplog entry carries the same state, and a `document` collection is as it was.

use bson::{Document, doc};
use kimmy_core::merge_state::MergeState;
use kimmy_core::{DocId, Hlc, MergePolicy, OpKind, OplogEntry};
use redb::ReadableDatabase;

use crate::meta::CollectionMeta;
use crate::{Engine, WalkScope};

fn open() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn pair(e: &Engine) -> (CollectionMeta, CollectionMeta) {
    (
        e.create_collection_with_policy("app", "f", MergePolicy::Field, None).unwrap(),
        e.create_collection_with_policy("app", "d", MergePolicy::Document, None).unwrap(),
    )
}

fn raw(e: &Engine, c: &CollectionMeta, id: i64) -> Option<Vec<u8>> {
    let key = crate::docs::doc_key(&DocId::Int64(id)).unwrap();
    let txn = e.db().begin_read().unwrap();
    let t = txn.open_table(crate::tables::DOCS).unwrap();
    t.get((c.id.0, key.as_slice())).unwrap().map(|v| v.value().to_vec())
}

fn entries(e: &Engine, c: &CollectionMeta) -> Vec<OplogEntry> {
    e.read_oplog_from(Hlc::ZERO, 1000, WalkScope::Background)
        .unwrap()
        .into_iter()
        .filter(|x| x.collection == c.id && x.kind.is_document())
        .collect()
}

fn bytes(d: &Document) -> Vec<u8> {
    bson::serialize_to_vec(d).unwrap()
}

#[test]
fn a_field_insert_stores_the_state_with_the_record_and_the_entry() {
    let (e, _d) = open();
    let (f, _) = pair(&e);
    let d = doc! { "_id": 1_i64, "z": 1, "a": { "y": 2, "b": 3 } };
    e.insert(&f, d.clone()).unwrap();
    let rec = crate::codec::decode_doc_record_with_meta(&raw(&e, &f, 1).unwrap()).unwrap();
    assert_eq!(rec.body, bytes(&d));
    let meta = rec.meta.clone().expect("a field record carries its state");
    let state = MergeState::decode_compact(&meta).unwrap();
    assert_eq!(state.vv.values().copied().max(), Some(rec.stamp.hlc), "record.stamp == max(vv)");
    assert!(state.losses.is_empty());
    let es = entries(&e, &f);
    assert_eq!(es.len(), 1);
    assert_eq!((es[0].kind, es[0].meta.as_deref()), (OpKind::Insert, Some(meta.as_slice())));
    assert_eq!(es[0].stamp, rec.stamp);
    assert_eq!(es[0].body.as_deref(), Some(rec.body.as_slice()));
    // the body-only reader skips the state
    assert_eq!(crate::codec::decode_doc_record(&raw(&e, &f, 1).unwrap()).unwrap().meta, None);
}

#[test]
fn a_document_collection_is_byte_for_byte_what_it_was() {
    let (e, _d) = open();
    let (_, dc) = pair(&e);
    e.insert(&dc, doc! { "_id": 1_i64, "a": 1 }).unwrap();
    let r = raw(&e, &dc, 1).unwrap();
    assert_eq!(r[0], 1, "version 1");
    assert_eq!(crate::codec::decode_doc_record_with_meta(&r).unwrap().meta, None);
    e.replace(&dc, &DocId::Int64(1), doc! { "_id": 1_i64, "a": 2 }, false).unwrap();
    e.delete(&dc, &DocId::Int64(1)).unwrap();
    assert_eq!(raw(&e, &dc, 1).unwrap()[0], 1);
    assert!(entries(&e, &dc).iter().all(|x| x.meta.is_none()));
}

#[test]
fn an_identical_replace_writes_nothing_a_reorder_replace_stores_the_new_order_and_fields_that_stay_keep_theirs()
 {
    let (e, _d) = open();
    let (f, _) = pair(&e);
    e.insert(&f, doc! { "_id": 1_i64, "a": 1, "b": 2 }).unwrap();
    let before = raw(&e, &f, 1).unwrap();
    let commits = e.commits();
    let out =
        e.replace(&f, &DocId::Int64(1), doc! { "_id": 1_i64, "a": 1, "b": 2 }, false).unwrap();
    assert!(out.matched && !out.modified && out.stamp.is_none(), "{out:?}");
    assert_eq!(raw(&e, &f, 1).unwrap(), before, "the same document changes nothing");
    assert_eq!(e.commits(), commits, "no commit");
    assert_eq!(entries(&e, &f).len(), 1);

    // A reorder-only REPLACE is a write that stores the new order: docs/deviations.md
    // `pipeline-reorder-only-q7` (a reorder-only pipeline update is no write, and its workaround
    // is a replaceOne with the document in the wanted order).
    let out =
        e.replace(&f, &DocId::Int64(1), doc! { "_id": 1_i64, "b": 2, "a": 1 }, false).unwrap();
    assert!(out.matched && out.modified && out.stamp.is_some(), "{out:?}");
    let rec = crate::codec::decode_doc_record_with_meta(&raw(&e, &f, 1).unwrap()).unwrap();
    assert_eq!(rec.body, bytes(&doc! { "_id": 1_i64, "b": 2, "a": 1 }), "the new order is stored");
    assert_eq!(e.commits(), commits + 1, "one commit");
    assert_eq!(entries(&e, &f).len(), 2, "one new entry");
    assert_eq!(entries(&e, &f)[1].stamp, rec.stamp);

    let out =
        e.replace(&f, &DocId::Int64(1), doc! { "_id": 1_i64, "c": 3, "b": 9 }, false).unwrap();
    assert!(out.modified);
    let rec = crate::codec::decode_doc_record_with_meta(&raw(&e, &f, 1).unwrap()).unwrap();
    assert_eq!(
        rec.body,
        bytes(&doc! { "_id": 1_i64, "b": 9, "c": 3 }),
        "b stays, a goes, c is new"
    );
    let state = MergeState::decode_compact(rec.meta.as_deref().unwrap()).unwrap();
    assert_eq!(state.vv.values().copied().max(), Some(rec.stamp.hlc));
    let es = entries(&e, &f);
    assert_eq!(
        (es.len(), es[2].kind, es[2].meta.as_deref()),
        (3, OpKind::Replace, rec.meta.as_deref())
    );
}

#[test]
fn a_field_delete_leaves_a_tombstone_with_the_state_and_an_insert_over_it_is_a_new_life() {
    let (e, _d) = open();
    let (f, _) = pair(&e);
    e.insert(&f, doc! { "_id": 1_i64, "a": 1 }).unwrap();
    assert!(e.delete(&f, &DocId::Int64(1)).unwrap());
    let t = crate::codec::decode_doc_record_with_meta(&raw(&e, &f, 1).unwrap()).unwrap();
    assert!(t.deleted && t.body.is_empty());
    let meta = t.meta.clone().expect("the tombstone keeps the state");
    let es = entries(&e, &f);
    assert_eq!(
        (es[1].kind, es[1].body.as_ref(), es[1].meta.as_deref()),
        (OpKind::Delete, None, Some(meta.as_slice()))
    );
    assert_eq!(
        MergeState::decode_compact(&meta).unwrap().vv.values().copied().max(),
        Some(t.stamp.hlc)
    );
    assert!(!e.delete(&f, &DocId::Int64(1)).unwrap(), "nothing left to delete");

    e.insert(&f, doc! { "_id": 1_i64, "b": 2 }).unwrap();
    let back = crate::codec::decode_doc_record_with_meta(&raw(&e, &f, 1).unwrap()).unwrap();
    assert_eq!(back.body, bytes(&doc! { "_id": 1_i64, "b": 2 }));
    let s = MergeState::decode_compact(back.meta.as_deref().unwrap()).unwrap();
    assert!(s.lives.len() >= 2, "the old life is kept for the join, a new one is added");
    assert!(s.losses.is_empty());
}

#[test]
fn a_refused_field_write_does_not_move_the_clock() {
    let (e, _d) = open();
    let (f, _) = pair(&e);
    e.insert(&f, doc! { "_id": 1_i64, "a": 1 }).unwrap();
    let last = e.clock_last();
    assert!(e.insert(&f, doc! { "_id": 1_i64, "a": 2 }).is_err(), "a duplicate key");
    assert_eq!(e.clock_last(), last);
    let same = e.replace(&f, &DocId::Int64(1), doc! { "_id": 1_i64, "a": 1 }, false).unwrap();
    assert!(same.stamp.is_none());
    assert_eq!(e.clock_last(), last, "a replace that changes nothing mints nothing");
}
