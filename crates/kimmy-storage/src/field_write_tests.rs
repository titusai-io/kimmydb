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
        bytes(&doc! { "_id": 1_i64, "c": 3, "b": 9 }),
        "a is removed, c is new, and the order is the one sent (pipeline-reorder-only-q7: a \
         replaceOne sets the order)"
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
    // A tombstone of a field collection keeps the `_id` and exactly that (C4c); it was an empty body.
    assert!(t.deleted && t.tombstone_id() == Some(DocId::Int64(1)), "the tombstone keeps the id");
    assert_eq!(t.body, bson::serialize_to_vec(&doc! { "_id": 1_i64 }).unwrap(), "and nothing else");
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

/// A spec that matches every document and answers `to(doc)`.
struct Spec<F: Fn(&Document) -> Option<Document>>(F);

impl<F: Fn(&Document) -> Option<Document>> crate::ModifySpec for Spec<F> {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        Ok(true)
    }
    fn compare(&self, _: &Document, _: &Document) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> Result<Option<Document>, String> {
        Ok((self.0)(doc))
    }
    fn upsert(&self) -> Option<Result<Document, String>> {
        None
    }
}

#[test]
fn an_update_of_a_field_collection_writes_the_state_and_a_removal_leaves_a_tombstone_with_it() {
    use crate::Candidates;
    let (e, _d) = open();
    let (f, dc) = pair(&e);
    for c in [&f, &dc] {
        e.insert(c, doc! { "_id": 1_i64, "n": 1, "k": "x" }).unwrap();
    }
    // a change: the record, and the entry, carry the state; the stamp is max(vv)
    let bump = Spec(|d: &Document| {
        let mut d = d.clone();
        d.insert("n", 2);
        Some(d)
    });
    let out = e.modify_where(&f, &Candidates::Scan, &bump, None).unwrap();
    assert_eq!((out.matched, out.modified), (1, 1));
    let rec = crate::codec::decode_doc_record_with_meta(&raw(&e, &f, 1).unwrap()).unwrap();
    assert_eq!(rec.body, bytes(&doc! { "_id": 1_i64, "n": 2, "k": "x" }));
    let state = MergeState::decode_compact(rec.meta.as_deref().unwrap()).unwrap();
    assert_eq!(state.vv.values().copied().max(), Some(rec.stamp.hlc));
    let es = entries(&e, &f);
    assert_eq!(
        (es.len(), es[1].kind, es[1].meta.as_deref()),
        (2, OpKind::Replace, rec.meta.as_deref())
    );
    // the same result again is no write
    let commits = e.commits();
    let same = Spec(|d: &Document| Some(d.clone()));
    let out = e.modify_where(&f, &Candidates::Scan, &same, None).unwrap();
    assert_eq!((out.matched, out.modified), (1, 0));
    assert_eq!(e.commits(), commits);
    // a removal
    let gone = Spec(|_: &Document| None);
    let out = e.modify_where(&f, &Candidates::Scan, &gone, None).unwrap();
    assert_eq!(out.modified, 1);
    let t = crate::codec::decode_doc_record_with_meta(&raw(&e, &f, 1).unwrap()).unwrap();
    assert!(t.deleted && t.meta.is_some());
    let es = entries(&e, &f);
    assert_eq!((es[2].kind, es[2].meta.as_deref()), (OpKind::Delete, t.meta.as_deref()));
    // the document collection is as it was
    e.modify_where(&dc, &Candidates::Scan, &bump, None).unwrap();
    assert_eq!(raw(&e, &dc, 1).unwrap()[0], 1);
    assert!(entries(&e, &dc).iter().all(|x| x.meta.is_none()));
}

/// A spec whose update is the effects `fx`, with the post-image `post`.
struct SpecFx {
    post: Document,
    fx: fn() -> kimmy_core::effects::Effects,
}

impl crate::ModifySpec for SpecFx {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        Ok(true)
    }
    fn compare(&self, _: &Document, _: &Document) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
    fn apply(&self, _: &Document) -> Result<Option<Document>, String> {
        Ok(Some(self.post.clone()))
    }
    fn effects(&self, _: &Document) -> Option<Result<kimmy_core::effects::Effects, String>> {
        Some(Ok((self.fx)()))
    }
    fn upsert(&self) -> Option<Result<Document, String>> {
        None
    }
}

#[test]
fn an_increment_of_a_field_collection_is_a_counter_slot_and_not_a_register_write() {
    use crate::Candidates;
    use kimmy_core::merge_state::{Path, Value};
    let (e, _d) = open();
    let (f, _) = pair(&e);
    e.insert(&f, doc! { "_id": 1_i64, "n": 1, "k": "x" }).unwrap();
    let inc = SpecFx {
        post: doc! { "_id": 1_i64, "n": 2, "k": "x" },
        fx: || {
            let mut fx = kimmy_core::effects::Effects::new();
            fx.inc(Path::flat("n").unwrap(), Value::Int32(1));
            fx
        },
    };
    let out = e.modify_where(&f, &Candidates::Scan, &inc, None).unwrap();
    assert_eq!((out.matched, out.modified), (1, 1));
    let rec = crate::codec::decode_doc_record_with_meta(&raw(&e, &f, 1).unwrap()).unwrap();
    assert_eq!(rec.body, bytes(&doc! { "_id": 1_i64, "n": 2, "k": "x" }));
    let state = MergeState::decode_compact(rec.meta.as_deref().unwrap()).unwrap();
    assert!(!state.slots.is_empty(), "the increment is a slot of the counter, not a register");
    assert_eq!(state.vv.values().copied().max(), Some(rec.stamp.hlc));
    // effects that change nothing write nothing
    let commits = e.commits();
    let none = SpecFx {
        post: doc! { "_id": 1_i64, "n": 2, "k": "x" },
        fx: kimmy_core::effects::Effects::new,
    };
    let out = e.modify_where(&f, &Candidates::Scan, &none, None).unwrap();
    assert_eq!((out.matched, out.modified), (1, 0));
    assert_eq!(e.commits(), commits);
}

/// An upsert spec: nothing matches, the engine inserts `seed`, created as `fx`.
struct SpecUpsert {
    seed: Document,
    fx: fn() -> kimmy_core::effects::UpsertEffects,
}

impl crate::ModifySpec for SpecUpsert {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        Ok(false)
    }
    fn compare(&self, _: &Document, _: &Document) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
    fn apply(&self, _: &Document) -> Result<Option<Document>, String> {
        Ok(None)
    }
    fn upsert(&self) -> Option<Result<Document, String>> {
        Some(Ok(self.seed.clone()))
    }
    fn upsert_effects(
        &self,
        _: &Document,
    ) -> Option<Result<kimmy_core::effects::UpsertEffects, String>> {
        Some(Ok((self.fx)()))
    }
}

#[test]
fn an_upsert_into_a_field_collection_is_created_through_the_typed_entry_with_one_record_and_one_entry()
 {
    use crate::Candidates;
    use kimmy_core::merge_state::{Path, Value};
    let (e, _d) = open();
    let (f, _) = pair(&e);
    let spec = SpecUpsert {
        seed: doc! { "_id": 7_i64, "k": 5, "a": 1, "n": 3 },
        fx: || kimmy_core::effects::UpsertEffects {
            from_filter: vec![(Path::flat("k").unwrap(), Value::Int32(5))],
            set: vec![(Path::flat("a").unwrap(), Value::Int32(1))],
            inc: vec![(Path::flat("n").unwrap(), Value::Int32(3))],
            ..Default::default()
        },
    };
    let out = e.find_and_modify(&f, &Candidates::Scan, &spec).unwrap();
    assert!(!out.matched && out.upserted.is_some(), "{out:?}");
    let rec = crate::codec::decode_doc_record_with_meta(&raw(&e, &f, 7).unwrap()).unwrap();
    assert_eq!(rec.body, bytes(&doc! { "_id": 7_i64, "k": 5, "a": 1, "n": 3 }));
    let state = MergeState::decode_compact(rec.meta.as_deref().unwrap()).unwrap();
    assert!(!state.slots.is_empty(), "the $inc of the creation is a counter slot");
    assert_eq!(
        state.vv.values().copied().max(),
        Some(rec.stamp.hlc),
        "the later of the two stamps"
    );
    let es = entries(&e, &f);
    assert_eq!(es.len(), 1, "one entry");
    assert_eq!(
        (es[0].kind, es[0].stamp, es[0].meta.as_deref()),
        (OpKind::Insert, rec.stamp, rec.meta.as_deref())
    );
}

#[test]
fn the_writers_refusal_of_an_update_reaches_the_request_with_its_own_text_and_writes_nothing() {
    use crate::Candidates;
    use kimmy_core::merge_state::{Path, Value};
    let (e, _d) = open();
    let (f, _) = pair(&e);
    e.insert(&f, doc! { "_id": 1_i64, "s": "x" }).unwrap();
    let before = raw(&e, &f, 1).unwrap();
    let last = e.clock_last();
    let bad = SpecFx {
        post: doc! { "_id": 1_i64, "s": "y" },
        fx: || {
            let mut fx = kimmy_core::effects::Effects::new();
            fx.inc(Path::flat("s").unwrap(), Value::Int32(1));
            fx
        },
    };
    let err = e.modify_where(&f, &Candidates::Scan, &bad, None).unwrap_err();
    match err {
        crate::StorageError::Core(kimmy_core::Error::InvalidUpdate(text)) => {
            assert!(text.contains("not a number"), "{text}")
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(raw(&e, &f, 1).unwrap(), before, "nothing was written");
    assert_eq!(e.clock_last(), last, "and no stamp was minted");
}

#[test]
fn a_remote_entry_for_a_field_collection_is_applied_as_a_joined_state_and_a_document_collection_still_applies_it()
 {
    let (a, _da) = open();
    let (b, _db) = open();
    let (fa, _) = pair(&a);
    let (fb, db_) = pair(&b);
    a.insert(&fa, doc! { "_id": 1_i64, "a": 1 }).unwrap();
    let entry = entries(&a, &fa).remove(0);
    assert!(b.apply_remote(&fb, &entry).unwrap(), "the entry is applied");
    assert_eq!(
        b.get(&fb, &kimmy_core::DocId::Int64(1)).unwrap(),
        Some(doc! { "_id": 1_i64, "a": 1 })
    );
    assert_eq!(raw(&b, &fb, 1), raw(&a, &fa, 1), "the same record, merge state included");
    assert!(!b.apply_remote(&fb, &entry).unwrap(), "the same entry again moves nothing");
    // the same write, as a document collection's entry, applies
    let (c, _dc) = open();
    let (_, dc) = pair(&c);
    c.insert(&dc, doc! { "_id": 1_i64, "a": 1 }).unwrap();
    let e2 = entries(&c, &dc).remove(0);
    assert!(b.apply_remote(&db_, &e2).unwrap());
}

#[test]
fn a_batch_with_a_field_entry_applies_both_and_counts_no_refusal() {
    let (a, _da) = open();
    let (b, _db) = open();
    let (fa, da) = pair(&a);
    a.insert(&fa, doc! { "_id": 1_i64, "a": 1 }).unwrap();
    a.insert(&da, doc! { "_id": 2_i64, "b": 2 }).unwrap();
    let all = a.entries_for_peer(Hlc::ZERO, 100, WalkScope::Background).unwrap().entries;
    let out = b.apply_batch(&all).unwrap();
    assert_eq!(out.applied, 2, "the field entry and the document entry");
    let fb = b.get_collection("app", "f").unwrap();
    let db_ = b.get_collection("app", "d").unwrap();
    assert!(raw(&b, &fb, 1).is_some(), "the field document is stored");
    assert!(raw(&b, &db_, 2).is_some(), "and the document collection's");
    let again = b.apply_batch(&all).unwrap();
    assert_eq!(again.applied, 0);
}

#[test]
fn a_snapshot_page_with_a_field_document_applies_it_as_state_and_the_cursor_moves_on() {
    let (a, _da) = open();
    let (b, _db) = open();
    let (fa, da) = pair(&a);
    a.insert(&fa, doc! { "_id": 1_i64, "a": 1 }).unwrap();
    a.insert(&da, doc! { "_id": 2_i64, "b": 2 }).unwrap();
    let mut progress = crate::SnapshotProgress::whole_database();
    let mut after = None;
    for _ in 0..20 {
        let page = a.snapshot_page(after.take(), None).unwrap();
        b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
        after = page.next.clone();
        if after.is_none() {
            break;
        }
    }
    assert!(after.is_none(), "the snapshot ended");
    let fb = b.get_collection("app", "f").unwrap();
    let db_ = b.get_collection("app", "d").unwrap();
    assert_eq!(
        raw(&b, &fb, 1),
        raw(&a, &fa, 1),
        "the field document is held as the donor holds it"
    );
    assert!(raw(&b, &db_, 2).is_some());
}

#[test]
fn the_version_of_a_field_document_is_its_stamp_and_the_hash_of_its_state() {
    use kimmy_core::DocVersion;
    let (e, _d) = open();
    let (f, dc) = pair(&e);
    for c in [&f, &dc] {
        e.insert(c, doc! { "_id": 1_i64, "a": 1 }).unwrap();
    }
    let rec = crate::codec::decode_doc_record_with_meta(&raw(&e, &f, 1).unwrap()).unwrap();
    let hash = MergeState::loss_free_hash_of(rec.meta.as_deref().unwrap()).unwrap();
    let (v, d) = e.get_versioned(&f, &DocId::Int64(1)).unwrap().unwrap();
    assert_eq!(v, DocVersion::Etag(rec.stamp, hash));
    assert_eq!(d, doc! { "_id": 1_i64, "a": 1 });
    let (v, _) = e.get_versioned(&dc, &DocId::Int64(1)).unwrap().unwrap();
    assert!(matches!(v, DocVersion::Stamp(_)), "{v:?}");
    assert!(e.get_versioned(&f, &DocId::Int64(9)).unwrap().is_none());
}

#[test]
fn a_write_conditional_on_an_etag_needs_the_stamp_and_the_state_to_match() {
    use crate::StorageError;
    let (e, _d) = open();
    let (f, _) = pair(&e);
    let id = DocId::Int64(1);
    e.insert(&f, doc! { "_id": 1_i64, "a": 1 }).unwrap();
    let (v, _) = e.get_versioned(&f, &id).unwrap().unwrap();
    let (stamp, hash) = (v.stamp(), v.state_hash().unwrap());
    let other = [hash[0] ^ 1; 16];
    // the right stamp with another state is stale, and writes nothing
    let last = e.clock_last();
    let r =
        e.replace_if_state(&f, &id, doc! { "_id": 1_i64, "a": 2 }, false, Some(stamp), Some(other));
    assert!(matches!(r, Err(StorageError::Stale { current: Some(c) }) if c == stamp), "{r:?}");
    let r = e.delete_if_state(&f, &id, Some(stamp), Some(other));
    assert!(matches!(r, Err(StorageError::Stale { .. })), "{r:?}");
    assert_eq!(e.clock_last(), last);
    assert_eq!(e.get(&f, &id).unwrap().unwrap(), doc! { "_id": 1_i64, "a": 1 });
    // the right etag writes, and the old etag is stale afterwards
    let out = e
        .replace_if_state(&f, &id, doc! { "_id": 1_i64, "a": 2 }, false, Some(stamp), Some(hash))
        .unwrap();
    assert!(out.modified);
    let r = e.delete_if_state(&f, &id, Some(stamp), Some(hash));
    assert!(matches!(r, Err(StorageError::Stale { .. })), "{r:?}");
    let (v2, _) = e.get_versioned(&f, &id).unwrap().unwrap();
    assert_ne!(v2, v);
    assert!(e.delete_if_state(&f, &id, Some(v2.stamp()), v2.state_hash()).unwrap().is_some());
}

#[test]
fn every_write_answers_the_version_it_produced() {
    use kimmy_core::DocVersion;
    let (e, _d) = open();
    let (f, dc) = pair(&e);
    let id = DocId::Int64(1);
    let (_, v) = e.insert_versioned(&f, doc! { "_id": 1_i64, "a": 1 }).unwrap();
    assert!(matches!(v, DocVersion::Etag(..)));
    assert_eq!(e.get_versioned(&f, &id).unwrap().unwrap().0, v);
    let out = e.replace(&f, &id, doc! { "_id": 1_i64, "a": 2 }, false).unwrap();
    assert_eq!(e.get_versioned(&f, &id).unwrap().unwrap().0, out.version.unwrap());
    let tombstone = e.delete_if_state(&f, &id, None, None).unwrap();
    assert!(tombstone.is_some());
    let (_, dv) = e.insert_versioned(&dc, doc! { "_id": 1_i64 }).unwrap();
    assert!(matches!(dv, DocVersion::Stamp(_)), "{dv:?}");
}
