//! The loss-record entry (`OpKind::LossRecord`, tag 14): what a member that receives one does with it. It keeps
//! it in its oplog and serves it onward, as it keeps any entry a servable vector covers, and applies it to no
//! document and no state (the commit that applies it comes after this one).

use bson::doc;
use kimmy_core::merge_state::{
    LossAt, LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, WriterId,
};
use kimmy_core::{DocId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp};

use crate::{CollectionMeta, Engine, WalkScope, codec};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn field_collection(e: &Engine) -> CollectionMeta {
    e.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap()
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// A pair of members of one `field` collection, the second holding the first's definitions.
fn pair() -> (Engine, Engine, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let coll = field_collection(&a);
    b.apply_batch(&everything(&a)).unwrap();
    (a, b, coll, [da, db])
}

/// A record `a` could have detected for document `1`: the key names a loser stamp of `a`'s origin.
fn a_record_of(a: &Engine) -> (LossKey, LossValue) {
    let writer = WriterId { node: a.node_id(), epoch: a.epoch() };
    let loser = MStamp { hlc: Hlc::new(crate::physical_now_ms(), 0), writer };
    (
        LossKey::field(LossKind::Clash, Path::flat("x").unwrap(), loser),
        LossValue::Value(Value::Int64(7)),
    )
}

/// The entry `a` would append for it: a fresh stamp of its own, above the loser's.
fn the_entry(a: &Engine, coll: &CollectionMeta, key: &LossKey, value: &LossValue) -> OplogEntry {
    let at = Hlc::new(crate::physical_now_ms() + 1, 0);
    OplogEntry {
        stamp: Stamp::new(at, a.node_id(), a.epoch()),
        kind: OpKind::LossRecord,
        collection: coll.id,
        doc_id: Some(DocId::Int64(1)),
        body: Some(MergeState::encode_loss_record(key, value)),
        meta: None,
    }
}

fn holds(e: &Engine, entry: &OplogEntry) -> bool {
    everything(e).iter().any(|held| held.stamp == entry.stamp && held.kind == OpKind::LossRecord)
}

#[test]
fn the_op_kind_tag_is_14_and_the_first_tag_no_kind_has_is_refused() {
    let (a, _da) = engine();
    let coll = field_collection(&a);
    let (key, value) = a_record_of(&a);
    let entry = the_entry(&a, &coll, &key, &value);

    let bytes = codec::encode_oplog_entry(&entry);
    assert_eq!(bytes[1], 14, "the kind byte follows the version byte");
    let back = codec::decode_oplog_entry(&codec::oplog_key(&entry.stamp), &bytes).unwrap();
    assert_eq!(back, entry);

    // 15, 16 and 17 are the swap's kinds (ADR-272); 18 is the first tag no build knows.
    let mut next = bytes.clone();
    next[1] = 18;
    let refused = codec::decode_oplog_entry(&codec::oplog_key(&entry.stamp), &next);
    assert!(refused.is_err(), "a kind no build knows is refused, not read as another");
    assert!(refused.unwrap_err().to_string().contains("unknown op kind"));
}

#[test]
fn a_received_loss_record_is_kept_in_the_oplog_and_served_onward_and_changes_no_document() {
    let (a, b, coll, _d) = pair();
    let (c, _dc) = engine();
    c.apply_batch(&everything(&a)).unwrap();
    let (key, value) = a_record_of(&a);
    let entry = the_entry(&a, &coll, &key, &value);
    let before = everything(&b).len();

    b.apply_batch(std::slice::from_ref(&entry)).unwrap();

    assert!(holds(&b, &entry), "it is in the receiver's oplog");
    assert_eq!(everything(&b).len(), before + 1, "and it is the only thing that was added");
    assert_eq!(
        b.get(&b.get_collection("shop", "orders").unwrap(), &DocId::Int64(1)).unwrap(),
        None,
        "no document"
    );

    c.apply_batch(&everything(&b)).unwrap();
    assert!(holds(&c, &entry), "and a member that takes b's window takes it too");
    assert!(
        b.version_vector().unwrap().get(a.origin()) >= entry.stamp.hlc,
        "the receiver's vector covers the origin's entry: what it covers, it holds"
    );
}

#[test]
fn a_loss_record_received_twice_is_kept_once() {
    let (a, b, coll, _d) = pair();
    let (key, value) = a_record_of(&a);
    let entry = the_entry(&a, &coll, &key, &value);

    b.apply_batch(std::slice::from_ref(&entry)).unwrap();
    b.apply_batch(std::slice::from_ref(&entry)).unwrap();

    assert_eq!(
        everything(&b).iter().filter(|e| e.stamp == entry.stamp).count(),
        1,
        "an entry already held is not appended again"
    );
}

#[test]
fn a_loss_record_whose_body_is_not_a_record_is_refused_and_nothing_is_written() {
    let (a, b, coll, _d) = pair();
    let (key, value) = a_record_of(&a);
    let mut entry = the_entry(&a, &coll, &key, &value);
    entry.body = Some(vec![1, 2, 3]);
    let before = everything(&b);

    let refused = b.apply_batch(std::slice::from_ref(&entry));

    assert!(refused.is_err(), "a body that does not decode is refused by name");
    assert_eq!(everything(&b), before, "and nothing is written");
}

#[test]
fn a_loss_record_for_a_collection_that_is_not_a_field_collection_is_ignored() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let plain =
        a.create_collection_with_policy("shop", "orders", MergePolicy::Document, None).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let (key, value) = a_record_of(&a);
    let entry = the_entry(&a, &plain, &key, &value);

    b.apply_batch(std::slice::from_ref(&entry)).unwrap();

    assert!(!holds(&b, &entry), "a document collection has no merge state to hold a record for");
    assert_eq!(
        b.get(&b.get_collection("shop", "orders").unwrap(), &DocId::Int64(1)).unwrap(),
        None,
        "and the body is not mistaken for a document"
    );
}

#[test]
fn a_loss_record_is_not_a_document_for_a_rewind() {
    let (a, _da) = engine();
    let coll = field_collection(&a);
    let id = a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let target = everything(&a).iter().map(|e| e.stamp.hlc).max().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let (key, value) = a_record_of(&a);
    let entry = the_entry(&a, &coll, &key, &value);
    let (b, _db) = engine();
    b.apply_batch(&everything(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    b.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert!(holds(&b, &entry), "premise: held above the target");

    b.rewind_to(target).unwrap();

    assert_eq!(
        b.get(&cb, &id).unwrap(),
        Some(doc! { "_id": 1, "x": 1 }),
        "the document is as it was"
    );
    assert!(
        !holds(&b, &entry),
        "an entry above the target is discarded with the rest of the future"
    );
    let _ = LossAt::Document;
}
