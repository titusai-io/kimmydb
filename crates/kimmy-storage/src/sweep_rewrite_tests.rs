//! Step 3, item (3.4b): the sweep of a late drop thins the records of a `field` collection that are stamped after
//! the drop but still carry cells of the life before it. Only what is stamped after the drop stays of them, the
//! records keep their stamp, the indexes, the counts and the digest follow, and nothing is published.

use bson::doc;
use kimmy_core::{Hlc, MergePolicy, OpKind, Stamp};

use crate::index::index_keys;
use crate::live_count::test_support::{assert_counts_exact, assert_digests_exact};
use crate::{Engine, tables};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

/// B stands over a collection created after a drop and holds X = {a, b}: A's write of b, stamped after the drop,
/// carrying in its state the cell of a, which is at the drop. B has not yet taken the drop.
struct Mixed {
    b: Engine,
    cb: crate::CollectionMeta,
    x: kimmy_core::DocId,
    dropped: Stamp,
    stamp: Stamp,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

fn mixed() -> Mixed {
    let (a, da) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let x = a.insert(&ca, doc! { "_id": 1, "a": 1 }).unwrap();
    let old = a
        .entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .find(|e| e.kind == OpKind::Insert)
        .expect("premise: A made the document in the life before");
    later();
    a.replace(&ca, &x, doc! { "_id": 1, "a": 1, "b": 2 }, false).unwrap();
    let write = a
        .entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .find(|e| e.kind == OpKind::Replace)
        .expect("premise: A wrote b after");
    later();
    let (b, db) = engine();
    b.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.create_index(
        "shop",
        "orders",
        vec![kimmy_core::IndexField { path: "a".into(), descending: false }],
        false,
        Some("by_a".into()),
    )
    .unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    assert!(b.apply_remote(&cb, &write).unwrap(), "premise: B takes A's write");
    assert_eq!(
        b.get(&cb, &x).unwrap(),
        Some(doc! { "_id": 1, "a": 1, "b": 2 }),
        "premise: B holds the cell of the life before with the write after it"
    );
    let dropped = Stamp::new(old.stamp.hlc, b.node_id(), b.epoch());
    assert!(write.stamp.hlc > dropped.hlc, "premise: the write is stamped after the drop");
    assert!(cb.created.wall_ms > dropped.hlc.wall_ms, "premise: B's life began after the drop");
    b.record_collection_drop(cb.id, dropped).unwrap();
    Mixed { b, cb, x, dropped, stamp: write.stamp, _dirs: (da, db) }
}

const MESSAGE: &str =
    "the cells of the life before the drop do not stay in a record stamped after it";

#[test]
fn the_sweep_keeps_only_what_is_stamped_after_the_drop_of_a_record_stamped_after_it() {
    let m = mixed();
    // The index on `a` answers an equality as the planner would: `index_candidates` is the stop-aware read of the
    // index's own entries, so it tells the key a document is filed under from the one it was.
    let by_a =
        m.b.list_indexes("shop", "orders").unwrap().into_iter().find(|i| i.name == "by_a").unwrap();
    let key_of = |document: bson::Document| index_keys(&by_a, &document).unwrap().remove(0);
    let (old_key, null_key) = (key_of(doc! { "a": 1 }), key_of(doc! { "b": 2 }));
    let doc_key = crate::docs::doc_key(&m.x).unwrap();
    let filed_under =
        |key: &[u8]| m.b.index_candidates(&m.cb, by_a.id, key, key).unwrap().contains(&doc_key);
    assert!(filed_under(&old_key), "premise: X is filed under a = 1");
    assert!(!filed_under(&null_key), "premise: and not under null");
    // What the record stores, as a snapshot page serves it: the merge state a peer would join.
    let stored = |m: &Mixed| {
        let page = m.b.snapshot_page(None, Some(m.cb.id)).unwrap();
        assert_eq!(page.documents.len(), 1, "premise: one document");
        crate::field_write::load(page.documents[0].meta.as_deref()).unwrap()
    };
    let identity = kimmy_core::merge_state::Path::flat("_id").unwrap();
    assert!(
        stored(&m)
            .regs
            .iter()
            .any(|((_, path), r)| *path != identity && r.stamp.hlc <= m.dropped.hlc),
        "premise: the stored state holds a cell at or below the drop"
    );
    m.b.sweep_late_drop("shop", "orders", m.dropped).unwrap();
    assert_eq!(m.b.get(&m.cb, &m.x).unwrap(), Some(doc! { "_id": 1, "b": 2 }), "{MESSAGE}");
    let (version, _) = m.b.get_versioned(&m.cb, &m.x).unwrap().unwrap();
    let kimmy_core::DocVersion::Etag(stamp, _) = version else { panic!("an etag") };
    assert_eq!(stamp, m.stamp, "the record keeps the stamp of its newest write");
    assert!(
        !filed_under(&old_key),
        "the index entry of the field that went moved with it: the old key is gone"
    );
    assert!(filed_under(&null_key), "and X is filed under null, a missing field");
    let after = stored(&m);
    assert!(
        after.regs.iter().all(|((_, path), r)| *path == identity || r.stamp.hlc > m.dropped.hlc),
        "the stored state carries a cell of the life before the drop: only `_id` may be at or below it"
    );
    assert!(
        after.lives.keys().all(|l| l.hlc > m.dropped.hlc || after.regs.keys().any(|k| k.0 == *l))
    );
    assert_counts_exact(&m.b, "a field record thinned by the sweep");
    assert_digests_exact(&m.b, "a field record thinned by the sweep");
}

#[test]
fn a_second_sweep_finds_nothing_more_to_thin() {
    let m = mixed();
    m.b.sweep_late_drop("shop", "orders", m.dropped).unwrap();
    let once = m.b.get_versioned(&m.cb, &m.x).unwrap();
    assert_eq!(m.b.sweep_late_drop("shop", "orders", m.dropped).unwrap(), 0);
    assert_eq!(m.b.get_versioned(&m.cb, &m.x).unwrap(), once, "nothing is rewritten twice");
}

/// A tombstone stamped after the drop keeps only what is stamped after it: A made X in the life before; B,
/// which had seen it, deleted it after the drop. M holds both. The delete is a write after the drop and stays,
/// with the tombstone, but the life it killed is of the life before and is not kept in the state.
#[test]
fn the_sweep_thins_the_state_a_tombstone_stamped_after_the_drop_keeps() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (m, _dm) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let x = a.insert(&ca, doc! { "_id": 1, "a": 1 }).unwrap();
    let everything = |e: &Engine| {
        e.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap().entries
    };
    let old = everything(&a).into_iter().find(|e| e.kind == OpKind::Insert).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    later();
    let cb = b.get_collection("shop", "orders").unwrap();
    b.delete(&cb, &x).unwrap();
    let del = everything(&b).into_iter().find(|e| e.kind == OpKind::Delete).unwrap();
    later();
    let cm = m.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let cm = m.get_collection("shop", &cm.name).unwrap();
    assert!(m.apply_remote(&cm, &old).unwrap(), "premise: M takes the life before");
    assert!(m.apply_remote(&cm, &del).unwrap(), "premise: and the delete B made after the drop");
    assert_eq!(m.get(&cm, &x).unwrap(), None, "premise: the document is deleted");
    let dropped = Stamp::new(old.stamp.hlc, m.node_id(), m.epoch());
    assert!(del.stamp.hlc > dropped.hlc, "premise: the delete is stamped after the drop");
    let state_of = |m: &Engine| {
        let page = m.snapshot_page(None, Some(cm.id)).unwrap();
        assert_eq!(page.deleted_documents.len(), 1, "premise: one tombstone");
        let tombstone = &page.deleted_documents[0];
        (tombstone.stamp, crate::field_write::load(tombstone.meta.as_deref()).unwrap())
    };
    let (_, before) = state_of(&m);
    assert!(!before.lives.is_empty(), "premise: the tombstone lists the life before the drop");
    m.record_collection_drop(cm.id, dropped).unwrap();
    m.sweep_late_drop("shop", "orders", dropped).unwrap();
    let (stamp, after) = state_of(&m);
    assert_eq!(stamp, del.stamp, "the tombstone keeps its stamp");
    assert!(after.lives.is_empty(), "{MESSAGE}: the tombstone keeps no life of the life before");
    assert_eq!(after.deletes.len(), 1, "and keeps the delete after the drop");
    assert_eq!(
        m.get_collection("shop", "orders").unwrap().incarnation_floor,
        Some(dropped.hlc),
        "the collection stands, over the drop"
    );
    assert_counts_exact(&m, "a tombstone thinned by the sweep");
    assert_digests_exact(&m, "a tombstone thinned by the sweep");
}

/// A live record whose state renders dead once the drop's cells are gone: a life before the drop that lists a
/// delete after it it has not seen, which honest histories never make. The sweep makes it a tombstone at its
/// stamp, keeping the delete, and the live count falls by exactly that one.
#[test]
fn a_live_record_whose_surviving_state_renders_dead_becomes_a_tombstone_and_is_counted() {
    use kimmy_core::merge_state::{Life, LifeKind, MStamp, MergeState, Path, Reg, Value, WriterId};
    use std::collections::BTreeSet;
    let node_a = kimmy_core::NodeId::from_bytes([1; 16]);
    let node_b = kimmy_core::NodeId::from_bytes([2; 16]);
    let (wa, wb) = (
        WriterId { node: node_a, epoch: kimmy_core::Epoch::ZERO },
        WriterId { node: node_b, epoch: kimmy_core::Epoch::ZERO },
    );
    let life = MStamp { hlc: Hlc::new(100, 0), writer: wa };
    let delete = MStamp { hlc: Hlc::new(300, 0), writer: wb };
    let x = Path::flat("x").unwrap();
    let mut state = MergeState::default();
    state.lives.insert(
        life,
        Life { seen: BTreeSet::from([delete]), order: vec![x.clone()], kind: LifeKind::Insert },
    );
    state.regs.insert(
        (life, x),
        Reg { stamp: life, value: Value::Int64(1), generation: None, soi: false },
    );
    state.deletes.insert(delete, [(wb, delete.hlc)].into_iter().collect());
    state.vv.insert(wa, life.hlc);
    state.vv.insert(wb, delete.hlc);
    assert_eq!(
        MergeState::decode_compact(&state.encode_compact()).as_ref(),
        Ok(&state),
        "premise: the hand-built state is one decode accepts"
    );

    let (b, _db) = engine();
    let cb = b.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let key = crate::docs::doc_key(&kimmy_core::DocId::Int64(1)).unwrap();
    let stamp = Stamp::new(delete.hlc, node_b, kimmy_core::Epoch::ZERO);
    {
        let mut record = kimmy_core::DocRecord::live(
            stamp,
            bson::serialize_to_vec(&doc! { "_id": 1, "x": 1 }).unwrap(),
        );
        record.meta = Some(state.encode_compact());
        let txn = b.begin_write(crate::engine::WriterHolder::Replication).unwrap();
        let mut docs = txn.open_table(tables::DOCS).unwrap();
        crate::live_count::put_record(
            &txn,
            &mut docs,
            cb.id.0,
            &key,
            &crate::codec::encode_doc_record(&record),
        )
        .unwrap();
        drop(docs);
        txn.commit().unwrap();
    }
    assert_eq!(b.count_by_id(cb.id).unwrap(), Some(1), "premise: B holds the live record");
    let dropped = Stamp::new(Hlc::new(200, 0), b.node_id(), b.epoch());
    b.record_collection_drop(cb.id, dropped).unwrap();
    assert_eq!(
        b.sweep_late_drop("shop", "orders", dropped).unwrap(),
        1,
        "the record that left the live set is counted"
    );
    assert_eq!(b.count_by_id(cb.id).unwrap(), Some(0), "the live count fell by that one");
    assert_eq!(b.get(&cb, &kimmy_core::DocId::Int64(1)).unwrap(), None, "it is a tombstone");
    let page = b.snapshot_page(None, Some(cb.id)).unwrap();
    assert_eq!(page.deleted_documents.len(), 1);
    assert_eq!(page.deleted_documents[0].stamp, stamp, "at its own stamp");
    let kept = crate::field_write::load(page.deleted_documents[0].meta.as_deref()).unwrap();
    assert!(kept.lives.is_empty() && kept.deletes.len() == 1, "keeping the delete after the drop");
    assert_counts_exact(&b, "a live record thinned to a tombstone");
}
