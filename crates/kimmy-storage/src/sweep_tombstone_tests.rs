//! The burial tombstone of a `field` record the late-drop sweep removes keeps a state: a stateless tombstone of a
//! `field` collection is not a valid row (a member that has not taken the drop refuses a repair's page that carries
//! one as corrupt), and the sweep is the one producer that wrote one. Every cell of a record at or below the drop is at or below it, so the state
//! kept is the record's version vector alone.

use bson::doc;
use kimmy_core::{Hlc, MergePolicy, OpKind, OplogEntry, Stamp};

use crate::{Engine, SnapshotProgress, tables};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, crate::WalkScope::Background).unwrap().entries
}

/// B stands over a collection created after a drop; it holds a record of the life before, written by A at or
/// below the drop. The sweep that the drop owes buries it.
struct Swept {
    b: Engine,
    collection: kimmy_core::CollectionId,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

fn swept() -> Swept {
    let (a, da) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    a.insert(&ca, doc! { "_id": 1, "a": 1 }).unwrap();
    let old = everything(&a)
        .into_iter()
        .find(|e| e.kind == OpKind::Insert)
        .expect("premise: A made a record of the life before");
    later();
    let (b, db) = engine();
    let cb = b.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    {
        let key = crate::docs::doc_key(old.doc_id.as_ref().unwrap()).unwrap();
        let mut record = kimmy_core::DocRecord::live(old.stamp, old.body.clone().unwrap());
        record.meta = old.meta.clone();
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
    assert_eq!(
        b.count_by_id(cb.id).unwrap(),
        Some(1),
        "premise: B holds the record of the life before"
    );
    // The drop is B's own, at the record's stamp: the tie goes to the drop.
    let dropped = Stamp::new(old.stamp.hlc, b.node_id(), b.epoch());
    b.sweep_after_an_ignored_drop(&cb, dropped).unwrap();
    assert_eq!(b.count_by_id(cb.id).unwrap(), Some(0), "premise: the sweep removed the record");
    Swept { b, collection: cb.id, _dirs: (da, db) }
}

#[test]
fn the_burial_tombstone_of_a_field_record_carries_its_state() {
    let s = swept();
    let page = s.b.snapshot_page(None, Some(s.collection)).unwrap();
    assert_eq!(page.deleted_documents.len(), 1, "premise: the page carries the burial tombstone");
    let meta = page.deleted_documents[0].meta.as_deref();
    assert!(meta.is_some(), "the burial tombstone of a field record carries its state");
    let state = crate::field_write::load(meta).unwrap();
    assert!(!state.vv.is_empty(), "the state kept is the record's version vector");
    assert!(
        state.lives.is_empty() && state.regs.is_empty() && state.deletes.is_empty(),
        "no life, cell or delete of the record is kept: all of it is at or below the drop"
    );
}

/// A whole-database page is judged by the floor it carries: the burial tombstone is at the drop, so the receiver
/// takes it as history and never reads its state. A Converge page, the document repair of a confirmed digest
/// finding, does not carry the floor: a member that holds the collection and has not taken the drop meets the
/// tombstone as a carried delete of a field document, and refuses one with no state as corrupt.
#[test]
fn a_converge_page_carrying_the_burial_tombstone_of_a_field_record_applies_at_a_peer() {
    let s = swept();
    let (c, _dc) = engine();
    c.apply_batch(&everything(&s.b)).unwrap();
    let held = c.get_collection("shop", "orders").unwrap();
    assert!(held.incarnation_floor.is_none(), "premise: C has not taken the drop");
    let page = s.b.snapshot_page(None, Some(s.collection)).unwrap();
    assert_eq!(page.deleted_documents.len(), 1, "premise: the page carries the burial tombstone");
    let cut = page.versions.clone();
    c.apply_snapshot_page(
        s.b.node_id(),
        &mut SnapshotProgress::of_collection_converging(s.collection, cut),
        &page,
    )
    .expect("the burial tombstone of a field record applies at a peer");
}
