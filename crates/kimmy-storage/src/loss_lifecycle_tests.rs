//! The life of the parked loss records around what removes or replaces rows: a rewind, the drop of a collection and a
//! restore (the Step 4 design, v4).

use bson::doc;
use kimmy_core::merge_state::{
    LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, WriterId,
};
use kimmy_core::{DocId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp};
use redb::{ReadableDatabase, ReadableTable};

use crate::{CollectionMeta, Engine, WalkScope, tables};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn parked_rows(e: &Engine) -> usize {
    let txn = e.db().begin_read().unwrap();
    txn.open_table(tables::LOSS_PENDING).unwrap().iter().unwrap().count()
}

fn marker(e: &Engine) -> bool {
    let txn = e.db().begin_read().unwrap();
    txn.open_table(tables::META).unwrap().get(tables::META_LOSS_PENDING_BUILT).unwrap().is_some()
}

/// A member `b` of a `field` collection (made by `a`) with no row for document `1`.
fn pair() -> (Engine, Engine, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let coll = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    (a, b, coll, [da, db])
}

/// The loss entry `a` would append at wall time `at_ms`, for a record whose loser is `a`'s write at the wall time now.
fn loss_entry(a: &Engine, coll: &CollectionMeta, at_ms: u64) -> OplogEntry {
    let writer = WriterId { node: a.node_id(), epoch: a.epoch() };
    let loser = MStamp { hlc: Hlc::new(crate::physical_now_ms(), 0), writer };
    let key = LossKey::field(LossKind::Clash, Path::flat("x").unwrap(), loser);
    OplogEntry {
        stamp: Stamp::new(Hlc::new(at_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::LossRecord,
        collection: coll.id,
        doc_id: Some(DocId::Int64(1)),
        body: Some(MergeState::encode_loss_record(&key, &LossValue::Value(Value::Int64(7)))),
        meta: None,
    }
}

#[test]
fn a_rewind_deletes_the_parked_records_of_the_entries_it_discards_and_keeps_the_others() {
    let (a, b, coll, _d) = pair();
    let now = crate::physical_now_ms();
    let (early, late) = (loss_entry(&a, &coll, now + 10), loss_entry(&a, &coll, now + 20_000));
    b.apply_batch(&[early.clone(), late.clone()]).unwrap();
    assert_eq!(parked_rows(&b), 2, "premise: both parked, for want of a row");

    b.rewind_to(early.stamp.hlc).unwrap();

    assert_eq!(parked_rows(&b), 1, "the one parked from the entry above the target went with it");
    let held: Vec<_> =
        everything(&b).into_iter().filter(|e| e.kind == OpKind::LossRecord).collect();
    assert_eq!(held.len(), 1, "and so did the entry");
    assert_eq!(held[0].stamp, early.stamp);
}

#[test]
fn a_row_the_rewind_writes_folds_in_what_is_parked_for_its_document() {
    let (_a, b, coll, _d) = pair();
    let cb = b.get_collection("shop", "orders").unwrap();
    let id = b.insert(&cb, doc! { "_id": 1, "x": 1 }).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let target = everything(&b).iter().map(|e| e.stamp.hlc).max().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    b.replace(&cb, &id, doc! { "_id": 1, "x": 2 }, false).unwrap();
    // A record parked for the document (as an entry that arrived ahead of its row would have been), from an entry
    // at or below the target.
    let (a2, _d2) = engine();
    let entry = loss_entry(&a2, &coll, target.wall_ms);
    let key = crate::docs::doc_key(&id).unwrap();
    {
        let txn = b.begin_write(crate::engine::WriterHolder::Write).unwrap();
        crate::loss_pending::park(
            &txn,
            coll.id.0,
            &key,
            &crate::codec::oplog_key(&entry.stamp),
            entry.body.as_deref().unwrap(),
            1_000_000_000,
            crate::physical_now_ms(),
        )
        .unwrap();
        txn.commit().unwrap();
    }
    assert_eq!(parked_rows(&b), 1, "premise");

    b.rewind_to(target).unwrap();

    assert_eq!(parked_rows(&b), 0, "the row the rewind wrote took what was parked");
    let page = b.snapshot_page(None, Some(coll.id)).unwrap();
    let state = crate::field_write::load(page.documents[0].meta.as_deref()).unwrap();
    assert_eq!(state.losses.len(), 1, "and it is in its state");
}

#[test]
fn the_purge_of_a_dropped_collection_removes_the_records_parked_from_entries_at_or_below_the_drop()
{
    let (a, b, coll, _d) = pair();
    let now = crate::physical_now_ms();
    b.apply_batch(&[loss_entry(&a, &coll, now)]).unwrap();
    assert_eq!(parked_rows(&b), 1, "premise");
    // A row parked from an entry stamped far above the drop: not the dropped collection's.
    let later = loss_entry(&a, &coll, now + 1_000_000_000);
    {
        let txn = b.begin_write(crate::engine::WriterHolder::Write).unwrap();
        crate::loss_pending::park(
            &txn,
            coll.id.0,
            b"other",
            &crate::codec::oplog_key(&later.stamp),
            later.body.as_deref().unwrap(),
            1_000_000_000,
            now,
        )
        .unwrap();
        txn.commit().unwrap();
    }
    assert_eq!(parked_rows(&b), 2, "premise");

    // The purger runs for a collection that has rows filed under it: one that has only parked records is left to
    // the retention pass, which deletes them when their window closes.
    b.insert(&b.get_collection("shop", "orders").unwrap(), doc! { "_id": 2 }).unwrap();

    b.drop_collection("shop", "orders").unwrap();
    b.finish_purges_now().unwrap();

    assert_eq!(
        parked_rows(&b),
        1,
        "the one parked from an entry at or below the drop went with the collection"
    );
}

#[test]
fn a_restored_store_parks_again_what_its_backup_carries_as_entries() {
    let (a, b, coll, _d) = pair();
    let now = crate::physical_now_ms();
    let (early, late) = (loss_entry(&a, &coll, now + 10), loss_entry(&a, &coll, now + 20_000));
    b.apply_batch(&[early.clone(), late.clone()]).unwrap();
    // A document that has a row: its loss entry is applied to the state, and is not parked again by a rebuild.
    let cb = b.get_collection("shop", "orders").unwrap();
    b.insert(&cb, doc! { "_id": 2, "x": 1 }).unwrap();
    let mut with_a_row = loss_entry(&a, &coll, now + 30);
    with_a_row.doc_id = Some(DocId::Int64(2));
    b.apply_batch(&[with_a_row]).unwrap();
    assert!(marker(&b), "premise: the source marks its table built, and a backup carries META");
    assert_eq!(parked_rows(&b), 2, "premise");
    let mut backup = Vec::new();
    b.backup_to(&mut backup, WalkScope::Request).unwrap();

    let dest = tempfile::tempdir().unwrap();
    let path = dest.path().join("restored.redb");
    crate::backup::restore(&path, &mut backup.as_slice()).unwrap();
    let restored = Engine::open(&path).unwrap();

    assert!(marker(&restored), "the first open built the table and marked it");
    assert_eq!(
        parked_rows(&restored),
        2,
        "from the entries the backup carried, though the marker it carried said built, and not for the document that has a row"
    );

    // `restore --until`: the rewind then discards the entries above the target and what was parked from them.
    restored.rewind_to(early.stamp.hlc).unwrap();
    assert_eq!(parked_rows(&restored), 1, "nothing of the undone future stays parked");
    assert_eq!(everything(&restored).iter().filter(|e| e.kind == OpKind::LossRecord).count(), 1);
}

#[test]
fn a_store_that_has_built_the_table_does_not_build_it_again() {
    let (a, b, coll, _d) = pair();
    let dir = tempfile::tempdir().unwrap();
    drop(b);
    let (c, dc) = engine();
    c.apply_batch(&everything(&a)).unwrap();
    c.apply_batch(&[loss_entry(&a, &coll, crate::physical_now_ms() + 10)]).unwrap();
    assert_eq!(parked_rows(&c), 1, "premise");
    let path = dc.path().join("kimmy.redb");
    {
        let txn = c.begin_write(crate::engine::WriterHolder::Write).unwrap();
        txn.open_table(tables::LOSS_PENDING).unwrap().retain(|_, _| false).unwrap();
        txn.commit().unwrap();
    }
    drop(c);
    drop(dir);

    let reopened = Engine::open(&path).unwrap();

    assert_eq!(
        parked_rows(&reopened),
        0,
        "a marked store is not walked: the row that was deleted stays so"
    );
}

#[test]
fn a_fresh_store_has_the_marker_from_its_first_commit_and_the_open_spends_no_commit_on_it() {
    // The marker is written with the tables of a fresh store (it has no loss entries to park), not by a later
    // commit of its own: a commit more than the open used to make moves where damage placed in a store's last
    // commits lands, and a store the open then refuses must not have been written to.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    crate::hold_meter::test_hooks::STOP_OPEN_AFTER.with(|s| s.set(Some("tables")));
    let stopped = Engine::open(&path);
    crate::hold_meter::test_hooks::STOP_OPEN_AFTER.with(|s| s.set(None));
    assert!(
        stopped.is_err(),
        "premise: the open was stopped after the commit that makes the tables"
    );

    let db = redb::Builder::new().open_read_only(&path).unwrap();
    let txn = db.begin_read().unwrap();
    let meta = txn.open_table(tables::META).unwrap();
    assert!(
        meta.get(tables::META_LOSS_PENDING_BUILT).unwrap().is_some(),
        "the marker is in the commit that makes the tables"
    );
}
