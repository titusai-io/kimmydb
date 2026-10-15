//! The `Converge` policy of a scoped snapshot pull (the document repair of a
//! confirmed digest finding): documents only, and only what this member can judge
//! by itself. See [`crate::SnapshotPolicy`].
//!
//! Each test builds a sender `a` and a requester `b` that disagree, takes the
//! sender's scoped page, and applies it under `Converge`.

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, Stamp};

use crate::{Engine, SnapshotApplied, SnapshotPolicy, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn orders() -> CollectionId {
    CollectionId::derive("shop", "orders")
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn pull(into: &Engine, from: &Engine) {
    let entries = from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

/// A sender and a requester that have replicated everything so far.
fn pair() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    a.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    (a, b, [da, db])
}

fn coll(e: &Engine) -> crate::CollectionMeta {
    e.get_collection("shop", "orders").unwrap()
}

fn value(e: &Engine, id: i64) -> Option<i32> {
    e.get(&coll(e), &DocId::Int64(id)).unwrap().map(|d| d.get_i32("v").unwrap())
}

fn held_row(e: &Engine, id: i64) -> bool {
    use redb::ReadableDatabase;
    let key = crate::docs::doc_key(&DocId::Int64(id)).unwrap();
    let txn = e.db().begin_read().unwrap();
    let table = txn.open_table(crate::tables::DOCS).unwrap();
    table.get((orders().0, key.as_slice())).unwrap().is_some()
}

/// Apply the sender's whole scoped snapshot to `into` under `Converge`, page by page,
/// returning the sum of what every page reported.
fn converge(into: &Engine, from: &Engine) -> SnapshotApplied {
    let mut progress = SnapshotProgress::of_collection_converging(orders());
    assert_eq!(progress.policy(), SnapshotPolicy::Converge);
    let mut total = SnapshotApplied::default();
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        let done = into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
        total.applied += done.applied;
        total.left += done.left;
        total.left_tombstone += done.left_tombstone;
        total.created += done.created;
        total.superseded += done.superseded;
        total.sender_dropped |= done.sender_dropped;
        assert!(done.shadows_missing.is_empty() && done.shadows_orphaned.is_empty());
        if done.sender_dropped {
            break;
        }
    }
    total
}

/// [`converge`] with the sender's horizon stripped from every page: the young-stamp rule is
/// off for the pull, as it is against a sender that predates the field.
fn converge_without_a_horizon(into: &Engine, from: &Engine) -> SnapshotApplied {
    let mut progress = SnapshotProgress::of_collection_converging(orders());
    let mut total = SnapshotApplied::default();
    while !progress.is_complete() {
        let mut page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        page.tombstone_horizon = None;
        let done = into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
        total.applied += done.applied;
        total.left += done.left;
        total.left_tombstone += done.left_tombstone;
        total.created += done.created;
    }
    total
}

// -- Rows one to three of the table: both hold the id, at different stamps ---------------

#[test]
fn a_newer_live_row_is_applied_over_a_held_live_row() {
    let (a, b, _d) = pair();
    let ca = coll(&a);
    a.insert(&ca, doc! { "_id": 1, "v": 1 }).unwrap();
    pull(&b, &a);
    later();
    a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "v": 2 }, false).unwrap();
    let done = converge(&b, &a);
    assert_eq!(value(&b, 1), Some(2), "a missed update was not repaired");
    assert_eq!(done.applied, 1);
    assert_eq!(done.left, 0);
}

#[test]
fn a_newer_live_row_is_applied_over_a_held_tombstone() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 1, "v": 1 }).unwrap();
    pull(&b, &a);
    later();
    b.delete(&coll(&b), &DocId::Int64(1)).unwrap();
    later();
    a.replace(&coll(&a), &DocId::Int64(1), doc! { "_id": 1, "v": 2 }, false).unwrap();
    let done = converge(&b, &a);
    assert_eq!(value(&b, 1), Some(2), "a re-insert this member missed was not repaired");
    assert_eq!(done.applied, 1);
}

#[test]
fn a_newer_tombstone_deletes_a_held_live_row() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 1, "v": 1 }).unwrap();
    pull(&b, &a);
    later();
    a.delete(&coll(&a), &DocId::Int64(1)).unwrap();
    let done = converge(&b, &a);
    assert_eq!(value(&b, 1), None, "a missed delete was not repaired");
    assert_eq!(done.applied, 1);
}

// -- The rows nothing is written for ----------------------------------------------------

#[test]
fn this_member_ahead_is_left_alone() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 1, "v": 1 }).unwrap();
    pull(&b, &a);
    later();
    b.replace(&coll(&b), &DocId::Int64(1), doc! { "_id": 1, "v": 9 }, false).unwrap();
    let done = converge(&b, &a);
    assert_eq!(value(&b, 1), Some(9), "an older row overwrote a newer one");
    assert_eq!(done.applied, 0);
}

#[test]
fn a_held_tombstone_rejects_an_older_live_row() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 1, "v": 1 }).unwrap();
    pull(&b, &a);
    later();
    b.delete(&coll(&b), &DocId::Int64(1)).unwrap();
    let done = converge(&b, &a);
    assert_eq!(value(&b, 1), None, "an older live row beat a newer tombstone");
    assert_eq!(done.applied, 0);
}

#[test]
fn a_live_row_this_member_holds_nothing_for_is_left_not_written() {
    // Ambiguous: never got it, or deleted it and collected the tombstone (F2).
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 3, "v": 3 }).unwrap();
    // Against a sender that sends no horizon the young-stamp rule is off.
    let done = converge_without_a_horizon(&b, &a);
    assert_eq!(value(&b, 3), None, "Converge created a row");
    assert!(!held_row(&b, 3));
    assert_eq!(done.applied, 0);
    assert_eq!(done.left, 1, "the row was not counted left");
}

#[test]
fn a_tombstone_for_an_id_with_no_row_writes_nothing_and_records_no_bare_tombstone() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 4, "v": 4 }).unwrap();
    a.delete(&coll(&a), &DocId::Int64(4)).unwrap();
    let done = converge(&b, &a);
    assert!(!held_row(&b, 4), "a bare tombstone was recorded");
    assert_eq!(done.applied, 0);
    assert_eq!(done.left, 0, "a tombstone row is not a live row left (the WARN cannot name it)");
    assert_eq!(done.left_tombstone, 1, "the tombstone was not counted");
}

#[test]
fn a_tombstone_the_senders_coverage_does_not_name_is_ignored() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 1, "v": 1 }).unwrap();
    pull(&b, &a);
    later();
    a.delete(&coll(&a), &DocId::Int64(1)).unwrap();
    let mut page = a.snapshot_page(None, Some(orders())).unwrap();
    page.versions = kimmy_core::VersionVector::new();
    let mut progress = SnapshotProgress::of_collection_converging(orders());
    let done = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert_eq!(value(&b, 1), Some(1), "an unvouched delete was applied");
    assert_eq!(done.applied, 0);
    assert_eq!(done.left, 0, "an ignored tombstone is neither applied nor left");
}

#[test]
fn a_row_of_a_buried_life_is_superseded_not_left() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 9, "v": 9 }).unwrap();
    pull(&b, &a);
    later();
    // The requester buries its life and makes another: its floor is above the row.
    b.drop_collection("shop", "orders").unwrap();
    b.finish_purges_now().unwrap();
    later();
    b.create_collection("shop", "orders").unwrap();
    let done = converge(&b, &a);
    assert_eq!(value(&b, 9), None, "a row of the buried life came back");
    assert_eq!(done.applied, 0);
    assert_eq!(done.left, 0, "a superseded row is not left");
    assert_eq!(done.superseded, 1);
}

// -- The page's other parts are skipped -------------------------------------------------

#[test]
fn the_pages_definitions_are_not_restored() {
    let (a, b, _d) = pair();
    use crate::IndexField;
    a.create_index_with(
        "shop",
        "orders",
        vec![IndexField::ascending("t")],
        false,
        crate::Enforcement::Local,
        Some("ttl_t".into()),
        Some(3600),
        None,
    )
    .unwrap();
    a.create_index("shop", "orders", vec![IndexField::ascending("u")], true, Some("uniq_u".into()))
        .unwrap();
    a.create_index("shop", "orders", vec![IndexField::ascending("k")], false, Some("ik".into()))
        .unwrap();
    // The requester has a same-name rival for `ik`, over another field.
    b.create_index(
        "shop",
        "orders",
        vec![IndexField::ascending("other")],
        false,
        Some("ik".into()),
    )
    .unwrap();
    let before = b.list_indexes("shop", "orders").unwrap();
    converge(&b, &a);
    let after = b.list_indexes("shop", "orders").unwrap();
    assert_eq!(
        serde_json::to_vec(&before).unwrap(),
        serde_json::to_vec(&after).unwrap(),
        "a Converge page changed the requester's definitions"
    );
}

#[test]
fn the_pages_carried_index_drops_are_not_applied() {
    let (a, b, _d) = pair();
    use crate::IndexField;
    b.create_index("shop", "orders", vec![IndexField::ascending("k")], false, Some("ik".into()))
        .unwrap();
    later();
    // The sender created and dropped an index of the same name: the drop is carried.
    a.create_index("shop", "orders", vec![IndexField::ascending("k")], false, Some("ik".into()))
        .unwrap();
    later();
    a.drop_index("shop", "orders", "ik").unwrap();
    let page = a.snapshot_page(None, Some(orders())).unwrap();
    assert!(!page.dropped_indexes.is_empty(), "premise: the page carries the drop");
    let mut progress = SnapshotProgress::of_collection_converging(orders());
    b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert_eq!(b.list_indexes("shop", "orders").unwrap().len(), 1, "a carried drop was applied");
}

#[test]
fn a_page_that_says_the_sender_dropped_the_collection_applies_nothing() {
    let (a, b, _d) = pair();
    a.insert(&coll(&a), doc! { "_id": 1, "v": 1 }).unwrap();
    pull(&b, &a);
    later();
    a.replace(&coll(&a), &DocId::Int64(1), doc! { "_id": 1, "v": 2 }, false).unwrap();
    let mut page = a.snapshot_page(None, Some(orders())).unwrap();
    page.dropped = Some(Stamp::new(Hlc::new(1, 0), a.node_id()));
    let mut progress = SnapshotProgress::of_collection_converging(orders());
    let done = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert!(done.sender_dropped);
    assert_eq!(done.applied, 0);
    assert_eq!(
        value(&b, 1),
        Some(1),
        "a page from a sender that dropped the collection applied rows"
    );
    assert_eq!(progress.pages(), 0, "the progress moved");
    assert!(b.get_collection("shop", "orders").is_ok(), "the requester's collection was dropped");
}

// -- Progress is never persisted, resumed or forgotten ----------------------------------

fn many(a: &Engine) {
    let ca = coll(a);
    for i in 0..(crate::SNAPSHOT_PAGE as i64 + 40) {
        a.insert(&ca, doc! { "_id": 1000 + i, "v": 1 }).unwrap();
    }
}

#[test]
fn a_converge_pull_persists_no_cursor() {
    let (a, b, _d) = pair();
    many(&a);
    // The requester holds every id (so rows are strictly-newer candidates and the page writes).
    pull(&b, &a);
    later();
    let ca = coll(&a);
    for i in 0..(crate::SNAPSHOT_PAGE as i64 + 40) {
        a.replace(&ca, &DocId::Int64(1000 + i), doc! { "_id": 1000 + i, "v": 2 }, false).unwrap();
    }
    let mut progress = SnapshotProgress::of_collection_converging(orders());
    let page = a.snapshot_page(None, Some(orders())).unwrap();
    assert!(page.next.is_some(), "premise: more than one page");
    let done = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert!(done.applied > 0, "premise: the page wrote");
    assert!(
        !b.snapshot_progress_recorded(a.node_id(), Some(orders())).unwrap(),
        "a Converge pull recorded a cursor"
    );
    assert!(b.snapshots_to_resume().unwrap().is_empty(), "a Converge cursor would be resumed");
}

#[test]
fn a_converge_completion_leaves_a_stored_all_cursor_in_place() {
    let (a, b, _d) = pair();
    many(&a);
    // An `All` pull that wrote a page and recorded its cursor.
    let mut all = SnapshotProgress::of_collection(orders());
    let first = a.snapshot_page(None, Some(orders())).unwrap();
    assert!(first.next.is_some(), "premise: more than one page");
    b.apply_snapshot_page(a.node_id(), &mut all, &first).unwrap();
    assert!(
        b.snapshot_progress_recorded(a.node_id(), Some(orders())).unwrap(),
        "premise: the All pull recorded its cursor"
    );
    // A Converge pull of the same scope runs to completion against the same peer.
    converge(&b, &a);
    assert!(
        b.snapshot_progress_recorded(a.node_id(), Some(orders())).unwrap(),
        "a Converge completion forgot the stored All cursor"
    );
}

// -- The young-stamp rule (design 13.2) -------------------------------------------------

/// A pair whose engines were both opened before the rows of the test are written, so the
/// reset-at-open horizons sit below every row.
fn young_pair() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    a.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    later();
    (a, b, [da, db])
}

#[test]
fn a_young_lost_insert_is_written_and_counted_created() {
    let (a, b, _d) = young_pair();
    a.insert(&coll(&a), doc! { "_id": 3, "v": 3 }).unwrap();
    let done = converge(&b, &a);
    assert_eq!(value(&b, 3), Some(3), "a young row this member holds nothing for was not written");
    assert_eq!(done.created, 1);
    assert_eq!(done.applied, 0, "created rows are not counted applied");
    assert_eq!(done.left, 0);
}

#[test]
fn a_row_at_or_below_the_receivers_own_horizon_is_left() {
    let (a, b, _d) = young_pair();
    a.insert(&coll(&a), doc! { "_id": 3, "v": 3 }).unwrap();
    // The receiver has collected tombstones up to a point beyond the row.
    b.test_set_tombstone_horizon(Hlc::new(crate::physical_now_ms() + 3_600_000, 0)).unwrap();
    let done = converge(&b, &a);
    assert_eq!(value(&b, 3), None);
    assert_eq!((done.created, done.left), (0, 1));
}

#[test]
fn a_row_at_or_below_the_senders_horizon_is_left() {
    let (a, b, _d) = young_pair();
    a.insert(&coll(&a), doc! { "_id": 3, "v": 3 }).unwrap();
    a.test_set_tombstone_horizon(Hlc::new(crate::physical_now_ms() + 3_600_000, 0)).unwrap();
    let done = converge(&b, &a);
    assert_eq!(value(&b, 3), None);
    assert_eq!((done.created, done.left), (0, 1));
}

#[test]
fn a_young_row_is_not_written_into_a_life_that_owes_a_sweep() {
    use redb::ReadableDatabase;
    let (a, b, _d) = young_pair();
    a.insert(&coll(&a), doc! { "_id": 3, "v": 3 }).unwrap();
    // A drop tombstone below the receiver's life with the floor not taken up to it: the
    // life owes ADR-231's sweep, so what it holds may include the life before.
    let created = coll(&b).created;
    let stamp = kimmy_core::Stamp::new(Hlc::new(created.wall_ms - 1, 0), b.node_id());
    {
        let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
        txn.open_table(crate::tables::COLLECTIONS_DROPPED)
            .unwrap()
            .insert(orders().0, crate::codec::oplog_key(&stamp).as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    assert!(coll(&b).incarnation_floor.is_none(), "premise");
    let done = converge(&b, &a);
    assert_eq!(value(&b, 3), None, "a row was written while a sweep was owed");
    assert_eq!((done.created, done.left), (0, 1));
    let _ = b.db().begin_read();
}

#[test]
fn a_young_row_is_not_written_into_a_life_that_owes_a_sweep_for_a_database_drop() {
    let (a, b, _d) = young_pair();
    a.insert(&coll(&a), doc! { "_id": 3, "v": 3 }).unwrap();
    let created = coll(&b).created;
    let stamp = kimmy_core::Stamp::new(Hlc::new(created.wall_ms - 1, 0), b.node_id());
    {
        let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
        txn.open_table(crate::tables::COLLECTIONS_DROPPED)
            .unwrap()
            .insert(
                kimmy_core::database_drop_id("shop").0,
                crate::codec::oplog_key(&stamp).as_slice(),
            )
            .unwrap();
        txn.commit().unwrap();
    }
    let done = converge(&b, &a);
    assert_eq!(value(&b, 3), None, "the database tombstone was not consulted");
    assert_eq!((done.created, done.left), (0, 1));
}

#[test]
fn a_created_row_maintains_the_indexes_like_any_replicated_row() {
    let (a, b, _d) = young_pair();
    use crate::IndexField;
    for e in [&a, &b] {
        e.create_index(
            "shop",
            "orders",
            vec![IndexField::ascending("v")],
            false,
            Some("by_v".into()),
        )
        .unwrap();
    }
    a.insert(&coll(&a), doc! { "_id": 3, "v": 33 }).unwrap();
    let done = converge(&b, &a);
    assert_eq!(done.created, 1);
    let id = crate::IndexMeta::derive_id("by_v");
    let entries = crate::index::scan_range(
        &b,
        crate::WalkScope::Request,
        orders(),
        id,
        &[],
        None,
        crate::index::Unkeyed::Include,
    )
    .unwrap();
    assert_eq!(entries.len(), 1, "the index does not hold the created row");
}
