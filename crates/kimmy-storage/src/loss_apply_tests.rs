//! A loss entry applied to the state of its document, or parked for a document with no row, and folded in by the
//! write that gives it one (the Step 4 design, v4).

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

fn orders() -> kimmy_core::CollectionId {
    kimmy_core::CollectionId::derive("shop", "orders")
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// Two members of one `field` collection, the second holding only its definitions.
fn pair() -> (Engine, Engine, CollectionMeta, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    let coll = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    (a, b, coll, [da, db])
}

/// The record `a` could have detected for the document `1`, and the entry it would append for it.
fn a_loss_entry(a: &Engine, coll: &CollectionMeta) -> (LossKey, OplogEntry) {
    let writer = WriterId { node: a.node_id(), epoch: a.epoch() };
    let loser = MStamp { hlc: Hlc::new(crate::physical_now_ms(), 0), writer };
    let key = LossKey::field(LossKind::Clash, Path::flat("x").unwrap(), loser);
    let value = LossValue::Value(Value::Int64(7));
    let entry = OplogEntry {
        stamp: Stamp::new(Hlc::new(crate::physical_now_ms() + 1, 0), a.node_id(), a.epoch()),
        kind: OpKind::LossRecord,
        collection: coll.id,
        doc_id: Some(DocId::Int64(1)),
        body: Some(MergeState::encode_loss_record(&key, &value)),
        meta: None,
    };
    (key, entry)
}

/// The state stored for the one document of `from`, as its page carries it.
fn stored_state(from: &Engine) -> MergeState {
    let page = from.snapshot_page(None, Some(orders())).unwrap();
    assert_eq!(page.documents.len(), 1, "premise: one row");
    crate::field_write::load(page.documents[0].meta.as_deref()).unwrap()
}

fn parked_rows(e: &Engine) -> usize {
    let txn = e.db().begin_read().unwrap();
    redb::ReadableTable::iter(&txn.open_table(tables::LOSS_PENDING).unwrap()).unwrap().count()
}

#[test]
fn a_loss_entry_for_a_document_with_a_row_is_applied_to_its_state_and_parks_nothing() {
    let (a, b, coll, _d) = pair();
    a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let (key, entry) = a_loss_entry(&a, &coll);
    assert!(!stored_state(&b).losses.contains_key(&key), "premise");

    b.apply_batch(std::slice::from_ref(&entry)).unwrap();

    assert!(stored_state(&b).losses.contains_key(&key), "the record is in the stored state");
    assert_eq!(parked_rows(&b), 0, "nothing is parked for a document that has a row");
}

/// Take the loss records out of the stored state of the one document, as if the entry that brought them had been
/// kept in the oplog and not applied.
fn forget_the_records(e: &Engine, coll: &CollectionMeta) {
    let key = crate::docs::doc_key(&DocId::Int64(1)).unwrap();
    let txn = e.begin_write(crate::engine::WriterHolder::Write).unwrap();
    {
        let mut docs = txn.open_table(tables::DOCS).unwrap();
        let raw = docs.get((coll.id.0, key.as_slice())).unwrap().unwrap().value().to_vec();
        let held = crate::codec::decode_doc_record_with_meta(&raw).unwrap();
        let mut state = crate::field_write::load(held.meta.as_deref()).unwrap();
        state.losses.clear();
        let mut stored = kimmy_core::DocRecord::live(held.stamp, held.body.clone());
        stored.meta = Some(state.encode_compact());
        crate::live_count::put_record(
            &txn,
            &mut docs,
            coll.id.0,
            &key,
            &crate::codec::encode_doc_record(&stored),
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

#[test]
fn a_loss_entry_applied_alone_is_committed_as_the_state_move_it_is() {
    // C0's outcome covers a move of the state that leaves the document as it was: the single-entry apply commits
    // it. The entry is already in the oplog (so nothing is owed to it) and the record is not in the state.
    let (a, b, coll, _d) = pair();
    a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let (key, entry) = a_loss_entry(&a, &coll);
    let cb = b.get_collection("shop", "orders").unwrap();
    b.apply_batch(std::slice::from_ref(&entry)).unwrap();
    forget_the_records(&b, &cb);
    assert!(
        !stored_state(&b).losses.contains_key(&key),
        "premise: the entry is held and the record is not"
    );

    let applied = b.apply_remote(&cb, &entry).unwrap();

    assert!(!applied, "the document did not move");
    assert!(stored_state(&b).losses.contains_key(&key), "and the record was written");
}

#[test]
fn a_loss_record_moves_neither_the_etag_nor_a_conditional_write_over_it() {
    let (a, b, coll, _d) = pair();
    let id = a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    let (version, _) = b.get_versioned(&cb, &id).unwrap().unwrap();
    let kimmy_core::DocVersion::Etag(stamp, hash) = version else { panic!("an etag: {version:?}") };
    let (key, entry) = a_loss_entry(&a, &coll);

    b.apply_batch(std::slice::from_ref(&entry)).unwrap();

    assert!(stored_state(&b).losses.contains_key(&key), "premise: the state changed");
    assert_eq!(
        b.get_versioned(&cb, &id).unwrap().unwrap().0,
        version,
        "the etag is the one it was"
    );
    let written = b
        .replace_if_state(&cb, &id, doc! { "_id": 1, "x": 2 }, false, Some(stamp), Some(hash))
        .expect("an If-Match over the etag read before the record arrived");
    assert!(
        written.matched && written.modified,
        "and the write is made: the record did not fail it"
    );
}

#[test]
fn a_loss_entry_for_a_document_with_no_row_is_parked_and_the_first_entry_of_the_document_folds_it_in()
 {
    let (a, b, coll, _d) = pair();
    a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    let (key, entry) = a_loss_entry(&a, &coll);

    b.apply_batch(std::slice::from_ref(&entry)).unwrap();

    assert_eq!(parked_rows(&b), 1, "parked: there is no row to hold it");
    let cb = b.get_collection("shop", "orders").unwrap();
    assert_eq!(b.get(&cb, &DocId::Int64(1)).unwrap(), None, "and no row was made of it");

    b.apply_batch(&everything(&a)).unwrap();

    assert!(
        stored_state(&b).losses.contains_key(&key),
        "the document's entry brought the row, and the record with it"
    );
    assert_eq!(parked_rows(&b), 0, "and nothing stays parked");
}

#[test]
fn a_local_write_that_gives_a_document_its_row_folds_in_what_was_parked() {
    let (a, b, coll, _d) = pair();
    let (key, entry) = a_loss_entry(&a, &coll);
    b.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert_eq!(parked_rows(&b), 1, "premise");
    let cb = b.get_collection("shop", "orders").unwrap();

    b.insert(&cb, doc! { "_id": 1, "x": 5 }).unwrap();

    assert!(stored_state(&b).losses.contains_key(&key), "the record is in the new row's state");
    assert_eq!(parked_rows(&b), 0);
}

#[test]
fn a_parked_record_whose_window_has_closed_is_deleted_by_the_retention_pass_and_is_never_folded() {
    let (a, b, coll, _d) = pair();
    a.insert(&coll, doc! { "_id": 1, "x": 1 }).unwrap();
    let (key, entry) = a_loss_entry(&a, &coll);
    b.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert_eq!(parked_rows(&b), 1, "premise");

    // A pass at a moment past the window (R after the loser's wall time).
    let past = crate::physical_now_ms() + crate::field_write::DEFAULT_JOIN_RETENTION_MS + 60_000;
    b.collect_garbage_at(past, crate::RetentionPolicy::new(0, u64::MAX)).unwrap();
    assert_eq!(parked_rows(&b), 0, "the pass deleted it");

    b.apply_batch(&everything(&a)).unwrap();
    assert!(
        !stored_state(&b).losses.contains_key(&key),
        "so the row it came to hold has no record"
    );
}

/// Park the records of `walls` (entry numbers 1, 2, ...) for the document `k` of collection 7 at `parked_at`, with a
/// window of `retention`; the walls the loser stamps carry. Returns, per wall, whether the row was parked.
fn park_all(
    e: &Engine,
    writer: WriterId,
    walls: &[u64],
    retention: u64,
    parked_at: u64,
) -> Vec<bool> {
    walls
        .iter()
        .enumerate()
        .map(|(n, wall)| {
            let loser = MStamp { hlc: Hlc::new(*wall, 0), writer };
            let key = LossKey::field(LossKind::Clash, Path::flat("x").unwrap(), loser);
            let txn = e.begin_write(crate::engine::WriterHolder::Write).unwrap();
            let body = MergeState::encode_loss_record(&key, &LossValue::Value(Value::Int64(1)));
            let parked =
                crate::loss_pending::park(&txn, 7, b"k", &[n as u8], &body, retention, parked_at)
                    .unwrap();
            txn.commit().unwrap();
            parked
        })
        .collect()
}

fn the_key_at(writer: WriterId, wall: u64) -> LossKey {
    LossKey::field(
        LossKind::Clash,
        Path::flat("x").unwrap(),
        MStamp { hlc: Hlc::new(wall, 0), writer },
    )
}

#[test]
fn a_parked_record_whose_window_has_closed_is_not_folded_and_one_that_never_had_a_window_is_not_parked()
 {
    let (_a, b, _coll, _d) = pair();
    let writer = WriterId { node: b.node_id(), epoch: b.epoch() };
    // Retention 500_000, parked at 1_000_000: the walls 920_000 and 990_000 close at 1_420_000 and 1_490_000; the wall
    // 400_000 closed at 900_000, before it came.
    let parked = park_all(&b, writer, &[920_000, 990_000, 400_000], 500_000, 1_000_000);
    assert_eq!(
        parked,
        [true, true, false],
        "an entry already out of the window when it comes is not parked"
    );
    assert_eq!(parked_rows(&b), 2, "two rows, and not three");
    let mut state = MergeState::default();
    state.vv.insert(writer, Hlc::new(999_000, 0));

    let txn = b.begin_write(crate::engine::WriterHolder::Write).unwrap();
    let options = kimmy_core::merge_state::JoinOptions::default();
    let entered =
        crate::loss_pending::fold_into(&txn, 7, b"k", &mut state, &options, None, 1_420_000)
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
    txn.commit().unwrap();

    assert_eq!(
        entered,
        vec![the_key_at(writer, 990_000)],
        "only the one still inside its window enters"
    );
    assert_eq!(parked_rows(&b), 0, "and the fold took every row it read");
}

#[test]
fn a_parked_record_at_or_below_the_floor_is_not_folded() {
    let (_a, b, _coll, _d) = pair();
    let writer = WriterId { node: b.node_id(), epoch: b.epoch() };
    park_all(&b, writer, &[930_000, 940_000, 990_000], 500_000, 1_000_000);
    let mut state = MergeState::default();
    state.vv.insert(writer, Hlc::new(999_000, 0));

    let txn = b.begin_write(crate::engine::WriterHolder::Write).unwrap();
    let options = kimmy_core::merge_state::JoinOptions::default();
    let entered = crate::loss_pending::fold_into(
        &txn,
        7,
        b"k",
        &mut state,
        &options,
        Some(Hlc::new(940_000, 0)),
        1_100_000,
    )
    .unwrap()
    .into_iter()
    .map(|(key, _)| key)
    .collect::<Vec<_>>();
    txn.commit().unwrap();

    assert_eq!(
        entered,
        vec![the_key_at(writer, 990_000)],
        "the ones at or below the floor are not folded, the one above is"
    );
}
