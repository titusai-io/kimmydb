//! The tests the B5 build review's M1 found missing: each kills a mutant that
//! survived every branch suite (`docs/adr-240-mutation-rows.md`, "M1 of the
//! build review"). Retention of every record and of the change log, the change
//! log across a pull's pages, and a records-only page cut inside a records phase.
//! The fifth, a replay page's F3 records, is in
//! `kimmy-cluster/src/transport/replay_tests.rs`.

use std::cmp::Ordering;
use std::sync::Arc;

use bson::{Bson, Document, doc};
use kimmy_core::{DocId, Fingerprint, Hlc, NodeId, ScopeHash, Stamp};

use crate::idempotency::{
    self, IdempotencyRecord, Keyed, KeyedWrite, ModifyRecorded, RecordOutcome, RecordState,
    ReturnedImage,
};
use crate::{
    Candidates, CollectionMeta, Engine, ModifySpec, RetentionPolicy, SNAPSHOT_PAGE, SnapshotPage,
    SnapshotProgress, tables,
};

const DAY: u64 = 24 * 60 * 60;
const DAY_MS: u64 = DAY * 1000;

struct Inc;

impl ModifySpec for Inc {
    fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
        Ok(doc.contains_key("n"))
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.insert("n", Bson::Int64(doc.get_i64("n").map_err(|e| e.to_string())? + 1));
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

fn open(dir: &tempfile::TempDir, name: &str) -> Engine {
    Engine::open(&dir.path().join(format!("{name}.redb"))).unwrap()
}

fn keyed(n: u8) -> KeyedWrite {
    KeyedWrite::new(ScopeHash([n; 32]), Fingerprint([n; 32]))
}

fn key_of(i: u64) -> KeyedWrite {
    let mut scope = [0u8; 32];
    scope[..8].copy_from_slice(&i.to_be_bytes());
    KeyedWrite::new(ScopeHash(scope), Fingerprint([1; 32]))
}

fn fresh<T: std::fmt::Debug>(answer: Keyed<T>) -> T {
    match answer {
        Keyed::Fresh(value) => value,
        other => panic!("expected a fresh write, got {other:?}"),
    }
}

fn stamp(ms: u64, counter: u16, node: u8) -> Stamp {
    Stamp::new(Hlc::new(ms, counter), NodeId::from_bytes([node; 16]))
}

fn table_len(engine: &Engine, table: redb::TableDefinition<&[u8], &[u8]>) -> u64 {
    use redb::{ReadableDatabase, ReadableTableMetadata};
    let txn = engine.db().begin_read().unwrap();
    txn.open_table(table).unwrap().len().unwrap()
}

fn n_of(engine: &Engine, coll: &CollectionMeta, id: i64) -> Option<i64> {
    engine.get(coll, &DocId::Int64(id)).unwrap().and_then(|d| d.get_i64("n").ok())
}

fn take(to: &Engine, from: &Engine, progress: &mut SnapshotProgress) -> SnapshotPage {
    let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
    to.apply_snapshot_page(from.node_id(), progress, &page).unwrap();
    page
}

fn finish(to: &Engine, from: &Engine, progress: &mut SnapshotProgress) {
    while !progress.is_complete() {
        take(to, from, progress);
    }
}

fn retry_must_replay(m: &Engine, cm: &CollectionMeta, k: &KeyedWrite, id: i64) {
    let before = n_of(m, cm, id);
    let held = m.idempotency_record(cm.id, &k.scope).unwrap();
    let retry = m.modify_where_keyed(cm, &Candidates::Scan, &Inc, Some(1), k).unwrap();
    let after = n_of(m, cm, id);
    assert!(
        held.is_some() && matches!(retry, Keyed::Replayed(_)) && after == before,
        "the receiver holds the effect (n = {before:?}) without the record ({held:?}); the \
         retry answered {retry:?} and n is now {after:?}"
    );
}

/// A collection on `y` of three snapshot pages of documents, `target` holding
/// `n: 10`.
fn three_pages(y: &Engine, target: i64) -> CollectionMeta {
    let cy = y.create_collection("shop", "orders").unwrap();
    let docs = (1..(3 * SNAPSHOT_PAGE as i64))
        .map(|i| {
            if i == target {
                doc! { "_id": i, "n": 10i64 }
            } else {
                doc! { "_id": i, "v": i }
            }
        })
        .collect();
    y.insert_many(&cy, docs).unwrap();
    cy
}

/// X07 and X05 (N3) of the build review's M1. After every record has passed its
/// window, one retention pass leaves the three tables empty, the change log
/// keeping only where it began: local single- and multi-commit records, a merged
/// record whose `first` and `last` are of two origins, a frame-stored record with
/// a carrying row of a third origin, a page-stored one, an undecodable one (a
/// newer format) with one row, a delete, a `find_and_modify`, and an orphan row.
/// Retention reading only the first origin with rows leaves the others' records;
/// a change log never trimmed keeps its rows.
#[test]
fn everything_expires_and_nothing_is_left() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open(&dir, "a"));
    let coll = engine.create_collection("shop", "orders").unwrap();
    fresh(engine.insert_keyed(&coll, doc! { "a": 1 }, &keyed(1)).unwrap());
    fresh(
        engine.insert_many_keyed(&coll, vec![doc! { "b": 1 }, doc! { "b": 2 }], &keyed(2)).unwrap(),
    );
    engine.set_multi_chunk_docs(1);
    let docs: Vec<Document> = (0..3).map(|i| doc! { "_id": i as i64, "n": 0i64 }).collect();
    engine.insert_many(&coll, docs).unwrap();
    let mover = Arc::clone(&engine);
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::BetweenChunks, move || {
        let now = crate::physical_now_ms() + 10_000;
        mover.advance_clock_past(&Stamp::new(Hlc::new(now, 0), mover.node_id()));
    });
    fresh(engine.modify_where_keyed(&coll, &Candidates::Scan, &Inc, None, &keyed(3)).unwrap());
    crate::sync::race_hooks::clear();
    fresh(engine.delete_keyed(&coll, &DocId::Int64(0), None, &keyed(4)).unwrap());
    fresh(
        engine
            .find_and_modify_keyed(&coll, &Candidates::Scan, &Inc, &keyed(5), ReturnedImage::After)
            .unwrap(),
    );
    // From peers: a merge of two origins and a carrying row of a third; a
    // page-stored record; an undecodable one with one row.
    let now = crate::physical_now_ms();
    let peer = |first: Stamp, last: Stamp, state| IdempotencyRecord {
        fingerprint: Fingerprint([6; 32]),
        first,
        last,
        origin: last.node,
        state,
        contested: false,
        outcome: RecordOutcome::Modify(ModifyRecorded::default()),
    };
    {
        let txn = engine.begin_write(crate::WriterHolder::Replication).unwrap();
        let s = ScopeHash([6; 32]);
        let a =
            peer(stamp(now - 5_000, 0, 0xA1), stamp(now - 4_000, 0, 0xA1), RecordState::Applying);
        let b =
            peer(stamp(now - 3_000, 0, 0xB2), stamp(now - 2_000, 0, 0xB2), RecordState::Applying);
        engine.store_record(&txn, coll.id, &s, &a, Some(a.first)).unwrap();
        let third = Some(stamp(now - 1_000, 0, 0xC3));
        engine.store_record(&txn, coll.id, &s, &b, third).unwrap();
        let p = peer(stamp(now - 6_000, 0, 0xD4), stamp(now - 6_000, 0, 0xD4), RecordState::Done);
        let page = ScopeHash([7; 32]);
        engine.store_record(&txn, coll.id, &page, &p, None).unwrap();
        let mut newer = p.encode();
        newer[0] = idempotency::RECORD_FORMAT + 1;
        txn.open_table(tables::IDEMPOTENCY)
            .unwrap()
            .insert((coll.id.0, [8u8; 32].as_slice()), newer.as_slice())
            .unwrap();
        let mut row = [0u8; 40];
        row[..8].copy_from_slice(&coll.id.0.to_be_bytes());
        row[8..].copy_from_slice(&[8u8; 32]);
        txn.open_table(tables::IDEMPOTENCY_BY_STAMP)
            .unwrap()
            .insert(idempotency::row_key(&stamp(now - 7_000, 0, 0xE5)).as_slice(), row.as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    // An orphan row: a record gone, its rows left.
    fresh(engine.insert_keyed(&coll, doc! { "z": 1 }, &keyed(9)).unwrap());
    assert!(engine.remove_idempotency_record_for_test(coll.id, &keyed(9).scope).unwrap());

    let records = engine.idempotency_records().unwrap();
    let rows = table_len(&engine, tables::IDEMPOTENCY_BY_STAMP);
    let log = table_len(&engine, tables::IDEMPOTENCY_CHANGES);
    assert!(records >= 8 && rows >= 10 && log >= 8, "premise: {records} {rows} {log}");
    let policy = RetentionPolicy::new(DAY, DAY);
    let inside = engine.collect_garbage_at(now, policy).unwrap();
    assert_eq!(inside.records_removed, 0, "a pass inside every window removes nothing");
    let past = engine.collect_garbage_at(now + 2 * DAY_MS + 60_000, policy).unwrap();
    let records = engine.idempotency_records().unwrap();
    let rows = table_len(&engine, tables::IDEMPOTENCY_BY_STAMP);
    let log = table_len(&engine, tables::IDEMPOTENCY_CHANGES);
    assert!(past.records_removed >= 8, "{past:?}");
    assert_eq!((records, rows, log), (0, 0, 1), "only the log's beginning stays");
}

/// The change log's trim (X05 of the build review's M1): a pass drops the log's
/// rows older than the record window and keeps the newer ones and where the log
/// began, so a page base cut before the pass still reads what changed since.
#[test]
fn retention_trims_the_change_log_below_the_window_and_keeps_its_beginning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(&dir, "a");
    let coll = engine.create_collection("shop", "orders").unwrap();
    let (_, old) = fresh(engine.insert_keyed(&coll, doc! { "v": 1 }, &keyed(1)).unwrap());
    let later = Stamp::new(Hlc::new(old.hlc.wall_ms + 10_000, 0), engine.node_id());
    engine.advance_clock_past(&later);
    fresh(engine.insert_keyed(&coll, doc! { "v": 2 }, &keyed(2)).unwrap());
    assert_eq!(table_len(&engine, tables::IDEMPOTENCY_CHANGES), 3, "premise: begin and two rows");
    let begun = {
        use redb::ReadableDatabase;
        let txn = engine.db().begin_read().unwrap();
        idempotency::changes_begin(&txn).unwrap().expect("premise: begun")
    };
    // The cutoff between the two changes.
    let policy = RetentionPolicy::new(DAY, DAY);
    engine.collect_garbage_at(old.hlc.wall_ms + DAY_MS + 5_000, policy).unwrap();
    assert_eq!(table_len(&engine, tables::IDEMPOTENCY_CHANGES), 2, "the older row went");
    let kept = {
        use redb::ReadableDatabase;
        let txn = engine.db().begin_read().unwrap();
        (
            idempotency::changes_begin(&txn).unwrap(),
            idempotency::changes_high_water(&txn).unwrap().map(|s| s.hlc),
        )
    };
    assert_eq!(kept.0, Some(begun), "where the log began, and its epoch, stay");
    assert!(kept.1.is_some_and(|high| high >= later.hlc), "the newer row stays: {kept:?}");
}

/// N1 of the build review's M1. A retention pass inside the record window, run
/// while a pull is part-way, keeps the change-log rows the next page's delta
/// reads: a pass that trimmed the whole log would leave the next page with an
/// empty delta, and the page would bring the effect without its record.
#[test]
fn a_retention_pass_during_a_pull_keeps_the_log_the_next_page_reads() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    // A sender whose change log has begun, so the next page is answered by the
    // delta from the base page 1 cut: a log that has not begun gives the zero base,
    // whose page is refused and restarts the records phase, which would carry the
    // record whatever the log holds.
    idempotency::begin_log_for_test(&y).unwrap();
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    assert!(progress.after().unwrap().records_base.unwrap().epoch.is_some(), "premise: an epoch");
    let k = keyed(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    y.collect_garbage(RetentionPolicy::new(DAY, DAY)).unwrap();
    finish(&x, &y, &mut progress);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&x, &cx, target), Some(11), "premise: the page brought the effect");
    retry_must_replay(&x, &cx, &k, target);
}

/// M13a of the build review's M1. A records-only page cut while the cursor stands
/// inside a collection's records phase keeps that position: the rest of the phase
/// still comes. Dropping it skipped the rest of the collection's records, and its
/// documents came without them.
#[test]
fn a_records_only_page_inside_a_records_phase_keeps_the_phase() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let c1 = y.create_collection("shop", "a").unwrap();
    y.insert(&c1, doc! { "_id": 1i64 }).unwrap();
    let c2 = y.create_collection("shop", "b").unwrap();
    let n = idempotency::RECORD_PAGE as u64 + 20;
    for i in 0..n {
        fresh(y.insert_keyed(&c2, doc! { "_id": i as i64 }, &key_of(1_000 + i)).unwrap());
    }
    let mut progress = SnapshotProgress::whole_database();
    // Pages until the cursor stands inside c2's records phase.
    loop {
        take(&x, &y, &mut progress);
        let at = progress.after().cloned().unwrap();
        if at.collection == c2.id && at.records.as_ref().is_some_and(|r| !r.is_empty()) {
            break;
        }
        assert!(!progress.is_complete());
    }
    // A delta too large for one page, so the next page is records-only.
    let one = y.idempotency_record(c2.id, &key_of(1_000).scope).unwrap().unwrap().encode().len();
    y.set_record_delta_budget_for_test(Some((2 * (one + 96), idempotency::RECORD_DELTA_ROWS)));
    for i in 0..5u64 {
        fresh(y.insert_keyed(&c1, doc! { "_id": 100 + i as i64 }, &key_of(i)).unwrap());
    }
    finish(&x, &y, &mut progress);
    y.set_record_delta_budget_for_test(None);
    assert_eq!(
        x.idempotency_records().unwrap(),
        y.idempotency_records().unwrap(),
        "every record of the records phase arrived"
    );
}
