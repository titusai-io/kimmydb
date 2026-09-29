//! The violations table against the oplog it indexes (ADR-200): after every
//! step the table's rows are exactly the violation entries the oplog holds,
//! and `through` never claims more than the oplog's tail.

use std::collections::BTreeSet;
use std::time::Duration;

use bson::doc;
use kimmy_core::{Hlc, NodeId, OpKind, OplogEntry, Stamp};
use redb::{ReadableDatabase, ReadableTable};

use crate::engine::Engine;
use crate::gc::RetentionPolicy;
use crate::violations_table::{SENTINEL, through_of};
use crate::watch::ExamineBudget;
use crate::{CollectionMeta, WalkScope, codec, tables};

fn store() -> (Engine, CollectionMeta, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    engine.create_collection("db", "c").unwrap();
    engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
    let coll = engine.get_collection("db", "c").unwrap();
    (engine, coll, dir)
}

fn field(path: &str) -> crate::IndexField {
    crate::IndexField { path: path.into(), descending: false }
}

/// Write `n` violations: a local document and a peer's with the same email.
fn violate(engine: &Engine, coll: &CollectionMeta, tag: &str, n: usize) {
    for i in 0..n {
        let email = format!("{tag}{i}@x");
        engine.insert(coll, doc! { "_id": format!("{tag}l{i}"), "email": email.clone() }).unwrap();
        let entry = OplogEntry {
            stamp: Stamp::new(Hlc::new(9_000 + i as u64, 0), NodeId::generate()),
            kind: OpKind::Insert,
            collection: coll.id,
            doc_id: Some(kimmy_core::DocId::String(format!("{tag}r{i}"))),
            body: Some(
                bson::serialize_to_vec(&doc! { "_id": format!("{tag}r{i}"), "email": email })
                    .unwrap(),
            ),
        };
        assert!(engine.apply_remote(coll, &entry).unwrap());
    }
}

/// `(collection, stamp key)` of every violation entry in the oplog.
fn in_oplog(engine: &Engine) -> BTreeSet<Vec<u8>> {
    let txn = engine.db().begin_read().unwrap();
    let oplog = txn.open_table(tables::OPLOG).unwrap();
    oplog
        .iter()
        .unwrap()
        .filter_map(|row| {
            let (key, value) = row.unwrap();
            match codec::decode_oplog_kind_and_collection(value.value()) {
                Ok((OpKind::UniqueViolation, coll)) => {
                    Some(crate::violations_table::row_key(coll, key.value()))
                }
                _ => None,
            }
        })
        .collect()
}

fn in_table(engine: &Engine) -> (BTreeSet<Vec<u8>>, Option<Vec<u8>>) {
    let txn = engine.db().begin_read().unwrap();
    let table = txn.open_table(tables::UNIQUE_VIOLATIONS).unwrap();
    let through = through_of(&table).unwrap();
    let rows = table
        .iter()
        .unwrap()
        .map(|r| r.unwrap().0.value().to_vec())
        .filter(|k| k.as_slice() != SENTINEL)
        .collect();
    (rows, through)
}

fn tail(engine: &Engine) -> Option<Vec<u8>> {
    let txn = engine.db().begin_read().unwrap();
    let oplog = txn.open_table(tables::OPLOG).unwrap();
    oplog.last().unwrap().map(|(k, _)| k.value().to_vec())
}

/// The structural check: no row the oplog lacks, none missing at or below
/// `through`, and `through` not past the tail.
fn assert_in_step(engine: &Engine, what: &str) {
    let (rows, through) = in_table(engine);
    let oplog = in_oplog(engine);
    let tail = tail(engine);
    if let (Some(through), Some(tail)) = (&through, &tail) {
        assert!(through <= tail, "{what}: through is past the tail");
    }
    let stale: Vec<_> = rows.difference(&oplog).collect();
    assert!(stale.is_empty(), "{what}: rows whose entry is gone: {stale:?}");
    let missing: Vec<_> = oplog
        .difference(&rows)
        .filter(|k| {
            let stamp = crate::violations_table::stamp_of(k).unwrap();
            through.as_deref().is_some_and(|t| stamp <= t)
        })
        .collect();
    assert!(missing.is_empty(), "{what}: violations at or below through with no row: {missing:?}");
    if engine.violations_table_ready() {
        assert_eq!(rows, oplog, "{what}: a ready table is the oplog's violations");
    }
}

fn same_answer(engine: &Engine, coll: &CollectionMeta, what: &str) {
    let table = engine.live_unique_violations(coll, None, WalkScope::Request).unwrap();
    let oplog = engine.live_unique_violations_from_oplog(coll, None, WalkScope::Request).unwrap();
    assert_eq!(table, oplog, "{what}: the table and the oplog walk answer alike");
}

fn wipe_table(engine: &Engine) {
    let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
    {
        let mut table = txn.open_table(tables::UNIQUE_VIOLATIONS).unwrap();
        let keys: Vec<Vec<u8>> =
            table.iter().unwrap().map(|r| r.unwrap().0.value().to_vec()).collect();
        for key in keys {
            table.remove(key.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
}

fn drain(engine: &Engine, rows: u64) -> u64 {
    let mut steps = 0;
    loop {
        let budget = ExamineBudget { time: Duration::from_secs(60), rows };
        steps += 1;
        assert!(steps < 10_000, "the backfill must finish");
        if engine.violations_backfill_step(budget).unwrap().done {
            return steps;
        }
    }
}

#[test]
fn a_new_store_is_ready_and_the_table_follows_every_write() {
    let (engine, coll, _dir) = store();
    assert!(engine.violations_table_ready());
    assert_in_step(&engine, "empty");
    violate(&engine, &coll, "a", 3);
    assert_in_step(&engine, "three violations");
    same_answer(&engine, &coll, "three violations");
    assert_eq!(in_table(&engine).0.len(), 3);
    engine.insert(&coll, doc! { "_id": "plain", "email": "plain@x" }).unwrap();
    assert_in_step(&engine, "a plain write");
}

#[test]
fn a_rewind_removes_the_rows_of_the_entries_it_discards_and_lowers_through() {
    let (engine, coll, _dir) = store();
    violate(&engine, &coll, "a", 2);
    std::thread::sleep(Duration::from_millis(20));
    let mark = Hlc::new(crate::physical_now_ms(), u16::MAX);
    std::thread::sleep(Duration::from_millis(20));
    violate(&engine, &coll, "b", 2);
    engine.collect_garbage(RetentionPolicy::new(3_600, 3_600)).unwrap();
    assert_in_step(&engine, "before the rewind");
    assert_eq!(in_table(&engine).0.len(), 4);

    engine.rewind_to(mark).unwrap();
    assert_in_step(&engine, "after the rewind");
    assert_eq!(in_table(&engine).0.len(), 2, "the later violations' rows went with them");
    same_answer(&engine, &coll, "after the rewind");
}

#[test]
fn retention_removes_rows_with_their_entries_and_through_follows_it() {
    let (engine, coll, _dir) = store();
    violate(&engine, &coll, "a", 2);
    std::thread::sleep(Duration::from_millis(30));
    let middle = crate::physical_now_ms();
    std::thread::sleep(Duration::from_millis(30));
    violate(&engine, &coll, "b", 2);
    // Only what is older than `middle` expires.
    engine.collect_garbage_at(middle + 1_000, RetentionPolicy::new(1, 3_600)).unwrap();
    assert_in_step(&engine, "after retention");
    assert_eq!(in_table(&engine).0.len(), 2, "the newer violations' rows stay");
    let (_, through) = in_table(&engine);
    assert_eq!(through, tail(&engine), "a pass that removed entries advances through");
    same_answer(&engine, &coll, "after retention");
}

#[test]
fn a_dropped_collections_rows_are_not_another_collections() {
    let (engine, coll, _dir) = store();
    violate(&engine, &coll, "a", 2);
    engine.drop_collection("db", "c").unwrap();
    engine.create_collection("db", "d").unwrap();
    engine.create_index("db", "d", vec![field("email")], true, None).unwrap();
    let other = engine.get_collection("db", "d").unwrap();
    assert_in_step(&engine, "after the drop");
    assert!(engine.live_unique_violations(&other, None, WalkScope::Request).unwrap().is_empty());
    same_answer(&engine, &other, "the new collection");
}

#[test]
fn a_table_an_older_build_left_behind_is_backfilled_in_steps_and_ready_only_after_the_last() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let coll = {
        let (engine, coll, _keep) = {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("db", "c").unwrap();
            engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
            let coll = engine.get_collection("db", "c").unwrap();
            (engine, coll, ())
        };
        violate(&engine, &coll, "a", 3);
        engine.insert(&coll, doc! { "_id": "p", "email": "p@x" }).unwrap();
        violate(&engine, &coll, "b", 2);
        // What a build that does not know the table leaves: violations with no
        // rows, and no `through`.
        wipe_table(&engine);
        engine.close().unwrap();
        coll
    };
    let engine = Engine::open(&path).unwrap();
    assert!(!engine.violations_table_ready(), "a store the table does not describe is not ready");
    // The route answers from the oplog meanwhile.
    let before = engine.violations_snapshot();
    let walked = engine.live_unique_violations(&coll, None, WalkScope::Request).unwrap();
    assert_eq!(walked.len(), 5);
    assert_eq!(engine.violations_snapshot().calls_from_oplog, before.calls_from_oplog + 1);

    let mut steps = 0;
    loop {
        let budget = ExamineBudget { time: Duration::from_secs(60), rows: 4 };
        let step = engine.violations_backfill_step(budget).unwrap();
        steps += 1;
        assert_in_step(&engine, "mid-backfill");
        assert_eq!(engine.violations_table_ready(), step.done);
        if step.done {
            break;
        }
        assert!(steps < 200);
    }
    assert!(steps > 1, "the row budget split the scan: {steps}");
    assert_eq!(in_table(&engine).0.len(), 5);
    same_answer(&engine, &coll, "after the backfill");
    let snap = engine.violations_snapshot();
    assert!(snap.ready && snap.backfilled_rows > 0);
    let before = snap.calls_from_table;
    engine.live_unique_violations(&coll, None, WalkScope::Request).unwrap();
    assert_eq!(engine.violations_snapshot().calls_from_table, before + 1);
}

#[test]
fn a_backfill_that_stopped_resumes_from_through_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let coll = {
        let engine = Engine::open(&path).unwrap();
        engine.create_collection("db", "c").unwrap();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        violate(&engine, &coll, "a", 6);
        wipe_table(&engine);
        engine.close().unwrap();
        coll
    };
    let engine = Engine::open(&path).unwrap();
    let step = engine
        .violations_backfill_step(ExamineBudget { time: Duration::from_secs(60), rows: 5 })
        .unwrap();
    assert!(!step.done);
    let (_, through) = in_table(&engine);
    assert!(through.is_some(), "the step committed its progress");
    engine.close().unwrap();

    let engine = Engine::open(&path).unwrap();
    assert!(!engine.violations_table_ready());
    assert_eq!(in_table(&engine).1, through, "through survived the restart");
    let steps = drain(&engine, 5);
    assert!(steps >= 1);
    assert_in_step(&engine, "after the resume");
    assert_eq!(in_table(&engine).0.len(), 6);
    same_answer(&engine, &coll, "after the resume");
}

#[test]
fn writes_and_retention_between_backfill_steps_are_covered() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let (coll, middle) = {
        let engine = Engine::open(&path).unwrap();
        engine.create_collection("db", "c").unwrap();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        violate(&engine, &coll, "a", 3);
        std::thread::sleep(Duration::from_millis(30));
        let middle = crate::physical_now_ms();
        std::thread::sleep(Duration::from_millis(30));
        violate(&engine, &coll, "b", 3);
        wipe_table(&engine);
        engine.close().unwrap();
        (coll, middle)
    };
    let engine = Engine::open(&path).unwrap();
    let budget = ExamineBudget { time: Duration::from_secs(60), rows: 3 };
    assert!(!engine.violations_backfill_step(budget).unwrap().done);
    // A violation is written while the table is not ready: its row goes in with
    // the entry, above `through`, and the scan meets it again harmlessly.
    violate(&engine, &coll, "live", 1);
    assert_in_step(&engine, "a write during the backfill");
    // Retention removes the oldest entries, and must not move `through` to a
    // tail the scan has not reached: the "b" violations are still unscanned.
    engine.collect_garbage_at(middle + 1_000, RetentionPolicy::new(1, 3_600)).unwrap();
    assert_in_step(&engine, "retention during the backfill");
    drain(&engine, 3);
    assert_in_step(&engine, "after");
    assert_eq!(in_table(&engine).0.len(), 4, "the three unscanned and the live one");
    same_answer(&engine, &coll, "after");
}

/// The tail the write phase reads is the tail then, not the one the scan saw:
/// a rewind between the two must not leave `through` above the oplog.
#[test]
fn a_rewind_between_a_steps_read_and_write_does_not_leave_through_past_the_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let (coll, mark) = {
        let engine = Engine::open(&path).unwrap();
        engine.create_collection("db", "c").unwrap();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        violate(&engine, &coll, "a", 2);
        std::thread::sleep(Duration::from_millis(30));
        let mark = Hlc::new(crate::physical_now_ms(), u16::MAX);
        std::thread::sleep(Duration::from_millis(30));
        violate(&engine, &coll, "b", 2);
        wipe_table(&engine);
        engine.close().unwrap();
        (coll, mark)
    };
    let engine = Engine::open(&path).unwrap();
    crate::violations_table::test_hooks::between_read_and_write(move |engine| {
        engine.rewind_to(mark).unwrap();
    });
    engine
        .violations_backfill_step(ExamineBudget { time: Duration::from_secs(60), rows: 1_000 })
        .unwrap();
    assert_in_step(&engine, "after the rewind between the phases");
    drain(&engine, 1_000);
    assert_in_step(&engine, "after");
    same_answer(&engine, &coll, "after");
}

#[test]
fn a_through_past_the_tail_is_not_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let coll = {
        let engine = Engine::open(&path).unwrap();
        engine.create_collection("db", "c").unwrap();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        violate(&engine, &coll, "a", 2);
        // A rewind by a build that does not know the table: through stays high,
        // and the row of an entry now gone stays too.
        let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
        {
            let mut table = txn.open_table(tables::UNIQUE_VIOLATIONS).unwrap();
            table.insert(SENTINEL, [0xFFu8; codec::STAMP_LEN].as_slice()).unwrap();
        }
        txn.commit().unwrap();
        engine.close().unwrap();
        coll
    };
    let engine = Engine::open(&path).unwrap();
    assert!(!engine.violations_table_ready(), "a through past the tail is discarded");
    assert_eq!(in_table(&engine).1, None);
    drain(&engine, 100);
    assert_in_step(&engine, "rebuilt");
    same_answer(&engine, &coll, "rebuilt");
}

/// A row whose entry retention collected (by a build that leaves the row) is
/// skipped by the route, and one whose entry is another kind is never decoded.
#[test]
fn a_row_with_no_entry_is_skipped_by_the_route() {
    let (engine, coll, _dir) = store();
    violate(&engine, &coll, "a", 1);
    let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
    {
        let mut table = txn.open_table(tables::UNIQUE_VIOLATIONS).unwrap();
        let ghost = Stamp::new(Hlc::new(1, 0), NodeId::from_bytes([1; 16]));
        table
            .insert(
                crate::violations_table::row_key(coll.id, &codec::oplog_key(&ghost)).as_slice(),
                [].as_slice(),
            )
            .unwrap();
    }
    txn.commit().unwrap();
    let live = engine.live_unique_violations(&coll, None, WalkScope::Request).unwrap();
    assert_eq!(live.len(), 1);
}

/// The route judges a row on its entry's header: a row that names an entry of
/// another kind, or another collection's violation, reports nothing.
#[test]
fn a_row_whose_entry_is_not_this_collections_violation_reports_nothing() {
    let (engine, coll, _dir) = store();
    engine.insert(&coll, doc! { "_id": "p", "email": "p@x" }).unwrap();
    let insert = engine.read_arrival_from(0, 100).unwrap().pop().unwrap();
    let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
    {
        let mut table = txn.open_table(tables::UNIQUE_VIOLATIONS).unwrap();
        let key = crate::violations_table::row_key(coll.id, &codec::oplog_key(&insert.stamp));
        table.insert(key.as_slice(), [].as_slice()).unwrap();
    }
    txn.commit().unwrap();
    let live = engine.live_unique_violations(&coll, None, WalkScope::Request).unwrap();
    assert!(live.is_empty(), "{live:?}");
}
