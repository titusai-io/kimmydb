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
    // The whole report, and each `?index` form: one named, one that names no
    // unique index at all.
    for index in [None, Some("email_1"), Some("no_such_index")] {
        let table = engine.live_unique_violations(coll, index, WalkScope::Request).unwrap();
        let oplog =
            engine.live_unique_violations_from_oplog(coll, index, WalkScope::Request).unwrap();
        assert_eq!(table, oplog, "{what}: the table and the oplog walk answer alike ({index:?})");
    }
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

/// A collection dropped and created again under the same name has the same
/// derived id, so the old incarnation's violation records name it too. The route
/// reports only what still stands in the new one, from the table and from the
/// oplog alike.
#[test]
fn a_collection_recreated_under_its_name_reports_only_what_stands_in_the_new_one() {
    let (engine, coll, _dir) = store();
    violate(&engine, &coll, "old", 2);
    engine.drop_collection("db", "c").unwrap();
    engine.finish_purges_now().unwrap();
    engine.create_collection("db", "c").unwrap();
    engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
    let again = engine.get_collection("db", "c").unwrap();
    assert_eq!(again.id, coll.id, "the same name derives the same id");
    assert_in_step(&engine, "after the recreation");
    assert!(engine.live_unique_violations(&again, None, WalkScope::Request).unwrap().is_empty());
    same_answer(&engine, &again, "the recreated collection, before a new violation");
    violate(&engine, &again, "new", 1);
    assert_in_step(&engine, "after a violation in the new incarnation");
    let live = engine.live_unique_violations(&again, None, WalkScope::Request).unwrap();
    assert_eq!(live.len(), 1, "only the new incarnation's: {live:?}");
    same_answer(&engine, &again, "the recreated collection");
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
    // Two budgets: one the scan does not reach, and one exactly the oplog's
    // length, so the scan ends on its budget at the very last row and the tail
    // the rewind then lowers is below what the scan reached.
    for exact in [false, true] {
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
        let rows = if exact {
            let txn = engine.db().begin_read().unwrap();
            redb::ReadableTableMetadata::len(&txn.open_table(tables::OPLOG).unwrap()).unwrap()
        } else {
            1_000
        };
        crate::violations_table::test_hooks::between_read_and_write(move |engine| {
            engine.rewind_to(mark).unwrap();
        });
        engine
            .violations_backfill_step(ExamineBudget { time: Duration::from_secs(60), rows })
            .unwrap();
        assert_in_step(&engine, &format!("after the rewind between the phases (exact: {exact})"));
        drain(&engine, 1_000);
        assert_in_step(&engine, "after");
        same_answer(&engine, &coll, "after");
    }
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
    assert!(in_table(&engine).0.is_empty(), "and every row goes with it");
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

/// A commit between a step's scan and its write leaves the tail ahead of what the
/// scan saw. A scan that used up its range still completes the table: what was
/// appended in between went through the code that maintains it.
#[test]
fn a_write_between_the_scan_and_the_write_does_not_keep_the_table_from_finishing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = Engine::open(&path).unwrap();
        engine.create_collection("db", "c").unwrap();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        violate(&engine, &coll, "a", 2);
        wipe_table(&engine);
        engine.close().unwrap();
    }
    let engine = Engine::open(&path).unwrap();
    assert!(!engine.violations_table_ready());
    crate::violations_table::test_hooks::between_read_and_write(|engine| {
        let coll = engine.get_collection("db", "c").unwrap();
        engine.insert(&coll, doc! { "_id": "late", "email": "late@x" }).unwrap();
        violate(engine, &coll, "late", 1);
    });
    let step = engine
        .violations_backfill_step(ExamineBudget { time: Duration::from_secs(60), rows: 1_000 })
        .unwrap();
    assert!(step.done, "{step:?}");
    assert!(engine.violations_table_ready());
    assert_in_step(&engine, "after a write between the scan and the write");
    assert_eq!(in_table(&engine).0.len(), 3, "the two scanned and the one the write appended");
}

/// A writer that overtakes **every** step, so no step ever leaves the tail where
/// its scan saw it: the backfill still finishes, because a scan that used up its
/// range completes the table. This is the deterministic guard of that rule; the
/// test with real writers only shows it under load.
#[test]
fn a_backfill_finishes_though_a_write_overtakes_every_step() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = Engine::open(&path).unwrap();
        engine.create_collection("db", "c").unwrap();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        violate(&engine, &coll, "a", 3);
        for i in 0..400 {
            engine
                .insert(&coll, doc! { "_id": format!("f{i}"), "email": format!("f{i}@x") })
                .unwrap();
        }
        wipe_table(&engine);
        engine.close().unwrap();
    }
    let engine = Engine::open(&path).unwrap();
    let counter = std::rc::Rc::new(std::cell::Cell::new(0u32));
    crate::violations_table::test_hooks::between_read_and_write_every({
        let counter = std::rc::Rc::clone(&counter);
        move |engine| {
            let coll = engine.get_collection("db", "c").unwrap();
            let n = counter.get();
            counter.set(n + 1);
            engine
                .insert(&coll, doc! { "_id": format!("o{n}"), "email": format!("o{n}@x") })
                .unwrap();
        }
    });
    let budget = ExamineBudget { time: Duration::from_secs(60), rows: 100 };
    let mut steps = 0;
    while !engine.violations_backfill_step(budget).unwrap().done {
        steps += 1;
        assert!(steps < 50, "every step was overtaken and none finished");
    }
    crate::violations_table::test_hooks::clear_every();
    assert!(
        steps >= 2 && counter.get() as usize == steps + 1,
        "steps {steps}, overtaken {}",
        counter.get()
    );
    assert_in_step(&engine, "after a backfill overtaken at every step");
}

/// A row left by a build that does not remove rows (its entry gone, below the
/// oldest the oplog holds) is trimmed by the next pass that removes entries.
#[test]
fn retention_trims_a_row_an_older_build_left_behind() {
    let (engine, coll, _dir) = store();
    violate(&engine, &coll, "a", 2);
    let ghost = Stamp::new(Hlc::new(1, 0), NodeId::from_bytes([1; 16]));
    let ghost_row = crate::violations_table::row_key(coll.id, &codec::oplog_key(&ghost));
    let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
    {
        let mut table = txn.open_table(tables::UNIQUE_VIOLATIONS).unwrap();
        table.insert(ghost_row.as_slice(), [].as_slice()).unwrap();
    }
    txn.commit().unwrap();
    assert!(in_table(&engine).0.contains(&ghost_row));
    std::thread::sleep(Duration::from_millis(20));
    violate(&engine, &coll, "b", 1);
    let outcome = engine
        .collect_garbage_at(crate::physical_now_ms() + 1_000, RetentionPolicy::new(1, 3_600))
        .unwrap();
    assert!(outcome.oplog_removed > 0);
    assert!(!in_table(&engine).0.contains(&ghost_row), "the stale row went");
    assert_in_step(&engine, "after the trim");
}

/// A small deterministic generator, so a failing history names its seed.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: u64) -> u64 {
        self.0 =
            self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

/// A generated history over two collections: violations, plain writes,
/// retention passes of every reach, rewinds to earlier moments, reopens, and a
/// table wiped and completed again. After every step the structural assertion
/// holds and the table's answer is the oplog walk's.
#[test]
fn a_generated_history_keeps_the_table_the_oplogs() {
    for seed in 0..12u64 {
        let mut rng = Lcg(seed + 1);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let mut engine = Engine::open(&path).unwrap();
        for name in ["a", "b"] {
            engine.create_collection("db", name).unwrap();
            engine.create_index("db", name, vec![field("email")], true, None).unwrap();
        }
        let mut marks: Vec<Hlc> = Vec::new();
        let mut trace = Vec::new();
        for step in 0..14 {
            let name = if rng.below(2) == 0 { "a" } else { "b" };
            let op = rng.below(8);
            trace.push(format!("{step}:{op}{name}"));
            let coll = engine.get_collection("db", name).unwrap();
            match op {
                0 | 1 | 7 => {
                    violate(&engine, &coll, &format!("s{step}"), 1 + rng.below(3) as usize)
                }
                2 => {
                    for i in 0..(1 + rng.below(4)) {
                        engine
                            .insert(&coll, doc! { "_id": format!("p{step}-{i}"), "email": format!("p{step}-{i}@x") })
                            .unwrap();
                    }
                }
                3 => {
                    // Retention of every reach: nothing, some, all but the tail.
                    let reach = [0u64, 1, 100][rng.below(3) as usize];
                    engine
                        .collect_garbage_at(
                            crate::physical_now_ms() + reach * 1_000,
                            RetentionPolicy::new(1, 3_600),
                        )
                        .unwrap();
                }
                4 if !marks.is_empty() => {
                    let until = marks[rng.below(marks.len() as u64) as usize];
                    // Refused, unchanged, when retention has collected past `until`.
                    let _ = engine.rewind_to(until);
                }
                5 => {
                    engine.close().unwrap();
                    engine = Engine::open(&path).unwrap();
                }
                6 => {
                    wipe_table(&engine);
                    engine.close().unwrap();
                    engine = Engine::open(&path).unwrap();
                    drain(&engine, 1 + rng.below(40));
                }
                _ => {}
            }
            std::thread::sleep(Duration::from_millis(3));
            marks.push(Hlc::new(crate::physical_now_ms(), u16::MAX));
            let what = format!("seed {seed}, after {trace:?}");
            assert_in_step(&engine, &what);
            for name in ["a", "b"] {
                if let Ok(coll) = engine.get_collection("db", name) {
                    same_answer(&engine, &coll, &what);
                }
            }
        }
        engine.close().unwrap();
    }
}

/// Writers that never stop do not keep the backfill from finishing, and retention
/// runs between its steps: a scan that used up its range sets `through` to the
/// tail as the writer sees it, since every entry appended after its snapshot was
/// written by the code that maintains the table.
#[test]
fn a_backfill_finishes_under_concurrent_writers_and_retention_runs_meanwhile() {
    for retention_between_steps in [true, false] {
        backfill_under_writers(retention_between_steps);
    }
}

/// One run of the test above. **Without retention between the steps** the tail
/// only moves away from a scan, so a step that stopped at its snapshot's tail
/// could not finish: that variant fails without the rule that a scan which used
/// up its range sets `through` to the tail as the writer sees it.
fn backfill_under_writers(retention_between_steps: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = Engine::open(&path).unwrap();
        engine.create_collection("db", "c").unwrap();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        violate(&engine, &coll, "seed", 6);
        for i in 0..1_500 {
            engine
                .insert(&coll, doc! { "_id": format!("f{i}"), "email": format!("f{i}@x") })
                .unwrap();
        }
        wipe_table(&engine);
        engine.close().unwrap();
    }
    let engine = std::sync::Arc::new(Engine::open(&path).unwrap());
    assert!(!engine.violations_table_ready());
    let coll = engine.get_collection("db", "c").unwrap();
    let stop = std::sync::atomic::AtomicBool::new(false);
    let mut passes = 0u32;
    let mut removed = 0usize;
    let started = std::time::Instant::now();
    let steps = std::thread::scope(|scope| {
        for writer in 0..4 {
            let (engine, coll, stop) = (&engine, &coll, &stop);
            scope.spawn(move || {
                let mut n = 0;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    engine
                        .insert(coll, doc! { "_id": format!("w{writer}-{n}"), "email": format!("w{writer}-{n}@x") })
                        .unwrap();
                    n += 1;
                    if n % 10 == 0 {
                        violate(engine, coll, &format!("live{writer}-{n}"), 1);
                    }
                }
            });
        }
        let budget = ExamineBudget { time: Duration::from_secs(60), rows: 400 };
        let mut steps = 0u32;
        loop {
            steps += 1;
            assert!(steps < 400 && started.elapsed() < Duration::from_secs(60), "never finished");
            if engine.violations_backfill_step(budget).unwrap().done {
                break;
            }
            if !retention_between_steps {
                continue;
            }
            // A pass between two steps, as the daemon interleaves them.
            let outcome = engine
                .collect_garbage_at(
                    crate::physical_now_ms() + 1_000,
                    RetentionPolicy::new(1, 3_600),
                )
                .unwrap();
            passes += 1;
            removed += outcome.oplog_removed;
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        steps
    });
    assert!(engine.violations_table_ready());
    assert!(steps >= 2, "steps {steps}");
    if retention_between_steps {
        assert!(passes >= 1 && removed > 0, "steps {steps}, passes {passes}, removed {removed}");
    }
    assert_in_step(&engine, "after the backfill under writers");
    same_answer(&engine, &coll, "after the backfill under writers");
}

/// Writer holds taken since the engine opened, in all, and by the backfill's
/// holder: counted from the hold meter, which no host stall moves.
fn holds(engine: &Engine) -> (u64, u64) {
    let count = engine.writer_hold().count;
    (count.iter().sum(), count[crate::engine::WriterHolder::Violations.slot()])
}

/// **A start whose tail holds no violation makes no commit** (ADR-200). Every
/// restart of a store with writes since the last retention pass finds `through`
/// behind the tail and scans the stretch between; the scan used to commit
/// `through` even when it found nothing, so that most starts opened with a
/// fsync before the node answered. Now the scan completes the table in memory
/// and the retention pass is what moves `through`.
#[test]
fn a_start_whose_tail_holds_no_violation_makes_no_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let (engine, coll, _keep) = {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("db", "c").unwrap();
            engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
            let coll = engine.get_collection("db", "c").unwrap();
            (engine, coll, ())
        };
        for i in 0..5 {
            engine
                .insert(&coll, doc! { "_id": format!("p{i}"), "email": format!("p{i}@x") })
                .unwrap();
        }
        engine.close().unwrap();
    }
    let engine = Engine::open(&path).unwrap();
    assert!(!engine.violations_table_ready(), "writes since `through` leave the open behind");
    let through = in_table(&engine).1;
    let before = holds(&engine);

    let step = engine
        .violations_backfill_step(ExamineBudget { time: Duration::from_secs(60), rows: 1_000 })
        .unwrap();
    assert!(step.done && step.found == 0 && step.scanned > 0, "{step:?}");
    assert!(engine.violations_table_ready(), "ready in memory");
    assert_eq!(holds(&engine), before, "the scan took the writer: a commit at the start");
    assert_eq!(in_table(&engine).1, through, "through stays where the last commit left it");
    assert_in_step(&engine, "after the skipped commit");
    engine.close().unwrap();

    // The next start scans the same tail again, still with no commit.
    let engine = Engine::open(&path).unwrap();
    assert!(!engine.violations_table_ready(), "through was not advanced, so the open is behind");
    let before = holds(&engine);
    assert!(engine.violations_backfill_step(ExamineBudget::serve()).unwrap().done);
    assert_eq!(holds(&engine), before, "the second start commits nothing either");

    // The retention pass, once the table is ready, is what moves `through`.
    engine.advance_violations_through().unwrap();
    assert_eq!(in_table(&engine).1, tail(&engine));
    engine.close().unwrap();
    let engine = Engine::open(&path).unwrap();
    assert!(engine.violations_table_ready(), "ready at the open, nothing to scan");
}

/// The other half of the skip: a tail that holds a violation is recorded with
/// its commit, `through` is persisted, and the next start finds the table ready
/// and complete.
#[test]
fn a_tail_with_a_violation_is_still_recorded_and_persisted_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let coll = {
        let engine = Engine::open(&path).unwrap();
        engine.create_collection("db", "c").unwrap();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        engine.insert(&coll, doc! { "_id": "p", "email": "p@x" }).unwrap();
        violate(&engine, &coll, "a", 1);
        // What a build that does not know the table leaves behind.
        wipe_table(&engine);
        engine.close().unwrap();
        coll
    };
    let engine = Engine::open(&path).unwrap();
    assert!(!engine.violations_table_ready());
    let before = holds(&engine);
    let step = engine
        .violations_backfill_step(ExamineBudget { time: Duration::from_secs(60), rows: 1_000 })
        .unwrap();
    assert!(step.done && step.found == 1, "{step:?}");
    let after = holds(&engine);
    assert_eq!(after.0, before.0 + 1, "one commit for the step");
    assert_eq!(after.1, before.1 + 1, "held as the violations holder");
    assert_eq!(in_table(&engine).0.len(), 1, "the violation is recorded");
    assert_eq!(in_table(&engine).1, tail(&engine), "through is persisted at the tail");
    same_answer(&engine, &coll, "after the recording step");
    engine.close().unwrap();

    let engine = Engine::open(&path).unwrap();
    assert!(engine.violations_table_ready(), "the recorded table is ready at the next open");
    assert_in_step(&engine, "after the restart");
    assert_eq!(engine.live_unique_violations(&coll, None, WalkScope::Request).unwrap().len(), 1);
}

/// **A retention pass moves `through` to the tail, whether or not it removed
/// anything, and a pass with nothing written since commits nothing.** A start
/// whose backfill found no violation leaves `through` behind and counts on the
/// pass to move it, so the next start's rescan is a pass's writes. Counted from
/// the engine's commit counter.
#[test]
fn a_retention_pass_advances_through_after_writes_and_commits_nothing_when_idle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let policy = RetentionPolicy::new(86_400, 172_800);
    let (engine, coll, _keep) = {
        let engine = Engine::open(&path).unwrap();
        engine.create_collection("db", "c").unwrap();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        (engine, coll, ())
    };
    for i in 0..3 {
        engine.insert(&coll, doc! { "_id": format!("p{i}"), "email": format!("p{i}@x") }).unwrap();
    }
    assert_ne!(in_table(&engine).1, tail(&engine), "writes since `through`");

    let before = engine.commits();
    assert_eq!(engine.collect_garbage(policy).unwrap().oplog_removed, 0, "nothing to collect");
    assert_eq!(engine.commits(), before + 1, "a pass after writes commits `through` once");
    assert_eq!(in_table(&engine).1, tail(&engine), "through is at the tail");
    assert_in_step(&engine, "after the advance");

    let idle = engine.commits();
    engine.collect_garbage(policy).unwrap();
    engine.collect_garbage(policy).unwrap();
    assert_eq!(engine.commits(), idle, "a pass with nothing written since commits nothing");

    engine.insert(&coll, doc! { "_id": "q", "email": "q@x" }).unwrap();
    let after_write = engine.commits();
    engine.collect_garbage(policy).unwrap();
    assert_eq!(engine.commits(), after_write + 1, "and one more after the next write");
    engine.close().unwrap();
    let engine = Engine::open(&path).unwrap();
    assert!(engine.violations_table_ready(), "ready at the open, with nothing to scan");
}

/// A write that lands between the pass's read, which finds `through` behind the
/// tail, and its write, which advances it, is covered: the advance takes the tail
/// the writer sees, and a violation the write made is in the table already,
/// because it went in with its own entry. The structural check holds after it: no
/// row missing at or below `through`, and the ready table is the oplog's.
#[test]
fn a_violation_written_between_the_passes_check_and_its_advance_is_covered() {
    let (engine, coll, _dir) = store();
    engine.insert(&coll, doc! { "_id": "p", "email": "p@x" }).unwrap();
    assert!(engine.violations_table_ready());
    let seen_before = tail(&engine);
    crate::violations_table::test_hooks::between_advance_check_and_write({
        let coll = coll.clone();
        move |engine| violate(engine, &coll, "gap", 1)
    });
    engine.collect_garbage(RetentionPolicy::new(86_400, 172_800)).unwrap();
    assert_ne!(tail(&engine), seen_before, "the write landed in the gap");
    assert_eq!(in_table(&engine).1, tail(&engine), "through is at the tail the writer saw");
    assert_eq!(in_table(&engine).0.len(), 1, "the violation is recorded");
    assert_in_step(&engine, "after the interleaved advance");
    same_answer(&engine, &coll, "after the interleaved advance");
}
