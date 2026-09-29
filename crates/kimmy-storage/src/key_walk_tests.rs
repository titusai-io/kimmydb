//! The key walk against the linear walk (ADR-197).
//!
//! `Engine::read_oplog_by_arrival_keys_in` reads the same window as
//! `Engine::read_oplog_linear_in` from the keys of `OPLOG_ARRIVAL_SEQ`. These
//! tests run **both in one read transaction**, so a difference is the walks'
//! and never a write's, and compare everything a requester's coverage rule
//! rests on: the entries, `scanned_to`, `exhausted`, `passed_through`, and the
//! count of entries examined and not served.

use std::cell::Cell;
use std::time::Duration;

use kimmy_core::{CollectionId, Hlc, NodeId, OpKind, OplogEntry, Stamp, VersionVector};
use proptest::prelude::*;
use redb::ReadableDatabase;

use crate::engine::{Engine, WriterHolder, append_oplog};
use crate::sync::MarkedRange;
use crate::walk::WalkScope;
use crate::watch::{ExamineBudget, KeyWalkFallback, OplogWindow};
use crate::{ServeWalk, WalkPath, codec, tables};

const ORIGINS: usize = 4;

fn node(origin: usize) -> NodeId {
    // Origin 0 is the all-zero id, so a stamp can sit exactly on a lower
    // bound built from `(hlc, zero node)`.
    NodeId::from_bytes([origin as u8; 16])
}

fn at(offset: u64) -> Hlc {
    Hlc::new(1_000 + offset, 0)
}

/// One oplog row to write: which origin wrote it, when, and whether it is a
/// unique-violation record (which a served window withholds).
#[derive(Clone, Debug)]
struct Row {
    origin: usize,
    at: u64,
    violation: bool,
}

fn stamp_of(row: &Row) -> Stamp {
    Stamp::new(at(row.at), node(row.origin))
}

fn entry_of(row: &Row) -> OplogEntry {
    OplogEntry {
        stamp: stamp_of(row),
        kind: if row.violation { OpKind::UniqueViolation } else { OpKind::Insert },
        collection: CollectionId(7),
        doc_id: None,
        body: Some(vec![row.origin as u8; 3]),
    }
}

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    (engine, dir)
}

/// Write `rows` in this order, in one transaction. The arrival order is the
/// order given, and the stamp order is not: a row written later with a lower
/// stamp is a late relayed entry.
fn write(engine: &Engine, rows: &[Row]) {
    let txn = engine.begin_write(WriterHolder::Write).unwrap();
    for row in rows {
        append_oplog(&txn, &entry_of(row)).unwrap();
    }
    txn.commit().unwrap();
}

/// What a request asks for, as `entries_for_peer_counting` builds it.
#[derive(Clone, Debug)]
struct Ask {
    from: Hlc,
    limit: usize,
    held: VersionVector,
    marked: Vec<MarkedRange>,
    /// A row budget, or none. Time is not compared: it is not deterministic.
    rows: Option<u64>,
}

struct Read {
    window: OplogWindow,
    passed: u64,
}

fn budget_of(ask: &Ask) -> Option<(ExamineBudget, Hlc)> {
    let start = ask.marked.iter().map(|span| span.from).fold(ask.from, Hlc::min);
    ask.rows
        .map(|rows| (ExamineBudget { time: Duration::from_secs(3_600), rows }, start.successor()))
}

fn start_of(ask: &Ask) -> Hlc {
    ask.marked.iter().map(|span| span.from).fold(ask.from, Hlc::min)
}

/// Both walks in one read transaction: the linear one, and the key walk.
fn both(engine: &Engine, ask: &Ask) -> (Read, std::result::Result<Read, KeyWalkFallback>) {
    let txn = engine.db().begin_read().unwrap();
    let start = start_of(ask);
    let run = |keys: bool| {
        let passed = Cell::new(0u64);
        let skip = |stamp: &Stamp| {
            let skip = stamp.hlc <= ask.held.get(stamp.node)
                && !ask.marked.iter().any(|span| span.contains(stamp));
            passed.set(passed.get() + u64::from(skip));
            skip
        };
        let keep = |entry: &OplogEntry| {
            let keep = entry.kind != OpKind::UniqueViolation;
            passed.set(passed.get() + u64::from(!keep));
            keep
        };
        let window = if keys {
            match engine
                .read_oplog_by_arrival_keys_in(
                    &txn,
                    start,
                    ask.limit,
                    WalkScope::Request,
                    skip,
                    keep,
                    budget_of(ask),
                )
                .unwrap()
            {
                Ok(window) => window,
                Err(why) => return Err(why),
            }
        } else {
            engine
                .read_oplog_linear_in(
                    &txn,
                    start,
                    ask.limit,
                    WalkScope::Request,
                    skip,
                    keep,
                    budget_of(ask),
                )
                .unwrap()
        };
        Ok(Read { window, passed: passed.get() })
    };
    (run(false).ok().unwrap(), run(true))
}

fn assert_same(engine: &Engine, ask: &Ask) {
    let (linear, keys) = both(engine, ask);
    let keys = keys.unwrap_or_else(|why| panic!("{ask:?}: the key walk fell back: {why:?}"));
    assert_eq!(keys.window, linear.window, "the window differs for {ask:?}");
    assert_eq!(keys.passed, linear.passed, "the passed count differs for {ask:?}");
}

fn held_of(offsets: &[Option<u64>; ORIGINS]) -> VersionVector {
    let mut held = VersionVector::new();
    for (origin, offset) in offsets.iter().enumerate() {
        if let Some(offset) = offset {
            held.insert(node(origin), at(*offset));
        }
    }
    held
}

fn span_of(origin: usize, from: u64, len: u64) -> MarkedRange {
    MarkedRange { origin: node(origin), from: at(from), through: at(from + len) }
}

fn rows_strategy() -> impl Strategy<Value = Vec<Row>> {
    // A small range of times, so that stamps tie on the hlc across origins.
    prop::collection::vec(
        (0..ORIGINS, 0u64..40, prop::bool::weighted(0.15))
            .prop_map(|(origin, at, violation)| Row { origin, at, violation }),
        1..70,
    )
}

fn ask_strategy() -> impl Strategy<Value = Ask> {
    (
        prop::option::of(0u64..45),
        1usize..12,
        prop::array::uniform4(prop::option::of(0u64..45)),
        prop::collection::vec((0..ORIGINS, 0u64..40, 0u64..12), 0..3),
        prop::option::of(1u64..30),
    )
        .prop_map(|(from, limit, held, spans, rows)| Ask {
            from: from.map_or(Hlc::ZERO, at),
            limit,
            held: held_of(&held),
            marked: spans.into_iter().map(|(o, f, l)| span_of(o, f, l)).collect(),
            rows,
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// The key walk and the linear walk answer the same window: entries,
    /// `scanned_to`, `exhausted`, `passed_through` and the passed count, over
    /// four origins with stamp ties, late relayed entries (arrival order is
    /// not stamp order), held vectors with absent and unknown origins, marked
    /// spans, unique violations at any position, a `from` on a tie, and any
    /// row budget, in one read transaction.
    #[test]
    fn the_key_walk_reads_the_window_the_linear_walk_reads(
        rows in rows_strategy(),
        ask in ask_strategy(),
    ) {
        let (engine, _dir) = engine();
        write(&engine, &rows);
        assert_same(&engine, &ask);
    }
}

/// Follow a drain the way a requester does, window after window, and compare
/// every window: `from` moves to where the last one ended.
#[test]
fn a_drain_reads_the_same_windows_by_both_walks() {
    let (engine, _dir) = engine();
    let rows: Vec<Row> = (0..200u64)
        .map(|i| Row { origin: (i % 4) as usize, at: (i * 7) % 60, violation: i % 11 == 0 })
        .collect();
    write(&engine, &rows);
    let held = held_of(&[Some(30), None, Some(10), Some(45)]);
    let mut from = Hlc::ZERO;
    for _ in 0..200 {
        let ask = Ask { from, limit: 5, held: held.clone(), marked: vec![], rows: Some(9) };
        let (linear, keys) = both(&engine, &ask);
        let keys = keys.unwrap();
        assert_eq!(keys.window, linear.window, "from {from:?}");
        assert_eq!(keys.passed, linear.passed, "from {from:?}");
        if linear.window.exhausted {
            return;
        }
        // The next window starts where this one ended, on the stamp it
        // passed through, or on its last scanned time.
        from = linear.window.passed_through.map_or(linear.window.scanned_to, |s| s.hlc);
    }
    panic!("the drain never reached the end");
}

/// Ties at `from`, on two and three origins, at a row-budget boundary: the
/// window must start at the first stamp at or above `from` on every origin.
#[test]
fn stamps_that_tie_at_from_are_all_read_at_a_row_budget_boundary() {
    let (engine, _dir) = engine();
    let mut rows = Vec::new();
    for at in [5u64, 6, 6, 6, 7] {
        for origin in 0..3 {
            rows.push(Row { origin, at, violation: false });
        }
    }
    write(&engine, &rows);
    for budget in 1..=10 {
        let ask = Ask {
            from: at(6),
            limit: 100,
            held: VersionVector::new(),
            marked: vec![],
            rows: Some(budget),
        };
        assert_same(&engine, &ask);
    }
    // The first three rows of the window are the three at exactly `from`.
    let ask = Ask { from: at(6), limit: 3, held: VersionVector::new(), marked: vec![], rows: None };
    let (_, keys) = both(&engine, &ask);
    let keys = keys.unwrap();
    let stamps: Vec<_> = keys.window.entries.iter().map(|e| e.stamp).collect();
    assert_eq!(stamps, (0..3).map(|o| Stamp::new(at(6), node(o))).collect::<Vec<_>>());
}

/// A corrupt body is met, or not, in the same place by both walks: a row the
/// requester holds is never decoded, so it raises nothing; one it lacks is
/// decoded, and both fail with the same error.
#[test]
fn a_corrupt_body_is_the_same_error_at_the_same_row_and_skipped_rows_raise_none() {
    let (engine, _dir) = engine();
    let rows: Vec<Row> =
        (0..30u64).map(|i| Row { origin: (i % 3) as usize, at: i, violation: false }).collect();
    write(&engine, &rows);
    let damage = |origin: usize, at: u64| {
        let row = rows.iter().find(|r| r.origin == origin && r.at == at).unwrap();
        let txn = engine.begin_write(WriterHolder::Write).unwrap();
        {
            let mut oplog = txn.open_table(tables::OPLOG).unwrap();
            let key = codec::oplog_key(&stamp_of(row));
            oplog.insert(key.as_slice(), [0xFFu8; 4].as_slice()).unwrap();
        }
        txn.commit().unwrap();
    };
    // Origin 0 is held through 10, origin 1 through 5, origin 2 through 5.
    let held = held_of(&[Some(10), Some(5), Some(5), None]);
    let run = |keys: bool| -> std::result::Result<usize, String> {
        let txn = engine.db().begin_read().unwrap();
        let skip = |s: &Stamp| s.hlc <= held.get(s.node);
        let keep = |_: &OplogEntry| true;
        if keys {
            match engine
                .read_oplog_by_arrival_keys_in(
                    &txn,
                    Hlc::ZERO,
                    1000,
                    WalkScope::Request,
                    skip,
                    keep,
                    None,
                )
                .map_err(|e| e.to_string())?
            {
                Ok(w) => Ok(w.entries.len()),
                Err(why) => Err(format!("fell back: {why:?}")),
            }
        } else {
            engine
                .read_oplog_linear_in(&txn, Hlc::ZERO, 1000, WalkScope::Request, skip, keep, None)
                .map(|w| w.entries.len())
                .map_err(|e| e.to_string())
        }
    };

    // Damage a row the requester holds: neither walk decodes it.
    damage(0, 6);
    let (linear, keys) = (run(false), run(true));
    assert!(linear.is_ok(), "a skipped row's body is never read: {linear:?}");
    assert_eq!(keys, linear);

    // Damage a row it lacks: both meet it, and both say the same thing.
    damage(1, 19);
    let (linear, keys) = (run(false), run(true));
    assert!(linear.is_err(), "the linear walk must meet a damaged row it does not skip");
    assert_eq!(keys, linear, "the same error");
}

/// A key walk whose two tables disagree gives up, and says which way.
#[test]
fn a_short_index_gives_up_for_the_linear_walk_and_the_window_is_the_linear_one() {
    let (engine, _dir) = engine();
    let rows: Vec<Row> = (0..40u64)
        .map(|i| Row { origin: (i % 4) as usize, at: i, violation: i % 9 == 0 })
        .collect();
    write(&engine, &rows);
    let txn = engine.begin_write(WriterHolder::Write).unwrap();
    {
        let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
        let key = codec::oplog_key(&stamp_of(&rows[17]));
        assert!(by_stamp.remove(key.as_slice()).unwrap().is_some());
    }
    txn.commit().unwrap();

    let ask = Ask {
        from: Hlc::ZERO,
        limit: 1000,
        held: held_of(&[Some(20), None, Some(5), None]),
        marked: vec![],
        rows: None,
    };
    let (linear, keys) = both(&engine, &ask);
    assert_eq!(keys.err(), Some(KeyWalkFallback::Length));

    // Served, the window is the linear one, the path is counted once, and the
    // entries examined and not served are counted once (the abandoned key
    // walk's count is not added to the linear walk's).
    let before = engine.serve_counters().snapshot();
    let served =
        engine.serve_entries_to_peer(ask.from, ask.limit, Some(&ask.held), &[], None).unwrap();
    let after = engine.serve_counters().snapshot();
    assert_eq!(served, linear.window);
    let path = |s: &crate::ServeSnapshot, p: WalkPath| s.paths[ServeWalk::Serve.slot()][p.slot()];
    assert_eq!(path(&after, WalkPath::FallbackLength) - path(&before, WalkPath::FallbackLength), 1);
    assert_eq!(path(&after, WalkPath::Keys), path(&before, WalkPath::Keys));
    assert_eq!(after.passed - before.passed, linear.passed, "counter parity");
}

/// A key with no body, with the lengths made equal by a row missing from the
/// index elsewhere, is met on the row and gives up the same way: the
/// compensating pair the length guard alone cannot see, when the dangling key
/// is in the window.
#[test]
fn a_key_with_no_body_gives_up_even_when_the_lengths_agree() {
    let (engine, _dir) = engine();
    let rows: Vec<Row> =
        (0..30u64).map(|i| Row { origin: (i % 3) as usize, at: i, violation: false }).collect();
    write(&engine, &rows);
    let txn = engine.begin_write(WriterHolder::Write).unwrap();
    {
        // A body gone from the oplog, its key still in the index...
        let mut oplog = txn.open_table(tables::OPLOG).unwrap();
        let key = codec::oplog_key(&stamp_of(&rows[12]));
        assert!(oplog.remove(key.as_slice()).unwrap().is_some());
        // ...and a row gone from the index, so the two lengths agree again.
        let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
        let other = codec::oplog_key(&stamp_of(&rows[25]));
        assert!(by_stamp.remove(other.as_slice()).unwrap().is_some());
    }
    txn.commit().unwrap();

    let ask = Ask {
        from: Hlc::ZERO,
        limit: 1000,
        // The requester holds the early rows, so the key walk passes over some
        // (and counts them) before it meets the dangling key.
        held: held_of(&[Some(10), Some(10), Some(10), None]),
        marked: vec![],
        rows: None,
    };
    let (linear, keys) = both(&engine, &ask);
    assert_eq!(keys.err(), Some(KeyWalkFallback::MissingBody));

    // Served, the linear walk answers, and it serves the row the index lost.
    let before = engine.serve_counters().snapshot();
    let served =
        engine.serve_entries_to_peer(ask.from, ask.limit, Some(&ask.held), &[], None).unwrap();
    let after = engine.serve_counters().snapshot();
    assert_eq!(served, linear.window);
    assert!(served.entries.iter().any(|e| e.stamp == stamp_of(&rows[25])));
    assert_eq!(after.passed - before.passed, linear.passed, "counter parity after a late fallback");
    let slot = |s: &crate::ServeSnapshot| {
        s.paths[ServeWalk::Serve.slot()][WalkPath::FallbackMissingBody.slot()]
    };
    assert_eq!(slot(&after) - slot(&before), 1);
}

/// A request that names nothing held is read by the linear walk, and counted
/// as such; one that names something is read by the keys.
#[test]
fn the_path_a_window_took_is_counted_by_kind_of_request() {
    let (engine, _dir) = engine();
    let rows: Vec<Row> =
        (0..20u64).map(|i| Row { origin: (i % 2) as usize, at: i, violation: false }).collect();
    write(&engine, &rows);
    let held = held_of(&[Some(5), None, None, None]);
    let count = |walk: ServeWalk, path: WalkPath| {
        engine.serve_counters().snapshot().paths[walk.slot()][path.slot()]
    };

    engine.serve_entries_to_peer(Hlc::ZERO, 100, None, &[], None).unwrap();
    assert_eq!(count(ServeWalk::Serve, WalkPath::Linear), 1);
    assert_eq!(count(ServeWalk::Serve, WalkPath::Keys), 0);

    engine.serve_entries_to_peer(Hlc::ZERO, 100, Some(&held), &[], None).unwrap();
    assert_eq!(count(ServeWalk::Serve, WalkPath::Keys), 1);

    // A push is separate from a pull, and a local read is neither.
    engine
        .entries_for_peer_within(
            Hlc::ZERO,
            100,
            Some(&held),
            WalkScope::Request,
            ExamineBudget::serve(),
        )
        .unwrap();
    assert_eq!(count(ServeWalk::Push, WalkPath::Keys), 1);
    engine.entries_for_peer_holding(Hlc::ZERO, 100, Some(&held), WalkScope::Background).unwrap();
    assert_eq!(count(ServeWalk::Serve, WalkPath::Keys), 1, "a local read is not a served window");
    assert_eq!(count(ServeWalk::Push, WalkPath::Keys), 1);
}

/// Every row of the two tables is present in both: the invariant the guard
/// leans on, over what the writers in this crate produce.
#[test]
fn the_oplog_and_its_arrival_index_hold_the_same_keys() {
    let (engine, _dir) = engine();
    let rows: Vec<Row> = (0..120u64)
        .map(|i| Row { origin: (i % 4) as usize, at: (i * 13) % 50, violation: i % 7 == 0 })
        .collect();
    write(&engine, &rows);
    let txn = engine.db().begin_read().unwrap();
    let oplog = txn.open_table(tables::OPLOG).unwrap();
    let by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
    let a: Vec<Vec<u8>> =
        redb::ReadableTable::iter(&oplog).unwrap().map(|r| r.unwrap().0.value().to_vec()).collect();
    let b: Vec<Vec<u8>> = redb::ReadableTable::iter(&by_stamp)
        .unwrap()
        .map(|r| r.unwrap().0.value().to_vec())
        .collect();
    assert_eq!(a, b);
}

/// The open's verification walk reads the arrival index's keys in step with
/// the oplog's, so a pair the two counts cannot see (a row missing from the
/// index, a stray key in its place) is found, and **repaired in that open**:
/// both arrival tables are rebuilt from the oplog before the record is written,
/// and the key walk then reads the right window.
#[test]
fn the_verification_walk_repairs_a_pair_the_counts_cannot_see_before_it_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let rows: Vec<Row> =
        (0..40u64).map(|i| Row { origin: (i % 3) as usize, at: i, violation: false }).collect();
    {
        let engine = Engine::open(&path).unwrap();
        write(&engine, &rows);
        // A row missing from the index and a stray key in the index: the two
        // counts still agree.
        let txn = engine.begin_write(WriterHolder::Write).unwrap();
        {
            let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
            let key = codec::oplog_key(&stamp_of(&rows[20]));
            assert!(by_stamp.remove(key.as_slice()).unwrap().is_some());
            let stray = codec::oplog_key(&Stamp::new(at(9_000), node(3)));
            by_stamp.insert(stray.as_slice(), 999_999u64).unwrap();
        }
        txn.commit().unwrap();
        engine.close().unwrap();
    }

    // A start that skips the walk (the record says the vector is verified)
    // cannot see it: the property test and the walk cover that, and the walk
    // runs after a schema change, a restore, or with the variable set.
    // A start that walks finds it and repairs it.
    let engine = crate::verified::test_support::forcing(|| Engine::open(&path).unwrap());
    let txn = engine.db().begin_read().unwrap();
    let oplog_keys: Vec<Vec<u8>> =
        redb::ReadableTable::iter(&txn.open_table(tables::OPLOG).unwrap())
            .unwrap()
            .map(|r| r.unwrap().0.value().to_vec())
            .collect();
    let index_keys: Vec<Vec<u8>> =
        redb::ReadableTable::iter(&txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap())
            .unwrap()
            .map(|r| r.unwrap().0.value().to_vec())
            .collect();
    assert_eq!(index_keys, oplog_keys, "the index is the oplog's");
    drop(txn);
    assert!(engine.version_vector_verified().unwrap().is_some(), "and the record is written");

    // The key walk now reads what the linear one does, and the row the index
    // lost is served.
    let ask = Ask {
        from: Hlc::ZERO,
        limit: 1000,
        held: held_of(&[Some(10), Some(10), Some(10), None]),
        marked: vec![],
        rows: None,
    };
    let (linear, keys) = both(&engine, &ask);
    let keys = keys.expect("the repaired index is read by the key walk");
    assert_eq!(keys.window, linear.window);
    assert!(keys.window.entries.iter().any(|e| e.stamp == stamp_of(&rows[20])));
}

/// A walk whose index cannot be read is not a failed serve: the oplog can, so
/// the linear walk answers and the fallback is counted. Here a key of the index
/// that does not decode, with the counts made equal by a row missing elsewhere.
#[test]
fn an_index_key_that_cannot_be_read_falls_back_and_serves() {
    let (engine, _dir) = engine();
    let rows: Vec<Row> =
        (0..30u64).map(|i| Row { origin: (i % 3) as usize, at: i, violation: false }).collect();
    write(&engine, &rows);
    let txn = engine.begin_write(WriterHolder::Write).unwrap();
    {
        let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
        let key = codec::oplog_key(&stamp_of(&rows[25]));
        assert!(by_stamp.remove(key.as_slice()).unwrap().is_some());
        // Too short to be a stamp, and sorting inside the walk's range.
        by_stamp.insert([0x00u8, 0x01, 0x02].as_slice(), 7u64).unwrap();
    }
    txn.commit().unwrap();
    let ask = Ask {
        from: Hlc::ZERO,
        limit: 1000,
        held: held_of(&[Some(10), Some(10), Some(10), None]),
        marked: vec![],
        rows: None,
    };
    // The key walk itself fails on the key that does not decode; the serve
    // answers from the oplog.
    let txn = engine.db().begin_read().unwrap();
    let failed = engine.read_oplog_by_arrival_keys_in(
        &txn,
        Hlc::ZERO,
        1000,
        WalkScope::Request,
        |s: &Stamp| s.hlc <= ask.held.get(s.node),
        |_: &OplogEntry| true,
        None,
    );
    assert!(failed.is_err(), "an undecodable index key is the walk's error");
    drop(txn);
    let before = engine.serve_cost();
    let served =
        engine.serve_entries_to_peer(ask.from, ask.limit, Some(&ask.held), &[], None).unwrap();
    let after = engine.serve_cost();
    let expected = {
        let txn = engine.db().begin_read().unwrap();
        let passed = Cell::new(0u64);
        engine
            .read_oplog_linear_in(
                &txn,
                Hlc::ZERO,
                1000,
                WalkScope::Background,
                |s: &Stamp| {
                    let skip = s.hlc <= ask.held.get(s.node);
                    passed.set(passed.get() + u64::from(skip));
                    skip
                },
                |e: &OplogEntry| e.kind != OpKind::UniqueViolation,
                None,
            )
            .unwrap()
    };
    assert_eq!(served, expected, "the serve is the linear window");
    let slot =
        |s: &crate::ServeSnapshot| s.paths[ServeWalk::Serve.slot()][WalkPath::FallbackError.slot()];
    assert_eq!(slot(&after) - slot(&before), 1);
}

/// A window re-read to fit its frame is one confirmation push, not two: the
/// re-walk is not counted. A pull's re-walk is (`served_windows` says so).
#[test]
fn a_pushs_frame_refit_is_not_a_second_push_window() {
    let (engine, _dir) = engine();
    let rows: Vec<Row> =
        (0..20u64).map(|i| Row { origin: (i % 2) as usize, at: i, violation: false }).collect();
    write(&engine, &rows);
    let held = held_of(&[Some(5), None, None, None]);
    let pushes = |e: &Engine| e.serve_cost().paths[ServeWalk::Push.slot()];
    let budget = ExamineBudget::serve();
    engine
        .entries_for_peer_within(Hlc::ZERO, 100, Some(&held), WalkScope::Request, budget)
        .unwrap();
    assert_eq!(pushes(&engine)[WalkPath::Keys.slot()], 1);
    engine.entries_for_peer_refit(Hlc::ZERO, 10, Some(&held), WalkScope::Request, budget).unwrap();
    assert_eq!(pushes(&engine)[WalkPath::Keys.slot()], 1, "the refit is the same push");
}

/// What a key walk that gave up leaves the linear walk: the time it did not
/// spend, the rows and the floor as they were.
#[test]
fn the_linear_walk_after_a_fallback_has_only_the_time_that_was_left() {
    let floor = at(5);
    let budget = Some((ExamineBudget { time: Duration::from_millis(1_500), rows: 77 }, floor));
    let (left, kept_floor) =
        crate::sync::after_attempt(budget, Duration::from_millis(600)).unwrap();
    assert_eq!((left.time, left.rows, kept_floor), (Duration::from_millis(900), 77, floor));
    // A spent budget stays spent, never negative, and the floor still applies.
    let (left, _) = crate::sync::after_attempt(budget, Duration::from_secs(9)).unwrap();
    assert_eq!(left.time, Duration::ZERO);
    assert!(crate::sync::after_attempt(None, Duration::from_secs(1)).is_none());
}

// -- The repair keeps positions (ADR-197) ------------------------------------

fn arrival_positions(engine: &Engine) -> Vec<(u64, Vec<u8>)> {
    let txn = engine.db().begin_read().unwrap();
    let arrival = txn.open_table(tables::OPLOG_ARRIVAL).unwrap();
    redb::ReadableTable::iter(&arrival)
        .unwrap()
        .map(|row| {
            let (seq, key) = row.unwrap();
            (seq.value(), key.value().to_vec())
        })
        .collect()
}

/// Rows in arrival order whose stamp order is not: entries relayed late, with
/// a lower stamp than the local ones that came before them.
fn relayed_late() -> Vec<Row> {
    let at = [0u64, 1, 2, 3, 4, 5, 6, 7, 2, 4, 8, 9, 3, 10, 11, 12];
    at.iter()
        .enumerate()
        .map(|(i, at)| Row { origin: if i < 8 { 0 } else { 1 + i % 3 }, at: *at, violation: false })
        .collect()
}

/// A store whose arrival index the counts cannot see is wrong: a position
/// missing (and its stamp half), a stray position, a second position naming a
/// stamp, and a stray key in the stamp half. Returns the store's path and
/// the positions before the damage.
fn damaged_store(
    dir: &tempfile::TempDir,
    rows: &[Row],
) -> (std::path::PathBuf, Vec<(u64, Vec<u8>)>) {
    let path = dir.path().join("kimmy.redb");
    let engine = Engine::open(&path).unwrap();
    write(&engine, rows);
    let before = arrival_positions(&engine);
    let txn = engine.begin_write(WriterHolder::Write).unwrap();
    {
        let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL).unwrap();
        let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
        // Lost from the index, both halves: the middle of the order.
        let lost = codec::oplog_key(&stamp_of(&rows[10]));
        let lost_seq = by_stamp.remove(lost.as_slice()).unwrap().unwrap().value();
        assert!(arrival.remove(lost_seq).unwrap().is_some());
        // A position for an entry the oplog does not hold, beyond the end.
        let gone = codec::oplog_key(&Stamp::new(at(9_000), node(3)));
        arrival.insert(5_003u64, gone.as_slice()).unwrap();
        by_stamp.insert(gone.as_slice(), 5_003u64).unwrap();
        // A second position naming a stamp that has one.
        let named = codec::oplog_key(&stamp_of(&rows[2]));
        arrival.insert(5_001u64, named.as_slice()).unwrap();
    }
    txn.commit().unwrap();
    engine.close().unwrap();
    (path, before)
}

/// The repair is not a renumbering: arrival order and every surviving position
/// are what they were; a stray and a duplicate position are dropped; the entry
/// the index lost takes the next position past the highest ever issued.
#[test]
fn the_repair_keeps_every_surviving_position_and_appends_what_the_index_lost() {
    let dir = tempfile::tempdir().unwrap();
    let rows = relayed_late();
    let (path, before) = damaged_store(&dir, &rows);
    let engine = crate::verified::test_support::forcing(|| Engine::open(&path).unwrap());
    let after = arrival_positions(&engine);

    let lost = codec::oplog_key(&stamp_of(&rows[10]));
    let mut expected: Vec<(u64, Vec<u8>)> =
        before.iter().filter(|(_, k)| k.as_slice() != lost.as_slice()).cloned().collect();
    expected.push((5_004, lost.to_vec()));
    assert_eq!(after, expected);
    assert!(
        before.windows(2).any(|w| w[0].1 > w[1].1),
        "premise: arrival order is not stamp order in this store"
    );

    // Both halves are the oplog's, and agree with each other.
    let txn = engine.db().begin_read().unwrap();
    let by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
    let oplog = txn.open_table(tables::OPLOG).unwrap();
    for (seq, key) in &after {
        assert_eq!(by_stamp.get(key.as_slice()).unwrap().unwrap().value(), *seq);
        assert!(oplog.get(key.as_slice()).unwrap().is_some());
    }
    assert_eq!(redb::ReadableTableMetadata::len(&by_stamp).unwrap(), after.len() as u64);
    assert_eq!(redb::ReadableTableMetadata::len(&oplog).unwrap(), after.len() as u64);
}

/// A store from before the index existed has no positions: every entry takes
/// one in stamp order, from 0.
#[test]
fn a_store_with_no_arrival_index_is_numbered_in_stamp_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let rows: Vec<Row> =
        (0..20u64).map(|i| Row { origin: (i % 3) as usize, at: i, violation: false }).collect();
    {
        let engine = Engine::open(&path).unwrap();
        write(&engine, &rows);
        let txn = engine.begin_write(WriterHolder::Write).unwrap();
        {
            let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL).unwrap();
            let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
            for row in &rows {
                let key = codec::oplog_key(&stamp_of(row));
                let seq = by_stamp.remove(key.as_slice()).unwrap().unwrap().value();
                arrival.remove(seq).unwrap();
            }
        }
        txn.commit().unwrap();
        engine.close().unwrap();
    }
    let engine = Engine::open(&path).unwrap();
    let positions = arrival_positions(&engine);
    assert_eq!(positions.len(), rows.len());
    assert!(positions.iter().enumerate().all(|(i, (seq, _))| *seq == i as u64));
    assert!(positions.windows(2).all(|w| w[0].1 < w[1].1), "in stamp order");
}

/// A change stream resumed from a token at a stamp reads on from that stamp's
/// position; an entry relayed after it (with a lower stamp) must still be
/// delivered once the index has been repaired.
#[tokio::test]
async fn a_stream_resumed_after_the_repair_still_delivers_the_relayed_entry() {
    use crate::watch::{ChangeEvent, WatchOptions, WatchScope};
    use kimmy_core::ResumeToken;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let (token, coll_id, relayed_id) = {
        let engine = Engine::open(&path).unwrap();
        let coll = engine.create_collection("app", "docs").unwrap();
        for n in 0..3i64 {
            engine.insert(&coll, bson::doc! { "_id": n }).unwrap();
        }
        let local = engine.read_arrival_from(0, 100).unwrap().pop().unwrap().stamp;
        // Relayed after the local writes, stamped long before them.
        let relayed = OplogEntry {
            stamp: Stamp::new(Hlc::new(1_000, 0), node(2)),
            kind: OpKind::Insert,
            collection: coll.id,
            doc_id: Some(kimmy_core::DocId::Int64(99)),
            body: Some(bson::serialize_to_vec(&bson::doc! { "_id": 99i64 }).unwrap()),
        };
        assert!(engine.apply_remote(&coll, &relayed).unwrap());
        // Damage the index the way the counts cannot see.
        let victim = engine.read_arrival_from(0, 100).unwrap()[0].stamp;
        let txn = engine.begin_write(WriterHolder::Write).unwrap();
        {
            let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
            assert!(by_stamp.remove(codec::oplog_key(&victim).as_slice()).unwrap().is_some());
            let stray = codec::oplog_key(&Stamp::new(at(9_000), node(3)));
            by_stamp.insert(stray.as_slice(), 999_999u64).unwrap();
        }
        txn.commit().unwrap();
        let token = ResumeToken::from_stamp(local);
        engine.close().unwrap();
        (token, coll.id, 99i64)
    };
    let engine = crate::verified::test_support::forcing(|| Engine::open(&path).unwrap());
    let mut stream = engine
        .watch(
            WatchScope::Collection(coll_id),
            WatchOptions { resume_after: Some(token), ..Default::default() },
        )
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next(&engine))
        .await
        .expect("the relayed entry is delivered")
        .expect("the stream is open");
    match event {
        ChangeEvent::Change { entry, .. } => {
            assert_eq!(entry.doc_id, Some(kimmy_core::DocId::Int64(relayed_id)));
        }
        other => panic!("expected the relayed change, got {other:?}"),
    }
}

/// The record says the vector is verified, so it is written only after the
/// repair has committed: a failure inside the repair or just after it leaves no
/// record, and the next open walks and repairs again. The damage is what the
/// counts cannot see, so the verification walk is what finds it.
#[test]
fn no_record_is_written_until_the_repair_has_committed() {
    let rows = relayed_late();
    let stamp_half = |engine: &Engine| -> Vec<Vec<u8>> {
        let txn = engine.db().begin_read().unwrap();
        let by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
        redb::ReadableTable::iter(&by_stamp)
            .unwrap()
            .map(|r| r.unwrap().0.value().to_vec())
            .collect()
    };
    for after_commit in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (damaged, oplog_keys) = {
            let engine = Engine::open(&path).unwrap();
            write(&engine, &rows);
            let txn = engine.begin_write(WriterHolder::Write).unwrap();
            {
                let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ).unwrap();
                let key = codec::oplog_key(&stamp_of(&rows[10]));
                assert!(by_stamp.remove(key.as_slice()).unwrap().is_some());
                let stray = codec::oplog_key(&Stamp::new(at(9_000), node(3)));
                by_stamp.insert(stray.as_slice(), 999_999u64).unwrap();
            }
            txn.commit().unwrap();
            let damaged = stamp_half(&engine);
            let txn = engine.db().begin_read().unwrap();
            let oplog_keys: Vec<Vec<u8>> =
                redb::ReadableTable::iter(&txn.open_table(tables::OPLOG).unwrap())
                    .unwrap()
                    .map(|r| r.unwrap().0.value().to_vec())
                    .collect();
            drop(txn);
            engine.close().unwrap();
            (damaged, oplog_keys)
        };
        assert_ne!(damaged, oplog_keys, "premise: the stamp half is damaged");
        let schema = crate::migrate::SCHEMA_VERSION;
        // Nothing says the vector is verified, so the open walks.
        {
            let db = redb::Database::open(&path).unwrap();
            crate::verified::test_support::write_raw(&db, &[]);
            assert!(crate::verified::read_db(&db, schema).unwrap().is_none());
        }

        let failed = std::thread::spawn({
            let path = path.clone();
            move || {
                if after_commit {
                    crate::verified::test_support::fail_after_the_arrival_repair();
                } else {
                    crate::index::clear_hooks::keep_keys_of("the arrival index by stamp");
                }
                Engine::open(&path).map(drop).map_err(|e| e.to_string())
            }
        })
        .join()
        .unwrap();
        assert!(
            failed.is_err(),
            "the injected failure fails the open (after_commit: {after_commit})"
        );

        let db = redb::Database::open(&path).unwrap();
        assert!(
            crate::verified::read_db(&db, schema).unwrap().is_none(),
            "no record while the repair is unfinished (after_commit: {after_commit})"
        );
        drop(db);

        // After the failure inside the repair the store is as it was; after the
        // one past its commit it is repaired: either way the reopen ends whole
        // and records.
        let engine = Engine::open(&path).unwrap();
        assert!(engine.version_vector_verified().unwrap().is_some(), "the reopen records it");
        assert_eq!(stamp_half(&engine), oplog_keys, "and the index is the oplog's");
    }
}

/// A serve that fails on a body no walk can decode served no window, so it
/// does not use up the fallback line's ten-minute slot: the next fallback that
/// does serve one is still due its line.
#[test]
fn a_serve_that_fails_does_not_use_the_fallback_lines_slot() {
    let (engine, _dir) = engine();
    let rows: Vec<Row> =
        (0..20u64).map(|i| Row { origin: (i % 3) as usize, at: i, violation: false }).collect();
    write(&engine, &rows);
    let txn = engine.begin_write(WriterHolder::Write).unwrap();
    {
        let mut oplog = txn.open_table(tables::OPLOG).unwrap();
        let key = codec::oplog_key(&stamp_of(&rows[15]));
        oplog.insert(key.as_slice(), [0xFFu8; 4].as_slice()).unwrap();
    }
    txn.commit().unwrap();
    let held = held_of(&[Some(5), Some(5), Some(5), None]);
    let served = engine.serve_entries_to_peer(Hlc::ZERO, 1000, Some(&held), &[], None);
    assert!(served.is_err(), "the linear walk meets the same body: no window is served");
    assert!(engine.serve_counters().fallback_is_due_a_line(), "and the line's slot is still free");
}

/// A lost positions row whose stamp the stamp half still names keeps the
/// position, on a plain open (the counts differ, so the stale-count path
/// repairs): stamp 20 first, the relayed 15 after it, ten more; a token for 20
/// resumes at the entry after it and so still delivers 15 and the rest.
#[test]
fn a_lost_position_the_stamp_half_still_names_is_given_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let mut rows = vec![Row { origin: 0, at: 20, violation: false }];
    rows.push(Row { origin: 1, at: 15, violation: false });
    rows.extend((0..10).map(|i| Row { origin: 0, at: 21 + i, violation: false }));
    let before = {
        let engine = Engine::open(&path).unwrap();
        write(&engine, &rows);
        let before = arrival_positions(&engine);
        let txn = engine.begin_write(WriterHolder::Write).unwrap();
        {
            let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL).unwrap();
            assert!(arrival.remove(0u64).unwrap().is_some());
        }
        txn.commit().unwrap();
        engine.close().unwrap();
        before
    };
    let engine = Engine::open(&path).unwrap();
    assert_eq!(arrival_positions(&engine), before, "the position came back, not the top");
    let after_20 = engine.read_arrival_from(1, 100).unwrap();
    assert_eq!(after_20.len(), 11, "a token for 20 still reaches 15 and the ten after it");
    assert_eq!(after_20[0].stamp, stamp_of(&rows[1]));
}

/// The same with every positions row lost and the stamp half whole: each
/// entry gets its position back.
#[test]
fn a_store_that_lost_its_positions_but_not_its_stamp_half_gets_them_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let rows = relayed_late();
    let before = {
        let engine = Engine::open(&path).unwrap();
        write(&engine, &rows);
        let before = arrival_positions(&engine);
        let txn = engine.begin_write(WriterHolder::Write).unwrap();
        {
            let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL).unwrap();
            for (seq, _) in &before {
                arrival.remove(*seq).unwrap();
            }
        }
        txn.commit().unwrap();
        engine.close().unwrap();
        before
    };
    let engine = Engine::open(&path).unwrap();
    assert_eq!(arrival_positions(&engine), before);
}
