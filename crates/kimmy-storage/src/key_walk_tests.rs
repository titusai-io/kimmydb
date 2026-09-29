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
    let slot = |s: &crate::ServeSnapshot| {
        s.paths[ServeWalk::Serve.slot()][WalkPath::FallbackMissingBody.slot()]
    };
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
