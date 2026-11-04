//! Tests of the oracle itself: `sound_record` must reject a record that the log does not back.
//! Each case builds a small causal log with the simulated nodes, and checks one genuine record
//! (accepted) and the false records a lenient check would pass (rejected).

use kimmy_core::merge_state::{LossKey, LossKind, MStamp, Value};

use crate::oracle::{self, Log};
use crate::sim::{Cfg, Node, fixpoint, p, pull};

fn key(kind: LossKind, path: &str, stamp: MStamp) -> LossKey {
    LossKey { kind, path: p(path), stamp }
}

/// The oracle's `alive` and `dels` for a finished log.
fn facts(log: &Log, ret: u64) -> (std::collections::BTreeSet<MStamp>, Vec<MStamp>) {
    let (_, alive, dels) = oracle::oracle(log, ret, i64::MAX);
    (alive, dels)
}

#[test]
fn deletewon_must_name_a_path_the_write_wrote() {
    // A inserts {x} and deletes; B, which had only seen the insert, sets x concurrently.
    let cfg = Cfg::default_ret();
    let ret = cfg.opts.retention;
    let mut log = Log::new();
    let mut ms: Vec<Node> = "AB".chars().map(|c| Node::new(c, &cfg)).collect();
    ms[0].insert(&[("x", Value::Int64(1))], &mut log, 0).unwrap();
    pull(&mut ms, 1, 0, None);
    ms[0].delete(&mut log, 0).unwrap();
    let b2 = ms[1].set("x", Value::Int64(5), &mut log, 0).unwrap();
    fixpoint(&mut ms, 40);
    let (alive, dels) = facts(&log, ret);
    assert!(alive.is_empty());
    let ok = |k: LossKey| oracle::sound_record(&k, &log, &alive, &dels, ret);
    assert!(ok(key(LossKind::DeleteWon, "x", b2)), "the genuine record");
    assert!(ms[0].state().losses.contains_key(&key(LossKind::DeleteWon, "x", b2)));
    assert!(!ok(key(LossKind::DeleteWon, "no_such_field", b2)), "a path B never wrote");
    assert!(!ok(key(LossKind::DeleteWon, "_", b2)), "the whole document is not a write's path");
}

#[test]
fn insertlost_must_name_a_path_the_write_wrote() {
    // A first insert {x}; C takes it and writes `note` into it; B, newer, never saw it and
    // inserts {y}. A's insert is lost to B's, and so is C's write into it.
    let cfg = Cfg::default_ret();
    let ret = cfg.opts.retention;
    let mut log = Log::new();
    let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, &cfg)).collect();
    let a1 = ms[0].insert(&[("x", Value::Int64(1))], &mut log, 0).unwrap();
    pull(&mut ms, 2, 0, None);
    let c2 = ms[2].set("note", Value::String("c".into()), &mut log, 0).unwrap();
    ms[1].insert(&[("y", Value::Int64(2))], &mut log, 4).unwrap();
    fixpoint(&mut ms, 40);
    let (alive, dels) = facts(&log, ret);
    let ok = |k: LossKey| oracle::sound_record(&k, &log, &alive, &dels, ret);
    assert!(ok(key(LossKind::InsertLost, "_", a1)), "the lost document");
    assert!(ok(key(LossKind::InsertLost, "note", c2)), "the lost write");
    assert!(ms[0].state().losses.contains_key(&key(LossKind::InsertLost, "note", c2)));
    assert!(!ok(key(LossKind::InsertLost, "bogus", c2)), "a path C never wrote");
    assert!(!ok(key(LossKind::InsertLost, "_", c2)), "C's write is not a whole insert");
}

#[test]
fn deletewon_needs_a_delete_that_killed_the_life() {
    // Retention 10. A inserts {x} and deletes at 2. B inserts {y} at 41 and C inserts {w} at 60,
    // both without having seen anything: B's insert is superseded by C's. The only delete is
    // older than B's life minus the retention, so it did not kill it.
    let cfg = Cfg::new(10);
    let ret = cfg.opts.retention;
    let mut log = Log::new();
    let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, &cfg)).collect();
    ms[0].insert(&[("x", Value::Int64(1))], &mut log, 0).unwrap();
    ms[0].delete(&mut log, 0).unwrap();
    let b = ms[1].insert(&[("y", Value::Int64(2))], &mut log, 40).unwrap();
    ms[2].insert(&[("w", Value::Int64(3))], &mut log, 59).unwrap();
    assert_eq!((b.hlc.wall_ms, ret), (41, 10));
    let (alive, dels) = facts(&log, ret);
    assert_eq!(dels.len(), 1);
    assert!(!alive.contains(&b), "B's insert is superseded");
    let ok = |k: LossKey| oracle::sound_record(&k, &log, &alive, &dels, ret);
    assert!(ok(key(LossKind::InsertLost, "_", b)), "the genuine record");
    assert!(!ok(key(LossKind::DeleteWon, "_", b)), "the delete is beyond B's retention");
    assert!(!ok(key(LossKind::DeleteWon, "y", b)), "the delete is beyond B's retention");
    // With a retention that reaches back to the delete it did kill the life.
    let (alive, dels) = facts(&log, 1_000_000_000);
    let ok = |k: LossKey| oracle::sound_record(&k, &log, &alive, &dels, 1_000_000_000);
    assert!(ok(key(LossKind::DeleteWon, "y", b)));
}

#[test]
fn clash_must_name_a_path_the_write_wrote() {
    // A inserts {x, y}, then sets x (A@2) and y (A@3); B, which had only the insert, sets y later
    // by its clock (B@6). Only A's write of y clashes with B's: A's write of x never touched y.
    let cfg = Cfg::default_ret();
    let ret = cfg.opts.retention;
    let mut log = Log::new();
    let mut ms: Vec<Node> = "AB".chars().map(|c| Node::new(c, &cfg)).collect();
    ms[0].insert(&[("x", Value::Int64(1)), ("y", Value::Int64(1))], &mut log, 0).unwrap();
    pull(&mut ms, 1, 0, None);
    let a_x = ms[0].set("x", Value::Int64(5), &mut log, 0).unwrap();
    let a_y = ms[0].set("y", Value::Int64(2), &mut log, 0).unwrap();
    let b_y = ms[1].set("y", Value::Int64(9), &mut log, 4).unwrap();
    assert!(a_y < b_y && a_x < b_y);
    fixpoint(&mut ms, 40);
    let (alive, dels) = facts(&log, ret);
    let ok = |k: LossKey| oracle::sound_record(&k, &log, &alive, &dels, ret);
    assert!(ok(key(LossKind::Clash, "y", a_y)), "the genuine clash");
    assert!(ms[0].state().losses.contains_key(&key(LossKind::Clash, "y", a_y)));
    assert!(!ok(key(LossKind::Clash, "y", a_x)), "A's write of x never wrote y");
    assert!(!ok(key(LossKind::Clash, "nope", a_y)), "a path nobody wrote");
}
