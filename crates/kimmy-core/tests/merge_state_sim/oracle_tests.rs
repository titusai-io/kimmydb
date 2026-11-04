//! Tests of the oracle itself: `sound_record` must reject a record that the log does not back.
//! Each case builds a small causal log with the simulated nodes, and checks one genuine record
//! (accepted) and the false records a lenient check would pass (rejected).

use kimmy_core::merge_state::{LossKey, LossKind, MStamp, Value};

use crate::oracle::{self, Doc, Log};
use crate::sim::{Cfg, Node, fixpoint, np, p, pull, sub};

fn key(kind: LossKind, path: &str, stamp: MStamp) -> LossKey {
    if path == "_" { LossKey::document(kind, stamp) } else { LossKey::field(kind, np(path), stamp) }
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

/// The three cells of `a.d.f` as the flat map the oracle and the states return.
fn deep(scalar_a: Option<i64>, f: i64) -> Doc {
    let mut d = Doc::new();
    d.insert(p("a"), Value::Map);
    d.insert(np("a.d"), Value::Map);
    d.insert(np("a.d.f"), Value::Int64(f));
    if let Some(n) = scalar_a {
        // a scalar at `a` that hides everything below it
        d = Doc::new();
        d.insert(p("a"), Value::Int64(n));
    }
    d
}

/// A inserts {a: {d: {e: 1}}}; B and C take it; B sets the scalar `a` and C sets `a.d.f` two
/// levels below it, concurrently. `b_newer` says whose clock is ahead.
fn scalar_against_a_deep_write(b_newer: bool) -> (Log, Vec<Node>, MStamp, MStamp) {
    let cfg = Cfg::default_ret();
    let mut log = Log::new();
    let mut ms: Vec<Node> = "ABC".chars().map(|c| Node::new(c, &cfg)).collect();
    ms[0].insert(&[("a", sub(&[("d", sub(&[("e", Value::Int64(1))]))]))], &mut log, 0).unwrap();
    pull(&mut ms, 1, 0, None);
    pull(&mut ms, 2, 0, None);
    let (skew_b, skew_c) = if b_newer { (4, 0) } else { (0, 4) };
    let b = ms[1].set("a", Value::Int64(5), &mut log, skew_b).unwrap();
    let c = ms[2].set_at(&np("a.d.f"), Value::Int64(7), &mut log, skew_c).unwrap();
    fixpoint(&mut ms, 40);
    (log, ms, b, c)
}

#[test]
fn a_newer_deep_write_makes_the_scalar_a_map_and_the_scalar_loses() {
    let (log, ms, b, c) = scalar_against_a_deep_write(false);
    assert!(b < c);
    let ret = Cfg::default_ret().opts.retention;
    let (doc, alive, dels) = oracle::oracle(&log, ret, i64::MAX);
    // only what is newer than the scalar shows below it: `a.d.f`, not `a.d.e`
    assert_eq!(doc, Some(deep(None, 7)));
    assert!(ms.iter().all(|m| m.visible() == doc));
    let ok = |k: LossKey| oracle::sound_record(&k, &log, &alive, &dels, ret);
    assert!(ok(key(LossKind::TypeClash, "a", b)), "the scalar lost to a cell two levels down");
    assert!(ms[0].state().losses.contains_key(&key(LossKind::TypeClash, "a", b)));
    assert!(!ok(key(LossKind::TypeClash, "a.d.f", c)), "the newer write did not lose");
}

#[test]
fn a_newer_scalar_hides_the_deep_write_and_the_deep_write_is_the_record() {
    let (log, ms, b, c) = scalar_against_a_deep_write(true);
    assert!(c < b);
    let ret = Cfg::default_ret().opts.retention;
    let (doc, alive, dels) = oracle::oracle(&log, ret, i64::MAX);
    assert_eq!(doc, Some(deep(Some(5), 0)));
    assert!(ms.iter().all(|m| m.visible() == doc));
    let ok = |k: LossKey| oracle::sound_record(&k, &log, &alive, &dels, ret);
    assert!(ok(key(LossKind::TypeClash, "a.d.f", c)), "the hidden cell is reported at its path");
    assert!(ms[0].state().losses.contains_key(&key(LossKind::TypeClash, "a.d.f", c)));
    assert!(!ok(key(LossKind::TypeClash, "a", b)), "the newer scalar did not lose");
    assert!(!ok(key(LossKind::TypeClash, "a.d.e", c)), "C never wrote a.d.e");
}

#[test]
fn the_leaf_effects_of_a_nested_write_are_the_changed_leaves() {
    use crate::oracle::{Expected, Intent, expected_effects};
    let mut old = Doc::new();
    old.insert(p("a"), Value::Map);
    old.insert(np("a.b"), Value::Int64(1));
    old.insert(np("a.d"), Value::Map);
    old.insert(np("a.d.e"), Value::Int64(3));
    // a sub-document that keeps `a.b`, drops `a.d` and its leaf, and adds `a.c`
    let Expected::Effects(e) = expected_effects(
        Some(&old),
        &Intent::Set(p("a"), sub(&[("b", Value::Int64(1)), ("c", Value::Int64(2))])),
    ) else {
        panic!("refused")
    };
    let want: Doc = [
        (np("a.c"), Value::Int64(2)),
        (np("a.d"), Value::Unset),
        (np("a.d.e"), Value::Unset),
        (p("a"), Value::Map),
    ]
    .into_iter()
    .collect();
    assert_eq!(e, want, "a.b is unchanged and is not written; a.d and below are unset");
    // the same document again is a no-op
    let same = Intent::Set(np("a.b"), Value::Int64(1));
    assert_eq!(expected_effects(Some(&old), &same), Expected::Effects(Default::default()));
    // below a scalar is refused
    assert_eq!(
        expected_effects(Some(&old), &Intent::Set(np("a.b.q"), Value::Int64(1))),
        Expected::Refused
    );
}

// ---- the read-your-writes oracle (`order_oracle.rs`) on small documents ------------------------

mod order_oracle_cases {
    use std::collections::BTreeSet;

    use kimmy_core::merge_state::{OrderedValue, Value};

    use crate::order_oracle::{OrdOp, check, reorders};
    use crate::sim::np;

    type Doc = Vec<(String, OrderedValue)>;

    fn leaf(n: &str) -> (String, OrderedValue) {
        (n.to_string(), OrderedValue::Leaf(Value::Int64(1)))
    }
    fn empty_map(n: &str) -> (String, OrderedValue) {
        (n.to_string(), OrderedValue::Doc(vec![("c".to_string(), OrderedValue::Doc(Vec::new()))]))
    }

    fn history(of: &[&[&str]]) -> BTreeSet<Vec<String>> {
        of.iter().map(|p| p.iter().map(|s| (*s).to_string()).collect()).collect()
    }

    fn set_n(before: &Doc, after: &Doc, hist: &BTreeSet<Vec<String>>) -> Result<bool, String> {
        check(Some(before), Some(after), &OrdOp::Set(np("n"), Value::Int64(1)), hist, true)
            .map(|c| c.trailing)
    }

    /// O2-3: maps with no leaf below them and no history have no place; a field created after them goes BEFORE them, and
    /// they trail in path order (`k` before `m`), wherever they were in the document.
    #[test]
    fn maps_without_a_place_trail_in_path_order() {
        let hist = history(&[&["a"]]);
        let before: Doc = vec![leaf("a"), empty_map("k"), empty_map("m")];
        let ok: Doc = vec![leaf("a"), leaf("n"), empty_map("k"), empty_map("m")];
        assert_eq!(
            set_n(&before, &ok, &hist),
            Ok(true),
            "decided, on a level with maps that have no place"
        );
        // before the maps' path order is wrong (m before k), after the fields with a place is wrong (n last)
        let unsorted: Doc = vec![leaf("a"), leaf("n"), empty_map("m"), empty_map("k")];
        assert!(set_n(&before, &unsorted, &hist).is_err());
        let last: Doc = vec![leaf("a"), empty_map("k"), empty_map("m"), leaf("n")];
        assert!(set_n(&before, &last, &hist).is_err());
        let first: Doc = vec![leaf("n"), leaf("a"), empty_map("k"), empty_map("m")];
        assert!(set_n(&before, &first, &hist).is_err());
        // a map WITH a leaf below it has a place: the new field is after it
        let with_leaf: Doc =
            vec![leaf("a"), ("m".to_string(), OrderedValue::Doc(vec![leaf("x")])), empty_map("k")];
        let after: Doc = vec![
            leaf("a"),
            ("m".to_string(), OrderedValue::Doc(vec![leaf("x")])),
            leaf("n"),
            empty_map("k"),
        ];
        assert_eq!(set_n(&with_leaf, &after, &hist), Ok(true));
        // and a map with history has one too
        let hist_m = history(&[&["a"], &["m"]]);
        let placed: Doc = vec![leaf("a"), leaf("n"), empty_map("k"), empty_map("m")];
        assert!(set_n(&before, &placed, &hist_m).is_err(), "m has a place now: n goes after it");
    }

    /// O2-4: a replace that puts the same fields in another order is a write; the same order is not.
    #[test]
    fn a_replace_in_another_order_is_a_write() {
        let before: Doc = vec![leaf("a"), leaf("b")];
        let swapped = OrdOp::Make(vec![
            ("b".to_string(), Value::Int64(1)),
            ("a".to_string(), Value::Int64(1)),
        ]);
        let same = OrdOp::Make(vec![
            ("a".to_string(), Value::Int64(1)),
            ("b".to_string(), Value::Int64(1)),
        ]);
        assert!(reorders(Some(&before), &swapped));
        assert!(!reorders(Some(&before), &same));
        // a sub-document set, at a path
        let nested: Doc = vec![("m".to_string(), OrderedValue::Doc(vec![leaf("p"), leaf("q")]))];
        let set = |a: &str, b: &str| {
            OrdOp::Set(
                np("m"),
                Value::Document(vec![
                    (a.to_string(), Value::Int64(1)),
                    (b.to_string(), Value::Int64(1)),
                ]),
            )
        };
        assert!(reorders(Some(&nested), &set("q", "p")));
        assert!(!reorders(Some(&nested), &set("p", "q")));
        assert!(!reorders(Some(&nested), &set("p", "z")), "other fields: not a reorder");
    }
}
