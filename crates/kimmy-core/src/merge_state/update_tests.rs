//! Tests of the multi-operator update (`Writer::write_update`, `write_update_stamped`): the refusals
//! ([`UpdateRefused`]), the no-ops, atomicity, and the stamped form. Each refusal leaves the writer exactly
//! as it was: no stamp, no clock move, no entry.

use std::collections::BTreeMap;

use super::tests::{fl, i, opts, s};
use super::*;
use crate::hlc::Hlc;
use crate::ids::NodeId;

fn np(dotted: &str) -> Path {
    Path::new(dotted.split('.')).unwrap()
}

fn doc(f: &[(&str, Value)]) -> Value {
    Value::Document(f.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect())
}

fn writer(name: u8) -> Writer {
    let id = WriterId { node: NodeId::from_bytes([name; 16]), epoch: Epoch::from_u64(0) };
    Writer::new(id, opts())
}

/// A writer holding `{a: 1, b: "str", m: {x: 1}, arr: [1], d: 2.5, s5: 5}`.
fn seeded() -> Writer {
    let mut w = writer(1);
    w.insert(
        &fl(&[
            ("a", i(1)),
            ("b", s("str")),
            ("m", doc(&[("x", i(1))])),
            ("arr", Value::Array(vec![i(1)])),
            ("d", Value::Double(2.5)),
        ]),
        0,
    )
    .unwrap();
    w
}

/// The update is refused with `want`, and the writer is as it was.
fn refused(w: &mut Writer, ops: &[UpdateOp], want: UpdateRefused) {
    let (state, clock) = (w.state.clone(), w.clock);
    assert_eq!(w.write_update(ops, 0).unwrap_err(), want, "{ops:?}");
    assert_eq!(w.state, state, "a refused update changed the state: {ops:?}");
    assert_eq!(w.clock, clock, "a refused update moved the clock: {ops:?}");
}

#[test]
fn each_refusal_is_named_and_changes_nothing() {
    let mut w = seeded();
    refused(
        &mut w,
        &[UpdateOp::Set(np("a"), i(2)), UpdateOp::Set(np("a"), i(3))],
        UpdateRefused::Overlap(np("a"), np("a")),
    );
    refused(
        &mut w,
        &[UpdateOp::Set(np("m"), doc(&[])), UpdateOp::Inc(np("m.x"), i(1))],
        UpdateRefused::Overlap(np("m"), np("m.x")),
    );
    refused(&mut w, &[UpdateOp::Set(np("a.q"), i(1))], UpdateRefused::BelowScalar(np("a.q")));
    refused(&mut w, &[UpdateOp::Inc(np("arr.k"), i(1))], UpdateRefused::BelowScalar(np("arr.k")));
    refused(
        &mut w,
        &[UpdateOp::Set(np("z"), Value::Map)],
        UpdateRefused::BadValue(np("z"), InvalidValue::MapMarker),
    );
    refused(
        &mut w,
        &[UpdateOp::Set(np("z"), doc(&[("k\0", i(1))]))],
        UpdateRefused::BadValue(np("z"), InvalidValue::BadFieldName),
    );
    // the $inc refusals
    refused(
        &mut w,
        &[UpdateOp::Inc(np("a"), s("x"))],
        UpdateRefused::IncOperandNotANumber(np("a")),
    );
    refused(
        &mut w,
        &[UpdateOp::Inc(np("a"), Value::Null)],
        UpdateRefused::IncOperandNotANumber(np("a")),
    );
    refused(&mut w, &[UpdateOp::Inc(np("b"), i(1))], UpdateRefused::IncTargetNotANumber(np("b")));
    refused(&mut w, &[UpdateOp::Inc(np("m"), i(1))], UpdateRefused::IncTargetIsAContainer(np("m")));
    refused(
        &mut w,
        &[UpdateOp::Inc(np("arr"), i(1))],
        UpdateRefused::IncTargetIsAContainer(np("arr")),
    );
    refused(
        &mut w,
        &[UpdateOp::Inc(np("a"), Value::Double(0.5))],
        UpdateRefused::IncDoubleOnCounter(np("a")),
    );
    refused(&mut w, &[UpdateOp::Inc(np("a"), i(i64::MAX))], UpdateRefused::IncOutOfRange(np("a")));
    // atomic: the good operators before and after a bad one are not written either
    refused(
        &mut w,
        &[UpdateOp::Set(np("z"), i(1)), UpdateOp::Inc(np("b"), i(1)), UpdateOp::Unset(np("a"))],
        UpdateRefused::IncTargetNotANumber(np("b")),
    );
    // no life
    let mut empty = writer(2);
    refused(&mut empty, &[UpdateOp::Set(np("a"), i(1))], UpdateRefused::NoLife);
}

#[test]
fn a_field_name_that_cannot_be_a_segment_is_a_bad_path() {
    // a path of 60 segments and a document 60 deep pass the document's own depth check and not the path's
    let mut w = seeded();
    let mut v = i(1);
    for _ in 0..60 {
        v = doc(&[("k", v)]);
    }
    let long = Path::new((0..60).map(|n| format!("p{n}"))).unwrap();
    let before = w.state.clone();
    let e = w.write_update(&[UpdateOp::Set(long, v)], 0).unwrap_err();
    assert!(matches!(e, UpdateRefused::BadPath(_)), "{e:?}");
    assert_eq!(w.state, before);
}

fn counter_of(n: usize) -> MergeState {
    let mut state = MergeState::default();
    for k in 0..n {
        let mut w = writer(k as u8 + 1);
        w.receive(&state);
        if k == 0 {
            w.insert(&fl(&[("n", i(0))]), 0).unwrap();
        }
        w.write_inc(&np("n"), 1, 0).unwrap();
        state = w.state;
    }
    state
}

#[test]
fn the_caps_are_refusals_of_the_update() {
    // a 65th contributor
    let full = counter_of(MAX_CONTRIBUTORS);
    let mut newcomer = writer(200);
    newcomer.receive(&full);
    refused(
        &mut newcomer,
        &[UpdateOp::Set(np("other"), i(1)), UpdateOp::Inc(np("n"), i(1))],
        UpdateRefused::TooManyContributors(np("n")),
    );
    let mut old = writer(5);
    old.receive(&full);
    assert!(old.write_update(&[UpdateOp::Inc(np("n"), i(1))], 0).unwrap().is_some());
    // a 257th writer: node ids are 16 bytes, so the epoch separates the writers past 255
    let mut state = MergeState::default();
    for k in 0..MAX_WRITERS {
        let id = WriterId {
            node: NodeId::from_bytes([(k % 200) as u8 + 1; 16]),
            epoch: Epoch::from_u64((k / 200) as u64),
        };
        let mut w = Writer::new(id, opts());
        w.receive(&state);
        if k == 0 {
            w.insert(&fl(&[("n", i(0))]), 0).unwrap();
        }
        w.write_set(&np(&format!("f{k}")), i(k as i64), 0).unwrap();
        state = w.state;
    }
    assert_eq!(state.writers().len(), MAX_WRITERS);
    let late_id = WriterId { node: NodeId::from_bytes([250; 16]), epoch: Epoch::from_u64(9) };
    let mut late = Writer::new(late_id, opts());
    late.receive(&state);
    refused(&mut late, &[UpdateOp::Set(np("late"), i(1))], UpdateRefused::WriterCap);
}

#[test]
fn an_update_that_changes_nothing_is_ok_none_with_no_stamp() {
    let mut w = seeded();
    let (state, clock) = (w.state.clone(), w.clock);
    for ops in [
        vec![],
        vec![UpdateOp::Set(np("a"), i(1))],
        vec![UpdateOp::Unset(np("nothing"))],
        vec![UpdateOp::Inc(np("d"), Value::Double(0.0))],
        vec![UpdateOp::Set(np("m.x"), i(1)), UpdateOp::Unset(np("gone.too"))],
    ] {
        assert_eq!(w.write_update(&ops, 0), Ok(None), "{ops:?}");
        assert_eq!(w.state, state);
        assert_eq!(w.clock, clock);
    }
}

#[test]
fn an_update_is_one_write_with_one_stamp_in_the_order_of_the_list() {
    let mut w = seeded();
    let r = w
        .write_update(
            &[
                UpdateOp::Set(np("z"), i(1)),
                UpdateOp::Unset(np("a")),
                UpdateOp::Inc(np("y"), i(2)),
                UpdateOp::Set(np("arr"), Value::Array(vec![i(1), i(2)])),
                UpdateOp::Set(np("k.w"), i(3)),
            ],
            0,
        )
        .unwrap()
        .unwrap();
    let doc = w.state.ordered_document(&w.opts).unwrap();
    let names: Vec<&str> = doc.iter().map(|(n, _)| n.as_str()).collect();
    // b, m, arr, d keep their places (arr is set in place); the new fields follow in the order of the list
    assert_eq!(names, ["b", "m", "arr", "d", "z", "y", "k"]);
    let vis = w.visible().unwrap();
    assert_eq!(
        vis.get(&np("arr")),
        Some(&Value::Array(vec![i(1), i(2)])),
        "an array is one atomic value"
    );
    assert!(!vis.contains_key(&np("a")));
    assert_eq!(vis.get(&np("y")), Some(&i(2)));
    assert_eq!(vis.get(&np("k")), Some(&Value::Map));
    // one stamp: every cell the update wrote has it
    let st = r.stamp;
    let written: BTreeMap<_, _> = w
        .state
        .regs
        .iter()
        .filter(|(_, reg)| reg.stamp == st)
        .map(|((_, p), _)| p.to_string())
        .map(|p| (p, ()))
        .collect();
    assert_eq!(
        written.keys().cloned().collect::<Vec<_>>(),
        ["a", "arr", "k", "k.w", "z"],
        "the registers of the one stamp (a unset, an array, a marker, a leaf, a leaf)"
    );
    assert!(w.state.slots.values().any(|sl| sl.stamp == st), "and the slot of the $inc");
}

#[test]
fn the_stamped_form_asks_for_a_stamp_once_and_only_when_it_writes() {
    let mut w = seeded();
    let top = w.clock;
    let next = MStamp { hlc: Hlc::new(top.wall_ms + 10, 0), writer: w.id };
    let mut asked = 0;
    // refused: no stamp asked, UpdateError::Refused
    let e = w
        .write_update_stamped(&[UpdateOp::Inc(np("b"), i(1))], &mut || {
            asked += 1;
            next
        })
        .unwrap_err();
    assert_eq!(e, UpdateError::Refused(UpdateRefused::IncTargetNotANumber(np("b"))));
    // a no-op: no stamp asked
    assert_eq!(
        w.write_update_stamped(&[UpdateOp::Set(np("a"), i(1))], &mut || {
            asked += 1;
            next
        }),
        Ok(None)
    );
    assert_eq!(asked, 0);
    // a stamp that is not above everything the writer has used is refused, and nothing is written
    let state = w.state.clone();
    let low = MStamp { hlc: top, writer: w.id };
    assert_eq!(
        w.write_update_stamped(&[UpdateOp::Set(np("z"), i(1))], &mut || low),
        Err(UpdateError::Stamp(StampError::NotAbove { top, stamp: top }))
    );
    assert_eq!(w.state, state);
    // the stamp is another writer's
    let other = WriterId { node: NodeId::from_bytes([9; 16]), epoch: Epoch::from_u64(0) };
    let theirs = MStamp { hlc: next.hlc, writer: other };
    assert_eq!(
        w.write_update_stamped(&[UpdateOp::Set(np("z"), i(1))], &mut || theirs),
        Err(UpdateError::Stamp(StampError::OtherWriter { writer: w.id, stamp: other }))
    );
    // a good stamp is the write's stamp, asked for once
    let r = w
        .write_update_stamped(&[UpdateOp::Set(np("z"), i(1))], &mut || {
            asked += 1;
            next
        })
        .unwrap()
        .unwrap();
    assert_eq!((r.stamp, asked, w.clock), (next, 1, next.hlc));
}

/// The overlap check the update used before it was O(n log n): every pair, the first by operator order. The sorted check
/// must refuse exactly when this does, with the same pair.
fn reference_overlap(ops: &[UpdateOp]) -> Option<(Path, Path)> {
    let paths: Vec<&Path> = ops
        .iter()
        .map(|o| match o {
            UpdateOp::Set(p, _) | UpdateOp::Unset(p) | UpdateOp::Inc(p, _) => p,
        })
        .collect();
    for (i, a) in paths.iter().enumerate() {
        for b in &paths[i + 1..] {
            if a == b || a.is_below(b) || b.is_below(a) {
                return Some(((*a).clone(), (*b).clone()));
            }
        }
    }
    None
}

fn overlap_of(ops: &[UpdateOp]) -> Option<(Path, Path)> {
    match seeded().write_update(ops, 0) {
        Err(UpdateRefused::Overlap(a, b)) => Some((a, b)),
        _ => None,
    }
}

#[test]
fn the_overlap_check_finds_every_kind_of_overlap_and_reports_the_pair_in_operator_order() {
    let set = |p: &str| UpdateOp::Set(np(p), i(1));
    // an equal pair
    assert_eq!(overlap_of(&[set("q"), set("q")]), Some((np("q"), np("q"))));
    // a prefix pair far apart in the input, the extension first and the prefix last
    let mut ops = vec![set("q.b.c")];
    ops.extend((0..50).map(|n| set(&format!("s{n}"))));
    ops.push(set("q.b"));
    assert_eq!(overlap_of(&ops), Some((np("q.b.c"), np("q.b"))));
    // unrelated neighbours in the order of the paths (a name that starts with another is not below it)
    assert_eq!(overlap_of(&[set("q.a"), set("q.b"), set("qa"), set("r")]), None);
    assert!(
        seeded().write_update(&[set("q.a"), set("q.b"), set("qa"), set("r")], 0).unwrap().is_some()
    );
    // many siblings with one overlap hidden in the middle
    let mut ops: Vec<UpdateOp> = (0..1000).map(|n| set(&format!("p{n}"))).collect();
    ops.insert(500, set("p700.child"));
    assert_eq!(overlap_of(&ops), Some((np("p700.child"), np("p700"))));
    // the same as the pairwise check on many random small lists
    let mut x = 0x2545_f491_4f6c_dd1du64;
    let mut next = |n: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        (x % n) as usize
    };
    let names = ["a", "b", "ab", "a.b", "a.b.c", "a.c", "b.a", "c", "c.c", "c.c.c"];
    let (mut refused, mut accepted) = (0, 0);
    for _ in 0..3000 {
        let n = 1 + next(6);
        let ops: Vec<UpdateOp> = (0..n).map(|_| set(names[next(names.len() as u64)])).collect();
        let want = reference_overlap(&ops);
        if want.is_some() {
            refused += 1;
        } else {
            accepted += 1;
        }
        assert_eq!(overlap_of(&ops), want, "{ops:?}");
    }
    assert!(refused > 500 && accepted > 100, "{refused} refused, {accepted} accepted");
}

/// The overlap check is O(n log n): 300,000 no-op `$unset`s of distinct paths complete (the pairwise check was about
/// 4.5 * 10^10 path comparisons here, minutes in a debug build).
#[test]
fn a_large_update_is_checked_for_overlap_in_n_log_n() {
    let ops: Vec<UpdateOp> =
        (0..300_000).map(|n| UpdateOp::Unset(Path::flat(&format!("p{n}")).unwrap())).collect();
    let mut w = seeded();
    let t = std::time::Instant::now();
    assert_eq!(w.write_update(&ops, 0), Ok(None), "every operator is a no-op");
    eprintln!("300,000 operators: {:?}", t.elapsed());
    // and one overlap at the very end is found
    let mut ops = ops;
    ops.push(UpdateOp::Unset(Path::new(["p123", "child"]).unwrap()));
    let e = w.write_update(&ops, 0).unwrap_err();
    assert_eq!(
        e,
        UpdateRefused::Overlap(Path::flat("p123").unwrap(), Path::new(["p123", "child"]).unwrap())
    );
}

#[test]
fn sees_counter_is_true_only_for_int_and_decimal_cells() {
    let mut w = writer(1);
    assert!(!w.sees_counter(&np("n")), "no life");
    w.insert(
        &fl(&[
            ("i32", Value::Int32(1)),
            ("i64", i(1)),
            ("dec", Value::Decimal128([0x30; 16])),
            ("dbl", Value::Double(1.5)),
            ("str", s("x")),
            ("m", doc(&[("x", i(1))])),
        ]),
        0,
    )
    .unwrap();
    for p in ["i32", "i64", "dec", "m.x"] {
        assert!(w.sees_counter(&np(p)), "{p}");
    }
    for p in ["dbl", "str", "m", "missing", "m.missing"] {
        assert!(!w.sees_counter(&np(p)), "{p}");
    }
    // a counter made by an increment of an absent field
    w.write_inc(&np("fresh"), 1, 0).unwrap();
    assert!(w.sees_counter(&np("fresh")));
    // read-only: the state did not move
    let before = w.state.clone();
    let _ = w.sees_counter(&np("i64"));
    assert_eq!(w.state, before);
}

#[test]
fn a_register_set_over_a_counter_of_two_writers_folds_what_it_saw() {
    // slots of two writers at `n` (counter_of: base 0, one increment each)
    let both = counter_of(2);
    let mut a = writer(1);
    a.receive(&both);
    let mut b = writer(2);
    b.receive(&both);
    assert_eq!(a.visible().unwrap()[&np("n")], i(2));
    // writer a sets n = 100 (a register write over both slots: the fold)
    a.write_update(&[UpdateOp::Set(np("n"), i(100))], 0).unwrap().unwrap();
    assert_eq!(a.visible().unwrap()[&np("n")], i(100));
    // writer b, which has not seen the set, adds 5: an unseen increment adds on top
    b.write_update(&[UpdateOp::Inc(np("n"), i(5))], 0).unwrap().unwrap();
    a.receive(&b.state);
    b.receive(&a.state);
    assert_eq!(a.state, b.state);
    assert_eq!(a.visible().unwrap()[&np("n")], i(105));
}

#[test]
fn sees_counter_at_or_under_looks_through_sub_documents_but_not_arrays() {
    let mut w = writer(1);
    w.insert(
        &fl(&[
            ("withint", doc(&[("x", s("a")), ("c", i(1))])),
            ("deep", doc(&[("l1", doc(&[("l2", doc(&[("c", Value::Int32(1))]))]))])),
            ("plain", doc(&[("d", Value::Double(1.5)), ("s", s("t"))])),
            ("arr", Value::Array(vec![i(1), i(2)])),
            ("n", i(1)),
        ]),
        0,
    )
    .unwrap();
    for p in ["withint", "deep", "deep.l1", "n", "deep.l1.l2.c"] {
        assert!(w.sees_counter_at_or_under(&np(p)), "{p}");
    }
    for p in ["plain", "arr", "missing", "plain.d"] {
        assert!(!w.sees_counter_at_or_under(&np(p)), "{p}");
    }
    // a sibling sharing a name prefix is not under the path
    assert!(!w.sees_counter_at_or_under(&np("with")));
}
