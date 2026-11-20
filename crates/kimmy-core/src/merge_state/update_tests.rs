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

/// A NUL in a field name is refused whole as a bad value before any leaf path is built: the value
/// check is the first to see it, so `BadPath` cannot come out of a `$set` any more (the over-long
/// leaf path it used to guard is `TooDeep` now, see the next test).
#[test]
fn a_field_name_that_cannot_be_a_segment_is_refused_whole() {
    let mut w = seeded();
    let before = w.state.clone();
    let v = doc(&[("a", doc(&[("b\0c", i(1))]))]);
    let e = w.write_update(&[UpdateOp::Set(np("p"), v)], 0).unwrap_err();
    assert!(matches!(e, UpdateRefused::BadValue(_, InvalidValue::BadFieldName)), "{e:?}");
    assert_eq!(w.state, before);
}

#[test]
fn a_path_and_its_value_count_together_against_the_depth_limit() {
    let nest = |levels: usize| (0..levels).fold(i(1), |v, _| doc(&[("k", v)]));
    let long = |n: usize| Path::new((0..n).map(|k| format!("p{k}"))).unwrap();
    // 60 segments and a document 60 deep: 120 levels, refused whole
    let mut w = seeded();
    let before = w.state.clone();
    let e = w.write_update(&[UpdateOp::Set(long(60), nest(60))], 0).unwrap_err();
    assert!(matches!(e, UpdateRefused::BadValue(_, InvalidValue::TooDeep)), "{e:?}");
    assert_eq!(w.state, before);
    // exactly at the limit (60 + 40) is accepted, one level over (60 + 41) is refused
    let mut w = seeded();
    w.write_update(&[UpdateOp::Set(long(60), nest(40))], 0).unwrap();
    let mut w = seeded();
    let e = w.write_update(&[UpdateOp::Set(long(60), nest(41))], 0).unwrap_err();
    assert!(matches!(e, UpdateRefused::BadValue(_, InvalidValue::TooDeep)), "{e:?}");
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
fn is_incremented_number_at_or_under_looks_through_sub_documents_but_not_arrays() {
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
    // a base with no slot is no counter, wherever it sits
    for p in ["withint", "deep", "deep.l1", "n", "deep.l1.l2.c", "plain", "arr", "missing"] {
        assert!(!w.is_incremented_number_at_or_under(&np(p)), "{p}");
    }
    w.write_inc(&np("withint.c"), 1, 0).unwrap();
    w.write_inc(&np("deep.l1.l2.c"), 1, 0).unwrap();
    w.write_inc(&np("n"), 1, 0).unwrap();
    for p in ["withint", "withint.c", "deep", "deep.l1", "n", "deep.l1.l2.c"] {
        assert!(w.is_incremented_number_at_or_under(&np(p)), "{p}");
    }
    for p in ["plain", "arr", "missing", "plain.d", "withint.x"] {
        assert!(!w.is_incremented_number_at_or_under(&np(p)), "{p}");
    }
    // a sibling sharing a name prefix is not under the path
    assert!(!w.is_incremented_number_at_or_under(&np("with")));
}

#[test]
fn the_renaming_entry_refuses_a_counter_source_before_the_clock_moves() {
    let mut w = seeded();
    w.write_inc(&np("n"), 1, 0).unwrap();
    let (state, clock) = (w.state.clone(), w.clock);
    let ops = [UpdateOp::Unset(np("n")), UpdateOp::Set(np("m"), i(1))];
    let r = w.write_update_renaming(&ops, &[np("n")], 0);
    assert_eq!(r, Err(UpdateRefused::RenameOfCounter(np("n"))));
    let mut called = false;
    let mut stamp = || {
        called = true;
        MStamp {
            hlc: Hlc::new(1, 0),
            writer: WriterId { node: NodeId::from_bytes([1; 16]), epoch: Epoch::from_u64(0) },
        }
    };
    let r = w.write_update_renaming_stamped(&ops, &[np("n")], &mut stamp);
    assert_eq!(r, Err(UpdateError::Refused(UpdateRefused::RenameOfCounter(np("n")))));
    assert!(!called);
    assert_eq!((w.state.clone(), w.clock), (state, clock));
    // a non-counter source is an ordinary update
    let ok = [UpdateOp::Unset(np("b")), UpdateOp::Set(np("b2"), s("str"))];
    assert!(w.write_update_renaming(&ok, &[np("b")], 0).unwrap().is_some());
}

// ---- `$rename` is refused only for a numeric field that has been incremented (Q7 amendment 8) ----

/// The ops of `$rename from to` over the value `v` that the product read at `from`.
fn rename_ops(from: &str, to: &str, v: Value) -> Vec<UpdateOp> {
    vec![UpdateOp::Unset(np(from)), UpdateOp::Set(np(to), v)]
}

/// Rename `from` to `to` through the writer; `Ok` carries whether anything was written.
fn rename(w: &mut Writer, from: &str, to: &str, v: Value) -> Result<bool, UpdateRefused> {
    w.write_update_renaming(&rename_ops(from, to, v), &[np(from)], 0).map(|r| r.is_some())
}

fn refusal(p: &str) -> Result<bool, UpdateRefused> {
    Err(UpdateRefused::RenameOfCounter(np(p)))
}

const DEC: Value = Value::Decimal128([0x30; 16]);

#[test]
fn rn_a_never_incremented_number_is_renamed_like_a_register() {
    for v in [Value::Int32(4), i(4), DEC] {
        let mut w = writer(1);
        w.insert(&fl(&[("a", v.clone()), ("k", s("keep"))]), 0).unwrap();
        assert_eq!(rename(&mut w, "a", "b", v.clone()), Ok(true), "{v:?}");
        let got = w.visible().unwrap();
        assert_eq!(got.get(&np("b")), Some(&v), "{v:?}");
        assert_eq!(got.get(&np("a")), None, "{v:?}");
        assert_eq!(got.get(&np("k")), Some(&s("keep")), "{v:?}");
        // a plain `$set` of a number is no counter either
        let mut w = writer(1);
        w.insert(&fl(&[("k", s("keep"))]), 0).unwrap();
        w.write_set(&np("a"), v.clone(), 0).unwrap();
        assert_eq!(rename(&mut w, "a", "b", v.clone()), Ok(true), "set {v:?}");
        assert_eq!(w.visible().unwrap().get(&np("b")), Some(&v), "set {v:?}");
    }
}

#[test]
fn rn_a_sub_document_of_plain_numbers_is_renamed_and_the_source_is_gone() {
    let m = doc(&[("x", i(1)), ("y", Value::Int32(2)), ("z", DEC)]);
    let mut w = writer(1);
    w.insert(&fl(&[("m", m.clone())]), 0).unwrap();
    // what `Effects::rename` sends: `Unset(m)` and `Set(n, Document)`
    let ops = rename_ops("m", "n", m);
    assert!(w.write_update_renaming(&ops, &[np("m")], 0).unwrap().is_some());
    let got = w.visible().unwrap();
    assert!(
        got.keys().all(|k| *k != np("m") && !k.is_below(&np("m"))),
        "the source stays: {got:?}"
    );
    assert_eq!(got.get(&np("n.x")), Some(&i(1)));
    assert_eq!(got.get(&np("n.y")), Some(&Value::Int32(2)));
    assert_eq!(got.get(&np("n.z")), Some(&DEC));
}

#[test]
fn rn_a_field_with_one_slot_is_refused() {
    let mut w = writer(1);
    w.insert(&fl(&[("a", i(1))]), 0).unwrap();
    w.write_inc(&np("a"), 1, 0).unwrap();
    assert_eq!(rename(&mut w, "a", "b", i(2)), refusal("a"));
}

#[test]
fn rn_a_numeric_set_over_an_incremented_field_does_not_free_it() {
    let mut w = writer(1);
    w.insert(&fl(&[("a", i(1))]), 0).unwrap();
    w.write_inc(&np("a"), 1, 0).unwrap();
    w.write_set(&np("a"), i(100), 0).unwrap();
    assert!(w.state.slots.values().all(|s| !s.absorbed.is_empty()), "the set absorbed the slot");
    assert_eq!(rename(&mut w, "a", "b", i(100)), refusal("a"));
}

#[test]
fn rn_a_slot_of_an_older_generation_under_a_number_still_refuses() {
    // `$inc a`, then `$set a "s"` (a generation starts), then `$set a 5`: the visible number
    // carries the newer generation, and the only slot is of the older one ("any generation")
    let mut w = writer(1);
    w.insert(&fl(&[("a", i(1))]), 0).unwrap();
    w.write_inc(&np("a"), 1, 0).unwrap();
    w.write_set(&np("a"), s("s"), 0).unwrap();
    assert_eq!(rename(&mut w.clone(), "a", "b", s("s")), Ok(true));
    w.write_set(&np("a"), i(5), 0).unwrap();
    assert_eq!(w.visible().unwrap().get(&np("a")), Some(&i(5)));
    assert_eq!(rename(&mut w, "a", "b", i(5)), refusal("a"));
}

#[test]
fn rn_a_name_freed_and_used_as_a_number_again_in_the_same_life_is_refused_again() {
    // the slot lives as long as the life: unset, then a plain number at the same name
    let mut w = writer(1);
    w.insert(&fl(&[("a", i(1))]), 0).unwrap();
    w.write_inc(&np("a"), 1, 0).unwrap();
    w.write_unset(&np("a"), 0).unwrap();
    w.write_set(&np("a"), i(9), 0).unwrap();
    assert_eq!(rename(&mut w, "a", "b", i(9)), refusal("a"));
}

#[test]
fn rn_a_field_incremented_and_then_set_to_a_non_number_is_renamed() {
    let mut base = writer(1);
    base.insert(&fl(&[("a", i(1))]), 0).unwrap();
    base.write_inc(&np("a"), 1, 0).unwrap();
    assert_eq!(rename(&mut base.clone(), "a", "b", i(2)), refusal("a"));
    for v in [s("s"), Value::Array(vec![i(1)]), doc(&[("x", i(1))])] {
        let mut w = base.clone();
        w.write_set(&np("a"), v.clone(), 0).unwrap();
        assert!(!w.state.slots.is_empty(), "the slot is still there");
        assert!(!w.is_incremented_number_at_or_under(&np("a")), "{v:?}");
        assert_eq!(rename(&mut w, "a", "b", v.clone()), Ok(true), "{v:?}");
        let got = w.visible().unwrap();
        assert!(got.keys().all(|k| *k != np("a") && !k.is_below(&np("a"))), "{v:?}: {got:?}");
        assert!(got.contains_key(&np("b")), "{v:?}");
    }
}

#[test]
fn rn_a_parent_whose_incremented_child_is_gone_is_renamed() {
    for how in ["unset", "replace"] {
        let mut w = writer(1);
        w.insert(&fl(&[("m", doc(&[("x", i(1)), ("c", i(1))]))]), 0).unwrap();
        w.write_inc(&np("m.c"), 1, 0).unwrap();
        let rest = doc(&[("x", i(1))]);
        assert_eq!(rename(&mut w.clone(), "m", "n", rest.clone()), refusal("m"), "{how}");
        match how {
            "unset" => w.write_unset(&np("m.c"), 0).unwrap(),
            _ => w.write_replace(&fl(&[("m", rest.clone())]), 0).unwrap(),
        };
        assert!(!w.is_incremented_number_at_or_under(&np("m")), "{how}");
        assert_eq!(rename(&mut w, "m", "n", rest), Ok(true), "{how}");
        let got = w.visible().unwrap();
        assert!(got.keys().all(|k| *k != np("m") && !k.is_below(&np("m"))), "{how}: {got:?}");
        assert_eq!(got.get(&np("n.x")), Some(&i(1)), "{how}");
    }
}

#[test]
fn rn_a_slot_in_the_older_of_two_alive_lives_is_found() {
    // two alive lives from an explicit insert and a creating upsert (concurrent first inserts
    // supersede each other, so they cannot be used); the slot of `a` is in the OLDER life, in
    // both ways round
    for upsert_is_older in [false, true] {
        let (skew_insert, skew_upsert) = if upsert_is_older { (1000, 0) } else { (0, 1000) };
        let mut ins = writer(1);
        let mut ups = writer(2);
        ins.insert(&fl(&[("a", i(1))]), skew_insert).unwrap();
        if upsert_is_older {
            ups.upsert_inc(&np("a"), 1, skew_upsert).unwrap();
        } else {
            ins.write_inc(&np("a"), 1, 0).unwrap();
            ups.upsert_inc(&np("y"), 1, skew_upsert).unwrap();
        }
        let mut c = writer(3);
        c.receive(&ins.state);
        c.receive(&ups.state);
        let alive = c.alive();
        assert_eq!(alive.len(), 2, "two alive lives (upsert older: {upsert_is_older})");
        let slot_life = c.state.slots.keys().find(|k| k.1 == np("a")).unwrap().0;
        assert!(slot_life < alive[1], "the slot is in the older life: {slot_life:?} {alive:?}");
        assert!(c.is_incremented_number_at_or_under(&np("a")), "upsert older: {upsert_is_older}");
        assert_eq!(rename(&mut c, "a", "z", i(0)), refusal("a"), "upsert older: {upsert_is_older}");
    }
}

#[test]
fn rn_the_second_of_two_renames_is_checked_too() {
    let mut w = writer(1);
    w.insert(&fl(&[("p", i(1)), ("c", i(1))]), 0).unwrap();
    w.write_inc(&np("c"), 1, 0).unwrap();
    let (state, clock) = (w.state.clone(), w.clock);
    let mut ops = rename_ops("p", "p2", i(1));
    ops.extend(rename_ops("c", "c2", i(2)));
    let r = w.write_update_renaming(&ops, &[np("p"), np("c")], 0);
    assert_eq!(r, Err(UpdateRefused::RenameOfCounter(np("c"))));
    assert_eq!((w.state.clone(), w.clock), (state, clock));
}

#[test]
fn rn_a_sub_document_holding_one_incremented_field_is_refused() {
    let mut w = writer(1);
    w.insert(&fl(&[("m", doc(&[("x", i(1)), ("y", i(2))]))]), 0).unwrap();
    w.write_inc(&np("m.y"), 1, 0).unwrap();
    assert_eq!(rename(&mut w, "m", "n", i(0)), refusal("m"));
    // the plain sibling alone is renamable
    assert_eq!(rename(&mut w, "m.x", "m2", i(1)), Ok(true));
}

#[test]
fn rn_a_field_created_by_inc_on_an_absent_path_is_refused() {
    let mut w = writer(1);
    w.insert(&fl(&[("k", i(1))]), 0).unwrap();
    w.write_inc(&np("c"), 3, 0).unwrap();
    assert_eq!(rename(&mut w, "c", "d", i(3)), refusal("c"));
}

#[test]
fn rn_a_creating_upserts_filter_and_set_fields_are_renamed_but_its_inc_field_is_not() {
    let mut w = writer(1);
    w.upsert_soi(&fl(&[("f", i(3))]), &np("c"), 1, &fl(&[("t", Value::Int32(4))]), 0).unwrap();
    assert_eq!(rename(&mut w, "f", "f2", i(3)), Ok(true));
    assert_eq!(rename(&mut w, "t", "t2", Value::Int32(4)), Ok(true));
    assert_eq!(rename(&mut w, "c", "c2", i(1)), refusal("c"));
}

#[test]
fn rn_the_race_rename_of_a_plain_field_against_a_first_inc_reads_rename_then_inc_in_every_order() {
    // both stamp orders: the rename is stamped before or after the increment
    for (skew_a, skew_b) in [(1000u64, 0u64), (0, 1000)] {
        let mut a = writer(1);
        a.insert(&fl(&[("a", i(7))]), 0).unwrap();
        let mut b = writer(2);
        b.receive(&a.state);
        let mut c = writer(3);
        c.receive(&a.state);
        // A renames `a` to `b`; B, which has not seen it, makes the first increment of `a`
        let ops = rename_ops("a", "b", i(7));
        assert!(a.write_update_renaming(&ops, &[np("a")], skew_a).unwrap().is_some());
        b.write_inc(&np("a"), 5, skew_b).unwrap();
        let want: BTreeMap<Path, Value> = [(np("a"), i(5)), (np("b"), i(7))].into_iter().collect();
        let o = opts();
        let tag = format!("skews ({skew_a}, {skew_b})");
        let (ab, l1) = a.state.join(&b.state, &o);
        let (ba, l2) = b.state.join(&a.state, &o);
        assert_eq!(ab, ba, "{tag}");
        assert!(l1.is_empty() && l2.is_empty(), "{tag}: no loss record: {l1:?} {l2:?}");
        assert_eq!(ab.visible(&o).unwrap(), want, "{tag}");
        let order = ab.ordered_document(&o);
        assert_eq!(order, ba.ordered_document(&o), "{tag}");
        let names: Vec<String> = order.iter().flatten().map(|(n, _)| n.clone()).collect();
        // the serial order "rename, then increment": `b` is written first, `a` is created after
        // it (the reading of `ord.rs`; the hold shows both skews agree)
        assert_eq!(names, ["b", "a"], "{tag}");
        // a third member that has seen neither, receiving the two states in each order
        for (first, second) in [(&a.state, &b.state), (&b.state, &a.state)] {
            let mut m = c.clone();
            let mut l = m.receive(first);
            l.extend(m.receive(second));
            assert!(l.is_empty(), "{tag}: no loss record: {l:?}");
            assert_eq!(m.state, ab, "{tag}");
            assert_eq!(m.visible().unwrap(), want, "{tag}");
            assert_eq!(m.state.ordered_document(&o), order, "{tag}");
        }
    }
}
