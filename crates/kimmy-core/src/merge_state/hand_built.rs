//! Hand-built merge states: the public fields of [`MergeState`] filled directly, not through a
//! [`Writer`], in the shapes the fields allow and no writer makes.
//!
//! The generator has three modes. [`Mode::Any`]: a stamp need not say what it wrote (the same stamp
//! can hold different values in two lives, or under one key in the two states of a join).
//! [`Mode::Coherent`]: everything a stamp carries (the value, the absorbed pair, the generation, a
//! life's kind and what it had seen) is a function of the stamp and the path, so that two states
//! never disagree about what one write was (a property of the whole set of states, which no
//! decode can check). [`Mode::Closed`]: coherent, and the state satisfies the structural
//! invariants `decode` checks (`MergeState::check_decoded`), no more and no fewer: every dead life
//! is listed in `lives`, the version vector covers every stamp an honest state's vector covers (life, delete, register, slot, absorbed, generation, `seen`),
//! and every order entry is normal, belongs to a listed life and has its stamps covered. The join
//! laws are claimed for the closed states, and only those are what `decode` accepts.
//!
//! The order entries of a hand-built state are MOMENTS OF ONE HISTORY: for a key `(life, path)` the
//! key fixes a script of events (writes, unsets, replaces, level marks by three writers), and a
//! state holds the entry after the first one to three of them, applied by `ord_apply` as a
//! writer's replica does (`ord_chain`). Two states then hold two moments of one history, the later
//! extending the earlier, which is what the join is claimed associative on; arbitrary entries are
//! not (a writer cannot have two histories: item 22 of the inventory of `check_decoded`).
//!
//! The cases are tagged with the shape they were built for ([`BUILDERS`]), and [`shapes_of`]
//! tells from the states themselves which shapes a case holds, so that a test can count what it
//! really covered.

use std::collections::{BTreeMap, BTreeSet};

use super::prop_tests::Rng;
use super::state::sub_ret;
use super::*;
use crate::hlc::Hlc;
use crate::ids::NodeId;

/// How much a stamp pins down.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Mode {
    /// Anything the public fields allow.
    Any,
    /// A stamp, with the path, fixes the value and the rest of the cell it labels.
    Coherent,
    /// [`Mode::Coherent`], and the state satisfies exactly what `decode` requires of a state (see
    /// [`close`]).
    Closed,
}

/// The builders: one per shape, then the wild one (random cells in a small shared universe).
pub(super) const BUILDERS: [&str; 11] = [
    "same stamp, two lives, different values",
    "equal stamps under one key",
    "cells of an absent or dead life",
    "odd paths beside a document insertLost",
    "unset against null",
    "suppressed soi register and a slot",
    "marker and scalar below, scalar and newer cell below",
    "arrays holding documents",
    "dead lives and deletes straddling h0 in the second state",
    "slot totals near MAX_TOTAL",
    "wild",
];

/// The shapes [`shapes_of`] recognises, by the states (not by the builder).
pub(super) const SHAPES: [&str; 13] = [
    "same stamp at one path in two lives, different values, both sides",
    "equal stamps under one key of both states, different values",
    "registers or slots of a life absent from lives or dead",
    "odd path beside a whole-document insertLost",
    "unset against null at one path, concurrent",
    "suppressed soi register and a slot at one path",
    "map marker with a scalar below it",
    "scalar with a newer cell below it",
    "array holding a document",
    "dead life and delete of t each on both sides of h0, s with none",
    "slot or absorbed total within 8 of MAX_TOTAL",
    "an odd path (empty, `_` or dotted segment)",
    "an order entry at one key of both states, different",
];

const PATHS: &[&[&str]] = &[
    &["x"],
    &["n"],
    &["a"],
    &["a", "b"],
    &["a", "b", "c"],
    &[""],
    &["", "a"],
    &["a", ""],
    &["_"],
    &["a.b"],
    &["a", "b.c"],
];

fn path(segs: &[&str]) -> Path {
    Path::new(segs.iter().copied()).unwrap()
}

/// Is this one of the odd paths: an empty segment, `_`, or a dot inside a segment?
fn is_odd(p: &Path) -> bool {
    p.segments().iter().any(|s| s.is_empty() || s == "_" || s.contains('.'))
}

fn wid(k: u8) -> WriterId {
    WriterId { node: NodeId::from_bytes([b'A' + k; 16]), epoch: Epoch::from_u64(0) }
}

fn stamp(wall: u64, ctr: u16, k: u8) -> MStamp {
    MStamp { hlc: Hlc::new(wall, ctr), writer: wid(k) }
}

fn dec(s: &str) -> Num {
    Num::Decimal128(s.parse::<bson::Decimal128>().unwrap().bytes())
}

fn value_from(r: &mut Rng) -> Value {
    match r.below(18) {
        0 => Value::Int64(0),
        1 => Value::Int64(7),
        2 => Value::Int64(-7),
        3 => Value::Int64(i64::MAX),
        4 => Value::Int32(5),
        5 => Value::Double(2.5),
        6 => Value::String("s".into()),
        7 => Value::String(String::new()),
        8 => Value::Null,
        9 => Value::Unset,
        10 => Value::Bool(true),
        11 => Value::Array(vec![Value::Int64(1)]),
        12 => Value::Array(vec![Value::Document(vec![("k".into(), Value::Int64(1))])]),
        13 => Value::Array(vec![Value::Document(vec![(String::new(), Value::Null)])]),
        14 => Value::Map,
        15 => Value::Decimal128("0.1".parse::<bson::Decimal128>().unwrap().bytes()),
        16 => Value::Array(Vec::new()),
        _ => Value::Double(-0.0),
    }
}

fn num_from(r: &mut Rng) -> Num {
    match r.below(10) {
        0 => Num::Int64(0),
        1 => Num::Int64(1),
        2 => Num::Int64(-5),
        3 => Num::Int64(1000),
        4 => Num::Int64(MAX_TOTAL),
        5 => Num::Int64(-MAX_TOTAL),
        6 => Num::Int64(MAX_TOTAL - 1),
        7 => Num::Int32(7),
        8 => dec("1E+34"),
        _ => Num::Int64(1 << 62),
    }
}

/// The generator's state: the rng and the mode.
struct Cx<'a> {
    rng: &'a mut Rng,
    coh: bool,
    /// [`Mode::Closed`]: the invariants of the writers' states are kept.
    closed: bool,
}

impl Cx<'_> {
    /// Draw with `f`: from the rng in [`Mode::Any`]; in [`Mode::Coherent`] from a generator seeded
    /// by `key`, so that the same key always gives the same draw.
    fn draw<T>(&mut self, key: impl std::fmt::Debug, f: impl FnOnce(&mut Rng) -> T) -> T {
        if self.coh {
            let mut h = 0xcbf2_9ce4_8422_2325u64;
            for b in format!("{key:?}").bytes() {
                h = (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
            }
            let mut r = Rng(h | 1);
            r.next();
            f(&mut r)
        } else {
            f(self.rng)
        }
    }

    fn value(&mut self, at: MStamp, p: &Path) -> Value {
        self.draw(("value", at, p), value_from)
    }

    fn total(&mut self, at: MStamp, p: &Path) -> Num {
        self.draw(("total", at, p), num_from)
    }

    /// A register of the stamp `at` at `p`, with a value (drawn when `value` is `None`), a
    /// generation (a base starts one; a number may have one).
    fn reg(&mut self, at: MStamp, p: &Path, value: Option<Value>) -> Reg {
        let value = value.unwrap_or_else(|| self.value(at, p));
        let (generation, soi) = self.draw(("reg", at, p), |r| {
            let lives = [stamp(1, 0, 0), stamp(2, 0, 1)];
            let mut seen = BTreeMap::new();
            for _ in 0..r.below(3) {
                let key = (*r.pick(&lives), wid(r.below(3) as u8), None);
                seen.insert(key, num_from(r));
            }
            let generation = (!value.is_numeric() || r.below(3) == 0).then(|| Gen {
                stamp: at,
                vv: Vv::new(),
                seen,
            });
            (generation, r.below(4) == 0)
        });
        Reg { stamp: at, value, generation, soi }
    }

    fn life(&mut self, l: MStamp, kind: LifeKind) -> Life {
        let order = self.draw(("order", l), |r| {
            (0..r.below(3)).map(|_| path(r.pick(PATHS))).collect::<Vec<_>>()
        });
        Life { seen: BTreeSet::new(), order, kind }
    }

    /// A slot of a key of `writer`, written by the write `at`. Its stamp is the write's with the
    /// counter raised by [`SLOT_CTR`], so that no register and slot of a path share a stamp: a
    /// write is a register write or an increment, never both at one path.
    ///
    /// In the coherent modes the slot is the writer's own, as `decode` requires (items 26 to 28 of
    /// the inventory): the write is taken as `at`'s HLC by `writer`, so the slot's stamp, the
    /// pair's stamp and every total drawn below are functions of that stamp and the path, and
    /// two writes that differ only in the writer `at` named cannot give one stamp two totals.
    ///
    /// Its absorbed pair, when it has one, is the slot's own past or present, as an honest slot's is:
    /// the pair at the slot's own stamp (the slot as it is), or at the write `at` below it (an
    /// earlier total, a function of that absorbed stamp and the path). In the coherent modes its
    /// type is not wider than the slot's: the slot takes the pair's type when the draw gave it a
    /// wider one (both are functions of the write, so a stamp still fixes what it carries). The
    /// draw is a function of the write and the path in the coherent modes, so that the same
    /// stamp always carries the same pair.
    fn slot(&mut self, at: MStamp, p: &Path, writer: WriterId) -> Slot {
        let at = if self.coh { MStamp { hlc: at.hlc, writer } } else { at };
        let (stamp, mut total) = (slot_stamp(at), self.total(at, p));
        let absorbed = self.draw(("absorbed", at, p), |r| match r.below(4) {
            1 => Some((stamp, total)),
            2 => Some((at, num_from(r))),
            _ => None,
        });
        if self.coh
            && let Some((_, a)) = &absorbed
            && a.label() > total.label()
        {
            total = total.to_kind(a.label());
            // (the pair at the slot's own stamp is the slot, so it has the slot's type: it is not
            // the case that gets here)
        }
        Slot { stamp, total, absorbed }
    }
}

const SLOT_CTR: u16 = 100;

fn slot_stamp(at: MStamp) -> MStamp {
    MStamp { hlc: Hlc::new(at.hlc.wall_ms, at.hlc.counter + SLOT_CTR), writer: at.writer }
}

fn put_reg(s: &mut MergeState, l: MStamp, p: &Path, r: Reg) {
    s.regs.insert((l, p.clone()), r);
}

/// The slot as a slot of the key of `writer` can be: an absorbed pair is a past state of that very
/// slot, so its stamp is by the key's writer. The pair keeps its HLC and moves to that writer; one
/// that then stands above the slot is dropped, and one at the slot's own stamp takes the slot's total
/// (a function of the slot and the key, so a stamp still fixes what it carries).
///
/// A pair made by [`Cx::slot`] in a coherent mode is already by the key's writer, and its total a
/// function of `("absorbed", moved stamp, path)`, so this changes nothing there; it moves the pairs
/// that [`Mode::Any`] and the builders with a pair of their own (`near_max_total`) give, whose totals
/// are functions of the HLC alone, so that the moved stamp still fixes its total.
fn fit_pair(mut sl: Slot, writer: WriterId) -> Slot {
    sl.absorbed = sl.absorbed.and_then(|(a, total)| {
        let a = MStamp { hlc: a.hlc, writer };
        match a.cmp(&sl.stamp) {
            std::cmp::Ordering::Greater => None,
            std::cmp::Ordering::Equal => Some((a, sl.total)),
            std::cmp::Ordering::Less => Some((a, total)),
        }
    });
    sl
}

fn put_slot(s: &mut MergeState, l: MStamp, p: &Path, k: u8, generation: Option<MStamp>, sl: Slot) {
    let sl = fit_pair(sl, wid(k));
    s.slots.insert((l, p.clone(), wid(k), generation), sl);
}

fn vv(s: &mut MergeState, k: u8, wall: u64, ctr: u16) {
    s.vv.insert(wid(k), Hlc::new(wall, ctr));
}

/// The lives every targeted shape uses: stamps 1, 2, 3 and 4 of writers A, B, C and A.
fn la() -> MStamp {
    stamp(1, 0, 0)
}
fn lb() -> MStamp {
    stamp(2, 0, 1)
}
fn lc() -> MStamp {
    stamp(3, 0, 2)
}

fn upsert_lives(cx: &mut Cx, s: &mut MergeState, ls: &[MStamp]) {
    for l in ls {
        s.lives.insert(*l, cx.life(*l, LifeKind::Upsert));
    }
}

fn pick_path(rng: &mut Rng) -> Path {
    path(rng.pick(PATHS))
}

/// Two lives, one stamp: each side holds the same stamp at one path in two lives, with values
/// that differ, and the sides' stamps are concurrent. The record a clash keeps is the last one
/// inserted, so the order of the pairs shows in it.
fn same_stamp_two_lives(cx: &mut Cx, ret: u64) -> (MergeState, MergeState) {
    let _ = ret;
    let (mut s, mut t) = (MergeState::default(), MergeState::default());
    for x in [&mut s, &mut t] {
        upsert_lives(cx, x, &[la(), lb()]);
    }
    let (x, y) = (stamp(10, 0, 0), stamp(11, 0, 1));
    // p: s holds the lower stamp (twice), t the higher; q: the other way round.
    let p = pick_path(cx.rng);
    let q = pick_path(cx.rng);
    let arrays = cx.rng.below(3) == 0;
    let v = |cx: &mut Cx, at: MStamp, p: &Path, k: usize| -> Value {
        let arrays = if cx.coh { cx.draw(("arrays", at, p), |r| r.below(3) == 0) } else { arrays };
        if arrays {
            let k = if cx.coh { 0 } else { k as i64 };
            Value::Array(vec![Value::Int64(k + at.hlc.wall_ms as i64)])
        } else {
            cx.value(at, p)
        }
    };
    for (l, k) in [(la(), 0), (lb(), 1)] {
        let (a, b) = (v(cx, x, &p, k), v(cx, y, &p, k));
        put_reg(&mut s, l, &p, cx.reg(x, &p, Some(a)));
        put_reg(&mut t, l, &p, cx.reg(y, &p, Some(b)));
        let (a, b) = (v(cx, y, &q, k), v(cx, x, &q, k));
        put_reg(&mut s, l, &q, cx.reg(y, &q, Some(a)));
        put_reg(&mut t, l, &q, cx.reg(x, &q, Some(b)));
    }
    vv(&mut s, 0, 10, 0);
    vv(&mut t, 1, 11, 0);
    (s, t)
}

/// One `(life, path)` key in both states with equal stamps and different values (and one slot
/// key likewise), beside keys where `t` is newer or older.
fn equal_stamps_under_one_key(cx: &mut Cx, _ret: u64) -> (MergeState, MergeState) {
    let (mut s, mut t) = (MergeState::default(), MergeState::default());
    for x in [&mut s, &mut t] {
        upsert_lives(cx, x, &[la()]);
    }
    let (p1, p2, p3, p4) = (path(&["x"]), path(&["a"]), path(&["n"]), path(&["a", "b"]));
    let (x, y) = (stamp(10, 0, 0), stamp(12, 0, 1));
    let (va, vb) = if cx.coh {
        let v = cx.value(x, &p1);
        (v.clone(), v)
    } else {
        (Value::Int64(1), Value::Int64(2))
    };
    put_reg(&mut s, la(), &p1, cx.reg(x, &p1, Some(va)));
    put_reg(&mut t, la(), &p1, cx.reg(x, &p1, Some(vb)));
    put_reg(&mut s, la(), &p2, cx.reg(x, &p2, None));
    put_reg(&mut t, la(), &p2, cx.reg(y, &p2, None));
    put_reg(&mut s, la(), &p4, cx.reg(y, &p4, None));
    put_reg(&mut t, la(), &p4, cx.reg(x, &p4, None));
    let (ta, tb) = if cx.coh {
        let n = cx.total(x, &p3);
        (n, n)
    } else {
        (Num::Int64(5), Num::Int64(6))
    };
    put_slot(&mut s, la(), &p3, 0, None, Slot { stamp: slot_stamp(x), total: ta, absorbed: None });
    put_slot(&mut t, la(), &p3, 0, None, Slot { stamp: slot_stamp(x), total: tb, absorbed: None });
    let sl = cx.slot(x, &p1, wid(1));
    put_slot(&mut s, la(), &p1, 1, None, sl);
    let sl = cx.slot(y, &p1, wid(1));
    put_slot(&mut t, la(), &p1, 1, None, sl);
    vv(&mut s, 0, 10, 0);
    vv(&mut t, 1, 12, 0);
    (s, t)
}

/// Registers, slots and generations of a life that `lives` does not list, and of lives in
/// `dead`, in one state or both.
fn absent_or_dead(cx: &mut Cx, _ret: u64) -> (MergeState, MergeState) {
    let (mut s, mut t) = (MergeState::default(), MergeState::default());
    upsert_lives(cx, &mut s, &[la()]);
    upsert_lives(cx, &mut t, &[lb(), lc()]);
    s.dead.insert(lc());
    t.dead.insert(la());
    if cx.rng.below(2) == 0 {
        t.dead.insert(lb());
    }
    let absent = stamp(5, 0, 2);
    for (k, st) in [&mut s, &mut t].into_iter().enumerate() {
        for life in [la(), lb(), lc(), absent] {
            for p in [path(&["x"]), path(&["a", "b"]), path(&[""])] {
                let at = stamp(10 + k as u64 + life.hlc.wall_ms, 0, k as u8);
                put_reg(st, life, &p, cx.reg(at, &p, None));
                let sl = cx.slot(at, &p, wid(k as u8));
                put_slot(st, life, &p, k as u8, None, sl);
            }
            st.gens.insert((life, path(&["x"])), (stamp(9, 0, 0), Vv::new()));
        }
        vv(st, k as u8, 20, 0);
    }
    (s, t)
}

/// Odd paths beside the insertLost of a whole document: two first inserts (the older is
/// superseded and reported) with registers at odd paths, concurrent, and records already in the
/// states' `losses` at the document and at a field named `_`.
fn odd_paths(cx: &mut Cx, _ret: u64) -> (MergeState, MergeState) {
    let (mut s, mut t) = (MergeState::default(), MergeState::default());
    let (l1, l2) = (stamp(3, 0, 0), stamp(4, 0, 1));
    s.lives.insert(l1, cx.life(l1, LifeKind::Insert));
    t.lives.insert(l2, cx.life(l2, LifeKind::Insert));
    let odd: Vec<Path> = PATHS.iter().copied().map(path).filter(is_odd).collect();
    for (k, p) in odd.iter().enumerate() {
        let (a, b) = (stamp(10 + k as u64, 0, 0), stamp(10 + k as u64, 1, 1));
        put_reg(&mut s, l1, p, cx.reg(a, p, None));
        put_reg(&mut t, l2, p, cx.reg(b, p, None));
        let sl = cx.slot(a, p, wid(0));
        put_slot(&mut s, l1, p, 0, None, sl);
    }
    // an ancestor and its odd descendants: scalar against cell below
    let (top, below) = (path(&[""]), path(&["", "a"]));
    put_reg(&mut s, l1, &top, cx.reg(stamp(30, 0, 0), &top, Some(Value::Int64(1))));
    put_reg(&mut t, l2, &below, cx.reg(stamp(31, 0, 1), &below, Some(Value::Int64(2))));
    let (top, below) = (path(&["a"]), path(&["a", ""]));
    put_reg(&mut s, l1, &top, cx.reg(stamp(32, 0, 0), &top, Some(Value::Int64(1))));
    put_reg(&mut t, l2, &below, cx.reg(stamp(29, 0, 1), &below, Some(Value::Int64(2))));
    s.losses.insert(
        LossKey::document(LossKind::InsertLost, stamp(2, 0, 2)),
        LossValue::Fields(vec![path(&["_"]), path(&[""])]),
    );
    t.losses.insert(
        LossKey::field(LossKind::InsertLost, path(&["_"]), stamp(2, 0, 2)),
        LossValue::Value(Value::Int64(3)),
    );
    vv(&mut s, 0, 40, 0);
    vv(&mut t, 1, 41, 0);
    (s, t)
}

/// `Unset` against `Null` at one path, concurrent, with a counter slot beside them half the time.
fn unset_vs_null(cx: &mut Cx, _ret: u64) -> (MergeState, MergeState) {
    let (mut s, mut t) = (MergeState::default(), MergeState::default());
    for x in [&mut s, &mut t] {
        upsert_lives(cx, x, &[la()]);
    }
    let flip = cx.rng.below(2) == 0;
    for (n, p) in [path(&["x"]), path(&["a", "b"]), path(&["_"])].iter().enumerate() {
        let (a, b) = (stamp(10 + n as u64, 0, 0), stamp(11 + n as u64, 0, 1));
        // (a coherent case fixes it by the path, since a stamp at a path has one value)
        let flip = if cx.coh { n % 2 == 0 } else { flip };
        let (vs, vt) = if flip { (Value::Null, Value::Unset) } else { (Value::Unset, Value::Null) };
        put_reg(&mut s, la(), p, cx.reg(a, p, Some(vs)));
        put_reg(&mut t, la(), p, cx.reg(b, p, Some(vt)));
        if n == 1 {
            let sl = cx.slot(a, p, wid(0));
            put_slot(&mut s, la(), p, 0, None, sl);
        }
    }
    vv(&mut s, 0, 10, 0);
    vv(&mut t, 1, 11, 0);
    (s, t)
}

/// An explicit insert alive beside an upsert life whose `$setOnInsert` creation register is
/// suppressed, with a counter slot at the same path.
fn soi_and_slot(cx: &mut Cx, _ret: u64) -> (MergeState, MergeState) {
    let (mut s, mut t) = (MergeState::default(), MergeState::default());
    s.lives.insert(lc(), cx.life(lc(), LifeKind::Insert));
    s.lives.insert(la(), cx.life(la(), LifeKind::Upsert));
    t.lives.insert(la(), cx.life(la(), LifeKind::Upsert));
    if cx.rng.below(2) == 0 {
        t.lives.insert(lc(), cx.life(lc(), LifeKind::Insert));
    }
    for (n, p) in [path(&["x"]), path(&["n"]), path(&["a", "b"])].iter().enumerate() {
        let a = stamp(10 + n as u64, 0, 0);
        let mut r = cx.reg(a, p, None);
        r.soi = true;
        put_reg(&mut s, la(), p, r);
        let sl = cx.slot(a, p, wid(0));
        put_slot(&mut s, la(), p, 0, None, sl);
        let b = stamp(12 + n as u64, 0, 1);
        let mut r = cx.reg(b, p, None);
        r.soi = n != 1;
        put_reg(&mut t, la(), p, r);
        let sl = cx.slot(b, p, wid(1));
        put_slot(&mut t, la(), p, 1, None, sl);
    }
    vv(&mut s, 0, 12, 0);
    vv(&mut t, 1, 14, 0);
    (s, t)
}

/// A map marker with a scalar below it, a scalar with a newer cell below it, and the reverse,
/// across the two states.
fn marker_and_scalar(cx: &mut Cx, _ret: u64) -> (MergeState, MergeState) {
    let (mut s, mut t) = (MergeState::default(), MergeState::default());
    for x in [&mut s, &mut t] {
        upsert_lives(cx, x, &[la()]);
    }
    let a = path(&["a"]);
    let ab = path(&["a", "b"]);
    let abc = path(&["a", "b", "c"]);
    let e = path(&[""]);
    let ea = path(&["", "a"]);
    let (v1, v2, v3, v4, v5, v6, v7) = (
        stamp(10, 0, 0),
        stamp(11, 0, 1),
        stamp(12, 0, 0),
        stamp(13, 0, 1),
        stamp(14, 0, 0),
        stamp(15, 0, 1),
        stamp(16, 0, 0),
    );
    let flip = cx.rng.below(2) == 0;
    let (x, y) = if flip { (&mut t, &mut s) } else { (&mut s, &mut t) };
    // x: a map marker at a; y: a scalar below it
    put_reg(x, la(), &a, cx.reg(v1, &a, Some(Value::Map)));
    put_reg(y, la(), &ab, cx.reg(v2, &ab, Some(Value::Int64(1))));
    // x: a scalar at ab, y: a newer slot and a newer register below it
    put_reg(x, la(), &ab, cx.reg(v3, &ab, Some(Value::String("s".into()))));
    let sl = cx.slot(v4, &abc, wid(1));
    put_slot(y, la(), &abc, 1, None, sl);
    // x: a newer scalar at "", y: an older cell below it
    put_reg(y, la(), &ea, cx.reg(v5, &ea, Some(Value::Int64(3))));
    put_reg(x, la(), &e, cx.reg(v6, &e, Some(Value::Null)));
    put_reg(y, la(), &abc, cx.reg(v7, &abc, Some(Value::Int64(4))));
    vv(&mut s, 0, 16, 0);
    vv(&mut t, 1, 15, 0);
    (s, t)
}

/// Arrays holding documents, at one path on both sides, against a scalar, and with a cell below.
fn arrays_with_documents(cx: &mut Cx, _ret: u64) -> (MergeState, MergeState) {
    let (mut s, mut t) = (MergeState::default(), MergeState::default());
    for x in [&mut s, &mut t] {
        upsert_lives(cx, x, &[la(), lb()]);
    }
    let doc =
        |k: &str, n: i64| Value::Array(vec![Value::Document(vec![(k.into(), Value::Int64(n))])]);
    for (n, p) in
        [path(&["items"]), path(&["a"]), path(&["a", "b"]), path(&["_"])].iter().enumerate()
    {
        let (a, b) = (stamp(10 + n as u64, 0, 0), stamp(11 + n as u64, 0, 1));
        put_reg(&mut s, la(), p, cx.reg(a, p, Some(doc("k", n as i64))));
        let vt = if n == 1 { Value::Int64(1) } else { doc("", n as i64 + 1) };
        put_reg(&mut t, lb(), p, cx.reg(b, p, Some(vt)));
        if n == 0 {
            put_reg(&mut t, la(), p, cx.reg(b, p, Some(doc("k", 9))));
        }
    }
    vv(&mut s, 0, 13, 0);
    vv(&mut t, 1, 14, 0);
    (s, t)
}

/// `t` holds dead lives and deletes on both sides of the horizon, `s` none and a late clock.
fn horizon_straddle(cx: &mut Cx, ret: u64) -> (MergeState, MergeState) {
    let (mut s, mut t) = (MergeState::default(), MergeState::default());
    upsert_lives(cx, &mut s, &[la()]);
    s.lives.insert(lc(), cx.life(lc(), LifeKind::Insert));
    vv(&mut s, 0, 60, 0);
    let p = path(&["x"]);
    put_reg(&mut s, la(), &p, cx.reg(stamp(60, 0, 0), &p, None));
    let h0 = 60u64.saturating_sub(ret.min(1000));
    for (n, w) in [h0.saturating_sub(3), h0.saturating_sub(1), h0, h0 + 1, h0 + 5]
        .into_iter()
        .map(|w| w.max(1))
        .enumerate()
    {
        let l = stamp(w, 0, 1);
        t.lives.insert(l, cx.life(l, LifeKind::Upsert));
        if cx.rng.below(4) != 0 {
            t.dead.insert(l);
        }
        let d = stamp(w, 1, 1);
        t.deletes.insert(d, Vv::from([(wid(1), d.hlc)]));
        let q = path(PATHS[n]);
        put_reg(&mut t, l, &q, cx.reg(stamp(w, 2, 1), &q, None));
        let sl = cx.slot(stamp(w, 2, 1), &q, wid(1));
        put_slot(&mut t, l, &q, 1, None, sl);
        t.losses.insert(
            LossKey::field(LossKind::Clash, q.clone(), stamp(w, 3, 1)),
            LossValue::Value(Value::Int64(n as i64)),
        );
    }
    vv(&mut t, 1, 20, 0);
    (s, t)
}

/// Slot totals at and near `MAX_TOTAL`, many writers on one path, with observations and a
/// generation of the same size.
fn near_max_total(cx: &mut Cx, _ret: u64) -> (MergeState, MergeState) {
    let (mut s, mut t) = (MergeState::default(), MergeState::default());
    for x in [&mut s, &mut t] {
        upsert_lives(cx, x, &[la()]);
    }
    let p = path(&["x"]);
    let big = [MAX_TOTAL, -MAX_TOTAL, MAX_TOTAL - 1, 1 << 62, -(1 << 62), 1, 0];
    let writers: u8 = if cx.coh || cx.rng.below(2) == 0 { 40 } else { 3 };
    let g = stamp(9, 0, 0);
    for (st, base_at) in [(&mut s, 10u64), (&mut t, 12u64)] {
        let at = stamp(base_at, 0, 0);
        let n = cx.draw(("n", at), |r| *r.pick(&big));
        let mut seen = BTreeMap::new();
        for k in 0..writers {
            let slot_at = stamp(base_at + u64::from(k), 0, k);
            let total = Num::Int64(cx.draw(("total", slot_at), |r| *r.pick(&big)));
            let generation = if k % 5 == 4 { Some(g) } else { None };
            // the pair an earlier base write absorbed: the write `at`, below the slot's stamp
            let absorbed = Some((at, Num::Int64(-n)));
            put_slot(
                st,
                la(),
                &p,
                k,
                generation,
                Slot { stamp: slot_stamp(slot_at), total, absorbed },
            );
            seen.insert((la(), wid(k), generation), Num::Int64(n));
        }
        let base = cx.draw(("base", at), |r| {
            r.pick(&[Value::Int64(5), Value::String("s".into()), Value::Unset]).clone()
        });
        let generation = Some(Gen { stamp: g, vv: Vv::new(), seen });
        put_reg(st, la(), &p, Reg { stamp: at, value: base, generation, soi: false });
        vv(st, 0, base_at + 40, 0);
    }
    (s, t)
}

/// A small universe shared by the states of one wild case.
struct Uni {
    /// The clock of the case runs from 1 to this.
    max_wall: u64,
    lives: Vec<MStamp>,
    deletes: Vec<MStamp>,
    stamps: Vec<MStamp>,
    paths: Vec<Path>,
}

fn rand_stamp(r: &mut Rng, max_wall: u64) -> MStamp {
    stamp(1 + r.below(max_wall as usize) as u64, r.below(2) as u16, r.below(3) as u8)
}

fn uni(r: &mut Rng, max_wall: u64) -> Uni {
    Uni {
        max_wall,
        lives: (0..4).map(|_| rand_stamp(r, max_wall)).collect(),
        deletes: (0..3).map(|_| rand_stamp(r, max_wall)).collect::<Vec<_>>(),
        stamps: (0..8).map(|_| rand_stamp(r, max_wall)).collect(),
        paths: (0..5).map(|_| pick_path(r)).collect(),
    }
}

/// The position a write gives a path, a function of the write: one stamp is one write, and a write
/// names a path once.
pub(super) fn pos_of(c: MStamp) -> u64 {
    (c.hlc.wall_ms + u64::from(c.hlc.counter)) % 4
}

/// The entry of `(life, at)` after `steps` events of the history the key fixes: one replica that
/// received writes by three writers to this path (a write, an unset, a replace, a write or a
/// replace of a whole sub-document, an unset in an update that also writes a sibling), at the
/// stamps 2, 3, 4 and so on, each the work of `ord_apply` on the one entry, as the model's writers
/// do. So the entries two states hold of one key are two moments of ONE history (the later extends
/// the earlier: their join is the later), and they satisfy every clause `decode` checks. The root
/// of a replace only ever gets a level mark.
fn ord_chain(cx: &mut Cx, life: MStamp, at: &OrdAt, steps: usize) -> FieldOrd {
    let o = JoinOptions::default();
    let mut scratch = MergeState::default();
    let sibling = OrdAt::Field(path(&["sib"]));
    for i in 1..=steps {
        let (kind, who, extra) =
            cx.draw(("event", life, at, i), |r| (r.below(5), r.below(3) as u8, r.below(2) == 0));
        let stamp = MStamp { hlc: Hlc::new(i as u64 + 1, 0), writer: wid(who) };
        let known = Hlc::new(i as u64, 0);
        let here = || at.clone();
        let items = if *at == OrdAt::Root {
            vec![(OrdAt::Root, OrdMode::Level)]
        } else {
            match kind {
                0 => vec![(here(), OrdMode::Write)],
                1 if extra => vec![(here(), OrdMode::Unset), (sibling.clone(), OrdMode::Write)],
                1 => vec![(here(), OrdMode::Unset)],
                2 => vec![(here(), OrdMode::Reset)],
                3 => vec![(here(), OrdMode::Write), (here(), OrdMode::Level)],
                _ => vec![(here(), OrdMode::Reset), (here(), OrdMode::Level)],
            }
        };
        scratch.ord_apply(life, stamp, known, &ord_events(&items, &o), &o);
    }
    scratch.ords.remove(&(life, at.clone())).unwrap_or_default()
}

fn wild_state(cx: &mut Cx, u: &Uni) -> MergeState {
    let mut s = MergeState::default();
    for l in &u.lives {
        let dead = cx.rng.below(4) == 0;
        // a closed state lists every life it marks dead
        if cx.rng.below(10) < 6 || (cx.closed && dead) {
            let kind = cx.draw(("kind", l), |r| {
                if r.below(2) == 0 { LifeKind::Insert } else { LifeKind::Upsert }
            });
            let mut life = cx.life(*l, kind);
            life.seen = cx.draw(("seen", l), |r| {
                u.deletes.iter().filter(|_| r.below(10) < 3).copied().collect()
            });
            s.lives.insert(*l, life);
        }
        if dead {
            s.dead.insert(*l);
        }
    }
    for d in &u.deletes {
        if cx.rng.below(10) < 4 {
            let v = cx.draw(("dvv", d), |r| {
                let mut v = Vv::new();
                for k in 0..3u8 {
                    if r.below(2) == 0 {
                        v.insert(wid(k), Hlc::new(1 + r.below(u.max_wall as usize) as u64, 0));
                    }
                }
                v
            });
            s.deletes.insert(*d, v);
        }
    }
    for _ in 0..cx.rng.below(9) {
        let (l, at, p) = (*cx.rng.pick(&u.lives), *cx.rng.pick(&u.stamps), cx.rng.pick(&u.paths));
        let r = cx.reg(at, p, None);
        put_reg(&mut s, l, p, r);
    }
    for _ in 0..cx.rng.below(6) {
        let (l, at, p) = (*cx.rng.pick(&u.lives), *cx.rng.pick(&u.stamps), cx.rng.pick(&u.paths));
        let generation = (cx.rng.below(3) == 0).then(|| *cx.rng.pick(&u.stamps));
        let k = cx.rng.below(3) as u8;
        let sl = cx.slot(at, p, wid(k));
        put_slot(&mut s, l, p, k, generation, sl);
    }
    for _ in 0..cx.rng.below(3) {
        let (l, p) = (*cx.rng.pick(&u.lives), cx.rng.pick(&u.paths).clone());
        s.gens.insert((l, p), (*cx.rng.pick(&u.stamps), Vv::new()));
    }
    // order entries: at a few keys of the universe, so that the two states of a case meet at them
    for _ in 0..cx.rng.below(6) {
        let l = u.lives[cx.rng.below(2)];
        let at = if cx.rng.below(4) == 0 {
            OrdAt::Root
        } else {
            OrdAt::Field(u.paths[cx.rng.below(2)].clone())
        };
        let steps = 1 + cx.rng.below(3);
        let x = ord_chain(cx, l, &at, steps);
        s.ords.insert((l, at), x);
    }
    for k in 0..3u8 {
        if cx.rng.below(4) != 0 {
            vv(&mut s, k, 1 + cx.rng.below(u.max_wall as usize) as u64, cx.rng.below(2) as u16);
        }
    }
    for _ in 0..cx.rng.below(4) {
        let kind = *cx.rng.pick(&[
            LossKind::Clash,
            LossKind::DeleteWon,
            LossKind::InsertLost,
            LossKind::TypeClash,
            LossKind::ArrayClash,
        ]);
        let at = *cx.rng.pick(&u.stamps);
        let p = cx.rng.pick(&u.paths).clone();
        let whole = cx.rng.below(3) == 0;
        let key = if cx.coh {
            // a record the join never makes (it makes the kinds above), so that no record of
            // the state is about a cell of its own, with a value that is not that cell's
            let about = if whole { LossAt::Document } else { LossAt::Field(p.clone()) };
            LossKey { kind: LossKind::RetiredWriter, at: about, stamp: at }
        } else if whole {
            LossKey::document(LossKind::InsertLost, at)
        } else {
            LossKey::field(kind, p.clone(), at)
        };
        let v = cx.draw(("loss", &key), |r| match r.below(3) {
            0 => LossValue::Value(value_from(r)),
            1 => LossValue::Total(num_from(r)),
            _ => LossValue::Fields(vec![path(r.pick(PATHS))]),
        });
        s.losses.insert(key, v);
    }
    s
}

/// A state like `s` with some cells re-stamped (and, in [`Mode::Any`], some values changed under
/// the same stamp).
fn derived(cx: &mut Cx, u: &Uni, s: &MergeState) -> MergeState {
    let mut t = s.clone();
    let keys: Vec<_> = t.regs.keys().cloned().collect();
    for k in keys {
        match cx.rng.below(10) {
            0..=2 => {
                let at = *cx.rng.pick(&u.stamps);
                let r = cx.reg(at, &k.1, None);
                t.regs.insert(k, r);
            }
            3..=4 if !cx.coh => {
                let v = value_from(cx.rng);
                if let Some(r) = t.regs.get_mut(&k) {
                    r.value = v;
                }
            }
            5 => {
                t.regs.remove(&k);
            }
            _ => {}
        }
    }
    let keys: Vec<_> = t.slots.keys().cloned().collect();
    for k in keys {
        match cx.rng.below(10) {
            0..=2 => {
                let at = *cx.rng.pick(&u.stamps);
                let sl = cx.slot(at, &k.1, k.2);
                let sl = fit_pair(sl, k.2);
                t.slots.insert(k, sl);
            }
            3..=4 if !cx.coh => {
                let n = num_from(cx.rng);
                if let Some(sl) = t.slots.get_mut(&k) {
                    sl.total = n;
                }
            }
            _ => {}
        }
    }
    for l in &u.lives {
        if cx.rng.below(10) < 2 && !t.dead.remove(l) && (!cx.closed || t.lives.contains_key(l)) {
            t.dead.insert(*l);
        }
    }
    for k in 0..3u8 {
        if cx.rng.below(3) == 0 {
            vv(&mut t, k, 1 + cx.rng.below(u.max_wall as usize) as u64, 0);
        }
    }
    t
}

fn wild(cx: &mut Cx, ret: u64) -> (MergeState, MergeState) {
    // coherent cases keep the clocks of the states within the retention of each other (the
    // join is not claimed to be associative across a partition longer than that)
    let max_wall = if cx.coh { ret.clamp(2, 24) } else { 24 };
    let mut u = uni(cx.rng, max_wall);
    if cx.coh {
        // what a life has seen is a function of the life, so the deletes it can name are fixed
        u.deletes = vec![stamp(5, 0, 0), stamp(8, 1, 1), stamp(10, 0, 2)];
    }
    let s = wild_state(cx, &u);
    let t = if cx.rng.below(2) == 0 { derived(cx, &u, &s) } else { wild_state(cx, &u) };
    (s, t)
}

/// One case: two states and the retention it is meant for.
pub(super) struct Case {
    pub s: MergeState,
    pub t: MergeState,
}

/// A case of the builder `builder` (an index of [`BUILDERS`]), drawn from `rng`.
pub(super) fn case(rng: &mut Rng, mode: Mode, builder: usize, ret: u64) -> Case {
    let mut cx = Cx { rng, coh: mode != Mode::Any, closed: mode == Mode::Closed };
    let (s, t) = match builder {
        0 => same_stamp_two_lives(&mut cx, ret),
        1 => equal_stamps_under_one_key(&mut cx, ret),
        2 => absent_or_dead(&mut cx, ret),
        3 => odd_paths(&mut cx, ret),
        4 => unset_vs_null(&mut cx, ret),
        5 => soi_and_slot(&mut cx, ret),
        6 => marker_and_scalar(&mut cx, ret),
        7 => arrays_with_documents(&mut cx, ret),
        8 => horizon_straddle(&mut cx, ret),
        9 => near_max_total(&mut cx, ret),
        _ => wild(&mut cx, ret),
    };
    let (mut s, mut t) = (s, t);
    if mode == Mode::Closed {
        for x in [&mut s, &mut t] {
            close(&mut cx, x);
        }
    }
    Case { s, t }
}

/// What every state that writers build satisfies, and a coherent hand-built one must too: a dead
/// life is a life the state lists (the join drops a dead life from `lives` only past the horizon,
/// together with its mark), and the version vector covers every stamp the state holds (a writer
/// has seen its own writes).
fn close(cx: &mut Cx, x: &mut MergeState) {
    // (the wild builder never has a dead life it does not list; the others make upsert lives)
    for l in x.dead.clone() {
        if let std::collections::btree_map::Entry::Vacant(e) = x.lives.entry(l) {
            e.insert(cx.life(l, LifeKind::Upsert));
        }
    }
    // an order entry belongs to a listed life: the others go (a life is not invented here, since
    // another state may list it as another kind); an entry that says nothing is no entry
    x.ords.retain(|(l, _), e| x.lives.contains_key(l) && !e.is_empty());
    let mut stamps: Vec<MStamp> = x.lives.keys().copied().collect();
    stamps.extend(x.deletes.keys());
    stamps.extend(x.regs.values().map(|r| r.stamp));
    stamps.extend(x.slots.values().map(|s| s.stamp));
    stamps.extend(x.slots.values().filter_map(|s| s.absorbed.as_ref().map(|a| a.0)));
    stamps.extend(x.slots.keys().filter_map(|k| k.3));
    stamps.extend(x.lives.values().flat_map(|l| l.seen.iter().copied()));
    for g in x.regs.values().filter_map(|r| r.generation.as_ref()) {
        stamps.push(g.stamp);
        stamps.extend(g.seen.keys().flat_map(|k| [k.0].into_iter().chain(k.2)));
    }
    // the vectors of the deletes: (writer, HLC) entries the state's own vector must reach
    let delete_vvs: Vec<(WriterId, Hlc)> =
        x.deletes.values().flat_map(|v| v.iter().map(|(w, h)| (*w, *h))).collect();
    for (w, h) in delete_vvs {
        let e = x.vv.entry(w).or_insert(Hlc::ZERO);
        *e = (*e).max(h);
    }
    for e in x.ords.values() {
        stamps.extend(e.floor.map(|f| f.stamp()));
        stamps.extend(e.firsts.values().map(|c| c.0));
        stamps.extend(e.last.map(|c| c.0));
        stamps.extend(e.lvl);
    }
    for st in stamps {
        let e = x.vv.entry(st.writer).or_insert(Hlc::ZERO);
        *e = (*e).max(st.hlc);
    }
}

/// Which of [`SHAPES`] the pair holds, read off the states (at retention `ret`).
pub(super) fn shapes_of(s: &MergeState, t: &MergeState, ret: u64) -> Vec<&'static str> {
    let mut out = Vec::new();
    let mut add = |on: bool, i: usize| {
        if on {
            out.push(SHAPES[i]);
        }
    };
    // one stamp at one path in two lives with different values
    let twice = |x: &MergeState| {
        x.regs.iter().any(|((l1, p1), r1)| {
            x.regs.iter().any(|((l2, p2), r2)| {
                l1 < l2 && p1 == p2 && r1.stamp == r2.stamp && r1.value != r2.value
            })
        })
    };
    add(twice(s) && twice(t), 0);
    add(
        s.regs
            .iter()
            .any(|(k, r)| t.regs.get(k).is_some_and(|q| q.stamp == r.stamp && q.value != r.value))
            && s.slots.iter().any(|(k, v)| {
                t.slots.get(k).is_some_and(|q| q.stamp == v.stamp && q.total != v.total)
            }),
        1,
    );
    let orphan = |x: &MergeState| {
        x.regs
            .keys()
            .map(|k| &k.0)
            .chain(x.slots.keys().map(|k| &k.0))
            .any(|l| !x.lives.contains_key(l) || x.dead.contains(l))
    };
    add(orphan(s) || orphan(t), 2);
    let odd = |x: &MergeState| x.regs.keys().any(|k| is_odd(&k.1));
    let doc_loss = |x: &MergeState| {
        x.losses.keys().any(|k| k.kind == LossKind::InsertLost && k.at == LossAt::Document)
    };
    let two_first = {
        let firsts = |x: &MergeState| {
            x.lives.iter().filter(|(_, v)| v.kind == LifeKind::Insert && v.seen.is_empty()).count()
        };
        firsts(s) + firsts(t) >= 2
    };
    add((odd(s) || odd(t)) && (doc_loss(s) || doc_loss(t) || two_first), 3);
    let unset_null = |x: &MergeState, y: &MergeState| {
        x.regs.iter().any(|((_, p), r)| {
            r.value == Value::Unset
                && y.regs.iter().any(|((_, q), b)| {
                    p == q
                        && b.value == Value::Null
                        && b.stamp != r.stamp
                        && !super::state::cov(&x.vv, &b.stamp)
                        && !super::state::cov(&y.vv, &r.stamp)
                })
        })
    };
    add(unset_null(s, t) || unset_null(t, s), 4);
    let soi_slot = |x: &MergeState| {
        let al = x.alive_set(ret);
        let insert = al.iter().any(|l| x.lives.get(l).is_some_and(|v| v.kind == LifeKind::Insert));
        insert
            && x.regs.iter().any(|((l, p), r)| {
                r.soi
                    && al.contains(l)
                    && x.lives.get(l).is_some_and(|v| v.kind == LifeKind::Upsert)
                    && x.slots.keys().any(|k| &k.1 == p && al.contains(&k.0))
            })
    };
    add(soi_slot(s) || soi_slot(t), 5);
    // cells by path over both states
    let mut cells: BTreeMap<&Path, Vec<(MStamp, bool, bool)>> = BTreeMap::new();
    for x in [s, t] {
        for ((_, p), r) in &x.regs {
            cells.entry(p).or_default().push((r.stamp, r.value == Value::Map, r.value.is_plain()));
        }
        for ((_, p, _, _), sl) in &x.slots {
            cells.entry(p).or_default().push((sl.stamp, false, true));
        }
    }
    let marker_below = cells.iter().any(|(p, cs)| {
        cs.iter().any(|c| c.1)
            && cells.iter().any(|(q, qs)| q.is_below(p) && qs.iter().any(|c| c.2))
    });
    add(marker_below, 6);
    let newer_below = cells.iter().any(|(p, cs)| {
        cs.iter().any(|c| c.2 && !c.1)
            && cells.iter().any(|(q, qs)| {
                q.is_below(p) && qs.iter().any(|d| cs.iter().any(|c| c.2 && !c.1 && d.0 > c.0))
            })
    });
    add(newer_below, 7);
    let doc_array = |x: &MergeState| {
        x.regs.values().any(|r| matches!(&r.value, Value::Array(a) if a.iter().any(|v| matches!(v, Value::Document(_)))))
    };
    add(doc_array(s) || doc_array(t), 8);
    let h0 = sub_ret(s.max_vv().max(t.max_vv()), ret);
    let both_sides = |it: &mut dyn Iterator<Item = Hlc>| {
        let v: Vec<Hlc> = it.collect();
        v.iter().any(|h| *h < h0) && v.iter().any(|h| *h >= h0)
    };
    add(
        h0 > Hlc::ZERO
            && both_sides(&mut t.dead.iter().map(|l| l.hlc))
            && both_sides(&mut t.deletes.keys().map(|d| d.hlc))
            && s.dead.is_empty()
            && s.deletes.is_empty(),
        9,
    );
    add(
        s.totals().chain(t.totals()).any(|n| n.int().is_some_and(|v| v.abs() >= MAX_TOTAL - 8)),
        10,
    );
    add(odd(s) || odd(t), 11);
    add(s.ords.iter().any(|(k, x)| t.ords.get(k).is_some_and(|y| x != y)), 12);
    out
}

#[test]
fn the_cases_cover_every_shape_and_the_closed_ones_are_what_decode_accepts() {
    let mut rng = Rng(0x5348_4150_4553);
    let mut found = BTreeMap::new();
    let (mut closed, mut refused) = (0, BTreeMap::new());
    for builder in 0..BUILDERS.len() {
        for k in 0..300 {
            let ret = [1_000_000_000, 40, 10, 4][k % 4];
            let c = case(&mut rng, Mode::Any, builder, ret);
            for s in shapes_of(&c.s, &c.t, ret) {
                *found.entry(s).or_insert(0) += 1;
            }
            // closed: accepted by both forms (the version 1 form where it can spell the state);
            // coherent, not closed: refused exactly when `check_decoded` says so
            let cl = case(&mut rng, Mode::Closed, builder, ret);
            let co = case(&mut rng, Mode::Coherent, builder, ret);
            for x in [cl.s, cl.t] {
                closed += 1;
                assert_eq!(x.check_decoded(), Ok(()), "closed, builder {builder}");
                assert_eq!(MergeState::decode_compact(&x.encode_compact()).as_ref(), Ok(&x));
            }
            for x in [co.s, co.t] {
                let verdict = MergeState::decode_compact(&x.encode_compact());
                match x.check_decoded() {
                    Ok(()) => assert_eq!(verdict.as_ref(), Ok(&x)),
                    Err(e) => {
                        assert_eq!(verdict.as_ref(), Err(&e));
                        *refused.entry(format!("{e:?}")).or_insert(0) += 1;
                    }
                }
            }
        }
    }
    for s in SHAPES {
        assert!(found.get(s).copied().unwrap_or(0) >= 50, "the shape `{s}` is missing: {found:?}");
    }
    assert!(closed > 1000);
    assert!(
        ["DeadLifeNotListed", "VersionVectorBehind"].iter().all(|k| refused.contains_key(*k)),
        "coherent states that are not closed must show both refusals: {refused:?}"
    );
}

/// A coherent slot is the key's writer's, whatever writer the write `at` named, and what its stamp
/// carries is a function of the stamp and the path: the slot's total, and its pair (the pair's
/// total a function of the absorbed stamp and the path, its type not wider than the slot's). Without
/// it two states could hold one absorbed stamp under one key with two totals (a latent defect the
/// review found at `fit_pair`: the total was drawn from the write, writer included, and the stamp
/// then moved to the key's writer). Every such slot is what `decode` accepts.
#[test]
fn a_coherent_slot_is_the_key_writers_and_a_function_of_its_stamp_and_path() {
    let mut rng = Rng(0x5354_414d_5053);
    let mut cx = Cx { rng: &mut rng, coh: true, closed: true };
    let (mut pairs, mut wider) = (0, 0);
    for wall in 1..40u64 {
        for ctr in 0..2u16 {
            for pa in PATHS {
                let pa = path(pa);
                for key in 0..3u8 {
                    let slots: Vec<Slot> =
                        (0..3u8).map(|w| cx.slot(stamp(wall, ctr, w), &pa, wid(key))).collect();
                    // the writer the write named does not matter
                    assert!(slots.windows(2).all(|w| w[0] == w[1]), "{wall} {ctr} {pa:?} {key}");
                    let sl = fit_pair(slots[0].clone(), wid(key));
                    assert_eq!(sl, slots[0], "the pair is already the key writer's");
                    assert_eq!(sl.stamp.writer, wid(key));
                    let mut s = MergeState::default();
                    s.lives.insert(
                        la(),
                        Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Upsert },
                    );
                    s.slots.insert((la(), pa.clone(), wid(key), None), sl.clone());
                    for k in 0..3u8 {
                        vv(&mut s, k, 1000, 0);
                    }
                    assert_eq!(s.check_decoded(), Ok(()), "{sl:?}");
                    if let Some((_, a)) = &sl.absorbed {
                        pairs += 1;
                        wider += usize::from(a.label() < sl.total.label());
                    }
                }
            }
        }
    }
    assert!(pairs > 500 && wider > 20, "{pairs} pairs, {wider} narrower than their slot");
}
