//! Tests of the counter cell: a slot is `(stamp, total)` and an optional absorbed `(stamp, total)`;
//! a base write folds every slot of its path the writer holds in an alive life; the read side
//! counts what a slot gained since its pair; the compact form spells the type and the pair;
//! `decode` refuses a pair above its slot, a pair that disagrees at the slot's stamp, and a vector
//! that does not cover the stamps an honest state's does.
//!
//! The model has one integer type, so the typed totals (`Int32`, `Decimal128`) are tested here and
//! nowhere else.

use std::collections::{BTreeMap, BTreeSet};

use super::tests::{fl, h, i, opts, p, s, wid};
use super::*;
use crate::hlc::Hlc;

type SlotKey = (MStamp, Path, WriterId, Option<MStamp>);

fn dec(t: &str) -> Num {
    Num::Decimal128(t.parse::<bson::Decimal128>().unwrap().bytes())
}

fn dval(t: &str) -> Value {
    Value::Decimal128(t.parse::<bson::Decimal128>().unwrap().bytes())
}

fn st(wall: u64, c: char) -> MStamp {
    MStamp { hlc: h(wall), writer: wid(c, 0) }
}

fn upsert_life() -> Life {
    Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Upsert }
}

fn mesh(ws: &mut [Writer]) {
    let all: Vec<MergeState> = ws.iter().map(|w| w.state.clone()).collect();
    for w in ws.iter_mut() {
        for s in &all {
            w.receive(s);
        }
    }
}

// ---- the fold (review item M2) -----------------------------------------------------------

/// The review's four-step case. x = 100 (A). B adds 5. C, which has seen only the insert, sets x to
/// a string (a new generation), then receives B's 5 (an older generation: hidden, not counted),
/// then sets x to 7: the 5 is in C's causal past though no base of C's generation counts it. D, which
/// saw only the insert, sets x to 50 with the newest stamp. Serial orders read 7 or 50; B's 5 is
/// before C's 7, so 55 would need the 5 after D's 50, which is impossible. Returns every member's
/// value of x after a full mesh.
fn four_step(o: &JoinOptions) -> Vec<Option<Value>> {
    let x = p("x");
    let mk = |c: char| Writer::new(wid(c, 0), o.clone());
    let (a, b, c, d) = (mk('A'), mk('B'), mk('C'), mk('D'));
    let mut ws = [a, b, c, d];
    ws[0].insert(&fl(&[("x", i(100))]), 0).unwrap();
    let ins = ws[0].state.clone();
    ws[1].receive(&ins);
    ws[1].write_inc(&x, 5, 0).unwrap();
    ws[2].receive(&ins);
    ws[2].write_set(&x, s("s"), 0).unwrap();
    let b_state = ws[1].state.clone();
    ws[2].receive(&b_state);
    ws[2].write_set(&x, i(7), 0).unwrap();
    ws[3].receive(&ins);
    ws[3].write_set(&x, i(50), 100).unwrap();
    mesh(&mut ws);
    ws.iter().map(|w| w.state.value(&x, o)).collect()
}

#[test]
fn the_four_step_case_reads_50_not_55() {
    let o = opts();
    assert_eq!(four_step(&o), vec![Some(i(50)); 4]);
}

/// The switch `fold_counted_only` folds only the slots a base counts (the old `obs` set): the
/// older-generation slot C holds is left out, and the four-step case reads 55. This is its killer;
/// the design reads 50.
#[test]
fn fold_counted_only_is_killed_by_the_four_step_case() {
    let mut o = opts();
    o.set_fold_counted_only(true);
    assert_eq!(four_step(&o), vec![Some(i(55)); 4], "the mutant counts the 5 twice");
    assert_eq!(four_step(&opts()), vec![Some(i(50)); 4], "the design does not");
}

/// EG2: the same shape with a double base, which starts a generation and takes an `$inc` as a
/// register write. A adds 1; B sets a to 2.5 (a newer generation, which hides A's increment); A
/// receives it and sets a to 5; C, which saw only the insert, sets a to 5 with the newest stamp.
/// Every serial order reads 5; the fold of counted slots only reads 6.
fn eg2(o: &JoinOptions) -> Vec<Option<Value>> {
    let a_path = p("a");
    let mk = |c: char| Writer::new(wid(c, 0), o.clone());
    let mut ws = [mk('A'), mk('B'), mk('C')];
    ws[0].insert(&fl(&[("a", i(0))]), 0).unwrap();
    let ins = ws[0].state.clone();
    ws[0].write_inc(&a_path, 1, 0).unwrap();
    ws[1].receive(&ins);
    ws[1].write_set(&a_path, Value::Double(2.5), 0).unwrap();
    let b_state = ws[1].state.clone();
    ws[0].receive(&b_state);
    ws[0].write_set(&a_path, i(5), 0).unwrap();
    ws[2].receive(&ins);
    ws[2].write_set(&a_path, i(5), 100).unwrap();
    mesh(&mut ws);
    ws.iter().map(|w| w.state.value(&a_path, o)).collect()
}

#[test]
fn a_double_base_set_a_2_5_reads_5_not_6() {
    assert_eq!(eg2(&opts()), vec![Some(i(5)); 3]);
    let mut o = opts();
    o.set_fold_counted_only(true);
    assert_eq!(eg2(&o), vec![Some(i(6)); 3], "the counted-only fold reads the mutant's 6");
}

/// A map-marker base over a counter folds the counter's slots: `$set a {..}` over a counter at `a`
/// writes the marker as a register of `a` (a base) and a leaf below it. Returns the slot of the
/// writer's counter at `a` after the write.
fn marker_over_a_counter(o: &JoinOptions) -> Slot {
    let a_path = p("a");
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(&fl(&[("a", i(1))]), 0).unwrap();
    w.write_inc(&a_path, 5, 0).unwrap();
    let before = w.state.slots.values().next().unwrap().clone();
    assert_eq!(before.absorbed, None, "no base has held the increment yet");
    w.write_set(&a_path, Value::Document(vec![("b".to_string(), i(1))]), 0).unwrap();
    assert_eq!(
        w.state.regs.iter().find(|((_, q), _)| *q == a_path).map(|(_, r)| r.value.clone()),
        Some(Value::Map),
        "the write made a at a map marker"
    );
    assert_eq!(w.state.slots.len(), 1);
    w.state.slots.values().next().unwrap().clone()
}

/// The killer of the switch `no_fold_marker` (a write of a map marker folds no slot): the design
/// folds the counter's slot into its absorbed pair, the mutant leaves the pair out.
#[test]
fn a_map_marker_base_over_a_counter_folds_its_slots() {
    let sl = marker_over_a_counter(&opts());
    assert_eq!(sl.absorbed, Some((sl.stamp, sl.total)), "the marker write held the increment");
    assert_eq!(sl.total, Num::Int64(5));
    let mut o = opts();
    o.set_no_fold_marker(true);
    assert_eq!(marker_over_a_counter(&o).absorbed, None, "the mutant folds nothing at a marker");
}

/// A base write in the NEWEST alive life folds a slot of ANOTHER alive life: the shape of seed 792 of
/// the nested differential. C inserts `c` and adds 5 (a slot in C's insert life); D upserts a
/// counter at `y` (an upsert life beside the insert; D's life is the newest, same HLC and the
/// greater writer); C receives it and sets `c` to a double, a base written in D's life. Returns the
/// slot of C's counter at `c` (in C's life) after the write, and whether it is a slot of another
/// life than the one the write is in.
fn base_in_the_newest_life(o: &JoinOptions) -> (Slot, bool) {
    let c_path = p("c");
    let mut c = Writer::new(wid('C', 0), o.clone());
    let mut d = Writer::new(wid('D', 0), o.clone());
    c.insert(&fl(&[("c", i(0))]), 0).unwrap();
    c.write_inc(&c_path, 5, 0).unwrap();
    d.upsert_inc(&p("y"), 1, 0).unwrap();
    c.receive(&d.state.clone());
    let alive = c.alive();
    assert_eq!(alive.len(), 2, "an insert life and an upsert life are alive");
    c.write_set(&c_path, Value::Double(2.5), 0).unwrap();
    let key = (st(1, 'C'), c_path.clone(), wid('C', 0), None);
    let in_other_life = key.0 != *alive.last().unwrap();
    (c.state.slots[&key].clone(), in_other_life)
}

/// The killer of the switch `fold_life_only` (a base write folds only the slots of its own life): the
/// design folds the slot of the other alive life, the mutant leaves it unfolded.
#[test]
fn a_base_write_in_the_newest_alive_life_folds_a_slot_of_another_alive_life() {
    let (sl, other) = base_in_the_newest_life(&opts());
    assert!(other, "the slot is in the older life, the write in the newest");
    assert_eq!(sl.absorbed, Some((sl.stamp, sl.total)));
    let mut o = opts();
    o.set_fold_life_only(true);
    let (sl, other) = base_in_the_newest_life(&o);
    assert!(other);
    assert_eq!(sl.absorbed, None, "the mutant folds only the write's own life");
}

#[test]
fn a_base_write_folds_every_slot_of_the_path_of_any_generation() {
    let o = opts();
    let x = p("x");
    let mut b = Writer::new(wid('B', 0), o.clone());
    let mut c = Writer::new(wid('C', 0), o.clone());
    c.insert(&fl(&[("x", i(100)), ("y", i(1))]), 0).unwrap();
    b.receive(&c.state);
    b.write_inc(&x, 5, 0).unwrap();
    b.write_inc(&p("y"), 2, 0).unwrap();
    // C starts a generation, then receives B's slots (generation None: older, hidden)
    c.write_set(&x, s("s"), 0).unwrap();
    c.receive(&b.state);
    let slot = |w: &Writer, path: &Path| -> (Slot, SlotKey) {
        let (k, v) = w.state.slots.iter().find(|(k, _)| k.1 == *path).unwrap();
        (v.clone(), k.clone())
    };
    assert_eq!(slot(&c, &x).0.absorbed, None, "nothing has absorbed it yet");
    assert!(c.state.counted(&x, &o).is_empty(), "the older-generation slot is not counted");
    c.write_set(&x, i(7), 0).unwrap();
    let (sl, _) = slot(&c, &x);
    assert_eq!(sl.absorbed, Some((sl.stamp, sl.total)), "folded though not counted");
    // a base write on x folds the slots of x only
    assert_eq!(slot(&c, &p("y")).0.absorbed, None);
    c.write_set(&p("y"), i(9), 0).unwrap();
    let (sy, _) = slot(&c, &p("y"));
    assert_eq!(sy.absorbed, Some((sy.stamp, sy.total)));
}

#[test]
fn a_later_increment_keeps_the_absorbed_pair() {
    let o = opts();
    let x = p("x");
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(&fl(&[("x", i(10))]), 0).unwrap();
    w.write_inc(&x, 3, 0).unwrap();
    w.write_set(&x, i(20), 0).unwrap();
    let (_, before) = w.state.slots.iter().next().map(|(k, v)| (k.clone(), v.clone())).unwrap();
    let pair = before.absorbed.unwrap();
    assert_eq!(pair.1, Num::from(3));
    w.write_inc(&x, 4, 0).unwrap();
    let after = w.state.slots.values().next().unwrap();
    assert_eq!(after.total, Num::from(7));
    assert_eq!(after.absorbed, Some(pair), "an increment moves the slot, not what a base absorbed");
    assert_eq!(w.state.value(&x, &o), Some(i(24)), "20 + (7 - 3)");
}

/// The creation bases fold nothing, and need not: a creation (an insert, an upserting `$inc`, the
/// creation write of a `$setOnInsert` upsert) is refused while ANY life is alive, so at a creation
/// base the writer holds no slot in an alive life, and there is nothing to fold. This pins the
/// precondition: refused with a life of its own, with a life received by join, and below an existing
/// map; and accepted, with no slot to fold, when every life is dead. (The sim and the differentials
/// assert the same at every accepted creation they make.)
#[test]
fn a_creation_is_refused_while_any_life_is_alive_and_succeeds_with_dead_lives_only() {
    let o = opts();
    let nested = Path::new(["m", "n"]).unwrap();
    let below = Path::new(["m", "k"]).unwrap();
    let doc = |n: i64| vec![(p("m"), Value::Document(vec![("n".into(), i(n))]))];
    // every creation shape; each returns whether the writer accepted it
    let creations = |w: &mut Writer| -> Vec<bool> {
        vec![
            w.insert(&fl(&[("x", i(1))]), 0).is_some(),
            w.insert(&doc(5), 0).is_some(),
            w.upsert_inc(&p("x"), 1, 0).is_some(),
            // an upsert whose creation path sits under the existing map
            w.upsert_inc(&below, 1, 0).is_some(),
            w.upsert_soi_create(&fl(&[("x", i(1))]), &p("z"), &[], 0).is_some(),
            w.upsert_soi_create(&[], &below, &fl(&[("y", i(2))]), 0).is_some(),
            w.upsert_soi(&fl(&[("x", i(1))]), &below, 1, &[], 0).is_some(),
        ]
    };
    // a life of the writer's own, with a counter slot in it
    let mut a = Writer::new(wid('A', 0), o.clone());
    a.insert(&doc(1), 0).unwrap();
    a.write_inc(&nested, 2, 0).unwrap();
    let alive = a.alive();
    assert!(a.state.slots.keys().any(|k| alive.contains(&k.0)), "a slot in an alive life");
    let held = a.state.clone();
    assert_eq!(creations(&mut a), vec![false; 7], "refused while a life is alive");
    assert_eq!(a.state, held, "and nothing was written");
    // a life received by join
    let mut b = Writer::new(wid('B', 0), o.clone());
    b.receive(&held);
    assert_eq!(b.alive(), alive);
    let before = b.state.clone();
    assert_eq!(creations(&mut b), vec![false; 7], "refused while a received life is alive");
    assert_eq!(b.state, before);
    // a life that is alive in one of two lives (the other dead): still refused
    let mut c = Writer::new(wid('C', 0), o.clone());
    c.receive(&held);
    c.write_delete(0).unwrap();
    c.insert(&doc(7), 0).unwrap();
    c.write_inc(&nested, 1, 0).unwrap();
    let before = c.state.clone();
    assert_eq!(creations(&mut c), vec![false; 7]);
    assert_eq!(c.state, before);
    // dead lives only: every creation shape is accepted in turn, and none finds a slot to fold
    for shape in 0..7 {
        let mut d = Writer::new(wid('D', 0), o.clone());
        d.receive(&held);
        d.write_delete(0).unwrap();
        assert!(d.alive().is_empty());
        assert!(d.state.slots.is_empty(), "the dead life's slots are dropped with it");
        let ok = match shape {
            0 => d.insert(&fl(&[("x", i(1))]), 0).is_some(),
            1 => d.insert(&doc(5), 0).is_some(),
            2 => d.upsert_inc(&p("x"), 1, 0).is_some(),
            3 => d.upsert_inc(&below, 1, 0).is_some(),
            4 => d.upsert_soi_create(&fl(&[("x", i(1))]), &p("z"), &[], 0).is_some(),
            5 => d.upsert_soi_create(&[], &below, &fl(&[("y", i(2))]), 0).is_some(),
            _ => d.upsert_soi(&fl(&[("x", i(1))]), &below, 1, &[], 0).is_some(),
        };
        assert!(ok, "shape {shape} is accepted when every life is dead");
        assert!(
            d.state.slots.values().all(|s| s.absorbed.is_none()),
            "shape {shape}: nothing folded"
        );
    }
}

// ---- the read side: what a slot gained since its pair; B1 ---------------------------------

fn counter_state(slot: Slot) -> (MergeState, Path) {
    let life = st(1, 'A');
    let mut s = MergeState::default();
    s.lives.insert(life, upsert_life());
    let x = p("x");
    s.slots.insert((life, x.clone(), wid('A', 0), None), slot);
    s.vv.insert(wid('A', 0), h(9));
    (s, x)
}

#[test]
fn b1_the_field_is_present_when_the_slot_moved_since_its_absorbed_stamp() {
    let o = opts();
    let slot = |stamp: u64, total: i128, abs: Option<(u64, i128)>| Slot {
        stamp: st(stamp, 'A'),
        total: total.into(),
        absorbed: abs.map(|(sa, t)| (st(sa, 'A'), t.into())),
    };
    let value = |sl: Slot| {
        let (s, x) = counter_state(sl);
        s.check_decoded().unwrap();
        s.value(&x, &o)
    };
    // no pair: the slot counts whole, and a total of 0 is a present field
    assert_eq!(value(slot(5, 0, None)), Some(i(0)));
    // the pair is the slot as it is: nothing since, nothing to add, and no base: absent
    assert_eq!(value(slot(5, 3, Some((5, 3)))), None);
    // the slot moved (a later stamp) and came back to the pair's total: 0 more, but present
    assert_eq!(value(slot(6, 3, Some((5, 3)))), Some(i(0)));
    // it moved to another total: the difference
    assert_eq!(value(slot(6, 7, Some((5, 3)))), Some(i(4)));
    // the mutant that ignores the stamp reads the moved-and-came-back slot as absent again
    let mut m = opts();
    m.set_value_ignores_slot_stamp(true);
    let (s, x) = counter_state(slot(6, 3, Some((5, 3))));
    assert_eq!(s.value(&x, &m), None);
}

// ---- the join of a slot --------------------------------------------------------------------

fn one_slot(s: Slot) -> MergeState {
    counter_state(s).0
}

#[test]
fn a_slot_joins_by_the_higher_stamp_and_its_pair_by_the_higher_absorbed_stamp() {
    let o = opts();
    let key = (st(1, 'A'), p("x"), wid('A', 0), None::<MStamp>);
    let slot = |stamp: u64, total: i128, abs: Option<(u64, i128)>| Slot {
        stamp: st(stamp, 'A'),
        total: total.into(),
        absorbed: abs.map(|(sa, t)| (st(sa, 'A'), t.into())),
    };
    // s holds the newer slot with the older pair; t the older slot with the newer pair
    let s = one_slot(slot(6, 7, Some((3, 3))));
    let t = one_slot(slot(5, 5, Some((5, 5))));
    let (st_, _) = join(&s, &t, &o);
    let (ts, _) = join(&t, &s, &o);
    assert_eq!(st_, ts, "commutative");
    assert_eq!(st_.slots[&key], slot(6, 7, Some((5, 5))), "slot of s, pair of t");
    assert_eq!(join(&st_, &st_, &o).0, st_, "idempotent");
    assert_eq!(join(&st_, &s, &o).0, st_);
    // a pair on one side only is kept
    let u = one_slot(slot(5, 5, None));
    assert_eq!(join(&u, &s, &o).0.slots[&key], slot(6, 7, Some((3, 3))));
    assert_eq!(join(&s, &u, &o).0.slots[&key], slot(6, 7, Some((3, 3))));
    let v = one_slot(slot(8, 9, None));
    assert_eq!(join(&v, &t, &o).0.slots[&key], slot(8, 9, Some((5, 5))));
    // the same slot stamp, no pair against a pair: the pair is kept
    assert_eq!(
        join(&v, &one_slot(slot(8, 9, Some((8, 9)))), &o).0.slots[&key].absorbed,
        Some((st(8, 'A'), 9.into()))
    );
}

#[test]
fn an_absorbed_pair_goes_with_the_slots_dead_life() {
    let o = opts();
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(&fl(&[("x", i(1))]), 0).unwrap();
    w.write_inc(&p("x"), 2, 0).unwrap();
    w.write_set(&p("x"), i(9), 0).unwrap();
    assert!(w.state.slots.values().any(|s| s.absorbed.is_some()));
    let before = w.state.clone();
    w.write_delete(0).unwrap();
    assert!(w.state.slots.is_empty(), "the slot and its pair are dropped with the life");
    // a state that still holds them, joined into the writer, brings nothing back
    let (joined, _) = join(&w.state, &before, &o);
    assert!(joined.slots.is_empty());
}

// ---- the compact spelling: the flag byte, the type byte -------------------------------------

fn writer_a() -> WriterId {
    wid('A', 0)
}

/// The compact bytes of a state with one life (upsert, stamp (1, A)) and one slot of `x`, written by
/// hand: the writer and life tables, the sections, and the slot as given.
fn hand_slot_bytes(slot_tail: &[u8]) -> Vec<u8> {
    let mut b = b"QMS\x02".to_vec();
    b.push(1); // the writer table: one writer
    b.extend_from_slice(&writer_a().node.to_bytes());
    b.extend_from_slice(&writer_a().epoch.0);
    b.push(1); // the life table: one life (1, A)
    b.extend_from_slice(&h(1).to_bytes());
    b.push(0);
    b.extend_from_slice(&[1, 0, 0, 0, 1]); // lives: one, index 0, no seen, no order, kind upsert
    b.extend_from_slice(&[0, 0, 0]); // dead, deletes, regs
    b.extend_from_slice(&[1, 0, 1, 1, b'x', 0, 0]); // slots: one; life 0; path x; writer 0; no generation
    b.extend_from_slice(slot_tail);
    b.extend_from_slice(&[1, 0]); // the version vector: A ...
    b.extend_from_slice(&h(9).to_bytes());
    b.extend_from_slice(&[0, 0]); // losses, ords
    b
}

fn stamp_bytes(wall: u64) -> Vec<u8> {
    let mut b = h(wall).to_bytes().to_vec();
    b.push(0);
    b
}

#[test]
fn the_slot_flag_byte_spells_the_pair_and_both_types() {
    let zig = |v: i128| ((v << 1) ^ (v >> 127)) as u8; // small values: one byte
    let tail = |flags: u8, total: &[u8], abs: Option<(u64, &[u8])>| {
        let mut t = stamp_bytes(5);
        t.push(flags);
        t.extend_from_slice(total);
        if let Some((w, a)) = abs {
            t.extend(stamp_bytes(w));
            t.extend_from_slice(a);
        }
        t
    };
    let dec_bytes = |t: &str| t.parse::<bson::Decimal128>().unwrap().bytes();
    let slot = |total: Num, absorbed: Option<(u64, Num)>| Slot {
        stamp: st(5, 'A'),
        total,
        absorbed: absorbed.map(|(w, n)| (st(w, 'A'), n)),
    };
    let cases: Vec<(&str, Slot, Vec<u8>)> = vec![
        // Int64, no pair: flags 0
        ("int64", slot(Num::Int64(3), None), tail(0, &[zig(3)], None)),
        // Int32, no pair: type 1 in bits 1-2
        ("int32", slot(Num::Int32(3), None), tail(2, &[zig(3)], None)),
        // Decimal128, no pair: type 2
        ("decimal", slot(dec("1.5"), None), tail(4, &dec_bytes("1.5"), None)),
        // Int64 with an Int64 pair: bit 0
        (
            "int64 pair",
            slot(Num::Int64(3), Some((4, Num::Int64(-2)))),
            tail(1, &[zig(3)], Some((4, &[zig(-2)]))),
        ),
        // Int64 total, Int32 pair: the pair's type in bits 3-4
        (
            "int64 with int32 pair",
            slot(Num::Int64(3), Some((4, Num::Int32(3)))),
            tail(1 | 1 << 3, &[zig(3)], Some((4, &[zig(3)]))),
        ),
        // Int32 total, Decimal128 pair
        (
            "int32 with decimal pair",
            slot(Num::Int32(3), Some((4, dec("0.25")))),
            tail(1 | 1 << 1 | 2 << 3, &[zig(3)], Some((4, &dec_bytes("0.25")))),
        ),
        // Decimal128 total and pair
        (
            "decimal pair",
            slot(dec("3"), Some((4, dec("2")))),
            tail(1 | 2 << 1 | 2 << 3, &dec_bytes("3"), Some((4, &dec_bytes("2")))),
        ),
    ];
    for (name, sl, want_tail) in cases {
        let mut state = MergeState::default();
        state.lives.insert(st(1, 'A'), upsert_life());
        state.slots.insert((st(1, 'A'), p("x"), writer_a(), None), sl.clone());
        state.vv.insert(writer_a(), h(9));
        let want = hand_slot_bytes(&want_tail);
        assert_eq!(state.encode_compact(), want, "{name}");
        // the form spells a pair wider than its slot (an Int32 slot with a Decimal128 pair), and
        // decode then refuses it by the type clause (item 28 of the inventory): only the bytes
        // are under test here
        if sl.absorbed.as_ref().is_some_and(|a| a.1.label() > sl.total.label()) {
            assert_eq!(
                MergeState::decode_compact(&want),
                Err(DecodeError::AbsorbedWiderThanSlot),
                "{name}"
            );
            continue;
        }
        let back = MergeState::decode_compact(&want).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        assert_eq!(back, state, "{name}");
        assert_eq!(back.slots.values().next().unwrap(), &sl, "{name}: the types survive");
    }
}

#[test]
fn the_slot_flag_byte_refuses_what_it_does_not_spell() {
    let zig3 = 6u8;
    let with = |flags: u8, rest: &[u8]| {
        let mut t = stamp_bytes(5);
        t.push(flags);
        t.extend_from_slice(rest);
        hand_slot_bytes(&t)
    };
    let pair = |w: u64| {
        let mut r = vec![zig3];
        r.extend(stamp_bytes(w));
        r.push(zig3);
        r
    };
    let dec_check = |b: &[u8]| MergeState::decode_compact(b).map(|_| ());
    // the neighbours that are accepted
    assert_eq!(dec_check(&with(0, &[zig3])), Ok(()));
    assert_eq!(dec_check(&with(1, &pair(5))), Ok(()));
    assert_eq!(dec_check(&with(1 | 1 << 1 | 1 << 3, &pair(5))), Ok(()));
    // type 3 of the total, type 3 of the pair
    assert_eq!(dec_check(&with(3 << 1, &[zig3])), Err(DecodeError::BadTag(6)));
    assert_eq!(dec_check(&with(1 | 3 << 3, &pair(5))), Err(DecodeError::BadTag(25)));
    // a pair type with no pair
    assert_eq!(dec_check(&with(1 << 3, &[zig3])), Err(DecodeError::BadTag(8)));
    assert_eq!(dec_check(&with(2 << 3, &[zig3])), Err(DecodeError::BadTag(16)));
    // any other bit
    for bit in [0x20u8, 0x40, 0x80] {
        assert_eq!(dec_check(&with(bit, &[zig3])), Err(DecodeError::BadTag(bit)), "{bit:#x}");
    }
    // a total that is not the shortest varint, and one past the bound
    assert_eq!(dec_check(&with(0, &[0x86, 0x00])), Err(DecodeError::NotCanonical));
    let mut big = state_with_total(Num::Int64(MAX_TOTAL + 1));
    assert_eq!(
        MergeState::decode_compact(&big.encode_compact()),
        Err(DecodeError::TotalOutOfRange)
    );
    big.slots.values_mut().for_each(|s| s.total = Num::Int32(-MAX_TOTAL - 1));
    assert_eq!(
        MergeState::decode_compact(&big.encode_compact()),
        Err(DecodeError::TotalOutOfRange)
    );
    // an absorbed total past the bound
    let mut big = state_with_total(Num::Int64(1));
    big.slots.values_mut().for_each(|s| s.absorbed = Some((s.stamp, Num::Int64(MAX_TOTAL + 1))));
    assert_eq!(
        MergeState::decode_compact(&big.encode_compact()),
        Err(DecodeError::TotalOutOfRange)
    );
}

fn state_with_total(total: Num) -> MergeState {
    let mut state = MergeState::default();
    state.lives.insert(st(1, 'A'), upsert_life());
    state.slots.insert(
        (st(1, 'A'), p("x"), writer_a(), None),
        Slot { stamp: st(5, 'A'), total, absorbed: None },
    );
    state.vv.insert(writer_a(), h(9));
    state
}

#[test]
fn a_generations_seen_entry_and_a_loss_total_carry_a_type_byte() {
    // a typed total outside a slot: the type byte, then the payload
    let life = st(1, 'A');
    let mut state = MergeState::default();
    state.lives.insert(life, upsert_life());
    state.vv.insert(writer_a(), h(9));
    let gen_stamp = st(2, 'A');
    let mut seen = BTreeMap::new();
    seen.insert((life, writer_a(), None), Num::Int32(4));
    seen.insert((life, wid('B', 0), None), dec("2.50"));
    state.vv.insert(wid('B', 0), h(1));
    state.regs.insert(
        (life, p("x")),
        Reg {
            stamp: gen_stamp,
            value: s("t"),
            generation: Some(Gen { stamp: gen_stamp, vv: Vv::new(), seen: seen.clone() }),
            soi: false,
        },
    );
    state.losses.insert(
        LossKey::field(LossKind::Clash, p("x"), gen_stamp),
        LossValue::Total(Num::Int32(-3)),
    );
    state
        .losses
        .insert(LossKey::field(LossKind::Clash, p("y"), gen_stamp), LossValue::Total(dec("7")));
    state.losses.insert(
        LossKey::field(LossKind::Clash, p("z"), gen_stamp),
        LossValue::Total(Num::Int64(9)),
    );
    let enc = state.encode_compact();
    let back = MergeState::decode_compact(&enc).unwrap();
    assert_eq!(back, state);
    let g = back.regs.values().next().unwrap().generation.as_ref().unwrap();
    assert_eq!(g.seen, seen, "the types of the seen totals survive");
    assert_eq!(back.encode_compact(), enc);
    // an Int64 total costs the type byte: the loss value tag 1, the type byte 0, the zigzag of 9
    assert!(
        enc.windows(3).any(|w| w == [1u8, 0, 18]),
        "an Int64 loss total is a type byte 0 and a zigzag varint"
    );
    // a type byte of 3 or more is refused, in a loss total and in a seen entry
    let mut bad = enc.clone();
    let loss_at = enc.windows(3).position(|w| w == [1u8, 0, 18]).unwrap() + 1;
    bad[loss_at] = 3;
    assert_eq!(MergeState::decode_compact(&bad), Err(DecodeError::BadTag(3)));
    let seen_at = enc.windows(2).position(|w| w == [1u8, 8]).expect("the Int32 seen entry");
    let mut bad = enc.clone();
    bad[seen_at] = 7;
    assert_eq!(MergeState::decode_compact(&bad), Err(DecodeError::BadTag(7)));
}

#[test]
fn typed_slots_read_and_round_trip_through_a_writer() {
    let o = opts();
    let x = p("x");
    // Decimal128: a decimal base, a decimal increment, an Int32 base over it, a decimal increment on top
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(&fl(&[("x", dval("1.5"))]), 0).unwrap();
    w.write_inc_value(&x, &dval("2.25"), 0).unwrap();
    w.write_set(&x, Value::Int32(7), 0).unwrap();
    let sl = w.state.slots.values().next().unwrap().clone();
    assert_eq!(sl.total, dec("2.25"));
    assert_eq!(sl.absorbed, Some((sl.stamp, dec("2.25"))));
    w.write_inc_value(&x, &dval("1"), 0).unwrap();
    let sl = w.state.slots.values().next().unwrap().clone();
    assert_eq!(sl.total, dec("3.25"));
    assert_eq!(sl.absorbed.as_ref().unwrap().1, dec("2.25"));
    // the difference 3.25 - 2.25 = 1.00 on top of the Int32 base 7
    match w.state.value(&x, &o) {
        Some(Value::Decimal128(b)) => {
            assert_eq!(bson::Decimal128::from_bytes(b).to_string(), "8.00");
        }
        v => panic!("{v:?}"),
    }
    let enc = w.state.encode_compact();
    assert_eq!(MergeState::decode_compact(&enc).unwrap(), w.state);
    assert_eq!(w.state.try_encode(), Err(V1CannotHold));

    // Int32 widening: the slot becomes Int64 while its pair stays the Int32 it absorbed
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(&fl(&[("x", Value::Int32(1))]), 0).unwrap();
    w.write_inc_value(&x, &Value::Int32(5), 0).unwrap();
    w.write_set(&x, Value::Int32(100), 0).unwrap();
    assert_eq!(w.state.slots.values().next().unwrap().absorbed.as_ref().unwrap().1, Num::Int32(5));
    w.write_inc_value(&x, &Value::Int32(i32::MAX), 0).unwrap();
    let sl = w.state.slots.values().next().unwrap().clone();
    assert_eq!(sl.total, Num::Int64(5 + i128::from(i32::MAX)));
    assert_eq!(sl.absorbed.as_ref().unwrap().1, Num::Int32(5));
    assert_eq!(w.state.value(&x, &o), Some(Value::Int64(100 + i64::from(i32::MAX))));
    let enc = w.state.encode_compact();
    let back = MergeState::decode_compact(&enc).unwrap();
    assert_eq!(back, w.state);
    assert_eq!(back.slots.values().next().unwrap().absorbed.as_ref().unwrap().1, Num::Int32(5));
}

#[test]
fn a_state_with_an_absorbed_pair_has_no_version_1_spelling() {
    let o = opts();
    let mut w = Writer::new(wid('A', 0), o);
    w.insert(&fl(&[("x", i(1))]), 0).unwrap();
    w.write_inc(&p("x"), 2, 0).unwrap();
    assert!(w.state.v1_can_hold() || !w.state.ords.is_empty());
    let mut bare = w.state.clone();
    bare.ords.clear();
    assert!(bare.v1_can_hold(), "no pair yet");
    w.write_set(&p("x"), i(5), 0).unwrap();
    let mut bare = w.state.clone();
    bare.ords.clear();
    assert!(bare.slots.values().any(|s| s.absorbed.is_some()));
    assert!(!bare.v1_can_hold());
    assert_eq!(bare.try_canonical_bytes(), Err(V1CannotHold));
    assert_eq!(bare.try_state_hash(), Err(V1CannotHold));
    assert_eq!(bare.try_encode(), Err(V1CannotHold));
    // the compact form spells it
    assert_eq!(MergeState::decode_compact(&bare.encode_compact()).unwrap(), bare);
    // and v1 decode never produces a pair or an observed total
    let none = {
        let mut x = bare.clone();
        x.slots.values_mut().for_each(|s| s.absorbed = None);
        x
    };
    assert_eq!(MergeState::decode(&none.try_encode().unwrap()).unwrap(), none);
}

// ---- decode: the new refusals and the vector coverage ---------------------------------------

/// A state with a slot and an absorbed pair, every stamp covered.
fn paired() -> MergeState {
    let life = st(1, 'A');
    let mut state = MergeState::default();
    state.lives.insert(life, upsert_life());
    state.slots.insert(
        (life, p("x"), writer_a(), None),
        Slot { stamp: st(5, 'A'), total: 7.into(), absorbed: Some((st(3, 'A'), 4.into())) },
    );
    state.vv.insert(writer_a(), h(9));
    state
}

fn refuses(state: &MergeState, err: DecodeError) {
    assert_eq!(MergeState::decode_compact(&state.encode_compact()), Err(err));
}

fn accepts(state: &MergeState) {
    assert_eq!(MergeState::decode_compact(&state.encode_compact()).as_ref(), Ok(state));
}

#[test]
fn decode_refuses_an_absorbed_stamp_above_its_slot() {
    accepts(&paired());
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(6, 'A'), 4.into())));
    refuses(&s, DecodeError::AbsorbedAboveSlot);
    // above by the counter, by the writer: the order of stamps is (hlc, node, epoch)
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(5, 'B'), 7.into())));
    s.vv.insert(wid('B', 0), h(5));
    refuses(&s, DecodeError::AbsorbedAboveSlot);
    // at the slot's own stamp or below it is accepted
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(5, 'A'), 7.into())));
    accepts(&s);
}

#[test]
fn decode_refuses_an_absorbed_total_that_disagrees_at_the_slots_own_stamp() {
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(5, 'A'), 8.into())));
    refuses(&s, DecodeError::AbsorbedTotalMismatch);
    // the same number in another type is another total
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(5, 'A'), Num::Int32(7))));
    refuses(&s, DecodeError::AbsorbedTotalMismatch);
    // below the slot's stamp the totals may differ
    accepts(&paired());
}

#[test]
fn decode_refuses_an_absorbed_stamp_by_another_writer_than_the_slots_key() {
    // the pair is below the slot's stamp, so only the writer is wrong; B's entry in the vector
    // covers it, so the refusal is the writer's and not the vector's
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(3, 'B'), 4.into())));
    s.vv.insert(wid('B', 0), h(3));
    assert_eq!(s.check_decoded(), Err(DecodeError::AbsorbedWriterMismatch));
    // it can be encoded (the compact form spells the absorbed stamp in full), and is refused on decode
    refuses(&s, DecodeError::AbsorbedWriterMismatch);
    // another epoch of the same node is another writer
    let mut s = paired();
    s.slots.values_mut().for_each(|x| {
        x.absorbed = Some((MStamp { hlc: h(3), writer: wid('A', 1) }, 4.into()));
    });
    s.vv.insert(wid('A', 1), h(3));
    refuses(&s, DecodeError::AbsorbedWriterMismatch);
    // the same pair by the key's writer is accepted
    accepts(&paired());
    // a slot of a key of B with a pair by B is accepted, and with a pair by A refused
    let mut s = MergeState::default();
    s.lives.insert(st(1, 'A'), upsert_life());
    s.vv.insert(wid('A', 0), h(9));
    s.vv.insert(wid('B', 0), h(9));
    let key = (st(1, 'A'), p("x"), wid('B', 0), None);
    s.slots.insert(
        key.clone(),
        Slot { stamp: st(5, 'B'), total: 7.into(), absorbed: Some((st(3, 'B'), 4.into())) },
    );
    accepts(&s);
    s.slots.get_mut(&key).unwrap().absorbed = Some((st(3, 'A'), 4.into()));
    refuses(&s, DecodeError::AbsorbedWriterMismatch);
}

/// Item 27 of the inventory: the slot's OWN stamp is by the writer of its key, like the pair's. A
/// slot of A's key stamped by B was accepted before, with no pair, with a pair by A, and refused
/// only with a pair by B (as the pair's mismatch).
#[test]
fn decode_refuses_a_slot_stamped_by_another_writer_than_its_key() {
    let key = (st(1, 'A'), p("x"), wid('A', 0), None);
    let mut s = MergeState::default();
    s.lives.insert(st(1, 'A'), upsert_life());
    s.vv.insert(wid('A', 0), h(9));
    s.vv.insert(wid('B', 0), h(9));
    // no pair
    s.slots.insert(key.clone(), Slot { stamp: st(5, 'B'), total: 3.into(), absorbed: None });
    assert_eq!(s.check_decoded(), Err(DecodeError::SlotWriterMismatch));
    refuses(&s, DecodeError::SlotWriterMismatch);
    // a pair by the key's writer, below the slot
    s.slots.get_mut(&key).unwrap().absorbed = Some((st(3, 'A'), 1.into()));
    refuses(&s, DecodeError::SlotWriterMismatch);
    // a pair by the slot stamp's writer: the slot's refusal comes first
    s.slots.get_mut(&key).unwrap().absorbed = Some((st(3, 'B'), 1.into()));
    refuses(&s, DecodeError::SlotWriterMismatch);
    // another epoch of the key's node is another writer
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.stamp = MStamp { hlc: h(5), writer: wid('A', 1) });
    s.vv.insert(wid('A', 1), h(5));
    s.slots.values_mut().for_each(|x| x.absorbed = None);
    refuses(&s, DecodeError::SlotWriterMismatch);
    // the slot of a key of B, stamped by B, is accepted (with and without a pair), and by A refused
    let mut s = MergeState::default();
    s.lives.insert(st(1, 'A'), upsert_life());
    s.vv.insert(wid('A', 0), h(9));
    s.vv.insert(wid('B', 0), h(9));
    let key = (st(1, 'A'), p("x"), wid('B', 0), None);
    s.slots.insert(key.clone(), Slot { stamp: st(5, 'B'), total: 7.into(), absorbed: None });
    accepts(&s);
    s.slots.get_mut(&key).unwrap().absorbed = Some((st(3, 'B'), 4.into()));
    accepts(&s);
    s.slots.get_mut(&key).unwrap().stamp = st(5, 'A');
    refuses(&s, DecodeError::SlotWriterMismatch);
    // a stamp by the key's writer with the pair by another is still the pair's refusal
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(3, 'B'), 4.into())));
    s.vv.insert(wid('B', 0), h(3));
    refuses(&s, DecodeError::AbsorbedWriterMismatch);
}

/// Item 28 of the inventory: a pair's type label is not wider than its slot's, in the order
/// `Int32 < Int64 < Decimal128` (the label as spelled, not the promoted kind).
#[test]
fn decode_refuses_an_absorbed_total_of_a_wider_type_than_the_slot() {
    // the slot (5, A) with `slot` as its total, the pair (3, A) with `pair`
    let with = |slot: Num, pair: Num| {
        let mut s = paired();
        s.slots.values_mut().for_each(|x| {
            x.total = slot;
            x.absorbed = Some((st(3, 'A'), pair));
        });
        s
    };
    let (i32n, i64n, d) = (Num::Int32(4), Num::Int64(4), dec("4"));
    // the reviewer's case: an Int64 slot with a Decimal128 pair read as a Decimal128
    refuses(&with(i64n, d), DecodeError::AbsorbedWiderThanSlot);
    refuses(&with(i32n, d), DecodeError::AbsorbedWiderThanSlot);
    refuses(&with(i32n, i64n), DecodeError::AbsorbedWiderThanSlot);
    // the same label, and every narrower pair, is accepted
    for (slot, pair) in
        [(i32n, i32n), (i64n, i64n), (d, d), (i64n, i32n), (d, i32n), (d, i64n), (d, dec("4.00"))]
    {
        accepts(&with(slot, pair));
    }
    // the label, not the kind: an Int32 amount outside the i32 range is an Int32 label (its kind is
    // Int64, but a writer that keeps adding Int32s keeps the label), so an Int64 pair is wider
    let big = Num::Int32(1 << 40);
    accepts(&with(big, Num::Int32(5)));
    refuses(&with(big, Num::Int64(5)), DecodeError::AbsorbedWiderThanSlot);
    assert_eq!(with(i64n, d).check_decoded(), Err(DecodeError::AbsorbedWiderThanSlot));
    // at the slot's own stamp the totals are equal, so the labels are: the type clause never
    // reports what the total clause does
    let mut s = paired();
    s.slots.values_mut().for_each(|x| {
        x.total = i64n;
        x.absorbed = Some((st(5, 'A'), d));
    });
    refuses(&s, DecodeError::AbsorbedTotalMismatch);
    // the clauses before it keep their verdicts
    let mut s = with(i64n, d);
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(6, 'A'), d)));
    refuses(&s, DecodeError::AbsorbedAboveSlot);
}

/// The honest relation, from writers: the slot's label only widens across its writer's writes and
/// the pair keeps the label the slot had when a base write absorbed it, so every state of an
/// Int32 / Int64 / Decimal128 history decodes.
#[test]
fn every_state_of_a_typed_history_decodes() {
    let o = opts();
    let x = p("x");
    let mut w = Writer::new(wid('A', 0), o.clone());
    let mut states = Vec::new();
    w.insert(&fl(&[("x", Value::Int32(1))]), 0).unwrap();
    states.push(w.state.clone());
    for step in 0..3 {
        w.write_set(&x, Value::Int32(step), 0).unwrap();
        w.write_inc_value(&x, &Value::Int32(i32::MAX), 0).unwrap();
        states.push(w.state.clone());
        w.write_set(&x, Value::Int32(step), 0).unwrap();
        states.push(w.state.clone());
        w.write_inc_value(&x, &Value::Int64(1 << 40), 0).unwrap();
        states.push(w.state.clone());
        w.write_set(&x, s("base"), 0).unwrap();
        states.push(w.state.clone());
    }
    w.write_set(&x, Value::Int32(1), 0).unwrap();
    w.write_inc_value(&x, &dval("2.5"), 0).unwrap();
    states.push(w.state.clone());
    w.write_set(&x, Value::Int64(9), 0).unwrap();
    states.push(w.state.clone());
    w.write_inc_value(&x, &dval("1"), 0).unwrap();
    states.push(w.state.clone());
    // the widest label a pair ever had was below the slot's
    let widths: BTreeSet<_> = states
        .iter()
        .flat_map(|s| s.slots.values())
        .filter_map(|sl| sl.absorbed.as_ref().map(|a| (a.1.label(), sl.total.label())))
        .collect();
    assert!(
        widths.iter().any(|(a, t)| a < t),
        "a history with a pair narrower than its slot: {widths:?}"
    );
    for state in &states {
        assert_eq!(state.check_decoded(), Ok(()));
        accepts(state);
    }
}

/// `check_decoded` is what `decode_compact` runs, so the state itself gives the same verdicts.
#[test]
fn the_new_refusals_are_the_verdicts_of_check_decoded() {
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(6, 'A'), 4.into())));
    assert_eq!(s.check_decoded(), Err(DecodeError::AbsorbedAboveSlot));
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(5, 'A'), 8.into())));
    assert_eq!(s.check_decoded(), Err(DecodeError::AbsorbedTotalMismatch));
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.stamp = st(5, 'B'));
    assert_eq!(s.check_decoded(), Err(DecodeError::SlotWriterMismatch));
    let mut s = paired();
    s.slots.values_mut().for_each(|x| x.absorbed = Some((st(3, 'A'), dec("1"))));
    assert_eq!(s.check_decoded(), Err(DecodeError::AbsorbedWiderThanSlot));
    assert_eq!(paired().check_decoded(), Ok(()));
}

/// Condition (d): the vector covers the absorbed stamp, the generation of a slot key, a register's
/// generation stamp, the life and generation of a `seen` key, the deletes a life has seen and the
/// entries of a delete's vector. Each: behind by one tick is `VersionVectorBehind`, covered exactly is
/// accepted. Loss stamps are not part of it.
#[test]
fn the_version_vector_covers_every_stamp_an_honest_state_covers() {
    let w = writer_a();
    let base = |vv: u64| {
        let mut state = MergeState::default();
        state.lives.insert(st(1, 'A'), upsert_life());
        state.vv.insert(w, h(vv));
        state
    };
    type Add = fn(&mut MergeState);
    // the stamp each case adds is (9, B); B's entry in the vector is the thing under test
    let b = wid('B', 0);
    let cases: [(&str, Add); 6] = [
        ("an absorbed stamp", |state| {
            state.slots.insert(
                (st(1, 'A'), p("x"), wid('A', 0), None),
                Slot { stamp: st(5, 'A'), total: 1.into(), absorbed: Some((st(5, 'A'), 1.into())) },
            );
            state.slots.insert(
                (st(1, 'A'), p("y"), wid('B', 0), None),
                Slot { stamp: st(9, 'B'), total: 1.into(), absorbed: Some((st(9, 'B'), 1.into())) },
            );
        }),
        ("the generation of a slot key", |state| {
            state.slots.insert(
                (st(1, 'A'), p("x"), wid('A', 0), Some(st(9, 'B'))),
                Slot { stamp: st(5, 'A'), total: 1.into(), absorbed: None },
            );
        }),
        ("a register's generation", |state| {
            state.regs.insert(
                (st(1, 'A'), p("x")),
                Reg {
                    stamp: st(5, 'A'),
                    value: s("t"),
                    generation: Some(Gen {
                        stamp: st(9, 'B'),
                        vv: Vv::new(),
                        seen: BTreeMap::new(),
                    }),
                    soi: false,
                },
            );
        }),
        ("the generation of a seen key", |state| {
            let mut seen = BTreeMap::new();
            seen.insert((st(1, 'A'), wid('A', 0), Some(st(9, 'B'))), Num::Int64(1));
            state.regs.insert(
                (st(1, 'A'), p("x")),
                Reg {
                    stamp: st(5, 'A'),
                    value: s("t"),
                    generation: Some(Gen { stamp: st(5, 'A'), vv: Vv::new(), seen }),
                    soi: false,
                },
            );
        }),
        ("the life of a seen key", |state| {
            let mut seen = BTreeMap::new();
            seen.insert((st(9, 'B'), wid('A', 0), None), Num::Int64(1));
            state.regs.insert(
                (st(1, 'A'), p("x")),
                Reg {
                    stamp: st(5, 'A'),
                    value: s("t"),
                    generation: Some(Gen { stamp: st(5, 'A'), vv: Vv::new(), seen }),
                    soi: false,
                },
            );
        }),
        ("a delete a life has seen", |state| {
            let life = state.lives.get_mut(&st(1, 'A')).unwrap();
            life.seen.insert(st(9, 'B'));
        }),
    ];
    for (what, add) in cases {
        let (mut behind, mut exact) = (base(9), base(9));
        // A covers its own stamps up to 9; B's entry is 8 (behind) or 9 (exact)
        behind.vv.insert(b, h(8));
        exact.vv.insert(b, h(9));
        add(&mut behind);
        add(&mut exact);
        refuses(&behind, DecodeError::VersionVectorBehind);
        accepts(&exact);
        // and a vector that does not mention B at all is behind too
        let mut missing = base(9);
        add(&mut missing);
        refuses(&missing, DecodeError::VersionVectorBehind);
        let _ = what;
    }
    // the vector of a delete: its entries are covered, each by the state's own entry
    for (entry, ok) in [(8u64, false), (9, true)] {
        let mut state = base(9);
        state.vv.insert(b, h(entry));
        let mut dvv = Vv::new();
        dvv.insert(b, h(9));
        state.deletes.insert(st(2, 'A'), dvv);
        if ok {
            accepts(&state);
        } else {
            refuses(&state, DecodeError::VersionVectorBehind);
        }
    }
    // the stamp of a loss record is not covered by the vector (a loss entry arrives without its write)
    let mut state = base(9);
    state
        .losses
        .insert(LossKey::field(LossKind::Clash, p("x"), st(99, 'C')), LossValue::Value(i(1)));
    accepts(&state);
    let _ = Hlc::ZERO;
}

/// A register's generation `seen` is started by a NON-numeric base only (`Writer::setreg`, the
/// model's `_setreg`); a numeric `$set` or an `$unset` carries the winner's generation, dangling
/// keys included; another alive life keeps its copy until that life ends. The seed 792 shape,
/// built with writers (the reviewer's probe P5): C's first insert is superseded by B's newer first
/// insert and then collected, and the generation `seen` that C's register wrote names C's slot.
/// Those keys are dead for every read (`hidden` reads `seen` for the slots of alive lives only)
/// and bounded by the slots alive at the generation start; decode accepts the state.
#[test]
fn a_dangling_seen_key_survives_a_numeric_base_and_ends_with_a_non_numeric_one() {
    let o = JoinOptions::new(5, i64::MAX).unwrap();
    let c_path = p("c");
    let mut c = Writer::new(wid('C', 0), o.clone());
    let mut d = Writer::new(wid('D', 0), o.clone());
    let mut b = Writer::new(wid('B', 0), o.clone());
    c.insert(&fl(&[("c", i(0))]), 0).unwrap();
    c.write_inc(&c_path, 5, 0).unwrap();
    d.upsert_inc(&p("y"), 1, 0).unwrap();
    c.receive(&d.state.clone());
    assert_eq!(c.alive().len(), 2, "C's insert and D's upsert life are alive");
    // a double base starts a generation that saw C's slot
    c.write_set(&c_path, Value::Double(2.5), 0).unwrap();
    // the `seen` keys of every register's generation that name a slot the state does not hold
    let dangling = |w: &Writer| -> usize {
        w.state
            .regs
            .iter()
            .filter_map(|((_, q), r)| r.generation.as_ref().map(|g| (q, g)))
            .flat_map(|(q, g)| g.seen.keys().map(move |k| (q.clone(), *k)))
            .filter(|(q, k)| !w.state.slots.contains_key(&(k.0, q.clone(), k.1, k.2)))
            .count()
    };
    assert_eq!(dangling(&c), 0);
    // B's newer first insert supersedes C's insert; the clock then passes the retention
    b.insert(&fl(&[("z", i(1))]), 3).unwrap();
    c.receive(&b.state.clone());
    for k in 0..8 {
        c.write_set(&p("w"), i(k), 0).unwrap();
    }
    let life_c = MStamp { hlc: h(1), writer: wid('C', 0) };
    assert!(!c.state.lives.contains_key(&life_c), "C's insert is collected");
    assert_eq!(dangling(&c), 1, "the register's generation names the collected slot");
    // a NUMERIC base carries the winner's generation, dangling key included: the register the
    // write makes in the newest life copies it, the old one stays where it is
    c.write_set(&c_path, i(7), 0).unwrap();
    assert_eq!(dangling(&c), 2);
    // an unset carries it too
    c.write_unset(&c_path, 0).unwrap();
    assert_eq!(dangling(&c), 2);
    // the state is one decode accepts, and no read depends on the dangling keys
    accepts(&c.state);
    // a NON-numeric base starts a generation over the alive slots: the copy in the newest life
    // is replaced, the one in the other alive life stays
    c.write_set(&c_path, s("t"), 0).unwrap();
    assert_eq!(dangling(&c), 1);
}

/// A `Decimal128` total that is a NaN or has a non-canonical payload. THE SLOT-TOTAL RULE: the
/// compact reader takes the 16 bytes of a `Decimal128` as they are (`num_payload`: no check of the
/// class, the coefficient or the exponent; only an integer payload is bounded, by `MAX_TOTAL`), and
/// the same reader reads the slot's total, the absorbed total, a `seen` total and a loss total, so
/// the pair is accepted exactly as the slot is: the bytes survive a round trip untouched, and a pair
/// that differs from the slot at the slot's own stamp is refused by its bytes (the totals are
/// compared byte for byte, `Num::eq`), a NaN with another payload included. The reads
/// (`dec_raw_bits`) take any bit pattern, a NaN as the one NaN, and do not panic.
#[test]
fn a_nan_or_non_canonical_decimal_total_is_accepted_as_spelled_for_the_pair_as_for_the_slot() {
    let raw = |bits: u128| Num::Decimal128(bits.to_le_bytes());
    const NAN: u128 = 0b11111 << 122;
    const INF: u128 = 0b11110 << 122;
    let payloads = [
        ("quiet NaN", raw(NAN)),
        ("negative NaN with a payload", raw(NAN | 1 << 127 | 0x1234)),
        ("NaN with a payload and the signalling bit", raw(NAN | (1 << 121) | 0xdead_beef)),
        // a coefficient above 10^34 - 1 reads as a zero (IEEE 754 3.5.2), spelled as it is
        ("coefficient past the bound", raw((6176u128 << 113) | (10u128.pow(34) + 7))),
        // the large-coefficient form (bits 125-126 set, neither Infinity nor NaN) reads as a zero
        ("large-coefficient form", raw(0b11 << 125 | 5)),
        ("negative infinity", raw(INF | 1 << 127)),
        ("a zero with an exponent beyond the range", raw(0x1fffu128 << 113)),
    ];
    let o = opts();
    for (what, payload) in payloads {
        // as the slot's total, alone
        let mut slot_only = paired();
        slot_only.slots.values_mut().for_each(|x| {
            x.total = payload;
            x.absorbed = None;
        });
        accepts(&slot_only);
        // as the absorbed total, below a Decimal128 slot (the pair is not wider than its slot)
        let mut pair_only = paired();
        pair_only.slots.values_mut().for_each(|x| {
            x.total = dec("7");
            x.absorbed = Some((st(3, 'A'), payload));
        });
        accepts(&pair_only);
        // as both, at the slot's own stamp: equal bytes, accepted
        let mut both = paired();
        both.slots.values_mut().for_each(|x| {
            x.total = payload;
            x.absorbed = Some((x.stamp, payload));
        });
        accepts(&both);
        // and the exact bytes are the ones that come back
        for state in [&slot_only, &pair_only, &both] {
            let back = MergeState::decode_compact(&state.encode_compact()).unwrap();
            for (a, b) in state.slots.values().zip(back.slots.values()) {
                let bytes = |n: &Num| match n {
                    Num::Decimal128(x) => *x,
                    n => panic!("{what}: {n:?}"),
                };
                assert_eq!(bytes(&a.total), bytes(&b.total), "{what}");
                assert_eq!(
                    a.absorbed.map(|x| bytes(&x.1)),
                    b.absorbed.map(|x| bytes(&x.1)),
                    "{what}"
                );
            }
            // the reads take the pattern as it is and do not panic
            let _ = back.value(&p("x"), &o);
            let _ = back.hidden(&p("x"), &o);
        }
        // at the slot's own stamp the pair must equal the slot byte for byte: another payload of
        // the same class is another total, for the pair as for any other comparison of totals
        let other = match payload {
            Num::Decimal128(b) => {
                let mut b = b;
                b[0] ^= 1;
                Num::Decimal128(b)
            }
            n => n,
        };
        let mut mismatch = paired();
        mismatch.slots.values_mut().for_each(|x| {
            x.total = payload;
            x.absorbed = Some((x.stamp, other));
        });
        refuses(&mismatch, DecodeError::AbsorbedTotalMismatch);
    }
}

/// WHAT THE WRITER CAN PRODUCE as a Decimal128 total (the derivation behind the decode of decimals,
/// kept as a test so it cannot drift). `inc_result` / `sum_checked` refuse a decimal sum only when it
/// overflows from FINITE inputs (`Num::decimal_overflowed`), and no check of the operand looks at its
/// class (`Value::check_input`), so a NaN or an infinite operand is a legal `$inc`; and the first slot
/// of a writer at a path stores the operand as it was given (`Writer::inc_plan`: `None => op`), so the
/// bytes of a NaN with a payload or of a non-canonical coefficient survive into the slot verbatim.
/// Later sums are canonical (`pack_raw` asserts it), but a base write folds the verbatim total into
/// the pair, which a later increment leaves. So every 16-byte pattern is a slot total, a pair, and
/// (copied) a `seen` or a loss total of an honest history, and decode refuses none of them.
#[test]
fn the_writer_takes_nan_infinite_and_non_canonical_decimal_operands() {
    let o = opts();
    let x = p("x");
    let raw = |bits: u128| Value::Decimal128(bits.to_le_bytes());
    const NAN: u128 = 0b11111 << 122;
    const INF: u128 = 0b11110 << 122;
    let cases: [(&str, u128); 7] = [
        ("quiet NaN", NAN),
        ("negative NaN with a payload", NAN | 1 << 127 | 0x1234),
        ("NaN with a payload and the signalling bit", NAN | (1 << 121) | 0xdead_beef),
        ("coefficient past 10^34 - 1", (6176u128 << 113) | (10u128.pow(34) + 7)),
        ("large-coefficient form", 0b11 << 125 | 5),
        ("negative infinity", INF | 1 << 127),
        ("a zero with an exponent beyond the range", 0x1fffu128 << 113),
    ];
    let bytes = |n: &Num| match n {
        Num::Decimal128(b) => u128::from_le_bytes(*b),
        n => panic!("{n:?}"),
    };
    for (what, bits) in cases {
        let mut w = Writer::new(wid('A', 0), o.clone());
        w.insert(&fl(&[("x", i(1))]), 0).unwrap();
        // the first increment of the writer: the operand's bytes are the slot total, untouched
        w.write_inc_value(&x, &raw(bits), 0).unwrap_or_else(|| panic!("{what}: refused"));
        let sl = w.state.slots.values().next().unwrap().clone();
        assert_eq!(bytes(&sl.total), bits, "{what}: stored verbatim");
        accepts(&w.state);
        // a base write folds it into the pair, and a later increment moves the slot and leaves the pair
        w.write_set(&x, i(5), 0).unwrap();
        w.write_inc_value(&x, &dval("1"), 0).unwrap();
        let sl = w.state.slots.values().next().unwrap().clone();
        assert_eq!(
            bytes(&sl.absorbed.as_ref().unwrap().1),
            bits,
            "{what}: the pair keeps the bytes"
        );
        assert!(sl.absorbed.as_ref().unwrap().0 < sl.stamp);
        accepts(&w.state);
    }
}
