//! Tests of the typed counters (D6): `Int32`, `Int64`, `Double` and `Decimal128` amounts merge as
//! counters. The model has no such types, so these are Rust-only tests; each rule has a
//! scenario that fails without it (a mutation switch of [`JoinOptions`], run by
//! `the_counter_mutants_are_caught`).

use super::tests::{Mem, all_orders, fl, opts, p, wid};
use super::*;

fn dec(s: &str) -> Value {
    Value::Decimal128(s.parse::<bson::Decimal128>().unwrap().bytes())
}

/// A visible value as text: a decimal by its digits, anything else as `Debug`.
fn text(v: &Value) -> String {
    match v {
        Value::Decimal128(b) => bson::Decimal128::from_bytes(*b).to_string(),
        v => format!("{v:?}"),
    }
}

fn inc_v(m: &mut Mem, path: &str, by: &Value, sk: u64) -> MStamp {
    let r = m.w.write_inc_value(&p(path), by, sk);
    m.log(r).expect("the $inc was refused")
}

/// `I` creates the document with `init`; `A` and `B`, both holding it, each `$inc` `path`
/// concurrently (with the given clock skews). The value in every delivery order of the two
/// entries into a fresh observer, which must be one value with one state hash.
fn concurrent(
    o: &JoinOptions,
    init: &[(&str, Value)],
    path: &str,
    a: (&Value, u64),
    b: (&Value, u64),
) -> Value {
    let (mut mi, mut ma, mut mb) = (Mem::new('I', o), Mem::new('A', o), Mem::new('B', o));
    let i0 = mi.ins(init, 0).unwrap();
    ma.receive_entry(&mi.ent(i0));
    mb.receive_entry(&mi.ent(i0));
    let sa = inc_v(&mut ma, path, a.0, a.1);
    let sb = inc_v(&mut mb, path, b.0, b.1);
    let ents = [ma.ent(sa), mb.ent(sb)];
    let orders = all_orders(o, &ents, &[mi.ent(i0)]);
    let first = orders[0].1.w.state.compact_hash();
    let v = orders[0].1.visible().unwrap()[&p(path)].clone();
    for (ord, m) in &orders {
        assert_eq!(m.visible().unwrap()[&p(path)], v, "delivery {ord:?}");
        assert_eq!(m.w.state.compact_hash(), first, "hash, delivery {ord:?}");
    }
    v
}

fn i32v(n: i32) -> Value {
    Value::Int32(n)
}
fn i64v(n: i64) -> Value {
    Value::Int64(n)
}
fn dbl(d: f64) -> Value {
    Value::Double(d)
}

// ---- the type of a merged cell ---------------------------------------------------------------

fn type_join(o: &JoinOptions) -> Result<(), String> {
    let c = |init: Value, a: Value, b: Value| concurrent(o, &[("n", init)], "n", (&a, 0), (&b, 3));
    let want = |what: &str, got: Value, want: Value| {
        (got == want).then_some(()).ok_or(format!("{what}: read {got:?}, want {want:?}"))
    };
    want("Int32 + Int32 + Int32", c(i32v(5), i32v(2), i32v(3)), i32v(10))?;
    want("an Int64 on one side", c(i32v(5), i32v(2), i64v(3)), i64v(10))?;
    want("an Int64 base", c(i64v(5), i32v(2), i32v(3)), i64v(10))?;
    want("a Double on one side", c(i32v(5), i32v(2), dbl(0.5)), dbl(7.5))?;
    want("an Int base, two Doubles", c(i64v(5), dbl(0.5), dbl(0.25)), dbl(5.75))?;
    want("a Decimal128 on one side", c(i64v(5), i32v(2), dec("0.5")), dec("7.5"))?;
    want("a Decimal128 above a Double", c(i32v(5), dbl(0.5), dec("0.25")), dec("5.75"))?;
    want("a Double base, an Int32 each", c(dbl(2.5), i32v(1), i32v(2)), dbl(5.5))?;
    Ok(())
}

#[test]
fn the_type_of_a_merged_cell_is_the_join_of_its_parts() {
    type_join(&opts()).unwrap();
}

#[test]
fn an_int32_total_that_outgrows_i32_is_promoted_to_int64() {
    let o = opts();
    // each side fits an i32; the merged total does not
    let two_billion = i32v(2_000_000_000);
    let v = concurrent(&o, &[("n", i32v(0))], "n", (&two_billion, 0), (&two_billion, 3));
    assert_eq!(v, i64v(4_000_000_000));
    // locally too: production's rule
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(&fl(&[("n", i32v(i32::MAX))]), 0).unwrap();
    w.write_inc_value(&p("n"), &i32v(1), 0).unwrap();
    assert_eq!(w.visible().unwrap()[&p("n")], i64v(i64::from(i32::MAX) + 1));
}

fn promotion_scenario(o: &JoinOptions) -> Result<(), String> {
    let t = i32v(2_000_000_000);
    let v = concurrent(o, &[("n", i32v(0))], "n", (&t, 0), (&t, 3));
    (v == i64v(4_000_000_000)).then_some(()).ok_or(format!("read {v:?}"))
}

/// A writer whose increment made the cell an Int64 leaves it one: the total dips back under the
/// i32 range and the type stays.
fn no_narrowing_scenario(o: &JoinOptions) -> Result<(), String> {
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(&fl(&[("n", i32v(2_000_000_000))]), 0).unwrap();
    w.write_inc_value(&p("n"), &i32v(2_000_000_000), 0).unwrap();
    let up = w.visible().unwrap()[&p("n")].clone();
    if up != i64v(4_000_000_000) {
        return Err(format!("up: {up:?}"));
    }
    w.write_inc_value(&p("n"), &i32v(-2_000_000_000), 0).unwrap();
    w.write_inc_value(&p("n"), &i32v(-1_000_000_000), 0).unwrap();
    let down = w.visible().unwrap()[&p("n")].clone();
    // 1e9 fits an i32, and the cell stays an Int64
    (down == i64v(1_000_000_000)).then_some(()).ok_or(format!("down: {down:?}"))
}

#[test]
fn a_cell_that_became_an_int64_by_a_write_never_narrows_again() {
    no_narrowing_scenario(&opts()).unwrap();
}

/// THE ONE DEVIATION: a total that crossed the i32 range only in a merge narrows when a later
/// merge brings it back, because no write saw it.
#[test]
fn a_total_that_crossed_only_in_a_merge_narrows_again() {
    let o = opts();
    let (mut mi, mut ma, mut mb) = (Mem::new('I', &o), Mem::new('A', &o), Mem::new('B', &o));
    let i0 = mi.ins(&[("n", i32v(0))], 0).unwrap();
    ma.receive_entry(&mi.ent(i0));
    mb.receive_entry(&mi.ent(i0));
    let sa = inc_v(&mut ma, "n", &i32v(2_000_000_000), 0);
    let sb = inc_v(&mut mb, "n", &i32v(2_000_000_000), 3);
    let mut obs = Mem::new('O', &o);
    for e in [mi.ent(i0), ma.ent(sa), mb.ent(sb)] {
        obs.receive_entry(&e);
    }
    assert_eq!(obs.visible().unwrap()[&p("n")], i64v(4_000_000_000));
    // B takes its 2e9 back before it ever sees A's increment: the merge totals 2e9 again, and
    // nobody wrote an Int64, so the cell is an Int32 again
    let sb2 = inc_v(&mut mb, "n", &i32v(-2_000_000_000), 3);
    obs.receive_entry(&mb.ent(sb2));
    assert_eq!(obs.visible().unwrap()[&p("n")], i32v(2_000_000_000), "narrowed again");
}

// ---- production's rule for a local $inc -----------------------------------------------------

#[test]
fn a_local_inc_follows_the_production_rule_and_refuses_what_it_refuses() {
    let o = opts();
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(
        &fl(&[
            ("a", i32v(5)),
            ("b", dec("19.99")),
            ("c", i64v(i64::MAX)),
            ("s", Value::String("t".into())),
        ]),
        0,
    )
    .unwrap();
    let at = |w: &Writer, k: &str| w.visible().unwrap()[&p(k)].clone();
    // Int32 + Int32 stays Int32, an Int64 operand gives an Int64
    w.write_inc_value(&p("a"), &i32v(1), 0).unwrap();
    assert_eq!(at(&w, "a"), i32v(6));
    w.write_inc_value(&p("a"), &i64v(1), 0).unwrap();
    assert_eq!(at(&w, "a"), i64v(7));
    // an absent field is the Int32 zero
    w.write_inc_value(&p("new32"), &i32v(7), 0).unwrap();
    w.write_inc_value(&p("new64"), &i64v(7), 0).unwrap();
    assert_eq!((at(&w, "new32"), at(&w, "new64")), (i32v(7), i64v(7)));
    // an Int64 past the bound, a string, a bool operand: refused
    assert!(w.write_inc_value(&p("c"), &i32v(1), 0).is_none());
    assert!(w.write_inc_value(&p("s"), &i32v(1), 0).is_none());
    assert!(w.write_inc_value(&p("a"), &Value::Bool(true), 0).is_none());
    // a decimal: an exact double is applied, an inexact one is refused (the 400 of production)
    w.write_inc_value(&p("b"), &dbl(0.5), 0).unwrap();
    assert_eq!(text(&at(&w, "b")), "20.49");
    assert!(w.write_inc_value(&p("b"), &dbl(0.01), 0).is_none());
    assert_eq!(text(&at(&w, "b")), "20.49", "a refusal changed nothing");
    // a decimal that overflows from finite inputs is refused
    w.write_set(&p("b"), dec("9.999999999999999999999999999999999E+6144"), 0).unwrap();
    assert!(
        w.write_inc_value(&p("b"), &dec("9.999999999999999999999999999999999E+6144"), 0).is_none()
    );
}

// ---- the canonical order ---------------------------------------------------------------------

/// `1e16 + 1.0 - 1e16`: the order the two writers' parts are added in decides between 0.0 and
/// 1.0. A's stamp is the later one, so the canonical order (stamp) adds B's `-1e16` first.
fn double_order_scenario(o: &JoinOptions) -> Result<(), String> {
    let v = concurrent(o, &[("x", dbl(1e16))], "x", (&dbl(1.0), 9), (&dbl(-1e16), 0));
    (v == dbl(1.0)).then_some(()).ok_or(format!("read {v:?}, want the canonical 1.0"))
}

/// The same with decimals: `1E+34 + 5` rounds to `1E+34` (a tie to even), so adding the `5`
/// last keeps it and adding it first loses it.
fn decimal_order_scenario(o: &JoinOptions) -> Result<(), String> {
    let v = concurrent(o, &[("x", dec("1E+34"))], "x", (&dec("5"), 9), (&dec("-1E+34"), 0));
    (text(&v) == "5").then_some(()).ok_or(format!("read {}, want the canonical 5", text(&v)))
}

#[test]
fn double_and_decimal_cells_add_their_parts_in_the_canonical_order() {
    double_order_scenario(&opts()).unwrap();
    decimal_order_scenario(&opts()).unwrap();
    // and the other way round: B's stamp is the later one, so A's part is added first
    let v = concurrent(&opts(), &[("x", dbl(1e16))], "x", (&dbl(1.0), 0), (&dbl(-1e16), 9));
    assert_eq!(v, dbl(0.0));
}

#[test]
fn decimal_counters_are_exact() {
    // 0.1 + 0.2 is 0.3 in a decimal; in a double it is 0.30000000000000004
    let v = concurrent(&opts(), &[("x", dec("0"))], "x", (&dec("0.1"), 0), (&dec("0.2"), 3));
    assert_eq!(text(&v), "0.3");
    let v = concurrent(&opts(), &[("x", dbl(0.0))], "x", (&dbl(0.1), 0), (&dbl(0.2), 3));
    assert_eq!(v, dbl(0.1 + 0.2));
}

// ---- both codecs -----------------------------------------------------------------------------

#[test]
fn typed_counters_round_trip_in_the_compact_form_and_version_1_refuses_them() {
    let o = opts();
    let mut w = Writer::new(wid('A', 0), o.clone());
    w.insert(&fl(&[("a", i32v(5)), ("b", dbl(1.5)), ("c", dec("2.5")), ("d", i64v(9))]), 0)
        .unwrap();
    let mut v = Writer::new(wid('B', 0), o.clone());
    v.receive(&w.state);
    for (k, by) in [("a", i32v(1)), ("b", dbl(0.25)), ("c", dec("0.5")), ("d", i64v(1))] {
        v.write_inc_value(&p(k), &by, 0).unwrap();
        w.write_inc_value(&p(k), &by, 3).unwrap();
    }
    // a rebase over the observed slots, and a lost total
    w.write_set(&p("a"), i32v(100), 0).unwrap();
    w.receive(&v.state);
    for st in [&w.state, &v.state] {
        let c = st.encode_compact();
        let back = MergeState::decode_compact(&c).unwrap();
        assert_eq!(&back, st);
        assert_eq!(back.encode_compact(), c);
        assert_eq!(back.visible(&o), st.visible(&o));
    }
    assert!(!v.state.v1_can_hold());
    assert!(v.state.try_encode().is_err() && v.state.try_canonical_bytes().is_err());
    // an Int64-only state is spelled by version 1 as before
    let mut only = Writer::new(wid('C', 0), o);
    only.insert(&fl(&[("d", i64v(9))]), 0).unwrap();
    only.write_inc(&p("d"), 1, 0).unwrap();
    assert!(only.state.v1_can_hold());
    assert_eq!(MergeState::decode(&only.state.encode()).unwrap(), only.state);
}

// ---- every rule has a mutant that a scenario catches ------------------------------------------

#[test]
fn the_counter_mutants_are_caught() {
    type Switch = fn(&mut JoinOptions);
    type Scenario = fn(&JoinOptions) -> Result<(), String>;
    let cases: [(&str, Switch, Scenario); 4] = [
        (
            "slots are added in key order, not stamp order",
            |o| o.set_sum_in_key_order(true),
            double_order_scenario,
        ),
        ("the same for decimals", |o| o.set_sum_in_key_order(true), decimal_order_scenario),
        (
            "an Int32 cell saturates instead of becoming an Int64",
            |o| o.set_int32_saturates(true),
            promotion_scenario,
        ),
        (
            "a slot stays an Int32 when the cell became an Int64",
            |o| o.set_slot_never_widens(true),
            no_narrowing_scenario,
        ),
    ];
    for (what, mutate, scenario) in cases {
        assert_eq!(scenario(&opts()), Ok(()), "the scenario holds without the mutant: {what}");
        let mut o = opts();
        mutate(&mut o);
        assert!(scenario(&o).is_err(), "not caught: {what}");
    }
}

// ---- the caps --------------------------------------------------------------------------------

fn nth(k: usize, o: &JoinOptions) -> Writer {
    let node = crate::ids::NodeId::from_bytes([k as u8; 16]);
    let id = WriterId { node, epoch: Epoch::from_u64((k / 256) as u64) };
    Writer::new(id, o.clone())
}

/// A state in which writers `0..n` each `$inc` the Int64 counter `n` once, one after another,
/// and the writer that made it.
fn counter_of(n: usize, o: &JoinOptions) -> MergeState {
    let mut state = MergeState::default();
    for k in 0..n {
        let mut w = nth(k, o);
        w.receive(&state);
        if k == 0 {
            w.insert(&fl(&[("n", i64v(0))]), 0).unwrap();
        }
        w.write_inc(&p("n"), 1, 0).unwrap();
        state = w.state;
    }
    state
}

fn contributor_cap(o: &JoinOptions) -> Result<(), String> {
    let full = counter_of(MAX_CONTRIBUTORS, o);
    let n = full.contributors(&p("n"), o).len();
    if n != MAX_CONTRIBUTORS {
        return Err(format!("{n} contributors"));
    }
    // the 65th is refused; a contributor is not
    let mut newcomer = nth(200, o);
    newcomer.receive(&full);
    if newcomer.write_inc(&p("n"), 1, 0).is_some() {
        return Err("the 65th contributor was accepted".into());
    }
    let mut old = nth(5, o);
    old.receive(&full);
    old.write_inc(&p("n"), 1, 0).ok_or("a contributor was refused")?;
    Ok(())
}

#[test]
fn a_65th_contributor_is_refused_and_an_existing_one_is_not() {
    contributor_cap(&opts()).unwrap();
}

/// Two members each add the 64th contributor to a counter of 63: the merge has 65. It is kept
/// and reported (Q8), a newcomer is still refused, and a write that brings the counter back
/// under the cap lets the newcomer in.
#[test]
fn a_merge_past_the_contributor_cap_is_kept_and_reported_never_refused() {
    let o = opts();
    let s63 = counter_of(MAX_CONTRIBUTORS - 1, &o);
    let (mut a, mut b) = (nth(100, &o), nth(101, &o));
    a.receive(&s63);
    b.receive(&s63);
    a.write_inc(&p("n"), 1, 0).unwrap();
    b.write_inc(&p("n"), 1, 3).unwrap();
    assert_eq!(a.state.over_caps(&o), OverCaps::default());
    let (ab, ba) = (join(&a.state, &b.state, &o).0, join(&b.state, &a.state, &o).0);
    assert_eq!(ab, ba);
    assert_eq!(ab.compact_hash(), ba.compact_hash());
    assert_eq!(ab.visible(&o).unwrap()[&p("n")], i64v(65), "every increment counts");
    assert_eq!(
        ab.over_caps(&o),
        OverCaps { writers: None, counters: [(p("n"), MAX_CONTRIBUTORS + 1)].into() }
    );
    // the receiving writer keeps the whole state
    let mut c = nth(102, &o);
    assert!(c.receive_report(&ab).changed);
    assert_eq!(c.state.over_caps(&o).counters.len(), 1);
    assert!(c.write_inc(&p("n"), 1, 0).is_none(), "a newcomer is still refused");
    let mut old = nth(100, &o);
    old.receive(&ab);
    old.write_inc(&p("n"), 1, 0).expect("a contributor may go on");
    // a string base and a number over it: the old slots are no longer counted
    old.write_set(&p("n"), Value::String("s".into()), 0).unwrap();
    old.write_set(&p("n"), i64v(0), 0).unwrap();
    assert!(old.state.contributors(&p("n"), &o).is_empty());
    c.receive(&old.state);
    c.write_inc(&p("n"), 1, 0).expect("back under the cap: the newcomer is let in");
}

fn writer_cap(o: &JoinOptions) -> Result<(), String> {
    let mut state = MergeState::default();
    for k in 0..MAX_WRITERS {
        let mut w = nth(k, o);
        w.receive(&state);
        if k == 0 {
            w.insert(&fl(&[("n", i64v(0))]), 0).unwrap();
        }
        w.write_set(&p(&format!("f{k}")), i64v(k as i64), 0).ok_or("a write was refused early")?;
        state = w.state;
    }
    let n = state.writers().len();
    if n != MAX_WRITERS {
        return Err(format!("{n} writers"));
    }
    let mut late = nth(MAX_WRITERS, o);
    late.receive(&state);
    if late.write_set(&p("late"), i64v(1), 0).is_some() {
        return Err("the 257th writer was accepted".into());
    }
    let mut old = nth(7, o);
    old.receive(&state);
    old.write_set(&p("again"), i64v(1), 0).ok_or("a known writer was refused")?;
    Ok(())
}

#[test]
fn a_257th_writer_is_refused_and_a_known_one_is_not() {
    writer_cap(&opts()).unwrap();
}

#[test]
fn a_merge_past_the_writer_cap_is_kept_and_reported_never_refused() {
    let o = opts();
    let mut state = MergeState::default();
    for k in 0..MAX_WRITERS - 1 {
        let mut w = nth(k, &o);
        w.receive(&state);
        if k == 0 {
            w.insert(&fl(&[("n", i64v(0))]), 0).unwrap();
        }
        w.write_set(&p(&format!("f{k}")), i64v(1), 0).unwrap();
        state = w.state;
    }
    // 255 writers; two newcomers each make the 256th
    let (mut a2, mut b2) = (nth(255, &o), nth(256, &o));
    a2.receive(&state);
    b2.receive(&state);
    a2.write_set(&p("a"), i64v(1), 0).unwrap();
    b2.write_set(&p("b"), i64v(1), 3).unwrap();
    let ab = join(&a2.state, &b2.state, &o).0;
    assert_eq!(ab.over_caps(&o).writers, Some(MAX_WRITERS + 1));
    assert!(
        ab.visible(&o).unwrap().contains_key(&p("a"))
            && ab.visible(&o).unwrap().contains_key(&p("b"))
    );
    let mut late = nth(300, &o);
    late.receive(&ab);
    assert!(late.write_set(&p("c"), i64v(1), 0).is_none(), "a new writer is refused");
    a2.receive(&ab);
    assert!(a2.write_set(&p("c"), i64v(1), 0).is_some(), "a writer the document has may go on");
}

#[test]
fn the_cap_mutants_are_caught() {
    for (what, mutate, scenario) in [
        (
            "no contributor cap",
            (|o: &mut JoinOptions| o.set_no_contributor_cap(true)) as fn(&mut JoinOptions),
            contributor_cap as fn(&JoinOptions) -> Result<(), String>,
        ),
        ("no writer cap", |o| o.set_no_writer_cap(true), writer_cap),
    ] {
        assert_eq!(scenario(&opts()), Ok(()), "{what}");
        let mut o = opts();
        mutate(&mut o);
        assert!(scenario(&o).is_err(), "not caught: {what}");
    }
}
