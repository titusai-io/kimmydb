//! R1: the type of a counter is a function of the merged state, because concurrent increments
//! have no order. It is the narrowest of `Int32` and `Int64` that holds the total, never narrower
//! than the widest type among the base and every operand seen.

use super::join::join;
use super::tests::{fl, opts, p, wid};
use super::*;

fn i32v(n: i32) -> Value {
    Value::Int32(n)
}

fn i64v(n: i64) -> Value {
    Value::Int64(n)
}

fn n(w: &Writer) -> Value {
    w.visible().unwrap()[&p("n")].clone()
}

fn base(v: Value) -> Writer {
    let mut w = Writer::new(wid('I', 0), opts());
    w.insert(&fl(&[("n", v)]), 0).unwrap();
    w
}

/// The input the property test of the update path found: the base `Int32` minimum, then `$inc 7`
/// and `$inc 2147483647`, both `Int32`, total 6. The writer's own running sum passes the `i32`
/// range (2147483654) and used to make the cell an `Int64`; the engine says `Int32`.
#[test]
fn the_minimised_input_is_an_int32() {
    // one writer, one after the other
    let mut w = base(i32v(i32::MIN));
    w.write_inc_value(&p("n"), &i32v(7), 0).unwrap();
    w.write_inc_value(&p("n"), &i32v(i32::MAX), 0).unwrap();
    assert_eq!(n(&w), i32v(6));
    // two writers, concurrently, in both join orders
    let root = base(i32v(i32::MIN));
    let (mut a, mut b) = (Writer::new(wid('A', 0), opts()), Writer::new(wid('B', 0), opts()));
    a.receive(&root.state);
    b.receive(&root.state);
    a.write_inc_value(&p("n"), &i32v(7), 0).unwrap();
    b.write_inc_value(&p("n"), &i32v(i32::MAX), 3).unwrap();
    let ab = join(&a.state, &b.state, &opts()).0;
    let ba = join(&b.state, &a.state, &opts()).0;
    assert_eq!(ab.visible(&opts()).unwrap()[&p("n")], i32v(6));
    assert_eq!(ba.visible(&opts()).unwrap()[&p("n")], i32v(6));
}

fn inc(w: &mut Writer, by: Value) -> Result<Option<Written>, UpdateRefused> {
    w.write_update(&[UpdateOp::Inc(p("n"), by)], 0)
}

fn dec(s: &str) -> Value {
    Value::Decimal128(s.parse::<bson::Decimal128>().unwrap().bytes())
}

fn dec_text(v: &Value) -> String {
    match v {
        Value::Decimal128(b) => bson::Decimal128::from_bytes(*b).to_string(),
        v => panic!("not a decimal: {v:?}"),
    }
}

/// The recorded integer-type cases, `q7s2-probes/reference-6.0.5/q7s2.out` (line of the result,
/// operands, result of the reference):
///
/// | lines (reply-after) | case | operands | reference result | here |
/// |---|---|---|---|---|
/// | 267-268 | d8 | Int32 5 `$inc` Int64 1 | `n` = NumberLong 6 | `Int64(6)`, modified |
/// | 272-273 | d9 | Int32 2147483647 `$inc` Int32 1 | `n` = NumberLong 2147483648 | `Int64(2147483648)`, modified |
/// | 277-278 | d10 | Int64 max `$inc` Int32 1 | error code 2, the document unchanged | `IncOutOfRange`, state and clock unchanged |
/// | 381-382 | z2 | Int32 5 `$inc` Int64 0 | `modified` 1, `n` = NumberLong 5 | modified, `Int64(5)` |
/// | 386-387 | z3 | Int32 5 `$inc` Double 0.0 | `modified` 1, `n` = 5.0 (Double) | THE DEVIATION `field-merge-refusals-q7` (2): refused, `IncDoubleOnCounter`, nothing changed |
///
/// z4 (lines 392-393: Int64 5 `$inc` Int32 0, `modified` 0, `n` stays NumberLong 5): the value and
/// the type are the reference's, but the WRITER records every increment it is given, this one included
/// (a slot move). That is the writer's contract; the no-op of an increment whose result is unchanged
/// by representation (ADR-208) belongs to the effects layer, in one place, and its test (z4, with
/// the typed `$inc` table) is the effects layer's.
#[test]
fn the_recorded_integer_types() {
    let mut w = base(i32v(5)); // d8
    assert!(inc(&mut w, i64v(1)).unwrap().is_some());
    assert_eq!(n(&w), i64v(6));
    let mut w = base(i32v(i32::MAX)); // d9
    assert!(inc(&mut w, i32v(1)).unwrap().is_some());
    assert_eq!(n(&w), i64v(i64::from(i32::MAX) + 1));
    let mut w = base(i64v(i64::MAX)); // d10
    let before = (w.state.clone(), w.clock);
    assert_eq!(inc(&mut w, i32v(1)), Err(UpdateRefused::IncOutOfRange(p("n"))));
    assert_eq!((w.state.clone(), w.clock), before);
    assert_eq!(n(&w), i64v(i64::MAX));
    let mut w = base(i32v(5)); // z2: a modification
    assert!(inc(&mut w, i64v(0)).unwrap().is_some());
    assert_eq!(n(&w), i64v(5));
    let mut w = base(i32v(5)); // z3: the deviation, refused whole
    let before = (w.state.clone(), w.clock);
    assert_eq!(inc(&mut w, Value::Double(0.0)), Err(UpdateRefused::IncDoubleOnCounter(p("n"))));
    assert_eq!((w.state.clone(), w.clock), before);
    assert_eq!(n(&w), i32v(5));
    // z4: the reference reports `modified` 0 and writes nothing; the writer records the increment
    // it is given (a slot move) and the value and the type are the reference's: the effects layer
    // drops such an increment before it reaches the writer
    let mut w = base(i64v(5));
    assert!(inc(&mut w, i32v(0)).unwrap().is_some());
    assert_eq!(n(&w), i64v(5));
}

/// The other recorded `$inc` cases of the same recording (line of the result):
///
/// | lines (reply-after) | case | operands | reference result | here |
/// |---|---|---|---|---|
/// | 229-230 | d1 | Int32 5 `$inc` Double 1.5 | Double 6.5 | DEVIATION (`field-merge-refusals-q7`, 2): refused |
/// | 235-236 | d2 | Int64 7 `$inc` Double 0.5 | Double 7.5 | DEVIATION: refused |
/// | 240-241 | d3 | Int32 5 `$inc` Double 1.0 | Double 6.0 | DEVIATION: refused |
/// | 246-247 | d4 | Int64 5 `$inc` Double 1.0 | Double 6.0 | DEVIATION: refused |
/// | 251-252 | d5 | Decimal128 2.5 `$inc` Double 1.5 | Decimal128 4.00000000000000 | DEVIATION: refused |
/// | 256-257 | d6 | Decimal128 2.5 `$inc` Int32 1 | Decimal128 3.5 | Decimal128 3.5 |
/// | 261-262 | d7 | Int32 5 `$inc` Decimal128 1.5 | Decimal128 6.5 | Decimal128 6.5 |
/// | 282-283 | d11 | Double 1.5 `$inc` Int32 1 | Double 2.5 | Double 2.5 (a double is a register) |
#[test]
fn the_recorded_double_and_decimal_cases() {
    let refused = |base_v: Value, by: Value| {
        let mut w = base(base_v.clone());
        let before = (w.state.clone(), w.clock);
        assert_eq!(inc(&mut w, by), Err(UpdateRefused::IncDoubleOnCounter(p("n"))));
        assert_eq!((w.state.clone(), w.clock), before);
        assert_eq!(n(&w), base_v);
    };
    refused(i32v(5), Value::Double(1.5)); // d1
    refused(i64v(7), Value::Double(0.5)); // d2
    refused(i32v(5), Value::Double(1.0)); // d3
    refused(i64v(5), Value::Double(1.0)); // d4
    refused(dec("2.5"), Value::Double(1.5)); // d5
    let mut w = base(dec("2.5")); // d6
    assert!(inc(&mut w, i32v(1)).unwrap().is_some());
    assert_eq!(dec_text(&n(&w)), "3.5");
    let mut w = base(i32v(5)); // d7
    assert!(inc(&mut w, dec("1.5")).unwrap().is_some());
    assert_eq!(dec_text(&n(&w)), "6.5");
    let mut w = base(Value::Double(1.5)); // d11
    assert!(inc(&mut w, i32v(1)).unwrap().is_some());
    assert_eq!(n(&w), Value::Double(2.5));
}

/// An `Int64` operand keeps the counter an `Int64` for good, wherever the total goes.
#[test]
fn an_int64_operand_makes_the_counter_int64_whatever_the_total() {
    let mut w = base(i32v(5));
    w.write_inc_value(&p("n"), &i64v(10), 0).unwrap();
    w.write_inc_value(&p("n"), &i32v(-10), 0).unwrap();
    assert_eq!(n(&w), i64v(5));
}

/// THE DEVIATION (`counter-type-q7`): a sequential history whose running value leaves the `i32`
/// range and comes back is an `Int32` again here; the reference stays an `Int64`.
#[test]
fn a_sequential_history_that_leaves_and_returns_is_the_deviation() {
    let mut w = base(i32v(2_000_000_000));
    w.write_inc_value(&p("n"), &i32v(2_000_000_000), 0).unwrap();
    assert_eq!(n(&w), i64v(4_000_000_000));
    w.write_inc_value(&p("n"), &i32v(-2_000_000_000), 0).unwrap();
    w.write_inc_value(&p("n"), &i32v(-1_000_000_000), 0).unwrap();
    assert_eq!(n(&w), i32v(1_000_000_000));
}

fn lcg(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
    *seed >> 33
}

/// The rendered type and value do not depend on the order in which the contributions are
/// joined: three writers each add one to three operands to the same base, and the six orders of
/// the join (and a repeat) give one answer.
#[test]
fn the_type_does_not_depend_on_the_order_the_contributions_are_joined() {
    let o = opts();
    let operands = [
        i32v(7),
        i32v(i32::MAX),
        i32v(i32::MIN),
        i32v(-5),
        i64v(3),
        i64v(-4_000_000_000),
        i32v(1_000_000_000),
        i32v(2_000_000_000),
    ];
    let bases = [i32v(i32::MIN), i32v(5), i64v(5), i32v(i32::MAX), i32v(0)];
    let orders: [[usize; 3]; 6] =
        [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
    let mut seed = 7u64;
    let mut types = std::collections::HashSet::new();
    for case in 0..400 {
        let root = base(bases[(lcg(&mut seed) % bases.len() as u64) as usize].clone());
        let mut states = Vec::new();
        for (k, c) in ['A', 'B', 'C'].into_iter().enumerate() {
            let mut w = Writer::new(wid(c, 0), o.clone());
            w.receive(&root.state);
            for _ in 0..(1 + lcg(&mut seed) % 3) {
                let by = &operands[(lcg(&mut seed) % operands.len() as u64) as usize];
                // a refused increment (past the bound) is simply not part of the history
                let _ = w.write_inc_value(&p("n"), by, k as u64);
            }
            states.push(w.state);
        }
        let mut answers = Vec::new();
        for order in orders {
            let mut acc = states[order[0]].clone();
            for &next in &order[1..] {
                acc = join(&acc, &states[next], &o).0;
            }
            answers.push(acc.visible(&o).unwrap()[&p("n")].clone());
        }
        assert!(answers.windows(2).all(|w| w[0] == w[1]), "case {case}: {answers:?}");
        types.insert(std::mem::discriminant(&answers[0]));
    }
    assert!(types.len() >= 2, "the cases reach both Int32 and Int64 totals");
}

// ---------------------------------------------------------------------------------------------
// R1 as a property: the type and the value of a counter are the base plus every operand ever
// added, whatever the order of the joins, the writers that saw each other, and the number of
// writers. The expected answer is computed here, from the operands alone.
// ---------------------------------------------------------------------------------------------

/// One amount of the property: an operand or a base.
#[derive(Clone, Debug)]
enum Amt {
    I32(i32),
    I64(i64),
    /// A decimal with two decimals, as text.
    Dec(&'static str),
}

impl Amt {
    fn value(&self) -> Value {
        match self {
            Amt::I32(v) => i32v(*v),
            Amt::I64(v) => i64v(*v),
            Amt::Dec(t) => dec(t),
        }
    }

    /// The width of the type: `Int32` 0, `Int64` 1, `Decimal128` 2.
    fn width(&self) -> u8 {
        match self {
            Amt::I32(_) => 0,
            Amt::I64(_) => 1,
            Amt::Dec(_) => 2,
        }
    }

    /// The amount in hundredths.
    fn cents(&self) -> i128 {
        match self {
            Amt::I32(v) => i128::from(*v) * 100,
            Amt::I64(v) => i128::from(*v) * 100,
            Amt::Dec(t) => cents_of(t),
        }
    }
}

fn cents_of(text: &str) -> i128 {
    let negative = text.starts_with('-');
    let text = text.trim_start_matches('-');
    let (whole, frac) = text.split_once('.').unwrap_or((text, "00"));
    assert_eq!(frac.len(), 2, "two decimals: {text}");
    let v = whole.parse::<i128>().unwrap() * 100 + frac.parse::<i128>().unwrap();
    if negative { -v } else { v }
}

fn cents_text(cents: i128) -> String {
    let sign = if cents < 0 { "-" } else { "" };
    format!("{sign}{}.{:02}", cents.abs() / 100, cents.abs() % 100)
}

/// What R1 says the counter reads: the widest type among the base and the operands, an integer
/// total outside the `i32` range an `Int64`, a decimal total exact to the hundredth.
fn r1_expected(base: &Amt, ops: &[Amt]) -> String {
    let width = ops.iter().map(Amt::width).chain([base.width()]).max().unwrap();
    let total: i128 = ops.iter().map(Amt::cents).chain([base.cents()]).sum();
    match width {
        2 => format!("Decimal128({})", cents_text(total)),
        _ => {
            let whole = total / 100;
            if width == 0 && i32::try_from(whole).is_ok() {
                format!("Int32({whole})")
            } else {
                format!("Int64({whole})")
            }
        }
    }
}

fn rendered(v: &Value) -> String {
    match v {
        Value::Decimal128(_) => format!("Decimal128({})", dec_text(v)),
        v => format!("{v:?}"),
    }
}

/// One case: the base inserted by a first writer, then `ops.len()` writers that each receive the
/// base, some of the writers before them, and add their operands. Joined in several orders.
fn r1_case(
    o: &JoinOptions,
    base: &Amt,
    ops: &[Vec<Amt>],
    sees: &[Vec<usize>],
) -> Result<(), String> {
    let mut root = Writer::new(wid('R', 0), o.clone());
    root.insert(&fl(&[("n", base.value())]), 0).unwrap();
    let mut states: Vec<MergeState> = Vec::new();
    let mut added: Vec<Amt> = Vec::new();
    for (k, mine) in ops.iter().enumerate() {
        let mut w = Writer::new(wid(char::from(b'A' + k as u8), 0), o.clone());
        w.receive(&root.state);
        for &j in &sees[k] {
            w.receive(&states[j]);
        }
        for a in mine {
            if w.write_inc_value(&p("n"), &a.value(), k as u64).is_some() {
                added.push(a.clone());
            }
        }
        states.push(w.state);
    }
    // what the writers saw of each other's operands is part of their states: the expected answer
    // is over every operand any of them added, each once
    let want = r1_expected(base, &added);
    let n = states.len();
    let mut orders: Vec<Vec<usize>> = vec![(0..n).collect(), (0..n).rev().collect()];
    orders.push((0..n).map(|x| (x + n / 2) % n).collect());
    for order in orders {
        let mut acc = root.state.clone();
        for &k in &order {
            acc = join(&acc, &states[k], o).0;
        }
        let got = rendered(&acc.visible(o).unwrap()[&p("n")]);
        if got != want {
            return Err(format!(
                "base {base:?}, ops {ops:?}, sees {sees:?}, order {order:?}: {got}, want {want}"
            ));
        }
    }
    Ok(())
}

/// The cases, fixed ones first (the minimised input, a decimal base, a wider operand, five
/// writers), then random ones with up to five writers, partial receives and decimal operands. Each case
/// is joined three ways, all of them LEFT FOLDS of the writers' states into the root's: in ascending
/// order, in descending order, and in the ascending order rotated by half (`n / 2`); the states are
/// not joined as a tree and not in every permutation.
fn r1_check(o: &JoinOptions) -> Result<usize, String> {
    use Amt::{Dec, I32, I64};
    let fixed: Vec<(Amt, Vec<Vec<Amt>>)> = vec![
        (I32(i32::MIN), vec![vec![I32(7), I32(i32::MAX)]]),
        (I32(i32::MIN), vec![vec![I32(7)], vec![I32(i32::MAX)]]),
        (Dec("1.50"), vec![vec![I32(2)], vec![I32(3)]]),
        (I32(5), vec![vec![I64(1)], vec![I32(1)]]),
        (
            I64(5),
            vec![vec![I32(1), I32(-1)], vec![I32(2)], vec![I32(3)], vec![I32(4)], vec![I32(5)]],
        ),
        (I32(0), vec![vec![Dec("0.25")], vec![I32(i32::MAX)], vec![I32(i32::MAX)]]),
    ];
    let mut cases = 0;
    for (base, ops) in &fixed {
        let sees = vec![Vec::new(); ops.len()];
        r1_case(o, base, ops, &sees)?;
        cases += 1;
    }
    let bases = [I32(i32::MIN), I32(5), I64(5), I32(i32::MAX), I32(0), Dec("1.50"), Dec("-2.00")];
    let operands = [
        I32(7),
        I32(i32::MAX),
        I32(i32::MIN),
        I32(-5),
        I32(0),
        I32(1_000_000_000),
        I32(2_000_000_000),
        I64(3),
        I64(-4_000_000_000),
        I64(1 << 40),
        Dec("0.25"),
        Dec("-2.00"),
        Dec("10.75"),
    ];
    let mut seed = 11u64;
    for _ in 0..300 {
        let base = bases[(lcg(&mut seed) % bases.len() as u64) as usize].clone();
        let writers = 2 + (lcg(&mut seed) % 4) as usize; // 2 to 5
        let mut ops = Vec::new();
        let mut sees = Vec::new();
        for k in 0..writers {
            let mine: Vec<Amt> = (0..(1 + lcg(&mut seed) % 3))
                .map(|_| operands[(lcg(&mut seed) % operands.len() as u64) as usize].clone())
                .collect();
            ops.push(mine);
            sees.push((0..k).filter(|_| lcg(&mut seed).is_multiple_of(3)).collect());
        }
        r1_case(o, &base, &ops, &sees)?;
        cases += 1;
    }
    Ok(cases)
}

/// R1 holds for the three left folds of `r1_check` (ascending, descending, rotated by half), with
/// partial receives, decimal operands and up to five writers: the answer is the expected one, not
/// only the same one.
#[test]
fn the_counter_is_the_base_and_every_operand_whatever_the_joins_and_the_receives() {
    let cases = r1_check(&opts()).unwrap();
    assert!(cases >= 300, "{cases}");
}

/// The mutant that does not count the base's own label is killed by an observable value: an
/// `Int64` base with `Int32` operands reads as an `Int32`.
#[test]
fn a_width_that_ignores_the_base_is_killed() {
    let mut o = opts();
    o.set_counter_ignores_base(true);
    assert!(r1_check(&o).is_err());
}

/// The mutant that reads the width from the value of each part (the sticky rule R1 replaced) is
/// killed by the minimised input: `Int32` minimum, then `7` and `2147483647` is an `Int32` 6.
#[test]
fn a_width_read_from_the_value_of_each_part_is_killed() {
    let mut o = opts();
    o.set_counter_kind_by_value(true);
    let err = r1_check(&o).unwrap_err();
    assert!(err.contains("Int32"), "{err}");
}

/// Review item 1: a slot's type label does not survive a base write that absorbed it. `{n: Int32 1}`,
/// `$inc n Decimal 1.5`, `$set n Int32 5`, `$inc n Int32 1` reads `Int32 6`, as the reference does
/// (likewise with an `Int64` operand): the slot keeps the stamps of its latest wide and decimal
/// operands, and the type of what it gained since the absorb is the width of the operands newer than it.
#[test]
fn a_base_write_that_absorbed_a_wider_operand_leaves_its_type_behind() {
    for wide in [dec("1.5"), i64v(1)] {
        let mut w = base(i32v(1));
        w.write_inc_value(&p("n"), &wide, 0).unwrap();
        w.write_set(&p("n"), i32v(5), 0).unwrap();
        w.write_inc_value(&p("n"), &i32v(1), 0).unwrap();
        assert_eq!(n(&w), i32v(6), "after {wide:?}");
    }
}

/// The absorb case as a property, the review's counterexample over many shapes: A adds `first`, W
/// receives A and `$set`s a numeric base (the fold absorbs A's slot), W adds `w_ops`, and A, unaware
/// of W, adds `second`. Joined either way, the counter is the base W wrote plus the operands NOT
/// absorbed, with the width of those operands and of the base.
fn r1_absorb_case(
    o: &JoinOptions,
    first: &[Amt],
    base_after: &Amt,
    second: &[Amt],
    w_ops: &[Amt],
) -> Result<bool, String> {
    let mut root = Writer::new(wid('R', 0), o.clone());
    root.insert(&fl(&[("n", i32v(1))]), 0).unwrap();
    let mut a = Writer::new(wid('A', 0), o.clone());
    a.receive(&root.state);
    for x in first {
        let _ = a.write_inc_value(&p("n"), &x.value(), 0);
    }
    let mut w = Writer::new(wid('W', 0), o.clone());
    w.receive(&a.state);
    // a `$set` of the value the counter already reads is a no-op and absorbs nothing: the case is skipped
    if w.visible().unwrap()[&p("n")] == base_after.value() {
        return Ok(false);
    }
    w.write_set(&p("n"), base_after.value(), 0).unwrap();
    let mut added: Vec<Amt> = Vec::new();
    for x in w_ops {
        if w.write_inc_value(&p("n"), &x.value(), 0).is_some() {
            added.push(x.clone());
        }
    }
    for x in second {
        if a.write_inc_value(&p("n"), &x.value(), 1).is_some() {
            added.push(x.clone());
        }
    }
    let want = r1_expected(base_after, &added);
    for (x, y) in [(&a.state, &w.state), (&w.state, &a.state)] {
        let got = rendered(&join(x, y, o).0.visible(o).unwrap()[&p("n")]);
        if got != want {
            return Err(format!(
                "first {first:?}, base {base_after:?}, W {w_ops:?}, A after {second:?}: {got}, want {want}"
            ));
        }
    }
    Ok(true)
}

/// What `r1_absorb_check` ran: the cases that ran, the cases SKIPPED (a no-op `$set` absorbs nothing), and the
/// cases where an absorbed operand was WIDER than everything that counts (the base and the operands after it),
/// so the type must narrow back: the ones the property is about.
#[derive(Debug)]
struct AbsorbRun {
    ran: usize,
    skipped: usize,
    narrowing: usize,
}

fn r1_absorb_check(o: &JoinOptions) -> Result<AbsorbRun, String> {
    use Amt::{Dec, I32, I64};
    let mut run = AbsorbRun { ran: 0, skipped: 0, narrowing: 0 };
    let mut one =
        |first: &[Amt], base: &Amt, second: &[Amt], w_ops: &[Amt]| -> Result<(), String> {
            if r1_absorb_case(o, first, base, second, w_ops)? {
                run.ran += 1;
                let width = |xs: &[Amt]| xs.iter().map(Amt::width).max().unwrap_or(0);
                let rest = width(second).max(width(w_ops)).max(base.width());
                run.narrowing += usize::from(width(first) > rest);
            } else {
                run.skipped += 1;
            }
            Ok(())
        };
    // the review's counterexample and its Int64 form
    one(&[Dec("1.50")], &I32(5), &[I32(1)], &[])?;
    one(&[I64(1)], &I32(5), &[I32(1)], &[])?;
    let bases = [I32(5), I32(i32::MAX), I64(7), Dec("2.00"), Dec("-0.50")];
    let operands = [
        I32(1),
        I32(-3),
        I32(i32::MAX),
        I64(2),
        I64(-(1 << 33)),
        Dec("1.50"),
        Dec("-0.25"),
        Dec("2.00"),
    ];
    let mut seed = 23u64;
    let pick = |xs: &[Amt], seed: &mut u64| xs[(lcg(seed) % xs.len() as u64) as usize].clone();
    for _ in 0..300 {
        let first: Vec<Amt> =
            (0..1 + lcg(&mut seed) % 2).map(|_| pick(&operands, &mut seed)).collect();
        let second: Vec<Amt> =
            (0..lcg(&mut seed) % 3).map(|_| pick(&operands, &mut seed)).collect();
        let w_ops: Vec<Amt> = (0..lcg(&mut seed) % 2).map(|_| pick(&operands, &mut seed)).collect();
        let base = pick(&bases, &mut seed);
        one(&first, &base, &second, &w_ops)?;
    }
    Ok(run)
}

/// R1 with base writes: the counter is the current base plus the operands it has not absorbed, in
/// the width of both, whichever way the two states are joined.
#[test]
fn a_base_write_leaves_only_the_operands_it_did_not_absorb_in_the_type() {
    let run = r1_absorb_check(&opts()).unwrap();
    eprintln!(
        "absorb property: {} ran, {} skipped, {} narrowing",
        run.ran, run.skipped, run.narrowing
    );
    // NON-VACUITY: of the 302 cases, the ones that ran are most of them, and in at least 40 an absorbed
    // operand was wider than the base and every operand after it (the type must narrow back). N = 40 is
    // set before the first run (the pool makes about a fifth of the cases such); the run prints the real
    // figures and N is raised to what it shows.
    assert!(run.ran >= 200, "{} ran, {} skipped", run.ran, run.skipped);
    assert!(run.narrowing >= 40, "{} narrowing cases", run.narrowing);
}

/// The mutant that keeps the width of the absorbed operands (the label of a slot is the join of every
/// operand it ever took) is killed by an observable value: the counterexample reads `Decimal128`.
#[test]
fn a_label_that_survives_the_absorb_is_killed() {
    let mut o = opts();
    o.set_counter_label_survives_absorb(true);
    let err = r1_absorb_check(&o).unwrap_err();
    assert!(err.contains("Int32(6)"), "{err}");
}

/// An integral Decimal128 gain after the absorb reads a Decimal128: `{n: Int32 1}`, `$inc` Decimal 1.5,
/// `$set` Int32 5, `$inc` Decimal 2.00 is a Decimal 7.00, not an integer. A slot whose latest
/// decimal operand did not move past the absorb (the mutant that keeps the FIRST stamp of `decimal_at`) reads
/// the gain as an integer, which this kills.
#[test]
fn an_integral_decimal_gain_after_the_absorb_reads_a_decimal() {
    let mut w = base(i32v(1));
    w.write_inc_value(&p("n"), &dec("1.5"), 0).unwrap();
    w.write_set(&p("n"), i32v(5), 0).unwrap();
    w.write_inc_value(&p("n"), &dec("2.00"), 0).unwrap();
    assert_eq!(n(&w), dec("7.00"));
    assert_eq!(dec_text(&n(&w)), "7.00");
}

/// Fixed cases of the absorb. A base write that LOSES to a concurrent base still absorbed the slots it
/// held: the pair is the maximum over all the base writes, so what the winner did not absorb is what
/// came after the latest of them, and the type is the base's and the width of those operands.
#[test]
fn an_absorbing_base_that_loses_to_a_concurrent_base_still_absorbs() {
    let o = opts();
    let root = base(i32v(1));
    let (mut a, mut w, mut b) = (
        Writer::new(wid('A', 0), o.clone()),
        Writer::new(wid('W', 0), o.clone()),
        Writer::new(wid('B', 0), o.clone()),
    );
    a.receive(&root.state);
    b.receive(&root.state);
    a.write_inc_value(&p("n"), &i64v(3), 0).unwrap();
    w.receive(&a.state);
    w.write_set(&p("n"), i32v(10), 0).unwrap(); // absorbs A's Int64 operand
    b.write_set(&p("n"), i32v(20), 50).unwrap(); // a concurrent base, newer: it wins
    for (x, y) in [(&w.state, &b.state), (&b.state, &w.state)] {
        let all = join(&join(&a.state, x, &o).0, y, &o).0;
        assert_eq!(all.visible(&o).unwrap()[&p("n")], i32v(20));
    }
    // when the absorbing base is the one that wins, the same: nothing of A's remains, in the Int32 base's width
    w.write_set(&p("n"), i32v(30), 100).unwrap();
    for (x, y) in [(&w.state, &b.state), (&b.state, &w.state)] {
        let all = join(&join(&a.state, x, &o).0, y, &o).0;
        assert_eq!(all.visible(&o).unwrap()[&p("n")], i32v(30));
    }
}

/// Two bases absorb one slot at different stamps (the second after one more operand): the pair of the
/// slot is the later one, whichever base wins, and the operand between them is not counted twice.
#[test]
fn two_bases_absorbing_one_slot_at_different_stamps() {
    let o = opts();
    let root = base(i32v(1));
    let (mut a, mut w1, mut w2) = (
        Writer::new(wid('A', 0), o.clone()),
        Writer::new(wid('B', 0), o.clone()),
        Writer::new(wid('C', 0), o.clone()),
    );
    a.receive(&root.state);
    a.write_inc_value(&p("n"), &i32v(1), 0).unwrap();
    w1.receive(&a.state);
    w1.write_set(&p("n"), i32v(10), 50).unwrap(); // absorbs the first operand; the newer base
    a.write_inc_value(&p("n"), &i32v(2), 0).unwrap();
    w2.receive(&a.state);
    w2.write_set(&p("n"), i32v(100), 0).unwrap(); // absorbs both
    for (x, y) in [(&w1.state, &w2.state), (&w2.state, &w1.state)] {
        let all = join(&join(x, y, &o).0, &a.state, &o).0;
        assert_eq!(all.visible(&o).unwrap()[&p("n")], i32v(10));
    }
}

/// An older-generation slot with an operand no base absorbed does not leak its width into the type: the
/// string base starts a generation, a numeric base after it carries that generation, and an `Int64`
/// operand of a writer that never saw the string is not counted, so the counter stays an `Int32`.
#[test]
fn an_older_generation_slot_with_an_unabsorbed_int64_operand_does_not_leak_into_the_type() {
    let o = opts();
    let root = base(i32v(1));
    let (mut w, mut b) = (Writer::new(wid('W', 0), o.clone()), Writer::new(wid('B', 0), o.clone()));
    w.receive(&root.state);
    b.receive(&root.state);
    b.write_inc_value(&p("n"), &i64v(7), 1).unwrap();
    w.write_set(&p("n"), Value::String("t".into()), 0).unwrap();
    w.write_set(&p("n"), i32v(100), 0).unwrap();
    for (x, y) in [(&w.state, &b.state), (&b.state, &w.state)] {
        let all = join(x, y, &o).0;
        assert_eq!(all.visible(&o).unwrap()[&p("n")], i32v(100));
    }
}

/// Amendment 9, ruling 2: a Decimal128 increment is refused (`IncOutOfRange`) when the writer's resulting SLOT
/// total is not exactly representable in 34 digits, as it is when the sum overflows. `{n: Int32 0}`, `$inc` by
/// 1E+33, `$set` Int32 0, `$inc` by 0.5: the slot total 1E+33 + 0.5 has 35 digits and was rounded at the write,
/// so the counter read a Decimal 0 where the true answer is 0.5. An increment that fits (1E+33 + 1) is accepted.
#[test]
fn a_decimal_increment_whose_slot_total_is_not_exact_is_refused() {
    let mut w = base(i32v(0));
    w.write_inc_value(&p("n"), &dec("1E+33"), 0).unwrap();
    w.write_set(&p("n"), i32v(0), 0).unwrap();
    let before = (w.state.clone(), w.clock);
    assert_eq!(inc(&mut w, dec("0.5")), Err(UpdateRefused::IncOutOfRange(p("n"))));
    assert_eq!((w.state.clone(), w.clock), before, "nothing moved");
    assert!(inc(&mut w, dec("1")).unwrap().is_some());
    assert_eq!(dec_text(&n(&w)), "1");
}

/// What the same increment gives where the writer's slot is not the one holding 1E+33: in a new generation (a
/// non-numeric base started it, so the writer has no slot in it yet) and on another writer (its own slot, which
/// holds the operand alone). Both are accepted and read the reference's 0.5: the slot total is the operand.
#[test]
fn the_same_increment_is_accepted_in_a_new_generation_and_by_another_writer() {
    let mut w = base(i32v(0));
    w.write_inc_value(&p("n"), &dec("1E+33"), 0).unwrap();
    w.write_set(&p("n"), Value::String("t".into()), 0).unwrap();
    w.write_set(&p("n"), i32v(0), 0).unwrap();
    assert!(inc(&mut w, dec("0.5")).unwrap().is_some());
    assert_eq!(dec_text(&n(&w)), "0.5");
    let mut a = base(i32v(0));
    a.write_inc_value(&p("n"), &dec("1E+33"), 0).unwrap();
    a.write_set(&p("n"), i32v(0), 0).unwrap();
    let mut b = Writer::new(wid('B', 0), opts());
    b.receive(&a.state);
    assert!(inc(&mut b, dec("0.5")).unwrap().is_some());
    assert_eq!(dec_text(&n(&b)), "0.5");
}

/// The recorded Decimal128 cases past 34 digits (`q7s2.out` lines 326-345), decided by NUMERIC exactness: a sum is
/// accepted when its exact value is a Decimal128 at some exponent, refused when it would round or overflow.
/// x1 (326-330) 34 nines + Int32 1 and x3 (336-340) + Decimal 1 are exactly 10^34, which the reference stores as
/// `1.000000000000000000000000000000000E+34`: accepted, and read as that value (the text is what this build
/// stores; a different cohort than the recorded one would be a difference of representation). x2 (331-335)
/// 34 nines + 0.5 rounds (the true sum is 10^34 - 0.5): refused. x4 (341-345) the largest decimal added to
/// itself is `Infinity` there: refused. Two halves of 10^34 are accepted.
#[test]
fn the_recorded_decimal_sums_past_34_digits_are_decided_by_numeric_exactness() {
    let nines = "9999999999999999999999999999999999";
    let max = "9.999999999999999999999999999999999E+6144";
    let ten_34 = "1.000000000000000000000000000000000E+34";
    let refused = Err(UpdateRefused::IncOutOfRange(p("n")));
    for (what, by, accepted) in [
        ("x1 (326-330): + Int32 1", i32v(1), true),
        ("x2 (331-335): + 0.5", dec("0.5"), false),
        ("x3 (336-340): + Decimal 1", dec("1"), true),
    ] {
        let mut w = base(dec(nines));
        let before = (w.state.clone(), w.clock);
        if accepted {
            assert!(inc(&mut w, by).unwrap().is_some(), "{what}");
            assert_eq!(dec_text(&n(&w)), ten_34, "{what}");
        } else {
            assert_eq!(inc(&mut w, by), refused, "{what}");
            assert_eq!((w.state.clone(), w.clock), before, "{what}: nothing moved");
        }
    }
    let mut w = base(dec(max));
    assert_eq!(inc(&mut w, dec(max)), refused, "x4 (341-345): overflow");
    // two 34-digit halves of 10^34 are exactly 10^34
    let mut w = base(dec("5000000000000000000000000000000000"));
    assert!(inc(&mut w, dec("5000000000000000000000000000000000")).unwrap().is_some());
    assert_eq!(dec_text(&n(&w)), ten_34);
    // sums that fit without a rise of the exponent
    let mut w = base(dec(nines));
    assert!(inc(&mut w, dec("0")).unwrap().is_some());
    assert!(inc(&mut w, dec("-1")).unwrap().is_some());
    assert_eq!(dec_text(&n(&w)), "9999999999999999999999999999999998");
}

/// An ordinary money-like history is never refused: three writers add and subtract two-decimal amounts, `$set`
/// a price now and then and receive each other's states, 300 steps; and the counters converge.
#[test]
fn an_ordinary_money_like_history_is_never_refused() {
    let o = opts();
    let mut root = Writer::new(wid('R', 0), o.clone());
    root.insert(&fl(&[("n", dec("100.00"))]), 0).unwrap();
    let mut ws: Vec<Writer> = ['A', 'B', 'C']
        .into_iter()
        .map(|c| {
            let mut w = Writer::new(wid(c, 0), o.clone());
            w.receive(&root.state);
            w
        })
        .collect();
    let mut seed = 5u64;
    for step in 0..300 {
        let k = (lcg(&mut seed) % 3) as usize;
        let amount = format!("{}.{:02}", lcg(&mut seed) % 5000, lcg(&mut seed) % 100);
        match lcg(&mut seed) % 10 {
            0 => {
                let _ = ws[k].write_set(&p("n"), dec(&amount), 0);
            }
            1 | 2 => {
                let j = (lcg(&mut seed) % 3) as usize;
                let theirs = ws[j].state.clone();
                ws[k].receive(&theirs);
            }
            r => {
                let by = dec(&format!("{}{amount}", if r % 2 == 0 { "-" } else { "" }));
                assert!(matches!(inc(&mut ws[k], by), Ok(Some(_))), "step {step}: refused");
            }
        }
    }
    let all = |order: [usize; 3]| {
        let mut acc = ws[order[0]].state.clone();
        for &i in &order[1..] {
            acc = join(&acc, &ws[i].state, &o).0;
        }
        acc.visible(&o).unwrap()[&p("n")].clone()
    };
    assert_eq!(all([0, 1, 2]), all([2, 1, 0]));
}
