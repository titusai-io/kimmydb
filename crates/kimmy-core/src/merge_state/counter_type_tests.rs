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
