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

/// The recorded cases (`q7s2.out`: d8, d9, z2).
#[test]
fn the_recorded_integer_types() {
    let mut w = base(i32v(5)); // d8: Int32 + Int64 is Int64
    w.write_inc_value(&p("n"), &i64v(1), 0).unwrap();
    assert_eq!(n(&w), i64v(6));
    let mut w = base(i32v(i32::MAX)); // d9: Int32 max + 1 is Int64
    w.write_inc_value(&p("n"), &i32v(1), 0).unwrap();
    assert_eq!(n(&w), i64v(i64::from(i32::MAX) + 1));
    let mut w = base(i32v(5)); // z2: Int32 5 + Int64 0 is 5L, a modification
    assert!(w.write_inc_value(&p("n"), &i64v(0), 0).is_some());
    assert_eq!(n(&w), i64v(5));
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
