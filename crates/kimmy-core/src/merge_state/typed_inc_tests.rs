//! Typed `$inc` against the recordings of the reference (`q7s2-probes/reference-6.0.5/q7s2.out`,
//! cases d8 to d10, z2, z3, z7 to z9): the result type of an integer sum, the Int64 bound, and a
//! zero increment that changes the type.

use super::tests::{fl, opts, p, wid};
use super::*;

fn with(base: Value) -> Writer {
    let mut w = Writer::new(wid('A', 0), opts());
    w.insert(&fl(&[("n", base), ("x", Value::Int32(0))]), 0).unwrap();
    w
}

fn inc(w: &mut Writer, by: Value) -> Result<Option<Written>, UpdateRefused> {
    w.write_update(&[UpdateOp::Inc(p("n"), by)], 0)
}

fn n(w: &Writer) -> Value {
    w.visible().unwrap()[&p("n")].clone()
}

#[test]
fn int32_plus_int64_is_int64() {
    let mut w = with(Value::Int32(5)); // d8
    assert!(inc(&mut w, Value::Int64(1)).unwrap().is_some());
    assert_eq!(n(&w), Value::Int64(6));
}

#[test]
fn int32_max_plus_one_is_int64() {
    let mut w = with(Value::Int32(i32::MAX)); // d9
    assert!(inc(&mut w, Value::Int32(1)).unwrap().is_some());
    assert_eq!(n(&w), Value::Int64(i64::from(i32::MAX) + 1));
}

#[test]
fn int64_max_plus_one_is_refused_and_changes_nothing() {
    let mut w = with(Value::Int64(i64::MAX)); // d10
    let before = (w.state.clone(), w.clock);
    assert_eq!(inc(&mut w, Value::Int32(1)), Err(UpdateRefused::IncOutOfRange(p("n"))));
    assert_eq!((w.state.clone(), w.clock), before);
    assert_eq!(n(&w), Value::Int64(i64::MAX));
}

#[test]
fn a_zero_int64_increment_of_an_int32_is_a_modification_and_makes_it_int64() {
    let mut w = with(Value::Int32(5)); // z2
    assert!(inc(&mut w, Value::Int64(0)).unwrap().is_some(), "modified: the type changes");
    assert_eq!(n(&w), Value::Int64(5));
}

/// z3: the reference turns an Int32 5 into the double 5.0 on `$inc 0.0`. A double is not a
/// counter here: the writer refuses it (the ruled deviation of `field-merge-refusals-q7`, item 2),
/// the merge model cannot express an integer counter that becomes a register by an increment.
#[test]
fn a_double_increment_of_an_int32_is_refused_here() {
    let mut w = with(Value::Int32(5));
    let before = (w.state.clone(), w.clock);
    assert_eq!(inc(&mut w, Value::Double(0.0)), Err(UpdateRefused::IncDoubleOnCounter(p("n"))));
    assert_eq!((w.state.clone(), w.clock), before);
    assert_eq!(n(&w), Value::Int32(5));
}

#[test]
fn a_zero_increment_of_an_absent_field_creates_it_in_the_operand_type() {
    // z7, z8, z9
    for (by, want) in [
        (Value::Int32(0), Value::Int32(0)),
        (Value::Int64(0), Value::Int64(0)),
        (Value::Double(0.0), Value::Double(0.0)),
    ] {
        let mut w = Writer::new(wid('A', 0), opts());
        w.insert(&fl(&[("x", Value::Int32(0))]), 0).unwrap();
        assert!(inc(&mut w, by.clone()).unwrap().is_some(), "modified: {by:?}");
        assert_eq!(n(&w), want, "{by:?}");
    }
}

/// The same operands through the upsert creation (one write: the life and the slot).
#[test]
fn the_upsert_creation_types_its_increments_as_an_update_does() {
    use crate::effects::{CellTag, CreationCell};
    for (by, want) in [
        (Value::Int32(0), Value::Int32(0)),
        (Value::Int64(0), Value::Int64(0)),
        (Value::Double(0.0), Value::Double(0.0)),
        (Value::Int64(7), Value::Int64(7)),
    ] {
        let mut w = Writer::new(wid('A', 0), opts());
        let list = [CreationCell { path: p("n"), value: by.clone(), tag: CellTag::Inc }];
        w.upsert_create(&list, 0).unwrap();
        assert_eq!(n(&w), want, "{by:?}");
    }
}
