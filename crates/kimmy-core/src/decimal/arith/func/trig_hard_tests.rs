//! Direct tests of the pieces of `trig.rs` that only exact values reach: each answer here is
//! held to the digit, against values computed off-line (`tests/data/gen_trig_oracle.py` and
//! `tests/data/gen_trig_hard.py`, Python's `decimal` at 250 digits and no algorithm of this
//! file), so that a slip in a boundary or a width shows as a wrong digit.

use crate::decimal::arith::func;
use crate::decimal::{Raw, decode_raw, parse};

fn raw(text: &str) -> Raw {
    decode_raw(&parse(text).unwrap_or_else(|e| panic!("{text}: {e}")))
}

/// The value of a finite answer with its trailing zeros off: equal values are equal tuples.
fn value(answer: &Raw) -> (bool, u128, i64) {
    match *answer {
        Raw::Finite { coefficient: 0, .. } => (false, 0, 0),
        Raw::Finite { negative, mut coefficient, exponent } => {
            let mut exponent = i64::from(exponent);
            while coefficient % 10 == 0 {
                coefficient /= 10;
                exponent += 1;
            }
            (negative, coefficient, exponent)
        }
        other => panic!("not a finite answer: {other:?}"),
    }
}

fn call(name: &str, operand: &str) -> Result<Raw, func::Domain> {
    if name == "atan2" {
        let (y, x) = operand.split_once(' ').expect("y and x");
        return Ok(func::atan2(&raw(y), &raw(x)));
    }
    let x = raw(operand);
    match name {
        "sin" => func::sin(&x),
        "cos" => func::cos(&x),
        "tan" => func::tan(&x),
        "asin" => func::asin(&x),
        "acos" => func::acos(&x),
        "atan" => func::atan(&x),
        "sinh" => func::sinh(&x),
        "cosh" => func::cosh(&x),
        "tanh" => func::tanh(&x),
        "asinh" => func::asinh(&x),
        "acosh" => func::acosh(&x),
        "atanh" => func::atanh(&x),
        other => panic!("{other}"),
    }
}

/// The rows of a table (`function`, `operand`, `answer`, tab-separated) that disagree with the
/// functions, and how many rows there were.
fn disagreements(table: &str) -> (Vec<String>, usize) {
    let mut wrong = Vec::new();
    let mut count = 0;
    for line in table.lines() {
        let mut cols = line.split('\t');
        let (name, operand, want) =
            (cols.next().unwrap(), cols.next().unwrap(), cols.next().unwrap());
        count += 1;
        if want.ends_with("Infinity") {
            let negative = want.starts_with('-');
            match call(name, operand) {
                Ok(Raw::Inf { negative: n }) if n == negative => {}
                other => wrong.push(format!("{name}({operand}): want {want}, got {other:?}")),
            }
            continue;
        }
        match call(name, operand) {
            Ok(got) if matches!(got, Raw::Finite { .. }) && value(&got) == value(&raw(want)) => {}
            other => wrong.push(format!("{name}({operand}): want {want}, got {other:?}")),
        }
    }
    (wrong, count)
}

#[test]
fn the_oracle_table_is_held_to_the_digit_by_the_unit_tests_too() {
    // The integration test `decimal_trig_oracle` holds the same rows; it is repeated here so
    // that the library's own tests (the ones a mutation run of the crate executes) hold them.
    let table = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/trig_oracle.tsv"));
    let (wrong, count) = disagreements(table);
    assert!(wrong.is_empty(), "{} of {count}:\n{}", wrong.len(), wrong.join("\n"));
    assert!(count >= 300, "{count}");
}
