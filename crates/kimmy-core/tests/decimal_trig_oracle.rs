//! The `Decimal128` trigonometric and hyperbolic functions against the **true** values,
//! correctly rounded to 34 digits (review q8d, m4): `data/trig_oracle.tsv`, 351 rows over
//! every function, `$atan2` included, at operands from `1E-100` to `1E+6144` (the sines and
//! tangents of `1E+6000` among them), generated off-line at 250 digits by
//! `data/gen_trig_oracle.py` (which cross-checks each by a second route and drops an operand
//! whose true value is within 1e-40 of a rounding midpoint). The comparison is the value to the
//! last digit, whatever the cohort of the answer (`1` and `1.000000000000000000000000000000000`
//! are one value here; `decimal_trig_reference` holds the cohorts to the reference's).
//!
//! The reference's own answers differ from these by a unit in the last place on about one call
//! in seven; this file is what holds the build to the nearest value, which the register and the
//! aggregation reference claim.

use kimmy_core::decimal::arith::func;
use kimmy_core::decimal::{Raw, decode_raw, parse};

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

#[test]
fn every_answer_is_the_true_value_correctly_rounded() {
    let table = include_str!("data/trig_oracle.tsv");
    let mut wrong = Vec::new();
    let mut count = 0;
    let mut names = std::collections::BTreeSet::new();
    for line in table.lines() {
        let mut cols = line.split('\t');
        let (name, operand, want) =
            (cols.next().unwrap(), cols.next().unwrap(), cols.next().unwrap());
        count += 1;
        names.insert(name);
        if want.ends_with("Infinity") {
            // A result past the largest number is an infinity of its sign.
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
    assert!(wrong.is_empty(), "{} of {count}:\n{}", wrong.len(), wrong.join("\n"));
    assert!(count >= 300, "{count}");
    assert_eq!(names.len(), 13, "{names:?}");
}

/// Three calls on which the reference is one unit in the last place from the true value
/// (review q8d, m4). The reference's answers are in the comments; the build's are the nearest.
#[test]
fn the_large_operands_the_reference_rounds_the_other_way_are_the_nearest_value() {
    let pinned = |name: &str, operand: &str, want: &str| {
        let got = call(name, operand).expect("an answer");
        assert_eq!(value(&got), value(&raw(want)), "{name}({operand})");
    };
    // The reference: -0.9280819050746553434561946437769560
    pinned("cos", "1E+100", "-0.9280819050746553434561946437769559");
    // The reference: 1.052408860229401513505574301283601
    pinned("tan", "1E+6000", "1.052408860229401513505574301283600");
    // The reference: 0.4012319619908143541857543436532949
    pinned("tan", "1E+100", "0.4012319619908143541857543436532950");
    // And the one it has right, reduced exactly: 1E+6000 is 6000 digits past the point.
    pinned("sin", "1E+6000", "-0.7249266534376325863453460198136025");
}
