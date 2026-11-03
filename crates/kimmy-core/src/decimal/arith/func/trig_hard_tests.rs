//! Direct tests of the pieces of `trig.rs` that only exact values reach: each answer here is
//! held to the digit, against values computed off-line (`tests/data/gen_trig_oracle.py` and
//! `tests/data/gen_trig_hard.py`, Python's `decimal` at 250 digits and no algorithm of this
//! file), so that a slip in a boundary or a width shows as a wrong digit.

use super::super::{LIMB_DIGITS, Mag, SL, m_cmp, m_sub};
use super::{
    Fx, SERIES, Scale, at_least_large, fixed_ratio, ln_fixed, ratio_scale, reduce, result, tangent,
};
use crate::decimal::arith::func;
use crate::decimal::{Raw, decode_raw, parse};
use std::cmp::Ordering;

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

#[test]
fn the_hard_cases_are_held_to_the_digit() {
    // `tests/data/trig_hard.tsv`: the edges of `LARGE`, of the overflow of `sinh` and `cosh`, of
    // `TINY` and `HUGE`, the operands that put the logarithm's argument near one and at its
    // largest, and `atan2` with a ratio 79, 80 and 81 digits down and far below (for a negative
    // `x` as well), among them ratios that are exactly midpoints of the 34-digit grid.
    let table = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/trig_hard.tsv"));
    let (wrong, count) = disagreements(table);
    assert!(wrong.is_empty(), "{} of {count}:\n{}", wrong.len(), wrong.join("\n"));
    assert!(count >= 250, "{count}");
}

/// The limbs of an integer given in decimal digits.
fn mag(digits: &str) -> Mag {
    let digits: Vec<u8> = digits.bytes().collect();
    assert!(digits.iter().all(u8::is_ascii_digit), "{digits:?}");
    let mut limbs = Vec::new();
    let mut end = digits.len();
    while end > 0 {
        let start = end.saturating_sub(LIMB_DIGITS as usize);
        limbs.push(digits[start..end].iter().fold(0u64, |n, d| n * 10 + u64::from(d - b'0')));
        end = start;
    }
    let mut m = Mag(limbs);
    m.trim();
    m
}

/// `|a - b|` as an integer.
fn distance(a: &Mag, b: &Mag) -> u128 {
    let d = if m_cmp(a, b) == Ordering::Less { m_sub(b, a) } else { m_sub(a, b) };
    d.to_u128().expect("a distance below 2^128")
}

fn kernel_rows(kind: &str) -> Vec<Vec<&'static str>> {
    let table = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/trig_kernels.tsv"));
    table
        .lines()
        .map(|l| l.split('\t').collect::<Vec<_>>())
        .filter(|cols| cols[0] == kind)
        .collect()
}

#[test]
fn ln_fixed_is_the_logarithm_to_within_a_few_units_of_the_last_place() {
    // `ln(N / 10^114)` for `N` from a hair above `10^114` (one) to a number of 40 digits, and for
    // the values below one that no caller passes: the value is the truth truncated at 114 places
    // (Python's `decimal` at 400 digits), and the answer is within `TOLERANCE` units of it (the
    // 40-digit values are the worst, at 143: a multiple of `ln 2` for each halving; the budget
    // of `ln_fx` is a few hundred).
    const TOLERANCE: u128 = 200;
    let rows = kernel_rows("ln_fixed");
    assert_eq!(rows.len(), 29);
    for cols in rows {
        let (negative, digits) = match cols[2].strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, cols[2]),
        };
        let want = mag(digits);
        let got = ln_fixed(&mag(cols[1]));
        let off = distance(&got.mag, &want);
        assert!(off <= TOLERANCE, "ln({}): {off} units off", cols[1]);
        assert!(
            got.neg == negative || got.mag.is_zero() || want.is_zero(),
            "sign of ln({})",
            cols[1]
        );
    }
}

#[test]
fn the_ratio_of_atan2_is_the_quotient_floored_at_the_scale() {
    // `⌊|y / x| · 10^(19 limbs)⌋` for a numerator that is shifted up (the scale is wider than the
    // gap between the exponents), left alone, and cut down to a few of its digits.
    let rows = kernel_rows("fixed_ratio");
    assert_eq!(rows.len(), 12);
    for cols in rows {
        let limbs: usize = cols[1].parse().unwrap();
        let cy: u128 = cols[2].parse().unwrap();
        let ey: i64 = cols[3].parse().unwrap();
        let cx: u128 = cols[4].parse().unwrap();
        let ex: i64 = cols[5].parse().unwrap();
        let got = fixed_ratio(Scale { limbs }, (cy, ey), (cx, ex));
        assert_eq!(got, mag(cols[6]), "{cy}E{ey} / {cx}E{ex} at {limbs} limbs");
    }
}

#[test]
fn the_reduction_is_the_remainder_at_133_places_and_the_quadrant() {
    // `SERIES` is a limb wider than the standard scale: the remainder of a large operand is cut to
    // it, and a small operand is reduced at it. The quadrant is exact; the remainder is the
    // true one (Python's `decimal`, pi to 6,500 places) to the `10^-115` the reduction keeps.
    let rows = kernel_rows("reduce");
    assert_eq!(rows.len(), 11);
    assert_eq!(SERIES.limbs, SL + 1);
    for cols in rows {
        let coefficient: u128 = cols[1].parse().unwrap();
        let exponent: i64 = cols[2].parse().unwrap();
        let adjusted = i64::from(coefficient.ilog10()) + exponent;
        let (scale, quadrant, remainder) = reduce(coefficient, exponent, adjusted);
        assert_eq!(scale.limbs, 7, "{coefficient}E{exponent}");
        assert_eq!(
            u128::from(quadrant),
            cols[3].parse::<u128>().unwrap(),
            "{coefficient}E{exponent}"
        );
        let off = distance(&remainder, &mag(cols[4]));
        // `10^-115` is `10^18` units of the 133rd place
        assert!(off <= 2_000_000_000_000_000_000, "{coefficient}E{exponent}: {off} units");
    }
}

#[test]
fn a_result_is_cut_to_34_digits_from_a_magnitude_of_any_length() {
    let fin = |negative, coefficient, exponent| Raw::Finite { negative, coefficient, exponent };
    let zero = |negative| fin(negative, 0, 0);
    // nothing left is a zero of the sign
    assert_eq!(result(false, Mag::zero(), 133), zero(false));
    assert_eq!(result(true, Mag::zero(), 5), zero(true));
    // five digits (padded to 40, then cut): 12345e-20, the zeros of the coefficient kept
    assert_eq!(
        result(false, Mag::from_u64(12345), 20),
        fin(false, 1_234_500_000_000_000_000_000_000_000_000_000, -49)
    );
    assert_eq!(
        result(true, Mag::from_u64(12345), 20),
        fin(true, 1_234_500_000_000_000_000_000_000_000_000_000, -49)
    );
    // 34 digits exactly, 35 and 39: padded by 6, 5 and 1
    let digits34 = mag("1234567890123456789012345678901234");
    assert_eq!(
        result(false, digits34, 34),
        fin(false, 1_234_567_890_123_456_789_012_345_678_901_234, -34)
    );
    let tie = mag("12345678901234567890123456789012345");
    assert_eq!(
        result(false, tie, 40),
        // the 5 is a tie, and a sticky one is above it: up, not to the even 4
        fin(false, 1_234_567_890_123_456_789_012_345_678_901_235, -39)
    );
    let digits39 = mag("123456789012345678901234567890123456789");
    assert_eq!(
        result(false, digits39, 39),
        fin(false, 1_234_567_890_123_456_789_012_345_678_901_235, -34)
    );
    // 40 digits (no padding) and more
    let digits40 = mag("1234567890123456789012345678901234567890");
    assert_eq!(
        result(false, digits40, 40),
        fin(false, 1_234_567_890_123_456_789_012_345_678_901_235, -34)
    );
    // an exact one is spelled `1`, just under it is not
    let one = Mag::from_u64(1);
    let mut ten20 = one;
    ten20.mul_pow10(20);
    assert_eq!(result(false, ten20, 20), fin(false, 1, 0));
    assert_eq!(
        result(false, mag("99999999999999999999"), 20),
        fin(false, 9_999_999_999_999_999_999_900_000_000_000_000, -34)
    );
}

#[test]
fn a_tangent_is_the_ratio_with_the_sign_of_the_quadrant_and_the_operand() {
    let s = Scale::standard();
    let one = || Mag::from_u64(1).tap(|m| m.mul_pow10(114));
    let fin = |negative, coefficient, exponent| Raw::Finite { negative, coefficient, exponent };
    for (negative, sin_neg, cos_neg) in [
        (false, false, false),
        (false, true, false),
        (false, false, true),
        (false, true, true),
        (true, false, false),
        (true, true, false),
        (true, false, true),
        (true, true, true),
    ] {
        let got = tangent(negative, s, &Fx::new(sin_neg, one()), &Fx::new(cos_neg, one()));
        assert_eq!(
            got,
            fin(negative != (sin_neg != cos_neg), 1, 0),
            "{negative} {sin_neg} {cos_neg}"
        );
    }
    // a cosine with no digit left at the scale is an infinity, of the sign of sine times operand
    for (negative, sin_neg) in [(false, false), (false, true), (true, false), (true, true)] {
        let got = tangent(negative, s, &Fx::new(sin_neg, one()), &Fx::new(false, Mag::zero()));
        assert_eq!(got, Raw::Inf { negative: negative != sin_neg }, "{negative} {sin_neg}");
    }
}

trait Tap: Sized {
    fn tap(self, f: impl FnOnce(&mut Self)) -> Self;
}

impl Tap for Mag {
    fn tap(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self
    }
}

#[test]
fn large_starts_at_50_whatever_the_digits_of_the_operand() {
    // (coefficient, exponent), the adjusted exponent worked out here
    for (coefficient, exponent, adjusted, large) in [
        (1u128, -5i64, -5i64, false),
        (99, -1, 0, false),
        (5, 0, 0, false),
        (1, 1, 1, false),
        (49, 0, 1, false),
        (4_999_999_999_999_999_999_999_999_999_999_999, -32, 1, false),
        (5, 1, 1, true),
        (50, 0, 1, true),
        (5_000_000_000_000_000_000_000_000_000_000_001, -32, 1, true),
        (6, 1, 1, true),
        (99, 0, 1, true),
        (1, 2, 2, true),
        (1, 4, 4, true),
    ] {
        assert_eq!(
            at_least_large(coefficient, exponent, adjusted),
            large,
            "{coefficient}E{exponent}"
        );
    }
}

#[test]
fn atan2_answers_the_exact_points_as_the_reference_does() {
    let pi = Raw::Finite {
        negative: false,
        coefficient: 3_141_592_653_589_793_238_462_643_383_279_503,
        exponent: -33,
    };
    let negative_pi = Raw::Finite {
        negative: true,
        coefficient: 3_141_592_653_589_793_238_462_643_383_279_503,
        exponent: -33,
    };
    let zero = |negative| Raw::Finite { negative, coefficient: 0, exponent: -6176 };
    // y zero: pi for a negative x, a zero for a positive one, with the sign of y
    assert_eq!(func::atan2(&raw("0"), &raw("-1")), pi);
    assert_eq!(func::atan2(&raw("-0"), &raw("-1")), negative_pi);
    assert_eq!(func::atan2(&raw("0"), &raw("1")), zero(false));
    assert_eq!(func::atan2(&raw("-0"), &raw("1")), zero(true));
    assert_eq!(func::atan2(&raw("0"), &raw("-1E-6000")), pi);
    // an infinity over an infinity: pi/4 with 33 digits, 3 pi/4 with 34
    let quarter = |negative| Raw::Finite {
        negative,
        coefficient: 785_398_163_397_448_309_615_660_845_819_876,
        exponent: -33,
    };
    let three_quarters = |negative| Raw::Finite {
        negative,
        coefficient: 2_356_194_490_192_344_928_846_982_537_459_627,
        exponent: -33,
    };
    assert_eq!(func::atan2(&raw("Infinity"), &raw("Infinity")), quarter(false));
    assert_eq!(func::atan2(&raw("-Infinity"), &raw("Infinity")), quarter(true));
    assert_eq!(func::atan2(&raw("Infinity"), &raw("-Infinity")), three_quarters(false));
    assert_eq!(func::atan2(&raw("-Infinity"), &raw("-Infinity")), three_quarters(true));
    // a ratio far below the width of any scale is itself, at the digits of its operands
    for (y, x, want) in [
        ("1E-6000", "1", "1E-6000"),
        ("-1E-6000", "1", "-1E-6000"),
        ("3E-6100", "7E+40", "4.285714285714285714285714285714286E-6141"),
        ("1E-300", "1E+300", "1E-600"),
    ] {
        let got = func::atan2(&raw(y), &raw(x));
        assert_eq!(value(&got), value(&raw(want)), "atan2({y}, {x})");
    }
}

#[test]
fn the_scale_of_atan2_follows_the_gap_between_the_operands_to_80_digits() {
    // `reaching(gap)`: 6 limbs and one for each 19 digits of the gap and one more
    for (ax, ay, limbs) in [
        (0, 0, 7),
        (60, 50, 7),     // gap 10
        (-20, -70, 9),   // 50: 6 + ceil(51 / 19)
        (-21, -100, 11), // 79
        (-20, -100, 11), // 80
        (-20, -101, 11), // 81, capped at 80
        (-20, -5000, 11),
        (50, 60, 7), // y larger: the standard scale
        (-100, 100, 7),
    ] {
        assert_eq!(ratio_scale(ax, ay).limbs, limbs, "ax {ax}, ay {ay}");
    }
}
