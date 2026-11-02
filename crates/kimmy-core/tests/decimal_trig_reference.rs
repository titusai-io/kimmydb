//! The `Decimal128` trigonometric and hyperbolic functions against the reference's answers
//! (the reference, 6.0.5), `data/trig_reference.tsv`: operator, operand and expected answer, one
//! tab-separated row per call, `ERR` for a refusal. 1478 calls: random 34-digit operands at
//! many magnitudes, the exact points, the infinities and the very small and very large.
//!
//! The functions here are correctly rounded to 34 digits; the reference's library is not
//! (it is within a unit in the last place or two, and is off the other way in some
//! cases), so an answer is held to the reference's within three units of its last place,
//! and the exact points to the same digits and cohort. The share that agrees to the last
//! digit is pinned too, so a regression in the kernels shows.

use kimmy_core::decimal::arith::func;
use kimmy_core::decimal::{Raw, decode_raw, pack_raw, parse};

fn run(op: &str, x: &Raw) -> Result<Raw, func::Domain> {
    match op {
        "$sin" => func::sin(x),
        "$cos" => func::cos(x),
        "$tan" => func::tan(x),
        "$asin" => func::asin(x),
        "$acos" => func::acos(x),
        "$atan" => func::atan(x),
        "$sinh" => func::sinh(x),
        "$cosh" => func::cosh(x),
        "$tanh" => func::tanh(x),
        "$asinh" => func::asinh(x),
        "$acosh" => func::acosh(x),
        "$atanh" => func::atanh(x),
        other => panic!("not a function under test: {other}"),
    }
}

/// `raw` with its coefficient widened to 34 digits (the same value, the cohort of 34 digits).
fn widened(raw: &Raw) -> Raw {
    match *raw {
        Raw::Finite { negative, coefficient, exponent } if coefficient != 0 => {
            let pad = 34 - coefficient.ilog10() - 1;
            Raw::Finite {
                negative,
                coefficient: coefficient * 10u128.pow(pad),
                exponent: exponent - pad as i16,
            }
        }
        other => other,
    }
}

/// Whether two answers are within three units of the last place of 34 digits, whatever
/// their cohorts.
fn close(a: &Raw, b: &Raw) -> bool {
    let (
        Raw::Finite { negative: na, coefficient: ca, exponent: ea },
        Raw::Finite { negative: nb, coefficient: cb, exponent: eb },
    ) = (widened(a), widened(b))
    else {
        return a == b;
    };
    if ca == 0 || cb == 0 {
        return ca == cb;
    }
    if na != nb {
        return false;
    }
    let (high, low, shift) = if ea >= eb { (ca, cb, ea - eb) } else { (cb, ca, eb - ea) };
    shift <= 3 && (high * 10u128.pow(shift as u32)).abs_diff(low) <= 3
}

/// Calls whose answer differs from the reference's by more than its last digit, recorded in
/// `docs/deviations.md`: the reference answers `0` for the inverse hyperbolic sine of `1E-6000`.
const DEVIATIONS: &[&str] = &["$asinh 1E-6000"];

/// Operands whose answer is an exact point: the answer, cohort included, is the reference's.
const EXACT_OPERANDS: &[&str] = &["0", "-0", "0E-5", "0E+3", "NaN", "Infinity", "-Infinity"];

#[test]
fn the_decimal_functions_answer_what_the_reference_answers() {
    let data = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/trig_reference.tsv"),
    )
    .unwrap();
    let (mut total, mut same, mut far, mut exact_wrong) = (0usize, 0usize, Vec::new(), Vec::new());
    for line in data.lines() {
        let mut parts = line.split('\t');
        let (op, input, want) =
            (parts.next().unwrap(), parts.next().unwrap(), parts.next().unwrap());
        if op == "$degreesToRadians" || op == "$radiansToDegrees" {
            continue;
        }
        total += 1;
        let x = decode_raw(&parse(input).unwrap());
        let got = run(op, &x);
        if want == "ERR" {
            assert!(got.is_err(), "{op} {input}: the reference refuses, got {got:?}");
            same += 1;
            continue;
        }
        if DEVIATIONS.contains(&format!("{op} {input}").as_str()) {
            continue;
        }
        let want_raw = decode_raw(&parse(want).unwrap());
        let got =
            got.unwrap_or_else(|_| panic!("{op} {input}: refused, the reference answers {want}"));
        if pack_raw(got) == pack_raw(want_raw) {
            same += 1;
        } else if EXACT_OPERANDS.contains(&input) {
            exact_wrong.push(format!("{op} {input}: want {want} got {}", pack_raw(got)));
        } else if !close(&got, &want_raw) {
            far.push(format!("{op} {input}: want {want} got {}", pack_raw(got)));
        }
    }
    println!("{total} calls, {same} identical");
    for f in far.iter().chain(&exact_wrong) {
        println!("{f}");
    }
    assert!(exact_wrong.is_empty());
    assert!(far.is_empty(), "{far:#?}");
    // The count of identical answers is exact, so that a kernel that slips a unit shows. It moves
    // only if a function changes its last digit: then the figure here, in the register and in
    // `docs/aggregation.md` are changed together, after `decimal_trig_oracle` has held the
    // answers to the true values.
    assert_eq!((total, same), (1478, 1253), "{same} of {total} identical");
}
