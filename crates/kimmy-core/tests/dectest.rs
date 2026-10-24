//! `kimmy_core::decimal::arith` against the General Decimal Arithmetic suite's
//! `dq` files: `dqAdd`, `dqSubtract`, `dqMultiply` and `dqDivide` (ADR-252).
//! The conversion file, `dqBase`, is `dq_base.rs`.
//!
//! The files are fetched and never vendored: see `tests/common/dectest.rs` and
//! `dq_base.rs`. Both accepted sets (2.62 and CPython's 2.59) are run, each with
//! its own fixed counts.
//!
//! **Which cases count.** kimmydb has one context: 34 digits, the format's
//! exponent range, round half to even. A case is held to it when the suite ran
//! it under `rounding: half_even`, or when its result does not depend on the
//! rounding mode (no `Inexact`, `Rounded`, `Overflow` or `Underflow` condition),
//! except under `floor`, where an exact zero sum is `-0` (IEEE 754 §6.3) and
//! kimmydb's is `+0`. Cases with an operand written as DPD hex (`#…`) are left
//! out: the operand reader here is text. A `Division_by_zero` or
//! `Division_undefined` case must be refused, since kimmydb refuses every zero
//! divisor. Everything else is counted and left out, and the counts are
//! asserted, so a drop fails the test.
//!
//! **What is compared.** The result's bytes, canonical BID, with any NaN
//! against the one quiet NaN (kimmydb propagates no payload).

#[path = "common/dectest.rs"]
mod dectest;

use kimmy_core::decimal::arith;
use kimmy_core::decimal::{Raw, decode_raw, decode_raw_bits, pack_raw, parse};

/// What one file made of the cases it holds.
#[derive(Debug, Default, PartialEq, Eq)]
struct Tally {
    /// Run and equal.
    passed: u64,
    /// A zero divisor the suite flags `Division_by_zero` or `Division_undefined`,
    /// refused.
    refused: u64,
    /// A zero divisor it does not (`Inf / 0` is `Infinity`), refused too.
    refused_unflagged: u64,
    /// Under another rounding mode and not exact, or an exact zero under `floor`.
    other_mode: u64,
    floor_zero_sign: u64,
    /// An operand written as DPD hex.
    hex_operand: u64,
}

fn operand(text: &str) -> Raw {
    decode_raw(&parse(text).unwrap_or_else(|e| panic!("operand {text:?}: {e}")))
}

/// The result as the suite writes it, as arithmetic returns it: NaNs are one.
fn expected(result: &str) -> Raw {
    decode_raw_bits(dectest::result_bits(result))
}

fn run(file: &str, text: &str) -> Tally {
    let mut tally = Tally::default();
    let mut rounding = String::from("half_even");
    for line in text.lines() {
        let t = dectest::tokens(line);
        if t.is_empty() {
            continue;
        }
        if t[0].ends_with(':') {
            if t[0] == "rounding:" {
                rounding = t[1].to_ascii_lowercase();
            }
            continue;
        }
        let Some(arrow) = t.iter().position(|x| x == "->") else { continue };
        let (id, op) = (&t[0], t[1].to_ascii_lowercase());
        if op == "apply" {
            // The one-operand rounding check, not an operation of kimmydb's.
            continue;
        }
        assert_eq!(arrow, 4, "{file} {id}: two operands");
        let (a, b, result) = (&t[2], &t[3], &t[5]);
        let conditions: Vec<&str> = t[6..].iter().map(String::as_str).collect();
        if a.starts_with('#') || b.starts_with('#') {
            tally.hex_operand += 1;
            continue;
        }
        let (x, y) = (operand(a), operand(b));
        if op == "divide" && y.is_zero() {
            // Every zero divisor is refused, whatever the dividend: IEEE gives
            // an infinity or a NaN, and `Inf / 0` and `NaN / 0` too.
            assert_eq!(arith::div(&x, &y), Err(arith::DivisionByZero), "{file} {id}: {a} / {b}");
            if conditions.iter().any(|c| matches!(*c, "Division_by_zero" | "Division_undefined")) {
                tally.refused += 1;
            } else {
                tally.refused_unflagged += 1;
            }
            continue;
        }
        let rounds = conditions
            .iter()
            .any(|c| matches!(*c, "Inexact" | "Rounded" | "Overflow" | "Underflow"));
        if rounding != "half_even" && rounds {
            tally.other_mode += 1;
            continue;
        }
        let want = expected(result);
        let got = match op.as_str() {
            "add" => arith::add(&x, &y),
            "subtract" => arith::sub(&x, &y),
            "multiply" => arith::mul(&x, &y),
            "divide" => arith::div(&x, &y).unwrap_or_else(|_| panic!("{file} {id}: refused")),
            other => panic!("{file} {id}: an operation this test does not know: {other}"),
        };
        let bits = |raw: Raw| u128::from_le_bytes(pack_raw(raw).bytes());
        if bits(got) != bits(want) {
            // Under `floor` an exact zero sum is `-0` (IEEE 754 §6.3) and
            // kimmydb's is `+0`: the same zero, the other sign. Anything else
            // is a failure.
            let zero_sign = rounding == "floor"
                && matches!(
                    (got, want),
                    (
                        Raw::Finite { coefficient: 0, negative: false, exponent: e1 },
                        Raw::Finite { coefficient: 0, negative: true, exponent: e2 },
                    ) if e1 == e2
                );
            assert!(zero_sign, "{file} {id}: {a} {op} {b} -> {result} {conditions:?}, got {got:?}");
            tally.floor_zero_sign += 1;
            continue;
        }
        tally.passed += 1;
    }
    tally
}

#[test]
#[ignore = "needs the dectest files, which are not in the repository: see the module documentation"]
fn arithmetic_meets_the_reference_suite() {
    let Some((version, files)) = dectest::load() else { return };
    let text = |name: &str| &files.iter().find(|(n, _)| *n == name).expect(name).1;
    let add = run("dqAdd", text("dqAdd.decTest"));
    let subtract = run("dqSubtract", text("dqSubtract.decTest"));
    let multiply = run("dqMultiply", text("dqMultiply.decTest"));
    let divide = run("dqDivide", text("dqDivide.decTest"));
    eprintln!(
        "dectest {version}: add {add:?}, subtract {subtract:?}, multiply {multiply:?}, divide {divide:?}"
    );
    let passed = add.passed + subtract.passed + multiply.passed + divide.passed;
    let refused = add.refused + subtract.refused + multiply.refused + divide.refused;
    let other = add.other_mode + subtract.other_mode + multiply.other_mode + divide.other_mode;
    let floor_zero = add.floor_zero_sign
        + subtract.floor_zero_sign
        + multiply.floor_zero_sign
        + divide.floor_zero_sign;
    let hex = add.hex_operand + subtract.hex_operand + multiply.hex_operand + divide.hex_operand;
    // The counts are fixed per set; a drop is a regression.
    // 2.62 has one more case than 2.59, in an other-mode section where it rounds
    // (`dqmul1110`), so the passing counts are the same.
    let want_other = match version {
        "2.62" => 212,
        "2.59" => 211,
        other => panic!("a set with no fixed counts: {other}"),
    };
    assert_eq!(
        (add.passed, subtract.passed, multiply.passed, divide.passed),
        (870, 446, 470, 617),
        "passing cases per file"
    );
    let unflagged = divide.refused_unflagged;
    assert_eq!(passed + unflagged, 2_411, "the design's 2,411 cases: run, and `Inf / 0`");
    assert_eq!(refused, 56, "zero-divisor cases the suite flags, refused");
    assert_eq!(unflagged, 8, "zero-divisor cases it does not flag, refused");
    assert_eq!(other, want_other, "cases under another rounding mode that round");
    assert_eq!(floor_zero, 4, "exact zero sums under floor");
    assert_eq!(hex, 8, "DPD hex operands");
}
