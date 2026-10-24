//! `kimmy_core::decimal::arith` against decNumber (ADR-252): the reference
//! implementation of the General Decimal Arithmetic specification, through the
//! `dec` crate, as a second oracle beside the rational reference in the unit
//! tests and the dectest files.
//!
//! The context is decNumber's `DEC_INIT_DECQUAD`: clamp, 34 digits, `emax`
//! 6144, `emin` -6143, round half to even, traps off. Operands are edge-biased
//! pairs of every shape the format has.
//!
//! **Mapping** (the two ways decNumber differs on purpose): it propagates NaN
//! payloads, so every NaN is one class here; and it divides by a zero and
//! answers an infinity or a NaN, where kimmydb refuses every zero divisor, so
//! that case checks the refusal and compares nothing.

use dec::{Context, Decimal128};
use kimmy_core::decimal::arith;
use kimmy_core::decimal::{MAX_COEFFICIENT, MAX_EXPONENT, MIN_EXPONENT, Raw};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

/// An operand as decNumber's text reader takes it.
fn dec_text(raw: &Raw) -> String {
    match raw {
        Raw::Nan => "NaN".into(),
        Raw::Inf { negative } => format!("{}Infinity", if *negative { "-" } else { "" }),
        Raw::Finite { negative, coefficient, exponent } => {
            format!("{}{}E{}", if *negative { "-" } else { "" }, coefficient, exponent)
        }
    }
}

fn to_dec(raw: &Raw) -> Decimal128 {
    Context::<Decimal128>::default().parse(dec_text(raw)).expect("decNumber reads it")
}

/// What decNumber's answer is, as this crate's `Raw`.
fn from_dec(d: &Decimal128) -> Raw {
    if d.is_nan() {
        Raw::Nan
    } else if d.is_infinite() {
        Raw::Inf { negative: d.is_signed() }
    } else {
        Raw::Finite {
            negative: d.is_signed(),
            coefficient: d.coefficient().unsigned_abs(),
            exponent: i16::try_from(d.exponent()).expect("in range"),
        }
    }
}

fn coefficient() -> impl Strategy<Value = u128> {
    prop_oneof![
        3 => (1u32..=34).prop_flat_map(|digits| {
            let low = if digits == 1 { 0 } else { 10u128.pow(digits - 1) };
            low..=(10u128.pow(digits) - 1)
        }),
        1 => Just(0u128),
        1 => Just(1u128),
        1 => Just(MAX_COEFFICIENT),
        1 => (1u32..=33).prop_map(|k| 10u128.pow(k)),
        1 => (1u32..=33).prop_map(|k| 5 * 10u128.pow(k - 1)),
        1 => (1u32..=33).prop_map(|k| 10u128.pow(k) + 1),
    ]
}

fn exponent() -> impl Strategy<Value = i16> {
    prop_oneof![
        3 => -40i16..=40,
        2 => MIN_EXPONENT..=MAX_EXPONENT,
        1 => Just(MIN_EXPONENT),
        1 => Just(MAX_EXPONENT),
        1 => -6176i16..-6100,
        1 => 6050i16..=6111,
    ]
}

fn operand() -> impl Strategy<Value = Raw> {
    prop_oneof![
        12 => (any::<bool>(), coefficient(), exponent())
            .prop_map(|(negative, coefficient, exponent)| Raw::Finite { negative, coefficient, exponent }),
        1 => Just(Raw::Nan),
        1 => any::<bool>().prop_map(|negative| Raw::Inf { negative }),
    ]
}

/// Pairs that tend to meet: the second near the first in exponent, so sums
/// cancel and carry, and quotients and products land on the edges.
fn pair() -> impl Strategy<Value = (Raw, Raw)> {
    prop_oneof![
        2 => (operand(), operand()),
        2 => (operand(), -40i16..=40, coefficient(), any::<bool>()).prop_map(|(a, delta, c, n)| {
            let e = match a {
                Raw::Finite { exponent, .. } => i32::from(exponent) + i32::from(delta),
                _ => i32::from(delta),
            };
            let e = e.clamp(i32::from(MIN_EXPONENT), i32::from(MAX_EXPONENT)) as i16;
            (a, Raw::Finite { negative: n, coefficient: c, exponent: e })
        }),
    ]
}

fn check(a: &Raw, b: &Raw) -> Result<(), TestCaseError> {
    let (x, y) = (to_dec(a), to_dec(b));
    let mut ctx = Context::<Decimal128>::default();
    let cases: [(&str, Raw, Decimal128); 3] = [
        ("+", arith::add(a, b), ctx.add(x, y)),
        ("-", arith::sub(a, b), ctx.sub(x, y)),
        ("*", arith::mul(a, b), ctx.mul(x, y)),
    ];
    for (op, got, want) in cases {
        prop_assert_eq!(got, from_dec(&want), "{} {} {}", dec_text(a), op, dec_text(b));
    }
    if b.is_zero() {
        prop_assert_eq!(
            arith::div(a, b),
            Err(arith::DivisionByZero),
            "{} / {}",
            dec_text(a),
            dec_text(b)
        );
    } else {
        let want = from_dec(&ctx.div(x, y));
        prop_assert_eq!(arith::div(a, b), Ok(want), "{} / {}", dec_text(a), dec_text(b));
    }
    Ok(())
}

fn run(cases: u32) {
    let mut runner =
        TestRunner::new(Config { cases, failure_persistence: None, ..Config::default() });
    runner.run(&pair(), |(a, b)| check(&a, &b)).unwrap_or_else(|e| panic!("{e}"));
}

/// A chain of multiplications, left to right and each step rounded, is what
/// decNumber computes by multiplying one pair after another (ADR-252).
#[test]
fn a_product_is_decnumbers_chain_of_steps() {
    let mut runner =
        TestRunner::new(Config { cases: 20_000, failure_persistence: None, ..Config::default() });
    runner
        .run(&prop::collection::vec(operand(), 2..9), |ops| {
            let mut ctx = Context::<Decimal128>::default();
            let decs: Vec<Decimal128> = ops.iter().map(to_dec).collect();
            let want = decs[1..].iter().fold(decs[0], |acc, next| ctx.mul(acc, *next));
            prop_assert_eq!(
                arith::product(&ops),
                from_dec(&want),
                "{:?}",
                ops.iter().map(dec_text).collect::<Vec<_>>()
            );
            Ok(())
        })
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn arithmetic_agrees_with_decnumber() {
    run(20_000);
}

/// The long run: a million pairs through each of the four operations.
#[test]
#[ignore = "a million pairs; run in CI's decimal job"]
fn arithmetic_agrees_with_decnumber_over_a_million_pairs() {
    run(1_000_000);
}

#[test]
fn the_context_is_the_one_the_design_names() {
    // The arithmetic context decNumber starts from for a decQuad: 34 digits,
    // the format's range, an overflow that is an infinity, ties to even.
    let mut ctx = Context::<Decimal128>::default();
    let mut read = |text: &str| ctx.parse(text).unwrap();
    let (max, ten, three, two) =
        (read("9.999999999999999999999999999999999E+6144"), read("10"), read("3"), read("2"));
    let (even, odd, half) = (
        read("1234567890123456789012345678901234"),
        read("1234567890123456789012345678901235"),
        read("0.5"),
    );
    assert!(ctx.mul(max, ten).is_infinite());
    assert_eq!(from_dec(&ctx.div(three, two)), from_dec(&ctx.parse("1.5").unwrap()));
    // A tie goes to the even neighbour, up or down.
    assert_eq!(from_dec(&ctx.add(even, half)), from_dec(&even));
    assert_eq!(
        from_dec(&ctx.add(odd, half)),
        from_dec(&ctx.parse("1234567890123456789012345678901236").unwrap())
    );
}

/// Why the oracle's own conversion from a double is not used: it formats the
/// double with the shortest round trip, which is not the double's value.
#[test]
fn decnumbers_double_conversion_is_not_r1() {
    let shortest = dec::Decimal::<13>::from(0.1f64);
    assert_eq!(shortest.to_string(), "0.1");
    let (r1, inexact) = arith::from_binary64(0.1f64.to_bits());
    assert!(inexact);
    assert_eq!(dec_text(&r1), "1000000000000000055511151231257827E-34");
}
