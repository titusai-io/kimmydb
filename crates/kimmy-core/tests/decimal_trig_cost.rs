//! What the Decimal128 `sin`, `cos`, `tan` and `atan2` cost for the operands that are dear to
//! reduce: of exponent `1E+6000` and past, and the ratios `1E-6176 / 9E+6144`.
//!
//! The reduction of `1E+6144` modulo `pi/2` divides numbers of some 12,000 digits by one of
//! 6,300, and the series that follows runs at the standard width and not at the operand's
//! (review q8d, M1: it ran at 6,270 places, recomputed `pi` on every call, and took seconds).
//! What this file bounds is that a call is milliseconds. That `pi` is read once and never built
//! is not a matter of time and is held exactly, by a count, in `func/trig_tests.rs`.
//!
//! The tests are `#[ignore]`d and run in release by CI's "Cost ceilings" step, as the other cost
//! files are:
//!
//! ```text
//! cargo test --release -p kimmy-core --test decimal_trig_cost -- --ignored --test-threads 1
//! ```
//!
//! The ceiling is an absolute 250 ms for a call, some two hundred times the 0.5 to 1.4 ms
//! measured in release: a shared runner does not reach it, and the seconds the work once took
//! do. It is deliberately not tight, because a tight one flakes on a runner and a flaky required
//! job costs more than it protects.

use std::time::{Duration, Instant};

use kimmy_core::decimal::Raw;
use kimmy_core::decimal::arith::func;

fn fin(negative: bool, coefficient: u128, exponent: i16) -> Raw {
    Raw::Finite { negative, coefficient, exponent }
}

/// The operands, each with a name: coefficient and exponent of the 1E+6000 class.
fn operands() -> Vec<(&'static str, Raw)> {
    vec![
        ("1E+6000", fin(false, 1, 6000)),
        ("-1E+6000", fin(true, 1, 6000)),
        (
            "1.234567890123456789012345678901234E+6000",
            fin(false, 1_234_567_890_123_456_789_012_345_678_901_234, 5967),
        ),
        ("1E+6144", fin(false, 1, 6144)),
        (
            "9.999999999999999999999999999999999E+6144",
            fin(false, 9_999_999_999_999_999_999_999_999_999_999_999, 6111),
        ),
    ]
}

fn timed<T>(each: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let value = each();
    (value, start.elapsed())
}

/// A call must end inside this; see the file's doc for why it is loose.
const CEILING: Duration = Duration::from_millis(250);

fn check(name: &str, took: Duration) {
    eprintln!("{name}: {took:?} (ceiling {CEILING:?})");
    assert!(took < CEILING, "{name}: {took:?}");
}

#[test]
#[ignore = "time; run in release by CI's Cost ceilings step"]
fn a_huge_operand_costs_milliseconds_not_seconds() {
    // The first call of the process reads `pi`'s digits into limbs and is held to the same.
    let (first, took) = timed(|| func::sin(&fin(false, 1, 6144)).expect("a sine"));
    assert!(matches!(first, Raw::Finite { .. }));
    check("sin 1E+6144, first call", took);

    for (name, x) in operands() {
        for (op, run) in [
            ("sin", func::sin as fn(&Raw) -> Result<Raw, func::Domain>),
            ("cos", func::cos),
            ("tan", func::tan),
        ] {
            let (answer, took) = timed(|| run(&x).expect("a finite answer"));
            assert!(matches!(answer, Raw::Finite { .. }), "{op} {name}");
            check(&format!("{op} {name}"), took);
        }
    }
}

#[test]
#[ignore = "time; run in release by CI's Cost ceilings step"]
fn atan2_of_a_tiny_ratio_costs_milliseconds_not_seconds() {
    let smallest = fin(false, 1, -6176);
    let biggest = fin(false, 9, 6144);
    let negative_biggest = fin(true, 9, 6144);
    for (name, y, x) in [
        ("atan2(1E-6176, 9E+6144)", smallest, biggest),
        ("atan2(1E-6176, -9E+6144)", smallest, negative_biggest),
        ("atan2(-1E-6176, 9E+6144)", fin(true, 1, -6176), biggest),
        ("atan2(9E+6144, 1E-6176)", biggest, smallest),
        ("atan2(1E-100, 1)", fin(false, 1, -100), fin(false, 1, 0)),
        ("atan2(1E-60, -1)", fin(false, 1, -60), fin(true, 1, 0)),
    ] {
        let (answer, took) = timed(|| func::atan2(&y, &x));
        assert!(matches!(answer, Raw::Finite { .. }), "{name}");
        check(name, took);
    }
}

#[test]
#[ignore = "run in release with the other two by CI's Cost ceilings step"]
fn the_answers_for_a_tiny_ratio_are_the_ratio_and_the_complement() {
    let atan2 = |y: Raw, x: Raw| func::atan2(&y, &x);
    // y/x = 1E-6176 / 9E+6144 = 1.111...E-12321, below the smallest number: the nearest
    // 34 digits of it, which the format cannot hold, are a subnormal that rounds to zero.
    let tiny = atan2(fin(false, 1, -6176), fin(false, 9, 6144));
    assert!(matches!(tiny, Raw::Finite { coefficient: 0, negative: false, .. }), "{tiny:?}");
    // 1E-100 / 3E+10 = 3.333...E-111: the ratio to 34 digits.
    let ratio = atan2(fin(false, 1, -100), fin(false, 3, 10));
    assert_eq!(ratio, fin(false, 3_333_333_333_333_333_333_333_333_333_333_333, -144));
    // With a negative x the angle is pi less the ratio, which is pi to 34 digits.
    let pi = atan2(fin(false, 1, -100), fin(true, 3, 10));
    assert_eq!(pi, fin(false, 3_141_592_653_589_793_238_462_643_383_279_503, -33));
    // And the sign of y is the sign of the angle.
    assert_eq!(
        atan2(fin(true, 1, -100), fin(false, 3, 10)),
        fin(true, 3_333_333_333_333_333_333_333_333_333_333_333, -144)
    );
}
