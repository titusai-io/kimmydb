//! What the Decimal128 arithmetic costs, in a release build (ADR-252).
//!
//! The ceilings are the design's. They are time, so they are ignored in the
//! ordinary run and held by CI's `decimal` job, which runs this file in release
//! with `--ignored`. Each asserts a ceiling several times the figure measured on
//! the development machine, so that a noisy runner does not turn a pass into a
//! failure and a real regression still does.

use std::time::{Duration, Instant};

use kimmy_core::decimal::arith::{self, ExactSum};
use kimmy_core::decimal::{MAX_COEFFICIENT, Raw};

fn fin(negative: bool, coefficient: u128, exponent: i16) -> Raw {
    Raw::Finite { negative, coefficient, exponent }
}

/// The 99th percentile of `n` timed calls, in nanoseconds.
fn p99(n: usize, mut each: impl FnMut(usize)) -> u128 {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let start = Instant::now();
        each(i);
        times.push(start.elapsed().as_nanos());
    }
    times.sort_unstable();
    times[n * 99 / 100]
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[test]
#[ignore = "time; run in release by CI's decimal job"]
fn a_two_operand_operation_is_fast() {
    let mut state = 7u64;
    // Prices and ordinary values.
    let ordinary: Vec<(Raw, Raw)> = (0..20_000)
        .map(|_| {
            let a = fin(false, u128::from(splitmix(&mut state) % 10_000_000), -2);
            let b = fin(false, u128::from(splitmix(&mut state) % 1_000_000 + 1), -3);
            (a, b)
        })
        .collect();
    // The extremes: 34 digits at the ends of the exponent range.
    let extreme: Vec<(Raw, Raw)> = (0..20_000)
        .map(|i| {
            let (hi, lo) = (
                fin(false, MAX_COEFFICIENT - (i as u128), 6111),
                fin(true, 1 + (i as u128), -6176),
            );
            if i % 2 == 0 { (hi, lo) } else { (lo, hi) }
        })
        .collect();
    let mut sink = 0u128;
    for (name, pairs, ceiling) in
        [("ordinary", &ordinary, 3_000u128), ("extreme", &extreme, 150_000)]
    {
        for (op, f) in [
            ("add", (|a: &Raw, b: &Raw| arith::add(a, b)) as fn(&Raw, &Raw) -> Raw),
            ("mul", |a, b| arith::mul(a, b)),
            ("div", |a, b| arith::div(a, b).unwrap_or(Raw::Nan)),
        ] {
            let got = p99(pairs.len(), |i| {
                let r = f(&pairs[i].0, &pairs[i].1);
                if let Raw::Finite { coefficient, .. } = r {
                    sink = sink.wrapping_add(coefficient);
                }
            });
            eprintln!("{name} {op}: p99 {got} ns (ceiling {ceiling})");
            assert!(got < ceiling, "{name} {op}: p99 {got} ns");
        }
    }
    std::hint::black_box(sink);
}

#[test]
#[ignore = "time; run in release by CI's decimal job"]
fn the_worst_double_converts_quickly() {
    // The smallest normal: 2^-1022, whose exact value has 708 digits.
    let worst = f64::MIN_POSITIVE.to_bits();
    let got = p99(2_000, |_| {
        std::hint::black_box(arith::from_binary64(std::hint::black_box(worst)));
    });
    eprintln!("R1 of the smallest normal: p99 {got} ns");
    assert!(got < 60_000, "p99 {got} ns");
}

#[test]
#[ignore = "time; run in release by CI's decimal job"]
fn a_sum_of_doubles_from_every_binade_and_one_decimal_is_bounded() {
    let mut state = 11u64;
    let start = Instant::now();
    let mut sum = ExactSum::new();
    for i in 0..100_000u64 {
        let exponent = (splitmix(&mut state) % 2046) + 1;
        let bits = (exponent << 52) | (splitmix(&mut state) & ((1 << 52) - 1)) | ((i & 1) << 63);
        sum.add(&arith::from_binary64(bits).0);
    }
    sum.add(&fin(false, 1, 0));
    std::hint::black_box(sum.finish_sum());
    let took = start.elapsed();
    eprintln!("100,000 doubles from every binade and a decimal: {took:?}");
    assert!(took < Duration::from_secs(4), "{took:?}");
}

#[test]
#[ignore = "time; run in release by CI's decimal job"]
fn a_long_chain_of_multiplications_is_linear() {
    // A product is a chain of rounded steps (ADR-252), so each operand costs one
    // two-operand multiplication whatever the chain so far: 100,000 operands cost
    // about ten times 10,000, not a hundred times.
    let chain = |n: usize| -> (Duration, Raw) {
        let mut state = 13u64;
        // 34-digit coefficients near 1 and its reciprocal, so the value stays in
        // range and every step rounds.
        let ops: Vec<Raw> = (0..n)
            .map(|i| {
                let c = 1_000_000_000_000_000_000_000_000_000_000_000u128
                    + u128::from(splitmix(&mut state) % 1_000_000);
                if i % 2 == 0 {
                    fin(false, c, -33)
                } else {
                    fin(
                        false,
                        9_999_999_999_999_999_999_999_999_999_999_999
                            - u128::from(splitmix(&mut state) % 1_000_000),
                        -34,
                    )
                }
            })
            .collect();
        let start = Instant::now();
        let product = arith::product(&ops);
        (start.elapsed(), std::hint::black_box(product))
    };
    let (small, _) = chain(10_000);
    let (large, product) = chain(100_000);
    eprintln!("10,000 operands {small:?}, 100,000 operands {large:?}");
    assert!(matches!(product, Raw::Finite { .. }));
    assert!(large < Duration::from_millis(500), "{large:?}");
    assert!(
        large.as_secs_f64() < 40.0 * small.as_secs_f64().max(2e-3),
        "{small:?} against {large:?}"
    );
}
