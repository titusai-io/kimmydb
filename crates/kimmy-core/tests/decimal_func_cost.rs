//! What the Decimal128 functions cost (ADR-252): `sqrt`, `exp`, `ln`, `log10`
//! and `pow` on 34-digit arguments of every magnitude, and the rounding family.
//!
//! The ceilings are loose multiples of the figures measured on the development
//! machine in release (tens of microseconds for the transcendental ones, under
//! a microsecond for `round`), so that a noisy runner does not turn a pass into
//! a failure and a real regression still does. They bind only in an optimised
//! build; a debug build runs the loops for the panics and asserts nothing about
//! time.

use std::time::Instant;

use kimmy_core::decimal::arith::func;
use kimmy_core::decimal::{MAX_COEFFICIENT, Raw};

fn fin(negative: bool, coefficient: u128, exponent: i16) -> Raw {
    Raw::Finite { negative, coefficient, exponent }
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
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

/// A 34-digit positive value of any magnitude in the exponent range.
fn wide(state: &mut u64) -> Raw {
    let c =
        (u128::from(splitmix(state)) << 64 | u128::from(splitmix(state))) % (MAX_COEFFICIENT + 1);
    let e = (splitmix(state) % 12_000) as i16 - 6000;
    fin(false, c.max(10u128.pow(33)), e - 33)
}

fn check(name: &str, got: u128, ceiling_ns: u128) {
    eprintln!("{name}: p99 {got} ns (ceiling {ceiling_ns})");
    if !cfg!(debug_assertions) {
        assert!(got < ceiling_ns, "{name}: p99 {got} ns");
    }
}

#[test]
fn the_five_cost_tens_of_microseconds() {
    let mut state = 21u64;
    let n = if cfg!(debug_assertions) { 200 } else { 5_000 };
    let xs: Vec<Raw> = (0..n).map(|_| wide(&mut state)).collect();
    // Arguments of `exp` over its finite range, and exponents of `pow`.
    let args: Vec<Raw> = (0..n)
        .map(|_| {
            let c = (u128::from(splitmix(&mut state)) << 64 | u128::from(splitmix(&mut state)))
                % 10u128.pow(34);
            fin(splitmix(&mut state).is_multiple_of(2), c.max(10u128.pow(33)), -30)
        })
        .collect();
    let ys: Vec<Raw> = (0..n)
        .map(|_| {
            fin(
                splitmix(&mut state).is_multiple_of(2),
                u128::from(splitmix(&mut state) % 100_000_000),
                -4,
            )
        })
        .collect();
    let bases: Vec<Raw> = (0..n)
        .map(|_| fin(false, u128::from(splitmix(&mut state) % 1_000_000_000_000) + 1, -6))
        .collect();
    let mut sink = 0u128;
    let mut take = |r: Raw| {
        if let Raw::Finite { coefficient, .. } = r {
            sink = sink.wrapping_add(coefficient);
        }
    };
    // Warm the constants: the first call builds them.
    take(func::exp(&args[0]));
    let got = p99(n, |i| take(func::sqrt(&xs[i]).unwrap()));
    check("sqrt", got, 100_000);
    let got = p99(n, |i| take(func::exp(&args[i])));
    check("exp", got, 500_000);
    let got = p99(n, |i| take(func::ln(&xs[i]).unwrap()));
    check("ln", got, 500_000);
    let got = p99(n, |i| take(func::log10(&xs[i]).unwrap()));
    check("log10", got, 500_000);
    let got = p99(n, |i| take(func::pow(&bases[i], &ys[i]).unwrap()));
    check("pow (inexact)", got, 1_000_000);
    let whole = fin(false, 7, 0);
    let got = p99(n, |i| take(func::pow(&bases[i], &whole).unwrap()));
    check("pow (exact, y = 7)", got, 50_000);
    let got = p99(n, |i| take(func::round(&xs[i], 3)));
    check("round", got, 10_000);
    take(func::ceil(&xs[0]));
    std::hint::black_box(sink);
}

#[test]
fn the_first_call_builds_the_constants_quickly() {
    // The first call of a transcendental builds `ln 2` and `ln 10`; in a fresh
    // process that is the whole cost, a few hundred small divisions. Run in its
    // own test binary's thread it may not be first, so this only bounds the call.
    let start = Instant::now();
    std::hint::black_box(func::ln(&fin(false, 2, 0)).unwrap());
    let took = start.elapsed();
    eprintln!("a ln, constants included if first: {took:?}");
    if !cfg!(debug_assertions) {
        assert!(took.as_millis() < 50, "{took:?}");
    }
}
