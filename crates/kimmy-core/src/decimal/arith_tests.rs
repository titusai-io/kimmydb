//! `decimal::arith` against an exact-rational reference that shares no code with
//! it, against Rust's own correctly rounded text-to-double parse, and against the
//! design's worked examples (ADR-252).
//!
//! The reference holds a value as a numerator over a positive denominator in
//! `num-bigint` integers, never reduced, finds the digit at which to round by
//! comparing against powers of ten, and rounds half to even with integer
//! division. It is written from the definition of the format and not from
//! `arith.rs`: a rounding bug in one is a disagreement with the other.

use std::cmp::Ordering;

use num_bigint::{BigInt, Sign};
use proptest::prelude::*;

use super::arith::{self, ExactSum};
use super::{
    MAX_COEFFICIENT, MAX_EXPONENT, MIN_EXPONENT, Raw, decode_raw, decode_raw_bits, pack_raw,
};

const ETINY: i64 = MIN_EXPONENT as i64;
const ETOP: i64 = MAX_EXPONENT as i64;

fn fin(negative: bool, coefficient: u128, exponent: i16) -> Raw {
    Raw::Finite { negative, coefficient, exponent }
}

fn d(text: &str) -> Raw {
    decode_raw(&super::parse(text).unwrap_or_else(|e| panic!("{text}: {e}")))
}

/// A result as text: scientific, with the cohort member visible.
fn show(raw: &Raw) -> String {
    match raw {
        Raw::Nan => "NaN".into(),
        Raw::Inf { negative } => format!("{}Infinity", if *negative { "-" } else { "" }),
        Raw::Finite { negative, coefficient, exponent } => {
            format!("{}{}E{}", if *negative { "-" } else { "" }, coefficient, exponent)
        }
    }
}

fn same(got: Raw, want: &str) {
    assert_eq!(show(&got), show(&d(want)), "want {want}");
}

/// A property's configuration: `cases` of them, and rejections (a divisor of
/// zero, a double that is not finite) that do not abort the run however many
/// cases `PROPTEST_CASES` asks for.
fn config(cases: u32) -> ProptestConfig {
    ProptestConfig { cases, max_global_rejects: 1_000_000, ..ProptestConfig::default() }
}

// ---------------------------------------------------------------------------
// The reference
// ---------------------------------------------------------------------------

fn pow10(k: i64) -> BigInt {
    BigInt::from(10).pow(u32::try_from(k).expect("a non-negative power"))
}

fn digits(n: &BigInt) -> i64 {
    if n.sign() == Sign::NoSign { 0 } else { n.magnitude().to_string().len() as i64 }
}

/// The exact value of a finite decimal as `(numerator, denominator)`.
fn value(raw: &Raw) -> (BigInt, BigInt) {
    let Raw::Finite { negative, coefficient, exponent } = *raw else { panic!("not finite") };
    let mut n = BigInt::from(coefficient);
    if negative {
        n = -n;
    }
    if exponent >= 0 {
        (n * pow10(i64::from(exponent)), BigInt::from(1))
    } else {
        (n, pow10(i64::from(-exponent)))
    }
}

/// `⌊log10 |n/d|⌋` for a nonzero value.
fn adjusted(n: &BigInt, d: &BigInt) -> i64 {
    let n = BigInt::from(n.magnitude().clone());
    let mut guess = digits(&n) - digits(d);
    // `10^guess <= n/d < 10^(guess + 1)`, to within one.
    let at_least = |k: i64| if k >= 0 { n >= d * pow10(k) } else { &n * pow10(-k) >= *d };
    while !at_least(guess) {
        guess -= 1;
    }
    while at_least(guess + 1) {
        guess += 1;
    }
    guess
}

/// `n/d` rounded to the format, half to even, with `pref` the preferred
/// exponent of an exact result; the sign of a zero is `zero_negative`.
fn round_reference(n: &BigInt, d: &BigInt, pref: i64, zero_negative: bool) -> Raw {
    let clamp = |e: i64| e.clamp(ETINY, ETOP) as i16;
    if n.sign() == Sign::NoSign {
        return fin(zero_negative, 0, clamp(pref));
    }
    let negative = n.sign() == Sign::Minus;
    let magnitude = BigInt::from(n.magnitude().clone());
    let adj = adjusted(&magnitude, d);
    // The exact result has a member of the cohort when `n/d = c · 10^q` has an
    // integral `c` of at most 34 digits at some `q` in range. `q_max` is the
    // value's own exponent, trailing zeros taken off; `q_min` the least with 34
    // digits.
    let q_min = (adj - 33).max(ETINY);
    // `n/d / 10^q` integral?
    let integral_at = |q: i64| -> Option<BigInt> {
        let (num, den) = if q >= 0 {
            (magnitude.clone(), d * pow10(q))
        } else {
            (&magnitude * pow10(-q), d.clone())
        };
        (&num % &den == BigInt::from(0)).then(|| &num / &den)
    };
    if integral_at(q_min).is_some() {
        // The largest `q` at which it is still integral, at most `ETOP`.
        let mut q_max = q_min;
        while q_max < ETOP && integral_at(q_max + 1).is_some() {
            q_max += 1;
        }
        // `q_max` can run to thousands of steps only for a value that is a
        // large power of ten; the tests keep those few.
        let q = pref.clamp(q_min, q_max);
        let c = integral_at(q).expect("integral between q_min and q_max");
        let c: u128 = u128::try_from(c).expect("at most 34 digits");
        if q + i64::from(digits_u(c)) - 1 <= 6144 {
            return pad(negative, c, q);
        }
    }
    // Inexact (or not representable): round at `q_min`.
    let (num, den) = if q_min >= 0 {
        (magnitude.clone(), d * pow10(q_min))
    } else {
        (&magnitude * pow10(-q_min), d.clone())
    };
    let (mut q, r) = (&num / &den, &num % &den);
    let twice: BigInt = &r * BigInt::from(2);
    match twice.cmp(&den) {
        Ordering::Greater => q += 1,
        Ordering::Equal if (&q % 2) != BigInt::from(0) => q += 1,
        _ => {}
    }
    let mut e = q_min;
    let mut c: BigInt = q;
    if digits(&c) > 34 {
        c /= 10;
        e += 1;
    }
    if c.sign() == Sign::NoSign {
        return fin(negative, 0, clamp(e));
    }
    let c: u128 = u128::try_from(c).expect("34 digits");
    if e + i64::from(digits_u(c)) - 1 > 6144 {
        return Raw::Inf { negative };
    }
    pad(negative, c, e)
}

fn digits_u(c: u128) -> u32 {
    c.to_string().len() as u32
}

/// Fold an exponent above 6111 down by padding the coefficient.
fn pad(negative: bool, mut c: u128, mut e: i64) -> Raw {
    if e > ETOP {
        c *= 10u128.pow((e - ETOP) as u32);
        e = ETOP;
    }
    fin(negative, c, e as i16)
}

fn parts(raw: &Raw) -> (bool, u128, i64) {
    let Raw::Finite { negative, coefficient, exponent } = *raw else { panic!("not finite") };
    (negative, coefficient, i64::from(exponent))
}

/// Reference sum of finite terms.
fn reference_sum(terms: &[Raw]) -> Raw {
    let min_e = terms.iter().map(|t| parts(t).2).min().unwrap();
    let mut total = BigInt::from(0);
    for t in terms {
        let (neg, c, e) = parts(t);
        let term = BigInt::from(c) * pow10(e - min_e);
        total += if neg { -term } else { term };
    }
    let all_negative_zero = terms.iter().all(|t| matches!(parts(t), (true, 0, _)));
    if min_e >= 0 {
        round_reference(&(total * pow10(min_e)), &BigInt::from(1), min_e, all_negative_zero)
    } else {
        round_reference(&total, &pow10(-min_e), min_e, all_negative_zero)
    }
}

fn reference_product(ops: &[Raw]) -> Raw {
    let mut n = BigInt::from(1);
    let mut e = 0i64;
    let mut negative = false;
    for o in ops {
        let (neg, c, ex) = parts(o);
        n *= BigInt::from(c);
        e += ex;
        negative ^= neg;
    }
    let n = if negative { -n } else { n };
    if e >= 0 {
        round_reference(&(n * pow10(e)), &BigInt::from(1), e, negative)
    } else {
        round_reference(&n, &pow10(-e), e, negative)
    }
}

/// `a × b` for any two operands, rounded once: the reference for one step of a
/// chain, with the specials the exact reference does not take.
fn reference_mul2(a: &Raw, b: &Raw) -> Raw {
    let negative =
        |r: &Raw| matches!(r, Raw::Inf { negative: true } | Raw::Finite { negative: true, .. });
    match (a, b) {
        (Raw::Nan, _) | (_, Raw::Nan) => Raw::Nan,
        (Raw::Inf { .. }, o) | (o, Raw::Inf { .. }) => {
            if o.is_zero() {
                Raw::Nan
            } else {
                Raw::Inf { negative: negative(a) != negative(b) }
            }
        }
        _ => reference_product(&[*a, *b]),
    }
}

/// A chain left to right, each step rounded.
fn reference_chain(ops: &[Raw]) -> Raw {
    ops[1..].iter().fold(ops[0], |acc, next| reference_mul2(&acc, next))
}

fn reference_div(a: &Raw, b: &Raw) -> Raw {
    let (na, da) = value(a);
    let (nb, db) = value(b);
    let negative = parts(a).0 ^ parts(b).0;
    let (mut n, mut den) = (na * &db, da * nb);
    if den.sign() == Sign::Minus {
        n = -n;
        den = -den;
    }
    round_reference(&n, &den, parts(a).2 - parts(b).2, negative)
}

fn reference_avg(terms: &[Raw]) -> Raw {
    let min_e = terms.iter().map(|t| parts(t).2).min().unwrap();
    let mut total = BigInt::from(0);
    for t in terms {
        let (neg, c, e) = parts(t);
        let term = BigInt::from(c) * pow10(e - min_e);
        total += if neg { -term } else { term };
    }
    let all_negative_zero = terms.iter().all(|t| matches!(parts(t), (true, 0, _)));
    let count = BigInt::from(terms.len());
    // `total · 10^min_e / count`.
    if min_e >= 0 {
        round_reference(&(total * pow10(min_e)), &count, min_e, all_negative_zero)
    } else {
        round_reference(&total, &(count * pow10(-min_e)), min_e, all_negative_zero)
    }
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

fn coefficient() -> impl Strategy<Value = u128> {
    prop_oneof![
        3 => (1u32..=34).prop_flat_map(|digits| {
            let low = if digits == 1 { 0 } else { 10u128.pow(digits - 1) };
            let high = 10u128.pow(digits) - 1;
            low..=high
        }),
        1 => Just(0u128),
        1 => Just(1u128),
        1 => Just(MAX_COEFFICIENT),
        1 => (1u32..=33).prop_map(|k| 10u128.pow(k)),
        1 => (1u32..=33).prop_map(|k| 10u128.pow(k) - 1),
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
        1 => Just(0i16),
    ]
}

fn finite_raw() -> impl Strategy<Value = Raw> {
    (any::<bool>(), coefficient(), exponent())
        .prop_map(|(negative, coefficient, exponent)| fin(negative, coefficient, exponent))
}

/// A pair that tends to meet: the second exponent near the first, so sums
/// cancel and carry, and products and quotients land on the range's edges.
fn pair() -> impl Strategy<Value = (Raw, Raw)> {
    prop_oneof![
        3 => (finite_raw(), finite_raw()),
        3 => (finite_raw(), -40i16..=40, coefficient(), any::<bool>()).prop_map(|(a, delta, c, n)| {
            let e = parts(&a).2 + i64::from(delta);
            (a, fin(n, c, e.clamp(ETINY, ETOP) as i16))
        }),
    ]
}

fn any_special() -> impl Strategy<Value = Raw> {
    prop_oneof![
        Just(Raw::Nan),
        Just(Raw::Inf { negative: false }),
        Just(Raw::Inf { negative: true }),
        finite_raw(),
        finite_raw(),
        finite_raw(),
    ]
}

// ---------------------------------------------------------------------------
// The design's worked examples
// ---------------------------------------------------------------------------

#[test]
fn two_operand_results_keep_the_preferred_quantum() {
    same(arith::add(&d("1.10"), &d("1.20")), "2.30");
    same(arith::add(&d("1.5"), &d("0.00")), "1.50");
    same(arith::add(&d("1.5"), &d("0")), "1.5");
    same(arith::sub(&d("1.00"), &d("1.00")), "0.00");
    same(arith::product(&[d("1.5"), d("1.0")]), "1.50");
    same(arith::product(&[d("0"), d("1.50")]), "0.00");
    same(arith::div(&d("2.30"), &d("2")).unwrap(), "1.15");
    same(arith::div(&d("3"), &d("2")).unwrap(), "1.5");
    same(arith::div(&d("6"), &d("3")).unwrap(), "2");
    same(arith::div(&d("1.00"), &d("4")).unwrap(), "0.25");
    same(arith::sub(&d("10.00"), &d("0.50")), "9.50");
}

#[test]
fn inexact_results_have_34_digits() {
    same(arith::div(&d("1"), &d("3")).unwrap(), "0.3333333333333333333333333333333333");
    same(arith::add(&d("1"), &d("1E-40")), "1.000000000000000000000000000000000");
    same(arith::div(&d("2"), &d("3")).unwrap(), "0.6666666666666666666666666666666667");
}

#[test]
fn a_sum_has_the_least_exponent_of_all_its_terms() {
    let mut sum = ExactSum::new();
    for t in ["1.10", "1.20"] {
        sum.add(&d(t));
    }
    sum.add_integers(3, 1);
    same(sum.finish_sum(), "5.30");
    // An integer counts: its exponent is 0.
    let mut sum = ExactSum::new();
    sum.add(&d("1E+2"));
    sum.add_integers(3, 1);
    same(sum.finish_sum(), "103");
}

#[test]
fn a_sum_does_not_depend_on_the_order_of_its_terms() {
    // Per-step rounding: a tie to even, twice, in the first order.
    let terms = [d("1E+34"), d("0.5"), d("0.5"), d("-1E+34")];
    let mut left_to_right = terms[0];
    for t in &terms[1..] {
        left_to_right = arith::add(&left_to_right, t);
    }
    same(left_to_right, "0E+1");
    for order in [[0, 1, 2, 3], [0, 3, 1, 2], [3, 2, 1, 0], [1, 0, 3, 2]] {
        let mut sum = ExactSum::new();
        for i in order {
            sum.add(&terms[i]);
        }
        same(sum.finish_sum(), "1.0");
    }
}

#[test]
fn a_product_is_a_chain_with_each_step_rounded() {
    // Left to right, as MongoDB multiplies and IEEE 754 defines a sequence of
    // operations (ADR-252): the order matters where a step overflows or rounds.
    let chain = |ops: &[&str]| arith::product(&ops.iter().map(|t| d(t)).collect::<Vec<_>>());
    same(chain(&["1E+5000", "1E+5000", "1E-5000"]), "Infinity");
    same(chain(&["1E+5000", "1E-5000", "1E+5000"]), "1E+5000");
    same(chain(&["1E-5000", "1E+5000", "1E+5000"]), "1E+5000");
    same(chain(&["1.5", "1.0", "2"]), "3.00");
    // A step that rounds: the first two factors' product has 68 digits and is
    // rounded to 34 before the third, where the exact product of all three
    // rounds once. The pair differs by one unit in the last place (a row of the
    // golden corpus, with libmpdec multiplying one pair after another).
    let ops = [
        "1000000000000000000000000957877941E-34",
        "9999999999999999999999999787852355E-35",
        "1000000000000000000000000407746305E-32",
    ];
    let terms: Vec<Raw> = ops.iter().map(|t| d(t)).collect();
    let stepwise = arith::product(&terms);
    let exact_once = reference_product(&terms);
    assert_ne!(stepwise, exact_once, "the premise: rounding each step differs");
    same(stepwise, "1000000000000000000000001344409481E-34");
    same(exact_once, "1000000000000000000000001344409482E-34");
    // One operand is itself, canonical; none is one.
    same(chain(&["2.30"]), "2.30");
    assert_eq!(arith::product(&[]), arith::from_i64(1));
    // The same chain with the operands reversed.
    assert_eq!(chain(&["-0", "5", "Infinity"]), Raw::Nan);
    assert_eq!(chain(&["5", "Infinity", "-0"]), Raw::Nan);
}

#[test]
fn a_mean_is_rounded_once() {
    let mut avg = ExactSum::new();
    avg.add(&d("2.693319332926108795000640676319904E+35"));
    avg.add(&d("7.447367703126639551991307315256628E+34"));
    same(avg.finish_avg(), "1.719028051619386375099885703922783E+35");
    // The sum rounded first and then divided is one unit higher.
    let mut sum = ExactSum::new();
    sum.add(&d("2.693319332926108795000640676319904E+35"));
    sum.add(&d("7.447367703126639551991307315256628E+34"));
    let rounded_sum = sum.finish_sum();
    same(arith::div(&rounded_sum, &d("2")).unwrap(), "1.719028051619386375099885703922784E+35");

    let mut avg = ExactSum::new();
    avg.add(&d("1E+6111"));
    avg.add(&d("1E-6176"));
    same(avg.finish_avg(), "5.000000000000000000000000000000000E+6110");

    let mut avg = ExactSum::new();
    for t in ["1.00", "2.00", "2.00"] {
        avg.add(&d(t));
    }
    same(avg.finish_avg(), "1.666666666666666666666666666666667");

    let mut avg = ExactSum::new();
    avg.add(&d("1.10"));
    avg.add(&d("1.20"));
    same(avg.finish_avg(), "1.15");
}

#[test]
fn overflow_is_the_infinity_and_underflow_is_gradual() {
    same(arith::product(&[d("9.999999999999999999999999999999999E+6144"), d("10")]), "Infinity");
    same(arith::product(&[d("-9.999999999999999999999999999999999E+6144"), d("10")]), "-Infinity");
    same(arith::product(&[d("1E-6143"), d("0.1")]), "1E-6144");
    same(arith::div(&d("3E-6176"), &d("2")).unwrap(), "2E-6176");
    same(arith::div(&d("1E-6176"), &d("2")).unwrap(), "0E-6176");
    same(arith::product(&[d("1E-6000"), d("1E-6000")]), "0E-6176");
    // An exact result above the range's top exponent is folded down.
    same(arith::add(&d("1E+6144"), &d("0")), "1.000000000000000000000000000000000E+6144");
}

#[test]
fn nan_infinity_and_zero_follow_the_standard() {
    let inf = Raw::Inf { negative: false };
    let ninf = Raw::Inf { negative: true };
    assert_eq!(arith::add(&inf, &ninf), Raw::Nan);
    assert_eq!(arith::sub(&inf, &inf), Raw::Nan);
    assert_eq!(arith::product(&[inf, d("0")]), Raw::Nan);
    assert_eq!(arith::product(&[inf, d("-2")]), ninf);
    assert_eq!(arith::add(&inf, &d("5")), inf);
    assert_eq!(arith::add(&d("5"), &ninf), ninf);
    assert_eq!(arith::div(&inf, &inf), Ok(Raw::Nan));
    assert_eq!(arith::div(&inf, &d("-2")), Ok(ninf));
    same(arith::div(&d("5"), &ninf).unwrap(), "-0E-6176");
    assert_eq!(arith::add(&Raw::Nan, &d("1")), Raw::Nan);
    assert_eq!(arith::div(&d("1"), &d("0")), Err(arith::DivisionByZero));
    assert_eq!(arith::div(&Raw::Nan, &d("-0.0")), Err(arith::DivisionByZero));
    // The sign of an exact zero.
    same(arith::add(&d("1.00"), &d("-1.00")), "0.00");
    same(arith::add(&d("-0"), &d("-0")), "-0");
    same(arith::add(&d("-0"), &d("0")), "0");
    same(arith::sub(&d("-0"), &d("0")), "-0");
    same(arith::sub(&d("0"), &d("0")), "0");
}

#[test]
fn the_zero_divisor_is_refused_in_every_form() {
    for zero in ["0", "-0", "0E-100", "0.0", "-0.0", "0E+6111"] {
        assert_eq!(arith::div(&d("1"), &d(zero)), Err(arith::DivisionByZero), "{zero}");
    }
    // A non-canonical coefficient, and the large-coefficient form, are zeros.
    let over = (1u128 << 113) - 1; // above 10^34 - 1
    for bits in [over | (6176u128 << 113), (0b11u128 << 125) | (6176u128 << 111)] {
        let zero = decode_raw_bits(bits);
        assert!(zero.is_zero(), "{bits:x}");
        assert_eq!(arith::div(&d("1"), &zero), Err(arith::DivisionByZero));
    }
}

#[test]
fn an_integer_enters_exactly_even_the_smallest() {
    same(arith::sub(&d("0"), &arith::from_i64(i64::MIN)), "9223372036854775808");
    same(arith::add(&arith::from_i64(i64::MIN), &d("0")), "-9223372036854775808");
    let mut sum = ExactSum::new();
    sum.add_integers(i128::from(i64::MAX) + 1, 1);
    sum.add(&d("0"));
    same(sum.finish_sum(), "9223372036854775808");
}

// ---------------------------------------------------------------------------
// Against the reference
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(config(4000))]

    #[test]
    fn add_agrees_with_the_reference((a, b) in pair()) {
        prop_assert_eq!(arith::add(&a, &b), reference_sum(&[a, b]), "{} + {}", show(&a), show(&b));
        prop_assert_eq!(arith::add(&a, &b), arith::add(&b, &a));
    }

    #[test]
    fn sub_agrees_with_the_reference((a, b) in pair()) {
        prop_assert_eq!(arith::sub(&a, &b), reference_sum(&[a, b.negated()]), "{} - {}", show(&a), show(&b));
    }

    #[test]
    fn mul_agrees_with_the_reference((a, b) in pair()) {
        prop_assert_eq!(arith::product(&[a, b]), reference_product(&[a, b]), "{} * {}", show(&a), show(&b));
    }

    #[test]
    fn div_agrees_with_the_reference((a, b) in pair()) {
        prop_assume!(!b.is_zero());
        prop_assert_eq!(arith::div(&a, &b).unwrap(), reference_div(&a, &b), "{} / {}", show(&a), show(&b));
    }
}

proptest! {
    #![proptest_config(config(600))]

    #[test]
    fn sums_and_means_agree_with_the_reference_in_any_order(
        terms in prop::collection::vec(finite_raw(), 1..9),
        seed in any::<u64>(),
    ) {
        let want_sum = reference_sum(&terms);
        let want_avg = reference_avg(&terms);
        let mut shuffled = terms.clone();
        // A fixed permutation from the seed.
        let mut state = seed | 1;
        for i in (1..shuffled.len()).rev() {
            state ^= state << 13; state ^= state >> 7; state ^= state << 17;
            shuffled.swap(i, (state % (i as u64 + 1)) as usize);
        }
        for order in [&terms, &shuffled] {
            let mut sum = ExactSum::new();
            for t in order.iter() { sum.add(t); }
            prop_assert_eq!(sum.finish_sum(), want_sum, "{:?}", terms.iter().map(show).collect::<Vec<_>>());
            prop_assert_eq!(sum.finish_avg(), want_avg, "{:?}", terms.iter().map(show).collect::<Vec<_>>());
        }
    }

    #[test]
    fn a_product_is_the_chain_of_rounded_steps(ops in prop::collection::vec(any_special(), 1..7)) {
        let want = if ops.len() == 1 { arith::product(&ops) } else { reference_chain(&ops) };
        prop_assert_eq!(arith::product(&ops), want, "{:?}", ops.iter().map(show).collect::<Vec<_>>());
    }

    #[test]
    fn specials_follow_the_table(a in any_special(), b in any_special()) {
        let got = arith::add(&a, &b);
        match (a, b) {
            (Raw::Nan, _) | (_, Raw::Nan) => prop_assert_eq!(got, Raw::Nan),
            (Raw::Inf { negative: x }, Raw::Inf { negative: y }) if x != y => prop_assert_eq!(got, Raw::Nan),
            (Raw::Inf { negative }, _) | (_, Raw::Inf { negative }) => prop_assert_eq!(got, Raw::Inf { negative }),
            _ => {
                let finite_or_infinite = matches!(got, Raw::Finite { .. } | Raw::Inf { .. });
                prop_assert!(finite_or_infinite)
            }
        }
    }
}

#[test]
fn a_wide_span_of_exponents_sums_exactly() {
    // The span attack: terms at every exponent from the bottom of the range to
    // the top, in a sparse map, then cancelled.
    let mut sum = ExactSum::new();
    let mut want = BigInt::from(0);
    for e in (MIN_EXPONENT..=MAX_EXPONENT).step_by(97) {
        sum.add(&fin(false, 123_456_789, e));
        want += BigInt::from(123_456_789u64) * pow10(i64::from(e) - ETINY);
    }
    for e in (MIN_EXPONENT..=MAX_EXPONENT).step_by(97) {
        sum.add(&fin(true, 123_456_789, e));
    }
    sum.add(&fin(false, 7, -6176));
    same(sum.finish_sum(), "7E-6176");
    assert!(want.sign() == Sign::Plus);
}

#[test]
fn the_window_and_the_map_hold_the_same_sum() {
    // Terms within 16 limb positions stay in a window; one far away moves the
    // sum to the map. The value is the same as the reference either way.
    let near = [fin(false, 12345, -3), fin(false, 99, 20), fin(true, 7, 5)];
    let far = [fin(false, 1, 6000)];
    let mut windowed = ExactSum::new();
    for t in near {
        windowed.add(&t);
    }
    assert_eq!(windowed.finish_sum(), reference_sum(&near));
    let all: Vec<Raw> = near.iter().chain(far.iter()).copied().collect();
    let mut sparse = ExactSum::new();
    for t in &all {
        sparse.add(t);
    }
    assert_eq!(sparse.finish_sum(), reference_sum(&all));
}

// ---------------------------------------------------------------------------
// Binary64
// ---------------------------------------------------------------------------

/// The exact reference for R1: the double's value as a rational, rounded.
fn reference_r1(v: f64) -> (Raw, bool) {
    let bits = v.to_bits();
    let negative = bits >> 63 == 1;
    let biased = ((bits >> 52) & 0x7FF) as i64;
    let fraction = bits & ((1u64 << 52) - 1);
    if biased == 0x7FF {
        return (if fraction == 0 { Raw::Inf { negative } } else { Raw::Nan }, false);
    }
    let (m, f) = if biased == 0 { (fraction, -1074) } else { (fraction | 1 << 52, biased - 1075) };
    let (n, den) = if f >= 0 {
        (BigInt::from(m) << (f as usize), BigInt::from(1))
    } else {
        (BigInt::from(m), BigInt::from(1) << ((-f) as usize))
    };
    if m == 0 {
        return (fin(negative, 0, 0), false);
    }
    let n = if negative { -n } else { n };
    // Use the reference rounding with preferred exponent 0: an exact result
    // takes the member closest to 0, which for a non-integer is the largest
    // exponent that holds the value; an integer is at exponent 0.
    let result = round_reference(&n, &den, 0, negative);
    let exact = {
        let (num, den) = (&n, &den);
        // Exact iff rounding changed nothing.
        let (rn, rd) = value(&result);
        &rn * den == num * &rd
    };
    (result, !exact)
}

fn double_cases() -> impl Strategy<Value = f64> {
    prop_oneof![
        4 => any::<u64>().prop_map(f64::from_bits),
        // Every binade, edge mantissas.
        2 => (0u64..2047, prop_oneof![Just(0u64), Just((1u64 << 52) - 1), Just(1u64), 0u64..(1 << 52)], any::<bool>())
            .prop_map(|(e, m, s)| f64::from_bits(((s as u64) << 63) | (e << 52) | m)),
        // The tie doubles: 35-digit expansions, f in [-50, -26].
        2 => (1u64 << 52..1 << 53, 26i32..=50, any::<bool>())
            .prop_map(|(m, k, s)| { let v = m as f64 * 2f64.powi(-k); if s { -v } else { v } }),
        // Two-place prices and tidy values.
        2 => (0i64..10_000_000, 0u32..4).prop_map(|(c, p)| c as f64 / 10f64.powi(p as i32)),
        1 => (-1_000_000i64..1_000_000).prop_map(|i| i as f64),
        1 => (0u32..1024).prop_map(|k| 2f64.powi(k as i32)),
    ]
}

#[test]
fn r1_examples_from_the_design() {
    let r1 = |v: f64| arith::from_binary64(v.to_bits());
    let (x, inexact) = r1(0.1);
    same(x, "0.1000000000000000055511151231257827");
    assert!(inexact);
    same(r1(0.01).0, "0.01000000000000000020816681711721685");
    for (v, text) in [(2.5, "2.5"), (0.5, "0.5"), (0.125, "0.125"), (100.0, "100")] {
        let (x, inexact) = r1(v);
        same(x, text);
        assert!(!inexact, "{v}");
    }
    same(r1(1e20).0, "100000000000000000000");
    same(r1(1e300).0, "1.000000000000000052504760255204420E+300");
    same(r1(2f64.powi(-49)).0, "1.776356839400250464677810668945312E-15");
    same(r1(5e-324).0, "4.940656458412465441765687928682214E-324");
    same(r1(1.7976931348623157e308).0, "1.797693134862315708145274237317044E+308");
    same(r1(-0.0).0, "-0");
    same(r1(0.0).0, "0");
    same(r1(f64::NAN).0, "NaN");
    same(r1(f64::INFINITY).0, "Infinity");
    same(r1(f64::NEG_INFINITY).0, "-Infinity");
    // An integral double of 35 digits whose last digits are zeros drops only
    // zeros: it is its exact value, so the conversion is exact.
    let zeros = (3 * 5u64.pow(22)) as f64 * 2f64.powi(61);
    let (x, inexact) = r1(zeros);
    assert!(!inexact, "{zeros:e} is 3 · 2^39 · 10^22 exactly");
    same(x, "16492674416640000000000000000000000");
    same(r1(0.3).0, "0.2999999999999999888977697537484346");
    same(r1(0.1 + 0.2).0, "0.3000000000000000444089209850062616");
}

proptest! {
    #![proptest_config(config(5000))]

    #[test]
    fn r1_agrees_with_the_reference(v in double_cases()) {
        prop_assume!(!v.is_nan());
        let (got, inexact) = arith::from_binary64(v.to_bits());
        let (want, want_inexact) = reference_r1(v);
        prop_assert_eq!(got, want, "{:e}", v);
        prop_assert_eq!(inexact, want_inexact, "{:e}", v);
    }

    #[test]
    fn r1_round_trips_through_the_conversion_back(v in double_cases()) {
        prop_assume!(!v.is_nan());
        let (r1, _) = arith::from_binary64(v.to_bits());
        let back = f64::from_bits(arith::to_binary64(&r1));
        prop_assert_eq!(back.to_bits(), v.to_bits(), "{:e}", v);
    }

    #[test]
    fn r1_is_monotone(a in double_cases(), b in double_cases()) {
        prop_assume!(a.is_finite() && b.is_finite());
        let (ra, rb) = (arith::from_binary64(a.to_bits()).0, arith::from_binary64(b.to_bits()).0);
        let order = |x: &Raw, y: &Raw| -> Ordering {
            let ((nx, dx), (ny, dy)) = (value(x), value(y));
            (nx * dy).cmp(&(ny * dx))
        };
        let doubles = a.partial_cmp(&b).unwrap();
        // Monotone: never reverses; two doubles may merge only where a decimal
        // of 34 digits cannot tell them apart, which R1 does not do for
        // doubles: it is injective on finite doubles.
        prop_assert_eq!(order(&ra, &rb), doubles, "{:e} {:e}", a, b);
    }

    #[test]
    fn a_decimal_converts_to_the_nearest_double(
        negative in any::<bool>(),
        c in coefficient(),
        e in prop_oneof![-400i16..=400, MIN_EXPONENT..=MAX_EXPONENT, -330i16..=-300, 290i16..=320],
    ) {
        let raw = fin(negative, c, e);
        let got = f64::from_bits(arith::to_binary64(&raw));
        let text = format!("{}{}e{}", if negative { "-" } else { "" }, c, e);
        // Rust's text-to-double parse is correctly rounded, half to even.
        let want: f64 = text.parse().unwrap();
        prop_assert_eq!(got.to_bits(), want.to_bits(), "{}", text);
    }
}

/// Where the value sits a hair above or below the midpoint between two doubles
/// (`delta` of 128 or 256 in 2^-127 relative terms), the 128-bit window reads
/// exactly half and only the end bit, `inexact`, says which side. Ignoring it
/// rounds a decimal above the midpoint to the even neighbour (it must go up).
/// Random inputs land here about once in a million midpoints, so the values are
/// constructed (see `halfway_rows` in `scripts/decimal_golden.py`).
#[test]
fn a_decimal_a_hair_off_a_midpoint_is_decided_by_the_end_bit() {
    // Just above the midpoint: up to the odd neighbour, 0x...8341, not 0x...8340.
    let above = "4355614296620508741932764053194405E+7";
    assert_eq!(arith::to_binary64(&d(above)), 0x4860_0000_0000_8341);
    // And a second, in the same binade, with its own bits.
    let second = "4355614296641093363648506267077962E+7";
    for sign in ["", "-"] {
        for text in [above, second] {
            let text = format!("{sign}{text}");
            let want: f64 = text.replace("E+", "e").parse().unwrap();
            assert_eq!(arith::to_binary64(&d(&text)), want.to_bits(), "{text}");
        }
    }
}

#[test]
fn a_decimal_converts_to_a_double_at_the_edges() {
    let to = |t: &str| f64::from_bits(arith::to_binary64(&d(t)));
    assert_eq!(to("0.1"), 0.1);
    assert_eq!(to("1E+400"), f64::INFINITY);
    assert_eq!(to("-1E+400"), f64::NEG_INFINITY);
    assert_eq!(to("1E-400"), 0.0);
    assert_eq!(to("1E-320"), 1e-320);
    assert_eq!(to("-0"), -0.0);
    assert!(to("-0").is_sign_negative());
    assert!(to("NaN").is_nan());
    assert_eq!(to("Infinity"), f64::INFINITY);
    // The largest double and the first value that rounds to infinity.
    assert_eq!(to("1.7976931348623157E+308"), f64::MAX);
    assert_eq!(to("1.7976931348623158E+308"), f64::MAX);
    assert_eq!(to("1.797693134862315808E+308"), f64::INFINITY);
    // The smallest subnormal, and half of it (a tie, to even: zero).
    assert_eq!(to("4.9406564584124654E-324"), 5e-324);
    assert_eq!(to("2.4703282292062327208E-324"), 0.0);
    assert_eq!(to("2.4703282292062327209E-324"), 5e-324);
}

// ---------------------------------------------------------------------------
// The raw form
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn a_result_is_canonical_and_decodes_to_itself(a in finite_raw(), b in finite_raw()) {
        for r in [arith::add(&a, &b), arith::product(&[a, b])] {
            let bits = u128::from_le_bytes(pack_raw(r).bytes());
            prop_assert_eq!(decode_raw_bits(bits), r);
            if let Raw::Finite { coefficient, exponent, .. } = r {
                prop_assert!(coefficient <= MAX_COEFFICIENT);
                prop_assert!((MIN_EXPONENT..=MAX_EXPONENT).contains(&exponent));
            }
        }
    }

    #[test]
    fn the_raw_decoder_agrees_with_the_value_decoder(bits in any::<u128>()) {
        use super::{Decimal, decode_bits};
        let raw = decode_raw_bits(bits);
        match (decode_bits(bits), raw) {
            (Decimal::Nan, Raw::Nan) => {}
            (Decimal::PosInfinity, Raw::Inf { negative: false }) => {}
            (Decimal::NegInfinity, Raw::Inf { negative: true }) => {}
            (Decimal::Zero, Raw::Finite { coefficient: 0, .. }) => {}
            (
                Decimal::Finite { negative, coefficient, exponent },
                Raw::Finite { negative: n, coefficient: c, exponent: e },
            ) => prop_assert_eq!((negative, coefficient, exponent), (n, c, e)),
            (value, raw) => prop_assert!(false, "{:?} against {:?} for {:x}", value, raw, bits),
        }
    }
}

#[test]
fn the_raw_decoder_keeps_a_zeros_sign_and_exponent() {
    assert_eq!(d("-0"), fin(true, 0, 0));
    assert_eq!(d("0E-100"), fin(false, 0, -100));
    assert_eq!(d("0.00"), fin(false, 0, -2));
    // A non-canonical coefficient reads as a zero with the encoded exponent.
    let over = ((1u128 << 113) - 1) | (6175u128 << 113);
    assert_eq!(decode_raw_bits(over), fin(false, 0, -1));
    // The large-coefficient form takes its exponent from bits 124..111, not
    // from the normal form's 126..113.
    let large = (0b11u128 << 125) | (6177u128 << 111);
    assert_eq!(decode_raw_bits(large), fin(false, 0, 1));
}

// ---------------------------------------------------------------------------
// Against the comparator
// ---------------------------------------------------------------------------

/// The exact value of a finite double as `(numerator, denominator)`.
fn double_value(v: f64) -> (BigInt, BigInt) {
    let bits = v.to_bits();
    let biased = ((bits >> 52) & 0x7FF) as i64;
    let fraction = bits & ((1u64 << 52) - 1);
    let (m, f) =
        if biased == 0 { (fraction, -1074i64) } else { (fraction | 1 << 52, biased - 1075) };
    let n = BigInt::from(m);
    let n = if bits >> 63 == 1 { -n } else { n };
    if f >= 0 {
        (n << (f as usize), BigInt::from(1))
    } else {
        (n, BigInt::from(1) << ((-f) as usize))
    }
}

mod against_the_comparator {
    use super::*;
    use crate::canonical_cmp;
    use bson::Bson;

    fn decimal(raw: Raw) -> Bson {
        Bson::Decimal128(pack_raw(raw))
    }

    /// The decimals next to `raw` in its own cohort's coefficient line:
    /// `±1` and `±2` units of the last place.
    fn neighbours(raw: Raw) -> Vec<Raw> {
        let Raw::Finite { negative, coefficient, exponent } = raw else { return vec![] };
        let mut out = Vec::new();
        for delta in [-2i128, -1, 1, 2] {
            let signed = if negative { -(coefficient as i128) } else { coefficient as i128 };
            let moved = signed + delta;
            let c = moved.unsigned_abs();
            if c <= MAX_COEFFICIENT {
                out.push(fin(moved < 0, c, exponent));
            }
        }
        out
    }

    proptest! {
        #![proptest_config(config(4000))]

        /// **R1 never reverses an order the comparator gives.** For every
        /// double `d` and decimal `x`, `d < x` means `R1(d) <= x`, and the
        /// mirror. The shortest round-trip decimal and 15 digits both break it
        /// (`0.3` against `0.29999999999999999`), which is why R1 is the
        /// exact value rounded once.
        #[test]
        fn r1_never_reverses_an_order_the_comparator_gives(v in double_cases()) {
            prop_assume!(v.is_finite());
            let (r1, _) = arith::from_binary64(v.to_bits());
            for x in neighbours(r1) {
                let double = Bson::Double(v);
                let against = canonical_cmp(&double, &decimal(x));
                let after = canonical_cmp(&decimal(r1), &decimal(x));
                match against {
                    Ordering::Less => prop_assert_ne!(after, Ordering::Greater, "{:e} against {}", v, show(&x)),
                    Ordering::Greater => prop_assert_ne!(after, Ordering::Less, "{:e} against {}", v, show(&x)),
                    Ordering::Equal => prop_assert_eq!(after, Ordering::Equal, "{:e} against {}", v, show(&x)),
                }
            }
        }

        /// A result is a canonical decimal the key encoder takes whatever its
        /// exponent, and its key orders as the comparator orders (the keys are
        /// cohort-independent, so `2.30` and `2.3` are one key).
        #[test]
        fn the_key_of_a_result_orders_as_the_comparator_does((a, b) in pair(), (c, d) in pair()) {
            let results = [arith::add(&a, &b), arith::product(&[a, b]), arith::sub(&c, &d)];
            for x in results {
                for y in results {
                    let (bx, by) = (decimal(x), decimal(y));
                    let (kx, ky) = (crate::keyenc::encode(&bx).unwrap(), crate::keyenc::encode(&by).unwrap());
                    prop_assert_eq!(kx.cmp(&ky), canonical_cmp(&bx, &by), "{} against {}", show(&x), show(&y));
                }
            }
        }

        /// A decimal converts to the nearest double, by exact distances: the
        /// chosen double is no farther from the decimal than either of its
        /// neighbours, and when it is as far as one of them it is the even one.
        #[test]
        fn a_decimal_converts_to_the_nearest_double_by_exact_distances(
            negative in any::<bool>(),
            c in coefficient(),
            e in -330i16..=310,
        ) {
            prop_assume!(c != 0);
            let raw = fin(negative, c, e);
            let chosen = f64::from_bits(arith::to_binary64(&raw));
            prop_assume!(chosen.is_finite());
            let step = |v: f64, up: bool| -> f64 {
                let bits = v.to_bits();
                if v == 0.0 {
                    let tiny = f64::from_bits(1);
                    return if up { tiny } else { -tiny };
                }
                f64::from_bits(if (v > 0.0) == up { bits + 1 } else { bits - 1 })
            };
            let (nx, dx) = value(&raw);
            let distance = |v: f64| -> (BigInt, BigInt) {
                let (n, d) = double_value(v);
                // |x - v| = |nx·d - n·dx| / (dx·d)
                let top = (&nx * &d - n * &dx).magnitude().clone();
                (BigInt::from(top), dx.clone() * d)
            };
            let smaller_or_equal = |a: &(BigInt, BigInt), b: &(BigInt, BigInt)| (&a.0 * &b.1).cmp(&(&b.0 * &a.1));
            let mine = distance(chosen);
            for neighbour in [step(chosen, false), step(chosen, true)] {
                if !neighbour.is_finite() {
                    continue;
                }
                let theirs = distance(neighbour);
                match smaller_or_equal(&mine, &theirs) {
                    Ordering::Less => {}
                    Ordering::Equal => prop_assert_eq!(chosen.to_bits() & 1, 0, "a tie goes to the even double: {}", show(&raw)),
                    Ordering::Greater => prop_assert!(false, "{} is nearer to {:e} than to {:e}", show(&raw), neighbour, chosen),
                }
            }
        }
    }
}
