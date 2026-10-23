//! The exact-rational oracle the number comparator and the number keys are
//! checked against (tests only).
//!
//! It shares no code with either. A `Decimal128` is read from the `bson`
//! crate's own text (`Display`) by a parser written here, never through
//! [`crate::decimal::decode`]; a `Double` through `num-rational`'s exact
//! conversion; an integer as itself. Values are `num-rational` rationals and
//! are compared by cross-multiplication, never by `Ratio`'s own `Ord`, which
//! walks a continued fraction and, on two equal values with twenty-thousand-bit
//! denominators, recurses for as long as the fraction is.

use std::cmp::Ordering;
use std::sync::OnceLock;

use bson::Bson;
use num_bigint::{BigInt, Sign};
use num_rational::BigRational;
use proptest::prelude::*;

use crate::decimal::{self, MAX_COEFFICIENT, MAX_EXPONENT, MIN_EXPONENT};

/// A `Decimal128` as its text spells it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum Text {
    Nan,
    Infinity {
        negative: bool,
    },
    /// `±coefficient × 10^exponent`, exactly as written (a zero keeps its
    /// exponent and sign).
    Finite {
        negative: bool,
        coefficient: BigInt,
        exponent: i64,
    },
}

/// Parse the text `bson` prints for a `Decimal128`: `NaN`, `[-]Infinity`,
/// plain (`-12.50`, `0.000123`) or scientific (`1.05E+3`, `5E-6176`).
pub(crate) fn parse_text(text: &str) -> Text {
    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    match rest {
        "NaN" => return Text::Nan,
        "Infinity" => return Text::Infinity { negative },
        _ => {}
    }
    let (digits, exponent) = match rest.split_once(['E', 'e']) {
        Some((digits, exponent)) => (digits, exponent.parse::<i64>().expect("an exponent")),
        None => (rest, 0),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    assert!(
        !whole.is_empty() && (whole.bytes().chain(fraction.bytes())).all(|b| b.is_ascii_digit()),
        "not a decimal: {text:?}"
    );
    let coefficient: BigInt = format!("{whole}{fraction}").parse().expect("digits");
    Text::Finite { negative, coefficient, exponent: exponent - fraction.len() as i64 }
}

/// A number's exact value: one class per special, one rational per finite value.
#[derive(Clone, Debug)]
pub(crate) enum Exact {
    Nan,
    NegInfinity,
    /// Zero included, with a positive denominator.
    Finite(BigRational),
    PosInfinity,
}

/// `10^k`, from a table built once per test binary: a debug build spends
/// milliseconds on each `10^6176` otherwise.
pub(crate) fn pow10(k: u32) -> BigInt {
    static TABLE: OnceLock<Vec<BigInt>> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut power = BigInt::from(1);
        (0..=6400)
            .map(|_| {
                let this = power.clone();
                power *= 10u32;
                this
            })
            .collect()
    });
    table.get(k as usize).cloned().unwrap_or_else(|| BigInt::from(10u32).pow(k))
}

/// `±c × 10^e` as an unreduced rational with a positive denominator. Reducing
/// would cost a twenty-thousand-bit gcd and buy nothing: every comparison
/// cross-multiplies.
pub(crate) fn scaled(negative: bool, coefficient: BigInt, exponent: i64) -> BigRational {
    if coefficient.sign() == Sign::NoSign {
        // A zero's exponent can be anything its bits say; its value is zero.
        return BigRational::from_integer(coefficient);
    }
    let sign = if negative { BigInt::from(-1) } else { BigInt::from(1) };
    let magnitude = u32::try_from(exponent.unsigned_abs()).expect("an exponent in range");
    if exponent >= 0 {
        BigRational::new_raw(sign * coefficient * pow10(magnitude), BigInt::from(1))
    } else {
        BigRational::new_raw(sign * coefficient, pow10(magnitude))
    }
}

pub(crate) fn exact(value: &Bson) -> Option<Exact> {
    let finite = |r: BigRational| Some(Exact::Finite(r));
    match value {
        Bson::Int32(v) => finite(BigRational::from_integer(BigInt::from(*v))),
        Bson::Int64(v) => finite(BigRational::from_integer(BigInt::from(*v))),
        Bson::Double(v) if v.is_nan() => Some(Exact::Nan),
        Bson::Double(v) if *v == f64::INFINITY => Some(Exact::PosInfinity),
        Bson::Double(v) if *v == f64::NEG_INFINITY => Some(Exact::NegInfinity),
        Bson::Double(v) => finite(BigRational::from_float(*v).expect("a finite double")),
        Bson::Decimal128(d) => match parse_text(&d.to_string()) {
            Text::Nan => Some(Exact::Nan),
            Text::Infinity { negative: true } => Some(Exact::NegInfinity),
            Text::Infinity { negative: false } => Some(Exact::PosInfinity),
            Text::Finite { negative, coefficient, exponent } => {
                finite(scaled(negative, coefficient, exponent))
            }
        },
        _ => None,
    }
}

fn class(value: &Exact) -> u8 {
    match value {
        Exact::Nan => 0,
        Exact::NegInfinity => 1,
        Exact::Finite(r) => match r.numer().sign() {
            Sign::Minus => 2,
            Sign::NoSign => 3,
            Sign::Plus => 4,
        },
        Exact::PosInfinity => 5,
    }
}

/// Compare two rationals with positive denominators by cross-multiplication.
pub(crate) fn cmp_rational(a: &BigRational, b: &BigRational) -> Ordering {
    assert!(a.denom().sign() == Sign::Plus && b.denom().sign() == Sign::Plus);
    (a.numer() * b.denom()).cmp(&(b.numer() * a.denom()))
}

pub(crate) fn cmp_exact(a: &Exact, b: &Exact) -> Ordering {
    match (a, b) {
        (Exact::Finite(x), Exact::Finite(y)) => cmp_rational(x, y),
        _ => class(a).cmp(&class(b)),
    }
}

/// The canonical order of two numbers, by their exact values.
pub(crate) fn oracle_cmp(a: &Bson, b: &Bson) -> Ordering {
    cmp_exact(&exact(a).expect("a number"), &exact(b).expect("a number"))
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

/// A deterministic generator, so a corpus is the same corpus on every run.
pub(crate) struct SplitMix(pub(crate) u64);

impl SplitMix {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub(crate) fn below(&mut self, n: u128) -> u128 {
        ((u128::from(self.next()) << 64) | u128::from(self.next())) % n
    }

    pub(crate) fn coefficient(&mut self) -> u128 {
        // Uniform over the digit counts, then over the values of that length.
        let digits = 1 + self.below(34) as u32;
        let low = 10u128.pow(digits - 1);
        low + self.below(10u128.pow(digits) - low)
    }
}

pub(crate) fn dec(negative: bool, coefficient: u128, exponent: i16) -> Bson {
    Bson::Decimal128(decimal::pack(negative, coefficient, exponent))
}

pub(crate) fn raw(bits: u128) -> Bson {
    Bson::Decimal128(bson::Decimal128::from_bytes(bits.to_le_bytes()))
}

/// Coefficients concentrated where digit counts, powers of two and the limit
/// change.
pub(crate) fn any_coefficient() -> BoxedStrategy<u128> {
    prop_oneof![
        3 => 1..=MAX_COEFFICIENT,
        1 => 1..1000u128,
        1 => (0u32..34).prop_map(|k| 10u128.pow(k)),
        1 => (1u32..=34).prop_map(|k| 10u128.pow(k) - 1),
        1 => (0u32..34).prop_map(|k| 10u128.pow(k) + 1),
        1 => (0u32..113).prop_map(|k| 1u128 << k),
        1 => (1u32..113).prop_map(|k| (1u128 << k) - 1),
        1 => (0u32..113).prop_map(|k| (1u128 << k) + 1),
        1 => Just(MAX_COEFFICIENT),
    ]
    .boxed()
}

/// Exponents over the whole range, near zero, around where a decimal can meet
/// a double (`-357..=308`), and at the edges of each.
pub(crate) fn any_exponent() -> BoxedStrategy<i16> {
    prop_oneof![
        2 => MIN_EXPONENT..=MAX_EXPONENT,
        2 => -40i16..=40,
        2 => -400i16..=330,
        1 => prop::sample::select(vec![
            MIN_EXPONENT,
            MIN_EXPONENT + 33,
            -6143,
            -359,
            -358,
            -357,
            -356,
            -341,
            -340,
            -324,
            -308,
            -49,
            -48,
            -1,
            0,
            1,
            22,
            23,
            27,
            28,
            307,
            308,
            309,
            MAX_EXPONENT - 33,
            MAX_EXPONENT,
        ]),
    ]
    .boxed()
}

pub(crate) fn any_finite_decimal() -> BoxedStrategy<Bson> {
    (any::<bool>(), any_coefficient(), any_exponent()).prop_map(|(n, c, e)| dec(n, c, e)).boxed()
}

/// Every special spelling: NaN of either sign, quiet or signalling, with a
/// payload; an infinity with payload bits; a zero of any sign and exponent; a
/// coefficient above `10^34 − 1`; and the `11` large-coefficient form.
pub(crate) fn any_special_decimal() -> BoxedStrategy<Bson> {
    let sign = any::<bool>().prop_map(|n| u128::from(n) << 127);
    prop_oneof![
        (sign.clone(), any::<u128>()).prop_map(|(s, p)| raw(s | (0b11111 << 122) | (p >> 6))),
        (sign.clone(), any::<u128>()).prop_map(|(s, p)| raw(s | (0b11110 << 122) | (p >> 6))),
        (sign.clone(), 0u128..0x3000).prop_map(|(s, e)| raw(s | (e << 113))),
        (sign.clone(), 0u128..0x3000, (MAX_COEFFICIENT + 1)..(1u128 << 113))
            .prop_map(|(s, e, c)| raw(s | (e << 113) | c)),
        (sign, 0b11000u128..=0b11101, any::<u128>())
            .prop_map(|(s, top, tail)| raw(s | (top << 122) | (tail >> 6))),
    ]
    .boxed()
}

/// The 34-digit decimals nearest a finite nonzero double, and a few either
/// side: the values a comparison or a key must tell from the double itself.
pub(crate) fn decimal_near_double(d: f64, offset: i64) -> Option<Bson> {
    if !d.is_finite() || d == 0.0 {
        return None;
    }
    let r = BigRational::from_float(d.abs()).unwrap();
    // A first guess from the float, corrected exactly below.
    let mut e = d.abs().log10().floor() as i64 - 33;
    loop {
        let c = (&r / scaled(false, BigInt::from(1), e)).floor().to_integer();
        if c >= pow10(34) {
            e += 1;
        } else if c < pow10(33) {
            e -= 1;
        } else {
            let c = u128::try_from(c + offset).ok()?;
            let e = i16::try_from(e).ok()?;
            let ok =
                (1..=MAX_COEFFICIENT).contains(&c) && (MIN_EXPONENT..=MAX_EXPONENT).contains(&e);
            return ok.then(|| dec(d < 0.0, c, e));
        }
    }
}

/// Doubles concentrated on the edges: the specials, the subnormals, powers of
/// two and of ten, and the integers a double can no longer count past.
pub(crate) fn any_double() -> BoxedStrategy<f64> {
    prop_oneof![
        3 => any::<f64>(),
        1 => prop::sample::select(vec![
            0.0,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
            5e-324,
            -5e-324,
            f64::MAX,
            f64::MIN,
            0.1,
            0.5,
            1.0,
            1e-3,
            9_007_199_254_740_992.0,
            9_223_372_036_854_775_808.0,
            -9_223_372_036_854_775_808.0,
        ]),
        1 => (-1074i32..=1023).prop_map(power_of_two),
        1 => (-323i32..=308).prop_map(|k| format!("1e{k}").parse::<f64>().unwrap()),
        1 => (any::<i64>()).prop_map(|v| v as f64),
    ]
    .boxed()
}

/// `2^k` as a double, built from its bits: `2f64.powi(k)` is `0.0` below
/// `2^-1023`, where it divides by an overflowed `2^|k|`.
pub(crate) fn power_of_two(k: i32) -> f64 {
    assert!((-1074..=1023).contains(&k));
    if k >= -1022 {
        f64::from_bits(((k + 1023) as u64) << 52)
    } else {
        f64::from_bits(1u64 << (k + 1074))
    }
}

/// Every kind of number, with decimals concentrated on the doubles' edges.
///
/// Every strategy here is boxed, and so is every arm of this union: unboxed,
/// a union's value tree holds each arm's tree inline, and a debug build built
/// those nested unions on the test thread's stack, near enough to its 2 MiB
/// that a rare input overflowed it.
pub(crate) fn any_number() -> BoxedStrategy<Bson> {
    prop_oneof![
        1 => any::<i32>().prop_map(Bson::Int32).boxed(),
        1 => any::<i64>().prop_map(Bson::Int64).boxed(),
        1 => prop::sample::select(vec![i64::MIN, i64::MAX, 0, 1, -1, 1 << 53, (1 << 53) + 1])
            .prop_map(Bson::Int64).boxed(),
        2 => any_double().prop_map(Bson::Double).boxed(),
        3 => any_finite_decimal(),
        1 => any_special_decimal(),
        2 => (any_double(), -2i64..=2)
            .prop_filter_map("a decimal near the double", |(d, k)| decimal_near_double(d, k)).boxed(),
    ]
    .boxed()
}
