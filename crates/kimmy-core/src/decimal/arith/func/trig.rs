//! The `Decimal128` trigonometric and hyperbolic functions: `sin`, `cos`, `tan`, `asin`,
//! `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh` and `atanh`, in integers alone
//! and in the context of ADR-252, as the child of [`super`] that shares its fixed point.
//!
//! **Rounded once.** Each result is computed to far more digits than the format holds and
//! cut to 34 by the arithmetic's own rounding, half to even, so it is the 34-digit decimal
//! nearest the exact value whenever the exact value is not within `10^-75` of a midpoint.
//! The coefficient keeps its 34 digits (a trailing zero is not stripped) and an answer that
//! is exactly one is spelled `1`, as the reference spells it.
//!
//! **The exact points.** The identities (`sin 0`, `cos 0`, `asin 0`, `acosh 1`, ...) answer
//! the cohort the reference answers, which is not one rule (`sin 0E+3` is `0E+3`, `tan 0E+3`
//! is `0E-37`, `asinh 0E+3` is `0E-6176`); each function's own is in its body. A tiny operand
//! (an adjusted exponent of -40 or less) of a function that is the identity there answers the
//! operand, padded to 34 digits.
//!
//! **Large operands.** `sin`, `cos` and `tan` reduce the operand modulo `pi / 2` in a fixed
//! point as wide as the operand is large (`pi` to 114 digits more than the operand's
//! exponent, cut from the 6,400 places that are embedded: see [`pi_constant`]), so an operand
//! of `1E+6000` is reduced exactly. The other
//! functions work at [`S`] places, and the large operands of `asinh` and `acosh` are
//! `ln(2x)`, to which the correction `1 / 4x²` cannot reach the 34th digit.
//!
//! **Domain.** `sin`, `cos` and `tan` refuse an infinity; `asin` and `acos` a magnitude above
//! one; `acosh` a value below one; `atanh` a magnitude above one. A NaN answers the NaN. The
//! caller words the refusal.

use std::cmp::Ordering;
use std::sync::OnceLock;

use super::{
    Domain, ETINY, Fx, INFINITY, LIMB_DIGITS, Mag, NAN, PREC, S, SL, SQRT_TEN_LEAD, ZERO, consts,
    digits_u128, exp_fx, exp_limit, fx_div, ln_fx, ln_scaled, m_add, m_cmp, m_div_rem, m_div_small,
    m_isqrt, m_mul, m_sub, pow10, rounded, signed_zero, strip, to_fx,
};
use crate::decimal::Raw;

/// The adjusted exponent at and below which `x - x^3/6` and its kin are `x` to 34 digits.
const TINY: i64 = -40;

/// The adjusted exponent from which `asinh` and `acosh` are `ln(2x)`.
const HUGE: i64 = 40;

/// The magnitude from which `sinh` and `cosh` are `e^x / 2` and `tanh` is one.
const LARGE: u64 = 50;

// ---------------------------------------------------------------------------
// Fixed points of any width
// ---------------------------------------------------------------------------

/// A fixed-point scale of `limbs` limbs: a magnitude `N` stands for `N / 10^(19 limbs)`.
#[derive(Clone, Copy)]
struct Scale {
    limbs: usize,
}

impl Scale {
    fn standard() -> Scale {
        Scale { limbs: SL }
    }

    /// The scale that holds an operand of the adjusted exponent `adjusted` and 114 places
    /// past the point beyond it.
    fn reaching(adjusted: i64) -> Scale {
        let extra = adjusted.max(0) as usize + 1;
        Scale { limbs: SL + extra.div_ceil(LIMB_DIGITS as usize) }
    }

    fn places(self) -> u32 {
        self.limbs as u32 * LIMB_DIGITS
    }

    fn one(self) -> Mag {
        pow10(self.places())
    }

    /// `⌊a · b / 10^places⌋`.
    fn mul(self, a: &Mag, b: &Mag) -> Mag {
        let product = m_mul(a, b);
        if product.0.len() <= self.limbs {
            return Mag::zero();
        }
        Mag(product.0[self.limbs..].to_vec())
    }

    /// `⌊a · 10^places / b⌋` for `b` nonzero.
    fn div(self, a: &Mag, b: &Mag) -> Mag {
        let mut numerator = a.clone();
        if !numerator.is_zero() {
            numerator.0.splice(0..0, std::iter::repeat_n(0, self.limbs));
        }
        m_div_rem(&numerator, b).0
    }

    /// `⌊√a⌋` in this scale.
    fn root_of(self, a: &Mag) -> Mag {
        let mut n = a.clone();
        n.mul_pow10(self.places());
        m_isqrt(&n)
    }

    /// `coefficient · 10^exponent`, truncated.
    fn of_decimal(self, coefficient: u128, exponent: i64) -> Mag {
        let mut m = Mag::from_u128(coefficient);
        let shift = exponent + i64::from(self.places());
        if shift >= 0 {
            m.mul_pow10(shift as u32);
            m
        } else if -shift > i64::from(PREC) + 1 {
            Mag::zero()
        } else {
            m.split_digits((-shift) as u32).0
        }
    }

    /// `pi` cut to this scale, from the embedded digits: it is computed by no call, and the
    /// widest scale a Decimal128 operand reaches (6,270 places) is inside them. Correct to the
    /// last place, below it by less than a unit.
    fn pi(self) -> Mag {
        debug_assert!(self.places() <= PI_PLACES, "a scale wider than the embedded pi");
        let wide = pi_constant();
        let cut = PI_PLACES.saturating_sub(self.places());
        if cut == 0 { wide.clone() } else { wide.split_digits(cut).0 }
    }

    fn half_pi(self) -> Mag {
        m_div_small(&self.pi(), 2).0
    }
}

/// `pi` to [`PI_PLACES`] places: the digits `3` and the 6,400 after the point, generated
/// off-line by `gen_pi.py` (the Chudnovsky series, integer binary splitting) and checked by a
/// test against a fresh Machin build at the widest width. That is more than the 6,270 places
/// the operand of the largest exponent needs (`Scale::reaching(6144)` is 330 limbs), so no
/// call builds `pi`, and none stalls where the request deadline cannot see it.
const PI_DIGITS: &str = include_str!("pi.txt");

/// The places of [`PI_DIGITS`] after the point.
const PI_PLACES: u32 = 6400;

/// Times the embedded `pi` was read into limbs, which a test holds to one for the process.
#[cfg(test)]
static PI_PARSES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// `floor(pi · 10^PI_PLACES)` as limbs, read from [`PI_DIGITS`] once.
fn pi_constant() -> &'static Mag {
    static PI: OnceLock<Mag> = OnceLock::new();
    PI.get_or_init(|| {
        #[cfg(test)]
        PI_PARSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let digits: Vec<u8> = PI_DIGITS.bytes().filter(u8::is_ascii_digit).collect();
        let mut limbs = Vec::with_capacity(digits.len() / LIMB_DIGITS as usize + 1);
        let mut end = digits.len();
        while end > 0 {
            let start = end.saturating_sub(LIMB_DIGITS as usize);
            limbs.push(digits[start..end].iter().fold(0u64, |n, d| n * 10 + u64::from(d - b'0')));
            end = start;
        }
        let mut pi = Mag(limbs);
        pi.trim();
        pi
    })
}

#[cfg(test)]
#[path = "trig_tests.rs"]
mod trig_tests;

/// The scale the series of `sin` and `cos` run at, once the operand is reduced: the standard
/// scale and a limb. The reduction leaves an absolute error of about `10^-115`, and a result
/// is never below about `10^-40` (the operand is a 34-digit decimal), so 133 places keep
/// every digit that rounding to 34 needs.
const SERIES: Scale = Scale { limbs: SL + 1 };

/// The ratio of two operands, in adjusted exponent, beyond which `atan2` stops widening its
/// scale for the ratio. For a positive `x` the answer is then `y / x` itself, rounded once
/// (`atan(r) = r - r^3/3`, and `r^2` is below `10^-158`); for a negative `x` the angle is
/// computed as ever, `pi` less the arctangent, at a scale capped at this many digits of ratio
/// (11 limbs), which keeps the cost bounded and loses nothing: a ratio below `10^-80` does not
/// reach the 34th digit of a result near `pi`.
const DIRECT_RATIO: i64 = 80;

fn negate(x: &Fx) -> Fx {
    Fx { neg: !x.neg && !x.mag.is_zero(), mag: x.mag.clone() }
}

// ---------------------------------------------------------------------------
// The kernels
// ---------------------------------------------------------------------------

/// `(sin r, cos r)` for `r` in `[0, pi/2]`, by the Taylor series.
fn sin_cos(s: Scale, r: &Mag) -> (Mag, Mag) {
    let r2 = s.mul(r, r);
    let (mut sp, mut sn) = (r.clone(), Mag::zero());
    let mut term = r.clone();
    let (mut k, mut negative) = (1u64, true);
    loop {
        term = m_div_small(&s.mul(&term, &r2), 2 * k * (2 * k + 1)).0;
        if term.is_zero() {
            break;
        }
        if negative {
            sn = m_add(&sn, &term);
        } else {
            sp = m_add(&sp, &term);
        }
        negative = !negative;
        k += 1;
    }
    let one = s.one();
    let (mut cp, mut cn) = (one.clone(), Mag::zero());
    let mut term = one;
    let (mut k, mut negative) = (1u64, true);
    loop {
        term = m_div_small(&s.mul(&term, &r2), (2 * k - 1) * (2 * k)).0;
        if term.is_zero() {
            break;
        }
        if negative {
            cn = m_add(&cn, &term);
        } else {
            cp = m_add(&cp, &term);
        }
        negative = !negative;
        k += 1;
    }
    (m_sub(&sp, &sn), m_sub(&cp, &cn))
}

/// `atan x` for `x >= 0`: above one it is `pi/2 - atan(1/x)`, and the argument is halved
/// four times (`x / (1 + √(1 + x²))`) before the series, which is then multiplied by 16.
fn atan_pos(s: Scale, x: &Mag) -> Mag {
    let one = s.one();
    if m_cmp(x, &one) == Ordering::Greater {
        let inverse = atan_pos(s, &s.div(&one, x));
        return m_sub(&s.half_pi(), &inverse);
    }
    let mut t = x.clone();
    for _ in 0..4 {
        let root = s.root_of(&m_add(&one, &s.mul(&t, &t)));
        t = s.div(&t, &m_add(&one, &root));
    }
    let t2 = s.mul(&t, &t);
    let (mut sum_p, mut sum_n) = (t.clone(), Mag::zero());
    let mut power = t;
    let (mut k, mut negative) = (3u64, true);
    loop {
        power = s.mul(&power, &t2);
        let term = m_div_small(&power, k).0;
        if term.is_zero() {
            break;
        }
        if negative {
            sum_n = m_add(&sum_n, &term);
        } else {
            sum_p = m_add(&sum_p, &term);
        }
        negative = !negative;
        k += 2;
    }
    let mut sum = m_sub(&sum_p, &sum_n);
    sum.mul_small(16);
    sum
}

/// `ln v` for a positive fixed point `v` at [`S`] places.
fn ln_fixed(v: &Mag) -> Fx {
    let digits = i64::from(v.digits());
    let adjusted = digits - 1 - i64::from(S);
    let lead = if digits > i64::from(PREC) {
        v.split_digits((digits - i64::from(PREC)) as u32).0.to_u128().expect("34 digits")
    } else {
        v.to_u128().expect("at most 34 digits") * 10u128.pow((i64::from(PREC) - digits) as u32)
    };
    // `v = m · 10^q` with `m` in [0.316, 3.16).
    let q = if lead < SQRT_TEN_LEAD { adjusted } else { adjusted + 1 };
    let mut m = v.clone();
    if q > 0 {
        m = m.split_digits(q as u32).0;
    } else if q < 0 {
        m.mul_pow10(q.unsigned_abs() as u32);
    }
    ln_scaled(m, q)
}

/// `e^x` for a fixed point `x >= 0` at [`S`] places that is below the limit of
/// [`exp_limit`], as a fixed point at [`S`] places.
fn exp_standard(x: &Mag) -> Mag {
    let (e, k) = exp_fx(false, x);
    if k >= 0 {
        let mut e = e;
        e.mul_pow10(k as u32);
        e
    } else {
        e.split_digits(k.unsigned_abs() as u32).0
    }
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

/// A fixed-point result of `places` places (inexact: the true value is a hair above) cut to
/// 34 digits, the coefficient keeping all of them; an answer of exactly one is `1`.
fn result(negative: bool, mut mag: Mag, places: u32) -> Raw {
    let digits = mag.digits();
    if digits == 0 {
        return signed_zero(negative);
    }
    let mut exponent = -i64::from(places);
    // [`rounded`] cuts a sticky result: give it more than 34 digits to cut.
    if digits < 40 {
        mag.mul_pow10(40 - digits);
        exponent -= i64::from(40 - digits);
    }
    let raw = rounded(negative, mag, exponent, true, exponent);
    match raw {
        Raw::Finite {
            coefficient: 1_000_000_000_000_000_000_000_000_000_000_000,
            exponent: -33,
            negative,
        } => Raw::Finite { negative, coefficient: 1, exponent: 0 },
        other => other,
    }
}

fn fx_result(x: Fx, places: u32) -> Raw {
    result(x.neg, x.mag, places)
}

/// `coefficient · 10^exponent` padded to 34 digits, the exponent going down by the padding
/// (to the smallest exponent at most).
fn padded(negative: bool, coefficient: u128, exponent: i64) -> Raw {
    let pad = (i64::from(PREC) - i64::from(digits_u128(coefficient))).min(exponent - ETINY).max(0);
    Raw::Finite {
        negative,
        coefficient: coefficient * 10u128.pow(pad as u32),
        exponent: (exponent - pad) as i16,
    }
}

/// What an operand is, for the identity and the special values.
enum Class {
    Nan,
    Infinite(bool),
    Zero,
    Number { negative: bool, coefficient: u128, exponent: i64, adjusted: i64 },
}

fn classify(x: &Raw) -> Class {
    match *x {
        Raw::Nan => Class::Nan,
        Raw::Inf { negative } => Class::Infinite(negative),
        Raw::Finite { coefficient: 0, .. } => Class::Zero,
        Raw::Finite { negative, coefficient, exponent } => {
            let exponent = i64::from(exponent);
            Class::Number {
                negative,
                coefficient,
                exponent,
                adjusted: i64::from(digits_u128(coefficient)) - 1 + exponent,
            }
        }
    }
}

/// A zero of the sign and exponent of `x`, `shift` places further down: the zero the
/// reference answers for `tan`, `asin` and `sinh` (40) and the conversions.
fn zero_below(x: &Raw, shift: i64) -> Raw {
    match *x {
        Raw::Finite { negative, exponent, .. } => Raw::Finite {
            negative,
            coefficient: 0,
            exponent: (i64::from(exponent) - shift).max(ETINY) as i16,
        },
        other => other,
    }
}

/// A zero of the sign of `x` at the smallest exponent.
fn zero_tiny(x: &Raw) -> Raw {
    match *x {
        Raw::Finite { negative, .. } => {
            Raw::Finite { negative, coefficient: 0, exponent: ETINY as i16 }
        }
        other => other,
    }
}

// ---------------------------------------------------------------------------
// sin, cos, tan
// ---------------------------------------------------------------------------

/// `|x|` reduced: the fixed-point scale, the quadrant of `|x|` (its multiple of `pi/2`
/// modulo 4) and the remainder in `[0, pi/2)`.
fn reduce(coefficient: u128, exponent: i64, adjusted: i64) -> (Scale, u8, Mag) {
    let s = Scale::reaching(adjusted);
    let x = s.of_decimal(coefficient, exponent);
    let (quotient, remainder) = m_div_rem(&x, &s.half_pi());
    // `10^19` is a multiple of 4: the low limb decides the quadrant.
    let quadrant = quotient.0.first().map_or(0, |&limb| (limb % 4) as u8);
    // The remainder is the operand's distance from a multiple of `pi/2`, wherever the operand
    // is, so the series on it is the one a small operand has: cut to its scale.
    if s.limbs <= SERIES.limbs {
        return (s, quadrant, remainder);
    }
    let cut = (s.limbs - SERIES.limbs) as u32 * LIMB_DIGITS;
    (SERIES, quadrant, remainder.split_digits(cut).0)
}

/// `(sin x, cos x)` as signed fixed points, for `x = coefficient · 10^exponent > 0`.
fn sin_cos_of(coefficient: u128, exponent: i64, adjusted: i64) -> (Scale, Fx, Fx) {
    let (s, quadrant, r) = reduce(coefficient, exponent, adjusted);
    let (sin, cos) = sin_cos(s, &r);
    let (sin_x, cos_x) = match quadrant {
        0 => (Fx::new(false, sin), Fx::new(false, cos)),
        1 => (Fx::new(false, cos), Fx::new(true, sin)),
        2 => (Fx::new(true, sin), Fx::new(true, cos)),
        _ => (Fx::new(true, cos), Fx::new(false, sin)),
    };
    (s, sin_x, cos_x)
}

/// `sin x`. A zero is itself.
pub fn sin(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(_) => Err(Domain),
        Class::Zero => Ok(*x),
        Class::Number { negative, coefficient, exponent, adjusted } => {
            if adjusted <= TINY {
                return Ok(padded(negative, coefficient, exponent));
            }
            let (s, sin_x, _) = sin_cos_of(coefficient, exponent, adjusted);
            let signed = if negative { negate(&sin_x) } else { sin_x };
            Ok(fx_result(signed, s.places()))
        }
    }
}

/// `cos x`. A zero is one, in the cohort of 34 digits.
pub fn cos(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(_) => Err(Domain),
        Class::Zero => Ok(one_padded()),
        Class::Number { coefficient, exponent, adjusted, .. } => {
            // The reference spells the one of a tiny operand with 34 digits only from
            // `1E-55` down; above that it is `1`.
            if adjusted <= -55 {
                return Ok(one_padded());
            }
            let (s, _, cos_x) = sin_cos_of(coefficient, exponent, adjusted);
            Ok(fx_result(cos_x, s.places()))
        }
    }
}

/// One with 34 digits: `1.000000000000000000000000000000000`.
fn one_padded() -> Raw {
    Raw::Finite {
        negative: false,
        coefficient: 1_000_000_000_000_000_000_000_000_000_000_000,
        exponent: -33,
    }
}

/// `tan x`. A zero is a zero 40 places finer.
pub fn tan(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(_) => Err(Domain),
        Class::Zero => Ok(zero_below(x, 40)),
        Class::Number { negative, coefficient, exponent, adjusted } => {
            if adjusted <= TINY {
                return Ok(padded(negative, coefficient, exponent));
            }
            let (s, sin_x, cos_x) = sin_cos_of(coefficient, exponent, adjusted);
            if cos_x.mag.is_zero() {
                return Ok(Raw::Inf { negative: sin_x.neg != negative });
            }
            let ratio = Fx::new(sin_x.neg != cos_x.neg, s.div(&sin_x.mag, &cos_x.mag));
            let signed = if negative { negate(&ratio) } else { ratio };
            Ok(fx_result(signed, s.places()))
        }
    }
}

// ---------------------------------------------------------------------------
// asin, acos, atan
// ---------------------------------------------------------------------------

/// `pi/2` as a 34-digit decimal.
fn half_pi_raw() -> Raw {
    let s = Scale::standard();
    result(false, s.half_pi(), s.places())
}

/// `asin |x|` for `|x|` in `(0, 1]`, at [`S`] places.
fn asin_pos(coefficient: u128, exponent: i64) -> Mag {
    let s = Scale::standard();
    let x = s.of_decimal(coefficient, exponent);
    let one = s.one();
    if m_cmp(&x, &one) != Ordering::Less {
        return s.half_pi();
    }
    let root = s.root_of(&m_sub(&one, &s.mul(&x, &x)));
    atan_pos(s, &s.div(&x, &root))
}

/// Whether `|x| > 1`, for a number of the class.
fn above_one(coefficient: u128, exponent: i64, adjusted: i64) -> bool {
    adjusted > 0 || (adjusted == 0 && strip(coefficient, exponent) != (1, 0))
}

/// `asin x`. A zero is a zero 40 places finer.
pub fn asin(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(_) => Err(Domain),
        Class::Zero => Ok(zero_below(x, 40)),
        Class::Number { negative, coefficient, exponent, adjusted } => {
            if above_one(coefficient, exponent, adjusted) {
                return Err(Domain);
            }
            if adjusted <= TINY {
                return Ok(padded(negative, coefficient, exponent));
            }
            let s = Scale::standard();
            Ok(fx_result(Fx::new(negative, asin_pos(coefficient, exponent)), s.places()))
        }
    }
}

/// `acos x = pi/2 - asin x`. One is a zero of the smallest exponent.
pub fn acos(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(_) => Err(Domain),
        Class::Zero => Ok(half_pi_raw()),
        Class::Number { negative, coefficient, exponent, adjusted } => {
            if above_one(coefficient, exponent, adjusted) {
                return Err(Domain);
            }
            let s = Scale::standard();
            if !negative && strip(coefficient, exponent) == (1, 0) {
                return Ok(zero_tiny(x));
            }
            // A tiny operand's arcsine is below the last place of 34 digits of `pi/2`.
            let asin = if adjusted <= TINY {
                Fx::new(negative, s.of_decimal(coefficient, exponent))
            } else {
                Fx::new(negative, asin_pos(coefficient, exponent))
            };
            Ok(fx_result(Fx::new(false, s.half_pi()).plus(&negate(&asin)), s.places()))
        }
    }
}

/// `atan x`. A zero is itself; an infinity is `±pi/2`.
pub fn atan(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(negative) => {
            let half = half_pi_raw();
            Ok(match half {
                Raw::Finite { coefficient, exponent, .. } => {
                    Raw::Finite { negative, coefficient, exponent }
                }
                other => other,
            })
        }
        Class::Zero => Ok(*x),
        Class::Number { negative, coefficient, exponent, adjusted } => {
            if adjusted <= TINY {
                return Ok(padded(negative, coefficient, exponent));
            }
            let s = Scale::standard();
            let mag = if adjusted >= 100 {
                s.half_pi()
            } else {
                atan_pos(s, &s.of_decimal(coefficient, exponent))
            };
            Ok(fx_result(Fx::new(negative, mag), s.places()))
        }
    }
}

// ---------------------------------------------------------------------------
// sinh, cosh, tanh
// ---------------------------------------------------------------------------

/// `e^|x|` for a number below [`LARGE`] in magnitude, at [`S`] places.
fn exp_below_large(coefficient: u128, exponent: i64) -> Mag {
    exp_standard(&to_fx(coefficient, exponent))
}

/// Whether `|x| >= LARGE`.
fn at_least_large(coefficient: u128, exponent: i64, adjusted: i64) -> bool {
    if adjusted < 1 {
        return false;
    }
    if adjusted >= 2 {
        return true;
    }
    let mut limit = Mag::from_u64(LARGE);
    limit.mul_pow10(S);
    m_cmp(&to_fx(coefficient, exponent), &limit) != Ordering::Less
}

/// `e^|x| / 2`, which is `E · 10^(k - S) / 2` for `e^x = E · 10^(k - S)`, rounded:
/// `sinh` and `cosh` of a number from [`LARGE`] up, whose `e^-x` is below the last place.
/// `Infinity` past the largest number the format holds.
fn half_exp(negative: bool, coefficient: u128, exponent: i64, adjusted: i64) -> Raw {
    if adjusted > 4 {
        return Raw::Inf { negative };
    }
    let x = to_fx(coefficient, exponent);
    if m_cmp(&x, &exp_limit()) == Ordering::Greater {
        return Raw::Inf { negative };
    }
    let (mut e, k) = exp_fx(false, &x);
    e.mul_small(5);
    let mut exponent = k - i64::from(S) - 1;
    let digits = e.digits();
    if digits < 40 {
        e.mul_pow10(40 - digits);
        exponent -= i64::from(40 - digits);
    }
    rounded(negative, e, exponent, true, exponent)
}

/// `sinh x`. A zero is a zero 40 places finer.
pub fn sinh(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(_) => Ok(*x),
        Class::Zero => Ok(zero_below(x, 40)),
        Class::Number { negative, coefficient, exponent, adjusted } => {
            if adjusted <= TINY {
                return Ok(padded(negative, coefficient, exponent));
            }
            if at_least_large(coefficient, exponent, adjusted) {
                return Ok(half_exp(negative, coefficient, exponent, adjusted));
            }
            let s = Scale::standard();
            let e = exp_below_large(coefficient, exponent);
            let inverse = s.div(&s.one(), &e);
            let diff = m_div_small(&m_sub(&e, &inverse), 2).0;
            Ok(fx_result(Fx::new(negative, diff), s.places()))
        }
    }
}

/// `cosh x`. A zero is one with 34 digits.
pub fn cosh(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(_) => Ok(INFINITY),
        Class::Zero => Ok(one_padded()),
        Class::Number { coefficient, exponent, adjusted, .. } => {
            if adjusted <= -45 {
                return Ok(one_padded());
            }
            if at_least_large(coefficient, exponent, adjusted) {
                return Ok(half_exp(false, coefficient, exponent, adjusted));
            }
            let s = Scale::standard();
            let e = exp_below_large(coefficient, exponent);
            let inverse = s.div(&s.one(), &e);
            let sum = m_div_small(&m_add(&e, &inverse), 2).0;
            Ok(fx_result(Fx::new(false, sum), s.places()))
        }
    }
}

/// `tanh x`. A zero is itself; an infinity is `±1`.
pub fn tanh(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(negative) => Ok(Raw::Finite { negative, coefficient: 1, exponent: 0 }),
        Class::Zero => Ok(*x),
        Class::Number { negative, coefficient, exponent, adjusted } => {
            if adjusted <= TINY {
                return Ok(padded(negative, coefficient, exponent));
            }
            if at_least_large(coefficient, exponent, adjusted) {
                return Ok(Raw::Finite { negative, coefficient: 1, exponent: 0 });
            }
            let s = Scale::standard();
            // `e^(2|x|)`: twice the operand, at [`S`] places.
            let mut twice = to_fx(coefficient, exponent);
            twice.mul_small(2);
            let e2 = exp_standard(&twice);
            let one = s.one();
            let ratio = s.div(&m_sub(&e2, &one), &m_add(&e2, &one));
            Ok(fx_result(Fx::new(negative, ratio), s.places()))
        }
    }
}

// ---------------------------------------------------------------------------
// asinh, acosh, atanh
// ---------------------------------------------------------------------------

/// `ln(2x)` for a large operand, as a fixed point at [`S`] places.
fn ln_two_x(coefficient: u128, exponent: i64) -> Fx {
    ln_fx(coefficient, exponent).plus(&Fx::new(false, consts().ln2.clone()))
}

/// `asinh x`. A zero is a zero of the smallest exponent.
pub fn asinh(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(_) => Ok(*x),
        Class::Zero => Ok(zero_tiny(x)),
        Class::Number { negative, coefficient, exponent, adjusted } => {
            if adjusted <= TINY {
                return Ok(padded(negative, coefficient, exponent));
            }
            let s = Scale::standard();
            let l = if adjusted >= HUGE {
                ln_two_x(coefficient, exponent)
            } else {
                let ax = to_fx(coefficient, exponent);
                let root = s.root_of(&m_add(&s.one(), &s.mul(&ax, &ax)));
                ln_fixed(&m_add(&ax, &root))
            };
            Ok(fx_result(Fx::new(negative, l.mag), s.places()))
        }
    }
}

/// `acosh x` for `x >= 1`. One is `0`.
pub fn acosh(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(false) => Ok(INFINITY),
        Class::Infinite(true) => Err(Domain),
        Class::Zero => Err(Domain),
        Class::Number { negative, coefficient, exponent, adjusted } => {
            let s = Scale::standard();
            let one_dec = strip(coefficient, exponent) == (1, 0);
            if negative || adjusted < 0 {
                return Err(Domain);
            }
            if one_dec {
                return Ok(ZERO);
            }
            let l = if adjusted >= HUGE {
                ln_two_x(coefficient, exponent)
            } else {
                let ax = to_fx(coefficient, exponent);
                let root = s.root_of(&m_sub(&s.mul(&ax, &ax), &s.one()));
                ln_fixed(&m_add(&ax, &root))
            };
            Ok(fx_result(l, s.places()))
        }
    }
}

/// `atanh x = ln((1 + x) / (1 - x)) / 2` for `|x| < 1`. A zero is a zero of the smallest
/// exponent; `±1` is `±Infinity`.
pub fn atanh(x: &Raw) -> Result<Raw, Domain> {
    match classify(x) {
        Class::Nan => Ok(NAN),
        Class::Infinite(_) => Err(Domain),
        Class::Zero => Ok(zero_tiny(x)),
        Class::Number { negative, coefficient, exponent, adjusted } => {
            if above_one(coefficient, exponent, adjusted) {
                return Err(Domain);
            }
            if strip(coefficient, exponent) == (1, 0) {
                return Ok(Raw::Inf { negative });
            }
            // An operand whose cubic term is below the last place is its own answer. The
            // reference spells it as it was given when it has more digits than about
            // `49.5 + 0.96 · adjusted` (fitted to its answers; padded to 34 digits below),
            // which is the one place an answer's cohort follows the operand's.
            let own = i64::from(digits_u128(coefficient)) * 100 > 4950 + 96 * adjusted;
            if adjusted <= TINY {
                return Ok(if own { *x } else { padded(negative, coefficient, exponent) });
            }
            let s = Scale::standard();
            let ax = to_fx(coefficient, exponent);
            let one = s.one();
            let ratio = fx_div(&m_add(&one, &ax), &m_sub(&one, &ax));
            let l = ln_fixed(&ratio);
            let half = m_div_small(&l.mag, 2).0;
            let answer = fx_result(Fx::new(negative, half), s.places());
            Ok(match answer {
                Raw::Finite { coefficient: c, exponent: e, .. }
                    if own && strip(c, i64::from(e)) == strip(coefficient, exponent) =>
                {
                    *x
                }
                other => other,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// atan2
// ---------------------------------------------------------------------------

/// `pi` as a 34-digit decimal.
fn pi_raw() -> Raw {
    let s = Scale::standard();
    result(false, s.pi(), s.places())
}

/// `pi/4` and `3pi/4` as the reference answers an infinity over an infinity: the first with
/// 33 digits, the second with 34.
fn quarter_pi_raw() -> Raw {
    Raw::Finite {
        negative: false,
        coefficient: 785_398_163_397_448_309_615_660_845_819_876,
        exponent: -33,
    }
}

fn three_quarter_pi_raw() -> Raw {
    let s = Scale::standard();
    let mut three = s.pi();
    three.mul_small(3);
    result(false, m_div_small(&three, 4).0, s.places())
}

fn with_sign(raw: Raw, negative: bool) -> Raw {
    match raw {
        Raw::Finite { coefficient, exponent, .. } => {
            Raw::Finite { negative, coefficient, exponent }
        }
        Raw::Inf { .. } => Raw::Inf { negative },
        other => other,
    }
}

/// `atan2(y, x)`: the angle of the point `(x, y)`, in `(-pi, pi]`. The sign of a zero `y` is
/// kept, and a zero answer is a zero of the smallest exponent. An infinity over an infinity
/// is `±pi/4` (33 digits) or `±3pi/4`, as the reference answers it.
pub fn atan2(y: &Raw, x: &Raw) -> Raw {
    let (cy, cx) = (classify(y), classify(x));
    let x_negative = matches!(*x, Raw::Finite { negative: true, .. } | Raw::Inf { negative: true });
    let y_negative = matches!(*y, Raw::Finite { negative: true, .. } | Raw::Inf { negative: true });
    match (cy, cx) {
        (Class::Nan, _) | (_, Class::Nan) => NAN,
        (Class::Infinite(_), Class::Infinite(false)) => with_sign(quarter_pi_raw(), y_negative),
        (Class::Infinite(_), Class::Infinite(true)) => {
            with_sign(three_quarter_pi_raw(), y_negative)
        }
        (Class::Infinite(_), _) => with_sign(half_pi_raw(), y_negative),
        (_, Class::Infinite(false)) => zero_tiny(&with_sign(ZERO, y_negative)),
        (_, Class::Infinite(true)) => with_sign(pi_raw(), y_negative),
        (Class::Zero, _) if x_negative => with_sign(pi_raw(), y_negative),
        (Class::Zero, _) => zero_tiny(&with_sign(ZERO, y_negative)),
        (_, Class::Zero) => with_sign(half_pi_raw(), y_negative),
        (
            Class::Number { coefficient: cy, exponent: ey, adjusted: ay, .. },
            Class::Number { coefficient: cx, exponent: ex, adjusted: ax, .. },
        ) => {
            // `atan(|y| / |x|)` in a scale wide enough for a ratio as small as the operands
            // make it, but no wider than 80 digits of ratio; `pi/2` when the ratio is past 120
            // digits the other way; and a positive `x` with a ratio below that is answered by
            // the ratio itself, rounded once. A negative `x` is `pi` less the angle, computed
            // at the capped scale.
            if ax - ay > DIRECT_RATIO && !x_negative {
                return small_ratio(y_negative, (cy, ey), (cx, ex));
            }
            let s = Scale::reaching((ax - ay).clamp(0, DIRECT_RATIO));
            let angle = if ay - ax >= 120 {
                s.half_pi()
            } else {
                let shift = ey - ex + i64::from(s.places());
                let mut numerator = Mag::from_u128(cy);
                if shift >= 0 {
                    numerator.mul_pow10(shift as u32);
                } else {
                    numerator = numerator.split_digits((-shift) as u32).0;
                }
                let ratio = m_div_rem(&numerator, &Mag::from_u128(cx)).0;
                atan_pos(s, &ratio)
            };
            let angle = if x_negative { m_sub(&s.pi(), &angle) } else { angle };
            fx_result(Fx::new(y_negative, angle), s.places())
        }
    }
}

/// `±|y| / |x|` for a positive `x` and operands whose ratio is below `10^-79`, rounded once:
/// the arctangent of it is the ratio less a part below `10^-158` of it, so the answer is the
/// ratio's nearest 34 digits, the part counting only as the hair below an exact one. The
/// quotient is taken to 40 digits or more, and a division with no remainder is one unit short
/// and sticky.
fn small_ratio(negative: bool, (cy, ey): (u128, i64), (cx, ex): (u128, i64)) -> Raw {
    let lift = (40 + i64::from(digits_u128(cx)) - i64::from(digits_u128(cy))).max(0);
    let mut numerator = Mag::from_u128(cy);
    numerator.mul_pow10(lift as u32);
    let (mut quotient, remainder) = m_div_rem(&numerator, &Mag::from_u128(cx));
    if remainder.is_zero() {
        quotient = m_sub(&quotient, &Mag::from_u64(1));
    }
    let exponent = ey - ex - lift;
    rounded(negative, quotient, exponent, true, exponent)
}
