//! `Decimal128` functions: `abs`, `ceil`, `floor`, `round`, `trunc`, and the
//! five that need a real computation, `sqrt`, `exp`, `ln`, `log10` and `pow`,
//! in integers alone and in the context of ADR-252 (34 digits, round half to
//! even, one NaN, overflow is an infinity, gradual underflow).
//!
//! **The five are rounded once.** Each result is computed to far more digits
//! than the format holds and cut to 34 by [`rounded`] (the arithmetic's own
//! routine, so the exponent range, the overflow and the underflow are the
//! arithmetic's). Their cohort is **normalised**: trailing zeros come off the
//! coefficient while the exponent stays at most 6111, so `sqrt(4)` is `2`,
//! `pow(1.1, 2)` is `1.21` and `log10(1000)` is `3`; a zero result is `+0` at
//! exponent 0 (a `pow` that underflows from a negative base keeps its sign, as
//! IEEE's does). A value has one spelling here whatever its argument's cohort.
//!
//! **What is exact.** `sqrt` is an integer square root of the coefficient scaled
//! to at least 36 root digits, and the remainder is the sticky bit: correctly
//! rounded for every input, no exceptions. `pow` with a whole-number exponent of
//! at most 1024 in magnitude is the exact power (repeated squaring in [`Mag`],
//! up to 4000 digits) rounded once, and a negative exponent is the exact integer
//! quotient `10^s / x^n` with its remainder as the sticky bit, so `pow(1.1, 2)`
//! is exactly `1.21`; a power of ten to a whole power is exact at any size. `exp(0)`,
//! `ln(1)`, `log10(10^k)` and `pow(x, 0)` are exact special cases. `round`,
//! `trunc`, `ceil`, `floor` and `abs` are exact integer operations on the
//! coefficient.
//!
//! **What is approximate, and how well.** `exp`, `ln`, `log10` and a `pow` that
//! is not exact are computed in a signed fixed point of [`S`] = 114 decimal
//! places over [`Mag`] (a multiple of 19, so a rescale is a limb shift), with
//! these reductions:
//!
//! - `exp`: `x = k ln 10 + r`, `0 <= r < ln 10`; `e^r` by the Taylor series of
//!   `r / 4096` (a few dozen terms, each a multiplication and a division by a
//!   small integer), squared twelve times; the result is `e^r · 10^k`.
//! - `ln`: `x = m · 10^q` with `m` in `[0.316, 3.16)`, then `m = m' · 2^a` with
//!   `m'` in `[0.707, 1.414)`; `ln m' = 2 atanh((m' - 1) / (m' + 1))` by its
//!   series; `ln x = q ln 10 + a ln 2 + ln m'`. `ln 2` and `ln 10` are built once
//!   from `atanh` of `1/3` and `1/9`, 19 digits wider than used.
//! - `log10` is `ln` divided by `ln 10`; `pow` is `exp(y · ln |x|)` with the
//!   sign decided by the parity of `y`.
//!
//! Every step truncates, so the error before the final rounding is a few
//! hundred units in the 114th place. For `exp` that is a relative error near
//! `10^-109` (a result in `[1, 10)` times a power of ten). For `ln` away from 1
//! it is the same; for the logarithm of a value within `10^-33` of 1, whose
//! result is as small as `10^-34` (the nearest a 34-digit `x` gets to 1 is
//! `1 - 10^-34`), it is about `10^-80`. In `pow` the error of `ln |x|` is
//! multiplied by `y`, which is bounded because a `|y ln x|` above 14300 is
//! decided at once (an overflow or an underflow); the worst is about `10^-76`.
//! **So the relative error before the last rounding is below `10^-75` for
//! every input, against the `10^-60` the contract asks, and the only inputs
//! that can round the wrong way are those whose exact result lies within
//! `10^-75` of a midpoint between two 34-digit neighbours.** No input is known
//! to: the oracle test against decNumber at 120 digits (about 30,000 inputs
//! across magnitudes from `1E-6176` to `1E+6111`, values within `10^-33` of 1,
//! the overflow and underflow thresholds and the subnormal range) finds none.
//!
//! **Cost.** Everything is a few dozen to a hundred multiplications of
//! 115-digit numbers (six or seven limbs), each a column-wise product that
//! divides by `10^19` once per three partial products; a call is some tens of
//! microseconds in release (99th percentile about 25 µs for `ln`, `exp`, `log10`
//! and an inexact `pow`, 4 µs for `sqrt`, under a microsecond for an exact
//! `pow` and for `round`). The first call of `exp`, `ln`, `log10` or an
//! inexact `pow` builds the two constants (about 120 small divisions each).
//!
//! **Nothing is shared.** The only static is the pair of constants, immutable
//! once built and the same everywhere; nothing reads or keeps a status.
//!
//! **No panics.** Every `Raw` gives a result or [`Domain`]: the arguments of
//! the private routines are checked by their callers, and a `place` outside the
//! caller's range is clamped to the format's.
//!
//! Integer-only, like the file it is a child of: no floating-point type, no
//! formatter, no native byte order. `tests/decimal_arith_guard.rs` holds this file
//! to the same tokens (but for the name `sqrt`, which is this module's).

use std::cmp::Ordering;
use std::sync::OnceLock;

use super::round as rounded;
use super::{
    ETINY, ETOP, LIMB, LIMB_DIGITS, LIMB_WIDE, Mag, PREC, digits_u128, finite, split_limb,
};
use crate::decimal::{MAX_EXPONENT, Raw};

#[cfg(test)]
#[path = "func_tests.rs"]
mod func_tests;

/// An argument outside the function's domain: the square root or logarithm of
/// a negative number, the logarithm of zero, zero to a negative power, a
/// negative base to a power that is not a whole number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Domain;

const ZERO: Raw = Raw::Finite { negative: false, coefficient: 0, exponent: 0 };
const ONE: Raw = Raw::Finite { negative: false, coefficient: 1, exponent: 0 };
const NAN: Raw = Raw::Nan;
const INFINITY: Raw = Raw::Inf { negative: false };

// ---------------------------------------------------------------------------
// abs, ceil, floor, round, trunc
// ---------------------------------------------------------------------------

/// `|x|`: the sign cleared, the coefficient and exponent (the cohort member)
/// kept. `-0` is `+0` and `-Infinity` is `Infinity`.
pub fn abs(x: &Raw) -> Raw {
    match *x {
        Raw::Nan => Raw::Nan,
        Raw::Inf { .. } => INFINITY,
        Raw::Finite { coefficient, exponent, .. } => {
            Raw::Finite { negative: false, coefficient, exponent }
        }
    }
}

/// How the digits cut off a coefficient decide the one kept.
#[derive(Clone, Copy)]
enum Mode {
    HalfEven,
    TowardZero,
    TowardPositive,
    TowardNegative,
}

/// `x` at the exponent `target` (clamped into the format's), if its coefficient
/// then has at most 34 digits; otherwise `x` itself.
///
/// - A finite value at an exponent above the target is padded with zeros: the
///   same value, one more cohort member, unless the padding needs more than 34
///   digits, when it is returned unchanged.
/// - Below the target the digits under it are cut, and `mode` says whether the
///   kept part goes up by one. A carry to 10^34 re-normalises one exponent up.
/// - A zero result keeps the sign of the input (`ceil(-0.5)` is `-0`).
fn quantize(x: &Raw, target: i64, mode: Mode) -> Raw {
    let Raw::Finite { negative, coefficient, exponent } = *x else {
        return *x;
    };
    let exponent = i64::from(exponent);
    let target = target.clamp(ETINY, ETOP);
    if exponent == target {
        return *x;
    }
    if exponent > target {
        let pad = exponent - target;
        let fits = coefficient == 0 || i64::from(digits_u128(coefficient)) + pad <= i64::from(PREC);
        return if coefficient == 0 {
            finite(negative, 0, target)
        } else if fits {
            finite(negative, coefficient * 10u128.pow(pad as u32), target)
        } else {
            *x
        };
    }
    let cut = target - exponent;
    // Above 34 digits cut, the whole coefficient is below half a unit.
    let (mut kept, against_half, rest_nonzero) = if cut > i64::from(PREC) {
        (0, Ordering::Less, coefficient != 0)
    } else {
        let unit = 10u128.pow(cut as u32);
        let (kept, rest) = (coefficient / unit, coefficient % unit);
        (kept, (2 * rest).cmp(&unit), rest != 0)
    };
    let up = match mode {
        Mode::HalfEven => match against_half {
            Ordering::Greater => true,
            Ordering::Equal => kept % 2 == 1,
            Ordering::Less => false,
        },
        Mode::TowardZero => false,
        Mode::TowardPositive => !negative && rest_nonzero,
        Mode::TowardNegative => negative && rest_nonzero,
    };
    kept += u128::from(up);
    let mut target = target;
    if digits_u128(kept) > PREC {
        kept /= 10;
        target += 1;
    }
    finite(negative, kept, target)
}

/// The least integer at least `x`. A value whose exponent is at least 0 is
/// already one and is returned unchanged; any other has exponent 0 in the
/// result (`ceil(2.00)` is `2`, `ceil(1.2)` is `2`, `ceil(-0.5)` is `-0`).
pub fn ceil(x: &Raw) -> Raw {
    match *x {
        Raw::Finite { exponent, .. } if exponent < 0 => quantize(x, 0, Mode::TowardPositive),
        _ => *x,
    }
}

/// The greatest integer at most `x`, as [`ceil`] (`floor(-1.2)` is `-2`).
pub fn floor(x: &Raw) -> Raw {
    match *x {
        Raw::Finite { exponent, .. } if exponent < 0 => quantize(x, 0, Mode::TowardNegative),
        _ => *x,
    }
}

/// `x` rounded half to even to `place` digits after the point: the exponent of
/// the result is exactly `-place` whenever the coefficient then fits in 34
/// digits (`round(1.5, 3)` is `1.500`, `round(1234.5, -2)` is `1.2E+3`,
/// `round(2.5, 0)` is `2`, `round(3.5, 0)` is `4`); when the zeros it would pad
/// are too many to fit, `x` is returned unchanged. The caller validates
/// `place`; an outside one is clamped to the exponent range, never a panic.
pub fn round(x: &Raw, place: i32) -> Raw {
    quantize(x, -i64::from(place), Mode::HalfEven)
}

/// `x` cut toward zero at `place` digits after the point, as [`round`].
pub fn trunc(x: &Raw, place: i32) -> Raw {
    quantize(x, -i64::from(place), Mode::TowardZero)
}

// ---------------------------------------------------------------------------
// Magnitude helpers
// ---------------------------------------------------------------------------

/// Compare two magnitudes (limbs have no zero on top, so length decides first).
fn m_cmp(a: &Mag, b: &Mag) -> Ordering {
    a.0.len().cmp(&b.0.len()).then_with(|| a.0.iter().rev().cmp(b.0.iter().rev()))
}

fn m_add(a: &Mag, b: &Mag) -> Mag {
    let (long, short) = if a.0.len() >= b.0.len() { (a, b) } else { (b, a) };
    let mut out = Vec::with_capacity(long.0.len() + 1);
    let mut carry = 0u128;
    for (i, &l) in long.0.iter().enumerate() {
        let t = u128::from(l) + u128::from(short.0.get(i).copied().unwrap_or(0)) + carry;
        let (c, r) = split_limb(t);
        out.push(r);
        carry = c;
    }
    if carry != 0 {
        out.push(carry as u64);
    }
    Mag(out)
}

/// `a - b` for `a >= b` (and zero otherwise, which no caller relies on).
fn m_sub(a: &Mag, b: &Mag) -> Mag {
    if m_cmp(a, b) != Ordering::Greater {
        return Mag::zero();
    }
    let mut out = Vec::with_capacity(a.0.len());
    let mut borrow = 0i128;
    for (i, &l) in a.0.iter().enumerate() {
        let mut t = i128::from(l) - borrow - i128::from(b.0.get(i).copied().unwrap_or(0));
        borrow = 0;
        if t < 0 {
            t += LIMB_WIDE as i128;
            borrow = 1;
        }
        out.push(t as u64);
    }
    let mut mag = Mag(out);
    mag.trim();
    mag
}

/// `(⌊a / d⌋, a mod d)` for `d` nonzero (a zero `d` gives `(0, 0)`).
fn m_div_small(a: &Mag, d: u64) -> (Mag, u64) {
    if d == 0 {
        return (Mag::zero(), 0);
    }
    let d = u128::from(d);
    let mut out = vec![0u64; a.0.len()];
    let mut rest = 0u128;
    for (slot, &limb) in out.iter_mut().zip(&a.0).rev() {
        // `rest < d < 2^64`, so this stays below 2^64 · 10^19.
        let cur = rest * LIMB_WIDE + u128::from(limb);
        *slot = (cur / d) as u64;
        rest = cur % d;
    }
    let mut mag = Mag(out);
    mag.trim();
    (mag, rest as u64)
}

/// `a · b`, column by column: the partial products of one column are summed in
/// a `u128` and reduced by `10^19` once per three of them (three products of
/// two limbs are below 3·10^38, under 2^128), not once per product as
/// [`Mag::mul`] does.
fn m_mul(a: &Mag, b: &Mag) -> Mag {
    let (a, b) = (&a.0, &b.0);
    if a.is_empty() || b.is_empty() {
        return Mag::zero();
    }
    let n = a.len() + b.len();
    let mut out = vec![0u64; n];
    let mut carry = 0u128;
    for k in 0..n - 1 {
        let (low, high) = (k.saturating_sub(b.len() - 1), k.min(a.len() - 1));
        let (mut acc, mut upper, mut pending) = (0u128, 0u128, 0u8);
        for i in low..=high {
            acc += u128::from(a[i]) * u128::from(b[k - i]);
            pending += 1;
            if pending == 3 {
                let (q, r) = split_limb(acc);
                upper += q;
                acc = u128::from(r);
                pending = 0;
            }
        }
        let (q, r) = split_limb(acc + carry);
        out[k] = r;
        carry = upper + q;
    }
    // The product is below `10^(19 n)`: what is left is the top limb.
    out[n - 1] = carry as u64;
    let mut mag = Mag(out);
    mag.trim();
    mag
}

/// `(⌊a / b⌋, a mod b)`, Knuth's algorithm D in base `10^19`. A zero `b` gives
/// `(0, a)`: no caller passes one.
fn m_div_rem(a: &Mag, b: &Mag) -> (Mag, Mag) {
    if b.is_zero() || m_cmp(a, b) == Ordering::Less {
        return (Mag::zero(), a.clone());
    }
    if b.0.len() == 1 {
        let (q, r) = m_div_small(a, b.0[0]);
        return (q, Mag::from_u64(r));
    }
    let n = b.0.len();
    let m = a.0.len() - n;
    // Scale both so the divisor's top limb is at least half a limb: the
    // estimate of a quotient limb from the top two limbs is then at most two
    // too large.
    let f = LIMB / (b.0[n - 1] + 1);
    let mut u = a.clone();
    u.mul_small(f);
    u.0.resize(a.0.len() + 1, 0);
    let mut v = b.clone();
    v.mul_small(f);
    let (vt, vs) = (u128::from(v.0[n - 1]), u128::from(v.0[n - 2]));
    let mut q = vec![0u64; m + 1];
    let u = &mut u.0;
    for j in (0..=m).rev() {
        let top = u128::from(u[j + n]) * LIMB_WIDE + u128::from(u[j + n - 1]);
        let (mut qhat, mut rhat) = (top / vt, top % vt);
        while qhat >= LIMB_WIDE || qhat * vs > rhat * LIMB_WIDE + u128::from(u[j + n - 2]) {
            qhat -= 1;
            rhat += vt;
            if rhat >= LIMB_WIDE {
                break;
            }
        }
        // u[j..=j+n] -= qhat · v
        let (mut borrow, mut carry) = (0i128, 0u128);
        for i in 0..n {
            let p = qhat * u128::from(v.0[i]) + carry;
            let (c, low) = split_limb(p);
            carry = c;
            let mut t = i128::from(u[i + j]) - borrow - i128::from(low);
            borrow = 0;
            if t < 0 {
                t += LIMB_WIDE as i128;
                borrow = 1;
            }
            u[i + j] = t as u64;
        }
        let mut t = i128::from(u[j + n]) - borrow - carry as i128;
        let negative = t < 0;
        if negative {
            t += LIMB_WIDE as i128;
        }
        u[j + n] = t as u64;
        if negative {
            // qhat was one too large: add the divisor back.
            qhat -= 1;
            let mut c = 0u128;
            for i in 0..n {
                let (hi, lo) = split_limb(u128::from(u[i + j]) + u128::from(v.0[i]) + c);
                u[i + j] = lo;
                c = hi;
            }
            u[j + n] = ((u128::from(u[j + n]) + c) % LIMB_WIDE) as u64;
        }
        q[j] = qhat as u64;
    }
    let mut quotient = Mag(q);
    quotient.trim();
    let mut remainder = Mag(u[..n].to_vec());
    remainder.trim();
    (quotient, m_div_small(&remainder, f).0)
}

/// `⌊√n⌋` by Newton's iteration from a power of ten above the root.
fn m_isqrt(n: &Mag) -> Mag {
    if n.is_zero() {
        return Mag::zero();
    }
    let mut x = Mag::from_u64(1);
    x.mul_pow10(n.digits().div_ceil(2));
    loop {
        let (q, _) = m_div_rem(n, &x);
        let y = m_div_small(&m_add(&x, &q), 2).0;
        if m_cmp(&y, &x) != Ordering::Less {
            return x;
        }
        x = y;
    }
}

fn pow10(k: u32) -> Mag {
    let mut m = Mag::from_u64(1);
    m.mul_pow10(k);
    m
}

// ---------------------------------------------------------------------------
// Fixed point
// ---------------------------------------------------------------------------

/// The working scale: a fixed-point magnitude `N` stands for `N / 10^S`. A
/// multiple of 19, so a rescale after a product is a drop of [`SL`] limbs.
const S: u32 = 114;
const SL: usize = (S / LIMB_DIGITS) as usize;

/// A signed fixed-point number: `±mag / 10^S`. A zero is never negative.
#[derive(Clone, Debug)]
struct Fx {
    neg: bool,
    mag: Mag,
}

impl Fx {
    fn new(neg: bool, mag: Mag) -> Fx {
        Fx { neg: neg && !mag.is_zero(), mag }
    }

    fn plus(&self, other: &Fx) -> Fx {
        if self.neg == other.neg {
            return Fx::new(self.neg, m_add(&self.mag, &other.mag));
        }
        match m_cmp(&self.mag, &other.mag) {
            Ordering::Less => Fx::new(other.neg, m_sub(&other.mag, &self.mag)),
            _ => Fx::new(self.neg, m_sub(&self.mag, &other.mag)),
        }
    }

    /// `n · constant`.
    fn times(neg: bool, constant: &Mag, n: u64) -> Fx {
        let mut mag = constant.clone();
        mag.mul_small(n);
        Fx::new(neg, mag)
    }
}

/// `⌊a · b / 10^S⌋`.
fn fx_mul(a: &Mag, b: &Mag) -> Mag {
    let product = m_mul(a, b);
    if product.0.len() <= SL {
        return Mag::zero();
    }
    Mag(product.0[SL..].to_vec())
}

/// `⌊a · 10^S / b⌋` for `b` nonzero.
fn fx_div(a: &Mag, b: &Mag) -> Mag {
    let mut numerator = a.clone();
    if !numerator.is_zero() {
        numerator.0.splice(0..0, std::iter::repeat_n(0, SL));
    }
    m_div_rem(&numerator, b).0
}

/// `coefficient · 10^exponent` as a fixed-point magnitude, truncated.
fn to_fx(coefficient: u128, exponent: i64) -> Mag {
    let mut m = Mag::from_u128(coefficient);
    if m.is_zero() {
        return m;
    }
    let shift = exponent + i64::from(S);
    if shift >= 0 {
        m.mul_pow10(shift as u32);
        m
    } else if -shift > i64::from(PREC) + 1 {
        Mag::zero()
    } else {
        m.split_digits((-shift) as u32).0
    }
}

/// `Σ 1 / ((2k + 1) n^(2k+1))` = `atanh(1/n)`, to `S + 19` places.
fn atanh_inverse(n: u64) -> Mag {
    let mut power = pow10(S + LIMB_DIGITS);
    power = m_div_small(&power, n).0;
    let mut sum = Mag::zero();
    let mut k = 1u64;
    while !power.is_zero() {
        sum = m_add(&sum, &m_div_small(&power, k).0);
        power = m_div_small(&power, n * n).0;
        k += 2;
    }
    sum
}

/// `a` (at `S + 19` places) at `S`, rounded half up.
fn narrow(a: &Mag) -> Mag {
    let (q, tail) = a.split_digits(LIMB_DIGITS);
    if tail.digit >= 5 { m_add(&q, &Mag::from_u64(1)) } else { q }
}

struct Consts {
    ln2: Mag,
    ln10: Mag,
}

/// `ln 2` and `ln 10`, built once: `ln 2 = 2 atanh(1/3)` and
/// `ln 10 = 3 ln 2 + ln(5/4) = 3 ln 2 + 2 atanh(1/9)`.
fn consts() -> &'static Consts {
    static CONSTS: OnceLock<Consts> = OnceLock::new();
    CONSTS.get_or_init(|| {
        let mut ln2 = atanh_inverse(3);
        ln2.mul_small(2);
        let mut ln10 = ln2.clone();
        ln10.mul_small(3);
        let mut quarter = atanh_inverse(9);
        quarter.mul_small(2);
        Consts { ln2: narrow(&ln2), ln10: narrow(&m_add(&ln10, &quarter)) }
    })
}

/// `e^r` for `0 <= r < ln 10`: a fixed-point number in `[1, 10)`.
///
/// `r / 4096` is below 0.0006, so the Taylor series is a short one; squaring
/// twelve times gives `e^r` and multiplies the relative error by at most 4096.
fn exp_reduced(r: &Mag) -> Mag {
    const HALVINGS: u32 = 12;
    let r = m_div_small(r, 1 << HALVINGS).0;
    let one = pow10(S);
    let (mut sum, mut term) = (one.clone(), one);
    let mut k = 1u64;
    loop {
        term = m_div_small(&fx_mul(&term, &r), k).0;
        if term.is_zero() {
            break;
        }
        sum = m_add(&sum, &term);
        k += 1;
    }
    for _ in 0..HALVINGS {
        sum = fx_mul(&sum, &sum);
    }
    sum
}

/// `e^x` for a fixed-point `x` as `(E, k)` with `e^x = E · 10^(k - S)` and `E`
/// in `[10^S, 10^(S+1))`: `x = k ln 10 + r` with `r` in `[0, ln 10)`.
fn exp_fx(negative: bool, x: &Mag) -> (Mag, i64) {
    let ln10 = &consts().ln10;
    let (q, rest) = m_div_rem(x, ln10);
    let q = q.to_u128().map_or(0, |q| q as i64);
    if !negative {
        (exp_reduced(&rest), q)
    } else if rest.is_zero() {
        (pow10(S), -q)
    } else {
        (exp_reduced(&m_sub(ln10, &rest)), -q - 1)
    }
}

/// A fixed-point number above which `e^x` certainly overflows or, negated,
/// underflows to zero: 14300. The largest finite value is below `10^6145`
/// (`ln` is 14149.4) and half the smallest subnormal is `5 · 10^-6177`
/// (`ln` is -14221.46).
fn exp_limit() -> Mag {
    let mut limit = Mag::from_u64(14300);
    limit.mul_pow10(S);
    limit
}

/// The leading 34 digits of `√10`, above which a mantissa is taken as `x / 10^(a+1)`.
const SQRT_TEN_LEAD: u128 = 3_162_277_660_168_379_331_998_893_544_432_719;

/// `ln x` for `x = coefficient · 10^exponent > 0`, as a fixed point of absolute
/// error a few hundred units of `10^-S`.
fn ln_fx(coefficient: u128, exponent: i64) -> Fx {
    let digits = i64::from(digits_u128(coefficient));
    let adjusted = digits - 1 + exponent;
    let lead = coefficient * 10u128.pow((i64::from(PREC) - digits) as u32);
    // `x = m · 10^q` with `m` in [0.316, 3.16).
    let q = if lead < SQRT_TEN_LEAD { adjusted } else { adjusted + 1 };
    let mut m = Mag::from_u128(coefficient);
    // `exponent - q >= -digits`, so the shift is at least `S - 34`: exact.
    m.mul_pow10((exponent - q + i64::from(S)) as u32);
    // `m = m' · 2^a` with `m'` in [0.707, 1.414).
    let (mut hi, mut lo) = (Mag::from_u64(14142), Mag::from_u64(7071));
    hi.mul_pow10(S - 4);
    lo.mul_pow10(S - 4);
    let mut twos = 0i64;
    while m_cmp(&m, &hi) != Ordering::Less {
        m = m_div_small(&m, 2).0;
        twos += 1;
    }
    while m_cmp(&m, &lo) == Ordering::Less {
        m.mul_small(2);
        twos -= 1;
    }
    // `ln m' = 2 atanh(z)`, `z = (m' - 1) / (m' + 1)`.
    let one = pow10(S);
    let below = m_cmp(&m, &one) == Ordering::Less;
    let numerator = if below { m_sub(&one, &m) } else { m_sub(&m, &one) };
    let z = fx_div(&numerator, &m_add(&m, &one));
    let z2 = fx_mul(&z, &z);
    let (mut sum, mut power, mut k) = (Mag::zero(), z, 1u64);
    while !power.is_zero() {
        sum = m_add(&sum, &m_div_small(&power, k).0);
        power = fx_mul(&power, &z2);
        k += 2;
    }
    sum.mul_small(2);
    let c = consts();
    Fx::times(q < 0, &c.ln10, q.unsigned_abs())
        .plus(&Fx::times(twos < 0, &c.ln2, twos.unsigned_abs()))
        .plus(&Fx::new(below, sum))
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

/// The cohort the five functions return: trailing zeros off the coefficient
/// while the exponent is at most 6111, and a zero is `+0` at exponent 0.
fn normalise(raw: Raw) -> Raw {
    match raw {
        Raw::Finite { coefficient: 0, .. } => ZERO,
        Raw::Finite { negative, mut coefficient, mut exponent } => {
            while coefficient.is_multiple_of(10) && exponent < MAX_EXPONENT {
                coefficient /= 10;
                exponent += 1;
            }
            Raw::Finite { negative, coefficient, exponent }
        }
        other => other,
    }
}

/// A fixed-point result `mag · 10^exponent` (inexact: the true value is a hair
/// above) rounded once to the format and normalised.
fn finish(negative: bool, mut mag: Mag, mut exponent: i64) -> Raw {
    let digits = mag.digits();
    if digits == 0 {
        return ZERO;
    }
    // [`rounded`] cuts a sticky result: give it more than 34 digits to cut.
    if digits < 40 {
        mag.mul_pow10(40 - digits);
        exponent -= i64::from(40 - digits);
    }
    normalise(rounded(negative, mag, exponent, true, exponent))
}

/// `raw` (a magnitude result) with the sign `negative`; an infinity and a zero
/// take it too.
fn with_sign(raw: Raw, negative: bool) -> Raw {
    match raw {
        Raw::Finite { coefficient, exponent, .. } => {
            Raw::Finite { negative, coefficient, exponent }
        }
        Raw::Inf { .. } => Raw::Inf { negative },
        Raw::Nan => Raw::Nan,
    }
}

fn signed_zero(negative: bool) -> Raw {
    Raw::Finite { negative, coefficient: 0, exponent: 0 }
}

/// A zero result takes the sign `negative`: a power that underflows keeps the
/// sign it would have had.
fn keep_sign(raw: Raw, negative: bool) -> Raw {
    if raw.is_zero() { signed_zero(negative) } else { raw }
}

/// `coefficient` without trailing zeros, and the exponent that makes up for
/// them. The coefficient is nonzero.
fn strip(mut coefficient: u128, mut exponent: i64) -> (u128, i64) {
    while coefficient.is_multiple_of(10) {
        coefficient /= 10;
        exponent += 1;
    }
    (coefficient, exponent)
}

/// An integer, exactly, at exponent 0.
fn integer(k: i64) -> Raw {
    if k == 0 {
        return ZERO;
    }
    Raw::Finite { negative: k < 0, coefficient: u128::from(k.unsigned_abs()), exponent: 0 }
}

// ---------------------------------------------------------------------------
// sqrt, exp, ln, log10
// ---------------------------------------------------------------------------

/// `√x`, correctly rounded for every input. A negative `x` (not `-0`) is
/// outside the domain; `±0` is returned as it is, at exponent 0; `Infinity` is
/// `Infinity`.
///
/// The coefficient is made an even power of ten above its exponent and scaled
/// by `10^(2p)` to at least 72 digits, so the integer square root has at least
/// 36: the root is exact in its floor, and `n - root² != 0` is the sticky bit
/// [`rounded`] needs for a tie that was not one.
pub fn sqrt(x: &Raw) -> Result<Raw, Domain> {
    let (coefficient, exponent) = match *x {
        Raw::Nan => return Ok(NAN),
        Raw::Inf { negative: true } => return Err(Domain),
        Raw::Inf { negative: false } => return Ok(INFINITY),
        Raw::Finite { negative, coefficient: 0, .. } => return Ok(signed_zero(negative)),
        Raw::Finite { negative: true, .. } => return Err(Domain),
        Raw::Finite { coefficient, exponent, .. } => (coefficient, i64::from(exponent)),
    };
    let mut n = Mag::from_u128(coefficient);
    let mut exponent = exponent;
    if exponent.rem_euclid(2) == 1 {
        n.mul_small(10);
        exponent -= 1;
    }
    let digits = n.digits();
    if digits < 72 {
        let pad = (72 - digits).div_ceil(2);
        n.mul_pow10(2 * pad);
        exponent -= 2 * i64::from(pad);
    }
    let root = m_isqrt(&n);
    let sticky = m_mul(&root, &root) != n;
    let exponent = exponent / 2;
    Ok(normalise(rounded(false, root, exponent, sticky, exponent)))
}

/// The result of `e^x` for a fixed-point `x` of the given sign, rounded.
fn exp_signed(negative: bool, x: &Mag) -> Raw {
    if m_cmp(x, &exp_limit()) == Ordering::Greater {
        return if negative { ZERO } else { INFINITY };
    }
    let (e, k) = exp_fx(negative, x);
    finish(false, e, k - i64::from(S))
}

/// `e^x`. `Infinity` is `Infinity`, `-Infinity` is `+0`, `exp(0)` is exactly 1;
/// an overflow is `Infinity` and an underflow is gradual and ends in `+0`.
pub fn exp(x: &Raw) -> Raw {
    match *x {
        Raw::Nan => NAN,
        Raw::Inf { negative: false } => INFINITY,
        Raw::Inf { negative: true } => ZERO,
        Raw::Finite { coefficient: 0, .. } => ONE,
        Raw::Finite { negative, coefficient, exponent } => {
            let exponent = i64::from(exponent);
            // 10^5 and more is far past the point of overflow or underflow.
            if i64::from(digits_u128(coefficient)) + exponent > 5 {
                return if negative { ZERO } else { INFINITY };
            }
            exp_signed(negative, &to_fx(coefficient, exponent))
        }
    }
}

/// The finite positive `x` of a logarithm, or the answer without one.
fn log_argument(x: &Raw) -> Result<(u128, i64), Result<Raw, Domain>> {
    match *x {
        Raw::Nan => Err(Ok(NAN)),
        Raw::Inf { negative: false } => Err(Ok(INFINITY)),
        Raw::Inf { negative: true } => Err(Err(Domain)),
        Raw::Finite { coefficient: 0, .. } | Raw::Finite { negative: true, .. } => Err(Err(Domain)),
        Raw::Finite { coefficient, exponent, .. } => Ok((coefficient, i64::from(exponent))),
    }
}

/// The natural logarithm. `x <= 0` (zeros included) and `-Infinity` are outside
/// the domain; `ln(1)` is `+0` and `ln(Infinity)` is `Infinity`.
pub fn ln(x: &Raw) -> Result<Raw, Domain> {
    let (coefficient, exponent) = match log_argument(x) {
        Ok(argument) => argument,
        Err(answer) => return answer,
    };
    if strip(coefficient, exponent) == (1, 0) {
        return Ok(ZERO);
    }
    let l = ln_fx(coefficient, exponent);
    Ok(finish(l.neg, l.mag, -i64::from(S)))
}

/// The base-10 logarithm, with `ln`'s domain. An exact power of ten `10^k` gives
/// exactly `k`.
pub fn log10(x: &Raw) -> Result<Raw, Domain> {
    let (coefficient, exponent) = match log_argument(x) {
        Ok(argument) => argument,
        Err(answer) => return answer,
    };
    let (stripped, shifted) = strip(coefficient, exponent);
    if stripped == 1 {
        return Ok(normalise(integer(shifted)));
    }
    let l = ln_fx(coefficient, exponent);
    Ok(finish(l.neg, fx_div(&l.mag, &consts().ln10), -i64::from(S)))
}

// ---------------------------------------------------------------------------
// pow
// ---------------------------------------------------------------------------

/// What `y` is, as an exponent.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Whole {
    No,
    Even,
    Odd,
}

/// Whether `coefficient · 10^exponent` is a whole number, and its parity.
fn classify(coefficient: u128, exponent: i64) -> Whole {
    if coefficient == 0 || exponent > 0 {
        return Whole::Even;
    }
    let parity = |n: u128| if n % 2 == 1 { Whole::Odd } else { Whole::Even };
    if exponent == 0 {
        return parity(coefficient);
    }
    // A nonzero coefficient below 10^34 is not a multiple of 10^35.
    if exponent < -i64::from(PREC) {
        return Whole::No;
    }
    let unit = 10u128.pow((-exponent) as u32);
    if !coefficient.is_multiple_of(unit) { Whole::No } else { parity(coefficient / unit) }
}

/// The magnitude of a nonzero whole number, saturated at `2^40`.
fn whole_magnitude(coefficient: u128, exponent: i64) -> u64 {
    const SATURATED: u128 = 1 << 40;
    let value = if exponent >= 0 {
        if exponent > 12 {
            SATURATED
        } else {
            coefficient.saturating_mul(10u128.pow(exponent as u32))
        }
    } else {
        coefficient / 10u128.pow((-exponent) as u32)
    };
    value.min(SATURATED) as u64
}

/// The digits of the largest exact power `pow` forms before it falls back to the
/// logarithm: `x^n` for `n <= 1024` of a 34-digit `x` has up to 34,816, and the
/// fallback is as correct (a power with that many digits is not a tie).
const EXACT_DIGITS: u64 = 4000;

/// `|x|` against 1; an infinity is above it.
fn against_one(x: &Raw) -> Ordering {
    match *x {
        Raw::Finite { coefficient, exponent, .. } if coefficient != 0 => {
            let (c, e) = strip(coefficient, i64::from(exponent));
            let adjusted = i64::from(digits_u128(c)) - 1 + e;
            match adjusted.cmp(&0) {
                Ordering::Equal if c == 1 => Ordering::Equal,
                Ordering::Equal => Ordering::Greater,
                other => other,
            }
        }
        Raw::Finite { .. } => Ordering::Less,
        _ => Ordering::Greater,
    }
}

/// `(sign) x^n` or `x^-n` exactly, rounded once, for a coefficient with no
/// trailing zero and `1 <= n <= 1024`; `None` if the exact power is too big.
fn pow_exact(negative: bool, c: u128, e: i64, inverse: bool, n: u64) -> Option<Raw> {
    let exponent = e * n as i64;
    let mut power = Mag::from_u64(1);
    if c != 1 {
        let cap = u32::try_from(EXACT_DIGITS).unwrap_or(u32::MAX);
        if (u64::from(digits_u128(c)) - 1) * n + 1 > EXACT_DIGITS {
            return None;
        }
        let (mut base, mut left) = (Mag::from_u128(c), n);
        loop {
            if left & 1 == 1 {
                power = m_mul(&power, &base);
            }
            left >>= 1;
            if left == 0 {
                break;
            }
            base = m_mul(&base, &base);
            if base.digits() > cap {
                return None;
            }
        }
        if power.digits() > cap {
            return None;
        }
    }
    if !inverse {
        return Some(normalise(rounded(negative, power, exponent, false, exponent)));
    }
    // `10^s / x^n` with at least 40 quotient digits; the remainder is the sticky bit.
    let s = power.digits() + 40;
    let (quotient, rest) = m_div_rem(&pow10(s), &power);
    let exponent = -exponent - i64::from(s);
    Some(normalise(rounded(negative, quotient, exponent, !rest.is_zero(), exponent)))
}

/// `x^y` as `e^(y ln x)` for a finite `x` away from 1 and a finite nonzero `y`.
fn pow_general(negative: bool, xc: u128, xe: i64, y_negative: bool, yc: u128, ye: i64) -> Raw {
    let l = ln_fx(xc, xe);
    let t_negative = l.neg != y_negative;
    let overflow = || if t_negative { signed_zero(negative) } else { Raw::Inf { negative } };
    // `|ln x|` is at least 10^-34, so a `y` of 10^39 or more makes `|y ln x|`
    // exceed 14300 whatever x is.
    if i64::from(digits_u128(yc)) + ye > 39 {
        return overflow();
    }
    let mut t = m_mul(&l.mag, &Mag::from_u128(yc));
    if ye >= 0 {
        t.mul_pow10(ye as u32);
    } else if !t.is_zero() {
        t = t.split_digits((-ye) as u32).0;
    }
    if m_cmp(&t, &exp_limit()) == Ordering::Greater {
        return overflow();
    }
    let (e, k) = exp_fx(t_negative, &t);
    with_sign(finish(false, e, k - i64::from(S)), negative)
}

/// `x^y`.
///
/// The order of the rules is C99's Annex F, which IEEE 754 follows:
///
/// - `y` zero is 1, even for a NaN `x`; `x` 1 is 1, even for a NaN `y`; any
///   other NaN is NaN.
/// - `x` zero: a positive `y` is a zero, `-0` for `x = -0` and an odd integer
///   `y`; a negative `y` is outside the domain (kimmydb refuses a zero divisor).
/// - A negative finite `x` to a `y` that is not a whole number is outside the
///   domain; to a whole number, the result is negative exactly when `y` is odd.
/// - An infinite `y`: 1 for `|x| = 1`, otherwise an infinity or zero by whether
///   `|x| > 1` agrees with the sign of `y`. An infinite `x`: an infinity for a
///   positive `y`, zero for a negative one, negative for `-Infinity` and an odd
///   integer `y`.
/// - A finite `x` and whole `y` of at most 1024 in magnitude: the exact power
///   (or the exact quotient) rounded once. Everything else: `exp(y ln |x|)`.
pub fn pow(x: &Raw, y: &Raw) -> Result<Raw, Domain> {
    if matches!(y, Raw::Finite { coefficient: 0, .. }) {
        return Ok(ONE);
    }
    if let Raw::Finite { negative: false, coefficient, exponent } = *x
        && coefficient != 0
        && strip(coefficient, i64::from(exponent)) == (1, 0)
    {
        return Ok(ONE);
    }
    if matches!(x, Raw::Nan) || matches!(y, Raw::Nan) {
        return Ok(NAN);
    }
    let y_negative = matches!(y, Raw::Inf { negative: true } | Raw::Finite { negative: true, .. });
    let y_parity = match *y {
        Raw::Finite { coefficient, exponent, .. } => classify(coefficient, i64::from(exponent)),
        _ => Whole::No,
    };
    if let Raw::Finite { negative, coefficient: 0, .. } = *x {
        if y_negative {
            return Err(Domain);
        }
        return Ok(signed_zero(negative && y_parity == Whole::Odd));
    }
    if matches!(y, Raw::Inf { .. }) {
        return Ok(match (against_one(x), y_negative) {
            (Ordering::Equal, _) => ONE,
            (Ordering::Greater, false) | (Ordering::Less, true) => INFINITY,
            _ => ZERO,
        });
    }
    let Raw::Finite { coefficient: yc, exponent: ye, .. } = *y else {
        return Ok(NAN);
    };
    let ye = i64::from(ye);
    if let Raw::Inf { negative: x_negative } = *x {
        let negative = x_negative && y_parity == Whole::Odd;
        return Ok(if y_negative { signed_zero(negative) } else { Raw::Inf { negative } });
    }
    let Raw::Finite { negative: x_negative, coefficient: xc, exponent: xe } = *x else {
        return Ok(NAN);
    };
    if x_negative && y_parity == Whole::No {
        return Err(Domain);
    }
    let negative = x_negative && y_parity == Whole::Odd;
    let (sc, se) = strip(xc, i64::from(xe));
    if sc == 1 && se == 0 {
        return Ok(Raw::Finite { negative, coefficient: 1, exponent: 0 });
    }
    if y_parity != Whole::No {
        let n = whole_magnitude(yc, ye);
        if sc == 1 {
            // A power of ten to a whole power is a power of ten, at any size.
            let total = i128::from(se) * i128::from(n) * if y_negative { -1 } else { 1 };
            let total = total.clamp(-100_000_000, 100_000_000) as i64;
            let power = normalise(rounded(negative, Mag::from_u64(1), total, false, total));
            return Ok(keep_sign(power, negative));
        }
        if n <= 1024
            && let Some(exact) = pow_exact(negative, sc, se, y_negative, n)
        {
            return Ok(keep_sign(exact, negative));
        }
    }
    Ok(pow_general(negative, xc, i64::from(xe), y_negative, yc, ye))
}
