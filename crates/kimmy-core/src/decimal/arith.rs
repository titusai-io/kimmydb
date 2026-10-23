//! `Decimal128` arithmetic: IEEE 754 decimal128 operations under round-half-even
//! in one fixed context, in integers alone (ADR-252).
//!
//! **The context** is a constant: 34 digits, the interchange format's exponent
//! range, round half to even, and IEEE's preferred exponents for exact results
//! (`1.10 + 1.20` is `2.30`). Nothing here reads or keeps a status, and nothing
//! is global or thread-local.
//!
//! **Every operation is exact, then rounded once** ([`round`]): a sum is the
//! exact sum of its operands as they were converted, a product the exact
//! product, a quotient the exact quotient plus a sticky bit. So a sum over a
//! multiset gives the same bytes in any order.
//!
//! **Integer-only by construction.** This file names no binary floating-point
//! type: a double enters through its 64 bits ([`from_binary64`]) and leaves as
//! 64 ([`to_binary64`]), and the caller does the one reinterpretation. A source
//! guard (`tests/decimal_arith_guard.rs`) fails the build on any such token
//! here, on the platform-byte-order conversions and the reinterpretation of
//! bits, and on anything that formats a number to digits. Arithmetic is on
//! integers of fixed width, or on
//! [`Mag`], a magnitude in base 10^19 limbs, whose operations are explicit about
//! their bounds.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::{MAX_EXPONENT, MIN_EXPONENT, Raw};
use crate::cmp::big::Big;
use crate::keyenc::number;

/// Digits of a coefficient.
const PREC: u32 = 34;
/// `emax`: the largest adjusted exponent.
const EMAX: i64 = 6144;
/// The smallest and largest stored exponent.
const ETINY: i64 = MIN_EXPONENT as i64;
const ETOP: i64 = MAX_EXPONENT as i64;

/// A limb of [`Mag`]: `10^19`, the largest power of ten a `u64` holds.
const LIMB: u64 = 10_000_000_000_000_000_000;
const LIMB_WIDE: u128 = LIMB as u128;
const LIMB_DIGITS: u32 = 19;

/// `t / 10^19` and `t % 10^19`, from one division.
#[inline]
fn split_limb(t: u128) -> (u128, u64) {
    let quotient = t / LIMB_WIDE;
    (quotient, (t - quotient * LIMB_WIDE) as u64)
}

/// Dividing by zero: the one refusal in this file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DivisionByZero;

// ---------------------------------------------------------------------------
// Mag: a magnitude in base 10^19
// ---------------------------------------------------------------------------

/// An unsigned integer as little-endian limbs in base `10^19`, no zero limb at
/// the top, so zero has none. A decimal digit count and a cut of the low digits
/// are then limb operations.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Mag(Vec<u64>);

/// What was cut off the bottom of a [`Mag`] by [`Mag::split_digits`]: its
/// highest digit, and whether any digit below that one is nonzero. Enough to
/// compare the cut part with half a unit and to tell whether it was zero.
#[derive(Clone, Copy, Debug)]
struct Tail {
    digit: u8,
    rest_nonzero: bool,
}

impl Tail {
    /// The cut part against half a unit of the digit above it.
    fn against_half(self) -> Ordering {
        match self.digit.cmp(&5) {
            Ordering::Equal if self.rest_nonzero => Ordering::Greater,
            other => other,
        }
    }

    fn is_zero(self) -> bool {
        self.digit == 0 && !self.rest_nonzero
    }
}

impl Mag {
    fn zero() -> Mag {
        Mag(Vec::new())
    }

    fn from_u128(mut value: u128) -> Mag {
        let mut limbs = Vec::with_capacity(3);
        while value != 0 {
            limbs.push((value % LIMB_WIDE) as u64);
            value /= LIMB_WIDE;
        }
        Mag(limbs)
    }

    fn from_u64(value: u64) -> Mag {
        Mag::from_u128(u128::from(value))
    }

    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    /// The number of decimal digits; zero has none.
    fn digits(&self) -> u32 {
        match self.0.last() {
            None => 0,
            Some(&top) => LIMB_DIGITS * (self.0.len() as u32 - 1) + top.ilog10() + 1,
        }
    }

    /// The value, if it fits a `u128`.
    fn to_u128(&self) -> Option<u128> {
        self.0
            .iter()
            .rev()
            .try_fold(0u128, |acc, &l| acc.checked_mul(LIMB_WIDE)?.checked_add(u128::from(l)))
    }

    /// `self *= m`.
    fn mul_small(&mut self, m: u64) {
        if m == 0 {
            self.0.clear();
            return;
        }
        let mut carry = 0u128;
        for limb in &mut self.0 {
            // At most (10^19 - 1)(2^64 - 1) + carry, far below 2^128.
            let product = u128::from(*limb) * u128::from(m) + carry;
            (carry, *limb) = split_limb(product);
        }
        while carry != 0 {
            self.0.push((carry % LIMB_WIDE) as u64);
            carry /= LIMB_WIDE;
        }
    }

    /// `self · other`, schoolbook, the shorter operand outside so the long one
    /// is a single run of multiply-and-carry per limb of the short.
    fn mul(&self, other: &Mag) -> Mag {
        if self.is_zero() || other.is_zero() {
            return Mag::zero();
        }
        let (short, long) =
            if self.0.len() <= other.0.len() { (self, other) } else { (other, self) };
        let mut out = vec![0u64; short.0.len() + long.0.len()];
        for (i, &a) in short.0.iter().enumerate() {
            let mut carry = 0u128;
            for (j, &b) in long.0.iter().enumerate() {
                // (10^19 - 1)^2 + 2 · 10^19 < 2^127.
                let t = u128::from(a) * u128::from(b) + u128::from(out[i + j]) + carry;
                (carry, out[i + j]) = split_limb(t);
            }
            let mut k = i + long.0.len();
            while carry != 0 {
                (carry, out[k]) = split_limb(u128::from(out[k]) + carry);
                k += 1;
            }
        }
        let mut mag = Mag(out);
        mag.trim();
        mag
    }

    /// `self *= 10^k`.
    fn mul_pow10(&mut self, k: u32) {
        if self.is_zero() || k == 0 {
            return;
        }
        let (whole, part) = ((k / LIMB_DIGITS) as usize, k % LIMB_DIGITS);
        self.mul_small(10u64.pow(part));
        self.0.splice(0..0, std::iter::repeat_n(0, whole));
    }

    /// The decimal digit at `position` (0 is the lowest); zero past the top.
    fn digit_at(&self, position: u32) -> u8 {
        let (limb, offset) = ((position / LIMB_DIGITS) as usize, position % LIMB_DIGITS);
        self.0.get(limb).map_or(0, |&l| ((l / 10u64.pow(offset)) % 10) as u8)
    }

    /// Whether some digit below `position` is nonzero.
    fn nonzero_below(&self, position: u32) -> bool {
        let (limb, offset) = ((position / LIMB_DIGITS) as usize, position % LIMB_DIGITS);
        self.0.iter().take(limb).any(|&l| l != 0)
            || (offset > 0 && self.0.get(limb).is_some_and(|&l| l % 10u64.pow(offset) != 0))
    }

    /// The number of trailing zero digits, at most `cap`. The value is nonzero.
    fn trailing_zeros(&self, cap: u32) -> u32 {
        let mut zeros = 0u32;
        for &limb in &self.0 {
            if limb == 0 {
                zeros += LIMB_DIGITS;
            } else {
                let mut rest = limb;
                while rest % 10 == 0 {
                    rest /= 10;
                    zeros += 1;
                }
                break;
            }
            if zeros >= cap {
                break;
            }
        }
        zeros.min(cap)
    }

    /// `⌊self / 10^k⌋` for `k >= 1`, and what was cut off.
    fn split_digits(&self, k: u32) -> (Mag, Tail) {
        let tail = Tail { digit: self.digit_at(k - 1), rest_nonzero: self.nonzero_below(k - 1) };
        let (whole, part) = ((k / LIMB_DIGITS) as usize, k % LIMB_DIGITS);
        if whole >= self.0.len() {
            return (Mag::zero(), tail);
        }
        let mut limbs = self.0[whole..].to_vec();
        if part > 0 {
            let divisor = u128::from(10u64.pow(part));
            let mut remainder = 0u128;
            for limb in limbs.iter_mut().rev() {
                // `remainder < 10^18`, so this stays below 10^37.
                let current = remainder * LIMB_WIDE + u128::from(*limb);
                *limb = (current / divisor) as u64;
                remainder = current % divisor;
            }
        }
        let mut quotient = Mag(limbs);
        quotient.trim();
        (quotient, tail)
    }

    /// The same value in base 2^64.
    fn to_big(&self) -> Big {
        let mut big = Big::from_u128(0);
        for &limb in self.0.iter().rev() {
            big.mul_add_small(LIMB, limb);
        }
        big
    }
}

// ---------------------------------------------------------------------------
// Rounding
// ---------------------------------------------------------------------------

/// The zero, or the finite value, with an exponent clamped into the range.
fn finite(negative: bool, coefficient: u128, exponent: i64) -> Raw {
    Raw::Finite { negative, coefficient, exponent: exponent.clamp(ETINY, ETOP) as i16 }
}

/// The number of decimal digits of `c` (zero has none).
fn digits_u128(c: u128) -> u32 {
    c.checked_ilog10().map_or(0, |log| log + 1)
}

/// `(C + d) · 10^E` rounded to a `Decimal128`: `d` in `[0, 1)`, and nonzero
/// exactly when `sticky`. `pref` is the preferred exponent.
///
/// - An exact result (no `sticky`) takes the cohort member closest to `pref`
///   that fits: trailing zeros come off down to `pref`, never below `E`.
/// - Otherwise the digits above 34, and those below the smallest exponent, are
///   cut off, half to even; a tie whose `sticky` is set was not a tie.
/// - An adjusted exponent above 6144 is an overflow, and gives the infinity.
/// - An exponent above 6111 is folded down by padding the coefficient.
fn round(negative: bool, mut c: Mag, mut e: i64, sticky: bool, pref: i64) -> Raw {
    if c.is_zero() && !sticky {
        return finite(negative, 0, pref);
    }
    if !sticky {
        if e < pref {
            let cap = u32::try_from(pref - e).unwrap_or(u32::MAX);
            let zeros = c.trailing_zeros(cap);
            if zeros > 0 {
                c = c.split_digits(zeros).0;
                e += i64::from(zeros);
            }
        }
        if c.digits() <= PREC && (ETINY..=ETOP).contains(&e) {
            let coefficient = c.to_u128().expect("at most 34 digits");
            return finite(negative, coefficient, e);
        }
    }
    let digits = i64::from(c.digits());
    let cut = (digits - i64::from(PREC)).max(ETINY - e).max(0);
    let mut coefficient;
    if cut > 0 {
        if cut > digits {
            // Everything is below half a unit of the digit above the cut.
            coefficient = 0;
        } else {
            let (quotient, tail) = c.split_digits(cut as u32);
            coefficient = quotient.to_u128().expect("at most 34 digits");
            let up = match tail.against_half() {
                Ordering::Greater => true,
                Ordering::Equal => sticky || coefficient % 2 == 1,
                Ordering::Less => false,
            };
            coefficient += u128::from(up);
        }
        e += cut;
        if digits_u128(coefficient) > PREC {
            coefficient /= 10;
            e += 1;
        }
    } else {
        debug_assert!(!sticky, "a sticky result is cut");
        coefficient = c.to_u128().expect("at most 34 digits");
    }
    if coefficient == 0 {
        return finite(negative, 0, e);
    }
    if e + i64::from(digits_u128(coefficient)) - 1 > EMAX {
        return Raw::Inf { negative };
    }
    if e > ETOP {
        coefficient *= 10u128.pow((e - ETOP) as u32);
        e = ETOP;
    }
    finite(negative, coefficient, e)
}

// ---------------------------------------------------------------------------
// Sums
// ---------------------------------------------------------------------------

/// The signed limbs of a sum, by position (position `p` is `10^(19 p)`).
///
/// A contiguous window while the positions held span at most [`WINDOW`], and a
/// map beyond: a window costs its span, not its terms, and 50,000 groups each
/// holding `1E+6111` and `1E-6176` would hold about 520 MB as windows. Both are
/// deterministic in content and in iteration order.
#[derive(Clone, Debug)]
enum Limbs {
    Empty,
    Window { base: i64, limbs: Vec<i128> },
    Sparse(BTreeMap<i64, i128>),
}

/// The widest window, in limb positions (256 bytes; a price total uses one or
/// two).
const WINDOW: i64 = 16;

impl Limbs {
    /// Add `value` at `position`.
    fn add(&mut self, position: i64, value: i128) {
        if value == 0 {
            return;
        }
        match self {
            Limbs::Empty => {
                *self = Limbs::Window { base: position, limbs: vec![value] };
            }
            Limbs::Window { base, limbs } => {
                let low = (*base).min(position);
                let high = (*base + limbs.len() as i64 - 1).max(position);
                if high - low < WINDOW {
                    if low < *base {
                        let grow = (*base - low) as usize;
                        limbs.splice(0..0, std::iter::repeat_n(0, grow));
                        *base = low;
                    }
                    let needed = (high - *base + 1) as usize;
                    if limbs.len() < needed {
                        limbs.resize(needed, 0);
                    }
                    limbs[(position - *base) as usize] += value;
                } else {
                    let mut map: BTreeMap<i64, i128> = limbs
                        .iter()
                        .enumerate()
                        .filter(|(_, v)| **v != 0)
                        .map(|(i, v)| (*base + i as i64, *v))
                        .collect();
                    *map.entry(position).or_insert(0) += value;
                    *self = Limbs::Sparse(map);
                }
            }
            Limbs::Sparse(map) => {
                *map.entry(position).or_insert(0) += value;
            }
        }
    }

    /// The lowest and highest position held, if any.
    fn span(&self) -> Option<(i64, i64)> {
        match self {
            Limbs::Empty => None,
            Limbs::Window { base, limbs } => Some((*base, *base + limbs.len() as i64 - 1)),
            Limbs::Sparse(map) => Some((*map.keys().next()?, *map.keys().next_back()?)),
        }
    }

    fn get(&self, position: i64) -> i128 {
        match self {
            Limbs::Empty => 0,
            Limbs::Window { base, limbs } => usize::try_from(position - *base)
                .ok()
                .and_then(|i| limbs.get(i))
                .copied()
                .unwrap_or(0),
            Limbs::Sparse(map) => map.get(&position).copied().unwrap_or(0),
        }
    }
}

/// An exact sum of decimals, rounded once at the end.
///
/// The result is a function of the multiset of terms: the exact integer sum
/// (commutative and associative), the least exponent (a minimum), "every term
/// a negative zero" (a conjunction), the NaN and infinity flags
/// (disjunctions), and the count. It does not depend on the order terms were
/// added, so a `$sum` over a group gives the same bytes under any plan.
#[derive(Clone, Debug)]
pub struct ExactSum {
    limbs: Limbs,
    /// The least exponent among the terms, zeros included.
    min_exponent: i64,
    all_negative_zero: bool,
    nan: bool,
    positive_infinity: bool,
    negative_infinity: bool,
    count: u64,
}

impl Default for ExactSum {
    fn default() -> Self {
        ExactSum::new()
    }
}

impl ExactSum {
    pub fn new() -> Self {
        ExactSum {
            limbs: Limbs::Empty,
            min_exponent: i64::MAX,
            all_negative_zero: true,
            nan: false,
            positive_infinity: false,
            negative_infinity: false,
            count: 0,
        }
    }

    /// The number of terms added.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Add one term, as it was converted.
    pub fn add(&mut self, term: &Raw) {
        self.count += 1;
        match *term {
            Raw::Nan => self.nan = true,
            Raw::Inf { negative: true } => self.negative_infinity = true,
            Raw::Inf { negative: false } => self.positive_infinity = true,
            Raw::Finite { negative, coefficient, exponent } => {
                let exponent = i64::from(exponent);
                self.min_exponent = self.min_exponent.min(exponent);
                self.all_negative_zero &= coefficient == 0 && negative;
                let (position, offset) = (
                    exponent.div_euclid(i64::from(LIMB_DIGITS)),
                    exponent.rem_euclid(i64::from(LIMB_DIGITS)) as u32,
                );
                // `coefficient · 10^offset`: below 10^53, three limbs.
                let (low, high) = (coefficient % LIMB_WIDE, coefficient / LIMB_WIDE);
                let scale = u128::from(10u64.pow(offset));
                let l0 = low * scale;
                let l1 = high * scale + l0 / LIMB_WIDE;
                let limbs = [l0 % LIMB_WIDE, l1 % LIMB_WIDE, l1 / LIMB_WIDE];
                for (i, limb) in limbs.into_iter().enumerate() {
                    self.limbs.add(
                        position + i as i64,
                        if negative { -(limb as i128) } else { limb as i128 },
                    );
                }
            }
        }
    }

    /// Add `total`, the exact sum of `count` integers, as one term of exponent
    /// 0 and a positive zero's sign.
    pub fn add_integers(&mut self, total: i128, count: u64) {
        if count == 0 {
            return;
        }
        self.count += count;
        self.min_exponent = self.min_exponent.min(0);
        self.all_negative_zero = false;
        let mut magnitude = total.unsigned_abs();
        let mut position = 0;
        while magnitude != 0 {
            let limb = (magnitude % LIMB_WIDE) as i128;
            self.limbs.add(position, if total < 0 { -limb } else { limb });
            magnitude /= LIMB_WIDE;
            position += 1;
        }
    }

    /// The NaN or infinity this sum is, if it is one.
    fn special(&self) -> Option<Raw> {
        if self.nan || (self.positive_infinity && self.negative_infinity) {
            Some(Raw::Nan)
        } else if self.positive_infinity || self.negative_infinity {
            Some(Raw::Inf { negative: self.negative_infinity })
        } else {
            None
        }
    }

    /// The exact sum of the finite terms: its sign and magnitude, at the
    /// exponent `min_exponent`.
    fn exact(&self) -> (bool, Mag) {
        let Some((low, high)) = self.limbs.span() else {
            return (false, Mag::zero());
        };
        let mut digits: Vec<u64> = Vec::with_capacity((high - low + 3) as usize);
        let mut carry: i128 = 0;
        for position in low..=high {
            let value = self.limbs.get(position) + carry;
            digits.push(value.rem_euclid(LIMB_WIDE as i128) as u64);
            carry = value.div_euclid(LIMB_WIDE as i128);
        }
        let negative = carry < 0;
        if negative {
            // The value is `digits - |carry| · B^n`: negate it by borrowing.
            let mut borrow = 0u64;
            for limb in &mut digits {
                let taken = *limb + borrow;
                if taken == 0 {
                    *limb = 0;
                    borrow = 0;
                } else {
                    *limb = LIMB - taken;
                    borrow = 1;
                }
            }
            let mut top = carry.unsigned_abs() - u128::from(borrow);
            while top != 0 {
                digits.push((top % LIMB_WIDE) as u64);
                top /= LIMB_WIDE;
            }
        } else {
            let mut top = carry.unsigned_abs();
            while top != 0 {
                digits.push((top % LIMB_WIDE) as u64);
                top /= LIMB_WIDE;
            }
        }
        let mut magnitude = Mag(digits);
        magnitude.trim();
        // The limbs start at `10^(19 low)`; the sum is wanted at `min_exponent`.
        let shift = 19 * low - self.min_exponent;
        if shift >= 0 {
            magnitude.mul_pow10(shift as u32);
        } else if !magnitude.is_zero() {
            // Every term's exponent is at least `min_exponent`: the digits
            // below it are zeros.
            magnitude = magnitude.split_digits((-shift) as u32).0;
        }
        (negative, magnitude)
    }

    /// The sum, rounded once. The quantum is the least exponent among the terms
    /// (zeros and integers included).
    pub fn finish_sum(&self) -> Raw {
        if let Some(special) = self.special() {
            return special;
        }
        if self.count == 0 {
            return finite(false, 0, 0);
        }
        let (negative, magnitude) = self.exact();
        if magnitude.is_zero() {
            return finite(self.all_negative_zero, 0, self.min_exponent);
        }
        round(negative, magnitude, self.min_exponent, false, self.min_exponent)
    }

    /// The mean: the exact sum divided by the number of terms, rounded once,
    /// at the least term exponent. Not the sum rounded and then divided, which
    /// rounds twice.
    ///
    /// The sum's top `35 + digits(n) + 1` digits and one sticky bit for the
    /// rest are divided by `n`: the quotient has at least 35 digits and the
    /// rounding is decided by the digits kept and the sticky bit. The cut is
    /// exact: with `C = C'·10^t + L`, `L < 10^t`, `⌊C / (n·10^t)⌋ = ⌊C' / n⌋`,
    /// and the true quotient lies in `[q, q + 1)` units of the kept digit.
    pub fn finish_avg(&self) -> Raw {
        if let Some(special) = self.special() {
            return special;
        }
        if self.count == 0 {
            return Raw::Nan;
        }
        let n = self.count;
        let (negative, magnitude) = self.exact();
        if magnitude.is_zero() {
            return finite(self.all_negative_zero, 0, self.min_exponent);
        }
        let keep = i64::from(PREC) + 1 + i64::from(digits_u128(u128::from(n))) + 1;
        let cut = (i64::from(magnitude.digits()) - keep).max(0);
        if cut == 0 {
            return divide(negative, &magnitude, self.min_exponent, u128::from(n), 0);
        }
        let (top, tail) = magnitude.split_digits(cut as u32);
        let (quotient, remainder) = top.to_big().div_rem_u128(u128::from(n));
        round(
            negative,
            Mag::from_u128(quotient),
            self.min_exponent + cut,
            remainder != 0 || !tail.is_zero(),
            self.min_exponent,
        )
    }
}

/// `a + b`, rounded once.
pub fn add(a: &Raw, b: &Raw) -> Raw {
    let mut sum = ExactSum::new();
    sum.add(a);
    sum.add(b);
    sum.finish_sum()
}

/// `a - b`, rounded once: the exact sum of `a` and `-b`.
pub fn sub(a: &Raw, b: &Raw) -> Raw {
    add(a, &b.negated())
}

// ---------------------------------------------------------------------------
// Products and quotients
// ---------------------------------------------------------------------------

/// `a × b`, rounded once: the exact product of the coefficients at the sum of
/// the exponents (the preferred exponent), so `1.5 × 1.0` is `1.50` and
/// `0 × 1.50` is `0.00`.
pub fn mul(a: &Raw, b: &Raw) -> Raw {
    let negative =
        |r: &Raw| matches!(r, Raw::Inf { negative: true } | Raw::Finite { negative: true, .. });
    if matches!(a, Raw::Nan) || matches!(b, Raw::Nan) {
        return Raw::Nan;
    }
    let sign = negative(a) != negative(b);
    match (a, b) {
        (Raw::Inf { .. }, other) | (other, Raw::Inf { .. }) => {
            // An infinity with a zero is invalid; with anything else it is an
            // infinity of the product's sign.
            if other.is_zero() { Raw::Nan } else { Raw::Inf { negative: sign } }
        }
        (
            Raw::Finite { coefficient: c1, exponent: e1, .. },
            Raw::Finite { coefficient: c2, exponent: e2, .. },
        ) => {
            let exponent = i64::from(*e1) + i64::from(*e2);
            round(sign, Mag::from_u128(*c1).mul(&Mag::from_u128(*c2)), exponent, false, exponent)
        }
        _ => unreachable!("a NaN returned above"),
    }
}

/// The product of any number of operands, **left to right, each step rounded**:
/// a chain of two-operand multiplications, as IEEE 754 defines a sequence of
/// operations and as MongoDB multiplies. It is linear in the number of operands
/// and a long chain cannot grow an exact value without bound. It depends on the
/// order: `[1E+5000, 1E+5000, 1E-5000]` overflows on the first step and
/// `[1E+5000, 1E-5000, 1E+5000]` does not. A single operand is returned as the
/// canonical decimal it is.
pub fn product(operands: &[Raw]) -> Raw {
    match operands {
        [] => from_i64(1),
        [only] => mul(&from_i64(1), only),
        [first, rest @ ..] => rest.iter().fold(*first, |acc, next| mul(&acc, next)),
    }
}

/// `c1 · 10^e1 / (c2 · 10^e2)`, rounded once, for a nonzero `c2`: the dividend
/// is scaled to give at least 35 quotient digits and the remainder is the
/// sticky bit.
fn divide(negative: bool, c1: &Mag, e1: i64, c2: u128, e2: i64) -> Raw {
    let scale = (i64::from(PREC) + 1 + i64::from(digits_u128(c2)) - i64::from(c1.digits())).max(0);
    let mut dividend = c1.clone();
    dividend.mul_pow10(scale as u32);
    let (quotient, remainder) = dividend.to_big().div_rem_u128(c2);
    round(negative, Mag::from_u128(quotient), e1 - scale - e2, remainder != 0, e1 - e2)
}

/// `a / b`. Any zero divisor is refused, whatever the dividend: the rule
/// `$divide` applies to every number.
pub fn div(a: &Raw, b: &Raw) -> Result<Raw, DivisionByZero> {
    if b.is_zero() {
        return Err(DivisionByZero);
    }
    let (
        Raw::Finite { negative: na, coefficient: c1, exponent: e1 },
        Raw::Finite { negative: nb, coefficient: c2, exponent: e2 },
    ) = (a, b)
    else {
        return Ok(match (a, b) {
            (Raw::Nan, _) | (_, Raw::Nan) => Raw::Nan,
            (Raw::Inf { .. }, Raw::Inf { .. }) => Raw::Nan,
            (Raw::Inf { negative: na }, Raw::Finite { negative: nb, .. }) => {
                Raw::Inf { negative: na != nb }
            }
            (Raw::Finite { negative: na, .. }, Raw::Inf { negative: nb }) => {
                finite(na != nb, 0, ETINY)
            }
            (Raw::Finite { .. }, Raw::Finite { .. }) => unreachable!("matched above"),
        });
    };
    let negative = na != nb;
    Ok(divide(negative, &Mag::from_u128(*c1), i64::from(*e1), *c2, i64::from(*e2)))
}

/// An integer, exactly, at exponent 0.
pub fn from_i64(value: i64) -> Raw {
    Raw::Finite { negative: value < 0, coefficient: u128::from(value.unsigned_abs()), exponent: 0 }
}

// ---------------------------------------------------------------------------
// Binary64
// ---------------------------------------------------------------------------

/// `5^(27 j)` for `j` up to 40, built once: the largest `k = -f` a double has
/// is 1074, which is 39 whole steps of 27 and a remainder.
fn powers_of_five() -> &'static [Mag] {
    static TABLE: std::sync::OnceLock<Vec<Mag>> = std::sync::OnceLock::new();
    TABLE.get_or_init(|| {
        let step = 5u64.pow(27);
        let mut table = vec![Mag::from_u64(1)];
        for _ in 1..=40 {
            let mut next = table.last().expect("one entry").clone();
            next.mul_small(step);
            table.push(next);
        }
        table
    })
}

/// **R1**: a binary64 (its 64 bits) as a `Decimal128`, IEEE 754
/// `convertFormat` under round-half-even: its exact value, rounded once to 34
/// digits. Whether the conversion was inexact is the second answer; it is IEEE's
/// inexact signal, which the update guard reads.
///
/// - NaN is the quiet NaN, an infinity an infinity, and `±0.0` is `±0` at
///   exponent 0.
/// - Otherwise the double is `m · 2^f` with `m` odd: the integer `m · 2^f` at
///   exponent 0 when `f >= 0`, and `(m · 5^-f) · 10^f` when `f < 0`, IEEE's
///   preferred exponent for an exact conversion (0 when integral, otherwise
///   the largest exponent that holds the value). More than 34 digits are
///   rounded half to even and the exponent rises by the digits dropped.
/// - It cannot overflow, underflow or need clamping.
///
/// The answer is inexact exactly when the value changed (IEEE's inexact
/// signal), not whenever digits were dropped: dropping zeros loses nothing.
pub fn from_binary64(bits: u64) -> (Raw, bool) {
    let negative = bits >> 63 == 1;
    let biased = ((bits >> 52) & 0x7FF) as i64;
    let fraction = bits & ((1u64 << 52) - 1);
    if biased == 0x7FF {
        return (if fraction == 0 { Raw::Inf { negative } } else { Raw::Nan }, false);
    }
    if biased == 0 && fraction == 0 {
        return (finite(negative, 0, 0), false);
    }
    // No implicit bit for a subnormal.
    let (mut m, mut f) =
        if biased == 0 { (fraction, -1074i64) } else { (fraction | 1 << 52, biased - 1075) };
    let shift = m.trailing_zeros();
    m >>= shift;
    f += i64::from(shift);
    let (mut value, exponent) = if f >= 0 {
        let mut value = Mag::from_u64(m);
        let mut left = f;
        while left > 0 {
            let step = left.min(63);
            value.mul_small(1u64 << step);
            left -= step;
        }
        (value, 0)
    } else {
        // `m · 5^k` with `k = -f` up to 1074: `5^(k mod 27)` folds into the
        // mantissa (below 2^113), and the rest is one entry of the table of
        // `5^(27 j)`, so the worst double is a single pass over about 40 limbs.
        let k = (-f) as u32;
        let small = u128::from(m) * u128::from(5u64.pow(k % 27));
        let value = Mag::from_u128(small);
        let value = match k / 27 {
            0 => value,
            j => value.mul(&powers_of_five()[j as usize]),
        };
        (value, f)
    };
    let digits = value.digits();
    if digits <= PREC {
        let coefficient = value.to_u128().expect("at most 34 digits");
        return (finite(negative, coefficient, exponent), false);
    }
    let cut = digits - PREC;
    let (quotient, tail) = value.split_digits(cut);
    value = quotient;
    let mut coefficient = value.to_u128().expect("34 digits");
    let mut e = exponent + i64::from(cut);
    let up = match tail.against_half() {
        Ordering::Greater => true,
        Ordering::Equal => coefficient % 2 == 1,
        Ordering::Less => false,
    };
    coefficient += u128::from(up);
    if digits_u128(coefficient) > PREC {
        coefficient /= 10;
        e += 1;
    }
    // Inexact only if the digits dropped were not all zero: an integral double
    // with trailing zeros past the 34th digit is still its exact value.
    (finite(negative, coefficient, e), !tail.is_zero())
}

/// A `Decimal128` as the nearest binary64 (its 64 bits), round half to even,
/// from the exact value: IEEE 754 `convertFormat` decimal128 to binary64.
///
/// It reads the 128-bit binary window and the end bit that the key encoder
/// computes for the exact value, never the text. Beyond the double range the
/// result is an infinity, a tiny value is a subnormal or a zero, and a NaN is
/// the quiet NaN.
pub fn to_binary64(raw: &Raw) -> u64 {
    const QUIET_NAN: u64 = 0x7FF8_0000_0000_0000;
    const SIGN: u64 = 1 << 63;
    const INFINITY: u64 = 0x7FF0_0000_0000_0000;
    let (negative, coefficient, exponent) = match *raw {
        Raw::Nan => return QUIET_NAN,
        Raw::Inf { negative } => return INFINITY | if negative { SIGN } else { 0 },
        Raw::Finite { negative, coefficient, exponent } => (negative, coefficient, exponent),
    };
    let sign = if negative { SIGN } else { 0 };
    if coefficient == 0 {
        return sign;
    }
    let body = number::decimal(coefficient, exponent);
    // The value is `(window + tail) · 2^(exp − 127)`, the window's top bit set.
    let (exp, window) = (i64::from(body.exp), body.window);
    if exp > 1023 {
        return sign | INFINITY;
    }
    // How many low bits of the window fall below the last kept bit: 75 leave
    // 53 for a normal number, and a subnormal keeps fewer, down to none.
    let (dropped, normal) = if exp >= -1022 {
        (75u32, true)
    } else {
        (u32::try_from((127 - exp) - 1074).unwrap_or(u32::MAX), false)
    };
    if dropped > 128 {
        // Strictly below half of the smallest subnormal: it rounds to zero.
        return sign;
    }
    let (mut kept, remainder, half) = if dropped == 128 {
        // Only the window's own bits are below the kept ones: none are kept.
        (0u128, window, 1u128 << 127)
    } else {
        (window >> dropped, window & ((1u128 << dropped) - 1), 1u128 << (dropped - 1))
    };
    let up = match remainder.cmp(&half) {
        Ordering::Greater => true,
        Ordering::Equal => body.inexact || kept % 2 == 1,
        Ordering::Less => false,
    };
    kept += u128::from(up);
    let kept = kept as u64;
    if normal {
        let (mut mantissa, mut exp) = (kept, exp);
        if mantissa == 1 << 53 {
            mantissa >>= 1;
            exp += 1;
        }
        if exp > 1023 {
            return sign | INFINITY;
        }
        sign | (((exp + 1023) as u64) << 52) | (mantissa & ((1 << 52) - 1))
    } else {
        // A subnormal's bits are its quantum count; a carry into bit 52 is the
        // smallest normal, whose exponent field is 1.
        sign | kept
    }
}
