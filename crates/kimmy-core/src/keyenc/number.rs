//! The body of a finite number's key: its exponent, the first 128 bits of its
//! binary expansion, and whether any bit after those is nonzero.
//!
//! For `Int32`, `Int64` and `Double` all three come from the 64-bit normalised
//! mantissa: the window is the mantissa followed by 64 zero bits, and nothing
//! follows it. For a `Decimal128` `c · 10^e` the window is the head of an
//! expansion that can run to 14,000 bits (`10^-6176`), so it is taken from a
//! table of powers of five the way a float parser does, with an exact path
//! behind it, and the exactness mark has a closed form that needs no long
//! division (design §4.3).
//!
//! **Two table tiers, so no coefficient reaches the slow path.** The first tier
//! (192 bits) answers almost every value, but a writer can choose coefficients
//! (continued fractions of a table entry) that land within the table's error of
//! a carry, and each of those cost the exact path: 80 µs at `e = −6176`. The
//! second tier (384 bits) answers those: at its width, a coefficient below
//! `10^34` can sit that close to a carry only if its expansion really ends,
//! which is proved over every entry by the test
//! `no_coefficient_defers_past_the_second_tier`. Such a value (`5^q` divides `c`,
//! `q <= 48`) goes straight to the exact path, which is short at those
//! exponents.
//!
//! This is written independently of the comparator: it shares only the small
//! big integer. The tests check it against the comparator, against an
//! exact-rational key written separately, and each path against the other.

use std::sync::OnceLock;

use crate::cmp::big::Big;
use crate::decimal::{MAX_EXPONENT, MIN_EXPONENT};

/// The three parts of a finite number's key body.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Body {
    /// `⌊log2 |x|⌋`: the key's exponent field before the bias.
    pub(crate) exp: i32,
    /// `⌊|x| · 2^(127 − exp)⌋`, the first 128 bits of the expansion; its top bit
    /// is set.
    pub(crate) window: u128,
    /// Whether some bit after the window is nonzero, so the value is strictly
    /// greater than the window says. A property of the value alone (equal
    /// values agree on it), never of how the value was written.
    pub(crate) inexact: bool,
}

/// The body of `mantissa · 2^(exp − 63)`, mantissa normalised: every binary
/// value has at most 64 significant bits, so its key is always exact.
pub(crate) fn binary(exp: i16, mantissa: u64) -> Body {
    Body { exp: i32::from(exp), window: u128::from(mantissa) << 64, inexact: false }
}

/// The body of the decimal `c · 10^e` (`c` nonzero).
pub(crate) fn decimal(c: u128, e: i16) -> Body {
    let inexact = inexact(c, e);
    let (exp, window) = if e < 0 && !inexact {
        // The expansion ends (`5^q` divides `c`, `q <= 48`): its window is
        // followed by zeros only, and both tiers' truncated reciprocals put the
        // product just below that carry, so both would defer. The exact path is
        // tens of nanoseconds at these exponents.
        exact_window(c, e)
    } else {
        fast_window(c, e).or_else(|| wide_window(c, e)).unwrap_or_else(|| {
            // Unreachable for a canonical coefficient (see the module
            // documentation), and correct regardless.
            #[cfg(test)]
            tests::FELL_BACK.with(|n| n.set(n.get() + 1));
            exact_window(c, e)
        })
    };
    Body { exp, window, inexact }
}

fn exact_window(c: u128, e: i16) -> (i32, u128) {
    let exact = exact_body(c, e);
    (exact.exp, exact.window)
}

/// `log2 5` in fixed point with 64 fractional bits, rounded down. It is
/// irrational, so the true value lies strictly between this and the next
/// integer. (`log2 10 = 1 + log2 5`: the comparator's constant less `2^64`.)
const LOG2_5_LO: u128 = 42_832_013_323_943_160_825;
const LOG2_5_HI: u128 = LOG2_5_LO + 1;

/// `⌈k · log2 5⌉` or a little above it, never below.
fn ceil_log2_pow5(k: u32) -> u32 {
    ((u128::from(k) * LOG2_5_HI + (1 << 64) - 1) >> 64) as u32
}

// ---------------------------------------------------------------------------
// END, from a closed form
// ---------------------------------------------------------------------------

/// Whether some bit of `c · 10^e`'s expansion after the first 128 is nonzero.
///
/// - `e >= 0`: the value is `N · 2^e` with `N = c · 5^e`, and `2^e` only moves
///   the exponent, so the tail of the value is the tail of `N`. With
///   `B = bit_length(N)`, the bits of `N` after its first 128 are all zero
///   exactly when `B <= 128` or `B − 128 <= v2(N)`, and `v2(N) = v2(c)` because
///   `5^e` is odd.
/// - `e < 0`, `q = −e`: the value is `c / (5^q · 2^q)`, whose expansion ends
///   exactly when `5^q` divides `c`. Every `q >= 49` is inexact, since
///   `c < 10^34 < 5^49`; below that it is one `u128` remainder. When it ends,
///   the quotient is an integer at exponent `−q` and the `e >= 0` rule applies
///   to it.
pub(crate) fn inexact(c: u128, e: i16) -> bool {
    if e >= 0 {
        return integer_tail_nonzero(c, e as u32);
    }
    let q = u32::from(e.unsigned_abs());
    if q >= 49 {
        return true;
    }
    let power = 5u128.pow(q);
    if !c.is_multiple_of(power) {
        return true;
    }
    // `c / 5^q` is below `2^113`, so this is always exact; kept as the rule
    // says rather than as its consequence.
    integer_tail_nonzero(c / power, 0)
}

/// Whether `N = c · 5^e` has a nonzero bit after its first 128.
///
/// `B` is known to within a couple of bits from the `log2 5` constants; only
/// when the answer differs across that range is `N` built.
fn integer_tail_nonzero(c: u128, e: u32) -> bool {
    let exact_up_to = 128 + c.trailing_zeros();
    // `log2 N` lies in `[log2_c + e · log2 5, log2_c + 1 + e · log2 5)`.
    let log2_c = 127 - c.leading_zeros();
    let lowest = log2_c + ((u128::from(e) * LOG2_5_LO) >> 64) as u32 + 1;
    let highest = log2_c + ceil_log2_pow5(e) + 1;
    if highest <= exact_up_to {
        return false;
    }
    if lowest > exact_up_to {
        return true;
    }
    let mut n = Big::from_u128(c);
    n.mul_pow5(e);
    n.bit_len() > exact_up_to
}

// ---------------------------------------------------------------------------
// The window, exactly
// ---------------------------------------------------------------------------

/// The body of `c · 10^e` by exact integer arithmetic: the fallback that makes
/// the fast path correct, and the reference the fast path is tested against.
///
/// - `e >= 0`: `N = c · 5^e`, built by multiplications by `5^27`; the window is
///   its top 128 bits, `exp = bit_length(N) − 1 + e`.
/// - `e < 0`, `q = −e`: `Y = c · 2^k` divided by `5^q` (divisions by `5^27`; a
///   floor of a floor is the floor), the window the top 128 bits of the
///   quotient. `k = 129 + ⌈q · log2 5⌉ − bit_length(c) + 1` makes the quotient
///   at least `2^129`, so its top 128 bits are a truncation, never a shift that
///   would invent zeros where the floor discarded bits.
///
/// Its own exactness mark (every remainder zero and every bit below the window
/// zero) is what the closed form of [`inexact`] is tested against.
pub(crate) fn exact_body(c: u128, e: i16) -> Body {
    let mut n = Big::from_u128(c);
    let (exp, divides) = if e >= 0 {
        n.mul_pow5(e as u32);
        (n.bit_len() as i32 - 1 + i32::from(e), true)
    } else {
        let q = u32::from(e.unsigned_abs());
        let length_c = 128 - c.leading_zeros();
        let k = (129 + ceil_log2_pow5(q) + 1).saturating_sub(length_c);
        n.shl(k);
        let divides = n.div_pow5(q);
        debug_assert!(n.bit_len() >= 130, "the quotient keeps 130 bits");
        (n.bit_len() as i32 - 1 - q as i32 - k as i32, divides)
    };
    let (top, rest_zero) = n.top_bits::<2>(128);
    Body {
        exp,
        window: u128::from(top[0]) | (u128::from(top[1]) << 64),
        inexact: !(divides && rest_zero),
    }
}

// ---------------------------------------------------------------------------
// The window, fast
// ---------------------------------------------------------------------------

/// The width of each tier's mantissas, in 64-bit limbs: 192 and 384 bits.
pub(crate) const NARROW: usize = 3;
pub(crate) const WIDE: usize = 6;

/// `5^e` to `64 · L` bits: `5^e = (mantissa + δ) · 2^shift` with `0 <= δ < 1`,
/// the mantissa's top bit set. Truncated, never rounded, so the error is
/// one-sided.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Pow5<const L: usize> {
    pub(crate) mantissa: [u64; L],
    pub(crate) shift: i32,
}

impl<const L: usize> Pow5<L> {
    fn of(value: &Big, scale: u32) -> Self {
        let width = 64 * L as u32;
        let (mantissa, _) = value.top_bits::<L>(width);
        Pow5 { mantissa, shift: value.bit_len() as i32 - width as i32 - scale as i32 }
    }
}

/// Every power `5^0 ..= 5^6111`, each exact to its first `64 · L` bits, by
/// multiplying by five.
fn positive_powers<const L: usize>() -> Vec<Pow5<L>> {
    let mut power = Big::from_u128(1);
    (0..=MAX_EXPONENT)
        .map(|_| {
            let entry = Pow5::of(&power, 0);
            power.mul_small(5);
            entry
        })
        .collect()
}

/// `5^-1 ..= 5^-6176`, each the exact truncation of its first `64 · L` bits.
///
/// Built by repeated division of one wide power of two by five:
/// `⌊⌊2^W / 5^q⌋ / 5⌋ = ⌊2^W / 5^(q+1)⌋`, so every step is still the exact
/// floor, and its top bits are the exact truncation of `2^W / 5^q`. `W` leaves
/// at least `64 · L` bits at `q = 6176` (`5^6176` is 14,341 bits long).
fn negative_powers<const L: usize>() -> Vec<Pow5<L>> {
    let width = 14_341 + 64 * L as u32 + 64;
    let mut reciprocal = Big::from_u128(1);
    reciprocal.shl(width);
    (1..=u32::from(MIN_EXPONENT.unsigned_abs()))
        .map(|_| {
            reciprocal.div_small(5);
            assert!(reciprocal.bit_len() >= 64 * L as u32, "the table keeps its width");
            Pow5::of(&reciprocal, width)
        })
        .collect()
}

/// The first tier's entry for `5^e`. Built on first use, each half on its own:
/// a workload with no positive (or no negative) decimal exponent never builds
/// that half. About 390 KB, and a millisecond and three to four milliseconds
/// to build in a release build, once, inside whatever first encodes such a
/// decimal.
pub(crate) fn narrow(e: i16) -> &'static Pow5<NARROW> {
    static POSITIVE: OnceLock<Vec<Pow5<NARROW>>> = OnceLock::new();
    static NEGATIVE: OnceLock<Vec<Pow5<NARROW>>> = OnceLock::new();
    if e >= 0 {
        &POSITIVE.get_or_init(positive_powers)[e as usize]
    } else {
        &NEGATIVE.get_or_init(negative_powers)[usize::from(e.unsigned_abs()) - 1]
    }
}

/// The second tier's entry for `5^e`, built on first use like the first: about
/// 690 KB. Only a coefficient chosen against the first tier reaches it.
pub(crate) fn wide(e: i16) -> &'static Pow5<WIDE> {
    static POSITIVE: OnceLock<Vec<Pow5<WIDE>>> = OnceLock::new();
    static NEGATIVE: OnceLock<Vec<Pow5<WIDE>>> = OnceLock::new();
    if e >= 0 {
        &POSITIVE.get_or_init(positive_powers)[e as usize]
    } else {
        &NEGATIVE.get_or_init(negative_powers)[usize::from(e.unsigned_abs()) - 1]
    }
}

/// The window of `c · 10^e` from the first tier, or `None` when the table's
/// error could reach the window.
pub(crate) fn fast_window(c: u128, e: i16) -> Option<(i32, u128)> {
    window_from::<NARROW, { NARROW + 2 }>(c, e, narrow(e))
}

/// The window of `c · 10^e` from the second tier, or `None` as above.
pub(crate) fn wide_window(c: u128, e: i16) -> Option<(i32, u128)> {
    window_from::<WIDE, { WIDE + 2 }>(c, e, wide(e))
}

/// The window from one tier's entry `5^e = (mantissa + δ) · 2^shift`, or `None`.
///
/// `c · 10^e = c · 5^e · 2^e = (P + c · δ) · 2^(shift + e)` with
/// `P = c · mantissa` and `0 <= c · δ < c`. So the true value, in units of
/// `2^(shift + e)`, lies in `[P, P + c)`, and its top 128 bits are `P`'s
/// whenever adding anything below `c` to `P` leaves them as they are, which is
/// a question about `P` and `c` alone: does `P + c − 1` still have the same
/// bits from the cut up? Below the cut sit `64 · L − 129` bits more than `c`
/// has (63 in the first tier, 255 in the second), so a random coefficient
/// defers about once in `2^63` from the first; a value whose expansion has a
/// long run of zeros or ones just below the window defers, as it must.
///
/// **Every decimal whose expansion ends, at a negative exponent, is such a
/// value** (`1.5`, `0.25`, `19.50`: `5^q` divides `c`). Its window is followed
/// by zeros only, and the truncated reciprocal puts `P` just below that carry
/// (`P = c' · 2^S − c' · (2^S mod 5^q)` with `c = c' · 5^q`), in either tier;
/// [`decimal`] sends those to the exact path without asking.
///
/// Fixed widths (`P = L + 2` limbs for the product), so each tier compiles to
/// straight-line arithmetic.
fn window_from<const L: usize, const P: usize>(
    c: u128,
    e: i16,
    power: &Pow5<L>,
) -> Option<(i32, u128)> {
    let mut product = mul_wide::<L, P>(c, &power.mantissa);
    let length = 64 * P as u32 - leading_zeros(&product);
    let cut = length - 128;
    let window = bits_from(&product, cut);
    add(&mut product, c - 1);
    if bits_from(&product, cut) != window {
        return None;
    }
    Some((length as i32 - 1 + power.shift + i32::from(e), window))
}

/// `c · t` for a 128-bit `c`, in two limbs more than `t` has (`P = L + 2`).
fn mul_wide<const L: usize, const P: usize>(c: u128, t: &[u64; L]) -> [u64; P] {
    const { assert!(P == L + 2) };
    let mut product = [0u64; P];
    for (i, a) in [c as u64, (c >> 64) as u64].into_iter().enumerate() {
        let mut carry = 0u128;
        for (j, &b) in t.iter().enumerate() {
            // At most (2^64 − 1)^2 + 2 · (2^64 − 1) = 2^128 − 1.
            let current = u128::from(a) * u128::from(b) + u128::from(product[i + j]) + carry;
            product[i + j] = current as u64;
            carry = current >> 64;
        }
        product[i + L] = carry as u64;
    }
    product
}

fn leading_zeros(n: &[u64]) -> u32 {
    let mut zeros = 0;
    for &limb in n.iter().rev() {
        zeros += limb.leading_zeros();
        if limb != 0 {
            break;
        }
    }
    zeros
}

/// The 128 bits of `n` from bit `cut` up.
fn bits_from(n: &[u64], cut: u32) -> u128 {
    let (whole, part) = ((cut / 64) as usize, cut % 64);
    let limb = |i: usize| n.get(i).copied().map_or(0, u128::from);
    let low = limb(whole) | (limb(whole + 1) << 64);
    if part == 0 { low } else { (low >> part) | (limb(whole + 2) << (128 - part)) }
}

/// `n += v`. A product is at most `113 + 64 · L` bits long in a buffer of
/// `64 · (L + 2)`, and `v` is below `2^113`, so the sum cannot leave it.
fn add(n: &mut [u64], v: u128) {
    let mut carry = v;
    for limb in n.iter_mut() {
        if carry == 0 {
            break;
        }
        let sum = u128::from(*limb) + (carry & u128::from(u64::MAX));
        *limb = sum as u64;
        carry = (carry >> 64) + (sum >> 64);
    }
}

#[cfg(test)]
mod tests {
    use num_bigint::BigUint;
    use proptest::prelude::*;

    use super::*;
    use crate::cmp::oracle::{SplitMix, any_coefficient, any_exponent};
    use crate::decimal::MAX_COEFFICIENT;

    thread_local! {
        /// How many times [`decimal`] fell back to the exact path on this
        /// thread: the route, observed, so a test can say the tables answered.
        pub(super) static FELL_BACK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    fn fell_back() -> usize {
        FELL_BACK.with(|n| n.get())
    }

    fn big(limbs: &[u64]) -> BigUint {
        BigUint::from_slice(
            &limbs.iter().flat_map(|&l| [l as u32, (l >> 32) as u32]).collect::<Vec<_>>(),
        )
    }

    fn to_u128(n: &BigUint) -> u128 {
        let digits = n.to_u64_digits();
        assert!(digits.len() <= 2, "{n} does not fit 128 bits");
        u128::from(*digits.first().unwrap_or(&0)) | (u128::from(*digits.get(1).unwrap_or(&0)) << 64)
    }

    /// Every entry of one tier against num-bigint, by the defining inequality
    /// rather than by the same construction: `5^e = (mantissa + δ) · 2^shift`,
    /// `0 <= δ < 1`, and the mantissa has exactly `64 · L` bits.
    fn check_tier<const L: usize>(entry: fn(i16) -> &'static Pow5<L>) {
        let width = 64 * L as u64;
        let two = |n: u32| BigUint::from(1u32) << n;
        let mut power = BigUint::from(1u32);
        for e in 0..=MAX_EXPONENT {
            let entry = entry(e);
            let m = big(&entry.mantissa);
            assert_eq!(m.bits(), width, "5^{e}");
            if entry.shift >= 0 {
                let s = entry.shift as u32;
                assert!((&m << s) <= power && power < ((&m + 1u32) << s), "5^{e}");
            } else {
                assert_eq!(m, &power << (-entry.shift) as u32, "5^{e} fits: exact");
            }
            power *= 5u32;
        }
        let mut power = BigUint::from(1u32);
        for q in 1..=u32::from(MIN_EXPONENT.unsigned_abs()) {
            power *= 5u32;
            let entry = entry(-(q as i16));
            let m = big(&entry.mantissa);
            assert_eq!(m.bits(), width, "5^-{q}");
            // `5^-q = (m + δ) · 2^shift`  <=>  `m · 5^q <= 2^-shift < (m + 1) · 5^q`.
            let scale = two((-entry.shift) as u32);
            assert!(&m * &power <= scale && scale < (&m + 1u32) * &power, "5^-{q}");
        }
    }

    #[test]
    fn every_first_tier_entry_is_the_exact_truncation_of_its_power() {
        // One flipped low bit in one entry fails.
        check_tier(narrow);
    }

    #[test]
    fn every_second_tier_entry_is_the_exact_truncation_of_its_power() {
        check_tier(wide);
    }

    /// The values whose `END` is decided right at the edge of the closed form:
    /// `B − 128` equal to `v2(c)` and one above it, and `5^q` dividing `c`
    /// exactly against `5^(q−1)` only.
    fn end_boundary_set() -> Vec<(u128, i16)> {
        let one = || BigUint::from(1u32);
        let mut out = Vec::new();
        // Up to `5^50`, which still leaves a choice of odd `o` at each length.
        for e in 0u32..=50 {
            let five = BigUint::from(5u32).pow(e);
            // Odd `o` with `bit_length(o · 5^e)` exactly 127, 128 and 129, the
            // least and the greatest of each: shifted by `v`, `B − 128` is then
            // `v − 1`, `v` (both exact) and `v + 1` (inexact), with `v = v2(c)`.
            for length in [127u32, 128, 129] {
                let least = ((one() << (length - 1)) + &five - 1u32) / &five;
                let greatest = ((one() << length) - 1u32) / &five;
                for o in [least | one(), (greatest.clone() - 1u32) | one()] {
                    let o = if o > greatest { o - 2u32 } else { o };
                    assert_eq!((&o * &five).bits(), u64::from(length), "o · 5^{e}");
                    for v in 0u32..=112 {
                        let c = &o << v;
                        if c > BigUint::from(MAX_COEFFICIENT) {
                            break;
                        }
                        let digits = c.to_u64_digits();
                        let c = u128::from(digits[0])
                            | (u128::from(*digits.get(1).unwrap_or(&0)) << 64);
                        out.push((c, e as i16));
                    }
                }
            }
        }
        for q in 1u32..=48 {
            let five = 5u128.pow(q);
            for k in [1u128, 2, 3, 4, 6, 7, 8, 1 << 20, 3 << 40] {
                let Some(multiple) = five.checked_mul(k) else { continue };
                for c in [multiple, (five / 5) * k, multiple + 1, multiple - 1] {
                    if (1..=MAX_COEFFICIENT).contains(&c) {
                        out.push((c, -(q as i16)));
                    }
                }
            }
        }
        out
    }

    #[test]
    fn the_closed_form_end_agrees_with_the_exact_path_on_the_boundary_set() {
        let set = end_boundary_set();
        let (mut exact, mut truncated) = (0, 0);
        for &(c, e) in &set {
            let reference = exact_body(c, e);
            assert_eq!(inexact(c, e), reference.inexact, "{c}E{e}");
            if reference.inexact { truncated += 1 } else { exact += 1 }
        }
        // Both sides of every edge are in the set.
        assert!(exact > 300 && truncated > 300, "exact {exact}, truncated {truncated}");
    }

    #[test]
    fn the_closed_form_end_agrees_with_the_exact_path_over_a_corpus() {
        // Every exponent where `END` can be exact (`-48..=103`) and well beyond,
        // many coefficients each, plus every exponent of the whole range once.
        let mut rng = SplitMix(1);
        let mut exact = 0;
        for e in -60i16..=130 {
            for _ in 0..200 {
                let c = rng.coefficient();
                let reference = exact_body(c, e);
                assert_eq!(inexact(c, e), reference.inexact, "{c}E{e}");
                exact += usize::from(!reference.inexact);
            }
            for k in 0..113 {
                let c = 1u128 << k;
                assert_eq!(inexact(c, e), exact_body(c, e).inexact, "2^{k}E{e}");
            }
        }
        for e in (MIN_EXPONENT..=MAX_EXPONENT).step_by(7) {
            let c = rng.coefficient();
            assert_eq!(inexact(c, e), exact_body(c, e).inexact, "{c}E{e}");
        }
        assert!(exact > 3000, "the corpus reaches exact values: {exact}");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(3000))]

        /// The fast window, whenever it answers, is the exact window; and the
        /// encoder's body is the exact path's body, `END` included.
        #[test]
        fn the_fast_window_is_the_exact_window(c in any_coefficient(), e in any_exponent()) {
            let reference = exact_body(c, e);
            if let Some((exp, window)) = fast_window(c, e) {
                prop_assert_eq!((exp, window), (reference.exp, reference.window), "{}E{}", c, e);
            }
            if let Some((exp, window)) = wide_window(c, e) {
                prop_assert_eq!((exp, window), (reference.exp, reference.window), "{}E{}", c, e);
            }
            prop_assert_eq!(decimal(c, e), reference, "{}E{}", c, e);
            prop_assert!(reference.window >> 127 == 1);
        }
    }

    /// The continued-fraction denominators of `num / den`: `q_0 = 1`,
    /// `q_n = a_n · q_(n−1) + q_(n−2)`. The integer part `a_0` takes no part.
    fn convergent_denominators(num: BigUint, den: BigUint, limit: &BigUint) -> Vec<BigUint> {
        let (mut num, mut den) = (den.clone(), num % den);
        let (mut previous, mut current) = (BigUint::from(0u32), BigUint::from(1u32));
        let mut out = vec![current.clone()];
        while den != BigUint::from(0u32) && &current <= limit {
            let a = &num / &den;
            (num, den) = (den.clone(), &num % &den);
            (previous, current) = (current.clone(), a * &current + previous);
            out.push(current.clone());
        }
        out
    }

    /// Coefficients whose product with a table entry `t` (`width` bits) ends,
    /// below the cut, within `c` of a carry: the values that tier must hand on.
    /// Random data reaches them about once in `2^(width − 129)`, so they are
    /// constructed: `c · t mod 2^K` is near `2^K` exactly when `c · (t / 2^K)` is
    /// just below an integer, which the denominators of the continued fraction
    /// of `t / 2^K` (and their multiples up to `multiples`, and the
    /// semiconvergents between them) give. Each case says whether half the
    /// bound would have accepted it, and whether the window `P` alone shows is
    /// the wrong one.
    fn must_defer(t: &BigUint, width: u32, e: i16, multiples: u32) -> Vec<(u128, bool, bool)> {
        let limit = BigUint::from(MAX_COEFFICIENT);
        let offset = width - 129;
        let mut out = Vec::new();
        // The cut sits `width − 129` or `width − 128` bits above a coefficient of
        // `b` bits, so only denominators of about that size can come close.
        for k in (width - 128)..=(width - 15) {
            let qs = convergent_denominators(t.clone(), BigUint::from(1u32) << k, &limit);
            for pair in qs.windows(2) {
                for j in 1..=multiples {
                    for c in [&pair[1] * j, &pair[1] * j + &pair[0]] {
                        let b = c.bits() as u32;
                        if c > limit || k + 2 < b + offset || k > b + offset + 3 {
                            continue;
                        }
                        let p = &c * t;
                        let cut = p.bits() as u32 - 128;
                        let distance =
                            (BigUint::from(1u32) << cut) - (&p % (BigUint::from(1u32) << cut));
                        if distance >= c {
                            continue;
                        }
                        let c = to_u128(&c);
                        let half = c >= 2 && distance >= BigUint::from(c / 2);
                        let wrong = to_u128(&(p >> cut)) != exact_body(c, e).window;
                        out.push((c, half, wrong));
                    }
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// The exponents the always-on tests construct values at: both ends, where
    /// the exact path is slowest, and the edges of where tables are exact.
    const CRAFTED_AT: [i16; 15] =
        [-6176, -3000, -358, -100, -28, -27, -10, -2, -1, 83, 100, 200, 308, 3000, 6111];

    #[test]
    fn values_crafted_against_the_first_tier_are_answered_by_the_second() {
        // Every one of these sits within the first tier's error of a carry, so
        // the first tier must defer; the second answers it with the exact
        // window, and none reaches the exact path. (A value whose expansion
        // ends is sent there by design and is not counted.)
        let (mut total, mut half, mut wrong, mut ending) = (0, 0, 0, 0);
        for e in CRAFTED_AT {
            let cases = must_defer(&big(&narrow(e).mantissa), 192, e, 6);
            assert!(!cases.is_empty(), "no deferral constructed at e = {e}");
            let before = fell_back();
            for &(c, needs_half, naive_wrong) in &cases {
                assert_eq!(fast_window(c, e), None, "{c}E{e} is within the first tier's error");
                let reference = exact_body(c, e);
                if e < 0 && !inexact(c, e) {
                    ending += 1;
                } else {
                    assert_eq!(
                        wide_window(c, e),
                        Some((reference.exp, reference.window)),
                        "{c}E{e}: the second tier answers"
                    );
                }
                assert_eq!(decimal(c, e), reference, "{c}E{e}");
                half += usize::from(needs_half);
                wrong += usize::from(naive_wrong);
            }
            assert_eq!(fell_back(), before, "a crafted value reached the exact path at e = {e}");
            total += cases.len();
        }
        // The set reaches both reasons to defer: a window `P` shows wrongly (the
        // no-deferral mutant), and a distance at least half the bound (the
        // halved-bound mutant).
        assert!(
            wrong > 10 && half > 10 && ending < total / 10,
            "{total} cases, {wrong} naive-wrong, {half} beyond half, {ending} ending"
        );
    }

    #[test]
    fn a_value_whose_expansion_ends_defers_in_both_tiers() {
        // `c = c' · 5^q`, `e = −q`: the window is followed by zeros, and each
        // tier's truncated reciprocal sits just below that carry, by
        // `c' · (2^S mod 5^q)`, under `c`. Both must defer (which is why
        // `decimal` does not ask them); about half sit beyond half the bound,
        // which a halved bound would accept with a window one too small.
        let (mut cases, mut beyond_half) = (0, 0);
        for q in 1u32..=48 {
            let five = 5u128.pow(q);
            for multiple in [1u128, 2, 3, 7, 1 << 20, 3 << 40, 12_345_678_901] {
                let Some(c) = five.checked_mul(multiple).filter(|&c| c <= MAX_COEFFICIENT) else {
                    continue;
                };
                let e = -(q as i16);
                assert!(!inexact(c, e), "{c}E{e} ends");
                assert_eq!(fast_window(c, e), None, "{c}E{e}: first tier");
                assert_eq!(wide_window(c, e), None, "{c}E{e}: second tier");
                assert_eq!(decimal(c, e), exact_body(c, e), "{c}E{e}");
                let p = BigUint::from(c) * big(&wide(e).mantissa);
                let cut = p.bits() as u32 - 128;
                let distance = (BigUint::from(1u32) << cut) - (&p % (BigUint::from(1u32) << cut));
                assert!(distance < BigUint::from(c), "{c}E{e} is within the bound");
                beyond_half += usize::from(distance * 2u32 >= BigUint::from(c));
                cases += 1;
            }
        }
        assert!(beyond_half > 20 && beyond_half < cases, "{beyond_half} of {cases} beyond half");
    }

    /// Every class of coefficient that one tier entry `t` (`width` bits) hands
    /// on, at every cut a coefficient up to `10^34 − 1` can produce:
    /// `(cut, convergent denominator, how many coefficients)`.
    ///
    /// A coefficient `c` defers when `c · t = p · 2^K − d` with `0 < d < c`, so
    /// `|t / 2^K − p / c| < 2^-K`. When `2^K >= 2 · (10^34)^2` (every cut of the
    /// second tier: `K >= 256`), Legendre's theorem makes `p / c` in lowest
    /// terms a convergent of `t / 2^K`, so `c` is a multiple `g · q` of a
    /// convergent denominator `q`, and `g · q` defers exactly when `q` does
    /// (`d` scales with `g`). The enumeration is then **complete**. For the
    /// first tier (`K < 227`) it is a lower bound.
    fn deferral_classes(t: &BigUint, width: u32) -> Vec<(u32, BigUint, u128)> {
        let one = || BigUint::from(1u32);
        let limit = BigUint::from(MAX_COEFFICIENT);
        let mut out = Vec::new();
        for k in (width - 128)..=(width - 15) {
            // The coefficients whose product has its cut at `k`.
            let low = ((one() << (k + 127)) + t - 1u32) / t;
            let high = ((one() << (k + 128)) - 1u32) / t;
            let high = if high > limit { limit.clone() } else { high };
            if low > high {
                continue;
            }
            let modulus = one() << k;
            for q in convergent_denominators(t.clone(), modulus.clone(), &high) {
                if q > high {
                    break;
                }
                let r = (&q * t) % &modulus;
                if r == BigUint::from(0u32) || &modulus - &r >= q {
                    continue;
                }
                let (g_low, g_high) = ((&low + &q - 1u32) / &q, &high / &q);
                if g_low <= g_high {
                    out.push((k, q, to_u128(&(g_high - g_low + 1u32))));
                }
            }
        }
        out
    }

    /// **The second tier hands on no coefficient but those whose expansion
    /// ends**, at any exponent: proved over every entry of the table, with the
    /// first tier's count beside it. Run with
    /// `cargo test -p kimmy-core --release --lib keyenc::number::tests::no_coefficient_defers_past_the_second_tier -- --ignored --nocapture`;
    /// with `KIMMY_DEFERRAL_REPORT=<file>` it writes one CSV row per exponent.
    #[test]
    #[ignore]
    fn no_coefficient_defers_past_the_second_tier() {
        let exponents: Vec<i16> = (MIN_EXPONENT..=MAX_EXPONENT).collect();
        let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
        let rows: Vec<String> = std::thread::scope(|scope| {
            let handles: Vec<_> = exponents
                .chunks(exponents.len().div_ceil(threads))
                .map(|chunk| {
                    scope.spawn(move || {
                        chunk
                            .iter()
                            .map(|&e| {
                                let first = deferral_classes(&big(&narrow(e).mantissa), 192);
                                let second = deferral_classes(&big(&wide(e).mantissa), 384);
                                let ending = |q: &BigUint| {
                                    (-48..0).contains(&e)
                                        && q % BigUint::from(5u32).pow(u32::from(e.unsigned_abs()))
                                            == BigUint::from(0u32)
                                };
                                for (k, q, n) in &second {
                                    assert!(
                                        ending(q),
                                        "e = {e}, cut {k}: {n} multiples of {q} defer past the \
                                         second tier and their expansion does not end"
                                    );
                                }
                                let count = |classes: &[(u32, BigUint, u128)]| {
                                    classes.iter().map(|c| c.2).sum::<u128>()
                                };
                                let not_ending: Vec<_> =
                                    first.iter().filter(|c| !ending(&c.1)).cloned().collect();
                                format!(
                                    "{e},{},{},{},{},{}",
                                    first.len(),
                                    count(&first),
                                    count(&not_ending),
                                    second.len(),
                                    count(&second)
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles.into_iter().flat_map(|h| h.join().unwrap()).collect()
        });
        let field = |row: &str, i: usize| row.split(',').nth(i).unwrap().parse::<u128>().unwrap();
        let with_first = rows.iter().filter(|r| field(r, 3) > 0).count();
        let with_second = rows.iter().filter(|r| field(r, 5) > 0).count();
        let first_total: u128 = rows.iter().map(|r| field(r, 3)).sum();
        let first_least = rows.iter().map(|r| field(r, 3)).filter(|&n| n > 0).min().unwrap_or(0);
        let second_total: u128 = rows.iter().map(|r| field(r, 5)).sum();
        println!(
            "first tier: {with_first} of {} exponents hand on coefficients whose expansion does \
             not end, at least {first_total:.3e} in all (at least {first_least} at each of them); \
             second tier: none; it hands on {second_total:.3e} coefficients at {with_second} \
             exponents, every one a value whose expansion ends",
            rows.len(),
        );
        // The construction of `must_defer` itself, with more multiples, at the
        // exponents the always-on tests use: what it builds against the first
        // tier, and what it builds against the second.
        for e in CRAFTED_AT {
            let first = must_defer(&big(&narrow(e).mantissa), 192, e, 49);
            let second = must_defer(&big(&wide(e).mantissa), 384, e, 49);
            let ending = second.iter().filter(|c| e < 0 && !inexact(c.0, e)).count();
            assert_eq!(ending, second.len(), "e = {e}: only ending values defer past the second");
            println!(
                "must_defer at e = {e}: {} first tier, {} second tier (all ending)",
                first.len(),
                second.len()
            );
        }
        if let Ok(path) = std::env::var("KIMMY_DEFERRAL_REPORT") {
            let header = "exponent,first_tier_classes,first_tier_coefficients_at_least,\
                          first_tier_not_ending_at_least,second_tier_classes,\
                          second_tier_coefficients_all_ending\n";
            std::fs::write(&path, header.to_string() + &rows.join("\n") + "\n").unwrap();
            println!("wrote {path}");
        }
    }

    /// `n` distinct coefficients at `e` chosen against the first tier, as a
    /// writer could choose them: each lands within that tier's error of a
    /// carry, and none is a value whose expansion ends.
    fn crafted(e: i16, n: usize) -> Vec<u128> {
        let mut multiples = 6;
        loop {
            let found: Vec<u128> = must_defer(&big(&narrow(e).mantissa), 192, e, multiples)
                .into_iter()
                .map(|c| c.0)
                .filter(|&c| !(e < 0 && !inexact(c, e)))
                .collect();
            if found.len() >= n {
                return found[..n].to_vec();
            }
            assert!(multiples < 1000, "only {} crafted at e = {e}", found.len());
            multiples *= 4;
        }
    }

    /// The most expensive document an index keys: an array of 999 decimals
    /// crafted at both extreme exponents (`kimmy-storage` keys each element
    /// and the whole array, and refuses more than 1,000 keys, so 999 is the
    /// longest array it keys rather than files unkeyed).
    fn crafted_document(chosen: bool) -> bson::Document {
        let mut rng = SplitMix(9);
        let (low, high) = if chosen {
            (crafted(MIN_EXPONENT, 500), crafted(MAX_EXPONENT, 499))
        } else {
            (
                (0..500).map(|_| rng.coefficient()).collect(),
                (0..499).map(|_| rng.coefficient()).collect(),
            )
        };
        let values = low
            .into_iter()
            .map(|c| (c, MIN_EXPONENT))
            .chain(high.into_iter().map(|c| (c, MAX_EXPONENT)))
            .map(|(c, e)| bson::Bson::Decimal128(crate::decimal::pack(false, c, e)))
            .collect();
        bson::doc! { "a": bson::Bson::Array(values) }
    }

    /// What one index's keys cost for `doc`: every element of `a` and the
    /// whole array, as `kimmy-storage`'s `document_keys` derives them.
    fn key_for_one_index(doc: &bson::Document) -> usize {
        let a = doc.get("a").unwrap();
        let bson::Bson::Array(items) = a else { unreachable!() };
        let mut bytes = 0;
        for value in items.iter().chain([a]) {
            bytes +=
                crate::keyenc::encode_compound_ordered(&[(value.clone(), false)]).unwrap().len();
        }
        bytes
    }

    /// The write costs of design §4.3, encode only, per index: one document,
    /// a 2 MiB `insert_many` of such documents, and a replicated run of 1,024
    /// of them. Returns `(one document, insert_many, replicated run, documents
    /// in 2 MiB)`.
    fn encode_costs(
        doc: &bson::Document,
    ) -> (std::time::Duration, std::time::Duration, std::time::Duration, usize) {
        use std::time::Instant;
        // A client sends JSON, so the documents in a 2 MiB body are counted in
        // its relaxed extended JSON (a decimal is `{"$numberDecimal": "…"}`).
        let json = bson::Bson::Document(doc.clone()).into_relaxed_extjson().to_string();
        let per_body = (2 << 20) / json.len();
        let time = |documents: usize| {
            let started = Instant::now();
            for _ in 0..documents {
                std::hint::black_box(key_for_one_index(std::hint::black_box(doc)));
            }
            started.elapsed()
        };
        let one = (0..5).map(|_| time(1)).min().unwrap();
        (one, time(per_body), time(1024), per_body)
    }

    #[test]
    fn crafted_extreme_decimals_cost_no_more_to_key_than_random_ones() {
        // Design §4.3's ceilings, on the encoder's part of the cost, with values
        // chosen against the first tier at both extreme exponents. The route is
        // the meaningful assertion: none reaches the exact path (80 µs each at
        // `e = −6176`, which made one such document cost about 160 ms per index
        // before the second tier). The times are bounded generously, so a
        // debug build under load passes, yet an exact path taken a thousand
        // times a document would not.
        let crafted = crafted_document(true);
        let before = fell_back();
        key_for_one_index(&crafted);
        assert_eq!(fell_back(), before, "a crafted value reached the exact path");
        let (one, insert_many, _, per_body) = encode_costs(&crafted);
        let slack = if cfg!(debug_assertions) { 40 } else { 1 };
        let ceiling = |ms: u64| std::time::Duration::from_millis(ms * slack);
        assert!(one <= ceiling(5), "one document: {one:?} per index");
        assert!(insert_many <= ceiling(500), "{per_body} documents: {insert_many:?} per index");
        assert!(per_body >= 16, "a 2 MiB body holds {per_body} such documents");
    }

    /// The same, in a release build, against the design's ceilings exactly,
    /// beside random values at the same exponents, printed. Run with
    /// `cargo test -p kimmy-core --release --lib keyenc::number::tests::encode_cost_ceilings -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn encode_cost_ceilings() {
        for (label, chosen) in [("random", false), ("crafted", true)] {
            let doc = crafted_document(chosen);
            let before = fell_back();
            let (one, insert_many, replicated, per_body) = encode_costs(&doc);
            println!(
                "{label}: one document of 999 extreme decimals {one:?} per index (ceiling 5 ms); \
                 a 2 MiB insert_many ({per_body} documents) {insert_many:?} per index (ceiling \
                 0.5 s); a replicated run of 1,024 documents {replicated:?} per index (ceiling \
                 5 s); exact path reached {} times",
                fell_back() - before
            );
            assert!(one.as_secs_f64() <= 0.005);
            assert!(insert_many.as_secs_f64() <= 0.5);
            assert!(replicated.as_secs_f64() <= 5.0);
        }
    }

    #[test]
    fn random_values_almost_never_defer() {
        // The fallback is reached by about 2^-63 of random data. Over a corpus of
        // extreme exponents, where the tables are least exact, it must stay far
        // below the 0.1% the design allows.
        let mut rng = SplitMix(7);
        let n = 20_000;
        let deferred = (0..n)
            .filter(|i| {
                let e = if i % 2 == 0 {
                    MIN_EXPONENT + rng.below(200) as i16
                } else {
                    MAX_EXPONENT - rng.below(200) as i16
                };
                fast_window(rng.coefficient(), e).is_none()
            })
            .count();
        assert!(deferred * 1000 < n, "{deferred} of {n} deferred");
    }

    /// The closed-form `END` and the fast window against the exact path over
    /// every exponent of the range, a dozen coefficients each, and the boundary
    /// set: about 150,000 values. Run with
    /// `cargo test -p kimmy-core --release --lib keyenc::number::tests::the_large_corpus -- --ignored`.
    #[test]
    #[ignore]
    fn the_large_corpus() {
        let mut rng = SplitMix(5);
        let (mut values, mut exact, mut deferred) = (0usize, 0usize, 0usize);
        let mut check = |c: u128, e: i16| {
            let reference = exact_body(c, e);
            assert_eq!(inexact(c, e), reference.inexact, "{c}E{e}");
            match fast_window(c, e) {
                Some(window) => assert_eq!(window, (reference.exp, reference.window), "{c}E{e}"),
                None => deferred += 1,
            }
            assert_eq!(decimal(c, e), reference, "{c}E{e}");
            values += 1;
            exact += usize::from(!reference.inexact);
        };
        for e in MIN_EXPONENT..=MAX_EXPONENT {
            for _ in 0..10 {
                check(rng.coefficient(), e);
            }
            check(1, e);
            check(MAX_COEFFICIENT, e);
        }
        for (c, e) in end_boundary_set() {
            check(c, e);
        }
        println!("{values} values, {exact} exact, {deferred} deferred");
    }

    /// Release-mode timings and the fallback rate, printed. Run with
    /// `cargo test -p kimmy-core --release --lib keyenc::number::tests::timings -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn timings() {
        use std::hint::black_box;
        use std::time::Instant;

        let build = |label: &str, f: &dyn Fn() -> usize| {
            let started = Instant::now();
            let bytes = f();
            println!("table build, {label}: {:?}, {} KB", started.elapsed(), bytes / 1000);
        };
        let size = |n: usize, entry: usize| n * entry;
        build("first tier 5^0..=5^6111", &|| {
            size(black_box(positive_powers::<NARROW>()).len(), size_of::<Pow5<NARROW>>())
        });
        build("first tier 5^-1..=5^-6176", &|| {
            size(black_box(negative_powers::<NARROW>()).len(), size_of::<Pow5<NARROW>>())
        });
        build("second tier 5^0..=5^6111", &|| {
            size(black_box(positive_powers::<WIDE>()).len(), size_of::<Pow5<WIDE>>())
        });
        build("second tier 5^-1..=5^-6176", &|| {
            size(black_box(negative_powers::<WIDE>()).len(), size_of::<Pow5<WIDE>>())
        });
        // Warm both tiers, so no timing below includes a build.
        let _ = (narrow(1), narrow(-1), wide(1), wide(-1));

        let mut rng = SplitMix(3);
        let mut time = |label: &str,
                        n: usize,
                        values: &dyn Fn(&mut SplitMix) -> (u128, i16),
                        f: &dyn Fn(u128, i16) -> Body| {
            let inputs: Vec<_> = (0..n).map(|_| values(&mut rng)).collect();
            let started = Instant::now();
            for &(c, e) in &inputs {
                black_box(f(black_box(c), black_box(e)));
            }
            let each = started.elapsed() / n as u32;
            println!("{label}: {each:?} per value over {n}");
        };
        let small = |r: &mut SplitMix| (r.coefficient(), r.below(41) as i16 - 20);
        let lowest = |r: &mut SplitMix| (r.coefficient(), MIN_EXPONENT);
        let highest = |r: &mut SplitMix| (r.coefficient(), MAX_EXPONENT);
        time("encode |e| <= 20", 1_000_000, &small, &decimal);
        time("encode e = -6176", 1_000_000, &lowest, &decimal);
        time("encode e = +6111", 1_000_000, &highest, &decimal);
        time("exact path |e| <= 20", 100_000, &small, &exact_body);
        time("exact path e = -6176", 2_000, &lowest, &exact_body);
        time("exact path e = +6111", 2_000, &highest, &exact_body);
        let ending = |r: &mut SplitMix| {
            let q = 1 + r.below(48) as u32;
            let most = MAX_COEFFICIENT / 5u128.pow(q);
            ((1 + r.below(most)) * 5u128.pow(q), -(q as i16))
        };
        time("encode, an expansion that ends (e < 0)", 1_000_000, &ending, &decimal);
        // Values crafted against the first tier, as a writer could choose them.
        for e in [MIN_EXPONENT, -3000, 3000, MAX_EXPONENT] {
            let crafted: Vec<u128> = must_defer(&big(&narrow(e).mantissa), 192, e, 49)
                .into_iter()
                .map(|c| c.0)
                .filter(|&c| !(e < 0 && !inexact(c, e)))
                .collect();
            let n = crafted.len();
            let started = Instant::now();
            for _ in 0..100 {
                for &c in &crafted {
                    black_box(decimal(black_box(c), black_box(e)));
                }
            }
            let each = started.elapsed() / (100 * n as u32);
            let started = Instant::now();
            for &c in crafted.iter().take(200) {
                black_box(exact_body(black_box(c), black_box(e)));
            }
            let exact = started.elapsed() / n.min(200) as u32;
            println!(
                "encode, crafted against the first tier, e = {e}: {each:?} per value over {n} \
                 distinct (the exact path they deferred to before: {exact:?})"
            );
        }

        // The fallback rate: over extreme exponents alone, and over the whole
        // range, where a decimal whose expansion ends (`5^q` divides `c`, `e < 0`)
        // always defers: its window is followed by zeros only, and the truncated
        // reciprocal puts `P` just below that carry.
        let n = 2_000_000;
        let (mut extreme, mut whole, mut ending) = (0, 0, 0);
        for i in 0..n {
            let c = rng.coefficient();
            let e = if i % 2 == 0 {
                MIN_EXPONENT + rng.below(400) as i16
            } else {
                MAX_EXPONENT - rng.below(400) as i16
            };
            extreme += usize::from(fast_window(c, e).is_none());
            let e = (rng.below(12_288) as i32 + i32::from(MIN_EXPONENT)) as i16;
            if fast_window(c, e).is_none() {
                whole += 1;
                ending += usize::from(e < 0 && !inexact(c, e));
            }
        }
        println!(
            "fallback: {extreme} of {n} at extreme exponents; {whole} of {n} over the whole range, {ending} of them values whose expansion ends"
        );
        assert!(extreme * 1000 < n && whole * 1000 < n);
    }
}
