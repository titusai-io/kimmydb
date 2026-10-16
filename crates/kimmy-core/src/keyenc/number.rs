//! The body of a finite number's key: its exponent, the first 128 bits of its
//! binary expansion, and whether any bit after those is nonzero.
//!
//! For `Int32`, `Int64` and `Double` all three come from the 64-bit normalised
//! mantissa: the window is the mantissa followed by 64 zero bits, and nothing
//! follows it. For a `Decimal128` `c · 10^e` the window is the head of an
//! expansion that can run to 14,000 bits (`10^-6176`), so it is taken on a fast
//! path with an exact path behind it, the way a float parser does, and the
//! exactness mark has a closed form that needs no long division (design §4.3).
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
    let (exp, window) = fast_window(c, e).unwrap_or_else(|| {
        let exact = exact_body(c, e);
        (exact.exp, exact.window)
    });
    Body { exp, window, inexact: inexact(c, e) }
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
    let (top, rest_zero) = n.top_bits(128);
    Body {
        exp,
        window: u128::from(top[0]) | (u128::from(top[1]) << 64),
        inexact: !(divides && rest_zero),
    }
}

// ---------------------------------------------------------------------------
// The window, fast
// ---------------------------------------------------------------------------

/// `5^e` to 192 bits: `5^e = (mantissa + δ) · 2^shift` with `0 <= δ < 1`, the
/// mantissa's top bit set. Truncated, never rounded, so the error is one-sided.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Pow5 {
    pub(crate) mantissa: [u64; 3],
    pub(crate) shift: i32,
}

impl Pow5 {
    fn of(value: &Big, scale: u32) -> Self {
        let (mantissa, _) = value.top_bits(192);
        Pow5 { mantissa, shift: value.bit_len() as i32 - 192 - scale as i32 }
    }
}

/// Every power `5^0 ..= 5^6111`, each exact to its first 192 bits. Built on
/// first use, by multiplying by five; a workload with no positive decimal
/// exponent never builds it. The two tables hold about 390 KB and take about a
/// millisecond and three to four milliseconds to build in a release build,
/// once, inside whatever first encodes such a decimal.
fn positive_powers() -> &'static [Pow5] {
    static TABLE: OnceLock<Vec<Pow5>> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut power = Big::from_u128(1);
        (0..=MAX_EXPONENT)
            .map(|_| {
                let entry = Pow5::of(&power, 0);
                power.mul_small(5);
                entry
            })
            .collect()
    })
}

/// `5^-1 ..= 5^-6176`, each the exact truncation of its first 192 bits.
///
/// Built by repeated division of one wide power of two by five:
/// `⌊⌊2^W / 5^q⌋ / 5⌋ = ⌊2^W / 5^(q+1)⌋`, so every step is still the exact
/// floor, and its top 192 bits are the exact truncation of `2^W / 5^q`.
/// `W` leaves at least 192 bits at `q = 6176` (`5^6176` is 14,341 bits long).
fn negative_powers() -> &'static [Pow5] {
    const WIDTH: u32 = 14_341 + 192 + 64;
    static TABLE: OnceLock<Vec<Pow5>> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut reciprocal = Big::from_u128(1);
        reciprocal.shl(WIDTH);
        (1..=u32::from(MIN_EXPONENT.unsigned_abs()))
            .map(|_| {
                reciprocal.div_small(5);
                assert!(reciprocal.bit_len() >= 192, "the table keeps 192 bits");
                Pow5::of(&reciprocal, WIDTH)
            })
            .collect()
    })
}

pub(crate) fn pow5(e: i16) -> &'static Pow5 {
    if e >= 0 {
        &positive_powers()[e as usize]
    } else {
        &negative_powers()[usize::from(e.unsigned_abs()) - 1]
    }
}

/// The window of `c · 10^e` from the table, or `None` when the table's error
/// could reach the window and only the exact path can say.
///
/// `c · 10^e = c · 5^e · 2^e = (P + c · δ) · 2^(shift + e)` with
/// `P = c · mantissa`, a 320-bit product, and `0 <= c · δ < c`. So the true
/// value, in units of `2^(shift + e)`, lies in `[P, P + c)`, and its top 128
/// bits are `P`'s whenever adding anything below `c` to `P` leaves them as
/// they are, which is a question about `P` and `c` alone: does `P + c − 1`
/// still have the same bits from the cut up? Below the cut sit at least 63
/// bits more than `c` has, so a random coefficient defers about once in
/// `2^63`; a value whose expansion has a long run of zeros or ones just below
/// the window defers, as it must.
///
/// **Every decimal whose expansion ends, at a negative exponent, is such a
/// value** (`1.5`, `0.25`, `19.50`: `5^q` divides `c`). Its window is followed
/// by zeros only, and the truncated reciprocal puts `P` just below that carry
/// (`P = c' · 2^W − c' · (2^W mod 5^q)` with `c = c' · 5^q`), so all of them take
/// the exact path, which at those exponents (`q <= 48`) costs tens of
/// nanoseconds, not microseconds.
pub(crate) fn fast_window(c: u128, e: i16) -> Option<(i32, u128)> {
    let power = pow5(e);
    let product = mul_128_192(c, &power.mantissa);
    let length = 320 - leading_zeros(&product);
    let cut = length - 128;
    let window = bits_from(&product, cut);
    let mut highest = product;
    add(&mut highest, c - 1);
    if bits_from(&highest, cut) != window {
        return None;
    }
    Some((length as i32 - 1 + power.shift + i32::from(e), window))
}

/// `c · t` for a 128-bit `c` and a 192-bit `t`, little-endian limbs.
fn mul_128_192(c: u128, t: &[u64; 3]) -> [u64; 5] {
    let mut out = [0u64; 5];
    for (i, a) in [c as u64, (c >> 64) as u64].into_iter().enumerate() {
        let mut carry = 0u128;
        for (j, &b) in t.iter().enumerate() {
            // At most (2^64 − 1)^2 + 2 · (2^64 − 1) = 2^128 − 1.
            let current = u128::from(a) * u128::from(b) + u128::from(out[i + j]) + carry;
            out[i + j] = current as u64;
            carry = current >> 64;
        }
        out[i + 3] = carry as u64;
    }
    out
}

fn leading_zeros(n: &[u64; 5]) -> u32 {
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
fn bits_from(n: &[u64; 5], cut: u32) -> u128 {
    let (whole, part) = ((cut / 64) as usize, cut % 64);
    let limb = |i: usize| n.get(i).copied().map_or(0, u128::from);
    let low = limb(whole) | (limb(whole + 1) << 64);
    if part == 0 { low } else { (low >> part) | (limb(whole + 2) << (128 - part)) }
}

/// `n += v`. The products here are below `2^305` and `v` below `2^113`, so it
/// cannot leave the five limbs.
fn add(n: &mut [u64; 5], v: u128) {
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

    fn mantissa(p: &Pow5) -> BigUint {
        BigUint::from_slice(
            &p.mantissa.iter().flat_map(|&l| [l as u32, (l >> 32) as u32]).collect::<Vec<_>>(),
        )
    }

    #[test]
    fn every_table_entry_is_the_exact_truncation_of_its_power() {
        // Against num-bigint, by the defining inequality rather than by the same
        // construction: `5^e = (mantissa + δ) · 2^shift`, `0 <= δ < 1`, and the
        // mantissa has exactly 192 bits. One flipped low bit in one entry fails.
        let two = |n: u32| BigUint::from(1u32) << n;
        let mut power = BigUint::from(1u32);
        for e in 0..=MAX_EXPONENT {
            let entry = pow5(e);
            let m = mantissa(entry);
            assert_eq!(m.bits(), 192, "5^{e}");
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
            let entry = pow5(-(q as i16));
            let m = mantissa(entry);
            assert_eq!(m.bits(), 192, "5^-{q}");
            // `5^-q = (m + δ) · 2^shift`  <=>  `m · 5^q <= 2^-shift < (m + 1) · 5^q`.
            let scale = two((-entry.shift) as u32);
            assert!(&m * &power <= scale && scale < (&m + 1u32) * &power, "5^-{q}");
        }
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

    /// Coefficients whose product with the table entry ends, below the cut,
    /// within `c` of a carry: the values the fast path must hand to the exact
    /// path. Random data reaches them about once in `2^63`, so they are
    /// constructed: `c · T mod 2^K` is near `2^K` exactly when `c · (T / 2^K)` is
    /// just below an integer, which the denominators of the continued fraction
    /// of `T / 2^K` (and their multiples and the semiconvergents between them)
    /// give.
    fn must_defer(e: i16) -> Vec<(u128, bool, bool)> {
        let t = mantissa(pow5(e));
        let limit = BigUint::from(MAX_COEFFICIENT);
        let to_u128 = |n: &BigUint| {
            let digits = n.to_u64_digits();
            u128::from(*digits.first().unwrap_or(&0))
                | (u128::from(*digits.get(1).unwrap_or(&0)) << 64)
        };
        let mut out = Vec::new();
        // The cut sits 63 or 64 bits above a coefficient of `b` bits, so only
        // the denominators of about that size can come within `c` of a carry.
        for k in 64u32..=177 {
            let qs = convergent_denominators(t.clone(), BigUint::from(1u32) << k, &limit);
            for pair in qs.windows(2) {
                for j in 1u32..=6 {
                    for c in [&pair[1] * j, &pair[1] * j + &pair[0]] {
                        let b = c.bits() as u32;
                        if c > limit || b + 66 < k || b + 61 > k {
                            continue;
                        }
                        let p = &c * &t;
                        let cut = p.bits() as u32 - 128;
                        let distance =
                            (BigUint::from(1u32) << cut) - (&p % (BigUint::from(1u32) << cut));
                        if distance >= c {
                            continue;
                        }
                        let c = to_u128(&c);
                        // Whether half the bound would have accepted it, and
                        // whether the window `P` alone shows is the wrong one.
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

    #[test]
    fn values_within_the_error_of_a_carry_defer_to_the_exact_path() {
        let (mut total, mut half, mut wrong) = (0, 0, 0);
        for e in [-6176i16, -3000, -358, -100, -28, -27, -10, -2, -1, 83, 100, 200, 308, 3000, 6111]
        {
            let cases = must_defer(e);
            assert!(!cases.is_empty(), "no deferral constructed at e = {e}");
            for &(c, needs_half, naive_wrong) in &cases {
                assert_eq!(
                    fast_window(c, e),
                    None,
                    "{c}E{e} sits within the error bound of a carry"
                );
                assert_eq!(decimal(c, e), exact_body(c, e), "{c}E{e}");
                half += usize::from(needs_half);
                wrong += usize::from(naive_wrong);
            }
            total += cases.len();
        }
        // The set reaches both reasons to defer: a window `P` shows wrongly (the
        // no-deferral mutant), and a distance at least half the bound (the
        // halved-bound mutant).
        assert!(wrong > 10 && half > 10, "{total} cases, {wrong} naive-wrong, {half} beyond half");
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

        let started = Instant::now();
        positive_powers();
        let positive = started.elapsed();
        let started = Instant::now();
        negative_powers();
        let negative = started.elapsed();
        println!("table build: 5^0..=5^6111 {positive:?}, 5^-1..=5^-6176 {negative:?}");

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
