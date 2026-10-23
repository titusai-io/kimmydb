//! A small unsigned big integer: exactly the operations the exact number
//! comparator and the number key encoder use, and nothing else.
//!
//! The comparator brings a `Decimal128` and a binary number to integers and
//! compares them (`c · 5^p · 2^(p+t)` against `m · 5^q · 2^(q+s)`), which takes
//! a multiplication by a word, a shift and a comparison. The encoder reads the
//! top bits of `c · 5^e`, or of `c · 2^k / 5^q`, which adds a division by a word
//! and a read of the top bits. The decimal arithmetic adds the three it needs:
//! a multiply-and-add by a word (building a value from base-10^19 limbs), a
//! read of one bit, and a division by a `u128` whose quotient fits one. That is
//! the whole surface: no general multiplication, no general division, nothing
//! that a crate would add but nobody here calls. It is not exported, and every operation is checked
//! against `num-bigint` (a dev-dependency) in the tests below.

use std::cmp::Ordering;

/// `5^27`, the largest power of five a `u64` holds. A multiplication or a
/// division by a power of five goes 27 powers at a time.
pub(crate) const POW5_27: u64 = 7_450_580_596_923_828_125;

/// An unsigned integer of any size.
///
/// Little-endian 64-bit limbs with no zero limb at the top, so zero has no
/// limbs and equal values have equal limbs, which is what lets [`Ord`] compare
/// lengths first.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct Big {
    limbs: Vec<u64>,
}

impl Big {
    pub(crate) fn from_u128(value: u128) -> Self {
        let mut big = Big { limbs: vec![value as u64, (value >> 64) as u64] };
        big.trim();
        big
    }

    fn trim(&mut self) {
        while self.limbs.last() == Some(&0) {
            self.limbs.pop();
        }
    }

    /// `self *= m`.
    pub(crate) fn mul_small(&mut self, m: u64) {
        let mut carry = 0u64;
        for limb in &mut self.limbs {
            // At most (2^64 - 1)^2 + (2^64 - 1) < 2^128: it cannot overflow.
            let product = u128::from(*limb) * u128::from(m) + u128::from(carry);
            *limb = product as u64;
            carry = (product >> 64) as u64;
        }
        if carry != 0 {
            self.limbs.push(carry);
        }
        // Only a multiplication by zero leaves a zero top limb.
        self.trim();
    }

    /// `self <<= bits`.
    pub(crate) fn shl(&mut self, bits: u32) {
        if self.limbs.is_empty() {
            return;
        }
        let (whole, part) = ((bits / 64) as usize, bits % 64);
        if part != 0 {
            let mut carry = 0u64;
            for limb in &mut self.limbs {
                let out = *limb >> (64 - part);
                *limb = (*limb << part) | carry;
                carry = out;
            }
            if carry != 0 {
                self.limbs.push(carry);
            }
        }
        if whole != 0 {
            self.limbs.splice(0..0, std::iter::repeat_n(0, whole));
        }
    }

    /// `self /= d`, rounding down, and the remainder.
    ///
    /// # Panics
    /// If `d` is zero.
    pub(crate) fn div_small(&mut self, d: u64) -> u64 {
        let d = u128::from(d);
        let mut remainder = 0u128;
        for limb in self.limbs.iter_mut().rev() {
            let current = (remainder << 64) | u128::from(*limb);
            *limb = (current / d) as u64;
            remainder = current % d;
        }
        self.trim();
        remainder as u64
    }

    /// `self = self · m + a`.
    pub(crate) fn mul_add_small(&mut self, m: u64, a: u64) {
        let mut carry = u128::from(a);
        for limb in &mut self.limbs {
            let product = u128::from(*limb) * u128::from(m) + carry;
            *limb = product as u64;
            carry = product >> 64;
        }
        if carry != 0 {
            self.limbs.push(carry as u64);
        }
        self.trim();
    }

    /// Bit `i` (0 is the least significant); a bit past the top is zero.
    pub(crate) fn bit(&self, i: u32) -> bool {
        self.limbs.get((i / 64) as usize).is_some_and(|limb| (limb >> (i % 64)) & 1 == 1)
    }

    /// `⌊self / d⌋` and the remainder, for a quotient that fits a `u128`.
    ///
    /// Binary long division: the remainder stays below `d`, so it takes one
    /// bit at a time without overflowing a `u128` when `d < 2^127`. A value of
    /// at most 128 bits goes straight to the hardware's division.
    ///
    /// # Panics
    /// If `d` is zero or at least `2^127`, or if the quotient needs more than
    /// 128 bits.
    pub(crate) fn div_rem_u128(&self, d: u128) -> (u128, u128) {
        assert!(d != 0 && d < 1 << 127, "div_rem_u128 by {d}");
        let len = self.bit_len();
        if len <= 128 {
            let n = self.limbs.iter().rev().fold(0u128, |acc, &l| (acc << 64) | u128::from(l));
            return (n / d, n % d);
        }
        let (mut quotient, mut remainder) = (0u128, 0u128);
        for i in (0..len).rev() {
            assert!(quotient >> 127 == 0, "the quotient does not fit 128 bits");
            remainder = (remainder << 1) | u128::from(self.bit(i));
            quotient <<= 1;
            if remainder >= d {
                remainder -= d;
                quotient |= 1;
            }
        }
        (quotient, remainder)
    }

    /// The number of significant bits; zero has none.
    pub(crate) fn bit_len(&self) -> u32 {
        match self.limbs.last() {
            None => 0,
            Some(top) => 64 * (self.limbs.len() as u32 - 1) + (64 - top.leading_zeros()),
        }
    }

    /// The top `n` bits (`1 <= n <= 64 · L`) as an `n`-bit integer whose top bit
    /// is set, in `L` little-endian limbs, and whether every bit below them is
    /// zero.
    ///
    /// The integer is `⌊self · 2^(n − len)⌋`: the leading `n` bits, truncated
    /// when the value is longer and shifted up (exactly) when it is shorter.
    ///
    /// # Panics
    /// If `self` is zero (it has no top bit) or `n` is out of range.
    pub(crate) fn top_bits<const L: usize>(&self, n: u32) -> ([u64; L], bool) {
        assert!(
            (1..=64 * L as u32).contains(&n) && !self.limbs.is_empty(),
            "top_bits({n}) of {self:?}"
        );
        let len = self.bit_len();
        let mut out = [0u64; L];
        if len <= n {
            let mut shifted = self.clone();
            shifted.shl(n - len);
            out[..shifted.limbs.len()].copy_from_slice(&shifted.limbs);
            return (out, true);
        }
        let cut = len - n;
        let (whole, part) = ((cut / 64) as usize, cut % 64);
        let limb = |i: usize| self.limbs.get(i).copied().unwrap_or(0);
        for (i, slot) in out.iter_mut().enumerate() {
            *slot = if part == 0 {
                limb(whole + i)
            } else {
                (limb(whole + i) >> part) | (limb(whole + i + 1) << (64 - part))
            };
        }
        let below_zero = self.limbs[..whole].iter().all(|&l| l == 0)
            && self.limbs[whole] & ((1u64 << part) - 1) == 0;
        (out, below_zero)
    }

    /// `self *= 5^k`.
    pub(crate) fn mul_pow5(&mut self, mut k: u32) {
        while k >= 27 {
            self.mul_small(POW5_27);
            k -= 27;
        }
        self.mul_small(5u64.pow(k));
    }

    /// `self = ⌊self / 5^k⌋`, and whether `5^k` divided it exactly.
    ///
    /// The division goes 27 powers at a time, and that is exact because a floor
    /// of a floor is the floor (`⌊⌊y/a⌋/b⌋ = ⌊y/(ab)⌋`); `5^k` divides `y`
    /// exactly when every step's remainder is zero.
    pub(crate) fn div_pow5(&mut self, mut k: u32) -> bool {
        let mut divides = true;
        while k >= 27 {
            divides &= self.div_small(POW5_27) == 0;
            k -= 27;
        }
        divides &= self.div_small(5u64.pow(k)) == 0;
        divides
    }
}

impl Ord for Big {
    fn cmp(&self, other: &Self) -> Ordering {
        // No zero top limb on either side, so the longer one is the larger.
        self.limbs
            .len()
            .cmp(&other.limbs.len())
            .then_with(|| self.limbs.iter().rev().cmp(other.limbs.iter().rev()))
    }
}

impl PartialOrd for Big {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use num_bigint::BigUint;
    use proptest::prelude::*;

    use super::*;

    fn oracle(big: &Big) -> BigUint {
        BigUint::new(big.limbs.iter().flat_map(|&l| [l as u32, (l >> 32) as u32]).collect())
    }

    fn from_limbs(limbs: Vec<u64>) -> Big {
        let mut big = Big { limbs };
        big.trim();
        big
    }

    /// Operands of zero to seven limbs, biased toward the shapes that break limb
    /// arithmetic: all-ones limbs (every carry propagates), a lone top bit, and
    /// zero limbs in the middle.
    fn any_big() -> impl Strategy<Value = Big> {
        let limb =
            prop_oneof![any::<u64>(), Just(u64::MAX), Just(0u64), Just(1u64), Just(1u64 << 63),];
        prop::collection::vec(limb, 0..8).prop_map(from_limbs)
    }

    fn any_word() -> impl Strategy<Value = u64> {
        prop_oneof![any::<u64>(), Just(u64::MAX), Just(POW5_27), Just(5u64), Just(1u64), 2..1000u64]
    }

    proptest! {
        #[test]
        fn mul_add_small_agrees_with_num_bigint(a in any_big(), m in any_word(), x in any::<u64>()) {
            let mut got = a.clone();
            got.mul_add_small(m, x);
            prop_assert_eq!(oracle(&got), oracle(&a) * m + x);
            prop_assert_eq!(got.limbs.last() != Some(&0), true);
        }

        #[test]
        fn bit_reads_the_bits_of_the_value(a in any_big(), i in 0u32..600) {
            prop_assert_eq!(a.bit(i), oracle(&a).bit(u64::from(i)));
        }

        #[test]
        fn div_rem_u128_agrees_with_num_bigint(
            a in prop::collection::vec(any::<u64>(), 0..4).prop_map(from_limbs),
            d in prop_oneof![1u128..1000, any::<u128>().prop_map(|d| (d >> 15).max(1)), Just((1u128 << 127) - 1)],
        ) {
            // Only a quotient that fits 128 bits: it has at most
            // `len(a) - len(d) + 1` of them.
            let d_bits = 128 - d.leading_zeros();
            prop_assume!(a.bit_len() <= 128 || a.bit_len() <= d_bits + 127);
            let (q, r) = a.div_rem_u128(d);
            let n = oracle(&a);
            prop_assert_eq!(BigUint::from(q), &n / d);
            prop_assert_eq!(BigUint::from(r), &n % d);
        }
    }

    #[test]
    fn the_power_of_five_constant_is_the_largest_that_fits() {
        assert_eq!(u128::from(POW5_27), 5u128.pow(27));
        assert!(5u128.pow(28) > u128::from(u64::MAX));
    }

    #[test]
    fn zero_has_no_limbs_and_no_bits() {
        let zero = Big::from_u128(0);
        assert!(zero.limbs.is_empty());
        assert_eq!(zero.bit_len(), 0);
        let mut also_zero = Big::from_u128(12345);
        also_zero.mul_small(0);
        assert_eq!(also_zero, zero);
        let mut shifted = zero.clone();
        shifted.shl(300);
        assert_eq!(shifted, zero, "shifting zero must not grow zero limbs");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        #[test]
        fn from_u128_and_bit_len_agree_with_num_bigint(v in any::<u128>()) {
            let big = Big::from_u128(v);
            prop_assert_eq!(oracle(&big), BigUint::from(v));
            prop_assert_eq!(u64::from(big.bit_len()), BigUint::from(v).bits());
        }

        #[test]
        fn mul_small_agrees_with_num_bigint(a in any_big(), m in any_word()) {
            let expected = oracle(&a) * m;
            let mut big = a;
            big.mul_small(m);
            prop_assert_eq!(oracle(&big), expected.clone());
            prop_assert_eq!(u64::from(big.bit_len()), expected.bits());
        }

        #[test]
        fn shl_agrees_with_num_bigint(a in any_big(), bits in 0u32..400) {
            let expected = oracle(&a) << bits;
            let mut big = a;
            big.shl(bits);
            prop_assert_eq!(oracle(&big), expected);
        }

        #[test]
        fn div_small_agrees_with_num_bigint(a in any_big(), d in any_word().prop_filter("nonzero", |d| *d != 0)) {
            let (q, r) = (oracle(&a) / d, oracle(&a) % d);
            let mut big = a;
            let remainder = big.div_small(d);
            prop_assert_eq!(oracle(&big), q);
            prop_assert_eq!(BigUint::from(remainder), r);
        }

        #[test]
        fn cmp_agrees_with_num_bigint(a in any_big(), b in any_big()) {
            prop_assert_eq!(a.cmp(&b), oracle(&a).cmp(&oracle(&b)));
            // Random pairs rarely share a length; a value against itself and
            // against its successor do, and differ (or not) only in the limbs.
            prop_assert_eq!(a.cmp(&a.clone()), Ordering::Equal);
            let next = from_limbs({
                let mut limbs = a.limbs.clone();
                match limbs.iter().position(|&l| l != u64::MAX) {
                    Some(i) => {
                        limbs[i] += 1;
                        limbs[..i].iter_mut().for_each(|l| *l = 0);
                    }
                    None => {
                        limbs.iter_mut().for_each(|l| *l = 0);
                        limbs.push(1);
                    }
                }
                limbs
            });
            prop_assert_eq!(oracle(&next), oracle(&a) + 1u32);
            prop_assert_eq!(a.cmp(&next), Ordering::Less);
            prop_assert_eq!(next.cmp(&a), Ordering::Greater);
        }

        #[test]
        fn top_bits_agree_with_num_bigint(a in any_big(), n in 1u32..=384) {
            prop_assume!(!a.limbs.is_empty());
            let value = oracle(&a);
            let len = value.bits();
            let (expected, below_zero) = if len <= u64::from(n) {
                (value.clone() << (u64::from(n) - len), true)
            } else {
                let cut = len - u64::from(n);
                let top = value.clone() >> cut;
                (top.clone(), (top << cut) == value)
            };
            let (limbs, exact) = a.top_bits::<6>(n);
            prop_assert_eq!(oracle(&from_limbs(limbs.to_vec())), expected.clone());
            prop_assert_eq!(expected.bits(), u64::from(n), "the top bit is set");
            prop_assert_eq!(exact, below_zero);
        }

        #[test]
        fn powers_of_five_agree_with_num_bigint(a in any_big(), k in 0u32..400) {
            let five = BigUint::from(5u32).pow(k);
            let mut product = a.clone();
            product.mul_pow5(k);
            prop_assert_eq!(oracle(&product), oracle(&a) * &five);
            let mut quotient = a.clone();
            let divides = quotient.div_pow5(k);
            prop_assert_eq!(oracle(&quotient), oracle(&a) / &five);
            prop_assert_eq!(divides, (oracle(&a) % &five) == BigUint::from(0u32));
        }
    }
}
