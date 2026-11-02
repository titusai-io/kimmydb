//! The embedded `pi` of `trig.rs`: it is the digits of `pi`, long enough for the widest
//! operand, read once, and no call builds it.

use std::sync::atomic::Ordering;

use super::super::{LIMB_DIGITS, Mag, m_add, m_div_small, m_sub, pow10};
use super::{PI_DIGITS, PI_PARSES, PI_PLACES, Scale, pi_constant};
use crate::decimal::Raw;
use crate::decimal::arith::func;

/// `atan(1/n)` to `places` places, truncated: `Σ (-1)^k / ((2k + 1) n^(2k+1))`.
fn atan_inverse(n: u64, places: u32) -> Mag {
    let mut power = m_div_small(&pow10(places), n).0;
    let n2 = n * n;
    let (mut plus, mut minus) = (Mag::zero(), Mag::zero());
    let (mut k, mut add) = (1u64, true);
    while !power.is_zero() {
        let term = m_div_small(&power, k).0;
        if add {
            plus = m_add(&plus, &term);
        } else {
            minus = m_add(&minus, &term);
        }
        power = m_div_small(&power, n2).0;
        k += 2;
        add = !add;
    }
    m_sub(&plus, &minus)
}

/// `pi = 16 atan(1/5) - 4 atan(1/239)` (Machin) to `places` places, computed 19 places wider
/// and cut back with the half-up rounding of a limb.
fn machin(places: u32) -> Mag {
    let wide = places + LIMB_DIGITS;
    let mut a = atan_inverse(5, wide);
    a.mul_small(16);
    let mut b = atan_inverse(239, wide);
    b.mul_small(4);
    let pi = m_sub(&a, &b);
    let (q, tail) = pi.split_digits(LIMB_DIGITS);
    if tail.digit >= 5 { m_add(&q, &Mag::from_u64(1)) } else { q }
}

fn fin(coefficient: u128, exponent: i16) -> Raw {
    Raw::Finite { negative: false, coefficient, exponent }
}

#[test]
fn the_embedded_pi_is_the_machin_value_at_the_widest_width() {
    // Machin's formula, computed here, against the Chudnovsky digits in `pi.txt`, over the
    // whole width: every place but the last ten, which a rounding in either could move.
    let wide = machin(PI_PLACES);
    let embedded = pi_constant();
    assert_eq!(wide.digits(), embedded.digits());
    assert_eq!(wide.split_digits(10).0, embedded.split_digits(10).0);
}

#[test]
fn the_embedded_pi_covers_the_widest_scale_an_operand_reaches() {
    // `reaching(6144)`: an operand of adjusted exponent 6,144 (a coefficient of 34 digits and
    // the largest exponent), and 114 places beyond it.
    let widest = Scale::reaching(6144);
    assert_eq!(widest.limbs, 330);
    assert!(PI_PLACES >= widest.places(), "{PI_PLACES} places for {}", widest.places());
    assert_eq!(PI_DIGITS.trim_end().len(), PI_PLACES as usize + 1);
    // Cut to a scale, `pi` is the leading digits.
    assert_eq!(Scale::standard().pi().digits(), Scale::standard().places() + 1);
    assert_eq!(widest.pi().digits(), widest.places() + 1);
}

#[test]
fn many_calls_across_widths_read_pi_once_and_build_none() {
    // Operands of every width (the exponent decides the scale), through all four users of `pi`.
    for exponent in [0i16, 5, 19, 20, 40, 100, 300, 1000, 3000, 6000, 6111] {
        let x = fin(1_234_567_890_123_456_789_012_345_678_901_234, exponent);
        for call in [func::sin, func::cos, func::tan] {
            assert!(matches!(call(&x), Ok(Raw::Finite { .. })));
        }
        let y = fin(3, 0);
        assert!(matches!(func::atan2(&y, &fin(7, exponent.min(100) - 30)), Raw::Finite { .. }));
        assert!(matches!(func::atan2(&y, &fin(0, 0)), Raw::Finite { .. }));
        assert!(matches!(func::acos(&fin(5, -1)), Ok(Raw::Finite { .. })));
    }
    // The count is global to the process: whatever ran first read it, and it is read once.
    assert_eq!(PI_PARSES.load(Ordering::Relaxed), 1);
}

#[test]
fn reaching_is_the_standard_scale_and_one_limb_for_each_19_digits_of_the_exponent() {
    // The width is `SL + ceil((max(a, 0) + 1) / 19)` limbs, worked out here by hand and not
    // by the function: the first limb past the standard six holds the digit of an operand
    // with an adjusted exponent of 0, and a further limb comes at 19, 38, 57, ...
    for (adjusted, limbs) in [
        (-5, 7),     // 1 digit: 1 limb
        (-1, 7),     // 1
        (0, 7),      // 1
        (1, 7),      // 2 digits: 1 limb
        (18, 7),     // 19 digits: 1 limb
        (19, 8),     // 20 digits: 2 limbs
        (37, 8),     // 38 digits: 2 limbs
        (38, 9),     // 39 digits: 3 limbs
        (56, 9),     // 57 digits: 3 limbs
        (57, 10),    // 58 digits: 4 limbs
        (100, 12),   // 101 digits: 6 limbs
        (1000, 59),  // 1,001 digits: 53 limbs
        (6143, 330), // 6,144 digits: 324 limbs (6,144 / 19 = 323.37)
        (6144, 330), // 6,145 digits: 324 limbs
    ] {
        assert_eq!(Scale::reaching(adjusted).limbs, limbs, "adjusted {adjusted}");
        assert_eq!(Scale::reaching(adjusted).places(), limbs as u32 * 19, "adjusted {adjusted}");
    }
    // Whatever the exponent, the scale holds the operand and 114 places beyond it, in whole
    // limbs, and a larger exponent never gives a narrower scale.
    let mut previous = 0;
    for adjusted in 0..=6200i64 {
        let places = i64::from(Scale::reaching(adjusted).places());
        assert!(places >= adjusted + 1 + 114, "adjusted {adjusted}: {places} places");
        assert!(
            places < adjusted + 1 + 114 + 19,
            "adjusted {adjusted}: {places} places is a limb too wide"
        );
        assert!(places >= previous, "adjusted {adjusted}");
        previous = places;
    }
}
