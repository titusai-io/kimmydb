//! The one `Decimal128` decoder.
//!
//! BSON stores a `Decimal128` as 16 little-endian bytes in the IEEE 754-2008
//! binary integer decimal (BID) layout. `bson::Decimal128` exposes the bytes and
//! a text form and nothing else, so the coefficient and exponent are read here.
//! Every consumer of a decimal's value — the exact comparator, the key encoder,
//! `DocId::Decimal128`, the expression layer's truthiness — reads it through
//! [`decode`], so there is exactly one answer to "what number is this".
//!
//! Text is read by [`parse`], kimmydb's own strict and exact reader, and never
//! by `bson`'s `FromStr`, which takes `""` as zero (see the `parse` module).

use bson::Decimal128;

mod parse;

pub use parse::{ParseError, parse};

/// The largest coefficient a `Decimal128` can hold: `10^34 - 1`.
pub const MAX_COEFFICIENT: u128 = 9_999_999_999_999_999_999_999_999_999_999_999;

/// The smallest and largest exponent of a finite `Decimal128`.
pub const MIN_EXPONENT: i16 = -6176;
pub const MAX_EXPONENT: i16 = 6111;

const EXPONENT_BIAS: i32 = 6176;

/// What a `Decimal128` denotes, with every cohort member and every spelling of
/// zero, infinity and NaN collapsed to one class.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Decimal {
    /// Quiet or signalling, any payload, either sign: one NaN class.
    Nan,
    NegInfinity,
    PosInfinity,
    /// `+0`, `-0`, `0E-100`, and a non-canonical coefficient (IEEE 754-2008).
    Zero,
    /// The exact value `±coefficient × 10^exponent`, `0 < coefficient < 10^34`.
    ///
    /// The pair is *as written*: `1.0` and `1.00` are different pairs of the same
    /// value. [`Decimal::normalized`] gives the canonical pair.
    Finite {
        negative: bool,
        coefficient: u128,
        exponent: i16,
    },
}

/// Decode the BID bytes of a `Decimal128`.
pub fn decode(d: &Decimal128) -> Decimal {
    decode_bits(u128::from_le_bytes(d.bytes()))
}

/// [`decode`] on the 128 bits of the BID word.
pub fn decode_bits(bits: u128) -> Decimal {
    let negative = bits >> 127 == 1;
    // Bits 126..125 are `11` for infinity, NaN and the (non-canonical here)
    // large-coefficient form.
    if (bits >> 125) & 0b11 == 0b11 {
        return match (bits >> 122) & 0b11111 {
            0b11110 if negative => Decimal::NegInfinity,
            0b11110 => Decimal::PosInfinity,
            0b11111 => Decimal::Nan,
            // The large-coefficient form: its coefficient is at least 2^113,
            // above 10^34 - 1, which IEEE 754-2008 defines as zero.
            _ => Decimal::Zero,
        };
    }
    let biased = ((bits >> 113) & 0x3FFF) as i32;
    let coefficient = bits & ((1u128 << 113) - 1);
    if coefficient == 0 || coefficient > MAX_COEFFICIENT {
        return Decimal::Zero;
    }
    Decimal::Finite { negative, coefficient, exponent: (biased - EXPONENT_BIAS) as i16 }
}

/// Pack a finite value into canonical BID.
///
/// # Panics
/// If `coefficient` is zero or above `10^34 - 1`, or `exponent` is outside
/// `MIN_EXPONENT..=MAX_EXPONENT`: none of those is a canonical finite value.
pub fn pack(negative: bool, coefficient: u128, exponent: i16) -> Decimal128 {
    assert!(
        (1..=MAX_COEFFICIENT).contains(&coefficient)
            && (MIN_EXPONENT..=MAX_EXPONENT).contains(&exponent),
        "not a canonical finite Decimal128: {coefficient}E{exponent}"
    );
    let biased = (i32::from(exponent) + EXPONENT_BIAS) as u128;
    let bits = (u128::from(negative) << 127) | (biased << 113) | coefficient;
    Decimal128::from_bytes(bits.to_le_bytes())
}

impl Decimal {
    /// The canonical pair of a finite value: trailing decimal zeros stripped from
    /// the coefficient and moved into the exponent. Equal values give equal
    /// pairs. The exponent may leave the BID range (`1E+6144` is `1E6144`);
    /// [`pack`] does not accept that, [`pack_padded`] does.
    pub fn normalized(self) -> Self {
        match self {
            Decimal::Finite { negative, mut coefficient, mut exponent } => {
                while coefficient % 10 == 0 {
                    coefficient /= 10;
                    exponent += 1;
                }
                Decimal::Finite { negative, coefficient, exponent }
            }
            other => other,
        }
    }
}

/// Pack a normalised pair back into BID, padding the coefficient with zeros when
/// the exponent is above `MAX_EXPONENT`, or `None` if no `Decimal128` holds it.
///
/// Normalising can leave BID's exponent range (`1E+6144` normalises to `c = 1`,
/// `e = 6144`). Packed straight in, the biased exponent's top two bits are `11`,
/// the large-coefficient form, which decodes as a non-canonical zero. The
/// re-padded form `(c × 10^(e − 6111), 6111)` fits exactly when the pair was a
/// `Decimal128`'s to begin with ([`representable`]); for any other pair, such as
/// one read from a corrupt or hostile byte string, the padding is checked, never
/// wrapped, and the answer is `None`.
pub fn pack_padded(negative: bool, coefficient: u128, exponent: i16) -> Option<Decimal128> {
    let pad = u32::try_from(i32::from(exponent) - i32::from(MAX_EXPONENT)).unwrap_or(0);
    let coefficient = coefficient.checked_mul(10u128.checked_pow(pad)?)?;
    let exponent = exponent.min(MAX_EXPONENT);
    ((1..=MAX_COEFFICIENT).contains(&coefficient) && exponent >= MIN_EXPONENT)
        .then(|| pack(negative, coefficient, exponent))
}

/// The number of decimal digits of `c` (zero has none).
pub fn digits(c: u128) -> u32 {
    c.checked_ilog10().map_or(0, |log| log + 1)
}

/// Whether a `Decimal128` holds the normalised pair `(c, e)`: `c` has no
/// trailing zero digit, `1 <= c <= 10^34 − 1`.
///
/// Exactly when `e >= −6176` and `e + digits(c) <= 6111 + 34`. A `Decimal128`
/// holding it is `(c · 10^z, e − z)` for some `z >= 0` with `c · 10^z` at most
/// 34 digits (`z <= 34 − digits(c)`) and `−6176 <= e − z <= 6111`; such a `z`
/// exists exactly when `max(0, e − 6111) <= min(34 − digits(c), e + 6176)`,
/// which is those two conditions.
pub fn representable(coefficient: u128, exponent: i16) -> bool {
    (1..=MAX_COEFFICIENT).contains(&coefficient)
        && !coefficient.is_multiple_of(10)
        && exponent >= MIN_EXPONENT
        && i32::from(exponent) + digits(coefficient) as i32 <= i32::from(MAX_EXPONENT) + 34
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal128 {
        s.parse().unwrap()
    }

    #[test]
    fn finite_values_decode_to_their_coefficient_and_exponent() {
        assert_eq!(
            decode(&dec("1.50")),
            Decimal::Finite { negative: false, coefficient: 150, exponent: -2 }
        );
        assert_eq!(
            decode(&dec("-12E+5")),
            Decimal::Finite { negative: true, coefficient: 12, exponent: 5 }
        );
        assert_eq!(
            decode(&dec("9.999999999999999999999999999999999E+6144")),
            Decimal::Finite {
                negative: false,
                coefficient: MAX_COEFFICIENT,
                exponent: MAX_EXPONENT
            }
        );
        assert_eq!(
            decode(&dec("1E-6176")),
            Decimal::Finite { negative: false, coefficient: 1, exponent: MIN_EXPONENT }
        );
    }

    #[test]
    fn zero_infinity_and_nan_collapse_to_one_class_each() {
        for z in ["0", "-0", "0E-100", "0E+6111", "0.000"] {
            assert_eq!(decode(&dec(z)), Decimal::Zero, "{z}");
        }
        assert_eq!(decode(&dec("Infinity")), Decimal::PosInfinity);
        assert_eq!(decode(&dec("-Infinity")), Decimal::NegInfinity);
        assert_eq!(decode(&dec("NaN")), Decimal::Nan);
        assert_eq!(decode(&dec("-NaN")), Decimal::Nan);
        // Signalling and payload bits are ignored; so are infinity's payload bits.
        for payload in [0u128, 1, 0xFFFF, (1 << 100) | 7] {
            let nan = (0b11111u128 << 122) | payload;
            assert_eq!(decode_bits(nan), Decimal::Nan);
            assert_eq!(decode_bits(nan | (1 << 127)), Decimal::Nan);
            assert_eq!(decode_bits(nan | (1 << 121)), Decimal::Nan);
            let inf = (0b11110u128 << 122) | payload;
            assert_eq!(decode_bits(inf), Decimal::PosInfinity);
            assert_eq!(decode_bits(inf | (1 << 127)), Decimal::NegInfinity);
        }
    }

    #[test]
    fn a_non_canonical_coefficient_is_zero() {
        // Above 10^34 - 1 in the 113-bit field.
        let bits = (6176u128 << 113) | (MAX_COEFFICIENT + 1);
        assert_eq!(decode_bits(bits), Decimal::Zero);
        let bits = (6176u128 << 113) | ((1u128 << 113) - 1);
        assert_eq!(decode_bits(bits), Decimal::Zero);
        // The `11` combination form that is neither infinity nor NaN: every
        // pattern of the remaining bits.
        for tail in [0u128, 1, 0x1234_5678, (1u128 << 122) - 1] {
            for top in [0b11000u128, 0b11001, 0b11010, 0b11011, 0b11100, 0b11101] {
                let bits = (top << 122) | tail;
                assert_eq!(decode_bits(bits), Decimal::Zero, "{bits:#x}");
                assert_eq!(decode_bits(bits | (1 << 127)), Decimal::Zero);
            }
        }
    }

    #[test]
    fn pack_round_trips_decode() {
        for (neg, c, e) in [
            (false, 1u128, 0i16),
            (true, MAX_COEFFICIENT, MAX_EXPONENT),
            (false, 1, MIN_EXPONENT),
            (true, 123_456_789, -17),
        ] {
            assert_eq!(
                decode(&pack(neg, c, e)),
                Decimal::Finite { negative: neg, coefficient: c, exponent: e }
            );
        }
    }

    mod against_the_text {
        //! The decoder against the `bson` crate's own `Display` and `FromStr`,
        //! read by the exact parser of the comparator's oracle, which shares no
        //! code with [`decode`]. Review A re-derived the BID layout by hand; this
        //! is what checks the code that implements it.

        use num_bigint::BigInt;
        use proptest::prelude::*;

        use super::*;
        use crate::cmp::oracle::{Text, any_coefficient, parse_text};

        /// The decoder and the text agree on what `bits` is.
        fn agree(bits: u128) -> Result<(), TestCaseError> {
            let d = Decimal128::from_bytes(bits.to_le_bytes());
            let text = d.to_string();
            match (decode(&d), parse_text(&text)) {
                (Decimal::Nan, Text::Nan) => {}
                (Decimal::PosInfinity, Text::Infinity { negative: false }) => {}
                (Decimal::NegInfinity, Text::Infinity { negative: true }) => {}
                (Decimal::Zero, Text::Finite { coefficient, .. }) => {
                    prop_assert_eq!(coefficient, BigInt::from(0), "{}", text);
                }
                (
                    Decimal::Finite { negative, coefficient, exponent },
                    Text::Finite { negative: n, coefficient: c, exponent: e },
                ) => {
                    prop_assert_eq!(
                        (negative, BigInt::from(coefficient), i64::from(exponent)),
                        (n, c, e),
                        "{}",
                        text
                    );
                }
                (decoded, read) => prop_assert!(false, "{bits:#034x}: {decoded:?} vs {read:?}"),
            }
            Ok(())
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(5000))]

            /// Canonical values decode to exactly the pair their text spells, and
            /// the text parses back to the same bits.
            #[test]
            fn a_canonical_value_decodes_to_the_pair_its_text_spells(
                negative in any::<bool>(),
                coefficient in prop_oneof![Just(0u128), any_coefficient()],
                biased in prop_oneof![0u128..12288, Just(0u128), Just(6176u128), Just(12287u128)],
            ) {
                let bits = (u128::from(negative) << 127) | (biased << 113) | coefficient;
                agree(bits)?;
                let d = Decimal128::from_bytes(bits.to_le_bytes());
                let back: Decimal128 = d.to_string().parse().unwrap();
                prop_assert_eq!(back.bytes(), d.bytes(), "{}", d);
            }

            /// Any 128 bits at all: NaN, infinity, the `11` form and coefficients
            /// above the limit decode to the class the text names.
            #[test]
            fn any_bit_pattern_decodes_to_the_class_its_text_names(
                bits in any::<u128>(),
                form in 0u8..4,
            ) {
                let bits = match form {
                    // A zero coefficient, anywhere in the exponent range.
                    0 => bits & !((1u128 << 113) - 1),
                    // The two specials and the `11` form.
                    1 => bits | (0b11 << 125),
                    // A canonical-looking exponent with any coefficient bits.
                    2 => bits & !(0b11 << 125),
                    _ => bits,
                };
                agree(bits)?;
            }
        }
    }

    #[test]
    fn padding_a_pair_no_decimal_holds_is_refused_not_wrapped() {
        // At the edge: `1E+6144` and `9.99…E+6144` pad to fit; one digit or one
        // power more does not, and a coefficient whose padding would overflow a
        // `u128` is refused rather than wrapped into a wrong value.
        assert!(pack_padded(false, 1, 6144).is_some());
        assert!(pack_padded(false, MAX_COEFFICIENT, MAX_EXPONENT).is_some());
        assert!(pack_padded(false, 12, 6143).is_some());
        for (c, e) in [
            (12, 6144),
            (1, 6145),
            (MAX_COEFFICIENT, 6112),
            (u128::MAX / 3, 6140),
            // `(2^128 + 4) / 10`: padded by one digit it is `2^128 + 4`, which a
            // wrapping multiplication would turn into a canonical `4E+6111`.
            (34_028_236_692_093_846_346_337_460_743_176_821_146, 6112),
            (u128::MAX, i16::MAX),
            (1, MIN_EXPONENT - 1),
            (0, 0),
            (MAX_COEFFICIENT + 1, 0),
        ] {
            assert_eq!(pack_padded(false, c, e), None, "{c}E{e}");
            assert!(!representable(c, e), "{c}E{e}");
        }
    }

    use proptest::strategy::Strategy as _;

    proptest::proptest! {
        /// `representable` is the closed form of "padding fits": the two agree
        /// on every normalised pair, the edges included.
        #[test]
        fn representable_is_exactly_what_padding_fits(
            c in proptest::prop_oneof![
                1..=MAX_COEFFICIENT,
                1..1000u128,
                (0u32..34).prop_map(|k| 10u128.pow(k) + 1),
                proptest::prelude::Just(MAX_COEFFICIENT),
            ],
            e in proptest::prop_oneof![MIN_EXPONENT - 5..=MIN_EXPONENT + 5, 6100i16..=6150, proptest::prelude::any::<i16>()],
        ) {
            proptest::prop_assume!(c % 10 != 0);
            let fits = pack_padded(false, c, e).is_some_and(|d| decode(&d).normalized()
                == Decimal::Finite { negative: false, coefficient: c, exponent: e });
            proptest::prop_assert_eq!(representable(c, e), fits, "{}E{}", c, e);
        }
    }

    #[test]
    fn normalisation_strips_zeros_and_padding_restores_the_range() {
        let big = dec("1E+6144"); // c = 10^33, e = 6111
        let Decimal::Finite { negative, coefficient, exponent } = decode(&big).normalized() else {
            panic!("finite")
        };
        assert_eq!((coefficient, exponent), (1, 6144));
        assert_eq!(pack_padded(negative, coefficient, exponent), Some(big));
        let Decimal::Finite { coefficient, exponent, .. } = decode(&dec("1.00")).normalized()
        else {
            panic!("finite")
        };
        assert_eq!((coefficient, exponent), (1, 0));
    }
}
