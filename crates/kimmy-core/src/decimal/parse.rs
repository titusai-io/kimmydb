//! Reading a `Decimal128` from text: the strict, exact parser.
//!
//! `bson::Decimal128`'s own `FromStr` is lax: it reads a coefficient with
//! `u128::from_str`, which takes a leading `+`, and it reads an empty
//! coefficient as `0`. So `""` and `"."` became `0`, `"++1"` became `1`,
//! `"-+1"` became `-1` and `"e100"` became `0E+100`: 13 of the 99 syntax errors
//! in the General Decimal Arithmetic suite's `dqBase.decTest`. A blank imported
//! price was stored as zero, not refused. No released `bson` fixes it (3.1.0 is
//! the latest), so every place the product reads a decimal from text reads it
//! here instead.
//!
//! **The syntax** is the to-number syntax of the General Decimal Arithmetic
//! specification, which IEEE 754-2008's decimal character sequences follow,
//! and nothing else: no white space, ASCII digits only, letters in either case.
//!
//! ```text
//! numeric-string  = [sign] ( decimal-part [exponent-part] | infinity | nan )
//! decimal-part    = digits "." [digits] | ["."] digits
//! exponent-part   = ("e" | "E") [sign] digits
//! infinity        = "Inf" | "Infinity"
//! nan             = ["s"] "NaN" [digits]        ; the payload, at most 33 digits
//! sign            = "+" | "-"
//! ```
//!
//! **The value is exact or the text is refused.** A conversion never rounds:
//! a coefficient with a nonzero digit past the 34th, a value below `1E-6176`
//! that would need rounding to reach it, and a value above the largest
//! `Decimal128` are each refused, where IEEE 754 would round, flush or
//! overflow. Within that, the result is the representation IEEE 754 gives the
//! exact value, so a value's written quantum is kept wherever it fits: `1.50`
//! is `150E-2`. Where it cannot, the exponent moves only as far as the
//! format's limits require and the value does not change: zeros past the 34th
//! digit are dropped (`1.0000000000000000000000000000000000000` is
//! `1.000000000000000000000000000000000`), a large exponent is clamped by
//! padding the coefficient with zeros (`1E+6144` is
//! `1000000000000000000000000000000000E+6111`), trailing zeros are taken off a
//! value below the range (`1.0E-6176` is `1E-6176`), and a zero's exponent is
//! clamped into the range (`0E+9999` is `0E+6111`). Those are the IEEE
//! results, and the ones `dqBase.decTest` expects. A NaN keeps its sign, its
//! signalling bit and its payload; an infinity its sign.

use bson::Decimal128;

use super::{MAX_COEFFICIENT, MAX_EXPONENT, MIN_EXPONENT};

/// The digits of a `Decimal128` coefficient.
const PRECISION: usize = 34;

/// The most digits a NaN's payload may have: one fewer than the precision, as
/// the specification's clamped formats allow.
const PAYLOAD_DIGITS: usize = PRECISION - 1;

/// Why a text is not a `Decimal128`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// Not a number in the syntax above.
    Syntax,
    /// A nonzero digit past the 34th: the value would have to be rounded.
    Inexact,
    /// Above the largest `Decimal128`, `9.999…E+6144`.
    Overflow,
    /// Nonzero and below `1E-6176`, or with a nonzero digit below it: the value
    /// would have to be rounded, perhaps to zero.
    Underflow,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ParseError::Syntax => {
                "not a decimal number: write digits with at most one `.`, an optional sign \
                 and an optional exponent such as `E-3`, or Infinity or NaN"
            }
            ParseError::Inexact => {
                "more than 34 significant digits: a Decimal128 holds 34, and the value would \
                 have to be rounded"
            }
            ParseError::Overflow => {
                "above the largest Decimal128, 9.999999999999999999999999999999999E+6144"
            }
            ParseError::Underflow => {
                "below the smallest Decimal128, 1E-6176, or with a digit below it: the value \
                 would have to be rounded"
            }
        })
    }
}

impl std::error::Error for ParseError {}

/// Read `text` as a `Decimal128`, exactly, or refuse it.
pub fn parse(text: &str) -> Result<Decimal128, ParseError> {
    let (negative, rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    if rest.eq_ignore_ascii_case("inf") || rest.eq_ignore_ascii_case("infinity") {
        return Ok(bits((u128::from(negative) << 127) | INFINITY));
    }
    if let Some(nan) = nan(rest) {
        let (signalling, payload) = nan?;
        let quiet = (u128::from(negative) << 127) | NAN | payload;
        return Ok(bits(if signalling { quiet | SIGNALLING } else { quiet }));
    }
    let number = finite(rest)?;
    let (coefficient, exponent) = fit(&number)?;
    let biased = (i32::from(exponent) - i32::from(MIN_EXPONENT)) as u128;
    Ok(bits((u128::from(negative) << 127) | (biased << 113) | coefficient))
}

/// Bits 126..122 of an infinity, and of a NaN; bit 121 marks a signalling NaN.
const INFINITY: u128 = 0b11110 << 122;
const NAN: u128 = 0b11111 << 122;
const SIGNALLING: u128 = 1 << 121;

fn bits(word: u128) -> Decimal128 {
    Decimal128::from_bytes(word.to_le_bytes())
}

/// A NaN's signalling bit and payload, if `rest` names a NaN at all.
fn nan(rest: &str) -> Option<Result<(bool, u128), ParseError>> {
    let starts = |prefix: &str| {
        rest.get(..prefix.len()).is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    };
    let (signalling, payload) = if starts("snan") {
        (true, &rest[4..])
    } else if starts("nan") {
        (false, &rest[3..])
    } else {
        return None;
    };
    if !payload.bytes().all(|b| b.is_ascii_digit()) {
        return Some(Err(ParseError::Syntax));
    }
    let significant = payload.trim_start_matches('0');
    if significant.len() > PAYLOAD_DIGITS {
        return Some(Err(ParseError::Syntax));
    }
    // At most 33 digits, so it parses, and it is below 10^33: canonical.
    let payload =
        if significant.is_empty() { 0 } else { significant.parse().expect("at most 33 digits") };
    Some(Ok((signalling, payload)))
}

/// A finite number as written: its significant digits (leading zeros taken
/// off, so empty for a zero) and the exponent of the last of them.
struct Written<'a> {
    digits: std::borrow::Cow<'a, str>,
    exponent: i64,
}

/// The exponent is read saturating at this magnitude: beyond it every value is
/// out of range or a zero clamped into it, so the exact figure does not matter,
/// and the arithmetic below stays far from `i64`'s limits.
const EXPONENT_CAP: i64 = 1 << 40;

fn finite(text: &str) -> Result<Written<'_>, ParseError> {
    let (mantissa, exponent) = match text.find(['e', 'E']) {
        Some(at) => (&text[..at], Some(&text[at + 1..])),
        None => (text, None),
    };
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (mantissa, ""),
    };
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    // At least one digit, and nothing but digits around the one `.`.
    if whole.len() + fraction.len() == 0 || !digits(whole) || !digits(fraction) {
        return Err(ParseError::Syntax);
    }
    let exponent = match exponent {
        None => 0,
        Some(written) => {
            let unsigned = written.strip_prefix(['+', '-']).unwrap_or(written);
            if unsigned.is_empty() || !digits(unsigned) {
                return Err(ParseError::Syntax);
            }
            let magnitude = unsigned
                .bytes()
                .fold(0i64, |acc, b| (acc * 10 + i64::from(b - b'0')).min(EXPONENT_CAP));
            if written.starts_with('-') { -magnitude } else { magnitude }
        }
    };
    let fraction_len = i64::try_from(fraction.len()).unwrap_or(EXPONENT_CAP).min(EXPONENT_CAP);
    let digits = if fraction.is_empty() {
        std::borrow::Cow::Borrowed(whole.trim_start_matches('0'))
    } else {
        let joined = format!("{whole}{fraction}");
        std::borrow::Cow::Owned(joined.trim_start_matches('0').to_string())
    };
    Ok(Written { digits, exponent: exponent - fraction_len })
}

/// The coefficient and exponent of the exact value, within the format's
/// limits, or why it does not fit.
fn fit(number: &Written<'_>) -> Result<(u128, i16), ParseError> {
    let mut digits: &str = &number.digits;
    let mut exponent = number.exponent;
    if digits.is_empty() {
        // A zero: its exponent clamps into the range and its value is exact.
        let clamped = exponent.clamp(i64::from(MIN_EXPONENT), i64::from(MAX_EXPONENT));
        return Ok((0, clamped as i16));
    }
    // More digits than a coefficient holds: the ones past the 34th must be
    // zeros, which move into the exponent.
    if digits.len() > PRECISION {
        let (kept, dropped) = digits.split_at(PRECISION);
        if dropped.bytes().any(|b| b != b'0') {
            return Err(ParseError::Inexact);
        }
        exponent += dropped.len() as i64;
        digits = kept;
    }
    // Below the range: zeros at the end may be dropped, nothing else.
    if exponent < i64::from(MIN_EXPONENT) {
        let excess = i64::from(MIN_EXPONENT) - exponent;
        let keep = digits.len() as i64 - excess;
        if keep <= 0 {
            return Err(ParseError::Underflow);
        }
        let (kept, dropped) = digits.split_at(keep as usize);
        if dropped.bytes().any(|b| b != b'0') {
            return Err(ParseError::Underflow);
        }
        digits = kept;
        exponent = i64::from(MIN_EXPONENT);
    }
    let mut coefficient: u128 = digits.parse().expect("at most 34 ASCII digits");
    // Above the range: the coefficient takes zeros while it has room (the
    // clamp), and the value is too large where it has none.
    if exponent > i64::from(MAX_EXPONENT) {
        let pad = exponent - i64::from(MAX_EXPONENT);
        if digits.len() as i64 + pad > PRECISION as i64 {
            return Err(ParseError::Overflow);
        }
        coefficient *= 10u128.pow(pad as u32);
        exponent = i64::from(MAX_EXPONENT);
    }
    debug_assert!(coefficient <= MAX_COEFFICIENT);
    Ok((coefficient, exponent as i16))
}

#[cfg(test)]
mod tests {
    use num_bigint::BigInt;
    use proptest::prelude::*;

    use super::*;
    use crate::cmp::oracle::{Rational, scaled};
    use crate::decimal::{Decimal, decode};

    fn word(d: Decimal128) -> u128 {
        u128::from_le_bytes(d.bytes())
    }

    /// The three fields of a finite or zero result, read from the bits.
    fn fields(text: &str) -> (bool, u128, i16) {
        let w = word(parse(text).unwrap_or_else(|e| panic!("{text:?}: {e}")));
        assert_ne!((w >> 125) & 0b11, 0b11, "{text:?} is not finite");
        let exponent = ((w >> 113) & 0x3FFF) as i32 + i32::from(MIN_EXPONENT);
        (w >> 127 == 1, w & ((1 << 113) - 1), exponent as i16)
    }

    /// Every class of malformed text, the 13 `bson` 3.1.0 read as numbers among
    /// them (the first three groups), with the rest of each class around them.
    #[test]
    fn every_class_of_malformed_text_is_a_syntax_error() {
        let classes: [(&str, &[&str]); 9] = [
            // `bson` read these as zero.
            ("no digit in the mantissa", &["", ".", "+", "-", "-.", "+.", "..", "-.."]),
            // `bson` read `++1` as 1, `-+1` as -1, `0+5` as 5 and `.+1` as 0.01:
            // a `+` where the digits start, after any leading zeros.
            (
                "a second sign",
                &[
                    "++1", "--1", "-+1", "+-1", ".+1", "+.+1", "0+5", "00+12", "0.+5", "1.+5",
                    "1+1", "1E++1", "1E+-1", "1E-+1", "1E--1",
                ],
            ),
            // `bson` read `e100` as 0E+100 and the rest as 0E+1.
            (
                "an exponent with no mantissa",
                &["e100", "e+1", ".e+1", "+.e+1", "E+1", ".E+1", "+.E+1", "-.e", "-.E+", "E"],
            ),
            ("an exponent with no digits", &["1e", "12e+", "1E-", "1e.1", "1ee", "1e+e1", "7E"]),
            (
                "a second point or a misplaced one",
                &["1..2", "1.2.3", ".1.", "1e1.0", "1.e.1", "01.35."],
            ),
            (
                "white space, separators and other characters",
                &[
                    " 1", "1 ", "+ 1", " ", "1_000", "1,5", "12f4", "0x10", "1e5x", "1e1e1",
                    "\u{0}1",
                ],
            ),
            (
                "a digit that is not ASCII",
                &["\u{663}", "\u{ff11}", "1\u{663}", "\u{e5a}", "1e\u{663}"],
            ),
            (
                "a misspelt infinity or NaN",
                &[
                    "Infi",
                    "Infinit",
                    "Infinityy",
                    "0Inf",
                    "-0Inf",
                    "Inf0",
                    "-sNa",
                    "xNaN",
                    "qNaN",
                    "NaNq",
                    "NaNs",
                    "0sNaN",
                    ".NaN",
                    "+.Inf",
                    "NaN12.45",
                    "NaN-12",
                    "NaN+12",
                    "NaN123e+1",
                    "sNaN72.45",
                    "s",
                    "sN",
                ],
            ),
            (
                "a NaN payload of more than 33 digits",
                &[
                    "NaN1234567890123456789012345678901234",
                    "sNaN1234567890123456787234561234567890",
                    "-NaN0001234567890123456789012345678901234",
                ],
            ),
        ];
        for (class, texts) in classes {
            for text in texts {
                assert_eq!(parse(text), Err(ParseError::Syntax), "{class}: {text:?}");
            }
        }
        // The lax parser's answers, kept here so the difference is visible.
        for (text, lax) in
            [("", "0"), (".", "0"), ("++1", "1"), ("-+1", "-1"), ("0+5", "5"), ("e100", "0E+100")]
        {
            assert_eq!(text.parse::<Decimal128>().unwrap().to_string(), lax, "{text:?}");
        }
    }

    #[test]
    fn a_value_that_would_need_rounding_or_is_out_of_range_is_refused() {
        for text in [
            "1.0000000000000000000000000000000001",
            "12345678901234567890123456789012345",
            "-1.11111111111111111111111111111234549",
            "1234567890123456789012345678901234.5",
        ] {
            assert_eq!(parse(text), Err(ParseError::Inexact), "{text}");
        }
        for text in [
            "1E+6145",
            "10E+6144",
            "-1E+6145",
            "1E+99999999999999999999999",
            "1234567890123456789012345678901234E+6112",
        ] {
            assert_eq!(parse(text), Err(ParseError::Overflow), "{text}");
        }
        for text in [
            "1E-6177",
            "1.5E-6176",
            "0.1E-6176",
            "-0.00013E-6172",
            "1E-99999999999999999999999",
            "11E-6177",
        ] {
            assert_eq!(parse(text), Err(ParseError::Underflow), "{text}");
        }
    }

    /// The value as written, its quantum kept where it fits, and moved only as
    /// far as the limits require where it does not.
    #[test]
    fn a_valid_text_is_read_exactly_keeping_its_quantum_where_it_fits() {
        let ten33 = 10u128.pow(33);
        for (text, want) in [
            ("1.50", (false, 150, -2)),
            ("-12E+5", (true, 12, 5)),
            ("1.", (false, 1, 0)),
            (".5", (false, 5, -1)),
            ("+1e+1", (false, 1, 1)),
            ("1e0001", (false, 1, 1)),
            ("00012", (false, 12, 0)),
            ("0.0012", (false, 12, -4)),
            ("1E+6144", (false, ten33, 6111)),
            ("1.0000000000000000000000000000000000000", (false, ten33, -33)),
            ("10000000000000000000000000000000000000000", (false, ten33, 7)),
            ("1.0E-6176", (false, 1, -6176)),
            ("100E-6178", (false, 1, -6176)),
            ("0.00010E-6172", (false, 1, -6176)),
            ("9.999999999999999999999999999999999E+6144", (false, MAX_COEFFICIENT, 6111)),
            ("-9.999999999999999999999999999999999E+6144", (true, MAX_COEFFICIENT, 6111)),
            ("1E-6176", (false, 1, -6176)),
            (
                "12345678901234567890123456789012340E-1",
                (false, 1234567890123456789012345678901234, 0),
            ),
            // Zeros: the sign and the exponent are kept, the exponent clamped.
            ("0", (false, 0, 0)),
            ("-0", (true, 0, 0)),
            ("0.000", (false, 0, -3)),
            ("-0E+9999", (true, 0, 6111)),
            ("0E-9999", (false, 0, -6176)),
            ("0E+99999999999999999999999", (false, 0, 6111)),
            ("000.000E-2", (false, 0, -5)),
        ] {
            assert_eq!(fields(text), want, "{text}");
        }
    }

    #[test]
    fn infinity_and_nan_keep_their_sign_signalling_bit_and_payload() {
        for (text, negative) in [
            ("Inf", false),
            ("iNf", false),
            ("Infinity", false),
            ("-INFINITY", true),
            ("+inf", false),
        ] {
            let d = parse(text).unwrap();
            assert_eq!(
                decode(&d),
                if negative { Decimal::NegInfinity } else { Decimal::PosInfinity }
            );
            assert_eq!(word(d), (u128::from(negative) << 127) | INFINITY, "{text}");
        }
        let max_payload = "9".repeat(33);
        for (text, sign, signalling, payload) in [
            ("NaN", 0u128, false, 0u128),
            ("nan", 0, false, 0),
            ("-NaN", 1, false, 0),
            ("+nAn", 0, false, 0),
            ("NaN0", 0, false, 0),
            ("NaN0012", 0, false, 12),
            ("sNaN", 0, true, 0),
            ("-SNAN", 1, true, 0),
            ("sNaN007234", 0, true, 7234),
            ("sNaN0000", 0, true, 0),
        ] {
            let d = parse(text).unwrap();
            assert_eq!(decode(&d), Decimal::Nan);
            let mut want = (sign << 127) | NAN | payload;
            if signalling {
                want |= SIGNALLING;
            }
            assert_eq!(word(d), want, "{text}");
        }
        let d = parse(&format!("NaN{max_payload}")).unwrap();
        assert_eq!(word(d), NAN | (10u128.pow(33) - 1));
    }

    /// The exact rational a text in the grammar denotes, by a reader that
    /// shares nothing with [`parse`].
    fn rational(text: &str) -> Rational {
        let (negative, rest) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        let (mantissa, exponent) = match rest.split_once(['e', 'E']) {
            Some((m, e)) => (m, e.parse::<i64>().unwrap()),
            None => (rest, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let digits: BigInt = format!("0{whole}{fraction}").parse().unwrap();
        scaled(negative, digits, exponent - fraction.len() as i64)
    }

    fn value(d: Decimal128) -> Rational {
        match decode(&d) {
            Decimal::Zero => Rational::from_integer(0.into()),
            Decimal::Finite { negative, coefficient, exponent } => {
                scaled(negative, coefficient.into(), i64::from(exponent))
            }
            other => panic!("{other:?}"),
        }
    }

    /// Texts in the grammar: a sign, up to 40 digits around an optional point,
    /// and an exponent across and beyond the range.
    fn number_text() -> impl Strategy<Value = String> {
        (
            prop_oneof![Just(""), Just("+"), Just("-")],
            "[0-9]{0,20}",
            prop::option::of("[0-9]{0,20}"),
            prop::option::of((
                prop_oneof![Just("e"), Just("E")],
                prop_oneof![Just(""), Just("+"), Just("-")],
                prop_oneof![0i64..40, 6100i64..6250, 0i64..100_000],
            )),
        )
            .prop_filter("a digit", |(_, w, f, _)| {
                !w.is_empty() || f.as_ref().is_some_and(|f| !f.is_empty())
            })
            .prop_map(|(sign, whole, fraction, exponent)| {
                let mut s = format!("{sign}{whole}");
                if let Some(fraction) = fraction {
                    s.push('.');
                    s.push_str(&fraction);
                }
                if let Some((e, es, n)) = exponent {
                    s.push_str(&format!("{e}{es}{n}"));
                }
                s
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4000))]

        /// A text in the grammar is read to its exact value, keeping its written
        /// exponent when the coefficient and the exponent fit, or refused, and
        /// refused only when no `Decimal128` holds the value.
        #[test]
        fn a_number_is_read_exactly_or_refused_for_a_reason(text in number_text()) {
            let exact = rational(&text);
            match parse(&text) {
                Ok(d) => {
                    prop_assert_eq!(value(d), exact, "{}", text);
                    let (_, digits, written) = written_form(&text);
                    let (_, _, exponent) = fields(&text);
                    if digits <= 34 && (i64::from(MIN_EXPONENT)..=i64::from(MAX_EXPONENT)).contains(&written) {
                        prop_assert_eq!(i64::from(exponent), written, "{}", text);
                    }
                }
                Err(ParseError::Syntax) => prop_assert!(false, "{} is in the grammar", text),
                Err(reason) => {
                    prop_assert!(!representable(&text), "{} refused ({:?}) though a Decimal128 holds it", text, reason);
                }
            }
        }

        /// Against `bson` 3.1.0's parser: the same bytes wherever both read the
        /// text; where only `bson` reads it, the text is in one of the three lax
        /// classes; where only this one does, it is a NaN with a payload.
        #[test]
        fn the_same_bytes_as_bson_except_its_lax_classes(
            text in prop_oneof![
                number_text(),
                "[+\\-]{0,2}[0-9.eE+\\-]{0,6}",
                "[+\\-]?(s?nan|inf|infinity)[0-9]{0,3}",
            ]
        ) {
            match (parse(&text), text.parse::<Decimal128>()) {
                (Ok(ours), Ok(theirs)) => prop_assert_eq!(ours.bytes(), theirs.bytes(), "{}", text),
                (Ok(_), Err(_)) => prop_assert!(nan_with_payload(&text), "{}", text),
                (Err(ParseError::Syntax), Ok(_)) => prop_assert!(lax(&text), "{}", text),
                (Err(reason), Ok(_)) => prop_assert!(false, "{} refused ({:?}), bson reads it", text, reason),
                (Err(_), Err(_)) => {}
            }
        }
    }

    /// `(sign, significant digits, exponent of the last digit)` as written.
    fn written_form(text: &str) -> (bool, usize, i64) {
        let rest = text.trim_start_matches(['+', '-']);
        let (mantissa, exponent) = match rest.split_once(['e', 'E']) {
            Some((m, e)) => (m, e.parse::<i64>().unwrap()),
            None => (rest, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let significant = format!("{whole}{fraction}").trim_start_matches('0').len();
        (text.starts_with('-'), significant, exponent - fraction.len() as i64)
    }

    /// Whether some `Decimal128` holds the value `text` denotes: with its
    /// trailing zeros moved into the exponent it is `c × 10^q`, and the largest
    /// exponent that keeps the coefficient whole, `min(q, 6111)`, must be in the
    /// range and leave the coefficient at most 34 digits.
    fn representable(text: &str) -> bool {
        let rest = text.trim_start_matches(['+', '-']);
        let (mantissa, exponent) = match rest.split_once(['e', 'E']) {
            Some((m, e)) => (m, e.parse::<i64>().unwrap()),
            None => (rest, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let joined = format!("{whole}{fraction}");
        let significant = joined.trim_start_matches('0');
        if significant.is_empty() {
            return true;
        }
        let c = significant.trim_end_matches('0');
        let q = exponent - fraction.len() as i64 + (significant.len() - c.len()) as i64;
        let e = q.min(i64::from(MAX_EXPONENT));
        e >= i64::from(MIN_EXPONENT) && c.len() as i64 + (q - e) <= 34
    }

    fn nan_with_payload(text: &str) -> bool {
        let rest = text.trim_start_matches(['+', '-']).to_ascii_lowercase();
        let payload = rest.strip_prefix("snan").or_else(|| rest.strip_prefix("nan"));
        payload.is_some_and(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
    }

    /// `bson`'s lax classes: no digit in the mantissa (with an exponent or
    /// without), or a `+` where its digits start once the first `.` and any
    /// leading zeros are taken out (it reads them with `u128::from_str`).
    fn lax(text: &str) -> bool {
        let rest = text.strip_prefix(['+', '-']).unwrap_or(text);
        let mantissa = rest.split(['e', 'E']).next().unwrap_or("");
        let joined = mantissa.replacen('.', "", 1);
        !mantissa.bytes().any(|b| b.is_ascii_digit())
            || joined.trim_start_matches('0').starts_with('+')
    }
}
