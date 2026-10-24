//! Order-preserving byte encoding for BSON values.
//!
//! Secondary indexes are redb key ranges, and redb compares keys with `memcmp`.
//! So an index is only correct if the byte encoding of a value sorts exactly
//! the way the value itself does. That invariant is:
//!
//! ```text
//! encode(a).cmp(encode(b)) == canonical_cmp(a, b)
//! ```
//!
//! and it is property-tested against [`crate::cmp::canonical_cmp`] for
//! arbitrary value pairs. A bug here does not crash anything — it silently
//! returns wrong query results — which is why the encoder and the comparator
//! are written independently rather than one delegating to the other.
//!
//! Encodings are *self-delimiting*: a compound index key is the concatenation
//! of its parts, with no separators needed.
//!
//! **Numbers** (`Int32`, `Int64`, `Double`, `Decimal128`) share one form, so
//! equal values have equal keys whatever their types (storage schema 5):
//!
//! ```text
//! 20 subtag                                   NaN 00, -Inf 01, zero 03, +Inf 05
//! 20 subtag ‖ exp(2) ‖ esc(M) ‖ END           negative 02, positive 04
//! ```
//!
//! `exp` is `⌊log2 |x|⌋ + 32768`, big-endian. `M` is the first 128 bits of the
//! binary expansion (top bit set), as 16 bytes with the trailing zero bytes
//! stripped, escaped like a string (`00` is written `00 FF`). `END` is `00 00`
//! when no later bit is nonzero and `00 01` when one is: the value is then
//! strictly above `M`, and `00 01` sorts above the exact `M` and below every
//! longer exact value, which continues with `00 FF` or a nonzero byte. A
//! negative number inverts the whole body. `Int64(5)` is `20 04 80 02 A0 00 00`.
//!
//! 128 bits suffice: two distinct decimals differ within their first 114 bits
//! (`10^34 < 2^113`), and a binary value has at most 64, so equal keys mean
//! equal values and the mark settles the rest (design §4.4).

use bson::Bson;

use crate::cmp::{Numeric, decompose};
use crate::decimal::{self, Decimal};
use crate::error::Result;

pub(crate) mod number;

// Type tags. These match the ranks in `cmp::type_rank` so that cross-type
// ordering falls out of the leading byte alone.
const TAG_MIN_KEY: u8 = 0x01;

/// The lowest byte any real encoded key can begin with.
///
/// Exported so that the storage layer can place its sentinel runs **below every
/// real key** without repeating a literal that would then be able to drift away
/// from this table. `kimmy-storage`'s `UNKEYED` run sits below this, and so did a
/// schema 4 store's `UNDECIDABLE` run (ADR-185, removed by ADR-238; the schema 5
/// migration still reads it there), and a query's range starts at it.
pub const LEAST_KEY_TAG: u8 = TAG_MIN_KEY;
const TAG_NULL: u8 = 0x10;
const TAG_NUMBER: u8 = 0x20;
const TAG_STRING: u8 = 0x30;
const TAG_DOCUMENT: u8 = 0x40;
const TAG_ARRAY: u8 = 0x50;
const TAG_BINARY: u8 = 0x60;
const TAG_OBJECT_ID: u8 = 0x70;
const TAG_BOOL: u8 = 0x80;
const TAG_DATE: u8 = 0x90;
const TAG_TIMESTAMP: u8 = 0xA0;
const TAG_REGEX: u8 = 0xB0;
const TAG_DB_POINTER: u8 = 0xC0;
const TAG_JS_CODE: u8 = 0xD0;
const TAG_JS_CODE_WITH_SCOPE: u8 = 0xE0;
const TAG_MAX_KEY: u8 = 0xF0;

// Sub-tags within the number group, ordered so the leading byte alone sorts
// NaN < -Inf < negatives < zero < positives < +Inf.
const NUM_NAN: u8 = 0x00;
const NUM_NEG_INF: u8 = 0x01;
const NUM_NEGATIVE: u8 = 0x02;
const NUM_ZERO: u8 = 0x03;
const NUM_POSITIVE: u8 = 0x04;
const NUM_POS_INF: u8 = 0x05;

// Byte-string framing. A terminator of `00 00` sorts below an escaped NUL of
// `00 FF`, which is what makes "ab" sort before "ab\0c".
const ESCAPE_TERMINATOR: [u8; 2] = [0x00, 0x00];
const ESCAPED_NUL: [u8; 2] = [0x00, 0xFF];

// The end of a number's mantissa: `00 00` (the string terminator) when the
// key holds the whole value, `00 01` when a nonzero bit follows the 128 the key
// holds. Between the terminator and an escaped NUL, so a truncated value sorts
// above its exact prefix and below every longer exact value.
const EXACT_END: [u8; 2] = ESCAPE_TERMINATOR;
const TRUNCATED_END: [u8; 2] = [0x00, 0x01];

// Element framing inside documents and arrays. A present element leads with
// `01` and the end of the composite is `00`, so a prefix sorts before a longer
// sequence.
const ELEMENT_PRESENT: u8 = 0x01;
const ELEMENT_END: u8 = 0x00;

/// Encode a value into `out`.
///
/// Every BSON value has a key, a `Decimal128` included (schema 5: its exact
/// value, in the one number form). The `Result` is kept for the callers, which
/// were written when a decimal was refused here.
pub fn encode_into(value: &Bson, out: &mut Vec<u8>) -> Result<()> {
    match value {
        Bson::MinKey => out.push(TAG_MIN_KEY),
        Bson::MaxKey => out.push(TAG_MAX_KEY),
        Bson::Null | Bson::Undefined => out.push(TAG_NULL),

        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) => {
            out.push(TAG_NUMBER);
            let n = decompose(value).expect("numeric variant decomposes");
            encode_numeric(n, out);
        }

        Bson::Decimal128(d) => {
            out.push(TAG_NUMBER);
            encode_decimal(decimal::decode(d), out);
        }

        Bson::String(s) | Bson::Symbol(s) => {
            out.push(TAG_STRING);
            encode_bytes(s.as_bytes(), out);
        }

        Bson::Document(d) => {
            out.push(TAG_DOCUMENT);
            for (key, val) in d {
                out.push(ELEMENT_PRESENT);
                encode_bytes(key.as_bytes(), out);
                encode_into(val, out)?;
            }
            out.push(ELEMENT_END);
        }

        Bson::Array(a) => {
            out.push(TAG_ARRAY);
            for element in a {
                out.push(ELEMENT_PRESENT);
                encode_into(element, out)?;
            }
            out.push(ELEMENT_END);
        }

        Bson::Binary(b) => {
            out.push(TAG_BINARY);
            // Length first, matching Mongo's ordering for binary data.
            out.extend_from_slice(&(b.bytes.len() as u64).to_be_bytes());
            out.push(u8::from(b.subtype));
            out.extend_from_slice(&b.bytes);
        }

        Bson::ObjectId(oid) => {
            out.push(TAG_OBJECT_ID);
            // ObjectIds are already big-endian and fixed width.
            out.extend_from_slice(&oid.bytes());
        }

        Bson::Boolean(v) => {
            out.push(TAG_BOOL);
            out.push(u8::from(*v));
        }

        Bson::DateTime(dt) => {
            out.push(TAG_DATE);
            encode_i64(dt.timestamp_millis(), out);
        }

        Bson::Timestamp(ts) => {
            out.push(TAG_TIMESTAMP);
            out.extend_from_slice(&ts.time.to_be_bytes());
            out.extend_from_slice(&ts.increment.to_be_bytes());
        }

        Bson::RegularExpression(re) => {
            out.push(TAG_REGEX);
            encode_bytes(re.pattern.as_str().as_bytes(), out);
            encode_bytes(re.options.as_str().as_bytes(), out);
        }

        Bson::DbPointer(_) => out.push(TAG_DB_POINTER),

        Bson::JavaScriptCode(code) => {
            out.push(TAG_JS_CODE);
            encode_bytes(code.as_bytes(), out);
        }

        Bson::JavaScriptCodeWithScope(cws) => {
            out.push(TAG_JS_CODE_WITH_SCOPE);
            encode_bytes(cws.code.as_bytes(), out);
            encode_into(&Bson::Document(cws.scope.clone()), out)?;
        }
    }
    Ok(())
}

/// Encode a single value to a fresh buffer.
pub fn encode(value: &Bson) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(16);
    encode_into(value, &mut out)?;
    Ok(out)
}

/// Encode a compound key: the concatenation of its parts.
///
/// No separator is needed because every individual encoding is self-delimiting.
pub fn encode_compound(values: &[Bson]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(16 * values.len());
    for value in values {
        encode_into(value, &mut out)?;
    }
    Ok(out)
}

/// Encode a compound key where individual components may sort descending.
///
/// A descending component has every byte of its encoding inverted. That
/// reverses its order *exactly* — but only because this encoding is
/// **prefix-free**: no encoding is ever a proper prefix of another.
///
/// Prefix-freeness is what makes the trick sound. For a code with prefixes, if
/// `A` is a prefix of `B` then `A < B`, and `flip(A)` is still a prefix of
/// `flip(B)`, so `flip(A) < flip(B)` — the order is *preserved* rather than
/// reversed, and a descending index would silently sort ascending. Fixed-width
/// encodings share a length, variable-width ones are terminated, and composites
/// end with a terminator that cannot appear where an element starts, so the
/// property holds throughout.
pub fn encode_compound_ordered(components: &[(Bson, bool)]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(16 * components.len());
    for (value, descending) in components {
        let start = out.len();
        encode_into(value, &mut out)?;
        if *descending {
            for byte in &mut out[start..] {
                *byte = !*byte;
            }
        }
    }
    Ok(out)
}

/// Encode a binary number in the one number form.
///
/// Because every numeric type reduces to the same normalized form, `5i32`,
/// `5i64`, and `5.0f64` all produce identical bytes — which is exactly what an
/// index needs, so that a lookup for `5` finds a document that stored `5.0`.
fn encode_numeric(n: Numeric, out: &mut Vec<u8>) {
    match n {
        Numeric::Nan => out.push(NUM_NAN),
        Numeric::NegInfinity => out.push(NUM_NEG_INF),
        Numeric::Zero => out.push(NUM_ZERO),
        Numeric::PosInfinity => out.push(NUM_POS_INF),
        Numeric::Finite { negative, exp, mantissa } => {
            encode_finite(negative, number::binary(exp, mantissa), out);
        }
    }
}

/// Encode a decimal in the one number form: a decimal equal to a binary value
/// has that value's bytes, and every spelling of zero, NaN or an infinity is
/// that class's key.
fn encode_decimal(d: Decimal, out: &mut Vec<u8>) {
    match d {
        Decimal::Nan => out.push(NUM_NAN),
        Decimal::NegInfinity => out.push(NUM_NEG_INF),
        Decimal::Zero => out.push(NUM_ZERO),
        Decimal::PosInfinity => out.push(NUM_POS_INF),
        Decimal::Finite { negative, coefficient, exponent } => {
            encode_finite(negative, number::decimal(coefficient, exponent), out);
        }
    }
}

/// `subtag ‖ exp ‖ esc(M) ‖ END`, the body inverted for a negative value.
fn encode_finite(negative: bool, body: number::Body, out: &mut Vec<u8>) {
    out.push(if negative { NUM_NEGATIVE } else { NUM_POSITIVE });
    let start = out.len();
    // Bias the exponent so it encodes as an unsigned big-endian value that
    // sorts in numeric order. The widest decimal exponents are -20517 and
    // 20413, well inside the field.
    let biased = u16::try_from(body.exp + 32_768).expect("a number's exponent fits the field");
    out.extend_from_slice(&biased.to_be_bytes());
    // The window's top bit is set, so at least its first byte survives the
    // stripping of trailing zero bytes.
    let window = body.window.to_be_bytes();
    escape_into(&window[..16 - (body.window.trailing_zeros() / 8) as usize], out);
    out.extend_from_slice(if body.inexact { &TRUNCATED_END } else { &EXACT_END });
    if negative {
        // Larger magnitude means a smaller value, so invert. The body is
        // prefix-free, so the inversion reverses its order exactly.
        for byte in &mut out[start..] {
            *byte = !*byte;
        }
    }
}

/// Encode a signed integer so that its bytes sort in numeric order.
fn encode_i64(v: i64, out: &mut Vec<u8>) {
    // Flipping the sign bit maps [i64::MIN, i64::MAX] onto [0, u64::MAX]
    // monotonically.
    out.extend_from_slice(&((v as u64) ^ (1u64 << 63)).to_be_bytes());
}

/// Write a byte string with NUL escaping and a terminator, so that it can be
/// followed by another encoded value without ambiguity.
fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    escape_into(bytes, out);
    out.extend_from_slice(&ESCAPE_TERMINATOR);
}

/// Write bytes with every NUL escaped, unterminated.
fn escape_into(bytes: &[u8], out: &mut Vec<u8>) {
    for &byte in bytes {
        if byte == 0x00 {
            out.extend_from_slice(&ESCAPED_NUL);
        } else {
            out.push(byte);
        }
    }
}

#[cfg(test)]
mod tests {
    /// The values both key-encoding premises are asserted over.
    ///
    /// One list, so a value added for either premise is covered by both: they are
    /// two halves of one assumption, that no real key can reach a sentinel run.
    fn corpus() -> Vec<Bson> {
        let dec = |s: &str| Bson::Decimal128(s.parse().unwrap());
        let mut out = vec![
            Bson::MinKey,
            Bson::MaxKey,
            Bson::Null,
            Bson::Undefined,
            Bson::Boolean(false),
            Bson::Int32(0),
            Bson::Int64(0),
            Bson::Double(0.0),
            Bson::Double(-0.0),
            Bson::Double(f64::NAN),
            Bson::Double(f64::INFINITY),
            Bson::Double(f64::NEG_INFINITY),
            Bson::String(String::new()),
            Bson::Document(bson::Document::new()),
            Bson::Array(vec![]),
            Bson::Array(vec![Bson::Array(vec![])]),
            Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: Vec::new(),
            }),
            Bson::Timestamp(bson::Timestamp { time: 0, increment: 0 }),
            Bson::DateTime(bson::DateTime::from_millis(0)),
        ];
        // Decimals are keyed like every other number (schema 5), so they must sort
        // above the sentinels too: one equal to an integer, one whose key is
        // truncated (`00 01`), the extremes, and the specials and zero spelt as
        // decimals, each in both signs where there is one.
        out.extend(
            [
                "1",
                "-1",
                "0.1",
                "-0.1",
                "1E-6176",
                "-1E-6176",
                "9.999999999999999999999999999999999E+6144",
                "-9.999999999999999999999999999999999E+6144",
                "NaN",
                "Infinity",
                "-Infinity",
                "-0",
                "0E-100",
            ]
            .map(dec),
        );
        out
    }

    #[test]
    fn every_real_key_sorts_above_every_sentinel() {
        // `kimmy-storage` files documents it **cannot key** under the empty key,
        // and through schema 4 filed documents whose membership its filter
        // could not decide under `[0x00]` (ADR-185; the schema 5 migration
        // reads that run on such a store before draining it) — both below every
        // real key, so each run was walked exactly once by its own prepended
        // range while a query's ranges start at `LEAST_KEY_TAG`.
        //
        // All of that rests on one fact, and the fact lives here rather than
        // there: no real key begins below `LEAST_KEY_TAG`. A tag added below it
        // would put real documents *into* a sentinel run — rechecked on every
        // scan, and reported to an operator as a fault the index has.
        //
        // This is the test the second sentinel made necessary. Before it, the
        // boundary was `[0x00]`, which was above every sentinel until `[0x00]`
        // became one.
        let sentinels: [&[u8]; 2] = [&[], &[0x00]];
        for value in corpus() {
            for descending in [false, true] {
                let key = encode_compound_ordered(&[(value.clone(), descending)]).unwrap();
                for sentinel in sentinels {
                    assert!(
                        key.as_slice() > sentinel,
                        "{value:?} (descending: {descending}) encodes to {key:?}, which does not \
                         sort above the sentinel {sentinel:?}, so it would land in that run"
                    );
                }
                assert!(
                    key[0] >= LEAST_KEY_TAG,
                    "{value:?} begins with {:#04x}, below LEAST_KEY_TAG, and every query range \
                     starts at that tag — so this key would be outside all of them",
                    key[0]
                );
            }
        }
    }

    #[test]
    /// No document ever encodes to an empty key — which is what lets the empty
    /// key be the sentinel for the unkeyed run.
    ///
    /// **What breaking this actually costs, measured rather than assumed.** The
    /// obvious fear is a lost document: every query range starts at
    /// `LEAST_KEY_TAG`, so a real key that encoded empty would sit below every
    /// range. It is **not** lost — a document filed at the empty key is in the
    /// unkeyed run, which is prepended whenever it holds anything, so the
    /// document is visited there and rechecked against the full filter. Making
    /// the empty string encode empty leaves `executor_containment.rs` green, in
    /// both deliveries.
    ///
    /// What it costs is quieter: that document would be found by a recheck on
    /// **every** scan of the index rather than by its key, and it would be
    /// counted as unkeyable when it is nothing of the kind. So this test guards
    /// an assumption about cost and honesty, not about correctness, and says so
    /// rather than claiming a red it cannot produce.
    fn no_value_encodes_to_an_empty_key() {
        let dec = |s: &str| Bson::Decimal128(s.parse().unwrap());
        let values = corpus();
        // The empty ones are the interesting ones: an empty string, an empty
        // document, an empty array and empty binary are the values a reader
        // would guess might encode to nothing at all.
        for value in &values {
            for descending in [false, true] {
                let one = encode_compound_ordered(&[(value.clone(), descending)])
                    .expect("every value has a key");
                assert!(
                    !one.is_empty(),
                    "{value:?} (descending: {descending}) encodes to an empty key, which is the \
                     unkeyed run's sentinel — it would be filed there and dropped from every \
                     query range"
                );
                // And in compound form, where an empty component could vanish
                // between two others.
                let compound = encode_compound_ordered(&[
                    (value.clone(), descending),
                    (value.clone(), !descending),
                ])
                .expect("every value here encodes");
                assert!(!compound.is_empty(), "{value:?} compounds to an empty key");
                assert!(
                    compound.len() > one.len(),
                    "a second component must add bytes, or one of them contributed none: \
                     {value:?}"
                );
            }
        }
        assert!(values.len() > 15, "premise: the corpus is the corpus ({})", values.len());

        // **A `Decimal128` is keyed, not refused and not encoded to nothing.**
        // Until schema 5 it was refused, which filed its document in the unkeyed
        // run on purpose; an empty key would have put it there silently and hidden
        // it from every query range. Now it has the key of its value: the key of
        // `1` for `1.00`, in both directions.
        for descending in [false, true] {
            let keyed = encode_compound_ordered(&[(dec("1.00"), descending)])
                .expect("a Decimal128 is keyed");
            assert!(!keyed.is_empty());
            assert_eq!(keyed, encode_compound_ordered(&[(Bson::Int32(1), descending)]).unwrap());
        }
    }

    use std::cmp::Ordering;

    use bson::{Bson, doc};

    use super::*;
    use crate::cmp::canonical_cmp;

    fn enc(v: &Bson) -> Vec<u8> {
        encode(v).unwrap_or_else(|e| panic!("encoding {v:?} failed: {e}"))
    }

    fn regex(pattern: &str) -> Bson {
        Bson::RegularExpression(bson::Regex {
            pattern: pattern.to_string().try_into().expect("valid pattern"),
            options: String::new().try_into().expect("valid options"),
        })
    }

    /// Assert the encoding agrees with the comparator, in both directions.
    fn assert_agrees(a: &Bson, b: &Bson) {
        let expected = canonical_cmp(a, b);
        let actual = enc(a).cmp(&enc(b));
        assert_eq!(actual, expected, "encoding disagrees with canonical_cmp for {a:?} vs {b:?}");
    }

    fn lt(a: Bson, b: Bson) {
        assert_eq!(enc(&a).cmp(&enc(&b)), Ordering::Less, "expected {a:?} < {b:?}");
        assert_agrees(&a, &b);
    }

    fn eq_bytes(a: Bson, b: Bson) {
        assert_eq!(enc(&a), enc(&b), "expected identical encodings for {a:?} and {b:?}");
    }

    #[test]
    fn type_tags_order_across_types() {
        lt(Bson::MinKey, Bson::Null);
        lt(Bson::Null, Bson::Int32(-99999));
        lt(Bson::Double(f64::INFINITY), Bson::String(String::new()));
        lt(Bson::String("zzz".into()), Bson::Document(doc! {}));
        lt(Bson::Document(doc! {}), Bson::Array(vec![]));
        lt(Bson::Boolean(true), Bson::DateTime(bson::DateTime::from_millis(i64::MIN)));
        lt(regex("z"), Bson::MaxKey);
    }

    #[test]
    fn equal_numbers_encode_identically_across_types() {
        // An index lookup for `5` must find a document that stored `5.0`.
        eq_bytes(Bson::Int32(5), Bson::Int64(5));
        eq_bytes(Bson::Int64(5), Bson::Double(5.0));
        eq_bytes(Bson::Int32(-1), Bson::Double(-1.0));
        eq_bytes(Bson::Double(0.0), Bson::Double(-0.0));
        eq_bytes(Bson::Int32(0), Bson::Double(0.0));
    }

    #[test]
    fn numbers_sort_numerically() {
        lt(Bson::Double(f64::NAN), Bson::Double(f64::NEG_INFINITY));
        lt(Bson::Double(f64::NEG_INFINITY), Bson::Int64(i64::MIN));
        lt(Bson::Int64(i64::MIN), Bson::Int64(-1));
        lt(Bson::Int64(-1), Bson::Double(-0.5));
        lt(Bson::Double(-0.5), Bson::Int32(0));
        lt(Bson::Int32(0), Bson::Double(0.5));
        lt(Bson::Double(0.5), Bson::Int64(1));
        lt(Bson::Int64(i64::MAX), Bson::Double(f64::INFINITY));
    }

    #[test]
    fn large_integers_stay_distinct() {
        // The pair f64 cannot represent separately. Encoding through a double
        // would collapse these into one index entry.
        let a = Bson::Int64(9_007_199_254_740_992);
        let b = Bson::Int64(9_007_199_254_740_993);
        assert_ne!(enc(&a), enc(&b), "2^53 and 2^53+1 must not collide");
        lt(a, b);
        lt(Bson::Int64(i64::MAX - 1), Bson::Int64(i64::MAX));
    }

    #[test]
    fn subnormals_order_correctly() {
        lt(Bson::Double(0.0), Bson::Double(5e-324));
        lt(Bson::Double(5e-324), Bson::Double(1e-323));
        lt(Bson::Double(-5e-324), Bson::Double(0.0));
        lt(Bson::Double(1e-323), Bson::Double(f64::MIN_POSITIVE));
    }

    #[test]
    fn strings_escape_nul_and_respect_prefixes() {
        lt(Bson::String("ab".into()), Bson::String("abc".into()));
        // The terminator must sort below an escaped NUL, or a string
        // containing a NUL would sort before its own prefix.
        lt(Bson::String("ab".into()), Bson::String("ab\u{0}c".into()));
        lt(Bson::String("ab\u{0}".into()), Bson::String("ab\u{0}\u{0}".into()));
        lt(Bson::String(String::new()), Bson::String("\u{0}".into()));
    }

    #[test]
    fn dates_handle_negative_timestamps() {
        lt(
            Bson::DateTime(bson::DateTime::from_millis(-1)),
            Bson::DateTime(bson::DateTime::from_millis(0)),
        );
        lt(
            Bson::DateTime(bson::DateTime::from_millis(i64::MIN)),
            Bson::DateTime(bson::DateTime::from_millis(i64::MAX)),
        );
    }

    #[test]
    fn arrays_and_documents_nest() {
        lt(Bson::Array(vec![Bson::Int32(1)]), Bson::Array(vec![Bson::Int32(2)]));
        lt(Bson::Array(vec![Bson::Int32(1)]), Bson::Array(vec![Bson::Int32(1), Bson::Int32(0)]));
        lt(Bson::Document(doc! { "a": 99 }), Bson::Document(doc! { "b": 1 }));
        lt(
            Bson::Document(doc! { "a": doc! { "b": 1 } }),
            Bson::Document(doc! { "a": doc! { "b": 2 } }),
        );
    }

    #[test]
    fn decimal128_is_encoded_by_its_exact_value() {
        // Once refused because an approximate key would silently mis-order
        // values; now exact, so a decimal equal to a double has the double's
        // bytes and one that is not equal never does.
        let d = |s: &str| Bson::Decimal128(s.parse().unwrap());
        eq_bytes(d("1.5"), Bson::Double(1.5));
        eq_bytes(d("1.50"), d("15E-1"));
        eq_bytes(d("-1.5"), Bson::Double(-1.5));
        assert_ne!(enc(&d("0.1")), enc(&Bson::Double(0.1)));
        lt(d("0.1"), Bson::Double(0.1));
        lt(Bson::Double(f64::from_bits(0.1f64.to_bits() - 1)), d("0.1"));
        lt(d("1.499999999999999999999999999999999"), Bson::Double(1.5));
    }

    #[test]
    fn compound_keys_are_self_delimiting() {
        // A longer first component must not be confusable with a shorter first
        // component plus a second one.
        let a = encode_compound(&[Bson::String("a".into()), Bson::String("b".into())]).unwrap();
        let b = encode_compound(&[Bson::String("ab".into()), Bson::String("".into())]).unwrap();
        assert_ne!(a, b);
        assert!(a < b, "('a','b') should sort before ('ab','')");
    }

    #[test]
    fn descending_components_reverse_their_order() {
        let asc = |a: i32, b: i32| {
            encode_compound_ordered(&[(Bson::Int32(a), false), (Bson::Int32(b), false)]).unwrap()
        };
        let desc = |a: i32, b: i32| {
            encode_compound_ordered(&[(Bson::Int32(a), false), (Bson::Int32(b), true)]).unwrap()
        };

        // Leading component ascending in both.
        assert!(asc(1, 0) < asc(2, 0));
        assert!(desc(1, 0) < desc(2, 0));

        // Trailing component: ascending normally, reversed when descending.
        assert!(asc(1, 1) < asc(1, 2));
        assert!(desc(1, 2) < desc(1, 1), "a descending component must invert");
    }

    #[test]
    fn descending_works_for_variable_width_components() {
        // Strings are terminated rather than fixed-width, so this exercises the
        // prefix-free property the inversion depends on.
        let d = |s: &str| encode_compound_ordered(&[(Bson::String(s.into()), true)]).unwrap();
        assert!(d("b") < d("a"));
        assert!(d("abc") < d("ab"), "a longer string must sort first descending");
        assert!(d("ab\u{0}c") < d("ab"));
    }

    #[test]
    fn descending_handles_mixed_types_and_numeric_edges() {
        let d = |v: Bson| encode_compound_ordered(&[(v, true)]).unwrap();
        // Canonical order reversed: MaxKey first, MinKey last.
        assert!(d(Bson::MaxKey) < d(Bson::MinKey));
        assert!(d(Bson::Int64(i64::MAX)) < d(Bson::Int64(i64::MIN)));
        assert!(d(Bson::Double(f64::INFINITY)) < d(Bson::Double(f64::NAN)));
        // Equal values across types still collide, ascending or descending.
        assert_eq!(d(Bson::Int32(5)), d(Bson::Double(5.0)));
    }

    #[test]
    fn compound_keys_order_by_leading_component() {
        let first = encode_compound(&[Bson::Int32(1), Bson::String("zzz".into())]).unwrap();
        let second = encode_compound(&[Bson::Int32(2), Bson::String("aaa".into())]).unwrap();
        assert!(first < second, "the leading component must dominate");
    }

    mod props {
        use proptest::prelude::*;

        use super::*;

        /// Values chosen to concentrate on the boundaries where an
        /// order-preserving encoding actually breaks: type edges, numeric
        /// limits, NUL bytes, and empty composites. Numbers come from the
        /// comparator's oracle generators too, so decimals of every spelling
        /// and decimals beside doubles reach compound and descending keys.
        fn any_bson() -> impl Strategy<Value = Bson> {
            let leaf = prop_oneof![
                crate::cmp::oracle::any_number(),
                crate::cmp::oracle::any_number(),
                Just(Bson::MinKey),
                Just(Bson::MaxKey),
                Just(Bson::Null),
                any::<i32>().prop_map(Bson::Int32),
                any::<i64>().prop_map(Bson::Int64),
                any::<f64>().prop_map(Bson::Double),
                prop_oneof![
                    Just(f64::NAN),
                    Just(f64::INFINITY),
                    Just(f64::NEG_INFINITY),
                    Just(0.0f64),
                    Just(-0.0f64),
                    Just(f64::MIN_POSITIVE),
                    Just(5e-324f64),
                ]
                .prop_map(Bson::Double),
                "[a-c\u{0}]{0,4}".prop_map(Bson::String),
                any::<bool>().prop_map(Bson::Boolean),
                any::<i64>().prop_map(|m| Bson::DateTime(bson::DateTime::from_millis(m))),
                any::<[u8; 12]>().prop_map(|b| Bson::ObjectId(bson::oid::ObjectId::from_bytes(b))),
                (
                    prop_oneof![
                        Just(0u8),
                        Just(2u8),
                        Just(3u8),
                        Just(4u8),
                        Just(0x80u8),
                        Just(0xFFu8),
                        any::<u8>(),
                    ],
                    prop::collection::vec(any::<u8>(), 0..4),
                )
                    .prop_map(|(subtype, bytes)| Bson::Binary(bson::Binary {
                        subtype: bson::spec::BinarySubtype::from(subtype),
                        bytes,
                    })),
            ];

            leaf.prop_recursive(3, 12, 3, |inner| {
                prop_oneof![
                    prop::collection::vec(inner.clone(), 0..3).prop_map(Bson::Array),
                    prop::collection::vec(("[a-b]{1,2}", inner), 0..3).prop_map(|pairs| {
                        let mut doc = bson::Document::new();
                        for (k, v) in pairs {
                            doc.insert(k, v);
                        }
                        Bson::Document(doc)
                    }),
                ]
            })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(2000))]

            /// The invariant the whole index layer rests on.
            #[test]
            fn encoding_order_matches_canonical_order(a in any_bson(), b in any_bson()) {
                let (ea, eb) = (encode(&a).unwrap(), encode(&b).unwrap());
                prop_assert_eq!(
                    ea.cmp(&eb),
                    canonical_cmp(&a, &b),
                    "encoding disagrees for {:?} vs {:?}", a, b
                );
            }

            /// Equal values must encode identically, or an index lookup for a
            /// value would miss documents that stored an equal one.
            #[test]
            fn equal_values_encode_identically(a in any_bson(), b in any_bson()) {
                if canonical_cmp(&a, &b) == Ordering::Equal {
                    prop_assert_eq!(encode(&a).unwrap(), encode(&b).unwrap());
                }
            }

            /// Inverting a component's bytes must reverse its order exactly.
            ///
            /// This is the property descending index fields rest on, and it
            /// only holds because the encoding is prefix-free.
            #[test]
            fn descending_reverses_order_for_any_value(a in any_bson(), b in any_bson()) {
                let asc = |v: &Bson| encode_compound_ordered(&[(v.clone(), false)]).unwrap();
                let desc = |v: &Bson| encode_compound_ordered(&[(v.clone(), true)]).unwrap();
                prop_assert_eq!(
                    desc(&a).cmp(&desc(&b)),
                    asc(&a).cmp(&asc(&b)).reverse(),
                    "descending did not invert for {:?} vs {:?}", a, b
                );
            }

            /// A mixed-direction compound key must order by each component's
            /// own direction, leading component first.
            #[test]
            fn mixed_direction_compound_keys_order_per_component(
                a1 in any_bson(), a2 in any_bson(),
                b1 in any_bson(), b2 in any_bson(),
                d1 in any::<bool>(), d2 in any::<bool>(),
            ) {
                let dir = |o: Ordering, desc: bool| if desc { o.reverse() } else { o };
                let expected = match dir(canonical_cmp(&a1, &b1), d1) {
                    Ordering::Equal => dir(canonical_cmp(&a2, &b2), d2),
                    other => other,
                };
                let key = |x: &Bson, y: &Bson| {
                    encode_compound_ordered(&[(x.clone(), d1), (y.clone(), d2)]).unwrap()
                };
                prop_assert_eq!(key(&a1, &a2).cmp(&key(&b1, &b2)), expected);
            }

            /// Concatenation must not create ambiguity between different
            /// component splits.
            #[test]
            fn compound_encoding_matches_lexicographic_component_order(
                a in prop::collection::vec(any_bson(), 1..3),
                b in prop::collection::vec(any_bson(), 1..3),
            ) {
                let expected = a
                    .iter()
                    .zip(b.iter())
                    .map(|(x, y)| canonical_cmp(x, y))
                    .find(|o| *o != Ordering::Equal)
                    .unwrap_or_else(|| a.len().cmp(&b.len()));
                let actual = encode_compound(&a).unwrap().cmp(&encode_compound(&b).unwrap());
                prop_assert_eq!(actual, expected);
            }
        }
    }
}

/// The number keys against the comparator and against a third implementation:
/// a key function written here on exact rationals, from the layout alone,
/// sharing no code with the encoder or the comparator (design §4.5, §10).
#[cfg(test)]
mod number_keys {
    use std::cmp::Ordering;

    use bson::{Bson, doc};
    use num_bigint::{BigUint, Sign};
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;

    use super::*;
    use crate::cmp::canonical_cmp;
    use crate::cmp::oracle::{self, Exact, SplitMix, any_number, dec, oracle_cmp};
    use crate::decimal::{MAX_COEFFICIENT, MAX_EXPONENT, MIN_EXPONENT};

    /// What a finite nonzero number's key holds, read off its exact value.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    struct Parts {
        negative: bool,
        /// `⌊log2 |x|⌋`.
        exp: i64,
        /// `⌊|x| · 2^(127 − exp)⌋`.
        window: u128,
        /// Whether that floor left a remainder.
        truncated: bool,
    }

    fn to_u128(n: &BigUint) -> u128 {
        let digits = n.to_u64_digits();
        assert!(digits.len() <= 2, "{n} does not fit 128 bits");
        u128::from(*digits.first().unwrap_or(&0)) | (u128::from(*digits.get(1).unwrap_or(&0)) << 64)
    }

    fn parts(value: &Bson) -> Option<Parts> {
        let Exact::Finite(r) = oracle::exact(value)? else { return None };
        let (sign, n) = r.numer().clone().into_parts();
        if sign == Sign::NoSign {
            return None;
        }
        let d = r.denom().magnitude().clone();
        let at_least =
            |l: i64| if l >= 0 { n >= (&d << l as usize) } else { (&n << (-l) as usize) >= d };
        let mut exp = n.bits() as i64 - d.bits() as i64;
        while !at_least(exp) {
            exp -= 1;
        }
        while at_least(exp + 1) {
            exp += 1;
        }
        let shift = 127 - exp;
        let (num, den) =
            if shift >= 0 { (n << shift as usize, d) } else { (n, d << (-shift) as usize) };
        let window = to_u128(&(&num / &den));
        assert_eq!(window >> 127, 1, "the window's top bit is set");
        let truncated = num % den != BigUint::from(0u32);
        Some(Parts { negative: sign == Sign::Minus, exp, window, truncated })
    }

    /// The bytes of §4.1 for a finite body: `20 subtag ‖ exp ‖ esc(M) ‖ END`.
    fn layout(p: Parts) -> Vec<u8> {
        let mut body = u16::try_from(p.exp + 32_768).unwrap().to_be_bytes().to_vec();
        let mut m = p.window.to_be_bytes().to_vec();
        while m.last() == Some(&0) {
            m.pop();
        }
        for byte in m {
            if byte == 0 {
                body.extend([0x00, 0xFF]);
            } else {
                body.push(byte);
            }
        }
        body.extend(if p.truncated { [0x00, 0x01] } else { [0x00, 0x00] });
        if p.negative {
            body.iter_mut().for_each(|b| *b = !*b);
        }
        let mut key = vec![0x20, if p.negative { 0x02 } else { 0x04 }];
        key.extend(body);
        key
    }

    /// The reference key of a number.
    fn reference_key(value: &Bson) -> Vec<u8> {
        match oracle::exact(value).expect("a number") {
            Exact::Nan => vec![0x20, 0x00],
            Exact::NegInfinity => vec![0x20, 0x01],
            Exact::PosInfinity => vec![0x20, 0x05],
            Exact::Finite(_) => parts(value).map_or(vec![0x20, 0x03], layout),
        }
    }

    fn key(v: &Bson) -> Vec<u8> {
        encode(v).expect("every number has a key")
    }

    /// The three implementations agree on `a`, `b` and their order.
    fn agree(a: &Bson, b: &Bson) {
        let (ka, kb) = (key(a), key(b));
        assert_eq!(ka, reference_key(a), "{a} ({a:?})");
        assert_eq!(kb, reference_key(b), "{b} ({b:?})");
        let expected = oracle_cmp(a, b);
        assert_eq!(canonical_cmp(a, b), expected, "comparator: {a} vs {b}");
        assert_eq!(ka.cmp(&kb), expected, "keys: {a} vs {b}: {ka:02X?} vs {kb:02X?}");
        assert!(ka.len() <= 36 && kb.len() <= 36, "a number key is at most 36 bytes");
    }

    fn d(text: &str) -> Bson {
        Bson::Decimal128(text.parse().unwrap())
    }

    /// The double `m · 2^f`, exactly (`m < 2^53`, no overflow or underflow).
    fn double(negative: bool, m: u64, f: i32) -> Bson {
        assert!(m < 1 << 53);
        let v = m as f64 * 2f64.powi(f);
        assert_eq!(BigUint::from(m), oracle_mantissa(v, f), "{m} · 2^{f} is a double");
        Bson::Double(if negative { -v } else { v })
    }

    fn oracle_mantissa(v: f64, f: i32) -> BigUint {
        let r = num_rational::BigRational::from_float(v).unwrap();
        let scaled = if f >= 0 {
            r / num_bigint::BigInt::from(BigUint::from(1u32) << f as usize)
        } else {
            r * num_bigint::BigInt::from(BigUint::from(1u32) << (-f) as usize)
        };
        assert!(scaled.is_integer());
        scaled.to_integer().into_parts().1
    }

    /// The inverse of an odd `a` modulo `2^64`.
    fn inverse_mod_2_64(a: u64) -> u64 {
        let mut x = a; // right to three bits: a·a ≡ 1 (mod 8)
        for _ in 0..5 {
            x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        }
        assert_eq!(a.wrapping_mul(x), 1);
        x
    }

    fn checked_decimal(negative: bool, c: &BigUint, e: i64) -> Option<Bson> {
        let c = (c.bits() <= 113).then(|| to_u128(c))?;
        let e = i16::try_from(e).ok()?;
        ((1..=MAX_COEFFICIENT).contains(&c) && (MIN_EXPONENT..=MAX_EXPONENT).contains(&e))
            .then(|| dec(negative, c, e))
    }

    /// Decimals within a hair of a double, built rather than searched for
    /// (design §10 item 3): `(decimal, double, decimal is above)`.
    ///
    /// - `e < 0`: an odd 53-bit `m` with `m · 5^q ≡ ∓1 (mod 2^r)` makes
    ///   `c = (m · 5^q ± 1) / 2^r` an integer, and `c · 10^-q` is
    ///   `m · 2^(−q−r) ± 10^-q · 2^-r`.
    /// - `e > 0`: `m · 2^(f−e) ≡ ∓1 (mod 5^e)` makes `c = (m · 2^(f−e) ± 1) / 5^e`
    ///   an integer, and `c · 10^e` is `m · 2^f ± 2^e`.
    ///
    /// Above the double, most share its whole 128-bit window and differ only in
    /// the mark, which is the only thing that orders them; random search finds
    /// two such pairs in a million and a half.
    fn near_double_pairs(rng: &mut SplitMix, n: usize) -> Vec<(Bson, Bson, bool)> {
        let mut out = Vec::new();
        while out.len() < n {
            let (negative, above) = (rng.next() & 1 == 1, rng.next() & 1 == 1);
            if rng.next() & 1 == 0 {
                let r = 16 + rng.below(37) as u32; // 16..=52
                let q = 1 + rng.below(48) as u32; // 1..=48
                let modulus = 1u64 << r;
                let inverse = inverse_mod_2_64(5u64.wrapping_pow(q)) & (modulus - 1);
                let target = if above { inverse.wrapping_neg() & (modulus - 1) } else { inverse };
                let (lo, hi) = (
                    ((1u64 << 52) - target).div_ceil(modulus),
                    ((1u64 << 53) - 1 - target) / modulus,
                );
                let m = target + (lo + rng.below(u128::from(hi - lo + 1)) as u64) * modulus;
                let numerator = BigUint::from(m) * BigUint::from(5u32).pow(q);
                let numerator = if above { numerator + 1u32 } else { numerator - 1u32 };
                assert_eq!(&numerator % modulus, BigUint::from(0u32));
                if let Some(x) = checked_decimal(negative, &(numerator >> r), -i64::from(q)) {
                    out.push((x, double(negative, m, -((q + r) as i32)), above));
                }
            } else {
                // From `e = 8` the range of `f` below is not empty; to `e = 22`,
                // `5^e < 2^53` leaves `m` free modulo `5^e`.
                let e = 8 + rng.below(15) as u32; // 8..=22
                let f_max = 60 + (332 * e) / 100;
                let f = e + 76 + rng.below(u128::from(f_max - e - 76)) as u32;
                let modulus = 5u128.pow(e);
                // The inverse of 2^(f−e) modulo 5^e: (5^e + 1) / 2 is that of 2.
                let mut inverse = 1u128;
                for _ in 0..f - e {
                    inverse = inverse * modulus.div_ceil(2) % modulus;
                }
                let target = if above { (modulus - inverse) % modulus } else { inverse };
                let (lo, hi) = (
                    (1u128 << 52).saturating_sub(target).div_ceil(modulus),
                    ((1u128 << 53) - 1 - target) / modulus,
                );
                let m = (target + (lo + rng.below(hi - lo + 1)) * modulus) as u64;
                let numerator = BigUint::from(m) << (f - e) as usize;
                let numerator = if above { numerator + 1u32 } else { numerator - 1u32 };
                assert_eq!(&numerator % modulus, BigUint::from(0u32));
                if let Some(x) = checked_decimal(negative, &(numerator / modulus), i64::from(e)) {
                    out.push((x, double(negative, m, f as i32), above));
                }
            }
        }
        out
    }

    /// Decimals exactly equal to doubles, for both signs of the exponent:
    /// `r · 2^(f−j)` at exponent `j` is the double `r · 5^j · 2^f`, and `m · 5^k`
    /// at exponent `−k` is the double `m · 2^-k`.
    fn equal_double_pairs(rng: &mut SplitMix, n: usize) -> Vec<(Bson, Bson)> {
        let mut out = Vec::new();
        while out.len() < n {
            let negative = rng.next() & 1 == 1;
            if rng.next() & 1 == 0 {
                let j = rng.below(23) as u32; // 5^j < 2^53
                let r = (rng.below(u128::from((1u64 << 53) / 5u64.pow(j))) as u64) | 1;
                let shift = rng.below(120) as u32;
                let c = BigUint::from(r) << shift as usize;
                if let Some(x) = checked_decimal(negative, &c, i64::from(j)) {
                    out.push((x, double(negative, r * 5u64.pow(j), (shift + j) as i32)));
                }
            } else {
                let k = 1 + rng.below(48) as u32;
                let limit = (MAX_COEFFICIENT / 5u128.pow(k)).min((1 << 53) - 1);
                let m = 1 + rng.below(limit) as u64;
                let c = BigUint::from(m) * BigUint::from(5u32).pow(k);
                if let Some(x) = checked_decimal(negative, &c, -i64::from(k)) {
                    out.push((x, double(negative, m, -(k as i32))));
                }
            }
        }
        out
    }

    #[test]
    fn the_layout_in_bytes_and_sizes() {
        // §4.1: the mantissa byte of 5 is A0, and the key is 7 bytes.
        assert_eq!(key(&Bson::Int64(5)), [0x20, 0x04, 0x80, 0x02, 0xA0, 0x00, 0x00]);
        assert_eq!(key(&Bson::Int32(1)), [0x20, 0x04, 0x80, 0x00, 0x80, 0x00, 0x00]);
        assert_eq!(key(&Bson::Int32(-1)), [0x20, 0x02, 0x7F, 0xFF, 0x7F, 0xFF, 0xFF]);
        assert_eq!(key(&Bson::Int64(i64::MIN)), [0x20, 0x02, 0x7F, 0xC0, 0x7F, 0xFF, 0xFF]);
        assert_eq!(key(&d("5.0")), key(&Bson::Int64(5)));
        // An interior zero byte costs an escape: these are bounds, not averages.
        assert_eq!(key(&Bson::Double(1.0 + f64::EPSILON)).len(), 18);
        assert_eq!(key(&Bson::Int64((1 << 62) + 1)).len(), 20);
        // 0.1 fills the window with CC and is truncated: 22 bytes.
        let tenth = key(&d("0.1"));
        assert_eq!(tenth.len(), 22);
        assert_eq!(&tenth[..4], [0x20, 0x04, 0x7F, 0xFC]);
        assert!(tenth[4..20].iter().all(|&b| b == 0xCC));
        assert_eq!(&tenth[20..], [0x00, 0x01]);
        // Zero and the specials stay two bytes, in every spelling.
        for (value, subtag) in [
            (Bson::Int32(0), 0x03),
            (d("-0E-100"), 0x03),
            (Bson::Double(f64::NAN), 0x00),
            (d("-NaN"), 0x00),
            (Bson::Double(f64::NEG_INFINITY), 0x01),
            (d("-Infinity"), 0x01),
            (d("Infinity"), 0x05),
        ] {
            assert_eq!(key(&value), [0x20, subtag], "{value}");
        }
        // The longest key a decimal can have: a window of 80, fourteen zero
        // bytes and a last byte, truncated. 16 + 14 escapes + 6 = 36.
        let longest = key(&d("8714930974926918267958656901890887E+5940"));
        assert_eq!(longest.len(), 36);
        assert_eq!(longest[4], 0x80);
        assert!(longest[5..33].chunks(2).all(|pair| pair == [0x00, 0xFF]));
        assert_eq!(&longest[33..], [0x09, 0x00, 0x01]);
        // A decimal just above a power of two: the window is `80` alone and the
        // mark is set, 7 bytes, between `2^9852` and everything above it.
        let above_power = d("5591357763832179507651766021379519E+2932");
        let k = key(&above_power);
        assert_eq!(k.len(), 7);
        assert_eq!(&k[4..], [0x80, 0x00, 0x01]);
        agree(&above_power, &d("5591357763832179507651766021379520E+2932"));
        agree(&d("5591357763832179507651766021379518E+2932"), &above_power);
        // The exponent field at both ends of the decimal range.
        assert_eq!(&key(&d("1E-6176"))[2..4], (32_768u16 - 20_517).to_be_bytes());
        let largest = key(&d("9.999999999999999999999999999999999E+6144"));
        assert_eq!(&largest[2..4], (32_768u16 + 20_413).to_be_bytes());
    }

    #[test]
    fn the_a1_pair_has_equal_bytes() {
        // `c = 2^100` at `e = 22`: its integer significand `c · 5^22` is 152 bits
        // long, but every bit after the 128th is zero, so the key is exact, as
        // the equal double's is. A mark taken from the bit count would differ.
        let decimal = d("1267650600228229401496703205376E+22");
        let binary = double(false, 5u64.pow(22), 122);
        assert_eq!(((BigUint::from(1u32) << 100usize) * BigUint::from(5u32).pow(22)).bits(), 152);
        assert_eq!(key(&decimal), key(&binary));
        assert_eq!(&key(&decimal)[key(&decimal).len() - 2..], [0x00, 0x00]);
        agree(&decimal, &binary);
    }

    #[test]
    fn long_binary_windows_are_exact_and_equal_their_doubles() {
        // `c = 2^k`, `e = 0..=27`: windows up to 113 + 2.32·27 bits long. Up to
        // `e = 22`, `5^e` is below 2^53 and the value is a double too.
        for k in 0..=112u32 {
            for e in 0..=27i16 {
                for negative in [false, true] {
                    let x = dec(negative, 1u128 << k, e);
                    assert_eq!(key(&x), reference_key(&x), "2^{k}E{e}");
                    if e <= 22 {
                        let y = double(negative, 5u64.pow(e as u32), (k + e as u32) as i32);
                        assert_eq!(key(&x), key(&y), "2^{k}E{e}");
                        assert_eq!(canonical_cmp(&x, &y), Ordering::Equal);
                    }
                }
            }
        }
    }

    #[test]
    fn decimals_beside_doubles_order_by_their_mark_alone() {
        let mut rng = SplitMix(11);
        let pairs = near_double_pairs(&mut rng, 4000);
        let mut shared = 0;
        for (x, y, above) in &pairs {
            agree(x, y);
            assert_ne!(key(x), key(y), "{x} is not {y}");
            let (kx, ky) = (key(x), key(y));
            if kx.len() == ky.len() && kx[..kx.len() - 1] == ky[..ky.len() - 1] {
                // The same exponent and window: only the mark orders them.
                assert!(*above, "only a decimal above the double can share its window");
                let last = if kx[1] == 0x02 { 0xFE } else { 0x01 };
                assert_eq!(kx[kx.len() - 1], last, "{x}");
                shared += 1;
            }
        }
        // These pairs are what kill a mark that is dropped: by order alone,
        // nothing else can tell such a decimal from its double.
        assert!(shared > 1000, "{shared} of {} pairs share the whole window", pairs.len());
    }

    #[test]
    fn decimals_equal_to_doubles_have_their_bytes() {
        let mut rng = SplitMix(12);
        for (x, y) in equal_double_pairs(&mut rng, 4000) {
            assert_eq!(key(&x), key(&y), "{x} == {y}");
            assert_eq!(canonical_cmp(&x, &y), Ordering::Equal, "{x} == {y}");
            agree(&x, &y);
        }
        // Integers above 2^53 and 2^64, and a double above 2^113.
        agree(&d("9007199254740993"), &Bson::Int64((1 << 53) + 1));
        assert_eq!(key(&d("9007199254740993")), key(&Bson::Int64((1 << 53) + 1)));
        assert_eq!(key(&d("18446744073709551616")), key(&Bson::Double(2f64.powi(64))));
        assert_eq!(
            key(&d("1208925819614629174706176E+20")),
            key(&double(false, 5u64.pow(20), 100))
        );
    }

    /// The most leading bits two finite values' keys share.
    fn shared_bits(a: &Bson, b: &Bson) -> u32 {
        match (parts(a), parts(b)) {
            (Some(p), Some(q)) if p.exp == q.exp && p.negative == q.negative => {
                (p.window ^ q.window).leading_zeros()
            }
            _ => 0,
        }
    }

    #[test]
    fn adjacent_decimals_share_at_most_113_bits() {
        // Lemma 1: two distinct decimals differ by at least 10^-34 of the larger
        // (2^-112.95), so their windows share at most 113 leading bits and differ
        // by the 114th: 15 bits of margin to 128.
        // (2^113 − 1 is not here: it is above 10^34 − 1, a non-canonical zero.)
        let mut widest = 0;
        for c in [10u128.pow(33), MAX_COEFFICIENT, 1 << 112] {
            for e in [MIN_EXPONENT, -358, -340, -1, 0, 1, 308, 309, MAX_EXPONENT] {
                for negative in [false, true] {
                    let mut neighbours = vec![(c - 1, c)];
                    if c < MAX_COEFFICIENT {
                        neighbours.push((c, c + 1));
                    }
                    for (low, high) in neighbours {
                        let (x, y) = (dec(negative, low, e), dec(negative, high, e));
                        agree(&x, &y);
                        assert_ne!(key(&x), key(&y));
                        let shared = shared_bits(&x, &y);
                        assert!(shared <= 113, "{x} and {y} share {shared} bits");
                        widest = widest.max(shared);
                    }
                }
            }
        }
        // And the bound is reached: the margin to 128 is 15 bits, no more.
        assert_eq!(widest, 113, "the pairs reach the bound");
    }

    /// Read one number key from the front of `bytes`, from the layout alone:
    /// its length, and for a finite value its parts.
    fn parse_number(bytes: &[u8], descending: bool) -> (usize, Option<Parts>) {
        let flip = if descending { 0xFF } else { 0x00 };
        let at = |i: usize| bytes[i] ^ flip;
        assert_eq!(at(0), 0x20, "a number key");
        let negative = match at(1) {
            0x00 | 0x01 | 0x03 | 0x05 => return (2, None),
            0x02 => true,
            0x04 => false,
            other => panic!("no subtag {other:#04x}"),
        };
        let body = |i: usize| at(i) ^ if negative { 0xFF } else { 0x00 };
        let exp = i64::from(u16::from_be_bytes([body(2), body(3)])) - 32_768;
        let (mut i, mut m) = (4, Vec::new());
        let truncated = loop {
            match (body(i), body(i + 1)) {
                (0x00, 0x00) => break false,
                (0x00, 0x01) => break true,
                (0x00, 0xFF) => m.push(0x00),
                (0x00, other) => panic!("no escape 00 {other:02X}"),
                (byte, _) => {
                    m.push(byte);
                    i += 1;
                    continue;
                }
            }
            i += 2;
        };
        assert!(!m.is_empty() && m.len() <= 16 && m.last() != Some(&0x00), "stripped: {m:02X?}");
        m.resize(16, 0);
        let window = u128::from_be_bytes(m.try_into().unwrap());
        (i + 2, Some(Parts { negative, exp, window, truncated }))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2500))]

        /// The invariant, over all four types: the key is the reference key, and
        /// keys order as the comparator and as the exact values do.
        #[test]
        fn number_keys_are_the_reference_keys_and_order_by_value(a in any_number(), b in any_number()) {
            agree(&a, &b);
            if canonical_cmp(&a, &b) == Ordering::Equal {
                prop_assert_eq!(key(&a), key(&b));
            }
        }

        /// A key read back from the front of a compound key ends exactly where
        /// the number's own key ends, ascending and descending, and holds the
        /// number's parts.
        #[test]
        fn a_number_key_delimits_itself(
            a in any_number(),
            b in prop_oneof![any_number(), "[a\u{0}]{0,3}".prop_map(Bson::String)],
            da in any::<bool>(),
            db in any::<bool>(),
        ) {
            let alone = encode_compound_ordered(&[(a.clone(), da)]).unwrap();
            let both = encode_compound_ordered(&[(a.clone(), da), (b.clone(), db)]).unwrap();
            let (length, read) = parse_number(&both, da);
            prop_assert_eq!(length, alone.len(), "{} then {}", a, b);
            prop_assert_eq!(&both[..length], &alone[..]);
            prop_assert_eq!(read, parts(&a));
        }

        /// Every cohort member has one key.
        #[test]
        fn cohort_members_have_one_key(
            negative in any::<bool>(),
            c in 1..10_000_000u128,
            e in (MIN_EXPONENT + 34)..=MAX_EXPONENT,
        ) {
            let first = key(&dec(negative, c, e));
            let mut k = 1u32;
            while c * 10u128.pow(k) <= MAX_COEFFICIENT {
                prop_assert_eq!(&key(&dec(negative, c * 10u128.pow(k), e - k as i16)), &first);
                k += 1;
            }
        }

        /// Random neighbours keep the Lemma 1 margin too.
        #[test]
        fn neighbouring_decimals_keep_the_margin(
            c in 1..MAX_COEFFICIENT,
            e in MIN_EXPONENT..=MAX_EXPONENT,
        ) {
            let (x, y) = (dec(false, c, e), dec(false, c + 1, e));
            prop_assert!(shared_bits(&x, &y) <= 113);
            prop_assert!(key(&x) < key(&y));
        }
    }

    /// The three implementations over a large deterministic corpus: every
    /// generator above at a hundred times the default size, and random numbers
    /// of every kind in adjacent pairs. Run with
    /// `cargo test -p kimmy-core --release --lib keyenc::number_keys::a_large_corpus -- --ignored`.
    #[test]
    #[ignore]
    fn a_large_corpus() {
        let mut rng = SplitMix(21);
        let (mut shared, mut pairs) = (0, 0);
        for (x, y, _) in near_double_pairs(&mut rng, 200_000) {
            agree(&x, &y);
            let (kx, ky) = (key(&x), key(&y));
            shared += usize::from(kx.len() == ky.len() && kx[..kx.len() - 1] == ky[..ky.len() - 1]);
            pairs += 1;
        }
        for (x, y) in equal_double_pairs(&mut rng, 100_000) {
            assert_eq!(key(&x), key(&y), "{x} == {y}");
            agree(&x, &y);
        }
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let strategy = (any_number(), any_number());
        for _ in 0..200_000 {
            let (a, b) = strategy.new_tree(&mut runner).unwrap().current();
            agree(&a, &b);
            if canonical_cmp(&a, &b) == Ordering::Equal {
                assert_eq!(key(&a), key(&b), "{a} == {b}");
            }
        }
        println!("{pairs} near-double pairs, {shared} sharing the whole window");
    }

    #[test]
    fn the_layout_orders_the_three_kinds_of_key_that_share_a_window() {
        // §4.4: the exact window (`00 00`) < the window with a nonzero tail
        // (`00 01`) < every longer exact value (`00 FF …` or a nonzero byte),
        // whatever the window's zero bytes, and reversed for negatives. Built from
        // parts through the encoder's own body writer, so windows no real value
        // reaches are covered too.
        let windows: Vec<u128> = vec![
            1 << 127,
            (1 << 127) | 1,
            (1 << 127) | (1 << 8),
            (1 << 127) | (1 << 120),
            (0x80 << 120) | (0x01 << 112),
            u128::MAX,
            u128::MAX - 1,
            (0xA0 << 120) | 0xFF,
            0xFF00_FF00_FF00_FF00_FF00_FF00_FF00_FF00,
            0x8000_0000_0000_0001_0000_0000_0000_0000,
        ];
        let mut keys = Vec::new();
        for &window in &windows {
            for truncated in [false, true] {
                for exp in [-1, 0, 1] {
                    for negative in [false, true] {
                        let p = Parts { negative, exp, window, truncated };
                        let mut out = vec![TAG_NUMBER];
                        encode_finite(
                            negative,
                            number::Body { exp: exp as i32, window, inexact: truncated },
                            &mut out,
                        );
                        assert_eq!(out, layout(p), "{p:?}");
                        keys.push((p, out));
                    }
                }
            }
        }
        let value_order = |p: &Parts, q: &Parts| {
            let magnitude = (p.exp, p.window, p.truncated).cmp(&(q.exp, q.window, q.truncated));
            match (p.negative, q.negative) {
                (false, false) => magnitude,
                (true, true) => magnitude.reverse(),
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
            }
        };
        for (p, kp) in &keys {
            for (q, kq) in &keys {
                assert_eq!(kp.cmp(kq), value_order(p, q), "{p:?} vs {q:?}");
            }
        }
    }

    #[test]
    fn number_keys_are_prefix_free_in_both_directions() {
        let mut rng = SplitMix(13);
        let mut values: Vec<Bson> = near_double_pairs(&mut rng, 600)
            .into_iter()
            .flat_map(|(x, y, _)| [x, y])
            .chain(equal_double_pairs(&mut rng, 300).into_iter().flat_map(|(x, y)| [x, y]))
            .collect();
        values.extend((0..113).map(|k| dec(false, 1 << k, 0)));
        values.extend([Bson::Int32(0), Bson::Double(f64::NAN), d("Infinity"), d("-Infinity")]);
        values.extend(["0.1", "1E-6176", "8714930974926918267958656901890887E+5940"].map(d));
        for descending in [false, true] {
            let mut keys: Vec<Vec<u8>> = values
                .iter()
                .map(|v| encode_compound_ordered(&[(v.clone(), descending)]).unwrap())
                .collect();
            keys.sort();
            keys.dedup();
            for pair in keys.windows(2) {
                assert!(
                    !pair[1].starts_with(&pair[0]),
                    "{:02X?} is a prefix of {:02X?}",
                    pair[0],
                    pair[1]
                );
            }
        }
    }

    #[test]
    fn a_descending_number_first_orders_a_compound_key() {
        // The truncated mark inverted (`FF FE`) against the exact one (`FF FF`),
        // with a second component after it whose bytes must not leak into the
        // comparison of the first.
        let mut rng = SplitMix(14);
        let seconds =
            [Bson::Int32(0), Bson::Double(-1.5), Bson::String("a".into()), d("0.1"), Bson::MinKey];
        for (x, y, _) in near_double_pairs(&mut rng, 200) {
            for s in &seconds {
                for t in &seconds {
                    for (d1, d2) in [(true, false), (true, true), (false, false), (false, true)] {
                        let dir = |o: Ordering, desc: bool| if desc { o.reverse() } else { o };
                        let expected = match dir(canonical_cmp(&x, &y), d1) {
                            Ordering::Equal => dir(canonical_cmp(s, t), d2),
                            other => other,
                        };
                        let a =
                            encode_compound_ordered(&[(x.clone(), d1), (s.clone(), d2)]).unwrap();
                        let b =
                            encode_compound_ordered(&[(y.clone(), d1), (t.clone(), d2)]).unwrap();
                        assert_eq!(a.cmp(&b), expected, "({x}, {s}) vs ({y}, {t}), {d1} {d2}");
                    }
                }
            }
        }
    }

    #[test]
    fn a_truncated_mark_inside_arrays_and_documents() {
        // The mark `00 01` followed by an element's `01` or a composite's `00`.
        let mut rng = SplitMix(15);
        let others =
            [Bson::Int32(1), Bson::String("a".into()), Bson::MinKey, Bson::MaxKey, d("-0.1")];
        for (x, y, _) in near_double_pairs(&mut rng, 100) {
            let mut values = Vec::new();
            for n in [&x, &y] {
                values.push(Bson::Array(vec![n.clone()]));
                values.push(Bson::Document(doc! { "a": n.clone() }));
                for other in &others {
                    values.push(Bson::Array(vec![n.clone(), other.clone()]));
                    values.push(Bson::Array(vec![other.clone(), n.clone()]));
                    values.push(Bson::Document(doc! { "a": n.clone(), "b": other.clone() }));
                }
            }
            for a in &values {
                for b in &values {
                    assert_eq!(key(a).cmp(&key(b)), canonical_cmp(a, b), "{a} vs {b}");
                }
            }
        }
    }
}
