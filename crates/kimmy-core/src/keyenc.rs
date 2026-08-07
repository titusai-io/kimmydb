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

use bson::Bson;

use crate::cmp::{Numeric, decompose};
use crate::error::{Error, Result};

// Type tags. These match the ranks in `cmp::type_rank` so that cross-type
// ordering falls out of the leading byte alone.
const TAG_MIN_KEY: u8 = 0x01;
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

// Element framing inside documents and arrays. A present element leads with
// `01` and the end of the composite is `00`, so a prefix sorts before a longer
// sequence.
const ELEMENT_PRESENT: u8 = 0x01;
const ELEMENT_END: u8 = 0x00;

/// Encode a value into `out`.
///
/// Fails only for `Decimal128`, which has no exact representation in the
/// mantissa/exponent form used for numbers. Encoding it approximately would
/// silently mis-order values, so it is rejected instead.
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

        Bson::Decimal128(_) => {
            return Err(Error::UnsupportedOperator(
                "Decimal128 cannot be used as an index key or _id".to_string(),
            ));
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

/// Encode the exact sign/exponent/mantissa form.
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
            out.push(if negative { NUM_NEGATIVE } else { NUM_POSITIVE });
            // Bias the exponent so it encodes as an unsigned big-endian value
            // that sorts in numeric order.
            let biased = (exp as i32 + 32_768) as u16;
            let mut body = [0u8; 10];
            body[..2].copy_from_slice(&biased.to_be_bytes());
            body[2..].copy_from_slice(&mantissa.to_be_bytes());
            if negative {
                // Larger magnitude means a smaller value, so invert.
                for byte in &mut body {
                    *byte = !*byte;
                }
            }
            out.extend_from_slice(&body);
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
    for &byte in bytes {
        if byte == 0x00 {
            out.extend_from_slice(&ESCAPED_NUL);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&ESCAPE_TERMINATOR);
}

#[cfg(test)]
mod tests {
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
    fn decimal128_is_rejected_rather_than_mis_encoded() {
        let d: bson::Decimal128 = "1.5".parse().unwrap();
        assert!(
            encode(&Bson::Decimal128(d)).is_err(),
            "approximate encoding would silently mis-order values"
        );
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
        /// limits, NUL bytes, and empty composites.
        fn any_bson() -> impl Strategy<Value = Bson> {
            let leaf = prop_oneof![
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
                prop::collection::vec(any::<u8>(), 0..4).prop_map(|bytes| Bson::Binary(
                    bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes }
                )),
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
