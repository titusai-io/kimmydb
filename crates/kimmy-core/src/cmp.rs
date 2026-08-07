//! Canonical BSON value ordering.
//!
//! This is the semantic definition of "less than" for KimmyDB: it drives
//! `$gt`/`$lt`, `sort`, and index range bounds. [`crate::keyenc`] produces a
//! byte encoding that must sort identically, and the two are checked against
//! each other by property tests. Keeping them as separate implementations is
//! deliberate — the encoder is bit manipulation, and bit manipulation needs an
//! oracle.
//!
//! The ordering follows MongoDB's cross-type comparison order, so that queries
//! written against Mongo behave the same way here.

use std::cmp::Ordering;

use bson::Bson;

/// Cross-type sort rank. Values of different ranks never compare by content.
///
/// Gaps are intentional: they leave room to slot in a type later without
/// renumbering, and they mirror the tag values in [`crate::keyenc`].
fn type_rank(value: &Bson) -> u8 {
    match value {
        Bson::MinKey => 0x01,
        // Mongo treats a missing field and an explicit null as equivalent for
        // ordering, and sorts undefined alongside null.
        Bson::Null | Bson::Undefined => 0x10,
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => 0x20,
        Bson::String(_) | Bson::Symbol(_) => 0x30,
        Bson::Document(_) => 0x40,
        Bson::Array(_) => 0x50,
        Bson::Binary(_) => 0x60,
        Bson::ObjectId(_) => 0x70,
        Bson::Boolean(_) => 0x80,
        Bson::DateTime(_) => 0x90,
        Bson::Timestamp(_) => 0xA0,
        Bson::RegularExpression(_) => 0xB0,
        Bson::DbPointer(_) => 0xC0,
        Bson::JavaScriptCode(_) => 0xD0,
        Bson::JavaScriptCodeWithScope(_) => 0xE0,
        Bson::MaxKey => 0xF0,
    }
}

/// Compare two BSON values in canonical order.
pub fn canonical_cmp(a: &Bson, b: &Bson) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }

    match (a, b) {
        (Bson::MinKey, _) | (Bson::MaxKey, _) => Ordering::Equal,
        (Bson::Null | Bson::Undefined, _) => Ordering::Equal,

        // Numbers compare by mathematical value regardless of storage type, so
        // that `{n: {$gt: 5}}` matches a document storing `n` as a double.
        _ if ra == 0x20 => cmp_numbers(a, b),

        (Bson::String(x) | Bson::Symbol(x), Bson::String(y) | Bson::Symbol(y)) => x.cmp(y),

        (Bson::Document(x), Bson::Document(y)) => cmp_documents(x, y),

        (Bson::Array(x), Bson::Array(y)) => {
            for (ex, ey) in x.iter().zip(y.iter()) {
                match canonical_cmp(ex, ey) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            x.len().cmp(&y.len())
        }

        // Mongo orders binary by length, then subtype, then contents.
        (Bson::Binary(x), Bson::Binary(y)) => x
            .bytes
            .len()
            .cmp(&y.bytes.len())
            .then_with(|| u8::from(x.subtype).cmp(&u8::from(y.subtype)))
            .then_with(|| x.bytes.cmp(&y.bytes)),

        (Bson::ObjectId(x), Bson::ObjectId(y)) => x.bytes().cmp(&y.bytes()),
        (Bson::Boolean(x), Bson::Boolean(y)) => x.cmp(y),
        (Bson::DateTime(x), Bson::DateTime(y)) => x.timestamp_millis().cmp(&y.timestamp_millis()),
        (Bson::Timestamp(x), Bson::Timestamp(y)) => {
            x.time.cmp(&y.time).then_with(|| x.increment.cmp(&y.increment))
        }
        (Bson::RegularExpression(x), Bson::RegularExpression(y)) => {
            x.pattern.cmp(&y.pattern).then_with(|| x.options.cmp(&y.options))
        }
        (Bson::DbPointer(_), Bson::DbPointer(_)) => Ordering::Equal,
        (Bson::JavaScriptCode(x), Bson::JavaScriptCode(y)) => x.cmp(y),
        (Bson::JavaScriptCodeWithScope(x), Bson::JavaScriptCodeWithScope(y)) => {
            x.code.cmp(&y.code).then_with(|| cmp_documents(&x.scope, &y.scope))
        }

        // Unreachable: equal ranks are exhausted above.
        _ => Ordering::Equal,
    }
}

/// Documents compare field by field, key before value, then by field count.
fn cmp_documents(x: &bson::Document, y: &bson::Document) -> Ordering {
    for ((kx, vx), (ky, vy)) in x.iter().zip(y.iter()) {
        match kx.cmp(ky) {
            Ordering::Equal => {}
            other => return other,
        }
        match canonical_cmp(vx, vy) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    x.len().cmp(&y.len())
}

/// A number decomposed into an exact sign/magnitude form.
///
/// Every finite `i32`, `i64`, and `f64` is exactly `mantissa * 2^(exp - 63)`
/// with the mantissa normalized so its high bit is set. Reducing all numeric
/// types to this one representation is what makes cross-type comparison exact
/// rather than "exact until you exceed 2^53".
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Numeric {
    Nan,
    NegInfinity,
    /// `negative` carries the sign; magnitude is `mantissa * 2^(exp - 63)`.
    Finite {
        negative: bool,
        exp: i16,
        mantissa: u64,
    },
    Zero,
    PosInfinity,
}

impl Numeric {
    /// Rank used to order the special cases against each other and against
    /// finite values. NaN sorts below every number, matching Mongo.
    fn class_rank(self) -> u8 {
        match self {
            Numeric::Nan => 0,
            Numeric::NegInfinity => 1,
            Numeric::Finite { negative: true, .. } => 2,
            Numeric::Zero => 3,
            Numeric::Finite { negative: false, .. } => 4,
            Numeric::PosInfinity => 5,
        }
    }
}

/// Decompose a BSON number. Returns `None` for non-numeric values and for
/// `Decimal128`, which this representation cannot hold exactly.
pub(crate) fn decompose(value: &Bson) -> Option<Numeric> {
    match value {
        Bson::Int32(v) => Some(from_i64(i64::from(*v))),
        Bson::Int64(v) => Some(from_i64(*v)),
        Bson::Double(v) => Some(from_f64(*v)),
        _ => None,
    }
}

pub(crate) fn from_i64(v: i64) -> Numeric {
    if v == 0 {
        return Numeric::Zero;
    }
    // Negating i64::MIN overflows, so take the magnitude in u64 space.
    let magnitude = v.unsigned_abs();
    let shift = magnitude.leading_zeros();
    Numeric::Finite {
        negative: v < 0,
        // value = magnitude * 2^0, renormalized to mantissa * 2^(exp - 63)
        exp: 63 - shift as i16,
        mantissa: magnitude << shift,
    }
}

pub(crate) fn from_f64(v: f64) -> Numeric {
    if v.is_nan() {
        return Numeric::Nan;
    }
    if v == f64::INFINITY {
        return Numeric::PosInfinity;
    }
    if v == f64::NEG_INFINITY {
        return Numeric::NegInfinity;
    }
    if v == 0.0 {
        // Treats -0.0 as 0.0, which is what numeric comparison requires.
        return Numeric::Zero;
    }

    let bits = v.to_bits();
    let negative = bits >> 63 == 1;
    let exponent_field = ((bits >> 52) & 0x7FF) as i32;
    let fraction = bits & ((1u64 << 52) - 1);

    // Normal values carry an implicit leading 1; subnormals do not.
    let (significand, power) = if exponent_field == 0 {
        (fraction, -1074i32)
    } else {
        ((1u64 << 52) | fraction, exponent_field - 1075)
    };

    let shift = significand.leading_zeros();
    Numeric::Finite {
        negative,
        // value = significand * 2^power, renormalized to mantissa * 2^(exp - 63)
        exp: (power - shift as i32 + 63) as i16,
        mantissa: significand << shift,
    }
}

fn cmp_numbers(a: &Bson, b: &Bson) -> Ordering {
    // Decimal128 has no exact representation here. Rather than silently
    // mis-order it, fall back to comparing the raw values as equal-ranked and
    // let the caller's recheck sort it out; encoding rejects it outright.
    let (Some(na), Some(nb)) = (decompose(a), decompose(b)) else {
        return Ordering::Equal;
    };
    cmp_numeric(na, nb)
}

pub(crate) fn cmp_numeric(a: Numeric, b: Numeric) -> Ordering {
    let (ca, cb) = (a.class_rank(), b.class_rank());
    if ca != cb {
        return ca.cmp(&cb);
    }
    match (a, b) {
        (
            Numeric::Finite { negative, exp: ea, mantissa: ma },
            Numeric::Finite { exp: eb, mantissa: mb, .. },
        ) => {
            let magnitude = ea.cmp(&eb).then_with(|| ma.cmp(&mb));
            // For negatives, a larger magnitude means a smaller value.
            if negative { magnitude.reverse() } else { magnitude }
        }
        _ => Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use bson::{Bson, doc};

    use super::*;

    fn lt(a: Bson, b: Bson) {
        assert_eq!(canonical_cmp(&a, &b), Ordering::Less, "expected {a:?} < {b:?}");
        assert_eq!(canonical_cmp(&b, &a), Ordering::Greater, "expected {b:?} > {a:?}");
    }

    fn eq(a: Bson, b: Bson) {
        assert_eq!(canonical_cmp(&a, &b), Ordering::Equal, "expected {a:?} == {b:?}");
    }

    #[test]
    fn types_order_canonically() {
        lt(Bson::MinKey, Bson::Null);
        lt(Bson::Null, Bson::Int32(0));
        lt(Bson::Int32(0), Bson::String("a".into()));
        lt(Bson::String("a".into()), Bson::Document(doc! {}));
        lt(Bson::Document(doc! {}), Bson::Array(vec![]));
        lt(Bson::Array(vec![]), Bson::Boolean(false));
        lt(Bson::Boolean(true), Bson::DateTime(bson::DateTime::from_millis(0)));
        lt(Bson::DateTime(bson::DateTime::from_millis(0)), Bson::MaxKey);
    }

    #[test]
    fn numbers_compare_across_types() {
        eq(Bson::Int32(5), Bson::Int64(5));
        eq(Bson::Int64(5), Bson::Double(5.0));
        lt(Bson::Int32(5), Bson::Double(5.5));
        lt(Bson::Double(-1.5), Bson::Int32(-1));
        lt(Bson::Int64(-10), Bson::Int64(-5));
    }

    #[test]
    fn large_integers_compare_exactly() {
        // Both round to the same f64. Comparing via f64 would call them equal,
        // which would corrupt an index range scan over large ids.
        let a = Bson::Int64(i64::MAX);
        let b = Bson::Int64(i64::MAX - 1);
        lt(b.clone(), a.clone());
        assert_ne!(canonical_cmp(&a, &b), Ordering::Equal);

        // 2^53 and 2^53 + 1 are the classic pair f64 cannot distinguish.
        lt(Bson::Int64(9_007_199_254_740_992), Bson::Int64(9_007_199_254_740_993));
    }

    #[test]
    fn i64_min_does_not_overflow() {
        lt(Bson::Int64(i64::MIN), Bson::Int64(i64::MIN + 1));
        lt(Bson::Int64(i64::MIN), Bson::Int64(0));
    }

    #[test]
    fn special_doubles_sort_at_the_extremes() {
        lt(Bson::Double(f64::NAN), Bson::Double(f64::NEG_INFINITY));
        lt(Bson::Double(f64::NEG_INFINITY), Bson::Int64(i64::MIN));
        lt(Bson::Int64(i64::MAX), Bson::Double(f64::INFINITY));
        eq(Bson::Double(0.0), Bson::Double(-0.0));
    }

    #[test]
    fn subnormal_doubles_order_correctly() {
        lt(Bson::Double(0.0), Bson::Double(f64::MIN_POSITIVE));
        lt(Bson::Double(5e-324), Bson::Double(1e-323));
        lt(Bson::Double(-1e-323), Bson::Double(-5e-324));
    }

    #[test]
    fn arrays_compare_elementwise_then_by_length() {
        lt(Bson::Array(vec![Bson::Int32(1)]), Bson::Array(vec![Bson::Int32(1), Bson::Int32(0)]));
        lt(Bson::Array(vec![Bson::Int32(1)]), Bson::Array(vec![Bson::Int32(2)]));
    }

    #[test]
    fn documents_compare_key_before_value() {
        lt(Bson::Document(doc! { "a": 99 }), Bson::Document(doc! { "b": 1 }));
        lt(Bson::Document(doc! { "a": 1 }), Bson::Document(doc! { "a": 2 }));
    }

    /// An oracle for numeric comparison that shares no code with
    /// [`decompose`].
    ///
    /// This matters because `keyenc` encodes numbers *through* `decompose`, so
    /// the encoder and `canonical_cmp` are not independent for numeric values —
    /// a bug in the decomposition would corrupt both identically and the
    /// cross-checking property test would happily pass. This oracle compares
    /// integers as integers and only ever converts in the direction that cannot
    /// lose information.
    fn oracle_cmp(a: &Bson, b: &Bson) -> Ordering {
        fn as_i64(v: &Bson) -> Option<i64> {
            match v {
                Bson::Int32(x) => Some(i64::from(*x)),
                Bson::Int64(x) => Some(*x),
                _ => None,
            }
        }
        fn as_f64(v: &Bson) -> Option<f64> {
            match v {
                Bson::Double(x) => Some(*x),
                _ => None,
            }
        }

        // Exact: no conversion at all.
        if let (Some(x), Some(y)) = (as_i64(a), as_i64(b)) {
            return x.cmp(&y);
        }
        if let (Some(x), Some(y)) = (as_f64(a), as_f64(b)) {
            return match (x.is_nan(), y.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                // Handles -0.0 == 0.0 via IEEE equality.
                (false, false) => x.partial_cmp(&y).expect("neither is NaN"),
            };
        }

        // Mixed integer and double. Compare exactly by splitting the double
        // into its floor and a fractional remainder, never rounding the
        // integer.
        let (int, float, flipped) = match (as_i64(a), as_f64(b)) {
            (Some(i), Some(f)) => (i, f, false),
            _ => (as_i64(b).unwrap(), as_f64(a).unwrap(), true),
        };

        let result = if float.is_nan() {
            Ordering::Greater
        } else if float == f64::INFINITY {
            Ordering::Less
        } else if float == f64::NEG_INFINITY {
            Ordering::Greater
        } else {
            let floor = float.floor();
            // 2^63 is the first f64 above the i64 range.
            const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
            if floor >= TWO_POW_63 {
                Ordering::Less
            } else if floor < -TWO_POW_63 {
                Ordering::Greater
            } else {
                // `floor` is now exactly representable as i64.
                match int.cmp(&(floor as i64)) {
                    // Equal integer parts: a fractional remainder tips it.
                    Ordering::Equal if float > floor => Ordering::Less,
                    other => other,
                }
            }
        };

        if flipped { result.reverse() } else { result }
    }

    #[test]
    fn binary_orders_by_length_first() {
        let short = Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![0xFF],
        });
        let long = Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![0x00, 0x00],
        });
        lt(short, long);
    }

    mod props {
        use proptest::prelude::*;

        use super::*;

        fn any_number() -> impl Strategy<Value = Bson> {
            prop_oneof![
                any::<i32>().prop_map(Bson::Int32),
                any::<i64>().prop_map(Bson::Int64),
                any::<f64>().prop_map(Bson::Double),
                // Boundaries where naive implementations break.
                prop_oneof![
                    Just(0.0f64),
                    Just(-0.0f64),
                    Just(f64::NAN),
                    Just(f64::INFINITY),
                    Just(f64::NEG_INFINITY),
                    Just(f64::MIN_POSITIVE),
                    Just(5e-324f64),
                    Just(9_007_199_254_740_992.0f64),
                    Just(9_223_372_036_854_775_808.0f64),
                    Just(-9_223_372_036_854_775_808.0f64),
                ]
                .prop_map(Bson::Double),
                prop_oneof![
                    Just(i64::MIN),
                    Just(i64::MAX),
                    Just(0i64),
                    Just(9_007_199_254_740_992i64),
                    Just(9_007_199_254_740_993i64),
                ]
                .prop_map(Bson::Int64),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(4000))]

            /// Cross-check the mantissa/exponent decomposition against an
            /// oracle that shares none of its code.
            #[test]
            fn numeric_order_matches_an_independent_oracle(
                a in any_number(),
                b in any_number(),
            ) {
                prop_assert_eq!(
                    canonical_cmp(&a, &b),
                    oracle_cmp(&a, &b),
                    "disagreement on {:?} vs {:?}", a, b
                );
            }

            /// Ordering must be a total order, or sorts and range scans built
            /// on it are undefined.
            #[test]
            fn ordering_is_transitive(a in any_number(), b in any_number(), c in any_number()) {
                let (ab, bc, ac) = (
                    canonical_cmp(&a, &b),
                    canonical_cmp(&b, &c),
                    canonical_cmp(&a, &c),
                );
                if ab == Ordering::Less && bc == Ordering::Less {
                    prop_assert_eq!(ac, Ordering::Less);
                }
                if ab == Ordering::Equal && bc == Ordering::Equal {
                    prop_assert_eq!(ac, Ordering::Equal);
                }
            }

            #[test]
            fn ordering_is_antisymmetric(a in any_number(), b in any_number()) {
                prop_assert_eq!(canonical_cmp(&a, &b), canonical_cmp(&b, &a).reverse());
            }
        }
    }
}
