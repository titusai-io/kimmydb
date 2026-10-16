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
//!
//! **Numbers are exact values.** `Int32`, `Int64`, `Double` and `Decimal128` all
//! denote one exact rational each, and the order is the order of those values:
//! `Int64(1) == Double(1.0) == Decimal128("1.00")`, and `Decimal128("0.1")` is
//! *not* `Double(0.1)`, whose exact value is slightly above one tenth. Exact is
//! the only choice that is transitive, which a sort and an index need.

use std::cmp::Ordering;

use bson::Bson;

use crate::decimal::{self, Decimal};

pub(crate) mod big;
#[cfg(test)]
pub(crate) mod oracle;

use big::Big;

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
/// `Decimal128`, which this representation cannot hold exactly: a decimal is
/// read through [`decimal::decode`] instead, and compared by its own arms of
/// [`cmp_numbers`].
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

/// A number as the comparator reads it: a binary type decomposed, a decimal
/// decoded.
#[derive(Clone, Copy, Debug)]
enum Number {
    Binary(Numeric),
    Decimal(Decimal),
}

impl Number {
    fn of(value: &Bson) -> Option<Self> {
        match value {
            Bson::Decimal128(d) => Some(Number::Decimal(decimal::decode(d))),
            other => decompose(other).map(Number::Binary),
        }
    }

    /// The classes of [`Numeric::class_rank`], in its order: a decimal NaN of
    /// any sign or payload is the NaN class, a decimal zero of any spelling is
    /// zero, and so on.
    fn class_rank(self) -> u8 {
        match self {
            Number::Binary(n) => n.class_rank(),
            Number::Decimal(d) => match d {
                Decimal::Nan => 0,
                Decimal::NegInfinity => 1,
                Decimal::Finite { negative: true, .. } => 2,
                Decimal::Zero => 3,
                Decimal::Finite { negative: false, .. } => 4,
                Decimal::PosInfinity => 5,
            },
        }
    }
}

/// The class rank of the negative finite values, where a larger magnitude is a
/// smaller value.
const NEGATIVE_CLASS: u8 = 2;

fn cmp_numbers(a: &Bson, b: &Bson) -> Ordering {
    let (Some(x), Some(y)) = (Number::of(a), Number::of(b)) else {
        // Unreachable: only the number rank comes here, and every number reads.
        return Ordering::Equal;
    };
    let class = x.class_rank();
    if class != y.class_rank() {
        return class.cmp(&y.class_rank());
    }
    let magnitude = match (x, y) {
        (Number::Binary(x), Number::Binary(y)) => return cmp_numeric(x, y),
        (
            Number::Decimal(Decimal::Finite { coefficient: c, exponent: e, .. }),
            Number::Decimal(Decimal::Finite { coefficient: d, exponent: f, .. }),
        ) => cmp_decimals(c, e, d, f),
        (
            Number::Decimal(Decimal::Finite { coefficient, exponent, .. }),
            Number::Binary(Numeric::Finite { exp, mantissa, .. }),
        ) => cmp_decimal_binary(coefficient, exponent, exp, mantissa),
        (
            Number::Binary(Numeric::Finite { exp, mantissa, .. }),
            Number::Decimal(Decimal::Finite { coefficient, exponent, .. }),
        ) => cmp_decimal_binary(coefficient, exponent, exp, mantissa).reverse(),
        // One class and not finite: both NaN, both the same infinity, or both
        // zero.
        _ => return Ordering::Equal,
    };
    if class == NEGATIVE_CLASS { magnitude.reverse() } else { magnitude }
}

/// `10^0 ..= 10^34`.
const POW10: [u128; 35] = {
    let mut table = [1u128; 35];
    let mut i = 1;
    while i < table.len() {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
};

/// The number of decimal digits of a coefficient, `1 <= c < 10^34`.
fn digits(c: u128) -> usize {
    POW10.iter().position(|&power| c < power).expect("a coefficient is below 10^34")
}

/// The magnitudes `c · 10^e` and `d · 10^f` (nonzero coefficients), compared
/// without a big integer.
///
/// A coefficient of `n` digits puts `c · 10^e` in `[10^(n+e−1), 10^(n+e))`, so
/// the adjusted exponent `n + e` orders any two values that differ in it. When
/// it is equal, both coefficients scaled to 34 digits are on one scale; each is
/// below `10^34 < 2^113`, so a `u128` holds it. Every cohort member (`1.0`,
/// `1.00`, `10E-1`) scales to the same pair, which is what makes them one value.
fn cmp_decimals(c: u128, e: i16, d: u128, f: i16) -> Ordering {
    let (n, m) = (digits(c), digits(d));
    let adjusted = |digits: usize, exponent: i16| digits as i32 + i32::from(exponent);
    adjusted(n, e).cmp(&adjusted(m, f)).then_with(|| (c * POW10[34 - n]).cmp(&(d * POW10[34 - m])))
}

/// `log2 10` in fixed point with 64 fractional bits, rounded down. The number
/// is irrational, so its true value lies strictly between this and the next
/// integer, which is `LOG2_10_HI`.
const LOG2_10_LO: i128 = 61_278_757_397_652_712_441;
const LOG2_10_HI: i128 = LOG2_10_LO + 1;

/// Integers `lo <= ⌊log2(c · 10^e)⌋ <= hi`, `hi − lo <= 2`, from fixed-point
/// arithmetic alone.
///
/// `⌊log2 c⌋` is exact (the leading zeros). `e · log2 10` lies between the
/// products with the two constants, and **which product is the lower bound
/// depends on the sign of `e`**: for a negative `e`, the larger constant gives
/// the smaller product. The floor of a negative product must be a true floor,
/// toward minus infinity; a truncation toward zero raises the lower bound by
/// one for every negative exponent. The arithmetic shift of an `i128` is that
/// floor.
fn log2_interval(c: u128, e: i16) -> (i32, i32) {
    let log2_c = 127 - c.leading_zeros() as i32;
    let e = i128::from(e);
    let (low, high) =
        if e >= 0 { (e * LOG2_10_LO, e * LOG2_10_HI) } else { (e * LOG2_10_HI, e * LOG2_10_LO) };
    // `log2 x` is in `[log2_c + low, log2_c + 1 + high)`.
    let floor = (low >> 64) as i32;
    let ceiling = ((high + (1 << 64) - 1) >> 64) as i32;
    (log2_c + floor, log2_c + ceiling)
}

/// The magnitude `c · 10^e` (a decimal, `c` nonzero) against
/// `mantissa · 2^(exp − 63)` (a binary value, mantissa normalised).
///
/// The binary side's `⌊log2⌋` is `exp`, exactly. Where the decimal's interval
/// does not reach it, the intervals answer: every decimal at `e >= 309` (at
/// least `10^309 > Double::MAX`) or `e <= -358` (at most `(10^34 − 1) · 10^-358
/// < 2^-1074`) ends here, as does almost every ordinary comparison. Only an
/// overlap is compared exactly.
fn cmp_decimal_binary(c: u128, e: i16, exp: i16, mantissa: u64) -> Ordering {
    let (lo, hi) = log2_interval(c, e);
    let exp = i32::from(exp);
    if hi < exp {
        return Ordering::Less;
    }
    if lo > exp {
        return Ordering::Greater;
    }
    cmp_decimal_binary_exactly(c, e, mantissa, exp - 63)
}

/// `c · 10^e` against `m · 2^f`, exactly.
///
/// With `p = max(e, 0)`, `q = max(−e, 0)`, `s = max(f, 0)`, `t = max(−f, 0)`,
/// multiplying both sides by `10^q · 2^t` makes both integers:
/// `c · 5^p · 2^(p+t)` against `m · 5^q · 2^(q+s)`. Only the difference of the
/// two powers of two is applied. Reached only on an overlap of the intervals,
/// so `|e| <= 357`: at most `5^357`, about 830 bits, beside a shift of at most
/// 1,137 bits (a subnormal double).
fn cmp_decimal_binary_exactly(c: u128, e: i16, m: u64, f: i32) -> Ordering {
    let e = i32::from(e);
    let (p, q) = (e.max(0) as u32, (-e).max(0) as u32);
    let (s, t) = (f.max(0) as u32, (-f).max(0) as u32);
    let mut decimal = Big::from_u128(c);
    decimal.mul_pow5(p);
    let mut binary = Big::from_u128(u128::from(m));
    binary.mul_pow5(q);
    let (twos_decimal, twos_binary) = (p + t, q + s);
    if twos_decimal > twos_binary {
        decimal.shl(twos_decimal - twos_binary);
    } else {
        binary.shl(twos_binary - twos_decimal);
    }
    decimal.cmp(&binary)
}

/// Whether two values are comparable, i.e. in the same canonical type group.
///
/// `canonical_cmp` orders every value against every other, which is what a
/// sort and an index need. A filter's `$gt` does not: a string is not greater
/// than a number, it is not comparable with one. [`crate::matching`] asks
/// this before it compares.
pub(crate) fn same_type_group(a: &Bson, b: &Bson) -> bool {
    fn group(v: &Bson) -> u8 {
        match v {
            Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => 1,
            Bson::String(_) | Bson::Symbol(_) => 2,
            Bson::Document(_) => 3,
            Bson::Array(_) => 4,
            Bson::Binary(_) => 5,
            Bson::ObjectId(_) => 6,
            Bson::Boolean(_) => 7,
            Bson::DateTime(_) => 8,
            Bson::Timestamp(_) => 9,
            Bson::Null | Bson::Undefined => 10,
            _ => 11,
        }
    }
    group(a) == group(b)
}

/// Whether a value is, or contains, a `Decimal128`.
///
/// A decimal used to be the one BSON type this order could not place (it
/// ranked equal to every other number, and [`crate::keyenc`] refused it), so
/// anything that compared values on a caller's behalf — a filter operand, a
/// sort key, a partial index's bound — asked this first and refused by name.
/// The order now places a decimal exactly and the keyer accepts it; this stays
/// for those callers until their refusals are lifted.
/// Every arm that [`canonical_cmp`] descends into is answered here, whether
/// or not the JSON edge can currently produce it: a scoped JavaScript value
/// is compared by its scope through `cmp_documents`, so it is searched too.
/// The edge has no `$code` decoder today — it writes the wrapper and does
/// not read it — but this whole unit exists because an edge that emitted a
/// wrapper it did not read became silent data loss, and a `$code` decoder
/// added later must not reopen that.
pub fn holds_decimal128(value: &Bson) -> bool {
    match value {
        Bson::Decimal128(_) => true,
        Bson::Array(items) => items.iter().any(holds_decimal128),
        Bson::Document(doc) => doc.values().any(holds_decimal128),
        Bson::JavaScriptCodeWithScope(code) => code.scope.values().any(holds_decimal128),
        _ => false,
    }
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

#[cfg(test)]
mod decimal128 {
    use super::oracle::{
        any_coefficient, any_double, any_exponent, any_number, dec, decimal_near_double,
        oracle_cmp, raw,
    };
    use super::*;
    use crate::decimal::{MAX_COEFFICIENT, MAX_EXPONENT, MIN_EXPONENT};
    use bson::{Decimal128, doc};
    use num_bigint::BigInt;
    use proptest::prelude::*;

    fn d(text: &str) -> Bson {
        Bson::Decimal128(text.parse().unwrap())
    }

    /// Assert `a` against `b` in both directions, and that the exact-rational
    /// oracle agrees, so a wrong expectation written here fails too.
    fn check(a: &Bson, b: &Bson, expected: Ordering) {
        assert_eq!(canonical_cmp(a, b), expected, "{a:?} ({a}) vs {b:?} ({b})");
        assert_eq!(canonical_cmp(b, a), expected.reverse(), "{b:?} vs {a:?}");
        assert_eq!(oracle_cmp(a, b), expected, "the oracle disagrees: {a} vs {b}");
    }

    fn lt(a: Bson, b: Bson) {
        check(&a, &b, Ordering::Less);
    }

    fn eq(a: Bson, b: Bson) {
        check(&a, &b, Ordering::Equal);
    }

    /// The double with these bits.
    fn bits(b: u64) -> Bson {
        Bson::Double(f64::from_bits(b))
    }

    #[test]
    fn decimals_are_in_the_number_group() {
        // `$gt` and friends ask this before they compare; a decimal that left
        // the group would match nothing.
        for n in [Bson::Int32(1), Bson::Int64(1), Bson::Double(1.0), d("1")] {
            assert!(same_type_group(&d("1.5"), &n), "{n:?}");
            assert_eq!(type_rank(&d("1.5")), type_rank(&n));
        }
        assert!(!same_type_group(&d("1.5"), &Bson::String("1.5".into())));
        assert!(decompose(&d("1.5")).is_none(), "a decimal is read by its own decoder");
    }

    #[test]
    fn cohort_members_are_one_value() {
        eq(d("1.0"), d("1.00"));
        eq(d("1.00"), d("10E-1"));
        eq(d("1.0"), Bson::Int32(1));
        eq(d("-1.00"), Bson::Double(-1.0));
        eq(d("1E+3"), d("1000"));
        // Every `(c · 10^k, e − k)`: a coefficient of `n` digits has `34 − n`
        // more spellings of the same value.
        for c in [1u128, 7, 12, 999, 123_456_789, 10u128.pow(20) + 1] {
            for e in [MIN_EXPONENT + 40, -400, -20, -1, 0, 1, 20, 300, MAX_EXPONENT] {
                let members: Vec<(u128, i16)> = (0..=(34 - digits(c)) as u32)
                    .map(|k| (c * 10u128.pow(k), e - k as i16))
                    .collect();
                assert_eq!(members.last().map(|m| digits(m.0)), Some(34), "up to 34 digits");
                for negative in [false, true] {
                    for &(x, f) in &members {
                        for &(y, g) in &members {
                            let (a, b) = (dec(negative, x, f), dec(negative, y, g));
                            assert_eq!(canonical_cmp(&a, &b), Ordering::Equal, "{a} vs {b}");
                        }
                    }
                }
            }
        }
    }

    /// Every spelling of zero: either sign, any exponent, a coefficient above
    /// `10^34 − 1`, and the `11` large-coefficient form with assorted tails.
    fn zeros() -> Vec<Bson> {
        let mut out = vec![d("0"), d("-0"), d("0E-100"), d("0E+6111"), d("-0E-6176"), d("0.000")];
        for biased in [0u128, 6176, 12287] {
            // (`2^112` itself is below `10^34`: canonical, and not a zero.)
            for c in [MAX_COEFFICIENT + 1, (1u128 << 113) - 1, MAX_COEFFICIENT + 0x1234_5678_9ABC] {
                out.push(raw((biased << 113) | c));
                out.push(raw((1 << 127) | (biased << 113) | c));
            }
        }
        for top in 0b11000u128..=0b11101 {
            for tail in [0u128, 1, 0xDEAD_BEEF, (1u128 << 122) - 1] {
                out.push(raw((top << 122) | tail));
                out.push(raw((1 << 127) | (top << 122) | tail));
            }
        }
        out
    }

    #[test]
    fn every_zero_is_zero() {
        let zero_forms = zeros();
        for z in &zero_forms {
            for other in &zero_forms {
                eq(z.clone(), other.clone());
            }
            eq(z.clone(), Bson::Int32(0));
            eq(z.clone(), Bson::Double(-0.0));
            lt(z.clone(), dec(false, 1, MIN_EXPONENT));
            lt(dec(true, 1, MIN_EXPONENT), z.clone());
            lt(z.clone(), Bson::Double(5e-324));
        }
    }

    #[test]
    fn nan_and_infinity_ignore_their_payloads() {
        let mut nans = vec![d("NaN"), d("-NaN"), Bson::Double(f64::NAN)];
        let mut infinities = vec![d("Infinity"), Bson::Double(f64::INFINITY)];
        let mut negative_infinities = vec![d("-Infinity"), Bson::Double(f64::NEG_INFINITY)];
        for payload in [0u128, 1, 0xFFFF, (1 << 100) | 7, (1u128 << 121) - 1] {
            let nan = (0b11111u128 << 122) | payload;
            // Quiet and signalling (bit 121), either sign.
            nans.extend([raw(nan), raw(nan | (1 << 121)), raw(nan | (1 << 127))]);
            let inf = (0b11110u128 << 122) | payload;
            infinities.push(raw(inf));
            negative_infinities.push(raw(inf | (1 << 127)));
        }
        for nan in &nans {
            for other in &nans {
                eq(nan.clone(), other.clone());
            }
            for inf in &negative_infinities {
                lt(nan.clone(), inf.clone());
            }
        }
        for inf in &infinities {
            for other in &infinities {
                eq(inf.clone(), other.clone());
            }
            lt(dec(false, MAX_COEFFICIENT, MAX_EXPONENT), inf.clone());
            lt(Bson::Double(f64::MAX), inf.clone());
        }
        for inf in &negative_infinities {
            lt(inf.clone(), dec(true, MAX_COEFFICIENT, MAX_EXPONENT));
            lt(inf.clone(), Bson::Int64(i64::MIN));
        }
    }

    #[test]
    fn the_extremes_against_the_binary_extremes() {
        let tiny = d("1E-6176");
        let huge = d("9.999999999999999999999999999999999E+6144");
        lt(Bson::Int32(0), tiny.clone());
        lt(tiny.clone(), Bson::Double(5e-324));
        lt(tiny.clone(), Bson::Double(f64::MIN_POSITIVE));
        lt(Bson::Double(-5e-324), d("-1E-6176"));
        lt(d("-1E-6176"), Bson::Double(-0.0));
        lt(Bson::Double(f64::MAX), huge.clone());
        lt(Bson::Int64(i64::MAX), huge.clone());
        lt(d("-9.999999999999999999999999999999999E+6144"), Bson::Double(f64::MIN));
        lt(d("-9.999999999999999999999999999999999E+6144"), Bson::Int64(i64::MIN));
        // The edges of where a decimal can meet a double at all.
        lt(d("9.999999999999999999999999999999999E-325"), Bson::Double(5e-324));
        lt(Bson::Double(f64::MAX), d("1E+309"));
        lt(Bson::Double(f64::MAX), d("1.797693134862315708145274237317044E+308"));
        lt(d("1.797693134862315708145274237317043E+308"), Bson::Double(f64::MAX));
    }

    #[test]
    fn one_tenth_is_not_the_double_one_tenth() {
        let tenth = d("0.1");
        // 0.1 as a double is 0.1000000000000000055511151231257827021181583404541015625.
        lt(tenth.clone(), Bson::Double(0.1));
        lt(bits(0.1f64.to_bits() - 1), tenth.clone());
        lt(d("0.1000000000000000055511151231257827"), Bson::Double(0.1));
        lt(Bson::Double(0.1), d("0.1000000000000000055511151231257828"));
        eq(tenth.clone(), d("0.10"));
        lt(d("0.09999999999999999999999999999999999"), tenth);
    }

    #[test]
    fn a_decimal_equal_to_a_double_or_an_integer_is_equal() {
        eq(d("0.5"), Bson::Double(0.5));
        eq(d("0.125"), Bson::Double(0.125));
        eq(d("-0.125"), Bson::Double(-0.125));
        // 2^-48 = 5^48 · 10^-48: 34 digits, the smallest such power a decimal holds.
        eq(d("3.552713678800500929355621337890625E-15"), Bson::Double(2f64.powi(-48)));
        lt(d("3.552713678800500929355621337890624E-15"), Bson::Double(2f64.powi(-48)));
        // Above 2^53, where a double no longer counts by one.
        eq(d("9007199254740993"), Bson::Int64((1 << 53) + 1));
        lt(Bson::Double(9_007_199_254_740_992.0), d("9007199254740993"));
        eq(d("9007199254740992"), Bson::Double(9_007_199_254_740_992.0));
        eq(d("9223372036854775807"), Bson::Int64(i64::MAX));
        eq(d("9.223372036854775807E+18"), Bson::Int64(i64::MAX));
        eq(d("-9223372036854775808"), Bson::Int64(i64::MIN));
        eq(d("9223372036854775808"), Bson::Double(9_223_372_036_854_775_808.0));
        lt(Bson::Int64(i64::MAX), d("9223372036854775808"));
        lt(d("9223372036854775806.5"), Bson::Int64(i64::MAX));
        // 2^70 = 1180591620717411303424, and 2^80 · 10^20 above 2^113.
        eq(d("1180591620717411303424"), Bson::Double(2f64.powi(70)));
        eq(d("1208925819614629174706176E+20"), Bson::Double(2f64.powi(100) * 5f64.powi(20)));
    }

    #[test]
    fn negative_exponents_sit_on_the_right_side_of_the_interval() {
        // The bound for `e < 0` takes the other constant and a true floor. Each
        // power of ten here is beside the powers of two around it, and beside
        // doubles in its own binade, where only the exact step can answer.
        lt(Bson::Double(0.0625), d("0.1"));
        lt(d("0.1"), Bson::Double(0.125));
        lt(Bson::Double(0.0078125), d("0.01"));
        lt(d("0.01"), Bson::Double(0.015625));
        lt(Bson::Double(2f64.powi(-10)), d("1E-3"));
        lt(d("1E-3"), Bson::Double(2f64.powi(-9)));
        lt(d("1E-3"), Bson::Double(0.0015));
        lt(Bson::Double(0.00099), d("1E-3"));
        lt(d("1000"), Bson::Int32(1024));
        lt(Bson::Double(999.5), d("1000"));
        lt(d("1000"), Bson::Double(1000.5));
        // Every power of ten from 10^-30 to 10^30 against the doubles either side
        // of it and the powers of two around it, for a few coefficients.
        for e in -30i16..=30 {
            for c in [1u128, 9, 99, 10u128.pow(33), MAX_COEFFICIENT] {
                let x = dec(false, c, e);
                let nearest: f64 = x.to_string().parse().unwrap();
                for b in [nearest.to_bits() - 1, nearest.to_bits(), nearest.to_bits() + 1] {
                    for sign in [false, true] {
                        let y = bits(b | (u64::from(sign) << 63));
                        let x = dec(sign, c, e);
                        check(&x, &y, oracle_cmp(&x, &y));
                    }
                }
                let k = nearest.log2().floor() as i32;
                for p in [k - 1, k, k + 1, k + 2] {
                    let y = Bson::Double(2f64.powi(p));
                    check(&x, &y, oracle_cmp(&x, &y));
                }
            }
        }
    }

    /// `⌊log2(c · 10^e)⌋`, exactly.
    fn floor_log2(c: u128, e: i64) -> i32 {
        let ten = super::oracle::pow10(e.unsigned_abs() as u32);
        let (num, den) =
            if e >= 0 { (BigInt::from(c) * ten, BigInt::from(1)) } else { (BigInt::from(c), ten) };
        let mut l = num.bits() as i64 - den.bits() as i64;
        let at_least = |l: i64| {
            if l >= 0 { num >= (&den << l as usize) } else { (&num << (-l) as usize) >= den }
        };
        while !at_least(l) {
            l -= 1;
        }
        while at_least(l + 1) {
            l += 1;
        }
        l as i32
    }

    #[test]
    fn the_interval_holds_the_true_exponent_for_every_exponent() {
        // Exhaustive over the exponent: the interval must contain the truth and
        // be at most two wide. A truncation toward zero for a negative product
        // puts `lo` one above the truth; a widened bound fails the width.
        let mut low_end_hit = 0;
        for c in [1u128, 2, 9, 10, 1 << 64, (1 << 64) - 1, 10u128.pow(33), MAX_COEFFICIENT] {
            for e in MIN_EXPONENT..=MAX_EXPONENT {
                let (lo, hi) = log2_interval(c, e);
                let truth = floor_log2(c, i64::from(e));
                assert!(lo <= truth && truth <= hi, "{c}E{e}: {truth} outside [{lo}, {hi}]");
                assert!(hi - lo <= 2, "{c}E{e}: [{lo}, {hi}] is too wide");
                low_end_hit += usize::from(e < 0 && lo == truth);
            }
        }
        assert!(low_end_hit > 1000, "the lower end is reached often: {low_end_hit}");
    }

    #[test]
    fn decimals_order_by_value_not_by_spelling() {
        lt(d("9.99"), d("10"));
        lt(d("99E-1"), d("1E+1"));
        lt(d("-10"), d("-9.99"));
        lt(d("1E-6176"), d("2E-6176"));
        lt(d("9999999999999999999999999999999999"), d("1E+34"));
        eq(d("1E+34"), d("1000000000000000000000000000000000E+1"));
        lt(d("9.999999999999999999999999999999999E+6144"), d("Infinity"));
        lt(d("1.000000000000000000000000000000001"), d("1.000000000000000000000000000000002"));
        lt(d("1"), d("1.000000000000000000000000000000001"));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(3000))]

        /// The comparator against exact rationals, over every kind of number.
        #[test]
        fn decimals_order_as_their_exact_values(a in any_number(), b in any_number()) {
            prop_assert_eq!(canonical_cmp(&a, &b), oracle_cmp(&a, &b), "{} vs {}", a, b);
        }

        /// Pairs on the boundaries: equal exponents with off-by-one coefficients,
        /// cohort spellings, and a decimal against the double it is nearest.
        #[test]
        fn neighbours_order_as_their_exact_values(
            negative in any::<bool>(),
            c in any_coefficient(),
            e in any_exponent(),
            step in -2i64..=2,
            double in any_double(),
            offset in -2i64..=2,
        ) {
            let x = dec(negative, c, e);
            if let Ok(next) = u128::try_from(c as i128 + i128::from(step))
                && (1..=MAX_COEFFICIENT).contains(&next)
            {
                let y = dec(negative, next, e);
                prop_assert_eq!(canonical_cmp(&x, &y), oracle_cmp(&x, &y));
            }
            if c * 10 <= MAX_COEFFICIENT && e > MIN_EXPONENT {
                prop_assert_eq!(canonical_cmp(&x, &dec(negative, c * 10, e - 1)), Ordering::Equal);
            }
            if let Some(near) = decimal_near_double(double, offset) {
                let y = Bson::Double(double);
                prop_assert_eq!(canonical_cmp(&near, &y), oracle_cmp(&near, &y), "{} vs {}", near, double);
                prop_assert_eq!(canonical_cmp(&near, &x), oracle_cmp(&near, &x));
            }
        }

        #[test]
        fn ordering_with_decimals_is_transitive(a in any_number(), b in any_number(), c in any_number()) {
            let (ab, bc, ac) = (canonical_cmp(&a, &b), canonical_cmp(&b, &c), canonical_cmp(&a, &c));
            if ab != Ordering::Greater && bc != Ordering::Greater {
                prop_assert_ne!(ac, Ordering::Greater, "{} {} {}", a, b, c);
            }
            if ab == Ordering::Equal && bc == Ordering::Equal {
                prop_assert_eq!(ac, Ordering::Equal);
            }
        }

        #[test]
        fn ordering_with_decimals_is_antisymmetric(a in any_number(), b in any_number()) {
            prop_assert_eq!(canonical_cmp(&a, &b), canonical_cmp(&b, &a).reverse());
        }
    }

    /// Release-mode comparator timings, printed. Run with
    /// `cargo test -p kimmy-core --release --lib cmp::decimal128::timings -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn timings() {
        use std::hint::black_box;
        use std::time::Instant;

        let time = |label: &str, a: Bson, b: Bson| {
            let n = 200_000;
            let started = Instant::now();
            for _ in 0..n {
                black_box(canonical_cmp(black_box(&a), black_box(&b)));
            }
            println!("{label}: {:?} per comparison", started.elapsed() / n);
        };
        time("decimal vs decimal, one adjusted exponent", d("1.2345"), d("1.2346"));
        time("decimal vs int, apart", d("19.99"), Bson::Int32(5));
        time("decimal vs double, overlapping", d("0.1"), Bson::Double(0.1));
        time(
            "decimal vs double, e = -357 against 5e-324",
            d("4.940656458412465441765687928682213E-324"),
            Bson::Double(5e-324),
        );
        time(
            "decimal vs double, e = 308 against MAX",
            d("1.797693134862315708145274237317043E+308"),
            Bson::Double(f64::MAX),
        );
        time("decimal vs double, extreme and apart", d("1E-6176"), Bson::Double(5e-324));
    }

    #[test]
    fn a_decimal128_is_found_wherever_it_is_nested() {
        // What every comparing caller asks before it compares: the value
        // itself, an element, a field, and a field of an element all count,
        // because the order compares documents and arrays by their contents.
        let d = Bson::Decimal128("1.5".parse::<Decimal128>().unwrap());
        assert!(holds_decimal128(&d));
        assert!(holds_decimal128(&Bson::Array(vec![Bson::Int32(1), d.clone()])));
        assert!(holds_decimal128(&Bson::Document(doc! { "a": { "b": [d.clone()] } })));
        assert!(!holds_decimal128(&Bson::Double(1.5)));
        assert!(!holds_decimal128(&Bson::Document(doc! { "a": [1, "1.5", 1.5] })));

        // A scoped JavaScript value is compared by its scope, so it is
        // searched by its scope: the edge cannot produce one today, and a
        // `$code` decoder added later must not make this a way through.
        let scoped = |scope| {
            Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
                code: "return 1".into(),
                scope,
            })
        };
        assert!(holds_decimal128(&scoped(doc! { "n": d })));
        assert!(!holds_decimal128(&scoped(doc! { "n": 1.5 })));
    }
}
