//! How a filter operator holds on the values a path resolves to, as `find`
//! evaluates it.
//!
//! Lives in `kimmy-core` so that there is one definition. `kimmy-query`'s
//! filter evaluates these operators through it, and `kimmy-storage` needs the
//! same answer without depending on `kimmy-query`: TTL expiry re-checks an
//! index's partial filter with it before deleting (ADR-181). The same move
//! `keyenc` and [`crate::cmp`] made, for the same reason — two definitions of
//! what one expression selects is how the partial-filter defects happened.
//!
//! `values` is what [`crate::path::resolve`] returns: empty when the path is
//! absent.

use std::cmp::Ordering;

use bson::Bson;

use crate::cmp::{canonical_cmp, same_type_group};

/// Apply a predicate to each value, and — because a field holding an array
/// matches if any *element* matches — to each element as well.
pub fn any_element(values: &[&Bson], predicate: impl Fn(&Bson) -> bool) -> bool {
    values.iter().any(|value| {
        if predicate(value) {
            return true;
        }
        match value {
            Bson::Array(items) => items.iter().any(&predicate),
            _ => false,
        }
    })
}

/// `{path: {$exists: want}}`.
pub fn exists(values: &[&Bson], want: bool) -> bool {
    values.is_empty() != want
}

/// Whether the path holds no value for the document, which a comparison against `null` reads as
/// a `null`: `{a: null}` holds for it, and so does a `MinKey` or `MaxKey` bound of a range, and a
/// `$gte` or `$lte` of `null`. The one place that says so, asked by [`equals`] and by
/// [`compares`], so that what a missing field is to a comparison is decided here and nowhere
/// else.
pub fn reads_as_null(values: &[&Bson]) -> bool {
    values.is_empty()
}

/// `{path: expected}`: the value, or any element of it, equals `expected`.
///
/// `{a: null}` matches an explicit null *and* a missing field, which is the
/// single most surprising Mongo rule to get wrong.
pub fn equals(values: &[&Bson], expected: &Bson) -> bool {
    match expected {
        Bson::Null => reads_as_null(values) || any_element(values, |v| value_equals(v, expected)),
        expected => any_element(values, |v| value_equals(v, expected)),
    }
}

/// Whether one value equals `expected`, with no look into it if it is an
/// array: the predicate [`equals`] applies to each value and to each element.
pub fn value_equals(value: &Bson, expected: &Bson) -> bool {
    match expected {
        Bson::Null => matches!(value, Bson::Null),
        expected => canonical_cmp(value, expected) == Ordering::Equal,
    }
}

/// `$gt`, `$gte`, `$lt` or `$lte` against `bound`: some value, or element,
/// orders as `accept` says against it.
///
/// Comparisons only apply within a type group; Mongo does not report that a
/// string is greater than a number, and neither does this. A `MinKey` or `MaxKey` bound is an
/// exception: it is ordered against every type. A missing field reads as `null` against it, and
/// against a `null` bound: `{$gt: MinKey}` holds for every document but the `MinKey` ones, a
/// missing field included, and `{$gte: null}` and `{$lte: null}` hold for a `null`, a `[null]`
/// and a missing field (q7s3a0168, a0238, q7s3b0075 and b0077), where `$gt` and `$lt` of `null`
/// hold for nothing.
pub fn compares(values: &[&Bson], bound: &Bson, accept: &[Ordering]) -> bool {
    if reads_as_null(values) {
        return (is_end_key(bound) || matches!(bound, Bson::Null))
            && value_compares(&Bson::Null, bound, accept);
    }
    any_element(values, |v| value_compares(v, bound, accept))
}

/// Whether one value orders as `accept` says against `bound`: the predicate
/// [`compares`] applies to each value and to each element.
///
/// A NaN has no order against a number: `{$lt: 5}` and `{$gt: -Infinity}` hold for no
/// NaN, and `{$gt: NaN}` for no number. It is equal to a NaN, so `$gte` and `$lte` of NaN
/// hold for the NaNs, a double's and a decimal's (q7s3a0124, a0159, a0222 and the rest of
/// the sweep over 50 values, q7s3a.out:4812 to 11907). This is the answer of a filter only:
/// a sort and an index place NaN below every number, by [`canonical_cmp`], and nothing here
/// changes that order.
///
/// A `MinKey` or `MaxKey` bound is ordered against a value of every type, `null`, a regular
/// expression and an array included (q7s3a0147, a0182, a0218 and a0253, q7s3a.out:6077, 8002,
/// 9982 and 11907, and q7s3f0132 to f0145): `{$gt: MinKey}` holds for all but a `MinKey`,
/// `{$lte: MaxKey}` for all, and `{$lt: MinKey}` and `{$gt: MaxKey}` for none. Other bounds are
/// ordered only within their own type group, where `MinKey` and `MaxKey` once shared one with
/// the regular expressions and the code types, so that `{$gt: MinKey}` held for a regular
/// expression and a `MaxKey` and for nothing else.
pub fn value_compares(value: &Bson, bound: &Bson, accept: &[Ordering]) -> bool {
    if is_end_key(bound) {
        return accept.contains(&canonical_cmp(value, bound));
    }
    match (is_nan(value), is_nan(bound)) {
        (false, false) => {
            same_type_group(value, bound) && accept.contains(&canonical_cmp(value, bound))
        }
        (true, true) => accept.contains(&Ordering::Equal),
        _ => false,
    }
}

/// Whether a value is `MinKey` or `MaxKey`.
pub fn is_end_key(value: &Bson) -> bool {
    matches!(value, Bson::MinKey | Bson::MaxKey)
}

/// Whether a value is a NaN, a double's or a decimal's.
pub fn is_nan(value: &Bson) -> bool {
    match value {
        Bson::Double(d) => d.is_nan(),
        Bson::Decimal128(d) => crate::decimal::decode(d) == crate::decimal::Decimal::Nan,
        _ => false,
    }
}
