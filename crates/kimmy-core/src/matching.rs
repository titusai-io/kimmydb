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

/// `{path: expected}`: the value, or any element of it, equals `expected`.
///
/// `{a: null}` matches an explicit null *and* a missing field, which is the
/// single most surprising Mongo rule to get wrong.
pub fn equals(values: &[&Bson], expected: &Bson) -> bool {
    match expected {
        Bson::Null => values.is_empty() || any_element(values, |v| value_equals(v, expected)),
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
/// string is greater than a number, and neither does this.
pub fn compares(values: &[&Bson], bound: &Bson, accept: &[Ordering]) -> bool {
    any_element(values, |v| value_compares(v, bound, accept))
}

/// Whether one value orders as `accept` says against `bound`: the predicate
/// [`compares`] applies to each value and to each element.
pub fn value_compares(value: &Bson, bound: &Bson, accept: &[Ordering]) -> bool {
    same_type_group(value, bound) && accept.contains(&canonical_cmp(value, bound))
}
