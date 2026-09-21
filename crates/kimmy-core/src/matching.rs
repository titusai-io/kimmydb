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
        Bson::Null => values.is_empty() || any_element(values, |v| matches!(v, Bson::Null)),
        expected => any_element(values, |v| canonical_cmp(v, expected) == Ordering::Equal),
    }
}

/// `$gt`, `$gte`, `$lt` or `$lte` against `bound`: some value, or element,
/// orders as `accept` says against it.
///
/// Comparisons only apply within a type group; Mongo does not report that a
/// string is greater than a number, and neither does this.
pub fn compares(values: &[&Bson], bound: &Bson, accept: &[Ordering]) -> bool {
    any_element(values, |v| same_type_group(v, bound) && accept.contains(&canonical_cmp(v, bound)))
}
