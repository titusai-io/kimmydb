//! The fields an upsert seeds from its filter.
//!
//! One function, so that every caller that builds the document an upsert
//! inserts reads a filter the same way.

use bson::{Bson, Document};

use crate::filter::{Condition, Filter, filter_has_expr};

/// The value a condition on a path pins that path to, if it pins one: an `$eq`,
/// a `$in` of exactly one value, or an `$all` of exactly one value. A `$in` of
/// two, a range, or an `$all` of two pins nothing. (A `$regex` in a `$in` or an
/// `$all`, and an `$elemMatch` in an `$all`, are refused when the filter is
/// parsed, so none reaches here: UNRECORDED whether the reference seeds them.
/// Also UNRECORDED, and seeded as the value given: an array or a document as the
/// single `$all` member, and `null` (`{a: null}`, `$eq: null`).)
fn pinned(condition: &Condition) -> Option<&Bson> {
    match condition {
        Condition::Eq(value) => Some(value),
        Condition::In(values) | Condition::All(values) => match values.as_slice() {
            [only] => Some(only),
            _ => None,
        },
        _ => None,
    }
}

/// Equality constraints a match necessarily satisfies, for seeding an upsert.
///
/// A path counts when a condition on it is an `$eq`, a `$in` of one value or an
/// `$all` of one value (see [`pinned`]), reached through `$and` of any of
/// these, or through a `$or` of exactly one branch, however deeply those nest.
/// An equality inside a `$or` of two or more branches is **not** implied by a
/// match, so seeding from it would invent a field the caller never asked for:
/// the kind of quiet wrongness that is hard to notice in a document that
/// otherwise looks right. Nor is one from `$in` of two values, or from a
/// `$gte` with a `$lte` of one value.
///
/// A dotted key seeds nested documents, and their fields are in name order at
/// each level whatever order the filter wrote its keys in; a whole document
/// given as the value of a key keeps its own order.
///
/// Two of those equalities may not overlap, through any of the forms: the same
/// path twice, or one path inside another (`a` with `a.b`), has no one
/// document to seed, and is refused. `claimed` holds the paths taken so far.
///
/// A filter that holds a `$expr` anywhere refuses the upsert, as the reference
/// does (code 224, q7s2n.out n0035, n0485); within an `$elemMatch` too
/// (UNRECORDED). An `$all` of two or more values is the path matched twice,
/// as two equalities on it would be (code 54, n0027, n0477).
pub fn implied_equalities(
    filter: &Filter,
    out: &mut Document,
    claimed: &mut Vec<String>,
) -> Result<(), String> {
    if filter_has_expr(filter) {
        return Err("$expr is not allowed in the query predicate for an upsert".into());
    }
    seed_from(filter, out, claimed)
}

fn seed_from(filter: &Filter, out: &mut Document, claimed: &mut Vec<String>) -> Result<(), String> {
    match filter {
        Filter::And(branches) => {
            for branch in branches {
                seed_from(branch, out, claimed)?;
            }
        }
        Filter::Or(branches) => {
            if let [only] = &branches[..] {
                seed_from(only, out, claimed)?;
            }
        }
        Filter::Field { path, conditions } => {
            if conditions.iter().any(|c| matches!(c, Condition::All(values) if values.len() > 1)) {
                return Err(format!(
                    "cannot infer the fields an upsert seeds from the filter: path '{path}' is \
                     matched twice"
                ));
            }
            for value in conditions.iter().filter_map(pinned) {
                for taken in claimed.iter() {
                    let inside = |outer: &str, inner: &str| {
                        inner.strip_prefix(outer).is_some_and(|rest| rest.starts_with('.'))
                    };
                    if taken == path {
                        return Err(format!(
                            "cannot infer the fields an upsert seeds from the filter: path \
                             '{path}' is matched twice"
                        ));
                    }
                    if inside(taken, path) || inside(path, taken) {
                        let (long, short) =
                            if taken.len() > path.len() { (taken, path) } else { (path, taken) };
                        return Err(format!(
                            "cannot infer the fields an upsert seeds from the filter: both \
                             paths '{long}' and '{short}' are matched"
                        ));
                    }
                }
                claimed.push(path.clone());
                // Dotted paths go through `path::set` so `{"a.b": 1}`
                // seeds a nested document rather than a literal key.
                let _ = crate::path::set(out, path, value.clone());
                // The levels a dotted key creates are sorted, at every
                // depth; a document given as the value of a key is not.
                crate::path::sort_levels_through(out, path);
            }
        }
        _ => {}
    }
    Ok(())
}
