//! Filter parsing and evaluation.
//!
//! A filter document is parsed into an AST once and then evaluated, rather than
//! being walked as BSON on every document. The AST is also what the index
//! planner reads to find usable predicates, so parsing is not just an
//! optimisation — it is the shared representation.

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::{Error, Result};
use std::cmp::Ordering;

use crate::path;

/// A parsed filter.
#[derive(Clone, Debug, PartialEq)]
pub enum Filter {
    /// Matches everything. The parse of `{}`.
    AlwaysTrue,
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Nor(Vec<Filter>),
    /// All conditions on one field path must hold.
    Field {
        path: String,
        conditions: Vec<Condition>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Condition {
    /// Imposes no constraint. Produced by a bare `$options`.
    AlwaysTrue,
    /// Both must hold. Only produced when negating a multi-operator `$not`.
    Both(Box<Condition>, Box<Condition>),
    Eq(Bson),
    Ne(Bson),
    Gt(Bson),
    Gte(Bson),
    Lt(Bson),
    Lte(Bson),
    In(Vec<Bson>),
    Nin(Vec<Bson>),
    Exists(bool),
    Type(Vec<String>),
    Regex {
        pattern: String,
        options: String,
    },
    /// Every listed value must be present in the field's array.
    All(Vec<Bson>),
    /// At least one array element must match the inner filter.
    ElemMatch(Box<Filter>),
    Size(i64),
    Not(Box<Condition>),
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parse a filter document into a [`Filter`].
pub fn parse(doc: &Document) -> Result<Filter> {
    let mut clauses = Vec::new();

    for (key, value) in doc {
        if let Some(op) = key.strip_prefix('$') {
            clauses.push(parse_logical(op, value)?);
        } else {
            clauses.push(Filter::Field { path: key.clone(), conditions: parse_conditions(value)? });
        }
    }

    Ok(match clauses.len() {
        0 => Filter::AlwaysTrue,
        1 => clauses.pop().expect("length checked"),
        _ => Filter::And(clauses),
    })
}

fn parse_logical(op: &str, value: &Bson) -> Result<Filter> {
    let branches = |value: &Bson| -> Result<Vec<Filter>> {
        let Bson::Array(items) = value else {
            return Err(Error::InvalidQuery(format!("${op} requires an array")));
        };
        if items.is_empty() {
            return Err(Error::InvalidQuery(format!("${op} requires a non-empty array")));
        }
        items
            .iter()
            .map(|item| match item {
                Bson::Document(d) => parse(d),
                _ => Err(Error::InvalidQuery(format!("${op} entries must be documents"))),
            })
            .collect()
    };

    Ok(match op {
        "and" => Filter::And(branches(value)?),
        "or" => Filter::Or(branches(value)?),
        "nor" => Filter::Nor(branches(value)?),
        // `$not` is only meaningful applied to a field's operators; at the top
        // level Mongo rejects it too, and the clearer error is worth it.
        "not" => {
            return Err(Error::InvalidQuery(
                "$not must be applied to a field, e.g. {field: {$not: {$gt: 5}}}".into(),
            ));
        }
        other => return Err(Error::UnsupportedOperator(format!("${other}"))),
    })
}

/// Decide whether a field's value is an operator document or a literal.
///
/// `{a: {$gt: 1}}` is a comparison; `{a: {b: 1}}` is equality against a nested
/// document. Mongo's rule is that the *first* key decides, and mixing is an
/// error rather than a silent reinterpretation.
fn parse_conditions(value: &Bson) -> Result<Vec<Condition>> {
    let Bson::Document(doc) = value else {
        return Ok(vec![Condition::Eq(value.clone())]);
    };

    let mut keys = doc.keys();
    let Some(first) = keys.next() else {
        // `{}` as a value is an equality match against an empty document.
        return Ok(vec![Condition::Eq(value.clone())]);
    };
    if !first.starts_with('$') {
        return Ok(vec![Condition::Eq(value.clone())]);
    }

    if let Some(plain) = doc.keys().find(|k| !k.starts_with('$')) {
        return Err(Error::InvalidQuery(format!(
            "cannot mix operators and plain fields in one condition (found {plain:?})"
        )));
    }

    // `$options` is a modifier on a sibling `$regex`, not an operator of its
    // own, so it has to be read before the operators are parsed independently.
    let sibling_options = match doc.get("$options") {
        Some(Bson::String(s)) => s.clone(),
        _ => String::new(),
    };

    doc.iter().map(|(key, arg)| parse_condition(&key[1..], arg, &sibling_options)).collect()
}

/// Parse the body of an `$elemMatch`.
///
/// It has two shapes. `{$elemMatch: {qty: {$gt: 5}}}` matches document elements
/// by field, and is an ordinary filter. `{$elemMatch: {$gt: 5}}` matches
/// *scalar* elements directly — the operators apply to the element itself, not
/// to any field of it — which an ordinary parse would misread as a top-level
/// logical operator and reject.
fn parse_elem_match(doc: &Document) -> Result<Filter> {
    const LOGICAL: [&str; 3] = ["$and", "$or", "$nor"];
    let scalar_form =
        !doc.is_empty() && doc.keys().all(|k| k.starts_with('$') && !LOGICAL.contains(&k.as_str()));

    if scalar_form {
        // The empty path is the marker that these conditions target the
        // element itself; see `matches_scalar_against`.
        return Ok(Filter::Field {
            path: String::new(),
            conditions: parse_conditions(&Bson::Document(doc.clone()))?,
        });
    }
    parse(doc)
}

fn parse_condition(op: &str, arg: &Bson, sibling_options: &str) -> Result<Condition> {
    let array_arg = |arg: &Bson| -> Result<Vec<Bson>> {
        match arg {
            Bson::Array(items) => Ok(items.clone()),
            _ => Err(Error::InvalidQuery(format!("${op} requires an array"))),
        }
    };

    Ok(match op {
        "eq" => Condition::Eq(arg.clone()),
        "ne" => Condition::Ne(arg.clone()),
        "gt" => Condition::Gt(arg.clone()),
        "gte" => Condition::Gte(arg.clone()),
        "lt" => Condition::Lt(arg.clone()),
        "lte" => Condition::Lte(arg.clone()),
        "in" => Condition::In(array_arg(arg)?),
        "nin" => Condition::Nin(array_arg(arg)?),
        "all" => Condition::All(array_arg(arg)?),
        "exists" => Condition::Exists(truthy(arg)),
        "size" => match arg {
            Bson::Int32(n) => Condition::Size(i64::from(*n)),
            Bson::Int64(n) => Condition::Size(*n),
            Bson::Double(n) if n.fract() == 0.0 => Condition::Size(*n as i64),
            _ => return Err(Error::InvalidQuery("$size requires an integer".into())),
        },
        "type" => Condition::Type(parse_type_arg(arg)?),
        "regex" => match arg {
            Bson::String(pattern) => {
                Condition::Regex { pattern: pattern.clone(), options: sibling_options.to_string() }
            }
            Bson::RegularExpression(re) => Condition::Regex {
                pattern: re.pattern.as_str().to_string(),
                // Flags written on the literal win over a sibling `$options`.
                options: if re.options.as_str().is_empty() {
                    sibling_options.to_string()
                } else {
                    re.options.as_str().to_string()
                },
            },
            _ => return Err(Error::InvalidQuery("$regex requires a string or regex".into())),
        },
        // Already folded into the sibling `$regex` above. On its own it
        // constrains nothing, matching Mongo's leniency.
        "options" => Condition::AlwaysTrue,
        "elemMatch" => match arg {
            Bson::Document(d) => Condition::ElemMatch(Box::new(parse_elem_match(d)?)),
            _ => return Err(Error::InvalidQuery("$elemMatch requires a document".into())),
        },
        "not" => match arg {
            Bson::Document(d) => {
                // `{$not: {$gt: 1, $lt: 5}}` negates the whole conjunction, so
                // the operators are combined before the negation is applied.
                let combined = parse_conditions(&Bson::Document(d.clone()))?
                    .into_iter()
                    .reduce(|a, b| Condition::Both(Box::new(a), Box::new(b)))
                    .ok_or_else(|| {
                        Error::InvalidQuery("$not requires at least one operator".into())
                    })?;
                Condition::Not(Box::new(combined))
            }
            Bson::RegularExpression(re) => Condition::Not(Box::new(Condition::Regex {
                pattern: re.pattern.as_str().to_string(),
                options: re.options.as_str().to_string(),
            })),
            _ => return Err(Error::InvalidQuery("$not requires a document or regex".into())),
        },
        other => return Err(Error::UnsupportedOperator(format!("${other}"))),
    })
}

fn truthy(value: &Bson) -> bool {
    match value {
        Bson::Boolean(b) => *b,
        Bson::Int32(n) => *n != 0,
        Bson::Int64(n) => *n != 0,
        Bson::Double(n) => *n != 0.0,
        Bson::Null | Bson::Undefined => false,
        _ => true,
    }
}

fn parse_type_arg(arg: &Bson) -> Result<Vec<String>> {
    let one = |value: &Bson| -> Result<String> {
        match value {
            Bson::String(s) => Ok(s.clone()),
            Bson::Int32(n) => type_name_for_code(i64::from(*n)),
            Bson::Int64(n) => type_name_for_code(*n),
            _ => Err(Error::InvalidQuery("$type requires a string alias or type code".into())),
        }
    };
    match arg {
        Bson::Array(items) => items.iter().map(one).collect(),
        other => Ok(vec![one(other)?]),
    }
}

fn type_name_for_code(code: i64) -> Result<String> {
    Ok(match code {
        1 => "double",
        2 => "string",
        3 => "object",
        4 => "array",
        5 => "binData",
        6 => "undefined",
        7 => "objectId",
        8 => "bool",
        9 => "date",
        10 => "null",
        11 => "regex",
        13 => "javascript",
        16 => "int",
        17 => "timestamp",
        18 => "long",
        19 => "decimal",
        -1 => "minKey",
        127 => "maxKey",
        other => return Err(Error::InvalidQuery(format!("unknown $type code {other}"))),
    }
    .to_string())
}

fn type_name_of(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "undefined",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => "javascript",
        Bson::Int32(_) => "int",
        Bson::Timestamp(_) => "timestamp",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
        Bson::Symbol(_) => "symbol",
        Bson::DbPointer(_) => "dbPointer",
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// Test a document against a parsed filter.
pub fn matches(filter: &Filter, doc: &Document) -> bool {
    match filter {
        Filter::AlwaysTrue => true,
        Filter::And(branches) => branches.iter().all(|f| matches(f, doc)),
        Filter::Or(branches) => branches.iter().any(|f| matches(f, doc)),
        Filter::Nor(branches) => !branches.iter().any(|f| matches(f, doc)),
        Filter::Field { path, conditions } => {
            let values = path::resolve(doc, path);
            conditions.iter().all(|c| condition_matches(c, &values))
        }
    }
}

/// Evaluate one condition against the values found at a path.
///
/// `values` is empty when the path is absent, which several operators treat
/// specially.
fn condition_matches(condition: &Condition, values: &[&Bson]) -> bool {
    match condition {
        Condition::Exists(want) => values.is_empty() != *want,

        // `{a: null}` matches an explicit null *and* a missing field, which is
        // the single most surprising Mongo rule to get wrong.
        Condition::Eq(Bson::Null) => {
            values.is_empty() || any_element(values, |v| matches!(v, Bson::Null))
        }

        Condition::Eq(expected) => {
            any_element(values, |v| canonical_cmp(v, expected) == Ordering::Equal)
        }
        Condition::Ne(expected) => !condition_matches(&Condition::Eq(expected.clone()), values),

        Condition::Gt(bound) => compare_any(values, bound, &[Ordering::Greater]),
        Condition::Gte(bound) => compare_any(values, bound, &[Ordering::Greater, Ordering::Equal]),
        Condition::Lt(bound) => compare_any(values, bound, &[Ordering::Less]),
        Condition::Lte(bound) => compare_any(values, bound, &[Ordering::Less, Ordering::Equal]),

        Condition::In(options) => {
            options.iter().any(|option| condition_matches(&Condition::Eq(option.clone()), values))
        }
        Condition::Nin(options) => !condition_matches(&Condition::In(options.clone()), values),

        Condition::Type(names) => {
            any_element(values, |v| names.iter().any(|n| n == type_name_of(v)))
        }

        Condition::Regex { pattern, options } => match compile_regex(pattern, options) {
            Some(re) => any_element(values, |v| match v {
                Bson::String(s) => re.is_match(s),
                _ => false,
            }),
            None => false,
        },

        // Array-shaped operators inspect the array itself rather than its
        // elements, so they must not go through `any_element`.
        Condition::Size(want) => values.iter().any(|v| match v {
            Bson::Array(items) => items.len() as i64 == *want,
            _ => false,
        }),

        Condition::All(required) => values.iter().any(|v| match v {
            Bson::Array(items) => required.iter().all(|needle| {
                items.iter().any(|item| canonical_cmp(item, needle) == Ordering::Equal)
            }),
            // A non-array matches `$all` only for a single-element list.
            other => required.len() == 1 && canonical_cmp(other, &required[0]) == Ordering::Equal,
        }),

        Condition::ElemMatch(inner) => values.iter().any(|v| match v {
            Bson::Array(items) => items.iter().any(|item| match item {
                Bson::Document(d) => matches(inner, d),
                // A scalar element is tested by wrapping it so that
                // `{$elemMatch: {$gt: 5}}` works on an array of numbers.
                scalar => matches_scalar_against(inner, scalar),
            }),
            _ => false,
        }),

        Condition::Not(inner) => !condition_matches(inner, values),

        Condition::AlwaysTrue => true,
        Condition::Both(a, b) => condition_matches(a, values) && condition_matches(b, values),
    }
}

/// Apply a predicate to each value, and — because a field holding an array
/// matches if any *element* matches — to each element as well.
fn any_element(values: &[&Bson], predicate: impl Fn(&Bson) -> bool) -> bool {
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

fn compare_any(values: &[&Bson], bound: &Bson, accept: &[Ordering]) -> bool {
    any_element(values, |v| {
        // Comparisons only apply within a type group; Mongo does not report
        // that a string is greater than a number.
        same_type_group(v, bound) && accept.contains(&canonical_cmp(v, bound))
    })
}

/// Whether two values are comparable, i.e. in the same canonical type group.
fn same_type_group(a: &Bson, b: &Bson) -> bool {
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

/// Evaluate a filter whose conditions target the element itself, used by
/// `$elemMatch` over an array of scalars.
fn matches_scalar_against(filter: &Filter, scalar: &Bson) -> bool {
    match filter {
        Filter::AlwaysTrue => true,
        Filter::And(branches) => branches.iter().all(|f| matches_scalar_against(f, scalar)),
        Filter::Or(branches) => branches.iter().any(|f| matches_scalar_against(f, scalar)),
        Filter::Nor(branches) => !branches.iter().any(|f| matches_scalar_against(f, scalar)),
        Filter::Field { path, conditions } => {
            // A scalar element has no fields, so only an empty path applies.
            if !path.is_empty() {
                return false;
            }
            conditions.iter().all(|c| condition_matches(c, &[scalar]))
        }
    }
}

fn compile_regex(pattern: &str, options: &str) -> Option<regex::Regex> {
    let mut builder = regex::RegexBuilder::new(pattern);
    for flag in options.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'x' => {
                builder.ignore_whitespace(true);
            }
            // Unknown flags are ignored rather than failing the whole query.
            _ => {}
        }
    }
    builder.build().ok()
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    fn hits(query: Document, d: Document) -> bool {
        let filter = parse(&query).unwrap_or_else(|e| panic!("parse failed: {e}"));
        matches(&filter, &d)
    }

    // -----------------------------------------------------------------------
    // Equality and the null/missing rule
    // -----------------------------------------------------------------------

    #[test]
    fn implicit_equality() {
        assert!(hits(doc! { "a": 1 }, doc! { "a": 1 }));
        assert!(!hits(doc! { "a": 1 }, doc! { "a": 2 }));
        assert!(!hits(doc! { "a": 1 }, doc! { "b": 1 }));
    }

    #[test]
    fn an_empty_filter_matches_everything() {
        assert!(hits(doc! {}, doc! {}));
        assert!(hits(doc! {}, doc! { "a": 1 }));
    }

    #[test]
    fn equality_spans_numeric_types() {
        // Storing 5 as an int and querying 5.0 must still match.
        assert!(hits(doc! { "a": 5.0 }, doc! { "a": 5i32 }));
        assert!(hits(doc! { "a": 5i64 }, doc! { "a": 5.0 }));
    }

    #[test]
    fn null_matches_both_explicit_null_and_a_missing_field() {
        // The single most surprising Mongo rule, and the easiest to get wrong.
        assert!(hits(doc! { "a": Bson::Null }, doc! { "a": Bson::Null }));
        assert!(hits(doc! { "a": Bson::Null }, doc! { "b": 1 }));
        assert!(!hits(doc! { "a": Bson::Null }, doc! { "a": 1 }));
    }

    #[test]
    fn exists_distinguishes_null_from_missing() {
        assert!(hits(doc! { "a": { "$exists": true } }, doc! { "a": Bson::Null }));
        assert!(!hits(doc! { "a": { "$exists": true } }, doc! { "b": 1 }));
        assert!(hits(doc! { "a": { "$exists": false } }, doc! { "b": 1 }));
        assert!(!hits(doc! { "a": { "$exists": false } }, doc! { "a": Bson::Null }));
    }

    #[test]
    fn ne_matches_a_missing_field() {
        assert!(hits(doc! { "a": { "$ne": 1 } }, doc! { "b": 1 }));
        assert!(!hits(doc! { "a": { "$ne": 1 } }, doc! { "a": 1 }));
    }

    #[test]
    fn a_nested_document_value_is_an_equality_match_not_an_operator() {
        assert!(hits(doc! { "a": { "b": 1 } }, doc! { "a": { "b": 1 } }));
        // Whole-document equality is order sensitive, as in Mongo.
        assert!(!hits(doc! { "a": { "b": 1 } }, doc! { "a": { "b": 1, "c": 2 } }));
    }

    // -----------------------------------------------------------------------
    // Comparisons
    // -----------------------------------------------------------------------

    #[test]
    fn range_operators() {
        let d = doc! { "n": 5 };
        assert!(hits(doc! { "n": { "$gt": 4 } }, d.clone()));
        assert!(!hits(doc! { "n": { "$gt": 5 } }, d.clone()));
        assert!(hits(doc! { "n": { "$gte": 5 } }, d.clone()));
        assert!(hits(doc! { "n": { "$lt": 6 } }, d.clone()));
        assert!(hits(doc! { "n": { "$lte": 5 } }, d.clone()));
    }

    #[test]
    fn multiple_operators_on_one_field_must_all_hold() {
        let q = doc! { "n": { "$gt": 1, "$lt": 10 } };
        assert!(hits(q.clone(), doc! { "n": 5 }));
        assert!(!hits(q.clone(), doc! { "n": 0 }));
        assert!(!hits(q, doc! { "n": 20 }));
    }

    #[test]
    fn comparisons_do_not_cross_type_groups() {
        // Mongo does not report that a string is greater than a number, even
        // though the canonical sort order places it later.
        assert!(!hits(doc! { "a": { "$gt": 1 } }, doc! { "a": "text" }));
        assert!(!hits(doc! { "a": { "$lt": "m" } }, doc! { "a": 5 }));
        assert!(hits(doc! { "a": { "$gt": "a" } }, doc! { "a": "b" }));
    }

    #[test]
    fn in_and_nin() {
        assert!(hits(doc! { "a": { "$in": [1, 2, 3] } }, doc! { "a": 2 }));
        assert!(!hits(doc! { "a": { "$in": [1, 2, 3] } }, doc! { "a": 9 }));
        assert!(hits(doc! { "a": { "$nin": [1, 2] } }, doc! { "a": 9 }));
        // A missing field is "not in" any list of non-null values.
        assert!(hits(doc! { "a": { "$nin": [1, 2] } }, doc! { "b": 1 }));
    }

    // -----------------------------------------------------------------------
    // Arrays
    // -----------------------------------------------------------------------

    #[test]
    fn equality_matches_any_element_of_an_array() {
        assert!(hits(doc! { "tags": "b" }, doc! { "tags": ["a", "b", "c"] }));
        assert!(!hits(doc! { "tags": "z" }, doc! { "tags": ["a", "b"] }));
    }

    #[test]
    fn equality_also_matches_the_whole_array() {
        assert!(hits(doc! { "tags": ["a", "b"] }, doc! { "tags": ["a", "b"] }));
        assert!(!hits(doc! { "tags": ["b", "a"] }, doc! { "tags": ["a", "b"] }));
    }

    #[test]
    fn comparisons_apply_to_array_elements() {
        assert!(hits(doc! { "n": { "$gt": 8 } }, doc! { "n": [1, 5, 9] }));
        assert!(!hits(doc! { "n": { "$gt": 10 } }, doc! { "n": [1, 5, 9] }));
    }

    #[test]
    fn size_counts_array_elements() {
        assert!(hits(doc! { "a": { "$size": 3 } }, doc! { "a": [1, 2, 3] }));
        assert!(!hits(doc! { "a": { "$size": 2 } }, doc! { "a": [1, 2, 3] }));
        // $size inspects the array itself, so a scalar never matches.
        assert!(!hits(doc! { "a": { "$size": 1 } }, doc! { "a": 1 }));
    }

    #[test]
    fn all_requires_every_listed_value() {
        assert!(hits(doc! { "a": { "$all": [1, 3] } }, doc! { "a": [1, 2, 3] }));
        assert!(!hits(doc! { "a": { "$all": [1, 9] } }, doc! { "a": [1, 2, 3] }));
    }

    #[test]
    fn elem_match_requires_one_element_to_satisfy_everything() {
        let q = doc! { "items": { "$elemMatch": { "qty": { "$gt": 5 }, "sku": "a" } } };
        // One element satisfies both conditions.
        assert!(hits(q.clone(), doc! { "items": [ { "sku": "a", "qty": 9 } ] }));
        // Conditions satisfied, but by *different* elements — must not match.
        assert!(!hits(q, doc! { "items": [ { "sku": "a", "qty": 1 }, { "sku": "b", "qty": 9 } ] }));
    }

    #[test]
    fn elem_match_works_on_arrays_of_scalars() {
        // The operators apply to the element itself rather than to a field of
        // it, so a single element must satisfy the whole range.
        let q = doc! { "n": { "$elemMatch": { "$gt": 5, "$lt": 10 } } };
        assert!(hits(q.clone(), doc! { "n": [1, 7, 20] }));
        // 1 and 20 straddle the range but neither is inside it.
        assert!(!hits(q.clone(), doc! { "n": [1, 20] }));
        assert!(!hits(q, doc! { "n": 7 }));
    }

    #[test]
    fn separate_conditions_may_be_satisfied_by_different_elements() {
        // Without $elemMatch, Mongo allows the split — the contrast with the
        // test above is the whole point of the operator.
        let q = doc! { "items.sku": "a", "items.qty": 9 };
        assert!(hits(q, doc! { "items": [ { "sku": "a", "qty": 1 }, { "sku": "b", "qty": 9 } ] }));
    }

    // -----------------------------------------------------------------------
    // Logical operators
    // -----------------------------------------------------------------------

    #[test]
    fn logical_operators() {
        let d = doc! { "a": 1, "b": 2 };
        assert!(hits(doc! { "$and": [ { "a": 1 }, { "b": 2 } ] }, d.clone()));
        assert!(!hits(doc! { "$and": [ { "a": 1 }, { "b": 9 } ] }, d.clone()));
        assert!(hits(doc! { "$or": [ { "a": 9 }, { "b": 2 } ] }, d.clone()));
        assert!(!hits(doc! { "$or": [ { "a": 9 }, { "b": 9 } ] }, d.clone()));
        assert!(hits(doc! { "$nor": [ { "a": 9 }, { "b": 9 } ] }, d.clone()));
        assert!(!hits(doc! { "$nor": [ { "a": 1 } ] }, d));
    }

    #[test]
    fn top_level_fields_are_implicitly_anded() {
        assert!(hits(doc! { "a": 1, "b": 2 }, doc! { "a": 1, "b": 2 }));
        assert!(!hits(doc! { "a": 1, "b": 2 }, doc! { "a": 1, "b": 3 }));
    }

    #[test]
    fn not_negates_a_field_condition() {
        assert!(hits(doc! { "n": { "$not": { "$gt": 5 } } }, doc! { "n": 1 }));
        assert!(!hits(doc! { "n": { "$not": { "$gt": 5 } } }, doc! { "n": 9 }));
        // $not also matches when the field is absent.
        assert!(hits(doc! { "n": { "$not": { "$gt": 5 } } }, doc! { "other": 1 }));
    }

    #[test]
    fn not_negates_the_whole_conjunction() {
        // NOT (n > 1 AND n < 10): true outside the range, false inside it.
        let q = doc! { "n": { "$not": { "$gt": 1, "$lt": 10 } } };
        assert!(!hits(q.clone(), doc! { "n": 5 }));
        assert!(hits(q.clone(), doc! { "n": 0 }));
        assert!(hits(q, doc! { "n": 50 }));
    }

    // -----------------------------------------------------------------------
    // Types and regex
    // -----------------------------------------------------------------------

    #[test]
    fn type_accepts_aliases_and_numeric_codes() {
        assert!(hits(doc! { "a": { "$type": "string" } }, doc! { "a": "x" }));
        assert!(hits(doc! { "a": { "$type": 2 } }, doc! { "a": "x" }));
        assert!(!hits(doc! { "a": { "$type": "int" } }, doc! { "a": "x" }));
        assert!(hits(doc! { "a": { "$type": ["int", "string"] } }, doc! { "a": 1i32 }));
        // int and long are distinct types even though they compare equal.
        assert!(hits(doc! { "a": { "$type": "long" } }, doc! { "a": 1i64 }));
        assert!(!hits(doc! { "a": { "$type": "int" } }, doc! { "a": 1i64 }));
    }

    #[test]
    fn regex_matches_strings() {
        assert!(hits(doc! { "s": { "$regex": "^ab" } }, doc! { "s": "abc" }));
        assert!(!hits(doc! { "s": { "$regex": "^ab" } }, doc! { "s": "xabc" }));
        // A non-string can never match a regex.
        assert!(!hits(doc! { "s": { "$regex": "1" } }, doc! { "s": 1 }));
    }

    #[test]
    fn regex_honours_sibling_options() {
        // $options is a modifier on $regex, not an operator in its own right;
        // parsing them independently would silently drop the flags.
        assert!(hits(doc! { "s": { "$regex": "^AB", "$options": "i" } }, doc! { "s": "abc" }));
        assert!(!hits(doc! { "s": { "$regex": "^AB" } }, doc! { "s": "abc" }));
    }

    #[test]
    fn an_invalid_regex_matches_nothing_rather_than_erroring() {
        assert!(!hits(doc! { "s": { "$regex": "(unclosed" } }, doc! { "s": "x" }));
    }

    // -----------------------------------------------------------------------
    // Parse errors
    // -----------------------------------------------------------------------

    #[test]
    fn unknown_operators_are_rejected() {
        assert!(parse(&doc! { "a": { "$nope": 1 } }).is_err());
        assert!(parse(&doc! { "$nope": [] }).is_err());
    }

    #[test]
    fn malformed_logical_operators_are_rejected() {
        assert!(parse(&doc! { "$and": 1 }).is_err());
        assert!(parse(&doc! { "$and": [] }).is_err());
        assert!(parse(&doc! { "$or": [1, 2] }).is_err());
    }

    #[test]
    fn mixing_operators_and_fields_is_rejected() {
        // Silently picking one reading would make the query mean something the
        // author did not write.
        assert!(parse(&doc! { "a": { "$gt": 1, "plain": 2 } }).is_err());
    }

    #[test]
    fn top_level_not_is_rejected_with_a_useful_message() {
        let err = parse(&doc! { "$not": { "a": 1 } }).unwrap_err().to_string();
        assert!(err.contains("$not"), "unhelpful error: {err}");
    }

    #[test]
    fn nested_paths_match_through_documents_and_arrays() {
        assert!(hits(doc! { "a.b": 1 }, doc! { "a": { "b": 1 } }));
        assert!(hits(doc! { "a.b": 2 }, doc! { "a": [ { "b": 1 }, { "b": 2 } ] }));
        assert!(hits(doc! { "a.0": 10 }, doc! { "a": [10, 20] }));
    }
}
