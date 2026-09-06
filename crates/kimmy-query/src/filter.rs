//! Filter parsing and evaluation.
//!
//! A filter document is parsed into an AST once and then evaluated, rather than
//! being walked as BSON on every document. The AST is also what the index
//! planner reads to find usable predicates, so parsing is not just an
//! optimisation — it is the shared representation.

use bson::{Bson, Document};
use kimmy_core::cmp::{canonical_cmp, holds_decimal128};
use kimmy_core::{Error, Result};
use std::cmp::Ordering;

use crate::expr::{self, Expr};
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
    /// `{$expr: <expression>}` — an aggregation expression evaluated against
    /// the whole document, matching when the result is truthy.
    ///
    /// This is the one clause that can compare two fields of the same
    /// document, and it borrows the expression language wholesale rather than
    /// growing a field-reference syntax of its own (ADR-106). The planner
    /// never reads it: an expression names no field it could put bounds on.
    Expr(Box<Expr>),
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
    /// `{$mod: [divisor, remainder]}` — the value, truncated to an integer,
    /// leaves `remainder` when divided by `divisor`. Numeric values only.
    Mod {
        divisor: i64,
        remainder: i64,
    },
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
        // The expression parser reports its own errors — an unknown operator,
        // a wrong arity — in the same error type as the rest of this module,
        // so a caller sees one kind of `400` whichever half of the filter was
        // malformed.
        "expr" => Filter::Expr(Box::new(Expr::parse(value)?)),
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
    let equality = |value: &Bson| -> Result<Vec<Condition>> {
        comparable("equality", value)?;
        Ok(vec![Condition::Eq(value.clone())])
    };
    let Bson::Document(doc) = value else {
        return equality(value);
    };

    let mut keys = doc.keys();
    let Some(first) = keys.next() else {
        // `{}` as a value is an equality match against an empty document.
        return equality(value);
    };
    if !first.starts_with('$') {
        return equality(value);
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
///
/// `$expr` is on the document side of that split: it reads the element's
/// fields, so `{$elemMatch: {$expr: {$gt: ["$qty", "$min"]}}}` compares two
/// fields of one element. MongoDB refuses `$expr` under `$elemMatch` outright;
/// accepting it here is a strict superset, noted in `docs/deviations.md`.
fn parse_elem_match(doc: &Document) -> Result<Filter> {
    const DOCUMENT_LEVEL: [&str; 4] = ["$and", "$or", "$nor", "$expr"];
    let scalar_form = !doc.is_empty()
        && doc.keys().all(|k| k.starts_with('$') && !DOCUMENT_LEVEL.contains(&k.as_str()));

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

    // Every operator that compares its operand against the field refuses a
    // Decimal128 in it, for the reason on `comparable`; the ones below this
    // block ask about shape or type and never compare.
    if matches!(op, "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "in" | "nin" | "all") {
        comparable(&format!("${op}"), arg)?;
    }

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
        "mod" => parse_mod(arg)?,
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

/// Refuse a `Decimal128` anywhere in a comparison operand.
///
/// `canonical_cmp` ranks a Decimal128 equal to every other number — it has no
/// exact representation there, and the key encoder refuses it outright
/// (ADR-005) — so an operand holding one would match every numeric value of
/// the field and could never be bounded by an index. The refusal lands at
/// parse, where the caller can read it; matching everything would say
/// nothing. The check is recursive because equality against a document or
/// an array compares their contents by the same order.
fn comparable(what: &str, operand: &Bson) -> Result<()> {
    if holds_decimal128(operand) {
        return Err(Error::InvalidQuery(format!(
            "{what} operand holds a Decimal128, which cannot be compared in a filter: it has no \
             exact key encoding in this engine and ranks equal to every other number, so the \
             match would be neither exact nor indexable; compare a double or a long instead"
        )));
    }
    Ok(())
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

/// `{$mod: [divisor, remainder]}`, exactly two numbers.
///
/// A double operand is truncated toward zero, as MongoDB does — `{$mod: [4.5,
/// 0]}` is `{$mod: [4, 0]}` — but one with no integer at all (NaN, infinity,
/// beyond 2^63) is refused rather than guessed at. A zero divisor is refused
/// here rather than matching nothing per document, because a filter that can
/// never match is a mistake the caller wants to hear about.
fn parse_mod(arg: &Bson) -> Result<Condition> {
    let Bson::Array(items) = arg else {
        return Err(Error::InvalidQuery("$mod requires an array of [divisor, remainder]".into()));
    };
    if items.len() != 2 {
        return Err(Error::InvalidQuery(format!(
            "$mod requires exactly two elements, [divisor, remainder], found {}",
            items.len()
        )));
    }
    let divisor = mod_operand(&items[0], "divisor")?;
    let remainder = mod_operand(&items[1], "remainder")?;
    if divisor == 0 {
        return Err(Error::InvalidQuery("$mod divisor cannot be 0".into()));
    }
    Ok(Condition::Mod { divisor, remainder })
}

fn mod_operand(value: &Bson, what: &str) -> Result<i64> {
    match value {
        Bson::Int32(n) => Ok(i64::from(*n)),
        Bson::Int64(n) => Ok(*n),
        Bson::Double(d) => truncate_to_i64(*d).ok_or_else(|| {
            Error::InvalidQuery(format!(
                "$mod {what} must be representable as a 64-bit integer, found {d}"
            ))
        }),
        other => Err(Error::InvalidQuery(format!(
            "$mod {what} must be a number, found {}",
            type_name_of(other)
        ))),
    }
}

/// A double truncated toward zero, when that is an `i64`.
fn truncate_to_i64(d: f64) -> Option<i64> {
    // `as` saturates, so the range is checked first: 2^63 is exactly
    // representable as a double and is one past the largest i64.
    let t = d.trunc();
    (d.is_finite() && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&t))
        .then_some(t as i64)
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
        Filter::Expr(e) => expr_matches(e, doc),
    }
}

/// Evaluate an `$expr` clause against a document.
///
/// Truthiness is the expression language's — `false`, `null`, `0` and a
/// missing field are false, everything else including `""` and `[]` is true —
/// because the value came out of that language and `$cond` already reads it
/// this way.
///
/// **A type violation is "no match", not an error.** `{$expr: {$gt: [{$add:
/// ["$name", 1]}, 0]}}` on a document whose `name` is a string is a
/// document-dependent failure that only shows up mid-scan, and this function
/// answers a `bool` for every caller — the scan, `$elemMatch`, the executor's
/// residual check. Failing the whole request from here would have to thread a
/// `Result` through all of them for a case the regex arm already resolves the
/// other way: an unusable pattern matches nothing. Recorded in
/// `docs/deviations.md`, because MongoDB does fail the query.
fn expr_matches(e: &Expr, doc: &Document) -> bool {
    e.eval(doc).is_ok_and(|v| expr::truthy(&v))
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

        // Element-wise like the comparisons, and numeric only: a string never
        // has a remainder. The value is truncated to an integer first, so 8.5
        // satisfies `[4, 0]`, and the remainder keeps the dividend's sign —
        // `-7` satisfies `[3, -1]`, not `[3, 2]` — both as MongoDB does.
        Condition::Mod { divisor, remainder } => any_element(values, |v| {
            let n = match v {
                Bson::Int32(n) => i64::from(*n),
                Bson::Int64(n) => *n,
                Bson::Double(d) => match truncate_to_i64(*d) {
                    Some(n) => n,
                    None => return false,
                },
                _ => return false,
            };
            // `i64::MIN % -1` overflows; its remainder is zero.
            n.checked_rem(*divisor).unwrap_or(0) == *remainder
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

/// Test one array element against a filter, for the update language's
/// `$[<identifier>]` segments (`update::parse_with_filters`).
///
/// The filter arrives with the identifier prefix already removed, so a
/// condition on the empty path is a condition on the element itself —
/// `{"line": {$gt: 5}}` became `{"": {$gt: 5}}` — and a condition on any other
/// path reads a field of a document element. A scalar element has no fields,
/// so a dotted condition sees an absent path there, exactly as a document
/// without the field would: `$exists: false` holds, `$gt` does not.
pub fn matches_element(filter: &Filter, element: &Bson) -> bool {
    match filter {
        Filter::AlwaysTrue => true,
        Filter::And(branches) => branches.iter().all(|f| matches_element(f, element)),
        Filter::Or(branches) => branches.iter().any(|f| matches_element(f, element)),
        Filter::Nor(branches) => !branches.iter().any(|f| matches_element(f, element)),
        Filter::Field { path, conditions } => {
            if path.is_empty() {
                return conditions.iter().all(|c| condition_matches(c, &[element]));
            }
            let values = match element {
                Bson::Document(doc) => path::resolve(doc, path),
                _ => Vec::new(),
            };
            conditions.iter().all(|c| condition_matches(c, &values))
        }
        // Unreachable through `arrayFilters` today: `update::strip_identifier`
        // refuses every `$`-operator but `$and`/`$or`/`$nor`, so no `$expr`
        // survives parsing into a filter that lands here. Answered rather than
        // left to `unreachable!` because the restriction is one parser's rule,
        // not a property of this function: against a document element the
        // expression evaluates over the element — `$$ROOT` there is the
        // element, as it is under a document-form `$elemMatch` — and a scalar
        // element offers neither a field to read nor a document to name, so it
        // does not match, which is what `matches_scalar_against` answers too.
        Filter::Expr(e) => match element {
            Bson::Document(doc) => expr_matches(e, doc),
            _ => false,
        },
    }
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
        // An expression reads fields, and a scalar has none to read.
        Filter::Expr(_) => false,
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

    // -----------------------------------------------------------------------
    // $mod
    // -----------------------------------------------------------------------

    #[test]
    fn mod_matches_a_remainder() {
        assert!(hits(doc! { "n": { "$mod": [4, 0] } }, doc! { "n": 8 }));
        assert!(!hits(doc! { "n": { "$mod": [4, 0] } }, doc! { "n": 9 }));
        assert!(hits(doc! { "n": { "$mod": [4, 1] } }, doc! { "n": 9i64 }));
        assert!(hits(doc! { "n": { "$mod": [4i64, 1i64] } }, doc! { "n": 9 }));
    }

    #[test]
    fn mod_truncates_doubles_toward_zero_on_both_sides() {
        // 8.5 becomes 8, and a double operand becomes its integer part.
        assert!(hits(doc! { "n": { "$mod": [4, 0] } }, doc! { "n": 8.5 }));
        assert!(hits(doc! { "n": { "$mod": [4.9, 0.7] } }, doc! { "n": 8 }));
        assert!(hits(doc! { "n": { "$mod": [4, -3] } }, doc! { "n": -7.9 }));
    }

    #[test]
    fn mod_remainder_keeps_the_dividends_sign() {
        // -7 = 3 * -2 + -1, as in C and MongoDB, not -7 = 3 * -3 + 2.
        assert!(hits(doc! { "n": { "$mod": [3, -1] } }, doc! { "n": -7 }));
        assert!(!hits(doc! { "n": { "$mod": [3, 2] } }, doc! { "n": -7 }));
        // A negative divisor leaves the sign with the dividend too.
        assert!(hits(doc! { "n": { "$mod": [-3, 1] } }, doc! { "n": 7 }));
    }

    #[test]
    fn mod_never_matches_a_non_number() {
        assert!(!hits(doc! { "n": { "$mod": [4, 0] } }, doc! { "n": "8" }));
        assert!(!hits(doc! { "n": { "$mod": [4, 0] } }, doc! { "n": Bson::Null }));
        assert!(!hits(doc! { "n": { "$mod": [4, 0] } }, doc! { "other": 8 }));
        assert!(!hits(doc! { "n": { "$mod": [4, 0] } }, doc! { "n": f64::NAN }));
        // But `$not` inverts it, so a non-number then matches.
        assert!(hits(doc! { "n": { "$not": { "$mod": [4, 0] } } }, doc! { "n": "8" }));
    }

    #[test]
    fn mod_applies_to_array_elements() {
        // Element-wise, like the comparison operators.
        assert!(hits(doc! { "n": { "$mod": [5, 0] } }, doc! { "n": [1, 10, 3] }));
        assert!(!hits(doc! { "n": { "$mod": [5, 0] } }, doc! { "n": [1, 11, 3] }));
        assert!(hits(doc! { "n": { "$mod": [5, 0] } }, doc! { "n": [1, "x", 15] }));
    }

    #[test]
    fn mod_does_not_overflow_on_the_smallest_integer() {
        assert!(hits(doc! { "n": { "$mod": [-1, 0] } }, doc! { "n": i64::MIN }));
    }

    #[test]
    fn mod_rejects_a_zero_divisor_and_a_malformed_argument() {
        let zero = parse(&doc! { "n": { "$mod": [0, 1] } }).unwrap_err().to_string();
        assert!(zero.contains("divisor"), "unhelpful error: {zero}");
        // 0.4 truncates to 0 and is refused for the same reason.
        assert!(parse(&doc! { "n": { "$mod": [0.4, 1] } }).is_err());
        assert!(parse(&doc! { "n": { "$mod": 4 } }).is_err());
        assert!(parse(&doc! { "n": { "$mod": [4] } }).is_err());
        assert!(parse(&doc! { "n": { "$mod": [4, 0, 1] } }).is_err());
        assert!(parse(&doc! { "n": { "$mod": ["4", 0] } }).is_err());
        assert!(parse(&doc! { "n": { "$mod": [4, f64::NAN] } }).is_err());
        assert!(parse(&doc! { "n": { "$mod": [4, 1.0e19] } }).is_err());
    }

    // -----------------------------------------------------------------------
    // $expr
    // -----------------------------------------------------------------------

    #[test]
    fn expr_compares_two_fields_of_the_same_document() {
        // The case no other filter operator can express: the right-hand side
        // is a field, not a constant.
        let q = doc! { "$expr": { "$gt": ["$spent", "$budget"] } };
        assert!(hits(q.clone(), doc! { "spent": 120, "budget": 100 }));
        assert!(!hits(q.clone(), doc! { "spent": 80, "budget": 100 }));
        assert!(!hits(q, doc! { "spent": 100, "budget": 100 }));
    }

    #[test]
    fn expr_evaluates_arithmetic() {
        let q = doc! { "$expr": { "$gt": [ { "$multiply": ["$qty", "$price"] }, 100 ] } };
        assert!(hits(q.clone(), doc! { "qty": 3, "price": 40 }));
        assert!(!hits(q.clone(), doc! { "qty": 2, "price": 40 }));
        // Integer times double is a double, and 2.5 * 50 is over the line.
        assert!(hits(q, doc! { "qty": 2.5, "price": 50 }));
    }

    #[test]
    fn expr_reads_a_missing_field_as_null() {
        // Null is what the expression language makes of absence, so the same
        // null rules apply: it equals null, and arithmetic on it is null,
        // which is not greater than anything.
        assert!(hits(doc! { "$expr": { "$eq": ["$a", Bson::Null] } }, doc! { "b": 1 }));
        assert!(hits(doc! { "$expr": { "$eq": ["$a", Bson::Null] } }, doc! { "a": Bson::Null }));
        assert!(!hits(doc! { "$expr": { "$eq": ["$a", Bson::Null] } }, doc! { "a": 1 }));
        assert!(!hits(doc! { "$expr": { "$gt": [ { "$add": ["$a", 1] }, 0 ] } }, doc! { "b": 1 }));
        // Two missing fields are equal to each other.
        assert!(hits(doc! { "$expr": { "$eq": ["$a", "$b"] } }, doc! { "c": 1 }));
    }

    #[test]
    fn expr_comparisons_use_the_canonical_order_not_type_groups() {
        // The one place a filter document compares across type groups: the
        // expression language ranks null below numbers below strings, so a
        // missing field is *less than* zero here. `{a: {$lt: 0}}` would not
        // match either document — and that contrast is documented.
        assert!(hits(doc! { "$expr": { "$lt": ["$a", 0] } }, doc! { "b": 1 }));
        assert!(hits(doc! { "$expr": { "$gt": ["$a", 1] } }, doc! { "a": "text" }));
        assert!(!hits(doc! { "a": { "$gt": 1 } }, doc! { "a": "text" }));
    }

    #[test]
    fn expr_does_not_match_array_elements() {
        // A filter comparison looks inside an array; an expression compares
        // the array itself. `{n: 5}` finds the element, `$expr` does not.
        assert!(hits(doc! { "n": 5 }, doc! { "n": [1, 5, 9] }));
        assert!(!hits(doc! { "$expr": { "$eq": ["$n", 5] } }, doc! { "n": [1, 5, 9] }));
        // And the array as a whole ranks *above* every number in the
        // canonical order, so the two readings can disagree in either
        // direction: the filter finds 1 below 100, the expression does not.
        assert!(hits(doc! { "n": { "$lt": 100 } }, doc! { "n": [1, 5, 9] }));
        assert!(!hits(doc! { "$expr": { "$lt": ["$n", 100] } }, doc! { "n": [1, 5, 9] }));
        // Whole-array equality is element by element, in order.
        assert!(hits(doc! { "$expr": { "$eq": ["$n", [1, 5, 9]] } }, doc! { "n": [1, 5, 9] }));
        assert!(!hits(doc! { "$expr": { "$eq": ["$n", [9, 5, 1]] } }, doc! { "n": [1, 5, 9] }));
    }

    #[test]
    fn expr_applies_truthiness_to_a_bare_field_reference() {
        let q = doc! { "$expr": "$active" };
        assert!(hits(q.clone(), doc! { "active": true }));
        assert!(hits(q.clone(), doc! { "active": 1 }));
        assert!(hits(q.clone(), doc! { "active": "yes" }));
        // The empty string and the empty array are truthy, as in MongoDB.
        assert!(hits(q.clone(), doc! { "active": "" }));
        assert!(hits(q.clone(), doc! { "active": [] }));
        assert!(!hits(q.clone(), doc! { "active": false }));
        assert!(!hits(q.clone(), doc! { "active": 0 }));
        assert!(!hits(q.clone(), doc! { "active": 0.0 }));
        assert!(!hits(q.clone(), doc! { "active": Bson::Null }));
        assert!(!hits(q, doc! { "other": 1 }));
    }

    #[test]
    fn expr_accepts_a_literal() {
        assert!(hits(doc! { "$expr": true }, doc! {}));
        assert!(!hits(doc! { "$expr": false }, doc! { "a": 1 }));
    }

    #[test]
    fn expr_nests_inside_logical_operators_and_beside_fields() {
        let d = doc! { "spent": 120, "budget": 100, "status": "open" };
        let q = doc! { "$or": [ { "status": "closed" }, { "$expr": { "$gt": ["$spent", "$budget"] } } ] };
        assert!(hits(q.clone(), d.clone()));
        assert!(!hits(q, doc! { "spent": 80, "budget": 100, "status": "open" }));

        // Top-level siblings are implicitly `$and`-ed, `$expr` included.
        let q = doc! { "status": "open", "$expr": { "$gt": ["$spent", "$budget"] } };
        assert!(hits(q.clone(), d.clone()));
        assert!(!hits(q, doc! { "spent": 120, "budget": 100, "status": "closed" }));

        let q = doc! { "$nor": [ { "$expr": { "$gt": ["$spent", "$budget"] } } ] };
        assert!(!hits(q.clone(), d));
        assert!(hits(q, doc! { "spent": 80, "budget": 100 }));

        // Negation is the expression language's own `$not`.
        let q = doc! { "$expr": { "$not": [ { "$gt": ["$spent", "$budget"] } ] } };
        assert!(hits(q, doc! { "spent": 80, "budget": 100 }));
    }

    #[test]
    fn expr_inside_elem_match_sees_the_element() {
        // The document form of `$elemMatch` is an ordinary filter over each
        // element, so an expression there reads the element's fields.
        let q = doc! { "lines": { "$elemMatch": { "$expr": { "$gt": ["$qty", "$min"] } } } };
        assert!(hits(
            q.clone(),
            doc! { "lines": [ { "qty": 1, "min": 5 }, { "qty": 9, "min": 5 } ] }
        ));
        assert!(!hits(q.clone(), doc! { "lines": [ { "qty": 1, "min": 5 } ] }));
        // A scalar element has no fields for the expression to read.
        assert!(!hits(q, doc! { "lines": [1, 2, 3] }));
    }

    /// `$$ROOT` was a parse-time refusal in a filter until the expression scope
    /// (ADR-105) bound it everywhere. It names whatever document the filter is
    /// being applied to — which inside `$elemMatch` is the element, not the
    /// document that contains it.
    #[test]
    fn expr_reads_the_document_under_consideration_through_root() {
        let q = doc! { "$expr": { "$gt": ["$$ROOT.spent", "$budget"] } };
        assert!(hits(q.clone(), doc! { "spent": 120, "budget": 100 }));
        assert!(!hits(q, doc! { "spent": 80, "budget": 100 }));

        let q = doc! { "lines": { "$elemMatch": { "$expr": { "$gt": ["$$ROOT.qty", 5] } } } };
        assert!(hits(q.clone(), doc! { "qty": 0, "lines": [ { "qty": 9 } ] }));
        assert!(!hits(q, doc! { "qty": 9, "lines": [ { "qty": 1 } ] }));
    }

    #[test]
    fn expr_treats_a_type_violation_as_no_match() {
        // Adding to a string is an error in a pipeline; here it is a document
        // that does not match, the same way an unusable regex matches nothing.
        let q = doc! { "$expr": { "$gt": [ { "$add": ["$name", 1] }, 0 ] } };
        assert!(!hits(q.clone(), doc! { "name": "text" }));
        assert!(hits(q, doc! { "name": 1 }));
    }

    #[test]
    fn expr_parse_errors_are_the_filter_parsers_errors() {
        // An unknown expression operator and a wrong arity both surface as the
        // filter parser's own error variants, not as a distinct kind.
        assert!(matches!(
            parse(&doc! { "$expr": { "$nope": 1 } }),
            Err(Error::UnsupportedOperator(_))
        ));
        assert!(matches!(parse(&doc! { "$expr": { "$gt": ["$a"] } }), Err(Error::InvalidQuery(_))));
        // A `$$name` nothing binds is still refused before a document is read.
        // `$$ROOT` and `$$CURRENT` are not such a name: the expression scope
        // (ADR-105) binds them everywhere, so a filter's `$expr` takes them too.
        assert!(parse(&doc! { "$expr": "$$nope" }).is_err());
        assert!(parse(&doc! { "$expr": { "$gt": ["$$ROOT.a", 1] } }).is_ok());
        // `$expr` is not a field operator.
        assert!(parse(&doc! { "a": { "$expr": { "$gt": ["$a", 1] } } }).is_err());
    }
}

#[cfg(test)]
mod decimal128 {
    use super::*;
    use bson::doc;

    fn dec() -> Bson {
        Bson::Decimal128("1.5".parse().unwrap())
    }

    fn refused(filter: Document) {
        let Err(err) = parse(&filter) else { panic!("{filter} should be refused") };
        let msg = err.to_string();
        assert!(
            msg.contains("Decimal128") && msg.contains("cannot be compared in a filter"),
            "{filter}: the refusal should say why: {msg}"
        );
    }

    #[test]
    fn a_decimal128_operand_is_refused_wherever_a_filter_would_compare_it() {
        // `canonical_cmp` ranks a Decimal128 equal to every number, so a
        // parse that let one through would match every numeric value of the
        // field. Refused on purpose, by every operator that compares — and
        // inside a document or array literal, which compare by contents.
        refused(doc! { "v": dec() });
        refused(doc! { "v": { "$eq": dec() } });
        refused(doc! { "v": { "$ne": dec() } });
        refused(doc! { "v": { "$gt": dec() } });
        refused(doc! { "v": { "$gte": dec() } });
        refused(doc! { "v": { "$lt": dec() } });
        refused(doc! { "v": { "$lte": dec() } });
        refused(doc! { "v": { "$in": [1, dec()] } });
        refused(doc! { "v": { "$nin": [dec()] } });
        refused(doc! { "v": { "$all": [dec()] } });
        refused(doc! { "v": { "$not": { "$lt": dec() } } });
        refused(doc! { "v": { "$elemMatch": { "$gte": dec() } } });
        refused(doc! { "v": { "$elemMatch": { "n": dec() } } });
        refused(doc! { "$and": [{ "v": 1 }, { "v": dec() }] });
        refused(doc! { "v": { "n": dec() } });
        refused(doc! { "v": [1, [dec()]] });
        refused(doc! { "_id": dec() });
    }

    #[test]
    fn the_refusal_is_about_comparing_not_about_the_type() {
        // A filter that asks what type a field holds, or whether it exists,
        // compares nothing and still finds a stored Decimal128.
        let stored = doc! { "v": dec() };
        let by_type = parse(&doc! { "v": { "$type": "decimal" } }).unwrap();
        assert!(matches(&by_type, &stored));
        let exists = parse(&doc! { "v": { "$exists": true } }).unwrap();
        assert!(matches(&exists, &stored));
        assert!(parse(&doc! { "v": { "$eq": 1.5 } }).is_ok(), "a double compares as ever");
    }
}
