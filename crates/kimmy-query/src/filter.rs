//! Filter parsing and evaluation.
//!
//! A filter document is parsed into an AST once and then evaluated, rather than
//! being walked as BSON on every document. The AST is also what the index
//! planner reads to find usable predicates, so parsing is not just an
//! optimisation — it is the shared representation.

use bson::{Bson, Document};
use kimmy_core::cmp::{canonical_cmp, holds_decimal128};
use kimmy_core::matching::{self, any_element};
use kimmy_core::{Error, Result};
use std::cmp::Ordering;

use regex_automata::meta;
use regex_automata::util::syntax;

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
    /// `$regex`, compiled when the filter was parsed.
    Regex(RegexPattern),
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
///
/// Every `$regex` the filter holds is charged to one budget
/// ([`REGEX_CONDITIONS_LIMIT`], [`REGEX_TOTAL_LIMIT_BYTES`]), or to the
/// budget of the enclosing pipeline when one is being parsed.
pub fn parse(doc: &Document) -> Result<Filter> {
    parse_with_vars(doc, &[])
}

/// [`parse`] for an `arrayFilters` entry (ADR-209): the element is bound to
/// the one name in `vars`, which a `$expr` in the entry reads as `$$name`.
///
/// Only a `$expr` at the entry's own level, or under its `$and`, `$or` and
/// `$nor`, sees the name; a `$elemMatch` body is a filter over the elements
/// of another array, parsed with nothing bound. An expression that reads the
/// document (a field, `$$ROOT`, `$$CURRENT`) is refused there, so that the
/// element has one spelling.
pub fn parse_with_vars(doc: &Document, vars: &[String]) -> Result<Filter> {
    let _budget = RegexBudget::open();
    let mut clauses = Vec::new();

    for (key, value) in doc {
        if let Some(op) = key.strip_prefix('$') {
            clauses.push(parse_logical(op, value, vars)?);
        } else {
            clauses.push(Filter::Field {
                path: key.clone(),
                conditions: parse_conditions(key, value)?,
            });
        }
    }

    Ok(match clauses.len() {
        0 => Filter::AlwaysTrue,
        1 => clauses.pop().expect("length checked"),
        _ => conjunction(clauses),
    })
}

/// `$and` of `clauses`, with the clauses that hold no `$expr` first.
///
/// Every list the matcher evaluates is put in that order when it is built —
/// the clauses of an `$and`, `$or` or `$nor`, a field's conditions — so that
/// evaluation, which stops at the first clause that decides, reaches an
/// expression only when nothing cheaper has answered (ADR-206). The order is
/// decided once here rather than for every document. The order is not what
/// makes the answer independent of the written order: the matcher keeps an
/// expression's error until no clause has decided, whatever came first.
pub fn conjunction(clauses: Vec<Filter>) -> Filter {
    Filter::And(cheap_first(clauses, filter_has_expr))
}

/// `items` with those `has_expr` marks moved to the end, keeping each
/// group's order.
fn cheap_first<T>(items: Vec<T>, has_expr: fn(&T) -> bool) -> Vec<T> {
    let (cheap, expensive): (Vec<T>, Vec<T>) = items.into_iter().partition(|i| !has_expr(i));
    cheap.into_iter().chain(expensive).collect()
}

fn parse_logical(op: &str, value: &Bson, vars: &[String]) -> Result<Filter> {
    let branches = |value: &Bson| -> Result<Vec<Filter>> {
        let Bson::Array(items) = value else {
            return Err(Error::InvalidQuery(format!("${op} requires an array")));
        };
        if items.is_empty() {
            return Err(Error::InvalidQuery(format!("${op} requires a non-empty array")));
        }
        let parsed = items
            .iter()
            .map(|item| match item {
                Bson::Document(d) => parse_with_vars(d, vars),
                _ => Err(Error::InvalidQuery(format!("${op} entries must be documents"))),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(cheap_first(parsed, filter_has_expr))
    };

    Ok(match op {
        "and" => Filter::And(branches(value)?),
        "or" => Filter::Or(branches(value)?),
        "nor" => Filter::Nor(branches(value)?),
        // The expression parser reports its own errors — an unknown operator,
        // a wrong arity — in the same error type as the rest of this module,
        // so a caller sees one kind of `400` whichever half of the filter was
        // malformed.
        "expr" => {
            let expr = Expr::parse_with_vars(value, vars)?;
            if let Some(element) = vars.first()
                && expr.reads_document()
            {
                return Err(Error::InvalidQuery(format!(
                    "an $expr in an arrayFilters entry reads the element as $${element}, as in \
                     $${element}.qty; a field, $$ROOT and $$CURRENT name the document, which \
                     it cannot read"
                )));
            }
            Filter::Expr(Box::new(expr))
        }
        // `$not` is only meaningful applied to a field's operators; at the top
        // level Mongo rejects it too, and the clearer error is worth it.
        "not" => {
            return Err(Error::InvalidQuery(
                "$not must be applied to a field, e.g. {field: {$not: {$gt: 5}}}".into(),
            ));
        }
        other => {
            return Err(Error::UnsupportedOperator { operator: format!("${other}"), reason: None });
        }
    })
}

/// Decide whether a field's value is an operator document or a literal.
///
/// `{a: {$gt: 1}}` is a comparison; `{a: {b: 1}}` is equality against a nested
/// document. Mongo's rule is that the *first* key decides, and mixing is an
/// error rather than a silent reinterpretation.
///
/// `path` is the field the conditions apply to, empty for an array element,
/// and only names it in a refusal.
fn parse_conditions(path: &str, value: &Bson) -> Result<Vec<Condition>> {
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
        Some(_) => return Err(Error::InvalidQuery("$options requires a string of flags".into())),
        None => String::new(),
    };

    let conditions = doc
        .iter()
        .map(|(key, arg)| parse_condition(path, &key[1..], arg, &sibling_options))
        .collect::<Result<Vec<_>>>()?;
    Ok(cheap_first(conditions, condition_has_expr))
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
            conditions: parse_conditions("", &Bson::Document(doc.clone()))?,
        });
    }
    parse(doc)
}

fn parse_condition(path: &str, op: &str, arg: &Bson, sibling_options: &str) -> Result<Condition> {
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
        "ne" => Condition::Ne(not_regex(arg)?.clone()),
        "gt" => Condition::Gt(arg.clone()),
        "gte" => Condition::Gte(arg.clone()),
        "lt" => Condition::Lt(arg.clone()),
        "lte" => Condition::Lte(arg.clone()),
        "in" => Condition::In(without_regex(op, array_arg(arg)?)?),
        "nin" => Condition::Nin(without_regex(op, array_arg(arg)?)?),
        "all" => Condition::All(non_empty_all(
            path,
            without_elem_match(without_regex(op, array_arg(arg)?)?)?,
        )?),
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
            Bson::String(pattern) => regex_condition(pattern, sibling_options)?,
            Bson::RegularExpression(re) => {
                // Flags written on the literal win over a sibling `$options`.
                let options = if re.options.as_str().is_empty() {
                    sibling_options
                } else {
                    re.options.as_str()
                };
                regex_condition(re.pattern.as_str(), options)?
            }
            _ => return Err(Error::InvalidQuery("$regex requires a string or regex".into())),
        },
        // Already folded into the sibling `$regex` above. On its own it
        // constrains nothing, but its flags are still checked: a flag this
        // database does not implement is refused wherever it is written.
        "options" => {
            check_regex_options(sibling_options)?;
            Condition::AlwaysTrue
        }
        "elemMatch" => match arg {
            Bson::Document(d) => Condition::ElemMatch(Box::new(parse_elem_match(d)?)),
            _ => return Err(Error::InvalidQuery("$elemMatch requires a document".into())),
        },
        "not" => match arg {
            Bson::Document(d) => {
                // `{$not: {$gt: 1, $lt: 5}}` negates the whole conjunction, so
                // the operators are combined before the negation is applied.
                let combined = parse_conditions(path, &Bson::Document(d.clone()))?
                    .into_iter()
                    .reduce(|a, b| Condition::Both(Box::new(a), Box::new(b)))
                    .ok_or_else(|| {
                        Error::InvalidQuery("$not requires at least one operator".into())
                    })?;
                Condition::Not(Box::new(combined))
            }
            Bson::RegularExpression(re) => {
                Condition::Not(Box::new(regex_condition(re.pattern.as_str(), re.options.as_str())?))
            }
            _ => return Err(Error::InvalidQuery("$not requires a document or regex".into())),
        },
        other => {
            return Err(Error::UnsupportedOperator { operator: format!("${other}"), reason: None });
        }
    })
}

/// Whether `value` is a regular expression in one of the shapes a filter
/// can carry: a regex literal, a document whose keys are `$regex` and
/// optionally `$options`, or a document whose only key is
/// `$regularExpression`, which is how one arrives over HTTP.
fn is_regex_shaped(value: &Bson) -> bool {
    match value {
        Bson::RegularExpression(_) => true,
        Bson::Document(d) => {
            (d.contains_key("$regex") && d.keys().all(|k| k == "$regex" || k == "$options"))
                || (d.len() == 1 && d.contains_key("$regularExpression"))
        }
        _ => false,
    }
}

/// Refuse a regular expression among the values of `$in`, `$nin` or `$all`.
///
/// These operators compare values, so a regex among them was compared as a
/// value: `{s: {$in: [/abc/]}}` matched only a stored regex `/abc/` and never
/// the string `"xabcx"`, and `$nin` matched every string. Over HTTP a regex
/// cannot even be written as one, so `{$regex: "abc"}` or the Extended JSON
/// `{$regularExpression: ...}` arrived as a document and was compared as
/// that. Each answered a result that looks right and is not.
///
/// Only those shapes are refused ([`is_regex_shaped`]). A stored document
/// may hold a key beginning with `$`, and `$in` still compares against any
/// other document, a `{$regex: ..., other: ...}` among them.
fn without_regex(op: &str, values: Vec<Bson>) -> Result<Vec<Bson>> {
    match values.iter().find(|v| is_regex_shaped(v)) {
        Some(regex) => Err(Error::InvalidQuery(format!(
            "${op} cannot hold a regular expression, and {regex} is one: ${op} compares \
             values, so it would match a stored value of that shape and never a string the \
             pattern matches; to match any of several patterns, write an $or of $regex \
             clauses, as in {{\"$or\": [{{\"name\": {{\"$regex\": \"^a\"}}}}, {{\"name\": \
             {{\"$regex\": \"^b\"}}}}]}}, or one pattern, as in {{\"$regex\": \"^(a|b)\"}}"
        ))),
        None => Ok(values),
    }
}

/// Refuse a regular expression as the operand of `$ne`.
///
/// `$ne` compares values, so `{s: {$ne: /abc/}}` held for every string, the
/// ones the pattern matches included, and over HTTP `{$ne: {$regex: "abc"}}`
/// did the same through a document. `$eq` keeps comparing a regex as a value,
/// as a regex written directly as a field's value does (`docs/deviations.md`).
fn not_regex(operand: &Bson) -> Result<&Bson> {
    if is_regex_shaped(operand) {
        return Err(Error::InvalidQuery(format!(
            "$ne cannot take a regular expression, and {operand} is one: $ne compares values, \
             so it would hold for every string, the ones the pattern matches included; to \
             exclude the strings a pattern matches, write {{\"$not\": {{\"$regex\": \
             \"abc\"}}}}"
        )));
    }
    Ok(operand)
}

/// Refuse an `$elemMatch` among the values of `$all`.
///
/// `$all` compares values, so `{items: {$all: [{$elemMatch: {qty: 5}}]}}`
/// compared every element with the document `{$elemMatch: {qty: 5}}` and
/// matched nothing, even when an element has `qty: 5`. Only a document whose
/// only key is `$elemMatch` is refused; any other document is still a value.
fn without_elem_match(values: Vec<Bson>) -> Result<Vec<Bson>> {
    let is_elem_match =
        |v: &Bson| matches!(v, Bson::Document(d) if d.len() == 1 && d.contains_key("$elemMatch"));
    match values.iter().find(|v| is_elem_match(v)) {
        Some(elem_match) => Err(Error::InvalidQuery(format!(
            "$all cannot hold an $elemMatch, and {elem_match} is one: $all compares values, so \
             it would match only a stored document of that shape; to require an element \
             matching each condition, write an $and of $elemMatch clauses, as in \
             {{\"$and\": [{{\"items\": {{\"$elemMatch\": {{\"qty\": 5}}}}}}, {{\"items\": \
             {{\"$elemMatch\": {{\"sku\": \"a\"}}}}}}]}}"
        ))),
        None => Ok(values),
    }
}

/// Refuse an empty `$all`.
///
/// "Every one of no values" holds for any array, so `{tags: {$all: []}}`
/// matched every document whose field holds an array, and under `$not` every
/// document whose field does not. A list that arrives empty is far more
/// likely a client's bug than a question, and on a `multi` update or delete
/// it reached every such document with a `200`.
fn non_empty_all(path: &str, values: Vec<Bson>) -> Result<Vec<Bson>> {
    if values.is_empty() {
        let field =
            if path.is_empty() { "an array element".to_string() } else { format!("{path:?}") };
        return Err(Error::InvalidQuery(format!(
            "$all on {field} has an empty list, which would match every document whose field \
             holds an array, so a list emptied by mistake would reach all of them; send at least \
             one value, or leave the condition out"
        )));
    }
    Ok(values)
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

/// Every name a value can report: what [`type_name_of`] returns, and so every
/// alias `$type` accepts. Not [`type_name_for_code`]'s table, which has no
/// code for `symbol` or `dbPointer` and would refuse types `$type` finds.
const TYPE_NAMES: [&str; 20] = [
    "double",
    "string",
    "object",
    "array",
    "binData",
    "undefined",
    "objectId",
    "bool",
    "date",
    "null",
    "regex",
    "javascript",
    "int",
    "timestamp",
    "long",
    "decimal",
    "minKey",
    "maxKey",
    "symbol",
    "dbPointer",
];

/// What the alias `number` stands for: the four numeric types. It has no
/// numeric code, and no value reports it, so it is expanded when the filter is
/// parsed and the evaluator only ever sees concrete names.
const NUMBER_TYPES: [&str; 4] = ["double", "int", "long", "decimal"];

fn parse_type_arg(arg: &Bson) -> Result<Vec<String>> {
    let one = |value: &Bson| -> Result<Vec<String>> {
        match value {
            Bson::String(s) if s == "number" => {
                Ok(NUMBER_TYPES.iter().map(|n| (*n).to_string()).collect())
            }
            Bson::String(s) if TYPE_NAMES.contains(&s.as_str()) => Ok(vec![s.clone()]),
            Bson::String(s) => Err(Error::InvalidQuery(format!(
                "unknown $type alias {s:?}; the aliases are number and {}",
                TYPE_NAMES.join(", ")
            ))),
            Bson::Int32(n) => type_name_for_code(i64::from(*n)).map(|name| vec![name]),
            Bson::Int64(n) => type_name_for_code(*n).map(|name| vec![name]),
            _ => Err(Error::InvalidQuery("$type requires a string alias or type code".into())),
        }
    };
    match arg {
        Bson::Array(items) if items.is_empty() => {
            Err(Error::InvalidQuery("$type requires at least one alias or type code".into()))
        }
        Bson::Array(items) => {
            Ok(items.iter().map(one).collect::<Result<Vec<_>>>()?.into_iter().flatten().collect())
        }
        other => one(other),
    }
}

/// A `$regex` condition, once its flags are known and its pattern compiles.
fn regex_condition(pattern: &str, options: &str) -> Result<Condition> {
    Ok(Condition::Regex(RegexPattern::new(pattern, options)?))
}

/// The most memory one `$regex` pattern may compile to: 1 MiB, as the engine
/// counts the automaton it builds (`regex`'s own default is 10 MiB).
///
/// A compiled pattern is kept in the parsed filter for the life of the
/// request, so its size is held, not passed through. Most patterns compile to
/// a few kilobytes; what reaches the limit is a Unicode class repeated many
/// times — `\w` alone is some 55 KiB, so `\w{20}` is about 1 MiB and
/// `\w{209}` would be 11 MiB.
pub const REGEX_PATTERN_LIMIT_BYTES: usize = 1 << 20;

/// The most a compiled pattern's lazy DFA may grow to while it matches:
/// 256 KiB (`regex`'s default is 2 MiB).
///
/// The cache stays with the pattern once grown, so a filter can hold one per
/// `$regex` until the request ends: a hundred patterns of the shape
/// `[ab]*a[ab]{14}[^ab]` over strings of `a` and `b` held 92 MiB after matching
/// at 1 MiB each. Past the capacity the engine clears the cache or falls back
/// to a slower search, and the answer is the same. Measured on 10,000 short
/// strings, only a pattern near [`REGEX_PATTERN_LIMIT_BYTES`] slows down
/// (`\w{20}` from 0.3 ms to 6 ms); an ordinary one is as fast as at 2 MiB.
pub const REGEX_CACHE_LIMIT_BYTES: usize = 256 << 10;

/// The most `$regex` conditions one filter may hold.
///
/// Every `$match` of a pipeline, its `$lookup` sub-pipelines included, counts
/// as one filter, and so do all the entries of an update's `arrayFilters`; an
/// update's or a `find_and_modify`'s query filter is a budget of its own,
/// apart from its `arrayFilters`.
///
/// Each pattern that has matched keeps its search caches until the request
/// ends: up to two lazy DFA caches of [`REGEX_CACHE_LIMIT_BYTES`], one for
/// each direction it searches in, and the smaller caches of the slower
/// engines. So this is what limits a request's memory after matching, not
/// only after parse. **Measured, not bounded:** the most measured for
/// thirty-two patterns is 17.7 MiB after matching, about 0.53 MiB of caches
/// per pattern, from reverse-anchored alternations such as
/// `(a|b)*a(a|b){13}(a|b)*$` over long strings of `a` and `b`
/// (`tests/regex_memory.rs`). A filter of more than a couple of dozen
/// patterns is not a workload this serves; alternatives fit in one pattern,
/// `"^(abc|def)"`.
pub const REGEX_CONDITIONS_LIMIT: usize = 32;

/// The most memory the compiled patterns of one filter may hold together,
/// counted over the same scope as [`REGEX_CONDITIONS_LIMIT`]: 2 MiB.
pub const REGEX_TOTAL_LIMIT_BYTES: usize = 2 << 20;

/// What the `$regex` conditions parsed so far under one budget have cost.
#[derive(Clone, Copy, Default)]
struct RegexSpend {
    conditions: usize,
    bytes: usize,
}

thread_local! {
    /// The budget being charged, while one is open on this thread.
    ///
    /// Parsing is synchronous and never yields, so a thread-local scope is
    /// exactly one parse; it lets a pipeline or an update charge every filter
    /// it parses to one budget without threading it through each parser.
    static REGEX_SPEND: std::cell::Cell<Option<RegexSpend>> =
        const { std::cell::Cell::new(None) };
}

/// One budget for the `$regex` conditions parsed until it is dropped.
///
/// Opening one while another is open joins it, so a filter parsed inside a
/// pipeline charges the pipeline's budget. Only the outermost closes it.
pub(crate) struct RegexBudget {
    outermost: bool,
}

impl RegexBudget {
    pub(crate) fn open() -> Self {
        REGEX_SPEND.with(|spend| {
            let outermost = spend.get().is_none();
            if outermost {
                spend.set(Some(RegexSpend::default()));
            }
            Self { outermost }
        })
    }
}

impl Drop for RegexBudget {
    fn drop(&mut self) {
        if self.outermost {
            REGEX_SPEND.with(|spend| spend.set(None));
        }
    }
}

/// A `$regex` pattern under its flags, compiled once, when the filter is
/// parsed, and kept in the condition.
///
/// Matching used to compile the pattern again for every document it was
/// tested against — after parsing had already compiled it once to check it
/// and thrown the result away. Compiling is the expensive half of a regex
/// match, so a scan paid for it once per document. Now nothing compiles after
/// parse: the matcher only runs the compiled program, which the engine shares
/// between clones, so cloning a filter copies no program either.
///
/// Keeping it means holding it, so what one pattern and one filter may hold
/// is bounded: [`REGEX_PATTERN_LIMIT_BYTES`], [`REGEX_CACHE_LIMIT_BYTES`],
/// [`REGEX_CONDITIONS_LIMIT`] and [`REGEX_TOTAL_LIMIT_BYTES`].
///
/// The fields are private, so the only way to build one is [`Self::new`],
/// which refuses what the matcher cannot use. A condition carrying a flag
/// nothing implements, or a pattern that does not compile, cannot exist.
///
/// Two patterns are equal when their source and flags are: the compiled form
/// is a function of those two.
#[derive(Clone)]
pub struct RegexPattern {
    pattern: String,
    options: String,
    compiled: meta::Regex,
}

impl RegexPattern {
    /// Compile `pattern` under `options`.
    ///
    /// Both are decided here, when the filter is parsed, so a pattern the
    /// matcher cannot use is a `400` naming what is wrong with it. It used to
    /// match nothing, which reads as an empty result: a backreference or a
    /// lookaround, which the engine does not support, and a plain typo look
    /// the same as "no document matches". While a filter is being parsed the
    /// pattern is charged to its budget, and refused past it.
    pub fn new(pattern: &str, options: &str) -> Result<Self> {
        let spent = REGEX_SPEND.with(std::cell::Cell::get);
        // Counted before compiling, so a filter of many patterns stops at the
        // limit rather than compiling them all first.
        if spent.is_some_and(|s| s.conditions >= REGEX_CONDITIONS_LIMIT) {
            return Err(Error::InvalidQuery(format!(
                "a filter may hold at most {REGEX_CONDITIONS_LIMIT} $regex conditions, the stages \
                 of a pipeline counting as one filter and the arrayFilters of an update as \
                 another, apart from its query filter; this one holds more: write alternatives \
                 as one pattern, as in \"^(abc|def)\""
            )));
        }
        let compiled = build_regex(pattern, options)?;
        if let Some(spent) = spent {
            let bytes = spent.bytes + compiled.memory_usage();
            if bytes > REGEX_TOTAL_LIMIT_BYTES {
                return Err(Error::InvalidQuery(format!(
                    "the $regex patterns of this filter compile to more than {} MiB together, \
                     the limit for one filter, the stages of a pipeline counting as one filter \
                     and the arrayFilters of an update as another, apart from its query \
                     filter; {pattern:?} is the one past it: a Unicode class \
                     repeated many times, such as \\w{{20}}, is the usual cause, and an ASCII \
                     class such as [A-Za-z0-9_] compiles far smaller",
                    REGEX_TOTAL_LIMIT_BYTES >> 20
                )));
            }
            REGEX_SPEND.with(|s| {
                s.set(Some(RegexSpend { conditions: spent.conditions + 1, bytes }));
            });
        }
        Ok(Self { pattern: pattern.to_string(), options: options.to_string(), compiled })
    }

    /// The pattern as the filter wrote it.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// The flags it was compiled under.
    pub fn options(&self) -> &str {
        &self.options
    }

    /// Whether the pattern matches somewhere in `s`.
    pub fn is_match(&self, s: &str) -> bool {
        self.compiled.is_match(s)
    }
}

impl PartialEq for RegexPattern {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.options == other.options
    }
}

impl std::fmt::Debug for RegexPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegexPattern")
            .field("pattern", &self.pattern)
            .field("options", &self.options)
            .finish()
    }
}

/// Refuse a `$regex` flag this database does not implement. The four it does
/// (`i`, `m`, `s`, `x`) are all there are: an unknown letter used to be
/// dropped, so `"I"` compiled the pattern case-sensitively and answered an
/// empty result with nothing said.
fn check_regex_options(options: &str) -> Result<()> {
    match options.chars().find(|flag| !matches!(flag, 'i' | 'm' | 's' | 'x')) {
        Some(flag) => Err(unknown_regex_flag(flag)),
        None => Ok(()),
    }
}

fn unknown_regex_flag(flag: char) -> Error {
    Error::InvalidQuery(format!("unknown $regex flag {flag:?}; the flags are i, m, s and x"))
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
///
/// `Err` when a `$expr` the evaluation reached cannot be evaluated against
/// this document — `{$add: ["$qty", 1]}` where `qty` is a string — and every
/// caller fails its request with it (ADR-206). The error names the document's
/// `_id` when it has one, and the evaluator's reason, which names the operator
/// and the type it could not take.
///
/// **A document fails exactly when the answer for it depends on a `$expr`
/// that cannot be evaluated** (ADR-206). An `$and` (an implicit one
/// included) is false if any clause is false, whatever the others say; an
/// `$or` is true if any branch is true; `$nor` is the inverse of `$or`, and
/// `$elemMatch` is true if any element matches. Only when no clause decides
/// does an expression that could not be evaluated fail the document. So the
/// outcome is the same in any order the clauses are written, and the same
/// whichever access path offered the document: an index can only leave out a
/// document that a clause without `$expr` already excludes.
///
/// Within each of them the clauses that hold no `$expr` are evaluated first,
/// and evaluation stops at the first clause that decides, so an expression is
/// not evaluated where a cheaper clause has already answered.
pub fn matches(filter: &Filter, doc: &Document) -> Result<bool> {
    evaluate(filter, doc).map_err(|e| unevaluable(e, doc.get("_id"), ""))
}

/// The refusal for a `$expr` that could not be evaluated, against the
/// document with `_id` when it has one, or against something `within` it.
fn unevaluable(e: Error, id: Option<&Bson>, within: &str) -> Error {
    let reason = match e {
        Error::InvalidQuery(reason) => reason,
        other => other.to_string(),
    };
    let at = match id {
        Some(id) => format!(" for {within}the document with _id {id}"),
        None if within.is_empty() => String::new(),
        None => format!(" for {}", within.trim_end_matches(" of ")),
    };
    Error::InvalidQuery(format!("$expr cannot be evaluated{at}: {reason}"))
}

/// Whether a filter holds a `$expr` anywhere it is evaluated from.
fn filter_has_expr(filter: &Filter) -> bool {
    match filter {
        Filter::AlwaysTrue => false,
        Filter::And(branches) | Filter::Or(branches) | Filter::Nor(branches) => {
            branches.iter().any(filter_has_expr)
        }
        Filter::Field { conditions, .. } => conditions.iter().any(condition_has_expr),
        Filter::Expr(_) => true,
    }
}

/// Whether a condition holds a `$expr`, which only an `$elemMatch` can.
fn condition_has_expr(condition: &Condition) -> bool {
    match condition {
        Condition::ElemMatch(inner) => filter_has_expr(inner),
        Condition::Not(inner) => condition_has_expr(inner),
        Condition::Both(a, b) => condition_has_expr(a) || condition_has_expr(b),
        _ => false,
    }
}

/// Whether every item satisfies `f`, in the order the parser left them in
/// (cheap first). The first `false` decides, whatever came before it; an
/// error is kept, and returned only if no item said `false`.
fn all_of<T>(items: &[T], mut f: impl FnMut(&T) -> Result<bool>) -> Result<bool> {
    let mut undecided = None;
    for item in items {
        match f(item) {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(e) => {
                undecided.get_or_insert(e);
            }
        }
    }
    undecided.map_or(Ok(true), Err)
}

/// Whether any item satisfies `f`, in the order the parser left them in
/// (cheap first). The first `true` decides, whatever came before it; an
/// error is kept, and returned only if no item said `true`.
fn any_of<T>(items: &[T], mut f: impl FnMut(&T) -> Result<bool>) -> Result<bool> {
    let mut undecided = None;
    for item in items {
        match f(item) {
            Ok(false) => {}
            Ok(true) => return Ok(true),
            Err(e) => {
                undecided.get_or_insert(e);
            }
        }
    }
    undecided.map_or(Ok(false), Err)
}

fn evaluate(filter: &Filter, doc: &Document) -> Result<bool> {
    match filter {
        Filter::AlwaysTrue => Ok(true),
        Filter::And(branches) => all_of(branches, |f| evaluate(f, doc)),
        Filter::Or(branches) => any_of(branches, |f| evaluate(f, doc)),
        Filter::Nor(branches) => Ok(!any_of(branches, |f| evaluate(f, doc))?),
        Filter::Field { path, conditions } => {
            let values = path::resolve(doc, path);
            all_of(conditions, |c| condition_matches(c, &values))
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
/// **An evaluation error is the caller's error, not "no match"** (ADR-206).
/// `{$expr: {$gt: [{$add: ["$name", 1]}, 0]}}` on a document whose `name` is
/// a string has no truth value, and reading it as false made a document with
/// bad data look like one that did not satisfy the filter: the result could
/// not be told from a correct one. It used to be read that way because this
/// function answered a `bool` to every caller.
fn expr_matches(e: &Expr, doc: &Document) -> Result<bool> {
    #[cfg(test)]
    EXPR_EVALUATIONS.with(|n| n.set(n.get() + 1));
    e.eval(doc).map(|v| expr::truthy(&v))
}

#[cfg(test)]
thread_local! {
    /// How many `$expr` clauses this thread has evaluated, for the tests that
    /// hold the matcher to evaluating cheap clauses first.
    static EXPR_EVALUATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// `$expr` clauses this thread has evaluated so far.
#[cfg(test)]
pub(crate) fn expr_evaluations() -> usize {
    EXPR_EVALUATIONS.with(std::cell::Cell::get)
}

/// Evaluate one condition against the values found at a path.
///
/// `values` is empty when the path is absent, which several operators treat
/// specially. Only `$elemMatch` can fail, through a `$expr` in its body.
fn condition_matches(condition: &Condition, values: &[&Bson]) -> Result<bool> {
    Ok(match condition {
        // The operators a partial filter may also carry are evaluated in
        // `kimmy_core::matching`, once, for both (ADR-181).
        Condition::Exists(want) => matching::exists(values, *want),
        Condition::Eq(expected) => matching::equals(values, expected),
        Condition::Ne(expected) => !matching::equals(values, expected),

        Condition::Gt(bound) => matching::compares(values, bound, &[Ordering::Greater]),
        Condition::Gte(bound) => {
            matching::compares(values, bound, &[Ordering::Greater, Ordering::Equal])
        }
        Condition::Lt(bound) => matching::compares(values, bound, &[Ordering::Less]),
        Condition::Lte(bound) => {
            matching::compares(values, bound, &[Ordering::Less, Ordering::Equal])
        }

        Condition::In(options) => options.iter().any(|option| matching::equals(values, option)),
        Condition::Nin(options) => !options.iter().any(|option| matching::equals(values, option)),

        Condition::Type(names) => {
            any_element(values, |v| names.iter().any(|n| n == type_name_of(v)))
        }

        Condition::Regex(re) => any_element(values, |v| match v {
            Bson::String(s) => re.is_match(s),
            _ => false,
        }),

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

        Condition::ElemMatch(inner) => any_of(values, |v| match v {
            Bson::Array(items) => any_of(items, |item| match item {
                Bson::Document(d) => evaluate(inner, d),
                // A scalar element is tested by wrapping it so that
                // `{$elemMatch: {$gt: 5}}` works on an array of numbers.
                scalar => matches_scalar_against(inner, scalar),
            }),
            _ => Ok(false),
        })?,

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

        Condition::Not(inner) => !condition_matches(inner, values)?,

        Condition::AlwaysTrue => true,
        Condition::Both(a, b) => {
            all_of(&[a.as_ref(), b.as_ref()], |c| condition_matches(c, values))?
        }
    })
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
///
/// A `$expr` in the entry reads the element as `$$<identifier>` (ADR-209),
/// whatever its type: `identifier` is the name it was parsed with, and is
/// bound to `element` while it evaluates. Nothing else is bound, and the
/// parser refused an expression that reads the document, so the scope's root
/// is an empty document nothing reads.
///
/// `Err` as for [`matches`], when a `$expr` cannot be evaluated against the
/// element, or, under an `$elemMatch` on a field of the element, against an
/// element of that field. `id` is the `_id` of the document the element is
/// in, for the message.
pub fn matches_element(
    filter: &Filter,
    identifier: &str,
    element: &Bson,
    id: Option<&Bson>,
) -> Result<bool> {
    element_matches(filter, identifier, element)
        .map_err(|e| unevaluable(e, id, "an array element of "))
}

fn element_matches(filter: &Filter, identifier: &str, element: &Bson) -> Result<bool> {
    match filter {
        Filter::AlwaysTrue => Ok(true),
        Filter::And(branches) => all_of(branches, |f| element_matches(f, identifier, element)),
        Filter::Or(branches) => any_of(branches, |f| element_matches(f, identifier, element)),
        Filter::Nor(branches) => {
            Ok(!any_of(branches, |f| element_matches(f, identifier, element))?)
        }
        Filter::Field { path, conditions } => {
            if path.is_empty() {
                return all_of(conditions, |c| condition_matches(c, &[element]));
            }
            let values = match element {
                Bson::Document(doc) => path::resolve(doc, path),
                _ => Vec::new(),
            };
            all_of(conditions, |c| condition_matches(c, &values))
        }
        Filter::Expr(e) => {
            #[cfg(test)]
            EXPR_EVALUATIONS.with(|n| n.set(n.get() + 1));
            let root = Document::new();
            let bindings = [(identifier, element)];
            e.eval_in(&expr::Scope::with_bindings(&root, &bindings)).map(|v| expr::truthy(&v))
        }
    }
}

/// Evaluate a filter whose conditions target the element itself, used by
/// `$elemMatch` over an array of scalars.
fn matches_scalar_against(filter: &Filter, scalar: &Bson) -> Result<bool> {
    match filter {
        Filter::AlwaysTrue => Ok(true),
        Filter::And(branches) => all_of(branches, |f| matches_scalar_against(f, scalar)),
        Filter::Or(branches) => any_of(branches, |f| matches_scalar_against(f, scalar)),
        Filter::Nor(branches) => Ok(!any_of(branches, |f| matches_scalar_against(f, scalar))?),
        Filter::Field { path, conditions } => {
            // A scalar element has no fields, so only an empty path applies.
            if !path.is_empty() {
                return Ok(false);
            }
            all_of(conditions, |c| condition_matches(c, &[scalar]))
        }
        // An expression reads fields, and a scalar has none to read: nothing
        // is evaluated, so there is nothing to fail.
        Filter::Expr(_) => Ok(false),
    }
}

/// The pattern under its flags, compiled within [`REGEX_PATTERN_LIMIT_BYTES`]
/// and [`REGEX_CACHE_LIMIT_BYTES`].
///
/// The flags are checked here, before anything is compiled, and this is the
/// check a `$regex` meets; a lone `$options` meets [`check_regex_options`].
/// The engine is `regex`'s own, configured as `regex::RegexBuilder` configures
/// it but for the two limits, and used directly because it reports how much
/// memory a compiled pattern holds, which the filter's budget counts.
///
/// Called only from [`RegexPattern::new`]: once per `$regex` in a filter, when
/// it is parsed, and never while matching.
fn build_regex(pattern: &str, options: &str) -> Result<meta::Regex> {
    #[cfg(test)]
    REGEX_COMPILATIONS.with(|n| n.set(n.get() + 1));
    let mut syntax = syntax::Config::new();
    for flag in options.chars() {
        syntax = match flag {
            'i' => syntax.case_insensitive(true),
            'm' => syntax.multi_line(true),
            's' => syntax.dot_matches_new_line(true),
            'x' => syntax.ignore_whitespace(true),
            other => return Err(unknown_regex_flag(other)),
        };
    }
    meta::Builder::new()
        .syntax(syntax)
        .configure(
            meta::Config::new()
                .nfa_size_limit(Some(REGEX_PATTERN_LIMIT_BYTES))
                .hybrid_cache_capacity(REGEX_CACHE_LIMIT_BYTES),
        )
        .build(pattern)
        .map_err(|e| {
            let reason = match (e.syntax_error(), e.size_limit()) {
                // The engine's message is a multi-line rendering of the
                // pattern with a caret under the fault. The reason is its
                // last line.
                (Some(syntax), _) => {
                    let message = syntax.to_string();
                    message
                        .lines()
                        .last()
                        .unwrap_or_default()
                        .trim_start_matches("error: ")
                        .to_string()
                }
                (None, Some(limit)) => format!(
                    "it compiles to more than {} MiB, the limit for one pattern; a Unicode class \
                     repeated many times, such as \\w{{100}}, is the usual cause, and an ASCII \
                     class such as [A-Za-z0-9_] or a smaller count compiles far smaller",
                    limit >> 20
                ),
                (None, None) => e.to_string(),
            };
            Error::InvalidQuery(format!("$regex pattern {pattern:?} cannot be used: {reason}"))
        })
}

#[cfg(test)]
thread_local! {
    /// How many regexes this thread has compiled, for the tests that hold
    /// matching to compiling none.
    static REGEX_COMPILATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Regexes this thread has compiled so far.
#[cfg(test)]
pub(crate) fn regex_compilations() -> usize {
    REGEX_COMPILATIONS.with(std::cell::Cell::get)
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    fn hits(query: Document, d: Document) -> bool {
        let filter = parse(&query).unwrap_or_else(|e| panic!("parse failed: {e}"));
        matches(&filter, &d).unwrap_or_else(|e| panic!("evaluation failed: {e}"))
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

    fn literal(pattern: &str, options: &str) -> Bson {
        Bson::RegularExpression(bson::Regex {
            pattern: pattern.to_string().try_into().expect("valid pattern"),
            options: options.to_string().try_into().expect("valid options"),
        })
    }

    fn refused(query: Document) -> String {
        match parse(&query) {
            Err(Error::InvalidQuery(message)) => message,
            other => panic!("{query:?} must be refused as an invalid query, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_type_alias_is_refused_naming_it() {
        for alias in ["boolean", "Int", "nosuchtype", "number ", ""] {
            let message = refused(doc! { "a": { "$type": alias } });
            assert!(message.contains(&format!("{alias:?}")), "{message}");
        }
        // Inside an array the bad one is named, whatever its neighbours are.
        let message = refused(doc! { "a": { "$type": ["string", "nosuch"] } });
        assert!(message.contains("nosuch"), "{message}");
        // The numeric spelling was already refused, and stays refused.
        assert!(refused(doc! { "a": { "$type": 999 } }).contains("999"));
    }

    #[test]
    fn an_empty_all_is_refused_naming_the_field() {
        for (q, field) in [
            (doc! { "tags": { "$all": [] } }, r#""tags""#),
            (doc! { "a.b": { "$all": [] } }, r#""a.b""#),
            (doc! { "tags": { "$not": { "$all": [] } } }, r#""tags""#),
            (doc! { "$or": [ { "x": 1 }, { "tags": { "$all": [] } } ] }, r#""tags""#),
            (doc! { "items": { "$elemMatch": { "tags": { "$all": [] } } } }, r#""tags""#),
            (doc! { "grid": { "$elemMatch": { "$all": [] } } }, "an array element"),
        ] {
            let message = refused(q.clone());
            assert!(
                message.starts_with(&format!("$all on {field} has an empty list"))
                    && message.contains("send at least one value"),
                "{q}: {message}"
            );
        }
        // One value or more is unchanged.
        assert!(hits(doc! { "tags": { "$all": ["a"] } }, doc! { "tags": ["a", "b"] }));
        assert!(!hits(doc! { "tags": { "$all": ["a", "c"] } }, doc! { "tags": ["a", "b"] }));
    }

    #[test]
    fn an_empty_type_list_is_refused() {
        refused(doc! { "a": { "$type": [] } });
    }

    #[test]
    fn every_name_a_value_reports_is_a_type_alias() {
        // The table validated against is `type_name_of`'s, not the code
        // table's: `symbol` and `dbPointer` have no code and still match.
        let samples: Vec<Bson> = vec![
            Bson::Double(1.0),
            Bson::String("s".into()),
            Bson::Document(doc! {}),
            Bson::Array(vec![]),
            Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: vec![],
            }),
            Bson::Undefined,
            Bson::ObjectId(bson::oid::ObjectId::new()),
            Bson::Boolean(true),
            Bson::DateTime(bson::DateTime::now()),
            Bson::Null,
            literal("a", "i"),
            Bson::JavaScriptCode("1".into()),
            Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
                code: "1".into(),
                scope: doc! {},
            }),
            Bson::Int32(1),
            Bson::Timestamp(bson::Timestamp { time: 1, increment: 1 }),
            Bson::Int64(1),
            Bson::Decimal128("1".parse().unwrap()),
            Bson::MinKey,
            Bson::MaxKey,
            Bson::Symbol("s".into()),
        ];
        let mut seen = std::collections::BTreeSet::new();
        for value in &samples {
            let name = type_name_of(value);
            seen.insert(name);
            let filter = parse(&doc! { "a": { "$type": name } })
                .unwrap_or_else(|e| panic!("{name} must parse: {e}"));
            assert!(
                matches(&filter, &doc! { "a": value.clone() }).unwrap(),
                "{name} must match itself"
            );
        }
        // `dbPointer` cannot be built through the public API; it is in the
        // table all the same, and parses.
        seen.insert("dbPointer");
        assert!(parse(&doc! { "a": { "$type": "dbPointer" } }).is_ok());
        let table: std::collections::BTreeSet<&str> = TYPE_NAMES.iter().copied().collect();
        assert_eq!(seen, table, "the alias table and the names values report must be one set");
        // And every name a code spells is in it.
        for code in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 16, 17, 18, 19, -1, 127] {
            let name = type_name_for_code(code).unwrap();
            assert!(table.contains(name.as_str()), "{name}");
        }
    }

    #[test]
    fn number_matches_the_four_numeric_types_and_nothing_else() {
        let q = doc! { "a": { "$type": "number" } };
        for numeric in [
            Bson::Int32(4),
            Bson::Int64(9_007_199_254_740_993),
            Bson::Double(2.5),
            Bson::Decimal128("1.25".parse().unwrap()),
        ] {
            assert!(hits(q.clone(), doc! { "a": numeric.clone() }), "{numeric:?}");
        }
        for other in [
            Bson::String("many".into()),
            Bson::Boolean(true),
            Bson::Null,
            Bson::Document(doc! { "n": 1 }),
        ] {
            assert!(!hits(q.clone(), doc! { "a": other.clone() }), "{other:?}");
        }
        // One level into an array, like every other `$type`: an element.
        assert!(hits(q.clone(), doc! { "a": [3, "boxed"] }));
        assert!(!hits(q.clone(), doc! { "a": [["deeper", 3]] }));
        assert!(!hits(q, doc! { "a": ["x", "y"] }));
    }

    #[test]
    fn number_is_the_same_filter_as_listing_the_four() {
        let by_alias = parse(&doc! { "a": { "$type": "number" } }).unwrap();
        let listed =
            parse(&doc! { "a": { "$type": ["int", "long", "double", "decimal"] } }).unwrap();
        for value in [
            Bson::Int32(1),
            Bson::Int64(1),
            Bson::Double(1.0),
            Bson::Decimal128("1".parse().unwrap()),
            Bson::String("1".into()),
            Bson::Boolean(false),
        ] {
            let d = doc! { "a": value };
            assert_eq!(matches(&by_alias, &d).unwrap(), matches(&listed, &d).unwrap(), "{d:?}");
        }
    }

    #[test]
    fn number_mixes_with_other_aliases_and_codes() {
        let q = doc! { "a": { "$type": ["number", "string"] } };
        assert!(hits(q.clone(), doc! { "a": 1 }));
        assert!(hits(q.clone(), doc! { "a": "x" }));
        assert!(!hits(q, doc! { "a": true }));
        // A code and the alias in one list.
        let q = doc! { "a": { "$type": [8, "number"] } };
        assert!(hits(q.clone(), doc! { "a": true }));
        assert!(hits(q, doc! { "a": 2.5 }));
        // Naming one of the four still means that one.
        assert!(!hits(doc! { "a": { "$type": "int" } }, doc! { "a": 1i64 }));
        assert!(!hits(doc! { "a": { "$type": "bool" } }, doc! { "a": 1 }));
    }

    #[test]
    fn an_unknown_regex_flag_is_refused_naming_it() {
        for flags in ["I", "g", "ix ", "u", "l"] {
            let message = refused(doc! { "s": { "$regex": "s", "$options": flags } });
            assert!(message.contains("flag"), "{message}");
        }
        assert!(refused(doc! { "s": { "$regex": "s", "$options": "iI" } }).contains("'I'"));
        // The literal's own flags, and a lone `$options`, are checked too.
        refused(doc! { "s": { "$regex": literal("s", "g") } });
        refused(doc! { "s": { "$options": "I" } });
        refused(doc! { "s": { "$not": literal("s", "g") } });
    }

    #[test]
    fn the_four_regex_flags_and_none_are_accepted() {
        assert!(hits(doc! { "s": { "$regex": "^AB", "$options": "i" } }, doc! { "s": "abc" }));
        assert!(hits(doc! { "s": { "$regex": "a b", "$options": "x" } }, doc! { "s": "ab" }));
        assert!(hits(doc! { "s": { "$regex": "^b", "$options": "m" } }, doc! { "s": "a\nb" }));
        assert!(hits(doc! { "s": { "$regex": "a.b", "$options": "s" } }, doc! { "s": "a\nb" }));
        assert!(hits(doc! { "s": { "$regex": "^AB", "$options": "imsx" } }, doc! { "s": "abc" }));
        assert!(hits(doc! { "s": { "$regex": "^ab", "$options": "" } }, doc! { "s": "abc" }));
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
    fn an_invalid_regex_is_refused_naming_the_fault_in_every_place_one_is_written() {
        // A string pattern, a literal, a literal under `$not`, and each with
        // the kind of pattern the engine cannot use: an unclosed group, and a
        // backreference and a lookahead, which it does not support.
        for pattern in ["(unclosed", r"(a)\1", "a(?=b)"] {
            let message = refused(doc! { "s": { "$regex": pattern } });
            assert!(message.contains(&format!("{pattern:?}")), "{message}");
            refused(doc! { "s": { "$regex": pattern, "$options": "i" } });
            refused(doc! { "s": { "$regex": literal(pattern, "") } });
            refused(doc! { "s": { "$regex": literal(pattern, "i") } });
            refused(doc! { "s": { "$not": literal(pattern, "") } });
        }
        // The reason is in the message, and only its last line.
        let message = refused(doc! { "s": { "$regex": "(unclosed" } });
        assert!(message.contains("unclosed group") && !message.contains('\n'), "{message}");
        // A pattern that compiles is untouched.
        assert!(hits(doc! { "s": { "$regex": "^a(b|c)+$" } }, doc! { "s": "abcb" }));
    }

    #[test]
    fn a_regex_among_the_values_of_in_nin_or_all_is_refused() {
        let shapes: Vec<Bson> = vec![
            literal("abc", ""),
            literal("^a", "i"),
            Bson::Document(doc! { "$regex": "abc" }),
            Bson::Document(doc! { "$regex": "abc", "$options": "i" }),
            Bson::Document(doc! { "$options": "i", "$regex": literal("abc", "") }),
            Bson::Document(doc! { "$regularExpression": { "pattern": "abc", "options": "" } }),
        ];
        for op in ["$in", "$nin", "$all"] {
            for shape in &shapes {
                // Alone, beside an ordinary value, under `$not` and inside an
                // `$elemMatch`: every place the operator can be written.
                for q in [
                    doc! { "s": { op: [shape.clone()] } },
                    doc! { "s": { op: ["plain", shape.clone()] } },
                    doc! { "s": { "$not": { op: [shape.clone()] } } },
                    doc! { "s": { "$elemMatch": { op: [shape.clone()] } } },
                ] {
                    let message = refused(q.clone());
                    assert!(
                        message.starts_with(&format!("{op} cannot hold a regular expression"))
                            && message.contains("write an $or of $regex clauses"),
                        "{q}: {message}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_regex_as_the_operand_of_ne_is_refused() {
        for shape in [
            literal("abc", ""),
            literal("^a", "i"),
            Bson::Document(doc! { "$regex": "abc" }),
            Bson::Document(doc! { "$regex": "abc", "$options": "i" }),
            Bson::Document(doc! { "$regularExpression": { "pattern": "abc", "options": "" } }),
        ] {
            for q in [
                doc! { "s": { "$ne": shape.clone() } },
                doc! { "s": { "$not": { "$ne": shape.clone() } } },
                doc! { "s": { "$elemMatch": { "$ne": shape.clone() } } },
            ] {
                let message = refused(q.clone());
                assert!(
                    message.starts_with("$ne cannot take a regular expression")
                        && message.contains("$not"),
                    "{q}: {message}"
                );
            }
        }
        // Any other document is still a value to `$ne`, and `$eq` still
        // compares a regex as a value.
        let d = doc! { "s": { "$regex": "x", "other": 1 }, "r": literal("abc", "") };
        assert!(!hits(doc! { "s": { "$ne": { "$regex": "x", "other": 1 } } }, d.clone()));
        assert!(hits(doc! { "s": { "$ne": { "a": 1 } } }, d.clone()));
        assert!(hits(doc! { "r": { "$eq": literal("abc", "") } }, d));
    }

    #[test]
    fn an_elem_match_among_the_values_of_all_is_refused() {
        for q in [
            doc! { "items": { "$all": [ { "$elemMatch": { "qty": 5 } } ] } },
            doc! { "items": { "$all": [ { "$elemMatch": { "$eq": "q" } } ] } },
            doc! { "items": { "$all": [ { "$elemMatch": { "qty": 5 } }, { "$elemMatch": { "sku": "a" } } ] } },
            doc! { "items": { "$all": [ "plain", { "$elemMatch": { "qty": 5 } } ] } },
            doc! { "items": { "$not": { "$all": [ { "$elemMatch": { "qty": 5 } } ] } } },
        ] {
            let message = refused(q.clone());
            assert!(
                message.starts_with("$all cannot hold an $elemMatch")
                    && message.contains("write an $and of $elemMatch clauses"),
                "{q}: {message}"
            );
        }
        // What the message points to answers it; a document with another key
        // beside `$elemMatch` is still a value.
        let d = doc! { "items": [ { "qty": 5, "sku": "b" }, { "qty": 1, "sku": "a" } ] };
        assert!(hits(
            doc! { "$and": [
                { "items": { "$elemMatch": { "qty": 5 } } },
                { "items": { "$elemMatch": { "sku": "a" } } },
            ] },
            d.clone()
        ));
        let odd = doc! { "items": [ { "$elemMatch": { "qty": 5 }, "x": 1 } ] };
        assert!(hits(
            doc! { "items": { "$all": [ { "$elemMatch": { "qty": 5 }, "x": 1 } ] } },
            odd
        ));
    }

    #[test]
    fn in_still_compares_documents_that_are_not_regex_shaped() {
        // A stored document may hold a key beginning with `$`; only the exact
        // regex shapes are refused.
        let d =
            doc! { "s": { "$regex": "x", "other": 1 }, "t": { "$options": "i" }, "u": { "a": 1 } };
        for q in [
            doc! { "s": { "$in": [ { "$regex": "x", "other": 1 } ] } },
            doc! { "t": { "$in": [ { "$options": "i" } ] } },
            doc! { "u": { "$in": [ { "a": 1 }, "abc" ] } },
            doc! { "u": { "$all": [ { "a": 1 } ] } },
        ] {
            assert!(hits(q.clone(), d.clone()), "{q} must match {d}");
        }
        assert!(!hits(doc! { "u": { "$nin": [ { "a": 1 } ] } }, d.clone()));
        // A regex is still an ordinary value to `$eq`, as it is written directly
        // as a field's value.
        assert!(hits(
            doc! { "r": { "$eq": literal("abc", "") } },
            doc! { "r": literal("abc", "") }
        ));
    }

    #[test]
    fn the_pattern_itself_refuses_an_unknown_flag_naming_it() {
        // `RegexPattern::new` is the check a `$regex` meets, whether the flag
        // came from `$options` or a literal; a lone `$options` has its own.
        for flag in ['z', 'I', 'g', 'u'] {
            match RegexPattern::new("a", &format!("i{flag}")) {
                Err(Error::InvalidQuery(message)) => assert_eq!(
                    message,
                    format!("unknown $regex flag {flag:?}; the flags are i, m, s and x")
                ),
                other => panic!("flag {flag:?} must be refused, got {other:?}"),
            }
        }
    }

    /// `n` distinct `$regex` clauses on `s` under one `$or`.
    fn or_of(n: usize, pattern: impl Fn(usize) -> String) -> Document {
        let clauses: Vec<Bson> =
            (0..n).map(|i| Bson::Document(doc! { "s": { "$regex": pattern(i) } })).collect();
        doc! { "$or": clauses }
    }

    #[test]
    fn a_pattern_compiling_past_the_one_pattern_limit_is_refused_naming_it() {
        // `\w` is a Unicode class of some 55 KiB compiled; twenty of them fit
        // in 1 MiB and a hundred do not. `regex`'s own limit, 10 MiB, let
        // `\w{209}` through at 11 MiB held for the life of the request.
        for pattern in [r"\w{100}", r"\w{209}", r"(?i)\p{L}{30}"] {
            let message = refused(doc! { "s": { "$regex": pattern } });
            assert!(
                message.contains(&format!("$regex pattern {pattern:?} cannot be used"))
                    && message.contains("more than 1 MiB, the limit for one pattern"),
                "{message}"
            );
        }
        for pattern in [r"\w{20}", "[a-z]{50}", r"^\w+@\w+\.com$"] {
            parse(&doc! { "s": { "$regex": pattern, "$options": "i" } }).unwrap();
        }
    }

    #[test]
    fn one_filter_holds_at_most_thirty_two_regex_conditions() {
        parse(&or_of(REGEX_CONDITIONS_LIMIT, |i| format!("^a{i}"))).unwrap();
        let message = refused(or_of(REGEX_CONDITIONS_LIMIT + 1, |i| format!("^a{i}")));
        assert!(message.contains("at most 32 $regex conditions"), "{message}");
        // Wherever they are written: under `$not` and inside `$elemMatch` too.
        let mut q = or_of(REGEX_CONDITIONS_LIMIT, |i| format!("^a{i}"));
        q.insert("t", doc! { "$not": literal("x", "") });
        refused(q);
        let mut q = or_of(REGEX_CONDITIONS_LIMIT, |i| format!("^a{i}"));
        q.insert("u", doc! { "$elemMatch": { "v": { "$regex": "y" } } });
        refused(q);
        // The budget is the filter's, not the thread's: the next one starts
        // from nothing, and a pattern built outside a parse is not counted.
        parse(&or_of(REGEX_CONDITIONS_LIMIT, |i| format!("^a{i}"))).unwrap();
        for i in 0..=REGEX_CONDITIONS_LIMIT {
            RegexPattern::new(&format!("^a{i}"), "").unwrap();
        }
    }

    #[test]
    fn one_filter_holds_at_most_two_mib_of_compiled_patterns() {
        // Each `\w{20}` compiles to just over 1 MiB: one fits, two do not.
        // Each `\w{12}` is about 0.64 MiB: three fit, four do not.
        let pattern = |i: usize| format!(r"\w{{20}}{i}");
        parse(&or_of(1, pattern)).unwrap();
        let message = refused(or_of(2, pattern));
        assert!(
            message.contains("compile to more than 2 MiB together, the limit for one filter")
                && message.contains(r#""\\w{20}1" is the one past it"#),
            "{message}"
        );
        let pattern = |i: usize| format!(r"\w{{12}}{i}");
        parse(&or_of(3, pattern)).unwrap();
        refused(or_of(4, pattern));
        // The reviewer's shape: forty patterns of 11 MiB each, 446 MiB held,
        // from a 1.4 KB filter. Refused at the first.
        refused(or_of(40, |_| r"\w{209}".to_string()));
    }

    #[test]
    fn a_regex_condition_cannot_be_built_with_an_unchecked_flag_or_pattern() {
        // `Condition` is a public type, and a regex condition built by hand
        // used to carry an unchecked flag to the matcher, which had to read it
        // as a no-match. Its pattern is now built only through the same check
        // the parser makes.
        for (pattern, options) in [("a", "z"), ("a", "I"), ("(unclosed", ""), ("(?=a)", "")] {
            assert!(
                matches!(RegexPattern::new(pattern, options), Err(Error::InvalidQuery(_))),
                "{pattern:?} under {options:?} must be refused"
            );
        }
        let re = RegexPattern::new("^a", "i").unwrap();
        let filter = Filter::Field { path: "s".into(), conditions: vec![Condition::Regex(re)] };
        assert!(matches(&filter, &doc! { "s": "Abc" }).unwrap());
    }

    #[test]
    fn regex_conditions_compare_by_pattern_and_flags() {
        let re = |p: &str, o: &str| RegexPattern::new(p, o).unwrap();
        assert_eq!(re("^a", "i"), re("^a", "i"));
        assert_ne!(re("^a", "i"), re("^a", ""));
        assert_ne!(re("^a", "i"), re("^b", "i"));
        // One filter parsed twice is equal to itself, compiled programs and all.
        let q = doc! { "s": { "$regex": literal("^a", "i") } };
        assert_eq!(parse(&q).unwrap(), parse(&q).unwrap());
        assert_ne!(
            parse(&q).unwrap(),
            parse(&doc! { "s": { "$regex": literal("^a", "") } }).unwrap()
        );
        // A clone shares the compiled program and still matches.
        let cloned = re("^a", "i").clone();
        assert!(cloned.is_match("AB") && cloned.pattern() == "^a" && cloned.options() == "i");
        // The debug form names the pattern and flags, not the program.
        assert_eq!(
            format!("{:?}", re("^a", "i")),
            r#"RegexPattern { pattern: "^a", options: "i" }"#
        );
    }

    #[test]
    fn a_regex_is_compiled_once_at_parse_and_never_while_matching() {
        // Every shape a `$regex` reaches the matcher in: on a field, on a
        // nested path through an array, beside `$options`, as a literal, under
        // `$not`, inside an `$elemMatch` over documents and over scalars, and
        // in an `$or` branch.
        let queries = [
            doc! { "s": { "$regex": "^a.*z$" } },
            doc! { "s": { "$regex": "^A", "$options": "i" } },
            doc! { "s": { "$regex": literal("b+", "") } },
            doc! { "tags": { "$regex": "^t[0-3]$" } },
            doc! { "items.name": { "$regex": "x$" } },
            doc! { "s": { "$not": literal("^a", "") } },
            doc! { "s": { "$not": { "$regex": "^a", "$options": "i" } } },
            doc! { "items": { "$elemMatch": { "name": { "$regex": "^n" } } } },
            doc! { "tags": { "$elemMatch": { "$regex": "1$" } } },
            doc! { "$or": [ { "s": { "$regex": "q" } }, { "n": 3 } ] },
        ];
        let docs: Vec<Document> = (0..200)
            .map(|i| {
                doc! {
                    "_id": i,
                    "s": if i % 2 == 0 { format!("a{i}z") } else { format!("b{i}") },
                    "n": i % 5,
                    "tags": ["x", format!("t{}", i % 7)],
                    "items": [ { "name": format!("n{i}x") }, { "name": "other" } ],
                }
            })
            .collect();
        for q in queries {
            let before = regex_compilations();
            let filter = parse(&q).unwrap();
            assert_eq!(regex_compilations() - before, 1, "{q} compiles its pattern once at parse");

            let before = regex_compilations();
            let mut hits = 0;
            for d in &docs {
                hits += usize::from(matches(&filter, d).unwrap());
            }
            // Cloning a filter compiles nothing either.
            let copy = filter.clone();
            hits += usize::from(matches(&copy, &docs[0]).unwrap());
            assert_eq!(
                regex_compilations() - before,
                0,
                "{q} compiled a regex while matching {} documents",
                docs.len()
            );
            assert!(hits > 0, "premise: {q} matched something");
        }
    }

    #[test]
    fn options_must_be_a_string() {
        for options in [Bson::Int32(1), Bson::Boolean(true), Bson::Null, Bson::Array(vec![])] {
            let message = refused(doc! { "s": { "$regex": "a", "$options": options.clone() } });
            assert!(message.contains("$options requires a string"), "{message}");
            // Alone, and beside a literal, it is refused the same way.
            refused(doc! { "s": { "$options": options.clone() } });
            refused(doc! { "s": { "$regex": literal("a", ""), "$options": options } });
        }
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

    /// The evaluation error for `query` against `d`, as every caller gets it.
    fn unevaluable(query: Document, d: Document) -> String {
        let filter = parse(&query).unwrap_or_else(|e| panic!("parse failed: {e}"));
        match matches(&filter, &d) {
            Ok(answer) => panic!("{query} on {d} answered {answer}, expected an error"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn expr_fails_on_a_type_violation_rather_than_not_matching() {
        // Adding to a string has no value, so the clause has no truth value:
        // read as false, the document looked like one that did not qualify.
        let q = doc! { "$expr": { "$gt": [ { "$add": ["$qty", 1] }, 10 ] } };
        assert_eq!(
            unevaluable(q.clone(), doc! { "_id": 3, "qty": "twelve" }),
            "invalid query: $expr cannot be evaluated for the document with _id 3: $add needs \
             numbers, found a string"
        );
        // Without an `_id` (a pipeline's derived document) it says no more.
        assert_eq!(
            unevaluable(q.clone(), doc! { "qty": "twelve" }),
            "invalid query: $expr cannot be evaluated: $add needs numbers, found a string"
        );
        // A clean false is still a document that does not match, and null
        // still propagates through arithmetic rather than failing.
        let filter = parse(&q).unwrap();
        assert!(!matches(&filter, &doc! { "_id": 1, "qty": 5 }).unwrap());
        assert!(matches(&filter, &doc! { "_id": 2, "qty": 12 }).unwrap());
        assert!(!matches(&filter, &doc! { "_id": 4 }).unwrap());
        assert!(!matches(&filter, &doc! { "_id": 5, "qty": Bson::Null }).unwrap());
    }

    #[test]
    fn expr_errors_wherever_the_evaluation_reaches_it() {
        let bad = doc! { "$add": ["$qty", 1] };
        let d = || doc! { "_id": 1, "status": "open", "qty": "x" };
        for q in [
            doc! { "$expr": { "$gt": [bad.clone(), 0] } },
            doc! { "status": "open", "$expr": { "$gt": [bad.clone(), 0] } },
            doc! { "$or": [ { "status": "closed" }, { "$expr": { "$gt": [bad.clone(), 0] } } ] },
            doc! { "$nor": [ { "$expr": { "$gt": [bad.clone(), 0] } } ] },
            doc! { "$and": [ { "$expr": { "$gt": [bad.clone(), 0] } } ] },
            // `$cond` evaluates only the branch it takes; this one takes it.
            doc! { "$expr": { "$cond": [true, bad.clone(), false] } },
        ] {
            let err = unevaluable(q.clone(), d());
            assert!(err.contains("$add needs numbers, found a string"), "{q}: {err}");
        }
        // Inside `$elemMatch` the expression reads the element, and an element
        // it cannot be evaluated against fails the request the same way.
        let q = doc! { "lines": { "$elemMatch": { "$expr": { "$gt": [bad.clone(), 0] } } } };
        let err = unevaluable(q, doc! { "_id": 2, "lines": [ { "qty": -5 }, { "qty": "x" } ] });
        assert!(err.contains("_id 2") && err.contains("$add needs numbers"), "{err}");
        let q =
            doc! { "a": { "$not": { "$elemMatch": { "$expr": { "$gt": [bad.clone(), 0] } } } } };
        assert!(unevaluable(q, doc! { "a": [ { "qty": "x" } ] }).contains("$add"));
    }

    #[test]
    fn a_clause_that_decides_the_answer_spares_an_expression_that_cannot_be_evaluated() {
        // A document fails only when its answer depends on the expression: a
        // false clause of an `$and`, or a true branch of an `$or`, decides it
        // whatever the expression would have said, and wherever it is written.
        let bad = doc! { "$gt": [ { "$add": ["$qty", 1] }, 0 ] };
        let d = doc! { "_id": 1, "status": "open", "qty": "x" };
        let answer = |q: Document| matches(&parse(&q).unwrap(), &d).unwrap();
        for (q, expected) in [
            (doc! { "status": "closed", "$expr": bad.clone() }, false),
            (doc! { "$expr": bad.clone(), "status": "closed" }, false),
            (doc! { "$and": [ { "$expr": bad.clone() }, { "status": "closed" } ] }, false),
            (doc! { "$or": [ { "status": "open" }, { "$expr": bad.clone() } ] }, true),
            (doc! { "$or": [ { "$expr": bad.clone() }, { "status": "open" } ] }, true),
            (doc! { "$nor": [ { "$expr": bad.clone() }, { "status": "open" } ] }, false),
            // Between two expressions, the one that can be evaluated decides.
            (doc! { "$and": [ { "$expr": bad.clone() }, { "$expr": false } ] }, false),
            (doc! { "$or": [ { "$expr": bad.clone() }, { "$expr": true } ] }, true),
            // `$cond` evaluates only the branch it takes.
            (doc! { "$expr": { "$cond": [false, { "$add": ["$qty", 1] }, false] } }, false),
        ] {
            assert_eq!(answer(q.clone()), expected, "{q}");
        }
        // An element that matches decides an `$elemMatch`, whichever element
        // could not be evaluated, and in either order.
        let q = doc! { "lines": { "$elemMatch": { "$expr": bad.clone() } } };
        let filter = parse(&q).unwrap();
        for lines in [
            bson::bson!([{ "qty": "x" }, { "qty": 5 }]),
            bson::bson!([{ "qty": 5 }, { "qty": "x" }]),
        ] {
            assert!(matches(&filter, &doc! { "lines": lines.clone() }).unwrap(), "{lines}");
        }
        // Inside `$elemMatch` too, a cheap condition decides first.
        let q = doc! { "lines": { "$elemMatch": { "$expr": bad.clone(), "sku": "z" } } };
        let d = doc! { "lines": [ { "sku": "a", "qty": "x" } ] };
        assert!(!matches(&parse(&q).unwrap(), &d).unwrap());
        // An element a scalar `$elemMatch` reads has no fields: nothing is
        // evaluated, so nothing fails.
        let q = doc! { "lines": { "$elemMatch": { "$expr": bad } } };
        assert!(!matches(&parse(&q).unwrap(), &doc! { "lines": [1, 2] }).unwrap());
    }

    #[test]
    fn an_expression_that_decides_nothing_alone_still_fails_the_document() {
        let bad = doc! { "$gt": [ { "$add": ["$qty", 1] }, 0 ] };
        let d = || doc! { "_id": 1, "status": "open", "qty": "x" };
        for q in [
            doc! { "$and": [ { "$expr": bad.clone() }, { "$expr": true } ] },
            doc! { "$or": [ { "$expr": bad.clone() }, { "$expr": false } ] },
            doc! { "$expr": bad.clone(), "status": "open" },
            doc! { "$nor": [ { "status": "closed" }, { "$expr": bad.clone() } ] },
        ] {
            assert!(unevaluable(q.clone(), d()).contains("$add needs numbers"), "{q}");
        }
    }

    #[test]
    fn clauses_without_an_expression_are_evaluated_first() {
        // Not only the answer but the work: where a clause without `$expr`
        // decides, the expression is never evaluated, wherever it is written.
        // Counted on parsed filters, so the parser's ordering is what is held.
        let gt = doc! { "$gt": ["$qty", 1] };
        let d = doc! { "_id": 1, "kind": "a", "qty": 5, "lines": [ { "sku": "a", "qty": 5 } ] };
        for (q, expected) in [
            (doc! { "$expr": gt.clone(), "kind": "c" }, false),
            (doc! { "$and": [ { "$expr": gt.clone() }, { "kind": "c" } ] }, false),
            (doc! { "$or": [ { "$expr": gt.clone() }, { "kind": "a" } ] }, true),
            (doc! { "$nor": [ { "$expr": gt.clone() }, { "kind": "a" } ] }, false),
            // A field's conditions: the `$elemMatch` holding the expression
            // goes after `$size`, which decides.
            (doc! { "lines": { "$elemMatch": { "$expr": gt.clone() }, "$size": 0 } }, false),
            // An `$elemMatch` body.
            (doc! { "lines": { "$elemMatch": { "$expr": gt.clone(), "sku": "z" } } }, false),
        ] {
            let filter = parse(&q).unwrap();
            let before = expr_evaluations();
            assert_eq!(matches(&filter, &d).unwrap(), expected, "{q}");
            assert_eq!(expr_evaluations() - before, 0, "{q} evaluated its expression");
        }
        // Where nothing cheaper decides, the expression is evaluated, once.
        let filter = parse(&doc! { "$expr": gt, "kind": "a" }).unwrap();
        let before = expr_evaluations();
        assert!(matches(&filter, &d).unwrap());
        assert_eq!(expr_evaluations() - before, 1);
    }

    #[test]
    fn an_element_filter_fails_on_an_expression_it_cannot_evaluate() {
        // An `arrayFilters` entry's `$expr` reads the element as `$$line`; one it
        // cannot evaluate is an error, not "not selected", naming the document
        // the element is in.
        let filter = parse_with_vars(
            &doc! { "$expr": { "$gt": [ { "$add": ["$$line.qty", 1] }, 0 ] } },
            &["line".to_string()],
        )
        .unwrap();
        let id = Bson::Int32(7);
        let err = matches_element(&filter, "line", &Bson::Document(doc! { "qty": "x" }), Some(&id))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid query: $expr cannot be evaluated for an array element of the document with \
             _id 7: $add needs numbers, found a string"
        );
        let err = matches_element(&filter, "line", &Bson::Document(doc! { "qty": "x" }), None)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid query: $expr cannot be evaluated for an array element: $add needs numbers, \
             found a string"
        );
        assert!(
            matches_element(&filter, "line", &Bson::Document(doc! { "qty": 1 }), None).unwrap()
        );
        // A scalar element is evaluated like any other: it has no `qty`, which
        // is null, and null does not exceed zero.
        assert!(!matches_element(&filter, "line", &Bson::Int32(1), None).unwrap());
    }

    #[test]
    fn an_element_filter_does_not_evaluate_an_expression_a_cheaper_clause_decides() {
        // The same ordering the document filters have (ADR-206), counted on an
        // entry's parsed filter: `kind` is stripped of its identifier, as the
        // update parser leaves it.
        let vars = ["line".to_string()];
        let gt = doc! { "$gt": ["$$line.qty", 1] };
        let element = Bson::Document(doc! { "kind": "b", "qty": 5 });
        for entry in [
            doc! { "$expr": gt.clone(), "kind": "a" },
            doc! { "$and": [ { "$expr": gt.clone() }, { "kind": "a" } ] },
            doc! { "$nor": [ { "$expr": gt.clone() }, { "kind": "b" } ] },
        ] {
            let filter = parse_with_vars(&entry, &vars).unwrap();
            let before = expr_evaluations();
            assert!(!matches_element(&filter, "line", &element, None).unwrap(), "{entry}");
            assert_eq!(expr_evaluations() - before, 0, "{entry} evaluated its expression");
        }
        // Where nothing cheaper decides, it is evaluated, once.
        let filter = parse_with_vars(&doc! { "$expr": gt, "kind": "b" }, &vars).unwrap();
        let before = expr_evaluations();
        assert!(matches_element(&filter, "line", &element, None).unwrap());
        assert_eq!(expr_evaluations() - before, 1);
    }

    #[test]
    fn expr_parse_errors_are_the_filter_parsers_errors() {
        // An unknown expression operator and a wrong arity both surface as the
        // filter parser's own error variants, not as a distinct kind.
        assert!(matches!(
            parse(&doc! { "$expr": { "$nope": 1 } }),
            Err(Error::UnsupportedOperator { .. })
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
        assert!(matches(&by_type, &stored).unwrap());
        let exists = parse(&doc! { "v": { "$exists": true } }).unwrap();
        assert!(matches(&exists, &stored).unwrap());
        assert!(parse(&doc! { "v": { "$eq": 1.5 } }).is_ok(), "a double compares as ever");
    }
}
