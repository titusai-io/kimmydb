//! The small pipeline stages (ADR-263): `$sortByCount`, `$unset`, `$sample`, `$redact`.
//!
//! `$replaceWith` is `$replaceRoot` under another name and `$unionWith` is run by the
//! caller (it reads a second collection), so both live in [`crate::aggregate`]. What is
//! here is the parsing of the other four and the code that runs them.

use bson::{Bson, Document};
use kimmy_core::{Error, Result};

use crate::aggregate::{Accumulator, Limits, Stage, as_document, group_keyed};
use crate::expr::{Binding, Expr, REDACT_DESCEND, REDACT_KEEP, REDACT_PRUNE, Scope, type_name};
use crate::shape::{self, SortKey};

// ---------------------------------------------------------------------------
// $sortByCount
// ---------------------------------------------------------------------------

/// `{$sortByCount: <expression>}`: a field path (`"$a"`, `"$$ROOT"`) or an expression
/// document that is not empty. A number, a boolean, an array, `null`, a string that is
/// not a path and an empty document are refused.
pub(crate) fn parse_sort_by_count(value: &Bson, vars: &[String]) -> Result<Stage> {
    let ok = match value {
        Bson::String(s) => s.len() > 1 && s.starts_with('$'),
        Bson::Document(d) => !d.is_empty(),
        _ => false,
    };
    if !ok {
        return Err(Error::InvalidQuery(format!(
            "$sortByCount takes a field path such as \"$field\" or an expression document, \
             found {}",
            match value {
                Bson::String(s) => format!("the string {s:?}"),
                Bson::Document(_) => "an empty document".to_string(),
                other => type_name(other).to_string(),
            }
        )));
    }
    Ok(Stage::SortByCount(Expr::parse_with_vars(value, vars)?))
}

/// Run `$sortByCount`: `{$group: {_id: <expr>, count: {$sum: 1}}}` then
/// `{$sort: {count: -1}}`. The groups come out in the order their keys were first seen
/// and the sort is stable, so groups with the same count stay in that order.
pub(crate) fn sort_by_count(
    input: Vec<Document>,
    expr: &Expr,
    limits: &Limits,
    vars: &[Binding<'_>],
) -> Result<Vec<Document>> {
    let fields = [("count".to_string(), Accumulator::Sum(Expr::Literal(Bson::Int32(1))))];
    let mut groups = group_keyed("$sortByCount", input, &fields, limits, vars, |scope| {
        expr.eval_in(scope).map(Some)
    })?;
    shape::sort(&[SortKey { path: "count".into(), descending: true }], &mut groups);
    Ok(groups)
}

// ---------------------------------------------------------------------------
// $unset
// ---------------------------------------------------------------------------

/// `{$unset: "a"}` or `{$unset: ["a", "b.c"]}`: a `$project` that excludes those paths.
///
/// Refused: an empty array, an element that is not a string, an empty path or a path
/// with an empty or `$`-leading segment, the same path twice, and one path inside
/// another (`"a"` with `"a.b"`).
pub(crate) fn parse_unset(value: &Bson) -> Result<Stage> {
    let paths: Vec<&str> = match value {
        Bson::String(path) => vec![path.as_str()],
        Bson::Array(items) => {
            if items.is_empty() {
                return Err(Error::InvalidQuery(
                    "$unset needs at least one path: an array of field names".into(),
                ));
            }
            items
                .iter()
                .map(|item| match item {
                    Bson::String(path) => Ok(path.as_str()),
                    other => Err(Error::InvalidQuery(format!(
                        "$unset takes a field name or an array of field names, found {} in the \
                         array",
                        type_name(other)
                    ))),
                })
                .collect::<Result<_>>()?
        }
        other => {
            return Err(Error::InvalidQuery(format!(
                "$unset takes a field name or an array of field names, found {}",
                type_name(other)
            )));
        }
    };
    for path in &paths {
        if path.is_empty() || path.split('.').any(|seg| seg.is_empty() || seg.starts_with('$')) {
            return Err(Error::InvalidQuery(format!(
                "$unset paths are field names whose parts are not empty and do not start with \
                 `$`, found {path:?}"
            )));
        }
    }
    for (at, a) in paths.iter().enumerate() {
        for b in &paths[at + 1..] {
            let inside = |long: &str, short: &str| {
                long.len() > short.len()
                    && long.starts_with(short)
                    && long.as_bytes()[short.len()] == b'.'
            };
            if a == b || inside(a, b) || inside(b, a) {
                return Err(Error::InvalidQuery(format!(
                    "$unset names {a:?} and {b:?}, which are the same field or one inside the \
                     other"
                )));
            }
        }
    }
    let mut spec = Document::new();
    for path in paths {
        spec.insert(path, 0);
    }
    Ok(Stage::Project { projection: shape::parse_projection(&spec)?, computed: Vec::new() })
}

// ---------------------------------------------------------------------------
// $sample
// ---------------------------------------------------------------------------

/// `{$sample: {size: N}}`: `N` a positive whole number (an int, a long, a double or
/// decimal with no fraction). A missing `size`, any other key, `0`, a negative, a
/// fraction and a non-number are refused.
pub(crate) fn parse_sample(value: &Bson) -> Result<Stage> {
    let spec = as_document("$sample", value)?;
    for key in spec.keys() {
        if key != "size" {
            return Err(Error::InvalidQuery(format!(
                "$sample does not accept `{key}`; it takes: size"
            )));
        }
    }
    let Some(size) = spec.get("size") else {
        return Err(Error::InvalidQuery("$sample needs a `size`: {$sample: {size: N}}".into()));
    };
    let refused = || {
        Error::InvalidQuery(format!(
            "$sample `size` is a positive whole number, found {}",
            match size {
                Bson::Int32(n) => n.to_string(),
                Bson::Int64(n) => n.to_string(),
                Bson::Double(n) => n.to_string(),
                other => type_name(other).to_string(),
            }
        ))
    };
    let n = match size {
        Bson::Int32(n) => i64::from(*n),
        Bson::Int64(n) => *n,
        // Strictly inside the range an `i64` holds exactly, so the cast cannot saturate.
        Bson::Double(d) if d.fract() == 0.0 && d.abs() < 9.0e18 => *d as i64,
        Bson::Decimal128(d) => kimmy_core::decimal::whole_number(d).ok_or_else(refused)?,
        _ => return Err(refused()),
    };
    if n < 1 {
        return Err(refused());
    }
    Ok(Stage::Sample(n as usize))
}

/// A source of random positions: `below(n)` answers a number in `0..n`, `n > 0`.
pub type Below<'a> = &'a mut dyn FnMut(usize) -> usize;

/// The system's source for [`sample`]: a generator seeded from the operating system's,
/// fresh for each run of the stage, so no two runs share a seed.
pub fn system_below() -> impl FnMut(usize) -> usize {
    use rand::RngExt;
    let mut rng: rand::rngs::StdRng = rand::make_rng();
    move |n| rng.random_range(0..n)
}

/// `size` documents of `input` chosen uniformly at random, by reservoir sampling
/// (algorithm R): the first `size` fill the reservoir and document number `i` (from 0)
/// then replaces a random slot with probability `size / (i + 1)`. Every subset of `size`
/// documents is equally likely.
///
/// **Order**: the reservoir's slot order, which is the input order for the documents that
/// were never replaced. With `size >= input.len()` that is the whole input, in input
/// order, as nothing is replaced.
///
/// **Bounds, refused and never cut short**: `size` over the pipeline's document limit is
/// `Error::Limit`, and the reservoir's encoded bytes passing
/// [`kimmy_core::limits::MAX_DOCUMENT_BYTES`] (the cap `$facet`'s answer is held to) is
/// `Error::AnswerTooLarge`, raised the moment a document pushes it over.
pub fn sample(
    input: Vec<Document>,
    size: usize,
    limits: &Limits,
    below: Below<'_>,
) -> Result<Vec<Document>> {
    if size > limits.max_documents {
        return Err(Error::Limit(format!(
            "$sample size {size} is over the pipeline limit of {} documents",
            limits.max_documents
        )));
    }
    let mut reservoir: Vec<(Document, usize)> = Vec::with_capacity(size.min(input.len()));
    let mut bytes = 0usize;
    for (seen, doc) in input.into_iter().enumerate() {
        let slot = if seen < size {
            reservoir.len()
        } else {
            let at = below(seen + 1);
            if at >= size {
                continue;
            }
            at
        };
        let len = bson::serialize_to_vec(&doc)
            .map_err(|e| Error::Internal(format!("$sample input could not be encoded: {e}")))?
            .len();
        if slot == reservoir.len() {
            reservoir.push((doc, len));
            bytes += len;
        } else {
            bytes = bytes - reservoir[slot].1 + len;
            reservoir[slot] = (doc, len);
        }
        if bytes > kimmy_core::limits::MAX_DOCUMENT_BYTES {
            return Err(Error::AnswerTooLarge {
                what: "$sample reservoir",
                bytes,
                max: kimmy_core::limits::MAX_DOCUMENT_BYTES,
                remedy: "lower the size, or $project the documents down before $sample",
            });
        }
    }
    Ok(reservoir.into_iter().map(|(doc, _)| doc).collect())
}

// ---------------------------------------------------------------------------
// $redact
// ---------------------------------------------------------------------------

/// Run `$redact`: the expression is evaluated for the document and, as it descends,
/// for each embedded document, with `$$CURRENT` that document and `$$ROOT` the top one.
/// It answers `$$KEEP` (the document as it is, nothing below it looked at), `$$PRUNE`
/// (the document is left out) or `$$DESCEND` (the document is kept and each document
/// among its fields, and among the elements of its arrays at any depth, is decided in
/// turn); any other answer is an error. An array element that is not a document or an
/// array is kept.
pub(crate) fn redact(
    input: Vec<Document>,
    expr: &Expr,
    vars: &[Binding<'_>],
) -> Result<Vec<Document>> {
    let mut out = Vec::with_capacity(input.len());
    for doc in &input {
        if let Some(kept) = redact_level(doc, doc, expr, vars)? {
            out.push(kept);
        }
    }
    Ok(out)
}

fn redact_level(
    current: &Document,
    top: &Document,
    expr: &Expr,
    vars: &[Binding<'_>],
) -> Result<Option<Document>> {
    let scope = if std::ptr::eq(current, top) {
        Scope::with_bindings(current, vars)
    } else {
        Scope::with_top(current, top, vars)
    };
    match expr.eval_in(&scope)? {
        Bson::String(answer) if answer == REDACT_KEEP => Ok(Some(current.clone())),
        Bson::String(answer) if answer == REDACT_PRUNE => Ok(None),
        Bson::String(answer) if answer == REDACT_DESCEND => {
            let mut out = Document::new();
            for (key, value) in current {
                if let Some(value) = redact_value(value, top, expr, vars)? {
                    out.insert(key.clone(), value);
                }
            }
            Ok(Some(out))
        }
        other => Err(Error::InvalidQuery(format!(
            "$redact's expression must answer $$DESCEND, $$PRUNE or $$KEEP, found {}",
            type_name(&other)
        ))),
    }
}

/// A value below a level that descends: a document is decided (`None` when pruned), an
/// array has each document and array in it decided and a pruned one dropped, anything
/// else stays as it is.
fn redact_value(
    value: &Bson,
    top: &Document,
    expr: &Expr,
    vars: &[Binding<'_>],
) -> Result<Option<Bson>> {
    match value {
        Bson::Document(doc) => Ok(redact_level(doc, top, expr, vars)?.map(Bson::Document)),
        Bson::Array(items) => {
            let mut kept = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Bson::Document(_) | Bson::Array(_) => {
                        if let Some(item) = redact_value(item, top, expr, vars)? {
                            kept.push(item);
                        }
                    }
                    other => kept.push(other.clone()),
                }
            }
            Ok(Some(Bson::Array(kept)))
        }
        other => Ok(Some(other.clone())),
    }
}
