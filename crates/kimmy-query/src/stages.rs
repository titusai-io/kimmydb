//! The small pipeline stages (ADR-263): `$sortByCount`, `$unset`, `$sample`, `$redact`.
//!
//! `$replaceWith` is `$replaceRoot` under another name and `$unionWith` is run by the
//! caller (it reads a second collection), so both live in [`crate::aggregate`]. What is
//! here is the parsing of the other four and the code that runs the last three (`$sortByCount`
//! runs on [`crate::aggregate::GroupState`], with `$group`).

use bson::{Bson, Document};
use kimmy_core::{Error, Result};

use crate::aggregate::{Limits, Stage, as_document};
use crate::budget::Lease;
use crate::deadline::{self, Ticker};
use crate::expr::{Binding, Expr, REDACT_DESCEND, REDACT_KEEP, REDACT_PRUNE, Scope, type_name};
use crate::shape;

// ---------------------------------------------------------------------------
// $sortByCount
// ---------------------------------------------------------------------------

/// `{$sortByCount: <expression>}`: a field path (`"$a"`, `"$$ROOT"`) or an expression
/// document (`{$mod: ["$n", 2]}`). The reference's refusals, in its words: a number,
/// boolean, `null` or array is "must be specified as a string or as an object"; a string
/// that is not a path, an empty document and an object that is not an operator expression
/// (`{who: "$k"}`) are "must be defined as a $-prefixed path or an expression inside an
/// object"; and `"$"` alone is not a field path.
pub(crate) fn parse_sort_by_count(value: &Bson, vars: &[String]) -> Result<Stage> {
    let refused = |what: &str| Error::InvalidQuery(format!("the sortByCount field must be {what}"));
    match value {
        Bson::String(s) if s == "$" => {
            return Err(Error::InvalidQuery("'$' by itself is not a valid FieldPath".into()));
        }
        Bson::String(s) if s.starts_with('$') => {}
        Bson::Document(d) if d.keys().next().is_some_and(|k| k.starts_with('$')) => {}
        Bson::String(_) | Bson::Document(_) => {
            return Err(refused("defined as a $-prefixed path or an expression inside an object"));
        }
        _ => return Err(refused("specified as a string or as an object")),
    }
    Ok(Stage::SortByCount(Expr::parse_with_vars(value, vars)?))
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

/// `{$sample: {size: N}}`: `N` a number. A double truncates toward zero (`1.5` is `1`),
/// a decimal rounds half to even (`2.5` is `2`, `3.5` is `4`), as the reference does; `0`
/// and `NaN` answer nothing. A negative, a non-number, a missing `size` and any other
/// key are refused.
pub(crate) fn parse_sample(value: &Bson) -> Result<Stage> {
    let spec = as_document("$sample", value)?;
    for key in spec.keys() {
        if key != "size" {
            return Err(Error::InvalidQuery(format!("unrecognized option to $sample: {key}")));
        }
    }
    let Some(size) = spec.get("size") else {
        return Err(Error::InvalidQuery("$sample stage must specify a size".into()));
    };
    // `as` saturates, so an enormous size is every document, and NaN is 0.
    let n = match size {
        Bson::Int32(n) => i64::from(*n),
        Bson::Int64(n) => *n,
        Bson::Double(d) => d.trunc() as i64,
        Bson::Decimal128(d) => d
            .to_string()
            .parse::<f64>()
            .map_err(|_| Error::InvalidQuery("size argument to $sample must be a number".into()))?
            .round_ties_even() as i64,
        _ => {
            return Err(Error::InvalidQuery("size argument to $sample must be a number".into()));
        }
    };
    if n < 0 {
        return Err(Error::InvalidQuery("size argument to $sample must not be negative".into()));
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
/// **Order**: random. The reservoir is shuffled once (Fisher-Yates over the same source
/// of positions) before it is answered, so every order of the sample is equally likely,
/// `size >= input.len()` included, which makes `{$sample: {size: <large>}}` a shuffle,
/// as in the reference.
///
/// **Bounds**: a `size` over the input is the whole input (the input has already been
/// held to the pipeline's document limit, so nothing here can hold more). The reservoir
/// is documents the pipeline already holds and moves none in, so it is charged to no
/// second budget.
pub fn sample(
    input: Vec<Document>,
    size: usize,
    _limits: &Limits,
    below: Below<'_>,
) -> Result<Vec<Document>> {
    if size == 0 {
        return Ok(Vec::new());
    }
    let mut reservoir: Vec<Document> = Vec::with_capacity(size.min(input.len()));
    // A `Ticker` for each of the two loops (ADR-266), so that neither hides the other.
    let mut ticker = Ticker::new();
    for (seen, doc) in input.into_iter().enumerate() {
        ticker.tick()?;
        if seen < size {
            reservoir.push(doc);
        } else {
            let at = below(seen + 1);
            if at < size {
                reservoir[at] = doc;
            }
        }
    }
    let mut ticker = Ticker::new();
    for i in (1..reservoir.len()).rev() {
        ticker.tick()?;
        reservoir.swap(i, below(i + 1));
    }
    Ok(reservoir)
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
    held: &mut Lease,
) -> Result<Vec<Document>> {
    let mut out = Vec::with_capacity(input.len());
    for doc in &input {
        if let Some(kept) = redact_doc(doc, expr, vars)? {
            held.charge_doc(&kept)?;
            out.push(kept);
        }
    }
    Ok(out)
}

/// One document through `$redact`: what is left of it, or `None` when it is pruned (ADR-270:
/// the body of the stage for one document, which [`redact`] loops over and the chain of sinks
/// calls as each document arrives).
pub(crate) fn redact_doc(
    doc: &Document,
    expr: &Expr,
    vars: &[Binding<'_>],
) -> Result<Option<Document>> {
    redact_level(doc, doc, expr, vars)
}

fn redact_level(
    current: &Document,
    top: &Document,
    expr: &Expr,
    vars: &[Binding<'_>],
) -> Result<Option<Document>> {
    // The thread's shared count (ADR-266): once for each document decided, the top one
    // and every embedded one the descent reaches, whatever the depth.
    deadline::tick()?;
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
                deadline::tick()?;
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
