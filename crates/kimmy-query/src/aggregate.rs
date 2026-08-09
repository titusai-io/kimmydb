//! The aggregation pipeline.
//!
//! A pipeline is a list of stages, each taking a stream of documents and
//! producing another. Stages are parsed into an AST once and then executed,
//! matching how filters already work ([`crate::filter`]).
//!
//! # What lives here and what does not
//!
//! Everything except `$lookup`. A lookup reads a *second collection*, and this
//! crate deliberately does not depend on storage — the same boundary that keeps
//! the index planner readable without a database. So [`Stage::Lookup`] is
//! parsed and represented here, and the executor that owns a storage handle is
//! responsible for running it. [`apply`] refuses it rather than silently
//! returning the input unchanged, because a join that quietly does nothing is
//! the kind of wrong answer this codebase tries hardest to avoid.
//!
//! # Blocking stages and why there is a hard cap
//!
//! `$sort` and `$group` cannot emit anything until they have consumed
//! everything: a sort has no first element until the last is seen, and a group
//! has no totals until the last member arrives. `$unwind` and `$lookup` can
//! *grow* their input rather than shrink it.
//!
//! `find` is bounded by `MAX_LIMIT`, but a pipeline's input is a whole
//! collection, so an unbounded pipeline is a way for one request to occupy all
//! the memory on a node. Every stage therefore checks its output against
//! [`Limits`], and exceeding it is an **error naming the stage** rather than a
//! truncated result. Truncating would return an answer that is wrong in a way
//! no caller could detect — a `$group` over 90% of the input looks exactly like
//! a `$group` over all of it.
//!
//! # Expressions are deliberately shallow
//!
//! An accumulator argument is a field path (`"$qty"`) or a literal. There is no
//! `$add`, `$concat` or nested expression tree. Those are a language, and a
//! language wants its own design pass; the stages below cover the reporting
//! shapes people actually reach for, and the boundary is documented rather than
//! discovered.

use std::collections::HashSet;

use bson::{Bson, Document};
use kimmy_core::{Error, Result, path};

use crate::filter::{self, Filter};
use crate::shape::{self, Projection, SortKey};

/// How many documents a single stage may hold or emit.
///
/// Deliberately a hard ceiling rather than a spill-to-disk: a pipeline that
/// cannot run should say so immediately, not become slow in a way that is
/// harder to diagnose than a refusal.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_documents: usize,
}

/// The default ceiling: ten times `find`'s page cap.
///
/// Large enough that ordinary reporting over a sizeable collection works,
/// small enough that a hostile pipeline cannot exhaust a node.
pub const DEFAULT_MAX_DOCUMENTS: usize = 100_000;

impl Default for Limits {
    fn default() -> Self {
        Self { max_documents: DEFAULT_MAX_DOCUMENTS }
    }
}

/// An accumulator's argument: a field reference or a constant.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// `"$qty"` — the value at a dot path in the incoming document.
    Field(String),
    /// Any other BSON value, used as-is.
    Literal(Bson),
}

impl Expr {
    /// Parse the `"$field"` convention.
    ///
    /// A bare string that does not start with `$` is a literal string, which is
    /// what MongoDB does and what callers expect — `{$sum: "total"}` sums the
    /// constant `"total"`, not the field.
    pub fn parse(value: &Bson) -> Self {
        match value {
            Bson::String(s) => match s.strip_prefix('$') {
                Some(field) if !field.is_empty() => Expr::Field(field.to_string()),
                _ => Expr::Literal(value.clone()),
            },
            other => Expr::Literal(other.clone()),
        }
    }

    /// Resolve against a document. A missing field is `Null`, matching how the
    /// filter layer treats absence.
    pub fn eval(&self, doc: &Document) -> Bson {
        match self {
            Expr::Field(p) => value_at(doc, p).cloned().unwrap_or(Bson::Null),
            Expr::Literal(v) => v.clone(),
        }
    }
}

/// What a `$group` computes per bucket.
#[derive(Clone, Debug, PartialEq)]
pub enum Accumulator {
    Sum(Expr),
    Avg(Expr),
    Min(Expr),
    Max(Expr),
    First(Expr),
    Last(Expr),
    Push(Expr),
    AddToSet(Expr),
}

/// One pipeline stage.
#[derive(Clone, Debug)]
pub enum Stage {
    Match(Box<Filter>),
    Project(Option<Projection>),
    Sort(Vec<SortKey>),
    Limit(usize),
    Skip(usize),
    Unwind {
        path: String,
        preserve_null_and_empty: bool,
    },
    Group {
        id: Expr,
        fields: Vec<(String, Accumulator)>,
    },
    Count(String),
    /// Join against another collection. Executed by the caller — see the module
    /// documentation.
    Lookup {
        from: String,
        local_field: String,
        foreign_field: String,
        as_field: String,
    },
}

impl Stage {
    /// Whether running this stage needs a storage handle.
    pub fn needs_storage(&self) -> bool {
        matches!(self, Stage::Lookup { .. })
    }

    /// The name as written, for error messages.
    pub fn name(&self) -> &'static str {
        match self {
            Stage::Match(_) => "$match",
            Stage::Project(_) => "$project",
            Stage::Sort(_) => "$sort",
            Stage::Limit(_) => "$limit",
            Stage::Skip(_) => "$skip",
            Stage::Unwind { .. } => "$unwind",
            Stage::Group { .. } => "$group",
            Stage::Count(_) => "$count",
            Stage::Lookup { .. } => "$lookup",
        }
    }
}

/// Parse a pipeline.
pub fn parse(pipeline: &[Document]) -> Result<Vec<Stage>> {
    pipeline.iter().map(parse_stage).collect()
}

fn parse_stage(stage: &Document) -> Result<Stage> {
    if stage.len() != 1 {
        return Err(Error::InvalidQuery(format!(
            "a pipeline stage must have exactly one key naming the operator, found {}",
            stage.len()
        )));
    }
    let (name, value) = stage.iter().next().expect("length checked above");

    match name.as_str() {
        "$match" => Ok(Stage::Match(Box::new(filter::parse(as_document(name, value)?)?))),
        "$project" => Ok(Stage::Project(shape::parse_projection(as_document(name, value)?)?)),
        "$sort" => Ok(Stage::Sort(shape::parse_sort(as_document(name, value)?)?)),
        "$limit" => Ok(Stage::Limit(as_count(name, value)?)),
        "$skip" => Ok(Stage::Skip(as_count(name, value)?)),
        "$count" => match value {
            Bson::String(field) if !field.is_empty() => Ok(Stage::Count(field.clone())),
            _ => Err(Error::InvalidQuery(
                "$count takes the name of the output field, e.g. {$count: \"total\"}".into(),
            )),
        },
        "$unwind" => parse_unwind(value),
        "$group" => parse_group(as_document(name, value)?),
        "$lookup" => parse_lookup(as_document(name, value)?),
        // `UnsupportedOperator` renders its payload quoted — `unsupported
        // operator "x"` — so the guidance goes in an `InvalidQuery`, whose
        // format composes with a sentence. Both are a 400; this is about the
        // message a caller actually reads.
        other => Err(Error::InvalidQuery(format!(
            "{other} is not a pipeline stage; supported: $match, $project, $sort, $limit, \
             $skip, $unwind, $group, $count, $lookup"
        ))),
    }
}

fn as_document<'a>(stage: &str, value: &'a Bson) -> Result<&'a Document> {
    value.as_document().ok_or_else(|| {
        Error::InvalidQuery(format!("{stage} takes a document, found {}", type_name(value)))
    })
}

fn as_count(stage: &str, value: &Bson) -> Result<usize> {
    let n = match value {
        Bson::Int32(n) => i64::from(*n),
        Bson::Int64(n) => *n,
        // A double that is not a whole number is a mistake worth reporting
        // rather than rounding: `$limit: 2.5` has no defensible meaning.
        Bson::Double(d) if d.fract() == 0.0 => *d as i64,
        _ => {
            return Err(Error::InvalidQuery(format!(
                "{stage} takes a non-negative whole number, found {}",
                type_name(value)
            )));
        }
    };
    if n < 0 {
        return Err(Error::InvalidQuery(format!("{stage} cannot be negative, found {n}")));
    }
    Ok(n as usize)
}

fn parse_unwind(value: &Bson) -> Result<Stage> {
    // Both the shorthand `{$unwind: "$tags"}` and the document form are
    // accepted, because the shorthand is what people write and the document
    // form is the only way to ask for the empty-array behaviour.
    match value {
        Bson::String(_) => match Expr::parse(value) {
            Expr::Field(p) => Ok(Stage::Unwind { path: p, preserve_null_and_empty: false }),
            Expr::Literal(_) => Err(Error::InvalidQuery(
                "$unwind takes a field path beginning with $, e.g. {$unwind: \"$tags\"}".into(),
            )),
        },
        Bson::Document(d) => {
            let field = d.get("path").ok_or_else(|| {
                Error::InvalidQuery("$unwind needs a `path`, e.g. {path: \"$tags\"}".into())
            })?;
            let Expr::Field(p) = Expr::parse(field) else {
                return Err(Error::InvalidQuery(
                    "$unwind `path` must begin with $, e.g. \"$tags\"".into(),
                ));
            };
            let preserve =
                d.get("preserveNullAndEmptyArrays").and_then(Bson::as_bool).unwrap_or(false);
            Ok(Stage::Unwind { path: p, preserve_null_and_empty: preserve })
        }
        other => Err(Error::InvalidQuery(format!(
            "$unwind takes a field path or a document, found {}",
            type_name(other)
        ))),
    }
}

fn parse_group(spec: &Document) -> Result<Stage> {
    let id = spec.get("_id").ok_or_else(|| {
        Error::InvalidQuery(
            "$group needs an _id naming the grouping key; use {_id: null} to group everything \
             into one bucket"
                .into(),
        )
    })?;
    let id = Expr::parse(id);

    let mut fields = Vec::new();
    for (name, value) in spec {
        if name == "_id" {
            continue;
        }
        let acc = value.as_document().ok_or_else(|| {
            Error::InvalidQuery(format!(
                "$group field `{name}` must be an accumulator document, e.g. {{$sum: 1}}"
            ))
        })?;
        if acc.len() != 1 {
            return Err(Error::InvalidQuery(format!(
                "$group field `{name}` must name exactly one accumulator"
            )));
        }
        let (op, arg) = acc.iter().next().expect("length checked above");
        let expr = Expr::parse(arg);
        let accumulator = match op.as_str() {
            "$sum" => Accumulator::Sum(expr),
            "$avg" => Accumulator::Avg(expr),
            "$min" => Accumulator::Min(expr),
            "$max" => Accumulator::Max(expr),
            "$first" => Accumulator::First(expr),
            "$last" => Accumulator::Last(expr),
            "$push" => Accumulator::Push(expr),
            "$addToSet" => Accumulator::AddToSet(expr),
            other => {
                return Err(Error::InvalidQuery(format!(
                    "{other} is not an accumulator; supported: $sum, $avg, $min, $max, $first, \
                     $last, $push, $addToSet"
                )));
            }
        };
        fields.push((name.clone(), accumulator));
    }
    Ok(Stage::Group { id, fields })
}

fn parse_lookup(spec: &Document) -> Result<Stage> {
    let required = |key: &str| -> Result<String> {
        spec.get_str(key).map(str::to_string).map_err(|_| {
            Error::InvalidQuery(format!(
                "$lookup needs a string `{key}`; it takes from, localField, foreignField and as"
            ))
        })
    };
    Ok(Stage::Lookup {
        from: required("from")?,
        local_field: required("localField")?,
        foreign_field: required("foreignField")?,
        as_field: required("as")?,
    })
}

/// The single value at a path.
///
/// [`path::resolve`] follows arrays implicitly and can return several values,
/// which is what filters want — `{tags: "a"}` matches any element. An
/// accumulator argument wants the value *itself*, so the array stays an array
/// and `$unwind` has something to expand.
fn value_at<'a>(doc: &'a Document, p: &str) -> Option<&'a Bson> {
    path::resolve(doc, p).into_iter().next()
}

/// A total, order-consistent byte key for grouping.
///
/// `keyenc` is the same encoding indexes use, so numerically equal values of
/// different types collapse into one bucket — 5 and 5.0 group together, exactly
/// as they would match one index entry. It refuses `Decimal128` (ADR-005), and
/// a grouping key is not a place to fail the whole query over a type that is
/// merely awkward to order, so that falls back to a debug rendering: distinct
/// values stay distinct, they simply do not participate in cross-type equality.
fn group_key(value: &Bson) -> Vec<u8> {
    kimmy_core::keyenc::encode(value).unwrap_or_else(|_| format!("raw:{value:?}").into_bytes())
}

fn type_name(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "a double",
        Bson::String(_) => "a string",
        Bson::Array(_) => "an array",
        Bson::Document(_) => "a document",
        Bson::Boolean(_) => "a boolean",
        Bson::Null => "null",
        Bson::Int32(_) | Bson::Int64(_) => "an integer",
        _ => "that type",
    }
}

/// Run one stage.
///
/// Every stage checks its **output** against the cap, so a stage that grows its
/// input (`$unwind`) is caught as well as one that merely holds it.
pub fn apply(stage: &Stage, input: Vec<Document>, limits: &Limits) -> Result<Vec<Document>> {
    let out = match stage {
        Stage::Match(f) => input.into_iter().filter(|d| filter::matches(f, d)).collect(),
        Stage::Project(p) => input.iter().map(|d| shape::project(p.as_ref(), d)).collect(),
        Stage::Sort(keys) => {
            let mut docs = input;
            shape::sort(keys, &mut docs);
            docs
        }
        Stage::Limit(n) => input.into_iter().take(*n).collect(),
        Stage::Skip(n) => input.into_iter().skip(*n).collect(),
        Stage::Count(field) => {
            let n = input.len() as i64;
            vec![bson::doc! { field.as_str(): n }]
        }
        Stage::Unwind { path: p, preserve_null_and_empty } => {
            unwind(input, p, *preserve_null_and_empty, limits)?
        }
        Stage::Group { id, fields } => group(input, id, fields, limits)?,
        Stage::Lookup { .. } => {
            // Not silently passed through: a join that returns its input
            // unchanged is a wrong answer wearing a right answer's shape.
            return Err(Error::Unsupported(
                "$lookup reads another collection and must be run by the executor that holds a \
                 storage handle, not by the pure pipeline"
                    .into(),
            ));
        }
    };
    check_limit(stage.name(), out.len(), limits)?;
    Ok(out)
}

/// Refuse rather than truncate.
pub fn check_limit(stage: &str, produced: usize, limits: &Limits) -> Result<()> {
    if produced > limits.max_documents {
        return Err(Error::InvalidQuery(format!(
            "{stage} produced {produced} documents, over the pipeline limit of {}. Narrow the \
             pipeline with an earlier $match, or raise server.aggregate.max_documents",
            limits.max_documents
        )));
    }
    Ok(())
}

fn unwind(
    input: Vec<Document>,
    field: &str,
    preserve: bool,
    limits: &Limits,
) -> Result<Vec<Document>> {
    let mut out = Vec::with_capacity(input.len());
    for doc in input {
        match value_at(&doc, field) {
            Some(Bson::Array(items)) if !items.is_empty() => {
                for item in items.clone() {
                    let mut copy = doc.clone();
                    let _ = path::set(&mut copy, field, item);
                    out.push(copy);
                }
                // Checked inside the loop as well as after: a handful of
                // documents each holding a huge array can exceed the cap long
                // before the outer loop ends, and the point of the cap is to
                // stop allocating, not to report afterwards.
                check_limit("$unwind", out.len(), limits)?;
            }
            // A missing field, an explicit null, or an empty array: MongoDB
            // drops the document unless asked to keep it.
            Some(Bson::Array(_)) | Some(Bson::Null) | None => {
                if preserve {
                    let mut copy = doc.clone();
                    path::unset(&mut copy, field);
                    out.push(copy);
                }
            }
            // A non-array value unwinds to itself, which is what MongoDB does
            // and saves callers a `$type` check for a field that is sometimes
            // scalar and sometimes an array.
            Some(_) => out.push(doc),
        }
    }
    Ok(out)
}

/// Bucket state while grouping.
struct Bucket {
    key: Bson,
    values: Vec<AccState>,
}

enum AccState {
    Sum(f64, bool),
    Avg(f64, usize),
    MinMax(Option<Bson>),
    FirstLast(Option<Bson>),
    Push(Vec<Bson>),
    AddToSet(Vec<Bson>),
}

fn group(
    input: Vec<Document>,
    id: &Expr,
    fields: &[(String, Accumulator)],
    limits: &Limits,
) -> Result<Vec<Document>> {
    // Keyed by the *encoded* group value rather than by `Bson`, because `Bson`
    // is not `Hash` and because two numerically equal values of different types
    // must land in the same bucket — the same rule indexes use.
    let mut order: Vec<Bucket> = Vec::new();
    let mut index: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();

    for doc in &input {
        let key = id.eval(doc);
        let encoded = group_key(&key);
        let slot = match index.get(&encoded) {
            Some(&slot) => slot,
            None => {
                let slot = order.len();
                order.push(Bucket { key: key.clone(), values: fields.iter().map(init).collect() });
                index.insert(encoded, slot);
                // One bucket per distinct value, so a high-cardinality key is
                // exactly the shape that exhausts memory.
                check_limit("$group", order.len(), limits)?;
                slot
            }
        };
        for (state, (_, acc)) in order[slot].values.iter_mut().zip(fields) {
            accumulate(state, acc, doc);
        }
    }

    Ok(order
        .into_iter()
        .map(|bucket| {
            let mut out = Document::new();
            out.insert("_id", bucket.key);
            for (state, (name, _)) in bucket.values.into_iter().zip(fields) {
                out.insert(name.clone(), finish(state));
            }
            out
        })
        .collect())
}

fn init((_, acc): &(String, Accumulator)) -> AccState {
    match acc {
        Accumulator::Sum(_) => AccState::Sum(0.0, true),
        Accumulator::Avg(_) => AccState::Avg(0.0, 0),
        Accumulator::Min(_) | Accumulator::Max(_) => AccState::MinMax(None),
        Accumulator::First(_) | Accumulator::Last(_) => AccState::FirstLast(None),
        Accumulator::Push(_) => AccState::Push(Vec::new()),
        Accumulator::AddToSet(_) => AccState::AddToSet(Vec::new()),
    }
}

fn accumulate(state: &mut AccState, acc: &Accumulator, doc: &Document) {
    match (state, acc) {
        (AccState::Sum(total, all_int), Accumulator::Sum(e)) => {
            let v = e.eval(doc);
            if !matches!(v, Bson::Int32(_) | Bson::Int64(_)) {
                *all_int = false;
            }
            *total += numeric(&v).unwrap_or(0.0);
        }
        (AccState::Avg(total, n), Accumulator::Avg(e)) => {
            // Non-numeric values are skipped rather than counted as zero, or a
            // field that is missing on half the documents would halve the mean.
            if let Some(x) = numeric(&e.eval(doc)) {
                *total += x;
                *n += 1;
            }
        }
        (AccState::MinMax(current), Accumulator::Min(e)) => {
            let v = e.eval(doc);
            if !matches!(v, Bson::Null)
                && current.as_ref().is_none_or(|c| kimmy_core::cmp::canonical_cmp(&v, c).is_lt())
            {
                *current = Some(v);
            }
        }
        (AccState::MinMax(current), Accumulator::Max(e)) => {
            let v = e.eval(doc);
            if !matches!(v, Bson::Null)
                && current.as_ref().is_none_or(|c| kimmy_core::cmp::canonical_cmp(&v, c).is_gt())
            {
                *current = Some(v);
            }
        }
        (AccState::FirstLast(current), Accumulator::First(e)) => {
            if current.is_none() {
                *current = Some(e.eval(doc));
            }
        }
        (AccState::FirstLast(current), Accumulator::Last(e)) => {
            *current = Some(e.eval(doc));
        }
        (AccState::Push(items), Accumulator::Push(e)) => items.push(e.eval(doc)),
        (AccState::AddToSet(items), Accumulator::AddToSet(e)) => {
            let v = e.eval(doc);
            // Linear scan rather than a hash set: `Bson` is not `Hash`, and a
            // set is small in every case that is not already refused by the cap.
            if !items.iter().any(|existing| existing == &v) {
                items.push(v);
            }
        }
        // Unreachable: `init` pairs each accumulator with its own state.
        _ => {}
    }
}

fn finish(state: AccState) -> Bson {
    match state {
        // An integer sum stays an integer. Widening every total to a double
        // would lose precision above 2^53 and break `$type` on the result,
        // which is the same reasoning as ADR-002.
        AccState::Sum(total, all_int) if all_int && total.fract() == 0.0 => {
            Bson::Int64(total as i64)
        }
        AccState::Sum(total, _) => Bson::Double(total),
        AccState::Avg(_, 0) => Bson::Null,
        AccState::Avg(total, n) => Bson::Double(total / n as f64),
        AccState::MinMax(v) | AccState::FirstLast(v) => v.unwrap_or(Bson::Null),
        AccState::Push(items) | AccState::AddToSet(items) => Bson::Array(items),
    }
}

fn numeric(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(n) => Some(f64::from(*n)),
        Bson::Int64(n) => Some(*n as f64),
        Bson::Double(d) => Some(*d),
        _ => None,
    }
}

/// The distinct local-field values a `$lookup` needs from its input.
///
/// Exposed so the executor can fetch the foreign side in **one** pass instead
/// of once per document: a join done per input document is O(n·m), and on a
/// collection of any size that is the difference between a query and an outage.
pub fn lookup_keys(input: &[Document], local_field: &str) -> Vec<Bson> {
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut keys = Vec::new();
    for doc in input {
        let value = value_at(doc, local_field).cloned().unwrap_or(Bson::Null);
        if seen.insert(group_key(&value)) {
            keys.push(value);
        }
    }
    keys
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    fn docs(items: Vec<Document>) -> Vec<Document> {
        items
    }

    fn run(pipeline: Vec<Document>, input: Vec<Document>) -> Result<Vec<Document>> {
        let stages = parse(&pipeline)?;
        let limits = Limits::default();
        let mut current = input;
        for stage in &stages {
            current = apply(stage, current, &limits)?;
        }
        Ok(current)
    }

    fn sample() -> Vec<Document> {
        docs(vec![
            doc! { "_id": 1, "city": "London", "qty": 5, "tags": ["a", "b"] },
            doc! { "_id": 2, "city": "London", "qty": 15, "tags": ["b"] },
            doc! { "_id": 3, "city": "Paris", "qty": 10, "tags": [] },
        ])
    }

    #[test]
    fn match_then_count() {
        let out =
            run(vec![doc! {"$match": {"city": "London"}}, doc! {"$count": "n"}], sample()).unwrap();
        assert_eq!(out, vec![doc! { "n": 2i64 }]);
    }

    #[test]
    fn group_sums_per_key() {
        let out = run(
            vec![
                doc! {"$group": {"_id": "$city", "total": {"$sum": "$qty"}}},
                doc! {"$sort": {"_id": 1}},
            ],
            sample(),
        )
        .unwrap();
        assert_eq!(
            out,
            vec![doc! { "_id": "London", "total": 20i64 }, doc! { "_id": "Paris", "total": 10i64 }]
        );
    }

    #[test]
    fn an_integer_sum_stays_an_integer() {
        // Widening to double would lose precision above 2^53 and change what
        // `$type` reports — the same reason documents keep their integer types.
        let out = run(vec![doc! {"$group": {"_id": null, "n": {"$sum": 1}}}], sample()).unwrap();
        assert_eq!(out[0].get_i64("n").unwrap(), 3);
    }

    #[test]
    fn avg_skips_missing_values_rather_than_counting_them_as_zero() {
        // Counting a missing field as zero would halve the mean, silently.
        let input = docs(vec![doc! { "x": 10 }, doc! { "y": 1 }, doc! { "x": 20 }]);
        let out = run(vec![doc! {"$group": {"_id": null, "m": {"$avg": "$x"}}}], input).unwrap();
        assert_eq!(out[0].get_f64("m").unwrap(), 15.0);
    }

    #[test]
    fn group_buckets_numerically_equal_keys_together() {
        // 5 and 5.0 are the same value everywhere else in this database; a
        // grouping that split them would contradict the index encoding.
        let input = docs(vec![doc! { "k": 5i32 }, doc! { "k": 5.0 }, doc! { "k": 5i64 }]);
        let out = run(vec![doc! {"$group": {"_id": "$k", "n": {"$sum": 1}}}], input).unwrap();
        assert_eq!(out.len(), 1, "5, 5.0 and 5i64 must share a bucket: {out:?}");
        assert_eq!(out[0].get_i64("n").unwrap(), 3);
    }

    #[test]
    fn unwind_expands_arrays_and_drops_empty_ones() {
        let out = run(vec![doc! {"$unwind": "$tags"}], sample()).unwrap();
        assert_eq!(out.len(), 3, "two tags plus one, and the empty array drops: {out:?}");
        assert_eq!(out[0].get_str("tags").unwrap(), "a");
    }

    #[test]
    fn unwind_can_preserve_empty_arrays() {
        let out = run(
            vec![doc! {"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": true}}],
            sample(),
        )
        .unwrap();
        assert_eq!(out.len(), 4, "the empty array now yields one document: {out:?}");
    }

    #[test]
    fn sort_skip_limit_compose() {
        let out =
            run(vec![doc! {"$sort": {"qty": -1}}, doc! {"$skip": 1}, doc! {"$limit": 1}], sample())
                .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get_i32("qty").unwrap(), 10);
    }

    #[test]
    fn add_to_set_deduplicates_and_push_does_not() {
        let out = run(
            vec![doc! {"$group": {
                "_id": null,
                "all": {"$push": "$city"},
                "distinct": {"$addToSet": "$city"}
            }}],
            sample(),
        )
        .unwrap();
        assert_eq!(out[0].get_array("all").unwrap().len(), 3);
        assert_eq!(out[0].get_array("distinct").unwrap().len(), 2);
    }

    #[test]
    fn a_blocking_stage_refuses_rather_than_truncating() {
        // Truncating would return a `$group` over part of the input that looks
        // exactly like one over all of it — undetectable by the caller.
        let input: Vec<Document> = (0..50).map(|i| doc! { "k": i }).collect();
        let limits = Limits { max_documents: 10 };
        let stages = parse(&[doc! {"$group": {"_id": "$k", "n": {"$sum": 1}}}]).unwrap();

        let err = apply(&stages[0], input, &limits).unwrap_err().to_string();
        assert!(err.contains("$group"), "the error must name the stage: {err}");
        assert!(err.contains("limit"), "and say what was exceeded: {err}");
    }

    #[test]
    fn unwind_is_capped_while_it_expands_not_after() {
        // A few documents holding huge arrays exceed the cap long before the
        // outer loop ends; checking only at the end would allocate all of it.
        let input: Vec<Document> =
            (0..5).map(|_| doc! { "xs": (0..100).collect::<Vec<i32>>() }).collect();
        let limits = Limits { max_documents: 50 };
        let stages = parse(&[doc! {"$unwind": "$xs"}]).unwrap();

        let err = apply(&stages[0], input, &limits).unwrap_err().to_string();
        assert!(err.contains("$unwind"), "{err}");
    }

    #[test]
    fn lookup_is_refused_by_the_pure_pipeline() {
        // It must not pass its input through unchanged: a join that silently
        // does nothing is a wrong answer shaped like a right one.
        let stages = parse(&[doc! {"$lookup": {
            "from": "users", "localField": "uid", "foreignField": "_id", "as": "user"
        }}])
        .unwrap();
        assert!(stages[0].needs_storage());
        assert!(apply(&stages[0], sample(), &Limits::default()).is_err());
    }

    #[test]
    fn lookup_keys_are_distinct_and_ordered() {
        let input = docs(vec![doc! {"u": 1}, doc! {"u": 2}, doc! {"u": 1}, doc! {"u": 1.0}]);
        let keys = lookup_keys(&input, "u");
        assert_eq!(keys.len(), 2, "1, 2 and 1.0 yield two distinct keys: {keys:?}");
    }

    #[test]
    fn an_unknown_stage_names_what_is_supported() {
        let err = parse(&[doc! {"$bucketAuto": {}}]).unwrap_err().to_string();
        assert!(err.contains("$bucketAuto"), "{err}");
        assert!(err.contains("$group"), "the error should list what does work: {err}");
    }

    #[test]
    fn a_stage_with_two_keys_is_rejected() {
        // `{$match: ..., $limit: ...}` has no defined order, so accepting it
        // would make the pipeline's meaning depend on BSON key order.
        let err = parse(&[doc! {"$match": {}, "$limit": 1}]).unwrap_err().to_string();
        assert!(err.contains("exactly one key"), "{err}");
    }

    #[test]
    fn limit_rejects_nonsense() {
        assert!(parse(&[doc! {"$limit": -1}]).is_err());
        assert!(parse(&[doc! {"$limit": 2.5}]).is_err());
        assert!(parse(&[doc! {"$limit": "ten"}]).is_err());
        assert!(parse(&[doc! {"$limit": 10}]).is_ok());
    }

    #[test]
    fn a_bare_string_is_a_literal_not_a_field() {
        assert_eq!(Expr::parse(&Bson::String("total".into())), Expr::Literal("total".into()));
        assert_eq!(Expr::parse(&Bson::String("$total".into())), Expr::Field("total".into()));
    }
}
