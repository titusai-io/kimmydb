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
//! # Expressions live next door
//!
//! An accumulator argument, a `$group` key and a computed field are all
//! [`crate::expr::Expr`], which is a full tree — arithmetic, strings,
//! conditionals, comparison, boolean, date parts and arrays. That module owns
//! the operator set and its evaluation; this one owns stages and the flow of
//! documents between them.
//!
//! Expressions evaluate in a [`Scope`]. At the top level that is the document
//! and nothing else; inside a `$lookup` sub-pipeline it also carries the
//! `let` bindings, which is why every entry point here has a `_with_vars`
//! form. The plain form is the empty-variables case and is what every caller
//! outside a sub-pipeline uses.

use std::collections::HashSet;

use bson::{Bson, Document};
use kimmy_core::{Error, Result, path};

use crate::expr::{Binding, Expr, Scope, Total, type_name};
use crate::filter::{self, Filter};
use crate::shape::{self, ID_FIELD, Projection, SortKey};

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
    /// Reshape. `computed` is applied **after** `projection`, so a computed
    /// field may overwrite a projected one and never sees its own output.
    Project {
        projection: Option<Projection>,
        computed: Vec<(String, Expr)>,
    },
    /// `$addFields` and its alias `$set` — every input field kept, these added.
    AddFields(Vec<(String, Expr)>),
    /// `$replaceRoot` — the computed document *becomes* the document.
    ReplaceRoot(Expr),
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
        as_field: String,
        join: Join,
    },
}

/// How a `$lookup` decides which foreign documents belong to an input document.
#[derive(Clone, Debug)]
pub enum Join {
    /// `localField` / `foreignField`: equality on one key. The executor scans
    /// the foreign collection **once** and indexes it by that key.
    Equality { local_field: String, foreign_field: String },
    /// `let` / `pipeline`: `stages` run over the foreign collection **once per
    /// input document**, with `vars` evaluated against that document and bound
    /// for the sub-pipeline to read as `$$name`. A nested loop by construction;
    /// see `docs/aggregation.md` for when to prefer the other form.
    Pipeline { vars: Vec<(String, Expr)>, stages: Vec<Stage> },
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
            Stage::Project { .. } => "$project",
            Stage::AddFields(_) => "$addFields",
            Stage::ReplaceRoot(_) => "$replaceRoot",
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
    parse_with_vars(pipeline, &[])
}

/// Parse a pipeline whose expressions may read `vars` — a `$lookup`
/// sub-pipeline, with the names its `let` binds.
pub fn parse_with_vars(pipeline: &[Document], vars: &[String]) -> Result<Vec<Stage>> {
    pipeline.iter().map(|stage| parse_stage(stage, vars)).collect()
}

/// The `$match` stages a pipeline *begins* with, merged into one filter, and
/// how many stages they were.
///
/// This is what lets the executor read a pipeline's source through the same
/// planner `find` uses: a leading `$match` sees documents exactly as stored,
/// so a filter over it is a filter over the collection and an index answers
/// it. Only the leading run qualifies. A `$match` after a `$project`,
/// `$unwind` or `$group` reads reshaped documents, and pushing it down would
/// change what it matches — so it stays where it was written, and the
/// executor applies it as an ordinary stage. Consecutive leading matches are
/// one conjunction, which is what running them in sequence already meant.
///
/// `None` when the pipeline does not begin with `$match`.
pub fn leading_match(stages: &[Stage]) -> Option<(Filter, usize)> {
    let mut filters: Vec<Filter> = Vec::new();
    for stage in stages {
        match stage {
            Stage::Match(f) => filters.push((**f).clone()),
            _ => break,
        }
    }
    match filters.len() {
        0 => None,
        1 => filters.pop().map(|f| (f, 1)),
        n => Some((Filter::And(filters), n)),
    }
}

fn parse_stage(stage: &Document, vars: &[String]) -> Result<Stage> {
    if stage.len() != 1 {
        return Err(Error::InvalidQuery(format!(
            "a pipeline stage must have exactly one key naming the operator, found {}",
            stage.len()
        )));
    }
    let (name, value) = stage.iter().next().expect("length checked above");

    match name.as_str() {
        "$match" => Ok(Stage::Match(Box::new(filter::parse(as_document(name, value)?)?))),
        "$project" => parse_project(as_document(name, value)?, vars),
        "$addFields" | "$set" => {
            Ok(Stage::AddFields(parse_computed(name, as_document(name, value)?, vars)?))
        }
        "$replaceRoot" => parse_replace_root(as_document(name, value)?, vars),
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
        "$group" => parse_group(as_document(name, value)?, vars),
        "$lookup" => parse_lookup(as_document(name, value)?, vars),
        // `UnsupportedOperator` renders its payload quoted — `unsupported
        // operator "x"` — so the guidance goes in an `InvalidQuery`, whose
        // format composes with a sentence. Both are a 400; this is about the
        // message a caller actually reads.
        other => Err(Error::InvalidQuery(format!(
            "{other} is not a pipeline stage; supported: $match, $project, $addFields, $set, \
             $replaceRoot, $sort, $limit, $skip, $unwind, $group, $count, $lookup"
        ))),
    }
}

/// `{field: <expression>, ...}` — every value is an expression.
fn parse_computed(stage: &str, doc: &Document, vars: &[String]) -> Result<Vec<(String, Expr)>> {
    if doc.is_empty() {
        return Err(Error::InvalidQuery(format!("{stage} needs at least one field")));
    }
    doc.iter()
        .map(|(name, value)| Ok((name.clone(), Expr::parse_with_vars(value, vars)?)))
        .collect()
}

/// `$project` is two things at once: the include/exclude flags it always was,
/// and computed fields.
///
/// **A number or boolean stays a flag**, as it has always been here, and
/// anything else is an expression. `{$literal: 5}` is how a caller asks for a
/// constant number, which is the reason that operator exists.
///
/// Computed fields force inclusion mode. Excluding some fields while computing
/// others is refused for the same reason mixing inclusion and exclusion is —
/// the result would be ambiguous about every field nobody named.
fn parse_project(doc: &Document, vars: &[String]) -> Result<Stage> {
    let mut computed = Vec::new();
    let mut include = Vec::new();
    let mut excluded_id = false;
    let mut excluded_other = false;

    for (key, value) in doc {
        match value {
            Bson::Int32(0) | Bson::Int64(0) | Bson::Double(0.0) | Bson::Boolean(false) => {
                if key == ID_FIELD {
                    excluded_id = true;
                } else {
                    excluded_other = true;
                }
            }
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Boolean(true) => {
                include.push(key.clone());
            }
            other => computed.push((key.clone(), Expr::parse_with_vars(other, vars)?)),
        }
    }

    // With no computed fields this is exactly the projection it always was,
    // so defer to the one parser rather than growing a second dialect.
    if computed.is_empty() {
        return Ok(Stage::Project { projection: shape::parse_projection(doc)?, computed });
    }

    if excluded_other {
        return Err(Error::InvalidQuery(
            "$project cannot exclude fields and compute others in one stage \
             (excluding _id is the exception)"
                .into(),
        ));
    }
    if !excluded_id && !include.iter().any(|p| p == ID_FIELD) {
        include.push(ID_FIELD.to_string());
    }

    Ok(Stage::Project { projection: Some(Projection::Include(include)), computed })
}

fn parse_replace_root(spec: &Document, vars: &[String]) -> Result<Stage> {
    let Some(new_root) = spec.get("newRoot") else {
        return Err(Error::InvalidQuery(
            "$replaceRoot needs a `newRoot`, e.g. {newRoot: \"$address\"}".into(),
        ));
    };
    Ok(Stage::ReplaceRoot(Expr::parse_with_vars(new_root, vars)?))
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
        Bson::String(_) => match Expr::parse(value)? {
            Expr::Field(p) => Ok(Stage::Unwind { path: p, preserve_null_and_empty: false }),
            _ => Err(Error::InvalidQuery(
                "$unwind takes a field path beginning with $, e.g. {$unwind: \"$tags\"}".into(),
            )),
        },
        Bson::Document(d) => {
            let field = d.get("path").ok_or_else(|| {
                Error::InvalidQuery("$unwind needs a `path`, e.g. {path: \"$tags\"}".into())
            })?;
            let Expr::Field(p) = Expr::parse(field)? else {
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

fn parse_group(spec: &Document, vars: &[String]) -> Result<Stage> {
    let id = spec.get("_id").ok_or_else(|| {
        Error::InvalidQuery(
            "$group needs an _id naming the grouping key; use {_id: null} to group everything \
             into one bucket"
                .into(),
        )
    })?;
    let id = Expr::parse_with_vars(id, vars)?;

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
        let expr = Expr::parse_with_vars(arg, vars)?;
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

/// Two forms, told apart by their keys and never mixed.
///
/// `localField`/`foreignField` is the equality join; `let`/`pipeline` is the
/// general one. MongoDB also accepts both at once — a "concise correlated
/// subquery" — and that is refused here: the executor would have to combine an
/// indexed pass with a per-document loop, and a caller who wants to reshape
/// what an equality join attached can do that with `$filter` or `$map` in the
/// stage after it.
fn parse_lookup(spec: &Document, vars: &[String]) -> Result<Stage> {
    let string = |key: &str| -> Result<String> {
        spec.get_str(key).map(str::to_string).map_err(|_| {
            Error::InvalidQuery(format!(
                "$lookup needs a string `{key}`; it takes from and as, then either localField \
                 and foreignField or let and pipeline"
            ))
        })
    };
    let from = string("from")?;
    let as_field = string("as")?;

    let equality = spec.contains_key("localField") || spec.contains_key("foreignField");
    let pipeline = spec.contains_key("pipeline") || spec.contains_key("let");
    let join = match (equality, pipeline) {
        (true, true) => {
            return Err(Error::InvalidQuery(
                "$lookup takes either localField/foreignField or let/pipeline, not both; join \
                 on the key and reshape the joined array with $filter or $map in the next stage"
                    .into(),
            ));
        }
        (false, false) => {
            return Err(Error::InvalidQuery(
                "$lookup needs either localField and foreignField, or a pipeline".into(),
            ));
        }
        (true, false) => Join::Equality {
            local_field: string("localField")?,
            foreign_field: string("foreignField")?,
        },
        (false, true) => {
            let let_vars = match spec.get("let") {
                None => Vec::new(),
                Some(Bson::Document(raw)) => Expr::parse_bindings(raw, vars)?,
                Some(other) => {
                    return Err(Error::InvalidQuery(format!(
                        "$lookup `let` is a document of name: expression, found {}",
                        type_name(other)
                    )));
                }
            };
            let Some(Bson::Array(raw_stages)) = spec.get("pipeline") else {
                return Err(Error::InvalidQuery("$lookup `pipeline` is an array of stages".into()));
            };
            let raw_stages = raw_stages
                .iter()
                .map(|stage| {
                    stage.as_document().cloned().ok_or_else(|| {
                        Error::InvalidQuery(format!(
                            "each $lookup pipeline stage is a document, found {}",
                            type_name(stage)
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            // The sub-pipeline sees the outer variables as well as its own:
            // a `$lookup` nested inside another's pipeline can still read the
            // outer `let`.
            let mut in_scope = vars.to_vec();
            in_scope.extend(let_vars.iter().map(|(name, _)| name.clone()));
            let stages = parse_with_vars(&raw_stages, &in_scope)?;
            Join::Pipeline { vars: let_vars, stages }
        }
    };
    Ok(Stage::Lookup { from, as_field, join })
}

/// Evaluate a `$lookup` `let` against one input document.
///
/// `outer` is whatever an enclosing `$lookup` already bound, visible to the
/// expressions here and shadowed by any name they rebind. The executor appends
/// the result to `outer` and runs the sub-pipeline in that.
pub fn bind_let(
    vars: &[(String, Expr)],
    doc: &Document,
    outer: &[Binding<'_>],
) -> Result<Vec<(String, Bson)>> {
    let scope = Scope::with_bindings(doc, outer);
    vars.iter().map(|(name, expr)| Ok((name.clone(), expr.eval_in(&scope)?))).collect()
}

/// The single value at a **field path** — a stage option that names a field,
/// as distinct from an expression that reads one.
///
/// The two contexts have different rules in MongoDB and here. An expression
/// (`$project`, `$addFields`, `$replaceRoot`, a `$group` key or accumulator
/// argument, `$expr`) goes through [`Expr`], where a path that crosses an
/// array fans out into an array of what it found. A field path names a place
/// in the document: `$unwind`'s `path`, `$lookup`'s `localField` and
/// `foreignField`, `$sort`'s keys. Those do not fan out, and this helper is
/// theirs. [`path::resolve`] follows arrays implicitly and can return several
/// values, which is what filters want — `{tags: "a"}` matches any element;
/// a field-path option wants the value *itself*, so the array stays an array
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

/// Run one stage.
///
/// Every stage checks its **output** against the cap, so a stage that grows its
/// input (`$unwind`) is caught as well as one that merely holds it.
pub fn apply(stage: &Stage, input: Vec<Document>, limits: &Limits) -> Result<Vec<Document>> {
    apply_with_vars(stage, input, limits, &[])
}

/// [`apply`], with `vars` bound for every expression the stage evaluates —
/// how a `$lookup` sub-pipeline's stages see its `let`.
pub fn apply_with_vars(
    stage: &Stage,
    input: Vec<Document>,
    limits: &Limits,
    vars: &[Binding<'_>],
) -> Result<Vec<Document>> {
    let out = match stage {
        Stage::Match(f) => input.into_iter().filter(|d| filter::matches(f, d)).collect(),
        Stage::Project { projection, computed } => {
            let mut out = Vec::with_capacity(input.len());
            for doc in &input {
                let mut projected = shape::project(projection.as_ref(), doc);
                // Computed fields read the *input*, not the projection's
                // output — otherwise `{$project: {a: 0, b: "$a"}}` would
                // depend on key order, which BSON preserves and nobody
                // reasons about.
                set_computed(&mut projected, computed, &Scope::with_bindings(doc, vars))?;
                out.push(projected);
            }
            out
        }
        Stage::AddFields(computed) => {
            let mut out = Vec::with_capacity(input.len());
            for doc in &input {
                let mut with = doc.clone();
                set_computed(&mut with, computed, &Scope::with_bindings(doc, vars))?;
                out.push(with);
            }
            out
        }
        Stage::ReplaceRoot(expr) => {
            let mut out = Vec::with_capacity(input.len());
            for doc in &input {
                match expr.eval_in(&Scope::with_bindings(doc, vars))? {
                    Bson::Document(new_root) => out.push(new_root),
                    // Refused rather than wrapped: a root that is not a
                    // document would have to be given a field name this stage
                    // has no business inventing.
                    other => {
                        return Err(Error::InvalidQuery(format!(
                            "$replaceRoot needs a document, found {}",
                            type_name(&other)
                        )));
                    }
                }
            }
            out
        }
        Stage::Sort(keys) => {
            // A sort key is a field path, not an expression, and orders by
            // the same rules `find`'s sort does — an array sorts by its
            // elements, and a path that crosses one is not fanned out.
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
        Stage::Group { id, fields } => group(input, id, fields, limits, vars)?,
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

/// Write computed fields into `target`, evaluating each in `source`.
///
/// A dotted name writes into a nested document, matching `$set`'s behaviour on
/// paths. A path that cannot be written — into an array without an index —
/// is an error rather than a silent no-op, because a field the caller asked
/// for and did not get is exactly the failure this codebase keeps meeting.
fn set_computed(
    target: &mut Document,
    computed: &[(String, Expr)],
    source: &Scope<'_>,
) -> Result<()> {
    for (name, expr) in computed {
        let value = expr.eval_in(source)?;
        path::set(target, name, value)
            .map_err(|e| Error::InvalidQuery(format!("cannot set {name:?}: {e}")))?;
    }
    Ok(())
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
        // `path` is a field path: `$unwind` names the array to expand, and
        // `path::set` writes each element back to that same place. A fanned
        // read would have no single place to write to.
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
    /// Exact while every input is integral — see [`Total`]. The previous
    /// `(f64, bool)` accumulated in a double and cast back, so a total above
    /// 2^53 was silently wrong despite the flag saying it was an integer.
    Sum(Total),
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
    vars: &[Binding<'_>],
) -> Result<Vec<Document>> {
    // Keyed by the *encoded* group value rather than by `Bson`, because `Bson`
    // is not `Hash` and because two numerically equal values of different types
    // must land in the same bucket — the same rule indexes use.
    let mut order: Vec<Bucket> = Vec::new();
    let mut index: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();

    for doc in &input {
        let scope = Scope::with_bindings(doc, vars);
        let key = id.eval_in(&scope)?;
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
            accumulate(state, acc, &scope)?;
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
        Accumulator::Sum(_) => AccState::Sum(Total::default()),
        Accumulator::Avg(_) => AccState::Avg(0.0, 0),
        Accumulator::Min(_) | Accumulator::Max(_) => AccState::MinMax(None),
        Accumulator::First(_) | Accumulator::Last(_) => AccState::FirstLast(None),
        Accumulator::Push(_) => AccState::Push(Vec::new()),
        Accumulator::AddToSet(_) => AccState::AddToSet(Vec::new()),
    }
}

fn accumulate(state: &mut AccState, acc: &Accumulator, scope: &Scope<'_>) -> Result<()> {
    match (state, acc) {
        (AccState::Sum(total), Accumulator::Sum(e)) => total.add(&e.eval_in(scope)?),
        (AccState::Avg(total, n), Accumulator::Avg(e)) => {
            // Non-numeric values are skipped rather than counted as zero, or a
            // field that is missing on half the documents would halve the mean.
            if let Some(x) = numeric(&e.eval_in(scope)?) {
                *total += x;
                *n += 1;
            }
        }
        (AccState::MinMax(current), Accumulator::Min(e)) => {
            let v = e.eval_in(scope)?;
            if !matches!(v, Bson::Null)
                && current.as_ref().is_none_or(|c| kimmy_core::cmp::canonical_cmp(&v, c).is_lt())
            {
                *current = Some(v);
            }
        }
        (AccState::MinMax(current), Accumulator::Max(e)) => {
            let v = e.eval_in(scope)?;
            if !matches!(v, Bson::Null)
                && current.as_ref().is_none_or(|c| kimmy_core::cmp::canonical_cmp(&v, c).is_gt())
            {
                *current = Some(v);
            }
        }
        (AccState::FirstLast(current), Accumulator::First(e)) => {
            if current.is_none() {
                *current = Some(e.eval_in(scope)?);
            }
        }
        (AccState::FirstLast(current), Accumulator::Last(e)) => {
            *current = Some(e.eval_in(scope)?);
        }
        (AccState::Push(items), Accumulator::Push(e)) => items.push(e.eval_in(scope)?),
        (AccState::AddToSet(items), Accumulator::AddToSet(e)) => {
            let v = e.eval_in(scope)?;
            // Linear scan rather than a hash set: `Bson` is not `Hash`, and a
            // set is small in every case that is not already refused by the cap.
            if !items.iter().any(|existing| existing == &v) {
                items.push(v);
            }
        }
        // Unreachable: `init` pairs each accumulator with its own state.
        _ => {}
    }
    Ok(())
}

fn finish(state: AccState) -> Bson {
    match state {
        // An integer sum stays an integer, exactly rather than approximately:
        // `Total` accumulates in i64 and only widens when an operand is a
        // double or the addition overflows. Same reasoning as ADR-002.
        AccState::Sum(total) => total.to_bson(),
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
        // `localField` is a field path, not an expression: it names the key
        // to join on, and the executor reads the same path the same way
        // when it matches the foreign side, so the two must agree.
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

    fn orders() -> Vec<Document> {
        docs(vec![
            doc! { "_id": 1, "items": [{"sku": "a"}, {"sku": "b"}] },
            doc! { "_id": 2, "items": [{"sku": "a"}, {"sku": "b"}] },
            doc! { "_id": 3, "items": [{"sku": "c"}] },
            doc! { "_id": 4, "items": [] },
            doc! { "_id": 5 },
        ])
    }

    fn strings(items: &[&str]) -> Bson {
        Bson::Array(items.iter().map(|s| Bson::String((*s).into())).collect())
    }

    #[test]
    fn group_by_a_path_through_an_array_buckets_by_the_whole_array() {
        // MongoDB groups on the array value itself, not on each element: the
        // two orders with the same skus share a bucket, and the empty array
        // and the missing field are distinct buckets.
        let out = run(
            vec![
                doc! {"$group": {"_id": "$items.sku", "n": {"$sum": 1}}},
                doc! {"$sort": {"n": -1, "_id": 1}},
            ],
            orders(),
        )
        .unwrap();
        assert_eq!(out.len(), 4, "{out:?}");
        assert_eq!(out[0], doc! { "_id": strings(&["a", "b"]), "n": 2i64 });
        let keys: Vec<&Bson> = out.iter().map(|d| d.get("_id").unwrap()).collect();
        assert!(keys.contains(&&strings(&["c"])), "{keys:?}");
        assert!(keys.contains(&&Bson::Array(vec![])), "{keys:?}");
        assert!(keys.contains(&&Bson::Null), "{keys:?}");
    }

    #[test]
    fn project_and_add_fields_write_the_fanned_array() {
        let out = run(
            vec![
                doc! {"$addFields": {"skus": "$items.sku"}},
                doc! {"$project": {"_id": 1, "skus": 1, "n": {"$size": "$items.sku"}}},
            ],
            orders(),
        )
        .unwrap();
        assert_eq!(out[0], doc! { "_id": 1, "skus": strings(&["a", "b"]), "n": 2i64 });
        assert_eq!(out[3], doc! { "_id": 4, "skus": [], "n": 0i64 });
        // A missing path is null, as it always was; `$size` of null is null
        // rather than an error, as the deviations register records.
        assert_eq!(out[4], doc! { "_id": 5, "skus": Bson::Null, "n": Bson::Null });
    }

    #[test]
    fn a_match_expr_over_a_fanned_path_sees_the_array() {
        // `$expr` is evaluated by the expression layer, so `$items.sku` is
        // the array of skus there — and `$in` can test membership in it.
        let out =
            run(vec![doc! {"$match": {"$expr": {"$in": ["b", "$items.sku"]}}}], orders()).unwrap();
        let ids: Vec<i32> = out.iter().map(|d| d.get_i32("_id").unwrap()).collect();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn push_of_a_fanned_path_pushes_one_array_per_document() {
        let out = run(
            vec![
                doc! {"$match": {"_id": {"$lte": 3}}},
                doc! {"$group": {"_id": null, "all": {"$push": "$items.sku"}}},
            ],
            orders(),
        )
        .unwrap();
        assert_eq!(
            out[0].get("all").unwrap(),
            &Bson::Array(vec![strings(&["a", "b"]), strings(&["a", "b"]), strings(&["c"])])
        );
    }

    #[test]
    fn unwind_and_lookup_keys_read_a_field_path_and_do_not_fan_out() {
        // `$unwind` names a place to write back to, so `$a.b` where `a` is an
        // array reads the first element's `b` as it always has; the fan-out
        // is an expression rule and these are not expressions.
        let input = docs(vec![doc! { "_id": 1, "a": [{"b": [1, 2]}, {"b": [3]}] }]);
        let out = run(vec![doc! {"$unwind": "$a.b"}], input.clone()).unwrap();
        assert_eq!(out.len(), 2, "{out:?}");
        let keys = lookup_keys(&input, "a.b");
        assert_eq!(keys, vec![Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)])]);
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
        assert_eq!(
            Expr::parse(&Bson::String("total".into())).unwrap(),
            Expr::Literal("total".into())
        );
        assert_eq!(
            Expr::parse(&Bson::String("$total".into())).unwrap(),
            Expr::Field("total".into())
        );
    }

    // -- computed expressions in stages -----------------------------------

    #[test]
    fn add_fields_keeps_everything_and_adds() {
        let out = run(
            vec![doc! {"$addFields": {"double": {"$multiply": ["$qty", 2]}}}],
            vec![doc! {"_id": 1, "city": "London", "qty": 5}],
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"_id": 1, "city": "London", "qty": 5, "double": 10i64}]);
    }

    #[test]
    fn set_is_an_alias_for_add_fields() {
        let by_set = run(vec![doc! {"$set": {"n": {"$add": ["$qty", 1]}}}], sample()).unwrap();
        let by_add =
            run(vec![doc! {"$addFields": {"n": {"$add": ["$qty", 1]}}}], sample()).unwrap();
        assert_eq!(by_set, by_add);
    }

    #[test]
    fn add_fields_can_overwrite_an_existing_field() {
        let out = run(
            vec![doc! {"$addFields": {"qty": {"$multiply": ["$qty", 10]}}}],
            vec![doc! {"_id": 1, "qty": 5}],
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"_id": 1, "qty": 50i64}]);
    }

    #[test]
    fn add_fields_writes_a_dotted_path_into_a_subdocument() {
        let out = run(
            vec![doc! {"$addFields": {"totals.doubled": {"$multiply": ["$qty", 2]}}}],
            vec![doc! {"_id": 1, "qty": 5}],
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"_id": 1, "qty": 5, "totals": {"doubled": 10i64}}]);
    }

    #[test]
    fn project_computes_alongside_inclusions() {
        let out = run(
            vec![doc! {"$project": {"city": 1, "double": {"$multiply": ["$qty", 2]}}}],
            vec![doc! {"_id": 1, "city": "London", "qty": 5}],
        )
        .unwrap();
        // `_id` rides along by default, exactly as an ordinary projection.
        assert_eq!(out, vec![doc! {"city": "London", "_id": 1, "double": 10i64}]);
    }

    #[test]
    fn project_can_drop_id_while_computing() {
        let out = run(
            vec![doc! {"$project": {"_id": 0, "double": {"$multiply": ["$qty", 2]}}}],
            vec![doc! {"_id": 1, "qty": 5}],
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"double": 10i64}]);
    }

    #[test]
    fn project_refuses_to_exclude_and_compute_at_once() {
        // Ambiguous about every field nobody named, exactly as mixing
        // inclusion and exclusion is.
        assert!(
            parse(&[doc! {"$project": {"city": 0, "double": {"$multiply": ["$qty", 2]}}}]).is_err()
        );
    }

    #[test]
    fn a_project_without_expressions_behaves_exactly_as_before() {
        let out = run(vec![doc! {"$project": {"city": 1}}], sample()).unwrap();
        assert_eq!(
            out,
            vec![
                doc! {"city": "London", "_id": 1},
                doc! {"city": "London", "_id": 2},
                doc! {"city": "Paris", "_id": 3},
            ]
        );
    }

    #[test]
    fn project_computed_fields_read_the_input_not_the_projection() {
        // `qty` is not projected, so reading it from the output would give
        // null and make the result depend on key order.
        let out = run(
            vec![doc! {"$project": {"city": 1, "double": {"$multiply": ["$qty", 2]}}}],
            vec![doc! {"_id": 1, "city": "London", "qty": 5}],
        )
        .unwrap();
        assert_eq!(out[0].get("double"), Some(&Bson::Int64(10)));
    }

    #[test]
    fn replace_root_promotes_a_subdocument() {
        let out = run(
            vec![doc! {"$replaceRoot": {"newRoot": "$address"}}],
            vec![doc! {"_id": 1, "address": {"city": "London", "zip": "E1"}}],
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"city": "London", "zip": "E1"}]);
    }

    #[test]
    fn replace_root_builds_a_new_document_from_an_expression() {
        let out = run(
            vec![doc! {"$replaceRoot": {"newRoot": {"name": {"$toUpper": "$city"}}}}],
            vec![doc! {"_id": 1, "city": "london"}],
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"name": "LONDON"}]);
    }

    #[test]
    fn replace_root_refuses_a_non_document() {
        // Wrapping it would mean inventing a field name.
        let err =
            run(vec![doc! {"$replaceRoot": {"newRoot": "$qty"}}], vec![doc! {"_id": 1, "qty": 5}]);
        assert!(err.is_err());
    }

    #[test]
    fn replace_root_needs_a_new_root() {
        assert!(parse(&[doc! {"$replaceRoot": {}}]).is_err());
    }

    #[test]
    fn group_takes_an_expression_as_its_key() {
        let out =
            run(vec![doc! {"$group": {"_id": {"$toUpper": "$city"}, "n": {"$sum": 1}}}], sample())
                .unwrap();
        assert_eq!(out, vec![doc! {"_id": "LONDON", "n": 2i64}, doc! {"_id": "PARIS", "n": 1i64}]);
    }

    #[test]
    fn group_takes_an_expression_as_an_accumulator_argument() {
        // The limit this milestone removes: an accumulator argument used to be
        // a field path or a literal and nothing else.
        let out = run(
            vec![
                doc! {"$group": {"_id": Bson::Null, "total": {"$sum": {"$multiply": ["$qty", 2]}}}},
            ],
            sample(),
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"_id": Bson::Null, "total": 60i64}]);
    }

    #[test]
    fn a_compound_group_key_now_computes_instead_of_being_one_bucket() {
        // Behaviour change, deliberately: `{_id: {c: "$city"}}` used to be a
        // constant document, so every input landed in a single bucket.
        let out = run(vec![doc! {"$group": {"_id": {"c": "$city"}, "n": {"$sum": 1}}}], sample())
            .unwrap();
        assert_eq!(
            out,
            vec![doc! {"_id": {"c": "London"}, "n": 2i64}, doc! {"_id": {"c": "Paris"}, "n": 1i64},]
        );
    }

    #[test]
    fn sum_of_large_integers_is_exact_through_the_pipeline() {
        // The precision fix, end to end rather than only on `Total`.
        let big = 9_007_199_254_740_993i64; // 2^53 + 1
        let out = run(
            vec![doc! {"$group": {"_id": Bson::Null, "total": {"$sum": "$n"}}}],
            vec![doc! {"n": big}, doc! {"n": 1i64}],
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"_id": Bson::Null, "total": big + 1}]);
    }

    #[test]
    fn an_expression_error_fails_the_stage_rather_than_yielding_null() {
        let err = run(
            vec![doc! {"$addFields": {"bad": {"$divide": ["$qty", 0]}}}],
            vec![doc! {"_id": 1, "qty": 5}],
        );
        assert!(err.is_err());
    }

    #[test]
    fn the_unsupported_stage_message_lists_the_new_stages() {
        let err = parse(&[doc! {"$bucket": {}}]).unwrap_err().to_string();
        assert!(err.contains("$addFields"), "got: {err}");
        assert!(err.contains("$replaceRoot"), "got: {err}");
    }

    // -- type conversion in stages ----------------------------------------

    #[test]
    fn group_by_a_date_parsed_from_a_string() {
        // The motivating case: a timestamp stored as text, bucketed by month
        // after `$toDate` makes it a date the date operators can read.
        let input = docs(vec![
            doc! { "at": "2026-08-12T10:00:00Z", "n": 1 },
            doc! { "at": "2026-08-20", "n": 2 },
            doc! { "at": "2026-09-01T00:00:00Z", "n": 4 },
        ]);
        let out = run(
            vec![
                doc! {"$group": {
                    "_id": {"$month": {"$toDate": "$at"}},
                    "total": {"$sum": "$n"},
                }},
                doc! {"$sort": {"_id": 1}},
            ],
            input,
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"_id": 8, "total": 3i64}, doc! {"_id": 9, "total": 4i64}]);
    }

    #[test]
    fn a_failed_conversion_fails_the_stage_unless_on_error_is_given() {
        let input = docs(vec![doc! { "qty": "ten" }]);
        assert!(run(vec![doc! {"$addFields": {"n": {"$toInt": "$qty"}}}], input.clone()).is_err());
        let out = run(
            vec![doc! {"$addFields": {
                "n": {"$convert": {"input": "$qty", "to": "int", "onError": 0}}
            }}],
            input,
        )
        .unwrap();
        assert_eq!(out, vec![doc! { "qty": "ten", "n": 0 }]);
    }

    // -- the leading $match -----------------------------------------------

    #[test]
    fn a_leading_match_is_split_off_as_a_filter() {
        let stages = parse(&[doc! {"$match": {"a": 1}}, doc! {"$count": "n"}]).unwrap();
        let (filter, consumed) = leading_match(&stages).expect("a leading $match");
        assert_eq!(consumed, 1);
        assert_eq!(filter, filter::parse(&doc! {"a": 1}).unwrap());
    }

    #[test]
    fn consecutive_leading_matches_merge_into_one_conjunction() {
        let stages = parse(&[
            doc! {"$match": {"a": 1}},
            doc! {"$match": {"b": {"$gt": 2}}},
            doc! {"$count": "n"},
            doc! {"$match": {"n": 0}},
        ])
        .unwrap();
        let (filter, consumed) = leading_match(&stages).expect("a leading $match");
        assert_eq!(consumed, 2, "the $match after $count is not part of the run");
        assert_eq!(
            filter,
            Filter::And(vec![
                filter::parse(&doc! {"a": 1}).unwrap(),
                filter::parse(&doc! {"b": {"$gt": 2}}).unwrap(),
            ])
        );
    }

    // -- variables and arrays in stages -----------------------------------

    #[test]
    fn root_embeds_the_source_document_in_a_projection() {
        let out = run(
            vec![doc! {"$project": {"_id": 0, "src": "$$ROOT", "city": "$$ROOT.city"}}],
            vec![doc! {"_id": 1, "city": "London"}],
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"src": {"_id": 1, "city": "London"}, "city": "London"}]);
    }

    #[test]
    fn array_operators_derive_fields_and_group_keys() {
        let out = run(
            vec![
                doc! {"$addFields": {
                    "n": {"$size": "$tags"},
                    "first": {"$first": "$tags"},
                    "loud": {"$map": {"input": "$tags", "in": {"$toUpper": "$$this"}}},
                }},
                doc! {"$group": {"_id": "$n", "ids": {"$push": "$_id"}}},
                doc! {"$sort": {"_id": 1}},
            ],
            sample(),
        )
        .unwrap();
        assert_eq!(
            out,
            vec![
                doc! {"_id": 0i64, "ids": [3]},
                doc! {"_id": 1i64, "ids": [2]},
                doc! {"_id": 2i64, "ids": [1]},
            ]
        );
    }

    #[test]
    fn a_match_that_is_not_first_is_not_pushed_down() {
        // A `$match` after `$project` reads reshaped documents; moving it to
        // the source would change what it matches.
        let stages = parse(&[doc! {"$project": {"a": 1}}, doc! {"$match": {"a": 1}}]).unwrap();
        assert!(leading_match(&stages).is_none());
        assert!(leading_match(&[]).is_none());
    }

    #[test]
    fn an_unbound_variable_in_a_stage_fails_at_parse() {
        assert!(parse(&[doc! {"$project": {"x": "$$this"}}]).is_err());
        assert!(parse(&[doc! {"$group": {"_id": "$$order"}}]).is_err());
    }

    // -- $lookup: the pipeline form ---------------------------------------

    #[test]
    fn lookup_parses_either_form_and_refuses_a_mixture() {
        let equality = parse(&[doc! {"$lookup": {
            "from": "users", "localField": "uid", "foreignField": "_id", "as": "user"
        }}])
        .unwrap();
        assert!(matches!(&equality[0], Stage::Lookup { join: Join::Equality { .. }, .. }));

        let pipeline = parse(&[doc! {"$lookup": {
            "from": "items",
            "let": {"order_id": "$_id"},
            "pipeline": [{"$project": {"_id": 0, "order": "$$order_id", "sku": 1}}],
            "as": "items",
        }}])
        .unwrap();
        assert!(matches!(&pipeline[0], Stage::Lookup { join: Join::Pipeline { .. }, .. }));
        assert!(pipeline[0].needs_storage());
        assert!(apply(&pipeline[0], sample(), &Limits::default()).is_err());

        // `let` is optional; a pipeline alone is an uncorrelated join.
        assert!(parse(&[doc! {"$lookup": {"from": "a", "pipeline": [], "as": "x"}}]).is_ok());

        // Both at once is refused rather than guessed at.
        let err = parse(&[doc! {"$lookup": {
            "from": "items", "localField": "a", "foreignField": "b",
            "pipeline": [], "as": "items"
        }}])
        .unwrap_err()
        .to_string();
        assert!(err.contains("not both"), "{err}");
        // And neither is refused too.
        assert!(parse(&[doc! {"$lookup": {"from": "items", "as": "items"}}]).is_err());
    }

    #[test]
    fn a_lookup_let_declares_names_for_its_pipeline_and_nothing_outside_it() {
        // Inside the sub-pipeline the variable is known.
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "items", "let": {"oid": "$_id"},
                "pipeline": [{"$addFields": {"o": "$$oid"}}], "as": "items"
            }}])
            .is_ok()
        );
        // A name the `let` does not bind is refused where it is written.
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "items", "let": {"oid": "$_id"},
                "pipeline": [{"$addFields": {"o": "$$order"}}], "as": "items"
            }}])
            .is_err()
        );
        // The `let` values themselves are over the local document, not the
        // sub-pipeline, so they cannot use its names.
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "items", "let": {"a": "$_id", "b": "$$a"},
                "pipeline": [], "as": "items"
            }}])
            .is_err()
        );
        // Names follow the variable rule, and the shape is checked.
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "items", "let": {"Oid": "$_id"}, "pipeline": [], "as": "items"
            }}])
            .is_err()
        );
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "items", "let": "$_id", "pipeline": [], "as": "items"
            }}])
            .is_err()
        );
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "items", "pipeline": {"$match": {}}, "as": "items"
            }}])
            .is_err()
        );
        // After the stage, the name is gone again.
        assert!(parse(&[
            doc! {"$lookup": {"from": "items", "let": {"oid": "$_id"}, "pipeline": [], "as": "i"}},
            doc! {"$addFields": {"o": "$$oid"}},
        ])
        .is_err());
    }

    /// `$match` is parsed with no names bound, so a `let` variable cannot reach
    /// it — which is what lets the executor hoist a leading `$match` out of the
    /// per-input-document loop and apply it to the foreign collection once.
    /// `$expr` put variables within a filter's reach (ADR-106 over ADR-105), so
    /// the boundary is worth holding down: a `let` name is refused, and
    /// `$$ROOT` is accepted because it names the foreign document either way.
    /// If this ever starts parsing, `exec::lookup_pipeline`'s hoist is wrong.
    #[test]
    fn a_match_in_a_lookup_sub_pipeline_cannot_read_the_let() {
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "items", "let": {"oid": "$_id"},
                "pipeline": [{"$match": {"$expr": {"$eq": ["$order", "$$oid"]}}}], "as": "items"
            }}])
            .is_err()
        );
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "items", "let": {"oid": "$_id"},
                "pipeline": [{"$match": {"$expr": {"$gt": ["$$ROOT.qty", 0]}}}], "as": "items"
            }}])
            .is_ok()
        );
    }

    /// The executor's loop, in miniature: bind the `let` per input document
    /// and run the sub-pipeline over the foreign documents with it in scope.
    /// What `kimmy-api` does with a storage handle, done here over vectors so
    /// the variable plumbing is tested where it lives.
    fn join_in_memory(stage: &Stage, input: Vec<Document>, foreign: &[Document]) -> Vec<Document> {
        let Stage::Lookup { as_field, join: Join::Pipeline { vars, stages }, .. } = stage else {
            panic!("expected the pipeline form");
        };
        let limits = Limits::default();
        let mut out = Vec::new();
        for mut doc in input {
            let bound = bind_let(vars, &doc, &[]).unwrap();
            let frame: Vec<Binding<'_>> = bound.iter().map(|(n, v)| (n.as_str(), v)).collect();
            let mut current = foreign.to_vec();
            for stage in stages {
                current = apply_with_vars(stage, current, &limits, &frame).unwrap();
            }
            doc.insert(
                as_field.clone(),
                Bson::Array(current.into_iter().map(Bson::from).collect()),
            );
            out.push(doc);
        }
        out
    }

    #[test]
    fn a_lookup_let_is_evaluated_per_input_document_and_read_by_the_sub_pipeline() {
        let stages = parse(&[doc! {"$lookup": {
            "from": "items",
            "let": {"order": "$_id", "min_qty": "$threshold"},
            "pipeline": [
                {"$match": {"kind": "line"}},
                {"$project": {
                    "_id": 0,
                    "sku": 1,
                    "for_order": "$$order",
                    "over": {"$gte": ["$qty", "$$min_qty"]},
                }},
            ],
            "as": "lines",
        }}])
        .unwrap();
        let orders = vec![doc! {"_id": 1, "threshold": 5}, doc! {"_id": 2, "threshold": 1}];
        let items = vec![
            doc! {"sku": "a", "qty": 3, "kind": "line"},
            doc! {"sku": "b", "qty": 9, "kind": "line"},
            doc! {"sku": "c", "qty": 9, "kind": "note"},
        ];

        let out = join_in_memory(&stages[0], orders, &items);
        assert_eq!(
            out[0].get_array("lines").unwrap(),
            &vec![
                Bson::Document(doc! {"sku": "a", "for_order": 1, "over": false}),
                Bson::Document(doc! {"sku": "b", "for_order": 1, "over": true}),
            ]
        );
        assert_eq!(
            out[1].get_array("lines").unwrap(),
            &vec![
                Bson::Document(doc! {"sku": "a", "for_order": 2, "over": true}),
                Bson::Document(doc! {"sku": "b", "for_order": 2, "over": true}),
            ]
        );
    }

    #[test]
    fn inside_a_sub_pipeline_root_is_the_foreign_document_and_let_reaches_every_stage() {
        // `$group` and `$replaceRoot` take the variables too, not only the
        // computed-field stages.
        let stages = parse(&[doc! {"$lookup": {
            "from": "items",
            "let": {"tag": "$label"},
            "pipeline": [
                {"$group": {"_id": "$$tag", "n": {"$sum": 1}, "docs": {"$push": "$$ROOT.sku"}}},
                {"$replaceRoot": {"newRoot": {"label": "$_id", "count": "$n", "skus": "$docs"}}},
            ],
            "as": "summary",
        }}])
        .unwrap();
        let out = join_in_memory(
            &stages[0],
            vec![doc! {"_id": 1, "label": "x"}],
            &[doc! {"sku": "a"}, doc! {"sku": "b"}],
        );
        assert_eq!(
            out[0].get_array("summary").unwrap(),
            &vec![Bson::Document(doc! {"label": "x", "count": 2i64, "skus": ["a", "b"]})]
        );
    }

    #[test]
    fn bind_let_layers_over_outer_bindings_and_shadows_them() {
        let vars = vec![
            (
                "inner".to_string(),
                Expr::parse_with_vars(&"$$outer".into(), &["outer".into()]).unwrap(),
            ),
            ("outer".to_string(), Expr::Literal("rebound".into())),
        ];
        let outer_value = Bson::String("from outside".into());
        let outer = [("outer", &outer_value)];
        let bound = bind_let(&vars, &doc! {}, &outer).unwrap();
        assert_eq!(bound[0], ("inner".to_string(), "from outside".into()));
        assert_eq!(bound[1], ("outer".to_string(), "rebound".into()));

        // Appended after the outer frame, the rebinding wins.
        let frame: Vec<Binding<'_>> =
            outer.iter().copied().chain(bound.iter().map(|(n, v)| (n.as_str(), v))).collect();
        let d = doc! {};
        let read = Expr::parse_with_vars(&"$$outer".into(), &["outer".into()]).unwrap();
        assert_eq!(read.eval_in(&Scope::with_bindings(&d, &frame)).unwrap(), "rebound".into());
    }
}
