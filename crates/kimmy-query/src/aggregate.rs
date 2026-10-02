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
//! `$sort`, `$group` and `$count` cannot emit anything until they have consumed
//! everything: a sort has no first element until the last is seen, a group has
//! no totals until the last member arrives, and a count has no number until the
//! last document has gone past. `$count` emits one document whatever it
//! consumed. `$unwind` and `$lookup` can *grow* their input rather than shrink
//! it.
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
        /// `includeArrayIndex`: the name of a field to hold the position of
        /// the element that produced each output document, `null` on a
        /// document that was not fanned out from an array.
        include_array_index: Option<String>,
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

/// Parse a pipeline whose expressions may read `vars`, at top-level
/// placement.
///
/// A `$lookup` sub-pipeline is not parsed through here: `parse_lookup` parses
/// it internally, with the outer names and its own `let` in scope and at
/// [`Placement::SubPipeline`], which is where the `$match` variable refusal
/// applies. This entry point exists for a caller that has names bound by
/// some other means and wants the top-level rules.
///
/// Every `$regex` in the pipeline, its `$lookup` sub-pipelines included, is
/// charged to one budget, the one a single filter has
/// ([`crate::filter::REGEX_TOTAL_LIMIT_BYTES`]): the compiled patterns of every
/// stage are held together while the pipeline runs.
pub fn parse_with_vars(pipeline: &[Document], vars: &[String]) -> Result<Vec<Stage>> {
    let _budget = crate::filter::RegexBudget::open();
    parse_pipeline(pipeline, vars, Placement::TopLevel)
}

/// Where a pipeline sits, for the one rule that depends on it.
///
/// A `$match` reads the same filter language in both places; the difference
/// is what a `"$$name"` string value most likely means there. At the top
/// level there is no `let` for it to refer to, so it is the literal string a
/// stored document may hold. Inside a `$lookup` sub-pipeline it is almost
/// certainly a variable the author expected the filter to substitute, and
/// the filter never will — see [`refuse_variables_in_match`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Placement {
    TopLevel,
    SubPipeline,
}

fn parse_pipeline(pipeline: &[Document], vars: &[String], at: Placement) -> Result<Vec<Stage>> {
    pipeline.iter().map(|stage| parse_stage(stage, vars, at)).collect()
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
        // Cheap clauses first, as within one filter (ADR-206).
        n => Some((filter::conjunction(filters), n)),
    }
}

fn parse_stage(stage: &Document, vars: &[String], at: Placement) -> Result<Stage> {
    if stage.len() != 1 {
        return Err(Error::InvalidQuery(format!(
            "a pipeline stage must have exactly one key naming the operator, found {}",
            stage.len()
        )));
    }
    let (name, value) = stage.iter().next().expect("length checked above");

    match name.as_str() {
        "$match" => {
            let raw = as_document(name, value)?;
            if at == Placement::SubPipeline {
                refuse_variables_in_match(raw)?;
            }
            Ok(Stage::Match(Box::new(filter::parse(raw)?)))
        }
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

/// Refuse a `"$$name"` string value anywhere in a sub-pipeline `$match`.
///
/// The filter language has no variables. `filter::parse` reads `"$$oid"` as
/// the five-character string it is, so `{$match: {_id: "$$oid"}}` in a
/// `$lookup` sub-pipeline matched nothing, and the join came back as an empty
/// array on every document with no error — a wrong answer that looks exactly
/// like a right one, which is the kind this codebase refuses hardest. Inside
/// a sub-pipeline a `$$` string can only be a `let` name the author expected
/// to be substituted (or `$$ROOT`, which a filter has no use for either), so
/// it is refused with the idiom that works. The rule is deliberately scoped:
/// a top-level `$match` and `find` keep the literal reading, because a stored
/// document may hold that string and there is no `let` to confuse it with.
///
/// The walk covers every value at every depth — a plain equality, an
/// operator document (`$eq`, `$in: [...]`, `$not`), `$elemMatch`, the arrays
/// under `$and`/`$or`/`$nor` — and skips the subtree under `$expr`, which is
/// the expression parser's: it resolves variables itself and already refuses
/// an unbound one. `$regex` and `$options` strings are values like any other
/// and are not special-cased, so a pattern beginning with `$$` is refused
/// too; nothing meaningful is lost, since `$` anchors and a pattern that
/// opens with two of them can match nothing a `$$`-free one cannot.
fn refuse_variables_in_match(doc: &Document) -> Result<()> {
    fn walk_doc(doc: &Document) -> Result<()> {
        doc.iter().filter(|(key, _)| key.as_str() != "$expr").try_for_each(|(_, value)| walk(value))
    }
    fn walk(value: &Bson) -> Result<()> {
        match value {
            Bson::String(s) if s.starts_with("$$") => Err(Error::InvalidQuery(format!(
                "{s} is not available in a $match: the filter language has no variables; bind \
                 it in an $addFields stage and $match on the computed field, or use $expr once \
                 it composes with let"
            ))),
            Bson::Document(doc) => walk_doc(doc),
            Bson::Array(items) => items.iter().try_for_each(walk),
            _ => Ok(()),
        }
    }
    walk_doc(doc)
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

    Ok(Stage::Project { projection: Some(Projection::Include(include.into())), computed })
}

fn parse_replace_root(spec: &Document, vars: &[String]) -> Result<Stage> {
    deny_unknown_keys("$replaceRoot", spec, &["newRoot"])?;
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

/// Refuse a key `doc` carries that is not in `allowed`.
///
/// A stage operand with a *fixed* key set — `$unwind`'s document form,
/// `$lookup`'s both forms, `$replaceRoot` — gets the same closure ADR-121
/// gives the request body and the shapes nested inside it: a field the stage
/// does not define is refused rather than quietly ignored. `$match` and
/// `$project` are field-path maps, not fixed-key documents — every key in
/// them is data, not vocabulary — so they are never run through this and
/// must stay open.
fn deny_unknown_keys(stage: &str, doc: &Document, allowed: &[&str]) -> Result<()> {
    for key in doc.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(Error::InvalidQuery(format!(
                "{stage} does not accept `{key}`; it takes: {}",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
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
            Expr::Field(p) => Ok(Stage::Unwind {
                path: p,
                preserve_null_and_empty: false,
                include_array_index: None,
            }),
            _ => Err(Error::InvalidQuery(
                "$unwind takes a field path beginning with $, e.g. {$unwind: \"$tags\"}".into(),
            )),
        },
        Bson::Document(d) => {
            deny_unknown_keys(
                "$unwind",
                d,
                &["path", "preserveNullAndEmptyArrays", "includeArrayIndex"],
            )?;
            let field = d.get("path").ok_or_else(|| {
                Error::InvalidQuery("$unwind needs a `path`, e.g. {path: \"$tags\"}".into())
            })?;
            let Expr::Field(p) = Expr::parse(field)? else {
                return Err(Error::InvalidQuery(
                    "$unwind `path` must begin with $, e.g. \"$tags\"".into(),
                ));
            };
            // A wrong-typed value is refused by name, not read as `false` —
            // the same shape of hazard the finding this ADR is named after
            // reports: `"preserveNullAndEmptyArrays": "true"` or `: 1` would
            // otherwise answer `200` with the option silently reverted.
            let preserve = match d.get("preserveNullAndEmptyArrays") {
                None => false,
                Some(Bson::Boolean(b)) => *b,
                Some(other) => {
                    return Err(Error::InvalidQuery(format!(
                        "$unwind `preserveNullAndEmptyArrays` must be a boolean, found {}",
                        type_name(other)
                    )));
                }
            };
            let include_array_index = match d.get("includeArrayIndex") {
                None => None,
                Some(Bson::String(name)) => {
                    if name.is_empty() {
                        return Err(Error::InvalidQuery(
                            "$unwind `includeArrayIndex` cannot be an empty field name".into(),
                        ));
                    }
                    // A name this language's own field-path syntax cannot
                    // read back: `"$name"` in an expression means the field
                    // called `name`, not one called `$name`, so a field
                    // beginning with `$` could be written by this stage and
                    // never referenced by a later one.
                    if name.split('.').any(|seg| seg.starts_with('$')) {
                        return Err(Error::InvalidQuery(format!(
                            "$unwind `includeArrayIndex` cannot be {name:?}: a field name \
                             beginning with $ cannot be read back by this language's own \
                             field-path syntax"
                        )));
                    }
                    // Naming the unwound path itself would overwrite the
                    // element `$unwind` just placed there with its index,
                    // silently, on every row.
                    if *name == p {
                        return Err(Error::InvalidQuery(format!(
                            "$unwind `includeArrayIndex` cannot be the same field as `path` \
                             ({name:?}): it would overwrite the unwound element with its index"
                        )));
                    }
                    Some(name.clone())
                }
                Some(other) => {
                    return Err(Error::InvalidQuery(format!(
                        "$unwind `includeArrayIndex` takes the name of a field, e.g. \"i\", \
                         found {}",
                        type_name(other)
                    )));
                }
            };
            Ok(Stage::Unwind { path: p, preserve_null_and_empty: preserve, include_array_index })
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
    // Closed over the union of both forms' keys; which subset is actually
    // legal together is decided below, where the two forms are told apart.
    deny_unknown_keys(
        "$lookup",
        spec,
        &["from", "as", "localField", "foreignField", "let", "pipeline"],
    )?;
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
            // A sub-pipeline is one whether or not this `$lookup` has a
            // `let`, and wherever the `$lookup` itself sits.
            let stages = parse_pipeline(&raw_stages, &in_scope, Placement::SubPipeline)?;
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
pub(crate) fn group_key(value: &Bson) -> Vec<u8> {
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
        // A `$expr` that cannot be evaluated against a document fails the
        // pipeline, as in a top-level filter (ADR-206).
        Stage::Match(f) => {
            let mut out = Vec::with_capacity(input.len());
            for doc in input {
                if filter::matches(f, &doc)? {
                    out.push(doc);
                }
            }
            out
        }
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
            for doc in &docs {
                shape::refuse_unsortable(keys, doc)?;
            }
            shape::sort(keys, &mut docs);
            docs
        }
        Stage::Limit(n) => input.into_iter().take(*n).collect(),
        Stage::Skip(n) => input.into_iter().skip(*n).collect(),
        Stage::Count(field) => {
            let n = input.len() as i64;
            vec![bson::doc! { field.as_str(): n }]
        }
        Stage::Unwind { path: p, preserve_null_and_empty, include_array_index } => {
            unwind(input, p, *preserve_null_and_empty, include_array_index.as_deref(), limits)?
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
///
/// An [`Error::Limit`], not an `InvalidQuery`: the ceiling is a budget for
/// the whole request, which an `$and`, `$or` or `$last` must never hold back
/// while another value decides (ADR-211).
pub fn check_limit(stage: &str, produced: usize, limits: &Limits) -> Result<()> {
    if produced > limits.max_documents {
        return Err(Error::Limit(format!(
            "{stage} produced {produced} documents, over the pipeline limit of {}. Narrow the \
             pipeline with an earlier $match",
            limits.max_documents
        )));
    }
    Ok(())
}

/// Refuse a `$lookup` that would attach more than the ceiling's worth of
/// documents in all (ADR-210): the sum of the lengths of the arrays it adds.
/// Checked before any is copied, because each attachment is a copy of the
/// foreign document, and a key an array fans out can attach one document to
/// every element of every input document.
pub fn check_attached(stage: &str, attached: usize, limits: &Limits) -> Result<()> {
    if attached > limits.max_documents {
        return Err(Error::InvalidQuery(format!(
            "{stage} would attach {attached} documents in all, over the pipeline limit of {}. \
             Narrow the pipeline with an earlier $match, or $unwind the array first",
            limits.max_documents
        )));
    }
    Ok(())
}

/// Where writing to `field` in `doc` would fail, if it would: `(array_path,
/// remainder)`, `array_path` the dotted prefix naming the array itself,
/// `remainder` what was left of `field` to read from each of its elements.
/// `None` when the write would succeed.
///
/// Mirrors `path::set`'s own traversal read-only, without cloning `doc` to
/// probe it — a non-terminal segment lands on an array whose next segment
/// names a field rather than a numeric position, `path::set`'s one failure
/// mode. A segment that is missing, a scalar, or an array reached by an
/// index that is not already a document is what `path::set` vivifies or
/// overwrites rather than fails on; none of those can already contain an
/// array of their own, so this walk stops there and reports no crossing,
/// exactly matching `path::set` succeeding by vivifying it.
///
/// A **numeric** segment after the array is the one case this deliberately
/// does not report — `$unwind: "$a.0.b"` writes by index, same as any
/// numeric-indexed write, and is not "crossing" in this function's sense.
/// ADR-116 names this the one place its own non-fanning read and the
/// expression layer's fanning read disagree, and that disagreement stays
/// observable through `$unwind` here — see
/// `unwind_over_a_numeric_segment_into_a_crossed_array_reads_by_index_not_by_fanning`
/// and ADR-130. An earlier revision of this function's own documentation
/// claimed the uniform check made `$unwind`'s reader choice unobservable
/// everywhere; that was wrong precisely because of this case, and is
/// corrected here rather than repeated.
fn crossing_array(doc: &Document, field: &str) -> Option<(String, String)> {
    let segs: Vec<&str> = field.split('.').collect();
    let mut current = doc;
    let mut i = 0;
    while i + 1 < segs.len() {
        match current.get(segs[i]) {
            Some(Bson::Document(child)) => {
                current = child;
                i += 1;
            }
            Some(Bson::Array(items)) => match segs[i + 1].parse::<usize>() {
                // A numeric next segment addresses a position, not a
                // crossing; recurse into that element only if it is already
                // a document — anything else is what `path::set` vivifies or
                // overwrites, so nothing further along this path can fail.
                Ok(index) => match items.get(index) {
                    Some(Bson::Document(child)) => {
                        current = child;
                        i += 2;
                    }
                    _ => return None,
                },
                Err(_) => return Some((segs[..=i].join("."), segs[i + 1..].join("."))),
            },
            _ => return None,
        }
    }
    None
}

fn unwind(
    input: Vec<Document>,
    field: &str,
    preserve: bool,
    include_index: Option<&str>,
    limits: &Limits,
) -> Result<Vec<Document>> {
    // Write the index field alongside the unwound value, or `null` on a
    // document that passed through without being fanned out — MongoDB's rule,
    // and the only sane reading of "which element produced this row" for a
    // row that was not produced by one.
    let set_index = |copy: &mut Document, index: Option<i64>| -> Result<()> {
        let Some(name) = include_index else { return Ok(()) };
        let value = index.map_or(Bson::Null, Bson::Int64);
        path::set(copy, name, value)
            .map_err(|e| Error::InvalidQuery(format!("cannot set `{name}`: {e}")))
    };

    let mut out = Vec::with_capacity(input.len());
    for doc in input {
        // Refused uniformly, for every document, before anything at `field`
        // is read: `$unwind` names a place to write each element back to,
        // and a path that crosses an array has none, regardless of what is
        // sitting there — an array to expand, a scalar, `null`, or nothing
        // at all. See ADR-130.
        if let Some((array_path, remainder)) = crossing_array(&doc, field) {
            return Err(Error::InvalidQuery(format!(
                "$unwind cannot expand `{field}`: `{array_path}` is an array, so there is no \
                 single place to write each element back to. Unwind `${array_path}` first, then \
                 read `{remainder}` on each resulting row"
            )));
        }
        // `path` is a field path: `$unwind` names the array to expand, and
        // `path::set` writes each element back to that same place. Past the
        // check above, `field` does not cross an array in this document, so
        // every write below is guaranteed to succeed; the `Result` is still
        // propagated with `?` rather than unwrapped, on the same principle
        // as everywhere else in this module — a proof that holds today is
        // not a reason to let a future change panic instead of refuse.
        match value_at(&doc, field) {
            Some(Bson::Array(items)) if !items.is_empty() => {
                for (index, item) in items.clone().into_iter().enumerate() {
                    let mut copy = doc.clone();
                    path::set(&mut copy, field, item)
                        .map_err(|e| Error::InvalidQuery(format!("cannot set `{field}`: {e}")))?;
                    set_index(&mut copy, Some(index as i64))?;
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
                    set_index(&mut copy, None)?;
                    out.push(copy);
                }
            }
            // A non-array value unwinds to itself, which is what MongoDB does
            // and saves callers a `$type` check for a field that is sometimes
            // scalar and sometimes an array.
            Some(_) => {
                let mut copy = doc;
                set_index(&mut copy, None)?;
                out.push(copy);
            }
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
    First(Option<Bson>),
    /// The latest document's value, or why it has none. Only the last
    /// document's value is the answer, so an earlier one that cannot be
    /// evaluated is replaced by the next, and fails the request only if no
    /// later document comes (ADR-211).
    Last(Option<Result<Bson>>),
    Push(Vec<Bson>),
    /// The members in first-seen order, and the [`group_key`] of each, which
    /// is what makes two values one member (ADR-186).
    AddToSet(Vec<Bson>, std::collections::HashSet<Vec<u8>>),
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

    order
        .into_iter()
        .map(|bucket| {
            let mut out = Document::new();
            out.insert("_id", bucket.key);
            for (state, (name, _)) in bucket.values.into_iter().zip(fields) {
                out.insert(name.clone(), finish(state)?);
            }
            Ok(out)
        })
        .collect()
}

fn init((_, acc): &(String, Accumulator)) -> AccState {
    match acc {
        Accumulator::Sum(_) => AccState::Sum(Total::default()),
        Accumulator::Avg(_) => AccState::Avg(0.0, 0),
        Accumulator::Min(_) | Accumulator::Max(_) => AccState::MinMax(None),
        Accumulator::First(_) => AccState::First(None),
        Accumulator::Last(_) => AccState::Last(None),
        Accumulator::Push(_) => AccState::Push(Vec::new()),
        Accumulator::AddToSet(_) => AccState::AddToSet(Vec::new(), Default::default()),
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
        (AccState::First(current), Accumulator::First(e)) => {
            if current.is_none() {
                *current = Some(e.eval_in(scope)?);
            }
        }
        (AccState::Last(current), Accumulator::Last(e)) => match e.eval_in(scope) {
            // A broken invariant is not a value a later document replaces.
            Err(err) if !err.is_deferrable() => return Err(err),
            value => *current = Some(value),
        },
        (AccState::Push(items), Accumulator::Push(e)) => items.push(e.eval_in(scope)?),
        (AccState::AddToSet(items, keys), Accumulator::AddToSet(e)) => {
            let v = e.eval_in(scope)?;
            // A member is identified the way `$group` identifies a bucket, and
            // the first occurrence stays (ADR-186).
            if keys.insert(group_key(&v)) {
                items.push(v);
            }
        }
        // Unreachable: `init` pairs each accumulator with its own state.
        _ => {}
    }
    Ok(())
}

fn finish(state: AccState) -> Result<Bson> {
    Ok(match state {
        // An integer sum stays an integer, exactly rather than approximately:
        // `Total` accumulates in i64 and only widens when an operand is a
        // double or the addition overflows. Same reasoning as ADR-002.
        AccState::Sum(total) => total.to_bson(),
        AccState::Avg(_, 0) => Bson::Null,
        AccState::Avg(total, n) => Bson::Double(total / n as f64),
        AccState::MinMax(v) | AccState::First(v) => v.unwrap_or(Bson::Null),
        AccState::Last(v) => v.transpose()?.unwrap_or(Bson::Null),
        AccState::Push(items) | AccState::AddToSet(items, _) => Bson::Array(items),
    })
}

fn numeric(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(n) => Some(f64::from(*n)),
        Bson::Int64(n) => Some(*n as f64),
        Bson::Double(d) => Some(*d),
        _ => None,
    }
}

/// The values a document joins under at `field`, as a filter reads the path
/// (ADR-210): every value the path resolves to, and, for one that is an array,
/// each of its elements as well, one level down. `items.sku` over `items:
/// [{sku: "a"}, {sku: "b"}]` is `"a"` and `"b"`; `tags` over `tags: ["a", "b"]`
/// is `["a", "b"]`, `"a"` and `"b"`; `a.b` over `a: [{b: [1, 2]}, {b: [3]}]` is
/// `[1, 2]`, `1`, `2`, `[3]` and `3`. Nothing at the path is no value, so a
/// document without the field joins nothing. In a filter, `{field: v}` matches a
/// document when `v` is one of these (`kimmy_core::matching::any_element`), and
/// a join matches two documents when their values share one, so a join and a
/// filter agree about what a field's value is.
///
/// Not deduplicated: the executor keys them, and equal keys are one there.
pub fn join_values<'a>(doc: &'a Document, field: &str) -> Vec<&'a Bson> {
    let mut out = Vec::new();
    for value in path::resolve(doc, field) {
        out.push(value);
        if let Bson::Array(items) = value {
            out.extend(items.iter());
        }
    }
    out
}

/// The distinct values a `$lookup` needs from its input: every value of
/// [`join_values`] of every input document, once each.
///
/// Exposed so the executor can fetch the foreign side in **one** pass instead
/// of once per document: a join done per input document is O(n·m), and on a
/// collection of any size that is the difference between a query and an outage.
/// The executor reads the foreign side through the same function, so the two
/// agree about what a key is, and a document that has no value at the path
/// wants nothing.
pub fn lookup_keys(input: &[Document], local_field: &str) -> Vec<Bson> {
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut keys = Vec::new();
    for doc in input {
        for value in join_values(doc, local_field) {
            if seen.insert(group_key(value)) {
                keys.push(value.clone());
            }
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
    fn a_match_stage_compiles_its_regex_once_whatever_the_input() {
        // A leading `$match` and one after another stage both run the filter
        // parsed with the pipeline, so its `$regex` is compiled there and
        // never per document.
        let many: Vec<Document> = (0..300)
            .map(|i| doc! { "_id": (i as i64), "city": (["London", "Paris", "Lisbon"][i % 3]), "qty": (i as i64) })
            .collect();
        let before = crate::filter::regex_compilations();
        let stages = parse(&[
            doc! {"$match": {"city": {"$regex": "^L", "$options": "i"}}},
            doc! {"$addFields": {"twice": {"$multiply": ["$qty", 2]}}},
            doc! {"$match": {"city": {"$not": {"$regex": "bon$"}}}},
        ])
        .unwrap();
        assert_eq!(crate::filter::regex_compilations() - before, 2, "one per $regex, at parse");
        let before = crate::filter::regex_compilations();
        let mut current = many;
        for stage in &stages {
            current = apply(stage, current, &Limits::default()).unwrap();
        }
        assert_eq!(current.len(), 100, "the London documents");
        assert_eq!(crate::filter::regex_compilations() - before, 0, "none while matching");
    }

    #[test]
    fn a_pipeline_holds_its_regex_patterns_under_one_budget() {
        // One `\w{15}`, about 0.8 MiB, fits one filter's 2 MiB, and so do
        // two stages of one; three stages do not, because every stage's
        // patterns are held together.
        let one = |n: usize| doc! {"$match": {"s": {"$regex": format!(r"\w{{15}}{n}")}}};
        parse(&[one(0), one(1)]).unwrap();
        let err = parse(&[one(0), doc! {"$limit": 5}, one(1), one(2)]).unwrap_err().to_string();
        assert!(err.contains("more than 2 MiB together"), "{err}");
        // A `$lookup` sub-pipeline is charged to the same budget.
        let lookup = doc! {"$lookup": {"from": "items", "pipeline": [one(2)], "as": "items"}};
        parse(&[one(0), lookup.clone()]).unwrap();
        let err = parse(&[one(0), one(1), lookup]).unwrap_err().to_string();
        assert!(err.contains("more than 2 MiB together"), "{err}");
        // And each pipeline has its own.
        parse(&[one(0), one(1)]).unwrap();
    }

    #[test]
    fn match_then_count() {
        let out =
            run(vec![doc! {"$match": {"city": "London"}}, doc! {"$count": "n"}], sample()).unwrap();
        assert_eq!(out, vec![doc! { "n": 2i64 }]);
    }

    #[test]
    fn count_over_an_empty_stream_is_one_document_holding_zero() {
        // The count of nothing is defined, and it is 0. Emitting no document
        // would make every caller spell a defensive read for a value the stage
        // always knows. Four differently-shaped ways of emptying the stream —
        // a filter matching nothing, a fan-out dropping every row, a `$skip`
        // past the end, an explicit `$limit: 0` — and one answer for all.

        // The unwind case needs pinning beyond "the prefix came out empty":
        // if `_id: 3` ever left `sample()`, `$match` would select nothing,
        // `$unwind` would never run, the prefix would still be empty, and the
        // case would have silently degenerated into a second copy of the
        // filter one below. So assert one document reaches `$unwind`, and that
        // its `tags` is the empty array that makes the fan-out drop it.
        let reaches_unwind = run(vec![doc! {"$match": {"_id": 3}}], sample()).unwrap();
        assert_eq!(reaches_unwind.len(), 1, "$unwind must do the emptying: {reaches_unwind:?}");
        assert!(reaches_unwind[0].get_array("tags").unwrap().is_empty(), "{reaches_unwind:?}");

        for prefix in [
            vec![doc! {"$match": {"city": "Berlin"}}],
            vec![doc! {"$match": {"_id": 3}}, doc! {"$unwind": "$tags"}],
            vec![doc! {"$skip": 999}],
            vec![doc! {"$limit": 0}],
        ] {
            // Each prefix must really empty the stream, or the count below
            // would be the count of a non-empty one — this catches `$skip:
            // 999` becoming reachable, or `"Berlin"` starting to match.
            let emptied = run(prefix.clone(), sample()).unwrap();
            assert!(emptied.is_empty(), "{prefix:?} left {emptied:?}");

            let mut pipeline = prefix.clone();
            pipeline.push(doc! {"$count": "n"});
            let out = run(pipeline, sample()).unwrap();
            assert_eq!(out, vec![doc! { "n": 0i64 }], "{prefix:?}");
        }
    }

    #[test]
    fn group_over_an_empty_stream_produces_no_groups_at_all() {
        // One row per distinct key, and an empty stream has no keys — `_id:
        // null` included, which is the one a caller expects to be exempt. Not
        // an inconsistency with `$count` above: it is the same rule answering
        // a different question, so a pipeline ending in `$group` can
        // legitimately answer nothing.
        let emptied = run(vec![doc! {"$match": {"city": "Berlin"}}], sample()).unwrap();
        assert!(emptied.is_empty(), "the filter must select nothing: {emptied:?}");

        let out = run(
            vec![
                doc! {"$match": {"city": "Berlin"}},
                doc! {"$group": {"_id": null, "n": {"$sum": 1}}},
            ],
            sample(),
        )
        .unwrap();
        assert!(out.is_empty(), "{out:?}");
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

    // -- ADR-129: stage operand documents are closed -----------------------

    #[test]
    fn a_misspelled_unwind_option_is_refused_not_silently_ignored() {
        // The load-bearing pair, one character apart. Before ADR-129 both
        // answered `200`: the correctly-spelled option kept the document
        // (1 row), and the misspelling silently reverted to `false` and
        // dropped it (0 rows) — a typo quietly changing the result.
        let input = docs(vec![doc! { "_id": 1 }]);

        let out = run(
            vec![doc! {"$unwind": {"path": "$nosuchfield", "preserveNullAndEmptyArrays": true}}],
            input.clone(),
        )
        .unwrap();
        assert_eq!(out.len(), 1, "correctly spelled: kept, as asked");

        let err = run(
            vec![doc! {"$unwind": {"path": "$nosuchfield", "preserveNullAndEmptyArray": true}}],
            input,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("preserveNullAndEmptyArray"), "{err}");
        assert!(err.contains("$unwind"), "{err}");
    }

    #[test]
    fn unwind_include_array_index_is_implemented_not_dropped() {
        // A real MongoDB option a porting client would send; before ADR-129
        // it was accepted and silently dropped (a `200` with no index field).
        let out =
            run(vec![doc! {"$unwind": {"path": "$tags", "includeArrayIndex": "i"}}], sample())
                .unwrap();
        assert_eq!(out.len(), 3, "{out:?}");
        assert_eq!(out[0].get_i64("i").unwrap(), 0);
        assert_eq!(out[1].get_i64("i").unwrap(), 1);

        // A document that unwinds to itself (or is preserved) gets `null`,
        // not the field omitted — it did not come from an array element.
        let input = docs(vec![doc! { "_id": 1, "tags": "scalar" }]);
        let out = run(vec![doc! {"$unwind": {"path": "$tags", "includeArrayIndex": "i"}}], input)
            .unwrap();
        assert_eq!(out[0].get("i"), Some(&Bson::Null));
    }

    #[test]
    fn a_wrong_typed_preserve_null_and_empty_arrays_is_refused_not_silently_false() {
        // The load-bearing pair with the typo moved from the key to the
        // value: `Bson::as_bool` returns `None` for anything that is not
        // literally a boolean, so `"true"` and `1` used to fall through
        // `.unwrap_or(false)` exactly like a missing key — the same silent
        // `preserveNullAndEmptyArrays` failure finding 11 is named after,
        // now in the value rather than the key.
        let input = docs(vec![doc! { "_id": 1 }]);
        for wrong in [Bson::String("true".into()), Bson::Int32(1)] {
            let err = run(
                vec![
                    doc! {"$unwind": {"path": "$nosuchfield", "preserveNullAndEmptyArrays": wrong}},
                ],
                input.clone(),
            )
            .unwrap_err()
            .to_string();
            assert!(err.contains("preserveNullAndEmptyArrays"), "{err}");
            assert!(err.contains("boolean"), "{err}");
        }
    }

    #[test]
    fn include_array_index_refuses_a_name_this_language_cannot_read_back() {
        // A field beginning with `$` can be written here but never read back
        // through `"$name"` field-path syntax, which means the field called
        // `name`, not one called `$name` — see aggregation.md's operator
        // rule (a document whose first key starts with `$`).
        let err =
            run(vec![doc! {"$unwind": {"path": "$tags", "includeArrayIndex": "$bad"}}], sample())
                .unwrap_err()
                .to_string();
        assert!(err.contains("$bad"), "{err}");

        // Naming the unwound path itself would overwrite the element
        // `$unwind` just placed there with its own index, silently.
        let err =
            run(vec![doc! {"$unwind": {"path": "$tags", "includeArrayIndex": "tags"}}], sample())
                .unwrap_err()
                .to_string();
        assert!(err.contains("tags"), "{err}");
        assert!(err.contains("path"), "{err}");
    }

    #[test]
    fn an_unknown_unwind_key_is_refused() {
        let err = run(vec![doc! {"$unwind": {"path": "$tags", "bogusOption": true}}], sample())
            .unwrap_err()
            .to_string();
        assert!(err.contains("bogusOption"), "{err}");
        assert!(err.contains("$unwind"), "{err}");
    }

    #[test]
    fn an_unknown_replace_root_key_is_refused() {
        let err =
            parse(&[doc! {"$replaceRoot": {"newRoot": "$a", "bogus": 1}}]).unwrap_err().to_string();
        assert!(err.contains("bogus"), "{err}");
        assert!(err.contains("$replaceRoot"), "{err}");
    }

    #[test]
    fn an_unknown_lookup_key_is_refused_in_both_forms() {
        let err = parse(&[doc! {"$lookup": {
            "from": "users", "localField": "uid", "foreignField": "_id", "as": "user", "bogus": 1
        }}])
        .unwrap_err()
        .to_string();
        assert!(err.contains("bogus"), "{err}");
        assert!(err.contains("$lookup"), "{err}");

        let err = parse(&[doc! {"$lookup": {
            "from": "items", "pipeline": [], "as": "items", "bogus": 1
        }}])
        .unwrap_err()
        .to_string();
        assert!(err.contains("bogus"), "{err}");
    }

    #[test]
    fn match_filters_and_project_specifications_stay_open() {
        // The line ADR-129 draws: `$match` and `$project` are field-path
        // maps, not fixed-key documents, so any field name is legal data —
        // closing them would refuse every ordinary pipeline. Neither of
        // these unusual-looking field names is a typo of anything the stage
        // defines; both must be accepted.
        assert!(
            parse(&[doc! {"$match": {"bogusFieldName": 1, "another.nested.one": {"$gt": 2}}}])
                .is_ok()
        );
        assert!(
            parse(&[doc! {"$project": {"whateverTheCallerNamedIt": 1, "computed": "$other"}}])
                .is_ok()
        );
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
    fn a_leading_run_of_matches_puts_its_expressions_last() {
        // The run is one `$and`, and orders its clauses as one filter does.
        let stages = parse(&[
            doc! {"$match": {"$expr": {"$gt": ["$qty", 1]}}},
            doc! {"$match": {"kind": "c"}},
        ])
        .unwrap();
        let (filter, consumed) = leading_match(&stages).unwrap();
        assert_eq!(consumed, 2);
        let before = filter::expr_evaluations();
        assert!(!filter::matches(&filter, &doc! { "kind": "a", "qty": 5 }).unwrap());
        assert_eq!(filter::expr_evaluations() - before, 0);
    }

    #[test]
    fn a_match_whose_expr_cannot_be_evaluated_fails_the_pipeline() {
        // As in a top-level filter (ADR-206): the document is not dropped.
        let input = docs(vec![
            doc! { "_id": 1, "qty": 5 },
            doc! { "_id": 2, "qty": 12 },
            doc! { "_id": 3, "qty": "twelve" },
        ]);
        let gt = doc! {"$match": {"$expr": {"$gt": [{"$add": ["$qty", 1]}, 10]}}};
        let err = run(vec![gt.clone()], input.clone()).unwrap_err().to_string();
        assert_eq!(
            err,
            "invalid query: $expr cannot be evaluated for the document with _id 3: $add needs \
             numbers, found a string"
        );
        // After a stage that derives the documents, the same rule.
        let err = run(vec![doc! {"$project": {"_id": 0, "qty": 1}}, gt], input.clone())
            .unwrap_err()
            .to_string();
        assert!(err.contains("$expr cannot be evaluated: $add"), "{err}");
        // A clean false still drops the document.
        let ok = doc! {"$match": {"$expr": {"$eq": ["$qty", 12]}}};
        let out = run(vec![ok], input).unwrap();
        assert_eq!(out, vec![doc! { "_id": 2, "qty": 12 }]);
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
    fn lookup_keys_are_every_value_a_path_yields_and_each_element_of_an_array() {
        // `$unwind` refuses a path that crosses an array (ADR-130, below): it
        // has to write each value back to one place. A join only reads, so
        // `lookup_keys` reads the path as a filter does (ADR-210): every value it
        // resolves to, and the elements of any that is an array.
        let arr = |items: &[i32]| Bson::Array(items.iter().map(|n| Bson::Int32(*n)).collect());
        let input = docs(vec![doc! { "_id": 1, "a": [{"b": [1, 2]}, {"b": [3]}] }]);
        assert_eq!(
            lookup_keys(&input, "a.b"),
            vec![arr(&[1, 2]), Bson::Int32(1), Bson::Int32(2), arr(&[3]), Bson::Int32(3),],
            "each element's `b`, and the elements of a `b` that is an array"
        );
        // The shop example: every line's sku.
        let input = docs(vec![doc! { "items": [{"sku": "ef-9"}, {"sku": "gh-3"}] }]);
        assert_eq!(
            lookup_keys(&input, "items.sku"),
            vec![Bson::String("ef-9".into()), Bson::String("gh-3".into())]
        );
        // Arrays at two levels: `a.b.c` yields every `c`.
        let input = docs(vec![doc! { "a": [{"b": [{"c": 1}, {"c": 2}]}, {"b": [{"c": 3}]}] }]);
        assert_eq!(
            lookup_keys(&input, "a.b.c"),
            vec![Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]
        );
        // A field that holds an array is the whole array and each element.
        let input = docs(vec![doc! { "tags": ["a", "b"] }]);
        assert_eq!(
            lookup_keys(&input, "tags"),
            vec![
                Bson::Array(vec![Bson::String("a".into()), Bson::String("b".into())]),
                Bson::String("a".into()),
                Bson::String("b".into()),
            ]
        );
        // One level: an array in an array is an element, and is not opened.
        let input = docs(vec![doc! { "m": [[1, 2], 3] }]);
        assert_eq!(
            lookup_keys(&input, "m"),
            vec![Bson::Array(vec![arr(&[1, 2]), Bson::Int32(3)]), arr(&[1, 2]), Bson::Int32(3)]
        );
        // Equal values from different elements and documents are one key.
        let input = docs(vec![
            doc! { "items": [{"sku": "a"}, {"sku": "a"}, {"sku": "b"}] },
            doc! { "items": [{"sku": "b"}, {"sku": "c"}] },
        ]);
        assert_eq!(lookup_keys(&input, "items.sku").len(), 3);
        // Nothing at the path is nothing wanted, and an explicit null is a key.
        assert!(lookup_keys(&docs(vec![doc! { "x": 1 }]), "items.sku").is_empty());
        assert_eq!(lookup_keys(&docs(vec![doc! { "k": Bson::Null }]), "k"), vec![Bson::Null]);
    }

    #[test]
    fn unwind_over_a_numeric_segment_into_a_crossed_array_reads_by_index_not_by_fanning() {
        // The residue ADR-130 leaves: a numeric segment after a crossed
        // array writes by index (`path::set`'s only way into an array), so
        // it is not "crossing" in `crossing_array`'s sense and `$unwind`
        // does not refuse. `value_at` (`path::resolve`) reads `"0"` both as
        // the index 0 and as a field literally named `"0"` — the filter
        // language's rule, which ADR-116 keeps for every stage option that
        // names a field rather than computes one. Neither element here has
        // a field called `"0"`, so only the index reading contributes:
        // `a.0.b` is `[1, 2]`, found once, and unwinds to two rows.
        //
        // The fanning expression reader ADR-116 gives `$addFields`,
        // `$group` and the rest reads a numeric segment *only* as a field
        // name — never an index — so `$a.0.b` there finds no element named
        // `"0"` and is `[]`: zero rows, not two. This is the one shape
        // where `$unwind`'s own output would differ depending on which
        // reader answered it, and it is why `unwind_and_lookup_keys_…`
        // above no longer needs — and cannot have — an equivalent case of
        // its own for every crossing; this one is elsewhere.
        let input = docs(vec![doc! { "a": [{"b": [1, 2]}, {"b": [3]}] }]);
        let out = run(vec![doc! {"$unwind": "$a.0.b"}], input).unwrap();
        assert_eq!(
            out,
            docs(vec![doc! { "a": [{"b": 1}, {"b": [3]}] }, doc! { "a": [{"b": 2}, {"b": [3]}] },])
        );
    }

    #[test]
    fn unwind_refuses_a_path_that_crosses_an_array() {
        // The finding's shape (ADR-130): `a` is an array, so `$unwind:
        // "$a.b"` reads the first element's `b` — [1, 2], an array — and then
        // has to write 1 and 2 back to `a.b`, which is not a place: `a` holds
        // two elements, and there is no single one to put either value in.
        // Before the fix this silently emitted two byte-identical copies of
        // the input (nothing unwound) rather than refusing; the write
        // failure was discarded. Now it is a 400 naming the stage.
        let input = docs(vec![doc! { "_id": 1, "a": [{"b": [1, 2]}, {"b": 3}] }]);
        let err = run(vec![doc! {"$unwind": "$a.b"}], input).unwrap_err().to_string();
        assert!(err.contains("$unwind"), "{err}");
        assert!(err.contains("a.b"), "{err}");

        // The refusal is uniform: it depends only on the path crossing an
        // array in this document, never on what turns out to be at the far
        // end of it. Before this test's second fix, refusal depended on the
        // *type* of the first crossed element's value — a scalar there (`b:
        // 9`) skipped the write entirely and answered `200` with one row
        // that looked unwound but was not, exactly the wrong-answer class
        // this ADR exists to close. `a` here is one element short of the
        // first case and its `b` is a scalar at that element; both are
        // refused all the same.
        let input = docs(vec![doc! { "_id": 1, "a": [{"b": 9}] }]);
        assert!(run(vec![doc! {"$unwind": "$a.b"}], input).is_err());
        let input = docs(vec![doc! { "_id": 1, "a": [{"b": 9}, {"b": [1, 2]}] }]);
        assert!(
            run(vec![doc! {"$unwind": "$a.b"}], input).is_err(),
            "a scalar found first must not skip the refusal"
        );

        // A control proving it is the crossed array, not the dotted path,
        // that is refused: no array is crossed here, so both elements write
        // back cleanly and nothing changes from before.
        let input = docs(vec![doc! { "y": {"b": [1, 2]} }]);
        let out = run(vec![doc! {"$unwind": "$y.b"}], input).unwrap();
        assert_eq!(out, docs(vec![doc! {"y": {"b": 1}}, doc! {"y": {"b": 2}}]));

        // The real corpus shape: `items` is an array of `{sku, qty}`. Under
        // the narrow, read-dependent trigger this used to pass through
        // unchanged — `sku` is a scalar at the first element, so nothing was
        // ever attempted to be written — which was finding 10's own row 5,
        // still unfixed by that version of this ADR. The uniform rule
        // refuses it: `items` is an array and `sku` is not a numeric
        // segment, so there is no single place to write to regardless of
        // what `sku` holds.
        let input = docs(vec![
            doc! { "_id": 1, "items": [{"sku": "a", "qty": 1}, {"sku": "b", "qty": 2}] },
        ]);
        let err = run(vec![doc! {"$unwind": "$items.sku"}], input).unwrap_err().to_string();
        assert!(err.contains("$unwind"), "{err}");
        assert!(err.contains("items.sku"), "{err}");
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

    fn dec(text: &str) -> Bson {
        Bson::Decimal128(text.parse().unwrap())
    }

    /// Seventeen values that exercise every way two values can be one member or
    /// two: NaN against itself, one number in three widths, signed zeros, the
    /// two nulls, documents differing in key order and in a nested width,
    /// `Decimal128`s by their bytes, and a string against a symbol.
    fn set_corpus() -> Vec<Bson> {
        vec![
            Bson::Double(f64::NAN),
            Bson::Double(f64::NAN),
            Bson::Int32(1),
            Bson::Double(1.0),
            Bson::Int64(1),
            Bson::Double(-0.0),
            Bson::Double(0.0),
            Bson::Null,
            Bson::Undefined,
            dec("1"),
            dec("1.0"),
            dec("1"),
            Bson::Document(doc! { "a": 1, "b": 2 }),
            Bson::Document(doc! { "b": 2, "a": 1 }),
            Bson::Document(doc! { "a": 1.0, "b": 2 }),
            Bson::String("s".into()),
            Bson::Symbol("s".into()),
        ]
    }

    fn as_docs(values: &[Bson]) -> Vec<Document> {
        values.iter().map(|v| doc! { "v": v.clone() }).collect()
    }

    fn members(values: &[Bson]) -> Vec<Bson> {
        let out =
            run(vec![doc! {"$group": {"_id": null, "s": {"$addToSet": "$v"}}}], as_docs(values))
                .unwrap();
        out[0].get_array("s").unwrap().clone()
    }

    fn keys_of(values: &[Bson]) -> std::collections::BTreeSet<Vec<u8>> {
        values.iter().map(group_key).collect()
    }

    #[test]
    fn add_to_set_members_are_the_buckets_group_makes() {
        let corpus = set_corpus();
        let buckets: Vec<Bson> = run(vec![doc! {"$group": {"_id": "$v"}}], as_docs(&corpus))
            .unwrap()
            .into_iter()
            .map(|d| d.get("_id").unwrap().clone())
            .collect();
        let got = members(&corpus);
        // Premises: a set that is empty or collapses everything would satisfy
        // the equality below.
        assert_eq!(buckets.len(), 9, "buckets: {buckets:?}");
        assert_eq!(got.len(), 9, "members: {got:?}");
        assert_eq!(keys_of(&got), keys_of(&buckets));
        // One member per key, and each is the first of its kind in input order.
        assert_eq!(keys_of(&got).len(), got.len());
        for member in &got {
            let first = corpus.iter().find(|v| group_key(v) == group_key(member)).unwrap();
            assert!(
                format!("{first:?}") == format!("{member:?}"),
                "kept {member:?}, first seen {first:?}"
            );
        }
    }

    #[test]
    fn a_value_holding_a_decimal128_is_identified_by_its_rendering() {
        // `keyenc` refuses a `Decimal128` anywhere in a value, so the whole
        // value is identified by its rendering and `1` against `1.0` no longer
        // collapses inside it. `$group` does the same, and the two agree.
        let pairs = [
            (
                Bson::Document(doc! { "a": 1, "d": dec("1") }),
                Bson::Document(doc! { "a": 1.0, "d": dec("1") }),
            ),
            (
                Bson::Array(vec![Bson::Double(0.0), dec("1")]),
                Bson::Array(vec![Bson::Double(-0.0), dec("1")]),
            ),
        ];
        for (x, y) in pairs {
            let both = [x, y];
            let buckets = run(vec![doc! {"$group": {"_id": "$v"}}], as_docs(&both)).unwrap();
            assert_eq!(buckets.len(), 2, "{both:?}");
            assert_eq!(members(&both).len(), 2, "{both:?}");
        }
    }

    #[test]
    fn add_to_set_members_do_not_depend_on_input_order() {
        // `Decimal128` ranks equal to every number in the canonical order, so a
        // set built on that order kept `[1, 2]` from one order and `[Dec(5)]`
        // from the other.
        for order in
            [[Bson::Int32(1), dec("5"), Bson::Int32(2)], [dec("5"), Bson::Int32(1), Bson::Int32(2)]]
        {
            assert_eq!(members(&order).len(), 3, "{order:?}");
        }
        let corpus = set_corpus();
        let expected = keys_of(&members(&corpus));
        assert_eq!(expected.len(), 9);
        let mut reversed = corpus.clone();
        reversed.reverse();
        let mut orders = vec![reversed];
        for shift in 1..corpus.len() {
            let mut rotated = corpus.clone();
            rotated.rotate_left(shift);
            orders.push(rotated);
        }
        for order in orders {
            assert_eq!(keys_of(&members(&order)), expected, "{order:?}");
        }
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
    fn the_pipeline_ceiling_is_a_request_limit_that_is_never_deferred() {
        // Worded and answered as before, but its own kind, so an `$and`,
        // `$or` or `$last` cannot hold it back (ADR-211).
        let input: Vec<Document> = (0..3).map(|i| doc! { "k": i }).collect();
        let limits = Limits { max_documents: 2 };
        let stages = parse(&[doc! {"$group": {"_id": "$k"}}]).unwrap();
        let err = apply(&stages[0], input, &limits).unwrap_err();
        assert!(matches!(err, Error::Limit(_)), "{err:?}");
        assert!(!err.is_deferrable());
        assert_eq!(
            err.to_string(),
            "invalid query: $group produced 3 documents, over the pipeline limit of 2. Narrow \
             the pipeline with an earlier $match"
        );
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
    fn project_through_an_array_keeps_the_array_beside_computed_fields() {
        let input = vec![doc! {"_id": 7, "qty": 2, "p": [{"_id": 1, "n": "a"}, 5, {"n": "b"}]}];
        // The flags alone, inclusion and exclusion, and with a computed
        // field beside them: one projection, so one answer.
        let out = run(vec![doc! {"$project": {"p._id": 1}}], input.clone()).unwrap();
        assert_eq!(out, vec![doc! {"_id": 7, "p": [{"_id": 1}, {}]}]);
        let out = run(vec![doc! {"$project": {"p._id": 1, "q": "$qty"}}], input.clone()).unwrap();
        assert_eq!(out, vec![doc! {"_id": 7, "p": [{"_id": 1}, {}], "q": 2}]);
        let out = run(vec![doc! {"$project": {"p._id": 0}}], input.clone()).unwrap();
        assert_eq!(out, vec![doc! {"_id": 7, "qty": 2, "p": [{"n": "a"}, 5, {"n": "b"}]}]);
        // A computed field written into the kept array has no one place to
        // go, and is refused as `$addFields` refuses it, not written into a
        // document standing in for the array.
        let err =
            run(vec![doc! {"$project": {"p._id": 1, "p.q": "$qty"}}], input.clone()).unwrap_err();
        assert!(err.to_string().contains("cannot set \"p.q\""), "{err}");
        // A numeric segment is the documented exception: the write follows
        // the write rules and replaces element 0 by position, as `$addFields`
        // does, while the flags beside it read `p.0` as a field name.
        let out = run(vec![doc! {"$project": {"p._id": 1, "p.0": "$qty"}}], input.clone()).unwrap();
        assert_eq!(out, vec![doc! {"_id": 7, "p": [2, {}]}]);
        let out = run(vec![doc! {"$addFields": {"p.0": "$qty"}}], input).unwrap();
        assert_eq!(out, vec![doc! {"_id": 7, "qty": 2, "p": [2, 5, {"n": "b"}]}]);
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

    // -- $and and $or in a stage (ADR-211) ----------------------------------

    /// `qty` cannot be added to; `kind` decides.
    fn bad_row() -> Vec<Document> {
        docs(vec![doc! { "_id": 3, "qty": "twelve", "kind": "a" }])
    }

    #[test]
    fn a_stage_answers_where_an_argument_decides_an_and_or_an_or() {
        let bad = doc! { "$add": ["$qty", 1] };
        let out = run(
            vec![doc! {"$project": {
                "x": {"$and": [false, bad.clone()]},
                "y": {"$and": [bad.clone(), {"$eq": ["$kind", "b"]}]},
                "z": {"$or": [bad.clone(), {"$eq": ["$kind", "a"]}]},
            }}],
            bad_row(),
        )
        .unwrap();
        assert_eq!(out, vec![doc! { "_id": 3, "x": false, "y": false, "z": true }]);

        let out =
            run(vec![doc! {"$addFields": {"x": {"$or": [true, bad.clone()]}}}], bad_row()).unwrap();
        assert_eq!(out[0].get("x"), Some(&Bson::Boolean(true)));

        // An accumulator whose `$cond` reads a deciding `$and`.
        let out = run(
            vec![doc! {"$group": {"_id": null, "n": {"$sum": {"$cond": [
                {"$and": [{"$eq": ["$kind", "b"]}, {"$gt": [bad.clone(), 0]}]}, 1, 0
            ]}}}}],
            bad_row(),
        )
        .unwrap();
        assert_eq!(out, vec![doc! { "_id": Bson::Null, "n": 0_i64 }]);

        // `$filter`'s `cond`, per element.
        let out = run(
            vec![doc! {"$project": {"kept": {"$filter": {
                "input": [1, 2, 6],
                "as": "i",
                "cond": {"$and": [{"$gt": ["$$i", 5]}, {"$add": ["$qty", "$$i"]}]},
            }}}}],
            bad_row(),
        )
        .unwrap_err();
        // 6 passes the guard, so its `$add` is reached and fails the stage.
        assert!(out.to_string().contains("$add needs numbers"), "{out}");
        let out = run(
            vec![doc! {"$project": {"kept": {"$filter": {
                "input": [1, 2],
                "as": "i",
                "cond": {"$and": [{"$gt": ["$$i", 5]}, {"$add": ["$qty", "$$i"]}]},
            }}}}],
            bad_row(),
        )
        .unwrap();
        assert_eq!(out[0].get("kept"), Some(&Bson::Array(vec![])));
    }

    #[test]
    fn a_stage_still_fails_where_no_argument_decides() {
        let bad = doc! { "$add": ["$qty", 1] };
        for stage in [
            doc! {"$project": {"x": {"$and": [bad.clone(), {"$eq": ["$kind", "a"]}]}}},
            doc! {"$addFields": {"x": {"$or": [bad.clone(), false]}}},
            doc! {"$group": {"_id": null, "n": {"$sum": {"$cond": [
                {"$and": [{"$eq": ["$kind", "a"]}, {"$gt": [bad.clone(), 0]}]}, 1, 0
            ]}}}},
            doc! {"$match": {"$expr": {"$and": [bad.clone(), true]}}},
        ] {
            let e = run(vec![stage.clone()], bad_row()).unwrap_err();
            assert!(e.to_string().contains("$add needs numbers"), "{stage}: {e}");
        }
        // A `$match` whose `$and` is decided keeps or drops the document.
        let kept =
            run(vec![doc! {"$match": {"$expr": {"$or": [bad.clone(), true]}}}], bad_row()).unwrap();
        assert_eq!(kept.len(), 1);
        let dropped =
            run(vec![doc! {"$match": {"$expr": {"$and": [bad, false]}}}], bad_row()).unwrap();
        assert!(dropped.is_empty());
    }

    #[test]
    fn last_fails_only_when_the_last_value_cannot_be_evaluated() {
        // Only the last document's value is the answer, so an earlier one
        // that cannot be evaluated is not part of it (ADR-211).
        let stage = || doc! {"$group": {"_id": "$k", "v": {"$last": {"$add": ["$qty", 1]}}}};
        let bad = |k: &str| doc! { "k": k, "qty": "twelve" };
        let good = |k: &str, q: i32| doc! { "k": k, "qty": q };
        let out = run(vec![stage()], vec![bad("a"), good("a", 5), good("b", 1)]).unwrap();
        assert_eq!(out, vec![doc! { "_id": "a", "v": 6_i64 }, doc! { "_id": "b", "v": 2_i64 }]);
        // Another group's bad value does not touch this one's.
        let out = run(vec![stage()], vec![good("a", 5), bad("b"), good("b", 1)]).unwrap();
        assert_eq!(out[1], doc! { "_id": "b", "v": 2_i64 });
        // The last value fails, so the request does, whatever came before.
        for input in
            [vec![good("a", 5), bad("a")], vec![bad("a"), good("a", 5), bad("a")], vec![bad("a")]]
        {
            let e = run(vec![stage()], input).unwrap_err();
            assert!(e.to_string().contains("$add needs numbers"), "{e}");
        }
        // `$first` reads only the first document, as before.
        let first = doc! {"$group": {"_id": null, "v": {"$first": {"$add": ["$qty", 1]}}}};
        let out = run(vec![first], vec![good("a", 5), bad("a")]).unwrap();
        assert_eq!(out[0].get("v"), Some(&Bson::Int64(6)));
    }

    #[test]
    fn last_does_not_hold_back_an_error_that_cannot_wait() {
        // An unbound variable is a bug, not a value a later document
        // replaces: the group fails on the document that raised it, even
        // though the last document's value is good.
        let stages = parse_with_vars(
            &[doc! {"$group": {"_id": null, "v": {"$last": {"$cond": [
                {"$eq": ["$k", "a"]}, "$$x", 1
            ]}}}}],
            &["x".to_string()],
        )
        .unwrap();
        let input = docs(vec![doc! { "k": "a" }, doc! { "k": "b" }]);
        let err = apply(&stages[0], input, &Limits::default()).unwrap_err();
        assert!(matches!(err, Error::Internal(_)), "{err:?}");
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

    /// The other way to write the same mistake: `{_id: "$$oid"}`, which
    /// parsed as the literal string, matched nothing, and returned an empty
    /// join with no error. A `$$` string value anywhere in a sub-pipeline
    /// `$match` is refused now, naming the variable and the idiom.
    #[test]
    fn a_variable_string_in_a_sub_pipeline_match_is_refused() {
        let refused = |stage: Document| {
            let err = parse(&[doc! {"$lookup": {
                "from": "a", "let": {"oid": "$_id"}, "pipeline": [stage], "as": "x"
            }}])
            .unwrap_err()
            .to_string();
            assert!(err.contains("$$oid"), "{err}");
            assert!(err.contains("$addFields"), "{err}");
            err
        };
        // The finding's exact pipeline.
        let err = refused(doc! {"$match": {"_id": "$$oid"}});
        assert!(err.contains("no variables"), "{err}");
        // Inside an operator document, an array operator, a negation.
        refused(doc! {"$match": {"_id": {"$eq": "$$oid"}}});
        refused(doc! {"$match": {"_id": {"$in": ["$$oid"]}}});
        refused(doc! {"$match": {"_id": {"$nin": ["$$oid"]}}});
        refused(doc! {"$match": {"tags": {"$all": ["$$oid"]}}});
        refused(doc! {"$match": {"_id": {"$not": {"$gt": "$$oid"}}}});
        // Nested through `$elemMatch` and the logical operators.
        refused(doc! {"$match": {"lines": {"$elemMatch": {"order": "$$oid"}}}});
        refused(doc! {"$match": {"$or": [{"a": 1}, {"$and": [{"order": "$$oid"}]}]}});
        // A regex pattern is a value too and is not special-cased.
        refused(doc! {"$match": {"name": {"$regex": "$$oid"}}});

        // `$expr` is the expression parser's subtree and keeps its own,
        // more specific, refusal.
        let err = parse(&[doc! {"$lookup": {
            "from": "a", "let": {"oid": "$_id"},
            "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$oid"]}}}], "as": "x"
        }}])
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown variable"), "{err}");

        // A sub-pipeline `$match` with no `$$` value is unchanged, `$$ROOT`
        // under `$expr` included.
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "a", "let": {"oid": "$_id"},
                "pipeline": [{"$match": {"kind": "line", "qty": {"$gt": 1}, "tag": "$plain",
                                         "$expr": {"$gt": ["$$ROOT.qty", 0]}}}],
                "as": "x"
            }}])
            .is_ok()
        );
        // No `let` does not make it a top-level `$match`.
        let err = parse(&[doc! {"$lookup": {
            "from": "a", "pipeline": [{"$match": {"_id": "$$oid"}}], "as": "x"
        }}])
        .unwrap_err()
        .to_string();
        assert!(err.contains("$$oid"), "{err}");
        // Nor does a `$lookup` one level down.
        let err = parse(&[doc! {"$lookup": {
            "from": "a", "let": {"oid": "$_id"},
            "pipeline": [{"$lookup": {
                "from": "b", "pipeline": [{"$match": {"_id": "$$oid"}}], "as": "y"
            }}],
            "as": "x"
        }}])
        .unwrap_err()
        .to_string();
        assert!(err.contains("$$oid"), "{err}");
    }

    /// The refusal is scoped to sub-pipelines: at the top level there is no
    /// `let` a `$$` string could mean, and a stored document may hold one.
    #[test]
    fn a_top_level_match_still_reads_a_variable_string_literally() {
        let stages = parse(&[doc! {"$match": {"category": "$$oid"}}]).unwrap();
        let input =
            docs(vec![doc! {"_id": 1, "category": "$$oid"}, doc! {"_id": 2, "category": "oid"}]);
        let out = apply(&stages[0], input, &Limits::default()).unwrap();
        assert_eq!(out, docs(vec![doc! {"_id": 1, "category": "$$oid"}]));
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

    /// An array written in a stage is an array of expressions in every place
    /// an expression sits (ADR-215): `$project`, `$addFields`, `$replaceRoot`,
    /// a `$group` key and an accumulator, `$match`'s `$expr` and a `$lookup`
    /// `let`.
    #[test]
    fn an_array_in_any_stage_is_read_as_expressions() {
        let input = || {
            docs(vec![
                doc! {"_id": 1, "a": 1, "b": 2, "tags": ["x"], "extra": "y"},
                doc! {"_id": 2, "a": 1, "b": 3, "tags": [], "extra": "x"},
            ])
        };
        let out = run(
            vec![doc! {"$project": {
                "pair": ["$a", "$b"],
                "u": {"$setUnion": ["$tags", ["$extra"]]},
                "kept": {"$literal": ["$a"]},
            }}],
            input(),
        )
        .unwrap();
        assert_eq!(out[0], doc! {"_id": 1, "pair": [1, 2], "u": ["x", "y"], "kept": ["$a"]});

        let out = run(vec![doc! {"$addFields": {"pair": [["$b"], "$missing"]}}], input()).unwrap();
        assert_eq!(
            out[1].get("pair"),
            Some(&Bson::from(vec![Bson::from(vec![Bson::Int32(3)]), Bson::Null]))
        );

        let out = run(vec![doc! {"$replaceRoot": {"newRoot": {"v": ["$_id"]}}}], input()).unwrap();
        assert_eq!(out, docs(vec![doc! {"v": [1]}, doc! {"v": [2]}]));

        // A group key that is an array groups by the values, not by one
        // constant: two buckets here, where the strings made one.
        let mut out = run(
            vec![doc! {"$group": {"_id": ["$a", "$b"], "pairs": {"$push": ["$_id", "$extra"]}}}],
            input(),
        )
        .unwrap();
        out.sort_by_key(|d| d.get_array("_id").unwrap()[1].as_i32());
        assert_eq!(
            out,
            docs(vec![
                doc! {"_id": [1, 2], "pairs": [[1, "y"]]},
                doc! {"_id": [1, 3], "pairs": [[2, "x"]]},
            ])
        );

        let out =
            run(vec![doc! {"$match": {"$expr": {"$in": ["$extra", ["$tags", "x"]]}}}], input())
                .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get_i32("_id").unwrap(), 2);
    }

    #[test]
    fn a_lookup_let_array_is_read_and_its_names_reach_the_sub_pipeline() {
        let stages = parse(&[doc! {"$lookup": {
            "from": "items",
            "let": {"wanted": ["$first", "$second"]},
            "pipeline": [
                {"$addFields": {"hit": {"$in": ["$sku", "$$wanted"]}}},
                {"$match": {"hit": true}},
                {"$project": {"_id": 0, "sku": 1, "pair": ["$$wanted", "$sku"]}},
            ],
            "as": "lines",
        }}])
        .unwrap();
        let orders = vec![doc! {"_id": 1, "first": "a", "second": "c"}];
        let items = vec![doc! {"sku": "a"}, doc! {"sku": "b"}, doc! {"sku": "c"}];
        let out = join_in_memory(&stages[0], orders, &items);
        assert_eq!(
            out[0].get_array("lines").unwrap(),
            &vec![
                Bson::Document(doc! {"sku": "a", "pair": [["a", "c"], "a"]}),
                Bson::Document(doc! {"sku": "c", "pair": [["a", "c"], "c"]}),
            ]
        );
    }
}

#[cfg(test)]
mod decimal128 {
    use super::*;
    use bson::doc;

    #[test]
    fn a_sort_stage_refuses_a_document_it_cannot_place() {
        // The same refusal `find`'s sort makes, from the stage that sorts a
        // pipeline's documents in hand.
        let d = Bson::Decimal128("1.5".parse().unwrap());
        let input =
            vec![doc! { "_id": 1, "v": 1 }, doc! { "_id": 2, "v": d }, doc! { "_id": 3, "v": 2 }];
        let stages = parse(&[doc! { "$sort": { "v": 1 } }]).unwrap();
        let msg = apply(&stages[0], input.clone(), &Limits::default()).unwrap_err().to_string();
        assert!(msg.contains("cannot sort by \"v\"") && msg.contains("document 2"), "{msg}");
        // Sorting by a path the Decimal128 is not on is unaffected.
        let stages = parse(&[doc! { "$sort": { "_id": -1 } }]).unwrap();
        let out = apply(&stages[0], input, &Limits::default()).unwrap();
        assert_eq!(out[0].get_i32("_id").unwrap(), 3);
    }
}
