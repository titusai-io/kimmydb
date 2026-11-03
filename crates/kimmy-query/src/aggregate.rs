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
//! # The request deadline (ADR-266)
//!
//! A pipeline runs on the worker thread without yielding, so the request timeout
//! cannot end it. It ends itself: [`apply_with_vars`] looks at the thread's deadline
//! ([`crate::deadline`]) as each stage starts, and every loop in a stage ticks a
//! `deadline::Ticker`, which reads the clock on the first iteration and then at a
//! stride that follows the cost of an iteration (about every 1 to 10 ms of work), and fails with `Error::DeadlineExceeded` once the deadline has passed.
//! Outside a handler that set a deadline every call is a no-op.
//!
//! **A stage added to this module takes the same two lines**: `let mut ticker =
//! Ticker::new();` before its loop and `ticker.tick()?;` as the loop's first statement
//! (a stage that only calls other stages needs neither, `apply_with_vars` having
//! checked on its entry). A loop that runs a few iterations but is entered many times
//! (an expression operator) calls `deadline::tick()?`, which counts across calls. A
//! stage the caller executes (`$lookup`, `$graphLookup`, `$merge`, `$out`) does the same
//! in `kimmy-api`'s `exec.rs`; its reads of storage already tick per row.
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

use crate::budget::{Lease, input_of, output_of};
use crate::deadline::{self, Ticker};
use crate::expr::{Binding, Expr, Mean, Scope, Total, type_name};
use crate::filter::{self, Filter};
use crate::shape::{self, ID_FIELD, Projection, SortKey};
use crate::stages;

/// How many documents a single stage may hold or emit.
///
/// Deliberately a hard ceiling rather than a spill-to-disk: a pipeline that
/// cannot run should say so immediately, not become slow in a way that is
/// harder to diagnose than a refusal.
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_documents: usize,
    /// The most bytes a document a stage builds as its output may take: 16 MiB. A `$group`,
    /// `$bucket` or `$bucketAuto` output document is checked against it as a whole (ADR-269),
    /// since each accumulator's value is only held to 16 MiB on its own (ADR-267).
    pub max_document_bytes: usize,
    /// The bytes the request holds, in BSON: the source read, each stage's input and
    /// output, the groups and joins it builds, what the window stages buffer and the
    /// answer, one total for the whole pipeline and its `$facet` branches and
    /// sub-pipelines (ADR-269, which widens ADR-264's). Drawing on the node's pool when it
    /// was made from one. A fresh `Limits::default()` is a fresh budget of the library's
    /// fallback (1 GiB, see `budget::DEFAULT_MEMORY_LIMIT_BYTES`), so make one per request.
    pub buffer: crate::budget::BufferBudget,
}

/// The default ceiling: ten times `find`'s page cap.
///
/// Large enough that ordinary reporting over a sizeable collection works,
/// small enough that a hostile pipeline cannot exhaust a node.
pub const DEFAULT_MAX_DOCUMENTS: usize = 100_000;

/// The most stages one pipeline may have, a `$facet` branch and a `$lookup` or `$unionWith`
/// sub-pipeline each counted on its own, and a stage that the reference expands into two
/// counted as two ([`stage_weight`]). It is the reference's limit, measured (see
/// `docs/deviations.md`). It is a bound on the body a client can send, not what keeps the stack
/// safe: [`crate::pipeline`] runs a run of stages that stream in one loop, whatever its length.
pub const MAX_PIPELINE_STAGES: usize = 1_000;

/// What a stage counts for against [`MAX_PIPELINE_STAGES`]: the stages it is made of in the
/// reference, which counts what a stage expands to. `$count`, `$sortByCount`, `$bucket`,
/// `$setWindowFields` and `$densify` are two; every other stage probed is one, and one that
/// was not (a `$fill` with a method, a `$densify` variant, `$geoNear`) is counted as one here.
pub fn stage_weight(name: &str) -> usize {
    match name {
        "$count" | "$sortByCount" | "$bucket" | "$setWindowFields" | "$densify" => 2,
        _ => 1,
    }
}

/// What the stages of a pipeline count for, as they are written: the name of a stage is its
/// one key, and a stage that has not one is counted as one (it is refused when it is parsed).
pub fn pipeline_weight<'a>(stages: impl IntoIterator<Item = &'a Document>) -> usize {
    stages
        .into_iter()
        .map(|stage| match (stage.len(), stage.keys().next()) {
            (1, Some(name)) => stage_weight(name),
            _ => 1,
        })
        .sum()
}

/// The refusal of a pipeline that weighs `weight`. An update pipeline has its own words
/// ([`crate::update_pipeline`]): it has no `$facet` or sub-pipeline to name.
pub(crate) fn too_many_stages(weight: usize) -> Error {
    Error::InvalidQuery(format!(
        "a pipeline has {weight} stages, over the limit of {MAX_PIPELINE_STAGES} (a stage that \
         expands, such as $count, counts as two). A $facet branch and a $lookup or $unionWith \
         sub-pipeline are each counted on their own"
    ))
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_documents: DEFAULT_MAX_DOCUMENTS,
            max_document_bytes: kimmy_core::limits::MAX_DOCUMENT_BYTES,
            buffer: crate::budget::BufferBudget::default(),
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
    MergeObjects(Expr),
    /// `$stdDevPop`, `$stdDevSamp`, `$top`, `$bottom`, `$topN`, `$bottomN`, `$firstN`,
    /// `$lastN`, `$maxN`, `$minN` and `$count` (ADR-262).
    Ext(crate::accumulators::Spec),
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
    /// Vector search as the pipeline's source (ADR-216). Executed by the
    /// caller, which owns the embedding provider and the index: the stage
    /// replaces the collection scan, so it is only ever first.
    VectorSearch(VectorSearch),
    /// Join against another collection. Executed by the caller — see the module
    /// documentation.
    Lookup {
        from: String,
        as_field: String,
        join: Join,
    },
    /// `$facet`: several sub-pipelines over the same input, answered as one
    /// document holding each one's output under its name (ADR-254). Run by
    /// [`run_facet`], which the caller gives the way to run a sub-pipeline.
    Facet(Vec<(String, Vec<Stage>)>),
    /// `$bucket`: documents grouped into the ranges `[b0, b1)`, `[b1, b2)`, ... of
    /// `boundaries`, and the rest into `default` if there is one (ADR-254).
    Bucket {
        group_by: Expr,
        boundaries: Vec<Bson>,
        default: Option<Bson>,
        fields: Vec<(String, Accumulator)>,
    },
    /// `$bucketAuto`: documents sorted by `group_by` and cut into at most `buckets`
    /// runs of about equal size, a value never split across two (ADR-254).
    BucketAuto {
        group_by: Expr,
        buckets: usize,
        fields: Vec<(String, Accumulator)>,
    },
    /// `$graphLookup`: for each input document, the documents of another collection
    /// reached by following `connect_from` to `connect_to` from `start_with`, at most
    /// `max_depth` steps. Executed by the caller, which holds a storage handle (ADR-254).
    GraphLookup(Box<GraphLookup>),
    /// `$merge`: the pipeline's output written into a collection, each document
    /// matched to a stored one by `_id` (ADR-255). Only ever last. Executed by the
    /// caller, which holds a storage handle and the write path.
    Merge(Box<MergeSpec>),
    /// `$out`: the pipeline's output inserted into a collection that is new or empty
    /// (ADR-255). Only ever last. Executed by the caller.
    Out(Target),
    /// `$setWindowFields` (ADR-264): per-document windows over sorted partitions.
    SetWindowFields(Box<crate::window::WindowSpec>),
    /// `$densify` (ADR-264): generated documents that close the gaps in a numeric or date field.
    Densify(Box<crate::densify::DensifySpec>),
    /// `$fill` (ADR-264): null and missing values filled from a value or its neighbours.
    Fill(Box<crate::fill::FillSpec>),
    /// `$sortByCount`: `$group` on the expression, counting, then `$sort` by the count,
    /// largest first (ADR-263).
    SortByCount(Expr),
    /// `$sample`: this many documents, chosen at random (ADR-263).
    Sample(usize),
    /// `$redact`: each document and each embedded document kept, pruned or descended
    /// into by what the expression answers (ADR-263).
    Redact(Expr),
    /// `$unionWith`: the input, then another collection's documents (ADR-263). Executed
    /// by the caller, which holds a storage handle.
    UnionWith(Box<UnionWith>),
}

/// The fields of a `$unionWith` stage.
#[derive(Clone, Debug)]
pub struct UnionWith {
    /// The collection, in the database the pipeline runs in.
    pub coll: String,
    /// Run over that collection's documents before they are appended.
    pub stages: Vec<Stage>,
}

/// A collection a write stage names: `"name"`, or `{db, coll}` for one in another
/// database. `db` is `None` for the database the pipeline runs in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub db: Option<String>,
    pub coll: String,
}

/// What a `$merge` does with a document that matches a stored one by `_id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhenMatched {
    /// Replace the stored document whole, keeping its `_id`.
    Replace,
    /// Lay the incoming document's top-level fields over the stored one's.
    Merge,
    /// Leave the stored document alone.
    KeepExisting,
}

/// The fields of a `$merge` stage. The match key is always `_id`: see
/// [`parse_merge`] for why `on` is not a choice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeSpec {
    pub into: Target,
    pub when_matched: WhenMatched,
    /// `whenNotMatched: "insert"`; `false` is `"discard"`.
    pub insert_unmatched: bool,
}

/// The fields of a `$graphLookup` stage.
#[derive(Clone, Debug)]
pub struct GraphLookup {
    pub from: String,
    /// Where each walk starts: the value, or the elements of an array.
    pub start_with: Expr,
    /// The field of a reached document whose values are followed.
    pub connect_from: String,
    /// The field of a foreign document a followed value is matched against.
    pub connect_to: String,
    pub as_field: String,
    /// The most steps taken after the first match; `0` is the first match alone.
    pub max_depth: usize,
    /// A field to hold the step (`0` for the first match) each document was reached in.
    pub depth_field: Option<String>,
    /// Only foreign documents this admits are reachable.
    pub restrict: Option<Filter>,
}

/// The most steps a `$graphLookup` may be asked to take. A walk visits each
/// document once however deep it goes, so the number bounds the rounds of lookups,
/// not the memory; a chain longer than this is a request for a different tool.
pub const MAX_GRAPH_DEPTH: usize = 1000;

/// The fields of a `$vectorSearch` stage: the endpoint's own request fields
/// and no others (ADR-216).
#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearch {
    /// Query text, embedded server-side.
    pub query: Option<String>,
    /// A pre-computed query vector; required under the `byo` provider.
    pub vector: Option<Vec<f32>>,
    /// Hits wanted; the endpoint's default and clamp apply when absent.
    pub k: Option<usize>,
    /// Chunks per document allowed into the hits.
    pub per_document: Option<usize>,
    /// Restrict hits to documents matching this filter.
    pub filter: Option<Document>,
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
    ///
    /// `lead` is set when the leading `$match` reads a `let` variable, and says how
    /// to split it (see [`Lead`]); `None` for a sub-pipeline whose leading `$match`,
    /// if any, reads none (it is applied once, before the loop).
    Pipeline { vars: Vec<(String, Expr)>, stages: Vec<Stage>, lead: Option<Box<Lead>> },
}

/// A sub-pipeline's leading `$match` that reads `let` variables, taken apart at
/// parse time. The conjunction is flattened through `$and` only (never `$or`,
/// `$nor` or an `$expr`'s own `$and`) into its leaf clauses **in evaluation
/// order**, and each leaf is independent (reads no variable) or variable-reading.
///
/// Both halves are `And` of their leaves in that order, so evaluating `variable`
/// over a pair that `independent` accepts decides it exactly as the whole
/// conjunction does (ADR-211: a false clause decides, a clause that cannot be
/// evaluated is held back). The executor uses them as an attempt that may only
/// decline: see `docs/decisions.md`, ADR-222.
#[derive(Clone, Debug)]
pub struct Lead {
    /// The leaves that read no variable, applied once to the foreign set.
    pub independent: Filter,
    /// The leaves that read a variable, evaluated per input document.
    pub variable: Filter,
    /// A variable leaf `{$expr: {$eq: [A, B]}}` that the executor may hash on.
    pub key: Option<JoinKey>,
    /// `variable` without the key leaf, in order.
    pub residual: Filter,
}

/// The two sides of a recognised `$eq`: `foreign` reads the document and no
/// variable, `local` does not read the document.
#[derive(Clone, Debug)]
pub struct JoinKey {
    pub foreign: Expr,
    pub local: Expr,
    /// Whether `$eq` was written `[local, foreign]`, which fixes the order a
    /// fallback evaluates them in.
    pub local_first: bool,
}

impl Lead {
    fn analyze(filter: &Filter, names: &[String]) -> Option<Lead> {
        fn flatten<'f>(filter: &'f Filter, out: &mut Vec<&'f Filter>) {
            match filter {
                Filter::AlwaysTrue => {}
                Filter::And(branches) => branches.iter().for_each(|b| flatten(b, out)),
                leaf => out.push(leaf),
            }
        }
        fn key_of(leaf: &Filter, names: &[String]) -> Option<JoinKey> {
            let Filter::Expr(e) = leaf else { return None };
            let Expr::Op(crate::expr::Op::Eq, args) = e.as_ref() else { return None };
            let [a, b] = args.as_slice() else { return None };
            let foreign_side = |x: &Expr, y: &Expr| {
                x.reads_document() && !x.reads_variable(names) && !y.reads_document()
            };
            if foreign_side(a, b) {
                Some(JoinKey { foreign: a.clone(), local: b.clone(), local_first: false })
            } else if foreign_side(b, a) {
                Some(JoinKey { foreign: b.clone(), local: a.clone(), local_first: true })
            } else {
                None
            }
        }
        let mut leaves = Vec::new();
        flatten(filter, &mut leaves);
        let (variable, independent): (Vec<&Filter>, Vec<&Filter>) =
            leaves.into_iter().partition(|leaf| filter::reads_variable(leaf, names));
        if variable.is_empty() {
            return None;
        }
        let all = |leaves: &[&Filter]| match leaves {
            [] => Filter::AlwaysTrue,
            leaves => Filter::And(leaves.iter().map(|l| (*l).clone()).collect::<Vec<_>>().into()),
        };
        let key_at = variable.iter().position(|leaf| key_of(leaf, names).is_some());
        let key = key_at.and_then(|at| key_of(variable[at], names));
        let residual: Vec<&Filter> = variable
            .iter()
            .enumerate()
            .filter(|(at, _)| Some(*at) != key_at)
            .map(|(_, leaf)| *leaf)
            .collect();
        Some(Lead {
            independent: all(&independent),
            variable: all(&variable),
            key,
            residual: all(&residual),
        })
    }
}

impl Stage {
    /// Whether running this stage needs a storage handle.
    pub fn needs_storage(&self) -> bool {
        match self {
            Stage::Lookup { .. }
            | Stage::GraphLookup(_)
            | Stage::Merge(_)
            | Stage::Out(_)
            | Stage::UnionWith(_) => true,
            Stage::Facet(facets) => {
                facets.iter().any(|(_, stages)| stages.iter().any(Stage::needs_storage))
            }
            _ => false,
        }
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
            Stage::VectorSearch(_) => "$vectorSearch",
            Stage::Facet(_) => "$facet",
            Stage::Bucket { .. } => "$bucket",
            Stage::BucketAuto { .. } => "$bucketAuto",
            Stage::GraphLookup(_) => "$graphLookup",
            Stage::Merge(_) => "$merge",
            Stage::Out(_) => "$out",
            Stage::SetWindowFields(_) => "$setWindowFields",
            Stage::Densify(_) => "$densify",
            Stage::Fill(_) => "$fill",
            Stage::SortByCount(_) => "$sortByCount",
            Stage::Sample(_) => "$sample",
            Stage::Redact(_) => "$redact",
            Stage::UnionWith(_) => "$unionWith",
        }
    }

    /// Whether this stage writes a collection rather than producing documents.
    pub fn writes(&self) -> bool {
        matches!(self, Stage::Merge(_) | Stage::Out(_))
    }
}

/// The collections a pipeline's `$lookup`, `$graphLookup` and `$unionWith` stages read, nested
/// sub-pipelines and `$facet` branches included, in the order a run reaches them,
/// each once. All are in the database the pipeline runs in.
pub fn foreign_collections(stages: &[Stage]) -> Vec<&str> {
    fn walk<'a>(stages: &'a [Stage], out: &mut Vec<&'a str>) {
        for stage in stages {
            match stage {
                Stage::Lookup { from, join, .. } => {
                    if !out.contains(&from.as_str()) {
                        out.push(from);
                    }
                    if let Join::Pipeline { stages: sub, .. } = join {
                        walk(sub, out);
                    }
                }
                Stage::GraphLookup(spec) if !out.contains(&spec.from.as_str()) => {
                    out.push(&spec.from);
                }
                Stage::UnionWith(spec) => {
                    if !out.contains(&spec.coll.as_str()) {
                        out.push(&spec.coll);
                    }
                    walk(&spec.stages, out);
                }
                Stage::Facet(facets) => facets.iter().for_each(|(_, sub)| walk(sub, out)),
                _ => {}
            }
        }
    }
    let mut out = Vec::new();
    walk(stages, &mut out);
    out
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
    let _clock = crate::expr::RequestClock::open();
    parse_pipeline(pipeline, vars, Placement::TOP)
}

/// Where a pipeline sits, for the one rule that depends on it.
///
/// A `$match` reads the same filter language in both places; the difference
/// is what a `"$$name"` string value most likely means there. At the top
/// level there is no `let` for it to refer to, so it is the literal string a
/// stored document may hold. Inside a `$lookup` sub-pipeline it is almost
/// certainly a variable the author expected the filter to substitute, and
/// the filter never will — see [`refuse_variables_in_match`].
///
/// `in_facet` marks a pipeline anywhere under a `$facet`, a `$lookup` sub-pipeline of
/// one included, where `$facet` and `$vectorSearch` are refused.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Placement {
    /// Inside a `$lookup` sub-pipeline.
    sub: bool,
    in_facet: bool,
    /// Anywhere under a `$unionWith` sub-pipeline, where `$out`, `$merge` and
    /// `$vectorSearch` are refused.
    in_union: bool,
}

impl Placement {
    const TOP: Placement = Placement { sub: false, in_facet: false, in_union: false };
}

fn parse_pipeline(pipeline: &[Document], vars: &[String], at: Placement) -> Result<Vec<Stage>> {
    let weight = pipeline_weight(pipeline);
    if weight > MAX_PIPELINE_STAGES {
        return Err(too_many_stages(weight));
    }
    let stages = pipeline
        .iter()
        .enumerate()
        .map(|(i, stage)| parse_stage(stage, vars, at, i))
        .collect::<Result<Vec<_>>>()?;
    if let Some((at, stage)) =
        stages.iter().enumerate().find(|(i, s)| s.writes() && i + 1 < stages.len())
    {
        return Err(Error::InvalidQuery(format!(
            "{} must be the last stage of the pipeline, but it is stage {} of {}",
            stage.name(),
            at + 1,
            stages.len()
        )));
    }
    Ok(move_windows(stages))
}

/// Whether a `$skip` and a `$limit` are moved ahead of the stage at `at` in `stages` (ADR-270),
/// as the reference's optimizer moves them: it neither filters nor changes the number of
/// documents, so the documents the window drops need not go through it. That is `$project`
/// (and `$unset`, which is one), `$addFields`/`$set`, `$replaceRoot`/`$replaceWith` and
/// `$lookup` (measured: `q8-probes/adr270-reorder.js` and `adr270-reorder2.js`), and a `$match`
/// of nothing, which the reference drops, unless it is one of the `$match` stages the
/// pipeline begins with, which are the filter of its source ([`leading_match`]). It is not
/// `$match`, `$redact`, `$group`, `$sort`, `$unwind`, `$sample`, `$sortByCount`,
/// `$setWindowFields`, `$graphLookup`, `$unionWith`, `$facet`, `$merge` or `$out`: a window
/// stays behind each of those.
fn lets_a_window_pass(stages: &[Stage], at: usize) -> bool {
    match &stages[at] {
        Stage::Project { .. }
        | Stage::AddFields(_)
        | Stage::ReplaceRoot(_)
        | Stage::Lookup { .. } => true,
        Stage::Match(filter) => {
            matches!(**filter, Filter::AlwaysTrue)
                && stages[..at].iter().any(|stage| !matches!(stage, Stage::Match(_)))
        }
        _ => false,
    }
}

/// A run of `$skip` and `$limit` as the one window it is: the first `skip` documents dropped,
/// then `limit` taken (all the rest if there is none).
#[derive(Clone, Copy, Default)]
struct Window {
    skip: usize,
    limit: Option<usize>,
}

impl Window {
    /// The window of this one followed by a `$skip` of `n`: it drops what the window takes
    /// first, and what is dropped is no longer taken.
    fn then_skip(self, n: usize) -> Window {
        match self.limit {
            None => Window { skip: self.skip.saturating_add(n), limit: None },
            Some(limit) => {
                let dropped = n.min(limit);
                Window { skip: self.skip.saturating_add(dropped), limit: Some(limit - dropped) }
            }
        }
    }

    /// The window of this one followed by a `$limit` of `n`.
    fn then_limit(self, n: usize) -> Window {
        Window { skip: self.skip, limit: Some(self.limit.map_or(n, |limit| limit.min(n))) }
    }

    /// The stages that are this window: a `$skip` if it drops any, then a `$limit` if it takes
    /// a number. A window that takes nothing is a `$limit` of 0 alone, which no request can
    /// write (it is a `400`) and `$limit: 5` then `$skip: 5` makes: what it drops is of no
    /// account, and a `$skip` before it would only be a number of documents for a `$sort` to
    /// keep.
    fn stages(self) -> impl Iterator<Item = Stage> {
        let takes_nothing = self.limit == Some(0);
        (self.skip > 0 && !takes_nothing)
            .then_some(Stage::Skip(self.skip))
            .into_iter()
            .chain(self.limit.map(Stage::Limit))
    }
}

/// The reference's reordering of `$skip` and `$limit` (ADR-270): each run of them is one
/// window ([`Window`]: `$skip: 2, $skip: 3` is `$skip: 5`, `$limit: 10, $limit: 3` is
/// `$limit: 3`, `$limit: 8, $skip: 5` is `$skip: 5, $limit: 3`) and the window is moved ahead of
/// the stages it [passes](lets_a_window_pass), repeatedly, so that it meets the window that is already
/// there and the two are one. The documents a window drops are then never evaluated by the
/// stages it passed, so an error in one is not raised, as in the reference.
///
/// A rewrite of the stages and not a change to how they run: the pipeline that is run, and
/// that `explain` reports, is the one written here.
fn move_windows(stages: Vec<Stage>) -> Vec<Stage> {
    let mut out: Vec<Stage> = Vec::with_capacity(stages.len());
    for stage in stages {
        let (skip, limit) = match stage {
            Stage::Skip(n) => (n, None),
            Stage::Limit(n) => (0, Some(n)),
            other => {
                out.push(other);
                continue;
            }
        };
        // Past the stages it passes, to the window that ends there, if one does.
        let mut end = out.len();
        while end > 0 && lets_a_window_pass(&out, end - 1) {
            end -= 1;
        }
        let mut start = end;
        while start > 0 && matches!(out[start - 1], Stage::Skip(_) | Stage::Limit(_)) {
            start -= 1;
        }
        let window = out[start..end].iter().fold(Window::default(), |window, stage| match stage {
            Stage::Skip(n) => window.then_skip(*n),
            Stage::Limit(n) => window.then_limit(*n),
            _ => window,
        });
        let window = match limit {
            Some(n) => window.then_limit(n),
            None => window.then_skip(skip),
        };
        out.splice(start..end, window.stages());
    }
    out
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

fn parse_stage(stage: &Document, vars: &[String], at: Placement, index: usize) -> Result<Stage> {
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
            if at.sub {
                refuse_variables_in_match(raw)?;
                // A `$expr` here reads the `let` names in scope (ADR-222).
                return Ok(Stage::Match(Box::new(filter::parse_in_scope(raw, vars)?)));
            }
            Ok(Stage::Match(Box::new(filter::parse(raw)?)))
        }
        "$project" => parse_project(as_document(name, value)?, vars),
        "$addFields" | "$set" => {
            Ok(Stage::AddFields(parse_computed(name, as_document(name, value)?, vars)?))
        }
        "$replaceRoot" => parse_replace_root(as_document(name, value)?, vars),
        "$sort" => Ok(Stage::Sort(shape::parse_sort(as_document(name, value)?)?)),
        "$limit" => match as_count(name, value)? {
            // As the reference refuses it: a limit of nothing is not a window, and `$skip` past
            // the end or a `$match` that selects nothing say it.
            0 => Err(Error::InvalidQuery("$limit must be positive, found 0".into())),
            n => Ok(Stage::Limit(n)),
        },
        "$skip" => Ok(Stage::Skip(as_count(name, value)?)),
        "$count" => match value {
            Bson::String(field) if !field.is_empty() => Ok(Stage::Count(field.clone())),
            _ => Err(Error::InvalidQuery(
                "$count takes the name of the output field, e.g. {$count: \"total\"}".into(),
            )),
        },
        "$unwind" => parse_unwind(value),
        "$group" => parse_group(as_document(name, value)?, vars),
        "$lookup" => parse_lookup(as_document(name, value)?, vars, at),
        "$facet" => {
            if at.in_facet {
                return Err(Error::InvalidQuery(
                    "$facet cannot be used inside a $facet, a $lookup sub-pipeline of one \
                     included"
                        .into(),
                ));
            }
            parse_facet(as_document(name, value)?, vars, at)
        }
        "$graphLookup" => parse_graph_lookup(as_document(name, value)?, vars, at),
        "$replaceWith" => Ok(Stage::ReplaceRoot(Expr::parse_with_vars(value, vars)?)),
        "$sortByCount" => stages::parse_sort_by_count(value, vars),
        "$unset" => stages::parse_unset(value),
        "$sample" => stages::parse_sample(value),
        "$redact" => Ok(Stage::Redact(Expr::parse_redact(value, vars)?)),
        "$unionWith" => parse_union_with(value, vars, at),
        "$bucket" => parse_bucket(as_document(name, value)?, vars),
        "$bucketAuto" => parse_bucket_auto(as_document(name, value)?, vars),
        "$setWindowFields" => crate::window::parse(as_document(name, value)?, vars)
            .map(|spec| Stage::SetWindowFields(Box::new(spec))),
        "$densify" => crate::densify::parse(as_document(name, value)?)
            .map(|spec| Stage::Densify(Box::new(spec))),
        "$fill" => crate::fill::parse(as_document(name, value)?, vars)
            .map(|spec| Stage::Fill(Box::new(spec))),
        "$vectorSearch" => {
            // The stage is the pipeline's source, in place of the collection
            // scan, so it can only be the first stage of the top-level
            // pipeline; a `$lookup` sub-pipeline's source is the foreign
            // collection scan.
            if at.in_facet {
                return Err(Error::InvalidQuery(
                    "$vectorSearch cannot be used inside a $facet: it is a pipeline's source, \
                     and a facet's source is the stage's input"
                        .into(),
                ));
            }
            if at.in_union {
                return Err(Error::InvalidQuery(
                    "$vectorSearch cannot be used inside a $unionWith sub-pipeline: it is a \
                     pipeline's source"
                        .into(),
                ));
            }
            if at.sub {
                return Err(Error::InvalidQuery(
                    "$vectorSearch must be the first stage of the top-level pipeline; it cannot \
                     run inside a $lookup sub-pipeline"
                        .into(),
                ));
            }
            if index != 0 {
                return Err(Error::InvalidQuery(format!(
                    "$vectorSearch must be the first stage of the pipeline (it is the source), \
                     but it is stage {}",
                    index + 1
                )));
            }
            parse_vector_search(as_document(name, value)?)
        }
        "$merge" | "$out" => {
            if at.in_facet || at.sub || at.in_union {
                return Err(Error::InvalidQuery(format!(
                    "{name} writes a collection, so it can only be the last stage of the \
                     top-level pipeline; it cannot run inside a {}",
                    if at.in_facet {
                        "$facet"
                    } else if at.in_union {
                        "$unionWith sub-pipeline"
                    } else {
                        "$lookup sub-pipeline"
                    }
                )));
            }
            if name == "$merge" {
                parse_merge(as_document(name, value)?)
            } else {
                parse_target(name, value).map(Stage::Out)
            }
        }
        // `UnsupportedOperator` renders its payload quoted — `unsupported
        // operator "x"` — so the guidance goes in an `InvalidQuery`, whose
        // format composes with a sentence. Both are a 400; this is about the
        // message a caller actually reads.
        other => Err(Error::InvalidQuery(format!(
            "{other} is not a pipeline stage; supported: $match, $project, $addFields, $set, \
             $replaceRoot, $replaceWith, $unset, $sort, $sortByCount, $limit, $skip, $sample, \
             $redact, $unwind, $group, $count, $lookup, $unionWith, $facet, $bucket, \
             $bucketAuto, $graphLookup, $setWindowFields, $densify, $fill, $vectorSearch, $merge, $out"
        ))),
    }
}

/// The target of `$merge`'s `into` or of `$out`: a collection name, or `{db, coll}`.
fn parse_target(stage: &str, value: &Bson) -> Result<Target> {
    match value {
        Bson::String(coll) if !coll.is_empty() => Ok(Target { db: None, coll: coll.clone() }),
        Bson::Document(spec) => {
            if spec.contains_key("timeseries") {
                return Err(Error::InvalidQuery(format!(
                    "{stage} cannot create a time series collection: its `timeseries` option is \
                     not supported"
                )));
            }
            deny_unknown_keys(stage, spec, &["db", "coll"])?;
            let name = |key: &str| match spec.get(key) {
                Some(Bson::String(s)) if !s.is_empty() => Ok(s.clone()),
                _ => Err(Error::InvalidQuery(format!(
                    "{stage} takes a collection name or {{db: <name>, coll: <name>}}, with both \
                     as non-empty strings"
                ))),
            };
            Ok(Target { db: Some(name("db")?), coll: name("coll")? })
        }
        _ => Err(Error::InvalidQuery(format!(
            "{stage} takes a collection name or {{db: <name>, coll: <name>}}, found {}",
            type_name(value)
        ))),
    }
}

/// `{$merge: {into, on, whenMatched, whenNotMatched}}` (ADR-255).
///
/// The match key is `_id` and nothing else. A match on another field is a read
/// followed by a write, and with no uniqueness enforced across members two members
/// running the same `$merge` at the same time each read "no match" and each insert:
/// the collection ends up with the duplicate the key was meant to prevent. `_id` is
/// the one key the store keeps one document per, so `on` is accepted only as
/// absent or `"_id"`. `whenMatched: "fail"` and `whenNotMatched: "fail"` are
/// refused for the same kind of reason (they are checks that would have to hold
/// across members), a pipeline-form `whenMatched` and `let` because they are not
/// built.
fn parse_merge(spec: &Document) -> Result<Stage> {
    if spec.contains_key("let") {
        return Err(Error::InvalidQuery(
            "$merge `let` is not supported: it only serves a pipeline-form `whenMatched`, which \
             is not supported either"
                .into(),
        ));
    }
    deny_unknown_keys("$merge", spec, &["into", "on", "whenMatched", "whenNotMatched"])?;
    let Some(into) = spec.get("into") else {
        return Err(Error::InvalidQuery(
            "$merge needs `into`: a collection name or {db, coll}".into(),
        ));
    };
    let into = parse_target("$merge", into)?;
    match spec.get("on") {
        None => {}
        Some(Bson::String(field)) if field == ID_FIELD => {}
        Some(other) => {
            return Err(Error::InvalidQuery(format!(
                "$merge `on` can only be \"_id\" (or left out), found {}: a match on any other \
                 key is a read then a write, and two members merging at the same time would each \
                 find no match and each insert, leaving duplicates that nothing enforces against",
                match other {
                    Bson::String(field) => format!("{field:?}"),
                    other => type_name(other).to_string(),
                }
            )));
        }
    }
    let when_matched = match spec.get("whenMatched") {
        None => WhenMatched::Merge,
        Some(Bson::String(mode)) => match mode.as_str() {
            "replace" => WhenMatched::Replace,
            "merge" => WhenMatched::Merge,
            "keepExisting" => WhenMatched::KeepExisting,
            "fail" => {
                return Err(Error::InvalidQuery(
                    "$merge whenMatched \"fail\" is not supported: it cannot be checked across \
                     members, which can each accept a write of the same `_id` before either \
                     hears of the other"
                        .into(),
                ));
            }
            other => {
                return Err(Error::InvalidQuery(format!(
                    "$merge whenMatched is \"replace\", \"merge\" or \"keepExisting\", found \
                     {other:?}"
                )));
            }
        },
        Some(Bson::Array(_)) => {
            return Err(Error::InvalidQuery(
                "a pipeline-form $merge `whenMatched` is not supported; use \"replace\", \
                 \"merge\" or \"keepExisting\""
                    .into(),
            ));
        }
        Some(other) => {
            return Err(Error::InvalidQuery(format!(
                "$merge whenMatched is \"replace\", \"merge\" or \"keepExisting\", found {}",
                type_name(other)
            )));
        }
    };
    let insert_unmatched = match spec.get("whenNotMatched") {
        None => true,
        Some(Bson::String(mode)) => match mode.as_str() {
            "insert" => true,
            "discard" => false,
            "fail" => {
                return Err(Error::InvalidQuery(
                    "$merge whenNotMatched \"fail\" is not supported: it cannot be checked \
                     across members, which can each lack a document another already has"
                        .into(),
                ));
            }
            other => {
                return Err(Error::InvalidQuery(format!(
                    "$merge whenNotMatched is \"insert\" or \"discard\", found {other:?}"
                )));
            }
        },
        Some(other) => {
            return Err(Error::InvalidQuery(format!(
                "$merge whenNotMatched is \"insert\" or \"discard\", found {}",
                type_name(other)
            )));
        }
    };
    Ok(Stage::Merge(Box::new(MergeSpec { into, when_matched, insert_unmatched })))
}

/// The most facets one `$facet` may name. Each is a pipeline over the whole input,
/// run in turn, so the number is a bound on the work one stage can ask for.
pub const MAX_FACETS: usize = 64;

/// `{$facet: {<name>: [<stages>], ...}}`.
///
/// Each facet is a pipeline parsed in the scope the `$facet` sits in, at the
/// same placement (so a `$lookup` `let` name is readable in it, and the rule
/// about a `$$name` string in a `$match` applies where it applies outside).
/// Refused: no facets, more than [`MAX_FACETS`], a name that is empty, starts
/// with `$` or holds a `.` or a NUL (it becomes a field of the answer), a facet
/// that is not an array of stage documents, and, inside a facet, `$facet` (a
/// facet's output is one document, and a facet of facets has no use another
/// stage does not have) and `$vectorSearch` (it is the pipeline's source, and a
/// facet's source is the stage's input).
fn parse_facet(spec: &Document, vars: &[String], at: Placement) -> Result<Stage> {
    if spec.is_empty() {
        return Err(Error::InvalidQuery(
            "$facet needs at least one facet: {$facet: {<name>: [<stages>]}}".into(),
        ));
    }
    if spec.len() > MAX_FACETS {
        return Err(Error::InvalidQuery(format!(
            "$facet names {} facets, over the limit of {MAX_FACETS}",
            spec.len()
        )));
    }
    let mut facets = Vec::with_capacity(spec.len());
    for (name, value) in spec {
        if name.is_empty() || name.starts_with('$') || name.contains('.') || name.contains('\0') {
            return Err(Error::InvalidQuery(format!(
                "a $facet name becomes a field of the answer, so it cannot be empty, start with \
                 `$`, or contain `.` or a NUL: {name:?}"
            )));
        }
        let Bson::Array(raw) = value else {
            return Err(Error::InvalidQuery(format!(
                "$facet `{name}` is an array of pipeline stages, found {}",
                type_name(value)
            )));
        };
        let raw = raw
            .iter()
            .map(|stage| {
                let doc = stage.as_document().ok_or_else(|| {
                    Error::InvalidQuery(format!(
                        "each $facet `{name}` stage is a document, found {}",
                        type_name(stage)
                    ))
                })?;
                if let Some(refused) = doc.keys().find(|k| *k == "$facet" || *k == "$vectorSearch")
                {
                    return Err(Error::InvalidQuery(format!(
                        "{refused} cannot be used inside a $facet (in `{name}`)"
                    )));
                }
                Ok(doc.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        facets
            .push((name.clone(), parse_pipeline(&raw, vars, Placement { in_facet: true, ..at })?));
    }
    Ok(Stage::Facet(facets))
}

/// Run a `$facet`: every facet over the same `input`, in the order written, and
/// the answer is the one document `{<name>: [<output>], ...}`.
///
/// `run` runs one facet's stages over the documents it is given; the caller
/// supplies it because only it can run a `$lookup` (the pure pipeline in
/// [`apply_with_vars`] passes one that cannot). The input is copied for every
/// facet but the last, which takes it, and a facet's output is kept while the
/// next runs, so what is held is bounded two ways, both **refused**, never cut
/// short: the facets' outputs together may not exceed the pipeline's document
/// ceiling, and the answer, which is one document, may not exceed the size a
/// stored document may (`kimmy_core::limits::MAX_DOCUMENT_BYTES`), counted as the
/// facets finish and refused at the first document that passes it, so a later
/// facet never runs for an answer that cannot be returned. A facet that
/// fails fails the stage. `$$NOW` is the same date in every facet: it is read
/// when the pipeline is parsed.
pub fn run_facet<E: From<Error>>(
    facets: &[(String, Vec<Stage>)],
    input: Vec<Document>,
    limits: &Limits,
    mut run: impl FnMut(&[Stage], Vec<Document>) -> std::result::Result<Vec<Document>, E>,
) -> std::result::Result<Document, E> {
    // What the stage holds (ADR-269): its input until the last branch takes it, and a
    // copy of it for each earlier branch, charged before the copy is made. The stages
    // inside a branch charge what they hold, input included, to the same budget; the
    // branches' outputs are alive together, but the answer is held to 16 MiB below.
    let mut input_lease = limits.buffer.lease("the input of $facet");
    input_lease.charge_docs(&input)?;
    let mut answer = Document::new();
    let mut held = 0usize;
    // The bytes the answer will encode to, kept as it grows: a document header and
    // terminator, then for each facet its element and its array.
    let mut bytes = 5usize;
    // Every increment is checked, so no part of the answer (a facet's header included,
    // an empty facet's with a long name) can pass the cap unseen.
    let charge = |bytes: &mut usize, n: usize| -> Result<()> {
        *bytes += n;
        if *bytes > kimmy_core::limits::MAX_DOCUMENT_BYTES {
            return Err(Error::AnswerTooLarge {
                what: "$facet answer",
                bytes: *bytes,
                max: kimmy_core::limits::MAX_DOCUMENT_BYTES,
                remedy: "split the facets into separate requests, or $limit and $project \
                         inside them",
            });
        }
        Ok(())
    };
    let mut input = Some(input);
    let mut ticker = Ticker::new();
    for (at, (name, stages)) in facets.iter().enumerate() {
        // Between facets, as well as inside each one's stages and below.
        deadline::check()?;
        let docs = if at + 1 == facets.len() {
            input_lease.release(input_lease.bytes());
            input.take().unwrap_or_default()
        } else {
            // The copy is charged before it is made and the charge handed back as it is
            // passed on: the branch's first stage charges it as its own input.
            let mut copy_lease = limits.buffer.lease("a copy of the input of $facet");
            copy_lease.charge_docs(input.as_deref().unwrap_or_default())?;
            let copy = input.clone().unwrap_or_default();
            drop(copy_lease);
            copy
        };
        let out = run(stages, docs)?;
        held += out.len();
        check_limit("$facet", held, limits)?;
        // The element (type byte, name, terminator), the array's header and terminator,
        // and each document as an element of it (type byte, decimal index, terminator).
        charge(&mut bytes, 1 + name.len() + 1 + 4 + 1)?;
        for (index, doc) in out.iter().enumerate() {
            ticker.tick()?;
            let encoded = bson::serialize_to_vec(doc)
                .map_err(|e| Error::Internal(format!("$facet output could not be encoded: {e}")))?;
            // Refused as soon as it is over, so a later facet is never run for an answer
            // that cannot be returned.
            charge(&mut bytes, 1 + index.to_string().len() + 1 + encoded.len())?;
        }
        answer.insert(name.clone(), Bson::Array(out.into_iter().map(Bson::Document).collect()));
    }
    Ok(answer)
}

/// Parse a `$vectorSearch` stage.
///
/// The spelling is the vector endpoint's own: `query` or `vector`, `k`,
/// `per_document`, `filter`. The names another system uses for the same things
/// are refused with the KimmyDB spelling, not aliased (ADR-216).
fn parse_vector_search(spec: &Document) -> Result<Stage> {
    let mut out =
        VectorSearch { query: None, vector: None, k: None, per_document: None, filter: None };
    for (key, value) in spec {
        match key.as_str() {
            "query" => match value {
                Bson::String(text) => out.query = Some(text.clone()),
                _ => {
                    return Err(Error::InvalidQuery(format!(
                        "$vectorSearch `query` must be a string, found {}",
                        type_name(value)
                    )));
                }
            },
            "vector" => out.vector = Some(parse_query_vector(value)?),
            "k" => out.k = Some(as_count("$vectorSearch `k`", value)?),
            "per_document" => {
                out.per_document = Some(as_count("$vectorSearch `per_document`", value)?)
            }
            "filter" => out.filter = Some(as_document("$vectorSearch `filter`", value)?.clone()),
            "queryVector" => {
                return Err(Error::InvalidQuery(
                    "$vectorSearch does not take `queryVector`; use `vector`".into(),
                ));
            }
            "limit" => {
                return Err(Error::InvalidQuery(
                    "$vectorSearch does not take `limit`; use `k` for the number of hits".into(),
                ));
            }
            "numCandidates" => {
                return Err(Error::InvalidQuery(
                    "$vectorSearch does not take `numCandidates`: there is no such knob in \
                     KimmyDB, which has one vector index per collection; `k` is the number of hits"
                        .into(),
                ));
            }
            "index" => {
                return Err(Error::InvalidQuery(
                    "$vectorSearch does not take `index`: there is nothing to name, KimmyDB has \
                     one vector index per collection"
                        .into(),
                ));
            }
            other => {
                return Err(Error::InvalidQuery(format!(
                    "$vectorSearch does not take `{other}`; its fields are query, vector, k, \
                     per_document and filter"
                )));
            }
        }
    }
    if out.query.is_none() && out.vector.is_none() {
        return Err(Error::InvalidQuery("$vectorSearch needs `query` text or a `vector`".into()));
    }
    Ok(Stage::VectorSearch(out))
}

fn parse_query_vector(value: &Bson) -> Result<Vec<f32>> {
    let Bson::Array(items) = value else {
        return Err(Error::InvalidQuery(format!(
            "$vectorSearch `vector` must be an array of numbers, found {}",
            type_name(value)
        )));
    };
    items
        .iter()
        .map(|item| match item {
            Bson::Double(d) => Ok(*d as f32),
            Bson::Int32(n) => Ok(*n as f32),
            Bson::Int64(n) => Ok(*n as f32),
            // A number, but one whose conversion to a 32-bit float needs the
            // decimal-to-binary rounding this build does not have.
            Bson::Decimal128(_) => Err(Error::InvalidQuery(
                "$vectorSearch `vector` must hold doubles or integers: a Decimal128 would have to \
                 be rounded to a 32-bit float, which this build does not do"
                    .into(),
            )),
            other => Err(Error::InvalidQuery(format!(
                "$vectorSearch `vector` must hold only numbers, found {}",
                type_name(other)
            ))),
        })
        .collect()
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
    match first_variable_string(doc) {
        Some(s) => Err(Error::InvalidQuery(format!(
            "{s} is not available as a value in a $match: the filter language has no \
             variables; write the comparison in $expr, {{$expr: {{$eq: [\"$field\", \
             \"{s}\"]}}}}, or bind it in an $addFields stage and $match on the computed \
             field"
        ))),
        None => Ok(()),
    }
}

/// The first `"$$name"` string value a filter holds, outside any `$expr`.
fn first_variable_string(doc: &Document) -> Option<String> {
    fn walk_doc(doc: &Document) -> Option<String> {
        doc.iter().filter(|(key, _)| key.as_str() != "$expr").find_map(|(_, value)| walk(value))
    }
    fn walk(value: &Bson) -> Option<String> {
        match value {
            Bson::String(s) if s.starts_with("$$") => Some(s.clone()),
            Bson::Document(doc) => walk_doc(doc),
            Bson::Array(items) => items.iter().find_map(walk),
            _ => None,
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
            // A decimal flag reads as the number it is, as a double does: zero
            // excludes, anything else includes.
            Bson::Decimal128(d)
                if kimmy_core::decimal::decode(d) == kimmy_core::decimal::Decimal::Zero =>
            {
                if key == ID_FIELD {
                    excluded_id = true;
                } else {
                    excluded_other = true;
                }
            }
            Bson::Int32(_)
            | Bson::Int64(_)
            | Bson::Double(_)
            | Bson::Decimal128(_)
            | Bson::Boolean(true) => {
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

/// `{$unionWith: "coll"}` or `{$unionWith: {coll, pipeline}}` (ADR-263).
///
/// The collection is in the database the pipeline runs in, as a `$lookup`'s `from` is.
/// The sub-pipeline is parsed in the scope the stage sits in (so an enclosing
/// `$lookup`'s `let` names are readable) with `$out`, `$merge` and `$vectorSearch`
/// refused anywhere under it. A `$facet` is allowed in it, as the reference allows one.
fn parse_union_with(value: &Bson, vars: &[String], at: Placement) -> Result<Stage> {
    let (coll, raw) = match value {
        Bson::String(coll) => (coll.clone(), None),
        Bson::Document(spec) => {
            deny_unknown_keys("$unionWith", spec, &["coll", "pipeline"])?;
            let coll = match spec.get("coll") {
                Some(Bson::String(coll)) => coll.clone(),
                Some(other) => {
                    return Err(Error::InvalidQuery(format!(
                        "$unionWith `coll` is a collection name, found {}",
                        type_name(other)
                    )));
                }
                None => {
                    return Err(Error::InvalidQuery(
                        "$unionWith needs `coll`, the collection to read: {$unionWith: {coll: \
                         <name>, pipeline: [..]}}"
                            .into(),
                    ));
                }
            };
            let raw = match spec.get("pipeline") {
                None => None,
                Some(Bson::Array(raw)) => Some(raw),
                Some(other) => {
                    return Err(Error::InvalidQuery(format!(
                        "$unionWith `pipeline` is an array of stages, found {}",
                        type_name(other)
                    )));
                }
            };
            (coll, raw)
        }
        other => {
            return Err(Error::InvalidQuery(format!(
                "$unionWith takes a collection name or {{coll, pipeline}}, found {}",
                type_name(other)
            )));
        }
    };
    if coll.is_empty() {
        return Err(Error::InvalidQuery("$unionWith needs a non-empty collection name".into()));
    }
    let stages = match raw {
        None => Vec::new(),
        Some(raw) => {
            let raw = raw
                .iter()
                .map(|stage| {
                    stage.as_document().cloned().ok_or_else(|| {
                        Error::InvalidQuery(format!(
                            "each $unionWith pipeline stage is a document, found {}",
                            type_name(stage)
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            parse_pipeline(&raw, vars, Placement { in_union: true, ..at })?
        }
    };
    Ok(Stage::UnionWith(Box::new(UnionWith { coll, stages })))
}

pub(crate) fn as_document<'a>(stage: &str, value: &'a Bson) -> Result<&'a Document> {
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
pub(crate) fn deny_unknown_keys(stage: &str, doc: &Document, allowed: &[&str]) -> Result<()> {
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
    let refused = || {
        Error::InvalidQuery(format!(
            "{stage} takes a non-negative whole number, found {}",
            type_name(value)
        ))
    };
    let n = match value {
        Bson::Int32(n) => i64::from(*n),
        Bson::Int64(n) => *n,
        // A double that is not a whole number is a mistake worth reporting
        // rather than rounding: `$limit: 2.5` has no defensible meaning.
        Bson::Double(d) if d.fract() == 0.0 => *d as i64,
        // A decimal by its exact value, as the double of that value is.
        Bson::Decimal128(d) => kimmy_core::decimal::whole_number(d).ok_or_else(refused)?,
        _ => return Err(refused()),
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

    let fields = parse_accumulators("$group", spec, vars, true)?;
    Ok(Stage::Group { id, fields })
}

/// The `{<name>: {<accumulator>: <expression>}}` fields of `$group`, `$bucket`
/// and `$bucketAuto`. `skip_id` leaves out the `_id` key, which `$group` reads
/// itself.
pub(crate) fn parse_accumulators(
    stage: &str,
    spec: &Document,
    vars: &[String],
    skip_id: bool,
) -> Result<Vec<(String, Accumulator)>> {
    let mut fields = Vec::new();
    for (name, value) in spec {
        if skip_id && name == "_id" {
            continue;
        }
        let acc = value.as_document().ok_or_else(|| {
            Error::InvalidQuery(format!(
                "{stage} field `{name}` must be an accumulator document, e.g. {{$sum: 1}}"
            ))
        })?;
        if acc.len() != 1 {
            return Err(Error::InvalidQuery(format!(
                "{stage} field `{name}` must name exactly one accumulator"
            )));
        }
        let (op, arg) = acc.iter().next().expect("length checked above");
        crate::accumulators::check_unary(op, arg)?;
        if let Some(spec) = crate::accumulators::parse(op, arg, vars)? {
            fields.push((name.clone(), Accumulator::Ext(spec)));
            continue;
        }
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
            "$mergeObjects" => Accumulator::MergeObjects(expr),
            other => {
                return Err(Error::InvalidQuery(format!(
                    "{other} is not an accumulator; supported: $sum, $avg, $min, $max, $first, \
                     $last, $push, $addToSet, $mergeObjects, $stdDevPop, $stdDevSamp, $top, \
                     $bottom, $topN, $bottomN, $firstN, $lastN, $maxN, $minN, $count"
                )));
            }
        };
        fields.push((name.clone(), accumulator));
    }
    Ok(fields)
}

/// `_id` is the bucket's key, so an `output` field of that name would overwrite it.
fn refuse_id_in_output(stage: &str, output: &Document) -> Result<()> {
    if output.contains_key("_id") {
        return Err(Error::InvalidQuery(format!(
            "{stage} `output` cannot name `_id`: it is the bucket's key; name the accumulator \
             something else"
        )));
    }
    Ok(())
}

/// What a `$bucket` or `$bucketAuto` computes when it is given no `output`.
fn default_bucket_output() -> Vec<(String, Accumulator)> {
    vec![("count".to_string(), Accumulator::Sum(Expr::Literal(Bson::Int32(1))))]
}

/// The kind of value a `$bucket` boundary is, so that a list of them is one
/// ordered series: every number is one kind (they compare by value), and so on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BoundaryKind {
    Number,
    String,
    Date,
    ObjectId,
    Bool,
}

impl BoundaryKind {
    fn of(value: &Bson) -> Option<Self> {
        match value {
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
                Some(Self::Number)
            }
            Bson::String(_) => Some(Self::String),
            Bson::DateTime(_) => Some(Self::Date),
            Bson::ObjectId(_) => Some(Self::ObjectId),
            Bson::Boolean(_) => Some(Self::Bool),
            _ => None,
        }
    }
}

/// Whether a boundary is a number that is not one: a `NaN` has no place in an
/// ordered series (it compares below every number).
fn is_nan(value: &Bson) -> bool {
    match value {
        Bson::Double(d) => d.is_nan(),
        Bson::Decimal128(d) => {
            matches!(kimmy_core::decimal::decode_raw(d), kimmy_core::decimal::Raw::Nan)
        }
        _ => false,
    }
}

/// `{$graphLookup: {from, startWith, connectFromField, connectToField, as,
/// maxDepth, depthField?, restrictSearchWithMatch?}}`.
///
/// **`maxDepth` is required**, a whole number from 0 to [`MAX_GRAPH_DEPTH`]: a walk
/// that follows links until it runs out has no bound the caller chose, and one the
/// server chose would cut it off without a word. The traversal is the caller's
/// depth and nothing else; what bounds the memory is refused, not cut (see the
/// executor). Every other key is closed. The two connect fields and the field
/// names are paths, not expressions: a name beginning with `$` is refused.
fn parse_graph_lookup(spec: &Document, vars: &[String], at: Placement) -> Result<Stage> {
    deny_unknown_keys(
        "$graphLookup",
        spec,
        &[
            "from",
            "startWith",
            "connectFromField",
            "connectToField",
            "as",
            "maxDepth",
            "depthField",
            "restrictSearchWithMatch",
        ],
    )?;
    let name = |key: &str| -> Result<String> {
        match spec.get(key) {
            Some(Bson::String(s))
                if !s.is_empty() && !s.split('.').any(|seg| seg.starts_with('$')) =>
            {
                Ok(s.clone())
            }
            Some(Bson::String(_)) => Err(Error::InvalidQuery(format!(
                "$graphLookup `{key}` is a field name: not empty, and no part of it begins with $"
            ))),
            _ => Err(Error::InvalidQuery(format!(
                "$graphLookup needs a string `{key}`; it takes from, startWith, \
                 connectFromField, connectToField, as and maxDepth"
            ))),
        }
    };
    let from = name("from")?;
    let connect_from = name("connectFromField")?;
    let connect_to = name("connectToField")?;
    let as_field = name("as")?;
    // `as` and `depthField` are written as a field of the document, whole: a dotted name
    // would be a path, which nests, fails through an array and is not what the caller
    // reads back, so it is refused rather than guessed at.
    if as_field.contains('.') {
        return Err(Error::InvalidQuery(format!(
            "$graphLookup `as` is the name of one field, not a path: {as_field:?}"
        )));
    }
    let start_with = spec.get("startWith").ok_or_else(|| {
        Error::InvalidQuery(
            "$graphLookup needs `startWith`, an expression for where each walk starts".into(),
        )
    })?;
    let start_with = Expr::parse_with_vars(start_with, vars)?;
    let max_depth = spec.get("maxDepth").ok_or_else(|| {
        Error::InvalidQuery(format!(
            "$graphLookup needs `maxDepth`, the most steps to follow (0 to {MAX_GRAPH_DEPTH}); \
             a walk with no bound the caller chose is not run"
        ))
    })?;
    let max_depth = match crate::expr::as_integer(max_depth, "$graphLookup `maxDepth`") {
        Ok(n) if (0..=MAX_GRAPH_DEPTH as i64).contains(&n) => n as usize,
        Ok(n) => {
            return Err(Error::InvalidQuery(format!(
                "$graphLookup `maxDepth` is from 0 to {MAX_GRAPH_DEPTH}, found {n}"
            )));
        }
        Err(e) => return Err(e),
    };
    let depth_field = match spec.get("depthField") {
        None => None,
        Some(_) => {
            let field = name("depthField")?;
            if field.contains('.') {
                return Err(Error::InvalidQuery(format!(
                    "$graphLookup `depthField` is the name of one field, not a path: {field:?}"
                )));
            }
            Some(field)
        }
    };
    let restrict = match spec.get("restrictSearchWithMatch") {
        None => None,
        Some(Bson::Document(raw)) => {
            // As a `$match` in a `$lookup` sub-pipeline: a `"$$name"` string would be a
            // literal here, and the author almost certainly meant the variable.
            if at.sub
                && let Some(s) = first_variable_string(raw)
            {
                // Not `$match`'s advice: this filter is parsed with no `let` bindings at
                // all, `$expr` included, so writing the comparison there is a 400 too.
                return Err(Error::InvalidQuery(format!(
                    "{s} is not available in $graphLookup `restrictSearchWithMatch`: `let` \
                     variables are not available there, and a \"$$name\" string would be the \
                     literal text. Restrict with a field condition, and compare against the \
                     variable in a stage after the $graphLookup"
                )));
            }
            Some(filter::parse(raw)?)
        }
        Some(other) => {
            return Err(Error::InvalidQuery(format!(
                "$graphLookup `restrictSearchWithMatch` is a filter document, found {}",
                type_name(other)
            )));
        }
    };
    Ok(Stage::GraphLookup(Box::new(GraphLookup {
        from,
        start_with,
        connect_from,
        connect_to,
        as_field,
        max_depth,
        depth_field,
        restrict,
    })))
}

/// `{$bucket: {groupBy, boundaries, default?, output?}}`.
///
/// Refused at parse: a key not in that list; no `groupBy`; `boundaries` that is
/// not an array of at least two values, holds a value that is not a number, a
/// string, a date, an ObjectId or a boolean (a string beginning with `$` is a
/// field path in an expression, and is not read as one here: a boundary is a
/// constant), a `NaN`, values of more than one kind, or values not strictly
/// ascending; a `default` that falls inside the ranges (it must be below the first
/// boundary or at or above the last, by the order every comparison uses); an
/// `output` that is not a non-empty document of accumulators.
fn parse_bucket(spec: &Document, vars: &[String]) -> Result<Stage> {
    deny_unknown_keys("$bucket", spec, &["groupBy", "boundaries", "default", "output"])?;
    let group_by = spec.get("groupBy").ok_or_else(|| {
        Error::InvalidQuery("$bucket needs a `groupBy` expression, e.g. \"$price\"".into())
    })?;
    let group_by = Expr::parse_with_vars(group_by, vars)?;
    let Some(Bson::Array(boundaries)) = spec.get("boundaries") else {
        return Err(Error::InvalidQuery(
            "$bucket needs `boundaries`, an array of at least two values in ascending order".into(),
        ));
    };
    if boundaries.len() < 2 {
        return Err(Error::InvalidQuery(format!(
            "$bucket `boundaries` needs at least two values, found {}",
            boundaries.len()
        )));
    }
    let mut kind = None;
    for (at, boundary) in boundaries.iter().enumerate() {
        let Some(this) = BoundaryKind::of(boundary).filter(|_| !is_nan(boundary)) else {
            return Err(Error::InvalidQuery(format!(
                "$bucket `boundaries` holds numbers, strings, dates, ObjectIds or booleans, \
                 found {} at position {at}",
                if is_nan(boundary) { "a NaN" } else { type_name(boundary) }
            )));
        };
        if matches!(boundary, Bson::String(s) if s.starts_with('$')) {
            return Err(Error::InvalidQuery(format!(
                "$bucket `boundaries` are constants, and position {at} begins with $"
            )));
        }
        match kind {
            None => kind = Some(this),
            Some(first) if first != this => {
                return Err(Error::InvalidQuery(format!(
                    "$bucket `boundaries` must all be of one type, and position {at} is {} \
                     where position 0 is {}",
                    type_name(boundary),
                    type_name(&boundaries[0])
                )));
            }
            Some(_) => {}
        }
        if at > 0 && !kimmy_core::cmp::canonical_cmp(&boundaries[at - 1], boundary).is_lt() {
            return Err(Error::InvalidQuery(format!(
                "$bucket `boundaries` must be in ascending order without repeats, and \
                 position {at} is not above position {}",
                at - 1
            )));
        }
    }
    let default = spec.get("default").cloned();
    if let Some(Bson::String(s)) = &default
        && s.starts_with('$')
    {
        return Err(Error::InvalidQuery(
            "$bucket `default` is a constant, and it begins with $".into(),
        ));
    }
    if let Some(default) = &default {
        let (first, last) = (&boundaries[0], &boundaries[boundaries.len() - 1]);
        let inside = kimmy_core::cmp::canonical_cmp(default, first).is_ge()
            && kimmy_core::cmp::canonical_cmp(default, last).is_lt();
        if inside {
            return Err(Error::InvalidQuery(
                "$bucket `default` must be below the first boundary or at or above the last, \
                 not inside the ranges"
                    .into(),
            ));
        }
    }
    let fields = match spec.get("output") {
        None => default_bucket_output(),
        Some(Bson::Document(output)) if !output.is_empty() => {
            refuse_id_in_output("$bucket", output)?;
            parse_accumulators("$bucket", output, vars, false)?
        }
        Some(_) => {
            return Err(Error::InvalidQuery(
                "$bucket `output` is a document of accumulators, e.g. {n: {$sum: 1}}, with at \
                 least one"
                    .into(),
            ));
        }
    };
    Ok(Stage::Bucket { group_by, boundaries: boundaries.clone(), default, fields })
}

/// `{$bucketAuto: {groupBy, buckets, output?}}`. `granularity` is refused: the
/// preferred-number series that round the boundaries are not built, and a stage
/// that ignored the option would answer boundaries the caller did not ask for.
fn parse_bucket_auto(spec: &Document, vars: &[String]) -> Result<Stage> {
    deny_unknown_keys("$bucketAuto", spec, &["groupBy", "buckets", "output", "granularity"])?;
    if spec.contains_key("granularity") {
        return Err(Error::InvalidQuery(
            "$bucketAuto `granularity` is not supported: the boundaries are the values at the \
             cut points, and no preferred-number series rounds them"
                .into(),
        ));
    }
    let group_by = spec.get("groupBy").ok_or_else(|| {
        Error::InvalidQuery("$bucketAuto needs a `groupBy` expression, e.g. \"$price\"".into())
    })?;
    let group_by = Expr::parse_with_vars(group_by, vars)?;
    let buckets = spec.get("buckets").ok_or_else(|| {
        Error::InvalidQuery("$bucketAuto needs `buckets`, the number of buckets wanted".into())
    })?;
    let buckets = match crate::expr::as_integer(buckets, "$bucketAuto `buckets`") {
        Ok(n) if n >= 1 => usize::try_from(n).unwrap_or(usize::MAX),
        // A whole number below 1 is named by its value, anything else by its type.
        Ok(n) => {
            return Err(Error::InvalidQuery(format!(
                "$bucketAuto `buckets` is a whole number of at least 1, found {n}"
            )));
        }
        Err(_) => {
            return Err(Error::InvalidQuery(format!(
                "$bucketAuto `buckets` is a whole number of at least 1, found {}",
                type_name(buckets)
            )));
        }
    };
    let fields = match spec.get("output") {
        None => default_bucket_output(),
        Some(Bson::Document(output)) if !output.is_empty() => {
            refuse_id_in_output("$bucketAuto", output)?;
            parse_accumulators("$bucketAuto", output, vars, false)?
        }
        Some(_) => {
            return Err(Error::InvalidQuery(
                "$bucketAuto `output` is a document of accumulators, e.g. {n: {$sum: 1}}, with \
                 at least one"
                    .into(),
            ));
        }
    };
    Ok(Stage::BucketAuto { group_by, buckets, fields })
}

/// Two forms, told apart by their keys and never mixed.
///
/// `localField`/`foreignField` is the equality join; `let`/`pipeline` is the
/// general one. MongoDB also accepts both at once — a "concise correlated
/// subquery" — and that is refused here: the executor would have to combine an
/// indexed pass with a per-document loop, and a caller who wants to reshape
/// what an equality join attached can do that with `$filter` or `$map` in the
/// stage after it.
fn parse_lookup(spec: &Document, vars: &[String], at: Placement) -> Result<Stage> {
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
            let stages = parse_pipeline(&raw_stages, &in_scope, Placement { sub: true, ..at })?;
            let lead = match stages.first() {
                Some(Stage::Match(f)) => Lead::analyze(f, &in_scope).map(Box::new),
                _ => None,
            };
            Join::Pipeline { vars: let_vars, stages, lead }
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
/// different types collapse into one bucket — 5, 5.0 and `Decimal128("5.00")`
/// group together, exactly as they would match one index entry. Every value has a
/// key, so the rendering fallback is unreachable; it stays so that a grouping key
/// is never a place to fail the whole query.
pub(crate) fn group_key(value: &Bson) -> Vec<u8> {
    kimmy_core::keyenc::encode(value).unwrap_or_else(|_| format!("raw:{value:?}").into_bytes())
}

/// What one stage holds while it runs (ADR-269): its input, until it returns, and the
/// documents it builds. Dropped when the stage returns or fails, which gives both back.
///
/// A stage that only moves its input (`$match`, `$limit`, `$skip`, `$count`, `$sample`)
/// builds nothing, so its input is all it holds. `$sort` holds the sort keys it reads from
/// each document as well, and charges them to `output`. `$setWindowFields`,
/// `$densify` and `$fill` charge the documents they buffer themselves (`$fill` also every
/// `value` it writes, on every path), and `$facet` its input and its branches, so none of
/// those charges its input here as well.
pub struct Held {
    /// The documents the stage builds: its output, or its buckets; the sort keys of `$sort`.
    pub output: Lease,
    _input: Option<Lease>,
}

impl Held {
    /// Charge `input` to the request's budget for `stage`.
    pub fn enter(stage: &Stage, input: &[Document], limits: &Limits) -> Result<Self> {
        Self::enter_with(stage, input, limits, None)
    }

    /// [`Held::enter`], for an input whose bytes are already charged: `arrived` is the lease
    /// that charged `input` document by document as a stage before this one handed them over
    /// (ADR-270). It becomes the stage's hold on its input, and nothing is charged again. A
    /// stage that charges its own input is given back its bytes first, so that it does not
    /// hold them twice.
    pub fn enter_with(
        stage: &Stage,
        input: &[Document],
        limits: &Limits,
        arrived: Option<Lease>,
    ) -> Result<Self> {
        let name = stage.name();
        let output = limits.buffer.lease(output_of(name));
        let charged = !matches!(
            stage,
            Stage::SetWindowFields(_) | Stage::Densify(_) | Stage::Fill(_) | Stage::Facet(_)
        );
        if !charged {
            drop(arrived);
            return Ok(Held { output, _input: None });
        }
        let lease = match arrived {
            Some(lease) => lease,
            None => {
                let mut lease = limits.buffer.lease(input_of(name));
                lease.charge_docs(input)?;
                lease
            }
        };
        Ok(Held { output, _input: Some(lease) })
    }
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
    apply_charged(stage, input, limits, vars, None)
}

/// [`apply_with_vars`] for an input already charged to the request's budget by `arrived`, the
/// lease of the sink that collected it document by document (ADR-270, [`Held::enter_with`]).
pub fn apply_charged(
    stage: &Stage,
    input: Vec<Document>,
    limits: &Limits,
    vars: &[Binding<'_>],
    arrived: Option<Lease>,
) -> Result<Vec<Document>> {
    // Every stage starts by looking at the request's deadline (ADR-266), and every
    // loop below holds a `Ticker` and ticks first in its body. A stage added to this
    // match does the same: `let mut ticker = Ticker::new();` before its loop and
    // `ticker.tick()?;` as the loop's first statement. See `kimmy_core::deadline`.
    deadline::check()?;
    let mut ticker = Ticker::new();
    // What the stage holds while it runs is charged to the request's budget (ADR-269): its
    // input until it returns, and the documents it builds as it builds them. A blocking
    // stage that buffers its own input, and `$facet`, charge for themselves.
    let mut held = Held::enter_with(stage, &input, limits, arrived)?;
    let out = match stage {
        // A `$expr` that cannot be evaluated against a document fails the
        // pipeline, as in a top-level filter (ADR-206).
        Stage::Match(f) => {
            let mut out = Vec::with_capacity(input.len());
            for doc in input {
                ticker.tick()?;
                if filter::matches_with(f, &doc, vars)? {
                    out.push(doc);
                }
            }
            out
        }
        Stage::Project { projection, computed } => {
            let mut out = Vec::with_capacity(input.len());
            for doc in &input {
                ticker.tick()?;
                let projected = project_doc(projection.as_ref(), computed, doc, vars)?;
                held.output.charge_doc(&projected)?;
                out.push(projected);
            }
            out
        }
        Stage::AddFields(computed) => {
            let mut out = Vec::with_capacity(input.len());
            for doc in &input {
                ticker.tick()?;
                let with = add_fields_doc(computed, doc, vars)?;
                held.output.charge_doc(&with)?;
                out.push(with);
            }
            out
        }
        Stage::ReplaceRoot(expr) => {
            let mut out = Vec::with_capacity(input.len());
            for doc in &input {
                ticker.tick()?;
                let new_root = replace_root_doc(expr, doc, vars)?;
                held.output.charge_doc(&new_root)?;
                out.push(new_root);
            }
            out
        }
        Stage::Sort(keys) => {
            // A sort key is a field path, not an expression, and orders by
            // the same rules `find`'s sort does — an array sorts by its
            // elements, and a path that crosses one is not fanned out.
            // In runs, with a look at the deadline between them (ADR-266). The keys it
            // reads from each document are held to the end, and charged beside the input.
            shape::sort_charging_keys(keys, input, &mut held.output)?
        }
        Stage::Limit(n) => input.into_iter().take(*n).collect(),
        Stage::Skip(n) => input.into_iter().skip(*n).collect(),
        Stage::Count(field) => {
            // An `Int32` while it fits, as the reference counts (ADR-268).
            let n = crate::expr::count_bson(input.len());
            vec![bson::doc! { field.as_str(): n }]
        }
        Stage::Unwind { path: p, preserve_null_and_empty, include_array_index } => unwind(
            input,
            p,
            *preserve_null_and_empty,
            include_array_index.as_deref(),
            limits,
            &mut held.output,
        )?,
        Stage::Group { .. } | Stage::Bucket { .. } | Stage::SortByCount(_) => {
            group_all(stage, &input, limits, vars, &mut held.output)?
        }
        Stage::BucketAuto { group_by, buckets, fields } => {
            bucket_auto(input, group_by, *buckets, fields, limits, vars, &mut held.output)?
        }
        Stage::SetWindowFields(spec) => crate::window::apply(spec, input, limits, vars)?,
        Stage::Densify(spec) => crate::densify::apply(spec, input, limits)?,
        Stage::Fill(spec) => crate::fill::apply(spec, input, limits, vars)?,
        Stage::VectorSearch(_) => {
            return Err(Error::Unsupported(
                "$vectorSearch is the pipeline's source and must be run by the executor that \
                 holds the vector index, not by the pure pipeline"
                    .into(),
            ));
        }
        Stage::Facet(facets) => {
            if stage.needs_storage() {
                return Err(Error::Unsupported(
                    "a $facet holding a $lookup reads another collection and must be run by the \
                     executor that holds a storage handle, not by the pure pipeline"
                        .into(),
                ));
            }
            let answer = run_facet(facets, input, limits, |stages, docs| {
                stages
                    .iter()
                    .try_fold(docs, |docs, stage| apply_with_vars(stage, docs, limits, vars))
            })?;
            vec![answer]
        }
        Stage::Merge(_) | Stage::Out(_) => {
            return Err(Error::Unsupported(format!(
                "{} writes a collection and must be run by the executor that holds a storage \
                 handle, not by the pure pipeline",
                stage.name()
            )));
        }
        Stage::GraphLookup(_) => {
            return Err(Error::Unsupported(
                "$graphLookup reads another collection and must be run by the executor that \
                 holds a storage handle, not by the pure pipeline"
                    .into(),
            ));
        }
        Stage::Sample(size) => stages::sample(input, *size, limits, &mut stages::system_below())?,
        Stage::Redact(expr) => stages::redact(input, expr, vars, &mut held.output)?,
        Stage::UnionWith(_) => {
            return Err(Error::Unsupported(
                "$unionWith reads another collection and must be run by the executor that \
                 holds a storage handle, not by the pure pipeline"
                    .into(),
            ));
        }
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

/// One document through `$project` (ADR-270: the body of the stage for one document, which
/// [`apply`] loops over and the chain of sinks calls as each document arrives).
pub(crate) fn project_doc(
    projection: Option<&Projection>,
    computed: &[(String, Expr)],
    doc: &Document,
    vars: &[Binding<'_>],
) -> Result<Document> {
    let mut projected = shape::project(projection, doc);
    // Computed fields read the *input*, not the projection's
    // output — otherwise `{$project: {a: 0, b: "$a"}}` would
    // depend on key order, which BSON preserves and nobody
    // reasons about.
    set_computed(&mut projected, computed, &Scope::with_bindings(doc, vars))?;
    Ok(projected)
}

/// One document through `$addFields` (see [`project_doc`]).
pub(crate) fn add_fields_doc(
    computed: &[(String, Expr)],
    doc: &Document,
    vars: &[Binding<'_>],
) -> Result<Document> {
    let mut with = doc.clone();
    set_computed(&mut with, computed, &Scope::with_bindings(doc, vars))?;
    Ok(with)
}

/// One document through `$replaceRoot` (see [`project_doc`]).
pub(crate) fn replace_root_doc(
    expr: &Expr,
    doc: &Document,
    vars: &[Binding<'_>],
) -> Result<Document> {
    match expr.eval_in(&Scope::with_bindings(doc, vars))? {
        Bson::Document(new_root) => Ok(new_root),
        // Refused rather than wrapped: a root that is not a
        // document would have to be given a field name this stage
        // has no business inventing.
        other => Err(Error::InvalidQuery(format!(
            "$replaceRoot needs a document, found {}",
            type_name(&other)
        ))),
    }
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
    set_computed_within(target, computed, source, crate::expr::MAX_ARRAY_BYTES)
}

/// [`set_computed`] with the document it writes held to `max` bytes. A computed
/// value can hold a copy of the document it is added to (`{a: "$$ROOT"}`), so a
/// stage that did only that, repeated, would double the document each time.
///
/// The size is counted before a value is stored: from the document's own size,
/// less the value a field replaces, plus the new one, and a field that would take
/// it over the limit is refused with nothing stored. A value that is the whole
/// input (`$$ROOT`, `$$CURRENT`) is counted from the input, before the copy is
/// made. A name that is not a plain top-level one may add nested documents whose
/// framing is not known until it is written; when the count cannot rule the
/// write out nor in, the document is measured after it.
fn set_computed_within(
    target: &mut Document,
    computed: &[(String, Expr)],
    source: &Scope<'_>,
    max: usize,
) -> Result<()> {
    if computed.is_empty() {
        return Ok(());
    }
    let what = "the document the stage builds";
    let mut size = crate::expr::document_bytes(target);
    for (name, expr) in computed {
        // The input as a whole is counted without copying it.
        let known = match expr {
            Expr::Var { name: var, path: None } if var == "ROOT" || var == "CURRENT" => {
                Some(crate::expr::document_bytes(source.root()))
            }
            _ => None,
        };
        let replaced = replaced_bytes(target, name);
        if let Some(payload) = known {
            admit(what, size, name, replaced, payload, max)?;
        }
        // `$$REMOVE` leaves the field out: not written, and removed if the stage
        // would otherwise carry it (ADR-253).
        match expr.eval_field(source)? {
            Some(value) => {
                let payload = known.unwrap_or_else(|| crate::expr::bson_bytes(&value));
                let exact = admit(what, size, name, replaced, payload, max)?;
                path::set(target, name, value)
                    .map_err(|e| Error::InvalidQuery(format!("cannot set {name:?}: {e}")))?;
                size = match exact {
                    Some(after) => after,
                    None => {
                        let after = crate::expr::document_bytes(target);
                        crate::expr::check_document_bytes(what, after, max)?;
                        after
                    }
                };
            }
            None => {
                if !path::unset(target, name) && crossing_array(target, name).is_some() {
                    // A path the write of a value would refuse is refused for the
                    // removal too, with the same error: a field named through an array
                    // by a name, not a position, is not a place a field can be left out
                    // of. A numeric position leaves a null in the array, as `$unset` does.
                    let mut probe = target.clone();
                    if let Err(e) = path::set(&mut probe, name, Bson::Null) {
                        return Err(Error::InvalidQuery(format!("cannot set {name:?}: {e}")));
                    }
                }
                size = crate::expr::document_bytes(target);
            }
        }
    }
    Ok(())
}

/// The payload of the value at `name` in `target`, if the path runs through
/// documents to it. A path through an array is not followed: nothing is known.
fn replaced_bytes(target: &Document, name: &str) -> Option<usize> {
    let mut segments = name.split('.');
    let mut current = target.get(segments.next()?)?;
    for segment in segments {
        current = match current {
            Bson::Document(d) => d.get(segment)?,
            _ => return None,
        };
    }
    Some(crate::expr::bson_bytes(current))
}

/// Whether a value of `payload` bytes may be written at `name` into a document of
/// `size` bytes, within `max`; refused, with nothing stored, when even the
/// smallest the document can become is over it. `Some(size)` is the size the
/// document will be when that is known exactly (the field is replaced in place),
/// `None` when the write may add nested documents and the document must be
/// measured after it.
fn admit(
    what: &'static str,
    size: usize,
    name: &str,
    replaced: Option<usize>,
    payload: usize,
    max: usize,
) -> Result<Option<usize>> {
    let leaf = name.rsplit('.').next().map_or(name.len(), str::len);
    match replaced {
        Some(old) => {
            let after = size.saturating_sub(old).saturating_add(payload);
            crate::expr::check_document_bytes(what, after, max)?;
            Ok(Some(after))
        }
        None => {
            let least = size.saturating_add(2 + leaf).saturating_add(payload);
            crate::expr::check_document_bytes(what, least, max)?;
            // Every segment is a field, and each one but the last holds a document of
            // five bytes of framing: the most the write can add.
            let segments = name.split('.').count();
            let framing: usize =
                name.split('.').map(|s| 2 + s.len()).sum::<usize>() + 5 * (segments - 1);
            let most = size.saturating_add(framing).saturating_add(payload);
            Ok((most <= max).then_some(most))
        }
    }
}

/// The `as` array of a join (`$lookup`, `$graphLookup`), held to 16 MiB of BSON
/// like every array an expression builds, and counted as it grows: a document is
/// counted before it is copied in, and the join is refused with the `413`
/// `Error::ValueTooLarge` at the document that takes the array over the limit.
/// The documents it joins are counted by the pipeline's document ceiling; this
/// bounds their bytes.
#[derive(Debug)]
pub struct JoinedArray {
    budget: crate::expr::ArrayBudget,
    items: Vec<Bson>,
}

impl JoinedArray {
    /// The array `what` builds ("the array $lookup builds"), within 16 MiB.
    pub fn new(what: &'static str) -> Self {
        Self::within(what, crate::expr::MAX_ARRAY_BYTES)
    }

    /// [`Self::new`] within `max` bytes.
    pub fn within(what: &'static str, max: usize) -> Self {
        Self { budget: crate::expr::ArrayBudget::new(what, max), items: Vec::new() }
    }

    /// Join a copy of `doc`, with `Int64` `depth` written at `depth_field` when
    /// given. Counted before the copy is made.
    pub fn push_copy(&mut self, doc: &Document, depth_field: Option<(&str, i64)>) -> Result<()> {
        let mut payload = crate::expr::document_bytes(doc);
        if let Some((field, _)) = depth_field {
            // Counted as a new field: a field of that name already there is
            // replaced, so this can only be high.
            payload = payload.saturating_add(2 + field.len() + 8);
        }
        let held =
            crate::budget::held_bytes(doc).saturating_add(depth_field.map_or(0, |(f, _)| {
                crate::budget::held_element(f, &Bson::Int64(0))
                    + crate::budget::held_frame_growth(doc.len(), 1)
            }));
        self.budget.add_payload_held(payload, held)?;
        let mut copy = doc.clone();
        if let Some((field, depth)) = depth_field {
            copy.insert(field, Bson::Int64(depth));
        }
        self.items.push(Bson::Document(copy));
        Ok(())
    }

    /// Join `docs`, which are already built, by moving them in.
    pub fn push_all(&mut self, docs: Vec<Document>) -> Result<()> {
        for doc in docs {
            self.budget.add_payload_held(
                crate::expr::document_bytes(&doc),
                crate::budget::held_bytes(&doc),
            )?;
            self.items.push(Bson::Document(doc));
        }
        Ok(())
    }

    /// The size of the array as BSON, as counted so far.
    pub fn bytes(&self) -> usize {
        self.budget.bytes()
    }

    /// The estimate of the memory the array holds parsed (ADR-269): what the request's budget
    /// is charged for it.
    pub fn held_bytes(&self) -> usize {
        self.budget.held()
    }

    /// The array.
    pub fn finish(self) -> Bson {
        Bson::Array(self.items)
    }
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

/// What `$unwind` does, whatever runs it.
pub(crate) struct UnwindSpec<'a> {
    pub(crate) field: &'a str,
    pub(crate) preserve: bool,
    pub(crate) include_index: Option<&'a str>,
    /// Held to the limit as a whole, as the other stages that add fields are.
    pub(crate) max_document_bytes: usize,
}

impl UnwindSpec<'_> {
    /// Write the index field alongside the unwound value, or `null` on a document that
    /// passed through without being fanned out — the reference's rule, and the only sane reading
    /// of "which element produced this row" for a row that was not produced by one.
    fn set_index(&self, copy: &mut Document, index: Option<i64>) -> Result<()> {
        let Some(name) = self.include_index else { return Ok(()) };
        let value = index.map_or(Bson::Null, Bson::Int64);
        path::set(copy, name, value)
            .map_err(|e| Error::InvalidQuery(format!("cannot set `{name}`: {e}")))?;
        // The element in place of the array is no larger than the input, but the index field
        // is added to it: a document at the limit goes over it by the field. Held to the
        // limit as a whole, as the other stages that add fields are.
        check_built_document(copy, self.max_document_bytes)
    }
}

/// The documents one document of `$unwind`'s input becomes, handed out one at a time so that
/// a caller that needs no more can stop in the middle of an array (ADR-270). [`apply`] runs
/// it to its end for each document; a chain of sinks stops at a `Done`.
pub(crate) struct Fan {
    /// The input document, kept only while an array is being handed out: each element is a
    /// copy of it with the element in the array's place, which is `null` here, the array
    /// itself being in `elements`. A document that is not fanned out is moved into `single`
    /// and nothing is kept.
    doc: Document,
    /// The elements of the array, still to come, with the position of the next.
    elements: Option<(std::vec::IntoIter<Bson>, i64)>,
    /// A document that is not fanned out: what it becomes, and whether the stage built it
    /// (it is then charged to the request's budget; the input that is passed on unchanged
    /// was charged as an input).
    single: Option<(Document, bool)>,
}

impl Fan {
    /// Look at `doc`, and decide what it becomes.
    pub(crate) fn start(spec: &UnwindSpec<'_>, mut doc: Document) -> Result<Fan> {
        let field = spec.field;
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
                // The array moves out of the document, so that it is held once and each
                // element's copy of the document does not carry it.
                let items = items.clone();
                path::set(&mut doc, field, Bson::Null)
                    .map_err(|e| Error::InvalidQuery(format!("cannot set `{field}`: {e}")))?;
                Ok(Fan { doc, elements: Some((items.into_iter(), 0)), single: None })
            }
            // A missing field, an explicit null, or an empty array: the reference
            // drops the document unless asked to keep it.
            Some(Bson::Array(_)) | Some(Bson::Null) | None => {
                let single = if spec.preserve {
                    path::unset(&mut doc, field);
                    spec.set_index(&mut doc, None)?;
                    Some((doc, true))
                } else {
                    None
                };
                Ok(Fan { doc: Document::new(), elements: None, single })
            }
            // A non-array value unwinds to itself, which is what the reference does
            // and saves callers a `$type` check for a field that is sometimes
            // scalar and sometimes an array.
            Some(_) => {
                spec.set_index(&mut doc, None)?;
                Ok(Fan { doc: Document::new(), elements: None, single: Some((doc, false)) })
            }
        }
    }

    /// What the fan holds while it hands its documents out, as the budget counts it (ADR-269):
    /// the document it keeps, if it keeps one, and the elements still to come. It shrinks as
    /// the elements are handed out, and is nothing once the last has taken the document. A
    /// document that is not fanned out is handed on whole and held by nobody.
    pub(crate) fn held_bytes(&self) -> usize {
        let kept = if self.doc.is_empty() { 0 } else { crate::budget::held_bytes(&self.doc) };
        let rest = self.elements.as_ref().map_or(0, |(rest, _)| {
            rest.as_slice()
                .iter()
                .fold(0usize, |sum, item| sum.saturating_add(crate::budget::held_bytes_bson(item)))
        });
        kept.saturating_add(rest)
    }

    /// Whether the document is expanded from an array, as against dropped or passed on.
    pub(crate) fn is_array(&self) -> bool {
        self.elements.is_some()
    }

    /// The next document, and whether the stage built it, or `None` when there are no more.
    pub(crate) fn next(
        &mut self,
        spec: &UnwindSpec<'_>,
        ticker: &mut Ticker,
    ) -> Result<Option<(Document, bool)>> {
        if let Some((elements, index)) = &mut self.elements {
            let Some(item) = elements.next() else { return Ok(None) };
            // A document holding a huge array is many rows from one iteration
            // of the loop that drives this.
            ticker.tick()?;
            // The last element takes the document itself, so that a fan over a single-element
            // array, and the last of any, keeps nothing once its element is handed out.
            let mut copy = if elements.as_slice().is_empty() {
                std::mem::take(&mut self.doc)
            } else {
                self.doc.clone()
            };
            path::set(&mut copy, spec.field, item)
                .map_err(|e| Error::InvalidQuery(format!("cannot set `{}`: {e}", spec.field)))?;
            spec.set_index(&mut copy, Some(*index))?;
            *index += 1;
            return Ok(Some((copy, true)));
        }
        Ok(self.single.take())
    }
}

fn unwind(
    input: Vec<Document>,
    field: &str,
    preserve: bool,
    include_index: Option<&str>,
    limits: &Limits,
    held: &mut Lease,
) -> Result<Vec<Document>> {
    let spec = &UnwindSpec {
        field,
        preserve,
        include_index,
        max_document_bytes: limits.max_document_bytes,
    };
    let mut out = Vec::with_capacity(input.len());
    let mut ticker = Ticker::new();
    for doc in input {
        ticker.tick()?;
        let mut fan = Fan::start(spec, doc)?;
        while let Some((copy, built)) = fan.next(spec, &mut ticker)? {
            if built {
                held.charge_doc(&copy)?;
            }
            out.push(copy);
        }
        if fan.is_array() {
            // Checked after each document's fan-out as well as at the end of the stage: a
            // handful of documents each holding a huge array can exceed the cap long
            // before the outer loop ends, and the point of the cap is to
            // stop allocating, not to report afterwards.
            check_limit("$unwind", out.len(), limits)?;
        }
    }
    Ok(out)
}

/// Bucket state while grouping.
struct Bucket {
    key: Bson,
    values: Vec<AccState>,
}

pub(crate) enum AccState {
    /// Exact while every input is integral — see [`Total`]. The previous
    /// `(f64, bool)` accumulated in a double and cast back, so a total above
    /// 2^53 was silently wrong despite the flag saying it was an integer.
    Sum(Total),
    /// The mean: a double while no value is a `Decimal128`, an exact sum divided
    /// by the count once one is (ADR-252).
    Avg(Mean),
    MinMax(Option<Bson>),
    First(Option<Bson>),
    /// The latest document's value, or why it has none. Only the last
    /// document's value is the answer, so an earlier one that cannot be
    /// evaluated is replaced by the next, and fails the request only if no
    /// later document comes (ADR-211).
    Last(Option<Result<Bson>>),
    /// The values, and the size of the array they make, counted against the 16 MiB bound
    /// of one value (ADR-267) and read as the bytes the group holds (ADR-269).
    Push(Vec<Bson>, crate::expr::ArrayBudget),
    /// The members in first-seen order, and the [`group_key`] of each, which
    /// is what makes two values one member (ADR-186); the size of the array, and the
    /// bytes of the keys.
    AddToSet(Vec<Bson>, std::collections::HashSet<Vec<u8>>, crate::expr::ArrayBudget, usize),
    /// The fields merged so far, a later document's value winning, and their size.
    MergeObjects(Document, crate::expr::DocBudget),
    Ext(crate::accumulators::State),
}

/// The estimate of the memory a value takes in a group's state (ADR-269): its slot and what
/// it holds parsed.
fn state_bytes(value: &Bson) -> usize {
    crate::budget::held_bytes_bson(value)
}

impl AccState {
    /// The estimate of the memory this state holds now, for the request's budget (ADR-269).
    /// Constant time for the states that grow with the group (`$push`, `$addToSet`,
    /// `$mergeObjects`, the ranked accumulators); the others hold one value.
    pub(crate) fn held(&self) -> usize {
        match self {
            AccState::Sum(_) | AccState::Avg(_) => 0,
            AccState::MinMax(v) | AccState::First(v) => v.as_ref().map_or(0, state_bytes),
            AccState::Last(v) => match v {
                Some(Ok(v)) => state_bytes(v),
                _ => 0,
            },
            // The estimate the array's budget keeps beside the size the value bound
            // (ADR-267) holds it to; the lease reads it, so the 16 MiB bound is not touched.
            AccState::Push(_, budget) => budget.held(),
            AccState::AddToSet(_, _, budget, keys) => budget.held().saturating_add(*keys),
            AccState::MergeObjects(_, budget) => budget.held(),
            AccState::Ext(state) => state.held(),
        }
    }
}

/// The estimate of what a bucket takes before any accumulator has seen a document: the
/// output document, its `_id` (and the key the bucket keeps) and one element for each
/// accumulator.
fn bucket_base_bytes(key: &Bson, fields: &[(String, Accumulator)]) -> usize {
    fields.iter().fold(
        std::mem::size_of::<Document>()
            .saturating_add(crate::budget::held_frame_growth(0, fields.len() + 1))
            .saturating_add(crate::budget::held_element("_id", key))
            .saturating_add(crate::budget::held_bytes_bson(key)),
        |sum, (name, _)| sum.saturating_add(crate::budget::held_element(name, &Bson::Null)),
    )
}

/// The name of a group's output document in a refusal for its size.
fn group_document_name(stage: &str) -> &'static str {
    match stage {
        "$bucket" => "the document $bucket builds",
        "$bucketAuto" => "the document $bucketAuto builds",
        "$sortByCount" => "the document $sortByCount builds",
        _ => "the document $group builds",
    }
}

/// Refuse a group's output document when it is over `max` bytes as a whole. Each
/// accumulator's array or document is held to 16 MiB as a value (ADR-267), and two of them
/// together make a document that no collection could hold and no answer could carry; the
/// sum is checked as the document is built (ADR-269).
fn check_group_document(stage: &str, doc: &Document, max: usize) -> Result<()> {
    crate::expr::check_document_bytes(
        group_document_name(stage),
        crate::expr::document_bytes(doc),
        max,
    )
}

/// Refuse a document a stage built by adding fields to its input when it is over `max`
/// bytes as a whole, with the refusal `$addFields` gives (`the document the stage builds`).
/// `$fill`, `$setWindowFields`, `$lookup` and `$graphLookup` add values that are each held
/// to 16 MiB on their own, and two of them in one document make one that no collection could
/// hold and no answer could carry; the document is checked as it is completed.
pub fn check_built_document(doc: &Document, max: usize) -> Result<()> {
    crate::expr::check_document_bytes(
        "the document the stage builds",
        crate::expr::document_bytes(doc),
        max,
    )
}

/// Run a grouping stage over a whole input: the documents through a [`GroupState`] one after
/// the other, then its answer. The state is charged to `held` as it grows (ADR-269).
fn group_all(
    stage: &Stage,
    input: &[Document],
    limits: &Limits,
    vars: &[Binding<'_>],
    held: &mut Lease,
) -> Result<Vec<Document>> {
    let mut state = GroupState::new(stage, limits, vars).expect("the stage groups");
    for doc in input {
        state.add(doc, held)?;
    }
    state.finish()
}

/// How a grouping stage reads the bucket a document belongs to.
enum GroupKind<'a> {
    /// `$group`: the value of `_id`.
    Group(&'a Expr),
    /// `$sortByCount`: the value of the expression, counted, and the groups sorted by count.
    SortByCount(&'a Expr),
    /// `$bucket`: the range of `boundaries` the value of `group_by` falls in, `[b_i, b_i+1)`,
    /// whose key is `b_i` as written; `default` if the value is outside every range or is not
    /// of the boundaries' kind; and a document with neither is **refused**, naming the value.
    /// Only buckets that received a document are answered, in boundary order and the
    /// `default` bucket last.
    Bucket {
        group_by: &'a Expr,
        boundaries: &'a [Bson],
        default: Option<&'a Bson>,
        kind: Option<BoundaryKind>,
    },
}

/// What `$group`, `$bucket` and `$sortByCount` hold while they take their input one document
/// at a time (ADR-270): the buckets and what each accumulator keeps, and not the documents.
/// [`apply`] runs a whole input through one, and the chain of sinks feeds it as the source
/// reads, so there is one implementation of each stage.
///
/// Keyed by the *encoded* key rather than by `Bson`, because `Bson` is not `Hash` and because
/// two numerically equal values of different types must land in the same bucket, the same
/// rule indexes use.
pub struct GroupState<'a> {
    stage: &'static str,
    kind: GroupKind<'a>,
    fields: std::borrow::Cow<'a, [(String, Accumulator)]>,
    limits: &'a Limits,
    vars: &'a [Binding<'a>],
    order: Vec<Bucket>,
    index: std::collections::HashMap<Vec<u8>, usize>,
    ticker: Ticker,
}

impl<'a> GroupState<'a> {
    /// The state of `stage`, or `None` if it is not a stage that groups.
    pub fn new(stage: &'a Stage, limits: &'a Limits, vars: &'a [Binding<'a>]) -> Option<Self> {
        let (kind, fields) = match stage {
            Stage::Group { id, fields } => {
                (GroupKind::Group(id), std::borrow::Cow::Borrowed(&fields[..]))
            }
            Stage::SortByCount(expr) => {
                // The count is `$group`'s own `{$sum: 1}`: an int, widening to a long past
                // 2^31-1, as the reference's is (ADR-268).
                let count = ("count".to_string(), Accumulator::Sum(Expr::Literal(Bson::Int32(1))));
                (GroupKind::SortByCount(expr), std::borrow::Cow::Owned(vec![count]))
            }
            Stage::Bucket { group_by, boundaries, default, fields } => (
                GroupKind::Bucket {
                    group_by,
                    boundaries,
                    default: default.as_ref(),
                    kind: BoundaryKind::of(&boundaries[0]),
                },
                std::borrow::Cow::Borrowed(&fields[..]),
            ),
            _ => return None,
        };
        Some(GroupState {
            stage: stage.name(),
            kind,
            fields,
            limits,
            vars,
            order: Vec::new(),
            index: std::collections::HashMap::new(),
            ticker: Ticker::new(),
        })
    }

    /// The key of the bucket `scope`'s document belongs to.
    fn key(&self, scope: &Scope<'_>) -> Result<Bson> {
        match &self.kind {
            GroupKind::Group(expr) | GroupKind::SortByCount(expr) => expr.eval_in(scope),
            GroupKind::Bucket { group_by, boundaries, default, kind } => {
                let value = group_by.eval_in(scope)?;
                let inside = (BoundaryKind::of(&value) == *kind && !is_nan(&value))
                    .then(|| {
                        boundaries
                            .partition_point(|b| kimmy_core::cmp::canonical_cmp(b, &value).is_le())
                    })
                    .filter(|&above| above > 0 && above < boundaries.len());
                match (inside, default) {
                    (Some(above), _) => Ok(boundaries[above - 1].clone()),
                    (None, Some(default)) => Ok((*default).clone()),
                    (None, None) => Err(Error::InvalidQuery(format!(
                        "$bucket: {} falls in no bucket and there is no `default`; add a \
                         `default`, or a boundary below and above it",
                        describe_value(&value)
                    ))),
                }
            }
        }
    }

    /// One more document. What the buckets hold is charged to `held` as it grows
    /// (ADR-269): a bucket when it is made, and the values each accumulator keeps as they are
    /// taken, so a `$push` of large values is refused at the document that passes the limit.
    /// One bucket per distinct value, so a high-cardinality key is exactly the shape that
    /// exhausts memory: the buckets are held to the pipeline's document limit.
    pub fn add(&mut self, doc: &Document, held: &mut Lease) -> Result<()> {
        self.ticker.tick()?;
        let scope = Scope::with_bindings(doc, self.vars);
        let key = self.key(&scope)?;
        let encoded = group_key(&key);
        let slot = match self.index.get(&encoded) {
            Some(&slot) => slot,
            None => {
                let slot = self.order.len();
                // The bucket, its key twice over (the bucket and the index).
                held.charge(
                    bucket_base_bytes(&key, &self.fields)
                        .saturating_add(crate::budget::held_set_key(encoded.len())),
                )?;
                self.order.push(Bucket {
                    key: key.clone(),
                    values: self.fields.iter().map(init).collect(),
                });
                self.index.insert(encoded, slot);
                check_limit(self.stage, self.order.len(), self.limits)?;
                slot
            }
        };
        for (state, (_, acc)) in self.order[slot].values.iter_mut().zip(self.fields.iter()) {
            let before = state.held();
            accumulate(state, acc, &scope)?;
            held.resize(before, state.held())?;
        }
        Ok(())
    }

    /// The input is complete: the groups, in the order their keys were first seen, but for
    /// `$bucket`'s, which are in boundary order and the `default` last, and `$sortByCount`'s,
    /// which are sorted by count, largest first, the sort stable so that groups with the same
    /// count stay in first-seen order (the reference leaves it unspecified).
    pub fn finish(self) -> Result<Vec<Document>> {
        // Not ticked: one cheap step per bucket, and the buckets are no more than the
        // documents that were.
        let mut out = self
            .order
            .into_iter()
            .map(|bucket| {
                let mut out = Document::new();
                out.insert("_id", bucket.key);
                for (state, (name, _)) in bucket.values.into_iter().zip(self.fields.iter()) {
                    out.insert(name.clone(), finish(state)?);
                }
                check_group_document(self.stage, &out, self.limits.max_document_bytes)?;
                Ok(out)
            })
            .collect::<Result<Vec<_>>>()?;
        match self.kind {
            GroupKind::Group(_) => Ok(out),
            GroupKind::SortByCount(_) => shape::sort_until_deadline(
                &[SortKey { path: "count".into(), descending: true }],
                out,
            ),
            GroupKind::Bucket { default, .. } => {
                let default_key = default.map(group_key);
                out.sort_by(|a, b| {
                    let (ka, kb) =
                        (a.get("_id").unwrap_or(&Bson::Null), b.get("_id").unwrap_or(&Bson::Null));
                    let is_default =
                        |k: &Bson| default_key.as_ref().is_some_and(|d| *d == group_key(k));
                    is_default(ka)
                        .cmp(&is_default(kb))
                        .then_with(|| kimmy_core::cmp::canonical_cmp(ka, kb))
                });
                Ok(out)
            }
        }
    }
}

/// A value in an error message: its text if it is short, its type if not.
fn describe_value(value: &Bson) -> String {
    let text = match value {
        Bson::Null => return "null or a missing value".to_string(),
        Bson::String(s) => format!("{s:?}"),
        Bson::Int32(_)
        | Bson::Int64(_)
        | Bson::Double(_)
        | Bson::Boolean(_)
        | Bson::Decimal128(_)
        | Bson::DateTime(_)
        | Bson::ObjectId(_) => value.to_string(),
        other => return format!("a value of {}", type_name(other)),
    };
    if text.chars().count() > 60 {
        format!("a value of {}", type_name(value))
    } else {
        format!("the value {text}")
    }
}

/// Run a `$bucketAuto`: the documents sorted by their `groupBy` value (the
/// order `$sort` uses, stable) and cut into at most `buckets` runs.
///
/// With `r` documents and `b` buckets left, the next bucket takes `ceil(r / b)`
/// documents and then every further document holding the value of its last (a value
/// is never split across two buckets), so buckets are as even as the values allow
/// and there are fewer than asked for when values repeat. A bucket is answered as
/// `_id: {min, max}`: its first value, and the next bucket's first value (the
/// last bucket's own largest). Nothing is answered for an empty input.
fn bucket_auto(
    input: Vec<Document>,
    group_by: &Expr,
    buckets: usize,
    fields: &[(String, Accumulator)],
    limits: &Limits,
    vars: &[Binding<'_>],
    held: &mut Lease,
) -> Result<Vec<Document>> {
    let mut keyed: Vec<(Bson, Document)> = Vec::with_capacity(input.len());
    let mut ticker = Ticker::new();
    for doc in input {
        ticker.tick()?;
        let key = group_by.eval_in(&Scope::with_bindings(&doc, vars))?;
        // The keys are a second column beside the documents the stage was given.
        held.charge(state_bytes(&key))?;
        keyed.push((key, doc));
    }
    keyed.sort_by(|a, b| kimmy_core::cmp::canonical_cmp(&a.0, &b.0));
    let same = |a: &Bson, b: &Bson| kimmy_core::cmp::canonical_cmp(a, b).is_eq();

    let mut out = Vec::new();
    let (mut start, mut left) = (0usize, buckets);
    while start < keyed.len() {
        let remaining = keyed.len() - start;
        let mut end = if left <= 1 { keyed.len() } else { start + remaining.div_ceil(left) };
        while end < keyed.len() && same(&keyed[end - 1].0, &keyed[end].0) {
            end += 1;
        }
        let mut states: Vec<AccState> = fields.iter().map(init).collect();
        let first_key = &keyed[start].0;
        held.charge(bucket_base_bytes(first_key, fields).saturating_mul(2))?;
        for (_, doc) in &keyed[start..end] {
            ticker.tick()?;
            let scope = Scope::with_bindings(doc, vars);
            for (state, (_, acc)) in states.iter_mut().zip(fields) {
                let before = state.held();
                accumulate(state, acc, &scope)?;
                held.resize(before, state.held())?;
            }
        }
        let max = if end < keyed.len() { keyed[end].0.clone() } else { keyed[end - 1].0.clone() };
        let mut answer = Document::new();
        answer.insert("_id", bson::doc! { "min": keyed[start].0.clone(), "max": max });
        for (state, (name, _)) in states.into_iter().zip(fields) {
            answer.insert(name.clone(), finish(state)?);
        }
        check_group_document("$bucketAuto", &answer, limits.max_document_bytes)?;
        out.push(answer);
        // Counted as they are emitted: `buckets` asked for is not the buckets made.
        check_limit("$bucketAuto", out.len(), limits)?;
        start = end;
        left = left.saturating_sub(1).max(1);
    }
    Ok(out)
}

pub(crate) fn init(field: &(String, Accumulator)) -> AccState {
    init_within(field, crate::expr::MAX_ARRAY_BYTES)
}

/// [`init`] with the arrays and documents an accumulator builds held to `max`
/// bytes, counted as they grow.
fn init_within((_, acc): &(String, Accumulator), max: usize) -> AccState {
    match acc {
        Accumulator::Sum(_) => AccState::Sum(Total::default()),
        Accumulator::Avg(_) => AccState::Avg(Mean::default()),
        Accumulator::Min(_) | Accumulator::Max(_) => AccState::MinMax(None),
        Accumulator::First(_) => AccState::First(None),
        Accumulator::Last(_) => AccState::Last(None),
        Accumulator::Push(_) => {
            AccState::Push(Vec::new(), crate::expr::ArrayBudget::new("the array $push builds", max))
        }
        Accumulator::AddToSet(_) => AccState::AddToSet(
            Vec::new(),
            Default::default(),
            crate::expr::ArrayBudget::new("the array $addToSet builds", max),
            0,
        ),
        Accumulator::MergeObjects(_) => {
            AccState::MergeObjects(Document::new(), crate::expr::merge_budget(max))
        }
        Accumulator::Ext(spec) => AccState::Ext(spec.init_within(max)),
    }
}

/// Feed one document to one accumulator.
pub(crate) fn accumulate(state: &mut AccState, acc: &Accumulator, scope: &Scope<'_>) -> Result<()> {
    // One tick for each accumulator step (ADR-266): the argument can be an expression of any
    // cost, and the stage's own ticker counts documents, not steps.
    deadline::tick()?;
    match (state, acc) {
        (AccState::Ext(state), Accumulator::Ext(spec)) => state.accumulate(spec, scope)?,
        (AccState::Sum(total), Accumulator::Sum(e)) => {
            total.add(&e.eval_in(scope)?);
        }
        (AccState::Avg(mean), Accumulator::Avg(e)) => mean.add(&e.eval_in(scope)?),
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
        // A missing value is skipped, as the reference skips it; an explicit `null` is
        // a value (ADR-264). A value is counted before it is kept: the group refuses at
        // the value that takes its array over the limit.
        (AccState::Push(items, budget), Accumulator::Push(e)) => {
            if let Some(v) = e.eval_present(scope)? {
                budget.add_tracked(&v)?;
                items.push(v);
            }
        }
        (AccState::AddToSet(items, keys, budget, key_bytes), Accumulator::AddToSet(e)) => {
            if let Some(v) = e.eval_present(scope)? {
                // A member is identified the way `$group` identifies a bucket, and
                // the first occurrence stays (ADR-186).
                let key = group_key(&v);
                if !keys.contains(&key) {
                    budget.add_tracked(&v)?;
                    *key_bytes = key_bytes.saturating_add(crate::budget::held_set_key(key.len()));
                    keys.insert(key);
                    items.push(v);
                }
            }
        }
        (AccState::MergeObjects(merged, budget), Accumulator::MergeObjects(e)) => {
            crate::expr::merge_into(merged, budget, &e.eval_in(scope)?)?;
        }
        // Unreachable: `init` pairs each accumulator with its own state.
        _ => {}
    }
    Ok(())
}

pub(crate) fn finish(state: AccState) -> Result<Bson> {
    Ok(match state {
        // An integer sum stays an integer, exactly rather than approximately:
        // `Total` accumulates in i64 and only widens when an operand is a
        // double or the addition overflows. Same reasoning as ADR-002.
        AccState::Sum(total) => total.to_bson(),
        AccState::Avg(mean) => mean.to_bson(),
        AccState::MinMax(v) | AccState::First(v) => v.unwrap_or(Bson::Null),
        AccState::Last(v) => v.transpose()?.unwrap_or(Bson::Null),
        AccState::Push(items, _) | AccState::AddToSet(items, ..) => Bson::Array(items),
        AccState::MergeObjects(merged, _) => Bson::Document(merged),
        AccState::Ext(state) => state.finish(),
    })
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
mod deadline_tests;

#[cfg(test)]
mod reorder_tests;

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

    /// `$addFields: {a: "$$ROOT"}` copies the document into itself, so the stage
    /// repeated doubles it. The document a stage writes is held to the limit, here
    /// a few hundred bytes.
    #[test]
    fn computed_fields_are_held_to_the_document_limit() {
        let copy = || Expr::parse(&Bson::String("$$ROOT".into())).unwrap();
        let stage = vec![("a".to_string(), copy()), ("b".to_string(), copy())];
        let mut doc = doc! {"x": "y"};
        let mut steps = 0;
        let refused = loop {
            let mut with = doc.clone();
            match set_computed_within(&mut with, &stage, &Scope::new(&doc), 512) {
                Ok(()) => {
                    doc = with;
                    steps += 1;
                }
                Err(e) => break (e, with),
            }
        };
        assert!((3..9).contains(&steps), "{steps}");
        assert!(matches!(refused.0, Error::ValueTooLarge { max: 512, .. }), "{:?}", refused.0);
        assert!(refused.0.to_string().contains("16 MiB document limit"));
        assert!(crate::expr::bson_bytes(&Bson::Document(doc.clone())) <= 512);
    }

    /// The stages read the real limit: a copy of a nine-megabyte document in itself,
    /// and nine megabytes merged beside nine, are 413s, not documents.
    #[test]
    fn the_stages_write_no_document_over_16_mib() {
        let big = "x".repeat(9 << 20);
        let input = vec![doc! {"_id": 1, "s": big.clone()}, doc! {"_id": 2, "t": big}];
        for stage in [
            doc! {"$addFields": {"copy": "$$ROOT"}},
            doc! {"$project": {"a": "$$ROOT", "b": "$$ROOT"}},
            doc! {"$group": {"_id": null, "all": {"$mergeObjects": "$$ROOT"}}},
        ] {
            let err = run(vec![stage.clone()], input.clone()).unwrap_err();
            assert!(
                matches!(err, Error::ValueTooLarge { max, .. } if max == 16 << 20),
                "{stage}: {err:?}"
            );
        }
        // One copy is within the limit, and so is a field beside the document.
        let one = run(vec![doc! {"$project": {"copy": "$$ROOT"}}], input.clone());
        assert!(one.is_ok(), "{one:?}");
        let beside = run(vec![doc! {"$addFields": {"n": 1}}], input);
        assert!(beside.is_ok(), "{beside:?}");
    }

    /// `$push` and `$addToSet` count the array as it grows and refuse at the value
    /// that takes it over the limit; an `$addToSet` member already there is not
    /// counted again.
    #[test]
    fn push_and_add_to_set_are_held_to_the_limit_as_they_grow() {
        let fields = [
            ("p".to_string(), Accumulator::Push(Expr::parse(&Bson::String("$v".into())).unwrap())),
            (
                "s".to_string(),
                Accumulator::AddToSet(Expr::parse(&Bson::String("$v".into())).unwrap()),
            ),
        ];
        // Room for three strings of ten bytes (5, then 2 + 1 + 15 each... counted
        // as the array encodes: the index digits are one each below ten).
        let max = crate::expr::array_bytes(3, 3 * 15);
        for field in &fields {
            let mut state = init_within(field, max);
            for i in 0..3 {
                let row = doc! {"v": format!("{i:010}")};
                accumulate(&mut state, &field.1, &Scope::new(&row)).unwrap();
            }
            let row = doc! {"v": format!("{:010}", 3)};
            let err = accumulate(&mut state, &field.1, &Scope::new(&row)).unwrap_err();
            assert!(
                matches!(err, Error::ValueTooLarge { bytes, max: m, .. }
                    if bytes == crate::expr::array_bytes(4, 4 * 15) && m == max),
                "{err:?}"
            );
        }
        // A member that is already in the set costs nothing.
        let mut state = init_within(&fields[1], max);
        for i in 0..100 {
            let row = doc! {"v": format!("{:010}", i % 3)};
            accumulate(&mut state, &fields[1].1, &Scope::new(&row)).unwrap();
        }
        let Bson::Array(members) = finish(state).unwrap() else { panic!("an array") };
        assert_eq!(members.len(), 3);
    }

    /// The `as` array of a join is counted before each copy.
    #[test]
    fn a_joined_array_is_counted_before_it_is_copied() {
        let a = doc! {"_id": 1, "t": "x".repeat(100)};
        let one = crate::expr::array_bytes(1, crate::expr::document_bytes(&a));
        let mut joined = JoinedArray::within("the array $lookup builds", one);
        joined.push_copy(&a, None).unwrap();
        let err = joined.push_copy(&a, None).unwrap_err();
        assert!(
            matches!(&err, Error::ValueTooLarge { max, what, .. }
                if *max == one && *what == "the array $lookup builds"),
            "{err:?}"
        );
        // The refused document was not joined.
        assert_eq!(joined.finish(), Bson::Array(vec![Bson::Document(a.clone())]));
        // Exactly the limit is within it; one byte less is not.
        let mut joined = JoinedArray::within("the array $lookup builds", one - 1);
        assert!(joined.push_copy(&a, None).is_err());
        // A depth field is counted, and written as an Int64.
        let with_depth = doc! {"_id": 1, "t": "x".repeat(100), "d": 3i64};
        let size = crate::expr::array_bytes(1, crate::expr::document_bytes(&with_depth));
        let mut joined = JoinedArray::within("the array $graphLookup builds", size);
        joined.push_copy(&a, Some(("d", 3))).unwrap();
        assert_eq!(joined.finish(), Bson::Array(vec![Bson::Document(with_depth)]));
        let mut joined = JoinedArray::within("the array $graphLookup builds", size - 1);
        assert!(joined.push_copy(&a, Some(("d", 3))).is_err());
        // Documents already built are moved in and counted the same way.
        let mut joined = JoinedArray::within(
            "the array $lookup builds",
            crate::expr::array_bytes(2, 2 * crate::expr::document_bytes(&a)),
        );
        joined.push_all(vec![a.clone(), a.clone()]).unwrap();
        assert!(joined.push_all(vec![a]).is_err());
    }

    /// A computed field is refused before it is stored, and `$$ROOT` is counted
    /// without being copied.
    #[test]
    fn a_computed_field_is_counted_before_it_is_stored() {
        let stage = vec![("a".to_string(), Expr::parse(&Bson::String("$big".into())).unwrap())];
        let input = doc! {"big": "x".repeat(300), "b": 1};
        let size = crate::expr::bson_bytes(&Bson::Document(input.clone()));
        // A new field of 300 bytes: refused, and the target is as it was.
        let mut target = input.clone();
        let err =
            set_computed_within(&mut target, &stage, &Scope::new(&input), size + 300).unwrap_err();
        assert!(matches!(err, Error::ValueTooLarge { .. }), "{err:?}");
        assert_eq!(target, input);
        // A field replaced by a smaller value never counts what it replaced.
        let mut target = doc! {"a": "y".repeat(1000), "big": "x".repeat(300), "b": 1};
        let size = crate::expr::bson_bytes(&Bson::Document(target.clone()));
        set_computed_within(&mut target, &stage, &Scope::new(&input), size).unwrap();
        // A dotted name that adds nested documents is measured after the write.
        let dotted =
            vec![("p.q.r".to_string(), Expr::parse(&Bson::String("$big".into())).unwrap())];
        let mut target = input.clone();
        let least = crate::expr::bson_bytes(&Bson::Document(input.clone())) + 2 + 1 + 305;
        let err =
            set_computed_within(&mut target, &dotted, &Scope::new(&input), least + 5).unwrap_err();
        assert!(matches!(err, Error::ValueTooLarge { .. }), "{err:?}");
        let mut target = input.clone();
        set_computed_within(&mut target, &dotted, &Scope::new(&input), 1 << 20).unwrap();
        assert_eq!(target.get_document("p").unwrap().get_document("q").unwrap().len(), 1);
        // `$$ROOT` is refused from the input's own size, before the copy.
        let copy = vec![("c".to_string(), Expr::parse(&Bson::String("$$ROOT".into())).unwrap())];
        let mut target = input.clone();
        let own = crate::expr::bson_bytes(&Bson::Document(input.clone()));
        let err = set_computed_within(&mut target, &copy, &Scope::new(&input), own + own - 1)
            .unwrap_err();
        assert!(matches!(err, Error::ValueTooLarge { bytes, .. } if bytes >= own + own), "{err:?}");
        assert_eq!(target, input);
    }

    /// A field that replaces another is counted for the difference, so a stage that
    /// rewrites a large field in place never meets the limit.
    #[test]
    fn a_computed_field_that_replaces_one_is_counted_once() {
        let big = "z".repeat(300);
        let doc = doc! {"a": big.clone(), "b": 1};
        let size = crate::expr::bson_bytes(&Bson::Document(doc.clone()));
        let same = vec![("a".to_string(), Expr::parse(&Bson::String("$a".into())).unwrap())];
        let mut with = doc.clone();
        set_computed_within(&mut with, &same, &Scope::new(&doc), size).unwrap();
        assert_eq!(with, doc);
        // Two fields that each replace it, the second of the first's size.
        let twice = vec![
            ("a".to_string(), Expr::parse(&Bson::String("$a".into())).unwrap()),
            ("a".to_string(), Expr::parse(&Bson::String("$a".into())).unwrap()),
        ];
        let mut with = doc.clone();
        set_computed_within(&mut with, &twice, &Scope::new(&doc), size).unwrap();
        // A new field beside it is over by exactly its own bytes.
        let beside = vec![("c".to_string(), Expr::parse(&Bson::String("$a".into())).unwrap())];
        let mut with = doc.clone();
        let err =
            set_computed_within(&mut with, &beside, &Scope::new(&doc), size + 300).unwrap_err();
        assert!(matches!(err, Error::ValueTooLarge { max, .. } if max == size + 300), "{err:?}");
        // $$REMOVE and an empty list never refuse, even over the limit.
        let mut with = doc.clone();
        set_computed_within(&mut with, &[], &Scope::new(&doc), 1).unwrap();
        let remove =
            vec![("b".to_string(), Expr::parse(&Bson::String("$$REMOVE".into())).unwrap())];
        set_computed_within(&mut with, &remove, &Scope::new(&doc), size).unwrap();
        assert_eq!(with, doc! {"a": big});
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
        // One Unicode word class repeated 15 times, about 0.8 MiB, fits one
        // filter's 2 MiB, and so do two stages of one; three stages do not,
        // because every stage's patterns are held together.
        let w = kimmy_core::regex::UNICODE_WORD_CLASS;
        let one = |n: usize| doc! {"$match": {"s": {"$regex": format!("{w}{{15}}{n}")}}};
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
    fn a_pipeline_is_held_to_a_thousand_stages_wherever_it_is() {
        // (Not `$skip`, which a window of nothing is no stage: the parse is of the stages as
        // they are counted, and a run of `$skip`s is one.)
        let fillers = |n: usize| vec![doc! {"$redact": "$$KEEP"}; n];
        assert_eq!(MAX_PIPELINE_STAGES, 1_000);
        assert_eq!(parse(&fillers(1_000)).unwrap().len(), 1_000);
        let refused = "a pipeline has 1001 stages, over the limit of 1000 (a stage that expands, \
                       such as $count, counts as two). A $facet branch and a $lookup or \
                       $unionWith sub-pipeline are each counted on their own";
        let err = parse(&fillers(1_001)).unwrap_err();
        assert_eq!(err.to_string(), format!("invalid query: {refused}"));
        // A trailing `$out` is one of the stages counted.
        let mut with_out = fillers(1_000);
        with_out.push(doc! {"$out": "o"});
        assert!(parse(&with_out).unwrap_err().to_string().contains(refused));
        // A `$facet` branch, a `$lookup` sub-pipeline and a `$unionWith` sub-pipeline are each
        // counted on their own, so each takes a thousand and the pipeline around them is
        // not counted with them, and each is refused at one more.
        let bson = |stages: Vec<Document>| -> Vec<Bson> {
            stages.into_iter().map(Bson::Document).collect()
        };
        let wrappers: [fn(Vec<Bson>) -> Document; 3] = [
            |stages| doc! {"$facet": {"a": stages}},
            |stages| doc! {"$lookup": {"from": "items", "pipeline": stages, "as": "items"}},
            |stages| doc! {"$unionWith": {"coll": "items", "pipeline": stages}},
        ];
        for wrap in wrappers {
            let whole = vec![wrap(bson(fillers(1_000))), doc! {"$redact": "$$KEEP"}];
            assert_eq!(parse(&whole).unwrap().len(), 2);
            let err = parse(&[wrap(bson(fillers(1_001)))]).unwrap_err().to_string();
            assert!(err.contains(refused), "{err}");
        }
    }

    #[test]
    fn a_stage_that_expands_counts_as_two_against_the_limit_as_the_reference_counts_it() {
        // Measured on the reference: with 999 stages, `$count`, `$sortByCount`, `$bucket`,
        // `$setWindowFields` and `$densify` are refused and with 998 they are accepted; every
        // other stage probed is accepted with 999.
        // (Not `$skip`, which a window of nothing is no stage: the parse is of the stages as
        // they are counted, and a run of `$skip`s is one.)
        let fillers = |n: usize| vec![doc! {"$redact": "$$KEEP"}; n];
        let doubles = [
            doc! {"$count": "n"},
            doc! {"$sortByCount": "$a"},
            doc! {"$bucket": {"groupBy": "$a", "boundaries": [0, 1]}},
            doc! {"$setWindowFields": {"output": {"n": {"$documentNumber": {}}}, "sortBy": {"a": 1}}},
            doc! {"$densify": {"field": "a", "range": {"step": 1, "bounds": "full"}}},
        ];
        for stage in doubles {
            let name = stage.keys().next().unwrap().clone();
            assert_eq!(stage_weight(&name), 2, "{name}");
            let mut at_999 = fillers(999);
            at_999.push(stage.clone());
            let err = parse(&at_999).unwrap_err().to_string();
            assert!(
                err.contains("a pipeline has 1001 stages, over the limit of 1000"),
                "{name}: {err}"
            );
            let mut at_998 = fillers(998);
            at_998.push(stage);
            assert_eq!(parse(&at_998).unwrap().len(), 999, "{name}");
        }
        // The rest are one each.
        for stage in [
            doc! {"$project": {"a": 1}},
            doc! {"$group": {"_id": "$a"}},
            doc! {"$bucketAuto": {"groupBy": "$a", "buckets": 2}},
            doc! {"$unset": "a"},
            doc! {"$set": {"a": 1}},
            doc! {"$replaceWith": "$a"},
            doc! {"$sort": {"a": 1}},
            doc! {"$sample": {"size": 1}},
            doc! {"$redact": "$$KEEP"},
            doc! {"$unwind": "$a"},
            doc! {"$fill": {"output": {"a": {"value": 0}}}},
            doc! {"$facet": {"x": []}},
            doc! {"$lookup": {"from": "b", "localField": "a", "foreignField": "a", "as": "j"}},
            doc! {"$unionWith": "b"},
            doc! {"$out": "o"},
        ] {
            let name = stage.keys().next().unwrap().clone();
            assert_eq!(stage_weight(&name), 1, "{name}");
            let mut at_999 = fillers(999);
            at_999.push(stage);
            assert_eq!(parse(&at_999).unwrap().len(), 1_000, "{name}");
        }
        // A sub-pipeline weighs its own stages the same way.
        let branch = |stages: Vec<Document>| {
            parse(&[
                doc! {"$facet": {"a": stages.into_iter().map(Bson::Document).collect::<Vec<_>>()}},
            ])
        };
        let mut over = fillers(999);
        over.push(doc! {"$count": "n"});
        assert!(branch(over).unwrap_err().to_string().contains("a pipeline has 1001 stages"));
        let mut under = fillers(998);
        under.push(doc! {"$count": "n"});
        assert!(branch(under).is_ok());
    }

    #[test]
    fn a_long_run_of_stages_that_stream_answers_what_the_loop_answers() {
        // The order the stages run in is not visible in the answer of a pipeline that cannot
        // fail: a long run of them is the same documents.
        let mut specs = Vec::new();
        for at in 0..400 {
            specs.push(match at % 4 {
                0 => doc! {"$addFields": {format!("f{at}"): at}},
                1 => doc! {"$skip": 0},
                2 => doc! {"$match": {"qty": {"$gte": 0}}},
                _ => doc! {"$unset": format!("f{}", at - 3)},
            });
        }
        let stages = parse(&specs).unwrap();
        let run = |stage: &Stage, input, arrived| {
            apply_charged(stage, input, &Limits::default(), &[], arrived)
        };
        let limits = Limits::default();
        let mut chain = crate::pipeline::build(&stages, &run, &limits, &[]);
        let _more = chain.push_all(sample()).unwrap();
        let mut want = sample();
        for stage in &stages {
            want = apply(stage, want, &Limits::default()).unwrap();
        }
        assert_eq!(chain.finish().unwrap(), want);
    }

    #[test]
    fn match_then_count() {
        let out =
            run(vec![doc! {"$match": {"city": "London"}}, doc! {"$count": "n"}], sample()).unwrap();
        assert_eq!(out, vec![doc! { "n": 2 }]);
    }

    #[test]
    fn count_over_an_empty_stream_is_one_document_holding_zero() {
        // The count of nothing is defined, and it is 0. Emitting no document
        // would make every caller spell a defensive read for a value the stage
        // always knows. Three differently-shaped ways of emptying the stream —
        // a filter matching nothing, a fan-out dropping every row, a `$skip`
        // past the end — and one answer for all. (A `$limit: 0` is a `400`, as it is in the
        // reference.)

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
        ] {
            // Each prefix must really empty the stream, or the count below
            // would be the count of a non-empty one — this catches `$skip:
            // 999` becoming reachable, or `"Berlin"` starting to match.
            let emptied = run(prefix.clone(), sample()).unwrap();
            assert!(emptied.is_empty(), "{prefix:?} left {emptied:?}");

            let mut pipeline = prefix.clone();
            pipeline.push(doc! {"$count": "n"});
            let out = run(pipeline, sample()).unwrap();
            assert_eq!(out, vec![doc! { "n": 0 }], "{prefix:?}");
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
    fn push_and_add_to_set_skip_a_missing_value_and_keep_an_explicit_null() {
        let input = docs(vec![
            doc! { "a": 1 },
            doc! {},
            doc! { "a": Bson::Null },
            doc! { "a": 1 },
            doc! { "x": { "y": 2 } },
        ]);
        let out = run(
            vec![doc! {"$group": {"_id": null,
            "p": {"$push": "$a"}, "s": {"$addToSet": "$a"},
            "n": {"$push": "$x.y"}, "r": {"$push": "$$REMOVE"}}}],
            input,
        )
        .unwrap();
        assert_eq!(
            out[0].get_array("p").unwrap(),
            &vec![Bson::Int32(1), Bson::Null, Bson::Int32(1)]
        );
        assert_eq!(out[0].get_array("s").unwrap(), &vec![Bson::Int32(1), Bson::Null]);
        assert_eq!(out[0].get_array("n").unwrap(), &vec![Bson::Int32(2)]);
        assert!(out[0].get_array("r").unwrap().is_empty());
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
            vec![doc! { "_id": "London", "total": 20 }, doc! { "_id": "Paris", "total": 10 }]
        );
    }

    #[test]
    fn an_integer_sum_stays_an_integer() {
        // Widening to double would lose precision above 2^53 and change what
        // `$type` reports — the same reason documents keep their integer types.
        let out = run(vec![doc! {"$group": {"_id": null, "n": {"$sum": 1}}}], sample()).unwrap();
        assert_eq!(out[0].get_i32("n").unwrap(), 3);
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
        assert_eq!(out[0].get_i32("n").unwrap(), 3);
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
        assert_eq!(out[0], doc! { "_id": strings(&["a", "b"]), "n": 2 });
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
        assert_eq!(out[0], doc! { "_id": 1, "skus": strings(&["a", "b"]), "n": 2 });
        assert_eq!(out[3], doc! { "_id": 4, "skus": [], "n": 0 });
        // A missing path leaves the field out of `$addFields` (so there is no `skus` to
        // project); `$size` of a missing field is null rather than an error, as the
        // deviations register records.
        assert_eq!(out[4], doc! { "_id": 5, "n": Bson::Null });
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
    /// two: NaN against itself, one number in three widths and as a
    /// `Decimal128`, signed zeros, the two nulls, documents differing in key
    /// order and in a nested width, and a string against a symbol.
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
        assert_eq!(buckets.len(), 7, "buckets: {buckets:?}");
        assert_eq!(got.len(), 7, "members: {got:?}");
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
    fn a_value_holding_a_decimal128_is_identified_by_its_value() {
        // A `Decimal128` has an exact key anywhere in a value, so `1` against
        // `1.0` collapses inside a document or array exactly as it does bare, and
        // a decimal equal to the number beside it is the same member. `$group`
        // does the same, and the two agree.
        let one_member = [
            (
                Bson::Document(doc! { "a": 1, "d": dec("1") }),
                Bson::Document(doc! { "a": 1.0, "d": dec("1.00") }),
            ),
            (
                Bson::Array(vec![Bson::Double(0.0), dec("1")]),
                Bson::Array(vec![dec("-0"), Bson::Int64(1)]),
            ),
        ];
        for (x, y) in one_member {
            let both = [x, y];
            let buckets = run(vec![doc! {"$group": {"_id": "$v"}}], as_docs(&both)).unwrap();
            assert_eq!(buckets.len(), 1, "{both:?}");
            assert_eq!(members(&both).len(), 1, "{both:?}");
        }
        // Different values stay different, and 0.1 the decimal is not 0.1 the double.
        let two_members = [dec("0.1"), Bson::Double(0.1)];
        let buckets = run(vec![doc! {"$group": {"_id": "$v"}}], as_docs(&two_members)).unwrap();
        assert_eq!(buckets.len(), 2);
        assert_eq!(members(&two_members).len(), 2);
    }

    #[test]
    fn add_to_set_members_do_not_depend_on_input_order() {
        // A set built on an order that ranked `Decimal128` equal to every number
        // kept `[1, 2]` from one order and `[Dec(5)]` from the other.
        for order in
            [[Bson::Int32(1), dec("5"), Bson::Int32(2)], [dec("5"), Bson::Int32(1), Bson::Int32(2)]]
        {
            assert_eq!(members(&order).len(), 3, "{order:?}");
        }
        let corpus = set_corpus();
        let expected = keys_of(&members(&corpus));
        assert_eq!(expected.len(), 7);
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
        let limits = Limits { max_documents: 10, ..Default::default() };
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
        let limits = Limits { max_documents: 2, ..Default::default() };
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
        let limits = Limits { max_documents: 50, ..Default::default() };
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
        let err = parse(&[doc! {"$typo": {}}]).unwrap_err().to_string();
        assert!(err.contains("$typo"), "{err}");
        assert!(err.contains("$group"), "the error should list what does work: {err}");
        assert!(err.contains("$merge") && err.contains("$out"), "and the write stages: {err}");
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
    fn a_limit_of_zero_is_refused_wherever_it_is_as_the_reference_refuses_it() {
        let refused = "invalid query: $limit must be positive, found 0";
        assert_eq!(parse(&[doc! {"$limit": 0}]).unwrap_err().to_string(), refused);
        // However it is spelled, and after other stages.
        assert!(parse(&[doc! {"$skip": 1}, doc! {"$limit": 0.0}]).is_err());
        assert!(parse(&[doc! {"$limit": 1}]).is_ok());
        let zero = vec![Bson::Document(doc! {"$limit": 0})];
        for wrapped in [
            doc! {"$facet": {"a": zero.clone()}},
            doc! {"$lookup": {"from": "items", "pipeline": zero.clone(), "as": "items"}},
            doc! {"$unionWith": {"coll": "items", "pipeline": zero.clone()}},
        ] {
            let err = parse(&[wrapped]).unwrap_err().to_string();
            assert_eq!(err, refused);
        }
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
        assert_eq!(out, vec![doc! {"_id": 1, "city": "London", "qty": 5, "double": 10}]);
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
        assert_eq!(out, vec![doc! {"_id": 1, "qty": 50}]);
    }

    #[test]
    fn add_fields_writes_a_dotted_path_into_a_subdocument() {
        let out = run(
            vec![doc! {"$addFields": {"totals.doubled": {"$multiply": ["$qty", 2]}}}],
            vec![doc! {"_id": 1, "qty": 5}],
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"_id": 1, "qty": 5, "totals": {"doubled": 10}}]);
    }

    #[test]
    fn project_computes_alongside_inclusions() {
        let out = run(
            vec![doc! {"$project": {"city": 1, "double": {"$multiply": ["$qty", 2]}}}],
            vec![doc! {"_id": 1, "city": "London", "qty": 5}],
        )
        .unwrap();
        // `_id` rides along by default, exactly as an ordinary projection.
        assert_eq!(out, vec![doc! {"city": "London", "_id": 1, "double": 10}]);
    }

    #[test]
    fn project_can_drop_id_while_computing() {
        let out = run(
            vec![doc! {"$project": {"_id": 0, "double": {"$multiply": ["$qty", 2]}}}],
            vec![doc! {"_id": 1, "qty": 5}],
        )
        .unwrap();
        assert_eq!(out, vec![doc! {"double": 10}]);
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
        assert_eq!(out[0].get("double"), Some(&Bson::Int32(10)));
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
        assert_eq!(out, vec![doc! {"_id": "LONDON", "n": 2}, doc! {"_id": "PARIS", "n": 1}]);
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
        assert_eq!(out, vec![doc! {"_id": Bson::Null, "total": 60}]);
    }

    #[test]
    fn a_compound_group_key_now_computes_instead_of_being_one_bucket() {
        // Behaviour change, deliberately: `{_id: {c: "$city"}}` used to be a
        // constant document, so every input landed in a single bucket.
        let out = run(vec![doc! {"$group": {"_id": {"c": "$city"}, "n": {"$sum": 1}}}], sample())
            .unwrap();
        assert_eq!(
            out,
            vec![doc! {"_id": {"c": "London"}, "n": 2}, doc! {"_id": {"c": "Paris"}, "n": 1},]
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
    fn vector_search_parses_with_the_endpoints_fields() {
        let stages = parse(&[
            doc! {"$vectorSearch": {"query": "shoes", "k": 5, "per_document": 2,
            "filter": {"brand": "acme"}}},
            doc! {"$match": {"price": {"$lt": 100}}},
        ])
        .unwrap();
        let Stage::VectorSearch(v) = &stages[0] else { panic!("{:?}", stages[0]) };
        assert_eq!(v.query.as_deref(), Some("shoes"));
        assert_eq!((v.k, v.per_document), (Some(5), Some(2)));
        assert_eq!(v.filter, Some(doc! {"brand": "acme"}));
        let byo = parse(&[doc! {"$vectorSearch": {"vector": [1, 0.5, 0.0]}}]).unwrap();
        let Stage::VectorSearch(v) = &byo[0] else { panic!() };
        assert_eq!(v.vector.as_deref(), Some(&[1.0f32, 0.5, 0.0][..]));
    }

    #[test]
    fn vector_search_must_be_the_first_stage() {
        let err = parse(&[doc! {"$match": {}}, doc! {"$vectorSearch": {"query": "x"}}])
            .unwrap_err()
            .to_string();
        assert!(err.contains("first stage"), "{err}");
        assert!(err.contains("stage 2"), "{err}");
        let err = parse(&[doc! {"$lookup": {
            "from": "other", "let": {}, "pipeline": [{"$vectorSearch": {"query": "x"}}],
            "as": "hits"
        }}])
        .unwrap_err()
        .to_string();
        assert!(err.contains("first stage of the top-level pipeline"), "{err}");
    }

    fn refusal(field: &str, value: Bson) -> String {
        let spec = doc! {"query": "x", field: value};
        parse(&[doc! {"$vectorSearch": spec}]).unwrap_err().to_string()
    }

    #[test]
    fn vector_search_refuses_query_vector_and_says_use_vector() {
        let err = refusal("queryVector", Bson::Array(vec![]));
        assert!(err.contains("`queryVector`") && err.contains("use `vector`"), "{err}");
    }

    #[test]
    fn vector_search_refuses_limit_and_says_use_k() {
        let err = refusal("limit", Bson::Int32(5));
        assert!(err.contains("`limit`") && err.contains("use `k`"), "{err}");
    }

    #[test]
    fn vector_search_refuses_num_candidates_and_says_there_is_no_such_knob() {
        let err = refusal("numCandidates", Bson::Int32(100));
        assert!(err.contains("`numCandidates`") && err.contains("no such knob"), "{err}");
        assert!(err.contains("one vector index per collection") && err.contains("`k`"), "{err}");
    }

    #[test]
    fn vector_search_refuses_index_and_says_there_is_one_index() {
        let err = refusal("index", Bson::String("idx".into()));
        assert!(
            err.contains("`index`") && err.contains("one vector index per collection"),
            "{err}"
        );
    }

    #[test]
    fn vector_search_refuses_unknown_and_malformed_fields() {
        for spec in [
            doc! {"query": "x", "bogus": 1},
            doc! {},
            doc! {"query": 3},
            doc! {"vector": "abc"},
            doc! {"vector": ["a"]},
            doc! {"query": "x", "k": -1},
            doc! {"query": "x", "filter": "no"},
        ] {
            assert!(parse(&[doc! {"$vectorSearch": spec.clone()}]).is_err(), "{spec:?}");
        }
        let err =
            parse(&[doc! {"$vectorSearch": {"query": "x", "bogus": 1}}]).unwrap_err().to_string();
        assert!(err.contains("bogus") && err.contains("per_document"), "{err}");
    }

    #[test]
    fn no_vector_search_message_has_a_run_of_spaces() {
        // A lost `\` continuation leaves the next line's indentation inside the
        // message; substring tests do not see it.
        let mut messages = Vec::new();
        for field in ["queryVector", "limit", "numCandidates", "index", "bogus"] {
            let spec = doc! {"query": "x", field: 1};
            messages.push(parse(&[doc! {"$vectorSearch": spec}]).unwrap_err().to_string());
        }
        for spec in [
            doc! {},
            doc! {"query": 3},
            doc! {"vector": "abc"},
            doc! {"vector": ["a"]},
            doc! {"query": "x", "k": -1},
            doc! {"query": "x", "filter": "no"},
        ] {
            messages.push(parse(&[doc! {"$vectorSearch": spec}]).unwrap_err().to_string());
        }
        messages.push(
            parse(&[doc! {"$match": {}}, doc! {"$vectorSearch": {"query": "x"}}])
                .unwrap_err()
                .to_string(),
        );
        messages.push(
            parse(&[doc! {"$lookup": {
                "from": "o", "let": {}, "pipeline": [{"$vectorSearch": {"query": "x"}}],
                "as": "h"
            }}])
            .unwrap_err()
            .to_string(),
        );
        messages.push(parse(&[doc! {"$bucket": {}}]).unwrap_err().to_string());
        assert_eq!(messages.len(), 14);
        for message in messages {
            assert!(!message.contains("  "), "a run of spaces in {message:?}");
        }
    }

    #[test]
    fn the_unsupported_stage_message_lists_vector_search() {
        let err = parse(&[doc! {"$typo": "x"}]).unwrap_err().to_string();
        assert!(err.contains("$vectorSearch"), "{err}");
    }

    #[test]
    fn the_unsupported_stage_message_lists_the_new_stages() {
        let err = parse(&[doc! {"$typo": "x"}]).unwrap_err().to_string();
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
        assert_eq!(out, vec![doc! {"_id": 8, "total": 3}, doc! {"_id": 9, "total": 4}]);
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
        assert_eq!(out, vec![doc! { "_id": Bson::Null, "n": 0_i32 }]);

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
        assert_eq!(out, vec![doc! { "_id": "a", "v": 6_i32 }, doc! { "_id": "b", "v": 2_i32 }]);
        // Another group's bad value does not touch this one's.
        let out = run(vec![stage()], vec![good("a", 5), bad("b"), good("b", 1)]).unwrap();
        assert_eq!(out[1], doc! { "_id": "b", "v": 2_i32 });
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
        assert_eq!(out[0].get("v"), Some(&Bson::Int32(6)));
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
            Filter::And(
                vec![
                    filter::parse(&doc! {"a": 1}).unwrap(),
                    filter::parse(&doc! {"b": {"$gt": 2}}).unwrap(),
                ]
                .into()
            )
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
                doc! {"_id": 0, "ids": [3]},
                doc! {"_id": 1, "ids": [2]},
                doc! {"_id": 2, "ids": [1]},
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

    /// A `$match` in a sub-pipeline reads the `let` names in scope through `$expr`
    /// and nowhere else (ADR-222; the filter language has no variables, so a bare
    /// `"$$oid"` value is refused below). A name that is not in scope is still
    /// refused, and `$$ROOT` still names the foreign document. A `$match` outside a
    /// sub-pipeline binds nothing.
    #[test]
    fn a_match_in_a_lookup_sub_pipeline_reads_the_let_through_expr_only() {
        let lookup = |stage: Document| {
            parse(&[doc! {"$lookup": {
                "from": "items", "let": {"oid": "$_id"}, "pipeline": [stage], "as": "items"
            }}])
        };
        assert!(lookup(doc! {"$match": {"$expr": {"$eq": ["$order", "$$oid"]}}}).is_ok());
        assert!(lookup(doc! {"$match": {"$expr": {"$eq": ["$order", "$$nope"]}}}).is_err());
        assert!(lookup(doc! {"$match": {"$expr": {"$gt": ["$$ROOT.qty", 0]}}}).is_ok());
        // Under the logical operators and an `$elemMatch` body too.
        assert!(
            lookup(doc! {"$match": {"$or": [{"k": 1}, {"$expr": {"$eq": ["$o", "$$oid"]}}]}})
                .is_ok()
        );
        assert!(
            lookup(doc! {"$match": {"lines": {"$elemMatch": {"$expr": {"$eq": ["$o", "$$oid"]}}}}})
                .is_ok()
        );
        // A later stage's `$match` reads it as well.
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "items", "let": {"oid": "$_id"},
                "pipeline": [{"$limit": 5}, {"$match": {"$expr": {"$eq": ["$o", "$$oid"]}}}],
                "as": "items"
            }}])
            .is_ok()
        );
        // A top-level `$match` has no names to read.
        assert!(parse(&[doc! {"$match": {"$expr": {"$eq": ["$o", "$$oid"]}}}]).is_err());
        // Outside the lookup the name is gone again.
        assert!(
            parse(&[
                doc! {"$lookup": {"from": "a", "let": {"oid": "$_id"}, "pipeline": [], "as": "i"}},
                doc! {"$match": {"$expr": {"$eq": ["$o", "$$oid"]}}},
            ])
            .is_err()
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

        // `$expr` is the expression parser's subtree: it reads the names in scope,
        // and keeps its own, more specific, refusal for one that is not.
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "a", "let": {"oid": "$_id"},
                "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$oid"]}}}], "as": "x"
            }}])
            .is_ok()
        );
        let err = parse(&[doc! {"$lookup": {
            "from": "a", "let": {"oid": "$_id"},
            "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$nope"]}}}], "as": "x"
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

    /// The refusal has a way round, recorded in the Deviations register:
    /// `$literal` under `$expr`. The `$expr` subtree is the expression
    /// parser's, which reads a string a `$literal` holds as a string and
    /// never as a reference, so a stored value that begins with `$$` can be
    /// matched from inside a sub-pipeline without a `let` name being guessed.
    #[test]
    fn a_literal_under_expr_matches_a_stored_variable_string_in_a_sub_pipeline() {
        let stages = parse(&[doc! {"$lookup": {
            "from": "coupons", "let": {"oid": "$_id"},
            "pipeline": [{"$match": {"$expr": {"$eq": ["$code", {"$literal": "$$promo"}]}}}],
            "as": "c"
        }}])
        .unwrap();
        let foreign = [
            doc! {"_id": 1, "code": "$$promo", "pct": 10},
            doc! {"_id": 2, "code": "SAVE5", "pct": 5},
            doc! {"_id": 3, "code": "promo", "pct": 1},
        ];
        let out = join_in_memory(&stages[0], vec![doc! {"_id": 100}], &foreign);
        assert_eq!(out[0].get_array("c").unwrap(), &vec![Bson::from(foreign[0].clone())]);
        // The same string written bare, in the same stage, is still refused.
        assert!(
            parse(&[doc! {"$lookup": {
                "from": "coupons", "let": {"oid": "$_id"},
                "pipeline": [{"$match": {"code": "$$promo"}}], "as": "c"
            }}])
            .is_err()
        );
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
        let Stage::Lookup { as_field, join: Join::Pipeline { vars, stages, .. }, .. } = stage
        else {
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
            &vec![Bson::Document(doc! {"label": "x", "count": 2, "skus": ["a", "b"]})]
        );
    }

    /// A `$lookup` `let` binds every variable for each input document, read or
    /// not, unlike `$let` (ADR-219): its sub-pipeline runs in the frame these
    /// make, which holds values and not expressions.
    #[test]
    fn a_lookup_let_still_evaluates_a_variable_nothing_reads() {
        let vars = vec![
            ("kept".to_string(), Expr::Literal(1.into())),
            ("unread".to_string(), Expr::parse(&doc! {"$size": "$name"}.into()).unwrap()),
        ];
        let err = bind_let(&vars, &doc! {"name": "text"}, &[]).unwrap_err().to_string();
        assert!(err.contains("$size needs an array"), "{err}");
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
            vec![doc! {"$group": {"_id": ["$a", "$b"], "pairs": {"$push": {"$concatArrays": [["$_id", "$extra"]]}}}}],
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

    /// The stage that sorts a pipeline's documents in hand places a decimal
    /// among the numbers by its value, as `find`'s sort does. It used to
    /// refuse the pipeline naming the document.
    #[test]
    fn a_sort_stage_places_a_decimal128_by_its_value() {
        let dec = |text: &str| Bson::Decimal128(text.parse().unwrap());
        let input = vec![
            doc! { "_id": 1, "v": 1 },
            doc! { "_id": 2, "v": dec("1.5") },
            doc! { "_id": 3, "v": 2 },
            doc! { "_id": 4, "v": dec("0.1") },
            doc! { "_id": 5, "v": 0.1 },
        ];
        let ids = |spec: Document| -> Vec<i32> {
            let stages = parse(&[doc! { "$sort": spec }]).unwrap();
            let out = apply(&stages[0], input.clone(), &Limits::default()).unwrap();
            out.iter().map(|d| d.get_i32("_id").unwrap()).collect()
        };
        assert_eq!(ids(doc! { "v": 1 }), vec![4, 5, 1, 2, 3]);
        assert_eq!(ids(doc! { "v": -1 }), vec![3, 2, 1, 5, 4]);
    }

    /// `$limit`, `$skip` and `$vectorSearch`'s `k` take a decimal of whole
    /// value as they take a double, and `$project` reads a decimal flag as the
    /// number it is. They used to refuse it, and `$project` computed the
    /// decimal as a literal instead of including the field.
    #[test]
    fn stage_arguments_read_a_decimal_as_the_double_of_its_value() {
        let dec = |text: &str| Bson::Decimal128(text.parse().unwrap());
        let input: Vec<Document> = (1..=5).map(|i| doc! { "_id": i, "a": i, "b": -i }).collect();
        let run = |stage: Document| {
            let stages = parse(&[stage])?;
            apply(&stages[0], input.clone(), &Limits::default())
        };
        for (decimal, double) in [
            (doc! { "$limit": dec("2") }, doc! { "$limit": 2.0 }),
            (doc! { "$limit": dec("2.00") }, doc! { "$limit": 2.0 }),
            (doc! { "$skip": dec("3") }, doc! { "$skip": 3.0 }),
            (doc! { "$project": { "a": dec("1") } }, doc! { "$project": { "a": 1.0 } }),
            (doc! { "$project": { "a": dec("0E+3") } }, doc! { "$project": { "a": 0.0 } }),
            (
                doc! { "$project": { "a": dec("-0.5"), "_id": dec("0") } },
                doc! { "$project": { "a": -0.5, "_id": 0.0 } },
            ),
        ] {
            assert_eq!(run(decimal.clone()).unwrap(), run(double).unwrap(), "{decimal}");
        }
        assert_eq!(
            run(doc! { "$project": { "a": dec("1") } }).unwrap()[0],
            doc! { "_id": 1, "a": 1 }
        );
        for bad in [dec("2.5"), dec("NaN"), dec("-1")] {
            let msg = run(doc! { "$limit": bad.clone() }).unwrap_err().to_string();
            assert!(
                msg.contains("non-negative whole number, found a Decimal128")
                    || msg.contains("cannot be negative"),
                "{bad}: {msg}"
            );
        }
        let k = parse(&[doc! { "$vectorSearch": { "vector": [1, 0.5], "k": dec("5") } }]).unwrap();
        let k_double =
            parse(&[doc! { "$vectorSearch": { "vector": [1, 0.5], "k": 5.0 } }]).unwrap();
        assert_eq!(format!("{k:?}"), format!("{k_double:?}"));
        // A decimal component of a query vector stays refused: converting it to
        // a 32-bit float needs a rounding rule this build does not have.
        let err = parse(&[doc! { "$vectorSearch": { "vector": [1, dec("0.5")] } }]).unwrap_err();
        assert!(err.to_string().contains("would have to be rounded to a 32-bit float"), "{err}");
    }
}

/// How a sub-pipeline's leading `$match` is taken apart for the join (ADR-222).
#[cfg(test)]
mod lead {
    use super::*;
    use bson::doc;

    fn lead_of(stage: Document) -> Option<Box<Lead>> {
        let stages = parse(&[doc! {"$lookup": {
            "from": "a", "let": {"v": "$v", "w": "$w"}, "pipeline": [stage], "as": "x"
        }}])
        .unwrap();
        let Stage::Lookup { join: Join::Pipeline { lead, .. }, .. } =
            stages.into_iter().next().unwrap()
        else {
            panic!("a pipeline lookup");
        };
        lead
    }

    fn clauses(filter: &Filter) -> usize {
        match filter {
            Filter::AlwaysTrue => 0,
            Filter::And(leaves) => leaves.len(),
            _ => 1,
        }
    }

    #[test]
    fn a_match_that_reads_no_variable_has_no_lead() {
        assert!(lead_of(doc! {"$match": {"kind": "x", "$expr": {"$gt": ["$q", 1]}}}).is_none());
    }

    #[test]
    fn an_eq_between_the_document_and_a_variable_is_the_key_either_way_round() {
        for eq in [json_eq("$k", "$$v"), json_eq("$$v", "$k")] {
            let lead = lead_of(doc! {"$match": {"$expr": eq}}).expect("a lead");
            let key = lead.key.expect("the key");
            assert!(key.foreign.reads_document());
            assert!(!key.local.reads_document());
            assert_eq!(clauses(&lead.residual), 0);
            assert_eq!(clauses(&lead.independent), 0);
        }
        let flipped = lead_of(doc! {"$match": {"$expr": json_eq("$$v", "$k")}}).unwrap();
        assert!(flipped.key.unwrap().local_first);
    }

    fn json_eq(a: &str, b: &str) -> Document {
        doc! {"$eq": [a, b]}
    }

    /// The key is any expression of the document alone against any of the variables
    /// alone, not only a field path.
    #[test]
    fn the_sides_are_any_expressions_of_the_document_and_of_the_variables() {
        let lead = lead_of(doc! {"$match": {"$expr": {"$eq": [
        {"$toLower": "$k"}, {"$toLower": {"$concat": ["$$v", "x"]}}]}}})
        .unwrap();
        assert!(lead.key.is_some());
    }

    #[test]
    fn independent_clauses_are_split_off_and_the_rest_are_residual_in_written_order() {
        let lead = lead_of(doc! {"$match": {"$and": [
        {"kind": "line"},
        {"$expr": {"$gt": ["$q", 1]}},
        {"$expr": {"$lte": ["$z", "$$w"]}},
        {"$expr": {"$eq": ["$k", "$$v"]}},
        {"$expr": {"$gte": ["$z", "$$w"]}}]}})
        .unwrap();
        assert!(lead.key.is_some());
        // Two independent clauses (the plain field and the independent `$expr`),
        // and the variable-reading ones other than the key.
        assert_eq!(clauses(&lead.independent), 2);
        assert_eq!(clauses(&lead.variable), 3);
        assert_eq!(clauses(&lead.residual), 2);
        // The first recognised `$eq` is the key even when other variable clauses
        // are written before it.
        let Filter::And(rest) = &lead.residual else { panic!() };
        assert!(matches!(&rest[0], Filter::Expr(e) if format!("{e:?}").contains("Lte")));
        assert!(matches!(&rest[1], Filter::Expr(e) if format!("{e:?}").contains("Gte")));
    }

    #[test]
    fn nested_and_is_flattened_in_order_and_nothing_else_is() {
        let lead = lead_of(doc! {"$match": {"$and": [
        {"$and": [{"kind": "line"}, {"$expr": {"$eq": ["$k", "$$v"]}}]},
        {"$expr": {"$gt": ["$z", "$$w"]}}]}})
        .unwrap();
        assert!(lead.key.is_some());
        assert_eq!(clauses(&lead.independent), 1);
        assert_eq!(clauses(&lead.residual), 1);
    }

    /// A join is recognised through `$and` and nowhere else: under `$or`, `$nor`, and
    /// an `$expr`'s own `$and`, or with the wrong number of arguments, there is a lead
    /// (the clause reads a variable) and no key.
    #[test]
    fn a_join_is_not_recognised_below_or_nor_or_an_expressions_own_and() {
        let eq = doc! {"$expr": {"$eq": ["$k", "$$v"]}};
        let shapes = [
            doc! {"$match": {"$or": [eq.clone(), {"kind": "x"}]}},
            doc! {"$match": {"$nor": [eq.clone()]}},
            doc! {"$match": {"$expr": {"$and": [{"$eq": ["$k", "$$v"]}, {"$gt": ["$q", 0]}]}}},
            doc! {"$match": {"$expr": {"$gt": ["$k", "$$v"]}}},
            doc! {"$match": {"$expr": {"$ne": ["$k", "$$v"]}}},
            // Both sides read the document and a variable.
            doc! {"$match": {"$expr": {"$eq": [{"$add": ["$k", "$$w"]}, "$$v"]}}},
            // The other side reads the document.
            doc! {"$match": {"$expr": {"$eq": ["$k", {"$add": ["$$v", "$q"]}]}}},
            doc! {"$match": {"lines": {"$elemMatch": {"$expr": {"$eq": ["$q", "$$v"]}}}}},
        ];
        // Another arity is no `$eq` at all: the parser refuses it before a join is asked.
        assert!(
            parse(&[doc! {"$lookup": {"from": "a", "let": {"v": "$v"}, "as": "x", "pipeline": [
            {"$match": {"$expr": {"$eq": ["$k", "$$v", 1]}}}]}}])
            .is_err()
        );
        for shape in shapes {
            let lead = lead_of(shape.clone()).unwrap_or_else(|| panic!("a lead for {shape}"));
            assert!(lead.key.is_none(), "no key for {shape}");
        }
    }

    /// `$$NOW` and the other system variables a build might one day add are refused as
    /// they are today, so a key expression is a function of the document and the bound
    /// variables only.
    #[test]
    fn system_variables_other_than_root_and_current_stay_refused() {
        for name in ["DESCEND", "CLUSTER_TIME", "PRUNE", "USER_ROLES", "KEEP", "SEARCH_META"] {
            let refused = parse(&[doc! {"$lookup": {
                "from": "a", "let": {"v": "$v"},
                "pipeline": [{"$match": {"$expr": {"$eq": ["$k", format!("$${name}")]}}}],
                "as": "x"
            }}]);
            assert!(refused.is_err(), "$${name} must stay refused");
        }
    }

    /// Variables from an enclosing `$lookup` are in scope for the inner one's `$match`.
    #[test]
    fn an_enclosing_lookups_names_are_in_scope_for_the_inner_match() {
        let stages = parse(&[doc! {"$lookup": {
            "from": "a", "let": {"outer": "$v"}, "as": "x",
            "pipeline": [{"$lookup": {
                "from": "b", "let": {"inner": "$k"}, "as": "y",
                "pipeline": [{"$match": {"$expr": {"$eq": ["$z", "$$outer"]}}}]
            }}]
        }}])
        .unwrap();
        let Stage::Lookup { join: Join::Pipeline { stages: inner, .. }, .. } = &stages[0] else {
            panic!()
        };
        let Stage::Lookup { join: Join::Pipeline { lead, .. }, .. } = &inner[0] else { panic!() };
        assert!(lead.as_ref().is_some_and(|l| l.key.is_some()));
    }
}

/// The grouping identity against the canonical order (ADR-186, amended by the
/// Decimal128 design, 2.4): two values are one `$group` bucket, one
/// `$addToSet` member and one set-operator member exactly when `$eq` calls
/// them equal, a `Decimal128` in every special form included, bare and nested.
#[cfg(test)]
mod identity {
    use std::cmp::Ordering;

    use super::*;
    use bson::doc;
    use kimmy_core::canonical_cmp;
    use proptest::prelude::*;

    fn dec(text: &str) -> Bson {
        Bson::Decimal128(text.parse().unwrap_or_else(|e| panic!("{text}: {e:?}")))
    }

    /// A `Decimal128` from its 128 bits, sign first.
    fn raw(bits: u128) -> Bson {
        Bson::Decimal128(bson::Decimal128::from_bytes(bits.to_le_bytes()))
    }

    /// Scalars meant to meet every way two numbers can be one value or two:
    /// a decimal NaN of either sign, signalling or with a payload, against a
    /// double NaN; an infinity with payload bits set; a zero in every spelling,
    /// a non-canonical coefficient and the `11` combination form included;
    /// cohorts; a decimal equal to an integer above 2^53, to `i64::MAX` and to
    /// a power of two below one; the extremes; and `0.1` the decimal apart
    /// from `0.1` the double, which is slightly above a tenth.
    fn leaves() -> Vec<Bson> {
        vec![
            Bson::Double(f64::NAN),
            raw(0x7c_u128 << 120),
            raw(0xfc_u128 << 120),
            raw(0x7e_u128 << 120),
            raw((0x7c_u128 << 120) | 12_345),
            Bson::Double(f64::INFINITY),
            dec("Infinity"),
            raw((0x78_u128 << 120) | 7),
            Bson::Double(f64::NEG_INFINITY),
            dec("-Infinity"),
            raw((0xf8_u128 << 120) | 7),
            Bson::Int32(0),
            Bson::Double(-0.0),
            dec("0"),
            dec("-0"),
            dec("0E-6176"),
            dec("0E+6111"),
            dec("-0.000"),
            raw((1u128 << 113) - 1),
            raw((0b011_u128 << 125) | 1),
            Bson::Int32(1),
            Bson::Double(1.0),
            dec("1"),
            dec("1.0"),
            dec("1.00"),
            dec("10E-1"),
            dec("0.1E1"),
            dec("1.000000000000000000000000000000000"),
            dec("1.000000000000000000000000000000001"),
            Bson::Double(0.1),
            dec("0.1"),
            dec("0.10"),
            dec("0.1000000000000000055511151231257827"),
            Bson::Int64(5),
            dec("5"),
            dec("5.000000000000000000000000000000001"),
            dec("4.999999999999999999999999999999999"),
            Bson::Double(9_007_199_254_740_992.0),
            Bson::Int64(9_007_199_254_740_993),
            dec("9007199254740993"),
            dec("9007199254740992.0"),
            Bson::Int64(i64::MAX),
            dec("9223372036854775807"),
            Bson::Double(9_223_372_036_854_775_808.0),
            dec("9223372036854775808"),
            Bson::Double(2f64.powi(-48)),
            dec("3.552713678800500929355621337890625E-15"),
            dec("1E-6176"),
            Bson::Double(5e-324),
            dec("9.999999999999999999999999999999999E+6144"),
            Bson::Double(f64::MAX),
            Bson::Double(-1.5),
            dec("-1.50"),
            dec("-15E-1"),
            Bson::Null,
            Bson::Undefined,
            Bson::String("1".into()),
            Bson::Symbol("1".into()),
            Bson::Boolean(true),
            Bson::MinKey,
            Bson::MaxKey,
        ]
    }

    /// Each leaf bare, and nested where the comparison reaches it: in a
    /// document, in an array beside another value, in an array in a document,
    /// and in a scoped JavaScript value, which compares by its scope.
    fn corpus() -> Vec<Bson> {
        let mut out = Vec::new();
        for leaf in leaves() {
            out.push(Bson::Document(doc! { "a": leaf.clone() }));
            out.push(Bson::Array(vec![leaf.clone(), Bson::String("s".into())]));
            out.push(Bson::Document(doc! { "a": [leaf.clone()] }));
            out.push(Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
                code: "f".into(),
                scope: doc! { "a": leaf.clone() },
            }));
            out.push(leaf);
        }
        out
    }

    fn equal(a: &Bson, b: &Bson) -> bool {
        canonical_cmp(a, b) == Ordering::Equal
    }

    /// Whether a set operator holds `a` and `b` as one member: `[a]` is a
    /// subset of `[b]`, and the union of the two has one member.
    fn one_member(a: &Bson, b: &Bson) -> (bool, bool) {
        static SUBSET: std::sync::LazyLock<Expr> = std::sync::LazyLock::new(|| {
            Expr::parse(&Bson::Document(doc! { "$setIsSubset": ["$a", "$b"] })).unwrap()
        });
        static UNION: std::sync::LazyLock<Expr> = std::sync::LazyLock::new(|| {
            Expr::parse(&Bson::Document(doc! { "$setUnion": ["$a", "$b"] })).unwrap()
        });
        let d = doc! { "a": [a.clone()], "b": [b.clone()] };
        let subset = SUBSET.eval(&d).unwrap();
        let union = UNION.eval(&d).unwrap();
        let Bson::Array(union) = union else { panic!("{union:?}") };
        (subset == Bson::Boolean(true), union.len() == 1)
    }

    /// The differential that replaces ADR-186's 9 = 9: on every pair of the
    /// corpus, the key bytes are equal exactly when `$eq` says equal, and the
    /// set operators' member agrees with both.
    #[test]
    fn group_key_and_the_set_member_are_eq_on_every_pair() {
        let corpus = corpus();
        let keys: Vec<Vec<u8>> = corpus.iter().map(group_key).collect();
        let (mut equal_pairs, mut decimal_equal_pairs) = (0usize, 0usize);
        for (i, a) in corpus.iter().enumerate() {
            for (j, b) in corpus.iter().enumerate() {
                let eq = equal(a, b);
                assert_eq!(keys[i] == keys[j], eq, "group_key and $eq disagree: {a:?} / {b:?}");
                if j >= i {
                    // The set operators are slower to ask; one direction of
                    // each pair is enough, `$eq` being symmetric.
                    let (subset, union) = one_member(a, b);
                    assert_eq!(subset, eq, "$setIsSubset and $eq disagree: {a:?} / {b:?}");
                    assert_eq!(union, eq, "$setUnion and $eq disagree: {a:?} / {b:?}");
                }
                if eq && i != j {
                    equal_pairs += 1;
                    if kimmy_core::holds_decimal128(a) != kimmy_core::holds_decimal128(b) {
                        decimal_equal_pairs += 1;
                    }
                }
            }
        }
        // Premises: a corpus with no equal pairs, or none across a decimal and
        // another type, satisfies every assertion above.
        assert!(equal_pairs > 400, "premise: {equal_pairs} equal pairs");
        assert!(decimal_equal_pairs > 150, "premise: {decimal_equal_pairs} across a decimal");
    }

    fn run(pipeline: Vec<Document>, input: Vec<Document>) -> Vec<Document> {
        let stages = parse(&pipeline).unwrap();
        let mut current = input;
        for stage in &stages {
            current = apply(stage, current, &Limits::default()).unwrap();
        }
        current
    }

    /// `$group` makes one bucket per class of `$eq`, its `_id` the first member
    /// seen, and `$addToSet` keeps the same members (design 2.2: `$group` over
    /// `[Decimal128("1.00"), 1]` reports `_id: Decimal128("1.00")`).
    #[test]
    fn a_bucket_is_a_class_of_eq_and_its_id_the_first_member_seen() {
        let corpus = corpus();
        let input: Vec<Document> = corpus
            .iter()
            .enumerate()
            .map(|(i, v)| doc! { "i": i as i64, "v": v.clone() })
            .collect();
        let mut classes: Vec<&Bson> = Vec::new();
        for v in &corpus {
            if !classes.iter().any(|c| equal(c, v)) {
                classes.push(v);
            }
        }
        let buckets =
            run(vec![doc! { "$group": { "_id": "$v", "n": { "$sum": 1 } } }], input.clone());
        assert_eq!(buckets.len(), classes.len());
        for bucket in &buckets {
            let id = bucket.get("_id").unwrap();
            let first = corpus.iter().find(|v| equal(v, id)).unwrap();
            assert_eq!(format!("{id:?}"), format!("{first:?}"), "the first member seen");
            let members = corpus.iter().filter(|v| equal(v, id)).count() as i32;
            assert_eq!(bucket.get_i32("n").unwrap(), members, "{id:?}");
        }
        let set = run(vec![doc! { "$group": { "_id": null, "s": { "$addToSet": "$v" } } }], input);
        let members = set[0].get_array("s").unwrap();
        assert_eq!(members.len(), classes.len());
        for (member, class) in members.iter().zip(&classes) {
            assert_eq!(format!("{member:?}"), format!("{class:?}"), "first-seen order");
        }

        // The design's worked row.
        let out = run(
            vec![doc! { "$group": { "_id": "$v" } }],
            vec![
                doc! { "v": dec("1.00") },
                doc! { "v": 1 },
                doc! { "v": 1.0 },
                doc! { "v": dec("1") },
            ],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("_id"), Some(&dec("1.00")));
    }

    /// `$max` and `$min` rank a decimal among the numbers by its value
    /// (design 2.2: `$max` over `[Decimal128("1.5"), 5]` is 5; it used to be
    /// 1.5, the decimal ranked equal to 5 and kept as the first seen).
    #[test]
    fn the_max_and_min_accumulators_rank_a_decimal128_by_its_value() {
        let extremes = |values: Vec<Bson>| {
            let input = values.into_iter().map(|v| doc! { "v": v }).collect();
            let out = run(
                vec![
                    doc! { "$group": { "_id": null, "hi": { "$max": "$v" }, "lo": { "$min": "$v" } } },
                ],
                input,
            );
            (out[0].get("hi").unwrap().clone(), out[0].get("lo").unwrap().clone())
        };
        assert_eq!(extremes(vec![dec("1.5"), Bson::Int32(5)]), (Bson::Int32(5), dec("1.5")));
        assert_eq!(extremes(vec![Bson::Int32(5), dec("1.5")]), (Bson::Int32(5), dec("1.5")));
        assert_eq!(
            extremes(vec![Bson::Double(0.1), dec("0.1"), dec("1E+400"), Bson::Double(f64::MAX)]),
            (dec("1E+400"), dec("0.1"))
        );
        // Equal values keep the first seen, whatever its type.
        assert_eq!(extremes(vec![dec("2.0"), Bson::Int32(2)]), (dec("2.0"), dec("2.0")));
    }

    /// The error a pipeline gives, which must fail.
    fn refused(pipeline: Vec<Document>, input: Vec<Document>) -> String {
        let stages = parse(&pipeline).unwrap();
        let mut current = input;
        for stage in &stages {
            match apply(stage, current, &Limits::default()) {
                Ok(next) => current = next,
                Err(e) => return e.to_string(),
            }
        }
        panic!("{pipeline:?} should be refused, gave {current:?}");
    }

    /// `$sum` and `$avg` over a group that holds a `Decimal128` are exact
    /// (ADR-252): every numeric value as converted, rounded once, as a
    /// `Decimal128`. Over `1`, `Decimal128("2.5")` and `2` the values add up to
    /// 5.5, and the mean is that sum over three, to 34 digits.
    #[test]
    fn sum_and_avg_over_a_group_with_a_decimal128_are_exact() {
        let input = || vec![doc! { "v": 1 }, doc! { "v": dec("2.5") }, doc! { "v": 2 }];
        let out = run(
            vec![doc! { "$group": { "_id": null, "s": { "$sum": "$v" }, "m": { "$avg": "$v" } } }],
            input(),
        );
        assert_eq!(out[0].get("s"), Some(&dec("5.5")));
        assert_eq!(out[0].get("m"), Some(&dec("1.833333333333333333333333333333333")));
        // Wherever the decimal stands, and in every order: the same bytes.
        let values = [dec("2.5"), Bson::Int32(1), Bson::Int64(2), Bson::Double(0.5), dec("0.10")];
        let mut seen = std::collections::BTreeSet::new();
        for rotation in 0..values.len() {
            for reversed in [false, true] {
                let mut order: Vec<Bson> =
                    values.iter().cycle().skip(rotation).take(5).cloned().collect();
                if reversed {
                    order.reverse();
                }
                let docs: Vec<Document> = order.into_iter().map(|v| doc! { "v": v }).collect();
                let out = run(
                    vec![
                        doc! { "$group": { "_id": null, "s": { "$sum": "$v" }, "m": { "$avg": "$v" } } },
                    ],
                    docs,
                );
                let (s, m) = (out[0].get("s").unwrap().clone(), out[0].get("m").unwrap().clone());
                let bytes = |b: &Bson| match b {
                    Bson::Decimal128(d) => d.bytes(),
                    other => panic!("not a decimal: {other:?}"),
                };
                seen.insert((bytes(&s), bytes(&m)));
            }
        }
        assert_eq!(seen.len(), 1, "one answer in any order");
        // Per bucket: one with a decimal and one without keep their own types.
        let grouped = vec![
            doc! { "k": 1, "v": 1 },
            doc! { "k": 2, "v": dec("0.10") },
            doc! { "k": 1, "v": 2 },
        ];
        let out = run(
            vec![
                doc! { "$group": { "_id": "$k", "s": { "$sum": "$v" }, "m": { "$avg": "$v" } } },
                doc! { "$sort": { "_id": 1 } },
            ],
            grouped,
        );
        assert_eq!(out[0].get("s"), Some(&Bson::Int32(3)));
        assert_eq!(out[0].get("m"), Some(&Bson::Double(1.5)));
        assert_eq!(out[1].get("s"), Some(&dec("0.10")));
        assert_eq!(out[1].get("m"), Some(&dec("0.10")));
        // A decimal literal is an operand on every document.
        let mut literal = Document::new();
        literal.insert("$sum", dec("1"));
        let out = run(vec![doc! { "$group": { "_id": null, "r": literal } }], input());
        assert_eq!(out[0].get("r"), Some(&dec("3")));
    }

    /// Only the operand itself is summed. An array or a document is not a
    /// number to these accumulators, which never read inside one, so it is
    /// skipped whatever it holds, a decimal included, exactly as before; a
    /// missing field and `null` are skipped; and the accumulators that do no
    /// arithmetic take a decimal as they take any value.
    #[test]
    fn only_a_decimal128_operand_is_summed_and_the_other_accumulators_take_one() {
        let input = vec![
            doc! { "v": 1, "a": [1, dec("2.5")], "d": { "n": dec("2.5") } },
            doc! { "v": 2, "a": [1, 2], "d": { "n": 2 } },
            doc! { "v": Bson::Null },
            doc! {},
        ];
        let out = run(
            vec![doc! { "$group": {
                "_id": null,
                "s": { "$sum": "$v" }, "m": { "$avg": "$v" },
                "sa": { "$sum": "$a" }, "ma": { "$avg": "$a" },
                "sd": { "$sum": "$d" }, "md": { "$avg": "$d" },
                "n": { "$sum": 1 },
            } }],
            input,
        );
        assert_eq!(out[0].get("s"), Some(&Bson::Int32(3)));
        assert_eq!(out[0].get("m"), Some(&Bson::Double(1.5)));
        assert_eq!(out[0].get("sa"), Some(&Bson::Int32(0)), "an array is not summed");
        assert_eq!(out[0].get("ma"), Some(&Bson::Null));
        assert_eq!(out[0].get("sd"), Some(&Bson::Int32(0)), "nor a document");
        assert_eq!(out[0].get("md"), Some(&Bson::Null));
        assert_eq!(out[0].get("n"), Some(&Bson::Int32(4)), "`$sum: 1` counts as ever");

        let decimals = vec![doc! { "v": dec("2.5") }, doc! { "v": 1 }, doc! { "v": dec("2.50") }];
        let out = run(
            vec![doc! { "$group": {
                "_id": null,
                "lo": { "$min": "$v" }, "hi": { "$max": "$v" },
                "first": { "$first": "$v" }, "last": { "$last": "$v" },
                "all": { "$push": "$v" }, "set": { "$addToSet": "$v" },
                "n": { "$sum": 1 },
            } }],
            decimals.clone(),
        );
        assert_eq!(out[0].get("lo"), Some(&Bson::Int32(1)));
        assert_eq!(out[0].get("hi"), Some(&dec("2.5")));
        assert_eq!(out[0].get("first"), Some(&dec("2.5")));
        assert_eq!(out[0].get("last"), Some(&dec("2.50")));
        assert_eq!(
            out[0].get_array("all").unwrap(),
            &vec![dec("2.5"), Bson::Int32(1), dec("2.50")]
        );
        assert_eq!(out[0].get_array("set").unwrap(), &vec![dec("2.5"), Bson::Int32(1)]);
        assert_eq!(out[0].get("n"), Some(&Bson::Int32(3)));
        let counted = run(vec![doc! { "$count": "n" }], decimals);
        assert_eq!(counted, vec![doc! { "n": 3_i32 }]);
    }

    /// `$sum` and `$avg` are accumulators only: as expression operators, in
    /// `$project` or anywhere an expression is parsed, they are not built and
    /// are refused at parse whatever their operands, a decimal or an array
    /// holding one included. So there is no expression form to refuse a
    /// decimal in, and no array an expression sums.
    #[test]
    fn sum_and_avg_are_not_expression_operators() {
        for op in ["$sum", "$avg"] {
            for arg in [
                Bson::Array(vec![Bson::Int32(1), dec("2.5")]),
                Bson::String("$a".into()),
                Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]),
            ] {
                let mut expr = Document::new();
                expr.insert(op, arg.clone());
                let err = parse(&[doc! { "$project": { "r": expr } }]).unwrap_err().to_string();
                assert!(err.contains(op) && err.contains("not an expression operator"), "{err}");
            }
        }
        // The other arithmetic takes a decimal when it evaluates (ADR-252),
        // except `$mod`, which refuses one by name.
        for (op, want) in [
            ("$add", dec("2.5")),
            ("$subtract", dec("0.5")),
            ("$multiply", dec("1.5")),
            ("$divide", dec("1.5")),
        ] {
            let mut expr = Document::new();
            expr.insert(op, vec![Bson::from("$v"), Bson::Int32(1)]);
            let out = run(vec![doc! { "$project": { "r": expr } }], vec![doc! { "v": dec("1.5") }]);
            assert_eq!(out[0].get("r"), Some(&want), "{op}");
        }
        // `$mod` takes a decimal too: the exact remainder.
        let mut expr = Document::new();
        expr.insert("$mod", vec![Bson::from("$v"), Bson::Int32(1)]);
        let out = run(vec![doc! { "$project": { "r": expr } }], vec![doc! { "v": dec("1.5") }]);
        assert_eq!(out[0].get("r"), Some(&dec("0.5")));
    }

    /// A number re-spelled as an equal value of another type, or a decimal as
    /// another member of its cohort, where one exists: the transformations
    /// that must never change a value's identity.
    fn respell(value: &Bson, pick: u8) -> Bson {
        match value {
            Bson::Int32(n) => match pick % 3 {
                0 => Bson::Int64(i64::from(*n)),
                1 => dec(&format!("{n}.000")),
                _ => Bson::Double(f64::from(*n)),
            },
            Bson::Int64(n) => dec(&format!("{n}{}", ".0".repeat(usize::from(pick % 2)))),
            Bson::Decimal128(d) => {
                // `c E e` is `c·10^k E (e−k)` while the coefficient has room.
                let text = d.to_string();
                if text.contains("NaN") || text.contains("Inf") || text.contains('E') {
                    return value.clone();
                }
                let digits = text.chars().filter(char::is_ascii_digit).count();
                if digits + 1 > 34 {
                    return value.clone();
                }
                let padded =
                    if text.contains('.') { format!("{text}0") } else { format!("{text}.0") };
                dec(&padded)
            }
            Bson::Array(items) => {
                Bson::Array(items.iter().map(|v| respell(v, pick.wrapping_add(1))).collect())
            }
            Bson::Document(d) => Bson::Document(
                d.iter().map(|(k, v)| (k.clone(), respell(v, pick.wrapping_add(1)))).collect(),
            ),
            other => other.clone(),
        }
    }

    fn any_leaf() -> impl Strategy<Value = Bson> {
        let fixed = leaves();
        prop_oneof![
            (0..fixed.len()).prop_map(move |i| fixed[i].clone()),
            any::<i32>().prop_map(Bson::Int32),
            any::<i64>().prop_map(Bson::Int64),
            (any::<i64>(), -40i32..40).prop_map(|(c, e)| dec(&format!("{c}E{e}"))),
            (-1000i32..1000).prop_map(|n| Bson::Double(f64::from(n) / 8.0)),
            any::<u128>().prop_map(raw),
        ]
    }

    fn any_value() -> impl Strategy<Value = Bson> {
        any_leaf().prop_recursive(3, 24, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(Bson::Array),
                prop::collection::vec(("[ab]", inner), 0..3)
                    .prop_map(|fields| Bson::Document(fields.into_iter().collect())),
            ]
        })
    }

    proptest! {
        #[test]
        fn group_key_is_eq_over_generated_values(a in any_value(), b in any_value(), pick in any::<u8>()) {
            let same = respell(&a, pick);
            prop_assert!(equal(&a, &same), "the respelling changed the value: {:?} / {:?}", a, same);
            prop_assert_eq!(group_key(&a), group_key(&same), "{:?} / {:?}", a, same);
            prop_assert_eq!(group_key(&a) == group_key(&b), equal(&a, &b), "{:?} / {:?}", a, b);
            let (subset, union) = one_member(&a, &same);
            prop_assert!(subset && union, "{:?} / {:?}", a, same);
            prop_assert_eq!(one_member(&a, &b), (equal(&a, &b), equal(&a, &b)), "{:?} / {:?}", a, b);
        }
    }
}
