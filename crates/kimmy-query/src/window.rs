//! `$setWindowFields` (ADR-264): a value computed for every document from a window of the
//! documents around it in its partition.
//!
//! ```text
//! {$setWindowFields: {
//!   partitionBy: <expression>,            // optional; absent or constant = one partition
//!   sortBy: {<field>: 1 | -1, ...},       // optional; required by anything order-dependent
//!   output: {<field>: {<operator>: <args>, window: {documents: [lo, hi]}
//!                                                | {range: [lo, hi], unit: <unit>}}, ...}
//! }}
//! ```
//!
//! # How a run goes
//!
//! The stage is blocking. Every input document is buffered and filed under its partition
//! (the value of `partitionBy`, compared as `$group` compares keys); each partition is sorted
//! by `sortBy` (stable, the order `$sort` uses); then every output is computed for every
//! document of the partition from the **original** documents, so an output cannot read
//! another output of the same stage, and written into the document at the end. Documents leave
//! partition by partition in the order the partitions were first seen, and within a partition in
//! `sortBy` order (arrival order when there is no `sortBy`).
//!
//! # Windows
//!
//! A window is a pair of bounds, each `"unbounded"`, `"current"` or a number, and the documents
//! of the partition between them, ends included. A `documents` window counts positions from the
//! current document; a `range` window counts distance in the `sortBy` value (a number, or a date
//! with a `unit`), so every document tied with the current one is in it. With a descending
//! `sortBy` the bounds follow the sort order, not the number line: `[-1, 0]` is the previous
//! document in the sort and the current one, whichever way the numbers run.
//!
//! Because the bounds are relative to the current document, both ends of the window only ever
//! move forward as the current document does, so each document is added to the running state once
//! and removed once. `$sum`, `$avg` and `$count`, `$stdDevPop`, `$stdDevSamp` have an inverse
//! and slide in constant time per document; `$min` and `$max` keep a monotone deque; `$first` and
//! `$last` read one document; the window `[unbounded, current]` only ever adds, so it needs no
//! inverse at all. The rest (`$push`, `$addToSet`, `$top`, ...) are rebuilt from the documents of
//! the window when it loses one.
//!
//! # Memory
//!
//! Every document buffered, and every value produced, is charged to a byte total that is checked as
//! it grows ([`MAX_BUFFER_BYTES`], an [`Error::AnswerTooLarge`] the API answers `413` to). A cumulative
//! `$push` over a large partition, whose output is quadratic, is refused when its output passes the
//! cap and not after it has been built.

use std::collections::VecDeque;

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::{Error, Result, path};

use crate::accumulators;
use crate::aggregate::{
    AccState, Accumulator, Limits, accumulate, group_key, init, parse_accumulators,
};
use crate::expr::{Binding, Expr, Scope, Total, add_to_date, as_integer, type_name, unit_millis};
use crate::shape::{self, SortKey};

/// The most bytes one `$setWindowFields`, `$densify` or `$fill` holds: the documents it buffers
/// plus what it adds to them. A request over it is refused with a `413` as soon as the total passes
/// it, not after the work is done.
pub const MAX_BUFFER_BYTES: usize = 256 * 1024 * 1024;

/// The most removals an incrementally maintained standard deviation takes before its state is
/// rebuilt from the documents of the window, which bounds the drift that subtracting accumulates.
const REBUILD_AFTER_REMOVALS: usize = 1024;

/// How a stage splits its input.
#[derive(Clone, Debug)]
pub enum Partition {
    /// Everything is one partition.
    Whole,
    /// By the value of an expression.
    Expr(Expr),
    /// By the values of these fields (a missing field is not the same partition as a null one).
    Fields(Vec<String>),
}

/// The bounds of a window, in the units of the window kind.
#[derive(Clone, Debug, PartialEq)]
pub enum Window {
    /// The whole partition.
    Whole,
    /// Positions around the current document; `None` is `"unbounded"`.
    Documents(Option<i64>, Option<i64>),
    /// Distances in the `sortBy` value; `None` is `"unbounded"`. `unit` is set for a date.
    Range { lo: Option<f64>, hi: Option<f64>, unit: Option<String> },
}

/// What one output field computes.
#[derive(Clone, Debug)]
pub enum Func {
    /// A `$group` accumulator over a window.
    Acc {
        acc: Accumulator,
        window: Window,
    },
    Rank,
    DenseRank,
    DocumentNumber,
    Shift {
        output: Expr,
        by: i64,
        default: Option<Expr>,
    },
    Derivative {
        input: Expr,
        unit: Option<String>,
        window: Window,
    },
    Integral {
        input: Expr,
        unit: Option<String>,
        window: Window,
    },
    ExpMovingAvg {
        input: Expr,
        alpha: f64,
    },
    Locf(Expr),
    LinearFill(Expr),
}

/// One output field.
#[derive(Clone, Debug)]
pub struct Output {
    pub name: String,
    pub func: Func,
}

/// A parsed `$setWindowFields`.
#[derive(Clone, Debug)]
pub struct WindowSpec {
    pub partition: Partition,
    pub sort: Vec<SortKey>,
    pub outputs: Vec<Output>,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

fn bad<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::InvalidQuery(message.into()))
}

/// The window accumulators: the `$group` ones without `$mergeObjects`.
const ACCUMULATORS: [&str; 19] = [
    "$sum",
    "$avg",
    "$min",
    "$max",
    "$count",
    "$push",
    "$addToSet",
    "$first",
    "$last",
    "$stdDevPop",
    "$stdDevSamp",
    "$top",
    "$bottom",
    "$topN",
    "$bottomN",
    "$firstN",
    "$lastN",
    "$maxN",
    "$minN",
];

/// Parse `sortBy`: a non-empty document of `1` and `-1`.
pub(crate) fn parse_sort_by(stage: &str, value: &Bson) -> Result<Vec<SortKey>> {
    let Some(doc) = value.as_document() else {
        return bad(format!("{stage} `sortBy` must be a document, found {}", type_name(value)));
    };
    if doc.is_empty() {
        return bad(format!("{stage} `sortBy` must name at least one field"));
    }
    for (field, direction) in doc {
        if field.is_empty() || field.starts_with('$') {
            return bad(format!("{stage} `sortBy` names a field, not {field:?}"));
        }
        if shape::direction_of(direction).is_none() {
            return bad(format!("{stage} `sortBy` direction for {field:?} must be 1 or -1"));
        }
    }
    shape::parse_sort(doc)
}

/// Parse a field name an operator writes: not empty, no `$` start, no empty path segment.
pub(crate) fn check_output_name(stage: &str, name: &str) -> Result<()> {
    if name.is_empty()
        || name.starts_with('$')
        || name.contains('\0')
        || name.split('.').any(str::is_empty)
    {
        return bad(format!(
            "{stage} output field names cannot be empty, start with `$`, or hold an empty path \
             segment: {name:?}"
        ));
    }
    Ok(())
}

/// `{$setWindowFields: {partitionBy, sortBy, output}}`.
pub fn parse(spec: &Document, vars: &[String]) -> Result<WindowSpec> {
    for key in spec.keys() {
        if !matches!(key.as_str(), "partitionBy" | "sortBy" | "output") {
            return bad(format!(
                "$setWindowFields does not take `{key}`; its fields are partitionBy, sortBy and \
                 output"
            ));
        }
    }
    let partition = match spec.get("partitionBy") {
        None | Some(Bson::Null) => Partition::Whole,
        Some(value) => match Expr::parse_with_vars(value, vars)? {
            Expr::Literal(_) => Partition::Whole,
            expr => Partition::Expr(expr),
        },
    };
    let sort = match spec.get("sortBy") {
        None => Vec::new(),
        Some(value) => parse_sort_by("$setWindowFields", value)?,
    };
    let Some(output) = spec.get("output") else {
        return bad("$setWindowFields needs `output`: {<field>: {<operator>: <args>, window: ..}}");
    };
    let Some(output) = output.as_document() else {
        return bad(format!(
            "$setWindowFields `output` must be a document, found {}",
            type_name(output)
        ));
    };
    let mut outputs = Vec::with_capacity(output.len());
    for (name, value) in output {
        check_output_name("$setWindowFields", name)?;
        outputs.push(Output { name: name.clone(), func: parse_output(name, value, &sort, vars)? });
    }
    Ok(WindowSpec { partition, sort, outputs })
}

fn parse_output(name: &str, value: &Bson, sort: &[SortKey], vars: &[String]) -> Result<Func> {
    let Some(spec) = value.as_document() else {
        return bad(format!(
            "$setWindowFields output `{name}` must be a document naming a window operator, found {}",
            type_name(value)
        ));
    };
    let mut op: Option<(&String, &Bson)> = None;
    let mut window: Option<&Bson> = None;
    for (key, arg) in spec {
        if key == "window" {
            window = Some(arg);
        } else if key.starts_with('$') {
            if let Some((first, _)) = op {
                return bad(format!(
                    "$setWindowFields output `{name}` names two window operators, {first} and {key}"
                ));
            }
            op = Some((key, arg));
        } else {
            return bad(format!(
                "$setWindowFields output `{name}` has the unknown field `{key}`; it is one window \
                 operator and an optional `window`"
            ));
        }
    }
    let Some((op, arg)) = op else {
        return bad(format!("$setWindowFields output `{name}` names no window operator"));
    };
    let need_sort = |why: &str| -> Result<()> {
        if sort.is_empty() {
            return bad(format!("{op} requires a `sortBy`: {why}"));
        }
        Ok(())
    };
    let refuse_window = || -> Result<()> {
        if window.is_some() {
            return bad(format!(
                "{op} does not take a `window`: it is computed over the whole partition"
            ));
        }
        Ok(())
    };
    let single_sort = || -> Result<()> {
        if sort.len() != 1 {
            return bad(format!("{op} requires a `sortBy` of exactly one field"));
        }
        Ok(())
    };
    match op.as_str() {
        "$rank" | "$denseRank" | "$documentNumber" => {
            if !matches!(arg, Bson::Document(d) if d.is_empty()) {
                return bad(format!("{op} takes no arguments, i.e. {op}: {{}}"));
            }
            refuse_window()?;
            need_sort("it ranks the documents by their sort values")?;
            Ok(match op.as_str() {
                "$rank" => Func::Rank,
                "$denseRank" => Func::DenseRank,
                _ => Func::DocumentNumber,
            })
        }
        "$shift" => {
            let d = argument(op, arg, &["output", "by", "default"], &["output", "by"])?;
            refuse_window()?;
            need_sort("it counts documents in the sort order")?;
            let by = as_integer(&d["by"], "$shift `by`")?;
            let default = match d.get("default") {
                None => None,
                Some(v) => {
                    let expr = Expr::parse_with_vars(v, vars)?;
                    if expr.reads_document() {
                        return bad(
                            "$shift `default` must be a constant: it cannot read the document",
                        );
                    }
                    Some(expr)
                }
            };
            Ok(Func::Shift { output: Expr::parse_with_vars(&d["output"], vars)?, by, default })
        }
        "$derivative" | "$integral" => {
            let d = argument(op, arg, &["input", "unit"], &["input"])?;
            single_sort()?;
            let unit = match d.get("unit") {
                None => None,
                Some(Bson::String(u))
                    if matches!(
                        u.as_str(),
                        "week" | "day" | "hour" | "minute" | "second" | "millisecond"
                    ) =>
                {
                    Some(u.clone())
                }
                Some(other) => {
                    return bad(format!(
                        "{op} `unit` must be one of week, day, hour, minute, second or \
                         millisecond, found {}",
                        match other {
                            Bson::String(u) => format!("{u:?}"),
                            other => type_name(other).to_string(),
                        }
                    ));
                }
            };
            let Some(window) = window else {
                return bad(format!("{op} requires a `window`"));
            };
            let window = parse_window(op, window, sort)?;
            let input = Expr::parse_with_vars(&d["input"], vars)?;
            Ok(if op == "$derivative" {
                Func::Derivative { input, unit, window }
            } else {
                Func::Integral { input, unit, window }
            })
        }
        "$expMovingAvg" => {
            let d = argument(op, arg, &["input", "N", "alpha"], &["input"])?;
            refuse_window()?;
            need_sort("it weights documents by their position")?;
            let alpha = match (d.get("N"), d.get("alpha")) {
                (Some(_), Some(_)) => {
                    return bad("$expMovingAvg takes exactly one of `N` and `alpha`, found both");
                }
                (None, None) => return bad("$expMovingAvg requires `N` or `alpha`"),
                (Some(n), None) => {
                    let n = as_integer(n, "$expMovingAvg `N`")?;
                    if n < 1 {
                        return bad(format!("$expMovingAvg `N` must be at least 1, found {n}"));
                    }
                    2.0 / (n as f64 + 1.0)
                }
                (None, Some(a)) => match accumulators::numeric(a) {
                    Some(a) if a > 0.0 && a < 1.0 => a,
                    Some(a) => {
                        return bad(format!(
                            "$expMovingAvg `alpha` must be greater than 0 and less than 1, found {a}"
                        ));
                    }
                    None => {
                        return bad(format!(
                            "$expMovingAvg `alpha` must be a number, found {}",
                            type_name(a)
                        ));
                    }
                },
            };
            Ok(Func::ExpMovingAvg { input: Expr::parse_with_vars(&d["input"], vars)?, alpha })
        }
        "$locf" | "$linearFill" => {
            refuse_window()?;
            need_sort("it fills from the neighbours in the sort order")?;
            if op == "$linearFill" {
                single_sort()?;
            }
            let expr = Expr::parse_with_vars(arg, vars)?;
            Ok(if op == "$locf" { Func::Locf(expr) } else { Func::LinearFill(expr) })
        }
        "$covariancePop" | "$covarianceSamp" => bad(format!(
            "{op} is not supported: it is not built in this version (see docs/deviations.md)"
        )),
        op if ACCUMULATORS.contains(&op) => {
            let mut call = Document::new();
            call.insert(op, arg.clone());
            let mut wrapper = Document::new();
            wrapper.insert("x", call);
            let mut parsed = parse_accumulators("$setWindowFields", &wrapper, vars, false)?;
            let acc = parsed.remove(0).1;
            let window = match window {
                None => Window::Whole,
                Some(w) => parse_window(op, w, sort)?,
            };
            Ok(Func::Acc { acc, window })
        }
        other => bad(format!(
            "{other} is not a window operator; supported: {}, $rank, $denseRank, $documentNumber, \
             $shift, $derivative, $integral, $expMovingAvg, $locf, $linearFill",
            ACCUMULATORS.join(", ")
        )),
    }
}

/// The argument document of an operator: the `allowed` fields only, and all of `required`.
fn argument<'a>(
    op: &str,
    arg: &'a Bson,
    allowed: &[&str],
    required: &[&str],
) -> Result<&'a Document> {
    let Some(d) = arg.as_document() else {
        return bad(format!("{op} requires an object as its argument, found {}", type_name(arg)));
    };
    for key in d.keys() {
        if !allowed.contains(&key.as_str()) {
            return bad(format!("{op} found an unknown argument: {key}"));
        }
    }
    for key in required {
        if !d.contains_key(key) {
            return bad(format!("{op} requires `{key}` to be specified"));
        }
    }
    Ok(d)
}

fn parse_window(op: &str, value: &Bson, sort: &[SortKey]) -> Result<Window> {
    let Some(w) = value.as_document() else {
        return bad(format!("{op} `window` must be a document, found {}", type_name(value)));
    };
    for key in w.keys() {
        if !matches!(key.as_str(), "documents" | "range" | "unit") {
            return bad(format!(
                "{op} `window` does not take `{key}`; it is `documents` or `range` (with `unit`)"
            ));
        }
    }
    match (w.get("documents"), w.get("range")) {
        (Some(_), Some(_)) => {
            bad(format!("{op} `window` takes one of `documents` and `range`, found both"))
        }
        (None, None) => bad(format!("{op} `window` needs `documents` or `range`")),
        (Some(docs), None) => {
            if w.contains_key("unit") {
                return bad(format!("{op} `window` `unit` goes with `range`, not `documents`"));
            }
            let [lo, hi] = bound_pair(op, "documents", docs)?;
            let one = |b: &Bson| -> Result<Option<i64>> {
                match b {
                    Bson::String(s) if s == "unbounded" => Ok(None),
                    Bson::String(s) if s == "current" => Ok(Some(0)),
                    Bson::String(s) => bad(format!(
                        "{op} `window` bound must be \"unbounded\", \"current\" or a whole \
                         number, found {s:?}"
                    )),
                    other => as_integer(other, &format!("{op} `window` documents bound")).map(Some),
                }
            };
            let (lo, hi) = (one(lo)?, one(hi)?);
            if let (Some(l), Some(h)) = (lo, hi)
                && l > h
            {
                return bad(format!(
                    "{op} `window` lower bound {l} must not be above the upper bound {h}"
                ));
            }
            Ok(Window::Documents(lo, hi))
        }
        (None, Some(range)) => {
            let unit = match w.get("unit") {
                None => None,
                Some(Bson::String(u)) if crate::expr::is_date_unit(u) => Some(u.clone()),
                Some(other) => {
                    return bad(format!(
                        "{op} `window` `unit` must be one of year, quarter, month, week, day, \
                         hour, minute, second, millisecond, found {}",
                        match other {
                            Bson::String(u) => format!("{u:?}"),
                            other => type_name(other).to_string(),
                        }
                    ));
                }
            };
            if sort.len() != 1 {
                return bad(format!(
                    "{op} with a range window requires a `sortBy` of exactly one field"
                ));
            }
            let [lo, hi] = bound_pair(op, "range", range)?;
            let whole_unit = matches!(unit.as_deref(), Some("year" | "quarter" | "month"));
            let one = |b: &Bson| -> Result<Option<f64>> {
                match b {
                    Bson::String(s) if s == "unbounded" => Ok(None),
                    Bson::String(s) if s == "current" => Ok(Some(0.0)),
                    Bson::String(s) => bad(format!(
                        "{op} `window` bound must be \"unbounded\", \"current\" or a number, \
                         found {s:?}"
                    )),
                    other => match accumulators::numeric(other) {
                        Some(x) if x.is_finite() => {
                            if whole_unit && x.fract() != 0.0 {
                                return bad(format!(
                                    "{op} `window` range bounds in {} must be whole numbers, \
                                     found {x}",
                                    unit.as_deref().unwrap_or("")
                                ));
                            }
                            Ok(Some(x))
                        }
                        _ => bad(format!(
                            "{op} `window` range bound must be \"unbounded\", \"current\" or a \
                             number, found {}",
                            type_name(other)
                        )),
                    },
                }
            };
            let (lo, hi) = (one(lo)?, one(hi)?);
            if let (Some(l), Some(h)) = (lo, hi)
                && l > h
            {
                return bad(format!(
                    "{op} `window` lower bound {l} must not be above the upper bound {h}"
                ));
            }
            Ok(Window::Range { lo, hi, unit })
        }
    }
}

fn bound_pair<'a>(op: &str, kind: &str, value: &'a Bson) -> Result<[&'a Bson; 2]> {
    match value {
        Bson::Array(items) if items.len() == 2 => Ok([&items[0], &items[1]]),
        _ => bad(format!("{op} window `{kind}` must be an array of two bounds, [lower, upper]")),
    }
}

// ---------------------------------------------------------------------------
// The byte budget
// ---------------------------------------------------------------------------

/// A running total of the bytes a stage holds, checked as it grows.
pub(crate) struct Budget {
    used: usize,
    cap: usize,
    stage: &'static str,
}

impl Budget {
    pub(crate) fn new(stage: &'static str, cap: usize) -> Self {
        Self { used: 0, cap, stage }
    }

    pub(crate) fn charge(&mut self, bytes: usize) -> Result<()> {
        self.used = self.used.saturating_add(bytes);
        if self.used > self.cap {
            return Err(Error::AnswerTooLarge {
                what: self.stage,
                bytes: self.used,
                max: self.cap,
                remedy: "narrow the pipeline with an earlier $match or $project, or partition \
                         the input into smaller pieces",
            });
        }
        Ok(())
    }

    pub(crate) fn charge_doc(&mut self, doc: &Document) -> Result<()> {
        let bytes = bson::serialize_to_vec(doc)
            .map_err(|e| Error::Internal(format!("{} could not size a document: {e}", self.stage)))?
            .len();
        self.charge(bytes)
    }
}

/// The bytes a field adds to a document: type byte, name, terminator, value.
fn element_bytes(name: &str, value: &Bson) -> usize {
    let body = match value {
        Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey => 0,
        Bson::Boolean(_) => 1,
        Bson::Int32(_) => 4,
        Bson::Int64(_) | Bson::Double(_) | Bson::DateTime(_) | Bson::Timestamp(_) => 8,
        Bson::ObjectId(_) => 12,
        Bson::Decimal128(_) => 16,
        Bson::String(s) => 4 + s.len() + 1,
        other => bson::serialize_to_vec(&bson::doc! { "": other.clone() })
            .map_or(16, |v| v.len().saturating_sub(5 + 1)),
    };
    1 + name.len() + 1 + body
}

// ---------------------------------------------------------------------------
// Running
// ---------------------------------------------------------------------------

/// The values one output takes, a document each.
enum Cells {
    Same(Bson),
    Per(Vec<Bson>),
}

impl Cells {
    fn at(&self, i: usize) -> &Bson {
        match self {
            Cells::Same(v) => v,
            Cells::Per(v) => &v[i],
        }
    }
}

struct Sink<'a> {
    budget: &'a mut Budget,
    name: &'a str,
}

impl Sink<'_> {
    fn charge(&mut self, value: &Bson) -> Result<()> {
        self.budget.charge(element_bytes(self.name, value))
    }
}

struct Run<'a, 'v> {
    vars: &'a [Binding<'v>],
    limits: &'a Limits,
}

fn eval(expr: &Expr, doc: &Document, vars: &[Binding<'_>]) -> Result<Bson> {
    expr.eval_in(&Scope::with_bindings(doc, vars))
}

/// The partition key of a document.
pub(crate) fn partition_key(
    partition: &Partition,
    doc: &Document,
    vars: &[Binding<'_>],
) -> Result<Vec<u8>> {
    Ok(match partition {
        Partition::Whole => Vec::new(),
        Partition::Expr(e) => group_key(&eval(e, doc, vars)?),
        Partition::Fields(fields) => group_key(&Bson::Document(fields_of(doc, fields))),
    })
}

/// The partition fields a document has, as a document of them (nested for a dotted name).
pub(crate) fn fields_of(doc: &Document, fields: &[String]) -> Document {
    let mut out = Document::new();
    for field in fields {
        if let Some(v) = path::resolve(doc, field).into_iter().next() {
            // A dotted name addresses a place, so it is set as one; the encoded key of
            // `{a: {b: 1}}` is the same whichever document it came from.
            let _ = path::set(&mut out, field, v.clone());
        }
    }
    out
}

/// Run a parsed `$setWindowFields`.
pub fn apply(
    spec: &WindowSpec,
    input: Vec<Document>,
    limits: &Limits,
    vars: &[Binding<'_>],
) -> Result<Vec<Document>> {
    apply_capped(spec, input, limits, vars, MAX_BUFFER_BYTES)
}

/// [`apply`] with the byte cap given, for tests.
pub fn apply_capped(
    spec: &WindowSpec,
    input: Vec<Document>,
    limits: &Limits,
    vars: &[Binding<'_>],
    cap: usize,
) -> Result<Vec<Document>> {
    let mut budget = Budget::new("$setWindowFields", cap);
    let partitions = partitions(&spec.partition, input, vars, &mut budget)?;
    let run = Run { vars, limits };
    let mut out = Vec::new();
    for docs in partitions {
        out.extend(run_partition(spec, docs, &run, &mut budget)?);
    }
    Ok(out)
}

/// The input filed under its partitions in first-seen order, each charged to the budget.
pub(crate) fn partitions(
    partition: &Partition,
    input: Vec<Document>,
    vars: &[Binding<'_>],
    budget: &mut Budget,
) -> Result<Vec<Vec<Document>>> {
    let mut order: Vec<Vec<Document>> = Vec::new();
    let mut index: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
    for doc in input {
        budget.charge_doc(&doc)?;
        let slot = if matches!(partition, Partition::Whole) {
            if order.is_empty() {
                order.push(Vec::new());
            }
            0
        } else {
            let key = partition_key(partition, &doc, vars)?;
            *index.entry(key).or_insert_with(|| {
                order.push(Vec::new());
                order.len() - 1
            })
        };
        order[slot].push(doc);
    }
    Ok(order)
}

fn run_partition(
    spec: &WindowSpec,
    docs: Vec<Document>,
    run: &Run<'_, '_>,
    budget: &mut Budget,
) -> Result<Vec<Document>> {
    // Sorted once, on the values taken once.
    let mut keyed: Vec<(Vec<Bson>, Document)> =
        docs.into_iter().map(|d| (shape::sort_keys(&spec.sort, &d), d)).collect();
    if !spec.sort.is_empty() {
        keyed.sort_by(|a, b| shape::compare_keys(&spec.sort, &a.0, &b.0));
    }
    let (keys, mut docs): (Vec<Vec<Bson>>, Vec<Document>) = keyed.into_iter().unzip();

    let mut results: Vec<Cells> = Vec::with_capacity(spec.outputs.len());
    for output in &spec.outputs {
        let mut sink = Sink { budget, name: &output.name };
        results.push(compute(&output.func, &docs, &keys, &spec.sort, run, &mut sink)?);
    }
    for (i, doc) in docs.iter_mut().enumerate() {
        for (output, cells) in spec.outputs.iter().zip(&results) {
            path::set(doc, &output.name, cells.at(i).clone())
                .map_err(|e| Error::InvalidQuery(format!("cannot set {:?}: {e}", output.name)))?;
        }
    }
    Ok(docs)
}

fn compute(
    func: &Func,
    docs: &[Document],
    keys: &[Vec<Bson>],
    sort: &[SortKey],
    run: &Run<'_, '_>,
    sink: &mut Sink<'_>,
) -> Result<Cells> {
    let n = docs.len();
    match func {
        Func::Acc { acc, window } => {
            let ends = endpoints(acc_name(acc), window, n, keys, sort)?;
            accumulator(acc, docs, &ends, run, sink)
        }
        Func::Rank | Func::DenseRank | Func::DocumentNumber => {
            let (mut rank, mut dense) = (0i64, 0i64);
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                if i == 0 || shape::compare_keys(sort, &keys[i - 1], &keys[i]).is_ne() {
                    rank = i as i64 + 1;
                    dense += 1;
                }
                let v = Bson::Int64(match func {
                    Func::Rank => rank,
                    Func::DenseRank => dense,
                    _ => i as i64 + 1,
                });
                sink.charge(&v)?;
                out.push(v);
            }
            Ok(Cells::Per(out))
        }
        Func::Shift { output, by, default } => {
            let default = match default {
                Some(e) => eval(e, &docs[0], run.vars)?,
                None => Bson::Null,
            };
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let at = i as i128 + i128::from(*by);
                let v = if at >= 0 && (at as usize) < n {
                    eval(output, &docs[at as usize], run.vars)?
                } else {
                    default.clone()
                };
                sink.charge(&v)?;
                out.push(v);
            }
            Ok(Cells::Per(out))
        }
        Func::Derivative { input, unit, window } => {
            let ends = endpoints("$derivative", window, n, keys, sort)?;
            let x = axis_values("$derivative", keys, unit.as_deref())?;
            let y = numeric_inputs(input, docs, run.vars)?;
            let mut out = Vec::with_capacity(n);
            for &(s, e) in &ends {
                let v = if e - s < 2 {
                    Bson::Null
                } else {
                    y.check("$derivative", s, e)?;
                    let dx = x[e - 1] - x[s];
                    if dx == 0.0 {
                        Bson::Null
                    } else {
                        Bson::Double((y.values[e - 1] - y.values[s]) / dx)
                    }
                };
                sink.charge(&v)?;
                out.push(v);
            }
            Ok(Cells::Per(out))
        }
        Func::Integral { input, unit, window } => {
            let ends = endpoints("$integral", window, n, keys, sort)?;
            let x = axis_values("$integral", keys, unit.as_deref())?;
            let y = numeric_inputs(input, docs, run.vars)?;
            // The area under the line through each neighbouring pair, summed from the start;
            // a window is the difference of two sums.
            let mut prefix = vec![0.0f64; n];
            for k in 1..n {
                prefix[k] =
                    prefix[k - 1] + (x[k] - x[k - 1]) * (y.values[k - 1] + y.values[k]) / 2.0;
            }
            let mut out = Vec::with_capacity(n);
            for &(s, e) in &ends {
                let v = if e - s < 2 {
                    Bson::Double(0.0)
                } else {
                    y.check("$integral", s, e)?;
                    Bson::Double(prefix[e - 1] - prefix[s])
                };
                sink.charge(&v)?;
                out.push(v);
            }
            Ok(Cells::Per(out))
        }
        Func::ExpMovingAvg { input, alpha } => {
            let mut ema: Option<f64> = None;
            let mut out = Vec::with_capacity(n);
            for doc in docs {
                let v = match accumulators::numeric(&eval(input, doc, run.vars)?) {
                    Some(x) => {
                        let next = match ema {
                            None => x,
                            Some(prev) => alpha * x + (1.0 - alpha) * prev,
                        };
                        ema = Some(next);
                        Bson::Double(next)
                    }
                    None => Bson::Null,
                };
                sink.charge(&v)?;
                out.push(v);
            }
            Ok(Cells::Per(out))
        }
        Func::Locf(expr) => {
            let mut last = Bson::Null;
            let mut out = Vec::with_capacity(n);
            for doc in docs {
                let v = eval(expr, doc, run.vars)?;
                if !matches!(v, Bson::Null) {
                    last = v.clone();
                    sink.charge(&v)?;
                    out.push(v);
                } else {
                    sink.charge(&last)?;
                    out.push(last.clone());
                }
            }
            Ok(Cells::Per(out))
        }
        Func::LinearFill(expr) => {
            let x = axis_values("$linearFill", keys, None)?;
            let mut values = Vec::with_capacity(n);
            for doc in docs {
                let v = eval(expr, doc, run.vars)?;
                if !matches!(v, Bson::Null) && accumulators::numeric(&v).is_none() {
                    return bad(format!("$linearFill needs numbers, found {}", type_name(&v)));
                }
                values.push(v);
            }
            // The nearest value before each position and after it.
            let mut before: Vec<Option<usize>> = vec![None; n];
            let mut last = None;
            for i in 0..n {
                if !matches!(values[i], Bson::Null) {
                    last = Some(i);
                }
                before[i] = last;
            }
            let mut after: Vec<Option<usize>> = vec![None; n];
            let mut next = None;
            for i in (0..n).rev() {
                if !matches!(values[i], Bson::Null) {
                    next = Some(i);
                }
                after[i] = next;
            }
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let v = if !matches!(values[i], Bson::Null) {
                    values[i].clone()
                } else if let (Some(a), Some(b)) = (before[i], after[i]) {
                    let num = |j: usize| accumulators::numeric(&values[j]).unwrap_or(f64::NAN);
                    let (y0, y1) = (num(a), num(b));
                    if x[b] == x[a] {
                        Bson::Double(y0)
                    } else {
                        Bson::Double(y0 + (y1 - y0) * ((x[i] - x[a]) / (x[b] - x[a])))
                    }
                } else {
                    Bson::Null
                };
                sink.charge(&v)?;
                out.push(v);
            }
            Ok(Cells::Per(out))
        }
    }
}

fn acc_name(acc: &Accumulator) -> &'static str {
    match acc {
        Accumulator::Sum(_) => "$sum",
        Accumulator::Avg(_) => "$avg",
        Accumulator::Min(_) => "$min",
        Accumulator::Max(_) => "$max",
        Accumulator::First(_) => "$first",
        Accumulator::Last(_) => "$last",
        Accumulator::Push(_) => "$push",
        Accumulator::AddToSet(_) => "$addToSet",
        Accumulator::MergeObjects(_) => "$mergeObjects",
        Accumulator::Ext(_) => "a window accumulator",
    }
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

/// The sort values of a partition as numbers on one axis.
enum Axis {
    Num(Vec<f64>),
    Date(Vec<i64>),
}

fn axis(op: &str, keys: &[Vec<Bson>]) -> Result<Axis> {
    let mut nums: Vec<f64> = Vec::with_capacity(keys.len());
    let mut dates: Vec<i64> = Vec::new();
    for key in keys {
        match &key[0] {
            Bson::DateTime(_) if !nums.is_empty() => {
                return bad(format!(
                    "{op} needs the sortBy field to be all numbers or all dates, found both"
                ));
            }
            Bson::DateTime(d) => dates.push(d.timestamp_millis()),
            other if dates.is_empty() => match accumulators::numeric(other) {
                Some(x) if !x.is_nan() => nums.push(x),
                _ => {
                    return bad(format!(
                        "{op} needs the sortBy field to be a number or a date in every document, \
                         found {}",
                        match other {
                            Bson::Null => "null or a missing value",
                            other => type_name(other),
                        }
                    ));
                }
            },
            _ => {
                return bad(format!(
                    "{op} needs the sortBy field to be all numbers or all dates, found both"
                ));
            }
        }
    }
    Ok(if dates.is_empty() { Axis::Num(nums) } else { Axis::Date(dates) })
}

/// The sort values as numbers for `$derivative`, `$integral` and `$linearFill`: a number as it
/// is, a date in `unit`s (milliseconds when there is none).
fn axis_values(op: &str, keys: &[Vec<Bson>], unit: Option<&str>) -> Result<Vec<f64>> {
    Ok(match axis(op, keys)? {
        Axis::Num(v) => {
            if unit.is_some() {
                return bad(format!(
                    "{op} `unit` is for a date sortBy field, and this one is numeric"
                ));
            }
            v
        }
        Axis::Date(v) => {
            let per = unit.and_then(unit_millis).unwrap_or(1) as f64;
            v.into_iter().map(|ms| ms as f64 / per).collect()
        }
    })
}

/// The numeric input of every document, and which are not numeric.
struct Inputs {
    values: Vec<f64>,
    /// `bad_before[i]` is how many of the first `i` are not numeric.
    bad_before: Vec<usize>,
    first_bad: Option<String>,
}

impl Inputs {
    fn check(&self, op: &str, s: usize, e: usize) -> Result<()> {
        if self.bad_before[e] > self.bad_before[s] {
            return bad(format!(
                "{op} needs a numeric input in every document of its window, found {}",
                self.first_bad.as_deref().unwrap_or("a non-numeric value")
            ));
        }
        Ok(())
    }
}

fn numeric_inputs(input: &Expr, docs: &[Document], vars: &[Binding<'_>]) -> Result<Inputs> {
    let mut values = Vec::with_capacity(docs.len());
    let mut bad_before = Vec::with_capacity(docs.len() + 1);
    bad_before.push(0);
    let mut first_bad = None;
    let mut count = 0;
    for doc in docs {
        let v = eval(input, doc, vars)?;
        match accumulators::numeric(&v) {
            Some(x) => values.push(x),
            None => {
                count += 1;
                values.push(0.0);
                first_bad.get_or_insert_with(|| match &v {
                    Bson::Null => "null or a missing value".to_string(),
                    other => type_name(other).to_string(),
                });
            }
        }
        bad_before.push(count);
    }
    Ok(Inputs { values, bad_before, first_bad })
}

/// For each position, the half-open run `[start, end)` of the partition its window covers.
/// Both ends never decrease as the position moves on.
fn endpoints(
    op: &str,
    window: &Window,
    n: usize,
    keys: &[Vec<Bson>],
    sort: &[SortKey],
) -> Result<Vec<(usize, usize)>> {
    match window {
        Window::Whole => Ok(vec![(0, n); n]),
        Window::Documents(lo, hi) => Ok((0..n)
            .map(|i| {
                let clamp = |v: i128| v.clamp(0, n as i128) as usize;
                let s = lo.map_or(0, |l| clamp(i as i128 + i128::from(l)));
                let e = hi.map_or(n, |h| clamp(i as i128 + i128::from(h) + 1));
                (s, e.max(s))
            })
            .collect()),
        Window::Range { lo, hi, unit } => {
            let desc = sort[0].descending;
            let sign = if desc { -1.0 } else { 1.0 };
            match axis(op, keys)? {
                Axis::Num(v) => {
                    if unit.is_some() {
                        return bad(format!(
                            "{op}: a range window `unit` is for a date sortBy field, and this one \
                             is numeric"
                        ));
                    }
                    // `before(x, t)`: x comes before t in the sort order.
                    let before = |x: f64, t: f64| if desc { x > t } else { x < t };
                    Ok((0..n)
                        .map(|i| {
                            let s = lo.map_or(0, |l| {
                                let t = v[i] + sign * l;
                                v.partition_point(|&x| before(x, t))
                            });
                            let e = hi.map_or(n, |h| {
                                let t = v[i] + sign * h;
                                v.partition_point(|&x| !before(t, x))
                            });
                            (s, e.max(s))
                        })
                        .collect())
                }
                Axis::Date(v) => {
                    let Some(unit) = unit else {
                        return bad(format!(
                            "{op}: a range window over a date sortBy field needs a `unit`"
                        ));
                    };
                    // The calendar units step by the calendar; the rest are lengths of time,
                    // which may be fractional.
                    let shift = |from: i64, off: f64| -> Result<i64> {
                        let off = sign * off;
                        match unit_millis(unit) {
                            Some(len) => add_to_date(
                                from,
                                "millisecond",
                                (off * len as f64).round() as i64,
                                op,
                            ),
                            None => add_to_date(from, unit, off as i64, op),
                        }
                    };
                    let before = |x: i64, t: i64| if desc { x > t } else { x < t };
                    let mut out = Vec::with_capacity(n);
                    for i in 0..n {
                        let s = match lo {
                            None => 0,
                            Some(l) => {
                                let t = shift(v[i], *l)?;
                                v.partition_point(|&x| before(x, t))
                            }
                        };
                        let e = match hi {
                            None => n,
                            Some(h) => {
                                let t = shift(v[i], *h)?;
                                v.partition_point(|&x| !before(t, x))
                            }
                        };
                        out.push((s, e.max(s)));
                    }
                    Ok(out)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Accumulators over windows
// ---------------------------------------------------------------------------

/// The accumulator's answer without consuming its state.
fn peek(state: &AccState) -> Result<Bson> {
    Ok(match state {
        AccState::Sum(total) => total.to_bson(),
        AccState::Avg(mean) => mean.to_bson(),
        AccState::MinMax(v) | AccState::First(v) => v.clone().unwrap_or(Bson::Null),
        AccState::Last(v) => match v {
            Some(Ok(v)) => v.clone(),
            Some(Err(e)) => return Err(Error::InvalidQuery(e.to_string())),
            None => Bson::Null,
        },
        AccState::Push(items) | AccState::AddToSet(items, _) => Bson::Array(items.clone()),
        AccState::MergeObjects(doc) => Bson::Document(doc.clone()),
        AccState::Ext(state) => state.clone().finish(),
    })
}

/// The accumulator over exactly the documents `docs`, from nothing.
fn rebuild(acc: &Accumulator, docs: &[Document], run: &Run<'_, '_>) -> Result<Bson> {
    let field = (String::new(), acc.clone());
    let mut state = init(&field);
    for doc in docs {
        accumulate(&mut state, acc, &Scope::with_bindings(doc, run.vars))?;
    }
    peek(&state)
}

fn accumulator(
    acc: &Accumulator,
    docs: &[Document],
    ends: &[(usize, usize)],
    run: &Run<'_, '_>,
    sink: &mut Sink<'_>,
) -> Result<Cells> {
    let n = docs.len();
    if ends.iter().all(|&e| e == (0, n)) {
        // The same answer for every document: computed once, charged for every copy.
        let v = rebuild(acc, docs, run)?;
        sink.budget.charge(element_bytes(sink.name, &v).saturating_mul(n))?;
        return Ok(Cells::Same(v));
    }
    let edge = |expr: &Expr, at: Option<usize>| -> Result<Bson> {
        match at {
            Some(i) => eval(expr, &docs[i], run.vars),
            None => Ok(Bson::Null),
        }
    };
    let mut out: Vec<Bson> = Vec::with_capacity(n);
    match acc {
        Accumulator::First(expr) => {
            for &(s, e) in ends {
                let v = edge(expr, (s < e).then_some(s))?;
                sink.charge(&v)?;
                out.push(v);
            }
        }
        Accumulator::Last(expr) => {
            for &(s, e) in ends {
                let v = edge(expr, (s < e).then(|| e - 1))?;
                sink.charge(&v)?;
                out.push(v);
            }
        }
        Accumulator::Sum(expr) | Accumulator::Avg(expr) => {
            let mut slider = SumSlider {
                acc,
                expr,
                avg: matches!(acc, Accumulator::Avg(_)),
                st: NumState::default(),
                run,
            };
            drive(&mut slider, docs, ends, sink, &mut out)?;
        }
        Accumulator::Min(expr) | Accumulator::Max(expr) => {
            let mut slider = ExtremeSlider {
                expr,
                max: matches!(acc, Accumulator::Max(_)),
                deque: VecDeque::new(),
                run,
            };
            drive(&mut slider, docs, ends, sink, &mut out)?;
        }
        _ => {
            let std_dev = matches!(
                acc,
                Accumulator::Ext(s) if matches!(
                    s.kind,
                    accumulators::Kind::StdDevPop | accumulators::Kind::StdDevSamp
                )
            );
            let field = (String::new(), acc.clone());
            let mut slider = GenericSlider {
                acc,
                state: init(&field),
                field,
                dirty: false,
                removals: 0,
                std_dev,
                run,
            };
            drive(&mut slider, docs, ends, sink, &mut out)?;
        }
    }
    Ok(Cells::Per(out))
}

/// A running state over a window that moves forward.
trait Slider {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()>;
    fn remove(&mut self, i: usize, docs: &[Document]) -> Result<()>;
    fn reset(&mut self);
    /// The answer for the window `[s, e)`, which the state holds.
    fn value(&mut self, docs: &[Document], s: usize, e: usize) -> Result<Bson>;
}

fn drive(
    slider: &mut dyn Slider,
    docs: &[Document],
    ends: &[(usize, usize)],
    sink: &mut Sink<'_>,
    out: &mut Vec<Bson>,
) -> Result<()> {
    let mut at = (0usize, 0usize);
    let mut previous: Option<((usize, usize), Bson)> = None;
    for &(s, e) in ends {
        if let Some((window, v)) = &previous
            && *window == (s, e)
        {
            let v = v.clone();
            sink.charge(&v)?;
            out.push(v);
            continue;
        }
        if s >= at.1 {
            slider.reset();
            at = (s, s);
        }
        while at.1 < e {
            slider.add(at.1, docs)?;
            at.1 += 1;
        }
        while at.0 < s {
            slider.remove(at.0, docs)?;
            at.0 += 1;
        }
        let v = slider.value(docs, s, e)?;
        sink.charge(&v)?;
        previous = Some(((s, e), v.clone()));
        out.push(v);
    }
    Ok(())
}

/// Any accumulator: added to as the window grows, rebuilt when it loses a document the state
/// cannot give back.
struct GenericSlider<'a, 'v> {
    acc: &'a Accumulator,
    field: (String, Accumulator),
    state: AccState,
    dirty: bool,
    removals: usize,
    std_dev: bool,
    run: &'a Run<'a, 'v>,
}

impl Slider for GenericSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        if !self.dirty {
            let scope = Scope::with_bindings(&docs[i], self.run.vars);
            accumulate(&mut self.state, self.acc, &scope)?;
        }
        Ok(())
    }

    fn remove(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        if self.dirty {
            return Ok(());
        }
        let scope = Scope::with_bindings(&docs[i], self.run.vars);
        let removed = match (&mut self.state, self.acc) {
            (AccState::Ext(state), Accumulator::Ext(spec)) => state.remove(spec, &scope)?,
            _ => false,
        };
        if removed {
            self.removals += 1;
            if self.std_dev && self.removals >= REBUILD_AFTER_REMOVALS {
                self.dirty = true;
            }
        } else {
            self.dirty = true;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.state = init(&self.field);
        self.dirty = false;
        self.removals = 0;
    }

    fn value(&mut self, docs: &[Document], s: usize, e: usize) -> Result<Bson> {
        if self.dirty {
            self.reset();
            for doc in &docs[s..e] {
                let scope = Scope::with_bindings(doc, self.run.vars);
                accumulate(&mut self.state, self.acc, &scope)?;
            }
        }
        peek(&self.state)
    }
}

/// `$sum` and `$avg`: exact integer and compensated double totals that give a document back. A
/// window holding a `Decimal128`, a non-finite double, or a total beyond a 64-bit integer is
/// rebuilt with the group accumulator, which handles those.
#[derive(Default)]
struct NumState {
    ints: i128,
    abs: i128,
    n_int: usize,
    sum: f64,
    comp: f64,
    n_dbl: usize,
    special: usize,
}

impl NumState {
    fn apply(&mut self, v: &Bson, sign: i64) {
        let k = |sum: &mut f64, comp: &mut f64, x: f64| {
            let t = *sum + x;
            if sum.abs() >= x.abs() {
                *comp += (*sum - t) + x;
            } else {
                *comp += (x - t) + *sum;
            }
            *sum = t;
        };
        let count = |n: &mut usize| {
            if sign > 0 { *n += 1 } else { *n -= 1 }
        };
        match v {
            Bson::Int32(_) | Bson::Int64(_) => {
                let x = match v {
                    Bson::Int32(n) => i128::from(*n),
                    Bson::Int64(n) => i128::from(*n),
                    _ => 0,
                };
                self.ints += x * i128::from(sign);
                self.abs += x.abs() * i128::from(sign);
                count(&mut self.n_int);
            }
            Bson::Double(d) if d.is_finite() => {
                k(&mut self.sum, &mut self.comp, *d * sign as f64);
                count(&mut self.n_dbl);
            }
            Bson::Double(_) | Bson::Decimal128(_) => count(&mut self.special),
            _ => {}
        }
    }
}

struct SumSlider<'a, 'v> {
    acc: &'a Accumulator,
    expr: &'a Expr,
    avg: bool,
    st: NumState,
    run: &'a Run<'a, 'v>,
}

impl Slider for SumSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        let v = eval(self.expr, &docs[i], self.run.vars)?;
        self.st.apply(&v, 1);
        Ok(())
    }

    fn remove(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        let v = eval(self.expr, &docs[i], self.run.vars)?;
        self.st.apply(&v, -1);
        Ok(())
    }

    fn reset(&mut self) {
        self.st = NumState::default();
    }

    fn value(&mut self, docs: &[Document], s: usize, e: usize) -> Result<Bson> {
        let st = &self.st;
        if st.special > 0 || st.abs > i128::from(i64::MAX) {
            return rebuild(self.acc, &docs[s..e], self.run);
        }
        let count = st.n_int + st.n_dbl;
        let total = st.ints as f64 + (st.sum + st.comp);
        Ok(if self.avg {
            if count == 0 { Bson::Null } else { Bson::Double(total / count as f64) }
        } else if st.n_dbl > 0 {
            Bson::Double(total)
        } else {
            let mut t = Total::default();
            t.add(&Bson::Int64(st.ints as i64));
            t.to_bson()
        })
    }
}

/// `$min` and `$max`: the candidates that could still be the answer, best first.
struct ExtremeSlider<'a, 'v> {
    expr: &'a Expr,
    max: bool,
    deque: VecDeque<(usize, Bson)>,
    run: &'a Run<'a, 'v>,
}

impl Slider for ExtremeSlider<'_, '_> {
    fn add(&mut self, i: usize, docs: &[Document]) -> Result<()> {
        let v = eval(self.expr, &docs[i], self.run.vars)?;
        if matches!(v, Bson::Null) {
            return Ok(());
        }
        // A candidate the newcomer beats strictly can never be the answer again; an equal
        // one stays, ahead of it, as the first of equals does in `$group`.
        while let Some((_, back)) = self.deque.back() {
            let o = canonical_cmp(back, &v);
            if (self.max && o.is_lt()) || (!self.max && o.is_gt()) {
                self.deque.pop_back();
            } else {
                break;
            }
        }
        self.deque.push_back((i, v));
        Ok(())
    }

    fn remove(&mut self, i: usize, _: &[Document]) -> Result<()> {
        if self.deque.front().is_some_and(|(at, _)| *at == i) {
            self.deque.pop_front();
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.deque.clear();
    }

    fn value(&mut self, _: &[Document], _: usize, _: usize) -> Result<Bson> {
        Ok(self.deque.front().map_or(Bson::Null, |(_, v)| v.clone()))
    }
}
