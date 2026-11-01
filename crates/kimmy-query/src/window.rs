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
//! partition by partition, the partitions in ascending order of the value of `partitionBy`
//! (null and missing are one partition; `1` and `1.0` are one), and within a partition in
//! `sortBy` order (arrival order when there is no `sortBy`).
//!
//! # Windows
//!
//! A window is a pair of bounds, each `"unbounded"`, `"current"` or a number, and the documents
//! of the partition between them, ends included. A `documents` window counts positions from the
//! current document. A `range` window counts distance in the one ascending `sortBy` field (a
//! number, or a date with a `unit`): a numeric bound includes every document tied with the
//! current one at that distance, and `"current"` is the current document itself, so
//! `[unbounded, current]` takes the tied documents that come before it and not those after.
//!
//! Because the bounds are relative to the current document, both ends of the window only ever
//! move forward as the current document does, so each document is added to the running state once
//! and removed once ([`crate::window_acc`] holds the states). The loops that walk a partition
//! are the places a request-deadline tick belongs.
//!
//! # Memory
//!
//! Every document buffered, every sort key taken and every value produced is charged to the
//! request's byte budget ([`crate::budget`]), as it grows, and the stage is refused `413` at
//! the first byte over. A cumulative `$push` over a large partition, whose output is
//! quadratic, is refused while it is made and not after.

use std::cmp::Ordering;

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::decimal::arith;
use kimmy_core::decimal::{Raw, decode_raw, pack_raw};
use kimmy_core::{Error, Result, path};

use crate::accumulators::{self, Kind};
use crate::aggregate::{Accumulator, Limits, group_key, parse_accumulators};
use crate::budget::Lease;
use crate::expr::{Binding, Expr, add_to_date, as_integer, type_name, unit_millis};
use crate::shape::{self, SortKey};
use crate::window_acc::{
    BestSlider, CovSlider, ExactDoubles, ExtremeSlider, Queue, QueueSlider, RankedSlider,
    SetSlider, Slider, StdDevSlider, SumSlider, eval, eval_present,
};

pub use crate::budget::MAX_BUFFER_BYTES;

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

/// One end of a `range` window.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RangeBound {
    Unbounded,
    /// The current document itself.
    Current,
    /// A distance from the current document's sort value.
    Offset(f64),
}

/// The bounds of a window, in the units of the window kind.
#[derive(Clone, Debug, PartialEq)]
pub enum Window {
    /// The whole partition.
    Whole,
    /// Positions around the current document; `None` is `"unbounded"`.
    Documents(Option<i64>, Option<i64>),
    /// Distances in the `sortBy` value. `unit` is set for a date.
    Range { lo: RangeBound, hi: RangeBound, unit: Option<String> },
}

/// What one output field computes.
#[derive(Clone, Debug)]
pub enum Func {
    /// A `$group` accumulator over a window.
    Acc {
        acc: Accumulator,
        window: Window,
    },
    /// `$covariancePop` and `$covarianceSamp` over a window.
    Covariance {
        exprs: Box<[Expr; 2]>,
        sample: bool,
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
    /// The stage that runs it: `$setWindowFields`, or `$fill` for a `$fill` with a method.
    pub stage: &'static str,
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

/// The window accumulators: the `$group` ones without `$mergeObjects`, which the reference
/// refuses too.
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

/// Parse `sortBy`: a document of `1` and `-1`; `{}` is no sort.
pub(crate) fn parse_sort_by(stage: &str, value: &Bson) -> Result<Vec<SortKey>> {
    let Some(doc) = value.as_document() else {
        return bad(format!("{stage} `sortBy` must be a document, found {}", type_name(value)));
    };
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
    Ok(WindowSpec { stage: "$setWindowFields", partition, sort, outputs })
}

/// A whole number that fits an `Int32`: how far a `documents` bound, a `$shift` and a date
/// `range` bound may reach.
fn small_integer(value: &Bson, what: &str) -> Result<i64> {
    let n = as_integer(value, what)?;
    if i32::try_from(n).is_err() {
        return bad(format!("{what} must be an integer that fits 32 bits, found {n}"));
    }
    Ok(n)
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
            single_sort()?;
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
            let by = small_integer(&d["by"], "$shift `by`")?;
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
            // `$locf` carries a value forward in the order the documents arrive when there
            // is no `sortBy`; `$linearFill` needs the one field to interpolate along.
            if op == "$linearFill" {
                single_sort()?;
            }
            let expr = Expr::parse_with_vars(arg, vars)?;
            Ok(if op == "$locf" { Func::Locf(expr) } else { Func::LinearFill(expr) })
        }
        "$covariancePop" | "$covarianceSamp" => {
            let exprs = match arg {
                Bson::Array(items) if items.len() == 2 => [
                    Expr::parse_with_vars(&items[0], vars)?,
                    Expr::parse_with_vars(&items[1], vars)?,
                ],
                _ => return bad(format!("{op} takes an array of two expressions, [x, y]")),
            };
            let window = match window {
                None => Window::Whole,
                Some(w) => parse_window(op, w, sort)?,
            };
            Ok(Func::Covariance { exprs: Box::new(exprs), sample: op == "$covarianceSamp", window })
        }
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
            "{other} is not a window operator; supported: {}, $covariancePop, $covarianceSamp, \
             $rank, $denseRank, $documentNumber, $shift, $derivative, $integral, \
             $expMovingAvg, $locf, $linearFill",
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
        // An empty window is the whole partition, as the reference reads it.
        (None, None) if w.is_empty() => Ok(Window::Whole),
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
                    other => {
                        small_integer(other, &format!("{op} `window` documents bound")).map(Some)
                    }
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
            // A position needs an order to be a position in: only the whole partition does not.
            if sort.is_empty() && (lo.is_some() || hi.is_some()) {
                return bad(format!("{op} with a documents window requires a `sortBy`"));
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
            if sort[0].descending {
                return bad(format!(
                    "{op} with a range window requires an ascending `sortBy`; the reference \
                     refuses a range window over a descending one"
                ));
            }
            let [lo, hi] = bound_pair(op, "range", range)?;
            let one = |b: &Bson| -> Result<RangeBound> {
                match b {
                    Bson::String(s) if s == "unbounded" => Ok(RangeBound::Unbounded),
                    Bson::String(s) if s == "current" => Ok(RangeBound::Current),
                    Bson::String(s) => bad(format!(
                        "{op} `window` bound must be \"unbounded\", \"current\" or a number, \
                         found {s:?}"
                    )),
                    other => match accumulators::numeric(other) {
                        // An infinite bound is no bound.
                        Some(x) if x.is_infinite() => Ok(RangeBound::Unbounded),
                        Some(x) if x.is_finite() => {
                            if unit.is_some() {
                                small_integer(other, &format!("{op} `window` range bound"))?;
                            }
                            Ok(RangeBound::Offset(x))
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
            let value = |b: RangeBound, unbounded: f64| match b {
                RangeBound::Unbounded => unbounded,
                RangeBound::Current => 0.0,
                RangeBound::Offset(x) => x,
            };
            let (l, h) = (value(lo, f64::NEG_INFINITY), value(hi, f64::INFINITY));
            if l > h {
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

/// What a stage names itself in a refusal for its buffer.
pub(crate) fn buffer_name(stage: &str) -> &'static str {
    match stage {
        "$fill" => "the documents $fill buffers",
        "$densify" => "the documents $densify buffers and generates",
        _ => "the documents $setWindowFields buffers",
    }
}

/// The bytes a field adds to a document: type byte, name, terminator, value.
pub(crate) fn element_bytes(name: &str, value: &Bson) -> usize {
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

/// The values one output takes, a document each; `None` leaves the field out.
enum Cells {
    Same(Option<Bson>),
    Per(Vec<Option<Bson>>),
}

impl Cells {
    fn at(&self, i: usize) -> Option<&Bson> {
        match self {
            Cells::Same(v) => v.as_ref(),
            Cells::Per(v) => v[i].as_ref(),
        }
    }
}

struct Sink<'a> {
    lease: &'a mut Lease,
    name: &'a str,
}

impl Sink<'_> {
    fn charge(&mut self, value: &Option<Bson>) -> Result<()> {
        match value {
            Some(v) => self.lease.charge(element_bytes(self.name, v)),
            None => Ok(()),
        }
    }
}

struct Run<'a, 'v> {
    vars: &'a [Binding<'v>],
}

/// The partition key of a document: its encoded bytes, and the value it stands for.
pub(crate) fn partition_key(
    partition: &Partition,
    doc: &Document,
    vars: &[Binding<'_>],
) -> Result<(Vec<u8>, Rep)> {
    Ok(match partition {
        Partition::Whole => (Vec::new(), Rep::One(Bson::Null)),
        Partition::Expr(e) => {
            let value = eval(e, doc, vars)?;
            if matches!(value, Bson::Array(_)) {
                return bad("an expression used to partition cannot evaluate to an array");
            }
            (group_key(&value), Rep::One(value))
        }
        Partition::Fields(fields) => {
            let held = fields_of(doc, fields);
            let values = fields.iter().map(|f| held_value(&held, f)).collect();
            (group_key(&Bson::Document(held)), Rep::Fields(values))
        }
    })
}

fn held_value(held: &Document, field: &str) -> Option<Bson> {
    path::resolve(held, field).into_iter().next().cloned()
}

/// What a partition stands for, for ordering partitions.
pub(crate) enum Rep {
    One(Bson),
    /// The value of each partition field, `None` where the document has none.
    Fields(Vec<Option<Bson>>),
}

/// Partitions order by their values in the canonical order, `null` before a missing field.
pub(crate) fn compare_fields(a: &[Option<Bson>], b: &[Option<Bson>]) -> Ordering {
    for (x, y) in a.iter().zip(b) {
        let o = match (x, y) {
            (Some(x), Some(y)) => canonical_cmp(x, y),
            (None, None) => Ordering::Equal,
            // A missing field sorts as null, after a null that is there.
            (None, Some(y)) => canonical_cmp(&Bson::Null, y).then(Ordering::Greater),
            (Some(x), None) => canonical_cmp(x, &Bson::Null).then(Ordering::Less),
        };
        if o.is_ne() {
            return o;
        }
    }
    Ordering::Equal
}

impl Rep {
    fn compare(&self, other: &Rep) -> Ordering {
        match (self, other) {
            (Rep::One(a), Rep::One(b)) => canonical_cmp(a, b),
            (Rep::Fields(a), Rep::Fields(b)) => compare_fields(a, b),
            _ => Ordering::Equal,
        }
    }
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
    let mut lease = limits.buffer.lease(buffer_name(spec.stage));
    let partitions = partitions(&spec.partition, input, vars, &mut lease)?;
    let run = Run { vars };
    let mut out = Vec::new();
    for docs in partitions {
        out.extend(run_partition(spec, docs, &run, &mut lease)?);
    }
    Ok(out)
}

/// The input filed under its partitions, each charged to the budget, the partitions in
/// ascending order of their values (stably: equal partitions cannot be two).
fn partitions(
    partition: &Partition,
    input: Vec<Document>,
    vars: &[Binding<'_>],
    lease: &mut Lease,
) -> Result<Vec<Vec<Document>>> {
    let mut order: Vec<(Rep, Vec<Document>)> = Vec::new();
    if matches!(partition, Partition::Whole) {
        for doc in &input {
            lease.charge_doc(doc)?;
        }
        return Ok(if input.is_empty() { Vec::new() } else { vec![input] });
    }
    let mut index: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
    for doc in input {
        lease.charge_doc(&doc)?;
        let (key, rep) = partition_key(partition, &doc, vars)?;
        let slot = *index.entry(key).or_insert_with(|| {
            order.push((rep, Vec::new()));
            order.len() - 1
        });
        order[slot].1.push(doc);
    }
    order.sort_by(|a, b| a.0.compare(&b.0));
    Ok(order.into_iter().map(|(_, docs)| docs).collect())
}

/// The value at `path` as the document holds it: `None` when it is missing.
fn raw(doc: &Document, path: &str) -> Option<Bson> {
    path::resolve(doc, path).into_iter().next().cloned()
}

fn run_partition(
    spec: &WindowSpec,
    docs: Vec<Document>,
    run: &Run<'_, '_>,
    lease: &mut Lease,
) -> Result<Vec<Document>> {
    let mut docs = docs;
    if !spec.sort.is_empty() {
        // Sorted once, on the values taken once, which are charged while they are held.
        let mut keyed: Vec<(Vec<Bson>, Document)> = Vec::with_capacity(docs.len());
        for doc in docs {
            let keys = shape::sort_keys(&spec.sort, &doc);
            lease.charge(keys.iter().map(|k| element_bytes("", k)).sum())?;
            keyed.push((keys, doc));
        }
        keyed.sort_by(|a, b| shape::compare_keys(&spec.sort, &a.0, &b.0));
        docs = keyed.into_iter().map(|(_, d)| d).collect();
    }

    let mut results: Vec<Cells> = Vec::with_capacity(spec.outputs.len());
    for output in &spec.outputs {
        let mut sink = Sink { lease, name: &output.name };
        results.push(compute(&output.func, &docs, &spec.sort, run, &mut sink)?);
    }
    for (i, doc) in docs.iter_mut().enumerate() {
        for (output, cells) in spec.outputs.iter().zip(&results) {
            if let Some(v) = cells.at(i) {
                path::set(doc, &output.name, v.clone()).map_err(|e| {
                    Error::InvalidQuery(format!("cannot set {:?}: {e}", output.name))
                })?;
            }
        }
    }
    Ok(docs)
}

fn per(out: Vec<Option<Bson>>) -> Result<Cells> {
    Ok(Cells::Per(out))
}

fn compute(
    func: &Func,
    docs: &[Document],
    sort: &[SortKey],
    run: &Run<'_, '_>,
    sink: &mut Sink<'_>,
) -> Result<Cells> {
    let n = docs.len();
    match func {
        Func::Acc { acc, window } => {
            let ends = endpoints(acc_name(acc), window, docs, sort)?;
            accumulator(acc, docs, &ends, run, sink)
        }
        Func::Covariance { exprs, sample, window } => {
            let op = if *sample { "$covarianceSamp" } else { "$covariancePop" };
            let ends = endpoints(op, window, docs, sort)?;
            let mut slider = CovSlider::new(exprs, *sample, run.vars);
            slide(&mut slider, docs, &ends, sink)
        }
        Func::Rank | Func::DenseRank | Func::DocumentNumber => {
            let path = &sort[0].path;
            let (mut rank, mut dense) = (0i64, 0i64);
            let mut previous: Option<Option<Bson>> = None;
            let mut out = Vec::with_capacity(n);
            for (i, doc) in docs.iter().enumerate() {
                // Documents tie when the value is the same; a missing one is not a null one.
                let value = raw(doc, path);
                let tie = match (&previous, &value) {
                    (Some(None), None) => true,
                    (Some(Some(a)), Some(b)) => canonical_cmp(a, b).is_eq(),
                    _ => false,
                };
                if !tie {
                    rank = i as i64 + 1;
                    dense += 1;
                }
                previous = Some(value);
                let v = Some(Bson::Int32(clamp32(match func {
                    Func::Rank => rank,
                    Func::DenseRank => dense,
                    _ => i as i64 + 1,
                })));
                sink.charge(&v)?;
                out.push(v);
            }
            per(out)
        }
        Func::Shift { output, by, default } => {
            let default = match default {
                Some(e) => Some(eval(e, &docs[0], run.vars)?),
                None => Some(Bson::Null),
            };
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let at = i as i128 + i128::from(*by);
                let v = if at >= 0 && (at as usize) < n {
                    // A shifted document that has no value leaves the field out.
                    eval_present(output, &docs[at as usize], run.vars)?
                } else {
                    default.clone()
                };
                sink.charge(&v)?;
                out.push(v);
            }
            per(out)
        }
        Func::Derivative { input, unit, window } => {
            let ends = endpoints("$derivative", window, docs, sort)?;
            let x = Axis::of("$derivative", docs, &sort[0].path, unit.as_deref())?;
            let y = numeric_inputs(input, docs, run.vars)?;
            let mut out = Vec::with_capacity(n);
            for &(s, e) in &ends {
                let v = if e - s < 2 {
                    Bson::Null
                } else {
                    y.check("$derivative", s, e)?;
                    let dx = x.delta(s, e - 1);
                    if dx == 0.0 {
                        Bson::Null
                    } else {
                        Bson::Double((y.values[e - 1] - y.values[s]) / dx)
                    }
                };
                let v = Some(v);
                sink.charge(&v)?;
                out.push(v);
            }
            per(out)
        }
        Func::Integral { input, unit, window } => {
            let ends = endpoints("$integral", window, docs, sort)?;
            let x = Axis::of("$integral", docs, &sort[0].path, unit.as_deref())?;
            let y = numeric_inputs(input, docs, run.vars)?;
            integral(&ends, &x, &y, sink)
        }
        Func::ExpMovingAvg { input, alpha } => {
            let mut ema: Option<f64> = None;
            let mut out = Vec::with_capacity(n);
            for doc in docs {
                // A value that is not a number leaves the average where it is.
                if let Some(x) = accumulators::numeric(&eval(input, doc, run.vars)?) {
                    ema = Some(match ema {
                        None => x,
                        Some(prev) => prev + alpha * (x - prev),
                    });
                }
                let v = Some(ema.map_or(Bson::Null, Bson::Double));
                sink.charge(&v)?;
                out.push(v);
            }
            per(out)
        }
        Func::Locf(expr) => {
            let mut last = Bson::Null;
            let mut out = Vec::with_capacity(n);
            for doc in docs {
                let v = eval(expr, doc, run.vars)?;
                if !matches!(v, Bson::Null) {
                    last = v;
                }
                let v = Some(last.clone());
                sink.charge(&v)?;
                out.push(v);
            }
            per(out)
        }
        Func::LinearFill(expr) => linear_fill(expr, docs, sort, run, sink),
    }
}

/// A rank as an `Int32`, which the reference answers; a long past it.
fn clamp32(n: i64) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
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
// The sort axis, and the endpoints of a window
// ---------------------------------------------------------------------------

/// The sort values of a partition as numbers on one axis.
enum Axis {
    /// Numbers.
    Num(Vec<f64>),
    /// Dates, in milliseconds, and the milliseconds one unit of the axis is.
    Date(Vec<i64>, f64),
}

impl Axis {
    /// The one sort field of `docs` read as an axis: a number or a date in every document,
    /// and not an array. A date takes `unit` (milliseconds when there is none).
    fn of(op: &str, docs: &[Document], path: &str, unit: Option<&str>) -> Result<Axis> {
        let mut nums: Vec<f64> = Vec::with_capacity(docs.len());
        let mut dates: Vec<i64> = Vec::new();
        for doc in docs {
            let found = raw(doc, path);
            match &found {
                Some(Bson::DateTime(_)) if !nums.is_empty() => {
                    return bad(format!(
                        "{op} needs the sortBy field to be all numbers or all dates, found both"
                    ));
                }
                Some(Bson::DateTime(d)) => dates.push(d.timestamp_millis()),
                other if dates.is_empty() => match other.as_ref().and_then(accumulators::numeric) {
                    Some(x) if !x.is_nan() => nums.push(x),
                    _ => {
                        return bad(format!(
                            "{op} needs the sortBy field to be a number or a date in every \
                             document, found {}",
                            match other {
                                None | Some(Bson::Null) => "null or a missing value",
                                Some(other) => type_name(other),
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
        if dates.is_empty() {
            if unit.is_some() {
                return bad(format!(
                    "{op} `unit` is for a date sortBy field, and this one is numeric"
                ));
            }
            Ok(Axis::Num(nums))
        } else {
            let per = unit.and_then(unit_millis).unwrap_or(1) as f64;
            Ok(Axis::Date(dates, per))
        }
    }

    /// How far `b` is from `a` along the axis: dates are subtracted in milliseconds first,
    /// then scaled to the unit.
    fn delta(&self, a: usize, b: usize) -> f64 {
        match self {
            Axis::Num(v) => v[b] - v[a],
            Axis::Date(v, per) => (v[b] - v[a]) as f64 / per,
        }
    }

    /// How far `b` is from `a` in the units of the values themselves: milliseconds for a date.
    fn raw_delta(&self, a: usize, b: usize) -> f64 {
        match self {
            Axis::Num(v) => v[b] - v[a],
            Axis::Date(v, _) => (v[b] - v[a]) as f64,
        }
    }

    /// What one unit of the axis is in the values themselves: 1 for a number.
    fn per(&self) -> f64 {
        match self {
            Axis::Num(_) => 1.0,
            Axis::Date(_, per) => *per,
        }
    }

    fn is_date(&self) -> bool {
        matches!(self, Axis::Date(..))
    }
}

/// For each position, the half-open run `[start, end)` of the partition its window covers.
/// Both ends never decrease as the position moves on.
fn endpoints(
    op: &str,
    window: &Window,
    docs: &[Document],
    sort: &[SortKey],
) -> Result<Vec<(usize, usize)>> {
    let n = docs.len();
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
            let axis = range_axis(op, docs, &sort[0].path, unit.as_deref())?;
            let mut out = Vec::with_capacity(n);
            match &axis {
                Axis::Num(v) => {
                    for i in 0..n {
                        let s = match lo {
                            RangeBound::Unbounded => 0,
                            RangeBound::Current => i,
                            RangeBound::Offset(l) => {
                                let t = v[i] + l;
                                v.partition_point(|&x| x < t)
                            }
                        };
                        let e = match hi {
                            RangeBound::Unbounded => n,
                            RangeBound::Current => i + 1,
                            RangeBound::Offset(h) => {
                                let t = v[i] + h;
                                v.partition_point(|&x| x <= t)
                            }
                        };
                        out.push((s, e.max(s)));
                    }
                }
                Axis::Date(v, _) => {
                    let Some(unit) = unit else {
                        return bad(format!(
                            "{op}: a range window over a date sortBy field needs a `unit`"
                        ));
                    };
                    for i in 0..n {
                        let s = match lo {
                            RangeBound::Unbounded => 0,
                            RangeBound::Current => i,
                            RangeBound::Offset(l) => {
                                let t = shift_date(v[i], *l, unit, op)?;
                                v.partition_point(|&x| x < t)
                            }
                        };
                        let e = match hi {
                            RangeBound::Unbounded => n,
                            RangeBound::Current => i + 1,
                            RangeBound::Offset(h) => {
                                let t = shift_date(v[i], *h, unit, op)?;
                                v.partition_point(|&x| x <= t)
                            }
                        };
                        out.push((s, e.max(s)));
                    }
                }
            }
            Ok(out)
        }
    }
}

/// The axis of a range window: numbers, or dates with the `unit`.
fn range_axis(op: &str, docs: &[Document], path: &str, unit: Option<&str>) -> Result<Axis> {
    // A date sort field takes a `unit` here; the check below tells which it is.
    match Axis::of(op, docs, path, None) {
        Ok(axis) => {
            if unit.is_some() && !axis.is_date() {
                return bad(format!(
                    "{op}: a range window `unit` is for a date sortBy field, and this one is \
                     numeric"
                ));
            }
            Ok(axis)
        }
        Err(e) => Err(e),
    }
}

/// `from` moved by `offset` units: a calendar step for a month, quarter or year, a length of
/// time for the rest.
fn shift_date(from: i64, offset: f64, unit: &str, op: &str) -> Result<i64> {
    match unit_millis(unit) {
        Some(len) => add_to_date(from, "millisecond", (offset * len as f64).round() as i64, op),
        None => add_to_date(from, unit, offset as i64, op),
    }
}

// ---------------------------------------------------------------------------
// Accumulators over windows
// ---------------------------------------------------------------------------

fn accumulator(
    acc: &Accumulator,
    docs: &[Document],
    ends: &[(usize, usize)],
    run: &Run<'_, '_>,
    sink: &mut Sink<'_>,
) -> Result<Cells> {
    let n = docs.len();
    let vars = run.vars;
    let edge = |expr: &Expr, at: Option<usize>| -> Result<Option<Bson>> {
        match at {
            // A value that is missing leaves the field out; no document gives `null`.
            Some(i) => eval_present(expr, &docs[i], vars),
            None => Ok(Some(Bson::Null)),
        }
    };
    let mut out: Vec<Option<Bson>> = Vec::with_capacity(n);
    match acc {
        Accumulator::First(expr) | Accumulator::Last(expr) => {
            let first = matches!(acc, Accumulator::First(_));
            for &(s, e) in ends {
                let at = (s < e).then(|| if first { s } else { e - 1 });
                let v = edge(expr, at)?;
                sink.charge(&v)?;
                out.push(v);
            }
            return per(out);
        }
        Accumulator::Ext(spec) if spec.kind == Kind::Count => {
            for &(s, e) in ends {
                let count = e - s;
                let v = Some(match i32::try_from(count) {
                    Ok(c) => Bson::Int32(c),
                    Err(_) => Bson::Int64(count as i64),
                });
                sink.charge(&v)?;
                out.push(v);
            }
            return per(out);
        }
        _ => {}
    }
    let mut slider = make_slider(acc, docs, vars)?;
    slide(slider.as_mut(), docs, ends, sink)
}

/// The slider for an accumulator that is not a closed form of the ends.
fn make_slider<'a, 'v>(
    acc: &'a Accumulator,
    docs: &[Document],
    vars: &'a [Binding<'v>],
) -> Result<Box<dyn Slider + 'a>> {
    Ok(match acc {
        Accumulator::Sum(e) => Box::new(SumSlider::new(e, false, vars)),
        Accumulator::Avg(e) => Box::new(SumSlider::new(e, true, vars)),
        Accumulator::Min(e) => Box::new(ExtremeSlider::new(e, false, vars)),
        Accumulator::Max(e) => Box::new(ExtremeSlider::new(e, true, vars)),
        Accumulator::Push(e) => Box::new(QueueSlider::new(e, Queue::Push, usize::MAX, vars)),
        Accumulator::AddToSet(e) => Box::new(SetSlider::new(e, vars)),
        Accumulator::Ext(spec) => match spec.kind {
            Kind::StdDevPop => Box::new(StdDevSlider::new(&spec.value, false, vars)),
            Kind::StdDevSamp => Box::new(StdDevSlider::new(&spec.value, true, vars)),
            Kind::Top | Kind::Bottom | Kind::TopN | Kind::BottomN => {
                Box::new(RankedSlider::new(spec, docs, vars)?)
            }
            Kind::MaxN | Kind::MinN => Box::new(BestSlider::new(spec, docs, vars)?),
            Kind::FirstN | Kind::LastN => {
                let n = spec.resolve(&crate::expr::Scope::with_bindings(&docs[0], vars))?;
                let kind = if spec.kind == Kind::FirstN { Queue::First } else { Queue::Last };
                Box::new(QueueSlider::new(&spec.value, kind, n, vars))
            }
            Kind::Count => {
                return bad("$count is answered from the ends of the window and has no state");
            }
        },
        Accumulator::First(_) | Accumulator::Last(_) | Accumulator::MergeObjects(_) => {
            return bad(format!("{} is not a window operator", acc_name(acc)));
        }
    })
}

/// Drive a slider over the windows of a partition.
fn slide(
    slider: &mut dyn Slider,
    docs: &[Document],
    ends: &[(usize, usize)],
    sink: &mut Sink<'_>,
) -> Result<Cells> {
    let n = docs.len();
    if ends.iter().all(|&e| e == (0, n)) {
        // The same answer for every document: computed once, charged for every copy.
        for i in 0..n {
            slider.add(i, docs)?;
        }
        let v = Some(slider.value(docs, 0, n)?);
        if let Some(b) = &v {
            sink.lease.charge(element_bytes(sink.name, b).saturating_mul(n))?;
        }
        return Ok(Cells::Same(v));
    }
    let mut out: Vec<Option<Bson>> = Vec::with_capacity(n);
    let mut at = (0usize, 0usize);
    let mut previous: Option<((usize, usize), Bson)> = None;
    for &(s, e) in ends {
        // A request-deadline tick goes here (ADR-266).
        if let Some((window, v)) = &previous
            && *window == (s, e)
        {
            let v = Some(v.clone());
            sink.charge(&v)?;
            out.push(v);
            continue;
        }
        if s >= at.1 {
            slider.reset(s);
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
        previous = Some(((s, e), v.clone()));
        let v = Some(v);
        sink.charge(&v)?;
        out.push(v);
    }
    per(out)
}

// ---------------------------------------------------------------------------
// $derivative, $integral, $linearFill
// ---------------------------------------------------------------------------

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

/// `$integral`: the area of each trapezoid between neighbours, summed exactly over the
/// window so a large area that has left cannot take the small ones with it.
fn integral(ends: &[(usize, usize)], x: &Axis, y: &Inputs, sink: &mut Sink<'_>) -> Result<Cells> {
    let n = y.values.len();
    // Each area in milliseconds times the value for a date, and divided into the unit once at
    // the end, as the reference does: the epoch's milliseconds are never scaled and subtracted.
    let areas: Vec<f64> =
        (1..n).map(|k| x.raw_delta(k - 1, k) * (y.values[k - 1] + y.values[k]) / 2.0).collect();
    let unit_len = x.per();
    let mut sum = ExactDoubles::default();
    let (mut a_lo, mut a_hi) = (0usize, 0usize);
    let mut out = Vec::with_capacity(n);
    for &(s, e) in ends {
        let v = if e - s < 2 {
            // The reference answers an integer zero over a number line, a double over dates.
            if x.is_date() { Bson::Double(0.0) } else { Bson::Int32(0) }
        } else {
            y.check("$integral", s, e)?;
            // The areas between the documents of the window.
            let (lo, hi) = (s, e - 1);
            if lo >= a_hi {
                sum.clear();
                a_lo = lo;
                a_hi = lo;
            }
            while a_hi < hi {
                sum.add(areas[a_hi]);
                a_hi += 1;
            }
            while a_lo < lo {
                sum.add(-areas[a_lo]);
                a_lo += 1;
            }
            let total = if sum.overflowed { areas[lo..hi].iter().sum() } else { sum.value() };
            Bson::Double(total / unit_len)
        };
        let v = Some(v);
        sink.charge(&v)?;
        out.push(v);
    }
    per(out)
}

/// `$linearFill`: a null or missing value is the point on the line between the nearest
/// numbers on either side of it along the one sort field; `Decimal128` endpoints are
/// interpolated in decimal.
fn linear_fill(
    expr: &Expr,
    docs: &[Document],
    sort: &[SortKey],
    run: &Run<'_, '_>,
    sink: &mut Sink<'_>,
) -> Result<Cells> {
    let n = docs.len();
    let x = Axis::of("$linearFill", docs, &sort[0].path, None)?;
    // The value of the sort field at each position, as the decimal the line is drawn in.
    let xs: Vec<f64> = match &x {
        Axis::Num(v) => v.clone(),
        Axis::Date(v, _) => v.iter().map(|&ms| ms as f64).collect(),
    };
    for pair in xs.windows(2) {
        if pair[0] == pair[1] {
            return bad("$linearFill: there can be no repeated values in the sort field");
        }
    }
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
    let sort_decimal = |i: usize, docs: &[Document]| -> Option<Raw> {
        match raw(&docs[i], &sort[0].path)? {
            Bson::DateTime(d) => Some(arith::from_i64(d.timestamp_millis())),
            other => decimal_of(&other),
        }
    };
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let v = if !matches!(values[i], Bson::Null) {
            values[i].clone()
        } else if let (Some(a), Some(b)) = (before[i], after[i]) {
            let decimal = matches!(values[a], Bson::Decimal128(_))
                || matches!(values[b], Bson::Decimal128(_));
            if decimal {
                match (
                    decimal_of(&values[a]),
                    decimal_of(&values[b]),
                    sort_decimal(a, docs),
                    sort_decimal(b, docs),
                    sort_decimal(i, docs),
                ) {
                    (Some(y0), Some(y1), Some(x0), Some(x1), Some(xi)) => {
                        // y0 + (y1 - y0) (x - x0) / (x1 - x0): multiply, then divide once.
                        let rise = arith::mul(&arith::sub(&y1, &y0), &arith::sub(&xi, &x0));
                        match arith::div(&rise, &arith::sub(&x1, &x0)) {
                            Ok(q) => Bson::Decimal128(pack_raw(arith::add(&y0, &q))),
                            Err(_) => values[a].clone(),
                        }
                    }
                    _ => Bson::Null,
                }
            } else {
                let num = |j: usize| accumulators::numeric(&values[j]).unwrap_or(f64::NAN);
                let (y0, y1) = (num(a), num(b));
                Bson::Double(y0 + (y1 - y0) * (xs[i] - xs[a]) / (xs[b] - xs[a]))
            }
        } else {
            Bson::Null
        };
        let v = Some(v);
        sink.charge(&v)?;
        out.push(v);
    }
    per(out)
}

/// A number as a decimal operand: an integer exactly, a double by its exact binary value.
fn decimal_of(v: &Bson) -> Option<Raw> {
    match v {
        Bson::Int32(n) => Some(arith::from_i64(i64::from(*n))),
        Bson::Int64(n) => Some(arith::from_i64(*n)),
        Bson::Double(d) => Some(arith::from_binary64(d.to_bits()).0),
        Bson::Decimal128(d) => Some(decode_raw(d)),
        _ => None,
    }
}
