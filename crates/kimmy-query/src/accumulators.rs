//! The `$group` accumulators beyond the original eight (ADR-262): `$stdDevPop`,
//! `$stdDevSamp`, `$top`, `$bottom`, `$topN`, `$bottomN`, `$firstN`, `$lastN`,
//! `$maxN`, `$minN` and `$count`.
//!
//! Each is a [`Spec`] (what was written, parsed once) and a [`State`] (what one
//! group has seen). The pair is driven by [`State::accumulate`] for every
//! document and [`State::finish`] once, and [`State::remove`] undoes a document
//! where the state can, which is the shape a sliding window needs. Only `$group`,
//! `$bucket` and `$bucketAuto` drive them today.
//!
//! Every buffer an `n` bounds is charged to the cap `$group` already holds its
//! buckets to (`Limits::max_documents`): [`State::accumulate`] reports how many
//! rows the state grew by and the caller totals them across the groups of the
//! stage. An `n` above the cap is refused the first time it is read.

use std::cmp::Ordering;
use std::collections::VecDeque;

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::decimal::{arith, decode_raw};
use kimmy_core::{Error, Result, path};

use crate::aggregate::Limits;
use crate::expr::{Expr, Scope};
use crate::shape::{self, SortKey};

/// Which accumulator, without its arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    StdDevPop,
    StdDevSamp,
    Count,
    Top,
    Bottom,
    TopN,
    BottomN,
    FirstN,
    LastN,
    MaxN,
    MinN,
}

/// One of the accumulators of this module, as written.
#[derive(Clone, Debug, PartialEq)]
pub struct Spec {
    pub kind: Kind,
    /// The value: `$stdDev*`'s operand, `$firstN`'s/`$lastN`'s/`$maxN`'s/`$minN`'s
    /// `input`, `$top*`'s/`$bottom*`'s `output`. Unused by `$count`.
    pub value: Expr,
    /// `n`, for the `N` forms.
    pub n: Option<Expr>,
    /// `sortBy`, for `$top*` and `$bottom*`.
    pub sort: Vec<SortKey>,
}

/// Names this module owns.
pub fn handles(op: &str) -> bool {
    kind_of(op).is_some()
}

fn kind_of(op: &str) -> Option<Kind> {
    Some(match op {
        "$stdDevPop" => Kind::StdDevPop,
        "$stdDevSamp" => Kind::StdDevSamp,
        "$count" => Kind::Count,
        "$top" => Kind::Top,
        "$bottom" => Kind::Bottom,
        "$topN" => Kind::TopN,
        "$bottomN" => Kind::BottomN,
        "$firstN" => Kind::FirstN,
        "$lastN" => Kind::LastN,
        "$maxN" => Kind::MaxN,
        "$minN" => Kind::MinN,
        _ => return None,
    })
}

/// Parse `{<op>: <arg>}` if `op` is one of ours.
pub fn parse(op: &str, arg: &Bson, vars: &[String]) -> Result<Option<Spec>> {
    let Some(kind) = kind_of(op) else { return Ok(None) };
    let none = Expr::Literal(Bson::Null);
    let spec = match kind {
        Kind::StdDevPop | Kind::StdDevSamp => {
            Spec { kind, value: Expr::parse_with_vars(arg, vars)?, n: None, sort: Vec::new() }
        }
        Kind::Count => {
            if !matches!(arg, Bson::Document(d) if d.is_empty()) {
                return Err(Error::InvalidQuery(
                    "$count takes no arguments, i.e. $count: {}".into(),
                ));
            }
            Spec { kind, value: none, n: None, sort: Vec::new() }
        }
        Kind::Top | Kind::Bottom | Kind::TopN | Kind::BottomN => {
            let with_n = matches!(kind, Kind::TopN | Kind::BottomN);
            let d = argument(op, arg, with_n, "output")?;
            let sort_by = d.get("sortBy").and_then(Bson::as_document).ok_or_else(|| {
                Error::InvalidQuery(format!("{op} requires `sortBy` to be a document"))
            })?;
            if sort_by.is_empty() {
                return Err(Error::InvalidQuery(format!(
                    "{op} requires `sortBy` to name at least one field"
                )));
            }
            Spec {
                kind,
                value: Expr::parse_with_vars(&d["output"], vars)?,
                n: parse_n(op, d, vars)?,
                sort: shape::parse_sort(sort_by)?,
            }
        }
        Kind::FirstN | Kind::LastN | Kind::MaxN | Kind::MinN => {
            let d = argument(op, arg, true, "input")?;
            Spec {
                kind,
                value: Expr::parse_with_vars(&d["input"], vars)?,
                n: parse_n(op, d, vars)?,
                sort: Vec::new(),
            }
        }
    };
    Ok(Some(spec))
}

/// The argument document of an accumulator that names its fields: exactly `n`
/// (if `with_n`), `sortBy` (for the `$top` family, i.e. when `value` is
/// `output`) and `value`, and no other.
fn argument<'a>(op: &str, arg: &'a Bson, with_n: bool, value: &str) -> Result<&'a Document> {
    let d = arg.as_document().ok_or_else(|| {
        Error::InvalidQuery(format!("{op} requires an object as its argument, found {arg}"))
    })?;
    let sorted = value == "output";
    let mut required = vec![value];
    if with_n {
        required.push("n");
    }
    if sorted {
        required.push("sortBy");
    }
    for key in d.keys() {
        if !required.contains(&key.as_str()) {
            return Err(Error::InvalidQuery(format!("{op} found an unknown argument: {key}")));
        }
    }
    for key in &required {
        if !d.contains_key(key) {
            return Err(Error::InvalidQuery(format!("{op} requires `{key}` to be specified")));
        }
    }
    Ok(d)
}

fn parse_n(op: &str, d: &Document, vars: &[String]) -> Result<Option<Expr>> {
    let Some(n) = d.get("n") else { return Ok(None) };
    let expr = Expr::parse_with_vars(n, vars)?;
    // An expression that needs no document is read now, so a bad constant is a
    // parse error. One that reads a variable is read at first use. A field path
    // reads the empty document, is missing there, and is refused: `n` is one
    // number for the whole group.
    match expr.eval_in(&Scope::new(&Document::new())) {
        Ok(value) => {
            read_n(op, &value)?;
            Ok(Some(Expr::Literal(value)))
        }
        Err(_) => Ok(Some(expr)),
    }
}

/// `n` as a positive integer.
fn read_n(op: &str, value: &Bson) -> Result<usize> {
    let n: i64 = match value {
        Bson::Int32(n) => i64::from(*n),
        Bson::Int64(n) => *n,
        Bson::Double(d) if d.fract() == 0.0 && d.abs() < 9.2e18 => *d as i64,
        Bson::Decimal128(d) => kimmy_core::decimal::whole_number(d).ok_or_else(|| {
            Error::InvalidQuery(format!("{op}: `n` must be an integer, found {value}"))
        })?,
        Bson::Double(_) => {
            return Err(Error::InvalidQuery(format!(
                "{op}: `n` must be an integer, found {value}"
            )));
        }
        other => {
            return Err(Error::InvalidQuery(format!(
                "{op}: `n` must be an integer, found {}",
                if matches!(other, Bson::Null) {
                    "null or a missing value".to_string()
                } else {
                    format!("{:?}", other.element_type())
                }
            )));
        }
    };
    if n <= 0 {
        return Err(Error::InvalidQuery(format!("{op}: `n` must be greater than 0, found {n}")));
    }
    Ok(usize::try_from(n).unwrap_or(usize::MAX))
}

impl Spec {
    fn op(&self) -> &'static str {
        match self.kind {
            Kind::StdDevPop => "$stdDevPop",
            Kind::StdDevSamp => "$stdDevSamp",
            Kind::Count => "$count",
            Kind::Top => "$top",
            Kind::Bottom => "$bottom",
            Kind::TopN => "$topN",
            Kind::BottomN => "$bottomN",
            Kind::FirstN => "$firstN",
            Kind::LastN => "$lastN",
            Kind::MaxN => "$maxN",
            Kind::MinN => "$minN",
        }
    }

    /// A fresh state for one group.
    pub fn init(&self) -> State {
        let body = match self.kind {
            Kind::StdDevPop | Kind::StdDevSamp => Body::Welford(Welford::default()),
            Kind::Count => Body::Count(0),
            Kind::Top | Kind::Bottom | Kind::TopN | Kind::BottomN => Body::Ranked(Vec::new()),
            Kind::FirstN | Kind::LastN | Kind::MaxN | Kind::MinN => Body::Values(VecDeque::new()),
        };
        State {
            kind: self.kind,
            // `$top` and `$bottom` keep one row.
            limit: matches!(self.kind, Kind::Top | Kind::Bottom).then_some(1),
            body,
        }
    }

    /// `n`, read the first time a document arrives and checked against the cap.
    fn resolve(&self, scope: &Scope<'_>, limits: &Limits) -> Result<usize> {
        let n = match &self.n {
            Some(expr) => read_n(self.op(), &expr.eval_in(scope)?)?,
            None => 1,
        };
        if n > limits.max_documents {
            return Err(Error::Limit(format!(
                "{}: n = {n} is over the pipeline limit of {} documents held per group. Use a \
                 smaller n, or narrow the pipeline with an earlier $match",
                self.op(),
                limits.max_documents
            )));
        }
        Ok(n)
    }
}

/// What one group has seen.
#[derive(Clone, Debug)]
pub struct State {
    kind: Kind,
    limit: Option<usize>,
    body: Body,
}

#[derive(Clone, Debug)]
enum Body {
    Welford(Welford),
    Count(u64),
    /// Rows in rank order (sort key, then arrival).
    Ranked(Vec<Row>),
    /// `$firstN`/`$lastN` in arrival order; `$maxN`/`$minN` in rank order, best first.
    Values(VecDeque<Bson>),
}

#[derive(Clone, Debug)]
struct Row {
    keys: Vec<Bson>,
    output: Bson,
}

/// Running mean and sum of squared deviations, updated one value at a time
/// (Welford), so a large offset does not cost the digits the spread needs.
#[derive(Clone, Debug, Default)]
struct Welford {
    n: u64,
    mean: f64,
    m2: f64,
}

impl Welford {
    fn add(&mut self, x: f64) {
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        self.m2 += delta * (x - self.mean);
    }

    fn remove(&mut self, x: f64) {
        if self.n <= 1 {
            *self = Self::default();
            return;
        }
        let old_mean = self.mean;
        self.mean = (self.n as f64 * old_mean - x) / (self.n - 1) as f64;
        self.m2 -= (x - old_mean) * (x - self.mean);
        self.n -= 1;
        if self.m2 < 0.0 {
            self.m2 = 0.0;
        }
    }
}

/// A numeric operand as a double; anything else is not counted. A `Decimal128`
/// is converted to the nearest double (documented deviation: the result is a
/// double either way).
fn numeric(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(n) => Some(f64::from(*n)),
        Bson::Int64(n) => Some(*n as f64),
        Bson::Double(d) => Some(*d),
        Bson::Decimal128(d) => Some(f64::from_bits(arith::to_binary64(&decode_raw(d)))),
        _ => None,
    }
}

/// The value a document sorts by at `path`, for a key of the given direction.
/// A path that meets an array sorts by the array's smallest element ascending
/// and its largest descending, as the reference's sort does.
fn sort_value(doc: &Document, path: &str, descending: bool) -> Bson {
    let values = path::resolve(doc, path);
    if values.is_empty() {
        return Bson::Null;
    }
    let mut candidates: Vec<&Bson> = Vec::new();
    for value in &values {
        match value {
            Bson::Array(items) if !items.is_empty() => candidates.extend(items.iter()),
            other => candidates.push(other),
        }
    }
    let pick = |a: &&Bson, b: &&Bson| canonical_cmp(a, b);
    let best = if descending {
        candidates.into_iter().max_by(pick)
    } else {
        candidates.into_iter().min_by(pick)
    };
    best.cloned().unwrap_or(Bson::Null)
}

impl State {
    /// Rows held, which the caller charges to the stage's cap.
    pub fn held(&self) -> usize {
        match &self.body {
            Body::Ranked(rows) => rows.len(),
            Body::Values(rows) => rows.len(),
            _ => 0,
        }
    }

    /// Take one document. Returns how many rows the state grew by.
    pub fn accumulate(&mut self, spec: &Spec, scope: &Scope<'_>, limits: &Limits) -> Result<usize> {
        let before = self.held();
        if self.limit.is_none() && self.needs_limit() {
            self.limit = Some(spec.resolve(scope, limits)?);
        }
        let limit = self.limit.unwrap_or(usize::MAX);
        match &mut self.body {
            Body::Welford(w) => {
                if let Some(x) = numeric(&spec.value.eval_in(scope)?) {
                    w.add(x);
                }
            }
            Body::Count(n) => *n += 1,
            Body::Ranked(rows) => {
                let bottom = matches!(self.kind, Kind::Bottom | Kind::BottomN);
                let keys: Vec<Bson> = spec
                    .sort
                    .iter()
                    .map(|k| sort_value(scope.root(), &k.path, k.descending))
                    .collect();
                // Evaluate before deciding, so a failing `output` fails the
                // request whether or not the row would have been kept.
                let output = spec.value.eval_in(scope)?;
                // After every row that ranks equal or earlier: arrival breaks ties.
                let at = rows.partition_point(|r| {
                    shape::compare_keys(&spec.sort, &r.keys, &keys) != Ordering::Greater
                });
                rows.insert(at, Row { keys, output });
                if rows.len() > limit {
                    if bottom {
                        rows.remove(0);
                    } else {
                        rows.pop();
                    }
                }
            }
            Body::Values(values) => {
                let value = spec.value.eval_in(scope)?;
                match self.kind {
                    Kind::FirstN => {
                        if values.len() < limit {
                            values.push_back(value);
                        }
                    }
                    Kind::LastN => {
                        values.push_back(value);
                        if values.len() > limit {
                            values.pop_front();
                        }
                    }
                    kind => {
                        // `$maxN` and `$minN` skip null and missing.
                        if !matches!(value, Bson::Null) {
                            let max = kind == Kind::MaxN;
                            // Best first; after equals, so the earlier of two equal
                            // values stays.
                            let at = values.partition_point(|v| {
                                let o = canonical_cmp(v, &value);
                                if max { o != Ordering::Less } else { o != Ordering::Greater }
                            });
                            values.insert(at, value);
                            if values.len() > limit {
                                values.pop_back();
                            }
                        }
                    }
                }
            }
        }
        Ok(self.held().saturating_sub(before))
    }

    fn needs_limit(&self) -> bool {
        matches!(
            self.kind,
            Kind::TopN | Kind::BottomN | Kind::FirstN | Kind::LastN | Kind::MaxN | Kind::MinN
        )
    }

    /// Undo one document that [`Self::accumulate`] took, for a window that slides
    /// past it. `Ok(true)` when it is undone; `Ok(false)` when this accumulator
    /// cannot (a bounded buffer has forgotten what it dropped), and the caller
    /// must rebuild the state from the documents still in the window.
    pub fn remove(&mut self, spec: &Spec, scope: &Scope<'_>) -> Result<bool> {
        match &mut self.body {
            Body::Welford(w) => {
                if let Some(x) = numeric(&spec.value.eval_in(scope)?) {
                    w.remove(x);
                }
                Ok(true)
            }
            Body::Count(n) => {
                *n = n.saturating_sub(1);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// The group's answer.
    pub fn finish(self) -> Bson {
        let single = matches!(self.kind, Kind::Top | Kind::Bottom);
        match self.body {
            Body::Welford(w) => {
                let sample = self.kind == Kind::StdDevSamp;
                match (w.n, sample) {
                    (0, _) | (1, true) => Bson::Null,
                    (n, true) => Bson::Double((w.m2 / (n - 1) as f64).sqrt()),
                    (n, false) => Bson::Double((w.m2 / n as f64).sqrt()),
                }
            }
            Body::Count(n) => match i32::try_from(n) {
                Ok(n) => Bson::Int32(n),
                Err(_) => Bson::Int64(i64::try_from(n).unwrap_or(i64::MAX)),
            },
            Body::Ranked(rows) => {
                if single {
                    rows.into_iter().next().map_or(Bson::Null, |r| r.output)
                } else {
                    Bson::Array(rows.into_iter().map(|r| r.output).collect())
                }
            }
            Body::Values(values) => Bson::Array(values.into_iter().collect()),
        }
    }
}
