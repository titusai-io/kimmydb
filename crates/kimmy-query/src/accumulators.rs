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
//! No `n` is refused for its size and nothing is allocated from it: a buffer
//! grows with the rows it receives, so each accumulator holds at most the rows of
//! its group, which the stage's input already bounds (as `$push` does).
//!
//! The ranked forms keep a heap and a sequence number, so a row costs
//! O(log n) and equal rows keep their order of arrival.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::decimal::{arith, decode_raw};
use kimmy_core::{Error, Result, path};

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

/// `$sum`, `$avg`, `$stdDevPop` and `$stdDevSamp` take one expression, not a
/// list: given an array literal of any length but one, the reference refuses
/// them (`The $sum accumulator is a unary operator`, error 40237). A list of one
/// is not probed and stays an array expression. Every other name passes.
pub fn check_unary(op: &str, arg: &Bson) -> Result<()> {
    match (op, arg) {
        ("$sum" | "$avg" | "$stdDevPop" | "$stdDevSamp", Bson::Array(items))
            if items.len() != 1 =>
        {
            Err(Error::InvalidQuery(format!("The {op} accumulator is a unary operator")))
        }
        _ => Ok(()),
    }
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
            Spec {
                kind,
                value: Expr::parse_with_vars(&d["output"], vars)?,
                n: parse_n(d, vars)?,
                sort: shape::parse_sort(sort_by)?,
            }
        }
        Kind::FirstN | Kind::LastN | Kind::MaxN | Kind::MinN => {
            let d = argument(op, arg, true, "input")?;
            Spec {
                kind,
                value: Expr::parse_with_vars(&d["input"], vars)?,
                n: parse_n(d, vars)?,
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

fn parse_n(d: &Document, vars: &[String]) -> Result<Option<Expr>> {
    let Some(n) = d.get("n") else { return Ok(None) };
    let expr = Expr::parse_with_vars(n, vars)?;
    // An expression that needs no document is read now, to a constant. It is not
    // checked now: a bad `n` is refused for a group, when its first document
    // arrives, so over no input there is no group and no error (as the
    // reference answers). One that reads a variable is read at first use. A
    // field path reads the empty document, is missing there, and is refused then:
    // `n` is one number for the whole group.
    match expr.eval_in(&Scope::new(&Document::new())) {
        Ok(value) => Ok(Some(Expr::Literal(value))),
        Err(_) => Ok(Some(expr)),
    }
}

/// `n` as a positive integer.
pub(crate) fn read_n(op: &str, value: &Bson) -> Result<usize> {
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
    pub(crate) fn op(&self) -> &'static str {
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
            Kind::Top | Kind::Bottom | Kind::TopN | Kind::BottomN => {
                Body::Ranked(BinaryHeap::new())
            }
            Kind::MaxN | Kind::MinN => Body::Best(BinaryHeap::new()),
            Kind::FirstN | Kind::LastN => Body::Values(VecDeque::new()),
        };
        // A row ranks by its keys in the direction asked for; `$bottom*` keeps
        // the rows that rank last, so it ranks by the reverse direction and puts
        // them back in order when it answers.
        let bottom = matches!(self.kind, Kind::Bottom | Kind::BottomN);
        State {
            kind: self.kind,
            // `$top` and `$bottom` keep one row.
            limit: matches!(self.kind, Kind::Top | Kind::Bottom).then_some(1),
            seq: 0,
            dirs: self.sort.iter().map(|k| k.descending != bottom).collect(),
            body,
            held: 0,
        }
    }

    /// Per sort key, whether a row ranks by the reverse of the key's order: the key's own
    /// direction, reversed for `$bottom*`, which keeps the rows that rank last.
    pub(crate) fn dirs(&self) -> Arc<[bool]> {
        let bottom = matches!(self.kind, Kind::Bottom | Kind::BottomN);
        self.sort.iter().map(|k| k.descending != bottom).collect()
    }

    /// `n`, read the first time a document arrives.
    pub(crate) fn resolve(&self, scope: &Scope<'_>) -> Result<usize> {
        match &self.n {
            Some(expr) => read_n(self.op(), &expr.eval_in(scope)?),
            None => Ok(1),
        }
    }
}

/// What one group has seen.
#[derive(Clone, Debug)]
pub struct State {
    kind: Kind,
    limit: Option<usize>,
    /// Arrivals so far, the tie-break among rows that rank equal.
    seq: u64,
    /// Per sort key, whether a row ranks by the reverse of the key's order.
    dirs: Arc<[bool]>,
    body: Body,
    /// The BSON bytes of the rows and values kept now, for the request's budget
    /// (ADR-269): added when one is kept, taken off when one is dropped.
    held: usize,
}

#[derive(Clone, Debug)]
enum Body {
    Welford(Welford),
    Count(u64),
    /// `$top*` and `$bottom*`: the best `n` rows by rank, the worst at the top of
    /// the heap.
    Ranked(BinaryHeap<Row>),
    /// `$maxN` and `$minN`: the best `n` values, the worst at the top of the heap.
    Best(BinaryHeap<Best>),
    /// `$firstN` and `$lastN`, in arrival order.
    Values(VecDeque<Bson>),
}

#[derive(Clone, Debug)]
pub(crate) struct Row {
    pub(crate) keys: Vec<Bson>,
    pub(crate) output: Bson,
    pub(crate) seq: u64,
    pub(crate) dirs: Arc<[bool]>,
}

impl Row {
    /// Rank order, best first: key by key, then arrival. `flip` reads every
    /// direction the other way round.
    pub(crate) fn cmp_rank(&self, other: &Self, flip: bool) -> Ordering {
        for (i, (a, b)) in self.keys.iter().zip(&other.keys).enumerate() {
            let o = key_cmp(a, b);
            let o = if self.dirs.get(i).is_some_and(|d| *d != flip) { o.reverse() } else { o };
            if o != Ordering::Equal {
                return o;
            }
        }
        self.seq.cmp(&other.seq)
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.cmp_rank(other, false) == Ordering::Equal
    }
}
impl Eq for Row {}
impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Row {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_rank(other, false)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Best {
    pub(crate) value: Bson,
    pub(crate) seq: u64,
    /// `$maxN`: the larger value ranks first.
    pub(crate) max: bool,
}

impl PartialEq for Best {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Best {}
impl PartialOrd for Best {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Best {
    fn cmp(&self, other: &Self) -> Ordering {
        let o = canonical_cmp(&self.value, &other.value);
        let o = if self.max { o.reverse() } else { o };
        o.then_with(|| self.seq.cmp(&other.seq))
    }
}

/// Running mean and sum of squared deviations, updated one value at a time
/// (Welford), so a large offset does not cost the digits the spread needs.
#[derive(Clone, Debug, Default)]
pub(crate) struct Welford {
    pub(crate) n: u64,
    mean: f64,
    m2: f64,
}

impl Welford {
    pub(crate) fn add(&mut self, x: f64) {
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        self.m2 += delta * (x - self.mean);
    }

    pub(crate) fn remove(&mut self, x: f64) {
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

    pub(crate) fn finish(&self, sample: bool) -> Bson {
        match (self.n, sample) {
            (0, _) | (1, true) => Bson::Null,
            (n, true) => Bson::Double((self.m2 / (n - 1) as f64).sqrt()),
            (n, false) => Bson::Double((self.m2 / n as f64).sqrt()),
        }
    }
}

/// A numeric operand as a double; anything else is not counted. A `Decimal128`
/// is converted to the nearest double (documented deviation: the result is a
/// double either way).
pub(crate) fn numeric(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(n) => Some(f64::from(*n)),
        Bson::Int64(n) => Some(*n as f64),
        Bson::Double(d) => Some(*d),
        Bson::Decimal128(d) => Some(f64::from_bits(arith::to_binary64(&decode_raw(d)))),
        _ => None,
    }
}

/// The standard deviation of the numbers among `values`, as `$stdDevPop` /
/// `$stdDevSamp` answer it for a group; the array form of the operators uses it.
pub(crate) fn std_dev_of<'a>(values: impl IntoIterator<Item = &'a Bson>, sample: bool) -> Bson {
    let mut w = Welford::default();
    for x in values.into_iter().filter_map(numeric) {
        w.add(x);
    }
    w.finish(sample)
}

/// `$firstN`, `$lastN`, `$maxN` or `$minN` over the items of an array, as the
/// array form of the operators answers it.
pub(crate) fn n_of_array(kind: Kind, items: &[Bson], n: usize) -> Bson {
    let body = match kind {
        Kind::MaxN | Kind::MinN => Body::Best(BinaryHeap::new()),
        _ => Body::Values(VecDeque::new()),
    };
    let mut state = State { kind, limit: Some(n), seq: 0, dirs: Arc::from([]), body, held: 0 };
    for item in items {
        state.take(item.clone());
    }
    state.finish()
}

/// The value of `path` in `doc` as the reference's field path reads it: through
/// a document by key, through an array by collecting what each document in it
/// holds (the array of them, empty when none holds the path). A missing path is
/// `None`.
fn whole_value(doc: &Document, segs: &[&str]) -> Option<Bson> {
    let (head, rest) = segs.split_first()?;
    in_value(doc.get(*head)?, rest)
}

fn in_value(value: &Bson, segs: &[&str]) -> Option<Bson> {
    if segs.is_empty() {
        return Some(value.clone());
    }
    match value {
        Bson::Document(d) => whole_value(d, segs),
        Bson::Array(items) => Some(Bson::Array(
            items
                .iter()
                .filter_map(|item| match item {
                    Bson::Document(d) => whole_value(d, segs),
                    _ => None,
                })
                .collect(),
        )),
        _ => None,
    }
}

/// The value a document sorts by at `path`: the whole value, compared in the
/// canonical order (an array element by element, after strings). A missing path
/// is `null`.
pub(crate) fn sort_key(doc: &Document, path: &str) -> Bson {
    whole_value(doc, &path::segments(path)).unwrap_or(Bson::Null)
}

/// The canonical order, except that an empty array sorts below `null` (and above
/// `MinKey`), as the reference orders sort keys.
fn key_cmp(a: &Bson, b: &Bson) -> Ordering {
    let empty = |v: &Bson| matches!(v, Bson::Array(items) if items.is_empty());
    match (empty(a), empty(b)) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if matches!(b, Bson::MinKey) {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, true) => key_cmp(b, a).reverse(),
        (false, false) => canonical_cmp(a, b),
    }
}

/// The bytes a value takes in a group's state: its BSON payload and a type byte.
fn value_bytes(value: &Bson) -> usize {
    1 + crate::expr::sizes::bson_bytes(value)
}

impl Row {
    /// The bytes this row takes in a group's state.
    fn bytes(&self) -> usize {
        self.keys
            .iter()
            .fold(value_bytes(&self.output), |sum, k| sum.saturating_add(value_bytes(k)))
    }
}

impl State {
    /// The BSON bytes of the rows and values this group keeps now (ADR-269).
    pub fn held(&self) -> usize {
        self.held
    }

    /// Take one document.
    pub fn accumulate(&mut self, spec: &Spec, scope: &Scope<'_>) -> Result<()> {
        if self.limit.is_none() && self.needs_limit() {
            self.limit = Some(spec.resolve(scope)?);
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
                let keys: Vec<Bson> =
                    spec.sort.iter().map(|k| sort_key(scope.root(), &k.path)).collect();
                // Evaluate before deciding, so a failing `output` fails the
                // request whether or not the row would have been kept.
                let output = spec.value.eval_in(scope)?;
                let row = Row { keys, output, seq: self.seq, dirs: Arc::clone(&self.dirs) };
                self.held = self.held.saturating_add(row.bytes());
                rows.push(row);
                self.seq += 1;
                if rows.len() > limit
                    && let Some(dropped) = rows.pop()
                {
                    self.held = self.held.saturating_sub(dropped.bytes());
                }
            }
            Body::Best(_) | Body::Values(_) => {
                let value = spec.value.eval_in(scope)?;
                self.take(value);
            }
        }
        Ok(())
    }

    /// Take one value into a `$firstN`, `$lastN`, `$maxN` or `$minN`.
    fn take(&mut self, value: Bson) {
        let limit = self.limit.unwrap_or(usize::MAX);
        match &mut self.body {
            Body::Values(values) => {
                if self.kind == Kind::FirstN {
                    if values.len() < limit {
                        self.held = self.held.saturating_add(value_bytes(&value));
                        values.push_back(value);
                    }
                } else {
                    self.held = self.held.saturating_add(value_bytes(&value));
                    values.push_back(value);
                    if values.len() > limit
                        && let Some(dropped) = values.pop_front()
                    {
                        self.held = self.held.saturating_sub(value_bytes(&dropped));
                    }
                }
            }
            // `$maxN` and `$minN` skip null and missing.
            Body::Best(heap) if !matches!(value, Bson::Null | Bson::Undefined) => {
                self.held = self.held.saturating_add(value_bytes(&value));
                heap.push(Best { value, seq: self.seq, max: self.kind == Kind::MaxN });
                self.seq += 1;
                if heap.len() > limit
                    && let Some(dropped) = heap.pop()
                {
                    self.held = self.held.saturating_sub(value_bytes(&dropped.value));
                }
            }
            _ => {}
        }
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
        let bottom = matches!(self.kind, Kind::Bottom | Kind::BottomN);
        match self.body {
            Body::Welford(w) => w.finish(self.kind == Kind::StdDevSamp),
            Body::Count(n) => match i32::try_from(n) {
                Ok(n) => Bson::Int32(n),
                Err(_) => Bson::Int64(i64::try_from(n).unwrap_or(i64::MAX)),
            },
            Body::Ranked(rows) => {
                // `$bottom*` ranked by the reverse order to choose its rows and
                // answers them in the order asked for; equal rows stay in arrival
                // order either way.
                let mut rows = rows.into_vec();
                rows.sort_by(|a, b| a.cmp_rank(b, bottom));
                if single {
                    rows.into_iter().next().map_or(Bson::Null, |r| r.output)
                } else {
                    Bson::Array(rows.into_iter().map(|r| r.output).collect())
                }
            }
            Body::Best(heap) => {
                Bson::Array(heap.into_sorted_vec().into_iter().map(|b| b.value).collect())
            }
            Body::Values(values) => Bson::Array(values.into_iter().collect()),
        }
    }
}
