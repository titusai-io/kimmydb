//! `$densify` (ADR-264): documents generated to close the gaps in a numeric or date field.
//!
//! ```text
//! {$densify: {
//!   field: <path>,
//!   partitionByFields: [<path>, ...],     // optional
//!   range: {step: <num>, unit: <unit>, bounds: "full" | "partition" | [lo, hi]}
//! }}
//! ```
//!
//! # What is generated
//!
//! The range has a start, and from the start a **grid**: the start, the start plus a step, plus
//! another, and so on. A document is generated at every grid point of the range that no document
//! of the partition is at. The grid is built by adding the step to the point before it, so a date
//! stepped by months from the 31st drifts to the 28th and stays there (31 Jan, 28 Feb, 28 Mar), as
//! the reference's does, and a fraction accumulates as it does when added. A generated document
//! holds the partition fields and then `field`, and nothing else.
//!
//! `"full"` starts every partition at the lowest value of the whole input and ends it at the
//! highest, which is included; `"partition"` starts and ends each partition at its own lowest and
//! highest value; `[lo, hi]` runs from `lo` included to `hi` excluded (`[lo, lo]` is `lo`), and
//! documents outside it are passed through. Dates step by `unit` in UTC.
//!
//! A document whose field is missing or null is passed through, ahead of all the others, in the
//! order it arrived. A field that is some other kind of value, or a number in one document and a date
//! in another, is refused.
//!
//! # Order
//!
//! With `"full"` the output is one sequence in ascending order of `field`, the partitions
//! interleaved: before each document, the grid points of its partition that are below it and
//! that the partition has not reached; at the end, the rest of each partition's grid, partition by
//! partition in ascending order of the partition values. (The reference ends in a hash order.)
//! With `"partition"` and with `[lo, hi]` it is partition by partition in ascending order of the
//! partition values, `null` before a missing field, and each partition in ascending order of
//! `field`. Documents of one value keep the order they arrived in.
//!
//! # Limits
//!
//! The documents the range would generate are counted **before any is made**, by walking the same
//! grid without building a document, and held with the input to the pipeline's document limit,
//! which ends the count at the first one over; the bytes buffered and generated are held to the
//! request's byte budget ([`crate::budget`]).

use std::cmp::Ordering;

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::{Error, Result, path};

use crate::aggregate::{Limits, check_limit, group_key};
use crate::budget::Lease;
use crate::expr::{add_to_date, is_date_unit, type_name};
use crate::window::{buffer_name, compare_fields, fields_of};

fn bad<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::InvalidQuery(message.into()))
}

/// The distance between grid points.
#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    Int32(i32),
    Int64(i64),
    Double(f64),
}

impl Step {
    fn as_i64(&self) -> i64 {
        match self {
            Step::Int32(n) => i64::from(*n),
            Step::Int64(n) => *n,
            Step::Double(d) => *d as i64,
        }
    }
}

/// How far the range reaches.
#[derive(Clone, Debug, PartialEq)]
pub enum Bounds {
    Full,
    Partition,
    /// `[lo, hi)`, both of one kind.
    Explicit(Box<(Bson, Bson)>),
}

/// A parsed `$densify`.
#[derive(Clone, Debug)]
pub struct DensifySpec {
    pub field: String,
    pub partition_fields: Vec<String>,
    pub step: Step,
    pub unit: Option<String>,
    pub bounds: Bounds,
}

fn field_path(what: &str, name: &str) -> Result<()> {
    if name.is_empty()
        || name.starts_with('$')
        || name.contains('\0')
        || name.split('.').any(str::is_empty)
    {
        return bad(format!(
            "$densify {what} is a field path without a leading `$`, found {name:?}"
        ));
    }
    Ok(())
}

/// The numeric type of a bound or step: the three the reference lets agree.
fn number_type(v: &Bson) -> Option<&'static str> {
    match v {
        Bson::Int32(_) => Some("int"),
        Bson::Int64(_) => Some("long"),
        Bson::Double(_) => Some("double"),
        _ => None,
    }
}

pub fn parse(spec: &Document) -> Result<DensifySpec> {
    for key in spec.keys() {
        if !matches!(key.as_str(), "field" | "partitionByFields" | "range") {
            return bad(format!(
                "$densify does not take `{key}`; its fields are field, partitionByFields and range"
            ));
        }
    }
    let field = match spec.get("field") {
        Some(Bson::String(f)) => f.clone(),
        Some(other) => {
            return bad(format!("$densify `field` must be a string, found {}", type_name(other)));
        }
        None => return bad("$densify needs `field`"),
    };
    field_path("`field`", &field)?;
    let mut partition_fields: Vec<String> = Vec::new();
    if let Some(value) = spec.get("partitionByFields") {
        let Bson::Array(items) = value else {
            return bad(format!(
                "$densify `partitionByFields` must be an array of field paths, found {}",
                type_name(value)
            ));
        };
        for item in items {
            let Bson::String(f) = item else {
                return bad(format!(
                    "$densify `partitionByFields` holds field paths, found {}",
                    type_name(item)
                ));
            };
            field_path("`partitionByFields` entry", f)?;
            if *f == field
                || f.starts_with(&format!("{field}."))
                || field.starts_with(&format!("{f}."))
            {
                return bad(format!(
                    "$densify `partitionByFields` cannot hold the densified field {field:?}"
                ));
            }
            // A field named twice is one field.
            if !partition_fields.contains(f) {
                partition_fields.push(f.clone());
            }
        }
    }
    let range = match spec.get("range") {
        Some(Bson::Document(r)) => r,
        Some(other) => {
            return bad(format!("$densify `range` must be a document, found {}", type_name(other)));
        }
        None => return bad("$densify needs `range`: {step, unit, bounds}"),
    };
    for key in range.keys() {
        if !matches!(key.as_str(), "step" | "unit" | "bounds") {
            return bad(format!(
                "$densify `range` does not take `{key}`; its fields are step, unit and bounds"
            ));
        }
    }
    let unit = match range.get("unit") {
        None => None,
        Some(Bson::String(u)) if is_date_unit(u) => Some(u.clone()),
        Some(Bson::String(u)) => {
            return bad(format!(
                "$densify `unit` must be one of year, quarter, month, week, day, hour, minute, \
                 second, millisecond, found {u:?}"
            ));
        }
        Some(other) => {
            return bad(format!("$densify `unit` must be a string, found {}", type_name(other)));
        }
    };
    let step_value = range.get("step");
    let step = match step_value {
        None => return bad("$densify `range` needs `step`"),
        Some(Bson::Int32(n)) => Step::Int32(*n),
        Some(Bson::Int64(n)) => Step::Int64(*n),
        Some(Bson::Double(d)) => Step::Double(*d),
        Some(other) => {
            return bad(format!(
                "$densify `step` must be a number (Decimal128 is not supported), found {}",
                type_name(other)
            ));
        }
    };
    match step {
        Step::Int32(_) | Step::Int64(_) if step.as_i64() <= 0 => {
            return bad(format!("$densify `step` must be greater than 0, found {}", step.as_i64()));
        }
        Step::Double(d) if !(d.is_finite() && d > 0.0) => {
            return bad(format!(
                "$densify `step` must be a finite number greater than 0, found {d}"
            ));
        }
        Step::Double(d) if unit.is_some() && d.fract() != 0.0 => {
            return bad(format!("$densify `step` must be a whole number with a `unit`, found {d}"));
        }
        _ => {}
    }
    let bounds = match range.get("bounds") {
        None => return bad("$densify `range` needs `bounds`: \"full\", \"partition\" or [lo, hi]"),
        Some(Bson::String(s)) if s == "full" => Bounds::Full,
        Some(Bson::String(s)) if s == "partition" => {
            if partition_fields.is_empty() {
                return bad("$densify bounds \"partition\" needs `partitionByFields`");
            }
            Bounds::Partition
        }
        Some(Bson::String(s)) => {
            return bad(format!(
                "$densify `bounds` is \"full\", \"partition\" or [lo, hi], found {s:?}"
            ));
        }
        Some(Bson::Array(items)) if items.len() == 2 => {
            let (lo, hi) = (&items[0], &items[1]);
            match (lo, hi) {
                (Bson::DateTime(_), Bson::DateTime(_)) => {
                    if unit.is_none() {
                        return bad("$densify with date bounds needs a `unit`");
                    }
                }
                (a, b) if number_type(a).is_some() && number_type(b).is_some() => {
                    if unit.is_some() {
                        return bad("$densify `unit` is for dates, and these bounds are numbers");
                    }
                    if [a, b].iter().any(|v| matches!(v, Bson::Double(d) if !d.is_finite())) {
                        return bad("$densify bounds must be finite");
                    }
                    // The bounds and the step are one numeric type, as the reference needs.
                    let types =
                        [number_type(a), number_type(b), number_type(step_value.unwrap_or(a))];
                    if types.iter().any(|t| *t != types[0]) {
                        return bad(
                            "$densify explicit bounds and the step must all be of the same \
                             numeric type: int, long or double",
                        );
                    }
                }
                (a, b) => {
                    return bad(format!(
                        "$densify explicit bounds are two numbers or two dates, found {} and {}",
                        type_name(a),
                        type_name(b)
                    ));
                }
            }
            if canonical_cmp(lo, hi) == Ordering::Greater {
                return bad("$densify lower bound must not be above the upper bound");
            }
            Bounds::Explicit(Box::new((lo.clone(), hi.clone())))
        }
        Some(other) => {
            return bad(format!(
                "$densify `bounds` is \"full\", \"partition\" or [lo, hi], found {}",
                type_name(other)
            ));
        }
    };
    Ok(DensifySpec { field, partition_fields, step, unit, bounds })
}

/// What a row of the work is: a document with a value of the field.
struct Row {
    value: Bson,
    doc: usize,
    part: usize,
}

/// One partition: the fields a generated document carries, and the values that order it.
struct Part {
    fields: Document,
    rep: Vec<Option<Bson>>,
}

/// Where a grid has got to in one partition.
struct Cursor {
    /// The next grid point to consider.
    next: Bson,
    /// The grid has run out of room (an integer past what it can hold).
    done: bool,
}

/// The grid: the step added to the point before.
struct Grid<'a> {
    step: &'a Step,
    unit: Option<&'a str>,
}

impl Grid<'_> {
    /// The grid point after `from`; `None` when an integer or a double has no room for it.
    fn after(&self, from: &Bson) -> Result<Option<Bson>> {
        if let Bson::DateTime(d) = from {
            let unit = self.unit.unwrap_or("millisecond");
            let ms = add_to_date(d.timestamp_millis(), unit, self.step.as_i64(), "$densify")?;
            return Ok(Some(Bson::DateTime(bson::DateTime::from_millis(ms))));
        }
        Ok(match (from, self.step) {
            (Bson::Int32(b), Step::Int32(s)) => {
                let v = i64::from(*b) + i64::from(*s);
                Some(match i32::try_from(v) {
                    Ok(v) => Bson::Int32(v),
                    Err(_) => Bson::Int64(v),
                })
            }
            (Bson::Int32(b), Step::Int64(s)) => i64::from(*b).checked_add(*s).map(Bson::Int64),
            (Bson::Int64(b), Step::Int32(s)) => b.checked_add(i64::from(*s)).map(Bson::Int64),
            (Bson::Int64(b), Step::Int64(s)) => b.checked_add(*s).map(Bson::Int64),
            (base, step) => {
                let b = match base {
                    Bson::Int32(n) => f64::from(*n),
                    Bson::Int64(n) => *n as f64,
                    Bson::Double(d) => *d,
                    _ => return Ok(None),
                };
                let s = match step {
                    Step::Int32(n) => f64::from(*n),
                    Step::Int64(n) => *n as f64,
                    Step::Double(d) => *d,
                };
                let v = b + s;
                if v.is_finite() && v <= b {
                    // Adding the step changes nothing at this size: the grid would never
                    // move, and the documents it asks for never end.
                    return Err(Error::Limit(format!(
                        "$densify step {s} is too small to move the grid from {b}"
                    )));
                }
                v.is_finite().then_some(Bson::Double(v))
            }
        })
    }
}

/// The range of one partition: its start, its end, and whether the end is a grid point.
struct Span {
    lo: Bson,
    hi: Bson,
    inclusive: bool,
}

impl Span {
    fn contains(&self, g: &Bson) -> bool {
        match canonical_cmp(g, &self.hi) {
            Ordering::Less => true,
            Ordering::Equal => self.inclusive,
            Ordering::Greater => false,
        }
    }
}

/// Where the walk puts what it finds: the count pass counts, the second pass builds.
trait Sink {
    fn doc(&mut self, row: &Row) -> Result<()>;
    fn generated(&mut self, part: usize, value: Bson) -> Result<()>;
}

/// Generate the grid points of `span` below `before` (all of them with `None`) that the
/// cursor has not passed.
fn catch_up(
    grid: &Grid<'_>,
    cursor: &mut Cursor,
    span: &Span,
    before: Option<&Bson>,
    part: usize,
    sink: &mut dyn Sink,
) -> Result<()> {
    while !cursor.done && span.contains(&cursor.next) {
        if before.is_some_and(|v| canonical_cmp(&cursor.next, v).is_ge()) {
            break;
        }
        sink.generated(part, cursor.next.clone())?;
        match grid.after(&cursor.next)? {
            Some(next) => cursor.next = next,
            None => cursor.done = true,
        }
    }
    Ok(())
}

/// Take a document at `value` into the grid: the point it sits on is not generated.
fn pass(grid: &Grid<'_>, cursor: &mut Cursor, value: &Bson) -> Result<()> {
    while !cursor.done && canonical_cmp(&cursor.next, value).is_eq() {
        match grid.after(&cursor.next)? {
            Some(next) => cursor.next = next,
            None => cursor.done = true,
        }
    }
    Ok(())
}

/// Run a parsed `$densify`.
pub fn apply(spec: &DensifySpec, input: Vec<Document>, limits: &Limits) -> Result<Vec<Document>> {
    let mut lease = limits.buffer.lease(buffer_name("$densify"));
    let input_len = input.len();

    // Read every document: its value of the field, its partition.
    let mut docs: Vec<Option<Document>> = Vec::with_capacity(input_len);
    let mut passthrough: Vec<usize> = Vec::new();
    let mut rows: Vec<Row> = Vec::new();
    let mut parts: Vec<Part> = Vec::new();
    let mut index: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
    let mut dates: Option<bool> = None;
    for doc in input {
        lease.charge_doc(&doc)?;
        let at = docs.len();
        let value = match path::resolve(&doc, &spec.field).into_iter().next() {
            // A document without the field is not part of the series.
            Some(Bson::Null) | None => {
                passthrough.push(at);
                docs.push(Some(doc));
                continue;
            }
            Some(v @ (Bson::Int32(_) | Bson::Int64(_) | Bson::DateTime(_))) => v.clone(),
            Some(Bson::Double(d)) if d.is_finite() => Bson::Double(*d),
            Some(Bson::Double(_)) => {
                return bad(format!(
                    "$densify cannot step from a non-finite value of {:?}",
                    spec.field
                ));
            }
            Some(other) => {
                return bad(format!(
                    "$densify requires the field {:?} to be a number or a date in every \
                     document, found {}",
                    spec.field,
                    match other {
                        Bson::Decimal128(_) => "a Decimal128 (not supported)",
                        other => type_name(other),
                    }
                ));
            }
        };
        let is_date = matches!(value, Bson::DateTime(_));
        if dates.is_some_and(|d| d != is_date) {
            return bad(format!(
                "$densify requires the field {:?} to be all numbers or all dates, found both",
                spec.field
            ));
        }
        dates = Some(is_date);
        let held = fields_of(&doc, &spec.partition_fields);
        let part = *index.entry(group_key(&Bson::Document(held.clone()))).or_insert_with(|| {
            let rep = spec
                .partition_fields
                .iter()
                .map(|f| path::resolve(&held, f).into_iter().next().cloned())
                .collect();
            parts.push(Part { fields: held, rep });
            parts.len() - 1
        });
        rows.push(Row { value, doc: at, part });
        docs.push(Some(doc));
    }
    if let Some(is_date) = dates {
        match (&spec.unit, is_date) {
            (None, true) => return bad("$densify over a date field needs a `unit`"),
            (Some(_), false) => {
                return bad("$densify `unit` is for a date field, and this one is numeric");
            }
            _ => {}
        }
        if let Bounds::Explicit(bounds) = &spec.bounds
            && matches!(bounds.0, Bson::DateTime(_)) != is_date
        {
            return bad(format!(
                "$densify bounds and the field {:?} must be of one kind, both numbers or both dates",
                spec.field
            ));
        }
    }

    // Partitions in ascending order of their values, by value within.
    let mut order: Vec<usize> = (0..parts.len()).collect();
    order.sort_by(|&a, &b| compare_fields(&parts[a].rep, &parts[b].rep));
    rows.sort_by(|a, b| canonical_cmp(&a.value, &b.value));

    let grid = Grid { step: &spec.step, unit: spec.unit.as_deref() };
    let spans = spans(spec, &rows, &parts);

    // First pass: how many documents the range makes, before any is made.
    let mut count = Count { total: input_len, limits };
    walk(spec, &grid, &rows, &order, &spans, &mut count)?;

    // Second pass: the documents.
    let mut made = Build {
        spec,
        parts: &parts,
        docs: &mut docs,
        out: Vec::with_capacity(count.total),
        lease: &mut lease,
    };
    // Documents without a value lead, as they arrived.
    for &at in &passthrough {
        if let Some(doc) = made.docs[at].take() {
            made.out.push(doc);
        }
    }
    walk(spec, &grid, &rows, &order, &spans, &mut made)?;
    Ok(made.out)
}

/// The span of every partition, by partition index.
fn spans(spec: &DensifySpec, rows: &[Row], parts: &[Part]) -> Vec<Option<Span>> {
    // The lowest and highest value of each partition, and of all of them.
    let mut own: Vec<Option<(Bson, Bson)>> = vec![None; parts.len()];
    let mut all: Option<(Bson, Bson)> = None;
    let widen = |found: &mut Option<(Bson, Bson)>, v: &Bson| match found {
        None => *found = Some((v.clone(), v.clone())),
        Some((lo, hi)) => {
            if canonical_cmp(v, lo).is_lt() {
                *lo = v.clone();
            }
            if canonical_cmp(v, hi).is_gt() {
                *hi = v.clone();
            }
        }
    };
    for r in rows {
        widen(&mut own[r.part], &r.value);
        widen(&mut all, &r.value);
    }
    let inclusive = |(lo, hi): (Bson, Bson)| Span { lo, hi, inclusive: true };
    match &spec.bounds {
        Bounds::Explicit(bounds) => {
            let inclusive = canonical_cmp(&bounds.0, &bounds.1).is_eq();
            (0..parts.len())
                .map(|_| Some(Span { lo: bounds.0.clone(), hi: bounds.1.clone(), inclusive }))
                .collect()
        }
        Bounds::Full => (0..parts.len()).map(|_| all.clone().map(inclusive)).collect(),
        Bounds::Partition => own.into_iter().map(|e| e.map(inclusive)).collect(),
    }
}

/// One document: bring its partition's grid up to it; below the start, pass it by.
fn take(
    grid: &Grid<'_>,
    spans: &[Option<Span>],
    cursors: &mut [Option<Cursor>],
    row: &Row,
    sink: &mut dyn Sink,
) -> Result<()> {
    if let (Some(span), Some(cursor)) = (&spans[row.part], &mut cursors[row.part])
        && canonical_cmp(&row.value, &span.lo).is_ge()
    {
        catch_up(grid, cursor, span, Some(&row.value), row.part, sink)?;
        pass(grid, cursor, &row.value)?;
    }
    sink.doc(row)
}

/// The rest of a partition's grid, to the end of its range.
fn finish(
    grid: &Grid<'_>,
    spans: &[Option<Span>],
    cursors: &mut [Option<Cursor>],
    part: usize,
    sink: &mut dyn Sink,
) -> Result<()> {
    if let (Some(span), Some(cursor)) = (&spans[part], &mut cursors[part]) {
        catch_up(grid, cursor, span, None, part, sink)?;
    }
    Ok(())
}

/// Walk the documents and the grids in the order the output is in.
fn walk(
    spec: &DensifySpec,
    grid: &Grid<'_>,
    rows: &[Row],
    order: &[usize],
    spans: &[Option<Span>],
    sink: &mut dyn Sink,
) -> Result<()> {
    let mut cursors: Vec<Option<Cursor>> = spans
        .iter()
        .map(|s| s.as_ref().map(|s| Cursor { next: s.lo.clone(), done: false }))
        .collect();
    match spec.bounds {
        Bounds::Full => {
            // One sequence in order of the field; the grids' tails after it.
            for row in rows {
                take(grid, spans, &mut cursors, row, sink)?;
            }
            for &p in order {
                finish(grid, spans, &mut cursors, p, sink)?;
            }
        }
        Bounds::Partition | Bounds::Explicit(_) => {
            let mut by_part: Vec<Vec<usize>> = vec![Vec::new(); spans.len()];
            for (i, r) in rows.iter().enumerate() {
                by_part[r.part].push(i);
            }
            for &p in order {
                for &i in &by_part[p] {
                    take(grid, spans, &mut cursors, &rows[i], sink)?;
                }
                finish(grid, spans, &mut cursors, p, sink)?;
            }
        }
    }
    Ok(())
}

struct Count<'a> {
    total: usize,
    limits: &'a Limits,
}

impl Sink for Count<'_> {
    fn doc(&mut self, _: &Row) -> Result<()> {
        Ok(())
    }

    fn generated(&mut self, _: usize, _: Bson) -> Result<()> {
        self.total = self.total.saturating_add(1);
        if self.total > self.limits.max_documents {
            return Err(Error::Limit(format!(
                "$densify would produce more than {} documents, over the pipeline limit of {}. \
                 Narrow the range, widen the step, or narrow the pipeline with an earlier $match",
                self.total - 1,
                self.limits.max_documents
            )));
        }
        check_limit("$densify", self.total, self.limits)
    }
}

struct Build<'a> {
    spec: &'a DensifySpec,
    parts: &'a [Part],
    docs: &'a mut Vec<Option<Document>>,
    out: Vec<Document>,
    lease: &'a mut Lease,
}

impl Sink for Build<'_> {
    fn doc(&mut self, row: &Row) -> Result<()> {
        if let Some(doc) = self.docs[row.doc].take() {
            self.out.push(doc);
        }
        Ok(())
    }

    fn generated(&mut self, part: usize, value: Bson) -> Result<()> {
        // The partition fields first, then the densified field.
        let mut doc = Document::new();
        for f in &self.spec.partition_fields {
            if let Some(v) = path::resolve(&self.parts[part].fields, f).into_iter().next() {
                path::set(&mut doc, f, v.clone())
                    .map_err(|e| Error::InvalidQuery(format!("cannot set {f:?}: {e}")))?;
            }
        }
        path::set(&mut doc, &self.spec.field, value)
            .map_err(|e| Error::InvalidQuery(format!("cannot set {:?}: {e}", self.spec.field)))?;
        self.lease.charge_doc(&doc)?;
        self.out.push(doc);
        Ok(())
    }
}
