//! `$densify` (ADR-264): documents generated to close the gaps in a numeric or date field.
//!
//! ```text
//! {$densify: {
//!   field: <path>,
//!   partitionByFields: [<path>, ...],     // optional
//!   range: {step: <number>, unit: <unit>, bounds: "full" | "partition" | [lo, hi]}
//! }}
//! ```
//!
//! # What is generated
//!
//! In each partition the documents are taken in ascending order of `field`. Between one document
//! and the next, and from the start of the range to the first document, and from the last document
//! to the end of the range, a document is generated at every `step` from the value before: the
//! values `v + step`, `v + 2 step`, ... that fall before the next document's value. The generated
//! values are computed from the value they start at (`v + k step`, not by repeated addition), so a
//! month step from the 31st does not drift to the 28th. A generated document holds `field` and the
//! `partitionByFields`, and nothing else.
//!
//! The range runs from the lowest value to the highest, both included, for `"full"` (every
//! partition, over the lowest and highest value of the whole input) and `"partition"` (each
//! partition over its own); for `[lo, hi]` it is `lo` included to `hi` **excluded**, and documents
//! outside it are passed through untouched. Dates step by `unit` in UTC: a month, quarter or year
//! is a calendar step, a week or a day 7 or 1 days of 24 hours.
//!
//! # Order
//!
//! The output is partition by partition, partitions in ascending order of their field values, and
//! each partition in ascending order of `field`, generated documents among the others; documents of
//! the same value keep the order they arrived in. Without partitions it is one partition.
//!
//! # Limits
//!
//! The number of documents the range would generate is counted **before any is made** and held,
//! with the input, to the pipeline's document limit; the bytes buffered and generated are held to
//! [`crate::window::MAX_BUFFER_BYTES`]. A field that is missing, null, or neither a number nor a date
//! in some document is refused, as is a mixture of numbers and dates.

use std::cmp::Ordering;

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::{Error, Result, path};

use crate::aggregate::{Limits, check_limit, group_key};
use crate::expr::{add_to_date, is_date_unit, type_name};
use crate::window::{Budget, MAX_BUFFER_BYTES, fields_of};

fn bad<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::InvalidQuery(message.into()))
}

/// The distance between generated values.
#[derive(Clone, Debug, PartialEq)]
pub enum Step {
    Int(i64),
    Double(f64),
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

fn is_number(v: &Bson) -> bool {
    matches!(v, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_))
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
    let mut partition_fields = Vec::new();
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
            if partition_fields.contains(f) {
                return bad(format!("$densify `partitionByFields` names {f:?} twice"));
            }
            if *f == field
                || f.starts_with(&format!("{field}."))
                || field.starts_with(&format!("{f}."))
            {
                return bad(format!(
                    "$densify `partitionByFields` cannot hold the densified field {field:?}"
                ));
            }
            partition_fields.push(f.clone());
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
    let step = match range.get("step") {
        None => return bad("$densify `range` needs `step`"),
        Some(Bson::Int32(n)) => Step::Int(i64::from(*n)),
        Some(Bson::Int64(n)) => Step::Int(*n),
        Some(Bson::Double(d)) => Step::Double(*d),
        Some(other) => {
            return bad(format!(
                "$densify `step` must be a number (Decimal128 is not supported), found {}",
                type_name(other)
            ));
        }
    };
    match step {
        Step::Int(n) if n <= 0 => {
            return bad(format!("$densify `step` must be greater than 0, found {n}"));
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
                (a, b) if is_number(a) && is_number(b) => {
                    if unit.is_some() {
                        return bad("$densify `unit` is for dates, and these bounds are numbers");
                    }
                    if [a, b].iter().any(|v| matches!(v, Bson::Double(d) if !d.is_finite())) {
                        return bad("$densify bounds must be finite");
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

/// Run a parsed `$densify`.
pub fn apply(spec: &DensifySpec, input: Vec<Document>, limits: &Limits) -> Result<Vec<Document>> {
    apply_capped(spec, input, limits, MAX_BUFFER_BYTES)
}

/// One partition: its fields and its documents with their values, ascending.
struct Part {
    fields: Document,
    rows: Vec<(Bson, Document)>,
}

/// [`apply`] with the byte cap given, for tests.
pub fn apply_capped(
    spec: &DensifySpec,
    input: Vec<Document>,
    limits: &Limits,
    cap: usize,
) -> Result<Vec<Document>> {
    let mut budget = Budget::new("$densify", cap);
    let input_len = input.len();
    let mut parts: Vec<Part> = Vec::new();
    let mut index: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
    let mut dates: Option<bool> = None;
    for doc in input {
        budget.charge_doc(&doc)?;
        let value = match path::resolve(&doc, &spec.field).into_iter().next() {
            Some(v @ (Bson::Int32(_) | Bson::Int64(_) | Bson::DateTime(_))) => v.clone(),
            Some(Bson::Double(d)) if d.is_finite() => Bson::Double(*d),
            Some(Bson::Double(_)) => {
                return bad(format!(
                    "$densify cannot step from a non-finite value of {:?}",
                    spec.field
                ));
            }
            Some(Bson::Null) | None => {
                return bad(format!(
                    "$densify requires the field {:?} to be a number or a date in every \
                     document, found it missing or null",
                    spec.field
                ));
            }
            Some(other) => {
                return bad(format!(
                    "$densify requires the field {:?} to be a number or a date in every \
                     document, found {}",
                    spec.field,
                    type_name(other)
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
        let fields = fields_of(&doc, &spec.partition_fields);
        let slot = *index.entry(group_key(&Bson::Document(fields.clone()))).or_insert_with(|| {
            parts.push(Part { fields, rows: Vec::new() });
            parts.len() - 1
        });
        parts[slot].rows.push((value, doc));
    }
    let Some(is_date) = dates else { return Ok(Vec::new()) };
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

    // Partitions in ascending order of their field values, each by ascending value.
    let partition_key = |p: &Part| -> Vec<Bson> {
        spec.partition_fields
            .iter()
            .map(|f| path::resolve(&p.fields, f).into_iter().next().cloned().unwrap_or(Bson::Null))
            .collect()
    };
    parts.sort_by(|a, b| {
        let (ka, kb) = (partition_key(a), partition_key(b));
        ka.iter()
            .zip(&kb)
            .map(|(x, y)| canonical_cmp(x, y))
            .find(|o| o.is_ne())
            .unwrap_or(Ordering::Equal)
    });
    for part in &mut parts {
        part.rows.sort_by(|a, b| canonical_cmp(&a.0, &b.0));
    }

    // The ends of the range, for every partition or for each.
    let extent = |rows: &mut dyn Iterator<Item = &Bson>| -> Option<(Bson, Bson)> {
        let mut lo: Option<&Bson> = None;
        let mut hi: Option<&Bson> = None;
        for v in rows {
            if lo.is_none_or(|l| canonical_cmp(v, l).is_lt()) {
                lo = Some(v);
            }
            if hi.is_none_or(|h| canonical_cmp(v, h).is_gt()) {
                hi = Some(v);
            }
        }
        lo.zip(hi).map(|(l, h)| (l.clone(), h.clone()))
    };
    let global = extent(&mut parts.iter().flat_map(|p| p.rows.iter().map(|r| &r.0)));
    let range_of = |part: &Part| -> (Bson, Bson, bool) {
        match &spec.bounds {
            Bounds::Explicit(b) => (b.0.clone(), b.1.clone(), false),
            Bounds::Full => {
                let (lo, hi) = global.clone().expect("a non-empty input has an extent");
                (lo, hi, true)
            }
            Bounds::Partition => {
                let (lo, hi) = extent(&mut part.rows.iter().map(|r| &r.0))
                    .expect("a partition holds a document");
                (lo, hi, true)
            }
        }
    };

    // First pass: how many documents the range makes, before any is made.
    let walker = Walker { spec, is_date };
    let mut total = input_len;
    for part in &parts {
        let (lo, hi, inclusive) = range_of(part);
        walker.walk(part, &lo, &hi, inclusive, &mut |event| {
            if let Event::Gap(base, include_base, upto, upto_inclusive) = event {
                let n = walker.count(base, include_base, upto, upto_inclusive)?;
                total = total.saturating_add(n);
                check_limit_generated(total, limits)?;
            }
            Ok(())
        })?;
    }

    // Second pass: the documents.
    let mut out: Vec<Document> = Vec::with_capacity(total);
    for part in parts {
        let (lo, hi, inclusive) = range_of(&part);
        let mut emitted: Vec<Document> = Vec::new();
        walker.walk(&part, &lo, &hi, inclusive, &mut |event| {
            match event {
                Event::Doc(at) => emitted.push(part.rows[at].1.clone()),
                Event::Gap(base, include_base, upto, upto_inclusive) => {
                    for value in walker.values(base, include_base, upto, upto_inclusive)? {
                        let generated = walker.generate(&part.fields, value)?;
                        budget.charge_doc(&generated)?;
                        emitted.push(generated);
                    }
                }
            }
            Ok(())
        })?;
        out.extend(emitted);
    }
    Ok(out)
}

fn check_limit_generated(total: usize, limits: &Limits) -> Result<()> {
    if total > limits.max_documents {
        return Err(Error::Limit(format!(
            "$densify would produce {total} documents, over the pipeline limit of {}. Narrow the \
             range, widen the step, or narrow the pipeline with an earlier $match",
            limits.max_documents
        )));
    }
    check_limit("$densify", total, limits)
}

enum Event<'a> {
    /// The document at this row of the partition, passed through.
    Doc(usize),
    /// Generate from `base` (itself too, when the flag is set) up to a value: `(base, include
    /// base, upto, upto included)`.
    Gap(&'a Bson, bool, &'a Bson, bool),
}

struct Walker<'a> {
    spec: &'a DensifySpec,
    is_date: bool,
}

impl Walker<'_> {
    /// The `k`th value after `base`: `base + k * step`, or `None` when it does not exist (it
    /// overflows).
    fn at(&self, base: &Bson, k: i64) -> Option<Bson> {
        if self.is_date {
            let Bson::DateTime(d) = base else { return None };
            let step = match self.spec.step {
                Step::Int(n) => n,
                Step::Double(x) => x as i64,
            };
            return add_to_date(
                d.timestamp_millis(),
                self.spec.unit.as_deref()?,
                step.checked_mul(k)?,
                "$densify",
            )
            .ok()
            .map(|ms| Bson::DateTime(bson::DateTime::from_millis(ms)));
        }
        match (&self.spec.step, base) {
            (Step::Int(step), Bson::Int32(b)) => {
                let v = i128::from(*b) + i128::from(*step) * i128::from(k);
                if let (Ok(v32), true) = (i32::try_from(v), i32::try_from(*step).is_ok()) {
                    Some(Bson::Int32(v32))
                } else {
                    i64::try_from(v).ok().map(Bson::Int64)
                }
            }
            (Step::Int(step), Bson::Int64(b)) => {
                let v = i128::from(*b) + i128::from(*step) * i128::from(k);
                i64::try_from(v).ok().map(Bson::Int64)
            }
            (step, base) => {
                let b = match base {
                    Bson::Int32(n) => f64::from(*n),
                    Bson::Int64(n) => *n as f64,
                    Bson::Double(d) => *d,
                    _ => return None,
                };
                let s = match step {
                    Step::Int(n) => *n as f64,
                    Step::Double(d) => *d,
                };
                let v = b + s * k as f64;
                v.is_finite().then_some(Bson::Double(v))
            }
        }
    }

    /// Whether the `k`th value is inside the gap.
    fn inside(&self, base: &Bson, k: i64, upto: &Bson, upto_inclusive: bool) -> bool {
        match self.at(base, k) {
            None => false,
            Some(v) => match canonical_cmp(&v, upto) {
                Ordering::Less => true,
                Ordering::Equal => upto_inclusive,
                Ordering::Greater => false,
            },
        }
    }

    /// How many values the gap holds: found by search, since the values grow with `k`.
    fn count(
        &self,
        base: &Bson,
        include_base: bool,
        upto: &Bson,
        upto_inclusive: bool,
    ) -> Result<usize> {
        let first = i64::from(!include_base);
        if !self.inside(base, first, upto, upto_inclusive) {
            return Ok(0);
        }
        // Double until outside, then bisect between the last inside and the first outside.
        let (mut inside, mut outside) = (first, first.max(1));
        while self.inside(base, outside, upto, upto_inclusive) {
            inside = outside;
            match outside.checked_mul(2) {
                Some(next) => outside = next,
                None => {
                    outside = i64::MAX;
                    if self.inside(base, outside, upto, upto_inclusive) {
                        return Ok(usize::MAX);
                    }
                    break;
                }
            }
        }
        while outside - inside > 1 {
            let mid = inside + (outside - inside) / 2;
            if self.inside(base, mid, upto, upto_inclusive) {
                inside = mid;
            } else {
                outside = mid;
            }
        }
        Ok(usize::try_from(inside - first + 1).unwrap_or(usize::MAX))
    }

    fn values(
        &self,
        base: &Bson,
        include_base: bool,
        upto: &Bson,
        upto_inclusive: bool,
    ) -> Result<Vec<Bson>> {
        let mut out = Vec::new();
        let mut k = i64::from(!include_base);
        while self.inside(base, k, upto, upto_inclusive) {
            out.push(self.at(base, k).expect("inside implies a value"));
            k += 1;
        }
        Ok(out)
    }

    fn generate(&self, partition: &Document, value: Bson) -> Result<Document> {
        let mut doc = Document::new();
        path::set(&mut doc, &self.spec.field, value)
            .map_err(|e| Error::InvalidQuery(format!("cannot set {:?}: {e}", self.spec.field)))?;
        for f in &self.spec.partition_fields {
            if let Some(v) = path::resolve(partition, f).into_iter().next() {
                path::set(&mut doc, f, v.clone())
                    .map_err(|e| Error::InvalidQuery(format!("cannot set {f:?}: {e}")))?;
            }
        }
        Ok(doc)
    }

    /// Walk a partition in value order, calling `on` for each document and each gap to fill.
    /// `[lo, hi)` is the range (`hi` included when `inclusive`).
    fn walk(
        &self,
        part: &Part,
        lo: &Bson,
        hi: &Bson,
        inclusive: bool,
        on: &mut dyn FnMut(Event<'_>) -> Result<()>,
    ) -> Result<()> {
        let mut base: &Bson = lo;
        let mut include_base = true;
        let mut finished = false;
        let past = |v: &Bson| match canonical_cmp(v, hi) {
            Ordering::Greater => true,
            Ordering::Equal => !inclusive,
            Ordering::Less => false,
        };
        for (at, (value, _)) in part.rows.iter().enumerate() {
            if canonical_cmp(value, lo).is_lt() {
                on(Event::Doc(at))?;
                continue;
            }
            if past(value) {
                if !finished {
                    on(Event::Gap(base, include_base, hi, inclusive))?;
                    finished = true;
                }
                on(Event::Doc(at))?;
                continue;
            }
            on(Event::Gap(base, include_base, value, false))?;
            on(Event::Doc(at))?;
            base = value;
            include_base = false;
        }
        if !finished {
            on(Event::Gap(base, include_base, hi, inclusive))?;
        }
        Ok(())
    }
}
