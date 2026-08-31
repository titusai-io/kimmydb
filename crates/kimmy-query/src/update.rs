//! Update operators.
//!
//! An update document is either a set of `$`-prefixed operators or a whole
//! replacement document — never a mix, because the two mean very different
//! things and guessing would silently discard fields.

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::{Error, Result};
use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::filter::{self, Filter};
use crate::path;

/// What an update document asks for.
#[derive(Clone, Debug, PartialEq)]
pub enum Update {
    /// Replace the document wholesale, preserving `_id`.
    Replace(Document),
    /// Apply operators in order.
    Operators(Vec<Operation>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Operation {
    pub path: String,
    pub kind: OpKind,
    /// The `arrayFilters` entries this path's `$[<identifier>]` segments name,
    /// resolved at parse time so that applying the operation needs nothing
    /// but the document. Empty for a path with no named positional segment.
    pub array_filters: Vec<ArrayFilter>,
}

/// One entry of a request's `arrayFilters`: the identifier that `$[<identifier>]`
/// segments refer to it by, and what an array element must satisfy to be
/// addressed through it.
#[derive(Clone, Debug, PartialEq)]
pub struct ArrayFilter {
    pub identifier: String,
    /// The filter with its identifier prefix removed, evaluated per element by
    /// [`filter::matches_element`].
    pub filter: Filter,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OpKind {
    Set(Bson),
    Unset,
    Inc(Bson),
    Mul(Bson),
    /// Set only if the new value is smaller than the current one.
    Min(Bson),
    /// Set only if the new value is larger than the current one.
    Max(Bson),
    Push(Bson),
    /// Append several values (`$push` with `$each`).
    PushEach(Vec<Bson>),
    /// Append only values not already present.
    AddToSet(Vec<Bson>),
    /// Remove every element equal to this value.
    Pull(Bson),
    /// Remove the first (`-1`) or last (`1`) element.
    Pop(i32),
    Rename(String),
    CurrentDate,
}

/// The primary key field, which updates may not move.
const ID_FIELD: &str = "_id";

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

pub fn parse(doc: &Document) -> Result<Update> {
    parse_with_filters(doc, &[])
}

/// Parse an update together with the request's `arrayFilters`.
///
/// A path may contain `$[]` (every element) and `$[<identifier>]` (the
/// elements a filter selects) segments. Each filter document names exactly
/// one identifier — every top-level field starts with it — and is evaluated
/// against each element with that prefix removed: `{"line.qty": {$gt: 5}}`
/// tests `{qty: {$gt: 5}}` against each element, and a bare `{"line": {$gt:
/// 5}}` tests the element itself, which is how an array of scalars is
/// addressed. Every identifier a path uses must have a filter, and every
/// filter must be used by some path: a filter nothing refers to is nearly
/// always a misspelt identifier, and applying the update regardless would
/// change elements the caller never selected.
pub fn parse_with_filters(doc: &Document, array_filters: &[Document]) -> Result<Update> {
    let has_operators = doc.keys().any(|k| k.starts_with('$'));
    let has_plain = doc.keys().any(|k| !k.starts_with('$'));

    if has_operators && has_plain {
        return Err(Error::InvalidUpdate(
            "an update cannot mix operators with replacement fields".into(),
        ));
    }
    if !has_operators {
        if !array_filters.is_empty() {
            return Err(Error::InvalidUpdate(
                "arrayFilters apply to update operators, not to a replacement document".into(),
            ));
        }
        return Ok(Update::Replace(doc.clone()));
    }

    let filters = parse_array_filters(array_filters)?;
    let mut used = BTreeSet::new();

    let mut operations = Vec::new();
    for (key, value) in doc {
        let op = &key[1..];
        let Bson::Document(targets) = value else {
            return Err(Error::InvalidUpdate(format!("${op} requires a document")));
        };
        for (target_path, arg) in targets {
            if target_path == ID_FIELD {
                return Err(Error::InvalidUpdate("_id is immutable and cannot be updated".into()));
            }
            let kind = parse_op(op, arg)?;
            let identifiers = positional_identifiers(target_path)?;
            if let OpKind::Rename(target) = &kind
                && (has_positional(target_path) || has_positional(target))
            {
                // A rename names two fixed places; "wherever the filter
                // matches" is not a place a value can be moved from or to.
                return Err(Error::InvalidUpdate(
                    "$rename cannot use positional segments in its source or destination".into(),
                ));
            }
            let mut op_filters = Vec::new();
            for identifier in identifiers {
                let Some(found) = filters.iter().find(|f| f.identifier == identifier) else {
                    return Err(Error::InvalidUpdate(format!(
                        "the update path {target_path:?} uses identifier {identifier:?}, but \
                         arrayFilters has no filter for it"
                    )));
                };
                used.insert(identifier);
                op_filters.push(found.clone());
            }
            operations.push(Operation {
                path: target_path.clone(),
                kind,
                array_filters: op_filters,
            });
        }
    }

    if let Some(unused) = filters.iter().find(|f| !used.contains(&f.identifier)) {
        return Err(Error::InvalidUpdate(format!(
            "arrayFilters names identifier {:?}, which no update path uses",
            unused.identifier
        )));
    }

    Ok(Update::Operators(operations))
}

/// Whether a path has any `$[...]` segment.
fn has_positional(path: &str) -> bool {
    path::segments(path).iter().any(|segment| segment.starts_with("$["))
}

/// Check a path's positional segments and return the identifiers they name,
/// in order of first use and without repeats. `$[]` names none.
fn positional_identifiers(path: &str) -> Result<Vec<String>> {
    let mut identifiers: Vec<String> = Vec::new();
    for (position, segment) in path::segments(path).iter().enumerate() {
        if *segment == "$" {
            return Err(Error::InvalidUpdate(format!(
                "the `$` positional operator is not supported (in path {path:?}); use \
                 `$[<identifier>]` with arrayFilters, or `$[]` for every element"
            )));
        }
        let Some(inner) = segment.strip_prefix("$[") else {
            continue;
        };
        let Some(identifier) = inner.strip_suffix(']') else {
            return Err(Error::InvalidUpdate(format!(
                "malformed positional segment {segment:?} in path {path:?}"
            )));
        };
        if position == 0 {
            return Err(Error::InvalidUpdate(format!(
                "path {path:?} begins with a positional segment, but a document is not an array"
            )));
        }
        if identifier.is_empty() {
            continue;
        }
        check_identifier(identifier)?;
        if !identifiers.iter().any(|known| known == identifier) {
            identifiers.push(identifier.to_string());
        }
    }
    Ok(identifiers)
}

/// MongoDB's rule for an identifier: a lowercase letter, then letters and
/// digits. Kept so that a filter written for MongoDB is accepted unchanged and
/// one written here is accepted there.
fn check_identifier(identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let well_formed = chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_alphanumeric());
    if well_formed {
        Ok(())
    } else {
        Err(Error::InvalidUpdate(format!(
            "array filter identifier {identifier:?} must start with a lowercase letter and \
             contain only letters and digits"
        )))
    }
}

/// Parse the request's `arrayFilters` documents.
fn parse_array_filters(docs: &[Document]) -> Result<Vec<ArrayFilter>> {
    let mut out: Vec<ArrayFilter> = Vec::new();
    for doc in docs {
        let mut identifier = None;
        let stripped = strip_identifier(doc, &mut identifier)?;
        let Some(identifier) = identifier else {
            return Err(Error::InvalidUpdate(format!(
                "an arrayFilters entry must name an identifier in every field, as in \
                 {{\"line.qty\": ...}}; got {doc}"
            )));
        };
        check_identifier(&identifier)?;
        if out.iter().any(|f| f.identifier == identifier) {
            return Err(Error::InvalidUpdate(format!(
                "arrayFilters has more than one filter for identifier {identifier:?}"
            )));
        }
        let filter = filter::parse(&stripped)?;
        out.push(ArrayFilter { identifier, filter });
    }
    Ok(out)
}

/// Rewrite a filter document with the identifier prefix removed from every
/// field, recording the identifier and refusing a second one. `{"line.qty":
/// 1}` becomes `{"qty": 1}`; a bare `{"line": 1}` becomes `{"": 1}`, the empty
/// path that [`filter::matches_element`] reads as the element itself.
/// `$and`/`$or`/`$nor` are rewritten through; no other `$`-operator has a
/// meaning at the top of an array filter.
fn strip_identifier(doc: &Document, identifier: &mut Option<String>) -> Result<Document> {
    let mut out = Document::new();
    for (key, value) in doc {
        if let Some(op) = key.strip_prefix('$') {
            if !matches!(op, "and" | "or" | "nor") {
                return Err(Error::InvalidUpdate(format!(
                    "an arrayFilters entry takes field conditions and $and/$or/$nor, not ${op}"
                )));
            }
            let Bson::Array(branches) = value else {
                return Err(Error::InvalidUpdate(format!("${op} requires an array")));
            };
            let mut rewritten = Vec::with_capacity(branches.len());
            for branch in branches {
                let Bson::Document(branch) = branch else {
                    return Err(Error::InvalidUpdate(format!("${op} requires documents")));
                };
                rewritten.push(Bson::Document(strip_identifier(branch, identifier)?));
            }
            out.insert(key.clone(), Bson::Array(rewritten));
            continue;
        }
        let (prefix, rest) = key.split_once('.').unwrap_or((key.as_str(), ""));
        match identifier {
            Some(seen) if seen != prefix => {
                return Err(Error::InvalidUpdate(format!(
                    "an arrayFilters entry names one identifier, but this one names both \
                     {seen:?} and {prefix:?}"
                )));
            }
            Some(_) => {}
            None => *identifier = Some(prefix.to_string()),
        }
        out.insert(rest, value.clone());
    }
    Ok(out)
}

fn parse_op(op: &str, arg: &Bson) -> Result<OpKind> {
    let numeric = |arg: &Bson| -> Result<Bson> {
        match arg {
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => Ok(arg.clone()),
            _ => Err(Error::InvalidUpdate(format!("${op} requires a number"))),
        }
    };

    Ok(match op {
        "set" => OpKind::Set(arg.clone()),
        "unset" => OpKind::Unset,
        "inc" => OpKind::Inc(numeric(arg)?),
        "mul" => OpKind::Mul(numeric(arg)?),
        "min" => OpKind::Min(arg.clone()),
        "max" => OpKind::Max(arg.clone()),
        "push" => match each_values(arg) {
            Some(values) => OpKind::PushEach(values),
            None => OpKind::Push(arg.clone()),
        },
        "addToSet" => match each_values(arg) {
            Some(values) => OpKind::AddToSet(values),
            None => OpKind::AddToSet(vec![arg.clone()]),
        },
        "pull" => OpKind::Pull(arg.clone()),
        "pop" => match arg {
            Bson::Int32(1) | Bson::Int64(1) => OpKind::Pop(1),
            Bson::Int32(-1) | Bson::Int64(-1) => OpKind::Pop(-1),
            _ => return Err(Error::InvalidUpdate("$pop requires 1 or -1".into())),
        },
        "rename" => match arg {
            Bson::String(target) => OpKind::Rename(target.clone()),
            _ => return Err(Error::InvalidUpdate("$rename requires a field name".into())),
        },
        "currentDate" => OpKind::CurrentDate,
        other => return Err(Error::UnsupportedOperator(format!("${other}"))),
    })
}

/// Extract the values of a `{$each: [...]}` modifier, if present.
fn each_values(arg: &Bson) -> Option<Vec<Bson>> {
    let Bson::Document(doc) = arg else {
        return None;
    };
    match doc.get("$each") {
        Some(Bson::Array(items)) => Some(items.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

/// Apply an update to a document in place.
///
/// `now_ms` is passed in rather than read from the clock so that `$currentDate`
/// stays deterministic in tests and consistent with the write's own timestamp.
pub fn apply(update: &Update, doc: &mut Document, now_ms: i64) -> Result<()> {
    let operations = match update {
        Update::Replace(replacement) => {
            // `_id` belongs to the document's identity, not its contents.
            let id = doc.get(ID_FIELD).cloned();
            *doc = replacement.clone();
            match id {
                Some(id) => {
                    doc.insert(ID_FIELD, id);
                }
                None => {
                    doc.remove(ID_FIELD);
                }
            }
            return Ok(());
        }
        Update::Operators(ops) => ops,
    };

    for op in operations {
        if !has_positional(&op.path) {
            apply_one(op, doc, now_ms)?;
            continue;
        }
        // Resolve every positional segment against the document as it is
        // before this operation touches it, then apply to each concrete
        // index path in turn. Positions are stable across the applications:
        // `$unset` leaves a null hole rather than shifting later elements.
        for concrete in expand_positional(&op.path, &op.array_filters, doc)? {
            let at = Operation { path: concrete, kind: op.kind.clone(), array_filters: Vec::new() };
            apply_one(&at, doc, now_ms)?;
        }
    }
    Ok(())
}

/// The concrete index paths a positional path names in `doc`, in element
/// order: `items.$[line].qty` over a document whose second and fourth line
/// items satisfy `line` becomes `["items.1.qty", "items.3.qty"]`. An empty
/// result means no element was selected, and the operation leaves the
/// document as it found it.
fn expand_positional(path: &str, filters: &[ArrayFilter], doc: &Document) -> Result<Vec<String>> {
    let segments = path::segments(path);
    let (head, rest) = segments.split_first().expect("non-empty path");
    let mut prefix = vec![(*head).to_string()];
    let mut out = Vec::new();
    expand_into(doc.get(*head), rest, filters, path, &mut prefix, &mut out)?;
    Ok(out)
}

fn expand_into(
    value: Option<&Bson>,
    segments: &[&str],
    filters: &[ArrayFilter],
    path: &str,
    prefix: &mut Vec<String>,
    out: &mut Vec<String>,
) -> Result<()> {
    let Some((head, rest)) = segments.split_first() else {
        out.push(prefix.join("."));
        return Ok(());
    };

    if let Some(identifier) = head.strip_prefix("$[").and_then(|s| s.strip_suffix(']')) {
        // A positional segment has nothing to select from unless there is an
        // array here. Creating one, or treating a scalar as a one-element
        // array, would write somewhere the caller did not point at.
        let Some(Bson::Array(items)) = value else {
            let at = prefix.join(".");
            return Err(Error::InvalidUpdate(match value {
                None => format!("the path {at:?} must exist to apply {head:?} in {path:?}"),
                Some(_) => {
                    format!("the path {at:?} must be an array to apply {head:?} in {path:?}")
                }
            }));
        };
        let selector = match identifier {
            "" => None,
            named => Some(
                &filters
                    .iter()
                    .find(|f| f.identifier == named)
                    .ok_or_else(|| {
                        Error::InvalidUpdate(format!("no array filter for identifier {named:?}"))
                    })?
                    .filter,
            ),
        };
        for (index, item) in items.iter().enumerate() {
            if selector.is_none_or(|f| filter::matches_element(f, item)) {
                prefix.push(index.to_string());
                expand_into(Some(item), rest, filters, path, prefix, out)?;
                prefix.pop();
            }
        }
        return Ok(());
    }

    // A plain segment descends when it can; when it cannot, the remaining
    // segments are carried along as they are, and the operator decides what
    // an absent path means (`$set` creates it, `$pull` ignores it).
    let next = match value {
        Some(Bson::Document(child)) => child.get(*head),
        Some(Bson::Array(items)) => head.parse::<usize>().ok().and_then(|i| items.get(i)),
        _ => None,
    };
    prefix.push((*head).to_string());
    expand_into(next, rest, filters, path, prefix, out)?;
    prefix.pop();
    Ok(())
}

fn apply_one(op: &Operation, doc: &mut Document, now_ms: i64) -> Result<()> {
    let current = path::resolve(doc, &op.path).first().cloned().cloned();

    let invalid = |msg: String| Error::InvalidUpdate(msg);
    let set = |doc: &mut Document, value: Bson| -> Result<()> {
        path::set(doc, &op.path, value).map_err(invalid)
    };

    match &op.kind {
        OpKind::Set(value) => set(doc, value.clone())?,

        OpKind::Unset => {
            path::unset(doc, &op.path);
        }

        OpKind::Inc(delta) => {
            // An absent field starts at zero, so `$inc` on a new counter works.
            let base = current.unwrap_or(Bson::Int32(0));
            set(doc, arithmetic(&base, delta, Arith::Add, &op.path)?)?;
        }

        OpKind::Mul(factor) => {
            let base = current.unwrap_or(Bson::Int32(0));
            set(doc, arithmetic(&base, factor, Arith::Mul, &op.path)?)?;
        }

        OpKind::Min(candidate) => {
            let replace = match &current {
                Some(existing) => canonical_cmp(candidate, existing) == Ordering::Less,
                // A missing field takes the value outright.
                None => true,
            };
            if replace {
                set(doc, candidate.clone())?;
            }
        }

        OpKind::Max(candidate) => {
            let replace = match &current {
                Some(existing) => canonical_cmp(candidate, existing) == Ordering::Greater,
                None => true,
            };
            if replace {
                set(doc, candidate.clone())?;
            }
        }

        OpKind::Push(value) => {
            let mut items = as_array(&current, &op.path)?;
            items.push(value.clone());
            set(doc, Bson::Array(items))?;
        }

        OpKind::PushEach(values) => {
            let mut items = as_array(&current, &op.path)?;
            items.extend(values.iter().cloned());
            set(doc, Bson::Array(items))?;
        }

        OpKind::AddToSet(values) => {
            let mut items = as_array(&current, &op.path)?;
            for value in values {
                let present =
                    items.iter().any(|item| canonical_cmp(item, value) == Ordering::Equal);
                if !present {
                    items.push(value.clone());
                }
            }
            set(doc, Bson::Array(items))?;
        }

        OpKind::Pull(value) => {
            // Pulling from a missing field is a no-op, not an error.
            let Some(Bson::Array(items)) = current else {
                return Ok(());
            };
            let kept: Vec<Bson> = items
                .into_iter()
                .filter(|item| canonical_cmp(item, value) != Ordering::Equal)
                .collect();
            set(doc, Bson::Array(kept))?;
        }

        OpKind::Pop(direction) => {
            let Some(Bson::Array(mut items)) = current else {
                return Ok(());
            };
            if !items.is_empty() {
                if *direction == 1 {
                    items.pop();
                } else {
                    items.remove(0);
                }
            }
            set(doc, Bson::Array(items))?;
        }

        OpKind::Rename(target) => {
            if target == ID_FIELD {
                return Err(invalid("_id is immutable and cannot be renamed onto".into()));
            }
            // Renaming a missing field is a no-op, matching Mongo.
            if let Some(value) = current {
                path::unset(doc, &op.path);
                path::set(doc, target, value).map_err(invalid)?;
            }
        }

        OpKind::CurrentDate => {
            set(doc, Bson::DateTime(bson::DateTime::from_millis(now_ms)))?;
        }
    }
    Ok(())
}

enum Arith {
    Add,
    Mul,
}

/// Apply arithmetic, preserving integer types where the result still fits.
fn arithmetic(base: &Bson, operand: &Bson, op: Arith, path: &str) -> Result<Bson> {
    let both_int = matches!(base, Bson::Int32(_) | Bson::Int64(_))
        && matches!(operand, Bson::Int32(_) | Bson::Int64(_));

    if both_int {
        let a = as_i64(base).expect("checked int");
        let b = as_i64(operand).expect("checked int");
        let result = match op {
            Arith::Add => a.checked_add(b),
            Arith::Mul => a.checked_mul(b),
        };
        // On overflow, widening to a double loses precision silently; refusing
        // is the honest outcome.
        return match result {
            Some(v) => Ok(Bson::Int64(v)),
            None => Err(Error::InvalidUpdate(format!(
                "arithmetic on field {path:?} overflowed a 64-bit integer"
            ))),
        };
    }

    let a = as_f64(base).ok_or_else(|| {
        Error::InvalidUpdate(format!("cannot apply arithmetic to non-numeric field {path:?}"))
    })?;
    let b = as_f64(operand).ok_or_else(|| {
        Error::InvalidUpdate("cannot apply arithmetic with a non-numeric operand".to_string())
    })?;
    Ok(Bson::Double(match op {
        Arith::Add => a + b,
        Arith::Mul => a * b,
    }))
}

fn as_i64(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(v) => Some(i64::from(*v)),
        Bson::Int64(v) => Some(*v),
        _ => None,
    }
}

fn as_f64(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(v) => Some(f64::from(*v)),
        Bson::Int64(v) => Some(*v as f64),
        Bson::Double(v) => Some(*v),
        _ => None,
    }
}

/// Interpret the current value as an array for the array operators.
fn as_array(current: &Option<Bson>, path: &str) -> Result<Vec<Bson>> {
    match current {
        Some(Bson::Array(items)) => Ok(items.clone()),
        // A missing field becomes a new array, which is what makes `$push` to a
        // fresh field work.
        None => Ok(Vec::new()),
        Some(_) => Err(Error::InvalidUpdate(format!(
            "cannot apply an array operator to non-array field {path:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    const NOW: i64 = 1_700_000_000_000;

    fn applied(update: Document, mut doc: Document) -> Document {
        let parsed = parse(&update).unwrap_or_else(|e| panic!("parse failed: {e}"));
        apply(&parsed, &mut doc, NOW).unwrap_or_else(|e| panic!("apply failed: {e}"));
        doc
    }

    fn apply_err(update: Document, mut doc: Document) -> String {
        let parsed = match parse(&update) {
            Ok(p) => p,
            Err(e) => return e.to_string(),
        };
        apply(&parsed, &mut doc, NOW).unwrap_err().to_string()
    }

    #[test]
    fn set_writes_scalars_and_nested_paths() {
        assert_eq!(applied(doc! { "$set": { "a": 2 } }, doc! { "a": 1 }), doc! { "a": 2 });
        assert_eq!(applied(doc! { "$set": { "a.b": 1 } }, doc! {}), doc! { "a": { "b": 1 } });
    }

    #[test]
    fn unset_removes_a_field() {
        assert_eq!(
            applied(doc! { "$unset": { "a": "" } }, doc! { "a": 1, "b": 2 }),
            doc! { "b": 2 }
        );
    }

    #[test]
    fn inc_starts_a_missing_field_from_zero() {
        assert_eq!(applied(doc! { "$inc": { "n": 5 } }, doc! {}), doc! { "n": 5i64 });
        assert_eq!(applied(doc! { "$inc": { "n": 5 } }, doc! { "n": 1 }), doc! { "n": 6i64 });
        assert_eq!(applied(doc! { "$inc": { "n": -2 } }, doc! { "n": 1 }), doc! { "n": -1i64 });
    }

    #[test]
    fn arithmetic_keeps_integers_integral() {
        // Silently widening to a double would lose precision on large ids.
        let out = applied(doc! { "$inc": { "n": 1 } }, doc! { "n": 9_007_199_254_740_992i64 });
        assert_eq!(out.get_i64("n").unwrap(), 9_007_199_254_740_993);
    }

    #[test]
    fn arithmetic_promotes_to_double_when_either_side_is_one() {
        assert_eq!(applied(doc! { "$inc": { "n": 0.5 } }, doc! { "n": 1 }), doc! { "n": 1.5 });
    }

    #[test]
    fn integer_overflow_is_refused_rather_than_silently_widened() {
        let err = apply_err(doc! { "$inc": { "n": 1 } }, doc! { "n": i64::MAX });
        assert!(err.contains("overflow"), "unhelpful error: {err}");
    }

    #[test]
    fn arithmetic_on_a_non_numeric_field_is_an_error() {
        let err = apply_err(doc! { "$inc": { "s": 1 } }, doc! { "s": "text" });
        assert!(err.contains("non-numeric"), "unhelpful error: {err}");
    }

    #[test]
    fn mul_multiplies() {
        assert_eq!(applied(doc! { "$mul": { "n": 3 } }, doc! { "n": 4 }), doc! { "n": 12i64 });
        // A missing field is treated as zero, as in Mongo.
        assert_eq!(applied(doc! { "$mul": { "n": 3 } }, doc! {}), doc! { "n": 0i64 });
    }

    #[test]
    fn min_and_max_only_move_in_one_direction() {
        assert_eq!(applied(doc! { "$min": { "n": 1 } }, doc! { "n": 5 }), doc! { "n": 1 });
        assert_eq!(applied(doc! { "$min": { "n": 9 } }, doc! { "n": 5 }), doc! { "n": 5 });
        assert_eq!(applied(doc! { "$max": { "n": 9 } }, doc! { "n": 5 }), doc! { "n": 9 });
        assert_eq!(applied(doc! { "$max": { "n": 1 } }, doc! { "n": 5 }), doc! { "n": 5 });
        // A missing field takes the value outright.
        assert_eq!(applied(doc! { "$min": { "n": 3 } }, doc! {}), doc! { "n": 3 });
    }

    #[test]
    fn push_appends_and_creates_missing_arrays() {
        assert_eq!(applied(doc! { "$push": { "a": 2 } }, doc! { "a": [1] }), doc! { "a": [1, 2] });
        assert_eq!(applied(doc! { "$push": { "a": 1 } }, doc! {}), doc! { "a": [1] });
    }

    #[test]
    fn push_each_appends_several() {
        assert_eq!(
            applied(doc! { "$push": { "a": { "$each": [2, 3] } } }, doc! { "a": [1] }),
            doc! { "a": [1, 2, 3] }
        );
    }

    #[test]
    fn push_onto_a_non_array_is_an_error() {
        let err = apply_err(doc! { "$push": { "a": 1 } }, doc! { "a": "scalar" });
        assert!(err.contains("non-array"), "unhelpful error: {err}");
    }

    #[test]
    fn add_to_set_skips_duplicates() {
        assert_eq!(
            applied(doc! { "$addToSet": { "a": 2 } }, doc! { "a": [1, 2] }),
            doc! { "a": [1, 2] }
        );
        assert_eq!(
            applied(doc! { "$addToSet": { "a": 3 } }, doc! { "a": [1, 2] }),
            doc! { "a": [1, 2, 3] }
        );
        // Equality is numeric, so 2 and 2.0 are the same member.
        assert_eq!(
            applied(doc! { "$addToSet": { "a": 2.0 } }, doc! { "a": [1, 2] }),
            doc! { "a": [1, 2] }
        );
    }

    #[test]
    fn pull_removes_every_matching_element() {
        assert_eq!(
            applied(doc! { "$pull": { "a": 2 } }, doc! { "a": [1, 2, 3, 2] }),
            doc! { "a": [1, 3] }
        );
        // Pulling from a missing field is a no-op.
        assert_eq!(applied(doc! { "$pull": { "a": 2 } }, doc! { "b": 1 }), doc! { "b": 1 });
    }

    #[test]
    fn pop_removes_from_either_end() {
        assert_eq!(
            applied(doc! { "$pop": { "a": 1 } }, doc! { "a": [1, 2, 3] }),
            doc! { "a": [1, 2] }
        );
        assert_eq!(
            applied(doc! { "$pop": { "a": -1 } }, doc! { "a": [1, 2, 3] }),
            doc! { "a": [2, 3] }
        );
        // Popping an empty array is a no-op rather than an error.
        assert_eq!(applied(doc! { "$pop": { "a": 1 } }, doc! { "a": [] }), doc! { "a": [] });
    }

    #[test]
    fn pop_rejects_directions_other_than_one() {
        assert!(parse(&doc! { "$pop": { "a": 2 } }).is_err());
    }

    #[test]
    fn rename_moves_a_field() {
        assert_eq!(applied(doc! { "$rename": { "a": "b" } }, doc! { "a": 1 }), doc! { "b": 1 });
        // Renaming a missing field is a no-op.
        assert_eq!(applied(doc! { "$rename": { "a": "b" } }, doc! { "c": 1 }), doc! { "c": 1 });
    }

    #[test]
    fn current_date_uses_the_supplied_timestamp() {
        let out = applied(doc! { "$currentDate": { "at": true } }, doc! {});
        assert_eq!(out.get_datetime("at").unwrap().timestamp_millis(), NOW);
    }

    #[test]
    fn several_operators_apply_together() {
        let out = applied(
            doc! { "$set": { "a": 1 }, "$inc": { "n": 1 }, "$push": { "tags": "x" } },
            doc! { "n": 5, "tags": [] },
        );
        assert_eq!(out.get_i32("a").unwrap(), 1);
        assert_eq!(out.get_i64("n").unwrap(), 6);
        assert_eq!(out.get_array("tags").unwrap().len(), 1);
    }

    // -----------------------------------------------------------------------
    // Replacement form and _id protection
    // -----------------------------------------------------------------------

    #[test]
    fn a_document_without_operators_is_a_replacement() {
        assert_eq!(
            applied(doc! { "x": 1 }, doc! { "_id": 7, "a": 1, "b": 2 }),
            doc! { "x": 1, "_id": 7 }
        );
    }

    #[test]
    fn a_replacement_preserves_the_existing_id() {
        // Even when the replacement names a different one: _id is identity,
        // not content, and a replace must not relocate the document.
        let out = applied(doc! { "_id": 999, "x": 1 }, doc! { "_id": 7, "a": 1 });
        assert_eq!(out.get_i32("_id").unwrap(), 7);
    }

    #[test]
    fn mixing_operators_and_replacement_fields_is_rejected() {
        let err = parse(&doc! { "$set": { "a": 1 }, "b": 2 }).unwrap_err().to_string();
        assert!(err.contains("cannot mix"), "unhelpful error: {err}");
    }

    #[test]
    fn operators_may_not_touch_id() {
        assert!(parse(&doc! { "$set": { "_id": 1 } }).is_err());
        assert!(parse(&doc! { "$unset": { "_id": "" } }).is_err());
        assert!(parse(&doc! { "$rename": { "a": "_id" } }).is_ok_and(|u| {
            let mut d = doc! { "_id": 1, "a": 2 };
            apply(&u, &mut d, NOW).is_err()
        }));
    }

    #[test]
    fn unknown_operators_are_rejected() {
        assert!(parse(&doc! { "$frobnicate": { "a": 1 } }).is_err());
    }

    #[test]
    fn operators_require_a_document_argument() {
        assert!(parse(&doc! { "$set": 1 }).is_err());
    }

    // -----------------------------------------------------------------------
    // Positional paths: `$[]` and `$[<identifier>]` with arrayFilters
    // -----------------------------------------------------------------------

    fn applied_with(update: Document, filters: Vec<Document>, mut doc: Document) -> Document {
        let parsed =
            parse_with_filters(&update, &filters).unwrap_or_else(|e| panic!("parse failed: {e}"));
        apply(&parsed, &mut doc, NOW).unwrap_or_else(|e| panic!("apply failed: {e}"));
        doc
    }

    fn apply_err_with(update: Document, filters: Vec<Document>, mut doc: Document) -> String {
        let parsed = match parse_with_filters(&update, &filters) {
            Ok(p) => p,
            Err(e) => return e.to_string(),
        };
        apply(&parsed, &mut doc, NOW).unwrap_err().to_string()
    }

    fn order() -> Document {
        doc! {
            "_id": 1,
            "items": [
                { "sku": "a", "qty": 1, "shipped": false },
                { "sku": "b", "qty": 5, "shipped": false },
                { "sku": "c", "qty": 9, "shipped": false },
            ]
        }
    }

    /// The `shipped` flag of each line item, in order.
    fn shipped(order: &Document) -> Vec<bool> {
        order
            .get_array("items")
            .unwrap()
            .iter()
            .map(|line| line.as_document().unwrap().get_bool("shipped").unwrap())
            .collect()
    }

    #[test]
    fn a_filtered_identifier_reaches_one_element() {
        // The line-item case the feature exists for: mark one line shipped
        // without rewriting the document.
        let out = applied_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.sku": "b" }],
            order(),
        );
        assert_eq!(shipped(&out), vec![false, true, false]);
    }

    #[test]
    fn every_matching_element_is_updated() {
        let out = applied_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.qty": { "$gt": 2 } }],
            order(),
        );
        assert_eq!(shipped(&out), vec![false, true, true]);
    }

    #[test]
    fn a_filter_may_combine_several_conditions_on_the_element() {
        // Two fields of the same element, as `$elemMatch` would test them.
        let out = applied_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.qty": { "$gt": 2 }, "line.sku": "c" }],
            order(),
        );
        assert_eq!(shipped(&out), vec![false, false, true]);

        // And through `$or`, which is rewritten branch by branch.
        let out = applied_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "$or": [ { "line.sku": "a" }, { "line.qty": 9 } ] }],
            order(),
        );
        assert_eq!(shipped(&out), vec![true, false, true]);
    }

    #[test]
    fn a_bare_identifier_addresses_scalar_elements() {
        assert_eq!(
            applied_with(
                doc! { "$inc": { "grades.$[g]": 10 } },
                vec![doc! { "g": { "$gte": 80 } }],
                doc! { "grades": [70, 80, 90] },
            ),
            doc! { "grades": [70, 90i64, 100i64] }
        );
        // Equality against the element itself.
        assert_eq!(
            applied_with(
                doc! { "$set": { "tags.$[t]": "B" } },
                vec![doc! { "t": "b" }],
                doc! { "tags": ["a", "b", "b"] },
            ),
            doc! { "tags": ["a", "B", "B"] }
        );
    }

    #[test]
    fn all_positional_updates_every_element() {
        assert_eq!(
            applied_with(
                doc! { "$inc": { "grades.$[]": 5 } },
                vec![],
                doc! { "grades": [1, 2, 3] },
            ),
            doc! { "grades": [6i64, 7i64, 8i64] }
        );
        let out = applied_with(doc! { "$set": { "items.$[].shipped": true } }, vec![], order());
        assert_eq!(shipped(&out), vec![true, true, true]);
    }

    #[test]
    fn no_matching_element_leaves_the_document_alone() {
        // Not an error: "ship the lines with sku z" on an order without one
        // is a legitimate no-op. The write still counts as `modified`, which
        // is the register's documented meaning of that field.
        let out = applied_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.sku": "z" }],
            order(),
        );
        assert_eq!(out, order());
    }

    #[test]
    fn nested_identifiers_address_inner_arrays() {
        let doc = doc! {
            "orders": [
                { "id": 1, "items": [ { "sku": "a", "qty": 1 }, { "sku": "b", "qty": 2 } ] },
                { "id": 2, "items": [ { "sku": "a", "qty": 3 }, { "sku": "b", "qty": 4 } ] },
            ]
        };
        let out = applied_with(
            doc! { "$set": { "orders.$[o].items.$[i].qty": 0 } },
            vec![doc! { "o.id": 2 }, doc! { "i.sku": "b" }],
            doc,
        );
        assert_eq!(
            out,
            doc! {
                "orders": [
                    { "id": 1, "items": [ { "sku": "a", "qty": 1 }, { "sku": "b", "qty": 2 } ] },
                    { "id": 2, "items": [ { "sku": "a", "qty": 3 }, { "sku": "b", "qty": 0 } ] },
                ]
            }
        );
    }

    #[test]
    fn inc_mul_min_max_and_current_date_work_through_a_positional_path() {
        let out = applied_with(
            doc! {
                "$inc": { "items.$[line].qty": 1 },
                "$mul": { "items.$[line].price": 2 },
                "$max": { "items.$[line].seen": 7 },
                "$currentDate": { "items.$[line].at": true },
            },
            vec![doc! { "line.sku": "b" }],
            doc! { "items": [ { "sku": "a", "qty": 1 }, { "sku": "b", "qty": 5, "price": 3 } ] },
        );
        let line = out.get_array("items").unwrap()[1].as_document().unwrap();
        assert_eq!(line.get_i64("qty").unwrap(), 6);
        assert_eq!(line.get_i64("price").unwrap(), 6);
        assert_eq!(line.get_i32("seen").unwrap(), 7);
        assert_eq!(line.get_datetime("at").unwrap().timestamp_millis(), NOW);
        // The unselected line is exactly as it was.
        assert_eq!(
            out.get_array("items").unwrap()[0],
            Bson::Document(doc! { "sku": "a", "qty": 1 })
        );
    }

    #[test]
    fn unset_of_a_matched_element_leaves_a_null_hole() {
        // Compacting would renumber the elements after it, so an element that
        // is unset becomes null, as in MongoDB.
        assert_eq!(
            applied_with(
                doc! { "$unset": { "grades.$[g]": "" } },
                vec![doc! { "g": { "$lt": 60 } }],
                doc! { "grades": [90, 50, 70] },
            ),
            doc! { "grades": [90, Bson::Null, 70] }
        );
        // Unsetting a field *of* a matched element removes the field.
        let out = applied_with(
            doc! { "$unset": { "items.$[line].shipped": "" } },
            vec![doc! { "line.sku": "a" }],
            order(),
        );
        assert_eq!(
            out.get_array("items").unwrap()[0],
            Bson::Document(doc! { "sku": "a", "qty": 1 })
        );
    }

    #[test]
    fn array_operators_apply_inside_a_matched_subdocument() {
        let doc = doc! {
            "items": [
                { "sku": "a", "tags": ["x", "y"] },
                { "sku": "b", "tags": ["x", "z"] },
            ]
        };
        let out = applied_with(
            doc! {
                "$pull": { "items.$[line].tags": "x" },
                "$push": { "items.$[line].tags": "w" },
                "$addToSet": { "items.$[line].tags": "z" },
            },
            vec![doc! { "line.sku": "b" }],
            doc,
        );
        assert_eq!(
            out,
            doc! {
                "items": [
                    { "sku": "a", "tags": ["x", "y"] },
                    { "sku": "b", "tags": ["z", "w"] },
                ]
            }
        );
        // `$pop` and `$push` with `$each` go through the same path.
        let out = applied_with(
            doc! { "$pop": { "items.$[].tags": -1 } },
            vec![],
            doc! { "items": [ { "tags": [1, 2] }, { "tags": [3, 4] } ] },
        );
        assert_eq!(out, doc! { "items": [ { "tags": [2] }, { "tags": [4] } ] });
    }

    #[test]
    fn a_positional_segment_needs_an_array_to_select_from() {
        let err = apply_err_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.sku": "a" }],
            doc! { "items": "not an array" },
        );
        assert!(err.contains("must be an array"), "unhelpful error: {err}");

        let err = apply_err_with(
            doc! { "$set": { "items.$[].shipped": true } },
            vec![],
            doc! { "other": 1 },
        );
        assert!(err.contains("must exist"), "unhelpful error: {err}");

        // The same rule one level down: an element without the inner array.
        let err = apply_err_with(
            doc! { "$set": { "orders.$[].items.$[].qty": 0 } },
            vec![],
            doc! { "orders": [ { "items": [ { "qty": 1 } ] }, { "id": 2 } ] },
        );
        assert!(err.contains("\"orders.1.items\" must exist"), "unhelpful error: {err}");
    }

    #[test]
    fn an_identifier_without_a_filter_is_rejected() {
        let err =
            apply_err_with(doc! { "$set": { "items.$[line].shipped": true } }, vec![], order());
        assert!(err.contains("no filter for it"), "unhelpful error: {err}");
    }

    #[test]
    fn a_filter_without_an_identifier_is_rejected() {
        let err = apply_err_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "line.sku": "a" }, doc! { "other.sku": "b" }],
            order(),
        );
        assert!(err.contains("no update path uses"), "unhelpful error: {err}");
        // Including on a path without positional segments at all.
        let err = apply_err_with(doc! { "$set": { "n": 1 } }, vec![doc! { "x.y": 1 }], order());
        assert!(err.contains("no update path uses"), "unhelpful error: {err}");
    }

    #[test]
    fn a_filter_document_names_exactly_one_identifier() {
        let err = apply_err_with(
            doc! { "$set": { "items.$[a].shipped": true, "items.$[b].shipped": true } },
            vec![doc! { "a.sku": "a", "b.sku": "b" }],
            order(),
        );
        assert!(err.contains("names both"), "unhelpful error: {err}");

        let err = apply_err_with(
            doc! { "$set": { "items.$[a].shipped": true } },
            vec![doc! { "a.sku": "a" }, doc! { "a.sku": "b" }],
            order(),
        );
        assert!(err.contains("more than one filter"), "unhelpful error: {err}");

        let err = apply_err_with(
            doc! { "$set": { "items.$[a].shipped": true } },
            vec![doc! { "$and": [] }],
            order(),
        );
        assert!(err.contains("must name an identifier"), "unhelpful error: {err}");
    }

    #[test]
    fn identifiers_follow_the_naming_rule() {
        for bad in ["Line", "1st", "a-b"] {
            let path = format!("items.$[{bad}].shipped");
            let err = apply_err_with(doc! { "$set": { path: true } }, vec![], order());
            assert!(err.contains("lowercase letter"), "{bad:?}: unhelpful error: {err}");
        }
        let err = apply_err_with(doc! { "$set": { "items.$[line": true } }, vec![], order());
        assert!(err.contains("malformed"), "unhelpful error: {err}");
    }

    #[test]
    fn a_path_cannot_begin_with_a_positional_segment() {
        let err = apply_err_with(doc! { "$set": { "$[].x": 1 } }, vec![], order());
        assert!(err.contains("not an array"), "unhelpful error: {err}");
    }

    #[test]
    fn the_dollar_positional_operator_is_refused_with_a_pointer() {
        let err = apply_err_with(doc! { "$set": { "items.$.shipped": true } }, vec![], order());
        assert!(err.contains("$[<identifier>]"), "unhelpful error: {err}");
    }

    #[test]
    fn rename_refuses_positional_paths() {
        let err = apply_err_with(doc! { "$rename": { "items.$[].sku": "code" } }, vec![], order());
        assert!(err.contains("$rename"), "unhelpful error: {err}");
        let err = apply_err_with(doc! { "$rename": { "a": "items.$[].a" } }, vec![], order());
        assert!(err.contains("$rename"), "unhelpful error: {err}");
    }

    #[test]
    fn array_filters_do_not_apply_to_a_replacement() {
        let err = apply_err_with(doc! { "x": 1 }, vec![doc! { "a.b": 1 }], order());
        assert!(err.contains("replacement"), "unhelpful error: {err}");
    }

    #[test]
    fn only_logical_operators_are_accepted_at_the_top_of_a_filter() {
        let err = apply_err_with(
            doc! { "$set": { "items.$[line].shipped": true } },
            vec![doc! { "$where": "1" }],
            order(),
        );
        assert!(err.contains("$where"), "unhelpful error: {err}");
    }

    #[test]
    fn a_dotted_condition_on_a_scalar_element_sees_an_absent_field() {
        // `{"e.x": {$exists: false}}` selects scalars, which have no `x`;
        // `{"e.x": {$gt: 0}}` selects nothing among them.
        assert_eq!(
            applied_with(
                doc! { "$set": { "mixed.$[e]": 0 } },
                vec![doc! { "e.x": { "$exists": false } }],
                doc! { "mixed": [ 7, { "x": 1 } ] },
            ),
            doc! { "mixed": [ 0, { "x": 1 } ] }
        );
        assert_eq!(
            applied_with(
                doc! { "$set": { "mixed.$[e]": 0 } },
                vec![doc! { "e.x": { "$gt": 0 } }],
                doc! { "mixed": [ 7, { "x": 1 } ] },
            ),
            doc! { "mixed": [ 7, 0 ] }
        );
    }
}
