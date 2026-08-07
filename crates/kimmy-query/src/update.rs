//! Update operators.
//!
//! An update document is either a set of `$`-prefixed operators or a whole
//! replacement document — never a mix, because the two mean very different
//! things and guessing would silently discard fields.

use bson::{Bson, Document};
use kimmy_core::cmp::canonical_cmp;
use kimmy_core::{Error, Result};
use std::cmp::Ordering;

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
    let has_operators = doc.keys().any(|k| k.starts_with('$'));
    let has_plain = doc.keys().any(|k| !k.starts_with('$'));

    if has_operators && has_plain {
        return Err(Error::InvalidUpdate(
            "an update cannot mix operators with replacement fields".into(),
        ));
    }
    if !has_operators {
        return Ok(Update::Replace(doc.clone()));
    }

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
            operations.push(Operation { path: target_path.clone(), kind: parse_op(op, arg)? });
        }
    }

    Ok(Update::Operators(operations))
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
        apply_one(op, doc, now_ms)?;
    }
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
}
