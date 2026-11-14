//! The effects of an update, as the merge layer takes them (Q7 Step 2, query side).
//!
//! [`apply_with_effects`] runs the update with the engine of [`crate::update`] (so the
//! post-image, every refusal and every message are the ones the `document` policy has), then
//! classifies what each operation did into the per-leaf effects of
//! [`kimmy_core::effects::Effects`]: the operator names the effect, never a diff (`$inc 5` is
//! `Inc(path, 5)`, not `Set(path, old + 5)`). The operator-by-effect table is in the module docs
//! of `kimmy_core::effects`.
//!
//! # How an operation becomes effects
//!
//! * `$inc` on a path that crosses no array: `Inc(path, operand)`, the operand as written (an
//!   `Int32`, `Int64`, `Double` or `Decimal128`), unless the update leaves the value as it was
//!   (`$inc: {n: 0}` over a number of the operand's type): then no effect.
//! * `$rename`: [`Effects::rename`] (`Unset(from)` and `Set(to, value)`), none when the source is
//!   absent.
//! * `$unset`: `Unset(path)` when the field was there.
//! * every other operator (`$set`, `$mul`, `$bit`, `$min`, `$max`, `$currentDate`, `$push`,
//!   `$addToSet`, `$pull`, `$pullAll`, `$pop`): `Set(path, result)` when the result differs from
//!   the stored value by representation ([`Value`] equality), none otherwise.
//! * a path that CROSSES an array (an index, `$`, `$[]`, `$[id]`, nested arrays): ONE
//!   `Set(outermost_array, whole_array)` per array, whatever the number of operations that touch
//!   it. Which paths cross an array is decided from the stored document, not from the spelling
//!   (`x.0` over an absent `x` is a field named `0`).
//!
//! # Order
//!
//! The effects are listed in the order of the post-image the engine builds: an effect whose field
//! is new takes the place its field has there, so `field` and `document` collections show the
//! same field order. Unsets come first, in operator order.

use bson::{Bson, Document};
use kimmy_core::effects::Effects;
use kimmy_core::merge_state::bridge::bson_to_value;
use kimmy_core::merge_state::{Path, Value};
use kimmy_core::{Error, Result};
use std::collections::HashSet;

use crate::filter::{self, Filter};
use crate::path;
use crate::update::{
    OpKind, Operation, Target, Update, apply_to_at, concrete_paths, has_positional,
};

/// The plan of one update on an existing document.
#[derive(Debug)]
pub enum Plan {
    /// An operator or pipeline update: the effects (empty when nothing changes) and the document
    /// the update produces (what a `document` collection stores, and what validators, indexes
    /// and size limits look at).
    Effects { effects: Effects, post_image: Document },
    /// A replacement document: not an effect list; it stays the replace path of the storage layer.
    Replace { post_image: Document },
}

/// One classified write, before the order is fixed.
enum Item {
    Set(Path, Value),
    Unset(Path),
    Inc(Path, Value),
    Rename(Path, Path, Value),
}

/// Plan the effects of `update` on `doc`. `filter` is what matched the document: it resolves a
/// `$` positional segment (as [`crate::update::apply_matched`]); `None` for an update with none.
/// `now_ms` is the one date of `$currentDate`.
pub fn apply_with_effects(
    update: &Update,
    filter: Option<&Filter>,
    doc: &Document,
    now_ms: i64,
) -> Result<Plan> {
    let at = match filter {
        Some(f) if update.uses_matched_position() => filter::position(f, doc)?,
        _ => None,
    };
    let mut next = doc.clone();
    apply_to_at(update, &mut next, now_ms, Target::Existing, at.as_ref())?;
    let operations = match update {
        Update::Replace(_) => return Ok(Plan::Replace { post_image: next }),
        Update::Pipeline(_) => {
            return Err(Error::Internal("pipeline-update effects are not built yet".into()));
        }
        Update::Operators(ops) => ops,
    };
    let concrete = concrete_paths(operations, doc, at.as_ref(), |op| {
        !matches!(op.kind, OpKind::SetOnInsert(_))
    })?;
    let mut items = Vec::new();
    let mut hoisted = HashSet::new();
    for (op, paths) in operations.iter().zip(&concrete) {
        for p in paths {
            classify(op, p, doc, &next, &mut hoisted, &mut items)?;
        }
    }
    Ok(Plan::Effects { effects: build(items, &next), post_image: next })
}

fn bad(e: impl std::fmt::Display) -> Error {
    Error::InvalidUpdate(e.to_string())
}

fn to_path(segs: &[&str]) -> Result<Path> {
    Path::new(segs.iter().copied()).map_err(bad)
}

fn value_of(b: &Bson) -> Result<Value> {
    bson_to_value(b).map_err(bad)
}

/// The value at `segs` through documents only.
fn get_in<'a>(doc: &'a Document, segs: &[&str]) -> Option<&'a Bson> {
    let (last, parents) = segs.split_last()?;
    let mut cur = doc;
    for seg in parents {
        match cur.get(*seg) {
            Some(Bson::Document(d)) => cur = d,
            _ => return None,
        }
    }
    cur.get(*last)
}

/// How many leading segments name the outermost array on the way down `segs` in `doc`.
fn outermost_array(doc: &Document, segs: &[&str]) -> Option<usize> {
    let mut cur = doc;
    for (i, seg) in segs.iter().enumerate() {
        match cur.get(*seg) {
            Some(Bson::Array(_)) => return Some(i + 1),
            Some(Bson::Document(d)) => cur = d,
            _ => return None,
        }
    }
    None
}

fn same(before: Option<&Bson>, after: Option<&Bson>) -> Result<bool> {
    Ok(match (before, after) {
        (None, None) => true,
        (Some(b), Some(a)) => value_of(b)? == value_of(a)?,
        _ => false,
    })
}

/// The write that takes `segs` from its stored value to its value in `next`, as a leaf, or as the
/// whole outermost array when the path crosses one (once per array).
fn write_of(
    segs: &[&str],
    doc: &Document,
    next: &Document,
    hoisted: &mut HashSet<Path>,
    items: &mut Vec<Item>,
) -> Result<()> {
    let target = match outermost_array(doc, segs) {
        Some(n) if n < segs.len() => &segs[..n],
        _ => segs,
    };
    let path = to_path(target)?;
    if target.len() < segs.len() && !hoisted.insert(path.clone()) {
        return Ok(());
    }
    let (before, after) = (get_in(doc, target), get_in(next, target));
    if same(before, after)? {
        return Ok(());
    }
    items.push(match after {
        Some(a) => Item::Set(path, value_of(a)?),
        None => Item::Unset(path),
    });
    Ok(())
}

fn classify(
    op: &Operation,
    concrete: &str,
    doc: &Document,
    next: &Document,
    hoisted: &mut HashSet<Path>,
    items: &mut Vec<Item>,
) -> Result<()> {
    debug_assert!(!has_positional(concrete));
    let segs = path::segments(concrete);
    let crosses = |segs: &[&str]| outermost_array(doc, segs).is_some_and(|n| n < segs.len());
    match &op.kind {
        OpKind::Inc(by) if !crosses(&segs) => {
            if !same(get_in(doc, &segs), get_in(next, &segs))? {
                items.push(Item::Inc(to_path(&segs)?, value_of(by)?));
            }
        }
        OpKind::Rename(target) => {
            let to_segs = path::segments(target);
            if crosses(&segs) || crosses(&to_segs) {
                // inside an array element: the arrays are rewritten whole, and a missing
                // source changes nothing, so each side is its own write (none when unchanged)
                write_of(&segs, doc, next, hoisted, items)?;
                write_of(&to_segs, doc, next, hoisted, items)?;
            } else if let Some(source) = get_in(doc, &segs) {
                items.push(Item::Rename(to_path(&segs)?, to_path(&to_segs)?, value_of(source)?));
            }
        }
        _ => write_of(&segs, doc, next, hoisted, items)?,
    }
    Ok(())
}

/// The place of a path in the post-image: the index of its field at each level. A field that is
/// not there sorts last.
fn rank(next: &Document, p: &Path) -> Vec<usize> {
    let mut out = Vec::new();
    let mut cur = next;
    for seg in p.segments() {
        match cur.keys().position(|k| k == seg) {
            Some(i) => out.push(i),
            None => {
                out.push(usize::MAX);
                break;
            }
        }
        match cur.get(seg) {
            Some(Bson::Document(d)) => cur = d,
            _ => break,
        }
    }
    out
}

/// The effects in post-image order; unsets first, in operator order.
fn build(mut items: Vec<Item>, next: &Document) -> Effects {
    let key = |i: &Item| match i {
        Item::Unset(_) => Vec::new(),
        Item::Set(p, _) | Item::Inc(p, _) | Item::Rename(_, p, _) => rank(next, p),
    };
    items.sort_by_cached_key(key);
    let mut fx = Effects::new();
    for item in items {
        match item {
            Item::Set(p, v) => fx.set(p, v),
            Item::Unset(p) => fx.unset(p),
            Item::Inc(p, by) => fx.inc(p, by),
            Item::Rename(from, to, v) => fx.rename(from, to, v),
        };
    }
    fx
}
