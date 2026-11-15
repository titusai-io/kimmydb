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
//! same field order. A plain `$unset` comes first, in operator order; a `$rename` is its `Unset`
//! and its `Set` together ([`Effects::rename`]), at the place of the DESTINATION in the post-image.
//!
//! # `field` collections only
//!
//! [`apply_with_effects`] and [`plan_upsert`] are for collections under the `field` merge policy.
//! They run the engine first, so every refusal of the `document` path is theirs too, and they can
//! refuse where the `document` path succeeds: a BSON type the merge state cannot hold (the
//! bridge), and the writer's own refusals once the effects are applied. A `document` collection
//! keeps using [`crate::update::apply`] and its siblings.
//!
//! # Pending
//!
//! * Costs to measure with L34-B B2-1 (no change now): the update runs on a clone of the
//!   document, the effects convert before and after values to merge-state values twice, and the
//!   order of the effects ([`build`]) ranks each effect against the post-image, O(effects x
//!   fields).

use bson::{Bson, Document};
use kimmy_core::effects::{Effects, UpsertEffects};
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

/// Plan the effects of `update` on `doc`, for a `field` collection ONLY (see the module docs):
/// it can refuse where [`crate::update::apply`] succeeds. `filter` is what matched the document: it resolves a
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
            let items = pipeline_items(doc, &next)?;
            return Ok(Plan::Effects { effects: build(items, &next), post_image: next });
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

/// The effects of a pipeline update (ADR-265): one `Set` per top-level field the stages changed
/// or added, one `Unset` per field they removed, each a [`Path`] of ONE segment (a field named
/// `a.b` or `$x` is one field: the name is never split). The writer's diff writes only the
/// changed leaves and unsets only what it saw, so this is not a replace: the stored order of the
/// fields that stay is the writer's, and a result that only reorders fields is no effect. `_id`
/// is never an effect (it is the document's identity: a result that restates it, even as another
/// type of the same id, leaves the stored one).
fn pipeline_items(doc: &Document, next: &Document) -> Result<Vec<Item>> {
    let mut items = Vec::new();
    for (name, after) in next {
        if name == "_id" || same(doc.get(name), Some(after))? {
            continue;
        }
        items.push(Item::Set(Path::flat(name).map_err(bad)?, value_of(after)?));
    }
    for name in doc.keys() {
        if name != "_id" && !next.contains_key(name) {
            items.push(Item::Unset(Path::flat(name).map_err(bad)?));
        }
    }
    Ok(items)
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
        // the engine refused a rename that goes through an array or a scalar (its source rule,
        // `q7s2f.out`): what is left is a plain move of a document field
        OpKind::Rename(target) => {
            if let Some(source) = get_in(doc, &segs) {
                let to_segs = path::segments(target);
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

// ---------------------------------------------------------------------------------------------
// Upserts
// ---------------------------------------------------------------------------------------------

/// The plan of an upsert that matched no document.
#[derive(Debug)]
pub enum UpsertPlan {
    /// An operator or pipeline update: the creation as [`UpsertEffects`] (the filter's equality
    /// fields, `$setOnInsert`, `$set` and the computed values of the register operators, `$inc`),
    /// already checked ([`UpsertEffects::check`] and [`UpsertEffects::check_creation`]), and the
    /// document the engine inserts (what a `document` collection stores).
    Effects { effects: UpsertEffects, post_image: Document },
    /// A replacement document: the creation is the replacement, not an effect list.
    Replace { post_image: Document },
}

fn dotted(p: &str) -> Result<Path> {
    to_path(&path::segments(p))
}

/// Plan the creation of an upsert whose `filter` matched nothing, for a `field` collection ONLY
/// (see the module docs): it can refuse where [`crate::update::apply_on_insert`] succeeds. The
/// engine builds the inserted
/// document as it always did (the filter's equalities, then the update); the effects are the
/// layers of the creation, each as written: `from_filter` (an `_id` path is left out, the upsert's
/// `_id` logic owns it), `set_on_insert`, `set` (including the creation value of `$mul`, `$min`,
/// `$max`, `$currentDate`, `$push`, `$addToSet`, `$bit` and the other register operators) and
/// `inc`. New fields of `set_on_insert` and `set` are listed in the order of the inserted
/// document. A positional `$` is refused by the engine on an insert, as before.
pub fn plan_upsert(update: &Update, filter: &Filter, now_ms: i64) -> Result<UpsertPlan> {
    let mut seed = Document::new();
    // the shared seed (`crate::seed`, the one `kimmy-api` uses): the filter's equalities at their
    // paths, the levels a dotted key makes in name order; `claimed` lists the paths as written
    let mut claimed = Vec::new();
    crate::seed::implied_equalities(filter, &mut seed, &mut claimed)
        .map_err(Error::InvalidUpdate)?;
    // `from_filter` is built from it, as written: each claimed path with the value found at it
    let mut equalities: Vec<(String, Bson)> = Vec::new();
    for p in claimed {
        if let Some(v) = path::resolve(&seed, &p).first() {
            equalities.push((p, (*v).clone()));
        }
    }
    // the seed's top-level fields are in name order, `_id` first, whichever order the filter
    // wrote them in (q7s2.out u1, u2); the fields the update adds follow them. The shared
    // function leaves this to its callers, as `kimmy-api` does.
    let mut fields: Vec<(String, Bson)> = std::mem::take(&mut seed).into_iter().collect();
    fields.sort_by(|a, b| (a.0 != "_id", &a.0).cmp(&(b.0 != "_id", &b.0)));
    seed.extend(fields);
    let mut post = seed.clone();
    apply_to_at(update, &mut post, now_ms, Target::Inserted, None)?;
    let mut fx = UpsertEffects::default();
    for (p, v) in &equalities {
        if p.split('.').next() != Some("_id") {
            fx.from_filter.push((dotted(p)?, value_of(v)?));
        }
    }
    match update {
        Update::Replace(_) => return Ok(UpsertPlan::Replace { post_image: post }),
        Update::Pipeline(_) => {
            for item in pipeline_items(&seed, &post)? {
                fx.set.push(match item {
                    Item::Set(p, v) => (p, v),
                    Item::Unset(p) => (p, Value::Unset),
                    _ => unreachable!("a pipeline diff holds sets and unsets"),
                });
            }
        }
        Update::Operators(operations) => {
            let concrete = concrete_paths(operations, &seed, None, |_| true)?;
            let mut hoisted = HashSet::new();
            for (op, paths) in operations.iter().zip(&concrete) {
                for p in paths {
                    creation_of(op, p, &seed, &post, &mut hoisted, &mut fx)?;
                }
            }
        }
    }
    let rank_of =
        |p: &Path, v: &Value| if *v == Value::Unset { Vec::new() } else { rank(&post, p) };
    fx.set.sort_by_cached_key(|(p, v)| rank_of(p, v));
    fx.set_on_insert.sort_by_cached_key(|(p, v)| rank_of(p, v));
    fx.check().map_err(bad)?;
    fx.check_creation().map_err(bad)?;
    Ok(UpsertPlan::Effects { effects: fx, post_image: post })
}

/// Whether an `$inc` at `at` is routed to `set_on_insert` (amendment 7 of the Q7 design): `at`
/// equals a path the filter wrote, whose value is not a Document, or lies strictly below one
/// whose value is a Document (the empty one included). Decided on `from_filter` AS WRITTEN, never
/// on the seed: filter `{"a.b": 1}` with `$inc a.c` is not routed, it stays a counter slot beside
/// the leaf (the seed would have made `a` a Document). An `$inc` above a filter path, or below a
/// scalar one, is the engine's refusal.
fn inc_on_filter_path(fx: &UpsertEffects, at: &Path) -> bool {
    fx.from_filter.iter().any(|(written, value)| match value {
        Value::Document(_) => at.is_below(written),
        _ => at == written,
    })
}

/// One operation's part of the creation.
fn creation_of(
    op: &Operation,
    concrete: &str,
    seed: &Document,
    post: &Document,
    hoisted: &mut HashSet<Path>,
    fx: &mut UpsertEffects,
) -> Result<()> {
    let segs = path::segments(concrete);
    let crosses = |segs: &[&str]| outermost_array(seed, segs).is_some_and(|n| n < segs.len());
    let generic = |segs: &[&str], hoisted: &mut HashSet<Path>, fx: &mut UpsertEffects| {
        let mut items = Vec::new();
        write_of(segs, seed, post, hoisted, &mut items)?;
        for item in items {
            fx.set.push(match item {
                Item::Set(p, v) => (p, v),
                Item::Unset(p) => (p, Value::Unset),
                _ => unreachable!("a write is a set or an unset"),
            });
        }
        Ok::<(), Error>(())
    };
    match &op.kind {
        OpKind::SetOnInsert(v) if !crosses(&segs) => {
            fx.set_on_insert.push((to_path(&segs)?, value_of(v)?));
        }
        OpKind::Set(v) if !crosses(&segs) => fx.set.push((to_path(&segs)?, value_of(v)?)),
        OpKind::Unset if !crosses(&segs) => fx.set.push((to_path(&segs)?, Value::Unset)),
        // Amendment 7: an `$inc` on a path the filter wrote is the engine's post-image value at
        // that path, as a creation-tagged `$setOnInsert`: no slot, nothing silent when an
        // explicit insert wins (it drops it and reports `insertLost`)
        OpKind::Inc(_) if !crosses(&segs) && inc_on_filter_path(fx, &to_path(&segs)?) => {
            if let Some(after) = get_in(post, &segs) {
                fx.set_on_insert.push((to_path(&segs)?, value_of(after)?));
            }
        }
        OpKind::Inc(by) if !crosses(&segs) => fx.inc.push((to_path(&segs)?, value_of(by)?)),
        OpKind::Rename(target) => {
            if let Some(source) = get_in(seed, &segs) {
                let to_segs = path::segments(target);
                fx.set.push((to_path(&segs)?, Value::Unset));
                fx.set.push((to_path(&to_segs)?, value_of(source)?));
            }
        }
        _ => generic(&segs, hoisted, fx)?,
    }
    Ok(())
}
