//! The order a writer's own read must show right after its own write: read-your-writes order.
//!
//! A write is a diff against what its writer SAW, so the writer's document after it is the document
//! before it with the write applied serially (the model's `Ref`: a `$set` keeps the place of a field that
//! exists and appends one that does not, an unset removes, an `$inc` of an absent field appends, an insert
//! or a replace puts the fields it names in the order it names them). That holds whatever else the writer
//! has not received, because the writer's state holds none of it. The check reads two ordered documents
//! of the writer and the write: it is independent of the feed, the join and the read it grades. Only the
//! shape is compared; values are the oracle's business (`oracle.rs`).
//!
//! Three parts, from the strongest to the one the order state's own rules weaken:
//! * an insert, a replace or an upsert gives EXACTLY the document the write names, in its order;
//! * a write moves only what it names: at every level, the fields the writer saw before and after, that the
//!   write does not name (the path, the maps above it, and everything under a document it sets), keep their
//!   relative order;
//! * a field the write creates, that no earlier write of the order state gave a place (no HISTORY: a
//!   standing candidate in some life), in a document with one alive life and no floor anywhere in its order
//!   state (no unset and no replace has been made), is the LAST of its level. Floors bring in what the
//!   serial reference cannot say: content that survived an unset is placed by the survivor clamp, and a
//!   document-valued write puts the fields it never saw after its own, wherever the later writes go.
//!
//! The third part is left out for a field with history: the place an earlier write gave it stands (the
//! model's rule: the earliest write since the last void), as with a map that was emptied and vanished
//! because the implicit marker is not built (item O4), and for content that survived an unset, which the
//! survivor clamp placed and the write then gives a standing candidate.

use std::collections::BTreeSet;

use kimmy_core::merge_state::{OrderedValue, Path, Value};

use crate::oracle::Intent;

/// An ordered document: fields in order, each a leaf (`None`) or a map of fields.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Tree(pub Vec<(String, Option<Tree>)>);

impl std::ops::Deref for Tree {
    type Target = Vec<(String, Option<Tree>)>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Tree {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// What a write does to the order, as the serial reference reads it.
#[derive(Clone, Debug)]
pub enum OrdOp {
    /// The fields of the document an insert or a replace makes (in order; sub-documents are maps).
    Make(Vec<(String, Value)>),
    Set(Path, Value),
    Unset(Path),
    /// `$inc`: an absent field appears at the end; a field that exists stays where it is.
    Inc(Path),
    /// An upsert (`$setOnInsert` fields, `$set` fields, then the incremented field): a new document.
    Upsert(Vec<String>),
}

impl Tree {
    /// The children at a path (`[]` is the top level).
    fn level(&self, at: &[String]) -> Option<&Tree> {
        let Some((head, rest)) = at.split_first() else { return Some(self) };
        match self.iter().find(|(n, _)| n == head) {
            Some((_, Some(kids))) => kids.level(rest),
            _ => None,
        }
    }

    /// The paths of the fields, sorted, with whether each is a map.
    fn paths(&self) -> Vec<(Vec<String>, bool)> {
        fn walk(t: &Tree, at: &mut Vec<String>, out: &mut Vec<(Vec<String>, bool)>) {
            for (n, kids) in &t.0 {
                at.push(n.clone());
                out.push((at.clone(), kids.is_some()));
                if let Some(k) = kids {
                    walk(k, at, out);
                }
                at.pop();
            }
        }
        let mut out = Vec::new();
        walk(self, &mut Vec::new(), &mut out);
        out.sort();
        out
    }
}

pub fn tree_of_ordered(d: &[(String, OrderedValue)]) -> Tree {
    Tree(
        d.iter()
            .map(|(n, v)| {
                (
                    n.clone(),
                    match v {
                        OrderedValue::Doc(kids) => Some(tree_of_ordered(kids)),
                        OrderedValue::Leaf(_) => None,
                    },
                )
            })
            .collect(),
    )
}

fn node_of(v: &Value) -> Option<Tree> {
    match v {
        Value::Document(f) => Some(tree_of_fields(f)),
        _ => None,
    }
}

/// The fields a document value names, in order. (A field whose value is `Unset` is not a field.)
fn tree_of_fields(f: &[(String, Value)]) -> Tree {
    Tree(
        f.iter()
            .filter(|(_, v)| *v != Value::Unset)
            .map(|(n, v)| (n.clone(), node_of(v)))
            .collect(),
    )
}

/// Put `new` at `segs`, creating the maps above it at the end; `false` when a leaf is in the way.
fn put(t: &mut Tree, segs: &[String], new: Option<Tree>, keep_leaf: bool) -> bool {
    let (head, rest) = segs.split_first().expect("a path has a segment");
    let at = t.iter().position(|(n, _)| n == head);
    if rest.is_empty() {
        match at {
            Some(i) if keep_leaf && t[i].1.is_none() => {}
            Some(i) => t[i].1 = new,
            None => t.push((head.clone(), new)),
        }
        return true;
    }
    let i = at.unwrap_or_else(|| {
        t.push((head.clone(), Some(Tree::default())));
        t.len() - 1
    });
    match &mut t[i].1 {
        Some(kids) => put(kids, rest, new, keep_leaf),
        None => false,
    }
}

fn remove(t: &mut Tree, segs: &[String]) {
    let (head, rest) = segs.split_first().expect("a path has a segment");
    let Some(i) = t.iter().position(|(n, _)| n == head) else { return };
    if rest.is_empty() {
        t.remove(i);
    } else if let Some(kids) = &mut t[i].1 {
        remove(kids, rest);
    }
}

/// The document after the write, or `None` when the serial reference has no outcome for it (a leaf in the
/// way, an `$inc` of a map).
pub fn apply(before: Option<&Tree>, op: &OrdOp) -> Option<Tree> {
    let segs = |p: &Path| p.segments().to_vec();
    match op {
        OrdOp::Make(f) => Some(tree_of_fields(f)),
        OrdOp::Upsert(names) => {
            let mut t = Tree::default();
            for n in names {
                if !t.iter().any(|(m, _)| m == n) {
                    t.push((n.clone(), None));
                }
            }
            Some(t)
        }
        OrdOp::Set(p, v) => {
            let mut t = before?.clone();
            put(&mut t, &segs(p), node_of(v), false).then_some(t)
        }
        OrdOp::Unset(p) => {
            let mut t = before?.clone();
            remove(&mut t, &segs(p));
            Some(t)
        }
        OrdOp::Inc(p) => {
            let mut t = before?.clone();
            // an existing map cannot be incremented
            let mut level = &t;
            for (i, s) in p.segments().iter().enumerate() {
                match level.iter().find(|(n, _)| n == s) {
                    Some((_, Some(kids))) if i + 1 < p.segments().len() => level = kids,
                    Some((_, Some(_))) => return None,
                    _ => break,
                }
            }
            put(&mut t, &segs(p), None, true).then_some(t)
        }
    }
}

/// What the client asked, as the reference reads it.
pub fn op_of(intent: &Intent) -> OrdOp {
    match intent {
        Intent::Insert(f) | Intent::Replace(f) => OrdOp::Make(f.clone()),
        Intent::Set(p, v) => OrdOp::Set(p.clone(), v.clone()),
        Intent::Unset(p) => OrdOp::Unset(p.clone()),
        Intent::Inc(p) => OrdOp::Inc(p.clone()),
    }
}

/// The writer's document after its write against the rules above. `Ok(true)` when a part was compared and
/// holds, `Ok(false)` when none applies, `Err` with the first disagreement. `history` is the set of paths
/// with a standing candidate in the order state before the write; `one_life` says the document had one
/// alive life before and after.
pub fn check(
    before: Option<&[(String, OrderedValue)]>,
    after: Option<&[(String, OrderedValue)]>,
    op: &OrdOp,
    history: &BTreeSet<Vec<String>>,
    one_life: bool,
) -> Result<bool, String> {
    let before = before.map(tree_of_ordered);
    let got = after.map(tree_of_ordered);
    let fail = |what: &str| Err(format!("{what}: {op:?}: before {before:?}, after {got:?}"));
    match op {
        OrdOp::Make(_) | OrdOp::Upsert(_) => {
            let Some(want) = apply(None, op) else { return Ok(false) };
            // which fields a document shows is the value rules' business: compare the order where the fields agree
            if got.as_ref().is_none_or(|g| g.paths() != want.paths()) {
                return Ok(false);
            }
            if got.as_ref() == Some(&want) {
                Ok(true)
            } else {
                fail("not the order the write names")
            }
        }
        OrdOp::Set(p, _) | OrdOp::Inc(p) | OrdOp::Unset(p) => {
            let (Some(b), Some(g)) = (before.as_ref(), got.as_ref()) else { return Ok(false) };
            let named: BTreeSet<Vec<String>> =
                p.ancestors().chain([p.clone()]).map(|q| q.segments().to_vec()).collect();
            if !stable(b, g, &mut Vec::new(), &named, p.segments()) {
                return fail("a write moved a field it does not name");
            }
            if one_life && !matches!(op, OrdOp::Unset(_)) {
                let had: BTreeSet<Vec<String>> = b.paths().into_iter().map(|q| q.0).collect();
                let at = p.segments().to_vec();
                if !had.contains(&at)
                    && !history.contains(&at)
                    && g.paths().iter().any(|q| q.0 == at)
                {
                    let parent = &at[..at.len() - 1];
                    let level = g.level(parent);
                    if level.and_then(|l| l.last().map(|(n, _)| n.clone())) != at.last().cloned() {
                        return fail("a new field is not the last of its level");
                    }
                }
            }
            Ok(true)
        }
    }
}

/// Do the fields present at a level before and after, and not named, keep their relative order, at every level?
fn stable(
    before: &Tree,
    after: &Tree,
    at: &mut Vec<String>,
    named: &BTreeSet<Vec<String>>,
    written: &[String],
) -> bool {
    let key = |n: &String, at: &Vec<String>| {
        let mut p = at.clone();
        p.push(n.clone());
        p
    };
    let kept = |t: &Tree, other: &Tree| -> Vec<String> {
        t.iter()
            .filter(|(n, _)| other.iter().any(|(m, _)| m == n) && !named.contains(&key(n, at)))
            .map(|(n, _)| n.clone())
            .collect()
    };
    if kept(before, after) != kept(after, before) {
        return false;
    }
    // below: every map present on both sides, but not the written path itself (a document set there is
    // free to order what is under it)
    for (n, kids) in before.iter() {
        let path = key(n, at);
        if path == written {
            continue;
        }
        if let (Some(b), Some((_, Some(a)))) = (kids, after.iter().find(|(m, _)| m == n)) {
            at.push(n.clone());
            let ok = stable(b, a, at, named, written);
            at.pop();
            if !ok {
                return false;
            }
        }
    }
    true
}
