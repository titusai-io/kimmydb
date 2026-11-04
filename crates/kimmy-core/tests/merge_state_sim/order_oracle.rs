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
//!   standing candidate in some life), in a document with one alive life, goes AFTER every field of its level
//!   that has a born and BEFORE every field that has none. The fields without a born are the MAPS WITHOUT A LEAF
//!   ANYWHERE BELOW THEM and without history of their own: such a map is visible through empty maps alone (the
//!   unset that took its own cell left a floor and no candidate, and a child marker written by someone the
//!   unsetter had not heard from survived), and nothing places it. They trail, in order of their paths. Every
//!   other field has a born: a leaf has one (a candidate, or the survivor clamp), and a map with a leaf below
//!   it that survived is placed by that survivor (after the fields of the write that unset it). This is
//!   `model6.py`, `State.visible`, `okey()` lines 535-536 (`(0, born, path)` if there is a born, else
//!   `(1, (), path)`) and `INTERFACE.md` line 134 ("sorted by `(born, path)`, a child without a born last"),
//!   with the born of a map decided from the documents alone, as the survivor placement decides it in the
//!   cases the seeds reach (1080, 1357, 2626, 2654 of the bounded runs, pinned below).
//!
//! The third part is left out for a field with history: the place an earlier write gave it stands (the
//! model's rule: the earliest write since the last void). A map that was emptied no longer vanishes (the implicit
//! marker is built), so that cause of history is gone. What is left is a field that is created again after the
//! cell it had was unset by a write the place did not outlive: nine seeds of the 3,000-seed bounded sweeps (see
//! `history_seed!` in `main.rs`) disagree with the Writer without the exclusion, and the model agrees with the
//! Writer on every one. The exclusion goes when the oracle encodes that rule from the log instead of reading the
//! writer's order state.

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
    /// The fields an insert makes the document of (in order; sub-documents are maps).
    Insert(Vec<(String, Value)>),
    /// The fields a replace makes the document of (in order).
    Replace(Vec<(String, Value)>),
    Set(Path, Value),
    Unset(Path),
    /// `$inc`: an absent field appears at the end; a field that exists stays where it is.
    Inc(Path),
    /// An upsert (`$setOnInsert` fields, `$set` fields, then the incremented field): a new document.
    Upsert(Vec<String>),
}

impl Tree {
    /// Is there a leaf anywhere below (or is this a leaf-less tree of empty maps)?
    fn has_leaf(&self) -> bool {
        self.iter().any(|(_, kids)| kids.as_ref().is_none_or(Tree::has_leaf))
    }

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
        OrdOp::Insert(f) | OrdOp::Replace(f) => Some(tree_of_fields(f)),
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
        Intent::Insert(f) => OrdOp::Insert(f.clone()),
        Intent::Replace(f) => OrdOp::Replace(f.clone()),
        Intent::Set(p, v) => OrdOp::Set(p.clone(), v.clone()),
        Intent::Unset(p) => OrdOp::Unset(p.clone()),
        Intent::Inc(p) => OrdOp::Inc(p.clone()),
    }
}

/// Does the write put the fields the writer sees in another ORDER, with the same fields? Then it is a write whatever the
/// values (a replace, or a `$set` of a sub-document), and a writer that refuses it has dropped a reorder.
pub fn reorders(before: Option<&[(String, OrderedValue)]>, op: &OrdOp) -> bool {
    let Some(b) = before.map(tree_of_ordered) else { return false };
    let (cur, want) = match op {
        // (an insert is refused over a live document, model6.py `insert`, `if s.alive(): return None`: not a reorder)
        OrdOp::Replace(f) => (b, tree_of_fields(f)),
        OrdOp::Set(p, v @ Value::Document(_)) => {
            let (Some(cur), Some(want)) = (b.level(p.segments()), node_of(v)) else { return false };
            (cur.clone(), want)
        }
        _ => return false,
    };
    cur != want && cur.paths() == want.paths()
}

/// What a check decided.
#[derive(Clone, Copy, Default)]
pub struct Checked {
    /// Some part was compared and holds.
    pub any: bool,
    /// The part "a field the write creates goes after the fields with a place" was decided.
    pub last: bool,
    /// ... on a level that held a map without a place, which trails.
    pub trailing: bool,
}

/// The writer's document after its write against the rules above. `Err` with the first disagreement. `history` is the set of paths
/// with a standing candidate in the order state before the write; `one_life` says the document had one
/// alive life before and after.
pub fn check(
    before: Option<&[(String, OrderedValue)]>,
    after: Option<&[(String, OrderedValue)]>,
    op: &OrdOp,
    history: &BTreeSet<Vec<String>>,
    one_life: bool,
) -> Result<Checked, String> {
    let before = before.map(tree_of_ordered);
    let got = after.map(tree_of_ordered);
    let fail = |what: &str| Err(format!("{what}: {op:?}: before {before:?}, after {got:?}"));
    match op {
        OrdOp::Insert(_) | OrdOp::Replace(_) | OrdOp::Upsert(_) => {
            let Some(want) = apply(None, op) else { return Ok(Checked::default()) };
            // which fields a document shows is the value rules' business: compare the order where the fields agree
            if got.as_ref().is_none_or(|g| g.paths() != want.paths()) {
                return Ok(Checked::default());
            }
            if got.as_ref() == Some(&want) {
                Ok(Checked { any: true, ..Checked::default() })
            } else {
                fail("not the order the write names")
            }
        }
        OrdOp::Set(p, _) | OrdOp::Inc(p) | OrdOp::Unset(p) => {
            let (Some(b), Some(g)) = (before.as_ref(), got.as_ref()) else {
                return Ok(Checked::default());
            };
            let named: BTreeSet<Vec<String>> =
                p.ancestors().chain([p.clone()]).map(|q| q.segments().to_vec()).collect();
            if !stable(b, g, &mut Vec::new(), &named, p.segments()) {
                return fail("a write moved a field it does not name");
            }
            let (mut last, mut trailing) = (false, false);
            if one_life && !matches!(op, OrdOp::Unset(_)) {
                let had: BTreeSet<Vec<String>> = b.paths().into_iter().map(|q| q.0).collect();
                let at = p.segments().to_vec();
                if !had.contains(&at)
                    && !history.contains(&at)
                    && g.paths().iter().any(|q| q.0 == at)
                {
                    let parent = &at[..at.len() - 1];
                    let key = |n: &String| {
                        let mut q = parent.to_vec();
                        q.push(n.clone());
                        q
                    };
                    // the maps of the level, before the write, that no earlier write gave a place
                    let unplaced: Vec<String> = b
                        .level(parent)
                        .map(|l| {
                            l.iter()
                                .filter(|(n, kids)| {
                                    kids.as_ref().is_some_and(|k| !k.has_leaf())
                                        && !history.contains(&key(n))
                                })
                                .map(|(n, _)| n.clone())
                                .collect()
                        })
                        .unwrap_or_default();
                    last = true;
                    trailing = !unplaced.is_empty();
                    let names: Vec<String> = g
                        .level(parent)
                        .map(|l| l.iter().map(|(n, _)| n.clone()).collect())
                        .unwrap_or_default();
                    let i = names
                        .iter()
                        .position(|n| Some(n) == at.last())
                        .expect("the field is there");
                    let mut want_after: Vec<String> =
                        unplaced.iter().filter(|n| names.contains(n)).cloned().collect();
                    want_after.sort();
                    if names[i + 1..] != want_after[..]
                        || names[..i].iter().any(|n| unplaced.contains(n))
                    {
                        return fail(
                            "a new field is not after the fields with a place and before those without",
                        );
                    }
                }
            }
            Ok(Checked { any: true, last, trailing })
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
