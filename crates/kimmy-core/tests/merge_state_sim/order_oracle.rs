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
//! * a field the write creates, that no earlier write of its causal past gave a standing place (see below), in a
//!   document with one alive life, goes AFTER every field of its level
//!   that has a born and BEFORE every field that has none. The fields without a born are the MAPS WITHOUT A LEAF
//!   ANYWHERE BELOW THEM and without a standing place of their own: such a map is visible through empty maps alone (the
//!   unset that took its own cell left a floor and no candidate, and a child marker written by someone the
//!   unsetter had not heard from survived), and nothing places it. They trail, in order of their paths. Every
//!   other field has a born: a leaf has one (a candidate, or the survivor clamp), and a map with a leaf below
//!   it that survived is placed by that survivor (after the fields of the write that unset it). This is
//!   `model6.py`, `State.visible`, `okey()` lines 535-536 (`(0, born, path)` if there is a born, else
//!   `(1, (), path)`) and `INTERFACE.md` line 134 ("sorted by `(born, path)`, a child without a born last"),
//!   with the born of a map decided from the documents alone, as the survivor placement decides it in the
//!   cases the seeds reach (1080, 1357, 2626, 2654 of the bounded runs, pinned below).
//!
//! The third part is decided by the PLACES the log gives (see [`places`]): the model's rule, restated from the design and
//! read from the writes of the writer's causal past alone (what each write named, what it unset and which levels it
//! marked; never the writer's or the model's order state). A field with no standing place goes after the fields that have
//! one; a field whose place stands (a write in the past named it at a stamp above every unset of it in the past) is
//! ordered by that place against the fields whose places are plain.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use kimmy_core::merge_state::{MStamp, OrderedValue, Path, Value};

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

/// One name a write gave a field: its path, and whether the write RESET it (a replace's fields, the fields of a
/// sub-document: they re-assign the place and void the older ones) or only wrote it (a `$set` of the path, its
/// ancestors, an `$inc`).
#[derive(Clone, Debug)]
pub struct NameEv {
    pub path: Vec<String>,
    pub reset: bool,
}

/// What one write did to the order, from its intent and the document its writer saw: the fields it named (in the order
/// of the write), the paths it unset (what its writer saw below an unset, or removed by a replace or a sub-document) and
/// the levels it marked (a replace marks the root, a sub-document its path).
#[derive(Clone, Debug, Default)]
pub struct WriteOrd {
    pub names: Vec<NameEv>,
    pub unsets: Vec<Vec<String>>,
    pub levels: Vec<Vec<String>>,
    /// The life the write went into (set when it is logged).
    pub life: Option<MStamp>,
}

thread_local! {
    /// ORACLE ONLY: `WriteOrd` of every logged write, by stamp. The sim clears it when a new log starts.
    static PAST: RefCell<BTreeMap<MStamp, WriteOrd>> = const { RefCell::new(BTreeMap::new()) };
}

/// Record the order facts of the write that got `st`. `new_log`: the log was empty, so this starts a run.
pub fn record(st: MStamp, mut w: WriteOrd, life: Option<MStamp>, new_log: bool) {
    w.life = life;
    PAST.with(|t| {
        let mut t = t.borrow_mut();
        if new_log {
            t.clear();
        }
        t.insert(st, w);
    });
}

fn sub_paths(t: &Tree, at: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
    for (n, kids) in t.iter() {
        at.push(n.clone());
        out.push(at.clone());
        if let Some(k) = kids {
            sub_paths(k, at, out);
        }
        at.pop();
    }
}

/// The node at `segs` in a tree: `Some(None)` a leaf, `Some(Some(kids))` a map.
fn node_at<'a>(t: &'a Tree, segs: &[String]) -> Option<&'a Option<Tree>> {
    let (head, rest) = segs.split_first()?;
    let (_, kids) = t.iter().find(|(n, _)| n == head)?;
    if rest.is_empty() {
        return Some(kids);
    }
    kids.as_ref().and_then(|k| node_at(k, rest))
}

/// Names of the fields of a document value written under `at` (each RESET, a sub-document marks its level), in order.
fn doc_names(at: &[String], f: &[(String, Value)], w: &mut WriteOrd) {
    for (n, v) in f.iter().filter(|(_, v)| *v != Value::Unset) {
        let mut q = at.to_vec();
        q.push(n.clone());
        w.names.push(NameEv { path: q.clone(), reset: true });
        if let Value::Document(inner) = v {
            w.levels.push(q.clone());
            doc_names(&q, inner, w);
        }
    }
}

/// The paths a document value written over the map `old` (at `at`) unsets: the old fields it lacks, with everything
/// below them, and below a field that was a map and is not one now.
fn doc_unsets(old: &Tree, at: &[String], f: &[(String, Value)], out: &mut Vec<Vec<String>>) {
    for (n, kids) in old.iter() {
        let mut q = at.to_vec();
        q.push(n.clone());
        let new = f.iter().find(|(m, v)| m == n && *v != Value::Unset).map(|(_, v)| v);
        match (new, kids) {
            (None, k) => {
                out.push(q.clone());
                if let Some(k) = k {
                    sub_paths(k, &mut q.clone(), out);
                }
            }
            (Some(Value::Document(inner)), Some(k)) => doc_unsets(k, &q, inner, out),
            (Some(_), Some(k)) => sub_paths(k, &mut q.clone(), out),
            _ => {}
        }
    }
}

fn write_names(segs: &[String], w: &mut WriteOrd) {
    for n in 1..=segs.len() {
        w.names.push(NameEv { path: segs[..n].to_vec(), reset: false });
    }
}

/// What a write did to the order: the model's order items of the write (`_update`, `_diff`), restated from the intent
/// and the document the writer saw.
pub fn write_ord(before: Option<&[(String, OrderedValue)]>, op: &OrdOp) -> WriteOrd {
    let tree = before.map(tree_of_ordered).unwrap_or_default();
    let mut w = WriteOrd::default();
    match op {
        OrdOp::Insert(f) => {
            fn flat(at: &[String], f: &[(String, Value)], w: &mut WriteOrd) {
                for (n, v) in f.iter().filter(|(_, v)| *v != Value::Unset) {
                    let mut q = at.to_vec();
                    q.push(n.clone());
                    w.names.push(NameEv { path: q.clone(), reset: false });
                    if let Value::Document(inner) = v {
                        flat(&q, inner, w);
                    }
                }
            }
            flat(&[], f, &mut w);
        }
        OrdOp::Upsert(names) => {
            for n in names {
                w.names.push(NameEv { path: vec![n.clone()], reset: false });
            }
        }
        OrdOp::Replace(f) => {
            w.levels.push(Vec::new());
            doc_names(&[], f, &mut w);
            doc_unsets(&tree, &[], f, &mut w.unsets);
        }
        OrdOp::Set(p, v) => {
            let segs = p.segments().to_vec();
            write_names(&segs, &mut w);
            let old_kids = match node_at(&tree, &segs) {
                Some(Some(k)) => Some(k),
                _ => None,
            };
            if let Value::Document(f) = v {
                w.levels.push(segs.clone());
                doc_names(&segs, f, &mut w);
                if let Some(k) = old_kids {
                    doc_unsets(k, &segs, f, &mut w.unsets);
                }
            } else if let Some(k) = old_kids {
                sub_paths(k, &mut segs.clone(), &mut w.unsets);
            }
        }
        OrdOp::Inc(p) => write_names(p.segments(), &mut w),
        OrdOp::Unset(p) => {
            let segs = p.segments().to_vec();
            if let Some(n) = node_at(&tree, &segs) {
                w.unsets.push(segs.clone());
                if let Some(k) = n {
                    sub_paths(k, &mut segs.clone(), &mut w.unsets);
                }
            }
        }
    }
    w
}

/// The place of a field: `(stamp, position)` of its earliest standing candidate; a field the latest level mark above it
/// never named is put after the fields that mark names: `(mark, MAX, place)`.
pub type Key = (MStamp, usize, Option<(MStamp, usize)>);

/// What the log says about the places of the fields, for a writer whose causal past is `past`.
#[derive(Clone, Debug, Default)]
pub struct Places {
    /// Fields with a standing place (a candidate named by a write in the past, above the unsets and resets of it).
    pub born: BTreeMap<Vec<String>, Key>,
    /// Paths unset in the past (their own floor is an unset): their place may be a survivor's, which this does not restate.
    pub touched: BTreeSet<Vec<String>>,
}

impl Places {
    /// For the unit tests: the given paths have a place, in the order given, and no unset touched anything.
    pub fn of_paths(paths: &[&[&str]]) -> Places {
        let z = MStamp {
            hlc: kimmy_core::hlc::Hlc::ZERO,
            writer: kimmy_core::merge_state::WriterId {
                node: kimmy_core::ids::NodeId::from_bytes([0; 16]),
                epoch: kimmy_core::merge_state::Epoch::from_u64(0),
            },
        };
        Places {
            born: paths
                .iter()
                .enumerate()
                .map(|(i, p)| (p.iter().map(|s| (*s).to_string()).collect(), (z, i, None)))
                .collect(),
            touched: BTreeSet::new(),
        }
    }
}

/// The places of the fields in the causal past `past` (the model's rule, restated from the design: a field's place is its
/// earliest candidate since its last unset floor (strict) and its last reset (inclusive); a field a later level mark
/// did not name goes after the fields the mark names).
pub fn places(past: &BTreeSet<MStamp>, life: Option<MStamp>) -> Places {
    PAST.with(|t| {
        let t = t.borrow();
        let mut names: BTreeMap<Vec<String>, Vec<(MStamp, usize, bool)>> = BTreeMap::new();
        let mut floor_u: BTreeMap<Vec<String>, MStamp> = BTreeMap::new();
        let mut lvl: BTreeMap<Vec<String>, MStamp> = BTreeMap::new();
        let mut out = Places::default();
        for st in past {
            let Some(w) = t.get(st) else { continue };
            if w.life != life {
                continue; // another life of the document: its places are dead
            }
            for (i, n) in w.names.iter().enumerate() {
                names.entry(n.path.clone()).or_default().push((*st, i, n.reset));
            }
            for u in &w.unsets {
                let e = floor_u.entry(u.clone()).or_insert(*st);
                *e = (*e).max(*st);
                // the paths whose OWN floor is an unset (the model's survivor clamp applies to those alone)
                out.touched.insert(u.clone());
            }
            for l in &w.levels {
                let e = lvl.entry(l.clone()).or_insert(*st);
                *e = (*e).max(*st);
            }
        }
        for (path, ns) in names {
            let fu = floor_u.get(&path);
            let fr = ns.iter().filter(|n| n.2).map(|n| n.0).max();
            let born = ns
                .iter()
                .filter(|n| fu.is_none_or(|f| n.0 > *f) && fr.is_none_or(|f| n.0 >= f))
                .map(|n| (n.0, n.1))
                .min();
            let Some(b) = born else { continue };
            let parent = path[..path.len() - 1].to_vec();
            let key = match lvl.get(&parent) {
                Some(l) if b.0 < *l => (*l, usize::MAX, Some(b)),
                _ => (b.0, b.1, None),
            };
            out.born.insert(path, key);
        }
        out
    })
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

/// The writer's document after its write against the rules above. `Err` with the first disagreement. `places` is what the log
/// says about the places of the fields before the write; `one_life` says the document had one alive life before and after.
pub fn check(
    before: Option<&[(String, OrderedValue)]>,
    after: Option<&[(String, OrderedValue)]>,
    op: &OrdOp,
    places: &Places,
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
                if !had.contains(&at) && g.paths().iter().any(|q| q.0 == at) {
                    let parent = &at[..at.len() - 1];
                    let key = |n: &String| {
                        let mut q = parent.to_vec();
                        q.push(n.clone());
                        q
                    };
                    if let Some(kf) = places.born.get(&at) {
                        // the field's place stands: the fields whose places are plain are ordered against it by their places
                        let names: Vec<String> = g
                            .level(parent)
                            .map(|l| l.iter().map(|(n, _)| n.clone()).collect())
                            .unwrap_or_default();
                        let i = names
                            .iter()
                            .position(|n| Some(n) == at.last())
                            .expect("the field is there");
                        // the maps of the level with no leaf below them and no place (the no-born group of the model's `okey`)
                        // trail every field that has a place: the created field comes before each of them
                        let unplaced_map = |n: &String| {
                            b.level(parent).is_some_and(|l| {
                                l.iter().any(|(m, kids)| {
                                    m == n
                                        && kids.as_ref().is_some_and(|k| !k.has_leaf())
                                        && !places.born.contains_key(&key(m))
                                })
                            })
                        };
                        for (j, m) in names.iter().enumerate() {
                            let q = key(m);
                            if j == i || named.contains(&q) {
                                continue;
                            }
                            if unplaced_map(m) {
                                if j < i {
                                    return fail(
                                        "a field created again is after a map that has no place",
                                    );
                                }
                                continue;
                            }
                            if places.touched.contains(&q) {
                                continue;
                            }
                            // every visible field has a place in the model (a leaf has one, a map with a leaf below it has
                            // one); a sibling that is neither placed, nor touched, nor an unplaced leafless map is drift
                            // between this oracle's restatement and the model, never skipped
                            let Some(kg) = places.born.get(&q) else {
                                return fail(&format!(
                                    "a sibling the log gives no place: {} at the level of {}",
                                    q.join("."),
                                    if parent.is_empty() {
                                        "the root".to_string()
                                    } else {
                                        parent.join(".")
                                    }
                                ));
                            };
                            if (kf < kg) != (i < j) {
                                return fail(
                                    "a field created again is not at the place a write of the past gave it",
                                );
                            }
                        }
                        return Ok(Checked { any: true, ..Checked::default() });
                    }
                    // the maps of the level, before the write, that no earlier write gave a place
                    let unplaced: Vec<String> = b
                        .level(parent)
                        .map(|l| {
                            l.iter()
                                .filter(|(n, kids)| {
                                    kids.as_ref().is_some_and(|k| !k.has_leaf())
                                        && !places.born.contains_key(&key(n))
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
