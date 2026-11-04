//! The order state (option A): the position of every field of the materialised document.
//!
//! A document's fields have an order, and two replicas that merge the same writes must show it
//! in the same order. The order cannot be a list that writes edit (an edit in a list does not
//! commute), so it is DERIVED: every field path keeps a small [`FieldOrd`] of the writes that
//! could give it a place, and the position of a field is a pure function of those (the model's
//! `Ord`, `ord_join` and `born_of`; the design's "materialized document's field order").
//!
//! # The state of one path
//!
//! For every `(life, path)` that a write named (every cell path, every map path, every proper
//! ancestor of a written path, the map a document-valued write replaces, and the root of a
//! replace):
//!
//! * `floor`: the newest write that made earlier candidates void. A [`Floor::U`] is an unset
//!   (STRICT: a candidate must be stamped above it) and remembers `prev`, the highest HLC its
//!   writer had seen, and whether the same write also wrote other fields (the update bit). A
//!   [`Floor::R`] is a replace (or a `$set` of a whole sub-document) that re-assigned the position
//!   of the path (INCLUSIVE: a candidate stamped at the floor is its own).
//! * `firsts`: for each writer, the earliest write to the path valid under the floor, with its
//!   position among the siblings that write named.
//! * `last`: the latest write to the path (the register's own birth), valid under the floor.
//! * `lvl`: the newest document-valued write at this MAP path (the level floor): the fields it
//!   never saw go after the fields it names.
//!
//! `born` of a path is the smallest valid candidate; the children of a map are shown in order of
//! `(born, path)`, a child with no born last. [`MergeState::ordered_document`] is that read.
//!
//! # What lives here, and what does not
//!
//! The type, its join ([`ord_join`]), the lowest feed primitive of the writer side
//! ([`MergeState::ord_apply`], with [`ord_events`]) and the read. The WRITER does not call the
//! feed yet: a state built by `Writer` carries no `FieldOrd`, and joins of such states are what
//! they were. The feed exists so that the join, the read and the codec can be tested against the
//! reference model on states that do carry them.
//!
//! # Keys and ordering
//!
//! A cell and an order entry are keyed by life and path alike. The ONE extra key is
//! [`OrdAt::Root`]: the model keys the root of a replace by the empty path, which a [`Path`]
//! cannot be, so it is a variant of its own (it sorts first, as `""` does).
//!
//! Stamps order by `(hlc, node, epoch)` and a path by its segments. Where the model compares a
//! dotted string the Rust compares the segment lists (`INTERFACE.md`, section 2).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use super::codec::DecodeError;
use super::join::JoinOptions;
use super::state::MergeState;
use super::types::{Epoch, InvalidPath, MStamp, Path, Value, WriterId};
use crate::hlc::Hlc;
use crate::ids::NodeId;

/// What an order entry is about: a field path, or the root of the document (the model's `""`,
/// which a replace names and which keeps only the level floor).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum OrdAt {
    Root,
    Field(Path),
}

impl OrdAt {
    /// The entry of the map one level up: the root for a top-level field, `None` for the root.
    fn parent(&self) -> Option<OrdAt> {
        match self {
            OrdAt::Root => None,
            OrdAt::Field(p) => Some(p.parent().map_or(OrdAt::Root, OrdAt::Field)),
        }
    }
}

/// A candidate position: the stamp of a write and the index of the field among the siblings
/// that write named.
pub type Cand = (MStamp, u64);

/// The write that voids the candidates older than it. Ordered as the model orders its floor
/// tuples `(stamp, "u", prev, bit)` and `(stamp, "r")`: by stamp, then a replace BELOW an unset
/// (`"r" < "u"`, so at one stamp the model's `max` is the unset), then `prev`, then the bit. (Unreachable
/// honestly: one write never names a path as both an unset and a write.)
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Floor {
    /// An `$unset` (u*): candidates must be stamped ABOVE it. `prev`: the highest HLC the
    /// unsetter had seen before the write. `others`: the update bit, "the unsetting write also
    /// wrote other fields" (what survives it goes after all of them).
    U { stamp: MStamp, prev: Hlc, others: bool },
    /// A replace: candidates must be stamped AT or above it. It carries no version vector (Q4(b)
    /// is rejected) and no bit.
    R { stamp: MStamp },
}

impl Floor {
    pub fn stamp(&self) -> MStamp {
        match self {
            Floor::U { stamp, .. } | Floor::R { stamp } => *stamp,
        }
    }

    fn key(&self) -> (MStamp, u8, Hlc, bool) {
        match self {
            Floor::U { stamp, prev, others } => (*stamp, 1, *prev, *others),
            Floor::R { stamp } => (*stamp, 0, Hlc::ZERO, false),
        }
    }
}

impl PartialOrd for Floor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Floor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

/// The order state of one path (the model's `Ord`).
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct FieldOrd {
    pub floor: Option<Floor>,
    /// The earliest valid write per writer. At most one entry per writer.
    pub firsts: BTreeMap<WriterId, Cand>,
    pub last: Option<Cand>,
    pub lvl: Option<MStamp>,
}

/// Is the candidate valid under the floor? `keep_all` is the mutant that never voids one.
fn valid_under(floor: &Option<Floor>, c: &Cand, keep_all: bool) -> bool {
    match floor {
        None => true,
        Some(_) if keep_all => true,
        Some(Floor::U { stamp, .. }) => c.0 > *stamp,
        Some(Floor::R { stamp }) => c.0 >= *stamp,
    }
}

impl FieldOrd {
    /// Does the entry say nothing: no floor, no candidate, no level mark? An empty entry and no
    /// entry are the same state, so the join never keeps one and `decode` refuses one. (An entry
    /// cannot void to nothing: voiding needs a floor, and the floor stays.)
    pub fn is_empty(&self) -> bool {
        self.floor.is_none() && self.firsts.is_empty() && self.last.is_none() && self.lvl.is_none()
    }

    /// The first clause of the reachability invariant (see `MergeState::check_decoded`, items 15 to
    /// 20) that this entry breaks, judged from the entry alone and the key it is under; `None` when
    /// it breaks none. Every clause is one a causal history cannot break, and none changes what the
    /// join does: they are what makes an entry look like the work of the feed.
    pub fn shape_error(&self, at: &OrdAt) -> Option<DecodeError> {
        // the root of a replace keeps a level mark and nothing else
        if *at == OrdAt::Root
            && (self.floor.is_some() || !self.firsts.is_empty() || self.last.is_some())
        {
            return Some(DecodeError::OrdRootNotALevel);
        }
        // a writer's first is a write of that writer
        if self.firsts.iter().any(|(w, c)| c.0.writer != *w) {
            return Some(DecodeError::OrdFirstNotByItsWriter);
        }
        // an unsetter knew only what is below its own stamp
        if let Some(Floor::U { stamp, prev, .. }) = self.floor
            && prev >= stamp.hlc
        {
            return Some(DecodeError::OrdPrevNotBelowStamp);
        }
        // `last` is the newest write the entry knows: a standing first is never after it, and
        // is never without it (what voids `last` voids every earlier first)
        if !self.firsts.is_empty()
            && self.last.is_none_or(|l| self.firsts.values().any(|c| c.0 > l.0))
        {
            return Some(DecodeError::OrdLastBehindAFirst);
        }
        // one write names a path once, so it has one position there
        let mut seen: BTreeMap<MStamp, u64> = BTreeMap::new();
        for c in self.firsts.values().copied().chain(self.last) {
            if *seen.entry(c.0).or_insert(c.1) != c.1 {
                return Some(DecodeError::OrdPositionsDisagree);
            }
        }
        // a standing level mark at a path is a document-valued write there, which also named the
        // path as a candidate: `last` is at or after it (the root has no candidates)
        if let Some(l) = self.lvl
            && *at != OrdAt::Root
            && valid_under(&self.floor, &(l, 0), false)
            && self.last.is_none_or(|c| c.0 < l)
        {
            return Some(DecodeError::OrdLevelWithoutLast);
        }
        None
    }

    /// Is `c` a candidate this entry's floor lets stand?
    pub fn valid(&self, c: &Cand, o: &JoinOptions) -> bool {
        valid_under(&self.floor, c, o.ord_keep_all())
    }

    /// Does every candidate stand under the floor? The entry of a state is always so (the join
    /// leaves it so, and `decode` refuses one that is not).
    pub fn is_normal(&self, o: &JoinOptions) -> bool {
        self.firsts.values().all(|c| self.valid(c, o))
            && self.last.is_none_or(|c| self.valid(&c, o))
    }

    /// Drop the candidates the floor voids.
    pub fn normalise(&mut self, o: &JoinOptions) {
        let (floor, keep_all) = (self.floor, o.ord_keep_all());
        self.firsts.retain(|_, c| valid_under(&floor, c, keep_all));
        if self.last.is_some_and(|c| !valid_under(&floor, &c, keep_all)) {
            self.last = None;
        }
    }

    /// The position this entry gives its path: the smallest valid candidate (the earliest write
    /// since the last void), or `None`.
    pub(crate) fn born(&self, o: &JoinOptions) -> Option<Born> {
        let lasts = if o.ord_no_last() { None } else { self.last };
        let cs = self.firsts.values().copied().chain(lasts).filter(|c| self.valid(c, o));
        let pick = if o.ord_latest() { cs.max() } else { cs.min() };
        pick.map(Born::of)
    }
}

/// The join of two order entries of one path (the model's `ord_join`): commutative and
/// idempotent on any two entries that are normal, associative on the entries of histories within
/// retention (ADR-085; the join-law tests pin it on the entries of the model's own runs). Beyond
/// retention an honest history can reach a non-associative triple (`ord_codec_tests`, the seed 1421
/// case), as the state itself is not associative there. The floor is the larger, each
/// writer keeps the earlier of the candidates the joined floor lets stand, `last` is the later,
/// `lvl` the newer; what the joined floor voids goes.
pub fn ord_join(a: &FieldOrd, b: &FieldOrd, o: &JoinOptions) -> FieldOrd {
    let floor = a.floor.max(b.floor);
    let keep_all = o.ord_keep_all();
    let mut out = FieldOrd { floor, ..FieldOrd::default() };
    for w in a.firsts.keys().chain(b.firsts.keys()) {
        let best = [a.firsts.get(w), b.firsts.get(w)]
            .into_iter()
            .flatten()
            .filter(|c| valid_under(&floor, c, keep_all))
            .min();
        if let Some(c) = best {
            out.firsts.insert(*w, *c);
        }
    }
    out.last = a.last.max(b.last);
    out.lvl = a.lvl.max(b.lvl);
    out.normalise(o);
    out
}

/// Fold one more entry into a join (`None` is "nothing yet"): the join of one entry is its
/// normal form.
pub(crate) fn ord_fold(acc: Option<FieldOrd>, x: &FieldOrd, o: &JoinOptions) -> FieldOrd {
    match acc {
        None => {
            let mut x = x.clone();
            x.normalise(o);
            x
        }
        Some(a) => ord_join(&a, x, o),
    }
}

/// The position key of a path: a candidate, or a candidate moved behind a write. The model's
/// tuples `(stamp, pos)` and `(stamp, 10**9, born)`; the derived order is the tuple order (a
/// shorter tuple before a longer one with the same start).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) struct Born {
    stamp: MStamp,
    pos: u64,
    behind: Option<Box<Born>>,
}

/// The position the model gives "after everything this write names" (`10**9`).
const AFTER_ALL: u64 = 1_000_000_000;

impl Born {
    /// The stamp the position starts with.
    #[cfg(test)]
    pub(crate) fn stamp(&self) -> MStamp {
        self.stamp
    }

    fn of((stamp, pos): Cand) -> Self {
        Born { stamp, pos, behind: None }
    }

    /// `b` placed after every field of the write stamped `stamp`.
    fn after(stamp: MStamp, b: Born) -> Self {
        Born { stamp, pos: AFTER_ALL, behind: Some(Box::new(b)) }
    }
}

/// The largest stamp at an HLC: "just after everything the writer saw at `prev`".
fn after_hlc(prev: Hlc) -> MStamp {
    let writer =
        WriterId { node: NodeId::from_bytes([0xff; 16]), epoch: Epoch([0xff; Epoch::LEN]) };
    MStamp { hlc: prev, writer }
}

// ---- the feed ---------------------------------------------------------------------------

/// What a write does to the order state of one path (the model's modes `w`, `r`, `u`, `L`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum OrdMode {
    /// The write names the field: a candidate.
    Write,
    /// A replace, or a `$set` of a whole sub-document, names the field: the floor goes up to the
    /// write, then it is a candidate.
    Reset,
    /// An unset of the field: the floor goes up.
    Unset,
    /// The write is a document-valued write at this map: the level floor goes up.
    Level,
}

/// One order event of a write, positioned: the index of `at` among the siblings the write names.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OrdEvent {
    pub at: OrdAt,
    pub mode: OrdMode,
    pub pos: u64,
}

/// The order events of a write from its `(path, mode)` items in write order: an unset and a level
/// mark take no position, every other item takes its index among the siblings (the paths with
/// the same parent) the write names, in the order it names them; an item repeated keeps its first
/// place (the model's `Member._events`).
pub fn ord_events(items: &[(OrdAt, OrdMode)], o: &JoinOptions) -> Vec<OrdEvent> {
    let mut seen: BTreeMap<Option<OrdAt>, BTreeMap<&OrdAt, u64>> = BTreeMap::new();
    let mut done: BTreeSet<(&OrdAt, OrdMode)> = BTreeSet::new();
    let mut out = Vec::new();
    for (at, mode) in items {
        if !done.insert((at, *mode)) {
            continue;
        }
        if matches!(mode, OrdMode::Unset | OrdMode::Level) {
            out.push(OrdEvent { at: at.clone(), mode: *mode, pos: 0 });
            continue;
        }
        let siblings = seen.entry(at.parent()).or_default();
        let next = siblings.len() as u64;
        let pos = *siblings.entry(at).or_insert(next);
        out.push(OrdEvent {
            at: at.clone(),
            mode: *mode,
            pos: if o.ord_no_pos() { 0 } else { pos },
        });
    }
    out
}

impl MergeState {
    /// The lowest feed of the order state (the model's `Member._ord_apply`): the events of the
    /// write `stamp` into `life`. `known` is the highest HLC the writer had seen before the write
    /// (what a `u` floor remembers). Only the tests call it until the writer feeds the order
    /// state (item O2).
    #[allow(dead_code)]
    pub(crate) fn ord_apply(
        &mut self,
        life: MStamp,
        stamp: MStamp,
        known: Hlc,
        events: &[OrdEvent],
        o: &JoinOptions,
    ) {
        let writer = stamp.writer;
        // the unsetting write also writes other fields: any event that is not an unset or a mark
        let others = events.iter().any(|e| !matches!(e.mode, OrdMode::Unset | OrdMode::Level));
        for e in events {
            let key = (life, e.at.clone());
            let mut ord = self.ords.get(&key).cloned().unwrap_or_default();
            match e.mode {
                OrdMode::Level => {
                    if o.ord_no_level_floor() {
                        continue;
                    }
                    ord.lvl = ord.lvl.max(Some(stamp));
                }
                OrdMode::Unset => {
                    ord.floor = ord.floor.max(Some(Floor::U { stamp, prev: known, others }));
                    ord.normalise(o);
                }
                OrdMode::Write | OrdMode::Reset => {
                    if e.mode == OrdMode::Reset && !o.ord_no_reset() {
                        ord.floor = ord.floor.max(Some(Floor::R { stamp }));
                    }
                    ord.normalise(o);
                    ord.firsts.entry(writer).or_insert((stamp, e.pos));
                    ord.last = Some((stamp, e.pos));
                }
            }
            // an entry that says nothing is no entry (the events above always say something)
            if !ord.is_empty() {
                self.ords.insert(key, ord);
            }
        }
    }
}

// ---- the read ---------------------------------------------------------------------------

/// The present children of each map (`None` is the root) as `(has no position, position, path)`:
/// sorted, that is the field order of the level.
type Kids<'a> = BTreeMap<Option<Path>, Vec<(bool, Option<Born>, &'a Path)>>;

/// A field of the ordered document: a value, or a sub-document whose fields are in order. The
/// document of a state is at most 100 levels deep (a path has at most 100 segments), so the DERIVED
/// `Clone`, `PartialEq`, `Debug` and `Drop` recurse at most 100 frames of a few hundred bytes (a test runs
/// all four on a 100-deep value in a thread of 256 KiB).
#[derive(Clone, PartialEq, Debug)]
pub enum OrderedValue {
    Leaf(Value),
    Doc(Vec<(String, OrderedValue)>),
}

impl OrderedValue {
    /// The paths of a document's fields, a map before the fields in it, each level in its order. An
    /// error (the `InvalidPath` of `Path::new`) when a field name has a NUL byte or a path is deeper than
    /// the limit of 100 segments, which a document built by a caller can be and the document of a state
    /// never is: no panic for caller input. The recursion stops at the first path that is refused, so it
    /// is at most 101 levels deep.
    pub fn paths(doc: &[(String, OrderedValue)]) -> Result<Vec<Path>, InvalidPath> {
        fn walk(
            at: &[String],
            doc: &[(String, OrderedValue)],
            out: &mut Vec<Path>,
        ) -> Result<(), InvalidPath> {
            for (name, v) in doc {
                let mut segs = at.to_vec();
                segs.push(name.clone());
                out.push(Path::new(segs.clone())?);
                if let OrderedValue::Doc(kids) = v {
                    walk(&segs, kids, out)?;
                }
            }
            Ok(())
        }
        let mut out = Vec::new();
        walk(&[], doc, &mut out)?;
        Ok(out)
    }
}

/// The results of `born_of` within ONE read of one state: `born_of` is a pure function of the state,
/// the path and the alive lives, and the clamp asks for the position of every descendant, each of
/// which asks for its own descendants (the model recomputes them: 2^(depth - 1) evaluations for a
/// chain of survivors, a few kilobytes of decodable input). Memoised, each path is evaluated once.
/// `memoise: false` is the unmemoised reference the tests compare with.
pub(crate) struct BornMemo {
    memoise: bool,
    memo: RefCell<BTreeMap<Path, Option<Born>>>,
    /// How many times `born_of` evaluated a path (not answered from the memo).
    evaluations: Cell<usize>,
}

impl BornMemo {
    pub(crate) fn new(memoise: bool) -> Self {
        Self { memoise, memo: RefCell::new(BTreeMap::new()), evaluations: Cell::new(0) }
    }

    #[cfg(test)]
    pub(crate) fn evaluations(&self) -> usize {
        self.evaluations.get()
    }
}

impl MergeState {
    /// The position key of `p` over the alive lives (the model's `born_of`): the order entries of
    /// the alive lives, which are one document, join first; then the survivor clamp and the level
    /// floor. `None` when nothing gives `p` a place.
    #[cfg(test)]
    pub(crate) fn born_of(&self, p: &Path, al: &BTreeSet<MStamp>, o: &JoinOptions) -> Option<Born> {
        self.born_in(p, al, o, &BornMemo::new(true))
    }

    /// [`MergeState::born_of`] within one read: answered from `memo` when the path was evaluated.
    fn born_in(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
        memo: &BornMemo,
    ) -> Option<Born> {
        if memo.memoise
            && let Some(b) = memo.memo.borrow().get(p)
        {
            return b.clone();
        }
        memo.evaluations.set(memo.evaluations.get() + 1);
        let b = self.born_eval(p, al, o, memo);
        if memo.memoise {
            memo.memo.borrow_mut().insert(p.clone(), b.clone());
        }
        b
    }

    fn born_eval(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
        memo: &BornMemo,
    ) -> Option<Born> {
        let at = OrdAt::Field(p.clone());
        let joined = al
            .iter()
            .filter_map(|l| self.ords.get(&(*l, at.clone())))
            .fold(None, |acc, x| Some(ord_fold(acc, x, o)));
        let mut b = joined.as_ref().and_then(|j| j.born(o));
        if !o.ord_no_survivor_clamp()
            && let Some(Floor::U { stamp, prev, others }) = joined.as_ref().and_then(|j| j.floor)
        {
            for c in self.survivors(p, al, (stamp, prev, others), o, memo) {
                if b.as_ref().is_none_or(|x| c < *x) {
                    b = Some(c);
                }
            }
        }
        let b = b?;
        if o.ord_no_level_floor() {
            return Some(b);
        }
        let parent = p.parent().map_or(OrdAt::Root, OrdAt::Field);
        let lvl = al
            .iter()
            .filter_map(|l| self.ords.get(&(*l, parent.clone())))
            .filter_map(|x| x.lvl)
            .max();
        Some(match lvl {
            // a field the replacing document never saw (it survives the diff) goes after the
            // fields that document names
            Some(lv) if b.stamp < lv => Born::after(lv, b),
            _ => b,
        })
    }

    /// The candidates the content that survives the unset of `p` gives `p` (the model's
    /// `_survivors`). An unset removes exactly what its writer saw; what is still there below it
    /// was not seen and survives. Replayed in the serial order that explains the value, the unset
    /// comes first and the survivor is (re-)created after it: not before everything the unsetter
    /// had seen (`prev`), and, when the unset's write also wrote other fields, after all of them.
    ///
    /// A slot survives when it moved since the winning base observed it (`obs` of the winner). The
    /// model's A2 round also remembers, per slot, the highest observation any base write made
    /// (`absorbed`); the Rust counter cell has no such field yet (the cell is not frozen), so the
    /// clamp reads the winner's `obs` only, which is the model with `ABSORBED` off. The
    /// goldens' generator asserts that every scenario orders the same with it on and off.
    ///
    /// Cost. Each path under an unset floor looks at every path below it, so a chain of depth d costs
    /// d(d - 1)/2 visits: quadratic only because of the cap of 100 on the depth of a path (at most about
    /// 5,000 pairs per chain, `ord_tests`). If the depth limit is ever raised, a bottom-up form exists:
    /// the survivors of a path are "the smallest candidate below a threshold" over its subtree
    /// (the placement of a candidate depends on the asking path's floor only through a threshold and the
    /// update bit), which a merge of the children's sorted candidates answers in about O(n log^2 n).
    fn survivors(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        unset: (MStamp, Hlc, bool),
        o: &JoinOptions,
        memo: &BornMemo,
    ) -> Vec<Born> {
        let (floor, prev, others) = unset;
        let others = others && !o.ord_ignores_bit();
        let after_known = after_hlc(prev);
        let place = |c: Born| -> Born {
            if others {
                Born::after(floor, c)
            } else if c.stamp >= after_known {
                c
            } else {
                Born { stamp: after_known, pos: 0, behind: None }
            }
        };
        let slot_survivors = |q: &Path| -> Vec<Born> {
            let win = self.winner_in(q, al, o);
            let mut out = Vec::new();
            for (k, s) in self.counted_in(q, al, win, o) {
                let seen = win.and_then(|w| w.obs.get(&k));
                let moved = match seen {
                    None => true,
                    Some((st, total)) => {
                        !s.total.sub_sat(total, &mut None).is_zero() || *st != s.stamp
                    }
                };
                if s.stamp < floor && moved {
                    out.push(place(Born::of((s.stamp, 0))));
                }
            }
            out
        };
        let mut out = slot_survivors(p);
        if let Some(w) = self.winner_in(p, al, o)
            && w.value.is_plain()
            && w.stamp < floor
        {
            // only reachable with a floor that no unset register backs (the no-op unset)
            out.push(place(Born::of((w.stamp, 0))));
        }
        let below = self.paths_below(p, al);
        for q in below {
            if let Some(w) = self.winner_in(q, al, o)
                && w.value.is_plain()
                && w.stamp < floor
            {
                let bq = self.born_in(q, al, o, memo).unwrap_or_else(|| Born::of((w.stamp, 0)));
                out.push(place(bq));
            }
            out.extend(slot_survivors(q));
        }
        out
    }

    /// The paths below `p` that have a register or a counter slot in an alive life (the model's
    /// `{k[1] for k in regs + slots if k[0] in al and is_under(k[1], p)}`), without a scan of the maps:
    /// the maps are ordered by (life, path) and a path sorts before every path it is a prefix of, so
    /// the descendants of `p` in one life are the keys right after `p`, one contiguous run. The cost is
    /// that of the descendants, not of the state (D-1 of the review: a scan made the read quadratic).
    fn paths_below<'a>(&'a self, p: &Path, al: &BTreeSet<MStamp>) -> BTreeSet<&'a Path> {
        let mut out = BTreeSet::new();
        // the smallest writer, and no generation (which sorts first), as `slots_at` does
        let least = WriterId { node: NodeId::from_bytes([0; 16]), epoch: Epoch([0; Epoch::LEN]) };
        for l in al {
            for ((kl, q), _) in
                self.regs.range((Bound::Excluded((*l, p.clone())), Bound::Unbounded))
            {
                #[cfg(test)]
                super::work::bump(super::work::Counter::Visits, 1);
                if kl != l || !q.is_below(p) {
                    break;
                }
                out.insert(q);
            }
            for ((kl, q, _, _), _) in self.slots.range((*l, p.clone(), least, None)..) {
                #[cfg(test)]
                super::work::bump(super::work::Counter::Visits, 1);
                if kl != l || !(q == p || q.is_below(p)) {
                    break;
                }
                if q != p {
                    out.insert(q);
                }
            }
        }
        out
    }

    /// The materialised document in field order (Step 2's `render()` in the making): the
    /// visible document of the type rule, as a tree whose every level lists its fields by
    /// `(born, path)`, a field with no born last. `None` when no life is alive. Two ordered
    /// documents are the same when they are equal as values: type for type, field order
    /// included (the model's `ordered` and `tyrepr`).
    pub fn ordered_document(&self, o: &JoinOptions) -> Option<Vec<(String, OrderedValue)>> {
        self.ordered_document_with(o, &BornMemo::new(true))
    }

    /// [`MergeState::ordered_document`] with the memo of the read given (tests count its
    /// evaluations, or switch it off to get the unmemoised reference).
    pub(crate) fn ordered_document_with(
        &self,
        o: &JoinOptions,
        memo: &BornMemo,
    ) -> Option<Vec<(String, OrderedValue)>> {
        let vis = self.visible(o)?;
        let al = self.alive_set(o.retention);
        let mut kids: Kids = BTreeMap::new();
        for p in vis.keys() {
            let b = if o.ord_by_name() { None } else { self.born_in(p, &al, o, memo) };
            kids.entry(p.parent()).or_default().push((b.is_none(), b, p));
        }
        for list in kids.values_mut() {
            list.sort();
        }
        fn build(
            at: Option<Path>,
            kids: &Kids,
            vis: &BTreeMap<Path, Value>,
        ) -> Vec<(String, OrderedValue)> {
            kids.get(&at)
                .into_iter()
                .flatten()
                .map(|(_, _, p)| {
                    let v = match &vis[*p] {
                        Value::Map => OrderedValue::Doc(build(Some((*p).clone()), kids, vis)),
                        v => OrderedValue::Leaf(v.clone()),
                    };
                    (p.name().to_string(), v)
                })
                .collect()
        }
        Some(build(None, &kids, &vis))
    }
}
