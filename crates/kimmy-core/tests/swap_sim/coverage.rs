//! The coverage predicate, in one place. Step 1 (epochs and coverage) is only partly frozen, so the harness
//! decides "this entry, or everything up to this stamp, is covered" only through this trait.
//!
//! FROZEN and coded against: the stamp is `hlc || node || epoch`; coverage vectors are keyed by (node, epoch);
//! an entry is covered when the receiver's vector for the writer's (node, epoch) reaches its HLC; the oplog key
//! order is `(hlc || node || epoch)`.
//!
//! NOT FROZEN (assumed, to be listed in every report): the names and signatures below; that coverage is a
//! per-origin contiguous prefix of the origin's entries (the model's reading), equal to the product's vector when
//! delivery is in position; ADR-127's rule for raising the vector past a window's end; the full-stamp cursor;
//! far-future clock checks; epoch retirement.

use crate::entry::EntryId;
use crate::world::World;

pub trait CoverageOf {
    /// Does this member's coverage of `origin` reach `hlc`: every entry of the origin with `hlc <=` the given
    /// one is applied, history, or held as state? (`swapmodel5.py` `covers`, 358-366)
    fn covers(&mut self, w: &World, origin: usize, hlc: i64) -> bool;
    /// `covers`, and some held entry of the origin is above `hlc` (the D9 safety net; `covers_past`, 368-372).
    fn covers_past(&mut self, w: &World, origin: usize, hlc: i64) -> bool;
    /// The member holds the entry (applied, history, witnessed, or held as state).
    fn has(&self, id: EntryId) -> bool;
    /// Mark an entry held (the coverage claim rises when the prefix is contiguous).
    fn add(&mut self, id: EntryId);
}

/// The model's coverage: the contiguous prefix of the origin's entries (in the world's per-origin order)
/// that the member holds, with a monotone cursor per origin.
#[derive(Clone, Debug, Default)]
pub struct PrefixCoverage {
    held: Vec<u64>,
    cursor: Vec<usize>,
}

impl PrefixCoverage {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn count(&self) -> usize {
        self.held.iter().map(|w| w.count_ones() as usize).sum()
    }
    /// All held ids in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = EntryId> + '_ {
        self.held
            .iter()
            .enumerate()
            .flat_map(|(wi, &w)| (0..64).filter(move |b| w >> b & 1 == 1).map(move |b| wi * 64 + b))
    }
    pub fn union_with(&mut self, other: &PrefixCoverage) {
        if self.held.len() < other.held.len() {
            self.held.resize(other.held.len(), 0);
        }
        for (i, w) in other.held.iter().enumerate() {
            self.held[i] |= w;
        }
    }
}

impl CoverageOf for PrefixCoverage {
    fn covers(&mut self, w: &World, origin: usize, hlc: i64) -> bool {
        if self.cursor.len() <= origin {
            self.cursor.resize(origin + 1, 0);
        }
        let lst = &w.by_origin[origin];
        let mut i = self.cursor[origin];
        let n = lst.len();
        while i < n && self.has(lst[i]) {
            i += 1;
        }
        self.cursor[origin] = i;
        i >= n || w.ent(lst[i]).hlc > hlc
    }

    fn covers_past(&mut self, w: &World, origin: usize, hlc: i64) -> bool {
        if !self.covers(w, origin, hlc) {
            return false;
        }
        w.by_origin[origin].iter().any(|&id| w.ent(id).hlc > hlc && self.has(id))
    }

    fn has(&self, id: EntryId) -> bool {
        self.held.get(id / 64).is_some_and(|w| w >> (id % 64) & 1 == 1)
    }

    fn add(&mut self, id: EntryId) {
        if self.held.len() <= id / 64 {
            self.held.resize(id / 64 + 1, 0);
        }
        self.held[id / 64] |= 1 << (id % 64);
    }
}
