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

use std::cell::RefCell;

use crate::entry::EntryId;
use crate::world::World;

pub trait CoverageOf {
    /// Does this member's coverage of `origin` reach `hlc`: every entry of the origin with `hlc <=` the given
    /// one is applied, history, or held as state? (`swapmodel5.py` `covers`, 358-366)
    fn covers(&self, w: &World, origin: usize, hlc: i64) -> bool;
    /// `covers`, and some held entry of the origin is above `hlc` (the D9 safety net; `covers_past`, 368-372).
    fn covers_past(&self, w: &World, origin: usize, hlc: i64) -> bool;
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
    cursor: RefCell<Vec<usize>>,
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

impl PrefixCoverage {
    /// How many of the origin's entries (in the world's per-origin order) form the contiguous held prefix (the cursor of `covers`).
    pub fn prefix_len(&self, w: &World, origin: usize) -> usize {
        let mut cursor = self.cursor.borrow_mut();
        if cursor.len() <= origin {
            cursor.resize(origin + 1, 0);
        }
        let lst = &w.by_origin[origin];
        let mut i = cursor[origin];
        while i < lst.len() && self.has(lst[i]) {
            i += 1;
        }
        cursor[origin] = i;
        i
    }
}

/// The product's coverage (B2 of the harness review; FIX-1): a per-origin (node, epoch) MAXIMUM, `vector.get(stamp.node) >=
/// stamp.hlc`. It is raised by an apply under `Position::Raise` or `InWindow` (and by a completed grant, to the grant's vector), never
/// by `Position::Hold`. It is not a prefix: a `Raise` over a hole, a push applied under `Raise`, or a replay that skips an entry make
/// it cover an entry the member never received, which the prefix cannot represent. `has`/`add` keep the ids the member holds.
#[derive(Clone, Debug, Default)]
pub struct VectorCoverage {
    max: Vec<i64>,
    ids: PrefixCoverage,
}

impl VectorCoverage {
    pub fn new() -> Self {
        Self::default()
    }
    /// Raise the origin's maximum to `hlc` (an apply under Raise or InWindow, a grant).
    pub fn raise(&mut self, origin: usize, hlc: i64) {
        if self.max.len() <= origin {
            self.max.resize(origin + 1, -1);
        }
        if self.max[origin] < hlc {
            self.max[origin] = hlc;
        }
    }
    /// The origin's maximum (-1: nothing).
    pub fn max_of(&self, origin: usize) -> i64 {
        self.max.get(origin).copied().unwrap_or(-1)
    }
    /// The vector a coverage prefix implies (a restore from a backup whose vector is its own contiguous prefix).
    pub fn from_prefix(w: &World, cv: &PrefixCoverage) -> Self {
        let mut v = Self::new();
        for o in 0..w.by_origin.len() {
            let n = cv.prefix_len(w, o);
            if n > 0 {
                v.raise(o, w.ent(w.by_origin[o][n - 1]).hlc);
            }
        }
        for i in cv.iter() {
            v.ids.add(i);
        }
        v
    }
}

impl CoverageOf for VectorCoverage {
    fn covers(&self, _w: &World, origin: usize, hlc: i64) -> bool {
        self.max_of(origin) >= hlc
    }
    fn covers_past(&self, _w: &World, origin: usize, hlc: i64) -> bool {
        self.max_of(origin) > hlc
    }
    fn has(&self, id: EntryId) -> bool {
        self.ids.has(id)
    }
    fn add(&mut self, id: EntryId) {
        self.ids.add(id);
    }
}

impl CoverageOf for PrefixCoverage {
    fn covers(&self, w: &World, origin: usize, hlc: i64) -> bool {
        let mut cursor = self.cursor.borrow_mut();
        if cursor.len() <= origin {
            cursor.resize(origin + 1, 0);
        }
        let lst = &w.by_origin[origin];
        let mut i = cursor[origin];
        let n = lst.len();
        while i < n && self.has(lst[i]) {
            i += 1;
        }
        cursor[origin] = i;
        i >= n || w.ent(lst[i]).hlc > hlc
    }

    fn covers_past(&self, w: &World, origin: usize, hlc: i64) -> bool {
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
