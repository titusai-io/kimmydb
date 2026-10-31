//! The join of two merge states (the model's `join`).

use std::collections::{BTreeMap, BTreeSet};

use super::state::{LifeKind, MergeState, cov, sub_ret};
use super::types::{LossKey, LossKind, LossValue, MStamp};
use crate::hlc::Hlc;

/// A refused integer bound: it must not be negative.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("merge state: the integer bound {0} is negative")]
pub struct InvalidIntBound(pub i64);

/// Parameters of the join and of the read side.
///
/// The fields are private so that the set of fields never depends on a build
/// configuration: build one with [`JoinOptions::new`] or [`Default`], and change the
/// retention through the public field. The mutation switches (horizon-first,
/// supersession, reported cells, `$setOnInsert` suppression, the v3 generation rule, the
/// two read and creation rules of the B1 and M1 fixes, and write normalisation) can be
/// changed only in a test or `merge-mutants` build, through the `set_*` methods that exist
/// there: they let the harness reproduce the model's mutants. The production code path is
/// the same code with the switches at their correct values.
#[derive(Clone, Debug)]
pub struct JoinOptions {
    /// `R`: how far behind the newest known HLC (in wall milliseconds) deletes,
    /// dead lives and loss records are kept.
    pub retention: u64,
    /// `BOUND` (Q11): an integer total saturates to `[-bound - 1, bound]`. Never negative.
    int_bound: i64,
    /// V3M1: filter by the JOINED horizon before the union.
    #[cfg(any(test, feature = "merge-mutants"))]
    horizon_first: bool,
    /// Q2: concurrent first inserts, the newest whole document wins.
    #[cfg(any(test, feature = "merge-mutants"))]
    supersede: bool,
    /// V6M1: writes into a superseded first insert are reported.
    #[cfg(any(test, feature = "merge-mutants"))]
    cells_reported: bool,
    /// V6m2/V7m1: `$setOnInsert` creation cells of an upsert are dropped from
    /// the read side while an explicit insert is alive.
    #[cfg(any(test, feature = "merge-mutants"))]
    suppress_soi: bool,
    /// V3M3: the v3 generation rule (the newest non-numeric base counts, even
    /// when it lost). Off in the design.
    #[cfg(any(test, feature = "merge-mutants"))]
    v3_generation: bool,
    /// B1 mutant: the read side ignores the slot stamp in a register's `obs`, so a
    /// counter whose increments net to 0 is absent again. Off in the design.
    #[cfg(any(test, feature = "merge-mutants"))]
    value_ignores_slot_stamp: bool,
    /// M1 mutant: an upsert creation with a non-numeric value starts no
    /// generation (the model before the fix). Off in the design.
    #[cfg(any(test, feature = "merge-mutants"))]
    creation_without_generation: bool,
    /// The model's `NORMALIZE_WRITES`: a writer normalises its own state through
    /// a join with the empty state after each write (off only to reproduce the
    /// un-normalised states of `model5.Member`).
    #[cfg(any(test, feature = "merge-mutants"))]
    normalize_writes: bool,
}

impl Default for JoinOptions {
    fn default() -> Self {
        Self {
            // The model's default (10^9), far beyond any test horizon.
            retention: 1_000_000_000,
            int_bound: i64::MAX,
            #[cfg(any(test, feature = "merge-mutants"))]
            horizon_first: true,
            #[cfg(any(test, feature = "merge-mutants"))]
            supersede: true,
            #[cfg(any(test, feature = "merge-mutants"))]
            cells_reported: true,
            #[cfg(any(test, feature = "merge-mutants"))]
            suppress_soi: true,
            #[cfg(any(test, feature = "merge-mutants"))]
            v3_generation: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            value_ignores_slot_stamp: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            creation_without_generation: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            normalize_writes: true,
        }
    }
}

/// One accessor per mutation switch: the field in a test or `merge-mutants` build, the
/// correct value otherwise.
macro_rules! switch {
    ($name:ident, $setter:ident, $correct:expr) => {
        #[cfg(any(test, feature = "merge-mutants"))]
        pub(super) fn $name(&self) -> bool {
            self.$name
        }
        #[cfg(not(any(test, feature = "merge-mutants")))]
        pub(super) fn $name(&self) -> bool {
            $correct
        }
        #[cfg(any(test, feature = "merge-mutants"))]
        pub fn $setter(&mut self, on: bool) {
            self.$name = on;
        }
    };
}

impl JoinOptions {
    /// Options with the given retention and integer bound. The bound must not be negative.
    pub fn new(retention: u64, int_bound: i64) -> Result<Self, InvalidIntBound> {
        let mut o = Self { retention, ..Self::default() };
        o.set_int_bound(int_bound)?;
        Ok(o)
    }

    /// The integer bound: totals saturate to `[-bound - 1, bound]`.
    pub fn int_bound(&self) -> i64 {
        self.int_bound
    }

    /// Change the integer bound; a negative one is refused and nothing changes.
    pub fn set_int_bound(&mut self, bound: i64) -> Result<(), InvalidIntBound> {
        if bound < 0 {
            return Err(InvalidIntBound(bound));
        }
        self.int_bound = bound;
        Ok(())
    }

    switch!(horizon_first, set_horizon_first, true);
    switch!(supersede, set_supersede, true);
    switch!(cells_reported, set_cells_reported, true);
    switch!(suppress_soi, set_suppress_soi, true);
    switch!(v3_generation, set_v3_generation, false);
    switch!(value_ignores_slot_stamp, set_value_ignores_slot_stamp, false);
    switch!(creation_without_generation, set_creation_without_generation, false);
    switch!(normalize_writes, set_normalize_writes, true);
}

/// The model's `_prefilter`: drop deletes and dead lives older than `h`.
fn prefilter(st: &MergeState, h: Hlc) -> MergeState {
    let mut st = st.clone();
    st.deletes.retain(|d, _| d.hlc >= h);
    let gone: Vec<MStamp> = st.dead.iter().filter(|l| l.hlc < h).copied().collect();
    for l in gone {
        st.dead.remove(&l);
        st.lives.remove(&l);
    }
    st
}

/// Join `t` into `s`. Returns the new state and the keys of the loss records
/// that are new relative to `s` (the model's `fresh`).
///
/// V3M1: the horizon of the JOINED version vectors is applied to both sides
/// BEFORE the union and before aliveness.
pub fn join(s: &MergeState, t: &MergeState, o: &JoinOptions) -> (MergeState, BTreeSet<LossKey>) {
    let ret = o.retention;
    let h0 = sub_ret(s.max_vv().max(t.max_vv()), ret);
    let (s, t) = if o.horizon_first() {
        (prefilter(s, h0), prefilter(t, h0))
    } else {
        (s.clone(), t.clone())
    };
    let mut out = s.clone();

    // Union. `ok` is evaluated against the dead set of the moment, as in the model.
    for (l, v) in &t.lives {
        if !out.dead.contains(l) {
            out.lives.entry(*l).or_insert_with(|| v.clone());
        }
    }
    out.dead.extend(t.dead.iter().copied());
    for (d, vv) in &t.deletes {
        out.deletes.entry(*d).or_insert_with(|| vv.clone());
    }
    for (k, r) in &t.regs {
        if !out.dead.contains(&k.0) && out.regs.get(k).is_none_or(|x| r.stamp > x.stamp) {
            out.regs.insert(k.clone(), r.clone());
        }
    }
    for (k, v) in &t.slots {
        if !out.dead.contains(&k.0) && out.slots.get(k).is_none_or(|x| v.stamp > x.stamp) {
            out.slots.insert(k.clone(), v.clone());
        }
    }
    for (k, g) in &t.gens {
        if !out.dead.contains(&k.0) && out.gens.get(k).is_none_or(|x| g.0 > x.0) {
            out.gens.insert(k.clone(), g.clone());
        }
    }
    for (w, h) in &t.vv {
        let e = out.vv.entry(*w).or_insert(Hlc::ZERO);
        *e = (*e).max(*h);
    }

    let mut new: BTreeMap<LossKey, LossValue> = BTreeMap::new();
    let key =
        |kind: LossKind, path: &str, stamp: MStamp| LossKey { kind, path: path.to_string(), stamp };

    // Q2: concurrent FIRST inserts of one _id (explicit inserts whose creator had
    // seen no delete): the newest whole document wins, the others are superseded
    // and reported. Re-inserts after a delete and upserts still merge.
    let first: Vec<MStamp> = out
        .lives
        .iter()
        .filter(|(_, v)| v.kind == LifeKind::Insert && v.seen.is_empty())
        .map(|(l, _)| *l)
        .collect();
    let newest_first = first.iter().max().copied();
    let superseded: BTreeSet<MStamp> =
        first.iter().copied().filter(|l| Some(*l) != newest_first).collect();
    let reported: BTreeSet<MStamp>;
    if o.supersede() {
        out.dead.extend(superseded.iter().copied());
        // V6m1: a first insert that a delete already killed is dead, not lost to a newer insert.
        let killed: BTreeSet<MStamp> = superseded
            .iter()
            .copied()
            .filter(|l| {
                out.deletes
                    .keys()
                    .any(|d| !out.lives[l].seen.contains(d) && d.hlc >= sub_ret(l.hlc, ret))
            })
            .collect();
        reported = superseded.difference(&killed).copied().collect();
        for l in &reported {
            new.insert(
                key(LossKind::InsertLost, "_", *l),
                LossValue::Fields(out.lives[l].order.clone()),
            );
        }
    } else {
        reported = BTreeSet::new();
    }
    let al = out.alive_set(ret);
    let not_alive: Vec<MStamp> = out.lives.keys().filter(|l| !al.contains(l)).copied().collect();
    out.dead.extend(not_alive);

    // (1) same-path register clashes: a register of s against a register of t,
    // concurrent by the version vectors.
    let paths: BTreeSet<&String> = s.regs.keys().chain(t.regs.keys()).map(|k| &k.1).collect();
    for p in paths {
        let rs = s.regs.iter().filter(|(k, _)| &k.1 == p && al.contains(&k.0)).map(|(_, r)| r);
        let rt: Vec<_> =
            t.regs.iter().filter(|(k, _)| &k.1 == p && al.contains(&k.0)).map(|(_, r)| r).collect();
        for a in rs {
            for b in &rt {
                if a.stamp != b.stamp && !cov(&t.vv, &a.stamp) && !cov(&s.vv, &b.stamp) {
                    let lo = if a.stamp < b.stamp { a } else { b };
                    new.insert(
                        key(LossKind::Clash, p, lo.stamp),
                        LossValue::Value(lo.value.clone()),
                    );
                }
            }
        }
    }

    // (2) a cell in a dead life that no known delete saw.
    let seen_by_delete = |st: &MStamp| out.deletes.values().any(|vv| cov(vv, st));
    for src in [&s, &t] {
        for (k, r) in &src.regs {
            if o.cells_reported() && reported.contains(&k.0) && r.stamp != k.0 {
                // V6M1: a write made into the losing life.
                new.insert(
                    key(LossKind::InsertLost, &k.1, r.stamp),
                    LossValue::Value(r.value.clone()),
                );
            } else if !al.contains(&k.0) && !reported.contains(&k.0) && !seen_by_delete(&r.stamp) {
                new.insert(
                    key(LossKind::DeleteWon, &k.1, r.stamp),
                    LossValue::Value(r.value.clone()),
                );
            }
        }
        for (k, v) in &src.slots {
            if o.cells_reported() && reported.contains(&k.0) {
                new.insert(key(LossKind::InsertLost, &k.1, v.stamp), LossValue::Total(v.total));
            } else if !al.contains(&k.0) && !reported.contains(&k.0) && !seen_by_delete(&v.stamp) {
                new.insert(key(LossKind::DeleteWon, &k.1, v.stamp), LossValue::Total(v.total));
            }
        }
    }

    // V6m2: creation cells of an upsert life dropped for an alive explicit first insert.
    if al.iter().any(|l| out.lives[l].kind == LifeKind::Insert) {
        for src in [&s, &t] {
            for (k, r) in &src.regs {
                if r.soi
                    && al.contains(&k.0)
                    && out.lives.get(&k.0).is_some_and(|l| l.kind == LifeKind::Upsert)
                {
                    new.insert(
                        key(LossKind::InsertLost, &k.1, r.stamp),
                        LossValue::Value(r.value.clone()),
                    );
                }
            }
        }
    }

    // Drop cells of dead lives; filter records by the horizon.
    out.regs.retain(|k, _| al.contains(&k.0));
    out.slots.retain(|k, _| al.contains(&k.0));
    out.gens.retain(|k, _| al.contains(&k.0));
    let h = out.horizon(o);
    out.deletes.retain(|d, _| d.hlc >= h);
    let dead_old: Vec<MStamp> =
        out.lives.keys().filter(|l| l.hlc < h && out.dead.contains(l)).copied().collect();
    for l in dead_old {
        out.lives.remove(&l);
    }
    out.dead.retain(|l| l.hlc >= h);
    out.losses = BTreeMap::new();
    let all = s.losses.iter().chain(t.losses.iter()).chain(new.iter());
    for (k, v) in all {
        if k.stamp.hlc >= h {
            out.losses.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
    let fresh = out.losses.keys().filter(|k| !s.losses.contains_key(k)).cloned().collect();
    (out, fresh)
}

impl MergeState {
    /// `join(self, other)`, see [`join()`].
    pub fn join(&self, other: &MergeState, o: &JoinOptions) -> (MergeState, BTreeSet<LossKey>) {
        join(self, other, o)
    }
}
