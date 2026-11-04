//! The join of two merge states (the model's `join`).

use std::collections::{BTreeMap, BTreeSet};

use super::state::{LifeKind, MergeState, cov, sub_ret};
use super::types::{LossKey, LossKind, LossValue, MStamp, Path, Value};
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
/// two read and creation rules of the B1 and M1 fixes, write normalisation and the six nested
/// mutants: the type rule's recency, a marker that hides, `typeClash`, `arrayClash`, the
/// sub-document diff and the refusal below a scalar) can be
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
    /// Nested mutant: a scalar register hides every child, even a newer one.
    #[cfg(any(test, feature = "merge-mutants"))]
    type_ignores_recency: bool,
    /// Nested mutant: a map marker hides its children.
    #[cfg(any(test, feature = "merge-mutants"))]
    marker_hides: bool,
    /// Nested mutant: the join reports no `typeClash`.
    #[cfg(any(test, feature = "merge-mutants"))]
    no_type_clash: bool,
    /// Nested mutant: two concurrent arrays are reported as a plain `clash`.
    #[cfg(any(test, feature = "merge-mutants"))]
    array_clash_as_clash: bool,
    /// Nested mutant: a sub-document write rewrites every leaf, changed or not.
    #[cfg(any(test, feature = "merge-mutants"))]
    rewrite_unchanged: bool,
    /// Nested mutant: a write below a scalar or an array is accepted.
    #[cfg(any(test, feature = "merge-mutants"))]
    write_below_scalar: bool,
    /// The read side adds a cell's slots in the order of their stamps instead of the canonical
    /// order (writer `(node, epoch)`, then life and generation): a `Decimal128` total then
    /// depends on whose write was the later.
    #[cfg(any(test, feature = "merge-mutants"))]
    sum_in_stamp_order: bool,
    /// An integer cell that crosses the integer bound wraps instead of saturating.
    #[cfg(any(test, feature = "merge-mutants"))]
    no_saturation: bool,
    /// A `Decimal128` cell that overflows from finite parts becomes an infinity instead of
    /// saturating at the largest finite value of its sign.
    #[cfg(any(test, feature = "merge-mutants"))]
    no_decimal_saturation: bool,
    /// An `Int32` cell that outgrows the `i32` range saturates instead of becoming an `Int64`.
    #[cfg(any(test, feature = "merge-mutants"))]
    int32_saturates: bool,
    /// A writer whose increment made the visible value an `Int64` still leaves its slot an
    /// `Int32`, so a total that dips back under the `i32` range narrows again.
    #[cfg(any(test, feature = "merge-mutants"))]
    slot_never_widens: bool,
    /// A local `$inc` by a 65th contributor is accepted.
    #[cfg(any(test, feature = "merge-mutants"))]
    no_contributor_cap: bool,
    /// A local write by a 257th writer is accepted.
    #[cfg(any(test, feature = "merge-mutants"))]
    no_writer_cap: bool,
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
            #[cfg(any(test, feature = "merge-mutants"))]
            type_ignores_recency: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            marker_hides: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            no_type_clash: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            array_clash_as_clash: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            rewrite_unchanged: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            write_below_scalar: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            sum_in_stamp_order: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            no_saturation: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            no_decimal_saturation: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            int32_saturates: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            slot_never_widens: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            no_contributor_cap: false,
            #[cfg(any(test, feature = "merge-mutants"))]
            no_writer_cap: false,
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
    switch!(type_ignores_recency, set_type_ignores_recency, false);
    switch!(marker_hides, set_marker_hides, false);
    switch!(no_type_clash, set_no_type_clash, false);
    switch!(array_clash_as_clash, set_array_clash_as_clash, false);
    switch!(rewrite_unchanged, set_rewrite_unchanged, false);
    switch!(write_below_scalar, set_write_below_scalar, false);
    switch!(sum_in_stamp_order, set_sum_in_stamp_order, false);
    switch!(no_saturation, set_no_saturation, false);
    switch!(no_decimal_saturation, set_no_decimal_saturation, false);
    switch!(int32_saturates, set_int32_saturates, false);
    switch!(slot_never_widens, set_slot_never_widens, false);
    switch!(no_contributor_cap, set_no_contributor_cap, false);
    switch!(no_writer_cap, set_no_writer_cap, false);
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
        |kind: LossKind, path: &Path, stamp: MStamp| LossKey::field(kind, path.clone(), stamp);

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
                LossKey::document(LossKind::InsertLost, *l),
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
    // concurrent by the version vectors. Two arrays are an `arrayClash`.
    let paths: BTreeSet<&Path> = s.regs.keys().chain(t.regs.keys()).map(|k| &k.1).collect();
    for p in paths {
        let rs = s.regs.iter().filter(|(k, _)| &k.1 == p && al.contains(&k.0)).map(|(_, r)| r);
        let rt: Vec<_> =
            t.regs.iter().filter(|(k, _)| &k.1 == p && al.contains(&k.0)).map(|(_, r)| r).collect();
        for a in rs {
            for b in &rt {
                if a.stamp != b.stamp && !cov(&t.vv, &a.stamp) && !cov(&s.vv, &b.stamp) {
                    let lo = if a.stamp < b.stamp { a } else { b };
                    let arrays = matches!((&a.value, &b.value), (Value::Array(_), Value::Array(_)));
                    let kind = if arrays && !o.array_clash_as_clash() {
                        LossKind::ArrayClash
                    } else {
                        LossKind::Clash
                    };
                    new.insert(key(kind, p, lo.stamp), LossValue::Value(lo.value.clone()));
                }
            }
        }
    }

    // (1b) typeClash: a scalar or array register at p against a cell below p (a register with
    // a plain value, or a counter slot), concurrent by the version vectors. The loser is the
    // side the type rule hides: the older stamp (a newer register at p hides the cell below; a
    // newer cell below makes p a map, and the register at p loses). Any depth below counts.
    if !o.no_type_clash() {
        for (a_st, b_st) in [(&s, &t), (&t, &s)] {
            // b's cells in path order, so that those below a path are one run.
            let mut cells: Vec<(&Path, MStamp, LossValue)> = b_st
                .regs
                .iter()
                .filter(|(k, r)| al.contains(&k.0) && r.value.is_plain())
                .map(|(k, r)| (&k.1, r.stamp, LossValue::Value(r.value.clone())))
                .chain(
                    b_st.slots
                        .iter()
                        .filter(|(k, _)| al.contains(&k.0))
                        .map(|(k, v)| (&k.1, v.stamp, LossValue::Total(v.total))),
                )
                .collect();
            cells.sort_by(|x, y| x.0.cmp(y.0));
            for ((l, p), r) in &a_st.regs {
                if !al.contains(l) || !r.value.is_plain() || cov(&b_st.vv, &r.stamp) {
                    continue;
                }
                let start = cells.partition_point(|c| c.0 <= p);
                for (q, qst, qv) in cells[start..].iter().take_while(|c| c.0.is_below(p)) {
                    if *qst != r.stamp && !cov(&a_st.vv, qst) {
                        if r.stamp > *qst {
                            new.insert(key(LossKind::TypeClash, q, *qst), qv.clone());
                        } else {
                            new.insert(
                                key(LossKind::TypeClash, p, r.stamp),
                                LossValue::Value(r.value.clone()),
                            );
                        }
                    }
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
