//! The merge state and its read side (the model's `State`).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use super::join::JoinOptions;
use super::types::{LossKey, LossValue, MStamp, Value, WriterId};
use crate::hlc::Hlc;

/// A version vector: the newest HLC seen per writer.
pub type Vv = BTreeMap<WriterId, Hlc>;

/// Key of an observed or counted slot: `(life, writer, generation)`.
pub type ObsKey = (MStamp, WriterId, Option<MStamp>);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LifeKind {
    Insert,
    Upsert,
}

/// A life of the document: one creation. `seen` is the set of delete stamps the
/// creator held; `order` the field order of the creating write.
#[derive(Clone, PartialEq, Debug)]
pub struct Life {
    pub seen: BTreeSet<MStamp>,
    pub order: Vec<String>,
    pub kind: LifeKind,
}

/// The generation a base starts or belongs to: the starter's stamp and the totals the starter
/// had seen.
///
/// `vv` (the starter's version vector) is memory for the V4M1 mutant of the test harness and
/// nothing else: it is kept only in a test or `merge-mutants` build, is empty in a production
/// build, and is not encoded, hashed or compared (two generations are equal when `stamp` and
/// `seen` are).
#[derive(Clone, Debug)]
pub struct Gen {
    pub stamp: MStamp,
    pub vv: Vv,
    pub seen: BTreeMap<ObsKey, i128>,
}

impl Gen {
    /// A generation started by the write `stamp`, with the totals it had seen.
    pub(crate) fn start(stamp: MStamp, vv: &Vv, seen: BTreeMap<ObsKey, i128>) -> Self {
        Gen { stamp, vv: mutant_vv(vv), seen }
    }
}

impl PartialEq for Gen {
    fn eq(&self, other: &Self) -> bool {
        self.stamp == other.stamp && self.seen == other.seen
    }
}

/// The version vector the mutants of the harness read (see [`Gen`] and
/// [`MergeState::gens`]): kept in a test or `merge-mutants` build, empty otherwise.
#[cfg(any(test, feature = "merge-mutants"))]
fn mutant_vv(vv: &Vv) -> Vv {
    vv.clone()
}
#[cfg(not(any(test, feature = "merge-mutants")))]
fn mutant_vv(_: &Vv) -> Vv {
    Vv::new()
}

/// A register cell: higher stamp wins. `obs` is what the writer had counted: for each
/// counted slot the slot's stamp and total at that time (the stamp tells a slot that has
/// not moved since from one that moved and came back to the same total). `generation` the generation of this base, `soi` marks `$setOnInsert` creation cells.
#[derive(Clone, PartialEq, Debug)]
pub struct Reg {
    pub stamp: MStamp,
    pub value: Value,
    pub obs: BTreeMap<ObsKey, (MStamp, i128)>,
    pub generation: Option<Gen>,
    pub soi: bool,
}

/// The largest magnitude of a counter total (a slot's, or a copy of one kept in a register's
/// `obs`, a generation's `seen` or a loss record): 2^119.
///
/// A THEORETICAL bound, there only so that the `i128` arithmetic of the read side is exact.
/// A visible value saturates at the Int64 bound, but the totals underneath are `i128` sums that
/// a writer's own `$inc`s can drive past it (a refused `$inc` is judged on the saturated value,
/// not on the sum). Each term the read side adds is a total minus the total a base observed, so
/// under 2^120 in magnitude, and 64 writers' terms sum to under 2^126, inside `i128`. Each
/// `$inc` moves a slot by less than 2^63, so an honest writer needs more than 2^56 maximal
/// increments to reach the bound: it is not reachable in practice, and no client sees a refusal
/// at it.
///
/// It is enforced where totals come from outside, in [`MergeState::decode`] (a slot, an `obs`
/// or `seen` entry, a loss record) and [`super::Writer::receive_loss`], which refuse a larger
/// one, so that a member never stores a state it cannot decode; and where they grow, in
/// [`super::Writer::write_inc`], which refuses an `$inc` that would pass it. The read side also
/// adds with saturation, as a backstop for a state assembled by hand through the public fields.
pub const MAX_TOTAL: i128 = 1 << 119;

// 64 writers' terms, each under twice the bound, sum inside i128.
const _: () = assert!(MAX_TOTAL.checked_mul(128).is_some());

/// A counter slot: one writer's running total in one generation.
#[derive(Clone, PartialEq, Debug)]
pub struct Slot {
    pub stamp: MStamp,
    pub total: i128,
}

/// The replicated merge state. Grow-only components are joined by union, cells
/// by the higher stamp, the version vector by max; see [`super::join()`].
#[derive(Clone, Debug, Default)]
pub struct MergeState {
    pub lives: BTreeMap<MStamp, Life>,
    /// Delete stamp -> the version vector of the deleting writer.
    pub deletes: BTreeMap<MStamp, Vv>,
    /// Lives known dead; dropped once their stamp passes the horizon.
    pub dead: BTreeSet<MStamp>,
    pub regs: BTreeMap<(MStamp, String), Reg>,
    pub slots: BTreeMap<(MStamp, String, WriterId, Option<MStamp>), Slot>,
    /// Newest non-numeric base per cell, with its writer's version vector. NOTE(model):
    /// vestigial in the model (kept for its V3M3 mutant, read nowhere else). Mutant memory,
    /// like [`Gen::vv`]: filled only in a test or `merge-mutants` build, joined and dropped
    /// like cells, but never encoded, hashed or compared.
    pub gens: BTreeMap<(MStamp, String), (MStamp, Vv)>,
    pub vv: Vv,
    pub losses: BTreeMap<LossKey, LossValue>,
}

impl PartialEq for MergeState {
    /// Equality of the replicated state: everything that is encoded. `gens` (mutant memory)
    /// is left out.
    fn eq(&self, other: &Self) -> bool {
        let MergeState { lives, deletes, dead, regs, slots, gens: _, vv, losses } = self;
        *lives == other.lives
            && *deletes == other.deletes
            && *dead == other.dead
            && *regs == other.regs
            && *slots == other.slots
            && *vv == other.vv
            && *losses == other.losses
    }
}

impl MergeState {
    /// Record the newest non-numeric base of a cell for the V3M3 mutant. The callers are
    /// compiled only in a test or `merge-mutants` build.
    #[cfg(any(test, feature = "merge-mutants"))]
    pub(crate) fn note_base(&mut self, cell: (MStamp, String), stamp: MStamp, vv: &Vv) {
        self.gens.insert(cell, (stamp, vv.clone()));
    }
}

/// The model's `cov`: is `stamp` covered by `vv`?
pub(crate) fn cov(vv: &Vv, stamp: &MStamp) -> bool {
    vv.get(&stamp.writer).copied().unwrap_or(Hlc::ZERO) >= stamp.hlc
}

/// `h - ret`, clamped so that a "negative" value is below every stamp.
pub(crate) fn sub_ret(h: Hlc, ret: u64) -> Hlc {
    if h.wall_ms < ret { Hlc::ZERO } else { Hlc::new(h.wall_ms - ret, h.counter) }
}

impl MergeState {
    pub(crate) fn max_vv(&self) -> Hlc {
        self.vv.values().copied().max().unwrap_or(Hlc::ZERO)
    }

    /// `H(state) = max(vv) - R`, a pure function of the state.
    pub fn horizon(&self, o: &JoinOptions) -> Hlc {
        sub_ret(self.max_vv(), o.retention)
    }

    pub(crate) fn alive_set(&self, ret: u64) -> BTreeSet<MStamp> {
        self.lives
            .iter()
            .filter(|(l, v)| {
                !self.dead.contains(l)
                    && self
                        .deletes
                        .keys()
                        .all(|d| d.hlc < sub_ret(l.hlc, ret) || v.seen.contains(d))
            })
            .map(|(l, _)| *l)
            .collect()
    }

    /// The alive lives, in stamp order. A life is alive iff it is not dead and
    /// every delete (inside retention of the life) is in its `seen`.
    pub fn alive(&self, o: &JoinOptions) -> Vec<MStamp> {
        self.alive_set(o.retention).into_iter().collect()
    }

    /// V6m2: the creation cells of an upsert-created life are dropped when an
    /// explicit insert is alive (any insert, re-inserts included).
    pub(crate) fn suppressed(
        &self,
        life: &MStamp,
        r: &Reg,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
    ) -> bool {
        match self.lives.get(life) {
            Some(l) if l.kind == LifeKind::Upsert && r.soi && o.suppress_soi() => {
                al.iter().any(|a| self.lives.get(a).is_some_and(|v| v.kind == LifeKind::Insert))
            }
            _ => false,
        }
    }

    fn winner_in(&self, p: &str, al: &BTreeSet<MStamp>, o: &JoinOptions) -> Option<&Reg> {
        let mut best: Option<&Reg> = None;
        for ((l, q), r) in &self.regs {
            if q == p
                && al.contains(l)
                && !self.suppressed(l, r, al, o)
                && best.is_none_or(|b| r.stamp > b.stamp)
            {
                best = Some(r);
            }
        }
        best
    }

    /// The winning register of `p` among the alive lives.
    pub fn winner(&self, p: &str, o: &JoinOptions) -> Option<&Reg> {
        self.winner_in(p, &self.alive_set(o.retention), o)
    }

    /// V3M3: the generation of the winning base.
    pub fn gen_of(&self, p: &str, o: &JoinOptions) -> Option<Cow<'_, Gen>> {
        let al = self.alive_set(o.retention);
        self.gen_in(p, &al, self.winner_in(p, &al, o), o)
    }

    /// The generation `win` (the winner of `p`) belongs to. Under the V3M3
    /// mutant switch, the v3 rule instead: the newest non-numeric base among
    /// the alive lives, even when it lost.
    fn gen_in<'a>(
        &'a self,
        p: &str,
        al: &BTreeSet<MStamp>,
        win: Option<&'a Reg>,
        o: &JoinOptions,
    ) -> Option<Cow<'a, Gen>> {
        if !o.v3_generation() {
            return win.and_then(|w| w.generation.as_ref()).map(Cow::Borrowed);
        }
        self.gens
            .iter()
            .filter(|((l, q), _)| q == p && al.contains(l))
            .map(|(_, g)| g)
            .max_by_key(|g| g.0)
            .map(|(st, vv)| Cow::Owned(Gen::start(*st, vv, BTreeMap::new())))
    }

    /// Slots of the winning base's generation and of NEWER generations (their
    /// starter lost: the increments are kept). Slots of OLDER generations are
    /// overwritten (hidden).
    pub fn counted(&self, p: &str, o: &JoinOptions) -> BTreeMap<ObsKey, &Slot> {
        let al = self.alive_set(o.retention);
        let win = self.winner_in(p, &al, o);
        self.counted_in(p, &al, win, o)
    }

    fn counted_in(
        &self,
        p: &str,
        al: &BTreeSet<MStamp>,
        win: Option<&Reg>,
        o: &JoinOptions,
    ) -> BTreeMap<ObsKey, &Slot> {
        let g = self.gen_in(p, al, win, o).map(|g| g.stamp);
        self.slots
            .iter()
            .filter(|((l, q, _, generation), _)| {
                q == p
                    && al.contains(l)
                    && (*generation == g
                        || (generation.is_some() && (g.is_none() || *generation > g)))
            })
            .map(|((l, _, w, generation), s)| ((*l, *w, *generation), s))
            .collect()
    }

    /// The increments the winning generation overwrote, as state derived from
    /// the current merge: the NET SUM (signed), over the slots of older
    /// generations, of the part the starter of the winning generation had not
    /// seen (slot total minus the total the starter saw).
    pub fn hidden(&self, p: &str, o: &JoinOptions) -> i128 {
        let al = self.alive_set(o.retention);
        self.hidden_in(p, &al, o)
    }

    fn hidden_in(&self, p: &str, al: &BTreeSet<MStamp>, o: &JoinOptions) -> i128 {
        let win = self.winner_in(p, al, o);
        let Some(g) = self.gen_in(p, al, win, o) else {
            return 0;
        };
        self.slots
            .iter()
            .filter(|((l, q, _, generation), _)| {
                q == p && al.contains(l) && generation.is_none_or(|x| x < g.stamp)
            })
            .map(|((l, _, w, generation), s)| {
                s.total.saturating_sub(g.seen.get(&(*l, *w, *generation)).copied().unwrap_or(0))
            })
            .fold(0i128, i128::saturating_add)
    }

    /// The hidden figure of every path that has a nonzero one.
    pub fn hidden_all(&self, o: &JoinOptions) -> BTreeMap<String, i128> {
        let al = self.alive_set(o.retention);
        let paths: BTreeSet<&String> =
            self.slots.keys().filter(|k| al.contains(&k.0)).map(|k| &k.1).collect();
        paths
            .into_iter()
            .filter_map(|p| {
                let h = self.hidden_in(p, &al, o);
                (h != 0).then(|| (p.clone(), h))
            })
            .collect()
    }

    /// The visible value of one path (`None` when absent).
    pub fn value(&self, p: &str, o: &JoinOptions) -> Option<Value> {
        let al = self.alive_set(o.retention);
        self.value_in(p, &al, o)
    }

    fn value_in(&self, p: &str, al: &BTreeSet<MStamp>, o: &JoinOptions) -> Option<Value> {
        let win = self.winner_in(p, al, o);
        if let Some(w) = win
            && !w.value.is_numeric()
        {
            return Some(w.value.clone());
        }
        let base =
            win.and_then(|w| if let Value::Int(i) = w.value { Some(i128::from(i)) } else { None });
        let mut total = base.unwrap_or(0);
        let mut seen = base.is_some();
        for (k, s) in self.counted_in(p, al, win, o) {
            let ob = win.and_then(|w| w.obs.get(&k));
            let d = s.total.saturating_sub(ob.map_or(0, |x| x.1));
            // B1: the field is present when the slot moved since the base saw it,
            // even if its increments net to 0 (a counter back at 0 is still there).
            if d != 0 || (!o.value_ignores_slot_stamp() && ob.is_none_or(|x| x.0 != s.stamp)) {
                total = total.saturating_add(d);
                seen = true;
            }
        }
        // Q11: a merge that crosses either bound saturates.
        let b = i128::from(o.int_bound());
        seen.then(|| Value::Int(total.clamp(-b - 1, b) as i64))
    }

    /// The visible document, or `None` when no life is alive.
    pub fn visible(&self, o: &JoinOptions) -> Option<BTreeMap<String, Value>> {
        let al = self.alive_set(o.retention);
        if al.is_empty() {
            return None;
        }
        let mut paths: BTreeSet<&str> = BTreeSet::new();
        for ((l, p), r) in &self.regs {
            if al.contains(l) && !self.suppressed(l, r, &al, o) {
                paths.insert(p);
            }
        }
        for (l, p, _, _) in self.slots.keys() {
            if al.contains(l) {
                paths.insert(p);
            }
        }
        Some(
            paths
                .into_iter()
                .filter_map(|p| {
                    self.value_in(p, &al, o)
                        .filter(|v| *v != Value::Null)
                        .map(|v| (p.to_string(), v))
                })
                .collect(),
        )
    }
}
