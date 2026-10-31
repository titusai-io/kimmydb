//! The merge state and its read side (the model's `State`).

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

/// The generation a base starts or belongs to: the starter's stamp, the
/// starter's version vector, and the totals the starter had seen.
#[derive(Clone, PartialEq, Debug)]
pub struct Gen {
    pub stamp: MStamp,
    pub vv: Vv,
    pub seen: BTreeMap<ObsKey, i128>,
}

/// A register cell: higher stamp wins. `obs` is what the writer had counted,
/// `generation` the generation of this base, `soi` marks `$setOnInsert` creation cells.
#[derive(Clone, PartialEq, Debug)]
pub struct Reg {
    pub stamp: MStamp,
    pub value: Value,
    pub obs: BTreeMap<ObsKey, i128>,
    pub generation: Option<Gen>,
    pub soi: bool,
}

/// A counter slot: one writer's running total in one generation.
#[derive(Clone, PartialEq, Debug)]
pub struct Slot {
    pub stamp: MStamp,
    pub total: i128,
}

/// The replicated merge state. Grow-only components are joined by union, cells
/// by the higher stamp, the version vector by max; see [`super::join()`].
#[derive(Clone, PartialEq, Debug, Default)]
pub struct MergeState {
    pub lives: BTreeMap<MStamp, Life>,
    /// Delete stamp -> the version vector of the deleting writer.
    pub deletes: BTreeMap<MStamp, Vv>,
    /// Lives known dead; dropped once their stamp passes the horizon.
    pub dead: BTreeSet<MStamp>,
    pub regs: BTreeMap<(MStamp, String), Reg>,
    pub slots: BTreeMap<(MStamp, String, WriterId, Option<MStamp>), Slot>,
    /// Newest non-numeric base per cell. NOTE(model): vestigial in the model
    /// (kept for its V3M3 mutant, read nowhere); joined and dropped like cells
    /// and carried by `encode`, but absent from `canonical_bytes` like `canon()`.
    pub gens: BTreeMap<(MStamp, String), (MStamp, Vv)>,
    pub vv: Vv,
    pub losses: BTreeMap<LossKey, LossValue>,
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
    pub(crate) fn suppressed(&self, life: &MStamp, r: &Reg, al: &BTreeSet<MStamp>) -> bool {
        match self.lives.get(life) {
            Some(l) if l.kind == LifeKind::Upsert && r.soi => {
                al.iter().any(|a| self.lives.get(a).is_some_and(|v| v.kind == LifeKind::Insert))
            }
            _ => false,
        }
    }

    fn winner_in(&self, p: &str, al: &BTreeSet<MStamp>) -> Option<&Reg> {
        let mut best: Option<&Reg> = None;
        for ((l, q), r) in &self.regs {
            if q == p
                && al.contains(l)
                && !self.suppressed(l, r, al)
                && best.is_none_or(|b| r.stamp > b.stamp)
            {
                best = Some(r);
            }
        }
        best
    }

    /// The winning register of `p` among the alive lives.
    pub fn winner(&self, p: &str, o: &JoinOptions) -> Option<&Reg> {
        self.winner_in(p, &self.alive_set(o.retention))
    }

    /// V3M3: the generation of the winning base.
    pub fn gen_of(&self, p: &str, o: &JoinOptions) -> Option<&Gen> {
        self.winner(p, o).and_then(|w| w.generation.as_ref())
    }

    /// Slots of the winning base's generation and of NEWER generations (their
    /// starter lost: the increments are kept). Slots of OLDER generations are
    /// overwritten (hidden).
    pub fn counted(&self, p: &str, o: &JoinOptions) -> BTreeMap<ObsKey, &Slot> {
        let al = self.alive_set(o.retention);
        let win = self.winner_in(p, &al);
        self.counted_in(p, &al, win)
    }

    fn counted_in(
        &self,
        p: &str,
        al: &BTreeSet<MStamp>,
        win: Option<&Reg>,
    ) -> BTreeMap<ObsKey, &Slot> {
        let g = win.and_then(|w| w.generation.as_ref()).map(|g| g.stamp);
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
        self.hidden_in(p, &al)
    }

    fn hidden_in(&self, p: &str, al: &BTreeSet<MStamp>) -> i128 {
        let Some(g) = self.winner_in(p, al).and_then(|w| w.generation.as_ref()) else {
            return 0;
        };
        self.slots
            .iter()
            .filter(|((l, q, _, generation), _)| {
                q == p && al.contains(l) && generation.is_none_or(|x| x < g.stamp)
            })
            .map(|((l, _, w, generation), s)| {
                s.total - g.seen.get(&(*l, *w, *generation)).copied().unwrap_or(0)
            })
            .sum()
    }

    /// The hidden figure of every path that has a nonzero one.
    pub fn hidden_all(&self, o: &JoinOptions) -> BTreeMap<String, i128> {
        let al = self.alive_set(o.retention);
        let paths: BTreeSet<&String> =
            self.slots.keys().filter(|k| al.contains(&k.0)).map(|k| &k.1).collect();
        paths
            .into_iter()
            .filter_map(|p| {
                let h = self.hidden_in(p, &al);
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
        let win = self.winner_in(p, al);
        if let Some(w) = win
            && !w.value.is_numeric()
        {
            return Some(w.value.clone());
        }
        let base =
            win.and_then(|w| if let Value::Int(i) = w.value { Some(i128::from(i)) } else { None });
        let mut total = base.unwrap_or(0);
        let mut seen = base.is_some();
        for (k, s) in self.counted_in(p, al, win) {
            let d = s.total - win.and_then(|w| w.obs.get(&k).copied()).unwrap_or(0);
            if d != 0 {
                total += d;
                seen = true;
            }
        }
        // Q11: a merge that crosses either bound saturates.
        let b = i128::from(o.int_bound);
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
            if al.contains(l) && !self.suppressed(l, r, &al) {
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
