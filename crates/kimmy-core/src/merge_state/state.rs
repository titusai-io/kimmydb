//! The merge state and its read side (the model's `State`).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use super::counter::Num;
use super::join::JoinOptions;
use super::types::{InvalidValue, LossKey, LossValue, MStamp, Path, Value, WriterId};
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
    pub order: Vec<Path>,
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
    pub seen: BTreeMap<ObsKey, Num>,
}

impl Gen {
    /// A generation started by the write `stamp`, with the totals it had seen.
    pub(crate) fn start(stamp: MStamp, vv: &Vv, seen: BTreeMap<ObsKey, Num>) -> Self {
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
    pub obs: BTreeMap<ObsKey, (MStamp, Num)>,
    pub generation: Option<Gen>,
    pub soi: bool,
}

impl Reg {
    /// A register. The value must be one a register can hold: a top-level `Document` is refused
    /// ([`Value::check_register`]).
    pub fn new(
        stamp: MStamp,
        value: Value,
        obs: BTreeMap<ObsKey, (MStamp, Num)>,
        generation: Option<Gen>,
        soi: bool,
    ) -> Result<Self, InvalidValue> {
        value.check_register()?;
        Ok(Reg { stamp, value, obs, generation, soi })
    }
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

/// The most writers that may contribute to one counter (design-ruled, Q9): a local `$inc` by a
/// writer that is not one of them yet is refused (a `400`) at this many. A state received with
/// more is kept and reported ([`MergeState::over_caps`]), never refused (Q8).
pub const MAX_CONTRIBUTORS: usize = 64;

/// The most writers (incarnations: a node and an epoch, so every restore adds one) a document
/// may mention (design-ruled, Q8; the compact form's writer table indexes them in one byte): a
/// local write by a writer the document does not mention yet is refused at this many. A state
/// received with more is kept and reported ([`MergeState::over_caps`]), never refused.
pub const MAX_WRITERS: usize = 256;

/// What the saturation of a merged counter cut off (Q11). Derived at read time, never stored.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Excess {
    /// An integer total past the integer bound: the signed amount past it (the visible value is
    /// the bound; the exact sum underneath is kept, so a later opposite increment brings the
    /// value back).
    Int(i128),
    /// A decimal sum that left the format at some step: the visible value is the largest finite
    /// `Decimal128` of the sign (`negative`) at that step. The amount is not reported: the sum
    /// is rounded at every step, and what lay beyond the format has no exact `Decimal128`.
    Decimal128 { negative: bool },
}

/// What a state holds past the caps (Q8: an over-cap result is stored and reported, and a local
/// write is refused only if it would add to what is over).
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct OverCaps {
    /// The number of writers the document mentions, when it is past [`MAX_WRITERS`].
    pub writers: Option<usize>,
    /// Each counter with more than [`MAX_CONTRIBUTORS`] contributors, and how many it has.
    pub counters: BTreeMap<Path, usize>,
}

/// A counter slot: one writer's running total in one generation.
#[derive(Clone, PartialEq, Debug)]
pub struct Slot {
    pub stamp: MStamp,
    pub total: Num,
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
    pub regs: BTreeMap<(MStamp, Path), Reg>,
    pub slots: BTreeMap<(MStamp, Path, WriterId, Option<MStamp>), Slot>,
    /// Newest non-numeric base per cell, with its writer's version vector. NOTE(model):
    /// vestigial in the model (kept for its V3M3 mutant, read nowhere else). Mutant memory,
    /// like [`Gen::vv`]: filled only in a test or `merge-mutants` build, joined and dropped
    /// like cells, but never encoded, hashed or compared.
    pub gens: BTreeMap<(MStamp, Path), (MStamp, Vv)>,
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
    pub(crate) fn note_base(&mut self, cell: (MStamp, Path), stamp: MStamp, vv: &Vv) {
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
    /// Every counter amount the state holds: slot totals, the `obs` entries of the registers,
    /// the `seen` entries of their generations, and the lost totals.
    pub(crate) fn totals(&self) -> impl Iterator<Item = &Num> {
        let regs = self.regs.values().flat_map(|r| {
            r.obs.values().map(|(_, n)| n).chain(r.generation.iter().flat_map(|g| g.seen.values()))
        });
        let lost = self.losses.values().filter_map(|v| match v {
            LossValue::Total(n) => Some(n),
            _ => None,
        });
        self.slots.values().map(|s| &s.total).chain(regs).chain(lost)
    }

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

    pub(crate) fn winner_in(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
    ) -> Option<&Reg> {
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
    pub fn winner(&self, p: &Path, o: &JoinOptions) -> Option<&Reg> {
        self.winner_in(p, &self.alive_set(o.retention), o)
    }

    /// V3M3: the generation of the winning base.
    pub fn gen_of(&self, p: &Path, o: &JoinOptions) -> Option<Cow<'_, Gen>> {
        let al = self.alive_set(o.retention);
        self.gen_in(p, &al, self.winner_in(p, &al, o), o)
    }

    /// The generation `win` (the winner of `p`) belongs to. Under the V3M3
    /// mutant switch, the v3 rule instead: the newest non-numeric base among
    /// the alive lives, even when it lost.
    fn gen_in<'a>(
        &'a self,
        p: &Path,
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
    pub fn counted(&self, p: &Path, o: &JoinOptions) -> BTreeMap<ObsKey, &Slot> {
        let al = self.alive_set(o.retention);
        let win = self.winner_in(p, &al, o);
        self.counted_in(p, &al, win, o)
    }

    fn counted_in(
        &self,
        p: &Path,
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
    pub fn hidden(&self, p: &Path, o: &JoinOptions) -> i128 {
        let al = self.alive_set(o.retention);
        self.hidden_in(p, &al, o)
    }

    fn hidden_in(&self, p: &Path, al: &BTreeSet<MStamp>, o: &JoinOptions) -> i128 {
        let win = self.winner_in(p, al, o);
        let Some(g) = self.gen_in(p, al, win, o) else {
            return 0;
        };
        // An integer figure: the amounts of the other types are not reported (provisional, see
        // the module docs of `counter`).
        self.slots
            .iter()
            .filter(|((l, q, _, generation), _)| {
                q == p && al.contains(l) && generation.is_none_or(|x| x < g.stamp)
            })
            .filter_map(|((l, _, w, generation), s)| {
                let seen = g.seen.get(&(*l, *w, *generation)).and_then(Num::int).unwrap_or(0);
                s.total.int().map(|t| t.saturating_sub(seen))
            })
            .fold(0i128, i128::saturating_add)
    }

    /// The hidden figure of every path that has a nonzero one.
    pub fn hidden_all(&self, o: &JoinOptions) -> BTreeMap<Path, i128> {
        let al = self.alive_set(o.retention);
        let paths: BTreeSet<&Path> =
            self.slots.keys().filter(|k| al.contains(&k.0)).map(|k| &k.1).collect();
        paths
            .into_iter()
            .filter_map(|p| {
                let h = self.hidden_in(p, &al, o);
                (h != 0).then(|| (p.clone(), h))
            })
            .collect()
    }

    /// The writers that contribute to the counter at `p`: those with a slot the read side counts
    /// (the winning base's generation and the newer ones), in the alive lives.
    pub fn contributors(&self, p: &Path, o: &JoinOptions) -> BTreeSet<WriterId> {
        self.counted(p, o).keys().map(|k| k.1).collect()
    }

    /// The caps the state is past, if any. Never an error: a merge can exceed a cap that each
    /// side kept, and the state is stored whole.
    pub fn over_caps(&self, o: &JoinOptions) -> OverCaps {
        let writers = self.writers().len();
        let al = self.alive_set(o.retention);
        let paths: BTreeSet<&Path> =
            self.slots.keys().filter(|k| al.contains(&k.0)).map(|k| &k.1).collect();
        OverCaps {
            writers: (writers > MAX_WRITERS).then_some(writers),
            counters: paths
                .into_iter()
                .filter_map(|p| {
                    let n = self.contributors(p, o).len();
                    (n > MAX_CONTRIBUTORS).then(|| (p.clone(), n))
                })
                .collect(),
        }
    }

    /// The visible value of one path (`None` when absent).
    pub fn value(&self, p: &Path, o: &JoinOptions) -> Option<Value> {
        let al = self.alive_set(o.retention);
        self.value_in(p, &al, o)
    }

    pub(crate) fn value_in(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
    ) -> Option<Value> {
        self.cell_in(p, al, o).map(|c| c.0)
    }

    /// The value of a path and what its saturation cut off, if anything.
    fn cell_in(
        &self,
        p: &Path,
        al: &BTreeSet<MStamp>,
        o: &JoinOptions,
    ) -> Option<(Value, Option<Excess>)> {
        let win = self.winner_in(p, al, o);
        // A base that is not a counter amount (a string, a double, a map marker, ...) is the
        // value: doubles are not counters.
        let base = match win.map(|w| &w.value) {
            Some(v) if !matches!(v, Value::Unset) => match Num::from_value(v) {
                Some(n) => Some(n),
                None => return Some((v.clone(), None)),
            },
            _ => None,
        };
        // What each slot gained since the base observed it. B1: the field is present when the
        // slot moved since the base saw it, even if its increments net to 0 (a counter back at
        // 0 is still there).
        let mut cut = None;
        let mut terms: Vec<(MStamp, ObsKey, Num)> = Vec::new();
        for (k, s) in self.counted_in(p, al, win, o) {
            let ob = win.and_then(|w| w.obs.get(&k));
            let d = ob.map_or(s.total, |x| {
                if o.no_decimal_saturation() {
                    s.total.sub(&x.1)
                } else {
                    s.total.sub_sat(&x.1, &mut cut)
                }
            });
            if !d.is_zero() || (!o.value_ignores_slot_stamp() && ob.is_none_or(|x| x.0 != s.stamp))
            {
                terms.push((s.stamp, k, d));
            }
        }
        // The canonical order (see `counter`): the base, then the slots by writer (node, epoch),
        // then life and generation.
        if o.sum_in_stamp_order() {
            terms.sort_by_key(|t| (t.0, t.1));
        } else {
            terms.sort_by_key(|t| (t.1.1, t.1.0, t.1.2));
        }
        if base.is_none() && terms.is_empty() {
            return None;
        }
        let kind = terms.iter().map(|t| t.2.kind()).chain(base.map(|b| b.kind())).max()?;
        let mut acc = base.map_or(Num::zero(kind), |b| b.to_kind(kind));
        for (_, _, d) in &terms {
            acc = if o.no_decimal_saturation() {
                acc.add(&d.to_kind(kind))
            } else {
                acc.add_sat(&d.to_kind(kind), &mut cut)
            };
        }
        // Q11: a merge that crosses either bound saturates: an integer at the integer bound, a
        // decimal at the largest finite value of its sign.
        let excess = match acc.int_excess(o.int_bound()) {
            0 => cut.map(|negative| Excess::Decimal128 { negative }),
            e => Some(Excess::Int(e)),
        };
        if o.no_saturation()
            && let Some(v) = acc.int()
        {
            return Some((Value::Int64(v as i64), excess));
        }
        Some((acc.into_value(o.int_bound(), o.int32_saturates()), excess))
    }

    /// What the saturation of a merged counter cut off, per path: the paths whose visible value
    /// stopped at a bound (Q11). A pure function of the state, like [`MergeState::hidden`]:
    /// nothing is stored, and no join law, hash or byte of either codec depends on it.
    pub fn excess(&self, o: &JoinOptions) -> BTreeMap<Path, Excess> {
        let al = self.alive_set(o.retention);
        let paths: BTreeSet<&Path> =
            self.slots.keys().filter(|k| al.contains(&k.0)).map(|k| &k.1).collect();
        paths
            .into_iter()
            .filter_map(|p| self.cell_in(p, &al, o)?.1.map(|e| (p.clone(), e)))
            .collect()
    }

    /// The visible document, or `None` when no life is alive: every present path, in path order
    /// (a map before the paths below it), with the value of a leaf and [`Value::Map`] at a map.
    /// A document with only flat fields has no map entry, so for it this is the map of its
    /// fields.
    ///
    /// The TYPE RULE (D4), at every depth: a path is a map when a map marker is its winning
    /// register, or when it has a visible child that is newer than its own scalar or array (a
    /// path with no cell of its own is a map too, if a child is visible); otherwise it is its
    /// register's value. Recency is decided per child: a scalar or array at `p` hides only the
    /// children that are older than it, and a newer child makes `p` a map showing just the
    /// newer children. A map marker never hides a child.
    pub fn visible(&self, o: &JoinOptions) -> Option<BTreeMap<Path, Value>> {
        let al = self.alive_set(o.retention);
        if al.is_empty() {
            return None;
        }
        let mut newest: BTreeMap<Path, MStamp> = BTreeMap::new();
        let mut note = |p: &Path, st: MStamp| {
            let e = newest.entry(p.clone()).or_insert(st);
            *e = (*e).max(st);
        };
        for ((l, p), r) in &self.regs {
            if al.contains(l) && !self.suppressed(l, r, &al, o) {
                note(p, r.stamp);
            }
        }
        for ((l, p, _, _), s) in &self.slots {
            if al.contains(l) {
                note(p, s.stamp);
            }
        }
        // every path with a cell, and every path above one
        let mut all: BTreeSet<Path> = BTreeSet::new();
        for p in newest.keys() {
            all.extend(p.ancestors());
            all.insert(p.clone());
        }
        let mut kids: BTreeMap<Path, Vec<Path>> = BTreeMap::new();
        let mut tops = Vec::new();
        for p in &all {
            match p.parent() {
                None => tops.push(p.clone()),
                Some(par) => kids.entry(par).or_default().push(p.clone()),
            }
        }
        let cx = Tree { st: self, al: &al, o, newest: &newest, kids: &kids };
        let mut out = BTreeMap::new();
        for t in &tops {
            if let Some(n) = cx.node(t) {
                n.emit(t, &mut out);
            }
        }
        Some(out)
    }
}

/// What the type rule decided for one path.
struct Node {
    /// The newest stamp among the cells of the visible subtree.
    newest: MStamp,
    shape: Shape,
}

enum Shape {
    Leaf(Value),
    Map(BTreeMap<Path, Node>),
}

impl Node {
    fn emit(&self, at: &Path, out: &mut BTreeMap<Path, Value>) {
        match &self.shape {
            Shape::Leaf(v) => {
                out.insert(at.clone(), v.clone());
            }
            Shape::Map(kids) => {
                out.insert(at.clone(), Value::Map);
                for (c, n) in kids {
                    n.emit(c, out);
                }
            }
        }
    }
}

/// The inputs of the type rule, for the recursion of [`MergeState::visible`].
struct Tree<'a> {
    st: &'a MergeState,
    al: &'a BTreeSet<MStamp>,
    o: &'a JoinOptions,
    newest: &'a BTreeMap<Path, MStamp>,
    kids: &'a BTreeMap<Path, Vec<Path>>,
}

impl Tree<'_> {
    /// The node of `q`, `None` when `q` is absent.
    fn node(&self, q: &Path) -> Option<Node> {
        let ks: BTreeMap<Path, Node> = self
            .kids
            .get(q)
            .into_iter()
            .flatten()
            .filter_map(|c| self.node(c).map(|n| (c.clone(), n)))
            .collect();
        let own = self.st.value_in(q, self.al, self.o);
        let own_n = self.newest.get(q).copied();
        let top = |v: &BTreeMap<Path, Node>| v.values().map(|n| n.newest).max();
        if ks.is_empty() {
            let v = own?;
            let shape = if v == Value::Map { Shape::Map(BTreeMap::new()) } else { Shape::Leaf(v) };
            return Some(Node { newest: own_n?, shape });
        }
        match own {
            // a scalar, number or array at q: only the children newer than its register show
            Some(v) if v != Value::Map => {
                let reg = self.st.winner_in(q, self.al, self.o);
                let rs = match reg {
                    Some(r) if r.value.is_plain() => r.stamp,
                    _ => own_n?,
                };
                let shown: BTreeMap<Path, Node> = if self.o.type_ignores_recency() {
                    BTreeMap::new()
                } else {
                    ks.into_iter().filter(|(_, n)| n.newest > rs).collect()
                };
                if shown.is_empty() {
                    return Some(Node { newest: own_n?, shape: Shape::Leaf(v) });
                }
                let newest = top(&shown).into_iter().chain(own_n).max()?;
                Some(Node { newest, shape: Shape::Map(shown) })
            }
            Some(_) if self.o.marker_hides() => {
                Some(Node { newest: own_n?, shape: Shape::Map(BTreeMap::new()) })
            }
            // a map marker, an unset or no cell at all: a map of every visible child
            _ => {
                let newest = top(&ks).into_iter().chain(own_n).max()?;
                Some(Node { newest, shape: Shape::Map(ks) })
            }
        }
    }
}
