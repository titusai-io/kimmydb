//! A writer incarnation: local write operations over its own [`MergeState`]
//! (the model's `Member` with the `Node.finish` normalisation).

use std::collections::{BTreeMap, BTreeSet};

use super::counter::{self, Inc, Kind, Num};
use super::join::{JoinOptions, join, join_into};
use super::ord::{OrdAt, OrdMode, OrderedValue, ord_events};
use super::state::{
    Gen, Life, LifeKind, MAX_CONTRIBUTORS, MAX_TOTAL, MAX_WRITERS, MergeState, ObsKey, Reg, Slot,
};
use super::types::{InvalidPath, LossKey, LossValue, MStamp, Path, Value, WriterId};
use crate::hlc::Hlc;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WriteKind {
    Insert,
    Inc,
    Reg,
    Delete,
}

/// What the model's `finish(..., meta)` logs about a write, for a harness that
/// builds the causal log.
#[derive(Clone, PartialEq, Debug)]
pub struct WriteRecord {
    pub kind: WriteKind,
    /// The life written into (the new life for an insert or an upserting inc).
    pub life: Option<MStamp>,
    pub paths: Vec<Path>,
    /// Register values written (`Insert`, `Reg`).
    pub vals: BTreeMap<Path, Value>,
    /// The increment (`Inc`), when it is an integer that fits an `i64`.
    pub n: Option<i64>,
    /// The increment (`Inc`) with its type: an `Int32`, `Int64`, `Double` or `Decimal128`.
    pub amount: Option<Num>,
    /// The generation an increment belongs to (`Inc`).
    pub generation: Option<MStamp>,
    /// An upsert that created the life (the `Inc` of `upsert_inc`, the creation write of `upsert_soi`).
    pub insert: bool,
    /// The creation write of a `$setOnInsert` upsert: a life creation that is not an explicit insert.
    pub upsert: bool,
    /// The `$setOnInsert` paths of an upsert.
    pub soi: Vec<Path>,
}

impl WriteRecord {
    fn new(kind: WriteKind, life: Option<MStamp>, paths: Vec<Path>) -> Self {
        Self {
            kind,
            life,
            paths,
            vals: BTreeMap::new(),
            n: None,
            amount: None,
            generation: None,
            insert: false,
            upsert: false,
            soi: Vec::new(),
        }
    }
}

/// The result of an accepted local write.
#[derive(Clone, PartialEq, Debug)]
pub struct Written {
    pub stamp: MStamp,
    pub record: WriteRecord,
}

/// Why an externally minted stamp was refused.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum StampError {
    /// The stamp names another node or epoch than the writer's.
    #[error("merge state: the stamp is for writer {stamp:?}, not {writer:?}")]
    OtherWriter { writer: WriterId, stamp: WriterId },
    /// The stamp is not above the newest HLC the writer has used or seen.
    #[error("merge state: the stamp {stamp:?} is not above {top:?}")]
    NotAbove { top: Hlc, stamp: Hlc },
}

/// Where the stamps of one write come from.
enum Source<'a> {
    /// The writer's own clock, by `1 + skew`.
    Skew(u64),
    /// Minted by the caller.
    Given(&'a mut dyn FnMut() -> MStamp),
}

/// The stamp source of one write, and the first stamp it refused.
struct Mint<'a> {
    source: Source<'a>,
    error: Option<StampError>,
}

/// The register effects of one write: the cells to set, in the order they were named.
#[derive(Default)]
struct Eff {
    order: Vec<Path>,
    vals: BTreeMap<Path, Value>,
}

impl Eff {
    fn put(&mut self, p: Path, v: Value) {
        if self.vals.insert(p.clone(), v).is_none() {
            self.order.push(p);
        }
    }
    fn len(&self) -> usize {
        self.order.len()
    }
}

/// The cells a value stands for at `at`: a `Document` is a map marker and the cells of its
/// fields (the writer's expansion of a sub-document); `Unset` is none (no such field); anything
/// else is its one cell.
fn expand(at: &Path, v: &Value, out: &mut Vec<(Path, Value)>) -> Result<(), InvalidPath> {
    if *v == Value::Unset {
        // an Unset field is no field: no register, so no stamp for it to win a race with (the model's absent field)
        return Ok(());
    }
    if let Value::Document(fields) = v {
        out.push((at.clone(), Value::Map));
        for (name, x) in fields {
            expand(&at.child(name)?, x, out)?;
        }
    } else {
        out.push((at.clone(), v.clone()));
    }
    Ok(())
}

/// The fields of a whole document (insert, replace, creation of an upsert) as cells: each name
/// is a one-segment path, distinct, and each value legal input. `None` refuses the document.
fn expand_document(fields: &[(Path, Value)]) -> Option<Vec<(Path, Value)>> {
    let mut out = Vec::new();
    let mut named = BTreeSet::new();
    for (p, v) in fields {
        if p.segments().len() != 1 || !named.insert(p) {
            return None;
        }
        v.check_input().ok()?;
        expand(p, v, &mut out).ok()?;
    }
    Some(out)
}

/// The visible entries strictly below `p`, in path order.
fn below<'a>(
    vis: &'a BTreeMap<Path, Value>,
    p: &Path,
) -> impl Iterator<Item = (&'a Path, &'a Value)> {
    let p = p.clone();
    vis.range(p.clone()..).skip(1).take_while(move |(k, _)| k.is_below(&p))
}

/// The order items of one write, in write order: the model's `items`, `(path, mode)`.
type Items = Vec<(OrdAt, OrdMode)>;

/// The model's `_unset_subtree`: unset what the writer sees at `p` and below it, and name each
/// of them to the order state as an unset.
fn unset_subtree(
    vis: &BTreeMap<Path, Value>,
    p: &Path,
    eff: &mut Eff,
    items: &mut Items,
    o: &JoinOptions,
) {
    for (q, _) in below(vis, p) {
        if o.unset_shallow() && q.segments().len() > p.segments().len() + 1 {
            continue;
        }
        eff.put(q.clone(), Value::Unset);
        items.push((OrdAt::Field(q.clone()), OrdMode::Unset));
    }
    eff.put(p.clone(), Value::Unset);
    items.push((OrdAt::Field(p.clone()), OrdMode::Unset));
}

/// The model's `ancestors_of`: a write below a map is a write to the map, so each proper
/// ancestor of the path is named as a plain write (once per update: `seen`). (The marker the
/// model writes at an ancestor the writer does not see is the implicit marker, item O4: not
/// written here.)
fn ancestor_items(p: &Path, seen: &mut BTreeSet<Path>, items: &mut Items, o: &JoinOptions) {
    for a in p.ancestors() {
        if seen.insert(a.clone()) && !o.ord_no_ancestor() {
            items.push((OrdAt::Field(a), OrdMode::Write));
        }
    }
}

/// The model's `ordered_same` of a value the writer sees and the new value of a field: the same
/// field order at every level and the same leaves.
fn same_ordered(cur: &OrderedValue, new: &Value) -> bool {
    match (cur, new) {
        (OrderedValue::Doc(c), Value::Document(n)) => same_fields(c, n),
        // (a leaf is never equal to a document, so a sub-document where a leaf stands is a change)
        (OrderedValue::Leaf(c), n) => Value::same(Some(c), n),
        _ => false,
    }
}

/// [`same_ordered`] for the fields of a document. A field of the new document whose value is
/// [`Value::Unset`] is not a field (the writer's spelling of "no such field").
fn same_fields(cur: &[(String, OrderedValue)], new: &[(String, Value)]) -> bool {
    let mut new = new.iter().filter(|(_, v)| *v != Value::Unset);
    cur.iter().all(|(a, c)| new.next().is_some_and(|(b, n)| a == b && same_ordered(c, n)))
        && new.next().is_none()
}

/// The model's `getp` refusal (D-2): is some proper ancestor of `p` visible as a scalar or an
/// array? The reference cannot create a field below one, so the write is refused.
fn below_a_scalar(vis: &BTreeMap<Path, Value>, p: &Path) -> bool {
    p.ancestors().any(|a| vis.get(&a).is_some_and(|v| *v != Value::Map))
}

/// What an `$inc` does, decided before it is stamped.
enum IncPlan {
    /// A double base whose new value is the stored representation byte for byte: nothing to write.
    Nothing,
    /// A double base: a register write of the computed value.
    Register(Value),
    /// A counter slot of the writer, with its new total.
    Slot(Box<SlotPlan>),
}

/// The counter slot an `$inc` writes: in `life`, at `key`, to `total`.
struct SlotPlan {
    life: MStamp,
    key: (MStamp, Path, WriterId, Option<MStamp>),
    total: Num,
    op: Num,
    generation: Option<MStamp>,
}

/// The outcome of absorbing a state.
#[derive(Clone, PartialEq, Debug)]
pub struct Received {
    /// The canonical state changed (the model's `canon()` comparison).
    pub changed: bool,
    /// Loss records that are new to this writer, in key order.
    pub fresh: Vec<LossKey>,
}

/// One writer incarnation. Restoring a node is modelled by building a new
/// `Writer` with a new [`WriterId`] around the restored state.
#[derive(Clone, Debug)]
pub struct Writer {
    pub id: WriterId,
    pub clock: Hlc,
    pub state: MergeState,
    pub opts: JoinOptions,
}

impl Writer {
    pub fn new(id: WriterId, opts: JoinOptions) -> Self {
        Self { id, clock: Hlc::ZERO, state: MergeState::default(), opts }
    }

    /// A writer around an existing state (a restore under a new identity).
    pub fn from_state(id: WriterId, state: MergeState, opts: JoinOptions) -> Self {
        let clock = state.vv.values().copied().max().unwrap_or(Hlc::ZERO);
        Self { id, clock, state, opts }
    }

    /// The newest HLC this writer has used or seen: its clock and every version-vector entry.
    fn top(&self) -> Hlc {
        self.state.vv.values().copied().chain([self.clock]).max().unwrap_or(Hlc::ZERO)
    }

    /// The highest HLC in the writer's version vector: what an unset it is about to make will
    /// remember as `prev` (the model's `Member.known`, taken before the stamp). The clock is not
    /// part of it: a clock that ran ahead of the vector is not something the writer has seen.
    fn known(&self) -> Hlc {
        self.state.vv.values().copied().max().unwrap_or(Hlc::ZERO)
    }

    /// Feed the order state with the write `st` into `life`: the events of its items, applied to
    /// the writer's own state (the join of what it received and its own earlier writes).
    fn feed(&mut self, life: MStamp, st: MStamp, known: Hlc, items: &Items) {
        let events = ord_events(items, &self.opts);
        self.state.ord_apply(life, st, known, &events, &self.opts);
    }

    /// The next stamp: strictly above the clock and every version-vector entry,
    /// by `1 + skew` in the wall component.
    pub fn tick(&mut self, skew: u64) -> MStamp {
        self.clock = Hlc::new(self.top().wall_ms.saturating_add(1).saturating_add(skew), 0);
        MStamp { hlc: self.clock, writer: self.id }
    }

    /// Take a stamp minted outside (the engine's `next_stamp`) and move the clock to it. It
    /// must be this writer's own (same node and epoch) and strictly above everything the
    /// writer has used or seen: its clock and every version-vector entry, which is what
    /// [`Writer::tick`] guarantees by construction. A stamp that fails either check is refused
    /// and nothing moves.
    pub fn tick_to(&mut self, stamp: MStamp) -> Result<MStamp, StampError> {
        if stamp.writer != self.id {
            return Err(StampError::OtherWriter { writer: self.id, stamp: stamp.writer });
        }
        let top = self.top();
        if stamp.hlc <= top {
            return Err(StampError::NotAbove { top, stamp: stamp.hlc });
        }
        self.clock = stamp.hlc;
        Ok(stamp)
    }

    /// Mint the stamp of one write from `m`. A refused stamp is kept in `m` and reported as
    /// `None`, which the write bodies treat as a refused write (before anything is written).
    fn mint(&mut self, m: &mut Mint<'_>) -> Option<MStamp> {
        // The writer cap: a writer the document does not mention yet is refused at the cap (a
        // write adds the writer to the version vector). Every writer of the version vector is
        // mentioned, so the walk over the whole state is only needed for a writer that has not
        // written yet.
        if !self.opts.no_writer_cap() && !self.state.vv.contains_key(&self.id) {
            #[cfg(test)]
            super::work::bump(super::work::Counter::WriterScans, 1);
            let ws = self.state.writers();
            if ws.len() >= MAX_WRITERS && !ws.contains(&self.id) {
                return None;
            }
        }
        let r = match &mut m.source {
            Source::Skew(skew) => Ok(self.tick(*skew)),
            Source::Given(f) => {
                let st = f();
                self.tick_to(st)
            }
        };
        r.map_err(|e| m.error = Some(e)).ok()
    }

    /// Run a write body that ticks the writer's own clock.
    fn ticking<T>(
        &mut self,
        skew: u64,
        f: impl FnOnce(&mut Self, &mut Mint<'_>) -> Option<T>,
    ) -> Option<T> {
        let mut m = Mint { source: Source::Skew(skew), error: None };
        f(self, &mut m)
    }

    /// Run a write body that takes its stamps from `stamp`. A stamp that fails
    /// [`Writer::tick_to`] ends the write with the error. Every body checks its stamp before it
    /// writes anything, so a refused write has changed nothing (the one body that needs two
    /// stamps, [`Writer::upsert_soi_stamped`], undoes its first write itself).
    fn stamped<T>(
        &mut self,
        stamp: &mut dyn FnMut() -> MStamp,
        f: impl FnOnce(&mut Self, &mut Mint<'_>) -> Option<T>,
    ) -> Result<Option<T>, StampError> {
        let mut m = Mint { source: Source::Given(stamp), error: None };
        let r = f(self, &mut m);
        m.error.map_or(Ok(r), Err)
    }

    pub fn visible(&self) -> Option<BTreeMap<Path, Value>> {
        self.state.visible(&self.opts)
    }
    pub fn hidden_all(&self) -> BTreeMap<Path, i128> {
        self.state.hidden_all(&self.opts)
    }
    pub fn alive(&self) -> Vec<MStamp> {
        self.state.alive(&self.opts)
    }
    pub fn horizon(&self) -> Hlc {
        self.state.horizon(&self.opts)
    }

    /// V5m1: every slot total of `p` (any generation, `al` the alive lives) as this
    /// writer sees it, kept with a generation it starts.
    fn seen(&self, p: &Path, al: &BTreeSet<MStamp>) -> BTreeMap<ObsKey, Num> {
        self.state
            .slots_at(p, al)
            .into_iter()
            .map(|((l, _, w, g), v)| ((*l, *w, *g), v.total))
            .collect()
    }

    /// Set the register of `p` in the life `l` (the newest of the alive lives `al`), stamp `st`.
    /// What the write observed (the winning base, its generation, the counted slots) is read from
    /// the cells of `p` alone, so cells already set for other paths of the same write change
    /// nothing of it.
    fn setreg(&mut self, p: &Path, v: Value, st: MStamp, al: &BTreeSet<MStamp>) {
        let l = *al.last().expect("a register write needs an alive life");
        let win = self.state.winner_in(p, al, &self.opts);
        let generation = if v.is_numeric() {
            self.state.gen_in(p, al, win, &self.opts).map(std::borrow::Cow::into_owned)
        } else {
            Some(Gen::start(st, &self.state.vv, self.seen(p, al)))
        };
        let obs = self
            .state
            .counted_in(p, al, win, &self.opts)
            .into_iter()
            .map(|(k, v)| (k, (v.stamp, v.total)))
            .collect();
        #[cfg(any(test, feature = "merge-mutants"))]
        if !v.is_numeric() {
            let vv = self.state.vv.clone();
            self.state.note_base((l, p.clone()), st, &vv);
        }
        self.state
            .regs
            .insert((l, p.clone()), Reg { stamp: st, value: v, obs, generation, soi: false });
    }

    /// The writer's own state, normalised as every receiver's join normalises
    /// it (dead lives listed, their cells dropped). The state hash of the writer
    /// must equal every receiver's.
    fn finish(&mut self, st: MStamp, record: WriteRecord) -> Written {
        self.state.vv.insert(st.writer, st.hlc);
        if self.opts.normalize_writes() {
            // The state is moved out before the join (no copy of it), so a panic in `join_into`
            // would leave this writer holding an empty state instead of the written one: the
            // join must stay panic-free (today it indexes only keys drawn from the state itself,
            // and `sub_ret` clamps).
            let (s, _) =
                join_into(std::mem::take(&mut self.state), &MergeState::default(), &self.opts);
            self.state = s;
        }
        Written { stamp: st, record }
    }

    /// An explicit insert; refused (`None`) while a life is alive. Each field is a one-segment
    /// path (a sub-document is a `Value::Document` value, expanded into a map marker and its
    /// leaves); a repeated name or a value that is not legal input refuses the insert.
    pub fn insert(&mut self, fields: &[(Path, Value)], skew: u64) -> Option<Written> {
        self.ticking(skew, |w, m| w.insert_m(fields, m))
    }

    /// [`Writer::insert`] with the stamp taken from `stamp` instead of the writer's own clock:
    /// see [`Writer::tick_to`] for what a stamp must satisfy. `stamp` is called once for each stamp the
    /// write needs and never for a write that is refused or changes nothing; a stamp that
    /// fails the check is an `Err` and nothing is written.
    pub fn insert_stamped(
        &mut self,
        fields: &[(Path, Value)],
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<Written>, StampError> {
        self.stamped(stamp, |w, m| w.insert_m(fields, m))
    }

    /// V6m2/V7m2/V8: an upsert that creates the document with `$setOnInsert`
    /// `fields`, applies `$set` `sets` and `$inc` `inc_path` by `n`; the three path
    /// lists must be disjoint (a conflicting update is refused, `None`). It is two
    /// sequential local writes by one writer: the creation (the life and its
    /// cells, the `$setOnInsert` cells tagged) and the increment, each with its
    /// own stamp. Only the tagged cells can be dropped.
    pub fn upsert_soi(
        &mut self,
        fields: &[(Path, Value)],
        inc_path: &Path,
        n: i64,
        sets: &[(Path, Value)],
        skew: u64,
    ) -> Option<(Written, Written)> {
        self.ticking(skew, |w, m| w.upsert_soi_m(fields, inc_path, n, sets, m))
    }

    /// [`Writer::upsert_soi`] with the stamp taken from `stamp` instead of the writer's own clock:
    /// see [`Writer::tick_to`] for what a stamp must satisfy. `stamp` is called once for each stamp the
    /// write needs and never for a write that is refused or changes nothing; a stamp that
    /// fails the check is an `Err` and nothing is written.
    pub fn upsert_soi_stamped(
        &mut self,
        fields: &[(Path, Value)],
        inc_path: &Path,
        n: i64,
        sets: &[(Path, Value)],
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<(Written, Written)>, StampError> {
        // Two writes, two stamps: if the second is refused the first is taken back.
        let before = (self.state.clone(), self.clock);
        let r = self.stamped(stamp, |w, m| w.upsert_soi_m(fields, inc_path, n, sets, m));
        if r.is_err() {
            (self.state, self.clock) = before;
        }
        r
    }

    /// The creation write of a `$setOnInsert` upsert: the new life (kind upsert),
    /// the `$setOnInsert` cells tagged and the `$set` cells untagged, no slot.
    /// `inc_path` is the path of the `$inc` that [`Writer::upsert_inc_on`] applies next;
    /// the whole update is validated here, before anything is written or the clock moves:
    /// the `$setOnInsert`, `$set` and `$inc` paths must be pairwise distinct (a conflicting
    /// update is refused, `None`). Refused too while a life is alive.
    pub fn upsert_soi_create(
        &mut self,
        fields: &[(Path, Value)],
        inc_path: &Path,
        sets: &[(Path, Value)],
        skew: u64,
    ) -> Option<Written> {
        self.ticking(skew, |w, m| w.upsert_soi_create_m(fields, inc_path, sets, m))
    }

    /// [`Writer::upsert_soi_create`] with the stamp taken from `stamp` instead of the writer's own clock:
    /// see [`Writer::tick_to`] for what a stamp must satisfy. `stamp` is called once for each stamp the
    /// write needs and never for a write that is refused or changes nothing; a stamp that
    /// fails the check is an `Err` and nothing is written.
    pub fn upsert_soi_create_stamped(
        &mut self,
        fields: &[(Path, Value)],
        inc_path: &Path,
        sets: &[(Path, Value)],
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<Written>, StampError> {
        self.stamped(stamp, |w, m| w.upsert_soi_create_m(fields, inc_path, sets, m))
    }

    /// The increment write of an upsert on the life `life` (the stamp of its
    /// creation write): a slot of `path` for this writer, with the write's own
    /// stamp. Refused (`None`) unless `life` is alive and its creation did not write `path`.
    pub fn upsert_inc_on(
        &mut self,
        life: MStamp,
        path: &Path,
        n: i64,
        skew: u64,
    ) -> Option<Written> {
        self.ticking(skew, |w, m| w.upsert_inc_on_m(life, path, n, m))
    }

    /// [`Writer::upsert_inc_on`] with the stamp taken from `stamp` instead of the writer's own clock:
    /// see [`Writer::tick_to`] for what a stamp must satisfy. `stamp` is called once for each stamp the
    /// write needs and never for a write that is refused or changes nothing; a stamp that
    /// fails the check is an `Err` and nothing is written.
    pub fn upsert_inc_on_stamped(
        &mut self,
        life: MStamp,
        path: &Path,
        n: i64,
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<Written>, StampError> {
        self.stamped(stamp, |w, m| w.upsert_inc_on_m(life, path, n, m))
    }

    /// An upserting `$inc` that creates the document; refused while a life is alive.
    pub fn upsert_inc(&mut self, path: &Path, n: i64, skew: u64) -> Option<Written> {
        self.ticking(skew, |w, m| w.upsert_inc_m(path, n, m))
    }

    /// [`Writer::upsert_inc`] with the stamp taken from `stamp` instead of the writer's own clock:
    /// see [`Writer::tick_to`] for what a stamp must satisfy. `stamp` is called once for each stamp the
    /// write needs and never for a write that is refused or changes nothing; a stamp that
    /// fails the check is an `Err` and nothing is written.
    pub fn upsert_inc_stamped(
        &mut self,
        path: &Path,
        n: i64,
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<Written>, StampError> {
        self.stamped(stamp, |w, m| w.upsert_inc_m(path, n, m))
    }

    /// `$set path = v`; `None` when no life is alive, the value is unchanged (a no-op is a write
    /// that would leave the stored representation byte-identical) or the write is refused: below a
    /// scalar or an array, or with a value that is not legal input ([`Value::check_input`]). A
    /// `Value::Document` is a sub-document: only the leaves that changed are written.
    pub fn write_set(&mut self, path: &Path, v: Value, skew: u64) -> Option<Written> {
        self.ticking(skew, |w, m| w.write_set_m(path, v, m))
    }

    /// [`Writer::write_set`] with the stamp taken from `stamp` instead of the writer's own clock:
    /// see [`Writer::tick_to`] for what a stamp must satisfy. `stamp` is called once for each stamp the
    /// write needs and never for a write that is refused or changes nothing; a stamp that
    /// fails the check is an `Err` and nothing is written.
    pub fn write_set_stamped(
        &mut self,
        path: &Path,
        v: Value,
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<Written>, StampError> {
        self.stamped(stamp, |w, m| w.write_set_m(path, v, m))
    }

    /// `$unset path`; `None` when no life is alive or the path is absent.
    pub fn write_unset(&mut self, path: &Path, skew: u64) -> Option<Written> {
        self.ticking(skew, |w, m| w.write_unset_m(path, m))
    }

    /// [`Writer::write_unset`] with the stamp taken from `stamp` instead of the writer's own clock:
    /// see [`Writer::tick_to`] for what a stamp must satisfy. `stamp` is called once for each stamp the
    /// write needs and never for a write that is refused or changes nothing; a stamp that
    /// fails the check is an `Err` and nothing is written.
    pub fn write_unset_stamped(
        &mut self,
        path: &Path,
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<Written>, StampError> {
        self.stamped(stamp, |w, m| w.write_unset_m(path, m))
    }

    /// `$inc path by n`, an `Int64` operand. A local increment past the bound is refused (Q11),
    /// and so is one that would carry the writer's slot past [`MAX_TOTAL`]; on a string or a map
    /// it is refused; on a double base it is a register write. See [`Writer::write_inc_value`]
    /// for the other operand types.
    pub fn write_inc(&mut self, p: &Path, n: i64, skew: u64) -> Option<Written> {
        self.ticking(skew, |w, m| w.write_inc_m(p, &Value::Int64(n), m))
    }

    /// `$inc path by by`, where `by` is an `Int32`, `Int64`, `Decimal128` or `Double` (anything
    /// else is refused). The result type is production's (`counter`): two `Int32`s stay an
    /// `Int32` while the total fits, any `Int64` makes it an `Int64`, a `Decimal128` a
    /// `Decimal128` (refused when it overflows from finite inputs). Concurrent increments from
    /// other writers sum with this one. DOUBLES ARE NOT COUNTERS: a `Double` operand against an
    /// `Int32`, `Int64` or `Decimal128` counter is refused, and `$inc` of a double base (or of
    /// an absent field by a double) is a register write.
    pub fn write_inc_value(&mut self, p: &Path, by: &Value, skew: u64) -> Option<Written> {
        self.ticking(skew, |w, m| w.write_inc_m(p, by, m))
    }

    /// [`Writer::write_inc_value`] with the stamp taken from `stamp` instead of the writer's own
    /// clock: see [`Writer::tick_to`] for what a stamp must satisfy.
    pub fn write_inc_value_stamped(
        &mut self,
        p: &Path,
        by: &Value,
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<Written>, StampError> {
        self.stamped(stamp, |w, m| w.write_inc_m(p, by, m))
    }

    /// [`Writer::write_inc`] with the stamp taken from `stamp` instead of the writer's own clock:
    /// see [`Writer::tick_to`] for what a stamp must satisfy. `stamp` is called once for each stamp the
    /// write needs and never for a write that is refused or changes nothing; a stamp that
    /// fails the check is an `Err` and nothing is written.
    pub fn write_inc_stamped(
        &mut self,
        p: &Path,
        n: i64,
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<Written>, StampError> {
        self.stamped(stamp, |w, m| w.write_inc_m(p, &Value::Int64(n), m))
    }

    /// Replace the whole document: changed fields are set, absent ones unset. Each field is a
    /// one-segment path; a `Value::Document` value is diffed against the map the writer sees.
    pub fn write_replace(&mut self, doc: &[(Path, Value)], skew: u64) -> Option<Written> {
        self.ticking(skew, |w, m| w.write_replace_m(doc, m))
    }

    /// [`Writer::write_replace`] with the stamp taken from `stamp` instead of the writer's own clock:
    /// see [`Writer::tick_to`] for what a stamp must satisfy. `stamp` is called once for each stamp the
    /// write needs and never for a write that is refused or changes nothing; a stamp that
    /// fails the check is an `Err` and nothing is written.
    pub fn write_replace_stamped(
        &mut self,
        doc: &[(Path, Value)],
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<Written>, StampError> {
        self.stamped(stamp, |w, m| w.write_replace_m(doc, m))
    }

    /// Delete the document; `None` when no life is alive.
    pub fn write_delete(&mut self, skew: u64) -> Option<Written> {
        self.ticking(skew, |w, m| w.write_delete_m(m))
    }

    /// [`Writer::write_delete`] with the stamp taken from `stamp` instead of the writer's own clock:
    /// see [`Writer::tick_to`] for what a stamp must satisfy. `stamp` is called once for each stamp the
    /// write needs and never for a write that is refused or changes nothing; a stamp that
    /// fails the check is an `Err` and nothing is written.
    pub fn write_delete_stamped(
        &mut self,
        stamp: &mut dyn FnMut() -> MStamp,
    ) -> Result<Option<Written>, StampError> {
        self.stamped(stamp, |w, m| w.write_delete_m(m))
    }

    /// The body of [`Writer::insert`], with the stamp from `m`.
    fn insert_m(&mut self, fields: &[(Path, Value)], m: &mut Mint<'_>) -> Option<Written> {
        let cells = expand_document(fields)?;
        if !self.state.alive(&self.opts).is_empty() {
            return None;
        }
        let known = self.known();
        let st = self.mint(m)?;
        let s = &mut self.state;
        s.lives.insert(
            st,
            Life {
                seen: s.deletes.keys().copied().collect(),
                order: cells.iter().map(|(p, _)| p.clone()).collect(),
                kind: LifeKind::Insert,
            },
        );
        for (p, v) in &cells {
            let generation = (!v.is_numeric()).then(|| {
                let mut vv = s.vv.clone();
                vv.insert(st.writer, st.hlc);
                Gen::start(st, &vv, BTreeMap::new())
            });
            s.regs.insert(
                (st, p.clone()),
                Reg { stamp: st, value: v.clone(), obs: BTreeMap::new(), generation, soi: false },
            );
            #[cfg(any(test, feature = "merge-mutants"))]
            if !v.is_numeric() {
                s.note_base((st, p.clone()), st, &BTreeMap::from([(st.writer, st.hlc)]));
            }
        }
        s.vv.insert(st.writer, st.hlc);
        let items: Items =
            cells.iter().map(|(p, _)| (OrdAt::Field(p.clone()), OrdMode::Write)).collect();
        self.feed(st, st, known, &items);
        let mut rec = WriteRecord::new(
            WriteKind::Insert,
            Some(st),
            cells.iter().map(|(p, _)| p.clone()).collect(),
        );
        rec.vals = cells.iter().map(|(p, v)| (p.clone(), v.clone())).collect();
        Some(self.finish(st, rec))
    }

    /// The body of [`Writer::upsert_soi`], with the stamp from `m`.
    fn upsert_soi_m(
        &mut self,
        fields: &[(Path, Value)],
        inc_path: &Path,
        n: i64,
        sets: &[(Path, Value)],
        m: &mut Mint<'_>,
    ) -> Option<(Written, Written)> {
        let created = self.upsert_soi_create_m(fields, inc_path, sets, m)?;
        let life = created.stamp;
        let incd = self.upsert_inc_on_m(life, inc_path, n, m)?;
        Some((created, incd))
    }

    /// The body of [`Writer::upsert_soi_create`], with the stamp from `m`.
    fn upsert_soi_create_m(
        &mut self,
        fields: &[(Path, Value)],
        inc_path: &Path,
        sets: &[(Path, Value)],
        m: &mut Mint<'_>,
    ) -> Option<Written> {
        if fields.iter().chain(sets).any(|(_, v)| v.check_input().is_err()) {
            return None;
        }
        // The paths of one update must not meet: no path twice, none below another (the
        // reference refuses a conflicting update).
        // (an Unset field is no field: it names nothing that could meet another path)
        let mut paths: Vec<&Path> = fields
            .iter()
            .chain(sets.iter())
            .filter(|(_, v)| *v != Value::Unset)
            .map(|(q, _)| q)
            .chain([inc_path])
            .collect();
        paths.sort_unstable();
        if paths.windows(2).any(|w| w[0] == w[1] || w[1].is_below(w[0])) {
            return None;
        }
        let (mut soi_cells, mut set_cells) = (Vec::new(), Vec::new());
        for (q, v) in fields {
            expand(q, v, &mut soi_cells).ok()?;
        }
        for (q, v) in sets {
            expand(q, v, &mut set_cells).ok()?;
        }
        if !self.state.alive(&self.opts).is_empty() {
            return None;
        }
        let known = self.known();
        let st = self.mint(m)?;
        let s = &mut self.state;
        s.lives.insert(
            st,
            Life {
                seen: s.deletes.keys().copied().collect(),
                order: soi_cells.iter().map(|(p, _)| p.clone()).collect(),
                kind: LifeKind::Upsert,
            },
        );
        // M1: a non-numeric creation value starts a generation, as in `insert`.
        let starts_gen = !self.opts.creation_without_generation();
        let mut cells: Vec<(&Path, &Value, bool)> = Vec::new();
        cells.extend(soi_cells.iter().map(|(q, v)| (q, v, true)));
        cells.extend(set_cells.iter().map(|(q, v)| (q, v, false)));
        for (q, v, soi) in cells {
            let generation = (starts_gen && !v.is_numeric()).then(|| {
                let mut vv = s.vv.clone();
                vv.insert(st.writer, st.hlc);
                Gen::start(st, &vv, BTreeMap::new())
            });
            s.regs.insert(
                (st, q.clone()),
                Reg { stamp: st, value: v.clone(), obs: BTreeMap::new(), generation, soi },
            );
            #[cfg(any(test, feature = "merge-mutants"))]
            if starts_gen && !v.is_numeric() {
                s.note_base((st, q.clone()), st, &BTreeMap::from([(st.writer, st.hlc)]));
            }
        }
        let mut paths = Vec::new();
        let mut vals = BTreeMap::new();
        for (q, v) in soi_cells.iter().chain(set_cells.iter()) {
            if vals.insert(q.clone(), v.clone()).is_none() {
                paths.push(q.clone());
            }
        }
        s.vv.insert(st.writer, st.hlc);
        // a write below a map is a write to the map: each path names its ancestors first
        let (mut items, mut seen) = (Items::new(), BTreeSet::new());
        for q in &paths {
            ancestor_items(q, &mut seen, &mut items, &self.opts);
            items.push((OrdAt::Field(q.clone()), OrdMode::Write));
        }
        self.feed(st, st, known, &items);
        let mut rec = WriteRecord::new(WriteKind::Insert, Some(st), paths);
        rec.vals = vals;
        rec.upsert = true;
        rec.insert = true;
        rec.soi = soi_cells.iter().map(|(p, _)| p.clone()).collect();
        Some(self.finish(st, rec))
    }

    /// The body of [`Writer::upsert_inc_on`], with the stamp from `m`.
    fn upsert_inc_on_m(
        &mut self,
        life: MStamp,
        path: &Path,
        n: i64,
        m: &mut Mint<'_>,
    ) -> Option<Written> {
        if !self.state.alive(&self.opts).contains(&life) {
            return None;
        }
        // The creation wrote this path as a register: a counter slot beside it would belong to
        // no generation of that base (the update conflicts and is refused).
        if self.state.regs.contains_key(&(life, path.clone())) {
            return None;
        }
        let known = self.known();
        let st2 = self.mint(m)?;
        self.state.vv.insert(st2.writer, st2.hlc);
        self.state.slots.insert(
            (life, path.clone(), self.id, None),
            Slot { stamp: st2, total: Num::from(i128::from(n)) },
        );
        let mut items = Items::new();
        ancestor_items(path, &mut BTreeSet::new(), &mut items, &self.opts);
        items.push((OrdAt::Field(path.clone()), OrdMode::Write));
        self.feed(life, st2, known, &items);
        let mut rec = WriteRecord::new(WriteKind::Inc, Some(life), vec![path.clone()]);
        rec.n = Some(n);
        rec.amount = Some(Num::from(i128::from(n)));
        Some(self.finish(st2, rec))
    }

    /// The body of [`Writer::upsert_inc`], with the stamp from `m`.
    fn upsert_inc_m(&mut self, path: &Path, n: i64, m: &mut Mint<'_>) -> Option<Written> {
        if !self.state.alive(&self.opts).is_empty() {
            return None;
        }
        let known = self.known();
        let st = self.mint(m)?;
        let s = &mut self.state;
        s.lives.insert(
            st,
            Life {
                seen: s.deletes.keys().copied().collect(),
                order: Vec::new(),
                kind: LifeKind::Upsert,
            },
        );
        s.slots.insert(
            (st, path.clone(), self.id, None),
            Slot { stamp: st, total: Num::from(i128::from(n)) },
        );
        let mut items = Items::new();
        ancestor_items(path, &mut BTreeSet::new(), &mut items, &self.opts);
        items.push((OrdAt::Field(path.clone()), OrdMode::Write));
        self.feed(st, st, known, &items);
        let mut rec = WriteRecord::new(WriteKind::Inc, Some(st), vec![path.clone()]);
        rec.n = Some(n);
        rec.amount = Some(Num::from(i128::from(n)));
        rec.insert = true;
        Some(self.finish(st, rec))
    }

    /// The model's `_diff`: the leaf effects of changing `path` from what the writer sees there
    /// to `n`, and the order items the change names. Type-strict (Q4, ruled: only changed fields
    /// are written): a leaf is set when it changed; a sub-document sets its changed leaves,
    /// unsets the old fields it lacks, and writes its map marker when the path was not a map
    /// before or anything below it was written. A scalar or an array over a map unsets what the
    /// writer saw below it. The ORDER items name every field the new value names, changed or
    /// not: `mode` for `path` itself (a plain write for the path of a `$set`, a reset for the
    /// fields of a replace and for everything inside a sub-document), a level mark at a
    /// sub-document, an unset for each old field it lacks. `Err` when a field name cannot be a
    /// segment.
    fn diff(
        &self,
        vis: &BTreeMap<Path, Value>,
        path: &Path,
        n: &Value,
        eff: &mut Eff,
        items: &mut Items,
        mode: OrdMode,
    ) -> Result<(), InvalidPath> {
        let o = vis.get(path);
        let rewrite = self.opts.rewrite_unchanged();
        let was_map = o == Some(&Value::Map);
        let at = OrdAt::Field(path.clone());
        if *n == Value::Unset && !rewrite {
            // the writer's spelling of an unset inside a document: it is the model's `None`
            if o.is_some() {
                unset_subtree(vis, path, eff, items, &self.opts);
            }
            return Ok(());
        }
        if let Value::Document(fields) = n {
            let n0 = eff.len();
            items.push((at.clone(), mode));
            items.push((at, OrdMode::Level));
            for (name, v) in fields {
                self.diff(vis, &path.child(name)?, v, eff, items, OrdMode::Reset)?;
            }
            if was_map {
                let old: Vec<&Path> = below(vis, path)
                    .map(|(q, _)| q)
                    .filter(|q| q.segments().len() == path.segments().len() + 1)
                    .collect();
                let named: BTreeSet<&str> = fields.iter().map(|(name, _)| name.as_str()).collect();
                for q in old {
                    if !named.contains(q.name()) {
                        unset_subtree(vis, q, eff, items, &self.opts);
                    }
                }
            }
            if !was_map || eff.len() > n0 || rewrite {
                eff.put(path.clone(), Value::Map);
            }
        } else {
            if was_map {
                for (q, _) in below(vis, path) {
                    eff.put(q.clone(), Value::Unset);
                    items.push((OrdAt::Field(q.clone()), OrdMode::Unset));
                }
                eff.put(path.clone(), n.clone());
            } else if rewrite || !Value::same(o, n) {
                eff.put(path.clone(), n.clone());
            }
            items.push((at, mode));
        }
        Ok(())
    }

    /// Did the new value of `path` change the ORDER of the fields the writer sees there, with
    /// no leaf different (the model's `not ordered_same(cur, arg)`)? Only a sub-document has an
    /// order. A path that is not a sub-document in the writer's view counts as changed.
    fn reordered(&self, path: &Path, new: &Value) -> bool {
        let Value::Document(fields) = new else { return false };
        if self.opts.ord_reorder_is_noop() {
            return false;
        }
        let Some(doc) = self.state.ordered_document(&self.opts) else { return false };
        let mut level: &[(String, OrderedValue)] = &doc;
        for (i, seg) in path.segments().iter().enumerate() {
            let Some((_, v)) = level.iter().find(|(name, _)| name == seg) else { return true };
            match v {
                OrderedValue::Doc(children) => level = children,
                OrderedValue::Leaf(_) => return true,
            }
            if i + 1 == path.segments().len() {
                return !same_fields(level, fields);
            }
        }
        true
    }

    /// Set the cells of `eff` in one write (one stamp) and feed the order state with its items,
    /// as the model's `_write_effects`. An order-only write (no cell, a changed field order) is a
    /// write like any other.
    fn commit(&mut self, eff: Eff, items: &Items, m: &mut Mint<'_>) -> Option<Written> {
        let known = self.known();
        let st = self.mint(m)?;
        self.state.vv.insert(st.writer, st.hlc);
        let al = self.state.alive_set(self.opts.retention);
        let l = al.last().copied();
        for q in &eff.order {
            self.setreg(q, eff.vals[q].clone(), st, &al);
        }
        if let Some(life) = l {
            self.feed(life, st, known, items);
        }
        let mut rec = WriteRecord::new(WriteKind::Reg, l, eff.order);
        rec.vals = eff.vals;
        Some(self.finish(st, rec))
    }

    /// The body of [`Writer::write_set`], with the stamp from `m`.
    fn write_set_m(&mut self, path: &Path, v: Value, m: &mut Mint<'_>) -> Option<Written> {
        if v == Value::Unset {
            // the writer's spelling of an unset: it is one, and names no ancestor (the unset body)
            return self.write_unset_m(path, m);
        }
        v.check_input().ok()?;
        let vis = self.visible()?;
        if !self.opts.write_below_scalar() && below_a_scalar(&vis, path) {
            return None;
        }
        let (mut eff, mut items) = (Eff::default(), Items::new());
        ancestor_items(path, &mut BTreeSet::new(), &mut items, &self.opts);
        self.diff(&vis, path, &v, &mut eff, &mut items, OrdMode::Write).ok()?;
        if eff.len() == 0 && !self.reordered(path, &v) {
            return None;
        }
        self.commit(eff, &items, m)
    }

    /// The body of [`Writer::write_unset`], with the stamp from `m`. An unset of a path the writer
    /// does not see changes nothing: no stamp and no floor (the hybrid floor rides only on an
    /// update that another change stamps: [`Writer::write_update`]).
    fn write_unset_m(&mut self, path: &Path, m: &mut Mint<'_>) -> Option<Written> {
        let vis = self.visible()?;
        if !self.opts.write_below_scalar() && below_a_scalar(&vis, path) {
            return None;
        }
        let mut items = Items::new();
        if !vis.contains_key(path) {
            if !self.opts.ord_noop_unset_stands() {
                return None;
            }
            items.push((OrdAt::Field(path.clone()), OrdMode::Unset));
            return self.commit(Eff::default(), &items, m);
        }
        let mut eff = Eff::default();
        unset_subtree(&vis, path, &mut eff, &mut items, &self.opts);
        self.commit(eff, &items, m)
    }

    /// What an `$inc` of `p` by `by` does to the state the writer sees (`vis`), decided before
    /// anything is stamped: refused (`None`), a register write (a double base), or a counter slot.
    fn inc_plan(&self, vis: &BTreeMap<Path, Value>, p: &Path, by: &Value) -> Option<IncPlan> {
        // The production rule decides: a string or a map is refused, so is an integer past the
        // bound, a decimal that overflows and a double operand against a counter. A double base
        // is no counter: its `$inc` is a register write of the computed value.
        let res = match counter::inc_result(vis.get(p), by, self.opts.int_bound())? {
            Inc::Register(v) => {
                // A no-op writes nothing (ADR-208), as for a `$set`: the computed value is the
                // stored representation byte for byte, e.g. `1e300 + 1.0`, or `$inc 0.0`.
                if !self.opts.rewrite_unchanged() && Value::same(vis.get(p), &v) {
                    return Some(IncPlan::Nothing);
                }
                return Some(IncPlan::Register(v));
            }
            Inc::Slot(res) => res,
        };
        let op = Num::from_value(by)?;
        let al = self.state.alive_set(self.opts.retention);
        let life = *al.last()?;
        let win = self.state.winner_in(p, &al, &self.opts);
        let generation = self.state.gen_in(p, &al, win, &self.opts).map(|g| g.stamp);
        let key = (life, p.clone(), self.id, generation);
        // The writer's slot is its running sum in the join of the types it has added. The
        // `Int64` check above is on the saturated visible value, which does not bound the sum
        // under it: refuse the `$inc` that would carry the slot past `MAX_TOTAL`.
        let mut total = match self.state.slots.get(&key) {
            None => op,
            Some(s) => counter::sum_checked(&s.total, &op)?,
        };
        if total.int().is_some_and(|v| v.unsigned_abs() > MAX_TOTAL.unsigned_abs()) {
            return None;
        }
        // The contributor cap: a writer the counter does not have yet is refused at the cap.
        if !self.opts.no_contributor_cap() {
            let cs: BTreeSet<WriterId> =
                self.state.counted_in(p, &al, win, &self.opts).keys().map(|k| k.1).collect();
            if cs.len() >= MAX_CONTRIBUTORS && !cs.contains(&self.id) {
                return None;
            }
        }
        // The visible value became an Int64: the slot is an Int64 from now on, so a total that
        // later dips back under the i32 range does not narrow again.
        if res.kind() == Kind::Int64 && !self.opts.slot_never_widens() {
            total = total.widened();
        }
        Some(IncPlan::Slot(Box::new(SlotPlan { life, key, total, op, generation })))
    }

    /// The body of [`Writer::write_inc`], with the stamp from `m`. `op` is the operand with its
    /// type; the rules are in the module docs of `counter`.
    fn write_inc_m(&mut self, p: &Path, by: &Value, m: &mut Mint<'_>) -> Option<Written> {
        let vis = self.visible()?;
        if !self.opts.write_below_scalar() && below_a_scalar(&vis, p) {
            return None;
        }
        let plan = self.inc_plan(&vis, p, by)?;
        let mut items = Items::new();
        ancestor_items(p, &mut BTreeSet::new(), &mut items, &self.opts);
        items.push((OrdAt::Field(p.clone()), OrdMode::Write));
        match plan {
            IncPlan::Nothing => None,
            IncPlan::Register(v) => {
                let mut eff = Eff::default();
                eff.put(p.clone(), v);
                self.commit(eff, &items, m)
            }
            IncPlan::Slot(plan) => {
                let SlotPlan { life, key, total, op, generation } = *plan;
                let known = self.known();
                let st = self.mint(m)?;
                self.state.vv.insert(st.writer, st.hlc);
                self.state.slots.insert(key, Slot { stamp: st, total });
                self.feed(life, st, known, &items);
                let mut rec = WriteRecord::new(WriteKind::Inc, Some(life), vec![p.clone()]);
                rec.n = op.int().and_then(|v| i64::try_from(v).ok());
                rec.amount = Some(op);
                rec.generation = generation;
                Some(self.finish(st, rec))
            }
        }
    }

    /// The body of [`Writer::write_replace`], with the stamp from `m`. The document's fields
    /// are one-segment paths; a field that is a `Document` is diffed against the map the writer
    /// sees, so only the leaves that changed are written. A replace names the root as a level mark
    /// and each of its fields as a reset; one that changes nothing but the ORDER of the fields is
    /// a write.
    fn write_replace_m(&mut self, doc: &[(Path, Value)], m: &mut Mint<'_>) -> Option<Written> {
        expand_document(doc)?;
        let vis = self.visible()?;
        let (mut eff, mut items) = (Eff::default(), vec![(OrdAt::Root, OrdMode::Level)]);
        let mode =
            if self.opts.ord_replace_without_reset() { OrdMode::Write } else { OrdMode::Reset };
        for (q, v) in doc {
            self.diff(&vis, q, v, &mut eff, &mut items, mode).ok()?;
        }
        let kept: BTreeSet<&Path> = doc.iter().map(|(d, _)| d).collect();
        let tops: Vec<&Path> = vis.keys().filter(|q| q.segments().len() == 1).collect();
        for q in tops {
            if !kept.contains(q) {
                unset_subtree(&vis, q, &mut eff, &mut items, &self.opts);
            }
        }
        if eff.len() == 0 && !self.replace_reorders(doc) {
            return None;
        }
        self.commit(eff, &items, m)
    }

    /// Does a replace by `doc` change the order of the writer's top-level fields, with no leaf
    /// different (the model's `not ordered_same(vis, arg)` of a replace)?
    fn replace_reorders(&self, doc: &[(Path, Value)]) -> bool {
        if self.opts.ord_reorder_is_noop() {
            return false;
        }
        let Some(cur) = self.state.ordered_document(&self.opts) else { return false };
        let new: Vec<(String, Value)> =
            doc.iter().map(|(p, v)| (p.name().to_string(), v.clone())).collect();
        !same_fields(&cur, &new)
    }

    /// The body of [`Writer::write_delete`], with the stamp from `m`.
    fn write_delete_m(&mut self, m: &mut Mint<'_>) -> Option<Written> {
        if self.state.alive(&self.opts).is_empty() {
            return None;
        }
        let st = self.mint(m)?;
        self.state.vv.insert(st.writer, st.hlc);
        self.state.deletes.insert(st, self.state.vv.clone());
        Some(self.finish(st, WriteRecord::new(WriteKind::Delete, None, Vec::new())))
    }

    /// Absorb another state (the model's `_absorb`): join, move the clock to the
    /// max of the joined version vector, report the new loss records.
    pub fn receive_report(&mut self, other: &MergeState) -> Received {
        let (merged, fresh) = join(&self.state, other, &self.opts);
        self.clock = merged.vv.values().copied().chain([self.clock]).max().unwrap_or(Hlc::ZERO);
        let changed = merged != self.state;
        self.state = merged;
        Received { changed, fresh: fresh.into_iter().collect() }
    }

    /// [`Writer::receive_report`] returning only the fresh loss keys.
    pub fn receive(&mut self, other: &MergeState) -> Vec<LossKey> {
        self.receive_report(other).fresh
    }

    /// A loss-record entry (the model's `receive_entry` for a loss): kept unless
    /// it is already below the horizon, already known, or a counter total past
    /// [`MAX_TOTAL`]. True when added.
    pub fn receive_loss(&mut self, key: LossKey, val: LossValue) -> bool {
        if key.stamp.hlc < self.horizon() || self.state.losses.contains_key(&key) {
            return false;
        }
        // A total past `MAX_TOTAL` is one `decode` refuses: never store a state that cannot be read back.
        if matches!(val, LossValue::Total(t) if t.int().is_some_and(|v| v.unsigned_abs() > MAX_TOTAL.unsigned_abs()))
        {
            return false;
        }
        self.state.losses.insert(key, val);
        true
    }
}

/// One operator of a multi-operator update.
#[cfg(any(test, feature = "merge-mutants"))]
#[derive(Clone, Debug)]
pub enum UpdateOp {
    /// `$set path = value`
    Set(Path, Value),
    /// `$unset path`
    Unset(Path),
    /// `$inc path by value`
    Inc(Path, Value),
}

#[cfg(any(test, feature = "merge-mutants"))]
impl UpdateOp {
    fn path(&self) -> &Path {
        match self {
            UpdateOp::Set(p, _) | UpdateOp::Unset(p) | UpdateOp::Inc(p, _) => p,
        }
    }
}

/// The multi-operator update, as the order scenarios need it. It exists in a test or
/// `merge-mutants` build only: item O5 owns the public update (the operator table, the
/// refusals, the implicit markers). This is the model's `_update` for `$set`, `$unset` and
/// `$inc` on pairwise disjoint paths, with the single operators as its one-operator case.
#[cfg(any(test, feature = "merge-mutants"))]
impl Writer {
    /// ONE write, ONE stamp for a list of operators on pairwise disjoint paths. Atomic: any
    /// refusal refuses the whole update. A no-op `$unset` (the path is absent) writes nothing
    /// itself; if another operator makes the update a write, its floor rides on that stamp (the
    /// hybrid floor): metadata only, no entry of its own.
    pub fn write_update(&mut self, ops: &[UpdateOp], skew: u64) -> Option<Written> {
        self.ticking(skew, |w, m| w.write_update_m(ops, m))
    }

    fn write_update_m(&mut self, ops: &[UpdateOp], m: &mut Mint<'_>) -> Option<Written> {
        // an `Unset` spelled as the value of a `Set` is an `Unset`
        let ops: Vec<UpdateOp> = ops
            .iter()
            .map(|o| match o {
                UpdateOp::Set(p, Value::Unset) => UpdateOp::Unset(p.clone()),
                o => o.clone(),
            })
            .collect();
        let ops = &ops[..];
        for (i, a) in ops.iter().enumerate() {
            for b in &ops[i + 1..] {
                let (a, b) = (a.path(), b.path());
                if a == b || a.is_below(b) || b.is_below(a) {
                    return None;
                }
            }
        }
        let vis = self.visible()?;
        let (mut eff, mut items) = (Eff::default(), Items::new());
        let (mut seen, mut pending, mut slots) = (BTreeSet::new(), Vec::new(), Vec::new());
        let mut changed = false;
        for op in ops {
            let p = op.path();
            if !self.opts.write_below_scalar() && below_a_scalar(&vis, p) {
                return None;
            }
            match op {
                UpdateOp::Unset(p) => {
                    if vis.contains_key(p) {
                        unset_subtree(&vis, p, &mut eff, &mut items, &self.opts);
                        changed = true;
                    } else if self.opts.ord_noop_unset_stands() {
                        items.push((OrdAt::Field(p.clone()), OrdMode::Unset));
                        changed = true;
                    } else {
                        pending.push(p.clone());
                    }
                }
                UpdateOp::Set(p, v) => {
                    v.check_input().ok()?;
                    ancestor_items(p, &mut seen, &mut items, &self.opts);
                    let n0 = eff.len();
                    self.diff(&vis, p, v, &mut eff, &mut items, OrdMode::Write).ok()?;
                    changed = changed || eff.len() > n0 || self.reordered(p, v);
                }
                UpdateOp::Inc(p, by) => {
                    let plan = self.inc_plan(&vis, p, by)?;
                    ancestor_items(p, &mut seen, &mut items, &self.opts);
                    items.push((OrdAt::Field(p.clone()), OrdMode::Write));
                    match plan {
                        IncPlan::Nothing => {}
                        IncPlan::Register(v) => {
                            eff.put(p.clone(), v);
                            changed = true;
                        }
                        IncPlan::Slot(plan) => {
                            let SlotPlan { life, key, total, .. } = *plan;
                            slots.push((life, key, total));
                            changed = true;
                        }
                    }
                }
            }
        }
        if !changed {
            return None;
        }
        if !self.opts.ord_no_hybrid() {
            items.extend(pending.into_iter().map(|p| (OrdAt::Field(p), OrdMode::Unset)));
        }
        let known = self.known();
        let st = self.mint(m)?;
        self.state.vv.insert(st.writer, st.hlc);
        let al = self.state.alive_set(self.opts.retention);
        let l = al.last().copied();
        for q in &eff.order {
            self.setreg(q, eff.vals[q].clone(), st, &al);
        }
        let mut paths = eff.order.clone();
        for (_, key, total) in slots {
            paths.push(key.1.clone());
            self.state.slots.insert(key, Slot { stamp: st, total });
        }
        if let Some(life) = l {
            self.feed(life, st, known, &items);
        }
        let mut rec = WriteRecord::new(WriteKind::Reg, l, paths);
        rec.vals = eff.vals;
        Some(self.finish(st, rec))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{opts, wid};
    use super::*;

    fn i(n: i64) -> Value {
        Value::Int64(n)
    }

    fn doc(f: &[(&str, OrderedValue)]) -> Vec<(String, OrderedValue)> {
        f.iter().map(|(n, v)| ((*n).to_string(), v.clone())).collect()
    }

    fn fields(f: &[(&str, Value)]) -> Vec<(String, Value)> {
        f.iter().map(|(n, v)| ((*n).to_string(), v.clone())).collect()
    }

    fn leaf(n: i64) -> OrderedValue {
        OrderedValue::Leaf(Value::Int64(n))
    }

    /// The model's `ordered_same`: the same names in the same order with the same leaves. The scenarios
    /// reach it through the Writer; here its parts are told apart one by one (equal values under other
    /// names, another length either way, a field spelled `Unset`, a sub-document against a leaf).
    #[test]
    fn same_fields_compares_names_order_values_and_length() {
        let cur = doc(&[("p", leaf(1)), ("q", leaf(1))]);
        assert!(same_fields(&cur, &fields(&[("p", i(1)), ("q", i(1))])));
        assert!(!same_fields(&cur, &fields(&[("q", i(1)), ("p", i(1))])), "the names tell");
        assert!(!same_fields(&cur, &fields(&[("p", i(1))])), "a field fewer");
        assert!(!same_fields(&cur, &fields(&[("p", i(1)), ("q", i(1)), ("r", i(1))])), "one more");
        assert!(!same_fields(&cur, &fields(&[("p", i(1)), ("q", i(2))])), "a value");
        assert!(
            same_fields(&cur, &fields(&[("p", i(1)), ("z", Value::Unset), ("q", i(1))])),
            "a field spelled Unset is not a field"
        );
        assert!(!same_fields(&cur, &fields(&[("p", i(1)), ("q", Value::Unset)])));
        let nested = doc(&[("m", OrderedValue::Doc(vec![("a".into(), leaf(1))]))]);
        let same = fields(&[("m", Value::Document(fields(&[("a", i(1))])))]);
        assert!(same_fields(&nested, &same));
        assert!(!same_fields(&nested, &fields(&[("m", i(1))])), "a leaf where a map stands");
        assert!(!same_fields(&doc(&[("m", leaf(1))]), &same), "a map where a leaf stands");
        let deeper = fields(&[("m", Value::Document(fields(&[("a", i(2))])))]);
        assert!(!same_fields(&nested, &deeper), "a leaf one level down");
    }

    /// An `Unset` field of the document of an insert, or of the creation of an upsert, is NO FIELD: no register, so no stamp
    /// for it to win a race with. The model's insert has the field absent, and a register that carried the insert's stamp would
    /// act as an unset above a concurrent, lower-stamped `$set` of the field by another writer. Two lives are alive at once
    /// after a delete (a re-insert and an upsert merge): A upserts `a = 5` at a low stamp, B re-inserts with `a` spelled
    /// `Unset` at a high one; `a` must be 5 after the join, in both orders.
    #[test]
    fn an_unset_field_in_an_insert_or_an_upsert_creation_stores_nothing() {
        let (a, x, y) =
            (Path::flat("a").unwrap(), Path::flat("x").unwrap(), Path::flat("y").unwrap());
        for creation in 0..2 {
            let mut w0 = Writer::new(wid('I', 0), opts());
            w0.insert(&[(x.clone(), i(1))], 0).unwrap();
            w0.write_delete(0).unwrap();
            let (mut wa, mut wb) =
                (Writer::new(wid('A', 0), opts()), Writer::new(wid('B', 0), opts()));
            wa.receive(&w0.state);
            wb.receive(&w0.state);
            // A: a low-stamped creation that sets a = 5
            wa.upsert_soi(&[], &y, 1, &[(a.clone(), i(5))], 0).unwrap();
            // B: a high-stamped creation whose document spells `a` as an Unset
            let st = if creation == 0 {
                wb.insert(&[(a.clone(), Value::Unset), (x.clone(), i(2))], 9).unwrap().stamp
            } else {
                wb.upsert_soi(&[(a.clone(), Value::Unset)], &y, 1, &[], 9).unwrap().1.stamp
            };
            assert!(st.hlc > wa.state.vv[&wa.id], "B's stamp is above A's");
            let ab = join(&wa.state, &wb.state, &opts()).0;
            let ba = join(&wb.state, &wa.state, &opts()).0;
            assert_eq!(ab, ba, "creation {creation}");
            assert_eq!(
                ab.visible(&opts()).unwrap().get(&a),
                Some(&i(5)),
                "creation {creation}: a = 5 survives"
            );
            assert!(
                !wb.state.regs.keys().any(|(l, p)| *p == a && wb.state.lives.contains_key(l)),
                "creation {creation}: B stored a register for the Unset field"
            );
        }
    }

    /// An insert whose every field is `Unset` is the insert of the empty document, whatever the spelling: the same state, a
    /// life with no register and no order event (the model's `insert({})`: a stamp, an alive life, no cell).
    #[test]
    fn an_insert_of_only_unset_fields_is_the_insert_of_the_empty_document() {
        let (a, b) = (Path::flat("a").unwrap(), Path::flat("b").unwrap());
        let mut empty = Writer::new(wid('A', 0), opts());
        let mut unsets = Writer::new(wid('A', 0), opts());
        let e = empty.insert(&[], 3).unwrap();
        let u = unsets.insert(&[(a.clone(), Value::Unset), (b, Value::Unset)], 3).unwrap();
        assert_eq!(e.stamp, u.stamp);
        assert_eq!(empty.state, unsets.state);
        assert!(unsets.state.regs.is_empty(), "no register");
        assert_eq!(unsets.state.lives.len(), 1, "one life");
        assert_eq!(unsets.state.visible(&opts()).unwrap(), BTreeMap::new(), "alive, and empty");
    }

    /// A field spelled `Value::Unset` in a document handed to a write is an unset, for the order state
    /// as for the cells: the writer that spells it so and the one that unsets (or drops) the field end
    /// in the same state.
    #[test]
    fn an_unset_spelled_as_a_value_is_an_unset_for_the_order_state_too() {
        let (x, y) = (Path::flat("x").unwrap(), Path::flat("y").unwrap());
        let made = || {
            let mut w = Writer::new(wid('A', 0), opts());
            w.insert(&[(x.clone(), i(1)), (y.clone(), i(2))], 0).unwrap();
            w
        };
        let (mut a, mut b) = (made(), made());
        a.write_unset(&y, 0).unwrap();
        b.write_set(&y, Value::Unset, 0).unwrap();
        assert_eq!(a.state, b.state);
        let (mut c, mut d) = (made(), made());
        c.write_replace(&[(x.clone(), i(1))], 0).unwrap();
        d.write_replace(&[(x.clone(), i(1)), (y.clone(), Value::Unset)], 0).unwrap();
        assert_eq!(c.state, d.state);
        // and over a field that is not there it is no write at all
        assert!(d.write_set(&y, Value::Unset, 0).is_none());
        // a NESTED path too: the unset body names no ancestor, so the set that spells an unset must not either
        let (m, my) = (Path::flat("m").unwrap(), Path::new(["m", "y"]).unwrap());
        let nested = || {
            let mut w = Writer::new(wid('A', 0), opts());
            let doc = Value::Document(vec![("y".into(), i(1)), ("z".into(), i(2))]);
            w.insert(&[(x.clone(), i(1)), (m.clone(), doc)], 0).unwrap();
            w
        };
        let (mut e, mut f) = (nested(), nested());
        e.write_unset(&my, 0).unwrap();
        f.write_set(&my, Value::Unset, 0).unwrap();
        assert_eq!(e.state, f.state);
        // and in an update
        let (mut g, mut h) = (nested(), nested());
        g.write_update(&[UpdateOp::Unset(my.clone())], 0).unwrap();
        h.write_update(&[UpdateOp::Set(my, Value::Unset)], 0).unwrap();
        assert_eq!(g.state, h.state);
    }
}
