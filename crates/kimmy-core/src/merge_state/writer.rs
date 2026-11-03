//! A writer incarnation: local write operations over its own [`MergeState`]
//! (the model's `Member` with the `Node.finish` normalisation).

use std::collections::BTreeMap;

use super::counter::Num;
use super::join::{JoinOptions, join};
use super::state::{Gen, Life, LifeKind, MAX_TOTAL, MergeState, ObsKey, Reg, Slot};
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
    /// The increment (`Inc`).
    pub n: Option<i64>,
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
/// fields (the writer's expansion of a sub-document); anything else is its one cell.
fn expand(at: &Path, v: &Value, out: &mut Vec<(Path, Value)>) -> Result<(), InvalidPath> {
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
    for (i, (p, v)) in fields.iter().enumerate() {
        if p.segments().len() != 1 || fields[..i].iter().any(|(q, _)| q == p) {
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

/// The model's `_unset_subtree`: unset what the writer sees at `p` and below it.
fn unset_subtree(vis: &BTreeMap<Path, Value>, p: &Path, eff: &mut Eff) {
    for (q, _) in below(vis, p) {
        eff.put(q.clone(), Value::Unset);
    }
    eff.put(p.clone(), Value::Unset);
}

/// The model's `getp` refusal (D-2): is some proper ancestor of `p` visible as a scalar or an
/// array? The reference cannot create a field below one, so the write is refused.
fn below_a_scalar(vis: &BTreeMap<Path, Value>, p: &Path) -> bool {
    p.ancestors().any(|a| vis.get(&a).is_some_and(|v| *v != Value::Map))
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

    fn write_life(&self) -> Option<MStamp> {
        self.state.alive(&self.opts).into_iter().max()
    }

    /// V5m1: every slot total of `p` (any generation, alive lives) as this
    /// writer sees it, kept with a generation it starts.
    fn seen(&self, p: &Path) -> BTreeMap<ObsKey, Num> {
        let al = self.state.alive_set(self.opts.retention);
        self.state
            .slots
            .iter()
            .filter(|((l, q, _, _), _)| q == p && al.contains(l))
            .map(|((l, _, w, g), v)| ((*l, *w, *g), v.total))
            .collect()
    }

    fn obs(&self, p: &Path) -> BTreeMap<ObsKey, (MStamp, Num)> {
        self.state
            .counted(p, &self.opts)
            .into_iter()
            .map(|(k, v)| (k, (v.stamp, v.total)))
            .collect()
    }

    fn setreg(&mut self, p: &Path, v: Value, st: MStamp) {
        let l = self.write_life().expect("a register write needs an alive life");
        let generation = if v.is_numeric() {
            self.state.gen_of(p, &self.opts).map(std::borrow::Cow::into_owned)
        } else {
            Some(Gen::start(st, &self.state.vv, self.seen(p)))
        };
        let obs = self.obs(p);
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
            let (s, _) = join(&self.state, &MergeState::default(), &self.opts);
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

    /// `$inc path by n`. A local increment past the bound is refused (Q11), and so is one that
    /// would carry the writer's slot past [`MAX_TOTAL`]; on a double base it is a register
    /// write that starts a generation (Nm5); on a string it is refused.
    pub fn write_inc(&mut self, p: &Path, n: i64, skew: u64) -> Option<Written> {
        self.ticking(skew, |w, m| w.write_inc_m(p, n, m))
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
        self.stamped(stamp, |w, m| w.write_inc_m(p, n, m))
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
        let mut paths: Vec<&Path> =
            fields.iter().chain(sets.iter()).map(|(q, _)| q).chain([inc_path]).collect();
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
            if !paths.contains(q) {
                paths.push(q.clone());
            }
            vals.insert(q.clone(), v.clone());
        }
        s.vv.insert(st.writer, st.hlc);
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
        let st2 = self.mint(m)?;
        self.state.vv.insert(st2.writer, st2.hlc);
        self.state.slots.insert(
            (life, path.clone(), self.id, None),
            Slot { stamp: st2, total: Num::from(i128::from(n)) },
        );
        let mut rec = WriteRecord::new(WriteKind::Inc, Some(life), vec![path.clone()]);
        rec.n = Some(n);
        Some(self.finish(st2, rec))
    }

    /// The body of [`Writer::upsert_inc`], with the stamp from `m`.
    fn upsert_inc_m(&mut self, path: &Path, n: i64, m: &mut Mint<'_>) -> Option<Written> {
        if !self.state.alive(&self.opts).is_empty() {
            return None;
        }
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
        let mut rec = WriteRecord::new(WriteKind::Inc, Some(st), vec![path.clone()]);
        rec.n = Some(n);
        rec.insert = true;
        Some(self.finish(st, rec))
    }

    /// The model's `_diff`: the leaf effects of changing `path` from what the writer sees there
    /// to `n`. Type-strict (Q4, ruled: only changed fields are written): a leaf is set when it
    /// changed; a sub-document sets its changed leaves, unsets the old fields it lacks, and
    /// writes its map marker when the path was not a map before or anything below it was
    /// written. A scalar or an array over a map unsets what the writer saw below it. `Err`
    /// when a field name cannot be a segment.
    fn diff(
        &self,
        vis: &BTreeMap<Path, Value>,
        path: &Path,
        n: &Value,
        eff: &mut Eff,
    ) -> Result<(), InvalidPath> {
        let o = vis.get(path);
        let rewrite = self.opts.rewrite_unchanged();
        let was_map = o == Some(&Value::Map);
        if let Value::Document(fields) = n {
            let n0 = eff.len();
            for (name, v) in fields {
                self.diff(vis, &path.child(name)?, v, eff)?;
            }
            if was_map {
                let old: Vec<&Path> = below(vis, path)
                    .map(|(q, _)| q)
                    .filter(|q| q.segments().len() == path.segments().len() + 1)
                    .collect();
                for q in old {
                    if !fields.iter().any(|(name, _)| name == q.name()) {
                        unset_subtree(vis, q, eff);
                    }
                }
            }
            if !was_map || eff.len() > n0 || rewrite {
                eff.put(path.clone(), Value::Map);
            }
        } else if was_map {
            for (q, _) in below(vis, path) {
                eff.put(q.clone(), Value::Unset);
            }
            eff.put(path.clone(), n.clone());
        } else if rewrite || !Value::same(o, n) {
            eff.put(path.clone(), n.clone());
        }
        Ok(())
    }

    /// Set the cells of `eff` in one write (one stamp), as the model's `_write_effects`.
    fn commit(&mut self, eff: Eff, m: &mut Mint<'_>) -> Option<Written> {
        let st = self.mint(m)?;
        self.state.vv.insert(st.writer, st.hlc);
        let l = self.write_life();
        for q in &eff.order {
            self.setreg(q, eff.vals[q].clone(), st);
        }
        let mut rec = WriteRecord::new(WriteKind::Reg, l, eff.order);
        rec.vals = eff.vals;
        Some(self.finish(st, rec))
    }

    /// The body of [`Writer::write_set`], with the stamp from `m`.
    fn write_set_m(&mut self, path: &Path, v: Value, m: &mut Mint<'_>) -> Option<Written> {
        v.check_input().ok()?;
        let vis = self.visible()?;
        if !self.opts.write_below_scalar() && below_a_scalar(&vis, path) {
            return None;
        }
        let mut eff = Eff::default();
        self.diff(&vis, path, &v, &mut eff).ok()?;
        if eff.len() == 0 {
            return None;
        }
        self.commit(eff, m)
    }

    /// The body of [`Writer::write_unset`], with the stamp from `m`.
    fn write_unset_m(&mut self, path: &Path, m: &mut Mint<'_>) -> Option<Written> {
        let vis = self.visible()?;
        if !self.opts.write_below_scalar() && below_a_scalar(&vis, path) {
            return None;
        }
        vis.get(path)?;
        let mut eff = Eff::default();
        unset_subtree(&vis, path, &mut eff);
        self.commit(eff, m)
    }

    /// The body of [`Writer::write_inc`], with the stamp from `m`.
    fn write_inc_m(&mut self, p: &Path, n: i64, m: &mut Mint<'_>) -> Option<Written> {
        let vis = self.visible()?;
        if !self.opts.write_below_scalar() && below_a_scalar(&vis, p) {
            return None;
        }
        let cur = vis.get(p);
        let b = i128::from(self.opts.int_bound());
        match cur {
            None | Some(Value::Int64(_)) => {
                let c = if let Some(Value::Int64(c)) = cur { i128::from(*c) } else { 0 };
                if !(-b - 1..=b).contains(&(c + i128::from(n))) {
                    return None;
                }
            }
            Some(Value::Double(d)) => {
                let mut eff = Eff::default();
                eff.put(p.clone(), Value::Double(d + n as f64));
                return self.commit(eff, m);
            }
            _ => return None,
        }
        let l = self.write_life()?;
        let generation = self.state.gen_of(p, &self.opts).map(|g| g.stamp);
        let key = (l, p.clone(), self.id, generation);
        let old = self.state.slots.get(&key).and_then(|s| s.total.int()).unwrap_or(0);
        // The slot is this writer's running sum, which the saturated check above does not
        // bound; refuse the `$inc` that would carry it past `MAX_TOTAL`.
        let total = old.saturating_add(i128::from(n));
        if total.unsigned_abs() > MAX_TOTAL.unsigned_abs() {
            return None;
        }
        let st = self.mint(m)?;
        self.state.vv.insert(st.writer, st.hlc);
        self.state.slots.insert(key, Slot { stamp: st, total: Num::from(total) });
        let mut rec = WriteRecord::new(WriteKind::Inc, Some(l), vec![p.clone()]);
        rec.n = Some(n);
        rec.generation = generation;
        Some(self.finish(st, rec))
    }

    /// The body of [`Writer::write_replace`], with the stamp from `m`. The document's fields
    /// are one-segment paths; a field that is a `Document` is diffed against the map the writer
    /// sees, so only the leaves that changed are written.
    fn write_replace_m(&mut self, doc: &[(Path, Value)], m: &mut Mint<'_>) -> Option<Written> {
        expand_document(doc)?;
        let vis = self.visible()?;
        let mut eff = Eff::default();
        for (q, v) in doc {
            self.diff(&vis, q, v, &mut eff).ok()?;
        }
        let tops: Vec<&Path> = vis.keys().filter(|q| q.segments().len() == 1).collect();
        for q in tops {
            if !doc.iter().any(|(d, _)| d == q) {
                unset_subtree(&vis, q, &mut eff);
            }
        }
        if eff.len() == 0 {
            return None;
        }
        self.commit(eff, m)
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
