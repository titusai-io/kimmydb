//! A writer incarnation: local write operations over its own [`MergeState`]
//! (the model's `Member` with the `Node.finish` normalisation).

use std::collections::BTreeMap;

use super::join::{JoinOptions, join};
use super::state::{Gen, Life, LifeKind, MAX_TOTAL, MergeState, ObsKey, Reg, Slot};
use super::types::{LossKey, LossValue, MStamp, Path, Value, WriterId};
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
    fn seen(&self, p: &Path) -> BTreeMap<ObsKey, i128> {
        let al = self.state.alive_set(self.opts.retention);
        self.state
            .slots
            .iter()
            .filter(|((l, q, _, _), _)| q == p && al.contains(l))
            .map(|((l, _, w, g), v)| ((*l, *w, *g), v.total))
            .collect()
    }

    fn obs(&self, p: &Path) -> BTreeMap<ObsKey, (MStamp, i128)> {
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

    /// An explicit insert; refused (`None`) while a life is alive.
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

    /// `$set path = v`; `None` when no life is alive or the value is unchanged.
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

    /// Replace the whole document: changed fields are set, absent ones unset.
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
        if !self.state.alive(&self.opts).is_empty() {
            return None;
        }
        let st = self.mint(m)?;
        let s = &mut self.state;
        s.lives.insert(
            st,
            Life {
                seen: s.deletes.keys().copied().collect(),
                order: fields.iter().map(|(p, _)| p.clone()).collect(),
                kind: LifeKind::Insert,
            },
        );
        for (p, v) in fields {
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
            fields.iter().map(|(p, _)| p.clone()).collect(),
        );
        rec.vals = fields.iter().map(|(p, v)| (p.clone(), v.clone())).collect();
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
        let mut paths: Vec<&Path> =
            fields.iter().chain(sets.iter()).map(|(q, _)| q).chain([inc_path]).collect();
        paths.sort_unstable();
        if paths.windows(2).any(|w| w[0] == w[1]) {
            return None;
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
                order: fields.iter().map(|(p, _)| p.clone()).collect(),
                kind: LifeKind::Upsert,
            },
        );
        // M1: a non-numeric creation value starts a generation, as in `insert`.
        let starts_gen = !self.opts.creation_without_generation();
        let mut cells: Vec<(&Path, &Value, bool)> = Vec::new();
        cells.extend(fields.iter().map(|(q, v)| (q, v, true)));
        cells.extend(sets.iter().map(|(q, v)| (q, v, false)));
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
        for (q, v) in fields.iter().chain(sets.iter()) {
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
        rec.soi = fields.iter().map(|(p, _)| p.clone()).collect();
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
        self.state
            .slots
            .insert((life, path.clone(), self.id, None), Slot { stamp: st2, total: i128::from(n) });
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
        s.slots.insert((st, path.clone(), self.id, None), Slot { stamp: st, total: i128::from(n) });
        let mut rec = WriteRecord::new(WriteKind::Inc, Some(st), vec![path.clone()]);
        rec.n = Some(n);
        rec.insert = true;
        Some(self.finish(st, rec))
    }

    /// The body of [`Writer::write_set`], with the stamp from `m`.
    fn write_set_m(&mut self, path: &Path, v: Value, m: &mut Mint<'_>) -> Option<Written> {
        let vis = self.visible()?;
        if Value::same(vis.get(path), &v) {
            return None;
        }
        self.write_reg_unchecked(path, v, m)
    }

    /// The body of [`Writer::write_unset`], with the stamp from `m`.
    fn write_unset_m(&mut self, path: &Path, m: &mut Mint<'_>) -> Option<Written> {
        let vis = self.visible()?;
        vis.get(path)?;
        self.write_reg_unchecked(path, Value::Unset, m)
    }

    /// The body of [`Writer::write_inc`], with the stamp from `m`.
    fn write_inc_m(&mut self, p: &Path, n: i64, m: &mut Mint<'_>) -> Option<Written> {
        let vis = self.visible()?;
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
                return self.write_reg_unchecked(p, Value::Double(d + n as f64), m);
            }
            _ => return None,
        }
        let l = self.write_life()?;
        let generation = self.state.gen_of(p, &self.opts).map(|g| g.stamp);
        let key = (l, p.clone(), self.id, generation);
        let old = self.state.slots.get(&key).map_or(0, |s| s.total);
        // The slot is this writer's running sum, which the saturated check above does not
        // bound; refuse the `$inc` that would carry it past `MAX_TOTAL`.
        let total = old.saturating_add(i128::from(n));
        if total.unsigned_abs() > MAX_TOTAL.unsigned_abs() {
            return None;
        }
        let st = self.mint(m)?;
        self.state.vv.insert(st.writer, st.hlc);
        self.state.slots.insert(key, Slot { stamp: st, total });
        let mut rec = WriteRecord::new(WriteKind::Inc, Some(l), vec![p.clone()]);
        rec.n = Some(n);
        rec.generation = generation;
        Some(self.finish(st, rec))
    }

    /// The register write an increment of a double base turns into: the model
    /// rewrites `op, arg = "set", cur + arg` and then runs the set path, whose
    /// "unchanged value" check has already been passed.
    fn write_reg_unchecked(&mut self, p: &Path, v: Value, m: &mut Mint<'_>) -> Option<Written> {
        let st = self.mint(m)?;
        self.state.vv.insert(st.writer, st.hlc);
        let l = self.write_life();
        self.setreg(p, v.clone(), st);
        let mut rec = WriteRecord::new(WriteKind::Reg, l, vec![p.clone()]);
        rec.vals.insert(p.clone(), v);
        Some(self.finish(st, rec))
    }

    /// The body of [`Writer::write_replace`], with the stamp from `m`.
    fn write_replace_m(&mut self, doc: &[(Path, Value)], m: &mut Mint<'_>) -> Option<Written> {
        let vis = self.visible()?;
        let changes: Vec<(&Path, &Value)> =
            doc.iter().filter(|(q, v)| !Value::same(vis.get(q), v)).map(|(q, v)| (q, v)).collect();
        let drops: Vec<&Path> = vis.keys().filter(|q| !doc.iter().any(|(d, _)| d == *q)).collect();
        if changes.is_empty() && drops.is_empty() {
            return None;
        }
        let st = self.mint(m)?;
        self.state.vv.insert(st.writer, st.hlc);
        let l = self.write_life();
        let mut paths = Vec::new();
        let mut vals = BTreeMap::new();
        for (q, v) in changes {
            self.setreg(q, v.clone(), st);
            paths.push(q.clone());
            vals.insert(q.clone(), v.clone());
        }
        for q in drops {
            self.setreg(q, Value::Unset, st);
            paths.push(q.clone());
            vals.insert(q.clone(), Value::Unset);
        }
        let mut rec = WriteRecord::new(WriteKind::Reg, l, paths);
        rec.vals = vals;
        Some(self.finish(st, rec))
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
        let changed = merged.canonical_bytes() != self.state.canonical_bytes();
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
        if matches!(val, LossValue::Total(t) if t.unsigned_abs() > MAX_TOTAL.unsigned_abs()) {
            return false;
        }
        self.state.losses.insert(key, val);
        true
    }
}
