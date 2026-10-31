//! A writer incarnation: local write operations over its own [`MergeState`]
//! (the model's `Member` with the `Node.finish` normalisation).

use std::collections::BTreeMap;

use super::join::{JoinOptions, join};
use super::state::{Gen, Life, LifeKind, MergeState, ObsKey, Reg, Slot};
use super::types::{LossKey, LossValue, MStamp, Value, WriterId};
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
    pub paths: Vec<String>,
    /// Register values written (`Insert`, `Reg`).
    pub vals: BTreeMap<String, Value>,
    /// The increment (`Inc`).
    pub n: Option<i64>,
    /// The generation an increment belongs to (`Inc`).
    pub generation: Option<MStamp>,
    /// An upsert that created the life (the `Inc` of `upsert_inc`, the creation write of `upsert_soi`).
    pub insert: bool,
    /// The creation write of a `$setOnInsert` upsert: a life creation that is not an explicit insert.
    pub upsert: bool,
    /// The `$setOnInsert` paths of an upsert.
    pub soi: Vec<String>,
}

impl WriteRecord {
    fn new(kind: WriteKind, life: Option<MStamp>, paths: Vec<String>) -> Self {
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

    /// The next stamp: strictly above the clock and every version-vector entry,
    /// by `1 + skew` in the wall component.
    pub fn tick(&mut self, skew: u64) -> MStamp {
        let top = self.state.vv.values().copied().chain([self.clock]).max().unwrap_or(Hlc::ZERO);
        self.clock = Hlc::new(top.wall_ms.saturating_add(1).saturating_add(skew), 0);
        MStamp { hlc: self.clock, writer: self.id }
    }

    pub fn visible(&self) -> Option<BTreeMap<String, Value>> {
        self.state.visible(&self.opts)
    }
    pub fn hidden_all(&self) -> BTreeMap<String, i128> {
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
    fn seen(&self, p: &str) -> BTreeMap<ObsKey, i128> {
        let al = self.state.alive_set(self.opts.retention);
        self.state
            .slots
            .iter()
            .filter(|((l, q, _, _), _)| q == p && al.contains(l))
            .map(|((l, _, w, g), v)| ((*l, *w, *g), v.total))
            .collect()
    }

    fn obs(&self, p: &str) -> BTreeMap<ObsKey, (MStamp, i128)> {
        self.state
            .counted(p, &self.opts)
            .into_iter()
            .map(|(k, v)| (k, (v.stamp, v.total)))
            .collect()
    }

    fn setreg(&mut self, p: &str, v: Value, st: MStamp) {
        let l = self.write_life().expect("a register write needs an alive life");
        let generation = if v.is_numeric() {
            self.state.gen_of(p, &self.opts).map(std::borrow::Cow::into_owned)
        } else {
            Some(Gen { stamp: st, vv: self.state.vv.clone(), seen: self.seen(p) })
        };
        let obs = self.obs(p);
        if !v.is_numeric() {
            self.state.gens.insert((l, p.to_string()), (st, self.state.vv.clone()));
        }
        self.state
            .regs
            .insert((l, p.to_string()), Reg { stamp: st, value: v, obs, generation, soi: false });
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
    pub fn insert(&mut self, fields: &[(&str, Value)], skew: u64) -> Option<Written> {
        if !self.state.alive(&self.opts).is_empty() {
            return None;
        }
        let st = self.tick(skew);
        let s = &mut self.state;
        s.lives.insert(
            st,
            Life {
                seen: s.deletes.keys().copied().collect(),
                order: fields.iter().map(|(p, _)| p.to_string()).collect(),
                kind: LifeKind::Insert,
            },
        );
        for (p, v) in fields {
            let generation = (!v.is_numeric()).then(|| {
                let mut vv = s.vv.clone();
                vv.insert(st.writer, st.hlc);
                Gen { stamp: st, vv, seen: BTreeMap::new() }
            });
            s.regs.insert(
                (st, p.to_string()),
                Reg { stamp: st, value: v.clone(), obs: BTreeMap::new(), generation, soi: false },
            );
            if !v.is_numeric() {
                s.gens.insert((st, p.to_string()), (st, BTreeMap::from([(st.writer, st.hlc)])));
            }
        }
        s.vv.insert(st.writer, st.hlc);
        let mut rec = WriteRecord::new(
            WriteKind::Insert,
            Some(st),
            fields.iter().map(|(p, _)| p.to_string()).collect(),
        );
        rec.vals = fields.iter().map(|(p, v)| (p.to_string(), v.clone())).collect();
        Some(self.finish(st, rec))
    }

    /// V6m2/V7m2/V8: an upsert that creates the document with `$setOnInsert`
    /// `fields`, applies `$set` `sets` and `$inc` `inc_path` by `n`. It is two
    /// sequential local writes by one writer: the creation (the life and its
    /// cells, the `$setOnInsert` cells tagged) and the increment, each with its
    /// own stamp. Only the tagged cells can be dropped.
    pub fn upsert_soi(
        &mut self,
        fields: &[(&str, Value)],
        inc_path: &str,
        n: i64,
        sets: &[(&str, Value)],
        skew: u64,
    ) -> Option<(Written, Written)> {
        let created = self.upsert_soi_create(fields, sets, skew)?;
        let life = created.stamp;
        let incd = self.upsert_inc_on(life, inc_path, n, skew)?;
        Some((created, incd))
    }

    /// The creation write of a `$setOnInsert` upsert: the new life (kind upsert),
    /// the `$setOnInsert` cells tagged and the `$set` cells untagged, no slot.
    /// Refused while a life is alive.
    pub fn upsert_soi_create(
        &mut self,
        fields: &[(&str, Value)],
        sets: &[(&str, Value)],
        skew: u64,
    ) -> Option<Written> {
        if !self.state.alive(&self.opts).is_empty() {
            return None;
        }
        let st = self.tick(skew);
        let s = &mut self.state;
        s.lives.insert(
            st,
            Life {
                seen: s.deletes.keys().copied().collect(),
                order: fields.iter().map(|(p, _)| p.to_string()).collect(),
                kind: LifeKind::Upsert,
            },
        );
        // M1: a non-numeric creation value starts a generation, as in `insert`.
        let starts_gen = !self.opts.creation_without_generation();
        let mut cells: Vec<(&str, &Value, bool)> = Vec::new();
        cells.extend(fields.iter().map(|(q, v)| (*q, v, true)));
        cells.extend(sets.iter().map(|(q, v)| (*q, v, false)));
        for (q, v, soi) in cells {
            let generation = (starts_gen && !v.is_numeric()).then(|| {
                let mut vv = s.vv.clone();
                vv.insert(st.writer, st.hlc);
                Gen { stamp: st, vv, seen: BTreeMap::new() }
            });
            s.regs.insert(
                (st, q.to_string()),
                Reg { stamp: st, value: v.clone(), obs: BTreeMap::new(), generation, soi },
            );
            if starts_gen && !v.is_numeric() {
                s.gens.insert((st, q.to_string()), (st, BTreeMap::from([(st.writer, st.hlc)])));
            }
        }
        let mut paths = Vec::new();
        let mut vals = BTreeMap::new();
        for (q, v) in fields.iter().chain(sets.iter()) {
            if !paths.contains(&q.to_string()) {
                paths.push(q.to_string());
            }
            vals.insert(q.to_string(), v.clone());
        }
        s.vv.insert(st.writer, st.hlc);
        let mut rec = WriteRecord::new(WriteKind::Insert, Some(st), paths);
        rec.vals = vals;
        rec.upsert = true;
        rec.insert = true;
        rec.soi = fields.iter().map(|(p, _)| p.to_string()).collect();
        Some(self.finish(st, rec))
    }

    /// The increment write of an upsert on the life `life` (the stamp of its
    /// creation write): a slot of `path` for this writer, with the write's own
    /// stamp. Refused (`None`) unless `life` is alive.
    pub fn upsert_inc_on(
        &mut self,
        life: MStamp,
        path: &str,
        n: i64,
        skew: u64,
    ) -> Option<Written> {
        if !self.state.alive(&self.opts).contains(&life) {
            return None;
        }
        let st2 = self.tick(skew);
        self.state.vv.insert(st2.writer, st2.hlc);
        self.state.slots.insert(
            (life, path.to_string(), self.id, None),
            Slot { stamp: st2, total: i128::from(n) },
        );
        let mut rec = WriteRecord::new(WriteKind::Inc, Some(life), vec![path.to_string()]);
        rec.n = Some(n);
        Some(self.finish(st2, rec))
    }

    /// An upserting `$inc` that creates the document; refused while a life is alive.
    pub fn upsert_inc(&mut self, path: &str, n: i64, skew: u64) -> Option<Written> {
        if !self.state.alive(&self.opts).is_empty() {
            return None;
        }
        let st = self.tick(skew);
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
            (st, path.to_string(), self.id, None),
            Slot { stamp: st, total: i128::from(n) },
        );
        let mut rec = WriteRecord::new(WriteKind::Inc, Some(st), vec![path.to_string()]);
        rec.n = Some(n);
        rec.insert = true;
        Some(self.finish(st, rec))
    }

    /// `$set path = v`; `None` when no life is alive or the value is unchanged.
    pub fn write_set(&mut self, path: &str, v: Value, skew: u64) -> Option<Written> {
        let vis = self.visible()?;
        if Value::same(vis.get(path), &v) {
            return None;
        }
        self.write_reg_unchecked(path, v, skew)
    }

    /// `$unset path`; `None` when no life is alive or the path is absent.
    pub fn write_unset(&mut self, path: &str, skew: u64) -> Option<Written> {
        let vis = self.visible()?;
        vis.get(path)?;
        self.write_reg_unchecked(path, Value::Null, skew)
    }

    /// `$inc path by n`. A local increment past the bound is refused (Q11); on a
    /// double base it is a register write that starts a generation (Nm5); on a
    /// string it is refused.
    pub fn write_inc(&mut self, p: &str, n: i64, skew: u64) -> Option<Written> {
        let vis = self.visible()?;
        let cur = vis.get(p);
        let b = i128::from(self.opts.int_bound);
        match cur {
            None | Some(Value::Int(_)) => {
                let c = if let Some(Value::Int(c)) = cur { i128::from(*c) } else { 0 };
                if !(-b - 1..=b).contains(&(c + i128::from(n))) {
                    return None;
                }
            }
            Some(Value::Double(d)) => {
                return self.write_reg_unchecked(p, Value::Double(d + n as f64), skew);
            }
            _ => return None,
        }
        let st = self.tick(skew);
        self.state.vv.insert(st.writer, st.hlc);
        let l = self.write_life()?;
        let generation = self.state.gen_of(p, &self.opts).map(|g| g.stamp);
        let key = (l, p.to_string(), self.id, generation);
        let old = self.state.slots.get(&key).map_or(0, |s| s.total);
        self.state.slots.insert(key, Slot { stamp: st, total: old + i128::from(n) });
        let mut rec = WriteRecord::new(WriteKind::Inc, Some(l), vec![p.to_string()]);
        rec.n = Some(n);
        rec.generation = generation;
        Some(self.finish(st, rec))
    }

    /// The register write an increment of a double base turns into: the model
    /// rewrites `op, arg = "set", cur + arg` and then runs the set path, whose
    /// "unchanged value" check has already been passed.
    fn write_reg_unchecked(&mut self, p: &str, v: Value, skew: u64) -> Option<Written> {
        let st = self.tick(skew);
        self.state.vv.insert(st.writer, st.hlc);
        let l = self.write_life();
        self.setreg(p, v.clone(), st);
        let mut rec = WriteRecord::new(WriteKind::Reg, l, vec![p.to_string()]);
        rec.vals.insert(p.to_string(), v);
        Some(self.finish(st, rec))
    }

    /// Replace the whole document: changed fields are set, absent ones unset.
    pub fn write_replace(&mut self, doc: &[(&str, Value)], skew: u64) -> Option<Written> {
        let vis = self.visible()?;
        let changes: Vec<(&str, &Value)> = doc
            .iter()
            .filter(|(q, v)| !Value::same(vis.get(*q), v))
            .map(|(q, v)| (*q, v))
            .collect();
        let drops: Vec<&String> = vis.keys().filter(|q| !doc.iter().any(|(d, _)| d == q)).collect();
        if changes.is_empty() && drops.is_empty() {
            return None;
        }
        let st = self.tick(skew);
        self.state.vv.insert(st.writer, st.hlc);
        let l = self.write_life();
        let mut paths = Vec::new();
        let mut vals = BTreeMap::new();
        for (q, v) in changes {
            self.setreg(q, v.clone(), st);
            paths.push(q.to_string());
            vals.insert(q.to_string(), v.clone());
        }
        for q in drops {
            self.setreg(q, Value::Null, st);
            paths.push(q.clone());
            vals.insert(q.clone(), Value::Null);
        }
        let mut rec = WriteRecord::new(WriteKind::Reg, l, paths);
        rec.vals = vals;
        Some(self.finish(st, rec))
    }

    /// Delete the document; `None` when no life is alive.
    pub fn write_delete(&mut self, skew: u64) -> Option<Written> {
        if self.state.alive(&self.opts).is_empty() {
            return None;
        }
        let st = self.tick(skew);
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
    /// it is already below the horizon or already known. True when added.
    pub fn receive_loss(&mut self, key: LossKey, val: LossValue) -> bool {
        if key.stamp.hlc < self.horizon() || self.state.losses.contains_key(&key) {
            return false;
        }
        self.state.losses.insert(key, val);
        true
    }
}
