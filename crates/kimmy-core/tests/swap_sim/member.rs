//! A member (`swapmodel5.py` 312-355) and its local operations: the clients' view, the `$out` writer.
//! The rest of `Member` lives in `register.rs` (the register and the flip machinery), `apply.rs` (apply),
//! `loss.rs` (loss records), `relay.rs` (the product's relay) and, in phase B, `snapshot.rs`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::coverage::{CoverageOf, PrefixCoverage};
use crate::entry::*;
use crate::omap::{OMap, OSet};
use crate::snapshot::Join;
use crate::world::{Policy, World};
use kimmy_core::merge_state::MergeState;
use std::rc::Rc;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DocOp {
    Insert,
    Update,
    Delete,
}

/// A D10 refusal episode (`swapmodel6a.py` `refusal_cause`): a member refusing writes because of one register event, from its first
/// refused write until its served physical is no longer superseded by that event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Episode {
    pub start: i64,
    pub last: i64,
    pub n: u32,
    pub end: Option<i64>,
    pub end_q: bool,
    /// the first refusal was on an event held as information (known, not covered); else covered and not yet visible
    pub info: bool,
    pub cover: Option<i64>,
    pub cover_q: bool,
}

#[derive(Clone)]
pub struct Member {
    pub id: usize,
    pub name: String,
    pub skew: i64,
    pub hlc: i64,
    /// `held`: the entries taken (the coverage claim, including whatever a snapshot grant adds)
    pub cv: PrefixCoverage,
    pub phys: OMap<String, Phys>,
    /// the valid bind / lcreate / ldrop entries held, and the same sorted by stamp
    pub events: OSet<EntryId>,
    pub evl: Vec<EntryId>,
    /// physical -> birth stamp (M1: one birth per physical, the stamp of its own event)
    pub bmap: HashMap<String, Stamp>,
    pub creates: HashMap<String, EntryRef>,
    pub abandoned: HashSet<String>,
    /// the visible event; `None` is the identity
    pub cur: Option<EntryRef>,
    /// the records in this member's state (their events are emitted)
    pub loss: OMap<LKey, Body>,
    /// received records whose write's physical is not buried here yet
    pub loss_pending: OMap<LKey, Body>,
    pub loss_phys: HashMap<LKey, String>,
    /// `field` with `K`: records this member entered and then dropped past `K` (never entered again)
    pub dropped_loss: HashSet<LKey>,
    /// the entries this member appended: the only ones it serves onward
    pub appended: BTreeSet<EntryId>,
    /// (phys, key, stamp) of page records held as an unfinished join's state, out of position
    pub held_records: HashSet<(String, u32, Stamp)>,
    /// ADR-224: origin -> hlc below which this member holds history only as a completed snapshot's state
    pub state_held: HashMap<usize, i64>,
    /// m-snap-delete-bare: snapshot tombstones held without an entry
    pub bare_state: HashSet<(String, u32, Stamp)>,
    /// document entries taken as entries (not as snapshot state)
    pub recv_entry: HashSet<EntryId>,
    /// (key, stamp) of a lost write -> events emitted at this member
    pub events_emitted: HashMap<HeadKey, u32>,
    pub hist_kept: HashSet<EntryId>,
    /// own writes + entries applied while the physical was alive + (retain_history) entries judged history
    pub dochist: OSet<EntryId>,
    /// `field`: the merge state of a retained page version (a version a snapshot page carried for a physical that is buried
    /// here, or that was rebuilt as an entry): evaluated cell by cell by D11 (design note 1.4, ASSUMED)
    pub page_states: HashMap<EntryId, Rc<MergeState>>,
    pub pend_tombs: BTreeMap<String, Stamp>,
    pub deferred: Vec<EntryRef>,
    pub learned: HashMap<String, i64>,
    pub join: Option<Join>,
    /// `stale_src_bug`: the source of the last `snapshot_begin` call, whether or not it was refused
    pub snap_src_stale: Option<usize>,
    /// placeholders of a superseded join, carried into the fresh pull (see `supersede_join`)
    pub carry_created: BTreeSet<String>,
    /// rule B: a register event known but not yet covered (information: it takes no effect)
    pub info: OMap<EntryId, EntryRef>,
    /// 6r rule B for records: a loss entry held ahead of its position: information, not in the state, until covered
    pub loss_info: OMap<EntryId, EntryRef>,
    /// the hold route: non-document entries held out of position (known, not appended; `promote_oop` appends them in position)
    pub held_oop: OSet<EntryId>,
    /// D10 refusal episodes, by the event that supersedes the served physical
    pub refusal_eps: OMap<EntryId, Episode>,
    pub vanished: bool,
    pub refused: u32,
    pub inv: BTreeSet<&'static str>,
    /// the step check (the reviewer's `backcheck.py`): kept apart from `inv`, which the differential compares with the
    /// model's, and added to the run's invariants by `finish`
    pub step_inv: BTreeSet<&'static str>,
    pub flips: u32,
    pub last_served: String,
}

impl Member {
    pub fn new(id: usize, name: &str, skew: i64) -> Self {
        let mut phys = OMap::new();
        phys.insert(L.to_string(), Phys::new(PKind::Id));
        let mut bmap = HashMap::new();
        bmap.insert(L.to_string(), neg());
        Self {
            id,
            name: name.to_string(),
            skew,
            hlc: 0,
            cv: PrefixCoverage::new(),
            phys,
            events: OSet::new(),
            evl: Vec::new(),
            bmap,
            creates: HashMap::new(),
            abandoned: HashSet::new(),
            cur: None,
            loss: OMap::new(),
            loss_pending: OMap::new(),
            loss_phys: HashMap::new(),
            dropped_loss: HashSet::new(),
            appended: BTreeSet::new(),
            held_records: HashSet::new(),
            state_held: HashMap::new(),
            bare_state: HashSet::new(),
            recv_entry: HashSet::new(),
            events_emitted: HashMap::new(),
            hist_kept: HashSet::new(),
            dochist: OSet::new(),
            page_states: HashMap::new(),
            pend_tombs: BTreeMap::new(),
            deferred: Vec::new(),
            learned: HashMap::new(),
            join: None,
            snap_src_stale: None,
            carry_created: BTreeSet::new(),
            info: OMap::new(),
            loss_info: OMap::new(),
            held_oop: OSet::new(),
            refusal_eps: OMap::new(),
            vanished: false,
            refused: 0,
            inv: BTreeSet::new(),
            step_inv: BTreeSet::new(),
            flips: 0,
            last_served: L.to_string(),
        }
    }

    // ---- clocks
    pub fn next_hlc(&mut self, w: &World) -> i64 {
        self.hlc = (self.hlc + 1).max(w.clock + self.skew);
        self.hlc
    }
    pub fn peek_hlc(&self, w: &World) -> i64 {
        (self.hlc + 1).max(w.clock + self.skew)
    }
    pub fn witness(&mut self, e: &Entry) {
        self.hlc = self.hlc.max(e.hlc);
    }

    /// The product routes' coverage (ASSUMED, design note phase G addition): an entry held out of position (`held_oop`) is not
    /// covered, as the servable vector skips the entries appended as state (`docs/oplog.md:473, 477`, `engine.rs:5630-5660`): the
    /// contiguous prefix of the origin's entries held and not held out of position.
    fn covers_servable(&self, w: &World, origin: usize, hlc: i64) -> bool {
        for &id in &w.by_origin[origin] {
            if w.ent(id).hlc > hlc {
                return true;
            }
            if !self.cv.has(id) || self.held_oop.contains(&id) {
                return false;
            }
        }
        true
    }

    pub fn holds(&self, id: EntryId) -> bool {
        self.cv.has(id)
    }
    pub fn covers(&self, w: &World, origin: usize, hlc: i64) -> bool {
        if w.held_uncovered && !self.held_oop.is_empty() {
            return self.covers_servable(w, origin, hlc);
        }
        self.cv.covers(w, origin, hlc)
    }
    pub fn covers_past(&self, w: &World, origin: usize, hlc: i64) -> bool {
        if w.held_uncovered && !self.held_oop.is_empty() {
            return self.covers_servable(w, origin, hlc)
                && w.by_origin[origin].iter().any(|&id| w.ent(id).hlc > hlc && self.cv.has(id));
        }
        self.cv.covers_past(w, origin, hlc)
    }

    // ---- local operations (the clients' view)
    pub fn local_doc(&mut self, w: &mut World, key: u32, op: DocOp) -> Option<EntryRef> {
        if self.vanished {
            return None;
        }
        if w.policy == Policy::Field {
            return self.local_doc_field(w, key, op);
        }
        let s = self.serving()?;
        if let Some(cause) = self.refusal_cause(w, &s) {
            self.refused += 1;
            w.stats.bump("refused");
            w.stats.bump(&format!("refused_{cause}"));
            return None;
        }
        let p = self.phys.get(&s).expect("the served physical exists");
        if !p.alive {
            return None;
        }
        let cur = p.docs.get(&key).cloned();
        let live = cur.as_ref().is_some_and(|v| v.body.is_some());
        let mark = format!("{}:{}", self.name, w.clock);
        let (body, opk): (Body, Opk) = match op {
            DocOp::Insert => {
                if live {
                    return None;
                }
                (Some(std::rc::Rc::new(vec![mark])), Opk::Insert)
            }
            DocOp::Update => {
                if !live {
                    return None;
                }
                let mut b: Vec<String> = cur.unwrap().body.unwrap().as_ref().clone();
                b.push(mark);
                (Some(std::rc::Rc::new(b)), Opk::Replace)
            }
            DocOp::Delete => {
                if !live {
                    return None;
                }
                (None, Opk::Delete)
            }
        };
        let pend = self.pending(w);
        let sup = self.first_superseding(w, &s).is_some();
        if self.info_superseding(w, &s).is_some() {
            w.stats.bump("accepted_while_info_superseded"); // a sanity counter: D10 refuses before this point
        }
        let h = self.next_hlc(w);
        let e = w.record(Entry::new(self.id, h, Kind::Doc).phys(&s).doc(key, body, opk), true);
        if pend {
            w.pend_acked.insert(e.idx);
            w.stats.bump("accepted_while_pending");
        }
        if sup {
            w.sup_acked.insert(e.idx);
            w.stats.bump("accepted_while_superseded");
            self.inv.insert("write_accepted_while_superseded");
        }
        w.stats.bump("client_writes");
        w.note(format!(
            "{} write {}{}",
            self.name,
            e,
            if sup { " (SUPERSEDED, known)" } else { "" }
        ));
        self.take(w, &e);
        self.dochist.insert(e.idx); // the origin holds every write it made
        Some(e)
    }

    /// `local_doc` under the `field` policy: the write is made by a `merge_state::Writer` around the document's state with
    /// the harness member's stamp; the entry carries the writer's whole state after the write. Insert writes the register
    /// `v`; an update is a `$set` of `v` (append a mark) or an `$inc` of the counter `n`, chosen by the clock; a delete
    /// is a document delete.
    fn local_doc_field(&mut self, w: &mut World, key: u32, op: DocOp) -> Option<EntryRef> {
        let s = self.serving()?;
        if let Some(cause) = self.refusal_cause(w, &s) {
            self.refused += 1;
            w.stats.bump("refused");
            w.stats.bump(&format!("refused_{cause}"));
            return None;
        }
        let p = self.phys.get(&s).expect("the served physical exists");
        if !p.alive {
            return None;
        }
        let cur = p.docs.get(&key).cloned();
        let live = cur.as_ref().is_some_and(|v| v.body.is_some());
        match op {
            DocOp::Insert if live => return None,
            DocOp::Update | DocOp::Delete if !live => return None,
            _ => {}
        }
        let pend = self.pending(w);
        let sup = self.first_superseding(w, &s).is_some();
        if self.info_superseding(w, &s).is_some() {
            w.stats.bump("accepted_while_info_superseded");
        }
        let h = self.next_hlc(w);
        let mark = format!("{}:{}", self.name, w.clock);
        let counter = w.field_counters && (w.clock + self.id as i64) % 2 == 1;
        let st = crate::field::local_write(
            self.id,
            cur.as_ref().and_then(|v| v.state.as_ref()),
            op,
            h,
            &mark,
            &w.fopts,
            counter,
        );
        let st = match st {
            Ok(Some(st)) => st,
            Ok(None) => return None,
            Err(_) => {
                // the stamp is not above something the document's state has merged: the HLC rule (witness, no tick) is meant to
                // make this impossible; it is counted and the run fails on it
                w.stats.bump("field_stamp_refused");
                return None;
            }
        };
        let opk = match op {
            DocOp::Insert => Opk::Insert,
            DocOp::Update => Opk::Replace,
            DocOp::Delete => Opk::Delete,
        };
        let mut e = Entry::new(self.id, h, Kind::Doc).phys(&s).doc(
            key,
            crate::field::body_of(&st, &w.fopts),
            opk,
        );
        e.fstate = Some(st);
        let e = w.record(e, true);
        if pend {
            w.pend_acked.insert(e.idx);
            w.stats.bump("accepted_while_pending");
        }
        if sup {
            w.sup_acked.insert(e.idx);
            w.stats.bump("accepted_while_superseded");
            self.inv.insert("write_accepted_while_superseded");
        }
        w.stats.bump("client_writes");
        w.note(format!(
            "{} write {}{}",
            self.name,
            e,
            if sup { " (SUPERSEDED, known)" } else { "" }
        ));
        self.take(w, &e);
        self.dochist.insert(e.idx);
        Some(e)
    }

    pub fn local_ldrop(&mut self, w: &mut World) -> Option<EntryRef> {
        if self.vanished {
            return None;
        }
        let s = self.serving()?;
        let h = self.next_hlc(w);
        let e = w.record(Entry::new(self.id, h, Kind::Ldrop).phys(&s), true);
        w.stats.bump("ldrop");
        w.note(format!("{} ldrop {} (viewing {})", self.name, e.hlc, s));
        self.take(w, &e);
        Some(e)
    }

    /// M1: a recreate makes a NEW physical named by its own event, birth = the event's stamp, no id reuse.
    pub fn local_lcreate(&mut self, w: &mut World) -> Option<EntryRef> {
        if self.vanished || self.serving().is_some() {
            return None;
        }
        let h = self.next_hlc(w);
        let name = if w.f.recreate_reuse {
            self.last_served.clone()
        } else {
            format!("R.{}.{}", self.name, h)
        };
        let e = w.record(
            Entry::new(self.id, h, Kind::Lcreate)
                .phys(&name)
                .extra(Extra::Phys(self.last_served.clone())),
            true,
        );
        w.stats.bump("lcreate");
        w.note(format!("{} lcreate {} -> {}", self.name, e.hlc, name));
        self.take(w, &e);
        Some(e)
    }

    // ---- the $out writer
    pub fn start_out(
        &mut self,
        w: &mut World,
        keys: Vec<u32>,
        kind: JobKind,
        bind_step: i64,
    ) -> Job {
        let h0 = self.next_hlc(w);
        let stage = format!("S.{}.{}", self.name, h0);
        let dl = h0 + w.d;
        let c = w.record(Entry::new(self.id, h0, Kind::Create).phys(&stage).dl(dl), true);
        self.take(w, &c);
        let mut recs: Vec<(u32, Stamp)> = Vec::new();
        for &k in &keys {
            let h = self.next_hlc(w);
            let body = Some(std::rc::Rc::new(vec![format!("out:{}:{}", self.name, h0)]));
            let mut row =
                Entry::new(self.id, h, Kind::Doc).phys(&stage).doc(k, body, Opk::Insert).staging();
            if w.policy == Policy::Field {
                // a staged row is an insert into an empty state; its register `v` carries the mark
                let mark = format!("out:{}:{}", self.name, h0);
                let st = crate::field::local_write(
                    self.id,
                    None,
                    DocOp::Insert,
                    h,
                    &mark,
                    &w.fopts,
                    false,
                )
                .expect("a stamp above an empty state")
                .expect("a staged insert");
                row.body = crate::field::body_of(&st, &w.fopts);
                row.fstate = Some(st);
            }
            let e = w.record(row, true);
            w.stage_docs.entry(stage.clone()).or_default().push(e.idx);
            recs.push((k, e.stamp));
            self.take(w, &e);
        }
        w.note(format!("{} stages {} dl={} writer={:?}", self.name, stage, dl, kind));
        Job { writer: self.id, keys, kind, bind_step, stage, dl, digest: recs }
    }

    /// m4: the writer's abandonment of its unbound stage (request failure / restart scan): an ordinary entry.
    pub fn abandon_stage(&mut self, w: &mut World, stage: &str) {
        let h = self.next_hlc(w);
        let e = w.record(Entry::new(self.id, h, Kind::Abandon).phys(stage), true);
        w.stats.bump("abandon_entries");
        w.note(format!("{} abandons {}", self.name, stage));
        self.take(w, &e);
    }

    /// `live_members`: how many members are not vanished (the model keeps at least one alive).
    pub fn do_bind(
        &mut self,
        w: &mut World,
        job: &mut Job,
        live_members: usize,
    ) -> Option<EntryRef> {
        if self.vanished {
            return None;
        }
        if job.kind == JobKind::Vanish && live_members <= 1 {
            job.kind = JobKind::CrashAbandon; // a cluster with no live member has no one to report
        }
        if job.kind == JobKind::Vanish {
            self.vanished = true;
            w.stats.bump("writer_vanished");
            w.note(format!(
                "{} VANISHES after staging {} (no abandon, never writes again)",
                self.name, job.stage
            ));
            return None;
        }
        if job.kind == JobKind::CrashAbandon {
            w.stats.bump("writer_crashed");
            let st = job.stage.clone();
            self.abandon_stage(w, &st);
            return None;
        }
        if job.kind == JobKind::Good && self.peek_hlc(w) > job.dl {
            w.stats.bump("writer_refused_late_bind");
            w.note(format!("{} refuses to bind {}: past its deadline", self.name, job.stage));
            let st = job.stage.clone();
            self.abandon_stage(w, &st);
            return None;
        }
        let h = self.next_hlc(w);
        let mut digest = job.digest.clone();
        digest.sort();
        let b = w.record(
            Entry::new(self.id, h, Kind::Bind)
                .phys(&job.stage)
                .dl(job.dl)
                .extra(Extra::Digest(digest)),
            true,
        );
        if !w.valid(&b) {
            w.stats.bump("invalid_binding");
        }
        w.note(format!("{} binds {} at {} valid={}", self.name, job.stage, b.hlc, w.valid(&b)));
        self.take(w, &b);
        Some(b)
    }
}
