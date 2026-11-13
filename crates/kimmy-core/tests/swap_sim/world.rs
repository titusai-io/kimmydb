//! The world (`swapmodel5.py` 248-306): the arena of entries, the per-origin lists coverage reads, the clock, the
//! counters, the random stream. `prefix_close` (280-296) belongs to the generous relay and is not ported.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use kimmy_core::merge_state::{JoinOptions, Path};

use crate::entry::{Entry, EntryId, EntryRef, Kind, Stamp, Ver};
use crate::flags::Flags;
use crate::rng::Rng;

#[derive(Default, Clone, Debug)]
pub struct Counter(pub BTreeMap<String, i64>);

impl Counter {
    pub fn bump(&mut self, k: &str) {
        *self.0.entry(k.to_string()).or_insert(0) += 1;
    }
    pub fn add(&mut self, k: &str, n: i64) {
        *self.0.entry(k.to_string()).or_insert(0) += n;
    }
    pub fn get(&self, k: &str) -> i64 {
        self.0.get(k).copied().unwrap_or(0)
    }
}

/// What a physical does with a document entry.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Policy {
    /// whole-document last-writer-wins (the model)
    #[default]
    Document,
    /// field-level merge: entries carry the writer's whole `merge_state` (Q7)
    Field,
}

pub struct World {
    pub policy: Policy,
    /// `field`: mint Q7's loss records as entries in the relay (V6M2, as the Q7 sim does); off for the comparison runs
    pub q7_entries: bool,
    /// `field`: `K` records per document (`outReplaced` and Q7's together); `None`: unbounded (the harness' default)
    pub k: Option<usize>,
    /// the run has converged and `K` is applied (see `Member::enforce_k`)
    pub k_final: bool,
    /// `field`: an update is a `$set` or an `$inc` by the clock's parity (default); off, every update is a `$set` of `v` (the replay
    /// of the model's traces)
    pub field_counters: bool,
    /// the `document` policy also witnesses the stamps of the page versions it joins (off: the model's behaviour; on: the
    /// metamorphic runs, so both policies' clocks agree)
    pub witness_pages: bool,
    /// `field`: the join options (retention 10^9, the Int64 bound), as the Q7 differential
    pub fopts: JoinOptions,
    /// `field`: the paths of loss records, interned (`LKey.path` indexes it)
    pub paths: Vec<Path>,
    pub f: Flags,
    pub rng: Rng,
    /// the deadline gap of a stage (the model's `D`)
    pub d: i64,
    pub trace: bool,
    /// the run has no route step (register events travel only in position): the open-refusal-at-end check applies
    pub in_order: bool,
    /// the run's steps are over and the end-of-run delivery is running (`swapmodel6a.py` `w.quiescing`)
    pub quiescing: bool,
    /// a register event taken out of position is an invariant violation (set by the random run when it has no model route; the
    /// scripted scenarios take one out of position on purpose, and the replay follows the model's own invariants)
    pub reg_in_position_check: bool,
    /// the product routes: an entry held out of position is not covered (the servable vector skips it); the model's hold route
    /// counts a held entry as covered, and the differential is made with this off
    pub held_uncovered: bool,
    /// HB-2 (B1 classification, the manager's ruling): on the product routes the oracle excludes, by name and with a count
    /// (`cnt_hb2_excluded`), a divergence confined to the stages of `hb2_stages` (see `oracle::hb2_stages`). Off only in the
    /// test that shows the exclusion is what clears HB-2's seeds.
    pub hb2_exclusion: bool,
    /// B2: the members' coverage predicate reads their `VectorCoverage` (the product's per-origin maximum) instead of the prefix;
    /// the vector is kept in every run, and compared with the prefix by the step check `vector_disagrees_with_prefix`
    pub vector_cov: bool,
    /// ITEM Q: the product's mark mechanism instead of the model's bookkeeping (default off): an in-window entry is never held for a
    /// held predecessor; a release raises the vector to the released entry (`release_held_in_position`, engine.rs); after each window the
    /// covered marks whose entry is superseded at its key are dropped (`drop_superseded_covered_marks`, engine.rs, #313). Implies the
    /// product's coverage, the vector (`vector_cov`)
    pub product_marks: bool,
    /// ITEM Q, the Q6 build item (with `product_marks`): a covered held mark is dropped also when its entry is LATE, a document entry
    /// addressed to a physical that is buried at this member
    pub drop_late_marks: bool,
    /// ITEM Q: the marks dropped because their physical is buried here (not because they were superseded): (member name, physical, the
    /// physical was alive at the window's sender), checked at the end of the run against the live members that still hold it alive
    pub late_drops: Vec<(String, String, bool)>,
    /// ITEM Q: the first vector-against-prefix disagreement of each member, described (member, step, the first entry the prefix lacks,
    /// the entries held out of position for that origin), for the report
    pub vp_notes: std::cell::RefCell<Vec<String>>,
    /// DF measurements (off by default; `Cfg::strict_holders`, `Cfg::state_survives`): a member HOLDS an entry only if it appended,
    /// retained or holds it out of position / as information (a witnessed-only entry is not held), and the oracle's served state
    /// counts a document version held as state on a live member (`state_versions`) as surviving
    pub strict_holders: bool,
    pub state_survives: bool,
    pub state_versions: Vec<(String, u32, Ver)>,
    pub steps_end: i64,
    /// replay hooks: the page order and the tombstone choice the traced model drew (consumed by the next call)
    pub forced_pages: Option<Vec<String>>,
    pub forced_choice: Option<String>,
    pub log: Vec<String>,
    pub all: Vec<EntryRef>,
    pub by_origin: Vec<Vec<EntryId>>,
    pub clock: i64,
    pub step: i64,
    pub names: Vec<String>,
    /// stage -> the staging entries (global truth, read by the partial-flip check)
    pub stage_docs: HashMap<String, Vec<EntryId>>,
    pub stats: Counter,
    /// (phys, key, stamp) -> the document entry
    pub rec_index: HashMap<(String, u32, Stamp), EntryId>,
    /// record key -> the loss entries of that key, in the order made (the step check and the grant filter read it)
    pub loss_by_key: HashMap<crate::entry::LKey, Vec<EntryId>>,
    /// entries held only by vanished members: lost with them
    pub lost: HashSet<EntryId>,
    pub lost_late: HashSet<(u32, Stamp)>,
    pub snap_carried: Vec<usize>,
    pub pend_acked: HashSet<EntryId>,
    pub sup_acked: HashSet<EntryId>,
}

impl World {
    pub fn new(names: Vec<String>, f: Flags, rng: Rng, d: i64, trace: bool) -> Self {
        let n = names.len();
        Self {
            policy: Policy::Document,
            q7_entries: true,
            k: None,
            k_final: false,
            field_counters: true,
            witness_pages: false,
            fopts: JoinOptions::new(1_000_000_000, i64::MAX).expect("a valid bound"),
            paths: Vec::new(),
            f,
            rng,
            d,
            trace,
            in_order: true,
            quiescing: false,
            reg_in_position_check: false,
            held_uncovered: false,
            hb2_exclusion: true,
            vector_cov: false,
            product_marks: false,
            drop_late_marks: false,
            late_drops: Vec::new(),
            vp_notes: Default::default(),
            strict_holders: false,
            state_survives: false,
            state_versions: Vec::new(),
            steps_end: 0,
            forced_pages: None,
            forced_choice: None,
            log: Vec::new(),
            all: Vec::new(),
            by_origin: vec![Vec::new(); n],
            clock: 0,
            step: 0,
            names,
            stage_docs: HashMap::new(),
            stats: Counter::default(),
            rec_index: HashMap::new(),
            loss_by_key: HashMap::new(),
            lost: HashSet::new(),
            lost_late: HashSet::new(),
            snap_carried: Vec::new(),
            pend_acked: HashSet::new(),
            sup_acked: HashSet::new(),
        }
    }

    pub fn ent(&self, id: EntryId) -> &Entry {
        &self.all[id]
    }

    pub fn ent_rc(&self, id: EntryId) -> EntryRef {
        Rc::clone(&self.all[id])
    }

    /// `record(e, cover)`: assign the next idx, list it under its origin when it is inside the coverage prefix.
    pub fn record(&mut self, mut e: Entry, cover: bool) -> EntryRef {
        e.idx = self.all.len();
        while self.by_origin.len() <= e.origin {
            self.by_origin.push(Vec::new());
        }
        if cover {
            self.by_origin[e.origin].push(e.idx);
        }
        if e.kind == Kind::Doc {
            self.rec_index.insert((e.phys.clone(), e.key, e.stamp), e.idx);
        }
        if e.kind == Kind::Loss
            && let Some(k) = e.lkey
        {
            self.loss_by_key.entry(k).or_default().push(e.idx);
        }
        let rc = Rc::new(e);
        self.all.push(Rc::clone(&rc));
        rc
    }

    /// The loss entries (`Kind::Loss`) of one record key.
    pub fn loss_entries_of(&self, k: &crate::entry::LKey) -> &[EntryId] {
        self.loss_by_key.get(k).map_or(&[], |v| v.as_slice())
    }

    /// R9: a binding stamped above its stage's deadline is invalid; a pure function of the stamps.
    pub fn valid(&self, e: &Entry) -> bool {
        if e.kind == Kind::Bind {
            return e.hlc <= e.dl;
        }
        true
    }

    pub fn note(&mut self, msg: String) {
        if self.trace {
            self.log.push(format!("[{:3} c={}] {}", self.step, self.clock, msg));
        }
    }

    /// The index of a path in the table (interned on first use).
    pub fn path_id(&mut self, p: &Path) -> u16 {
        if let Some(i) = self.paths.iter().position(|x| x == p) {
            return i as u16;
        }
        self.paths.push(p.clone());
        (self.paths.len() - 1) as u16
    }

    pub fn path_label(&self, id: u16) -> String {
        self.paths[id as usize].segments().join(".")
    }

    pub fn name(&self, origin: usize) -> String {
        self.names.get(origin).cloned().unwrap_or_else(|| format!("Z{}", origin - self.names.len()))
    }
}
