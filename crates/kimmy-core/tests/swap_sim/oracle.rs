//! The oracle and the checks (`swapmodel5.py` 1204-1236, 1241-1298, 1423-1605): views, the per-step invariants, the
//! oracle, and `finish` (divergence, oracle state, per-head record sets, event counts, invariants, non-vacuity).
//! The per-head oracle only: heads are required, any true late write is allowed.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::coverage::CoverageOf;
use crate::entry::*;
use crate::member::Member;
use crate::relay::take_quiesce_replica;
use crate::world::{Policy, World};

#[derive(Clone, Debug, PartialEq)]
pub struct State {
    pub serving: Option<String>,
    pub alive: Vec<(String, Vec<(u32, Ver)>)>,
}

impl Member {
    pub fn served(&self) -> BTreeMap<u32, Ver> {
        let Some(s) = self.serving() else { return BTreeMap::new() };
        match self.phys.get(&s) {
            Some(p) if p.alive => p
                .docs
                .iter()
                .filter(|(_, v)| v.body.is_some())
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
            _ => BTreeMap::new(),
        }
    }

    pub fn state(&self) -> State {
        let mut alive: Vec<(String, Vec<(u32, Ver)>)> = self
            .phys
            .iter()
            .filter(|(_, p)| p.alive)
            .map(|(n, p)| {
                let mut d: Vec<(u32, Ver)> = p
                    .docs
                    .iter()
                    .filter(|(_, v)| v.body.is_some())
                    .map(|(k, v)| (*k, v.clone()))
                    .collect();
                d.sort_by_key(|(k, _)| *k);
                (n.clone(), d)
            })
            .collect();
        alive.sort_by(|a, b| a.0.cmp(&b.0));
        State { serving: self.serving(), alive }
    }

    pub fn check_inv(&mut self, w: &World) {
        if let Some(s) = self.serving() {
            match self.phys.get(&s) {
                Some(p) if p.alive && !p.missing => {}
                _ => {
                    self.inv.insert("serve_not_alive");
                }
            }
        }
        let cur_stamp = self.cur.as_ref().map(|c| c.stamp);
        let mut bury_bad = false;
        for (name, p) in self.phys.iter() {
            if p.alive || p.orphan || p.kept {
                continue;
            }
            let b = self.bmap.get(name);
            match (cur_stamp, b) {
                (Some(cs), Some(&b)) if b < cs => {}
                _ => bury_bad = true,
            }
        }
        if bury_bad {
            self.inv.insert("bury_without_newer_visible");
        }
        // the step check (the reviewer's `backcheck.py`, invariant B): every record in a live member's state is backed by a
        // loss entry of that key which the SAME member has appended (serves onward)
        if !self.vanished {
            let unbacked = self
                .loss
                .keys()
                .chain(self.loss_pending.keys())
                .any(|k| !w.loss_entries_of(k).iter().any(|i| self.appended.contains(i)));
            if unbacked {
                self.step_inv.insert("unbacked_state_record");
            }
        }
        // product invariant (i) on the product routes (HB-1): an entry held out of position is neither served nor granted before
        // it is released in position
        if w.held_uncovered
            && !self.vanished
            && self.held_oop.iter().any(|i| self.appended.contains(i))
        {
            self.step_inv.insert("held_entry_servable");
        }
        // D9's safety net on the product routes (HB-2's two-replay sub-case): a stage this member orphan-buried without the
        // writer's abandon is buried only once the member covers the writer past the deadline with a COVERED witness entry above
        // it (an entry held out of position raises no vector). Computed here from the member's coverage and held set, apart
        // from `covers_past`
        if w.held_uncovered && !self.vanished {
            for (name, p) in self.phys.iter() {
                if !p.orphan || p.kind != PKind::Stage || self.abandoned.contains(name) {
                    continue;
                }
                let Some(cr) = self.creates.get(name) else { continue };
                let witness = w.by_origin[cr.origin].iter().any(|&id| {
                    w.ent(id).hlc > cr.dl && self.cv.has(id) && !self.held_oop.contains(&id)
                });
                if !(witness && self.covers_servable(w, cr.origin, cr.dl)) {
                    self.step_inv.insert("orphan_burial_without_covered_witness");
                }
            }
        }
        // B2's step check: on the routes that deliver in position (R1 windows and pushes, R4 grants; not the model's hold and repair
        // routes, which count a held entry as covered), the product's vector covers exactly the member's contiguous servable prefix
        // Skipped in two model-only states, neither of which the product has:
        // - while a member holds an entry the model's way (`held_uncovered` off): the model's hold route counts a held entry as
        //   covered, so the prefix covers it and the vector, never raised by a Hold, does not;
        // - once the end-of-run delivery's shortcut (`take_quiesce`, `witnessed_at_quiescence`) has witnessed here an entry that no
        //   live member appended: the shortcut raises the vector over a hole. The seed-1434 twin (`scenarios.rs`
        //   `seed_1434_twin`) reproduces exactly this: r_x is held at C out of position, B vanishes with y, and at the end A and E
        //   are handed r_x by the shortcut with y missing, so their vector covers r_x over the hole y. The product has no such
        //   witness (the FIX-5 ruling), so the check does not judge it.
        if w.in_order
            && !self.vanished
            && !self.quiesce_witnessed
            && (w.held_uncovered || self.held_oop.is_empty())
            && self.vector_prefix_disagreement(w).is_some()
        {
            self.step_inv.insert("vector_disagrees_with_prefix");
            if w.vp_notes.borrow().len() < 40
                && !w.vp_notes.borrow().iter().any(|n| n.starts_with(&format!("{} ", self.name)))
            {
                let o = self.vector_prefix_disagreement(w).unwrap_or(0);
                let i = self.cv.prefix_len(w, o);
                let hole = w.by_origin[o].get(i).map(|&id| {
                    let e = w.ent(id);
                    let p = self.phys.get(&e.phys);
                    let late_here = self.loss.keys().any(|lk| lk.key == e.key && lk.stamp == Some(e.stamp));
                    format!(
                        "hole #{id} hlc {} kind {:?} staging {} phys {} (alive here {:?}) key {} loss-record-here {late_here} held_oop-here {} appended-here {}",
                        e.hlc, e.kind, e.staging, e.phys, p.map(|p| p.alive), e.key,
                        self.held_oop.contains(&id), self.appended.contains(&id)
                    )
                });
                let held: Vec<String> = self
                    .held_oop
                    .iter()
                    .filter(|&&h| w.ent(h).origin == o && w.ent(h).hlc <= self.vc.max_of(o))
                    .map(|&h| {
                        let e = w.ent(h);
                        let p = self.phys.get(&e.phys);
                        let rec = self.loss.keys().any(|lk| lk.key == e.key && lk.stamp == Some(e.stamp));
                        format!(
                            "COVERED-HELD #{h}@{} kind {:?} staging {} phys {:?} (alive here {:?}) key {} loss-record-here {rec} lost-with-writer {} | superseding event known here: {:?}, birth known: {}, entry stamp {:?}",
                            e.hlc, e.kind, e.staging, e.phys, p.map(|p| p.alive), e.key, w.lost.contains(&h),
                            self.late_stamp(w, &e.phys), self.bmap.contains_key(&e.phys), e.stamp
                        )
                    })
                    .collect();
                w.vp_notes.borrow_mut().push(format!("{} step {} origin {o} vector {} prefix_len {i}: {} | held_oop of that origin: {held:?}", self.name, w.step, self.vc.max_of(o), hole.unwrap_or_else(|| "no hole entry".into())));
            }
        }
        // m3: no member holds a loss record for a write that it still serves
        if !self.loss.is_empty() {
            for (k, v) in self.served() {
                if self.loss.keys().any(|lk| lk.key == k && lk.stamp == Some(v.stamp))
                    || self.loss.contains_key(&LKey {
                        key: k,
                        path: None,
                        stamp: None,
                        minter: None,
                    })
                {
                    self.inv.insert("record_for_served_write_at_member");
                    break;
                }
            }
        }
    }
}

/// A lost write: `(_id, path, stamp)` (the `document` policy has no path).
pub type LateKey = (u32, Option<u16>, Stamp);

/// The writes an entry made, as `(path, stamp, record body)`: the `document` policy's one write, or the cells of a `field`
/// entry stamped with the entry's own stamp (a delete is the document-level write, path `None`).
fn writes_of(w: &mut World, e: &Entry, required: bool) -> Vec<(Option<u16>, Stamp, Body)> {
    if w.policy == Policy::Document {
        return vec![(None, e.stamp, e.body.clone())];
    }
    let cells = e
        .fstate
        .as_ref()
        .map(|s| crate::field::late_cells(s, neg(), Some(e.stamp)))
        .unwrap_or_default();
    // REQUIRED: an insert's own fields are covered by the whole-insert write (Q7 D10 (1): "the insert's own fields are
    // covered by (1)"); once its life is superseded the state keeps only that record. ALLOWED keeps every cell.
    let is_insert = e.fstate.as_ref().is_some_and(|s| s.lives.contains_key(&e.stamp));
    cells
        .into_iter()
        .filter(|c| {
            !(required
                && is_insert
                && c.path.is_some()
                && !crate::field::REQUIRE_INSERT_FIELD_CELLS)
        })
        .map(|c| {
            let path = c.path.as_ref().map(|p| w.path_id(p));
            (path, c.stamp, crate::field::record_body(&c.value))
        })
        .collect()
}

pub struct Oracle {
    pub top: Option<EntryRef>,
    pub alive: BTreeSet<String>,
    pub served: BTreeMap<u32, Ver>,
    /// REQUIRED pool: the late writes among the entries some live member holds, under the reachable events. Its heads
    /// must be reported on every live member.
    pub loss: BTreeMap<LateKey, Body>,
    /// ALLOWED: every acknowledged document write in the world's write log that is late under the D11 predicate
    /// (`birth(p) < E.stamp < w.stamp`) for a REACHABLE valid register event E, whether or not any live member still
    /// holds the write's entry. A record that names such a write is legitimate: the write was lost to the swap and the record
    /// is its report, and the record proves the write reached a member that has since vanished.
    pub allowed: BTreeSet<LateKey>,
    pub lingering: BTreeSet<String>,
}

/// The oracle. Entries held by no live member (lost with a vanished member: a write that never left it) are treated
/// as never having reached the cluster: they are excluded, and so is any register event of the same origin stamped
/// after one of them (its coverage can never complete).
pub fn oracle(w: &mut World) -> Oracle {
    let lost = w.lost.clone();
    let allx: Vec<EntryRef> = w.all.iter().filter(|e| !lost.contains(&e.idx)).cloned().collect();
    let unreachable = |w: &World, e: &Entry| {
        w.by_origin[e.origin].iter().any(|&i| lost.contains(&i) && w.ent(i).hlc < e.hlc)
    };
    let mut ev: Vec<EntryRef> = allx
        .iter()
        .filter(|e| {
            matches!(e.kind, Kind::Bind | Kind::Lcreate | Kind::Ldrop)
                && w.valid(e)
                && !unreachable(w, e)
        })
        .cloned()
        .collect();
    ev.sort_by_key(|e| e.stamp);
    let mut birth: HashMap<String, Stamp> = HashMap::new();
    birth.insert(L.to_string(), neg());
    for e in &ev {
        if matches!(e.kind, Kind::Bind | Kind::Lcreate) {
            birth.entry(e.phys.clone()).or_insert(e.stamp);
        }
    }
    let top = ev.last().cloned();
    let (head, mut alive): (Option<String>, BTreeSet<String>) = match &top {
        None => (Some(L.to_string()), [L.to_string()].into_iter().collect()),
        Some(t) if t.kind == Kind::Ldrop => (None, BTreeSet::new()),
        Some(t) => (Some(t.phys.clone()), [t.phys.clone()].into_iter().collect()),
    };
    let mut docs: BTreeMap<u32, Ver> = BTreeMap::new();
    if let Some(h) = &head {
        for e in &allx {
            if e.kind == Kind::Doc && &e.phys == h {
                if w.policy == Policy::Field {
                    // the served document is the join of every entry addressed to the head physical, in any order
                    let es = e.fstate.as_ref().expect("a field entry carries its state");
                    let nv = match docs.get(&e.key).and_then(|c| c.state.clone()) {
                        None => crate::field::ver_of(es.clone(), e.stamp, &w.fopts),
                        Some(cs) => {
                            let (m, _) = kimmy_core::merge_state::join(&cs, es, &w.fopts);
                            let stamp = docs.get(&e.key).map_or(e.stamp, |c| c.stamp.max(e.stamp));
                            crate::field::ver_of(std::rc::Rc::new(m), stamp, &w.fopts)
                        }
                    };
                    docs.insert(e.key, nv);
                } else {
                    let newer = docs.get(&e.key).is_none_or(|c| e.stamp > c.stamp);
                    if newer {
                        docs.insert(e.key, Ver::doc(e.stamp, e.body.clone()));
                    }
                }
            }
        }
    }
    // DF-2 (b): a version that survives as STATE on a live member counts as surviving (the entry may be lost with its writer)
    if w.state_survives
        && w.policy == Policy::Document
        && let Some(h) = &head
    {
        for (ph, k, v) in w.state_versions.iter() {
            if ph == h && docs.get(k).is_none_or(|c| v.stamp > c.stamp) {
                docs.insert(*k, v.clone());
            }
        }
    }
    let served: BTreeMap<u32, Ver> = docs.into_iter().filter(|(_, v)| v.body.is_some()).collect();
    let late = |e: &Entry, events: &[EntryRef]| -> bool {
        let Some(b) = birth.get(&e.phys) else { return false };
        events.iter().any(|x| {
            *b < x.stamp && x.stamp < e.stamp && (x.kind == Kind::Ldrop || x.phys != e.phys)
        })
    };
    let mut loss: BTreeMap<LateKey, Body> = BTreeMap::new();
    for e in &allx {
        if e.kind == Kind::Doc && !e.staging && late(e, &ev) {
            for (path, stamp, body) in writes_of(w, e, true) {
                loss.insert((e.key, path, stamp), body);
            }
        }
    }
    // ALLOWED: the whole write log (held or not), judged over the REACHABLE valid events only: an event whose entry
    // or whose origin's earlier entries died with a vanished member before any live member covered it makes nothing late
    let log: Vec<EntryRef> = w
        .all
        .iter()
        .filter(|e| e.kind == Kind::Doc && !e.staging && late(e, &ev))
        .cloned()
        .collect();
    let mut allowed: BTreeSet<LateKey> = BTreeSet::new();
    let mut lost_late: std::collections::HashSet<(u32, Stamp)> = std::collections::HashSet::new();
    for e in &log {
        for (path, stamp, _) in writes_of(w, e, false) {
            allowed.insert((e.key, path, stamp));
            if lost.contains(&e.idx) {
                lost_late.insert((e.key, stamp)); // the converse: late writes lost with a vanished member
            }
        }
    }
    w.lost_late = lost_late;
    let mut lingering: BTreeSet<String> = BTreeSet::new();
    for c in &allx {
        if c.kind != Kind::Create {
            continue;
        }
        if allx
            .iter()
            .any(|b| b.kind == Kind::Bind && b.phys == c.phys && w.valid(b) && !unreachable(w, b))
        {
            continue;
        }
        if allx.iter().any(|a| a.kind == Kind::Abandon && a.phys == c.phys) {
            continue;
        }
        let blocked =
            w.by_origin[c.origin].iter().any(|&i| lost.contains(&i) && w.ent(i).hlc <= c.dl);
        if !blocked
            && w.by_origin[c.origin].iter().any(|&i| w.ent(i).hlc > c.dl && !lost.contains(&i))
        {
            continue;
        }
        lingering.insert(c.phys.clone());
    }
    for l in &lingering {
        alive.insert(l.clone());
    }
    Oracle { top, alive, served, loss, allowed, lingering }
}

pub type Res = BTreeMap<String, i64>;

/// The named classes of the product-route grid (the B1 classification and the manager's rulings) that a run's problems fall in,
/// and the problems no class explains (a failure). Every class is defined by a structural marker, not by its symptom alone:
/// - `DF-1` (open design finding, atomicity): a partial flip in a run where a member treated as a loser an entry superseded only
///   by a version it does not cover (`cnt_df1_loser_superseded_by_uncovered`); explains `inv_partial_flip`.
/// - `DF-2` (open design finding, convergence): a live member holds state above its coverage behind a permanently lost entry on a
///   physical outside HB-2's set (`cnt_df2_held_above_coverage_behind_lost`); explains the divergence and oracle-state checks.
/// - `O8` (open design finding, FIX-1's consequence): a live member keeps alive a vanished writer's stage that the oracle buries,
///   because the writer's only non-lost entries above the stage's deadline are held out of position at live members and none is
///   covered (`cnt_o8`, `cnt_o8_stages`); explains `alive_mismatch` and `oracle_state`.
/// - `DF-2r` (the DF-2 mechanism applied to a loss record): a live member holds a document entry out of position behind a lost entry
///   of its origin while loss entries of the run are lost with their minters (`cnt_df2r`); explains `loss_missing`, `loss_mismatch`
///   and the head record/event-count invariants.
/// - `O-F1` (open item, `field` only; the model of record has no field policy): `q7_records_diverged`, and a missing head with its
///   record/event-count invariants. Printed and counted, never passed silently.
///
/// HB-2 is not here: the oracle excludes it by name (`cnt_hb2_excluded`).
pub fn product_classes(res: &Res, field: bool) -> (Vec<&'static str>, Vec<String>) {
    let on = |k: &str| res.get(k).copied().unwrap_or(0) != 0;
    let mut classes = Vec::new();
    let mut explained: BTreeSet<&str> = BTreeSet::new();
    if on("inv_partial_flip") && on("cnt_df1_loser_superseded_by_uncovered") {
        classes.push("DF-1");
        explained.insert("inv_partial_flip");
    }
    let df2_keys = [
        "diverged",
        "div_state",
        "div_replay",
        "div_replay_interleaved",
        "oracle_state",
        "served_mismatch",
        "alive_mismatch",
    ];
    // (a class is counted only for a run where it explains a problem)
    if on("cnt_df2_held_above_coverage_behind_lost") && df2_keys.iter().any(|k| on(k)) {
        classes.push("DF-2");
        explained.extend(df2_keys);
    }
    if on("cnt_o8") && (on("alive_mismatch") || on("oracle_state")) {
        classes.push("O8");
        explained.extend(["alive_mismatch", "oracle_state"]);
    }
    // DF-2r (the DF-2 mechanism applied to a loss record): a held late write behind a permanently lost entry, whose records died with
    // their minters; explains the missing head and its record/event-count invariants
    let df2r_keys = [
        "loss_missing",
        "loss_mismatch",
        "inv_head_not_exactly_one_record_and_event",
        "inv_vanish_replicated_late_write_not_exactly_one_record_and_event",
    ];
    let df2r = on("cnt_df2r") && df2r_keys.iter().any(|k| on(k));
    if df2r {
        classes.push("DF-2r");
        explained.extend(df2r_keys);
    }
    if field
        && ["q7_records_diverged", "loss_missing", "inv_head_not_exactly_one_record_and_event"]
            .iter()
            .any(|k| on(k) && !(df2r && *k != "q7_records_diverged"))
    {
        classes.push("O-F1");
        explained.extend([
            "q7_records_diverged",
            "loss_missing",
            "loss_mismatch",
            "inv_head_not_exactly_one_record_and_event",
            "inv_vanish_replicated_late_write_not_exactly_one_record_and_event",
        ]);
    }
    let mut s = crate::run::Summary::default();
    s.add(0, res);
    let p = s.problems();
    let all_inv_explained =
        p.keys().filter(|k| k.starts_with("inv_")).all(|k| explained.contains(k.as_str()));
    let residual: Vec<String> = p
        .keys()
        .filter(|k| {
            !explained.contains(k.as_str()) && !(k.as_str() == "invariant" && all_inv_explained)
        })
        .cloned()
        .collect();
    (classes, residual)
}

/// HB-2's structural set (B1 classification; a HARNESS class, the manager's ruling): on the product routes, the stages that
/// - were created by a writer that has vanished (its create is in the write log, its member is gone for good),
/// - have no valid binding anywhere in the write log (never validly bound: D9's rule, a pure function of the stamps), and
/// - no live member serves or has ever served (`serving`, `last_served`): no client can see them.
///
/// Defined from the write log and the members' states only, never from a divergence. D9 calls such a stage "lingering and
/// unserved"; its clean-up is the age fallback, which the model of record does not model (D9: "not modelled"), so the harness
/// cannot apply the real rule: a divergence confined to these stages is excluded from the oracle, by name, and counted on every
/// run (`cnt_hb2_excluded`). Empty off the product routes and with `World::hb2_exclusion` off.
pub fn hb2_stages(w: &World, ms: &[Member]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    if !w.held_uncovered || !w.hb2_exclusion {
        return out;
    }
    for c in w.all.iter().filter(|e| e.kind == Kind::Create) {
        let vanished_writer = ms.get(c.origin).is_some_and(|m| m.vanished);
        let validly_bound =
            w.all.iter().any(|b| b.kind == Kind::Bind && b.phys == c.phys && w.valid(b));
        let seen = ms
            .iter()
            .filter(|m| !m.vanished)
            .any(|m| m.serving().as_deref() == Some(c.phys.as_str()) || m.last_served == c.phys);
        if vanished_writer && !validly_bound && !seen {
            out.insert(c.phys.clone());
        }
    }
    out
}

/// A state without the physicals of `skip`.
fn without(st: &State, skip: &BTreeSet<String>) -> State {
    State {
        serving: st.serving.clone(),
        alive: st.alive.iter().filter(|(n, _)| !skip.contains(n)).cloned().collect(),
    }
}

fn bk(m: &Member) -> HashSet<HeadKey> {
    m.loss.keys().map(|k| k.head()).collect()
}

/// `finish`: every check, as a map of named counts (0 = clean), as the model's `res`.
pub fn finish(w: &mut World, ms: &mut [Member]) -> Res {
    let o = oracle(w);
    // the replays (two fresh replicas that receive every entry) keep every record they receive: with `K` on, a replica's Q7 side
    // of the window holds the records it detected itself, which no member has, so it would drop what the cluster keeps. They
    // still check that every required head is present and nothing but allowed records is
    let k_final = std::mem::replace(&mut w.k_final, false);
    let mut res: Res = BTreeMap::new();
    let live: Vec<usize> = (0..ms.len()).filter(|&i| !ms[i].vanished).collect();
    let first = live[0];
    // stamp-order replay: two fresh replicas that receive every entry (no snapshots) in stamp order from the live members' appended sets, after the cluster's quiescence
    let mut base_entries: Vec<EntryRef> =
        w.all.iter().filter(|e| !w.lost.contains(&e.idx)).cloned().collect();
    base_entries.sort_by_key(|e| e.stamp); // the relay's order (the model's shuffle is followed by a stable sort on unique stamps, so it never varied the order)
    let mut replays: Vec<(State, HashSet<HeadKey>)> = Vec::new();
    for i in 0..2 {
        let mut z = Member::new(ms.len() + i, &format!("Z{i}"), 0);
        for e in &base_entries {
            take_quiesce_replica(&mut z, ms, w, e);
        }
        z.settle(w);
        replays.push((z.state(), bk(&z)));
    }
    let okeys: HashSet<HeadKey> = o.loss.keys().map(|(k, p, s)| (*k, *p, Some(*s))).collect();
    let allowed: HashSet<HeadKey> = o.allowed.iter().map(|(k, p, s)| (*k, *p, Some(*s))).collect();
    let mut newest: HashMap<(String, u32), Stamp> = HashMap::new();
    for x in w.all.iter() {
        if x.kind == Kind::Doc && !x.staging && !w.lost.contains(&x.idx) {
            let kk = (x.phys.clone(), x.key);
            if newest.get(&kk).is_none_or(|&n| x.stamp > n) {
                newest.insert(kk, x.stamp);
            }
        }
    }
    // the late writes some live member can mint: (head key, the entry that made the write)
    let mut late_writes: Vec<(HeadKey, EntryRef)> = Vec::new();
    for x in w.all.iter().filter(|x| x.kind == Kind::Doc && !x.staging).cloned().collect::<Vec<_>>()
    {
        for (path, stamp, _) in writes_of(w, &x, true) {
            let hk = (x.key, path, Some(stamp));
            if okeys.contains(&hk) {
                late_writes.push((hk, x.clone()));
            }
        }
    }
    let late_by_key: HashMap<HeadKey, EntryRef> =
        late_writes.iter().map(|(k, x)| (*k, x.clone())).collect();
    // the head of a lost write: `document` policy, the newest version of the key in its physical; `field` policy, Q7's
    // head: no other late write on the same path is causally after it (its entry's version vector covers the write)
    let heads: HashSet<HeadKey> = late_writes
        .iter()
        .filter(|(hk, x)| {
            if w.policy == Policy::Document {
                return newest.get(&(x.phys.clone(), x.key)) == Some(&x.stamp);
            }
            let s = hk.2.unwrap();
            !late_writes.iter().any(|(hk2, y)| {
                hk2.0 == hk.0
                    && hk2.1 == hk.1
                    && y.phys == x.phys
                    && hk2.2.unwrap() > s
                    && y.fstate
                        .as_ref()
                        .is_some_and(|st| st.vv.get(&s.writer).is_some_and(|h| *h >= s.hlc))
            })
        })
        .map(|(k, _)| *k)
        .collect();
    // `K` (field collections only; ASSUMED rules in design note 1.4): a required head (or any late write) is exempt iff at least `K` records of its
    // document have later loser stamps: the records minted (a loss entry exists for each, dropped or not), the required heads,
    // and Q7's records of the document in the live members' head states (one family, one window)
    let (heads, exempt): (HashSet<HeadKey>, HashSet<HeadKey>) = match w.k {
        Some(kk) if w.policy == Policy::Field => {
            let minted: HashSet<HeadKey> = w
                .all
                .iter()
                .filter(|x| x.kind == Kind::Loss && !w.lost.contains(&x.idx))
                .filter_map(|x| x.lkey.map(|k| k.head()))
                .chain(heads.iter().copied())
                .collect();
            let mut q7: HashSet<(u32, Stamp, String)> = HashSet::new();
            for &i in &live {
                if let Some(sp) = ms[i].serving().and_then(|s| ms[i].phys.get(&s)) {
                    for (id, v) in sp.docs.iter() {
                        if let Some(st) = &v.state {
                            q7.extend(
                                st.losses.keys().map(|lk| (*id, lk.stamp, format!("{lk:?}"))),
                            );
                        }
                    }
                }
            }
            // the window's total order (as `Member::enforce_k`): loser stamp, then path for `outReplaced`'s records; a Q7 record is
            // older than an `outReplaced` one of the same stamp
            let later = |h: &HeadKey| {
                minted.iter().filter(|g| g.0 == h.0 && (g.2, g.1) > (h.2, h.1)).count()
                    + q7.iter().filter(|g| g.0 == h.0 && Some(g.1) > h.2).count()
            };
            // every key of the required pool is judged (a non-head dropped everywhere is not an unreported one either)
            let exempt: HashSet<HeadKey> =
                okeys.iter().filter(|h| later(h) >= kk).copied().collect();
            let keep: HashSet<HeadKey> =
                heads.iter().filter(|h| !exempt.contains(*h)).copied().collect();
            (keep, exempt)
        }
        _ => (heads, HashSet::new()),
    };
    res.insert("k_exempt_heads".into(), exempt.len() as i64);
    if let Some(kk) = w.k.filter(|_| w.policy == Policy::Field) {
        // no member holds an `outReplaced` record of a document that `K` or more records of it (both families, in its own
        // view: its records and Q7's in its serving physical's state) are newer than
        let over = live.iter().any(|&i| {
            ms[i].loss.keys().any(|k| {
                let Some(s) = k.stamp else { return false };
                let q7 = ms[i]
                    .serving()
                    .and_then(|sv| ms[i].phys.get(&sv))
                    .and_then(|sp| sp.docs.get(&k.key))
                    .and_then(|v| v.state.as_ref())
                    .map_or(0, |st| st.losses.keys().filter(|lk| lk.stamp > s).count());
                let mine = ms[i]
                    .loss
                    .keys()
                    .filter(|o| o.key == k.key && (o.stamp, o.path) > (Some(s), k.path))
                    .count();
                mine + q7 >= kk
            })
        });
        res.insert(
            "cnt_k_dropped".into(),
            live.iter().map(|&i| ms[i].dropped_loss.len() as i64).sum(),
        );
        res.insert("k_exceeded".into(), over as i64);
    }
    let m0state = ms[first].state();
    // HB-2: the verdicts are taken with the structural set's stages left out; when that changes a verdict the run is counted
    let hb2 = hb2_stages(w, ms);
    res.insert("cnt_hb2_stages".into(), hb2.len() as i64);
    let m0p = without(&m0state, &hb2);
    // DF-2's structural marker (B1 classification, an open design finding): a live member holds, in a physical outside HB-2's
    // set, a document version whose entry it does not cover, and an entry of that origin at or below it is lost for good
    let mut df2 = 0;
    for &i in &live {
        let m = &ms[i];
        for (n, p) in m.phys.iter().filter(|(n, p)| p.alive && !hb2.contains(*n)) {
            for (k, v) in p.docs.iter() {
                // the writes the version holds: `document`, the entry of its stamp; `field`, every writer of its version vector up
                // to the HLC merged (the state holds those writes, whichever entry carried them)
                let mut held: Vec<(usize, i64)> = Vec::new();
                if let Some(st) = &v.state {
                    for (wid, h) in st.vv.iter() {
                        let o = wid.node.to_bytes()[0].wrapping_sub(b'A') as usize;
                        if o < w.by_origin.len() {
                            held.push((o, h.wall_ms as i64));
                        }
                    }
                } else if let Some(&id) = w.rec_index.get(&(n.clone(), *k, v.stamp)) {
                    held.push((w.ent(id).origin, w.ent(id).hlc));
                }
                for (o, h) in held {
                    if !m.covers(w, o, h)
                        && w.by_origin[o].iter().any(|&x| w.lost.contains(&x) && w.ent(x).hlc <= h)
                    {
                        df2 += 1;
                    }
                }
            }
        }
    }
    res.insert("cnt_df2_held_above_coverage_behind_lost".into(), df2);
    // DF-2r's structural marker (the DF-2 mechanism applied to a loss record): a live member holds a document entry out of position
    // behind an entry of its origin that is lost for good, and loss entries of the run are lost with their minters, so the late
    // write can neither be applied in position nor get its record (Field 336, 1361, 1365, 1046)
    let df2r = live
        .iter()
        .flat_map(|&i| ms[i].held_oop.iter().copied())
        .filter(|&h| {
            let e = w.ent(h);
            e.kind == Kind::Doc
                && w.by_origin[e.origin]
                    .iter()
                    .any(|&x| w.lost.contains(&x) && w.ent(x).hlc < e.hlc)
        })
        .count() as i64;
    let loss_entry_lost =
        w.lost.iter().any(|&x| matches!(w.ent(x).kind, Kind::Loss | Kind::LossQ7));
    res.insert("cnt_df2r".into(), (df2r > 0 && loss_entry_lost) as i64);
    // O8's structural marker (an open design finding, FIX-1's consequence): a live member keeps alive a vanished writer's stage
    // that the oracle buries, and the writer's only non-lost entries above the stage's deadline are held out of position at live
    // members, none of them a covered witness at this member. Defined from the members' states and the write log, never from
    // the mismatch itself (the reference is `o8count.py`); HB-2's stages are counted apart (`cnt_o8_in_hb2`), their mismatch is excluded by name.
    // Definition: `o8count2.py` (the design-text classifier): a "covered witness" is a PREFIX test over the writer's entries, lost
    // entries are dropped from the entries above the deadline, HB-2's stages are counted apart. Differences from `o8count.py`: the
    // prefix witness (the old one read a single entry), the lost entries, the HB-2 split; and it is structural (no alive_mismatch gate)
    let (mut o8_stages, mut o8_in_hb2) = (0, 0);
    for &i in &live {
        let m = &ms[i];
        for (n, p) in m.phys.iter() {
            if !p.alive || p.kind != PKind::Stage || o.alive.contains(n) {
                continue;
            }
            let Some(cr) = m.creates.get(n) else { continue };
            if !ms[cr.origin].vanished {
                continue;
            }
            let above: Vec<EntryId> = w.by_origin[cr.origin]
                .iter()
                .copied()
                .filter(|&id| w.ent(id).hlc > cr.dl && !w.lost.contains(&id))
                .collect();
            // covered witness (the design text, `o8count2.py`): the writer's entries in order, EVERY one up to the first above the
            // deadline held here and none held out of position (a lost entry is held by no one, so it blocks the prefix)
            let mut covered_witness = false;
            for &id in &w.by_origin[cr.origin] {
                if !m.cv.has(id) || m.held_oop.contains(&id) {
                    break;
                }
                if w.ent(id).hlc > cr.dl {
                    covered_witness = true;
                    break;
                }
            }
            if covered_witness {
                continue;
            }
            if above.iter().any(|&id| live.iter().any(|&j| ms[j].held_oop.contains(&id))) {
                if hb2.contains(n) {
                    o8_in_hb2 += 1;
                } else {
                    o8_stages += 1;
                }
            }
        }
    }
    res.insert("cnt_o8_stages".into(), o8_stages);
    res.insert("cnt_o8".into(), (o8_stages > 0) as i64);
    // the same shape inside HB-2's stages (the mismatch there is excluded by name); 0 when the exclusion is off, the set being empty
    res.insert("cnt_o8_in_hb2".into(), o8_in_hb2);
    let replay_bad = |st: &State, recs: &HashSet<HeadKey>, m0: &State| {
        *st != *m0
            || heads.iter().any(|h| !recs.contains(h))
            || recs.iter().any(|r| !allowed.contains(r))
    };
    let div_replay_raw = replays.iter().any(|(st, recs)| replay_bad(st, recs, &m0state));
    let div_replay = replays.iter().any(|(st, recs)| replay_bad(&without(st, &hb2), recs, &m0p));
    let states_differ_raw = live.iter().any(|&i| ms[i].state() != m0state);
    let states_differ = live.iter().any(|&i| without(&ms[i].state(), &hb2) != m0p);
    let mut hb2_excluded = (div_replay_raw && !div_replay) || (states_differ_raw && !states_differ);
    let loss_sets: Vec<BTreeSet<LKey>> =
        live.iter().map(|&i| ms[i].loss.keys().copied().collect()).collect();
    let loss_differ = loss_sets.iter().any(|s| *s != loss_sets[0]);
    res.insert("div_replay".into(), div_replay as i64);
    res.insert("diverged".into(), (states_differ || loss_differ || div_replay) as i64);
    res.insert("div_state".into(), states_differ as i64);
    res.insert("div_loss".into(), loss_differ as i64);
    let served_mismatch = live.iter().any(|&i| ms[i].served() != o.served);
    let alive_of = |i: usize, skip: &BTreeSet<String>| -> (BTreeSet<String>, BTreeSet<String>) {
        let a: BTreeSet<String> = ms[i]
            .phys
            .iter()
            .filter(|(n, p)| p.alive && !skip.contains(*n))
            .map(|(n, _)| n.clone())
            .collect();
        (a, o.alive.iter().filter(|n| !skip.contains(*n)).cloned().collect())
    };
    let alive_mismatch_raw = live.iter().any(|&i| {
        let (a, b) = alive_of(i, &BTreeSet::new());
        a != b
    });
    let alive_mismatch = live.iter().any(|&i| {
        let (a, b) = alive_of(i, &hb2);
        a != b
    });
    hb2_excluded |= alive_mismatch_raw && !alive_mismatch;
    res.insert("served_mismatch".into(), served_mismatch as i64);
    res.insert("alive_mismatch".into(), alive_mismatch as i64);
    res.insert("oracle_state".into(), (served_mismatch || alive_mismatch) as i64);
    let loss_missing = live.iter().any(|&i| heads.iter().any(|h| !bk(&ms[i]).contains(h)));
    let loss_extra = live.iter().any(|&i| bk(&ms[i]).iter().any(|r| !allowed.contains(r)));
    res.insert("loss_missing".into(), loss_missing as i64);
    res.insert("loss_extra".into(), loss_extra as i64);
    res.insert("loss_mismatch".into(), (loss_missing || loss_extra) as i64);
    res.insert("heads_required".into(), heads.len() as i64);
    res.insert("true_late_writes_allowed".into(), allowed.len() as i64);
    res.insert("late_writes_required_pool".into(), okeys.len() as i64);
    let nonheads: Vec<HeadKey> =
        okeys.iter().filter(|k| !heads.contains(k) && !exempt.contains(k)).copied().collect();
    let unrep: Vec<HeadKey> = nonheads
        .iter()
        .filter(|k| live.iter().any(|&i| !bk(&ms[i]).contains(k)))
        .copied()
        .collect();
    res.insert("nonheads_unreported".into(), unrep.len() as i64);
    res.insert(
        "nonheads_unreported_vanished_origin".into(),
        unrep.iter().filter(|k| ms[late_by_key[*k].origin].vanished).count() as i64,
    );
    res.insert("nonheads_reported".into(), (nonheads.len() - unrep.len()) as i64);
    res.insert("nv_nonheads_unreported".into(), !unrep.is_empty() as i64);
    res.insert(
        "nv_late_delete_head_held_only_as_state".into(),
        heads.iter().any(|k| {
            let x = &late_by_key[k];
            x.body.is_none()
                && !live.iter().any(|&i| ms[i].recv_entry.contains(&x.idx))
                && live.iter().any(|&i| ms[i].dochist.contains(&x.idx))
        }) as i64,
    );
    res.insert(
        "nv_late_delete_heads".into(),
        heads.iter().any(|k| late_by_key[k].body.is_none()) as i64,
    );
    let mut bad = false;
    for k in &heads {
        for &i in &live {
            let m = &ms[i];
            let n = m.loss.keys().filter(|kk| kk.head() == *k).count();
            if n != 1 || m.events_emitted.get(k).copied().unwrap_or(0) != 1 {
                bad = true;
            }
        }
    }
    // an exempt head is reported at most once, and held at most once
    for k in &exempt {
        for &i in &live {
            let m = &ms[i];
            let n = m.loss.keys().filter(|kk| kk.head() == *k).count();
            if n > 1 || m.events_emitted.get(k).copied().unwrap_or(0) > 1 {
                bad = true;
            }
        }
    }
    res.insert("head_not_exactly_one_record_and_event".into(), bad as i64);
    let mut missing_keys: HashSet<HeadKey> = HashSet::new();
    for &i in &live {
        let b = bk(&ms[i]);
        missing_keys.extend(heads.iter().filter(|h| !b.contains(h)).copied());
    }
    if !missing_keys.is_empty() {
        // F4 class: the write's origin is gone for good and no live member holds its entry in its retained oplog
        let f4 = missing_keys.iter().all(|k| {
            let wr: Vec<&EntryRef> = w
                .all
                .iter()
                .filter(|x| {
                    x.kind == Kind::Doc
                        && x.key == k.0
                        && late_by_key.get(k).is_some_and(|y| y.idx == x.idx)
                })
                .collect();
            !wr.is_empty()
                && ms[wr[0].origin].vanished
                && !live.iter().any(|&i| ms[i].dochist.contains(&wr[0].idx))
        });
        res.insert("loss_missing_f4_class".into(), f4 as i64);
        res.insert("loss_missing_unexplained".into(), (!f4) as i64);
    }
    let body_mismatch = live.iter().any(|&i| {
        o.loss
            .iter()
            .any(|((k, p, s), v)| ms[i].loss.get(&LKey::at(*k, *p, *s)).is_some_and(|b| b != v))
    });
    res.insert("loss_body_mismatch".into(), body_mismatch as i64);
    // vanish after replication: a late write of a vanished origin that reached a live member must have exactly ONE
    // record on every live member and have emitted exactly ONE event there
    let vr: Vec<(HeadKey, EntryRef)> = heads
        .iter()
        .filter_map(|k| late_by_key.get(k).map(|x| (*k, x.clone())))
        .filter(|(_, x)| ms[x.origin].vanished)
        .collect();
    res.insert("vr_replicated_late_writes".into(), vr.len() as i64);
    res.insert("vr_unreplicated_late_writes".into(), w.lost_late.len() as i64);
    let mut vr_bad = false;
    for (k, _) in &vr {
        for &i in &live {
            let k = *k;
            if ms[i].loss.keys().filter(|kk| kk.head() == k).count() != 1
                || ms[i].events_emitted.get(&k).copied().unwrap_or(0) != 1
            {
                vr_bad = true;
            }
        }
    }
    let mut minters: HashMap<HeadKey, i64> = HashMap::new();
    for x in w.all.iter() {
        if x.kind == Kind::Loss {
            *minters.entry(x.lkey.unwrap().head()).or_insert(0) += 1;
        }
    }
    let several = vr.iter().any(|(k, _)| minters.get(k).copied().unwrap_or(0) > 1);
    res.insert("vr_several_minters_collapsed".into(), several as i64);
    let dup_events = live.iter().any(|&i| ms[i].events_emitted.values().any(|&c| c > 1));
    let mut inv: BTreeSet<String> = BTreeSet::new();
    if bad {
        inv.insert("head_not_exactly_one_record_and_event".into());
    }
    if vr_bad {
        inv.insert("vanish_replicated_late_write_not_exactly_one_record_and_event".into());
    }
    if dup_events {
        inv.insert("duplicate_event_for_one_write".into());
    }
    for &i in &live {
        let m = &ms[i];
        for k in m.inv.iter().chain(m.step_inv.iter()) {
            inv.insert((*k).to_string());
        }
        let differs = match (&m.cur, &o.top) {
            (None, None) => false,
            (Some(a), Some(b)) => a.idx != b.idx,
            _ => true,
        };
        if differs {
            inv.insert("not_serving_newest_valid_event".into());
        }
        for (n, p) in m.phys.iter() {
            if p.orphan && w.all.iter().any(|e| e.kind == Kind::Bind && &e.phys == n && w.valid(e))
            {
                inv.insert("orphan_with_valid_binding".into());
            }
        }
    }
    // invariant (i): no grant and no page names an entry above its sender's coverage
    if w.stats.get("grant_above_coverage") > 0 {
        inv.insert("grant_above_coverage".into());
    }
    if w.stats.get("page_above_coverage") > 0 {
        inv.insert("page_above_coverage".into());
    }
    // the HLC rule (design note Q-F6): no `field` write was refused for a stamp below what its document's state had merged
    if w.stats.get("field_stamp_refused") > 0 {
        inv.insert("field_write_below_merged_vv".into());
    }
    // Q7's loss entries: never a document write (not retained as one), never in Q6's record family, at most one per member
    // per record
    {
        let mut seen: HashSet<(usize, String, u32, kimmy_core::merge_state::LossKey)> =
            HashSet::new();
        for x in w.all.iter() {
            match x.kind {
                Kind::LossQ7 => {
                    if x.lkey.is_some()
                        || x.q7.is_none()
                        || w.rec_index.values().any(|&i| i == x.idx)
                    {
                        inv.insert("q7_loss_entry_in_the_q6_family".into());
                    }
                    let (lk, _) = x.q7.as_ref().unwrap();
                    if !seen.insert((x.origin, x.phys.clone(), x.key, lk.clone())) {
                        inv.insert("q7_loss_entry_minted_twice_by_a_member".into());
                    }
                }
                Kind::Loss if x.q7.is_some() => {
                    inv.insert("q7_loss_entry_in_the_q6_family".into());
                }
                _ => {}
            }
        }
        for &i in &live {
            if ms[i].dochist.iter().any(|&id| w.ent(id).kind != Kind::Doc) {
                inv.insert("a_non_document_entry_retained_as_a_write".into());
            }
        }
    }
    // Q7's record sets converge through the loss entries: every live member holds the same records for each document of the
    // physical it serves (the Q7 sim's record parity)
    if w.policy == Policy::Field {
        let mut diverged = false;
        let sets =
            |m: &Member| -> BTreeMap<(String, u32), BTreeSet<kimmy_core::merge_state::LossKey>> {
                let mut out = BTreeMap::new();
                for (n, p) in m.phys.iter().filter(|(_, p)| p.alive) {
                    for (k, v) in p.docs.iter() {
                        if let Some(st) = &v.state {
                            out.insert((n.clone(), *k), st.losses.keys().cloned().collect());
                        }
                    }
                }
                out
            };
        let first = sets(&ms[first]);
        for &i in &live {
            if sets(&ms[i]) != first {
                diverged = true;
            }
        }
        res.insert("q7_records_diverged".into(), diverged as i64);
    }
    // D3 invariant: a join's register events are its sender's own (never read from another member)
    if w.stats.get("register_read_from_a_member_that_is_not_the_sender") > 0 {
        inv.insert("register_read_from_a_member_that_is_not_the_sender".into());
    }
    // D10 liveness: in an in-order run (register events travel only in position) no live member may still be refusing
    // writes at the end of the run (a covered or parked event supersedes the physical it serves)
    if w.in_order {
        for &i in &live {
            if let Some(s) = ms[i].serving()
                && (ms[i].first_superseding(w, &s).is_some()
                    || ms[i].info_superseding(w, &s).is_some())
            {
                inv.insert("open_refusal_at_run_end".into());
            }
        }
    }
    let st = &w.stats;
    if st.get("accepted_while_pending") > 0 {
        inv.insert("write_accepted_while_pending".into());
        res.insert(
            "pending_acked_lost".into(),
            w.pend_acked
                .iter()
                .filter(|&&i| {
                    o.loss.keys().any(|(k, _, s)| *k == w.ent(i).key && *s == w.ent(i).stamp)
                })
                .count() as i64,
        );
    }
    if st.get("accepted_while_superseded") > 0 {
        res.insert(
            "superseded_acked_lost".into(),
            w.sup_acked
                .iter()
                .filter(|&&i| {
                    o.loss.keys().any(|(k, _, s)| *k == w.ent(i).key && *s == w.ent(i).stamp)
                })
                .count() as i64,
        );
    }
    for k in &inv {
        res.insert(format!("inv_{k}"), 1);
    }
    res.insert("invariant".into(), (!inv.is_empty()) as i64);
    let st = &w.stats;
    let any_ev_after_flip = w.all.iter().any(|e| {
        matches!(e.kind, Kind::Ldrop | Kind::Lcreate)
            && w.all.iter().any(|b| b.kind == Kind::Bind && w.valid(b) && b.stamp < e.stamp)
    });
    let ldrop_without = w.all.iter().any(|e| {
        e.kind == Kind::Ldrop
            && w.all.iter().any(|b| {
                b.kind == Kind::Bind && w.valid(b) && b.stamp < e.stamp && e.phys != b.phys
            })
    });
    let nv: Vec<(&str, bool)> = vec![
        ("flips", st.get("flips") > 0),
        ("refusals", st.get("refused") > 0),
        ("refusals_by_pending_binding", st.get("refused_bind") > 0),
        ("refusals_by_known_drop", st.get("refused_ldrop") > 0),
        ("refusals_by_known_recreate", st.get("refused_lcreate") > 0),
        ("snapshot_joins", st.get("snap") > 0),
        ("snapshot_after_postflip_write", st.get("snap_after_postflip_write") > 0),
        ("snapshots_carrying_loss_records", st.get("snap_carrying_records") > 0),
        ("snapshot_entries_not_carried", st.get("snap_entries_not_carried") > 0),
        ("late_writes(oracle)", !o.loss.is_empty()),
        ("loss_records", live.iter().any(|&i| !ms[i].loss.is_empty())),
        ("loss_records_minted_inside_prefix", st.get("loss_records_inside_prefix") > 0),
        ("reports_delayed_until_burial", st.get("report_delayed_until_burial") > 0),
        ("history_arrivals_dropped", st.get("history_arrivals_dropped") > 0),
        ("ldrop", st.get("ldrop") > 0),
        ("lcreate", st.get("lcreate") > 0),
        ("ldrop_or_lcreate_after_a_flip", st.get("ldrop_after_flip") > 0 || any_ev_after_flip),
        ("ldrop_stamped_after_f_made_without_it", ldrop_without),
        ("orphans_detected", st.get("orphans_buried") > 0),
        ("abandon_entries", st.get("abandon_entries") > 0),
        ("orphans_buried_by_abandon", st.get("orphans_buried_by_abandon") > 0),
        ("vanished_writer", st.get("writer_vanished") > 0),
        ("vanished_stage_lingering", !o.lingering.is_empty()),
        ("invalid_late_binding", st.get("invalid_binding") > 0),
        ("valid_bind_arrives_late", st.get("valid_bind_arrives_late") > 0),
        ("concurrent_outs_loser_buried", st.get("loser_stage_buried") > 0),
        ("swap_not_here_yet(R3 retry)", st.get("swap_not_here_yet") > 0),
        ("relayed_tombstone", st.get("relay_tomb") > 0),
        ("kept_path_taken", st.get("kept_by_adr231_path") > 0),
        ("write_accepted_while_pending", st.get("accepted_while_pending") > 0),
        ("write_accepted_while_superseded", st.get("accepted_while_superseded") > 0),
    ];
    for (k, v) in nv {
        res.insert(format!("nv_{k}"), v as i64);
    }
    res.insert("accepted_while_pending_count".into(), st.get("accepted_while_pending"));
    res.insert("accepted_while_superseded_count".into(), st.get("accepted_while_superseded"));
    res.insert("nv_vanish_origin_replicated_late_write".into(), !vr.is_empty() as i64);
    res.insert("nv_vanish_origin_unreplicated_late_write".into(), !w.lost_late.is_empty() as i64);
    res.insert("nv_vanish_several_minters_collapsed".into(), several as i64);
    res.insert("nv_entries_lost_with_a_vanished_member".into(), !w.lost.is_empty() as i64);
    for kk in [
        "register_events_above_grant",
        "register_events_not_carried_above_grant",
        "register_events_read_above_grant",
        "bind_between_first_and_last_page",
        "register_read_from_a_member_that_is_not_the_sender",
        "appended_in_position",
        "witnessed_without_receiving",
        "state_held_refusals",
        "snap_deletes_carried_with_id",
        "windows_served_by_a_join_in_progress",
        "lww_losers_not_kept",
        "witnessed_at_quiescence",
        "snap_versions_as_entries",
    ] {
        res.insert(format!("nv_{kk}"), (st.get(kk) > 0) as i64);
        res.insert(format!("cnt_{kk}"), st.get(kk));
    }
    for kk in [
        "loss_info_parked",
        "loss_info_taken_effect",
        "snap_records_not_carried_above_grant",
        "loss_records_read_late",
        "loss_records_above_grant",
        "loss_records_read_above_grant",
        "info_parked",
        "info_taken_effect",
        "accepted_while_info_superseded",
        "grant_above_coverage",
        "page_above_coverage",
        "grant_entries_withheld_above_coverage",
        "dead_page_versions_retained",
        "snap_completed",
        "join_superseded",
        "end_repairs",
        "join_restarted_dead_sender",
        "dead_sender_join_waiting",
        "dead_sender_join_open_at_end",
        "covered_skips",
        "hold_taken",
        "doc_hold_taken",
        "held_behind_held",
        "held_released_in_window",
        "held_kept_unservable",
        "df1_loser_superseded_by_uncovered",
        "late_d11_not_buried",
        "late_drop_phys_alive_at_sender",
        "late_drop_phys_alive_elsewhere",
        "late_buried_not_d11",
        "page_carries_held_state",
        "doc_repair",
        "route_carries_q6_kind",
        "route_picked_q6_kind",
        "register_event_out_of_position",
        "q6_kind_out_of_position",
        "repair_events",
        "refusal_episodes",
        "refusal_episodes_info",
        "refusal_open_at_steps_end",
        "refusal_open_at_steps_end_info",
        "refusal_open_at_run_end",
        "refusal_open_at_run_end_info",
        "q7_records_fresh",
        "same_write_held",
    ] {
        res.insert(format!("cnt_{kk}"), st.get(kk));
    }
    res.insert("late_write_count".into(), o.loss.len() as i64);
    res.insert("refused_count".into(), st.get("refused"));
    res.insert("refused_bind_count".into(), st.get("refused_bind"));
    res.insert("refused_ldrop_count".into(), st.get("refused_ldrop"));
    res.insert("refused_lcreate_count".into(), st.get("refused_lcreate"));
    // the interleaved replay: each origin's order kept, a random interleaving across origins, each entry served by a
    // random live member that appended it (witnessed if none did)
    let mut div_interleaved = false;
    let mut div_interleaved_raw = false;
    for i in 0..2 {
        let mut z = Member::new(ms.len() + 2 + i, &format!("Y{i}"), 0);
        let mut per: Vec<Vec<EntryRef>> = vec![Vec::new(); ms.len()];
        for e in &base_entries {
            per[e.origin].push(e.clone());
        }
        let mut next = vec![0usize; per.len()];
        loop {
            let open: Vec<usize> = (0..per.len()).filter(|&o| next[o] < per[o].len()).collect();
            if open.is_empty() {
                break;
            }
            let o = open[w.rng.below(open.len())];
            let e = per[o][next[o]].clone();
            next[o] += 1;
            let servers: Vec<usize> =
                live.iter().copied().filter(|&j| ms[j].appended.contains(&e.idx)).collect();
            if servers.is_empty() {
                if w.held_uncovered && live.iter().any(|&j| ms[j].held_oop.contains(&e.idx)) {
                    // as `take_quiesce_replica`: a live member holds it out of position, so it is not witnessed away. HB-2's
                    // two-replay sub-case (seeds 101, 730): this replay witnessed it while the stamp-order one did not; by the
                    // design text (FIX-1: a held entry raises no vector; the FIX-5 ruling: no member downstream witnesses a held
                    // entry away) the stamp-order replay is right
                    continue;
                }
                z.cv.add(e.idx);
                z.vc.add(e.idx);
                z.raise(w, e.idx);
                z.witness(&e);
            } else {
                let src = servers[w.rng.below(servers.len())];
                z.take_from(w, &ms[src], &e, true);
            }
        }
        z.settle(w);
        let recs = bk(&z);
        let raw = replay_bad(&z.state(), &recs, &m0state);
        div_interleaved_raw |= raw;
        if replay_bad(&without(&z.state(), &hb2), &recs, &m0p) {
            div_interleaved = true;
        } else if raw {
            hb2_excluded = true;
        }
    }
    res.insert("div_replay_interleaved".into(), div_interleaved as i64);
    // N3: the model's `diverged` includes the interleaved replay
    if div_interleaved {
        res.insert("diverged".into(), 1);
    }
    res.insert("cnt_hb2_excluded".into(), hb2_excluded as i64);
    // the verdicts of the two replays before the exclusion (HB-2's two-replay sub-case: they must agree)
    res.insert("raw_div_replay".into(), div_replay_raw as i64);
    res.insert("raw_div_replay_interleaved".into(), div_interleaved_raw as i64);
    w.k_final = k_final;
    res
}
