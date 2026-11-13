//! Snapshot joins (`swapmodel6a.py` 1100-1320, from `swapmodel5.py` 967-1181): the payload captured at the first page,
//! the product's grant (the sender's contiguous coverage), pages applied as state for a physical alive on the receiver
//! and as history for one the receiver has buried (the seed-885 rule), the register events limited to the grant, the
//! grant recorded when the last page lands, and (design-level, the model never abandons a join) the supersession of an
//! unfinished join by a fresh pull.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::coverage::CoverageOf;
use crate::entry::*;
use crate::member::Member;
use crate::omap::OMap;
use crate::world::{Policy, World};

/// A page of the payload: one physical as the sender held it at the first page.
#[derive(Clone, Debug)]
pub struct PhysSnap {
    pub kind: PKind,
    pub alive: bool,
    pub docs: Docs,
    pub kill: Option<Stamp>,
    pub orphan: bool,
}

/// What a snapshot carries: state, not the oplog, plus the grant.
#[derive(Clone, Debug)]
pub struct Payload {
    pub phys: OMap<String, PhysSnap>,
    pub events: Vec<EntryId>,
    pub loss: OMap<LKey, (Body, String)>,
    pub held: BTreeSet<EntryId>,
    pub creates: Vec<(String, EntryRef)>,
    pub learned: Vec<(String, i64)>,
    pub pend: BTreeMap<String, Stamp>,
    pub abandoned: HashSet<String>,
    pub hlc: i64,
    pub appended: BTreeSet<EntryId>,
    /// the sender's retained entries of buried physicals (F4)
    pub dochist: Vec<EntryId>,
    /// `field`: the merge states of those retained page versions
    pub page_states: Vec<(EntryId, std::rc::Rc<kimmy_core::merge_state::MergeState>)>,
    /// the grant: the sender's contiguous coverage per origin, as served with the FIRST page (ADR-152)
    pub vec: BTreeMap<usize, i64>,
}

/// A snapshot join in progress.
#[derive(Clone, Debug)]
pub struct Join {
    pub src: usize,
    pub pl: Payload,
    pub pages: Vec<String>,
    /// the placeholders this join created (`missing` until the grant lands)
    pub created: BTreeSet<String>,
    pub granted: bool,
    pub begin_events: HashSet<EntryId>,
    pub dead_pages: Vec<(String, Docs)>,
    pub bare_ids: HashSet<EntryId>,
    /// a scoped repair (the product's, document rows only): the last page lands and nothing completes: no grant, no coverage
    pub repair: bool,
}

impl Member {
    /// `field`: raise the clock by the version vectors of page states joined here (witness, no tick: the product's HLC
    /// rule, design note Q-F6), so the next local write ticks strictly above everything merged.
    pub fn witness_docs(&mut self, w: &World, docs: &Docs) {
        if w.f.fm_witness_off {
            return; // MUTANT: the page state's vv is not witnessed
        }
        for v in docs.values() {
            if w.policy == Policy::Document {
                self.hlc = self.hlc.max(v.stamp.hlc.wall_ms as i64);
            } else if let Some(st) = &v.state {
                for h in st.vv.values() {
                    self.hlc = self.hlc.max(h.wall_ms as i64);
                }
            }
        }
    }

    /// `field`: keep the merge state of a page version that became a retained entry (joined with what is kept).
    fn keep_page_state(&mut self, w: &World, eid: EntryId, v: &Ver) {
        if w.policy != Policy::Field {
            return;
        }
        if let Some(st) = &v.state {
            self.install_page_state(w, eid, st.clone());
        }
    }

    fn install_page_state(
        &mut self,
        w: &World,
        eid: EntryId,
        st: std::rc::Rc<kimmy_core::merge_state::MergeState>,
    ) {
        let merged = match self.page_states.get(&eid) {
            Some(old) => std::rc::Rc::new(kimmy_core::merge_state::join(old, &st, &w.fopts).0),
            None => st,
        };
        self.page_states.insert(eid, merged);
    }

    /// A member can serve a snapshot when it has no join of its own in progress, nothing deferred and no placeholder.
    pub fn eligible_source(&self) -> bool {
        self.join.is_none() && self.deferred.is_empty() && !self.phys.values().any(|p| p.missing)
    }

    pub fn make_payload(&self, w: &World) -> Payload {
        let f = w.f;
        let phys: OMap<String, PhysSnap> = {
            let mut m = OMap::new();
            for (n, p) in self.phys.iter() {
                m.insert(
                    n.clone(),
                    PhysSnap {
                        kind: p.kind,
                        alive: p.alive,
                        docs: p.docs.clone(),
                        kill: p.kill,
                        orphan: p.orphan,
                    },
                );
            }
            m
        };
        let loss = if f.snap_no_loss { OMap::new() } else { self.loss_records() };
        let dochist: Vec<EntryId> = if f.snap_late_entries {
            self.dochist
                .iter()
                .copied()
                .filter(|&i| self.phys.get(&w.ent(i).phys).is_some_and(|p| !p.alive))
                .collect()
        } else {
            Vec::new()
        };
        Payload {
            phys,
            events: if f.snap_no_events { Vec::new() } else { self.evl.clone() },
            loss,
            held: self
                .cv
                .iter()
                .filter(|i| !(w.held_uncovered && self.held_oop.contains(i)))
                .collect(),
            creates: self.creates.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            learned: self.learned.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            pend: self.pend_tombs.clone(),
            abandoned: self.abandoned.clone(),
            hlc: self.hlc,
            appended: self.appended.clone(),
            page_states: dochist
                .iter()
                .filter_map(|i| self.page_states.get(i).map(|st| (*i, st.clone())))
                .collect(),
            dochist,
            vec: BTreeMap::new(),
        }
    }

    /// The records of this member's state (pending ones too), with the physical of the lost write: what a snapshot carries.
    pub fn loss_records(&self) -> OMap<LKey, (Body, String)> {
        let mut loss: OMap<LKey, (Body, String)> = OMap::new();
        for (k, b) in self.loss_pending.iter().chain(self.loss.iter()) {
            loss.insert(*k, (b.clone(), self.loss_phys[k].clone()));
        }
        loss
    }

    /// Begin a snapshot join from `src`. Returns false (with no side effect on the join) when this member already has
    /// a join in progress, `src` is not an eligible source, or `src` is this member. A join is resumed only from its
    /// own sending peer: a begin from another peer while one is in progress is refused, never merged (D3 invariant).
    pub fn snapshot_begin(&mut self, w: &mut World, src: &Member, lazy: bool) -> bool {
        let f = w.f;
        if f.stale_src_bug {
            self.snap_src_stale = Some(src.id);
        }
        if self.join.is_some() || !src.eligible_source() || src.id == self.id {
            return false;
        }
        let mut pl = src.make_payload(w);
        w.stats.bump("snap");
        w.snap_carried.push(pl.dochist.len());
        if !pl.loss.is_empty() {
            w.stats.bump("snap_carrying_records");
        }
        w.stats.add("snap_entries_not_carried", src.dochist.len() as i64 - pl.dochist.len() as i64);
        if let Some(c) = &src.cur
            && c.kind == Kind::Bind
            && let Some(sp) = src.phys.get(&c.phys)
            && sp.docs.values().any(|v| v.stamp > c.stamp)
        {
            w.stats.bump("snap_after_postflip_write");
        }
        let mut pages: Vec<String> =
            pl.phys.iter().filter(|(_, pd)| pd.alive).map(|(n, _)| n.clone()).collect();
        match w.forced_pages.take() {
            Some(order) => {
                let (mut a, mut b) = (order.clone(), pages.clone());
                a.sort();
                b.sort();
                assert_eq!(a, b, "the replayed page order is a permutation of the alive pages");
                pages = order;
            }
            None => {
                // the Python replay takes the resulting order as one `P` line, not the draws inside the shuffle
                crate::rng::MUTE.with(|m| m.set(true));
                shuffle(&mut w.rng, &mut pages);
                crate::rng::MUTE.with(|m| m.set(false));
                crate::rng::record(format!("P {}", pages.join(",")));
            }
        }
        let mut vec: BTreeMap<usize, i64> = BTreeMap::new();
        if f.grant_prefix {
            // the product's grant (invariant (i)): the sender's CONTIGUOUS coverage per origin, and only that prefix of
            // its held set is granted; an entry the sender holds out of position is neither claimed nor served
            for (o, lst) in w.by_origin.iter().enumerate() {
                let mut top = -1;
                if w.vector_cov {
                    // B2: the product's grant is the sender's servable VECTOR as served with the first page; invariant (i) is then
                    // CHECKED below against the sender's true contiguous coverage, not given by construction
                    top = src.vc.max_of(o);
                } else {
                    for &x in lst {
                        if !pl.held.contains(&x) {
                            break;
                        }
                        top = w.ent(x).hlc;
                    }
                }
                if top >= 0 {
                    vec.insert(o, top);
                }
            }
            let g: BTreeSet<EntryId> = pl
                .held
                .iter()
                .copied()
                .filter(|&i| w.ent(i).hlc <= vec.get(&w.ent(i).origin).copied().unwrap_or(-1))
                .collect();
            w.stats.add("grant_entries_withheld_above_coverage", (pl.held.len() - g.len()) as i64);
            pl.appended = pl.appended.intersection(&g).copied().collect();
            if f.loss_grant_filter && !pl.loss.is_empty() {
                // 6r: a snapshot carries a record only if some entry of that record is within the grant (D3's register-event
                // rule, for records)
                let mut keep: OMap<LKey, (Body, String)> = OMap::new();
                for (k, v) in pl.loss.iter() {
                    if w.loss_entries_of(k).iter().any(|i| g.contains(i)) {
                        keep.insert(*k, v.clone());
                    } else {
                        w.stats.bump("snap_records_not_carried_above_grant");
                    }
                }
                pl.loss = keep;
            }
            pl.held = g;
        } else {
            for &i in &pl.held {
                let x = w.ent(i);
                if x.hlc > vec.get(&x.origin).copied().unwrap_or(-1) {
                    vec.insert(x.origin, x.hlc);
                }
            }
        }
        pl.vec = vec;
        // invariant (i), checked against the sender's own coverage (`covers`, not the prefix loop above): no grant and
        // no page names an entry above the sender's contiguous coverage
        // (with the vector coverage, against the sender's contiguous servable prefix: the vector is what is being checked)
        let truth = |w: &World, o: usize, h: i64| {
            if w.vector_cov { src.covers_servable(w, o, h) } else { src.covers(w, o, h) }
        };
        let grant_above = pl.held.iter().any(|&i| !truth(w, w.ent(i).origin, w.ent(i).hlc))
            || pl.vec.iter().any(|(&o, &h)| !truth(w, o, h));
        if grant_above {
            w.stats.bump("grant_above_coverage");
        }
        // a page version whose entry the sender does not cover: in the model's routes a violation of invariant (i); on the product
        // routes (`held_uncovered`) it is state the sender holds above the vector it advertises (written by a snapshot page, a carried
        // delete or a scoped repair: `docs/operations.md:1423`), carried and never claimed, counted `page_carries_held_state`
        let mut page_above = false;
        for (n, pd) in pl.phys.iter() {
            for (k, v) in pd.docs.iter() {
                let uncovered = w
                    .rec_index
                    .get(&(n.clone(), *k, v.stamp))
                    .is_some_and(|&i| !truth(w, w.ent(i).origin, w.ent(i).hlc));
                if uncovered {
                    if w.held_uncovered {
                        w.stats.bump("page_carries_held_state");
                    } else {
                        page_above = true;
                    }
                }
            }
        }
        if page_above {
            w.stats.bump("page_above_coverage");
        }
        let begin_events: HashSet<EntryId> = pl.events.iter().copied().collect();
        w.note(format!(
            "{} begins a snapshot join from {} lazy={} pages={}",
            self.name,
            src.name,
            lazy,
            pages.len()
        ));
        self.join = Some(Join {
            src: src.id,
            pl,
            pages,
            created: std::mem::take(&mut self.carry_created),
            granted: false,
            begin_events,
            dead_pages: Vec::new(),
            bare_ids: HashSet::new(),
            repair: false,
        });
        let src_evl = GrantRead {
            evl: src.evl.clone(),
            loss: src.loss_records(),
            dead: src.dead_physicals(),
        };
        if f.grant_before_pages {
            let mut j = self.join.take().unwrap();
            self.apply_grant(w, &mut j, &src_evl); // MUTANT: the grant lands before the pages
            self.join = Some(j);
            self.settle(w);
        }
        if !lazy {
            while self.join.as_ref().is_some_and(|j| !j.pages.is_empty()) {
                self.land_page(w, &src_evl);
            }
        }
        if self.join.as_ref().is_some_and(|j| j.pages.is_empty()) {
            self.complete_join(w, &src_evl);
        }
        true
    }

    /// The product's scoped repair (design note, phase G addition; `live_count.rs:599-608`): the source's document versions of the
    /// physicals this member holds alive are handed over as pages of a snapshot that grants NO coverage and carries no register
    /// events, so what they write stays held as state (`held_records`, the pages' retained versions) until the entries arrive in
    /// position. ASSUMED to be the whole of what a repair does in the harness. Returns false when nothing was repaired.
    pub fn scoped_repair(&mut self, w: &mut World, src: &Member) -> bool {
        if self.join.is_some() || !src.eligible_source() || src.id == self.id {
            return false;
        }
        let mut pl = src.make_payload(w);
        // R1: the scoped repair carries none of the Q6 kinds: the receiver's creates, abandons, register, burials and
        // visible event are what they were
        let before = self.q6_surface();
        let pages: Vec<String> = pl
            .phys
            .iter()
            .filter(|(n, pd)| pd.alive && self.phys.get(*n).is_some_and(|p| p.alive && !p.missing))
            .map(|(n, _)| n.clone())
            .collect();
        if pages.is_empty() {
            return false;
        }
        // no grant: nothing claimed, nothing served, no register events, no loss records, no creates
        pl.vec = BTreeMap::new();
        pl.held = BTreeSet::new();
        pl.appended = BTreeSet::new();
        pl.events = Vec::new();
        pl.creates = Vec::new();
        pl.abandoned = HashSet::new();
        pl.loss = OMap::new();
        pl.dochist = Vec::new();
        pl.page_states = Vec::new();
        let n = pages.len();
        self.join = Some(Join {
            src: src.id,
            pl,
            pages,
            created: BTreeSet::new(),
            granted: false,
            begin_events: HashSet::new(),
            dead_pages: Vec::new(),
            bare_ids: HashSet::new(),
            repair: true,
        });
        for _ in 0..n {
            self.land_page(w, &GrantRead::default());
        }
        self.join = None;
        if w.f.fm_repair_creates {
            // MUTANT (`m-repair-carries-creates`): the sender's stage creates and abandons ride along
            for (k, v) in &src.creates {
                self.creates.entry(k.clone()).or_insert_with(|| v.clone());
            }
            self.abandoned.extend(src.abandoned.iter().cloned());
        }
        if w.f.fm_repair_events {
            // MUTANT (`m-repair-carries-events`): the sender's register is handed over
            for &i in &src.evl {
                let e = w.ent_rc(i);
                self.add_event(w, &e);
            }
        }
        if w.f.fm_repair_dropped {
            // MUTANT (`m-repair-dropped-answer`): the sender's `dropped` answer buries what it holds dead
            for (n, pd) in src.phys.iter() {
                if !pd.alive
                    && let Some(p) = self.phys.get_mut(n)
                {
                    p.alive = false;
                }
            }
        }
        if self.q6_surface() != before {
            w.stats.bump("route_carries_q6_kind");
            self.inv.insert("route_carries_q6_kind");
        }
        true
    }

    /// What a route that carries no Q6 kind leaves alone: the creates and abandons, the register (events and the ones held as
    /// information), the records held as information, the physicals' burials and births, and the visible event.
    pub fn q6_surface(&self) -> String {
        format!(
            "{:?}",
            (
                self.creates.keys().cloned().collect::<BTreeSet<_>>(),
                self.abandoned.iter().cloned().collect::<BTreeSet<_>>(),
                self.events.iter().copied().collect::<BTreeSet<_>>(),
                self.info.keys().copied().collect::<BTreeSet<_>>(),
                self.loss_info.keys().copied().collect::<BTreeSet<_>>(),
                self.phys.iter().map(|(n, p)| (n.clone(), p.alive)).collect::<BTreeSet<_>>(),
                self.bmap.iter().map(|(n, s)| (n.clone(), *s)).collect::<BTreeSet<_>>(),
                self.cur.as_ref().map(|c| c.idx),
            )
        )
    }

    /// The physicals this member has buried by a register event (a drop or a bind out), with the stamp that buried them: what a
    /// page read now conveys about them (H-Q6-R4 / R4b).
    pub fn dead_physicals(&self) -> Vec<(String, Stamp)> {
        self.phys
            .iter()
            .filter(|(_, p)| !p.alive && !p.orphan && p.kill.is_some())
            .map(|(n, p)| (n.clone(), p.kill.unwrap()))
            .collect()
    }

    /// The Q6 entries this member knows: the stages it has a create for, the stages it knows abandoned, the register events in
    /// effect or held as information, the records held as information (M2: what a route that carries no Q6 kind cannot make grow).
    #[allow(clippy::type_complexity)]
    pub fn q6_known(
        &self,
    ) -> (BTreeSet<String>, BTreeSet<String>, BTreeSet<EntryId>, BTreeSet<EntryId>) {
        (
            self.creates.keys().cloned().collect(),
            self.abandoned.iter().cloned().collect(),
            self.events.iter().copied().chain(self.info.keys().copied()).collect(),
            self.loss_info.keys().copied().collect(),
        )
    }

    /// Some set of `q6_known` grew since `before`.
    #[allow(clippy::type_complexity)]
    pub fn q6_known_grew_from(
        &self,
        before: &(BTreeSet<String>, BTreeSet<String>, BTreeSet<EntryId>, BTreeSet<EntryId>),
    ) -> bool {
        let now = self.q6_known();
        !now.0.is_subset(&before.0)
            || !now.1.is_subset(&before.1)
            || !now.2.is_subset(&before.2)
            || !now.3.is_subset(&before.3)
    }

    /// The member whose register the grant reads (the sender; `stale_src_bug` reads the source of the last begin call).
    pub fn grant_source(&self, w: &World) -> Option<usize> {
        let j = self.join.as_ref()?;
        if w.f.stale_src_bug { Some(self.snap_src_stale.unwrap_or(j.src)) } else { Some(j.src) }
    }

    /// Land the next page of the join in progress. `src_evl`: the register events of `grant_source()` (read when the
    /// last page lands). Returns false when there is no join.
    pub fn land_page(&mut self, w: &mut World, src_evl: &GrantRead) -> bool {
        let Some(mut j) = self.join.take() else { return false };
        if j.pages.is_empty() {
            self.join = Some(j);
            self.complete_join(w, src_evl);
            return true;
        }
        let f = w.f;
        let name = j.pages.pop().unwrap();
        let pd = j.pl.phys.get(&name).expect("a page of the payload").clone();
        let ungranted = !j.granted;
        let dp_state = self.phys.get(&name).map(|p| p.alive);
        if w.policy == Policy::Document && w.witness_pages && matches!(dp_state, None | Some(true))
        {
            // the metamorphic runs: the document policy witnesses a page's versions' stamps as the field policy does its
            // states' version vectors (equal on a workload where each document's latest write is its greatest stamp), and for
            // the same pages: those of a physical not held here or alive here (the field policy does not witness a dead one's)
            self.witness_docs(w, &pd.docs);
        }
        match dp_state {
            None => {
                let mut dp = Phys::new(pd.kind);
                dp.docs = pd.docs.clone();
                if w.policy == Policy::Field {
                    self.witness_docs(w, &pd.docs);
                    let recs: Vec<(u32, Vec<_>)> =
                        pd.docs.iter().map(|(k, v)| (*k, crate::field::page_records(v))).collect();
                    for (k, fresh) in recs {
                        self.mint_q7(w, &name, k, &fresh);
                    }
                }
                if ungranted {
                    for (k, v) in pd.docs.iter() {
                        self.held_records.insert((name.clone(), *k, v.stamp)); // a page's record: state, out of position
                    }
                    dp.missing = true;
                    j.created.insert(name.clone());
                }
                self.phys.insert(name.clone(), dp);
                self.page_entries(w, &name, &pd.docs, &mut j);
            }
            Some(true) if w.policy == Policy::Field => {
                self.witness_docs(w, &pd.docs);
                for (k, v) in pd.docs.iter() {
                    let cur_stamp = self.phys.get(&name).unwrap().docs.get(k).map(|c| c.stamp);
                    if ungranted && cur_stamp.is_none_or(|c| v.stamp > c) {
                        self.held_records.insert((name.clone(), *k, v.stamp)); // as the document policy's held record
                    }
                    let (merged, fresh) = match self.phys.get(&name).unwrap().docs.get(k) {
                        Some(cur) => crate::field::merge_page(cur, v, ungranted, &w.fopts),
                        None => {
                            // a document this member did not hold: while the join is unfinished the entries delivered in
                            // position have built nothing for it yet
                            let mut nv = v.clone();
                            if ungranted {
                                nv.applied = Some(std::rc::Rc::new(
                                    kimmy_core::merge_state::MergeState::default(),
                                ));
                            }
                            let fresh = crate::field::page_records(&nv);
                            (nv, fresh)
                        }
                    };
                    self.phys.get_mut(&name).unwrap().docs.insert(*k, merged);
                    self.mint_q7(w, &name, *k, &fresh);
                }
                self.page_entries(w, &name, &pd.docs, &mut j);
            }
            Some(true) => {
                for (k, v) in pd.docs.iter() {
                    let cur = self.phys.get(&name).unwrap().docs.get(k).map(|c| c.stamp);
                    if ungranted && cur.is_none_or(|c| v.stamp > c) {
                        self.held_records.insert((name.clone(), *k, v.stamp));
                    }
                    let newer = cur.is_none_or(|c| v.stamp > c);
                    if newer {
                        self.phys.get_mut(&name).unwrap().docs.insert(*k, v.clone());
                    }
                }
                self.page_entries(w, &name, &pd.docs, &mut j);
            }
            Some(false) => {
                if f.state_scan {
                    j.dead_pages.push((name.clone(), pd.docs.clone())); // evaluated when the grant lands
                    if f.dead_page_retained {
                        // seed 885: a page of a physical BURIED HERE is history on arrival: each non-staging version (a
                        // delete with its _id) is retained (F4), so D11 evaluates it at once and on every later register
                        // event, and this member's own snapshots carry it onward
                        for (k, v) in pd.docs.iter() {
                            let Some(&eid) = w.rec_index.get(&(name.clone(), *k, v.stamp)) else {
                                continue;
                            };
                            if w.ent(eid).staging {
                                continue;
                            }
                            if v.body.is_none() && f.snap_delete_bare {
                                continue; // m-snap-delete-bare: ADR-228's bare tombstone has no _id and no entry
                            }
                            if !self.dochist.contains(&eid) {
                                self.dochist.insert(eid);
                                self.hist_kept.insert(eid);
                                w.stats.bump("dead_page_versions_retained");
                            }
                            self.keep_page_state(w, eid, v);
                        }
                    }
                }
            }
        }
        let done = j.pages.is_empty() && !j.repair;
        self.join = Some(j);
        if done {
            self.complete_join(w, src_evl);
        }
        true
    }

    /// gap 2: the product rebuilds a snapshot's current versions as held entries; for a physical of a once-swapped name
    /// a DELETED document carries its _id and the delete's stamp as an entry too. m-snap-delete-bare: ADR-228's bare
    /// tombstone, no _id, no entry.
    fn page_entries(&mut self, w: &mut World, name: &str, docs: &Docs, j: &mut Join) {
        let f = w.f;
        if !f.snap_versions_as_entries {
            return;
        }
        for (k, v) in docs.iter() {
            let Some(&eid) = w.rec_index.get(&(name.to_string(), *k, v.stamp)) else { continue };
            if w.ent(eid).staging {
                continue;
            }
            if v.body.is_none() && f.snap_delete_bare {
                if !self.cv.has(eid) && !self.dochist.contains(&eid) {
                    j.bare_ids.insert(eid);
                    self.bare_state.insert((name.to_string(), *k, v.stamp));
                }
                continue;
            }
            if !self.dochist.contains(&eid) {
                self.dochist.insert(eid);
                w.stats.bump("snap_versions_as_entries");
                if v.body.is_none() {
                    w.stats.bump("snap_deletes_carried_with_id");
                }
            }
            self.keep_page_state(w, eid, v);
        }
    }

    /// The grant lands: one step with the register events, the loss records, the retained entries and the receiver's
    /// own D4 evaluation. D3: the register events are the sender's own, stamped at or below the first page's vector.
    fn apply_grant(&mut self, w: &mut World, j: &mut Join, src_evl: &GrantRead) {
        let f = w.f;
        if j.granted {
            return;
        }
        j.granted = true;
        if f.paged_register_read && !f.snap_no_events && !f.grant_before_pages {
            let cand: Vec<EntryId> = src_evl.evl.clone();
            if f.stale_src_bug && self.grant_source_for(w, j) != j.src {
                w.stats.bump("register_read_from_a_member_that_is_not_the_sender");
                w.note(format!(
                    "{} reads the register of a member that is NOT its sender",
                    self.name
                ));
            }
            let vec = &j.pl.vec;
            let at_or_below =
                |i: &EntryId| w.ent(*i).hlc <= vec.get(&w.ent(*i).origin).copied().unwrap_or(-1);
            let carried: Vec<EntryId> = if f.register_grant_filter {
                cand.iter().copied().filter(at_or_below).collect()
            } else {
                cand.clone()
            };
            let above = cand.iter().filter(|i| !at_or_below(i)).count() as i64;
            if above > 0 {
                w.stats.add("register_events_above_grant", above);
                if f.register_grant_filter {
                    w.stats.add("register_events_not_carried_above_grant", above);
                } else {
                    w.stats.add("register_events_read_above_grant", above);
                }
            }
            if cand.iter().any(|&i| {
                let e = w.ent(i);
                e.kind == Kind::Bind && w.valid(e) && !j.begin_events.contains(&i)
            }) {
                w.stats.bump("bind_between_first_and_last_page");
            }
            j.pl.events = carried;
        }
        if f.paged_loss_read && !f.snap_no_loss && !f.grant_before_pages {
            // 6b (companion clause): the sender's loss records are read when the LAST page lands. Clause: a record is carried
            // only if some entry of it is within the grant (`j.pl.held`, the first page's granted set); a record whose entries
            // are all above it reaches this member by windows.
            let cand = &src_evl.loss;
            w.stats.add("loss_records_read_late", cand.len() as i64);
            let mut ok: OMap<LKey, (Body, String)> = OMap::new();
            for (k, v) in cand.iter() {
                if w.loss_entries_of(k).iter().any(|i| j.pl.held.contains(i)) {
                    ok.insert(*k, v.clone());
                }
            }
            w.stats.add("loss_records_above_grant", (cand.len() - ok.len()) as i64);
            if f.loss_grant_filter {
                j.pl.loss = ok;
            } else {
                w.stats.add("loss_records_read_above_grant", (cand.len() - ok.len()) as i64);
                j.pl.loss = cand.clone();
            }
        }
        let mut rows: HashMap<usize, i64> = HashMap::new();
        for &i in j.pl.held.iter().filter(|&&i| !self.cv.has(i)) {
            let x = w.ent(i);
            if !matches!(x.kind, Kind::Loss | Kind::LossQ7) {
                let r = rows.entry(x.origin).or_insert(-1);
                *r = (*r).max(x.hlc);
            }
        }
        for (o, h) in rows {
            let r = self.state_held.entry(o).or_insert(-1);
            *r = (*r).max(h); // ADR-224's state_held row, written at completion
        }
        for &i in &j.pl.appended {
            if f.snap_record_not_served && matches!(w.ent(i).kind, Kind::Loss | Kind::LossQ7) {
                continue; // m-snap-record-not-served: the carried record is state, not a served entry
            }
            if !j.bare_ids.contains(&i) {
                self.appended.insert(i);
            }
        }
        for &i in &j.pl.held {
            self.cv.add(i);
            self.vc.add(i);
        }
        for (&o, &h) in &j.pl.vec {
            self.vc.raise(o, h); // the completed grant raises the vector to the first page's
        }
        self.hlc = self.hlc.max(j.pl.hlc);
        for (k, v) in &j.pl.creates {
            self.creates.entry(k.clone()).or_insert_with(|| v.clone());
        }
        for (k, v) in &j.pl.learned {
            self.learned.entry(k.clone()).or_insert(*v);
        }
        for &i in &j.pl.events.clone() {
            let e = w.ent_rc(i);
            self.add_event(w, &e);
        }
        if !f.snap_drop_loss {
            for (k, (b, ph)) in j.pl.loss.clone().iter() {
                self.merge_loss(w, *k, b.clone(), ph);
            }
        }
        for &i in &j.pl.dochist {
            if !self.dochist.contains(&i) {
                self.hist_kept.insert(i);
            }
            if !self.dochist.contains(&i) {
                self.dochist.insert(i);
            }
        }
        for (i, st) in j.pl.page_states.clone() {
            self.install_page_state(w, i, st);
        }
        for (k, v) in &j.pl.pend {
            let e = self.pend_tombs.entry(k.clone()).or_insert(*v);
            if *v < *e {
                *e = *v;
            }
        }
        // H-Q6-R4 / R4b: the sender's drops and bound-out physicals read on a page are information only: the receiver buries by its
        // own register (D4), never by a page. The two mutants apply them: `m-page-drop-by-page-versions` a drop stamped above the
        // grant (route B: gated by the page's own versions), `m-page-drop-flag-ignored` every one (the receiver reads the drop as a
        // never-swapped name's, ADR-231)
        // (R1, M2: judged by the consequence, the receiver's Q6 surface before and after)
        let before = self.q6_surface();
        for (name, kill) in &src_evl.dead {
            if !self.phys.get(name).is_some_and(|p| p.alive) {
                continue;
            }
            let above =
                hlc_of(kill) > j.pl.vec.get(&origin_of_stamp(w, kill)).copied().unwrap_or(-1);
            if (f.fm_page_drop_by_versions && above) || f.fm_page_drop_flag_ignored {
                self.bury(w, name, *kill, "page_drop");
            } else {
                w.stats.bump("page_drop_information_only");
            }
        }
        if self.q6_surface() != before {
            w.stats.bump("route_carries_q6_kind");
            self.inv.insert("route_carries_q6_kind");
        }
        if !f.snap_no_orphan_burials {
            for a in &j.pl.abandoned {
                self.abandoned.insert(a.clone());
            }
        }
        let phys_snaps: Vec<(String, PhysSnap)> =
            j.pl.phys.iter().map(|(n, p)| (n.clone(), p.clone())).collect();
        for (name, pd) in phys_snaps {
            if f.snap_no_orphan_burials && pd.orphan {
                continue; // MUTANT: the pages do not carry the sender's orphan burials
            }
            if !pd.alive && !self.phys.contains_key(&name) {
                // the pages carry tombstone information for the sender's buried physicals; the receiver buries by its
                // own D4 evaluation, a stub only records that the physical is gone
                let mut st = Phys::new(pd.kind);
                st.alive = false;
                st.kill = pd.kill;
                st.orphan = pd.orphan;
                self.phys.insert(name, st);
            } else if !pd.alive && pd.orphan && self.phys.get(&name).is_some_and(|p| p.alive) {
                // the sender's orphan burial is honoured
                let p = self.phys.get_mut(&name).unwrap();
                p.docs = Docs::new();
                p.alive = false;
                p.missing = false;
                p.orphan = true;
                w.stats.bump("orphans_buried");
            }
        }
    }

    fn grant_source_for(&self, w: &World, j: &Join) -> usize {
        if w.f.stale_src_bug { self.snap_src_stale.unwrap_or(j.src) } else { j.src }
    }

    /// The grant is recorded when the LAST page lands, in one step with the register events and the receiver's own D4
    /// evaluation.
    pub fn complete_join(&mut self, w: &mut World, src_evl: &GrantRead) {
        let Some(mut j) = self.join.take() else { return };
        self.apply_grant(w, &mut j, src_evl);
        for n in &j.created {
            if let Some(p) = self.phys.get_mut(n) {
                p.missing = false;
            }
        }
        for (name, docs) in &j.dead_pages {
            // the sender still holds this physical, here it is buried: its late document versions are evaluated (once)
            // like entries judged history on arrival
            if let Some(ks) = self.late_stamp(w, name) {
                for (k, v) in docs.iter() {
                    if v.stamp > ks && w.policy == Policy::Field {
                        for c in v
                            .state
                            .as_ref()
                            .map(|st| crate::field::late_cells(st, ks, None))
                            .unwrap_or_default()
                        {
                            self.report_cell(w, *k, &c, name);
                        }
                    } else if v.stamp > ks {
                        if w.f.snap_delete_bare && v.body.is_none() {
                            let ent = w.rec_index.get(&(name.clone(), *k, v.stamp)).copied();
                            if ent.is_some_and(|e| !self.cv.has(e)) {
                                continue; // a bare tombstone: no _id, no entry
                            }
                        }
                        self.report(w, *k, v.stamp, v.body.clone(), name);
                    }
                }
            }
        }
        if w.policy == Policy::Field {
            // the pages' states are no longer held apart from the entries' (every completion clears the held records)
            let names: Vec<String> = self.phys.keys().cloned().collect();
            for n in names {
                let p = self.phys.get_mut(&n).unwrap();
                let keys: Vec<u32> = p.docs.keys().copied().collect();
                for k in keys {
                    p.docs.get_mut(&k).unwrap().applied = None;
                }
            }
        }
        self.held_records.clear(); // the grant has landed: ADR-224's state_held row governs serving from here
        w.stats.bump("snap_completed");
        self.rescan(w, None);
        self.settle(w);
    }

    /// DESIGN-LEVEL (the model never abandons a join; ADR-272 decision 7): a fresh pull from another peer supersedes an
    /// unfinished join. The old join is dropped and the fresh pull starts from page one. The placeholders the old join
    /// created (physicals still `missing`) are RE-FILLED by the new pull's pages and completed by its grant (they join
    /// the new join's `created` set), not removed: rows the member received by windows while the placeholder was
    /// missing are applied there and a removal would lose them, while the grant covers the entries that carried them.
    /// The held records stay until the new join completes (every completion clears them). Both are readings of "cleared,
    /// or re-filled" that the design text leaves open; see the report. Returns false when there is no join to
    /// supersede or the new source cannot serve.
    pub fn supersede_join(&mut self, w: &mut World, src: &Member, lazy: bool) -> bool {
        let Some(old) = self.join.take() else { return false };
        if !src.eligible_source() || src.id == self.id || src.id == old.src {
            self.join = Some(old);
            return false;
        }
        self.carry_created = old
            .created
            .iter()
            .filter(|n| self.phys.get(*n).is_some_and(|p| p.missing))
            .cloned()
            .collect();
        w.stats.bump("join_superseded");
        w.note(format!(
            "{} supersedes its unfinished join from {} with a fresh pull from {}",
            self.name, old.src, src.name
        ));
        self.snapshot_begin(w, src, lazy)
    }
}

/// The origin (member index) of a stamp's writer.
pub fn origin_of_stamp(w: &World, s: &Stamp) -> usize {
    let o = s.writer.node.to_bytes()[0].wrapping_sub(b'A') as usize;
    o.min(w.by_origin.len().saturating_sub(1))
}

/// `random.shuffle`.
pub fn shuffle<T>(rng: &mut crate::rng::Rng, v: &mut [T]) {
    for i in (1..v.len()).rev() {
        let j = rng.below(i + 1);
        v.swap(i, j);
    }
}

/// Land the next page of `dst`'s join (the grant's register is read from `grant_source`), in a cluster.
pub fn land_page_in(ms: &mut [Member], dst: usize, w: &mut World) -> bool {
    let evl = grant_evl(ms, dst, w);
    ms[dst].land_page(w, &evl)
}

/// What the sender (and, with `stale_src_bug`, the member the register is read from) offers at the last page: the register
/// events of `grant_source()` and the loss records of the sender (the paged loss read).
#[derive(Default)]
pub struct GrantRead {
    pub evl: Vec<EntryId>,
    pub loss: OMap<LKey, (Body, String)>,
    /// H-Q6-R4 / R4b: the sender's buried physicals as a later page reads them (pages are read live over rounds in the product;
    /// the harness' pages are otherwise frozen at the first page): (name, the stamp that buried it). Each is a drop or a bound-out
    /// physical of a once-swapped name, flagged as such by the sender; it is INFORMATION only here (the register governs burials).
    pub dead: Vec<(String, Stamp)>,
}

/// The grant's reads of `dst`'s join (empty without a join).
pub fn grant_evl(ms: &[Member], dst: usize, w: &World) -> GrantRead {
    match (ms[dst].grant_source(w), ms[dst].join.as_ref()) {
        (Some(s), Some(j)) => GrantRead {
            evl: ms[s].evl.clone(),
            loss: ms[j.src].loss_records(),
            dead: ms[j.src].dead_physicals(),
        },
        _ => GrantRead::default(),
    }
}

/// Complete `dst`'s join (all pages landed or none left).
pub fn complete_join_in(ms: &mut [Member], dst: usize, w: &mut World) {
    let evl = grant_evl(ms, dst, w);
    ms[dst].complete_join(w, &evl);
}

/// `dst` begins a snapshot join from `src` in a cluster.
pub fn snapshot_begin_in(
    ms: &mut [Member],
    dst: usize,
    src: usize,
    w: &mut World,
    lazy: bool,
) -> bool {
    if dst == src {
        if w.f.stale_src_bug {
            ms[dst].snap_src_stale = Some(src);
        }
        return false;
    }
    let (d, s) = crate::relay::pair(ms, dst, src);
    d.snapshot_begin(w, s, lazy)
}
