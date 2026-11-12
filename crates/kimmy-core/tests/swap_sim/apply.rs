//! Apply (`swapmodel5.py` 565-691, 754-770): `take`, the append decision, deferred entries, the late-write check.

use crate::coverage::CoverageOf;
use crate::entry::*;
use crate::member::Member;
use crate::world::{Policy, World};

impl Member {
    /// An entry that takes no effect at this member (`m-ignored-event-not-served`: such an entry is not appended).
    pub fn would_be_ignored(&self, w: &World, e: &Entry) -> bool {
        match e.kind {
            Kind::Loss => {
                let k = e.lkey.expect("a loss entry has a key");
                self.loss.contains_key(&k)
                    || self.loss_pending.contains_key(&k)
                    || self.dropped_loss.contains(&k)
            }
            Kind::Create => self.phys.contains_key(&e.phys),
            Kind::Abandon => match self.phys.get(&e.phys) {
                None => true,
                Some(p) => !p.alive || self.bmap.contains_key(&e.phys),
            },
            Kind::Bind => {
                !self.member_valid(w, e) || self.cur.as_ref().is_some_and(|c| e.stamp < c.stamp)
            }
            Kind::Ldrop | Kind::Lcreate => self.cur.as_ref().is_some_and(|c| e.stamp < c.stamp),
            Kind::LossQ7 => {
                let (k, _) = e.q7.as_ref().expect("a Q7 loss entry has its record");
                self.phys
                    .get(&e.phys)
                    .and_then(|p| p.docs.get(&e.key))
                    .and_then(|v| v.state.as_ref())
                    .is_some_and(|s| s.losses.contains_key(k))
            }
            Kind::Doc => false,
        }
    }

    /// The member takes an entry directly (no window, no witnessing).
    pub fn take(&mut self, w: &mut World, e: &EntryRef) {
        if self.cv.has(e.idx) {
            return;
        }
        let f = w.f;
        if e.kind.is_register_event() && e.origin != self.id {
            // invariant (ii): register events travel only in position: every earlier entry of the origin is held here
            let in_position = w.by_origin[e.origin]
                .iter()
                .take_while(|&&x| w.ent(x).hlc < e.hlc)
                .all(|&x| self.cv.has(x));
            if !in_position && w.reg_in_position_check {
                w.stats.bump("register_event_out_of_position");
                self.inv.insert("register_event_out_of_position");
            }
        }
        let ignored = f.ignored_not_served
            && e.kind != Kind::Doc
            && e.origin != self.id
            && self.would_be_ignored(w, e);
        self.cv.add(e.idx);
        self.witness(e);
        if e.kind != Kind::Doc
            && !ignored
            && !(f.fm_loss_not_appended && e.kind == Kind::Loss && e.origin != self.id)
        {
            self.appended.insert(e.idx); // register events, creates, abandons and loss entries are always appended and served onward
        }
        if e.kind == Kind::Doc {
            self.recv_entry.insert(e.idx);
        }
        let mut is_event = false;
        match e.kind {
            Kind::Create => {
                self.creates.insert(e.phys.clone(), e.clone());
                self.learned.entry(e.phys.clone()).or_insert(w.step);
                match self.phys.get_mut(&e.phys) {
                    None => {
                        self.phys.insert(e.phys.clone(), Phys::new(PKind::Stage));
                    }
                    Some(p) => p.kind = PKind::Stage,
                }
            }
            Kind::Abandon => {
                self.abandoned.insert(e.phys.clone());
            }
            Kind::Lcreate => {
                is_event = self.add_event(w, e);
                if !self.phys.contains_key(&e.phys) && (is_event || !w.f.rule_b) {
                    self.phys.insert(e.phys.clone(), Phys::new(PKind::Rc)); // (rule B: made when the event takes effect)
                }
            }
            Kind::Bind => {
                if self.member_valid(w, e) {
                    is_event = self.add_event(w, e);
                    if w.step - self.learned.get(&e.phys).copied().unwrap_or(w.step) > 12 {
                        w.stats.bump("valid_bind_arrives_late");
                    }
                }
            }
            Kind::Ldrop => {
                if f.ldrop_identity_only {
                    self.apply_ldrop_identity_only(w, e);
                } else {
                    is_event = self.add_event(w, e);
                }
            }
            Kind::Loss => {
                let k = e.lkey.expect("a loss entry has a key");
                let phys = match &e.extra {
                    Extra::Phys(p) => p.clone(),
                    _ => String::new(),
                };
                if f.loss_in_position && !self.covers(w, e.origin, e.hlc) {
                    // 6r (seed 1434): a record ahead of its position is information, like a register event under rule B: it
                    // does not enter the state (no event, no snapshot carry, no dedup of this member's own mint) until the
                    // member covers its origin up to it
                    if !self.loss_info.contains_key(&e.idx) {
                        self.loss_info.insert(e.idx, w.ent_rc(e.idx));
                        w.stats.bump("loss_info_parked");
                    }
                } else {
                    self.merge_loss(w, k, e.body.clone(), &phys);
                }
            }
            Kind::LossQ7 => self.apply_loss_q7(w, e),
            Kind::Doc => self.apply_doc(w, e),
        }
        if is_event {
            self.rescan(w, None);
        }
        self.settle(w);
    }

    /// V6M2 (Q7 D10): the member appends its own loss entry, and emits the event once, for every record that first enters its
    /// state through any join (a writer's entry that carries it, a snapshot page, a received loss entry). Off with
    /// `World.q7_entries` (the comparison runs).
    pub fn mint_q7(
        &mut self,
        w: &mut World,
        phys: &str,
        key: u32,
        fresh: &[(kimmy_core::merge_state::LossKey, kimmy_core::merge_state::LossValue)],
    ) {
        if fresh.is_empty() || !w.q7_entries {
            return;
        }
        for (lk, lv) in fresh {
            let h = self.next_hlc(w);
            let mut en = Entry::new(self.id, h, Kind::LossQ7).phys(phys);
            en.key = key;
            en.q7 = Some((lk.clone(), lv.clone()));
            let en = w.record(en, true);
            self.cv.add(en.idx);
            self.appended.insert(en.idx);
            w.stats.bump("q7_loss_entries_minted");
            w.stats.bump("q7_events_emitted");
            if w.trace {
                w.note(format!(
                    "{} mints Q7 loss entry {} ({:?}@{} of key {})",
                    self.name, en, lk.kind, lk.stamp.hlc.wall_ms, key
                ));
            }
        }
    }

    /// A Q7 loss entry: the record enters the document's state (`Writer::receive_loss`: kept unless below the horizon or already
    /// known) and its event is emitted once. An entry for a document or a physical not held here takes no effect (the record
    /// rides with the states that reach this member); it is appended and served onward all the same (D3).
    fn apply_loss_q7(&mut self, w: &mut World, e: &EntryRef) {
        w.stats.bump("q7_loss_entries_received");
        let (lk, lv) = e.q7.clone().expect("a Q7 loss entry has its record");
        if w.trace {
            w.note(format!(
                "{} receives Q7 loss entry {} ({:?}@{} of key {} in {})",
                self.name, e, lk.kind, lk.stamp.hlc.wall_ms, e.key, e.phys
            ));
        }
        let alive = match self.phys.get(&e.phys) {
            None => {
                // the physical is not known here yet: applied when it is (the deferral of document entries)
                if !self.deferred.iter().any(|d| d.idx == e.idx) {
                    self.deferred.push(e.clone());
                }
                return;
            }
            Some(p) => p.alive,
        };
        if !alive {
            return; // a buried physical keeps no documents
        }
        let cur =
            self.phys.get(&e.phys).and_then(|p| p.docs.get(&e.key)).and_then(|v| v.state.clone());
        // a document not held yet gets a state that holds the record alone: grow-only, reads as absent, joins with the real state
        let st = cur.clone().unwrap_or_default();
        let mut wr = kimmy_core::merge_state::Writer::from_state(
            writer(e.origin),
            (*st).clone(),
            w.fopts.clone(),
        );
        if wr.receive_loss(lk, lv) {
            w.stats.bump("q7_events_emitted");
            let ns = std::rc::Rc::new(wr.state);
            let p = self.phys.get_mut(&e.phys).unwrap();
            match p.docs.get_mut(&e.key) {
                Some(ver) => ver.state = Some(ns),
                None => {
                    p.docs.insert(
                        e.key,
                        Ver { stamp: neg(), body: None, state: Some(ns), applied: None },
                    );
                }
            }
        }
    }

    /// Mutant (R7): a logical drop addresses only the physical it names.
    fn apply_ldrop_identity_only(&mut self, w: &mut World, e: &Entry) {
        let (alive, kill) = match self.phys.get(&e.phys) {
            None => return,
            Some(p) => (p.alive, p.kill),
        };
        if alive {
            self.bury(w, &e.phys, e.stamp, "ldrop-identity");
        } else if kill.is_none_or(|k| e.stamp < k) {
            self.phys.get_mut(&e.phys).unwrap().kill = Some(e.stamp);
        }
    }

    pub fn retry_deferred(&mut self, w: &mut World) -> bool {
        let mut prog = false;
        let mut again = true;
        while again {
            again = false;
            let list: Vec<EntryRef> = self.deferred.clone();
            for e in list {
                if self.phys.contains_key(&e.phys) {
                    self.deferred.retain(|d| d.idx != e.idx);
                    self.apply_doc(w, &e);
                    again = true;
                    prog = true;
                }
            }
        }
        prog
    }

    fn lww(p: &mut Phys, key: u32, stamp: Stamp, body: Body) {
        let newer = p.docs.get(&key).is_none_or(|c| stamp > c.stamp);
        if newer {
            p.docs.insert(key, Ver::doc(stamp, body));
        }
    }

    /// Apply a document entry and make the append decision. Product invariant (i) (HB-1 of the B1 classification): an entry
    /// held out of position (`held_oop`, the product's OPLOG_HELD mark) is never servable before it is released in position
    /// (`promote_oop`, the in-window release of `take_from`): when the entry's physical was not known at hold time, `take_hold`'s
    /// removal from `appended` ran before the apply, and the deferred apply (`retry_deferred`) would otherwise append a winner
    /// that is still held. `m-held-servable-on-deferred-apply` keeps that append.
    pub fn apply_doc(&mut self, w: &mut World, e: &EntryRef) {
        self.apply_doc_decide(w, e);
        if w.held_uncovered
            && !w.f.fm_held_servable
            && self.held_oop.contains(&e.idx)
            && self.appended.remove(&e.idx)
        {
            w.stats.bump("held_kept_unservable");
        }
    }

    /// The append decision (the product's relay): a document entry is appended iff it won last-writer-wins on
    /// arrival, or it is the gap-1 case (superseded only by a record held as an unfinished join's page state).
    fn apply_doc_decide(&mut self, w: &mut World, e: &EntryRef) {
        if e.kind == Kind::LossQ7 {
            return self.apply_loss_q7(w, e); // a deferred Q7 loss entry
        }
        let f = w.f;
        let Some(p) = self.phys.get(&e.phys) else { return self.apply_doc_orig(w, e) };
        if !p.alive {
            return self.apply_doc_orig(w, e);
        }
        if w.policy == Policy::Field {
            return self.apply_doc_field(w, e);
        }
        let cur = p.docs.get(&e.key).map(|c| c.stamp);
        let wins = cur.is_none_or(|c| e.stamp >= c); // an equal stamp is the same entry, held as a snapshot's state
        let held_state = p.missing
            || cur.is_some_and(|c| self.held_records.contains(&(e.phys.clone(), e.key, c)));
        let loser_by_uncovered = !wins
            && !(f.append_in_position && held_state)
            && w.held_uncovered
            && cur.is_some_and(|c| self.superseded_by_uncovered(w, &e.phys, e.key, c));
        if loser_by_uncovered {
            w.stats.bump("df1_loser_superseded_by_uncovered");
        }
        self.apply_doc_orig(w, e);
        if wins {
            self.appended.insert(e.idx);
        } else if f.append_in_position && held_state {
            // gap-1 fix: superseded ONLY by a record held as an unfinished snapshot's state: appended in position,
            // servable, no change to the document
            self.appended.insert(e.idx);
            w.stats.bump("appended_in_position");
        } else if e.origin != self.id {
            self.dochist.remove(&e.idx); // superseded on arrival: witnessed, not in the product's oplog, never served
            w.stats.bump("lww_losers_not_kept");
        }
    }

    /// DF-1's structural marker (B1 classification, an open design finding): the version that supersedes an arriving entry is
    /// one this member does not cover (held above its coverage: a held document entry not yet released, or a page version whose
    /// entry is not covered), so the arriving entry is treated as a loser on the strength of a version the member does not cover.
    pub fn superseded_by_uncovered(&self, w: &World, phys: &str, key: u32, stamp: Stamp) -> bool {
        match w.rec_index.get(&(phys.to_string(), key, stamp)) {
            Some(&i) => !self.covers(w, w.ent(i).origin, w.ent(i).hlc),
            None => false,
        }
    }

    /// The `field` policy: the entry's state is joined into the document's (Q7 D11: "if unchanged only witness"); the entry
    /// is appended iff the join changed the state, or (the gap-1 analogue) iff the state of an unfinished join's pages
    /// already covers it; the covered-entry skip decides "witnessed only" before the join.
    fn apply_doc_field(&mut self, w: &mut World, e: &EntryRef) {
        let f = w.f;
        let p = self.phys.get(&e.phys).expect("the physical is alive here");
        let cur_stamp = p.docs.get(&e.key).map(|v| v.stamp);
        let mut a = crate::field::apply_entry(
            p.docs.get(&e.key),
            e,
            &w.fopts,
            p.missing,
            f.fm_stale_version_stamp,
        );
        if f.fm_stale_body
            && a.changed
            && let Some(old) = self.phys.get(&e.phys).and_then(|p| p.docs.get(&e.key))
        {
            // MUTANT: the joined state is kept but the body is not refreshed
            a.ver.body = old.body.clone();
        }
        self.phys.get_mut(&e.phys).unwrap().docs.insert(e.key, a.ver);
        if a.skipped {
            w.stats.bump("covered_skips");
        }
        if !a.fresh.is_empty() {
            w.stats.add("q7_records_fresh", a.fresh.len() as i64);
            self.mint_q7(w, &e.phys, e.key, &a.fresh);
        }
        if !e.staging {
            self.dochist.insert(e.idx); // m1: retained while the physical is alive here
            self.check_late(w, e);
        }
        if !(a.changed
            || f.fm_always_append
            || a.same_write
            || (f.append_in_position && a.held_state))
            && w.held_uncovered
            && cur_stamp.is_some_and(|c| self.superseded_by_uncovered(w, &e.phys, e.key, c))
        {
            w.stats.bump("df1_loser_superseded_by_uncovered");
        }
        if a.changed || f.fm_always_append {
            self.appended.insert(e.idx);
        } else if a.same_write {
            // the entry is the held version itself (stamp equal to the version's): the same entry, a winner (the model's equal stamp),
            // whatever the append-in-position flag
            self.appended.insert(e.idx);
            w.stats.bump("same_write_held");
        } else if f.append_in_position && a.held_state {
            self.appended.insert(e.idx);
            w.stats.bump("appended_in_position");
        } else if e.origin != self.id {
            self.dochist.remove(&e.idx); // unchanged on arrival: witnessed, not in the oplog, never served
            w.stats.bump("lww_losers_not_kept");
        }
    }

    fn apply_doc_orig(&mut self, w: &mut World, e: &EntryRef) {
        let Some(p) = self.phys.get_mut(&e.phys) else {
            if !self.deferred.iter().any(|d| d.idx == e.idx) {
                self.deferred.push(e.clone());
            }
            return;
        };
        if e.staging {
            if p.alive {
                Self::lww(p, e.key, e.stamp, e.body.clone());
            }
            return;
        }
        if p.alive {
            Self::lww(p, e.key, e.stamp, e.body.clone());
            self.dochist.insert(e.idx); // m1: retained while the physical is alive here
            self.check_late(w, e);
            return;
        }
        // R4/R6/m1: addressed to a buried (bound-out) physical: history on arrival
        if w.f.retain_history {
            self.dochist.insert(e.idx);
            self.hist_kept.insert(e.idx);
        } else {
            w.stats.bump("history_arrivals_dropped");
        }
        self.check_late(w, e);
    }

    /// D11, existential form, with m3: the write is late iff a known valid register event supersedes its physical
    /// with a stamp between the physical's birth and the write's stamp; it is REPORTED only once the physical is
    /// buried on this member.
    pub fn check_late(&mut self, w: &mut World, e: &Entry) {
        let Some(ks) = self.late_stamp(w, &e.phys) else { return };
        if e.stamp <= ks {
            return;
        }
        let alive_or_unknown = self.phys.get(&e.phys).is_none_or(|p| p.alive);
        if !w.f.report_before_burial && alive_or_unknown {
            w.stats.bump("report_delayed_until_burial");
            return;
        }
        if w.policy == Policy::Field {
            // per lost head: the entry's own write (the cells stamped with its stamp) and, for a retained page version, every
            // late cell of the page state and the losers its join recorded
            let mut cells = e
                .fstate
                .as_ref()
                .map(|s| crate::field::late_cells(s, ks, Some(e.stamp)))
                .unwrap_or_default();
            if w.f.fm_no_page_loss {
                // MUTANT: a retained page version's late cells are not reported
            } else if let Some(ps) = self.page_states.get(&e.idx).cloned() {
                for c in crate::field::late_cells(&ps, ks, None) {
                    if !cells.contains(&c) {
                        cells.push(c);
                    }
                }
            }
            for mut c in cells {
                if w.f.fm_loss_superseder_stamp {
                    c.stamp = ks; // MUTANT: keyed by the superseding event's stamp
                }
                self.report_cell(w, e.key, &c, &e.phys);
            }
            return;
        }
        self.report(w, e.key, e.stamp, e.body.clone(), &e.phys);
    }

    pub fn rescan(&mut self, w: &mut World, name: Option<&str>) {
        let ids: Vec<EntryId> = self.dochist.iter().copied().collect();
        for id in ids {
            let e = w.ent_rc(id);
            if name.is_none_or(|n| e.phys == n) {
                self.check_late(w, &e);
            }
        }
    }
}
