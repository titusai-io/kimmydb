//! The register and the flip machinery (`swapmodel5.py` 375-430 and 773-930): events, visibility, the bound-out
//! burial, the orphan rules, `settle`.

use crate::entry::*;
use crate::member::Member;
use crate::world::{Policy, World};

impl Member {
    // ---- the register
    pub fn member_valid(&self, w: &World, e: &Entry) -> bool {
        w.f.orphan_no_deadline || w.valid(e)
    }

    pub fn add_event(&mut self, w: &mut World, e: &Entry) -> bool {
        if self.events.contains(&e.idx) {
            return false;
        }
        if w.f.rule_b && !self.covers(w, e.origin, e.hlc) {
            // rule B: the event is ahead of its position here: information, no effect until the member covers it
            if !self.info.contains_key(&e.idx) {
                self.info.insert(e.idx, w.ent_rc(e.idx));
                w.stats.bump("info_parked");
            }
            return false;
        }
        if self.info.remove(&e.idx).is_some() {
            w.stats.bump("info_taken_effect");
        }
        self.events.insert(e.idx);
        let pos = self.evl.partition_point(|&i| w.ent(i).stamp <= e.stamp);
        self.evl.insert(pos, e.idx);
        if matches!(e.kind, Kind::Bind | Kind::Lcreate)
            && !(e.kind == Kind::Lcreate && w.f.recreate_reuse)
        {
            // M1: the birth of a physical is the stamp of the event that made it, set once
            self.bmap.entry(e.phys.clone()).or_insert(e.stamp);
        }
        true
    }

    /// The physical this member serves: the identity, the bound physical, or `None` after a logical drop.
    pub fn serving(&self) -> Option<String> {
        match &self.cur {
            None => Some(L.to_string()),
            Some(c) if c.kind == Kind::Ldrop => None,
            Some(c) => Some(c.phys.clone()),
        }
    }

    /// D11: the earliest known valid register event that supersedes the physical (stamp above its birth, an
    /// ldrop or an event naming another physical), or `None`.
    pub fn first_superseding(&self, w: &World, name: &str) -> Option<EntryRef> {
        let b = *self.bmap.get(name)?;
        for &id in &self.evl {
            let ev = w.ent(id);
            if ev.stamp > b && (ev.kind == Kind::Ldrop || ev.phys != name) {
                return Some(w.ent_rc(id));
            }
        }
        None
    }

    pub fn late_stamp(&self, w: &World, name: &str) -> Option<Stamp> {
        self.first_superseding(w, name).map(|e| e.stamp)
    }

    /// The earliest valid register event held as information that would supersede the physical (rule B), or `None`.
    pub fn info_superseding(&self, w: &World, name: &str) -> Option<EntryRef> {
        let b = *self.bmap.get(name)?;
        let mut xs: Vec<&EntryRef> = self.info.values().collect();
        xs.sort_by_key(|x| x.stamp);
        xs.into_iter()
            .find(|x| x.stamp > b && (x.kind == Kind::Ldrop || x.phys != name) && w.valid(x))
            .cloned()
    }

    /// Rule B: an event held as information whose origin the member now covers takes effect, as an arrival (D11's rescan).
    pub fn activate_info(&mut self, w: &mut World) -> bool {
        if self.info.is_empty() {
            return false;
        }
        let mut prog = false;
        let mut xs: Vec<EntryRef> = self.info.values().cloned().collect();
        xs.sort_by_key(|x| x.stamp);
        for e in xs {
            if self.covers(w, e.origin, e.hlc) {
                if self.add_event(w, &e) {
                    if e.kind == Kind::Lcreate && !self.phys.contains_key(&e.phys) {
                        self.phys.insert(e.phys.clone(), Phys::new(PKind::Rc));
                    }
                    self.rescan(w, None);
                    prog = true;
                } else {
                    self.info.remove(&e.idx);
                }
            }
        }
        prog
    }

    /// v2.1's R5 (kept for m-pending-narrow): a valid binding above the visible event whose stage is not buried.
    pub fn pending(&self, w: &World) -> bool {
        let curst = self.cur.as_ref().map_or_else(neg, |c| c.stamp);
        for &id in &self.evl {
            let ev = w.ent(id);
            if ev.kind == Kind::Bind && ev.stamp > curst {
                match self.phys.get(&ev.phys) {
                    None => return true,
                    Some(p) if p.alive => return true,
                    _ => {}
                }
            }
        }
        false
    }

    /// M3 (D10 as corrected): refuse whenever any known valid register event supersedes the served physical.
    pub fn refusal_cause(&self, w: &World, s: &str) -> Option<&'static str> {
        let f = w.f;
        if f.pend_accepts {
            return None;
        }
        if f.pend_narrow {
            return if self.pending(w) { Some("bind") } else { None };
        }
        // rule B / D10: "known" includes an event held as information ahead of its position
        let x = self.first_superseding(w, s).or_else(|| self.info_superseding(w, s));
        x.map(|ev| ev.kind.name())
    }

    // ---- R3, R4, R7, R9: the flip machinery
    pub fn newest_applicable(&mut self, w: &mut World) -> Option<EntryRef> {
        let f = w.f;
        let curst = self.cur.as_ref().map_or_else(neg, |c| c.stamp);
        let ids: Vec<EntryId> = self.evl.iter().rev().copied().collect();
        for id in ids {
            let ev = w.ent_rc(id);
            if ev.stamp <= curst {
                break;
            }
            if !f.visible_newest && !self.covers(w, ev.origin, ev.hlc) {
                continue;
            }
            if matches!(ev.kind, Kind::Bind | Kind::Lcreate) {
                let p = self.phys.get(&ev.phys);
                if ev.kind == Kind::Lcreate && f.recreate_reuse && p.is_some() {
                    return Some(ev); // m-recreate-reuse: revives the head physical
                }
                let Some(p) = p else { continue };
                if !p.alive {
                    continue;
                }
                if p.missing && !f.bury_without_alive {
                    w.stats.bump("swap_not_here_yet");
                    continue;
                }
                if f.digest && ev.kind == Kind::Bind {
                    let mut live: Vec<(u32, Stamp)> = p
                        .docs
                        .iter()
                        .filter(|(_, v)| v.body.is_some())
                        .map(|(k, v)| (*k, v.stamp))
                        .collect();
                    live.sort();
                    if Extra::Digest(live) != ev.extra {
                        w.stats.bump("digest_pending");
                        continue;
                    }
                }
            }
            return Some(ev);
        }
        None
    }

    pub fn enter(&mut self, w: &mut World, v: &EntryRef) {
        let f = w.f;
        self.cur = Some(v.clone());
        if matches!(v.kind, Kind::Bind | Kind::Lcreate) {
            self.last_served = v.phys.clone();
            if v.kind == Kind::Lcreate && f.recreate_reuse {
                // revive the head physical, floor = the latest drop known below the recreate
                let floor = self
                    .evl
                    .iter()
                    .map(|&i| w.ent(i))
                    .filter(|e| e.kind == Kind::Ldrop && e.stamp < v.stamp)
                    .map(|e| e.stamp)
                    .max()
                    .unwrap_or_else(neg);
                let p = self.phys.get_mut(&v.phys).expect("the recreated physical");
                p.alive = true;
                p.kill = None;
                p.missing = false;
                p.docs.retain(|_, x| x.stamp > floor);
                w.stats.bump("lcreate_entered");
                return;
            }
            let (missing, alive) = {
                let p = self.phys.get(&v.phys).expect("the entered physical");
                (p.missing, p.alive)
            };
            if missing || !alive {
                self.inv.insert("serve_not_alive");
            }
            if v.kind == Kind::Bind {
                self.flips += 1;
                w.stats.bump("flips");
                let p = self.phys.get(&v.phys).expect("the entered physical");
                if let Some(rows) = w.stage_docs.get(&v.phys) {
                    for &sid in rows {
                        let se = w.ent(sid);
                        let c = p.docs.get(&se.key);
                        if c.is_none_or(|c| c.stamp < se.stamp) {
                            self.inv.insert("partial_flip");
                            let have = c.map(|c| (hlc_of(&c.stamp), c.body.clone()));
                            let msg = format!(
                                "{} PARTIAL FLIP into {}: staged row {} (key {}) missing or older; the stage holds {:?}",
                                self.name, v.phys, se, se.key, have
                            );
                            w.note(msg);
                            break;
                        }
                    }
                }
            } else {
                w.stats.bump("lcreate_entered");
                if f.no_recreate_floor {
                    let last = match &v.extra {
                        Extra::Phys(s) => s.clone(),
                        _ => String::new(),
                    };
                    if let Some(h) = self.phys.get(&last) {
                        let src = h.ghost.clone().unwrap_or_else(|| h.docs.clone());
                        let hb = self.bmap.get(&last).copied().unwrap_or_else(neg);
                        let adds: Vec<(u32, Ver)> = src
                            .iter()
                            .filter(|(_, x)| x.stamp > hb && x.body.is_some())
                            .map(|(k, x)| (*k, x.clone()))
                            .collect();
                        let p = self.phys.get_mut(&v.phys).unwrap();
                        for (k, x) in adds {
                            p.docs.insert(k, x);
                        }
                    }
                }
            }
        } else {
            w.stats.bump("ldrop_entered");
            if self.flips > 0 {
                w.stats.bump("ldrop_after_flip");
            }
        }
    }

    /// R4: unconditional burial of a bound-out physical (never kept, never re-dated). m3: the retained writes
    /// addressed to it are re-evaluated now that it is buried.
    pub fn bury(&mut self, w: &mut World, name: &str, fst: Stamp, reason: &str) {
        let f = w.f;
        {
            let p = self.phys.get_mut(name).expect("the buried physical");
            if f.keep && p.docs.iter().any(|(_, v)| v.stamp > fst) {
                p.docs.retain(|_, v| v.stamp > fst);
                p.kept = true;
                p.kill = Some(fst);
                w.stats.bump("kept_by_adr231_path");
                return;
            }
            if f.no_recreate_floor {
                p.ghost = Some(p.docs.clone());
            }
        }
        let old_docs = {
            let p = self.phys.get_mut(name).unwrap();
            let old = std::mem::take(&mut p.docs);
            p.alive = false;
            p.missing = false;
            p.kill = Some(fst);
            old
        };
        if f.state_scan
            && let Some(ks) = self.late_stamp(w, name)
        {
            for (k, v) in old_docs.iter() {
                if w.policy == Policy::Field {
                    // per lost head: every late cell of the version and the losers its join recorded
                    if v.stamp > ks {
                        for c in v
                            .state
                            .as_ref()
                            .map(|st| crate::field::late_cells(st, ks, None))
                            .unwrap_or_default()
                        {
                            self.report_cell(w, *k, &c, name);
                        }
                    }
                } else if v.stamp > ks
                    && !(f.snap_delete_bare
                        && self.bare_state.contains(&(name.to_string(), *k, v.stamp)))
                {
                    self.report(w, *k, v.stamp, v.body.clone(), name);
                }
            }
        }
        w.stats.bump(&format!("buried_{reason}"));
        let is_stage = self.phys.get(name).is_some_and(|p| p.kind == PKind::Stage);
        if is_stage && reason == "sweep" {
            w.stats.bump("loser_stage_buried");
        }
        self.rescan(w, Some(name));
    }

    pub fn sweep(&mut self, w: &mut World) -> bool {
        let Some(cur) = self.cur.clone() else { return false };
        let f = w.f;
        let served: Option<String> = if matches!(cur.kind, Kind::Bind | Kind::Lcreate) {
            Some(cur.phys.clone())
        } else {
            None
        };
        let mut prog = false;
        let names: Vec<String> = self.phys.keys().cloned().collect();
        for name in names {
            let (alive, kept, kind) = {
                let p = self.phys.get(&name).unwrap();
                (p.alive, p.kept, p.kind)
            };
            if !alive || kept || Some(&name) == served.as_ref() {
                continue;
            }
            if f.stage_buried_early && kind == PKind::Stage && cur.kind == Kind::Bind {
                self.bury(w, &name, cur.stamp, "early");
                prog = true;
                continue;
            }
            let Some(&b) = self.bmap.get(&name) else { continue }; // R3: a stage with no known binding is never buried here
            if b < cur.stamp {
                self.bury(w, &name, cur.stamp, "sweep");
                prog = true;
            }
        }
        prog
    }

    /// R9/m4: a stage with no known valid binding is buried when its writer abandoned it (the witness), or, as a
    /// safety net, when the member covers the writer up to the deadline with a witness entry beyond it.
    pub fn orphans(&mut self, w: &mut World) -> bool {
        let f = w.f;
        let mut prog = false;
        let names: Vec<String> = self.phys.keys().cloned().collect();
        for name in names {
            let (kind, alive) = {
                let p = self.phys.get(&name).unwrap();
                (p.kind, p.alive)
            };
            if kind != PKind::Stage || !alive || self.bmap.contains_key(&name) {
                continue;
            }
            let Some(cr) = self.creates.get(&name).cloned() else { continue };
            let bury = match f.orphan_age {
                Some(a) => w.step - self.learned.get(&name).copied().unwrap_or(0) > a,
                None => self.abandoned.contains(&name) || self.covers_past(w, cr.origin, cr.dl),
            };
            if bury {
                let p = self.phys.get_mut(&name).unwrap();
                p.docs.clear();
                p.alive = false;
                p.missing = false;
                p.orphan = true;
                w.stats.bump("orphans_buried");
                if self.abandoned.contains(&name) {
                    w.stats.bump("orphans_buried_by_abandon");
                }
                prog = true;
            }
        }
        prog
    }

    pub fn settle(&mut self, w: &mut World) {
        for _ in 0..60 {
            let mut prog = false;
            if self.activate_info(w) {
                prog = true;
            }
            if self.retry_deferred(w) {
                prog = true; // held entries are applied before visibility is judged
            }
            if let Some(v) = self.newest_applicable(w)
                && self.cur.as_ref().is_none_or(|c| v.stamp > c.stamp)
            {
                self.enter(w, &v);
                prog = true;
            }
            if self.sweep(w) {
                prog = true;
            }
            if self.orphans(w) {
                prog = true;
            }
            if self.retry_deferred(w) {
                prog = true;
            }
            if !prog {
                break;
            }
        }
        if !self.loss_pending.is_empty() {
            self.promote_pending(w);
        }
    }
}
