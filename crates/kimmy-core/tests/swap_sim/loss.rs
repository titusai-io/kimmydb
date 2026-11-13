//! Loss records (`swapmodel5.py` 694-752): minting, the F3 deferral of received records, the member-local event.

use crate::coverage::CoverageOf;
use crate::entry::*;
use crate::member::Member;
use crate::world::World;
use std::collections::HashSet;

impl Member {
    pub fn report(&mut self, w: &mut World, key: u32, stamp: Stamp, body: Body, phys: &str) {
        self.report_path(w, key, None, stamp, body, phys);
    }

    /// `field`: the loss record of one lost cell (a head candidate): key `(_id, path, loser stamp)`, the value cut at 64 B.
    pub fn report_cell(&mut self, w: &mut World, key: u32, c: &crate::field::LateCell, phys: &str) {
        let path = c.path.as_ref().map(|p| w.path_id(p));
        self.report_path(w, key, path, c.stamp, crate::field::record_body(&c.value), phys);
    }

    pub fn report_path(
        &mut self,
        w: &mut World,
        key: u32,
        path: Option<u16>,
        stamp: Stamp,
        body: Body,
        phys: &str,
    ) {
        let f = w.f;
        if f.no_loss {
            return;
        }
        let mut k = if f.loss_key_collapse {
            LKey { key, path, stamp: None, minter: None }
        } else {
            LKey::at(key, path, stamp)
        };
        if f.record_per_minter {
            k = LKey { key, path, stamp: Some(stamp), minter: Some(self.id) }; // MUTANT: the minter is part of the key
        }
        if self.loss.contains_key(&k)
            || self.dropped_loss.contains(&k)
            || (f.pending_dedup && self.loss_pending.contains_key(&k))
        {
            return;
        }
        if self.vanished {
            return; // a vanished writer never writes again: a record IS an entry (Q7 D10), so it makes none
        }
        self.enter_loss(w, k, body.clone());
        self.loss_phys.insert(k, phys.to_string());
        if let Some(s) = self.serving()
            && let Some(sp) = self.phys.get(&s)
            && sp.alive
            && sp.docs.get(&key).is_some_and(|c| c.stamp == stamp)
        {
            self.inv.insert("report_minted_for_served_write");
        }
        // M4: an ordinary entry of the reporter's own origin, above everything it has witnessed, appended to the
        // end of its own sequence, so inside its coverage prefix (m-loss-outside-prefix: v2.1)
        let h = self.next_hlc(w);
        let mut e = Entry::new(self.id, h, Kind::Loss).extra(Extra::Phys(phys.to_string()));
        e.lkey = Some(k);
        e.body = body;
        let e = w.record(e, !f.loss_outside_prefix);
        self.cv.add(e.idx);
        self.vc.add(e.idx);
        self.raise(w, e.idx);
        self.appended.insert(e.idx);
        // The design rule, as an invariant (not a test of the flag): Q6 design page, 'Records': a loss record is an ordinary replicated
        // entry INSIDE ITS APPENDER'S COVERAGE PREFIX (minted under the reporter's own origin above everything it has witnessed and
        // appended at the end of its own sequence): checked as 'the record is the LAST entry of its origin's sequence at mint time'. The first version
        // asked the member's coverage instead and the mutant did not trip it (the member adds the record to its own coverage whatever the sequence),
        // so that was not the rule. `m-loss-outside-prefix` breaks it (the Python model fails nothing on that mutant)
        if w.by_origin[e.origin].last() != Some(&e.idx) {
            self.inv.insert("loss_record_outside_coverage_prefix");
        }
        w.stats.bump("loss_records_made");
        if !f.loss_outside_prefix {
            w.stats.bump("loss_records_inside_prefix");
        }
        w.note(format!("{} records loss ({}, {})", self.name, key, hlc_of(&stamp)));
    }

    /// v3fix (F3): a record is an ordinary entry and replicates, but it enters this member's state (and emits its
    /// outReplaced event) only once the physical of the lost write is buried HERE.
    pub fn merge_loss(&mut self, w: &mut World, k: LKey, body: Body, phys: &str) {
        if self.loss.contains_key(&k) || self.dropped_loss.contains(&k) {
            return;
        }
        self.loss_phys.insert(k, phys.to_string());
        if w.f.defer_received && !self.buried_here(phys) {
            if !self.loss_pending.contains_key(&k) {
                self.loss_pending.insert(k, body);
            }
            w.stats.bump("records_deferred_while_served");
            return;
        }
        self.enter_loss(w, k, body);
        self.loss_pending.remove(&k);
    }

    /// A record first enters this member's state: its outReplaced event is emitted, once per record key.
    pub fn enter_loss(&mut self, w: &World, k: LKey, body: Body) {
        self.loss.insert(k, body);
        *self.events_emitted.entry(k.head()).or_insert(0) += 1;
        self.enforce_k(w, k.key);
    }

    /// `K` (`field` collections, Chris's L34-B ruling, design note 1.4 rule 2): per `_id` the member keeps the `K` newest
    /// records by loser stamp across both families, `outReplaced`'s (`self.loss`) and Q7's (`MergeState.losses` of the head
    /// physical's state, ASSUMED here to be the serving physical's state at the end of the run) and drops the older `outReplaced` ones: a dropped record
    /// is reported at most once (its event was emitted on entry; `dropped_loss` refuses it a second entry). Q7's own records
    /// are never dropped here (`merge_state` has no `K`; Step 4).
    pub fn enforce_k(&mut self, w: &World, id: u32) {
        // the window is applied once the run has converged (`World::k_final`): the Q7 side of it is the head physical's state, which
        // is only the same at every member then; an early drop is irreversible
        let Some(kk) = w.k.filter(|_| w.k_final) else { return };
        if w.policy != crate::world::Policy::Field {
            return;
        }
        // the window's total order, the same at every member: loser stamp, then the family (Q7's records are older than an
        // `outReplaced` one of the same stamp), then the path
        type Order = (Stamp, u8, Option<u16>);
        let mut recs: Vec<(Order, Option<LKey>)> = self
            .loss
            .keys()
            .filter(|k| k.key == id)
            .filter_map(|k| k.stamp.map(|s| ((s, 1, k.path), Some(*k))))
            .collect();
        if !w.f.fm_k_ignore_q7 {
            // MUTANT (`m-field-k-ignore-q7`): Q7's records are not in the window. Otherwise: the records of the document in
            // the serving (head) physical's state, which at the end of a run is the converged one
            let q7: HashSet<_> = self
                .serving()
                .and_then(|s| self.phys.get(&s))
                .and_then(|p| p.docs.get(&id))
                .and_then(|v| v.state.as_ref())
                .map(|st| st.losses.keys().cloned().collect())
                .unwrap_or_default();
            recs.extend(q7.iter().map(|lk| ((lk.stamp, 0, None), None)));
        }
        recs.sort_by_key(|a| std::cmp::Reverse(a.0)); // newest first
        if w.f.fm_k_drop_newest {
            recs.reverse(); // MUTANT: the oldest are kept
        }
        let kk = if w.f.fm_k_plus_one { kk + 1 } else { kk }; // MUTANT
        for (_, k) in recs.into_iter().skip(kk) {
            if let Some(k) = k {
                self.loss.remove(&k);
                self.dropped_loss.insert(k);
            }
        }
    }

    pub fn enforce_k_all(&mut self, w: &World) {
        if w.k.is_none() {
            return;
        }
        let mut ids: Vec<u32> = self.loss.keys().map(|k| k.key).collect();
        ids.sort_unstable();
        ids.dedup();
        for id in ids {
            self.enforce_k(w, id);
        }
    }

    pub fn buried_here(&self, phys: &str) -> bool {
        self.phys.get(phys).is_some_and(|p| !p.alive)
    }

    pub fn promote_pending(&mut self, w: &mut World) {
        let keys: Vec<LKey> = self.loss_pending.keys().copied().collect();
        for k in keys {
            if self.buried_here(&self.loss_phys[&k].clone()) {
                let b = self.loss_pending.remove(&k).unwrap();
                self.enter_loss(w, k, b);
            }
        }
    }
}
