//! Loss records (`swapmodel5.py` 694-752): minting, the F3 deferral of received records, the member-local event.

use crate::coverage::CoverageOf;
use crate::entry::*;
use crate::member::Member;
use crate::world::World;

impl Member {
    pub fn report(&mut self, w: &mut World, key: u32, stamp: Stamp, body: Body, phys: &str) {
        let f = w.f;
        if f.no_loss {
            return;
        }
        let mut k = if f.loss_key_collapse {
            LKey { key, stamp: None, minter: None }
        } else {
            LKey::plain(key, stamp)
        };
        if f.record_per_minter {
            k = LKey { key, stamp: Some(stamp), minter: Some(self.id) }; // MUTANT: the minter is part of the key
        }
        if self.loss.contains_key(&k) || (f.pending_dedup && self.loss_pending.contains_key(&k)) {
            return;
        }
        if self.vanished {
            return; // a vanished writer never writes again: a record IS an entry (Q7 D10), so it makes none
        }
        self.enter_loss(k, body.clone());
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
        self.appended.insert(e.idx);
        w.stats.bump("loss_records_made");
        if !f.loss_outside_prefix {
            w.stats.bump("loss_records_inside_prefix");
        }
        w.note(format!("{} records loss ({}, {})", self.name, key, hlc_of(&stamp)));
    }

    /// v3fix (F3): a record is an ordinary entry and replicates, but it enters this member's state (and emits its
    /// outReplaced event) only once the physical of the lost write is buried HERE.
    pub fn merge_loss(&mut self, w: &mut World, k: LKey, body: Body, phys: &str) {
        if self.loss.contains_key(&k) {
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
        self.enter_loss(k, body);
        self.loss_pending.remove(&k);
    }

    /// A record first enters this member's state: its outReplaced event is emitted, once per record key.
    pub fn enter_loss(&mut self, k: LKey, body: Body) {
        self.loss.insert(k, body);
        *self.events_emitted.entry(k.head()).or_insert(0) += 1;
    }

    pub fn buried_here(&self, phys: &str) -> bool {
        self.phys.get(phys).is_some_and(|p| !p.alive)
    }

    pub fn promote_pending(&mut self, _w: &mut World) {
        let keys: Vec<LKey> = self.loss_pending.keys().copied().collect();
        for k in keys {
            if self.buried_here(&self.loss_phys[&k].clone()) {
                let b = self.loss_pending.remove(&k).unwrap();
                self.enter_loss(k, b);
            }
        }
    }
}
