//! Helpers for scripted scenarios (the Rust twins of `probes4.py`/`probes5.py`' `world`, `advance`, `pull`,
//! `quiesce`).

use crate::coverage::CoverageOf;
use crate::entry::*;
use crate::flags::flags_for;
use crate::member::Member;
use crate::relay::{Taken, pair, take_quiesce};
use crate::rng::Rng;
use crate::snapshot::{land_page_in, snapshot_begin_in};
use crate::world::World;

pub struct Sc {
    pub w: World,
    pub ms: Vec<Member>,
}

impl Sc {
    /// `members`: name and skew, in order; the index of a member is its position.
    pub fn new(mode: &str, members: &[(&str, i64)], d: i64) -> Self {
        // a mode "field:<mode>" runs the scenario under the `field` policy
        let (policy, mode) = match mode.strip_prefix("field:") {
            Some(m) => (crate::world::Policy::Field, m),
            None => (crate::world::Policy::Document, mode),
        };
        let names: Vec<String> = members.iter().map(|(n, _)| n.to_string()).collect();
        let mut w = World::new(names, flags_for(mode, "v6"), Rng::new(0), d, false);
        w.policy = policy;
        // invariant (ii) over every Q6 kind is ON in scripted scenarios (M1); a scenario that delivers one out of position on purpose
        // turns it off and says why
        w.reg_in_position_check = true;
        let ms = members.iter().enumerate().map(|(i, (n, s))| Member::new(i, n, *s)).collect();
        Self { w, ms }
    }
    /// The same scenario under the `field` policy.
    pub fn field(mode: &str, members: &[(&str, i64)], d: i64) -> Self {
        let mut sc = Self::new(mode, members, d);
        sc.w.policy = crate::world::Policy::Field;
        sc
    }
    pub fn idx(&self, name: &str) -> usize {
        self.ms.iter().position(|m| m.name == name).unwrap_or_else(|| panic!("no member {name}"))
    }
    pub fn advance(&mut self, to: i64) {
        self.w.clock = to;
        self.w.step += 1;
    }
    /// The product's window: the lowest-stamp entry `src` appended and `dst` lacks, until none (or a refusal).
    ///
    /// A clock refusal (U1) is what a pusher reads in the `Pushed` reply: `src` notes that `dst` refuses its entries until `dst`'s
    /// clock has caught up, on `src`'s own clock (b2).
    pub fn pull(&mut self, dst: &str, src: &str) {
        let (d, s) = (self.idx(dst), self.idx(src));
        loop {
            let (dm, sm) = pair(&mut self.ms, d, s);
            let Some(e) = dm.next_window_entry(&self.w, sm) else { return };
            let n = dm.cv.count();
            let taken = dm.take_from(&mut self.w, sm, &e, false);
            if taken == Taken::RefusedClock {
                let bound = self.w.clock_bound.expect("a clock refusal needs the bound");
                let due_in = e.hlc - (self.w.clock + self.ms[d].skew + bound);
                let until = self.w.clock + self.ms[s].skew + due_in;
                let src_m = &mut self.ms[s];
                src_m.own_refused_until =
                    Some(src_m.own_refused_until.map_or(until, |u| u.max(until)));
                return;
            }
            if dm.cv.count() == n {
                return; // refused (ADR-224)
            }
        }
    }
    /// U1 (b): whether this member's own stamps would be refused by the rest of the cluster: its logical clock is past its own clock
    /// plus the bound (b1), or a peer refused its entries and that has not come due on its own clock (b2).
    pub fn own_stamps_refused(&self, member: &str) -> bool {
        let m = &self.ms[self.idx(member)];
        let Some(bound) = self.w.clock_bound else { return false };
        let own_now = self.w.clock + m.skew;
        m.hlc > own_now + bound || m.own_refused_until.is_some_and(|until| until > own_now)
    }
    /// The coordinator's bind with the guard (b): refused, nothing minted, when its own stamps would be refused. `m-no-own-guard` turns the
    /// guard off.
    pub fn guarded_bind(&mut self, member: &str, job: &mut Job, live: usize) -> Option<EntryRef> {
        if !self.w.f.fm_no_own_guard && self.own_stamps_refused(member) {
            self.w.stats.bump("bind_refused_by_guard");
            return None;
        }
        let i = self.idx(member);
        self.ms[i].do_bind(&mut self.w, job, live)
    }
    /// `dst` takes entry `e` in a window from `src`.
    pub fn window(&mut self, dst: &str, src: &str, e: &EntryRef) {
        let (d, s) = (self.idx(dst), self.idx(src));
        let (dm, sm) = pair(&mut self.ms, d, s);
        dm.take_from(&mut self.w, sm, e, false);
    }
    /// `dst` takes entry `e` directly (a held entry out of position, or a seed delivery).
    pub fn take(&mut self, dst: &str, e: &EntryRef) {
        let d = self.idx(dst);
        self.ms[d].take(&mut self.w, e);
    }
    /// A window from `src` to `dst` restricted to the entries of the given origins (the model's `pull(.., only=..)`).
    pub fn pull_origins(&mut self, dst: &str, src: &str, only: &[&str]) {
        let (d, s) = (self.idx(dst), self.idx(src));
        let origins: Vec<usize> = only.iter().map(|n| self.idx(n)).collect();
        loop {
            let (dm, sm) = pair(&mut self.ms, d, s);
            let next = sm
                .appended
                .iter()
                .filter(|&&i| !dm.cv.has(i) && origins.contains(&self.w.ent(i).origin))
                .map(|&i| self.w.ent_rc(i))
                .min_by_key(|e| e.stamp);
            let Some(e) = next else { return };
            let n = dm.cv.count();
            dm.take_from(&mut self.w, sm, &e, false);
            if dm.cv.count() == n {
                return;
            }
        }
    }
    /// `dst` begins a snapshot join from `src`.
    pub fn snapshot(&mut self, dst: &str, src: &str, lazy: bool) -> bool {
        let (d, s) = (self.idx(dst), self.idx(src));
        snapshot_begin_in(&mut self.ms, d, s, &mut self.w, lazy)
    }
    /// `dst` lands the next page of its join.
    pub fn land(&mut self, dst: &str) -> bool {
        let d = self.idx(dst);
        land_page_in(&mut self.ms, d, &mut self.w)
    }
    /// The per-step checks (`check_inv`, the step checks included) on every live member, as the random run makes after each step.
    pub fn check(&mut self) {
        for m in self.ms.iter_mut().filter(|m| !m.vanished) {
            m.check_inv(&self.w);
        }
    }
    /// `dst` marks a member vanished (it acts, relays and joins no more).
    pub fn vanish(&mut self, names: &[&str]) {
        for n in names {
            let i = self.idx(n);
            self.ms[i].vanished = true;
            self.w.stats.bump("writer_vanished");
        }
    }
    /// Quiescence: entries held by no live member are lost; everything else reaches every live member in stamp order.
    pub fn quiesce(&mut self) -> Vec<usize> {
        let live: Vec<usize> = (0..self.ms.len()).filter(|&i| !self.ms[i].vanished).collect();
        for &i in &live {
            while self.ms[i].join.is_some() {
                land_page_in(&mut self.ms, i, &mut self.w);
            }
        }
        self.w.lost = self
            .w
            .all
            .iter()
            .filter(|e| !live.iter().any(|&i| self.ms[i].holds(e.idx)))
            .map(|e| e.idx)
            .collect();
        for _ in 0..4 {
            for &i in &live {
                while self.ms[i].join.is_some() {
                    land_page_in(&mut self.ms, i, &mut self.w);
                }
            }
            for &i in &live {
                let mut order: Vec<EntryRef> =
                    self.w.all.iter().filter(|e| !self.w.lost.contains(&e.idx)).cloned().collect();
                order.sort_by_key(|e| e.stamp);
                for e in &order {
                    take_quiesce(&mut self.ms, i, &mut self.w, e);
                }
                self.ms[i].settle(&mut self.w);
            }
        }
        live
    }
}
