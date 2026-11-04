//! Helpers for scripted scenarios (the Rust twins of `probes4.py`/`probes5.py`' `world`, `advance`, `pull`,
//! `quiesce`).

use crate::coverage::CoverageOf;
use crate::entry::*;
use crate::flags::flags_for;
use crate::member::Member;
use crate::relay::{pair, take_quiesce};
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
    pub fn pull(&mut self, dst: &str, src: &str) {
        let (d, s) = (self.idx(dst), self.idx(src));
        loop {
            let (dm, sm) = pair(&mut self.ms, d, s);
            let Some(e) = dm.next_window_entry(&self.w, sm) else { return };
            let n = dm.cv.count();
            dm.take_from(&mut self.w, sm, &e, false);
            if dm.cv.count() == n {
                return; // refused (ADR-224)
            }
        }
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
