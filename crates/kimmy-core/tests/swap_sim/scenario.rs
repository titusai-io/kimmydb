//! Helpers for scripted scenarios (the Rust twins of `probes4.py`/`probes5.py`' `world`, `advance`, `pull`,
//! `quiesce`).

use crate::entry::*;
use crate::flags::flags_for;
use crate::member::Member;
use crate::relay::{pair, take_quiesce};
use crate::rng::Rng;
use crate::world::World;

pub struct Sc {
    pub w: World,
    pub ms: Vec<Member>,
}

impl Sc {
    /// `members`: name and skew, in order; the index of a member is its position.
    pub fn new(mode: &str, members: &[(&str, i64)], d: i64) -> Self {
        let names: Vec<String> = members.iter().map(|(n, _)| n.to_string()).collect();
        let w = World::new(names, flags_for(mode, "v5"), Rng::new(0), d, false);
        let ms = members.iter().enumerate().map(|(i, (n, s))| Member::new(i, n, *s)).collect();
        Self { w, ms }
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
    /// Quiescence: entries held by no live member are lost; everything else reaches every live member in stamp order.
    pub fn quiesce(&mut self) -> Vec<usize> {
        let live: Vec<usize> = (0..self.ms.len()).filter(|&i| !self.ms[i].vanished).collect();
        self.w.lost = self
            .w
            .all
            .iter()
            .filter(|e| !live.iter().any(|&i| self.ms[i].holds(e.idx)))
            .map(|e| e.idx)
            .collect();
        for _ in 0..4 {
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
