//! The product's relay as a type (`swapmodel5.py` 933-964, 1183-1201 and the window choice of the run loop,
//! 1378-1385): a member serves onward only what it appended; a receiver witnesses, without receiving, the earlier
//! entries its source holds but did not append; windows are served from the receiver's single threshold in stamp
//! order; ADR-224 refuses a window across history a completed snapshot holds as state; a join in progress is not
//! protected; no prefix closure at death.

use crate::coverage::CoverageOf;
use crate::entry::*;
use crate::member::Member;
use crate::world::World;

/// How an entry reaches a member.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Delivery {
    /// The product's pull: the lowest-stamp entry the source appended and the receiver lacks, with witnessed skips
    /// and the ADR-224 refusal.
    Window,
    /// A known-but-out-of-position entry (scripted scenarios, the out-of-order mode): a direct `take`.
    Held,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Taken {
    Received,
    RefusedStateHeld,
    AlreadyHeld,
}

/// Two distinct members of a slice: the receiver mutably and the source read-only.
pub fn pair(ms: &mut [Member], dst: usize, src: usize) -> (&mut Member, &Member) {
    assert_ne!(dst, src);
    if dst < src {
        let (a, b) = ms.split_at_mut(src);
        (&mut a[dst], &b[0])
    } else {
        let (a, b) = ms.split_at_mut(dst);
        (&mut b[0], &a[src])
    }
}

impl Member {
    /// What this member serves onward: only what it appended.
    pub fn servable(&self, id: EntryId) -> bool {
        self.appended.contains(&id)
    }

    /// The receiver's single threshold: the lowest-stamp entry `src` appended that `self` lacks.
    pub fn next_window_entry(&self, w: &World, src: &Member) -> Option<EntryRef> {
        src.appended
            .iter()
            // an entry held out of position is not at the receiver's servable position: a window delivers it again (ADR-169)
            .filter(|&&i| !self.cv.has(i) || (w.held_uncovered && self.held_oop.contains(&i)))
            .map(|&i| w.ent_rc(i))
            .min_by_key(|e| e.stamp)
    }

    /// This member receives entry `e` in a window served by `src` (which appended it). The receiver witnesses,
    /// without receiving them, the earlier entries of `e`'s origin that `src` holds but did not append.
    /// ADR-224: `src` refuses a window below a row it holds only as a COMPLETED snapshot's state.
    pub fn take_from(&mut self, w: &mut World, src: &Member, e: &EntryRef, quiesce: bool) -> Taken {
        // ADR-169: an entry held as state that arrives in a window contiguous from the receiver's position is released
        let release = w.held_uncovered && self.held_oop.contains(&e.idx);
        if self.cv.has(e.idx) && !release {
            return Taken::AlreadyHeld;
        }
        if !quiesce
            && let Some(&row) = src.state_held.get(&e.origin)
            && !self.covers(w, e.origin, row)
        {
            w.stats.bump("state_held_refusals");
            return Taken::RefusedStateHeld;
        }
        if src.join.is_some() {
            w.stats.bump("windows_served_by_a_join_in_progress");
        }
        let earlier: Vec<EntryId> = w.by_origin[e.origin].clone();
        for xid in earlier {
            let x = w.ent_rc(xid);
            if x.hlc >= e.hlc {
                break;
            }
            if src.cv.has(xid) && !src.appended.contains(&xid) && !self.cv.has(xid) {
                self.cv.add(xid);
                self.witness(&x);
                w.stats.bump("witnessed_without_receiving");
                if w.trace {
                    w.note(format!(
                        "{} witnesses {} without receiving it (window from {} for {})",
                        self.name, x, src.name, e
                    ));
                }
            }
        }
        if release {
            self.held_oop.remove(&e.idx);
            self.appended.insert(e.idx);
            w.stats.bump("held_released_in_window");
            return Taken::Received;
        }
        self.take(w, e);
        if w.held_uncovered
            && w.by_origin[e.origin]
                .iter()
                .take_while(|&&x| w.ent(x).hlc < e.hlc)
                .any(|x| self.held_oop.contains(x))
        {
            // the servable position cannot pass an entry held out of position (docs/oplog.md:489-491: held entries sit above it):
            // an entry that arrives behind one is itself held until its predecessors are released
            self.appended.remove(&e.idx);
            self.held_oop.insert(e.idx);
            w.stats.bump("held_behind_held");
        }
        Taken::Received
    }

    /// R4: `src` relays the tombstone of a bound-out physical to this member.
    pub fn relay_tomb(&mut self, w: &mut World, src: &Member) -> bool {
        let f = w.f;
        let mut cands: Vec<String> = src
            .phys
            .iter()
            .filter(|(n, p)| {
                !p.alive
                    && !p.orphan
                    && p.kill.is_some()
                    && self.phys.get(*n).is_some_and(|mine| mine.alive)
                    && self.cur.as_ref().is_none_or(|c| c.stamp < p.kill.unwrap())
            })
            .map(|(n, _)| n.clone())
            .collect();
        if cands.is_empty() {
            return false;
        }
        cands.sort();
        let n = match w.forced_choice.take() {
            Some(n) => {
                assert!(cands.contains(&n), "the replayed tombstone choice is a candidate");
                n
            }
            None => w.rng.choice(&cands),
        };
        let k = src.phys.get(&n).unwrap().kill.unwrap();
        w.stats.bump("relay_tomb");
        if f.bt_early {
            w.note(format!(
                "{}: MUTANT buries {} on a relayed tombstone before its own flip",
                self.name, n
            ));
            self.bury(w, &n, k, "relay");
        } else {
            self.pend_tombs.insert(n.clone(), k);
            w.note(format!(
                "{}: keeps tombstone info for {} ({}), buries at its own flip",
                self.name,
                n,
                hlc_of(&k)
            ));
        }
        self.settle(w);
        true
    }
}

/// The index of the first live member other than `skip` that appended `id` (quiescence delivery).
pub fn quiesce_source(ms: &[Member], skip: Option<usize>, id: EntryId) -> Option<usize> {
    ms.iter().position(|m| !m.vanished && Some(m.id) != skip && m.appended.contains(&id))
}

/// Quiescence delivery to a member of the cluster.
pub fn take_quiesce(ms: &mut [Member], dst: usize, w: &mut World, e: &EntryRef) {
    if ms[dst].cv.has(e.idx) {
        return;
    }
    match quiesce_source(ms, Some(dst), e.idx) {
        Some(src) => {
            let (d, s) = pair(ms, dst, src);
            d.take_from(w, s, e, true);
        }
        None => {
            if w.held_uncovered && ms.iter().any(|m| !m.vanished && m.held_oop.contains(&e.idx)) {
                // a live member holds it out of position and will serve it once it is in position: not witnessed away (the
                // end-of-run delivery's shortcut is for an entry lost with a vanished member; REPORT6 (e))
                return;
            }
            if w.trace {
                w.note(format!(
                    "{} witnesses {} at quiescence (no live member appended it)",
                    ms[dst].name, e
                ));
            }
            ms[dst].cv.add(e.idx); // no live member appended it: it is only witnessed
            ms[dst].witness(e);
            w.stats.bump("witnessed_at_quiescence");
        }
    }
}

/// Quiescence delivery to a fresh replica that is not part of the cluster (the sources are the cluster's members).
pub fn take_quiesce_replica(replica: &mut Member, ms: &[Member], w: &mut World, e: &EntryRef) {
    if replica.cv.has(e.idx) {
        return;
    }
    match quiesce_source(ms, None, e.idx) {
        Some(src) => {
            replica.take_from(w, &ms[src], e, true);
        }
        None => {
            if w.held_uncovered && ms.iter().any(|m| !m.vanished && m.held_oop.contains(&e.idx)) {
                return;
            }
            replica.cv.add(e.idx);
            replica.witness(e);
            w.stats.bump("witnessed_at_quiescence");
        }
    }
}
