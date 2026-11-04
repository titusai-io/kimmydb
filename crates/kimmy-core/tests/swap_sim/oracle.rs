//! The oracle and the checks (`swapmodel5.py` 1204-1236, 1241-1298, 1423-1605): views, the per-step invariants, the
//! oracle, and `finish` (divergence, oracle state, per-head record sets, event counts, invariants, non-vacuity).
//! The per-head oracle only: heads are required, any true late write is allowed.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::entry::*;
use crate::member::Member;
use crate::relay::take_quiesce_replica;
use crate::world::World;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct State {
    pub serving: Option<String>,
    pub alive: Vec<(String, Vec<(u32, Ver)>)>,
}

type HeadKey = (u32, Option<Stamp>);

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

    pub fn check_inv(&mut self) {
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
        // m3: no member holds a loss record for a write that it still serves
        if !self.loss.is_empty() {
            for (k, v) in self.served() {
                if self.loss.contains_key(&LKey::plain(k, v.stamp))
                    || self.loss.contains_key(&LKey { key: k, stamp: None, minter: None })
                {
                    self.inv.insert("record_for_served_write_at_member");
                    break;
                }
            }
        }
    }
}

pub struct Oracle {
    pub top: Option<EntryRef>,
    pub alive: BTreeSet<String>,
    pub served: BTreeMap<u32, Ver>,
    pub loss: BTreeMap<(u32, Stamp), Body>,
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
                let newer = docs.get(&e.key).is_none_or(|c| e.stamp > c.stamp);
                if newer {
                    docs.insert(e.key, Ver { stamp: e.stamp, body: e.body.clone() });
                }
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
    let mut loss: BTreeMap<(u32, Stamp), Body> = BTreeMap::new();
    for e in &allx {
        if e.kind == Kind::Doc && !e.staging && late(e, &ev) {
            loss.insert((e.key, e.stamp), e.body.clone());
        }
    }
    // the converse: late writes (by the same rule) that were lost with a vanished member
    w.lost_late = w
        .all
        .iter()
        .filter(|e| lost.contains(&e.idx) && e.kind == Kind::Doc && !e.staging && late(e, &ev))
        .map(|e| (e.key, e.stamp))
        .collect();
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
    Oracle { top, alive, served, loss, lingering }
}

pub type Res = BTreeMap<String, i64>;

fn bk(m: &Member) -> HashSet<HeadKey> {
    m.loss.keys().map(|k| k.head()).collect()
}

/// `finish`: every check, as a map of named counts (0 = clean), as the model's `res`.
pub fn finish(w: &mut World, ms: &mut [Member]) -> Res {
    let o = oracle(w);
    let mut res: Res = BTreeMap::new();
    let live: Vec<usize> = (0..ms.len()).filter(|&i| !ms[i].vanished).collect();
    let first = live[0];
    // replay: two fresh replicas that receive every entry (no snapshots), after the cluster's quiescence
    let mut base_entries: Vec<EntryRef> =
        w.all.iter().filter(|e| !w.lost.contains(&e.idx)).cloned().collect();
    base_entries.sort_by_key(|e| e.stamp); // the model shuffles, then stably sorts by stamp; stamps are unique
    let mut replays: Vec<(State, HashSet<HeadKey>)> = Vec::new();
    for i in 0..2 {
        let mut z = Member::new(ms.len() + i, &format!("Z{i}"), 0);
        for e in &base_entries {
            take_quiesce_replica(&mut z, ms, w, e);
        }
        z.settle(w);
        replays.push((z.state(), bk(&z)));
    }
    let okeys: HashSet<HeadKey> = o.loss.keys().map(|(k, s)| (*k, Some(*s))).collect();
    let mut newest: HashMap<(String, u32), Stamp> = HashMap::new();
    for x in w.all.iter() {
        if x.kind == Kind::Doc && !x.staging && !w.lost.contains(&x.idx) {
            let kk = (x.phys.clone(), x.key);
            if newest.get(&kk).is_none_or(|&n| x.stamp > n) {
                newest.insert(kk, x.stamp);
            }
        }
    }
    let late_by_key: HashMap<HeadKey, EntryRef> = w
        .all
        .iter()
        .filter(|x| x.kind == Kind::Doc && !x.staging && okeys.contains(&(x.key, Some(x.stamp))))
        .map(|x| ((x.key, Some(x.stamp)), x.clone()))
        .collect();
    let heads: HashSet<HeadKey> = late_by_key
        .iter()
        .filter(|(_, x)| newest.get(&(x.phys.clone(), x.key)) == Some(&x.stamp))
        .map(|(k, _)| *k)
        .collect();
    let m0state = ms[first].state();
    let div_replay = replays.iter().any(|(st, recs)| {
        *st != m0state
            || heads.iter().any(|h| !recs.contains(h))
            || recs.iter().any(|r| !okeys.contains(r))
    });
    let states_differ = live.iter().any(|&i| ms[i].state() != m0state);
    let loss_sets: Vec<BTreeSet<LKey>> =
        live.iter().map(|&i| ms[i].loss.keys().copied().collect()).collect();
    let loss_differ = loss_sets.iter().any(|s| *s != loss_sets[0]);
    res.insert("div_replay".into(), div_replay as i64);
    res.insert("diverged".into(), (states_differ || loss_differ || div_replay) as i64);
    res.insert("div_state".into(), states_differ as i64);
    res.insert("div_loss".into(), loss_differ as i64);
    let served_mismatch = live.iter().any(|&i| ms[i].served() != o.served);
    let alive_mismatch = live.iter().any(|&i| {
        let a: BTreeSet<String> =
            ms[i].phys.iter().filter(|(_, p)| p.alive).map(|(n, _)| n.clone()).collect();
        a != o.alive
    });
    res.insert("served_mismatch".into(), served_mismatch as i64);
    res.insert("alive_mismatch".into(), alive_mismatch as i64);
    res.insert("oracle_state".into(), (served_mismatch || alive_mismatch) as i64);
    let loss_missing = live.iter().any(|&i| heads.iter().any(|h| !bk(&ms[i]).contains(h)));
    let loss_extra = live.iter().any(|&i| bk(&ms[i]).iter().any(|r| !okeys.contains(r)));
    res.insert("loss_missing".into(), loss_missing as i64);
    res.insert("loss_extra".into(), loss_extra as i64);
    res.insert("loss_mismatch".into(), (loss_missing || loss_extra) as i64);
    res.insert("heads_required".into(), heads.len() as i64);
    res.insert("true_late_writes_allowed".into(), okeys.len() as i64);
    let nonheads: Vec<HeadKey> = okeys.iter().filter(|k| !heads.contains(k)).copied().collect();
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
                .filter(|x| x.kind == Kind::Doc && Some(x.stamp) == k.1 && x.key == k.0)
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
            .any(|((k, s), v)| ms[i].loss.get(&LKey::plain(*k, *s)).is_some_and(|b| b != v))
    });
    res.insert("loss_body_mismatch".into(), body_mismatch as i64);
    // vanish after replication: a late write of a vanished origin that reached a live member must have exactly ONE
    // record on every live member and have emitted exactly ONE event there
    let late_entries: Vec<&EntryRef> = w
        .all
        .iter()
        .filter(|e| e.kind == Kind::Doc && !e.staging && o.loss.contains_key(&(e.key, e.stamp)))
        .collect();
    let vr: Vec<&EntryRef> = late_entries
        .iter()
        .filter(|e| ms[e.origin].vanished && heads.contains(&(e.key, Some(e.stamp))))
        .copied()
        .collect();
    res.insert("vr_replicated_late_writes".into(), vr.len() as i64);
    res.insert("vr_unreplicated_late_writes".into(), w.lost_late.len() as i64);
    let mut vr_bad = false;
    for e in &vr {
        for &i in &live {
            let k = (e.key, Some(e.stamp));
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
    let several = vr.iter().any(|e| minters.get(&(e.key, Some(e.stamp))).copied().unwrap_or(0) > 1);
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
        for k in &m.inv {
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
    let st = &w.stats;
    if st.get("accepted_while_pending") > 0 {
        inv.insert("write_accepted_while_pending".into());
        res.insert(
            "pending_acked_lost".into(),
            w.pend_acked
                .iter()
                .filter(|&&i| o.loss.contains_key(&(w.ent(i).key, w.ent(i).stamp)))
                .count() as i64,
        );
    }
    if st.get("accepted_while_superseded") > 0 {
        res.insert(
            "superseded_acked_lost".into(),
            w.sup_acked
                .iter()
                .filter(|&&i| o.loss.contains_key(&(w.ent(i).key, w.ent(i).stamp)))
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
    res.insert("late_write_count".into(), o.loss.len() as i64);
    res.insert("refused_count".into(), st.get("refused"));
    res.insert("refused_bind_count".into(), st.get("refused_bind"));
    res.insert("refused_ldrop_count".into(), st.get("refused_ldrop"));
    res.insert("refused_lcreate_count".into(), st.get("refused_lcreate"));
    res
}
