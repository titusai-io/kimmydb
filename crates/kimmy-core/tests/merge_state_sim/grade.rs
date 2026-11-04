//! `pullmodel5.grade`: the oracle checks of a finished run.

use std::collections::{BTreeMap, BTreeSet};

use kimmy_core::merge_state::{FieldOrd, LossKey, MStamp, MergeState, OrdAt, Path, ord_join};

use crate::oracle::{self, Head, Log};
use crate::sim::{Ent, Node};

/// The names of the components of the state that differ between members.
fn differing(ms: &[Node]) -> Vec<&'static str> {
    let s0 = ms[0].state();
    let mut out = Vec::new();
    let mut chk = |name: &'static str, f: &dyn Fn(&kimmy_core::merge_state::MergeState) -> bool| {
        if ms.iter().any(|m| !f(m.state())) {
            out.push(name);
        }
    };
    chk("lives", &|s| s.lives == s0.lives);
    chk("dead", &|s| s.dead == s0.dead);
    chk("deletes", &|s| s.deletes == s0.deletes);
    chk("regs", &|s| s.regs == s0.regs);
    chk("slots", &|s| s.slots == s0.slots);
    chk("vv", &|s| s.vv == s0.vv);
    chk("losses", &|s| s.losses == s0.losses);
    out
}

fn heads(have: &BTreeSet<LossKey>) -> BTreeSet<Head> {
    have.iter().map(|k| (oracle::at_path(&k.at), k.stamp)).collect()
}

/// Does every member's state decode (compact form) to itself? A state a writer builds satisfies
/// everything `decode` checks, among them that every dead life is listed and that the version
/// vector covers the state's stamps.
fn decodes(ms: &[Node]) -> bool {
    ms.iter().all(|m| {
        let st = m.state();
        MergeState::decode_compact(&st.encode_compact()).as_ref() == Ok(st)
    })
}

/// The checks of the order state, for a finished run whose members hold one canonical state: every member
/// reads the same ordered document; no writer read, right after a write of its own, an order the serial
/// reference does not give (`order_oracle`); and the join laws hold on the order entries the run reached.
fn order_problems(ms: &[Node]) -> Vec<String> {
    let mut out = Vec::new();
    let doc0 = ms[0].w.state.ordered_document(&ms[0].w.opts);
    if ms.iter().any(|m| m.w.state.ordered_document(&m.w.opts) != doc0) {
        out.push("ordered-document".into());
    }
    if ms.iter().any(|m| !m.order_problems.is_empty()) {
        out.push("order-ryw".into());
    }
    if !ord_laws(ms).is_empty() {
        out.push("ord-laws".into());
    }
    out
}

/// The join laws on the order entries of the states the run reached (every member's state and every state
/// in its oplog): commutative and idempotent on all of them; associative on the entries of runs within
/// retention (ADR-085: beyond it an honest history can reach a non-associative triple). Returns the
/// disagreements, by key.
fn ord_laws(ms: &[Node]) -> Vec<String> {
    let o = &ms[0].w.opts;
    let assoc = ms[0].ret() >= 1_000_000_000;
    let mut by_key: BTreeMap<(MStamp, OrdAt), Vec<FieldOrd>> = BTreeMap::new();
    let mut add = |s: &MergeState| {
        for (k, e) in &s.ords {
            let g = by_key.entry(k.clone()).or_default();
            if g.len() < 8 && !g.contains(e) {
                g.push(e.clone());
            }
        }
    };
    for m in ms {
        add(m.state());
        for ent in m.oplog.values() {
            if let Ent::W { state, .. } = &**ent {
                add(state);
            }
        }
    }
    let mut bad = Vec::new();
    for (k, es) in &by_key {
        for a in es {
            if ord_join(a, a, o) != *a {
                bad.push(format!("{k:?}: not idempotent"));
            }
            for b in es {
                if ord_join(a, b, o) != ord_join(b, a, o) {
                    bad.push(format!("{k:?}: not commutative"));
                }
                if assoc {
                    for c in es {
                        if ord_join(&ord_join(a, b, o), c, o) != ord_join(a, &ord_join(b, c, o), o)
                        {
                            bad.push(format!("{k:?}: not associative"));
                        }
                    }
                }
            }
        }
    }
    bad.truncate(3);
    bad
}

/// The problems of a finished run, by name (empty when the run is sound).
pub fn grade(ms: &[Node], log: &Log) -> Vec<String> {
    let mut errs = Vec::new();
    let ret = ms[0].ret();
    if !ms.iter().all(|m| m.canon() == ms[0].canon()) {
        let names = differing(ms);
        // equal canonical bytes with unequal fields cannot happen; `gens` is outside the canon
        errs.push(format!("diverged {}", names.join("+")));
        return errs;
    }
    if !decodes(ms) {
        errs.push("undecodable".into());
    }
    errs.extend(order_problems(ms));
    let (want, alive, dels) = oracle::oracle(log, ret, ms[0].bound());
    if ms.iter().any(|m| m.visible() != want) {
        errs.push("visible".into());
    }
    let must = oracle::loss_oracle(log, &alive, &dels, want.as_ref(), ret);
    let have: BTreeSet<LossKey> = ms[0].state().losses.keys().cloned().collect();
    // the leaf effects of every write, against the document its writer could see
    if ms.iter().any(|m| !m.problems.is_empty()) {
        errs.push("write-effects".into());
    }
    let hid = oracle::hidden_oracle(log, ret);
    if ms.iter().any(|m| m.w.hidden_all() != hid) {
        errs.push("hidden-state".into());
    }
    if !oracle::stale_records(&have, log, ret).is_empty() {
        errs.push("record-names-surviving-increment".into());
    }
    let hv = heads(&have);
    if oracle::killed_superseded_any(log, ret).iter().any(|grp| grp.is_disjoint(&hv)) {
        errs.push("record-missing-killed-superseded".into());
    }
    if !must.is_subset(&hv) {
        errs.push("record-missing".into());
    }
    if have.iter().any(|k| !oracle::sound_record(k, log, &alive, &dels, ret)) {
        errs.push("record-unsound".into());
    }
    for m in ms {
        let ev: BTreeSet<&LossKey> = m.events.iter().collect();
        let have_r: BTreeSet<&LossKey> = have.iter().collect();
        if !m.restored && (m.events.len() != ev.len() || ev != have_r) {
            errs.push(format!("emission {}", m.name));
            break;
        }
    }
    errs
}

/// The bounded run's horizon-aware grade (V4m3): a head below the final
/// horizon may be missing, and a collected record may have been emitted.
pub fn grade_bounded(ms: &[Node], log: &Log) -> Vec<String> {
    let mut bad = Vec::new();
    let ret = ms[0].ret();
    if !ms.iter().all(|m| m.canon() == ms[0].canon()) {
        bad.push("diverged".into());
        return bad;
    }
    if !decodes(ms) {
        bad.push("undecodable".into());
    }
    bad.extend(order_problems(ms));
    let (want, alive, dels) = oracle::oracle(log, ret, ms[0].bound());
    if ms.iter().any(|m| m.visible() != want) {
        bad.push("visible".into());
    }
    let h = ms[0].w.horizon();
    let must: BTreeSet<Head> = oracle::loss_oracle(log, &alive, &dels, want.as_ref(), ret)
        .into_iter()
        .filter(|(_, st): &(Path, MStamp)| st.hlc >= h)
        .collect();
    let have: BTreeSet<LossKey> = ms[0].state().losses.keys().cloned().collect();
    if !must.is_subset(&heads(&have)) {
        bad.push("record-missing".into());
    }
    if have.iter().any(|k| !oracle::sound_record(k, log, &alive, &dels, ret)) {
        bad.push("record-unsound".into());
    }
    if !oracle::stale_records(&have, log, ret).is_empty() {
        bad.push("record names a surviving increment".into());
    }
    for m in ms {
        let ev: BTreeSet<&LossKey> = m.events.iter().collect();
        if !m.restored && (m.events.len() != ev.len() || !have.iter().all(|k| ev.contains(k))) {
            bad.push("emission".into());
            break;
        }
    }
    bad
}
