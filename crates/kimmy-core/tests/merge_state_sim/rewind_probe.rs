//! The rewind's fold, graded (design q7-step3-rewind-design.md 5.2(iii), the Step 4 obligation of C7).
//!
//! For a finished run, a member's oplog is its retained history. This probe takes a member that holds every entry it
//! ever applied (no snapshot, no restore), *collects a prefix of it* as retention does (every entry stamped at or below
//! a horizon `H`, loss entries and state entries alike), picks a target `T` at or above `H`, and folds the retained entries
//! at or below `T` with `kimmy_core::merge_state::state_at`, the function the rewind calls. The outcome is one of:
//!
//! - **refused**, by the rewind's own rule (`kimmy-storage/src/missing.rs` and `rewind.rs`): a writer's newest collected
//!   *state* entry (a collected loss entry leaves no trace) that lies at or below `T` and above what the fold saw of that
//!   writer, or no retained state entry at or below `T` while the history has writes there. A refused rewind is counted
//!   as refused and never graded;
//! - **graded**, as design 17 grades a finished run, against the history at or below `T`: one record of some kind per
//!   required head (`record-missing`, `record-missing-killed-superseded`), every record sound (`record-unsound`), none
//!   naming a counted increment (`record-names-surviving-increment`), extras allowed. Also checked: the fold of the same
//!   entries in the reverse order is byte-equal (`order-dependent`).
//!
//! The documented claim this proves (the manager's ruling C, 2026-11-20): *a required head is never lost by a rewind that
//! is not refused*, so a record whose entries retention collected and that no retained state carries may be dropped, and
//! that drop is only ever of an allowed extra.

use std::collections::{BTreeMap, BTreeSet};

use kimmy_core::Hlc;
use kimmy_core::merge_state::{
    JoinOptions, LossKey, MStamp, MergeState, StateAtEntry, WriterId, join, state_at,
};

use crate::oracle::{self, Head, Log};
use crate::rng::Rng;
use crate::sim::{Ent, Node, RunOut};

/// What the probe saw, over every run it was given.
#[derive(Default)]
pub struct Tally {
    /// Folds attempted (a member, a horizon, a target).
    pub generated: usize,
    /// Of those with a nonempty prefix collected.
    pub collected_prefix: usize,
    /// Refused by the rewind's own rule.
    pub refused: usize,
    /// No document at the target (nothing written at or below it).
    pub gone: usize,
    /// Folds graded.
    pub graded: usize,
    /// Graded folds whose history at or below the target holds a required head.
    pub with_a_required_head: usize,
    /// Graded folds that hold a record.
    pub with_a_record: usize,
    /// Graded folds that hold fewer records than the member's live state held that are all extras by the oracle: the
    /// drops the ruling allows.
    pub dropped_extras: usize,
    /// Required heads without a record that the cap's under-report explains (the fold holds `K` records and the head ranks at
    /// or below the lowest of them), as the loss_law check exempts them.
    pub cap_exempt: usize,
    /// Graded folds that differ from the history's visible document at the target (reported, not gated: the visible
    /// document is the rewind's own first property, tested elsewhere).
    pub visible_differs: usize,
    /// Problems by tag: count and the first seed.
    pub failures: BTreeMap<String, (usize, u64)>,
}

impl Tally {
    fn fail(&mut self, tag: &str, seed: u64) {
        self.failures.entry(tag.to_string()).or_insert((0, seed)).0 += 1;
    }

    pub fn line(&self) -> String {
        format!(
            "{} folds ({} with a collected prefix): {} refused, {} gone, {} graded ({} with a required head, {} with a record, \
             {} dropped extras, {} cap-exempt heads, {} visible differs); failures {:?}",
            self.generated,
            self.collected_prefix,
            self.refused,
            self.gone,
            self.graded,
            self.with_a_required_head,
            self.with_a_record,
            self.dropped_extras,
            self.cap_exempt,
            self.visible_differs,
            self.failures.iter().map(|(k, v)| (k.as_str(), v.0)).collect::<Vec<_>>()
        )
    }
}

fn heads(have: &BTreeSet<LossKey>) -> BTreeSet<Head> {
    have.iter().map(|k| (oracle::at_path(&k.at), k.stamp)).collect()
}

/// The history at or below `target`.
fn log_at(log: &Log, target: Hlc) -> Log {
    log.iter().filter(|(stamp, _)| stamp.hlc <= target).map(|(s, v)| (*s, v.clone())).collect()
}

/// The fold of the retained entries at or below the target, as the rewind builds it (the collection's own options).
fn fold(node: &Node, retained: &[&Ent], target: Hlc, reversed: bool) -> MergeState {
    fold_with(&node.w.opts, retained, target, reversed)
}

/// [`fold`] with the options given.
fn fold_with(opts: &JoinOptions, retained: &[&Ent], target: Hlc, reversed: bool) -> MergeState {
    let mut items: Vec<(MStamp, StateAtEntry<'_>)> = retained
        .iter()
        .filter_map(|ent| match ent {
            Ent::W { st, state, .. } => Some((*st, StateAtEntry::Meta(state))),
            Ent::L { st, key, val } => Some((*st, StateAtEntry::Loss(key.clone(), val.clone()))),
            Ent::O { .. } => None,
        })
        .collect();
    if reversed {
        items.reverse();
    }
    state_at(items, target, None, opts)
}

/// Grade the folds of one run.
pub fn probe(out: &RunOut, seed: u64, tally: &mut Tally) {
    let Some(first) = out.nodes.first() else { return };
    let (ret, bound) = (first.ret(), first.bound());
    let write_hlcs: Vec<Hlc> = out.log.keys().map(|s| s.hlc).collect();
    if write_hlcs.is_empty() {
        return;
    }
    let mut rng = Rng::new(seed ^ 0x5eed_c7c7);
    for node in out.nodes.iter().filter(|n| n.snaps == 0 && !n.restored) {
        let entries: Vec<&Ent> = node.oplog.values().map(|e| &**e).collect();
        let stamps: Vec<MStamp> = node.oplog.keys().copied().collect();
        if stamps.is_empty() {
            continue;
        }
        for round in 0..4 {
            tally.generated += 1;
            // Retention collects every entry at or below a horizon; none at all in a quarter of the folds.
            let horizon = if round == 0 { Hlc::ZERO } else { stamps[rng.below(stamps.len())].hlc };
            let target = {
                let above: Vec<Hlc> =
                    write_hlcs.iter().copied().filter(|h| *h >= horizon).collect();
                if above.is_empty() { horizon } else { above[rng.below(above.len())] }
            };
            let retained: Vec<&Ent> = entries
                .iter()
                .copied()
                .filter(|e| match e {
                    Ent::W { st, .. } | Ent::L { st, .. } | Ent::O { st } => st.hlc > horizon,
                })
                .collect();
            let collected = entries.len() - retained.len();
            tally.collected_prefix += usize::from(collected > 0);
            // What retention left in the missing-through row: the newest collected STATE entry of each writer.
            let mut missing: BTreeMap<WriterId, Hlc> = BTreeMap::new();
            for e in &entries {
                if let Ent::W { st, .. } = e
                    && st.hlc <= horizon
                {
                    let held = missing.entry(st.writer).or_insert(Hlc::ZERO);
                    *held = (*held).max(st.hlc);
                }
            }
            let folded = fold(node, &retained, target, false);
            let holds_a_state =
                retained.iter().any(|e| matches!(e, Ent::W { st, .. } if st.hlc <= target));
            let history = log_at(&out.log, target);
            if history.is_empty() || folded.is_bare() && holds_a_state {
                tally.gone += 1;
                continue;
            }
            let incomplete = missing.iter().any(|(writer, at)| {
                *at <= target && *at > folded.vv.get(writer).copied().unwrap_or(Hlc::ZERO)
            });
            if incomplete || !holds_a_state {
                tally.refused += 1;
                continue;
            }
            tally.graded += 1;
            let (want, alive, dels) = oracle::oracle(&history, ret, bound);
            if folded.visible(&node.w.opts) != want {
                tally.visible_differs += 1;
            }
            if fold(node, &retained, target, true).encode_compact() != folded.encode_compact() {
                tally.fail("order-dependent", seed);
            }
            let must = oracle::loss_oracle(&history, &alive, &dels, want.as_ref(), ret);
            let have: BTreeSet<LossKey> = folded.losses.keys().cloned().collect();
            let hv = heads(&have);
            tally.with_a_required_head += usize::from(!must.is_empty());
            tally.with_a_record += usize::from(!have.is_empty());
            if !oracle::stale_records(&have, &history, ret).is_empty() {
                tally.fail("record-names-surviving-increment", seed);
            }
            // The cap rule (loss_law.rs, `check`): a required head with no record is the cap's under-report only in a result
            // at `K` records, at or below the lowest record held; the cap keeps the greatest, so nothing above the lowest can
            // have been dropped by it. With no cap in reach (fewer than `K` records) every required head is owed.
            let cap = node.w.opts.loss_cap();
            let lowest_held = have.iter().min_by(|a, b| a.rank_cmp(b)).map(|k| k.stamp);
            let at_cap = have.len() >= cap;
            let mut absent = must.iter().filter(|h| !oracle::covered(h, &hv, &history)).peekable();
            if absent.peek().is_some() {
                if absent.any(|(_, s)| !at_cap || lowest_held.is_none_or(|low| *s > low)) {
                    tally.fail("record-missing", seed);
                } else {
                    tally.cap_exempt += 1;
                }
            }
            if oracle::killed_superseded_any(&history, ret).iter().any(|grp| {
                grp.is_disjoint(&hv)
                    && !(at_cap
                        && grp.iter().any(|(_, s)| lowest_held.is_some_and(|low| *s <= low)))
            }) {
                tally.fail("record-missing-killed-superseded", seed);
            }
            if have.iter().any(|k| !oracle::sound_record(k, &history, &alive, &dels, ret)) {
                tally.fail("record-unsound", seed);
            }
            // A record the member held at the target (its live state's records stamped at or below it) and the fold lacks.
            let live_then = log_at_records(node, target);
            if live_then.iter().any(|k| !have.contains(k)) {
                tally.dropped_extras += 1;
            }
        }
    }
}

/// The records of the member's final state whose loser is at or below the target.
fn log_at_records(node: &Node, target: Hlc) -> Vec<LossKey> {
    node.state().losses.keys().filter(|k| k.stamp.hlc <= target).cloned().collect()
}

/// What [`options_compose`] saw.
#[derive(Default)]
pub struct Compose {
    pub folds: usize,
    /// Folds whose result with the wrong options, joined through the member's own, differs from the fold with its own.
    pub differs: usize,
    pub first_seed: Option<u64>,
}

/// Does the rewind's last join (`field_write::join(writer, None, &state)`, through the collection's own options) make the options of the
/// fold itself invisible? Fold the entries of the member's oplog at or below random targets with `wrong` options (the defaults), join the
/// result into the empty state through the member's own, and compare it with the fold made with the member's own. Equal everywhere means
/// that a rewind that folded with the wrong options (C7M9) would give the same state, so the mutant is equivalent at the result level.
pub fn options_compose(out: &RunOut, seed: u64, wrong: &JoinOptions, tally: &mut Compose) {
    let write_hlcs: Vec<Hlc> = out.log.keys().map(|s| s.hlc).collect();
    if write_hlcs.is_empty() {
        return;
    }
    let mut rng = Rng::new(seed ^ 0x0097_0009);
    for node in out.nodes.iter().filter(|n| n.snaps == 0 && !n.restored) {
        let entries: Vec<&Ent> = node.oplog.values().map(|e| &**e).collect();
        for _ in 0..4 {
            let target = write_hlcs[rng.below(write_hlcs.len())];
            tally.folds += 1;
            let own = fold_with(&node.w.opts, &entries, target, false);
            let other = fold_with(wrong, &entries, target, false);
            let joined = join(&MergeState::default(), &other, &node.w.opts).0;
            if joined.encode_compact() != own.encode_compact() {
                tally.differs += 1;
                tally.first_seed.get_or_insert(seed);
            }
        }
    }
}
