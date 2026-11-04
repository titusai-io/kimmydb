//! The mutants (mutants5.py, targeted5.py): each is graded by the random runs
//! on the pull protocol and, where one exists, by its named probe. A mutant
//! must be KILLED: failed by the random runs, or by its probe.
//!
//! The V3M3, V4M1, B1 and M1 mutants and the `$setOnInsert` suppression mutant use
//! test-only switches (`JoinOptions::set_v3_generation`, `SimFlags::hidden_records`,
//! `JoinOptions::set_value_ignores_slot_stamp`, `JoinOptions::set_creation_without_generation`,
//! `JoinOptions::set_suppress_soi`); the nested mutants use the switches of the nested rules
//! (`set_type_ignores_recency`, `set_marker_hides`, `set_no_type_clash`,
//! `set_array_clash_as_clash`, `set_rewrite_unchanged`, `set_write_below_scalar`, `set_unset_shallow`).

use std::collections::BTreeMap;
use std::time::Instant;

use kimmy_core::merge_state::LossKind;

use crate::grade::{grade, grade_bounded};
use crate::ord_replay;
use crate::probes;
use crate::sim::{Cfg, RunConfig, run, run_bounded, settle};

#[derive(Clone, Copy)]
pub enum Grader {
    /// `P.run` + `P.grade`, with (`true`) or without a restore.
    Random { restore: bool },
    /// `P.run(steps=24, ret=4)` + `settle`: a flip-flop is a failure.
    /// `skip` says whether the covered-entry skip is on for the runs (see the mutant V3M1).
    Horizon { skip: bool },
    /// The bounded runs of the runner (RET 40, a fixpoint every 3 steps, 80 steps), flat or nested.
    Bounded { nested: bool },
}

#[derive(Clone, Copy)]
pub enum Probe {
    None,
    /// V4M2: restore the same backup twice.
    EpochTwice,
    /// targeted5 `restore_inc_scenario`, all 720 final pull orders.
    RestoreInc,
    /// targeted5 `snapshot_loss_scenario`.
    SnapshotLoss,
    /// The B1 and M1 scenarios (`probes::scenarios`), which the design passes: the mutant is
    /// killed when one of them finds a problem. M1's random runs kill it about once in 200
    /// (a non-numeric creation value of an upsert and an increment on its path, in the one order
    /// that shows), so a change of the workload's random stream could leave it alive.
    Scenarios,
    /// The fold scenarios of the counter cell (`probes::fold_scenarios_with`: the four-step case of the
    /// review and EG2), read by value, and the marker scenario (`$set a {b: 1}` over a counter folds its
    /// slot), read through the slot's pair: the mutant is killed when a member reads another value than the
    /// design's or the marker write leaves the slot without its pair.
    Fold,
    /// The idle-origin and late-member scripts of probes_q3 (`probes::q3_idle_origin`, `q3_late_member`):
    /// the mutant is killed when the collected life comes back in one of them (the design keeps it dead).
    Q3Skip,
    /// The restore script of probes_q3 (`probes::q3_restore`): the start-HLC rule changes no state, only
    /// what a pull hands over, so the mutant is killed when the restored member's pull hands A MORE
    /// entries than the design's.
    Q3Start,
    /// `probes::q3_whole_vector`: an entry whose stamp the receiver covers and whose state it does not
    /// is applied by the design and skipped by the stamp-only mutant.
    Q3Stamp,
    /// `probes::v3m1_order_independent`: two members that apply the same entries in different orders, after
    /// a partition longer than retention, must hold the same state. The default protocol, skip on.
    HorizonOrder,
    /// The Ord differential's scripts (`tests/data/ord_diff_scripts.json`, the A2.1 model) replayed through
    /// the mutated writers: killed when one script disagrees with the model. The scripted probe for the
    /// mutants of the order state's writer-side rules that the random runs do not show (no multi-operator
    /// update is in the sim's workload, and a field order needs a model to tell it from another).
    OrdDiff,
}

pub struct Mutant {
    pub name: &'static str,
    pub apply: fn(&mut Cfg),
    pub grader: Grader,
    pub probe: Probe,
}

pub fn mutants() -> Vec<Mutant> {
    use Grader::*;
    use Probe::*;
    vec![
        Mutant {
            name: "coverage keyed by node, not (node, epoch)  [V3M2]",
            apply: |c| c.flags.epoch_keyed = false,
            grader: Random { restore: true },
            probe: RestoreInc,
        },
        Mutant {
            name: "no ADR-224 holes on a snapshot grant  [V3M4]",
            apply: |c| c.flags.holes = false,
            grader: Random { restore: false },
            probe: None,
        },
        Mutant {
            name: "no loss entries from any join",
            apply: |c| c.flags.no_loss_entries = true,
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "snapshot joins append no loss entries  [V3m1]",
            apply: |c| c.flags.snapshot_loss_entries = false,
            grader: Random { restore: true },
            probe: SnapshotLoss,
        },
        Mutant {
            name: "horizon applied after aliveness  [V3M1]",
            apply: |c| c.opts.set_horizon_first(false),
            // Two killers. The random runs show it as a flip-flop when entries the receiver holds are joined
            // again after the horizon moved (ADR-085), which the covered-entry skip short-circuits, so that
            // grader runs with the skip OFF. On the DEFAULT protocol (skip on) the scripted probe
            // `v3m1_order_independent` kills it: entries the receiver does not cover still meet a dead life older
            // than the joined horizon, and the order of the entries then decides (option (a) of the manager's
            // note: the rule is NOT equivalent under the skip).
            grader: Horizon { skip: false },
            probe: HorizonOrder,
        },
        Mutant {
            name: "v3 generation rule (newest non-numeric base)  [V3M3]",
            apply: |c| c.opts.set_v3_generation(true),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "hidden increments as replicated records again  [V4M1]",
            apply: |c| c.flags.hidden_records = true,
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "$setOnInsert cells not suppressed under an explicit insert  [V6m2]",
            apply: |c| c.opts.set_suppress_soi(false),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "no clash detection",
            apply: |c| c.flags.drop_kind = Some(LossKind::Clash),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "no deleteWon detection",
            apply: |c| c.flags.drop_kind = Some(LossKind::DeleteWon),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "epoch kept in the store, restored with the backup  [V4M2]",
            apply: |c| c.flags.epoch_from_backup = true,
            grader: Random { restore: true },
            probe: EpochTwice,
        },
        Mutant {
            name: "concurrent first inserts merge field by field again (v5)  [Q2]",
            apply: |c| c.opts.set_supersede(false),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "writes into a superseded first insert not reported  [V6M1]",
            apply: |c| c.opts.set_cells_reported(false),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "a counter that nets to zero is absent again  [B1]",
            apply: |c| c.opts.set_value_ignores_slot_stamp(true),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "a non-numeric upsert creation value starts no generation  [M1]",
            apply: |c| c.opts.set_creation_without_generation(true),
            grader: Random { restore: true },
            probe: Scenarios,
        },
        Mutant {
            name: "a base write folds only the slots it counts, not every slot held  [M2 fold]",
            apply: |c| c.opts.set_fold_counted_only(true),
            grader: Random { restore: true },
            probe: Fold,
        },
        Mutant {
            name: "a base write folds only the slots of its own life  [M2 fold, life only]",
            apply: |c| c.opts.set_fold_life_only(true),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "a map-marker base write folds no slot  [M2 fold, marker]",
            apply: |c| c.opts.set_no_fold_marker(true),
            grader: Random { restore: true },
            probe: Fold,
        },
        Mutant {
            name: "a superseded and killed first insert reported neither way  [V7m4]",
            apply: |c| c.flags.report_neither = true,
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "a scalar hides every child, even a newer one  [D-13]",
            apply: |c| c.opts.set_type_ignores_recency(true),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "a map marker hides its children",
            apply: |c| c.opts.set_marker_hides(true),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "no typeClash is reported",
            apply: |c| c.opts.set_no_type_clash(true),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "two arrays are a plain clash, not an arrayClash",
            apply: |c| c.opts.set_array_clash_as_clash(true),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "a sub-document or replace write rewrites every leaf",
            apply: |c| c.opts.set_rewrite_unchanged(true),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "a write below a scalar or an array is accepted",
            apply: |c| c.opts.set_write_below_scalar(true),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "an unset of a map unsets its direct children only",
            apply: |c| c.opts.set_unset_shallow(true),
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "no covered-entry skip: an entry the receiver holds is joined again  [Q3]",
            apply: |c| c.flags.covered_skip = false,
            grader: Bounded { nested: false },
            probe: Q3Skip,
        },
        Mutant {
            name: "covered skip by the entry's stamp only, not its whole vector  [Q3]",
            apply: |c| c.flags.covered_skip_by_stamp = true,
            grader: Bounded { nested: false },
            probe: Q3Stamp,
        },
        Mutant {
            name: "no start-HLC rule: a new origin is pulled from 0  [Q3]",
            apply: |c| c.flags.start_hlc = false,
            grader: Random { restore: true },
            probe: Q3Start,
        },
        Mutant {
            name: "a write below a map is not a write to the map (no ancestor candidates)  [O2]",
            apply: |c| c.opts.set_ord_no_ancestor(true),
            grader: Random { restore: true },
            probe: OrdDiff,
        },
        Mutant {
            name: "no hybrid floor: the floor of a no-op unset in an update is dropped  [O2]",
            apply: |c| c.opts.set_ord_no_hybrid(true),
            grader: Random { restore: true },
            probe: OrdDiff,
        },
        Mutant {
            name: "a no-op unset is a write of its own (a stamp and a floor)  [O2]",
            apply: |c| c.opts.set_ord_noop_unset_stands(true),
            grader: Random { restore: true },
            probe: OrdDiff,
        },
        Mutant {
            name: "the update bit is never set in an unset's floor  [O2]",
            apply: |c| c.opts.set_ord_bit_unpacked(true),
            grader: Random { restore: true },
            probe: OrdDiff,
        },
        Mutant {
            name: "the first write of a writer to a path is not recorded  [O2]",
            apply: |c| c.opts.set_ord_no_first(true),
            grader: Random { restore: true },
            probe: OrdDiff,
        },
        Mutant {
            name: "a replace names its fields as plain writes, not resets  [O2]",
            apply: |c| c.opts.set_ord_replace_without_reset(true),
            grader: Random { restore: true },
            probe: OrdDiff,
        },
        Mutant {
            name: "a write that only reorders the fields is a no-op (the stage-A rule)  [O2]",
            apply: |c| c.opts.set_ord_reorder_is_noop(true),
            grader: Random { restore: true },
            probe: OrdDiff,
        },
    ]
}

/// What a grader found over `n` seeded runs: how many runs it failed, and the problems it named
/// (the killers of a mutant), each with its count and the first seed that showed it.
#[derive(Default)]
pub struct Failing {
    pub runs: u64,
    pub named: BTreeMap<String, (u64, u64)>,
}

impl Failing {
    fn note(&mut self, seed: u64, problems: impl IntoIterator<Item = String>) {
        let mut any = false;
        for p in problems {
            // `diverged regs+slots` and `emission B` are one problem each
            let name = p.split(' ').next().unwrap_or("").to_string();
            let e = self.named.entry(name).or_insert((0, seed));
            e.0 += 1;
            any = true;
        }
        self.runs += u64::from(any);
    }

    /// The killers, as `name xN (seed S)` joined.
    pub fn killers(&self) -> String {
        self.named
            .iter()
            .map(|(k, (n, seed))| format!("{k} x{n} (first seed {seed})"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Which of `n` seeded runs the grader fails, and for what.
pub fn failing(cfg: &Cfg, g: Grader, n: u64) -> Failing {
    let mut f = Failing::default();
    for seed in 0..n {
        match g {
            Grader::Random { restore } => {
                let o = run(seed, &RunConfig { restore, ..RunConfig::default() }, cfg);
                let mut ps = grade(&o.nodes, &o.log);
                if !o.settled {
                    ps.push("no-fixpoint".into());
                }
                f.note(seed, ps);
            }
            Grader::Bounded { nested } => {
                let (o, _) = run_bounded(seed, cfg, 40, 3, 80, nested);
                f.note(seed, grade_bounded(&o.nodes, &o.log));
            }
            Grader::Horizon { skip } => {
                let rc = RunConfig { steps: 24, retention: Some(4), ..RunConfig::default() };
                let mut cfg = cfg.clone();
                cfg.flags.covered_skip = cfg.flags.covered_skip && skip;
                let mut o = run(seed, &rc, &cfg);
                let ps = if settle(&mut o.nodes, 6).is_none() {
                    vec!["never-settles".to_string()]
                } else {
                    Vec::new()
                };
                f.note(seed, ps);
            }
        }
    }
    f
}

/// How many of `n` seeded runs the grader fails.
pub fn failing_runs(cfg: &Cfg, g: Grader, n: u64) -> u64 {
    failing(cfg, g, n).runs
}

pub struct Outcome {
    pub name: &'static str,
    pub random_fail: u64,
    /// The problems the random runs named: the killers.
    pub killers: String,
    pub runs: u64,
    pub probe: Option<String>,
    pub killed: bool,
    pub millis: u128,
}

pub fn judge(m: &Mutant, runs: u64) -> Outcome {
    let t0 = Instant::now();
    let design = Cfg::default_ret();
    let mut cfg = Cfg::default_ret();
    (m.apply)(&mut cfg);
    let fails = failing(&cfg, m.grader, runs);
    let random_fail = fails.runs;
    let (probe, probe_kills) = match m.probe {
        Probe::None => (None, false),
        Probe::EpochTwice => {
            let k = probes::v4m2_check(&design, &cfg);
            (
                Some(format!(
                    "V4M2 epoch twice (125 vs 105): {}",
                    if k { "killed" } else { "NOT killed" }
                )),
                k,
            )
        }
        Probe::Scenarios => {
            let bad: Vec<String> = probes::scenarios_with(&cfg)
                .into_iter()
                .filter(|(_, problems)| !problems.is_empty())
                .map(|(name, _)| name)
                .collect();
            let k = !bad.is_empty();
            (Some(format!("scenarios {bad:?}: {}", if k { "killed" } else { "NOT killed" })), k)
        }
        Probe::Fold => {
            let bad: Vec<String> = probes::fold_scenarios_with(&cfg)
                .into_iter()
                .filter(|(_, problems)| !problems.is_empty())
                .map(|(name, _)| name)
                .collect();
            let k = !bad.is_empty();
            (
                Some(format!(
                    "fold scenarios {bad:?}: {}",
                    if k { "killed" } else { "NOT killed" }
                )),
                k,
            )
        }
        Probe::Q3Skip => {
            let found: Vec<&str> = [
                ("idle origin", probes::q3_idle_origin(&cfg)),
                ("late member", probes::q3_late_member(&cfg)),
            ]
            .into_iter()
            .filter(|(_, out)| out.resurrected())
            .map(|(name, _)| name)
            .collect();
            let k = !found.is_empty();
            (
                Some(format!(
                    "Q3 scripts, the collected life comes back in {found:?}: {}",
                    if k { "killed" } else { "NOT killed" }
                )),
                k,
            )
        }
        Probe::HorizonOrder => {
            let (d, m) =
                (probes::v3m1_order_independent(&design), probes::v3m1_order_independent(&cfg));
            let k = d && !m;
            (
                Some(format!(
                    "V3M1 order independence after a long partition (skip on): design {d}, mutant {m}: {}",
                    if k { "killed" } else { "NOT killed" }
                )),
                k,
            )
        }
        Probe::OrdDiff => {
            // the design must agree with the model first: a regression of the design must not hide behind
            // "every mutant disagrees"
            let d = ord_replay::replay(&design.opts);
            let f = ord_replay::replay(&cfg.opts);
            let k = d.bad.is_empty() && !f.bad.is_empty();
            (
                Some(format!(
                    "Ord differential: design {} disagreements, mutant {} in {} scripts: {}",
                    d.bad.len(),
                    f.bad.len(),
                    f.scripts,
                    if k { "killed" } else { "NOT killed" }
                )),
                k,
            )
        }
        Probe::Q3Stamp => {
            let (d, m) = (probes::q3_whole_vector(&design), probes::q3_whole_vector(&cfg));
            let k = d && !m;
            (
                Some(format!(
                    "Q3 whole vector: the receiver learns the write the entry carries: design {d}, mutant {m}: {}",
                    if k { "killed" } else { "NOT killed" }
                )),
                k,
            )
        }
        Probe::Q3Start => {
            let (d, m) = (probes::q3_restore(&design), probes::q3_restore(&cfg));
            let k = m.delivered > d.delivered && m.clean() == d.clean();
            (
                Some(format!(
                    "Q3 restore: the pull hands {} entries against the design's {}: {}",
                    m.delivered,
                    d.delivered,
                    if k { "killed" } else { "NOT killed" }
                )),
                k,
            )
        }
        Probe::RestoreInc | Probe::SnapshotLoss => {
            let t = if matches!(m.probe, Probe::RestoreInc) {
                probes::targeted_v3m2(&design, &cfg)
            } else {
                probes::targeted_v3m1(&design, &cfg)
            };
            let k = t.pass();
            (
                Some(format!(
                    "{}: {} orders, design bad {}, mutant not failing {}",
                    t.label, t.orders, t.design_bad, t.mutant_ok
                )),
                k,
            )
        }
    };
    Outcome {
        name: m.name,
        random_fail,
        killers: fails.killers(),
        runs,
        probe,
        killed: random_fail > 0 || probe_kills,
        millis: t0.elapsed().as_millis(),
    }
}
