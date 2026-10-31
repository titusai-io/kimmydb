//! The mutants (mutants5.py, targeted5.py): each is graded by the random runs
//! on the pull protocol and, where one exists, by its named probe. A mutant
//! must be KILLED: failed by the random runs, or by its probe.
//!
//! The V3M3 and V4M1 mutants and the `$setOnInsert` suppression mutant use
//! test-only switches (`JoinOptions::v3_generation`, `SimFlags::hidden_records`,
//! `JoinOptions::suppress_soi`).

use std::time::Instant;

use kimmy_core::merge_state::LossKind;

use crate::grade::grade;
use crate::probes;
use crate::sim::{Cfg, RunConfig, run, settle};

#[derive(Clone, Copy)]
pub enum Grader {
    /// `P.run` + `P.grade`, with (`true`) or without a restore.
    Random { restore: bool },
    /// `P.run(steps=24, ret=4)` + `settle`: a flip-flop is a failure.
    Horizon,
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
            apply: |c| c.opts.horizon_first = false,
            grader: Horizon,
            probe: None,
        },
        Mutant {
            name: "v3 generation rule (newest non-numeric base)  [V3M3]",
            apply: |c| c.opts.v3_generation = true,
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
            apply: |c| c.opts.suppress_soi = false,
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
            apply: |c| c.opts.supersede = false,
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "writes into a superseded first insert not reported  [V6M1]",
            apply: |c| c.opts.cells_reported = false,
            grader: Random { restore: true },
            probe: None,
        },
        Mutant {
            name: "a superseded and killed first insert reported neither way  [V7m4]",
            apply: |c| c.flags.report_neither = true,
            grader: Random { restore: true },
            probe: None,
        },
    ]
}

/// How many of `n` seeded runs the grader fails.
pub fn failing_runs(cfg: &Cfg, g: Grader, n: u64) -> u64 {
    let mut bad = 0;
    for seed in 0..n {
        bad += match g {
            Grader::Random { restore } => {
                let o = run(seed, &RunConfig { restore, ..RunConfig::default() }, cfg);
                u64::from(!grade(&o.nodes, &o.log).is_empty() || !o.settled)
            }
            Grader::Horizon => {
                let rc = RunConfig { steps: 24, retention: Some(4), ..RunConfig::default() };
                let mut o = run(seed, &rc, cfg);
                u64::from(settle(&mut o.nodes, 6).is_none())
            }
        };
    }
    bad
}

pub struct Outcome {
    pub name: &'static str,
    pub random_fail: u64,
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
    let random_fail = failing_runs(&cfg, m.grader, runs);
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
        runs,
        probe,
        killed: random_fail > 0 || probe_kills,
        millis: t0.elapsed().as_millis(),
    }
}
