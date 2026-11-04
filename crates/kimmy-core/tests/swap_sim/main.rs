//! The Q6 property harness for the atomic `$out` swap: a Rust, test-only model of the register and the drop routes,
//! ported from the reviewed Python model (`kimmydb-reviews-0470-0480/q6v2-model/swapmodel5.py`, base `v5`) with the
//! product's relay (appended-only serving, witnessed skips, window order, ADR-224 after completion). Design:
//! `q6-harness-design.md`. No product code is touched: the only product API read is `kimmy_core::merge_state`'s
//! stamp types (and, in a later phase, its `Writer` for the `field` policy).
//!
//! PHASE A (this file's state): the core, without snapshot joins: entries, the register and flip machinery, apply and
//! the append decision, loss records, the relay, the oracle and `finish`, the random run and the grid.
//!
//! Run with `Q6_RUNS=1500` for the full figure (default 40 seeds per configuration of the 36-configuration grid).
//!
//! The PRNG is a seeded xorshift, so the exact counts differ from the Python model's; what must hold is 0 problems on
//! the base.

// Phase A builds the whole core; the pieces phases B to F use (snapshots, the vector coverage, the `Delivery::Held` mode)
// are present but not yet called.
#![allow(dead_code)]

mod apply;
mod coverage;
mod entry;
mod flags;
mod loss;
mod member;
mod omap;
mod oracle;
mod register;
mod relay;
#[path = "../merge_state_sim/rng.rs"]
mod rng;
mod run;
mod scenario;
mod world;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn threads() -> usize {
    std::thread::available_parallelism().map(|n| n.get().min(8)).unwrap_or(2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::*;
    use crate::member::DocOp;
    use crate::oracle::finish;
    use crate::run::{Cfg, grid, run, run_grid};
    use crate::scenario::Sc;

    /// A binding reaches every member and each serves the output; a write that a member accepts after the binding's
    /// stamp but before it has heard of it is lost and reported exactly once on every member (a head).
    #[test]
    fn a_flip_reaches_every_member_and_a_late_write_is_reported_once() {
        let mut sc = Sc::new("v5", &[("W", 0), ("X", 0), ("Y", 0), ("O", 50)], 60);
        let (w_, x, y, o) = (sc.idx("W"), sc.idx("X"), sc.idx("Y"), sc.idx("O"));
        sc.advance(1);
        sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        for m in ["X", "Y", "O"] {
            sc.pull(m, "W");
        }
        sc.advance(10);
        let mut job = sc.ms[w_].start_out(&mut sc.w, vec![1], JobKind::Good, 0);
        sc.advance(12);
        let b = sc.ms[w_].do_bind(&mut sc.w, &mut job, 4).expect("the binding is written");
        sc.advance(14);
        let late = sc.ms[o]
            .local_doc(&mut sc.w, 0, DocOp::Update)
            .expect("O has not heard of the binding");
        assert!(late.stamp > b.stamp, "O's clock is ahead: the write is stamped after the binding");
        sc.quiesce();
        let stage = job.stage.clone();
        for i in [w_, x, y, o] {
            assert_eq!(
                sc.ms[i].serving().as_deref(),
                Some(stage.as_str()),
                "member {} serves the output",
                sc.ms[i].name
            );
        }
        let key = (0u32, Some(late.stamp));
        for i in [w_, x, y, o] {
            let m = &sc.ms[i];
            assert_eq!(
                m.loss.keys().filter(|k| k.head() == key).count(),
                1,
                "one record at {}",
                m.name
            );
            assert_eq!(m.events_emitted.get(&key).copied(), Some(1), "one event at {}", m.name);
        }
        let res = finish(&mut sc.w, &mut sc.ms);
        for k in ["diverged", "oracle_state", "loss_mismatch", "invariant", "loss_body_mismatch"] {
            assert_eq!(res[k], 0, "{k}");
        }
        assert_eq!(res["heads_required"], 1);
        assert_eq!(res["true_late_writes_allowed"], 1);
    }

    /// The model's seed-fixed run is deterministic: the same seed gives the same figures (the harness' own PRNG).
    #[test]
    fn a_run_is_a_function_of_its_seed() {
        let a = run("v5", "v5", 7, Cfg::new(4, 2, 4), false);
        let b = run("v5", "v5", 7, Cfg::new(4, 2, 4), false);
        assert_eq!(a.res, b.res);
        assert_eq!(a.w.all.len(), b.w.all.len());
    }

    /// The base is clean on the 36-configuration grid (members 2..5 x outs 1..3 x keys 3..5), and the run is not vacuous.
    #[test]
    fn the_base_grid_is_clean() {
        let seeds = env_u64("Q6_RUNS", 40);
        let cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        let s = run_grid("v5", "v5", &cfgs, 0, seeds, threads());
        eprintln!("base grid: {} runs", s.runs);
        for (k, v) in &s.tot {
            if k.starts_with("nv_")
                || k.starts_with("cnt_")
                || k.starts_with("refused")
                || k.ends_with("_count")
                || k.contains("heads")
                || k.contains("nonheads")
            {
                eprintln!("  {k} = {v}");
            }
        }
        let p = s.problems();
        assert!(p.is_empty(), "problems on the base: {p:?}");
        // non-vacuity: the mechanisms the base must exercise at this size
        for k in [
            "nv_flips",
            "nv_late_writes(oracle)",
            "nv_loss_records",
            "nv_reports_delayed_until_burial",
            "nv_ldrop",
            "nv_lcreate",
            "nv_abandon_entries",
            "nv_orphans_detected",
            "nv_vanished_writer",
            "nv_invalid_late_binding",
            "nv_concurrent_outs_loser_buried",
            "cnt_witnessed_without_receiving",
            "cnt_lww_losers_not_kept",
        ] {
            assert!(s.get(k) > 0, "vacuous: {k} never happened in {} runs", s.runs);
        }
    }

    /// The mutants that need no snapshot join die on the random grid (phase D grades all 29 with their expected
    /// killers; this is the port check available in phase A). Figures are runs with a problem of any kind.
    #[test]
    fn the_mutants_that_need_no_snapshot_die() {
        let seeds = env_u64("Q6_MUTANT_RUNS", 40);
        let cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        let mutants = [
            "m-keep",
            "m-stage-buried-early",
            "m-bound-out-tombstone-early",
            "m-ldrop-identity-only",
            "m-no-recreate-floor",
            "m-recreate-reuse",
            "m-orphan-age",
            "m-orphan-no-deadline",
            "m-no-loss-records",
            "m-loss-key-collapse",
            "m-report-before-burial",
            "m-record-per-minter",
            "m-ignored-event-not-served",
        ];
        // NOT here, because the Python model does not kill them without a snapshot join either (checked on a copy of
        // `swapmodel5.py` with the snapshot actions at weight 0, 1,440 runs each, 0 problems): `m-digest` (a joiner
        // that cannot recompute the staged digest), `m-pending-accepts` (D10 refusals need a member that knows an
        // event it does not cover), `m-no-pending-dedup`, `m-bury-without-alive`. Phase B (random) and phase C
        // (scripted) kill them.
        let mut survivors = Vec::new();
        for m in mutants {
            let s = run_grid(m, "v5", &cfgs, 0, seeds, threads());
            let p = s.problems();
            eprintln!(
                "{m:32} {} problem kinds: {:?}",
                p.len(),
                p.iter().map(|(k, v)| format!("{k}={}", v.0)).collect::<Vec<_>>()
            );
            if p.is_empty() {
                survivors.push(m);
            }
        }
        assert!(survivors.is_empty(), "mutants that survived: {survivors:?}");
    }

    /// Debug aid: Q6_SEED=<seed> prints the trace of every configuration of the grid that has a problem at that seed.
    #[test]
    #[ignore]
    fn show_failing_seed() {
        let seed = env_u64("Q6_SEED", 0);
        for c in grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]) {
            let r = run("v5", "v5", seed, c, true);
            let bad: Vec<_> = r
                .res
                .iter()
                .filter(|(k, v)| {
                    **v != 0
                        && (["diverged", "loss_extra", "loss_missing", "oracle_state", "invariant"]
                            .contains(&k.as_str())
                            || k.starts_with("inv_"))
                })
                .collect();
            if !bad.is_empty() {
                eprintln!("CONFIG {c:?}: {bad:?}");
                for l in &r.w.log {
                    eprintln!("{l}");
                }
                let mut r = r;
                let o = crate::oracle::oracle(&mut r.w);
                eprintln!(
                    "ORACLE loss keys {:?} top {:?} alive {:?}",
                    o.loss.keys().map(|(k, s)| (*k, s.hlc.wall_ms)).collect::<Vec<_>>(),
                    o.top.as_ref().map(|t| t.to_string()),
                    o.alive
                );
                eprintln!(
                    "LOST {:?}",
                    r.w.lost.iter().map(|&i| r.w.all[i].to_string()).collect::<Vec<_>>()
                );
                for e in r.w.all.iter().filter(|e| e.kind == crate::entry::Kind::Loss) {
                    eprintln!(
                        "LOSSENTRY {} key {:?} held_by {:?}",
                        e,
                        e.lkey.map(|k| (k.key, k.stamp.map(|s| s.hlc.wall_ms))),
                        r.ms.iter()
                            .filter(|m| m.holds(e.idx))
                            .map(|m| m.name.clone())
                            .collect::<Vec<_>>()
                    );
                }
                for m in &r.ms {
                    eprintln!(
                        "member {} cur={:?} serving={:?} loss={:?}",
                        m.name,
                        m.cur.as_ref().map(|c| c.to_string()),
                        m.serving(),
                        m.loss
                            .keys()
                            .map(|k| (k.key, k.stamp.map(|s| s.hlc.wall_ms)))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}
