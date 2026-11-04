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
mod field;
#[cfg(test)]
mod field_tests;
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
mod scenarios;
mod snapshot;
mod trace;
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
        let mut sc = Sc::new("v6", &[("W", 0), ("X", 0), ("Y", 0), ("O", 50)], 60);
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
        let key = (0u32, None, Some(late.stamp));
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

    /// The `field` policy on the 36-configuration grid: 0 problems (the oracle folds `join` over every entry addressed to the
    /// head physical; heads are Q7's, per path; records are per lost head), and the run is not vacuous.
    #[test]
    fn the_field_grid_is_clean() {
        let seeds = env_u64("Q6_RUNS", 40);
        let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        for c in cfgs.iter_mut() {
            c.policy = crate::world::Policy::Field;
        }
        let s = run_grid("v6", "v6", &cfgs, 0, seeds, threads());
        eprintln!("field grid: {} runs", s.runs);
        for (k, v) in &s.tot {
            if k.starts_with("nv_")
                || k.starts_with("cnt_")
                || k.contains("heads")
                || k.contains("nonheads")
            {
                eprintln!("  {k} = {v}");
            }
        }
        let p = s.problems();
        assert!(p.is_empty(), "problems on the field grid: {p:?}");
        assert_eq!(s.get("cnt_grant_above_coverage"), 0);
        assert_eq!(s.get("cnt_page_above_coverage"), 0);
        for k in [
            "nv_flips",
            "nv_late_writes(oracle)",
            "nv_loss_records",
            "nv_snapshot_joins",
            "nv_late_delete_heads",
            "cnt_appended_in_position",
            "cnt_dead_page_versions_retained",
            "cnt_state_held_refusals",
            "cnt_witnessed_without_receiving",
            "cnt_covered_skips",
            "cnt_snap_versions_as_entries",
        ] {
            assert!(s.get(k) > 0, "vacuous: {k} never happened in {} runs", s.runs);
        }
    }

    /// The model's other base configurations (the heavy one, the delete-heavy mix, the baseline): 0 problems.
    /// `Q6_HEAVY_RUNS` (default 20), `Q6_RUNS` (default 40 seeds per configuration of the grid).
    #[test]
    fn the_other_base_configurations_are_clean() {
        let heavy = {
            let mut c = Cfg::new(5, 3, 5);
            c.steps = Some(600);
            c
        };
        let s = run_grid("v6", "v6", &[heavy], 0, env_u64("Q6_HEAVY_RUNS", 20), threads());
        assert!(s.problems().is_empty(), "heavy: {:?}", s.problems());
        let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        for c in cfgs.iter_mut() {
            c.delete_bias = true;
        }
        let s = run_grid("v6", "v6", &cfgs, 0, env_u64("Q6_RUNS", 40), threads());
        assert!(s.problems().is_empty(), "delete-heavy: {:?}", s.problems());
        eprintln!(
            "delete-heavy: {} runs, late delete heads held only as state in {} runs",
            s.runs,
            s.get("nv_late_delete_head_held_only_as_state")
        );
    }

    /// Generate traces with the vendored tracer in the machine's small-job scope (256 MB, 60 s, no bytecode), or read a
    /// directory of traces made elsewhere (`Q6DIFF_DIR`, for the large samples generated under the Python lane).
    fn trace_dir(tag: &str, kind: &str, args: &[&str]) -> std::path::PathBuf {
        if let Ok(d) = std::env::var("Q6DIFF_DIR") {
            return std::path::PathBuf::from(d);
        }
        let dir =
            std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("q6diff-{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let tracer = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/swap_sim/reference_model/q6diff_gen.py");
        let st = std::process::Command::new("systemd-run")
            .args(["--user", "--scope", "-q", "-p", "MemoryMax=256M", "-p", "MemorySwapMax=0", "-p", "TasksMax=16", "--", "timeout", "60", "env", "PYTHONDONTWRITEBYTECODE=1", "python3", "-I", "-B"])
            .arg(&tracer)
            .arg(kind)
            .arg(&dir)
            .args(args)
            .status()
            .expect("python3 and systemd-run must be on the PATH: the differential fails, it does not skip");
        assert!(st.success(), "the tracer failed");
        dir
    }

    fn replay_all(dir: &std::path::Path) -> (usize, usize, usize) {
        let files = crate::trace::traces_in(dir);
        assert!(!files.is_empty(), "no traces in {}", dir.display());
        let (mut events, mut views, mut failures) = (0, 0, Vec::new());
        for f in &files {
            match crate::trace::replay_file(f) {
                Ok(o) => {
                    events += o.events;
                    views += o.member_views;
                }
                Err(e) => failures.push(e),
            }
        }
        for e in &failures {
            eprintln!("{e}");
        }
        eprintln!(
            "replayed {} traces: {events} events, {views} member states compared, {} disagreements",
            files.len(),
            failures.len()
        );
        assert!(
            failures.is_empty(),
            "{} trace(s) disagree with the model; first: {}",
            failures.len(),
            failures[0]
        );
        (files.len(), events, views)
    }

    /// The scripted scenarios, run by the vendored model (the Python twins) and replayed on the harness.
    #[test]
    fn the_differential_replays_the_scripted_traces() {
        let dir = trace_dir("scripted", "scripted", &[]);
        replay_all(&dir);
    }

    /// A bounded sample of random runs of the model, on the base and on every mutant the model has (`Q6DIFF_RUNS` runs of the
    /// base and `Q6DIFF_MUTANT_RUNS` of each mutant; `Q6DIFF_DIR` reads a directory made under the Python lane).
    #[test]
    fn the_differential_replays_random_traces_of_the_model() {
        if let Ok(d) = std::env::var("Q6DIFF_DIR") {
            replay_all(std::path::Path::new(&d));
            return;
        }
        let base_runs = env_u64("Q6DIFF_RUNS", 36).to_string();
        let dir = trace_dir("random", "random", &["--mode", "v6", "--count", &base_runs]);
        let mutant_runs = env_u64("Q6DIFF_MUTANT_RUNS", 0);
        if mutant_runs > 0 {
            for m in crate::flags::MUTANTS.iter().filter(|m| **m != "m-stale-src") {
                let st = std::process::Command::new("systemd-run")
                    .args([
                        "--user",
                        "--scope",
                        "-q",
                        "-p",
                        "MemoryMax=256M",
                        "-p",
                        "MemorySwapMax=0",
                        "-p",
                        "TasksMax=16",
                        "--",
                        "timeout",
                        "60",
                        "env",
                        "PYTHONDONTWRITEBYTECODE=1",
                        "python3",
                        "-I",
                        "-B",
                    ])
                    .arg(
                        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                            .join("tests/swap_sim/reference_model/q6diff_gen.py"),
                    )
                    .arg("random")
                    .arg(&dir)
                    .args(["--mode", m, "--count", &mutant_runs.to_string()])
                    .status()
                    .unwrap();
                assert!(st.success());
            }
        }
        replay_all(&dir);
    }

    /// The differential has teeth: a trace with one changed value (a member's clock after the third event) is reported at that
    /// event, with the path of the field.
    #[test]
    fn the_differential_notices_a_changed_trace() {
        let dir = trace_dir("teeth", "scripted", &[]);
        let src = dir.join("s-flip-v6.jsonl");
        let text = std::fs::read_to_string(&src).unwrap();
        let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
        let mut ev: serde_json::Value = serde_json::from_str(&lines[3]).unwrap();
        let hlc = ev["ms"][0]["hlc"].as_i64().unwrap();
        ev["ms"][0]["hlc"] = serde_json::json!(hlc + 1);
        lines[3] = ev.to_string();
        let bad = dir.join("changed.jsonl");
        std::fs::write(&bad, lines.join("\n")).unwrap();
        let err = crate::trace::replay_file(&bad).err().expect("the changed trace must disagree");
        assert!(err.contains("FIRST DISAGREEMENT at event 3"), "{err}");
        assert!(err.contains(".hlc: python"), "{err}");
        // the unchanged trace agrees
        assert!(crate::trace::replay_file(&src).is_ok());
    }

    /// The vendored Python model is the reviewed one (`reference_model/MODEL-HASHES.txt`). The differential (a later phase)
    /// is made from it; a changed byte fails here first.
    #[test]
    fn the_vendored_model_is_the_reviewed_one() {
        use sha2::{Digest, Sha256};
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/swap_sim/reference_model");
        let want = "40a44c8838f55988cda48c7f296edb13a759803d1790d812888126275d948084";
        let bytes = std::fs::read(dir.join("swapmodel6a.py")).unwrap();
        let got: String = Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(got, want, "swapmodel6a.py");
        let tracer = "79d9c55e7f363475ef1a1a13eb67e50af8a69a0c247acf514e2be11a87c1dc8b";
        let bytes = std::fs::read(dir.join("q6diff_gen.py")).unwrap();
        let got: String = Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(got, tracer, "q6diff_gen.py");
        let recorded = std::fs::read_to_string(dir.join("MODEL-HASHES.txt")).unwrap();
        assert!(recorded.contains(want) && recorded.contains(tracer));
    }

    /// The model's seed-fixed run is deterministic: the same seed gives the same figures (the harness' own PRNG).
    #[test]
    fn a_run_is_a_function_of_its_seed() {
        let a = run("v6", "v6", 7, Cfg::new(4, 2, 4), false);
        let b = run("v6", "v6", 7, Cfg::new(4, 2, 4), false);
        assert_eq!(a.res, b.res);
        assert_eq!(a.w.all.len(), b.w.all.len());
    }

    /// The base is clean on the 36-configuration grid (members 2..5 x outs 1..3 x keys 3..5), and the run is not vacuous.
    #[test]
    fn the_base_grid_is_clean() {
        let seeds = env_u64("Q6_RUNS", 40);
        let cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        let s = run_grid("v6", "v6", &cfgs, 0, seeds, threads());
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
        // invariant (i): no grant and no page names an entry above its sender's coverage; rule B never fires in order
        assert_eq!(s.get("cnt_grant_above_coverage"), 0);
        assert_eq!(s.get("cnt_page_above_coverage"), 0);
        assert_eq!(
            s.get("cnt_info_parked"),
            0,
            "in order a register event never arrives ahead of its position"
        );
        assert_eq!(s.get("cnt_register_read_from_a_member_that_is_not_the_sender"), 0);
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
            // the snapshot routes
            "nv_snapshot_joins",
            "nv_snapshot_after_postflip_write",
            "nv_snapshots_carrying_loss_records",
            "cnt_snap_completed",
            "cnt_snap_deletes_carried_with_id",
            "cnt_snap_versions_as_entries",
            "cnt_windows_served_by_a_join_in_progress",
            "cnt_state_held_refusals",
            "cnt_dead_page_versions_retained",
            "cnt_register_events_not_carried_above_grant",
            "cnt_bind_between_first_and_last_page",
        ] {
            assert!(s.get(k) > 0, "vacuous: {k} never happened in {} runs", s.runs);
        }
    }

    /// Debug aid: Q6_SEED=<seed> prints the trace of every configuration of the grid that has a problem at that seed.
    #[test]
    #[ignore = "debug aid: Q6_SEED=<seed> prints the trace of every grid configuration with a problem at that seed"]
    fn show_failing_seed() {
        let seed = env_u64("Q6_SEED", 0);
        let policy = if std::env::var("Q6_POLICY").is_ok_and(|p| p == "field") {
            crate::world::Policy::Field
        } else {
            crate::world::Policy::Document
        };
        for mut c in grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]) {
            c.policy = policy;
            let r = run("v6", "v6", seed, c, true);
            let bad: Vec<_> = r
                .res
                .iter()
                .filter(|(k, v)| {
                    **v != 0
                        && ([
                            "diverged",
                            "loss_extra",
                            "loss_missing",
                            "oracle_state",
                            "invariant",
                            "q7_records_diverged",
                        ]
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
                    o.loss.keys().map(|(k, _, s)| (*k, s.hlc.wall_ms)).collect::<Vec<_>>(),
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
                    for (n, p) in m.phys.iter().filter(|(_, p)| p.alive) {
                        for (k, v) in p.docs.iter() {
                            if let Some(st) = &v.state
                                && !st.losses.is_empty()
                            {
                                eprintln!(
                                    "Q7 {} {} key {} records {:?}",
                                    m.name,
                                    n,
                                    k,
                                    st.losses
                                        .keys()
                                        .map(|lk| format!("{:?}@{}", lk.kind, lk.stamp.hlc.wall_ms))
                                        .collect::<Vec<_>>()
                                );
                            }
                        }
                    }
                }
                eprintln!(
                    "ALLOWED {:?}",
                    o.allowed
                        .iter()
                        .map(|(k, p, s)| (*k, p.map(|p| r.w.path_label(p)), s.hlc.wall_ms))
                        .collect::<Vec<_>>()
                );
                for m in &r.ms {
                    for k in m.loss.keys() {
                        if !o.allowed.contains(&(k.key, k.path, k.stamp.unwrap())) {
                            eprintln!(
                                "EXTRA at {}: key {} path {:?} stamp {:?} body {:?}",
                                m.name,
                                k.key,
                                k.path.map(|p| r.w.path_label(p)),
                                k.stamp.map(|s| s.hlc.wall_ms),
                                m.loss.get(k)
                            );
                        }
                    }
                    eprintln!(
                        "member {} vanished={} cur={:?} serving={:?} loss={:?}",
                        m.name,
                        m.vanished,
                        m.cur.as_ref().map(|c| c.to_string()),
                        m.serving(),
                        m.loss
                            .keys()
                            .map(|k| (
                                k.key,
                                k.path.map(|p| r.w.path_label(p)),
                                k.stamp.map(|s| s.hlc.wall_ms)
                            ))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }

    /// Seed 530 by hand. B holds C's late write and mints its record; A gets the record from B; C vanishes after
    /// staging, then B vanishes. The write's entry is then held by no live member (it is lost with its holders), but
    /// its record survives at A. The record names a real late write, so it is legitimate: the oracle's ALLOWED set is
    /// the whole write log's late writes, and `loss_extra` fires only for a record outside it. The same scenario with
    /// only C vanishing holds the entry at B.
    #[test]
    fn corner_record_outlives_its_lost_entry() {
        for second_vanish in [false, true] {
            let (mut sc, late) = corner(second_vanish, false);
            let a = sc.idx("A");
            let res = finish(&mut sc.w, &mut sc.ms);
            for k in [
                "diverged",
                "div_state",
                "div_loss",
                "div_replay",
                "oracle_state",
                "loss_mismatch",
                "loss_missing",
                "loss_extra",
                "invariant",
            ] {
                assert_eq!(res[k], 0, "{k} (second vanish: {second_vanish})");
            }
            let a_loss: Vec<(u32, i64)> =
                sc.ms[a].loss.keys().map(|k| (k.key, hlc_of(&k.stamp.unwrap()))).collect();
            assert_eq!(a_loss, vec![(0, 64)], "A holds exactly the record of C's late write");
            assert_eq!(late.stamp, stamp(64, 2));
            let entry_lost = sc.w.lost.contains(&late.idx);
            assert_eq!(
                entry_lost, second_vanish,
                "the write's entry is lost only when both holders are gone"
            );
        }
    }

    /// (b) The record is an ordinary entry, appended and served onward: a member that joins later by windows from A
    /// ends with the same record, and so does a fresh replica taking what the live members serve. The record's
    /// minter (B) is gone; nothing about the record needs it.
    #[test]
    fn a_later_member_and_a_replica_get_the_record_from_a() {
        let (mut sc, _late) = corner(true, true);
        let (a, n) = (sc.idx("A"), sc.idx("N"));
        let stage = sc.ms[a].serving().expect("A serves the output");
        assert!(
            sc.ms[n].loss.is_empty() && sc.ms[n].serving().as_deref() == Some(L),
            "N has heard of nothing yet"
        );
        sc.pull("N", "A");
        sc.quiesce();
        let key = |m: &crate::member::Member| {
            m.loss.keys().map(|k| (k.key, hlc_of(&k.stamp.unwrap()))).collect::<Vec<_>>()
        };
        assert_eq!(key(&sc.ms[n]), vec![(0, 64)], "N holds the record after pulling from A");
        assert_eq!(sc.ms[n].serving().as_deref(), Some(stage.as_str()));
        assert_eq!(
            sc.ms[n].events_emitted.values().copied().collect::<Vec<_>>(),
            vec![1],
            "one event at N"
        );
        // a fresh replica, taking what the live members serve in the relay's order, holds it too
        let mut z = crate::member::Member::new(sc.ms.len(), "Z0", 0);
        let mut order: Vec<EntryRef> =
            sc.w.all.iter().filter(|e| !sc.w.lost.contains(&e.idx)).cloned().collect();
        order.sort_by_key(|e| e.stamp);
        for e in &order {
            crate::relay::take_quiesce_replica(&mut z, &sc.ms, &mut sc.w, e);
        }
        z.settle(&mut sc.w);
        assert_eq!(key(&z), vec![(0, 64)], "the replica holds the record");
        assert_eq!(z.state(), sc.ms[a].state());
        let res = finish(&mut sc.w, &mut sc.ms);
        for k in
            ["diverged", "div_replay", "loss_extra", "loss_missing", "oracle_state", "invariant"]
        {
            assert_eq!(res[k], 0, "{k}");
        }
    }

    /// An event that is not reachable (its entry died with a vanished member before any live member held it, so no
    /// live member has it in effect or can make it take effect) makes no write late: the allowed set does not absorb
    /// writes that only such an event would make late.
    #[test]
    fn an_unreachable_event_does_not_make_a_write_allowed() {
        for event_survives in [false, true] {
            let mut sc = Sc::new("v6", &[("A", 0), ("X", 0), ("Y", 50)], 60);
            let (a, x, y) = (sc.idx("A"), sc.idx("X"), sc.idx("Y"));
            sc.advance(1);
            sc.ms[a].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
            sc.pull("X", "A");
            sc.pull("Y", "A");
            sc.advance(10);
            let drop = sc.ms[x].local_ldrop(&mut sc.w).expect("X drops the name");
            sc.advance(14);
            // Y is ahead and has not heard of the drop: its write is stamped after it
            let w = sc.ms[y].local_doc(&mut sc.w, 0, DocOp::Update).unwrap();
            assert!(w.stamp > drop.stamp);
            sc.window("A", "Y", &w);
            if event_survives {
                sc.pull("A", "X");
            }
            let mut jx = sc.ms[x].start_out(&mut sc.w, vec![1], JobKind::Vanish, 0);
            sc.ms[x].do_bind(&mut sc.w, &mut jx, 3);
            sc.quiesce();
            assert_eq!(
                sc.w.lost.contains(&drop.idx),
                !event_survives,
                "the drop is lost only when no live member took it"
            );
            let o = crate::oracle::oracle(&mut sc.w);
            assert_eq!(
                o.allowed.contains(&(0, None, w.stamp)),
                event_survives,
                "allowed iff the event is reachable"
            );
            let res = finish(&mut sc.w, &mut sc.ms);
            for k in [
                "diverged",
                "oracle_state",
                "loss_mismatch",
                "loss_extra",
                "loss_missing",
                "invariant",
                "div_replay",
                "div_replay_interleaved",
            ] {
                assert_eq!(res[k], 0, "{k} (event survives: {event_survives})");
            }
        }
    }

    /// The scenario of seed 530. `with_n` adds a member N that has heard of nothing.
    fn corner(second_vanish: bool, with_n: bool) -> (Sc, EntryRef) {
        let mut members = vec![("A", 0), ("B", 0), ("C", 50)];
        if with_n {
            members.push(("N", 0));
        }
        let mut sc = Sc::new("v6", &members, 60);
        let (a, b, c) = (sc.idx("A"), sc.idx("B"), sc.idx("C"));
        let live = members.len();
        sc.advance(1);
        sc.ms[a].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.pull("B", "A");
        sc.pull("C", "A");
        sc.advance(10);
        let mut job = sc.ms[b].start_out(&mut sc.w, vec![1], JobKind::Good, 0);
        sc.advance(12);
        sc.ms[b].do_bind(&mut sc.w, &mut job, live).unwrap();
        sc.advance(14);
        let late = sc.ms[c].local_doc(&mut sc.w, 0, DocOp::Update).unwrap();
        sc.window("B", "C", &late); // B has flipped: the write is history there and B mints the record
        sc.pull("A", "B");
        let mut jc = sc.ms[c].start_out(&mut sc.w, vec![2], JobKind::Vanish, 0);
        sc.ms[c].do_bind(&mut sc.w, &mut jc, live);
        if second_vanish {
            let mut jb = sc.ms[b].start_out(&mut sc.w, vec![3], JobKind::Vanish, 0);
            sc.ms[b].do_bind(&mut sc.w, &mut jb, live - 1);
        }
        if !with_n {
            sc.quiesce();
        }
        (sc, late)
    }

    /// Every mutant on the v6 base with snapshot joins, on the random grid. A fixed seed range makes the survivors a
    /// deterministic set, asserted below; phase D grades each mutant against its expected killer. The survivors at this
    /// size are the mutants the random runs cannot kill and a scripted scenario does (or that are equivalent):
    /// `m-visible-newest` (equivalent in order; the route probes kill it), `m-rule-b-off` (rule B never fires in order;
    /// `rule_b_a_held_bind_is_information_until_covered` kills it), `m-dead-page-dropped` (0 in 28,800 in-order runs in
    /// the model; `seed_885_shape...` kills it), `m-loss-outside-prefix` (the model: fails nothing).
    #[test]
    fn the_mutants_on_the_snapshot_grid() {
        let seeds = env_u64("Q6_MUTANT_RUNS", 40);
        let cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        let mut survivors = Vec::new();
        for m in crate::flags::MUTANTS {
            let s = run_grid(m, "v6", &cfgs, 0, seeds, threads());
            let p = s.problems();
            eprintln!(
                "{m:32} {} kinds: {:?}",
                p.len(),
                p.iter().map(|(k, v)| format!("{k}={}", v.0)).collect::<Vec<_>>()
            );
            if p.is_empty() {
                survivors.push(m);
            }
        }
        eprintln!("survivors at {} runs: {survivors:?}", seeds * cfgs.len() as u64);
        let expected = std::env::var("Q6_MUTANT_RUNS").is_err().then_some(());
        if expected.is_some() {
            // the rare ones the model kills in about 5 of 28,800 runs may survive 1,440 runs; scripted scenarios kill them
            let allowed_survivors = [
                "m-visible-newest",
                "m-rule-b-off",
                "m-dead-page-dropped",
                "m-loss-outside-prefix",
                "m-pending-narrow",
                "m-no-defer-received",
                "m-no-append-in-position",
                "m-snap-register-late",
                "m-snap-drop-loss",
                "m-snap-no-loss-records",
                "m-snap-delete-bare",
                "m-snap-no-orphan-burials",
            ];
            let unexpected: Vec<&&str> =
                survivors.iter().filter(|m| !allowed_survivors.contains(m)).collect();
            assert!(
                unexpected.is_empty(),
                "mutants that survived but are not in the allowed list: {unexpected:?}"
            );
            for must in [
                "m-digest",
                "m-pending-accepts",
                "m-no-pending-dedup",
                "m-bury-without-alive",
                "m-grant-before-pages",
                "m-snap-no-events",
                "m-stale-src",
            ] {
                assert!(!survivors.contains(&must), "{must} must die on the snapshot grid");
            }
        }
    }
}
