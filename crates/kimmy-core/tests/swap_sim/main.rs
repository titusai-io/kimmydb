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
mod classes;
mod coverage;
mod entry;
mod field;
#[cfg(test)]
mod field_tests;
mod flags;
mod loss;
mod member;
#[cfg(test)]
mod metamorphic;
mod omap;
mod oracle;
mod register;
mod relay;
mod restore;
mod rng;
mod routes;
mod run;
mod scenario;
mod scenarios;
mod snapshot;
mod trace;
#[cfg(test)]
mod vector;
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

    /// `m-visible-newest` is EQUIVALENT to the base under rule B (N2, the review's reasoning, here RUN): an event enters the register
    /// list only once covered and coverage is monotone, so the newest-applicable scan never sees an uncovered event. Asserted: on
    /// the whole base grid (and the product routes) the mutant's results equal the base's, run for run, for every counter and check.
    #[test]
    fn m_visible_newest_is_equivalent_to_the_base_under_rule_b() {
        let seeds = env_u64("Q6_EQUIV_RUNS", 20);
        for routes in [false, true] {
            for c in grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]) {
                let mut c = c;
                if routes {
                    c.routes = Some(crate::run::Routes {
                        f: 0.3,
                        hold: false,
                        repair: false,
                        doc_hold: true,
                        doc_repair: true,
                    });
                }
                for seed in 0..seeds {
                    let a = crate::run::run("v6", "v6", seed, c, false);
                    let b = crate::run::run("m-visible-newest", "v6", seed, c, false);
                    assert_eq!(
                        a.res, b.res,
                        "routes {routes} {}/{}/{} seed {seed}",
                        c.members, c.outs, c.keys
                    );
                }
            }
        }
    }

    /// M6: the Rust-only mutants of the product routes are KILLED on the product-route grid (document policy, doc_hold + doc_repair
    /// f 0.3, `Q6_MUTROUTE_RUNS` seeds, default 250): H-Q6-R2 (a window delivers the highest stamp: not contiguous) and the release
    /// condition (a held entry released without its predecessors). And R4's grant gate: `m-snap-register-late` is equivalent to the
    /// base under rule B (the review), so its kill is REQUIRED in combination with rule B off.
    #[test]
    fn the_product_route_mutants_and_the_grant_gate_with_rule_b_off_are_killed() {
        let seeds = env_u64("Q6_MUTROUTE_RUNS", 250);
        let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        for c in cfgs.iter_mut() {
            c.routes = Some(crate::run::Routes {
                f: 0.3,
                hold: false,
                repair: false,
                doc_hold: true,
                doc_repair: true,
            });
        }
        let mut alive: Vec<&str> = Vec::new();
        for m in crate::flags::PRODUCT_MUTANTS {
            let s = run_grid(m, "v6", &cfgs, 0, seeds, threads());
            let p = s.problems();
            eprintln!(
                "{m:36} {} kinds: {:?}",
                p.len(),
                p.iter().map(|(k, v)| format!("{k}={}", v.0)).collect::<Vec<_>>()
            );
            // the base's own classified findings (DF-1, DF-2) are in every row: look at the checks the mutant adds
            let adds = p.keys().any(|k| {
                k.starts_with("inv_window")
                    || k.starts_with("inv_vector")
                    || k == "inv_partial_flip" && s.get("inv_partial_flip") > 30
                    || k.starts_with("inv_bury")
                    || k.starts_with("inv_orphan")
            });
            if !adds {
                alive.push(m);
            }
        }
        assert!(alive.is_empty(), "not killed on the product-route grid: {alive:?}");
        // R4's grant gate: the combination must be worse than rule B off alone (the mutant's own mechanism fires and its
        // consequences add to the base's: more runs with a partial flip, a divergence or a served mismatch)
        let off = run_grid("m-rule-b-off", "v6", &cfgs, 0, seeds, threads());
        let both = run_grid("m-snap-register-late+m-rule-b-off", "v6", &cfgs, 0, seeds, threads());
        let sum = |s: &crate::run::Summary| {
            ["inv_partial_flip", "diverged", "served_mismatch", "inv_bury_without_newer_visible"]
                .iter()
                .map(|k| s.get(k))
                .sum::<i64>()
        };
        eprintln!(
            "rule B off alone: {} (read above grant {}); with the grant gate off: {} (read above grant {})",
            sum(&off),
            off.get("cnt_register_events_read_above_grant"),
            sum(&both),
            both.get("cnt_register_events_read_above_grant")
        );
        assert!(
            both.get("cnt_register_events_read_above_grant") > 0,
            "the gate-off mutant never reads above the grant"
        );
        assert!(sum(&both) > sum(&off), "the grant gate is not killed even with rule B off");
    }

    /// ITEM Q: the product's mark mechanism (`World::product_marks`: in-window entries never held for a held predecessor, the vector
    /// raised at release, covered superseded marks dropped after each window, engine.rs `drop_superseded_covered_marks`) against the
    /// model's bookkeeping (OFF), on the SAME trace (`Q6DIFF_DIR`, the traces of the product-route rows). Every DOCUMENT-LAYER outcome
    /// (what each member serves, its documents, its loss records, the events it emitted, its invariants after every event, the verdict:
    /// divergences, mismatches, partial flips, the O8 markers) must be identical; the bookkeeping (held marks left, entries served,
    /// marks dropped, releases) is listed. Any document-layer difference is a FINDING.
    #[test]
    #[ignore = "item Q: Q6DIFF_DIR selects the traces of one row"]
    fn item_q_product_marks_on_and_off_agree_on_the_document_layer() {
        let dir = std::env::var("Q6DIFF_DIR").expect("Q6DIFF_DIR");
        let files = crate::trace::traces_in(std::path::Path::new(&dir));
        assert!(!files.is_empty());
        let run = |f: &std::path::Path, on: bool| -> Vec<String> {
            crate::trace::COLLECT.with(|c| *c.borrow_mut() = Some((on, Vec::new())));
            let _ = crate::trace::replay_file(f);
            crate::trace::COLLECT.with(|c| c.borrow_mut().take().unwrap().1)
        };
        let (mut diffs, mut events) = (Vec::new(), 0usize);
        let mut book = [0i64; 8]; // held_oop, appended, dropped, released: OFF then ON
        let num = |l: &str, k: &str| -> i64 {
            l.split(&format!("{k}="))
                .nth(1)
                .and_then(|r| r.split(' ').next())
                .and_then(|x| x.parse().ok())
                .unwrap_or(0)
        };
        let mut differing_book = 0;
        let (mut book_event_diffs, mut book_traces) = (0usize, 0usize);
        for f in &files {
            let off = run(f, false);
            let on = run(f, true);
            let doc = |v: &Vec<String>| {
                v.iter().filter(|l| !l.starts_with("BOOK")).cloned().collect::<Vec<_>>()
            };
            let (a, b) = (doc(&off), doc(&on));
            // bookkeeping that differs mid-run (not compared): events after which the marks held, the entries served, the marks
            // dropped or the held-behind-held decisions differ between OFF and ON
            let bev = |v: &Vec<String>| {
                v.iter().filter(|l| l.starts_with("BOOKEV")).cloned().collect::<Vec<_>>()
            };
            let (ea, eb) = (bev(&off), bev(&on));
            let n = ea.iter().zip(&eb).filter(|(x, y)| x != y).count();
            book_event_diffs += n;
            if n > 0 {
                book_traces += 1;
            }
            events += a.len();
            let first = a
                .iter()
                .zip(&b)
                .position(|(x, y)| x != y)
                .or((a.len() != b.len()).then_some(a.len().min(b.len())));
            if let Some(i) = first {
                diffs.push(format!(
                    "{}: event {i}: OFF {:.200} <> ON {:.200}",
                    f.file_name().unwrap().to_string_lossy(),
                    a.get(i).cloned().unwrap_or_default(),
                    b.get(i).cloned().unwrap_or_default()
                ));
            }
            let (bo, bn) = (
                off.iter().find(|l| l.starts_with("BOOK ")),
                on.iter().find(|l| l.starts_with("BOOK ")),
            );
            if let (Some(bo), Some(bn)) = (bo, bn) {
                if bo != bn {
                    differing_book += 1;
                }
                for (j, k) in
                    ["held_oop", "appended", "dropped", "released_in_window"].iter().enumerate()
                {
                    book[j] += num(bo, k);
                    book[4 + j] += num(bn, k);
                }
            }
        }
        eprintln!(
            "ITEM Q {dir}: {} traces, {events} events; document-layer differences {}; traces whose end bookkeeping differs {differing_book}; bookkeeping differs mid-run after {book_event_diffs} events in {book_traces} traces; \
             totals OFF/ON: held marks left {}/{}, entries served {}/{}, marks dropped {}/{}, releases in window {}/{}",
            files.len(),
            diffs.len(),
            book[0],
            book[4],
            book[1],
            book[5],
            book[2],
            book[6],
            book[3],
            book[7]
        );
        for d in diffs.iter().take(10) {
            eprintln!("  DOCUMENT-LAYER DIFFERENCE {d}");
        }
        assert!(diffs.is_empty(), "item Q: {} traces differ on the document layer", diffs.len());
    }

    /// M5, the abandon-and-restart mode (`Cfg::restart_joins`): a join whose sender has vanished lands no further page and is
    /// restarted from a live eligible peer (the product cannot complete a join from a dead member). The base grid and the
    /// product-route grid (document policy) stay clean (every problem classified), and the mode is not vacuous: joins are restarted.
    /// `dead_sender_join_open_at_end` (a join left open because no peer could serve it) is printed.
    #[test]
    fn the_restart_joins_mode_restarts_a_dead_senders_join_and_stays_clean() {
        let seeds = env_u64("Q6_RESTART_RUNS", 40);
        for routes in [false, true] {
            let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
            for c in cfgs.iter_mut() {
                c.restart_joins = true;
                if routes {
                    c.routes = Some(crate::run::Routes {
                        f: 0.3,
                        hold: false,
                        repair: false,
                        doc_hold: true,
                        doc_repair: true,
                    });
                }
            }
            let (s, classes, residual) =
                crate::run::run_grid_classified("v6", &cfgs, 0, seeds, threads());
            eprintln!(
                "restart_joins routes {routes}: {} runs; restarted {} waiting {} open at end {}; classes {classes:?}; unclassified {residual:?}",
                s.runs,
                s.get("cnt_join_restarted_dead_sender"),
                s.get("cnt_dead_sender_join_waiting"),
                s.get("cnt_dead_sender_join_open_at_end")
            );
            assert!(s.get("cnt_join_restarted_dead_sender") > 0, "vacuous: no join was restarted");
            assert!(residual.is_empty(), "unclassified: {residual:?}");
        }
    }

    /// `K = 4` on the field grid (design note 1.4, rule 2; `outReplaced` and Q7's records share one per-document `K`): 0 problems
    /// under the relaxed required set (up to `K` per document, the newest by loser stamp), no member holds a record that `K` newer
    /// ones push out, and the run is not vacuous (records are dropped, heads exempted). The default of the harness is `K` off.
    #[test]
    fn the_field_grid_with_k_4_is_clean() {
        let seeds = env_u64("Q6_K_RUNS", 40);
        let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        for c in cfgs.iter_mut() {
            c.policy = crate::world::Policy::Field;
            c.k = Some(4);
        }
        let s = run_grid("v6", "v6", &cfgs, 0, seeds, threads());
        eprintln!(
            "field grid with K = 4: {} runs; records dropped {}, runs with an exempt head {}, heads required {}",
            s.runs,
            s.get("cnt_k_dropped"),
            s.get("k_exempt_heads"),
            s.get("heads_required")
        );
        let p = s.problems();
        assert!(p.is_empty(), "problems on the K = 4 field grid: {p:?}");
        assert!(s.get("cnt_k_dropped") > 0, "vacuous: no record was dropped past K");
        assert!(s.get("k_exempt_heads") > 0, "vacuous: no head was exempted by K");
    }

    /// Each Rust-only mutant of `K` fails the `K = 4` grid.
    #[test]
    fn the_k_mutants_fail_the_k_4_grid() {
        let seeds = env_u64("Q6_K_MUTANT_RUNS", 40);
        let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        for c in cfgs.iter_mut() {
            c.policy = crate::world::Policy::Field;
            c.k = Some(4);
        }
        for m in crate::flags::K_MUTANTS {
            let s = run_grid(m, "v6", &cfgs, 0, seeds, threads());
            let p = s.problems();
            eprintln!("{m:28} {} kinds: {:?}", p.len(), p.keys().collect::<Vec<_>>());
            assert!(!p.is_empty(), "{m} survived the K = 4 grid");
        }
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

    /// Generate traces with the vendored tracer in the machine's small-job scope (256 MB, no bytecode; no wall-clock bound, the tracer is bounded by its event counts, so a loaded machine only makes it slower), or read a
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
            .args(["--user", "--scope", "-q", "-p", "MemoryMax=256M", "-p", "MemorySwapMax=0", "-p", "TasksMax=16", "--", "env", "PYTHONDONTWRITEBYTECODE=1", "python3", "-I", "-B"])
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
        replay_all_mode(dir, crate::trace::ReplayMode::Document, &[]).0
    }

    /// Replay every trace of a directory in a mode; the figures, and under `FieldRegisterLayer` the traces that stopped at a
    /// document-layer difference (events replayed before it are all compared) .
    /// (trace file, line, the document-layer difference) where a register-layer replay stopped
    type Stop = (String, usize, String);

    fn replay_all_mode(
        dir: &std::path::Path,
        mode: crate::trace::ReplayMode,
        known: &[(&str, &str)],
    ) -> ((usize, usize, usize), Vec<Stop>, usize) {
        let files = crate::trace::traces_in(dir);
        assert!(!files.is_empty(), "no traces in {}", dir.display());
        let (mut events, mut views, mut failures, mut stopped, mut raised) =
            (0, 0, Vec::new(), Vec::new(), 0);
        let mut known_seen = 0;
        for f in &files {
            if mode == crate::trace::ReplayMode::FieldExact && !crate::trace::is_field_safe(f) {
                continue;
            }
            match crate::trace::replay_file_mode(f, mode) {
                Ok(o) => {
                    events += o.events;
                    views += o.member_views;
                    raised += o.hlc_raised_mid_join;
                    if let Some((line, d)) = o.stopped_at {
                        stopped.push((
                            f.file_name().unwrap().to_string_lossy().into_owned(),
                            line,
                            d,
                        ));
                    }
                }
                Err(e) => {
                    // a finding the manager has been told of, asserted by name and text so that it flips when fixed
                    let name = f.file_name().unwrap().to_string_lossy().into_owned();
                    if let Some((_, text)) = known.iter().find(|(n, _)| *n == name) {
                        assert!(e.contains(text), "{name}: the known disagreement changed: {e}");
                        known_seen += 1;
                    } else {
                        failures.push(e);
                    }
                }
            }
        }
        assert_eq!(
            known_seen,
            known.len(),
            "a known disagreement is gone (fixed?) or its trace was not replayed"
        );
        for e in &failures {
            eprintln!("{e}");
        }
        eprintln!(
            "replayed {} traces ({mode:?}): {events} events, {views} member states compared, {} disagreements, {} stopped at a document-layer difference, member clocks raised above the model's mid-join {raised} times",
            files.len(),
            failures.len(),
            stopped.len()
        );
        assert!(
            failures.is_empty(),
            "{} trace(s) disagree with the model; first: {}",
            failures.len(),
            failures[0]
        );
        ((files.len(), events, views), stopped, raised)
    }

    /// The scripted scenarios, run by the vendored model (the Python twins) and replayed on the harness.
    #[test]
    fn the_differential_replays_the_scripted_traces() {
        let dir = trace_dir("scripted", "scripted", &[]);
        replay_all(&dir);
    }

    /// The scripted scenarios without a delete and without concurrent same-key writes (the tracer's `field_safe` marker), replayed
    /// under the `field` policy: last-writer-wins and the join agree there, so every difference is a disagreement. (FINDING
    /// F-3-1 of report 3, an entry whose stamp equals the held page version's, was fixed: the same write held by a page state is
    /// appended as a winner, as the model's equal stamp.)
    #[test]
    fn the_differential_replays_the_field_safe_scripted_traces_under_the_field_policy() {
        let dir = trace_dir("scripted-field", "scripted", &[]);
        let ((n, events, _), stopped, _) =
            replay_all_mode(&dir, crate::trace::ReplayMode::FieldExact, &[]);
        assert!(n > 0 && events > 0 && stopped.is_empty());
    }

    /// Random traces of the model under the `field` policy, register layer only: every event before the first document-layer
    /// difference (where the two policies decide differently, expected) is compared exactly; a difference in the register layer
    /// is a disagreement. The stops are listed.
    #[test]
    fn the_differential_replays_random_traces_under_the_field_policy_on_the_register_layer() {
        let runs = env_u64("Q6DIFF_FIELD_RUNS", 36).to_string();
        let dir = trace_dir("random-field", "random", &["--mode", "v6", "--count", &runs]);
        let ((n, events, views), stopped, _) =
            replay_all_mode(&dir, crate::trace::ReplayMode::FieldRegisterLayer, &[]);
        for (f, line, d) in &stopped {
            eprintln!("  stopped: {f} line {line}: {d}");
        }
        eprintln!(
            "field register-layer replay: {n} traces, {events} events, {views} member states, {} stopped",
            stopped.len()
        );
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
        // (default 3 per mutant, N3/L: the default sample includes mutant traces; `Q6DIFF_MUTANT_RUNS=0` turns them off)
        let mutant_runs = env_u64("Q6DIFF_MUTANT_RUNS", 3);
        if mutant_runs > 0 {
            for m in crate::flags::MUTANTS
                .iter()
                .filter(|m| !crate::flags::RUST_ONLY_MUTANTS.contains(m))
            {
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

    /// The model's route probes (`Q6_F` 0.1 and 0.3, the hold and the repair route, with rule B on and off), traced and replayed:
    /// the port of `take_hold` / `promote_oop` / the repair step, the D10 refusal episodes and the state counters are compared event
    /// by event with the model's own, so the Rust route code is tied to the model (`Q6DIFF_ROUTE_RUNS` traces per row).
    #[test]
    fn the_differential_replays_the_route_traces_of_the_model() {
        let runs = env_u64("Q6DIFF_ROUTE_RUNS", 12).to_string();
        let (mut events, mut views, mut holds) = (0, 0, 0);
        for (mode, route, f) in [
            ("v6", "hold", "0.3"),
            ("v6", "hold", "0.1"),
            ("v6", "repair", "0.3"),
            ("v6", "repair", "0.1"),
            ("m-rule-b-off", "hold", "0.3"),
            ("m-rule-b-off", "repair", "0.3"),
        ] {
            let tag = format!("routes-{mode}-{route}-{f}");
            let dir = trace_dir(
                &tag,
                "random",
                &["--mode", mode, "--count", &runs, "--f", f, "--routes", route],
            );
            let ((n, e, v), _, _) = replay_all_mode(&dir, crate::trace::ReplayMode::Document, &[]);
            eprintln!("  {tag}: {n} traces, {e} events, {v} member states");
            events += e;
            views += v;
            holds += n;
        }
        eprintln!("route traces: {holds} traces, {events} events, {views} member states compared");
    }

    /// REPORT6 (b) and (d), 54,000 runs per row (`q6v2-model/REPORT6.md`): (route, rate, rule B on, partial flip, bury without newer
    /// visible, alive mismatch, served mismatch, false records, heads missing, diverged, invariant runs, refusal episodes, member-runs
    /// refusing at the steps' end and at the run's end). The episodes and the end counters are given for rule B on only.
    #[allow(clippy::type_complexity)]
    const REPORT6_ROWS: [(&str, f64, bool, [i64; 8], [i64; 3]); 8] = [
        ("hold", 0.3, true, [0, 0, 23, 0, 19, 0, 704, 0], [62_603, 14_171, 249]),
        ("hold", 0.3, false, [218, 482, 101, 1, 95, 1, 782, 697], [0, 0, 0]),
        ("hold", 0.1, true, [0, 0, 29, 0, 4, 0, 257, 0], [19_587, 3_849, 53]),
        ("hold", 0.1, false, [167, 180, 52, 0, 21, 0, 283, 346], [0, 0, 0]),
        ("repair", 0.3, true, [0, 0, 0, 0, 0, 0, 0, 0], [99_899, 29_635, 233]),
        ("repair", 0.3, false, [438, 104, 187, 0, 151, 0, 243, 540], [0, 0, 0]),
        ("repair", 0.1, true, [0, 0, 0, 0, 0, 0, 0, 0], [46_270, 12_776, 94]),
        ("repair", 0.1, false, [210, 71, 64, 0, 72, 0, 105, 281], [0, 0, 0]),
    ];

    /// The eight route rows (hold / repair x 0.3 / 0.1 x rule B on / off) on the Rust harness against REPORT6's. Asserted: rule B on,
    /// every row: no partial flip, no premature burial, no served mismatch, no head missing, no grant or page above the sender's
    /// coverage, no orphan with a valid binding; the repair rows with rule B on: no problem at all; with rule B off (at 7,200 runs a
    /// row or more): partial flips and premature burials fire. PRINTED, not asserted (the PRNGs differ, so the counts cannot match):
    /// the hold remainder, the refusal episodes and the state counters, each with its rate per 1,000 runs next to REPORT6's and the
    /// ratio; a ratio outside a factor of 3 is reported and not tuned.
    #[test]
    fn the_route_rows_against_report6() {
        let seeds = env_u64("Q6_ROUTE_RUNS", 20);
        let mut violations: Vec<String> = Vec::new();
        for (route, f, rule_b, r6, r6d) in REPORT6_ROWS {
            let (hold, repair) = (route == "hold", route == "repair");
            let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
            for c in cfgs.iter_mut() {
                c.routes = Some(crate::run::Routes {
                    f,
                    hold,
                    repair,
                    doc_hold: false,
                    doc_repair: false,
                });
            }
            let mode = if rule_b { "v6" } else { "m-rule-b-off" };
            let s = run_grid(mode, "v6", &cfgs, 0, seeds, threads());
            let p = s.problems();
            let per_k = |n: i64, runs: i64| 1000.0 * n as f64 / runs as f64;
            let runs = s.runs as i64;
            let line = |name: &str, rust: i64, py: i64| {
                let (a, b) = (per_k(rust, runs), per_k(py, 54_000));
                let ratio = if b > 0.0 { format!("{:.2}", a / b) } else { "-".into() };
                let flag =
                    if b > 0.0 && (a / b > 3.0 || a / b < 1.0 / 3.0) { "  OUTSIDE x3" } else { "" };
                format!(
                    "    {name:30} rust {rust:6} ({a:8.2}/1000)  REPORT6 {py:6} ({b:8.2}/1000)  ratio {ratio}{flag}"
                )
            };
            eprintln!("{route} {f} rule B {}: {} runs", if rule_b { "on " } else { "off" }, s.runs);
            let g = |k: &str| s.get(k);
            for (name, rust, py) in [
                ("partial flip", g("inv_partial_flip"), r6[0]),
                ("bury without newer visible", g("inv_bury_without_newer_visible"), r6[1]),
                ("alive mismatch", g("alive_mismatch"), r6[2]),
                ("served mismatch", g("served_mismatch"), r6[3]),
                ("false records (loss_extra)", g("loss_extra"), r6[4]),
                ("heads missing", g("loss_missing"), r6[5]),
                ("diverged", g("diverged"), r6[6]),
                ("invariant runs", g("invariant"), r6[7]),
            ] {
                eprintln!("{}", line(name, rust, py));
            }
            if rule_b {
                eprintln!("{}", line("refusal episodes", g("cnt_refusal_episodes"), r6d[0]));
                eprintln!(
                    "{}",
                    line("refusing at the steps' end", g("cnt_refusal_open_at_steps_end"), r6d[1])
                );
                eprintln!(
                    "{}",
                    line("refusing at the run's end", g("cnt_refusal_open_at_run_end"), r6d[2])
                );
            }
            eprintln!(
                "    hold_taken {} repair_events {} info_parked {} grant/page above coverage {}/{}; all problems: {:?}",
                g("cnt_hold_taken"),
                g("cnt_repair_events"),
                g("cnt_info_parked"),
                g("cnt_grant_above_coverage"),
                g("cnt_page_above_coverage"),
                p.iter().map(|(k, v)| format!("{k}={}", v.0)).collect::<Vec<_>>()
            );
            // collected, not asserted one by one: every row runs, every violation is kept with its row and the first seed that
            // showed it (`first` is the lowest seed over the row's configurations, where the check counts runs; none for the
            // counters), and the test fails at the end if any was recorded
            let row = format!("{route} {f} rule B {}", if rule_b { "on" } else { "off" });
            let seed_of = |k: &str| match s.first.get(k) {
                Some(seed) => format!(" (lowest failing seed {seed})"),
                None => String::new(),
            };
            let mut viol = |ok: bool, what: String| {
                if !ok {
                    violations.push(format!("{row}: {what}"));
                }
            };
            let hk = if hold { "cnt_hold_taken" } else { "cnt_repair_events" };
            viol(g(hk) > 0, format!("{hk} = {} is not > 0", g(hk)));
            viol(
                g("cnt_refusal_episodes") > 0,
                format!("cnt_refusal_episodes = {} is not > 0", g("cnt_refusal_episodes")),
            );
            for k in ["cnt_grant_above_coverage", "cnt_page_above_coverage"] {
                viol(g(k) == 0, format!("{k} = {} is not 0", g(k)));
            }
            if rule_b {
                for k in [
                    "inv_partial_flip",
                    "inv_bury_without_newer_visible",
                    "served_mismatch",
                    "loss_missing",
                    "inv_orphan_with_valid_binding",
                    "inv_serve_not_alive",
                ] {
                    viol(g(k) == 0, format!("rule B on: {k} = {} is not 0{}", g(k), seed_of(k)));
                }
                if repair {
                    viol(
                        p.is_empty(),
                        format!(
                            "the repair route with rule B on is clean: {p:?}; first seeds {:?}",
                            s.first
                        ),
                    );
                }
            } else if s.runs >= 7_200 {
                viol(
                    g("inv_partial_flip") > 0,
                    "rule B off must fire partial flips (inv_partial_flip = 0)".into(),
                );
                viol(
                    g("inv_bury_without_newer_visible") > 0,
                    "rule B off must fire premature burials (inv_bury_without_newer_visible = 0)"
                        .into(),
                );
            }
        }
        if !violations.is_empty() {
            eprintln!("{} violation(s) over the eight rows:", violations.len());
            for v in &violations {
                eprintln!("  VIOLATION {v}");
            }
        }
        assert!(
            violations.is_empty(),
            "{} violation(s) over the eight rows: {violations:#?}",
            violations.len()
        );
    }

    /// The product's out-of-order routes (design note, phase G addition; no model counterpart): document entries held out of position
    /// (`doc_hold`), a scoped repair of document rows that grants no coverage (`doc_repair`), and both, with the windows running
    /// between; register events always in position. The full invariant and oracle set on BOTH policies, with `K` off; the
    /// assertion that no register event is ever taken out of position (`register_event_out_of_position`, invariant (ii)) is part
    /// of the invariants (the routes keep `w.in_order`). The mutant `m-register-ahead` (`doc_hold` also delivers register events
    /// ahead of their position) trips it.
    ///
    /// THE GRID IS ASSERTED (B1): every run with a problem falls in a named class (`oracle::product_classes`) or the test fails:
    /// - DF-1 and DF-2, open design findings under review, are EXPECTED classes, each with a pinning test in `classes.rs` that
    ///   fails once the class disappears (then the pin becomes a required-zero);
    /// - O-F1 (`field` only: the record-set and head classes the model of record cannot judge) is an OPEN ITEM, printed and
    ///   counted here, never passed silently;
    /// - HB-2 is excluded by the oracle by name and counted (`cnt_hb2_excluded`); HB-1 is fixed.
    #[test]
    fn the_product_routes_keep_register_events_in_position_on_both_policies() {
        let seeds = env_u64("Q6_PRODUCT_RUNS", 20);
        let mut total = 0;
        let mut residual_all: Vec<String> = Vec::new();
        let mut totals: std::collections::BTreeMap<String, i64> = Default::default();
        for policy in [crate::world::Policy::Document, crate::world::Policy::Field] {
            for (doc_hold, doc_repair) in [(true, false), (false, true), (true, true)] {
                for f in [0.3, 0.1] {
                    let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
                    for c in cfgs.iter_mut() {
                        c.policy = policy;
                        c.routes = Some(crate::run::Routes {
                            f,
                            hold: false,
                            repair: false,
                            doc_hold,
                            doc_repair,
                        });
                    }
                    let (s, classes, residual) =
                        crate::run::run_grid_classified("v6", &cfgs, 0, seeds, threads());
                    let p = s.problems();
                    eprintln!(
                        "{policy:?} doc_hold {doc_hold} doc_repair {doc_repair} f {f}: {} runs; doc_hold_taken {} doc_repair {} out-of-position register events {}; HB-2 excluded (runs) {}; O8 shape inside HB-2 (runs) {}; classes (runs) {:?}; UNCLASSIFIED runs {}; problems {:?}",
                        s.runs,
                        s.get("cnt_doc_hold_taken"),
                        s.get("cnt_doc_repair"),
                        s.get("cnt_register_event_out_of_position"),
                        s.get("cnt_hb2_excluded"),
                        s.get("cnt_o8_in_hb2"),
                        classes,
                        residual.len(),
                        p.iter().map(|(k, v)| format!("{k}={}", v.0)).collect::<Vec<_>>()
                    );
                    if doc_hold {
                        assert!(s.get("cnt_doc_hold_taken") > 0);
                    }
                    if doc_repair {
                        assert!(s.get("cnt_doc_repair") > 0);
                    }
                    assert_eq!(s.get("cnt_register_event_out_of_position"), 0);
                    assert_eq!(s.get("cnt_grant_above_coverage"), 0);
                    assert_eq!(s.get("inv_register_event_out_of_position"), 0);
                    // R1: neither route carries a Q6 kind
                    assert_eq!(s.get("cnt_route_carries_q6_kind"), 0);
                    assert_eq!(s.get("inv_route_carries_q6_kind"), 0);
                    *totals.entry("HB-2 excluded".into()).or_insert(0) += s.get("cnt_hb2_excluded");
                    *totals.entry("O8 shape inside HB-2".into()).or_insert(0) +=
                        s.get("cnt_o8_in_hb2");
                    for (k, v) in classes {
                        *totals.entry(k).or_insert(0) += v;
                    }
                    residual_all.extend(residual);
                    total += s.runs;
                }
            }
        }
        eprintln!("product routes: {total} runs; named classes (runs): {totals:?}");
        if totals.get("O-F1").copied().unwrap_or(0) > 0 {
            eprintln!(
                "OPEN ITEM O-F1 (field only, unclassified: the model of record has no field policy): {} runs, listed in the class counts above",
                totals["O-F1"]
            );
        }
        for r in &residual_all {
            eprintln!("  UNCLASSIFIED {r}");
        }
        assert!(
            residual_all.is_empty(),
            "{} product-route run(s) with a problem no named class explains: {residual_all:#?}",
            residual_all.len()
        );
    }

    /// `m-register-ahead` trips the assertion on the product routes (and so has teeth).
    #[test]
    fn m_register_ahead_trips_the_register_in_position_assertion() {
        let seeds = env_u64("Q6_PRODUCT_RUNS", 20);
        for policy in [crate::world::Policy::Document, crate::world::Policy::Field] {
            let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
            for c in cfgs.iter_mut() {
                c.policy = policy;
                c.routes = Some(crate::run::Routes {
                    f: 0.3,
                    hold: false,
                    repair: false,
                    doc_hold: true,
                    doc_repair: false,
                });
            }
            let s = run_grid("m-register-ahead", "v6", &cfgs, 0, seeds, threads());
            eprintln!(
                "m-register-ahead {policy:?}: {} runs, out-of-position register events {}, invariant runs {}",
                s.runs,
                s.get("cnt_register_event_out_of_position"),
                s.get("inv_register_event_out_of_position")
            );
            assert!(s.get("cnt_register_event_out_of_position") > 0);
            assert!(
                s.get("inv_register_event_out_of_position") > 0,
                "{policy:?}: the assertion must trip"
            );
        }
    }

    /// R1 (Q6 v2.5 FIX-2): no excluded route carries a Q6 kind. Each route's assertion has a mutant that trips it: the hold relog
    /// (`m-doc-hold-q6-kinds`: creates, abandons and loss entries out of position), the scoped repair (`m-repair-carries-creates`,
    /// `m-repair-carries-events`, and `m-repair-dropped-answer`: the sender's drops).
    #[test]
    fn the_r1_route_mutants_trip_the_route_assertion() {
        let seeds = env_u64("Q6_PRODUCT_RUNS", 20);
        for (m, doc_hold, doc_repair) in [
            ("m-doc-hold-q6-kinds", true, false),
            ("m-repair-carries-creates", false, true),
            ("m-repair-carries-events", false, true),
            ("m-repair-dropped-answer", false, true),
        ] {
            for policy in [crate::world::Policy::Document, crate::world::Policy::Field] {
                let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
                for c in cfgs.iter_mut() {
                    c.policy = policy;
                    c.routes = Some(crate::run::Routes {
                        f: 0.3,
                        hold: false,
                        repair: false,
                        doc_hold,
                        doc_repair,
                    });
                }
                let s = run_grid(m, "v6", &cfgs, 0, seeds, threads());
                // M2: the route assertion is judged by its consequence (the receiver's Q6 surface changed), not at the injection
                // site; the oracle-level failures it leads to are counted beside it
                let oracle_level: i64 = [
                    "diverged",
                    "oracle_state",
                    "loss_mismatch",
                    "inv_partial_flip",
                    "inv_q6_kind_out_of_position",
                ]
                .iter()
                .map(|k| s.get(k))
                .sum();
                eprintln!(
                    "{m} {policy:?}: {} runs, a Q6 kind picked {} times, the Q6 surface changed {} times in {} runs; oracle-level \
                     (diverged, oracle_state, loss_mismatch, partial flip, invariant (ii)) {:?}",
                    s.runs,
                    s.get("cnt_route_picked_q6_kind"),
                    s.get("cnt_route_carries_q6_kind"),
                    s.get("inv_route_carries_q6_kind"),
                    [
                        "diverged",
                        "oracle_state",
                        "loss_mismatch",
                        "inv_partial_flip",
                        "inv_q6_kind_out_of_position"
                    ]
                    .map(|k| (k, s.get(k)))
                );
                assert!(
                    s.get("inv_route_carries_q6_kind") > 0,
                    "{m} {policy:?}: the route assertion must trip"
                );
                assert!(
                    oracle_level > 0,
                    "{m} {policy:?}: the mutant must have an oracle-level consequence"
                );
            }
        }
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
        let record = "4343f5208763266f091c7f607335820bff22baea94110cf2d9c8f5fb727a07e2";
        let bytes = std::fs::read(dir.join("swapmodel6b.py")).unwrap();
        let got6b: String = Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(got6b, record, "swapmodel6b.py");
        let tracer = "98bbaaea4e963543b2278a83afeefe9ad9410eb439e852958b1106ef507d6476";
        let bytes = std::fs::read(dir.join("q6diff_gen.py")).unwrap();
        let got: String = Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(got, tracer, "q6diff_gen.py");
        let m6d = "409eb9d10f35682c3857b4e9e8adad32caa343576425a8a192f02c799a7db6fe";
        let bytes = std::fs::read(dir.join("swapmodel6d.py")).unwrap();
        let got6d: String = Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(got6d, m6d, "swapmodel6d.py");
        let recorded = std::fs::read_to_string(dir.join("MODEL-HASHES.txt")).unwrap();
        assert!(
            recorded.contains(want)
                && recorded.contains(tracer)
                && recorded.contains(record)
                && recorded.contains(m6d)
        );
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
            c.k = std::env::var("Q6_K").ok().and_then(|k| k.parse().ok());
            if let Ok(spec) = std::env::var("Q6_ROUTE") {
                // "<hold|repair|doc_hold|doc_repair|...,>:<f>", e.g. doc_hold:0.3
                let (kinds, f) = spec.split_once(':').expect("kinds:f");
                c.routes = Some(crate::run::Routes {
                    f: f.parse().unwrap(),
                    hold: kinds.split(',').any(|k| k == "hold"),
                    repair: kinds.split(',').any(|k| k == "repair"),
                    doc_hold: kinds.split(',').any(|k| k == "doc_hold"),
                    doc_repair: kinds.split(',').any(|k| k == "doc_repair"),
                });
            }
            let r = run(&std::env::var("Q6_MODE").unwrap_or("v6".into()), "v6", seed, c, true);
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
                if let Ok(spec) = std::env::var("Q6_WATCH") {
                    // "<origin index>:<hlc>,<hlc>": who holds / appended / holds out of position those entries at the end
                    let (o, hs) = spec.split_once(':').unwrap();
                    let o: usize = o.parse().unwrap();
                    for h in hs.split(',').map(|x| x.parse::<i64>().unwrap()) {
                        if let Some(e) = r.w.all.iter().find(|e| e.origin == o && e.hlc == h) {
                            for mm in &r.ms {
                                eprintln!(
                                    "WATCH {} ({h}) at {}: has {} appended {} oop {} covers_origin_to_it {}",
                                    e,
                                    mm.name,
                                    mm.holds(e.idx),
                                    mm.appended.contains(&e.idx),
                                    mm.held_oop.contains(&e.idx),
                                    mm.covers(&r.w, o, h)
                                );
                            }
                        }
                    }
                }
                if c.k.is_some() {
                    for mm in &r.ms {
                        let mut ks: Vec<_> = mm
                            .loss
                            .keys()
                            .map(|k| (k.key, k.path, k.stamp.map(|s| s.hlc.wall_ms)))
                            .collect();
                        ks.sort();
                        let mut dr: Vec<_> = mm
                            .dropped_loss
                            .iter()
                            .map(|k| (k.key, k.path, k.stamp.map(|s| s.hlc.wall_ms)))
                            .collect();
                        dr.sort();
                        eprintln!(
                            "RECORDS {} vanished={} kept {ks:?} dropped {dr:?}",
                            mm.name, mm.vanished
                        );
                    }
                }
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
    /// `rule_b_a_held_bind_is_information_until_covered` kills it), `m-loss-out-of-position` (a record is ahead of its position
    /// only on a route; `seed_1434_shape...` kills it), `m-dead-page-dropped` (0 in 28,800 in-order runs in
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
                "m-loss-out-of-position",
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
                // killed by the scripted rule-B scenario (`rule_b_a_held_bind_is_information_until_covered`), not by the random grid
                "m-d10-ignores-info",
                // killed on the product-route seeds 25, 138, 154 (`classes.rs`), not by the base grid (no routes)
                "m-release-behind-held",
                // killed by the scripted snapshot-between-release-and-effect scenario (`scenarios.rs`, item R)
                "m-release-without-effect",
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
