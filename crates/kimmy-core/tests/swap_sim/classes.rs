//! The B1 classification of the product-route grid (`q7-logs/q6h-b1/`, the manager's rulings), as named classes with their own
//! tests: HB-1 (a held entry servable before its release) is a harness bug, fixed; the rest are pinned here.

#[cfg(test)]
mod tests {
    use crate::coverage::CoverageOf;
    use crate::run::run_product;
    use crate::world::Policy;

    /// (class, policy, members/outs/keys, doc_hold, doc_repair, f, seed): one run of the B1 classification.
    type B1Seed = (&'static str, Policy, (usize, usize, usize), bool, bool, f64, u64);

    /// The B1 classification's seeds, by class.
    const B1_SEEDS: &[B1Seed] = &[
        ("HB-1", Policy::Document, (3, 1, 3), true, false, 0.3, 860),
        ("HB-1", Policy::Document, (3, 3, 5), true, false, 0.3, 277),
        ("HB-1", Policy::Document, (3, 2, 5), true, false, 0.3, 344),
        ("HB-1", Policy::Document, (3, 3, 4), true, false, 0.3, 360),
        ("DF-1", Policy::Document, (3, 1, 3), true, false, 0.1, 295),
        ("DF-1", Policy::Document, (3, 1, 3), true, false, 0.1, 1043),
        ("DF-1", Policy::Document, (3, 2, 5), true, false, 0.3, 468),
        ("DF-1", Policy::Document, (4, 3, 3), true, false, 0.1, 89),
        ("DF-1", Policy::Document, (4, 3, 4), false, true, 0.3, 1068),
        ("DF-1", Policy::Document, (4, 3, 5), false, true, 0.1, 44),
        ("DF-1", Policy::Document, (5, 2, 3), false, true, 0.3, 746),
        ("DF-2", Policy::Document, (2, 1, 3), false, true, 0.3, 391),
        ("DF-2", Policy::Document, (2, 1, 5), false, true, 0.3, 35),
        ("DF-2", Policy::Document, (2, 1, 3), false, true, 0.1, 1182),
        ("DF-2", Policy::Document, (4, 1, 4), false, true, 0.1, 426),
        ("DF-2", Policy::Document, (3, 1, 4), false, true, 0.3, 179),
        ("DF-2", Policy::Document, (5, 1, 3), true, false, 0.3, 135),
        ("DF-2", Policy::Document, (3, 2, 5), true, false, 0.1, 1),
        ("HB-2", Policy::Document, (2, 1, 3), true, false, 0.1, 201),
        ("HB-2", Policy::Document, (2, 1, 3), true, false, 0.1, 559),
        ("HB-2", Policy::Document, (2, 1, 3), true, false, 0.3, 386),
        ("HB-2", Policy::Document, (2, 1, 3), false, true, 0.3, 166),
        ("HB-2", Policy::Document, (2, 1, 3), false, true, 0.1, 166),
        ("HB-2", Policy::Document, (2, 1, 3), false, true, 0.3, 188),
        ("HB-2r", Policy::Document, (2, 3, 3), true, false, 0.3, 101),
        ("HB-2r", Policy::Document, (2, 2, 3), true, false, 0.1, 730),
        ("HB-2r", Policy::Document, (2, 3, 3), true, false, 0.1, 730),
        ("DF-2r", Policy::Field, (4, 2, 3), true, false, 0.3, 336),
        ("O-F1", Policy::Field, (4, 2, 5), true, false, 0.3, 1361),
        ("O-F1", Policy::Field, (3, 2, 3), false, true, 0.1, 1365),
        ("O-F1", Policy::Field, (4, 3, 5), false, true, 0.1, 1046),
    ];

    fn nonzero_problems(r: &crate::run::RunOut) -> Vec<String> {
        let mut s = crate::run::Summary::default();
        s.add(0, &r.res);
        s.problems().keys().cloned().collect()
    }

    /// HB-1's seeds (B1 classification, the manager's ruling: a harness bug): `doc_hold` takes a document entry whose physical the
    /// receiver does not know yet; `take_hold` removed it from `appended` at hold time, then the deferred apply appended the
    /// winner: held and servable at once, so a downstream member witnessed past it and flipped without the staged row.
    const HB1_SEEDS: [((usize, usize, usize), u64); 4] =
        [((3, 1, 3), 860), ((3, 3, 5), 277), ((3, 2, 5), 344), ((3, 3, 4), 360)];

    /// HB-1 is fixed: a held entry is not servable until it is released in position (product invariant (i), the step check
    /// `held_entry_servable`). The mutant `m-held-servable-on-deferred-apply` re-introduces the bug; on each seed the step check
    /// AND its consequence (the partial flip downstream) fire.
    #[test]
    fn hb1_a_held_entry_is_not_servable_until_released_in_position() {
        for (mok, seed) in HB1_SEEDS {
            let r = run_product("v6", Policy::Document, mok, true, false, 0.3, seed, false);
            let p = nonzero_problems(&r);
            assert!(p.is_empty(), "HB-1 seed {seed} {mok:?}: {p:?}");
            assert!(
                r.res["cnt_held_kept_unservable"] > 0,
                "seed {seed}: the deferred apply of a held entry is reached"
            );
            let m = run_product(
                "m-held-servable-on-deferred-apply",
                Policy::Document,
                mok,
                true,
                false,
                0.3,
                seed,
                false,
            );
            let p = nonzero_problems(&m);
            eprintln!("m-held-servable-on-deferred-apply seed {seed} {mok:?}: {p:?}");
            for k in ["inv_held_entry_servable", "inv_partial_flip"] {
                assert!(
                    p.iter().any(|x| x == k),
                    "the mutant at seed {seed}: {k} must fire: {p:?}"
                );
            }
        }
    }

    /// HB-2 (B1 classification, the manager's ruling: a harness class): a divergence confined to a vanished writer's never validly
    /// bound stage that no client can see. The oracle excludes it by the structural set `oracle::hb2_stages` (from the write log
    /// and the members' states) and counts it (`cnt_hb2_excluded`). On each of HB-2's seeds the run is clean only BECAUSE of the
    /// exclusion: with it off the divergence is back, and the set is not empty.
    #[test]
    fn hb2_a_divergence_confined_to_a_vanished_writers_unbound_unseen_stage_is_excluded_by_name_and_counted()
     {
        for &(cls, pol, mok, dh, dr, f, seed) in B1_SEEDS.iter().filter(|s| s.0 == "HB-2") {
            let r = run_product("v6", pol, mok, dh, dr, f, seed, false);
            let p = nonzero_problems(&r);
            assert!(p.is_empty(), "{cls} seed {seed} {mok:?} dh {dh} dr {dr} f {f}: {p:?}");
            assert_eq!(r.res["cnt_hb2_excluded"], 1, "{cls} seed {seed}: the exclusion is counted");
            assert!(r.res["cnt_hb2_stages"] > 0);
            let mut c = crate::run::product_cfg(pol, mok, dh, dr, f);
            c.hb2_exclusion = false;
            let off = crate::run::run("v6", "v6", seed, c, false);
            let p = nonzero_problems(&off);
            eprintln!("HB-2 seed {seed} {mok:?} dh {dh} dr {dr} f {f}, exclusion off: {p:?}");
            assert!(
                p.iter().any(|k| k == "diverged"),
                "seed {seed}: without the exclusion the run diverges: {p:?}"
            );
            assert_eq!(off.res["cnt_hb2_excluded"], 0);
        }
    }

    /// HB-2's two-replay sub-case (seeds 101, 730; signature `div_replay` without `div_replay_interleaved`): a live member held an
    /// entry no live member appended; the stamp-order replay did not witness it (`take_quiesce_replica`), the interleaved replay
    /// did. DECIDED by the design text: the stamp-order replay is right (FIX-1: a held entry raises no vector; the FIX-5 ruling:
    /// no member downstream can witness a held entry away). The interleaved replay now skips it too, and the member's own D9
    /// safety net no longer takes a held entry as its witness above a deadline (that was why the member buried a stage the
    /// replica kept). Asserted: the two replays agree on each seed (before the HB-2 exclusion), the run is clean, and the mutant
    /// `m-held-entry-witnesses-the-deadline` trips the step check `orphan_burial_without_covered_witness`.
    #[test]
    fn hb2_the_two_replays_agree_and_a_held_entry_is_no_deadline_witness() {
        for &(cls, pol, mok, dh, dr, f, seed) in B1_SEEDS.iter().filter(|s| s.0 == "HB-2r") {
            let r = run_product("v6", pol, mok, dh, dr, f, seed, false);
            let p = nonzero_problems(&r);
            assert!(p.is_empty(), "{cls} seed {seed} {mok:?}: {p:?}");
            assert_eq!(
                r.res["raw_div_replay"], r.res["raw_div_replay_interleaved"],
                "{cls} seed {seed} {mok:?}: the two replays agree"
            );
            let m = run_product(
                "m-held-entry-witnesses-the-deadline",
                pol,
                mok,
                dh,
                dr,
                f,
                seed,
                false,
            );
            let p = nonzero_problems(&m);
            eprintln!("m-held-entry-witnesses-the-deadline seed {seed} {mok:?}: {p:?}");
            assert!(
                p.iter().any(|k| k == "inv_orphan_burial_without_covered_witness"),
                "seed {seed}: {p:?}"
            );
        }
    }

    /// The named classes of one run (`oracle::product_classes`).
    fn classes_of(r: &crate::run::RunOut, pol: Policy) -> (Vec<&'static str>, Vec<String>) {
        crate::oracle::product_classes(&r.res, pol == Policy::Field)
    }

    /// DF-1 is REQUIRED-ZERO with rule S (v2.6, the staged-row rule, `swapmodel6d.py:833-838`): on each seed that pinned the open finding
    /// (a partial flip on the product-shaped document routes: a member treated as a loser an entry superseded only by a version it
    /// does not cover) the run is now clean, no run is classified DF-1 (the class is gone from `oracle::product_classes`, so the grid
    /// fails on a partial flip), and the mutant `m-staged-row-off` brings the partial flip back on every one of them (it kills the
    /// rule). The DF-1 counter `cnt_df1_loser_superseded_by_uncovered` still counts the structural marker.
    #[test]
    fn df1_is_required_zero_with_rule_s_and_the_partial_flip_returns_without_it() {
        for &(cls, pol, mok, dh, dr, f, seed) in B1_SEEDS.iter().filter(|s| s.0 == "DF-1") {
            let r = run_product("v6", pol, mok, dh, dr, f, seed, false);
            let (classes, residual) = classes_of(&r, pol);
            eprintln!(
                "{cls} seed {seed} {mok:?} dh {dh} dr {dr} f {f}: classes {classes:?} residual {residual:?} problems {:?}",
                nonzero_problems(&r)
            );
            assert!(
                !nonzero_problems(&r).iter().any(|k| k == "inv_partial_flip"),
                "DF-1 is back at seed {seed} {mok:?} dh {dh} dr {dr} f {f}"
            );
            assert!(residual.is_empty(), "seed {seed}: unexplained {residual:?}");
            let m = run_product("m-staged-row-off", pol, mok, dh, dr, f, seed, false);
            assert!(
                nonzero_problems(&m).iter().any(|k| k == "inv_partial_flip"),
                "the rule is not needed at seed {seed} {mok:?} dh {dh} dr {dr} f {f}: m-staged-row-off stays clean"
            );
        }
    }

    /// S1's NEGATIVE case: rule S is about the staging write of the stage's own job. An ordinary write arriving in position at a physical
    /// created as a stage (e.g. a post-flip write of another origin), superseded on arrival by a version held here, is NOT appended by
    /// the rule. Non-vacuous (the cases occur on the product routes) and never violated; the mutant is the rule applied to every entry
    /// addressed to a stage physical, which the check names (`inv_rule_s_appended_a_non_staging_write`) is not reachable from the model's
    /// own trigger, so the check is a structural assertion of the trigger
    #[test]
    fn rule_s_does_not_append_an_ordinary_write_to_a_stage_physical() {
        for policy in [Policy::Document, Policy::Field] {
            let (mut cases, mut bad) = (0i64, 0i64);
            for (dh, dr) in [(true, false), (false, true), (true, true)] {
                for m in [3usize, 4] {
                    for seed in 0..60u64 {
                        let r = run_product("v6", policy, (m, 2, 4), dh, dr, 0.3, seed, false);
                        cases += r.res.get("cnt_rule_s_negative_cases").copied().unwrap_or(0);
                        bad += r.res.get("cnt_rule_s_negative_appended").copied().unwrap_or(0);
                    }
                }
            }
            assert!(
                cases > 0,
                "{policy:?}: vacuous: no ordinary write superseded on arrival at a stage physical"
            );
            assert_eq!(
                bad, 0,
                "{policy:?}: rule S appended an ordinary write at a stage physical in {bad} runs"
            );
        }
    }

    /// Rule S on the `field` policy is an UNMODELLED extension (the Python model is `document`-only, open item O6): its only evidence is
    /// the field product-route grid at gate size: no partial flip with the rule, partial flips without it (`m-staged-row-off`).
    /// `Q6_FIELDS_RUNS` (default 250) seeds x 36 configurations; doc_hold and both routes at f 0.3.
    #[test]
    #[ignore = "gate size: 250 seeds x 36 configurations, field policy"]
    fn rule_s_on_the_field_policy_no_partial_flip_with_it_and_flips_without_it() {
        let seeds = crate::env_u64("Q6_FIELDS_RUNS", 250);
        for (dh, dr) in [(true, false), (true, true)] {
            let mut cfgs = crate::run::grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
            for c in cfgs.iter_mut() {
                *c = {
                    let mut n = crate::run::product_cfg(
                        Policy::Field,
                        (c.members, c.outs, c.keys),
                        dh,
                        dr,
                        0.3,
                    );
                    n.policy = Policy::Field;
                    n
                };
            }
            let on = crate::run::run_grid("v6", "v6", &cfgs, 0, seeds, 4);
            let off = crate::run::run_grid("m-staged-row-off", "v6", &cfgs, 0, seeds, 4);
            eprintln!(
                "FIELD GRID dh {dh} dr {dr}: {} runs: inv_partial_flip with rule S {} / without {}; rule S appended {} rows",
                seeds * 36,
                on.get("inv_partial_flip"),
                off.get("inv_partial_flip"),
                on.get("cnt_staged_row_appended")
            );
            assert_eq!(
                on.get("inv_partial_flip"),
                0,
                "a partial flip with rule S on the field policy (dh {dh} dr {dr})"
            );
            // O-F1 is a class on the field policy that explains any loss-record or head failure (the model of record has no field
            // policy): an upper bound on the runs it explains (measured 7 and 4 of 9,000 runs at gate size; the bound is 20), so that a loss-record
            // regression raises a failure, not a printed number
            let (_, classes, residual) = crate::run::run_grid_classified("v6", &cfgs, 0, seeds, 4);
            let of1 = classes.get("O-F1").copied().unwrap_or(0);
            eprintln!(
                "FIELD GRID dh {dh} dr {dr}: classes {classes:?}; unclassified {residual:?}; O-F1 {of1} of {} runs",
                seeds * 36
            );
            assert!(residual.is_empty(), "unclassified on the field policy: {residual:?}");
            assert!(
                of1 <= crate::env_u64("Q6_OF1_BOUND", 20) as i64,
                "O-F1 explains {of1} runs of {}: above its bound",
                seeds * 36
            );
            assert!(
                off.get("inv_partial_flip") > 0,
                "the rule is not needed on the field policy (dh {dh} dr {dr})"
            );
        }
    }

    /// R4's GRANT GATE (a snapshot's grant covers only what the sender's vector names; `m-snap-register-late`: pages read the register above
    /// the grant) is killed by SEEDS, not by a grid margin: with rule B off alone these runs are clean, with the gate off as well each has
    /// the named problem (found by `scan_for_a_killing_seed`: `Q6SCAN_MODE=m-snap-register-late+m-rule-b-off Q6SCAN_BASE=m-rule-b-off`).
    #[test]
    fn r4_the_grant_gate_is_killed_on_its_seeds() {
        // (members, outs, keys), doc_hold, doc_repair, f, seed, the problem the mutant adds
        type Pin = ((usize, usize, usize), bool, bool, f64, u64, &'static str);
        const SEEDS: [Pin; 3] = [
            ((3, 3, 4), true, true, 0.3, 222, "inv_partial_flip"),
            ((2, 3, 4), true, true, 0.3, 85, "diverged"),
            ((5, 2, 3), true, true, 0.3, 40, "diverged"),
        ];
        for (mok, dh, dr, f, seed, key) in SEEDS {
            let base = run_product("m-rule-b-off", Policy::Document, mok, dh, dr, f, seed, false);
            let mutant = run_product(
                "m-snap-register-late+m-rule-b-off",
                Policy::Document,
                mok,
                dh,
                dr,
                f,
                seed,
                false,
            );
            assert_eq!(
                base.res.get(key).copied().unwrap_or(0),
                0,
                "{mok:?} seed {seed}: rule B off alone is not clean on {key}"
            );
            assert!(
                mutant.res.get(key).copied().unwrap_or(0) != 0,
                "{mok:?} seed {seed}: the grant-gate mutant no longer shows {key}"
            );
        }
    }

    /// M4 (the Q6 design page, 'Records': a loss record is an ordinary replicated entry inside its appender's coverage prefix, appended at the
    /// end of its own sequence, and `appended and served onward`): the CONSEQUENCE is that the record reaches a peer that pulls only by
    /// window. A mints a record; C pulls from A by windows only (no snapshot, no third path); C must hold the record. Returns whether it does.
    fn a_record_reaches_a_member_that_pulls_only_by_window(mode: &str) -> bool {
        use crate::member::DocOp;
        let mut sc = crate::scenario::Sc::new(mode, &[("A", 0), ("B", 0), ("C", 0)], 60);
        sc.advance(1);
        let seed = sc.ms[0].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.window("B", "A", &seed);
        sc.window("C", "A", &seed);
        sc.advance(5);
        let before = sc.w.all.len();
        sc.ms[0].report_path(&mut sc.w, 0, None, seed.stamp, seed.body.clone(), "L");
        assert!(sc.w.all.len() > before, "A minted no record");
        let rec = sc.w.all.len() - 1;
        sc.pull("C", "A");
        sc.ms[2].cv.has(rec)
    }

    /// D9's witness path of `m-loss-outside-prefix` (review: `by_origin` also feeds `covers_past`): A creates a stage and never binds it; after the stage's
    /// deadline A mints a loss record, which is the ONLY entry of A's sequence above the deadline; B pulls from A by windows. In the base the record is the
    /// witness that B covers A past the deadline, so B's coverage safety net buries the stage; without the record in the sequence there is no witness.
    /// Returns whether B buried the stage.
    fn a_record_is_the_d9_witness_that_buries_an_unbound_stage(mode: &str) -> bool {
        use crate::member::DocOp;
        let mut sc = crate::scenario::Sc::new(mode, &[("A", 0), ("B", 0)], 20);
        sc.advance(1);
        let seed = sc.ms[0].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.window("B", "A", &seed);
        sc.advance(2);
        let job = sc.ms[0].start_out(&mut sc.w, vec![0], crate::entry::JobKind::Late, 0);
        let stage = job.stage.clone();
        sc.pull("B", "A");
        assert!(sc.ms[1].phys.contains_key(&stage), "B does not know the stage");
        sc.advance(60); // far past the deadline; the job never binds
        sc.ms[0].report_path(&mut sc.w, 0, None, seed.stamp, seed.body.clone(), "L");
        sc.pull("B", "A");
        sc.ms[1].settle(&mut sc.w);
        sc.ms[1].phys.get(&stage).is_none_or(|p| !p.alive)
    }

    /// On the WINDOW PATH ONLY, `m-loss-outside-prefix` is EQUIVALENT by consequence (the mutant is killed on the D9 witness path, above; the model fails nothing on it): the record's
    /// position in its origin's sequence (`World::by_origin`, which only the coverage prefix reads) is not observable, because a window
    /// serves the entries a member APPENDED (`Member::appended`, `next_window_entry`), not the origin's sequence, so the record reaches a
    /// member that pulls only by window either way. Pinned here: if this test ever fails, the sequence position became observable (a
    /// window ordered by the sequence, a vector check over it) and the mutant needs a kill. The design rule ('inside its appender's
    /// coverage prefix', Q6 design page, 'Records') is not tested by a flag check.
    /// `m-loss-outside-prefix` is KILLED on the D9 witness path (review: the origin's sequence, `World::by_origin`, also feeds coverage): the record
    /// is the only entry of its origin above the stage's deadline, so without it in the sequence the coverage safety net has no witness and the
    /// orphan stage the base buries lingers. The CONSEQUENCE is an alive orphan stage; the mutant is equivalent only on the window path (next test).
    #[test]
    fn m_loss_outside_prefix_is_killed_on_the_d9_witness_path() {
        assert!(
            a_record_is_the_d9_witness_that_buries_an_unbound_stage("v6"),
            "v6: the record is not the D9 witness (the stage was not buried)"
        );
        assert!(
            !a_record_is_the_d9_witness_that_buries_an_unbound_stage("m-loss-outside-prefix"),
            "m-loss-outside-prefix no longer changes D9's witness: it is equivalent on this path too"
        );
    }

    #[test]
    fn m_loss_outside_prefix_is_equivalent_by_consequence_on_the_window_path() {
        let base = a_record_reaches_a_member_that_pulls_only_by_window("v6");
        let mutant = a_record_reaches_a_member_that_pulls_only_by_window("m-loss-outside-prefix");
        assert!(base, "v6: the record does not reach a member that pulls only by window");
        assert!(
            mutant,
            "m-loss-outside-prefix changed what a window carries: it is no longer equivalent, write its kill"
        );
    }

    /// `m-snap-no-orphan-burials` (a snapshot does not carry the sender's orphan burials) is killed on the base (in-order) grid by seeds
    /// (found by `scan_for_a_killing_seed`, `Q6SCAN_INORDER=1`): v6 is clean on each, the mutant diverges with an oracle-state and an
    /// alive mismatch (a live member keeps an orphan stage the oracle buries).
    #[test]
    fn m_snap_no_orphan_burials_is_killed_on_its_seeds() {
        for (mok, seed) in [((3usize, 3usize, 3usize), 54u64), ((3, 3, 3), 246), ((4, 1, 3), 184)] {
            let cfg = crate::run::Cfg::new(mok.0, mok.1, mok.2);
            let base = crate::run::run("v6", "v6", seed, cfg, false);
            let mutant = crate::run::run("m-snap-no-orphan-burials", "v6", seed, cfg, false);
            for key in ["diverged", "oracle_state", "alive_mismatch"] {
                assert_eq!(
                    base.res.get(key).copied().unwrap_or(0),
                    0,
                    "{mok:?} seed {seed}: v6 is not clean on {key}"
                );
                assert!(
                    mutant.res.get(key).copied().unwrap_or(0) != 0,
                    "{mok:?} seed {seed}: m-snap-no-orphan-burials no longer shows {key}"
                );
            }
        }
    }

    /// The structural lost set as an OPTION of the oracle (`Cfg::lost_struct`, the model's `Q6_LOSTSTRUCT=1`; the default stays 6d's, the one item P
    /// compares against). Natively (the harness's own draws, so the model's four seeds are NOT the same runs): every run of the in-order grid is clean
    /// with the option on, and it never ADDS a required head (the structural set holds fewer entries than 6d's, so it can only remove requirements). The
    /// comparison against the MODEL's moved runs is done by the trace replay (the tracer writes the header field `lost_struct`): the LOSTSTRUCT replays at b872f81d, 10/10 (logs coder-s21-*.log).
    #[test]
    fn the_structural_lost_set_option_is_clean_and_never_adds_a_required_head() {
        let (mut runs, mut fewer) = (0, 0);
        for m in [3usize, 4] {
            for o in [2usize, 3] {
                for k in [3usize, 4, 5] {
                    for seed in 0..60u64 {
                        let off =
                            crate::run::run("v6", "v6", seed, crate::run::Cfg::new(m, o, k), false);
                        let mut c = crate::run::Cfg::new(m, o, k);
                        c.lost_struct = true;
                        let on = crate::run::run("v6", "v6", seed, c, false);
                        assert!(
                            nonzero_problems(&on).is_empty(),
                            "{m}/{o}/{k} seed {seed}: not clean with the structural set"
                        );
                        assert!(
                            on.res["heads_required"] <= off.res["heads_required"],
                            "{m}/{o}/{k} seed {seed}: the option ADDED a head"
                        );
                        runs += 1;
                        fewer += (on.res["heads_required"] < off.res["heads_required"]) as i32;
                    }
                }
            }
        }
        eprintln!("structural lost set: {runs} runs, {fewer} with fewer required heads");
    }

    /// HB-2's "has served" is an EVER-served set (review re-check, item 2): a stage some live member served earlier and no longer serves is not
    /// "unseen" and is not in HB-2. The shape needs a binding the oracle calls invalid but a member applied (`m-orphan-no-deadline`): the writer
    /// vanishes, X serves the stage, then stops serving it. Without the insert in `enter` the stage would be in HB-2 (the control at the end).
    #[test]
    fn hb2_uses_an_ever_served_set_not_the_last_served_physical() {
        use crate::member::DocOp;
        let mut sc = crate::scenario::Sc::new("m-orphan-no-deadline", &[("W", 0), ("X", 0)], 20);
        sc.advance(1);
        sc.ms[0].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.advance(2);
        let mut job = sc.ms[0].start_out(&mut sc.w, vec![0], crate::entry::JobKind::Late, 0);
        let stage = job.stage.clone();
        sc.advance(60); // past the deadline: the bind is not valid for the oracle
        sc.ms[0].do_bind(&mut sc.w, &mut job, 2);
        sc.pull("X", "W");
        let bind =
            sc.w.all
                .iter()
                .find(|e| e.kind == crate::entry::Kind::Bind && e.phys == stage)
                .cloned()
                .expect("the late bind was minted");
        assert!(
            !sc.w.valid(&bind),
            "the bind must be invalid for the oracle (never validly bound)"
        );
        // X serves the stage by the bind (the model's lenient member: `m-orphan-no-deadline`); entered directly so the shape does not depend on the relay
        sc.ms[1].enter(&mut sc.w, &bind);
        assert!(
            sc.ms[1].ever_served.contains(&stage),
            "enter() did not record the physical as ever served"
        );
        sc.ms[0].vanished = true;
        sc.w.held_uncovered = true;
        sc.ms[1].cur = None; // X no longer serves it (nor does anyone)
        assert_ne!(sc.ms[1].serving().as_deref(), Some(stage.as_str()));
        let hb2 = crate::oracle::hb2_stages(&sc.w, &sc.ms);
        assert!(!hb2.contains(&stage), "a stage a live member served earlier is in HB-2");
        // control: with the history forgotten the stage IS in HB-2 (the test is sensitive to the set)
        sc.ms[1].ever_served.remove(&stage);
        sc.ms[1].last_served = "L".to_string();
        assert!(
            crate::oracle::hb2_stages(&sc.w, &sc.ms).contains(&stage),
            "control: the stage should be in HB-2"
        );
    }

    /// PIN of DF-2 (open design finding, UNDER REVIEW: convergence, state held above coverage stranded behind a permanently lost
    /// entry on a served collection). As DF-1's pin: the class must occur on each listed seed and the pin fails loudly once it
    /// disappears.
    #[test]
    fn df2_pin_the_stranded_held_state_class_occurs_on_its_seeds() {
        for &(cls, pol, mok, dh, dr, f, seed) in B1_SEEDS.iter().filter(|s| s.0 == "DF-2") {
            let r = run_product("v6", pol, mok, dh, dr, f, seed, false);
            let (classes, residual) = classes_of(&r, pol);
            eprintln!(
                "{cls} seed {seed} {mok:?} dh {dh} dr {dr} f {f}: classes {classes:?} residual {residual:?}"
            );
            assert!(
                classes.contains(&"DF-2"),
                "DF-2 DISAPPEARED at seed {seed} {mok:?} dh {dh} dr {dr} f {f}: convert this pin to a required-zero and drop the \
                 class from oracle::product_classes ({:?})",
                nonzero_problems(&r)
            );
            assert!(residual.is_empty(), "seed {seed}: unexplained {residual:?}");
        }
    }

    /// DF-2r (the DF-2 mechanism applied to a loss record, Field 336): a late write is held out of position at live members behind
    /// an entry lost with its writer, and its loss records died with their minters, so the head has no record. A NAMED, counted
    /// class (`cnt_df2r`), no longer an open item: the pin fails loudly if it disappears.
    #[test]
    fn df2r_pin_the_held_late_write_without_a_record_class_occurs_on_field_336() {
        for &(cls, pol, mok, dh, dr, f, seed) in B1_SEEDS.iter().filter(|s| s.0 == "DF-2r") {
            let r = run_product("v6", pol, mok, dh, dr, f, seed, false);
            let (classes, residual) = classes_of(&r, pol);
            eprintln!(
                "{cls} seed {seed} {mok:?}: classes {classes:?} residual {residual:?} cnt_df2r {}",
                r.res["cnt_df2r"]
            );
            assert!(
                classes.contains(&"DF-2r"),
                "DF-2r DISAPPEARED at seed {seed}: convert this pin to a required-zero ({:?})",
                nonzero_problems(&r)
            );
            assert!(residual.is_empty(), "seed {seed}: unexplained {residual:?}");
        }
    }

    /// O-F1 (open item, `field` only; the model of record has no field policy): its seeds are counted as O-F1, never clean and never
    /// unexplained; a change in either direction fails here.
    #[test]
    fn of1_the_field_only_classes_stay_counted_as_an_open_item() {
        for &(cls, pol, mok, dh, dr, f, seed) in B1_SEEDS.iter().filter(|s| s.0 == "O-F1") {
            let r = run_product("v6", pol, mok, dh, dr, f, seed, false);
            let (classes, residual) = classes_of(&r, pol);
            eprintln!(
                "{cls} seed {seed} {mok:?} dh {dh} dr {dr} f {f}: classes {classes:?} residual {residual:?}"
            );
            assert!(
                classes.contains(&"O-F1"),
                "O-F1 is gone at seed {seed}: report it ({:?})",
                nonzero_problems(&r)
            );
            assert!(residual.is_empty(), "seed {seed}: unexplained {residual:?}");
        }
    }

    /// Debug aid: the O8 scan of the document-policy product grid over the seeds `Q6_O8_FROM..Q6_O8_FROM+Q6_O8_RUNS` (defaults 0 and
    /// 20, the asserted grid's size), one row per `Q6_O8_ROW` (`h` doc_hold, `r` doc_repair, `b` both; default `hrb`) and
    /// `Q6_O8_F` (default `0.3,0.1`): per row the runs, the runs with `cnt_o8` (outside HB-2), with `cnt_o8_in_hb2`, with
    /// `alive_mismatch`, the marker runs with a mismatch, and the (config, seed) of each marker run. The reference size is 1,500
    /// seeds over the 36 configurations (54,000 runs per row), chunked by seed range to bound a heavy-lock hold.
    #[test]
    #[ignore = "debug aid: Q6_O8_FROM / Q6_O8_RUNS / Q6_O8_ROW / Q6_O8_F select the O8 scan of the document-policy product grid"]
    fn scan_o8() {
        let ev = |k: &str, d: &str| {
            std::env::var(k).ok().filter(|v| !v.is_empty()).unwrap_or(d.to_string())
        };
        let from: u64 = ev("Q6_O8_FROM", "0").parse().unwrap();
        let seeds: u64 = ev("Q6_O8_RUNS", "20").parse().unwrap();
        let rows = ev("Q6_O8_ROW", "hrb");
        let fs: Vec<f64> =
            ev("Q6_O8_F", "0.3,0.1").split(',').map(|x| x.parse().unwrap()).collect();
        for row in rows.chars() {
            let (dh, dr) = match row {
                'h' => (true, false),
                'r' => (false, true),
                _ => (true, true),
            };
            for &f in &fs {
                let mut cfgs = crate::run::grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
                for c in cfgs.iter_mut() {
                    c.routes = Some(crate::run::Routes {
                        f,
                        hold: false,
                        repair: false,
                        doc_hold: dh,
                        doc_repair: dr,
                    });
                }
                let jobs: Vec<(crate::run::Cfg, u64)> =
                    cfgs.iter().flat_map(|c| (from..from + seeds).map(move |s| (*c, s))).collect();
                let next = std::sync::atomic::AtomicUsize::new(0);
                let out =
                    std::sync::Mutex::new((0u64, 0u64, 0u64, 0u64, 0u64, Vec::<String>::new()));
                let nth = std::thread::available_parallelism().map_or(4, |n| n.get());
                std::thread::scope(|sc| {
                    for _ in 0..nth {
                        sc.spawn(|| {
                            loop {
                                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if i >= jobs.len() {
                                    break;
                                }
                                let (c, s) = jobs[i];
                                let r = crate::run::run("v6", "v6", s, c, false);
                                let (mk, ih, am) = (
                                    r.res["cnt_o8"] != 0,
                                    r.res["cnt_o8_in_hb2"] != 0,
                                    r.res["alive_mismatch"] != 0,
                                );
                                let mut o = out.lock().unwrap();
                                o.0 += mk as u64;
                                o.1 += ih as u64;
                                o.2 += am as u64;
                                o.3 += (mk && am) as u64;
                                o.4 += 1;
                                if mk {
                                    o.5.push(format!(
                                        "({},{},{}) seed {s} am {am}",
                                        c.members, c.outs, c.keys
                                    ));
                                }
                            }
                        });
                    }
                });
                let (mk, ih, am, both, runs, mut hits) = out.into_inner().unwrap();
                hits.sort();
                eprintln!(
                    "O8 SCAN row {row} f {f} seeds {from}..{}: runs {runs}; cnt_o8 (outside HB-2) {mk}; cnt_o8_in_hb2 {ih}; alive_mismatch {am}; marker+mismatch {both}; {hits:?}",
                    from + seeds
                );
            }
        }
    }

    /// O8 (open design finding, FIX-1's consequence; `oracle::product_classes`): a live member keeps alive a vanished writer's stage
    /// that the oracle buries, because the writer's only non-lost entries above the stage's deadline are held out of position at
    /// live members and none is covered. The HB-2r seeds (101 and 730) show the shape inside HB-2's stages. Pinned in two
    /// configurations (the manager's ruling): DEFAULT (HB-2 exclusion on): the run is clean, `cnt_o8_in_hb2 >= 1`, `cnt_o8 == 0`,
    /// `cnt_hb2_excluded == 1`; EXCLUSION OFF: the run's `alive_mismatch` and `oracle_state` appear, the marker `cnt_o8` fires (the
    /// HB-2 set is empty then) and every problem is classified O8, none unclassified.
    #[test]
    fn o8_pin_the_uncovered_witness_shape_on_the_hb2r_seeds_inside_and_outside_the_exclusion() {
        for &(cls, pol, mok, dh, dr, f, seed) in B1_SEEDS.iter().filter(|s| s.0 == "HB-2r") {
            let r = run_product("v6", pol, mok, dh, dr, f, seed, false);
            assert!(
                nonzero_problems(&r).is_empty(),
                "{cls} seed {seed} {mok:?}: {:?}",
                nonzero_problems(&r)
            );
            assert!(
                r.res["cnt_o8_in_hb2"] >= 1,
                "seed {seed} {mok:?}: the shape inside HB-2 is gone"
            );
            assert_eq!(r.res["cnt_o8"], 0, "seed {seed} {mok:?}");
            assert_eq!(r.res["cnt_hb2_excluded"], 1, "seed {seed} {mok:?}");
            let mut c = crate::run::product_cfg(pol, mok, dh, dr, f);
            c.hb2_exclusion = false;
            let off = crate::run::run("v6", "v6", seed, c, false);
            let (classes, residual) = classes_of(&off, pol);
            let p = nonzero_problems(&off);
            eprintln!(
                "{cls} seed {seed} {mok:?}, exclusion off: problems {p:?} classes {classes:?} residual {residual:?}"
            );
            for k in ["alive_mismatch", "oracle_state"] {
                assert!(
                    p.iter().any(|x| x == k),
                    "seed {seed} {mok:?}: {k} expected with the exclusion off: {p:?}"
                );
            }
            assert!(
                off.res["cnt_o8"] == 1 && off.res["cnt_o8_stages"] > 0,
                "seed {seed} {mok:?}: marker"
            );
            assert!(classes.contains(&"O8"), "seed {seed} {mok:?}: {classes:?}");
            assert!(residual.is_empty(), "seed {seed} {mok:?}: unclassified {residual:?}");
        }
    }

    /// The vector step check's finding at 250 seeds (`inv_vector_disagrees_with_prefix`, Document 3/1/4 seed 25 and 3/3/3 seed 138 with
    /// `doc_hold`, 3/2/5 seed 154 with both routes, f 0.3): a window RELEASED a held mark (as the model, 6d `take_from`, does) although
    /// an earlier entry of the origin was still held, and the harness raised the receiver's VECTOR to the released entry, over the
    /// hold. The model's coverage never passes a hold (`covers` stops at a held-out-of-position entry). Fixed: a release raises the
    /// vector to the servable prefix only (`Member::raise_servable`); the mutant `m-release-behind-held` is the old behaviour. Pinned:
    /// clean (nothing unexplained) with the fix, and the mutant trips the step check on each seed.
    #[test]
    fn a_window_does_not_release_a_mark_past_an_earlier_held_entry_seeds_25_138_154() {
        type Pin = ((usize, usize, usize), bool, bool, u64);
        let seeds: [Pin; 3] = [
            ((3, 1, 4), true, false, 25),
            ((3, 3, 3), true, false, 138),
            ((3, 2, 5), true, true, 154),
        ];
        for (mok, dh, dr, seed) in seeds {
            let pol = Policy::Document;
            let r = run_product("v6", pol, mok, dh, dr, 0.3, seed, false);
            let (classes, residual) = classes_of(&r, pol);
            eprintln!("seed {seed} {mok:?}: classes {classes:?} residual {residual:?}");
            assert!(residual.is_empty(), "seed {seed} {mok:?}: unexplained {residual:?}");
            assert!(
                !nonzero_problems(&r).iter().any(|k| k == "inv_vector_disagrees_with_prefix"),
                "seed {seed}: the vector step check fires"
            );
            let m = run_product("m-release-behind-held", pol, mok, dh, dr, 0.3, seed, false);
            assert!(
                nonzero_problems(&m).iter().any(|k| k == "inv_vector_disagrees_with_prefix"),
                "seed {seed}: the mutant must trip the vector step check: {:?}",
                nonzero_problems(&m)
            );
        }
    }

    /// GATE SIZE (the manager's ruling): the product-route grid at `Q6_GATE_RUNS` seeds (default 250) x 36 configurations on doc_hold
    /// 0.3 and both routes 0.3, document policy, with every problem in a named class: 0 unclassified.
    #[test]
    #[ignore = "gate step: 250 seeds x 36 configs on two rows (set Q6_GATE_RUNS to change the size)"]
    fn the_product_routes_at_gate_size_leave_nothing_unclassified() {
        let seeds: u64 = std::env::var("Q6_GATE_RUNS")
            .ok()
            .filter(|v| !v.is_empty())
            .and_then(|v| v.parse().ok())
            .unwrap_or(250);
        for (dh, dr) in [(true, false), (true, true)] {
            let mut cfgs = crate::run::grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
            for c in cfgs.iter_mut() {
                c.routes = Some(crate::run::Routes {
                    f: 0.3,
                    hold: false,
                    repair: false,
                    doc_hold: dh,
                    doc_repair: dr,
                });
            }
            let (s, classes, residual) = crate::run::run_grid_classified(
                "v6",
                &cfgs,
                0,
                seeds,
                std::thread::available_parallelism().map_or(4, |n| n.get()),
            );
            eprintln!(
                "GATE SIZE doc_hold {dh} doc_repair {dr}: {} runs; classes {classes:?}; unclassified {}",
                s.runs,
                residual.len()
            );
            assert!(
                residual.is_empty(),
                "unclassified at gate size: {:?}",
                &residual[..residual.len().min(12)]
            );
        }
    }

    /// The DF measurement switches from the environment: `Q6_DF_FLAGS` is a comma list of `strict` (a witnessed-only entry is not
    /// held), `survive` (the oracle counts a version held as state as surviving) and `repair` (an end-of-run round of scoped repairs).
    fn df_flags(c: &mut crate::run::Cfg) {
        let v = std::env::var("Q6_DF_FLAGS").unwrap_or_default();
        c.strict_holders = v.split(',').any(|x| x == "strict");
        c.state_survives = v.split(',').any(|x| x == "survive");
        c.end_repair = v.split(',').any(|x| x == "repair");
    }

    /// Debug aid (DF items): the missing-head seeds (Field 336, 1361, 1365, 1046; Document 858) under `Q6_DF_FLAGS`, with the
    /// problems and classes of each.
    #[test]
    #[ignore = "debug aid: Q6_DF_FLAGS=strict,survive,repair selects the DF measurement switches"]
    fn show_df_missing_head_seeds() {
        let rows: [B1Seed; 5] = [
            ("O-F1", Policy::Field, (4, 2, 3), true, false, 0.3, 336),
            ("O-F1", Policy::Field, (4, 2, 5), true, false, 0.3, 1361),
            ("O-F1", Policy::Field, (3, 2, 3), false, true, 0.1, 1365),
            ("O-F1", Policy::Field, (4, 3, 5), false, true, 0.1, 1046),
            ("DOC-858", Policy::Document, (4, 3, 4), true, false, 0.3, 858),
        ];
        for (cls, pol, mok, dh, dr, f, seed) in rows {
            let mut c = crate::run::product_cfg(pol, mok, dh, dr, f);
            df_flags(&mut c);
            let r = crate::run::run("v6", "v6", seed, c, false);
            let (classes, residual) = classes_of(&r, pol);
            eprintln!(
                "DF {cls} {pol:?} {mok:?} seed {seed} flags {:?}: problems {:?} classes {classes:?} residual {residual:?}",
                std::env::var("Q6_DF_FLAGS").unwrap_or_default(),
                nonzero_problems(&r)
            );
        }
    }

    /// Debug aid (DF items): the document-policy product grid over seeds `Q6_DF_FROM..+Q6_DF_RUNS` (defaults 0 and 20) for the rows
    /// `Q6_DF_ROW` (`h`, `r`, `b`; default `hrb`) and `Q6_DF_F` (default `0.3,0.1`), under `Q6_DF_FLAGS`; `Q6_DF_POLICY=field`
    /// switches the policy. Prints, per row, the runs, the problems (runs) and the named classes.
    #[test]
    #[ignore = "debug aid: Q6_DF_FROM / Q6_DF_RUNS / Q6_DF_ROW / Q6_DF_F / Q6_DF_FLAGS / Q6_DF_POLICY select the DF grid measurement"]
    fn scan_df() {
        let ev = |k: &str, d: &str| {
            std::env::var(k).ok().filter(|v| !v.is_empty()).unwrap_or(d.to_string())
        };
        let from: u64 = ev("Q6_DF_FROM", "0").parse().unwrap();
        let seeds: u64 = ev("Q6_DF_RUNS", "20").parse().unwrap();
        let pol =
            if ev("Q6_DF_POLICY", "doc") == "field" { Policy::Field } else { Policy::Document };
        let fs: Vec<f64> =
            ev("Q6_DF_F", "0.3,0.1").split(',').map(|x| x.parse().unwrap()).collect();
        for row in ev("Q6_DF_ROW", "hrb").chars() {
            let (dh, dr) = match row {
                'h' => (true, false),
                'r' => (false, true),
                _ => (true, true),
            };
            for &f in &fs {
                let mut cfgs = crate::run::grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
                for c in cfgs.iter_mut() {
                    c.policy = pol;
                    c.routes = Some(crate::run::Routes {
                        f,
                        hold: false,
                        repair: false,
                        doc_hold: dh,
                        doc_repair: dr,
                    });
                    df_flags(c);
                }
                let (s, classes, residual) = crate::run::run_grid_classified(
                    &ev("Q6_DF_MODE", "v6"),
                    &cfgs,
                    from,
                    seeds,
                    std::thread::available_parallelism().map_or(4, |n| n.get()),
                );
                eprintln!(
                    "DF SCAN {pol:?} row {row} f {f} seeds {from}..{} flags {:?}: runs {}; problems {:?}; classes {classes:?}; unclassified {}; {:?}",
                    from + seeds,
                    ev("Q6_DF_FLAGS", ""),
                    s.runs,
                    s.problems().iter().map(|(k, v)| format!("{k}={}", v.0)).collect::<Vec<_>>(),
                    residual.len(),
                    &residual[..residual.len().min(12)]
                );
            }
        }
    }

    /// Debug aid: the draws of one product-route run, for the Python replay (`Q6_X=pol,m,o,k,dh,dr,f,seed`, mode `Q6_MODE`, output
    /// `Q6_OUT`): `B n v` / `F x` / `P order` lines as `replay8.py` reads them.
    #[test]
    #[ignore = "debug aid: Q6_X, Q6_MODE and Q6_OUT export one run's random draws"]
    fn export_draws() {
        let spec = std::env::var("Q6_X").expect("Q6_X");
        let t: Vec<&str> = spec.split(',').collect();
        let pol = if t[0] == "field" { Policy::Field } else { Policy::Document };
        let p = |i: usize| t[i].parse::<usize>().unwrap();
        let mode = std::env::var("Q6_MODE").unwrap_or("v6".into());
        crate::rng::LOG.with(|l| *l.borrow_mut() = Some(Vec::new()));
        let r = run_product(
            &mode,
            pol,
            (p(1), p(2), p(3)),
            t[4] == "1",
            t[5] == "1",
            t[6].parse().unwrap(),
            t[7].parse().unwrap(),
            false,
        );
        let lines = crate::rng::LOG.with(|l| l.borrow_mut().take().unwrap());
        std::fs::write(std::env::var("Q6_OUT").expect("Q6_OUT"), lines.join("\n") + "\n").unwrap();
        eprintln!(
            "EXPORTED {} draws; RES {:?}; page_carries_held_state {} page_above_coverage {} grant_above_coverage {}",
            lines.len(),
            nonzero_problems(&r),
            r.res["cnt_page_carries_held_state"],
            r.res["cnt_page_above_coverage"],
            r.res["cnt_grant_above_coverage"]
        );
    }

    /// Debug aid: one product-route run with its trace and every member's final state (`Q6_ONE=<doc|field>,m,o,k,dh,dr,f,seed`).
    #[test]
    #[ignore = "debug aid: Q6_ONE=<doc|field>,m,o,k,dh,dr,f,seed prints one product-route run"]
    fn show_b1_one() {
        let spec = std::env::var("Q6_ONE").expect("Q6_ONE");
        let t: Vec<&str> = spec.split(',').collect();
        let pol = if t[0] == "field" { Policy::Field } else { Policy::Document };
        let p = |i: usize| t[i].parse::<usize>().unwrap();
        let mode = std::env::var("Q6_MODE").unwrap_or("v6".into());
        let r = run_product(
            &mode,
            pol,
            (p(1), p(2), p(3)),
            t[4] == "1",
            t[5] == "1",
            t[6].parse().unwrap(),
            t[7].parse().unwrap(),
            true,
        );
        for l in &r.w.log {
            eprintln!("{l}");
        }
        for m in &r.ms {
            eprintln!(
                "MEMBER {} vanished {} serving {:?} oop {:?} info {:?}",
                m.name,
                m.vanished,
                m.serving(),
                m.held_oop.iter().map(|&i| r.w.ent(i).to_string()).collect::<Vec<_>>(),
                m.info.keys().map(|&i| r.w.ent(i).to_string()).collect::<Vec<_>>()
            );
            for (n, ph) in m.phys.iter() {
                eprintln!(
                    "    {n} alive {} missing {} orphan {} docs {:?}",
                    ph.alive,
                    ph.missing,
                    ph.orphan,
                    ph.docs
                        .iter()
                        .map(|(k, v)| (*k, crate::entry::hlc_of(&v.stamp), v.body.is_some()))
                        .collect::<Vec<_>>()
                );
            }
        }
        for m in r.ms.iter().filter(|m| !m.vanished) {
            eprintln!("STEPINV {} {:?} {:?}", m.name, m.step_inv, m.inv);
            for (n, ph) in m.phys.iter().filter(|(_, p)| p.orphan) {
                if let Some(cr) = m.creates.get(n) {
                    let wit: Vec<String> = r.w.by_origin[cr.origin]
                        .iter()
                        .filter(|&&id| r.w.ent(id).hlc > cr.dl && m.cv.has(id))
                        .map(|&id| {
                            format!(
                                "{}{}",
                                r.w.ent(id),
                                if m.held_oop.contains(&id) { "(oop)" } else { "" }
                            )
                        })
                        .collect();
                    eprintln!(
                        "ORPHAN {} {n} kind {:?} abandoned {} dl {} covers_servable {} witnesses {wit:?}",
                        m.name,
                        ph.kind,
                        m.abandoned.contains(n),
                        cr.dl,
                        m.covers_servable(&r.w, cr.origin, cr.dl)
                    );
                }
            }
        }
        eprintln!(
            "LOST {:?}",
            r.w.lost.iter().map(|&i| r.w.ent(i).to_string()).collect::<Vec<_>>()
        );
        eprintln!("RES {:?}", nonzero_problems(&r));
    }

    /// Debug aid: the B1 seeds under a mode (`Q6_MODE`), with the problems of each.
    #[test]
    #[ignore = "debug aid: prints the problems of the B1 classification's seeds"]
    fn show_b1_seeds() {
        let mode = std::env::var("Q6_MODE").unwrap_or("v6".into());
        for &(cls, pol, mok, dh, dr, f, seed) in B1_SEEDS {
            let r = run_product(&mode, pol, mok, dh, dr, f, seed, false);
            eprintln!(
                "{cls:5} {pol:?} {mok:?} dh {dh} dr {dr} f {f} seed {seed}: {:?} hb2 stages {} excluded {}",
                nonzero_problems(&r),
                r.res["cnt_hb2_stages"],
                r.res["cnt_hb2_excluded"]
            );
        }
    }
}
