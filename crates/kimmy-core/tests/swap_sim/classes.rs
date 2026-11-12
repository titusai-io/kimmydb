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
        ("O-F1", Policy::Field, (4, 2, 3), true, false, 0.3, 336),
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

    /// PIN of DF-1 (open design finding, UNDER REVIEW: atomicity, a partial flip on the product-shaped document routes: a member
    /// treats as a loser an entry superseded only by a version it does not cover). The class must OCCUR on each listed seed, with
    /// nothing else unexplained. This pin FAILS LOUDLY once the class disappears: when the design rule lands, convert it to a
    /// required-zero (and drop DF-1 from `oracle::product_classes`, so the grid fails on it).
    #[test]
    fn df1_pin_the_partial_flip_class_occurs_on_its_seeds() {
        for &(cls, pol, mok, dh, dr, f, seed) in B1_SEEDS.iter().filter(|s| s.0 == "DF-1") {
            let r = run_product("v6", pol, mok, dh, dr, f, seed, false);
            let (classes, residual) = classes_of(&r, pol);
            eprintln!(
                "{cls} seed {seed} {mok:?} dh {dh} dr {dr} f {f}: classes {classes:?} residual {residual:?}"
            );
            assert!(
                classes.contains(&"DF-1"),
                "DF-1 DISAPPEARED at seed {seed} {mok:?} dh {dh} dr {dr} f {f}: convert this pin to a required-zero and drop the \
                 class from oracle::product_classes ({:?})",
                nonzero_problems(&r)
            );
            assert!(residual.is_empty(), "seed {seed}: unexplained {residual:?}");
        }
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
