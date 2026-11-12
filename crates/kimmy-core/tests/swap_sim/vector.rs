//! B2 of the harness review: the product's coverage, a per-origin (node, epoch) maximum (`coverage::VectorCoverage`), beside the
//! model's prefix. The vector is kept in every run; with `World::vector_cov` the members' coverage predicate reads it. The step
//! check `vector_disagrees_with_prefix` (in `check_inv`) asserts on the in-position routes (R1 windows, R4 grants; R2 has its own test
//! in `routes.rs`) that the vector covers exactly the contiguous servable prefix. A grant read off the vector is checked against the
//! sender's true prefix (invariant (i), `grant_above_coverage`), not given by construction.

#[cfg(test)]
mod tests {
    use crate::entry::*;
    use crate::member::DocOp;
    use crate::oracle::finish;
    use crate::run::{grid, run_grid, run_grid_classified};
    use crate::scenario::Sc;

    fn nonzero(res: &crate::oracle::Res) -> Vec<String> {
        let mut s = crate::run::Summary::default();
        s.add(0, res);
        s.problems().keys().cloned().collect()
    }

    /// W stages S (create, a row of key 1), binds; X takes the create in a window, then the bind is PUSHED to X without an
    /// introduced window (the row is a hole). Correct: the push applies under Hold, the vector does not rise, rule B keeps the bind as
    /// information until X's windows fill the hole. `m-q6-kind-under-raise`: the push applies under Raise, the vector covers the bind
    /// over the hole, X flips without the row (a partial flip) and grants the lie to a joiner Y (invariant (i) checked).
    fn push_over_a_hole(mode: &str) -> (bool, bool, Vec<String>) {
        let mut sc = Sc::new(mode, &[("W", 0), ("X", 0), ("Y", 0)], 60);
        sc.w.vector_cov = true;
        sc.w.held_uncovered = true; // the product's routes: a held entry is not covered and a window re-delivers it
        sc.w.reg_in_position_check = false; // the push delivers the bind out of position on purpose (P1 asserts the check names it)
        let (w_, x) = (sc.idx("W"), sc.idx("X"));
        sc.advance(1);
        sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.pull("X", "W");
        sc.pull("Y", "W");
        sc.advance(10);
        let mut job = sc.ms[w_].start_out(&mut sc.w, vec![1], JobKind::Good, 0);
        sc.advance(12);
        let b = sc.ms[w_].do_bind(&mut sc.w, &mut job, 3).unwrap();
        let c =
            sc.w.all
                .iter()
                .find(|e| e.kind == Kind::Create && e.phys == job.stage)
                .unwrap()
                .clone();
        sc.window("X", "W", &c);
        let xi = sc.idx("X");
        sc.ms[xi].push_take(&mut sc.w, &b);
        sc.check();
        let flipped_over_hole = sc.ms[x].serving().as_deref() == Some(job.stage.as_str());
        // a joiner takes X's grant while the hole is open
        sc.snapshot("Y", "X", false);
        sc.check();
        let lie_granted = sc.w.stats.get("grant_above_coverage") > 0;
        sc.pull("X", "W");
        sc.pull("Y", "X");
        sc.quiesce();
        let res = finish(&mut sc.w, &mut sc.ms);
        (flipped_over_hole, lie_granted, nonzero(&res))
    }

    #[test]
    fn b2_a_push_without_a_window_raises_no_vector_and_the_raise_mutant_lies() {
        for pre in ["", "field:"] {
            let (flip, lie, p) = push_over_a_hole(&format!("{pre}v6"));
            assert!(!flip && !lie, "{pre}v6: flip {flip} lie {lie}");
            assert!(p.is_empty(), "{pre}v6: {p:?}");
            let (flip, lie, p) = push_over_a_hole(&format!("{pre}m-q6-kind-under-raise"));
            eprintln!(
                "{pre}m-q6-kind-under-raise: flipped over the hole {flip}, grant above coverage {lie}, {p:?}"
            );
            assert!(flip && lie, "the mutant's vector lies: flip {flip} lie {lie}");
            for k in
                ["inv_partial_flip", "inv_vector_disagrees_with_prefix", "inv_grant_above_coverage"]
            {
                assert!(p.iter().any(|x| x == k), "{pre}: {k} must fire: {p:?}");
            }
        }
    }

    /// The vector agrees with the prefix on the in-position routes: the base grid and the product-route grid under the vector coverage
    /// give exactly the figures of the prefix coverage (every check and counter), and the step check never fires.
    #[test]
    fn b2_the_grids_under_the_vector_coverage_equal_the_prefix_coverage() {
        let seeds = crate::env_u64("Q6_VECTOR_RUNS", 10);
        let mut cfgs = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        let a = run_grid("v6", "v6", &cfgs, 0, seeds, crate::threads());
        for c in cfgs.iter_mut() {
            c.vector_cov = true;
        }
        let b = run_grid("v6", "v6", &cfgs, 0, seeds, crate::threads());
        assert_eq!(a.tot, b.tot, "base grid: vector vs prefix");
        // (the product routes: a register event whose origin the window covers over an entry held here out of position is parked
        // for one settle by the prefix, until `promote_oop` releases the held entry in the same transaction, and takes effect at once
        // under the vector, which the window raised: `cnt_info_parked` / `cnt_info_taken_effect` differ, nothing else)
        let same = |s: &crate::run::Summary| -> std::collections::BTreeMap<String, i64> {
            s.tot
                .iter()
                .filter(|(k, _)| !k.starts_with("cnt_info_"))
                .map(|(k, v)| (k.clone(), *v))
                .collect()
        };
        assert_eq!(b.get("inv_vector_disagrees_with_prefix"), 0);
        for policy in [crate::world::Policy::Document, crate::world::Policy::Field] {
            for (dh, dr) in [(true, false), (false, true), (true, true)] {
                let mut cfgs: Vec<_> = grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5])
                    .into_iter()
                    .map(|c| {
                        crate::run::product_cfg(policy, (c.members, c.outs, c.keys), dh, dr, 0.3)
                    })
                    .collect();
                let (a, ca, ra) = run_grid_classified("v6", &cfgs, 0, seeds, crate::threads());
                for c in cfgs.iter_mut() {
                    c.vector_cov = true;
                }
                let (b, cb, rb) = run_grid_classified("v6", &cfgs, 0, seeds, crate::threads());
                eprintln!(
                    "{policy:?} doc_hold {dh} doc_repair {dr} 0.3 under the vector: {} runs, classes {cb:?}, unclassified {}",
                    b.runs,
                    rb.len()
                );
                assert_eq!(same(&a), same(&b), "{policy:?} {dh} {dr}: vector vs prefix");
                eprintln!(
                    "    info parked: prefix {} vector {}",
                    a.get("cnt_info_parked"),
                    b.get("cnt_info_parked")
                );
                assert_eq!((ca, ra), (cb, rb));
                assert_eq!(b.get("inv_vector_disagrees_with_prefix"), 0);
            }
        }
    }
}
