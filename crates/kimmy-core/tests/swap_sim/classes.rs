//! The B1 classification of the product-route grid (`q7-logs/q6h-b1/`, the manager's rulings), as named classes with their own
//! tests: HB-1 (a held entry servable before its release) is a harness bug, fixed; the rest are pinned here.

#[cfg(test)]
mod tests {
    use crate::run::run_product;
    use crate::world::Policy;

    /// (class, policy, members/outs/keys, doc_hold, doc_repair, f, seed): one run of the B1 classification.
    type B1Seed = (&'static str, Policy, (usize, usize, usize), bool, bool, f64, u64);

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

    /// Debug aid: the B1 seeds under a mode (`Q6_MODE`), with the problems of each.
    #[test]
    #[ignore = "debug aid: prints the problems of the B1 classification's seeds"]
    fn show_b1_seeds() {
        let mode = std::env::var("Q6_MODE").unwrap_or("v6".into());
        // (class, policy, m/o/k, doc_hold, doc_repair, f, seed)
        let seeds: &[B1Seed] = &[
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
        for &(cls, pol, mok, dh, dr, f, seed) in seeds {
            let r = run_product(&mode, pol, mok, dh, dr, f, seed, false);
            eprintln!(
                "{cls:5} {pol:?} {mok:?} dh {dh} dr {dr} f {f} seed {seed}: {:?}",
                nonzero_problems(&r)
            );
        }
    }
}
