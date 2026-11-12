//! B3 of the harness review: the route tests the design's ASSUMED table names ("The ASSUMED items, each with the harness test that
//! settles it"): H-Q6-K1, R2, W1, P1, R4, R4b, V1, L1. Where the harness lacked the route, it has a minimal emulation (named in each
//! test). Each test has a mutant that it kills. These settle the items in the HARNESS'S emulation only: each stays ASSUMED for the
//! product until the same test runs against the product's route.

#[cfg(test)]
mod tests {
    use crate::coverage::CoverageOf;
    use crate::entry::*;
    use crate::member::DocOp;
    use crate::oracle::finish;
    use crate::scenario::Sc;

    fn nonzero(res: &crate::oracle::Res) -> Vec<String> {
        let mut s = crate::run::Summary::default();
        s.add(0, res);
        s.problems().keys().cloned().collect()
    }

    fn step_invs(sc: &Sc) -> Vec<&'static str> {
        let mut v: Vec<&'static str> =
            sc.ms.iter().flat_map(|m| m.step_inv.iter().chain(m.inv.iter()).copied()).collect();
        v.sort();
        v.dedup();
        v
    }

    /// H-Q6-K1: the stage's create is a `CreateCollection` (`is_ddl`) carrying the deadline and the `mergePolicy`. EMULATION: the
    /// harness has no product oplog; `Entry::product_op_kind` is the mapping the build must confirm by decoding a real replacing
    /// `$out`'s creation entry. Mutant `m-stage-create-not-ddl`.
    fn k1(mode: &str) -> bool {
        let mut sc = Sc::new(mode, &[("W", 0), ("X", 0)], 60);
        sc.advance(1);
        let job = sc.ms[0].start_out(&mut sc.w, vec![1, 2], JobKind::Good, 0);
        let c =
            sc.w.all
                .iter()
                .find(|e| e.kind == Kind::Create && e.phys == job.stage)
                .unwrap()
                .clone();
        let k = c.product_op_kind(&sc.w.f);
        k == Some(kimmy_core::OpKind::CreateCollection)
            && k.is_some_and(|k| k.is_ddl())
            && c.dl == job.dl
            && c.merge_policy == Some(sc.w.policy)
    }

    #[test]
    fn h_q6_k1_the_stage_create_is_a_create_collection_with_its_deadline_and_merge_policy() {
        for pre in ["", "field:"] {
            assert!(k1(&format!("{pre}v6")), "{pre}v6");
            assert!(!k1(&format!("{pre}m-stage-create-not-ddl")), "{pre}: the mutant must fail K1");
        }
    }

    /// H-Q6-R2: `Repair::Replay` serves a range contiguous per origin from `min(behind, floor)`, and a Q6 kind inside it lands in
    /// position. EMULATION: `Member::replay_from`. Under the product's vector coverage: X's vector already covers a hole (the
    /// count-divergence premise: W's staged row r is missing at X, a later W entry was applied under Raise); the replay from the floor
    /// delivers r, then the bind, in position. Asserted: no entry of the served origin inside the range is left neither held nor
    /// witnessed, the bind lands in position (no `q6_kind_in_window_out_of_position`), X flips with the row. Mutant
    /// `m-replay-skips-an-entry`: the bind lands over the hole, X flips without the row.
    fn r2(mode: &str) -> (bool, bool, Vec<&'static str>, Vec<String>) {
        let mut sc = Sc::new(mode, &[("W", 0), ("X", 0), ("Y", 0)], 60);
        sc.w.vector_cov = true;
        let (w_, x) = (sc.idx("W"), sc.idx("X"));
        sc.advance(1);
        sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.pull("X", "W");
        sc.pull("Y", "W");
        sc.advance(10);
        let mut job = sc.ms[w_].start_out(&mut sc.w, vec![1], JobKind::Good, 0);
        let c =
            sc.w.all
                .iter()
                .find(|e| e.kind == Kind::Create && e.phys == job.stage)
                .unwrap()
                .clone();
        sc.advance(11);
        let d2 = sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Update).unwrap();
        sc.advance(12);
        let b = sc.ms[w_].do_bind(&mut sc.w, &mut job, 3).unwrap();
        sc.window("X", "W", &c);
        sc.take("X", &d2); // the hole: the vector rises over the staged row (a Raise), the count divergence the replay repairs
        let floor: std::collections::BTreeMap<usize, i64> = [(w_, c.hlc)].into_iter().collect();
        let (xm, wm) = crate::relay::pair(&mut sc.ms, x, w_);
        xm.replay_from(&mut sc.w, wm, &floor);
        sc.check();
        let contiguous = sc.w.by_origin[w_]
            .iter()
            .filter(|&&i| sc.w.ent(i).hlc > c.hlc && sc.w.ent(i).hlc <= b.hlc)
            .all(|&i| sc.ms[x].cv.has(i));
        let flipped = sc.ms[x].serving().as_deref() == Some(job.stage.as_str());
        let inv = step_invs(&sc);
        sc.pull("Y", "W");
        sc.pull("X", "W");
        sc.quiesce();
        let res = finish(&mut sc.w, &mut sc.ms);
        (contiguous, flipped, inv, nonzero(&res))
    }

    #[test]
    fn h_q6_r2_replay_serves_a_contiguous_range_per_origin_and_a_q6_kind_inside_lands_in_position()
    {
        for pre in ["", "field:"] {
            let (contig, flipped, inv, p) = r2(&format!("{pre}v6"));
            assert!(contig && flipped, "{pre}v6: contiguous {contig} flipped {flipped}");
            assert!(
                !inv.contains(&"q6_kind_in_window_out_of_position")
                    && !inv.contains(&"partial_flip"),
                "{inv:?}"
            );
            assert!(p.is_empty(), "{pre}v6: {p:?}");
            let (contig, _, inv, p) = r2(&format!("{pre}m-replay-skips-an-entry"));
            eprintln!("{pre}m-replay-skips-an-entry: contiguous {contig}, {inv:?}, {p:?}");
            assert!(!contig);
            for k in ["q6_kind_in_window_out_of_position", "partial_flip"] {
                assert!(inv.contains(&k), "{pre}: {k} must fire: {inv:?}");
            }
        }
    }

    /// H-Q6-W1: a sender holding a skipped entry (a last-writer-wins loser) serves a window; the receiver's witnessed vector rises
    /// only past entries sent or witnessed by the skip rule (the step checks `window_raises_past_unsent` and
    /// `vector_disagrees_with_prefix`), and the loser is witnessed, not received. The sender holds a staged row out of position (its
    /// stage unknown at hold time); the mutant `m-held-servable-on-deferred-apply` (HB-1) makes it serve that row while it lacks the row
    /// before it: the receiver's coverage would rise past an entry nobody sent or witnessed.
    fn w1(mode: &str) -> (bool, Vec<&'static str>) {
        let mut sc = Sc::new(mode, &[("W", 0), ("S", 0), ("R", 0), ("Y", 50)], 60);
        sc.w.vector_cov = true;
        sc.w.held_uncovered = true;
        let (w_, y, s) = (sc.idx("W"), sc.idx("Y"), sc.idx("S"));
        sc.advance(1);
        sc.ms[w_].local_doc(&mut sc.w, 5, DocOp::Insert).unwrap();
        for m in ["S", "R", "Y"] {
            sc.pull(m, "W");
        }
        sc.advance(2);
        let yv = sc.ms[y].local_doc(&mut sc.w, 5, DocOp::Update).unwrap(); // Y's clock is ahead: newer than W's next write
        sc.window("S", "Y", &yv);
        sc.advance(4);
        let x = sc.ms[w_].local_doc(&mut sc.w, 5, DocOp::Update).unwrap(); // loses at S: witnessed, not appended
        sc.advance(10);
        let job = sc.ms[w_].start_out(&mut sc.w, vec![1, 2], JobKind::Good, 0);
        let rows: Vec<EntryRef> =
            sc.w.stage_docs[&job.stage].iter().map(|&i| sc.w.ent_rc(i)).collect();
        let c =
            sc.w.all
                .iter()
                .find(|e| e.kind == Kind::Create && e.phys == job.stage)
                .unwrap()
                .clone();
        sc.ms[s].take_hold(&mut sc.w, &rows[1]); // held out of position, its stage unknown here: deferred
        sc.window("S", "W", &x);
        sc.window("S", "W", &c); // the deferred apply runs now; rows[0] is still missing at S
        // (`document`: x loses last-writer-wins at S and is witnessed; `field` joins the two writes, so x is appended there)
        let document = sc.w.policy == crate::world::Policy::Document;
        assert!(!document || !sc.ms[s].appended.contains(&x.idx), "x loses at S");
        sc.pull("R", "S");
        sc.check();
        let r = sc.idx("R");
        let witnessed =
            !document || (sc.ms[r].cv.has(x.idx) && !sc.ms[r].appended.contains(&x.idx));
        (witnessed, step_invs(&sc))
    }

    #[test]
    fn h_q6_w1_a_window_raises_coverage_only_past_entries_sent_or_witnessed() {
        for pre in ["", "field:"] {
            let (wit, inv) = w1(&format!("{pre}v6"));
            assert!(wit, "{pre}v6: R witnesses the loser through S's skip rule");
            assert!(inv.is_empty(), "{pre}v6: {inv:?}");
            let (_, inv) = w1(&format!("{pre}m-held-servable-on-deferred-apply"));
            eprintln!("{pre}m-held-servable-on-deferred-apply on W1: {inv:?}");
            assert!(inv.contains(&"window_raises_past_unsent"), "{pre}: {inv:?}");
        }
    }

    /// H-Q6-P1: a push with no introduced window. A register event is information only until covered (D10 counts it: a write on the
    /// physical it supersedes is refused); a stage create trips the debug assertion of invariant (ii) and makes no stage. EMULATION:
    /// `Member::push_take` (Hold). Mutant `m-q6-kind-under-raise` (Raise).
    fn p1(mode: &str) -> (bool, bool, bool, bool, Vec<String>) {
        let mut sc = Sc::new(mode, &[("W", 0), ("X", 0)], 60);
        sc.w.vector_cov = true;
        sc.w.held_uncovered = true;
        let (w_, x) = (sc.idx("W"), sc.idx("X"));
        sc.advance(1);
        sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.pull("X", "W");
        sc.advance(10);
        let mut job = sc.ms[w_].start_out(&mut sc.w, vec![1], JobKind::Good, 0);
        let c =
            sc.w.all
                .iter()
                .find(|e| e.kind == Kind::Create && e.phys == job.stage)
                .unwrap()
                .clone();
        sc.advance(12);
        let b = sc.ms[w_].do_bind(&mut sc.w, &mut job, 2).unwrap();
        sc.ms[x].push_take(&mut sc.w, &b);
        // invariant (ii)'s debug assertion names the out-of-position delivery (M1); it is then cleared so the run's checks judge the rest
        let named = sc.ms[x].inv.remove("register_event_out_of_position");
        assert!(named, "{mode}: invariant (ii) names the pushed bind");
        let info = sc.ms[x].info.contains_key(&b.idx) && !sc.ms[x].events.contains(&b.idx);
        sc.advance(13);
        let refused = sc.ms[x].local_doc(&mut sc.w, 0, DocOp::Update).is_none()
            && sc.ms[x].refusal_eps.values().any(|e| e.info);
        sc.ms[x].push_take(&mut sc.w, &c);
        let asserted = sc.ms[x].inv.contains("push_carries_q6_kind_out_of_position");
        let no_stage = !sc.ms[x].phys.contains_key(&job.stage);
        sc.ms[x].inv.remove("push_carries_q6_kind_out_of_position"); // asserted above; the run then converges by windows
        sc.pull("X", "W");
        sc.quiesce();
        let res = finish(&mut sc.w, &mut sc.ms);
        (info, refused, asserted, no_stage, nonzero(&res))
    }

    #[test]
    fn h_q6_p1_a_push_without_a_window_keeps_q6_kinds_as_information_or_refuses_them() {
        for pre in ["", "field:"] {
            let (info, refused, asserted, no_stage, p) = p1(&format!("{pre}v6"));
            assert!(
                info && refused && asserted && no_stage,
                "{pre}v6: {info} {refused} {asserted} {no_stage}"
            );
            assert!(p.is_empty(), "{pre}v6: {p:?}");
            let (info, refused, asserted, no_stage, p) = p1(&format!("{pre}m-q6-kind-under-raise"));
            eprintln!(
                "{pre}m-q6-kind-under-raise on P1: info {info} refused {refused} asserted {asserted} no stage {no_stage} {p:?}"
            );
            assert!(
                !info && !asserted && !no_stage,
                "{pre}: the mutant applies the push under Raise"
            );
        }
    }

    /// H-Q6-R4 and R4b: a whole-database page read after the grant carries a once-swapped name's drop (R4: a logical drop stamped above
    /// the grant) or a bound-out physical (R4b: the receiver has not received the bind). Both are information only: the receiver keeps
    /// serving the name, and buries only when the drop's or the bind's own entry arrives in position (by a window); then every member
    /// converges and nothing is resurrected. EMULATION: the pages' live read of the sender's buried physicals (`GrantRead::dead`).
    /// Mutants: `m-page-drop-by-page-versions` (R4: route B, gated by the page's versions) and `m-page-drop-flag-ignored` (R4b: the
    /// sender's once-swapped flag ignored, applied as a never-swapped name's drop).
    fn r4(mode: &str, bind: bool) -> (bool, Vec<&'static str>, Vec<String>) {
        let mut sc = Sc::new(mode, &[("A", 0), ("B", 0), ("R", 0)], 60);
        let (a, r) = (sc.idx("A"), sc.idx("R"));
        sc.advance(1);
        sc.ms[a].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.ms[a].local_doc(&mut sc.w, 1, DocOp::Insert).unwrap();
        sc.pull("B", "A");
        sc.pull("R", "A");
        sc.advance(5);
        assert!(sc.snapshot("R", "B", true), "R begins a lazy join from B");
        sc.advance(10);
        if bind {
            let mut job = sc.ms[a].start_out(&mut sc.w, vec![2], JobKind::Good, 0);
            sc.advance(12);
            sc.ms[a].do_bind(&mut sc.w, &mut job, 3).unwrap();
        } else {
            sc.ms[a].local_ldrop(&mut sc.w).unwrap();
        }
        sc.pull("B", "A"); // B buries L (bound out, or dropped) above R's grant
        while sc.ms[r].join.is_some() {
            sc.land("R");
        }
        sc.check();
        let still = sc.ms[r].phys.get(L).is_some_and(|p| p.alive)
            && sc.ms[r].serving().as_deref() == Some(L);
        let inv = step_invs(&sc);
        sc.pull("R", "A");
        sc.pull("R", "B");
        sc.check();
        sc.quiesce();
        let res = finish(&mut sc.w, &mut sc.ms);
        (still, inv, nonzero(&res))
    }

    #[test]
    fn h_q6_r4_a_page_drop_read_above_the_grant_is_information_only() {
        for pre in ["", "field:"] {
            let (still, inv, p) = r4(&format!("{pre}v6"), false);
            assert!(
                still && inv.is_empty() && p.is_empty(),
                "{pre}v6: still {still} {inv:?} {p:?}"
            );
            let (still, inv, p) = r4(&format!("{pre}m-page-drop-by-page-versions"), false);
            eprintln!(
                "{pre}m-page-drop-by-page-versions on R4: still serving L {still} {inv:?} {p:?}"
            );
            assert!(
                !still && (!inv.is_empty() || !p.is_empty()),
                "{pre}: the mutant buries L by a page"
            );
        }
    }

    #[test]
    fn h_q6_r4b_a_flagged_drop_of_a_once_swapped_physical_waits_for_the_bind() {
        for pre in ["", "field:"] {
            let (still, inv, p) = r4(&format!("{pre}v6"), true);
            assert!(
                still && inv.is_empty() && p.is_empty(),
                "{pre}v6: still {still} {inv:?} {p:?}"
            );
            let (still, inv, p) = r4(&format!("{pre}m-page-drop-flag-ignored"), true);
            eprintln!(
                "{pre}m-page-drop-flag-ignored on R4b: still serving L {still} {inv:?} {p:?}"
            );
            assert!(
                !still && (!inv.is_empty() || !p.is_empty()),
                "{pre}: the mutant buries L before the bind"
            );
        }
    }

    /// H-Q6-V1: the `AskVersions` reply equals the answerer's servable vector, also while it holds an entry out of position (the
    /// harness' stand-in for a relogged CreateIndex: a held document entry). The servable vector is computed apart, from the
    /// member's contiguous servable prefix. EMULATION: `Member::ask_versions`. Mutant `m-askversions-counts-held`.
    fn v1(mode: &str) -> bool {
        let mut sc = Sc::new(mode, &[("W", 0), ("X", 0)], 60);
        sc.w.held_uncovered = true;
        let (w_, x) = (sc.idx("W"), sc.idx("X"));
        sc.advance(1);
        sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.pull("X", "W");
        sc.advance(2);
        sc.ms[w_].local_doc(&mut sc.w, 1, DocOp::Insert).unwrap();
        sc.advance(3);
        let d2 = sc.ms[w_].local_doc(&mut sc.w, 2, DocOp::Insert).unwrap();
        sc.ms[x].take_hold(&mut sc.w, &d2);
        assert!(sc.ms[x].held_oop.contains(&d2.idx));
        let reply = sc.ms[x].ask_versions(&sc.w);
        let servable: i64 = sc.w.by_origin[w_]
            .iter()
            .take_while(|&&i| sc.ms[x].cv.has(i) && !sc.ms[x].held_oop.contains(&i))
            .map(|&i| sc.w.ent(i).hlc)
            .last()
            .unwrap_or(-1);
        reply.get(&w_).copied().unwrap_or(-1) == servable
    }

    #[test]
    fn h_q6_v1_askversions_answers_the_servable_vector() {
        assert!(v1("v6"));
        assert!(!v1("m-askversions-counts-held"), "the mutant answers a held entry");
    }

    /// M1 of the harness review: invariant (ii) is checked for EVERY Q6 kind (the stage's create, the abandon and the loss record, not
    /// only the register events), and it is on in scripted scenarios. A scripted delivery of each kind ahead of its position is named
    /// (seed 750's shape for the abandon); the same delivery in position is not. On the product-route grid, `m-doc-hold-q6-kinds`
    /// (the hold relog carries creates, abandons and loss entries) trips the general check, not only the route's own flag.
    #[test]
    fn m1_invariant_ii_names_every_q6_kind_delivered_out_of_position() {
        for kind in [Kind::Create, Kind::Abandon, Kind::Loss] {
            for in_position in [false, true] {
                let mut sc = Sc::new("v6", &[("W", 0), ("X", 0), ("Y", 0)], 60);
                let (w_, x, y) = (sc.idx("W"), sc.idx("X"), sc.idx("Y"));
                sc.advance(1);
                sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
                sc.pull("Y", "W");
                sc.advance(2);
                if kind == Kind::Loss {
                    // the record's origin is Y: Y drops the name (X lacks this entry of Y's), then W, which has not heard of the
                    // drop, writes; the write is late at Y, which mints its record
                    sc.ms[y].local_ldrop(&mut sc.w).unwrap();
                    sc.advance(3);
                    let d = sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Update).unwrap();
                    sc.window("Y", "W", &d);
                } else {
                    sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Update).unwrap(); // the entry of W's that X lacks
                }
                sc.advance(4);
                let job = sc.ms[w_].start_out(&mut sc.w, vec![1], JobKind::CrashAbandon, 0);
                if kind == Kind::Abandon {
                    sc.ms[w_].abandon_stage(&mut sc.w, &job.stage);
                }
                let e =
                    sc.w.all
                        .iter()
                        .rev()
                        .find(|e| e.kind == kind && (kind == Kind::Loss || e.phys == job.stage))
                        .unwrap_or_else(|| panic!("no {kind:?} entry"))
                        .clone();
                if in_position {
                    let src = if e.origin == w_ { "W" } else { "Y" };
                    sc.pull("X", src);
                } else {
                    sc.take("X", &e); // ahead of its position: X lacks an earlier entry of the origin
                }
                let named = sc.ms[x].inv.contains("q6_kind_out_of_position");
                assert_eq!(
                    named, !in_position,
                    "{kind:?} in position {in_position}: named {named}"
                );
            }
        }
        let mut cfgs = crate::run::grid(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5]);
        for c in cfgs.iter_mut() {
            *c = crate::run::product_cfg(
                crate::world::Policy::Document,
                (c.members, c.outs, c.keys),
                true,
                false,
                0.3,
            );
        }
        let s = crate::run::run_grid("m-doc-hold-q6-kinds", "v6", &cfgs, 0, 10, crate::threads());
        eprintln!(
            "m-doc-hold-q6-kinds: inv_q6_kind_out_of_position in {} of {} runs",
            s.get("inv_q6_kind_out_of_position"),
            s.runs
        );
        assert!(s.get("inv_q6_kind_out_of_position") > 0);
    }

    /// H-Q6-L1: a Definitions (or count) repair is not re-planned after its mark is released. EMULATION: a repair ledger per member
    /// (`plan_definition_repairs`, retired on release); the product's `repair_ledger.rs` and `peers.rs` are not modelled. Mutant
    /// `m-repair-replanned`.
    fn l1(mode: &str) -> (usize, usize) {
        let mut sc = Sc::new(mode, &[("W", 0), ("X", 0)], 60);
        sc.w.held_uncovered = true;
        let (w_, x) = (sc.idx("W"), sc.idx("X"));
        sc.ms[x].f_replanned = sc.w.f.fm_repair_replanned;
        sc.advance(1);
        sc.ms[w_].local_doc(&mut sc.w, 0, DocOp::Insert).unwrap();
        sc.advance(2);
        let d2 = sc.ms[w_].local_doc(&mut sc.w, 1, DocOp::Insert).unwrap();
        sc.ms[x].take_hold(&mut sc.w, &d2);
        let first = sc.ms[x].plan_definition_repairs();
        sc.pull("X", "W"); // the window releases the mark
        assert!(sc.ms[x].held_oop.is_empty());
        (first, sc.ms[x].plan_definition_repairs())
    }

    #[test]
    fn h_q6_l1_a_repair_is_not_replanned_after_its_mark_is_released() {
        assert_eq!(l1("v6"), (1, 0));
        let m = l1("m-repair-replanned");
        eprintln!("m-repair-replanned on L1: planned {m:?}");
        assert!(m.1 > 0, "the mutant re-plans the released mark's repair");
    }
}
