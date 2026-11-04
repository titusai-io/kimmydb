//! A one-time equivalence test for the D7 performance pass.
//!
//! This pins the equivalence of the D7 performance pass (`8e36e8de` against `a834ae2c`) on shapes
//! that no `Writer` builds: the fingerprints, the differentials, the sim and the join-law tests
//! only ever see states that honest writers produce, so none of them would notice a change of
//! result on a state assembled through the public fields of `MergeState`.
//!
//! The module `frozen` holds verbatim copies of the `8e36e8de` `join` and of the read side
//! (`winner_in`, `counted_in`, `hidden_in`, `hidden_all`, `cell_in`, `excess`, `over_caps`,
//! `visible` and the tree they build). Each test runs the live code and the frozen copy on the
//! same inputs and asserts that the results are identical: the pair `(state, fresh)` of every
//! join (the state also compared field by field, mutant memory included, as its `Debug` text),
//! and every read of every resulting state: `visible`, `hidden_all`, `excess`, `over_caps`,
//! `alive`, the per-path reads, both encodings (version 1 and the compact version 2) and both
//! hashes (`state_hash`, `compact_hash`).
//!
//! The inputs are:
//!
//! * the pools of the join-law tests (flat, nested and typed counters, at the retentions 1e9, 40,
//!   10 and 4): every second ordered pair of states of a run (every ordered pair, in the full
//!   `--ignored` sweep), a sample of pairs across runs, and sampled triples (both groupings, every
//!   step compared);
//! * the states the decode fuzz accepts, joined with themselves and with pool states;
//! * hand-built states (`hand_built`), in the shapes that the public fields allow and no writer
//!   makes. The tests count the shapes they really covered and fail when one is missing.
//!
//! ORDER ENTRIES CLEARED (restored after the O1 review, O1-9). This file was deleted when the order
//! state joined the join; the join's change was purely additive (the `t.ords` loop and the `ords`
//! retain, no line of the cell join changed), so the frozen copies of `8e36e8de` still pin the cells,
//! and the guard is needed while the writer's feed (O2, now built), the probes (O3) and the implicit
//! marker (O4) change the code around the join. Every input state has its `ords` cleared before the live code and
//! the frozen copy see it, so what is compared is the join and the reads of the cells, as before; the
//! order state has its own tests (`ord_tests`, `ord_codec_tests`).
//!
//! This test RETIRES deliberately (the file is deleted, with its line in `mod.rs`) at the counter cell,
//! the next change of join semantics (the absorbed rule and the new slot layout): from then on the
//! frozen copies describe code that is meant to differ. The five full sweeps stay `#[ignore]`d.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use super::hand_built::{self, Mode};
use super::prop_tests::{Rng, fuzz_corpus, fuzz_mutate, nested_pools, opts, pools, run_counters};
use super::*;

/// The retentions of the pools: far beyond any clock, and three that the pooled runs outrun.
const RETENTIONS: [u64; 4] = [1_000_000_000, 40, 10, 4];

/// What a test covered, printed at its end.
#[derive(Default)]
struct Tally {
    /// Joins compared.
    joins: u64,
    /// Joins whose states were also compared as `Debug` text and whose results were read.
    deep: u64,
    /// States read, live against frozen, with all their reads.
    reads: u64,
    /// Triples compared (four joins each).
    triples: u64,
    /// Joins whose result holds a loss record, by kind.
    records: BTreeMap<String, u64>,
    /// Joins whose `fresh` set is not empty.
    fresh: u64,
    /// Hand-built cases by builder and by shape found in them.
    builders: BTreeMap<&'static str, u64>,
    shapes: BTreeMap<&'static str, u64>,
    /// Joins by integer bound.
    bounds: BTreeMap<i64, u64>,
}

impl Tally {
    fn report(&self, what: &str) {
        eprintln!(
            "{what}: {} joins ({} deep, {} triples), {} states read, {} with fresh records; \
             records {:?}; bounds {:?}",
            self.joins, self.deep, self.triples, self.reads, self.fresh, self.records, self.bounds
        );
    }
}

/// Run `f`, mapping a panic to `Err(())`: the encoders are not changed by the pass, and a state
/// built by hand may be one they refuse by panicking; the two sides must then agree.
fn guard<T>(f: impl FnOnce() -> T) -> Result<T, ()> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|_| ())
}

/// Every read of a state, as comparable values (a value as its `Debug` text, which tells
/// `0.0` from `-0.0`).
#[derive(PartialEq, Debug)]
struct Reads {
    alive: Vec<MStamp>,
    visible: String,
    hidden_all: BTreeMap<Path, i128>,
    excess: BTreeMap<Path, Excess>,
    over_caps: OverCaps,
    per_path: Vec<String>,
    v1: Result<Option<Vec<u8>>, ()>,
    compact: Result<Vec<u8>, ()>,
    encode_compact: Result<Vec<u8>, ()>,
    state_hash: Result<Option<[u8; 16]>, ()>,
    compact_hash: Result<[u8; 16], ()>,
}

/// The paths a state has a cell at, and two it has not.
fn probe_paths(st: &MergeState) -> BTreeSet<Path> {
    let mut ps: BTreeSet<Path> = st.regs.keys().map(|k| k.1.clone()).collect();
    ps.extend(st.slots.keys().map(|k| k.1.clone()));
    ps.insert(Path::flat("zz").unwrap());
    ps.insert(Path::new(["zz", "a"]).unwrap());
    ps
}

macro_rules! encodings {
    ($st:expr) => {
        (
            guard(|| $st.try_encode().ok()),
            guard(|| $st.compact_bytes()),
            guard(|| $st.encode_compact()),
            guard(|| $st.v1_can_hold().then(|| $st.state_hash())),
            guard(|| $st.compact_hash()),
        )
    };
}

fn reads_live(st: &MergeState, o: &JoinOptions) -> Reads {
    let per_path = probe_paths(st)
        .iter()
        .map(|p| {
            format!(
                "{p:?} {:?} {:?} {:?} {} {:?} {:?}",
                st.winner(p, o),
                st.gen_of(p, o),
                st.counted(p, o),
                st.hidden(p, o),
                st.contributors(p, o),
                st.value(p, o)
            )
        })
        .collect();
    let (v1, compact, encode_compact, state_hash, compact_hash) = encodings!(st);
    Reads {
        alive: st.alive(o),
        visible: format!("{:?}", st.visible(o)),
        hidden_all: st.hidden_all(o),
        excess: st.excess(o),
        over_caps: st.over_caps(o),
        per_path,
        v1,
        compact,
        encode_compact,
        state_hash,
        compact_hash,
    }
}

fn reads_frozen(st: &MergeState, o: &JoinOptions) -> Reads {
    let per_path = probe_paths(st)
        .iter()
        .map(|p| {
            format!(
                "{p:?} {:?} {:?} {:?} {} {:?} {:?}",
                st.frozen_winner(p, o),
                st.frozen_gen_of(p, o),
                st.frozen_counted(p, o),
                st.frozen_hidden(p, o),
                st.frozen_contributors(p, o),
                st.frozen_value(p, o)
            )
        })
        .collect();
    let (v1, compact, encode_compact, state_hash, compact_hash) = encodings!(st);
    Reads {
        // `alive` was not touched by the pass: `alive_set` is the same function
        alive: st.alive(o),
        visible: format!("{:?}", st.frozen_visible(o)),
        hidden_all: st.frozen_hidden_all(o),
        excess: st.frozen_excess(o),
        over_caps: st.frozen_over_caps(o),
        per_path,
        v1,
        compact,
        encode_compact,
        state_hash,
        compact_hash,
    }
}

fn describe(o: &JoinOptions) -> String {
    format!("{o:?}")
}

/// A difference between the live code and the frozen copy: the case, in a form that can be
/// reproduced (the inputs as `Debug` text, the options), and both outputs.
#[track_caller]
fn differ(
    what: &str,
    ctx: &str,
    inputs: &[&MergeState],
    o: &JoinOptions,
    live: &str,
    frozen: &str,
) -> ! {
    let mut msg = format!("D7 EQUIVALENCE BROKEN: {what}\ncase: {ctx}\noptions: {}\n", describe(o));
    for (k, st) in inputs.iter().enumerate() {
        msg += &format!("input {k}: {st:?}\n");
    }
    panic!("{msg}live:   {live}\nfrozen: {frozen}");
}

/// The state without its order state: the frozen copies know none.
fn strip(s: &MergeState) -> Cow<'_, MergeState> {
    if s.ords.is_empty() {
        Cow::Borrowed(s)
    } else {
        let mut x = s.clone();
        x.ords.clear();
        Cow::Owned(x)
    }
}

/// The reads of `live` (by the live code) and of `frozen` (by the frozen copy) must be equal.
#[track_caller]
fn same_reads(live: &MergeState, frozen: &MergeState, o: &JoinOptions, ctx: &str, t: &mut Tally) {
    let (live, frozen) = (strip(live), strip(frozen));
    let (live, frozen) = (&*live, &*frozen);
    let (a, b) = (reads_live(live, o), reads_frozen(frozen, o));
    if a != b {
        differ("reads", ctx, &[live, frozen], o, &format!("{a:?}"), &format!("{b:?}"));
    }
    t.reads += 1;
}

fn same_state(x: &MergeState, y: &MergeState, deep: bool) -> bool {
    x == y && x.gens == y.gens && (!deep || format!("{x:?}") == format!("{y:?}"))
}

/// Join `a` and `b` with the live code and with the frozen copy and require the same
/// `(state, fresh)`; when `deep`, also the same `Debug` text of the state and the same reads of
/// the result. Returns the live result.
#[track_caller]
fn join_both(
    a: &MergeState,
    b: &MergeState,
    o: &JoinOptions,
    deep: bool,
    ctx: &dyn Fn() -> String,
    t: &mut Tally,
) -> (MergeState, BTreeSet<LossKey>) {
    let (a, b) = (strip(a), strip(b));
    let (a, b) = (&*a, &*b);
    let (ls, lf) = join(a, b, o);
    let (fs, ff) = frozen::join(a, b, o);
    if !same_state(&ls, &fs, deep) || lf != ff {
        differ(
            "join",
            &ctx(),
            &[a, b],
            o,
            &format!("{ls:?} fresh {lf:?}"),
            &format!("{fs:?} fresh {ff:?}"),
        );
    }
    t.joins += 1;
    *t.bounds.entry(o.int_bound()).or_default() += 1;
    t.fresh += u64::from(!lf.is_empty());
    for k in ls.losses.keys().map(|k| format!("{:?}", k.kind)).collect::<BTreeSet<_>>() {
        *t.records.entry(k).or_default() += 1;
    }
    if deep {
        t.deep += 1;
        same_reads(&ls, &fs, o, &ctx(), t);
    }
    (ls, lf)
}

/// The options variants the reads and hand-built joins are run under: the bounds, and each
/// switch that a read or the join consults.
fn variants(ret: u64) -> Vec<JoinOptions> {
    let mut v = vec![opts(ret)];
    for bound in [1000, 0, 1] {
        v.push(JoinOptions::new(ret, bound).unwrap());
    }
    let switches: [fn(&mut JoinOptions); 9] = [
        |o| o.set_horizon_first(false),
        |o| o.set_supersede(false),
        |o| o.set_cells_reported(false),
        |o| o.set_suppress_soi(false),
        |o| o.set_v3_generation(true),
        |o| o.set_array_clash_as_clash(true),
        |o| o.set_no_type_clash(true),
        |o| o.set_marker_hides(true),
        |o| o.set_type_ignores_recency(true),
    ];
    for f in switches {
        let mut o = opts(ret);
        f(&mut o);
        v.push(o);
    }
    for f in [
        (|o: &mut JoinOptions| o.set_sum_in_stamp_order(true)) as fn(&mut JoinOptions),
        |o| o.set_no_saturation(true),
        |o| o.set_int32_saturates(true),
    ] {
        let mut o = JoinOptions::new(ret, 1000).unwrap();
        f(&mut o);
        v.push(o);
    }
    v
}

// ---- the pools of the join-law tests ---------------------------------------------------------

/// How much of the pools to sweep.
struct Size {
    seeds: u64,
    steps: usize,
    /// Every `stride`th ordered pair of a run is joined (1: all of them).
    stride: usize,
    /// Every `deep_every`th join is compared deeply and its result read.
    deep_every: u64,
    /// Pairs across two runs.
    cross: usize,
    /// Triples of a run.
    triples: usize,
}

/// What the default run does: finishes in well under a minute.
const DEFAULT: Size =
    Size { seeds: 12, steps: 24, stride: 2, deep_every: 4, cross: 600, triples: 400 };
/// The full sweep, run once by hand (`--ignored`).
const FULL: Size =
    Size { seeds: 60, steps: 30, stride: 1, deep_every: 2, cross: 60_000, triples: 60_000 };

fn sweep_pools(family: &str, sz: &Size) {
    let mut t = Tally::default();
    for ret in RETENTIONS {
        let o = opts(ret);
        let ps: Vec<Vec<MergeState>> = match family {
            "flat" => pools(sz.seeds, sz.steps, &o),
            "nested" => nested_pools(sz.seeds, sz.steps, &o),
            _ => (1..=sz.seeds).map(|s| run_counters(s, sz.steps, &o)).collect(),
        };
        let vs = variants(ret);
        let empty = MergeState::default();
        let mut rng = Rng(0x00D7_0000_0000_0000 ^ ret);
        let mut n = 0usize;
        for (pi, pool) in ps.iter().enumerate() {
            for (i, a) in pool.iter().enumerate() {
                let ctx = || format!("{family} R={ret} pool {pi}: state {i} read");
                same_reads(a, a, &o, &ctx(), &mut t);
                // the other bounds and switches, on every sixth state
                if i % 6 == 0 {
                    for v in vs.iter().skip(1) {
                        same_reads(a, a, v, &format!("{} under {v:?}", ctx()), &mut t);
                    }
                }
                let ctx = || format!("{family} R={ret} pool {pi}: state {i} with the empty state");
                join_both(a, &empty, &o, true, &ctx, &mut t);
                join_both(&empty, a, &o, true, &ctx, &mut t);
                for (j, b) in pool.iter().enumerate() {
                    n += 1;
                    if !n.is_multiple_of(sz.stride) {
                        continue;
                    }
                    let deep = t.joins % sz.deep_every == 0;
                    let ctx = || format!("{family} R={ret} pool {pi}: join of states {i} and {j}");
                    join_both(a, b, &o, deep, &ctx, &mut t);
                    // and under one of the other bounds and switches
                    if n.is_multiple_of(7) {
                        let v = &vs[(n / 7) % vs.len()];
                        join_both(a, b, v, true, &ctx, &mut t);
                    }
                }
            }
        }
        for k in 0..sz.cross {
            let (p, q) = (&ps[rng.below(ps.len())], &ps[rng.below(ps.len())]);
            let (a, b) = (&p[rng.below(p.len())], &q[rng.below(q.len())]);
            let ctx = || format!("{family} R={ret}: cross pair {k} of seed {}", 0x00D7 ^ ret);
            join_both(a, b, &o, k % 4 == 0, &ctx, &mut t);
        }
        for k in 0..sz.triples {
            let pool = &ps[rng.below(ps.len())];
            let (ia, ib, ic) =
                (rng.below(pool.len()), rng.below(pool.len()), rng.below(pool.len()));
            let (a, b, c) = (&pool[ia], &pool[ib], &pool[ic]);
            let ctx = || format!("{family} R={ret}: triple {k} ({ia}, {ib}, {ic}) of a run");
            let deep = k % 4 == 0;
            let ab = join_both(a, b, &o, deep, &ctx, &mut t).0;
            join_both(&ab, c, &o, deep, &ctx, &mut t);
            let bc = join_both(b, c, &o, deep, &ctx, &mut t).0;
            join_both(a, &bc, &o, deep, &ctx, &mut t);
            t.triples += 1;
        }
    }
    t.report(&format!("D7 equivalence, {family} pools"));
    assert!(t.joins > 1000 && t.reads > 1000 && t.fresh > 0, "the sweep must exercise the code");
    // the typed counters make no whole-document insertLost; the others all three
    let kinds: &[&str] = if family == "counters" {
        &["Clash", "DeleteWon"]
    } else {
        &["Clash", "DeleteWon", "InsertLost"]
    };
    assert!(kinds.iter().all(|k| t.records.contains_key(*k)), "{:?}", t.records);
    assert!(t.bounds.len() > 1, "the bound variants must be joined under: {:?}", t.bounds);
}

#[test]
fn live_and_frozen_agree_on_the_flat_pools() {
    sweep_pools("flat", &DEFAULT);
}

#[test]
fn live_and_frozen_agree_on_the_nested_pools() {
    sweep_pools("nested", &DEFAULT);
}

#[test]
fn live_and_frozen_agree_on_the_typed_counter_pools() {
    sweep_pools("counters", &DEFAULT);
}

/// The full sweep: every ordered pair of every run of 60 seeds at the four retentions, and tens
/// of thousands of cross pairs and triples. Run by hand once, `cargo nextest run --run-ignored
/// only d7_equivalence`.
#[test]
#[ignore = "the full sweep; run once by hand"]
fn live_and_frozen_agree_on_the_full_pools_flat() {
    sweep_pools("flat", &FULL);
}

#[test]
#[ignore = "the full sweep; run once by hand"]
fn live_and_frozen_agree_on_the_full_pools_nested() {
    sweep_pools("nested", &FULL);
}

#[test]
#[ignore = "the full sweep; run once by hand"]
fn live_and_frozen_agree_on_the_full_pools_counters() {
    sweep_pools("counters", &FULL);
}

// ---- the states the decode fuzz accepts ------------------------------------------------------

#[test]
fn live_and_frozen_agree_on_the_states_the_decode_fuzz_accepts() {
    fuzz_sweep(15_000, 4);
}

#[test]
#[ignore = "the full sweep; run once by hand"]
fn live_and_frozen_agree_on_the_states_the_decode_fuzz_accepts_full() {
    fuzz_sweep(150_000, 10);
}

fn fuzz_sweep(inputs: u64, partners: usize) {
    let mut t = Tally::default();
    let o1 = opts(1_000_000_000);
    let corpus = fuzz_corpus(&o1);
    let mut rng = Rng(0x00C0_FFEE_D00D_5EED);
    let mut accepted = Vec::new();
    for _ in 0..inputs {
        let b = fuzz_mutate(&mut rng, &corpus);
        if let Ok(st) = MergeState::decode(&b) {
            accepted.push(st);
        }
    }
    // distinct states, and not merely the corpus back
    let distinct: BTreeSet<Vec<u8>> = accepted.iter().map(MergeState::encode).collect();
    let corpus_set: BTreeSet<&Vec<u8>> = corpus.iter().collect();
    let novel = distinct.iter().filter(|b| !corpus_set.contains(b)).count();
    let empty = MergeState::default();
    for ret in [1_000_000_000, 10] {
        let o = opts(ret);
        let partner_pool: Vec<MergeState> = pools(6, 24, &o).into_iter().flatten().collect();
        for (k, st) in accepted.iter().enumerate() {
            let ctx = || format!("fuzz-accepted state {k}, R={ret}");
            same_reads(st, st, &o, &ctx(), &mut t);
            join_both(st, st, &o, true, &ctx, &mut t);
            join_both(st, &empty, &o, true, &ctx, &mut t);
            for _ in 0..partners {
                let q = &partner_pool[rng.below(partner_pool.len())];
                let ctx = || format!("fuzz-accepted state {k} with a pool state, R={ret}");
                join_both(st, q, &o, true, &ctx, &mut t);
                join_both(q, st, &o, true, &ctx, &mut t);
            }
        }
    }
    t.report("D7 equivalence, decode-fuzz states");
    eprintln!(
        "  {inputs} fuzz inputs, {} accepted, {} distinct, {novel} not in the corpus",
        accepted.len(),
        distinct.len()
    );
    assert!(accepted.len() > 100 && novel > 20, "the fuzz must give states beyond the corpus");
}

// ---- hand-built states -----------------------------------------------------------------------

/// The cases per targeted builder, and of the wild one.
fn hand_built_sweep(targeted: usize, wild: usize, extra: usize, seed: u64) {
    let mut t = Tally::default();
    let mut rng = Rng(seed);
    let empty = MergeState::default();
    for (b, name) in hand_built::BUILDERS.iter().enumerate() {
        let n = if b == hand_built::BUILDERS.len() - 1 { wild } else { targeted };
        for k in 0..n {
            let ret = RETENTIONS[rng.below(RETENTIONS.len())];
            let c = hand_built::case(&mut rng, Mode::Any, b, ret);
            let c2 = hand_built::case(&mut rng, Mode::Any, b, ret);
            *t.builders.entry(name).or_default() += 1;
            for s in hand_built::shapes_of(&c.s, &c.t, ret) {
                *t.shapes.entry(s).or_default() += 1;
            }
            // the default options, and `extra` of the variants
            let all = variants(ret);
            let mut os = vec![&all[0]];
            for _ in 0..extra {
                os.push(&all[rng.below(all.len())]);
            }
            for o in os {
                let ctx = || format!("hand-built builder {b} ({name}), case {k}, R={ret}");
                let (s, u) = (&c.s, &c.t);
                for (x, y) in [(s, u), (u, s), (s, s), (u, u), (s, &empty), (&empty, u), (&c2.s, u)]
                {
                    join_both(x, y, o, true, &ctx, &mut t);
                }
                let su = join_both(s, u, o, true, &ctx, &mut t).0;
                join_both(&su, &c2.s, o, true, &ctx, &mut t);
                let uc = join_both(u, &c2.s, o, true, &ctx, &mut t).0;
                join_both(s, &uc, o, true, &ctx, &mut t);
                t.triples += 1;
                same_reads(s, s, o, &ctx(), &mut t);
                same_reads(u, u, o, &ctx(), &mut t);
            }
        }
    }
    t.report("D7 equivalence, hand-built states");
    eprintln!("  cases by builder: {:?}", t.builders);
    for s in hand_built::SHAPES {
        eprintln!("  shape found in {:>6} cases: {s}", t.shapes.get(s).copied().unwrap_or(0));
    }
    for s in hand_built::SHAPES {
        assert!(t.shapes.get(s).copied().unwrap_or(0) >= 20, "the shape `{s}` is missing");
    }
    for (name, n) in &t.builders {
        assert!(*n >= targeted as u64, "builder {name}");
    }
    for k in ["Clash", "ArrayClash", "DeleteWon", "InsertLost", "TypeClash"] {
        assert!(t.records.get(k).copied().unwrap_or(0) > 100, "no {k} records: {:?}", t.records);
    }
    for bound in [0, 1000, i64::MAX] {
        assert!(t.bounds.get(&bound).copied().unwrap_or(0) > 100, "bound {bound}: {:?}", t.bounds);
    }
}

// Four runs of the same sweep from different seeds, so that they run side by side.
#[test]
fn live_and_frozen_agree_on_hand_built_states_1() {
    hand_built_sweep(80, 500, 1, 0x00D7_1234_5678_9AB1);
}

#[test]
fn live_and_frozen_agree_on_hand_built_states_2() {
    hand_built_sweep(80, 500, 1, 0x00D7_1234_5678_9AB2);
}

#[test]
fn live_and_frozen_agree_on_hand_built_states_3() {
    hand_built_sweep(80, 500, 1, 0x00D7_1234_5678_9AB3);
}

#[test]
fn live_and_frozen_agree_on_hand_built_states_4() {
    hand_built_sweep(80, 500, 1, 0x00D7_1234_5678_9AB4);
}

#[test]
#[ignore = "the full sweep; run once by hand"]
fn live_and_frozen_agree_on_hand_built_states_full() {
    hand_built_sweep(1500, 20_000, 2, 0x00D7_FEED_FACE_CAFE);
}

/// The two kinds of state `decode` refuses (a dead life that `lives` omits, a vector that does not
/// cover the stamps) are the two the join is not commutative or idempotent on: the live join and
/// the frozen one give the same two different results, so the defect is the join's from before
/// the pass, and not the pass's (`prop_tests::the_join_is_not_commutative_or_idempotent_where_decode_refuses`
/// keeps why they are refused).
#[test]
fn the_join_defect_decode_keeps_out_is_the_same_before_the_pass() {
    use super::tests::{dead_but_unlisted, vv_does_not_cover};
    let o = opts(1_000_000_000);
    let mut t = Tally::default();
    let (a, b) = dead_but_unlisted();
    let ctx = || "a dead life that lives omits".to_string();
    let ab = join_both(&a, &b, &o, true, &ctx, &mut t).0;
    let ba = join_both(&b, &a, &o, true, &ctx, &mut t).0;
    assert!(ab != ba, "live: not commutative");
    assert!(frozen::join(&a, &b, &o).0 != frozen::join(&b, &a, &o).0, "frozen: not commutative");
    let n = join(&vv_does_not_cover(), &MergeState::default(), &o).0;
    let ctx = || "a vector that does not cover its stamps".to_string();
    let nn = join_both(&n, &n, &o, true, &ctx, &mut t).0;
    assert!(nn != n, "live: not idempotent");
    assert!(frozen::join(&n, &n, &o).0 != n, "frozen: not idempotent");
}

/// Verbatim copies of the `8e36e8de` code, from `git show 8e36e8de:crates/kimmy-core/src/merge_state/<file>`:
/// `join.rs` lines 231 to 459 (`prefilter` and `join`) and `state.rs` lines 257 to 658 (the read
/// side of `impl MergeState`, then `Node`, `Shape` and `Tree`).
///
/// Adapted, and only so far as needed to compile next to the live code:
///
/// * the `use` lines (paths into the parent modules);
/// * every method of the read side is renamed with a `frozen_` prefix (`winner_in` is
///   `frozen_winner_in`, and every call to one likewise), because the live `impl MergeState`
///   already has methods of the old names, and an inherent method of the live name would win
///   over the frozen one at a call site;
/// * `pub fn` and `pub(crate) fn` are `pub(super) fn`, so that the tests above reach them;
/// * the copies were then formatted by rustfmt (line breaks only).
///
/// Used from the live code, because the pass did not touch them: `alive_set`, `max_vv`,
/// `horizon`, `writers`, `cov`, `sub_ret`, the types, `JoinOptions` and its switches, and
/// `Reg::clone` (which the pass wrote by hand, field for field, as `derive(Clone)` did).
mod frozen {
    use std::borrow::Cow;
    use std::collections::{BTreeMap, BTreeSet};

    use super::super::counter::Num;
    use super::super::join::JoinOptions;
    use super::super::state::{
        Excess, Gen, LifeKind, MAX_CONTRIBUTORS, MAX_WRITERS, MergeState, ObsKey, OverCaps, Reg,
        Slot, cov, sub_ret,
    };
    use super::super::types::{LossKey, LossKind, LossValue, MStamp, Path, Value, WriterId};
    use crate::hlc::Hlc;

    /// The model's `_prefilter`: drop deletes and dead lives older than `h`.
    fn prefilter(st: &MergeState, h: Hlc) -> MergeState {
        let mut st = st.clone();
        st.deletes.retain(|d, _| d.hlc >= h);
        let gone: Vec<MStamp> = st.dead.iter().filter(|l| l.hlc < h).copied().collect();
        for l in gone {
            st.dead.remove(&l);
            st.lives.remove(&l);
        }
        st
    }

    /// Join `t` into `s`. Returns the new state and the keys of the loss records
    /// that are new relative to `s` (the model's `fresh`).
    ///
    /// V3M1: the horizon of the JOINED version vectors is applied to both sides
    /// BEFORE the union and before aliveness.
    pub(super) fn join(
        s: &MergeState,
        t: &MergeState,
        o: &JoinOptions,
    ) -> (MergeState, BTreeSet<LossKey>) {
        let ret = o.retention;
        let h0 = sub_ret(s.max_vv().max(t.max_vv()), ret);
        let (s, t) = if o.horizon_first() {
            (prefilter(s, h0), prefilter(t, h0))
        } else {
            (s.clone(), t.clone())
        };
        let mut out = s.clone();

        // Union. `ok` is evaluated against the dead set of the moment, as in the model.
        for (l, v) in &t.lives {
            if !out.dead.contains(l) {
                out.lives.entry(*l).or_insert_with(|| v.clone());
            }
        }
        out.dead.extend(t.dead.iter().copied());
        for (d, vv) in &t.deletes {
            out.deletes.entry(*d).or_insert_with(|| vv.clone());
        }
        for (k, r) in &t.regs {
            if !out.dead.contains(&k.0) && out.regs.get(k).is_none_or(|x| r.stamp > x.stamp) {
                out.regs.insert(k.clone(), r.clone());
            }
        }
        for (k, v) in &t.slots {
            if !out.dead.contains(&k.0) && out.slots.get(k).is_none_or(|x| v.stamp > x.stamp) {
                out.slots.insert(k.clone(), v.clone());
            }
        }
        for (k, g) in &t.gens {
            if !out.dead.contains(&k.0) && out.gens.get(k).is_none_or(|x| g.0 > x.0) {
                out.gens.insert(k.clone(), g.clone());
            }
        }
        for (w, h) in &t.vv {
            let e = out.vv.entry(*w).or_insert(Hlc::ZERO);
            *e = (*e).max(*h);
        }

        let mut new: BTreeMap<LossKey, LossValue> = BTreeMap::new();
        let key =
            |kind: LossKind, path: &Path, stamp: MStamp| LossKey::field(kind, path.clone(), stamp);

        // Q2: concurrent FIRST inserts of one _id (explicit inserts whose creator had
        // seen no delete): the newest whole document wins, the others are superseded
        // and reported. Re-inserts after a delete and upserts still merge.
        let first: Vec<MStamp> = out
            .lives
            .iter()
            .filter(|(_, v)| v.kind == LifeKind::Insert && v.seen.is_empty())
            .map(|(l, _)| *l)
            .collect();
        let newest_first = first.iter().max().copied();
        let superseded: BTreeSet<MStamp> =
            first.iter().copied().filter(|l| Some(*l) != newest_first).collect();
        let reported: BTreeSet<MStamp>;
        if o.supersede() {
            out.dead.extend(superseded.iter().copied());
            // V6m1: a first insert that a delete already killed is dead, not lost to a newer insert.
            let killed: BTreeSet<MStamp> = superseded
                .iter()
                .copied()
                .filter(|l| {
                    out.deletes
                        .keys()
                        .any(|d| !out.lives[l].seen.contains(d) && d.hlc >= sub_ret(l.hlc, ret))
                })
                .collect();
            reported = superseded.difference(&killed).copied().collect();
            for l in &reported {
                new.insert(
                    LossKey::document(LossKind::InsertLost, *l),
                    LossValue::Fields(out.lives[l].order.clone()),
                );
            }
        } else {
            reported = BTreeSet::new();
        }
        let al = out.alive_set(ret);
        let not_alive: Vec<MStamp> =
            out.lives.keys().filter(|l| !al.contains(l)).copied().collect();
        out.dead.extend(not_alive);

        // (1) same-path register clashes: a register of s against a register of t,
        // concurrent by the version vectors. Two arrays are an `arrayClash`.
        let paths: BTreeSet<&Path> = s.regs.keys().chain(t.regs.keys()).map(|k| &k.1).collect();
        for p in paths {
            let rs = s.regs.iter().filter(|(k, _)| &k.1 == p && al.contains(&k.0)).map(|(_, r)| r);
            let rt: Vec<_> = t
                .regs
                .iter()
                .filter(|(k, _)| &k.1 == p && al.contains(&k.0))
                .map(|(_, r)| r)
                .collect();
            for a in rs {
                for b in &rt {
                    if a.stamp != b.stamp && !cov(&t.vv, &a.stamp) && !cov(&s.vv, &b.stamp) {
                        let lo = if a.stamp < b.stamp { a } else { b };
                        let arrays =
                            matches!((&a.value, &b.value), (Value::Array(_), Value::Array(_)));
                        let kind = if arrays && !o.array_clash_as_clash() {
                            LossKind::ArrayClash
                        } else {
                            LossKind::Clash
                        };
                        new.insert(key(kind, p, lo.stamp), LossValue::Value(lo.value.clone()));
                    }
                }
            }
        }

        // (1b) typeClash: a scalar or array register at p against a cell below p (a register with
        // a plain value, or a counter slot), concurrent by the version vectors. The loser is the
        // side the type rule hides: the older stamp (a newer register at p hides the cell below; a
        // newer cell below makes p a map, and the register at p loses). Any depth below counts.
        if !o.no_type_clash() {
            for (a_st, b_st) in [(&s, &t), (&t, &s)] {
                // b's cells in path order, so that those below a path are one run.
                let mut cells: Vec<(&Path, MStamp, LossValue)> = b_st
                    .regs
                    .iter()
                    .filter(|(k, r)| al.contains(&k.0) && r.value.is_plain())
                    .map(|(k, r)| (&k.1, r.stamp, LossValue::Value(r.value.clone())))
                    .chain(
                        b_st.slots
                            .iter()
                            .filter(|(k, _)| al.contains(&k.0))
                            .map(|(k, v)| (&k.1, v.stamp, LossValue::Total(v.total))),
                    )
                    .collect();
                cells.sort_by(|x, y| x.0.cmp(y.0));
                for ((l, p), r) in &a_st.regs {
                    if !al.contains(l) || !r.value.is_plain() || cov(&b_st.vv, &r.stamp) {
                        continue;
                    }
                    let start = cells.partition_point(|c| c.0 <= p);
                    for (q, qst, qv) in cells[start..].iter().take_while(|c| c.0.is_below(p)) {
                        if *qst != r.stamp && !cov(&a_st.vv, qst) {
                            if r.stamp > *qst {
                                new.insert(key(LossKind::TypeClash, q, *qst), qv.clone());
                            } else {
                                new.insert(
                                    key(LossKind::TypeClash, p, r.stamp),
                                    LossValue::Value(r.value.clone()),
                                );
                            }
                        }
                    }
                }
            }
        }

        // (2) a cell in a dead life that no known delete saw.
        let seen_by_delete = |st: &MStamp| out.deletes.values().any(|vv| cov(vv, st));
        for src in [&s, &t] {
            for (k, r) in &src.regs {
                if o.cells_reported() && reported.contains(&k.0) && r.stamp != k.0 {
                    // V6M1: a write made into the losing life.
                    new.insert(
                        key(LossKind::InsertLost, &k.1, r.stamp),
                        LossValue::Value(r.value.clone()),
                    );
                } else if !al.contains(&k.0)
                    && !reported.contains(&k.0)
                    && !seen_by_delete(&r.stamp)
                {
                    new.insert(
                        key(LossKind::DeleteWon, &k.1, r.stamp),
                        LossValue::Value(r.value.clone()),
                    );
                }
            }
            for (k, v) in &src.slots {
                if o.cells_reported() && reported.contains(&k.0) {
                    new.insert(key(LossKind::InsertLost, &k.1, v.stamp), LossValue::Total(v.total));
                } else if !al.contains(&k.0)
                    && !reported.contains(&k.0)
                    && !seen_by_delete(&v.stamp)
                {
                    new.insert(key(LossKind::DeleteWon, &k.1, v.stamp), LossValue::Total(v.total));
                }
            }
        }

        // V6m2: creation cells of an upsert life dropped for an alive explicit first insert.
        if al.iter().any(|l| out.lives[l].kind == LifeKind::Insert) {
            for src in [&s, &t] {
                for (k, r) in &src.regs {
                    if r.soi
                        && al.contains(&k.0)
                        && out.lives.get(&k.0).is_some_and(|l| l.kind == LifeKind::Upsert)
                    {
                        new.insert(
                            key(LossKind::InsertLost, &k.1, r.stamp),
                            LossValue::Value(r.value.clone()),
                        );
                    }
                }
            }
        }

        // Drop cells of dead lives; filter records by the horizon.
        out.regs.retain(|k, _| al.contains(&k.0));
        out.slots.retain(|k, _| al.contains(&k.0));
        out.gens.retain(|k, _| al.contains(&k.0));
        let h = out.horizon(o);
        out.deletes.retain(|d, _| d.hlc >= h);
        let dead_old: Vec<MStamp> =
            out.lives.keys().filter(|l| l.hlc < h && out.dead.contains(l)).copied().collect();
        for l in dead_old {
            out.lives.remove(&l);
        }
        out.dead.retain(|l| l.hlc >= h);
        out.losses = BTreeMap::new();
        let all = s.losses.iter().chain(t.losses.iter()).chain(new.iter());
        for (k, v) in all {
            if k.stamp.hlc >= h {
                out.losses.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        let fresh = out.losses.keys().filter(|k| !s.losses.contains_key(k)).cloned().collect();
        (out, fresh)
    }

    impl MergeState {
        /// V6m2: the creation cells of an upsert-created life are dropped when an
        /// explicit insert is alive (any insert, re-inserts included).
        pub(super) fn frozen_suppressed(
            &self,
            life: &MStamp,
            r: &Reg,
            al: &BTreeSet<MStamp>,
            o: &JoinOptions,
        ) -> bool {
            match self.lives.get(life) {
                Some(l) if l.kind == LifeKind::Upsert && r.soi && o.suppress_soi() => {
                    al.iter().any(|a| self.lives.get(a).is_some_and(|v| v.kind == LifeKind::Insert))
                }
                _ => false,
            }
        }

        pub(super) fn frozen_winner_in(
            &self,
            p: &Path,
            al: &BTreeSet<MStamp>,
            o: &JoinOptions,
        ) -> Option<&Reg> {
            let mut best: Option<&Reg> = None;
            for ((l, q), r) in &self.regs {
                if q == p
                    && al.contains(l)
                    && !self.frozen_suppressed(l, r, al, o)
                    && best.is_none_or(|b| r.stamp > b.stamp)
                {
                    best = Some(r);
                }
            }
            best
        }

        /// The winning register of `p` among the alive lives.
        pub(super) fn frozen_winner(&self, p: &Path, o: &JoinOptions) -> Option<&Reg> {
            self.frozen_winner_in(p, &self.alive_set(o.retention), o)
        }

        /// V3M3: the generation of the winning base.
        pub(super) fn frozen_gen_of(&self, p: &Path, o: &JoinOptions) -> Option<Cow<'_, Gen>> {
            let al = self.alive_set(o.retention);
            self.frozen_gen_in(p, &al, self.frozen_winner_in(p, &al, o), o)
        }

        /// The generation `win` (the winner of `p`) belongs to. Under the V3M3
        /// mutant switch, the v3 rule instead: the newest non-numeric base among
        /// the alive lives, even when it lost.
        fn frozen_gen_in<'a>(
            &'a self,
            p: &Path,
            al: &BTreeSet<MStamp>,
            win: Option<&'a Reg>,
            o: &JoinOptions,
        ) -> Option<Cow<'a, Gen>> {
            if !o.v3_generation() {
                return win.and_then(|w| w.generation.as_ref()).map(Cow::Borrowed);
            }
            self.gens
                .iter()
                .filter(|((l, q), _)| q == p && al.contains(l))
                .map(|(_, g)| g)
                .max_by_key(|g| g.0)
                .map(|(st, vv)| Cow::Owned(Gen::start(*st, vv, BTreeMap::new())))
        }

        /// Slots of the winning base's generation and of NEWER generations (their
        /// starter lost: the increments are kept). Slots of OLDER generations are
        /// overwritten (hidden).
        pub(super) fn frozen_counted(&self, p: &Path, o: &JoinOptions) -> BTreeMap<ObsKey, &Slot> {
            let al = self.alive_set(o.retention);
            let win = self.frozen_winner_in(p, &al, o);
            self.frozen_counted_in(p, &al, win, o)
        }

        fn frozen_counted_in(
            &self,
            p: &Path,
            al: &BTreeSet<MStamp>,
            win: Option<&Reg>,
            o: &JoinOptions,
        ) -> BTreeMap<ObsKey, &Slot> {
            let g = self.frozen_gen_in(p, al, win, o).map(|g| g.stamp);
            self.slots
                .iter()
                .filter(|((l, q, _, generation), _)| {
                    q == p
                        && al.contains(l)
                        && (*generation == g
                            || (generation.is_some() && (g.is_none() || *generation > g)))
                })
                .map(|((l, _, w, generation), s)| ((*l, *w, *generation), s))
                .collect()
        }

        /// The increments the winning generation overwrote, as state derived from
        /// the current merge: the NET SUM (signed), over the slots of older
        /// generations, of the part the starter of the winning generation had not
        /// seen (slot total minus the total the starter saw).
        pub(super) fn frozen_hidden(&self, p: &Path, o: &JoinOptions) -> i128 {
            let al = self.alive_set(o.retention);
            self.frozen_hidden_in(p, &al, o)
        }

        fn frozen_hidden_in(&self, p: &Path, al: &BTreeSet<MStamp>, o: &JoinOptions) -> i128 {
            let win = self.frozen_winner_in(p, al, o);
            let Some(g) = self.frozen_gen_in(p, al, win, o) else {
                return 0;
            };
            // An integer figure: the amounts of the other types are not reported (provisional, see
            // the module docs of `counter`).
            self.slots
                .iter()
                .filter(|((l, q, _, generation), _)| {
                    q == p && al.contains(l) && generation.is_none_or(|x| x < g.stamp)
                })
                .filter_map(|((l, _, w, generation), s)| {
                    let seen = g.seen.get(&(*l, *w, *generation)).and_then(Num::int).unwrap_or(0);
                    s.total.int().map(|t| t.saturating_sub(seen))
                })
                .fold(0i128, i128::saturating_add)
        }

        /// The hidden figure of every path that has a nonzero one.
        pub(super) fn frozen_hidden_all(&self, o: &JoinOptions) -> BTreeMap<Path, i128> {
            let al = self.alive_set(o.retention);
            let paths: BTreeSet<&Path> =
                self.slots.keys().filter(|k| al.contains(&k.0)).map(|k| &k.1).collect();
            paths
                .into_iter()
                .filter_map(|p| {
                    let h = self.frozen_hidden_in(p, &al, o);
                    (h != 0).then(|| (p.clone(), h))
                })
                .collect()
        }

        /// The writers that contribute to the counter at `p`: those with a slot the read side counts
        /// (the winning base's generation and the newer ones), in the alive lives.
        pub(super) fn frozen_contributors(&self, p: &Path, o: &JoinOptions) -> BTreeSet<WriterId> {
            self.frozen_counted(p, o).keys().map(|k| k.1).collect()
        }

        /// The caps the state is past, if any. Never an error: a merge can exceed a cap that each
        /// side kept, and the state is stored whole.
        pub(super) fn frozen_over_caps(&self, o: &JoinOptions) -> OverCaps {
            let writers = self.writers().len();
            let al = self.alive_set(o.retention);
            let paths: BTreeSet<&Path> =
                self.slots.keys().filter(|k| al.contains(&k.0)).map(|k| &k.1).collect();
            OverCaps {
                writers: (writers > MAX_WRITERS).then_some(writers),
                counters: paths
                    .into_iter()
                    .filter_map(|p| {
                        let n = self.frozen_contributors(p, o).len();
                        (n > MAX_CONTRIBUTORS).then(|| (p.clone(), n))
                    })
                    .collect(),
            }
        }

        /// The visible value of one path (`None` when absent).
        pub(super) fn frozen_value(&self, p: &Path, o: &JoinOptions) -> Option<Value> {
            let al = self.alive_set(o.retention);
            self.frozen_value_in(p, &al, o)
        }

        pub(super) fn frozen_value_in(
            &self,
            p: &Path,
            al: &BTreeSet<MStamp>,
            o: &JoinOptions,
        ) -> Option<Value> {
            self.frozen_cell_in(p, al, o).map(|c| c.0)
        }

        /// The value of a path and what its saturation cut off, if anything.
        fn frozen_cell_in(
            &self,
            p: &Path,
            al: &BTreeSet<MStamp>,
            o: &JoinOptions,
        ) -> Option<(Value, Option<Excess>)> {
            let win = self.frozen_winner_in(p, al, o);
            // A base that is not a counter amount (a string, a double, a map marker, ...) is the
            // value: doubles are not counters.
            let base = match win.map(|w| &w.value) {
                Some(v) if !matches!(v, Value::Unset) => match Num::from_value(v) {
                    Some(n) => Some(n),
                    None => return Some((v.clone(), None)),
                },
                _ => None,
            };
            // What each slot gained since a base write absorbed it. B1: the field is present when
            // the slot moved since its absorbed stamp, even if its increments net to 0 (a counter
            // back at 0 is still there).
            let mut cut = None;
            let mut terms: Vec<(MStamp, ObsKey, Num)> = Vec::new();
            for (k, s) in self.frozen_counted_in(p, al, win, o) {
                let ob = s.absorbed.as_ref();
                let d = ob.map_or(s.total, |x| {
                    if o.no_decimal_saturation() {
                        s.total.sub(&x.1)
                    } else {
                        s.total.sub_sat(&x.1, &mut cut)
                    }
                });
                if !d.is_zero()
                    || (!o.value_ignores_slot_stamp() && ob.is_none_or(|x| x.0 != s.stamp))
                {
                    terms.push((s.stamp, k, d));
                }
            }
            // The canonical order (see `counter`): the base, then the slots by writer (node, epoch),
            // then life and generation.
            if o.sum_in_stamp_order() {
                terms.sort_by_key(|t| (t.0, t.1));
            } else {
                terms.sort_by_key(|t| (t.1.1, t.1.0, t.1.2));
            }
            if base.is_none() && terms.is_empty() {
                return None;
            }
            let kind = terms.iter().map(|t| t.2.kind()).chain(base.map(|b| b.kind())).max()?;
            let mut acc = base.map_or(Num::zero(kind), |b| b.to_kind(kind));
            for (_, _, d) in &terms {
                acc = if o.no_decimal_saturation() {
                    acc.add(&d.to_kind(kind))
                } else {
                    acc.add_sat(&d.to_kind(kind), &mut cut)
                };
            }
            // Q11: a merge that crosses either bound saturates: an integer at the integer bound, a
            // decimal at the largest finite value of its sign.
            let excess = match acc.int_excess(o.int_bound()) {
                0 => cut.map(|negative| Excess::Decimal128 { negative }),
                e => Some(Excess::Int(e)),
            };
            if o.no_saturation()
                && let Some(v) = acc.int()
            {
                return Some((Value::Int64(v as i64), excess));
            }
            Some((acc.into_value(o.int_bound(), o.int32_saturates()), excess))
        }

        /// What the saturation of a merged counter cut off, per path: the paths whose visible value
        /// stopped at a bound (Q11). A pure function of the state, like [`MergeState::hidden`]:
        /// nothing is stored, and no join law, hash or byte of either codec depends on it.
        pub(super) fn frozen_excess(&self, o: &JoinOptions) -> BTreeMap<Path, Excess> {
            let al = self.alive_set(o.retention);
            let paths: BTreeSet<&Path> =
                self.slots.keys().filter(|k| al.contains(&k.0)).map(|k| &k.1).collect();
            paths
                .into_iter()
                .filter_map(|p| self.frozen_cell_in(p, &al, o)?.1.map(|e| (p.clone(), e)))
                .collect()
        }

        /// The visible document, or `None` when no life is alive: every present path, in path order
        /// (a map before the paths below it), with the value of a leaf and [`Value::Map`] at a map.
        /// A document with only flat fields has no map entry, so for it this is the map of its
        /// fields.
        ///
        /// The TYPE RULE (D4), at every depth: a path is a map when a map marker is its winning
        /// register, or when it has a visible child that is newer than its own scalar or array (a
        /// path with no cell of its own is a map too, if a child is visible); otherwise it is its
        /// register's value. Recency is decided per child: a scalar or array at `p` hides only the
        /// children that are older than it, and a newer child makes `p` a map showing just the
        /// newer children. A map marker never hides a child.
        pub(super) fn frozen_visible(&self, o: &JoinOptions) -> Option<BTreeMap<Path, Value>> {
            let al = self.alive_set(o.retention);
            if al.is_empty() {
                return None;
            }
            let mut newest: BTreeMap<Path, MStamp> = BTreeMap::new();
            let mut note = |p: &Path, st: MStamp| {
                let e = newest.entry(p.clone()).or_insert(st);
                *e = (*e).max(st);
            };
            for ((l, p), r) in &self.regs {
                if al.contains(l) && !self.frozen_suppressed(l, r, &al, o) {
                    note(p, r.stamp);
                }
            }
            for ((l, p, _, _), s) in &self.slots {
                if al.contains(l) {
                    note(p, s.stamp);
                }
            }
            // every path with a cell, and every path above one
            let mut all: BTreeSet<Path> = BTreeSet::new();
            for p in newest.keys() {
                all.extend(p.ancestors());
                all.insert(p.clone());
            }
            let mut kids: BTreeMap<Path, Vec<Path>> = BTreeMap::new();
            let mut tops = Vec::new();
            for p in &all {
                match p.parent() {
                    None => tops.push(p.clone()),
                    Some(par) => kids.entry(par).or_default().push(p.clone()),
                }
            }
            let cx = Tree { st: self, al: &al, o, newest: &newest, kids: &kids };
            let mut out = BTreeMap::new();
            for t in &tops {
                if let Some(n) = cx.node(t) {
                    n.emit(t, &mut out);
                }
            }
            Some(out)
        }
    }

    /// What the type rule decided for one path.
    struct Node {
        /// The newest stamp among the cells of the visible subtree.
        newest: MStamp,
        shape: Shape,
    }

    enum Shape {
        Leaf(Value),
        Map(BTreeMap<Path, Node>),
    }

    impl Node {
        fn emit(&self, at: &Path, out: &mut BTreeMap<Path, Value>) {
            match &self.shape {
                Shape::Leaf(v) => {
                    out.insert(at.clone(), v.clone());
                }
                Shape::Map(kids) => {
                    out.insert(at.clone(), Value::Map);
                    for (c, n) in kids {
                        n.emit(c, out);
                    }
                }
            }
        }
    }

    /// The inputs of the type rule, for the recursion of [`MergeState::visible`].
    struct Tree<'a> {
        st: &'a MergeState,
        al: &'a BTreeSet<MStamp>,
        o: &'a JoinOptions,
        newest: &'a BTreeMap<Path, MStamp>,
        kids: &'a BTreeMap<Path, Vec<Path>>,
    }

    impl Tree<'_> {
        /// The node of `q`, `None` when `q` is absent.
        fn node(&self, q: &Path) -> Option<Node> {
            let ks: BTreeMap<Path, Node> = self
                .kids
                .get(q)
                .into_iter()
                .flatten()
                .filter_map(|c| self.node(c).map(|n| (c.clone(), n)))
                .collect();
            let own = self.st.frozen_value_in(q, self.al, self.o);
            let own_n = self.newest.get(q).copied();
            let top = |v: &BTreeMap<Path, Node>| v.values().map(|n| n.newest).max();
            if ks.is_empty() {
                let v = own?;
                let shape =
                    if v == Value::Map { Shape::Map(BTreeMap::new()) } else { Shape::Leaf(v) };
                return Some(Node { newest: own_n?, shape });
            }
            match own {
                // a scalar, number or array at q: only the children newer than its register show
                Some(v) if v != Value::Map => {
                    let reg = self.st.frozen_winner_in(q, self.al, self.o);
                    let rs = match reg {
                        Some(r) if r.value.is_plain() => r.stamp,
                        _ => own_n?,
                    };
                    let shown: BTreeMap<Path, Node> = if self.o.type_ignores_recency() {
                        BTreeMap::new()
                    } else {
                        ks.into_iter().filter(|(_, n)| n.newest > rs).collect()
                    };
                    if shown.is_empty() {
                        return Some(Node { newest: own_n?, shape: Shape::Leaf(v) });
                    }
                    let newest = top(&shown).into_iter().chain(own_n).max()?;
                    Some(Node { newest, shape: Shape::Map(shown) })
                }
                Some(_) if self.o.marker_hides() => {
                    Some(Node { newest: own_n?, shape: Shape::Map(BTreeMap::new()) })
                }
                // a map marker, an unset or no cell at all: a map of every visible child
                _ => {
                    let newest = top(&ks).into_iter().chain(own_n).max()?;
                    Some(Node { newest, shape: Shape::Map(ks) })
                }
            }
        }
    }
}
