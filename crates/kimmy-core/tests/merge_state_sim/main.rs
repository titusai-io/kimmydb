//! The Q7 P0 property-test harness for `kimmy_core::merge_state`: a port of the
//! reviewed Python model (`model5.py` oracle, `pullmodel5.py` cluster pull
//! protocol, `suite5.py` runner, `mutants5.py`, `targeted5.py`, `probes5.py`).
//!
//! Run with `Q7_RUNS=3000` for the full figure (default 300) and
//! `Q7_MUTANT_RUNS` (default 200) for the runs that grade each mutant. The
//! runner FAILS if a probe fails, any random, sequential, horizon or bounded run
//! reports a problem, or a mutant survives (section 17 / V5m3).
//!
//! The workload mixes in nested writes (`$set` and `$unset` at any depth, sub-documents,
//! replaces, arrays, writes below a scalar), the oracle is recursive (the type rule of D4 at
//! every depth), and every write's leaf effects are checked against it. The companion
//! differential test `merge_state_diff_nested` replays scripts of the vendored stage A model.
//! Each mutant is reported with the problems that killed it.
//!
//! The PRNG is a seeded xorshift, so the exact counts differ from the Python
//! model's; what must hold is 0 problems for the design and every mutant killed.

#[path = "../common/fingerprint.rs"]
mod fingerprint;
mod grade;
mod mutants;
mod oracle;
mod oracle_tests;
#[path = "../common/ord_replay.rs"]
mod ord_replay;
mod order_oracle;
mod probes;
mod rng;
mod sim;

use std::collections::BTreeMap;
use std::time::Instant;

use grade::{grade, grade_bounded};
use sim::{Cfg, RunConfig, run, run_bounded, run_bounded_observed, run_observed, settle};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// The failures of the whole runner; printed, then asserted empty.
#[derive(Default)]
struct Report {
    failures: Vec<String>,
}

impl Report {
    fn check(&mut self, what: &str, problems: &BTreeMap<String, (usize, u64)>) {
        for (k, (n, seed)) in problems {
            self.failures.push(format!("{what}: {n} x {k} (first seed {seed})"));
        }
    }
}

fn bump(c: &mut BTreeMap<String, (usize, u64)>, k: String, seed: u64) {
    c.entry(k).or_insert((0, seed)).0 += 1;
}

fn fmt_problems(c: &BTreeMap<String, (usize, u64)>) -> String {
    if c.is_empty() {
        "0".into()
    } else {
        format!("{:?}", c.iter().map(|(k, v)| (k.as_str(), v.0)).collect::<Vec<_>>())
    }
}

fn random_runs(rep: &mut Report, n: u64, label: &str, restore: bool, bound: Option<i64>) {
    let t0 = Instant::now();
    let cfg = Cfg::default_ret();
    let mut c = BTreeMap::new();
    let (mut snaps, mut rest, mut records) = (0, 0, 0);
    let mut kinds: BTreeMap<String, usize> = BTreeMap::new();
    for seed in 0..n {
        let rc = RunConfig { restore, int_bound: bound, ..RunConfig::default() };
        let o = run(seed, &rc, &cfg);
        snaps += usize::from(o.nodes.iter().any(|m| m.snaps > 0));
        rest += usize::from(o.nodes.iter().any(|m| m.restored));
        if !o.settled {
            bump(&mut c, "no fixpoint".into(), seed);
        }
        for e in grade(&o.nodes, &o.log) {
            bump(&mut c, e, seed);
        }
        records += usize::from(!o.nodes[0].state().losses.is_empty());
        for k in o.nodes[0].state().losses.keys() {
            *kinds.entry(format!("{:?}", k.kind)).or_default() += 1;
        }
    }
    println!(
        "[{label}] {n} runs ({snaps} snapshot, {rest} restore, {records} with a record): problems {}; kinds {kinds:?}; {:.2?}",
        fmt_problems(&c),
        t0.elapsed()
    );
    rep.check(label, &c);
}

fn sequential(rep: &mut Report, n: u64) {
    let t0 = Instant::now();
    let cfg = Cfg::default_ret();
    let mut c = BTreeMap::new();
    for seed in 0..n {
        let rc = RunConfig { steps: 14, sequential: true, ..RunConfig::default() };
        let o = run(seed, &rc, &cfg);
        if !o.nodes[0].state().losses.is_empty() {
            bump(&mut c, "a record in a sequential run".into(), seed);
        }
        for e in grade(&o.nodes, &o.log) {
            bump(&mut c, e, seed);
        }
    }
    println!(
        "[sequential] {n} runs, any record or problem: {}; {:.2?}",
        fmt_problems(&c),
        t0.elapsed()
    );
    rep.check("sequential", &c);
}

/// V3M1 / beyond retention (ADR-085): after the pulls, redeliver every entry
/// and repair by snapshot where the members differ; the cluster must SETTLE
/// (stop moving) and no unrestored member may emit a record twice.
fn horizon(rep: &mut Report, n: u64, ret: u64, steps: usize, label: &str) {
    let t0 = Instant::now();
    let cfg = Cfg::default_ret();
    let mut c = BTreeMap::new();
    let mut need = 0;
    for seed in 0..n {
        let rc = RunConfig { steps, retention: Some(ret), ..RunConfig::default() };
        let mut o = run(seed, &rc, &cfg);
        let c0 = o.nodes[0].canon();
        need += usize::from(o.nodes.iter().any(|m| m.canon() != c0));
        if settle(&mut o.nodes, 6).is_none() {
            bump(&mut c, "never settles".into(), seed);
        }
        for m in &o.nodes {
            let ev: std::collections::BTreeSet<_> = m.events.iter().collect();
            if !m.restored && m.events.len() != ev.len() {
                bump(&mut c, "emitted twice".into(), seed);
                break;
            }
        }
    }
    let what = format!("{label} RET={ret}, {steps} steps");
    println!(
        "[{what}] {n} runs, {need} differ after the pulls (ADR-085), problems: {}; {:.2?}",
        fmt_problems(&c),
        t0.elapsed()
    );
    rep.check(&what, &c);
}

/// Five seeds of the bounded runs (RET 40, a fixpoint every 3 steps, 80 steps) that FAILED before the sim's
/// protocol had the covered-entry skip, found by sweeping 0 to 3000 on both workloads, and how they failed.
///
/// All five are the same failure, the Q3 resurrection path. A pull window re-sends an old entry
/// (a writer's whole state at its write) that the receiver already holds, because the receiver's
/// `witnessed` vector for some origin is low (a restored member is a new origin: everyone's
/// vector for it starts at 0, so its first pulls replay whole oplogs). The entry brings a life
/// that a delete killed back to life: the horizon has already collected the delete and the dead
/// mark, so the join sees a live life nothing kills. In every one of the five, at the join that
/// brings the life back the receiver's version vector COVERS the entry's stamp (the entry is
/// already in the receiver), and a node that does not apply an entry its vector covers (the real
/// protocol's covered-entry skip, `SimFlags::covered_skip`) passes all five. The skip is in the sim's
/// protocol now (and so is the start-HLC rule, `SimFlags::start_hlc`), the five seeds pass in the
/// runner like every other, and no failure is known or listed. What is kept is the EVIDENCE:
/// `the_covered_entry_skip_clears_the_five_seeds_that_fail_without_it` runs them with the skip off (they
/// fail as listed here) and on (they pass); the mutant `no covered-entry skip` is graded by these runs. It
/// was a failure of the sim's protocol, not a partition longer than retention (ADR-085, graded by the
/// unbounded runs), and not the merge state: the same seeds failed on the code before the D7 pass. A
/// restore is the occasion in four of the five, never the cause (the nested one has none).
///
/// | seed | workload | problem | life brought back by the covered entry | restore (lag of its backup) |
/// |---|---|---|---|---|
/// | 106  | flat   | visible  | 1 and 8, entries 1 and 8, at horizon 15 | A, lag 49 |
/// | 1435 | flat   | diverged | 1 and 13, entries 1 and 13, at horizon 19 | B, lag 45 |
/// | 1551 | flat   | diverged | 1, entry 1, at horizon 10 | A, lag 25 |
/// | 2006 | flat   | diverged | 1, entry 1, at horizon 8 | A, lag 38 |
/// | 2396 | nested | visible  | 1, entry 22, at horizon 35 to 40 | none |
struct SkipEvidence {
    nested: bool,
    seed: u64,
    problems: &'static [&'static str],
}

const COVERED_SKIP_EVIDENCE: [SkipEvidence; 5] = [
    SkipEvidence { nested: false, seed: 106, problems: &["visible"] },
    SkipEvidence { nested: false, seed: 1435, problems: &["diverged"] },
    SkipEvidence { nested: false, seed: 1551, problems: &["diverged"] },
    SkipEvidence { nested: false, seed: 2006, problems: &["diverged"] },
    SkipEvidence { nested: true, seed: 2396, problems: &["visible"] },
];

fn bounded(rep: &mut Report, n: u64, ret: u64, sync_every: usize, steps: usize, nested: bool) {
    let t0 = Instant::now();
    let cfg = Cfg::default_ret();
    let mut c = BTreeMap::new();
    let mut gap_max = 0;
    for seed in 0..n {
        let (o, gap) = run_bounded(seed, &cfg, ret, sync_every, steps, nested);
        gap_max = gap_max.max(gap);
        for e in grade_bounded(&o.nodes, &o.log) {
            bump(&mut c, e, seed);
        }
    }
    if gap_max >= ret {
        bump(&mut c, format!("clock advance {gap_max} not below RET"), 0);
    }
    let what = format!(
        "bounded RET={ret}, fixpoint every {sync_every} steps, {steps} steps{}",
        if nested { ", nested, no restore" } else { "" }
    );
    println!(
        "[{what}] {n} runs, largest clock advance between fixpoints {gap_max} (< RET): problems {}; {:.2?}",
        fmt_problems(&c),
        t0.elapsed()
    );
    rep.check(&what, &c);
}

fn named_probes(rep: &mut Report) {
    let design = Cfg::default_ret();
    let mut epoch_from_backup = Cfg::default_ret();
    epoch_from_backup.flags.epoch_from_backup = true;
    let d = probes::v4m2_epoch_twice(&design);
    let m = probes::v4m2_epoch_twice(&epoch_from_backup);
    let ok = d.values().all(|v| *v == kimmy_core::merge_state::Value::Int64(125))
        && m.values().all(|v| *v == kimmy_core::merge_state::Value::Int64(105));
    println!(
        "[probe V4M2 epoch twice] fresh epoch {d:?}; epoch from the backup {m:?}: {}",
        if ok { "ok" } else { "FAILED" }
    );
    if !ok {
        rep.failures.push("probe V4M2 epoch twice".into());
    }
    let mut no_epoch = Cfg::default_ret();
    no_epoch.flags.epoch_keyed = false;
    let mut no_snap_loss = Cfg::default_ret();
    no_snap_loss.flags.snapshot_loss_entries = false;
    for t in
        [probes::targeted_v3m2(&design, &no_epoch), probes::targeted_v3m1(&design, &no_snap_loss)]
    {
        println!(
            "[targeted {}] {} final pull orders: design converges in all but {}, mutant fails in all but {}",
            t.label, t.orders, t.design_bad, t.mutant_ok
        );
        if !t.pass() {
            rep.failures.push(format!("targeted probe {}", t.label));
        }
    }
}

/// B1 and M1 scenarios (a counter that nets to zero; a non-numeric upsert creation value),
/// graded against the oracle.
fn scenario_probes(rep: &mut Report) {
    for (name, problems) in probes::scenarios() {
        println!(
            "[scenario {name}]: {}",
            if problems.is_empty() { "ok".to_string() } else { format!("{problems:?}") }
        );
        if !problems.is_empty() {
            rep.failures.push(format!("scenario {name}: {problems:?}"));
        }
    }
}

/// The scripts of probes_q3.py (the restore, the idle origin, the late member) with the two rules of Q3
/// on and off.
fn q3_probes(rep: &mut Report) {
    let bad = probes::q3_check();
    let design = probes::q3_scripts(&Cfg::default_ret());
    for (name, out) in &design {
        println!(
            "[Q3 {name}] design: {}, {} entries handed to A by the pull",
            if out.clean() { "clean" } else { "NOT CLEAN" },
            out.delivered
        );
    }
    println!(
        "[Q3 rules] the skip off brings the collected life back in the idle-origin and late-member scripts; both rules off in the restore; either alone clears it: {}",
        if bad.is_empty() { "ok".to_string() } else { format!("{bad:?}") }
    );
    for b in bad {
        rep.failures.push(format!("Q3: {b}"));
    }
}

#[test]
fn runner() {
    let t_all = Instant::now();
    let n = env_u64("Q7_RUNS", 300);
    let mruns = env_u64("Q7_MUTANT_RUNS", 200);
    let mut rep = Report::default();
    println!("Q7 P0 runner: Q7_RUNS={n}, Q7_MUTANT_RUNS={mruns}");

    random_runs(&mut rep, n, "no restore", false, None);
    random_runs(&mut rep, n, "with restore", true, None);
    // A lowered Int64 bound, so that saturation and the oracle's clamp are exercised.
    random_runs(&mut rep, n, "with restore, bound 1000", true, Some(1000));
    sequential(&mut rep, 400);
    horizon(&mut rep, 300, 4, 24, "horizon");
    named_probes(&mut rep);
    scenario_probes(&mut rep);
    q3_probes(&mut rep);
    bounded(&mut rep, 300, 40, 3, 80, false);
    bounded(&mut rep, 300, 40, 3, 80, true);
    horizon(&mut rep, 300, 40, 80, "beyond retention (no periodic pulls)");
    horizon(&mut rep, 300, 8, 30, "horizon");

    println!("mutants (graded over {mruns} runs, or by the named probe):");
    let base = mutants::failing_runs(
        &Cfg::default_ret(),
        mutants::Grader::Random { restore: true },
        mruns,
    );
    println!("  {:68} fails {base:3}/{mruns}", "(none, the design)");
    if base != 0 {
        rep.failures.push(format!("the design fails {base}/{mruns} mutant-grader runs"));
    }
    // The design mutants of section 17 (14), the B1 and M1 rules (2), the nested rules (7), the rules of Q3
    // (the covered-entry skip by the vector and by the stamp, the start-HLC rule) and the seven rules of the
    // writer's feed of the order state (O2): a removed one must be noticed.
    assert_eq!(mutants::mutants().len(), 33, "the mutant list changed");
    for m in mutants::mutants() {
        let o = mutants::judge(&m, mruns);
        let how = match (&o.probe, o.random_fail > 0) {
            (Some(p), true) => format!("KILLED by the runs [{}]; probe {p}", o.killers),
            (Some(p), false) => format!("KILLED by the probe only: {p}"),
            (None, true) => format!("KILLED by the runs [{}]", o.killers),
            (None, false) => "SURVIVED".to_string(),
        };
        println!("  {:68} fails {:3}/{}  {how}  [{} ms]", o.name, o.random_fail, o.runs, o.millis);
        if !o.killed {
            rep.failures.push(format!("mutant survived: {}", o.name));
        }
    }

    println!("total wall time {:.2?}", t_all.elapsed());
    assert!(rep.failures.is_empty(), "the runner found problems:\n{}", rep.failures.join("\n"));
}

/// One fingerprint over every member's state and reads before every step and after the last, for
/// a fixed set of seeds of the runs the runner grades. It pins the BEHAVIOUR of `merge_state`
/// (states, bytes, stamps, loss records, visible documents) so that a change that only makes it
/// cheaper can be shown to change nothing: the figures were taken before the performance pass of
/// Q7 step 0, and a change to them is a change of behaviour, never to be re-pinned for speed. They
/// were re-pinned once, when the odd field names (the empty name, `_`, a name with a dot) joined
/// the workload: the pools grew, the code did not change, and the code before the performance
/// pass (`8e36e8de`) gives these figures on the new workload as the code after it does.
#[test]
fn states_and_reads_match_the_pinned_fingerprints() {
    let cfg = Cfg::default_ret();
    let fold = |fp: &mut fingerprint::Fp, ms: &[sim::Node]| {
        for m in ms {
            fp.state(m.state(), &m.w.opts);
            fp.text(&format!("{:?} {:?}", m.w.clock, m.events));
        }
    };
    let random = |rc: RunConfig| {
        let mut fp = fingerprint::Fp::new();
        for seed in 0..40 {
            run_observed(seed, &rc, &cfg, &mut |ms| fold(&mut fp, ms));
        }
        fp.value()
    };
    let bounded = |sync_every: usize, nested: bool| {
        let mut fp = fingerprint::Fp::new();
        for seed in 0..40 {
            run_bounded_observed(seed, &cfg, 40, sync_every, 80, nested, &mut |ms| {
                fold(&mut fp, ms)
            });
        }
        fp.value()
    };
    let got = [
        ("restore", random(RunConfig::default())),
        ("no restore", random(RunConfig { restore: false, ..RunConfig::default() })),
        ("bound 1000", random(RunConfig { int_bound: Some(1000), ..RunConfig::default() })),
        ("sequential", random(RunConfig { steps: 14, sequential: true, ..RunConfig::default() })),
        ("horizon", random(RunConfig { steps: 24, retention: Some(4), ..RunConfig::default() })),
        ("bounded", bounded(3, false)),
        ("bounded nested", bounded(3, true)),
    ];
    for (name, fp) in got {
        println!("[fingerprint {name}] {fp:#018x}");
    }
    // RE-PINNED for item O2 (the Writer feeds the order state), each fingerprint now folding the compact bytes and
    // hash of the state WITH its order state and the document in the order the order state gives it:
    //   restore 0x4efca9449bca3d13 -> 0xd5f2038d6cb64fd0      no restore 0x08ad9af402612aaa -> 0x03c831e729d37118
    //   bound 1000 0xccecf868d8992847 -> 0xb5d376dace70d755    sequential 0xf5da0966517d1134 -> 0x5cf2b4f670f08b9e
    //   horizon 0x4173d22962941f92 -> 0xaab8149ae0f8c453       bounded 0xb59997222fd1686f -> 0x6a0197bd2778c8c7
    //   bounded nested 0x3eb070a76e6956fe -> 0x49b01bf0858bb6cd
    // (the last had moved once before, in Group A, when a write that only reorders became a write). Before
    // this commit they left the order state out, and six of the seven were the figures of the code before O2.
    let pinned: [(&str, u64); 7] = [
        ("restore", 0xd5f2038d6cb64fd0),
        ("no restore", 0x03c831e729d37118),
        ("bound 1000", 0xb5d376dace70d755),
        ("sequential", 0x5cf2b4f670f08b9e),
        ("horizon", 0xaab8149ae0f8c453),
        ("bounded", 0x6a0197bd2778c8c7),
        ("bounded nested", 0x49b01bf0858bb6cd),
    ];
    assert_eq!(got.map(|(n, f)| (n, f)), pinned, "a state or a read changed");
}

/// The order checks of the runner are not vacuous: over the default workloads a good share of the writes are
/// compared against the serial reference (read-your-writes order, `order_oracle.rs`), none disagrees, and the
/// members of every run read one ordered document (graded by `grade`, which also holds the join laws of the
/// order entries the run reached).
#[test]
fn the_read_your_writes_order_is_compared_on_a_good_share_of_the_writes() {
    let cfg = Cfg::default_ret();
    let (mut writes, mut compared, mut problems) = (0usize, 0usize, 0usize);
    for seed in 0..100 {
        let o = run(seed, &RunConfig::default(), &cfg);
        assert!(grade(&o.nodes, &o.log).is_empty(), "seed {seed}");
        writes += o.log.len();
        for m in &o.nodes {
            compared += m.order_checks;
            problems += m.order_problems.len();
        }
    }
    eprintln!(
        "read-your-writes order: {compared} of {writes} writes compared, {problems} problems"
    );
    assert_eq!(problems, 0);
    assert!(compared * 2 > writes, "{compared} of {writes}");
}

/// The odd field names (the empty name, `_`, a name with a dot) are in the workload at a rate that
/// was chosen once (`51dadd66`): a lower bound on the states that hold one, so that a later
/// reduction of the rate cannot happen quietly (the prop pools have the same guard,
/// `the_pools_hold_odd_names`). Counted over the members' states after the last step of the first
/// 100 runs of each workload, at the default configuration (365 states each); the bounds are about
/// half of the figures measured: flat 70 states with an odd-named register and 73 with an
/// odd-named slot, nested 84 and 47.
#[test]
fn the_sim_workload_holds_odd_names() {
    let cfg = Cfg::default_ret();
    let odd = |p: &kimmy_core::merge_state::Path| {
        p.segments().iter().any(|s| s.is_empty() || s == "_" || s.contains('.'))
    };
    for (name, nested, regs_at_least, slots_at_least) in
        [("flat", false, 35usize, 35usize), ("nested", true, 40usize, 20usize)]
    {
        let (mut states, mut with_reg, mut with_slot) = (0usize, 0usize, 0usize);
        for seed in 0..100 {
            let out = run(seed, &RunConfig { nested, ..RunConfig::default() }, &cfg);
            for m in &out.nodes {
                let st = m.state();
                states += 1;
                with_reg += usize::from(st.regs.keys().any(|k| odd(&k.1)));
                with_slot += usize::from(st.slots.keys().any(|k| odd(&k.1)));
            }
        }
        eprintln!(
            "{name}: odd names in {with_reg} of {states} states as registers, {with_slot} as slots"
        );
        assert!(
            with_reg > regs_at_least && with_slot > slots_at_least,
            "{name}: {with_reg} {with_slot}"
        );
    }
}

/// The bounded run of `seed` on the given workload, at the runner's parameters, with the covered-entry
/// skip and the start-HLC rule as given.
fn bounded_problems(seed: u64, nested: bool, covered_skip: bool, start_hlc: bool) -> Vec<String> {
    let mut cfg = Cfg::default_ret();
    cfg.flags.covered_skip = covered_skip;
    cfg.flags.start_hlc = start_hlc;
    let (o, _) = run_bounded(seed, &cfg, 40, 3, 80, nested);
    grade_bounded(&o.nodes, &o.log)
}

/// The five seeds that failed before the sim had the Q3 rules fail as listed with both rules off (the sim's
/// old protocol), and pass with the covered-entry skip alone (the entry's whole vector), with the start-HLC
/// rule alone for the four restores (not for the nested seed, which has no restore), and with both (the
/// default).
#[test]
fn the_covered_entry_skip_clears_the_five_seeds_that_fail_without_it() {
    for k in &COVERED_SKIP_EVIDENCE {
        assert_eq!(bounded_problems(k.seed, k.nested, false, false), k.problems, "seed {}", k.seed);
        for (skip, start) in [(true, false), (false, true), (true, true)] {
            // the start-HLC rule alone clears the restores (four flat seeds) but not the nested seed, which has
            // no restore: START_HLC alone is not the fix (review M1)
            if k.nested && !skip {
                assert_eq!(
                    bounded_problems(k.seed, k.nested, skip, start),
                    k.problems,
                    "seed {}",
                    k.seed
                );
                continue;
            }
            assert_eq!(
                bounded_problems(k.seed, k.nested, skip, start),
                Vec::<String>::new(),
                "seed {}, skip {skip}, start {start}",
                k.seed
            );
        }
    }
}

/// The sweep that found the five seeds, on the default protocol: the bounded runs (RET 40, a fixpoint every 3
/// steps, 80 steps) of the seeds 0 to 3000 (`Q7_BOUNDED_SEEDS`), flat and nested. No failure is known. By hand.
#[test]
#[ignore = "the extended sweep; run by hand"]
fn the_bounded_runs_of_3000_seeds_pass_on_both_workloads() {
    let n = env_u64("Q7_BOUNDED_SEEDS", 3000);
    let cfg = Cfg::default_ret();
    let mut failing = Vec::new();
    for nested in [false, true] {
        for seed in 0..n {
            let (o, _) = run_bounded(seed, &cfg, 40, 3, 80, nested);
            let ps = grade_bounded(&o.nodes, &o.log);
            if !ps.is_empty() {
                failing.push(format!(
                    "{} seed {seed}: {ps:?}",
                    if nested { "nested" } else { "flat" }
                ));
            }
        }
    }
    println!("bounded sweep, {n} seeds on each workload: {} failing {failing:?}", failing.len());
    assert!(failing.is_empty(), "{failing:?}");
}

/// The default protocol (the covered-entry skip and the start-HLC rule on) passes the five seeds.
macro_rules! bounded_seed {
    ($name:ident, $nested:expr, $seed:expr) => {
        #[test]
        fn $name() {
            let cfg = Cfg::default_ret();
            assert!(cfg.flags.covered_skip && cfg.flags.start_hlc);
            let (o, _) = run_bounded($seed, &cfg, 40, 3, 80, $nested);
            assert_eq!(grade_bounded(&o.nodes, &o.log), Vec::<String>::new());
        }
    };
}

bounded_seed!(bounded_flat_seed_106, false, 106);
bounded_seed!(bounded_flat_seed_1435, false, 1435);
bounded_seed!(bounded_flat_seed_1551, false, 1551);
bounded_seed!(bounded_flat_seed_2006, false, 2006);
bounded_seed!(bounded_nested_seed_2396, true, 2396);
