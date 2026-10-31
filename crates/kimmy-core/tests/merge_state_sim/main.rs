//! The Q7 P0 property-test harness for `kimmy_core::merge_state`: a port of the
//! reviewed Python model (`model5.py` oracle, `pullmodel5.py` cluster pull
//! protocol, `suite5.py` runner, `mutants5.py`, `targeted5.py`, `probes5.py`).
//!
//! Run with `Q7_RUNS=3000` for the full figure (default 300) and
//! `Q7_MUTANT_RUNS` (default 200) for the runs that grade each mutant. The
//! runner FAILS if a probe fails, any random, sequential, horizon or bounded run
//! reports a problem, or a mutant survives (section 17 / V5m3).
//!
//! The PRNG is a seeded xorshift, so the exact counts differ from the Python
//! model's; what must hold is 0 problems for the design and every mutant killed.

mod grade;
mod mutants;
mod oracle;
mod oracle_tests;
mod probes;
mod rng;
mod sim;

use std::collections::BTreeMap;
use std::time::Instant;

use grade::{grade, grade_bounded};
use sim::{Cfg, RunConfig, run, run_bounded, settle};

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

fn bounded(rep: &mut Report, n: u64, ret: u64, sync_every: usize, steps: usize) {
    let t0 = Instant::now();
    let cfg = Cfg::default_ret();
    let mut c = BTreeMap::new();
    let mut gap_max = 0;
    for seed in 0..n {
        let (o, gap) = run_bounded(seed, &cfg, ret, sync_every, steps);
        gap_max = gap_max.max(gap);
        for e in grade_bounded(&o.nodes, &o.log) {
            bump(&mut c, e, seed);
        }
    }
    if gap_max >= ret {
        bump(&mut c, format!("clock advance {gap_max} not below RET"), 0);
    }
    let what = format!("bounded RET={ret}, fixpoint every {sync_every} steps, {steps} steps");
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
    let ok = d.values().all(|v| *v == kimmy_core::merge_state::Value::Int(125))
        && m.values().all(|v| *v == kimmy_core::merge_state::Value::Int(105));
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

/// B1 and M1 scenarios, graded against the oracle (the reviewer's `zero_sum_counter_vanishes`
/// and `upsert_string_base_starts_no_generation`).
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
    bounded(&mut rep, 300, 40, 3, 80);
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
    // The design mutants of section 17 (14) and the B1 and M1 rules (2): a removed one must be noticed.
    assert_eq!(mutants::mutants().len(), 16, "the mutant list changed");
    for m in mutants::mutants() {
        let o = mutants::judge(&m, mruns);
        let how = match (&o.probe, o.random_fail > 0) {
            (Some(p), true) => format!("KILLED by the runs; probe {p}"),
            (Some(p), false) => format!("KILLED by the probe only: {p}"),
            (None, true) => "KILLED by the runs".to_string(),
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
