//! The Document-vs-Field metamorphic check (phase F, point 4; Rust only, no combined Python model, ruled).
//!
//! On a workload where last-writer-wins and the join agree (no delete, every document written by one member only; so no two
//! writes to a key are concurrent), the two policies must end identically: the same documents, the same appended / held /
//! retained sets, the same register state, and the same Q6 records, keyed `(_id, stamp)` (the `document` run's document-level
//! key; the `field` run's `(_id, path, stamp)` with the path dropped, the one path being the document's single register).
//! The runs share a seed, so they draw the same actions; `witness_pages` makes the `document` run witness the page versions'
//! stamps as the `field` run witnesses the states' version vectors (equal on this workload), so the stamps minted after a join agree.
//! Q7 loss entries and the `$inc` counters are off (the workload is the model's; the loss entries are the policy's own).
//!
//! Compared, per run: the entries minted (kind, origin, stamp, physical, key, body, opk), every member's whole view (the
//! trace replay's `member_view`: documents with stamps and bodies, register state, coverage, appended / dochist / retained
//! sets, held records, joins, the Q6 record keys with the loss bodies set aside, events emitted), and the oracle's results
//! (the REQUIRED and ALLOWED counts and every check), but for the counters only the `field` policy has (named below).

use std::collections::BTreeSet;

use serde_json::Value;

use crate::run::{Cfg, Summary, Writers, run};
use crate::trace::{all_diffs, entry_v, member_view};
use crate::world::Policy;

/// Counters only the `field` policy has.
const FIELD_ONLY: [&str; 3] = ["cnt_covered_skips", "cnt_q7_records_fresh", "cnt_same_write_held"];

/// A workload shape: a name and the configurations it runs.
pub struct Shape {
    pub name: &'static str,
    pub cfgs: Vec<Cfg>,
}

fn grid_with(members: &[usize], outs: &[usize], keys: &[usize], f: impl Fn(&mut Cfg)) -> Vec<Cfg> {
    let mut v = Vec::new();
    for &m in members {
        for &o in outs {
            for &k in keys {
                let mut c = Cfg::new(m, o, k);
                f(&mut c);
                v.push(c);
            }
        }
    }
    v
}

/// The workload shapes. Every one: no delete, one writer per document (`lww_workload`), Q7 entries and counters off.
pub fn shapes() -> Vec<Shape> {
    let base = |c: &mut Cfg, who: Writers| {
        c.lww_workload = Some(who);
        c.q7_entries = false;
        c.field_counters = false;
        c.witness_pages = true;
    };
    vec![
        // every document written by its owner; the relay's partitions, the outputs, the snapshot routes (joins, pages, grants)
        Shape {
            name: "owner-by-key, snapshots",
            cfgs: grid_with(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5], |c| {
                base(c, Writers::OwnerByKey)
            }),
        },
        // one member writes everything
        Shape {
            name: "one writer, snapshots",
            cfgs: grid_with(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5], |c| base(c, Writers::OneWriter)),
        },
        // no snapshot joins: the relay alone (partitions, witnessed skips, vanishing origins)
        Shape {
            name: "owner-by-key, no snapshots",
            cfgs: grid_with(&[2, 3, 4, 5], &[1, 2, 3], &[3, 4, 5], |c| {
                base(c, Writers::OwnerByKey);
                c.snapshots = false;
            }),
        },
        // long runs: three times the steps, so more partitions, flips, vanishes and joins per run
        Shape {
            name: "owner-by-key, long runs",
            cfgs: grid_with(&[3, 4, 5], &[2, 3], &[3, 5], |c| {
                base(c, Writers::OwnerByKey);
                c.steps = Some(3 * (120 + 40 * c.outs));
            }),
        },
        // one writer, many outputs, long: repeated re-binding with a single stream of writes behind each flip
        Shape {
            name: "one writer, many outputs, long runs",
            cfgs: grid_with(&[3, 5], &[3], &[3, 4], |c| {
                base(c, Writers::OneWriter);
                c.steps = Some(2 * (120 + 40 * c.outs));
            }),
        },
    ]
}

fn normalised(mut v: Value) -> Value {
    // the record bodies are the policy's (a value per lost write; the cut differs by rendering): the keys are compared
    for k in ["loss", "loss_pending"] {
        if let Some(a) = v[k].as_array_mut() {
            for rec in a.iter_mut() {
                rec[1] = Value::Null;
            }
            a.sort_by_key(|x| x.to_string());
        }
    }
    v
}

/// One seed under both policies; `mutant` is the mode of the `field` run (the `document` run is the base `v6`).
pub fn compare(
    mutant: &str,
    seed: u64,
    cfg: Cfg,
) -> (Vec<String>, crate::oracle::Res, crate::oracle::Res) {
    let mut cd = cfg;
    cd.policy = Policy::Document;
    let mut cf = cfg;
    cf.policy = Policy::Field;
    let d = run("v6", "v6", seed, cd, false);
    let f = run(mutant, "v6", seed, cf, false);
    let mut diffs: Vec<String> = Vec::new();
    if d.w.all.len() != f.w.all.len() {
        diffs.push(format!(
            "entries minted: document {} <> field {}",
            d.w.all.len(),
            f.w.all.len()
        ));
    }
    // the first entries that differ are the cause; the rest follow from it
    for (a, b) in d.w.all.iter().zip(f.w.all.iter()) {
        let (mut va, mut vb) = (entry_v(&d.w, a), entry_v(&f.w, b));
        // a loss entry's body is the policy's
        va["body"] = Value::Null;
        vb["body"] = Value::Null;
        all_diffs(&format!("entry {}", a.idx), &va, &vb, &mut diffs);
    }
    for (a, b) in d.ms.iter().zip(f.ms.iter()) {
        let (va, vb) = (normalised(member_view(&d.w, a)), normalised(member_view(&f.w, b)));
        all_diffs(&format!("member {}", a.name), &va, &vb, &mut diffs);
    }
    let keys: BTreeSet<&String> = d.res.keys().chain(f.res.keys()).collect();
    for k in keys {
        if FIELD_ONLY.contains(&k.as_str()) {
            continue;
        }
        // a check only the field policy has (`q7_records_diverged`) is absent from the document run's results: it counts as 0
        if d.res.get(k).copied().unwrap_or(0) != f.res.get(k).copied().unwrap_or(0) {
            diffs.push(format!(
                "result {k}: document {:?} <> field {:?}",
                d.res.get(k),
                f.res.get(k)
            ));
        }
    }
    (diffs, d.res, f.res)
}

/// Run `seeds` seeds of every configuration of the shapes under both policies (`mutant` for the `field` run); the number of
/// runs, the first few disagreements (shape, config, seed, the first differences) and the sums of the `field` runs' results.
pub fn sweep(
    mutant: &str,
    shapes: &[Shape],
    seeds: u64,
    threads: usize,
) -> (u64, Vec<String>, Summary) {
    let jobs: Vec<(usize, Cfg, u64)> = shapes
        .iter()
        .enumerate()
        .flat_map(|(i, s)| s.cfgs.iter().flat_map(move |c| (0..seeds).map(move |sd| (i, *c, sd))))
        .collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let out = std::sync::Mutex::new((0u64, Vec::<String>::new(), Summary::default()));
    std::thread::scope(|sc| {
        for _ in 0..threads.max(1) {
            sc.spawn(|| {
                let mut local = Summary::default();
                let mut bad: Vec<String> = Vec::new();
                let mut n = 0;
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= jobs.len() {
                        break;
                    }
                    let (si, c, s) = jobs[i];
                    let (diffs, _d, f) = compare(mutant, s, c);
                    n += 1;
                    local.add(s, &f);
                    if !diffs.is_empty() {
                        let shown: Vec<&String> = diffs.iter().take(3).collect();
                        bad.push(format!(
                            "[{}] {}m {}o {}k seed {s}: {} differences, first: {shown:?}",
                            shapes[si].name,
                            c.members,
                            c.outs,
                            c.keys,
                            diffs.len()
                        ));
                    }
                }
                let mut o = out.lock().unwrap();
                o.0 += n;
                o.1.extend(bad);
                o.2.merge(&local);
            });
        }
    });
    out.into_inner().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flags::FIELD_MUTANTS;

    fn seeds() -> u64 {
        std::env::var("Q6_META_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(8)
    }

    fn threads() -> usize {
        std::thread::available_parallelism().map(|n| n.get().min(4)).unwrap_or(2)
    }

    /// The two policies end identically on every shape, and the runs are not vacuous.
    #[test]
    fn the_document_and_field_policies_agree_on_a_workload_where_they_must() {
        let sh = shapes();
        let (runs, bad, s) = sweep("v6", &sh, seeds(), threads());
        eprintln!(
            "metamorphic: {runs} runs x 2 policies over {} shapes, {} disagreements",
            sh.len(),
            bad.len()
        );
        for k in [
            "nv_flips",
            "nv_late_writes(oracle)",
            "nv_loss_records",
            "nv_snapshot_joins",
            "cnt_appended_in_position",
            "cnt_same_write_held",
            "cnt_covered_skips",
            "cnt_dead_page_versions_retained",
            "cnt_witnessed_without_receiving",
            "cnt_snap_versions_as_entries",
            "heads_required",
            "late_write_count",
        ] {
            eprintln!("  {k} = {}", s.get(k));
        }
        for b in bad.iter().take(10) {
            eprintln!("  {b}");
        }
        assert!(bad.is_empty(), "{} runs disagree; first: {}", bad.len(), bad[0]);
        let p = s.problems();
        assert!(p.is_empty(), "problems on the field runs of the metamorphic shapes: {p:?}");
        for k in ["nv_flips", "nv_late_writes(oracle)", "nv_loss_records", "nv_snapshot_joins"] {
            assert!(s.get(k) > 0, "vacuous: {k}");
        }
    }

    /// The mutants the comparison cannot see on a workload of single writers (equivalent there, by construction): named, and
    /// killed by a scenario of the shape they break (the field grid does not see them either: 10,800 runs each, 0 problems).
    const EQUIVALENT_ON_SINGLE_WRITERS: [&str; 2] = [
        // a retained page version's late cells: with one writer per document the page version's only late cell is its own
        // write's, which the entry's own-write cells report anyway. Killed by `scenarios::a_field_mutant_the_random_runs_cannot_see_
        // is_killed_by_the_scenario_of_its_shape` (seed 885's shape with two late paths)
        "m-field-no-page-loss",
        // the HLC rule's witness of page states: a member's clock outruns its page states' stamps in these runs (the scripted
        // test `a_member_mid_join_writes_above_the_page_states_version_vector` kills it)
        "m-field-witness-off",
    ];

    /// Each Rust-only mutant of the field apply / loss path is killed by the comparison itself (it reports a disagreement),
    /// but the named equivalent ones, which must survive it (so the list stays honest).
    #[test]
    fn a_deliberate_fault_in_the_field_apply_or_loss_path_fails_the_comparison() {
        let sh = shapes();
        let mut wrong = Vec::new();
        for m in FIELD_MUTANTS {
            let (runs, bad, s) = sweep(m, &sh, seeds().min(4), threads());
            let own: Vec<String> = s.problems().keys().cloned().collect();
            eprintln!(
                "{m:34} {} of {runs} runs disagree; the field oracle alone: {}",
                bad.len(),
                if own.is_empty() { "silent".to_string() } else { format!("{own:?}") }
            );
            if bad.is_empty() != EQUIVALENT_ON_SINGLE_WRITERS.contains(&m) {
                wrong.push(m);
            }
        }
        assert!(
            wrong.is_empty(),
            "mutants with an unexpected verdict from the comparison: {wrong:?}"
        );
    }
}

#[cfg(test)]
mod debug {
    use super::*;

    /// Debug aid: `Q6_META_SEED=<seed> Q6_META_CFG=<members>,<outs>,<keys>,<shape index>` prints the disagreements of one run.
    #[test]
    #[ignore = "debug aid for one metamorphic run"]
    fn show_one_metamorphic_run() {
        let seed: u64 = std::env::var("Q6_META_SEED").unwrap().parse().unwrap();
        let spec = std::env::var("Q6_META_CFG").unwrap();
        let p: Vec<usize> = spec.split(',').map(|x| x.parse().unwrap()).collect();
        let sh = shapes();
        let cfg = sh[p[3]]
            .cfgs
            .iter()
            .find(|c| (c.members, c.outs, c.keys) == (p[0], p[1], p[2]))
            .unwrap();
        let (diffs, d, f) = compare("v6", seed, *cfg);
        if std::env::var("Q6_META_ENTRIES").is_ok() {
            let (mut cd, mut cf) = (*cfg, *cfg);
            cd.policy = Policy::Document;
            cf.policy = Policy::Field;
            let (rd, rf) = (run("v6", "v6", seed, cd, true), run("v6", "v6", seed, cf, true));
            for (tag, r) in [("document", &rd), ("field", &rf)] {
                for e in
                    r.w.all
                        .iter()
                        .skip(
                            std::env::var("Q6_META_FROM")
                                .ok()
                                .and_then(|v| v.parse().ok())
                                .unwrap_or(18),
                        )
                        .take(9)
                {
                    eprintln!("{tag}: {}", entry_v(&r.w, e));
                }
            }
        }
        for x in diffs.iter().take(30) {
            eprintln!("{x}");
        }
        for k in d.keys().chain(f.keys()).collect::<BTreeSet<_>>() {
            if d.get(k) != f.get(k) {
                eprintln!("res {k}: {:?} <> {:?}", d.get(k), f.get(k));
            }
        }
    }
}
