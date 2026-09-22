//! Every long-lived task is supervised, and the list of them is the truth.
//!
//! Two rules, both read from the source, because both are the kind that go
//! quietly false: a task added without supervision is exactly the defect
//! ADR-184 removes, and it would be added by someone who did not know the rule.
//!
//! **Why these live in `kimmy-api` rather than in `kimmy-task`, where they
//! belong.** They walk the repository from `join("../..")`, and
//! `documentation_tests_live_in_this_crate` — the guard that keeps CI's
//! documentation-only path honest — treats that idiom outside this crate as a
//! documentation read. Spelling the path some other way to slip past it is
//! precisely the evasion that guard names as its own blind spot, so these sit
//! where the idiom is already accounted for.

use std::path::{Path, PathBuf};

/// The repository root.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().expect("the repo root")
}

/// Every production `.rs` under `crates/`, read only as far as its own test
/// module, and outside `tests/`, `benches/` and `examples/`.
///
/// The same technique as the crate's other source rules: a spawn inside a test
/// module is a test's own business, and judging it would make every fixture an
/// exception to carry.
fn production_sources() -> Vec<(PathBuf, String)> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("a readable directory") {
            let path = entry.expect("a directory entry").path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            if path.is_dir() {
                // `examples/` is not the node: a demo that spawns a task and
                // ends is nobody's availability. `tests` and `benches` are
                // their own business, as the other source rules here treat them.
                let skip = ["target", "tests", "benches", "examples"];
                if !name.starts_with('.') && !skip.contains(&name) {
                    walk(&path, out);
                }
            } else if name.ends_with(".rs") {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(&root().join("crates"), &mut files);
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let body = std::fs::read_to_string(&path).expect("a readable source file");
            let cut = body
                .lines()
                .position(|l| {
                    let l = l.trim_start();
                    l.starts_with("#[cfg(test)]") || l.starts_with("mod tests")
                })
                .map(|line| {
                    body.lines().take(line).map(|l| format!("{l}\n")).collect::<String>()
                })
                .unwrap_or(body);
            (path, cut)
        })
        .collect()
}

/// Spawns that are deliberately not supervised, with the reason each is exempt.
///
/// A panic in one of these must **not** stop the process. That is the whole
/// reason ADR-184 supervises by name instead of setting `panic = "abort"`: a
/// crafted request that panicked a handler would otherwise be a remote kill
/// switch.
const NOT_SUPERVISED: &[(&str, &str)] = &[
    (
        "kimmy-cluster/src/transport.rs",
        "one task per inbound connection: a panic in one connection must not take the listener \
         down, which is the rule this file already states",
    ),
    (
        "kimmy-api/src/routes.rs",
        "spawn_blocking inside a request, awaited by the caller: it is that request's work, not \
         background work",
    ),
    (
        "kimmy-task/src/lib.rs",
        "the supervisor itself, which is what every other spawn goes through",
    ),
    (
        "kimmyd/src/node.rs",
        "two one-shots whose ending is the point: the previous-JWT-secret reminder, which warns \
         once and stops, and the shutdown watcher, whose return *is* shutdown",
    ),
];

#[test]
fn every_long_lived_task_is_spawned_through_the_supervisor() {
    let sources = production_sources();
    // Premise: the walk reached the files the tasks live in, so an empty answer
    // cannot be an empty walk.
    for must in ["kimmyd/src/node.rs", "kimmy-cluster/src/membership.rs"] {
        assert!(
            sources.iter().any(|(p, _)| p.to_string_lossy().contains(must)),
            "the walk did not reach {must}"
        );
    }

    let mut unexplained = Vec::new();
    for (path, body) in &sources {
        let shown = path.strip_prefix(root()).unwrap_or(path).display().to_string();
        let exempt = NOT_SUPERVISED.iter().any(|(file, _)| shown.ends_with(file));
        for (n, line) in body.lines().enumerate() {
            let spawns = line.contains("tokio::spawn(")
                || line.contains("tokio::task::spawn(")
                || line.contains("std::thread::spawn(");
            if spawns && !exempt {
                unexplained.push(format!("{shown}:{}: {}", n + 1, line.trim()));
            }
        }
    }

    assert!(
        unexplained.is_empty(),
        "these spawn a task without going through kimmy_task::supervise, so it dies alone and \
         nothing notices (ADR-184). Supervise it, or add the file to NOT_SUPERVISED with the \
         reason a panic there must not stop the process:\n  {}",
        unexplained.join("\n  ")
    );
}

#[test]
fn every_supervised_name_is_in_the_task_list_and_every_entry_is_used() {
    let sources = production_sources();
    let mut supervised: Vec<String> = Vec::new();
    for (_, body) in &sources {
        // The name is the first argument and may sit on its own line, which is
        // how `replication_server` hides from a line-at-a-time reader.
        let flat = body.replace(['\n', ' '], "");
        for call in ["supervise(\"", "supervise_judged(\"", "supervise_oneshot(\""] {
            let mut rest = flat.as_str();
            while let Some(at) = rest.find(call) {
                rest = &rest[at + call.len()..];
                let name: String = rest.chars().take_while(|c| *c != '"').collect();
                if !name.is_empty() {
                    supervised.push(name);
                }
            }
        }
    }
    supervised.sort();
    supervised.dedup();

    assert!(
        supervised.len() >= 10,
        "premise: the supervise calls were found, not just the list ({supervised:?})"
    );

    let declared: Vec<String> = kimmy_task::TASKS.iter().map(|t| (*t).to_string()).collect();
    let missing: Vec<&String> = supervised.iter().filter(|n| !declared.contains(n)).collect();
    let unused: Vec<&String> = declared.iter().filter(|n| !supervised.contains(n)).collect();

    assert!(
        missing.is_empty() && unused.is_empty(),
        "kimmy_task::TASKS and the supervise calls have drifted, so \
         kimmy_task_retries_total{{task}} is missing a label or carries one nothing writes.\n  \
         supervised but not in TASKS: {missing:?}\n  in TASKS but nothing supervises: {unused:?}"
    );
}

#[test]
fn no_profile_makes_a_panic_abort_the_process() {
    // The structural half of "a request handler's panic must not be a remote
    // kill switch" (ADR-184). Supervision is per task and by name precisely so
    // that this stays false: with `panic = "abort"` a crafted request that
    // panicked a handler would stop the node, and no amount of care about which
    // tasks are supervised would matter.
    let manifest = std::fs::read_to_string(root().join("Cargo.toml")).expect("the workspace");
    let offenders: Vec<&str> = manifest
        .lines()
        .filter(|l| {
            let l = l.trim();
            l.starts_with("panic") && l.contains("abort")
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "a profile sets panic = \"abort\", which makes a panic anywhere fatal -- including in a \
         request handler, which turns a crafted request into a remote kill switch. ADR-184 \
         supervises named tasks instead, for exactly this reason: {offenders:?}"
    );
    // Premise: the profiles this is about are in the file read, so an empty
    // answer is not an empty read.
    assert!(manifest.contains("[profile.release]"), "the release profile is not in this manifest");
}
