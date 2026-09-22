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
            // **Only at a module-level test module.** This cut at the first
            // line whose *trimmed* text began `#[cfg(test)]`, which includes
            // every indented one inside ordinary code -- a `#[cfg(test)]` on a
            // hook, a field, a helper. `engine.rs` was cut at line 625 when its
            // test module starts at about 3690, and thirteen files lost roughly
            // ten thousand lines between them: unread, so unjudged, so anything
            // in them passed every rule in this file.
            //
            // A module-level attribute is unindented, which is what
            // distinguishes it.
            let cut = body
                .lines()
                .position(|l| l == "#[cfg(test)]" || l.starts_with("mod tests"))
                .map(|line| body.lines().take(line).map(|l| format!("{l}\n")).collect::<String>())
                .unwrap_or(body);
            (path, cut)
        })
        .collect()
}

/// The marker that exempts one spawn, with the reason it is exempt.
///
/// **Per call site, never per file.** This file used to carry a `NOT_SUPERVISED`
/// list of *files*, and it exempted `kimmyd/src/node.rs` — where twelve of the
/// fifteen supervised tasks start. An unsupervised spawn added anywhere in that
/// file passed in silence, which is the whole defect ADR-184 exists to remove,
/// in the one file where it was most likely to be introduced. A reviewer proved
/// it by splicing a plain `tokio::spawn(` into `node.rs` and watching this pass.
///
/// A comment rather than a table keyed by file and line, because a table drifts
/// the moment anything above it moves, and because the reason belongs where the
/// person reading the spawn is.
const EXEMPT: &str = "UNSUPERVISED:";

/// Every `spawn`-shaped call in `body`, as (line number, the line).
///
/// **Matches the identifier, not a spelling of the path.** The rule used to be
/// three literal prefixes — `tokio::spawn(`, `tokio::task::spawn(`,
/// `std::thread::spawn(` — and a reviewer got fifteen of twenty spawn forms past
/// it: `use tokio::spawn; spawn(..)`, `task::spawn`, a `JoinSet`'s `.spawn`,
/// `Handle::current().spawn`, `spawn_local`, `thread::Builder::new().spawn`, a
/// call split across lines, one inside a macro, `tokio::spawn (` with a space,
/// the turbofish, a crate alias, and a long-lived `spawn_blocking`. Every one of
/// those is a task nothing was watching.
///
/// So it is loud by default: any call to something *named* `spawn`,
/// `spawn_local` or `spawn_blocking` is flagged, wherever it came from, and
/// anything that is not a background task says so at the call site. A
/// `Command::spawn` for a child process is a false positive by design — it
/// becomes one visible exemption rather than a hole in the pattern.
///
/// Whitespace and a turbofish between the name and its `(` are skipped, which is
/// the same thing as matching flattened source while keeping the line number to
/// report.
fn spawn_calls(body: &str) -> Vec<(usize, String)> {
    const NAMES: [&str; 3] = ["spawn", "spawn_local", "spawn_blocking"];
    let bytes = body.as_bytes();
    let word = |i: usize, n: usize| {
        let before = i == 0
            || !{
                let c = bytes[i - 1] as char;
                c.is_alphanumeric() || c == '_'
            };
        let after = {
            let j = i + n;
            j >= bytes.len()
                || !{
                    let c = bytes[j] as char;
                    c.is_alphanumeric() || c == '_'
                }
        };
        before && after
    };

    let mut out = Vec::new();
    for (i, _) in body.char_indices() {
        let Some(name) = NAMES
            .iter()
            .filter(|n| body[i..].starts_with(**n) && word(i, n.len()))
            // The longest match, so `spawn_blocking` is not read as `spawn`.
            .max_by_key(|n| n.len())
        else {
            continue;
        };
        let mut j = i + name.len();
        while body[j..].starts_with([' ', '\t', '\n', '\r']) {
            j += 1;
        }
        // An optional turbofish, which hid one form on its own.
        if body[j..].starts_with("::<") {
            match body[j..].find('(') {
                Some(k) => j += k,
                None => continue,
            }
        }
        if !body[j..].starts_with('(') {
            continue;
        }
        let line = body[..i].matches('\n').count();
        out.push((line + 1, body.lines().nth(line).unwrap_or_default().trim().to_string()));
    }
    out
}

/// Whether the spawn on `line` carries its exemption and a reason.
///
/// Read from the comment block immediately above it, or a trailing comment on
/// the line itself.
fn exempted(body: &str, line: usize) -> bool {
    let lines: Vec<&str> = body.lines().collect();
    let reason_after = |l: &str| l.split_once(EXEMPT).is_some_and(|(_, why)| why.trim().len() > 10);
    if lines.get(line - 1).is_some_and(|l| reason_after(l)) {
        return true;
    }
    // Walk up the contiguous comment block.
    let mut i = line - 1;
    while i > 0 {
        let above = lines[i - 1].trim();
        if !above.starts_with("//") {
            return false;
        }
        if reason_after(above) {
            return true;
        }
        i -= 1;
    }
    false
}

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
    // And that the cut is not eating the files: `engine.rs` was being read as
    // far as line 625 of about 3700.
    let engine = sources
        .iter()
        .find(|(p, _)| p.to_string_lossy().ends_with("kimmy-storage/src/engine.rs"))
        .expect("engine.rs is in the walk");
    assert!(
        engine.1.lines().count() > 3_000,
        "premise: the cut keeps the file, not its first few hundred lines ({} lines)",
        engine.1.lines().count()
    );

    let mut unexplained = Vec::new();
    let mut exempt_count = 0;
    for (path, body) in &sources {
        let shown = path.strip_prefix(root()).unwrap_or(path).display().to_string();
        for (line, text) in spawn_calls(body) {
            if exempted(body, line) {
                exempt_count += 1;
            } else {
                unexplained.push(format!("{shown}:{line}: {text}"));
            }
        }
    }

    assert!(
        exempt_count >= 5,
        "premise: the exemption marker is being found at all ({exempt_count})"
    );
    assert!(
        unexplained.is_empty(),
        "these spawn something without going through kimmy_task::supervise, so if it is a \
         background task it dies alone and nothing notices (ADR-184). Supervise it, or write \
         `// {EXEMPT} <why a panic there must not stop the process>` above the call:\n  {}",
        unexplained.join("\n  ")
    );
}

#[test]
fn every_supervised_name_is_in_the_task_list_and_every_entry_is_used() {
    let sources = production_sources();
    let mut supervised: Vec<String> = Vec::new();
    let mut unreadable: Vec<String> = Vec::new();
    for (path, body) in &sources {
        // The name is the first argument and may sit on its own line, which is
        // how `replication_server` hides from a line-at-a-time reader.
        let flat = body.replace(['\n', ' '], "");
        for call in ["supervise(", "supervise_judged(", "supervise_oneshot("] {
            let mut rest = flat.as_str();
            while let Some(at) = rest.find(call) {
                rest = &rest[at + call.len()..];
                // **The quote is checked, not assumed.** This used to search
                // for `supervise("`, so a call whose name is anything but a
                // string literal -- a `const`, a `&str` variable -- matched
                // nothing and was skipped in silence: the task would be
                // supervised, absent from this check, and its retry series
                // would have no label. Now it is a loud failure instead.
                match rest.strip_prefix('"') {
                    Some(after) => {
                        let name: String = after.chars().take_while(|c| *c != '"').collect();
                        if !name.is_empty() {
                            supervised.push(name);
                        }
                    }
                    None => unreadable.push(format!(
                        "{}: {call}{}",
                        path.strip_prefix(root()).unwrap_or(path).display(),
                        rest.chars().take(40).collect::<String>()
                    )),
                }
            }
        }
    }
    assert!(
        unreadable.is_empty(),
        "these supervise a task under a name that is not a string literal, so this check cannot \
         read it and cannot tell whether kimmy_task::TASKS carries it. Use a literal, or teach \
         this test to resolve the constant:\n  {}",
        unreadable.join("\n  ")
    );
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

    // And the OTLP bridge, which carries one instrument per task by the
    // convention that file states. Without this the bridge could fall behind
    // the task list silently: `/metrics` would grow a series the bridge does
    // not publish, and the bridge's own guard matches on the series *stem*, so
    // fourteen of fifteen instruments would satisfy it.
    let bridge = std::fs::read_to_string(root().join("crates/kimmyd/src/logging.rs"))
        .expect("the bridge's source");
    let unbridged: Vec<&String> = declared
        .iter()
        .filter(|task| !bridge.contains(&format!("\"kimmy.task.retries.{task}\", \"{task}\"")))
        .collect();
    assert!(
        unbridged.is_empty(),
        "these tasks are in kimmy_task::TASKS and have no instrument on the OTLP bridge, so \
         their retries reach /metrics and nothing else. OTLP is a standing requirement: add a \
         `task_retries!` line for each in `logging.rs`:\n  {unbridged:?}"
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
