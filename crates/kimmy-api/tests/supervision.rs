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

/// Every production `.rs` under `crates/`, with each `#[cfg(test)]` **module**
/// removed and nothing else.
///
/// **Cutting at the first `#[cfg(test)]` line hid production code, twice.** The
/// first version cut at the first line whose *trimmed* text began
/// `#[cfg(test)]`, which matches one on a hook or a field, so `engine.rs` was
/// read as far as line 625 of about 3,700. The second cut at the first
/// *unindented* one — and that attribute sits on a `const`, a `type` and a
/// `pub(crate) mod hooks` in six files here, with real production code after it.
/// A second review measured what that still hid: about 1,230 lines, including
/// `kimmy-vector`'s whole `IndexCache`, the OTLP bridge in `logging.rs`, and
/// `engine.rs`'s `doc_range`. A `tokio::spawn` in any of them passed in silence,
/// and so did the first review's own control.
///
/// It also matched `mod tests` as a *prefix*, so a `mod testsupport;` would have
/// hidden every line after it.
///
/// So nothing is cut at a line any more. `#[cfg(test)]` on a module removes that
/// module — the braced block, or the `mod x;` and the file it names. On anything
/// else it removes nothing: keeping a test-only `fn` or `const` is loud-safe,
/// because the worst it can do is ask for a marker that costs one comment.
fn production_sources() -> Vec<(PathBuf, String)> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        // Only a **crate's own** top-level `tests/`, `examples/` and `benches/`
        // are skipped. A production module that happens to be called `tests`
        // deeper in a crate is production code, and skipping it by name would be
        // the same class of hole as everything above.
        let is_crate_root = dir.join("Cargo.toml").is_file();
        for entry in std::fs::read_dir(dir).expect("a readable directory") {
            let path = entry.expect("a directory entry").path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            if path.is_dir() {
                let own_harness = is_crate_root && ["tests", "examples", "benches"].contains(&name);
                if !name.starts_with('.') && name != "target" && !own_harness {
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

    // A `#[cfg(test)] mod x;` names a file that is test code wherever it lives,
    // so it leaves the walk with the module.
    let mut test_only_files = Vec::new();
    let read: Vec<(PathBuf, String)> = files
        .iter()
        .map(|path| {
            let body = std::fs::read_to_string(path).expect("a readable source file");
            for named in test_modules_declared(&body) {
                let dir = path.parent().expect("a file has a parent");
                test_only_files.push(dir.join(format!("{named}.rs")));
                test_only_files.push(dir.join(&named).join("mod.rs"));
            }
            (path.clone(), body)
        })
        .collect();

    read.into_iter()
        .filter(|(path, _)| !test_only_files.contains(path))
        .map(|(path, body)| {
            let kept = without_test_modules(&body);
            (path, kept)
        })
        .collect()
}

/// Whether this line opens a `mod` item, at any visibility.
fn opens_a_module(line: &str) -> bool {
    let l = line.trim_start();
    for prefix in ["pub(crate) ", "pub(super) ", "pub ", ""] {
        if let Some(rest) = l.strip_prefix(prefix)
            && let Some(rest) = rest.strip_prefix("mod ")
            && rest.chars().next().is_some_and(|c| c.is_alphabetic() || c == '_')
        {
            return true;
        }
    }
    false
}

/// The names of every `#[cfg(test)] mod x;` in `body` — a module whose code is in
/// another file.
fn test_modules_declared(body: &str) -> Vec<String> {
    let lines: Vec<&str> = body.lines().collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if line.trim() != "#[cfg(test)]" {
            continue;
        }
        if let Some(item) = lines.get(i + 1)
            && opens_a_module(item)
            && let Some(rest) = item.trim().strip_suffix(';')
            && let Some((_, name)) = rest.rsplit_once("mod ")
        {
            out.push(name.trim().to_string());
        }
    }
    out
}

/// `body` with every `#[cfg(test)]`-attributed module removed.
fn without_test_modules(body: &str) -> String {
    let lines: Vec<&str> = body.lines().collect();
    let mut kept = String::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].trim() == "#[cfg(test)]" {
            // Any further attributes between the gate and the item.
            let mut j = i + 1;
            while j < lines.len()
                && (lines[j].trim().starts_with("#[") || lines[j].trim().is_empty())
            {
                j += 1;
            }
            if j < lines.len() && opens_a_module(lines[j]) {
                let item = lines[j].trim_end();
                if item.ends_with('{') {
                    // The closing brace sits at the module's own indentation.
                    // Sound here because `cargo fmt --all --check` is a gate, so
                    // there is no hand-laid-out block for this to misread.
                    let indent: String =
                        lines[j].chars().take_while(|c| c.is_whitespace()).collect();
                    let close = format!("{indent}}}");
                    let mut k = j + 1;
                    while k < lines.len() && lines[k] != close {
                        k += 1;
                    }
                    assert!(
                        k < lines.len(),
                        "a #[cfg(test)] module opened at line {} and never closed at its own \
                         indentation, so this walk cannot tell where its test code ends",
                        j + 1
                    );
                    i = k + 1;
                    continue;
                }
                if item.ends_with(';') {
                    i = j + 1;
                    continue;
                }
            }
        }
        kept.push_str(lines[i]);
        kept.push('\n');
        i += 1;
    }
    kept
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
const EXEMPT_LINE: &str = "// UNSUPERVISED:";

/// Every `spawn`-shaped call in `body`, as (line number, the line).
///
/// **Matches the identifier's prefix, not a spelling of the path.** The rule was
/// three literal prefixes — `tokio::spawn(`, `tokio::task::spawn(`,
/// `std::thread::spawn(` — and a review got fifteen of twenty spawn forms past
/// it: `use tokio::spawn; spawn(..)`, `task::spawn`, a `JoinSet`'s `.spawn`,
/// `Handle::current().spawn`, `spawn_local`, `thread::Builder::new().spawn`, a
/// call split across lines, one inside a macro, `tokio::spawn (` with a space,
/// the turbofish, a crate alias, and a long-lived `spawn_blocking`.
///
/// So any identifier **beginning** `spawn` counts: that takes in `spawn_on`,
/// `spawn_blocking_on`, `spawn_pinned`, `spawn_ok` and `task::Builder::spawn_on`,
/// which are ordinary names in tokio and its neighbours rather than disguises,
/// and it needs no list to keep up to date. Whitespace, newlines, a turbofish
/// **and a comment** may sit between the name and its `(`.
fn spawn_calls(
    body: &str,
    local_helpers: &std::collections::BTreeSet<String>,
) -> Vec<(usize, String)> {
    let bytes = body.as_bytes();
    let alnum =
        |i: usize| bytes.get(i).is_some_and(|b| (*b as char).is_alphanumeric() || *b == b'_');

    let mut out = Vec::new();
    let mut from = 0;
    while let Some(found) = body[from..].find("spawn") {
        let i = from + found;
        from = i + 5;
        if alnum(i.wrapping_sub(1)) && i > 0 {
            continue; // `respawn`, `my_spawn`
        }
        // The rest of the identifier: `spawn_blocking`, `spawn_on`, ...
        let mut j = i + 5;
        while alnum(j) {
            j += 1;
        }
        // Whitespace, comments and a turbofish before the `(`.
        loop {
            let rest = &body[j..];
            let trimmed = rest.trim_start();
            let skipped = rest.len() - trimmed.len();
            if skipped > 0 {
                j += skipped;
                continue;
            }
            if let Some(after) = rest.strip_prefix("//") {
                j += 2 + after.find('\n').map_or(after.len(), |n| n + 1);
                continue;
            }
            if let Some(after) = rest.strip_prefix("/*") {
                match after.find("*/") {
                    Some(n) => {
                        j += 2 + n + 2;
                        continue;
                    }
                    None => break,
                }
            }
            if rest.starts_with("::<") {
                match rest.find('(') {
                    Some(n) => j += n,
                    None => break,
                }
            }
            break;
        }
        if !body[j..].starts_with('(') {
            continue;
        }
        let name = &body[i..i + (j - i).min(body[i..].len())];
        let name: String = name.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
        // **This workspace's own `spawn_*` helpers are not spawn APIs.** The
        // prefix rule catches `spawn_cert_reloader`, `spawn_collector`,
        // `spawn_cluster` and `spawn_jwks_refresher`, which are functions that
        // call `supervise` inside — and their definitions too. Skipping them
        // hides nothing, because each body is itself in this walk, so a spawn
        // inside one is flagged where it really is. Marking them instead would
        // put seven markers on functions that are already supervised and teach
        // the next reader that a marker means nothing.
        //
        // **Two guards on that skip, because a name-only skip would be a hole.**
        // If anyone defined a local `fn spawn_blocking`, a skip by name alone
        // would silently stop flagging every real `tokio::task::spawn_blocking(`
        // in the workspace — one local definition disabling the rule everywhere.
        // So the skip applies only to an **unqualified** call, never one reached
        // through `::` or `.`, and never to a name that is a real spawn API
        // whatever this workspace does with it.
        const SPAWN_APIS: [&str; 7] = [
            "spawn",
            "spawn_local",
            "spawn_blocking",
            "spawn_on",
            "spawn_blocking_on",
            "spawn_pinned",
            "spawn_ok",
        ];
        let qualified = body[..i].ends_with("::") || body[..i].ends_with('.');
        if !qualified && !SPAWN_APIS.contains(&name.as_str()) && local_helpers.contains(&name) {
            continue;
        }
        let line = body[..i].matches('\n').count();
        out.push((line + 1, body.lines().nth(line).unwrap_or_default().trim().to_string()));
    }
    out
}

/// Every `fn spawn*` this workspace defines, by name.
///
/// Read from the same sources the rule is applied to, so a helper added later is
/// recognised without anyone editing a list.
fn local_spawn_helpers(sources: &[(PathBuf, String)]) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    for (_, body) in sources {
        for line in body.lines() {
            let l = line.trim_start();
            for prefix in [
                "pub(crate) async fn ",
                "pub(crate) fn ",
                "pub async fn ",
                "pub fn ",
                "async fn ",
                "fn ",
            ] {
                if let Some(rest) = l.strip_prefix(prefix)
                    && rest.starts_with("spawn")
                {
                    let name: String =
                        rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
                    out.insert(name);
                    break;
                }
            }
        }
    }
    out
}

/// How much reason a marker must carry.
///
/// Long enough to be a clause rather than a shrug: "why not" and "see above" do
/// not tell the next reader whether a panic there may stop the node. The first
/// version asked for eleven characters, which `"a process"` satisfies.
const REASON_FLOOR: usize = 25;

/// How many of the spawns on `line` carry their own marker.
///
/// **A marker is its own comment line directly above**, never prose in a doc
/// comment and never text inside a string: the line's trimmed text must begin
/// `// UNSUPERVISED:`. One marker covers one spawn, so a line with two spawns on
/// it needs two marker lines above it — or, better, two lines.
fn markers_above(body: &str, line: usize) -> usize {
    let lines: Vec<&str> = body.lines().collect();
    let mut count = 0;
    let mut i = line - 1; // 0-indexed line above the spawn
    while i > 0 {
        let above = lines[i - 1].trim();
        if let Some(reason) = above.strip_prefix(EXEMPT_LINE) {
            if reason.trim().len() >= REASON_FLOOR {
                count += 1;
            }
            i -= 1;
            continue;
        }
        // A continuation of the reason across lines is fine; anything else ends
        // the block.
        if above.starts_with("//") {
            i -= 1;
            continue;
        }
        break;
    }
    count
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
    // **The premise that matters, per file rather than for one file.** A line
    // count on `engine.rs` passed while the walk was still cutting 135 lines
    // short of its test module, so the check is now: for every file with a
    // `#[cfg(test)]` module, the kept text reaches the last production line
    // before it. Nothing after a test module's attribute is production code,
    // and nothing before it may be missing.
    let mut checked = 0;
    for (path, kept) in &sources {
        let body = std::fs::read_to_string(path).expect("a readable source file");
        let lines: Vec<&str> = body.lines().collect();
        let Some(gate) = lines.iter().enumerate().find_map(|(i, l)| {
            (l.trim() == "#[cfg(test)]"
                && lines.get(i + 1).is_some_and(|item| opens_a_module(item)))
            .then_some(i)
        }) else {
            continue;
        };
        if let Some(last) = lines[..gate].iter().rev().find(|l| !l.trim().is_empty()) {
            assert!(
                kept.lines().any(|l| l == *last),
                "premise: the walk stops short of {}'s test module at line {}. The last \
                 production line before it is {last:?} and the kept text does not hold it, so \
                 whatever lies between is unread and unjudged.",
                path.strip_prefix(root()).unwrap_or(path).display(),
                gate + 1
            );
            checked += 1;
        }
    }
    assert!(checked > 40, "premise: files with test modules were checked ({checked})");

    // And six regions the second review measured by hand, named so that a
    // regression says which one went dark rather than only that one did. Each
    // sits after a `#[cfg(test)]` that is **not** on a module -- a `const`, a
    // `type`, a test-hook module -- and each was hidden until the walk stopped
    // cutting at a line.
    for (file, production) in [
        ("kimmy-storage/src/migrate.rs", "fn rebuild_partial_indexes"),
        ("kimmy-vector/src/cache.rs", "impl IndexCache"),
        ("kimmyd/src/logging.rs", "fn hold_instruments"),
        ("kimmy-storage/src/engine.rs", "pub(crate) fn doc_range("),
        ("kimmy-storage/src/index.rs", "pub(crate) fn clear_index_entries"),
        ("kimmy-storage/src/expiry.rs", "pub fn ttl_indexes"),
    ] {
        let (_, kept) = sources
            .iter()
            .find(|(p, _)| p.to_string_lossy().ends_with(file))
            .unwrap_or_else(|| panic!("{file} is in the walk"));
        assert!(
            kept.contains(production),
            "premise: {file} is read past its first `#[cfg(test)]`, so {production:?} is judged"
        );
    }

    let helpers = local_spawn_helpers(&sources);
    assert!(
        helpers.contains("spawn_cert_reloader"),
        "premise: this workspace's own spawn_* helpers were found ({helpers:?})"
    );
    let mut unexplained = Vec::new();
    let mut exempt_count = 0;
    for (path, body) in &sources {
        let shown = path.strip_prefix(root()).unwrap_or(path).display().to_string();
        // Grouped by line, because a marker covers one spawn: two on a line need
        // two markers above it.
        let mut per_line: std::collections::BTreeMap<usize, Vec<String>> = Default::default();
        for (line, text) in spawn_calls(body, &helpers) {
            per_line.entry(line).or_default().push(text);
        }
        for (line, spawns) in per_line {
            let markers = markers_above(body, line);
            exempt_count += markers.min(spawns.len());
            for text in spawns.iter().skip(markers) {
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
         `{EXEMPT_LINE} <why a panic there must not stop the process>` on its own line \
         directly above the call, at least {REASON_FLOOR} characters of reason, one per spawn:\n  \
         {}",
        unexplained.join("\n  ")
    );
}

/// Lines that close a brace and then carry a comment: `} // mod tests`, or
/// `} /* mod tests */`.
///
/// [`without_test_modules`] ends a module at the first line that is exactly the
/// module's indentation and `}`. Under `cargo fmt` that is always the module's
/// own closer, **unless the closer carries a trailing comment**, which fmt
/// keeps. Then the match runs on to the next bare `}` at that indentation,
/// which is production code's, and everything between is removed unread. A
/// premise that tried to rule this out by what follows a test module was false
/// on the tree as it stands: hook modules sit mid-file with production code
/// after them. So the one shape that closes late is banned instead.
fn closers_with_a_comment(body: &str) -> Vec<usize> {
    body.lines()
        .enumerate()
        .filter(|(_, l)| {
            l.trim_start().strip_prefix('}').is_some_and(|r| {
                let r = r.trim_start();
                r.starts_with("//") || r.starts_with("/*")
            })
        })
        .map(|(i, _)| i + 1)
        .collect()
}

#[test]
fn no_closing_brace_in_the_walk_carries_a_comment() {
    let found: Vec<String> = production_sources()
        .iter()
        .flat_map(|(path, _)| {
            let body = std::fs::read_to_string(path).expect("a readable source file");
            let shown = path.strip_prefix(root()).unwrap_or(path).display().to_string();
            closers_with_a_comment(&body).into_iter().map(move |line| format!("{shown}:{line}"))
        })
        .collect();
    assert!(
        found.is_empty(),
        "a closing brace with a comment after it can end a #[cfg(test)] module late, so the spawn \
         lint stops reading production code there without saying so. Move the comment to its own \
         line:\n  {}",
        found.join("\n  ")
    );

    // The control: the shape this bans does hide a production spawn from the
    // walk, and the guard names it.
    let late = "#[cfg(test)]\nmod tests {\n    fn t() {}\n} // mod tests\n\nfn production() {\n    \
                tokio::spawn(async {});\n}\n";
    assert!(!without_test_modules(late).contains("tokio::spawn"), "premise: the walk loses it");
    assert_eq!(closers_with_a_comment(late), vec![4]);
    let block = late.replace("} // mod tests", "} /* mod tests */");
    assert!(!without_test_modules(&block).contains("tokio::spawn"), "premise: this one too");
    assert_eq!(closers_with_a_comment(&block), vec![4]);
}

#[test]
fn the_spawn_matcher_sees_every_ordinary_spelling() {
    // The rule is a text rule, so it is tested as one. The end-to-end splices
    // prove the *walk* reaches a file's production code; this proves the
    // *match*, over the forms two reviews found — and it needs no snippet to
    // compile, which is what let the earlier attempt at these controls defeat
    // itself: a control that defined `fn spawn_on` in this workspace was
    // skipped by the local-helper rule, correctly, and proved nothing.
    let seen = |src: &str| !spawn_calls(src, &Default::default()).is_empty();

    for form in [
        "tokio::spawn(async {});",
        "tokio::task::spawn(async {});",
        "std::thread::spawn(|| {});",
        "spawn(async {});",
        "task::spawn(async {});",
        "join_set.spawn(async {});",
        "Handle::current().spawn(async {});",
        "spawn_local(fut);",
        "std::thread::Builder::new().spawn(|| {});",
        "tokio::spawn\n(async {});",
        "tokio::\nspawn(async {});",
        "tokio::spawn (async {});",
        "tokio::spawn::<Fut>(fut);",
        "tk::spawn(async {});",
        "tokio::task::spawn_blocking(|| {});",
        // The names the prefix rule is for: ordinary API, not disguises.
        "handle.spawn_on(fut, &rt);",
        "tokio::task::Builder::new().spawn_on(fut, &rt);",
        "spawn_blocking_on(|| {}, &rt);",
        "pool.spawn_pinned(|| fut);",
        "executor.spawn_ok(fut);",
        // A comment where whitespace would be.
        "tokio::spawn /* later */ (async {});",
        "tokio::spawn // why not\n(async {});",
    ] {
        assert!(seen(form), "the matcher must see {form:?}");
    }

    // **A local definition must not disable the rule for the real API.** Skipping
    // this workspace's own `spawn_*` helpers is by unqualified name only, and
    // never for a name that is itself a spawn API — so a workspace that happens
    // to define `fn spawn_blocking` still has every `tokio::task::spawn_blocking(`
    // flagged. A skip by name alone would have turned one local definition into a
    // silent hole across every crate.
    let mut pretend_local = std::collections::BTreeSet::new();
    pretend_local.insert("spawn_blocking".to_string());
    pretend_local.insert("spawn_cert_reloader".to_string());
    assert!(
        !spawn_calls("tokio::task::spawn_blocking(|| {});", &pretend_local).is_empty(),
        "a local `fn spawn_blocking` must not stop the real API being flagged"
    );
    assert!(
        !spawn_calls("handle.spawn_on(fut, &rt);", &pretend_local).is_empty(),
        "nor a method call on a handle"
    );
    assert!(
        spawn_calls("spawn_cert_reloader(&config);", &pretend_local).is_empty(),
        "but an unqualified call to a helper this workspace defines is that helper, and its own \
         body is in the walk"
    );

    // Not spawns: the identifier has to *start* with `spawn`, and be followed by
    // a call.
    for not in ["let respawn = 1;", "fn my_spawner() {}", "spawning += 1;", "spawn;"] {
        assert!(!seen(not), "the matcher must not see {not:?}");
    }

    // **Over-flagged on purpose**: a comment or a string that happens to contain
    // `spawn(` is matched, because the alternative is a comment-and-string
    // stripper, and a stripper that gets one case wrong hides a real spawn —
    // which is the whole class of defect this rule has been fixed for twice. The
    // cost of over-flagging is one marker line; the cost of under-flagging is a
    // task nobody is watching. No production comment in this workspace trips it
    // today, and the rule stays loud rather than clever.
    for over in ["// prose mentioning spawn(..)", "let s = \"spawn(\";"] {
        assert!(seen(over), "documented over-flagging, now not happening: {over:?}");
    }

    // **The declared residue**, pinned as residue rather than left to be
    // discovered: the function referred to by anything other than its own name.
    // Flagging a bare mention would flag every comment and doc line with the
    // word in it, so closing these needs a parse rather than a text rule. If one
    // of these ever starts being seen, this test says so and the record's Gaps
    // entry is out of date.
    for residue in [
        "let go = tokio::spawn; go(async {});",
        "futures.map(tokio::spawn);",
        "use tokio::spawn as go;\ngo(async {});",
        "go!(tokio::spawn, async {});",
    ] {
        assert!(
            !seen(residue),
            "this is the documented residue; if it is now seen, update testing.md's Gaps and \
             ADR-184: {residue:?}"
        );
    }
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
fn every_retrying_task_is_declared() {
    // `kimmy_task::RETRYING` decides whether the test switch's `error` mode will
    // do anything for a named task, so a task that grows a retry loop without
    // being listed would get a switch that silently does nothing — the exact
    // failure this round already fixed once for `:error` as a whole.
    let sources = production_sources();
    let mut retrying: Vec<String> = Vec::new();
    for (path, body) in &sources {
        let flat = body.replace(['\n', ' '], "");
        let mut rest = flat.as_str();
        while let Some(at) = rest.find("Retry::new(") {
            rest = &rest[at + "Retry::new(".len()..];
            match rest.strip_prefix('"') {
                Some(after) => {
                    retrying.push(after.chars().take_while(|c| *c != '"').collect());
                }
                None => panic!(
                    "a Retry is created for a task whose name is not a string literal, so this \
                     check cannot read it: {}",
                    path.strip_prefix(root()).unwrap_or(path).display()
                ),
            }
        }
    }
    retrying.sort();
    retrying.dedup();
    assert!(!retrying.is_empty(), "premise: the Retry::new call sites were found");

    let declared: Vec<String> = kimmy_task::RETRYING.iter().map(|t| (*t).to_string()).collect();
    assert_eq!(
        retrying, declared,
        "kimmy_task::RETRYING and the Retry::new call sites disagree. A task that retries and is \
         not listed gets a KIMMY_TEST_KILL_TASK error mode that does nothing and says nothing; a \
         task listed that does not retry gets a switch that waits for a retry that never comes."
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
