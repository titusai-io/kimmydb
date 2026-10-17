//! Reading this workspace's production source, for the guards that check it.
//!
//! Shared by `supervision.rs`, `walks_leave_the_worker.rs` and `key_inventory.rs`, because all
//! ask the same first question — which lines of a file are production code —
//! and an answer written twice drifts. The walk it replaced in the second guard
//! cut each file at its first `#[cfg(test)]`, which in `sessions.rs` sits on a
//! test-only method, so everything after it went unread; the reasons that cut
//! is wrong are set out on [`production_sources`].

use std::path::{Path, PathBuf};

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
#[allow(dead_code)] // not every guard that reads source walks the whole tree
pub fn production_sources(root: &Path) -> Vec<(PathBuf, String)> {
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
    walk(&root.join("crates"), &mut files);
    files.sort();

    // A `#[cfg(test)] mod x;` names a file that is test code wherever it lives,
    // so it leaves the walk with the module.
    let mut test_only_files = Vec::new();
    let read: Vec<(PathBuf, String)> = files
        .iter()
        .map(|path| {
            let body = std::fs::read_to_string(path).expect("a readable source file");
            // A module declared in `x.rs` lives in `x/`, one declared in `lib.rs`,
            // `main.rs` or `mod.rs` beside it, and one with a `#[path]` where that says.
            // Reading only "beside it" missed every `exec.rs`-style test module
            // (`exec/join_tests.rs`), so all three are taken.
            let dir = path.parent().expect("a file has a parent");
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
            for named in test_modules_declared(&body) {
                for home in [dir.to_path_buf(), dir.join(stem)] {
                    test_only_files.push(home.join(format!("{named}.rs")));
                    test_only_files.push(home.join(&named).join("mod.rs"));
                }
            }
            let lines: Vec<&str> = body.lines().collect();
            for (i, line) in lines.iter().enumerate() {
                let gated = |j: usize| lines.get(j).is_some_and(|l| l.trim() == "#[cfg(test)]");
                if let Some(rest) = line.trim().strip_prefix("#[path = \"")
                    && let Some(file) = rest.strip_suffix("\"]")
                    && (gated(i + 1) || i.checked_sub(1).is_some_and(gated))
                {
                    test_only_files.push(dir.join(file));
                }
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
pub fn opens_a_module(line: &str) -> bool {
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
pub fn test_modules_declared(body: &str) -> Vec<String> {
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
pub fn without_test_modules(body: &str) -> String {
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
