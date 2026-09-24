//! Every storage walk reached from a request or a replication round runs under
//! `kimmy_storage::blocking`.
//!
//! [ADR-153](../../../docs/decisions.md) moved `exec::visit_matching`'s walks
//! off the async worker and said every read verb that walks goes through that
//! one function. It did not: `$lookup`, `describe_collection`'s total, the
//! violations pass and a backup each walked inline, and the violations pass
//! alone held a worker for ~800 ms per call on a member with a day of oplog.
//! A claim about "every walk" is only as good as the check that finds the next
//! one, so this reads the source the handlers run.
//!
//! A call counts as covered when it sits inside the parentheses of a
//! `blocking(` call — the name on its own, so `nonblocking(` is not cover —
//! which the scan follows by bracket depth, so a closure of any length is seen
//! through. The walks that are allowed inline are counted
//! per file with the reason they are bounded, so a new walk in one of those
//! files still fails here.
//!
//! **A replication round is held to the same rule.** Applying a peer's window
//! ran on the async worker, inside the round's poll, and that apply can take as
//! long as the work an entry carries: a replicated collection drop purged every
//! row of the collection there, about 120 s for 400,000 documents, holding the
//! worker the whole time. So `kimmy-cluster` is read too, and applying a batch
//! is a walk.

mod source;

use source::{test_modules_declared, without_test_modules};
use std::path::Path;

/// Storage calls whose cost is the size of a collection, the oplog or the
/// store rather than of one key.
const WALKS: [&str; 17] = [
    ".for_each_doc(",
    ".for_each_doc_or_undecodable(",
    ".for_each_doc_after(",
    ".for_each_record_after(",
    ".visit_index_candidates(",
    ".count(&",
    ".live_unique_violations(",
    ".backup_to(",
    // A peer's window: as long as whatever its entries carry, an index build
    // or a collection drop included.
    ".apply_peer_batch_into(",
    ".apply_peer_batch(",
    // Schema changes: an index build files every document in one
    // transaction, an index drop removes every entry in one, and any of them
    // can wait for the single writer for the whole of a request's budget.
    // A collection drop only buries now, and a creation refuses rather than
    // purges (ADR-189), but both still wait for the writer. System
    // collections, created once and empty, are not here.
    ".create_collection(",
    ".drop_collection(",
    ".drop_database(",
    ".create_index_with(",
    ".drop_index_stamped(",
    ".configure_vectors(",
    ".disable_vectors(",
];

/// Walks allowed on the worker, per file, each bounded by something other than
/// the data a client stored.
const BOUNDED: [(&str, usize, &str); 5] = [
    ("webhooks.rs", 1, "the webhook registry: one document per subscription"),
    ("dispatch.rs", 3, "webhook jobs and delivery progress: subscriptions times members"),
    ("topology.rs", 1, "the node registry: one document per member"),
    ("schema.rs", 1, "sample_documents stops at its limit"),
    ("vectors.rs", 1, "the emptiness check stops at the first live vector"),
];

/// A line with string and character literals and a trailing `//` comment
/// blanked, so that brackets and names inside them are not read as code.
/// `in_string` carries a literal that runs on to the next line. A `'"'` read
/// as the start of a string would blank the rest of the file, and every walk
/// in it would pass unseen.
fn code_of(line: &str, in_string: &mut bool) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if !*in_string && c == '\'' {
            // `'x'` and `'\x'` are characters; `'a` alone is a lifetime.
            let ahead: Vec<char> = chars.clone().take(3).collect();
            let skip = match ahead.as_slice() {
                ['\\', _, '\'', ..] => 3,
                [_, '\'', ..] => 2,
                _ => 0,
            };
            for _ in 0..skip {
                chars.next();
            }
            out.push_str(&" ".repeat(skip + 1));
            continue;
        }
        if *in_string {
            match c {
                '\\' => {
                    chars.next();
                    out.push_str("  ");
                }
                '"' => {
                    *in_string = false;
                    out.push('"');
                }
                _ => out.push(' '),
            }
            continue;
        }
        match c {
            '"' => {
                *in_string = true;
                out.push('"');
            }
            '/' if chars.peek() == Some(&'/') => break,
            _ => out.push(c),
        }
    }
    out
}

fn uncovered(dir: &Path) -> Vec<(String, usize, String)> {
    let mut found = Vec::new();
    let mut paths: Vec<_> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
    paths.sort();
    // A `#[cfg(test)] mod x;` names a file that is test code, so it is not read.
    let test_only: Vec<std::path::PathBuf> = paths
        .iter()
        .filter(|p| p.extension().is_some_and(|e| e == "rs"))
        .flat_map(|p| test_modules_declared(&std::fs::read_to_string(p).unwrap()))
        .map(|named| dir.join(format!("{named}.rs")))
        .collect();
    for path in paths {
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if test_only.contains(&path) {
            continue;
        }
        let body = std::fs::read_to_string(&path).unwrap();
        found.extend(uncovered_in(&name, &body));
    }
    found
}

/// The walks in one file's production code that no `blocking(` covers, as
/// (file, line, text).
fn uncovered_in(name: &str, body: &str) -> Vec<(String, usize, String)> {
    let mut found = Vec::new();
    // A file's own tests drive storage directly, on purpose, so its test
    // modules are removed — the modules, not everything after the first
    // `#[cfg(test)]`, which in `transport.rs` sits on a hook in the middle of
    // the pull and hid the rest of the file.
    let body = without_test_modules(body);
    {
        let mut in_string = false;
        let mut depth = 0i64;
        // The bracket depth just inside each enclosing `blocking(`.
        let mut open: Vec<i64> = Vec::new();
        for (n, line) in body.lines().enumerate() {
            let code = code_of(line, &mut in_string);
            let mut at = 0;
            while at < code.len() {
                let rest = &code[at..];
                // `blocking(` as a name of its own: `nonblocking(` is not it.
                let named = code[..at]
                    .chars()
                    .next_back()
                    .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
                if named && rest.starts_with("blocking(") {
                    at += "blocking(".len();
                    depth += 1;
                    open.push(depth);
                    continue;
                }
                if open.is_empty() && WALKS.iter().any(|w| rest.starts_with(w)) {
                    found.push((name.to_string(), n + 1, line.trim().to_string()));
                }
                match rest.as_bytes()[0] {
                    b'(' | b'{' | b'[' => depth += 1,
                    b')' | b'}' | b']' => {
                        if open.last() == Some(&depth) {
                            open.pop();
                        }
                        depth -= 1;
                    }
                    _ => {}
                }
                at += rest.chars().next().unwrap().len_utf8();
            }
        }
    }
    found
}

#[test]
fn every_walk_reached_from_a_request_or_a_round_runs_under_blocking() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut offenders = Vec::new();
    for dir in [
        crates.join("kimmy-api/src"),
        crates.join("kimmy-mcp/src"),
        crates.join("kimmy-cluster/src"),
    ] {
        let found = uncovered(&dir);
        let mut files: Vec<&str> = found.iter().map(|(f, _, _)| f.as_str()).collect();
        files.dedup();
        for file in files {
            let here: Vec<_> = found.iter().filter(|(f, _, _)| f == file).collect();
            let allowed = BOUNDED.iter().find(|(f, _, _)| *f == file).map_or(0, |(_, n, _)| *n);
            if here.len() != allowed {
                offenders.extend(here.iter().map(|(f, n, l)| format!("{f}:{n}: {l}")));
            }
        }
        for (file, n, _) in BOUNDED {
            if dir.ends_with("kimmy-api/src") && !found.iter().any(|(f, _, _)| f == file) && n > 0 {
                offenders.push(format!(
                    "{file}: the {n} bounded walk(s) allowed here are gone; lower the count"
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these storage walks run on the async worker, where a walk holds the worker for \
         as long as the data it reads and `/metrics` queues behind it (ADR-153). Wrap each \
         in `kimmy_storage::blocking`, or, if it is bounded by something other than client \
         data, count it in BOUNDED with the reason:\n  {}",
        offenders.join("\n  ")
    );
}

#[test]
fn the_walk_reads_past_a_test_hook_and_not_into_a_test_module() {
    // The shape `transport.rs` has: a `#[cfg(test)]` on a statement, with the
    // rest of the function and file after it. The first form of this guard cut
    // there, so an apply on the worker below the hook passed unseen.
    let hooked = "fn round() {\n    #[cfg(test)]\n    std::thread::sleep(HOOK);\n    \
                  let applied = engine.apply_peer_batch_into(&theirs);\n}\n";
    let found = uncovered_in("hooked.rs", hooked);
    assert_eq!(found.len(), 1, "the apply after the hook is read: {found:?}");
    assert_eq!(found[0].1, 4);

    // Covered, it passes.
    let covered = hooked.replace(
        "engine.apply_peer_batch_into(&theirs)",
        "kimmy_storage::blocking(|| engine.apply_peer_batch_into(&theirs))",
    );
    assert!(uncovered_in("covered.rs", &covered).is_empty());

    // A test module still drives storage directly and is not read.
    let tests = "fn production() {}\n\n#[cfg(test)]\nmod tests {\n    \
                 fn t() {\n        engine.apply_peer_batch(&theirs);\n    }\n}\n";
    assert!(uncovered_in("tests.rs", tests).is_empty());
}
