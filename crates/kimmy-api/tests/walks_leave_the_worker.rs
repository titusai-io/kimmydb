//! Every storage walk reached from a request runs under `kimmy_storage::blocking`.
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

use std::path::Path;

/// Storage calls whose cost is the size of a collection, the oplog or the
/// store rather than of one key.
const WALKS: [&str; 8] = [
    ".for_each_doc(",
    ".for_each_doc_or_undecodable(",
    ".for_each_doc_after(",
    ".for_each_record_after(",
    ".visit_index_candidates(",
    ".count(&",
    ".live_unique_violations(",
    ".backup_to(",
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
    for path in paths {
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let body = std::fs::read_to_string(&path).unwrap();
        // A file's own tests drive storage directly, on purpose.
        let body = body.split("#[cfg(test)]").next().unwrap();

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
                    found.push((name.clone(), n + 1, line.trim().to_string()));
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
fn every_walk_reached_from_a_request_runs_under_blocking() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut offenders = Vec::new();
    for dir in [crates.join("kimmy-api/src"), crates.join("kimmy-mcp/src")] {
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
