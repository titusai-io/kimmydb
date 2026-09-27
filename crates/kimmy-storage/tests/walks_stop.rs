//! Every walk of the store ends at the node's stop, or says why it need not.
//!
//! A walk holds the engine while it reads, and a stop can close the store
//! only once nothing holds it (ADR-147's 0.40.1 addendum). A walk serving a
//! peer's pull ran on past the stop's deadline, so the store was left for the
//! next start to repair. `walk::open_walk_table` gives a table whose `range`
//! and `iter` check the stop before every row; this reads the storage crate's
//! source and holds it to that, closed-world:
//!
//! - every function that iterates a table is in [`STOP_AWARE_WALKS`] or in
//!   [`BOUNDED_READS`] with the reason it cannot run long, and every entry in
//!   either names a function that still iterates one;
//! - a stop-aware walk opens its tables with `open_walk_table`, never
//!   `open_table`, and takes none as a plain redb table from its caller;
//! - a stop-aware walk does not swallow what a row yields: a row's `Err` is
//!   how the stop arrives, and a walk that dropped it would end early as if
//!   it had finished (a backup would write its `END` after half the store).
//!   `.ok()`, `.flatten()`, `Result::ok` and `let Ok(` are refused there,
//!   unless the line, or the one above it, says `// not a row:` and why;
//! - the storage walks the API's guard counts as walks
//!   (`kimmy-api/tests/walks_leave_the_worker.rs`) are stop-aware, or are
//!   writes, which `close_writes` bounds (ADR-192);
//! - the loops that do not read the store row by row, and so cannot use a
//!   walk table, call `check_walk(` themselves.

use std::collections::BTreeMap;
use std::path::Path;

/// Functions whose table walks end at the stop, as `file::function`.
const STOP_AWARE_WALKS: &[&str] = &[
    // A client's backup: the whole store.
    "backup::Engine::backup_to",
    // Request scans of documents and of the oplog.
    "docs::Engine::for_each_doc_or_undecodable",
    "docs::Engine::for_each_record_after",
    "docs::Engine::live_unique_violations",
    // Retention's scans, which serve no client.
    "gc::Engine::collect_oplog",
    "gc::Engine::collect_tombstones_within",
    // An index build's fill, and every read of an index's entries.
    "index::Engine::create_index_inner",
    "index::Engine::index_keyed_entries_after",
    "index::Walk::in_index_order",
    "index::Walk::in_key_order",
    "index::Walk::merged_runs",
    "index::Walk::one_run",
    "index::has_entries_at",
    "index::holders_of",
    "index::scan_table",
    // A snapshot page served to a peer, which passes over what a drop left.
    "snapshot::Engine::snapshot_documents",
    // The marks a pull names to its peer.
    "sync::Engine::held_marks_covered_by",
    // Where a change stream starts, and what it replays.
    "watch::Engine::first_arrival_at_or_after",
    "watch::Engine::first_arrival_stamped_after",
    "watch::Engine::read_arrival_batch",
    "watch::Seek::new",
    "watch::Seek::step",
    "watch::Walk::new",
    // The oplog window served to a peer, pushed to confirm a write, or read
    // for a client.
    "watch::Engine::read_oplog_from_skipping",
];

/// Functions that iterate a table without checking the stop, as
/// `(file::function, why it cannot hold a stop up)`.
const BOUNDED_READS: &[(&str, &str)] = &[
    (
        "divergence::Engine::collection_tombstones",
        "the dropped-collection registry: one row per drop",
    ),
    ("divergence::next_probe", "an in-memory set's range, not a table"),
    (
        "docs::WriteScope::for_each_doc",
        "inside a write scope, which holds the single writer: close_writes bounds it (ADR-192)",
    ),
    (
        "engine::Engine::all_collections",
        "the collection registry: one row per collection, however many documents",
    ),
    (
        "engine::Engine::bury_in_txn",
        "the registry, and one row: whether a database has a collection left",
    ),
    ("engine::Engine::clear_stamp_half", "at open, before the node serves or has a stop to answer"),
    (
        "engine::Engine::collection_range_is_empty",
        "one row of each table: whether anything is left under an id",
    ),
    ("engine::Engine::collections_dropped", "the dropped-collection registry: one row per drop"),
    (
        "engine::Engine::count_probe_reading",
        "the collection registry: one row per collection, however many documents",
    ),
    (
        "engine::Engine::create_collection_in_txn",
        "the registry, and one row of each table: whether an id is free",
    ),
    ("engine::Engine::drop_database", "one row: whether a database has a collection left"),
    (
        "engine::Engine::list_collections",
        "the collection registry: one row per collection, however many documents",
    ),
    ("engine::Engine::list_databases", "the database registry: one row per database"),
    (
        "engine::Engine::purge_chunk",
        "one chunk of DROP_PURGE_CHUNK rows, in a write: close_writes bounds it (ADR-192)",
    ),
    ("engine::Engine::read_versions_in", "a version vector: one row per origin"),
    (
        "engine::Engine::rebuild_arrival_index_if_stale",
        "at open, before the node serves or has a stop to answer",
    ),
    (
        "engine::Engine::rebuild_stamp_half_from_positions",
        "at open, before the node serves or has a stop to answer",
    ),
    (
        "engine::Engine::rebuild_version_vector_if_stale",
        "at open, before the node serves or has a stop to answer",
    ),
    (
        "engine::Engine::release_held_under",
        "inside the write that applies a peer's batch: close_writes bounds it (ADR-192)",
    ),
    (
        "engine::Engine::reset_version_vector_to_oplog",
        "`kimmyd rewind`, which runs on a stopped node",
    ),
    ("engine::Engine::snapshots_to_resume", "one row per peer a snapshot is under way with"),
    (
        "engine::collection_stands_under",
        "the collection registry: one row per collection, however many documents",
    ),
    (
        "gc::Engine::collect_dropped_collections",
        "the dropped-collection registry: one row per drop",
    ),
    ("gc::Engine::collect_dropped_indexes", "the dropped-index registry: one row per drop"),
    (
        "index::clear_index_entries",
        "an index drop's or rebuild's removal, in its write: close_writes bounds it (ADR-192)",
    ),
    ("index::partial_filters_holding_an_array", "at open, a migration's check of the registry"),
    ("live_count::rebuild_if_stale", "at open, before the node serves or has a stop to answer"),
    ("migrate::derive_collection_ids", "a migration, at open, before the node serves"),
    ("migrate::derive_index_ids", "a migration, at open, before the node serves"),
    ("migrate::move_documents", "a migration, at open, before the node serves"),
    ("migrate::move_index_entries", "a migration, at open, before the node serves"),
    ("migrate::move_index_ids", "a migration, at open, before the node serves"),
    ("migrate::partial_rebuild_plan", "a migration, at open, before the node serves"),
    ("migrate::persisted_violations", "a migration, at open, before the node serves"),
    ("migrate::rebuild_partial_indexes", "a migration, at open, before the node serves"),
    ("migrate::refuse_unparseable_partial_filters", "a migration, at open, before the node serves"),
    ("migrate::rewrite_oplog", "a migration, at open, before the node serves"),
    (
        "modify::scan_until",
        "inside find_and_modify's write transaction: close_writes bounds it (ADR-192)",
    ),
    (
        "purge::Engine::owed_purges",
        "the dropped registry, and one row per drop: whether rows are left",
    ),
    (
        "purge::Engine::rows_under",
        "test support for the purger: nothing that runs while serving calls it",
    ),
    ("rewind::Engine::rewind_to", "`kimmyd rewind`, which runs on a stopped node"),
    (
        "snapshot::Engine::collection_states",
        "the collection registry: one row per collection, however many documents",
    ),
    (
        "vectors::Engine::collection_by_id",
        "the collection registry: one row per collection, however many documents",
    ),
    (
        "watch::Engine::database_of_collection",
        "the collection registry: one row per collection, however many documents",
    ),
    (
        "watch::Engine::read_arrival_from",
        "at most `limit` entries; test support, which no stream reads through",
    ),
];

/// The API guard's walks that are writes: bounded by `close_writes`, which
/// waits for a write in progress up to its cap and then refuses a clean exit
/// (ADR-192). Every other entry there names a function in
/// [`STOP_AWARE_WALKS`].
const WRITES: &[&str] = &[
    "apply_peer_batch_into",
    "apply_peer_batch",
    "create_collection",
    "drop_collection",
    "drop_database",
    "drop_index_stamped",
    "configure_vectors",
    "disable_vectors",
];

/// Public walks that iterate no table themselves, and the function each
/// walks through, as `(Type::function, what its body calls)`: followed
/// until a [`STOP_AWARE_WALKS`] entry.
const DELEGATES: &[(&str, &str)] = &[
    ("docs::Engine::for_each_doc", "Engine::for_each_doc_after"),
    ("docs::Engine::for_each_doc_after", "Engine::for_each_record_after"),
    ("docs::Engine::count", "Engine::for_each_doc"),
    ("index::Engine::visit_index_candidates", "Walk::in_index_order"),
    ("index::Engine::create_index_with", "Engine::create_index_inner"),
];

/// Functions that open a table with `WalkStop::in_write()`, whose walks do
/// not stop: each must be inside a write transaction, which `close_writes`
/// bounds (ADR-192). A read path that used it would be a walk that ignores
/// the stop, so every use is listed here.
const IN_WRITE_SITES: &[&str] = &[
    // A write's index maintenance: its unique checks.
    "index::maintain",
    "index::maintain_remote",
    // find_and_modify's candidates, read in the write it commits.
    "index::scan_range_in_write",
];

/// Loops that do not iterate a table, each of which must call `check_walk(`,
/// as `(file under crates/, function)`.
const CHECKED_LOOPS: &[(&str, &str)] = &[
    // The HNSW insert loop, over vectors already read.
    ("kimmy-vector/src/index.rs", "HnswIndex::build_once"),
    // A local model's embedding, which holds the worker's engine while it runs.
    ("kimmy-vector/src/worker.rs", "EmbeddingWorker::call_provider"),
];

/// A line with string and character literals and a trailing `//` comment
/// blanked, so that brackets and names inside them are not read as code.
/// `in_string` carries a literal that runs on to the next line.
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

/// One function of production code: its name, qualified by the type whose
/// `impl` holds it (`Engine::for_each_doc`), where it starts, its lines as
/// written, and the same lines as code only.
struct Function {
    name: String,
    line: usize,
    text: Vec<String>,
    code: Vec<String>,
}

/// The type an `impl` line is for: `Walk` for `impl<'t> Walk<'t> {`, and
/// `WriterHold` for `impl Drop for WriterHold<'_> {`.
fn implements(code: &str) -> Option<String> {
    let rest = code.trim_start().strip_prefix("impl")?;
    if !(rest.starts_with(' ') || rest.starts_with('<')) {
        return None;
    }
    // Past the impl's own generics.
    let mut rest = rest;
    if rest.starts_with('<') {
        let mut depth = 0;
        for (i, c) in rest.char_indices() {
            match c {
                '<' => depth += 1,
                '>' => depth -= 1,
                _ => {}
            }
            if depth == 0 {
                rest = &rest[i + 1..];
                break;
            }
        }
    }
    let start = 0;
    let target = rest[start..].rsplit(" for ").next().unwrap().trim();
    // The type's own name: `Engine` for `crate::Engine`, `Walk` for `Walk<'t>`.
    let path = target.split(['<', ' ', '{']).next().unwrap_or("");
    let last = path.rsplit("::").next().unwrap_or("");
    let name: String = last.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    (!name.is_empty()).then_some(name)
}

/// The name a line declares a function by, if it does.
fn declares(code: &str) -> Option<String> {
    let at = code.find("fn ")?;
    let before = &code[..at];
    if !before.chars().all(|c| c.is_whitespace())
        && !["pub ", "pub(crate) ", "pub(super) ", "const ", "async ", "unsafe "]
            .iter()
            .any(|p| before.trim_start().starts_with(p.trim_end()))
    {
        return None;
    }
    let rest = &code[at + 3..];
    let name: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    (!name.is_empty()).then_some(name)
}

/// Every function in `body` that is not test code, the innermost owning each
/// line. A `#[cfg(test)]` item is skipped whole: a module or a function.
fn functions(body: &str) -> Vec<Function> {
    let lines: Vec<&str> = body.lines().collect();
    let mut out: Vec<Function> = Vec::new();
    // (index into `out`, depth at which its body opened)
    let mut open: Vec<(usize, i64)> = Vec::new();
    let mut depth = 0i64;
    let mut in_string = false;
    // Depth above which a `#[cfg(test)]` item is being skipped.
    let mut skipping: Option<i64> = None;
    let mut test_gate = false;
    let mut pending: Option<usize> = None;
    // (type, depth at which its impl opened), and an impl line not yet opened.
    let mut impls: Vec<(String, i64)> = Vec::new();
    let mut pending_impl: Option<String> = None;
    for (n, line) in lines.iter().enumerate() {
        let code = code_of(line, &mut in_string);
        let trimmed = code.trim();
        if skipping.is_none() && trimmed == "#[cfg(test)]" {
            test_gate = true;
            continue;
        }
        if test_gate && !trimmed.starts_with("#[") && !trimmed.is_empty() {
            test_gate = false;
            if trimmed.ends_with(';') {
                continue;
            }
            skipping = Some(depth);
        }
        if skipping.is_none()
            && open.is_empty()
            && let Some(ty) = implements(&code)
        {
            pending_impl = Some(ty);
        }
        if skipping.is_none()
            && let Some(name) = declares(&code)
            && !trimmed.ends_with(';')
        {
            let name = match impls.last() {
                Some((ty, _)) if open.is_empty() => format!("{ty}::{name}"),
                _ => name,
            };
            out.push(Function { name, line: n + 1, text: Vec::new(), code: Vec::new() });
            pending = Some(out.len() - 1);
        }
        if skipping.is_none() {
            if let Some((i, _)) = open.last() {
                out[*i].text.push((*line).to_string());
                out[*i].code.push(code.clone());
            } else if let Some(i) = pending {
                out[i].text.push((*line).to_string());
                out[i].code.push(code.clone());
            }
        }
        for c in code.chars() {
            match c {
                '{' => {
                    depth += 1;
                    if let Some(i) = pending.take() {
                        open.push((i, depth));
                    } else if let Some(ty) = pending_impl.take() {
                        impls.push((ty, depth));
                    }
                }
                '}' => {
                    if open.last().is_some_and(|(_, d)| *d == depth) {
                        open.pop();
                    }
                    if impls.last().is_some_and(|(_, d)| *d == depth) {
                        impls.pop();
                    }
                    depth -= 1;
                    if skipping == Some(depth) {
                        skipping = None;
                    }
                }
                _ => {}
            }
        }
    }
    out
}

/// Where `code`, one function's code joined, iterates a table: a `range`, an
/// `iter()` whose result is a `Result`, or a `retain` that returns one.
fn iterations(f: &Function) -> Vec<usize> {
    let mut found = Vec::new();
    for (i, line) in f.code.iter().enumerate() {
        let next = f.code.get(i + 1).map_or("", |l| l.trim_start());
        let ranges = line.contains(".range(") || line.contains(".range::<");
        let iter = line.find(".iter()").is_some_and(|at| {
            let after = line[at + ".iter()".len()..].trim_start();
            after.starts_with('?')
                || after.starts_with(".map_err")
                || (after.is_empty() && (next.starts_with('?') || next.starts_with(".map_err")))
        });
        let retains = (line.contains(".retain(") || line.contains(".retain_in("))
            && line.trim_end().ends_with(")?;");
        if ranges || iter || retains {
            found.push(f.line + i);
        }
    }
    found
}

/// The swallows a stop-aware walk may not make, unless marked.
const SWALLOWS: [&str; 6] =
    [".ok()", ".flatten()", "Result::ok", "let Ok(", "while let Some(Ok(", "filter_map(Result"];

fn storage_functions() -> Vec<(String, Function)> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut paths: Vec<_> = std::fs::read_dir(&src).unwrap().map(|e| e.unwrap().path()).collect();
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        // The wrapper itself, whose iterations are the checked ones.
        if !name.ends_with(".rs") || name == "walk.rs" {
            continue;
        }
        let body = std::fs::read_to_string(&path).unwrap();
        let stem = name.trim_end_matches(".rs").to_string();
        out.extend(functions(&body).into_iter().map(|f| (stem.clone(), f)));
    }
    out
}

#[test]
fn every_walk_of_the_store_stops_or_says_why_it_need_not() {
    let mut problems = Vec::new();
    let mut walking: BTreeMap<String, (Vec<usize>, &Function)> = BTreeMap::new();
    let all = storage_functions();
    for (file, f) in &all {
        let found = iterations(f);
        if !found.is_empty() {
            walking.insert(format!("{file}::{}", f.name), (found, f));
        }
    }
    let bounded: BTreeMap<&str, &str> = BOUNDED_READS.iter().copied().collect();
    for (key, (lines, _)) in &walking {
        let listed = STOP_AWARE_WALKS.contains(&key.as_str()) as u8
            + bounded.contains_key(key.as_str()) as u8;
        if listed != 1 {
            problems.push(format!(
                "{key} iterates a table (lines {lines:?}) and is listed {listed} times: make it \
                 stop-aware and list it in STOP_AWARE_WALKS, or list it once in BOUNDED_READS \
                 with why it cannot hold a stop up"
            ));
        }
    }
    for key in STOP_AWARE_WALKS.iter().chain(bounded.keys()) {
        if !walking.contains_key(*key) {
            problems.push(format!("{key} is listed but iterates no table; remove it"));
        }
    }
    for key in STOP_AWARE_WALKS {
        let Some((_, f)) = walking.get(*key) else { continue };
        let takes_walk_table = f.code.iter().any(|l| l.contains("WalkTable<"));
        for (i, code) in f.code.iter().enumerate() {
            let at = f.line + i;
            if code.contains(".open_table(") {
                problems.push(format!(
                    "{key}:{at}: opens a table with open_table; a stop-aware walk opens every \
                     table with open_walk_table"
                ));
            }
            // A generic bound is fine on a walk table's own type parameter.
            let plain = (code.contains("ReadableTable") && !takes_walk_table)
                || (["ReadOnlyTable<", "redb::Table<"].iter().any(|t| code.contains(t))
                    && !code.contains("WalkTable<"));
            if plain {
                problems.push(format!(
                    "{key}:{at}: takes a plain redb table; pass a WalkTable instead"
                ));
            }
            let marked = f.text[i].contains("// not a row:")
                || i.checked_sub(1)
                    .is_some_and(|p| f.text[p].trim_start().starts_with("// not a row:"));
            if !marked && let Some(s) = SWALLOWS.iter().find(|s| code.contains(*s)) {
                problems.push(format!(
                    "{key}:{at}: `{s}` in a stop-aware walk can drop the stop's Err; propagate \
                     it, or say `// not a row: <what it is>` on the line or the one above"
                ));
            }
        }
    }
    assert!(problems.is_empty(), "{} problem(s):\n  {}", problems.len(), problems.join("\n  "));
}

#[test]
fn the_walks_the_api_counts_are_stop_aware_or_writes() {
    let guard =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../kimmy-api/tests/walks_leave_the_worker.rs");
    let body = std::fs::read_to_string(guard).unwrap();
    let start = body.find("const WALKS").expect("the API guard's WALKS");
    let end = start + body[start..].find("];").unwrap();
    let names: Vec<&str> = body[start..end]
        .lines()
        .filter_map(|l| l.trim().strip_prefix("\".")?.split('(').next())
        .collect();
    assert!(names.len() > 10, "read the API guard's walks: {names:?}");
    let all = storage_functions();
    let body_of = |key: &str| {
        all.iter()
            .find(|(file, f)| format!("{file}::{}", f.name) == key)
            .map(|(_, f)| f.code.join("\n"))
    };
    // A delegate's name is `Type::function`; find it in any file.
    let resolve = |named: &str| {
        all.iter().find(|(_, f)| f.name == named).map(|(file, f)| format!("{file}::{}", f.name))
    };
    let stop_aware_by = |name: &str| -> Result<(), String> {
        let mut key = match STOP_AWARE_WALKS.iter().find(|k| k.ends_with(&format!("::{name}"))) {
            Some(_) => return Ok(()),
            None => DELEGATES
                .iter()
                .find(|(k, _)| k.ends_with(&format!("::{name}")))
                .map(|(k, _)| (*k).to_string())
                .ok_or(format!("{name}: neither stop-aware, a write, nor a delegate"))?,
        };
        for _ in 0..DELEGATES.len() + 1 {
            if STOP_AWARE_WALKS.contains(&key.as_str()) {
                return Ok(());
            }
            let (_, through) = DELEGATES
                .iter()
                .find(|(k, _)| *k == key)
                .ok_or(format!("{key}: not stop-aware and delegates nowhere"))?;
            let body = body_of(&key).ok_or(format!("{key} is gone; update DELEGATES"))?;
            let callee = through.rsplit("::").next().unwrap();
            if !body.contains(&format!("{callee}(")) {
                return Err(format!("{key} no longer calls {through}; update DELEGATES"));
            }
            key = resolve(through).ok_or(format!("{through} is gone; update DELEGATES"))?;
        }
        Err(format!("{name}: DELEGATES loops"))
    };
    let missing: Vec<String> = names
        .iter()
        .filter(|n| !WRITES.contains(n))
        .filter_map(|n| stop_aware_by(n).err())
        .collect();
    assert!(
        missing.is_empty(),
        "the API guard counts these as walks, and they are neither stop-aware walks here nor \
         writes: {missing:?}"
    );
}

#[test]
fn the_loops_that_read_no_table_check_the_stop() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut missing = Vec::new();
    for (file, name) in CHECKED_LOOPS {
        let body = std::fs::read_to_string(crates.join(file)).unwrap();
        let found = functions(&body).into_iter().find(|f| f.name == *name);
        match found {
            Some(f) if f.code.iter().any(|l| l.contains("check_walk(")) => {}
            Some(_) => missing.push(format!("{file}::{name} does not call check_walk(")),
            None => missing.push(format!("{file}::{name} is gone; update CHECKED_LOOPS")),
        }
    }
    assert!(missing.is_empty(), "{missing:#?}");
}

#[test]
fn the_scanner_finds_a_walk_and_skips_test_code() {
    let body = "fn walks(t: &T) -> Result<()> {\n    for row in t.range(..)? {\n        \
                let _ = row?;\n    }\n    Ok(())\n}\n\n#[cfg(test)]\nfn helper() {\n    \
                for row in t.iter()? {}\n}\n\n#[cfg(test)]\nmod tests {\n    fn t() {\n        \
                for row in t.iter()? {}\n    }\n}\n\nfn vec_only(v: &[u8]) -> usize {\n    \
                v.iter().count()\n}\n";
    let fs = functions(body);
    let names: Vec<&str> = fs.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["walks", "vec_only"], "test code skipped");
    let methods = functions(
        "impl<'t> Walk<'t> {\n    fn new() {}\n}\nimpl Drop for Hold<'_> {\n    fn drop(&mut self) {}\n}\n",
    );
    let names: Vec<&str> = methods.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["Walk::new", "Hold::drop"]);
    assert_eq!(iterations(&fs[0]), [2]);
    assert!(iterations(&fs[1]).is_empty(), "a Vec's iter is not a table's");
}

#[test]
fn a_walk_that_does_not_stop_is_inside_a_write() {
    let all = storage_functions();
    let mut problems = Vec::new();
    for (file, f) in &all {
        let key = format!("{file}::{}", f.name);
        let uses = f.code.iter().any(|l| l.contains("WalkStop::in_write()"));
        let listed = IN_WRITE_SITES.contains(&key.as_str());
        if uses && !listed {
            problems.push(format!(
                "{key} opens a table that does not stop (WalkStop::in_write()); only a write \
                 transaction's walks may, and each is listed in IN_WRITE_SITES"
            ));
        }
        if uses
            && !f.code.iter().any(|l| {
                l.contains("WriteTransaction")
                    || l.contains("begin_write(")
                    || l.contains("WriteTxn")
            })
        {
            problems.push(format!("{key} uses WalkStop::in_write() outside a write transaction"));
        }
    }
    for key in IN_WRITE_SITES {
        let found = all.iter().any(|(file, f)| {
            format!("{file}::{}", f.name) == *key
                && f.code.iter().any(|l| l.contains("WalkStop::in_write()"))
        });
        if !found {
            problems.push(format!("{key} is listed in IN_WRITE_SITES but does not use it"));
        }
    }
    assert!(problems.is_empty(), "{problems:#?}");
}
