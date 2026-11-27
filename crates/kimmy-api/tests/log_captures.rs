//! A test that records log lines is not switched off by another test's thread.
//!
//! While only one tracing dispatcher is registered, tracing asks the thread that first hits a callsite which events it wants,
//! and caches the answer for the whole process (tracing-core, `Rebuilder::JustOne`). Where tests share a process, a thread that
//! records nothing can so switch a line off for the test that is recording it: an assertion that the line is there fails now and
//! then, and one that it is absent passes while checking nothing. Two things close it, and each rule here holds one:
//!
//! 1. **A scoped capture holds a second dispatcher.** A function that calls `set_default`, `with_default` or `with_subscriber`
//!    holds, in its own body, `let _not_alone = ...;` with one of the [`SECOND`] dispatchers. There is no exception list.
//! 2. **A global subscriber is on a closed list.** One global for a whole test binary is asked on every thread, so it needs no
//!    second dispatcher. A function that installs one is in [`GLOBAL_INSTALLS`] with its reason.
//!
//! **What this does not see.** It checks that the line stands in the function, not that its value lives as long as the capture:
//! a line in an inner block that ends early passes, and so does a value dropped by hand. It knows these spellings only: a
//! subscriber installed under another name (a macro, a dependency's helper, a function passed by name) is unseen.
//!
//! It reads the whole repository from `join("../..")`, which `documentation_tests_live_in_this_crate` accounts for in this crate
//! only (see `supervision.rs`).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The calls that install a subscriber for a scope: a thread, a closure or a future.
const SCOPED: [&str; 3] = ["set_default", "with_default", "with_subscriber"];

/// What `let _not_alone =` may bind, written without whitespace. A library with several captures has the function.
const SECOND: [&str; 2] = [
    "a_second_dispatcher()",
    "tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default())",
];

/// Every function that installs a global subscriber: its file under `crates/`, its name, and why a global is right there.
const GLOBAL_INSTALLS: [(&str, &str, &str); 8] = [
    (
        "kimmy-storage/src/log_capture.rs",
        "install",
        "the one global of the storage library's tests, writing to a buffer for each thread",
    ),
    (
        "kimmy-storage/tests/live_walk_stop.rs",
        "a_stop_asked_for_before_a_forced_rebuild_ends_it_at_its_first_check",
        "the only test of its binary, since the stop it asks for is process-wide",
    ),
    (
        "kimmy-cluster/src/peers.rs",
        "logs_of",
        "the one global of the cluster library's tests that read formatted lines, writing to a buffer for each thread",
    ),
    (
        "kimmy-cluster/src/peers/revc_storm_tests.rs",
        "storm",
        "a measurement run only when its variable is set, alone in its process",
    ),
    ("kimmyd/src/logging.rs", "init", "the daemon's own subscriber, which is not a test's capture"),
    (
        "kimmyd/tests/live_walk_production.rs",
        "logs_of",
        "the one global of a binary that must hear threads the test did not start",
    ),
    (
        "kimmy-task/tests/exits.rs",
        "a_handle_dropped_outside_shutdown_says_so",
        "installed in the child process the test starts, whose lines its parent reads from standard error",
    ),
    (
        "kimmy-task/tests/exits.rs",
        "an_ordinary_ending_says_nothing_about_a_dropped_handle",
        "installed in the child process the test starts, whose lines its parent reads from standard error",
    ),
];

/// The fewest scoped captures the walk must find, so that a walk that reads nothing cannot pass.
const SCOPED_FLOOR: usize = 29;

// ---------------------------------------------------------------------------
// Reading a source file
// ---------------------------------------------------------------------------

fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// `source` with every comment, string and character literal turned to spaces, lines kept.
fn blanked(source: &str) -> Vec<char> {
    let text: Vec<char> = source.chars().collect();
    let mut out = text.clone();
    let at = |i: usize| text.get(i).copied().unwrap_or('\0');
    let mut i = 0;
    while i < text.len() {
        let end = if at(i) == '/' && at(i + 1) == '/' {
            (i..text.len()).find(|&j| text[j] == '\n').unwrap_or(text.len())
        } else if at(i) == '/' && at(i + 1) == '*' {
            let (mut depth, mut j) = (1, i + 2);
            while j < text.len() && depth > 0 {
                if at(j) == '/' && at(j + 1) == '*' {
                    (depth, j) = (depth + 1, j + 2);
                } else if at(j) == '*' && at(j + 1) == '/' {
                    (depth, j) = (depth - 1, j + 2);
                } else {
                    j += 1;
                }
            }
            j
        } else if let Some(end) = raw_string_end(&text, i) {
            end
        } else if at(i) == '"' {
            let mut j = i + 1;
            while j < text.len() && text[j] != '"' {
                j += if text[j] == '\\' { 2 } else { 1 };
            }
            j + 1
        } else if at(i) == '\'' && at(i + 1) == '\\' {
            // An escaped character: the escaped one is passed over, then the closing quote is looked for.
            (i + 3..text.len()).find(|&j| text[j] == '\'').map_or(text.len(), |j| j + 1)
        } else if at(i) == '\'' && at(i + 2) == '\'' {
            i + 3
        } else {
            // Anything else, a lifetime's quote among it, stays.
            i += 1;
            continue;
        };
        let end = end.min(text.len());
        for c in &mut out[i..end] {
            if *c != '\n' {
                *c = ' ';
            }
        }
        i = end;
    }
    out
}

/// Where the raw string starting at `i` ends (`r"..."`, `r#"..."#`, and their byte and C forms), if one starts there.
fn raw_string_end(text: &[char], i: usize) -> Option<usize> {
    if i > 0 && is_word(text[i - 1]) {
        return None;
    }
    let mut j = i;
    if matches!(text.get(j), Some('b' | 'c')) {
        j += 1;
    }
    if text.get(j) != Some(&'r') {
        return None;
    }
    j += 1;
    let hashes = text[j..].iter().take_while(|&&c| c == '#').count();
    j += hashes;
    if text.get(j) != Some(&'"') {
        return None;
    }
    j += 1;
    while j < text.len() {
        if text[j] == '"' && text[j + 1..].iter().take_while(|&&c| c == '#').count() >= hashes {
            return Some(j + 1 + hashes);
        }
        j += 1;
    }
    Some(text.len())
}

/// Every identifier in `code`, as (where it starts, where it ends, itself).
fn words(code: &[char]) -> Vec<(usize, usize, String)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < code.len() {
        if !is_word(code[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < code.len() && is_word(code[i]) {
            i += 1;
        }
        out.push((start, i, code[start..i].iter().collect()));
    }
    out
}

/// The first character at or after `i` that is not whitespace, and where it is.
fn next_mark(code: &[char], i: usize) -> Option<(usize, char)> {
    (i..code.len()).find(|&j| !code[j].is_whitespace()).map(|j| (j, code[j]))
}

/// A function with a body: its name, and the span from its `fn` to its closing brace.
struct Function {
    name: String,
    from: usize,
    to: usize,
}

fn functions(code: &[char], words: &[(usize, usize, String)]) -> Vec<Function> {
    let mut out = Vec::new();
    for pair in words.windows(2) {
        let ((from, end, keyword), (start, after, name)) = (&pair[0], &pair[1]);
        // `fn(u8)`, a pointer's type, has no name after it.
        if keyword != "fn" || code[*end..*start].iter().any(|c| !c.is_whitespace()) {
            continue;
        }
        // The body is the first `{` outside the signature's brackets; a `;` there ends a declaration without one.
        let (mut depth, mut i) = (0_i32, *after);
        let open = loop {
            let Some(&c) = code.get(i) else { break None };
            match c {
                '(' | '[' => depth += 1,
                ')' | ']' => depth -= 1,
                '{' if depth == 0 => break Some(i),
                ';' if depth == 0 => break None,
                _ => {}
            }
            i += 1;
        };
        let Some(open) = open else { continue };
        let (mut braces, mut j) = (0_i32, open);
        while j < code.len() {
            match code[j] {
                '{' => braces += 1,
                '}' => braces -= 1,
                _ => {}
            }
            if braces == 0 {
                break;
            }
            j += 1;
        }
        out.push(Function { name: name.clone(), from: *from, to: j });
    }
    out
}

/// The innermost function that holds position `i`.
fn innermost(functions: &[Function], i: usize) -> Option<usize> {
    (0..functions.len())
        .filter(|&f| functions[f].from < i && i <= functions[f].to)
        .max_by_key(|&f| functions[f].from)
}

/// Whether function `f`'s own text, the functions nested in it left out, holds `let _not_alone = <a second dispatcher>;`.
fn holds_the_second(
    code: &[char],
    words: &[(usize, usize, String)],
    functions: &[Function],
    f: usize,
) -> bool {
    words.iter().any(|(start, end, word)| {
        if word != "let" || innermost(functions, *start) != Some(f) {
            return false;
        }
        let statement: String =
            code[*end..].iter().take_while(|&&c| c != ';').filter(|c| !c.is_whitespace()).collect();
        statement.strip_prefix("_not_alone=").is_some_and(|bound| SECOND.contains(&bound))
    })
}

/// What one file installs: each function with a scoped capture and whether it holds the second dispatcher, and each function
/// that installs a global subscriber. A call outside every function is reported under `(no function)`.
#[derive(Default)]
struct Installs {
    scoped: BTreeSet<(String, bool)>,
    global: BTreeSet<String>,
}

fn installs(source: &str) -> Installs {
    let code = blanked(source);
    let words = words(&code);
    let functions = functions(&code, &words);
    let names_the_subscriber_crate = words.iter().any(|(_, _, w)| w == "tracing_subscriber");
    let mut out = Installs::default();
    for (k, (start, end, word)) in words.iter().enumerate() {
        let Some((open, '(')) = next_mark(&code, *end) else { continue };
        // A definition of a method under one of these names is no install.
        if k > 0 && words[k - 1].2 == "fn" {
            continue;
        }
        let scoped = SCOPED.contains(&word.as_str());
        let called_on_a_value =
            code[..*start].iter().rev().find(|c| !c.is_whitespace()) == Some(&'.');
        let global = word == "set_global_default"
            || (names_the_subscriber_crate
                && (word == "init" || word == "try_init")
                && called_on_a_value
                && matches!(next_mark(&code, open + 1), Some((_, ')'))));
        if !scoped && !global {
            continue;
        }
        let holder = innermost(&functions, *start);
        let name =
            holder.map_or_else(|| "(no function)".to_string(), |f| functions[f].name.clone());
        if scoped {
            let guarded = holder.is_some_and(|f| holds_the_second(&code, &words, &functions, f));
            out.scoped.insert((name, guarded));
        } else {
            out.global.insert(name);
        }
    }
    out
}

/// Both rules over `files`, each a path under `crates/` and its text, with `listed` as the closed list of global installs.
fn problems(files: &[(String, String)], listed: &[(&str, &str, &str)]) -> Vec<String> {
    let mut out = Vec::new();
    let mut found = BTreeSet::new();
    for (path, source) in files {
        let here = installs(source);
        for (name, guarded) in here.scoped {
            if !guarded {
                out.push(format!("unguarded capture: {path} {name}"));
            }
        }
        found.extend(here.global.into_iter().map(|name| (path.clone(), name)));
    }
    for (path, name) in &found {
        if !listed.iter().any(|(p, n, _)| p == path && n == name) {
            out.push(format!("unlisted global install: {path} {name}"));
        }
    }
    for (path, name, _) in listed {
        if !found.contains(&(path.to_string(), name.to_string())) {
            out.push(format!("stale entry, it installs no global: {path} {name}"));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// The repository
// ---------------------------------------------------------------------------

/// Every `.rs` under each crate's `src`, `tests`, `benches` and `examples`, as (path under `crates/`, text). This file is
/// left out: its fixtures are strings, and its constants name the calls.
fn sources() -> Vec<(String, String)> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for entry in entries {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }
    let crates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the repo root")
        .join("crates");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&crates).expect("crates/") {
        let one = entry.expect("a directory entry").path();
        for part in ["src", "tests", "benches", "examples"] {
            walk(&one.join(part), &mut files);
        }
    }
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let shown = path.strip_prefix(&crates).unwrap().to_string_lossy().replace('\\', "/");
            (shown, std::fs::read_to_string(&path).expect("a readable source file"))
        })
        .filter(|(shown, _)| shown != "kimmy-api/tests/log_captures.rs")
        .collect()
}

#[test]
fn every_log_capture_holds_a_second_dispatcher_and_every_global_one_is_listed() {
    let files = sources();
    let scoped: usize = files.iter().map(|(_, source)| installs(source).scoped.len()).sum();
    assert!(
        scoped >= SCOPED_FLOOR,
        "premise: the walk found the captures ({scoped} functions with a scoped one, at least {SCOPED_FLOOR})"
    );
    let found = problems(&files, &GLOBAL_INSTALLS);
    assert!(
        found.is_empty(),
        "{} problems. A function that records with `set_default`, `with_default` or `with_subscriber` holds, in its own body, \
         `let _not_alone = <{}>;` for as long as it records (the module doc says why). A global subscriber is in \
         GLOBAL_INSTALLS with its reason:\n  {}",
        found.len(),
        SECOND.join(" or "),
        found.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// The guard's own reading, on fixtures
// ---------------------------------------------------------------------------

/// The problems in one made-up file, with nothing listed.
fn of(source: &str) -> Vec<String> {
    problems(&[("x/src/a.rs".to_string(), source.to_string())], &[])
}

fn unguarded(name: &str) -> Vec<String> {
    vec![format!("unguarded capture: x/src/a.rs {name}")]
}

#[test]
fn a_capture_that_holds_the_second_dispatcher_passes() {
    for second in SECOND {
        for call in SCOPED {
            let source = format!("fn t() {{\n    let _not_alone = {second};\n    {call}(s);\n}}\n");
            assert_eq!(of(&source), Vec::<String>::new(), "{source}");
        }
    }
    // As rustfmt wraps it, after the capture, and on a future.
    let wrapped = "async fn t() {\n    let out = round.with_subscriber(s).await;\n    let _not_alone =\n        \
                   tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());\n}\n";
    assert_eq!(of(wrapped), Vec::<String>::new());
}

#[test]
fn a_capture_without_it_is_reported() {
    for call in SCOPED {
        assert_eq!(of(&format!("fn t() {{\n    {call}(s);\n}}\n")), unguarded("t"), "{call}");
    }
    assert_eq!(of("fn t() {\n    round.with_subscriber (s);\n}\n"), unguarded("t"));
    assert_eq!(
        of("fn t<F: Fn([u8; 2])>(f: F) -> [u8; 2] {\n    with_default(s, f);\n}\n"),
        unguarded("t")
    );
}

#[test]
fn a_binding_that_drops_the_second_dispatcher_at_once_is_reported() {
    for line in [
        "let _ = a_second_dispatcher();",
        "a_second_dispatcher();",
        "let _not_alone = tracing::Dispatch::none();",
        "let _not_alone = a_second_dispatcher;",
    ] {
        let source = format!("fn t() {{\n    {line}\n    set_default(s);\n}}\n");
        assert_eq!(of(&source), unguarded("t"), "{line}");
    }
}

#[test]
fn the_line_in_another_function_does_not_count() {
    let source = "fn a() {\n    let _not_alone = a_second_dispatcher();\n}\nfn b() {\n    set_default(s);\n}\n";
    assert_eq!(of(source), unguarded("b"));
}

#[test]
fn a_nested_function_neither_lends_the_line_nor_takes_the_capture() {
    // The capture and the line are the outer function's, though a nested one stands between the `fn` and them.
    let guarded = "fn logged_while(f: F) {\n    struct W;\n    impl W {\n        fn flush(&self) {}\n    }\n    \
                   let _not_alone = a_second_dispatcher();\n    with_default(s, f);\n}\n";
    assert_eq!(of(guarded), Vec::<String>::new());
    let lent = "fn outer() {\n    fn inner() {\n        let _not_alone = a_second_dispatcher();\n    }\n    \
                set_default(s);\n}\n";
    assert_eq!(of(lent), unguarded("outer"));
}

#[test]
fn what_stands_in_a_comment_or_a_literal_is_not_read() {
    let source = "fn t<'a>(x: &'a str) {\n    // set_default(s);\n    /* with_default(s, f) /* nested */ set_default(s) */\n    \
                  let s = \"set_default(\\\"{\";\n    let r = r#\"with_default(\" }\"#;\n    let c = ['{', '\\'', '}', '\\u{7b}'];\n}\n\
                  fn u() {\n    set_default(s);\n}\n";
    assert_eq!(of(source), unguarded("u"));
    // A line that is only quoted does not guard.
    let quoted =
        "fn t() {\n    // let _not_alone = a_second_dispatcher();\n    set_default(s);\n}\n";
    assert_eq!(of(quoted), unguarded("t"));
}

#[test]
fn other_names_are_not_installs() {
    let source = "fn t() {\n    engine.set_default_join_retention(h);\n    x.unwrap_or_default();\n    spec.init();\n}\n\
                  trait T {\n    fn with_subscriber(self);\n}\n";
    assert_eq!(of(source), Vec::<String>::new());
}

#[test]
fn a_global_install_that_is_not_listed_is_reported() {
    let unlisted = vec!["unlisted global install: x/src/a.rs t".to_string()];
    assert_eq!(of("fn t() {\n    tracing::subscriber::set_global_default(s);\n}\n"), unlisted);
    for call in ["init", "try_init"] {
        let source = format!("fn t() {{\n    tracing_subscriber::fmt().{call}();\n}}\n");
        assert_eq!(of(&source), unlisted, "{call}");
    }
    let source = "fn t() {\n    tracing::subscriber::set_global_default(s);\n}\n";
    let files = [("x/src/a.rs".to_string(), source.to_string())];
    assert_eq!(problems(&files, &[("x/src/a.rs", "t", "listed")]), Vec::<String>::new());
}

#[test]
fn a_listed_global_that_installs_nothing_is_reported() {
    let files = [("x/src/a.rs".to_string(), "fn t() {}\n".to_string())];
    assert_eq!(
        problems(&files, &[("x/src/a.rs", "t", "gone")]),
        vec!["stale entry, it installs no global: x/src/a.rs t".to_string()]
    );
}
