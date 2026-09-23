//! Reading this workspace's production source, for the guards that check it.
//!
//! Shared by `supervision.rs` and `walks_leave_the_worker.rs`, because both
//! ask the same first question — which lines of a file are production code —
//! and an answer written twice drifts. The walk it replaced in the second guard
//! cut each file at its first `#[cfg(test)]`, which in `sessions.rs` sits on a
//! test-only method, so everything after it went unread; the reasons that cut
//! is wrong are set out on `production_sources` in `supervision.rs`.

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
