//! The one door from a logical name to a collection (ADR-272, Q6.4 item 0): the API and the MCP resolve a name through
//! `Engine::resolve`, so a name that a swap rebound is never read or written as the physical it used to be. A caller that goes
//! round the door reads the old physical across a flip. The non-test callers of `get_collection(` in `kimmy-api` and `kimmy-mcp`
//! are counted per file: a new one fails here until it is routed through `resolve` or listed with its reason.

use std::path::Path;

/// (file, callers, why they may ask for a physical by name). These address the node's own internal collections or a physical that
/// the caller names itself, never a client's logical name.
const PHYSICAL_BY_NAME: &[(&str, usize, &str)] = &[
    ("topology.rs", 1, "the node registry: an internal collection"),
    ("webhooks.rs", 1, "the webhook registry: an internal collection"),
    ("dispatch.rs", 5, "the webhook registry and its progress rows: internal collections"),
    (
        "validator.rs",
        1,
        "reads whether a validator was set, after the schema change was addressed by name and authorised",
    ),
    (
        "vectors.rs",
        2,
        "the shadow collection and the vector configuration, addressed by derived physical names",
    ),
    (
        "exec.rs",
        3,
        "the two index paths and the `$out` target's existence check, which address the named collection itself",
    ),
];

fn non_test_callers(dir: &Path) -> Vec<(String, usize)> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if path.extension().is_none_or(|e| e != "rs") || name.ends_with("_tests.rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        // everything before the file's test module: the first `#[cfg(test)]` that opens a `mod`
        let lines: Vec<&str> = text.lines().collect();
        let end = lines
            .iter()
            .enumerate()
            .position(|(i, l)| {
                l.trim() == "#[cfg(test)]"
                    && lines.get(i + 1).is_some_and(|next| next.contains("mod tests"))
            })
            .unwrap_or(lines.len());
        let body = lines[..end].join("\n");
        let count = body
            .lines()
            .filter(|l| !l.trim_start().starts_with("//") && l.contains("get_collection("))
            .count();
        if count > 0 {
            found.push((name, count));
        }
    }
    found.sort();
    found
}

#[test]
fn no_data_door_of_the_api_or_the_mcp_resolves_a_logical_name_without_the_register() {
    let api = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mcp = Path::new(env!("CARGO_MANIFEST_DIR")).join("../kimmy-mcp/src");
    let mut found = non_test_callers(&api);
    found.extend(non_test_callers(&mcp));
    let mut allowed: Vec<(String, usize)> =
        PHYSICAL_BY_NAME.iter().map(|(file, n, _)| (file.to_string(), *n)).collect();
    allowed.sort();
    assert_eq!(
        found, allowed,
        "a caller of `get_collection(` outside the list: resolve the name through `Engine::resolve`, or add the file here with the reason"
    );
}
