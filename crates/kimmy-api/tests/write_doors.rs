//! The write doors (ADR-272, decision 7; Q6.4 item 3, D10): a write to a name is refused while the member knows of a register
//! event that supersedes what the name answers from (`503 swap_in_progress`). That refusal is made at ONE place, the API's
//! `authorize` for an `Action::Write` (through `Engine::resolve_for_write`), and not inside the engine's writers, which also carry
//! the `$out` executor's own staging writes and the replicated apply, and must never be refused. So the door has to be complete:
//! every client write reaches `resolve_for_write`. This scans the non-test source of `exec.rs` for every call of an engine writer
//! and requires its function to authorize for `Write` (or to be the sink writer, whose plan `plan_sink` resolves for a write), and
//! requires `authorize` and `plan_sink` to reach `resolve_for_write`. The MCP's write tools call these functions (`exec::insert`,
//! `insert_many`, `update`, `delete`, `aggregate`), which is asserted too. A new door added without the refusal fails here.

use std::path::Path;

/// The engine writers a client request can reach (the ones that take a collection's metadata and write documents).
const ENGINE_WRITERS: &[&str] = &[
    "insert_versioned(",
    "insert_many_versioned(",
    "replace_if_state(",
    "delete_if_state(",
    "modify_where(",
    "find_and_modify(",
    "write_documents(",
];

/// The functions of `exec.rs` that call one, as `(function, line)`: the client write doors.
const DOORS: &[&str] = &[
    "insert",
    "insert_many",
    "replace",
    "delete_by_id",
    "update",
    "find_and_modify",
    "delete",
    "write_sink",
];

fn source_before_tests(path: &Path) -> Vec<String> {
    let text = std::fs::read_to_string(path).unwrap();
    let lines: Vec<String> = text.lines().map(str::to_string).collect();
    let end = lines
        .iter()
        .enumerate()
        .position(|(i, l)| {
            l.trim() == "#[cfg(test)]" && lines.get(i + 1).is_some_and(|n| n.contains("mod tests"))
        })
        .unwrap_or(lines.len());
    lines[..end].to_vec()
}

fn is_fn_line(l: &str) -> Option<String> {
    let rest =
        ["pub(crate) async fn ", "pub async fn ", "pub(crate) fn ", "pub fn ", "async fn ", "fn "]
            .iter()
            .find_map(|p| l.strip_prefix(p))?;
    Some(rest.split(['(', '<']).next().unwrap_or_default().to_string())
}

/// The text of the function named `name`: from its line to the next function at the left margin.
fn body_of(lines: &[String], name: &str) -> String {
    let start = lines
        .iter()
        .position(|l| is_fn_line(l).as_deref() == Some(name))
        .unwrap_or_else(|| panic!("no function `{name}` in exec.rs"));
    let len = lines[start + 1..]
        .iter()
        .position(|l| is_fn_line(l).is_some())
        .unwrap_or(lines.len() - start - 1);
    lines[start..start + 1 + len].join("\n")
}

#[test]
fn every_client_write_door_authorizes_for_write_and_the_door_refuses_a_superseded_name() {
    let exec = source_before_tests(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src/exec.rs"));
    // 1. every call of an engine writer is in a door of the list, and every door of the list authorizes for Write
    let mut found: Vec<(String, usize)> = Vec::new();
    for (i, l) in exec.iter().enumerate() {
        if l.trim_start().starts_with("//")
            || !l.contains("state.engine.")
            || !ENGINE_WRITERS.iter().any(|w| l.contains(w))
        {
            continue;
        }
        let at =
            (0..=i).rev().find(|j| is_fn_line(&exec[*j]).is_some()).expect("inside a function");
        found.push((is_fn_line(&exec[at]).unwrap(), i + 1));
    }
    let mut names: Vec<&str> = found.iter().map(|(n, _)| n.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    let mut want: Vec<&str> = DOORS.to_vec();
    want.sort_unstable();
    assert_eq!(
        names, want,
        "a client write door that is not in the list (or one that went): add it with its authorize(Write), found at {found:?}"
    );
    for door in DOORS.iter().filter(|d| **d != "write_sink") {
        let body = body_of(&exec, door);
        assert!(
            body.contains("authorize(state, auth, Action::Write"),
            "`{door}` writes documents and must authorize for Action::Write, which resolves the name for a write"
        );
    }
    // 2. the sink: the plan resolves the target for a write
    assert!(
        body_of(&exec, "plan_sink").contains("resolve_for_write("),
        "a `$merge` or an insert-only `$out` into a superseded name must be refused: plan_sink resolves its target for a write"
    );
    // 3. authorize reaches resolve_for_write for a write
    let authorize = body_of(&exec, "authorize");
    assert!(
        authorize.contains("collection_via(") && authorize.contains("Action::Write"),
        "authorize resolves for a write when the action is Write"
    );
    assert!(
        body_of(&exec, "collection_via").contains("resolve_for_write("),
        "the write form of the resolution calls Engine::resolve_for_write"
    );
    // 4. the vector writes authorize the same way
    let vectors =
        source_before_tests(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src/vectors.rs"));
    assert!(
        vectors.join("\n").contains("crate::exec::authorize(state, auth, Action::Write"),
        "the vector writes go through authorize for Write"
    );
    // 5. the MCP's write tools call the exec doors
    let mcp = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../kimmy-mcp/src/tools.rs"),
    )
    .unwrap();
    for door in [
        "exec::insert(",
        "exec::insert_many(",
        "exec::update(",
        "exec::delete(",
        "exec::aggregate(",
    ] {
        assert!(
            mcp.contains(door),
            "the MCP write tool that calls {door} is gone or goes round it"
        );
    }
}
