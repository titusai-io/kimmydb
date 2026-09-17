//! A spawned `kimmyd`'s logs, kept when the test that spawned it fails.
//!
//! The harnesses write each node's stdout and stderr into a scratch
//! directory that is deleted when the test ends, pass or fail. A node's own
//! account of a failure — a refused peer connection, a panic in a connection
//! task — is in those files and nowhere else, so a red test on CI left
//! nothing to read, and "flake or regression?" was answered by rerunning.
//! Shared by the test binaries that spawn nodes, as a module rather than a
//! test target of its own.

use std::path::{Path, PathBuf};

/// Where kept logs go: `KIMMY_TEST_NODE_LOGS` when set, which CI uploads as
/// an artifact of a failed job, and `kimmy-node-logs` under the system temp
/// directory otherwise.
pub fn destination() -> PathBuf {
    std::env::var_os("KIMMY_TEST_NODE_LOGS")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("kimmy-node-logs"))
}

/// Copy `logs` to `<into>/<test>/<node>-<pid>/` when this thread is unwinding
/// from a failed assertion, and say where on stderr. Returns where they went,
/// or `None` when the test has not failed and nothing was kept.
///
/// For a `Drop` impl, after the node is killed so the files are complete.
/// It never panics: a panic inside a drop that runs during an unwind aborts
/// the whole test binary, which would lose the failure it is here to explain.
pub fn keep_if_failing(into: &Path, node: &str, pid: u32, logs: &[&Path]) -> Option<PathBuf> {
    if !std::thread::panicking() {
        return None;
    }
    // libtest names the thread a test runs on after the test, and a tokio
    // test's body and its nodes' drops run on that thread.
    let test = std::thread::current().name().unwrap_or("unnamed").replace("::", "-");
    let to = into.join(test).join(format!("{node}-{pid}"));
    std::fs::create_dir_all(&to).ok()?;
    for log in logs {
        if let Some(name) = log.file_name() {
            let _ = std::fs::copy(log, to.join(name));
        }
    }
    let _ = std::io::Write::write_all(
        &mut std::io::stderr(),
        format!("{node}'s logs kept in {}\n", to.display()).as_bytes(),
    );
    Some(to)
}
