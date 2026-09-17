//! Every exit is named in the log, and the one that cannot be is named by
//! the next start (ADR-147).
//!
//! Real `kimmyd` processes on a single data directory, because what is
//! asserted is what an operator reads: the lines the process writes on its
//! way out, and the line the next process writes about it. A signal, a
//! `SIGKILL`, and a start that fails to bind are the three ways a run ends
//! that a test can produce on demand.
//!
//! Not ignored, unlike the cluster harness: one node with clustering off is
//! ready in well under a second, and there is no gossip timing to wait on.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod node_logs;

const JWT_SECRET: &str = "a-lifecycle-harness-jwt-secret-value";
const PATIENCE: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(100);

/// The marker `node::run` leaves in the data directory on its way out.
const LAST_EXIT_FILE: &str = "kimmy.last-exit";

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// One spawned `kimmyd` on a caller-owned data directory, so a second run
/// can start where the first left off.
struct Run {
    child: Child,
    http: u16,
    stdout: PathBuf,
}

impl Run {
    fn spawn(dir: &Path, name: &str, http: u16) -> Run {
        let config = format!(
            r#"
[server]
bind = "127.0.0.1:{http}"

[storage]
data_dir = "{data}"

[auth]
jwt_secret = "{JWT_SECRET}"
"#,
            data = dir.join("data").display(),
        );
        let config_path = dir.join(format!("{name}.toml"));
        std::fs::write(&config_path, config).unwrap();
        let stdout = dir.join(format!("{name}.stdout.log"));
        let stderr = dir.join(format!("{name}.stderr.log"));

        let child = Command::new(env!("CARGO_BIN_EXE_kimmyd"))
            .arg("--config")
            .arg(&config_path)
            .env("KIMMY_ROOT_PASSWORD", "harness-root-password")
            .env_remove("RUST_LOG")
            .stdout(Stdio::from(std::fs::File::create(&stdout).unwrap()))
            .stderr(Stdio::from(std::fs::File::create(stderr).unwrap()))
            .spawn()
            .expect("spawning kimmyd");
        Run { child, http, stdout }
    }

    async fn wait_ready(&self, client: &reqwest::Client) {
        let deadline = Instant::now() + PATIENCE;
        loop {
            if let Ok(res) =
                client.get(format!("http://127.0.0.1:{}/healthz", self.http)).send().await
                && res.status().is_success()
            {
                return;
            }
            assert!(Instant::now() < deadline, "never became healthy; log: {}", self.log());
            tokio::time::sleep(POLL).await;
        }
    }

    fn signal(&self, sig: &str) {
        let status = Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(self.child.id().to_string())
            .status()
            .unwrap();
        assert!(status.success(), "kill -{sig} {}", self.child.id());
    }

    /// Wait for the process to end, and report its status.
    fn wait_exit(&mut self) -> std::process::ExitStatus {
        let deadline = Instant::now() + PATIENCE;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "did not exit; log: {}", self.log());
            std::thread::sleep(POLL);
        }
    }

    /// The log as written, with the pretty format's colour escapes removed
    /// so a field reads as `name=value` the way it does on a terminal.
    fn log(&self) -> String {
        let raw = std::fs::read_to_string(&self.stdout).unwrap_or_default();
        let mut out = String::with_capacity(raw.len());
        let mut chars = raw.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\x1b' && chars.peek() == Some(&'[') {
                // CSI: everything up to and including the final letter.
                for c in chars.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        let _ = Command::new("kill").arg("-KILL").arg(self.child.id().to_string()).status();
        let _ = self.child.wait();
        // The caller's scratch directory goes when the test ends; a failure
        // keeps this run's logs past it.
        let stderr = self.stdout.with_extension("").with_extension("stderr.log");
        let name = self.stdout.file_name().and_then(|n| n.to_str()).unwrap_or("run");
        let name = name.trim_end_matches(".stdout.log");
        node_logs::keep_if_failing(
            &node_logs::destination(),
            name,
            self.child.id(),
            &[&self.stdout, &stderr],
        );
    }
}

fn marker(dir: &Path) -> Option<String> {
    std::fs::read_to_string(dir.join("data").join(LAST_EXIT_FILE)).ok()
}

#[tokio::test]
async fn a_graceful_shutdown_logs_both_lines_and_leaves_a_marker_the_next_start_reads() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();

    let mut first = Run::spawn(dir.path(), "first", free_port());
    first.wait_ready(&client).await;
    assert!(marker(dir.path()).is_none(), "a running node has no marker");

    first.signal("TERM");
    let status = first.wait_exit();
    assert!(status.success(), "{status:?}");
    let log = first.log();
    assert!(log.contains("shutdown signal received, draining"), "{log}");
    assert!(log.contains("shutdown complete"), "{log}");
    // The first start in an empty directory has nothing to warn about.
    assert!(!log.contains("previous run"), "{log}");

    let marker = marker(dir.path()).expect("a graceful shutdown leaves the marker");
    assert!(marker.contains("exit = \"shutdown\""), "{marker}");
    assert!(marker.contains(&format!("pid = {}", first.child.id())), "{marker}");

    // The next start reads it, says so, and consumes it.
    let mut second = Run::spawn(dir.path(), "second", free_port());
    second.wait_ready(&client).await;
    let log = second.log();
    assert!(log.contains("previous run ended cleanly"), "{log}");
    assert!(log.contains("exit=\"shutdown\"") || log.contains("exit=shutdown"), "{log}");
    assert!(!log.contains("did not shut down cleanly"), "{log}");
    assert!(marker_absent(dir.path()), "the marker is consumed by the start that reads it");

    second.signal("TERM");
    assert!(second.wait_exit().success());
}

fn marker_absent(dir: &Path) -> bool {
    marker(dir).is_none()
}

#[tokio::test]
async fn a_start_after_a_kill_warns_that_the_previous_run_did_not_shut_down_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();

    let mut first = Run::spawn(dir.path(), "first", free_port());
    first.wait_ready(&client).await;
    // The exit the field saw: no signal the process could log, nothing
    // written on the way out.
    first.signal("KILL");
    let status = first.wait_exit();
    assert!(!status.success(), "{status:?}");
    let log = first.log();
    assert!(!log.contains("shutdown"), "a killed process logs no shutdown: {log}");
    assert!(marker_absent(dir.path()), "a killed process leaves no marker");

    let mut second = Run::spawn(dir.path(), "second", free_port());
    second.wait_ready(&client).await;
    let log = second.log();
    let line = log
        .lines()
        .find(|l| l.contains("previous run did not shut down cleanly"))
        .unwrap_or_else(|| panic!("no warning in the log:\n{log}"));
    assert!(line.contains("WARN"), "{line}");
    assert!(line.contains("last_database_write_secs_ago="), "{line}");
    // The warning sits under the banner of the run reporting it.
    let banner = log.find("starting kimmyd").expect("a banner");
    assert!(log.find("did not shut down cleanly").unwrap() > banner, "{log}");

    second.signal("TERM");
    assert!(second.wait_exit().success());
    assert!(marker(dir.path()).is_some(), "this run's own exit is recorded");
}

#[tokio::test]
async fn a_start_that_fails_logs_its_exit_and_the_next_start_does_not_call_it_unclean() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();

    // Hold the port, so the node opens its database and then cannot bind.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = taken.local_addr().unwrap().port();
    let mut failed = Run::spawn(dir.path(), "failed", port);
    let status = failed.wait_exit();
    assert!(!status.success(), "{status:?}");
    let log = failed.log();
    assert!(log.contains("exiting on an error"), "{log}");
    assert!(log.contains("binding"), "the line carries the error's text: {log}");
    let marker = marker(dir.path()).expect("an error exit records itself");
    assert!(marker.contains("exit = \"error\""), "{marker}");
    drop(taken);

    let mut next = Run::spawn(dir.path(), "next", free_port());
    next.wait_ready(&client).await;
    let log = next.log();
    assert!(log.contains("previous run ended cleanly"), "{log}");
    assert!(log.contains("exit=\"error\"") || log.contains("exit=error"), "{log}");
    assert!(!log.contains("did not shut down cleanly"), "{log}");

    next.signal("TERM");
    assert!(next.wait_exit().success());
}
