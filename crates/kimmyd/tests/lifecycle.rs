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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

mod node_logs;
mod ports;

const JWT_SECRET: &str = "a-lifecycle-harness-jwt-secret-value";
const PATIENCE: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(100);

/// The marker `node::run` leaves in the data directory on its way out.
const LAST_EXIT_FILE: &str = "kimmy.last-exit";

/// One spawned `kimmyd` on a caller-owned data directory, so a second run
/// can start where the first left off.
struct Run {
    /// Behind a lock so `wait_ready`, which only reads the run, can ask
    /// whether it has exited.
    child: std::sync::Mutex<Child>,
    pid: u32,
    /// The HTTP port the node bound: port 0 unless a test names one, read
    /// from its log once it listens.
    http: std::sync::OnceLock<u16>,
    stdout: PathBuf,
}

impl Run {
    /// Start a node on `dir` that binds its own HTTP port: none is chosen
    /// here, so none can be taken before the node binds it.
    fn spawn(dir: &Path, name: &str) -> Run {
        Run::spawn_with(dir, name, &[])
    }

    /// [`Run::spawn`], with extra environment — for `KIMMY_TEST_KILL_TASK`,
    /// which stops a named background task once the node is serving (ADR-184).
    fn spawn_with(dir: &Path, name: &str, env: &[(&str, &str)]) -> Run {
        Run::spawn_on_with(dir, name, 0, env)
    }

    /// [`Run::spawn`] on a port the test names, for a test about that port.
    fn spawn_on(dir: &Path, name: &str, http: u16) -> Run {
        Run::spawn_on_with(dir, name, http, &[])
    }

    fn spawn_on_with(dir: &Path, name: &str, http: u16, extra_env: &[(&str, &str)]) -> Run {
        Run::spawn_full(dir, name, http, extra_env, "", None, false, "")
    }

    /// [`Run::spawn_with`], with the node's stderr a pipe whose reader is
    /// already closed, so every write the node makes to it fails with EPIPE:
    /// the stderr a process has when whatever was reading it has gone.
    fn spawn_with_stderr_closed(dir: &Path, name: &str, env: &[(&str, &str)]) -> Run {
        Run::spawn_full(dir, name, 0, env, "", None, true, "")
    }

    /// [`Run::spawn`], with `storage` added to the config's `[storage]`.
    fn spawn_with_storage(dir: &Path, name: &str, storage: &str) -> Run {
        Run::spawn_full(dir, name, 0, &[], storage, None, false, "")
    }

    /// [`Run::spawn`], with the process allowed `nofile` file descriptors.
    fn spawn_with_fd_limit(dir: &Path, name: &str, nofile: u32) -> Run {
        Run::spawn_full(dir, name, 0, &[], "", Some(nofile), false, "")
    }

    /// [`Run::spawn`] as a member of a cluster whose only seed is `seed`: a port
    /// nothing listens on, so no peer is ever reached. `wait_secs` is
    /// `cluster.catch_up_wait_secs`.
    fn spawn_clustered(dir: &Path, name: &str, seed: u16, wait_secs: u64) -> Run {
        Run::spawn_clustered_on(dir, name, ports::choose(), &[seed], wait_secs)
    }

    /// [`Run::spawn_clustered`] on the cluster port `cluster`, seeded with `seeds`
    /// (one of which may be its own).
    fn spawn_clustered_on(
        dir: &Path,
        name: &str,
        cluster: u16,
        seeds: &[u16],
        wait_secs: u64,
    ) -> Run {
        Run::spawn_clustered_at(dir, name, "127.0.0.1", cluster, seeds, wait_secs)
    }

    /// [`Run::spawn_clustered_on`], binding the cluster port on `host`: a
    /// wildcard bind is reached by a seed that names it by any address of its own.
    fn spawn_clustered_at(
        dir: &Path,
        name: &str,
        host: &str,
        cluster: u16,
        seeds: &[u16],
        wait_secs: u64,
    ) -> Run {
        let seeds =
            seeds.iter().map(|p| format!("\"127.0.0.1:{p}\"")).collect::<Vec<_>>().join(", ");
        let tail = format!(
            r#"
[cluster]
enabled = true
bind = "{host}:{cluster}"
seeds = [{seeds}]
cluster_secret = "a-lifecycle-harness-cluster-secret"
sync_interval_secs = 1
discovery_interval_secs = 2
catch_up_wait_secs = {wait_secs}
"#
        );
        Run::spawn_full(dir, name, 0, &[], "", None, false, &tail)
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_full(
        dir: &Path,
        name: &str,
        http: u16,
        extra_env: &[(&str, &str)],
        storage: &str,
        nofile: Option<u32>,
        stderr_closed: bool,
        tail: &str,
    ) -> Run {
        let config = format!(
            r#"
[server]
bind = "127.0.0.1:{http}"

[storage]
data_dir = "{data}"
{storage}

[auth]
jwt_secret = "{JWT_SECRET}"
{tail}"#,
            data = dir.join("data").display(),
        );
        let config_path = dir.join(format!("{name}.toml"));
        std::fs::write(&config_path, config).unwrap();
        let stdout = dir.join(format!("{name}.stdout.log"));
        let stderr = dir.join(format!("{name}.stderr.log"));

        // A descriptor limit through the shell, which `exec`s the node so it
        // keeps the pid the harness signals.
        let mut command = match nofile {
            None => Command::new(env!("CARGO_BIN_EXE_kimmyd")),
            Some(nofile) => {
                let mut sh = Command::new("/bin/sh");
                sh.arg("-c")
                    .arg(format!("ulimit -n {nofile} && exec \"$0\" \"$@\""))
                    .arg(env!("CARGO_BIN_EXE_kimmyd"));
                sh
            }
        };
        for (key, value) in extra_env {
            command.env(key, value);
        }
        let child = command
            .arg("--config")
            .arg(&config_path)
            .env("KIMMY_ROOT_PASSWORD", "harness-root-password")
            .env_remove("RUST_LOG")
            .stdout(Stdio::from(std::fs::File::create(&stdout).unwrap()))
            .stderr(if stderr_closed {
                Stdio::piped()
            } else {
                Stdio::from(std::fs::File::create(stderr).unwrap())
            })
            .spawn()
            .expect("spawning kimmyd");
        let mut child = child;
        // The reader goes at once, so the node's first write to stderr fails.
        drop(child.stderr.take());
        let pid = child.id();
        let bound = std::sync::OnceLock::new();
        if http != 0 {
            let _ = bound.set(http);
        }
        Run { child: std::sync::Mutex::new(child), pid, http: bound, stdout }
    }

    /// Wait until the node answers `/readyz`, which it does only once its store
    /// is open (it answers `/healthz` from the bind, ADR-198), failing at once, with its
    /// log, if it exits first rather than waiting out `PATIENCE`.
    async fn wait_ready(&self, client: &reqwest::Client) {
        if let Err(why) = self.try_ready(client, ports::BOUND_HTTP_LINE).await {
            panic!("{why}");
        }
    }

    /// [`Run::wait_ready`], reading the HTTP port from `line`, and answering
    /// at once, never after `PATIENCE`, when the node exits or the port cannot
    /// be read from the log.
    async fn try_ready(&self, client: &reqwest::Client, line: &str) -> Result<(), String> {
        let deadline = Instant::now() + PATIENCE;
        let mut line_wait = ports::LineWait::default();
        loop {
            if self.http.get().is_none() {
                let bound = ports::bound_http_port(&self.stdout, line, self.pid, &[]);
                match line_wait.judge(bound, line) {
                    Ok(Some(port)) => {
                        let _ = self.http.set(port);
                    }
                    Ok(None) => {}
                    Err(why) => return Err(format!("{why}; log: {}", self.log())),
                }
            }
            if let Some(port) = self.http.get()
                && let Ok(res) = client.get(format!("http://127.0.0.1:{port}/readyz")).send().await
                && res.status().is_success()
            {
                return Ok(());
            }
            if let Ok(Some(status)) = self.child.lock().unwrap().try_wait() {
                return Err(format!(
                    "exited ({status}) before it became healthy; log: {}",
                    self.log()
                ));
            }
            assert!(Instant::now() < deadline, "never became healthy; log: {}", self.log());
            tokio::time::sleep(POLL).await;
        }
    }

    fn signal(&self, sig: &str) {
        let status =
            Command::new("kill").arg(format!("-{sig}")).arg(self.pid.to_string()).status().unwrap();
        assert!(status.success(), "kill -{sig} {}", self.pid);
    }

    /// Wait for the process to end, and report its status.
    fn wait_exit(&mut self) -> std::process::ExitStatus {
        let deadline = Instant::now() + PATIENCE;
        loop {
            if let Some(status) = self.child.get_mut().unwrap().try_wait().unwrap() {
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
        let _ = Command::new("kill").arg("-KILL").arg(self.pid.to_string()).status();
        if let Ok(child) = self.child.get_mut() {
            let _ = child.wait();
        }
        // The caller's scratch directory goes when the test ends; a failure
        // keeps this run's logs past it.
        let stderr = self.stdout.with_extension("").with_extension("stderr.log");
        let name = self.stdout.file_name().and_then(|n| n.to_str()).unwrap_or("run");
        let name = name.trim_end_matches(".stdout.log");
        node_logs::keep_if_failing(
            &node_logs::destination(),
            name,
            self.pid,
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

    let mut first = Run::spawn(dir.path(), "first");
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
    assert!(marker.contains(&format!("pid = {}", first.pid)), "{marker}");

    // The next start reads it, says so, and consumes it.
    let mut second = Run::spawn(dir.path(), "second");
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

    let mut first = Run::spawn(dir.path(), "first");
    first.wait_ready(&client).await;
    // The exit the field saw: no signal the process could log, nothing
    // written on the way out.
    first.signal("KILL");
    let status = first.wait_exit();
    assert!(!status.success(), "{status:?}");
    let log = first.log();
    assert!(!log.contains("shutdown"), "a killed process logs no shutdown: {log}");
    assert!(marker_absent(dir.path()), "a killed process leaves no marker");

    let mut second = Run::spawn(dir.path(), "second");
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
    let mut failed = Run::spawn_on(dir.path(), "failed", port);
    let status = failed.wait_exit();
    assert!(!status.success(), "{status:?}");
    let log = failed.log();
    assert!(log.contains("exiting on an error"), "{log}");
    assert!(log.contains("binding"), "the line carries the error's text: {log}");
    let marker = marker(dir.path()).expect("an error exit records itself");
    assert!(marker.contains("exit = \"error\""), "{marker}");
    drop(taken);

    let mut next = Run::spawn(dir.path(), "next");
    next.wait_ready(&client).await;
    let log = next.log();
    // Named for what it was, at WARN: not "ended cleanly", which an error is
    // not, and not unclean, which a start that logged its exit is not either.
    let line = log
        .lines()
        .find(|l| l.contains("the previous start failed before it served"))
        .unwrap_or_else(|| panic!("no failed-start line:\n{log}"));
    assert!(line.contains("WARN"), "{line}");
    assert!(line.contains("binding"), "with the error's text: {line}");
    assert!(!log.contains("previous run ended cleanly"), "{log}");
    assert!(!log.contains("did not shut down cleanly"), "{log}");

    next.signal("TERM");
    assert!(next.wait_exit().success());
}

/// The review's L1: a start that reaches serving settles what it inherited.
/// `.previous` is gone once it serves, and its own clean stop carries nothing
/// and is not a failed start.
#[tokio::test]
async fn a_start_that_serves_settles_what_it_inherited() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut first = Run::spawn(dir.path(), "first");
    first.wait_ready(&client).await;
    first.signal("TERM");
    assert!(first.wait_exit().success());

    let mut second = Run::spawn(dir.path(), "second");
    second.wait_ready(&client).await;
    assert!(
        !dir.path().join("kimmy.last-exit.previous").exists(),
        "serving removed what the start set aside"
    );
    second.signal("TERM");
    assert!(second.wait_exit().success());
    let marker = marker(dir.path()).expect("the clean stop is recorded");
    assert!(!marker.contains("previous") && !marker.contains("failed_start"), "{marker}");
}

/// Hold a port, so a start cannot bind and fails before it opens its
/// database (the bind comes first, ADR-198); return the start's log.
fn a_start_that_fails_to_bind(dir: &Path, name: &str) -> String {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = taken.local_addr().unwrap().port();
    let mut failed = Run::spawn_on(dir, name, port);
    assert!(!failed.wait_exit().success());
    failed.log()
}

/// Round 0380's sequence, with a start that fails to bind in place of an
/// older build refusing the store: a run killed, then a start that fails
/// before it serves, then a start. The last one reports both, the failed
/// start and the unclean end before it. The failed start used to replace the
/// evidence of the kill.
#[tokio::test]
async fn a_kill_then_a_failed_start_then_a_start_reports_both() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();

    let mut first = Run::spawn(dir.path(), "killed");
    first.wait_ready(&client).await;
    first.signal("KILL");
    assert!(!first.wait_exit().success());

    let failed = a_start_that_fails_to_bind(dir.path(), "failed");
    assert!(failed.contains("did not shut down cleanly"), "the failed start saw it: {failed}");
    let marker = marker(dir.path()).expect("the failed start records itself");
    assert!(marker.contains("[previous]") && marker.contains("unclean"), "{marker}");

    let mut next = Run::spawn(dir.path(), "next");
    next.wait_ready(&client).await;
    let log = next.log();
    assert!(log.contains("the previous start failed before it served"), "{log}");
    assert!(log.contains("and the run before it did not shut down cleanly"), "{log}");

    next.signal("TERM");
    assert!(next.wait_exit().success());
}

/// A clean stop, then a failed start: the clean stop is kept, as what came
/// before the failure.
#[tokio::test]
async fn a_clean_stop_then_a_failed_start_keeps_the_clean_stop() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();

    let mut first = Run::spawn(dir.path(), "stopped");
    first.wait_ready(&client).await;
    first.signal("TERM");
    assert!(first.wait_exit().success());

    a_start_that_fails_to_bind(dir.path(), "failed");

    let mut next = Run::spawn(dir.path(), "next");
    next.wait_ready(&client).await;
    let log = next.log();
    assert!(log.contains("the previous start failed before it served"), "{log}");
    assert!(log.contains("and before that, the run ended with exit shutdown"), "{log}");
    assert!(!log.contains("did not shut down cleanly"), "{log}");

    next.signal("TERM");
    assert!(next.wait_exit().success());
}

/// A second kimmyd started on a live data directory leaves it exactly as it
/// is: it refuses before reading the marker, so the live node's own set-aside
/// marker and its next exit are untouched. Before, it set the marker aside,
/// failed at redb's lock, and wrote a failed start there, and a later kill of
/// the live node was reported as "the previous start failed".
#[tokio::test]
async fn a_second_start_on_a_live_directory_leaves_its_markers_alone() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let data = dir.path().join("data");

    let mut stopped = Run::spawn(dir.path(), "stopped");
    stopped.wait_ready(&client).await;
    stopped.signal("TERM");
    assert!(stopped.wait_exit().success());

    let mut live = Run::spawn(dir.path(), "live");
    live.wait_ready(&client).await;
    // Put back what a start that has not settled holds, so the test sees
    // whether the second start disturbs a set-aside marker as well.
    let aside = data.join("kimmy.last-exit.previous");
    std::fs::write(&aside, "exit = \"shutdown\"\n").unwrap();
    let listing = |d: &Path| {
        let mut names: Vec<_> = std::fs::read_dir(d)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    };
    let before = (listing(&data), std::fs::read(&aside).unwrap());

    let mut second = Run::spawn(dir.path(), "second");
    assert!(!second.wait_exit().success());
    let log = second.log();
    assert!(log.contains("is in use by another kimmyd"), "{log}");
    assert!(!log.contains("previous run"), "it read nothing: {log}");
    assert_eq!((listing(&data), std::fs::read(&aside).unwrap()), before);
    assert!(marker(dir.path()).is_none(), "no marker is written for the live node");

    std::fs::remove_file(&aside).unwrap();
    live.signal("KILL");
    assert!(!live.wait_exit().success());
    let mut next = Run::spawn(dir.path(), "next");
    next.wait_ready(&client).await;
    let log = next.log();
    assert!(log.contains("previous run did not shut down cleanly"), "{log}");
    assert!(!log.contains("the previous start failed"), "{log}");
    next.signal("TERM");
    assert!(next.wait_exit().success());
}

/// A store held by a process that does not hold the data directory, such as a
/// build from before the directory was held: the start that meets it puts
/// back the marker it set aside and writes none, so the clean stop before it
/// is still what the next start reads.
#[tokio::test]
async fn a_start_that_finds_the_store_held_leaves_the_marker_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let data = dir.path().join("data");

    let mut stopped = Run::spawn(dir.path(), "stopped");
    stopped.wait_ready(&client).await;
    stopped.signal("TERM");
    assert!(stopped.wait_exit().success());
    let before = marker(dir.path()).expect("a clean stop leaves a marker");

    let holder = kimmy_storage::Engine::open(&data.join("kimmy.redb")).unwrap();
    let mut refused = Run::spawn(dir.path(), "refused");
    assert!(!refused.wait_exit().success());
    let log = refused.log();
    assert!(log.contains("is open in another process"), "{log}");
    drop(holder);
    assert_eq!(marker(dir.path()), Some(before), "the marker is back, unchanged");
    assert!(!data.join("kimmy.last-exit.previous").exists());

    let mut next = Run::spawn(dir.path(), "next");
    next.wait_ready(&client).await;
    let log = next.log();
    assert!(!log.contains("did not shut down cleanly"), "{log}");
    assert!(!log.contains("the previous start failed"), "{log}");
    next.signal("TERM");
    assert!(next.wait_exit().success());
}

/// ADR-190 through the shipped binary: a store stamped by a newer build is
/// refused at start, and neither the database nor its sidecar changes. Before,
/// a refused start opened the store read-write first, and round 0380 saw the
/// file change on a killed store and on a cleanly stopped one.
#[tokio::test]
async fn a_store_a_newer_build_wrote_is_refused_and_left_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut first = Run::spawn(dir.path(), "first");
    first.wait_ready(&client).await;
    first.signal("TERM");
    assert!(first.wait_exit().success());

    let database = dir.path().join("data").join("kimmy.redb");
    let sidecar = dir.path().join("data").join("kimmy.format");
    let stamped = std::fs::read_to_string(&sidecar).expect("the store has a sidecar");
    let newer = stamped
        .lines()
        .map(
            |l| if l.starts_with("schema = ") { "schema = 250".to_string() } else { l.to_string() },
        )
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&sidecar, format!("{newer}\n")).unwrap();
    let before = (std::fs::read(&database).unwrap(), std::fs::read(&sidecar).unwrap());

    let mut refused = Run::spawn(dir.path(), "refused");
    assert!(!refused.wait_exit().success());
    let log = refused.log();
    assert!(log.contains("is not opened by this build"), "{log}");
    assert!(log.contains("storage schema 250"), "the log names what is newer: {log}");
    let after = (std::fs::read(&database).unwrap(), std::fs::read(&sidecar).unwrap());
    assert!(before == after, "the database and its sidecar are exactly as they were");
}

/// Reading a node's HTTP port from its log couples the harness to that line.
/// A line renamed, or one without a readable `bind=`, must fail the wait at
/// once, not after `PATIENCE`. Both are simulated by asking for a line the
/// node does not log, and for one it logs without a `bind=`.
#[tokio::test]
async fn a_port_line_the_harness_cannot_read_fails_at_once() {
    let client = reqwest::Client::new();
    for (line, says) in
        [("a line kimmyd never logs", "never logged"), ("starting kimmyd", "without a bind=ADDR")]
    {
        let dir = tempfile::tempdir().unwrap();
        let run = Run::spawn(dir.path(), "unreadable");
        let started = Instant::now();
        let answer = run.try_ready(&client, line).await;
        let why = answer.expect_err("the port cannot be read, so the wait fails");
        assert!(why.contains(says), "{why}");
        assert!(
            started.elapsed() < PATIENCE / 4,
            "answered in {:?}, not at once: {why}",
            started.elapsed()
        );
    }
}

/// A supervised task that dies stops the process, and the next start says which
/// task and how (ADR-184).
///
/// Driven through a real `kimmyd` and the exit status it actually returns,
/// because the whole defect was that a task could die and the process not
/// notice: a test that called the supervisor directly would prove the classifier
/// and nothing about the node.
///
/// `KIMMY_TEST_KILL_TASK` is armed only once the node is serving, so a start
/// cannot become a crash loop. **That is read from the finished log, not from a
/// readiness check**: waiting for `/healthz` first raced the kill and failed as
/// "exited (70) before it became healthy", which is the thing this proves.
async fn a_task_that_dies_exits_and_the_next_start_names_it(
    label: &str,
    kill: &str,
    task: &str,
    cause: &str,
) {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();

    let mut first =
        Run::spawn_with(dir.path(), label, &[("KIMMY_TEST_KILL_TASK", &format!("{task}:{kill}"))]);

    // **Nothing is waited for here, deliberately.** This used to call
    // `wait_ready`, which raced the switch: the kill fires a fixed grace after
    // the node starts serving, and on a loaded machine the harness's first
    // `/healthz` could land after the process had already exited 70 — so the
    // test failed with "exited (70) before it became healthy", which is the
    // thing it is trying to prove. Both of these failed that way in one of a
    // reviewer's two full runs.
    //
    // Everything readiness was standing in for is asserted from the finished
    // log below, where there is no race left to lose: the node got as far as
    // serving, the switch announced itself, and only then did the death happen.
    let status = first.wait_exit();
    assert_eq!(
        status.code(),
        Some(70),
        "a supervised death exits 70, distinct from a configuration error's 1: {status:?}"
    );
    let log = first.log();
    assert!(
        log.contains("serving HTTP"),
        "the node must have reached serving before the switch fired, which is the switch's own \
         contract: {log}"
    );
    assert!(
        log.contains("a test switch is set"),
        "the switch must announce itself on every start where it is set: {log}"
    );
    assert!(log.contains("stopping the process so it is restarted"), "{log}");
    assert!(log.contains(task), "the log names the task: {log}");

    let marker = marker(dir.path()).expect("a task death leaves the marker");
    assert!(marker.contains("exit = \"task_died\""), "{marker}");
    assert!(marker.contains(task), "the marker names the task: {marker}");
    assert!(marker.contains(cause), "the marker names the cause: {marker}");

    // The next start is where an operator actually reads why, so that is part of
    // the claim rather than a separate nicety.
    let mut second = Run::spawn(dir.path(), &format!("{label}-second"));
    second.wait_ready(&client).await;
    let log = second.log();
    assert!(log.contains("stopped itself because a background task ended"), "{log}");
    assert!(log.contains(task), "{log}");
    assert!(log.contains(cause), "{log}");
    assert!(!log.contains("ended cleanly"), "a death is not a clean end: {log}");

    second.signal("TERM");
    assert!(second.wait_exit().success());
}

#[tokio::test]
async fn a_panicking_background_task_exits_the_process() {
    // The session invalidator, because it is the one whose death is a security
    // defect: while it is dead a revoked token keeps working on this node.
    a_task_that_dies_exits_and_the_next_start_names_it(
        "panicking",
        "panic",
        "session_invalidator",
        "panicked",
    )
    .await;
}

#[tokio::test]
async fn a_panicking_drop_purger_exits_the_process() {
    // The drop purger (ADR-189), started on every node whatever
    // `storage.gc_interval_secs` says: while it is dead, what a drop left stays
    // on disk and the name cannot be created again, so its death must stop the
    // process like any other writer's.
    a_task_that_dies_exits_and_the_next_start_names_it(
        "purger-panicking",
        "panic",
        "drop_purger",
        "panicked",
    )
    .await;
}

#[tokio::test]
async fn a_background_task_that_returns_exits_the_process() {
    // The stall probe, whose death makes `kimmy_runtime_stall_seconds` read 0 --
    // "no stall" -- for ever: a signal that cannot fail.
    a_task_that_dies_exits_and_the_next_start_names_it(
        "returning",
        "return",
        "stall_probe",
        "returned",
    )
    .await;
}

#[tokio::test]
async fn a_test_switch_that_will_do_nothing_says_so() {
    // The switch used to be announced only when it *parsed*: a typo in the
    // value, or a task name that matches nothing, produced no line at all. A
    // test that thought it had armed a kill then watched a node shut down
    // normally with no clue why, which is a worse failure than the switch not
    // existing.
    for (value, expect) in [
        ("embedding_worker", "malformed"),
        ("embeding_worker:panic", "matches no task"),
        ("membership_inbound:explode", "malformed"),
        // `error` asks for a retry, which only a retrying task can give.
        ("session_invalidator:error", "error mode only acts on a retrying task"),
        // A real task, on a node that does not run it: this harness starts a
        // single node, so nothing gossips membership.
        ("membership_inbound:panic", "did not start"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let mut node = Run::spawn_with(dir.path(), "inert", &[("KIMMY_TEST_KILL_TASK", value)]);
        node.wait_ready(&client).await;
        let log = node.log();
        node.signal("TERM");
        let status = node.wait_exit();

        assert!(
            log.contains(expect),
            "a switch set to {value:?} will do nothing, and the start must say which: {log}"
        );
        assert!(log.contains(value), "and quote it back: {log}");
        assert!(
            status.success(),
            "a switch that does nothing must not stop the node either: {status:?}"
        );
    }
}

#[tokio::test]
async fn a_task_asked_to_fail_retries_in_place_and_the_node_stays_up() {
    // The third of `KIMMY_TEST_KILL_TASK`'s three shapes, end to end, and the
    // one that had never worked: `:error` announced itself at WARN on every
    // start and then did nothing at all. Twice over, in fact -- nothing read
    // `Kill::Error` at all, and the obvious place to read it (the top of the
    // retry loop) is never reached again, because the embedding worker's `run`
    // does not return while it is working.
    //
    // Through a real node rather than at unit level, because the claim is about
    // the one task in the node that retries, and it is on by default
    // (`vector.worker_enabled`). The node staying up is half the assertion: a
    // transient error must not be a death.
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();

    let mut node =
        Run::spawn_with(dir.path(), "retry", &[("KIMMY_TEST_KILL_TASK", "embedding_worker:error")]);
    node.wait_ready(&client).await;

    // Arming happens when the node starts serving, and the switch waits
    // TEST_KILL_GRACE after that, so the line is a few seconds away.
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    loop {
        let log = node.log();
        // **The task field on the retry's own line**, not merely somewhere in the
        // log. The switch's startup WARN quotes its own value, so a check against
        // the whole log passed for a retry by any other task — it could not have
        // told the difference.
        if let Some(line) = log.lines().find(|l| l.contains("will retry in place")) {
            assert!(
                line.contains("embedding_worker"),
                "the retry line must name the task it is for, and this one does not: {line}"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the switch asked the embedding worker to fail once and nothing retried: {log}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Still serving: the failure was retried in place, not returned.
    node.wait_ready(&client).await;
    assert!(
        !node.log().contains("stopping the process so it is restarted"),
        "a transient failure is not a death: {}",
        node.log()
    );

    node.signal("TERM");
    let status = node.wait_exit();
    assert!(status.success(), "and the node still shuts down cleanly: {status:?}");
}

#[tokio::test]
async fn a_graceful_shutdown_is_not_a_task_death() {
    // The end-to-end control: a drain is a clean exit and leaves a `shutdown`
    // marker, not a `task_died` one.
    //
    // **It is not the test that holds the announcement open**, though it was
    // written as one. The drain aborts each supervised handle, and an aborted
    // supervisor is cancelled before it classifies anything, so this stays green
    // however the re-check behaves — measured, by blinding it
    // them. `a_task_that_ends_by_itself_during_shutdown_does_not_exit` in
    // kimmy-task is the one that fails when they go.
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();

    let mut node = Run::spawn(dir.path(), "graceful");
    node.wait_ready(&client).await;
    node.signal("TERM");
    let status = node.wait_exit();

    assert!(status.success(), "a drain is a clean exit, not a supervised death: {status:?}");
    let log = node.log();
    assert!(!log.contains("stopping the process so it is restarted"), "{log}");
    let marker = marker(dir.path()).expect("a graceful shutdown leaves a marker");
    assert!(marker.contains("exit = \"shutdown\""), "{marker}");
    assert!(!marker.contains("task_died"), "{marker}");
}

/// A storage engine that hits an I/O error stops the process, and the next
/// start says so, repairs the database, and serves what was written before
/// (ADR-188).
///
/// `KIMMY_TEST_FAIL_STORAGE=sync_data` fails the first fsync once the node is
/// serving, once, with EIO: the disk is healthy again at the next call, which is
/// the case that proves the point, because redb answers every later read and
/// write with `PreviousIo` all the same. Something commits soon after the node
/// serves, whether the writes below or a background task, so readiness is tried
/// and not required, as for the task deaths above.
///
/// **The failing run's stderr is a pipe nobody reads.** Every step of the report
/// before the exit can fail, and a write to stderr that panicked instead of
/// failing quietly once kept this node from exiting at all: the panic poisoned
/// the reaction with the failure already recorded, and the node served errors
/// with nothing to restart it. So the exit is asserted with stderr unwritable.
#[tokio::test]
async fn a_storage_io_error_exits_the_process_and_the_next_start_repairs_it() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let url = |run: &Run, path: &str| {
        format!("http://127.0.0.1:{}{path}", run.http.get().expect("a bound port"))
    };
    let login = |run: &Run| {
        client
            .post(url(run, "/v1/auth/login"))
            .json(&serde_json::json!({ "user": "root", "password": "harness-root-password" }))
            .send()
    };

    // Data written, and durable, before anything fails.
    let mut before = Run::spawn(dir.path(), "storage-before");
    before.wait_ready(&client).await;
    let body: serde_json::Value = login(&before).await.unwrap().json().await.unwrap();
    let token = body["token"].as_str().expect("a token").to_string();
    for (path, body) in [
        ("/v1/db/shop/collections", serde_json::json!({ "name": "orders" })),
        ("/v1/db/shop/coll/orders/docs", serde_json::json!({ "_id": 1, "item": "kept" })),
    ] {
        let res = client.post(url(&before, path)).bearer_auth(&token).json(&body).send().await;
        assert!(res.unwrap().status().is_success(), "{path}");
    }
    before.signal("TERM");
    assert!(before.wait_exit().success());

    let mut first = Run::spawn_with_stderr_closed(
        dir.path(),
        "storage-fails",
        &[("KIMMY_TEST_FAIL_STORAGE", "sync_data")],
    );
    if first.try_ready(&client, ports::BOUND_HTTP_LINE).await.is_ok() {
        // Any of these commits, and whichever does first fails. Their answers
        // are not the point.
        if let Ok(res) = login(&first).await
            && let Ok(body) = res.json::<serde_json::Value>().await
            && let Some(token) = body["token"].as_str()
        {
            let _ = client
                .post(url(&first, "/v1/db/shop/coll/orders/docs"))
                .bearer_auth(token)
                .json(&serde_json::json!({ "_id": 2 }))
                .send()
                .await;
        }
    }

    let status = first.wait_exit();
    assert_eq!(status.code(), Some(70), "a storage I/O error exits 70: {status:?}");
    let log = first.log();
    assert!(log.contains("a test switch is set that fails a storage call"), "{log}");
    assert!(log.contains("the storage engine hit an I/O error"), "{log}");
    assert!(log.contains("sync_data"), "the log names the call: {log}");
    let marker = marker(dir.path()).expect("a storage failure leaves the marker");
    assert!(marker.contains("exit = \"storage_failed\""), "{marker}");
    assert!(marker.contains("sync_data"), "the marker names the call: {marker}");

    let mut second = Run::spawn(dir.path(), "storage-fails-second");
    second.wait_ready(&client).await;
    let log = second.log();
    assert!(log.contains("stopped itself because its storage engine hit an I/O error"), "{log}");
    assert!(log.contains("repairing the database after an unclean stop"), "{log}");
    let ready = client.get(url(&second, "/readyz")).send().await.unwrap();
    assert_eq!(ready.status(), 200, "the repaired database serves");
    let body: serde_json::Value = login(&second).await.unwrap().json().await.unwrap();
    let token = body["token"].as_str().expect("a token");
    let kept: serde_json::Value = client
        .get(url(&second, "/v1/db/shop/coll/orders/docs/1"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(kept["item"], "kept", "what was written before the failure is there: {kept}");

    second.signal("TERM");
    assert!(second.wait_exit().success());
}

/// A client write whose own fsync fails is never told it failed (ADR-188's
/// addendum). `sync_data@write` fails the fsync of the first client write's
/// commit, and the node stops inside that call: the client's request was sent,
/// and the connection closes with no answer, which is an unknown outcome and
/// not a refusal. After the restart the write is there, because its pages
/// were written before the failed fsync.
///
/// The injection answers EIO in place of the fsync, and the kernel still
/// holds the pages, so this shows what the client sees and what the repair
/// keeps; it is not evidence of what a real failed fsync does to the platter.
#[tokio::test]
async fn a_write_whose_own_fsync_fails_is_not_answered_as_failed_and_is_there_after() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let url = |run: &Run, path: &str| {
        format!("http://127.0.0.1:{}{path}", run.http.get().expect("a bound port"))
    };
    let login = |run: &Run| {
        client
            .post(url(run, "/v1/auth/login"))
            .json(&serde_json::json!({ "user": "root", "password": "harness-root-password" }))
            .send()
    };

    let mut before = Run::spawn(dir.path(), "write-fsync-before");
    before.wait_ready(&client).await;
    let body: serde_json::Value = login(&before).await.unwrap().json().await.unwrap();
    let token = body["token"].as_str().expect("a token").to_string();
    let res = client
        .post(url(&before, "/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await;
    assert!(res.unwrap().status().is_success());
    before.signal("TERM");
    assert!(before.wait_exit().success());

    let mut failing = Run::spawn_with(
        dir.path(),
        "write-fsync-fails",
        &[("KIMMY_TEST_FAIL_STORAGE", "sync_data@write")],
    );
    failing.wait_ready(&client).await;
    let body: serde_json::Value = login(&failing).await.unwrap().json().await.unwrap();
    let token = body["token"].as_str().expect("a token").to_string();
    let answered = client
        .post(url(&failing, "/v1/db/shop/coll/orders/docs"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "_id": 7, "item": "maybe" }))
        .send()
        .await;
    match answered {
        Err(e) => assert!(!e.is_connect(), "the request was sent, and the answer never came: {e}"),
        Ok(res) => {
            // Answered before the stop: then only as unknown, never as failed.
            let status = res.status();
            let body: serde_json::Value = res.json().await.unwrap_or_default();
            assert_eq!(body["error"], "outcome_unknown", "{status} {body}");
            assert_eq!(body["retry"], "verify", "{body}");
        }
    }
    let status = failing.wait_exit();
    assert_eq!(status.code(), Some(70), "the fsync failure stops the node: {status:?}");
    let log = failing.log();
    assert!(log.contains("sync_data"), "the log names the call: {log}");

    let mut after = Run::spawn(dir.path(), "write-fsync-after");
    after.wait_ready(&client).await;
    let body: serde_json::Value = login(&after).await.unwrap().json().await.unwrap();
    let token = body["token"].as_str().expect("a token");
    let kept = client
        .get(url(&after, "/v1/db/shop/coll/orders/docs/7"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(kept.status(), 200, "the write whose fsync failed is there after the repair");
    after.signal("TERM");
    assert!(after.wait_exit().success());
}

/// Signal `run`, and wait for it to end: its status, and how long it took.
fn stop(run: &mut Run) -> (std::process::ExitStatus, Duration) {
    let started = Instant::now();
    run.signal("TERM");
    let status = run.wait_exit();
    (status, started.elapsed())
}

/// Start the next run on `dir`, wait for it to serve, and return its log
/// after stopping it cleanly.
async fn next_start_log(dir: &Path, name: &str, client: &reqwest::Client) -> String {
    let mut next = Run::spawn(dir, name);
    next.wait_ready(client).await;
    let log = next.log();
    let (status, _) = stop(&mut next);
    assert!(status.success(), "{status:?}");
    log
}

/// `exit = "shutdown"` is written only after redb closed (the 0.40.0
/// finding): a close slowed on purpose is waited for, `engine closed` comes
/// before `shutdown complete`, and the next start repairs nothing.
#[tokio::test]
async fn a_clean_stop_closes_the_store_before_its_marker() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run =
        Run::spawn_with(dir.path(), "slow-close", &[("KIMMY_TEST_STOP", "slow_close:1500")]);
    run.wait_ready(&client).await;
    // Watched from the signal to the exit: the marker must never say
    // `shutdown` while the close has not been logged. The close is slowed by
    // 1.5 s, so a marker written before it would be seen.
    let started = Instant::now();
    run.signal("TERM");
    let status = loop {
        let marker = marker(dir.path()).unwrap_or_default();
        if marker.contains("exit = \"shutdown\"") {
            assert!(run.log().contains("engine closed"), "the marker came first: {}", run.log());
        }
        if let Some(status) = run.child.get_mut().unwrap().try_wait().unwrap() {
            break status;
        }
        assert!(started.elapsed() < PATIENCE, "did not exit: {}", run.log());
        std::thread::sleep(Duration::from_millis(10));
    };
    let took = started.elapsed();
    assert!(status.success(), "{status:?}");
    assert!(took >= Duration::from_millis(1500), "the exit did not wait for the close: {took:?}");
    let log = run.log();
    assert!(log.contains("a test switch is set that acts on this node's stop"), "{log}");
    let closed = log.find("engine closed").unwrap_or_else(|| panic!("no close logged: {log}"));
    let complete = log.find("shutdown complete").unwrap_or_else(|| panic!("{log}"));
    assert!(closed < complete, "the marker came before the close: {log}");
    assert!(marker(dir.path()).unwrap().contains("exit = \"shutdown\""));

    let log = next_start_log(dir.path(), "slow-close-next", &client).await;
    assert!(log.contains("previous run ended cleanly"), "{log}");
    assert!(!log.contains("repairing the database"), "{log}");
}

/// A thread still holding the engine when the stop's time runs out, as a
/// walk serving a peer did in 0.40.0: the process exits 75 inside the stop's
/// budget instead of waiting for a supervisor's SIGKILL, and says why; the
/// marker says `storage_not_closed`, never `shutdown`; the next start says
/// the shutdown could not close its storage, and repairs it.
#[tokio::test]
async fn a_stop_with_the_engine_still_held_exits_75_and_the_next_start_repairs() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run = Run::spawn_with(dir.path(), "held", &[("KIMMY_TEST_STOP", "hold_engine")]);
    run.wait_ready(&client).await;
    let (status, took) = stop(&mut run);
    assert_eq!(status.code(), Some(75), "{status:?}");
    assert!(took < Duration::from_secs(27), "the stop was not bounded: {took:?}");
    let log = run.log();
    let line = log
        .lines()
        .find(|l| l.contains("exiting without closing the storage engine"))
        .unwrap_or_else(|| panic!("no error line: {log}"));
    assert!(line.contains("ERROR"), "{line}");
    assert!(line.contains("still held by a thread"), "{line}");
    assert!(!log.contains("shutdown complete"), "{log}");
    let marker = marker(dir.path()).expect("a marker");
    assert!(marker.contains("exit = \"storage_not_closed\""), "{marker}");
    assert!(marker.contains("still held by a thread"), "{marker}");

    let log = next_start_log(dir.path(), "held-next", &client).await;
    assert!(log.contains("the previous shutdown could not close its storage"), "{log}");
    assert!(log.contains("repairing the database after an unclean stop"), "{log}");
}

/// redb latches a need for repair when a write transaction is dropped while
/// its thread panics; the close then leaves the header dirty with no I/O
/// error for the storage's health to see. The stop reads the header, so the
/// marker says `storage_not_closed`, and the next start repairs.
#[tokio::test]
async fn a_panic_inside_a_write_is_never_recorded_as_a_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run = Run::spawn_with(dir.path(), "panicked", &[("KIMMY_TEST_STOP", "panic_in_write")]);
    run.wait_ready(&client).await;
    let deadline = Instant::now() + PATIENCE;
    while !run.log().contains("a panic inside a write transaction, on purpose") {
        assert!(Instant::now() < deadline, "the switch never panicked: {}", run.log());
        tokio::time::sleep(POLL).await;
    }
    let (status, _) = stop(&mut run);
    assert_eq!(status.code(), Some(75), "{status:?}");
    let log = run.log();
    assert!(!log.contains("shutdown complete"), "{log}");
    // The panic's line says on which thread, and where.
    // The message carries its own line break, so the record runs to the next
    // line that starts with a timestamp.
    let from = log.find("a thread panicked").expect("the panic's line");
    let panicked = log[from..].split("\n20").next().unwrap();
    for field in ["thread", "thread_id", "location", "engine.rs"] {
        assert!(panicked.contains(field), "{field}: {panicked}");
    }
    let marker = marker(dir.path()).expect("a marker");
    assert!(marker.contains("exit = \"storage_not_closed\""), "{marker}");
    assert!(marker.contains("needing recovery"), "{marker}");

    let log = next_start_log(dir.path(), "panicked-next", &client).await;
    assert!(log.contains("the previous shutdown could not close its storage"), "{log}");
    assert!(log.contains("repairing the database after an unclean stop"), "{log}");
}

/// Serving that fails with no signal ends bounded, as an error, with the
/// store closed first: the marker says `error`, and the next start repairs
/// nothing.
#[tokio::test]
async fn serving_that_fails_closes_the_store_and_records_the_error() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run = Run::spawn_with(dir.path(), "serve-error", &[("KIMMY_TEST_STOP", "serve_error")]);
    let status = run.wait_exit();
    assert_eq!(status.code(), Some(1), "{status:?}");
    let log = run.log();
    assert!(log.contains("engine closed"), "{log}");
    assert!(log.contains("serving failed on purpose"), "{log}");
    let marker = marker(dir.path()).expect("a marker");
    assert!(marker.contains("exit = \"error\""), "{marker}");

    let log = next_start_log(dir.path(), "serve-error-next", &client).await;
    assert!(log.contains("the previous run exited on an error"), "{log}");
    assert!(!log.contains("repairing the database"), "{log}");
}

/// A second SIGTERM during the stop changes nothing: the stop goes on as it
/// was, and ends clean.
#[tokio::test]
async fn a_second_signal_during_the_stop_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run = Run::spawn_with(dir.path(), "twice", &[("KIMMY_TEST_STOP", "slow_close:1500")]);
    run.wait_ready(&client).await;
    run.signal("TERM");
    std::thread::sleep(Duration::from_millis(500));
    run.signal("TERM");
    let status = run.wait_exit();
    assert!(status.success(), "{status:?}");
    assert!(marker(dir.path()).unwrap().contains("exit = \"shutdown\""));
    assert_eq!(run.log().matches("shutdown signal received").count(), 1, "{}", run.log());
}

/// A collector that accepts and never answers holds nothing up: the
/// exporters get two seconds between them, after the marker is written.
#[tokio::test]
async fn a_collector_that_never_answers_does_not_hold_the_stop() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    // Connections complete into the backlog and are never read.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", silent.local_addr().unwrap());
    let mut run = Run::spawn_with(dir.path(), "silent-otlp", &[("KIMMY_OTLP_ENDPOINT", &endpoint)]);
    run.wait_ready(&client).await;
    let (status, took) = stop(&mut run);
    assert!(status.success(), "{status:?}");
    assert!(took < Duration::from_secs(6), "the exporters held the stop: {took:?}");
    let log = run.log();
    assert!(log.contains("the collector did not take the last spans and metrics in time"), "{log}");
    // Where this process's service.instance.id meets the node id.
    let instance = log.lines().find(|l| l.contains("telemetry instance")).expect("the line");
    for field in ["service_instance_id", "node"] {
        assert!(instance.contains(field), "{field}: {instance}");
    }
    assert!(marker(dir.path()).unwrap().contains("exit = \"shutdown\""));
    drop(silent);
}

/// A collector that answers every export 503 with `Retry-After: {retry_after}`,
/// and how many exports it has answered.
fn throttling_collector(retry_after: u64) -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let at = listener.local_addr().unwrap();
    let throttled = Arc::new(AtomicUsize::new(0));
    let answered = Arc::clone(&throttled);
    // UNSUPERVISED: the test's collector, which lives as long as the test.
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let answered = Arc::clone(&answered);
            // UNSUPERVISED: one connection of the test's collector.
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream);
                loop {
                    // One request: its headers, then as much body as they say.
                    let mut length = 0usize;
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 {
                            return;
                        }
                        if line == "\r\n" {
                            break;
                        }
                        if let Some((name, value)) = line.split_once(':')
                            && name.eq_ignore_ascii_case("content-length")
                        {
                            length = value.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0; length];
                    if reader.read_exact(&mut body).is_err() {
                        return;
                    }
                    answered.fetch_add(1, Ordering::SeqCst);
                    let reply = format!(
                        "HTTP/1.1 503 Service Unavailable\r\nRetry-After: {retry_after}\r\n\
                         Content-Length: 0\r\n\r\n"
                    );
                    if reader.get_mut().write_all(reply.as_bytes()).is_err() {
                        return;
                    }
                }
            });
        }
    });
    (at, throttled)
}

/// A collector the exporter retries holds nothing up either, and what bounds
/// each case. Since opentelemetry-otlp 0.33 an export that cannot connect, or
/// is answered 429, 502, 503 or 504, is tried again with backoff; upstream
/// checks the time left only before each wait, so its own timeout does not
/// bound it.
///
/// - **Throttled with `Retry-After: 3`**, under the ten-second export
///   timeout: the exporter waits the three seconds and asks again, so only
///   the stop's two seconds for the exporters end it, and the node says it
///   exited without them.
/// - **Throttled with `Retry-After: 30`**, past the export timeout: the
///   exporter gives up at once rather than wait, and the stop's deadline is
///   never reached.
/// - **Refused**: three retries over about a second of backoff end on their
///   own, near the two seconds; bounded by the deadline either way.
#[tokio::test]
async fn a_collector_the_exporter_retries_does_not_hold_the_stop() {
    let refused = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let (waited_for, waiting) = throttling_collector(3);
    let (given_up_on, giving_up) = throttling_collector(30);
    let client = reqwest::Client::new();
    const LATE: &str = "the collector did not take the last spans and metrics in time";
    for (name, at, late) in [
        ("throttling-otlp", waited_for, Some(true)),
        ("throttled-past-the-timeout-otlp", given_up_on, Some(false)),
        ("refusing-otlp", refused, None),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let endpoint = format!("http://{at}");
        let mut run = Run::spawn_with(dir.path(), name, &[("KIMMY_OTLP_ENDPOINT", &endpoint)]);
        run.wait_ready(&client).await;
        let (status, took) = stop(&mut run);
        let log = run.log();
        assert!(status.success(), "{name}: {status:?}: {log}");
        assert!(took < Duration::from_secs(6), "{name}: the exporters held the stop: {took:?}");
        assert!(marker(dir.path()).unwrap().contains("exit = \"shutdown\""), "{name}");
        if let Some(late) = late {
            assert_eq!(log.contains(LATE), late, "{name}: {log}");
        }
    }
    assert!(
        waiting.load(Ordering::SeqCst) > 0,
        "the throttling collector was never sent an export"
    );
    assert!(giving_up.load(Ordering::SeqCst) > 0, "nor the other one");
}

/// An expiry pass in progress at the stop ends at its next delete: the
/// engine is closed to writes the moment serving ends, before the stop
/// waits for its tasks. A 0.40.2 candidate that waited first let the pass run
/// on through the wait, one commit per document, for 6 to 10 s, and past the
/// window with twelve collections due, when the stop exited 75 and the next
/// start repaired.
#[tokio::test]
async fn an_expiry_pass_in_progress_at_the_stop_ends_at_once() {
    const COLLECTIONS: usize = 12;
    const DUE: usize = 1_000;
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run = Run::spawn_with_storage(dir.path(), "expiring", "ttl_interval_secs = 1");
    run.wait_ready(&client).await;
    let url = |path: &str| format!("http://127.0.0.1:{}{path}", run.http.get().unwrap());
    let login = client
        .post(url("/v1/auth/login"))
        .json(&serde_json::json!({ "user": "root", "password": "harness-root-password" }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = login.json().await.unwrap();
    let token = body["token"].as_str().expect("a token").to_string();
    let post = |path: String, body: serde_json::Value| {
        client.post(url(&path)).bearer_auth(&token).json(&body).send()
    };
    // Every document is due, and no index exists until all are written, so
    // the passes begin with every collection's whole backlog waiting.
    for c in 0..COLLECTIONS {
        let made =
            post("/v1/db/shop/collections".into(), serde_json::json!({ "name": format!("t{c}") }));
        assert!(made.await.unwrap().status().is_success());
        let docs: Vec<_> = (0..DUE)
            .map(|i| serde_json::json!({ "_id": i, "at": { "$date": 1_000_000_000_000i64 } }))
            .collect();
        let bulk = post(format!("/v1/db/shop/coll/t{c}/bulk"), serde_json::json!(docs));
        assert!(bulk.await.unwrap().status().is_success());
    }
    for c in 0..COLLECTIONS {
        let index = post(
            format!("/v1/db/shop/coll/t{c}/indexes"),
            serde_json::json!({ "name": "ttl", "fields": [{ "path": "at" }], "expireAfterSeconds": 0 }),
        );
        let index = index.await.unwrap();
        assert!(index.status().is_success(), "{}", index.text().await.unwrap());
    }
    let deadline = Instant::now() + PATIENCE;
    while !run.log().contains("expired documents") {
        assert!(Instant::now() < deadline, "no expiry pass ran: {}", run.log());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (status, took) = stop(&mut run);
    let log = run.log();
    assert!(status.success(), "{status:?}: {log}");
    assert!(took < Duration::from_secs(3), "the pass held the stop: {took:?}");
    assert!(marker(dir.path()).unwrap().contains("exit = \"shutdown\""));
}

/// A client that sent half its request's headers holds the stop no longer
/// than the drain: its connection is closed at the drain's deadline. A 0.40.2
/// candidate that waited for plain HTTP's connections to end held it to 20 s.
#[tokio::test]
async fn a_half_sent_request_holds_the_stop_no_longer_than_the_drain() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run = Run::spawn(dir.path(), "half-sent");
    run.wait_ready(&client).await;
    let mut half = std::net::TcpStream::connect(("127.0.0.1", *run.http.get().unwrap())).unwrap();
    half.write_all(b"GET /healthz HTTP/1.1\r\nHost").unwrap();
    std::thread::sleep(Duration::from_millis(200));
    let (status, took) = stop(&mut run);
    assert!(status.success(), "{status:?}: {}", run.log());
    // The drain is ten seconds.
    assert!(took < Duration::from_secs(13), "the half-sent request held the stop: {took:?}");
    assert!(marker(dir.path()).unwrap().contains("exit = \"shutdown\""));
    drop(half);
}

/// A node out of file descriptors says it cannot accept new clients: the
/// HTTP listener logs the error, at most once a second, where 0.40.2's first
/// candidate went silent on plain HTTP.
#[tokio::test]
async fn a_listener_out_of_descriptors_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run = Run::spawn_with_fd_limit(dir.path(), "starved", 64);
    run.wait_ready(&client).await;
    let port = *run.http.get().unwrap();
    // More connections than the node has descriptors, held open.
    let mut held = Vec::new();
    for _ in 0..150 {
        match std::net::TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => held.push(stream),
            Err(_) => break,
        }
    }
    let deadline = Instant::now() + PATIENCE;
    while !run.log().contains("could not accept an HTTP connection") {
        assert!(Instant::now() < deadline, "no accept error was logged: {}", run.log());
        tokio::time::sleep(POLL).await;
    }
    let line = run.log().lines().find(|l| l.contains("could not accept")).unwrap().to_string();
    assert!(line.contains("ERROR") && line.contains("error"), "{line}");
    drop(held);
    // Recovered, and it counted: once the descriptors are back the node
    // answers, and `/metrics` says the HTTP listener had accept errors of its
    // own, on the series the replication listener shares.
    let series = "kimmy_accept_errors_total{listener=\"http\"} ";
    let deadline = Instant::now() + PATIENCE;
    let count = loop {
        assert!(Instant::now() < deadline, "the node did not recover: {}", run.log());
        if let Ok(body) = client.get(format!("http://127.0.0.1:{port}/metrics")).send().await
            && let Ok(body) = body.text().await
            && let Some(rest) = body.split(series).nth(1)
        {
            break rest.lines().next().unwrap().parse::<u64>().unwrap();
        }
        tokio::time::sleep(POLL).await;
    };
    assert!(count > 0, "descriptor exhaustion was not counted");
    let (status, _) = stop(&mut run);
    assert!(status.success(), "{status:?}: {}", run.log());
}

/// The serve budget's test switches ship in the binary (ADR-194), so a start
/// where either is set says so, as the other test switches do, with whether
/// the value was understood.
#[tokio::test]
async fn a_start_with_a_serve_walk_switch_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run = Run::spawn_with(
        dir.path(),
        "serve-walk",
        &[("KIMMY_TEST_SERVE_WALK_ROWS", "500"), ("KIMMY_TEST_SERVE_WALK_MS", "soon")],
    );
    run.wait_ready(&client).await;
    let log = run.log();
    let switch = |name: &str| {
        log.lines()
            .find(|l| l.contains("changes the budget of every window") && l.contains(name))
            .unwrap_or_else(|| panic!("no line for {name}: {log}"))
            .to_string()
    };
    assert!(switch("KIMMY_TEST_SERVE_WALK_ROWS").contains("recognised=true"));
    assert!(switch("KIMMY_TEST_SERVE_WALK_MS").contains("recognised=false"));
    let (status, _) = stop(&mut run);
    assert!(status.success(), "{status:?}");
}

/// The TLS listener's stop closes the store too: its drain and its
/// certificate reloader hold nothing past the runtime's shutdown.
#[tokio::test]
async fn a_stop_over_tls_closes_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let (cert, key) = (dir.path().join("server.crt"), dir.path().join("server.key"));
    std::fs::write(&cert, issued.cert.pem()).unwrap();
    std::fs::write(&key, issued.signing_key.serialize_pem()).unwrap();
    let (cert, key) = (cert.display().to_string(), key.display().to_string());
    let mut run = Run::spawn_with(
        dir.path(),
        "tls",
        &[("KIMMY_TLS_CERT", cert.as_str()), ("KIMMY_TLS_KEY", key.as_str())],
    );
    let client = reqwest::Client::builder().danger_accept_invalid_certs(true).build().unwrap();
    let deadline = Instant::now() + PATIENCE;
    loop {
        if let ports::Bound::Port(port) =
            ports::bound_http_port(&run.stdout, ports::BOUND_HTTP_LINE, run.pid, &[])
            && let Ok(res) = client.get(format!("https://localhost:{port}/readyz")).send().await
            && res.status().is_success()
        {
            break;
        }
        assert!(Instant::now() < deadline, "never served over TLS: {}", run.log());
        tokio::time::sleep(POLL).await;
    }
    let (status, _) = stop(&mut run);
    assert!(status.success(), "{status:?}: {}", run.log());
    assert!(run.log().contains("engine closed"), "{}", run.log());
    assert!(marker(dir.path()).unwrap().contains("exit = \"shutdown\""));
}

/// A panic out of `node::run` loses the engine with the future that held it,
/// so nothing can be closed: the runtime's shutdown is bounded all the same
/// (a thread holds the engine here, and would hold an unbounded one up for a
/// day), the process exits 101, and no marker is written, so the next start
/// calls the run unclean, and repairs.
#[tokio::test]
async fn a_panic_out_of_the_run_exits_bounded_and_leaves_no_marker() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let started = Instant::now();
    let mut run = Run::spawn_with(dir.path(), "panic-run", &[("KIMMY_TEST_STOP", "panic_in_run")]);
    let status = run.wait_exit();
    assert_eq!(status.code(), Some(101), "{status:?}: {}", run.log());
    assert!(started.elapsed() < Duration::from_secs(20), "{:?}", started.elapsed());
    assert!(run.log().contains("node::run panicked on purpose"), "{}", run.log());
    assert!(marker_absent(dir.path()), "{:?}", marker(dir.path()));

    let log = next_start_log(dir.path(), "panic-run-next", &client).await;
    assert!(log.contains("previous run did not shut down cleanly"), "{log}");
    assert!(log.contains("repairing the database after an unclean stop"), "{log}");
}

/// The HTTP base of a run that is serving.
fn base(run: &Run) -> String {
    format!("http://127.0.0.1:{}", run.http.get().expect("a bound port"))
}

async fn root_token(run: &Run, client: &reqwest::Client) -> String {
    let body: serde_json::Value = client
        .post(format!("{}/v1/auth/login", base(run)))
        .json(&serde_json::json!({ "user": "root", "password": "harness-root-password" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    body["token"].as_str().expect("a token").to_string()
}

/// How long each row of a walk takes under `KIMMY_TEST_WALK_ROW_MS` in the
/// tests below: long enough that the walks they start would still be
/// running when the stop's 22 s ran out, had they not stopped.
const SLOW_ROW_MS: &str = "25";

/// Start a node with every walk slowed on `dir`, whose store an earlier run
/// filled; start `slow` against it, and stop the node while that runs. The
/// stop is prompt, clean, and closes the store: exit 0, `engine closed`, the
/// `shutdown` marker, and a next start that repairs nothing.
async fn a_slow_walk_at_the_stop_ends_and_the_store_closes<F, Fut>(
    dir: &Path,
    name: &str,
    client: &reqwest::Client,
    slow: F,
) -> String
where
    F: FnOnce(String, String) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut run = Run::spawn_with(dir, name, &[("KIMMY_TEST_WALK_ROW_MS", SLOW_ROW_MS)]);
    run.wait_ready(client).await;
    let token = root_token(&run, client).await;
    tokio::spawn(slow(base(&run), token));
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let (status, took) = stop(&mut run);
    let log = run.log();
    assert!(status.success(), "{status:?}: {log}");
    assert!(log.contains("a test switch is set that slows every walk"), "{log}");
    assert!(log.contains("engine closed"), "{log}");
    assert!(took < Duration::from_secs(16), "the stop waited for the walk: {took:?}");
    assert!(marker(dir).unwrap().contains("exit = \"shutdown\""));
    let next = next_start_log(dir, &format!("{name}-next"), client).await;
    assert!(!next.contains("repairing the database"), "{next}");
    log
}

/// A backup in flight at the stop is a client's request: the drain lets it
/// run until its deadline, and then it ends, so the store closes, rather
/// than running on until the supervisor kills the process.
#[tokio::test]
async fn a_backup_in_flight_at_the_stop_ends_at_the_drain_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut fill = Run::spawn(dir.path(), "backup-fill");
    fill.wait_ready(&client).await;
    let token = root_token(&fill, &client).await;
    let docs: Vec<_> = (0..600).map(|i| serde_json::json!({ "_id": i })).collect();
    for (path, body) in [
        ("/v1/db/shop/collections", serde_json::json!({ "name": "orders" })),
        ("/v1/db/shop/coll/orders/bulk", serde_json::json!(docs)),
    ] {
        let res = client.post(format!("{}{path}", base(&fill))).bearer_auth(&token).json(&body);
        assert!(res.send().await.unwrap().status().is_success(), "{path}");
    }
    assert!(stop(&mut fill).0.success());

    a_slow_walk_at_the_stop_ends_and_the_store_closes(
        dir.path(),
        "backup-slow",
        &client,
        |base, token| async move {
            let _ = reqwest::Client::new()
                .get(format!("{base}/v1/admin/backup"))
                .bearer_auth(token)
                .send()
                .await;
        },
    )
    .await;
}

/// A search that builds its collection's vector graph is a client's
/// request too: the walks that count and read the vectors end at the
/// drain's deadline, and the build with them.
#[tokio::test]
async fn a_vector_graph_building_at_the_stop_ends_at_the_drain_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut fill = Run::spawn(dir.path(), "graph-fill");
    fill.wait_ready(&client).await;
    let token = root_token(&fill, &client).await;
    let post = |path: &str, body: serde_json::Value| {
        client.post(format!("{}{path}", base(&fill))).bearer_auth(&token).json(&body).send()
    };
    assert!(
        post("/v1/db/shop/collections", serde_json::json!({ "name": "vec" }))
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let vectors =
        serde_json::json!({ "fields": ["note"], "provider": { "kind": "byo" }, "dim": 3 });
    assert!(post("/v1/db/shop/coll/vec/vector", vectors).await.unwrap().status().is_success());
    // Above the 500 below which a search is exact, so a search builds a graph.
    let docs: Vec<_> = (0..600).map(|i| serde_json::json!({ "_id": i, "note": "n" })).collect();
    assert!(
        post("/v1/db/shop/coll/vec/bulk", serde_json::json!(docs))
            .await
            .unwrap()
            .status()
            .is_success()
    );
    for i in 0..600 {
        let res = client
            .put(format!("{}/v1/db/shop/coll/vec/docs/{i}/vectors", base(&fill)))
            .bearer_auth(&token)
            .json(&serde_json::json!([{ "chunk": 0, "vector": [1.0, i as f32, 0.0], "text": "n" }]))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success());
    }
    assert!(stop(&mut fill).0.success());

    a_slow_walk_at_the_stop_ends_and_the_store_closes(
        dir.path(),
        "graph-slow",
        &client,
        |base, token| async move {
            let _ = reqwest::Client::new()
                .post(format!("{base}/v1/db/shop/coll/vec/vector_search"))
                .bearer_auth(token)
                .json(&serde_json::json!({ "vector": [1.0, 0.0, 0.0], "k": 3 }))
                .send()
                .await;
        },
    )
    .await;
}

/// The port a node bound, read from its log at the bind: before its store is
/// open, when a probe already has something to ask (ADR-198).
async fn wait_bound(run: &Run) -> u16 {
    let deadline = Instant::now() + PATIENCE;
    let mut line_wait = ports::LineWait::default();
    loop {
        let bound = ports::bound_http_port(&run.stdout, ports::BOUND_HTTP_LINE, run.pid, &[]);
        match line_wait.judge(bound, ports::BOUND_HTTP_LINE) {
            Ok(Some(port)) => return port,
            Ok(None) => {}
            Err(why) => panic!("{why}; log: {}", run.log()),
        }
        assert!(Instant::now() < deadline, "never bound; log: {}", run.log());
        tokio::time::sleep(POLL).await;
    }
}

/// A node whose store takes seconds to open answers `/healthz` from the bind,
/// says on `/readyz` what it is doing, refuses everything else (`/v1/version`
/// included, which a roll polls), and then serves, with no connection lost at
/// the swap.
#[tokio::test]
async fn a_node_answers_its_probes_while_its_store_opens_and_serves_after() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run = Run::spawn_with(dir.path(), "opening", &[("KIMMY_TEST_OPEN_DELAY_SECS", "5")]);
    let port = wait_bound(&run).await;
    let url = |path: &str| format!("http://127.0.0.1:{port}{path}");
    let opened_by = Instant::now() + Duration::from_secs(5);

    // Liveness is up from the bind; readiness says what it is doing.
    let health = client.get(url("/healthz")).send().await.unwrap();
    assert_eq!(health.status(), 200, "{}", run.log());
    let ready = client.get(url("/readyz")).send().await.unwrap();
    assert_eq!(ready.status(), 503);
    let body: serde_json::Value = ready.json().await.unwrap();
    assert_eq!(body["error"], "starting", "{body}");
    assert_eq!(body["retry"], "elsewhere", "{body}");
    assert_eq!(body["status"], "opening", "{body}");
    assert_eq!(body["phase"], "opening", "{body}");
    assert!(body["phase_age_seconds"].is_number(), "{body}");

    // Everything else is refused, the two a roll and a scrape use included,
    // with no Retry-After (another member is the answer).
    for path in ["/v1/version", "/metrics", "/v1/db/shop/collections"] {
        let refused = client.get(url(path)).send().await.unwrap();
        assert_eq!(refused.status(), 503, "{path}");
        assert!(refused.headers().get("retry-after").is_none(), "{path}");
        let body: serde_json::Value = refused.json().await.unwrap();
        assert_eq!(body["error"], "starting", "{path}: {body}");
    }
    // The probes are not logged, however many the open receives.
    assert!(!run.log().contains("request failed"), "{}", run.log());

    // It becomes ready, and a request is answered by the router.
    run.wait_ready(&client).await;
    assert!(Instant::now() >= opened_by - Duration::from_secs(1), "the open was not delayed");
    let version = client.get(url("/v1/version")).send().await.unwrap();
    assert_eq!(version.status(), 200, "{}", run.log());
    let log = run.log();
    let bound = log.find("HTTP listener bound").expect("the bind is logged");
    let serving = log.find("serving HTTP and WebSocket").expect("serving is logged");
    let started = log.find("starting kimmyd").expect("the banner is logged");
    assert!(bound < started && started < serving, "bind, then open, then serving: {log}");

    run.signal("TERM");
    assert!(run.wait_exit().success());
}

/// A stop asked for while the store opens is heard (the handler is installed
/// at the bind), ends the wait, and stops the node without serving, with a
/// clean marker.
#[tokio::test]
async fn a_stop_during_the_open_stops_the_node_without_serving() {
    let dir = tempfile::tempdir().unwrap();
    let mut run = Run::spawn_with(dir.path(), "stopped", &[("KIMMY_TEST_OPEN_DELAY_SECS", "40")]);
    let port = wait_bound(&run).await;
    let client = reqwest::Client::new();
    assert_eq!(
        client.get(format!("http://127.0.0.1:{port}/healthz")).send().await.unwrap().status(),
        200
    );

    let before = Instant::now();
    run.signal("TERM");
    let status = run.wait_exit();
    let took = before.elapsed();
    assert!(status.success(), "{status:?}: {}", run.log());
    assert!(took < Duration::from_secs(10), "the stop waited for the whole open: {took:?}");
    let log = run.log();
    assert!(log.contains("stopping without serving"), "{log}");
    assert!(!log.contains("serving HTTP and WebSocket"), "it must never serve: {log}");
    assert!(marker(dir.path()).unwrap().contains("exit = \"shutdown\""));
}

/// A bind that fails is a start that never opened the store: no store file was
/// created, and the last run's verdict is inherited by the failed start.
#[tokio::test]
async fn a_bind_that_fails_never_opens_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let log = a_start_that_fails_to_bind(dir.path(), "unbindable");
    assert!(log.contains("Address already in use"), "{log}");
    assert!(!log.contains("starting kimmyd"), "the banner follows the open: {log}");
    assert!(
        !dir.path().join("data").join("kimmy.redb").exists(),
        "an address that cannot be bound must not create the store"
    );
}

/// A stop asked for while a migration runs ends the open between two of its
/// steps and concludes as a clean shutdown, with no engine: exit 0, a
/// `shutdown` marker, no serving. The next start resumes from the migration's
/// markers and finds nothing to repair.
#[tokio::test]
async fn a_stop_between_two_migration_steps_is_a_clean_shutdown_and_the_next_start_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let path = data.join("kimmy.redb");
    {
        let engine = kimmy_storage::Engine::open(&path).unwrap();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine
            .insert_many(&coll, (0..20i64).map(|i| bson::doc! { "_id": i, "n": i + 1 }).collect())
            .unwrap();
        for name in ["a", "b", "c"] {
            engine
                .create_index_with(
                    "shop",
                    "orders",
                    vec![kimmy_storage::IndexField { path: "n".into(), descending: false }],
                    false,
                    Default::default(),
                    Some(name.into()),
                    None,
                    Some(bson::doc! { "n": { "$gt": 0 } }),
                )
                .unwrap();
        }
        engine.close().unwrap();
    }
    kimmy_storage::make_schema_3_for_test(&path).unwrap();

    let mut run = Run::spawn_with(dir.path(), "migrating", &[("KIMMY_TEST_OPEN_STEP_MS", "20000")]);
    let deadline = Instant::now() + PATIENCE;
    while !run.log().contains("rebuilt a partial index") {
        assert!(Instant::now() < deadline, "no step finished: {}", run.log());
        tokio::time::sleep(POLL).await;
    }
    let before = Instant::now();
    run.signal("TERM");
    let status = run.wait_exit();
    assert!(status.success(), "{status:?}: {}", run.log());
    assert!(before.elapsed() < Duration::from_secs(10), "the stop waited for the open");
    let log = run.log();
    assert!(log.contains("stopping without serving"), "{log}");
    assert!(log.contains("shutdown complete"), "{log}");
    assert!(!log.contains("serving HTTP and WebSocket"), "{log}");
    assert!(!log.contains("exiting on an error"), "not a failed start: {log}");
    let marker = marker(dir.path()).expect("a stop leaves its marker");
    assert!(marker.contains("exit = \"shutdown\""), "{marker}");
    assert!(!marker.contains("failed_start"), "{marker}");

    let client = reqwest::Client::new();
    let mut next = Run::spawn(dir.path(), "resumed");
    next.wait_ready(&client).await;
    let log = next.log();
    assert!(log.contains("previous run ended cleanly"), "{log}");
    assert!(!log.contains("repair"), "{log}");
    assert!(log.contains("rebuilt a partial index"), "the migration resumed: {log}");
    next.signal("TERM");
    assert!(next.wait_exit().success());
}

/// A stop asked for while the node starts, after the store is open, stops it
/// and never says it serves (what an operator greps for is a node that did).
#[tokio::test]
async fn a_stop_while_the_node_starts_never_logs_that_it_serves() {
    let dir = tempfile::tempdir().unwrap();
    let mut run = Run::spawn_with(dir.path(), "starting", &[("KIMMY_TEST_START_DELAY_SECS", "4")]);
    let port = wait_bound(&run).await;
    let client = reqwest::Client::new();
    let deadline = Instant::now() + PATIENCE;
    loop {
        let body: serde_json::Value = client
            .get(format!("http://127.0.0.1:{port}/readyz"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if body["phase"] == "starting" {
            break;
        }
        assert!(Instant::now() < deadline, "never starting: {body}: {}", run.log());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    run.signal("TERM");
    let status = run.wait_exit();
    assert!(status.success(), "{status:?}: {}", run.log());
    let log = run.log();
    assert!(!log.contains("serving HTTP and WebSocket"), "{log}");
    assert!(log.contains("a stop was asked for while the node was starting"), "{log}");
    assert!(log.contains("shutdown complete"), "{log}");
}

/// The certificate the listener presents, as DER, from a handshake that
/// accepts anything: what a client of the node would be handed.
async fn served_certificate(port: u16) -> Vec<u8> {
    use std::sync::Arc;

    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};

    #[derive(Debug)]
    struct Anything;
    impl ServerCertVerifier for Anything {
        fn verify_server_cert(
            &self,
            _: &CertificateDer<'_>,
            _: &[CertificateDer<'_>],
            _: &ServerName<'_>,
            _: &[u8],
            _: UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(Anything))
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let tls = connector.connect(ServerName::try_from("localhost").unwrap(), tcp).await.unwrap();
    let (_, connection) = tls.get_ref();
    connection.peer_certificates().unwrap()[0].as_ref().to_vec()
}

/// A rotated certificate reaches the listener: SIGHUP reloads it into the
/// configuration the listener serves, and the next handshake presents it.
#[tokio::test]
async fn a_reloaded_certificate_is_the_one_the_listener_serves() {
    let dir = tempfile::tempdir().unwrap();
    let first = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let (cert, key) = (dir.path().join("server.crt"), dir.path().join("server.key"));
    std::fs::write(&cert, first.cert.pem()).unwrap();
    std::fs::write(&key, first.signing_key.serialize_pem()).unwrap();
    let (cert_arg, key_arg) = (cert.display().to_string(), key.display().to_string());
    let mut run = Run::spawn_with(
        dir.path(),
        "rotating",
        &[("KIMMY_TLS_CERT", cert_arg.as_str()), ("KIMMY_TLS_KEY", key_arg.as_str())],
    );
    let port = wait_bound(&run).await;
    let deadline = Instant::now() + PATIENCE;
    let before = loop {
        if run.log().contains("serving HTTPS") {
            break served_certificate(port).await;
        }
        assert!(Instant::now() < deadline, "never served: {}", run.log());
        tokio::time::sleep(POLL).await;
    };
    assert_eq!(before, first.cert.der().as_ref(), "the first certificate is served");

    let second = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    std::fs::write(&cert, second.cert.pem()).unwrap();
    std::fs::write(&key, second.signing_key.serialize_pem()).unwrap();
    run.signal("HUP");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let now = served_certificate(port).await;
        if now == second.cert.der().as_ref() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the rotated certificate is never served: {}",
            run.log()
        );
        tokio::time::sleep(POLL).await;
    }
    run.signal("TERM");
    assert!(run.wait_exit().success(), "{}", run.log());
}

// ---------------------------------------------------------------------------
// The catching-up marker (ADR-202)
// ---------------------------------------------------------------------------

/// The marker file the member keeps in its data directory.
const CATCHING_UP_FILE: &str = "kimmy.catching-up";

fn catching_up_marker(dir: &Path) -> Option<String> {
    std::fs::read_to_string(dir.join("data").join(CATCHING_UP_FILE)).ok()
}

/// A port nothing listens on: a cluster seed no peer is ever reached at.
fn dead_port() -> u16 {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    taken.local_addr().unwrap().port()
}

/// `/readyz` of a run: its status, its `x-kimmy-catching-up` header and its body.
async fn readyz(run: &Run, client: &reqwest::Client) -> (u16, Option<String>, serde_json::Value) {
    let port = *run.http.get().expect("the port was read from the log");
    let res = client.get(format!("http://127.0.0.1:{port}/readyz")).send().await.unwrap();
    let status = res.status().as_u16();
    let header = res.headers().get("x-kimmy-catching-up").map(|v| v.to_str().unwrap().to_string());
    (status, header, res.json().await.unwrap_or(serde_json::Value::Null))
}

async fn catching_up_gauge(run: &Run, client: &reqwest::Client) -> Vec<String> {
    let port = *run.http.get().expect("the port was read from the log");
    let body = client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    body.lines()
        .filter(|l| l.starts_with("kimmy_catching_up{") && l.ends_with(" 1"))
        .map(str::to_string)
        .collect()
}

/// Wait until `what` holds of `/readyz`, or fail with the run's log.
async fn until_ready_is(
    run: &Run,
    client: &reqwest::Client,
    what: &str,
    mut holds: impl FnMut(u16, &Option<String>, &serde_json::Value) -> bool,
) {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let (status, header, body) = readyz(run, client).await;
        if holds(status, &header, &body) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "never saw: {what}; last {status} {header:?} {body}; log: {}",
            run.log()
        );
        tokio::time::sleep(POLL).await;
    }
}

/// A member that creates its store in a cluster it has seeds for is marked
/// before the store exists, is not ready (it says why), and refuses data routes.
/// With no peer reachable it cannot learn, so past the wait it serves as
/// `unknown`, with the marker still set; the operator deleting the file clears
/// it live; and the next start, which finds a store, is not marked.
#[tokio::test]
async fn a_fresh_member_with_seeds_is_marked_and_the_marker_goes_by_the_wait_and_the_operator() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let mut run = Run::spawn_clustered(dir.path(), "fresh", dead_port(), 3);

    // Not ready at first: wait for the port, not for a 200.
    let deadline = Instant::now() + PATIENCE;
    while run.http.get().is_none() {
        let bound = ports::bound_http_port(&run.stdout, ports::BOUND_HTTP_LINE, run.pid, &[]);
        if let Ok(Some(port)) = ports::LineWait::default().judge(bound, ports::BOUND_HTTP_LINE) {
            let _ = run.http.set(port);
        }
        assert!(Instant::now() < deadline, "no port; log: {}", run.log());
        tokio::time::sleep(POLL).await;
    }
    until_ready_is(&run, &client, "503 catching_up seeded_empty", |status, header, body| {
        status == 503 && header.as_deref() == Some("seeded_empty") && body["error"] == "catching_up"
    })
    .await;
    let file = catching_up_marker(dir.path()).expect("the marker is written for a fresh member");
    assert!(file.contains("seeded_empty"), "{file}");
    assert_eq!(
        catching_up_gauge(&run, &client).await,
        ["kimmy_catching_up{reason=\"seeded_empty\"} 1"]
    );

    // The wait runs out with no peer reached: ready, and honest about it.
    until_ready_is(&run, &client, "200 unknown", |status, header, body| {
        status == 200 && header.as_deref() == Some("unknown") && body["catching_up"] == "unknown"
    })
    .await;
    assert!(catching_up_marker(dir.path()).is_some(), "unknown keeps the marker");
    assert_eq!(catching_up_gauge(&run, &client).await, ["kimmy_catching_up{reason=\"unknown\"} 1"]);

    // The operator's clear takes effect live, with no restart.
    std::fs::remove_file(dir.path().join("data").join(CATCHING_UP_FILE)).unwrap();
    until_ready_is(&run, &client, "ready and unmarked", |status, header, body| {
        status == 200 && header.is_none() && body.get("catching_up").is_none()
    })
    .await;
    assert_eq!(catching_up_gauge(&run, &client).await, ["kimmy_catching_up{reason=\"none\"} 1"]);
    assert!(run.log().contains("the catching-up marker was cleared"), "{}", run.log());

    run.signal("TERM");
    assert!(run.wait_exit().success());

    // The store exists now: an ordinary restart is not fresh, and is not marked.
    let mut again = Run::spawn_clustered(dir.path(), "again", dead_port(), 3);
    again.wait_ready(&client).await;
    assert!(catching_up_marker(dir.path()).is_none(), "an ordinary restart is never marked");
    assert_eq!(catching_up_gauge(&again, &client).await, ["kimmy_catching_up{reason=\"none\"} 1"]);
    again.signal("TERM");
    assert!(again.wait_exit().success());
}

/// A marker left by a node that was in a cluster does not gate a standalone one:
/// it is discarded at the start, and the node is ready at once.
#[tokio::test]
async fn a_standalone_start_discards_a_stale_marker() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    std::fs::create_dir_all(dir.path().join("data")).unwrap();
    std::fs::write(
        dir.path().join("data").join(CATCHING_UP_FILE),
        "reason = \"restored\"\nsince = 1\n",
    )
    .unwrap();
    let mut run = Run::spawn(dir.path(), "standalone");
    run.wait_ready(&client).await;
    assert!(catching_up_marker(dir.path()).is_none(), "the stale file is removed");
    let (status, header, _) = readyz(&run, &client).await;
    assert_eq!((status, header), (200, None));
    run.signal("TERM");
    assert!(run.wait_exit().success());
}

/// A member that cannot write its marker does not start: it fails before the
/// open like a bind that fails does, with the same exit and the same verdict for
/// the next start. The HTTP port was already bound, so this is the check that
/// the answer is "failed to start" and never "catching up": the store is not
/// created, the banner never follows, and no marker exists.
#[tokio::test]
async fn a_marker_that_cannot_be_written_fails_the_start_before_the_open() {
    let dir = tempfile::tempdir().unwrap();
    // The marker is written by way of a temporary file of this name; a directory
    // there makes the write fail, on any platform and as any user.
    std::fs::create_dir_all(dir.path().join("data").join(format!("{CATCHING_UP_FILE}.tmp")))
        .unwrap();
    let mut failed = Run::spawn_clustered(dir.path(), "unwritable", dead_port(), 3);
    let status = failed.wait_exit();
    assert!(!status.success(), "{status:?}");
    let log = failed.log();
    assert!(log.contains("exiting on an error"), "{log}");
    assert!(log.contains("catching-up marker"), "the line carries the error's text: {log}");
    assert!(!log.contains("starting kimmyd"), "the banner follows the open: {log}");
    assert!(!log.contains("catching up") || !log.contains("serving HTTP"), "{log}");
    assert!(
        !dir.path().join("data").join("kimmy.redb").exists(),
        "a marker that cannot be written must not let the store be created"
    );
    assert!(!dir.path().join("data").join(CATCHING_UP_FILE).exists());
    let recorded = marker(dir.path()).expect("an error exit records itself");
    assert!(recorded.contains("exit = \"error\""), "{recorded}");

    // The same exit as a bind that fails: one before-open class.
    let other = tempfile::tempdir().unwrap();
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut unbound = Run::spawn_on(other.path(), "unbindable", taken.local_addr().unwrap().port());
    assert_eq!(unbound.wait_exit().code(), status.code());
}

/// Seeds that name only this member name nobody to catch up from: a fresh member
/// whose static seed list is its own cluster address is ready at once and not
/// marked, where a replicas=1 first start would otherwise wait out the bound. With
/// a second seed that is a real (dead) peer it is marked, and refuses.
#[tokio::test]
async fn a_fresh_member_seeded_only_with_itself_is_not_marked_and_one_with_another_seed_is() {
    let client = reqwest::Client::new();

    let alone = tempfile::tempdir().unwrap();
    let cluster = ports::choose();
    let mut run = Run::spawn_clustered_on(alone.path(), "alone", cluster, &[cluster], 120);
    run.wait_ready(&client).await;
    assert!(catching_up_marker(alone.path()).is_none(), "nobody else can hold its data");
    assert_eq!(catching_up_gauge(&run, &client).await, ["kimmy_catching_up{reason=\"none\"} 1"]);
    let (status, header, _) = readyz(&run, &client).await;
    assert_eq!((status, header), (200, None));
    run.signal("TERM");
    assert!(run.wait_exit().success());

    let with_peer = tempfile::tempdir().unwrap();
    let cluster = ports::choose();
    let run = Run::spawn_clustered_on(
        with_peer.path(),
        "with-peer",
        cluster,
        &[cluster, dead_port()],
        120,
    );
    let deadline = Instant::now() + PATIENCE;
    while run.http.get().is_none() {
        let bound = ports::bound_http_port(&run.stdout, ports::BOUND_HTTP_LINE, run.pid, &[]);
        if let Ok(Some(port)) = ports::LineWait::default().judge(bound, ports::BOUND_HTTP_LINE) {
            let _ = run.http.set(port);
        }
        assert!(Instant::now() < deadline, "no port; log: {}", run.log());
        tokio::time::sleep(POLL).await;
    }
    until_ready_is(&run, &client, "503 catching_up seeded_empty", |status, header, body| {
        status == 503 && header.as_deref() == Some("seeded_empty") && body["error"] == "catching_up"
    })
    .await;
    assert!(catching_up_marker(with_peer.path()).is_some());
}

/// A member on a wildcard bind whose only seed names it by a loopback address dials
/// itself. It is not a peer: the contact is ended, the member is marked as any
/// fresh member with a seed is, and with nobody else reached it serves as `unknown`
/// once the wait runs out. It must never clear by the mutual clear against itself,
/// which it did before the transport refused a contact with its own node id, before
/// HTTP served.
#[tokio::test]
async fn a_member_whose_seed_reaches_itself_is_not_cleared_by_itself() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let cluster = ports::choose();
    let mut run =
        Run::spawn_clustered_at(dir.path(), "self-seeded", "0.0.0.0", cluster, &[cluster], 3);
    let deadline = Instant::now() + PATIENCE;
    while run.http.get().is_none() {
        let bound = ports::bound_http_port(&run.stdout, ports::BOUND_HTTP_LINE, run.pid, &[]);
        if let Ok(Some(port)) = ports::LineWait::default().judge(bound, ports::BOUND_HTTP_LINE) {
            let _ = run.http.set(port);
        }
        assert!(Instant::now() < deadline, "no port; log: {}", run.log());
        tokio::time::sleep(POLL).await;
    }
    until_ready_is(&run, &client, "503 catching_up seeded_empty", |status, header, _| {
        status == 503 && header.as_deref() == Some("seeded_empty")
    })
    .await;
    // Longer than the discovery interval, so it has dialled itself more than once.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    until_ready_is(&run, &client, "200 unknown", |status, header, _| {
        status == 200 && header.as_deref() == Some("unknown")
    })
    .await;
    let log = run.log();
    assert!(
        log.contains("a connection reached a node with this node's own id"),
        "the self-contact is said once: {log}"
    );
    assert!(
        !log.contains("the catching-up marker was cleared"),
        "a member cleared its own marker against itself: {log}"
    );
    assert!(catching_up_marker(dir.path()).is_some(), "still set");
    // It is not a peer that failed: no failed round counted, no back-off, and no
    // warning per tick.
    let port = *run.http.get().unwrap();
    let page = client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        page.lines().any(|l| l == "kimmy_sync_failures_total 0"),
        "a self-contact was counted as a failed round:\n{page}"
    );
    assert!(!log.contains("sync round failed"), "and warned about: {log}");
    run.signal("TERM");
    assert!(run.wait_exit().success());
}

// ---------------------------------------------------------------------------
// A restored store (ADR-202)
// ---------------------------------------------------------------------------

/// A backup of a small store, and the config a `restore` into `dir/data` reads.
fn backup_and_config(dir: &Path) -> (PathBuf, PathBuf) {
    let source = kimmy_storage::Engine::open(&dir.join("source.redb")).unwrap();
    let orders = source.create_collection("shop", "orders").unwrap();
    source.insert(&orders, bson::doc! { "_id": 1 }).unwrap();
    let backup = dir.join("backup.bin");
    source
        .backup_to(&mut std::fs::File::create(&backup).unwrap(), kimmy_storage::WalkScope::Request)
        .unwrap();
    let config = dir.join("restore.toml");
    std::fs::write(
        &config,
        format!(
            "[server]\nbind = \"127.0.0.1:0\"\n\n[storage]\ndata_dir = \"{}\"\n",
            dir.join("data").display()
        ),
    )
    .unwrap();
    (backup, config)
}

/// `kimmyd restore`, run to its end.
fn restore(config: &Path, backup: &Path) -> std::process::Output {
    let out = Command::new(env!("CARGO_BIN_EXE_kimmyd"))
        .arg("--config")
        .arg(config)
        .arg("restore")
        .arg("--from")
        .arg(backup)
        .env_remove("RUST_LOG")
        .output()
        .expect("running kimmyd restore");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    out
}

/// A restore writes the marker, and a standalone start after it discards it and
/// is ready and unmarked: the case the restore command has always served, which a
/// marker that outlived it would turn into a node that answers `503` for two
/// minutes.
#[tokio::test]
async fn a_standalone_start_after_a_restore_is_ready_and_unmarked() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let (backup, config) = backup_and_config(dir.path());
    let out = restore(&config, &backup);
    assert!(String::from_utf8_lossy(&out.stderr).contains("catching up"), "the restore says so");
    let file = catching_up_marker(dir.path()).expect("a restore marks the node");
    assert!(file.contains("restored"), "{file}");
    let floor = dir.path().join("data").join("kimmy.replay-floor");
    assert!(floor.exists(), "and leaves the replay the backup's position to ask from");
    let source = kimmy_storage::Engine::open(&dir.path().join("source.redb")).unwrap();
    assert_eq!(
        kimmy_cluster::catchup::replay_floor_on_disk(&dir.path().join("data")),
        Some(source.version_vector().unwrap().get(source.node_id())),
        "the floor is the backup's own-origin position"
    );
    drop(source);

    let mut run = Run::spawn(dir.path(), "standalone-after-restore");
    run.wait_ready(&client).await;
    assert!(catching_up_marker(dir.path()).is_none(), "nobody to catch up from: discarded");
    assert!(!floor.exists(), "and with clustering off there is no replay to keep a floor for");
    let (status, header, _) = readyz(&run, &client).await;
    assert_eq!((status, header), (200, None));
    assert_eq!(catching_up_gauge(&run, &client).await, ["kimmy_catching_up{reason=\"none\"} 1"]);
    run.signal("TERM");
    assert!(run.wait_exit().success());
}

/// A restored member with peers is marked `restored` (a store exists, so it is not
/// `seeded_empty`), refuses requests and is not ready, and with nobody reachable
/// serves as `unknown` once the wait runs out.
#[tokio::test]
async fn a_restored_member_with_peers_is_marked_restored_and_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let (backup, config) = backup_and_config(dir.path());
    restore(&config, &backup);

    let mut run = Run::spawn_clustered(dir.path(), "restored-with-peers", dead_port(), 3);
    let deadline = Instant::now() + PATIENCE;
    while run.http.get().is_none() {
        let bound = ports::bound_http_port(&run.stdout, ports::BOUND_HTTP_LINE, run.pid, &[]);
        if let Ok(Some(port)) = ports::LineWait::default().judge(bound, ports::BOUND_HTTP_LINE) {
            let _ = run.http.set(port);
        }
        assert!(Instant::now() < deadline, "no port; log: {}", run.log());
        tokio::time::sleep(POLL).await;
    }
    until_ready_is(&run, &client, "503 catching_up restored", |status, header, body| {
        status == 503 && header.as_deref() == Some("restored") && body["error"] == "catching_up"
    })
    .await;
    assert_eq!(
        catching_up_gauge(&run, &client).await,
        ["kimmy_catching_up{reason=\"restored\"} 1"]
    );
    until_ready_is(&run, &client, "200 unknown", |status, header, _| {
        status == 200 && header.as_deref() == Some("unknown")
    })
    .await;
    // Nobody has answered the replay, so the floor it asks from is still on disk.
    assert!(
        dir.path().join("data").join("kimmy.replay-floor").exists(),
        "the replay floor is kept until every member that can answer has"
    );
    run.signal("TERM");
    assert!(run.wait_exit().success());
}

/// A replay floor that cannot be written fails the start, as the marker does:
/// a start that went on without it would lose what it could not record. It
/// fails as every other start does: the exit is logged and recorded as an
/// error, and the next start reports the failed start with the floor's error.
/// (A directory where the file belongs is unreadable as a floor, which counts as
/// the lowest, and the clamped floor then has to be written over it.)
#[tokio::test]
async fn a_replay_floor_that_cannot_be_written_fails_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let client = reqwest::Client::new();
    let floor = dir.path().join("data").join("kimmy.replay-floor");
    std::fs::create_dir_all(&floor).unwrap();
    let mut run = Run::spawn_clustered(dir.path(), "unwritable-floor", dead_port(), 3);
    let status = run.wait_exit();
    assert!(!status.success(), "{status:?}");
    let log = run.log();
    assert!(log.contains("exiting on an error"), "{log}");
    assert!(log.contains("replay floor"), "the failure names the floor: {log}");
    let recorded = marker(dir.path()).expect("an error exit records itself");
    assert!(recorded.contains("exit = \"error\""), "{recorded}");

    std::fs::remove_dir(&floor).unwrap();
    let mut next = Run::spawn(dir.path(), "after-unwritable-floor");
    next.wait_ready(&client).await;
    let log = next.log();
    let line = log
        .lines()
        .find(|l| l.contains("the previous start failed before it served"))
        .unwrap_or_else(|| panic!("no failed-start line:\n{log}"));
    assert!(line.contains("WARN") && line.contains("replay floor"), "{line}");
    assert!(!log.contains("did not shut down cleanly"), "{log}");
    next.signal("TERM");
    assert!(next.wait_exit().success());
}
