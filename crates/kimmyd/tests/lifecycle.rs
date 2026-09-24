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
        Run::spawn_full(dir, name, http, extra_env, false)
    }

    /// [`Run::spawn_with`], with the node's stderr a pipe whose reader is
    /// already closed, so every write the node makes to it fails with EPIPE:
    /// the stderr a process has when whatever was reading it has gone.
    fn spawn_with_stderr_closed(dir: &Path, name: &str, env: &[(&str, &str)]) -> Run {
        Run::spawn_full(dir, name, 0, env, true)
    }

    fn spawn_full(
        dir: &Path,
        name: &str,
        http: u16,
        extra_env: &[(&str, &str)],
        stderr_closed: bool,
    ) -> Run {
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

        let mut command = Command::new(env!("CARGO_BIN_EXE_kimmyd"));
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

    /// Wait until the node answers `/healthz`, failing at once, with its
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
                && let Ok(res) = client.get(format!("http://127.0.0.1:{port}/healthz")).send().await
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

/// Hold a port, so a start opens its database, cannot bind, and fails
/// before it serves; return the start's log.
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
