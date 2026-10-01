//! `kimmyd check-store` through the shipped binary (ADR-204): its exit codes,
//! the one JSON line on stdout, and what it does beside a live node, a node
//! that starts during it, and a kill. What a check finds on each kind of store
//! is tested in kimmy-storage; here the claims are about processes.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

mod ports;

const JWT_SECRET: &str = "a-check-store-harness-jwt-secret-value";
const ROOT_PASSWORD: &str = "harness-root-password";
const PATIENCE: Duration = Duration::from_secs(60);

/// A data directory and the config that names it.
struct Dir {
    tmp: tempfile::TempDir,
    http: u16,
}

impl Dir {
    fn new() -> Dir {
        let tmp = tempfile::tempdir().unwrap();
        let http = ports::choose();
        let config = format!(
            "[server]\nbind = \"127.0.0.1:{http}\"\n\n[storage]\ndata_dir = \"{}\"\n\n[auth]\n\
             jwt_secret = \"{JWT_SECRET}\"\n",
            tmp.path().join("data").display()
        );
        std::fs::create_dir(tmp.path().join("data")).unwrap();
        std::fs::write(tmp.path().join("kimmy.toml"), config).unwrap();
        Dir { tmp, http }
    }

    fn data(&self) -> PathBuf {
        self.tmp.path().join("data")
    }

    fn store(&self) -> PathBuf {
        self.data().join("kimmy.redb")
    }

    fn command(&self, env: &[(&str, &str)]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_kimmyd"));
        command
            .arg("--config")
            .arg(self.tmp.path().join("kimmy.toml"))
            .env("KIMMY_ROOT_PASSWORD", ROOT_PASSWORD)
            .env_remove("RUST_LOG");
        for (key, value) in env {
            command.env(key, value);
        }
        command
    }

    /// Run `check-store` to its end.
    fn check(&self, env: &[(&str, &str)]) -> Checked {
        let output = self.command(env).arg("check-store").output().unwrap();
        Checked::from(output)
    }

    /// Start `check-store`, its stdout and stderr to files.
    fn spawn_check(&self, env: &[(&str, &str)]) -> (Child, PathBuf, PathBuf) {
        let stdout = self.tmp.path().join("check.stdout");
        let stderr = self.tmp.path().join("check.stderr");
        let child = self
            .command(env)
            .arg("check-store")
            .stdout(Stdio::from(std::fs::File::create(&stdout).unwrap()))
            .stderr(Stdio::from(std::fs::File::create(&stderr).unwrap()))
            .spawn()
            .unwrap();
        (child, stdout, stderr)
    }

    /// Start a node, its log to `<name>.log`.
    fn spawn_node(&self, name: &str) -> Node {
        let log = self.tmp.path().join(format!("{name}.log"));
        let child = self
            .command(&[])
            .stdout(Stdio::from(std::fs::File::create(&log).unwrap()))
            .stderr(Stdio::from(std::fs::File::create(log.with_extension("stderr")).unwrap()))
            .spawn()
            .unwrap();
        Node { child, log, http: self.http }
    }

    /// Every file in the data directory: name, contents and modification time.
    fn listing(&self) -> Vec<(String, Vec<u8>, std::time::SystemTime)> {
        let mut files: Vec<_> = std::fs::read_dir(self.data())
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                (
                    e.file_name().to_string_lossy().into_owned(),
                    std::fs::read(e.path()).unwrap_or_default(),
                    e.metadata().unwrap().modified().unwrap(),
                )
            })
            .collect();
        files.sort();
        files
    }

    fn write_marker(&self, exit: &str) {
        std::fs::write(
            self.data().join("kimmy.last-exit"),
            format!(
                "exit = \"{exit}\"\npid = 1\nversion = \"0.0.0\"\ncommit = \"test\"\nat_ms = 1\n"
            ),
        )
        .unwrap();
    }
}

/// A finished check: its exit code, its stdout's one line parsed, its stderr.
struct Checked {
    code: Option<i32>,
    json: serde_json::Value,
    stderr: String,
}

impl From<Output> for Checked {
    fn from(output: Output) -> Self {
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        let lines: Vec<&str> = stdout.lines().collect();
        assert_eq!(lines.len(), 1, "stdout is one line: {stdout:?}; stderr: {stderr}");
        let json: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        for key in ["verdict", "detail", "unclean_close", "rolled_back", "elapsed_ms", "bytes_read"]
        {
            assert!(json.get(key).is_some(), "{key} is missing: {json}");
        }
        Checked { code: output.status.code(), json, stderr }
    }
}

impl Checked {
    fn verdict(&self) -> &str {
        self.json["verdict"].as_str().unwrap()
    }
}

struct Node {
    child: Child,
    log: PathBuf,
    http: u16,
}

impl Node {
    /// The log as written, with the pretty format's colour escapes removed so
    /// a field reads as `name=value`.
    fn log(&self) -> String {
        let raw = std::fs::read_to_string(&self.log).unwrap_or_default();
        let mut out = String::with_capacity(raw.len());
        let mut chars = raw.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\x1b' && chars.peek() == Some(&'[') {
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

    async fn wait_ready(&mut self) {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + PATIENCE;
        loop {
            if let Ok(res) =
                client.get(format!("http://127.0.0.1:{}/readyz", self.http)).send().await
                && res.status().is_success()
            {
                return;
            }
            if let Ok(Some(status)) = self.child.try_wait() {
                panic!("the node exited ({status}) before it was ready: {}", self.log());
            }
            assert!(Instant::now() < deadline, "never ready: {}", self.log());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn signal(&self, sig: &str) {
        let status =
            Command::new("kill").arg(format!("-{sig}")).arg(self.child.id().to_string()).status();
        assert!(status.unwrap().success());
    }

    fn wait_exit(&mut self) -> std::process::ExitStatus {
        wait(&mut self.child)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + PATIENCE;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "did not exit");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Wait until `path` holds `needle`.
fn wait_for(path: &Path, needle: &str) {
    let deadline = Instant::now() + PATIENCE;
    while !std::fs::read_to_string(path).unwrap_or_default().contains(needle) {
        assert!(Instant::now() < deadline, "{} never said {needle:?}", path.display());
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Two values, each in its own commit, the second the latest; ASCII, so a
/// flipped low bit still decodes.
const OLDER: &str = "older-marker-0123456789-abcdefghijklmnopqrstuvwxyz-end";
const LATEST: &str = "latest-marker-9876543210-zyxwvutsrqponmlkjihgfedcba-end";

fn fill(engine: &kimmy_storage::Engine, docs: usize) {
    let c = engine.create_collection("shop", "orders").unwrap();
    let batch = (0..docs)
        .map(|i| bson::doc! { "_id": format!("d{i}"), "pad": format!("padding-{i:08}-padding") })
        .collect();
    engine.insert_many(&c, batch).unwrap();
    engine.insert(&c, bson::doc! { "_id": "a", "v": OLDER }).unwrap();
    engine.insert(&c, bson::doc! { "_id": "b", "v": LATEST }).unwrap();
}

/// A store an engine closed cleanly, behind a marker saying so.
fn a_clean_store(docs: usize) -> Dir {
    let dir = Dir::new();
    fill(&kimmy_storage::Engine::open(&dir.store()).unwrap(), docs);
    dir.write_marker("shutdown");
    dir
}

/// A store copied while an engine had it open, as a kill leaves it, with no
/// marker.
fn an_unclean_store() -> Dir {
    let dir = Dir::new();
    let live = dir.tmp.path().join("live.redb");
    let engine = kimmy_storage::Engine::open(&live).unwrap();
    fill(&engine, 500);
    std::fs::copy(&live, dir.store()).unwrap();
    std::fs::copy(live.with_extension("format"), dir.data().join("kimmy.format")).unwrap();
    drop(engine);
    dir
}

/// Flip the low bit of a byte inside every copy of `needle` in the store,
/// leaving its checksums as they were.
fn flip(dir: &Dir, needle: &str) {
    let mut bytes = std::fs::read(dir.store()).unwrap();
    let at: Vec<usize> = bytes
        .windows(needle.len())
        .enumerate()
        .filter(|(_, w)| *w == needle.as_bytes())
        .map(|(i, _)| i)
        .collect();
    assert!(!at.is_empty());
    for i in at {
        bytes[i + 16] ^= 1;
    }
    std::fs::write(dir.store(), &bytes).unwrap();
}

#[test]
fn a_clean_store_exits_0_with_one_json_line_and_logs_on_stderr() {
    let dir = a_clean_store(500);
    let before = dir.listing();
    let checked = dir.check(&[]);
    assert_eq!(checked.code, Some(0), "{} {}", checked.json, checked.stderr);
    assert_eq!(checked.verdict(), "clean");
    assert_eq!(checked.json["unclean_close"], false);
    assert!(checked.json["bytes_read"].as_u64().unwrap() > 0);
    assert!(checked.stderr.contains("checking the store"), "the log is on stderr");
    assert_eq!(dir.listing(), before, "nothing in the data directory changed");
}

#[test]
fn a_flipped_bit_in_a_clean_store_exits_65_untouched() {
    let dir = a_clean_store(500);
    flip(&dir, OLDER);
    let before = dir.listing();
    let checked = dir.check(&[]);
    assert_eq!(checked.code, Some(65), "{}", checked.json);
    assert_eq!(checked.verdict(), "damaged");
    assert!(checked.json["detail"].as_str().unwrap().contains("catch up from its peers"));
    assert_eq!(dir.listing(), before);
}

/// The previous run's marker decides what a rollback at the open means: with
/// none, the torn write a kill leaves (exit 0); after a clean shutdown,
/// damage (exit 65).
#[test]
fn a_rolled_back_latest_commit_is_judged_by_the_exit_marker() {
    let dir = an_unclean_store();
    flip(&dir, LATEST);
    let before = dir.listing();
    let checked = dir.check(&[]);
    assert_eq!(checked.code, Some(0), "{}", checked.json);
    assert_eq!(checked.verdict(), "rolled_back_after_unclean_stop");
    assert_eq!(checked.json["unclean_close"], true);
    assert_eq!(checked.json["rolled_back"], true);
    assert_eq!(dir.listing(), before);

    dir.write_marker("shutdown");
    let before = dir.listing();
    let checked = dir.check(&[]);
    assert_eq!(checked.code, Some(65), "{}", checked.json);
    assert_eq!(checked.json["rolled_back"], true);
    assert_eq!(dir.listing(), before);

    // A marker that is not a clean end is read as the kill it may have been.
    dir.write_marker("storage_not_closed");
    assert_eq!(dir.check(&[]).code, Some(0));
}

#[test]
fn a_missing_store_exits_1_and_nothing_is_created() {
    let dir = Dir::new();
    let checked = dir.check(&[]);
    assert_eq!(checked.code, Some(1), "{}", checked.json);
    assert_eq!(checked.verdict(), "not_checked");
    assert!(dir.listing().is_empty(), "{:?}", dir.listing());
}

/// A store a newer build wrote exits 1, never 65: it is healthy.
#[test]
fn a_store_a_newer_build_wrote_exits_1() {
    let dir = a_clean_store(10);
    let sidecar = dir.data().join("kimmy.format");
    let stamped = std::fs::read_to_string(&sidecar).unwrap();
    let newer: String = stamped
        .lines()
        .map(
            |l| if l.starts_with("schema = ") { "schema = 250\n".into() } else { format!("{l}\n") },
        )
        .collect();
    std::fs::write(&sidecar, newer).unwrap();
    let before = dir.listing();
    let checked = dir.check(&[]);
    assert_eq!(checked.code, Some(1), "{}", checked.json);
    assert!(checked.json["detail"].as_str().unwrap().contains("newer build"));
    assert_eq!(dir.listing(), before);
}

/// The config's serving checks do not apply: no root password, no secret.
#[test]
fn the_command_needs_no_serving_configuration() {
    let dir = a_clean_store(10);
    let output = Command::new(env!("CARGO_BIN_EXE_kimmyd"))
        .arg("--data-dir")
        .arg(dir.data())
        .arg("check-store")
        .env_remove("KIMMY_ROOT_PASSWORD")
        .env_remove("KIMMY_JWT_SECRET")
        .env_remove("KIMMY_CONFIG")
        .output()
        .unwrap();
    let checked = Checked::from(output);
    assert_eq!(checked.code, Some(0), "{} {}", checked.json, checked.stderr);
}

#[tokio::test]
async fn a_live_node_holds_the_store_and_the_check_exits_1() {
    let dir = a_clean_store(10);
    let mut node = dir.spawn_node("live");
    node.wait_ready().await;
    let checked = dir.check(&[]);
    assert_eq!(checked.code, Some(1), "{}", checked.json);
    assert!(checked.json["detail"].as_str().unwrap().contains("open in another process"));
    assert_eq!(checked.json["bytes_read"], 0);
    node.signal("TERM");
    assert!(node.wait_exit().success(), "{}", node.log());
}

/// A node started while a check runs meets the store's lock, is refused as
/// in use, and leaves the marker as it was; the check finishes clean.
#[tokio::test]
async fn a_node_started_during_a_check_is_refused_and_the_check_finishes() {
    let dir = a_clean_store(2_000);
    let marker = std::fs::read(dir.data().join("kimmy.last-exit")).unwrap();
    // About 7 s of reads, so a debug node on a loaded host reaches its open
    // well inside the check.
    let (mut check, stdout, stderr) = dir.spawn_check(&[("KIMMY_TEST_CHECK_READ_MS", "10")]);
    wait_for(&stderr, "checking the store");
    let mut node = dir.spawn_node("refused");
    let status = node.wait_exit();
    assert!(!status.success(), "{}", node.log());
    assert!(node.log().contains("is open in another process"), "{}", node.log());
    assert_eq!(std::fs::read(dir.data().join("kimmy.last-exit")).unwrap(), marker);
    assert!(check.try_wait().unwrap().is_none(), "the check was still running");
    let status = wait(&mut check);
    let line = std::fs::read_to_string(&stdout).unwrap();
    assert_eq!(status.code(), Some(0), "{line} {}", std::fs::read_to_string(&stderr).unwrap());
    assert!(line.contains("\"verdict\":\"clean\""), "{line}");
}

#[test]
fn a_kill_during_a_check_leaves_the_store_untouched() {
    let dir = a_clean_store(2_000);
    let before = dir.listing();
    let (mut check, _, stderr) = dir.spawn_check(&[("KIMMY_TEST_CHECK_READ_MS", "2")]);
    wait_for(&stderr, "checking the store");
    std::thread::sleep(Duration::from_millis(300));
    assert!(check.try_wait().unwrap().is_none(), "the check was still running");
    check.kill().unwrap();
    check.wait().unwrap();
    assert_eq!(dir.listing(), before);
    // And the store is still checkable, clean.
    assert_eq!(dir.check(&[]).code, Some(0));
}

#[test]
fn a_stop_signal_during_a_check_exits_1_untouched() {
    let dir = a_clean_store(2_000);
    let before = dir.listing();
    let (mut check, stdout, stderr) = dir.spawn_check(&[("KIMMY_TEST_CHECK_READ_MS", "2")]);
    wait_for(&stderr, "checking the store");
    std::thread::sleep(Duration::from_millis(300));
    let started = Instant::now();
    let status = Command::new("kill").arg("-TERM").arg(check.id().to_string()).status();
    assert!(status.unwrap().success());
    let status = wait(&mut check);
    let line = std::fs::read_to_string(&stdout).unwrap();
    assert_eq!(status.code(), Some(1), "{line}");
    assert!(line.contains("stopped"), "{line}");
    assert!(started.elapsed() < Duration::from_secs(5), "{:?}", started.elapsed());
    assert_eq!(dir.listing(), before);
}

/// A node killed with writes in flight leaves a store that was not closed:
/// the check repairs it in memory only, and the next start repairs it for
/// real and says so.
#[tokio::test]
async fn a_node_killed_mid_write_is_checked_untouched_and_the_next_start_repairs() {
    let dir = Dir::new();
    let mut node = dir.spawn_node("killed");
    node.wait_ready().await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", dir.http);
    let token: serde_json::Value = client
        .post(format!("{base}/v1/auth/login"))
        .json(&serde_json::json!({"user": "root", "password": ROOT_PASSWORD}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = token["token"].as_str().unwrap().to_string();
    client
        .post(format!("{base}/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({"name": "orders"}))
        .send()
        .await
        .unwrap();
    let writer = {
        let (client, base, token) = (client.clone(), base.clone(), token.clone());
        tokio::spawn(async move {
            for i in 0.. {
                let docs: Vec<_> =
                    (0..50).map(|j| serde_json::json!({"_id": format!("w{i}-{j}")})).collect();
                let sent = client
                    .post(format!("{base}/v1/db/shop/coll/orders/bulk"))
                    .bearer_auth(&token)
                    .json(&docs)
                    .send()
                    .await;
                if sent.is_err() {
                    break;
                }
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    node.signal("KILL");
    node.wait_exit();
    let _ = writer.await;

    let before = dir.listing();
    let checked = dir.check(&[]);
    assert_eq!(checked.code, Some(0), "{} {}", checked.json, checked.stderr);
    assert_eq!(checked.json["unclean_close"], true, "{}", checked.json);
    assert_eq!(dir.listing(), before, "the check repaired nothing on disk");

    let mut next = dir.spawn_node("next");
    next.wait_ready().await;
    let log = next.log();
    assert!(log.contains("repairing the database after an unclean stop"), "{log}");
    // The repair is named with its outcome, in the log and on /metrics: a
    // killed node's acknowledged commits are durable, so nothing is rolled
    // back.
    let line = log
        .lines()
        .find(|l| l.contains("database repaired after an unclean stop"))
        .unwrap_or_else(|| panic!("no repaired line: {log}"));
    assert!(line.contains("WARN") && line.contains("rolled_back=false"), "{line}");
    let metrics = client
        .get(format!("{base}/metrics"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("kimmy_store_repairs_total{rolled_back=\"false\"} 1\n"), "{metrics}");
    assert!(metrics.contains("kimmy_store_repairs_total{rolled_back=\"true\"} 0\n"), "{metrics}");
    next.signal("TERM");
    assert!(next.wait_exit().success());

    // A clean stop leaves nothing to repair, and the next start says none.
    let mut clean = dir.spawn_node("clean");
    clean.wait_ready().await;
    let metrics = client.get(format!("{base}/metrics")).send().await.unwrap().text().await.unwrap();
    assert!(metrics.contains("kimmy_store_repairs_total{rolled_back=\"false\"} 0\n"), "{metrics}");
    assert!(!clean.log().contains("database repaired"), "{}", clean.log());
    clean.signal("TERM");
    assert!(clean.wait_exit().success());
}
