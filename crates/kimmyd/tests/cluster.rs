//! The cluster verification harness: real `kimmyd` processes, driven the way
//! an operator's cluster actually runs.
//!
//! Every automated test before this one was transport-free or single-node by
//! design, and the two worst cluster bugs to date — gossip silently never
//! forming under the shipped compose file, and the collection-id encoding
//! that broke replication for ~48% of names — were both found only by
//! hand-driving real processes. This harness makes that discovery repeatable.
//!
//! # What is asserted, and why it is observable
//!
//! Membership is read from the `kimmy_cluster_members` gauge, added for this
//! harness: replication converging is **not** proof gossip formed, because
//! discovery alone can carry convergence while SWIM sits dark — which is
//! exactly what the compose bug did. The gauge reads the same `Members`
//! snapshot ownership derives from, so what these tests assert is what the
//! webhook dispatcher actually uses.
//!
//! # Why `SIGSTOP` stands in for a partition
//!
//! A stopped process holds its sockets but answers nothing — which is what a
//! partitioned peer looks like to SWIM: alive by every local record, silent on
//! the wire. Suspicion, declaration, and recovery on `SIGCONT` are the whole
//! partition story without container plumbing.
//!
//! # Running
//!
//! Ignored by default — each test boots a three-node cluster and waits on
//! real gossip timing. CI runs them explicitly:
//!
//! ```text
//! cargo test -p kimmyd --test cluster -- --ignored --test-threads=1
//! ```

#![cfg(unix)]

use std::process::{Child, Command, Stdio};
use std::time::Duration;

mod node_logs;
mod ports;

const ROOT_PASSWORD: &str = "harness-root-password";
const JWT_SECRET: &str = "a-shared-harness-jwt-secret-value";
const CLUSTER_SECRET: &str = "a-shared-harness-cluster-secret";

/// How long a condition may take before the harness gives up on it.
///
/// Generous on purpose: these tests run on loaded CI machines, and a timeout
/// that flakes teaches people to rerun failures instead of reading them. The
/// happy path resolves in a few seconds; the budget only matters when
/// something is genuinely wrong.
///
/// From `KIMMY_TEST_PATIENCE_SECS` when set, so CI can raise it without moving
/// the default a developer runs against. A local run resolves in a few seconds
/// and a longer budget would only make a genuine hang slower to report; a
/// two-core CI runner starting three daemons is a different machine and gets a
/// different number.
const DEFAULT_PATIENCE_SECS: u64 = 90;

fn patience() -> Duration {
    Duration::from_secs(
        std::env::var("KIMMY_TEST_PATIENCE_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_PATIENCE_SECS),
    )
}
const POLL: Duration = Duration::from_millis(250);

/// One spawned `kimmyd`, killed on drop.
struct Node {
    name: &'static str,
    /// Behind a lock so `wait_ready`, which only reads the node, can ask
    /// whether it has exited.
    child: std::sync::Mutex<Child>,
    pid: u32,
    cluster: u16,
    /// Chosen by [`ports::choose`], as the cluster port is, and not port 0:
    /// a clustered node advertises its configured HTTP address to its peers'
    /// topology (ADR-051), and `every_node_can_tell_a_client_about_every_node`
    /// checks that address is where it really listens.
    http: u16,
    dir: tempfile::TempDir,
}

impl Node {
    /// Spawn a node with clustering on, at a pre-allocated cluster port,
    /// seeded with the given cluster ports.
    ///
    /// The cluster port is chosen by the caller because seed lists are
    /// mutual: a node with `cluster.enabled` and no seeds refuses to start —
    /// the harness learned that by watching node A die on its first run — so
    /// every node's seed must name a port that exists before anything spawns.
    ///
    /// Sync and discovery intervals are shortened so the tests wait on
    /// gossip's own timing, not on configuration chosen for production.
    ///
    /// `storage` is added to the node's `[storage]` section, for a test that
    /// needs a setting the other tests do not.
    ///
    /// The sync interval is the test's: 1 s for most, and a test proving
    /// something about the gap *between* ticks needs the interval wide enough
    /// that the gap it is measuring cannot be mistaken for an ordinary tick
    /// boundary.
    fn spawn_with_interval(
        name: &'static str,
        cluster: u16,
        seeds: &[u16],
        storage: &str,
        sync_interval_secs: u64,
    ) -> Node {
        Self::spawn_configured(name, cluster, seeds, storage, sync_interval_secs, "")
    }

    /// [`Self::spawn_with_interval`], with `cluster_extra` added to the node's
    /// `[cluster]` section: `catch_up_wait_secs`, `expected_members`.
    fn spawn_configured(
        name: &'static str,
        cluster: u16,
        seeds: &[u16],
        storage: &str,
        sync_interval_secs: u64,
        cluster_extra: &str,
    ) -> Node {
        let dir = tempfile::tempdir().unwrap();
        let http = ports::choose();
        let seed_list =
            seeds.iter().map(|p| format!("\"127.0.0.1:{p}\"")).collect::<Vec<_>>().join(", ");

        let config = format!(
            r#"
[server]
bind = "127.0.0.1:{http}"

[storage]
data_dir = "{data}"
# Expiry runs every second rather than every sixty, so a TTL test waits on
# ownership settling rather than on a production cadence.
ttl_interval_secs = 1
{storage}

[auth]
jwt_secret = "{JWT_SECRET}"

[cluster]
enabled = true
bind = "127.0.0.1:{cluster}"
seeds = [{seed_list}]
cluster_secret = "{CLUSTER_SECRET}"
sync_interval_secs = {sync_interval_secs}
discovery_interval_secs = 2
{cluster_extra}

[webhooks]
allowed_hosts = ["127.0.0.1"]
"#,
            data = dir.path().join("data").display(),
        );
        let config_path = dir.path().join("kimmy.toml");
        std::fs::write(&config_path, config).unwrap();

        // Logs go to files in the scratch dir, so a failing run leaves
        // something to read rather than interleaved noise on the test output.
        let stdout = std::fs::File::create(dir.path().join("stdout.log")).unwrap();
        let stderr = std::fs::File::create(dir.path().join("stderr.log")).unwrap();

        let child = Command::new(env!("CARGO_BIN_EXE_kimmyd"))
            .arg("--config")
            .arg(&config_path)
            .env("KIMMY_ROOT_PASSWORD", ROOT_PASSWORD)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawning kimmyd");

        let pid = child.id();
        Node { name, child: std::sync::Mutex::new(child), pid, cluster, http, dir }
    }

    /// Stop this node with SIGTERM, wait for it, and start it again on the
    /// same data directory and ports, with `env` added. The run before keeps
    /// its logs as `stdout.before.log` and `stderr.before.log`.
    fn restart_with(&mut self, env: &[(&str, &str)]) -> std::process::ExitStatus {
        let exited = self.child.get_mut().unwrap().try_wait().unwrap();
        let status = match exited {
            Some(status) => status,
            None => {
                self.signal("TERM");
                self.wait_exit(Duration::from_secs(60))
            }
        };
        for log in ["stdout", "stderr"] {
            let _ = std::fs::rename(
                self.dir.path().join(format!("{log}.log")),
                self.dir.path().join(format!("{log}.before.log")),
            );
        }
        let stdout = std::fs::File::create(self.dir.path().join("stdout.log")).unwrap();
        let stderr = std::fs::File::create(self.dir.path().join("stderr.log")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_kimmyd"));
        for (key, value) in env {
            command.env(key, value);
        }
        let child = command
            .arg("--config")
            .arg(self.dir.path().join("kimmy.toml"))
            .env("KIMMY_ROOT_PASSWORD", ROOT_PASSWORD)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawning kimmyd");
        self.pid = child.id();
        *self.child.get_mut().unwrap() = child;
        status
    }

    /// Wait up to `within` for the process to end, and report its status.
    fn wait_exit(&self, within: Duration) -> std::process::ExitStatus {
        let deadline = std::time::Instant::now() + within;
        loop {
            if let Some(status) = self.child.lock().unwrap().try_wait().unwrap() {
                return status;
            }
            assert!(std::time::Instant::now() < deadline, "{} did not exit", self.name);
            std::thread::sleep(POLL);
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("stdout.log")).unwrap_or_default()
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }

    /// This node's durable id, read from `/readyz`.
    ///
    /// Webhook ownership hashes node ids rather than addresses (ADR-051), so
    /// the harness has to ask each node who it is rather than deriving it from
    /// where it is listening.
    async fn node_id(&self, client: &reqwest::Client) -> kimmy_core::NodeId {
        let body: serde_json::Value =
            client.get(self.url("/readyz")).send().await.unwrap().json().await.unwrap();
        body["node"].as_str().expect("readyz reports a node id").parse().expect("a valid node id")
    }

    async fn wait_ready(&self, client: &reqwest::Client) {
        if let Err(exited) = self.try_ready(client).await {
            panic!("{exited}");
        }
    }

    /// [`Self::wait_ready`], answering at once, with the node's stderr, when
    /// the node has exited instead of waiting out the patience budget for a
    /// process that is gone. A port taken before the node bound it cost CI
    /// four minutes that way.
    async fn try_ready(&self, client: &reqwest::Client) -> Result<(), Exited> {
        let budget = patience();
        let deadline = std::time::Instant::now() + budget;
        loop {
            if let Ok(res) = client.get(self.url("/readyz")).send().await
                && res.status().is_success()
            {
                return Ok(());
            }
            if let Ok(Some(status)) = self.child.lock().unwrap().try_wait() {
                let stderr =
                    std::fs::read_to_string(self.dir.path().join("stderr.log")).unwrap_or_default();
                return Err(Exited { name: self.name, status, stderr });
            }
            if std::time::Instant::now() >= deadline {
                panic!("{}", self.never_became_healthy(budget));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Why the node never answered, with its own stderr in the message.
    ///
    /// The failure used to name the path to `stderr.log` and stop there. On CI
    /// nothing ever reads that file -- the runner is gone by the time anyone
    /// looks -- so "never became healthy" was indistinguishable between a node
    /// still booting on a loaded two-core machine and one that died with a real
    /// startup error. That ambiguity has cost a human the question "is `main`
    /// broken?" three times.
    ///
    /// **A red that cannot tell a flake from a regression is the same defect as
    /// a green that cannot fail**: the signal does not discriminate, so it
    /// cannot be acted on. The log goes in the panic message, where the CI
    /// output already is.
    fn never_became_healthy(&self, budget: Duration) -> String {
        let path = self.dir.path().join("stderr.log");
        let log = stderr_tail(&path);
        format!(
            "{}: never became healthy within {budget:?}.\n\
             Read the log below before rerunning: empty or ending mid-startup means the \
             runner was slower than the budget, and naming an error means a regression. \
             Raise KIMMY_TEST_PATIENCE_SECS for the former; do not rerun for the latter.\n\
             --- {} stderr ({}) ---\n{log}\n--- end ---",
            self.name,
            self.name,
            path.display(),
        )
    }

    async fn login(&self, client: &reqwest::Client) -> String {
        let res: serde_json::Value = client
            .post(self.url("/v1/auth/login"))
            .json(&serde_json::json!({ "user": "root", "password": ROOT_PASSWORD }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        res["token"].as_str().expect("a token").to_string()
    }

    /// The raw text of one series from `/metrics`, scraped like an operator
    /// would. `None` only when the series itself is absent -- shared by
    /// [`Self::gauge`] and [`Self::gauge_f64`], so there is one place that
    /// reads the wire text rather than two that could drift.
    async fn scrape_value(&self, client: &reqwest::Client, name: &str) -> Option<String> {
        let body = client.get(self.url("/metrics")).send().await.ok()?.text().await.ok()?;
        let prefix = format!("{name} ");
        body.lines()
            .find(|l| l.starts_with(&prefix))
            .and_then(|l| l.split_whitespace().nth(1))
            .map(str::to_owned)
    }

    /// An integer gauge from `/metrics`. `None` only when the series is
    /// absent; a series present but not an integer is a caller's mistake,
    /// not a missing reading, and panics rather than returning `None` --
    /// `kimmy_replication_lag_seconds` and the like render as a float in
    /// seconds, and a `.parse::<u64>()` that silently dropped that to
    /// `None` is what let a real regression hide behind a passing test.
    /// Use [`Self::gauge_f64`] for a series that renders as a float.
    async fn gauge(&self, client: &reqwest::Client, name: &str) -> Option<u64> {
        let raw = self.scrape_value(client, name).await?;
        Some(
            raw.parse()
                .unwrap_or_else(|_| panic!("`{name}` = `{raw}` is not an integer; use gauge_f64")),
        )
    }

    /// [`Self::gauge`], for a gauge rendered as a float (seconds, not
    /// milliseconds) -- `kimmy_replication_lag_seconds` and the like.
    async fn gauge_f64(&self, client: &reqwest::Client, name: &str) -> Option<f64> {
        let raw = self.scrape_value(client, name).await?;
        Some(raw.parse().unwrap_or_else(|_| panic!("`{name}` = `{raw}` is not a float")))
    }

    async fn members_gauge(&self, client: &reqwest::Client) -> Option<u64> {
        self.gauge(client, "kimmy_cluster_members").await
    }

    fn signal(&self, sig: &str) {
        let status =
            Command::new("kill").arg(format!("-{sig}")).arg(self.pid.to_string()).status().unwrap();
        assert!(status.success(), "kill -{sig} {}", self.pid);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // SIGKILL rather than a graceful stop: a stopped (SIGSTOP) child
        // ignores everything else, and these are scratch nodes. Unchecked,
        // unlike `signal` — the child may already be dead by a test's own
        // hand, and panicking in a drop during unwind aborts the test binary.
        let _ = Command::new("kill").arg("-KILL").arg(self.pid.to_string()).status();
        if let Ok(child) = self.child.get_mut() {
            let _ = child.wait();
        }
        // Kept only when the test failed, and only after the kill, so the
        // files are complete. Every assertion but `wait_ready`'s used to lose
        // them with the scratch directory.
        node_logs::keep_if_failing(
            &node_logs::destination(),
            self.name,
            self.pid,
            &[&self.dir.path().join("stdout.log"), &self.dir.path().join("stderr.log")],
        );
    }
}

/// A node that exited before it became healthy.
struct Exited {
    name: &'static str,
    status: std::process::ExitStatus,
    stderr: String,
}

impl std::fmt::Display for Exited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} exited ({}) before it became healthy.\n--- {} stderr ---\n{}\n--- end ---",
            self.name, self.status, self.name, self.stderr
        )
    }
}

/// The tail of a node's `stderr.log`, as a reader needs it in a panic message.
///
/// A node that failed says so at the END, so an early boot line is not what is
/// wanted; and the three states have to be distinguishable, because that is the
/// whole point of printing it — an empty log means the process had not reached a
/// startup error, a log naming one means a regression, and an unreadable file
/// means neither and should say so rather than looking like the first.
fn stderr_tail(path: &std::path::Path) -> String {
    const TAIL: usize = 40;
    match std::fs::read_to_string(path) {
        Ok(text) if text.trim().is_empty() => {
            "<empty: the process wrote nothing, so it had not reached a startup error>".to_string()
        }
        Ok(text) => {
            let lines: Vec<&str> = text.lines().collect();
            let from = lines.len().saturating_sub(TAIL);
            format!("last {} line(s):\n{}", lines.len() - from, lines[from..].join("\n"))
        }
        Err(e) => format!("<could not be read: {e}>"),
    }
}

/// The three states `stderr_tail` has to tell apart.
///
/// Not `#[ignore]`d with the cluster tests: it needs no daemon, and the whole
/// value of the dump is that it discriminates — a message that said the same
/// thing in all three cases would be the ambiguity it was written to remove.
#[test]
fn the_stderr_tail_distinguishes_empty_failing_and_unreadable() {
    let dir = tempfile::tempdir().unwrap();

    let missing = dir.path().join("nothing-here.log");
    let answer = stderr_tail(&missing);
    assert!(answer.contains("could not be read"), "{answer}");

    let empty = dir.path().join("empty.log");
    std::fs::write(&empty, "   \n\n").unwrap();
    let answer = stderr_tail(&empty);
    assert!(answer.contains("had not reached a startup error"), "{answer}");

    // A real startup failure: the reason is the LAST thing written, which is
    // why the tail is what is kept.
    let failed = dir.path().join("failed.log");
    let mut text: String = (0..100).map(|i| format!("boot line {i}\n")).collect();
    text.push_str("Error: address already in use\n");
    std::fs::write(&failed, &text).unwrap();
    let answer = stderr_tail(&failed);
    assert!(answer.contains("address already in use"), "the reason must survive: {answer}");
    assert!(!answer.contains("boot line 0\n"), "and the early noise must not: {answer}");
    assert!(answer.contains("last 40 line(s)"), "{answer}");
}

/// A node's logs outlive its scratch directory when, and only when, the test
/// that spawned it fails. Not `#[ignore]`d either: it needs no daemon.
#[test]
fn a_failing_test_keeps_its_nodes_logs_and_a_passing_one_does_not() {
    struct Spawned<'a> {
        scratch: &'a std::path::Path,
        into: &'a std::path::Path,
        kept: &'a std::cell::Cell<Option<std::path::PathBuf>>,
    }
    impl Drop for Spawned<'_> {
        fn drop(&mut self) {
            let logs = [&self.scratch.join("stdout.log"), &self.scratch.join("stderr.log")];
            self.kept.set(node_logs::keep_if_failing(
                self.into,
                "node-a",
                42,
                &logs.map(|p| p.as_path()),
            ));
        }
    }
    let scratch = tempfile::tempdir().unwrap();
    std::fs::write(scratch.path().join("stdout.log"), "WARN peer connection failed\n").unwrap();
    std::fs::write(scratch.path().join("stderr.log"), "thread panicked\n").unwrap();
    let into = tempfile::tempdir().unwrap();
    let kept = std::cell::Cell::new(None);

    drop(Spawned { scratch: scratch.path(), into: into.path(), kept: &kept });
    assert_eq!(kept.take(), None, "a passing test keeps nothing");
    assert_eq!(std::fs::read_dir(into.path()).unwrap().count(), 0);

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _node = Spawned { scratch: scratch.path(), into: into.path(), kept: &kept };
        panic!("an assertion in the test body");
    }));
    assert!(failed.is_err());
    let at = kept.take().expect("a failing test keeps its nodes' logs");
    assert!(at.starts_with(into.path()) && at.ends_with("node-a-42"), "{}", at.display());
    let read = |name: &str| std::fs::read_to_string(at.join(name)).unwrap();
    assert_eq!(read("stdout.log"), "WARN peer connection failed\n");
    assert_eq!(read("stderr.log"), "thread panicked\n");
}

/// Wait until `condition` holds, or fail with `what` after [`patience`].
async fn eventually<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let budget = patience();
    let deadline = std::time::Instant::now() + budget;
    loop {
        if condition().await {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "gave up waiting for: {what} (after {budget:?})"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// Spawn the standard three-node shape: A and B seed each other, C seeds
/// only A.
///
/// C learning that B exists is therefore only possible through gossip — the
/// seed list never names B to C — which makes membership convergence itself
/// the "an unseeded member is learned" assertion.
async fn three_nodes(client: &reqwest::Client) -> (Node, Node, Node) {
    three_nodes_with(client, "").await
}

/// [`three_nodes`], with `storage` added to each node's `[storage]` section.
///
/// Ports come from [`ports::choose`], which no outbound connection can be
/// given. Should another process take one before its node binds it anyway,
/// the node exits with it in use, and the three start again on new ports:
/// a few attempts, each answered as soon as a node exits.
async fn three_nodes_with(client: &reqwest::Client, storage: &str) -> (Node, Node, Node) {
    three_nodes_configured(client, storage, "").await
}

/// [`three_nodes_with`], with `cluster_extra` added to each node's `[cluster]` section.
async fn three_nodes_configured(
    client: &reqwest::Client,
    storage: &str,
    cluster_extra: &str,
) -> (Node, Node, Node) {
    three_nodes_configured_at(client, storage, cluster_extra, 1).await
}

/// [`three_nodes_with`], every member at `sync_interval_secs`.
async fn three_nodes_at(
    client: &reqwest::Client,
    storage: &str,
    sync_interval_secs: u64,
) -> (Node, Node, Node) {
    three_nodes_configured_at(client, storage, "", sync_interval_secs).await
}

/// [`three_nodes_configured`], every member at `sync_interval_secs`.
async fn three_nodes_configured_at(
    client: &reqwest::Client,
    storage: &str,
    cluster_extra: &str,
    sync_interval_secs: u64,
) -> (Node, Node, Node) {
    const ATTEMPTS: usize = 5;
    for _ in 0..ATTEMPTS {
        let (pa, pb, pc) = (ports::choose(), ports::choose(), ports::choose());
        let interval = sync_interval_secs;
        let a = Node::spawn_configured("node-a", pa, &[pb], storage, interval, cluster_extra);
        let b = Node::spawn_configured("node-b", pb, &[pa], storage, interval, cluster_extra);
        let c = Node::spawn_configured("node-c", pc, &[pa], storage, interval, cluster_extra);
        let mut collided = false;
        for node in [&a, &b, &c] {
            match node.try_ready(client).await {
                Ok(()) => {}
                Err(exited) if ports::in_use(&exited.stderr) => {
                    eprintln!("{exited}\nstarting the three again on new ports");
                    collided = true;
                    break;
                }
                Err(exited) => panic!("{exited}"),
            }
        }
        if !collided {
            return (a, b, c);
        }
    }
    panic!("{ATTEMPTS} attempts at three nodes each lost a port to another process");
}

/// A cluster port taken by another process between its choosing and its
/// node's bind: the node exits with it in use, and the harness starts the
/// three again on new ports at once rather than waiting out its patience for
/// a process that is gone. The collision is made on purpose, on node A's
/// port; the cluster that forms is on the ports of the next attempt.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_cluster_port_taken_before_its_node_binds_starts_the_three_again() {
    let client = reqwest::Client::new();
    ports::occupy_next(1);
    let started = std::time::Instant::now();
    let (a, b, c) = three_nodes(&client).await;
    let taken = ports::occupied();
    assert_eq!(taken.len(), 1, "the collision was made: {taken:?}");
    assert!(![a.cluster, b.cluster, c.cluster].contains(&taken[0]), "on new ports");
    assert!(
        started.elapsed() < patience() / 2,
        "started again at once, not after the patience budget: {:?}",
        started.elapsed()
    );
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
}

/// Every node reports exactly `expected` on the members gauge.
///
/// Takes the nodes by value so the future owns its list — a borrowed slice
/// of temporaries does not outlive the future a closure returns.
async fn all_report(client: &reqwest::Client, nodes: Vec<&Node>, expected: u64) -> bool {
    for node in nodes {
        if node.members_gauge(client).await != Some(expected) {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Membership
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn gossip_forms_and_survives_a_stall_and_a_death() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;

    // Formation. The gauge counts *peers* — the live set SWIM maintains
    // never contains the node holding it — so a formed three-node cluster
    // reads 2 everywhere. C was never told about B, so this is also the
    // unseeded-member assertion. Asserting the *gauge* rather than
    // replication is the compose-bug regression: discovery alone once
    // carried replication while gossip sat dark.
    eventually("all three nodes to see two live peers", || {
        all_report(&client, vec![&a, &b, &c], 2)
    })
    .await;

    // A stall is a partition as SWIM sees one: the process holds its sockets
    // and answers nothing. Suspicion must remove it from the live set...
    c.signal("STOP");
    eventually("the survivors to declare the stalled node down", || {
        all_report(&client, vec![&a, &b], 1)
    })
    .await;

    // ...and a resumed node must rejoin without a restart: foca renews its
    // incarnation when it learns it was declared dead.
    c.signal("CONT");
    eventually("the resumed node to rejoin without restarting", || {
        all_report(&client, vec![&a, &b, &c], 2)
    })
    .await;

    // A real death, distinct from a stall: the port is gone, not silent.
    c.signal("KILL");
    eventually("the survivors to declare the killed node down", || {
        all_report(&client, vec![&a, &b], 1)
    })
    .await;
}

/// Each member hears the others' blocks on the replication contact: three real
/// nodes each see both peers as eligible, none unknown, none catching up or
/// yielding (ADR-201).
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn every_member_hears_every_other_members_block() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    for node in [&a, &b, &c] {
        eventually("a member to see both peers as eligible", || async {
            node.gauge(&client, "kimmy_ownership_peers{state=\"eligible\"}").await == Some(2)
        })
        .await;
        for other in ["ineligible_catching_up", "ineligible_yielding", "unknown", "stale"] {
            let series = format!("kimmy_ownership_peers{{state=\"{other}\"}}");
            assert_eq!(node.gauge(&client, &series).await, Some(0), "{series}");
        }
        for class in ["ttl", "webhooks", "embeddings"] {
            let series = format!("kimmy_yield_unconfirmed_peers{{class=\"{class}\"}}");
            assert_eq!(node.gauge(&client, &series).await, Some(0), "{series}");
        }
    }
}

// ---------------------------------------------------------------------------
// Replication through gossip
// ---------------------------------------------------------------------------

/// A collection name whose derived id needs the top bit of a `u64`.
///
/// The regression this pins: ids are derived by hashing, BSON has no unsigned
/// 64-bit integer, and a derived `Serialize` once made every oplog entry for
/// such a collection unsendable — silently, for ~48% of names. The fixture is
/// found by search rather than hardcoded, so it keeps the property even if
/// the hash changes.
fn high_hashing_name() -> String {
    for i in 0.. {
        let name = format!("orders_{i}");
        if kimmy_core::ids::CollectionId::derive("shop", &name).0 > i64::MAX as u64 {
            return name;
        }
    }
    unreachable!("half of all names hash high");
}

#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn replication_converges_through_gossip_discovered_peers() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;

    // Written to B, read from C. C's seed list names only A, so C can only
    // have learned about B through gossip — a converged read on C proves the
    // discovered-peer path, not the seeded one. The collection name hashes
    // above i64::MAX, so this is also the encoding regression.
    let coll = high_hashing_name();
    let token = b.login(&client).await;
    client
        .post(b.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": coll }))
        .send()
        .await
        .unwrap();
    client
        .post(b.url(&format!("/v1/db/shop/coll/{coll}/docs")))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "_id": 1, "item": "widget" }))
        .send()
        .await
        .unwrap();

    let c_token = c.login(&client).await;
    eventually("the write on B to arrive at C", || {
        let client = &client;
        let c = &c;
        let coll = &coll;
        let c_token = &c_token;
        async move {
            let Ok(res) = client
                .post(c.url(&format!("/v1/db/shop/coll/{coll}/find")))
                .bearer_auth(c_token)
                .json(&serde_json::json!({ "filter": { "_id": 1 } }))
                .send()
                .await
            else {
                return false;
            };
            let Ok(body) = res.json::<serde_json::Value>().await else {
                return false;
            };
            body["count"].as_u64() == Some(1)
        }
    })
    .await;

    // A converged cluster must read zero replication lag on every node — the
    // gauge measures how far behind a node is where a peer holds newer, and
    // no peer holds anything newer. This is asserted after convergence rather
    // than before because the gauge only updates when a sync round reaches a
    // peer.
    eventually("every node to report zero replication lag", || {
        let client = &client;
        let nodes = [&a, &b, &c];
        async move {
            for node in nodes {
                if node.gauge_f64(client, "kimmy_replication_lag_seconds").await != Some(0.0) {
                    return false;
                }
            }
            true
        }
    })
    .await;
}

/// A schema change made right after its collection reaches every member.
///
/// The regression from the first form of ADR-140's push. The index, pushed
/// alone, raised a member's witnessed vector past the collection created a
/// few milliseconds earlier and not yet pulled, so the member skipped the
/// index as unknown and was never sent the collection again. The two TTL
/// tests below failed on exactly that whenever the collection's expiry owner
/// was not the creating node. ADR-143 makes the push carry the window the
/// member lacks, and this asserts what those tests assumed: a collection and
/// an index created back to back are held by every node, and the response
/// says so.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_schema_change_made_right_after_its_collection_reaches_every_member() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;

    let token = a.login(&client).await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "sessions" }))
        .send()
        .await
        .unwrap();
    let created: serde_json::Value = client
        .post(a.url("/v1/db/shop/coll/sessions/indexes"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "by_seen", "fields": [{ "path": "seen" }] }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // Both peers were handed the collection and the index together, and
    // confirmed them before the response.
    assert_eq!(created["confirmation"]["confirmed"].as_array().map(Vec::len), Some(2), "{created}");
    assert_eq!(created["confirmation"]["pending"].as_array().map(Vec::len), Some(0), "{created}");
    // And counted where the push delivered them: the push hook records before
    // the peer answers, so each peer's count is in place by the time the
    // response is. The push always carries the index. Whether the peer
    // applied it from the push or already held it depends on whether its own
    // sync pull took the collection and the index first: a byte-identical
    // duplicate is held, not applied (ADR-180's addendum). So the push is
    // counted under one series or the other, and the two together are at
    // least one.
    for node in [&b, &c] {
        let applied = node.gauge(&client, "kimmy_sync_ddl_applied_total{via=\"push\"}").await;
        let held = node.gauge(&client, "kimmy_sync_ddl_held_total{via=\"push\"}").await;
        let (applied, held) =
            (applied.expect("the applied series"), held.expect("the held series"));
        assert!(
            applied + held >= 1,
            "{}'s pushed schema changes: applied {applied}, held {held}",
            node.name
        );
    }

    for node in [&b, &c] {
        let token = node.login(&client).await;
        eventually("the collection and its index to be listed on every node", || {
            let client = client.clone();
            let url = node.url("/v1/db/shop/coll/sessions/indexes");
            let token = token.clone();
            async move {
                let Ok(res) = client.get(url).bearer_auth(&token).send().await else {
                    return false;
                };
                if res.status() != 200 {
                    return false;
                }
                let Ok(body) = res.json::<serde_json::Value>().await else {
                    return false;
                };
                body["indexes"]
                    .as_array()
                    .is_some_and(|indexes| indexes.iter().any(|i| i["name"] == "by_seen"))
            }
        })
        .await;
    }
}

/// Wait until no member of `nodes` has pulled anything for two sync
/// intervals, so a counter read next is not raced by a tick still applying:
/// a pulled window reaches the metrics only when its tick ends.
async fn pulls_settle(client: &reqwest::Client, nodes: &[&Node]) {
    let pulls = || async {
        let mut all = Vec::new();
        for node in nodes {
            let body = client.get(node.url("/metrics")).send().await.unwrap().text().await.unwrap();
            let prefix = "kimmy_sync_pull_seconds_count{phase=\"apply\"} ";
            all.push(
                body.lines()
                    .find(|l| l.starts_with(prefix))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse::<u64>().ok())
                    .expect("the pull histogram is on the page"),
            );
        }
        all
    };
    let mut last = pulls().await;
    let mut quiet = 0;
    let deadline = std::time::Instant::now() + patience();
    while quiet < 2 {
        assert!(std::time::Instant::now() < deadline, "pulls to settle");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = pulls().await;
        quiet = if now == last { quiet + 1 } else { 0 };
        last = now;
    }
}

/// A burst of index creates on one member is applied once each on the others
/// (ADR-191, ADR-180). Before, each create pushed its own window, and the
/// windows overlapped: a burst of N cost each peer up to about N²/2 applies,
/// and the confirmations timed out behind them.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_burst_of_creates_on_one_member_is_applied_about_once_each_on_the_others() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;

    let token = a.login(&client).await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "burst" }))
        .send()
        .await
        .unwrap();
    // Every member holds the collection before the burst, so the burst's
    // windows carry the creates and nothing older.
    for node in [&b, &c] {
        let token = node.login(&client).await;
        eventually("the collection to reach every member", || {
            let client = client.clone();
            let url = node.url("/v1/db/shop/collections");
            let token = token.clone();
            async move {
                let Ok(res) = client.get(url).bearer_auth(&token).send().await else {
                    return false;
                };
                res.text().await.is_ok_and(|body| body.contains("\"burst\""))
            }
        })
        .await;
    }
    // Applied, pulled and pushed together: an entry a member already holds
    // is held, not applied, whichever way it came.
    let pushed = |node: &Node| {
        let client = client.clone();
        let node_url = node.url("/metrics");
        async move {
            let body = client.get(node_url).send().await.unwrap().text().await.unwrap();
            ["pull", "push"]
                .iter()
                .map(|via| {
                    let prefix = format!("kimmy_sync_ddl_applied_total{{via=\"{via}\"}} ");
                    body.lines()
                        .find(|l| l.starts_with(&prefix))
                        .and_then(|l| l.split_whitespace().nth(1))
                        .and_then(|v| v.parse::<u64>().ok())
                        .expect("the series is on the page")
                })
                .sum::<u64>()
        }
    };
    // The collection's own creation reaches the pulled count only when the
    // tick that pulled it ends; read before that, it lands in the burst.
    pulls_settle(&client, &[&b, &c]).await;
    let before = (pushed(&b).await, pushed(&c).await);

    const N: usize = 16;
    let mut creates = tokio::task::JoinSet::new();
    for i in 0..N {
        let client = client.clone();
        let url = a.url("/v1/db/shop/coll/burst/indexes");
        let token = token.clone();
        creates.spawn(async move {
            client
                .post(url)
                .bearer_auth(&token)
                .json(&serde_json::json!({ "name": format!("f{i}"), "fields": [{ "path": format!("f{i}") }] }))
                .send()
                .await
                .unwrap()
                .json::<serde_json::Value>()
                .await
                .unwrap()
        });
    }
    while let Some(created) = creates.join_next().await {
        let created = created.unwrap();
        assert_eq!(
            created["confirmation"]["pending"].as_array().map(Vec::len),
            Some(0),
            "{created}"
        );
    }

    // The creates once each, however they arrived. A window that a
    // member's own pull took first and a push then carried again, the one
    // overlap a single push in flight leaves (ADR-191), is held rather than
    // applied, and so is a relay of it from the third member; two applies
    // racing on one entry commit and count it once (ADR-180's addendum).
    // Against up to about N²/2, 128 here, before ADR-191.
    let after = (pushed(&b).await, pushed(&c).await);
    for (name, rise) in [("b", after.0 - before.0), ("c", after.1 - before.1)] {
        assert!(rise <= N as u64, "{name} applied {rise} schema changes for {N}");
    }
    let timeouts = a.gauge(&client, "kimmy_ddl_confirmations_total{outcome=\"timeout\"}").await;
    assert_eq!(timeouts, Some(0), "no confirmation timed out");
}

/// A burst of index creates on one member costs each other member one commit
/// per create, plus one per window it applied: the build carries the
/// originating entry, and an entry a window carries again that the member
/// already holds commits nothing (ADR-180, ADR-119). Before, every create
/// cost a peer a second commit to append its entry, and every duplicate a
/// third — measured on a three-member cluster at 94–111 commits per peer for
/// a burst of 32, nearly all of them an fsync.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_burst_of_creates_on_one_member_costs_the_others_one_commit_each() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;

    let token = a.login(&client).await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "burst" }))
        .send()
        .await
        .unwrap();
    let tokens = (b.login(&client).await, c.login(&client).await);
    let indexes = |node: &Node, token: &str| {
        let client = client.clone();
        let url = node.url("/v1/db/shop/coll/burst/indexes");
        let token = token.to_string();
        async move {
            let Ok(res) = client.get(url).bearer_auth(&token).send().await else {
                return None;
            };
            let body = res.json::<serde_json::Value>().await.ok()?;
            let listed = body["indexes"].as_array()?;
            Some(
                listed
                    .iter()
                    .filter(|i| i["name"].as_str().is_some_and(|n| n.starts_with('f')))
                    .count(),
            )
        }
    };
    for (node, token) in [(&b, &tokens.0), (&c, &tokens.1)] {
        eventually("the collection to reach every member", || {
            let indexes = indexes(node, token);
            async move { indexes.await.is_some() }
        })
        .await;
    }

    /// Everything the bound is made of, on one member, at one moment.
    #[derive(Clone, Copy, Debug, Default)]
    struct Reading {
        commits: u64,
        embedding: u64,
        pulls: u64,
        applied: u64,
        held: u64,
        pushes: u64,
    }
    let read = |node: &Node| {
        let client = client.clone();
        let url = node.url("/metrics");
        async move {
            let body = client.get(url).send().await.unwrap().text().await.unwrap();
            // A series missing from the page is a renamed metric, and would
            // read as 0 and pass the bound; so it fails instead.
            let value = |name: &str| {
                let prefix = format!("{name} ");
                body.lines()
                    .find(|l| l.starts_with(&prefix))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or_else(|| panic!("{name} is not on the metrics page"))
            };
            Reading {
                commits: value("kimmy_commits"),
                embedding: value("kimmy_write_lock_held_seconds_count{holder=\"embedding\"}"),
                pulls: value("kimmy_sync_pull_seconds_count{phase=\"apply\"}"),
                applied: value("kimmy_sync_ddl_applied_total{via=\"push\"}")
                    + value("kimmy_sync_ddl_applied_total{via=\"pull\"}"),
                held: value("kimmy_sync_ddl_held_total{via=\"push\"}")
                    + value("kimmy_sync_ddl_held_total{via=\"pull\"}"),
                pushes: value("kimmy_ddl_confirm_pushes_total"),
            }
        }
    };
    pulls_settle(&client, &[&b, &c]).await;
    let started = std::time::Instant::now();
    let before = (read(&a).await, read(&b).await, read(&c).await);

    const N: usize = 16;
    let mut creates = tokio::task::JoinSet::new();
    for i in 0..N {
        let client = client.clone();
        let url = a.url("/v1/db/shop/coll/burst/indexes");
        let token = token.clone();
        creates.spawn(async move {
            client
                .post(url)
                .bearer_auth(&token)
                .json(&serde_json::json!({ "name": format!("f{i}"), "fields": [{ "path": format!("f{i}") }] }))
                .send()
                .await
                .unwrap()
                .status()
        });
    }
    while let Some(status) = creates.join_next().await {
        assert!(status.unwrap().is_success());
    }
    for (node, token) in [(&b, &tokens.0), (&c, &tokens.1)] {
        eventually("every index to reach every member", || {
            let indexes = indexes(node, token);
            async move { indexes.await.is_some_and(|n| n >= N) }
        })
        .await;
    }
    pulls_settle(&client, &[&b, &c]).await;
    let window = started.elapsed();
    let after = (read(&a).await, read(&b).await, read(&c).await);

    // Pushes to both members: the counter names no member, so each is
    // bounded by all of them.
    let pushes = after.0.pushes - before.0.pushes;
    let mut report = Vec::new();
    let mut over = Vec::new();
    for (name, before, after) in [("b", before.1, after.1), ("c", before.2, after.2)] {
        let commits = after.commits - before.commits;
        let embedding = after.embedding - before.embedding;
        let pulls = after.pulls - before.pulls;
        let applied = after.applied - before.applied;
        let held = after.held - before.held;
        // One build per create, which carries its entry; one witness commit
        // per window applied, pushed or pulled; the vector worker's position
        // writes, subtracted; and 2 for whatever else a quiet member commits.
        let bound = N as u64 + pushes + pulls + 2;
        let spent = commits - embedding;
        report.push(format!(
            "{name}: commits {commits} (embedding {embedding}) -> {spent} against bound {bound} \
             = N {N} + pushes {pushes} + pulls {pulls} + 2; applied {applied}, held {held}; \
             window {:.1}s",
            window.as_secs_f64()
        ));
        if spent > bound {
            over.push(name);
        }
        assert!(applied <= N as u64, "{name} applied {applied} schema changes for {N}");
    }
    let report = report.join("\n");
    println!("{report}");
    assert!(over.is_empty(), "commits over the bound on {over:?}:\n{report}");
}

// ---------------------------------------------------------------------------
// Webhook ownership across the cluster
// ---------------------------------------------------------------------------

/// A receiver on a real socket, collecting request bodies.
async fn receiver() -> (std::net::SocketAddr, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let record = std::sync::Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            let record = std::sync::Arc::clone(&record);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                // Read until the headers' declared body length is satisfied.
                loop {
                    let Ok(n) = stream.read(&mut chunk).await else { return };
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf);
                    if let Some(head_end) = text.find("\r\n\r\n") {
                        let content_length = text
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        if buf.len() >= head_end + 4 + content_length {
                            break;
                        }
                    }
                }
                let text = String::from_utf8_lossy(&buf);
                if let Some(head_end) = text.find("\r\n\r\n") {
                    record.lock().unwrap().push(text[head_end + 4..].to_string());
                }
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            });
        }
    });
    (addr, seen)
}

#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn every_subscription_has_a_deliverer_and_a_dead_owners_are_taken_over() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;

    let (hook_addr, seen) = receiver().await;
    let token = a.login(&client).await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();

    // Register subscriptions until one is owned by each node. Ownership is
    // the same pure function the dispatchers compute — rendezvous over the
    // cluster's node ids — so the harness knows the intended owner without
    // asking anyone. This is the test for the standing hypothesis that a
    // node's member set might not include itself: if it does not, the
    // subscriptions owned by some node are owned by nobody in that node's
    // own view, and their events are never delivered by anyone.
    let members: std::collections::BTreeSet<kimmy_core::NodeId> =
        [a.node_id(&client).await, b.node_id(&client).await, c.node_id(&client).await].into();
    let mut owned_by: std::collections::HashMap<kimmy_core::NodeId, String> = Default::default();
    while owned_by.len() < 3 {
        let res: serde_json::Value = client
            .post(a.url("/v1/db/shop/coll/orders/webhooks"))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "url": format!("http://{hook_addr}/hook") }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let id = res["id"].as_str().expect("an id").to_string();
        let owner = kimmy_api::ownership::owner(&id, &members).expect("three live members");
        owned_by.entry(owner).or_insert(id);
    }

    // Give the registry a moment to replicate to every node, then write once.
    // All three subscriptions listen to the same collection, so one insert
    // must arrive three times — once per subscription, whoever owns it.
    tokio::time::sleep(Duration::from_secs(3)).await;
    client
        .post(a.url("/v1/db/shop/coll/orders/docs"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "_id": 1, "item": "widget" }))
        .send()
        .await
        .unwrap();

    let all_ids: Vec<String> = owned_by.values().cloned().collect();
    eventually("every subscription to receive the first write", || {
        let seen = std::sync::Arc::clone(&seen);
        let all_ids = all_ids.clone();
        async move {
            let bodies = seen.lock().unwrap();
            all_ids.iter().all(|id| bodies.iter().any(|body| body.contains(id.as_str())))
        }
    })
    .await;

    // Kill one owner and write again: its subscription must be delivered by
    // a survivor, resuming from replicated progress. The subscription owned
    // by C is the one whose deliverer just died.
    let doomed_id = owned_by.get(&c.node_id(&client).await).expect("C owns one").clone();
    c.signal("KILL");
    eventually("the survivors to notice the death", || all_report(&client, vec![&a, &b], 1)).await;

    client
        .post(a.url("/v1/db/shop/coll/orders/docs"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "_id": 2, "item": "gadget" }))
        .send()
        .await
        .unwrap();

    eventually("a survivor to deliver the dead owner's subscription", || {
        let seen = std::sync::Arc::clone(&seen);
        let doomed_id = doomed_id.clone();
        async move {
            let bodies = seen.lock().unwrap();
            bodies
                .iter()
                .any(|body| body.contains(doomed_id.as_str()) && body.contains("\"_id\":2"))
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// TTL expiry
// ---------------------------------------------------------------------------

/// The claim the ownership decision was made for: **one expired document
/// produces one delete cluster-wide, not one per node**, once the members'
/// blocks have settled (ADR-201 states the windows in which it does not).
///
/// Every node runs its own expiry timer, so the naive design has all three
/// notice the same document and issue three deletes. Those converge under
/// last-writer-wins, so correctness alone cannot tell the two designs apart —
/// only counting can, which is why `kimmy_ttl_expired_total` exists. Summed
/// across the cluster it must read exactly 1.
///
/// **The document must reach every member before it falls due**, or this
/// cannot see amplification at all: a document already expired when it lands
/// is deleted by the member it landed on before it replicates, and the others
/// receive the delete with the insert and have nothing to expire. So it is
/// dated a few seconds ahead, and read on all three before it is due.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn one_expired_document_produces_one_delete_cluster_wide() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;

    let token = a.login(&client).await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "sessions" }))
        .send()
        .await
        .unwrap();

    // A TTL index is DDL, so it replicates like any other index definition —
    // every node ends up holding the policy, which is exactly why ownership
    // has to decide who acts on it.
    let created = client
        .post(a.url("/v1/db/shop/coll/sessions/indexes"))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "name": "ttl_seen",
            "fields": [{ "path": "seen" }],
            "expireAfterSeconds": 1,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200, "creating the TTL index");

    // Until the members have told each other they hold the index, each counts
    // only itself a holder and so owns the collection: a window of duplicates,
    // which the design accepts over a gap (ADR-201) and which the block's next
    // contact closes. The counting below is of the settled state, so wait for
    // exactly one member to own it.
    eventually("exactly one member to own the collection", || {
        let client = client.clone();
        let nodes = [&a, &b, &c];
        async move {
            let mut owned = 0;
            for node in nodes {
                match node.gauge(&client, "kimmy_ttl_collections{state=\"owned\"}").await {
                    Some(n) => owned += n,
                    None => return false,
                }
            }
            owned == 1
        }
    })
    .await;

    // Tokens for all three first: the window below is only a few seconds, and
    // a login is not what it should be spent on.
    let mut tokens = Vec::new();
    for node in [&a, &b, &c] {
        tokens.push(node.login(&client).await);
    }

    // Due about four seconds from now (`seen` plus the index's one second), so
    // it can replicate to every member first and each has the chance to
    // expire it.
    let now_ms =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
            as i64;
    let due = std::time::Instant::now() + std::time::Duration::from_secs(4);
    let inserted = client
        .post(a.url("/v1/db/shop/coll/sessions/docs"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "_id": 1, "seen": { "$date": now_ms + 3_000 } }))
        .send()
        .await
        .unwrap();
    assert!(inserted.status().is_success(), "inserting the document: {}", inserted.status());

    // Readable on every member while it is still live.
    for (node, token) in [&a, &b, &c].into_iter().zip(&tokens) {
        loop {
            let res = client
                .get(node.url("/v1/db/shop/coll/sessions/docs/1"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap();
            if res.status() == 200 {
                break;
            }
            assert!(
                std::time::Instant::now() < due,
                "the document did not reach every member before it fell due, so this run \
                 cannot tell one delete from one per member"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    assert!(
        std::time::Instant::now() < due,
        "the document reached every member only after it fell due; this run proves nothing"
    );

    // Gone everywhere: the owner deletes, and the delete replicates.
    for (node, token) in [&a, &b, &c].into_iter().zip(&tokens) {
        eventually("the expired document to disappear from every node", || {
            let client = client.clone();
            let url = node.url("/v1/db/shop/coll/sessions/docs/1");
            let token = token.clone();
            async move {
                let res = client.get(url).bearer_auth(&token).send().await.unwrap();
                res.status() == 404
            }
        })
        .await;
    }

    // The measurement. Let a few more passes run first, so that a design where
    // every node expires independently has every chance to show itself.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let mut total = 0;
    for node in [&a, &b, &c] {
        total += node.gauge(&client, "kimmy_ttl_expired_total").await.unwrap_or(0);
    }
    assert_eq!(
        total, 1,
        "one document must produce exactly one delete cluster-wide; \
         {total} means expiry is amplifying across nodes"
    );
}

/// A document refreshed before the pass reaches it must survive.
///
/// The heartbeat case, on real nodes rather than in a unit test: the scan and
/// the delete are separate transactions, so without the guard re-reading
/// inside the write, a session extended in that window is deleted while live.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_refreshed_document_outlives_its_expiry() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;

    let token = a.login(&client).await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "sessions" }))
        .send()
        .await
        .unwrap();
    client
        .post(a.url("/v1/db/shop/coll/sessions/indexes"))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "name": "ttl_seen",
            "fields": [{ "path": "seen" }],
            "expireAfterSeconds": 3600,
        }))
        .send()
        .await
        .unwrap();

    // One already due, one comfortably fresh.
    for (id, seen) in [(1, 0i64), (2, i64::from(u32::MAX) * 1000)] {
        client
            .post(a.url("/v1/db/shop/coll/sessions/docs"))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "_id": id, "seen": { "$date": seen } }))
            .send()
            .await
            .unwrap();
    }

    eventually("the stale session to expire", || {
        let client = client.clone();
        let url = a.url("/v1/db/shop/coll/sessions/docs/1");
        let token = token.clone();
        async move { client.get(url).bearer_auth(&token).send().await.unwrap().status() == 404 }
    })
    .await;

    // The fresh one is still here, and stays here across several passes.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let res = client
        .get(a.url("/v1/db/shop/coll/sessions/docs/2"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "a document inside its TTL must not be expired");
}

// ---------------------------------------------------------------------------
// Client-visible topology
// ---------------------------------------------------------------------------

/// Every node can tell a client about every node, with its real address.
///
/// The reason this belongs in the harness rather than in an in-process test:
/// the two halves of the answer come from two different mechanisms that only
/// both exist on real nodes. Addresses arrive by **replication** of the node
/// registry, and liveness by **SWIM**, and a single process has neither. An
/// in-process test can prove the assembly is right and cannot prove the
/// assembled thing is true — which is the distinction M8 task 1 was built on,
/// where transport-free tests passed while clustered delivery was entirely
/// broken.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn every_node_can_tell_a_client_about_every_node() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    let token = a.login(&client).await;

    let ids = [
        a.node_id(&client).await.to_string(),
        b.node_id(&client).await.to_string(),
        c.node_id(&client).await.to_string(),
    ];
    let endpoints = [
        format!("http://127.0.0.1:{}", a.http),
        format!("http://127.0.0.1:{}", b.http),
        format!("http://127.0.0.1:{}", c.http),
    ];

    // The registry replicates like any other collection, so a node knows about
    // peers it was never told about — C's seed list never names B.
    eventually("every node to list all three", || {
        let client = &client;
        let nodes = [&a, &b, &c];
        async move {
            for node in nodes {
                let token = node.login(client).await;
                let Ok(res) = client.get(node.url("/v1/topology")).bearer_auth(&token).send().await
                else {
                    return false;
                };
                let body: serde_json::Value = res.json().await.unwrap();
                if body["count"].as_u64() != Some(3) {
                    return false;
                }
                // Every entry must be `live`: all three are up, so anything
                // `unknown` means membership and the registry disagree.
                let all_live =
                    body["nodes"].as_array().unwrap().iter().all(|n| n["status"] == "live");
                if !all_live {
                    return false;
                }
            }
            true
        }
    })
    .await;

    let body: serde_json::Value = client
        .get(a.url("/v1/topology"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let listed = body["nodes"].as_array().unwrap();

    // The answering node is first and marked `self` — `Members` holds peers
    // only, so a list derived from it alone would omit the node a client is
    // already talking to (ADR-051).
    assert_eq!(listed[0]["self"], true, "the answering node comes first: {body}");
    assert_eq!(listed[0]["node"], ids[0]);
    assert_eq!(listed.iter().filter(|n| n["self"] == true).count(), 1);

    // Real addresses, from the registry rather than inferred from a gossip
    // port. Each is the node's actual HTTP listener.
    for (id, endpoint) in ids.iter().zip(endpoints.iter()) {
        let entry = listed.iter().find(|n| n["node"] == *id).expect("every node is listed");
        assert_eq!(entry["endpoint"], *endpoint, "{id} advertises where it really is");
    }

    // And the addresses work: a client handed this list can use any of them.
    for endpoint in &endpoints {
        let res = client
            .get(format!("{endpoint}/v1/auth/whoami"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            200,
            "a token from one node is accepted by another at {endpoint} — one cluster, one \
             signing secret"
        );
    }

    // A node that dies is still listed, with its status downgraded rather than
    // disappearing: a client keeps the address and learns not to prefer it.
    drop(c);
    eventually("the dead node to be reported unknown", || {
        let client = &client;
        let a = &a;
        let token = token.clone();
        let dead = ids[2].clone();
        async move {
            let body: serde_json::Value = client
                .get(a.url("/v1/topology"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            body["count"].as_u64() == Some(3)
                && body["nodes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|n| n["node"] == dead && n["status"] == "unknown")
        }
    })
    .await;
}

/// A page from one node continues correctly on another.
///
/// The property cursors were designed for and never verified: a token is the
/// encoded `_id` of a page's last row, which is a pure function of the
/// document, so any node holding that document computes the same bound. Until
/// now that was an argument. Change-stream resume tokens have been checked on a
/// real cluster since M4; cursors inherited the claim by construction and
/// nothing exercised it.
///
/// It matters because a client handed `/v1/topology` is *expected* to spread
/// requests across nodes. Paging that silently repeated or skipped documents
/// when a client moved would be a data bug reached by taking the protocol's own
/// advice.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_page_from_one_node_continues_on_another() {
    const TOTAL: i64 = 60;
    const PAGE: usize = 10;

    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    let token = a.login(&client).await;

    // Seeded through one node, in one commit.
    let batch: Vec<serde_json::Value> =
        (0..TOTAL).map(|i| serde_json::json!({ "_id": i, "n": i })).collect();
    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    let inserted = client
        .post(a.url("/v1/db/shop/coll/orders/bulk"))
        .bearer_auth(&token)
        .json(&batch)
        .send()
        .await
        .unwrap();
    assert_eq!(inserted.status(), 200, "seeding: {:?}", inserted.text().await);

    // Every node holds every document before paging starts, so a missing one
    // later is a paging fault rather than replication lag.
    let nodes = [&a, &b, &c];
    eventually("all three nodes to hold the whole collection", || {
        let client = &client;
        let token = token.clone();
        async move {
            for node in nodes {
                let Ok(res) = client
                    .post(node.url("/v1/db/shop/coll/orders/count"))
                    .bearer_auth(&token)
                    .json(&serde_json::json!({ "filter": {} }))
                    .send()
                    .await
                else {
                    return false;
                };
                let body: serde_json::Value = res.json().await.unwrap();
                if body["count"].as_i64() != Some(TOTAL) {
                    return false;
                }
            }
            true
        }
    })
    .await;

    // Walk the collection, changing node on every page. Deliberately harsher
    // than a real client, which is sticky and only moves when a node stops
    // answering: if a cursor survives changing node on *every* page it survives
    // the failover `/v1/topology` exists to enable.
    let mut seen: Vec<i64> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut visited: Vec<&'static str> = Vec::new();

    for page in 0..20 {
        let node = nodes[page % nodes.len()];
        visited.push(node.name);

        let mut body = serde_json::json!({ "filter": {}, "limit": PAGE });
        if let Some(c) = &cursor {
            body["cursor"] = serde_json::json!(c);
        }
        let res: serde_json::Value = client
            .post(node.url("/v1/db/shop/coll/orders/find"))
            .bearer_auth(&node.login(&client).await)
            .json(&body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        for doc in res["documents"].as_array().expect("documents") {
            seen.push(doc["_id"].as_i64().expect("an integer id"));
        }
        match res["nextCursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => break,
        }
    }

    assert!(visited.len() >= 3, "the walk has to cross nodes to prove anything: {visited:?}");
    assert_eq!(
        seen.len(),
        TOTAL as usize,
        "a walk across nodes saw {} of {TOTAL} documents (order: {visited:?})",
        seen.len()
    );
    let mut unique = seen.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), seen.len(), "a document was returned twice across nodes");
    assert_eq!(unique, (0..TOTAL).collect::<Vec<_>>(), "the walk missed documents");
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "pages arrived out of _id order: {seen:?}");
}

/// A drop on one node ends the streams watching that collection on every node.
///
/// The half an in-process test cannot reach. A drop reaches other nodes as a
/// replicated oplog entry rather than as a local call, so "the node that
/// dropped it invalidates its own streams" says nothing about the node a
/// client happens to be connected to — and with `/v1/topology` telling clients
/// to spread out, that is frequently a different one.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_replicated_drop_ends_a_stream_on_another_node() {
    use futures::{SinkExt, StreamExt};

    let client = reqwest::Client::new();
    let (a, b, _c) = three_nodes(&client).await;
    let token = a.login(&client).await;

    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "watched" }))
        .send()
        .await
        .unwrap();

    // Node B must know the collection before it can be watched there.
    eventually("node B to learn the collection", || {
        let client = &client;
        let b = &b;
        let token = token.clone();
        async move {
            client
                .get(b.url("/v1/db/shop/coll/watched/indexes"))
                .bearer_auth(&token)
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false)
        }
    })
    .await;

    // Watch on B, using B's own token — one cluster, one signing secret, but
    // asking B for it keeps the test honest about what it is exercising.
    let b_token = b.login(&client).await;
    let (mut socket, _) = tokio_tungstenite::connect_async(
        tokio_tungstenite::tungstenite::http::Request::builder()
            .uri(format!("ws://127.0.0.1:{}/v1/db/shop/coll/watched/watch", b.http))
            .header("Host", format!("127.0.0.1:{}", b.http))
            .header("Authorization", format!("Bearer {b_token}"))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            )
            .body(())
            .unwrap(),
    )
    .await
    .expect("opening a change stream on node B");

    // Dropped on A.
    let dropped =
        client.delete(a.url("/v1/db/shop/coll/watched")).bearer_auth(&token).send().await.unwrap();
    assert_eq!(dropped.status(), 200);

    // B's stream must end, and say why.
    let event = tokio::time::timeout(patience(), async {
        while let Some(Ok(message)) = socket.next().await {
            if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                if value["operationType"] == "invalidate" {
                    return value;
                }
            }
        }
        panic!("the socket closed without an invalidate");
    })
    .await
    .expect("a replicated drop must invalidate a stream on another node");

    assert_eq!(event["reason"], "CollectionDropped", "{event}");
    let _ = socket.send(tokio_tungstenite::tungstenite::Message::Close(None)).await;
}

// ---------------------------------------------------------------------------
// A database dropped by a member that was behind stays dropped (ADR-229)
// ---------------------------------------------------------------------------

/// Whether `node` lists `db`.
async fn lists_database(
    client: &reqwest::Client,
    node: &Node,
    token: &str,
    db: &str,
) -> Option<bool> {
    let response = client.get(node.url("/v1/databases")).bearer_auth(token).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body: serde_json::Value = response.json().await.ok()?;
    let names = body["databases"].as_array()?;
    Some(names.iter().any(|d| d["name"] == db || d == db))
}

/// Whether `node` has the collection, by a read of its indexes.
async fn has_collection(
    client: &reqwest::Client,
    node: &Node,
    token: &str,
    db: &str,
    name: &str,
) -> bool {
    client
        .get(node.url(&format!("/v1/db/{db}/coll/{name}/indexes")))
        .bearer_auth(token)
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_database_dropped_by_a_member_that_was_behind_stays_dropped() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    let (ta, tb, tc) = (a.login(&client).await, b.login(&client).await, c.login(&client).await);

    let made = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&ta)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert!(made.status().is_success());
    eventually("every member to hold shop.orders", || async {
        has_collection(&client, &b, &tb, "shop", "orders").await
            && has_collection(&client, &c, &tc, "shop", "orders").await
    })
    .await;

    // `a` is held back while `b` creates a collection `c` then learns.
    a.signal("STOP");
    let made = client
        .post(b.url("/v1/db/shop/collections"))
        .bearer_auth(&tb)
        .json(&serde_json::json!({ "name": "late" }))
        .send()
        .await
        .unwrap();
    assert!(made.status().is_success());
    client
        .post(b.url("/v1/db/shop/coll/late/docs"))
        .bearer_auth(&tb)
        .json(&serde_json::json!({ "_id": "x" }))
        .send()
        .await
        .unwrap();
    eventually("c to learn shop.late", || async {
        has_collection(&client, &c, &tc, "shop", "late").await
    })
    .await;

    // `b` and `c` are held while `a` resumes and drops the database: `a` cannot
    // have received `late`, so it is the member that was behind.
    b.signal("STOP");
    c.signal("STOP");
    a.signal("CONT");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !has_collection(&client, &a, &ta, "shop", "late").await,
        "premise: a never heard of late"
    );
    let dropped = client.delete(a.url("/v1/db/shop")).bearer_auth(&ta).send().await.unwrap();
    assert_eq!(dropped.status(), 200);
    b.signal("CONT");
    c.signal("CONT");

    eventually("every member to list shop gone", || async {
        let mut gone = true;
        for (node, token) in [(&a, &ta), (&b, &tb), (&c, &tc)] {
            gone &= lists_database(&client, node, token, "shop").await == Some(false);
        }
        gone
    })
    .await;
    // Not a moment's agreement: it stays gone through further rounds.
    tokio::time::sleep(Duration::from_secs(5)).await;
    for (node, token) in [(&a, &ta), (&b, &tb), (&c, &tc)] {
        assert_eq!(lists_database(&client, node, token, "shop").await, Some(false));
        assert!(!has_collection(&client, node, token, "shop", "late").await);
    }

    // And it can be made again, on the member that dropped it, and replicates.
    let made = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&ta)
        .json(&serde_json::json!({ "name": "fresh" }))
        .send()
        .await
        .unwrap();
    assert!(made.status().is_success(), "{}", made.text().await.unwrap());
    eventually("the recreated database to reach the others", || async {
        has_collection(&client, &b, &tb, "shop", "fresh").await
            && has_collection(&client, &c, &tc, "shop", "fresh").await
    })
    .await;
}

// ---------------------------------------------------------------------------
// A document an index cannot key is filed the same way on every member
// ---------------------------------------------------------------------------

/// The `unkeyed` count an index reports on one node, if the node answers.
async fn unkeyed_on(
    client: &reqwest::Client,
    node: &Node,
    token: &str,
    index: &str,
) -> Option<u64> {
    let res = client
        .get(node.url("/v1/db/shop/coll/ledger/indexes"))
        .bearer_auth(token)
        .send()
        .await
        .ok()?;
    let body = res.json::<serde_json::Value>().await.ok()?;
    body["indexes"].as_array()?.iter().find(|i| i["name"] == index)?["unkeyed"].as_u64()
}

/// How many documents an indexed query counts on one node, if it answers.
async fn indexed_count_on(client: &reqwest::Client, node: &Node, token: &str) -> Option<u64> {
    let res = client
        .post(node.url("/v1/db/shop/coll/ledger/count"))
        .bearer_auth(token)
        .json(&serde_json::json!({ "filter": { "a": 1 }, "explain": true }))
        .send()
        .await
        .ok()?;
    let body = res.json::<serde_json::Value>().await.ok()?;
    (body["explain"]["strategy"] == "index").then(|| body["count"].as_u64()).flatten()
}

#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn every_member_files_an_unkeyable_document_the_same_way() {
    // The finding on a live cluster: a Decimal128 document written to one
    // member was filed under a real key there and unkeyed on both peers,
    // because the member's JSON edge left `$numberDecimal` a nested
    // document while every re-decode of the stored bytes held a Decimal128.
    // A `Decimal128` has a key now, and is written beside the shapes an index
    // still cannot key to keep the finding pinned: every node must report the
    // same `unkeyed` (the two shapes that cannot be keyed, not the decimal) and
    // count all three documents through the index — and again for the mirror
    // order, an index created after the documents have replicated.
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;

    let token = a.login(&client).await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "ledger" }))
        .send()
        .await
        .unwrap();
    let created = client
        .post(a.url("/v1/db/shop/coll/ledger/indexes"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "ab", "fields": [{ "path": "a" }, { "path": "b" }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);

    let shapes = [
        serde_json::json!({ "_id": 1, "a": [1, 2], "b": [3, 4] }),
        serde_json::json!({ "_id": 2, "a": (0..1_000).collect::<Vec<i32>>(), "b": 5 }),
        serde_json::json!({ "_id": 3, "a": 1, "b": { "$numberDecimal": "1.5" } }),
    ];
    for shape in &shapes {
        let res = client
            .post(a.url("/v1/db/shop/coll/ledger/docs"))
            .bearer_auth(&token)
            .json(shape)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200, "stored, not refused: {}", res.text().await.unwrap());
    }

    let tokens = [a.login(&client).await, b.login(&client).await, c.login(&client).await];
    let nodes = [&a, &b, &c];
    eventually(
        "every node to file the two unkeyable documents unkeyed and count all three",
        || {
            let client = &client;
            let tokens = &tokens;
            async move {
                for (node, token) in nodes.iter().zip(tokens) {
                    if unkeyed_on(client, node, token, "ab").await != Some(2) {
                        return false;
                    }
                    if indexed_count_on(client, node, token).await != Some(3) {
                        return false;
                    }
                }
                true
            }
        },
    )
    .await;

    // The mirror order: the documents are everywhere already, and a new
    // index over them is backfilled on every node from the bytes it holds.
    let created = client
        .post(a.url("/v1/db/shop/coll/ledger/indexes"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "ba", "fields": [{ "path": "b" }, { "path": "a" }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    eventually("every node to backfill the second index the same way", || {
        let client = &client;
        let tokens = &tokens;
        async move {
            for (node, token) in nodes.iter().zip(tokens) {
                if unkeyed_on(client, node, token, "ba").await != Some(2) {
                    return false;
                }
            }
            true
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// A change stream resumed on another member (ADR-173)
// ---------------------------------------------------------------------------

type Feed =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A change stream on `shop.feed` at `node`, resumed after `resume` when given.
async fn open_feed(node: &Node, bearer: &str, resume: Option<&str>) -> Feed {
    let query = resume.map(|t| format!("?resume_after={t}")).unwrap_or_default();
    let (socket, _) = tokio_tungstenite::connect_async(
        tokio_tungstenite::tungstenite::http::Request::builder()
            .uri(format!("ws://127.0.0.1:{}/v1/db/shop/coll/feed/watch{query}", node.http))
            .header("Host", format!("127.0.0.1:{}", node.http))
            .header("Authorization", format!("Bearer {bearer}"))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tokio_tungstenite::tungstenite::handshake::client::generate_key(),
            )
            .body(())
            .unwrap(),
    )
    .await
    .unwrap_or_else(|e| panic!("opening a change stream on {}: {e}", node.name));
    socket
}

/// The next document event on `feed`: its `_id` and its resume token.
async fn next_feed_event(feed: &mut Feed) -> (String, String) {
    use futures::StreamExt;
    loop {
        let message = feed.next().await.expect("the stream closed").expect("a readable frame");
        let tokio_tungstenite::tungstenite::Message::Text(text) = message else { continue };
        let event: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_ne!(event["operationType"], "invalidate", "the stream ended: {event}");
        if let (Some(id), Some(token)) =
            (event["documentKey"]["_id"].as_str(), event["resumeToken"].as_str())
        {
            return (id.to_string(), token.to_string());
        }
    }
}

/// Insert `ids` into `shop.feed` through `url`, one request each.
async fn write_feed(client: reqwest::Client, url: String, bearer: String, ids: Vec<String>) {
    for id in ids {
        let res = client
            .post(&url)
            .bearer_auth(&bearer)
            .json(&serde_json::json!([{ "_id": id }]))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200, "writing {id}: {:?}", res.text().await);
    }
}

fn feed_ids(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}-{i:03}")).collect()
}

/// A client's stream is cut mid-flow on one member and resumed on another.
///
/// The token names a position in the issuing member's arrival order, and the
/// member it resumes on ordered the same writes differently. The shape makes
/// that difference certain rather than a race on replication latency: while
/// the client reads from A, B and C are paused, so the token's event exists on
/// A alone; then A and C are paused while B takes writes of its own, so on B
/// those writes arrive *before* the token's event. A resume that translated
/// the token to its entry's position on B skipped all of them.
///
/// On B every id the client was not sent must be delivered, and repeats are
/// allowed. On A, the issuing member, the resume is exact.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_change_stream_resumed_on_another_node_misses_nothing() {
    use std::collections::BTreeSet;

    const FIRST: usize = 60;
    const CUT_AFTER: usize = 25;
    const SECOND: usize = 40;

    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    let bearer_a = a.login(&client).await;
    let bearer_b = b.login(&client).await;

    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&bearer_a)
        .json(&serde_json::json!({ "name": "feed" }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    for node in [&b, &c] {
        eventually("every member to learn the collection", || {
            let client = &client;
            let bearer = bearer_a.clone();
            async move {
                client
                    .get(node.url("/v1/db/shop/coll/feed/indexes"))
                    .bearer_auth(&bearer)
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false)
            }
        })
        .await;
    }

    // Phase 1: B and C paused. A writer runs on A, and the client's stream on
    // A is cut after CUT_AFTER events while the writer is still going.
    b.signal("STOP");
    c.signal("STOP");
    let mut first = open_feed(&a, &bearer_a, None).await;
    let writer = tokio::spawn(write_feed(
        client.clone(),
        a.url("/v1/db/shop/coll/feed/bulk"),
        bearer_a.clone(),
        feed_ids("a", FIRST),
    ));
    let mut seen = BTreeSet::new();
    let mut token = String::new();
    while seen.len() < CUT_AFTER {
        let (id, t) = next_feed_event(&mut first).await;
        seen.insert(id);
        token = t;
    }
    drop(first);
    writer.await.unwrap();

    // Phase 2: A and C paused, B running. B's writes arrive on B before the
    // token's event can reach it.
    a.signal("STOP");
    b.signal("CONT");
    b.wait_ready(&client).await;
    write_feed(
        client.clone(),
        b.url("/v1/db/shop/coll/feed/bulk"),
        bearer_b.clone(),
        feed_ids("b", FIRST),
    )
    .await;

    // Phase 3: everyone running, with writers on A and B at once.
    a.signal("CONT");
    c.signal("CONT");
    a.wait_ready(&client).await;
    c.wait_ready(&client).await;
    let concurrent_a = tokio::spawn(write_feed(
        client.clone(),
        a.url("/v1/db/shop/coll/feed/bulk"),
        bearer_a.clone(),
        feed_ids("a2", SECOND),
    ));
    let concurrent_b = tokio::spawn(write_feed(
        client.clone(),
        b.url("/v1/db/shop/coll/feed/bulk"),
        bearer_b.clone(),
        feed_ids("b2", SECOND),
    ));
    concurrent_a.await.unwrap();
    concurrent_b.await.unwrap();

    let written: BTreeSet<String> = [
        feed_ids("a", FIRST),
        feed_ids("b", FIRST),
        feed_ids("a2", SECOND),
        feed_ids("b2", SECOND),
    ]
    .concat()
    .into_iter()
    .collect();
    let missed: BTreeSet<String> = written.difference(&seen).cloned().collect();

    // Both members hold every write before either resumed stream is read, so
    // an id not delivered is the stream's fault and not replication lag.
    let total = written.len() as i64;
    for node in [&a, &b] {
        eventually("A and B to hold every write", || {
            let client = &client;
            let bearer = bearer_a.clone();
            async move {
                let Ok(res) = client
                    .post(node.url("/v1/db/shop/coll/feed/count"))
                    .bearer_auth(&bearer)
                    .json(&serde_json::json!({ "filter": {} }))
                    .send()
                    .await
                else {
                    return false;
                };
                let body: serde_json::Value = res.json().await.unwrap_or_default();
                body["count"].as_i64() == Some(total)
            }
        })
        .await;
    }

    // Resumed on B: every id the client was not sent, repeats allowed.
    let mut on_b = open_feed(&b, &bearer_b, Some(&token)).await;
    let mut from_b: Vec<String> = Vec::new();
    let waited = tokio::time::timeout(patience(), async {
        while !missed.iter().all(|id| from_b.contains(id)) {
            from_b.push(next_feed_event(&mut on_b).await.0);
        }
    })
    .await;
    let not_delivered: Vec<&String> = missed.iter().filter(|id| !from_b.contains(id)).collect();
    assert!(
        waited.is_ok(),
        "resumed on B, {} of {} ids the client was never sent were not delivered: {not_delivered:?}",
        not_delivered.len(),
        missed.len()
    );
    assert!(from_b.iter().all(|id| written.contains(id)), "{from_b:?}");
    let repeats = from_b.len() - from_b.iter().collect::<BTreeSet<_>>().len();
    let already_seen = from_b.iter().filter(|id| seen.contains(*id)).count();
    eprintln!(
        "resumed on B: {} delivered for {} missed; {already_seen} the client had already seen, \
         {repeats} repeated",
        from_b.len(),
        missed.len()
    );
    // Bounds the fixture can break. B holds each write once, so nothing is
    // delivered twice; and the only events the client had seen that B may send
    // again are the ones A's stream sent after its last read that reached the
    // tail. A resume that replayed from the start would send all CUT_AFTER.
    assert_eq!(repeats, 0, "resumed on B, an event arrived twice: {from_b:?}");
    assert!(
        already_seen < CUT_AFTER,
        "resumed on B, all {already_seen} events the client had seen were sent again"
    );

    // Resumed on A, the member that issued the token: exactly what was missed.
    let mut on_a = open_feed(&a, &bearer_a, Some(&token)).await;
    let mut from_a: Vec<String> = Vec::new();
    tokio::time::timeout(patience(), async {
        while from_a.len() < missed.len() {
            from_a.push(next_feed_event(&mut on_a).await.0);
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("resumed on A, only {} of {} ids arrived", from_a.len(), missed.len())
    });
    let unique: BTreeSet<String> = from_a.iter().cloned().collect();
    assert_eq!(unique.len(), from_a.len(), "the issuing member repeated an event: {from_a:?}");
    assert_eq!(unique, missed, "the issuing member delivers exactly what the client missed");
}

// ---------------------------------------------------------------------------
// The divergence check reads a kept count, not the collection (ADR-174)
// ---------------------------------------------------------------------------

/// Bytes the node's process has read through `read(2)` and `pread(2)`, page
/// cache hits included: `rchar` in `/proc/<pid>/io`.
#[cfg(target_os = "linux")]
fn rchar(node: &Node) -> u64 {
    let io = std::fs::read_to_string(format!("/proc/{}/io", node.pid)).unwrap();
    io.lines()
        .find_map(|line| line.strip_prefix("rchar: "))
        .and_then(|n| n.trim().parse().ok())
        .expect("rchar in /proc/<pid>/io")
}

/// A labelled counter from `/metrics`.
#[cfg(target_os = "linux")]
async fn labelled(client: &reqwest::Client, node: &Node, series: &str) -> u64 {
    let body = client.get(node.url("/metrics")).send().await.unwrap().text().await.unwrap();
    body.lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.split_whitespace().last())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

/// An idle, converged member holding one collection of large documents reads
/// far less than that collection while the divergence check runs.
///
/// The check rotates through every collection a member holds, one a contact,
/// and before ADR-174 both members of a contact walked the probed collection,
/// reading every page of it. Over a window long enough for the rotation to
/// reach the large collection several times, that was several times its size;
/// reading a kept count, it is sync traffic.
///
/// **redb's read cache is set below the collection's size.** A walk served from
/// the cache makes no `read` call, so with the default 256 MiB cache a
/// collection this size is walked invisibly: on 0.29.1 the same fixture read
/// 8,192 bytes in the window while comparing 58 counts. The cost this guards
/// was measured on a 400 MiB collection, larger than the cache, which is the
/// shape reproduced here at the smallest cache a node accepts.
///
/// Linux only, because `rchar` is a Linux counter. CI's cluster harness runs on
/// Linux.
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn the_divergence_check_reads_a_kept_count_not_the_collection() {
    const DOCS: usize = 160;
    const PAYLOAD: usize = 64 * 1024;
    const WINDOW: Duration = Duration::from_secs(30);

    let client = reqwest::Client::new();
    // 8 MiB, the smallest cache a node accepts, below the collection's 9.8 MiB.
    let (a, b, c) = three_nodes_with(&client, "cache_bytes = 8388608").await;
    let bearer = a.login(&client).await;

    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&bearer)
        .json(&serde_json::json!({ "name": "wide" }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    let payload = "x".repeat(PAYLOAD);
    for chunk in (0..DOCS as i64).collect::<Vec<_>>().chunks(16) {
        let batch: Vec<serde_json::Value> =
            chunk.iter().map(|i| serde_json::json!({ "_id": i, "payload": payload })).collect();
        let res = client
            .post(a.url("/v1/db/shop/coll/wide/bulk"))
            .bearer_auth(&bearer)
            .json(&batch)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200, "seeding: {:?}", res.text().await);
    }
    for node in [&a, &b, &c] {
        eventually("every member to hold the whole collection", || {
            let client = &client;
            let bearer = bearer.clone();
            async move {
                let Ok(res) = client
                    .post(node.url("/v1/db/shop/coll/wide/count"))
                    .bearer_auth(&bearer)
                    .json(&serde_json::json!({ "filter": {} }))
                    .send()
                    .await
                else {
                    return false;
                };
                let body: serde_json::Value = res.json().await.unwrap_or_default();
                body["count"].as_i64() == Some(DOCS as i64)
            }
        })
        .await;
    }

    let compared = r#"kimmy_sync_divergence_count_probes_total{outcome="compared"}"#;
    let probes_before = labelled(&client, &b, compared).await;
    let read_before = rchar(&b);
    tokio::time::sleep(WINDOW).await;
    let read = rchar(&b) - read_before;
    let probes = labelled(&client, &b, compared).await - probes_before;

    let collection = (DOCS * PAYLOAD) as u64;
    eprintln!(
        "B read {read} bytes over {WINDOW:?} and compared {probes} counts; the collection holds {collection}"
    );
    assert!(probes > 0, "the count half never compared, so the window measured nothing");
    assert!(
        read < collection,
        "B read {read} bytes over {WINDOW:?} while idle, and the collection holds {collection}: \
         the count is being walked"
    );
}

/// A peer connection the node fails to serve reaches
/// `kimmy_sync_serve_failures_total` through the hook the node installs on
/// its real listener: a TLS session that sends a frame the node cannot read.
#[tokio::test]
#[ignore = "boots a real clustered kimmyd; run with --ignored"]
async fn a_replication_connection_the_node_cannot_serve_is_counted_by_reason() {
    use tokio::io::AsyncWriteExt;
    let client = reqwest::Client::new();
    // Its one seed is nobody, so this fresh member has no peer to hear from and
    // serves as `unknown` once the wait runs out (ADR-202): a short one, so the
    // test waits on that and not on the default of two minutes.
    let node = Node::spawn_configured(
        "node-served",
        ports::choose(),
        &[ports::choose()],
        "",
        1,
        "catch_up_wait_secs = 2",
    );
    node.wait_ready(&client).await;
    let malformed = "kimmy_sync_serve_failures_total{reason=\"malformed\"}";
    assert_eq!(node.gauge(&client, malformed).await, Some(0), "premise: a row, at 0");

    let tls = kimmy_cluster::tls::ClusterTls::new().unwrap();
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", node.cluster)).await.unwrap();
    let mut stream =
        tls.connector().connect(kimmy_cluster::tls::ClusterTls::server_name(), tcp).await.unwrap();
    stream.write_all(&4u32.to_be_bytes()).await.unwrap();
    stream.write_all(b"junk").await.unwrap();
    stream.flush().await.unwrap();

    eventually("the unreadable frame to be counted as malformed", || {
        let (client, node) = (&client, &node);
        async move { node.gauge(client, malformed).await == Some(1) }
    })
    .await;
    drop(stream);
}

/// A member that has never completed a round reads a replication lag of 0,
/// the healthiest value there is. What says that 0 was never measured is the
/// replication loop's progress age (ADR-187): counted from the start, since
/// no round has completed, and still rising on the next scrape. Beside it the
/// stall probe's age stays small, so the age is the loop's own and not the
/// process's.
#[tokio::test]
#[ignore = "boots a real clustered kimmyd; run with --ignored"]
async fn a_member_that_never_completes_a_round_reads_lag_0_beside_an_age_that_climbs() {
    let client = reqwest::Client::new();
    // A seed nobody listens on, so every round fails. And no embedding
    // worker, so the row set below differs from the one a render with no
    // startup behind it shows: that is what a dropped fixing of the rows
    // would fall back to. The `[vector]` table rides in after `[storage]`'s
    // lines, where `spawn_with` puts what it is handed.
    let no_worker = "\n[vector]\nworker_enabled = false";
    // Fresh with a seed nobody answers, so it serves as `unknown` (ADR-202) once
    // the wait runs out, which is made short here.
    let node = Node::spawn_configured(
        "node-alone",
        ports::choose(),
        &[ports::choose()],
        no_worker,
        1,
        "catch_up_wait_secs = 2",
    );
    node.wait_ready(&client).await;

    let replication = "kimmy_task_progress_age_seconds{task=\"replication\"}";
    let probe = "kimmy_task_progress_age_seconds{task=\"stall_probe\"}";
    tokio::time::sleep(Duration::from_secs(2)).await;
    let first = node.gauge(&client, replication).await.expect("a replication age row");
    assert_eq!(node.gauge_f64(&client, "kimmy_replication_lag_seconds").await, Some(0.0));
    assert!(
        node.gauge(&client, "kimmy_sync_failures_total").await.is_some_and(|n| n > 0),
        "premise: the rounds are failing"
    );
    let uptime = node.gauge(&client, "kimmy_uptime_seconds").await.expect("uptime");
    assert!(first + 1 >= uptime, "counted from the start: age {first}, uptime {uptime}");

    tokio::time::sleep(Duration::from_secs(3)).await;
    let second = node.gauge(&client, replication).await.expect("a replication age row");
    assert!(second >= first + 2, "and still rising: {first} then {second}");
    let woke = node.gauge(&client, probe).await.expect("a stall probe age row");
    assert!(woke <= 1, "the probe is waking, so its age is its own: {woke}");

    // Exactly the writers this node starts, fixed at startup: a row missing
    // here is a writer nobody can alert on, and a row for one it does not run
    // would climb for ever.
    let body = client.get(node.url("/metrics")).send().await.unwrap().text().await.unwrap();
    let rows: Vec<&str> = body
        .lines()
        .filter_map(|l| l.strip_prefix("kimmy_task_progress_age_seconds{task=\""))
        .filter_map(|l| l.split('"').next())
        .collect();
    // The drop purger runs on every node, clustered or not (ADR-189), and so do the
    // expiry task and the yield evaluator (ADR-213).
    assert_eq!(
        rows,
        [
            "drop_purger",
            "replication",
            "stall_probe",
            "ttl_expiry",
            "webhook_dispatcher",
            "yield_evaluator"
        ]
    );
}

/// A stop with every duty running ends clean on every member: replication,
/// webhook dispatch, change streams open, a vector index built, a backup
/// served, and an exporter pointed at a collector that never answers. Each
/// member closes its store on the main thread, which it can only do when
/// nothing else holds the engine, so an `Arc` held past the runtime's
/// shutdown, a cycle among them, shows here as exit 75 and
/// `storage_not_closed` rather than as a repair on some later start.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_stop_with_every_duty_running_closes_every_members_store() {
    let client = reqwest::Client::new();
    // Connections complete into the backlog and are never read.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    // The storage section's text ends it, so a section of its own follows.
    let telemetry =
        format!("\n[telemetry]\nendpoint = \"http://{}\"\n", silent.local_addr().unwrap());
    let (a, b, c) = three_nodes_with(&client, &telemetry).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;

    let token = a.login(&client).await;
    let post = |node: &Node, path: &str, body: serde_json::Value| {
        client.post(node.url(path)).bearer_auth(&token).json(&body).send()
    };
    for name in ["feed", "vec"] {
        let res = post(&a, "/v1/db/shop/collections", serde_json::json!({ "name": name })).await;
        assert!(res.unwrap().status().is_success(), "{name}");
    }
    let (hook_addr, _seen) = receiver().await;
    let hook = serde_json::json!({ "url": format!("http://{hook_addr}/hook") });
    assert!(post(&a, "/v1/db/shop/coll/feed/webhooks", hook).await.unwrap().status().is_success());
    let vectors =
        serde_json::json!({ "fields": ["note"], "provider": { "kind": "byo" }, "dim": 3 });
    assert!(post(&a, "/v1/db/shop/coll/vec/vector", vectors).await.unwrap().status().is_success());
    // Above the 500 vectors below which a search is exact, so each member's
    // search builds a graph, which the index cache gauge then shows.
    const VECTORS: usize = 600;
    let docs: Vec<_> = (0..VECTORS).map(|i| serde_json::json!({ "_id": i, "note": "n" })).collect();
    let res = post(&a, "/v1/db/shop/coll/vec/bulk", serde_json::json!(docs)).await.unwrap();
    assert!(res.status().is_success(), "{:?}", res.text().await);
    for i in 0..VECTORS {
        let res = client
            .put(a.url(&format!("/v1/db/shop/coll/vec/docs/{i}/vectors")))
            .bearer_auth(&token)
            .json(&serde_json::json!([{ "chunk": 0, "vector": [1.0, i as f32, 0.0], "text": "n" }]))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success(), "{:?}", res.text().await);
    }
    pulls_settle(&client, &[&a, &b, &c]).await;

    let mut feeds = Vec::new();
    for node in [&a, &b, &c] {
        let bearer = node.login(&client).await;
        let search = client
            .post(node.url("/v1/db/shop/coll/vec/vector_search"))
            .bearer_auth(&bearer)
            .json(&serde_json::json!({ "vector": [1.0, 0.0, 0.0], "k": 3 }))
            .send()
            .await
            .unwrap();
        assert!(search.status().is_success(), "{}: {:?}", node.name, search.text().await);
        // The search queues the build, which a builder thread runs (ADR-245):
        // the gauge is polled, and must hold a graph for three reads running.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let mut held = 0;
        while held < 3 {
            let graph = node.gauge(&client, "kimmy_vector_index_cache_bytes").await;
            held = if graph.is_some_and(|bytes| bytes > 0) { held + 1 } else { 0 };
            assert!(
                std::time::Instant::now() < deadline,
                "{}: no graph built: {graph:?}",
                node.name
            );
            tokio::time::sleep(POLL).await;
        }
        feeds.push(open_feed(node, &bearer, None).await);
    }
    let backup = client.get(a.url("/v1/admin/backup")).bearer_auth(&token).send().await.unwrap();
    assert!(backup.status().is_success());
    assert!(!backup.bytes().await.unwrap().is_empty());
    for i in 0..50 {
        let doc = serde_json::json!({ "_id": format!("f{i}") });
        assert!(post(&a, "/v1/db/shop/coll/feed/docs", doc).await.unwrap().status().is_success());
    }

    for node in [&a, &b, &c] {
        node.signal("TERM");
    }
    for node in [&a, &b, &c] {
        let deadline = std::time::Instant::now() + Duration::from_secs(40);
        let status = loop {
            if let Some(status) = node.child.lock().unwrap().try_wait().unwrap() {
                break status;
            }
            assert!(std::time::Instant::now() < deadline, "{} did not exit", node.name);
            tokio::time::sleep(POLL).await;
        };
        let log = std::fs::read_to_string(node.dir.path().join("stdout.log")).unwrap_or_default();
        assert!(status.success(), "{}: {status:?}\n{log}", node.name);
        assert!(log.contains("engine closed"), "{}: {log}", node.name);
        let marker =
            std::fs::read_to_string(node.dir.path().join("data").join("kimmy.last-exit")).unwrap();
        assert!(marker.contains("exit = \"shutdown\""), "{}: {marker}", node.name);
    }
    drop(feeds);
    drop(silent);
}

/// **The sync livelock after a large one-origin load** (HIGH, round 0430;
/// since ADR-171, v0.29.0). A member that stayed quiet through a large load
/// on another serves its peers' pulls from their position on its own origin,
/// from before the load, and the walk passes over the whole load they already
/// hold. Uncapped, past the round's fixed 30 s it timed out, kept nothing,
/// and the next round walked the same rows, so the quiet member's own writes
/// never left it. Now the walk ends at a budget and answers a partial window,
/// whose end the requester keeps, so each pull starts further on (ADR-194).
///
/// Deterministic and small: C's every walk row is slowed
/// (`KIMMY_TEST_WALK_ROW_MS`), so a walk over A's 2,200-document load would
/// take about 55 s, past the round's budget, as a cold walk of 2 GB did on the
/// lab host; and C's serve budget is 500 rows with the time lifted
/// (`KIMMY_TEST_SERVE_WALK_*`), so each window is about 12.5 s, inside it.
/// Then C takes one write, which must reach A and B, each draining C in
/// exactly as many partial windows as C's oplog holds whole budgets of rows.
/// On a build before the budget, the write never reaches them.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_quiet_members_write_reaches_its_peers_after_a_large_load_elsewhere() {
    const LOAD: usize = 2_200;
    let client = reqwest::Client::new();
    let (a, b, mut c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let token = a.login(&client).await;
    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success());
    for batch in 0..LOAD / 550 {
        let docs: Vec<_> =
            (0..550).map(|i| serde_json::json!({ "_id": batch * 550 + i })).collect();
        let bulk = client
            .post(a.url("/v1/db/shop/coll/orders/bulk"))
            .bearer_auth(&token)
            .json(&docs)
            .send()
            .await
            .unwrap();
        assert!(bulk.status().is_success());
    }
    pulls_settle(&client, &[&a, &b, &c]).await;
    let last = format!("/v1/db/shop/coll/orders/docs/{}", LOAD - 1);
    let held = client.get(c.url(&last)).bearer_auth(&token).send().await.unwrap();
    assert!(held.status().is_success(), "premise: C holds the load");

    // Every walk row on C takes 25 ms from here: a walk over the load, 50 s.
    let before = c.restart_with(&[
        ("KIMMY_TEST_WALK_ROW_MS", "25"),
        ("KIMMY_TEST_SERVE_WALK_ROWS", "500"),
        ("KIMMY_TEST_SERVE_WALK_MS", "600000"),
    ]);
    assert!(before.success(), "{before:?}");
    c.wait_ready(&client).await;
    let c_token = c.login(&client).await;
    let wrote = client
        .post(c.url("/v1/db/shop/coll/orders/docs"))
        .bearer_auth(&c_token)
        .json(&serde_json::json!({ "_id": "from-c" }))
        .send()
        .await
        .unwrap();
    assert!(wrote.status().is_success(), "{}", wrote.text().await.unwrap());

    // What C's oplog holds now, the write included: every row a pull from
    // it walks, from the start of the log.
    let rows = c.gauge(&client, "kimmy_oplog_entries").await.expect("C's oplog length");
    // A partial window's end is re-examined by the next pull when an origin
    // sorts after the node its last row belongs to (the coverage rule's tie,
    // ADR-148): a row or two a pull, three origins at most. A load well away
    // from a multiple of the budget keeps that from moving the count.
    let (budget, margin) = (500, 3 * 4);
    assert!(
        rows % budget > margin && rows % budget < budget - margin,
        "premise: {rows} rows sit too near a multiple of {budget} for an exact count"
    );

    // A guard against a hang only: the assertions below are the pulls.
    let started = std::time::Instant::now();
    let guard = patience() * 4;
    loop {
        let got = client
            .get(a.url("/v1/db/shop/coll/orders/docs/from-c"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        if got.status().is_success() {
            break;
        }
        assert!(
            started.elapsed() < guard,
            "C's write never reached A; A's log:\n{}",
            a.log().lines().filter(|l| l.contains("sync round")).collect::<Vec<_>>().join("\n")
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    eventually("the write to reach B", || {
        let url = b.url("/v1/db/shop/coll/orders/docs/from-c");
        let (client, token) = (client.clone(), token.clone());
        async move {
            client.get(url).bearer_auth(&token).send().await.is_ok_and(|r| r.status().is_success())
        }
    })
    .await;

    // Each requester drained C on its own: every pull while more than a
    // budget of rows lay ahead ended at the budget, a partial window, and the
    // one after reached the tail and carried the write (ADR-194).
    let from_c = format!("peer=127.0.0.1:{}", c.cluster);
    // The log is coloured: `peer`, `=` and the value each sit in escapes.
    let plain = |line: &str| {
        let mut out = String::with_capacity(line.len());
        let mut chars = line.chars();
        while let Some(ch) = chars.next() {
            if ch == '\u{1b}' {
                chars.by_ref().find(|c| *c == 'm');
            } else {
                out.push(ch);
            }
        }
        out
    };
    for (name, node) in [("A", &a), ("B", &b)] {
        let partial: u64 = node
            .log()
            .lines()
            .map(plain)
            .filter(|l| l.contains("merged from peer") && l.contains(&from_c))
            .filter_map(|l| {
                let value = l.split("partial=").nth(1)?.split_whitespace().next()?;
                value.parse::<u64>().ok()
            })
            .sum();
        assert_eq!(
            partial,
            rows / budget,
            "{name} drained C's {rows} rows in {partial} partial windows"
        );
    }
}

/// A serve walk is in flight on `node`: one has started (`a serve walk started
/// for a peer's pull`, logged at the transport's debug level) and not ended (`a
/// serve walk ended`). Both lines come from the walk's own thread, so a walk
/// that a peer's connection no longer waits for is still one, and the
/// handshake and the version exchange before it are not. This replaced a guess
/// from which connections were open, which two overlapping pulls could satisfy
/// for a moment with no walk running.
fn a_serve_walk_is_in_flight(node: &Node) -> bool {
    let log = node.log();
    log.matches("a serve walk started for a peer's pull").count()
        > log.matches("a serve walk ended").count()
}

/// Wait until [`a_serve_walk_is_in_flight`], polling every 25 ms. Gives up
/// after [`patience`]. The walk is slowed by the test so that it lasts seconds,
/// and the caller signals the node at once: whether a walk was still in flight
/// at the signal is then asserted from the node's own log, `stopped serving a
/// peer's pull`.
async fn wait_for_a_serve_walk_in_flight(node: &Node) {
    let deadline = std::time::Instant::now() + patience();
    while !a_serve_walk_is_in_flight(node) {
        assert!(
            std::time::Instant::now() < deadline,
            "gave up waiting for a serve walk to start: {}",
            node.log()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// **The finding this change is for** (0.40.0, and since at least 0.39.0): a
/// member stopped while its peers are pulling from it kept serving the walk
/// in flight until it ended, so a supervisor killed it at its stop timeout
/// and its next start repaired the whole store.
///
/// Node B's every walk is slowed so that the pulls A and C make of it each
/// hold a serve walk for seconds, as a cold walk of a large oplog does, and
/// its served window's own budget is raised past that so the row count ends
/// it, not ADR-194's default two seconds. B is stopped once a pull has held
/// mid-walk ([`wait_for_a_serve_walk_in_flight`]), not on a bare connection: its serve walks end at the signal, and it exits 0
/// promptly with the store closed (`engine closed`, the `shutdown` marker),
/// and its next start repairs nothing.
///
/// Its drain is idle, so `close_writes` would end a request's walk at once
/// too: this test covers the finding, and with no row checks at all the stop
/// takes 10 s. `a_peers_pull_ends_at_the_signal_while_a_clients_request_drains`
/// is its pair, and covers a serve walk mislabelled as a request's.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_member_stopped_while_serving_its_peers_pulls_exits_promptly_and_closes_its_store() {
    let client = reqwest::Client::new();
    let (a, mut b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let token = a.login(&client).await;
    let post = |node: &Node, path: &str, body: serde_json::Value| {
        client.post(node.url(path)).bearer_auth(&token).json(&body).send()
    };
    assert!(
        post(&a, "/v1/db/shop/collections", serde_json::json!({ "name": "orders" }))
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let docs: Vec<_> = (0..400).map(|i| serde_json::json!({ "_id": i })).collect();
    assert!(
        post(&a, "/v1/db/shop/coll/orders/bulk", serde_json::json!(docs))
            .await
            .unwrap()
            .status()
            .is_success()
    );
    pulls_settle(&client, &[&a, &b, &c]).await;

    // Every row of every walk on B takes 25 ms from here, and a served
    // window's budget is 500 rows with its time lifted out of the way, so a
    // pull of its oplog holds a serve walk in flight for seconds, not just
    // ADR-194's default two: its peers' pulls keep serve walks in flight.
    let before = b.restart_with(&[
        ("KIMMY_TEST_WALK_ROW_MS", "25"),
        ("KIMMY_TEST_SERVE_WALK_ROWS", "500"),
        ("KIMMY_TEST_SERVE_WALK_MS", "600000"),
        ("RUST_LOG", "info,kimmy_cluster::transport=debug"),
    ]);
    assert!(before.success(), "{before:?}");
    b.wait_ready(&client).await;
    // Writes stream into B, so each of its peers' pulls, every second, has
    // new entries to be served: a serve walk of a second or more on B.
    let writer = {
        let url = b.url("/v1/db/shop/coll/orders/bulk");
        let token = b.login(&client).await;
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            for batch in 0u64.. {
                let docs: Vec<_> = (0..20)
                    .map(|i| serde_json::json!({ "_id": format!("s{batch}-{i}") }))
                    .collect();
                if client.post(&url).bearer_auth(&token).json(&docs).send().await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };
    wait_for_a_serve_walk_in_flight(&b).await;

    let started = std::time::Instant::now();
    b.signal("TERM");
    let status = b.wait_exit(Duration::from_secs(60));
    let took = started.elapsed();
    writer.abort();
    let log = b.log();
    assert!(status.success(), "{status:?}\n{log}");
    assert!(took < Duration::from_secs(3), "the stop waited for the serve walks: {took:?}\n{log}");
    assert!(log.contains("stopped serving a peer's pull"), "no serve walk was in flight:\n{log}");
    // B's own pulls stop at the signal too, and that is no peer's failure.
    let stopping = &log[log.find("shutdown signal received").unwrap_or(0)..];
    assert!(!stopping.contains("sync round failed"), "{stopping}");
    assert!(log.contains("engine closed"), "{log}");
    let marker =
        std::fs::read_to_string(b.dir.path().join("data").join("kimmy.last-exit")).unwrap();
    assert!(marker.contains("exit = \"shutdown\""), "{marker}");

    b.restart_with(&[]);
    b.wait_ready(&client).await;
    let log = b.log();
    assert!(log.contains("previous run ended cleanly"), "{log}");
    assert!(!log.contains("repairing the database"), "{log}");
}

/// **0.40.1's panic at the stop**: a member stopped while one of its
/// replication rounds was inside a storage step logged `a thread panicked`,
/// "A Tokio 1.x context was found, but it is being shutdown". The round's
/// task, aborted at the signal, ran on after the step until its next yield,
/// and polled its deadline there after the runtime had begun to shut down.
///
/// B's applies of its peers' batches are slowed (`slow_apply`), and B is
/// stopped just after one begins, while A's writes stream in. Each time, B
/// exits 0 with its store closed and logs no panic and no error on the way
/// out: the stop waits for its tasks to end before it shuts the runtime down.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_member_stopped_inside_an_apply_exits_without_a_panic() {
    const TIMES: usize = 5;
    const SLOWED: &str = "a slowed apply of a peer's batch, on purpose";
    let client = reqwest::Client::new();
    let (a, mut b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let token = a.login(&client).await;
    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success());
    // Writes stream into A, so B always has a batch to pull and apply.
    let writer = {
        let url = a.url("/v1/db/shop/coll/orders/bulk");
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            for batch in 0u64.. {
                let docs: Vec<_> = (0..20)
                    .map(|i| serde_json::json!({ "_id": format!("w{batch}-{i}") }))
                    .collect();
                if client.post(&url).bearer_auth(&token).json(&docs).send().await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    for time in 1..=TIMES {
        let before = b.restart_with(&[("KIMMY_TEST_STOP", "slow_apply:1500")]);
        assert!(before.success(), "stop {}: {before:?}\n{}", time - 1, b.log());
        b.wait_ready(&client).await;
        // Stopped just after an apply begins: its step has 1.5 s to run.
        let seen = b.log().matches(SLOWED).count();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while b.log().matches(SLOWED).count() == seen {
            assert!(std::time::Instant::now() < deadline, "no apply began:\n{}", b.log());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        b.signal("TERM");
        let status = b.wait_exit(Duration::from_secs(60));
        let log = b.log();
        let stopping = &log[log.rfind("shutdown signal received").expect("the stop's line")..];
        assert!(status.success(), "stop {time}: {status:?}\n{stopping}");
        assert!(!stopping.contains("panicked"), "stop {time}:\n{stopping}");
        // The level, whether the log is coloured or not.
        assert!(!stopping.contains("ERROR"), "stop {time}:\n{stopping}");
        assert!(!stopping.contains("being shutdown"), "stop {time}:\n{stopping}");
        assert!(stopping.contains("engine closed"), "stop {time}:\n{stopping}");
        let marker =
            std::fs::read_to_string(b.dir.path().join("data").join("kimmy.last-exit")).unwrap();
        assert!(marker.contains("exit = \"shutdown\""), "stop {time}: {marker}");
    }
    writer.abort();
    drop((a, c));
}

/// And a task that does not end within the stop's window is aborted there,
/// as before this waited for it, and the exit says what happened: B's apply
/// is slowed past the whole stop, so its thread still holds the engine when
/// the runtime has shut down, and the store is not closed. The stop still
/// ends within its budget, and the next start repairs.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_task_stuck_past_the_stops_window_is_aborted_and_the_exit_is_honest() {
    const SLOWED: &str = "a slowed apply of a peer's batch, on purpose";
    let client = reqwest::Client::new();
    let (a, mut b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let token = a.login(&client).await;
    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success());
    let before = b.restart_with(&[("KIMMY_TEST_STOP", "slow_apply:60000")]);
    assert!(before.success(), "{before:?}");
    b.wait_ready(&client).await;
    let docs: Vec<_> = (0..20).map(|i| serde_json::json!({ "_id": i })).collect();
    let bulk = client
        .post(a.url("/v1/db/shop/coll/orders/bulk"))
        .bearer_auth(&token)
        .json(&docs)
        .send()
        .await
        .unwrap();
    assert!(bulk.status().is_success());
    eventually("B to begin an apply", || {
        let begun = b.log().contains(SLOWED);
        async move { begun }
    })
    .await;

    let started = std::time::Instant::now();
    b.signal("TERM");
    let status = b.wait_exit(Duration::from_secs(60));
    let took = started.elapsed();
    let log = b.log();
    assert!(took < Duration::from_secs(30), "the stop outran its budget: {took:?}\n{log}");
    assert!(
        log.contains("a background task was still stopping at the end of the stop's window"),
        "{log}"
    );
    assert_eq!(status.code(), Some(75), "{status:?}\n{log}");
    let marker =
        std::fs::read_to_string(b.dir.path().join("data").join("kimmy.last-exit")).unwrap();
    assert!(marker.contains("exit = \"storage_not_closed\""), "{marker}");
    drop((a, c));
}

/// A schema change in flight at the signal is a client's request, which the
/// drain lets finish: its index build and its confirmation's push walk go
/// on through the drain (ADR-192), and the members confirm it before the
/// node stops. Only a walk that serves no client ends at the signal.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_schema_change_in_flight_at_the_signal_completes_in_the_drain() {
    let client = reqwest::Client::new();
    let (mut a, b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let token = a.login(&client).await;
    let post = |node: &Node, path: &str, body: serde_json::Value| {
        client.post(node.url(path)).bearer_auth(&token).json(&body).send()
    };
    assert!(
        post(&a, "/v1/db/shop/collections", serde_json::json!({ "name": "sessions" }))
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let docs: Vec<_> = (0..200).map(|i| serde_json::json!({ "_id": i, "seen": i })).collect();
    assert!(
        post(&a, "/v1/db/shop/coll/sessions/bulk", serde_json::json!(docs))
            .await
            .unwrap()
            .status()
            .is_success()
    );
    pulls_settle(&client, &[&a, &b, &c]).await;

    // A's build reads 200 documents at 20 ms each: seconds, so the signal
    // arrives while it runs.
    assert!(a.restart_with(&[("KIMMY_TEST_WALK_ROW_MS", "20")]).success());
    a.wait_ready(&client).await;
    let token = a.login(&client).await;
    let create = {
        let url = a.url("/v1/db/shop/coll/sessions/indexes");
        tokio::spawn(async move {
            let res = reqwest::Client::new()
                .post(url)
                .bearer_auth(token)
                .json(&serde_json::json!({ "name": "by_seen", "fields": [{ "path": "seen" }] }))
                .send()
                .await
                .unwrap();
            (res.status(), res.json::<serde_json::Value>().await.unwrap())
        })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    a.signal("TERM");
    let (status, created) = create.await.unwrap();
    assert!(status.is_success(), "{status}: {created}");
    assert_eq!(created["confirmation"]["confirmed"].as_array().map(Vec::len), Some(2), "{created}");
    let exited = a.wait_exit(Duration::from_secs(60));
    assert!(exited.success(), "{exited:?}\n{}", a.log());
    assert!(a.log().contains("engine closed"), "{}", a.log());
}

/// Seconds since midnight of a pretty log line's timestamp, such as
/// `2026-09-27T00:04:54.993839Z`, after its colour escapes.
fn logged_at(line: &str) -> Option<f64> {
    let plain: String = {
        let mut out = String::new();
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\x1b' {
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
    };
    let time = plain.trim_start().split('T').nth(1)?.split('Z').next()?;
    let mut parts = time.split(':');
    let h: f64 = parts.next()?.parse().ok()?;
    let m: f64 = parts.next()?.parse().ok()?;
    let s: f64 = parts.next()?.parse().ok()?;
    Some(h * 3600.0 + m * 60.0 + s)
}

/// The scopes apart: while a client's request keeps the drain busy, a pull a
/// peer is making of this node ends at the signal, not at the drain's
/// deadline. Its serve walk serves no client.
///
/// The pair of the headline test above: that one's idle drain ends a
/// request's walk as promptly as a background one, so only this one, with the
/// drain held, tells a serve walk mislabelled as a request's apart.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_peers_pull_ends_at_the_signal_while_a_clients_request_drains() {
    let client = reqwest::Client::new();
    let (a, mut b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let token = a.login(&client).await;
    assert!(
        client
            .post(a.url("/v1/db/shop/collections"))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "name": "orders" }))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let docs: Vec<_> = (0..200).map(|i| serde_json::json!({ "_id": i })).collect();
    assert!(
        client
            .post(a.url("/v1/db/shop/coll/orders/bulk"))
            .bearer_auth(&token)
            .json(&docs)
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    pulls_settle(&client, &[&a, &b, &c]).await;

    // 200 ms a row on B, and a served window's budget 60 rows with its time
    // lifted out of the way: a pull of a few new entries is a serve walk of
    // a dozen seconds by that row count, not cut short by ADR-194's default
    // two, so it is reliably still open when the signal arrives, and a
    // backup of the store outlasts the drain.
    assert!(
        b.restart_with(&[
            ("KIMMY_TEST_WALK_ROW_MS", "200"),
            ("KIMMY_TEST_SERVE_WALK_ROWS", "60"),
            ("KIMMY_TEST_SERVE_WALK_MS", "600000"),
            ("RUST_LOG", "info,kimmy_cluster::transport=debug"),
        ])
        .success()
    );
    b.wait_ready(&client).await;
    let token = b.login(&client).await;
    let writer = {
        let url = b.url("/v1/db/shop/coll/orders/bulk");
        let token = token.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            for batch in 0u64.. {
                let docs: Vec<_> = (0..20)
                    .map(|i| serde_json::json!({ "_id": format!("s{batch}-{i}") }))
                    .collect();
                if client.post(&url).bearer_auth(&token).json(&docs).send().await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(4)).await;
    let backup = {
        let url = b.url("/v1/admin/backup");
        tokio::spawn(async move { reqwest::Client::new().get(url).bearer_auth(token).send().await })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    wait_for_a_serve_walk_in_flight(&b).await;
    b.signal("TERM");
    let status = b.wait_exit(Duration::from_secs(60));
    writer.abort();
    backup.abort();
    let log = b.log();
    assert!(status.success(), "{status:?}\n{log}");
    let signal = log
        .lines()
        .find(|l| l.contains("shutdown signal received"))
        .and_then(logged_at)
        .expect("the signal logged");
    let stopped = log
        .lines()
        .filter(|l| l.contains("stopped serving a peer's pull"))
        .filter_map(logged_at)
        .find(|at| *at >= signal)
        .unwrap_or_else(|| panic!("no pull was being served at the signal:\n{log}"));
    assert!(
        stopped - signal < 3.0,
        "the serve walk ran on {:.1} s into the drain, as a request's would\n{log}",
        stopped - signal
    );
}

/// The finding: on 0.41.0 `kimmy_replication_lag_seconds` reads 0
/// regardless of how far behind a member actually is. Round 0440's sampler
/// found 0 non-zero readings out of 16,021 samples across three members;
/// rounds 0420 and 0430 (0.40.x) saw 80-201 non-zero readings per replica,
/// up to 72-235 s. This is a HIGH regression on the gauge an operator's
/// alert depends on (ADR-046, ADR-122).
///
/// Same shape as `a_quiet_members_write_reaches_its_peers_after_a_large_load_elsewhere`
/// above, because it needs the same thing that test needs: a real, sustained
/// gap between what C has written and what B has pulled, wide enough to
/// scrape mid-flight rather than race a single poll against it. C's serve
/// walk is slowed (`KIMMY_TEST_WALK_ROW_MS`) so a single new write still
/// takes many seconds to reach B, and B's `kimmy_replication_lag_seconds` is
/// polled throughout: it must read non-zero while the write is in flight,
/// and clear once it lands.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn kimmy_replication_lag_seconds_reads_non_zero_while_a_member_trails_and_clears_once_it_catches_up()
 {
    const LOAD: usize = 2_200;
    let client = reqwest::Client::new();
    let (a, b, mut c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let token = a.login(&client).await;
    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success());
    // C writes once, early, on the collection every member already has: so
    // B witnesses C's origin *before* the probe write below, matching the
    // finding's own shape ("kimmy1 provably trailed kimmy2, an origin it
    // had applied from") rather than a never-seen origin, which reads 0 by
    // design (ADR-122) and would prove nothing about this regression.
    let c_token = c.login(&client).await;
    eventually("the collection to reach C before it writes to it", || {
        let client = client.clone();
        let url = c.url("/v1/db/shop/collections");
        let tok = c_token.clone();
        async move {
            let Ok(res) = client.get(url).bearer_auth(&tok).send().await else {
                return false;
            };
            res.text().await.is_ok_and(|body| body.contains("\"orders\""))
        }
    })
    .await;
    // `lag_behind_ms` reads `now − held.wall_ms`: the age of the newest
    // entry from this origin B has *applied*, not of whatever C writes
    // next (sync.rs). Once "from-c" lands B's held stamp moves to it, but
    // until then this early write is B's held stamp for C's origin, and the
    // real bound the gauge answers to is its age, not "from-c"'s.
    let held_stamp_written = std::time::Instant::now();
    let wrote_early = client
        .post(c.url("/v1/db/shop/coll/orders/docs"))
        .bearer_auth(&c_token)
        .json(&serde_json::json!({ "_id": "from-c-early" }))
        .send()
        .await
        .unwrap();
    assert!(wrote_early.status().is_success(), "{}", wrote_early.text().await.unwrap());
    for batch in 0..LOAD / 550 {
        let docs: Vec<_> =
            (0..550).map(|i| serde_json::json!({ "_id": batch * 550 + i })).collect();
        let bulk = client
            .post(a.url("/v1/db/shop/coll/orders/bulk"))
            .bearer_auth(&token)
            .json(&docs)
            .send()
            .await
            .unwrap();
        assert!(bulk.status().is_success());
    }
    pulls_settle(&client, &[&a, &b, &c]).await;
    let held_early = client
        .get(b.url("/v1/db/shop/coll/orders/docs/from-c-early"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert!(held_early.status().is_success(), "premise: B has already witnessed C's origin");
    let last = format!("/v1/db/shop/coll/orders/docs/{}", LOAD - 1);
    let held = client.get(c.url(&last)).bearer_auth(&token).send().await.unwrap();
    assert!(held.status().is_success(), "premise: C holds the load");

    eventually(
        "the premise: converged, so the gauge already reads 0 before the probe write",
        || {
            let client = client.clone();
            let b = &b;
            async move { b.gauge_f64(&client, "kimmy_replication_lag_seconds").await == Some(0.0) }
        },
    )
    .await;

    // Every walk row on C takes 30 ms from here: a walk over the load is
    // tens of seconds, wide enough to scrape mid-flight reliably.
    let before = c.restart_with(&[
        ("KIMMY_TEST_WALK_ROW_MS", "30"),
        ("KIMMY_TEST_SERVE_WALK_ROWS", "500"),
        ("KIMMY_TEST_SERVE_WALK_MS", "600000"),
    ]);
    assert!(before.success(), "{before:?}");
    c.wait_ready(&client).await;
    let c_token = c.login(&client).await;
    let wrote = client
        .post(c.url("/v1/db/shop/coll/orders/docs"))
        .bearer_auth(&c_token)
        .json(&serde_json::json!({ "_id": "from-c" }))
        .send()
        .await
        .unwrap();
    assert!(wrote.status().is_success(), "{}", wrote.text().await.unwrap());

    // Poll B's gauge while the write is provably still in flight (B does not
    // yet hold it), and require at least one non-zero reading before it
    // arrives -- the regression this test exists to catch is "always 0",
    // not "sometimes slow to update". Every reading is also checked against
    // the test's own monotonic clock, started just before "from-c-early"
    // was written: `lag_behind_ms` reads `now − held.wall_ms`, the age of
    // B's held stamp for C's origin -- "from-c-early", until "from-c"
    // lands -- so it can never exceed how long it has actually been, real
    // wall time, since that entry was written. A wrapped, overflowed, or
    // otherwise made-up value would trip this, not just a merely-zero one.
    let deadline = std::time::Instant::now() + patience() * 4;
    let mut saw_nonzero = false;
    let mut max_lag_secs = 0.0_f64;
    loop {
        let has_it = client
            .get(b.url("/v1/db/shop/coll/orders/docs/from-c"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status()
            .is_success();
        if has_it {
            break;
        }
        let lag = b.gauge_f64(&client, "kimmy_replication_lag_seconds").await;
        if let Some(lag_secs) = lag {
            if lag_secs > 0.0 {
                saw_nonzero = true;
                max_lag_secs = max_lag_secs.max(lag_secs);
            }
            let real_elapsed_secs = held_stamp_written.elapsed().as_secs_f64();
            assert!(
                lag_secs <= real_elapsed_secs + 1.0,
                "kimmy_replication_lag_seconds read {lag_secs} s, more than the {real_elapsed_secs} \
                 s the test's own clock says have actually passed since B's held stamp \
                 (\"from-c-early\") was written -- lag_behind_ms disagrees with real time"
            );
        }
        assert!(
            std::time::Instant::now() < deadline,
            "C's write never reached B; B's log:\n{}",
            b.log().lines().filter(|l| l.contains("sync round")).collect::<Vec<_>>().join("\n")
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        saw_nonzero,
        "kimmy_replication_lag_seconds never read non-zero while B provably trailed C \
         (held an origin's entry, then a later one from the same origin had not arrived) -- \
         the gauge is stuck at 0 (max observed: {max_lag_secs} s)"
    );

    // And it clears once the write has actually landed -- not stuck non-zero
    // either, which would be the opposite failure.
    eventually("the lag gauge to clear once B holds the write", || {
        let client = client.clone();
        let b = &b;
        async move { b.gauge_f64(&client, "kimmy_replication_lag_seconds").await == Some(0.0) }
    })
    .await;
}

/// ADR-195: a contact that ends a tick's budget while still advancing
/// resumes on the very next tick, not after waiting out the rest of
/// `cluster.sync_interval_secs`.
///
/// `fits_before` (ADR-157) only stops a contact short of the tick's own
/// deadline once the *slowest pull it has already made* would not fit in
/// what is left -- so the wasted idle a reset reclaims is bounded by one
/// pull's own cost, never more. A pull cheap next to the interval (as a
/// fast pull always is on a healthy cluster) makes that idle gap tiny
/// either way, reset or not, and hides the very defect this test exists to
/// catch: it is only visible when one pull costs a large-enough share of
/// the interval that a contact can fit just one of them before its own
/// slowest-pull estimate refuses a second. This test's serve walk is
/// slowed to just over half the interval for exactly that reason.
#[tokio::test]
#[ignore = "boots a real two-node cluster; run with --ignored"]
async fn a_draining_advancing_contact_resumes_at_once_not_after_the_interval() {
    const TOTAL: i64 = 300;
    // Chosen so one pull's serve walk (below) costs just over half of it:
    // a contact fits exactly one such pull, then `fits_before` refuses a
    // second, ending `Budget` with close to half the interval still
    // unused -- unused time a reset reclaims and a full-interval wait
    // would otherwise burn.
    const INTERVAL_SECS: u64 = 5;
    // Every row A's serve walk examines from here costs 80 ms, and the
    // walk itself stops at 2,700 ms of that cost -- just over half of
    // `INTERVAL_SECS` -- so each pull serves roughly 33 rows and a
    // contact fits exactly one of them before the interval's own deadline
    // rules out a second (ADR-157's `fits_before`, judged against the
    // slowest pull already made). TOTAL/33 pulls, one per contact, is
    // enough to cross several tick boundaries.
    const WALK_ROW_MS: u64 = 80;
    const SERVE_WALK_MS: u64 = 2_700;
    let client = reqwest::Client::new();
    let (pa, pb) = (ports::choose(), ports::choose());
    let mut a = Node::spawn_with_interval("node-a", pa, &[pb], "", INTERVAL_SECS);
    let b = Node::spawn_with_interval("node-b", pb, &[pa], "", INTERVAL_SECS);
    a.wait_ready(&client).await;
    b.wait_ready(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b], 1)).await;

    let token = a.login(&client).await;
    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success());
    for batch in 0..TOTAL / 50 {
        let docs: Vec<_> = (0..50).map(|i| serde_json::json!({ "_id": batch * 50 + i })).collect();
        let bulk = client
            .post(a.url("/v1/db/shop/coll/orders/bulk"))
            .bearer_auth(&token)
            .json(&docs)
            .send()
            .await
            .unwrap();
        assert!(bulk.status().is_success());
    }

    let (walk_row_ms, serve_walk_ms) = (WALK_ROW_MS.to_string(), SERVE_WALK_MS.to_string());
    let before = a.restart_with(&[
        ("KIMMY_TEST_WALK_ROW_MS", walk_row_ms.as_str()),
        ("KIMMY_TEST_SERVE_WALK_MS", serve_walk_ms.as_str()),
    ]);
    assert!(before.success(), "{before:?}");
    a.wait_ready(&client).await;

    // "merged from peer" is logged once per CONTACT -- a whole tick's
    // worth of pulls against one peer, not one line per pull -- so only
    // lines seen from here count: the gossip contact that formed the
    // cluster before any of this test's load existed would otherwise
    // show up as a spurious, unrelated first sighting.
    let mut seen = b.log().lines().filter(|l| l.contains("merged from peer")).count();
    // Each sighting keeps its own contact's pull count and whether it
    // advanced (applied, ddl, or a partial window -- the same gate ADR-195
    // resumes on) alongside the Instant it was detected at, timestamped by
    // this test's own clock rather than the log's embedded one: at a 20 ms
    // poll against pulls hundreds of milliseconds apart, the poll
    // granularity cannot hide a gap anywhere near the five-second interval
    // it would take to be mistaken for one.
    let mut sightings: Vec<(std::time::Instant, usize, bool)> = Vec::new();
    let deadline = std::time::Instant::now() + patience() * 8;
    loop {
        let lines: Vec<String> =
            b.log().lines().filter(|l| l.contains("merged from peer")).map(str::to_owned).collect();
        if lines.len() > seen {
            let now = std::time::Instant::now();
            for line in &lines[seen..] {
                // The pretty log format's field names and their "=" are
                // colored separately (`with_ansi` has no terminal to
                // auto-detect against, writing to a file), so "pulls=" is
                // not contiguous text in the raw line without stripping the
                // SGR escapes between them first.
                let plain = strip_ansi(line);
                let pulls = parse_field(&plain, "pulls=").unwrap_or(1);
                let advanced = parse_field(&plain, "applied=").unwrap_or(0) > 0
                    || parse_field(&plain, "ddl=").unwrap_or(0) > 0
                    || parse_field(&plain, "partial=").unwrap_or(0) > 0;
                sightings.push((now, pulls, advanced));
            }
            seen = lines.len();
        }
        let count_res = client
            .post(b.url("/v1/db/shop/coll/orders/count"))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "filter": {} }))
            .send()
            .await;
        let done = if let Ok(res) = count_res {
            let body: serde_json::Value = res.json().await.unwrap_or_default();
            body["count"].as_i64() == Some(TOTAL)
        } else {
            false
        };
        if done {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the drain never finished; B's log:\n{}",
            b.log()
                .lines()
                .filter(|l| l.contains("merged from peer"))
                .collect::<Vec<_>>()
                .join("\n")
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        sightings.len() >= 3,
        "the drain must take several contacts to prove anything about crossing a tick \
         boundary: only {} sightings",
        sightings.len()
    );
    // A gap between two contacts is the time from the end of one to the
    // end of the next, which includes that next contact's own multi-pull
    // duration -- so a flat threshold cannot tell "waited out the
    // interval" apart from "did its own genuine (slow, by design here)
    // work". Bound it instead by what the next contact's own pull count
    // can explain: a per-pull ceiling generous enough to cover the serve
    // walk's own budget plus the apply and dial/handshake overhead besides,
    // times its pulls, plus a flat slack. This has to sit strictly between
    // the two cases it exists to tell apart: comfortably above a reset
    // resuming at once (one pull's own cost, `SERVE_WALK_MS` plus a small
    // constant overhead) and comfortably below waiting out the rest of
    // `INTERVAL_SECS` first. A gap the contact's own work can't account
    // for means the rest was spent waiting out the interval instead of
    // resuming at once.
    //
    // That bound only holds after a contact that *advanced* -- ADR-195
    // resumes an advancing contact at once, but a contact that found
    // nothing left to pull ends `CaughtUp`, not `Budget`, and legitimately
    // waits out the rest of the interval like any ordinary tick. The
    // drain's very last contact is exactly this: once B has everything, a
    // further contact against A that applies nothing is expected, and its
    // gap from the one before is the interval, not a bug.
    let per_pull_ceiling = Duration::from_millis(SERVE_WALK_MS + 800);
    let preamble_slack = Duration::from_millis(300);
    let mut checked = 0usize;
    for pair in sightings.windows(2) {
        let (t0, _, advanced0) = pair[0];
        let (t1, pulls1, _) = pair[1];
        if !advanced0 {
            continue;
        }
        checked += 1;
        let gap = t1.duration_since(t0);
        let allowed = per_pull_ceiling * pulls1 as u32 + preamble_slack;
        assert!(
            gap <= allowed,
            "a gap of {gap:?} after an advancing contact and before a {pulls1}-pull contact -- \
             allowing {allowed:?} for that contact's own cost -- is longer than its own work \
             explains, so this pull waited out the interval instead of resuming at once"
        );
    }
    assert!(
        checked >= 2,
        "the drain must cross at least two advancing contacts to prove anything about the \
         reset: only {checked} pairs followed an advancing contact"
    );
}

// ---------------------------------------------------------------------------
// The catching-up marker (ADR-202)
// ---------------------------------------------------------------------------

impl Node {
    fn catching_up_file(&self) -> std::path::PathBuf {
        self.dir.path().join("data").join("kimmy.catching-up")
    }

    /// `/readyz`: its status, the `x-kimmy-catching-up` header and the body.
    async fn readyz(
        &self,
        client: &reqwest::Client,
    ) -> Option<(u16, Option<String>, serde_json::Value)> {
        let res = client.get(self.url("/readyz")).send().await.ok()?;
        let status = res.status().as_u16();
        let header =
            res.headers().get("x-kimmy-catching-up").map(|v| v.to_str().unwrap().to_string());
        Some((status, header, res.json().await.unwrap_or(serde_json::Value::Null)))
    }

    /// Ready, with no marker: cleared, not merely serving as `unknown`.
    async fn cleared(&self, client: &reqwest::Client) -> bool {
        matches!(self.readyz(client).await, Some((200, None, body)) if body.get("catching_up").is_none())
            && !self.catching_up_file().exists()
    }

    /// Serving as `unknown`: the wait ran out with the marker still set.
    async fn unknown(&self, client: &reqwest::Client) -> bool {
        matches!(self.readyz(client).await, Some((200, Some(h), _)) if h == "unknown")
            && self.catching_up_file().exists()
    }
}

/// A whole cluster starting from nothing: every member is fresh, every member
/// has a root user and registers itself in the topology (both stamp the
/// member's own origin before its first contact), and every member clears, by the
/// mutual clear, within a few ticks. Nobody waits out the bound, nobody stays
/// `unknown`, and expiry runs once the marker is gone.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn an_all_fresh_cluster_clears_and_then_expires() {
    let client = reqwest::Client::new();
    let (a, b, c) = three_nodes(&client).await;
    for node in [&a, &b, &c] {
        eventually("the member to clear its marker", || node.cleared(&client)).await;
        assert_eq!(
            node.gauge(&client, "kimmy_catching_up{reason=\"none\"}").await,
            Some(1),
            "{}: {}",
            node.name,
            node.log()
        );
        assert!(
            node.log().contains("the catching-up marker was cleared"),
            "{}: {}",
            node.name,
            node.log()
        );
        assert!(!node.log().contains("catching up for longer than"), "no member waited the bound");
    }
    let token = a.login(&client).await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "sessions" }))
        .send()
        .await
        .unwrap();
    let created = client
        .post(a.url("/v1/db/shop/coll/sessions/indexes"))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "name": "ttl_seen",
            "fields": [{ "path": "seen" }],
            "expireAfterSeconds": 1,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    client
        .post(a.url("/v1/db/shop/coll/sessions/docs"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "_id": 1, "seen": { "$date": 0 } }))
        .send()
        .await
        .unwrap();
    eventually("the expired document to disappear", || {
        let (client, url, token) =
            (client.clone(), a.url("/v1/db/shop/coll/sessions/docs/1"), token.clone());
        async move { client.get(url).bearer_auth(&token).send().await.unwrap().status() == 404 }
    })
    .await;
}

/// Two fresh members whose seed list names a third that never starts, with
/// `cluster.expected_members = 3`: neither can clear against the other, because
/// the number of members they have reached plus themselves is 2. Past the wait
/// each serves as `unknown` with the marker set and expiry off; the WARN says so;
/// deleting the file on both brings expiry back with no restart.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn two_fresh_members_expecting_three_hold_and_the_operator_clears_them() {
    let client = reqwest::Client::new();
    let (pa, pb, gone) = (ports::choose(), ports::choose(), ports::choose());
    let extra = "catch_up_wait_secs = 3\nexpected_members = 3";
    let a = Node::spawn_configured("node-a", pa, &[pb, gone], "", 1, extra);
    let b = Node::spawn_configured("node-b", pb, &[pa, gone], "", 1, extra);
    eventually("both to serve as unknown", || async {
        a.unknown(&client).await && b.unknown(&client).await
    })
    .await;
    for node in [&a, &b] {
        assert_eq!(node.gauge(&client, "kimmy_catching_up{reason=\"unknown\"}").await, Some(1));
        // Said by the tick after the bound runs out, not by the request that sees it.
        eventually("the WARN that the marker is still set", || async {
            node.log().contains("catching up for longer than")
        })
        .await;
        assert!(node.catching_up_file().exists());
    }

    // Serving, but expiry is off: the collection is unowned for want of a member
    // not catching up, and the due document stays.
    let token = a.login(&client).await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "sessions" }))
        .send()
        .await
        .unwrap();
    client
        .post(a.url("/v1/db/shop/coll/sessions/indexes"))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "name": "ttl_seen",
            "fields": [{ "path": "seen" }],
            "expireAfterSeconds": 1,
        }))
        .send()
        .await
        .unwrap();
    client
        .post(a.url("/v1/db/shop/coll/sessions/docs"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "_id": 1, "seen": { "$date": 0 } }))
        .send()
        .await
        .unwrap();
    eventually("the collection to read as waiting on a member that is not catching up", || {
        let (client, a) = (client.clone(), &a);
        async move {
            a.gauge(&client, "kimmy_ttl_collections{state=\"unowned_catching_up\"}").await
                == Some(1)
        }
    })
    .await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let doc = client
        .get(a.url("/v1/db/shop/coll/sessions/docs/1"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(doc.status(), 200, "expiry is off while the marker is set");

    // The operator's clear, live, on both.
    std::fs::remove_file(a.catching_up_file()).unwrap();
    std::fs::remove_file(b.catching_up_file()).unwrap();
    eventually("both to clear", || async { a.cleared(&client).await && b.cleared(&client).await })
        .await;
    eventually("the expired document to go once expiry is back", || {
        let (client, url, token) =
            (client.clone(), a.url("/v1/db/shop/coll/sessions/docs/1"), token.clone());
        async move { client.get(url).bearer_auth(&token).send().await.unwrap().status() == 404 }
    })
    .await;
}

/// The same two members, with the third arriving: it is fresh too, the three
/// have now all been reached, and all three clear.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn the_third_member_arriving_clears_the_two_that_expected_it() {
    let client = reqwest::Client::new();
    let (pa, pb, pc) = (ports::choose(), ports::choose(), ports::choose());
    let extra = "catch_up_wait_secs = 3\nexpected_members = 3";
    let a = Node::spawn_configured("node-a", pa, &[pb, pc], "", 1, extra);
    let b = Node::spawn_configured("node-b", pb, &[pa, pc], "", 1, extra);
    eventually("both to serve as unknown", || async {
        a.unknown(&client).await && b.unknown(&client).await
    })
    .await;
    let c = Node::spawn_configured("node-c", pc, &[pa, pb], "", 1, extra);
    for node in [&a, &b, &c] {
        eventually("the member to clear", || node.cleared(&client)).await;
    }
}

/// The twin of the test above with `expected_members` unset: nothing tells the
/// two that a third exists, and it was never seen, so they clear against each
/// other. This is the documented cost of trusting discovery, made a test so it is
/// a decision on record.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn without_expected_members_two_fresh_members_clear_against_each_other() {
    let client = reqwest::Client::new();
    let (pa, pb, gone) = (ports::choose(), ports::choose(), ports::choose());
    let a = Node::spawn_configured("node-a", pa, &[pb, gone], "", 1, "");
    let b = Node::spawn_configured("node-b", pb, &[pa, gone], "", 1, "");
    for node in [&a, &b] {
        eventually("the member to clear against its neighbour", || node.cleared(&client)).await;
    }
}

/// A member restored from a backup that lacks writes it made afterwards: it is
/// marked `restored` by the restore, its first contact with a peer reads back the
/// writes it made and no longer holds (the peers hold them and no ordinary pull
/// would serve them), and it clears once it holds what its peers hold. The whole
/// path through real processes: the backup, the stop, the restore command, the
/// start, and the replay.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_member_restored_from_a_backup_reads_back_the_writes_it_made_after_it() {
    restore_and_read_back(false).await;
}

/// The same restore, but the restored member cannot dial either peer
/// (`KIMMY_TEST_REFUSE_DIAL` names the one peer that holds the lost writes; the other is down), as behind a filter that
/// drops what it opens and lets in what the peers open. Its only road to the writes
/// it lost is the peers' own contacts, which serve the replay (ADR-212's addendum).
/// The switch is armed before the first tick, and the counters assert the way: pages
/// applied inbound, none outbound.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_restored_member_that_cannot_dial_reads_its_writes_back_over_its_peers_contacts() {
    restore_and_read_back(true).await;
}

/// `kimmy_replay_total{via,result}` on a member.
async fn replay_count(client: &reqwest::Client, node: &Node, via: &str, result: &str) -> u64 {
    let series = format!("kimmy_replay_total{{via=\"{via}\",result=\"{result}\"}} ");
    let body = client.get(node.url("/metrics")).send().await.unwrap().text().await.unwrap();
    body.lines()
        .find_map(|line| line.strip_prefix(&series))
        .and_then(|n| n.trim().parse().ok())
        .expect("the replay series is always rendered")
}

/// Restore member A from a backup that lacks its later writes, start it (refusing
/// to dial its peers when `refuse_dial`), and check it reads them all back and
/// clears.
async fn restore_and_read_back(refuse_dial: bool) {
    let client = reqwest::Client::new();
    let (mut a, mut b, mut c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let token = a.login(&client).await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    let insert = |id: i64| {
        let (client, url, token) =
            (client.clone(), a.url("/v1/db/shop/coll/orders/docs"), token.clone());
        async move {
            let res = client
                .post(url)
                .bearer_auth(token)
                .json(&serde_json::json!({ "_id": id }))
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200);
        }
    };
    for id in 1..=3 {
        insert(id).await;
    }

    // The backup, then writes it will not have.
    let backup = client.get(a.url("/v1/admin/backup")).bearer_auth(&token).send().await.unwrap();
    assert_eq!(backup.status(), 200);
    let backup_path = a.dir.path().join("backup.bin");
    std::fs::write(&backup_path, backup.bytes().await.unwrap()).unwrap();
    // With the dial refused, one peer is down for the later writes and returns knowing nothing of them, so the one that
    // is up and holds them is the one the member cannot dial.
    if refuse_dial {
        b.signal("TERM");
        b.wait_exit(Duration::from_secs(60));
    }
    for id in 4..=8 {
        insert(id).await;
    }
    // Held by a peer before the member is lost.
    let holders = if refuse_dial { vec![&c] } else { vec![&b, &c] };
    for node in holders {
        let token = node.login(&client).await;
        eventually("a peer to hold the later writes", || {
            let (client, url, token) =
                (client.clone(), node.url("/v1/db/shop/coll/orders/docs/8"), token.clone());
            async move { client.get(url).bearer_auth(&token).send().await.unwrap().status() == 200 }
        })
        .await;
    }

    // The down peer returns, unable to dial the one that holds the writes, so it
    // stays as ignorant as it was and the member can reach it but learns nothing there.
    if refuse_dial {
        // The returning peer dials neither the holder nor the member (still running),
        // and the holder does not dial it, so nothing reaches it (a push would).
        let b_refuses = format!("127.0.0.1:{},127.0.0.1:{}", c.cluster, a.cluster);
        b.restart_with(&[("KIMMY_TEST_REFUSE_DIAL", &b_refuses)]);
        c.restart_with(&[("KIMMY_TEST_REFUSE_DIAL", &format!("127.0.0.1:{}", b.cluster))]);
        b.wait_ready(&client).await;
        c.wait_ready(&client).await;
    }

    // The member is stopped, its data replaced by the restore, and started again.
    a.signal("TERM");
    a.wait_exit(Duration::from_secs(60));
    std::fs::remove_dir_all(a.dir.path().join("data")).unwrap();
    let restored = Command::new(env!("CARGO_BIN_EXE_kimmyd"))
        .arg("--config")
        .arg(a.dir.path().join("kimmy.toml"))
        .arg("restore")
        .arg("--from")
        .arg(&backup_path)
        .env("KIMMY_ROOT_PASSWORD", ROOT_PASSWORD)
        .output()
        .expect("running kimmyd restore");
    assert!(restored.status.success(), "{}", String::from_utf8_lossy(&restored.stderr));
    let refused = format!("127.0.0.1:{}", c.cluster);
    if refuse_dial {
        a.restart_with(&[("KIMMY_TEST_REFUSE_DIAL", &refused)]);
    } else {
        a.restart_with(&[]);
    }
    a.wait_ready(&client).await;

    // It reads back what it made and lost, and says so.
    let token = a.login(&client).await;
    for id in 1..=8 {
        eventually("the restored member to hold every write", || {
            let (client, url, token) = (
                client.clone(),
                a.url(&format!("/v1/db/shop/coll/orders/docs/{id}")),
                token.clone(),
            );
            async move { client.get(url).bearer_auth(&token).send().await.unwrap().status() == 200 }
        })
        .await;
    }
    eventually("the marker to clear", || a.cleared(&client)).await;
    assert!(
        a.log().contains("the peer holds writes this member made and no longer holds"),
        "the replay says what it found: {}",
        a.log()
    );
    if refuse_dial {
        assert!(
            replay_count(&client, &a, "inbound", "applied").await > 0,
            "the replay came over the peers' contact"
        );
        assert_eq!(
            replay_count(&client, &a, "outbound", "applied").await,
            0,
            "this member dialled nobody that held its writes"
        );
    }
}

/// Pulls the integer value out of a `key=123` field in a log line, such as
/// `pulls=12` in a "merged from peer" line.
fn parse_field(line: &str, key: &str) -> Option<usize> {
    let after = line.split_once(key)?.1;
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// Drops ANSI SGR escapes (`\x1b[...m`), such as the pretty log format's
/// per-token coloring, leaving the plain text a field parser like
/// [`parse_field`] can split on.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for nc in chars.by_ref() {
                if nc == 'm' {
                    break;
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

// ---------------------------------------------------------------------------
// The divergence repair, on real processes (repair design sections 15 and 6)
// ---------------------------------------------------------------------------
//
// These need a member that loses one replicated entry, which only the `lab-hooks` build of
// `kimmyd` can do (`KIMMY_TEST_DROP_APPLY`, absent from every shipped build: the release check is
// `strings kimmyd | grep -c DROP_APPLY` reading 0). They are compiled only with that feature, so
// the default harness run does not see them, and CI runs them as a step of their own:
//
// ```text
// cargo test -p kimmyd --features lab-hooks --test cluster -- --ignored --test-threads=1 lab_hook_
// ```

/// Everything the three repair cases share: three members, B restarted with the hook set,
/// the collection and its first documents written through A.
#[cfg(feature = "lab-hooks")]
async fn lab_hook_cluster(client: &reqwest::Client, hook: &str) -> (Node, Node, Node, String) {
    let (a, mut b, c) = three_nodes(client).await;
    eventually("gossip to form", || all_report(client, vec![&a, &b, &c], 2)).await;
    // The restart must set the variable deliberately: a restart that keeps it re-arms it.
    b.restart_with(&[("KIMMY_TEST_DROP_APPLY", hook)]);
    b.wait_ready(client).await;
    eventually("gossip to re-form", || all_report(client, vec![&a, &b, &c], 2)).await;
    let token = a.login(client).await;
    (a, b, c, token)
}

#[cfg(feature = "lab-hooks")]
async fn lab_hook_scrape(client: &reqwest::Client, node: &Node, series: &str) -> u64 {
    node.gauge(client, series).await.unwrap_or(0)
}

/// A document B holds as a number, or `None` while it does not hold it.
#[cfg(feature = "lab-hooks")]
async fn lab_hook_version(
    client: &reqwest::Client,
    node: &Node,
    token: &str,
    coll: &str,
    id: i64,
) -> Option<i64> {
    let res = client
        .get(node.url(&format!("/v1/db/shop/coll/{coll}/docs/{id}")))
        .bearer_auth(token)
        .send()
        .await
        .ok()?;
    if res.status() != 200 {
        return None;
    }
    let body: serde_json::Value = res.json().await.ok()?;
    body["v"].as_i64().or_else(|| body["document"]["v"].as_i64())
}

/// B loses the replicated update of a document it holds: the counts agree, the cut digest finds
/// it, and the document repair applies the newer row. Judged effective at the next compared
/// digest, and the finding clears on every member.
#[cfg(feature = "lab-hooks")]
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn lab_hook_a_lost_update_is_repaired_by_the_document_repair() {
    let client = reqwest::Client::new();
    // The third document entry applied to shop.lost on B: two inserts, then the replace.
    let (a, b, c, token) = lab_hook_cluster(&client, "shop.lost:3").await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "lost" }))
        .send()
        .await
        .unwrap();
    for id in [1, 2] {
        client
            .post(a.url("/v1/db/shop/coll/lost/docs"))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "_id": id, "v": 0 }))
            .send()
            .await
            .unwrap();
    }
    let b_token = b.login(&client).await;
    eventually("both documents to reach B", || {
        let (client, b, b_token) = (&client, &b, &b_token);
        async move {
            lab_hook_version(client, b, b_token, "lost", 1).await.is_some()
                && lab_hook_version(client, b, b_token, "lost", 2).await.is_some()
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let put = client
        .put(a.url("/v1/db/shop/coll/lost/docs/1"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "v": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 200, "{:?}", put.text().await);
    eventually("the hook to lose the update on B", || {
        let b = &b;
        async move { b.log().contains("KIMMY_TEST_DROP_APPLY: lost a replicated entry") }
    })
    .await;

    // The repair brings the update to B, and B's loop judged it effective.
    let c_token = c.login(&client).await;
    eventually("B to hold the update", || {
        let (client, b, b_token) = (&client, &b, &b_token);
        async move { lab_hook_version(client, b, b_token, "lost", 1).await == Some(1) }
    })
    .await;
    assert_eq!(lab_hook_version(&client, &c, &c_token, "lost", 1).await, Some(1));
    eventually("the repair to be judged effective and the finding to clear", || {
        let (client, b) = (&client, &b);
        async move {
            lab_hook_scrape(
                client,
                b,
                "kimmy_sync_digest_repair_judgements_total{result=\"effective\"}",
            )
            .await
                >= 1
                && b.gauge(client, "kimmy_sync_digest_divergent_collections").await == Some(0)
        }
    })
    .await;
    assert!(
        lab_hook_scrape(&client, &b, "kimmy_sync_digest_repairs_total{outcome=\"applied\"}").await
            >= 1
    );
    assert_eq!(b.gauge(&client, "kimmy_sync_divergence_repair_enabled").await, Some(1));
    // And A and C, which saw the finding against B and cannot repair B's side, stop reporting it
    // at their next compared digest.
    eventually("A and C to stop reporting the finding", || {
        let (client, a, c) = (&client, &a, &c);
        async move {
            a.gauge(client, "kimmy_sync_digest_divergent_collections").await == Some(0)
                && c.gauge(client, "kimmy_sync_digest_divergent_collections").await == Some(0)
        }
    })
    .await;
}

/// B loses the creation of a plain index: the definition digest finds it and the definition
/// repair builds it, once, from the first page of A's snapshot.
#[cfg(feature = "lab-hooks")]
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn lab_hook_a_lost_plain_index_is_built_by_the_definition_repair() {
    let client = reqwest::Client::new();
    let (a, b, _c, token) = lab_hook_cluster(&client, "shop.idx:1:create_index").await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "idx" }))
        .send()
        .await
        .unwrap();
    client
        .post(a.url("/v1/db/shop/coll/idx/docs"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "_id": 1, "v": 3 }))
        .send()
        .await
        .unwrap();
    eventually("B to hold the collection", || {
        let (client, b) = (&client, &b);
        async move {
            let token = b.login(client).await;
            lab_hook_version(client, b, &token, "idx", 1).await.is_some()
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let created = client
        .post(a.url("/v1/db/shop/coll/idx/indexes"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "by_v", "fields": [{ "path": "v" }] }))
        .send()
        .await
        .unwrap();
    assert!(created.status().is_success(), "{:?}", created.text().await);
    eventually("the hook to lose the index on B", || {
        let b = &b;
        async move { b.log().contains("KIMMY_TEST_DROP_APPLY: lost a replicated entry") }
    })
    .await;
    let b_token = b.login(&client).await;
    eventually("B to build the index", || {
        let (client, b, b_token) = (&client, &b, &b_token);
        async move {
            let Ok(res) =
                client.get(b.url("/v1/db/shop/coll/idx/indexes")).bearer_auth(b_token).send().await
            else {
                return false;
            };
            let Ok(body) = res.json::<serde_json::Value>().await else {
                return false;
            };
            body["indexes"].as_array().is_some_and(|i| i.iter().any(|i| i["name"] == "by_v"))
        }
    })
    .await;
    eventually("the repair to be judged effective", || {
        let (client, b) = (&client, &b);
        async move {
            lab_hook_scrape(
                client,
                b,
                "kimmy_sync_definition_repair_judgements_total{result=\"effective\"}",
            )
            .await
                >= 1
        }
    })
    .await;
    assert_eq!(
        lab_hook_scrape(
            &client,
            &b,
            "kimmy_sync_definition_repair_indexes_total{result=\"built\"}"
        )
        .await,
        1
    );
    // B holds a finding against each peer; the one the repair did not come from clears at that
    // peer's next compared probe, so the gauge reaches 0 a contact later, not at the judgement.
    eventually("B's definition findings to clear", || {
        let (client, b) = (&client, &b);
        async move { b.gauge(client, "kimmy_sync_definition_divergent_collections").await == Some(0) }
    })
    .await;
}

/// B loses the replicated insert of a document: either half repairs it (the count half's
/// replay, or the document repair's young-stamp rule), and which one is recorded. The pass line
/// is that B ends holding the document and no finding stands.
#[cfg(feature = "lab-hooks")]
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn lab_hook_a_lost_insert_is_repaired_by_either_half() {
    let client = reqwest::Client::new();
    let (a, b, _c, token) = lab_hook_cluster(&client, "shop.ins:2:insert").await;
    client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "ins" }))
        .send()
        .await
        .unwrap();
    for id in [1, 2, 3] {
        client
            .post(a.url("/v1/db/shop/coll/ins/docs"))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "_id": id, "v": id }))
            .send()
            .await
            .unwrap();
    }
    eventually("the hook to lose the insert on B", || {
        let b = &b;
        async move { b.log().contains("KIMMY_TEST_DROP_APPLY: lost a replicated entry") }
    })
    .await;
    let b_token = b.login(&client).await;
    eventually("B to hold all three documents", || {
        let (client, b, b_token) = (&client, &b, &b_token);
        async move {
            for id in [1, 2, 3] {
                if lab_hook_version(client, b, b_token, "ins", id).await.is_none() {
                    return false;
                }
            }
            true
        }
    })
    .await;
    eventually("the finding to clear on B", || {
        let (client, b) = (&client, &b);
        async move { b.gauge(client, "kimmy_sync_divergent_collections").await == Some(0) }
    })
    .await;
    let count_half = lab_hook_scrape(&client, &b, "kimmy_sync_repair_rounds_total").await;
    let young =
        lab_hook_scrape(&client, &b, "kimmy_sync_digest_repair_rows_total{result=\"created\"}")
            .await;
    eprintln!("lost insert repaired: count-half rounds {count_half}, young rows created {young}");
}

/// The `Idempotency-Key` gate on a live cluster (ADR-240). A sets
/// `cluster.idempotency_keys`; B keeps the clustered default, `false`. B refuses a
/// keyed write `400`, naming the switch, counts it `gated`, and does not list
/// `idempotency-keys`; A takes a key once it has heard B say it carries records
/// (both run this build), lists the capability, and publishes its own window,
/// `storage.oplog_retention_secs`. The switch, the facts block the net reads, the
/// gate and the member set it is handed are the daemon's own, end to end.
#[tokio::test]
#[ignore = "boots a real two-node cluster; run with --ignored"]
async fn the_idempotency_gate_takes_keys_only_where_the_switch_is_set_and_every_member_carries_them()
 {
    const KEY: &str = "\"6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90\"";
    let client = reqwest::Client::new();
    let (pa, pb) = (ports::choose(), ports::choose());
    let a = Node::spawn_configured(
        "node-a",
        pa,
        &[pb],
        "oplog_retention_secs = 7200",
        1,
        "idempotency_keys = true",
    );
    let b = Node::spawn_configured("node-b", pb, &[pa], "", 1, "");
    for node in [&a, &b] {
        node.wait_ready(&client).await;
    }
    eventually("gossip to form", || all_report(&client, vec![&a, &b], 1)).await;
    let (ta, tb) = (a.login(&client).await, b.login(&client).await);
    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&ta)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    eventually("the collection on B", || async {
        client
            .get(b.url("/v1/db/shop/coll/orders/indexes"))
            .bearer_auth(&tb)
            .send()
            .await
            .is_ok_and(|res| res.status().is_success())
    })
    .await;
    let capabilities = |node: &Node| {
        let url = node.url("/v1/version");
        let client = &client;
        async move {
            let version: serde_json::Value =
                client.get(url).send().await.unwrap().json().await.unwrap();
            version
        }
    };

    // B's switch is unset, so a clustered B refuses whatever its peers carry.
    let refused = client
        .post(b.url("/v1/db/shop/coll/orders/docs"))
        .bearer_auth(&tb)
        .header("Idempotency-Key", KEY)
        .json(&serde_json::json!({ "n": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 400);
    let text = refused.text().await.unwrap();
    assert!(text.contains("cluster.idempotency_keys"), "the refusal names the switch: {text}");
    assert_eq!(
        b.gauge(&client, "kimmy_idempotency_requests_total{outcome=\"gated\"}").await,
        Some(1)
    );
    let version = capabilities(&b).await;
    assert!(!version["capabilities"].to_string().contains("idempotency-keys"), "{version}");
    assert_eq!(version["idempotencyWindowSecs"], 86_400);

    // A's switch is set: a key is taken once A has heard B say it carries records.
    eventually("A to take a key", || async {
        client
            .post(a.url("/v1/db/shop/coll/orders/docs"))
            .bearer_auth(&ta)
            .header("Idempotency-Key", KEY)
            .json(&serde_json::json!({ "n": 2 }))
            .send()
            .await
            .is_ok_and(|res| res.status().is_success())
    })
    .await;
    let version = capabilities(&a).await;
    assert!(version["capabilities"].to_string().contains("idempotency-keys"), "{version}");
    assert_eq!(version["idempotencyWindowSecs"], 7_200, "this member's own retention");
}

/// The `Decimal128` gate on a live cluster (ADR-238). A declares
/// `cluster.decimal128`; B and C keep the clustered default, `false`. A creates a
/// document whose `_id` is a `Decimal128` once every live member has said it can
/// hold one (all three run this build); B refuses the same creation `400`, naming
/// the key; the document reaches B and C all the same, since what replicates is
/// never gated, and the marker reads 1 on all three, on `/readyz` and on the
/// gauge. A partial index whose filter holds a `Decimal128` is the same: refused
/// on B, created on A, built on every member.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_decimal128_id_is_created_where_declared_and_applied_everywhere() {
    let client = reqwest::Client::new();
    let (pa, pb, pc) = (ports::choose(), ports::choose(), ports::choose());
    let a = Node::spawn_configured("node-a", pa, &[pb], "", 1, "decimal128 = true");
    let b = Node::spawn_configured("node-b", pb, &[pa], "", 1, "");
    let c = Node::spawn_configured("node-c", pc, &[pa], "", 1, "");
    for node in [&a, &b, &c] {
        node.wait_ready(&client).await;
    }
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    for node in [&a, &b, &c] {
        assert_eq!(node.gauge(&client, "kimmy_decimal128_id_present").await, Some(0));
    }

    let (ta, tb) = (a.login(&client).await, b.login(&client).await);
    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&ta)
        .json(&serde_json::json!({ "name": "prices" }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    let decimal_doc = serde_json::json!({ "_id": { "$numberDecimal": "1.5" }, "v": 1 });

    // B's key is unset, so a clustered B refuses whatever its peers can hold.
    eventually("the collection on B", || async {
        client
            .get(b.url("/v1/db/shop/coll/prices/indexes"))
            .bearer_auth(&tb)
            .send()
            .await
            .is_ok_and(|res| res.status().is_success())
    })
    .await;
    let refused = client
        .post(b.url("/v1/db/shop/coll/prices/docs"))
        .bearer_auth(&tb)
        .json(&decimal_doc)
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 400);
    let text = refused.text().await.unwrap();
    assert!(text.contains("cluster.decimal128"), "the refusal names the key: {text}");

    // A's key is set: accepted once A has heard every live member say it can.
    eventually("A to accept a decimal _id", || async {
        client
            .post(a.url("/v1/db/shop/coll/prices/docs"))
            .bearer_auth(&ta)
            .json(&decimal_doc)
            .send()
            .await
            .is_ok_and(|res| res.status().is_success())
    })
    .await;
    let index = serde_json::json!({
        "name": "positive",
        "fields": [{ "path": "v" }],
        "partialFilterExpression": { "v": { "$gt": { "$numberDecimal": "0" } } },
    });
    let refused = client
        .post(b.url("/v1/db/shop/coll/prices/indexes"))
        .bearer_auth(&tb)
        .json(&index)
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 400, "a decimal bound waits for B's key too");
    let built = client
        .post(a.url("/v1/db/shop/coll/prices/indexes"))
        .bearer_auth(&ta)
        .json(&index)
        .send()
        .await
        .unwrap();
    assert_eq!(built.status(), 200, "{}", built.text().await.unwrap());

    let tokens = [ta.clone(), tb.clone(), c.login(&client).await];
    let nodes = [&a, &b, &c];
    eventually("the document, its marker and the index on every member", || {
        let client = &client;
        let tokens = &tokens;
        async move {
            for (node, token) in nodes.iter().zip(tokens) {
                let ready: serde_json::Value = match client.get(node.url("/readyz")).send().await {
                    Ok(res) => res.json().await.unwrap_or_default(),
                    Err(_) => return false,
                };
                if ready["decimal128_id_present"] != true
                    || node.gauge(client, "kimmy_decimal128_id_present").await != Some(1)
                {
                    return false;
                }
                let indexes: serde_json::Value = match client
                    .get(node.url("/v1/db/shop/coll/prices/indexes"))
                    .bearer_auth(token)
                    .send()
                    .await
                {
                    Ok(res) => res.json().await.unwrap_or_default(),
                    Err(_) => return false,
                };
                if !indexes.to_string().contains("\"positive\"") {
                    return false;
                }
            }
            true
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// Idempotency keys across members (ADR-240)
// ---------------------------------------------------------------------------
//
// C1 to C9 over real processes and real HTTP, every member with
// `cluster.idempotency_keys = true`. The same rows run in process over real sockets in
// `kimmy-cluster/tests/idempotency.rs`; C4 (a hook between a keyed write's commit and anything
// after it) and C7 (the frames across a version boundary) are a storage and a codec test there.
// C9 and the race between chunks need the `lab-hooks` build (`KIMMY_TEST_STRIP_RECORDS`,
// `KIMMY_TEST_HOLD_BETWEEN_CHUNKS`), and are named `lab_hook_` for its CI step.

/// The `[cluster]` line every member of these tests carries.
const KEYED: &str = "idempotency_keys = true";

/// One keyed request's answer.
#[derive(Debug)]
struct KeyedAnswer {
    status: u16,
    /// `Idempotency-Replayed: ?1` on the answer.
    replayed: bool,
    text: String,
    body: serde_json::Value,
}

/// `body` sent to `node` with `Idempotency-Key: "<key>"`. The same `body` and `key` make
/// the same request, byte for byte, on any member.
///
/// Sent again, as a client must, while `node` answers `503 peer_unheard` (a live member not
/// heard from yet, after a restart, a pause or a rejoin): the same request with the same
/// key, until it is answered otherwise. A schedule that pauses or restarts members meets
/// that window by design; a `400` is never sent again.
async fn keyed_send(
    client: &reqwest::Client,
    node: &Node,
    token: &str,
    method: reqwest::Method,
    path: &str,
    body: &str,
    key: &str,
) -> KeyedAnswer {
    let deadline = std::time::Instant::now() + patience();
    loop {
        let res = client
            .request(method.clone(), node.url(path))
            .bearer_auth(token)
            .header("Idempotency-Key", format!("\"{key}\""))
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        let waited = res.status() == 503;
        let answer = keyed_answer(res).await;
        if !(waited && answer.body["error"] == "peer_unheard") {
            return answer;
        }
        assert_eq!(answer.body["retry"], "wait", "{}", answer.text);
        assert!(
            std::time::Instant::now() < deadline,
            "{} answered peer_unheard for {:?}: {}",
            node.name,
            patience(),
            answer.text
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn keyed_answer(res: reqwest::Response) -> KeyedAnswer {
    let status = res.status().as_u16();
    let replayed = match res.headers().get("idempotency-replayed") {
        Some(value) => {
            assert_eq!(value, "?1", "the marker's one value");
            true
        }
        None => false,
    };
    let text = res.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    KeyedAnswer { status, replayed, text, body }
}

/// Whether `node` lists `idempotency-keys`: its gate is open.
async fn takes_keys(client: &reqwest::Client, node: &Node) -> bool {
    let Ok(res) = client.get(node.url("/v1/version")).send().await else { return false };
    let version: serde_json::Value = res.json().await.unwrap_or_default();
    version["capabilities"].as_array().is_some_and(|c| c.iter().any(|c| c == "idempotency-keys"))
}

/// How many documents of `shop.orders` match `filter` on `node`, if it answers.
async fn orders_where(
    client: &reqwest::Client,
    node: &Node,
    token: &str,
    filter: serde_json::Value,
) -> Option<u64> {
    let res = client
        .post(node.url("/v1/db/shop/coll/orders/count"))
        .bearer_auth(token)
        .json(&serde_json::json!({ "filter": filter }))
        .send()
        .await
        .ok()?;
    if res.status() != 200 {
        return None;
    }
    res.json::<serde_json::Value>().await.ok()?["count"].as_u64()
}

/// The `n` of `shop.orders` document `id` on `node`, or `None` while it does not hold it.
async fn n_of(client: &reqwest::Client, node: &Node, token: &str, id: i64) -> Option<i64> {
    let res = client
        .get(node.url(&format!("/v1/db/shop/coll/orders/docs/{id}")))
        .bearer_auth(token)
        .send()
        .await
        .ok()?;
    if res.status() != 200 {
        return None;
    }
    let body: serde_json::Value = res.json().await.ok()?;
    body["n"].as_i64().or_else(|| body["document"]["n"].as_i64())
}

/// A counter from `/metrics`, 0 while the series is absent.
async fn series(client: &reqwest::Client, node: &Node, name: &str) -> u64 {
    node.gauge(client, name).await.unwrap_or(0)
}

const RECEIVED_PULL: &str = "kimmy_idempotency_records_received_total{via=\"pull\"}";
const RECEIVED_SNAPSHOT: &str = "kimmy_idempotency_records_received_total{via=\"snapshot\"}";
const PEER_REPLAYS: &str = "kimmy_idempotency_peer_replays_total";

/// Records `node` has stored from peers, by every way they come.
#[cfg(feature = "lab-hooks")]
async fn received(client: &reqwest::Client, node: &Node) -> u64 {
    let mut total = 0;
    for via in ["pull", "push", "replay", "snapshot", "repair"] {
        let name = format!("kimmy_idempotency_records_received_total{{via=\"{via}\"}}");
        total += series(client, node, &name).await;
    }
    total
}

/// Three members with `cluster.idempotency_keys` set and `storage` added, gossip formed,
/// `shop.orders` holding `docs` on every member, every member taking keys, and a token for
/// each, in the order A, B, C.
async fn keyed_cluster(
    client: &reqwest::Client,
    storage: &str,
    docs: serde_json::Value,
) -> (Node, Node, Node, [String; 3]) {
    keyed_cluster_at(client, storage, KEYED, 1, docs).await
}

/// [`keyed_cluster`], with `cluster` as every member's `[cluster]` lines (which must set
/// `idempotency_keys = true`) and every member at `sync_interval_secs`.
async fn keyed_cluster_at(
    client: &reqwest::Client,
    storage: &str,
    cluster: &str,
    sync_interval_secs: u64,
    docs: serde_json::Value,
) -> (Node, Node, Node, [String; 3]) {
    let (a, b, c) = three_nodes_configured_at(client, storage, cluster, sync_interval_secs).await;
    eventually("gossip to form", || all_report(client, vec![&a, &b, &c], 2)).await;
    let tokens = [a.login(client).await, b.login(client).await, c.login(client).await];
    let created = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&tokens[0])
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    let held = docs.as_array().map_or(0, Vec::len) as u64;
    if held > 0 {
        let seeded = client
            .post(a.url("/v1/db/shop/coll/orders/bulk"))
            .bearer_auth(&tokens[0])
            .json(&docs)
            .send()
            .await
            .unwrap();
        assert_eq!(seeded.status(), 200);
    }
    for (node, token) in [(&a, &tokens[0]), (&b, &tokens[1]), (&c, &tokens[2])] {
        eventually("every member to hold the collection and take keys", || async {
            orders_where(client, node, token, serde_json::json!({})).await == Some(held)
                && takes_keys(client, node).await
        })
        .await;
    }
    (a, b, c, tokens)
}

/// Until every member answers a retry of `body` under `key` from a record, with `expected`.
async fn every_member_replays(
    client: &reqwest::Client,
    nodes: [&Node; 3],
    tokens: &[String; 3],
    path: &str,
    body: &str,
    key: &str,
    expected: &str,
) {
    for (node, token) in nodes.iter().zip(tokens) {
        eventually("every member to replay the request with one answer", || async {
            let retry =
                keyed_send(client, node, token, reqwest::Method::POST, path, body, key).await;
            assert_eq!(retry.status, 200, "{}: {}", node.name, retry.text);
            retry.replayed && retry.text == expected
        })
        .await;
    }
}

/// C1. A keyed insert on A replicates to B, and the retry sent to B is answered from A's
/// record: `200` with A's body byte for byte and `Idempotency-Replayed: ?1`. B counts a peer
/// replay, and its pull counted the record it stored. One document, on every member.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn c1_a_retry_on_the_member_a_keyed_write_replicated_to_is_answered_from_its_record() {
    const PATH: &str = "/v1/db/shop/coll/orders/docs";
    const BODY: &str = r#"{"v":"c1"}"#;
    const KEY: &str = "c1-replicated-then-retried-on-b";
    let client = reqwest::Client::new();
    let (a, b, c, t) = keyed_cluster(&client, "", serde_json::json!([])).await;
    let pulled = series(&client, &b, RECEIVED_PULL).await;
    let peer = series(&client, &b, PEER_REPLAYS).await;

    let first = keyed_send(&client, &a, &t[0], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!(first.status, 200, "{}", first.text);
    assert!(!first.replayed, "a first answer carries no marker");
    eventually("B to hold the write", || async {
        orders_where(&client, &b, &t[1], serde_json::json!({ "v": "c1" })).await == Some(1)
    })
    .await;
    assert!(
        series(&client, &b, RECEIVED_PULL).await > pulled,
        "B's pull stored the record with the entry"
    );

    let retry = keyed_send(&client, &b, &t[1], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!(retry.status, 200, "{}", retry.text);
    assert!(retry.replayed, "the retry on B is a replay: {retry:?}");
    assert_eq!(retry.text, first.text, "A's answer, byte for byte");
    assert_eq!(series(&client, &b, PEER_REPLAYS).await, peer + 1, "counted as a peer's");
    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        eventually("one document on every member", || async {
            orders_where(&client, node, token, serde_json::json!({ "v": "c1" })).await == Some(1)
        })
        .await;
    }
}

/// C2. A commits a keyed insert and stops before any peer has it; the retry goes to B, which
/// holds no record and applies it, without the marker, under the key's derived `_id`. Once A
/// returns, every member holds one document, and the records converge to one: every member
/// answers a retry with B's answer, the later application's. The cut digest then compares
/// equal on every member.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn c2_a_retry_applied_while_the_first_member_was_stopped_converges_to_one_document() {
    const PATH: &str = "/v1/db/shop/coll/orders/docs";
    const BODY: &str = r#"{"v":"c2"}"#;
    const KEY: &str = "c2-a-stops-right-after-its-commit";
    let client = reqwest::Client::new();
    let (a, b, c, t) = keyed_cluster(&client, "", serde_json::json!([])).await;

    b.signal("STOP");
    c.signal("STOP");
    let first = keyed_send(&client, &a, &t[0], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!(first.status, 200, "{}", first.text);
    a.signal("STOP");
    b.signal("CONT");
    c.signal("CONT");
    let retry = keyed_send(&client, &b, &t[1], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!(retry.status, 200, "{}", retry.text);
    assert!(!retry.replayed, "B holds no record, and applies without the marker: {retry:?}");
    assert_eq!(retry.body["insertedId"], first.body["insertedId"], "the key's derived `_id`");
    assert_ne!(retry.text, first.text, "another application: its own stamp");
    a.signal("CONT");

    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        eventually("one document on every member", || async {
            orders_where(&client, node, token, serde_json::json!({ "v": "c2" })).await == Some(1)
        })
        .await;
    }
    every_member_replays(&client, [&a, &b, &c], &t, PATH, BODY, KEY, &retry.text).await;

    // The cut digest compares, on every member, and finds nothing.
    let compared = "kimmy_sync_divergence_digest_probes_total{outcome=\"compared\"}";
    let before = [
        series(&client, &a, compared).await,
        series(&client, &b, compared).await,
        series(&client, &c, compared).await,
    ];
    for (node, from) in [(&a, before[0]), (&b, before[1]), (&c, before[2])] {
        eventually("the cut digest to compare equal", || async {
            series(&client, node, compared).await >= from + 2
                && node.gauge(&client, "kimmy_sync_digest_divergent_collections").await == Some(0)
        })
        .await;
    }
}

/// C3. One keyed `$inc` applied on A and, before B has received it, on B, from the same
/// version: once each has the other's, every member holds base + 1, and answers a retry from
/// the one record.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn c3_one_keyed_inc_applied_on_two_members_ends_at_base_plus_one() {
    const PATH: &str = "/v1/db/shop/coll/orders/update";
    const BODY: &str = r#"{"filter":{"_id":1},"update":{"$inc":{"n":1}}}"#;
    const KEY: &str = "c3-one-inc-applied-on-two-members";
    let client = reqwest::Client::new();
    let (a, b, c, t) = keyed_cluster(&client, "", serde_json::json!([{ "_id": 1, "n": 0 }])).await;

    b.signal("STOP");
    c.signal("STOP");
    let on_a = keyed_send(&client, &a, &t[0], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!((on_a.status, on_a.replayed), (200, false), "{}", on_a.text);
    a.signal("STOP");
    b.signal("CONT");
    let on_b = keyed_send(&client, &b, &t[1], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!((on_b.status, on_b.replayed), (200, false), "B had not received A's: {}", on_b.text);
    a.signal("CONT");
    c.signal("CONT");

    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        eventually("base + 1 on every member", || async {
            n_of(&client, node, token, 1).await == Some(1)
        })
        .await;
    }
    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        eventually("every member to answer from the record", || async {
            let retry =
                keyed_send(&client, node, token, reqwest::Method::POST, PATH, BODY, KEY).await;
            retry.status == 200 && retry.replayed
        })
        .await;
        assert_eq!(n_of(&client, node, token, 1).await, Some(1), "{}: still base + 1", node.name);
    }
}

/// C5. B holds a newer, independent version of the document, so A's keyed `$inc` loses
/// there. B stores the record all the same, and a retry on B is answered from it rather than
/// applied after the version that beat it.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn c5_a_retry_is_answered_where_the_keyed_entry_lost() {
    const PATH: &str = "/v1/db/shop/coll/orders/update";
    const BODY: &str = r#"{"filter":{"_id":1},"update":{"$inc":{"n":1}}}"#;
    const KEY: &str = "c5-the-keyed-entry-loses-on-b";
    let client = reqwest::Client::new();
    let (a, b, c, t) = keyed_cluster(&client, "", serde_json::json!([{ "_id": 1, "n": 0 }])).await;

    b.signal("STOP");
    c.signal("STOP");
    let first = keyed_send(&client, &a, &t[0], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!((first.status, first.replayed), (200, false), "{}", first.text);
    a.signal("STOP");
    b.signal("CONT");
    let newer = client
        .put(b.url("/v1/db/shop/coll/orders/docs/1"))
        .bearer_auth(&t[1])
        .json(&serde_json::json!({ "n": 10 }))
        .send()
        .await
        .unwrap();
    assert_eq!(newer.status(), 200, "B's own, newer write");
    let pulled = series(&client, &b, RECEIVED_PULL).await;
    a.signal("CONT");
    c.signal("CONT");
    eventually("B to store A's record from its pull", || async {
        series(&client, &b, RECEIVED_PULL).await > pulled
    })
    .await;

    let retry = keyed_send(&client, &b, &t[1], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!(retry.status, 200, "{}", retry.text);
    assert!(retry.replayed, "answered from the record: {retry:?}");
    assert_eq!(retry.text, first.text);
    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        eventually("B's version on every member", || async {
            n_of(&client, node, token, 1).await == Some(10)
        })
        .await;
    }
}

/// C6. A member stopped while its peers collect the history it would need catches up by a
/// snapshot, and the snapshot carries the record of a keyed write made since: its snapshot
/// counter moves, and a retry on it is answered from the record.
///
/// One setting does two things here: the window that collects the history C would need,
/// which forces the snapshot, is also the record's window, which the keyed write starts
/// afresh after that collection. The record must still stand on the peer when C's snapshot
/// reads it, and must not be past its window on C when C stores it (C holds no record of the
/// key, and a record past its window is then refused). So the window is sized for the
/// margin: 60 s, of which C's catch-up takes a few, leaves a stall of C of half a minute or
/// more at its snapshot step (as a loaded scheduler causes) inside the record's life. At
/// 30 s a 32 s stall had A and B collect the record two seconds before C's snapshot landed,
/// and the poller below could never succeed.
///
/// C goes on only once B, as well as A, holds the keyed write and its record. C is behind
/// both peers' horizons, so whichever it asks first answers with a snapshot, and that
/// snapshot then carries the document and its record; nothing reaches C by a pull before it.
/// Sent on at once, C could take its snapshot from a B that had not yet pulled the write,
/// and then pull the write from A with its record (counted `via="pull"`): a real catch-up,
/// but not the one this test is about, and the snapshot counter would never move.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn c6_a_member_that_caught_up_by_snapshot_answers_a_retry_from_the_record() {
    const PATH: &str = "/v1/db/shop/coll/orders/docs";
    const BODY: &str = r#"{"v":"c6"}"#;
    const KEY: &str = "c6-retried-on-a-member-past-the-horizon";
    let client = reqwest::Client::new();
    let (a, b, c, t) = keyed_cluster(
        &client,
        "oplog_retention_secs = 60\ngc_interval_secs = 2",
        serde_json::json!([]),
    )
    .await;

    // Read before C stops: a stopped member answers nothing, a scrape included.
    let snapshots = series(&client, &c, RECEIVED_SNAPSHOT).await;
    c.signal("STOP");
    let docs: Vec<serde_json::Value> =
        (0..20).map(|i| serde_json::json!({ "_id": i, "n": 0 })).collect();
    let seeded = client
        .post(a.url("/v1/db/shop/coll/orders/bulk"))
        .bearer_auth(&t[0])
        .json(&docs)
        .send()
        .await
        .unwrap();
    assert_eq!(seeded.status(), 200);
    // The documents C has not seen, collected on A and on B. They were written at one
    // instant, after every other entry but a few, and expire together: once twenty entries
    // have left the oplog, they are among them. Each count is read as soon as its member
    // holds the documents: a slow runner can collect them before a later read, and the
    // premise is then lost to the clock and not to the code.
    let held_a = a.gauge(&client, "kimmy_oplog_entries").await.unwrap();
    let held_b = std::cell::Cell::new(0);
    eventually("B to hold the documents in its oplog", || async {
        held_b.set(b.gauge(&client, "kimmy_oplog_entries").await.unwrap_or(0));
        held_b.get() >= 20
    })
    .await;
    let held = [held_a, held_b.get()];
    assert!(held.iter().all(|n| *n >= 20), "premise: the documents are in the oplog: {held:?}");
    eventually("A and B to collect the history C would need", || async {
        a.gauge(&client, "kimmy_oplog_entries").await.is_some_and(|n| n + 20 <= held[0])
            && b.gauge(&client, "kimmy_oplog_entries").await.is_some_and(|n| n + 20 <= held[1])
    })
    .await;
    let first = keyed_send(&client, &a, &t[0], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!(first.status, 200, "{}", first.text);
    eventually("B to hold the keyed write and its record", || async {
        orders_where(&client, &b, &t[1], serde_json::json!({ "v": "c6" })).await == Some(1)
            && b.gauge(&client, "kimmy_idempotency_records").await.is_some_and(|n| n >= 1)
    })
    .await;
    c.signal("CONT");

    eventually("C to catch up by snapshot, with the record", || async {
        orders_where(&client, &c, &t[2], serde_json::json!({ "v": "c6" })).await == Some(1)
            && series(&client, &c, RECEIVED_SNAPSHOT).await > snapshots
    })
    .await;
    // A member back from a long stop has not yet heard its peers' blocks again, and its gate
    // refuses keys until it has (a live member not heard from counts as one that does not
    // carry records); a client branches on the capability per node, and so does this test.
    eventually("C to take keys again", || takes_keys(&client, &c)).await;
    let retry = keyed_send(&client, &c, &t[2], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!(retry.status, 200, "{}", retry.text);
    assert!(retry.replayed, "answered from the record the snapshot carried: {retry:?}");
    assert_eq!(retry.text, first.text);
    assert_eq!(orders_where(&client, &c, &t[2], serde_json::json!({})).await, Some(21));
}

/// C8. A key used in one life of a collection, the collection dropped and created again:
/// the key's record outlives the drop for its window (ADR-240, guarantee 7). A retry with
/// the old key, on every member, replays its first answer and writes nothing to the new
/// life; another request under the key is refused as a key reused, there too.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn c8_an_old_key_replays_after_a_drop_and_a_recreation() {
    const PATH: &str = "/v1/db/shop/coll/orders/docs";
    const BODY: &str = r#"{"_id":50,"v":"c8"}"#;
    const OTHER: &str = r#"{"_id":51,"v":"c8"}"#;
    const KEY: &str = "c8-a-key-from-the-life-before";
    let client = reqwest::Client::new();
    let (a, b, c, t) = keyed_cluster(&client, "", serde_json::json!([])).await;
    let first = keyed_send(&client, &a, &t[0], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!(first.status, 200, "{}", first.text);
    // A member holding the document holds its record: the record rides with the coverage
    // that brings the write.
    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        eventually("the write on every member", || async {
            orders_where(&client, node, token, serde_json::json!({ "v": "c8" })).await == Some(1)
        })
        .await;
    }

    let dropped =
        client.delete(a.url("/v1/db/shop/coll/orders")).bearer_auth(&t[0]).send().await.unwrap();
    assert_eq!(dropped.status(), 200);
    eventually("the name created again", || async {
        client
            .post(a.url("/v1/db/shop/collections"))
            .bearer_auth(&t[0])
            .json(&serde_json::json!({ "name": "orders" }))
            .send()
            .await
            .is_ok_and(|res| res.status() == 200)
    })
    .await;
    for (node, token) in [(&b, &t[1]), (&c, &t[2])] {
        eventually("the new, empty life on every member", || async {
            orders_where(&client, node, token, serde_json::json!({})).await == Some(0)
        })
        .await;
    }

    for (node, token) in [(&b, &t[1]), (&c, &t[2]), (&a, &t[0])] {
        let again = keyed_send(&client, node, token, reqwest::Method::POST, PATH, BODY, KEY).await;
        assert_eq!(again.status, 200, "{}: {}", node.name, again.text);
        assert!(again.replayed, "{}: the record outlives the drop: {again:?}", node.name);
        assert_eq!(again.text, first.text, "{}: the first answer", node.name);
        let other = keyed_send(&client, node, token, reqwest::Method::POST, PATH, OTHER, KEY).await;
        assert_eq!(other.status, 422, "{}: {}", node.name, other.text);
        assert_eq!(other.body["error"], "idempotency_key_reused", "{}", node.name);
    }
    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        assert_eq!(
            orders_where(&client, node, token, serde_json::json!({})).await,
            Some(0),
            "{}: nothing written to the new life",
            node.name
        );
    }
}

/// C9, the mixed-version test. C stands in for a release before 0.52: restarted with
/// `KIMMY_TEST_STRIP_RECORDS`, it serves every frame and page without records, and its block
/// omits `idempotency_records`. A and B have `cluster.idempotency_keys` set, so their refusal
/// is the live-peer net's:
///
/// 1. while C is live, a keyed write on A or on B is refused `400`, naming C, and neither
///    lists `idempotency-keys`. (C's own gate sees A and B carry records, and a keyed write C
///    takes reaches A and B without its record: what an older relay does.)
/// 2. without the net (a mutant), the schedule below would end at base + 2;
/// 3. once C runs this build again, the schedule is answered from the record on B, and ends
///    at base + 1: a keyed `$inc` on A; C pulls it from A; B pulls from C before it pulls
///    from A; A stops; the retry lands on B.
#[cfg(feature = "lab-hooks")]
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn lab_hook_c9_a_live_member_that_does_not_carry_records_closes_the_gate() {
    const PATH: &str = "/v1/db/shop/coll/orders/update";
    const BODY: &str = r#"{"filter":{"_id":1},"update":{"$inc":{"n":1}}}"#;
    let client = reqwest::Client::new();
    let (a, b, mut c, mut t) =
        keyed_cluster(&client, "", serde_json::json!([{ "_id": 1, "n": 0 }])).await;
    let c_id = c.node_id(&client).await;

    // 1. C as an older release.
    c.restart_with(&[("KIMMY_TEST_STRIP_RECORDS", "1")]);
    c.wait_ready(&client).await;
    eventually("gossip to re-form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    t[2] = c.login(&client).await;
    // A peer restarted keeps its last block on its peers until they see it renamed or
    // hear its new one, so the gate is first seen closed; no keyed write is sent before.
    eventually("A and B to close their gates", || async {
        !takes_keys(&client, &a).await && !takes_keys(&client, &b).await
    })
    .await;
    // Until A and B hold C's new block, C is live and not heard from: the transient
    // `503 peer_unheard`, which `keyed_send` sends again through. Once they hold it, the
    // block that says no is the `400`, which lasts.
    for (node, token) in [(&a, &t[0]), (&b, &t[1])] {
        let refused = keyed_send(
            &client,
            node,
            token,
            reqwest::Method::POST,
            PATH,
            BODY,
            "c9-refused-while-c-is-live",
        )
        .await;
        assert_eq!(refused.status, 400, "{}: {}", node.name, refused.text);
        assert_eq!(refused.body["retry"], "no", "{}", refused.text);
        assert!(refused.text.contains(&c_id.to_string()), "names C: {}", refused.text);
        assert!(!takes_keys(&client, node).await, "{}", node.name);
    }
    let base = [received(&client, &a).await, received(&client, &b).await];
    let relayed = keyed_send(
        &client,
        &c,
        &t[2],
        reqwest::Method::POST,
        "/v1/db/shop/coll/orders/docs",
        r#"{"v":"c9-through-c"}"#,
        "c9-a-write-c-relays-without-its-record",
    )
    .await;
    assert_eq!(relayed.status, 200, "{}", relayed.text);
    for (node, token) in [(&a, &t[0]), (&b, &t[1])] {
        eventually("C's write on A and B", || async {
            orders_where(&client, node, token, serde_json::json!({ "v": "c9-through-c" })).await
                == Some(1)
        })
        .await;
    }
    assert_eq!(
        [received(&client, &a).await, received(&client, &b).await],
        base,
        "C sent its write without the record"
    );
    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        assert_eq!(n_of(&client, node, token, 1).await, Some(0), "nothing applied: base");
    }

    // 3. C runs this build again.
    c.restart_with(&[]);
    c.wait_ready(&client).await;
    eventually("gossip to re-form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    t[2] = c.login(&client).await;
    eventually("A and B to take keys again", || async {
        takes_keys(&client, &a).await && takes_keys(&client, &b).await
    })
    .await;
    const KEY: &str = "c9-the-schedule-once-c-carries-records";
    b.signal("STOP");
    let first = keyed_send(&client, &a, &t[0], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!((first.status, first.replayed), (200, false), "{}", first.text);
    eventually("C to pull it from A", || async { n_of(&client, &c, &t[2], 1).await == Some(1) })
        .await;
    a.signal("STOP");
    b.signal("CONT");
    eventually("B to pull it from C", || async { n_of(&client, &b, &t[1], 1).await == Some(1) })
        .await;
    // B may have rejoined after its pause, dropping A's block while A is stopped: A
    // is then not heard from, and past B's lease it counts as a member that does not
    // carry records, until SWIM declares it down. As the other tests do, the retry
    // waits for B to take keys.
    eventually("B to take keys with A stopped", || takes_keys(&client, &b)).await;
    let retry = keyed_send(&client, &b, &t[1], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!(retry.status, 200, "{}", retry.text);
    assert!(retry.replayed, "answered from the record C relayed: {retry:?}");
    assert_eq!(n_of(&client, &b, &t[1], 1).await, Some(1), "base + 1");
    a.signal("CONT");
    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        eventually("base + 1 on every member", || async {
            n_of(&client, node, token, 1).await == Some(1)
        })
        .await;
    }
}

/// One answer of a gated request in the window: its status, its `Retry-After`, and its body.
struct GateAnswer {
    status: u16,
    retry_after: Option<String>,
    replayed: bool,
    text: String,
    body: serde_json::Value,
}

/// `body` POSTed to `path` on `node`, with `Idempotency-Key: "<key>"` when `key` is given.
async fn gate_send(
    client: &reqwest::Client,
    node: &Node,
    token: &str,
    path: &str,
    body: &str,
    key: Option<&str>,
) -> GateAnswer {
    let mut request = client
        .post(node.url(path))
        .bearer_auth(token)
        .header("Content-Type", "application/json")
        .body(body.to_string());
    if let Some(key) = key {
        request = request.header("Idempotency-Key", format!("\"{key}\""));
    }
    let res = request.send().await.unwrap();
    let status = res.status().as_u16();
    let header = |name: &str| res.headers().get(name).map(|v| v.to_str().unwrap().to_string());
    let retry_after = header("retry-after");
    let replayed = header("idempotency-replayed").is_some();
    let text = res.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    GateAnswer { status, retry_after, replayed, text, body }
}

/// The gate window after a member restarts twice within a sync interval.
///
/// A restarted member is renamed on each peer exactly once, which drops the facts block the
/// peer held from the old process, and the peer then waits for the new process's block
/// before it takes a keyed or `Decimal128`-`_id` request (ADR-238, ADR-242). Since ADR-247
/// the peer asks for that block at once, so after a single restart the window is about one
/// contact wide and a test cannot hold it open. It stays open by design in one case: the
/// peer starts at most one exchange per member per sync interval, and an ask the interval
/// holds becomes one trailing ask at the interval's end. So when B is restarted a second
/// time within the sync interval of the first, A and C hold B's block dropped from the
/// second rename until that trailing ask.
///
/// B is restarted twice in a row (each time waited ready), and from the second `/readyz` 200
/// A and C are each sent a keyed insert and a `Decimal128`-`_id` insert, again and again,
/// until each is taken. While the peer has not heard B's block, each answer is `503
/// peer_unheard`, `retry: wait`, with `Retry-After: 1` (the sync interval is 5 s here, so
/// the window is wide) and **never `400`**: every member runs this build and says it
/// carries records and can hold a decimal. The keyed request sent again with its key then
/// gets its first application, once, and sent again after that its replay. Up to `RESTARTS`
/// double restarts, until the window has been met for both requests.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_restarted_member_answers_503_until_it_has_heard_its_peers_and_never_400() {
    const RESTARTS: usize = 10;
    const SYNC_SECS: u64 = 5;
    const PATH: &str = "/v1/db/shop/coll/orders/docs";
    let client = reqwest::Client::new();
    let cluster = format!("{KEYED}\ndecimal128 = true");
    let (a, mut b, c, t) =
        keyed_cluster_at(&client, "", &cluster, SYNC_SECS, serde_json::json!([])).await;
    let peers = [(&a, &t[0]), (&c, &t[2])];

    let (mut keyed_waited, mut decimal_waited) = (0, 0);
    let mut rounds = 0;
    let mut sent_bodies = Vec::new();
    for round in 0..RESTARTS {
        rounds += 1;
        // Twice in a row: the second rename lands within the sync interval of the ask the
        // first one made, so that ask's answer is the trailing one at the interval's end.
        for _ in 0..2 {
            b.restart_with(&[]);
            eventually("B to be ready again", || async {
                client.get(b.url("/readyz")).send().await.is_ok_and(|r| r.status() == 200)
            })
            .await;
        }
        // Per peer: the keyed request (its key, its body, its first answer), whether the
        // decimal one is taken.
        let mut keyed: Vec<(String, String, Option<String>)> = Vec::new();
        for index in 0..peers.len() {
            let key = format!("gate-window-{round:02}-{index}-6f1c0e52-8a1d");
            let body = format!(r#"{{"v":"gate-window-{round}-{index}"}}"#);
            sent_bodies.push(body.clone());
            keyed.push((key, body, None));
        }
        let decimals: Vec<String> = (0..peers.len())
            .map(|index| format!(r#"{{"_id":{{"$numberDecimal":"{round}.{index}5"}}}}"#))
            .collect();
        let mut decimal_done = vec![false; peers.len()];
        let deadline = std::time::Instant::now() + patience();
        while keyed.iter().any(|k| k.2.is_none()) || decimal_done.contains(&false) {
            assert!(
                std::time::Instant::now() < deadline,
                "round {round}: B's peers never took both"
            );
            for (index, (node, token)) in peers.iter().enumerate() {
                let mut probes = Vec::new();
                if keyed[index].2.is_none() {
                    probes.push((true, keyed[index].1.clone(), Some(keyed[index].0.clone())));
                }
                if !decimal_done[index] {
                    probes.push((false, decimals[index].clone(), None));
                }
                for (is_keyed, sent, key) in probes {
                    let answer = gate_send(&client, node, token, PATH, &sent, key.as_deref()).await;
                    match answer.status {
                        200 if is_keyed => {
                            assert!(!answer.replayed, "round {round}: the first application");
                            keyed[index].2 = Some(answer.text);
                        }
                        200 => decimal_done[index] = true,
                        503 => {
                            assert_eq!(answer.body["error"], "peer_unheard", "{}", answer.text);
                            assert_eq!(answer.body["retry"], "wait", "{}", answer.text);
                            assert_eq!(
                                answer.retry_after.as_deref(),
                                Some("1"),
                                "a fixed second: {}",
                                answer.text
                            );
                            assert!(answer.text.contains("transient"), "{}", answer.text);
                            if is_keyed {
                                keyed_waited += 1;
                            } else {
                                decimal_waited += 1;
                            }
                        }
                        other => panic!(
                            "round {round}: {} answered {other}, never 400 nor anything but \
                             200 or 503 peer_unheard: {}",
                            node.name, answer.text
                        ),
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        for (index, (node, token)) in peers.iter().enumerate() {
            let (key, body, first) = &keyed[index];
            let again = gate_send(&client, node, token, PATH, body, Some(key)).await;
            assert_eq!(again.status, 200, "{}", again.text);
            assert!(again.replayed, "round {round}: sent again, the replay: {}", again.text);
            assert_eq!(Some(again.text), *first, "round {round}: the first answer");
        }
        if keyed_waited > 0 && decimal_waited > 0 {
            break;
        }
    }
    eprintln!(
        "gate window: {rounds} double restart(s), {keyed_waited} keyed and {decimal_waited} \
         decimal answers 503 peer_unheard"
    );
    assert!(
        keyed_waited > 0 && decimal_waited > 0,
        "no double restart in {RESTARTS} met the window (keyed {keyed_waited}, decimal \
         {decimal_waited}): the test proved nothing"
    );
    // Each keyed insert landed exactly once, wherever it was answered from.
    for body in &sent_bodies {
        let filter: serde_json::Value = serde_json::from_str(body).unwrap();
        eventually("each keyed insert to be held once on B", || async {
            orders_where(&client, &b, &t[1], filter.clone()).await == Some(1)
        })
        .await;
    }
}

/// A frozen peer is declared down before its unheard lease can turn the `503` into a `400`
/// (the lease's floor: SWIM's suspect-to-down time plus one sync interval). At a 1 s sync
/// interval the lease alone is 3 s, shorter than SWIM takes. B is stopped until A and C
/// declare it down, A is then stopped, and B is resumed: B rejoins, which drops every
/// block it held, so A is live and not heard from at B. From then on B is sent a keyed
/// insert again and again: every answer is `503 peer_unheard` until B takes the key, and
/// never `400` while A is live; B takes it once SWIM has declared A down.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_frozen_peer_is_declared_down_before_its_unheard_lease_runs_out() {
    const PATH: &str = "/v1/db/shop/coll/orders/docs";
    const BODY: &str = r#"{"v":"frozen-peer"}"#;
    const KEY: &str = "frozen-peer-is-declared-down-first";
    let client = reqwest::Client::new();
    let (a, b, c, t) = keyed_cluster(&client, "", serde_json::json!([])).await;

    let a_id = a.node_id(&client).await;
    let rejoins = |node: &Node| node.log().matches("rejoined the cluster").count();
    let before = rejoins(&b);
    b.signal("STOP");
    eventually("A and C to declare B down", || all_report(&client, vec![&a, &c], 1)).await;
    a.signal("STOP");
    let frozen = std::time::Instant::now();
    b.signal("CONT");
    eventually("B to rejoin", || async { rejoins(&b) > before }).await;
    let rejoined = frozen.elapsed();

    let deadline = std::time::Instant::now() + patience();
    let mut waited = 0;
    let taken = loop {
        assert!(std::time::Instant::now() < deadline, "B never took the key");
        let answer = gate_send(&client, &b, &t[1], PATH, BODY, Some(KEY)).await;
        match answer.status {
            200 => break answer,
            503 => {
                assert_eq!(answer.body["error"], "peer_unheard", "{}", answer.text);
                assert_eq!(answer.retry_after.as_deref(), Some("1"), "{}", answer.text);
                waited += 1;
            }
            other => panic!(
                "B answered {other} {:?} after A froze ({rejoined:?} at B's rejoin), with {:?} \
                 peers live at B, naming A: {}: {}",
                frozen.elapsed(),
                b.members_gauge(&client).await,
                answer.text.contains(&a_id.to_string()),
                answer.text
            ),
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert!(!taken.replayed, "{}", taken.text);
    assert!(waited > 0, "B rejoined and met the window: it answered 503 first");
    assert_eq!(b.members_gauge(&client).await, Some(1), "B took the key once A was declared down");
    eprintln!(
        "frozen peer: rejoined {rejoined:?} after A froze; {waited} answers 503 peer_unheard, \
         then 200 at {:?} with A down",
        frozen.elapsed()
    );
    a.signal("CONT");
}

/// The stamp rule on a fresh attempt (Chris's ruling, 2026-10-17): a keyed
/// `multi` `$inc` on A, two documents a chunk, held between its first and second chunk
/// (`KIMMY_TEST_HOLD_BETWEEN_CHUNKS`). Meanwhile B writes document 5, which A pulls, and a
/// client writes document 6 on A. Both are left alone and counted in `skipped`, the answer
/// says nothing of `resumed`, and after quiescence every member holds the four increments and
/// the two unrelated writes; a retry on B is answered from the record.
#[cfg(feature = "lab-hooks")]
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn lab_hook_a_fresh_keyed_multi_update_skips_what_others_wrote_between_its_chunks() {
    const PATH: &str = "/v1/db/shop/coll/orders/update";
    const BODY: &str = r#"{"filter":{},"update":{"$inc":{"n":1}},"multi":true}"#;
    const KEY: &str = "race-a-fresh-multi-chunk-request";
    let client = reqwest::Client::new();
    let docs: Vec<serde_json::Value> =
        (1..=6).map(|i| serde_json::json!({ "_id": i, "n": 0 })).collect();
    let (mut a, b, c, mut t) =
        keyed_cluster(&client, "multi_chunk_docs = 2", serde_json::json!(docs)).await;
    let release = a.dir.path().join("release-the-held-request");
    a.restart_with(&[("KIMMY_TEST_HOLD_BETWEEN_CHUNKS", release.to_str().unwrap())]);
    a.wait_ready(&client).await;
    eventually("gossip to re-form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    t[0] = a.login(&client).await;
    eventually("A to take keys", || takes_keys(&client, &a)).await;

    let held = tokio::spawn({
        let request = client
            .post(a.url(PATH))
            .bearer_auth(&t[0])
            .header("Idempotency-Key", format!("\"{KEY}\""))
            .header("Content-Type", "application/json")
            .body(BODY);
        async move { keyed_answer(request.send().await.unwrap()).await }
    });
    eventually("the request to be held between its chunks", || async {
        a.log().contains("holding a request between two chunks")
    })
    .await;
    let on_b = client
        .put(b.url("/v1/db/shop/coll/orders/docs/5"))
        .bearer_auth(&t[1])
        .json(&serde_json::json!({ "n": 100 }))
        .send()
        .await
        .unwrap();
    assert_eq!(on_b.status(), 200);
    eventually("A to take B's write", || async { n_of(&client, &a, &t[0], 5).await == Some(100) })
        .await;
    let on_a = client
        .put(a.url("/v1/db/shop/coll/orders/docs/6"))
        .bearer_auth(&t[0])
        .json(&serde_json::json!({ "n": 200 }))
        .send()
        .await
        .unwrap();
    assert_eq!(on_a.status(), 200);
    std::fs::write(&release, b"").unwrap();

    let answer = held.await.unwrap();
    assert_eq!((answer.status, answer.replayed), (200, false), "{}", answer.text);
    assert_eq!(answer.body["modified"], 4, "{}", answer.text);
    assert_eq!(answer.body["skipped"], 2, "{}", answer.text);
    assert!(answer.body.get("resumed").is_none(), "a fresh attempt: {}", answer.text);

    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        eventually("every member to settle on the same documents", || async {
            let mut held = Vec::new();
            for id in 1..=6 {
                held.push(n_of(&client, node, token, id).await);
            }
            held == [Some(1), Some(1), Some(1), Some(1), Some(100), Some(200)]
        })
        .await;
    }
    eventually("B to answer a retry from the record", || async {
        let retry = keyed_send(&client, &b, &t[1], reqwest::Method::POST, PATH, BODY, KEY).await;
        retry.status == 200 && retry.replayed && retry.text == answer.text
    })
    .await;
    assert_eq!(n_of(&client, &b, &t[1], 1).await, Some(1), "not applied again");
}

/// A cross-origin relay while the origin is down, over real processes: a keyed request's
/// effect reaches a member under another origin, from a relay that cannot vouch for the
/// request's own stamp, while the request's origin and the member that wrote on top of it
/// are down. M dials only X throughout (`KIMMY_TEST_REFUSE_DIAL` names Y and Z). Y's keyed
/// `$inc` reaches Z; X, cut off from both, writes the document later; Z writes on top of
/// the `$inc`. X then pulls Z: the `$inc` loses to X's write (stored, not appended, so X's
/// vector stays below it) and Z's version wins. With Y and Z down, M takes Z's version from
/// X, which holds the `$inc`; the record comes with it, as one X cannot vouch for by its
/// oplog, and a retry on M is answered from it.
#[tokio::test]
#[ignore = "boots a real four-node cluster; run with --ignored"]
async fn t2_a_cross_origin_relay_carries_the_record_while_its_origin_is_down() {
    const PATH: &str = "/v1/db/shop/coll/orders/update";
    const BODY: &str = r#"{"filter":{"_id":1},"update":{"$inc":{"n":1}}}"#;
    const KEY: &str = "t2-a-cross-origin-relay-while-y-and-z-are-down";
    let client = reqwest::Client::new();
    let (py, pz, px, pm) = (ports::choose(), ports::choose(), ports::choose(), ports::choose());
    let y = Node::spawn_configured("node-y", py, &[pz], "", 1, KEYED);
    let z = Node::spawn_configured("node-z", pz, &[py], "", 1, KEYED);
    let x = Node::spawn_configured("node-x", px, &[py], "", 1, KEYED);
    let mut m = Node::spawn_configured("node-m", pm, &[py], "", 1, KEYED);
    for node in [&y, &z, &x, &m] {
        node.wait_ready(&client).await;
    }
    let refused = format!("127.0.0.1:{py},127.0.0.1:{pz}");
    m.restart_with(&[("KIMMY_TEST_REFUSE_DIAL", refused.as_str())]);
    m.wait_ready(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&y, &z, &x, &m], 3)).await;
    let t = [
        y.login(&client).await,
        z.login(&client).await,
        x.login(&client).await,
        m.login(&client).await,
    ];
    let created = client
        .post(y.url("/v1/db/shop/collections"))
        .bearer_auth(&t[0])
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    let seeded = client
        .post(y.url("/v1/db/shop/coll/orders/docs"))
        .bearer_auth(&t[0])
        .json(&serde_json::json!({ "_id": 1, "n": 10 }))
        .send()
        .await
        .unwrap();
    assert_eq!(seeded.status(), 200);
    for (node, token) in [(&y, &t[0]), (&z, &t[1]), (&x, &t[2]), (&m, &t[3])] {
        eventually("every member to hold the document and take keys", || async {
            n_of(&client, node, token, 1).await == Some(10) && takes_keys(&client, node).await
        })
        .await;
    }
    let put = |node: &Node, token: &str, body: serde_json::Value| {
        let request = client
            .put(node.url("/v1/db/shop/coll/orders/docs/1"))
            .bearer_auth(token.to_string())
            .json(&body);
        async move { request.send().await.unwrap().status() }
    };

    // Y's keyed `$inc` reaches Z, and not X (stopped) nor M (which dials only X).
    // Y takes a key again once it no longer counts X (its block goes stale, then
    // SWIM declares it down).
    x.signal("STOP");
    eventually("Y to take keys with X down", || takes_keys(&client, &y)).await;
    let first = keyed_send(&client, &y, &t[0], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!((first.status, first.replayed), (200, false), "{}", first.text);
    eventually("Z to take the $inc", || async { n_of(&client, &z, &t[1], 1).await == Some(11) })
        .await;
    assert_eq!(n_of(&client, &m, &t[3], 1).await, Some(10), "premise: M has not");
    // X, cut off from Y and Z, writes the document after the `$inc`.
    y.signal("STOP");
    z.signal("STOP");
    x.signal("CONT");
    assert_eq!(put(&x, &t[2], serde_json::json!({ "n": 100 })).await, 200);
    // Z writes on top of the `$inc`, later still.
    x.signal("STOP");
    z.signal("CONT");
    assert_eq!(put(&z, &t[1], serde_json::json!({ "n": 11, "tag": "z" })).await, 200);
    // X pulls Z: the `$inc` loses to X's write, and Z's version wins.
    x.signal("CONT");
    eventually("Z's version on X", || async { n_of(&client, &x, &t[2], 1).await == Some(11) })
        .await;
    // Y and Z down: M takes Z's version from X, the one member it dials.
    z.signal("STOP");
    eventually("M to take Z's version from X", || async {
        n_of(&client, &m, &t[3], 1).await == Some(11)
    })
    .await;
    eventually("M to take keys with Y and Z down", || takes_keys(&client, &m)).await;
    let retry = keyed_send(&client, &m, &t[3], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!(retry.status, 200, "{}", retry.text);
    assert!(retry.replayed, "answered from the record X relayed: {retry:?}");
    assert_eq!(n_of(&client, &m, &t[3], 1).await, Some(11), "applied once");
    y.signal("CONT");
    z.signal("CONT");
}

/// G8 over real processes: a keyed multi-chunk update on an idle cluster writes
/// `done` with its last chunk's entry (the chunk's lookahead finds the end), so
/// every member that has caught up answers a retry from the record, as a replay
/// and not a resume. Nothing is written after the request.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_finished_multi_chunk_keyed_update_replays_on_every_caught_up_member() {
    const PATH: &str = "/v1/db/shop/coll/orders/update";
    const BODY: &str = r#"{"filter":{},"update":{"$inc":{"n":1}},"multi":true}"#;
    const KEY: &str = "g8-a-finished-multi-chunk-request";
    let client = reqwest::Client::new();
    let docs: Vec<serde_json::Value> =
        (1..=3).map(|i| serde_json::json!({ "_id": i, "n": 0 })).collect();
    let (a, b, c, t) =
        keyed_cluster(&client, "multi_chunk_docs = 1", serde_json::json!(docs)).await;
    let first = keyed_send(&client, &a, &t[0], reqwest::Method::POST, PATH, BODY, KEY).await;
    assert_eq!((first.status, first.replayed), (200, false), "{}", first.text);
    assert_eq!(first.body["commits"], 3, "premise: three chunks: {}", first.text);
    for (node, token) in [(&a, &t[0]), (&b, &t[1]), (&c, &t[2])] {
        eventually("every member to hold the three increments", || async {
            let mut all = true;
            for id in 1..=3 {
                all &= n_of(&client, node, token, id).await == Some(1);
            }
            all
        })
        .await;
    }
    for (node, token) in [(&b, &t[1]), (&c, &t[2]), (&a, &t[0])] {
        eventually("a retry answered from the finished record", || async {
            let retry =
                keyed_send(&client, node, token, reqwest::Method::POST, PATH, BODY, KEY).await;
            assert_eq!(retry.status, 200, "{}: {}", node.name, retry.text);
            assert!(retry.body.get("resumed").is_none(), "{}: resumed: {}", node.name, retry.text);
            retry.replayed && retry.text == first.text
        })
        .await;
        for id in 1..=3 {
            assert_eq!(
                n_of(&client, node, token, id).await,
                Some(1),
                "{}: applied once",
                node.name
            );
        }
    }
}

// ---------------------------------------------------------------------------
// A member declared down that rejoins goes on failure detecting
// ---------------------------------------------------------------------------

/// Y, Z, X and M, seeded as the field run that found it: Y and Z seed each
/// other, X and M seed Y.
fn rejoin_cluster() -> (Node, Node, Node, Node) {
    let (py, pz, px, pm) = (ports::choose(), ports::choose(), ports::choose(), ports::choose());
    (
        Node::spawn_configured("node-y", py, &[pz], "", 1, ""),
        Node::spawn_configured("node-z", pz, &[py], "", 1, ""),
        Node::spawn_configured("node-x", px, &[py], "", 1, ""),
        Node::spawn_configured("node-m", pm, &[py], "", 1, ""),
    )
}

/// Until X and M each count exactly one live member, the other, and return how
/// long that took.
async fn x_and_m_alone_together(client: &reqwest::Client, x: &Node, m: &Node) -> Duration {
    let started = std::time::Instant::now();
    eventually("X and M to declare Y and Z down and see each other live", || async {
        x.members_gauge(client).await == Some(1) && m.members_gauge(client).await == Some(1)
    })
    .await;
    started.elapsed()
}

/// X and M are frozen (SIGSTOP) until Y and Z declare them down, then Y and Z
/// are frozen and X and M resumed. X and M learn they were declared down and
/// rejoin; they must then declare the frozen Y and Z down within SWIM's own
/// timing (a probe, its round trip and the suspicion time, about 6.5 s here)
/// and find each other. Before the fix a member that rejoined on a `TurnUndead`
/// sat disconnected, probing no one: X and M held Y and Z live for good.
#[tokio::test]
#[ignore = "boots a real four-node cluster; run with --ignored"]
async fn a_member_declared_down_while_frozen_rejoins_and_declares_its_frozen_peers_down() {
    let client = reqwest::Client::new();
    let (y, z, x, m) = rejoin_cluster();
    for node in [&y, &z, &x, &m] {
        node.wait_ready(&client).await;
    }
    eventually("gossip to form", || all_report(&client, vec![&y, &z, &x, &m], 3)).await;
    x.signal("STOP");
    m.signal("STOP");
    eventually("Y and Z to declare X and M down", || async {
        y.members_gauge(&client).await == Some(1) && z.members_gauge(&client).await == Some(1)
    })
    .await;
    y.signal("STOP");
    z.signal("STOP");
    x.signal("CONT");
    tokio::time::sleep(Duration::from_secs(2)).await;
    m.signal("CONT");
    let took = x_and_m_alone_together(&client, &x, &m).await;
    assert!(took < Duration::from_secs(30), "X and M took {took:?}");
    y.signal("CONT");
    z.signal("CONT");
}

/// The same with Y and Z stopped for good (`TERM`) while X and M are frozen.
#[tokio::test]
#[ignore = "boots a real four-node cluster; run with --ignored"]
async fn a_member_declared_down_while_frozen_rejoins_and_declares_its_dead_peers_down() {
    let client = reqwest::Client::new();
    let (y, z, x, m) = rejoin_cluster();
    for node in [&y, &z, &x, &m] {
        node.wait_ready(&client).await;
    }
    eventually("gossip to form", || all_report(&client, vec![&y, &z, &x, &m], 3)).await;
    x.signal("STOP");
    m.signal("STOP");
    eventually("Y and Z to declare X and M down", || async {
        y.members_gauge(&client).await == Some(1) && z.members_gauge(&client).await == Some(1)
    })
    .await;
    y.signal("TERM");
    z.signal("TERM");
    y.wait_exit(Duration::from_secs(60));
    z.wait_exit(Duration::from_secs(60));
    x.signal("CONT");
    m.signal("CONT");
    let took = x_and_m_alone_together(&client, &x, &m).await;
    assert!(took < Duration::from_secs(30), "X and M took {took:?}");
}

// ---------------------------------------------------------------------------
// A restarted member is taken at once (ADR-242)
// ---------------------------------------------------------------------------

/// How many times this run of `node` rejoined the cluster.
fn rejoins(node: &Node) -> usize {
    node.log().matches("rejoined the cluster").count()
}

/// The incarnation `node`'s data directory records.
fn recorded_incarnation(node: &Node) -> u64 {
    let path = node.dir.path().join("data").join(kimmy_cluster::membership::incarnation::FILE);
    std::fs::read_to_string(&path).unwrap().trim().parse().unwrap()
}

/// `node` is ready, every member sees both others live, and the members have
/// had longer than SWIM's suspicion time (4.8 s) since: a suspicion a peer held
/// of the member's last identity has run out by then.
async fn retaken(client: &reqwest::Client, nodes: [&Node; 3], node: &Node) {
    node.wait_ready(client).await;
    eventually("every member to see both others live", || all_report(client, nodes.to_vec(), 2))
        .await;
    tokio::time::sleep(Duration::from_secs(8)).await;
}

/// The live peers `url`'s member holds no facts block from
/// (`kimmy_ownership_peers{state="unknown"}`), `None` when it does not answer.
async fn unheard_at(client: &reqwest::Client, url: &str) -> Option<u64> {
    let body = client.get(url).send().await.ok()?.text().await.ok()?;
    body.lines()
        .find_map(|line| line.strip_prefix(r#"kimmy_ownership_peers{state="unknown"} "#))
        .and_then(|value| value.trim().parse().ok())
}

/// The longest stretch over which `samples` (when, value) read above 0: from the
/// first sample above 0 to the first one after it that is not.
fn longest_unheard(samples: &[(std::time::Instant, Option<u64>)]) -> Duration {
    let (mut longest, mut since) = (Duration::ZERO, None);
    for (at, value) in samples {
        match (value, since) {
            (Some(1..), None) => since = Some(*at),
            (Some(0), Some(from)) => {
                longest = longest.max(at.duration_since(from));
                since = None;
            }
            _ => {}
        }
    }
    if let (Some(from), Some((last, _))) = (since, samples.last()) {
        longest = longest.max(last.duration_since(from));
    }
    longest
}

/// Sample `url`'s unheard peers every 10 ms until `stop`.
async fn sample_unheard(
    client: reqwest::Client,
    url: String,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Vec<(std::time::Instant, Option<u64>)> {
    let mut samples = Vec::new();
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        samples.push((std::time::Instant::now(), unheard_at(&client, &url).await));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    samples
}

/// A restart's peer window is about one contact (ADR-247). Each peer drops the
/// restarted member's last block when it sees it renamed (ADR-242) and exchanges
/// blocks with it at once, so at a 5 s sync interval a peer holds it live and
/// unheard for well under a second, not until its next tick (up to 5 s); and the
/// member, which exchanges with each peer it sees come up, holds none unheard
/// for long either. B is restarted three times with SIGTERM; A's and C's
/// `kimmy_ownership_peers{state="unknown"}` are sampled every 10 ms from before
/// the stop until 7 s after B is ready, and B's from its first ready answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_restarted_member_and_its_peers_hear_each_other_within_about_one_contact() {
    const SYNC_SECS: u64 = 5;
    const BOUND: Duration = Duration::from_secs(1);
    let client = reqwest::Client::new();
    let (a, mut b, c) = three_nodes_at(&client, "", SYNC_SECS).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let every_block_held = || async {
        for node in [&a, &b, &c] {
            if unheard_at(&client, &node.url("/metrics")).await != Some(0) {
                return false;
            }
        }
        true
    };
    eventually("every member to hold both others' blocks", every_block_held).await;

    for restart in 1..=3 {
        let before = recorded_incarnation(&b);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let peers = [&a, &c].map(|peer| {
            let url = peer.url("/metrics");
            tokio::spawn(sample_unheard(client.clone(), url, std::sync::Arc::clone(&stop)))
        });
        b.restart_with(&[]);
        b.wait_ready(&client).await;
        let own = tokio::spawn(sample_unheard(
            client.clone(),
            b.url("/metrics"),
            std::sync::Arc::clone(&stop),
        ));
        tokio::time::sleep(Duration::from_secs(SYNC_SECS + 2)).await;
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(recorded_incarnation(&b), before + 1, "premise: B started anew, renamed");
        let [sampled_a, sampled_c] = peers;
        for (name, sampled) in [("A", sampled_a), ("C", sampled_c), ("B", own)] {
            let samples = sampled.await.unwrap();
            let longest = longest_unheard(&samples);
            eprintln!(
                "restart {restart}: {name} held a live member unheard for {longest:?} at most"
            );
            assert!(
                longest < BOUND,
                "restart {restart}: {name} held a live member unheard for {longest:?}, about one \
                 contact expected, not a sync interval"
            );
            assert!(samples.iter().any(|(_, v)| *v == Some(0)), "{name} answered");
        }
        assert!(all_report(&client, vec![&a, &b, &c], 2).await, "restart {restart}: all live");
    }
}

/// A restarted member's first identity is one above the incarnation its last
/// run recorded, so its peers take it at once: no rejoin at any start, whether
/// they had declared it down, still held it live, or it had renewed in its last
/// run. Before ADR-242 every start was at incarnation 0 and renewed one step at
/// a time past the identity its peers held, each step dropping the blocks it
/// held, and the walk grew by one at every restart they noticed: the starts
/// after a stop the peers declared down took 1, 2 and 3 rejoins here.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_restarted_member_is_taken_by_its_peers_at_once() {
    let client = reqwest::Client::new();
    let (a, mut b, c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let mut last = recorded_incarnation(&b);
    assert_eq!(last, 0, "a first start records 0");

    let declared_down = || async {
        a.members_gauge(&client).await == Some(1) && c.members_gauge(&client).await == Some(1)
    };
    for cycle in 1..=2 {
        b.signal("TERM");
        b.wait_exit(Duration::from_secs(60));
        eventually("A and C to declare B down", declared_down).await;
        b.restart_with(&[]);
        retaken(&client, [&a, &b, &c], &b).await;
        assert_eq!(rejoins(&b), 0, "start {cycle} after B was declared down rejoined");
        assert_eq!(recorded_incarnation(&b), last + 1, "one above the last");
        last += 1;
    }

    // Renewed in its run: frozen until its peers declare it down, then resumed.
    b.signal("STOP");
    eventually("A and C to declare the frozen B down", declared_down).await;
    b.signal("CONT");
    eventually("B to be live again", || all_report(&client, vec![&a, &b, &c], 2)).await;
    assert!(rejoins(&b) >= 1, "premise: B renewed in its run");
    let renewed = recorded_incarnation(&b);
    assert!(renewed > last, "the renewal is recorded: {renewed} after {last}");
    b.signal("TERM");
    b.wait_exit(Duration::from_secs(60));
    eventually("A and C to declare B down", declared_down).await;
    b.restart_with(&[]);
    retaken(&client, [&a, &b, &c], &b).await;
    assert_eq!(rejoins(&b), 0, "the start after a renewal rejoined");
    last = renewed + 1;
    assert_eq!(recorded_incarnation(&b), last);

    // Restarted at once: its peers may still hold it live, or suspect it.
    for cycle in 1..=2 {
        b.restart_with(&[]);
        retaken(&client, [&a, &b, &c], &b).await;
        assert_eq!(rejoins(&b), 0, "quick restart {cycle} rejoined");
        assert_eq!(recorded_incarnation(&b), last + 1);
        last += 1;
    }
}

/// `orders/{id}`'s `v` on each of `nodes`.
async fn doc_vs(
    client: &reqwest::Client,
    nodes: &[&Node; 3],
    tokens: &[String; 3],
    id: i64,
) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for (node, token) in nodes.iter().zip(tokens) {
        out.push(doc_v(client, node, token, id).await);
    }
    out
}

/// A document's `v` on `node`, as text, or `None` while the node does not hold it.
async fn doc_v(client: &reqwest::Client, node: &Node, token: &str, id: i64) -> Option<String> {
    let res = client
        .get(node.url(&format!("/v1/db/shop/coll/orders/docs/{id}")))
        .bearer_auth(token)
        .send()
        .await
        .ok()?;
    if res.status() != 200 {
        return None;
    }
    let body: serde_json::Value = res.json().await.ok()?;
    let v = if body.get("v").is_some() { body["v"].clone() } else { body["document"]["v"].clone() };
    Some(v.to_string())
}

/// Every member of a cluster rewound to one target (`kimmyd restore --until`, the documented
/// procedure), where B and A judged C's write of `orders/1` against A's later write that the
/// rewind discards: C's write lost last-writer-wins on both, so neither appended it, and C's
/// later write of `orders/2`, at or below the target, keeps both covering C past it. After
/// the rewind B and A put `orders/1` back to A's first write, which they hold, and C holds its
/// own; no pull sends C's write again, since both cover it. The cut digest finds the
/// difference and the document repair (ADR-237) takes C's row: every member ends with C's
/// write.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_rewound_cluster_repairs_a_row_a_member_judged_against_a_discarded_write() {
    let client = reqwest::Client::new();
    let (mut a, mut b, mut c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let token = a.login(&client).await;
    let res = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{:?}", res.text().await);
    let put = |node: &Node, token: &str, id: i64, v: &str| {
        let (client, url, token, v) = (
            client.clone(),
            node.url(&format!("/v1/db/shop/coll/orders/docs/{id}")),
            token.to_string(),
            v.to_string(),
        );
        async move {
            let res = client
                .put(url)
                .bearer_auth(token)
                .json(&serde_json::json!({ "v": v }))
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200, "{:?}", res.text().await);
        }
    };
    let insert = |node: &Node, token: &str, id: i64, v: &str| {
        let (client, url, token, v) = (
            client.clone(),
            node.url("/v1/db/shop/coll/orders/docs"),
            token.to_string(),
            v.to_string(),
        );
        async move {
            let res = client
                .post(url)
                .bearer_auth(token)
                .json(&serde_json::json!({ "_id": id, "v": v }))
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200, "{:?}", res.text().await);
        }
    };
    insert(&a, &token, 1, "a-first").await;
    for node in [&b, &c] {
        let t = node.login(&client).await;
        eventually("orders/1 to reach every member", || {
            let (client, node, t) = (&client, node, &t);
            async move { doc_v(client, node, t, 1).await.is_some() }
        })
        .await;
    }

    // C is cut off: it dials nobody, and nobody dials it.
    let addr = |n: &Node| format!("127.0.0.1:{}", n.cluster);
    c.restart_with(&[("KIMMY_TEST_REFUSE_DIAL", &format!("{},{}", addr(&a), addr(&b)))]);
    a.restart_with(&[("KIMMY_TEST_REFUSE_DIAL", &addr(&c))]);
    b.restart_with(&[("KIMMY_TEST_REFUSE_DIAL", &addr(&c))]);
    for node in [&a, &b, &c] {
        node.wait_ready(&client).await;
    }
    let (token, c_token) = (a.login(&client).await, c.login(&client).await);
    put(&c, &c_token, 1, "c").await;
    insert(&c, &c_token, 2, "c").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let until_ms =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
            as u64;
    tokio::time::sleep(Duration::from_millis(50)).await;
    put(&a, &token, 1, "a-later").await;
    let b_token = b.login(&client).await;
    eventually("A's later write to reach B", || {
        let (client, b, t) = (&client, &b, &b_token);
        async move { doc_v(client, b, t, 1).await.as_deref() == Some("\"a-later\"") }
    })
    .await;

    // C rejoins: A and B take its writes, and its write of orders/1 loses to A's later one.
    for node in [&mut a, &mut b, &mut c] {
        node.restart_with(&[]);
    }
    for node in [&a, &b, &c] {
        node.wait_ready(&client).await;
    }
    for node in [&a, &b] {
        let t = node.login(&client).await;
        eventually("C's orders/2 to reach A and B", || {
            let (client, node, t) = (&client, node, &t);
            async move { doc_v(client, node, t, 2).await.is_some() }
        })
        .await;
        assert_eq!(doc_v(&client, node, &t, 1).await.as_deref(), Some("\"a-later\""));
    }
    let c_token = c.login(&client).await;
    eventually("A's later write to reach C", || {
        let (client, c, t) = (&client, &c, &c_token);
        async move { doc_v(client, c, t, 1).await.as_deref() == Some("\"a-later\"") }
    })
    .await;

    // Every member backed up, stopped, restored to the moment between C's writes and A's
    // later one, and started again; a restored cluster clears by the operator's removal of
    // the catching-up marker.
    let mut backups = Vec::new();
    for node in [&a, &b, &c] {
        let t = node.login(&client).await;
        let backup = client.get(node.url("/v1/admin/backup")).bearer_auth(&t).send().await.unwrap();
        assert_eq!(backup.status(), 200);
        let path = node.dir.path().join("backup.bin");
        std::fs::write(&path, backup.bytes().await.unwrap()).unwrap();
        backups.push(path);
    }
    for node in [&a, &b, &c] {
        node.signal("TERM");
        node.wait_exit(Duration::from_secs(60));
    }
    for (node, backup) in [&a, &b, &c].into_iter().zip(&backups) {
        std::fs::remove_dir_all(node.dir.path().join("data")).unwrap();
        let restored = Command::new(env!("CARGO_BIN_EXE_kimmyd"))
            .arg("--config")
            .arg(node.dir.path().join("kimmy.toml"))
            .arg("restore")
            .arg("--from")
            .arg(backup)
            .arg("--until")
            .arg(until_ms.to_string())
            .env("KIMMY_ROOT_PASSWORD", ROOT_PASSWORD)
            .output()
            .expect("running kimmyd restore --until");
        assert!(restored.status.success(), "{}", String::from_utf8_lossy(&restored.stderr));
        eprintln!(
            "{}: {}",
            node.name,
            String::from_utf8_lossy(&restored.stderr)
                .lines()
                .find(|l| l.starts_with("rewound"))
                .unwrap_or("")
        );
        for file in [kimmy_cluster::catchup::FILE, "kimmy.replay-floor"] {
            let _ = std::fs::remove_file(node.dir.path().join("data").join(file));
        }
    }
    for node in [&mut a, &mut b, &mut c] {
        node.restart_with(&[]);
    }
    for node in [&a, &b, &c] {
        node.wait_ready(&client).await;
    }
    let tokens = [a.login(&client).await, b.login(&client).await, c.login(&client).await];
    let nodes = [&a, &b, &c];
    eprintln!("after the rewind: orders/1 {:?}", doc_vs(&client, &nodes, &tokens, 1).await);
    let started = std::time::Instant::now();
    eventually("every member to hold C's write of orders/1", || {
        let (client, nodes, tokens) = (&client, &nodes, &tokens);
        async move {
            doc_vs(client, nodes, tokens, 1).await.iter().all(|v| v.as_deref() == Some("\"c\""))
        }
    })
    .await;
    eprintln!("every member holds C's write {} ms after the start", started.elapsed().as_millis());
    // A repair is counted when its pull ends, which can be after the page that wrote the row.
    let series = "kimmy_sync_digest_repairs_total{outcome=\"applied\"}";
    eventually("the document repair to count what it applied", || {
        let (client, a, b) = (&client, &a, &b);
        async move {
            a.gauge(client, series).await.unwrap_or(0) + b.gauge(client, series).await.unwrap_or(0)
                >= 1
        }
    })
    .await;
}

/// Every member of a cluster rewound to one target (`kimmyd restore --until`, the documented
/// procedure), where the rewind removes a document that two members judged a write at or below
/// the target against. C, cut off, inserts `orders/1` and then `orders/2`; A inserts `orders/1`
/// after the target, and B takes A's insert. When C rejoins, A and B judge C's insert of
/// `orders/1` against A's and neither appends it; C takes A's. The rewind removes `orders/1` on
/// A and B, whose oplogs hold no entry of it at or below the target, and leaves no tombstone;
/// C puts its own insert back. A and B keep C's insert of `orders/2`, so their coverage of C
/// stays above the judged write (ADR-246), and the divergence check's repair brings it back.
/// Every member ends with C's write, and a write on A and on B afterwards deletes it nowhere.
#[tokio::test]
#[ignore = "boots a real three-node cluster; run with --ignored"]
async fn a_rewound_cluster_keeps_a_write_judged_against_an_insert_the_rewind_removes() {
    let client = reqwest::Client::new();
    let (mut a, mut b, mut c) = three_nodes(&client).await;
    eventually("gossip to form", || all_report(&client, vec![&a, &b, &c], 2)).await;
    let token = a.login(&client).await;
    let res = client
        .post(a.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200, "{:?}", res.text().await);
    let insert = |node: &Node, token: &str, id: i64, v: &str| {
        let (client, url, token, v) = (
            client.clone(),
            node.url("/v1/db/shop/coll/orders/docs"),
            token.to_string(),
            v.to_string(),
        );
        async move {
            let res = client
                .post(url)
                .bearer_auth(token)
                .json(&serde_json::json!({ "_id": id, "v": v }))
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200, "{:?}", res.text().await);
        }
    };

    // The collection reaches every member before C is cut off.
    insert(&a, &token, 0, "seed").await;
    for node in [&b, &c] {
        let t = node.login(&client).await;
        eventually("orders/0 to reach every member", || {
            let (client, node, t) = (&client, node, &t);
            async move { doc_v(client, node, t, 0).await.is_some() }
        })
        .await;
    }

    // C is cut off: it dials nobody, and nobody dials it.
    let addr = |n: &Node| format!("127.0.0.1:{}", n.cluster);
    c.restart_with(&[("KIMMY_TEST_REFUSE_DIAL", &format!("{},{}", addr(&a), addr(&b)))]);
    a.restart_with(&[("KIMMY_TEST_REFUSE_DIAL", &addr(&c))]);
    b.restart_with(&[("KIMMY_TEST_REFUSE_DIAL", &addr(&c))]);
    for node in [&a, &b, &c] {
        node.wait_ready(&client).await;
    }
    let (token, c_token) = (a.login(&client).await, c.login(&client).await);
    insert(&c, &c_token, 1, "c").await;
    insert(&c, &c_token, 2, "c").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let until_ms =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
            as u64;
    tokio::time::sleep(Duration::from_millis(50)).await;
    insert(&a, &token, 1, "a-later").await;
    let b_token = b.login(&client).await;
    eventually("A's insert to reach B", || {
        let (client, b, t) = (&client, &b, &b_token);
        async move { doc_v(client, b, t, 1).await.as_deref() == Some("\"a-later\"") }
    })
    .await;

    // C rejoins: A and B take its writes, and its insert of orders/1 loses to A's.
    for node in [&mut a, &mut b, &mut c] {
        node.restart_with(&[]);
    }
    for node in [&a, &b, &c] {
        node.wait_ready(&client).await;
    }
    for node in [&a, &b] {
        let t = node.login(&client).await;
        eventually("C's orders/2 to reach A and B", || {
            let (client, node, t) = (&client, node, &t);
            async move { doc_v(client, node, t, 2).await.is_some() }
        })
        .await;
        assert_eq!(doc_v(&client, node, &t, 1).await.as_deref(), Some("\"a-later\""));
    }
    let c_token = c.login(&client).await;
    eventually("A's insert to reach C", || {
        let (client, c, t) = (&client, &c, &c_token);
        async move { doc_v(client, c, t, 1).await.as_deref() == Some("\"a-later\"") }
    })
    .await;

    // Every member backed up, stopped, restored to the moment between C's writes and A's
    // insert, and started again.
    let mut backups = Vec::new();
    for node in [&a, &b, &c] {
        let t = node.login(&client).await;
        let backup = client.get(node.url("/v1/admin/backup")).bearer_auth(&t).send().await.unwrap();
        assert_eq!(backup.status(), 200);
        let path = node.dir.path().join("backup.bin");
        std::fs::write(&path, backup.bytes().await.unwrap()).unwrap();
        backups.push(path);
    }
    for node in [&a, &b, &c] {
        node.signal("TERM");
        node.wait_exit(Duration::from_secs(60));
    }
    for (node, backup) in [&a, &b, &c].into_iter().zip(&backups) {
        std::fs::remove_dir_all(node.dir.path().join("data")).unwrap();
        let restored = Command::new(env!("CARGO_BIN_EXE_kimmyd"))
            .arg("--config")
            .arg(node.dir.path().join("kimmy.toml"))
            .arg("restore")
            .arg("--from")
            .arg(backup)
            .arg("--until")
            .arg(until_ms.to_string())
            .env("KIMMY_ROOT_PASSWORD", ROOT_PASSWORD)
            .output()
            .expect("running kimmyd restore --until");
        assert!(restored.status.success(), "{}", String::from_utf8_lossy(&restored.stderr));
        eprintln!(
            "{}: {}",
            node.name,
            String::from_utf8_lossy(&restored.stderr)
                .lines()
                .find(|l| l.starts_with("rewound"))
                .unwrap_or("")
        );
        for file in [kimmy_cluster::catchup::FILE, "kimmy.replay-floor"] {
            let _ = std::fs::remove_file(node.dir.path().join("data").join(file));
        }
    }
    for node in [&mut a, &mut b, &mut c] {
        node.restart_with(&[]);
    }
    for node in [&a, &b, &c] {
        node.wait_ready(&client).await;
    }
    let tokens = [a.login(&client).await, b.login(&client).await, c.login(&client).await];
    let nodes = [&a, &b, &c];
    eprintln!("after the rewind: orders/1 {:?}", doc_vs(&client, &nodes, &tokens, 1).await);
    eventually("every member to hold C's write of orders/1", || {
        let (client, nodes, tokens) = (&client, &nodes, &tokens);
        async move {
            doc_vs(client, nodes, tokens, 1).await.iter().all(|v| v.as_deref() == Some("\"c\""))
        }
    })
    .await;

    // A and B write again, another document each: their coverage of their own origin
    // moves on, and nothing the removal left behind may delete C's write anywhere.
    insert(&a, &tokens[0], 3, "a").await;
    insert(&b, &tokens[1], 4, "b").await;
    eventually("orders/3 and orders/4 to reach every member", || {
        let (client, nodes, tokens) = (&client, &nodes, &tokens);
        async move {
            doc_vs(client, nodes, tokens, 3).await.iter().all(Option::is_some)
                && doc_vs(client, nodes, tokens, 4).await.iter().all(Option::is_some)
        }
    })
    .await;
    assert_eq!(
        doc_vs(&client, &nodes, &tokens, 1).await,
        vec![Some("\"c\"".to_string()); 3],
        "every member still holds C's write"
    );
    eprintln!(
        "repairs applied A={:?} B={:?} C={:?}",
        a.gauge(&client, "kimmy_sync_digest_repairs_total{outcome=\"applied\"}").await,
        b.gauge(&client, "kimmy_sync_digest_repairs_total{outcome=\"applied\"}").await,
        c.gauge(&client, "kimmy_sync_digest_repairs_total{outcome=\"applied\"}").await,
    );
}
