//! Ownership yielding on real `kimmyd` processes (ADR-213): the process tests
//! P1 to P15 of the design: a stalled expiry task, dispatcher and embedding worker
//! (P1 to P3), a provider that fails and one that never answers (P4), an idle
//! cluster, a paused process, a stalled runtime and all of them stalled (P5 to P8),
//! a fault that follows the data and a bounded stall (P9, P10), a peer that never
//! completes a contact (P11), probation and an ordinary roll (P12, P13), the off
//! switch (P14), and an evaluator that stops ticking (P15).
//!
//! **Every test waits on events and asserts report fields, never elapsed time.**
//! The waits have generous ceilings that fail only on a hang. And **every
//! negative assertion carries its positive controls over the same window**: the
//! evaluator's ticks advanced, the class owned work, the induced condition was
//! really present, and a quiet verdict (good, idle or neutral) rose. A test that
//! asserts that something did not happen and cannot show that it could have
//! proves nothing, which is the failure this design is built to avoid.
//!
//! Each node runs at `KIMMY_TEST_YIELD_SCALE=5` (the evaluator's tick is a second,
//! and the latch hold and the back-off window a fifth of theirs), with
//! `ttl_interval_secs = 2` and `sync_interval_secs = 1`. The product's own cycles,
//! the dispatcher's, the worker's and expiry's, are the real ones.
//!
//! Ignored by default, as the cluster harness is; CI runs them with it:
//!
//! ```text
//! cargo test -p kimmyd --test yielding -- --ignored --test-threads=1
//! ```
//!
//! **The Linux harness job must run this file**: the start and exit paths change
//! (a supervised thread, a start record, the stop's first signal), and a pass on a
//! Mac is not a pass on Linux.

#![cfg(unix)]

use std::process::{Child, Command, Stdio};
use std::time::Duration;

mod node_logs;
mod ports;

const ROOT_PASSWORD: &str = "harness-root-password";
const JWT_SECRET: &str = "a-shared-harness-jwt-secret-value";
const CLUSTER_SECRET: &str = "a-shared-harness-cluster-secret";
const POLL: Duration = Duration::from_millis(250);

/// How long a condition may take before the harness gives up on it: generous, and
/// raised by `KIMMY_TEST_PATIENCE_SECS` on a slow runner. It fails only on a hang.
fn patience() -> Duration {
    Duration::from_secs(
        std::env::var("KIMMY_TEST_PATIENCE_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(150),
    )
}

/// The three classes, as the series name them.
const CLASSES: [&str; 3] = ["ttl", "webhooks", "embeddings"];
/// The classes probation judges: all three, now that the worker beats its cell.
const PROBATED: [&str; 3] = CLASSES;

/// The scale every node here runs at.
const SCALE: (&str, &str) = ("KIMMY_TEST_YIELD_SCALE", "5");

/// One spawned `kimmyd`, killed on drop.
struct Node {
    name: &'static str,
    child: std::sync::Mutex<Child>,
    pid: u32,
    http: u16,
    dir: tempfile::TempDir,
}

impl Node {
    /// A node with clustering on, seeded with `seeds`, with `env` added.
    fn spawn(name: &'static str, cluster: u16, seeds: &[u16], env: &[(&str, &str)]) -> Node {
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
ttl_interval_secs = 2

[auth]
jwt_secret = "{JWT_SECRET}"

[cluster]
enabled = true
bind = "127.0.0.1:{cluster}"
seeds = [{seed_list}]
cluster_secret = "{CLUSTER_SECRET}"
sync_interval_secs = 1
discovery_interval_secs = 2

[webhooks]
allowed_hosts = ["127.0.0.1"]

[vector.provider]
allowed_hosts = ["127.0.0.1"]
"#,
            data = dir.path().join("data").display(),
        );
        Self::start(name, dir, http, &config, env)
    }

    /// A node with no clustering.
    fn standalone(name: &'static str, env: &[(&str, &str)]) -> Node {
        let dir = tempfile::tempdir().unwrap();
        let http = ports::choose();
        let config = format!(
            r#"
[server]
bind = "127.0.0.1:{http}"

[storage]
data_dir = "{data}"
ttl_interval_secs = 2

[auth]
jwt_secret = "{JWT_SECRET}"
"#,
            data = dir.path().join("data").display(),
        );
        Self::start(name, dir, http, &config, env)
    }

    fn start(
        name: &'static str,
        dir: tempfile::TempDir,
        http: u16,
        config: &str,
        env: &[(&str, &str)],
    ) -> Node {
        let config_path = dir.path().join("kimmy.toml");
        std::fs::write(&config_path, config).unwrap();
        let child = Self::command(&dir, env);
        let pid = child.id();
        Node { name, child: std::sync::Mutex::new(child), pid, http, dir }
    }

    fn command(dir: &tempfile::TempDir, env: &[(&str, &str)]) -> Child {
        let stdout = std::fs::File::create(dir.path().join("stdout.log")).unwrap();
        let stderr = std::fs::File::create(dir.path().join("stderr.log")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_kimmyd"));
        for (key, value) in env {
            command.env(key, value);
        }
        command
            .arg("--config")
            .arg(dir.path().join("kimmy.toml"))
            .env("KIMMY_ROOT_PASSWORD", ROOT_PASSWORD)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawning kimmyd")
    }

    /// Stop this node (SIGTERM, or SIGKILL when `kill`), wait for it, and start it
    /// again on the same data directory and ports with `env`. The run before keeps
    /// its logs as `*.before.log`.
    fn restart(&mut self, kill: bool, env: &[(&str, &str)]) -> std::process::ExitStatus {
        let status = match self.child.get_mut().unwrap().try_wait().unwrap() {
            Some(status) => status,
            None => {
                self.signal(if kill { "KILL" } else { "TERM" });
                self.wait_exit(Duration::from_secs(90))
            }
        };
        for log in ["stdout", "stderr"] {
            let _ = std::fs::rename(
                self.dir.path().join(format!("{log}.log")),
                self.dir.path().join(format!("{log}.before.log")),
            );
        }
        let child = Self::command(&self.dir, env);
        self.pid = child.id();
        *self.child.get_mut().unwrap() = child;
        status
    }

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

    /// The node's log with its colour codes removed, so a field reads `name=value`.
    fn log(&self) -> String {
        strip_ansi(&std::fs::read_to_string(self.dir.path().join("stdout.log")).unwrap_or_default())
    }

    fn data(&self) -> std::path::PathBuf {
        self.dir.path().join("data")
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.http)
    }

    fn signal(&self, sig: &str) {
        let status =
            Command::new("kill").arg(format!("-{sig}")).arg(self.pid.to_string()).status().unwrap();
        assert!(status.success(), "kill -{sig} {}", self.pid);
    }

    /// Wait for `/readyz`, failing at once, with the node's stderr, if it exits.
    async fn wait_ready(&self, client: &reqwest::Client) {
        let deadline = std::time::Instant::now() + patience();
        loop {
            if let Ok(res) = client.get(self.url("/readyz")).send().await
                && res.status().is_success()
            {
                return;
            }
            if let Ok(Some(status)) = self.child.lock().unwrap().try_wait() {
                let stderr =
                    std::fs::read_to_string(self.dir.path().join("stderr.log")).unwrap_or_default();
                panic!("{} exited ({status}) before it became healthy:\n{stderr}", self.name);
            }
            assert!(std::time::Instant::now() < deadline, "{} never became healthy", self.name);
            tokio::time::sleep(POLL).await;
        }
    }

    async fn node_id(&self, client: &reqwest::Client) -> kimmy_core::NodeId {
        let body: serde_json::Value =
            client.get(self.url("/readyz")).send().await.unwrap().json().await.unwrap();
        body["node"].as_str().expect("a node id").parse().expect("a valid node id")
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

    /// One series from `/metrics`, by its full name and labels, as a number.
    async fn series(&self, client: &reqwest::Client, name: &str) -> Option<f64> {
        let body = client.get(self.url("/metrics")).send().await.ok()?.text().await.ok()?;
        let prefix = format!("{name} ");
        body.lines()
            .find(|l| l.starts_with(&prefix))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
    }

    async fn n(&self, client: &reqwest::Client, name: &str) -> u64 {
        self.series(client, name).await.unwrap_or(f64::NAN) as u64
    }

    /// A series that must be there: a scrape that misses it fails the test instead
    /// of reading as zero, which would pass an assertion that something is zero.
    async fn required(&self, client: &reqwest::Client, name: &str) -> u64 {
        let value = self
            .series(client, name)
            .await
            .unwrap_or_else(|| panic!("{}: the scrape has no `{name}`", self.name));
        value as u64
    }

    async fn yielding(&self, client: &reqwest::Client, class: &str) -> bool {
        self.series(client, &format!("kimmy_yielding{{class=\"{class}\"}}")).await == Some(1.0)
    }

    async fn ticks(&self, client: &reqwest::Client) -> u64 {
        self.n(client, "kimmy_yield_evaluator_ticks_total").await
    }

    async fn transitions(&self, client: &reqwest::Client, class: &str, direction: &str) -> u64 {
        self.n(
            client,
            &format!(
                "kimmy_yield_transitions_total{{class=\"{class}\",direction=\"{direction}\"}}"
            ),
        )
        .await
    }

    async fn observations(&self, client: &reqwest::Client, class: &str, verdict: &str) -> u64 {
        self.n(
            client,
            &format!("kimmy_yield_observations_total{{class=\"{class}\",verdict=\"{verdict}\"}}"),
        )
        .await
    }

    async fn suppressed(&self, client: &reqwest::Client, class: &str, reason: &str) -> bool {
        self.series(
            client,
            &format!("kimmy_yield_suppressed{{class=\"{class}\",reason=\"{reason}\"}}"),
        )
        .await
            == Some(1.0)
    }

    /// Whether the class is `stalled`, whatever the cause (the stalled series carry
    /// a cause label and no other state does).
    async fn is_stalled(&self, client: &reqwest::Client, class: &str) -> bool {
        for cause in ["local", "runtime", "probation"] {
            if self.stalled_by(client, class, cause).await {
                return true;
            }
        }
        false
    }

    /// Whether the class is `stalled` with this cause.
    async fn stalled_by(&self, client: &reqwest::Client, class: &str, cause: &str) -> bool {
        self.series(
            client,
            &format!(
                "kimmy_owner_class_state{{class=\"{class}\",state=\"stalled\",cause=\"{cause}\"}}"
            ),
        )
        .await
            == Some(1.0)
    }

    async fn owned(&self, client: &reqwest::Client, class: &str) -> u64 {
        self.n(client, &format!("kimmy_owner_class_owned{{class=\"{class}\"}}")).await
    }

    async fn faults(&self, client: &reqwest::Client, class: &str, kind: &str) -> u64 {
        self.n(client, &format!("kimmy_yield_faults_total{{class=\"{class}\",kind=\"{kind}\"}}"))
            .await
    }

    async fn members(&self, client: &reqwest::Client) -> Option<u64> {
        self.series(client, "kimmy_cluster_members").await.map(|v| v as u64)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // SIGKILL, unchecked: a stopped child ignores everything else, and the child
        // may already be dead by a test's own hand.
        let _ = Command::new("kill").arg("-KILL").arg(self.pid.to_string()).status();
        if let Ok(child) = self.child.get_mut() {
            let _ = child.wait();
        }
        node_logs::keep_if_failing(
            &node_logs::destination(),
            self.name,
            self.pid,
            &[&self.dir.path().join("stdout.log"), &self.dir.path().join("stderr.log")],
        );
    }
}

fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for next in chars.by_ref() {
                if next == 'm' {
                    break;
                }
            }
            continue;
        }
        out.push(c);
    }
    out
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

/// [`eventually`], and when it gives up, `report` is awaited and printed with the
/// failure: what a failed wait on a real cluster needs to answer itself.
async fn eventually_reporting<F, Fut, R, RFut>(what: &str, mut condition: F, report: R)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
    R: FnOnce() -> RFut,
    RFut: std::future::Future<Output = String>,
{
    let budget = patience();
    let deadline = std::time::Instant::now() + budget;
    loop {
        if condition().await {
            return;
        }
        if std::time::Instant::now() >= deadline {
            let report = report().await;
            panic!("gave up waiting for: {what} (after {budget:?})\n{report}");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// What each member says about ownership and embedding, and what reached the
/// provider: the lines a stuck embedding test is read from.
async fn embedding_report(
    client: &reqwest::Client,
    nodes: &[Node],
    provider: &Provider,
    owner: Option<usize>,
) -> String {
    let mut out = format!("owner: {owner:?}\n{}\n", provider.arrival_report());
    for node in nodes {
        let body = match client.get(node.url("/metrics")).send().await {
            Ok(res) => res.text().await.unwrap_or_default(),
            Err(e) => format!("(no metrics: {e})"),
        };
        out.push_str(&format!("== {}\n", node.name));
        for line in body.lines().filter(|l| {
            !l.starts_with('#')
                && [
                    "kimmy_owner_class_owned{class=\"embeddings\"",
                    "kimmy_embed_documents_total",
                    "kimmy_embed_failures_total",
                    "kimmy_ownership_peers",
                    "kimmy_yielding{class=\"embeddings\"",
                    "kimmy_yield_",
                ]
                .iter()
                .any(|k| l.starts_with(k))
        }) {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

/// `n` clustered nodes (two or three): A and B seed each other and C seeds A. Each
/// gets the scale and its own extra environment. A node that loses its port to
/// another process is answered by starting all again on new ports.
async fn cluster(client: &reqwest::Client, envs: &[&[(&str, &str)]]) -> Vec<Node> {
    const ATTEMPTS: usize = 5;
    assert!((2..=3).contains(&envs.len()));
    for _ in 0..ATTEMPTS {
        let ports: Vec<u16> = envs.iter().map(|_| ports::choose()).collect();
        let names = ["node-a", "node-b", "node-c"];
        let nodes: Vec<Node> = envs
            .iter()
            .enumerate()
            .map(|(i, env)| {
                let seeds: Vec<u16> = if i == 1 {
                    vec![ports[0]]
                } else if i == 0 {
                    vec![ports[1]]
                } else {
                    vec![ports[0]]
                };
                let mut all: Vec<(&str, &str)> = vec![SCALE];
                all.extend_from_slice(env);
                Node::spawn(names[i], ports[i], &seeds, &all)
            })
            .collect();
        let mut collided = false;
        for node in &nodes {
            let deadline = std::time::Instant::now() + patience();
            loop {
                if let Ok(res) = client.get(node.url("/readyz")).send().await
                    && res.status().is_success()
                {
                    break;
                }
                if let Ok(Some(_)) = node.child.lock().unwrap().try_wait() {
                    let stderr = std::fs::read_to_string(node.dir.path().join("stderr.log"))
                        .unwrap_or_default();
                    assert!(ports::in_use(&stderr), "{} exited:\n{stderr}", node.name);
                    collided = true;
                    break;
                }
                assert!(std::time::Instant::now() < deadline, "{} never became healthy", node.name);
                tokio::time::sleep(POLL).await;
            }
            if collided {
                break;
            }
        }
        if !collided {
            let peers = (nodes.len() - 1) as u64;
            eventually("gossip to form", || {
                let nodes = &nodes;
                async move {
                    for node in nodes {
                        if node.members(client).await != Some(peers) {
                            return false;
                        }
                    }
                    true
                }
            })
            .await;
            return nodes;
        }
    }
    panic!("{ATTEMPTS} attempts at a cluster each lost a port to another process");
}

/// The shop's collections and a TTL index on `sessions`, made through `node`, and
/// the wait for exactly one member to own it. Returns the token and the owner's
/// position.
async fn ttl_collection(client: &reqwest::Client, nodes: &[Node]) -> (String, usize) {
    let token = nodes[0].login(client).await;
    client
        .post(nodes[0].url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "sessions" }))
        .send()
        .await
        .unwrap();
    let created = client
        .post(nodes[0].url("/v1/db/shop/coll/sessions/indexes"))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "name": "ttl_seen", "fields": [{ "path": "seen" }], "expireAfterSeconds": 2,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200, "creating the TTL index");
    // **A settled owner, not the first one seen.** While the other members are still
    // catching up, the one that is not owns every collection; when they clear, the
    // top-ranked holder takes it. A test that restarts "the owner" read in that
    // window restarts a member that owns nothing once the cluster settles: it then
    // stalls and, owning nothing, rightly has nothing to yield. So the wait is for
    // every member to have cleared its marker and for the same single owner to be
    // read on `STABLE_POLLS` polls in a row.
    const STABLE_POLLS: u32 = 8;
    let owner = std::sync::Arc::new(std::sync::Mutex::new((None::<usize>, 0u32)));
    eventually("exactly one settled member to own the collection's expiry", || {
        let (owner, nodes) = (std::sync::Arc::clone(&owner), nodes);
        async move {
            let mut owners = Vec::new();
            for (i, node) in nodes.iter().enumerate() {
                let caught_up =
                    node.series(client, "kimmy_catching_up{reason=\"none\"}").await == Some(1.0);
                match node.series(client, "kimmy_ttl_collections{state=\"owned\"}").await {
                    Some(n) if caught_up && n >= 1.0 => owners.push(i),
                    Some(_) if caught_up => {}
                    _ => {
                        *owner.lock().unwrap() = (None, 0);
                        return false;
                    }
                }
            }
            let mut seen = owner.lock().unwrap();
            if owners.len() == 1 {
                seen.1 = if seen.0 == Some(owners[0]) { seen.1 + 1 } else { 1 };
                seen.0 = Some(owners[0]);
            } else {
                *seen = (None, 0);
            }
            seen.1 >= STABLE_POLLS
        }
    })
    .await;
    let owner = owner.lock().unwrap().0;
    let owner = owner.expect("an owner");
    (token, owner)
}

/// A webhook receiver that records what arrives.
async fn receiver() -> (std::net::SocketAddr, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let record = std::sync::Arc::clone(&seen);
    // UNSUPERVISED: a test receiver, ended with the test
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            let record = std::sync::Arc::clone(&record);
            // UNSUPERVISED: one connection of the test receiver
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let Ok(n) = stream.read(&mut chunk).await else { return };
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf);
                    if let Some(head_end) = text.find("\r\n\r\n") {
                        let length = text
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        if buf.len() >= head_end + 4 + length {
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

/// A collection and `count` subscriptions to it, made through `node`; returns the
/// subscription ids.
async fn subscriptions(
    client: &reqwest::Client,
    node: &Node,
    hook: std::net::SocketAddr,
    count: usize,
) -> (String, Vec<String>) {
    let token = node.login(client).await;
    client
        .post(node.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for _ in 0..count {
        let res: serde_json::Value = client
            .post(node.url("/v1/db/shop/coll/orders/webhooks"))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "url": format!("http://{hook}/hook") }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        ids.push(res["id"].as_str().expect("an id").to_string());
    }
    (token, ids)
}

/// What `class` owns, summed over `nodes`.
async fn owned_in_all(client: &reqwest::Client, nodes: &[Node], class: &str) -> u64 {
    let mut total = 0;
    for node in nodes {
        total += node.owned(client, class).await;
    }
    total
}

// ---------------------------------------------------------------------------
// P1, P2: a stalled class yields to a peer, and the work resumes there.
// ---------------------------------------------------------------------------

/// P1: one TTL collection, and `ttl_expiry:stall` on its owner. The owner yields,
/// another holder expires a due document, and the cluster's expiry is owned once.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p1_a_ttl_owner_that_stalls_yields_and_another_holder_expires_the_document() {
    let client = reqwest::Client::new();
    let mut nodes = cluster(&client, &[&[], &[], &[]]).await;
    let (token, owner) = ttl_collection(&client, &nodes).await;
    // Control: it was the owner, and owned work, before anything was wrong.
    assert_eq!(
        nodes[owner].series(&client, "kimmy_ttl_collections{state=\"owned\"}").await,
        Some(1.0)
    );

    nodes[owner].restart(false, &[SCALE, ("KIMMY_TEST_KILL_TASK", "ttl_expiry:stall")]);
    nodes[owner].wait_ready(&client).await;
    eventually("the stalled owner to yield its expiry", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.yielding(client, "ttl").await }
    })
    .await;
    // Controls: the stall really began, the evaluator ticked, and it is the
    // stalled member alone that yields.
    assert!(nodes[owner].log().contains("test stall began"), "{}", nodes[owner].log());
    for node in &nodes {
        assert!(node.ticks(&client).await >= 5, "every evaluator has ticked");
    }
    for (i, node) in nodes.iter().enumerate() {
        assert_eq!(node.yielding(&client, "ttl").await, i == owner, "{}", node.name);
    }

    // Once every peer has echoed the yield the stalled member stops owning, and
    // exactly one member (another holder) owns the collection's expiry.
    eventually("one other member to own the expiry, and the stalled one not", || {
        let (nodes, client) = (&nodes, &client);
        async move {
            let mut owners = Vec::new();
            for (i, node) in nodes.iter().enumerate() {
                if node.series(client, "kimmy_ttl_collections{state=\"owned\"}").await >= Some(1.0)
                {
                    owners.push(i);
                }
            }
            owners.len() == 1 && owners[0] != owner
        }
    })
    .await;

    // A document already due, inserted on a member that is not the owner: the
    // stalled owner cannot have deleted it.
    let other = (owner + 1) % nodes.len();
    let other_token = nodes[other].login(&client).await;
    let _ = token;
    let past =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
            as i64
            - 600_000;
    let inserted = client
        .post(nodes[other].url("/v1/db/shop/coll/sessions/docs"))
        .bearer_auth(&other_token)
        .json(&serde_json::json!({ "_id": 1, "seen": { "$date": past } }))
        .send()
        .await
        .unwrap();
    assert!(inserted.status().is_success());
    for node in &nodes {
        let node_token = node.login(&client).await;
        eventually("the due document to be gone on every member", || {
            let (client, url, token) =
                (&client, node.url("/v1/db/shop/coll/sessions/docs/1"), node_token.clone());
            async move { client.get(url).bearer_auth(&token).send().await.unwrap().status() == 404 }
        })
        .await;
    }
    // The stalled member never expired it: it deleted nothing itself.
    assert_eq!(nodes[owner].n(&client, "kimmy_ttl_expired_total").await, 0);
}

/// P2: one subscription, and `webhook_dispatcher:stall` on its owner. The owner
/// yields, and deliveries resume on another member.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p2_a_stalled_dispatcher_yields_and_deliveries_resume_on_another_member() {
    let client = reqwest::Client::new();
    let mut nodes = cluster(&client, &[&[], &[], &[]]).await;
    let (hook, seen) = receiver().await;
    let (token, ids) = subscriptions(&client, &nodes[0], hook, 1).await;
    eventually("exactly one member to own the subscription", || {
        let (nodes, client) = (&nodes, &client);
        async move { owned_in_all(client, nodes, "webhooks").await == 1 }
    })
    .await;
    let mut owner = None;
    for (i, node) in nodes.iter().enumerate() {
        if node.owned(&client, "webhooks").await == 1 {
            owner = Some(i);
        }
    }
    let owner = owner.expect("an owner");

    nodes[owner].restart(false, &[SCALE, ("KIMMY_TEST_KILL_TASK", "webhook_dispatcher:stall")]);
    nodes[owner].wait_ready(&client).await;
    eventually("the stalled owner to yield its deliveries", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.yielding(client, "webhooks").await }
    })
    .await;
    assert!(nodes[owner].log().contains("test stall began"));
    for node in &nodes {
        assert!(node.ticks(&client).await >= 5);
    }

    // A write after the yield is delivered by a member that is not the stalled one.
    let other = (owner + 1) % nodes.len();
    let other_token = nodes[other].login(&client).await;
    let _ = token;
    let inserted = client
        .post(nodes[other].url("/v1/db/shop/coll/orders/docs"))
        .bearer_auth(&other_token)
        .json(&serde_json::json!({ "_id": 1, "item": "widget" }))
        .send()
        .await
        .unwrap();
    assert!(inserted.status().is_success());
    eventually("the event to be delivered while the owner is stalled", || {
        let (seen, ids) = (std::sync::Arc::clone(&seen), ids.clone());
        async move {
            seen.lock()
                .unwrap()
                .iter()
                .any(|b| b.contains(ids[0].as_str()) && b.contains("\"_id\":1"))
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// The embedding half: P3 and P4, and the embedding class through the rest.
// ---------------------------------------------------------------------------

/// What the fake embedding provider answers (fixed for a provider's life).
const PROVIDER_OK: u8 = 0;
const PROVIDER_503: u8 = 1;
/// Accepts the connection, reads the request and never answers.
const PROVIDER_HANG: u8 = 2;

/// A fake `custom_http` embedding provider on loopback: `{"input": [..]}` in,
/// `{"embeddings": [[..], ..]}` out, at the mode the test sets. Counts requests.
struct Provider {
    addr: std::net::SocketAddr,
    requests: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// When the first request arrived, by the provider's own clock.
    first_at: std::sync::Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    /// When each request arrived, for the report a timed-out wait prints.
    arrivals: std::sync::Arc<std::sync::Mutex<Vec<std::time::Instant>>>,
    mode: std::sync::Arc<std::sync::atomic::AtomicU8>,
}

impl Provider {
    async fn start(initial: u8) -> Provider {
        use std::sync::atomic::Ordering::SeqCst;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mode = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(initial));
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let first_at = std::sync::Arc::new(std::sync::Mutex::new(None));
        let arrivals = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let (seen_mode, seen_requests, seen_first, seen_arrivals) = (
            std::sync::Arc::clone(&mode),
            std::sync::Arc::clone(&requests),
            std::sync::Arc::clone(&first_at),
            std::sync::Arc::clone(&arrivals),
        );
        // UNSUPERVISED: a test provider, ended with the test
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let (mode, requests, first, arrived) = (
                    std::sync::Arc::clone(&seen_mode),
                    std::sync::Arc::clone(&seen_requests),
                    std::sync::Arc::clone(&seen_first),
                    std::sync::Arc::clone(&seen_arrivals),
                );
                // UNSUPERVISED: one connection of the test provider
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 8192];
                    let body = loop {
                        let Ok(n) = stream.read(&mut chunk).await else { return };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        let text = String::from_utf8_lossy(&buf).to_string();
                        if let Some(head_end) = text.find("\r\n\r\n") {
                            let length = text
                                .lines()
                                .find_map(|l| {
                                    l.to_ascii_lowercase()
                                        .strip_prefix("content-length:")
                                        .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                                })
                                .unwrap_or(0);
                            if buf.len() >= head_end + 4 + length {
                                break text[head_end + 4..].to_string();
                            }
                        }
                    };
                    arrived.lock().unwrap().push(std::time::Instant::now());
                    if requests.fetch_add(1, SeqCst) == 0 {
                        *first.lock().unwrap() = Some(std::time::Instant::now());
                    }
                    match mode.load(SeqCst) {
                        PROVIDER_HANG => {
                            // Held open until the test ends, which drops the socket.
                            let mut rest = [0u8; 64];
                            while let Ok(n) = stream.read(&mut rest).await {
                                if n == 0 {
                                    break;
                                }
                            }
                        }
                        PROVIDER_503 => {
                            let _ = stream
                                .write_all(
                                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                )
                                .await;
                        }
                        _ => {
                            let inputs = serde_json::from_str::<serde_json::Value>(&body)
                                .ok()
                                .and_then(|v| v["input"].as_array().map(Vec::len))
                                .unwrap_or(1);
                            let vectors = vec![vec![0.1f32, 0.2, 0.3, 0.4]; inputs];
                            let reply = serde_json::json!({ "embeddings": vectors }).to_string();
                            let _ = stream
                                .write_all(
                                    format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                                        reply.len()
                                    )
                                    .as_bytes(),
                                )
                                .await;
                        }
                    }
                });
            }
        });
        Provider { addr, requests, first_at, arrivals, mode }
    }

    /// From now on accept requests and never answer them, and count from zero: the
    /// first request after this call is the first of the hang.
    fn hang_from_now(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        self.mode.store(PROVIDER_HANG, SeqCst);
        self.requests.store(0, SeqCst);
        *self.first_at.lock().unwrap() = None;
    }

    fn requests(&self) -> u64 {
        self.requests.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The requests so far, as seconds after the first, for a failure report.
    fn arrival_report(&self) -> String {
        let arrivals = self.arrivals.lock().unwrap();
        match arrivals.first() {
            None => "provider requests: none".to_string(),
            Some(first) => {
                let offsets: Vec<String> = arrivals
                    .iter()
                    .map(|at| format!("{:.1}", at.duration_since(*first).as_secs_f64()))
                    .collect();
                format!(
                    "provider requests: {} (seconds after the first: {})",
                    arrivals.len(),
                    offsets.join(", ")
                )
            }
        }
    }

    /// How long ago the first request arrived, if one has.
    fn first_request_age(&self) -> Option<Duration> {
        self.first_at.lock().unwrap().map(|at| at.elapsed())
    }
}

/// A collection with server-side embedding through `provider`, made through node 0,
/// with `documents` documents in it. Returns the token and the index of the node that
/// owns the embedding class (the one whose `owned` reads 1).
async fn embedded_collection(
    client: &reqwest::Client,
    nodes: &[Node],
    provider: &Provider,
    documents: i64,
) -> (String, usize) {
    let token = nodes[0].login(client).await;
    let made = client
        .post(nodes[0].url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "notes" }))
        .send()
        .await
        .unwrap();
    assert!(made.status().is_success(), "{:?}", made.text().await);
    let configured = client
        .post(nodes[0].url("/v1/db/shop/coll/notes/vector"))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "fields": ["title"],
            "provider": {
                "kind": "custom_http",
                "endpoint": format!("http://{}/embed", provider.addr)
            },
            "dim": 4
        }))
        .send()
        .await
        .unwrap();
    assert!(configured.status().is_success(), "{:?}", configured.text().await);
    // **Ownership settles before a document is written.** In a fresh cluster each
    // member owns what it has not heard a peer claim, so for a few seconds more than
    // one acts as owner, and the one that publishes `owned` first is not always the
    // one the cluster settles on. Documents written in that window can be embedded by
    // the early owner while the member this helper then picks, the settled one, has
    // embedded none. So the wait is for every member to see all the others and for the
    // same single member to publish `owned` for longer than two ownership ticks (five
    // seconds each), and only then are the documents written.
    const STABLE_POLLS: u32 = 48; // 12 s at a 250 ms poll
    let want_peers = (nodes.len() - 1) as f64;
    let steady = std::sync::Arc::new(std::sync::Mutex::new((None::<usize>, 0u32)));
    eventually_reporting(
        "every member to see the others and exactly one to own the embedding collection, steadily",
        || {
            let (nodes, client, steady) = (nodes, client, std::sync::Arc::clone(&steady));
            async move {
                let mut owners = Vec::new();
                for (i, node) in nodes.iter().enumerate() {
                    let sees =
                        node.series(client, "kimmy_ownership_peers{state=\"eligible\"}").await
                            == Some(want_peers);
                    if !sees {
                        *steady.lock().unwrap() = (None, 0);
                        return false;
                    }
                    if node.owned(client, "embeddings").await == 1 {
                        owners.push(i);
                    }
                }
                let mut seen = steady.lock().unwrap();
                if owners.len() == 1 {
                    seen.1 = if seen.0 == Some(owners[0]) { seen.1 + 1 } else { 1 };
                    seen.0 = Some(owners[0]);
                } else {
                    *seen = (None, 0);
                }
                seen.1 >= STABLE_POLLS
            }
        },
        || embedding_report(client, nodes, provider, None),
    )
    .await;
    let owner = steady.lock().unwrap().0.expect("an owner");
    for id in 0..documents {
        let res = client
            .post(nodes[0].url("/v1/db/shop/coll/notes/docs"))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "_id": id, "title": format!("a small note {id}") }))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success(), "{:?}", res.text().await);
    }
    (token, owner)
}

async fn embedded_documents(client: &reqwest::Client, node: &Node) -> u64 {
    node.n(client, "kimmy_embed_documents_total").await
}

/// P3: one vector collection and `embedding_worker:stall` on its owner. The owner
/// yields the embedding class, a peer gains the collection (the `rescan_gained`
/// start line on the gainer), and a document written afterwards is embedded by that
/// peer while the owner is stalled.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p3_a_stalled_embedding_worker_yields_and_the_gainer_rescans_and_embeds() {
    let client = reqwest::Client::new();
    let provider = Provider::start(PROVIDER_OK).await;
    let mut nodes = cluster(&client, &[&[], &[], &[]]).await;
    let (token, owner) = embedded_collection(&client, &nodes, &provider, 3).await;
    // Control: the owner owned the work, and embedded it, before anything was wrong.
    eventually_reporting(
        "the owner to embed the three documents",
        || {
            let (node, client) = (&nodes[owner], &client);
            async move { embedded_documents(client, node).await >= 3 }
        },
        || embedding_report(&client, &nodes, &provider, Some(owner)),
    )
    .await;
    assert!(provider.requests() >= 1, "the fake provider answered");

    nodes[owner].restart(false, &[SCALE, ("KIMMY_TEST_KILL_TASK", "embedding_worker:stall")]);
    nodes[owner].wait_ready(&client).await;
    eventually("the stalled worker's member to yield the embedding class", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.yielding(client, "embeddings").await }
    })
    .await;
    assert!(nodes[owner].log().contains("test stall began"));
    let others: Vec<usize> = (0..nodes.len()).filter(|i| *i != owner).collect();
    eventually("a peer to gain the collection and rescan it", || {
        let nodes = &nodes;
        let others = others.clone();
        async move {
            others.iter().any(|i| {
                nodes[*i]
                    .log()
                    .contains("rescanned a collection whose ownership moved to this member")
            })
        }
    })
    .await;
    // A write after the hand-over is embedded by the member that gained it.
    let gainer = *others
        .iter()
        .find(|i| {
            nodes[**i].log().contains("rescanned a collection whose ownership moved to this member")
        })
        .unwrap();
    let before = embedded_documents(&client, &nodes[gainer]).await;
    let res = client
        .post(nodes[gainer].url("/v1/db/shop/coll/notes/docs"))
        .bearer_auth(&nodes[gainer].login(&client).await)
        .json(&serde_json::json!({ "_id": 99, "title": "written after the stall" }))
        .send()
        .await
        .unwrap();
    assert!(res.status().is_success());
    let _ = token;
    eventually("the gainer to embed it", || {
        let (node, client) = (&nodes[gainer], &client);
        async move { embedded_documents(client, node).await > before }
    })
    .await;
    for node in &nodes {
        assert!(node.ticks(&client).await >= 5);
    }
}

/// P4: a provider that answers 503, or never answers, on every member. **Nothing
/// moves**: a provider failing is not this member failing, so no member stalls or
/// yields, and the controls say the class owned work, the evaluator watched, and the
/// failures were really there.
async fn a_failing_provider_moves_nothing(mode: u8) {
    let client = reqwest::Client::new();
    // A provider that never answers starts answering: the owner has to be found, and
    // it is only found at the worker's ownership tick, which a worker already inside
    // a hung call does not reach until the call ends. It hangs once the owner is known,
    // on a document written then, so the hold below is timed from a request the test
    // caused.
    let provider = Provider::start(if mode == PROVIDER_HANG { PROVIDER_OK } else { mode }).await;
    let nodes = cluster(&client, &[&[], &[], &[]]).await;
    let (token, owner) = embedded_collection(&client, &nodes, &provider, 3).await;
    if mode == PROVIDER_HANG {
        eventually_reporting(
            "the owner to embed the three documents while the provider answers",
            || {
                let (node, client) = (&nodes[owner], &client);
                async move { embedded_documents(client, node).await >= 3 }
            },
            || embedding_report(&client, &nodes, &provider, Some(owner)),
        )
        .await;
        provider.hang_from_now();
        let res = client
            .post(nodes[0].url("/v1/db/shop/coll/notes/docs"))
            .bearer_auth(&token)
            .json(
                &serde_json::json!({ "_id": 100, "title": "a note the provider will not answer" }),
            )
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success(), "{:?}", res.text().await);
    }
    let mut start = Vec::new();
    for node in &nodes {
        start.push(node.ticks(&client).await);
    }
    // (iii) the condition was really present. Refused: the owner counted four calls as
    // remote faults (the worker's own back-off is 5 s doubling, so four failed
    // attempts take about 35 s; ten cycles take minutes). Never answered: a
    // request reached the provider, and the first request has then been **held for
    // longer than the local stall bound**, by the provider's own clock, with no member
    // having counted a local fault; and while that request is younger than an attempt
    // (60 s) no call can have ended, so no member has counted a remote fault either.
    // No second request is waited for (one call alone is two attempts of 60 s, and the
    // request count is shared by every member). A call held that long is `Remote`,
    // bounded by the attempt's own timer: judged as local work it would be overdue by
    // now, and the end-state checks below would find the class stalled and yielding.
    if mode == PROVIDER_503 {
        eventually("the owner to have been refused four times", || {
            let (node, client) = (&nodes[owner], &client);
            async move { node.faults(client, "embeddings", "remote").await >= 4 }
        })
        .await;
    } else {
        eventually("a request to reach the provider", || {
            let provider = &provider;
            async move { provider.requests() >= 1 }
        })
        .await;
        // The provider's own clock says how long ago the first request arrived. The
        // owner is only found when the hung call ends if the worker's loop was already
        // inside it (the ownership tick that publishes `owned` runs in that loop), so
        // the setup above can have taken the whole call.
        const ATTEMPT: Duration = Duration::from_secs(60);
        let bound = kimmy_cluster::yielding::LOCAL_BOUND;
        let held = bound + Duration::from_secs(10);
        let age = provider.first_request_age().expect("a request arrived");
        if let Some(rest) = held.checked_sub(age) {
            tokio::time::sleep(rest).await;
        }
        let age = provider.first_request_age().unwrap();
        assert!(age > bound, "the first request has been held for longer than the local bound");
        for node in &nodes {
            assert_eq!(
                node.required(
                    &client,
                    "kimmy_yield_faults_total{class=\"embeddings\",kind=\"local\"}"
                )
                .await,
                0,
                "{}: a hung provider is not a local fault",
                node.name
            );
            // And no tick has judged the class bad: held past the local bound, a call
            // read as local work would be overdue and counted exactly here.
            assert_eq!(
                node.required(
                    &client,
                    "kimmy_yield_observations_total{class=\"embeddings\",verdict=\"bad\"}"
                )
                .await,
                0,
                "{}: a call held past the local bound is not a bad tick",
                node.name
            );
            // An attempt lasts 60 s: while the first request is younger than that no
            // call has ended, so no member can have counted a fault of any kind. Older
            // than that, a remote fault is expected, and is not asserted.
            if age < ATTEMPT - Duration::from_secs(5) {
                assert_eq!(
                    node.required(
                        &client,
                        "kimmy_yield_faults_total{class=\"embeddings\",kind=\"remote\"}"
                    )
                    .await,
                    0,
                    "{}: no call has ended yet",
                    node.name
                );
            }
        }
    }
    // (i) the evaluator watched, long enough for M ticks on every member.
    eventually("the evaluator to tick M more times everywhere", || {
        let (nodes, client, start) = (&nodes, &client, start.clone());
        async move {
            for (i, node) in nodes.iter().enumerate() {
                if node.ticks(client).await < start[i] + 12 {
                    return false;
                }
            }
            true
        }
    })
    .await;
    // (ii) the class owned work.
    assert!(owned_in_all(&client, &nodes, "embeddings").await > 0);
    // (iv) quiet verdicts were reached, not a silence.
    let mut quiet = 0;
    for node in &nodes {
        for verdict in ["good", "idle", "neutral"] {
            quiet += node.observations(&client, "embeddings", verdict).await;
        }
    }
    assert!(quiet > 0, "quiet verdicts rose");
    for node in &nodes {
        assert!(!node.is_stalled(&client, "embeddings").await, "{} stalled", node.name);
        assert!(!node.yielding(&client, "embeddings").await, "{} yields", node.name);
        for direction in ["yield", "withdraw", "reclaim"] {
            assert_eq!(node.transitions(&client, "embeddings", direction).await, 0);
        }
        assert_eq!(node.faults(&client, "embeddings", "local").await, 0, "{}", node.name);
    }
    if mode == PROVIDER_503 {
        assert!(nodes[owner].n(&client, "kimmy_embed_failures_total").await >= 1);
    }
}

#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p4_a_provider_answering_503_moves_nothing() {
    a_failing_provider_moves_nothing(PROVIDER_503).await;
}

/// The same with a provider that accepts the request and never answers (a hung
/// provider): the member is waiting on a timer of its own, so the class is `Remote`, bounded
/// per attempt, and never overdue.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p4b_a_provider_that_never_answers_moves_nothing() {
    a_failing_provider_moves_nothing(PROVIDER_HANG).await;
}

// ---------------------------------------------------------------------------
// P5, P6: what must not move.
// ---------------------------------------------------------------------------

/// P5: an idle three-node cluster. Nothing is bad, nothing yields, and the
/// evaluator really watched: its ticks rose by at least M on every member, and
/// quiet verdicts rose.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p5_an_idle_cluster_counts_nothing_bad_and_yields_nothing() {
    let client = reqwest::Client::new();
    let nodes = cluster(&client, &[&[], &[], &[]]).await;
    let mut start = Vec::new();
    for node in &nodes {
        start.push(node.ticks(&client).await);
    }
    // M ticks of the window, and twice R's worth beyond that for the idle ones.
    eventually("every member to tick at least 24 more times", || {
        let (nodes, client, start) = (&nodes, &client, &start);
        async move {
            for (node, from) in nodes.iter().zip(start) {
                if node.ticks(client).await < from + 24 {
                    return false;
                }
            }
            true
        }
    })
    .await;
    for node in &nodes {
        let mut quiet = 0;
        for class in CLASSES {
            assert_eq!(node.observations(&client, class, "bad").await, 0, "{} {class}", node.name);
            for direction in ["yield", "reclaim", "withdraw"] {
                assert_eq!(node.transitions(&client, class, direction).await, 0);
            }
            assert!(!node.yielding(&client, class).await);
            for verdict in ["good", "idle", "neutral"] {
                quiet += node.observations(&client, class, verdict).await;
            }
        }
        assert!(quiet >= 24, "{}: quiet verdicts rose with the ticks: {quiet}", node.name);
        assert_eq!(node.n(&client, "kimmy_runtime_responsive").await, 1, "{}", node.name);
    }
}

/// P6: the whole process paused (SIGSTOP) and resumed. The evaluator reads two void
/// ticks, judges nothing, and no member yields; and the paused member owned work
/// and ticked, so it could have.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p6_a_paused_process_reads_void_ticks_and_nothing_yields() {
    let client = reqwest::Client::new();
    let nodes = cluster(&client, &[&[], &[], &[]]).await;
    let (_token, owner) = ttl_collection(&client, &nodes).await;
    // Controls before: it owned work and its evaluator ticked.
    assert!(nodes[owner].ticks(&client).await >= 1);
    let void_before = nodes[owner].observations(&client, "ttl", "void").await;

    nodes[owner].signal("STOP");
    let others: Vec<&Node> =
        nodes.iter().enumerate().filter(|(i, _)| *i != owner).map(|(_, n)| n).collect();
    eventually("the others to see the paused member down", || {
        let (others, client) = (&others, &client);
        async move {
            for node in others {
                if node.members(client).await != Some(1) {
                    return false;
                }
            }
            true
        }
    })
    .await;
    nodes[owner].signal("CONT");
    eventually("the member to rejoin and read void ticks", || {
        let (nodes, client) = (&nodes, &client);
        async move {
            nodes[owner].members(client).await == Some(2)
                && nodes[owner].observations(client, "ttl", "void").await >= void_before + 2
        }
    })
    .await;
    // And then judged again, quietly, with nothing moved anywhere.
    let ticks = nodes[owner].ticks(&client).await;
    eventually("the evaluator to tick again after the pause", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.ticks(client).await >= ticks + 3 }
    })
    .await;
    for node in &nodes {
        for class in CLASSES {
            for direction in ["yield", "reclaim", "withdraw"] {
                assert_eq!(
                    node.transitions(&client, class, direction).await,
                    0,
                    "{} {class}",
                    node.name
                );
            }
            assert!(!node.stalled_by(&client, class, "local").await);
            assert!(!node.stalled_by(&client, class, "runtime").await);
        }
    }
}

// ---------------------------------------------------------------------------
// P7, P8: the runtime.
// ---------------------------------------------------------------------------

/// P7: `KIMMY_TEST_STALL_RUNTIME` on a TTL owner. The stall is really there (the
/// probe saw at least a second), the member is not responsive, and its class yields.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p7_a_member_whose_runtime_stalls_is_not_responsive_and_yields_what_it_owns() {
    let client = reqwest::Client::new();
    let mut nodes = cluster(&client, &[&[], &[], &[]]).await;
    let (_token, owner) = ttl_collection(&client, &nodes).await;

    nodes[owner].restart(false, &[SCALE, ("KIMMY_TEST_STALL_RUNTIME", "1500/2")]);
    nodes[owner].wait_ready(&client).await;
    let seen = std::sync::Arc::new(std::sync::Mutex::new((0.0f64, false)));
    eventually("the stall to be seen, and the member to yield", || {
        let (node, client, seen) = (&nodes[owner], &client, std::sync::Arc::clone(&seen));
        async move {
            // Read once per scrape, so each read can carry the worst since the last.
            let stall = node.series(client, "kimmy_runtime_stall_seconds").await;
            let responsive = node.series(client, "kimmy_runtime_responsive").await;
            {
                let mut seen = seen.lock().unwrap();
                if let Some(stall) = stall {
                    seen.0 = seen.0.max(stall);
                }
                seen.1 |= responsive == Some(0.0);
            }
            node.yielding(client, "ttl").await
        }
    })
    .await;
    let (worst, not_responsive) = *seen.lock().unwrap();
    assert!(
        worst >= 1.0,
        "the positive control: the probe saw a stall of at least a second ({worst})"
    );
    assert!(not_responsive, "the member was not responsive");
    assert!(
        nodes[owner].stalled_by(&client, "ttl", "runtime").await
            || nodes[owner].stalled_by(&client, "ttl", "local").await,
        "it is stalled"
    );
    for (i, node) in nodes.iter().enumerate() {
        if i != owner {
            assert!(!node.yielding(&client, "ttl").await, "only the stalled member yields");
            assert_eq!(node.n(&client, "kimmy_runtime_responsive").await, 1, "{}", node.name);
        }
    }
}

/// P8: the same stall on every member, with one subscription. No member is
/// responsive, so there is no target: nothing yields, and the owner says why.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p8_when_every_member_stalls_there_is_no_target_and_nothing_yields() {
    let client = reqwest::Client::new();
    let stall: &[(&str, &str)] = &[("KIMMY_TEST_STALL_RUNTIME", "1500/2")];
    let nodes = cluster(&client, &[stall, stall, stall]).await;
    let (hook, _seen) = receiver().await;
    let (_token, _ids) = subscriptions(&client, &nodes[0], hook, 1).await;
    eventually("one member to own the subscription", || {
        let (nodes, client) = (&nodes, &client);
        async move { owned_in_all(client, nodes, "webhooks").await >= 1 }
    })
    .await;
    let seen_stall = std::sync::Arc::new(std::sync::Mutex::new(vec![0.0f64; nodes.len()]));
    eventually("the owner to be stalled with no target to yield to", || {
        let (nodes, client, seen_stall) = (&nodes, &client, std::sync::Arc::clone(&seen_stall));
        async move {
            for (i, node) in nodes.iter().enumerate() {
                if let Some(s) = node.series(client, "kimmy_runtime_stall_seconds").await {
                    let mut seen = seen_stall.lock().unwrap();
                    seen[i] = seen[i].max(s);
                }
            }
            let mut found = false;
            for node in nodes {
                found |= node.suppressed(client, "webhooks", "no_target").await;
            }
            found
        }
    })
    .await;
    let seen_stall = seen_stall.lock().unwrap().clone();
    for (i, node) in nodes.iter().enumerate() {
        assert!(
            seen_stall[i] >= 1.0
                || node.series(&client, "kimmy_runtime_stall_seconds").await >= Some(1.0),
            "the stall control on {}: {}",
            node.name,
            seen_stall[i]
        );
        assert!(node.ticks(&client).await >= 5, "{} ticked", node.name);
        for class in CLASSES {
            assert_eq!(node.transitions(&client, class, "yield").await, 0, "{} {class}", node.name);
            assert!(!node.yielding(&client, class).await);
        }
    }
}

// ---------------------------------------------------------------------------
// P9, P10: the latch, and a bounded stall.
// ---------------------------------------------------------------------------

/// P9: a local failure of the webhook step on A from the start, and on B and C only
/// after 90 s of serving. A yields once and withdraws once (when the others fail
/// too, the latch is set); B and C never yield; and in the end nobody does, and
/// every member says why.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p9_a_fault_that_follows_the_data_costs_one_hand_off_and_then_the_latch_holds() {
    let client = reqwest::Client::new();
    let nodes = cluster(
        &client,
        &[
            &[("KIMMY_TEST_FAIL_STEP", "webhooks")],
            &[("KIMMY_TEST_FAIL_STEP", "webhooks:90")],
            &[("KIMMY_TEST_FAIL_STEP", "webhooks:90")],
        ],
    )
    .await;
    let (hook, _seen) = receiver().await;
    // Subscriptions, so every member would own some.
    let (_token, _ids) = subscriptions(&client, &nodes[1], hook, 12).await;
    let a = &nodes[0];

    eventually("A, failing locally from the start, to yield", || {
        let client = &client;
        async move { a.transitions(client, "webhooks", "yield").await >= 1 }
    })
    .await;
    // The positive controls: the induced condition was really present, and the
    // evaluator saw it.
    assert!(a.faults(&client, "webhooks", "local").await >= 6, "local faults rose on A");
    assert!(a.observations(&client, "webhooks", "bad").await >= 6);
    assert!(a.stalled_by(&client, "webhooks", "local").await);

    // B and C fail from 90 s on: now two or more are stalled locally in the class.
    eventually("B and C to be stalled locally, and A to have withdrawn", || {
        let (nodes, client) = (&nodes, &client);
        async move {
            nodes[1].stalled_by(client, "webhooks", "local").await
                && nodes[2].stalled_by(client, "webhooks", "local").await
                && nodes[0].transitions(client, "webhooks", "withdraw").await >= 1
        }
    })
    .await;
    assert_eq!(a.transitions(&client, "webhooks", "yield").await, 1, "exactly one yield on A");
    assert_eq!(a.transitions(&client, "webhooks", "withdraw").await, 1, "and one withdraw");
    for node in &nodes[1..] {
        assert_eq!(
            node.transitions(&client, "webhooks", "yield").await,
            0,
            "{} never yields",
            node.name
        );
    }
    // At the end nobody yields, and every member says it is the shared fault.
    eventually("every member to name the shared fault", || {
        let (nodes, client) = (&nodes, &client);
        async move {
            for node in nodes {
                if !node.suppressed(client, "webhooks", "shared_fault").await
                    || node.yielding(client, "webhooks").await
                {
                    return false;
                }
            }
            true
        }
    })
    .await;
    for node in &nodes {
        assert!(node.ticks(&client).await >= 10, "{} ticked", node.name);
    }
}

/// P10: a bounded stall of the TTL task. Exactly two transitions: a yield, then one
/// reclaim, at least R ticks after the stall ends.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p10_a_bounded_stall_is_one_yield_then_one_reclaim_after_r_good_ticks() {
    let client = reqwest::Client::new();
    let mut nodes = cluster(&client, &[&[], &[], &[]]).await;
    let (_token, owner) = ttl_collection(&client, &nodes).await;
    nodes[owner].restart(false, &[SCALE, ("KIMMY_TEST_KILL_TASK", "ttl_expiry:stall:30")]);
    nodes[owner].wait_ready(&client).await;
    let waited = tokio::time::timeout(
        Duration::from_secs(60),
        eventually("the yield", || {
            let (node, client) = (&nodes[owner], &client);
            async move { node.transitions(client, "ttl", "yield").await == 1 }
        }),
    )
    .await;
    if waited.is_err() {
        for node in &nodes {
            let body = client.get(node.url("/metrics")).send().await.unwrap().text().await.unwrap();
            let wanted: Vec<&str> = body
                .lines()
                .filter(|l| {
                    !l.starts_with('#')
                        && ["peer", "yield", "owner_class", "catching", "ttl_coll"]
                            .iter()
                            .any(|k| l.contains(k))
                })
                .collect();
            eprintln!("== {} metrics\n{}", node.name, wanted.join("\n"));
        }
        panic!("the owner never yielded; the metrics are above");
    }
    eventually("the stall to end", || {
        let node = &nodes[owner];
        async move { node.log().contains("test stall ended") }
    })
    .await;
    let ticks_at_end = nodes[owner].ticks(&client).await;
    eventually("the reclaim", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.transitions(client, "ttl", "reclaim").await == 1 }
    })
    .await;
    let ticks_at_reclaim = nodes[owner].ticks(&client).await;
    // R is 24 ticks, counted from the first good one after the stall; the end was
    // read a moment late, so allow the tick that was in flight.
    assert!(
        ticks_at_reclaim >= ticks_at_end + 22,
        "reclaimed {} ticks after the stall ended, and R is 24",
        ticks_at_reclaim - ticks_at_end
    );
    let log = nodes[owner].log();
    assert!(log.contains("r_ticks=24"), "{log}");
    // Exactly two transitions, over a window in which the evaluator went on ticking.
    assert_eq!(nodes[owner].transitions(&client, "ttl", "yield").await, 1);
    assert_eq!(nodes[owner].transitions(&client, "ttl", "reclaim").await, 1);
    assert_eq!(nodes[owner].transitions(&client, "ttl", "withdraw").await, 0);
    eventually("the member to own the expiry again", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.series(client, "kimmy_ttl_collections{state=\"owned\"}").await >= Some(1.0) }
    })
    .await;
}

// ---------------------------------------------------------------------------
// P12, P13: probation.
// ---------------------------------------------------------------------------

/// P12: after a storage failure the next start is in probation: it says so, and its
/// very first block yields every class, which its peers read.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p12_a_start_after_a_storage_failure_is_probation_and_yields_from_its_first_block() {
    let client = reqwest::Client::new();
    let mut nodes = cluster(&client, &[&[], &[], &[]]).await;
    let _ = ttl_collection(&client, &nodes).await;

    nodes[0].restart(false, &[SCALE, ("KIMMY_TEST_FAIL_STORAGE", "sync_data")]);
    // The node fails its first fsync; trigger one.
    if nodes[0].wait_ready_quietly(&client).await {
        let token = nodes[0].login(&client).await;
        let _ = client
            .post(nodes[0].url("/v1/db/shop/coll/sessions/docs"))
            .bearer_auth(&token)
            .json(&serde_json::json!({ "_id": 7, "seen": { "$date": 1 } }))
            .send()
            .await;
    }
    let status = nodes[0].wait_exit(Duration::from_secs(120));
    assert_eq!(status.code(), Some(70), "a storage I/O error exits 70: {status:?}");

    nodes[0].restart(false, &[SCALE]);
    nodes[0].wait_ready(&client).await;
    let log = nodes[0].log();
    assert!(log.contains("probation: yes"), "{log}");
    assert_eq!(nodes[0].n(&client, "kimmy_yield_probation").await, 1);
    // The first scrape the node answers already advertises the yield, with the
    // cause: the bits are published before its first tick.
    for class in PROBATED {
        assert!(nodes[0].yielding(&client, class).await, "{class} yields from the first block");
        assert!(nodes[0].stalled_by(&client, class, "probation").await, "{class}");
    }
    eventually("a peer to read the yield", || {
        let (peer, client) = (&nodes[1], &client);
        async move {
            peer.series(client, "kimmy_ownership_peers{state=\"ineligible_yielding\"}").await
                == Some(1.0)
        }
    })
    .await;
    // The ordinary reclaim follows, once its peers are fine and it has been quiet.
    eventually("the probation to end by reclaim", || {
        let (node, client) = (&nodes[0], &client);
        async move {
            for class in PROBATED {
                if node.transitions(client, class, "reclaim").await < 1 {
                    return false;
                }
            }
            true
        }
    })
    .await;
}

impl Node {
    /// Wait for `/readyz`, answering whether the node got there (it may exit first
    /// when a test switch stops it).
    async fn wait_ready_quietly(&self, client: &reqwest::Client) -> bool {
        let deadline = std::time::Instant::now() + patience();
        loop {
            if let Ok(res) = client.get(self.url("/readyz")).send().await
                && res.status().is_success()
            {
                return true;
            }
            if let Ok(Some(_)) = self.child.lock().unwrap().try_wait() {
                return false;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Write a `kimmy.last-start` that says the previous start was `ago` before now,
    /// with the sidecar's `written_by` and the inherited verdict `shutdown`.
    fn fabricate_last_start(&self, ago: Duration) {
        let sidecar = std::fs::read_to_string(self.data().join("kimmy.format")).expect("a sidecar");
        let written_by = sidecar
            .lines()
            .find_map(|l| l.strip_prefix("written_by = "))
            .expect("a written_by")
            .trim_matches('"')
            .to_string();
        let at_ms =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
                as u64
                - ago.as_millis() as u64;
        std::fs::write(
            self.data().join("kimmy.last-start"),
            format!(
                "boot = \"fabricated\"\nat_ms = {at_ms}\nversion = \"{written_by}\"\n\
                 inherited = \"shutdown\"\nwritten_by = \"{written_by}\"\n"
            ),
        )
        .unwrap();
    }
}

/// P13 and P13b: a SIGKILL after a long run, with the start record written after
/// the node serves, is an ordinary roll and no probation (the start says
/// `probation: no`, with the verdict `unclean` and a run of about an hour); the same
/// with a start a minute ago is probation (the positive control).
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p13_an_ordinary_roll_is_not_probation_and_a_recent_start_is() {
    let client = reqwest::Client::new();
    for (ago, expect, name) in
        [(Duration::from_secs(3600), false, "P13"), (Duration::from_secs(60), true, "P13b")]
    {
        let mut node = Node::standalone("solo", &[SCALE]);
        node.wait_ready(&client).await;
        // After the run's serving line: the run's own record is written by now, and
        // would overwrite one fabricated earlier.
        assert!(node.data().join("kimmy.last-start").exists(), "the start wrote its record");
        node.fabricate_last_start(ago);
        node.restart(true, &[SCALE]);
        node.wait_ready(&client).await;
        let log = node.log();
        let line = log
            .lines()
            .find(|l| l.contains("probation: "))
            .unwrap_or_else(|| panic!("{name}: no probation line:\n{log}"));
        assert!(
            line.contains(if expect { "probation: yes" } else { "probation: no" }),
            "{name}: {line}"
        );
        assert!(
            line.contains("verdict=\"unclean\""),
            "{name}: the control: the start read an unclean end: {line}"
        );
        assert_eq!(node.n(&client, "kimmy_yield_probation").await, u64::from(expect), "{name}");
        for class in PROBATED {
            assert_eq!(node.is_stalled(&client, class).await, expect, "{name} {class}");
            assert_eq!(
                node.stalled_by(&client, class, "probation").await,
                expect,
                "{name} {class}"
            );
        }
        if !expect {
            assert!(
                line.contains("previous_run_secs=3600") || line.contains("previous_run_secs=36"),
                "{line}"
            );
            for class in PROBATED {
                assert!(!node.yielding(&client, class).await);
                assert_eq!(node.transitions(&client, class, "yield").await, 0);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The test switches wait for the node to serve.
// ---------------------------------------------------------------------------

/// Nothing a test switch does happens before the node serves: the stall of a task and
/// the stall of the runtime are both announced after the line that says it serves,
/// so neither can turn a start into a startup failure. The WARN that names the
/// switches comes before it.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn the_test_switches_act_only_after_the_node_serves() {
    let client = reqwest::Client::new();
    let nodes = cluster(
        &client,
        &[
            &[],
            &[
                ("KIMMY_TEST_KILL_TASK", "ttl_expiry:stall:5"),
                ("KIMMY_TEST_STALL_RUNTIME", "500/2"),
                ("KIMMY_TEST_FAIL_STEP", "webhooks:0"),
            ],
        ],
    )
    .await;
    let node = &nodes[1];
    eventually("both switches to act", || {
        let log = node.log();
        async move { log.contains("test stall began") && log.contains("the runtime stall begins") }
    })
    .await;
    let log = node.log();
    let at = |needle: &str| {
        log.find(needle).unwrap_or_else(|| panic!("{needle:?} is not in the log:\n{log}"))
    };
    let serving = at("serving HTTP and WebSocket");
    let warned = at("a test switch is set that acts on a background task");
    assert!(warned < serving, "the switches are announced before the node serves");
    assert!(serving < at("test stall began"), "the task stall begins after serving");
    assert!(serving < at("the runtime stall begins"), "the runtime stall begins after serving");
}

// ---------------------------------------------------------------------------
// KIMMY_TEST_REFUSE_SYNC, and P11.
// ---------------------------------------------------------------------------

impl Node {
    async fn unconfirmed(&self, client: &reqwest::Client, class: &str) -> Option<f64> {
        self.series(client, &format!("kimmy_yield_unconfirmed_peers{{class=\"{class}\"}}")).await
    }
}

/// The switch refuses replication and nothing else: a peer's connection is accepted
/// and dropped, so the refusing node's own client API still answers (a login, a
/// write and a read all succeed), and SWIM still holds it live in the cluster. The
/// control is the same cluster without the switch, where sync works.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn the_refuse_sync_switch_drops_peers_and_leaves_the_client_api_alone() {
    let client = reqwest::Client::new();
    let nodes = cluster(&client, &[&[], &[], &[("KIMMY_TEST_REFUSE_SYNC", "1")]]).await;
    let refusing = &nodes[2];
    eventually("the switch to be armed, after the node serves", || {
        let log = refusing.log();
        async move { log.contains("the replication listener now drops every peer connection") }
    })
    .await;
    let log = refusing.log();
    assert!(
        log.find("serving HTTP and WebSocket").unwrap()
            < log.find("the replication listener now drops").unwrap(),
        "armed only after serving"
    );
    assert!(log.contains("KIMMY_TEST_REFUSE_SYNC"), "announced at WARN");
    // The client API of the refusing node works end to end.
    let token = refusing.login(&client).await;
    let made = client
        .post(refusing.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "local_only" }))
        .send()
        .await
        .unwrap();
    assert!(made.status().is_success(), "{:?}", made.status());
    let listed = client
        .get(refusing.url("/v1/db/shop/collections"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert!(listed.status().is_success());
    // And SWIM still holds it live: another member counts two live peers.
    eventually("the other members to still count two live peers", || {
        let (node, client) = (&nodes[0], &client);
        async move {
            let mut live = 0;
            for state in
                ["eligible", "ineligible_catching_up", "ineligible_yielding", "unknown", "stale"]
            {
                live +=
                    node.n(client, &format!("kimmy_ownership_peers{{state=\"{state}\"}}")).await;
            }
            live == 2
        }
    })
    .await;
    // Replication really is refused: what the refusing node wrote is not seen by
    // another member, which a working sync would have carried in a second or two.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let seen = client
        .get(nodes[0].url("/v1/db/shop/collections"))
        .bearer_auth(&nodes[0].login(&client).await)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!seen.contains("local_only"), "the write did not replicate: {seen}");
}

/// A bad value refuses the start.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn the_refuse_sync_switch_with_a_bad_value_refuses_the_start() {
    let node = Node::standalone("bad", &[("KIMMY_TEST_REFUSE_SYNC", "yes")]);
    let status = node.wait_exit(Duration::from_secs(60));
    assert!(!status.success(), "{status:?}");
    assert!(node.log().contains("KIMMY_TEST_REFUSE_SYNC"), "{}", node.log());
}

/// P11: a peer that is live in SWIM and never completes a contact. The stalled TTL
/// owner yields, but **keeps owning**, because that peer never confirms; its WARN
/// names the peer; and `kimmy_yield_unconfirmed_peers` is 1. Controls: the stall
/// really happened and the member yields, the other peer is a target and has
/// confirmed (so the one unconfirmed is exactly the refusing one), the refusing peer
/// is live, and the evaluator ticked.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p11_a_yield_the_refusing_peer_never_confirms_keeps_the_owner_owning() {
    let client = reqwest::Client::new();
    let mut nodes = cluster(&client, &[&[], &[], &[]]).await;
    let (_token, owner) = ttl_collection(&client, &nodes).await;
    // Who hears whom is fixed by the seeds: A and B seed each other and C seeds A,
    // and a member dials only its seeds, so C hears only A, B hears only A, and A
    // hears both. The refusing peer must not be the only member the yielder can hear:
    // the hub (A) refuses only when the yielder is A itself.
    let refusing = if owner == 0 { 1 } else { 3 - owner };
    let target = (0..nodes.len()).find(|i| *i != owner && *i != refusing).unwrap();
    let others = [refusing, target];
    let refusing_id = nodes[refusing].node_id(&client).await.to_string();
    let target_id = nodes[target].node_id(&client).await.to_string();
    nodes[refusing].restart(false, &[SCALE, ("KIMMY_TEST_REFUSE_SYNC", "1")]);
    nodes[refusing].wait_ready(&client).await;
    nodes[owner].restart(false, &[SCALE, ("KIMMY_TEST_KILL_TASK", "ttl_expiry:stall")]);
    nodes[owner].wait_ready(&client).await;
    // The class is stalled and the member says it yields.
    eventually("the owner to say it yields", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.yielding(client, "ttl").await }
    })
    .await;
    assert!(nodes[owner].log().contains("test stall began"), "control: the stall happened");
    // The WARN names the refusing peer.
    eventually("the WARN to name the peer that never echoes", || {
        let (log, id) = (nodes[owner].log(), refusing_id.clone());
        async move {
            log.lines().any(|l| l.contains("has not echoed this member's block") && l.contains(&id))
        }
    })
    .await;
    eventually("exactly one peer to be unconfirmed", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.unconfirmed(client, "ttl").await == Some(1.0) }
    })
    .await;
    // It keeps owning, and says so: the member that yields still holds the
    // collection's expiry in its own view (a peer that has read the yield takes it up
    // too, which is the designed overlap), for a window of M ticks and past the lease at
    // this scale, while the refusing peer never echoes.
    let ticks = nodes[owner].ticks(&client).await;
    eventually("the evaluator to tick M more times", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.ticks(client).await >= ticks + 12 }
    })
    .await;
    tokio::time::sleep(Duration::from_secs(20)).await;
    assert_eq!(
        nodes[owner].series(&client, "kimmy_ttl_collections{state=\"owned\"}").await,
        Some(1.0),
        "the yielder keeps owning while a peer has not confirmed"
    );
    // Control: the other peer did read it and confirmed, so the one unconfirmed peer
    // is exactly the refusing one.
    assert_eq!(nodes[others[1]].unconfirmed(&client, "ttl").await, Some(0.0));
    // And the WARN never named it: a current-version peer that echoes within a
    // sync round or two is not "a peer that has not echoed", and the first WARN
    // for a peer waits a lease for exactly that. This is a guard, not the red
    // proof: whether the first sweep beats the echo is a race (it went red in one
    // of five runs with the grace removed), and the proof is
    // `a_sweep_before_any_echo_names_nobody_and_later_only_the_peer_that_never_echoes`
    // in `kimmy-cluster`, which drives that order. The positive controls are the
    // WARN that did name the refusing peer, over the same log, and the target's
    // confirmation just above.
    let warns: Vec<String> = nodes[owner]
        .log()
        .lines()
        .filter(|l| l.contains("has not echoed this member's block"))
        .map(str::to_owned)
        .collect();
    assert!(warns.iter().any(|l| l.contains(&refusing_id)), "control: the refusing peer is named");
    assert!(
        !warns.iter().any(|l| l.contains(&target_id)),
        "a peer that echoed was named as one that has not: {warns:?}"
    );
    assert_eq!(nodes[owner].series(&client, "kimmy_yielding{class=\"ttl\"}").await, Some(1.0));
    assert_eq!(nodes[owner].unconfirmed(&client, "ttl").await, Some(1.0));
    // Controls: the refusing peer is live (SWIM), and its refusal is armed.
    assert!(nodes[refusing].log().contains("the replication listener now drops"));
    assert_eq!(nodes[owner].n(&client, "kimmy_ownership_peers{state=\"stale\"}").await, 0);
}

// ---------------------------------------------------------------------------
// P14, P15: the off switch, and a wedged evaluator.
// ---------------------------------------------------------------------------

/// P14a: `KIMMY_OWNERSHIP_YIELD=off` on a stalled owner. It judges and advertises
/// `stalled`, and never yields; the reason says why.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p14_with_the_switch_off_a_stalled_owner_advertises_stalled_and_never_yields() {
    let client = reqwest::Client::new();
    let mut nodes = cluster(&client, &[&[], &[], &[]]).await;
    let (_token, owner) = ttl_collection(&client, &nodes).await;
    nodes[owner].restart(
        false,
        &[SCALE, ("KIMMY_OWNERSHIP_YIELD", "off"), ("KIMMY_TEST_KILL_TASK", "ttl_expiry:stall")],
    );
    nodes[owner].wait_ready(&client).await;
    eventually("the member to be stalled and say why it does not yield", || {
        let (node, client) = (&nodes[owner], &client);
        async move {
            node.is_stalled(client, "ttl").await
                && node.suppressed(client, "ttl", "switched_off").await
        }
    })
    .await;
    // Controls: the switch is off, the stall really began, and the evaluator went on
    // ticking well past the time a yield takes.
    assert_eq!(nodes[owner].n(&client, "kimmy_ownership_yield_enabled").await, 0);
    assert!(nodes[owner].log().contains("test stall began"));
    let ticks = nodes[owner].ticks(&client).await;
    eventually("ten more ticks", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.ticks(client).await >= ticks + 10 }
    })
    .await;
    for class in CLASSES {
        assert!(!nodes[owner].yielding(&client, class).await);
        assert_eq!(nodes[owner].transitions(&client, class, "yield").await, 0);
    }
}

/// P14b: a member with the switch off still honours a peer's yield.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p14b_a_member_with_the_switch_off_still_honours_its_peers_yield() {
    let client = reqwest::Client::new();
    let mut nodes = cluster(&client, &[&[], &[("KIMMY_OWNERSHIP_YIELD", "off")], &[]]).await;
    let (_token, owner) = ttl_collection(&client, &nodes).await;
    // The stalling member is one with the switch on.
    let stalled = if owner == 1 { 0 } else { owner };
    if stalled == owner {
        nodes[stalled].restart(false, &[SCALE, ("KIMMY_TEST_KILL_TASK", "ttl_expiry:stall")]);
        nodes[stalled].wait_ready(&client).await;
        eventually("the stalled member to yield", || {
            let (node, client) = (&nodes[stalled], &client);
            async move { node.yielding(client, "ttl").await }
        })
        .await;
        eventually("the member with the switch off to read it", || {
            let (peer, client) = (&nodes[1], &client);
            async move {
                peer.series(client, "kimmy_ownership_peers{state=\"ineligible_yielding\"}").await
                    >= Some(1.0)
            }
        })
        .await;
        assert_eq!(nodes[1].n(&client, "kimmy_ownership_yield_enabled").await, 0);
    }
}

/// P14c: a value that is not on or off refuses the start, naming the variable.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p14c_a_bad_switch_value_refuses_the_start() {
    let node = Node::standalone("solo", &[("KIMMY_OWNERSHIP_YIELD", "maybe")]);
    let status = node.wait_exit(Duration::from_secs(60));
    assert!(!status.success(), "the start was refused: {status:?}");
    let stderr = std::fs::read_to_string(node.dir.path().join("stderr.log")).unwrap_or_default();
    assert!(stderr.contains("KIMMY_OWNERSHIP_YIELD"), "{stderr}");
    assert!(stderr.contains("off"), "and names the values: {stderr}");
    // And an over-large runtime stall refuses it too.
    let node = Node::standalone("solo-stall", &[("KIMMY_TEST_STALL_RUNTIME", "3001/5")]);
    let status = node.wait_exit(Duration::from_secs(60));
    assert!(!status.success());
    let stderr = std::fs::read_to_string(node.dir.path().join("stderr.log")).unwrap_or_default();
    assert!(stderr.contains("KIMMY_TEST_STALL_RUNTIME"), "{stderr}");
}

/// P15: a member whose evaluator has stopped ticking advertises every class as
/// `unknown` after two ticks, and is no target for a stalled peer: two members, B
/// with `yield_evaluator:stall` and A failing locally with the only subscription.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p15_a_member_whose_evaluator_is_wedged_is_unknown_and_no_target() {
    let client = reqwest::Client::new();
    let nodes = cluster(
        &client,
        &[
            &[("KIMMY_TEST_FAIL_STEP", "webhooks:45")],
            &[("KIMMY_TEST_KILL_TASK", "yield_evaluator:stall")],
        ],
    )
    .await;
    let (hook, _seen) = receiver().await;
    let (token, _ids) = subscriptions(&client, &nodes[0], hook, 1).await;
    // A owns the only subscription (A and B are the only candidates, and B's block
    // says nothing wrong yet: it is the one whose evaluator wedges).
    let a = &nodes[0];
    let b = &nodes[1];
    eventually("B to advertise every class unknown", || {
        let (a, client, token, b) = (a, &client, token.clone(), b);
        async move {
            let topology: serde_json::Value = client
                .get(a.url("/v1/topology"))
                .bearer_auth(&token)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let id = b.node_id(client).await.to_string();
            topology["nodes"].as_array().unwrap().iter().any(|n| {
                n["node"] == id.as_str()
                    && n["classState"]
                        == serde_json::json!({
                            "ttl": "unknown", "webhooks": "unknown", "embeddings": "unknown"
                        })
            })
        }
    })
    .await;
    assert!(b.log().contains("test stall began"), "the control: B's evaluator stalled");
    eventually("A to be stalled locally with nobody to yield to", || {
        let (a, client) = (a, &client);
        async move {
            a.stalled_by(client, "webhooks", "local").await
                && a.suppressed(client, "webhooks", "no_target").await
        }
    })
    .await;
    assert_eq!(a.transitions(&client, "webhooks", "yield").await, 0);
    assert!(a.faults(&client, "webhooks", "local").await >= 6, "the induced fault was present");
    assert!(
        !a.suppressed(&client, "webhooks", "shared_fault").await,
        "B is not stalled, so the latch cannot be what holds A"
    );
}

/// P16: a member whose writer is held past the wedge bound (ADR-220). One TTL
/// collection, and `KIMMY_TEST_HOLD_WRITER=<ms>,replication` on its owner, which
/// takes the writer once it serves and keeps it for longer than the bound. The
/// owner says so (`kimmy_writer_wedged`, the hold's age), a peer's `/v1/topology`
/// lists it `degraded: "writer"` while the hold lasts, it yields the expiry to
/// another holder, and when the hold ends the flag clears, the topology entry
/// loses `degraded`, and the member reclaims after R good ticks.
///
/// The bound is 60 s and is not scaled by `KIMMY_TEST_YIELD_SCALE`, so the hold is
/// 75 s. The hold is taken from the moment the node serves: the member's HTTP
/// reads go on, and its writes wait, so everything asked of the wedged member here
/// is a scrape.
#[tokio::test]
#[ignore = "boots real nodes; run with --ignored"]
async fn p16_a_member_whose_writer_is_wedged_says_so_yields_and_reclaims_after_the_hold() {
    const HOLD_MS: &str = "75000,replication";
    let client = reqwest::Client::new();
    let mut nodes = cluster(&client, &[&[], &[], &[]]).await;
    let (_token, owner) = ttl_collection(&client, &nodes).await;
    let peer = (owner + 1) % nodes.len();
    let owner_id = nodes[owner].node_id(&client).await.to_string();
    // Controls: before the hold nothing is wedged anywhere, and nobody is degraded.
    for node in &nodes {
        assert_eq!(node.required(&client, "kimmy_writer_wedged").await, 0, "{}", node.name);
    }

    nodes[owner].restart(false, &[SCALE, ("KIMMY_TEST_HOLD_WRITER", HOLD_MS)]);
    nodes[owner].wait_ready(&client).await;

    // The owner reads its own hold as old, then as past the bound.
    eventually("the owner to see the writer wedged", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.series(client, "kimmy_writer_wedged").await == Some(1.0) }
    })
    .await;
    let age = nodes[owner]
        .series(&client, "kimmy_writer_hold_age_seconds")
        .await
        .expect("the hold age is scraped");
    assert!(age > 60.0, "wedged means held past the bound; the age is {age}");
    assert!(nodes[owner].log().contains("test switch armed: the writer is held"));
    assert!(nodes[owner].log().contains("held past the wedge bound"), "{}", nodes[owner].log());

    // A peer's topology lists it degraded, and lists nobody else so.
    let peer_token = nodes[peer].login(&client).await;
    let degraded_of = |client: reqwest::Client, url: String, token: String| async move {
        let topology: serde_json::Value =
            client.get(url).bearer_auth(&token).send().await.unwrap().json().await.unwrap();
        topology["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| (n["node"].as_str().unwrap().to_string(), n.get("degraded").cloned()))
            .collect::<Vec<_>>()
    };
    eventually("a peer to list the owner degraded for its writer", || {
        let (client, url, token, owner_id) =
            (client.clone(), nodes[peer].url("/v1/topology"), peer_token.clone(), owner_id.clone());
        async move {
            degraded_of(client, url, token)
                .await
                .iter()
                .any(|(node, degraded)| *node == owner_id && *degraded == Some("writer".into()))
        }
    })
    .await;
    let listed =
        degraded_of(client.clone(), nodes[peer].url("/v1/topology"), peer_token.clone()).await;
    for (node, degraded) in &listed {
        assert_eq!(
            degraded.is_some(),
            *node == owner_id,
            "only the owner is degraded: {node} {degraded:?}"
        );
    }

    // It yields the expiry, and exactly one other member owns it.
    eventually("the wedged owner to yield its expiry", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.yielding(client, "ttl").await }
    })
    .await;
    assert!(nodes[owner].stalled_by(&client, "ttl", "local").await, "a wedge is a local cause");
    eventually("one other member to own the expiry, and the wedged one not", || {
        let (nodes, client) = (&nodes, &client);
        async move {
            let mut owners = Vec::new();
            for (i, node) in nodes.iter().enumerate() {
                if node.series(client, "kimmy_ttl_collections{state=\"owned\"}").await >= Some(1.0)
                {
                    owners.push(i);
                }
            }
            owners.len() == 1 && owners[0] != owner
        }
    })
    .await;
    // Controls over the same window: the evaluator went on ticking, and the others
    // are neither wedged nor yielding.
    assert!(nodes[owner].ticks(&client).await >= 20);
    for (i, node) in nodes.iter().enumerate() {
        if i != owner {
            assert_eq!(node.required(&client, "kimmy_writer_wedged").await, 0, "{}", node.name);
            assert!(!node.yielding(&client, "ttl").await, "{}", node.name);
        }
    }

    // The hold ends: the flag clears, the entry loses `degraded`, and the member
    // reclaims after R good ticks.
    eventually("the hold to end", || {
        let node = &nodes[owner];
        async move { node.log().contains("test switch ended: the writer is let go") }
    })
    .await;
    eventually("the owner to read the writer free again", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.series(client, "kimmy_writer_wedged").await == Some(0.0) }
    })
    .await;
    eventually("a peer to list the owner without `degraded`", || {
        let (client, url, token, owner_id) =
            (client.clone(), nodes[peer].url("/v1/topology"), peer_token.clone(), owner_id.clone());
        async move {
            degraded_of(client, url, token)
                .await
                .iter()
                .any(|(node, degraded)| *node == owner_id && degraded.is_none())
        }
    })
    .await;
    eventually("the reclaim", || {
        let (node, client) = (&nodes[owner], &client);
        async move { node.transitions(client, "ttl", "reclaim").await == 1 }
    })
    .await;
    assert_eq!(nodes[owner].transitions(&client, "ttl", "yield").await, 1);
    assert!(nodes[owner].log().contains("r_ticks=24"), "{}", nodes[owner].log());
}
