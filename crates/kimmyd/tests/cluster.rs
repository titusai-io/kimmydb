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

const ROOT_PASSWORD: &str = "harness-root-password";
const JWT_SECRET: &str = "a-shared-harness-jwt-secret-value";
const CLUSTER_SECRET: &str = "a-shared-harness-cluster-secret";

/// How long a condition may take before the harness gives up on it.
///
/// Generous on purpose: these tests run on loaded CI machines, and a timeout
/// that flakes teaches people to rerun failures instead of reading them. The
/// happy path resolves in a few seconds; the budget only matters when
/// something is genuinely wrong.
const PATIENCE: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(250);

/// A free localhost port.
///
/// Bind-then-drop has an inherent race, but the alternative — a hardcoded
/// range — collides with everything else on a shared CI machine. Losing the
/// race fails the node's startup loudly, which `wait_ready` surfaces.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// One spawned `kimmyd`, killed on drop.
struct Node {
    name: &'static str,
    child: Child,
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
    fn spawn(name: &'static str, cluster: u16, seeds: &[u16]) -> Node {
        let dir = tempfile::tempdir().unwrap();
        let http = free_port();
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

        Node { name, child, http, dir }
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
        let deadline = std::time::Instant::now() + PATIENCE;
        loop {
            if let Ok(res) = client.get(self.url("/healthz")).send().await
                && res.status().is_success()
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{}: never became healthy; its stderr is in {:?}",
                self.name,
                self.dir.path().join("stderr.log"),
            );
            tokio::time::sleep(POLL).await;
        }
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

    /// A gauge from `/metrics`, scraped like an operator would.
    async fn gauge(&self, client: &reqwest::Client, name: &str) -> Option<u64> {
        let body = client.get(self.url("/metrics")).send().await.ok()?.text().await.ok()?;
        let prefix = format!("{name} ");
        body.lines()
            .find(|l| l.starts_with(&prefix))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
    }

    async fn members_gauge(&self, client: &reqwest::Client) -> Option<u64> {
        self.gauge(client, "kimmy_cluster_members").await
    }

    fn signal(&self, sig: &str) {
        let status = Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(self.child.id().to_string())
            .status()
            .unwrap();
        assert!(status.success(), "kill -{sig} {}", self.child.id());
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // SIGKILL rather than a graceful stop: a stopped (SIGSTOP) child
        // ignores everything else, and these are scratch nodes. Unchecked,
        // unlike `signal` — the child may already be dead by a test's own
        // hand, and panicking in a drop during unwind aborts the test binary.
        let _ = Command::new("kill").arg("-KILL").arg(self.child.id().to_string()).status();
        let _ = self.child.wait();
    }
}

/// Wait until `condition` holds, or fail with `what` after [`PATIENCE`].
async fn eventually<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + PATIENCE;
    loop {
        if condition().await {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "gave up waiting for: {what}");
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
    let (pa, pb, pc) = (free_port(), free_port(), free_port());
    let a = Node::spawn("node-a", pa, &[pb]);
    let b = Node::spawn("node-b", pb, &[pa]);
    let c = Node::spawn("node-c", pc, &[pa]);
    a.wait_ready(client).await;
    b.wait_ready(client).await;
    c.wait_ready(client).await;
    (a, b, c)
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
    // gauge measures unapplied peer history, and there is none left. This is
    // asserted after convergence rather than before because the gauge only
    // updates when a sync round reaches a peer.
    eventually("every node to report zero replication lag", || {
        let client = &client;
        let nodes = [&a, &b, &c];
        async move {
            for node in nodes {
                if node.gauge(client, "kimmy_replication_lag_seconds").await != Some(0) {
                    return false;
                }
            }
            true
        }
    })
    .await;
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
/// produces one delete cluster-wide, not one per node.**
///
/// Every node runs its own expiry timer, so the naive design has all three
/// notice the same document and issue three deletes. Those converge under
/// last-writer-wins, so correctness alone cannot tell the two designs apart —
/// only counting can, which is why `kimmy_ttl_expired_total` exists. Summed
/// across the cluster it must read exactly 1.
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

    // Dated in the past, so it is already due the moment it lands.
    client
        .post(a.url("/v1/db/shop/coll/sessions/docs"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "_id": 1, "seen": { "$date": 0 } }))
        .send()
        .await
        .unwrap();

    // Gone everywhere: the owner deletes, and the delete replicates.
    for node in [&a, &b, &c] {
        let token = node.login(&client).await;
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
    let event = tokio::time::timeout(PATIENCE, async {
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
