//! What a client can actually get, over a socket.
//!
//! # Why this exists
//!
//! Every other benchmark in this repository is taken at the storage engine, so
//! nothing in [Benchmarks](../../../docs/benchmarks.md) includes the things a
//! client pays for: JSON and Extended JSON conversion in both directions,
//! per-request token verification, HTTP framing, TLS, and the contention of
//! several clients at once. Until this existed there was **no honest answer to
//! "what throughput can a client expect"** — and that file's retracted-figure
//! note is what happened the last time one was quoted anyway.
//!
//! # It drives the shipped binary
//!
//! `CARGO_BIN_EXE_kimmyd` under the bench profile is the release-optimized
//! daemon, spawned as a child process and reached over a real socket, with TLS
//! configured through the real config file. Nothing here reaches into the
//! library: an in-process router would measure a program nobody runs, and the
//! cluster harness already established that a real node is the only thing
//! whose behaviour counts.
//!
//! # Method, and the parts of it that matter
//!
//! - **Warm-up is discarded.** A first request against a fresh node is slow for
//!   reasons that are not the feature under test — the cursor drive measured
//!   20.7 ms on page zero and sub-millisecond after. Each cell runs a fixed
//!   warm-up before the clock starts.
//! - **Percentiles, not just a mean.** A mean throughput hides the tail, and
//!   the tail is what a client experiences as "the database is slow".
//! - **The load generator shares this machine with the server**, so both are
//!   competing for the same cores. That understates the server and is stated
//!   rather than corrected for; the numbers are useful as ratios.
//! - **Recorded, not gated**, like every other benchmark here. Paste the table
//!   into `docs/benchmarks.md` with its conditions.
//!
//! ```bash
//! cargo bench -p kimmyd --bench http
//! KIMMY_BENCH_CONCURRENCY=1,8,32,64 KIMMY_BENCH_MS=3000 cargo bench -p kimmyd --bench http
//! ```

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const ROOT_PASSWORD: &str = "bench-root-password";
const JWT_SECRET: &str = "a-bench-secret-long-enough-to-be-accepted";
/// Documents seeded before reads are measured.
const SEED: usize = 10_000;
/// Requests each cell throws away before the clock starts.
const WARMUP: usize = 200;

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
    listener.local_addr().unwrap().port()
}

/// One spawned `kimmyd`, killed on drop.
struct Node {
    child: Child,
    base: String,
    _dir: tempfile::TempDir,
}

impl Node {
    fn spawn(tls: bool) -> Node {
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        let mut tls_block = String::new();

        if tls {
            let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
            let cert = dir.path().join("server.crt");
            let key = dir.path().join("server.key");
            std::fs::write(&cert, issued.cert.pem()).unwrap();
            std::fs::write(&key, issued.signing_key.serialize_pem()).unwrap();
            tls_block = format!(
                "\n[server.tls]\ncert_file = \"{}\"\nkey_file = \"{}\"\n",
                cert.display(),
                key.display()
            );
        }

        let config = format!(
            r#"
[server]
bind = "127.0.0.1:{port}"
# Off: this measures the client protocol, and an unused endpoint is a
# difference between the thing measured and the thing described.
mcp = false

[storage]
data_dir = "{data}"

[auth]
jwt_secret = "{JWT_SECRET}"
{tls_block}"#,
            data = dir.path().join("data").display(),
        );
        let config_path = dir.path().join("kimmy.toml");
        std::fs::write(&config_path, config).unwrap();

        let log = std::fs::File::create(dir.path().join("node.log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_kimmyd"))
            .arg("--config")
            .arg(&config_path)
            .env("KIMMY_ROOT_PASSWORD", ROOT_PASSWORD)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawning kimmyd");

        let scheme = if tls { "https" } else { "http" };
        Node { child, base: format!("{scheme}://localhost:{port}"), _dir: dir }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// What one cell of the matrix measured.
struct Measurement {
    scenario: &'static str,
    /// Carried from the scenario so the report can separate reads from writes.
    /// Averaging them together would hide the finding: reads scale with
    /// clients and writes cannot, because redb has one writer.
    writes: bool,
    transport: &'static str,
    concurrency: usize,
    requests: usize,
    elapsed: Duration,
    /// Every request's latency, sorted. Kept whole rather than summarized as it
    /// goes, because a percentile computed from a running summary is a
    /// different number from the one a client would report.
    latencies: Vec<Duration>,
}

impl Measurement {
    fn per_second(&self) -> f64 {
        self.requests as f64 / self.elapsed.as_secs_f64()
    }

    fn percentile(&self, p: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let rank = ((self.latencies.len() as f64 - 1.0) * p).round() as usize;
        self.latencies[rank]
    }
}

fn ms(d: Duration) -> String {
    format!("{:.2}", d.as_secs_f64() * 1000.0)
}

/// A request the harness can repeat.
#[derive(Clone)]
struct Scenario {
    name: &'static str,
    method: reqwest::Method,
    /// Given the node's base URL and a per-request sequence number, the URL
    /// and body to send. Takes the base rather than the node so a worker needs
    /// nothing but a string — the child process belongs to the harness.
    build: fn(&str, usize) -> (String, Option<Value>),
    /// Whether repeating it writes. Writes are reported separately because
    /// redb has a single writer: concurrency cannot help them, and averaging
    /// them together with reads would hide exactly that.
    writes: bool,
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "point read",
            method: reqwest::Method::GET,
            build: |base, i| (format!("{base}/v1/db/bench/coll/docs/docs/{}", i % SEED), None),
            writes: false,
        },
        Scenario {
            name: "find, page of 100",
            method: reqwest::Method::POST,
            build: |base, _| {
                (
                    format!("{base}/v1/db/bench/coll/docs/find"),
                    Some(json!({ "filter": { "qty": { "$gte": 0 } }, "limit": 100 })),
                )
            },
            writes: false,
        },
        Scenario {
            name: "count, whole collection",
            method: reqwest::Method::POST,
            build: |base, _| {
                (format!("{base}/v1/db/bench/coll/docs/count"), Some(json!({ "filter": {} })))
            },
            writes: false,
        },
        Scenario {
            name: "insert one",
            method: reqwest::Method::POST,
            build: |base, i| {
                (
                    format!("{base}/v1/db/bench/coll/writes/docs"),
                    Some(document(1_000_000 + i as i64)),
                )
            },
            writes: true,
        },
        Scenario {
            name: "bulk insert 100",
            method: reqwest::Method::POST,
            build: |base, i| {
                let first = 10_000_000 + (i as i64) * 100;
                let batch: Vec<Value> = (0..100).map(|n| document(first + n)).collect();
                (format!("{base}/v1/db/bench/coll/writes/bulk"), Some(json!(batch)))
            },
            writes: true,
        },
    ]
}

/// A document of a size a real application might store — the same shape the
/// storage benchmarks use, so the two sets of numbers are comparable.
fn document(id: i64) -> Value {
    json!({
        "_id": id,
        "sku": format!("SKU-{id:08}"),
        "name": "a product with a name of unremarkable length",
        "qty": id % 500,
        "tags": ["alpha", "beta", "gamma"],
        "address": { "city": "Springfield", "zip": "12345" },
    })
}

async fn wait_ready(client: &reqwest::Client, node: &Node) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(res) = client.get(node.url("/healthz")).send().await
            && res.status().is_success()
        {
            return;
        }
        assert!(Instant::now() < deadline, "the node never became healthy");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn login(client: &reqwest::Client, node: &Node) -> String {
    let body: Value = client
        .post(node.url("/v1/auth/login"))
        .json(&json!({ "user": "root", "password": ROOT_PASSWORD }))
        .send()
        .await
        .expect("login")
        .json()
        .await
        .expect("a token");
    body["token"].as_str().expect("a token").to_string()
}

async fn seed(client: &reqwest::Client, node: &Node, token: &str) {
    for coll in ["docs", "writes"] {
        let res = client
            .post(node.url("/v1/db/bench/collections"))
            .bearer_auth(token)
            .json(&json!({ "name": coll }))
            .send()
            .await
            .expect("creating a collection");
        assert!(res.status().is_success(), "creating {coll}: {}", res.status());
    }

    // In batches, because seeding 10,000 documents one at a time is minutes of
    // commits and none of it is what is being measured.
    for chunk in (0..SEED as i64).collect::<Vec<_>>().chunks(500) {
        let batch: Vec<Value> = chunk.iter().map(|i| document(*i)).collect();
        let res = client
            .post(node.url("/v1/db/bench/coll/docs/bulk"))
            .bearer_auth(token)
            .json(&batch)
            .send()
            .await
            .expect("seeding");
        assert!(res.status().is_success(), "seeding: {}", res.status());
    }
}

/// Run one cell: `concurrency` clients hammering one scenario for `duration`.
async fn measure(
    client: &reqwest::Client,
    node: &Node,
    token: &str,
    scenario: &Scenario,
    transport: &'static str,
    concurrency: usize,
    duration: Duration,
) -> Measurement {
    // Warm-up, discarded. Connection setup, TLS handshakes, the first read of
    // a page from disk and the token cache all land here rather than in the
    // measurement.
    for i in 0..WARMUP {
        let (url, body) = (scenario.build)(&node.base, i);
        let mut req = client.request(scenario.method.clone(), url).bearer_auth(token);
        if let Some(body) = body {
            req = req.json(&body);
        }
        let _ = req.send().await;
    }

    let counter = Arc::new(AtomicUsize::new(WARMUP * 2));
    let started = Instant::now();
    let deadline = started + duration;

    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let client = client.clone();
        let token = token.to_string();
        let scenario = scenario.clone();
        let counter = Arc::clone(&counter);
        let base = node.base.clone();
        workers.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            while Instant::now() < deadline {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                let (url, body) = (scenario.build)(&base, i);
                let mut req = client.request(scenario.method.clone(), url).bearer_auth(&token);
                if let Some(body) = body {
                    req = req.json(&body);
                }
                let at = Instant::now();
                match req.send().await {
                    Ok(res) => {
                        // Drain the body: a client's cost includes reading the
                        // answer, and not reading it would measure the server
                        // writing into a socket buffer.
                        let status = res.status();
                        let _ = res.bytes().await;
                        if status.is_success() || status == 409 {
                            latencies.push(at.elapsed());
                        }
                    }
                    Err(_) => break,
                }
            }
            latencies
        }));
    }

    let mut latencies = Vec::new();
    for worker in workers {
        latencies.extend(worker.await.expect("a worker"));
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();

    Measurement {
        scenario: scenario.name,
        writes: scenario.writes,
        transport,
        concurrency,
        requests: latencies.len(),
        elapsed,
        latencies,
    }
}

fn report(measurements: &[Measurement]) {
    // Reads and writes in separate tables rather than one, because they answer
    // different questions: how far reads scale, and where writes stop.
    for (writes, heading) in [(false, "Reads"), (true, "Writes")] {
        println!("\n### {heading}\n");
        println!("| Scenario | Transport | Clients | req/s | p50 ms | p95 ms | p99 ms |");
        println!("|---|---|---:|---:|---:|---:|---:|");
        for m in measurements.iter().filter(|m| m.writes == writes) {
            println!(
                "| {} | {} | {} | **{:.0}** | {} | {} | {} |",
                m.scenario,
                m.transport,
                m.concurrency,
                m.per_second(),
                ms(m.percentile(0.50)),
                ms(m.percentile(0.95)),
                ms(m.percentile(0.99)),
            );
        }
    }
    println!();
}

#[tokio::main]
async fn main() {
    // `cargo bench` passes `--bench` to every target; there are no other flags
    // to parse, so anything else is a filter this harness does not implement.
    let duration = Duration::from_millis(
        std::env::var("KIMMY_BENCH_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(2_000),
    );
    let concurrencies: Vec<usize> = std::env::var("KIMMY_BENCH_CONCURRENCY")
        .ok()
        .map(|v| v.split(',').filter_map(|n| n.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1, 8, 32]);

    println!("kimmyd HTTP benchmark — the shipped binary, over a real socket");
    println!(
        "  {SEED} documents seeded · {} ms per cell after {WARMUP} discarded warm-up requests",
        duration.as_millis()
    );
    println!("  the load generator shares this machine with the server");

    let mut measurements = Vec::new();

    for (tls, transport) in [(false, "plaintext"), (true, "TLS")] {
        let node = Node::spawn(tls);
        let client = reqwest::Client::builder()
            // The certificate is generated per run, so trusting it by name is
            // not possible and not the point: this measures the cost of TLS,
            // not of a PKI.
            .danger_accept_invalid_certs(true)
            .build()
            .expect("a client");

        wait_ready(&client, &node).await;
        let token = login(&client, &node).await;
        seed(&client, &node, &token).await;

        for scenario in scenarios() {
            for &concurrency in &concurrencies {
                measurements.push(
                    measure(&client, &node, &token, &scenario, transport, concurrency, duration)
                        .await,
                );
            }
        }
    }

    report(&measurements);

    // The two findings worth stating in the output itself, because they are
    // what the numbers are *for*.
    let read = measurements.iter().filter(|m| m.scenario == "point read");
    if let (Some(one), Some(many)) = (
        read.clone().find(|m| m.concurrency == 1 && m.transport == "plaintext"),
        read.clone().max_by_key(|m| (m.transport == "plaintext", m.concurrency)),
    ) {
        println!(
            "reads scale with clients: {:.0}/s at 1 client → {:.0}/s at {}",
            one.per_second(),
            many.per_second(),
            many.concurrency,
        );
    }
    let writes = measurements.iter().filter(|m| m.scenario == "insert one");
    if let (Some(one), Some(many)) = (
        writes.clone().find(|m| m.concurrency == 1 && m.transport == "plaintext"),
        writes.clone().max_by_key(|m| (m.transport == "plaintext", m.concurrency)),
    ) {
        println!(
            "writes do not: {:.0}/s at 1 client → {:.0}/s at {} — one redb writer, and \
             concurrency queues rather than parallelizes",
            one.per_second(),
            many.per_second(),
            many.concurrency,
        );
    }
}
