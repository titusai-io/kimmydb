//! `Idempotency-Key` at the HTTP edge (ADR-240): which routes take it and which
//! refuse it, what a malformed key draws, the gate (the operator's switch and the
//! live-peer net), and the body path a keyed request is read through.
//!
//! Driven over a real socket with a real client, because the header's rules are
//! about field lines as they arrive (one line or two, bytes outside ASCII), and
//! the router's own layers, `/mcp` merged beside the table included, are the
//! thing under test.

use std::sync::Arc;
use std::time::Duration;

use kimmy_api::SharedState;
use kimmy_auth::TokenIssuer;
use kimmy_cluster::{Facts, IdempotencyGate, Members};
use kimmy_core::NodeId;
use kimmy_storage::Engine;
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::{Value, json};

const ROOT_PASSWORD: &str = "idempotency-test-password";
const SECRET: &str = "an-idempotency-test-signing-secret-value";
const KEY: &str = "\"6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90\"";

struct Node {
    base: String,
    state: SharedState,
    token: String,
    _dir: tempfile::TempDir,
}

/// A served node with `shop.orders` holding `{_id: 1}` and `{_id: 2}`, `/mcp`
/// merged beside the table as the daemon merges it.
async fn node() -> Node {
    node_with(kimmy_api::RequestLimits::default()).await
}

async fn node_with(limits: kimmy_api::RequestLimits) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let users = kimmy_auth::UserStore::open(&engine).unwrap();
    users.bootstrap_root(&engine, "root", ROOT_PASSWORD).unwrap();
    let state = kimmy_api::state(
        Arc::clone(&engine),
        TokenIssuer::new(SECRET, 3600).unwrap(),
        false,
        kimmy_api::RateLimits::disabled(),
    )
    .unwrap();
    let mcp = axum::Router::new().route("/mcp", axum::routing::any(|| async { "reached" }));
    let app = kimmy_api::router_with_limits(Arc::clone(&state), Some(mcp), limits);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    let base = format!("http://{addr}");
    let res = reqwest::Client::new()
        .post(format!("{base}/v1/auth/login"))
        .json(&json!({ "user": "root", "password": ROOT_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let token = res.json::<Value>().await.unwrap()["token"].as_str().unwrap().to_string();
    let node = Node { base, state, token, _dir: dir };
    assert_eq!(node.post("/v1/db/shop/collections", json!({ "name": "orders" })).await.status, 200);
    let seeded =
        node.post("/v1/db/shop/coll/orders/bulk", json!([{ "_id": 1 }, { "_id": 2 }])).await;
    assert_eq!(seeded.status, 200, "{}", seeded.text);
    node
}

struct Answer {
    status: u16,
    headers: reqwest::header::HeaderMap,
    body: Value,
    text: String,
}

/// What a request carries beyond its method and path.
#[derive(Default)]
struct Req {
    headers: Vec<(HeaderName, HeaderValue)>,
    body: Option<(Vec<u8>, &'static str)>,
    anonymous: bool,
    /// Another principal's token, in place of root's.
    token: Option<String>,
}

impl Req {
    fn key(value: &str) -> Self {
        Self::default().with_key(value)
    }

    fn with_key(mut self, value: &str) -> Self {
        self.headers.push((key_header(), HeaderValue::from_str(value).unwrap()));
        self
    }

    fn json(mut self, body: &Value) -> Self {
        self.body = Some((body.to_string().into_bytes(), "application/json"));
        self
    }

    fn raw(mut self, body: &[u8], content_type: &'static str) -> Self {
        self.body = Some((body.to_vec(), content_type));
        self
    }
}

fn key_header() -> HeaderName {
    HeaderName::from_static("idempotency-key")
}

impl Node {
    async fn send(&self, method: &str, path: &str, req: Req) -> Answer {
        let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
        let mut builder = reqwest::Client::new().request(method, format!("{}{path}", self.base));
        if !req.anonymous {
            builder = builder.bearer_auth(req.token.as_deref().unwrap_or(&self.token));
        }
        for (name, value) in req.headers {
            // `header` appends: two calls are two field lines.
            builder = builder.header(name, value);
        }
        if let Some((body, content_type)) = req.body {
            builder = builder.header("content-type", content_type).body(body);
        }
        let res = builder.send().await.unwrap();
        let status = res.status().as_u16();
        let headers = res.headers().clone();
        let text = res.text().await.unwrap();
        let body = serde_json::from_str(&text).unwrap_or(Value::Null);
        Answer { status, headers, body, text }
    }

    async fn post(&self, path: &str, body: Value) -> Answer {
        self.send("POST", path, Req::default().json(&body)).await
    }

    async fn count(&self) -> u64 {
        let counted = self.post("/v1/db/shop/coll/orders/count", json!({})).await;
        counted.body["count"].as_u64().unwrap_or_else(|| panic!("{}", counted.text))
    }

    async fn capabilities(&self) -> Vec<String> {
        let version = self.send("GET", "/v1/version", Req::default()).await;
        version.body["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_str().unwrap().to_string())
            .collect()
    }

    async fn gated_count(&self) -> u64 {
        let page = self.send("GET", "/metrics", Req::default()).await.text;
        let line = page
            .lines()
            .find(|l| l.starts_with("kimmy_idempotency_requests_total{outcome=\"gated\"}"))
            .unwrap_or_else(|| panic!("no gated series:\n{page}"));
        line.rsplit(' ').next().unwrap().parse().unwrap()
    }
}

fn is_route_refusal(answer: &Answer) -> bool {
    answer.status == 400
        && answer.body["error"] == "bad_request"
        && answer.body["message"]
            .as_str()
            .is_some_and(|m| m.starts_with("this route does not take an Idempotency-Key"))
}

/// One request on each route the header is taken on, in `IDEMPOTENT_ROUTES`'s
/// vocabulary, with a body that route accepts.
fn keyed_routes() -> Vec<(&'static str, &'static str, &'static str, Option<Value>)> {
    vec![
        (
            "POST",
            "/v1/db/{db}/coll/{coll}/docs",
            "/v1/db/shop/coll/orders/docs",
            Some(json!({ "n": 1 })),
        ),
        (
            "POST",
            "/v1/db/{db}/coll/{coll}/bulk",
            "/v1/db/shop/coll/orders/bulk",
            Some(json!([{ "n": 2 }])),
        ),
        (
            "PUT",
            "/v1/db/{db}/coll/{coll}/docs/{id}",
            "/v1/db/shop/coll/orders/docs/7?upsert=true",
            Some(json!({ "n": 3 })),
        ),
        ("DELETE", "/v1/db/{db}/coll/{coll}/docs/{id}", "/v1/db/shop/coll/orders/docs/7", None),
        (
            "POST",
            "/v1/db/{db}/coll/{coll}/update",
            "/v1/db/shop/coll/orders/update",
            Some(json!({ "filter": { "_id": 1 }, "update": { "$set": { "x": 1 } } })),
        ),
        (
            "POST",
            "/v1/db/{db}/coll/{coll}/find_and_modify",
            "/v1/db/shop/coll/orders/find_and_modify",
            Some(json!({ "filter": { "_id": 1 }, "update": { "$set": { "y": 1 } } })),
        ),
        (
            "POST",
            "/v1/db/{db}/coll/{coll}/delete",
            "/v1/db/shop/coll/orders/delete",
            Some(json!({ "filter": { "_id": 2 } })),
        ),
    ]
}

#[tokio::test]
async fn every_route_that_takes_the_header_accepts_a_well_formed_key() {
    let node = node().await;
    let routes = keyed_routes();
    let covered: std::collections::BTreeSet<(&str, &str)> =
        routes.iter().map(|(method, template, ..)| (*method, *template)).collect();
    let listed: std::collections::BTreeSet<(&str, &str)> =
        kimmy_api::routes::IDEMPOTENT_ROUTES.iter().copied().collect();
    assert_eq!(covered, listed, "this test drives every listed route and nothing else");

    // A key of its own for each: one key names one request, and the same key
    // on another route of this collection is a key reused.
    for (i, (method, _, path, body)) in routes.into_iter().enumerate() {
        let mut req = Req::key(&format!("\"route-{i:02}-6f1c0e52-8a1d-4f53\""));
        if let Some(body) = &body {
            req = req.json(body);
        }
        let answer = node.send(method, path, req).await;
        assert_eq!(answer.status, 200, "{method} {path}: {}", answer.text);
        assert!(
            answer.headers.get("idempotency-replayed").is_none(),
            "{method} {path}: a first answer is not a replay"
        );
    }
}

/// Every other route refuses the header, `/mcp` and the probes included: reads
/// (the `POST` `find`, `count` and `aggregate` among them), schema changes,
/// vectors, webhooks, users, roles and the backup. Nothing they would have done
/// is done: the database and the collection the drops name are still there.
#[tokio::test]
async fn every_other_route_refuses_the_header_by_name() {
    let node = node().await;
    let refused: Vec<(&str, &str, Option<Value>)> = vec![
        ("GET", "/healthz", None),
        ("GET", "/readyz", None),
        ("GET", "/metrics", None),
        ("GET", "/v1/version", None),
        ("GET", "/v1/topology", None),
        ("POST", "/v1/auth/login", Some(json!({ "user": "root", "password": ROOT_PASSWORD }))),
        ("POST", "/v1/auth/refresh", None),
        ("GET", "/v1/auth/whoami", None),
        ("GET", "/v1/users", None),
        (
            "POST",
            "/v1/users",
            Some(json!({ "name": "ada", "password": "a-long-password", "grants": [] })),
        ),
        ("DELETE", "/v1/users/root", None),
        ("GET", "/v1/roles", None),
        ("POST", "/v1/roles", Some(json!({ "name": "r", "grants": [] }))),
        ("GET", "/v1/databases", None),
        ("GET", "/v1/db/shop/collections", None),
        ("POST", "/v1/db/shop/collections", Some(json!({ "name": "carts" }))),
        ("DELETE", "/v1/db/shop/coll/orders", None),
        ("DELETE", "/v1/db/shop", None),
        ("GET", "/v1/db/shop/coll/orders/docs", None),
        ("GET", "/v1/db/shop/coll/orders/docs/1", None),
        ("POST", "/v1/db/shop/coll/orders/find", Some(json!({}))),
        ("POST", "/v1/db/shop/coll/orders/count", Some(json!({}))),
        ("POST", "/v1/db/shop/coll/orders/aggregate", Some(json!({ "pipeline": [] }))),
        ("GET", "/v1/db/shop/coll/orders/describe", None),
        ("GET", "/v1/db/shop/coll/orders/indexes", None),
        ("POST", "/v1/db/shop/coll/orders/indexes", Some(json!({ "fields": [{ "path": "n" }] }))),
        ("DELETE", "/v1/db/shop/coll/orders/indexes/n_1", None),
        ("GET", "/v1/db/shop/coll/orders/violations", None),
        ("GET", "/v1/db/shop/coll/orders/vector", None),
        ("POST", "/v1/db/shop/coll/orders/vector", Some(json!({}))),
        ("DELETE", "/v1/db/shop/coll/orders/vector", None),
        ("GET", "/v1/db/shop/coll/orders/docs/1/vectors", None),
        ("PUT", "/v1/db/shop/coll/orders/docs/1/vectors", Some(json!({}))),
        ("DELETE", "/v1/db/shop/coll/orders/docs/1/vectors", None),
        ("POST", "/v1/db/shop/coll/orders/vector_search", Some(json!({ "vector": [1.0] }))),
        ("POST", "/v1/db/shop/coll/orders/hybrid_search", Some(json!({ "query": "x" }))),
        ("GET", "/v1/db/shop/coll/orders/webhooks", None),
        (
            "POST",
            "/v1/db/shop/coll/orders/webhooks",
            Some(json!({ "url": "https://example.com/" })),
        ),
        ("DELETE", "/v1/db/shop/coll/orders/webhooks/w", None),
        ("GET", "/v1/db/shop/coll/orders/watch", None),
        ("GET", "/v1/admin/backup", None),
        ("POST", "/mcp", Some(json!({}))),
        ("GET", "/mcp", None),
    ];
    for (method, path, body) in refused {
        let mut req = Req::key(KEY);
        if let Some(body) = &body {
            req = req.json(body);
        }
        let answer = node.send(method, path, req).await;
        assert!(is_route_refusal(&answer), "{method} {path}: {} {}", answer.status, answer.text);
        assert_eq!(answer.body["retry"], "no", "{method} {path}");
    }
    assert_eq!(node.count().await, 2, "neither drop ran, and nothing was written");
    let listed = node.send("GET", "/v1/db/shop/collections", Req::default()).await;
    assert!(listed.text.contains("orders") && !listed.text.contains("carts"), "{}", listed.text);
    // And the same routes without the header are untouched by the layer.
    assert_eq!(node.send("GET", "/healthz", Req::default()).await.status, 200);
    assert_eq!(node.send("POST", "/mcp", Req::default().json(&json!({}))).await.text, "reached");
}

/// The guard answers before authentication, as the query-string guard does: it
/// reads the header and the route and examines no credential.
#[tokio::test]
async fn the_route_refusal_comes_before_authentication() {
    let node = node().await;
    let answer = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/find",
            Req { anonymous: true, ..Req::key(KEY).json(&json!({})) },
        )
        .await;
    assert!(is_route_refusal(&answer), "{} {}", answer.status, answer.text);
    // A malformed key on a route that takes one, likewise.
    let answer = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/docs",
            Req { anonymous: true, ..Req::key("short").json(&json!({})) },
        )
        .await;
    assert_eq!(answer.status, 400, "{}", answer.text);
    // And without a key, the same request is the 401 it always was.
    let answer = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/find",
            Req { anonymous: true, ..Req::default().json(&json!({})) },
        )
        .await;
    assert_eq!(answer.status, 401);
}

/// What a malformed key draws: a `400` naming the header and what is wrong,
/// with nothing written. Each case on a route that takes the header, so it is
/// the syntax being refused and not the route.
#[tokio::test]
async fn a_malformed_key_is_refused_naming_the_header_and_writes_nothing() {
    let node = node().await;
    let long = format!("\"{}\"", "k".repeat(256));
    let cases: Vec<(&str, Req)> = vec![
        ("a bare token", Req::key("6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90")),
        ("15 characters", Req::key("\"kkkkkkkkkkkkkkk\"")),
        ("256 characters", Req::key(&long)),
        ("an empty string", Req::key("\"\"")),
        ("parameters", Req::key(&format!("{KEY};a=1"))),
        ("a list", Req::key(&format!("{KEY}, {KEY}"))),
        ("a bad escape", Req::key("\"6f1c0e52-8a1d-4f53\\n\"")),
        ("two field lines", Req::key(KEY).with_key(KEY)),
        (
            "a byte outside ASCII",
            Req {
                headers: vec![(
                    key_header(),
                    HeaderValue::from_bytes(b"\"6f1c0e52-8a1d-4f53-\xe9\"").unwrap(),
                )],
                ..Req::default()
            },
        ),
    ];
    for (what, req) in cases {
        let answer =
            node.send("POST", "/v1/db/shop/coll/orders/docs", req.json(&json!({ "n": 1 }))).await;
        assert_eq!(answer.status, 400, "{what}: {}", answer.text);
        assert_eq!(answer.body["error"], "bad_request", "{what}");
        let message = answer.body["message"].as_str().unwrap();
        assert!(message.starts_with("Idempotency-Key: "), "{what}: {message}");
        assert!(message.contains("16 to 255"), "{what}: the format is published: {message}");
    }
    assert_eq!(node.count().await, 2, "nothing was written");

    // The bounds and an escaped quote are taken: 16 characters once unescaped.
    for good in ["\"kkkkkkkkkkkkkkkk\"", "\"kkkkkkkkkkkkkk\\\"\\\\\""] {
        let answer = node
            .send("POST", "/v1/db/shop/coll/orders/docs", Req::key(good).json(&json!({ "n": 1 })))
            .await;
        assert_eq!(answer.status, 200, "{good}: {}", answer.text);
    }
    let top = format!("\"{}\"", "k".repeat(255));
    let answer =
        node.send("POST", "/v1/db/shop/coll/orders/docs", Req::key(&top).json(&json!({}))).await;
    assert_eq!(answer.status, 200, "{}", answer.text);
}

/// `update` and `delete` with `explain: true` write nothing, so a key there is
/// refused; without one they plan as they always did.
#[tokio::test]
async fn explain_with_a_key_is_refused() {
    let node = node().await;
    for (path, body) in [
        (
            "/v1/db/shop/coll/orders/update",
            json!({ "filter": {}, "update": { "$set": { "x": 1 } }, "explain": true }),
        ),
        ("/v1/db/shop/coll/orders/delete", json!({ "filter": {}, "explain": true })),
    ] {
        let keyed = node.send("POST", path, Req::key(KEY).json(&body)).await;
        assert_eq!(keyed.status, 400, "{path}: {}", keyed.text);
        assert!(
            keyed.text.contains("explain") && keyed.text.contains("Idempotency-Key"),
            "{}",
            keyed.text
        );
        let plain = node.send("POST", path, Req::default().json(&body)).await;
        assert_eq!(plain.status, 200, "{path}: {}", plain.text);
    }
    assert_eq!(node.count().await, 2);
}

fn peer(n: u8) -> (std::net::SocketAddr, NodeId) {
    (format!("10.0.0.{n}:7900").parse().unwrap(), NodeId::from_bytes([n; 16]))
}

fn carries(value: Option<bool>) -> Facts {
    Facts { boot: vec![1; 16], idempotency_records: value, ..Facts::default() }
}

/// The switch off: every keyed route refuses naming `cluster.idempotency_keys`,
/// the capability is not listed, the refusal is counted, and the same request
/// without the header is untouched.
#[tokio::test]
async fn with_the_switch_off_a_key_is_refused_naming_the_switch() {
    let node = node().await;
    assert!(node.capabilities().await.contains(&"idempotency-keys".to_string()), "open first");
    node.state.set_idempotency_gate(IdempotencyGate::new(false, false, false).predicate());

    let before = node.gated_count().await;
    for (method, _, path, body) in keyed_routes() {
        let mut req = Req::key(KEY);
        if let Some(body) = &body {
            req = req.json(body);
        }
        let answer = node.send(method, path, req).await;
        assert_eq!(answer.status, 400, "{method} {path}: {}", answer.text);
        let message = answer.body["message"].as_str().unwrap();
        assert!(message.contains("Idempotency-Key"), "names the header: {message}");
        assert!(message.contains("cluster.idempotency_keys"), "names the switch: {message}");
    }
    assert_eq!(node.gated_count().await, before + 7, "each refusal counted");
    assert_eq!(node.count().await, 2, "nothing was written");
    assert!(!node.capabilities().await.contains(&"idempotency-keys".to_string()));

    // The gate is asked after authentication: a caller with no token is told
    // it has none, and learns nothing of the cluster from the key it sent.
    for (method, _, path, body) in keyed_routes() {
        let mut req = Req { anonymous: true, ..Req::key(KEY) };
        if let Some(body) = &body {
            req = req.json(body);
        }
        let answer = node.send(method, path, req).await;
        assert_eq!(answer.status, 401, "{method} {path}: {}", answer.text);
        assert!(!answer.text.contains("cluster.idempotency_keys"), "{}", answer.text);
    }
    assert_eq!(node.gated_count().await, before + 7, "an unauthenticated request is not gated");

    // The route refusal and the syntax refusal still say what they say: the
    // gate is asked only of a well-formed key on a route that takes one.
    let answer =
        node.send("POST", "/v1/db/shop/coll/orders/find", Req::key(KEY).json(&json!({}))).await;
    assert!(is_route_refusal(&answer), "{}", answer.text);
    let answer =
        node.send("POST", "/v1/db/shop/coll/orders/docs", Req::key("short").json(&json!({}))).await;
    assert!(answer.text.contains("16 to 255"), "{}", answer.text);
    assert_eq!(node.gated_count().await, before + 7, "neither is the gate's");

    let plain = node.post("/v1/db/shop/coll/orders/docs", json!({ "n": 9 })).await;
    assert_eq!(plain.status, 200, "a write without the header is untouched");
}

/// A clustered member with the switch on, over `members`.
fn clustered_gate(node: &Node) -> Members {
    let members = Members::default();
    members.configure_lease(Duration::from_secs(2), 3);
    let gate = IdempotencyGate::new(true, true, true);
    gate.set_members(members.clone());
    node.state.set_idempotency_gate(gate.predicate());
    members
}

/// The switch on, clustered, a live peer whose block lacks
/// `idempotency_records` (an older release): every keyed route refuses `400
/// bad_request`, `retry: no`, no `Retry-After`, naming the member, and the
/// capability is not listed. A second live peer not heard from yet changes
/// nothing: the block that says no decides, and only its member is named. Once
/// it says it carries records and the other is heard, keys are taken.
#[tokio::test]
async fn an_older_live_peer_refuses_400_naming_it_whoever_else_is_unheard() {
    let node = node().await;
    let members = clustered_gate(&node);
    let insert =
        || node.send("POST", "/v1/db/shop/coll/orders/docs", Req::key(KEY).json(&json!({})));
    assert_eq!(insert().await.status, 200, "alone, the switch decides");
    let documents = node.count().await;

    let (older, older_id) = peer(2);
    members.insert_for_test(older, older_id);
    members.record_peer_facts_for_test(older_id, carries(None), Duration::ZERO);
    let (unheard, unheard_id) = peer(3);
    for (what, with_unheard) in [("an older block", false), ("and an unheard peer", true)] {
        if with_unheard {
            members.insert_for_test(unheard, unheard_id);
        }
        let before = node.gated_count().await;
        for (method, _, path, body) in keyed_routes() {
            let mut req = Req::key(KEY);
            if let Some(body) = &body {
                req = req.json(body);
            }
            let answer = node.send(method, path, req).await;
            assert_eq!(answer.status, 400, "{what}: {method} {path}: {}", answer.text);
            assert_eq!(answer.body["error"], "bad_request", "{what}: {}", answer.text);
            assert_eq!(answer.body["retry"], "no", "{what}: {}", answer.text);
            assert!(answer.headers.get("retry-after").is_none(), "{what}: lasts a roll");
            let message = answer.body["message"].as_str().unwrap();
            assert!(message.contains(&older_id.to_string()), "{what}: names it: {message}");
            assert!(!message.contains(&unheard_id.to_string()), "{what}: only it: {message}");
            assert!(message.contains("Idempotency-Key"), "{what}: {message}");
        }
        assert_eq!(node.gated_count().await, before + 7, "{what}: each refusal counted");
        assert!(!node.capabilities().await.contains(&"idempotency-keys".to_string()), "{what}");
    }
    assert_eq!(node.count().await, documents, "nothing was written by a refusal");

    members.record_peer_facts_for_test(older_id, carries(Some(true)), Duration::ZERO);
    members.record_peer_facts_for_test(unheard_id, carries(Some(true)), Duration::ZERO);
    let answer = node
        .send("POST", "/v1/db/shop/coll/orders/docs", Req::key(UNHEARD_KEY).json(&json!({})))
        .await;
    assert_eq!(answer.status, 200, "{}", answer.text);
    assert!(node.capabilities().await.contains(&"idempotency-keys".to_string()));
}

/// A key of its own for [`a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken`].
const UNHEARD_KEY: &str = "\"unheard-6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e\"";

/// The switch on, clustered, a live peer not heard from yet and no held block
/// saying no (a peer that came up a moment ago, or whose block was dropped on a
/// rename): every keyed route answers `503 peer_unheard`, `retry: wait`, with
/// `Retry-After: 1`, naming the member and saying to send the
/// same key again; the refusal is counted as gated, writes nothing and the
/// capability is not listed. Once the peer is heard, the same request with the
/// same key is taken as a first answer, and sent again is its replay.
#[tokio::test]
async fn a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken() {
    let node = node().await;
    let members = clustered_gate(&node);
    let (addr, id) = peer(2);
    members.insert_for_test(addr, id);
    let documents = node.count().await;

    let before = node.gated_count().await;
    for (method, _, path, body) in keyed_routes() {
        let mut req = Req::key(UNHEARD_KEY);
        if let Some(body) = &body {
            req = req.json(body);
        }
        let answer = node.send(method, path, req).await;
        assert_eq!(answer.status, 503, "{method} {path}: {}", answer.text);
        assert_eq!(answer.body["error"], "peer_unheard", "{}", answer.text);
        assert_eq!(answer.body["retry"], "wait", "{}", answer.text);
        assert_eq!(
            answer.headers.get("retry-after").map(|v| v.to_str().unwrap()),
            Some("1"),
            "{method} {path}: a fixed second"
        );
        let message = answer.body["message"].as_str().unwrap();
        for said in [id.to_string().as_str(), "Idempotency-Key", "transient", "same key"] {
            assert!(message.contains(said), "{method} {path}: says {said:?}: {message}");
        }
    }
    assert_eq!(node.gated_count().await, before + 7, "each refusal counted as gated");
    assert_eq!(node.count().await, documents, "nothing was written");
    assert!(!node.capabilities().await.contains(&"idempotency-keys".to_string()));

    // A rename drops a block that was held: the same answer again.
    members.record_peer_facts_for_test(id, carries(Some(true)), Duration::ZERO);
    members.forget_peer_for_test(id);
    let insert = || {
        node.send("POST", "/v1/db/shop/coll/orders/docs", Req::key(UNHEARD_KEY).json(&json!({})))
    };
    assert_eq!(insert().await.status, 503, "a dropped block is unheard again");

    members.record_peer_facts_for_test(id, carries(Some(true)), Duration::ZERO);
    let first = insert().await;
    assert_eq!(first.status, 200, "{}", first.text);
    assert!(first.headers.get("idempotency-replayed").is_none(), "the first application");
    let again = insert().await;
    assert_eq!(again.status, 200, "{}", again.text);
    assert_eq!(again.headers.get("idempotency-replayed").unwrap(), "?1", "then its replay");
    assert_eq!(again.text, first.text);
    assert_eq!(node.count().await, documents + 1, "applied once");
    assert!(node.capabilities().await.contains(&"idempotency-keys".to_string()));
}

/// "Not heard" is bounded by the lease the unheard-peer WARN uses: a live peer
/// with no block for longer than it is answered as one that does not carry
/// records, `400 bad_request`, `retry: no`, no `Retry-After`, naming it; and a
/// peer whose block does not decode is that `400` at once.
#[tokio::test]
async fn a_peer_unheard_past_its_lease_or_whose_block_does_not_decode_is_400() {
    let node = node().await;
    let members = clustered_gate(&node);
    let (addr, id) = peer(2);
    members.insert_for_test(addr, id);
    let insert =
        || node.send("POST", "/v1/db/shop/coll/orders/docs", Req::key(KEY).json(&json!({})));
    assert_eq!(insert().await.status, 503, "within its lease");

    members.backdate_unheard_for_test(id, members.unheard_lease() + Duration::from_secs(1));
    let refused = |answer: &Answer, what: &str| {
        assert_eq!(answer.status, 400, "{what}: {}", answer.text);
        assert_eq!(answer.body["error"], "bad_request", "{what}: {}", answer.text);
        assert_eq!(answer.body["retry"], "no", "{what}: {}", answer.text);
        assert!(answer.headers.get("retry-after").is_none(), "{what}: no Retry-After");
        let message = answer.body["message"].as_str().unwrap();
        assert!(message.contains(&id.to_string()), "{what}: names it: {message}");
    };
    refused(&insert().await, "past its lease");

    let (addr, id) = peer(3);
    members.forget_peer_for_test(peer(2).1);
    members.record_peer_facts_for_test(peer(2).1, carries(Some(true)), Duration::ZERO);
    members.insert_for_test(addr, id);
    assert_eq!(insert().await.status, 503, "a new peer, within its lease");
    members.note_undecodable_for_test(id);
    let answer = insert().await;
    assert_eq!(answer.status, 400, "a block that does not decode: {}", answer.text);
    assert!(answer.body["message"].as_str().unwrap().contains(&id.to_string()));
    assert!(!node.capabilities().await.contains(&"idempotency-keys".to_string()));
}

/// The body a keyed request is read through refuses exactly what `JsonBody`
/// refuses, with the same status and the same envelope: a content type that is
/// not JSON (`415`), JSON that does not parse (`400`) and JSON of the wrong
/// shape (`422`).
#[tokio::test]
async fn a_keyed_body_is_refused_exactly_as_an_unkeyed_one_is() {
    let node = node().await;
    let cases: Vec<(&str, &str, &[u8], &'static str, u16)> = vec![
        ("POST", "/v1/db/shop/coll/orders/docs", b"{\"n\":1}", "text/plain", 415),
        ("PUT", "/v1/db/shop/coll/orders/docs/1", b"{\"n\":1}", "text/plain", 415),
        ("POST", "/v1/db/shop/coll/orders/docs", b"{\"n\":", "application/json", 400),
        ("POST", "/v1/db/shop/coll/orders/bulk", b"{\"n\":1}", "application/json", 422),
        (
            "POST",
            "/v1/db/shop/coll/orders/update",
            b"{\"update\":{},\"bogus\":1}",
            "application/json",
            422,
        ),
        ("POST", "/v1/db/shop/coll/orders/delete", b"{\"filter\":null}", "application/json", 422),
        (
            "POST",
            "/v1/db/shop/coll/orders/find_and_modify",
            b"{\"remove\":\"yes\"}",
            "application/json",
            422,
        ),
    ];
    for (method, path, body, content_type, status) in cases {
        let plain = node.send(method, path, Req::default().raw(body, content_type)).await;
        let keyed = node.send(method, path, Req::key(KEY).raw(body, content_type)).await;
        assert_eq!(plain.status, status, "{method} {path}: {}", plain.text);
        assert_eq!(keyed.status, plain.status, "{method} {path}");
        assert_eq!(keyed.text, plain.text, "{method} {path}: the same envelope, byte for byte");
    }
    assert_eq!(node.count().await, 2);
}

/// Past `server.max_body_bytes` a keyed body is the same `413`, and the client
/// receives it: the refused body is still handed back to the drain.
#[tokio::test]
async fn a_keyed_body_over_the_ceiling_is_the_same_413() {
    let node =
        node_with(kimmy_api::RequestLimits { max_body_bytes: 1024, ..Default::default() }).await;
    let big = json!({ "pad": "x".repeat(64 * 1024) });
    let plain = node.send("POST", "/v1/db/shop/coll/orders/docs", Req::default().json(&big)).await;
    let keyed = node.send("POST", "/v1/db/shop/coll/orders/docs", Req::key(KEY).json(&big)).await;
    assert_eq!(plain.status, 413, "{}", plain.text);
    assert_eq!(keyed.status, 413, "{}", keyed.text);
    assert_eq!(keyed.text, plain.text);
    assert_eq!(keyed.body["error"], "payload_too_large");
}

/// A successful keyed write answers with the body an unkeyed one does, field
/// for field: the renderers are shared, and nothing about a first answer says a
/// key was there.
#[tokio::test]
async fn a_keyed_first_answer_has_the_shape_of_an_unkeyed_one() {
    let node = node().await;
    let path = "/v1/db/shop/coll/orders/docs";
    let plain = node.send("POST", path, Req::default().json(&json!({ "_id": 10 }))).await;
    let keyed = node.send("POST", path, Req::key(KEY).json(&json!({ "_id": 11 }))).await;
    let keys = |a: &Answer| a.body.as_object().unwrap().keys().cloned().collect::<Vec<_>>();
    assert_eq!(keys(&keyed), keys(&plain), "{} / {}", keyed.text, plain.text);
    assert_eq!(keyed.body["insertedId"], 11);
}

/// The fingerprint a keyed request carries is taken over the method, the route
/// template, the path and query as sent and the body as sent; by id, the delete
/// hashes an empty body whatever it was sent with.
#[tokio::test]
async fn the_fingerprint_is_taken_over_the_request_as_sent() {
    use axum::routing::{delete, post};
    use kimmy_api::idempotency::{KeyedJson, KeyedNoBody, fingerprint};
    use tower::ServiceExt as _;

    // The extractors ask the gate, so they are routed with a node's state.
    let node = node().await;
    fn hex(fingerprint: kimmy_api::idempotency::Fingerprint) -> String {
        fingerprint.0.iter().map(|b| format!("{b:02x}")).collect()
    }
    let app = axum::Router::new()
        .route(
            "/v1/db/{db}/coll/{coll}/docs",
            post(|keyed: KeyedJson<Value>| async move {
                keyed.key.map(|k| hex(k.fingerprint)).unwrap_or_default()
            }),
        )
        .route(
            "/v1/db/{db}/coll/{coll}/docs/{id}",
            delete(|KeyedNoBody(key): KeyedNoBody| async move {
                key.map(|k| hex(k.fingerprint)).unwrap_or_default()
            }),
        )
        .with_state(std::sync::Arc::clone(&node.state));
    let send = |method: &str, uri: &str, body: &[u8]| {
        let request = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("idempotency-key", KEY)
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body.to_vec()))
            .unwrap();
        let app = app.clone();
        async move {
            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), 200);
            let bytes = axum::body::to_bytes(response.into_body(), 1 << 16).await.unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        }
    };

    let route = "/v1/db/{db}/coll/{coll}/docs";
    let body = br#"{"a":1,"b":2}"#;
    let base = send("POST", "/v1/db/shop/coll/orders/docs?x=1", body).await;
    assert_eq!(
        base,
        hex(fingerprint("POST", route, "/v1/db/shop/coll/orders/docs?x=1", body)),
        "method, route template, path and query, body"
    );
    assert_eq!(base, send("POST", "/v1/db/shop/coll/orders/docs?x=1", body).await, "stable");
    for (uri, body) in [
        ("/v1/db/shop/coll/orders/docs?x=1", &br#"{"a":1,"b":3}"#[..]),
        ("/v1/db/shop/coll/orders/docs?x=1", br#"{"b":2,"a":1}"#),
        ("/v1/db/shop/coll/orders/docs?x=2", body),
        ("/v1/db/shop/coll/orders/docs", body),
        ("/v1/db/shop/coll/carts/docs?x=1", body),
    ] {
        assert_ne!(base, send("POST", uri, body).await, "{uri} {}", String::from_utf8_lossy(body));
    }

    let by_id = send("DELETE", "/v1/db/shop/coll/orders/docs/7", b"").await;
    assert_eq!(
        by_id,
        hex(fingerprint(
            "DELETE",
            "/v1/db/{db}/coll/{coll}/docs/{id}",
            "/v1/db/shop/coll/orders/docs/7",
            b""
        ))
    );
    assert_eq!(by_id, send("DELETE", "/v1/db/shop/coll/orders/docs/7", b"{\"ignored\":1}").await);
}

/// `GET /v1/version` says how long this node remembers a key.
#[tokio::test]
async fn the_version_publishes_the_window() {
    let node = node().await;
    let version = node.send("GET", "/v1/version", Req::default()).await;
    assert_eq!(version.body["idempotencyWindowSecs"], 86_400);
    node.state.set_idempotency_window_secs(3_600);
    let version = node.send("GET", "/v1/version", Req::default()).await;
    assert_eq!(version.body["idempotencyWindowSecs"], 3_600);
}

/// Every series idempotency keys add is on the page from the start, at zero, the ones the
/// storage half feeds included: a series that appears only once something
/// happens is one a dashboard cannot be built on.
#[tokio::test]
async fn every_idempotency_series_is_on_the_page_at_zero() {
    let node = node().await;
    let page = node.send("GET", "/metrics", Req::default()).await.text;
    for line in [
        "kimmy_idempotency_requests_total{outcome=\"applied\"} 0",
        "kimmy_idempotency_requests_total{outcome=\"replayed\"} 0",
        "kimmy_idempotency_requests_total{outcome=\"resumed\"} 0",
        "kimmy_idempotency_requests_total{outcome=\"reused\"} 0",
        "kimmy_idempotency_requests_total{outcome=\"in_flight\"} 0",
        "kimmy_idempotency_requests_total{outcome=\"gated\"} 0",
        "kimmy_idempotency_peer_replays_total 0",
        "kimmy_idempotency_records 0",
        "kimmy_idempotency_records_received_total{via=\"pull\"} 0",
        "kimmy_idempotency_records_received_total{via=\"push\"} 0",
        "kimmy_idempotency_records_received_total{via=\"replay\"} 0",
        "kimmy_idempotency_records_received_total{via=\"snapshot\"} 0",
        "kimmy_idempotency_records_received_total{via=\"repair\"} 0",
        "kimmy_idempotency_records_undecodable_total 0",
        "kimmy_idempotency_records_collected_total 0",
    ] {
        assert!(page.lines().any(|l| l == line), "missing {line:?}");
    }
}

// ---------------------------------------------------------------------------
// What a key does: the record, the replay, the refusals it draws
// ---------------------------------------------------------------------------

fn replayed(answer: &Answer) -> bool {
    answer.headers.get("idempotency-replayed").is_some_and(|v| v == "?1")
}

fn counted(page: &str, series: &str) -> u64 {
    page.lines()
        .find(|l| l.starts_with(&format!("{series} ")))
        .and_then(|l| l.rsplit(' ').next())
        .unwrap_or_else(|| panic!("no {series}:\n{page}"))
        .parse()
        .unwrap()
}

impl Node {
    async fn metric(&self, series: &str) -> u64 {
        counted(&self.send("GET", "/metrics", Req::default()).await.text, series)
    }

    /// A user with `actions` over `shop.*`, and a token for it.
    async fn user(&self, name: &str, actions: &[&str]) -> String {
        let created = self
            .post(
                "/v1/users",
                json!({ "user": name, "password": "a-user-password",
                        "grants": [{ "db": "shop", "collection": "*", "actions": actions }] }),
            )
            .await;
        assert!(created.status / 100 == 2, "{}", created.text);
        self.login_as(name).await
    }

    async fn login_as(&self, name: &str) -> String {
        let res = self
            .send(
                "POST",
                "/v1/auth/login",
                Req { anonymous: true, ..Req::default() }
                    .json(&json!({ "user": name, "password": "a-user-password" })),
            )
            .await;
        assert_eq!(res.status, 200, "{}", res.text);
        res.body["token"].as_str().unwrap().to_string()
    }

    async fn doc(&self, id: &str) -> Value {
        self.send("GET", &format!("/v1/db/shop/coll/orders/docs/{id}"), Req::default()).await.body
    }
}

/// Every keyed route, sent twice with its key: the second answer is the first
/// one, status and body byte for byte, marked `Idempotency-Replayed: ?1`, and
/// nothing is written again: an `$inc` lands once.
#[tokio::test]
async fn a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte() {
    let node = node().await;
    let cases: Vec<(&str, &str, Option<&str>)> = vec![
        ("POST", "/v1/db/shop/coll/orders/docs", Some(r#"{"n":1}"#)),
        ("POST", "/v1/db/shop/coll/orders/docs", Some(r#"{"_id":"c-1","n":1}"#)),
        ("POST", "/v1/db/shop/coll/orders/bulk", Some(r#"[{"n":1},{"_id":"b-2"},{"n":3}]"#)),
        ("PUT", "/v1/db/shop/coll/orders/docs/20?upsert=true", Some(r#"{"n":20}"#)),
        ("PUT", "/v1/db/shop/coll/orders/docs/1", Some(r#"{"n":100}"#)),
        ("DELETE", "/v1/db/shop/coll/orders/docs/2", None),
        (
            "POST",
            "/v1/db/shop/coll/orders/update",
            Some(r#"{"filter":{"_id":1},"update":{"$inc":{"n":1}}}"#),
        ),
        (
            "POST",
            "/v1/db/shop/coll/orders/update",
            Some(r#"{"filter":{},"update":{"$set":{"seen":true}},"multi":true}"#),
        ),
        ("POST", "/v1/db/shop/coll/orders/delete", Some(r#"{"filter":{"_id":"c-1"}}"#)),
        ("POST", "/v1/db/shop/coll/orders/delete", Some(r#"{"filter":{"n":3},"multi":true}"#)),
        (
            "POST",
            "/v1/db/shop/coll/orders/find_and_modify",
            Some(
                r#"{"filter":{"_id":1},"update":{"$inc":{"n":1}},"returnDocument":"after","projection":{"n":1}}"#,
            ),
        ),
        (
            "POST",
            "/v1/db/shop/coll/orders/find_and_modify",
            Some(
                r#"{"filter":{"sku":"x"},"update":{"$set":{"q":1}},"upsert":true,"returnDocument":"after"}"#,
            ),
        ),
        (
            "POST",
            "/v1/db/shop/coll/orders/find_and_modify",
            Some(r#"{"filter":{"_id":20},"remove":true}"#),
        ),
    ];
    let before = node.metric("kimmy_idempotency_requests_total{outcome=\"replayed\"}").await;
    for (i, (method, path, body)) in cases.iter().enumerate() {
        let key = format!("\"replay-{i:02}-6f1c0e52-8a1d\"");
        let request = || {
            let req = Req::key(&key);
            match body {
                Some(body) => req.raw(body.as_bytes(), "application/json"),
                None => req,
            }
        };
        let first = node.send(method, path, request()).await;
        assert_eq!(first.status, 200, "{method} {path}: {}", first.text);
        assert!(!replayed(&first), "{method} {path}: a first answer is not a replay");
        let again = node.send(method, path, request()).await;
        assert_eq!(again.status, first.status, "{method} {path}");
        assert_eq!(again.text, first.text, "{method} {path}: the first answer, byte for byte");
        assert!(replayed(&again), "{method} {path}: marked as a replay");
        assert_eq!(
            again.headers.get("content-type"),
            first.headers.get("content-type"),
            "{method} {path}"
        );
    }
    assert_eq!(
        node.metric("kimmy_idempotency_requests_total{outcome=\"replayed\"}").await,
        before + cases.len() as u64
    );
    // Each write landed once: 100, then two `$inc`s.
    assert_eq!(node.doc("1").await["n"], 102);
    // The upsert created one document, and its replay none.
    let found =
        node.post("/v1/db/shop/coll/orders/find", json!({ "filter": { "sku": "x" } })).await;
    assert_eq!(found.body["documents"].as_array().map(Vec::len), Some(1), "{}", found.text);
    assert_eq!(node.metric("kimmy_idempotency_peer_replays_total").await, 0, "all this member's");
    // One record per keyed request that wrote, read from the engine at the
    // scrape; the replays added none.
    assert_eq!(node.metric("kimmy_idempotency_records").await, cases.len() as u64);
}

/// A keyed insert without `_id` stores the key's derived one, a string, and
/// its answer and its replay echo that; a bulk's documents each derive their
/// own, by position; a `find_and_modify` upsert's seed takes position 0's.
#[tokio::test]
async fn a_keyed_insert_without_an_id_stores_and_echoes_the_derived_one() {
    let node = node().await;
    let key = "\"derived-6f1c0e52-8a1d-4f53\"";
    let first = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/docs",
            Req::key(key).raw(br#"{"n":1}"#, "application/json"),
        )
        .await;
    let id = first.body["insertedId"].as_str().expect("a string id").to_string();
    assert_eq!(id.len(), 36, "a UUID's canonical form: {id}");
    assert_eq!(uuid::Uuid::parse_str(&id).unwrap().get_version_num(), 8);
    assert_eq!(node.doc(&id).await["n"], 1, "the stored document has it");
    let again = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/docs",
            Req::key(key).raw(br#"{"n":1}"#, "application/json"),
        )
        .await;
    assert_eq!(again.body["insertedId"], id);

    let bulk = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/bulk",
            Req::key("\"derived-bulk-6f1c0e52-8a1d\"")
                .raw(br#"[{"n":1},{"n":1},{"_id":"mine"}]"#, "application/json"),
        )
        .await;
    let ids = bulk.body["insertedIds"].as_array().unwrap().clone();
    assert_ne!(ids[0], ids[1], "each position derives its own");
    assert_eq!(ids[2], "mine", "a client's own `_id` is kept");
    for id in &ids {
        assert_eq!(node.doc(id.as_str().unwrap()).await["_id"], *id);
    }

    let upsert = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/find_and_modify",
            Req::key("\"derived-upsert-6f1c0e52-8a1d\"").raw(
                br#"{"filter":{"sku":"y"},"update":{"$set":{"q":1}},"upsert":true}"#,
                "application/json",
            ),
        )
        .await;
    let upserted = upsert.body["upsertedId"].as_str().expect("a derived string").to_string();
    assert_eq!(uuid::Uuid::parse_str(&upserted).unwrap().get_version_num(), 8);
    assert_eq!(node.doc(&upserted).await["sku"], "y");
}

/// The derived `_id` of each route that derives one, end to end, against values
/// computed outside this code from the stated layouts (the collection id, the
/// scope hash under `local:root`, the request's fingerprint as sent, the
/// position). A release whose retry derived another `_id` (another fingerprint
/// tag or layout, a renamed route template or parameter, another derivation)
/// would leave two documents for one request, and fails here.
#[tokio::test]
async fn the_derived_id_of_every_deriving_route_is_pinned_end_to_end() {
    let node = node().await;
    let insert = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/docs",
            Req::key(KEY).raw(br#"{"n":1}"#, "application/json"),
        )
        .await;
    assert_eq!(insert.status, 200, "{:?}", insert.body);
    assert_eq!(insert.body["insertedId"], "0f7f7545-80d8-81aa-8d3e-6f9b6000fd29");

    let bulk = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/bulk",
            Req::key("\"golden-bulk-6f1c0e52-8a1d\"")
                .raw(br#"[{"n":1},{"n":2}]"#, "application/json"),
        )
        .await;
    assert_eq!(bulk.status, 200, "{:?}", bulk.body);
    assert_eq!(
        bulk.body["insertedIds"],
        json!(["290b4314-23bf-8b26-b03f-0e68c4abe7e4", "e406aed1-772b-836c-9af2-c82ffd73b7ee"])
    );

    let upsert = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/find_and_modify",
            Req::key("\"golden-upsert-6f1c0e52-8a1d\"").raw(
                br#"{"filter":{"sku":"g"},"update":{"$set":{"q":1}},"upsert":true}"#,
                "application/json",
            ),
        )
        .await;
    assert_eq!(upsert.status, 200, "{:?}", upsert.body);
    assert_eq!(upsert.body["upsertedId"], "802a2ada-8b6a-8b0a-8a47-29a9f56ba277");
}

/// The derived `_id` is a function of the key, the principal, the collection
/// and the request: two separate engines given the same request under the same
/// key and principal derive the same one, which is `kimmy-core`'s derivation of
/// the edge's fingerprint; another request under the same key derives another.
#[tokio::test]
async fn the_derived_id_is_the_same_on_two_engines_and_another_for_another_request() {
    let (a, b) = (node().await, node().await);
    let path = "/v1/db/shop/coll/orders/docs";
    let key = "6f1c0e52-8a1d-4f53-9b3e-2c1d6a7e4b90";
    async fn send(node: &Node, key: &str, body: &[u8]) -> Answer {
        let path = "/v1/db/shop/coll/orders/docs";
        node.send("POST", path, Req::key(&format!("\"{key}\"")).raw(body, "application/json")).await
    }
    let on_a = send(&a, key, br#"{"n":1}"#).await.body["insertedId"].clone();
    let on_b = send(&b, key, br#"{"n":1}"#).await.body["insertedId"].clone();
    assert_eq!(on_a, on_b, "one request, one document key, on every member");

    let fingerprint = kimmy_api::idempotency::fingerprint(
        "POST",
        "/v1/db/{db}/coll/{coll}/docs",
        path,
        br#"{"n":1}"#,
    );
    let scope = kimmy_core::IdempotencyKey::from_raw(key).unwrap().scope_hash("local:root");
    let expected = kimmy_core::idempotency::derived_id(
        kimmy_core::CollectionId::derive("shop", "orders"),
        &scope,
        &fingerprint,
        0,
    );
    assert_eq!(on_a, expected.as_str());

    // The same key with another request is a key reused on A; on a member
    // without the record it is another request, and another document.
    let other = send(&b, key, br#"{"n":2}"#).await;
    assert_eq!(other.status, 422, "{}", other.text);
    let c = node().await;
    let elsewhere = send(&c, key, br#"{"n":2}"#).await.body["insertedId"].clone();
    assert_ne!(elsewhere, on_a, "never another request's document (I7)");
}

/// A key remembered for one request refuses another under it, `422
/// idempotency_key_reused`, `retry: no`, writing nothing: one body byte, the
/// path or the query string is enough to make it another request.
#[tokio::test]
async fn a_different_request_under_a_remembered_key_is_refused_and_writes_nothing() {
    let node = node().await;
    let insert = "/v1/db/shop/coll/orders/docs";
    let k1 = "\"reused-body-6f1c0e52-8a1d\"";
    assert_eq!(
        node.send("POST", insert, Req::key(k1).raw(br#"{"n":1}"#, "application/json")).await.status,
        200
    );
    let k2 = "\"reused-path-6f1c0e52-8a1d\"";
    assert_eq!(
        node.send(
            "PUT",
            "/v1/db/shop/coll/orders/docs/1?upsert=true",
            Req::key(k2).raw(br#"{"n":5}"#, "application/json")
        )
        .await
        .status,
        200
    );
    let count = node.count().await;
    let before = node.metric("kimmy_idempotency_requests_total{outcome=\"reused\"}").await;
    let others: Vec<(&str, &str, &str, &[u8])> = vec![
        ("one value differs", insert, k1, br#"{"n":2}"#),
        ("one byte of spacing differs", insert, k1, br#"{"n": 1}"#),
        ("the path differs", "/v1/db/shop/coll/orders/docs/2?upsert=true", k2, br#"{"n":5}"#),
        ("the query differs", "/v1/db/shop/coll/orders/docs/1", k2, br#"{"n":5}"#),
    ];
    for (what, path, key, body) in &others {
        let method = if *path == insert { "POST" } else { "PUT" };
        let answer = node.send(method, path, Req::key(key).raw(body, "application/json")).await;
        assert_eq!(answer.status, 422, "{what}: {}", answer.text);
        assert_eq!(answer.body["error"], "idempotency_key_reused", "{what}");
        assert_eq!(answer.body["retry"], "no", "{what}");
        assert!(!replayed(&answer), "{what}");
    }
    assert_eq!(node.count().await, count, "nothing written");
    assert_eq!(node.doc("1").await["n"], 5);
    assert_eq!(
        node.metric("kimmy_idempotency_requests_total{outcome=\"reused\"}").await,
        before + others.len() as u64
    );
}

/// The record a peer would send for another request under `key` (another
/// fingerprint, stamped just below this member's, from another member), stored
/// here as replication stores it: the two meet in the merge, and the record
/// turns contested. Returns the record as stored.
fn contest(engine: &Engine, key: &str) -> kimmy_storage::IdempotencyRecord {
    let orders = engine.get_collection("shop", "orders").unwrap();
    let scope = kimmy_core::IdempotencyKey::from_raw(key).unwrap().scope_hash("local:root");
    let live = engine.idempotency_record(orders.id, &scope).unwrap().expect("a record to contest");
    let other = NodeId::from_bytes([0x43; 16]);
    let at = kimmy_core::Stamp::new(kimmy_core::Hlc::new(live.first.hlc.wall_ms - 1, 0), other);
    let theirs = kimmy_storage::IdempotencyRecord {
        fingerprint: kimmy_core::Fingerprint([0x5a; 32]),
        first: at,
        last: at,
        origin: other,
        ..live.clone()
    };
    let stored = engine.store_peer_record_for_test(orders.id, &scope, &theirs).unwrap().unwrap();
    assert!(stored.contested, "premise: two requests met under the key");
    assert_eq!(
        stored.fingerprint, live.fingerprint,
        "premise: this member's record wins the merge"
    );
    stored
}

/// A contested key at the edge: a key two members accepted for two requests, whose records
/// met here in a merge, is refused for every retry, `422
/// idempotency_key_reused`, `retry: no`, writing nothing and never replayed,
/// though the merged record carries this request's own fingerprint and first
/// answer. Neither request's answer can stand for the key.
#[tokio::test]
async fn a_retry_under_a_key_two_requests_met_under_is_refused_and_writes_nothing() {
    let node = node().await;
    let cases: Vec<(&str, &str, &str, &[u8])> = vec![
        ("POST", "/v1/db/shop/coll/orders/docs", "contested-insert-6f1c0e52", br#"{"n":1}"#),
        ("PUT", "/v1/db/shop/coll/orders/docs/1", "contested-replace-6f1c0e52", br#"{"n":5}"#),
        (
            "POST",
            "/v1/db/shop/coll/orders/update",
            "contested-update-6f1c0e52",
            br#"{"filter":{"_id":2},"update":{"$inc":{"n":1}}}"#,
        ),
    ];
    for (method, path, key, body) in &cases {
        let request = || Req::key(&format!("\"{key}\"")).raw(body, "application/json");
        let first = node.send(method, path, request()).await;
        assert_eq!(first.status, 200, "{method} {path}: {}", first.text);
        contest(&node.state.engine, key);
        let count = node.count().await;
        let (one, two) = (node.doc("1").await, node.doc("2").await);
        let before = node.metric("kimmy_idempotency_requests_total{outcome=\"reused\"}").await;
        let retry = node.send(method, path, request()).await;
        assert_eq!(retry.status, 422, "{method} {path}: {}", retry.text);
        assert_eq!(retry.body["error"], "idempotency_key_reused", "{method} {path}");
        assert_eq!(retry.body["retry"], "no", "{method} {path}");
        assert!(!replayed(&retry), "{method} {path}: never the first answer");
        assert_eq!(node.count().await, count, "{method} {path}: nothing written");
        assert_eq!((node.doc("1").await, node.doc("2").await), (one, two), "{method} {path}");
        assert_eq!(
            node.metric("kimmy_idempotency_requests_total{outcome=\"reused\"}").await,
            before + 1,
            "{method} {path}: counted as a key reused"
        );
    }
}

/// A key belongs to a principal: the same key and request from another one is
/// another request, applied on its own, never the first principal's replay.
#[tokio::test]
async fn the_same_key_from_another_principal_applies_independently() {
    let node = node().await;
    let ada = node.user("ada", &["read", "write"]).await;
    let key = "\"scoped-6f1c0e52-8a1d-4f53\"";
    let body: &[u8] = br#"{"n":7}"#;
    let path = "/v1/db/shop/coll/orders/docs";
    let roots = node.send("POST", path, Req::key(key).raw(body, "application/json")).await;
    let adas = node
        .send(
            "POST",
            path,
            Req { token: Some(ada.clone()), ..Req::key(key).raw(body, "application/json") },
        )
        .await;
    assert_eq!((roots.status, adas.status), (200, 200), "{} {}", roots.text, adas.text);
    assert!(!replayed(&adas), "not root's answer");
    assert_ne!(roots.body["insertedId"], adas.body["insertedId"], "two documents");
    // Each principal's retry replays its own.
    let again = node
        .send("POST", path, Req { token: Some(ada), ..Req::key(key).raw(body, "application/json") })
        .await;
    assert!(replayed(&again));
    assert_eq!(again.text, adas.text);
}

/// Authorization comes before the record: a principal whose grant was taken
/// away is refused `403`, never answered from what it did while it held one.
#[tokio::test]
async fn a_revoked_grant_is_refused_before_the_record_is_read() {
    let node = node().await;
    let ada = node.user("ada", &["read", "write"]).await;
    let key = "\"revoked-6f1c0e52-8a1d-4f53\"";
    let request = |token: &str| Req {
        token: Some(token.to_string()),
        ..Req::key(key).raw(br#"{"n":9}"#, "application/json")
    };
    let first = node.send("POST", "/v1/db/shop/coll/orders/docs", request(&ada)).await;
    assert_eq!(first.status, 200, "{}", first.text);
    let revoked = node
        .post(
            "/v1/users/ada/grants",
            json!({ "grants": [{ "db": "shop", "collection": "*", "actions": ["read"] }] }),
        )
        .await;
    assert!(revoked.status / 100 == 2, "{}", revoked.text);
    let ada = node.login_as("ada").await;
    let retry = node.send("POST", "/v1/db/shop/coll/orders/docs", request(&ada)).await;
    assert_eq!(retry.status, 403, "{}", retry.text);
    assert!(!replayed(&retry));
}

/// A request that changed nothing leaves no record, so its retry is evaluated
/// again and answers without the marker.
#[tokio::test]
async fn a_request_that_changed_nothing_is_not_remembered() {
    let node = node().await;
    for (method, path, body) in [
        ("DELETE", "/v1/db/shop/coll/orders/docs/99", None),
        (
            "POST",
            "/v1/db/shop/coll/orders/update",
            Some(r#"{"filter":{"_id":99},"update":{"$set":{"a":1}}}"#),
        ),
        ("POST", "/v1/db/shop/coll/orders/delete", Some(r#"{"filter":{"_id":99},"multi":true}"#)),
    ] {
        let request = || {
            let req = Req::key("\"nothing-6f1c0e52-8a1d-4f53\"");
            match body {
                Some(body) => req.raw(body.as_bytes(), "application/json"),
                None => req,
            }
        };
        let first = node.send(method, path, request()).await;
        let again = node.send(method, path, request()).await;
        assert_eq!((first.status, again.status), (200, 200), "{method} {path}");
        assert_eq!(again.text, first.text);
        assert!(!replayed(&again), "{method} {path}: evaluated again, not replayed");
    }
}

/// One-document chunks over three documents, for the multi-chunk cases.
async fn three_chunk_node() -> Node {
    let node = node().await;
    node.state.engine.set_multi_chunk_docs(1);
    let seeded = node.post("/v1/db/shop/coll/orders/docs", json!({ "_id": 3 })).await;
    assert_eq!(seeded.status, 200);
    node
}

/// A duplicate of a keyed multi-document request still running on this member
/// is refused `409 idempotency_key_in_flight`, `retry: wait`, `Retry-After: 1`,
/// and writes nothing; once the request ends, the same request is its replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_of_a_running_multi_request_is_in_flight_and_then_its_replay() {
    let node = Arc::new(three_chunk_node().await);
    let (started, has_started) = std::sync::mpsc::channel::<()>();
    let (release, released) = std::sync::mpsc::channel::<()>();
    node.state.engine.before_next_continuing_write(move || {
        started.send(()).unwrap();
        released.recv().unwrap();
    });
    let key = "\"in-flight-6f1c0e52-8a1d-4f53\"";
    let body: &'static [u8] = br#"{"filter":{},"update":{"$inc":{"n":1}},"multi":true}"#;
    let path = "/v1/db/shop/coll/orders/update";
    let running = {
        let node = Arc::clone(&node);
        tokio::spawn(async move {
            node.send("POST", path, Req::key(key).raw(body, "application/json")).await
        })
    };
    tokio::task::spawn_blocking(move || has_started.recv().unwrap()).await.unwrap();

    let before = node.metric("kimmy_idempotency_requests_total{outcome=\"in_flight\"}").await;
    let duplicate = node.send("POST", path, Req::key(key).raw(body, "application/json")).await;
    assert_eq!(duplicate.status, 409, "{}", duplicate.text);
    assert_eq!(duplicate.body["error"], "idempotency_key_in_flight");
    assert_eq!(duplicate.body["retry"], "wait");
    assert_eq!(duplicate.headers.get("retry-after").map(|v| v.to_str().unwrap()), Some("1"));
    assert_eq!(
        node.metric("kimmy_idempotency_requests_total{outcome=\"in_flight\"}").await,
        before + 1
    );

    release.send(()).unwrap();
    let first = running.await.unwrap();
    assert_eq!(first.status, 200, "{}", first.text);
    assert_eq!(first.body["matched"], 3, "{}", first.text);
    let again = node.send("POST", path, Req::key(key).raw(body, "application/json")).await;
    assert!(replayed(&again));
    assert_eq!(again.text, first.text);
    for id in ["1", "2", "3"] {
        assert_eq!(node.doc(id).await["n"], 1, "each document incremented once: {id}");
    }
}

/// A keyed multi-document update that stopped part-way resumes when sent again
/// with its key: past the document it reached, leaving the match changed since
/// its first write (counted in `skipped`, not `matched`), with cumulative
/// counts and `resumed: true`; and once done, it replays that answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_multi_update_resumes_with_its_key_and_counts_what_it_left() {
    let node = three_chunk_node().await;
    // Document 2 cannot take the `$inc`, so the request stops after document 1.
    let poisoned = node
        .send("PUT", "/v1/db/shop/coll/orders/docs/2", Req::default().json(&json!({ "n": "two" })))
        .await;
    assert_eq!(poisoned.status, 200);
    let key = "\"resume-6f1c0e52-8a1d-4f53-9b3e\"";
    let body: &[u8] = br#"{"filter":{},"update":{"$inc":{"n":1}},"multi":true}"#;
    let path = "/v1/db/shop/coll/orders/update";
    let stopped = node.send("POST", path, Req::key(key).raw(body, "application/json")).await;
    assert_eq!(stopped.status, 500, "{}", stopped.text);
    assert_eq!(stopped.body["error"], "partially_applied");
    assert_eq!(stopped.body["applied"]["modified"], 1, "{}", stopped.text);

    // Document 2 changes after the request's first write: the resume leaves it.
    let fixed = node
        .send("PUT", "/v1/db/shop/coll/orders/docs/2", Req::default().json(&json!({ "n": 10 })))
        .await;
    assert_eq!(fixed.status, 200);
    let before = node.metric("kimmy_idempotency_requests_total{outcome=\"resumed\"}").await;
    let resumed = node.send("POST", path, Req::key(key).raw(body, "application/json")).await;
    assert_eq!(resumed.status, 200, "{}", resumed.text);
    assert!(!replayed(&resumed), "a resume is processed now, not replayed");
    assert_eq!(resumed.body["resumed"], true, "{}", resumed.text);
    assert_eq!(resumed.body["skipped"], 1, "{}", resumed.text);
    assert_eq!(resumed.body["matched"], 2, "documents 1 and 3, cumulative: {}", resumed.text);
    assert_eq!(resumed.body["modified"], 2, "{}", resumed.text);
    assert_eq!(resumed.body["commits"], 2, "{}", resumed.text);
    assert_eq!(
        node.metric("kimmy_idempotency_requests_total{outcome=\"resumed\"}").await,
        before + 1
    );
    assert_eq!(node.doc("1").await["n"], 1, "written once, by the first attempt");
    assert_eq!(node.doc("2").await["n"], 10, "left as the later write left it");
    assert_eq!(node.doc("3").await["n"], 1, "written by the resume");

    let again = node.send("POST", path, Req::key(key).raw(body, "application/json")).await;
    assert!(replayed(&again), "done now, so replayed");
    assert_eq!(again.text, resumed.text);
}

/// A fresh keyed request that runs past one chunk leaves a match written after
/// its first write, by anyone, and counts it in `skipped`, with no `resumed`:
/// every chunk after the first applies the stamp rule once the request's record
/// exists, since another attempt of it may be running on another member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fresh_multi_chunk_request_leaves_a_match_written_after_its_first_write() {
    let node = three_chunk_node().await;
    let engine = Arc::clone(&node.state.engine);
    // Between chunk one (document 1) and chunk two, another write changes
    // document 3, which the request has not reached yet.
    node.state.engine.before_next_continuing_write(move || {
        let orders = engine.get_collection("shop", "orders").unwrap();
        engine
            .replace(
                &orders,
                &kimmy_core::DocId::Int64(3),
                bson::doc! { "_id": 3i64, "n": 10 },
                false,
            )
            .unwrap();
    });
    let answer = node
        .send(
            "POST",
            "/v1/db/shop/coll/orders/update",
            Req::key("\"fresh-skip-6f1c0e52-8a1d-4f53\"").raw(
                br#"{"filter":{},"update":{"$inc":{"n":1}},"multi":true}"#,
                "application/json",
            ),
        )
        .await;
    assert_eq!(answer.status, 200, "{}", answer.text);
    assert_eq!(answer.body["skipped"], 1, "{}", answer.text);
    assert!(
        answer.body.get("resumed").is_none(),
        "a fresh attempt did not resume: {}",
        answer.text
    );
    assert_eq!(answer.body["matched"], 2, "{}", answer.text);
    assert_eq!(answer.body["modified"], 2, "{}", answer.text);
    assert_eq!(node.doc("3").await["n"], 10, "left as the other write left it");
    assert_eq!(node.doc("2").await["n"], 1);

    // The same request without a key writes every match it reaches, as always.
    let plain = node
        .post(
            "/v1/db/shop/coll/orders/update",
            json!({ "filter": {}, "update": {"$inc": {"n": 1}}, "multi": true }),
        )
        .await;
    assert_eq!(plain.body["matched"], 3, "{}", plain.text);
    assert!(plain.body.get("skipped").is_none(), "{}", plain.text);
}
