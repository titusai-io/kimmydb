//! `Idempotency-Key` at the HTTP edge (B5): which routes take it and which
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
            builder = builder.bearer_auth(&self.token);
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

/// The switch on, clustered: a live peer that has said nothing yet, or whose
/// block lacks `idempotency_records` (an older release), closes the gate, and
/// the refusal names it; once it says it carries records, keys are taken and the
/// capability is listed.
#[tokio::test]
async fn an_older_or_unheard_live_peer_refuses_naming_it_until_it_carries_records() {
    let node = node().await;
    let members = Members::default();
    let gate = IdempotencyGate::new(true, true, true);
    gate.set_members(members.clone());
    node.state.set_idempotency_gate(gate.predicate());

    let insert =
        || node.send("POST", "/v1/db/shop/coll/orders/docs", Req::key(KEY).json(&json!({})));
    assert_eq!(insert().await.status, 200, "alone, the switch decides");

    let (addr, id) = peer(2);
    members.insert_for_test(addr, id);
    for (what, block) in [("not heard from", None), ("an older block", Some(carries(None)))] {
        if let Some(block) = block {
            members.record_peer_facts_for_test(id, block, Duration::ZERO);
        }
        let answer = insert().await;
        assert_eq!(answer.status, 400, "{what}: {}", answer.text);
        let message = answer.body["message"].as_str().unwrap();
        assert!(message.contains(&id.to_string()), "{what}: names the member: {message}");
        assert!(message.contains("Idempotency-Key"), "{what}: {message}");
        assert!(!node.capabilities().await.contains(&"idempotency-keys".to_string()), "{what}");
    }

    members.record_peer_facts_for_test(id, carries(Some(true)), Duration::ZERO);
    let answer = insert().await;
    assert_eq!(answer.status, 200, "{}", answer.text);
    assert!(node.capabilities().await.contains(&"idempotency-keys".to_string()));
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

/// Every series B5 adds is on the page from the start, at zero, the ones the
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
