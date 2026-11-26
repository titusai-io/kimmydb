//! A host that was told a newer epoch of its node exists is halted, and says so (D13 containment,
//! ADR-274, unit 14a). It stays up: liveness answers, readiness reads not-ready with the reason,
//! and the scrape is served, so an orchestrator does not restart it into a loop and an operator
//! can read why. Every other route, the topology included, answers `503 halted`.
//!
//! The tests commit of the surface: each is red on its designed assertion against the scaffold,
//! whose `Engine::halt` halts nothing.

use std::sync::Arc;

use kimmy_api::SharedState;
use kimmy_auth::TokenIssuer;
use kimmy_core::{Epoch, Hlc};
use kimmy_storage::Engine;
use serde_json::Value;

const ROOT_PASSWORD: &str = "halted-test-password";
const SECRET: &str = "a-halted-test-signing-secret-value";

struct Member {
    base: String,
    engine: Arc<Engine>,
    token: String,
    told: Epoch,
    state: SharedState,
    _dir: tempfile::TempDir,
}

/// A served member, with a token and a collection made before it is halted: a login is a data
/// route, and is refused once the member is.
async fn member() -> Member {
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
    let app = kimmy_api::router_with_limits(
        Arc::clone(&state),
        Some(mcp),
        kimmy_api::RequestLimits {
            request_timeout: std::time::Duration::from_secs(3600),
            ..kimmy_api::RequestLimits::default()
        },
    );
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
    let login = reqwest::Client::new()
        .post(format!("{base}/v1/auth/login"))
        .json(&serde_json::json!({ "user": "root", "password": ROOT_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), 200);
    let body: Value = login.json().await.unwrap();
    let token = body["token"].as_str().unwrap().to_string();
    let created = reqwest::Client::new()
        .post(format!("{base}/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    // The topology's registry, which every real member has: without it the topology route would
    // try to make it, be refused by the halted store, and answer `503 halted` for that reason and
    // not because the gate refused the route.
    engine
        .create_system_collection(
            kimmy_api::topology::NODES_DB,
            kimmy_api::topology::NODES_COLLECTION,
        )
        .unwrap();
    let told = Epoch::new(Hlc::new(kimmy_storage::physical_now_ms() + 1_000, 0), [3; 8]);
    Member { base, engine, token, told, state, _dir: dir }
}

impl Member {
    /// The host is told of a newer epoch of its node and halts.
    fn halt(&self) {
        self.engine.halt(self.told).unwrap();
    }
}

struct Answer {
    status: u16,
    body: Value,
    text: String,
}

async fn send(member: &Member, method: &str, path: &str, body: Option<Value>) -> Answer {
    let client = reqwest::Client::new();
    let url = format!("{}{path}", member.base);
    let request = match method {
        "GET" => client.get(url),
        _ => client.post(url),
    }
    .bearer_auth(&member.token);
    let res = match body {
        Some(body) => request.json(&body).send().await.unwrap(),
        None => request.send().await.unwrap(),
    };
    let status = res.status().as_u16();
    let text = res.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or(Value::Null);
    Answer { status, body, text }
}

async fn get(member: &Member, path: &str) -> Answer {
    send(member, "GET", path, None).await
}

/// Routed requests a halted host refuses: a read, a write, a query, the version, an identity check,
/// the topology, and the MCP endpoint merged beside the table.
fn refused() -> Vec<(&'static str, &'static str, Option<Value>)> {
    vec![
        ("GET", "/v1/version", None),
        ("GET", "/v1/databases", None),
        ("GET", "/v1/db/shop/collections", None),
        ("GET", "/v1/db/shop/coll/orders/docs", None),
        ("POST", "/v1/db/shop/coll/orders/docs", Some(serde_json::json!({ "_id": 1 }))),
        ("POST", "/v1/db/shop/coll/orders/find", Some(serde_json::json!({}))),
        ("GET", "/v1/auth/whoami", None),
        ("GET", "/v1/topology", None),
        ("GET", "/mcp", None),
    ]
}

/// The control: a host that was never told refuses none of them, so the refusals below are the
/// halt's and not the harness's.
#[tokio::test]
async fn a_host_that_was_never_told_serves_every_route() {
    let member = member().await;
    for (method, path, body) in refused() {
        let res = send(&member, method, path, body).await;
        assert_ne!(res.status, 503, "{method} {path}: {}", res.text);
    }
    assert_eq!(get(&member, "/readyz").await.status, 200);
}

/// Reads are refused as well as writes (finding 8b): a halted host that answered reads would serve
/// stale data to clients that the rest of the cluster has moved past.
#[tokio::test]
async fn a_halted_host_refuses_every_routed_request_and_says_why() {
    let member = member().await;
    member.halt();
    for (method, path, body) in refused() {
        let res = send(&member, method, path, body).await;
        assert_eq!(res.status, 503, "{method} {path}: {}", res.text);
        assert_eq!(res.body["error"], "halted", "{method} {path}: {}", res.text);
    }
}

/// A WebSocket upgrade is an ordinary GET to the gate: a halted host opens no stream.
#[tokio::test]
async fn a_halted_host_opens_no_change_stream() {
    let member = member().await;
    member.halt();
    let upgrade = reqwest::Client::new()
        .get(format!("{}/v1/db/shop/coll/orders/watch", member.base))
        .bearer_auth(&member.token)
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .send()
        .await
        .unwrap();
    assert_eq!(upgrade.status(), 503);
}

/// The host stays up and observable (ruled 2026-11-24): liveness answers 200, so an orchestrator
/// does not restart it into a loop, and the scrape is served.
#[tokio::test]
async fn a_halted_host_stays_alive_and_is_scraped() {
    let member = member().await;
    member.halt();
    assert!(member.engine.halted().is_some(), "premise: the host is halted");
    let health = get(&member, "/healthz").await;
    assert_eq!(health.status, 200, "{}", health.text);
    let metrics = get(&member, "/metrics").await;
    assert_eq!(metrics.status, 200, "{}", metrics.text.chars().take(200).collect::<String>());
}

/// Readiness reads not-ready with the reason, and the reason names the epoch the host was told of:
/// the operator reads it here, in the standard envelope, without parsing a message.
#[tokio::test]
async fn a_halted_host_is_not_ready_and_says_why() {
    let member = member().await;
    member.halt();
    let ready = get(&member, "/readyz").await;
    assert_eq!(ready.status, 503, "{}", ready.text);
    assert_eq!(ready.body["error"], "halted", "{}", ready.text);
    assert!(
        ready.body["reason"].as_str().is_some_and(|r| r.contains(&member.told.to_hex())),
        "the reason names the newer epoch the host was told of: {}",
        ready.text
    );
}

// -- streams that are open when the halt lands (6e) ----------------------------------------------

/// Upgrade `path` on `host` by hand and return the socket once the handshake has answered `101`.
async fn open_stream(host: &str, path: &str, token: &str) -> tokio::net::TcpStream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Authorization: Bearer {token}\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.unwrap();
        head.push(byte[0]);
    }
    assert!(String::from_utf8_lossy(&head).starts_with("HTTP/1.1 101"), "the stream opens");
    stream
}

/// The close frame the server sends: its code and its reason, or a failure if none arrives.
async fn close_of(
    stream: &mut tokio::net::TcpStream,
    within: std::time::Duration,
) -> (u16, String) {
    use tokio::io::AsyncReadExt;

    let mut header = [0u8; 2];
    tokio::time::timeout(within, stream.read_exact(&mut header))
        .await
        .expect("the stream is closed")
        .unwrap();
    assert_eq!(header[0] & 0x0f, 8, "a close frame, opcode {}", header[0] & 0x0f);
    let mut payload = vec![0u8; usize::from(header[1] & 0x7f)];
    stream.read_exact(&mut payload).await.unwrap();
    (u16::from_be_bytes([payload[0], payload[1]]), String::from_utf8_lossy(&payload[2..]).into())
}

/// A change stream open on a host that is then halted is closed with the going-away code, naming
/// `halted`: left open it would go on delivering from a store the rest of the cluster has moved
/// past. Before the halt it is quiet.
#[tokio::test]
async fn an_open_change_stream_is_closed_when_the_host_is_halted() {
    let member = member().await;
    let host = member.base.strip_prefix("http://").unwrap().to_string();
    let mut stream = open_stream(&host, "/v1/db/shop/coll/orders/watch", &member.token).await;

    let quiet = tokio::time::timeout(
        std::time::Duration::from_millis(400),
        tokio::io::AsyncReadExt::read_u8(&mut stream),
    )
    .await;
    assert!(quiet.is_err(), "nothing arrives on an idle stream of a host that is not halted");

    member.halt();
    let (code, reason) = close_of(&mut stream, std::time::Duration::from_secs(5)).await;
    assert_eq!(code, 1001, "going away");
    assert_eq!(reason, "halted");
}

/// A stream that gets past the request's gate as the halt lands is closed when its pump starts: the
/// pump's receiver treats the value it subscribed at as seen, so without its own check after
/// subscribing nothing would wake it. The handler is mounted without the gate layer, which is the
/// request that passed the check a moment earlier.
#[tokio::test]
async fn a_change_stream_that_passed_the_gate_as_the_host_halted_is_closed_at_once() {
    let member = member().await;
    member.halt();
    let app = axum::Router::new()
        .route("/w/{db}/coll/{coll}/watch", axum::routing::get(kimmy_api::watch::watch_collection))
        .with_state(Arc::clone(&member.state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let mut stream =
        open_stream(&addr.to_string(), "/w/shop/coll/orders/watch", &member.token).await;
    let (code, reason) = close_of(&mut stream, std::time::Duration::from_secs(5)).await;
    assert_eq!(code, 1001, "going away");
    assert_eq!(reason, "halted");
}
