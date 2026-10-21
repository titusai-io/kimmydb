//! A member that knows it is behind refuses requests, and says so (ADR-202).
//!
//! The defect: a wiped member answered every read from an empty store as soon as
//! it opened, with readiness green and nothing anywhere saying it was behind. A
//! client saw "no such document" for data its peers hold. The marker turns that
//! into a refusal the client can act on (`503 catching_up`, retry elsewhere), on
//! every route but the four an operator or a client's routing needs.

use std::sync::Arc;
use std::time::Duration;

use kimmy_api::SharedState;
use kimmy_auth::TokenIssuer;
use kimmy_cluster::catchup::{CatchUp, STATES};
use kimmy_cluster::{CatchUpReason, Facts, Members};
use kimmy_storage::Engine;
use serde_json::Value;

const ROOT_PASSWORD: &str = "catching-up-test-password";
const SECRET: &str = "a-catching-up-test-signing-secret-value";

struct Member {
    base: String,
    state: SharedState,
    catch_up: Arc<CatchUp>,
    token: String,
    _dirs: (tempfile::TempDir, tempfile::TempDir),
}

/// A served member whose marker is set for `reason` (or not, when `None`), with
/// the wait `wait`. The token is minted first: `/v1/auth/login` is a data route
/// and is refused while gated, and a token from any member of the cluster works.
async fn member(reason: Option<CatchUpReason>, wait: Duration) -> Member {
    let dir = tempfile::tempdir().unwrap();
    let marker_dir = tempfile::tempdir().unwrap();
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
    let catch_up = CatchUp::open(marker_dir.path(), wait);
    // The wait counts from the first sync tick: this is it.
    catch_up.first_tick(std::time::Instant::now());
    state.set_catch_up(Arc::clone(&catch_up));

    // `/mcp` is merged beside the table before its layers, as the daemon merges
    // it; the gate must reach it.
    let mcp = axum::Router::new().route("/mcp", axum::routing::any(|| async { "reached" }));
    let app = kimmy_api::router_with_limits(
        Arc::clone(&state),
        Some(mcp),
        kimmy_api::RequestLimits::default(),
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
    let token = login(&base).await;
    // A collection to route requests to, made before the member is gated.
    let created = reqwest::Client::new()
        .post(format!("{base}/v1/db/shop/collections"))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "name": "orders" }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200);
    if let Some(reason) = reason {
        catch_up.mark(reason).unwrap();
    }
    Member { base, state, catch_up, token, _dirs: (dir, marker_dir) }
}

async fn login(base: &str) -> String {
    let res = reqwest::Client::new()
        .post(format!("{base}/v1/auth/login"))
        .json(&serde_json::json!({ "user": "root", "password": ROOT_PASSWORD }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    body["token"].as_str().unwrap().to_string()
}

struct Answer {
    status: u16,
    header: Option<String>,
    body: Value,
    text: String,
}

async fn get(member: &Member, path: &str) -> Answer {
    let res = reqwest::Client::new()
        .get(format!("{}{path}", member.base))
        .bearer_auth(&member.token)
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    let header = res.headers().get("x-kimmy-catching-up").map(|v| v.to_str().unwrap().to_string());
    let text = res.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or(Value::Null);
    Answer { status, header, body, text }
}

async fn post(member: &Member, path: &str, body: Value) -> Answer {
    let res = reqwest::Client::new()
        .post(format!("{}{path}", member.base))
        .bearer_auth(&member.token)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    let header = res.headers().get("x-kimmy-catching-up").map(|v| v.to_str().unwrap().to_string());
    let text = res.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or(Value::Null);
    Answer { status, header, body, text }
}

/// Routed requests a gated member refuses, by method, path and body: a read, a
/// write, a query, the version (which the deploy tooling polls to decide a rolled
/// member is up), an identity check, and the MCP endpoint merged beside the table.
fn refused() -> Vec<(&'static str, &'static str, Option<Value>)> {
    vec![
        ("GET", "/v1/version", None),
        ("GET", "/v1/databases", None),
        ("GET", "/v1/db/shop/collections", None),
        ("GET", "/v1/db/shop/coll/orders/docs", None),
        ("POST", "/v1/db/shop/coll/orders/docs", Some(serde_json::json!({ "_id": 1 }))),
        ("POST", "/v1/db/shop/coll/orders/find", Some(serde_json::json!({}))),
        ("GET", "/v1/auth/whoami", None),
        ("GET", "/mcp", None),
    ]
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
    let header = res.headers().get("x-kimmy-catching-up").map(|v| v.to_str().unwrap().to_string());
    let text = res.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or(Value::Null);
    Answer { status, header, body, text }
}

#[tokio::test]
async fn a_gated_member_refuses_every_routed_request_and_says_why() {
    for reason in CatchUpReason::ALL {
        let member = member(Some(reason), Duration::from_secs(120)).await;
        for (method, path, body) in refused() {
            let res = send(&member, method, path, body).await;
            assert_eq!(res.status, 503, "{reason:?} {method} {path}: {}", res.text);
            assert_eq!(res.body["error"], "catching_up", "{reason:?} {path}: {}", res.text);
            assert_eq!(res.header.as_deref(), Some(reason.label()), "{reason:?} {path}");
        }
        let login = post(
            &member,
            "/v1/auth/login",
            serde_json::json!({ "user": "root", "password": ROOT_PASSWORD }),
        )
        .await;
        assert_eq!(login.status, 503, "{reason:?}: a login is a data route: {}", login.text);
        // A WebSocket upgrade is an ordinary GET to the gate.
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
        assert_eq!(upgrade.status(), 503, "{reason:?}");
    }
}

/// The refusals are counted and traced like any other response: they pass through
/// the counting layer, and are not invisible to the scrape.
#[tokio::test]
async fn a_gated_refusal_is_counted() {
    async fn requests_total(member: &Member) -> u64 {
        get(member, "/metrics")
            .await
            .text
            .lines()
            .find_map(|l| l.strip_prefix("kimmy_requests_total ")?.trim().parse().ok())
            .expect("a requests counter")
    }
    let member = member(Some(CatchUpReason::SeededEmpty), Duration::from_secs(120)).await;
    let before = requests_total(&member).await;
    let n = refused().len() as u64;
    for (method, path, body) in refused() {
        assert_eq!(send(&member, method, path, body).await.status, 503);
    }
    let after = requests_total(&member).await;
    assert!(after >= before + n, "{n} refusals moved the counter from {before} to {after}");
    let page = get(&member, "/metrics").await.text;
    assert!(
        page.lines().any(|l| l.starts_with("kimmy_responses_total{class=\"5xx\"}")
            && l.rsplit(' ').next().and_then(|v| v.parse::<u64>().ok()).is_some_and(|v| v >= n)),
        "the refusals are 5xx responses:\n{page}"
    );
}

/// The four routes an operator or a client's routing needs are served.
#[tokio::test]
async fn the_probes_the_scrape_and_the_topology_are_still_answered() {
    let member = member(Some(CatchUpReason::SeededEmpty), Duration::from_secs(120)).await;

    let health = get(&member, "/healthz").await;
    assert_eq!(health.status, 200, "{}", health.text);
    assert_eq!(health.header.as_deref(), Some("seeded_empty"));

    let metrics = get(&member, "/metrics").await;
    assert_eq!(metrics.status, 200);
    assert!(metrics.text.contains("kimmy_catching_up{reason=\"seeded_empty\"} 1"));

    let topology = get(&member, "/v1/topology").await;
    assert_eq!(topology.status, 200, "{}", topology.text);
}

/// A gated member is not ready, and says why in the standard envelope; a
/// readiness probe keeps it out of the Service.
#[tokio::test]
async fn a_gated_member_is_not_ready() {
    let member = member(Some(CatchUpReason::Restored), Duration::from_secs(120)).await;
    let ready = get(&member, "/readyz").await;
    assert_eq!(ready.status, 503, "{}", ready.text);
    assert_eq!(ready.body["error"], "catching_up");
    assert_eq!(ready.body["reason"], "restored", "the reason rides beside the code");
    assert_eq!(ready.header.as_deref(), Some("restored"));

    member.catch_up.clear("the test");
    let ready = get(&member, "/readyz").await;
    assert_eq!(ready.status, 200, "{}", ready.text);
    assert_eq!(ready.header, None, "no header once it has cleared");
    assert!(ready.body.get("catching_up").is_none(), "the body is what it always was");
}

/// `/metrics` is one-hot over the states, exactly one series is 1, and it moves
/// with the marker: `none` on a member with no marker, the reason while it holds,
/// `unknown` once the wait has run out, `none` again when it clears.
#[tokio::test]
async fn the_gauge_is_one_hot_and_follows_the_marker() {
    async fn hot(member: &Member) -> Vec<String> {
        let metrics = get(member, "/metrics").await;
        let ones: Vec<String> = metrics
            .text
            .lines()
            .filter(|l| l.starts_with("kimmy_catching_up{"))
            .filter(|l| l.ends_with(" 1"))
            .map(|l| l.to_string())
            .collect();
        let all = metrics.text.lines().filter(|l| l.starts_with("kimmy_catching_up{")).count();
        assert_eq!(all, STATES.len(), "every reason is always rendered:\n{}", metrics.text);
        ones
    }

    let member = member(None, Duration::from_millis(2000)).await;
    assert_eq!(hot(&member).await, ["kimmy_catching_up{reason=\"none\"} 1"]);

    member.catch_up.mark(CatchUpReason::Snapshot).unwrap();
    assert_eq!(hot(&member).await, ["kimmy_catching_up{reason=\"snapshot\"} 1"]);

    tokio::time::sleep(Duration::from_millis(2400)).await;
    assert_eq!(hot(&member).await, ["kimmy_catching_up{reason=\"unknown\"} 1"]);

    member.catch_up.clear("the test");
    assert_eq!(hot(&member).await, ["kimmy_catching_up{reason=\"none\"} 1"]);
}

/// Past the wait the member serves, still marked `unknown` on every response and
/// in `/readyz`: refusing forever on a guess would be worse than serving, and the
/// header is how a caller through a load balancer still sees it.
#[tokio::test]
async fn past_the_bound_the_member_serves_and_says_unknown() {
    let member = member(Some(CatchUpReason::SeededEmpty), Duration::from_millis(2000)).await;
    assert_eq!(get(&member, "/v1/version").await.status, 503);

    tokio::time::sleep(Duration::from_millis(2400)).await;
    let version = get(&member, "/v1/version").await;
    assert_eq!(version.status, 200, "{}", version.text);
    assert_eq!(version.header.as_deref(), Some("unknown"));
    let ready = get(&member, "/readyz").await;
    assert_eq!(ready.status, 200, "{}", ready.text);
    assert_eq!(ready.body["catching_up"], "unknown");
}

/// A member with no marker sends no header and refuses nothing.
#[tokio::test]
async fn a_member_with_no_marker_is_unchanged() {
    let member = member(None, Duration::from_secs(120)).await;
    for path in [
        "/v1/version",
        "/v1/databases",
        "/v1/db/shop/collections",
        "/readyz",
        "/healthz",
        "/metrics",
    ] {
        let res = get(&member, path).await;
        assert_ne!(res.status, 503, "{path}: {}", res.text);
        assert_eq!(res.header, None, "{path}");
    }
}

/// `/v1/topology` marks the member itself with the state, and a peer that says it
/// is catching up as `true`, so a client that routes by topology can avoid both;
/// a member and a peer that are fine carry no such key.
#[tokio::test]
async fn the_topology_marks_a_member_and_a_peer_that_are_catching_up() {
    let member = member(None, Duration::from_secs(120)).await;
    kimmy_api::topology::register(&member.state, "http://127.0.0.1:1").unwrap();
    // A peer the registry lists and SWIM holds, whose last block says it is
    // catching up, and one whose block says it is fine.
    let (behind, fine) =
        (kimmy_core::NodeId::from_bytes([7; 16]), kimmy_core::NodeId::from_bytes([8; 16]));
    let members = Members::default();
    members.insert_for_test("127.0.0.1:7007".parse().unwrap(), behind);
    members.insert_for_test("127.0.0.1:7008".parse().unwrap(), fine);
    members.record_peer_facts_for_test(
        behind,
        Facts { boot: vec![7; 16], catching_up: true, ..Facts::default() },
        Duration::ZERO,
    );
    members.record_peer_facts_for_test(
        fine,
        Facts { boot: vec![8; 16], ..Facts::default() },
        Duration::ZERO,
    );
    member.state.set_members(members);
    let meta = member
        .state
        .engine
        .get_collection(kimmy_api::topology::NODES_DB, kimmy_api::topology::NODES_COLLECTION)
        .unwrap();
    for node in [behind, fine] {
        let id = node.to_string();
        member
            .state
            .engine
            .replace(
                &meta,
                &kimmy_core::DocId::String(id.clone()),
                bson::doc! { "_id": id, "endpoint": "http://127.0.0.1:2", "version": "0" },
                true,
            )
            .unwrap();
    }

    let listed = |body: &Value, node: &str| -> Value {
        body["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["node"] == node)
            .unwrap_or_else(|| panic!("{node} is listed: {body}"))
            .clone()
    };
    let me = member.state.engine.node_id().to_string();

    let clear = get(&member, "/v1/topology").await;
    assert_eq!(clear.status, 200, "{}", clear.text);
    assert!(listed(&clear.body, &me).get("catchingUp").is_none());
    assert_eq!(listed(&clear.body, &behind.to_string())["catchingUp"], true);
    assert!(listed(&clear.body, &fine.to_string()).get("catchingUp").is_none());

    member.catch_up.mark(CatchUpReason::Restored).unwrap();
    let marked = get(&member, "/v1/topology").await;
    assert_eq!(listed(&marked.body, &me)["catchingUp"], "restored", "{}", marked.text);
}

/// The gate the wait had opened closing again (a counting peer that is ahead is
/// reached) ends open streams too: a stream opened while the member served as
/// `unknown` is closed with the going-away code when it starts refusing again.
#[tokio::test]
async fn an_open_change_stream_is_closed_when_the_gate_closes_again() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // A wait long enough that the gate, closed again by the tick below, is still
    // closed when the stream is looked at, on a loaded host too.
    let member = member(Some(CatchUpReason::SeededEmpty), Duration::from_millis(1500)).await;
    tokio::time::sleep(Duration::from_millis(1800)).await;
    assert!(member.catch_up.unknown(std::time::Instant::now()), "premise: serving as unknown");

    let host = member.base.strip_prefix("http://").unwrap();
    let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
    let request = format!(
        "GET /v1/db/shop/coll/orders/watch HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n\
         Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Authorization: Bearer {}\r\n\r\n",
        member.token
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.unwrap();
        head.push(byte[0]);
    }
    assert!(String::from_utf8_lossy(&head).starts_with("HTTP/1.1 101"), "opens while unknown");

    // A counting peer that is ahead is reached: the gate closes again.
    let (me, ahead) = (member.state.engine.node_id(), kimmy_core::NodeId::from_bytes([3; 16]));
    let mut theirs = kimmy_core::VersionVector::new();
    theirs.insert(ahead, kimmy_core::Hlc::new(1_000, 0));
    let reached = [kimmy_cluster::catchup::Reached {
        node: ahead,
        servable: theirs,
        witnessed: None,
        facts: None,
    }];
    let mine = kimmy_core::VersionVector::new();
    let now = std::time::Instant::now();
    member.catch_up.evaluate(&kimmy_cluster::catchup::Tick {
        me,
        reached: &reached,
        mine_witnessed: &mine,
        mine_servable: &mine,
        snapshot_pending: false,
        live: None,
        expected_members: None,
        now,
    });
    assert!(member.catch_up.gated(now), "premise: gated again");

    let mut header = [0u8; 2];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut header))
        .await
        .expect("the stream is closed when the gate closes again")
        .unwrap();
    assert_eq!(header[0] & 0x0f, 8, "a close frame");
    let mut payload = vec![0u8; usize::from(header[1] & 0x7f)];
    stream.read_exact(&mut payload).await.unwrap();
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1001);
    assert_eq!(String::from_utf8_lossy(&payload[2..]), "catching_up");
}

/// A change stream open on a member that becomes gated is closed with a going-away
/// code that names `catching_up`: left open it would go on delivering from a store
/// that is now known to be behind. A stream on a member that is not gated is not
/// touched.
#[tokio::test]
async fn an_open_change_stream_is_closed_when_the_member_becomes_gated() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let member = member(None, Duration::from_secs(120)).await;
    let host = member.base.strip_prefix("http://").unwrap();
    let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
    let request = format!(
        "GET /v1/db/shop/coll/orders/watch HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n\
         Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Authorization: Bearer {}\r\n\r\n",
        member.token
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.unwrap();
        head.push(byte[0]);
    }
    assert!(String::from_utf8_lossy(&head).starts_with("HTTP/1.1 101"), "the stream opens");

    // Quiet while the member is not gated.
    let quiet = tokio::time::timeout(Duration::from_millis(400), stream.read_u8()).await;
    assert!(quiet.is_err(), "nothing arrives on an idle stream of a member that is not gated");

    member.catch_up.mark(CatchUpReason::SeededEmpty).unwrap();
    let mut header = [0u8; 2];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut header))
        .await
        .expect("the stream is closed once the member is gated")
        .unwrap();
    assert_eq!(header[0] & 0x0f, 8, "a close frame, opcode {}", header[0] & 0x0f);
    let mut payload = vec![0u8; usize::from(header[1] & 0x7f)];
    stream.read_exact(&mut payload).await.unwrap();
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1001, "going away");
    assert_eq!(String::from_utf8_lossy(&payload[2..]), "catching_up");
}

/// A stream that gets past the request's gate check just as the member becomes
/// gated is closed when its pump starts: the pump's receiver treats the value it
/// subscribed at as seen, so without its own check after subscribing nothing would
/// ever wake it. The handler is mounted without the gate layer, which is exactly
/// the request that passed the check a moment earlier.
#[tokio::test]
async fn a_change_stream_that_passed_the_check_as_the_gate_closed_is_closed_at_once() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let member = member(Some(CatchUpReason::SeededEmpty), Duration::from_secs(120)).await;
    let app = axum::Router::new()
        .route("/w/{db}/coll/{coll}/watch", axum::routing::get(kimmy_api::watch::watch_collection))
        .with_state(Arc::clone(&member.state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "GET /w/shop/coll/orders/watch HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\n\
         Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Authorization: Bearer {}\r\n\r\n",
        member.token
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.unwrap();
        head.push(byte[0]);
    }
    assert!(String::from_utf8_lossy(&head).starts_with("HTTP/1.1 101"), "the handler upgrades");
    let mut header = [0u8; 2];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut header))
        .await
        .expect("closed at once, not left open")
        .unwrap();
    assert_eq!(header[0] & 0x0f, 8, "a close frame");
    let mut payload = vec![0u8; usize::from(header[1] & 0x7f)];
    stream.read_exact(&mut payload).await.unwrap();
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1001);
    assert_eq!(String::from_utf8_lossy(&payload[2..]), "catching_up");
}

/// A member with no membership has no block to carry the catching-up bit, so its
/// own view reads `unowned_catching_up` for the TTL collections it holds while its
/// marker is set, and `owned` before and after. The gauge is read from the same
/// gated view the expiry loop uses.
#[tokio::test]
async fn a_member_without_membership_reads_its_ttl_collections_as_waiting_while_gated() {
    use kimmy_api::ownership::TtlState;

    let member = member(None, Duration::from_secs(120)).await;
    member.state.engine.create_collection("app", "sessions").unwrap();
    member
        .state
        .engine
        .create_index_with(
            "app",
            "sessions",
            vec![kimmy_storage::IndexField::ascending("seen")],
            false,
            Default::default(),
            Some("ttl_seen".into()),
            Some(60),
            None,
        )
        .unwrap();
    let slots = |m: &Member| m.state.storage_readings().unwrap().ownership.ttl_collections;
    assert_eq!(slots(&member)[TtlState::Owned.slot()], 1);

    member.catch_up.mark(CatchUpReason::SeededEmpty).unwrap();
    let gated = slots(&member);
    assert_eq!(gated[TtlState::UnownedCatchingUp.slot()], 1, "{gated:?}");
    assert_eq!(gated[TtlState::Owned.slot()], 0);

    member.catch_up.clear("the test");
    assert_eq!(slots(&member)[TtlState::Owned.slot()], 1);
}

/// Drive `catch_up` into `unknown (owed)` with virtual instants (ADR-212): B
/// (node 1) reached and answered, C (node 2) listed live and never answering,
/// judged five seconds after the arming and past C's hold. The routes read the
/// latch, a flag, so nothing sleeps. Returns C's id.
fn into_owed_replay(catch_up: &CatchUp) -> kimmy_core::NodeId {
    use kimmy_cluster::catchup::{Decision, Reached, ReplayTick, Tick, UnknownCause};
    use kimmy_core::{Hlc, NodeId, VersionVector};
    let id = |n: u8| NodeId::from_bytes([n; 16]);
    let (me, b, c) = (id(9), id(1), id(2));
    let members = Members::default();
    members.insert_for_test("127.0.0.1:7001".parse().unwrap(), b);
    members.insert_for_test("127.0.0.1:7002".parse().unwrap(), c);
    let live = members.node_ids();
    let mut servable = VersionVector::new();
    servable.insert(b, Hlc::new(100, 0));
    let mut mine = servable.clone();
    mine.insert(me, Hlc::new(200, 0));
    let reached = [Reached { node: b, servable, witnessed: None, facts: None }];
    let t0 = std::time::Instant::now();
    catch_up.arm_replay_at(Hlc::new(50, 0), None, t0).unwrap();
    catch_up.replay_finished(b);
    for at in [5, 130] {
        let now = t0 + Duration::from_secs(at);
        catch_up.replay_note_members(&ReplayTick {
            me,
            members: Some(&members),
            reached: &reached,
            expected_members: None,
            now,
        });
        catch_up.replay_settle();
        let decision = catch_up.evaluate(&Tick {
            me,
            reached: &reached,
            mine_witnessed: &mine,
            mine_servable: &mine,
            snapshot_pending: false,
            live: Some(&live),
            expected_members: None,
            now,
        });
        assert_eq!(decision, Decision::Kept, "{at} s");
    }
    assert_eq!(
        catch_up.unknown_because(t0 + Duration::from_secs(130)),
        Some(UnknownCause::OwedReplay),
        "premise: C owes the replay past its hold"
    );
    c
}

fn keys(body: &Value) -> std::collections::BTreeSet<String> {
    body.as_object().map(|o| o.keys().cloned().collect()).unwrap_or_default()
}

/// A member serving as `unknown` because a member owes it the replay says so in
/// `/readyz`, by cause and by member, with exactly these keys; a data route is
/// served, with the header (ADR-212).
#[tokio::test]
async fn in_owed_replay_readyz_names_the_cause_and_the_members_that_owe() {
    let member = member(Some(CatchUpReason::Restored), Duration::from_secs(120)).await;
    let owed = into_owed_replay(&member.catch_up);
    let ready = get(&member, "/readyz").await;
    assert_eq!(ready.status, 200, "{}", ready.text);
    assert_eq!(
        keys(&ready.body),
        ["catching_up", "node", "owed_members", "status", "unknown_because"]
            .map(String::from)
            .into(),
        "{}",
        ready.text
    );
    assert_eq!(ready.body["catching_up"], "unknown");
    assert_eq!(ready.body["unknown_because"], "owed_replay");
    assert_eq!(ready.body["owed_members"], serde_json::json!([owed.to_string()]));
    assert_eq!(ready.header.as_deref(), Some("unknown"));

    let version = get(&member, "/v1/version").await;
    assert_eq!(version.status, 200, "{}", version.text);
    assert_eq!(version.header.as_deref(), Some("unknown"));
}

/// With no counting peer for the wait, `/readyz` gives the other cause, and the
/// same keys.
#[tokio::test]
async fn with_no_counting_peer_readyz_names_that_cause() {
    let member = member(Some(CatchUpReason::SeededEmpty), Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let ready = get(&member, "/readyz").await;
    assert_eq!(ready.status, 200, "{}", ready.text);
    assert_eq!(
        keys(&ready.body),
        ["catching_up", "node", "owed_members", "status", "unknown_because"]
            .map(String::from)
            .into(),
        "{}",
        ready.text
    );
    assert_eq!(ready.body["unknown_because"], "no_counting_peer");
    assert_eq!(ready.body["owed_members"], serde_json::json!([]));
}

/// While gated with members owing the replay, the `503` names them beside the
/// reason: that is where an operator looks during the hold.
#[tokio::test]
async fn a_gated_member_that_is_owed_the_replay_names_the_members_in_the_refusal() {
    use kimmy_cluster::catchup::{Reached, ReplayTick};
    let member = member(Some(CatchUpReason::Restored), Duration::from_secs(120)).await;
    let c = kimmy_core::NodeId::from_bytes([2; 16]);
    let members = Members::default();
    members.insert_for_test("127.0.0.1:7002".parse().unwrap(), c);
    let now = std::time::Instant::now();
    member.catch_up.arm_replay_at(kimmy_core::Hlc::new(50, 0), None, now).unwrap();
    let reached: [Reached; 0] = [];
    member.catch_up.replay_note_members(&ReplayTick {
        me: kimmy_core::NodeId::from_bytes([9; 16]),
        members: Some(&members),
        reached: &reached,
        expected_members: None,
        now,
    });
    let ready = get(&member, "/readyz").await;
    assert_eq!(ready.status, 503, "{}", ready.text);
    assert_eq!(ready.body["reason"], "restored");
    assert_eq!(ready.body["owed_members"], serde_json::json!([c.to_string()]), "{}", ready.text);
}
