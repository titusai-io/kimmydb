//! End-to-end API tests.
//!
//! These drive the real router over a real TCP socket rather than calling
//! handlers directly, so routing, extractors, status codes, and the JSON
//! boundary are all exercised the way a client meets them.

use std::net::SocketAddr;
use std::sync::Arc;

use kimmy_auth::TokenIssuer;
use kimmy_storage::Engine;
use serde_json::{Value, json};

const SECRET: &str = "an-adequately-long-test-secret-for-hs256";
const ROOT_PASSWORD: &str = "root-password";

struct Server {
    base: String,
    client: Client,
    /// The server's own state, for the few tests whose subject is something a
    /// request cannot reach — the live member set, which only exists once a
    /// cluster has started.
    state: kimmy_api::SharedState,
    /// Shared, so a server "restarted" over the same database keeps the
    /// directory alive for as long as either handle does.
    _dir: Arc<tempfile::TempDir>,
}

/// A tiny HTTP client, so the tests do not pull in a dependency purely to make
/// half a dozen requests.
struct Client;

struct Res {
    status: u16,
    body: Value,
    /// The raw response head, so a test can assert on a header without the
    /// client growing a parser it would only use once.
    head: String,
}

impl Res {
    /// Case-insensitive header lookup over the raw head.
    fn header(&self, name: &str) -> Option<String> {
        let want = format!("{}:", name.to_ascii_lowercase());
        self.head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (format!("{}:", key.trim().to_ascii_lowercase()) == want)
                .then(|| value.trim().to_string())
        })
    }
}

impl Client {
    async fn request(
        &self,
        method: &str,
        url: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> Res {
        let rest = url.strip_prefix("http://").expect("http url");
        let (host, path) = rest.split_once('/').expect("path");
        let path = format!("/{path}");

        let payload = body.map(|b| b.to_string()).unwrap_or_default();

        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n",
            payload.len()
        );
        if let Some(token) = token {
            request.push_str(&format!("Authorization: Bearer {token}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(&payload);

        // A reset with nothing read is retried once. The server drains a body
        // it refuses before it answers, so an in-flight request is normally
        // read to its end and the connection closes cleanly; but a body past
        // the drain cap is still closed on with bytes unread, and the RST
        // that answers those can, on macOS, discard a response that had
        // already arrived. The response was sent, and the request it
        // answered was one the server never read to the end — so making it
        // once more is what a real client would do, and what these tests
        // are about is the status.
        let raw = match Self::exchange(host, request.as_bytes()).await {
            Ok(raw) => raw,
            Err(first) => match Self::exchange(host, request.as_bytes()).await {
                Ok(raw) => raw,
                Err(e) => panic!("read: {e:?} (after {first:?} on the first attempt)"),
            },
        };
        let text = String::from_utf8_lossy(&raw).into_owned();

        let (head, body_text) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        let status = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Responses are Connection: close, so the body is everything left; no
        // chunked decoding needed.
        let body = serde_json::from_str(body_text.trim()).unwrap_or(Value::Null);
        Res { status, body, head: head.to_string() }
    }

    /// One request on one connection, answered with whatever the server sent
    /// before the connection ended.
    ///
    /// A reset *after* the response is end-of-stream, not a failure: a body
    /// the server did not finish reading is answered and then closed on, so
    /// the bytes still in flight draw an RST, and on macOS that surfaces here
    /// as `ConnectionReset` rather than a clean EOF. A real client reads what
    /// arrived and moves on; reading anything at all is what says the
    /// response was received. The same goes for a write the server cut short
    /// — the response may be sitting there regardless, so it is read for.
    /// A reset with nothing read is the failure, and the caller's to judge.
    async fn exchange(host: &str, request: &[u8]) -> std::io::Result<Vec<u8>> {
        use std::io::ErrorKind::{BrokenPipe, ConnectionReset};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = tokio::net::TcpStream::connect(host).await.expect("connect");
        if let Err(e) = stream.write_all(request).await
            && !matches!(e.kind(), BrokenPipe | ConnectionReset)
        {
            panic!("write: {e:?}");
        }
        let mut raw = Vec::new();
        match stream.read_to_end(&mut raw).await {
            Ok(_) => Ok(raw),
            Err(e) if e.kind() == ConnectionReset && !raw.is_empty() => Ok(raw),
            Err(e) => Err(e),
        }
    }
}

impl Server {
    async fn start() -> Self {
        Self::start_with(false).await
    }

    async fn start_with(insecure_no_auth: bool) -> Self {
        // Most tests log in repeatedly and would otherwise trip the limiter for
        // reasons unrelated to what they assert.
        Self::build(insecure_no_auth, kimmy_api::RateLimits::disabled()).await
    }

    /// A server whose login limiter allows `burst` failures per minute.
    async fn start_rate_limited(burst: u32) -> Self {
        let limits = kimmy_api::RateLimits {
            login_ip: kimmy_api::Limiter::new(
                kimmy_api::RateLimit::new(burst, std::time::Duration::from_secs(60)),
                1024,
            ),
            ..kimmy_api::RateLimits::disabled()
        };
        Self::build(false, limits).await
    }

    /// A server that limits per username rather than per address.
    async fn start_user_rate_limited(burst: u32) -> Self {
        let limits = kimmy_api::RateLimits {
            login_user: kimmy_api::Limiter::new(
                kimmy_api::RateLimit::new(burst, std::time::Duration::from_secs(60)),
                1024,
            ),
            ..kimmy_api::RateLimits::disabled()
        };
        Self::build(false, limits).await
    }

    /// Every authenticated request per principal, `burst` per minute
    /// (ADR-099).
    fn principal_limits(burst: u32) -> kimmy_api::RateLimits {
        kimmy_api::RateLimits {
            per_principal: kimmy_api::Limiter::new(
                kimmy_api::RateLimit::new(burst, std::time::Duration::from_secs(60)),
                1024,
            ),
            ..kimmy_api::RateLimits::disabled()
        }
    }

    /// A server whose authenticated routes carry a per-principal budget.
    async fn start_principal_rate_limited(burst: u32) -> Self {
        Self::build(false, Self::principal_limits(burst)).await
    }

    /// A server with a chosen request deadline and body ceiling (ADR-099).
    async fn start_with_limits(request_limits: kimmy_api::RequestLimits) -> Self {
        Self::build_with(false, kimmy_api::RateLimits::disabled(), request_limits).await
    }

    /// A server that also federates with the stub identity provider below.
    ///
    /// The provider is a fixed key pair rather than a live IdP: the subject is
    /// how this node treats a token, and a test that needs somebody's identity
    /// provider reachable fails for reasons that have nothing to do with it.
    async fn start_federated() -> Self {
        Self::start_federated_for(oidc::AUDIENCE).await
    }

    /// The same, with the audience chosen — which is what decides whether this
    /// node names itself as an OAuth 2.0 protected resource (ADR-071).
    async fn start_federated_for(audience: &str) -> Self {
        Self::start_federated_with(audience, None, kimmy_api::RateLimits::disabled()).await
    }

    /// A federated server whose provider names its people by `claim` (ADR-100).
    async fn start_federated_with_subject_claim(claim: &str) -> Self {
        Self::start_federated_with(
            oidc::AUDIENCE,
            Some(claim.to_string()),
            kimmy_api::RateLimits::disabled(),
        )
        .await
    }

    /// The same, with the limiters chosen — for the per-principal limit, whose
    /// key has to tell a federated subject from a local user (ADR-099).
    async fn start_federated_with(
        audience: &str,
        subject_claim: Option<String>,
        limits: kimmy_api::RateLimits,
    ) -> Self {
        let server = Self::build(false, limits).await;
        let verifier = kimmy_auth::OidcVerifier::new(kimmy_auth::OidcSettings {
            issuer: oidc::ISSUER.into(),
            audience: audience.into(),
            roles_claim: "roles".into(),
            role_mappings: vec![kimmy_auth::RoleMapping {
                claim_value: "kimmydb-analyst".into(),
                role: None,
                grants: vec![kimmy_auth::Grant::new(
                    "sales",
                    "orders*",
                    vec![kimmy_auth::Action::Read],
                )],
            }],
            require_at_jwt: false,
            allow_federated_admin: false,
            subject_claim,
        })
        .unwrap();
        let federation = kimmy_api::Federation::new(verifier);
        federation.install_keys(oidc::jwks());
        server.state.set_federation(federation);
        server
    }

    /// A federated server whose one mapping names a **stored** role rather than
    /// carrying grants inline (ADR-073).
    ///
    /// `allow_federated_admin` is a parameter because it is the whole subject of
    /// ADR-074, and because it must be read where the role is *resolved* rather
    /// than captured at startup — a stored role can gain `admin` at any time.
    async fn start_federated_with_role(role: &str, allow_federated_admin: bool) -> Self {
        let server = Self::build(false, kimmy_api::RateLimits::disabled()).await;
        let verifier = kimmy_auth::OidcVerifier::new(kimmy_auth::OidcSettings {
            issuer: oidc::ISSUER.into(),
            audience: oidc::AUDIENCE.into(),
            roles_claim: "roles".into(),
            role_mappings: vec![kimmy_auth::RoleMapping {
                claim_value: "kimmydb-analyst".into(),
                role: Some(role.into()),
                grants: Vec::new(),
            }],
            require_at_jwt: false,
            allow_federated_admin,
            subject_claim: None,
        })
        .unwrap();
        let federation = kimmy_api::Federation::new(verifier);
        federation.install_keys(oidc::jwks());
        server.state.set_federation(federation);
        server
    }

    async fn build(insecure_no_auth: bool, limits: kimmy_api::RateLimits) -> Self {
        Self::build_with(insecure_no_auth, limits, kimmy_api::RequestLimits::default()).await
    }

    async fn build_with(
        insecure_no_auth: bool,
        limits: kimmy_api::RateLimits,
        request_limits: kimmy_api::RequestLimits,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());

        if !insecure_no_auth {
            let users = kimmy_auth::UserStore::open(&engine).unwrap();
            users.bootstrap_root(&engine, "root", ROOT_PASSWORD).unwrap();
        }

        let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
        let state =
            kimmy_api::state(Arc::clone(&engine), tokens, insecure_no_auth, limits).unwrap();
        Self::serve(state, Arc::new(dir), request_limits).await
    }

    /// The same database served again under a different token issuer — a node
    /// restarted with a new signing configuration (ADR-101).
    ///
    /// The engine handle is shared rather than the file reopened: the first
    /// listener is still alive and holds the file, and what a restart preserves
    /// is the stored state — users and their token versions — which is exactly
    /// what sharing the engine preserves. Everything in memory is rebuilt, the
    /// session cache included, as it would be across a real restart.
    async fn restart_with_issuer(&self, tokens: TokenIssuer) -> Self {
        let engine = Arc::clone(&self.state.engine);
        let state =
            kimmy_api::state(engine, tokens, false, kimmy_api::RateLimits::disabled()).unwrap();
        Self::serve(state, Arc::clone(&self._dir), kimmy_api::RequestLimits::default()).await
    }

    /// A server with a router merged beside the REST table, the way the daemon
    /// merges `/mcp` — for the tests whose subject is what that position does
    /// and does not put a route inside.
    async fn start_with_extra(extra: axum::Router) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let users = kimmy_auth::UserStore::open(&engine).unwrap();
        users.bootstrap_root(&engine, "root", ROOT_PASSWORD).unwrap();
        let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
        let state =
            kimmy_api::state(Arc::clone(&engine), tokens, false, kimmy_api::RateLimits::disabled())
                .unwrap();
        Self::serve_with(state, Arc::new(dir), kimmy_api::RequestLimits::default(), Some(extra))
            .await
    }

    async fn serve(
        state: kimmy_api::SharedState,
        dir: Arc<tempfile::TempDir>,
        request_limits: kimmy_api::RequestLimits,
    ) -> Self {
        Self::serve_with(state, dir, request_limits, None).await
    }

    async fn serve_with(
        state: kimmy_api::SharedState,
        dir: Arc<tempfile::TempDir>,
        request_limits: kimmy_api::RequestLimits,
        extra: Option<axum::Router>,
    ) -> Self {
        let app = kimmy_api::router_with_limits(Arc::clone(&state), extra, request_limits);

        // Port 0: let the OS pick, so parallel tests never collide.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // Served with connect info exactly as the daemon serves it, so the
            // peer address really does reach the limiter. Without this the
            // rate-limit tests would pass against a single shared bucket and
            // prove nothing about keying.
            let _ = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .await;
        });

        Self { base: format!("http://{addr}"), client: Client, state, _dir: dir }
    }

    async fn get(&self, path: &str, token: Option<&str>) -> Res {
        self.client.request("GET", &format!("{}{path}", self.base), token, None).await
    }

    async fn post(&self, path: &str, token: Option<&str>, body: Value) -> Res {
        self.client.request("POST", &format!("{}{path}", self.base), token, Some(body)).await
    }

    async fn put(&self, path: &str, token: Option<&str>, body: Value) -> Res {
        self.client.request("PUT", &format!("{}{path}", self.base), token, Some(body)).await
    }

    async fn delete(&self, path: &str, token: Option<&str>) -> Res {
        self.client.request("DELETE", &format!("{}{path}", self.base), token, None).await
    }

    async fn login(&self, user: &str, password: &str) -> String {
        let res =
            self.post("/v1/auth/login", None, json!({ "user": user, "password": password })).await;
        assert_eq!(res.status, 200, "login failed: {:?}", res.body);
        res.body["token"].as_str().expect("token").to_string()
    }

    async fn root(&self) -> String {
        self.login("root", ROOT_PASSWORD).await
    }

    /// Drive one request through the router as though it arrived from `peer`.
    ///
    /// The real socket in `build` can only ever produce a loopback peer, which
    /// is exactly the case `auth.local.login = "loopback_only"` admits. To
    /// exercise the refusal, the request is handed to the router directly with
    /// the connect-info extension a listener would have attached — the same
    /// extension `ClientAddr` and `LocalMinting` read, set by hand.
    async fn request_from(
        &self,
        peer: &str,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
    ) -> Res {
        use http_body_util::BodyExt as _;
        use tower::ServiceExt as _;

        let peer: SocketAddr = peer.parse().expect("a socket address");
        let mut request = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json");
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let payload = body.map(|b| b.to_string()).unwrap_or_default();
        let mut request = request.body(axum::body::Body::from(payload)).expect("a request");
        request.extensions_mut().insert(axum::extract::ConnectInfo(peer));

        let response =
            kimmy_api::router(Arc::clone(&self.state)).oneshot(request).await.expect("router");
        let status = response.status().as_u16();
        let head = format!("{:?}", response.headers());
        let bytes = response.into_body().collect().await.expect("body").to_bytes();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        Res { status, body, head }
    }

    /// Write a peer's record into the node registry directly.
    ///
    /// Standing in for the replication that would carry it in a real cluster:
    /// the registry is an ordinary collection, so a replicated record and a
    /// locally written one are the same thing by the time topology reads it.
    async fn register_peer(&self, node: &kimmy_core::NodeId, endpoint: &str) {
        let meta = self
            .state
            .engine
            .create_system_collection(
                kimmy_api::topology::NODES_DB,
                kimmy_api::topology::NODES_COLLECTION,
            )
            .unwrap();
        self.state
            .engine
            .insert(
                &meta,
                bson::doc! {
                    "_id": node.to_string(),
                    "endpoint": endpoint,
                    "version": "0.0.1-peer",
                    "updatedMs": 0i64,
                },
            )
            .unwrap();
    }
}

#[tokio::test]
async fn health_endpoints_need_no_credentials() {
    // A load balancer probing these should not need to hold a token.
    let server = Server::start().await;
    assert_eq!(server.get("/healthz", None).await.status, 200);
    assert_eq!(server.get("/readyz", None).await.status, 200);
}

#[tokio::test]
async fn data_endpoints_require_a_token() {
    let server = Server::start().await;
    assert_eq!(server.get("/v1/databases", None).await.status, 401);
    assert_eq!(server.get("/v1/databases", Some("garbage")).await.status, 401);
}

#[tokio::test]
async fn a_wrong_password_does_not_reveal_whether_the_user_exists() {
    let server = Server::start().await;
    let wrong = server.post("/v1/auth/login", None, json!({"user":"root","password":"nope"})).await;
    let missing =
        server.post("/v1/auth/login", None, json!({"user":"ghost","password":"nope"})).await;

    assert_eq!(wrong.status, 401);
    assert_eq!(missing.status, 401);
    assert_eq!(wrong.body, missing.body, "the responses must be indistinguishable");
}

#[tokio::test]
async fn repeated_failed_logins_are_rate_limited() {
    // Without this, a password is guessable at network speed, and every guess
    // costs the server a full Argon2id verification whether or not the user
    // exists.
    let server = Server::start_rate_limited(3).await;
    let bad = json!({"user":"root","password":"wrong"});

    for attempt in 1..=3 {
        let res = server.post("/v1/auth/login", None, bad.clone()).await;
        assert_eq!(res.status, 401, "attempt {attempt} is within the burst and should reach auth");
    }

    let res = server.post("/v1/auth/login", None, bad).await;
    assert_eq!(res.status, 429, "the fourth attempt is past a burst of 3: {:?}", res.body);
    assert_eq!(res.body["error"], "rate_limited");
    // A refusal that does not say when to come back leaves a client guessing.
    let retry = res.header("retry-after").expect("a 429 must carry Retry-After");
    assert!(retry.parse::<u64>().is_ok_and(|s| s > 0), "Retry-After should be seconds: {retry}");
}

#[tokio::test]
async fn a_successful_login_does_not_spend_the_budget() {
    // Only failures are recorded. A fleet re-authenticating on a short token
    // TTL is not the thing being defended against, and throttling it would turn
    // a security control into an outage.
    let server = Server::start_rate_limited(2).await;

    for attempt in 1..=10 {
        let res = server
            .post("/v1/auth/login", None, json!({"user":"root","password":ROOT_PASSWORD}))
            .await;
        assert_eq!(
            res.status, 200,
            "correct credentials must never be limited (attempt {attempt})"
        );
    }
}

#[tokio::test]
async fn the_limit_does_not_leak_whether_a_user_exists() {
    // A 429 for a real name and a 401 for an invented one would turn the
    // limiter into the user-enumeration oracle that login itself avoids being.
    let server = Server::start_rate_limited(1).await;

    server.post("/v1/auth/login", None, json!({"user":"root","password":"wrong"})).await;

    let real = server.post("/v1/auth/login", None, json!({"user":"root","password":"wrong"})).await;
    let fake =
        server.post("/v1/auth/login", None, json!({"user":"ghost","password":"wrong"})).await;

    assert_eq!(real.status, 429);
    assert_eq!(fake.status, 429, "the address is over its budget regardless of the name tried");
    assert_eq!(real.body, fake.body, "the responses must be indistinguishable");
}

#[tokio::test]
async fn limiting_by_username_is_off_unless_configured() {
    // It is a real defence against a distributed guess, and a real lockout: it
    // lets anyone keep a named user out for a window. That trade is an
    // operator's to make, so the default must be off — assert the default
    // rather than trusting it.
    let limits = kimmy_api::RateLimits::disabled();
    assert!(limits.login_user.limit().is_disabled());

    let server = Server::start_user_rate_limited(2).await;
    let bad = json!({"user":"root","password":"wrong"});
    for _ in 0..2 {
        assert_eq!(server.post("/v1/auth/login", None, bad.clone()).await.status, 401);
    }
    assert_eq!(
        server.post("/v1/auth/login", None, bad).await.status,
        429,
        "when it is switched on it must actually limit"
    );
}

// ---------------------------------------------------------------------------
// Authenticated-route limits (ADR-099)
// ---------------------------------------------------------------------------

/// Open a request and send only part of its declared body, then wait for
/// whatever the server says about it.
///
/// This is the shape of a client that holds a connection open at no cost to
/// itself — the case the request deadline exists for — and it is also the one
/// place a request to this server is genuinely *pending*, which is the only
/// state a deadline can interrupt. Bounded by its own five-second wait, so a
/// deadline that never fires is a failed test rather than a hung one.
async fn dribble(base: &str, path: &str, token: &str) -> (u16, Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let host = base.strip_prefix("http://").expect("http url");
    let mut stream = tokio::net::TcpStream::connect(host).await.expect("connect");
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: 64\r\n\
         Authorization: Bearer {token}\r\n\r\n{{\"pad\":\""
    );
    stream.write_all(head.as_bytes()).await.expect("write");
    // The rest of the 64 bytes never comes.

    let mut raw = Vec::new();
    let read = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let _ = stream.read_to_end(&mut raw).await;
    })
    .await;
    assert!(read.is_ok(), "no response within five seconds: the deadline did not fire");

    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, serde_json::from_str(body.trim()).unwrap_or(Value::Null))
}

#[tokio::test]
async fn a_request_still_waiting_for_its_body_at_the_deadline_is_abandoned() {
    // The route is real and authenticated: the token is checked from the
    // headers before the body is awaited, so what the deadline interrupts is
    // exactly the wait an authenticated caller can impose for free.
    let server = Server::start_with_limits(kimmy_api::RequestLimits {
        request_timeout: std::time::Duration::from_millis(300),
        ..kimmy_api::RequestLimits::default()
    })
    .await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let (status, body) = dribble(&server.base, "/v1/db/shop/coll/c/docs", &token).await;
    assert_eq!(status, 503, "a request past its deadline is refused: {body}");
    assert_eq!(body["error"], "timeout");
    assert_eq!(body["retry"], "wait");

    // The node is fine; only that request was abandoned.
    let res = server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id": 1})).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
}

/// A real WebSocket handshake that keeps the socket, so a test can watch what
/// arrives on it later.
async fn open_watch(base: &str, path: &str, token: &str) -> tokio::net::TcpStream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let host = base.strip_prefix("http://").expect("http url");
    let mut stream = tokio::net::TcpStream::connect(host).await.expect("connect");
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n\
         Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Authorization: Bearer {token}\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.expect("write");

    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte).await {
            Ok(0) | Err(_) => break,
            Ok(_) => head.push(byte[0]),
        }
    }
    let head = String::from_utf8_lossy(&head).into_owned();
    assert!(head.starts_with("HTTP/1.1 101"), "the watch route must upgrade: {head}");
    stream
}

/// Read one unmasked text frame, as a server sends them.
async fn read_text_frame(stream: &mut tokio::net::TcpStream) -> String {
    use tokio::io::AsyncReadExt;

    let mut header = [0u8; 2];
    stream.read_exact(&mut header).await.expect("frame header");
    assert_eq!(header[0] & 0x0f, 1, "expected a text frame, got opcode {}", header[0] & 0x0f);
    let mut len = u64::from(header[1] & 0x7f);
    if len == 126 {
        let mut ext = [0u8; 2];
        stream.read_exact(&mut ext).await.expect("extended length");
        len = u64::from(u16::from_be_bytes(ext));
    } else if len == 127 {
        let mut ext = [0u8; 8];
        stream.read_exact(&mut ext).await.expect("extended length");
        len = u64::from_be_bytes(ext);
    }
    let mut payload = vec![0u8; len as usize];
    stream.read_exact(&mut payload).await.expect("payload");
    String::from_utf8_lossy(&payload).into_owned()
}

#[tokio::test]
async fn the_deadline_does_not_apply_to_a_change_stream() {
    // A change stream's response is a connection, not a document, and it is
    // meant to outlive any deadline a document request could sensibly have.
    // Opened, left idle for longer than the deadline, and then shown to still
    // deliver.
    let server = Server::start_with_limits(kimmy_api::RequestLimits {
        request_timeout: std::time::Duration::from_millis(200),
        ..kimmy_api::RequestLimits::default()
    })
    .await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;

    let mut socket = open_watch(&server.base, "/v1/db/shop/coll/orders/watch", &token).await;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    let res = server.post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id": 7})).await;
    assert_eq!(res.status, 200, "{:?}", res.body);

    let frame =
        tokio::time::timeout(std::time::Duration::from_secs(5), read_text_frame(&mut socket))
            .await
            .expect("an event should arrive on a stream older than the deadline");
    let event: Value = serde_json::from_str(&frame).expect("a JSON event");
    assert_eq!(event["operationType"], "insert", "{event}");
    assert_eq!(event["documentKey"]["_id"], 7, "{event}");
}

#[tokio::test]
async fn a_body_at_the_ceiling_is_accepted_and_one_byte_over_is_refused() {
    // `{"pad":"…"}` is ten bytes of punctuation around the padding, so the
    // padding is what sets the body to exactly the ceiling.
    let server = Server::start_with_limits(kimmy_api::RequestLimits {
        max_body_bytes: 1024,
        ..kimmy_api::RequestLimits::default()
    })
    .await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let at_limit = json!({ "pad": "x".repeat(1014) });
    assert_eq!(at_limit.to_string().len(), 1024);
    let res = server.post("/v1/db/shop/coll/c/docs", Some(&token), at_limit).await;
    assert_eq!(res.status, 200, "exactly the ceiling must be accepted: {:?}", res.body);

    let over = json!({ "pad": "x".repeat(1015) });
    assert_eq!(over.to_string().len(), 1025);
    let res = server.post("/v1/db/shop/coll/c/docs", Some(&token), over).await;
    assert_eq!(res.status, 413, "one byte over must be refused: {:?}", res.body);
    assert_eq!(res.body["error"], "payload_too_large");
    assert_eq!(res.body["retry"], "no");
}

#[tokio::test]
async fn a_principal_over_its_budget_is_refused_while_another_on_the_same_address_is_not() {
    // Both callers arrive from loopback. Keying on the address would refuse
    // them together; keying on the principal refuses only the one that spent
    // its budget — which is the whole reason the second limiter exists.
    let server = Server::start_principal_rate_limited(3).await;
    let root = server.root().await;

    // Login is unauthenticated and spends nothing; creating the second user
    // is root's first request.
    let created = server
        .post("/v1/users", Some(&root), json!({"user":"other","password":"other-password"}))
        .await;
    assert!(created.status / 100 == 2, "{} {:?}", created.status, created.body);
    for attempt in 2..=3 {
        let res = server.get("/v1/databases", Some(&root)).await;
        assert_eq!(res.status, 200, "request {attempt} of 3 is within the burst: {:?}", res.body);
    }

    let refused = server.get("/v1/databases", Some(&root)).await;
    assert_eq!(refused.status, 429, "the fourth request is past a burst of 3: {:?}", refused.body);
    assert_eq!(refused.body["error"], "rate_limited");
    assert_eq!(refused.body["retry"], "wait");
    let retry = refused.header("retry-after").expect("a 429 must carry Retry-After");
    assert!(retry.parse::<u64>().is_ok_and(|s| s > 0), "Retry-After should be seconds: {retry}");

    let other = server.login("other", "other-password").await;
    let res = server.get("/v1/databases", Some(&other)).await;
    assert_eq!(res.status, 200, "another principal on the same address is unaffected");

    // Still refused: the other principal's request did not refill root.
    assert_eq!(server.get("/v1/databases", Some(&root)).await.status, 429);

    // Counted under its own series, and inside the total.
    let metrics = server.state.metrics.render();
    assert!(metrics.contains("kimmy_rate_limited_principal_total 2"), "{metrics}");
    assert!(metrics.contains("kimmy_rate_limited_total 2"), "{metrics}");
}

#[tokio::test]
async fn a_bad_token_is_refused_before_it_can_spend_a_budget() {
    // The limiter counts principals, not guesses: a token that fails
    // verification is a 401 and touches no bucket, so it cannot be used to
    // exhaust a real user's budget from outside.
    let server = Server::start_principal_rate_limited(1).await;
    let root = server.root().await;

    for _ in 0..3 {
        assert_eq!(server.get("/v1/databases", Some("not-a-token")).await.status, 401);
    }
    assert_eq!(
        server.get("/v1/databases", Some(&root)).await.status,
        200,
        "root's single token must still be there after three refused guesses"
    );
    assert!(server.state.metrics.render().contains("kimmy_rate_limited_principal_total 0"));
}

#[tokio::test]
async fn a_federated_subject_named_like_a_local_user_has_its_own_budget() {
    // Whoever controls the name `root` at the identity provider must not be
    // able to spend the local root's budget, nor be charged for it.
    let server =
        Server::start_federated_with(oidc::AUDIENCE, None, Server::principal_limits(1)).await;
    let local = server.root().await;
    let federated = oidc::token(oidc::claims("root", json!(["kimmydb-analyst"])));

    assert_eq!(server.get("/v1/databases", Some(&local)).await.status, 200);
    assert_eq!(server.get("/v1/databases", Some(&local)).await.status, 429, "local root is spent");

    assert_eq!(
        server.get("/v1/databases", Some(&federated)).await.status,
        200,
        "the provider's `root` has a budget of its own"
    );
    assert_eq!(server.get("/v1/databases", Some(&federated)).await.status, 429);
}

#[tokio::test]
async fn documents_round_trip_through_the_api() {
    let server = Server::start().await;
    let token = server.root().await;

    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;

    let res = server
        .post(
            "/v1/db/shop/coll/orders/docs",
            Some(&token),
            json!({"_id":1,"item":"widget","qty":5}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);

    let res = server.get("/v1/db/shop/coll/orders/docs/1", Some(&token)).await;
    assert_eq!(res.body["item"], "widget");
    assert_eq!(res.body["qty"], 5);
}

#[tokio::test]
async fn a_database_is_listed_while_it_has_collections_and_can_be_dropped_whole() {
    // Creation is implicit in the first collection; removal is implicit in
    // the last, so an emptied database does not linger in listings — and
    // DELETE /v1/db/{db} drops everything in one call.
    let server = Server::start().await;
    let token = server.root().await;

    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"items"})).await;
    let res = server.get("/v1/databases", Some(&token)).await;
    assert!(res.body["databases"].as_array().unwrap().contains(&json!("shop")));

    let res = server.delete("/v1/db/shop/coll/orders", Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let res = server.get("/v1/databases", Some(&token)).await;
    assert!(res.body["databases"].as_array().unwrap().contains(&json!("shop")), "one left");

    let res = server.delete("/v1/db/shop", Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["dropped"], true);
    let res = server.get("/v1/databases", Some(&token)).await;
    assert!(!res.body["databases"].as_array().unwrap().contains(&json!("shop")));
    let res = server.get("/v1/db/shop/collections", Some(&token)).await;
    assert_eq!(res.status, 404, "listing a missing database is a 404: {:?}", res.body);

    let res = server.delete("/v1/db/__kimmy", Some(&token)).await;
    assert_eq!(res.status, 400, "system databases are refused: {:?}", res.body);
}

#[tokio::test]
async fn a_missing_collection_says_elsewhere_only_when_the_node_has_peers() {
    // Through a round-robin front a create lands on one member and the next
    // request on another; "no" told that client to give up. With peers the
    // hint is "elsewhere"; alone it stays "no" — there is nowhere else.
    let server = Server::start().await;
    let token = server.root().await;
    let res = server.get("/v1/db/shop/coll/orders/docs/1", Some(&token)).await;
    assert_eq!(res.status, 404, "{:?}", res.body);
    assert_eq!(res.body["retry"], "no");

    let members = kimmy_cluster::Members::default();
    members
        .insert_for_test("10.0.0.2:7900".parse().unwrap(), kimmy_core::NodeId::from_bytes([7; 16]));
    server.state.set_members(members);
    let res = server.get("/v1/db/shop/coll/orders/docs/1", Some(&token)).await;
    assert_eq!(res.status, 404, "{:?}", res.body);
    assert_eq!(res.body["retry"], "elsewhere", "{:?}", res.body);
    assert!(res.body["message"].as_str().unwrap().contains("another member"));
}

#[tokio::test]
async fn replacing_by_id_reports_counts_and_needs_upsert_to_create() {
    // Two things nothing covered until the protocol was specified, both
    // client-visible:
    //
    // **Without `upsert` a missing document is not an error.** The answer is
    // `200 {"matched": 0}` and nothing is written — which is how a cluster
    // drive once built a whole conflict test on this route, wrote nothing, and
    // passed because five nodes agreed on the same non-answer.
    //
    // **`matched` and `modified` are counts**, as on `/update` and
    // `/find_and_modify`. This route used to serialize `WriteOutcome`'s bools
    // straight to the wire, so one protocol carried two types under one field
    // name (ADR-056).
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;

    let missing = server
        .put("/v1/db/shop/coll/orders/docs/ghost", Some(&token), json!({ "item": "widget" }))
        .await;
    assert_eq!(missing.status, 200, "{:?}", missing.body);
    assert_eq!(missing.body["matched"], 0, "a miss is a count, not `false`");
    assert_eq!(missing.body["modified"], 0);
    assert_eq!(missing.body["upserted"], false);
    assert_eq!(
        server.get("/v1/db/shop/coll/orders/docs/ghost", Some(&token)).await.status,
        404,
        "nothing may be written without ?upsert=true"
    );

    let created = server
        .put(
            "/v1/db/shop/coll/orders/docs/ghost?upsert=true",
            Some(&token),
            json!({ "item": "widget" }),
        )
        .await;
    assert_eq!(created.body["matched"], 0, "an upsert did not match, it created");
    assert_eq!(created.body["upserted"], true);

    let replaced = server
        .put("/v1/db/shop/coll/orders/docs/ghost", Some(&token), json!({ "item": "sprocket" }))
        .await;
    assert_eq!(replaced.body["matched"], 1);
    assert_eq!(replaced.body["modified"], 1);
    assert_eq!(replaced.body["upserted"], false);
    assert_eq!(
        server.get("/v1/db/shop/coll/orders/docs/ghost", Some(&token)).await.body["item"],
        "sprocket"
    );
}

#[tokio::test]
async fn a_duplicate_id_is_a_conflict() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let doc = json!({ "_id": 1 });
    assert_eq!(server.post("/v1/db/shop/coll/c/docs", Some(&token), doc.clone()).await.status, 200);
    let res = server.post("/v1/db/shop/coll/c/docs", Some(&token), doc).await;
    assert_eq!(res.status, 409);
    assert_eq!(res.body["error"], "duplicate_key");
}

#[tokio::test]
async fn a_bulk_insert_returns_an_id_for_every_document() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let res = server
        .post(
            "/v1/db/shop/coll/c/bulk",
            Some(&token),
            json!([{"_id":1,"item":"a"}, {"_id":2,"item":"b"}, {"item":"c"}]),
        )
        .await;

    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["inserted"], 3);
    assert_eq!(res.body["insertedIds"].as_array().unwrap().len(), 3);

    // The generated id is returned, so the caller need not re-read to find it.
    assert_eq!(server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await.body["item"], "a");
    let res = server.post("/v1/db/shop/coll/c/count", Some(&token), json!({})).await;
    assert_eq!(res.body["count"], 3);
}

#[tokio::test]
async fn a_bulk_insert_with_a_duplicate_id_inserts_nothing() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let res = server
        .post("/v1/db/shop/coll/c/bulk", Some(&token), json!([{"_id":1}, {"_id":2}, {"_id":1}]))
        .await;

    assert_eq!(res.status, 409, "{:?}", res.body);
    assert_eq!(res.body["error"], "duplicate_key");
    assert!(
        res.body["message"].as_str().unwrap().contains("index 2"),
        "the caller must be told which document failed: {:?}",
        res.body["message"]
    );

    // All-or-nothing: the documents before the failure must not have landed.
    let res = server.post("/v1/db/shop/coll/c/count", Some(&token), json!({})).await;
    assert_eq!(res.body["count"], 0, "a failed batch must leave the collection empty");
}

#[tokio::test]
async fn a_bulk_insert_over_the_cap_is_rejected() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let documents: Vec<_> = (0..1001).map(|i| json!({ "_id": i })).collect();
    let res = server.post("/v1/db/shop/coll/c/bulk", Some(&token), json!(documents)).await;

    assert_eq!(res.status, 400, "{:?}", res.body);
    assert_eq!(res.body["error"], "bad_request");

    let res = server.post("/v1/db/shop/coll/c/count", Some(&token), json!({})).await;
    assert_eq!(res.body["count"], 0, "a rejected batch must not partially apply");
}

#[tokio::test]
async fn a_bulk_insert_of_exactly_the_cap_is_accepted() {
    // The boundary, not just the far side of it. `>` and `>=` differ by one
    // document here, and a test that only sends 1001 cannot tell them apart —
    // which is how a cap that silently rejected a legal batch would ship.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let documents: Vec<_> = (0..1000).map(|i| json!({ "_id": i })).collect();
    let res = server.post("/v1/db/shop/coll/c/bulk", Some(&token), json!(documents)).await;

    assert_eq!(res.status, 200, "exactly the cap must be accepted: {:?}", res.body);
    assert_eq!(res.body["inserted"], 1000);
}

#[tokio::test]
async fn a_bulk_body_over_the_size_limit_is_413_with_a_stable_code() {
    // Distinct from the document cap: a batch well under 1000 documents can
    // still be too large, and axum's own rejection carries no `error` code
    // for a client to branch on.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let padding = "x".repeat(4000);
    let documents: Vec<_> = (0..600).map(|i| json!({ "_id": i, "pad": padding })).collect();
    let res = server.post("/v1/db/shop/coll/c/bulk", Some(&token), json!(documents)).await;

    assert_eq!(res.status, 413, "a body over 2 MB must be refused: {:?}", res.body);
    assert_eq!(res.body["error"], "payload_too_large");
}

/// Send `body` in one write and read whatever comes back, the two concurrent:
/// a server that stopped reading would stall the write once the socket
/// buffers filled, and a test that wrote first would hang there rather than
/// fail. Returns both outcomes, since which one a refusal shows up in is the
/// subject.
async fn upload(
    base: &str,
    path: &str,
    token: &str,
    body: Vec<u8>,
) -> (std::io::Result<()>, std::io::Result<Vec<u8>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let host = base.strip_prefix("http://").expect("http url");
    let stream = tokio::net::TcpStream::connect(host).await.expect("connect");
    let (mut reader, mut writer) = stream.into_split();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\
         Authorization: Bearer {token}\r\n\r\n",
        body.len()
    );
    let written = tokio::spawn(async move {
        let mut request = head.into_bytes();
        request.extend_from_slice(&body);
        // The half comes back with the result rather than being dropped
        // here: dropping it shuts the write side down, and hyper takes a FIN
        // that arrives before the response is written as the client having
        // left, and answers nothing. A real client keeps its socket open
        // until it has read the response, so this one does too.
        (writer.write_all(&request).await, writer)
    });
    let mut raw = Vec::new();
    let read = reader.read_to_end(&mut raw).await.map(|_| raw);
    let (written, _writer) = written.await.expect("the writer task");
    (written, read)
}

/// The status line and JSON body of a raw response.
fn status_and_body(raw: &[u8]) -> (u16, Value) {
    let text = String::from_utf8_lossy(raw);
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, serde_json::from_str(body.trim()).unwrap_or(Value::Null))
}

#[tokio::test]
async fn a_client_still_sending_past_the_ceiling_is_answered_with_the_413() {
    // A body a thousand times the ceiling, inside the drain cap. The server
    // refuses it on the second kilobyte; what the test holds is that it keeps
    // reading anyway, so the write completes, the response arrives, and the
    // connection ends cleanly — no reset to tolerate on either side.
    let server = Server::start_with_limits(kimmy_api::RequestLimits {
        max_body_bytes: 1024,
        ..kimmy_api::RequestLimits::default()
    })
    .await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let body = json!({ "pad": "x".repeat(1 << 20) }).to_string().into_bytes();
    let (written, read) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        upload(&server.base, "/v1/db/shop/coll/c/docs", &token, body),
    )
    .await
    .expect("the exchange completes: a draining server neither stalls nor is stalled");

    written.expect("the whole body is taken: the server reads on past the ceiling");
    let raw = read.expect("the response is read to a clean end of stream");
    let (status, body) = status_and_body(&raw);
    assert_eq!(status, 413, "{} bytes read: {:?}", raw.len(), String::from_utf8_lossy(&raw));
    assert_eq!(body["error"], "payload_too_large");
    assert_eq!(body["retry"], "no");
}

#[tokio::test]
async fn a_body_past_the_drain_cap_is_answered_or_closed_on_within_the_bound() {
    // Past the cap by a megabyte. The drain stops at the cap and the refusal
    // goes out over a body still arriving, so either the 413 is read or the
    // connection is reset — what is not allowed is a hang on either side,
    // which is what an unbounded drain, or a client waiting on one, would be.
    let server = Server::start_with_limits(kimmy_api::RequestLimits {
        max_body_bytes: 1024,
        ..kimmy_api::RequestLimits::default()
    })
    .await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let padding = kimmy_api::limits::MAX_BODY_DRAIN_BYTES + (1 << 20);
    let body = json!({ "pad": "x".repeat(padding) }).to_string().into_bytes();
    let (_written, read) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        upload(&server.base, "/v1/db/shop/coll/c/docs", &token, body),
    )
    .await
    .expect("the exchange completes within the drain's own bound");

    if let Ok(raw) = &read
        && !raw.is_empty()
    {
        let (status, body) = status_and_body(raw);
        assert_eq!(status, 413, "{body}");
        assert_eq!(body["error"], "payload_too_large");
    }

    // The node is fine; only that request was closed on.
    let res = server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id": 1})).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
}

#[tokio::test]
async fn a_bulk_insert_of_an_empty_array_is_a_no_op() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let res = server.post("/v1/db/shop/coll/c/bulk", Some(&token), json!([])).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["inserted"], 0);
}

#[tokio::test]
async fn a_bulk_insert_that_is_not_an_array_reports_a_stable_error_code() {
    // Axum's own rejection is bare text; without a mapping this would be the
    // one route a client cannot branch on.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let res = server.post("/v1/db/shop/coll/c/bulk", Some(&token), json!({"_id":1})).await;
    assert!((400..500).contains(&res.status), "an object is not a batch: {}", res.status);
    assert!(res.body.get("error").is_some(), "every failure carries an error code: {:?}", res.body);
}

/// Every route that takes a request shape refuses a field it does not
/// define, and says which one.
///
/// ADR-121. Before it, `{"limitt": 5}` on `find` answered `200` and ignored
/// the field, while the same typo in a vector configuration was a `422`: one
/// struct carried `deny_unknown_fields` and the rest did not. Each case below
/// sends a valid body with one extra top-level field and expects the refusal;
/// then, where the route is cheap to set up for, sends the same body without
/// it and expects success — so what is being refused is shown to be the
/// field, not the body around it.
#[tokio::test]
async fn a_request_field_the_route_does_not_define_is_refused_by_name() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server.post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id": 1, "qty": 5})).await;
    let res = server
        .post(
            "/v1/db/shop/coll/orders/vector",
            Some(&token),
            json!({ "fields": ["note"], "provider": { "kind": "byo" }, "dim": 3 }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let res = server
        .put(
            "/v1/db/shop/coll/orders/docs/1/vectors",
            Some(&token),
            json!([{ "chunk": 0, "vector": [1.0, 0.0, 0.0], "text": "a small blue widget" }]),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);

    // (method, path, needs a token, valid body, also run without the extra
    // field). Webhook registration is refused before its URL is checked, and
    // a URL that passes the egress policy needs a resolvable public host,
    // which a unit test does not get to assume.
    let cases: Vec<(&str, &str, bool, Value, bool)> = vec![
        ("POST", "/v1/auth/login", false, json!({"user": "root", "password": ROOT_PASSWORD}), true),
        ("POST", "/v1/db/shop/collections", true, json!({"name": "other"}), true),
        (
            "POST",
            "/v1/db/shop/coll/orders/find",
            true,
            json!({"filter": {"qty": 5}, "limit": 1}),
            true,
        ),
        ("POST", "/v1/db/shop/coll/orders/count", true, json!({"filter": {}}), true),
        (
            "POST",
            "/v1/db/shop/coll/orders/aggregate",
            true,
            json!({"pipeline": [{"$match": {"qty": 5}}]}),
            true,
        ),
        (
            "POST",
            "/v1/db/shop/coll/orders/update",
            true,
            json!({"filter": {"_id": 1}, "update": {"$set": {"qty": 6}}}),
            true,
        ),
        (
            "POST",
            "/v1/db/shop/coll/orders/find_and_modify",
            true,
            json!({"filter": {"_id": 1}, "update": {"$set": {"qty": 7}}, "returnDocument": "after"}),
            true,
        ),
        ("POST", "/v1/db/shop/coll/orders/delete", true, json!({"filter": {"_id": 999}}), true),
        (
            "POST",
            "/v1/db/shop/coll/orders/indexes",
            true,
            json!({"fields": [{"path": "qty"}]}),
            true,
        ),
        (
            "POST",
            "/v1/db/shop/coll/orders/vector_search",
            true,
            json!({"vector": [1.0, 0.0, 0.0], "k": 1}),
            true,
        ),
        (
            "POST",
            "/v1/db/shop/coll/orders/hybrid_search",
            true,
            json!({"query": "blue widget", "vector": [1.0, 0.0, 0.0], "weights": {"dense": 1.0}}),
            true,
        ),
        (
            "PUT",
            "/v1/db/shop/coll/orders/docs/1/vectors",
            true,
            json!([{ "chunk": 0, "vector": [1.0, 0.0, 0.0], "text": "a small blue widget" }]),
            true,
        ),
        ("POST", "/v1/users", true, json!({"user": "alice", "password": "alice-password"}), true),
        ("POST", "/v1/users/alice/password", true, json!({"password": "another-password"}), true),
        ("POST", "/v1/users/alice/grants", true, json!({"grants": []}), true),
        ("POST", "/v1/users/alice/disabled", true, json!({"disabled": false}), true),
        ("POST", "/v1/users/alice/roles", true, json!({"roles": []}), true),
        ("POST", "/v1/roles", true, json!({"name": "analyst"}), true),
        ("POST", "/v1/roles/analyst/grants", true, json!({"grants": []}), true),
        (
            "POST",
            "/v1/db/shop/coll/orders/webhooks",
            true,
            json!({"url": "https://hooks.example/orders"}),
            false,
        ),
    ];

    for (method, path, needs_token, body, run_valid) in cases {
        let auth = needs_token.then_some(token.as_str());
        let url = format!("{}{path}", server.base);

        // An array body carries the extra field inside its first element,
        // since the elements are the shapes.
        let mut with_extra = body.clone();
        match &mut with_extra {
            Value::Array(items) => {
                items[0]["zzz"] = json!(1);
            }
            other => {
                other["zzz"] = json!(1);
            }
        }
        let res = server.client.request(method, &url, auth, Some(with_extra)).await;
        assert_eq!(res.status, 422, "{method} {path} accepted an unknown field: {:?}", res.body);
        assert_eq!(res.body["error"], "bad_request", "{method} {path}: {:?}", res.body);
        assert_eq!(res.body["retry"], "no", "{method} {path}: {:?}", res.body);
        let message = res.body["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("unknown field `zzz`"),
            "{method} {path} must name the field it refused: {message}"
        );

        if run_valid {
            let res = server.client.request(method, &url, auth, Some(body)).await;
            assert!(
                (200..300).contains(&res.status),
                "{method} {path} without the extra field should succeed: {} {:?}",
                res.status,
                res.body
            );
        }
    }

    // A nested request shape is closed too: the field list of an index, the
    // weights of a hybrid search.
    let res = server
        .post(
            "/v1/db/shop/coll/orders/indexes",
            Some(&token),
            json!({"fields": [{"path": "qty", "descnding": true}]}),
        )
        .await;
    assert_eq!(res.status, 422, "{:?}", res.body);
    assert!(res.body["message"].as_str().unwrap_or_default().contains("`descnding`"));
    let res = server
        .post(
            "/v1/db/shop/coll/orders/hybrid_search",
            Some(&token),
            json!({"query": "blue", "vector": [1.0, 0.0, 0.0], "weights": {"dens": 2.0}}),
        )
        .await;
    assert_eq!(res.status, 422, "{:?}", res.body);
    assert!(res.body["message"].as_str().unwrap_or_default().contains("`dens`"));
}

/// A grant inside a user or role body is closed too, and this is the case
/// the rule exists for: `collection` defaults to `*`, so a misspelt
/// `colection` did not drop a field — it widened the grant to every
/// collection in the database, and the response showed a grant that looked
/// deliberate. The persisted `Grant` stays open (it must keep reading under a
/// later version); the request carries a closed mirror of it.
#[tokio::test]
async fn a_misspelt_grant_field_is_refused_rather_than_widening_the_grant() {
    let server = Server::start().await;
    let token = server.root().await;

    let typo = json!({ "db": "shop", "colection": "orders", "actions": ["read"] });
    let good = json!({ "db": "shop", "collection": "orders", "actions": ["read"] });

    let cases: Vec<(&str, Value, Value)> = vec![
        (
            "/v1/users",
            json!({ "user": "bob", "password": "bob-password", "grants": [typo] }),
            json!({ "user": "bob", "password": "bob-password", "grants": [good] }),
        ),
        ("/v1/users/bob/grants", json!({ "grants": [typo] }), json!({ "grants": [good] })),
        (
            "/v1/roles",
            json!({ "name": "clerk", "grants": [typo] }),
            json!({ "name": "clerk", "grants": [good] }),
        ),
        ("/v1/roles/clerk/grants", json!({ "grants": [typo] }), json!({ "grants": [good] })),
    ];
    for (path, with_typo, well_formed) in cases {
        let res = server.post(path, Some(&token), with_typo).await;
        assert_eq!(res.status, 422, "{path} accepted a misspelt grant field: {:?}", res.body);
        assert_eq!(res.body["error"], "bad_request", "{path}: {:?}", res.body);
        let message = res.body["message"].as_str().unwrap_or_default();
        assert!(message.contains("unknown field `colection`"), "{path}: {message}");

        let res = server.post(path, Some(&token), well_formed).await;
        assert!(
            (200..300).contains(&res.status),
            "{path} well-formed: {} {:?}",
            res.status,
            res.body
        );
    }

    // And what was stored is the grant that was sent, not a widened one.
    let res = server.get("/v1/users/bob", Some(&token)).await;
    assert_eq!(res.body["grants"][0]["collection"], "orders", "{:?}", res.body);
}

/// A query string is held to the same rule, at `400` rather than `422`: it is
/// part of the request line, not a body the server could not process, and
/// `400` is what axum answered before the envelope was added. Before this,
/// `?limt=5` returned the default page and `?limit=abc` was bare text with no
/// `error` code — the one refusal on a `GET` a client could not branch on.
#[tokio::test]
async fn a_query_parameter_the_route_does_not_define_is_refused_by_name() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    for id in 1..=3 {
        server.post("/v1/db/shop/coll/orders/docs", Some(&token), json!({ "_id": id })).await;
    }

    let res = server.get("/v1/db/shop/coll/orders/docs?limt=5", Some(&token)).await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert_eq!(res.body["error"], "bad_request", "{:?}", res.body);
    assert_eq!(res.body["retry"], "no", "{:?}", res.body);
    let message = res.body["message"].as_str().unwrap_or_default();
    assert!(message.contains("`limt`"), "must name the parameter: {message}");

    let res = server.get("/v1/db/shop/coll/orders/docs?limit=abc", Some(&token)).await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert_eq!(
        res.body["error"], "bad_request",
        "a bad value is still in the envelope: {:?}",
        res.body
    );

    let res = server.get("/v1/db/shop/coll/orders/docs?limit=1", Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["count"], 1);

    // The other query-string routes carry the same extractor.
    for (method, path) in [
        ("GET", "/v1/db/shop/coll/orders/describe?sampel=3"),
        ("GET", "/v1/db/shop/coll/orders/violations?indx=x"),
        ("PUT", "/v1/db/shop/coll/orders/docs/1?upsrt=true"),
        ("DELETE", "/v1/db/shop/coll/orders/docs/1?ifstamp=x"),
        ("DELETE", "/v1/db/shop/coll/orders/vector?drop=true"),
    ] {
        let url = format!("{}{path}", server.base);
        let body = (method == "PUT").then(|| json!({ "_id": 1 }));
        let res = server.client.request(method, &url, Some(&token), body).await;
        assert_eq!(res.status, 400, "{method} {path}: {:?}", res.body);
        assert_eq!(res.body["error"], "bad_request", "{method} {path}: {:?}", res.body);
    }
}

/// A query string on a route that reads none is refused, not ignored
/// (ADR-124).
///
/// ADR-121 closed query strings through `QueryParams<T>`, and a handler
/// without one never looked at its query string at all: `?if_stamp=<stale>`
/// on `update` — a body field there — answered `200` and rewrote the
/// document, which is the opposite of the condition the caller wrote. Found
/// by a test round against a three-member cluster running 0.20.0.
#[tokio::test]
async fn a_query_string_on_a_route_that_takes_none_is_refused_rather_than_ignored() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    let inserted =
        server.post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id":1,"n":0})).await;
    let first = stamp_of(&inserted.body);
    // Move the document on, so `first` is stale.
    server.put("/v1/db/shop/coll/orders/docs/1", Some(&token), json!({"n": 1})).await;
    let commits_before = server.state.engine.commits();

    // The case that mattered: a stale stamp on a route that takes it in the
    // body, where it used to be read as nothing and the write went through.
    let res = server
        .post(
            &format!("/v1/db/shop/coll/orders/update?if_stamp={first}"),
            Some(&token),
            json!({ "filter": {"_id": 1}, "update": {"$inc": {"n": 10}} }),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert_eq!(res.body["error"], "bad_request", "{:?}", res.body);
    assert_eq!(res.body["retry"], "no", "{:?}", res.body);
    let message = res.body["message"].as_str().unwrap_or_default();
    assert!(message.contains("`if_stamp`"), "must name the parameter: {message}");
    assert!(message.contains("takes none"), "and say the route takes none: {message}");
    let doc = server.get("/v1/db/shop/coll/orders/docs/1", Some(&token)).await;
    assert_eq!(doc.body["n"], 1, "the refused write must not have touched the document");
    assert_eq!(server.state.engine.commits(), commits_before, "a refused write commits nothing");

    // A parameter that means something in the body is not read from the
    // query string either; nor is one that means nothing anywhere.
    for (path, body) in [
        (
            "/v1/db/shop/coll/orders/update?multi=true",
            json!({ "filter": {}, "update": {"$set": {"n": 5}} }),
        ),
        ("/v1/db/shop/coll/orders/find?bogus=1", json!({ "filter": {} })),
        ("/v1/db/shop/coll/orders/count?bogus=1", json!({ "filter": {} })),
        ("/v1/db/shop/coll/orders/bulk?bogus=1", json!([{ "_id": 2 }])),
    ] {
        let res = server.post(path, Some(&token), body).await;
        assert_eq!(res.status, 400, "{path}: {:?}", res.body);
        assert_eq!(res.body["error"], "bad_request", "{path}: {:?}", res.body);
        let message = res.body["message"].as_str().unwrap_or_default();
        let name = path.split('?').nth(1).unwrap().split('=').next().unwrap();
        assert!(message.contains(&format!("`{name}`")), "{path}: {message}");
    }
    let doc = server.get("/v1/db/shop/coll/orders/docs/1", Some(&token)).await;
    assert_eq!(doc.body["n"], 1, "none of the refused writes landed");
    let res = server.post("/v1/db/shop/coll/orders/count", Some(&token), json!({})).await;
    assert_eq!(res.body["count"], 1, "the refused bulk insert inserted nothing");

    // A route that reads its query string still judges it for itself.
    let res = server.get("/v1/db/shop/coll/orders/docs?limt=5", Some(&token)).await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert!(res.body["message"].as_str().unwrap_or_default().contains("`limt`"), "{:?}", res.body);

    // A bare `?` names nothing and is not a query string: some clients
    // append one with nothing after it.
    let res = server.post("/v1/db/shop/coll/orders/find?", Some(&token), json!({})).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let res = server.get("/v1/db/shop/coll/orders/docs?", Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
}

/// The guard answers before authentication, and the placement is held.
///
/// It is a `Router::layer`; authentication is the `Auth` extractor a handler
/// takes. So a query string on a protected route is refused before any
/// token is looked at — `400`, not `401` — which is acceptable because
/// nothing is learned (the route templates are public in the specification
/// and the message echoes only the caller's own input) and nothing is
/// touched. It also means the refusal spends no per-principal budget and no
/// login-attempt budget, since neither is reached (ADR-124).
#[tokio::test]
async fn a_query_string_is_refused_before_the_token_is_examined() {
    let server = Server::start().await;
    for (method, path, body) in [
        ("POST", "/v1/users?zz=1", Some(json!({ "user": "x", "password": "x-password" }))),
        ("GET", "/v1/databases?zz=1", None),
        ("POST", "/v1/db/shop/coll/orders/find?zz=1", Some(json!({ "filter": {} }))),
    ] {
        let url = format!("{}{path}", server.base);
        let res = server.client.request(method, &url, None, body).await;
        assert_eq!(res.status, 400, "{method} {path} without a token: {:?}", res.body);
        assert_eq!(res.body["error"], "bad_request", "{method} {path}: {:?}", res.body);
        assert!(
            res.body["message"].as_str().unwrap_or_default().contains("`zz`"),
            "{:?}",
            res.body
        );
    }
    // And with no query string the same request is the 401 it always was.
    let res = server.get("/v1/databases", None).await;
    assert_eq!(res.status, 401, "{:?}", res.body);
}

/// Every documented operation refuses a query parameter it does not read.
///
/// Walks `docs/openapi.yaml` rather than a list kept here, so an operation
/// added to the specification is driven without anyone remembering to add
/// it. The routes that read their query string refuse `zz` through
/// `QueryParams<T>` (ADR-121), the rest through the guard over the table
/// (ADR-124); a client sees the same status, code and named parameter
/// either way. The path parameters are filled with things this test
/// created, so a `404` cannot stand in for the `400`.
#[tokio::test]
async fn every_documented_operation_refuses_a_query_parameter_it_does_not_read() {
    const SPEC: &str = include_str!("../../../docs/openapi.yaml");
    let spec: Value = serde_norway::from_str(SPEC).expect("docs/openapi.yaml is valid YAML");

    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({ "name": "orders" })).await;
    server
        .post("/v1/db/shop/coll/orders/docs", Some(&token), json!({ "_id": "a", "note": "x" }))
        .await;
    let index = server
        .post(
            "/v1/db/shop/coll/orders/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "note" }] }),
        )
        .await;
    let index_name = index.body["name"].as_str().expect("an index name").to_string();
    server
        .post(
            "/v1/users",
            Some(&token),
            json!({ "user": "clerk", "password": "clerk-password", "grants": [] }),
        )
        .await;
    server.post("/v1/roles", Some(&token), json!({ "name": "reader", "grants": [] })).await;
    let hook = server
        .post(
            "/v1/db/shop/coll/orders/webhooks",
            Some(&token),
            json!({ "url": "https://example.com/hook", "operations": ["insert"] }),
        )
        .await;
    let hook_id = hook.body["id"].as_str().expect("a subscription id").to_string();
    server
        .post(
            "/v1/db/shop/coll/orders/vector",
            Some(&token),
            json!({ "fields": ["note"], "provider": { "kind": "byo" }, "dim": 3 }),
        )
        .await;

    // The smallest body each bodied route accepts. Never read — the refusal
    // comes first — but sent so that the request is one the route would
    // otherwise answer, and the `400` is the query string's alone.
    let body_for = |method: &str, template: &str| -> Option<Value> {
        if !matches!(method, "POST" | "PUT") {
            return None;
        }
        Some(match template.rsplit('/').next().unwrap_or_default() {
            "login" => json!({ "user": "root", "password": ROOT_PASSWORD }),
            "refresh" => return None,
            "users" => json!({ "user": "temp", "password": "temp-password", "grants": [] }),
            "password" => json!({ "password": "another-password" }),
            "grants" => json!({ "grants": [] }),
            "disabled" => json!({ "disabled": false }),
            "roles" if template.starts_with("/v1/users") => json!({ "roles": [] }),
            "roles" => json!({ "name": "temp", "grants": [] }),
            "collections" => json!({ "name": "temp" }),
            "docs" => json!({ "_id": "z" }),
            "bulk" => json!([{ "_id": "z" }]),
            "find" | "count" => json!({ "filter": {} }),
            "aggregate" => json!({ "pipeline": [] }),
            "update" | "find_and_modify" => {
                json!({ "filter": { "_id": "a" }, "update": { "$set": { "note": "y" } } })
            }
            "delete" => json!({ "filter": { "_id": "z" } }),
            "{id}" => json!({ "note": "y" }),
            "indexes" => json!({ "fields": [{ "path": "other" }] }),
            "vector" => json!({ "fields": ["note"], "provider": { "kind": "byo" }, "dim": 3 }),
            "vectors" => json!([{ "chunk": 0, "vector": [1.0, 0.0, 0.0], "text": "x" }]),
            "vector_search" => json!({ "vector": [1.0, 0.0, 0.0], "k": 1 }),
            "hybrid_search" => json!({ "query": "x", "vector": [1.0, 0.0, 0.0], "k": 1 }),
            "webhooks" => json!({ "url": "https://example.com/hook", "operations": ["insert"] }),
            other => panic!("no minimal body known for {method} {template} ({other})"),
        })
    };

    let mut driven = 0;
    for (template, item) in spec["paths"].as_object().expect("paths") {
        for (method, _) in item.as_object().expect("a path item") {
            if method == "parameters" {
                continue;
            }
            let method = method.to_uppercase();
            let name = if template.starts_with("/v1/users") {
                "clerk"
            } else if template.starts_with("/v1/roles") {
                "reader"
            } else {
                index_name.as_str()
            };
            let id = if template.ends_with("/webhooks/{id}") { hook_id.as_str() } else { "a" };
            let path = template
                .replace("{db}", "shop")
                .replace("{coll}", "orders")
                .replace("{id}", id)
                .replace("{name}", name)
                .replace("{resource_path}", "nodes/one");
            let url = format!("{}{path}?zz=1", server.base);
            let body = body_for(&method, template);
            let res = server.client.request(&method, &url, Some(&token), body).await;
            assert_eq!(res.status, 400, "{method} {path}?zz=1: {:?}", res.body);
            assert_eq!(res.body["error"], "bad_request", "{method} {path}: {:?}", res.body);
            assert_eq!(res.body["retry"], "no", "{method} {path}: {:?}", res.body);
            let message = res.body["message"].as_str().unwrap_or_default();
            assert!(
                message.contains("`zz`"),
                "{method} {path}: must name the parameter: {message}"
            );
            driven += 1;
        }
    }
    // A walk that matched nothing would pass vacuously.
    assert!(driven > 50, "only {driven} operations were driven; the spec walk is broken");

    // Nothing the walk sent was acted on: the fixtures it named still stand,
    // and the document the bodied writes named is as it was.
    let doc = server.get("/v1/db/shop/coll/orders/docs/a", Some(&token)).await;
    assert_eq!(doc.body["note"], "x", "{:?}", doc.body);
    let users = server.get("/v1/users", Some(&token)).await;
    assert!(users.body.to_string().contains("clerk"), "{:?}", users.body);
}

/// A router merged beside the REST table keeps its own query semantics.
///
/// `/mcp` is merged through `router_with_limits` after `routes` has applied
/// the guard, so it is outside it by construction — MCP is a separate
/// transport, and what its query string means is rmcp's to decide (ADR-124).
/// A stand-in rather than the real MCP router, because this crate does not
/// depend on that one; `crates/kimmy-mcp` drives the real thing.
#[tokio::test]
async fn a_router_merged_beside_the_rest_table_is_outside_the_query_string_guard() {
    use axum::routing::post;

    let extra = axum::Router::new().route("/mcp", post(|| async { "answered" }));
    let server = Server::start_with_extra(extra).await;
    let token = server.root().await;

    let res = server.post("/mcp?zz=1", Some(&token), json!({})).await;
    assert_eq!(res.status, 200, "{:?}", res.body);

    // While the table beside it is guarded on the same listener.
    let res = server.post("/v1/db/shop/collections?zz=1", Some(&token), json!({})).await;
    assert_eq!(res.status, 400, "{:?}", res.body);
}

/// A document body is content, not a request shape, and stays open.
///
/// The names a request shape reserves — `filter`, `limit`, `update` — are
/// perfectly good field names for a document, and refusing them on insert
/// would make the store's schemalessness a lie (ADR-055).
#[tokio::test]
async fn a_document_body_may_carry_any_field() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;

    let document = json!({ "_id": 1, "zzz": 1, "filter": {"a": 1}, "limit": 3, "update": [] });
    let res = server.post("/v1/db/shop/coll/orders/docs", Some(&token), document.clone()).await;
    assert_eq!(res.status, 200, "insert: {:?}", res.body);

    let res = server
        .put("/v1/db/shop/coll/orders/docs/1", Some(&token), json!({ "zzz": 2, "explain": true }))
        .await;
    assert_eq!(res.status, 200, "replace: {:?}", res.body);

    let res = server
        .post("/v1/db/shop/coll/orders/bulk", Some(&token), json!([{ "zzz": 3, "pipeline": [] }]))
        .await;
    assert_eq!(res.status, 200, "bulk: {:?}", res.body);
    assert_eq!(res.body["inserted"], 1);
}

#[tokio::test]
async fn queries_filter_sort_and_project() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;

    for (id, item, qty) in [(1, "widget", 5), (2, "gadget", 12), (3, "widget", 1)] {
        server
            .post(
                "/v1/db/shop/coll/orders/docs",
                Some(&token),
                json!({ "_id": id, "item": item, "qty": qty }),
            )
            .await;
    }

    let res = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({ "filter": {"qty": {"$gt": 4}}, "sort": {"qty": -1}, "projection": {"item":1,"_id":0} }),
        )
        .await;

    assert_eq!(res.body["count"], 2);
    assert_eq!(res.body["documents"][0], json!({ "item": "gadget" }));
    assert_eq!(res.body["documents"][1], json!({ "item": "widget" }));
}

/// `$expr` is parsed by the one filter parser every endpoint shares, so proving
/// it on `find`, `count`, `$match` and `update` is proving the parser once and
/// the plumbing four times. The predicate compares two fields of the same
/// document, which no other filter operator can write.
#[tokio::test]
async fn expr_compares_fields_of_the_same_document_everywhere_a_filter_is_taken() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"accounts"})).await;

    for (id, account, spent, budget) in
        [(1, "acme", 120, 100), (2, "acme", 80, 100), (3, "globex", 500, 100), (4, "globex", 0, 0)]
    {
        let res = server
            .post(
                "/v1/db/shop/coll/accounts/docs",
                Some(&token),
                json!({"_id": id, "account": account, "spent": spent, "budget": budget}),
            )
            .await;
        assert_eq!(res.status, 200, "{:?}", res.body);
    }
    let over_budget = json!({"$expr": {"$gt": ["$spent", "$budget"]}});

    let res = server
        .post(
            "/v1/db/shop/coll/accounts/find",
            Some(&token),
            json!({"filter": over_budget, "sort": {"_id": 1}, "projection": {"_id": 1}}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["documents"], json!([{"_id": 1}, {"_id": 3}]));

    let res = server
        .post("/v1/db/shop/coll/accounts/count", Some(&token), json!({"filter": over_budget}))
        .await;
    assert_eq!(res.body["count"], 2, "{:?}", res.body);

    // `$expr` is never index-eligible on its own, but an equality beside it
    // still plans, and the expression is re-applied to every candidate.
    let res = server
        .post(
            "/v1/db/shop/coll/accounts/indexes",
            Some(&token),
            json!({"fields": [{"path": "account"}]}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let res = server
        .post(
            "/v1/db/shop/coll/accounts/find",
            Some(&token),
            json!({"filter": over_budget, "explain": true}),
        )
        .await;
    assert_eq!(res.body["explain"]["strategy"], "collectionScan", "{:?}", res.body);
    let res = server
        .post(
            "/v1/db/shop/coll/accounts/find",
            Some(&token),
            json!({
                "filter": {"account": "acme", "$expr": {"$gt": ["$spent", "$budget"]}},
                "projection": {"_id": 1},
                "explain": true
            }),
        )
        .await;
    assert_eq!(res.body["explain"]["strategy"], "index", "{:?}", res.body);
    assert_eq!(res.body["documents"], json!([{"_id": 1}]));

    // `$match` takes the same filter, and an expression there can lean on
    // arithmetic just as it does in `$addFields`.
    let res = server
        .post(
            "/v1/db/shop/coll/accounts/aggregate",
            Some(&token),
            json!({"pipeline": [
                {"$match": {"$expr": {"$gte": [{"$subtract": ["$spent", "$budget"]}, 400]}}},
                {"$project": {"_id": 1}}
            ]}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["documents"], json!([{"_id": 3}]));

    let res = server
        .post(
            "/v1/db/shop/coll/accounts/update",
            Some(&token),
            json!({"filter": over_budget, "update": {"$set": {"flag": "over"}}, "multi": true}),
        )
        .await;
    assert_eq!(res.body["matched"], 2, "{:?}", res.body);
    let res = server.get("/v1/db/shop/coll/accounts/docs/2", Some(&token)).await;
    assert!(res.body.get("flag").is_none(), "an in-budget account was flagged: {:?}", res.body);

    // A malformed expression is the filter parser's ordinary 400.
    let res = server
        .post(
            "/v1/db/shop/coll/accounts/find",
            Some(&token),
            json!({"filter": {"$expr": {"$nope": ["$spent", 1]}}}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
}

#[tokio::test]
async fn updates_apply_operators() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1,"n":5})).await;
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":2,"n":5})).await;

    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({ "filter": {}, "update": {"$inc": {"n": 10}}, "multi": true }),
        )
        .await;
    assert_eq!(res.body["modified"], 2);

    let res = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await;
    assert_eq!(res.body["n"], 15);
}

/// Two operators on one path apply in the order their keys arrive, and the
/// last one wins — the promise `docs/query-language.md` makes. The parser
/// kept it from the start; the boundary broke it, because a `serde_json::Map`
/// built without `preserve_order` is a `BTreeMap` and sorted every object's
/// keys before the parser saw them, so both orders below ran `$inc` first
/// and both left `1`. Driven over HTTP on purpose: a `doc!`-built test in the
/// parser never crossed the boundary and never saw the defect (ADR-120).
#[tokio::test]
async fn update_operators_apply_in_the_order_the_request_wrote_them() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    // The bodies are written as text, not with `json!`, so the key order on
    // the wire is exactly the order that appears here.
    async fn apply(server: &Server, token: &str, update: &str) -> Value {
        server.post("/v1/db/shop/coll/c/docs", Some(token), json!({"_id":1,"a":0})).await;
        let body = format!(r#"{{"filter":{{"_id":1}},"update":{update}}}"#);
        let body: Value = serde_json::from_str(&body).unwrap();
        let res = server.post("/v1/db/shop/coll/c/update", Some(token), body).await;
        assert_eq!(res.status, 200, "{:?}", res.body);
        let res = server.get("/v1/db/shop/coll/c/docs/1", Some(token)).await;
        let value = res.body["a"].clone();
        server.delete("/v1/db/shop/coll/c/docs/1", Some(token)).await;
        value
    }

    assert_eq!(apply(&server, &token, r#"{"$set":{"a":1},"$inc":{"a":5}}"#).await, 6);
    assert_eq!(apply(&server, &token, r#"{"$inc":{"a":5},"$set":{"a":1}}"#).await, 1);
    assert_eq!(apply(&server, &token, r#"{"$set":{"a":7},"$mul":{"a":10}}"#).await, 70);
    assert_eq!(apply(&server, &token, r#"{"$mul":{"a":10},"$set":{"a":7}}"#).await, 7);

    // `$min` then `$max` on 5: 3, then 10. The reverse: 10, then 3.
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":2,"a":5})).await;
    let body: Value =
        serde_json::from_str(r#"{"filter":{"_id":2},"update":{"$min":{"a":3},"$max":{"a":10}}}"#)
            .unwrap();
    server.post("/v1/db/shop/coll/c/update", Some(&token), body).await;
    let res = server.get("/v1/db/shop/coll/c/docs/2", Some(&token)).await;
    assert_eq!(res.body["a"], 10, "$min then $max on 5: {:?}", res.body);

    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":3,"a":5})).await;
    let body: Value =
        serde_json::from_str(r#"{"filter":{"_id":3},"update":{"$max":{"a":10},"$min":{"a":3}}}"#)
            .unwrap();
    server.post("/v1/db/shop/coll/c/update", Some(&token), body).await;
    let res = server.get("/v1/db/shop/coll/c/docs/3", Some(&token)).await;
    assert_eq!(res.body["a"], 3, "$max then $min on 5: {:?}", res.body);
}

/// A sort document's key order is its precedence, and it too crossed the
/// sorted-map boundary: `{"b": 1, "a": 1}` used to sort by `a` first. The
/// data is arranged so the two orders give opposite results.
#[tokio::test]
async fn a_sort_document_keeps_the_precedence_it_was_written_in() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1,"a":1,"b":2})).await;
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":2,"a":2,"b":1})).await;

    async fn order(server: &Server, token: &str, sort: &str) -> Vec<i64> {
        let body: Value = serde_json::from_str(&format!(r#"{{"sort":{sort}}}"#)).unwrap();
        let res = server.post("/v1/db/shop/coll/c/find", Some(token), body).await;
        assert_eq!(res.status, 200, "{:?}", res.body);
        res.body["documents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["_id"].as_i64().unwrap())
            .collect()
    }

    assert_eq!(order(&server, &token, r#"{"a":1,"b":1}"#).await, vec![1, 2]);
    assert_eq!(order(&server, &token, r#"{"b":1,"a":1}"#).await, vec![2, 1]);
}

/// A stored document's fields come back in the order they were written, as
/// BSON keeps them and MongoDB returns them. The test client parses the body
/// into a `serde_json::Value`, which since ADR-120 keeps the text's key order,
/// so `keys()` here is the order of the JSON text on the wire; rendering the
/// value back to text pins the same thing a client's raw reader would see.
#[tokio::test]
async fn a_document_reads_back_with_its_fields_in_the_order_they_were_written() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let body: Value = serde_json::from_str(r#"{"_id":"x","zeta":1,"alpha":2,"mid":3}"#).unwrap();
    let res = server.post("/v1/db/shop/coll/c/docs", Some(&token), body).await;
    assert_eq!(res.status, 200, "{:?}", res.body);

    let res = server.get("/v1/db/shop/coll/c/docs/x", Some(&token)).await;
    let keys: Vec<&str> = res.body.as_object().unwrap().keys().map(String::as_str).collect();
    assert_eq!(keys, vec!["_id", "zeta", "alpha", "mid"], "{:?}", res.body);
    assert_eq!(res.body.to_string(), r#"{"_id":"x","zeta":1,"alpha":2,"mid":3}"#);

    // The same document through `find`, which renders through a different
    // handler but the same `document_to_json`.
    let res = server.post("/v1/db/shop/coll/c/find", Some(&token), json!({})).await;
    assert_eq!(res.body["documents"][0].to_string(), r#"{"_id":"x","zeta":1,"alpha":2,"mid":3}"#);

    // An inclusion projection answers in the document's order, not the
    // specification's, and `_id` stays first rather than landing where the
    // parser appends it. Checked through `find` and the `$project` stage, the
    // two renderers that share `shape::project`.
    let body: Value = serde_json::from_str(r#"{"projection":{"alpha":1,"zeta":1}}"#).unwrap();
    let res = server.post("/v1/db/shop/coll/c/find", Some(&token), body).await;
    assert_eq!(res.body["documents"][0].to_string(), r#"{"_id":"x","zeta":1,"alpha":2}"#);
    let body: Value =
        serde_json::from_str(r#"{"pipeline":[{"$project":{"alpha":1,"zeta":1}}]}"#).unwrap();
    let res = server.post("/v1/db/shop/coll/c/aggregate", Some(&token), body).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["documents"][0].to_string(), r#"{"_id":"x","zeta":1,"alpha":2}"#);
}

/// A replace stores `_id` first like an insert does. The storage layer used
/// to append it to a body that left it out, which nobody could see while the
/// boundary sorted every object; a `PUT` and a `POST` of the same body now
/// read back identically.
#[tokio::test]
async fn a_replaced_document_reads_back_with_its_id_first() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    // Created through the upsert, which is the replace path too.
    let body: Value = serde_json::from_str(r#"{"zeta":1,"alpha":2}"#).unwrap();
    let res = server.put("/v1/db/shop/coll/c/docs/x?upsert=true", Some(&token), body).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["upserted"], true, "{:?}", res.body);
    let res = server.get("/v1/db/shop/coll/c/docs/x", Some(&token)).await;
    assert_eq!(res.body.to_string(), r#"{"_id":"x","zeta":1,"alpha":2}"#);

    // Replacing an existing document, and one whose body carries `_id`
    // somewhere other than first, land the same way.
    let body: Value = serde_json::from_str(r#"{"zeta":3,"_id":"x","alpha":4}"#).unwrap();
    let res = server.put("/v1/db/shop/coll/c/docs/x", Some(&token), body).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let res = server.get("/v1/db/shop/coll/c/docs/x", Some(&token)).await;
    assert_eq!(res.body.to_string(), r#"{"_id":"x","zeta":3,"alpha":4}"#);
}

/// `modified` counts documents **written**, not documents changed, so it equals
/// `matched` even when the operators moved nothing. This is the deliberate
/// deviation in `docs/deviations.md`, and `docs/openapi.yaml` stated the
/// opposite until 2026-08-26 — nothing caught it because nothing ever asserted
/// on the value for an update that changes nothing.
#[tokio::test]
async fn a_no_op_update_still_counts_as_modified() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1,"g":"x"})).await;
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":2,"g":"x"})).await;

    // Every document is already in the state the update asks for.
    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({ "filter": {}, "update": {"$set": {"g": "x"}}, "multi": true }),
        )
        .await;
    assert_eq!(res.body["matched"], 2);
    assert_eq!(res.body["modified"], 2, "modified counts writes, not changes");

    // The same holds for `$unset` of a field that was never present.
    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({ "filter": {}, "update": {"$unset": {"never_here": ""}}, "multi": true }),
        )
        .await;
    assert_eq!(res.body["modified"], 2, "modified counts writes, not changes");
}

/// `$[<identifier>]` with `arrayFilters` and `$[]` address array elements
/// through the request, on `update` and on `find_and_modify` (ADR-104). The
/// refusals are `400`s: an identifier without a filter, a filter without an
/// identifier, the `$` positional operator, and a positional segment where
/// there is no array.
#[tokio::test]
async fn positional_updates_address_array_elements() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    server
        .post(
            "/v1/db/shop/coll/c/docs",
            Some(&token),
            json!({"_id": 1, "items": [
                {"sku": "a", "qty": 1, "shipped": false, "tags": ["x", "y"]},
                {"sku": "b", "qty": 5, "shipped": false, "tags": ["x", "z"]},
            ]}),
        )
        .await;
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id": 2, "items": "no"})).await;

    // Mark one line item shipped, and nothing else.
    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({
                "filter": {"_id": 1},
                "update": {"$set": {"items.$[line].shipped": true}},
                "arrayFilters": [{"line.sku": "b"}],
            }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["modified"], 1);
    let doc = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await.body;
    assert_eq!(doc["items"][0]["shipped"], false, "{doc}");
    assert_eq!(doc["items"][1]["shipped"], true, "{doc}");

    // `$[]` reaches every element; `$inc` through it keeps integers integral.
    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({ "filter": {"_id": 1}, "update": {"$inc": {"items.$[].qty": 1}} }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let doc = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await.body;
    assert_eq!(doc["items"][0]["qty"], 2, "{doc}");
    assert_eq!(doc["items"][1]["qty"], 6, "{doc}");

    // No element selected: the document is written back unchanged, and
    // `modified` counts the write, as the register says it does.
    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({
                "filter": {"_id": 1},
                "update": {"$set": {"items.$[line].shipped": "never"}},
                "arrayFilters": [{"line.sku": "z"}],
            }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["matched"], 1);
    assert_eq!(res.body["modified"], 1, "modified counts writes, not changes");
    let after = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await.body;
    assert_eq!(after, doc, "nothing selected, nothing changed");

    // `find_and_modify` takes the same field and returns the new document;
    // a `$pull` inside the selected element's own array goes through too.
    let res = server
        .post(
            "/v1/db/shop/coll/c/find_and_modify",
            Some(&token),
            json!({
                "filter": {"_id": 1},
                "update": {"$pull": {"items.$[line].tags": "x"}},
                "arrayFilters": [{"line.qty": {"$gt": 3}}],
                "returnDocument": "after",
            }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let doc = &res.body["document"];
    assert_eq!(doc["items"][0]["tags"], json!(["x", "y"]), "{doc}");
    assert_eq!(doc["items"][1]["tags"], json!(["z"]), "{doc}");

    // The refusals.
    let refused = |update: Value, filters: Option<Value>, id: i64| {
        let server = &server;
        let token = &token;
        async move {
            let mut body = json!({ "filter": {"_id": id}, "update": update });
            if let Some(filters) = filters {
                body["arrayFilters"] = filters;
            }
            let res = server.post("/v1/db/shop/coll/c/update", Some(token), body).await;
            assert_eq!(res.status, 400, "{:?}", res.body);
            res.body["message"].as_str().unwrap_or_default().to_string()
        }
    };
    let msg = refused(json!({"$set": {"items.$[line].shipped": true}}), None, 1).await;
    assert!(msg.contains("no filter"), "{msg}");
    let msg = refused(
        json!({"$set": {"items.$[line].shipped": true}}),
        Some(json!([{"line.sku": "a"}, {"other.sku": "b"}])),
        1,
    )
    .await;
    assert!(msg.contains("no update path uses"), "{msg}");
    let msg = refused(json!({"$set": {"items.$.shipped": true}}), None, 1).await;
    assert!(msg.contains("$[<identifier>]"), "{msg}");
    let msg = refused(json!({"$set": {"items.$[].shipped": true}}), None, 2).await;
    assert!(msg.contains("must be an array"), "{msg}");
    // A refused update writes nothing.
    let untouched = server.get("/v1/db/shop/coll/c/docs/2", Some(&token)).await.body;
    assert_eq!(untouched["items"], "no");
}

/// A `multi: true` request lands in chunks of `storage.multi_chunk_docs`
/// (the engine default here, 1,000) and says how many (ADR-086).
#[tokio::test]
async fn a_multi_update_reports_its_commits() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    for batch in [0..1000i64, 1000..2000, 2000..2500] {
        let docs: Vec<Value> = batch.map(|i| json!({"_id": i, "n": 0})).collect();
        let res = server.post("/v1/db/shop/coll/c/bulk", Some(&token), json!(docs)).await;
        assert_eq!(res.status, 200, "{:?}", res.body);
    }

    let commits_before = server.state.engine.commits();
    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({ "filter": {}, "update": {"$inc": {"n": 1}}, "multi": true }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["matched"], 2500);
    assert_eq!(res.body["modified"], 2500);
    assert_eq!(res.body["commits"], 3, "1,000 + 1,000 + 500");
    assert_eq!(server.state.engine.commits() - commits_before, 3);

    let res = server.get("/v1/db/shop/coll/c/docs/2499", Some(&token)).await;
    assert_eq!(res.body["n"], 1);

    let res = server
        .post("/v1/db/shop/coll/c/delete", Some(&token), json!({ "filter": {}, "multi": true }))
        .await;
    assert_eq!(res.body["deleted"], 2500);
    assert_eq!(res.body["commits"], 3);

    // A single-document request is one chunk of one, and reports it.
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id": 1, "n": 0})).await;
    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({ "filter": {"_id": 1}, "update": {"$inc": {"n": 1}} }),
        )
        .await;
    assert_eq!(res.body["commits"], 1);
    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({ "filter": {"_id": 99}, "update": {"$inc": {"n": 1}} }),
        )
        .await;
    assert_eq!(res.body["commits"], 0, "nothing matched, nothing committed");
}

/// A cross-node unique collision is reported until one side is gone (ADR-087).
///
/// The collision is manufactured through `apply_remote`, exactly as the
/// storage tests do: a remote insert carrying a value a local document
/// already holds under a unique index. That is the same code path a
/// replicated write takes, without spawning a second process.
#[tokio::test]
async fn standing_unique_violations_are_reported_until_resolved() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"users"})).await;
    let res = server
        .post(
            "/v1/db/shop/coll/users/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "email" }], "unique": true }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    server
        .post(
            "/v1/db/shop/coll/users/docs",
            Some(&token),
            json!({"_id": "local", "email": "clash@x"}),
        )
        .await;

    let res = server.get("/v1/db/shop/coll/users/violations", Some(&token)).await;
    assert_eq!(res.body, json!({ "count": 0, "indexes": [] }));

    // A peer inserted the same email under another _id; merging it breaks
    // the constraint on this node.
    let meta = server.state.engine.get_collection("shop", "users").unwrap();
    let remote = kimmy_core::OplogEntry {
        stamp: kimmy_core::Stamp::new(
            kimmy_core::Hlc::new(9_000, 0),
            kimmy_core::NodeId::generate(),
        ),
        kind: kimmy_core::OpKind::Insert,
        collection: meta.id,
        doc_id: Some(kimmy_core::DocId::String("remote".into())),
        body: Some(
            bson::serialize_to_vec(&bson::doc! { "_id": "remote", "email": "clash@x" }).unwrap(),
        ),
    };
    server.state.engine.apply_remote(&meta, &remote).unwrap();

    let res = server.get("/v1/db/shop/coll/users/violations", Some(&token)).await;
    assert_eq!(res.body["count"], 1, "{:?}", res.body);
    assert_eq!(res.body["indexes"], json!([{ "name": "email_1", "count": 1 }]));

    let res = server.get("/v1/db/shop/coll/users/violations?index=email_1", Some(&token)).await;
    assert_eq!(res.body["count"], 1, "{:?}", res.body);
    let group = &res.body["groups"][0];
    assert_eq!(group["merged"], "remote");
    let mut ids: Vec<String> =
        group["ids"].as_array().unwrap().iter().map(|v| v.as_str().unwrap().to_string()).collect();
    ids.sort();
    assert_eq!(ids, vec!["local", "remote"]);
    assert_eq!(
        group["documents"].as_array().unwrap().len(),
        2,
        "both documents, to choose between"
    );

    let res = server.get("/v1/db/shop/coll/users/violations?index=other", Some(&token)).await;
    assert_eq!(res.body["count"], 0);

    // Resolve it by deleting one side: the report clears.
    server.delete("/v1/db/shop/coll/users/docs/remote", Some(&token)).await;
    let res = server.get("/v1/db/shop/coll/users/violations", Some(&token)).await;
    assert_eq!(res.body["count"], 0, "{:?}", res.body);

    // Read is enough to look; a principal without it is refused.
    let res = server.get("/v1/db/shop/coll/users/violations", None).await;
    assert_eq!(res.status, 401);
}

/// Rewriting a colliding value resolves a violation as surely as deleting the
/// document, and the report says so (ADR-087, amended).
///
/// Three documents share one email: one local, two merged from peers, so the
/// oplog holds a two-member record and then a three-member one, as it does in
/// production. The report is checked after each kind of resolution the
/// recipe offers — an unrelated rewrite (nothing changes), a rewrite of the
/// colliding value (the member leaves its group), and a delete of a member of
/// a three-way group (the other two still stand).
#[tokio::test]
async fn a_rewritten_member_leaves_its_violation_group() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"users"})).await;
    let res = server
        .post(
            "/v1/db/shop/coll/users/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "email" }], "unique": true }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    server
        .post(
            "/v1/db/shop/coll/users/docs",
            Some(&token),
            json!({"_id": "a", "email": "clash@x", "name": "first"}),
        )
        .await;

    let meta = server.state.engine.get_collection("shop", "users").unwrap();
    for (id, hlc) in [("b", 9_000), ("c", 9_001)] {
        let remote = kimmy_core::OplogEntry {
            stamp: kimmy_core::Stamp::new(
                kimmy_core::Hlc::new(hlc, 0),
                kimmy_core::NodeId::generate(),
            ),
            kind: kimmy_core::OpKind::Insert,
            collection: meta.id,
            doc_id: Some(kimmy_core::DocId::String(id.into())),
            body: Some(
                bson::serialize_to_vec(&bson::doc! { "_id": id, "email": "clash@x" }).unwrap(),
            ),
        };
        server.state.engine.apply_remote(&meta, &remote).unwrap();
    }

    let groups = |body: &Value| -> Vec<Vec<String>> {
        body["groups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| {
                let mut ids: Vec<String> = g["ids"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect();
                ids.sort();
                ids
            })
            .collect()
    };
    let by_index = "/v1/db/shop/coll/users/violations?index=email_1";

    let res = server.get(by_index, Some(&token)).await;
    let all_three = vec!["a".to_string(), "b".into(), "c".into()];
    assert!(
        groups(&res.body).contains(&all_three),
        "the three-way collision stands: {:?}",
        res.body
    );

    // A local rewrite that keeps the colliding value is refused, as any local
    // write into an occupied key is — the existing rule, which is what makes
    // "rewrite the value" the only local way out short of a delete — and the
    // group stands as it was.
    let res = server
        .post(
            "/v1/db/shop/coll/users/update",
            Some(&token),
            json!({ "filter": {"_id": "a"}, "update": {"$set": {"name": "renamed"}} }),
        )
        .await;
    assert_eq!(res.status, 409, "{:?}", res.body);
    let res = server.get(by_index, Some(&token)).await;
    assert!(groups(&res.body).contains(&all_three), "still colliding: {:?}", res.body);
    let group = res.body["groups"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["ids"].as_array().unwrap().len() == 3)
        .unwrap();
    assert_eq!(group["documents"].as_array().unwrap().len(), 3, "documents follow ids");

    // Rewriting c's email takes c out of the collision. What remains is one
    // group of a and b: the three-member record shrinks to that pair, and the
    // two-member record already names it, so they are reported once.
    let res = server
        .post(
            "/v1/db/shop/coll/users/update",
            Some(&token),
            json!({ "filter": {"_id": "c"}, "update": {"$set": {"email": "c@x"}} }),
        )
        .await;
    assert_eq!(res.body["modified"], 1, "{:?}", res.body);
    let res = server.get(by_index, Some(&token)).await;
    assert_eq!(groups(&res.body), vec![vec!["a".to_string(), "b".into()]], "{:?}", res.body);
    assert_eq!(res.body["count"], 1);
    let group = &res.body["groups"][0];
    assert_eq!(group["documents"].as_array().unwrap().len(), 2, "c's document is not listed");
    let res = server.get("/v1/db/shop/coll/users/violations", Some(&token)).await;
    assert_eq!(res.body, json!({ "count": 1, "indexes": [{ "name": "email_1", "count": 1 }] }));

    // Rewriting b's email too leaves a alone under the key: nothing stands.
    server
        .post(
            "/v1/db/shop/coll/users/update",
            Some(&token),
            json!({ "filter": {"_id": "b"}, "update": {"$set": {"email": "b@x"}} }),
        )
        .await;
    let res = server.get(by_index, Some(&token)).await;
    assert_eq!(res.body["count"], 0, "{:?}", res.body);
    let res = server.get("/v1/db/shop/coll/users/violations", Some(&token)).await;
    assert_eq!(res.body, json!({ "count": 0, "indexes": [] }));

    // Rewriting back into the collision is refused locally, as it always was,
    // so the report cannot be re-populated by this node's own writes.
    let res = server
        .post(
            "/v1/db/shop/coll/users/update",
            Some(&token),
            json!({ "filter": {"_id": "b"}, "update": {"$set": {"email": "clash@x"}} }),
        )
        .await;
    assert_eq!(res.status, 409, "{:?}", res.body);
}

/// Deleting one member of a three-way collision leaves the other two standing.
///
/// Under the original definition — every named document still exists — the
/// three-member record dropped out whole; with keys re-evaluated, the two
/// survivors still share the key and are still reported as a group.
#[tokio::test]
async fn deleting_one_member_of_a_three_way_collision_leaves_the_other_two_standing() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"users"})).await;
    server
        .post(
            "/v1/db/shop/coll/users/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "email" }], "unique": true }),
        )
        .await;
    server
        .post("/v1/db/shop/coll/users/docs", Some(&token), json!({"_id": "a", "email": "clash@x"}))
        .await;

    // Two merges from one peer: the oplog records {a, b} and then {a, b, c}.
    let meta = server.state.engine.get_collection("shop", "users").unwrap();
    let origin = kimmy_core::NodeId::generate();
    let entry =
        |hlc: u64, kind: kimmy_core::OpKind, id: &str, email: &str| kimmy_core::OplogEntry {
            stamp: kimmy_core::Stamp::new(kimmy_core::Hlc::new(hlc, 0), origin),
            kind,
            collection: meta.id,
            doc_id: Some(kimmy_core::DocId::String(id.into())),
            body: Some(bson::serialize_to_vec(&bson::doc! { "_id": id, "email": email }).unwrap()),
        };
    server
        .state
        .engine
        .apply_remote(&meta, &entry(9_000, kimmy_core::OpKind::Insert, "b", "clash@x"))
        .unwrap();
    server
        .state
        .engine
        .apply_remote(&meta, &entry(9_001, kimmy_core::OpKind::Insert, "c", "clash@x"))
        .unwrap();

    let res = server.get("/v1/db/shop/coll/users/violations?index=email_1", Some(&token)).await;
    assert!(
        res.body["groups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["ids"].as_array().unwrap().len() == 3),
        "{:?}",
        res.body
    );

    // Deleting c shrinks the three-member record to {a, b}, the same group the
    // two-member record names, so one group is reported rather than none.
    server.delete("/v1/db/shop/coll/users/docs/c", Some(&token)).await;
    let res = server.get("/v1/db/shop/coll/users/violations?index=email_1", Some(&token)).await;
    assert_eq!(res.body["count"], 1, "{:?}", res.body);
    let mut ids: Vec<String> = res.body["groups"][0]["ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["a", "b"], "the survivors still collide");

    // The index itself going away takes the constraint with it.
    server.delete("/v1/db/shop/coll/users/indexes/email_1", Some(&token)).await;
    let res = server.get("/v1/db/shop/coll/users/violations", Some(&token)).await;
    assert_eq!(res.body, json!({ "count": 0, "indexes": [] }));
}

/// `describe` carries the node's durability class, as `/v1/version` does
/// (ADR-088, amended): the same value from the same source, named for what it
/// is — a fact about the node, not the collection.
#[tokio::test]
async fn describe_reports_the_node_durability_class() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let described = server.get("/v1/db/shop/coll/c/describe", Some(&token)).await;
    let version = server.get("/v1/version", None).await;
    assert_eq!(described.body["nodeDurability"], "durable", "{:?}", described.body);
    assert_eq!(described.body["nodeDurability"], version.body["durability"]);

    server.state.engine.set_durability(
        kimmy_storage::DurabilityClass::Coalesced,
        std::time::Duration::from_millis(5),
    );
    let described = server.get("/v1/db/shop/coll/c/describe", Some(&token)).await;
    let version = server.get("/v1/version", None).await;
    assert_eq!(described.body["nodeDurability"], "coalesced");
    assert_eq!(version.body["durability"], "coalesced", "one fact, two routes");
}

#[tokio::test]
async fn describe_refuses_a_zero_sample_and_clamps_a_large_one() {
    // `sample=0` used to answer `200 {"sampled": 1}` — a clamp that
    // substituted a request the caller never made, against a specification
    // declaring `minimum: 1`. Found by a test round against a three-member
    // cluster running 0.20.0.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    for n in 0..3 {
        server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id": n, "n": n})).await;
    }

    let res = server.get("/v1/db/shop/coll/c/describe?sample=0", Some(&token)).await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert_eq!(res.body["error"], "bad_request");
    let message = res.body["message"].as_str().unwrap();
    assert!(message.contains("`sample`"), "names the parameter: {message}");
    assert!(message.contains("at least 1"), "states the minimum: {message}");

    // One is the smallest sample there is.
    let res = server.get("/v1/db/shop/coll/c/describe?sample=1", Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["sampled"], 1);

    // Above the ceiling is clamped, not refused: the caller asked for as
    // many as the server will read, and three is all there are.
    let res = server.get("/v1/db/shop/coll/c/describe?sample=5000", Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["sampled"], 3);

    // A value the parameter cannot hold at all is the ordinary parse refusal.
    let res = server.get("/v1/db/shop/coll/c/describe?sample=-1", Some(&token)).await;
    assert_eq!(res.status, 400, "{:?}", res.body);
}

/// Concurrent `$inc`s on one document must all land.
///
/// Multi-threaded on purpose: the defect this pins was a read transaction
/// collecting the match and a *separate* write transaction storing the
/// result, so two callers could read the same image and one increment was
/// lost — on a single node, against the documented per-document atomicity.
/// A current-thread runtime cannot interleave two synchronous executors and
/// would pass either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increments_through_update_are_all_kept() {
    const WRITERS: u64 = 4;
    const EACH: u64 = 500;

    let server = Arc::new(Server::start().await);
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1,"n":0})).await;

    let mut tasks = Vec::new();
    for _ in 0..WRITERS {
        let server = Arc::clone(&server);
        let token = token.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..EACH {
                let res = server
                    .post(
                        "/v1/db/shop/coll/c/update",
                        Some(&token),
                        json!({ "filter": {"_id": 1}, "update": {"$inc": {"n": 1}} }),
                    )
                    .await;
                assert_eq!(res.body["modified"], 1, "{:?}", res.body);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let res = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await;
    assert_eq!(
        res.body["n"],
        WRITERS * EACH,
        "an increment was lost: the operators ran outside the write transaction"
    );
}

// ---------------------------------------------------------------------------
// Conditional writes (ADR-084)
// ---------------------------------------------------------------------------

/// The stamp a write reported, or the one a stamped read returned.
fn stamp_of(body: &Value) -> String {
    body["stamp"].as_str().expect("a stamp is a string").to_string()
}

#[tokio::test]
async fn a_read_by_id_carries_its_stamp_as_an_etag_and_find_can_return_stamps() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    let inserted =
        server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1,"n":0})).await;
    let stamp = stamp_of(&inserted.body);

    let res = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await;
    assert_eq!(res.header("etag").as_deref(), Some(format!("\"{stamp}\"").as_str()));
    assert_eq!(res.body, json!({"_id": 1, "n": 0}), "the body is the document, nothing added");

    let res = server
        .post(
            "/v1/db/shop/coll/c/find",
            Some(&token),
            json!({ "filter": {"_id": 1}, "stamps": true }),
        )
        .await;
    assert_eq!(res.body["stamps"], json!([stamp]), "stamps parallel documents");
    assert_eq!(res.body["documents"][0], json!({"_id": 1, "n": 0}));

    let res = server.post("/v1/db/shop/coll/c/find", Some(&token), json!({ "filter": {} })).await;
    assert!(res.body.get("stamps").is_none(), "stamps only when asked for");

    let version = server.get("/v1/version", None).await;
    assert!(
        version.body["capabilities"].as_array().unwrap().contains(&json!("conditional-writes")),
        "{:?}",
        version.body
    );
    assert_eq!(version.body["durability"], "durable", "the class is queryable (ADR-088)");
}

#[tokio::test]
async fn a_delete_by_id_reports_the_tombstone_stamp() {
    // Every write reports the version it produced, and `POST .../delete` of
    // one document already did; the by-id route answered `{"deleted": 1}`
    // alone. Found by a test round against a three-member cluster running
    // 0.20.0.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1,"n":0})).await;

    let read = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await;
    let before = read.header("etag").expect("a read by id carries an ETag");
    let before = before.trim_matches('"').to_string();

    let res = server.delete("/v1/db/shop/coll/c/docs/1", Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["deleted"], 1, "{:?}", res.body);
    let stamp = stamp_of(&res.body);
    let decoded = kimmy_core::Stamp::decode(&stamp).expect("the stamp parses");
    assert_ne!(stamp, before, "the tombstone is a new version, not the one deleted");
    assert!(decoded > kimmy_core::Stamp::decode(&before).unwrap(), "and a later one");

    // Nothing to delete, nothing produced: `deleted` is 0 and there is no
    // stamp to report — the field is present exactly when `deleted` is 1.
    let res = server.delete("/v1/db/shop/coll/c/docs/1", Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body, json!({"deleted": 0}));

    // The conditional form reports it too.
    let inserted =
        server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":2,"n":0})).await;
    let current = stamp_of(&inserted.body);
    let res =
        server.delete(&format!("/v1/db/shop/coll/c/docs/2?if_stamp={current}"), Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["deleted"], 1);
    assert_ne!(stamp_of(&res.body), current);
}

#[tokio::test]
async fn a_conditional_replace_succeeds_at_the_current_stamp_and_moves_it() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    let inserted =
        server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1,"n":0})).await;
    let first = stamp_of(&inserted.body);

    let res = server
        .put(&format!("/v1/db/shop/coll/c/docs/1?if_stamp={first}"), Some(&token), json!({"n": 1}))
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["modified"], 1);
    let second = stamp_of(&res.body);
    assert_ne!(second, first, "a write moves the stamp");

    // The old stamp is now stale; the new one is current.
    let res = server
        .put(&format!("/v1/db/shop/coll/c/docs/1?if_stamp={first}"), Some(&token), json!({"n": 2}))
        .await;
    assert_eq!(res.status, 409, "{:?}", res.body);
    assert_eq!(res.body["error"], "stale");
    assert_eq!(res.body["retry"], "no");
    let res = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await;
    assert_eq!(res.body["n"], 1, "a stale write changes nothing");

    let res = server
        .put(&format!("/v1/db/shop/coll/c/docs/1?if_stamp={second}"), Some(&token), json!({"n": 2}))
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
}

#[tokio::test]
async fn a_stale_write_leaves_the_document_the_oplog_and_the_commit_count_alone() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    let inserted =
        server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1,"n":0})).await;
    let first = stamp_of(&inserted.body);
    // Move the document on, so `first` is stale.
    server.put("/v1/db/shop/coll/c/docs/1", Some(&token), json!({"n": 1})).await;

    let oplog_before = oplog_len(&server.state);
    let commits_before = server.state.engine.commits();

    let update = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({ "filter": {"_id": 1}, "update": {"$inc": {"n": 10}}, "if_stamp": first }),
        )
        .await;
    assert_eq!(update.status, 409, "{:?}", update.body);
    assert_eq!(update.body["error"], "stale");

    let fam = server
        .post(
            "/v1/db/shop/coll/c/find_and_modify",
            Some(&token),
            json!({ "filter": {"_id": 1}, "update": {"$set": {"n": 99}}, "if_stamp": first }),
        )
        .await;
    assert_eq!(fam.status, 409, "{:?}", fam.body);
    assert_eq!(fam.body["error"], "stale");

    let delete =
        server.delete(&format!("/v1/db/shop/coll/c/docs/1?if_stamp={first}"), Some(&token)).await;
    assert_eq!(delete.status, 409, "{:?}", delete.body);
    assert_eq!(delete.body["error"], "stale");

    assert_eq!(oplog_len(&server.state), oplog_before, "a refused write mints no oplog entry");
    assert_eq!(server.state.engine.commits(), commits_before, "a refused write commits nothing");
    let res = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await;
    assert_eq!(res.body["n"], 1);
}

#[tokio::test]
async fn a_conditional_write_on_a_missing_document_is_stale_not_a_no_op() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    let inserted = server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1})).await;
    let stamp = stamp_of(&inserted.body);
    server.delete("/v1/db/shop/coll/c/docs/1", Some(&token)).await;

    // Unconditional: a missing document is an ordinary miss.
    let res = server.put("/v1/db/shop/coll/c/docs/1", Some(&token), json!({"n": 1})).await;
    assert_eq!(res.body["matched"], 0);
    // Conditional: the caller expected a version, and it is gone.
    let res = server
        .put(&format!("/v1/db/shop/coll/c/docs/1?if_stamp={stamp}"), Some(&token), json!({"n": 1}))
        .await;
    assert_eq!(res.status, 409, "{:?}", res.body);
    assert_eq!(res.body["error"], "stale");
    // Even with upsert: the condition says "at this version", not "or create".
    let res = server
        .put(
            &format!("/v1/db/shop/coll/c/docs/1?upsert=true&if_stamp={stamp}"),
            Some(&token),
            json!({"n": 1}),
        )
        .await;
    assert_eq!(res.status, 409, "{:?}", res.body);
    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({ "filter": {"_id": 1}, "update": {"$set": {"n": 1}}, "if_stamp": stamp }),
        )
        .await;
    assert_eq!(res.status, 409, "{:?}", res.body);
    assert_eq!(res.body["error"], "stale");
}

#[tokio::test]
async fn if_stamp_refuses_multi_upsert_and_garbage() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    let inserted = server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1})).await;
    let stamp = stamp_of(&inserted.body);

    let res = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&token),
            json!({ "filter": {}, "update": {"$set": {"n": 1}}, "multi": true, "if_stamp": stamp }),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    let res = server
        .post(
            "/v1/db/shop/coll/c/find_and_modify",
            Some(&token),
            json!({ "filter": {"_id": 1}, "update": {"$set": {"n": 1}}, "upsert": true,
                    "if_stamp": stamp }),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    let res = server
        .put("/v1/db/shop/coll/c/docs/1?if_stamp=not-a-stamp", Some(&token), json!({"n": 1}))
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert_eq!(res.body["error"], "bad_request");
}

/// The reason to have this at all: check-then-act on one document, with
/// exactly one winner and no coordination.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_conditional_writers_produce_exactly_one_winner() {
    let server = Arc::new(Server::start().await);
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;
    let inserted =
        server.post("/v1/db/shop/coll/c/docs", Some(&token), json!({"_id":1,"owner":null})).await;
    let stamp = stamp_of(&inserted.body);

    let mut tasks = Vec::new();
    for writer in 0..8u64 {
        let server = Arc::clone(&server);
        let token = token.clone();
        let stamp = stamp.clone();
        tasks.push(tokio::spawn(async move {
            let res = server
                .post(
                    "/v1/db/shop/coll/c/find_and_modify",
                    Some(&token),
                    json!({ "filter": {"_id": 1}, "update": {"$set": {"owner": writer}},
                            "if_stamp": stamp, "returnDocument": "after" }),
                )
                .await;
            (writer, res.status, res.body)
        }));
    }
    let mut winners = Vec::new();
    for task in tasks {
        let (writer, status, body) = task.await.unwrap();
        match status {
            200 => winners.push(writer),
            409 => assert_eq!(body["error"], "stale"),
            other => panic!("writer {writer}: unexpected {other} {body:?}"),
        }
    }
    assert_eq!(winners.len(), 1, "exactly one conditional writer wins: {winners:?}");
    let res = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await;
    assert_eq!(res.body["owner"], winners[0]);
}

#[tokio::test]
async fn extended_json_types_survive_the_boundary() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let big = 9_007_199_254_740_993i64;
    server
        .post(
            "/v1/db/shop/coll/c/docs",
            Some(&token),
            json!({ "_id": 1, "big": big, "when": {"$date": 1_700_000_000_000i64} }),
        )
        .await;

    let res = server.get("/v1/db/shop/coll/c/docs/1", Some(&token)).await;
    // Exactness above 2^53 is the whole reason the boundary does not widen
    // whole numbers to double.
    assert_eq!(res.body["big"], json!(big));
    assert_eq!(res.body["when"], json!({ "$date": 1_700_000_000_000i64 }));
}

#[tokio::test]
async fn rbac_is_enforced_on_every_route() {
    let server = Server::start().await;
    let root = server.root().await;

    server.post("/v1/db/shop/collections", Some(&root), json!({"name":"orders"})).await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({
                "user": "analyst", "password": "analyst-password",
                "grants": [{"db":"shop","collection":"orders*","actions":["read","watch"]}]
            }),
        )
        .await;

    let analyst = server.login("analyst", "analyst-password").await;

    // Permitted.
    let res = server.post("/v1/db/shop/coll/orders/count", Some(&analyst), json!({})).await;
    assert_eq!(res.status, 200, "reading a granted collection must work");

    // Denied, each for a different reason.
    for (method, path, body) in [
        ("POST", "/v1/db/shop/coll/orders/docs", json!({"_id": 1})),
        ("POST", "/v1/db/shop/coll/orders/bulk", json!([{"_id": 1}])),
        ("POST", "/v1/db/shop/collections", json!({"name": "sneaky"})),
        ("POST", "/v1/users", json!({"user":"x","password":"password123"})),
    ] {
        let res = server
            .client
            .request(method, &format!("{}{path}", server.base), Some(&analyst), Some(body))
            .await;
        assert_eq!(res.status, 403, "{method} {path} should be forbidden");
    }

    // A collection outside the grant is forbidden, not "not found" — a 404
    // would let the caller probe for collections they cannot access.
    server.post("/v1/db/shop/collections", Some(&root), json!({"name":"salaries"})).await;
    let res = server.post("/v1/db/shop/coll/salaries/count", Some(&analyst), json!({})).await;
    assert_eq!(res.status, 403);

    // ...and a collection that does not exist at all gives the same answer.
    let res = server.post("/v1/db/shop/coll/imaginary/count", Some(&analyst), json!({})).await;
    assert_eq!(res.status, 403, "existence must not be observable through authorization");
}

#[tokio::test]
async fn listing_hides_what_the_caller_cannot_read() {
    let server = Server::start().await;
    let root = server.root().await;

    for name in ["orders", "salaries"] {
        server.post("/v1/db/shop/collections", Some(&root), json!({ "name": name })).await;
    }
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({
                "user": "analyst", "password": "analyst-password",
                "grants": [{"db":"shop","collection":"orders","actions":["read"]}]
            }),
        )
        .await;

    let analyst = server.login("analyst", "analyst-password").await;
    let res = server.get("/v1/db/shop/collections", Some(&analyst)).await;
    assert_eq!(res.body["collections"], json!(["orders"]));
}

#[tokio::test]
async fn the_last_user_cannot_be_deleted() {
    // Otherwise the server becomes unadministrable with no way back in short of
    // editing the data directory.
    let server = Server::start().await;
    let token = server.root().await;
    let res = server.delete("/v1/users/root", Some(&token)).await;
    assert_eq!(res.status, 409);
}

#[tokio::test]
async fn topology_lists_this_node_even_though_the_member_set_never_contains_it() {
    // The trap, stated as a test. `Members` holds **peers only**, so anything
    // derived from it alone can never include this node — the omission that
    // silently undelivered every clustered webhook (ADR-051). A topology that
    // inherited it would tell a client the cluster does not include the node
    // that just answered.
    let server = Server::start().await;
    let token = server.root().await;

    let res = server.get("/v1/topology", Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["count"], 1);
    let me = &res.body["nodes"][0];
    assert_eq!(me["self"], true);
    assert_eq!(me["status"], "live", "the node that answered is reachable by definition");
    assert_eq!(me["endpoint"], Value::Null, "nothing was advertised, and none is guessed");
}

/// `register` writes this node in, and `topology` still reports it exactly once.
///
/// Every in-process topology test wrote *peer* records directly and none ever
/// called `register`, so the registry never held this node and the
/// self-bookkeeping in `topology` was dead code in the whole default suite —
/// `me_seen` could be pinned to `false` with nothing failing, which would list
/// the answering node twice. The cluster harness does cover it, on three real
/// nodes, but it is `#[ignore]`: invisible to `cargo test --workspace` and to
/// any mutation pass.
#[tokio::test]
async fn a_node_registers_itself_and_is_listed_once() {
    let server = Server::start().await;
    let token = server.root().await;

    kimmy_api::topology::register(&server.state, "http://10.1.1.1:7878").expect("register");

    let res = server.get("/v1/topology", Some(&token)).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["count"], 1, "one node, listed once: {:?}", res.body);

    let nodes = res.body["nodes"].as_array().expect("a list");
    assert_eq!(
        nodes.iter().filter(|n| n["self"] == true).count(),
        1,
        "the answering node appears exactly once: {:?}",
        res.body
    );
    let me = &nodes[0];
    assert_eq!(me["self"], true);
    assert_eq!(me["endpoint"], "http://10.1.1.1:7878", "the advertised address, not a guess");
    assert_eq!(me["node"], server.state.engine.node_id().to_string());
}

/// Registering again writes nothing when nothing changed.
///
/// The docstring says so — "a node that restarts twice an hour should not
/// append to a log every other node then replicates" — and **nothing tested
/// it**. The registry is replicated, so a spurious rewrite is an oplog entry
/// every peer then carries; the guard is the whole reason `register` reads
/// before it writes. The cluster harness cannot catch this: it starts each
/// node once and never restarts one on an unchanged address.
#[tokio::test]
async fn registering_an_unchanged_record_appends_nothing() {
    let server = Server::start().await;
    let endpoint = "http://10.1.1.1:7878";

    kimmy_api::topology::register(&server.state, endpoint).expect("first register");
    let after_first = oplog_len(&server.state);

    kimmy_api::topology::register(&server.state, endpoint).expect("second register");
    assert_eq!(
        oplog_len(&server.state),
        after_first,
        "an unchanged record must be silent, or an idle cluster appends forever"
    );

    // But a real change still lands — silence must come from the comparison,
    // not from `register` having quietly become a no-op.
    kimmy_api::topology::register(&server.state, "http://10.1.1.2:7878").expect("moved");
    assert!(
        oplog_len(&server.state) > after_first,
        "a node that moved must advertise the new address"
    );

    let res = server.get("/v1/topology", Some(&server.root().await)).await;
    assert_eq!(res.body["nodes"][0]["endpoint"], "http://10.1.1.2:7878");
    assert_eq!(res.body["count"], 1, "moving is an update, not a second node");
}

/// How many entries the oplog holds, for tests whose subject is whether a
/// write happened at all.
fn oplog_len(state: &kimmy_api::SharedState) -> usize {
    state.engine.read_oplog_from(kimmy_core::Hlc::ZERO, 10_000).expect("reading the oplog").len()
}

#[tokio::test]
async fn a_registered_peer_is_reported_unknown_until_membership_sees_it() {
    // Address and liveness come from different places on purpose: the registry
    // says where a node is, SWIM says whether it is there. A registered node
    // nobody can vouch for is reported, not hidden — its gossip may be
    // partitioned while its HTTP is perfectly fine.
    let server = Server::start().await;
    let token = server.root().await;
    let peer = kimmy_core::NodeId::generate();

    server.register_peer(&peer, "http://10.0.0.9:7878").await;

    let nodes = server.get("/v1/topology", Some(&token)).await.body;
    assert_eq!(nodes["count"], 2);
    let entry = nodes["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["node"] == peer.to_string())
        .expect("the registered peer is listed");
    assert_eq!(entry["status"], "unknown");
    assert_eq!(entry["endpoint"], "http://10.0.0.9:7878");
    assert_eq!(entry["self"], false);

    // Now let membership see it, and the same record reads as live.
    let members = kimmy_cluster::Members::default();
    members.insert_for_test("10.0.0.9:7900".parse().unwrap(), peer);
    server.state.set_members(members);

    let nodes = server.get("/v1/topology", Some(&token)).await.body;
    let entry = nodes["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["node"] == peer.to_string())
        .expect("still listed");
    assert_eq!(entry["status"], "live", "membership is what makes it live: {nodes}");
}

/// The replication loop reports a peer that trails this node by more than
/// tombstone retention; topology shows it, and stops showing it once the peer
/// is back within the window (ADR-085).
#[tokio::test]
async fn topology_reports_a_peer_that_has_been_away_longer_than_tombstone_retention() {
    let server = Server::start().await;
    let token = server.root().await;
    let peer = kimmy_core::NodeId::generate();
    server.register_peer(&peer, "http://peer.example:7878").await;

    let find = |body: &Value| -> Value {
        body["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["node"] == peer.to_string())
            .cloned()
            .expect("the registered peer is listed")
    };

    let res = server.get("/v1/topology", Some(&token)).await;
    let entry = find(&res.body);
    assert!(entry.get("staleSince").is_none(), "fresh by default: {entry}");

    // Two rounds: the second refreshes the distance but not the start.
    server.state.report_peer_staleness(peer, Some(90_000_000));
    let first = find(&server.get("/v1/topology", Some(&token)).await.body);
    assert_eq!(first["behindSecs"], 90_000);
    assert!(first["staleSince"].is_number(), "{first}");
    server.state.report_peer_staleness(peer, Some(91_000_000));
    let second = find(&server.get("/v1/topology", Some(&token)).await.body);
    assert_eq!(second["behindSecs"], 91_000);
    assert_eq!(second["staleSince"], first["staleSince"], "the clock starts once");

    // This node itself is never stale to itself.
    let res = server.get("/v1/topology", Some(&token)).await;
    let me = res.body["nodes"].as_array().unwrap().iter().find(|n| n["self"] == true).unwrap();
    assert!(me.get("staleSince").is_none());

    // Caught up: the record goes, and the entry is the shape it always was.
    server.state.report_peer_staleness(peer, None);
    let entry = find(&server.get("/v1/topology", Some(&token)).await.body);
    assert!(entry.get("staleSince").is_none() && entry.get("behindSecs").is_none(), "{entry}");
}

#[tokio::test]
async fn topology_needs_a_token() {
    // Unlike /v1/version. A version is a fact about software; this is a map of
    // where a deployment's data lives.
    let server = Server::start().await;
    assert_eq!(server.get("/v1/topology", None).await.status, 401);
}

#[tokio::test]
async fn refresh_returns_a_working_token_and_says_how_long_it_lasts() {
    let server = Server::start().await;
    let root = server.root().await;

    let res = server.post("/v1/auth/refresh", Some(&root), json!({})).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["user"], "root");
    assert_eq!(res.body["expiresIn"], 3600, "the lifetime is told, not left to be decoded");

    let fresh = res.body["token"].as_str().expect("a token");
    assert_eq!(server.get("/v1/databases", Some(fresh)).await.status, 200);

    // Refreshing does not recall the old token. A stateless token cannot be
    // recalled, and saying so is better than implying otherwise.
    assert_eq!(server.get("/v1/databases", Some(&root)).await.status, 200);
}

#[tokio::test]
async fn refresh_cannot_revive_a_revoked_session() {
    // The failure this route most needed not to have. Refresh takes `Auth`, so
    // the presented token goes through the ADR-052 storage check before the
    // handler runs — a deleted account cannot refresh its way back in.
    let server = Server::start().await;
    let root = server.root().await;
    server.post("/v1/users", Some(&root), json!({"user":"ada","password":"ada-password"})).await;
    let ada = server.login("ada", "ada-password").await;
    assert_eq!(server.post("/v1/auth/refresh", Some(&ada), json!({})).await.status, 200);

    server.delete("/v1/users/ada", Some(&root)).await;

    let res = server.post("/v1/auth/refresh", Some(&ada), json!({})).await;
    assert_eq!(res.status, 401, "a deleted account must not refresh: {:?}", res.body);
}

#[tokio::test]
async fn a_changed_grant_stops_refresh_rather_than_being_carried_forward() {
    // Grants live in the token, so changing them bumps the token version and
    // every token the user holds stops working — including for refresh. That
    // is the deliberate cost of embedding grants: a narrowed permission takes
    // effect at once, and the price is logging in again.
    let server = Server::start().await;
    let root = server.root().await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({"user":"ada","password":"ada-password",
                   "grants":[{"db":"shop","collection":"*","actions":["read","write"]}]}),
        )
        .await;
    let ada = server.login("ada", "ada-password").await;

    server
        .post(
            "/v1/users/ada/grants",
            Some(&root),
            json!({"grants":[{"db":"shop","collection":"*","actions":["read"]}]}),
        )
        .await;

    let res = server.post("/v1/auth/refresh", Some(&ada), json!({})).await;
    assert_eq!(res.status, 401, "{:?}", res.body);

    // And logging in again gets the narrowed authority, not the old one.
    let ada = server.login("ada", "ada-password").await;
    let refreshed = server.post("/v1/auth/refresh", Some(&ada), json!({})).await;
    assert_eq!(refreshed.status, 200);
    let after = refreshed.body["token"].as_str().expect("a token");
    let whoami = server.get("/v1/auth/whoami", Some(after)).await;
    assert_eq!(whoami.body["grants"][0]["actions"], json!(["read"]));
}

#[tokio::test]
async fn an_expired_token_cannot_be_refreshed() {
    // No grace window: `exp` means the same thing on this route as on every
    // other one. A client idle past the lifetime logs in again.
    let server = Server::start().await;
    let issuer = TokenIssuer::new(SECRET, 3600).unwrap();
    let principal = kimmy_auth::Principal::superuser("root");
    // Issued two hours ago, so it expired an hour ago — without sleeping.
    let stale = issuer
        .issue_at(&principal, kimmy_storage::physical_now_ms() / 1000 - 7200)
        .expect("a token");

    let res = server.post("/v1/auth/refresh", Some(&stale), json!({})).await;
    assert_eq!(res.status, 401, "{:?}", res.body);
    assert_eq!(res.body["error"], "unauthorized");
}

/// The secret a rotation retires (ADR-101). `SECRET` is the one being retired
/// in these tests; this is the one taking over.
const NEXT_SECRET: &str = "the-secret-being-rotated-into-use!";

#[tokio::test]
async fn rotating_the_signing_secret_keeps_outstanding_tokens_working() {
    // The reason operators did not rotate: changing the secret ended every
    // session at once. With the old secret named as the previous one, a token
    // from before the rotation keeps working, while everything new is signed
    // with the new secret alone — so the window closes by itself one token
    // lifetime later.
    let before = Server::start().await;
    let root = before.root().await;
    before.post("/v1/users", Some(&root), json!({"user":"ada","password":"ada-password"})).await;
    let old_token = before.login("ada", "ada-password").await;
    assert_eq!(before.get("/v1/auth/whoami", Some(&old_token)).await.status, 200);

    // The rotation: the same database, restarted with the new secret current
    // and the old one previous.
    let rotating = before
        .restart_with_issuer(TokenIssuer::with_previous(NEXT_SECRET, Some(SECRET), 3600).unwrap())
        .await;
    let res = rotating.get("/v1/auth/whoami", Some(&old_token)).await;
    assert_eq!(res.status, 200, "a token from before the rotation must still work: {:?}", res.body);
    assert_eq!(res.body["user"], "ada");

    // A new login is signed with the new secret, and only the new secret: a
    // verifier holding just the retiring one refuses it.
    let new_token = rotating.login("ada", "ada-password").await;
    assert!(TokenIssuer::new(NEXT_SECRET, 3600).unwrap().verify(&new_token).is_ok());
    assert!(
        matches!(
            TokenIssuer::new(SECRET, 3600).unwrap().verify(&new_token),
            Err(kimmy_auth::AuthError::InvalidToken)
        ),
        "a token issued during the window must not be signed with the retiring secret"
    );

    // The window closed: the previous secret removed, the old token is refused
    // with the same 401 as any other bad token, and the new one is unaffected.
    let after = before.restart_with_issuer(TokenIssuer::new(NEXT_SECRET, 3600).unwrap()).await;
    let res = after.get("/v1/auth/whoami", Some(&old_token)).await;
    assert_eq!(res.status, 401, "{:?}", res.body);
    assert_eq!(res.body["error"], "unauthorized");
    assert_eq!(after.get("/v1/auth/whoami", Some(&new_token)).await.status, 200);
}

#[tokio::test]
async fn revocation_applies_to_a_token_verified_by_the_previous_secret() {
    // Rotation does not revoke, and revocation does not care which key verified
    // the signature: the ADR-052 version check runs after either one, so a
    // session opened before the rotation ends the moment its user's version
    // moves, exactly as it would have without a rotation in progress.
    let before = Server::start().await;
    let root = before.root().await;
    before.post("/v1/users", Some(&root), json!({"user":"ada","password":"ada-password"})).await;
    let old_token = before.login("ada", "ada-password").await;

    let rotating = before
        .restart_with_issuer(TokenIssuer::with_previous(NEXT_SECRET, Some(SECRET), 3600).unwrap())
        .await;
    assert_eq!(rotating.get("/v1/auth/whoami", Some(&old_token)).await.status, 200);

    // Root's token is from before the rotation too, and administers through
    // the window like any other outstanding session.
    let res = rotating
        .post("/v1/users/ada/password", Some(&root), json!({"password":"a-new-password"}))
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);

    let res = rotating.get("/v1/auth/whoami", Some(&old_token)).await;
    assert_eq!(
        res.status, 401,
        "a bumped version must end a previous-secret session: {:?}",
        res.body
    );

    // And the account itself is fine: a fresh login under the new password
    // opens a session signed with the current secret.
    let fresh = rotating.login("ada", "a-new-password").await;
    assert_eq!(rotating.get("/v1/auth/whoami", Some(&fresh)).await.status, 200);
    assert!(TokenIssuer::new(NEXT_SECRET, 3600).unwrap().verify(&fresh).is_ok());
}

#[tokio::test]
async fn refresh_is_refused_when_authentication_is_disabled() {
    // There is no token to refresh, and answering with one would suggest the
    // node cares about it.
    let server = Server::start_with(true).await;
    let res = server.post("/v1/auth/refresh", None, json!({})).await;
    assert_eq!(res.status, 400, "{:?}", res.body);
}

#[tokio::test]
async fn a_deleted_users_token_stops_working_at_once() {
    // The debt this closes: a deleted account kept working until its token
    // expired, up to an hour later.
    let server = Server::start().await;
    let root = server.root().await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({"user":"ada","password":"ada-password",
                   "grants":[{"db":"shop","collection":"*","actions":["read"]}]}),
        )
        .await;
    let ada = server.login("ada", "ada-password").await;
    assert_eq!(server.get("/v1/databases", Some(&ada)).await.status, 200);

    assert_eq!(server.delete("/v1/users/ada", Some(&root)).await.status, 200);

    let res = server.get("/v1/databases", Some(&ada)).await;
    assert_eq!(res.status, 401, "a deleted account must not keep working: {:?}", res.body);
}

#[tokio::test]
async fn changing_a_password_ends_the_sessions_the_old_one_opened() {
    let server = Server::start().await;
    let root = server.root().await;
    server.post("/v1/users", Some(&root), json!({"user":"ada","password":"ada-password"})).await;
    let ada = server.login("ada", "ada-password").await;
    assert_eq!(server.get("/v1/databases", Some(&ada)).await.status, 200);

    server.post("/v1/users/ada/password", Some(&root), json!({"password":"a-new-password"})).await;

    assert_eq!(
        server.get("/v1/databases", Some(&ada)).await.status,
        401,
        "the token issued under the old password must be refused"
    );
    // ...and the account itself still works.
    let fresh = server.login("ada", "a-new-password").await;
    assert_eq!(server.get("/v1/databases", Some(&fresh)).await.status, 200);
}

#[tokio::test]
async fn disabling_a_user_ends_its_sessions_and_refuses_new_logins() {
    let server = Server::start().await;
    let root = server.root().await;
    server.post("/v1/users", Some(&root), json!({"user":"ada","password":"ada-password"})).await;
    let ada = server.login("ada", "ada-password").await;
    assert_eq!(server.get("/v1/databases", Some(&ada)).await.status, 200);

    server.post("/v1/users/ada/disabled", Some(&root), json!({"disabled":true})).await;

    assert_eq!(
        server.get("/v1/databases", Some(&ada)).await.status,
        401,
        "disabling must end the sessions the account holds"
    );
    let res =
        server.post("/v1/auth/login", None, json!({"user":"ada","password":"ada-password"})).await;
    assert_eq!(
        res.status, 401,
        "a disabled account must not authenticate even with the right password"
    );

    // The reversible form of deletion: re-enable and the same credentials
    // work — but the sessions disabled away stay gone.
    server.post("/v1/users/ada/disabled", Some(&root), json!({"disabled":false})).await;
    let fresh = server.login("ada", "ada-password").await;
    assert_eq!(server.get("/v1/databases", Some(&fresh)).await.status, 200);
}

#[tokio::test]
async fn disabling_is_guarded_like_deletion() {
    let server = Server::start().await;
    let root = server.root().await;

    // Disabling yourself is a lockout with no undo from outside.
    let res = server.post("/v1/users/root/disabled", Some(&root), json!({"disabled":true})).await;
    assert_eq!(res.status, 409);

    // A second administrator makes cross-admin disables possible...
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({"user":"ada","password":"ada-password",
                   "grants":[{"db":"*","collection":"*","actions":["admin"]}]}),
        )
        .await;
    let ada = server.login("ada", "ada-password").await;
    assert_eq!(
        server.post("/v1/users/root/disabled", Some(&ada), json!({"disabled":true})).await.status,
        200,
        "one admin may disable another while remaining enabled"
    );
    let res = server
        .post("/v1/auth/login", None, json!({"user":"root","password":"root-password"}))
        .await;
    assert_eq!(res.status, 401, "disabled root must not log in");

    // ...and an unknown name is a 404, not a silent success.
    let res = server.post("/v1/users/ghost/disabled", Some(&ada), json!({"disabled":true})).await;
    assert_eq!(res.status, 404);
}

#[tokio::test]
async fn the_system_database_never_matches_a_wildcard() {
    // ADR-079. Before this, {"db":"*"} carried any caller into __kimmy —
    // whose __users holds password hashes — because wildcards matched it like
    // any other database. The cluster owner's own federated role surfaced it
    // in `kimmy databases`, which is what got the question asked.
    let server = Server::start().await;
    let root = server.root().await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({"user":"ada","password":"ada-password",
                   "grants":[{"db":"*","collection":"*","actions":["read","write"]}]}),
        )
        .await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({"user":"sys","password":"sys-password",
                   "grants":[{"db":"__kimmy","collection":"__users","actions":["read"]}]}),
        )
        .await;
    let ada = server.login("ada", "ada-password").await;
    let sys = server.login("sys", "sys-password").await;

    // The wildcard holder: system database invisible everywhere.
    let dbs = server.get("/v1/databases", Some(&ada)).await;
    assert_eq!(dbs.status, 200);
    let names = dbs.body["databases"].as_array().unwrap();
    assert!(
        !names.iter().any(|n| n == "__kimmy"),
        "listings filter through the same check as access: {names:?}"
    );

    // Collection listings filter rather than forbid (the same hidden-not-
    // forbidden rule as everywhere else): 200, and nothing in it.
    let res = server.get("/v1/db/__kimmy/collections", Some(&ada)).await;
    assert_eq!(res.status, 200);
    let listed = res.body["collections"].as_array().map(|c| c.len()).unwrap_or(0);
    assert_eq!(listed, 0, "no system collection may surface: {:?}", res.body);

    // Direct reads are refused outright: find and count on __users.
    let res = server.post("/v1/db/__kimmy/coll/__users/find", Some(&ada), json!({})).await;
    assert_eq!(res.status, 403);
    let res = server.post("/v1/db/__kimmy/coll/__users/count", Some(&ada), json!({})).await;
    assert_eq!(res.status, 403);

    // An exact grant naming the system database is honored as written —
    // including its collection pattern: __users yes, __roles no.
    let res = server.post("/v1/db/__kimmy/coll/__users/find", Some(&sys), json!({})).await;
    assert_eq!(res.status, 200);
    let res = server.post("/v1/db/__kimmy/coll/__roles/find", Some(&sys), json!({})).await;
    assert_eq!(res.status, 403, "the collection pattern still applies");

    // Administration reaches through every boundary, unchanged: root's only
    // grant is admin over the wildcard.
    let root_dbs = server.get("/v1/databases", Some(&root)).await;
    assert!(
        root_dbs.body["databases"].as_array().unwrap().iter().any(|n| n == "__kimmy"),
        "admin keeps its reach"
    );
}

#[tokio::test]
async fn narrowing_a_grant_takes_effect_without_waiting_for_the_token_to_expire() {
    // Grants ride inside the token, so before ADR-052 a *revoked* permission
    // kept working for the rest of the token's hour. This is the property that
    // was actually dangerous, rather than merely untidy.
    let server = Server::start().await;
    let root = server.root().await;
    server.post("/v1/db/shop/collections", Some(&root), json!({"name":"orders"})).await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({"user":"ada","password":"ada-password",
                   "grants":[{"db":"shop","collection":"*","actions":["read","write"]}]}),
        )
        .await;
    let ada = server.login("ada", "ada-password").await;
    assert_eq!(
        server.post("/v1/db/shop/coll/orders/docs", Some(&ada), json!({"_id":1})).await.status,
        200
    );

    // Take the write away.
    server
        .post(
            "/v1/users/ada/grants",
            Some(&root),
            json!({"grants":[{"db":"shop","collection":"*","actions":["read"]}]}),
        )
        .await;

    let res = server.post("/v1/db/shop/coll/orders/docs", Some(&ada), json!({"_id":2})).await;
    assert_eq!(
        res.status, 401,
        "the token carrying the old grants must be refused, not honoured: {:?}",
        res.body
    );
    // Logging in again picks up the narrowed grants, which now forbid the write.
    let ada = server.login("ada", "ada-password").await;
    assert_eq!(
        server.post("/v1/db/shop/coll/orders/docs", Some(&ada), json!({"_id":3})).await.status,
        403
    );
}

#[tokio::test]
async fn a_revoked_token_does_not_say_why() {
    // Whoever holds a stale token should not learn whether the account was
    // deleted, disabled, or merely logged out — that reports on an account to
    // someone who no longer has access to it.
    let server = Server::start().await;
    let root = server.root().await;
    server.post("/v1/users", Some(&root), json!({"user":"ada","password":"ada-password"})).await;
    let ada = server.login("ada", "ada-password").await;
    server.post("/v1/users/ada/password", Some(&root), json!({"password":"a-new-password"})).await;
    let bumped = server.get("/v1/databases", Some(&ada)).await;

    server.delete("/v1/users/ada", Some(&root)).await;
    let deleted = server.get("/v1/databases", Some(&ada)).await;

    assert_eq!(bumped.status, deleted.status);
    assert_eq!(bumped.body["error"], deleted.body["error"]);
    assert_eq!(bumped.body["message"], deleted.body["message"]);
}

#[tokio::test]
async fn short_passwords_are_refused() {
    let server = Server::start().await;
    let token = server.root().await;
    let res = server.post("/v1/users", Some(&token), json!({"user":"weak","password":"abc"})).await;
    assert_eq!(res.status, 400);
}

#[tokio::test]
async fn insecure_no_auth_grants_full_access_without_a_token() {
    let server = Server::start_with(true).await;
    assert_eq!(server.get("/v1/databases", None).await.status, 200);

    let res = server.get("/v1/auth/whoami", None).await;
    // Flagged, so audit output can tell this apart from a real root login.
    assert_eq!(res.body["authenticated"], false);
}

#[tokio::test]
async fn a_bad_filter_is_the_callers_fault() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"c"})).await;

    let res = server
        .post("/v1/db/shop/coll/c/find", Some(&token), json!({"filter": {"a": {"$nope": 1}}}))
        .await;
    assert_eq!(res.status, 400);
}

#[tokio::test]
async fn metrics_report_counts_without_naming_collections() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"secret_project"})).await;

    // Served as text/plain, so it parses as Null through the JSON client; fetch
    // it as raw text instead.
    let res = server.client.request("GET", &format!("{}/metrics", server.base), None, None).await;
    assert_eq!(res.status, 200);
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

/// Ids returned by a query, sorted so results are comparable across access
/// paths (without an explicit sort, order is unspecified).
async fn ids(server: &Server, token: &str, coll: &str, body: Value) -> Vec<i64> {
    let res = server.post(&format!("/v1/db/shop/coll/{coll}/find"), Some(token), body).await;
    let mut out: Vec<i64> = res.body["documents"]
        .as_array()
        .expect("documents")
        .iter()
        .map(|d| d["_id"].as_i64().expect("_id"))
        .collect();
    out.sort_unstable();
    out
}

/// Seed two identical collections; only `indexed` gets indexes.
async fn seed_pair(server: &Server, token: &str) {
    for coll in ["indexed", "control"] {
        server.post("/v1/db/shop/collections", Some(token), json!({ "name": coll })).await;
        for i in 1..=60i64 {
            server
                .post(
                    &format!("/v1/db/shop/coll/{coll}/docs"),
                    Some(token),
                    json!({ "_id": i, "qty": i % 7, "item": format!("w{}", i % 4) }),
                )
                .await;
        }
    }
}

#[tokio::test]
async fn an_index_never_changes_which_documents_a_query_returns() {
    let server = Server::start().await;
    let token = server.root().await;
    seed_pair(&server, &token).await;

    for fields in [
        json!([{ "path": "qty" }]),
        json!([{ "path": "item" }, { "path": "qty" }]),
        json!([{ "path": "qty", "descending": true }]),
    ] {
        let res = server
            .post("/v1/db/shop/coll/indexed/indexes", Some(&token), json!({ "fields": fields }))
            .await;
        assert_eq!(res.status, 200, "{:?}", res.body);
    }

    for query in [
        json!({ "filter": { "qty": 3 }, "limit": 500 }),
        json!({ "filter": { "qty": { "$gte": 4 } }, "limit": 500 }),
        json!({ "filter": { "qty": { "$gte": 2, "$lt": 5 } }, "limit": 500 }),
        json!({ "filter": { "item": "w1", "qty": 3 }, "limit": 500 }),
        json!({ "filter": { "qty": 3, "$or": [ { "item": "w1" }, { "item": "w2" } ] }, "limit": 500 }),
        json!({ "filter": { "qty": { "$ne": 3 } }, "limit": 500 }),
        json!({ "filter": {}, "limit": 500 }),
    ] {
        let indexed = ids(&server, &token, "indexed", query.clone()).await;
        let scanned = ids(&server, &token, "control", query.clone()).await;
        assert_eq!(indexed, scanned, "index and scan disagree for {query}");
    }
}

/// `find` on `_id` reads the document rather than scanning for it.
///
/// The register carried this as "correct but slow": the planner consults
/// *secondary* indexes only, so `{_id: 5}` scanned the whole collection while
/// `GET /docs/5` answered the same question with one read. Any client filtering
/// on `_id` through `find` paid it — including the MCP `find` tool, where an
/// agent has no reason to know a second route is the fast one.
#[tokio::test]
async fn a_find_on_id_examines_one_document_not_the_collection() {
    let server = Server::start().await;
    let token = server.root().await;
    seed_pair(&server, &token).await;

    let res = server
        .post(
            "/v1/db/shop/coll/control/find",
            Some(&token),
            json!({ "filter": { "_id": 5 }, "explain": true }),
        )
        .await;

    assert_eq!(res.body["explain"]["strategy"], "idLookup", "{:?}", res.body);
    assert_eq!(
        res.body["explain"]["documentsExamined"], 1,
        "one read, not a scan of sixty: {:?}",
        res.body
    );
    assert_eq!(res.body["explain"]["index"], Value::Null, "no index was consulted");
    assert_eq!(res.body["documents"].as_array().unwrap().len(), 1);
    assert_eq!(res.body["documents"][0]["_id"], 5);

    // And it is the same document the point route returns, which is the claim
    // that matters: a faster path must not be a different answer.
    let point = server.get("/v1/db/shop/coll/control/docs/5", Some(&token)).await;
    assert_eq!(res.body["documents"][0], point.body);
}

/// An `$in` on `_id` becomes one probe per value, still without a scan.
#[tokio::test]
async fn an_in_on_id_probes_each_value() {
    let server = Server::start().await;
    let token = server.root().await;
    seed_pair(&server, &token).await;

    let res = server
        .post(
            "/v1/db/shop/coll/control/find",
            Some(&token),
            // 5 twice, and one that was never stored: a repeat is one probe,
            // and a miss is an ordinary absence rather than an error.
            json!({ "filter": { "_id": { "$in": [5, 9, 5, 9999] } }, "explain": true }),
        )
        .await;

    assert_eq!(res.body["explain"]["strategy"], "idLookup", "{:?}", res.body);
    assert_eq!(res.body["explain"]["documentsExamined"], 2, "{:?}", res.body);
    let ids: Vec<i64> = res.body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![5, 9]);
}

/// The fast path narrows; the filter still decides.
///
/// A candidate is re-checked against the whole filter exactly as an index
/// candidate is, so a second predicate that excludes the document must still
/// exclude it.
#[tokio::test]
async fn an_id_lookup_still_applies_the_rest_of_the_filter() {
    let server = Server::start().await;
    let token = server.root().await;
    seed_pair(&server, &token).await;

    let five = server.get("/v1/db/shop/coll/control/docs/5", Some(&token)).await;
    let qty = five.body["qty"].clone();

    let matching = server
        .post(
            "/v1/db/shop/coll/control/find",
            Some(&token),
            json!({ "filter": { "_id": 5, "qty": qty }, "explain": true }),
        )
        .await;
    assert_eq!(matching.body["explain"]["strategy"], "idLookup");
    assert_eq!(matching.body["documents"].as_array().unwrap().len(), 1);

    let excluded = server
        .post(
            "/v1/db/shop/coll/control/find",
            Some(&token),
            json!({ "filter": { "_id": 5, "qty": 100000 }, "explain": true }),
        )
        .await;
    assert_eq!(excluded.body["explain"]["strategy"], "idLookup", "the path is still chosen");
    assert_eq!(
        excluded.body["documents"].as_array().unwrap().len(),
        0,
        "narrowing is not deciding: the filter still excludes it"
    );
}

/// `update`, `delete` and `count` inherit the fast path, because all three go
/// through `collect_matching`.
///
/// That sharing is deliberate — M9 found `update` and `delete` scanning while
/// `find` used the planner — and it means a targeted write on `_id` stops
/// costing a scan too. Asserted rather than assumed, since "they share a
/// function" is exactly the kind of claim that quietly stops being true.
#[tokio::test]
async fn a_targeted_write_on_id_also_takes_the_fast_path() {
    let server = Server::start().await;
    let token = server.root().await;
    seed_pair(&server, &token).await;

    let counted = server
        .post(
            "/v1/db/shop/coll/control/count",
            Some(&token),
            json!({ "filter": { "_id": 5 }, "explain": true }),
        )
        .await;
    assert_eq!(counted.body["count"], 1);
    assert_eq!(counted.body["explain"]["strategy"], "idLookup", "{:?}", counted.body);

    let updated = server
        .post(
            "/v1/db/shop/coll/control/update",
            Some(&token),
            json!({
                "filter": { "_id": 5 },
                "update": { "$set": { "item": "changed" } },
                "explain": true,
            }),
        )
        .await;
    assert_eq!(updated.body["matched"], 1, "{:?}", updated.body);
    assert_eq!(updated.body["explain"]["strategy"], "idLookup", "{:?}", updated.body);
    assert_eq!(updated.body["explain"]["documentsExamined"], 1, "{:?}", updated.body);

    let after = server.get("/v1/db/shop/coll/control/docs/5", Some(&token)).await;
    assert_eq!(after.body["item"], "changed", "the write landed on the right document");

    let deleted = server
        .post(
            "/v1/db/shop/coll/control/delete",
            Some(&token),
            json!({ "filter": { "_id": 5 }, "explain": true }),
        )
        .await;
    assert_eq!(deleted.body["deleted"], 1, "{:?}", deleted.body);
    assert_eq!(deleted.body["explain"]["strategy"], "idLookup", "{:?}", deleted.body);

    let gone = server.get("/v1/db/shop/coll/control/docs/5", Some(&token)).await;
    assert_eq!(gone.status, 404, "and only that document: {:?}", gone.body);
    let remaining =
        server.post("/v1/db/shop/coll/control/count", Some(&token), json!({ "filter": {} })).await;
    assert_eq!(remaining.body["count"], 59);
}

/// A `_id` the fast path cannot encode still finds its document, by scanning.
///
/// This is the safety case, and it is not hypothetical. Filter equality is
/// cross-type *within* the numeric group, so `{_id: 5.0}` really does match a
/// document stored under `Int64(5)`. `DocId` refuses a `Double`, so probing
/// only what normalizes would lose the document silently — the fast path has
/// to stand aside instead.
#[tokio::test]
async fn a_double_id_filter_still_finds_the_integer_keyed_document() {
    let server = Server::start().await;
    let token = server.root().await;
    seed_pair(&server, &token).await;

    let res = server
        .post(
            "/v1/db/shop/coll/control/find",
            Some(&token),
            json!({ "filter": { "_id": 5.0 }, "explain": true }),
        )
        .await;

    assert_eq!(
        res.body["explain"]["strategy"], "collectionScan",
        "a double is not a document id, so the fast path must stand aside: {:?}",
        res.body
    );
    assert_eq!(
        res.body["documents"].as_array().unwrap().len(),
        1,
        "and the document is still found: {:?}",
        res.body
    );
    assert_eq!(res.body["documents"][0]["_id"], 5);
}

/// An `_id` inside an `$or` is not a lookup, and every branch still answers.
#[tokio::test]
async fn an_id_inside_an_or_returns_both_branches() {
    let server = Server::start().await;
    let token = server.root().await;
    seed_pair(&server, &token).await;

    let res = server
        .post(
            "/v1/db/shop/coll/control/find",
            Some(&token),
            json!({
                "filter": { "$or": [ { "_id": 5 }, { "_id": 9 } ] },
                "explain": true,
                "limit": 500,
            }),
        )
        .await;

    assert_eq!(
        res.body["explain"]["strategy"], "collectionScan",
        "a disjunction constrains nothing that must universally hold: {:?}",
        res.body
    );
    let ids: Vec<i64> = res.body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![5, 9], "taking _id out of an $or would drop a branch");
}

#[tokio::test]
async fn explain_reports_which_access_path_was_used() {
    let server = Server::start().await;
    let token = server.root().await;
    seed_pair(&server, &token).await;

    let probe = json!({ "filter": { "qty": 3 }, "explain": true, "limit": 500 });

    let before = server.post("/v1/db/shop/coll/indexed/find", Some(&token), probe.clone()).await;
    assert_eq!(before.body["explain"]["strategy"], "collectionScan");
    let examined_before = before.body["explain"]["documentsExamined"].as_u64().unwrap();

    server
        .post(
            "/v1/db/shop/coll/indexed/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "qty" }] }),
        )
        .await;

    let after = server.post("/v1/db/shop/coll/indexed/find", Some(&token), probe).await;
    assert_eq!(after.body["explain"]["strategy"], "index");
    assert_eq!(after.body["explain"]["index"], "qty_1");
    // The whole point: the index must actually reduce the work done.
    let examined_after = after.body["explain"]["documentsExamined"].as_u64().unwrap();
    assert!(
        examined_after < examined_before,
        "index examined {examined_after}, scan examined {examined_before}"
    );
    // ...without changing the answer.
    assert_eq!(
        after.body["explain"]["documentsMatched"],
        before.body["explain"]["documentsMatched"]
    );
}

#[tokio::test]
async fn a_unique_index_rejects_duplicates_over_http() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({ "name": "users" })).await;
    server
        .post(
            "/v1/db/shop/coll/users/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "email" }], "unique": true }),
        )
        .await;

    let first = server
        .post("/v1/db/shop/coll/users/docs", Some(&token), json!({ "email": "a@x.com" }))
        .await;
    assert_eq!(first.status, 200);

    let second = server
        .post("/v1/db/shop/coll/users/docs", Some(&token), json!({ "email": "a@x.com" }))
        .await;
    assert_eq!(second.status, 409);
    assert_eq!(second.body["error"], "unique_violation");
    // The message must name the index, not be mangled into the _id wording.
    assert!(
        second.body["message"].as_str().unwrap().contains("email_1"),
        "unhelpful message: {}",
        second.body["message"]
    );
}

#[tokio::test]
async fn coordinated_enforcement_is_not_implemented_rather_than_a_bad_request() {
    // 501 says "this will exist"; 400 would wrongly blame the caller.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({ "name": "c" })).await;

    let res = server
        .post(
            "/v1/db/shop/coll/c/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "e" }], "unique": true, "enforcement": "coordinated" }),
        )
        .await;
    assert_eq!(res.status, 501);
    assert_eq!(res.body["error"], "not_implemented");
}

#[tokio::test]
async fn indexes_can_be_listed_and_dropped() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({ "name": "c" })).await;
    server
        .post(
            "/v1/db/shop/coll/c/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "a" }], "name": "mine" }),
        )
        .await;

    let listed = server.get("/v1/db/shop/coll/c/indexes", Some(&token)).await;
    assert_eq!(listed.body["indexes"][0]["name"], "mine");
    assert_eq!(listed.body["indexes"][0]["enforcement"], "local");

    let dropped = server.delete("/v1/db/shop/coll/c/indexes/mine", Some(&token)).await;
    assert_eq!(dropped.body["dropped"], true);
    assert!(
        server.get("/v1/db/shop/coll/c/indexes", Some(&token)).await.body["indexes"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn listing_collections_of_a_missing_database_is_a_404() {
    let server = Server::start().await;
    let root = server.root().await;
    server.post("/v1/db/shop/collections", Some(&root), json!({ "name": "c" })).await;

    assert_eq!(server.get("/v1/db/shop/collections", Some(&root)).await.status, 200);
    let missing = server.get("/v1/db/shopp/collections", Some(&root)).await;
    assert_eq!(missing.status, 404, "{:?}", missing.body);

    // Exists, but nothing in it is visible to this caller: still a list, and
    // still empty — zero grants is not a refusal (ADR-066).
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({
                "user": "elsewhere", "password": "elsewhere-password",
                "grants": [{ "db": "other", "collection": "*", "actions": ["read"] }]
            }),
        )
        .await;
    let elsewhere = server.login("elsewhere", "elsewhere-password").await;
    let hidden = server.get("/v1/db/shop/collections", Some(&elsewhere)).await;
    assert_eq!(hidden.status, 200, "{:?}", hidden.body);
    assert_eq!(hidden.body["collections"], json!([]));
}

#[tokio::test]
async fn filtered_writes_report_a_stamp_only_when_one_document_was_written() {
    // ADR-084 refuses `if_stamp` with `multi` because one version cannot name
    // several documents; the response is honest in the same way.
    let server = Server::start().await;
    let root = server.root().await;
    server.post("/v1/db/shop/collections", Some(&root), json!({ "name": "c" })).await;
    let bulk = server
        .post(
            "/v1/db/shop/coll/c/bulk",
            Some(&root),
            json!([{ "_id": 1, "n": 1 }, { "_id": 2, "n": 2 }]),
        )
        .await;
    assert_eq!(bulk.status, 200, "{:?}", bulk.body);
    let stamps = bulk.body["stamps"].as_array().expect("stamps").clone();
    assert_eq!(stamps.len(), 2);

    let one = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&root),
            json!({ "filter": { "_id": 1 }, "update": { "$set": { "n": 10 } } }),
        )
        .await;
    assert_eq!(one.status, 200, "{:?}", one.body);
    let stamp = one.body["stamp"].as_str().expect("a single update reports its stamp");

    // And the stamp is the one a conditional write wants.
    let conditional = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&root),
            json!({ "filter": { "_id": 1 }, "update": { "$set": { "n": 11 } }, "if_stamp": stamp }),
        )
        .await;
    assert_eq!(conditional.status, 200, "{:?}", conditional.body);
    assert_ne!(conditional.body["stamp"], one.body["stamp"]);

    let many = server
        .post(
            "/v1/db/shop/coll/c/update",
            Some(&root),
            json!({ "filter": {}, "update": { "$set": { "seen": true } }, "multi": true }),
        )
        .await;
    assert_eq!(many.status, 200, "{:?}", many.body);
    assert!(
        many.body.get("stamp").is_none(),
        "a multi write has no single version: {:?}",
        many.body
    );

    let gone = server
        .post("/v1/db/shop/coll/c/delete", Some(&root), json!({ "filter": { "_id": 2 } }))
        .await;
    assert_eq!(gone.status, 200, "{:?}", gone.body);
    assert!(gone.body["stamp"].is_string(), "{:?}", gone.body);
}

#[tokio::test]
async fn ddl_shapes_collections_without_administering_the_server() {
    // ADR-090: `ddl` is the part of `admin` that is about the data — create
    // and drop collections, manage indexes — and none of the part that is
    // about the server. A principal holding exactly `ddl` can do the former
    // and nothing else: it cannot write into what it created, cannot manage
    // users, and cannot reach the system database.
    let server = Server::start().await;
    let root = server.root().await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({
                "user": "shaper", "password": "shaper-password",
                "grants": [{ "db": "shop", "collection": "*", "actions": ["ddl"] }]
            }),
        )
        .await;
    let shaper = server.login("shaper", "shaper-password").await;

    let created =
        server.post("/v1/db/shop/collections", Some(&shaper), json!({ "name": "c" })).await;
    assert_eq!(created.status, 200, "ddl creates collections: {:?}", created.body);
    let indexed = server
        .post("/v1/db/shop/coll/c/indexes", Some(&shaper), json!({ "fields": [{ "path": "a" }] }))
        .await;
    assert_eq!(indexed.status, 200, "ddl creates indexes: {:?}", indexed.body);
    assert_eq!(server.delete("/v1/db/shop/coll/c/indexes/a_1", Some(&shaper)).await.status, 200);
    assert_eq!(server.delete("/v1/db/shop/coll/c", Some(&shaper)).await.status, 200);

    // Not a bundle: shaping a collection says nothing about its contents.
    server.post("/v1/db/shop/collections", Some(&shaper), json!({ "name": "c" })).await;
    let inserted = server.post("/v1/db/shop/coll/c/docs", Some(&shaper), json!({ "x": 1 })).await;
    assert_eq!(inserted.status, 403, "ddl must not imply write: {:?}", inserted.body);
    assert_eq!(server.get("/v1/db/shop/coll/c/docs", Some(&shaper)).await.status, 403);

    // And not `admin`: the server, and the database that holds its users, stay
    // closed.
    assert_eq!(server.get("/v1/users", Some(&shaper)).await.status, 403);
    let elsewhere =
        server.post("/v1/db/other/collections", Some(&shaper), json!({ "name": "c" })).await;
    assert_eq!(elsewhere.status, 403, "the grant is scoped to its database");
}

#[tokio::test]
async fn managing_indexes_requires_ddl() {
    let server = Server::start().await;
    let root = server.root().await;
    server.post("/v1/db/shop/collections", Some(&root), json!({ "name": "c" })).await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({
                "user": "reader", "password": "reader-password",
                "grants": [{ "db": "shop", "collection": "*", "actions": ["read"] }]
            }),
        )
        .await;
    let reader = server.login("reader", "reader-password").await;

    // Reading the index list is a read; creating and dropping are not.
    assert_eq!(server.get("/v1/db/shop/coll/c/indexes", Some(&reader)).await.status, 200);
    let created = server
        .post("/v1/db/shop/coll/c/indexes", Some(&reader), json!({ "fields": [{ "path": "a" }] }))
        .await;
    assert_eq!(created.status, 403);
    assert_eq!(server.delete("/v1/db/shop/coll/c/indexes/a_1", Some(&reader)).await.status, 403);
}

// ---------------------------------------------------------------------------
// Client-supplied vectors
// ---------------------------------------------------------------------------

/// Enable `byo` embeddings on a fresh collection and return a root token.
async fn byo_collection(server: &Server) -> String {
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({ "name": "docs" })).await;
    let res = server
        .post(
            "/v1/db/shop/coll/docs/vector",
            Some(&token),
            json!({
                "fields": ["text"],
                "provider": { "kind": "byo" },
                "dim": 3,
            }),
        )
        .await;
    assert_eq!(res.status, 200, "configuring vectors failed: {:?}", res.body);
    token
}

#[tokio::test]
async fn configuring_a_provider_with_a_node_secret_or_a_private_endpoint_is_refused() {
    // ADR-115, over the wire. A `ddl` holder naming the node's own signing
    // secret as the provider's key variable, or pointing the provider at the
    // node's own network, is answered 400 with the name and the setting —
    // and nothing is stored, so the collection is still unconfigured after.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({ "name": "docs" })).await;

    let stolen = json!({
        "fields": ["text"], "dim": 3,
        "provider": {
            "kind": "open_ai", "model": "m",
            "endpoint": "https://93.184.216.34",
            "api_key_env": "KIMMY_JWT_SECRET",
        },
    });
    let res = server.post("/v1/db/shop/coll/docs/vector", Some(&token), stolen).await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    let message = res.body["message"].as_str().unwrap_or_default().to_string();
    assert!(message.contains("KIMMY_JWT_SECRET"), "{message}");
    assert!(message.contains("api_key_env"), "{message}");

    let inward = json!({
        "fields": ["text"], "dim": 3,
        "provider": { "kind": "ollama", "model": "m", "endpoint": "http://169.254.169.254" },
    });
    let res = server.post("/v1/db/shop/coll/docs/vector", Some(&token), inward).await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    let message = res.body["message"].as_str().unwrap_or_default().to_string();
    assert!(message.contains("169.254.169.254"), "{message}");
    assert!(message.contains("vector.provider.allowed_hosts"), "{message}");

    let unknown = json!({
        "fields": ["text"], "dim": 3,
        "provider": { "kind": "profile", "name": "nope" },
    });
    let res = server.post("/v1/db/shop/coll/docs/vector", Some(&token), unknown).await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    let message = res.body["message"].as_str().unwrap_or_default().to_string();
    assert!(message.contains("vector.providers.nope"), "{message}");

    // Nothing was stored by any of the refusals.
    let res = server.get("/v1/db/shop/coll/docs/vector", Some(&token)).await;
    assert_eq!(res.status, 200);
    assert!(res.body["vector"].is_null(), "{:?}", res.body);

    // And the shape that has always worked still does: a hosted default
    // endpoint with its documented variable.
    let fine = json!({
        "fields": ["text"], "dim": 3,
        "provider": { "kind": "open_ai", "model": "m" },
    });
    let res = server.post("/v1/db/shop/coll/docs/vector", Some(&token), fine).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
}

#[tokio::test]
async fn searching_a_collection_with_no_vectors_says_so() {
    // An empty result set is indistinguishable from "nothing matched", which is
    // how `byo` being the default produced a collection that silently could
    // never return anything.
    let server = Server::start().await;
    let token = byo_collection(&server).await;

    let res = server
        .post(
            "/v1/db/shop/coll/docs/vector_search",
            Some(&token),
            json!({ "vector": [1.0, 0.0, 0.0] }),
        )
        .await;

    assert_eq!(res.status, 409, "expected a refusal, got {:?}", res.body);
    assert_eq!(res.body["error"], "no_vectors");
    let message = res.body["message"].as_str().unwrap_or_default();
    assert!(message.contains("/vectors"), "the message must say how to fix it: {message}");
}

#[tokio::test]
async fn a_deleted_document_does_not_surface_from_search() {
    // The shadow collection is cleaned up by the embedding worker *after* the
    // delete commits, from the oplog. Between the two — and for as long as it
    // takes, if the worker is behind or disabled, as it is in this harness —
    // the chunks are still there to be scored. ADR-022 promised that a deleted
    // document cannot surface; this is the check that keeps it (ADR-091).
    let server = Server::start().await;
    let token = byo_collection(&server).await;
    for (id, vector) in [("a", [1.0, 0.0, 0.0]), ("b", [0.0, 1.0, 0.0])] {
        server
            .post("/v1/db/shop/coll/docs/docs", Some(&token), json!({ "_id": id, "text": id }))
            .await;
        let stored = server
            .put(
                &format!("/v1/db/shop/coll/docs/docs/{id}/vectors"),
                Some(&token),
                json!([{ "chunk": 0, "vector": vector, "text": id }]),
            )
            .await;
        assert_eq!(stored.status, 200, "{:?}", stored.body);
    }

    let deleted = server.delete("/v1/db/shop/coll/docs/docs/a", Some(&token)).await;
    assert_eq!(deleted.status, 200, "{:?}", deleted.body);

    // The nearest vector to the query is the deleted document's.
    let found = server
        .post(
            "/v1/db/shop/coll/docs/vector_search",
            Some(&token),
            json!({ "vector": [1.0, 0.0, 0.0], "k": 2 }),
        )
        .await;
    assert_eq!(found.status, 200, "{:?}", found.body);
    let ids: Vec<&str> = found.body["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["b"], "the deleted document must not surface: {:?}", found.body);
    assert_eq!(found.body["count"], 1, "and the count agrees with the list");

    // Hybrid ranks by keyword too, and "a" is the keyword.
    let hybrid = server
        .post(
            "/v1/db/shop/coll/docs/hybrid_search",
            Some(&token),
            json!({ "query": "a", "vector": [1.0, 0.0, 0.0], "k": 2 }),
        )
        .await;
    assert_eq!(hybrid.status, 200, "{:?}", hybrid.body);
    let ids: Vec<&str> = hybrid.body["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["b"], "nor from hybrid search: {:?}", hybrid.body);
}

/// Three documents that separate the two halves of a hybrid search for the
/// query `"red blue"` with vector `[1, 0, 0]`:
///
/// - `x` is the nearest vector and shares no term with the query, so it is
///   dense-only evidence;
/// - `z` is second nearest and shares both terms;
/// - `y` is farthest and shares one term — the candidate `min_overlap` gates.
async fn fusion_fixture(server: &Server) -> String {
    let token = byo_collection(server).await;
    for (id, vector, text) in [
        ("x", [1.0, 0.0, 0.0], "green paint"),
        ("y", [0.0, 1.0, 0.0], "red apple"),
        ("z", [0.6, 0.0, 0.8], "red blue"),
    ] {
        server
            .post("/v1/db/shop/coll/docs/docs", Some(&token), json!({ "_id": id, "text": text }))
            .await;
        let stored = server
            .put(
                &format!("/v1/db/shop/coll/docs/docs/{id}/vectors"),
                Some(&token),
                json!([{ "chunk": 0, "vector": vector, "text": text }]),
            )
            .await;
        assert_eq!(stored.status, 200, "{:?}", stored.body);
    }
    token
}

/// `(_id, score)` in result order.
fn ranked(body: &Value) -> Vec<(String, f64)> {
    body["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| (m["_id"].as_str().unwrap().to_string(), m["score"].as_f64().unwrap()))
        .collect()
}

#[tokio::test]
async fn hybrid_fusion_controls_default_to_the_previous_ranking() {
    // ADR-094: the fields exist, and a request that sends their defaults gets
    // byte-for-byte what a request that omits them gets — same order, same
    // scores, same response shape.
    let server = Server::start().await;
    let token = fusion_fixture(&server).await;
    let path = "/v1/db/shop/coll/docs/hybrid_search";
    let base = json!({ "query": "red blue", "vector": [1.0, 0.0, 0.0], "k": 5 });

    let implicit = server.post(path, Some(&token), base.clone()).await;
    assert_eq!(implicit.status, 200, "{:?}", implicit.body);

    let mut explicit_body = base.clone();
    explicit_body["weights"] = json!({ "dense": 1.0, "lexical": 1.0 });
    explicit_body["min_overlap"] = json!(1);
    let explicit = server.post(path, Some(&token), explicit_body).await;
    assert_eq!(explicit.status, 200, "{:?}", explicit.body);

    assert_eq!(implicit.body, explicit.body, "the defaults must be the old behaviour exactly");
    let ranking = ranked(&implicit.body);
    let order: Vec<&str> = ranking.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(order, vec!["z", "y", "x"], "{:?}", implicit.body);

    // The response shape is the one every client already parses. The set of
    // keys is the shape; their order is not part of it (`docs/compatibility.md`
    // tells clients not to depend on envelope field order), and the alphabetical
    // order this once asserted was the sorted map the JSON layer used before
    // ADR-120, not a choice.
    assert_eq!(explicit.body["count"], 3);
    for m in explicit.body["matches"].as_array().unwrap() {
        let mut keys: Vec<&str> = m.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["_id", "chunk", "score", "text"], "response shape changed: {m}");
    }
}

#[tokio::test]
async fn min_overlap_removes_lexical_evidence_but_never_a_dense_hit() {
    let server = Server::start().await;
    let token = fusion_fixture(&server).await;
    let path = "/v1/db/shop/coll/docs/hybrid_search";

    let loose = server
        .post(path, Some(&token), json!({ "query": "red blue", "vector": [1.0, 0.0, 0.0], "k": 5 }))
        .await;
    let strict = server
        .post(
            path,
            Some(&token),
            json!({ "query": "red blue", "vector": [1.0, 0.0, 0.0], "k": 5, "min_overlap": 2 }),
        )
        .await;
    assert_eq!(strict.status, 200, "{:?}", strict.body);

    let loose = ranked(&loose.body);
    let strict = ranked(&strict.body);
    let order: Vec<&str> = strict.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(order, vec!["z", "x", "y"], "{strict:?}");

    let score = |ranking: &[(String, f64)], id: &str| {
        ranking.iter().find(|(i, _)| i == id).map(|(_, s)| *s).unwrap()
    };
    // `y` shared one term: gated out of the lexical half, it keeps its dense
    // contribution — it is still in the result — and loses only the lexical one.
    assert!(score(&strict, "y") < score(&loose, "y"), "y must lose its lexical share");
    assert_eq!(strict.len(), 3, "gating lexical evidence must not drop a dense hit");
    // `x` never had lexical evidence, so the gate cannot touch it...
    assert_eq!(score(&strict, "x"), score(&loose, "x"));
    // ...and `z` shares both terms, so it passes the gate unchanged.
    assert_eq!(score(&strict, "z"), score(&loose, "z"));
}

#[tokio::test]
async fn weights_scale_each_half_and_zero_switches_one_off() {
    let server = Server::start().await;
    let token = fusion_fixture(&server).await;
    let path = "/v1/db/shop/coll/docs/hybrid_search";
    let search = |weights: Value| {
        let token = token.clone();
        let server = &server;
        async move {
            let res = server
                .post(
                    path,
                    Some(&token),
                    json!({
                        "query": "red blue", "vector": [1.0, 0.0, 0.0], "k": 5,
                        "weights": weights,
                    }),
                )
                .await;
            assert_eq!(res.status, 200, "{:?}", res.body);
            ranked(&res.body)
        }
    };
    let ids = |ranking: &[(String, f64)]| -> Vec<String> {
        ranking.iter().map(|(id, _)| id.clone()).collect()
    };

    // Dense only: nearest vector first, and every dense hit present.
    assert_eq!(ids(&search(json!({ "dense": 1.0, "lexical": 0.0 })).await), ["x", "z", "y"]);
    // Lexical only: `x` shares no term, so it is not in the result at all.
    assert_eq!(ids(&search(json!({ "dense": 0.0, "lexical": 1.0 })).await), ["z", "y"]);

    // The weights are applied as stated: halving both halves the score of
    // every document and moves nothing, because only the ratio orders.
    let equal = search(json!({ "dense": 1.0, "lexical": 1.0 })).await;
    let halved = search(json!({ "dense": 0.5, "lexical": 0.5 })).await;
    assert_eq!(ids(&equal), ids(&halved));
    for ((id, full), (_, half)) in equal.iter().zip(&halved) {
        assert!((half - full / 2.0).abs() < 1e-6, "{id}: {half} is not half of {full}");
    }
}

#[tokio::test]
async fn meaningless_fusion_controls_are_a_400() {
    let server = Server::start().await;
    let token = fusion_fixture(&server).await;
    let path = "/v1/db/shop/coll/docs/hybrid_search";

    for (extra, names) in [
        (json!({ "weights": { "dense": -1.0 } }), "weights.dense"),
        (
            json!({ "weights": { "dense": 0.0, "lexical": 0.0 } }),
            "weights.dense and weights.lexical",
        ),
        (json!({ "min_overlap": 0 }), "min_overlap"),
    ] {
        let mut body = json!({ "query": "red blue", "vector": [1.0, 0.0, 0.0] });
        for (k, v) in extra.as_object().unwrap() {
            body[k] = v.clone();
        }
        let res = server.post(path, Some(&token), body).await;
        assert_eq!(res.status, 400, "{extra}: {:?}", res.body);
        assert_eq!(res.body["error"], "bad_request", "{extra}: {:?}", res.body);
        let message = res.body["message"].as_str().unwrap_or_default();
        assert!(message.contains(names), "{extra}: the message must name the field: {message}");
    }

    // `vector_search` has one half and ignores the controls rather than
    // refusing them, as the specification says.
    let res = server
        .post(
            "/v1/db/shop/coll/docs/vector_search",
            Some(&token),
            json!({ "vector": [1.0, 0.0, 0.0], "weights": { "dense": 0.0, "lexical": 0.0 } }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
}

#[tokio::test]
async fn client_supplied_vectors_become_searchable() {
    let server = Server::start().await;
    let token = byo_collection(&server).await;
    server
        .post("/v1/db/shop/coll/docs/docs", Some(&token), json!({ "_id": "a", "text": "alpha" }))
        .await;

    let stored = server
        .put(
            "/v1/db/shop/coll/docs/docs/a/vectors",
            Some(&token),
            json!([{ "chunk": 0, "vector": [1.0, 0.0, 0.0], "text": "alpha" }]),
        )
        .await;
    assert_eq!(stored.status, 200, "{:?}", stored.body);
    assert_eq!(stored.body["stored"], 1);

    let found = server
        .post(
            "/v1/db/shop/coll/docs/vector_search",
            Some(&token),
            json!({ "vector": [1.0, 0.0, 0.0] }),
        )
        .await;
    assert_eq!(found.status, 200, "{:?}", found.body);
    assert_eq!(found.body["count"], 1);
    assert_eq!(found.body["matches"][0]["_id"], "a");
    assert_eq!(found.body["matches"][0]["text"], "alpha");
}

#[tokio::test]
async fn storing_vectors_replaces_the_whole_set() {
    // Replace-all, so a document that shrinks to fewer chunks cannot leave
    // orphans matching text it no longer contains.
    let server = Server::start().await;
    let token = byo_collection(&server).await;
    server.post("/v1/db/shop/coll/docs/docs", Some(&token), json!({ "_id": "a" })).await;

    server
        .put(
            "/v1/db/shop/coll/docs/docs/a/vectors",
            Some(&token),
            json!([
                { "chunk": 0, "vector": [1.0, 0.0, 0.0], "text": "one" },
                { "chunk": 1, "vector": [0.0, 1.0, 0.0], "text": "two" },
            ]),
        )
        .await;

    server
        .put(
            "/v1/db/shop/coll/docs/docs/a/vectors",
            Some(&token),
            json!([{ "chunk": 0, "vector": [1.0, 0.0, 0.0], "text": "one" }]),
        )
        .await;

    let read = server.get("/v1/db/shop/coll/docs/docs/a/vectors", Some(&token)).await;
    assert_eq!(read.body["count"], 1, "the dropped chunk must be gone: {:?}", read.body);
}

#[tokio::test]
async fn a_wrong_width_vector_is_refused() {
    // A mis-sized vector would score against nothing and look like "no
    // matches" rather than "wrong input".
    let server = Server::start().await;
    let token = byo_collection(&server).await;
    server.post("/v1/db/shop/coll/docs/docs", Some(&token), json!({ "_id": "a" })).await;

    let res = server
        .put(
            "/v1/db/shop/coll/docs/docs/a/vectors",
            Some(&token),
            json!([{ "chunk": 0, "vector": [1.0, 0.0], "text": "short" }]),
        )
        .await;

    assert_eq!(res.status, 400, "{:?}", res.body);
    assert!(res.body["message"].as_str().unwrap_or_default().contains("dimensions"));
}

#[tokio::test]
async fn vectors_cannot_be_attached_to_a_document_that_does_not_exist() {
    // `source_hlc` comes from the document, so without one there is nothing
    // for staleness detection to compare against.
    let server = Server::start().await;
    let token = byo_collection(&server).await;

    let res = server
        .put(
            "/v1/db/shop/coll/docs/docs/ghost/vectors",
            Some(&token),
            json!([{ "chunk": 0, "vector": [1.0, 0.0, 0.0], "text": "x" }]),
        )
        .await;
    assert_eq!(res.status, 404, "{:?}", res.body);
}

#[tokio::test]
async fn storing_vectors_needs_write_access() {
    let server = Server::start().await;
    let token = byo_collection(&server).await;
    server.post("/v1/db/shop/coll/docs/docs", Some(&token), json!({ "_id": "a" })).await;

    server
        .post(
            "/v1/users",
            Some(&token),
            json!({
                "user": "reader",
                "password": "reader-password-1",
                "grants": [{ "db": "shop", "collection": "docs", "actions": ["read"] }],
            }),
        )
        .await;
    let reader = server.login("reader", "reader-password-1").await;

    let res = server
        .put(
            "/v1/db/shop/coll/docs/docs/a/vectors",
            Some(&reader),
            json!([{ "chunk": 0, "vector": [1.0, 0.0, 0.0], "text": "x" }]),
        )
        .await;
    assert_eq!(res.status, 403, "{:?}", res.body);
}

/// A collection of `n` documents, half of them `even`.
async fn paged(server: &Server, coll: &str, n: i64) -> String {
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name": coll})).await;
    let batch: Vec<Value> = (0..n)
        .map(|i| json!({"_id": i, "parity": if i % 2 == 0 { "even" } else { "odd" }}))
        .collect();
    server.post(&format!("/v1/db/shop/coll/{coll}/bulk"), Some(&token), json!(batch)).await;
    token
}

/// Walk every page, returning the `_id`s in the order they were delivered.
///
/// Exactly what a client does: ask for a page, follow `nextCursor` until it
/// stops coming. The first request carries no cursor.
async fn walk(server: &Server, token: &str, coll: &str, body: Value) -> Vec<i64> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..200 {
        let mut req = body.clone();
        if let Some(c) = &cursor {
            req["cursor"] = json!(c);
        }
        let res = server.post(&format!("/v1/db/shop/coll/{coll}/find"), Some(token), req).await;
        assert_eq!(res.status, 200, "{:?}", res.body);
        out.extend(
            res.body["documents"].as_array().unwrap().iter().map(|d| d["_id"].as_i64().unwrap()),
        );
        match res.body.get("nextCursor").and_then(|c| c.as_str()) {
            Some(next) => cursor = Some(next.to_string()),
            None => return out,
        }
    }
    panic!("pagination did not terminate");
}

#[tokio::test]
async fn paging_sees_every_document_exactly_once() {
    // The property that matters. A boundary bug shows up here as a missing or
    // repeated id, which is exactly what a range cursor gets wrong when the
    // bound is inclusive on the wrong side.
    let server = Server::start().await;
    let token = paged(&server, "orders", 250).await;

    let seen = walk(&server, &token, "orders", json!({"filter": {}, "limit": 37})).await;

    assert_eq!(seen.len(), 250, "wrong number of documents: {}", seen.len());
    assert_eq!(seen, (0..250).collect::<Vec<i64>>(), "wrong order or contents");
}

#[tokio::test]
async fn paging_a_filtered_query_sees_every_match_exactly_once() {
    let server = Server::start().await;
    let token = paged(&server, "orders", 250).await;

    let seen =
        walk(&server, &token, "orders", json!({"filter": {"parity": "even"}, "limit": 13})).await;

    let expected: Vec<i64> = (0..250).filter(|i| i % 2 == 0).collect();
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn paging_through_an_index_agrees_with_paging_through_a_scan() {
    // Index candidates arrive in document-key order too, so the cursor bound
    // applies to both paths — and both must produce the same walk.
    let server = Server::start().await;
    let token = paged(&server, "scanned", 200).await;
    let _ = paged(&server, "indexed", 200).await;
    server
        .post(
            "/v1/db/shop/coll/indexed/indexes",
            Some(&token),
            json!({"name": "parity_1", "fields": [{"path": "parity"}]}),
        )
        .await;

    let body = json!({"filter": {"parity": "odd"}, "limit": 11});
    let a = walk(&server, &token, "indexed", body.clone()).await;
    let b = walk(&server, &token, "scanned", body).await;

    // Confirm the index really was used, so this is not two scans agreeing.
    let plan = server
        .post(
            "/v1/db/shop/coll/indexed/find",
            Some(&token),
            json!({"filter": {"parity": "odd"}, "explain": true}),
        )
        .await;
    assert_eq!(plan.body["explain"]["strategy"], "index", "{:?}", plan.body);

    assert_eq!(a, b, "the index path and the scan path paged differently");
    assert_eq!(a, (0..200).filter(|i| i % 2 == 1).collect::<Vec<i64>>());
}

#[tokio::test]
async fn the_last_page_carries_no_cursor() {
    let server = Server::start().await;
    let token = paged(&server, "orders", 10).await;

    let res = server
        .post("/v1/db/shop/coll/orders/find", Some(&token), json!({"filter": {}, "limit": 100}))
        .await;
    assert_eq!(res.body["count"], 10);
    assert!(
        res.body.get("nextCursor").is_none(),
        "a short page is the end; offering a cursor invites a round trip to learn nothing"
    );
}

#[tokio::test]
async fn a_full_page_offers_a_continuation_without_being_asked() {
    // A client's first request carries no cursor, so the first reply has to
    // be the thing that hands one over. Requiring a cursor to receive a
    // cursor would leave no way to start.
    let server = Server::start().await;
    let token = paged(&server, "orders", 50).await;

    let res = server
        .post("/v1/db/shop/coll/orders/find", Some(&token), json!({"filter": {}, "limit": 10}))
        .await;
    assert_eq!(res.body["count"], 10);
    assert!(res.body.get("nextCursor").is_some(), "{:?}", res.body);
}

#[tokio::test]
async fn no_continuation_is_offered_for_a_query_a_cursor_cannot_page() {
    // The dangerous case: a caller sorts by a field, sees a token, follows
    // it, and silently gets _id order instead of the order they asked for.
    let server = Server::start().await;
    let token = paged(&server, "orders", 50).await;

    for body in [
        json!({"filter": {}, "limit": 10, "sort": {"parity": -1}}),
        json!({"filter": {}, "limit": 10, "skip": 5}),
    ] {
        let res = server.post("/v1/db/shop/coll/orders/find", Some(&token), body.clone()).await;
        assert_eq!(res.body["count"], 10);
        assert!(res.body.get("nextCursor").is_none(), "{body:?} -> {:?}", res.body);
    }
}

#[tokio::test]
async fn a_cursor_refuses_what_it_cannot_honour() {
    let server = Server::start().await;
    let token = paged(&server, "orders", 20).await;

    for (body, why) in [
        (json!({"cursor": "AA", "skip": 5}), "skip and cursor both say where to resume"),
        (json!({"cursor": "AA", "sort": {"parity": 1}}), "a cursor pages in _id order"),
        (json!({"cursor": "!!! not base64"}), "malformed"),
    ] {
        let res = server.post("/v1/db/shop/coll/orders/find", Some(&token), body).await;
        assert_eq!(res.status, 400, "{why}: {:?}", res.body);
    }

    // `_id` ascending is the order a cursor already pages in, so it is allowed.
    let ok = server
        .post("/v1/db/shop/coll/orders/find", Some(&token), json!({"sort": {"_id": 1}, "limit": 5}))
        .await;
    assert_eq!(ok.status, 200, "{:?}", ok.body);
    assert_eq!(ok.body["count"], 5);
}

#[tokio::test]
async fn an_unlimited_find_returns_a_page_and_not_the_collection() {
    // The trap the specification now names: a client that reads `find` with no
    // `limit` as "everything" processes a prefix of 100 and is told nothing.
    // `count` is the honest source for a total — it has no cap, because a count
    // that stopped early would be a wrong number rather than a short list.
    let server = Server::start().await;
    let token = paged(&server, "orders", 150).await;

    let all =
        server.post("/v1/db/shop/coll/orders/find", Some(&token), json!({ "filter": {} })).await;
    assert_eq!(all.body["count"], 100, "the default page is 100, not the collection");
    assert!(all.body["nextCursor"].is_string(), "and it says how to get the rest");

    let counted =
        server.post("/v1/db/shop/coll/orders/count", Some(&token), json!({ "filter": {} })).await;
    assert_eq!(counted.body["count"], 150);

    // Over the cap is clamped rather than refused: the request succeeds and
    // returns fewer documents than were asked for.
    let over = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({ "filter": {}, "limit": 50_000 }),
        )
        .await;
    assert_eq!(over.status, 200, "over the cap is not an error: {:?}", over.body);
    assert_eq!(over.body["count"], 150, "everything there was, and no complaint about the ask");
}

// ---------------------------------------------------------------------------
// Reads bounded by what they return (ADR-098)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn count_agrees_on_every_access_path_and_says_which_it_took() {
    // `count` no longer materialises its matches, on any of the three
    // routes. The number must be what it was — with and without an index,
    // with a residual filter the index cannot answer alone, over a `$in`
    // union, and through the primary key — and `explain` still describes it.
    let server = Server::start().await;
    let token = server.root().await;
    seed_pair(&server, &token).await;
    let count = |body: Value| async {
        let res = server.post("/v1/db/shop/coll/indexed/count", Some(&token), body).await;
        assert_eq!(res.status, 200, "{:?}", res.body);
        res.body
    };

    let plain = json!({ "filter": { "qty": 3 }, "explain": true });
    let residual = json!({ "filter": { "qty": 3, "item": "w1" }, "explain": true });
    let union = json!({ "filter": { "qty": { "$in": [1, 2] } }, "explain": true });

    let scanned =
        (count(plain.clone()).await, count(residual.clone()).await, count(union.clone()).await);
    assert_eq!(scanned.0["explain"]["strategy"], "collectionScan");
    assert_eq!(scanned.0["count"], 9, "qty == 3 for i in 1..=60");
    assert_eq!(scanned.0["explain"]["documentsExamined"], 60);
    assert_eq!(scanned.0["explain"]["documentsMatched"], 9);

    server
        .post(
            "/v1/db/shop/coll/indexed/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "qty" }] }),
        )
        .await;

    let indexed = (count(plain).await, count(residual).await, count(union).await);
    assert_eq!(indexed.0["explain"]["strategy"], "index");
    assert_eq!(indexed.1["explain"]["strategy"], "index");
    assert_eq!(indexed.2["explain"]["strategy"], "indexUnion");
    for (before, after) in
        [(&scanned.0, &indexed.0), (&scanned.1, &indexed.1), (&scanned.2, &indexed.2)]
    {
        assert_eq!(after["count"], before["count"], "{after} vs {before}");
        assert_eq!(after["explain"]["documentsMatched"], before["explain"]["documentsMatched"]);
    }
    // The index narrowed the work, and the count says how far into it went.
    assert_eq!(indexed.0["explain"]["documentsExamined"], 9);
    assert_eq!(indexed.0["explain"]["indexEntriesRead"], 9);
    assert!(scanned.0["explain"].get("indexEntriesRead").is_none(), "a scan read no index");

    let by_id =
        count(json!({ "filter": { "_id": { "$in": [1, 2, 3, 999] } }, "explain": true })).await;
    assert_eq!(by_id["count"], 3);
    assert_eq!(by_id["explain"]["strategy"], "idLookup");
}

#[tokio::test]
async fn a_sorted_page_is_the_page_the_full_sort_gives() {
    // A sorted `find` holds `skip + limit` documents instead of every match.
    // Every window must be the same slice of the same order as one request
    // for everything, ties — `qty` repeats every seven documents — included,
    // and the same again when asked twice.
    let server = Server::start().await;
    let token = server.root().await;
    seed_pair(&server, &token).await;
    let find = |body: Value| async {
        let res = server.post("/v1/db/shop/coll/control/find", Some(&token), body).await;
        assert_eq!(res.status, 200, "{:?}", res.body);
        res.body["documents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["_id"].as_i64().unwrap())
            .collect::<Vec<_>>()
    };

    for sort in [json!({ "qty": -1 }), json!({ "qty": 1, "item": -1 }), json!({ "item": 1 })] {
        let full = find(json!({ "sort": sort, "limit": 100 })).await;
        assert_eq!(full.len(), 60);
        for (skip, limit) in [(0usize, 10usize), (7, 5), (55, 10), (60, 5), (0, 1)] {
            let page = find(json!({ "sort": sort, "skip": skip, "limit": limit })).await;
            let expected: Vec<i64> = full.iter().copied().skip(skip).take(limit).collect();
            assert_eq!(page, expected, "sort {sort}, skip {skip}, limit {limit}");
        }
        assert_eq!(find(json!({ "sort": sort, "limit": 100 })).await, full, "deterministic");
    }

    // Ties are broken by `_id` ascending: within one `qty`, ids climb.
    let by_qty = find(json!({ "sort": { "qty": -1 }, "limit": 100 })).await;
    for pair in by_qty.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if a % 7 == b % 7 {
            assert!(a < b, "tie between {a} and {b} broken the wrong way");
        }
    }
}

#[tokio::test]
async fn a_sorted_window_past_the_cap_is_refused_and_says_what_to_do_instead() {
    // The one behaviour change in ADR-098. `skip + limit` on a sorted find
    // is what the server holds while it sorts, and it stops at the same
    // 10,000 `limit` stops at — refused rather than clamped, because a
    // clamped skip would quietly return the wrong page.
    let server = Server::start().await;
    let token = paged(&server, "orders", 20).await;
    let find = |body: Value| async {
        server.post("/v1/db/shop/coll/orders/find", Some(&token), body).await
    };

    let over = find(json!({ "sort": { "parity": 1 }, "skip": 9_950, "limit": 100 })).await;
    assert_eq!(over.status, 400, "{:?}", over.body);
    assert_eq!(over.body["error"], "bad_request");
    let message = over.body["message"].as_str().unwrap();
    assert!(message.contains("10000"), "the ceiling is named: {message}");
    assert!(message.contains("nextCursor"), "and the way past it: {message}");

    // Exactly at the ceiling is fine; the collection is just short.
    let at = find(json!({ "sort": { "parity": 1 }, "skip": 9_900, "limit": 100 })).await;
    assert_eq!(at.status, 200, "{:?}", at.body);
    assert_eq!(at.body["count"], 0);

    // The ceiling is about what a sort holds. An unsorted find holds only
    // its page, and `_id` ascending is the order the scan already delivers,
    // so neither has one.
    for body in [
        json!({ "skip": 20_000, "limit": 100 }),
        json!({ "sort": { "_id": 1 }, "skip": 20_000, "limit": 100 }),
    ] {
        let res = find(body.clone()).await;
        assert_eq!(res.status, 200, "{body}: {:?}", res.body);
        assert_eq!(res.body["count"], 0);
    }
    // And a deep unsorted skip still lands on the right page.
    let deep = find(json!({ "skip": 15, "limit": 100 })).await;
    let ids: Vec<i64> = deep.body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![15, 16, 17, 18, 19]);
}

#[tokio::test]
async fn an_in_over_overlapping_probes_pages_each_document_once_in_id_order() {
    // A document whose array holds two of the listed values is under two
    // probes. Streamed as a merge rather than gathered into a set, it must
    // still appear once, in `_id` order, and a cursor walk across the union
    // must neither skip nor repeat it.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({ "name": "tagged" })).await;
    let batch: Vec<Value> = (0..40i64)
        .map(|i| {
            let mut tags = Vec::new();
            if i % 2 == 0 {
                tags.push("a");
            }
            if i % 3 == 0 {
                tags.push("b");
            }
            if i % 5 == 0 {
                tags.push("c");
            }
            json!({ "_id": i, "tags": tags })
        })
        .collect();
    server.post("/v1/db/shop/coll/tagged/bulk", Some(&token), json!(batch)).await;
    server
        .post(
            "/v1/db/shop/coll/tagged/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "tags" }] }),
        )
        .await;

    let filter = json!({ "tags": { "$in": ["a", "b"] } });
    let expected: Vec<i64> = (0..40).filter(|i| i % 2 == 0 || i % 3 == 0).collect();

    let explained = server
        .post(
            "/v1/db/shop/coll/tagged/find",
            Some(&token),
            json!({ "filter": filter, "limit": 100, "explain": true }),
        )
        .await;
    assert_eq!(explained.body["explain"]["strategy"], "indexUnion", "{}", explained.body);
    let ids: Vec<i64> = explained.body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, expected, "once each, ascending");
    assert_eq!(explained.body["explain"]["documentsExamined"], expected.len());

    let walked = walk(&server, &token, "tagged", json!({ "filter": filter, "limit": 3 })).await;
    assert_eq!(walked, expected, "a cursor walk over the union sees each once");
}

#[tokio::test]
async fn an_indexed_find_with_limit_one_reads_one_entry() {
    // The unselective-equality case: 300 documents under one key. The old
    // path gathered all 300 candidate keys before reading the first
    // document; now it reads one entry, one document, and stops.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({ "name": "same" })).await;
    let batch: Vec<Value> = (0..300i64).map(|i| json!({ "_id": i, "k": 1 })).collect();
    server.post("/v1/db/shop/coll/same/bulk", Some(&token), json!(batch)).await;
    server
        .post("/v1/db/shop/coll/same/indexes", Some(&token), json!({ "fields": [{ "path": "k" }] }))
        .await;

    let one = server
        .post(
            "/v1/db/shop/coll/same/find",
            Some(&token),
            json!({ "filter": { "k": 1 }, "limit": 1, "explain": true }),
        )
        .await;
    assert_eq!(one.body["explain"]["strategy"], "index", "{}", one.body);
    assert_eq!(one.body["documents"][0]["_id"], 0, "the least _id");
    assert_eq!(one.body["explain"]["documentsExamined"], 1);
    assert_eq!(one.body["explain"]["indexEntriesRead"], 1);

    // The count reads them all — it has to — and says so.
    let all = server
        .post(
            "/v1/db/shop/coll/same/count",
            Some(&token),
            json!({ "filter": { "k": 1 }, "explain": true }),
        )
        .await;
    assert_eq!(all.body["count"], 300);
    assert_eq!(all.body["explain"]["indexEntriesRead"], 300);
}

#[tokio::test]
async fn a_full_last_page_still_offers_a_cursor_and_the_next_page_is_empty() {
    // A client must end its walk on a short page or a missing token, not on a
    // token stopping being offered. A collection whose size is an exact
    // multiple of the page size is where the two rules differ, and it is the
    // case a client author is most likely to get wrong.
    let server = Server::start().await;
    let token = paged(&server, "orders", 20).await;

    let page = server
        .post("/v1/db/shop/coll/orders/find", Some(&token), json!({ "filter": {}, "limit": 20 }))
        .await;
    assert_eq!(page.body["count"], 20);
    let cursor = page.body["nextCursor"].as_str().expect("a full page offers one").to_string();

    let past_the_end = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({ "filter": {}, "limit": 20, "cursor": cursor }),
        )
        .await;
    assert_eq!(past_the_end.status, 200);
    assert_eq!(past_the_end.body["count"], 0, "the page after the last one is empty, not an error");
    assert!(past_the_end.body["nextCursor"].is_null(), "and it ends the walk");
}

#[tokio::test]
async fn a_cursor_is_a_position_rather_than_a_query() {
    // Deliberate, and documented rather than enforced: the token encodes a
    // key, so it resumes *any* query after that key. The server does not check
    // that it came from the query it is used with, and a client should not
    // expect it to.
    let server = Server::start().await;
    let token = paged(&server, "orders", 20).await;

    let first = server
        .post("/v1/db/shop/coll/orders/find", Some(&token), json!({ "filter": {}, "limit": 10 }))
        .await;
    let cursor = first.body["nextCursor"].as_str().expect("a cursor").to_string();

    // The same token, a different filter: it resumes that filter after the
    // same position rather than being refused.
    let filtered = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({ "filter": { "parity": "even" }, "limit": 100, "cursor": cursor }),
        )
        .await;
    assert_eq!(filtered.status, 200, "{:?}", filtered.body);
    let ids: Vec<i64> = filtered.body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_i64().unwrap())
        .collect();
    assert!(
        ids.iter().all(|id| *id >= 10 && id % 2 == 0),
        "resumed after the key, filtered: {ids:?}"
    );
}

#[tokio::test]
async fn a_cursor_survives_writes_behind_and_ahead_of_it() {
    // Paging is not a snapshot, and should not pretend to be. What it must
    // guarantee is that it never skips or repeats a document that was present
    // for the whole walk.
    let server = Server::start().await;
    let token = paged(&server, "orders", 100).await;

    let first = server
        .post("/v1/db/shop/coll/orders/find", Some(&token), json!({"filter": {}, "limit": 50}))
        .await;
    let cursor = first.body["nextCursor"].as_str().expect("a cursor").to_string();

    // One behind the cursor, one ahead of it.
    server.post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id": -1})).await;
    server.post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id": 1000})).await;

    let rest = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({"filter": {}, "limit": 1000, "cursor": cursor}),
        )
        .await;
    let ids: Vec<i64> = rest.body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_i64().unwrap())
        .collect();

    assert!(!ids.contains(&-1), "an insert behind the cursor must not reappear");
    assert!(ids.contains(&1000), "an insert ahead of the cursor is seen, as any scan would");
    assert_eq!(ids.iter().filter(|&&i| i == 50).count(), 1, "no document repeats");
}

#[tokio::test]
async fn a_unique_index_can_apply_only_where_the_field_is_present() {
    // The motivating case, and impossible before partial indexes: a missing
    // field indexes as null, so two documents lacking it collided.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/app/collections", Some(&token), json!({"name":"users"})).await;

    let res = server
        .post(
            "/v1/db/app/coll/users/indexes",
            Some(&token),
            json!({
                "name": "email_unique_present",
                "fields": [{"path": "email"}],
                "unique": true,
                "partialFilterExpression": {"email": {"$exists": true}},
            }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["partialFilterExpression"]["email"]["$exists"], true);

    // Several documents with no email at all: all fine, none collide.
    for id in 1..=3 {
        let res = server.post("/v1/db/app/coll/users/docs", Some(&token), json!({"_id": id})).await;
        assert_eq!(res.status, 200, "document {id} should not collide: {:?}", res.body);
    }

    // The constraint still bites where the field is present.
    let ok = server
        .post("/v1/db/app/coll/users/docs", Some(&token), json!({"_id": 4, "email": "a@b.c"}))
        .await;
    assert_eq!(ok.status, 200, "{:?}", ok.body);
    let dup = server
        .post("/v1/db/app/coll/users/docs", Some(&token), json!({"_id": 5, "email": "a@b.c"}))
        .await;
    assert_eq!(dup.status, 409, "a duplicate present value must still be refused: {:?}", dup.body);
}

#[tokio::test]
async fn an_unsupported_partial_filter_is_refused_at_creation() {
    // The refusal has to land here, where an operator can act on it — not at
    // query time, where the only symptom is a plan that quietly stopped
    // applying.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/app/collections", Some(&token), json!({"name":"users"})).await;

    for bad in [
        json!({"$or": [{"a": 1}, {"b": 2}]}),
        json!({"a": {"$ne": 1}}),
        json!({"a": {"$in": [1, 2]}}),
        json!({"a": {"$regex": "^x"}}),
        json!({"a": {"$exists": false}}),
        json!({}),
    ] {
        let res = server
            .post(
                "/v1/db/app/coll/users/indexes",
                Some(&token),
                json!({"name": "bad", "fields": [{"path": "a"}], "partialFilterExpression": bad}),
            )
            .await;
        assert_eq!(res.status, 400, "should have been refused: {bad:?} -> {:?}", res.body);
    }
}

#[tokio::test]
async fn creating_a_partial_unique_index_judges_only_the_documents_it_covers() {
    // Existing data that violates the constraint *outside* the filter is not a
    // violation, because those documents are not in the index. Refusing here
    // would advertise a constraint stricter than the one being created.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/app/collections", Some(&token), json!({"name":"users"})).await;
    server
        .post(
            "/v1/db/app/coll/users/bulk",
            Some(&token),
            json!([
                {"_id": 1, "email": "dup@x.com", "status": "archived"},
                {"_id": 2, "email": "dup@x.com", "status": "archived"},
                {"_id": 3, "email": "solo@x.com", "status": "active"},
            ]),
        )
        .await;

    // Covers only the active one, so the archived duplicates do not count.
    let ok = server
        .post(
            "/v1/db/app/coll/users/indexes",
            Some(&token),
            json!({
                "name": "email_active_unique",
                "fields": [{"path": "email"}],
                "unique": true,
                "partialFilterExpression": {"status": "active"},
            }),
        )
        .await;
    assert_eq!(ok.status, 200, "duplicates outside the filter must not block it: {:?}", ok.body);

    // A plain unique index over the same data is still refused.
    let refused = server
        .post(
            "/v1/db/app/coll/users/indexes",
            Some(&token),
            json!({"name": "email_unique", "fields": [{"path": "email"}], "unique": true}),
        )
        .await;
    assert_eq!(refused.status, 409, "{:?}", refused.body);
}

/// A collection of 100 documents, half `active`, indexed partially on status.
async fn partial_seeded(server: &Server) -> String {
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server
        .post(
            "/v1/db/shop/coll/orders/indexes",
            Some(&token),
            json!({
                "name": "qty_active",
                "fields": [{"path": "qty"}],
                "partialFilterExpression": {"status": "active"},
            }),
        )
        .await;
    let batch: Vec<Value> = (0..100)
        .map(|i| {
            json!({
                "_id": i,
                "qty": i % 10,
                "status": if i % 2 == 0 { "active" } else { "done" },
            })
        })
        .collect();
    server.post("/v1/db/shop/coll/orders/bulk", Some(&token), json!(batch)).await;
    token
}

#[tokio::test]
async fn a_partial_index_is_used_only_when_the_query_proves_containment() {
    let server = Server::start().await;
    let token = partial_seeded(&server).await;

    // Proven: the query pins the same equality the filter does.
    let used = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({"filter": {"status": "active", "qty": 4}, "explain": true}),
        )
        .await;
    assert_eq!(used.body["explain"]["strategy"], "index", "{:?}", used.body);
    assert_eq!(used.body["explain"]["index"], "qty_active");

    // Unproven: no predicate on `status` at all, so the index would return a
    // subset. Falls back to a scan.
    let scanned = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({"filter": {"qty": 4}, "explain": true}),
        )
        .await;
    assert_eq!(scanned.body["explain"]["strategy"], "collectionScan", "{:?}", scanned.body);

    // Unproven: the wrong value.
    let other = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({"filter": {"status": "done", "qty": 4}, "explain": true}),
        )
        .await;
    assert_eq!(other.body["explain"]["strategy"], "collectionScan", "{:?}", other.body);
}

#[tokio::test]
async fn a_partial_index_never_changes_an_answer() {
    // The property that matters most: whichever access path is chosen, the
    // documents returned must be identical. A containment bug shows up here
    // as a short result, which is the failure mode this design is arranged
    // to prevent.
    let server = Server::start().await;
    let token = partial_seeded(&server).await;

    // The same data with no index at all.
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"plain"})).await;
    let batch: Vec<Value> = (0..100)
        .map(|i| {
            json!({
                "_id": i,
                "qty": i % 10,
                "status": if i % 2 == 0 { "active" } else { "done" },
            })
        })
        .collect();
    server.post("/v1/db/shop/coll/plain/bulk", Some(&token), json!(batch)).await;

    for filter in [
        json!({"status": "active", "qty": 4}),
        json!({"qty": 4}),
        json!({"status": "done", "qty": 4}),
        json!({"status": "active"}),
        json!({"qty": {"$gte": 5}, "status": "active"}),
        json!({"qty": null}),
        json!({"status": {"$ne": "active"}}),
        json!({}),
    ] {
        let a = server
            .post(
                "/v1/db/shop/coll/orders/find",
                Some(&token),
                json!({"filter": filter, "sort": {"_id": 1}, "limit": 1000}),
            )
            .await;
        let b = server
            .post(
                "/v1/db/shop/coll/plain/find",
                Some(&token),
                json!({"filter": filter, "sort": {"_id": 1}, "limit": 1000}),
            )
            .await;
        assert_eq!(
            a.body["documents"], b.body["documents"],
            "partial index changed the answer for {filter:?}"
        );
    }
}

#[tokio::test]
async fn a_document_leaving_the_filter_loses_its_index_entries() {
    // Membership is not decided once at insert: a document updated out of the
    // filter must stop being a candidate, or a later query finds a candidate
    // whose document no longer belongs.
    let server = Server::start().await;
    let token = partial_seeded(&server).await;

    // _id 0 is active with qty 0, so the index holds it.
    let before = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({"filter": {"status": "active", "qty": 0}, "explain": true}),
        )
        .await;
    assert_eq!(before.body["explain"]["strategy"], "index");
    let seen: Vec<i64> = before.body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_i64().unwrap())
        .collect();
    assert!(seen.contains(&0), "{seen:?}");

    // Move it out of the filter.
    server
        .post(
            "/v1/db/shop/coll/orders/update",
            Some(&token),
            json!({"filter": {"_id": 0}, "update": {"$set": {"status": "done"}}}),
        )
        .await;

    let after = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({"filter": {"status": "active", "qty": 0}, "explain": true}),
        )
        .await;
    let seen: Vec<i64> = after.body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_i64().unwrap())
        .collect();
    assert!(!seen.contains(&0), "a document that left the filter is still indexed: {seen:?}");

    // And back in again: entries must reappear.
    server
        .post(
            "/v1/db/shop/coll/orders/update",
            Some(&token),
            json!({"filter": {"_id": 0}, "update": {"$set": {"status": "active"}}}),
        )
        .await;
    let again = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({"filter": {"status": "active", "qty": 0}}),
        )
        .await;
    let seen: Vec<i64> = again.body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["_id"].as_i64().unwrap())
        .collect();
    assert!(seen.contains(&0), "rejoining the filter did not re-index: {seen:?}");
}

#[tokio::test]
async fn a_null_query_does_not_get_answered_by_a_sparse_style_index() {
    // The specific trap: `{email: null}` matches an explicit null *and* a
    // missing field, so it cannot prove `email` exists. Answering it from an
    // index that omits the absent ones would silently drop them.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/app/collections", Some(&token), json!({"name":"users"})).await;
    server
        .post(
            "/v1/db/app/coll/users/indexes",
            Some(&token),
            json!({
                "name": "email_present",
                "fields": [{"path": "email"}],
                "partialFilterExpression": {"email": {"$exists": true}},
            }),
        )
        .await;
    server
        .post(
            "/v1/db/app/coll/users/bulk",
            Some(&token),
            json!([
                {"_id": 1, "email": "a@b.c"},
                {"_id": 2, "email": Value::Null},
                {"_id": 3},
            ]),
        )
        .await;

    let res = server
        .post(
            "/v1/db/app/coll/users/find",
            Some(&token),
            json!({"filter": {"email": null}, "sort": {"_id": 1}, "explain": true}),
        )
        .await;
    assert_eq!(
        res.body["explain"]["strategy"], "collectionScan",
        "a null query must not use a presence-filtered index: {:?}",
        res.body
    );
    // Both the explicit null and the missing one.
    assert_eq!(res.body["count"], 2, "{:?}", res.body);
}

/// Seed a collection of 200 documents, indexed on `sku` unless `indexed` is
/// false, and return the token.
async fn seeded(server: &Server, coll: &str, indexed: bool) -> String {
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name": coll})).await;
    if indexed {
        server
            .post(
                &format!("/v1/db/shop/coll/{coll}/indexes"),
                Some(&token),
                json!({"name": "sku_1", "fields": [{"path": "sku"}]}),
            )
            .await;
    }
    let batch: Vec<Value> =
        (0..200).map(|i| json!({"_id": i, "sku": format!("sku-{}", i % 20), "n": 0})).collect();
    server.post(&format!("/v1/db/shop/coll/{coll}/bulk"), Some(&token), json!(batch)).await;
    token
}

#[tokio::test]
async fn update_uses_an_index_when_one_applies() {
    // The drift this fixes: `update` used to scan the collection however
    // selective the filter was, while `find` on the same filter planned.
    let server = Server::start().await;
    let token = seeded(&server, "orders", true).await;

    let res = server
        .post(
            "/v1/db/shop/coll/orders/update",
            Some(&token),
            json!({
                "filter": {"sku": "sku-7"},
                "update": {"$set": {"n": 1}},
                "multi": true,
                "explain": true,
            }),
        )
        .await;

    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["explain"]["strategy"], "index", "{:?}", res.body);
    assert_eq!(res.body["explain"]["index"], "sku_1");
    // Ten of the two hundred carry this sku, and only those were examined.
    assert_eq!(res.body["explain"]["documentsExamined"], 10, "{:?}", res.body);
    assert_eq!(res.body["matched"], 10);
    assert_eq!(res.body["modified"], 10);
}

#[tokio::test]
async fn delete_uses_an_index_when_one_applies() {
    let server = Server::start().await;
    let token = seeded(&server, "orders", true).await;

    let res = server
        .post(
            "/v1/db/shop/coll/orders/delete",
            Some(&token),
            json!({"filter": {"sku": "sku-3"}, "multi": true, "explain": true}),
        )
        .await;

    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["explain"]["strategy"], "index", "{:?}", res.body);
    assert_eq!(res.body["explain"]["documentsExamined"], 10);
    assert_eq!(res.body["deleted"], 10);
}

#[tokio::test]
async fn without_an_index_the_write_paths_still_scan_and_still_agree() {
    // The index must be an optimisation, not a change of answer.
    let server = Server::start().await;
    let indexed = seeded(&server, "with_index", true).await;
    let plain = seeded(&server, "no_index", false).await;

    let body = json!({
        "filter": {"sku": "sku-11"},
        "update": {"$set": {"n": 42}},
        "multi": true,
        "explain": true,
    });
    let a = server.post("/v1/db/shop/coll/with_index/update", Some(&indexed), body.clone()).await;
    let b = server.post("/v1/db/shop/coll/no_index/update", Some(&plain), body).await;

    assert_eq!(a.body["explain"]["strategy"], "index");
    assert_eq!(b.body["explain"]["strategy"], "collectionScan");
    assert_eq!(b.body["explain"]["documentsExamined"], 200, "a scan examines everything");

    // Same answer either way, which is the whole point.
    assert_eq!(a.body["matched"], b.body["matched"]);
    assert_eq!(a.body["modified"], b.body["modified"]);

    for coll in ["with_index", "no_index"] {
        let res = server
            .post(
                &format!("/v1/db/shop/coll/{coll}/find"),
                Some(&indexed),
                json!({"filter": {"n": 42}, "sort": {"_id": 1}}),
            )
            .await;
        assert_eq!(res.body["count"], 10, "{coll}: {:?}", res.body);
    }
}

#[tokio::test]
async fn a_single_update_still_touches_exactly_one_document() {
    // `multi: false` stops after the first match on the indexed path too.
    let server = Server::start().await;
    let token = seeded(&server, "orders", true).await;

    let res = server
        .post(
            "/v1/db/shop/coll/orders/update",
            Some(&token),
            json!({"filter": {"sku": "sku-5"}, "update": {"$set": {"n": 9}}, "explain": true}),
        )
        .await;
    assert_eq!(res.body["matched"], 1, "{:?}", res.body);
    assert_eq!(res.body["modified"], 1);
    assert_eq!(res.body["explain"]["documentsExamined"], 1, "stopped at the first match");

    let counted = server
        .post("/v1/db/shop/coll/orders/count", Some(&token), json!({"filter": {"n": 9}}))
        .await;
    assert_eq!(counted.body["count"], 1);
}

#[tokio::test]
async fn explain_is_absent_unless_asked_for() {
    let server = Server::start().await;
    let token = seeded(&server, "orders", true).await;

    let res = server
        .post(
            "/v1/db/shop/coll/orders/update",
            Some(&token),
            json!({"filter": {"sku": "sku-1"}, "update": {"$set": {"n": 1}}}),
        )
        .await;
    assert!(res.body.get("explain").is_none(), "{:?}", res.body);

    let res = server
        .post("/v1/db/shop/coll/orders/delete", Some(&token), json!({"filter": {"sku": "sku-1"}}))
        .await;
    assert!(res.body.get("explain").is_none(), "{:?}", res.body);
}

#[tokio::test]
async fn an_indexed_update_over_an_array_field_still_matches_every_document() {
    // Multikey is where an index-backed range quietly loses documents, and it
    // is the failure this codebase has already met once. An update through the
    // index must agree with a scan over array values too.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"tagged"})).await;
    server
        .post(
            "/v1/db/shop/coll/tagged/indexes",
            Some(&token),
            json!({"name": "tags_1", "fields": [{"path": "tags"}]}),
        )
        .await;
    server
        .post(
            "/v1/db/shop/coll/tagged/bulk",
            Some(&token),
            json!([
                {"_id": 1, "tags": ["a", "b"]},
                {"_id": 2, "tags": ["b"]},
                {"_id": 3, "tags": ["c"]},
                {"_id": 4, "tags": ["b", "b"]},
            ]),
        )
        .await;

    let res = server
        .post(
            "/v1/db/shop/coll/tagged/update",
            Some(&token),
            json!({
                "filter": {"tags": "b"},
                "update": {"$set": {"seen": true}},
                "multi": true,
                "explain": true,
            }),
        )
        .await;

    // Three documents carry "b"; _id 4 carries it twice and must be counted
    // once, not twice — the union deduplicates by document key.
    assert_eq!(res.body["matched"], 3, "{:?}", res.body);
    assert_eq!(res.body["modified"], 3);

    let counted = server
        .post("/v1/db/shop/coll/tagged/count", Some(&token), json!({"filter": {"seen": true}}))
        .await;
    assert_eq!(counted.body["count"], 3);
}

/// Seed a small job queue and return a token.
async fn jobs(server: &Server) -> String {
    let token = server.root().await;
    server.post("/v1/db/app/collections", Some(&token), json!({"name":"jobs"})).await;
    for (id, created, status) in
        [(1, 30, "pending"), (2, 10, "pending"), (3, 20, "done"), (4, 20, "pending")]
    {
        server
            .post(
                "/v1/db/app/coll/jobs/docs",
                Some(&token),
                json!({"_id": id, "created": created, "status": status}),
            )
            .await;
    }
    token
}

#[tokio::test]
async fn find_and_modify_claims_the_sorted_first_match() {
    let server = Server::start().await;
    let token = jobs(&server).await;

    let res = server
        .post(
            "/v1/db/app/coll/jobs/find_and_modify",
            Some(&token),
            json!({
                "filter": {"status": "pending"},
                "sort": {"created": 1},
                "update": {"$set": {"status": "claimed"}},
            }),
        )
        .await;

    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["matched"], 1);
    // Before-image by default, and the lowest `created` among pending.
    assert_eq!(res.body["document"]["_id"], 2);
    assert_eq!(res.body["document"]["status"], "pending");

    // The write really landed.
    let after = server.get("/v1/db/app/coll/jobs/docs/2", Some(&token)).await;
    assert_eq!(after.body["status"], "claimed");
}

#[tokio::test]
async fn return_document_after_gives_the_new_image() {
    let server = Server::start().await;
    let token = jobs(&server).await;

    let res = server
        .post(
            "/v1/db/app/coll/jobs/find_and_modify",
            Some(&token),
            json!({
                "filter": {"status": "pending"},
                "sort": {"created": 1},
                "update": {"$set": {"status": "claimed"}},
                "returnDocument": "after",
            }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["document"]["status"], "claimed");
}

#[tokio::test]
async fn draining_the_queue_never_repeats_a_job() {
    let server = Server::start().await;
    let token = jobs(&server).await;

    let mut claimed = Vec::new();
    for _ in 0..3 {
        let res = server
            .post(
                "/v1/db/app/coll/jobs/find_and_modify",
                Some(&token),
                json!({
                    "filter": {"status": "pending"},
                    "sort": {"created": 1},
                    "update": {"$set": {"status": "claimed"}},
                }),
            )
            .await;
        assert_eq!(res.body["matched"], 1, "{:?}", res.body);
        claimed.push(res.body["document"]["_id"].as_i64().unwrap());
    }
    claimed.sort();
    assert_eq!(claimed, vec![1, 2, 4]);

    // Queue empty: matched 0 and a null document, not an error.
    let res = server
        .post(
            "/v1/db/app/coll/jobs/find_and_modify",
            Some(&token),
            json!({"filter": {"status": "pending"}, "update": {"$set": {"status": "claimed"}}}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["matched"], 0);
    assert!(res.body["document"].is_null());
}

#[tokio::test]
async fn find_and_modify_can_remove_and_can_project() {
    let server = Server::start().await;
    let token = jobs(&server).await;

    let res = server
        .post(
            "/v1/db/app/coll/jobs/find_and_modify",
            Some(&token),
            json!({
                "filter": {"status": "pending"},
                "sort": {"created": 1},
                "remove": true,
                "projection": {"_id": 1, "created": 1},
            }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["document"]["_id"], 2);
    assert!(res.body["document"].get("status").is_none(), "projection applied");

    let gone = server.get("/v1/db/app/coll/jobs/docs/2", Some(&token)).await;
    assert_eq!(gone.status, 404);
}

#[tokio::test]
async fn upsert_seeds_the_filters_equalities() {
    // The Mongo behaviour people rely on: the created document carries the
    // fields the filter pinned, not just the update's.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/app/collections", Some(&token), json!({"name":"counters"})).await;

    let res = server
        .post(
            "/v1/db/app/coll/counters/find_and_modify",
            Some(&token),
            json!({
                "filter": {"_id": "hits", "scope": "global"},
                "update": {"$inc": {"n": 1}},
                "upsert": true,
                "returnDocument": "after",
            }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["document"]["_id"], "hits");
    assert_eq!(res.body["document"]["scope"], "global");
    assert_eq!(res.body["document"]["n"], 1);
    assert_eq!(res.body["matched"], 0, "an upsert did not match, it created");

    // A second call now matches and increments rather than creating again.
    let res = server
        .post(
            "/v1/db/app/coll/counters/find_and_modify",
            Some(&token),
            json!({
                "filter": {"_id": "hits", "scope": "global"},
                "update": {"$inc": {"n": 1}},
                "upsert": true,
                "returnDocument": "after",
            }),
        )
        .await;
    assert_eq!(res.body["matched"], 1);
    assert_eq!(res.body["document"]["n"], 2);
}

#[tokio::test]
async fn set_on_insert_applies_to_the_upsert_and_not_to_the_match() {
    // The created-at idiom: stamp the document once, when the upsert makes
    // it, and never touch the stamp again however many times it is hit.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/app/collections", Some(&token), json!({"name":"counters"})).await;

    let request = json!({
        "filter": {"_id": "hits"},
        "update": {"$setOnInsert": {"created_at": 100, "meta.origin": "upsert"}, "$inc": {"n": 1}},
        "upsert": true,
        "returnDocument": "after",
    });
    let res = server
        .post("/v1/db/app/coll/counters/find_and_modify", Some(&token), request.clone())
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["matched"], 0);
    assert_eq!(res.body["document"]["created_at"], 100);
    assert_eq!(res.body["document"]["meta"]["origin"], "upsert");
    assert_eq!(res.body["document"]["n"], 1);

    // Change what the insert would have set, then match: it stays as stored.
    server
        .post(
            "/v1/db/app/coll/counters/find_and_modify",
            Some(&token),
            json!({"filter": {"_id": "hits"}, "update": {"$set": {"created_at": 7}}}),
        )
        .await;
    let res = server.post("/v1/db/app/coll/counters/find_and_modify", Some(&token), request).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["matched"], 1);
    assert_eq!(res.body["document"]["created_at"], 7);
    assert_eq!(res.body["document"]["n"], 2);

    // The same path in $setOnInsert and another operator is refused up front,
    // whether or not the request would have inserted.
    let res = server
        .post(
            "/v1/db/app/coll/counters/find_and_modify",
            Some(&token),
            json!({
                "filter": {"_id": "hits"},
                "update": {"$setOnInsert": {"n": 0}, "$inc": {"n": 1}},
                "upsert": true,
            }),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert_eq!(res.body["error"], "bad_request");
    assert!(
        res.body["message"].as_str().unwrap_or_default().contains("conflicts"),
        "{:?}",
        res.body
    );
}

#[tokio::test]
async fn contradictory_find_and_modify_requests_are_refused() {
    let server = Server::start().await;
    let token = jobs(&server).await;

    for body in [
        // Both an update and a removal.
        json!({"filter": {}, "update": {"$set": {"a": 1}}, "remove": true}),
        // Neither.
        json!({"filter": {}}),
        // A removal has no "after".
        json!({"filter": {}, "remove": true, "returnDocument": "after"}),
        // Nothing to upsert into on a removal.
        json!({"filter": {}, "remove": true, "upsert": true}),
        // An unknown returnDocument, rather than silently defaulting.
        json!({"filter": {}, "update": {"$set": {"a": 1}}, "returnDocument": "sideways"}),
    ] {
        let res = server.post("/v1/db/app/coll/jobs/find_and_modify", Some(&token), body).await;
        assert_eq!(res.status, 400, "{:?}", res.body);
    }
}

#[tokio::test]
async fn find_and_modify_needs_write_permission() {
    let server = Server::start().await;
    let token = jobs(&server).await;
    server
        .post(
            "/v1/users",
            Some(&token),
            json!({
                "user": "reader", "password": "reader-password",
                "grants": [{"db":"app","collection":"*","actions":["read"]}]
            }),
        )
        .await;
    let reader = server.login("reader", "reader-password").await;

    let res = server
        .post(
            "/v1/db/app/coll/jobs/find_and_modify",
            Some(&reader),
            json!({"filter": {}, "update": {"$set": {"status": "claimed"}}}),
        )
        .await;
    assert_eq!(res.status, 403, "{:?}", res.body);
}

#[tokio::test]
async fn a_ttl_index_round_trips_through_the_api() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/app/collections", Some(&token), json!({"name":"sessions"})).await;

    let res = server
        .post(
            "/v1/db/app/coll/sessions/indexes",
            Some(&token),
            json!({"name":"ttl_seen","fields":[{"path":"seen"}],"expireAfterSeconds":3600}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["expireAfterSeconds"], 3600);

    let res = server.get("/v1/db/app/coll/sessions/indexes", Some(&token)).await;
    let listed = res.body["indexes"]
        .as_array()
        .expect("indexes")
        .iter()
        .find(|i| i["name"] == "ttl_seen")
        .expect("the TTL index is listed")
        .clone();
    assert_eq!(listed["expireAfterSeconds"], 3600);
}

#[tokio::test]
async fn an_ordinary_index_carries_no_expiry_key_at_all() {
    // Absent rather than null: listing indexes must not suggest every one of
    // them has an expiry policy that happens to be unset.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/app/collections", Some(&token), json!({"name":"orders"})).await;

    let res = server
        .post(
            "/v1/db/app/coll/orders/indexes",
            Some(&token),
            json!({"name":"item_1","fields":[{"path":"item"}]}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert!(res.body.get("expireAfterSeconds").is_none(), "{:?}", res.body);
}

#[tokio::test]
async fn a_malformed_ttl_index_is_a_400() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/app/collections", Some(&token), json!({"name":"sessions"})).await;

    // Compound: expiry reads one date, and there is no rule for which.
    let res = server
        .post(
            "/v1/db/app/coll/sessions/indexes",
            Some(&token),
            json!({"name":"ttl_ab","fields":[{"path":"a"},{"path":"b"}],"expireAfterSeconds":60}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);

    // Negative.
    let res = server
        .post(
            "/v1/db/app/coll/sessions/indexes",
            Some(&token),
            json!({"name":"ttl_neg","fields":[{"path":"seen"}],"expireAfterSeconds":-1}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
}

#[tokio::test]
async fn computed_expressions_derive_fields_over_http() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    for (id, city, qty, price) in [(1, "london", 5, 2.5), (2, "london", 15, 1.0)] {
        server
            .post(
                "/v1/db/shop/coll/orders/docs",
                Some(&token),
                json!({"_id": id, "city": city, "qty": qty, "price": price}),
            )
            .await;
    }

    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [
                {"$addFields": {
                    "value": {"$multiply": ["$qty", "$price"]},
                    "label": {"$toUpper": "$city"},
                    "band": {"$cond": [{"$gte": ["$qty", 10]}, "bulk", "single"]},
                }},
                {"$sort": {"_id": 1}}
            ]}),
        )
        .await;

    assert_eq!(res.status, 200, "{:?}", res.body);
    let docs = res.body["documents"].as_array().expect("documents");
    assert_eq!(docs[0]["value"], 12.5);
    assert_eq!(docs[0]["label"], "LONDON");
    assert_eq!(docs[0]["band"], "single");
    assert_eq!(docs[1]["band"], "bulk");
}

#[tokio::test]
async fn a_field_path_through_an_array_fans_out_in_every_expression_context() {
    // The same pipeline MongoDB runs: a path that crosses an array is the
    // array of what it found, in `$addFields`, in `$expr` and as a `$group`
    // key — and `$unwind`, which names a field rather than reading one, is
    // unchanged by it.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    for (id, items) in [
        (1, json!([{"sku": "a", "qty": 1}, {"sku": "b", "qty": 2}])),
        (2, json!([{"sku": "a", "qty": 5}, {"sku": "b", "qty": 1}])),
        (3, json!([{"sku": "c", "qty": 1}, {"qty": 9}, 7])),
    ] {
        let res = server
            .post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id": id, "items": items}))
            .await;
        assert_eq!(res.status, 200, "{:?}", res.body);
    }

    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [
                {"$match": {"$expr": {"$in": ["b", "$items.sku"]}}},
                {"$addFields": {"skus": "$items.sku", "n": {"$size": "$$ROOT.items.qty"}}},
                {"$sort": {"_id": 1}}
            ]}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let docs = res.body["documents"].as_array().expect("documents");
    assert_eq!(docs.len(), 2, "{docs:?}");
    assert_eq!(docs[0]["skus"], json!(["a", "b"]));
    assert_eq!(docs[0]["n"], 2);

    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [
                {"$group": {"_id": "$items.sku", "orders": {"$sum": 1}}},
                {"$sort": {"orders": -1}}
            ]}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let docs = res.body["documents"].as_array().expect("documents");
    assert_eq!(docs.len(), 2, "{docs:?}");
    assert_eq!(docs[0]["_id"], json!(["a", "b"]));
    assert_eq!(docs[0]["orders"], 2);
    // The element without a `sku` and the scalar element contribute nothing.
    assert_eq!(docs[1]["_id"], json!(["c"]));

    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [{"$unwind": "$items"}, {"$count": "n"}]}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["documents"][0]["n"], 7);
}

#[tokio::test]
async fn unwind_refuses_a_path_that_crosses_an_array_over_http() {
    // Finding 10's own reproduction, over the route rather than the parse
    // layer: `items` is an array, so `$unwind: "$items.sku"` has no single
    // place to write an element back to, whatever any particular document's
    // `sku` holds — refused uniformly, `400`, naming the stage (ADR-130).
    // Before this fix the server answered `200` with the document unchanged.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server
        .post(
            "/v1/db/shop/coll/orders/docs",
            Some(&token),
            json!({"_id": 1, "items": [{"sku": "a", "qty": 1}, {"sku": "b", "qty": 2}]}),
        )
        .await;

    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [{"$unwind": "$items.sku"}]}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert_eq!(res.body["error"], "bad_request");
    let body = format!("{:?}", res.body);
    assert!(body.contains("$unwind"), "{body}");
    // The refusal names the concrete fix, not just the problem: the caller
    // who wrote `$items.sku` is told to unwind `$items` first.
    assert!(body.contains("$items"), "{body}");
    assert!(body.contains("sku"), "{body}");
}

#[tokio::test]
async fn a_computed_date_survives_the_extended_json_boundary() {
    // Dates are the type JSON cannot express, so a date expression is where a
    // working evaluator and a working edge are hardest to tell apart.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"events"})).await;
    server
        .post(
            "/v1/db/shop/coll/events/docs",
            Some(&token),
            // 2026-08-12T13:45:07.250Z
            json!({"_id": 1, "at": {"$date": {"$numberLong": "1786542307250"}}}),
        )
        .await;

    let res = server
        .post(
            "/v1/db/shop/coll/events/aggregate",
            Some(&token),
            json!({"pipeline": [
                {"$project": {
                    "_id": 0,
                    "y": {"$year": "$at"},
                    "stamp": {"$dateToString": {"date": "$at", "format": "%Y-%m-%d"}},
                    "later": {"$add": ["$at", 86400000i64]},
                }}
            ]}),
        )
        .await;

    assert_eq!(res.status, 200, "{:?}", res.body);
    let d = &res.body["documents"][0];
    assert_eq!(d["y"], 2026);
    assert_eq!(d["stamp"], "2026-08-12");
    // A date in, a date out — still wrapped as a date, not a bare number.
    assert_eq!(d["later"]["$date"], 1_786_628_707_250i64);
}

#[tokio::test]
async fn a_bad_expression_is_a_400_naming_the_problem() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server.post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id": 1, "qty": 5})).await;

    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [{"$addFields": {"n": {"$nope": ["$qty", 1]}}}]}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);

    // A type violation refuses rather than quietly producing null.
    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [{"$addFields": {"n": {"$divide": ["$qty", 0]}}}]}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
}

#[tokio::test]
async fn an_aggregation_pipeline_groups_and_counts() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    for (id, city, qty) in [(1, "London", 5), (2, "London", 15), (3, "Paris", 10)] {
        server
            .post(
                "/v1/db/shop/coll/orders/docs",
                Some(&token),
                json!({"_id": id, "city": city, "qty": qty}),
            )
            .await;
    }

    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [
                {"$match": {"qty": {"$gte": 5}}},
                {"$group": {"_id": "$city", "total": {"$sum": "$qty"}, "n": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]}),
        )
        .await;

    assert_eq!(res.status, 200, "{:?}", res.body);
    let docs = res.body["documents"].as_array().expect("documents");
    assert_eq!(docs.len(), 2, "{docs:?}");
    assert_eq!(docs[0]["_id"], "London");
    assert_eq!(docs[0]["total"], 20);
    assert_eq!(docs[0]["n"], 2);
    assert_eq!(docs[1]["_id"], "Paris");
}

#[tokio::test]
async fn lookup_is_authorized_against_the_collection_it_joins() {
    // The load-bearing test for $lookup. A caller granted read on one
    // collection must not be able to pull a second one through a join —
    // that would be a privilege escalation shaped like a query, and it
    // would route around the single authorization point entirely.
    let server = Server::start().await;
    let token = server.root().await;

    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"customers"})).await;
    server
        .post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id": 1, "cust": "c1"}))
        .await;
    server
        .post(
            "/v1/db/shop/coll/customers/docs",
            Some(&token),
            json!({"_id": "c1", "secret": "not for everyone"}),
        )
        .await;

    // Superuser can join.
    let pipeline = json!({"pipeline": [
        {"$lookup": {"from": "customers", "localField": "cust",
                     "foreignField": "_id", "as": "customer"}}
    ]});
    let res =
        server.post("/v1/db/shop/coll/orders/aggregate", Some(&token), pipeline.clone()).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let joined = &res.body["documents"][0]["customer"];
    assert_eq!(joined[0]["secret"], "not for everyone", "the join must actually join");

    // A principal with read on `orders` only must be refused.
    server
        .post(
            "/v1/users",
            Some(&token),
            json!({"user":"limited","password":"limited-password",
                   "grants":[{"db":"shop","collection":"orders","actions":["read"]}]}),
        )
        .await;
    let limited = server.login("limited", "limited-password").await;

    let plain = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&limited),
            json!({"pipeline": [{"$match": {}}]}),
        )
        .await;
    assert_eq!(plain.status, 200, "reading the granted collection must still work");

    let res = server.post("/v1/db/shop/coll/orders/aggregate", Some(&limited), pipeline).await;
    assert_eq!(
        res.status, 403,
        "$lookup into an unreadable collection must be refused: {:?}",
        res.body
    );
    assert!(
        !format!("{:?}", res.body).contains("not for everyone"),
        "and must not leak the data it refused"
    );
}

#[tokio::test]
async fn array_operators_and_variables_work_over_http() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server
        .post(
            "/v1/db/shop/coll/orders/docs",
            Some(&token),
            json!({"_id": 1, "min": 2, "items": [
                {"sku": "a", "qty": 1}, {"sku": "b", "qty": 2}, {"sku": "c", "qty": 5}
            ]}),
        )
        .await;
    // No items at all: the null path, end to end.
    server.post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id": 2})).await;

    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [
                {"$project": {
                    "_id": 1,
                    "n": {"$size": "$items"},
                    "last": {"$last": "$items"},
                    "second": {"$arrayElemAt": [
                        {"$map": {"input": "$items", "in": "$$this.sku"}}, 1]},
                    "big": {"$filter": {"input": "$items", "as": "it",
                                        "cond": {"$gte": ["$$it.qty", "$min"]}}},
                    "skus": {"$map": {"input": "$items", "in": "$$this.sku"}},
                    "total": {"$reduce": {"input": "$items", "initialValue": 0,
                                          "in": {"$add": ["$$value", "$$this.qty"]}}},
                    "self": "$$ROOT._id",
                    "taxed": {"$let": {"vars": {"rate": 0.5},
                                       "in": {"$multiply": ["$min", "$$rate"]}}},
                }},
                {"$sort": {"_id": 1}}
            ]}),
        )
        .await;

    assert_eq!(res.status, 200, "{:?}", res.body);
    let docs = res.body["documents"].as_array().expect("documents");
    assert_eq!(docs.len(), 2);
    let d = &docs[0];
    assert_eq!(d["n"], 3);
    assert_eq!(d["last"], json!({"sku": "c", "qty": 5}));
    assert_eq!(d["second"], "b");
    assert_eq!(d["big"], json!([{"sku": "b", "qty": 2}, {"sku": "c", "qty": 5}]));
    assert_eq!(d["skus"], json!(["a", "b", "c"]));
    assert_eq!(d["total"], 8);
    assert_eq!(d["self"], 1);
    assert_eq!(d["taxed"], 1.0);
    // Null in, null out — not an error and not an empty array.
    let empty = &docs[1];
    assert_eq!(empty["n"], serde_json::Value::Null);
    assert_eq!(empty["big"], serde_json::Value::Null);
    assert_eq!(empty["total"], serde_json::Value::Null);

    // An unbound variable is a 400 naming it, before any document is read.
    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [{"$project": {"x": "$$nothing"}}]}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert!(format!("{:?}", res.body).contains("$$nothing"), "{:?}", res.body);
}

#[tokio::test]
async fn lookup_pipeline_form_binds_let_per_document_and_is_authorized() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"lines"})).await;
    for (id, min) in [(1, 5), (2, 1)] {
        server
            .post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id": id, "min": min}))
            .await;
    }
    for (id, order, qty, kind) in
        [(10, 1, 3, "line"), (11, 1, 9, "line"), (12, 2, 9, "line"), (13, 1, 9, "note")]
    {
        server
            .post(
                "/v1/db/shop/coll/lines/docs",
                Some(&token),
                json!({"_id": id, "order": order, "qty": qty, "kind": kind}),
            )
            .await;
    }

    // The correlation is written with the variable in a computed field; a
    // leading `$match` narrows the foreign side once for every input document.
    let pipeline = json!({"pipeline": [
        {"$lookup": {
            "from": "lines",
            "let": {"oid": "$_id", "min": "$min"},
            "pipeline": [
                {"$match": {"kind": "line"}},
                {"$addFields": {"mine": {"$eq": ["$order", "$$oid"]},
                                "over": {"$gte": ["$qty", "$$min"]}}},
                {"$match": {"mine": true}},
                {"$project": {"_id": 1, "over": 1}},
                {"$sort": {"_id": 1}}
            ],
            "as": "lines"
        }},
        {"$sort": {"_id": 1}}
    ]});
    let res =
        server.post("/v1/db/shop/coll/orders/aggregate", Some(&token), pipeline.clone()).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let docs = res.body["documents"].as_array().expect("documents");
    assert_eq!(docs[0]["lines"], json!([{"_id": 10, "over": false}, {"_id": 11, "over": true}]));
    assert_eq!(docs[1]["lines"], json!([{"_id": 12, "over": true}]));

    // Same boundary as the equality form: read on `orders` alone is refused.
    server
        .post(
            "/v1/users",
            Some(&token),
            json!({"user":"limited","password":"limited-password",
                   "grants":[{"db":"shop","collection":"orders","actions":["read"]}]}),
        )
        .await;
    let limited = server.login("limited", "limited-password").await;
    let res = server.post("/v1/db/shop/coll/orders/aggregate", Some(&limited), pipeline).await;
    assert_eq!(res.status, 403, "{:?}", res.body);

    // Mixing the two forms is a 400.
    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [{"$lookup": {
                "from": "lines", "localField": "_id", "foreignField": "order",
                "pipeline": [], "as": "lines"
            }}]}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
}

#[tokio::test]
async fn an_unknown_pipeline_stage_is_a_bad_request() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;

    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [{"$bucketAuto": {}}]}),
        )
        .await;
    // 400, not 501: an unknown stage is the same class as an unknown filter
    // operator — the pipeline as written is not valid for this server. 501 is
    // reserved for capabilities that are declared and deliberately unbuilt,
    // like `coordinated` unique enforcement.
    assert_eq!(res.status, 400, "{:?}", res.body);
    let body = format!("{:?}", res.body);
    assert!(body.contains("$bucketAuto"), "the error must name what was rejected: {body}");
    assert!(body.contains("$group"), "and list what is supported: {body}");
}

#[tokio::test]
async fn a_let_variable_in_a_sub_pipeline_match_is_a_bad_request() {
    // On 0.19.1 this was a 200 with `x: []` on every document: the filter
    // read `"$$oid"` as a literal string and matched nothing. It is refused
    // before any document is read, naming the variable.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"a"})).await;
    server.post("/v1/db/shop/coll/a/docs", Some(&token), json!({"_id": 1})).await;

    let res = server
        .post(
            "/v1/db/shop/coll/a/aggregate",
            Some(&token),
            json!({"pipeline": [
                {"$lookup": {"from": "a", "let": {"oid": "$_id"},
                             "pipeline": [{"$match": {"_id": "$$oid"}}], "as": "x"}},
                {"$limit": 1}
            ]}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert_eq!(res.body["error"], "bad_request");
    assert!(format!("{:?}", res.body).contains("$$oid"), "{:?}", res.body);
}

#[tokio::test]
async fn mod_pull_all_and_type_conversion_work_over_http() {
    // The three additions through the edge, where Extended JSON is what
    // carries a date or an ObjectId in and out.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server
        .post(
            "/v1/db/shop/coll/orders/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "city" }] }),
        )
        .await;
    for (id, city, qty, placed) in [
        (1, "london", "5", "2026-08-12T10:00:00Z"),
        (2, "london", "15", "2026-08-20"),
        (3, "paris", "ten", "2026-09-01T00:00:00Z"),
        (4, "paris", "8", "2026-09-02T00:00:00Z"),
    ] {
        server
            .post(
                "/v1/db/shop/coll/orders/docs",
                Some(&token),
                json!({"_id": id, "city": city, "qty": qty, "placed": placed, "tags": [1, 2, 3]}),
            )
            .await;
    }

    // $mod in a filter, and it is residual: the planner reports a scan.
    let res = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({ "filter": { "_id": { "$mod": [2, 0] } }, "sort": { "_id": 1 }, "explain": true }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["count"], 2, "{:?}", res.body);
    assert_eq!(res.body["documents"][0]["_id"], 2);
    assert_eq!(res.body["documents"][1]["_id"], 4);
    assert_eq!(res.body["explain"]["strategy"], "collectionScan");
    // A zero divisor is refused at parse.
    let res = server
        .post(
            "/v1/db/shop/coll/orders/find",
            Some(&token),
            json!({ "filter": { "_id": { "$mod": [0, 0] } } }),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);

    // $pullAll removes every listed value.
    let res = server
        .post(
            "/v1/db/shop/coll/orders/update",
            Some(&token),
            json!({ "filter": { "_id": 1 }, "update": { "$pullAll": { "tags": [1, 3] } } }),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let doc = server.get("/v1/db/shop/coll/orders/docs/1", Some(&token)).await;
    assert_eq!(doc.body["tags"], json!([2]));

    // A leading $match on the indexed field, then a conversion of a text
    // quantity with a fallback, and a group by the month of a text date.
    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [
                {"$match": {"city": {"$in": ["london", "paris"]}}},
                {"$addFields": {
                    "n": {"$convert": {"input": "$qty", "to": "int", "onError": 0}},
                    "month": {"$month": {"$toDate": "$placed"}},
                    "when": {"$toDate": "$placed"},
                    "idText": {"$toString": "$_id"},
                }},
                {"$group": {"_id": "$month", "total": {"$sum": "$n"},
                            "first": {"$first": "$when"}, "ids": {"$push": "$idText"}}},
                {"$sort": {"_id": 1}}
            ]}),
        )
        .await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    let docs = res.body["documents"].as_array().expect("documents");
    assert_eq!(docs.len(), 2, "{docs:?}");
    assert_eq!(docs[0]["_id"], 8);
    assert_eq!(docs[0]["total"], 20);
    assert_eq!(docs[0]["ids"], json!(["1", "2"]));
    // A date out is still a date on the wire.
    assert_eq!(docs[0]["first"]["$date"], 1_786_528_800_000i64);
    assert_eq!(docs[1]["_id"], 9);
    assert_eq!(docs[1]["total"], 8, "\"ten\" fell back to 0 through onError");

    // Without the fallback the unconvertible row is a 400 naming the problem.
    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [{"$addFields": {"n": {"$toInt": "$qty"}}}]}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
    assert!(format!("{:?}", res.body).contains("ten"), "{:?}", res.body);

    // And decimal is refused before a document is read.
    let res = server
        .post(
            "/v1/db/shop/coll/orders/aggregate",
            Some(&token),
            json!({"pipeline": [{"$addFields": {"n": {"$convert": {"input": "$qty", "to": "decimal"}}}}]}),
        )
        .await;
    assert_eq!(res.status, 400, "{:?}", res.body);
}

#[tokio::test]
async fn a_backup_requires_admin_over_everything() {
    // A backup is every document on the node, so anything less than full admin
    // would let a database-scoped administrator read past their own grants.
    // There is deliberately no grant-filtered backup: a partial backup that
    // looks whole is a restore that silently loses data.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server.post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id":1,"v":"present"})).await;

    server
        .post(
            "/v1/users",
            Some(&token),
            json!({"user":"dbadmin","password":"dbadmin-password",
                   "grants":[{"db":"shop","collection":"*","actions":["admin"]}]}),
        )
        .await;
    let scoped = server.login("dbadmin", "dbadmin-password").await;

    assert_eq!(
        server.get("/v1/admin/backup", Some(&scoped)).await.status,
        403,
        "a database-scoped admin must not be able to back up the whole node"
    );
    assert_eq!(server.get("/v1/admin/backup", None).await.status, 401);
}

#[tokio::test]
async fn a_backup_downloads_and_looks_like_a_backup() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;
    server.post("/v1/db/shop/coll/orders/docs", Some(&token), json!({"_id":1})).await;

    let res = server.get("/v1/admin/backup", Some(&token)).await;
    assert_eq!(res.status, 200);
    // The body is binary, so the JSON parse yields Null; the magic is what
    // matters and it is checked through the raw head plus content type.
    assert!(
        res.header("content-type").as_deref() == Some("application/octet-stream"),
        "head was: {}",
        res.head
    );
    assert!(
        res.header("content-disposition").is_some_and(|d| d.contains(".backup")),
        "a downloaded backup should arrive with a filename"
    );
}

#[tokio::test]
async fn the_metrics_endpoint_exposes_the_process_counters() {
    // Fetched as raw text: the client parses JSON, and /metrics is Prometheus
    // text, so this reads the socket directly.
    let server = Server::start().await;
    server.get("/v1/databases", None).await; // one 401

    let raw = {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let host = server.base.strip_prefix("http://").unwrap();
        let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
        let req = format!("GET /metrics HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    };

    for series in [
        "kimmy_up",
        "kimmy_storage_bytes",
        "kimmy_uptime_seconds",
        "kimmy_runtime_stall_seconds",
        "kimmy_requests_total",
        "kimmy_responses_total",
        "kimmy_authz_denied_total",
        "kimmy_auth_failures_total",
        "kimmy_rate_limited_total",
        "kimmy_backups_total",
    ] {
        assert!(raw.contains(series), "missing series {series} in:\n{raw}");
    }
    assert!(raw.contains("kimmy_auth_failures_total 1"), "the 401 should be counted:\n{raw}");
    assert!(
        !raw.contains("orders"),
        "metrics is unauthenticated and must not name collections:\n{raw}"
    );
}

#[tokio::test]
async fn the_metrics_body_exposes_exactly_these_series_in_exactly_this_order() {
    // The route's body is `Metrics::render` with the engine gauges prepended,
    // and `kimmy_storage_bytes` is a file size — not something a golden text
    // can hold. So this pins the *shape*: the exact ordered list of series
    // names, which is what a scrape config, a recording rule and a dashboard
    // panel each name. A series renamed or dropped is a panel that goes blank
    // and an alert that stops firing, and neither says anything when it
    // happens. The values themselves are pinned by the unit-level golden test
    // in `metrics.rs`.
    //
    // **Production clusters scrape this endpoint.** If this fails
    // because you meant to change the output, treat the diff as a release
    // note (ADR-070).
    let server = Server::start().await;

    let raw = {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let host = server.base.strip_prefix("http://").unwrap();
        let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
        let req = format!("GET /metrics HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    };
    let body = raw.split("\r\n\r\n").nth(1).expect("a response body");

    // Sample lines only, with labels and values stripped: what is left is the
    // series each line belongs to, in the order a scraper reads them.
    let series: Vec<&str> = body
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(|l| l.split(['{', ' ']).next().unwrap_or_default())
        .collect();

    assert_eq!(
        series,
        [
            "kimmy_databases",
            "kimmy_collections",
            "kimmy_unique_violations",
            "kimmy_commits",
            "kimmy_fsyncs",
            "kimmy_commits_grouped_total",
            "kimmy_storage_bytes",
            "kimmy_vector_index_cache_bytes",
            "kimmy_up",
            "kimmy_uptime_seconds",
            "kimmy_runtime_stall_seconds",
            "kimmy_requests_total",
            "kimmy_responses_total",
            "kimmy_responses_total",
            "kimmy_responses_total",
            "kimmy_authz_denied_total",
            "kimmy_auth_failures_total",
            "kimmy_rate_limited_total",
            "kimmy_rate_limited_principal_total",
            "kimmy_backups_total",
            "kimmy_ttl_expired_total",
            "kimmy_ttl_skipped_total",
            "kimmy_webhook_deliveries_total",
            "kimmy_webhook_deliveries_total",
            "kimmy_webhook_events_total",
            "kimmy_webhook_subscriptions",
            "kimmy_webhook_subscriptions",
            "kimmy_webhook_backlog_seconds",
            "kimmy_cluster_members",
            "kimmy_replication_lag_seconds",
            "kimmy_sync_failures_total",
            "kimmy_sync_peers_backing_off",
            "kimmy_sync_ddl_refused_total",
            "kimmy_tls_reloads_total",
            "kimmy_tls_reloads_total",
            "kimmy_jwks_refresh_total",
            "kimmy_jwks_refresh_total",
            "kimmy_embed_documents_total",
            "kimmy_embed_chunks_total",
            "kimmy_embed_deferred_total",
            "kimmy_embed_skipped_not_owned_total",
            "kimmy_embed_failures_total",
            "kimmy_embed_provider_errors_total",
            "kimmy_embed_provider_errors_total",
            "kimmy_embed_provider_errors_total",
            "kimmy_embed_provider_errors_total",
            "kimmy_embed_provider_requests_total",
            "kimmy_embed_provider_tokens_total",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_bucket",
            "kimmy_request_duration_seconds_sum",
            "kimmy_request_duration_seconds_count",
        ],
        "the /metrics series set or its order changed:\n{body}"
    );
}

#[tokio::test]
async fn health_probes_are_counted_but_not_timed() {
    // ADR-046's exclusion, which nothing checked: probes and scrapes fire every
    // few seconds forever, so timing them would crowd the buckets real traffic
    // lands in — but they must still show as traffic. Both halves matter, and
    // inverting the condition satisfies neither.
    let server = Server::start().await;

    let scrape = |server: &Server| {
        let base = server.base.clone();
        async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let host = base.strip_prefix("http://").unwrap();
            let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
            let req = format!("GET /metrics HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
            stream.write_all(req.as_bytes()).await.unwrap();
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf).into_owned()
        }
    };

    /// The value of a bare `name value` sample line.
    fn sample(raw: &str, name: &str) -> u64 {
        raw.lines()
            .find_map(|l| l.strip_prefix(&format!("{name} "))?.trim().parse().ok())
            .unwrap_or_else(|| panic!("no sample for {name} in:\n{raw}"))
    }

    let before = scrape(&server).await;
    let (timed_before, counted_before) = (
        sample(&before, "kimmy_request_duration_seconds_count"),
        sample(&before, "kimmy_requests_total"),
    );

    for _ in 0..5 {
        server.get("/healthz", None).await;
        server.get("/readyz", None).await;
    }

    let after = scrape(&server).await;
    assert_eq!(
        sample(&after, "kimmy_request_duration_seconds_count"),
        timed_before,
        "health probes must not enter the latency histogram"
    );
    assert!(
        sample(&after, "kimmy_requests_total") >= counted_before + 10,
        "...but they must still be counted as requests"
    );

    // And the other half of the condition: ordinary traffic *is* timed.
    let token = server.root().await;
    let timed_now = sample(&scrape(&server).await, "kimmy_request_duration_seconds_count");
    server.get("/v1/databases", Some(&token)).await;
    assert!(
        sample(&scrape(&server).await, "kimmy_request_duration_seconds_count") > timed_now,
        "a real request must be timed"
    );
}

// ---------------------------------------------------------------------------
// Webhook registration (M6)
// ---------------------------------------------------------------------------

/// Register a collection and a user holding exactly `actions` on it.
async fn with_scoped_user(server: &Server, actions: Value) -> (String, String) {
    let root = server.root().await;
    server.post("/v1/db/shop/collections", Some(&root), json!({"name":"orders"})).await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({"user":"scoped","password":"scoped-password",
                   "grants":[{"db":"shop","collection":"orders","actions":actions}]}),
        )
        .await;
    let scoped = server.login("scoped", "scoped-password").await;
    (root, scoped)
}

#[tokio::test]
async fn registering_a_webhook_returns_the_secret_exactly_once() {
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;

    let created = server
        .post(
            "/v1/db/shop/coll/orders/webhooks",
            Some(&token),
            json!({"url":"https://example.com/hook"}),
        )
        .await;
    assert_eq!(created.status, 200, "{:?}", created.body);
    let secret = created.body["secret"].as_str().expect("a secret at registration").to_string();
    assert!(secret.len() >= 64, "secret looks too short: {secret}");
    let id = created.body["id"].as_str().expect("an id").to_string();

    // ...and never again. Listing is the only other way to see a subscription,
    // so if the secret is anywhere it is here.
    let listed = server.get("/v1/db/shop/coll/orders/webhooks", Some(&token)).await;
    let rendered = listed.body.to_string();
    assert!(rendered.contains(&id), "the subscription should be listed: {rendered}");
    assert!(!rendered.contains(&secret), "the secret must never be retrievable: {rendered}");
    assert!(!rendered.contains("secret"), "not even the field: {rendered}");
}

#[tokio::test]
async fn registering_needs_the_webhook_action_and_watch_is_not_enough() {
    // The whole point of a separate action. A change stream ends with the
    // client and dies with its token; a webhook keeps sending to an address
    // the grant never named, long after that token expires.
    let server = Server::start().await;
    let (_root, watcher) = with_scoped_user(&server, json!(["read", "watch"])).await;

    let refused = server
        .post(
            "/v1/db/shop/coll/orders/webhooks",
            Some(&watcher),
            json!({"url":"https://example.com/hook"}),
        )
        .await;
    assert_eq!(refused.status, 403, "watch must not imply webhook: {:?}", refused.body);

    assert_eq!(
        server.get("/v1/db/shop/coll/orders/webhooks", Some(&watcher)).await.status,
        403,
        "nor should it allow listing them"
    );
}

#[tokio::test]
async fn the_webhook_action_grants_registration_without_granting_writes() {
    // And the converse: the action is independent, not a bundle.
    let server = Server::start().await;
    let (_root, hooker) = with_scoped_user(&server, json!(["read", "webhook"])).await;

    let created = server
        .post(
            "/v1/db/shop/coll/orders/webhooks",
            Some(&hooker),
            json!({"url":"https://example.com/hook"}),
        )
        .await;
    assert_eq!(created.status, 200, "{:?}", created.body);

    assert_eq!(
        server.post("/v1/db/shop/coll/orders/docs", Some(&hooker), json!({"_id": 1})).await.status,
        403,
        "registering a webhook must not have granted writing"
    );
}

#[tokio::test]
async fn a_webhook_pointed_at_the_metadata_endpoint_is_refused() {
    // Server-side request forgery, refused while the person who typed it is
    // watching rather than at the first delivery.
    let server = Server::start().await;
    let token = server.root().await;
    server.post("/v1/db/shop/collections", Some(&token), json!({"name":"orders"})).await;

    for url in [
        "http://169.254.169.254/latest/meta-data/",
        "http://127.0.0.1:7878/v1/databases",
        "http://10.0.0.5/hook",
        "file:///etc/passwd",
    ] {
        let refused = server
            .post("/v1/db/shop/coll/orders/webhooks", Some(&token), json!({"url": url}))
            .await;
        assert_eq!(refused.status, 400, "{url} should be refused: {:?}", refused.body);
    }

    let listed = server.get("/v1/db/shop/coll/orders/webhooks", Some(&token)).await;
    assert_eq!(listed.body["count"], 0, "nothing refused may have been stored");
}

#[tokio::test]
async fn a_webhook_can_only_be_removed_through_the_collection_it_belongs_to() {
    // Ids are guessable from a listing. Without this check, a caller with the
    // grant on one collection could delete another collection's subscription
    // by naming its id under their own.
    let server = Server::start().await;
    let token = server.root().await;
    for name in ["orders", "other"] {
        server.post("/v1/db/shop/collections", Some(&token), json!({"name": name})).await;
    }
    let created = server
        .post(
            "/v1/db/shop/coll/orders/webhooks",
            Some(&token),
            json!({"url":"https://example.com/hook"}),
        )
        .await;
    let id = created.body["id"].as_str().expect("an id").to_string();

    let wrong = server.delete(&format!("/v1/db/shop/coll/other/webhooks/{id}"), Some(&token)).await;
    assert_eq!(wrong.status, 404, "must not delete through the wrong collection");

    let right =
        server.delete(&format!("/v1/db/shop/coll/orders/webhooks/{id}"), Some(&token)).await;
    assert_eq!(right.status, 200, "{:?}", right.body);
    assert_eq!(server.get("/v1/db/shop/coll/orders/webhooks", Some(&token)).await.body["count"], 0);
}

#[tokio::test]
async fn registering_against_a_missing_collection_fails_now_rather_than_silently() {
    // Otherwise the subscription sits there delivering nothing, and the first
    // sign of trouble is someone asking why no events arrived.
    let server = Server::start().await;
    let token = server.root().await;
    let refused = server
        .post(
            "/v1/db/shop/coll/nosuch/webhooks",
            Some(&token),
            json!({"url":"https://example.com/hook"}),
        )
        .await;
    assert_eq!(refused.status, 404, "{:?}", refused.body);
}

// ---------------------------------------------------------------------------
// Federated identities
// ---------------------------------------------------------------------------

/// A stand-in identity provider: one fixed key pair, and tokens minted the way
/// a real provider would mint them.
mod oidc {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use kimmy_auth::{Jwk, JwkSet};
    use serde_json::{Value, json};

    pub const ISSUER: &str = "https://auth.example.com";
    pub const AUDIENCE: &str = "kimmydb";
    pub const KID: &str = "stub-key-1";

    /// A PKCS#8 P-256 private key. Fixed so the tests are deterministic and
    /// cost no key generation; it signs nothing outside this file.
    const EC_DER_B64: &str = concat!(
        "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgdYt6Sm2yyfFR8Bic5yJIzy6A",
        "Ra59sojVUjw/3t5rwyOhRANCAASTbia99nDdIMlZG1ND4yE0aYr4lybfQbD2whxMikG8lbsH",
        "O6OtfLKUpjzwvieZriD+AhtalEtnc1pXO6GvNSrL",
    );

    fn key() -> EncodingKey {
        use base64::Engine as _;
        let der = base64::engine::general_purpose::STANDARD.decode(EC_DER_B64).unwrap();
        EncodingKey::from_ec_der(&der)
    }

    /// The public half, as the provider would publish it.
    pub fn jwks() -> JwkSet {
        let mut jwk = Jwk::from_encoding_key(&key(), Algorithm::ES256).unwrap();
        jwk.common.key_id = Some(KID.to_string());
        JwkSet { keys: vec![jwk] }
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// The lifetime the stub provider mints. Ten minutes: inside the shipped
    /// limit, so every federated test also runs the default lifetime check.
    pub const LIFETIME_SECS: u64 = 600;

    pub fn claims(subject: &str, roles: Value) -> Value {
        let issued = now();
        json!({
            "sub": subject,
            "iss": ISSUER,
            "aud": AUDIENCE,
            "exp": issued + LIFETIME_SECS,
            "iat": issued,
            "roles": roles,
        })
    }

    /// A token the provider signed.
    pub fn token(claims: Value) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(KID.to_string());
        jsonwebtoken::encode(&header, &claims, &key()).unwrap()
    }

    /// A token with a chosen header, signed with a secret rather than the
    /// provider's key. For the algorithm-confusion cases.
    pub fn hmac_token(alg: Algorithm, claims: Value, secret: &str) -> String {
        let mut header = Header::new(alg);
        header.kid = Some(KID.to_string());
        jsonwebtoken::encode(&header, &claims, &EncodingKey::from_secret(secret.as_bytes()))
            .unwrap()
    }

    /// A syntactically valid token with an RS256 header and a signature that
    /// verifies against nothing. Enough to reach a verifier's algorithm check,
    /// which is the subject.
    pub fn unsigned_with_header(header_json: &str, claims: Value) -> String {
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        format!(
            "{}.{}.{}",
            b64.encode(header_json),
            b64.encode(serde_json::to_vec(&claims).unwrap()),
            b64.encode(b"not-a-signature"),
        )
    }
}

#[tokio::test]
async fn a_mapping_naming_a_stored_role_resolves_that_role_s_grants() {
    // The point of ADR-073: one role object both a local user and a federated
    // principal can point at, instead of the same permissions copied onto every
    // user record.
    let server = Server::start_federated_with_role("analyst", false).await;
    let root = server.root().await;
    server.post("/v1/db/sales/collections", Some(&root), json!({"name":"orders"})).await;
    server.post("/v1/db/sales/collections", Some(&root), json!({"name":"salaries"})).await;
    server
        .post(
            "/v1/roles",
            Some(&root),
            json!({
                "name": "analyst",
                "grants": [{"db":"sales","collection":"orders*","actions":["read"]}],
            }),
        )
        .await;

    let token = oidc::token(oidc::claims("ada@example.com", json!(["kimmydb-analyst"])));
    assert_eq!(
        server.get("/v1/db/sales/coll/orders/docs", Some(&token)).await.status,
        200,
        "the stored role's grants should apply"
    );
    assert_eq!(
        server.get("/v1/db/sales/coll/salaries/docs", Some(&token)).await.status,
        403,
        "and only those grants"
    );
}

#[tokio::test]
async fn editing_a_role_changes_the_next_federated_request_without_a_restart() {
    // The trap this feature exists to avoid. Resolving the mapping table once,
    // when the verifier is built, is the obvious cache and it silently freezes
    // every federated principal's permissions at startup — so a role edit would
    // do nothing until the node was restarted, which is the opposite of what
    // naming a stored role is for.
    //
    // Nothing here restarts anything: the same server, the same token.
    let server = Server::start_federated_with_role("analyst", false).await;
    let root = server.root().await;
    server.post("/v1/db/sales/collections", Some(&root), json!({"name":"orders"})).await;
    server.post("/v1/roles", Some(&root), json!({ "name": "analyst", "grants": [] })).await;

    let token = oidc::token(oidc::claims("ada@example.com", json!(["kimmydb-analyst"])));
    assert_eq!(
        server.get("/v1/db/sales/coll/orders/docs", Some(&token)).await.status,
        403,
        "an empty role grants nothing to begin with"
    );

    server
        .post(
            "/v1/roles/analyst/grants",
            Some(&root),
            json!({ "grants": [{"db":"sales","collection":"orders*","actions":["read"]}] }),
        )
        .await;

    assert_eq!(
        server.get("/v1/db/sales/coll/orders/docs", Some(&token)).await.status,
        200,
        "the widened role must apply to the very next request"
    );

    // And narrowing is honoured just as immediately, which is the direction
    // that actually matters for revocation.
    server.post("/v1/roles/analyst/grants", Some(&root), json!({ "grants": [] })).await;
    assert_eq!(
        server.get("/v1/db/sales/coll/orders/docs", Some(&token)).await.status,
        403,
        "the narrowed role must apply to the very next request too"
    );
}

#[tokio::test]
async fn a_mapping_naming_an_unknown_role_grants_nothing_and_is_not_an_error() {
    // A role can be deleted while a mapping still names it, and roles and users
    // are administered independently. Authenticating and being authorized for
    // nothing is the safe reading of that; refusing at the door would turn "your
    // administrator has not set this up yet" into "your login is broken".
    let server = Server::start_federated_with_role("never-created", false).await;
    let root = server.root().await;
    server.post("/v1/db/sales/collections", Some(&root), json!({"name":"orders"})).await;

    let token = oidc::token(oidc::claims("ada@example.com", json!(["kimmydb-analyst"])));
    let response = server.get("/v1/db/sales/coll/orders/docs", Some(&token)).await;
    assert_eq!(response.status, 403, "authenticated, authorized for nothing: {:?}", response.body);
}

#[tokio::test]
async fn a_federated_principal_does_not_get_admin_from_a_role_unless_it_is_allowed() {
    // ADR-067's break-glass boundary, in the one place a startup check cannot
    // reach it: a *stored* role can be edited to include `admin` long after the
    // node booted, so the check has to live where the role is resolved.
    //
    // Both halves are asserted against the same role, because the claim being
    // made is that the boundary is about the *principal*, not the role — the
    // local user holding it keeps its admin.
    let server = Server::start_federated_with_role("ops", false).await;
    let root = server.root().await;
    server
        .post(
            "/v1/roles",
            Some(&root),
            json!({
                "name": "ops",
                "grants": [{"db":"*","collection":"*","actions":["admin"]}],
            }),
        )
        .await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({ "user": "ada", "password": "a-good-password", "grants": [] }),
        )
        .await;
    server.post("/v1/users/ada/roles", Some(&root), json!({ "roles": ["ops"] })).await;

    let federated = oidc::token(oidc::claims("ada", json!(["kimmydb-analyst"])));
    assert_eq!(
        server.get("/v1/users", Some(&federated)).await.status,
        403,
        "`admin` is not federatable while allow_federated_admin is off"
    );

    let local = server.login("ada", "a-good-password").await;
    assert_eq!(
        server.get("/v1/users", Some(&local)).await.status,
        200,
        "the same role still gives a local user admin — the boundary is the principal"
    );
}

#[tokio::test]
async fn allow_federated_admin_is_what_changes_that_answer() {
    // The flag exists so the enterprise deployment stops being impossible, not
    // to change the default posture — so this is the only configuration in
    // which the previous test's answer flips.
    let server = Server::start_federated_with_role("ops", true).await;
    let root = server.root().await;
    server
        .post(
            "/v1/roles",
            Some(&root),
            json!({
                "name": "ops",
                "grants": [{"db":"*","collection":"*","actions":["admin"]}],
            }),
        )
        .await;

    let federated = oidc::token(oidc::claims("ada@example.com", json!(["kimmydb-analyst"])));
    assert_eq!(
        server.get("/v1/users", Some(&federated)).await.status,
        200,
        "with the flag on, a role may carry admin to a federated principal"
    );
}

#[tokio::test]
async fn a_federated_principal_keeps_ddl_from_a_role_while_admin_is_stripped() {
    // The reason `ddl` exists (ADR-090): an agent authenticating through an
    // identity provider must be able to create the collection it will write
    // to, and before the split the only action that allowed it was the one
    // ADR-067 refuses to federate. The same role carries both; only `admin`
    // is dropped.
    let server = Server::start_federated_with_role("builder", false).await;
    let root = server.root().await;
    server
        .post(
            "/v1/roles",
            Some(&root),
            json!({
                "name": "builder",
                "grants": [{"db":"*","collection":"*","actions":["ddl","write","admin"]}],
            }),
        )
        .await;

    let federated = oidc::token(oidc::claims("agent@example.com", json!(["kimmydb-analyst"])));
    let created = server
        .post("/v1/db/app/collections", Some(&federated), json!({ "name": "memories" }))
        .await;
    assert_eq!(created.status, 200, "ddl federates: {:?}", created.body);
    let inserted = server
        .post("/v1/db/app/coll/memories/docs", Some(&federated), json!({ "text": "hello" }))
        .await;
    assert_eq!(inserted.status, 200, "{:?}", inserted.body);

    assert_eq!(
        server.get("/v1/users", Some(&federated)).await.status,
        403,
        "`admin` is still stripped from the same role"
    );
    let system = server
        .post("/v1/db/__kimmy/collections", Some(&federated), json!({ "name": "__users" }))
        .await;
    assert_eq!(system.status, 403, "ddl over `*` must not reach the system database");
}

#[tokio::test]
async fn a_role_and_a_user_s_own_grants_are_a_union() {
    // Additive, always (ADR-073). Kubernetes RBAC is purely additive and
    // Postgres unions privileges across role membership; more to the point it
    // is the only rule that needs no rewrite of an existing user record.
    let server = Server::start().await;
    let root = server.root().await;
    server.post("/v1/db/sales/collections", Some(&root), json!({"name":"orders"})).await;
    server.post("/v1/db/sales/collections", Some(&root), json!({"name":"leads"})).await;
    server
        .post(
            "/v1/roles",
            Some(&root),
            json!({
                "name": "reader",
                "grants": [{"db":"sales","collection":"orders*","actions":["read"]}],
            }),
        )
        .await;
    server
        .post(
            "/v1/users",
            Some(&root),
            json!({
                "user": "ada",
                "password": "a-good-password",
                "grants": [{"db":"sales","collection":"leads","actions":["read"]}],
            }),
        )
        .await;
    server.post("/v1/users/ada/roles", Some(&root), json!({ "roles": ["reader"] })).await;

    let token = server.login("ada", "a-good-password").await;
    assert_eq!(
        server.get("/v1/db/sales/coll/orders/docs", Some(&token)).await.status,
        200,
        "the role's grant"
    );
    assert_eq!(
        server.get("/v1/db/sales/coll/leads/docs", Some(&token)).await.status,
        200,
        "and the user's own, both at once"
    );
}

#[tokio::test]
async fn a_federated_token_authorizes_exactly_the_grants_its_roles_map_to() {
    // The end-to-end shape: a token this cluster never issued, verified against
    // the provider's published key, becomes an ordinary principal that the same
    // RBAC check answers for.
    let server = Server::start_federated().await;
    let root = server.root().await;
    server.post("/v1/db/sales/collections", Some(&root), json!({"name":"orders"})).await;
    server.post("/v1/db/sales/collections", Some(&root), json!({"name":"salaries"})).await;

    let token = oidc::token(oidc::claims("ada@example.com", json!(["kimmydb-analyst"])));

    let allowed = server.get("/v1/db/sales/coll/orders/docs", Some(&token)).await;
    assert_eq!(allowed.status, 200, "{:?}", allowed.body);

    let denied = server.get("/v1/db/sales/coll/salaries/docs", Some(&token)).await;
    assert_eq!(denied.status, 403, "the mapping named orders*, not everything");

    let write =
        server.post("/v1/db/sales/coll/orders/docs", Some(&token), json!({"sku":"widget"})).await;
    assert_eq!(write.status, 403, "read must not imply write for a federated caller either");
}

#[tokio::test]
async fn a_federated_principal_needs_no_local_user_record() {
    // Revocation asymmetry (ADR-065): there is no `__users` row behind this
    // identity, and the absence of one is how the session check refuses a
    // deleted account. Without the skip, every federated request would be
    // refused as revoked.
    let server = Server::start_federated().await;
    let token = oidc::token(oidc::claims("nobody-here@example.com", json!(["kimmydb-analyst"])));

    let who = server.get("/v1/auth/whoami", Some(&token)).await;
    assert_eq!(who.status, 200, "{:?}", who.body);
    assert_eq!(who.body["user"], "nobody-here@example.com");
    // Flagged, so a client can tell this token apart from a local one — the
    // provider is free to assert a subject matching a local account.
    assert_eq!(who.body["federated"], true);
    assert_eq!(who.body["authenticated"], true);

    // ...and a local token is still the other thing.
    let local = server.get("/v1/auth/whoami", Some(&server.root().await)).await;
    assert_eq!(local.body["federated"], false);
}

#[tokio::test]
async fn a_federated_identity_with_no_mapped_role_is_authenticated_and_powerless() {
    // Authenticated but not authorized. The alternative — refusing the token —
    // reports "your login is broken" for what is really "your administrator has
    // not given you access to this database".
    let server = Server::start_federated().await;
    let root = server.root().await;
    server.post("/v1/db/sales/collections", Some(&root), json!({"name":"orders"})).await;

    let token = oidc::token(oidc::claims("ada@example.com", json!(["some-other-app-role"])));

    let who = server.get("/v1/auth/whoami", Some(&token)).await;
    assert_eq!(who.status, 200, "the token is good; the grants are empty");
    assert_eq!(who.body["grants"], json!([]));
    assert_eq!(server.get("/v1/db/sales/coll/orders/docs", Some(&token)).await.status, 403);
}

#[tokio::test]
async fn a_locally_signed_token_claiming_the_external_issuer_is_refused() {
    // The routing decision, attacked from the inside. Anyone holding a token
    // this cluster issued could add `iss` to it — except that changing the
    // payload breaks the HS256 signature, and a token that *does* carry the
    // external issuer is sent to the OIDC verifier, which will not accept HS256
    // and does not hold the cluster secret anyway.
    let server = Server::start_federated().await;
    let forged = oidc::hmac_token(
        jsonwebtoken::Algorithm::HS256,
        oidc::claims("root", json!(["kimmydb-analyst"])),
        SECRET,
    );

    let res = server.get("/v1/auth/whoami", Some(&forged)).await;
    assert_eq!(res.status, 401, "{:?}", res.body);
}

#[tokio::test]
async fn an_rs256_header_on_the_local_path_is_refused() {
    // Algorithm confusion, the other direction: a token naming no external
    // issuer routes to the HS256 verifier, which pins its own algorithm rather
    // than reading one out of the header.
    let server = Server::start_federated().await;
    let mut claims = oidc::claims("root", json!([]));
    claims.as_object_mut().unwrap().remove("iss");
    let forged =
        oidc::unsigned_with_header(r#"{"alg":"RS256","typ":"JWT","kid":"stub-key-1"}"#, claims);

    let res = server.get("/v1/auth/whoami", Some(&forged)).await;
    assert_eq!(res.status, 401, "{:?}", res.body);
}

#[tokio::test]
async fn a_token_from_a_third_issuer_is_refused_by_both_verifiers() {
    // Correctly signed by the provider's key, but naming an issuer this node
    // does not federate with. It routes to the local path — which does not hold
    // that key — and is refused there. The point is that "signed by somebody"
    // is never enough on either path.
    let server = Server::start_federated().await;
    let mut claims = oidc::claims("ada@example.com", json!(["kimmydb-analyst"]));
    claims["iss"] = json!("https://some-other-idp.example.com");

    let res = server.get("/v1/auth/whoami", Some(&oidc::token(claims))).await;
    assert_eq!(res.status, 401, "{:?}", res.body);
}

#[tokio::test]
async fn a_token_for_another_audience_is_refused() {
    // The provider signs for every application that trusts it, so only the
    // audience separates a token minted for the company wiki from one minted
    // for this database.
    let server = Server::start_federated().await;
    let mut claims = oidc::claims("ada@example.com", json!(["kimmydb-analyst"]));
    claims["aud"] = json!("the-company-wiki");

    let res = server.get("/v1/auth/whoami", Some(&oidc::token(claims))).await;
    assert_eq!(res.status, 401, "{:?}", res.body);
}

#[tokio::test]
async fn a_token_signed_by_a_key_the_node_has_not_fetched_is_refused_without_naming_it() {
    // Key rotation reaches the request path as a plain 401: which key ids this
    // node holds is not something an unauthenticated caller should be able to
    // learn by guessing. The recovery happens behind the response.
    let server = Server::start_federated().await;
    let claims = oidc::claims("ada@example.com", json!(["kimmydb-analyst"]));
    let token =
        oidc::unsigned_with_header(r#"{"alg":"ES256","typ":"JWT","kid":"rotated-key-2"}"#, claims);

    let res = server.get("/v1/auth/whoami", Some(&token)).await;
    assert_eq!(res.status, 401, "{:?}", res.body);
    let message = res.body["message"].as_str().unwrap_or_default();
    assert!(!message.contains("rotated-key-2"), "the message must not echo the key id: {message}");
}

#[tokio::test]
async fn a_node_without_federation_configured_treats_every_token_as_local() {
    // The default deployment. Nothing about adding the feature may change what
    // a node that does not use it does with a token.
    let server = Server::start().await;
    let token = oidc::token(oidc::claims("ada@example.com", json!(["kimmydb-analyst"])));
    assert_eq!(server.get("/v1/auth/whoami", Some(&token)).await.status, 401);

    // ...and a local login still works exactly as it did.
    let who = server.get("/v1/auth/whoami", Some(&server.root().await)).await;
    assert_eq!(who.status, 200);
    assert_eq!(who.body["federated"], false);
}

#[tokio::test]
async fn an_hour_long_federated_token_authenticates() {
    // The exact token ADR-096 refused, and the exact shape of the outage that
    // removing it was about: a provider minting hour-long access tokens — the
    // default for Okta, Google and Entra ID — against a node with no special
    // configuration. It authenticates (ADR-112).
    //
    // How long the provider made the token valid for is its business. The
    // window in which a revocation there goes unhonoured here is real
    // (ADR-073) and is documented in docs/security.md; it is not this node's
    // to refuse, and nothing in the response mentions it.
    let server = Server::start_federated_for(RESOURCE).await;
    let mut claims = oidc::claims("ada@example.com", json!(["kimmydb-analyst"]));
    claims["aud"] = json!(RESOURCE);
    claims["exp"] = json!(claims["iat"].as_u64().unwrap() + 3600);

    let res = server.get("/v1/auth/whoami", Some(&oidc::token(claims))).await;
    assert_eq!(res.status, 200, "an hour-long token must authenticate: {:?}", res.body);
    assert_eq!(res.body["federated"], true);
    assert!(res.header("www-authenticate").is_none(), "a success carries no challenge");
}

#[tokio::test]
async fn a_day_long_federated_token_authenticates_too() {
    // Auth0 defaults an API's access tokens to a day. Refusing it was the
    // clearest case ADR-096 argued for and the clearest case against it: the
    // lifetime is set at the provider by an operator who is entitled to
    // choose it, and a database is not where that decision gets overridden.
    let server = Server::start_federated_for(RESOURCE).await;
    let mut claims = oidc::claims("ada@example.com", json!(["kimmydb-analyst"]));
    claims["aud"] = json!(RESOURCE);
    claims["exp"] = json!(claims["iat"].as_u64().unwrap() + 86_400);

    let res = server.get("/v1/auth/whoami", Some(&oidc::token(claims))).await;
    assert_eq!(res.status, 200, "a day-long token must authenticate: {:?}", res.body);
}

#[tokio::test]
async fn a_federated_token_with_no_iat_authenticates() {
    // RFC 9068 §2.2 requires `iat`, but nothing reads it here any more — it
    // existed to measure a lifetime against a bound that no longer exists. A
    // provider that omits it is outside the profile and is still not this
    // node's to refuse (ADR-112).
    let server = Server::start_federated().await;
    let mut claims = oidc::claims("ada@example.com", json!(["kimmydb-analyst"]));
    claims.as_object_mut().unwrap().remove("iat");

    let res = server.get("/v1/auth/whoami", Some(&oidc::token(claims))).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
}

// ---------------------------------------------------------------------------
// The display name (ADR-100)
// ---------------------------------------------------------------------------

/// A subject shaped like a real provider's: opaque, and nothing a person
/// would recognise in an audit line.
const OPAQUE_SUBJECT: &str = "3f2a9c1e-7b4d-4e0a-9c1e-0f1e2d3c4b5a";

fn opaque_claims(email: Option<&str>) -> Value {
    let mut claims = oidc::claims(OPAQUE_SUBJECT, json!(["kimmydb-analyst"]));
    if let Some(email) = email {
        claims["email"] = json!(email);
    }
    claims
}

#[tokio::test]
async fn whoami_reports_the_display_name_beside_the_identity() {
    let server = Server::start_federated_with_subject_claim("email").await;

    let token = oidc::token(opaque_claims(Some("ada@example.com")));
    let who = server.get("/v1/auth/whoami", Some(&token)).await;
    assert_eq!(who.status, 200, "{:?}", who.body);
    // Both, and in that order of importance: `user` is what every decision
    // was made on, `display` is what a person reads.
    assert_eq!(who.body["user"], OPAQUE_SUBJECT);
    assert_eq!(who.body["display"], "ada@example.com");
    assert_eq!(who.body["federated"], true);

    // A token the provider minted without the claim is not refused; it just
    // has no better name than its subject.
    let who = server.get("/v1/auth/whoami", Some(&oidc::token(opaque_claims(None)))).await;
    assert_eq!(who.status, 200, "{:?}", who.body);
    assert_eq!(who.body["display"], OPAQUE_SUBJECT);

    // A local caller's display is its user name, always.
    let who = server.get("/v1/auth/whoami", Some(&server.root().await)).await;
    assert_eq!(who.body["user"], "root");
    assert_eq!(who.body["display"], "root");
}

#[tokio::test]
async fn without_a_subject_claim_the_display_is_the_subject_even_when_the_token_carries_one() {
    // The shipped default. A node that did not ask reads nothing extra out of
    // the token, so nothing about adding the setting changes what it shows.
    let server = Server::start_federated().await;
    let who = server
        .get("/v1/auth/whoami", Some(&oidc::token(opaque_claims(Some("ada@example.com")))))
        .await;
    assert_eq!(who.status, 200, "{:?}", who.body);
    assert_eq!(who.body["display"], OPAQUE_SUBJECT);
}

#[tokio::test]
async fn a_renamed_email_keeps_the_same_grants_because_the_identity_is_the_subject() {
    // The property the whole decision rests on (ADR-100). Two tokens for one
    // subject, minted before and after a rename at the provider, are the same
    // principal for every purpose but presentation — the display is never an
    // input to authorization.
    let server = Server::start_federated_with_subject_claim("email").await;
    let root = server.root().await;
    server.post("/v1/db/sales/collections", Some(&root), json!({"name":"orders"})).await;

    let before = oidc::token(opaque_claims(Some("ada@example.com")));
    let after = oidc::token(opaque_claims(Some("ada.lovelace@example.com")));

    for token in [&before, &after] {
        assert_eq!(server.get("/v1/db/sales/coll/orders/docs", Some(token)).await.status, 200);
    }
    let was = server.get("/v1/auth/whoami", Some(&before)).await.body;
    let now = server.get("/v1/auth/whoami", Some(&after)).await.body;
    assert_eq!(was["user"], now["user"], "one subject, one identity");
    assert_eq!(was["grants"], now["grants"], "and one set of grants");
    assert_ne!(was["display"], now["display"], "only the presentation moved");
}

// ---------------------------------------------------------------------------
// Local login modes (ADR-100)
// ---------------------------------------------------------------------------

/// A peer that is on some network and not on this host.
const OFF_HOST: &str = "203.0.113.9:4000";

fn credentials() -> Value {
    json!({ "user": "root", "password": ROOT_PASSWORD })
}

#[tokio::test]
async fn local_login_answers_every_peer_by_default() {
    // Nothing about adding the mode may change what a node that did not set
    // it does: the socket peer (loopback) and a peer off the host both log in.
    let server = Server::start().await;
    assert_eq!(server.state.local_login(), kimmy_api::LocalLogin::Always);
    assert_eq!(server.post("/v1/auth/login", None, credentials()).await.status, 200);
    let res =
        server.request_from(OFF_HOST, "POST", "/v1/auth/login", None, Some(credentials())).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert!(res.body["token"].is_string());
}

#[tokio::test]
async fn loopback_only_admits_the_host_and_refuses_the_network_with_a_403() {
    let server = Server::start().await;
    server.state.set_local_login(kimmy_api::LocalLogin::LoopbackOnly);

    // The host, both families.
    assert_eq!(server.post("/v1/auth/login", None, credentials()).await.status, 200);
    let v6 = server
        .request_from("[::1]:4000", "POST", "/v1/auth/login", None, Some(credentials()))
        .await;
    assert_eq!(v6.status, 200, "{:?}", v6.body);

    // The network: refused before the password is looked at, in the API's own
    // envelope, with the setting named so the caller knows it is policy and
    // not a wrong password.
    let res =
        server.request_from(OFF_HOST, "POST", "/v1/auth/login", None, Some(credentials())).await;
    assert_eq!(res.status, 403, "{:?}", res.body);
    assert_eq!(res.body["error"], "forbidden");
    assert_eq!(res.body["retry"], "no");
    let message = res.body["message"].as_str().unwrap_or_default();
    assert!(message.contains("auth.local.login"), "{message}");

    // A private address is still not the host.
    let lan = server
        .request_from("10.0.0.5:4000", "POST", "/v1/auth/login", None, Some(credentials()))
        .await;
    assert_eq!(lan.status, 403, "{:?}", lan.body);
}

#[tokio::test]
async fn loopback_only_governs_minting_and_not_verifying() {
    // The property an operator relies on when flipping the mode on a live
    // node: nobody's session ends, and a token minted from the host works
    // from anywhere. Only *renewing* it is confined to the host, because
    // refresh mints too.
    let server = Server::start().await;
    server.state.set_local_login(kimmy_api::LocalLogin::LoopbackOnly);
    let token = server.root().await;

    let who = server.request_from(OFF_HOST, "GET", "/v1/auth/whoami", Some(&token), None).await;
    assert_eq!(who.status, 200, "a token already issued verifies off the host: {:?}", who.body);
    assert_eq!(who.body["user"], "root");

    let refreshed =
        server.request_from(OFF_HOST, "POST", "/v1/auth/refresh", Some(&token), None).await;
    assert_eq!(refreshed.status, 403, "{:?}", refreshed.body);
    assert_eq!(refreshed.body["error"], "forbidden");
    // Not an RBAC refusal, so not an `insufficient_scope` challenge: a header
    // saying the principal lacks a grant would describe a refusal that did
    // not happen.
    assert!(
        !refreshed.head.to_ascii_lowercase().contains("www-authenticate"),
        "a refusal by mode is not a bearer challenge: {}",
        refreshed.head
    );

    let refreshed = server.post("/v1/auth/refresh", Some(&token), json!({})).await;
    assert_eq!(refreshed.status, 200, "from the host, refresh still works: {:?}", refreshed.body);
    assert!(refreshed.body["token"].is_string());
}

#[tokio::test]
async fn disabled_answers_404_from_everywhere_and_issued_tokens_keep_working() {
    let server = Server::start().await;
    // Minted before the door closes: the mode is fixed for the life of a
    // process, so this is the order a real deployment sees across a restart
    // with a still-valid token in hand.
    let token = server.root().await;
    server.state.set_local_login(kimmy_api::LocalLogin::Disabled);

    for res in [
        server.post("/v1/auth/login", None, credentials()).await,
        server.request_from(OFF_HOST, "POST", "/v1/auth/login", None, Some(credentials())).await,
    ] {
        assert_eq!(res.status, 404, "{:?}", res.body);
        assert_eq!(res.body["error"], "not_found");
        assert!(res.body["message"].as_str().unwrap_or_default().contains("disabled"));
    }

    // Refresh is a 404 too, and the mode is checked *before* the token — a
    // caller with none is not told to go and fetch one by a route that does
    // not exist for them.
    assert_eq!(server.post("/v1/auth/refresh", Some(&token), json!({})).await.status, 404);
    let bare = server.post("/v1/auth/refresh", None, json!({})).await;
    assert_eq!(bare.status, 404, "{:?}", bare.body);

    // The token itself is untouched.
    let who = server.get("/v1/auth/whoami", Some(&token)).await;
    assert_eq!(who.status, 200, "{:?}", who.body);
    assert_eq!(who.body["user"], "root");
}

#[tokio::test]
async fn disabling_local_login_leaves_the_identity_provider_as_the_way_in() {
    // The configuration `disabled` exists for, and the one startup allows it
    // in: a federated caller is unaffected, because nothing about the mode
    // touches the verifier a federated token is offered to.
    let server = Server::start_federated().await;
    server.state.set_local_login(kimmy_api::LocalLogin::Disabled);

    let token = oidc::token(oidc::claims("ada@example.com", json!(["kimmydb-analyst"])));
    let who = server.get("/v1/auth/whoami", Some(&token)).await;
    assert_eq!(who.status, 200, "{:?}", who.body);
    assert_eq!(who.body["federated"], true);

    assert_eq!(server.post("/v1/auth/login", None, credentials()).await.status, 404);
}

#[tokio::test]
async fn a_refusal_by_mode_is_not_a_failed_login_for_the_limiter() {
    // A refusal by policy costs no Argon2 work and reveals nothing about a
    // password, so it must not spend the caller's failure budget: an operator
    // who later reopens the route would otherwise find the host's neighbours
    // pre-throttled by requests that were never attempts.
    let server = Server::start_rate_limited(1).await;
    server.state.set_local_login(kimmy_api::LocalLogin::LoopbackOnly);
    for _ in 0..3 {
        let res = server
            .request_from(OFF_HOST, "POST", "/v1/auth/login", None, Some(credentials()))
            .await;
        assert_eq!(res.status, 403, "never 429: {:?}", res.body);
    }
}

// ---------------------------------------------------------------------------
// Resource identity: RFC 9728 metadata and RFC 6750 challenges (ADR-071)
// ---------------------------------------------------------------------------

/// The audience a node uses when it names itself as a protected resource.
const RESOURCE: &str = "https://kimmydb.example.com";

#[tokio::test]
async fn a_node_with_a_url_audience_publishes_protected_resource_metadata() {
    // The document a client reads to find out where to authenticate. Its whole
    // point is that `kimmy login --oidc --url ...` needs nothing else set.
    let server = Server::start_federated_for(RESOURCE).await;
    let res = server.get("/.well-known/oauth-protected-resource", None).await;

    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["resource"], RESOURCE);
    assert_eq!(res.body["authorization_servers"][0], oidc::ISSUER);
    assert_eq!(res.body["bearer_methods_supported"][0], "header");

    // Authorization here is roles carried in the token, never scopes.
    // Advertising a scope vocabulary would describe a model this database does
    // not implement, and a client that asked for those scopes would get them
    // and still be refused.
    assert!(
        res.body.get("scopes_supported").is_none(),
        "scopes must not be advertised: {:?}",
        res.body
    );
}

#[test]
fn the_well_known_route_matches_the_shared_constant() {
    // The router registers this path as a literal so the documentation contract
    // in tests/openapi.rs can see it, while the challenge header and the startup
    // log build their URLs from the constant. This is what keeps the two from
    // drifting into a node that advertises a document it does not serve.
    assert_eq!(
        kimmy_auth::PROTECTED_RESOURCE_METADATA_PATH,
        "/.well-known/oauth-protected-resource"
    );
}

#[tokio::test]
async fn the_metadata_is_unauthenticated() {
    // A client that has no token is exactly who needs this document, so
    // requiring one would make it useless.
    let server = Server::start_federated_for(RESOURCE).await;
    assert_eq!(server.get("/.well-known/oauth-protected-resource", None).await.status, 200);
}

#[tokio::test]
async fn an_opaque_audience_publishes_no_metadata() {
    // `audience = "kimmydb"` is a supported configuration, not a broken one --
    // it is what shipped first and what a provider with no RFC 8707 support
    // needs. There is simply nothing truthful to publish for it, and publishing
    // an identifier no token will ever carry would send every client to ask its
    // provider for a resource the provider refuses.
    let server = Server::start_federated_for("kimmydb").await;
    assert_eq!(server.get("/.well-known/oauth-protected-resource", None).await.status, 404);
}

#[tokio::test]
async fn a_node_without_federation_publishes_no_metadata() {
    let server = Server::start().await;
    assert_eq!(server.get("/.well-known/oauth-protected-resource", None).await.status, 404);
}

#[tokio::test]
async fn metadata_is_served_only_for_this_nodes_own_resource() {
    // RFC 9728 §3 puts the well-known segment between the authority and the
    // path, so the route has to accept a suffix -- and then answering for a
    // suffix that is not this node's resource would be a lie a client acts on.
    let server = Server::start_federated_for(RESOURCE).await;
    assert_eq!(
        server.get("/.well-known/oauth-protected-resource/somebody-else", None).await.status,
        404
    );
}

#[tokio::test]
async fn a_resource_identifier_with_a_path_is_served_one_level_down() {
    let server = Server::start_federated_for("https://kimmydb.example.com/nodes/one").await;

    // Not at the bare well-known path: that would be a different resource.
    assert_eq!(server.get("/.well-known/oauth-protected-resource", None).await.status, 404);

    let res = server.get("/.well-known/oauth-protected-resource/nodes/one", None).await;
    assert_eq!(res.status, 200, "{:?}", res.body);
    assert_eq!(res.body["resource"], "https://kimmydb.example.com/nodes/one");
}

#[tokio::test]
async fn a_request_with_no_credentials_is_challenged_without_an_error_code() {
    // RFC 6750 §3: a client that has not yet tried must not be told it failed.
    // The distinction is not cosmetic -- `invalid_token` tells a client to
    // refresh and retry, which is wrong advice for one that has no token.
    let server = Server::start_federated_for(RESOURCE).await;
    let res = server.get("/v1/auth/whoami", None).await;

    assert_eq!(res.status, 401);
    let challenge = res.header("www-authenticate").expect("RFC 6750 §3 makes this a MUST");
    assert!(challenge.starts_with("Bearer "), "{challenge}");
    assert!(!challenge.contains("error="), "an untried client has not failed: {challenge}");
    assert!(
        challenge.contains(&format!(
            r#"resource_metadata="{RESOURCE}/.well-known/oauth-protected-resource""#
        )),
        "the challenge must point at the metadata document: {challenge}"
    );
}

#[tokio::test]
async fn a_request_with_a_bad_token_is_challenged_with_invalid_token() {
    let server = Server::start_federated_for(RESOURCE).await;
    let res = server.get("/v1/auth/whoami", Some("not-a-token")).await;

    assert_eq!(res.status, 401);
    let challenge = res.header("www-authenticate").expect("a challenge");
    assert!(challenge.contains(r#"error="invalid_token""#), "{challenge}");
}

#[tokio::test]
async fn a_denied_request_is_challenged_with_insufficient_scope() {
    // RFC 6750 §3.1 pairs this code with 403. It says nothing the body does not
    // already say -- see the next test, which is the one that matters.
    let server = Server::start_federated().await;
    let token = oidc::token(oidc::claims("ada@example.com", json!(["kimmydb-analyst"])));
    let res = server.post("/v1/db/payroll/collections", Some(&token), json!({"name": "salaries"}));

    let res = res.await;
    assert_eq!(res.status, 403, "{:?}", res.body);
    let challenge = res.header("www-authenticate").expect("a challenge");
    assert!(challenge.contains(r#"error="insufficient_scope""#), "{challenge}");
}

#[tokio::test]
async fn the_challenge_does_not_distinguish_a_missing_target_from_a_forbidden_one() {
    // The uniform-403 property, re-checked now that 403 carries a header. A
    // caller who cannot read `sales` must not be able to learn from the
    // challenge whether `sales.orders` exists -- otherwise the header has
    // quietly become the probe the body was written to prevent.
    let server = Server::start_federated().await;
    let root = server.root().await;
    // Real, and outside the analyst's `orders*` grant -- so the only difference
    // between the two requests below is that one target exists.
    let made =
        server.post("/v1/db/sales/collections", Some(&root), json!({ "name": "secrets" })).await;
    assert_eq!(made.status, 200, "{:?}", made.body);

    let token = oidc::token(oidc::claims("ada@example.com", json!(["kimmydb-analyst"])));
    let missing = server.post("/v1/db/sales/coll/nonexistent/find", Some(&token), json!({})).await;
    let existing = server.post("/v1/db/sales/coll/secrets/find", Some(&token), json!({})).await;

    assert_eq!(missing.status, 403);
    assert_eq!(existing.status, 403);
    assert_eq!(
        missing.header("www-authenticate"),
        existing.header("www-authenticate"),
        "the challenge must not reveal which target exists"
    );
    assert_eq!(missing.body["error"], existing.body["error"]);
}

#[tokio::test]
async fn a_failed_password_login_is_not_challenged_for_a_bearer_token() {
    // `/v1/auth/login` is where a token comes from, not a bearer-protected
    // resource. Telling a client to come back with a token would be advice it
    // cannot act on and the opposite of what it should do.
    let server = Server::start_federated_for(RESOURCE).await;
    let res =
        server.post("/v1/auth/login", None, json!({"user": "root", "password": "wrong"})).await;

    assert_eq!(res.status, 401);
    assert_eq!(res.header("www-authenticate"), None, "a login failure is not a bearer challenge");
}

#[tokio::test]
async fn an_opaque_audience_still_challenges_but_names_no_metadata() {
    // The header is required regardless (RFC 6750 §3). Only the RFC 9728
    // pointer depends on this node having a name an authorization server knows.
    let server = Server::start_federated_for("kimmydb").await;
    let res = server.get("/v1/auth/whoami", None).await;

    assert_eq!(res.status, 401);
    let challenge = res.header("www-authenticate").expect("still a MUST");
    assert!(challenge.starts_with("Bearer "), "{challenge}");
    assert!(!challenge.contains("resource_metadata="), "there is no document to name: {challenge}");
}

#[tokio::test]
async fn a_search_filter_uses_the_index_and_returns_the_filtered_top_k() {
    // The filter is planned like a `find` — here through an index on `tag` —
    // and, admitting a small set, is joined by reading those documents'
    // chunks. The result must be the filtered nearest neighbours in order,
    // text included, for vector and hybrid search alike; and `find` with
    // `explain` on the same filter shows the strategy the search got.
    let server = Server::start().await;
    let token = byo_collection(&server).await;
    let created = server
        .post(
            "/v1/db/shop/coll/docs/indexes",
            Some(&token),
            json!({ "fields": [{ "path": "tag" }] }),
        )
        .await;
    assert_eq!(created.status, 200, "{:?}", created.body);

    // Nearest to the query first: a, b, c, d, e. Only b, d and e carry the tag.
    let docs = [
        ("a", "other", [1.0, 0.0, 0.0]),
        ("b", "wanted", [0.9, 0.1, 0.0]),
        ("c", "other", [0.8, 0.2, 0.0]),
        ("d", "wanted", [0.7, 0.3, 0.0]),
        ("e", "wanted", [0.0, 1.0, 0.0]),
    ];
    for (id, tag, vector) in docs {
        server
            .post(
                "/v1/db/shop/coll/docs/docs",
                Some(&token),
                json!({ "_id": id, "tag": tag, "text": format!("text of {id}") }),
            )
            .await;
        let stored = server
            .put(
                &format!("/v1/db/shop/coll/docs/docs/{id}/vectors"),
                Some(&token),
                json!([{ "chunk": 0, "vector": vector, "text": format!("text of {id}") }]),
            )
            .await;
        assert_eq!(stored.status, 200, "{:?}", stored.body);
    }

    let explained = server
        .post(
            "/v1/db/shop/coll/docs/find",
            Some(&token),
            json!({ "filter": { "tag": "wanted" }, "explain": true }),
        )
        .await;
    assert_eq!(explained.body["explain"]["strategy"], "index", "{:?}", explained.body);

    let found = server
        .post(
            "/v1/db/shop/coll/docs/vector_search",
            Some(&token),
            json!({ "vector": [1.0, 0.0, 0.0], "k": 2, "filter": { "tag": "wanted" } }),
        )
        .await;
    assert_eq!(found.status, 200, "{:?}", found.body);
    let matches = found.body["matches"].as_array().unwrap();
    let ids: Vec<&str> = matches.iter().map(|m| m["_id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["b", "d"], "the two nearest *tagged* documents: {:?}", found.body);
    assert_eq!(matches[0]["text"], "text of b", "the text travels with the hit");
    assert!(matches[0]["score"].as_f64().unwrap() > matches[1]["score"].as_f64().unwrap());

    // A filter admitting nothing returns nothing rather than failing.
    let none = server
        .post(
            "/v1/db/shop/coll/docs/vector_search",
            Some(&token),
            json!({ "vector": [1.0, 0.0, 0.0], "k": 2, "filter": { "tag": "absent" } }),
        )
        .await;
    assert_eq!(none.status, 200, "{:?}", none.body);
    assert_eq!(none.body["count"], 0);

    // Hybrid's dense half is filtered the same way; "e" is the keyword and
    // is tagged, so it fuses in, while "a" and "c" cannot appear.
    let hybrid = server
        .post(
            "/v1/db/shop/coll/docs/hybrid_search",
            Some(&token),
            json!({ "query": "e", "vector": [1.0, 0.0, 0.0], "k": 3, "filter": { "tag": "wanted" } }),
        )
        .await;
    assert_eq!(hybrid.status, 200, "{:?}", hybrid.body);
    let ids: Vec<&str> = hybrid.body["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["_id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"b") && ids.contains(&"e"), "{:?}", hybrid.body);
    assert!(!ids.contains(&"a") && !ids.contains(&"c"), "{:?}", hybrid.body);
}
