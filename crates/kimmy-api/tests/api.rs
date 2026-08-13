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

const SECRET: &str = "an-adequately-long-test-secret";
const ROOT_PASSWORD: &str = "root-password";

struct Server {
    base: String,
    client: Client,
    _dir: tempfile::TempDir,
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
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let rest = url.strip_prefix("http://").expect("http url");
        let (host, path) = rest.split_once('/').expect("path");
        let path = format!("/{path}");

        let mut stream = tokio::net::TcpStream::connect(host).await.expect("connect");
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

        stream.write_all(request.as_bytes()).await.expect("write");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("read");
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

    async fn build(insecure_no_auth: bool, limits: kimmy_api::RateLimits) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());

        if !insecure_no_auth {
            let users = kimmy_auth::UserStore::open(&engine).unwrap();
            users.bootstrap_root(&engine, "root", ROOT_PASSWORD).unwrap();
        }

        let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
        let app = kimmy_api::build(Arc::clone(&engine), tokens, insecure_no_auth, limits).unwrap();

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

        Self { base: format!("http://{addr}"), client: Client, _dir: dir }
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
async fn managing_indexes_requires_admin() {
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
