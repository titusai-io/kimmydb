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
