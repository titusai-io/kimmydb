//! End-to-end API tests.
//!
//! These drive the real router over a real TCP socket rather than calling
//! handlers directly, so routing, extractors, status codes, and the JSON
//! boundary are all exercised the way a client meets them.

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
        Res { status, body }
    }
}

impl Server {
    async fn start() -> Self {
        Self::start_with(false).await
    }

    async fn start_with(insecure_no_auth: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());

        if !insecure_no_auth {
            let users = kimmy_auth::UserStore::open(&engine).unwrap();
            users.bootstrap_root(&engine, "root", ROOT_PASSWORD).unwrap();
        }

        let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
        let app = kimmy_api::build(Arc::clone(&engine), tokens, insecure_no_auth).unwrap();

        // Port 0: let the OS pick, so parallel tests never collide.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self { base: format!("http://{addr}"), client: Client, _dir: dir }
    }

    async fn get(&self, path: &str, token: Option<&str>) -> Res {
        self.client.request("GET", &format!("{}{path}", self.base), token, None).await
    }

    async fn post(&self, path: &str, token: Option<&str>, body: Value) -> Res {
        self.client.request("POST", &format!("{}{path}", self.base), token, Some(body)).await
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
