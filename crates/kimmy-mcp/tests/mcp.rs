//! End-to-end MCP tests.
//!
//! These drive the real merged router over a real socket and speak JSON-RPC,
//! because the thing worth testing is not that a Rust function returns an
//! error — it is that the *transport* refuses an unauthenticated caller and
//! that a tool invoked over the wire runs as the token that invoked it.
//!
//! The authorization tests are the load-bearing ones. M3's whole premise is
//! that an MCP tool cannot be more permissive than the REST route beside it, and
//! a premise nothing checks is a premise that decays.

use std::sync::Arc;

use kimmy_auth::{Action, Grant, TokenIssuer, UserStore};
use kimmy_storage::Engine;
use serde_json::{Value, json};

const SECRET: &str = "an-adequately-long-test-secret";

struct Server {
    base: String,
    tokens: TokenIssuer,
    engine: Arc<Engine>,
    _dir: tempfile::TempDir,
}

impl Server {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        UserStore::open(&engine).unwrap();

        let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
        // No limits: these tests mint tokens directly and never log in, so a
        // limiter would add state without exercising anything.
        let state = kimmy_api::state(
            Arc::clone(&engine),
            tokens.clone(),
            false,
            kimmy_api::RateLimits::disabled(),
        )
        .unwrap();

        // Merged exactly as the daemon merges it, so the test exercises the
        // real mounting rather than a convenient stand-in.
        let app = kimmy_api::router(Arc::clone(&state))
            .merge(kimmy_mcp::mcp_router(Arc::clone(&state), Vec::new()));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self { base: format!("http://{addr}"), tokens, engine, _dir: dir }
    }

    /// Mint a token directly, so a test can describe the grants it needs
    /// instead of provisioning a user to get them.
    fn token(&self, user: &str, grants: Vec<Grant>) -> String {
        self.tokens.issue(&kimmy_auth::Principal::new(user, grants)).unwrap()
    }

    fn root(&self) -> String {
        self.token("root", vec![Grant::superuser()])
    }

    /// POST one JSON-RPC message to `/mcp`.
    async fn rpc(&self, token: Option<&str>, body: Value) -> (u16, Value) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let host = self.base.strip_prefix("http://").unwrap();
        let payload = body.to_string();

        let mut request = format!(
            "POST /mcp HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
             Content-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\n\
             Content-Length: {}\r\n",
            payload.len()
        );
        if let Some(token) = token {
            request.push_str(&format!("Authorization: Bearer {token}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(&payload);

        let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.unwrap();
        let text = String::from_utf8_lossy(&raw).into_owned();

        let (head, rest) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        let status = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        (status, parse_body(head, rest))
    }

    /// Call a tool, returning the JSON-RPC response.
    async fn call(&self, token: &str, name: &str, arguments: Value) -> Value {
        let (status, body) = self
            .rpc(
                Some(token),
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": { "name": name, "arguments": arguments },
                }),
            )
            .await;
        assert_eq!(status, 200, "tool {name} returned HTTP {status}: {body}");
        body
    }

    /// The structured result of a successful tool call.
    async fn call_ok(&self, token: &str, name: &str, arguments: Value) -> Value {
        let body = self.call(token, name, arguments).await;
        assert!(body["error"].is_null(), "tool {name} failed: {body}");
        body["result"]["structuredContent"].clone()
    }
}

/// Read a response body, transparently unwrapping an SSE frame.
///
/// The server is configured for JSON responses, but it falls back to
/// `text/event-stream` in some cases; a test that only understood one of the
/// two would fail for a reason unrelated to what it was checking.
fn parse_body(head: &str, body: &str) -> Value {
    let body = body.trim();
    if head.to_ascii_lowercase().contains("text/event-stream") {
        for line in body.lines() {
            if let Some(data) = line.strip_prefix("data:")
                && let Ok(value) = serde_json::from_str(data.trim())
            {
                return value;
            }
        }
    }
    serde_json::from_str(body).unwrap_or(Value::Null)
}

fn seed(server: &Server) {
    let meta = server.engine.create_collection("sales", "orders").unwrap();
    for (id, status, total) in [("a", "open", 10i32), ("b", "closed", 20), ("c", "open", 30)] {
        server
            .engine
            .insert(&meta, bson::doc! { "_id": id, "status": status, "total": total })
            .unwrap();
    }
    server.engine.create_collection("sales", "secrets").unwrap();
}

// ---------------------------------------------------------------------------
// The transport
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_requires_a_token() {
    // The rejection must come from the transport, before any tool runs — a
    // surface where each tool has to remember to check is one where a new tool
    // eventually forgets.
    let server = Server::start().await;
    let (status, _) = server.rpc(None, json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})).await;
    assert_eq!(status, 401);

    let (status, _) = server
        .rpc(Some("not-a-token"), json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .await;
    assert_eq!(status, 401);
}

#[tokio::test]
async fn the_tool_list_is_the_documented_surface() {
    let server = Server::start().await;
    let token = server.root();

    let (status, body) =
        server.rpc(Some(&token), json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})).await;
    assert_eq!(status, 200, "{body}");

    let names: Vec<&str> = body["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();

    for expected in [
        "list_databases",
        "list_collections",
        "describe_collection",
        "find",
        "count",
        "vector_search",
        "hybrid_search",
        "insert",
        "update",
        "delete",
        "create_collection",
        "create_index",
    ] {
        assert!(names.contains(&expected), "missing tool {expected}; have {names:?}");
    }
}

#[tokio::test]
async fn write_tools_are_listed_even_for_a_read_only_token() {
    // Capability is controlled by the role, not by hiding tools: an agent that
    // cannot see a tool cannot be told why it was refused, and hiding is not a
    // security boundary in any case.
    let server = Server::start().await;
    let token = server.token("reader", vec![Grant::new("sales", "*", vec![Action::Read])]);

    let (_, body) =
        server.rpc(Some(&token), json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})).await;
    let names: Vec<&str> = body["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"insert"), "write tools must still be advertised");
}

// ---------------------------------------------------------------------------
// Authorization — the reason MCP is in-process
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_read_only_token_can_read_but_not_write() {
    let server = Server::start().await;
    seed(&server);
    let token = server.token("reader", vec![Grant::new("sales", "orders", vec![Action::Read])]);

    let found =
        server.call_ok(&token, "find", json!({"database":"sales","collection":"orders"})).await;
    assert_eq!(found["count"], 3);

    let body = server
        .call(
            &token,
            "insert",
            json!({"database":"sales","collection":"orders","document":{"x":1}}),
        )
        .await;
    assert!(!body["error"].is_null(), "a read-only token must not be able to insert: {body}");

    // And nothing was written.
    let after = server
        .call_ok(&server.root(), "count", json!({"database":"sales","collection":"orders"}))
        .await;
    assert_eq!(after["count"], 3);
}

#[tokio::test]
async fn grants_are_scoped_per_collection() {
    let server = Server::start().await;
    seed(&server);
    let token = server.token("reader", vec![Grant::new("sales", "orders", vec![Action::Read])]);

    let body =
        server.call(&token, "find", json!({"database":"sales","collection":"secrets"})).await;
    assert!(!body["error"].is_null(), "a grant on `orders` must not reach `secrets`: {body}");
}

#[tokio::test]
async fn search_can_be_granted_without_read() {
    // `search` is its own action so an agent can be given semantic search over
    // a collection without raw document access. The MCP surface must honour
    // that split, or the distinction stops meaning anything.
    let server = Server::start().await;
    seed(&server);
    let token = server.token("searcher", vec![Grant::new("sales", "orders", vec![Action::Search])]);

    let body = server.call(&token, "find", json!({"database":"sales","collection":"orders"})).await;
    assert!(!body["error"].is_null(), "search alone must not permit find: {body}");

    // The collection has no embeddings configured, so this fails — but on the
    // *configuration*, having passed authorization, which is the distinction
    // being tested.
    let body = server
        .call(
            &token,
            "vector_search",
            json!({"database":"sales","collection":"orders","query":"x"}),
        )
        .await;
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("vector configuration"),
        "expected to get past authorization to the vector check, got: {body}"
    );
}

#[tokio::test]
async fn listing_hides_what_the_caller_cannot_read() {
    let server = Server::start().await;
    seed(&server);
    let token = server.token("reader", vec![Grant::new("sales", "orders", vec![Action::Read])]);

    let listed = server.call_ok(&token, "list_collections", json!({"database":"sales"})).await;
    let names: Vec<&str> =
        listed["collections"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
    assert_eq!(names, vec!["orders"], "enumeration must not leak `secrets`");
}

#[tokio::test]
async fn resources_are_filtered_by_grants_too() {
    let server = Server::start().await;
    seed(&server);
    let token = server.token("reader", vec![Grant::new("sales", "orders", vec![Action::Read])]);

    let (status, body) =
        server.rpc(Some(&token), json!({"jsonrpc":"2.0","id":1,"method":"resources/list"})).await;
    assert_eq!(status, 200, "{body}");

    let uris: Vec<&str> = body["result"]["resources"]
        .as_array()
        .expect("resources array")
        .iter()
        .map(|r| r["uri"].as_str().unwrap())
        .collect();
    assert_eq!(uris, vec!["kimmy://sales/orders"]);
}

#[tokio::test]
async fn the_user_store_is_never_offered_as_a_resource() {
    // Even to a superuser. A resource is material an agent attaches to its
    // context, and the user store holds password hashes.
    let server = Server::start().await;
    seed(&server);
    let token = server.root();

    let (status, body) =
        server.rpc(Some(&token), json!({"jsonrpc":"2.0","id":1,"method":"resources/list"})).await;
    assert_eq!(status, 200, "{body}");

    let uris: Vec<&str> = body["result"]["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["uri"].as_str().unwrap())
        .collect();
    assert!(
        !uris.iter().any(|u| u.contains("__")),
        "internal objects must not be listed: {uris:?}"
    );
    assert!(uris.contains(&"kimmy://sales/orders"), "user data must still be listed: {uris:?}");
}

#[tokio::test]
async fn reading_a_resource_the_caller_cannot_reach_is_refused() {
    let server = Server::start().await;
    seed(&server);
    let token = server.token("reader", vec![Grant::new("sales", "orders", vec![Action::Read])]);

    // Not listed, but a URI can be guessed — so the read itself must check.
    let (_, body) = server
        .rpc(
            Some(&token),
            json!({
                "jsonrpc":"2.0","id":1,"method":"resources/read",
                "params": {"uri": "kimmy://sales/secrets"},
            }),
        )
        .await;
    assert!(!body["error"].is_null(), "a guessed URI must not bypass the grant: {body}");
}

// ---------------------------------------------------------------------------
// Behaviour
// ---------------------------------------------------------------------------

#[tokio::test]
async fn find_accepts_the_query_language() {
    let server = Server::start().await;
    seed(&server);
    let token = server.root();

    let found = server
        .call_ok(
            &token,
            "find",
            json!({
                "database":"sales","collection":"orders",
                "filter": {"status":"open","total":{"$gt":10}},
            }),
        )
        .await;
    assert_eq!(found["count"], 1);
    assert_eq!(found["documents"][0]["_id"], "c");
}

#[tokio::test]
async fn describe_collection_reports_paths_and_types() {
    let server = Server::start().await;
    seed(&server);
    let token = server.root();

    let described = server
        .call_ok(&token, "describe_collection", json!({"database":"sales","collection":"orders"}))
        .await;

    assert_eq!(described["documentCount"], 3);
    assert_eq!(described["sampled"], 3);

    let fields = described["fields"].as_array().unwrap();
    let status = fields.iter().find(|f| f["path"] == "status").expect("status field");
    assert_eq!(status["types"], json!(["string"]));
    assert_eq!(status["presence"], 1.0);

    let total = fields.iter().find(|f| f["path"] == "total").expect("total field");
    assert_eq!(total["types"], json!(["int"]));
}

#[tokio::test]
async fn a_write_tool_actually_writes() {
    let server = Server::start().await;
    seed(&server);
    let token = server.root();

    server
        .call_ok(
            &token,
            "insert",
            json!({"database":"sales","collection":"orders","document":{"_id":"d","status":"open"}}),
        )
        .await;

    let counted =
        server.call_ok(&token, "count", json!({"database":"sales","collection":"orders"})).await;
    assert_eq!(counted["count"], 4);
}

#[tokio::test]
async fn a_malformed_filter_is_reported_to_the_caller() {
    // An agent that can read the reason can correct itself; an opaque failure
    // just gets retried.
    let server = Server::start().await;
    seed(&server);
    let token = server.root();

    let body = server
        .call(
            &token,
            "find",
            json!({"database":"sales","collection":"orders","filter":{"x":{"$nope":1}}}),
        )
        .await;
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(message.contains("$nope"), "the message must name the problem: {body}");
}

#[tokio::test]
async fn tool_results_carry_both_text_and_structured_content() {
    // Not every client renders structured content, and an answer no client
    // shows is not an answer.
    let server = Server::start().await;
    seed(&server);
    let token = server.root();

    let body = server.call(&token, "list_databases", json!({})).await;
    let result = &body["result"];
    assert!(result["structuredContent"]["databases"].is_array());
    assert!(
        result["content"][0]["text"].as_str().unwrap_or_default().contains("sales"),
        "expected a text block as well: {result}"
    );
}
