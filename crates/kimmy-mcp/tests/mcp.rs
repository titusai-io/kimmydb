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

const SECRET: &str = "an-adequately-long-test-secret-for-hs256";

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
        // real mounting rather than a convenient stand-in. That faithfulness
        // used to reproduce a defect instead of catching it: both sides said
        // `router(..).merge(..)`, which leaves `/mcp` outside the layer that
        // counts, times, traces and challenges.
        let app = kimmy_api::router_with(
            Arc::clone(&state),
            Some(kimmy_mcp::mcp_router(Arc::clone(&state), Vec::new())),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self { base: format!("http://{addr}"), tokens, engine, _dir: dir }
    }

    /// Mint a token directly, so a test can describe the grants it needs
    /// instead of spelling out a login.
    ///
    /// The user is created too, and that is not incidental: since ADR-052 a
    /// token for an account that does not exist is refused, which is the whole
    /// point of the feature. A token minted for an invented principal would be
    /// testing a state no real client can reach.
    fn token(&self, user: &str, grants: Vec<Grant>) -> String {
        let store = kimmy_auth::UserStore::open(&self.engine).unwrap();
        if store.get(&self.engine, user).unwrap().is_none() {
            store.create(&self.engine, user, "harness-password", grants.clone()).unwrap();
        }
        self.tokens.issue(&kimmy_auth::Principal::new(user, grants)).unwrap()
    }

    fn root(&self) -> String {
        self.token("root", vec![Grant::superuser()])
    }

    /// POST one JSON-RPC message to `/mcp`.
    async fn rpc(&self, token: Option<&str>, body: Value) -> (u16, Value) {
        self.rpc_at("/mcp", token, body).await
    }

    /// The same, at a request target of the test's choosing — for the one
    /// test whose subject is the target rather than the message.
    async fn rpc_at(&self, target: &str, token: Option<&str>, body: Value) -> (u16, Value) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let host = self.base.strip_prefix("http://").unwrap();
        let payload = body.to_string();

        let mut request = format!(
            "POST {target} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
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

    /// The `WWW-Authenticate` header `/mcp` answers a rejected request with.
    ///
    /// Read off the wire rather than through `rpc`, which keeps only the status
    /// and the body — and the header is the whole point here.
    async fn challenge(&self, token: Option<&str>) -> Option<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let host = self.base.strip_prefix("http://").unwrap();
        let payload = json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}).to_string();
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
        let (head, _) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));

        head.lines()
            .find(|line| line.to_ascii_lowercase().starts_with("www-authenticate:"))
            .map(|line| line.split_once(':').unwrap().1.trim().to_string())
    }

    /// `kimmy_requests_total`, scraped off `/metrics`.
    async fn request_count(&self) -> u64 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let host = self.base.strip_prefix("http://").unwrap();
        let request = format!("GET /metrics HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");

        let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.unwrap();
        let text = String::from_utf8_lossy(&raw).into_owned();

        text.lines()
            .find(|line| line.starts_with("kimmy_requests_total"))
            .and_then(|line| line.split_whitespace().last())
            .and_then(|n| n.parse().ok())
            .expect("kimmy_requests_total is missing from /metrics")
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

/// A `byo` collection with three vectors stored directly, so search can be
/// driven without an embedding provider or the worker.
///
/// For the query `"red blue"` with vector `[1, 0, 0]`: `x` is the nearest
/// vector and shares no term, `z` is second nearest and shares both, `y` is
/// farthest and shares one — the candidate `min_overlap` gates.
fn seed_vectors(server: &Server) {
    use kimmy_core::{ChunkConfig, DocId, Metric, ProviderConfig, VectorConfig, VectorRecord};

    let meta = server.engine.create_collection("kb", "notes").unwrap();
    server
        .engine
        .configure_vectors(
            "kb",
            "notes",
            VectorConfig {
                fields: vec!["text".into()],
                provider: ProviderConfig::Byo,
                dim: 3,
                metric: Metric::Cosine,
                chunk: ChunkConfig::default(),
                document_prefix: None,
                query_prefix: None,
            },
        )
        .unwrap();
    let shadow = server.engine.vector_collection("kb", "notes").unwrap().unwrap();
    for (id, vector, text) in [
        ("x", [1.0, 0.0, 0.0], "green paint"),
        ("y", [0.0, 1.0, 0.0], "red apple"),
        ("z", [0.6, 0.0, 0.8], "red blue"),
    ] {
        server.engine.insert(&meta, bson::doc! { "_id": id, "text": text }).unwrap();
        let source = DocId::String(id.to_string());
        let stamp = server.engine.document_stamp(&meta, &source).unwrap().unwrap();
        let record = VectorRecord {
            source: source.clone(),
            chunk: 0,
            source_hlc: stamp.hlc,
            vector: vector.to_vec(),
            text: text.to_string(),
        };
        server.engine.put_vectors(&shadow, &source, &[record]).unwrap();
    }
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
async fn a_rejected_mcp_request_says_where_to_authenticate() {
    // RFC 6750 §3, and the reason RFC 9728 exists at all: an MCP client holding
    // no credentials has no other way to discover its authorization server, so
    // a bare 401 leaves it needing to be configured by hand.
    //
    // This is a mounting test as much as a header test. `/mcp` is merged into
    // the same router as the REST API, and a router merged *after* the
    // challenge layer keeps its own empty middleware stack — which is exactly
    // how this regressed, silently, while every REST route stayed correct.
    let server = Server::start().await;

    let missing = server.challenge(None).await;
    let challenge = missing.expect("a 401 from /mcp must carry WWW-Authenticate");
    assert!(challenge.starts_with("Bearer"), "not a bearer challenge: {challenge}");
    // No `error` code when the request offered no credentials (RFC 6750 §3.1).
    assert!(
        !challenge.contains("error="),
        "a request with no credentials is not an error: {challenge}"
    );

    let rejected = server.challenge(Some("not-a-token")).await;
    let challenge = rejected.expect("a rejected token must also carry WWW-Authenticate");
    assert!(
        challenge.contains(r#"error="invalid_token""#),
        "a bad token is invalid_token: {challenge}"
    );
}

/// The REST table refuses a query string on a route that reads none
/// (ADR-124). `/mcp` is merged beside that table, not into it, and MCP is a
/// transport with query semantics of its own — so a query string here is
/// rmcp's to judge, and it answers the request as though there were none.
#[tokio::test]
async fn a_query_string_on_mcp_is_not_refused_by_the_rest_tables_guard() {
    let server = Server::start().await;
    let root = server.root();
    let (status, body) = server
        .rpc_at("/mcp?zz=1", Some(&root), json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
        .await;
    assert_eq!(status, 200, "{body}");
    assert!(body["result"]["tools"].is_array(), "{body}");
}

#[tokio::test]
async fn mcp_requests_are_counted_like_every_other_route() {
    // The counting layer is the single instrumentation site, so a route outside
    // it is invisible to `/metrics` and to tracing both. `/mcp` was.
    let server = Server::start().await;
    let token = server.root();

    // `kimmy_requests_total` is one unlabelled counter and a scrape counts
    // itself, so "it went up" would pass whether or not `/mcp` is counted.
    // Calibrate against a scrape-only interval instead, and require exactly one
    // more than that.
    let a = server.request_count().await;
    let b = server.request_count().await;
    let scrape_cost = b - a;

    let (status, _) =
        server.rpc(Some(&token), json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})).await;
    assert_eq!(status, 200);

    let c = server.request_count().await;
    assert_eq!(
        c - b,
        scrape_cost + 1,
        "an MCP request was not counted: scrape alone costs {scrape_cost}, \
         scrape plus one MCP call cost {}",
        c - b
    );
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
        "aggregate",
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
async fn a_ddl_token_can_create_a_collection_and_write_needs_its_own_grant() {
    // The first thing an agent does with a fresh database is create the
    // collection it will write to. `ddl` is what allows that (ADR-090), and it
    // is not implied by `write` — so a writer without it is told no, in the
    // words the executor uses everywhere else.
    let server = Server::start().await;
    let builder =
        server.token("builder", vec![Grant::new("app", "*", vec![Action::Ddl, Action::Write])]);
    let created = server
        .call_ok(&builder, "create_collection", json!({"database":"app","name":"memories"}))
        .await;
    assert_eq!(created["created"], "memories");
    let indexed = server
        .call_ok(
            &builder,
            "create_index",
            json!({"database":"app","collection":"memories","fields":[{"path":"topic"}]}),
        )
        .await;
    assert!(!indexed.is_null(), "a ddl token creates indexes too");
    server
        .call_ok(
            &builder,
            "insert",
            json!({"database":"app","collection":"memories","document":{"topic":"x"}}),
        )
        .await;

    let writer = server.token("writer", vec![Grant::new("app", "*", vec![Action::Write])]);
    let refused =
        server.call(&writer, "create_collection", json!({"database":"app","name":"other"})).await;
    assert!(!refused["error"].is_null(), "write must not imply ddl: {refused}");
    assert!(
        refused["error"]["message"].as_str().unwrap_or_default().contains("not authorized"),
        "the refusal names itself: {refused}"
    );
}

#[tokio::test]
async fn listing_a_database_that_does_not_exist_is_an_error_not_an_empty_list() {
    // `[]` already means "nothing you can see"; a mistyped database name must
    // not read the same way.
    let server = Server::start().await;
    seed(&server);
    let root = server.root();
    let listed = server.call_ok(&root, "list_collections", json!({"database":"sales"})).await;
    assert!(listed["collections"].as_array().unwrap().contains(&json!("orders")));

    let missing = server.call(&root, "list_collections", json!({"database":"salez"})).await;
    let message = missing["error"]["message"].as_str().unwrap_or_default();
    assert!(message.contains("salez"), "the error names the database: {missing}");
}

#[tokio::test]
async fn listings_omit_internals_but_tools_still_reach_them_by_name() {
    // A listing is an invitation (ADR-092): an agent shown `orders.__vectors`
    // next to `orders` opens it and pays for a collection of float arrays that
    // says nothing the source does not. The shadow is left out of the list --
    // and the system database out of the database list -- while `count` on it
    // by name still answers, because that is a specific question under the
    // ordinary access check, not a default.
    let server = Server::start().await;
    seed(&server);
    server
        .engine
        .configure_vectors(
            "sales",
            "orders",
            kimmy_core::vector_meta::VectorConfig {
                fields: vec!["status".into()],
                provider: kimmy_core::vector_meta::ProviderConfig::Byo,
                dim: 4,
                metric: Default::default(),
                document_prefix: None,
                query_prefix: None,
                chunk: Default::default(),
            },
        )
        .unwrap();
    assert!(server.engine.get_collection("sales", "orders.__vectors").is_ok(), "the shadow exists");
    let root = server.root();

    let listed = server.call_ok(&root, "list_collections", json!({"database":"sales"})).await;
    let names = listed["collections"].as_array().unwrap();
    assert!(names.contains(&json!("orders")), "{listed}");
    assert!(!names.contains(&json!("orders.__vectors")), "the shadow is not listed: {listed}");

    let databases = server.call_ok(&root, "list_databases", json!({})).await;
    let names = databases["databases"].as_array().unwrap();
    assert!(names.contains(&json!("sales")), "{databases}");
    assert!(!names.contains(&json!("__kimmy")), "the system database is not listed: {databases}");

    let counted = server
        .call_ok(&root, "count", json!({"database":"sales","collection":"orders.__vectors"}))
        .await;
    assert!(counted["count"].is_number(), "reachable by name: {counted}");
}

#[tokio::test]
async fn every_write_tool_reports_the_stamp_it_produced() {
    // `insert` reported one from the start; the other three did not, so a
    // client wanting to follow a bulk load or a single update with a
    // conditional write (ADR-084) had no version to name.
    let server = Server::start().await;
    seed(&server);
    let root = server.root();

    let many = server
        .call_ok(
            &root,
            "insert_many",
            json!({"database":"sales","collection":"orders",
                   "documents":[{"_id":"s1","total":1},{"_id":"s2","total":2}]}),
        )
        .await;
    let stamps = many["stamps"].as_array().expect("stamps");
    assert_eq!(stamps.len(), 2);
    assert_ne!(stamps[0], stamps[1], "each document lands at its own version");

    let updated = server
        .call_ok(
            &root,
            "update",
            json!({"database":"sales","collection":"orders",
                   "filter":{"_id":"s1"},"update":{"$set":{"total":10}}}),
        )
        .await;
    assert!(updated["stamp"].is_string(), "a single update names its version: {updated}");
    assert_ne!(updated["stamp"], stamps[0], "and it moved");

    let deleted = server
        .call_ok(
            &root,
            "delete",
            json!({"database":"sales","collection":"orders","filter":{"_id":"s2"}}),
        )
        .await;
    assert!(deleted["stamp"].is_string(), "a single delete names the tombstone: {deleted}");

    let multi = server
        .call_ok(
            &root,
            "update",
            json!({"database":"sales","collection":"orders",
                   "filter":{},"update":{"$set":{"seen":true}},"multi":true}),
        )
        .await;
    assert!(multi["stamp"].is_null(), "a multi write has no single version: {multi}");
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
async fn hybrid_search_takes_the_fusion_controls() {
    // ADR-094. The REST route and the tool share one implementation, so what
    // is tested here is that the tool's *arguments* carry the two fields
    // through — and that its advertised schema says they exist, since a
    // caller can only pass what the schema names.
    let server = Server::start().await;
    seed_vectors(&server);
    let token = server.root();

    let (_, listed) =
        server.rpc(Some(&token), json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})).await;
    let tool = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "hybrid_search")
        .expect("hybrid_search is listed");
    let schema = &tool["inputSchema"];
    let properties = &schema["properties"];
    for field in ["query", "vector", "filter", "k", "weights", "min_overlap"] {
        assert!(!properties[field].is_null(), "hybrid_search schema lacks {field}: {tool}");
    }
    // An optional nested object is emitted as `anyOf: [{$ref}, {type: null}]`
    // with the object itself under `$defs`; follow the reference if there is
    // one, so the check is about what the schema says and not how it says it.
    let weights = &properties["weights"];
    let reference = weights["anyOf"]
        .as_array()
        .and_then(|options| options.iter().find_map(|option| option["$ref"].as_str()));
    let weight_fields = match reference {
        Some(reference) => {
            let pointer = reference.strip_prefix('#').expect("a local reference");
            &schema.pointer(pointer).expect("the reference resolves")["properties"]
        }
        None => &weights["properties"],
    };
    assert!(
        !weight_fields["dense"].is_null() && !weight_fields["lexical"].is_null(),
        "weights must spell out dense and lexical: {weights}"
    );

    let order = |result: &Value| -> Vec<String> {
        result["matches"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["_id"].as_str().unwrap().to_string())
            .collect()
    };
    let base = json!({
        "database": "kb", "collection": "notes",
        "query": "red blue", "vector": [1.0, 0.0, 0.0], "k": 5,
    });

    // Without the fields: plain RRF, the one-term match `y` outranks the
    // nearest vector `x` on the strength of its lexical rank alone.
    let plain = server.call_ok(&token, "hybrid_search", base.clone()).await;
    assert_eq!(order(&plain), vec!["z", "y", "x"], "{plain}");

    // Gated at two distinct terms, `y` loses its lexical share but keeps its
    // dense one — it is still there, behind `x`.
    let mut gated = base.clone();
    gated["min_overlap"] = json!(2);
    let gated = server.call_ok(&token, "hybrid_search", gated).await;
    assert_eq!(order(&gated), vec!["z", "x", "y"], "{gated}");

    // Weighted entirely towards the dense half: the vector order.
    let mut dense = base.clone();
    dense["weights"] = json!({ "dense": 1.0, "lexical": 0.0 });
    let dense = server.call_ok(&token, "hybrid_search", dense).await;
    assert_eq!(order(&dense), vec!["x", "z", "y"], "{dense}");

    // And a meaningless control is refused in words, not accepted quietly.
    let mut bad = base.clone();
    bad["min_overlap"] = json!(0);
    let refused = server.call(&token, "hybrid_search", bad).await;
    let message = refused["error"]["message"].as_str().unwrap_or_default();
    assert!(message.contains("min_overlap"), "expected a refusal naming the field: {refused}");
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
    // The tool forwards the whole describe document, so the node's durability
    // class (ADR-088) reaches a client through the call it makes before writing.
    assert_eq!(described["nodeDurability"], "durable");

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

/// Tool arguments arrive as JSON like an HTTP body does and reach the same
/// `exec` layer, so the MCP boundary keeps operator order exactly as the HTTP
/// one does (ADR-120). Pinned here rather than assumed: the changelog says
/// both boundaries were affected and both are fixed. The argument is parsed
/// from text so the key order on the wire is the order written here.
#[tokio::test]
async fn the_update_tool_applies_operators_in_the_order_written() {
    let server = Server::start().await;
    seed(&server);
    let token = server.root();

    server
        .call_ok(
            &token,
            "insert",
            json!({"database":"sales","collection":"orders","document":{"_id":"o","a":0}}),
        )
        .await;

    let args: Value = serde_json::from_str(
        r#"{"database":"sales","collection":"orders","filter":{"_id":"o"},
            "update":{"$set":{"a":1},"$inc":{"a":5}}}"#,
    )
    .unwrap();
    server.call_ok(&token, "update", args).await;
    let found = server
        .call_ok(
            &token,
            "find",
            json!({"database":"sales","collection":"orders","filter":{"_id":"o"}}),
        )
        .await;
    assert_eq!(found["documents"][0]["a"], 6, "$set then $inc: {found}");

    let args: Value = serde_json::from_str(
        r#"{"database":"sales","collection":"orders","filter":{"_id":"o"},
            "update":{"$inc":{"a":5},"$set":{"a":1}}}"#,
    )
    .unwrap();
    server.call_ok(&token, "update", args).await;
    let found = server
        .call_ok(
            &token,
            "find",
            json!({"database":"sales","collection":"orders","filter":{"_id":"o"}}),
        )
        .await;
    assert_eq!(found["documents"][0]["a"], 1, "$inc then $set: {found}");
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
async fn an_argument_the_tool_does_not_define_is_refused_by_name() {
    // ADR-121, the tool side. A model that misspells `limit` and is answered
    // with every document has no way to notice; one told the name it used is
    // wrong corrects itself, as it does for a rejected filter.
    let server = Server::start().await;
    seed(&server);
    seed_vectors(&server);
    let token = server.root();

    // rmcp reports an argument it could not deserialize as the tool's own
    // error result rather than a protocol error, so the text is where a
    // model reads a tool's answer.
    let body = server
        .call(&token, "find", json!({"database":"sales","collection":"orders","limt":5}))
        .await;
    assert_eq!(body["result"]["isError"], json!(true), "an unknown argument must not run: {body}");
    let message = body["result"]["content"][0]["text"].as_str().unwrap_or_default();
    assert!(message.contains("`limt`"), "the message must name the field: {body}");

    // hybrid_search carries its search fields itself rather than flattening
    // the vector_search arguments, because serde cannot refuse unknown fields
    // across a flatten; this is the case that would regress if it did.
    let body = server
        .call(
            &token,
            "hybrid_search",
            json!({"database":"sales","collection":"orders","query":"widget","min_overlp":2}),
        )
        .await;
    assert_eq!(body["result"]["isError"], json!(true), "an unknown argument must not run: {body}");
    let message = body["result"]["content"][0]["text"].as_str().unwrap_or_default();
    assert!(message.contains("`min_overlp`"), "the message must name the field: {body}");

    // The schema the model reads says so up front. A tool that takes no
    // arguments has rmcp's empty schema rather than one of ours, and there is
    // nothing for it to refuse.
    let (_, listed) =
        server.rpc(Some(&token), json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})).await;
    for tool in listed["result"]["tools"].as_array().unwrap() {
        if tool["inputSchema"]["properties"].as_object().is_none_or(|p| p.is_empty()) {
            continue;
        }
        assert_eq!(
            tool["inputSchema"]["additionalProperties"],
            json!(false),
            "{} does not declare itself closed: {}",
            tool["name"],
            tool["inputSchema"]
        );
    }
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

#[tokio::test]
async fn the_aggregate_tool_runs_a_pipeline_as_the_calling_principal() {
    // The tool that has been advertised as planned since M3. It matters that it
    // runs through the same executor as the REST route: a second path to the
    // engine is how an agent tool ends up more permissive than the API.
    let server = Server::start().await;
    seed(&server);
    let token = server.token(
        "analyst",
        vec![Grant {
            db: "sales".into(),
            collection: "orders".into(),
            actions: vec![Action::Read],
        }],
    );

    let result = server
        .call_ok(
            &token,
            "aggregate",
            json!({
                "database": "sales",
                "collection": "orders",
                "pipeline": [{"$group": {"_id": "$status", "n": {"$sum": 1}}},
                             {"$sort": {"_id": 1}}]
            }),
        )
        .await;

    let docs = result["documents"].as_array().expect("documents");
    assert_eq!(docs.len(), 2, "open and closed: {docs:?}");
    assert_eq!(docs[0]["_id"], "closed");
    assert_eq!(docs[1]["n"], 2);
}

#[tokio::test]
async fn the_aggregate_tool_refuses_a_lookup_the_caller_cannot_read() {
    // Same boundary as the REST route, asserted at the MCP edge too, because
    // "both edges enforce the same authorization" is only true while tested.
    let server = Server::start().await;
    seed(&server);
    let token = server.token(
        "analyst",
        vec![Grant {
            db: "sales".into(),
            collection: "orders".into(),
            actions: vec![Action::Read],
        }],
    );

    let body = server
        .call(
            &token,
            "aggregate",
            json!({
                "database": "sales",
                "collection": "orders",
                "pipeline": [{"$lookup": {"from": "secrets", "localField": "status",
                                          "foreignField": "_id", "as": "joined"}}]
            }),
        )
        .await;

    let rendered = format!("{body:?}");
    assert!(
        rendered.contains("not authorized") || rendered.contains("forbidden"),
        "a $lookup into an ungranted collection must be refused: {rendered}"
    );
}
