//! The protocol specification, checked against the server it describes.
//!
//! `docs/openapi.yaml` is hand-written (ADR-056). What keeps it true is this
//! file, and it checks two different things — because a specification can be
//! wrong in two different ways:
//!
//! 1. **Inventory.** Every route the router registers is described, and every
//!    operation described is registered. This catches a route added without a
//!    spec entry, and a spec entry for a route that was renamed or removed.
//! 2. **Behaviour.** Every documented operation is driven against a real
//!    server over a real socket, and the response is validated against the
//!    schema the spec declares for that status. This catches the drift the
//!    inventory cannot see: a route that still exists and still answers, but
//!    no longer answers with what the document claims.
//!
//! The second half is the one that matters. An inventory check has existed
//! since M8 — `every_route_is_in_the_http_reference`, which moved here from
//! `routes.rs` so there is one route scanner rather than two — and inventory
//! alone would have been satisfied by a response whose fields had all been
//! renamed. It was, in fact, satisfied by a route it never looked at: it
//! matched `.route("` at the start of a line and so skipped the three
//! registrations rustfmt breaks across lines.
//!
//! **The coverage assertion is deliberate.** The live test ends by asserting
//! that every documented operation was actually exercised, so a new route
//! cannot be added to both the router and the spec without also being driven
//! here. A spec entry nothing executes is exactly the "prose nothing checks"
//! failure this milestone exists to end.
//!
//! What it does *not* check: that every documented *status* was produced. Some
//! — a provider failing upstream, a resume token past the retention horizon —
//! need conditions this harness has no cheap way to create. A representative
//! set of refusals is exercised below, and the rest are prose until something
//! makes them cheap.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use kimmy_auth::TokenIssuer;
use kimmy_storage::Engine;
use serde_json::{Value, json};

const SPEC_SOURCE: &str = include_str!("../../../docs/openapi.yaml");
const ROUTER_SOURCE: &str = include_str!("../src/routes.rs");

const SECRET: &str = "an-adequately-long-test-secret";
const ROOT_PASSWORD: &str = "root-password";

/// HTTP methods an OpenAPI path item may carry.
const METHODS: [&str; 7] = ["get", "put", "post", "delete", "options", "head", "patch"];

// ---------------------------------------------------------------------------
// The specification
// ---------------------------------------------------------------------------

fn spec() -> &'static Value {
    static SPEC: OnceLock<Value> = OnceLock::new();
    SPEC.get_or_init(|| {
        serde_norway::from_str(SPEC_SOURCE).expect("docs/openapi.yaml is not valid YAML")
    })
}

/// Every `(method, path)` the specification describes.
fn documented_operations() -> BTreeSet<(String, String)> {
    let paths = spec()["paths"].as_object().expect("the spec has a paths object");
    let mut out = BTreeSet::new();
    for (path, item) in paths {
        let item = item.as_object().expect("a path item is an object");
        for method in METHODS {
            if item.contains_key(method) {
                out.insert((method.to_uppercase(), path.clone()));
            }
        }
    }
    out
}

/// Every `(method, path)` the router registers.
///
/// Read out of the source rather than out of the `Router`, because axum
/// exposes no way to enumerate what a router holds. That makes this a text
/// scan, with the limitation text scans have: it sees `.route("…", …)` calls
/// in this file and nothing else. Routes mounted elsewhere are out of scope by
/// the same rule the spec states — `/mcp` is a different protocol, described by
/// `docs/mcp.md`.
fn registered_operations() -> BTreeSet<(String, String)> {
    let mut out = BTreeSet::new();
    let mut rest = ROUTER_SOURCE;

    while let Some(at) = rest.find(".route(") {
        let open = at + ".route(".len();
        let after = &rest[open..];

        // The path is the first argument, but not necessarily on the same
        // line: three registrations here carry enough methods that rustfmt
        // breaks them. Matching `.route("` — which the older inventory test in
        // `routes.rs` did — silently skipped exactly those three.
        let quoted = after.find('"').expect("a route path literal");
        let path = after[quoted + 1..].split('"').next().expect("a closing quote");

        // The registration runs to the paren matching `.route(`. Route paths
        // hold no parens, so counting them is enough.
        let mut depth = 1usize;
        let mut end = after.len();
        for (i, c) in after.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        let registration = &after[..end];

        for method in METHODS {
            if contains_method_call(registration, method) {
                out.insert((method.to_uppercase(), path.to_string()));
            }
        }

        rest = &rest[open + end..];
    }

    out
}

/// Whether a registration calls `method(...)` as a routing method rather than
/// merely containing those letters inside a handler's name.
fn contains_method_call(registration: &str, method: &str) -> bool {
    let needle = format!("{method}(");
    let mut from = 0;
    while let Some(at) = registration[from..].find(&needle) {
        let at = from + at;
        let preceding = registration[..at].chars().next_back();
        // `get(find_docs)` and `.get(find_docs)` are routing methods;
        // `budget(x)` is not.
        if matches!(preceding, None | Some('.') | Some('(') | Some(' ') | Some('\n')) {
            return true;
        }
        from = at + needle.len();
    }
    false
}

#[test]
fn the_specification_and_the_router_describe_the_same_operations() {
    let documented = documented_operations();
    let registered = registered_operations();

    // Sanity: a scan that silently matched nothing would make both directions
    // pass vacuously, which is the shape of a check that has stopped checking.
    assert!(
        registered.len() > 20,
        "the route scan found {} routes; it is broken",
        registered.len()
    );

    let undocumented: Vec<_> = registered.difference(&documented).collect();
    let unregistered: Vec<_> = documented.difference(&registered).collect();

    assert!(
        undocumented.is_empty(),
        "these routes are registered but absent from docs/openapi.yaml: {undocumented:#?}"
    );
    assert!(
        unregistered.is_empty(),
        "docs/openapi.yaml describes operations the router does not register: {unregistered:#?}"
    );
}

/// The prose reference stays complete too.
///
/// Moved here from `routes.rs` rather than left beside the router, so there is
/// one scanner instead of two. The one it replaces matched `.route("` at the
/// start of a line, which skipped the three registrations rustfmt breaks
/// across lines — including `/docs/{id}`, the busiest route on the API. It had
/// been passing while never checking them.
///
/// `http-api.md` is not redundant with the specification: it is the page
/// someone reads to *learn* the API, and its endpoint table reads as complete.
/// M8 found it missing six of twenty-eight routes.
#[test]
fn every_route_is_in_the_http_reference() {
    const REFERENCE: &str = include_str!("../../../docs/http-api.md");

    let missing: Vec<_> = registered_operations()
        .into_iter()
        .map(|(_, path)| path)
        .filter(|path| !REFERENCE.contains(path))
        .collect();

    assert!(
        missing.is_empty(),
        "these routes are registered but absent from docs/http-api.md: {missing:#?}"
    );
}

// ---------------------------------------------------------------------------
// The error taxonomy
// ---------------------------------------------------------------------------

/// The code set in the server and the code set in the document are the same
/// set, and they agree on what a client may do about each one.
///
/// This is what makes the taxonomy public surface rather than an accident of
/// where `ApiError::new` happens to be called. `ErrorCode` is an enum, so the
/// compiler already refuses an unlisted code and forces its retry class to be
/// decided; this closes the remaining gap, which is the document falling
/// behind the enum.
///
/// It would have caught `no_vectors`, which existed in `vectors.rs` and in
/// neither document — both were written by reading `error.rs`, and the codes
/// accrete across five modules.
#[test]
fn every_error_code_is_specified_with_the_retry_class_the_server_uses() {
    use kimmy_api::error::ErrorCode;

    let schema = &spec()["components"]["schemas"]["ErrorCode"];
    let documented: BTreeSet<String> = schema["enum"]
        .as_array()
        .expect("the ErrorCode schema is an enum")
        .iter()
        .map(|v| v.as_str().expect("a code is a string").to_string())
        .collect();
    let served: BTreeSet<String> = ErrorCode::ALL.iter().map(|c| c.as_str().to_string()).collect();

    assert_eq!(
        served.len(),
        ErrorCode::ALL.len(),
        "two variants of ErrorCode render to the same wire string"
    );
    assert_eq!(served, documented, "the server's codes and the specification's disagree");

    // The retry class travels in the envelope, so the table in the document is
    // a promise about what the server sends, not a description of it.
    let table = schema["description"].as_str().expect("the ErrorCode schema documents its codes");
    for code in ErrorCode::ALL {
        let row = table
            .lines()
            .find(|line| line.starts_with(&format!("| `{}` |", code.as_str())))
            .unwrap_or_else(|| panic!("{} has no row in the ErrorCode table", code.as_str()));
        let declared = row.split('|').nth(3).expect("a retry column").trim();
        assert_eq!(
            declared,
            code.retry().as_str(),
            "{} is served as `{}` and documented as `{declared}`",
            code.as_str(),
            code.retry().as_str()
        );
    }

    let classes: BTreeSet<&str> = spec()["components"]["schemas"]["Retry"]["enum"]
        .as_array()
        .expect("Retry is an enum")
        .iter()
        .map(|v| v.as_str().expect("a class is a string"))
        .collect();
    assert_eq!(classes, BTreeSet::from(["no", "wait", "elsewhere"]));
}

// ---------------------------------------------------------------------------
// The compatibility policy
// ---------------------------------------------------------------------------

/// The major version is in the path, and everything agrees about what it is.
///
/// `docs/compatibility.md` promises that `/v1` does not break and that the path
/// carries the major version. This is the part of that promise a test can hold:
/// the route prefixes, the version the server reports, and the specification's
/// own `info.version` cannot drift apart.
#[test]
fn every_versioned_route_carries_the_protocol_major() {
    // The unversioned routes, and why each one is allowed to be: an
    // infrastructure probe is not part of the client protocol and must not
    // move when the protocol majors.
    const UNVERSIONED: [&str; 3] = ["/healthz", "/readyz", "/metrics"];

    let protocol = kimmy_api::version::PROTOCOL;
    let stray: Vec<_> = documented_operations()
        .into_iter()
        .map(|(_, path)| path)
        .filter(|path| !UNVERSIONED.contains(&path.as_str()))
        .filter(|path| !path.starts_with(&format!("/{protocol}/")))
        .collect();
    assert!(stray.is_empty(), "these routes are outside /{protocol}/: {stray:#?}");

    let declared = spec()["info"]["version"].as_str().expect("info.version is a string");
    let major = declared.split('.').next().expect("a major component");
    assert_eq!(
        format!("v{major}"),
        protocol,
        "the specification is version {declared} and the server serves /{protocol}"
    );
}

/// A response schema may not forbid unknown properties.
///
/// This is what makes "a new response field is additive" true rather than
/// merely intended: a client validating against today's document has to keep
/// validating tomorrow's responses. `additionalProperties: false` anywhere in a
/// response would make the next added field a breaking change for every
/// validating client, silently, and only for them.
#[test]
fn no_response_schema_forbids_the_fields_it_has_not_seen() {
    let mut closed = Vec::new();
    find_closed_schemas(spec(), String::new(), &mut closed);
    assert!(
        closed.is_empty(),
        "these schemas forbid unknown properties, which makes adding a response field \
         breaking for a validating client: {closed:#?}"
    );
}

fn find_closed_schemas(node: &Value, path: String, closed: &mut Vec<String>) {
    match node {
        Value::Object(map) => {
            if map.get("additionalProperties") == Some(&Value::Bool(false)) {
                closed.push(path.clone());
            }
            for (key, value) in map {
                // Request bodies are allowed to be strict — several reject
                // unknown fields on purpose, so a typo is an error rather than
                // a silent no-op. It is *responses* that must stay open.
                if key == "requestBody" {
                    continue;
                }
                find_closed_schemas(value, format!("{path}/{key}"), closed);
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                find_closed_schemas(item, format!("{path}/{i}"), closed);
            }
        }
        _ => {}
    }
}

/// The capabilities a node advertises are the ones the specification names.
///
/// Same mechanism as the error codes, for the same reason: the set is public
/// surface and a hand-kept list drifts. A node cannot advertise a capability
/// the document does not define, and a feature added without naming it here is
/// a visible omission.
#[test]
fn the_capability_set_is_the_documented_one() {
    use kimmy_api::version::Capability;

    let documented: BTreeSet<String> = spec()["components"]["schemas"]["Capability"]["enum"]
        .as_array()
        .expect("Capability is an enum")
        .iter()
        .map(|v| v.as_str().expect("a capability is a string").to_string())
        .collect();
    let known: BTreeSet<String> = Capability::ALL.iter().map(|c| c.as_str().to_string()).collect();

    assert_eq!(known.len(), Capability::ALL.len(), "two capabilities share a name");
    assert_eq!(known, documented, "the server's capabilities and the specification's disagree");

    // Every capability is also explained, not just listed. A bare name tells a
    // client nothing about what it may then do.
    let table = spec()["components"]["schemas"]["Capability"]["description"]
        .as_str()
        .expect("the Capability schema explains its values");
    for capability in Capability::ALL {
        assert!(
            table.contains(&format!("| `{}` |", capability.as_str())),
            "{} is advertised with no explanation of what it means",
            capability.as_str()
        );
    }
}

// ---------------------------------------------------------------------------
// Schema validation
// ---------------------------------------------------------------------------

/// Follow a local `$ref`, once. Everything in this document refers to itself.
fn resolve<'a>(root: &'a Value, node: &'a Value) -> &'a Value {
    let Some(reference) = node.get("$ref").and_then(Value::as_str) else {
        return node;
    };
    let pointer = reference.strip_prefix('#').expect("only local references are used");
    root.pointer(pointer).unwrap_or_else(|| panic!("dangling reference {reference}"))
}

/// Validate a response body against the schema the spec declares for it.
///
/// An undocumented status is a failure rather than a skip: the server produced
/// it, so the document is incomplete.
fn validate_response(method: &str, template: &str, status: u16, body: &Value) {
    let spec = spec();
    let operation = &spec["paths"][template][method.to_lowercase()];
    assert!(
        !operation.is_null(),
        "docs/openapi.yaml documents no {method} for {template}, but the router answers it"
    );

    let response = &operation["responses"][status.to_string()];
    assert!(
        !response.is_null(),
        "docs/openapi.yaml documents no {status} for {method} {template}, but the server \
         returned one: {body}"
    );
    let response = resolve(spec, response);

    let schema = &response["content"]["application/json"]["schema"];
    if schema.is_null() {
        // A documented response with no JSON body — a 101 upgrade, or a
        // non-JSON payload checked by its own assertion at the call site.
        return;
    }

    // The validator is handed a document that carries the spec's `components`
    // alongside the schema, so `#/components/schemas/...` resolves.
    let root = json!({ "allOf": [schema], "components": spec["components"] });
    let validator = jsonschema::validator_for(&root)
        .unwrap_or_else(|e| panic!("the schema for {method} {template} {status} is invalid: {e}"));

    let errors: Vec<String> = validator.iter_errors(body).map(|e| format!("  {e}")).collect();
    assert!(
        errors.is_empty(),
        "the {status} response to {method} {template} does not match its schema:\n{}\n\nbody: {}",
        errors.join("\n"),
        serde_json::to_string_pretty(body).unwrap_or_default()
    );
}

// ---------------------------------------------------------------------------
// A server, and a client that can see the raw response
// ---------------------------------------------------------------------------

struct Res {
    status: u16,
    head: String,
    raw: Vec<u8>,
}

impl Res {
    fn json(&self) -> Value {
        let text = String::from_utf8_lossy(&self.raw);
        serde_json::from_str(text.trim()).unwrap_or(Value::Null)
    }

    fn header(&self, name: &str) -> Option<String> {
        let want = name.to_ascii_lowercase();
        self.head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key.trim().to_ascii_lowercase() == want).then(|| value.trim().to_string())
        })
    }
}

/// A tiny HTTP client.
///
/// Hand-rolled for the same reason `tests/api.rs` and `tests/webhooks.rs` each
/// have one: it needs to do something a client library makes awkward — here,
/// keep the raw bytes and the raw head, because `/metrics` is text and
/// `/v1/admin/backup` is binary and both are part of the contract.
struct Server {
    base: String,
    _dir: tempfile::TempDir,
}

impl Server {
    async fn start() -> Self {
        Self::build(kimmy_api::RateLimits::disabled()).await
    }

    async fn start_rate_limited(burst: u32) -> Self {
        let limits = kimmy_api::RateLimits {
            login_ip: kimmy_api::Limiter::new(
                kimmy_api::RateLimit::new(burst, std::time::Duration::from_secs(60)),
                1024,
            ),
            ..kimmy_api::RateLimits::disabled()
        };
        Self::build(limits).await
    }

    async fn build(limits: kimmy_api::RateLimits) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());

        let users = kimmy_auth::UserStore::open(&engine).unwrap();
        users.bootstrap_root(&engine, "root", ROOT_PASSWORD).unwrap();

        let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
        // Loopback is allowed out, so the documented webhook registration can
        // be exercised against an address that exists.
        let state = kimmy_api::state_with_egress(
            Arc::clone(&engine),
            tokens,
            false,
            limits,
            kimmy_api::egress::EgressPolicy::new(vec!["127.0.0.1".into()]),
        )
        .unwrap();
        let app = kimmy_api::router(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .await;
        });

        Self { base: format!("http://{addr}"), _dir: dir }
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<&Value>,
    ) -> Res {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let host = self.base.strip_prefix("http://").expect("http url");
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

        split(raw)
    }

    /// A real WebSocket handshake.
    ///
    /// A plain `GET` would be refused by the upgrade extractor with the
    /// framework's own plain-text rejection, which proves nothing about the
    /// documented `101` and would quietly stand in for it.
    async fn handshake(&self, path: &str, token: &str) -> Res {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let host = self.base.strip_prefix("http://").expect("http url");
        let mut stream = tokio::net::TcpStream::connect(host).await.expect("connect");

        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n\
             Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Authorization: Bearer {token}\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.expect("write");

        // An upgraded socket stays open, so read only as far as the head. A
        // read to end would wait for a change that is never coming.
        let mut raw = Vec::new();
        let mut byte = [0u8; 1];
        while !raw.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte).await {
                Ok(0) | Err(_) => break,
                Ok(_) => raw.push(byte[0]),
            }
        }

        split(raw)
    }
}

fn split(raw: Vec<u8>) -> Res {
    let terminator = raw.windows(4).position(|w| w == b"\r\n\r\n");
    let (head, body) = match terminator {
        Some(at) => (raw[..at].to_vec(), raw[at + 4..].to_vec()),
        None => (raw.clone(), Vec::new()),
    };
    let head = String::from_utf8_lossy(&head).into_owned();
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Res { status, head, raw: body }
}

// ---------------------------------------------------------------------------
// Driving every documented operation
// ---------------------------------------------------------------------------

struct Conformance {
    server: Server,
    covered: BTreeSet<(String, String)>,
}

impl Conformance {
    async fn start() -> Self {
        Self { server: Server::start().await, covered: BTreeSet::new() }
    }

    /// Drive one operation, check the status, and validate the body against
    /// the schema the specification declares for it.
    async fn check(
        &mut self,
        method: &str,
        template: &str,
        path: &str,
        token: Option<&str>,
        body: Option<Value>,
        want: u16,
    ) -> Value {
        let res = self.server.request(method, path, token, body.as_ref()).await;
        let payload = res.json();
        assert_eq!(res.status, want, "{method} {path} answered {} — {payload}", res.status);

        validate_response(method, template, res.status, &payload);
        self.covered.insert((method.to_string(), template.to_string()));
        payload
    }

    async fn login(&mut self, user: &str, password: &str) -> String {
        let body = self
            .check(
                "POST",
                "/v1/auth/login",
                "/v1/auth/login",
                None,
                Some(json!({ "user": user, "password": password })),
                200,
            )
            .await;
        body["token"].as_str().expect("a token").to_string()
    }
}

#[tokio::test]
async fn every_documented_operation_answers_as_the_specification_says() {
    let mut c = Conformance::start().await;

    // -- health, and the routes that need no token ------------------------
    c.check("GET", "/healthz", "/healthz", None, None, 200).await;
    c.check("GET", "/readyz", "/readyz", None, None, 200).await;

    let metrics = c.server.request("GET", "/metrics", None, None).await;
    assert_eq!(metrics.status, 200);
    assert!(
        metrics.header("content-type").is_some_and(|t| t.starts_with("text/plain")),
        "metrics must be Prometheus text, not JSON: {:?}",
        metrics.header("content-type")
    );
    assert!(String::from_utf8_lossy(&metrics.raw).contains("kimmy_up 1"));
    c.covered.insert(("GET".into(), "/metrics".into()));

    // Unauthenticated on purpose: a client negotiates before it holds a token.
    let advertised = c.check("GET", "/v1/version", "/v1/version", None, None, 200).await;
    assert_eq!(advertised["protocol"], kimmy_api::version::PROTOCOL);
    assert_eq!(advertised["version"], env!("CARGO_PKG_VERSION"));
    let served: Vec<&str> = advertised["capabilities"]
        .as_array()
        .expect("a list")
        .iter()
        .map(|c| c.as_str().unwrap())
        .collect();
    assert_eq!(
        served,
        kimmy_api::version::capabilities(),
        "the wire and the server's own list disagree"
    );
    assert!(
        !served.contains(&"local-embeddings"),
        "the default build has no in-process embedding, so it must not advertise it — \
         this is the capability that proves the list is answered rather than asserted"
    );

    // -- auth --------------------------------------------------------------
    let root = c.login("root", ROOT_PASSWORD).await;
    c.check("GET", "/v1/auth/whoami", "/v1/auth/whoami", Some(&root), None, 200).await;

    // -- users -------------------------------------------------------------
    let clerk_grants = json!([{ "db": "shop", "collection": "*", "actions": ["read"] }]);
    c.check(
        "POST",
        "/v1/users",
        "/v1/users",
        Some(&root),
        Some(json!({ "user": "clerk", "password": "clerk-password", "grants": clerk_grants })),
        201,
    )
    .await;
    c.check("GET", "/v1/users", "/v1/users", Some(&root), None, 200).await;
    c.check("GET", "/v1/users/{name}", "/v1/users/clerk", Some(&root), None, 200).await;
    c.check(
        "POST",
        "/v1/users/{name}/password",
        "/v1/users/clerk/password",
        Some(&root),
        Some(json!({ "password": "another-password" })),
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/users/{name}/grants",
        "/v1/users/clerk/grants",
        Some(&root),
        Some(json!({ "grants": clerk_grants })),
        200,
    )
    .await;

    // -- databases and collections ----------------------------------------
    c.check(
        "POST",
        "/v1/db/{db}/collections",
        "/v1/db/shop/collections",
        Some(&root),
        Some(json!({ "name": "orders" })),
        200,
    )
    .await;
    c.check("GET", "/v1/databases", "/v1/databases", Some(&root), None, 200).await;
    c.check("GET", "/v1/db/{db}/collections", "/v1/db/shop/collections", Some(&root), None, 200)
        .await;

    // -- documents ---------------------------------------------------------
    let docs = "/v1/db/shop/coll/orders/docs";
    let docs_t = "/v1/db/{db}/coll/{coll}/docs";
    c.check(
        "POST",
        docs_t,
        docs,
        Some(&root),
        Some(json!({ "_id": "a", "sku": "widget", "qty": 2, "note": "a small blue widget" })),
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/bulk",
        "/v1/db/shop/coll/orders/bulk",
        Some(&root),
        Some(json!([
            { "_id": "b", "sku": "sprocket", "qty": 1, "note": "a large red sprocket" },
            { "_id": "c", "sku": "gasket", "qty": 5, "note": "a gasket, sold in packs" },
        ])),
        200,
    )
    .await;
    c.check("GET", docs_t, &format!("{docs}?limit=2"), Some(&root), None, 200).await;

    let page = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/find",
            "/v1/db/shop/coll/orders/find",
            Some(&root),
            Some(json!({ "filter": { "qty": { "$gte": 1 } }, "limit": 2, "explain": true })),
            200,
        )
        .await;
    assert_eq!(page["count"], 2);
    assert!(page["nextCursor"].is_string(), "a full page offers a cursor: {page}");

    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/count",
        "/v1/db/shop/coll/orders/count",
        Some(&root),
        Some(json!({ "filter": {}, "explain": true })),
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/aggregate",
        "/v1/db/shop/coll/orders/aggregate",
        Some(&root),
        Some(json!({ "pipeline": [{ "$group": { "_id": "$sku", "total": { "$sum": "$qty" } } }] })),
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/update",
        "/v1/db/shop/coll/orders/update",
        Some(&root),
        Some(json!({ "filter": {}, "update": { "$set": { "channel": "web" } }, "multi": true,
                     "explain": true })),
        200,
    )
    .await;

    let modified = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/find_and_modify",
            "/v1/db/shop/coll/orders/find_and_modify",
            Some(&root),
            Some(json!({ "filter": { "_id": "a" }, "update": { "$set": { "status": "packed" } },
                         "returnDocument": "after" })),
            200,
        )
        .await;
    assert_eq!(modified["document"]["status"], "packed");

    let by_id = "/v1/db/shop/coll/orders/docs/a";
    let by_id_t = "/v1/db/{db}/coll/{coll}/docs/{id}";
    c.check("GET", by_id_t, by_id, Some(&root), None, 200).await;
    c.check(
        "PUT",
        by_id_t,
        &format!("{by_id}?upsert=true"),
        Some(&root),
        Some(json!({ "sku": "widget", "qty": 3, "note": "a small blue widget" })),
        200,
    )
    .await;

    // -- indexes -----------------------------------------------------------
    let index = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/indexes",
            "/v1/db/shop/coll/orders/indexes",
            Some(&root),
            Some(json!({ "fields": [{ "path": "sku" }], "unique": true })),
            200,
        )
        .await;
    let index_name = index["name"].as_str().expect("an index name").to_string();
    c.check(
        "GET",
        "/v1/db/{db}/coll/{coll}/indexes",
        "/v1/db/shop/coll/orders/indexes",
        Some(&root),
        None,
        200,
    )
    .await;

    // -- schema ------------------------------------------------------------
    c.check(
        "GET",
        "/v1/db/{db}/coll/{coll}/describe",
        "/v1/db/shop/coll/orders/describe?examples=true",
        Some(&root),
        None,
        200,
    )
    .await;

    // -- vectors -----------------------------------------------------------
    let vector = "/v1/db/shop/coll/orders/vector";
    let vector_t = "/v1/db/{db}/coll/{coll}/vector";
    c.check(
        "POST",
        vector_t,
        vector,
        Some(&root),
        Some(json!({ "fields": ["note"], "provider": { "kind": "byo" }, "dim": 3 })),
        200,
    )
    .await;
    c.check("GET", vector_t, vector, Some(&root), None, 200).await;

    let doc_vectors = "/v1/db/shop/coll/orders/docs/a/vectors";
    let doc_vectors_t = "/v1/db/{db}/coll/{coll}/docs/{id}/vectors";
    c.check(
        "PUT",
        doc_vectors_t,
        doc_vectors,
        Some(&root),
        Some(json!([{ "chunk": 0, "vector": [1.0, 0.0, 0.0], "text": "a small blue widget" }])),
        200,
    )
    .await;
    c.check("GET", doc_vectors_t, doc_vectors, Some(&root), None, 200).await;

    let hits = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/vector_search",
            "/v1/db/shop/coll/orders/vector_search",
            Some(&root),
            Some(json!({ "vector": [1.0, 0.0, 0.0], "k": 3 })),
            200,
        )
        .await;
    assert_eq!(hits["count"], 1, "the one stored chunk should be found: {hits}");
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/hybrid_search",
        "/v1/db/shop/coll/orders/hybrid_search",
        Some(&root),
        Some(json!({ "query": "blue widget", "vector": [1.0, 0.0, 0.0], "k": 3 })),
        200,
    )
    .await;

    // -- webhooks ----------------------------------------------------------
    let hooks = "/v1/db/shop/coll/orders/webhooks";
    let hooks_t = "/v1/db/{db}/coll/{coll}/webhooks";
    let registered = c
        .check(
            "POST",
            hooks_t,
            hooks,
            Some(&root),
            Some(json!({ "url": "http://127.0.0.1:9/hook", "operations": ["insert", "delete"] })),
            200,
        )
        .await;
    let hook_id = registered["id"].as_str().expect("a subscription id").to_string();
    c.check("GET", hooks_t, hooks, Some(&root), None, 200).await;

    // -- change streams ----------------------------------------------------
    let upgraded = c.server.handshake("/v1/db/shop/coll/orders/watch", &root).await;
    assert_eq!(upgraded.status, 101, "the watch route must upgrade: {}", upgraded.head);
    validate_response("GET", "/v1/db/{db}/coll/{coll}/watch", 101, &Value::Null);
    c.covered.insert(("GET".into(), "/v1/db/{db}/coll/{coll}/watch".into()));

    // -- backup ------------------------------------------------------------
    let backup = c.server.request("GET", "/v1/admin/backup", Some(&root), None).await;
    assert_eq!(backup.status, 200);
    assert_eq!(backup.header("content-type").as_deref(), Some("application/octet-stream"));
    assert!(
        backup.header("content-disposition").is_some_and(|d| d.starts_with("attachment;")),
        "a backup is served as an attachment"
    );
    assert!(!backup.raw.is_empty(), "a backup of a seeded node is not empty");
    c.covered.insert(("GET".into(), "/v1/admin/backup".into()));

    // -- teardown, which is also coverage of every remaining verb -----------
    c.check("DELETE", doc_vectors_t, doc_vectors, Some(&root), None, 200).await;
    c.check("DELETE", vector_t, &format!("{vector}?drop_vectors=true"), Some(&root), None, 200)
        .await;
    c.check(
        "DELETE",
        "/v1/db/{db}/coll/{coll}/webhooks/{id}",
        &format!("{hooks}/{hook_id}"),
        Some(&root),
        None,
        200,
    )
    .await;
    c.check(
        "DELETE",
        "/v1/db/{db}/coll/{coll}/indexes/{name}",
        &format!("/v1/db/shop/coll/orders/indexes/{index_name}"),
        Some(&root),
        None,
        200,
    )
    .await;
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/delete",
        "/v1/db/shop/coll/orders/delete",
        Some(&root),
        Some(json!({ "filter": { "_id": "c" } })),
        200,
    )
    .await;
    c.check("DELETE", by_id_t, "/v1/db/shop/coll/orders/docs/b", Some(&root), None, 200).await;
    c.check("DELETE", "/v1/users/{name}", "/v1/users/clerk", Some(&root), None, 200).await;
    c.check("DELETE", "/v1/db/{db}/coll/{coll}", "/v1/db/shop/coll/orders", Some(&root), None, 200)
        .await;

    // -- the gate ----------------------------------------------------------
    let documented = documented_operations();
    let missed: Vec<_> = documented.difference(&c.covered).collect();
    assert!(
        missed.is_empty(),
        "docs/openapi.yaml documents these operations and nothing here drives them: {missed:#?}"
    );
}

/// The refusals, which are as much a contract as the successes.
///
/// A client branches on these, so a status or an envelope that changes here is
/// a breaking change whether or not anyone meant it to be.
#[tokio::test]
async fn documented_refusals_use_the_documented_envelope() {
    let mut c = Conformance::start().await;
    let root = c.login("root", ROOT_PASSWORD).await;

    // No token at all.
    c.check("GET", "/v1/databases", "/v1/databases", None, None, 401).await;

    // Wrong password: indistinguishable from an unknown user, by design.
    c.check(
        "POST",
        "/v1/auth/login",
        "/v1/auth/login",
        None,
        Some(json!({ "user": "root", "password": "wrong" })),
        401,
    )
    .await;

    // A password policy failure is a 400, not a 500.
    c.check(
        "POST",
        "/v1/users",
        "/v1/users",
        Some(&root),
        Some(json!({ "user": "short", "password": "tiny" })),
        400,
    )
    .await;

    // Held by a principal who may read one collection and administer nothing.
    c.check(
        "POST",
        "/v1/users",
        "/v1/users",
        Some(&root),
        Some(json!({ "user": "clerk", "password": "clerk-password",
                     "grants": [{ "db": "shop", "collection": "*", "actions": ["read"] }] })),
        201,
    )
    .await;
    let clerk = c.login("clerk", "clerk-password").await;
    c.check("GET", "/v1/users", "/v1/users", Some(&clerk), None, 403).await;

    // A collection that does not exist.
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/find",
        "/v1/db/shop/coll/ghost/find",
        Some(&root),
        Some(json!({ "filter": {} })),
        404,
    )
    .await;

    // A duplicate `_id`.
    c.check(
        "POST",
        "/v1/db/{db}/collections",
        "/v1/db/shop/collections",
        Some(&root),
        Some(json!({ "name": "orders" })),
        200,
    )
    .await;
    let docs = "/v1/db/shop/coll/orders/docs";
    let docs_t = "/v1/db/{db}/coll/{coll}/docs";
    c.check("POST", docs_t, docs, Some(&root), Some(json!({ "_id": "a" })), 200).await;
    let conflict =
        c.check("POST", docs_t, docs, Some(&root), Some(json!({ "_id": "a" })), 409).await;
    assert_eq!(conflict["error"], "duplicate_key", "the code is what a client branches on");
    assert_eq!(conflict["retry"], "no", "a duplicate key does not become un-duplicate");

    // Valid JSON of the wrong shape. 422 rather than 400, and it was in the
    // HTTP reference but in no specification until the taxonomy was written
    // down: `/bulk` takes an array, and an object is not one.
    let shape = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/bulk",
            "/v1/db/shop/coll/orders/bulk",
            Some(&root),
            Some(json!({ "_id": "b" })),
            422,
        )
        .await;
    assert_eq!(shape["error"], "bad_request");

    // A wrong-shaped body on a route that is *not* `/bulk`. Until the
    // extractor carried the mapping, `/bulk` was the only one of nineteen
    // handlers that reached it, and every other route answered 422 as bare
    // text with no code — outside the taxonomy entirely. Found by driving a
    // real node, because this scenario had only ever been run against `/bulk`.
    let typed = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/vector",
            "/v1/db/shop/coll/orders/vector",
            Some(&root),
            Some(json!({ "fields": "title", "provider": { "kind": "byo" }, "dim": 3 })),
            422,
        )
        .await;
    assert_eq!(typed["error"], "bad_request", "a wrong-shaped body is still in the envelope");
    assert_eq!(typed["retry"], "no");

    // `no_vectors`: the code that existed in the server and in neither
    // document. A search against a collection nobody ingested vectors for is a
    // refusal, not an empty result — an empty result is indistinguishable from
    // "nothing matched", which is how a `byo` collection silently returns
    // nothing forever.
    c.check(
        "POST",
        "/v1/db/{db}/coll/{coll}/vector",
        "/v1/db/shop/coll/orders/vector",
        Some(&root),
        Some(json!({ "fields": ["title"], "provider": { "kind": "byo" }, "dim": 3 })),
        200,
    )
    .await;
    let empty = c
        .check(
            "POST",
            "/v1/db/{db}/coll/{coll}/vector_search",
            "/v1/db/shop/coll/orders/vector_search",
            Some(&root),
            Some(json!({ "vector": [1.0, 0.0, 0.0] })),
            409,
        )
        .await;
    assert_eq!(empty["error"], "no_vectors");
    assert_eq!(empty["retry"], "no", "ingesting vectors is the fix, not repeating the search");

    // A plain GET to the change-stream route. Found by driving a real node:
    // the framework's own rejection is bare text with no code, so this was the
    // one refusal on the API a client could not branch on. Same fix, and the
    // same reason, as the JSON body rejection.
    let not_upgraded = c
        .check(
            "GET",
            "/v1/db/{db}/coll/{coll}/watch",
            "/v1/db/shop/coll/orders/watch",
            Some(&root),
            None,
            400,
        )
        .await;
    assert_eq!(not_upgraded["error"], "bad_request");
    assert_eq!(not_upgraded["retry"], "no");
}

/// A 429 carries `Retry-After`, and the spec says so.
///
/// Its own server: the limiter has to be on, and every other test would trip
/// over it for reasons unrelated to what they assert.
#[tokio::test]
async fn a_rate_limited_login_matches_its_documented_response() {
    let server = Server::start_rate_limited(1).await;
    let attempt = json!({ "user": "root", "password": "wrong" });

    let first = server.request("POST", "/v1/auth/login", None, Some(&attempt)).await;
    assert_eq!(first.status, 401);

    let limited = server.request("POST", "/v1/auth/login", None, Some(&attempt)).await;
    assert_eq!(limited.status, 429, "the second attempt is over the limit");
    let body = limited.json();
    validate_response("POST", "/v1/auth/login", 429, &body);
    assert!(
        limited.header("retry-after").is_some(),
        "the spec declares Retry-After on a 429, and a refusal without one leaves a client \
         to guess"
    );

    // The one code whose answer is "the same node, later". `Retry-After` says
    // how much later; the class is what tells a client to wait at all rather
    // than moving on to a peer that shares nothing about this limit.
    assert_eq!(body["error"], "rate_limited");
    assert_eq!(body["retry"], "wait");
}

/// The document is a specification, so it has to be one.
#[test]
fn the_specification_is_well_formed() {
    let spec = spec();
    assert_eq!(spec["openapi"], "3.1.0");
    assert!(spec["info"]["version"].is_string());

    // Every `$ref` in the document resolves. A dangling one makes a generated
    // client fail at generation time, in a message about a pointer.
    let mut dangling = Vec::new();
    walk_refs(spec, spec, &mut dangling);
    assert!(dangling.is_empty(), "these references do not resolve: {dangling:#?}");
}

fn walk_refs(root: &Value, node: &Value, dangling: &mut Vec<String>) {
    match node {
        Value::Object(map) => {
            for (key, value) in map {
                if key == "$ref"
                    && let Some(reference) = value.as_str()
                {
                    match reference.strip_prefix('#') {
                        Some(pointer) if root.pointer(pointer).is_some() => {}
                        _ => dangling.push(reference.to_string()),
                    }
                }
                walk_refs(root, value, dangling);
            }
        }
        Value::Array(items) => {
            for item in items {
                walk_refs(root, item, dangling);
            }
        }
        _ => {}
    }
}
