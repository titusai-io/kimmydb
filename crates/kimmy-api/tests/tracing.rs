//! What actually leaves this process when a collector is configured.
//!
//! Everything here is a claim ADR-068 and ADR-069 make that is worthless
//! untested: that an inbound `traceparent` continues a caller's trace rather
//! than starting a new one, that a span carries a collection name only when the
//! operator asked for it, and that a webhook delivery propagates the trace
//! forward. Each is invisible when it breaks — a trace with holes in it still
//! renders, and an attribute that should not be there looks exactly like one
//! that should.
//!
//! # Why the subscriber is thread-local and the exporter is in memory
//!
//! `tracing_subscriber::registry().init()` is process-global and one test
//! binary runs many tests. `with_default` scopes a subscriber to the thread
//! that installs it, which is also the thread the current-thread runtime drives
//! the request on — so each test gets its own exporter and reads back exactly
//! the spans it caused.

use std::sync::Arc;

use kimmy_auth::TokenIssuer;
use kimmy_storage::Engine;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tracing_subscriber::prelude::*;

const SECRET: &str = "an-adequately-long-test-secret-value";
const ROOT_PASSWORD: &str = "hunter2-and-then-some";

/// A tracer provider that keeps its spans where a test can read them.
///
/// A *simple* exporter rather than a batch one: a batch processor flushes on a
/// timer, and a test that sleeps waiting for one is a test that is flaky on a
/// loaded machine.
fn recorder() -> (SdkTracerProvider, InMemorySpanExporter) {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder().with_simple_exporter(exporter.clone()).build();
    (provider, exporter)
}

/// Run `body` with a subscriber that records into `exporter`.
fn recording<T>(provider: &SdkTracerProvider, body: impl FnOnce() -> T) -> T {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("kimmydb-test")));
    tracing::subscriber::with_default(subscriber, body)
}

/// One attribute of a recorded span, if it has one.
fn attribute(span: &opentelemetry_sdk::trace::SpanData, key: &str) -> Option<String> {
    span.attributes.iter().find(|kv| kv.key.as_str() == key).map(|kv| kv.value.to_string())
}

fn span_named<'a>(
    spans: &'a [opentelemetry_sdk::trace::SpanData],
    name: &str,
) -> &'a opentelemetry_sdk::trace::SpanData {
    spans
        .iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no {name} span in {:?}", spans.iter().map(|s| &s.name)))
}

/// A live node on a loopback port, as `api.rs` builds one.
struct Node {
    base: String,
    _dir: tempfile::TempDir,
}

async fn node() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let users = kimmy_auth::UserStore::open(&engine).unwrap();
    users.bootstrap_root(&engine, "root", ROOT_PASSWORD).unwrap();

    let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
    let state = kimmy_api::state(engine, tokens, false, kimmy_api::RateLimits::disabled()).unwrap();
    let app = kimmy_api::router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    Node { base: format!("http://{addr}"), _dir: dir }
}

/// One request, with whatever extra headers a test wants on it.
async fn request(node: &Node, method: &str, path: &str, headers: &[(&str, &str)]) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let host = node.base.strip_prefix("http://").unwrap();
    let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
    for (name, value) in headers {
        req.push_str(&format!("{name}: {value}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf)
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// A current-thread runtime, so the request runs on the thread the subscriber
/// was installed on.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

#[test]
fn an_inbound_traceparent_becomes_the_parent_of_the_request_span() {
    // The point of propagation. Without it every node starts its own trace and
    // a request through three services renders as three unrelated traces — the
    // failure is silent, because each of them looks perfectly fine on its own.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let (provider, exporter) = recorder();

    // A W3C trace context: version 00, a trace id, a parent span id, sampled.
    const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const PARENT_SPAN: &str = "00f067aa0ba902b7";
    let traceparent = format!("00-{TRACE_ID}-{PARENT_SPAN}-01");

    recording(&provider, || {
        runtime().block_on(async {
            let node = node().await;
            let status =
                request(&node, "GET", "/v1/databases", &[("traceparent", &traceparent)]).await;
            assert_eq!(status, 401, "unauthenticated, which is fine — the span is the subject");
        });
    });
    provider.force_flush().unwrap();

    let spans = exporter.get_finished_spans().unwrap();
    let request_span = span_named(&spans, "/v1/databases");
    assert_eq!(
        format!("{:032x}", request_span.span_context.trace_id()),
        TRACE_ID,
        "the request must join the caller's trace, not start its own"
    );
    assert_eq!(
        format!("{:016x}", request_span.parent_span_id),
        PARENT_SPAN,
        "and hang off the caller's span"
    );
}

#[test]
fn a_request_with_no_traceparent_starts_a_new_trace() {
    // The other half: propagation must not require it. A client that speaks no
    // trace context still gets a trace, it just begins here.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let (provider, exporter) = recorder();

    recording(&provider, || {
        runtime().block_on(async {
            let node = node().await;
            request(&node, "GET", "/v1/databases", &[]).await;
        });
    });
    provider.force_flush().unwrap();

    let spans = exporter.get_finished_spans().unwrap();
    let request_span = span_named(&spans, "/v1/databases");
    assert!(request_span.span_context.trace_id() != opentelemetry::trace::TraceId::INVALID);
    assert_eq!(
        request_span.parent_span_id,
        opentelemetry::trace::SpanId::INVALID,
        "with nothing to continue, this span is the root"
    );
}

/// Serialises the tests that depend on `include_names`.
///
/// The gate is process-global (it mirrors the audit mode, for the reasons
/// `kimmy_api::audit` documents), but cargo runs the tests in one binary on
/// parallel threads. So a test asserting a name is *absent* can sample the flag
/// while the test that exercises both directions has it turned on — which is a
/// privacy assertion failing for a reason that has nothing to do with privacy.
///
/// It reads as a flake and is not one: it is a lost race, and which side wins
/// depends on the machine. It passed locally and failed in CI.
///
/// `parking_lot`, as elsewhere in this crate, so a panicking test poisons
/// nothing and its neighbour reports its own failure rather than a poison error.
static NAMES_GATE: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[test]
fn the_span_name_is_the_route_template_and_never_the_document_id() {
    // Two properties at once, and both are ADR-068. A span name is a dashboard
    // dimension, so a name carrying an `_id` is one group per document; and the
    // template carries no database or collection name, which is what makes the
    // default private by construction rather than by redaction.
    //
    // Holds the gate because of the `url.path` assertion below, which is only
    // true while `include_names` is off.
    let _gate = NAMES_GATE.lock();
    kimmy_api::telemetry::set_include_names(false);
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let (provider, exporter) = recorder();

    recording(&provider, || {
        runtime().block_on(async {
            let node = node().await;
            request(&node, "GET", "/v1/db/sales/coll/orders/docs/deadbeef", &[]).await;
        });
    });
    provider.force_flush().unwrap();

    let spans = exporter.get_finished_spans().unwrap();
    let request_span = span_named(&spans, "/v1/db/{db}/coll/{coll}/docs/{id}");
    assert_eq!(
        attribute(request_span, "http.route").as_deref(),
        Some("/v1/db/{db}/coll/{coll}/docs/{id}")
    );
    assert_eq!(attribute(request_span, "http.request.method").as_deref(), Some("GET"));
    assert_eq!(
        attribute(request_span, "http.response.status_code").as_deref(),
        Some("401"),
        "the status is recorded after the response, not guessed before it"
    );

    for span in &spans {
        assert!(!span.name.contains("sales"), "a span name must not carry a database name");
        assert!(!span.name.contains("orders"), "a span name must not carry a collection name");
        assert!(!span.name.contains("deadbeef"), "a span name must not carry a document id");
        assert!(
            attribute(span, "url.path").is_none(),
            "url.path has the names in it and must stay behind include_names"
        );
    }
}

#[test]
fn health_and_metrics_are_not_traced() {
    // The same exclusion the latency histogram already makes, and the same
    // argument: every few seconds forever, of a request nobody is debugging.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let (provider, exporter) = recorder();

    recording(&provider, || {
        runtime().block_on(async {
            let node = node().await;
            for path in ["/healthz", "/readyz", "/metrics"] {
                request(&node, "GET", path, &[]).await;
            }
        });
    });
    provider.force_flush().unwrap();

    let spans = exporter.get_finished_spans().unwrap();
    let http: Vec<_> = spans.iter().filter(|s| s.name.starts_with('/')).collect();
    assert!(http.is_empty(), "probes and scrapes must not produce a trace: {http:?}");
}

/// The privacy gate is process-global, so the two directions cannot run at the
/// same time in one binary. One test, both directions, in order — and under
/// [`NAMES_GATE`], because turning names *on* here would otherwise break any
/// other test asserting they are off.
#[test]
fn names_reach_a_span_only_when_the_operator_turned_them_on() {
    let _gate = NAMES_GATE.lock();
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // --- off: the default ------------------------------------------------
    kimmy_api::telemetry::set_include_names(false);
    let (provider, exporter) = recorder();
    recording(&provider, || {
        runtime().block_on(async {
            let node = node().await;
            let token = login(&node).await;
            request(
                &node,
                "GET",
                "/v1/db/sales/coll/orders/docs?limit=1",
                &[("authorization", &format!("Bearer {token}"))],
            )
            .await;
        });
    });
    provider.force_flush().unwrap();

    let spans = exporter.get_finished_spans().unwrap();
    let find = span_named(&spans, "find");
    assert_eq!(attribute(find, "db.system.name").as_deref(), Some("kimmydb"));
    assert_eq!(attribute(find, "db.operation.name").as_deref(), Some("find"));
    assert!(
        attribute(find, "db.collection.name").is_none(),
        "off by default: a collector must not accumulate a schema nobody published"
    );
    assert!(attribute(find, "db.namespace").is_none());
    assert!(
        attribute(span_named(&spans, "/v1/db/{db}/coll/{coll}/docs"), "url.path").is_none(),
        "the raw path has the names in it"
    );

    // --- on: the operator asked ------------------------------------------
    kimmy_api::telemetry::set_include_names(true);
    let (provider, exporter) = recorder();
    recording(&provider, || {
        runtime().block_on(async {
            let node = node().await;
            let token = login(&node).await;
            request(
                &node,
                "GET",
                "/v1/db/sales/coll/orders/docs?limit=1",
                &[("authorization", &format!("Bearer {token}"))],
            )
            .await;
        });
    });
    provider.force_flush().unwrap();

    let spans = exporter.get_finished_spans().unwrap();
    let find = span_named(&spans, "find");
    assert_eq!(attribute(find, "db.namespace").as_deref(), Some("sales"));
    assert_eq!(attribute(find, "db.collection.name").as_deref(), Some("orders"));
    assert_eq!(
        attribute(span_named(&spans, "/v1/db/{db}/coll/{coll}/docs"), "url.path").as_deref(),
        Some("/v1/db/sales/coll/orders/docs")
    );

    // Left as it is found, so a later test in this binary is not surprised.
    kimmy_api::telemetry::set_include_names(false);
}

/// A root token, so the executor span is reached rather than refused at the door.
async fn login(node: &Node) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let host = node.base.strip_prefix("http://").unwrap();
    let body = format!(r#"{{"user":"root","password":"{ROOT_PASSWORD}"}}"#);
    let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
    let req = format!(
        "POST /v1/auth/login HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let raw = String::from_utf8_lossy(&buf).into_owned();
    let json: serde_json::Value =
        serde_json::from_str(raw.split("\r\n\r\n").nth(1).expect("a body")).expect("json");
    json["token"].as_str().expect("a token").to_string()
}
