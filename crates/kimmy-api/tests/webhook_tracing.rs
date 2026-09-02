//! Outbound trace propagation on a webhook delivery, both directions.
//!
//! # Why this is its own test binary, and its own single test
//!
//! `tracing` caches a callsite's `Interest` **globally**, once, from whichever
//! subscriber happened to be installed when the callsite was first reached.
//! Alongside the two dozen tests in `webhooks.rs` that drive `deliver` with no
//! subscriber at all, the `webhook.deliver` callsite is registered as
//! uninteresting before this test ever gets to install one — and then no span
//! exists to propagate. It fails only under a loaded parallel run, which is the
//! worst kind of flake to leave behind.
//!
//! A separate integration test is a separate process, so the cache starts
//! empty. One test in it, because the propagator and the subscriber are
//! process-global too: "nothing is installed" is a state that exists exactly
//! once, before anything installs one, and splitting the two halves would make
//! the second depend on the order the harness ran them in.

use std::sync::Arc;

use bson::doc;
use kimmy_api::dispatch;
use kimmy_api::egress::EgressPolicy;
use kimmy_auth::TokenIssuer;
use kimmy_storage::Engine;
use opentelemetry::trace::TracerProvider as _;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing_subscriber::prelude::*;

const SECRET: &str = "an-adequately-long-test-secret-for-hs256";
const WEBHOOK_SECRET: &str = "test-webhook-secret";

/// What a delivery looked like from the receiving end.
#[derive(Clone, Debug, Default)]
struct Received {
    body: String,
    signature: String,
    timestamp: String,
    traceparent: String,
}

/// A receiver that records the headers it was called with and answers 200.
async fn receiver() -> (std::net::SocketAddr, Arc<Mutex<Vec<Received>>>) {
    let seen: Arc<Mutex<Vec<Received>>> = Arc::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let recorded = Arc::clone(&seen);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let recorded = Arc::clone(&recorded);
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let n = match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    raw.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&raw).into_owned();
                    let Some((head, body)) = text.split_once("\r\n\r\n") else { continue };
                    let header = |name: &str| {
                        head.lines()
                            .find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                k.trim().eq_ignore_ascii_case(name).then(|| v.trim().to_string())
                            })
                            .unwrap_or_default()
                    };
                    let len: usize = header("content-length").parse().unwrap_or(0);
                    if body.len() < len {
                        continue;
                    }
                    recorded.lock().push(Received {
                        body: body.to_string(),
                        signature: header("x-kimmy-signature"),
                        timestamp: header("x-kimmy-timestamp"),
                        traceparent: header("traceparent"),
                    });
                    break;
                }
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            });
        }
    });
    (addr, seen)
}

/// One insert, delivered once, as the receiver saw it.
fn deliver_one(runtime: &tokio::runtime::Runtime) -> Received {
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
        // 127.0.0.1 is refused by default — that is the SSRF guard doing its
        // job. Allowlisting it is the escape hatch a real operator would use.
        let policy = EgressPolicy::new(kimmy_api::egress::WEBHOOKS, vec!["127.0.0.1".into()]);
        let state = kimmy_api::state_with_egress(
            engine,
            tokens,
            false,
            kimmy_api::RateLimits::disabled(),
            policy.clone(),
        )
        .unwrap();

        let (addr, seen) = receiver().await;

        // Registered straight into the registry collection, as `webhooks.rs`
        // does: this test is about the headers on the wire, and going through
        // HTTP to get there would only add a token and a route to it.
        let meta = state.engine.create_system_collection("__kimmy", "__webhooks").unwrap();
        state
            .engine
            .insert(
                &meta,
                doc! {
                    "_id": "wh_trace",
                    "database": "shop",
                    "collection": "orders",
                    "url": format!("http://{addr}/hook"),
                    "operations": Vec::<String>::new(),
                    "secret": WEBHOOK_SECRET,
                    "createdBy": "root",
                    "createdMs": 1i64,
                },
            )
            .unwrap();

        let coll = state.engine.create_collection("shop", "orders").unwrap();
        state.engine.insert(&coll, doc! { "_id": 1, "item": "widget" }).unwrap();

        let client =
            reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
        let mut backoff = dispatch::Backoff::default();
        let outcome = dispatch::dispatch_once(
            &state,
            &client,
            &policy,
            kimmy_core::NodeId::from_bytes([0xAA; 16]),
            &std::collections::BTreeSet::new(),
            &mut backoff,
            dispatch::Limits::default(),
        )
        .await;
        assert_eq!(outcome.delivered, 1, "{outcome:?}");
        seen.lock().first().cloned().expect("one delivery")
    })
}

/// **Not signed.** `x-kimmy-signature` covers the body and the timestamp, which
/// is what replay protection needs; adding a header a tracing-aware proxy is
/// entitled to rewrite would turn every such hop into a delivery failure. A
/// receiver treats `traceparent` as a hint, never as evidence.
#[test]
fn a_delivery_carries_a_traceparent_only_when_this_node_is_tracing() {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    // --- nothing installed ------------------------------------------------
    //
    // A node with telemetry off must send exactly the headers it always did. A
    // `traceparent` naming an all-zero span would be worse than none: a
    // receiver would try to attach its work to a parent that does not exist.
    let before = deliver_one(&runtime);
    assert!(!before.signature.is_empty(), "the delivery itself still works");
    assert_eq!(before.traceparent, "", "with no tracer installed, nothing must be propagated");

    // --- propagator and tracer installed ----------------------------------
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(opentelemetry_sdk::trace::InMemorySpanExporter::default())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("kimmydb-test")));

    let after = tracing::subscriber::with_default(subscriber, || deliver_one(&runtime));

    // `00-<32 hex trace id>-<16 hex span id>-<2 hex flags>`, and neither id may
    // be the all-zero one that means "no trace".
    let parts: Vec<&str> = after.traceparent.split('-').collect();
    assert_eq!(parts.len(), 4, "not a W3C traceparent: {:?}", after.traceparent);
    assert_eq!(parts[0], "00", "version: {:?}", after.traceparent);
    assert_eq!(parts[1].len(), 32, "trace id: {:?}", after.traceparent);
    assert_ne!(parts[1], "0".repeat(32), "an all-zero trace id means no trace at all");
    assert_eq!(parts[2].len(), 16, "span id: {:?}", after.traceparent);
    assert_ne!(parts[2], "0".repeat(16), "an all-zero span id has nothing to hang off");

    // And the signature still covers what it always covered, unchanged by the
    // header added beside it.
    let timestamp: u64 = after.timestamp.parse().expect("a timestamp header");
    assert_eq!(
        after.signature,
        dispatch::sign(WEBHOOK_SECRET, timestamp, &after.body),
        "propagation must not disturb the signature a receiver verifies"
    );
}
