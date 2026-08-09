//! Webhook delivery, against a receiver on a real socket.
//!
//! The point of testing it this way rather than by calling `deliver` directly:
//! a webhook is an outbound HTTP request, and everything interesting about it —
//! the signature a receiver has to verify, the headers it reads, whether the
//! egress policy lets it out at all — only exists on the wire.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bson::doc;
use kimmy_api::dispatch;
use kimmy_api::egress::EgressPolicy;
use kimmy_auth::TokenIssuer;
use kimmy_storage::Engine;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SECRET: &str = "an-adequately-long-test-secret";

/// What a delivery looked like from the receiving end.
#[derive(Clone, Debug, Default)]
struct Received {
    body: String,
    signature: String,
    timestamp: String,
    event_id: String,
}

/// A receiver that records what arrives and answers with `status`.
///
/// Hand-rolled rather than a framework: it has to be able to answer 500 on
/// demand, and the whole thing is thirty lines of socket handling.
async fn receiver(
    status: u16,
) -> (std::net::SocketAddr, Arc<Mutex<Vec<Received>>>, Arc<AtomicUsize>) {
    let seen: Arc<Mutex<Vec<Received>>> = Arc::default();
    let hits = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let recorded = Arc::clone(&seen);
    let counted = Arc::clone(&hits);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let recorded = Arc::clone(&recorded);
            let counted = Arc::clone(&counted);
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                // Read until the body is complete, using Content-Length.
                loop {
                    let n = match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    raw.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&raw).into_owned();
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let len: usize = head
                            .lines()
                            .find_map(|l| {
                                l.to_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().to_string())
                            })
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0);
                        if body.len() >= len {
                            let header = |name: &str| {
                                head.lines()
                                    .find_map(|l| {
                                        let (k, v) = l.split_once(':')?;
                                        (k.trim().eq_ignore_ascii_case(name))
                                            .then(|| v.trim().to_string())
                                    })
                                    .unwrap_or_default()
                            };
                            recorded.lock().push(Received {
                                body: body.to_string(),
                                signature: header("x-kimmy-signature"),
                                timestamp: header("x-kimmy-timestamp"),
                                event_id: header("x-kimmy-event-id"),
                            });
                            counted.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });

    (addr, seen, hits)
}

/// An engine plus API state, with an egress policy that permits the receiver.
fn state_for(dir: &tempfile::TempDir) -> kimmy_api::SharedState {
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
    // 127.0.0.1 is refused by default -- that is the SSRF guard doing its job.
    // Allowlisting it here is exactly the escape hatch an operator would use,
    // so the test exercises that path too.
    kimmy_api::state_with_egress(
        engine,
        tokens,
        false,
        kimmy_api::RateLimits::disabled(),
        EgressPolicy::new(vec!["127.0.0.1".into()]),
    )
    .unwrap()
}

/// Register a subscription directly, without going through HTTP.
fn register(state: &kimmy_api::SharedState, url: &str, ops: Vec<&str>) -> String {
    let meta = state
        .engine
        .create_system_collection("__kimmy", "__webhooks")
        .unwrap_or_else(|_| state.engine.get_collection("__kimmy", "__webhooks").unwrap());
    let id = format!("wh_test_{}", uuid::Uuid::new_v4().simple());
    state
        .engine
        .insert(
            &meta,
            doc! {
                "_id": id.clone(),
                "database": "shop",
                "collection": "orders",
                "url": url.to_string(),
                "operations": ops.iter().map(|o| o.to_string()).collect::<Vec<_>>(),
                "secret": "test-webhook-secret",
                "createdBy": "root",
                "createdMs": 1i64,
            },
        )
        .unwrap();
    id
}

fn me() -> std::net::SocketAddr {
    "127.0.0.1:7900".parse().unwrap()
}

async fn pass(state: &kimmy_api::SharedState) -> dispatch::DispatchOutcome {
    let client =
        reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let policy = EgressPolicy::new(vec!["127.0.0.1".into()]);
    dispatch::dispatch_once(state, &client, &policy, me(), &BTreeSet::new()).await
}

#[tokio::test]
async fn a_write_is_delivered_and_the_signature_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, seen, _) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1, "item": "widget" }).unwrap();

    let outcome = pass(&state).await;
    assert_eq!(outcome.delivered, 1, "{outcome:?}");

    let deliveries = seen.lock().clone();
    assert_eq!(deliveries.len(), 1, "the receiver should have been called once");
    let delivery = &deliveries[0];

    // The signature is the whole point of the secret: without verifying it, a
    // receiver cannot tell a genuine delivery from anything that can reach its
    // URL. Recomputed here exactly as a real consumer would.
    let timestamp: u64 = delivery.timestamp.parse().expect("a timestamp header");
    let expected = dispatch::sign("test-webhook-secret", timestamp, &delivery.body);
    assert_eq!(delivery.signature, expected, "the signature must verify");

    assert!(!delivery.event_id.is_empty(), "a delivery must carry an event id");
    assert!(delivery.body.contains("\"operationType\":\"insert\""), "{}", delivery.body);
    assert!(delivery.body.contains("widget"), "the document should be included: {}", delivery.body);
}

#[tokio::test]
async fn a_tampered_body_fails_the_signature() {
    // The property a receiver actually relies on. If this ever passes, the
    // signature is decoration.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, seen, _) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);
    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    pass(&state).await;

    let delivery = seen.lock()[0].clone();
    let timestamp: u64 = delivery.timestamp.parse().unwrap();
    let tampered = delivery.body.replace("insert", "delete");
    assert_ne!(
        dispatch::sign("test-webhook-secret", timestamp, &tampered),
        delivery.signature,
        "a changed body must not keep its signature"
    );
}

#[tokio::test]
async fn nothing_is_delivered_twice() {
    // Progress is recorded after a successful delivery, so a second pass over
    // the same oplog must find nothing left to send.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, hits) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    for i in 0..3i64 {
        state.engine.insert(&coll, doc! { "_id": i }).unwrap();
    }

    assert_eq!(pass(&state).await.delivered, 3);
    assert_eq!(pass(&state).await.delivered, 0, "a second pass must send nothing");
    assert_eq!(hits.load(Ordering::Relaxed), 1, "one batch, one request");
}

#[tokio::test]
async fn a_failed_delivery_does_not_advance_progress() {
    // The other half: recording progress before the endpoint accepted would
    // turn a failed delivery into a silently skipped event.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, hits) = receiver(500).await;
    let id = register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();

    let outcome = pass(&state).await;
    assert_eq!(outcome.failed, 1, "{outcome:?}");
    assert_eq!(outcome.delivered, 0);
    assert!(
        dispatch::union_progress(&state, &id).is_empty(),
        "a refused delivery must leave progress untouched"
    );

    // ...and it is retried on the next pass rather than lost.
    pass(&state).await;
    assert_eq!(hits.load(Ordering::Relaxed), 2, "the event must be retried");
}

#[tokio::test]
async fn an_operation_filter_is_honoured_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, seen, _) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec!["delete"]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    assert_eq!(pass(&state).await.delivered, 0, "an insert must not reach a delete-only hook");

    state.engine.delete(&coll, &kimmy_core::DocId::Int64(1)).unwrap();
    assert_eq!(pass(&state).await.delivered, 1);
    assert!(seen.lock()[0].body.contains("\"operationType\":\"delete\""));
}

#[tokio::test]
async fn a_node_that_does_not_own_a_subscription_delivers_nothing() {
    // Ownership is what stops five nodes sending five copies. Here the member
    // set deliberately excludes this node, so it must stand down.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, hits) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);
    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();

    let others: BTreeSet<std::net::SocketAddr> =
        ["10.0.0.1:7900", "10.0.0.2:7900"].iter().map(|a| a.parse().unwrap()).collect();
    let client = reqwest::Client::new();
    let policy = EgressPolicy::new(vec!["127.0.0.1".into()]);
    let outcome = dispatch::dispatch_once(&state, &client, &policy, me(), &others).await;

    assert_eq!(outcome.skipped_not_owner, 1, "{outcome:?}");
    assert_eq!(outcome.delivered, 0);
    assert_eq!(hits.load(Ordering::Relaxed), 0, "a non-owner must not call the endpoint");
}

#[tokio::test]
async fn the_egress_policy_is_enforced_at_delivery_not_only_at_registration() {
    // A hostname is not a destination. This registers while the policy permits
    // the host and then delivers under a policy that does not — which is what a
    // DNS record changing under a live subscription looks like.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, hits) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);
    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();

    let client = reqwest::Client::new();
    let closed = EgressPolicy::default(); // 127.0.0.1 no longer permitted
    let outcome = dispatch::dispatch_once(&state, &client, &closed, me(), &BTreeSet::new()).await;

    assert_eq!(outcome.failed, 1, "{outcome:?}");
    assert_eq!(hits.load(Ordering::Relaxed), 0, "nothing may reach a now-forbidden address");
}
