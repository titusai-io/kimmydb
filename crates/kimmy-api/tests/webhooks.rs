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
    receiver_taking(status, std::time::Duration::ZERO).await
}

/// The same receiver, but it takes `delay` to answer.
///
/// What a slow endpoint looks like from the dispatcher's side, which is the
/// only way to observe whether deliveries actually overlap.
async fn receiver_taking(
    status: u16,
    delay: std::time::Duration,
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
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
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
    register_as(state, &format!("wh_test_{}", uuid::Uuid::new_v4().simple()), url, ops)
}

/// Register under a chosen id.
///
/// The dispatcher plans subscriptions in `_id` order, so a test that cares
/// which one is reached first has to choose the ids rather than take a UUID.
fn register_as(state: &kimmy_api::SharedState, id: &str, url: &str, ops: Vec<&str>) -> String {
    let meta = state
        .engine
        .create_system_collection("__kimmy", "__webhooks")
        .unwrap_or_else(|_| state.engine.get_collection("__kimmy", "__webhooks").unwrap());
    let id = id.to_string();
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
    let mut backoff = dispatch::Backoff::default();
    pass_with(state, &mut backoff).await
}

/// A pass that carries backoff state across calls, for the tests that need it.
async fn pass_with(
    state: &kimmy_api::SharedState,
    backoff: &mut dispatch::Backoff,
) -> dispatch::DispatchOutcome {
    pass_under(state, backoff, dispatch::Limits::default()).await
}

/// A pass under chosen limits, for the concurrency and payload-cap tests.
async fn pass_under(
    state: &kimmy_api::SharedState,
    backoff: &mut dispatch::Backoff,
    limits: dispatch::Limits,
) -> dispatch::DispatchOutcome {
    let client =
        reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let policy = EgressPolicy::new(vec!["127.0.0.1".into()]);
    dispatch::dispatch_once(state, &client, &policy, me(), &BTreeSet::new(), backoff, limits).await
}

#[tokio::test]
async fn the_dispatcher_loop_delivers_without_being_driven() {
    // Every other test drives `dispatch_once` by hand, so a `run` that did
    // nothing at all passed the entire suite — found by mutation testing
    // (replace `run` with `()`). This spawns the loop the daemon spawns and
    // waits for it to deliver on its own tick.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, hits) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1, "item": "widget" }).unwrap();

    let handle = tokio::spawn(dispatch::run(
        state.clone(),
        EgressPolicy::new(vec!["127.0.0.1".into()]),
        me(),
        None,
        dispatch::Limits::default(),
    ));

    // The first pass runs before the first sleep, so this resolves fast; the
    // generous ceiling is for a loaded CI machine, not the loop's cadence.
    let mut delivered = false;
    for _ in 0..300 {
        if hits.load(Ordering::SeqCst) > 0 {
            delivered = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    handle.abort();
    assert!(delivered, "the dispatcher loop must deliver on its own");
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
    // Asserted on the wire, not on `render`'s output: these two shipped as
    // empty strings for all of M6, and every test passed, because nothing
    // looked at the body a receiver actually gets.
    assert!(delivery.body.contains("\"database\":\"shop\""), "{}", delivery.body);
    assert!(delivery.body.contains("\"collection\":\"orders\""), "{}", delivery.body);
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

    // ...and it is retried rather than lost. A fresh `Backoff` stands in for
    // the delay having elapsed; that the delay exists at all is the subject of
    // its own test.
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
    let mut backoff = dispatch::Backoff::default();
    let outcome = dispatch::dispatch_once(
        &state,
        &client,
        &policy,
        me(),
        &others,
        &mut backoff,
        dispatch::Limits::default(),
    )
    .await;

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
    let mut backoff = dispatch::Backoff::default();
    let outcome = dispatch::dispatch_once(
        &state,
        &client,
        &closed,
        me(),
        &BTreeSet::new(),
        &mut backoff,
        dispatch::Limits::default(),
    )
    .await;

    assert_eq!(outcome.failed, 1, "{outcome:?}");
    assert_eq!(hits.load(Ordering::Relaxed), 0, "nothing may reach a now-forbidden address");
}

#[tokio::test]
async fn a_failing_endpoint_backs_off_without_stalling_another() {
    // The reason backoff is per subscription. A shared one lets a single dead
    // endpoint slow every delivery on the node, which is the failure mode most
    // likely to be blamed on the database rather than on the endpoint.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (dead, _dead_seen, dead_hits) = receiver(500).await;
    let (live, _live_seen, live_hits) = receiver(200).await;
    register(&state, &format!("http://{dead}/hook"), vec![]);
    register(&state, &format!("http://{live}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();

    let mut backoff = dispatch::Backoff::default();
    let first = pass_with(&state, &mut backoff).await;
    assert_eq!(first.failed, 1, "the dead endpoint fails: {first:?}");
    assert_eq!(first.delivered, 1, "the live one is delivered anyway: {first:?}");

    // A second pass immediately after: the failing subscription is in backoff
    // and is skipped, while the healthy one is simply up to date.
    state.engine.insert(&coll, doc! { "_id": 2 }).unwrap();
    let second = pass_with(&state, &mut backoff).await;
    assert_eq!(second.skipped_backoff, 1, "the failing one must wait: {second:?}");
    assert_eq!(second.delivered, 1, "the healthy one must not have to: {second:?}");

    assert_eq!(dead_hits.load(Ordering::Relaxed), 1, "the dead endpoint was tried once");
    assert_eq!(live_hits.load(Ordering::Relaxed), 2, "the live endpoint kept its cadence");
}

#[tokio::test]
async fn a_subscription_that_falls_past_retention_is_invalidated_not_silently_gapped() {
    // Resuming from whatever is left would skip every collected event, and the
    // receiver could never tell. Mirrors the `410` a lagging change stream gets.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, hits) = receiver(500).await; // never succeeds, so it falls behind
    let id = register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    for i in 0..12i64 {
        state.engine.insert(&coll, doc! { "_id": i }).unwrap();
    }
    pass(&state).await; // fails; progress stays empty

    // Collect the oplog out from under it.
    let much_later = kimmy_storage::physical_now_ms() + 365 * 24 * 60 * 60 * 1000;
    state
        .engine
        .collect_garbage_at(much_later, kimmy_storage::RetentionPolicy::new(0, 24 * 60 * 60))
        .unwrap();

    let before = hits.load(Ordering::Relaxed);
    let outcome = pass(&state).await;
    assert_eq!(outcome.invalidated, 1, "{outcome:?}");

    // It is recorded on the subscription, so an operator sees it in a listing
    // rather than only in a log.
    let meta = state.engine.get_collection("__kimmy", "__webhooks").unwrap();
    let stored = state
        .engine
        .get(&meta, &kimmy_core::DocId::String(id.clone()))
        .unwrap()
        .expect("the subscription");
    assert_eq!(stored.get_str("state").unwrap(), "invalidated");
    assert!(stored.get_str("invalidReason").unwrap().contains("retention"));

    // ...and it stops trying.
    let after = pass(&state).await;
    assert_eq!(after.delivered, 0);
    assert_eq!(after.invalidated, 0, "invalidating is not repeated every pass");
    assert_eq!(hits.load(Ordering::Relaxed), before, "an invalidated hook must stop dialling");
}

#[tokio::test]
async fn a_new_subscription_does_not_replay_history() {
    // Registering a webhook on a busy collection used to answer with up to a
    // whole retention window of events the caller never asked for. A new
    // subscription hears about what happens next.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, seen, _) = receiver(200).await;

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    for i in 0..5i64 {
        state.engine.insert(&coll, doc! { "_id": i, "when": "before" }).unwrap();
    }

    // Registered through the real path, so the seeding runs.
    let auth = kimmy_api::state::Auth(kimmy_auth::Principal::new(
        "root",
        vec![kimmy_auth::Grant::superuser()],
    ));
    let request = kimmy_api::webhooks::RegisterRequest {
        url: format!("http://{addr}/hook"),
        operations: None,
    };
    kimmy_api::webhooks::register(
        &state,
        &auth,
        "shop",
        "orders",
        &request,
        &EgressPolicy::new(vec!["127.0.0.1".into()]),
    )
    .expect("registration");

    assert_eq!(pass(&state).await.delivered, 0, "history must not be replayed");

    state.engine.insert(&coll, doc! { "_id": 99, "when": "after" }).unwrap();
    assert_eq!(pass(&state).await.delivered, 1, "but new writes must arrive");
    assert!(seen.lock()[0].body.contains("after"), "{}", seen.lock()[0].body);
}

#[tokio::test]
async fn a_caught_up_subscription_survives_garbage_collection() {
    // A healthy subscription used to be invalidated by the first collection
    // pass: `behind` returns `None` once progress covers everything, that fell
    // through to `Hlc::ZERO`, and zero is behind any retention horizon. The
    // effect was that every working webhook died one retention window after it
    // was registered.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, hits) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    assert_eq!(pass(&state).await.delivered, 1, "delivered, so it is caught up");

    // A heartbeat of zero is a minute's worth of idling, compressed. In a
    // running node this is what happens on its own once a minute.
    let mut backoff = dispatch::Backoff::default();
    pass_under(&state, &mut backoff, beating()).await;

    // Collect everything the retention window allows — which spares the newest
    // entry, so the horizon lands at or below the position this subscription
    // has written forward to. It has missed nothing.
    state
        .engine
        .collect_garbage_at(
            kimmy_storage::physical_now_ms(),
            kimmy_storage::RetentionPolicy::new(0, 24 * 60 * 60),
        )
        .unwrap();

    let outcome = pass(&state).await;
    assert_eq!(outcome.invalidated, 0, "a caught-up subscription must survive GC: {outcome:?}");

    // ...and it still works afterwards.
    state.engine.insert(&coll, doc! { "_id": 2 }).unwrap();
    assert_eq!(pass(&state).await.delivered, 1);
    assert_eq!(hits.load(Ordering::Relaxed), 2);
}

/// Limits whose progress heartbeat has already elapsed.
///
/// The resume point is written forward once a minute in a running node; a test
/// that would otherwise have to sleep for one sets the interval to zero.
fn beating() -> dispatch::Limits {
    dispatch::Limits { progress_heartbeat: std::time::Duration::ZERO, ..Default::default() }
}

#[tokio::test]
async fn a_webhook_on_a_quiet_collection_does_not_fall_past_retention() {
    // The same failure by a different route. Progress only ever advanced when
    // something was delivered, so a subscription on a collection nobody writes
    // to sat at its seed position while the rest of the database moved on, and
    // the retention horizon eventually overtook it. Deciding an entry is not
    // yours is work, and the position has to move over it.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, hits) = receiver(200).await;

    let watched = state.engine.create_collection("shop", "orders").unwrap();
    let busy = state.engine.create_collection("shop", "clicks").unwrap();
    for i in 0..50i64 {
        state.engine.insert(&busy, doc! { "_id": i }).unwrap();
    }

    let id = register(&state, &format!("http://{addr}/hook"), vec![]);
    assert!(dispatch::union_progress(&state, &id).is_empty(), "nothing delivered yet");

    // Everything written so far is what the resume point has to get past.
    let before = state.engine.version_vector().unwrap();

    let mut backoff = dispatch::Backoff::default();
    let outcome = pass_under(&state, &mut backoff, beating()).await;
    assert_eq!(outcome.delivered, 0, "none of that was its collection");
    assert_eq!(outcome.invalidated, 0, "{outcome:?}");

    // The property the retention horizon depends on: the position moved over
    // the entries that were not this subscription's, so it is no longer sitting
    // where it was when the database started moving without it.
    let progress = dispatch::union_progress(&state, &id);
    assert!(
        progress.covers(&before),
        "a pass that delivered nothing must still write its position forward"
    );

    state.engine.insert(&watched, doc! { "_id": 1 }).unwrap();
    assert_eq!(pass(&state).await.delivered, 1, "and it still delivers when its turn comes");
    assert_eq!(hits.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn the_position_is_written_forward_on_a_heartbeat_not_every_pass() {
    // The other half of the same rule. Recording progress is itself a write, so
    // it appends the entry the next pass reads; doing it every tick would have
    // an idle node writing to the oplog — and replicating it — every two
    // seconds forever.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, _) = receiver(200).await;
    let id = register(&state, &format!("http://{addr}/hook"), vec![]);

    // Deliver once, so the subscription has a position and it is a fresh one.
    let watched = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&watched, doc! { "_id": 1 }).unwrap();
    assert_eq!(pass(&state).await.delivered, 1);
    let settled = dispatch::union_progress(&state, &id);

    // Now the database moves without it.
    let busy = state.engine.create_collection("shop", "clicks").unwrap();
    state.engine.insert(&busy, doc! { "_id": 1 }).unwrap();

    // Inside the heartbeat interval, an idle pass leaves the position alone.
    assert_eq!(pass(&state).await.delivered, 0);
    assert_eq!(
        dispatch::union_progress(&state, &id),
        settled,
        "a pass inside the heartbeat interval must not write"
    );

    // Once it has elapsed, the position moves forward.
    let mut backoff = dispatch::Backoff::default();
    pass_under(&state, &mut backoff, beating()).await;
    assert_ne!(
        dispatch::union_progress(&state, &id),
        settled,
        "the heartbeat must write the position forward"
    );
}

#[tokio::test]
async fn a_bulk_load_is_batched_and_every_event_arrives_exactly_once() {
    // Batching is what keeps a bulk load from becoming one request per
    // document. The property that matters is that batching loses nothing and
    // duplicates nothing while doing it.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, seen, hits) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    for i in 0..200i64 {
        state.engine.insert(&coll, doc! { "_id": i }).unwrap();
    }

    let mut delivered = 0;
    for _ in 0..10 {
        let outcome = pass(&state).await;
        delivered += outcome.delivered;
        if outcome.delivered == 0 {
            break;
        }
    }
    assert_eq!(delivered, 200, "every write must be delivered");

    // 200 events at 64 to a batch is four requests, not two hundred.
    assert_eq!(hits.load(Ordering::Relaxed), 4, "batched, not one request per document");

    // Every event exactly once, and in oplog order.
    let ids: Vec<String> = seen
        .lock()
        .iter()
        .flat_map(|d| {
            let body: serde_json::Value = serde_json::from_str(&d.body).unwrap();
            body["events"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["eventId"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(ids.len(), 200);
    let unique: std::collections::BTreeSet<&String> = ids.iter().collect();
    assert_eq!(unique.len(), 200, "no event may be delivered twice");
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(sorted, ids, "events must arrive in oplog order");
}

#[tokio::test]
async fn removing_a_subscription_stops_delivery_and_clears_its_progress() {
    // A removed subscription must stop dialling, and must not leave a progress
    // record behind for every node that ever delivered it — nothing would ever
    // read or collect those again.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, hits) = receiver(200).await;

    let auth = kimmy_api::state::Auth(kimmy_auth::Principal::new(
        "root",
        vec![kimmy_auth::Grant::superuser()],
    ));
    let coll = state.engine.create_collection("shop", "orders").unwrap();
    let registered = kimmy_api::webhooks::register(
        &state,
        &auth,
        "shop",
        "orders",
        &kimmy_api::webhooks::RegisterRequest {
            url: format!("http://{addr}/hook"),
            operations: None,
        },
        &EgressPolicy::new(vec!["127.0.0.1".into()]),
    )
    .expect("registration");
    let id = registered["id"].as_str().unwrap().to_string();

    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    assert_eq!(pass(&state).await.delivered, 1);
    assert!(!dispatch::union_progress(&state, &id).is_empty(), "it delivered, so it has progress");

    kimmy_api::webhooks::remove(&state, &auth, "shop", "orders", &id).expect("removal");

    // Writes after the removal reach nobody.
    let before = hits.load(Ordering::Relaxed);
    for i in 2..10i64 {
        state.engine.insert(&coll, doc! { "_id": i }).unwrap();
    }
    let outcome = pass(&state).await;
    assert_eq!(outcome.delivered, 0, "a removed subscription must deliver nothing: {outcome:?}");
    assert_eq!(hits.load(Ordering::Relaxed), before, "and must not dial at all");

    assert!(
        dispatch::union_progress(&state, &id).is_empty(),
        "its progress records must go with it"
    );
}

#[tokio::test]
async fn a_slow_endpoint_does_not_hold_up_another_subscription() {
    // The reason delivery is concurrent. Serially, two endpoints that each take
    // a second cost two seconds and the second subscription waits on the first
    // for no reason of its own — the same cross-subscription interference the
    // per-subscription backoff exists to prevent, one layer up.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let slow = std::time::Duration::from_millis(1_000);
    let (first, _a, a_hits) = receiver_taking(200, slow).await;
    let (second, _b, b_hits) = receiver_taking(200, slow).await;
    // Chosen ids, because a pass plans in `_id` order.
    register_as(&state, "wh_test_aaa", &format!("http://{first}/hook"), vec![]);
    register_as(&state, "wh_test_zzz", &format!("http://{second}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();

    let started = std::time::Instant::now();
    let outcome = pass(&state).await;
    let elapsed = started.elapsed();

    assert_eq!(outcome.delivered, 2, "both must be delivered: {outcome:?}");
    assert_eq!(a_hits.load(Ordering::Relaxed), 1);
    assert_eq!(b_hits.load(Ordering::Relaxed), 1);
    assert!(
        elapsed < slow * 2,
        "two one-second endpoints took {elapsed:?}; they were delivered one after the other"
    );
}

#[tokio::test]
async fn the_concurrency_bound_is_real() {
    // The other half: the point of a bound is that it binds. With one permit
    // the same two deliveries must serialise, or the number in the config is
    // decoration.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let slow = std::time::Duration::from_millis(600);
    let (first, _a, _) = receiver_taking(200, slow).await;
    let (second, _b, _) = receiver_taking(200, slow).await;
    register_as(&state, "wh_test_aaa", &format!("http://{first}/hook"), vec![]);
    register_as(&state, "wh_test_zzz", &format!("http://{second}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();

    let limits = dispatch::Limits { max_concurrent_deliveries: 1, ..Default::default() };
    let mut backoff = dispatch::Backoff::default();
    let started = std::time::Instant::now();
    let outcome = pass_under(&state, &mut backoff, limits).await;
    let elapsed = started.elapsed();

    assert_eq!(outcome.delivered, 2, "{outcome:?}");
    assert!(elapsed >= slow * 2, "one permit must serialise them; took {elapsed:?}");
}

#[tokio::test]
async fn an_oversized_document_is_delivered_without_it_rather_than_dropped() {
    // Dropping the event would leave a gap the receiver could never detect,
    // which is the failure invalidation exists to avoid. The change still goes;
    // only the copy of the document does not.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, seen, _) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1, "blob": "x".repeat(4_000) }).unwrap();

    let limits = dispatch::Limits { max_payload_bytes: 1_000, ..Default::default() };
    let mut backoff = dispatch::Backoff::default();
    let outcome = pass_under(&state, &mut backoff, limits).await;
    assert_eq!(outcome.delivered, 1, "the event must still be delivered: {outcome:?}");

    let body = seen.lock()[0].body.clone();
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
    let event = &parsed["events"][0];
    assert_eq!(event["fullDocumentOmitted"], serde_json::json!(true), "{body}");
    assert!(event["fullDocument"].is_null(), "the document must not be there: {body}");
    assert_eq!(event["documentKey"]["_id"], serde_json::json!(1), "the key must be: {body}");
    assert!(!body.contains("xxxxxxxxxx"), "no part of the document may leak through: {body}");

    // And it advances, rather than wedging on the event forever.
    assert_eq!(pass_under(&state, &mut backoff, limits).await.delivered, 0);
}

#[tokio::test]
async fn a_batch_is_trimmed_to_the_payload_cap() {
    // Documents that each fit but together do not: the batch is short and the
    // rest goes next pass, rather than one oversized POST.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, seen, hits) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    for i in 0..8i64 {
        state.engine.insert(&coll, doc! { "_id": i, "blob": "y".repeat(300) }).unwrap();
    }

    let limits = dispatch::Limits { max_payload_bytes: 1_200, ..Default::default() };
    let mut backoff = dispatch::Backoff::default();
    let mut delivered = 0;
    for _ in 0..20 {
        let outcome = pass_under(&state, &mut backoff, limits).await;
        delivered += outcome.delivered;
        if outcome.delivered == 0 {
            break;
        }
    }

    assert_eq!(delivered, 8, "every event must arrive");
    assert!(hits.load(Ordering::Relaxed) > 1, "the batch must have been split");
    for delivery in seen.lock().iter() {
        assert!(
            delivery.body.len() <= 1_200,
            "a body of {} bytes exceeds the cap",
            delivery.body.len()
        );
    }
}

#[tokio::test]
async fn a_caught_up_subscription_reports_no_backlog() {
    // The gauge an operator alerts on. Derived from the resume point it would
    // grow with the clock on an idle, working webhook — an alert firing for
    // something that is not wrong. It is the age of the oldest *undelivered*
    // event, so a subscription with nothing to deliver reports zero.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, _) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    assert_eq!(pass(&state).await.delivered, 1);

    // Time passes; the resume point ages, but nothing is undelivered.
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    pass(&state).await;

    let rendered = state.metrics.render();
    assert!(
        rendered.contains("kimmy_webhook_backlog_seconds 0"),
        "a caught-up subscription must report no backlog:\n{rendered}"
    );
    assert!(rendered.contains("kimmy_webhook_subscriptions{state=\"active\"} 1"), "{rendered}");
}

#[tokio::test]
async fn an_undelivered_event_shows_up_as_backlog() {
    // The other direction: the gauge has to actually move, or it is decoration.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, _) = receiver(500).await; // never accepts
    register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    pass(&state).await;

    let rendered = state.metrics.render();
    assert!(
        !rendered.contains("kimmy_webhook_backlog_seconds 0"),
        "an event written a second ago and not delivered is backlog:\n{rendered}"
    );
}

#[tokio::test]
async fn backlog_is_the_age_of_the_event_not_of_the_resume_point() {
    // These differ, and only one of them is the truth. A subscription that
    // delivered an hour ago and has just been handed a fresh event is not an
    // hour behind — it is current. Measuring the resume point instead would
    // report the length of the quiet period as if it were lag, and page
    // somebody for a webhook that is working perfectly.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, _) = receiver(200).await;
    register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    assert_eq!(pass(&state).await.delivered, 1);

    // The quiet period. The resume point ages by this much; the event that
    // arrives after it has not.
    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;
    state.engine.insert(&coll, doc! { "_id": 2 }).unwrap();

    assert_eq!(pass(&state).await.delivered, 1);
    let rendered = state.metrics.render();
    assert!(
        rendered.contains("kimmy_webhook_backlog_seconds 0"),
        "a just-written event is not two seconds of backlog:\n{rendered}"
    );
}

#[tokio::test]
async fn progress_from_a_peer_that_is_ahead_does_not_invalidate_this_node() {
    // The failover case, which single-node tests otherwise never reach: the
    // union of progress can cover everything this node holds, because another
    // node delivered further than this one has seen. `behind` then answers
    // `None` — caught up — and treating that as "resume from zero" would put
    // the subscription behind any retention horizon and invalidate a
    // subscription that has missed nothing.
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let (addr, _seen, _) = receiver(200).await;
    let id = register(&state, &format!("http://{addr}/hook"), vec![]);

    let coll = state.engine.create_collection("shop", "orders").unwrap();
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();

    // A record written by another node, ahead of anything local.
    let peer = kimmy_core::NodeId::generate();
    let ahead = kimmy_core::Hlc::new(kimmy_storage::physical_now_ms() + 365 * 86_400_000, 0);
    let progress_meta = state
        .engine
        .create_system_collection("__kimmy", "__webhook_progress")
        .unwrap_or_else(|_| state.engine.get_collection("__kimmy", "__webhook_progress").unwrap());
    let mut delivered = bson::Document::new();
    for node in [peer, state.engine.node_id()] {
        delivered.insert(
            node.to_string(),
            bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: ahead.to_bytes().to_vec(),
            },
        );
    }
    let record = format!("{id}:{peer}");
    state.engine.insert(&progress_meta, doc! { "_id": record, "delivered": delivered }).unwrap();

    state
        .engine
        .collect_garbage_at(
            kimmy_storage::physical_now_ms(),
            kimmy_storage::RetentionPolicy::new(0, 24 * 60 * 60),
        )
        .unwrap();

    let outcome = pass(&state).await;
    assert_eq!(
        outcome.invalidated, 0,
        "a subscription a peer has already carried forward has missed nothing: {outcome:?}"
    );
}
