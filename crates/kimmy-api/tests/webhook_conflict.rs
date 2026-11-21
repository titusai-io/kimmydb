//! The `conflictResolved` webhook event (Q7 step 4, S4b): a subscription opts in by listing the operation, on a collection that can
//! produce it; what it is delivered is the loss records the owner holds, under the `eventId` of the lost write.

use std::sync::Arc;

use kimmy_api::egress::{EgressPolicy, WEBHOOKS};
use kimmy_auth::TokenIssuer;
use kimmy_core::MergePolicy;
use kimmy_storage::Engine;

const SECRET: &str = "an-adequately-long-test-secret-for-hs256";

/// An engine plus API state, with an egress policy that permits the loopback receiver.
fn state_for(dir: &tempfile::TempDir) -> kimmy_api::SharedState {
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let tokens = TokenIssuer::new(SECRET, 3600).unwrap();
    kimmy_api::state_with_egress(
        engine,
        tokens,
        false,
        kimmy_api::RateLimits::disabled(),
        EgressPolicy::new(WEBHOOKS, vec!["127.0.0.1".into()]),
    )
    .unwrap()
}

fn root() -> kimmy_api::state::Auth {
    kimmy_api::state::Auth(kimmy_auth::Principal::new("root", vec![kimmy_auth::Grant::superuser()]))
}

async fn register(
    state: &kimmy_api::SharedState,
    operations: Option<Vec<&str>>,
) -> Result<serde_json::Value, kimmy_api::error::ApiError> {
    let request = kimmy_api::webhooks::RegisterRequest {
        url: "http://127.0.0.1:9/hook".into(),
        operations: operations.map(|names| names.into_iter().map(String::from).collect()),
    };
    kimmy_api::webhooks::register(
        state,
        &root(),
        "shop",
        "orders",
        &request,
        &EgressPolicy::new(WEBHOOKS, vec!["127.0.0.1".into()]),
    )
    .await
}

#[tokio::test]
async fn a_subscription_may_list_conflict_resolved() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    state.engine.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();

    let registered = register(&state, Some(vec!["insert", "conflictResolved"])).await;
    let id = registered.expect("a field collection can be subscribed to conflictResolved")["id"]
        .as_str()
        .unwrap()
        .to_string();

    let listing = kimmy_api::webhooks::list(&state, &root(), "shop", "orders").unwrap();
    let listed = listing["webhooks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["id"] == id.as_str())
        .expect("the subscription is listed");
    assert_eq!(
        listed["operations"],
        serde_json::json!(["insert", "conflictResolved"]),
        "the listing names the operation as it was asked for"
    );
}

#[tokio::test]
async fn a_document_collection_cannot_be_subscribed_to_conflict_resolved() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    state
        .engine
        .create_collection_with_policy("shop", "orders", MergePolicy::Document, None)
        .unwrap();

    let err = register(&state, Some(vec!["conflictResolved"]))
        .await
        .expect_err("a document collection never produces a loss record");

    assert_eq!(err.status, axum::http::StatusCode::BAD_REQUEST);
    assert!(
        err.message.contains("mergePolicy"),
        "the refusal names the policy that cannot produce the event: {}",
        err.message
    );
    // And the other operations stay available there.
    register(&state, Some(vec!["insert"])).await.expect("insert on a document collection");
}

// ---- delivery (S4b.1) ------------------------------------------------------------------------------------------------------------------

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use bson::doc;
use kimmy_api::dispatch;
use kimmy_core::merge_state::{
    LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value as MValue, WriterId,
};
use kimmy_core::{DocId, Hlc, OpKind, OplogEntry, Stamp};
use kimmy_storage::{CollectionMeta, WalkScope};
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// What a delivery looked like from the receiving end.
#[derive(Clone, Debug, Default)]
struct Received {
    body: String,
    event_id: String,
}

/// A receiver that records what arrives and answers 200.
async fn receiver() -> (std::net::SocketAddr, Arc<Mutex<Vec<Received>>>) {
    let seen: Arc<Mutex<Vec<Received>>> = Arc::default();
    let hits = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let recorded = Arc::clone(&seen);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let recorded = Arc::clone(&recorded);
            let hits = Arc::clone(&hits);
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
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let header = |name: &str| {
                            head.lines()
                                .find_map(|l| {
                                    let (k, v) = l.split_once(':')?;
                                    k.trim()
                                        .eq_ignore_ascii_case(name)
                                        .then(|| v.trim().to_string())
                                })
                                .unwrap_or_default()
                        };
                        let len: usize = header("content-length").parse().unwrap_or(0);
                        if body.len() >= len {
                            recorded.lock().push(Received {
                                body: body.to_string(),
                                event_id: header("x-kimmy-event-id"),
                            });
                            hits.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                let _ = stream
                    .write_all(b"HTTP/1.1 200 X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            });
        }
    });
    (addr, seen)
}

/// A subscription stored directly, as the registry holds one, so that a test of delivery does not depend on registration. No progress
/// is seeded: it is delivered whatever the oplog holds.
fn subscribe(state: &kimmy_api::SharedState, url: &str, operations: &[&str]) -> String {
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
                "operations": operations.iter().map(|o| o.to_string()).collect::<Vec<_>>(),
                "secret": "test-webhook-secret",
                "createdBy": "root",
                "createdMs": 1i64,
            },
        )
        .unwrap();
    id
}

/// This node's id: ownership hashes node ids, so fixing it fixes "am I the owner".
fn me() -> kimmy_core::NodeId {
    kimmy_core::NodeId::from_bytes([0xAA; 16])
}

async fn pass_under(
    state: &kimmy_api::SharedState,
    backoff: &mut dispatch::Backoff,
    limits: dispatch::Limits,
) -> dispatch::DispatchOutcome {
    let client =
        reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let policy = EgressPolicy::new(WEBHOOKS, vec!["127.0.0.1".into()]);
    dispatch::dispatch_once(state, &client, &policy, me(), &BTreeSet::new(), backoff, limits).await
}

async fn pass(state: &kimmy_api::SharedState) -> dispatch::DispatchOutcome {
    pass_under(state, &mut dispatch::Backoff::default(), dispatch::Limits::default()).await
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(20));
}

/// The events of every delivery, in the order they arrived.
fn events(seen: &Arc<Mutex<Vec<Received>>>) -> Vec<Value> {
    seen.lock()
        .iter()
        .flat_map(|d| {
            serde_json::from_str::<Value>(&d.body).unwrap()["events"].as_array().unwrap().clone()
        })
        .collect()
}

fn conflict_events(seen: &Arc<Mutex<Vec<Received>>>) -> Vec<Value> {
    events(seen).into_iter().filter(|e| e["operationType"] == "conflictResolved").collect()
}

/// The webhook state's engine is the owner; `other` is a second member. Both write field `x` of document 1 at once and take each other's
/// entries: each derives the record of the write that lost and mints an entry of its own for it, and each holds the other's entry too.
fn a_clash(owner: &Engine) -> (Engine, tempfile::TempDir) {
    let ca = owner.get_collection("shop", "orders").unwrap();
    owner.insert(&ca, doc! { "_id": 1, "x": 0 }).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let other = Engine::open(&dir.path().join("other.redb")).unwrap();
    other.apply_batch(&everything(owner)).unwrap();
    let cb = other.get_collection("shop", "orders").unwrap();
    later();
    owner.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "x": 1 }, false).unwrap();
    other.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "x": 2 }, false).unwrap();
    later();
    owner.apply_batch(&everything(&other)).unwrap();
    other.apply_batch(&everything(owner)).unwrap();
    owner.apply_batch(&everything(&other)).unwrap();
    (other, dir)
}

fn loss_entries(e: &Engine) -> Vec<OplogEntry> {
    everything(e).into_iter().filter(|x| x.kind == OpKind::LossRecord).collect()
}

async fn an_owner_with_a_clash() -> (
    kimmy_api::SharedState,
    tempfile::TempDir,
    Engine,
    tempfile::TempDir,
    std::net::SocketAddr,
    Arc<Mutex<Vec<Received>>>,
) {
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    state.engine.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let (addr, seen) = receiver().await;
    let (other, other_dir) = a_clash(&state.engine);
    (state, dir, other, other_dir, addr, seen)
}

#[tokio::test]
async fn a_subscription_that_lists_conflict_resolved_is_delivered_the_clash() {
    let (state, _d, _other, _od, addr, seen) = an_owner_with_a_clash().await;
    subscribe(&state, &format!("http://{addr}/hook"), &["conflictResolved"]);

    let outcome = pass(&state).await;

    let told = events(&seen);
    assert!(!told.is_empty(), "the clash is delivered: {outcome:?}");
    assert!(
        told.iter().all(|e| e["operationType"] == "conflictResolved"),
        "only what was asked for: {told:#?}"
    );
    let event = &told[0];
    assert!(event["eventId"].as_str().unwrap().starts_with("cr1-"), "{event}");
    assert_eq!(event["database"], "shop", "{event}");
    assert_eq!(event["collection"], "orders", "{event}");
    assert_eq!(event["documentKey"]["_id"], 1, "{event}");
    assert_eq!(event["path"], "x", "{event}");
    assert_eq!(event["kind"], "clash", "{event}");
    assert!(event["loser"]["stamp"].is_string(), "{event}");
    let (loser, winner) =
        (event["loser"]["value"].as_i64().unwrap(), event["winner"]["value"].as_i64().unwrap());
    assert_eq!([loser, winner].iter().copied().min(), Some(1), "one write each: {event}");
    assert_eq!([loser, winner].iter().copied().max(), Some(2), "one write each: {event}");
    assert!(event.get("resumeToken").is_none(), "a webhook event carries no resume token: {event}");
}

#[tokio::test]
async fn the_header_and_the_body_carry_the_same_conflict_event_id() {
    let (state, _d, _other, _od, addr, seen) = an_owner_with_a_clash().await;
    subscribe(&state, &format!("http://{addr}/hook"), &["conflictResolved"]);

    pass(&state).await;

    let deliveries = seen.lock().clone();
    assert!(!deliveries.is_empty(), "the batch has the conflict event");
    let first = &serde_json::from_str::<Value>(&deliveries[0].body).unwrap()["events"][0];
    assert_eq!(
        deliveries[0].event_id,
        first["eventId"].as_str().unwrap(),
        "X-Kimmy-Event-Id is the first event's eventId"
    );
    assert!(
        deliveries[0].event_id.starts_with("cr1-"),
        "and not the stamp of the entry that carried it: {}",
        deliveries[0].event_id
    );
}

#[tokio::test]
async fn one_record_carried_by_two_members_entries_has_one_event_id() {
    let (state, _d, other, _od, addr, seen) = an_owner_with_a_clash().await;
    let held = loss_entries(&state.engine);
    let origins: BTreeSet<_> = held.iter().map(|e| e.stamp.node).collect();
    assert_eq!(
        origins.len(),
        2,
        "premise: the owner holds its own entry and the other member's: {held:?}"
    );
    assert_eq!(loss_entries(&other).len(), held.len(), "premise: one record, one entry per member");
    subscribe(&state, &format!("http://{addr}/hook"), &["conflictResolved"]);

    pass(&state).await;

    let told = conflict_events(&seen);
    assert_eq!(
        told.len(),
        held.len(),
        "every entry the owner holds is told, with no collapse: {told:#?}"
    );
    let ids: BTreeSet<_> =
        told.iter().map(|e| e["eventId"].as_str().unwrap().to_string()).collect();
    assert_eq!(ids.len(), 1, "the two entries of one record share one eventId: {ids:?}");
    let losers: BTreeSet<_> = told.iter().map(|e| e["loser"].to_string()).collect();
    assert_eq!(losers.len(), 1, "and one loser, whatever the entry: {losers:?}");
}

/// A record the owner holds only as another member's relayed entry (its document has no row here, so the record is parked and the owner
/// minted no entry of its own) is still delivered: the owner delivers what it holds, by origin (V6M2).
#[tokio::test]
async fn a_relayed_entry_the_owner_never_minted_is_delivered() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let coll = state
        .engine
        .create_collection_with_policy("shop", "orders", MergePolicy::Field, None)
        .unwrap();
    let (addr, seen) = receiver().await;
    let other_dir = tempfile::tempdir().unwrap();
    let other = Engine::open(&other_dir.path().join("other.redb")).unwrap();
    let writer = WriterId { node: other.node_id(), epoch: other.epoch() };
    let loser = MStamp { hlc: Hlc::new(kimmy_storage::physical_now_ms(), 0), writer };
    let key = LossKey::field(LossKind::Clash, Path::flat("x").unwrap(), loser);
    let entry = OplogEntry {
        stamp: Stamp::new(
            Hlc::new(kimmy_storage::physical_now_ms() + 1, 0),
            other.node_id(),
            other.epoch(),
        ),
        kind: OpKind::LossRecord,
        collection: coll.id,
        doc_id: Some(DocId::Int64(9)),
        body: Some(MergeState::encode_loss_record(&key, &LossValue::Value(MValue::Int64(1)))),
        meta: None,
    };
    state.engine.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert_eq!(
        loss_entries(&state.engine).len(),
        1,
        "premise: the relayed entry is kept, and the owner minted none"
    );
    subscribe(&state, &format!("http://{addr}/hook"), &["conflictResolved"]);

    pass(&state).await;

    let told = conflict_events(&seen);
    assert_eq!(told.len(), 1, "the relayed entry is delivered: {told:#?}");
    assert_eq!(told[0]["kind"], "clash", "{}", told[0]);
    assert_eq!(told[0]["documentKey"]["_id"], 9, "{}", told[0]);
    assert!(told[0]["winner"].is_null(), "the owner has no cell to name: {}", told[0]);
}

#[tokio::test]
async fn the_winner_is_the_cell_at_delivery() {
    let (state, _d, _other, _od, addr, seen) = an_owner_with_a_clash().await;
    let coll: CollectionMeta = state.engine.get_collection("shop", "orders").unwrap();
    later();
    // The cell is written again after the record's entries: the winner is what stands when the event is delivered.
    state.engine.replace(&coll, &DocId::Int64(1), doc! { "_id": 1, "x": 3 }, false).unwrap();
    subscribe(&state, &format!("http://{addr}/hook"), &["conflictResolved"]);

    pass(&state).await;

    let told = conflict_events(&seen);
    assert!(!told.is_empty(), "the clash is delivered");
    assert!(
        told.iter().all(|e| e["winner"]["value"] == json!(3)),
        "the winner is the cell now at the path: {told:#?}"
    );
}

/// A field removed after the record's entries is a cell too: the winner is the removal, with no value.
#[tokio::test]
async fn the_winner_of_a_removed_field_is_the_removal_with_no_value() {
    let (state, _d, _other, _od, addr, seen) = an_owner_with_a_clash().await;
    let coll: CollectionMeta = state.engine.get_collection("shop", "orders").unwrap();
    later();
    state.engine.replace(&coll, &DocId::Int64(1), doc! { "_id": 1 }, false).unwrap();
    subscribe(&state, &format!("http://{addr}/hook"), &["conflictResolved"]);

    pass(&state).await;

    let told = conflict_events(&seen);
    assert!(!told.is_empty(), "the clash is delivered");
    for event in &told {
        assert!(event["winner"]["stamp"].is_string(), "the removal is the winner: {event}");
        assert!(event["winner"]["value"].is_null(), "and it has no value: {event}");
        assert_ne!(event["winner"]["stamp"], event["loser"]["stamp"], "not the loser: {event}");
    }
}

/// A deleted document has no cell: `winner` is `null`.
#[tokio::test]
async fn the_winner_is_null_when_the_document_is_gone_at_delivery() {
    let (state, _d, _other, _od, addr, seen) = an_owner_with_a_clash().await;
    let coll: CollectionMeta = state.engine.get_collection("shop", "orders").unwrap();
    later();
    state.engine.delete(&coll, &DocId::Int64(1)).unwrap();
    subscribe(&state, &format!("http://{addr}/hook"), &["conflictResolved"]);

    pass(&state).await;

    let told = conflict_events(&seen);
    assert!(!told.is_empty(), "the clash is delivered");
    assert!(told.iter().all(|e| e["winner"].is_null()), "no document, no winner: {told:#?}");
}

/// A progress record as another node that delivered that far would have written it, over origins with their epochs (the entries a
/// member mints carry its real epoch, which the document-event helpers of tests/webhooks.rs, built on `Epoch::ZERO`, do not).
fn progress_of_a_previous_owner(
    state: &kimmy_api::SharedState,
    subscription: &str,
    previous_owner: kimmy_core::NodeId,
    covered: &[(kimmy_core::Origin, Hlc)],
) {
    let meta = state
        .engine
        .create_system_collection("__kimmy", "__webhook_progress")
        .unwrap_or_else(|_| state.engine.get_collection("__kimmy", "__webhook_progress").unwrap());
    let mut delivered = bson::Document::new();
    for (origin, through) in covered {
        delivered.insert(
            origin.to_string(),
            bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: through.to_bytes().to_vec(),
            },
        );
    }
    state
        .engine
        .insert(
            &meta,
            doc! { "_id": format!("{subscription}:{previous_owner}"), "delivered": delivered },
        )
        .unwrap();
}

/// The takeover clause of L37: a node that takes a subscription over delivers from the progress the previous owner replicated, so it sends
/// the loss entry that progress does not cover and not the one it does. (The previous owner delivered the other member's entry of the
/// record and not this node's own, which this node has been holding.)
#[tokio::test]
async fn a_node_that_took_the_subscription_over_resumes_from_replicated_progress_for_conflict_events()
 {
    let (state, _d, other, _od, addr, seen) = an_owner_with_a_clash().await;
    let held = loss_entries(&state.engine);
    let own: Vec<_> = held.iter().filter(|e| e.stamp.node == state.engine.node_id()).collect();
    let relayed: Vec<_> = held.iter().filter(|e| e.stamp.node == other.node_id()).collect();
    assert_eq!((own.len(), relayed.len()), (1, 1), "premise: one entry of each member's: {held:?}");
    let id = subscribe(&state, &format!("http://{addr}/hook"), &["conflictResolved"]);
    let previous_owner = kimmy_core::NodeId::from_bytes([0x11; 16]);
    progress_of_a_previous_owner(
        &state,
        &id,
        previous_owner,
        &[(relayed[0].stamp.origin(), relayed[0].stamp.hlc)],
    );

    pass(&state).await;

    let told = conflict_events(&seen);
    assert_eq!(
        told.len(),
        1,
        "the entry the previous owner's progress covers is not sent again: {told:#?}"
    );
    assert_eq!(
        told[0]["clusterTime"],
        json!(own[0].stamp.hlc.to_string()),
        "and the one it does not cover is: {}",
        told[0]
    );
    assert_eq!(pass(&state).await.delivered, 0, "nothing is left for the next pass");
}

/// Guard (green on the parent): a subscription that does not list the operation is sent no conflict event, however it is otherwise
/// registered, and its document events still arrive.
#[tokio::test]
async fn a_subscription_that_does_not_list_it_is_sent_none() {
    let (state, _d, _other, _od, addr, seen) = an_owner_with_a_clash().await;
    let everything_default = subscribe(&state, &format!("http://{addr}/hook"), &[]);
    let some = subscribe(&state, &format!("http://{addr}/hook"), &["insert", "replace"]);

    pass(&state).await;

    let told = events(&seen);
    assert!(!told.is_empty(), "premise: the document events are delivered");
    assert!(
        told.iter().all(|e| e["operationType"] != "conflictResolved"),
        "an absent or unlisted operation is never the new one: {told:#?}"
    );

    // And not held back, the one thing the opt-in could break for a receiver registered before it: the loss entries sit in the oplog beside
    // the document events, and the progress of a subscription that does not want them still steps over them, so they are never its backlog.
    let held = loss_entries(&state.engine);
    assert!(!held.is_empty(), "premise: the oplog holds loss entries");
    for id in [&everything_default, &some] {
        let progress = dispatch::union_progress(&state, id).expect("the progress is readable");
        for entry in &held {
            assert!(
                progress.get(entry.stamp.origin()) >= entry.stamp.hlc,
                "{id}: its progress covers the loss entry it does not want: {entry:?}"
            );
        }
    }
    assert_eq!(pass(&state).await.delivered, 0, "a second pass has nothing left to send");
}

/// Guard (green on the parent): a batch of document events only gets the stamp id of its first event in `X-Kimmy-Event-Id`.
#[tokio::test]
async fn a_batch_of_document_events_has_the_stamp_id_of_its_first_event_in_the_header() {
    let dir = tempfile::tempdir().unwrap();
    let state = state_for(&dir);
    let coll = state
        .engine
        .create_collection_with_policy("shop", "orders", MergePolicy::Field, None)
        .unwrap();
    let (addr, seen) = receiver().await;
    state.engine.insert(&coll, doc! { "_id": 1 }).unwrap();
    state.engine.insert(&coll, doc! { "_id": 2 }).unwrap();
    subscribe(&state, &format!("http://{addr}/hook"), &["insert"]);

    pass(&state).await;

    let deliveries = seen.lock().clone();
    assert_eq!(deliveries.len(), 1, "one batch");
    let first = serde_json::from_str::<Value>(&deliveries[0].body).unwrap()["events"][0].clone();
    assert_eq!(first["operationType"], "insert");
    let id = first["eventId"].as_str().unwrap();
    assert!(!id.starts_with("cr1-"), "a document event keeps the stamp id: {id}");
    assert_eq!(deliveries[0].event_id, id, "the header is the first event's id");
    let parts: Vec<&str> = id.splitn(3, '-').collect();
    assert_eq!(parts.len(), 3, "{{hlc}}-{{node}}-{{epoch}}: {id}");
}
