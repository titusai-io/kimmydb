//! Replication over real TCP sockets.
//!
//! The convergence rules are already tested between engines in one process
//! (`kimmy-storage/src/sync.rs`). These tests exist for what that could not
//! reach: that the wire carries the types faithfully, that the handshake
//! actually gates access, and that a listener survives a peer misbehaving.

use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_cluster::protocol::{Message, ProtocolError, read_frame, write_frame};
use kimmy_cluster::transport::{serve, sync_once};
use kimmy_core::DocId;
use kimmy_storage::Engine;
use tokio::net::{TcpListener, TcpStream};

const SECRET: &str = "a-shared-cluster-secret";

struct Node {
    engine: Arc<Engine>,
    addr: std::net::SocketAddr,
    _dir: tempfile::TempDir,
}

/// Start a node serving replication on an ephemeral port.
async fn node() -> Node {
    node_with_secret(SECRET).await
}

async fn node_with_secret(secret: &str) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());

    // Port 0: the OS picks, so parallel tests never collide.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(Arc::clone(&engine), listener, secret.to_string()));

    Node { engine, addr, _dir: dir }
}

/// Pull into `into` from `from`, both directions making a full round.
async fn sync(a: &Node, b: &Node) {
    sync_once(&a.engine, b.addr, SECRET).await.expect("a should pull from b");
    sync_once(&b.engine, a.addr, SECRET).await.expect("b should pull from a");
}

#[tokio::test]
async fn two_nodes_converge_over_the_network() {
    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "from-a", "v": 1 }).unwrap();

    let cb = b.engine.create_collection("shop", "orders").unwrap();
    b.engine.insert(&cb, doc! { "_id": "from-b", "v": 2 }).unwrap();

    sync(&a, &b).await;

    for (node, coll) in [(&a, &ca), (&b, &cb)] {
        assert!(node.engine.get(coll, &DocId::String("from-a".into())).unwrap().is_some());
        assert!(node.engine.get(coll, &DocId::String("from-b".into())).unwrap().is_some());
    }
}

#[tokio::test]
async fn a_collection_whose_id_is_above_i64_max_replicates() {
    // Every other test in this file uses "shop"."orders", whose derived id
    // happens to land in the low half of the u64 range. Roughly half of all
    // collection names do not — and BSON has no unsigned 64-bit type, so those
    // ids could not be encoded at all. The collection and every document in it
    // silently never replicated: the write succeeded locally, and the peer
    // logged one "malformed frame" warning per round.
    //
    // Found by running three containers, not by this suite, which passed
    // throughout because of the name it happened to pick.
    let id = kimmy_core::ids::CollectionId::derive("c", "t");
    assert!(
        id.0 > i64::MAX as u64,
        "this test is only meaningful while c.t derives a high id; it derives {}",
        id.0
    );

    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("c", "t").unwrap();
    a.engine.insert(&ca, doc! { "_id": "high-id", "v": 1 }).unwrap();

    sync(&a, &b).await;

    let cb = b.engine.get_collection("c", "t").expect("the collection must replicate");
    assert!(
        b.engine.get(&cb, &DocId::String("high-id".into())).unwrap().is_some(),
        "a document in a collection whose id exceeds i64::MAX must replicate like any other"
    );
}

#[tokio::test]
async fn a_collection_and_its_index_replicate_over_the_network() {
    // Schema changes carry BSON payloads, so this is the test that the wire
    // round-trips them rather than only documents.
    let a = node().await;
    let b = node().await;

    a.engine.create_collection("shop", "orders").unwrap();
    a.engine.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
    let ca = a.engine.get_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": 1, "email": "x@y" }).unwrap();

    sync_once(&b.engine, a.addr, SECRET).await.unwrap();

    let cb = b.engine.get_collection("shop", "orders").expect("the collection must replicate");
    let index = cb.indexes.iter().find(|i| i.name == "email_1").expect("the index must replicate");
    assert!(index.unique, "uniqueness must survive the wire");
    assert!(b.engine.get(&cb, &DocId::Int64(1)).unwrap().is_some());
}

#[tokio::test]
async fn a_node_joining_an_existing_cluster_catches_up() {
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    for i in 0..200i64 {
        a.engine.insert(&ca, doc! { "_id": i }).unwrap();
    }

    // b starts empty and knows nothing.
    let b = node().await;
    sync_once(&b.engine, a.addr, SECRET).await.unwrap();

    let cb = b.engine.get_collection("shop", "orders").unwrap();
    assert_eq!(b.engine.count(&cb).unwrap(), 200);
}

#[tokio::test]
async fn a_converged_round_transfers_nothing() {
    let a = node().await;
    let b = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": 1 }).unwrap();

    sync(&a, &b).await;
    let second = sync_once(&b.engine, a.addr, SECRET).await.unwrap();

    assert_eq!(second.total(), 0, "a converged pair must exchange nothing: {second:?}");
}

// ---------------------------------------------------------------------------
// The handshake
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_peer_with_the_wrong_secret_is_refused() {
    // Without this, anything that can reach the port joins the cluster and
    // merges its data in.
    let a = node().await;
    let intruder = node_with_secret("not-the-cluster-secret").await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "secret" }).unwrap();

    let err = sync_once(&intruder.engine, a.addr, "not-the-cluster-secret")
        .await
        .expect_err("a wrong secret must be refused");

    assert!(matches!(err, ProtocolError::Unauthenticated | ProtocolError::Fault(_)), "got {err:?}");
    assert!(
        intruder.engine.get_collection("shop", "orders").is_err(),
        "nothing may be transferred to an unauthenticated peer"
    );
}

#[tokio::test]
async fn a_peer_that_never_proves_itself_learns_nothing() {
    // The authentication has to gate *reads*, not just writes: an intruder that
    // simply asks for the oplog must not receive it.
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "secret" }).unwrap();

    let mut stream = TcpStream::connect(a.addr).await.unwrap();
    // Skip the handshake entirely and ask straight out.
    write_frame(&mut stream, &Message::AskVersions {}).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut stream)).await;
    match response {
        // Either a refusal or a dropped connection is fine; handing over a
        // version vector is not.
        Ok(Ok(Message::Fault(_))) | Ok(Err(_)) | Err(_) => {}
        Ok(Ok(other)) => panic!("an unauthenticated peer received {other:?}"),
    }
}

#[tokio::test]
async fn one_bad_connection_does_not_stop_the_listener() {
    // A peer that connects and says nonsense must not take replication down
    // for everyone else.
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": 1 }).unwrap();

    // Garbage: a valid length prefix followed by bytes that are not BSON.
    let mut rude = TcpStream::connect(a.addr).await.unwrap();
    use tokio::io::AsyncWriteExt;
    rude.write_all(&8u32.to_be_bytes()).await.unwrap();
    rude.write_all(b"notabson").await.unwrap();
    drop(rude);

    // A well-behaved peer still works.
    let b = node().await;
    sync_once(&b.engine, a.addr, SECRET).await.expect("the listener must still be serving");
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    assert_eq!(b.engine.count(&cb).unwrap(), 1);
}

#[tokio::test]
async fn a_peer_that_hangs_up_mid_handshake_is_survivable() {
    let a = node().await;
    let stream = TcpStream::connect(a.addr).await.unwrap();
    drop(stream);

    let b = node().await;
    b.engine.create_collection("shop", "orders").unwrap();
    sync_once(&b.engine, a.addr, SECRET).await.expect("the listener must still be serving");
}

#[tokio::test]
async fn connecting_to_a_dead_peer_is_an_error_not_a_hang() {
    // Discovery hands out addresses that may be stale; a node must not stall
    // on one that has gone away.
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();

    // Bind and immediately release, so the port is almost certainly unused.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = listener.local_addr().unwrap();
    drop(listener);

    let result = tokio::time::timeout(Duration::from_secs(10), sync_once(&engine, dead, SECRET))
        .await
        .expect("must not hang");
    assert!(result.is_err());
}

#[tokio::test]
async fn a_node_joining_a_cluster_past_its_retention_horizon_still_catches_up() {
    // The failure this exists for. Without snapshot fallback, a node added to a
    // cluster older than oplog_retention_secs receives nothing it can apply,
    // never advances its version vector, and retries forever.
    let a = node().await;
    a.engine.create_collection("shop", "orders").unwrap();
    a.engine.create_index("shop", "orders", vec![field("item")], true, None).unwrap();
    let ca = a.engine.get_collection("shop", "orders").unwrap();
    for i in 0..50i64 {
        a.engine.insert(&ca, doc! { "_id": i, "item": format!("item-{i}") }).unwrap();
    }

    // A has been running long enough that its history is gone.
    a.engine
        .collect_garbage_at(
            kimmy_storage::physical_now_ms() + 1_000_000_000,
            kimmy_storage::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();

    // B joins knowing nothing.
    let b = node().await;
    sync_once(&b.engine, a.addr, SECRET).await.unwrap();

    let cb = b.engine.get_collection("shop", "orders").expect("the collection must arrive");
    assert_eq!(b.engine.count(&cb).unwrap(), 50, "every document must arrive");
    assert!(
        cb.indexes.iter().any(|i| i.name == "item_1" && i.unique),
        "the index must arrive with its uniqueness"
    );

    // And it must stop asking for history that no longer exists.
    let second = sync_once(&b.engine, a.addr, SECRET).await.unwrap();
    assert_eq!(second.total(), 0, "a caught-up node must not keep resyncing: {second:?}");
}

#[tokio::test]
async fn a_snapshot_is_only_used_when_the_oplog_cannot_serve() {
    // Snapshots transfer everything, so they must be the fallback rather than
    // the ordinary path.
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    for i in 0..10i64 {
        a.engine.insert(&ca, doc! { "_id": i }).unwrap();
    }

    let b = node().await;
    let outcome = sync_once(&b.engine, a.addr, SECRET).await.unwrap();

    // An incremental round reports DDL separately; a snapshot reports only
    // applied documents, so a non-zero ddl count means the oplog served it.
    assert!(outcome.ddl > 0, "nothing was collected, so history should have served: {outcome:?}");
}

#[tokio::test]
async fn a_dead_peer_is_backed_off_rather_than_retried_every_round() {
    // Nothing breaks without this — anti-entropy is idempotent and a failed
    // round costs a refused connection — but every round pays for a node that
    // is not coming back, and the log fills with the same failure.
    use kimmy_cluster::PeerHealth;
    use std::collections::BTreeSet;
    use std::time::Instant;

    // A port nothing is listening on.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = listener.local_addr().unwrap();
    drop(listener);

    let node = node().await;
    let mut health = PeerHealth::new(3, Duration::from_secs(5));
    let peers: BTreeSet<_> = BTreeSet::from([dead]);

    // First round: contacted, and it really does fail.
    let now = Instant::now();
    assert_eq!(health.select(&peers, now), vec![dead]);
    assert!(sync_once(&node.engine, dead, SECRET).await.is_err());
    health.failed(dead, now);

    // A single failure is forgiven promptly — a blip should not cost a peer
    // several intervals of isolation.
    let next = now + Duration::from_secs(5);
    assert_eq!(health.select(&peers, next), vec![dead], "one failure should retry soon");
    assert!(sync_once(&node.engine, dead, SECRET).await.is_err());
    health.failed(dead, next);

    // Repeated failure is what earns the backoff.
    assert!(
        health.select(&peers, next + Duration::from_secs(5)).is_empty(),
        "a peer failing repeatedly must stop costing a connection every round"
    );
    assert_eq!(health.failures(dead), 2);
}

fn field(path: &str) -> kimmy_storage::IndexField {
    kimmy_storage::IndexField { path: path.into(), descending: false }
}
