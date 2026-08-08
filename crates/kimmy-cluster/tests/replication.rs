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

fn field(path: &str) -> kimmy_storage::IndexField {
    kimmy_storage::IndexField { path: path.into(), descending: false }
}
