//! Replication over real TCP sockets.
//!
//! The convergence rules are already tested between engines in one process
//! (`kimmy-storage/src/sync.rs`). These tests exist for what that could not
//! reach: that the wire carries the types faithfully, that the handshake
//! actually gates access, and that a listener survives a peer misbehaving.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use std::collections::BTreeSet;

use bson::doc;
use kimmy_cluster::protocol::{Message, ProtocolError, read_frame, write_frame};
use kimmy_cluster::transport::{
    DivergenceProbe, PeerStalls, entries_threshold, push_entry, serve, serve_with, sync_once,
    sync_once_with,
};
use kimmy_core::{DocId, Hlc};
use kimmy_storage::Engine;
use tokio::net::{TcpListener, TcpStream};

const SECRET: &str = "a-shared-cluster-secret";

struct Node {
    engine: Arc<Engine>,
    addr: std::net::SocketAddr,
    serving: tokio::task::JoinHandle<()>,
    path: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

/// Start a node serving replication on an ephemeral port.
async fn node() -> Node {
    node_with_secret(SECRET).await
}

async fn node_with_secret(secret: &str) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let engine = Arc::new(Engine::open(&path).unwrap());
    let (addr, serving) = listen(&engine, secret).await;
    Node { engine, addr, serving, path, _dir: dir }
}

/// Bind an ephemeral port and serve `engine` on it.
///
/// Port 0: the OS picks, so parallel tests never collide.
async fn listen(
    engine: &Arc<Engine>,
    secret: &str,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = tokio::spawn(serve(Arc::clone(engine), listener, secret.to_string()));
    (addr, serving)
}

impl Node {
    /// Stop serving, close the engine, and reopen the same database under the
    /// same identity — a member restarting, in process.
    ///
    /// The address changes, because the old listener is gone; what a restart
    /// preserves is the data directory and the node id, and those are what the
    /// tests using this are about.
    async fn restart(self) -> Node {
        let Node { engine, serving, path, _dir, .. } = self;
        serving.abort();
        let _ = serving.await;
        // Per-connection tasks hold their own handle and end when the peer
        // hangs up, which is moments after a round returns. redb allows one
        // open handle per process, so wait for the last one.
        while Arc::strong_count(&engine) > 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        drop(engine);

        let engine = Arc::new(Engine::open(&path).unwrap());
        let (addr, serving) = listen(&engine, SECRET).await;
        Node { engine, addr, serving, path, _dir }
    }
}

/// Pull into `into` from `from`, both directions making a full round.
async fn sync(a: &Node, b: &Node) {
    sync_once(&a.engine, b.addr, SECRET, None).await.expect("a should pull from b");
    sync_once(&b.engine, a.addr, SECRET, None).await.expect("b should pull from a");
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

/// A replicated document costs about its own size on the wire, not twelve times it.
///
/// Serde encodes a bare `Vec<u8>` as a BSON array of int32s — one element, with its own
/// index key, per byte. `OplogEntry::body` and `SnapshotDoc::body` therefore inflated every
/// replicated document roughly twelvefold: a 1 MiB document became a 12.5 MiB entry, and a
/// batch hit the 64 MiB frame limit at around 5 MiB of real data. `serde_bytes` makes them
/// binary.
///
/// Bounded from **both** sides on purpose. An upper bound alone passes just as happily when
/// the body is dropped altogether — the first version of this test did exactly that, and
/// went green while measuring a 27-byte schema entry it had picked by mistake.
#[tokio::test]
async fn a_replicated_document_costs_about_its_own_size_on_the_wire() {
    let a = node().await;

    let ca = a.engine.create_collection("shop", "wide").unwrap();
    let payload = "x".repeat(1024 * 1024);
    a.engine.insert(&ca, doc! { "_id": "d0", "blob": payload.clone() }).unwrap();

    let entries = a.engine.entries_for_peer(kimmy_core::Hlc::ZERO, 10).unwrap().entries;
    let insert =
        entries.iter().find(|e| e.kind == kimmy_core::OpKind::Insert).expect("the insert entry");

    let body = insert.body.as_ref().expect("the insert carries its post-image").len();
    assert!(body > payload.len(), "the body should hold the document, got {body} bytes");

    let encoded = bson::serialize_to_vec(insert).unwrap().len();
    assert!(encoded >= body, "the encoding cannot be smaller than the body it carries");
    assert!(
        encoded < body + (64 * 1024),
        "a {body}-byte body should cost about that on the wire, not {encoded} bytes"
    );

    // And it must still come back, which is the half an encoding change can quietly break.
    let round_tripped: kimmy_core::OplogEntry =
        bson::deserialize_from_slice(&bson::serialize_to_vec(insert).unwrap()).unwrap();
    assert_eq!(round_tripped.body.as_deref(), insert.body.as_deref());
}

/// A batch of large entries must not exceed the frame limit and wedge the cluster.
///
/// `MAX_BATCH` bounds a response by *entry count* while `MAX_FRAME` bounds it by *bytes*,
/// so entries big enough to average over 64 KiB make a full batch exceed the frame. The
/// serving side then fails to write it and drops the connection — and because the same
/// oversized batch is the next thing to send, it does so on every round, for ever. Nothing
/// converges and nothing recovers.
///
/// Seen on a three-node 0.13.0 cluster carrying 1024-dimension vectors:
/// `frame of 67789268 bytes exceeds the 67108864 byte limit`, every five seconds,
/// indefinitely.
#[tokio::test]
async fn a_batch_of_large_entries_still_replicates() {
    let a = node().await;
    let b = node().await;

    // Enough to exceed the 64 MiB frame in far fewer than the 1024 entries a batch is
    // allowed. It has to be this much real data now: bodies travel as binary, so a
    // document costs about its own size rather than twelve times it. When they were
    // arrays of int32s a tenth of this was plenty — which is exactly the sort of
    // quiet slackening that leaves a regression test measuring nothing.
    let ca = a.engine.create_collection("shop", "big").unwrap();
    let payload = "x".repeat(1024 * 1024);
    for i in 0..70 {
        a.engine.insert(&ca, doc! { "_id": format!("d{i}"), "blob": &payload }).unwrap();
    }

    // Several rounds, because a batch this heavy is deliberately not served in one.
    // The point is that it converges at all, rather than the peer refusing the same
    // oversized frame for ever. Ten is generous and still bounded, so a regression
    // fails rather than hangs.
    for _ in 0..10 {
        sync_once(&b.engine, a.addr, SECRET, None).await.expect("b should pull from a");
    }

    let cb = b.engine.get_collection("shop", "big").expect("the collection should have replicated");
    let missing: Vec<usize> = (0..70)
        .filter(|i| b.engine.get(&cb, &DocId::String(format!("d{i}"))).unwrap().is_none())
        .collect();
    assert!(missing.is_empty(), "these never replicated: {missing:?}");
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

    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();

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
    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();

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
    let second = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();

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

    let err = sync_once(&intruder.engine, a.addr, "not-the-cluster-secret", None)
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
    //
    // Since replication moved onto TLS this is refused one layer earlier — the
    // plaintext frame below is not a valid ClientHello, so the connection dies
    // before the protocol sees it. The property is unchanged and the test still
    // holds it; what proves the *handshake* gates reads is now
    // `a_peer_with_the_wrong_secret_is_refused`, which completes TLS and fails
    // on the HMAC.
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "secret" }).unwrap();

    let mut stream = TcpStream::connect(a.addr).await.unwrap();
    // Skip the handshake entirely and ask straight out.
    write_frame(&mut stream, &Message::AskVersions { witnessed: false }).await.unwrap();

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

    // Garbage: a valid length prefix followed by bytes that are not BSON. With
    // TLS in front it is now rejected as a malformed ClientHello rather than as
    // a malformed frame; either way the listener must survive it, which is what
    // this test is for.
    let mut rude = TcpStream::connect(a.addr).await.unwrap();
    use tokio::io::AsyncWriteExt;
    rude.write_all(&8u32.to_be_bytes()).await.unwrap();
    rude.write_all(b"notabson").await.unwrap();
    drop(rude);

    // A well-behaved peer still works.
    let b = node().await;
    sync_once(&b.engine, a.addr, SECRET, None).await.expect("the listener must still be serving");
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
    sync_once(&b.engine, a.addr, SECRET, None).await.expect("the listener must still be serving");
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

    let result =
        tokio::time::timeout(Duration::from_secs(10), sync_once(&engine, dead, SECRET, None))
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
    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();

    let cb = b.engine.get_collection("shop", "orders").expect("the collection must arrive");
    assert_eq!(b.engine.count(&cb).unwrap(), 50, "every document must arrive");
    assert!(
        cb.indexes.iter().any(|i| i.name == "item_1" && i.unique),
        "the index must arrive with its uniqueness"
    );

    // And it must stop asking for history that no longer exists.
    let second = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(second.total(), 0, "a caught-up node must not keep resyncing: {second:?}");
}

#[tokio::test]
async fn a_snapshot_of_a_high_bit_collection_crosses_the_wire() {
    // The test above passes for a collection whose derived id happens to sit
    // below i64::MAX. Half of them do not, and BSON has no unsigned 64-bit
    // integer: with the snapshot types carrying a bare u64, every page naming
    // such a collection failed to encode, the serving side logged "cannot fit
    // into BSON", and a member past its peers' retention horizon — the only
    // case a snapshot serves — never caught up. Observed on a three-member
    // cluster whose every pair went dark 24 h after birth.
    let name = (0u32..)
        .map(|i| format!("orders-{i}"))
        .find(|n| kimmy_core::CollectionId::derive("shop", n).0 > i64::MAX as u64)
        .unwrap();

    let a = node().await;
    let ca = a.engine.create_collection("shop", &name).unwrap();
    assert!(ca.id.0 > i64::MAX as u64);
    for i in 0..50i64 {
        a.engine.insert(&ca, doc! { "_id": i }).unwrap();
    }
    a.engine
        .collect_garbage_at(
            kimmy_storage::physical_now_ms() + 1_000_000_000,
            kimmy_storage::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();

    let b = node().await;
    let first = sync_once(&b.engine, a.addr, SECRET, None).await.expect("the snapshot must encode");
    assert_eq!(first.applied, 50);
    let cb = b.engine.get_collection("shop", &name).expect("the collection must arrive");
    assert_eq!(b.engine.count(&cb).unwrap(), 50);

    let second = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
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
    let outcome = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();

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
    assert!(sync_once(&node.engine, dead, SECRET, None).await.is_err());
    health.failed(dead, now);

    // A single failure is forgiven promptly — a blip should not cost a peer
    // several intervals of isolation.
    let next = now + Duration::from_secs(5);
    assert_eq!(health.select(&peers, next), vec![dead], "one failure should retry soon");
    assert!(sync_once(&node.engine, dead, SECRET, None).await.is_err());
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

// ---------------------------------------------------------------------------
// Channel binding
// ---------------------------------------------------------------------------

/// A man-in-the-middle that terminates TLS on both sides.
///
/// Nodes do not verify each other's certificates, so both handshakes succeed
/// and this relay can read every frame — which is the point. It is what makes
/// unverified TLS on its own insufficient, and therefore what the channel
/// binding exists to defeat.
///
/// Returns the address to dial and a handle that reports how many bytes it
/// managed to relay before the connection died.
async fn man_in_the_middle(
    target: std::net::SocketAddr,
) -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    let relayed = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let counted = Arc::clone(&relayed);
    tokio::spawn(async move {
        // Its own certificate, exactly as a real attacker would present.
        let attacker = kimmy_cluster::tls::ClusterTls::new().expect("attacker TLS");
        while let Ok((victim_tcp, _)) = listener.accept().await {
            let acceptor = attacker.acceptor();
            let connector = attacker.connector();
            let counted = Arc::clone(&counted);
            tokio::spawn(async move {
                let Ok(mut victim) = acceptor.accept(victim_tcp).await else {
                    return;
                };
                let Ok(upstream_tcp) = TcpStream::connect(target).await else {
                    return;
                };
                let Ok(mut upstream) = connector
                    .connect(kimmy_cluster::tls::ClusterTls::server_name(), upstream_tcp)
                    .await
                else {
                    return;
                };
                // Plain byte relay between two decrypted sessions.
                let (mut vr, mut vw) = tokio::io::split(&mut victim);
                let (mut ur, mut uw) = tokio::io::split(&mut upstream);
                let up = async {
                    let n = tokio::io::copy(&mut vr, &mut uw).await.unwrap_or(0);
                    counted.fetch_add(n as usize, Ordering::Relaxed);
                };
                let down = async {
                    let n = tokio::io::copy(&mut ur, &mut vw).await.unwrap_or(0);
                    counted.fetch_add(n as usize, Ordering::Relaxed);
                };
                tokio::join!(up, down);
            });
        }
    });

    (addr, relayed)
}

#[tokio::test]
async fn a_man_in_the_middle_cannot_relay_the_handshake() {
    // The single property the cluster's TLS rests on.
    //
    // Certificates are not verified, so an attacker who can intercept the
    // connection completes TLS with both sides and reads everything. What stops
    // it is that the handshake proof is computed over the TLS session's
    // exporter: the attacker holds two sessions with different exporters, so
    // the proof it forwards is over the wrong value and cannot be recomputed
    // without `cluster_secret`.
    //
    // If this test ever passes without the binding, replication is confidential
    // against a passive listener and nothing more — and it would still look
    // like it was working, which is why this is asserted rather than argued.
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "confidential" }).unwrap();

    let (mitm_addr, _relayed) = man_in_the_middle(a.addr).await;

    let b = node().await;
    let err = sync_once(&b.engine, mitm_addr, SECRET, None)
        .await
        .expect_err("a relayed handshake must be refused");

    assert!(
        matches!(
            err,
            ProtocolError::Unauthenticated
                | ProtocolError::Malformed(_)
                | ProtocolError::Io(_)
                | ProtocolError::Closed
        ),
        "expected the handshake to fail, got {err:?}"
    );
    assert!(
        b.engine.get_collection("shop", "orders").is_err(),
        "nothing may reach a peer whose handshake was relayed"
    );
}

#[tokio::test]
async fn the_same_two_nodes_converge_when_nobody_is_in_the_middle() {
    // The control for the test above. Without it, a bug that broke *all*
    // replication would make the man-in-the-middle test pass for the wrong
    // reason — which is the trap this suite has fallen into before.
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "confidential" }).unwrap();

    let b = node().await;
    sync_once(&b.engine, a.addr, SECRET, None).await.expect("a direct round must succeed");

    let cb = b.engine.get_collection("shop", "orders").expect("the collection must replicate");
    assert!(b.engine.get(&cb, &DocId::String("confidential".into())).unwrap().is_some());
}

/// A peer that completes the TCP handshake and then says nothing at all.
///
/// Accepted connections are held rather than dropped, because dropping them
/// would make the dial fail immediately and prove nothing.
async fn silent_peer() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    addr
}

#[tokio::test]
async fn a_silent_peer_cannot_stall_a_sync_round() {
    // The bug this pins: neither the TCP connect nor the TLS handshake that
    // follows it was bounded, and a sync round awaits its peers one at a time.
    // So a peer that accepts a connection and then never speaks did not merely
    // fail to sync — it held the round open indefinitely, and every healthy
    // peer scheduled behind it in that round waited too. Measured against a
    // running three-node cluster, one unreachable node took convergence between
    // the two healthy ones from about six seconds to about four minutes.
    //
    // The assertion is therefore about *time*, not about the error: any error
    // is fine, taking forever is not.
    let addr = silent_peer().await;
    let b = node().await;

    let started = std::time::Instant::now();
    let result =
        tokio::time::timeout(Duration::from_secs(30), sync_once(&b.engine, addr, SECRET, None))
            .await
            .expect("the dial must give up on its own rather than hang");
    let elapsed = started.elapsed();

    result.expect_err("a peer that never speaks cannot produce a successful round");
    assert!(
        elapsed < Duration::from_secs(15),
        "the dial must be bounded by CONNECT_TIMEOUT, took {elapsed:?}"
    );
}

/// Ignored: it spends the connect timeout waiting on a deliberately
/// unroutable address, and its result depends on how the host treats
/// TEST-NET-1 — a sandbox with no network at all fails instantly and proves
/// nothing. Run it on a real network with
/// `cargo test -p kimmy-cluster --test replication -- --ignored`.
/// See docs/testing.md.
#[tokio::test]
#[ignore = "waits on a real connect timeout; needs a network that drops rather than refuses"]
async fn an_unroutable_peer_cannot_stall_a_sync_round() {
    // The other half of the same bug. `silent_peer` covers the TLS handshake;
    // this covers the TCP connect, which is the one that actually bit — a
    // stopped container's address drops packets instead of refusing them, so
    // the kernel spent its full SYN retry budget before returning.
    let addr: std::net::SocketAddr = "192.0.2.1:7900".parse().unwrap();
    let b = node().await;

    let started = std::time::Instant::now();
    let result =
        tokio::time::timeout(Duration::from_secs(60), sync_once(&b.engine, addr, SECRET, None))
            .await
            .expect("the connect must give up on its own rather than hang");
    let elapsed = started.elapsed();

    result.expect_err("an unroutable address cannot produce a successful round");
    assert!(
        elapsed < Duration::from_secs(15),
        "the connect must be bounded by CONNECT_TIMEOUT, took {elapsed:?}"
    );
}

fn vector_config() -> kimmy_core::VectorConfig {
    kimmy_core::VectorConfig {
        fields: vec!["body".into()],
        provider: kimmy_core::ProviderConfig::Byo {},
        dim: 8,
        metric: kimmy_core::Metric::Cosine,
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

/// Two nodes where the *receiver* has dropped a collection the sender still
/// holds a schema change for.
///
/// The order is the whole point. Replaying a whole history in stamp order never
/// hits this: the collection is created before the change that names it and
/// dropped after. The failure needs the drop to be applied *first*, which is
/// what happens when the drop originates on the receiving node while an earlier
/// change is still only on the sender.
async fn sender_and_a_receiver_that_dropped_it() -> (Node, Node) {
    let a = node().await;
    let b = node().await;

    a.engine.create_collection("shelf", "doomed").unwrap();
    sync_once(&b.engine, a.addr, SECRET, None).await.expect("the create must replicate");

    // Recorded on the sender only -- the receiver has not seen it yet.
    a.engine.configure_vectors("shelf", "doomed", vector_config()).unwrap();

    // ...and meanwhile the receiver drops the collection.
    b.engine.drop_collection("shelf", "doomed").unwrap();

    (a, b)
}

#[tokio::test]
async fn a_dropped_collection_does_not_wedge_replication() {
    // The bug this pins. A schema change names its collection by name, so
    // replaying one the receiver has already dropped raised
    // `CollectionNotFound` -- which failed the *whole* round, not just that
    // entry. The entry never leaves the sender's oplog, so the position never
    // advanced and every later round died on the same entry. Measured on a real
    // three-node cluster: one dropped collection permanently stopped
    // replication, a node sat at four documents while its peer held six, new
    // writes never arrived, and nothing in the log said so.
    let (a, b) = sender_and_a_receiver_that_dropped_it().await;

    // Unrelated work recorded after all of that. This is what must still
    // arrive: the round has to get past the poisoned entry to reach it.
    let live = a.engine.create_collection("shelf", "survivor").unwrap();
    a.engine.insert(&live, doc! { "_id": "must-replicate" }).unwrap();

    sync_once(&b.engine, a.addr, SECRET, None)
        .await
        .expect("the round must not fail on a schema change for a dropped collection");

    let coll = b
        .engine
        .get_collection("shelf", "survivor")
        .expect("work recorded after the drop must replicate");
    assert!(
        b.engine.get(&coll, &DocId::String("must-replicate".into())).unwrap().is_some(),
        "a dropped collection must not block the entries behind it"
    );
}

#[tokio::test]
async fn a_dropped_collection_does_not_come_back_through_replication() {
    // The control. Skipping the entry must not turn into recreating the
    // collection it names -- resurrecting a drop would be a worse bug than the
    // one being fixed.
    let (a, b) = sender_and_a_receiver_that_dropped_it().await;
    sync_once(&b.engine, a.addr, SECRET, None).await.expect("the round must succeed");

    assert!(
        b.engine.get_collection("shelf", "doomed").is_err(),
        "a collection dropped on the receiver must not be recreated by replication"
    );
}

/// A sender holding an index definition over two paths, and a receiver
/// holding a document with arrays at both — a document that definition
/// cannot key.
///
/// A created the index while no document held arrays at both paths. B holds
/// one of its own, written while it had not heard of the definition, which
/// is the state a partition, a lagging member, or the seconds after a
/// `createIndex` leave behind.
async fn sender_with_an_index_over_a_document_the_receiver_cannot_key() -> (Node, Node) {
    let a = node().await;
    let b = node().await;

    a.engine.create_collection("shop", "orders").unwrap();
    a.engine
        .create_index(
            "shop",
            "orders",
            vec![
                kimmy_core::IndexField::ascending("tags"),
                kimmy_core::IndexField::ascending("cats"),
            ],
            false,
            None,
        )
        .expect("accepted: no document holds arrays at both paths yet");
    let ca = a.engine.get_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "fine", "tags": ["x", "y"], "cats": "p" }).unwrap();

    let cb = b.engine.create_collection("shop", "orders").unwrap();
    b.engine.insert(&cb, doc! { "_id": "both-b", "tags": ["x"], "cats": ["p"] }).unwrap();

    (a, b)
}

/// Whether `engine` holds `name` on `shop.orders`, and how many of its
/// documents that index could not key.
fn index_state(engine: &kimmy_storage::Engine, name: &str) -> Option<u64> {
    let coll = engine.get_collection("shop", "orders").unwrap();
    let index = coll.index(name)?;
    Some(engine.unkeyed_count(&coll, index.id).unwrap())
}

#[tokio::test]
async fn a_replayed_index_over_a_document_the_receiver_cannot_key_builds_and_does_not_wedge() {
    // The order ADR-123 answered by refusing the definition, over the wire.
    // Under ADR-139 the receiver builds it, files its own document unkeyed
    // under it, counts nothing as refused, and reaches everything behind it.
    let (a, b) = sender_with_an_index_over_a_document_the_receiver_cannot_key().await;

    // Unrelated work recorded after all of that, which the round has to get
    // past the definition to reach.
    let live = a.engine.create_collection("shelf", "survivor").unwrap();
    a.engine.insert(&live, doc! { "_id": "must-replicate" }).unwrap();

    let outcome = sync_once(&b.engine, a.addr, SECRET, None)
        .await
        .expect("the round must not fail on an index over a document this node cannot key");
    assert_eq!(outcome.ddl_refused, 0, "built, not refused: {outcome:?}");
    assert_eq!(
        index_state(&b.engine, "tags_1_cats_1"),
        Some(1),
        "the receiver holds the index, with its own document filed unkeyed"
    );

    let coll = b
        .engine
        .get_collection("shelf", "survivor")
        .expect("work recorded after the definition must replicate");
    assert!(
        b.engine.get(&coll, &DocId::String("must-replicate".into())).unwrap().is_some(),
        "a schema change must not block the entries behind it"
    );
    let second = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(second.total(), 0, "witnessed, so the window is not re-served: {second:?}");
}

#[tokio::test]
async fn a_document_an_index_here_cannot_key_does_not_wedge_replication() {
    // The wedge, over the wire: the member holding the index pulls a
    // document a member without it legally accepted. Before ADR-139 the
    // batch failed as a malformed frame, the witnessed vector was discarded,
    // and the same window was re-requested with backoff to 300 s for the
    // life of the process — observed on a three-member cluster running
    // 0.23.2, twice in one hour, with four collections diverging behind it.
    // Now it is a round like any other.
    let (a, b) = sender_with_an_index_over_a_document_the_receiver_cannot_key().await;
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    b.engine.insert(&cb, doc! { "_id": "later" }).unwrap();

    let outcome = sync_once(&a.engine, b.addr, SECRET, None)
        .await
        .expect("the round must not fail on a document this node's index cannot key");
    assert_eq!(outcome.applied, 2, "both of B's documents apply: {outcome:?}");
    assert_eq!(outcome.ddl_refused, 0, "{outcome:?}");
    let ca = a.engine.get_collection("shop", "orders").unwrap();
    assert!(a.engine.get(&ca, &DocId::String("both-b".into())).unwrap().is_some(), "stored");
    assert_eq!(
        index_state(&a.engine, "tags_1_cats_1"),
        Some(1),
        "the holder keeps its index, with the document filed unkeyed under it"
    );
    let second = sync_once(&a.engine, b.addr, SECRET, None).await.unwrap();
    assert_eq!(second.total(), 0, "witnessed, so the window is not re-served: {second:?}");

    // And the two members converge on one state, whichever order the pair
    // met in on each of them.
    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(index_state(&b.engine, "tags_1_cats_1"), Some(1));
    assert_eq!(a.engine.count(&ca).unwrap(), 3);
    assert_eq!(b.engine.count(&cb).unwrap(), 3);
}

#[tokio::test]
async fn a_dropped_index_never_comes_back_through_replication() {
    // The control, and the reason the index tombstone exists. B took A's
    // create and drop in one round, then wrote a two-array document. A later
    // round re-serves the window holding the create — here because A has
    // since taken an entry from a third member C that B has never seen, so
    // B's threshold against A falls to the beginning — and the create must
    // read as history against the tombstone, not rebuild the index, and not
    // fail the round trying.
    let c = node().await;
    let cc = c.engine.create_collection("shop", "orders").unwrap();
    c.engine.insert(&cc, doc! { "_id": "c-early" }).unwrap();

    let a = node().await;
    let b = node().await;
    a.engine.create_collection("shop", "orders").unwrap();
    a.engine
        .create_index(
            "shop",
            "orders",
            vec![
                kimmy_core::IndexField::ascending("tags"),
                kimmy_core::IndexField::ascending("cats"),
            ],
            false,
            None,
        )
        .unwrap();
    a.engine.drop_index("shop", "orders", "tags_1_cats_1").unwrap();
    sync_once(&b.engine, a.addr, SECRET, None).await.expect("the create and the drop replicate");
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    assert!(cb.index("tags_1_cats_1").is_none());
    b.engine.insert(&cb, doc! { "_id": "both", "tags": ["x", "y"], "cats": ["p", "q"] }).unwrap();

    // A learns of C; B has never heard of C, so its next round with A is
    // served from the beginning of everything A holds.
    sync_once(&a.engine, c.addr, SECRET, None).await.unwrap();
    let outcome = sync_once(&b.engine, a.addr, SECRET, None)
        .await
        .expect("a re-served create older than its drop must not fail the round");
    assert_eq!(outcome.ddl_refused, 0, "history, not a refusal: {outcome:?}");
    assert!(
        b.engine.get_collection("shop", "orders").unwrap().index("tags_1_cats_1").is_none(),
        "a dropped index must not come back through replication"
    );
    assert!(
        b.engine.get(&cb, &DocId::String("c-early".into())).unwrap().is_some(),
        "and the entry that widened the window arrived"
    );
}

/// A sender holding a definition the receiver refuses: the receiver holds
/// the same name with **no creation stamp** — what a definition written
/// before ADR-132 looks like on disk — and a rival it cannot arbitrate is
/// refused and counted rather than resolved (ADR-123). Under ADR-139 no
/// document can make a non-unique definition unbuildable, so this is the
/// refusal a wire test reaches for.
async fn sender_with_a_definition_the_receiver_cannot_arbitrate() -> (Node, Node) {
    let a = node().await;
    let b = node().await;
    let source = node().await;

    source.engine.create_collection("shop", "orders").unwrap();
    source
        .engine
        .create_index(
            "shop",
            "orders",
            vec![kimmy_core::IndexField::ascending("email")],
            false,
            Some("by_email".into()),
        )
        .unwrap();
    let mut page = source.engine.snapshot_page(None, None).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            index.created = None;
        }
    }
    page.documents.clear();
    page.versions = kimmy_core::VersionVector::default();
    b.engine
        .apply_snapshot_page(
            a.engine.node_id(),
            &mut kimmy_storage::SnapshotProgress::whole_database(),
            &page,
        )
        .unwrap();

    a.engine.create_collection("shop", "orders").unwrap();
    a.engine
        .create_index(
            "shop",
            "orders",
            vec![kimmy_core::IndexField::ascending("email")],
            true,
            Some("by_email".into()),
        )
        .unwrap();
    (a, b)
}

#[tokio::test]
async fn the_round_report_reaches_the_hook() {
    // What the replication loop tells the caller after each tick, beyond
    // lag: rounds that failed, peers it is backing off from, schema changes
    // it refused. Each one exists because the lag gauge said nothing while a
    // cluster was wedged (ADR-123). One peer that will refuse a connection,
    // one whose history holds a definition this node cannot arbitrate.
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let (a, b) = sender_with_a_definition_the_receiver_cannot_arbitrate().await;
    // Bound and released: a port with nothing listening refuses at once.
    let dead = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![a.addr, dead])], SECRET.into(), b.addr);
    config.sync_interval = Duration::from_millis(100);
    config.discovery_interval = Duration::from_millis(100);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&b.engine), config));

    let mut seen = RoundReport::default();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while seen.failed == 0 || seen.backing_off == 0 || seen.ddl_refused == 0 {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("no report carried every signal in time; saw {seen:?}"))
            .expect("the loop must keep reporting");
        seen.failed += report.failed;
        seen.ddl_refused += report.ddl_refused;
        seen.backing_off = seen.backing_off.max(report.backing_off);
    }
    looping.abort();

    assert!(seen.failed >= 1, "the dead peer's rounds fail: {seen:?}");
    assert!(seen.backing_off >= 1, "and it is backed off: {seen:?}");
    assert_eq!(seen.ddl_refused, 1, "the refused create is counted exactly once: {seen:?}");
}

#[tokio::test]
async fn a_peer_that_keeps_failing_is_reported_more_than_once() {
    // The wedge above was invisible as well as permanent: only the first
    // failure was reported, and every one after it went to `debug`, below the
    // default level. A peer that never recovers has to keep saying so.
    use kimmy_cluster::{PeerHealth, WARN_INTERVAL};
    let dead: std::net::SocketAddr = "127.0.0.1:9".parse().unwrap();
    let mut health = PeerHealth::new(3, Duration::from_secs(5));

    let start = std::time::Instant::now();
    assert!(health.failed(dead, start), "the first failure must be reported");
    assert!(
        !health.failed(dead, start + Duration::from_secs(1)),
        "a failure moments later must not be reported again"
    );
    assert!(
        health.failed(dead, start + WARN_INTERVAL),
        "a peer still failing a full interval later must be reported again"
    );
}

const HOUR_MS: u64 = 60 * 60 * 1000;
const DAY_SECS: u64 = 24 * 60 * 60;

/// Converge three nodes: every pair, both ways, twice, so a write reaches the
/// third member through the middle one as well as directly.
async fn converge(a: &Node, b: &Node, c: &Node) {
    for _ in 0..2 {
        sync(a, b).await;
        sync(b, c).await;
        sync(a, c).await;
    }
}

/// An entry from an origin nobody is, stamped `wall_ms`: applied to a node, it
/// moves that node's logical clock there, and once replicated it is the oplog
/// tail every member resumes its clock from after a restart. How a test makes
/// 36 hours pass without waiting for them.
fn entry_stamped(collection: kimmy_core::CollectionId, wall_ms: u64) -> kimmy_core::OplogEntry {
    kimmy_core::OplogEntry {
        stamp: kimmy_core::Stamp::new(
            kimmy_core::Hlc::new(wall_ms, 0),
            kimmy_core::NodeId::generate(),
        ),
        kind: kimmy_core::OpKind::Insert,
        collection,
        doc_id: Some(DocId::String("clock".into())),
        body: Some(bson::serialize_to_vec(&doc! { "_id": "clock" }).unwrap()),
    }
}

/// Run a retention pass on every node as though it were `now_ms`, with the
/// default day of retention for both the oplog and tombstones.
fn age_out(nodes: [&Node; 3], now_ms: u64) {
    for node in nodes {
        node.engine
            .collect_garbage_at(now_ms, kimmy_storage::RetentionPolicy::new(DAY_SECS, DAY_SECS))
            .unwrap();
    }
}

/// The rolling-restart shape (ADR-097): a three-member cluster, converged, in
/// which A last wrote 36 hours ago and everything from back then except the
/// tail has been collected on every member. Returns the nodes and the stamp of
/// A's last write before the silence — which is every peer's coverage of A.
///
/// A restart of A then writes once, the way a member re-registers itself in
/// the topology when its build or endpoint changed, and the tests below look
/// at A's first round afterwards from both sides.
async fn converged_after_a_long_silence() -> (Node, Node, Node, kimmy_core::Hlc) {
    let a = node().await;
    let b = node().await;
    let c = node().await;

    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "a-before" }).unwrap();
    converge(&a, &b, &c).await;
    // B and C both write after A, so the coarse horizon on every member ends
    // up above A's last write once all of it is collected.
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    b.engine.insert(&cb, doc! { "_id": "b-before" }).unwrap();
    let cc = c.engine.get_collection("shop", "orders").unwrap();
    c.engine.insert(&cc, doc! { "_id": "c-before" }).unwrap();
    converge(&a, &b, &c).await;
    let a_last = b.engine.witnessed_vector().unwrap().get(a.engine.node_id());
    assert!(a_last > kimmy_core::Hlc::ZERO);

    // 36 hours pass. Every member's clock moves, and the entry that moved it
    // becomes the tail each of them keeps through collection.
    let later = kimmy_storage::physical_now_ms() + 36 * HOUR_MS;
    b.engine.apply_batch(&[entry_stamped(ca.id, later)]).unwrap();
    converge(&a, &b, &c).await;
    age_out([&a, &b, &c], later + HOUR_MS);

    assert!(
        a.engine.oplog_collected_through().unwrap() > a_last,
        "A's last write was collected, and so was something after it"
    );
    (a, b, c, a_last)
}

#[tokio::test]
async fn a_restarted_member_does_not_name_its_converged_peers_stale_on_its_first_round() {
    // The first finding from the roll (ADR-097). Half a second after A came
    // back, its first round named both peers stale: behind by the time since
    // the *previous* restart, with the message that tells an operator to reset
    // them. Both were converged; neither had been away a minute.
    let (a, b, c, a_last) = converged_after_a_long_silence().await;

    let a = a.restart().await;
    let ca = a.engine.get_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "a-after-restart" }).unwrap();

    for peer in [&b, &c] {
        // The bare span is the incident's number: A's new write against the
        // peer's coverage of A, which is A's write from before the silence.
        let a_id = a.engine.node_id();
        let raw = a
            .engine
            .witnessed_vector()
            .unwrap()
            .get(a_id)
            .wall_ms
            .saturating_sub(peer.engine.version_vector().unwrap().get(a_id).wall_ms);
        assert!(raw > DAY_SECS * 1_000, "the scenario must reproduce the raw gap: {raw} ms");
        assert_eq!(peer.engine.version_vector().unwrap().get(a_id), a_last);

        let outcome = sync_once(&a.engine, peer.addr, SECRET, None).await.unwrap();
        assert_eq!(
            outcome.behind_ms, 0,
            "a peer that can still be served everything it lacks is not a stale rejoiner: {outcome:?}"
        );
    }
}

/// What the requester logs when a peer answers `BeyondHorizon` and the round
/// falls back to a snapshot.
const SNAPSHOT_FALLBACK: &str = "falling back to a snapshot";

/// `round`'s result, and every event message logged while it ran, from its
/// own task.
///
/// The cluster wire is TLS under the cluster secret, so a test cannot watch
/// which messages a round sent. The requester's own log line is what says it
/// asked for a snapshot.
async fn logged_during<T>(round: impl std::future::Future<Output = T>) -> (T, Vec<String>) {
    use tracing::instrument::WithSubscriber;
    let lines = Arc::new(std::sync::Mutex::new(Vec::new()));
    let out = round.with_subscriber(Recorder(Arc::clone(&lines))).await;
    let lines = lines.lock().unwrap().clone();
    (out, lines)
}

/// A subscriber that keeps each event's message and nothing else.
struct Recorder(Arc<std::sync::Mutex<Vec<String>>>);

impl tracing::Subscriber for Recorder {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0.lock().unwrap().push(message.0);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test]
async fn a_restarted_member_serves_its_first_puller_from_the_oplog() {
    // The second finding. The first peer to pull from a restarted member was
    // told it was beyond the horizon and fell back to a snapshot — of a store
    // it already held in full but for one entry. The threshold it asked from
    // is A's write before the silence, which A collected along with everything
    // around it; per origin, nothing B lacks is gone.
    let (a, b, _c, a_last) = converged_after_a_long_silence().await;

    let a = a.restart().await;
    let ca = a.engine.get_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "a-after-restart" }).unwrap();

    let held = b.engine.witnessed_vector().unwrap();
    let from = held.behind(&a.engine.version_vector().unwrap()).expect("B trails A by one write");
    assert_eq!(from, a_last);
    assert!(
        !a.engine.can_serve_from_oplog(from).unwrap(),
        "by the threshold alone B is beyond A's horizon — the snapshot the roll paid for"
    );

    // The round itself says which way it was served. This used to be read off
    // a re-served tail reported superseded; since ADR-171 an oplog round and a
    // snapshot of this store both apply one document and supersede nothing.
    // What differs is that the requester logs its fallback when the peer
    // answers `BeyondHorizon`, and asks for a snapshot next.
    let (outcome, logged) = logged_during(sync_once(&b.engine, a.addr, SECRET, None)).await;
    let outcome = outcome.unwrap();
    assert!(
        !logged.iter().any(|line| line.contains(SNAPSHOT_FALLBACK)),
        "the round must be served from the oplog, not a snapshot: {logged:?}"
    );
    assert_eq!(outcome.applied, 1, "{outcome:?}");
    assert_eq!(outcome.superseded, 0, "and nothing B holds is served again: {outcome:?}");
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    assert!(b.engine.get(&cb, &DocId::String("a-after-restart".into())).unwrap().is_some());

    let second = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(second.total(), 0, "and B is then caught up: {second:?}");
}

// -----------------------------------------------------------------------
// A caught-up member is not re-served what it holds (ADR-171)

/// Pull into `into` from `from` until a pull reaches the peer's tail, the way
/// one tick's contact drains a peer (ADR-157). Each pull's outcome, in order.
async fn drain(into: &Node, from: &Node) -> Vec<kimmy_storage::SyncOutcome> {
    let mut pulls = Vec::new();
    loop {
        let outcome = sync_once(&into.engine, from.addr, SECRET, None).await.expect("pull");
        let truncated = outcome.truncated;
        pulls.push(outcome);
        if !truncated {
            return pulls;
        }
        assert!(pulls.len() < 1_000, "a drain that does not end");
    }
}

/// Entries a busy member writes after a quiet member's last write: more than
/// four full windows, so a peer re-served them needs at least five pulls.
const BUSY: usize = 4 * kimmy_cluster::protocol::MAX_BATCH + 100;

fn busy_documents(prefix: &str) -> Vec<bson::Document> {
    (0..BUSY).map(|i| doc! { "_id": format!("{prefix}-{i}") }).collect()
}

/// Three converged members: B wrote once and then went quiet, and A then wrote
/// [`BUSY`] entries that B and C both hold. C's position on B is B's quiet
/// write, below every one of A's.
async fn a_quiet_member_among_caught_up_peers() -> (Node, Node, Node) {
    let a = node().await;
    let b = node().await;
    let c = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "a-first" }).unwrap();
    converge(&a, &b, &c).await;
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    b.engine.insert(&cb, doc! { "_id": "b-quiet" }).unwrap();
    converge(&a, &b, &c).await;

    a.engine.insert_many(&ca, busy_documents("busy")).unwrap();
    drain(&b, &a).await;
    drain(&c, &a).await;
    (a, b, c)
}

#[tokio::test]
async fn one_write_on_a_quiet_member_is_one_pull_for_a_caught_up_peer() {
    // The finding: one document written on a member whose previous local write
    // was forty minutes old made each peer re-read the oplog above that write,
    // about 300 back-to-back pulls applying nothing. The threshold is the
    // peer's position on the quiet member, and the range read from it carried
    // every other origin's entries above it, which the peer already held.
    let (a, b, c) = a_quiet_member_among_caught_up_peers().await;
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    b.engine.insert(&cb, doc! { "_id": "b-after-quiet" }).unwrap();

    let held = c.engine.witnessed_vector().unwrap();
    let from = held.behind(&b.engine.version_vector().unwrap()).expect("C trails B by one write");
    let above = b.engine.read_oplog_from(from, usize::MAX).unwrap().len();
    assert!(above > BUSY, "the fixture must put A's writes above C's position on B: {above}");
    assert!(held.get(a.engine.node_id()) > from, "and C must already hold them");

    let started = std::time::Instant::now();
    let pulls = drain(&c, &b).await;
    let superseded: usize = pulls.iter().map(|p| p.superseded).sum();
    eprintln!(
        "one write after {above} entries: {} pulls, {} superseded, {:?}",
        pulls.len(),
        superseded,
        started.elapsed()
    );
    assert_eq!(
        pulls.len(),
        1,
        "one write is one pull, not a pass over the {above} entries C holds: {pulls:?}"
    );
    assert_eq!(pulls[0].applied, 1, "{:?}", pulls[0]);
    assert_eq!(superseded, 0, "nothing C had processed is served to it again");
    let cc = c.engine.get_collection("shop", "orders").unwrap();
    assert!(c.engine.get(&cc, &DocId::String("b-after-quiet".into())).unwrap().is_some());
}

#[tokio::test]
async fn a_peer_partway_through_an_origin_is_served_everything_it_lacks_of_it() {
    // The guard on the other side: passing over what a peer has processed must
    // be judged per origin. C has pulled one window of A's writes and has then
    // written past all of them itself, so its vector's newest stamp is above
    // everything of A's it still lacks. B serves C, and C must end holding all
    // of A's writes.
    let a = node().await;
    let b = node().await;
    let c = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    converge(&a, &b, &c).await;
    a.engine.insert_many(&ca, busy_documents("busy")).unwrap();
    drain(&b, &a).await;
    let first = sync_once(&c.engine, a.addr, SECRET, None).await.unwrap();
    assert!(first.truncated, "C must stop partway through A: {first:?}");
    let cc = c.engine.get_collection("shop", "orders").unwrap();
    c.engine.insert(&cc, doc! { "_id": "c-later" }).unwrap();
    let held = c.engine.witnessed_vector().unwrap();
    let c_own = held.get(c.engine.node_id());
    let a_part = held.get(a.engine.node_id());
    assert!(
        c_own > a.engine.version_vector().unwrap().get(a.engine.node_id()) && a_part < c_own,
        "the fixture must put C's newest stamp above everything of A's it lacks"
    );

    drain(&c, &b).await;
    assert_eq!(
        c.engine.count(&cc).unwrap(),
        BUSY as u64 + 1,
        "every write of A's C lacked arrives from B"
    );
    assert_eq!(
        c.engine.witnessed_vector().unwrap().get(a.engine.node_id()),
        b.engine.version_vector().unwrap().get(a.engine.node_id())
    );
}

#[tokio::test]
async fn a_push_to_a_caught_up_member_carries_the_change_and_not_what_it_holds() {
    // The push of ADR-143 serves the window a pull from the member's position
    // would. Before ADR-171 that window, read from C's position on B, was a
    // full batch of A's writes C already held and stopped short of B's schema
    // change, so the push named C unreached and left the change to
    // anti-entropy.
    let (_a, b, c) = a_quiet_member_among_caught_up_peers().await;
    b.engine.create_index("shop", "orders", vec![field("n")], false, None).unwrap();
    // B's newest write of its own, read by its stamp: `newest` scans the first
    // two batches of the oplog, and A's writes sit between them and this.
    let b_id = b.engine.node_id();
    let stamp = kimmy_core::Stamp::new(b.engine.version_vector().unwrap().get(b_id), b_id);
    let entry = b.engine.oplog_entry(&stamp).unwrap().expect("B's schema change");
    assert_eq!(entry.kind, kimmy_core::OpKind::CreateIndex);

    let pushed = push_entry(&b.engine, c.addr, SECRET, &entry).await.unwrap();
    assert_eq!(pushed.unreached, None, "{pushed:?}");
    assert_eq!(pushed.outcome.ddl, 1, "the change is applied at once: {pushed:?}");
    assert_eq!(pushed.outcome.superseded, 0, "and nothing C holds is sent: {pushed:?}");
}

// -----------------------------------------------------------------------
// An entry held as state below the member's position is released (ADR-172)

/// What the requester logs when it names spans it holds as state below its
/// own position.
const ASKING_MARKED: &str = "asking the peer to serve entries this node holds as state";

/// Two members, where R holds A's last write S only as state below its own
/// position: R's witnessed vector covered S through a window that never
/// carried it, and a scoped repair then brought S under `Hold`. R is behind A
/// on nothing. The nodes, the collection, and S's stamp.
async fn a_member_holding_a_repaired_entry_below_its_position()
-> (Node, Node, kimmy_storage::CollectionMeta, Hlc) {
    let a = node().await;
    let r = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "a0" }).unwrap();
    a.engine.insert(&ca, doc! { "_id": "s" }).unwrap();
    let a_id = a.engine.node_id();
    let s = a.engine.version_vector().unwrap().get(a_id);

    let history = a.engine.entries_for_peer(Hlc::ZERO, usize::MAX).unwrap();
    let holed: Vec<kimmy_core::OplogEntry> = history
        .entries
        .iter()
        .filter(|e| !(e.stamp.node == a_id && e.stamp.hlc == s))
        .cloned()
        .collect();
    assert_eq!(holed.len() + 1, history.entries.len());
    r.engine
        .apply_peer_batch(&a.engine.version_vector().unwrap(), &holed, history.scanned_to, true)
        .unwrap();
    let mut progress = kimmy_storage::SnapshotProgress::of_collection(ca.id);
    while !progress.is_complete() {
        let page = a.engine.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        r.engine.apply_snapshot_page(a_id, &mut progress, &page).unwrap();
    }

    let cr = r.engine.get_collection("shop", "orders").unwrap();
    assert!(r.engine.get(&cr, &DocId::String("s".into())).unwrap().is_some(), "repaired");
    assert!(r.engine.version_vector().unwrap().get(a_id) < s, "held: R does not advertise S");
    assert!(
        r.engine.witnessed_vector().unwrap().covers(&a.engine.version_vector().unwrap()),
        "and R is behind A on nothing"
    );
    (a, r, ca, s)
}

#[tokio::test]
async fn a_held_entry_below_the_members_position_is_released_by_its_next_pull() {
    // ADR-171's residual, closed. R's witnessed vector covers S, so a window
    // passes over it, and R is behind on nothing, so before ADR-172 no round
    // would ever ask. R names its span and the next pull releases it, without
    // a snapshot.
    let (a, r, _ca, s) = a_member_holding_a_repaired_entry_below_its_position().await;

    let (outcome, logged) = logged_during(sync_once(&r.engine, a.addr, SECRET, None)).await;
    let outcome = outcome.unwrap();
    assert!(logged.iter().any(|l| l.contains(ASKING_MARKED)), "R names the span: {logged:?}");
    assert!(!logged.iter().any(|l| l.contains(SNAPSHOT_FALLBACK)), "and no snapshot: {logged:?}");
    assert!(outcome.exhausted && !outcome.truncated, "one pull: {outcome:?}");
    assert_eq!(outcome.superseded, 1, "S alone is served: {outcome:?}");
    assert_eq!(
        r.engine.version_vector().unwrap().get(a.engine.node_id()),
        s,
        "released: R advertises S"
    );

    let (again, logged) = logged_during(sync_once(&r.engine, a.addr, SECRET, None)).await;
    assert!(!logged.iter().any(|l| l.contains(ASKING_MARKED)), "nothing left: {logged:?}");
    assert_eq!(again.unwrap().total(), 0);
}

/// Three members for ADR-172's spans. O writes `writes` documents `d0..`. A
/// takes them with the documents in `a_holes` missing from its window, and R
/// with those in `r_holes` missing, and both windows' exhaustion covers the
/// holes anyway. R then repairs from O by a scoped snapshot, which brings its
/// holes under `Hold`: marks at or below R's position.
async fn holed_members(writes: usize, a_holes: &[usize], r_holes: &[usize]) -> (Node, Node, Node) {
    let o = node().await;
    let a = node().await;
    let r = node().await;
    let co = o.engine.create_collection("shop", "orders").unwrap();
    o.engine
        .insert_many(&co, (0..writes).map(|i| doc! { "_id": format!("d{i}") }).collect())
        .unwrap();
    let history = o.engine.entries_for_peer(Hlc::ZERO, usize::MAX).unwrap();
    let theirs = o.engine.version_vector().unwrap();
    for (member, holes) in [(&a, a_holes), (&r, r_holes)] {
        let missing: BTreeSet<DocId> =
            holes.iter().map(|i| DocId::String(format!("d{i}"))).collect();
        let window: Vec<kimmy_core::OplogEntry> = history
            .entries
            .iter()
            .filter(|e| e.doc_id.as_ref().is_none_or(|id| !missing.contains(id)))
            .cloned()
            .collect();
        assert_eq!(window.len() + holes.len(), history.entries.len());
        member.engine.apply_peer_batch(&theirs, &window, history.scanned_to, true).unwrap();
    }
    let mut progress = kimmy_storage::SnapshotProgress::of_collection(co.id);
    while !progress.is_complete() {
        let page = o.engine.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        r.engine.apply_snapshot_page(o.engine.node_id(), &mut progress, &page).unwrap();
    }
    let spans = r.engine.held_ranges_covered_by(&r.engine.witnessed_vector().unwrap()).unwrap();
    assert_eq!(spans.len(), 1, "R holds one origin's span: {spans:?}");
    (o, a, r)
}

#[tokio::test]
async fn a_span_whose_bottom_the_peer_cannot_serve_does_not_stop_replication_from_it() {
    // The second way a span's bottom is not served, the ADR-148 shape: A
    // witnessed past d10 without applying it, and R was repaired from O.
    // Found by review of ADR-172's first form. R's span runs from d10 to d2900.
    // A lacks d10, the same hole, but holds 2,890 of the span's entries. The
    // first form asked from the span's bottom on every pull and served the
    // same 1,024 superseded entries every time, truncated, so nothing above
    // the span ever arrived from A. Each span now resumes past what A served.
    let (o, a, r) = holed_members(3_000, &[10], &[10, 2_900]).await;
    let co = o.engine.get_collection("shop", "orders").unwrap();
    o.engine.insert(&co, doc! { "_id": "late" }).unwrap();
    sync_once(&a.engine, o.addr, SECRET, None).await.unwrap();
    let ca = a.engine.get_collection("shop", "orders").unwrap();
    assert!(a.engine.get(&ca, &DocId::String("late".into())).unwrap().is_some());

    let mut stalls = PeerStalls::new();
    let mut pulls = Vec::new();
    loop {
        let outcome =
            sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.expect("pull");
        let truncated = outcome.truncated;
        pulls.push(outcome);
        if !truncated || pulls.len() >= 10 {
            break;
        }
    }
    let superseded: Vec<usize> = pulls.iter().map(|p| p.superseded).collect();
    eprintln!("span walk: {} pulls, superseded per pull {superseded:?}", pulls.len());
    let cr = r.engine.get_collection("shop", "orders").unwrap();
    assert!(
        r.engine.get(&cr, &DocId::String("late".into())).unwrap().is_some(),
        "the write above the span arrives from A: {pulls:?}"
    );
    assert_eq!(
        pulls.len(),
        3,
        "the 2,890 entries A holds inside the span, a window at a time: {superseded:?}"
    );
}

#[tokio::test]
async fn a_peer_moving_on_the_origin_in_the_middle_of_a_walk_does_not_restart_it() {
    // Found on review: re-asking from the bottom whenever the peer moved on the
    // span's origin reset the walk between two pulls of one contact. R's span
    // is d10 to d2900 and A lacks d10; O writes one document before each of R's
    // pulls and A takes it. Every pull then came back 1,024 superseded and
    // truncated, and `late` never arrived, for a span of 2,890 entries. The
    // resume point is not reset by the peer moving, so the walk finishes.
    const CEILING: usize = 12;
    let (o, a, r) = holed_members(3_000, &[10], &[10, 2_900]).await;
    let co = o.engine.get_collection("shop", "orders").unwrap();
    let cr = r.engine.get_collection("shop", "orders").unwrap();
    o.engine.insert(&co, doc! { "_id": "late" }).unwrap();
    sync_once(&a.engine, o.addr, SECRET, None).await.unwrap();

    let mut stalls = PeerStalls::new();
    let mut superseded = Vec::new();
    let mut arrived_after = None;
    for pull in 0..CEILING {
        o.engine.insert(&co, doc! { "_id": format!("w{pull}") }).unwrap();
        sync_once(&a.engine, o.addr, SECRET, None).await.unwrap();
        let outcome = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
        superseded.push(outcome.superseded);
        if r.engine.get(&cr, &DocId::String("late".into())).unwrap().is_some() {
            arrived_after = Some(pull + 1);
            break;
        }
    }
    let bound = 2_890usize.div_ceil(1024) + 1;
    assert!(
        arrived_after.is_some_and(|pulls| pulls <= bound),
        "late arrives within {bound} pulls while A moves on the origin: {superseded:?}"
    );
    assert!(
        !superseded.windows(2).any(|w| w[0] == 1024 && w[1] == 1024 && superseded.len() > bound),
        "no run of full superseded windows: {superseded:?}"
    );
}

#[tokio::test]
async fn a_mark_added_below_a_spans_resume_point_reopens_the_span_from_its_new_bottom() {
    // R's span on O runs from d100 to d2900, and A lacks d100. One pull walks
    // the span partway and the resume point moves past d1124. A second scoped
    // repair then brings e50, an earlier entry of the same origin in another
    // collection, under a mark below that resume point. No window from A has
    // walked e50, so the next round names the span from e50 and A releases it.
    let o = node().await;
    let a = node().await;
    let r = node().await;
    let ce = o.engine.create_collection("shop", "early").unwrap();
    o.engine.insert_many(&ce, (0..100).map(|i| doc! { "_id": format!("e{i}") }).collect()).unwrap();
    let cd = o.engine.create_collection("shop", "orders").unwrap();
    o.engine
        .insert_many(&cd, (0..3_000).map(|i| doc! { "_id": format!("d{i}") }).collect())
        .unwrap();
    let history = o.engine.entries_for_peer(Hlc::ZERO, usize::MAX).unwrap();
    let theirs = o.engine.version_vector().unwrap();
    let stamp_of = |id: &str| {
        history
            .entries
            .iter()
            .find(|e| e.doc_id == Some(DocId::String(id.into())))
            .unwrap()
            .stamp
            .hlc
    };
    let e50 = stamp_of("e50");
    assert!(e50 < stamp_of("d100"), "e50 must sort below the span's bottom");
    for (member, holes) in [(&a, vec!["d100"]), (&r, vec!["d100", "d2900", "e50"])] {
        let missing: BTreeSet<DocId> = holes.iter().map(|h| DocId::String((*h).into())).collect();
        let window: Vec<kimmy_core::OplogEntry> = history
            .entries
            .iter()
            .filter(|e| e.doc_id.as_ref().is_none_or(|id| !missing.contains(id)))
            .cloned()
            .collect();
        member.engine.apply_peer_batch(&theirs, &window, history.scanned_to, true).unwrap();
    }
    let repair = |collection: kimmy_core::CollectionId| {
        let mut progress = kimmy_storage::SnapshotProgress::of_collection(collection);
        while !progress.is_complete() {
            let page = o.engine.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
            r.engine.apply_snapshot_page(o.engine.node_id(), &mut progress, &page).unwrap();
        }
    };
    let lowest = |r: &Node| {
        r.engine.held_ranges_covered_by(&r.engine.witnessed_vector().unwrap()).unwrap()[0].from
    };
    repair(cd.id);
    assert_eq!(lowest(&r), stamp_of("d100"));

    let mut stalls = PeerStalls::new();
    let first = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
    assert!(first.truncated && first.superseded == 1024, "walked partway: {first:?}");

    repair(ce.id);
    assert_eq!(lowest(&r), e50, "e50 held as state, below the resume point");

    let mut pulls = Vec::new();
    loop {
        let outcome = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
        let truncated = outcome.truncated;
        pulls.push(outcome);
        if !truncated || pulls.len() >= 6 {
            break;
        }
    }
    assert_eq!(
        lowest(&r),
        stamp_of("d100"),
        "e50 was named from the span's new bottom and released; d100, which A lacks, stays: {pulls:?}"
    );
}

#[tokio::test]
async fn a_window_ending_on_a_stamp_the_spans_origin_shares_resumes_at_that_stamp() {
    // The tie at a window's end. L sorts before O, and L's entry and R's marked
    // O entry share one timestamp, H. The first window is truncated on L's
    // entry, so O's entry at H was not examined. The span must resume at H, not
    // past it, or that entry is never served and its mark waits for the expiry.
    let a = node().await;
    let r = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    sync(&a, &r).await;
    let l = kimmy_core::NodeId::from_bytes([1; 16]);
    let o = kimmy_core::NodeId::from_bytes([2; 16]);
    let base = kimmy_storage::physical_now_ms() + 1_000;
    let entry = |origin: kimmy_core::NodeId, wall: u64, id: String| kimmy_core::OplogEntry {
        stamp: kimmy_core::Stamp::new(Hlc::new(wall, 0), origin),
        kind: kimmy_core::OpKind::Insert,
        collection: ca.id,
        doc_id: Some(DocId::String(id.clone())),
        body: Some(bson::serialize_to_vec(&doc! { "_id": id }).unwrap()),
    };
    let h = base + 1_024;
    let mut history: Vec<kimmy_core::OplogEntry> =
        (1..=1_024u64).map(|i| entry(o, base + i, format!("o{i}"))).collect();
    history.push(entry(l, h, "l".into()));
    history.sort_by_key(|e| e.stamp);
    let mut theirs = kimmy_core::VersionVector::new();
    for e in &history {
        theirs.observe(e.stamp);
    }
    a.engine.apply_peer_batch(&theirs, &history, Hlc::new(h, 0), true).unwrap();

    let mut r_vector = kimmy_core::VersionVector::new();
    r_vector.insert(o, Hlc::new(h, 0));
    let holes = [DocId::String("o1".into()), DocId::String("o1024".into())];
    let r_window: Vec<kimmy_core::OplogEntry> = history
        .iter()
        .filter(|e| e.stamp.node == o && !e.doc_id.as_ref().is_some_and(|id| holes.contains(id)))
        .cloned()
        .collect();
    r.engine.apply_peer_batch(&r_vector, &r_window, Hlc::new(h, 0), true).unwrap();
    let mut progress = kimmy_storage::SnapshotProgress::of_collection(ca.id);
    while !progress.is_complete() {
        let page = a.engine.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        r.engine.apply_snapshot_page(a.engine.node_id(), &mut progress, &page).unwrap();
    }
    let covered =
        |r: &Node| r.engine.held_ranges_covered_by(&r.engine.witnessed_vector().unwrap()).unwrap();
    assert_eq!(
        covered(&r),
        vec![kimmy_storage::MarkedRange {
            origin: o,
            from: Hlc::new(base + 1, 0),
            through: Hlc::new(h, 0)
        }],
        "R holds o1 and o1024 as state below its position"
    );

    let mut stalls = PeerStalls::new();
    let first = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
    assert!(first.truncated, "the first window is cut on L's entry at H: {first:?}");
    for _ in 0..4 {
        let outcome = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
        if !outcome.truncated {
            break;
        }
    }
    assert!(
        covered(&r).is_empty(),
        "o1024, at the tied stamp, is served and released: {:?}",
        covered(&r)
    );
}

#[tokio::test]
async fn a_span_whose_lowest_entry_the_peer_collected_does_not_stop_replication_from_it() {
    // The first of the two ways a span's bottom is not served: the peer has
    // collected it. R holds x10 and x2900 of origin X as state; A collected
    // x0..x10 and holds the rest, then writes. The first form of ADR-172 asked
    // from x10 on every pull and was served the same 1,024 superseded entries,
    // truncated, until R's own retention collected the mark. The span now
    // resumes past what A served, the write arrives in three pulls, and a
    // later round names nothing: A provably lacks x10.
    let a = node().await;
    let r = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    sync(&a, &r).await;
    let x = kimmy_core::NodeId::generate();
    let now = kimmy_storage::physical_now_ms();
    let entry = |i: u64| {
        let wall = if i <= 10 { now - 2 * DAY_SECS * 1_000 + i } else { now + i };
        kimmy_core::OplogEntry {
            stamp: kimmy_core::Stamp::new(Hlc::new(wall, 0), x),
            kind: kimmy_core::OpKind::Insert,
            collection: ca.id,
            doc_id: Some(DocId::String(format!("x{i}"))),
            body: Some(bson::serialize_to_vec(&doc! { "_id": format!("x{i}") }).unwrap()),
        }
    };
    let history: Vec<kimmy_core::OplogEntry> = (0..3_000).map(entry).collect();
    let end = history.last().unwrap().stamp;
    let mut theirs = kimmy_core::VersionVector::new();
    theirs.observe(end);
    a.engine.apply_peer_batch(&theirs, &history, end.hlc, true).unwrap();
    let holes = [DocId::String("x10".into()), DocId::String("x2900".into())];
    let holed: Vec<kimmy_core::OplogEntry> = history
        .iter()
        .filter(|e| !e.doc_id.as_ref().is_some_and(|id| holes.contains(id)))
        .cloned()
        .collect();
    r.engine.apply_peer_batch(&theirs, &holed, end.hlc, true).unwrap();
    let mut progress = kimmy_storage::SnapshotProgress::of_collection(ca.id);
    while !progress.is_complete() {
        let page = a.engine.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        r.engine.apply_snapshot_page(a.engine.node_id(), &mut progress, &page).unwrap();
    }
    assert_eq!(
        r.engine.held_ranges_covered_by(&r.engine.witnessed_vector().unwrap()).unwrap(),
        vec![kimmy_storage::MarkedRange {
            origin: x,
            from: history[10].stamp.hlc,
            through: history[2_900].stamp.hlc
        }],
        "R holds x10 and x2900 as state"
    );

    a.engine
        .collect_garbage_at(now, kimmy_storage::RetentionPolicy::new(DAY_SECS, DAY_SECS))
        .unwrap();
    assert!(
        a.engine.oplog_collected_through().unwrap() >= history[10].stamp.hlc,
        "A collected x10"
    );
    a.engine.insert(&ca, doc! { "_id": "late" }).unwrap();

    let mut stalls = PeerStalls::new();
    let mut pulls = Vec::new();
    loop {
        let outcome =
            sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.expect("pull");
        let truncated = outcome.truncated;
        pulls.push(outcome);
        if !truncated || pulls.len() >= 10 {
            break;
        }
    }
    let superseded: Vec<usize> = pulls.iter().map(|p| p.superseded).collect();
    eprintln!(
        "collected-lowest span walk: {} pulls, superseded per pull {superseded:?}",
        pulls.len()
    );
    let cr = r.engine.get_collection("shop", "orders").unwrap();
    assert!(
        r.engine.get(&cr, &DocId::String("late".into())).unwrap().is_some(),
        "the later write arrives from A: {superseded:?}"
    );
    assert_eq!(pulls.len(), 3, "the 2,890 entries A holds inside the span, a window at a time");
    assert_eq!(
        r.engine.held_ranges_covered_by(&r.engine.witnessed_vector().unwrap()).unwrap().len(),
        1,
        "x2900 was released and x10 is still held"
    );

    let again = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
    assert_eq!(again.total(), 0, "A provably lacks x10, so the span is not named again: {again:?}");
}

#[tokio::test]
async fn a_peer_moving_on_the_origin_does_not_re_walk_an_answered_span() {
    // A answered R's span without d10 or d20, so the span is left out. A then
    // takes d10 below the resume point and moves on O's origin. Entries below
    // the resume point were walked and did not carry d10, and a move re-walking
    // from the bottom costs the whole span on every tick of a busy origin, so
    // the round does not name the span again: it pulls d30 alone. d10 waits for
    // the record's expiry (`MARKS_REASK_AFTER`), which the resume points' unit
    // test pins.
    let (o, a, r) = holed_members(30, &[10, 20], &[10, 20]).await;
    let mut stalls = PeerStalls::new();
    let first = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
    assert_eq!(first.superseded, 9, "{first:?}");
    let second = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
    assert_eq!(second.total(), 0, "{second:?}");

    let d10 = o
        .engine
        .entries_for_peer(Hlc::ZERO, usize::MAX)
        .unwrap()
        .entries
        .into_iter()
        .find(|e| e.doc_id == Some(DocId::String("d10".into())))
        .unwrap();
    a.engine.apply_batch(&[d10]).unwrap();
    let co = o.engine.get_collection("shop", "orders").unwrap();
    o.engine.insert(&co, doc! { "_id": "d30" }).unwrap();
    sync_once(&a.engine, o.addr, SECRET, None).await.unwrap();

    let third = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
    assert_eq!((third.applied, third.superseded), (1, 0), "d30 alone: {third:?}");
    let spans = r.engine.held_ranges_covered_by(&r.engine.witnessed_vector().unwrap()).unwrap();
    assert!(
        spans.len() == 1 && spans[0].from < spans[0].through,
        "d10 and d20 still held: {spans:?}"
    );
}

#[tokio::test]
async fn continuous_writes_on_a_span_origin_do_not_re_walk_the_span_every_tick() {
    // The livelock a re-ask from the bottom brings back under continuous
    // writes. R's span, d10 to d4990, is wider than the scaled-down ceiling
    // below: 4,979 entries A holds, five windows. A lacks both ends, so
    // nothing in the span is ever released and it stays that wide, and O keeps
    // writing. Once the span has been walked, each tick must bring R the new
    // writes in one pull, never re-walking the span.
    const CEILING: usize = 4; // a tick's pull ceiling, scaled down from MAX_PULLS_PER_CONTACT
    const PER_TICK: usize = 10;
    let (o, a, r) = holed_members(5_000, &[10, 4_990], &[10, 4_990]).await;
    let co = o.engine.get_collection("shop", "orders").unwrap();
    let cr = r.engine.get_collection("shop", "orders").unwrap();
    let mut stalls = PeerStalls::new();

    let mut walk = 0;
    loop {
        walk += 1;
        let outcome = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
        if !outcome.truncated {
            break;
        }
        assert!(walk < 20, "the first walk must end");
    }
    assert!(walk > CEILING, "the span must be wider than the ceiling: {walk} pulls");

    for tick in 0..3 {
        let docs = (0..PER_TICK).map(|i| doc! { "_id": format!("t{tick}-{i}") }).collect();
        o.engine.insert_many(&co, docs).unwrap();
        drain(&a, &o).await;

        let mut pulls = Vec::new();
        loop {
            let outcome =
                sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
            let truncated = outcome.truncated;
            pulls.push(outcome);
            if !truncated || pulls.len() >= CEILING {
                break;
            }
        }
        let superseded: Vec<usize> = pulls.iter().map(|p| p.superseded).collect();
        assert!(
            pulls.len() < CEILING && !pulls.last().unwrap().truncated,
            "tick {tick} hit the ceiling: {superseded:?}"
        );
        assert!(
            pulls.len() <= PER_TICK.div_ceil(1024) + 1,
            "tick {tick}: {} pulls for {PER_TICK} new entries: {superseded:?}",
            pulls.len()
        );
        for i in 0..PER_TICK {
            let id = DocId::String(format!("t{tick}-{i}"));
            assert!(r.engine.get(&cr, &id).unwrap().is_some(), "tick {tick}'s writes arrive");
        }
    }
}

#[tokio::test]
async fn a_span_the_peer_answered_is_not_named_again_on_the_next_round() {
    // The resume points' wiring through the round. R and A share the holes at
    // d10 and d20, so A serves the nine entries between and can release
    // neither mark. With one `PeerStalls` across two rounds, the second names
    // nothing and pulls nothing.
    let (_o, a, r) = holed_members(30, &[10, 20], &[10, 20]).await;
    let mut stalls = PeerStalls::new();

    let (first, logged) =
        logged_during(sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls)).await;
    let first = first.unwrap();
    assert!(logged.iter().any(|l| l.contains(ASKING_MARKED)), "R names the span: {logged:?}");
    assert_eq!(first.superseded, 9, "A serves the span's entries it holds, d11..d19: {first:?}");

    let second = sync_once_with(&r.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
    assert_eq!(second.total(), 0, "a span answered to the tail is not named again: {second:?}");
}

#[tokio::test]
async fn a_requester_naming_spans_asks_a_sender_that_ignores_them_for_no_more_than_before() {
    // A sender that predates ADR-172 ignores `marked`, and one that predates
    // ADR-171 does not skip either. Neither can be run here, so each is what
    // it serves a request with: `entries_for_peer` for the release before
    // ADR-171, `entries_for_peer_holding` for ADR-171's. What they are asked
    // is what this build asks.
    let (o, a, r) = holed_members(30, &[10, 20], &[10, 20]).await;
    let held = r.engine.witnessed_vector().unwrap();
    let theirs = a.engine.version_vector().unwrap();
    let spans = r.engine.held_ranges_covered_by(&held).unwrap();
    assert!(held.behind(&theirs).is_none(), "R is behind A on nothing");

    // Behind on nothing, a requester before ADR-172 sends no request. This one
    // asks from A's newest stamp, never from the span's bottom.
    let from = entries_threshold(&held, &theirs, &spans).expect("a member holding spans asks");
    let before_171 = a.engine.entries_for_peer(from, 1024).unwrap();
    assert!(
        before_171.exhausted && before_171.entries.len() <= 1,
        "served from A's newest stamp, not the span's bottom: {:?}",
        before_171.entries
    );
    let adr_171 = a.engine.entries_for_peer_holding(from, 1024, Some(&held)).unwrap();
    assert!(adr_171.exhausted && adr_171.entries.is_empty(), "{:?}", adr_171.entries);

    // Behind, the request is the one a requester before ADR-172 sends.
    let co = o.engine.get_collection("shop", "orders").unwrap();
    o.engine.insert_many(&co, (0..100).map(|i| doc! { "_id": format!("e{i}") }).collect()).unwrap();
    sync_once(&a.engine, o.addr, SECRET, None).await.unwrap();
    let theirs = a.engine.version_vector().unwrap();
    assert!(held.behind(&theirs).is_some());
    assert_eq!(entries_threshold(&held, &theirs, &spans), held.behind(&theirs));
}

#[tokio::test]
async fn a_span_the_peer_has_collected_is_served_what_remains_and_never_a_snapshot() {
    // The horizon is judged on `held`, never on a span (ADR-172). A has
    // collected S; a span below A's horizon must not make the pull a
    // snapshot. R takes what A still has, and S's mark waits for R's own
    // retention.
    let (a, r, ca, s) = a_member_holding_a_repaired_entry_below_its_position().await;
    let later = kimmy_storage::physical_now_ms() + 36 * HOUR_MS;
    a.engine.apply_batch(&[entry_stamped(ca.id, later)]).unwrap();
    a.engine
        .collect_garbage_at(
            later + HOUR_MS,
            kimmy_storage::RetentionPolicy::new(DAY_SECS, DAY_SECS),
        )
        .unwrap();
    assert!(a.engine.oplog_collected_through().unwrap() >= s, "A collected S");

    let (outcome, logged) = logged_during(sync_once(&r.engine, a.addr, SECRET, None)).await;
    let outcome = outcome.unwrap();
    assert!(logged.iter().any(|l| l.contains(ASKING_MARKED)), "R names the span: {logged:?}");
    assert!(
        !logged.iter().any(|l| l.contains(SNAPSHOT_FALLBACK)),
        "a span below the peer's horizon is never a snapshot: {logged:?}"
    );
    assert_eq!(outcome.applied, 1, "R takes the later write: {outcome:?}");
    assert!(
        r.engine.version_vector().unwrap().get(a.engine.node_id()) < s,
        "S is gone from A, so R's mark stays for R's own retention"
    );
}

#[tokio::test]
async fn a_peer_that_missed_collected_history_is_still_named_and_still_snapshots() {
    // The control for both. B holds A's first write but never received the
    // second, and A has since collected it. B lacks something gone: A names
    // it on the first round that sees it, and serves it a snapshot rather
    // than a silent gap.
    let a = node().await;
    let b = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "a-1" }).unwrap();
    sync(&a, &b).await;
    a.engine.insert(&ca, doc! { "_id": "a-missed" }).unwrap();
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    b.engine.insert(&cb, doc! { "_id": "b-1" }).unwrap();
    b.engine.insert(&cb, doc! { "_id": "b-2" }).unwrap();
    sync_once(&a.engine, b.addr, SECRET, None).await.unwrap();

    let later = kimmy_storage::physical_now_ms() + 36 * HOUR_MS;
    a.engine.apply_batch(&[entry_stamped(ca.id, later)]).unwrap();
    a.engine
        .collect_garbage_at(
            later + HOUR_MS,
            kimmy_storage::RetentionPolicy::new(DAY_SECS, DAY_SECS),
        )
        .unwrap();
    a.engine.insert(&ca, doc! { "_id": "a-after" }).unwrap();

    let outcome = sync_once(&a.engine, b.addr, SECRET, None).await.unwrap();
    assert!(
        outcome.behind_ms > DAY_SECS * 1_000,
        "B lacks a collected write of A's, 36 hours behind: it is stale: {outcome:?}"
    );

    let (pulled, logged) = logged_during(sync_once(&b.engine, a.addr, SECRET, None)).await;
    let pulled = pulled.unwrap();
    assert!(
        logged.iter().any(|line| line.contains(SNAPSHOT_FALLBACK)),
        "served as a snapshot, and the recorder sees the line that says so: {logged:?}"
    );
    assert_eq!(pulled.superseded, 0, "served as a snapshot, not from the oplog: {pulled:?}");
    for id in ["a-missed", "a-after"] {
        assert!(
            b.engine.get(&cb, &DocId::String(id.into())).unwrap().is_some(),
            "{id} must arrive"
        );
    }
    let second = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(second.total(), 0, "{second:?}");
}

// -----------------------------------------------------------------------
// The cross-member divergence check (ADR-133)
// -----------------------------------------------------------------------

/// Reproduce the outside-visible shape of finding 14 directly, without
/// depending on its now-fixed cause: a witnessed vector that claims to cover
/// a peer's advertised one while a collection that peer holds is simply
/// missing here.
///
/// This checks the mechanism at `sync_once`'s level — that `outcome.divergent`
/// actually names the stranded collection. It is deliberately *not* offered
/// as proof that every other signal stays quiet: on this code path
/// `SyncOutcome`'s other fields (`lag_ms`, `applied`, `superseded`, `ddl`,
/// `ddl_refused`, `unknown_collection`) are `Default::default()` by
/// construction, whether or not anything is wrong, so asserting them here
/// would pass for any input and prove nothing. The real proof that the rest
/// of the signal surface stays healthy while this one moves is
/// `the_replication_loop_reports_a_stranded_collection_while_every_other_signal_stays_healthy`
/// below, which drives the actual hooks `kimmy-api`'s metrics are pushed
/// through.
#[tokio::test]
async fn the_divergence_check_finds_a_collection_the_witness_wrongly_claims_to_cover() {
    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "stranded").unwrap();
    a.engine.insert(&ca, doc! { "_id": "1" }).unwrap();

    // B is made to believe it has witnessed everything A has advertised,
    // without ever applying the entries that created the collection or its
    // document — exactly what a truncated window's absorbed vector looked
    // like from outside.
    let theirs = a.engine.version_vector().unwrap();
    b.engine.apply_peer_batch(&theirs, &[], Hlc::ZERO, true).unwrap();
    assert!(b.engine.witnessed_vector().unwrap().behind(&theirs).is_none(), "the false belief");
    assert!(b.engine.get_collection("shop", "stranded").is_err(), "and yet B does not have it");

    let probe = Some(DivergenceProbe { id: ca.id, mine_count: None, mine_at: None });
    let outcome = sync_once(&b.engine, a.addr, SECRET, probe).await.unwrap();

    assert_eq!(
        outcome.divergent,
        Some(BTreeSet::from([ca.id])),
        "the check must catch what the witnessed vector hides: {outcome:?}"
    );
}

/// A genuinely converged pair reports nothing divergent — the gauge's
/// resting state. `mine_count` comes from a real `count_by_id`, the way
/// `kimmy-cluster::peers` actually builds a probe, not a hardcoded value —
/// hardcoding it here would exercise the existence half only and never the
/// count comparison against genuine engine state.
#[tokio::test]
async fn a_converged_cluster_has_no_divergence() {
    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "from-a" }).unwrap();
    sync(&a, &b).await;
    sync(&a, &b).await; // both directions witness the other's tail

    let mine_count = b.engine.count_by_id(ca.id).unwrap();
    let probe = Some(DivergenceProbe { id: ca.id, mine_count, mine_at: None });
    let outcome = sync_once(&b.engine, a.addr, SECRET, probe).await.unwrap();
    assert_eq!(outcome.divergent, Some(BTreeSet::new()), "{outcome:?}");
}

/// A round whose pull does not reach the peer's true tail — the backlog
/// exceeds the batch cap — must not spend a message on the check at all:
/// `exhausted` is exactly the fact that a truncated window can fake, so the
/// check must not run on the strength of a batch that was itself truncated.
#[tokio::test]
async fn a_round_that_does_not_reach_the_peers_tail_skips_the_check_entirely() {
    use kimmy_cluster::protocol::MAX_BATCH;

    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "orders").unwrap();
    // Comfortably over the batch cap, so the first pull is truncated and
    // `exhausted` reads `false`.
    for i in 0..(MAX_BATCH + 200) {
        a.engine.insert(&ca, doc! { "_id": format!("d{i}") }).unwrap();
    }

    let probe = Some(DivergenceProbe { id: ca.id, mine_count: None, mine_at: None });
    let first = sync_once(&b.engine, a.addr, SECRET, probe).await.unwrap();
    assert!(first.applied > 0, "a genuine catch-up round: {first:?}");
    assert!(!first.exhausted, "the batch cap truncated this round's window: {first:?}");
    assert_eq!(first.divergent, None, "not checked, not found clean: {first:?}");
}

/// A round whose pull *does* reach the peer's true tail — the whole backlog
/// fits under the batch cap — runs the check too, on the same round, and it
/// correctly finds nothing wrong. This is what closes the gap a
/// nothing-to-pull-only gate leaves open on a busy cluster: a round that is
/// still pulling something is not automatically exempt, only a round whose
/// pull was itself truncated is.
#[tokio::test]
async fn a_round_that_reaches_the_peers_tail_runs_the_check_and_finds_nothing_wrong() {
    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "orders").unwrap();
    for i in 0..40 {
        a.engine.insert(&ca, doc! { "_id": format!("d{i}") }).unwrap();
    }

    let probe = Some(DivergenceProbe { id: ca.id, mine_count: None, mine_at: None });
    let first = sync_once(&b.engine, a.addr, SECRET, probe).await.unwrap();
    assert!(first.applied > 0, "a genuine catch-up round: {first:?}");
    assert!(first.exhausted, "40 entries fit comfortably under the batch cap: {first:?}");
    assert_eq!(first.divergent, Some(BTreeSet::new()), "checked, and correctly clean: {first:?}");
}

/// The probed collection's count is the only document read this exchange
/// performs — the bound `Engine::count_by_id` and `next_probe` exist to
/// keep. A second, unprobed collection with a *real* count mismatch costs
/// nothing this round: its existence is compared by id, never by walking
/// it, and its count is simply not asked for.
///
/// The mismatch on "big" is manufactured with `apply_peer_batch` rather than
/// a local write on B, deliberately: a local write on B would itself trip
/// the peer-staleness guard (`divergence_probe_for`) and suppress the probe
/// for an unrelated reason, which would not test the bound this case exists
/// to pin.
#[tokio::test]
async fn only_the_probed_collection_is_ever_counted() {
    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "small").unwrap();
    a.engine.insert(&ca, doc! { "_id": "1" }).unwrap();
    let big = a.engine.create_collection("shop", "big").unwrap();
    for i in 0..500 {
        a.engine.insert(&big, doc! { "_id": format!("d{i}") }).unwrap();
    }
    sync(&a, &b).await;
    sync(&a, &b).await;

    // A's "big" grows further; B is made to believe it has already
    // witnessed the growth without ever applying it — a real, genuine count
    // mismatch B does not know about, on a collection neither side is
    // probing this round.
    a.engine.insert(&big, doc! { "_id": "extra-on-a" }).unwrap();
    let theirs = a.engine.version_vector().unwrap();
    b.engine.apply_peer_batch(&theirs, &[], Hlc::ZERO, true).unwrap();

    let mine_count = b.engine.count_by_id(ca.id).unwrap();
    let probe = Some(DivergenceProbe { id: ca.id, mine_count, mine_at: None });
    let outcome = sync_once(&b.engine, a.addr, SECRET, probe).await.unwrap();
    assert_eq!(
        outcome.divergent,
        Some(BTreeSet::new()),
        "the unprobed collection's real count divergence is out of scope this round: {outcome:?}"
    );
}

/// A peer that has simply not pulled this node's own recent writes yet must
/// not be flagged divergent on their strength — that is ordinary
/// replication lag, indistinguishable from real divergence by a symmetric
/// count comparison alone, which is exactly why `divergence_probe_for` is
/// asymmetric. A is the requester and is *ahead* of B on A's own writes; B
/// has not pulled them, so B's answer would report a stale, lower count for
/// the same collection if asked.
#[tokio::test]
async fn a_peer_that_has_not_pulled_this_nodes_own_writes_is_not_flagged_divergent() {
    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "0" }).unwrap();
    sync(&a, &b).await;
    sync(&a, &b).await; // converged: both hold one document

    // A writes more; B never pulls. From A's side there is nothing new to
    // pull *from B*, so A's own gate reads "nothing to pull" — but B is the
    // one behind here, not A.
    for i in 1..=500 {
        a.engine.insert(&ca, doc! { "_id": i.to_string() }).unwrap();
    }

    let mine_count = a.engine.count_by_id(ca.id).unwrap();
    let probe = Some(DivergenceProbe { id: ca.id, mine_count, mine_at: None });
    let outcome = sync_once(&a.engine, b.addr, SECRET, probe).await.unwrap();
    assert_eq!(
        outcome.divergent,
        Some(BTreeSet::new()),
        "B merely trails A; the guard must suppress the stale count, not report it: {outcome:?}"
    );
}

/// The real proof that the silence claim holds: run the actual replication
/// loop, with the actual hooks `kimmyd` wires straight into `kimmy-api`'s
/// metrics, and watch every one of them while a stranded collection sits
/// undetected on the level below `sync_once`'s reported fields (which the
/// mechanism-level test above cannot use as evidence — see its own
/// comment). `on_round` is what `kimmy_sync_failures_total`,
/// `kimmy_sync_peers_backing_off`, `kimmy_sync_ddl_refused_total` and
/// `kimmy_sync_divergent_collections` are pushed from; `on_lag` is what
/// `kimmy_replication_lag_seconds` is pushed from.
#[tokio::test]
async fn the_replication_loop_reports_a_stranded_collection_while_every_other_signal_stays_healthy()
{
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "stranded").unwrap();
    a.engine.insert(&ca, doc! { "_id": "1" }).unwrap();

    let theirs = a.engine.version_vector().unwrap();
    b.engine.apply_peer_batch(&theirs, &[], Hlc::ZERO, true).unwrap();
    assert!(b.engine.get_collection("shop", "stranded").is_err());

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let (lag_tx, mut lag_rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![a.addr])], SECRET.into(), b.addr);
    config.sync_interval = Duration::from_millis(100);
    config.discovery_interval = Duration::from_millis(100);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    config.on_lag = Some(Arc::new(move |lag| {
        let _ = lag_tx.send(lag);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&b.engine), config));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut confirmed = 0usize;
    while confirmed == 0 {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the gauge-feeding report never confirmed the divergence"))
            .expect("the loop must keep reporting");
        assert_eq!(report.failed, 0, "nothing about this failed a round: {report:?}");
        assert_eq!(report.backing_off, 0, "the peer answered every round: {report:?}");
        assert_eq!(report.ddl_refused, 0, "nothing was refused: {report:?}");
        confirmed = report.divergent_collections;
    }
    assert_eq!(confirmed, 1, "exactly the one stranded collection");
    looping.abort();

    // Drain what `on_lag` saw across the whole run: it must never have
    // reported anything but 0 — the exact reading finding 14 left on every
    // member throughout.
    let mut lags = Vec::new();
    while let Ok(lag) = lag_rx.try_recv() {
        lags.push(lag);
    }
    assert!(!lags.is_empty(), "on_lag must have fired at least once");
    assert!(lags.iter().all(|&l| l == 0), "lag must read 0 throughout: {lags:?}");
}

/// Finding 14's own precondition was a member more than one batch behind —
/// the shape of a modest, continuously busy cluster, not an idle one. A gate
/// on "nothing left to pull" alone would never fire here, because there is
/// always something new to pull. The check must also run whenever a round's
/// own pull reaches the peer's tail, batch cap or not, so detection does not
/// go dark for the whole duration of ordinary write traffic.
#[tokio::test]
async fn the_check_still_runs_while_a_round_keeps_finding_new_entries_to_pull() {
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    // How busy the cluster is, and how it is busy. Three documents rewritten
    // between rounds is a handful of entries per round, well under the batch
    // cap, so a round pulling them still reaches the peer's tail.
    //
    // They are *rewritten*, never added to. That is what lets the assertion
    // below name a collection the report only ever counts: `busy` holds
    // exactly `BUSY_DOCS` live documents on A from before the hole is
    // induced until the end of the run, so the count half can never report
    // it, and the one collection this check can ever confirm is `stranded`.
    // Growing the collection instead — as this test used to — makes B
    // legitimately behind on `busy`'s count at every probe: the loop reads
    // B's own count once per tick, before the round pulls anything
    // (`peers.rs`), while A answers the probe with a live count a round trip
    // later, and nothing defers that probe, because the count half's gate
    // drops one only for a peer that trails *this* node and is still
    // advancing (ADR-145, ADR-146) — which A, the node B pulls from, never
    // does. `busy` then confirms beside `stranded`, correctly, and the gauge
    // reads 2 or 1 depending on nothing but how the writer's bursts fall
    // against the rounds.
    const BUSY_DOCS: usize = 3;

    let a = node().await;
    let b = node().await;

    // B converges on `busy` the ordinary way, before the hole exists. That
    // is not tidiness: it is what keeps the *divergence check* the only
    // thing that can find the stranded collection. A round whose batch stops
    // at an entry for a collection this node does not hold plans a snapshot
    // from that peer on the strength of the stop alone (ADR-148), and that
    // snapshot brings every collection — including `stranded`. Leaving B
    // without `busy` at the start makes the writer's very first rewrite such
    // an entry, so the hole is closed by the repair path before the check
    // has reported anything, and this test passes with the check switched
    // off entirely. With `busy` already here, every entry B ever pulls is
    // for a collection B holds, and nothing but the check can name
    // `stranded`.
    let busy = a.engine.create_collection("shop", "busy").unwrap();
    for i in 0..BUSY_DOCS {
        a.engine.insert(&busy, doc! { "_id": format!("h{i}"), "seq": 0i64 }).unwrap();
    }
    sync(&a, &b).await;
    let busy_here = b.engine.get_collection("shop", "busy").expect("B holds the busy collection");
    assert_eq!(b.engine.count(&busy_here).unwrap(), BUSY_DOCS as u64, "and all of its documents");

    // Only now the collection that gets stranded, and the induced hole: B's
    // witness claims to cover the creation it never applied.
    let stranded = a.engine.create_collection("shop", "stranded").unwrap();
    a.engine.insert(&stranded, doc! { "_id": "1" }).unwrap();

    // One more `busy` entry on top, and the reason is the same one as
    // above. `VersionVector::behind` is an inclusive threshold and
    // `entries_for_peer` serves the window *at or after* it, so the newest
    // entry the hole covers is re-served on B's very first pull. If that
    // entry is one of `stranded`'s, B stops at a collection it does not
    // hold and takes the ADR-148 snapshot instead of waiting for the check.
    // Whatever sits at the top of the covered window must therefore belong
    // to a collection B already holds — the assertion in the loop below
    // guards this, and will fail loudly if these lines are ever reordered.
    a.engine
        .replace(&busy, &DocId::String("h0".into()), doc! { "_id": "h0", "seq": 0i64 }, true)
        .unwrap();

    let theirs0 = a.engine.version_vector().unwrap();
    b.engine.apply_peer_batch(&theirs0, &[], Hlc::ZERO, true).unwrap();
    assert!(b.engine.get_collection("shop", "stranded").is_err());

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![a.addr])], SECRET.into(), b.addr);
    config.sync_interval = Duration::from_millis(100);
    config.discovery_interval = Duration::from_millis(100);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&b.engine), config));

    // The writer runs until it is told to stop rather than a fixed number of
    // times, so that on a slow or contended machine this cannot quietly
    // become a test of an idle cluster — the one shape it exists to rule
    // out. It yields between writes because `#[tokio::test]` is a
    // *current-thread* runtime: both nodes' listeners, B's replication loop
    // and this writer share one thread, and an unbroken burst of writes is a
    // blocking section on it. Each write costs milliseconds, so a burst that
    // does not yield starves the very loop this test is waiting on, and on a
    // two-core runner it starves it for longer than the burst interval.
    let writing = Arc::new(AtomicBool::new(true));
    let a_engine = Arc::clone(&a.engine);
    let keep_writing = Arc::clone(&writing);
    let writer = tokio::spawn(async move {
        let mut seq = 0i64;
        while keep_writing.load(Ordering::Relaxed) {
            seq += 1;
            for i in 0..BUSY_DOCS {
                let id = DocId::String(format!("h{i}"));
                let doc = doc! { "_id": format!("h{i}"), "seq": seq };
                a_engine.replace(&busy, &id, doc, true).unwrap();
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
    });

    // How far B has followed the rewrites. Every rise is a round that found
    // new entries and pulled them.
    let seq_on_b = || {
        b.engine
            .get(&busy_here, &DocId::String("h0".into()))
            .unwrap()
            .and_then(|d| d.get_i64("seq").ok())
            .unwrap_or(-1)
    };

    // Two conditions, one wait: the check confirms the stranded collection,
    // and rounds go on finding new entries to pull after it has. The second
    // is what keeps this a test of a busy cluster rather than of an idle
    // one; the doc comment above says why an idle cluster would exercise
    // none of it.
    //
    // The budget is not sized from what this needs when nothing goes wrong —
    // three checked contacts to confirm and one more for the rewrites to
    // reach B, four ticks, some 400 ms. It is sized so that a round which
    // hangs and burns `REQUEST_TIMEOUT` (30 s, `transport.rs`), plus the
    // peer backoff behind it, still leaves room for those contacts. On a
    // two-core runner with the rest of the suite in parallel that is the
    // shape this deadline must not mistake for a check that never ran.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut confirmed = 0usize;
    let mut at_confirmation = -1i64;
    let (mut checked, mut skipped, mut failed) = (0usize, 0usize, 0usize);
    while confirmed == 0 || seq_on_b() <= at_confirmation {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the check did not confirm and keep running under sustained write load: \
                     confirmed={confirmed} checks={checked} skips={skipped} failed={failed} \
                     seq on B={} (at confirmation {at_confirmation})",
                    seq_on_b()
                )
            })
            .expect("the loop must keep reporting");
        // The check must be the only thing that can find `stranded`. A
        // batch that stops at a collection this node lacks plans a snapshot
        // from the peer on the strength of the stop alone (ADR-148), and
        // that snapshot closes the hole — leaving this test green with the
        // check switched off entirely. See the setup above for what keeps
        // it from happening.
        assert_eq!(
            report.entries_skipped_unknown_collection, 0,
            "no batch may stop at the collection B lacks, or the repair path closes the hole \
             before the check reports it: {report:?}"
        );
        checked += report.divergence_checks;
        skipped += report.divergence_skips;
        failed += report.failed;
        // Latched at the first non-zero reading: the confirmed finding is a
        // level, and the repair a confirmation plans against the peer
        // (ADR-148) may close it again while this loop is still running.
        if confirmed == 0 && report.divergent_collections > 0 {
            confirmed = report.divergent_collections;
            at_confirmation = seq_on_b();
        }
    }
    assert_eq!(confirmed, 1, "the stranded collection, and only it, despite the concurrent writes");
    assert!(checked > 0, "confirmed from rounds that ran the check: checks={checked}");

    writing.store(false, Ordering::Relaxed);
    looping.abort();
    writer.abort();
}

/// P6's own reproduction, at the network level: finding 14's more serious
/// half was 500 and 517 documents missing from collections that existed,
/// correctly named, on every member — the case a names-only check would
/// have missed entirely, and the stated reason the count half of this check
/// exists at all. It must confirm through the real `replicate()` loop even
/// when this node holds several collections, so the probe rotation
/// naturally cycles away from the divergent one between contacts — the
/// exact shape that left the gauge structurally unable to move before this
/// was fixed (see `DivergenceTracker::observe`'s own documentation).
#[tokio::test]
async fn a_count_divergence_confirms_through_the_real_loop_despite_other_collections() {
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let a = node().await;
    let b = node().await;

    let mut collections = Vec::new();
    for name in ["alpha", "beta", "gamma", "delta"] {
        let c = a.engine.create_collection("shop", name).unwrap();
        a.engine.insert(&c, doc! { "_id": "0" }).unwrap();
        collections.push(c);
    }
    sync(&a, &b).await;
    sync(&a, &b).await; // converged: every collection holds one document on both

    // "gamma" grows further on A; B is made to believe it has already
    // witnessed the growth without ever applying it -- a real count
    // divergence B does not know about, on a collection whose name and
    // existence agree everywhere.
    let gamma = &collections[2];
    for i in 1..=20 {
        a.engine.insert(gamma, doc! { "_id": i.to_string() }).unwrap();
    }
    let theirs = a.engine.version_vector().unwrap();
    b.engine.apply_peer_batch(&theirs, &[], Hlc::ZERO, true).unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![a.addr])], SECRET.into(), b.addr);
    config.sync_interval = Duration::from_millis(50);
    config.discovery_interval = Duration::from_millis(50);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&b.engine), config));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut confirmed = 0usize;
    while confirmed == 0 {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("the count divergence never confirmed through the real loop")
            })
            .expect("the loop must keep reporting");
        confirmed = report.divergent_collections;
    }
    assert_eq!(confirmed, 1, "gamma's count divergence, despite alpha/beta/delta rotating through");
    looping.abort();
}

/// **The positive control.** Until a test drives
/// `kimmy_sync_divergent_collections` off `0` and back to `0`, a dead gauge
/// and a working one are indistinguishable, and no cluster round may cite a
/// `0` reading as evidence of anything: the reading a broken gauge produces
/// is the reading a healthy cluster produces.
///
/// Both directions matter, and each fails for a different reason. Off `0`
/// catches a gauge that never moves — a tracker that never confirms, a
/// finding never folded in, a hook never called. Back to `0` catches a gauge
/// that sticks, which is worse than one that never fires: an alert an
/// operator cannot clear by fixing the thing it reported is the one they
/// disable, which lands the cluster in exactly the state this gauge exists
/// to prevent.
///
/// The confirmation gate is asserted at the moment the gauge moves rather
/// than separately: `divergence_checks` counts contacts in which the check
/// actually ran, so requiring at least two of them before the gauge leaves
/// `0` is the existence half's "two consecutive contacts with the same peer"
/// rule, measured through the real loop (ADR-133, ADR-135).
#[tokio::test]
async fn the_divergence_gauge_leaves_zero_and_returns_to_zero_through_the_real_loop() {
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let a = node().await;
    let b = node().await;

    // A holds a collection B does not, and B believes it has already
    // witnessed everything A holds — so nothing about this fails a round,
    // no counter moves, and the lag gauge reads 0. The state the check
    // exists for.
    let ca = a.engine.create_collection("shop", "stranded").unwrap();
    a.engine.insert(&ca, doc! { "_id": "1" }).unwrap();
    let theirs = a.engine.version_vector().unwrap();
    b.engine.apply_peer_batch(&theirs, &[], Hlc::ZERO, true).unwrap();
    assert!(b.engine.get_collection("shop", "stranded").is_err());

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![a.addr])], SECRET.into(), b.addr);
    config.sync_interval = Duration::from_millis(100);
    config.discovery_interval = Duration::from_millis(100);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&b.engine), config));

    // Off 0.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut checks = 0usize;
    let mut skips = 0usize;
    let mut confirmed = 0usize;
    while confirmed == 0 {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the gauge never left 0: a dead gauge reads exactly this"))
            .expect("the loop must keep reporting");
        assert_eq!(report.failed, 0, "nothing about this failed a round: {report:?}");
        checks += report.divergence_checks;
        skips += report.divergence_skips;
        confirmed = report.divergent_collections;
        if confirmed > 0 {
            assert!(
                checks >= 2,
                "the existence half confirms across two consecutive contacts, never one: \
                 the gauge moved after {checks} checked contact(s)"
            );
        }
    }
    assert_eq!(confirmed, 1, "exactly the one stranded collection");
    assert_eq!(skips, 0, "nothing here has a backlog to truncate a round: {skips}");

    // Back to 0, on the repair `operations.md` tells an operator to make:
    // B now holds the collection it lacked, so the next contact with the
    // same peer no longer finds it and the level falls.
    b.engine.create_collection("shop", "stranded").unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut cleared = confirmed;
    while cleared != 0 {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("a resolved divergence left a permanent scar on the gauge"))
            .expect("the loop must keep reporting");
        cleared = report.divergent_collections;
    }
    looping.abort();
}

/// The same control at the level the loop composes, without the timers: one
/// checked contact is a *pending* finding and the gauge stays at 0; the
/// second consecutive contact with the same peer confirms it; the contact
/// after the repair clears it. `sync_once` and `DivergenceTracker` are the
/// two real pieces `replicate()` puts together, so this pins the boundary
/// exactly rather than waiting for a tick to land on the right side of it.
#[tokio::test]
async fn one_checked_contact_is_pending_and_the_second_moves_the_gauge() {
    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "stranded").unwrap();
    a.engine.insert(&ca, doc! { "_id": "1" }).unwrap();
    let theirs = a.engine.version_vector().unwrap();
    b.engine.apply_peer_batch(&theirs, &[], Hlc::ZERO, true).unwrap();

    let mut tracker = kimmy_storage::DivergenceTracker::new();
    let mut contact = |outcome: kimmy_storage::SyncOutcome| {
        let peer = outcome.peer.expect("the handshake introduces the peer");
        let existence = outcome.divergent.expect("the round reached the tail, so it checked");
        let found = existence.len();
        tracker.observe(peer, kimmy_storage::DivergenceFindings { existence, count: None });
        (found, tracker.confirmed_count())
    };

    // Found on the first contact, and deliberately not reported: a race
    // between the peer's version vector and its collection list, read a
    // message apart, produces exactly this and does not recur.
    let first = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(contact(first), (1, 0), "one contact is a pending finding, not a gauge reading");

    // Two consecutive contacts with the same peer: confirmed, and the gauge
    // leaves 0.
    let second = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(contact(second), (1, 1), "the gauge must be able to leave 0");

    // And falls again once the divergence is gone, rather than holding a
    // value nothing an operator does can clear.
    b.engine.create_collection("shop", "stranded").unwrap();
    let third = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(contact(third), (0, 0), "a resolved finding clears on its own next check");
}

/// What tells a quiet cluster from a blind one (ADR-135). ADR-133's skip is
/// correct and stays — a round whose pull the batch cap truncated cannot
/// distinguish "the peer genuinely holds a collection I lack" from "I have
/// not applied the entry that creates it here yet" — but while it applies,
/// `kimmy_sync_divergent_collections` reading 0 means *not checked*, and the
/// gauge alone cannot say so.
///
/// So the skipped count must rise for exactly the rounds the check did not
/// run on, and the checked count must not: an operator's alert rule turns on
/// the checked count still moving, and a checked count that ticked up for a
/// round nobody checked would make the blind state read as the healthy one.
#[tokio::test]
async fn a_cap_truncated_round_counts_a_skip_and_never_a_check() {
    use kimmy_cluster::protocol::MAX_BATCH;
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let a = node().await;
    let b = node().await;

    // Comfortably over the batch cap, so B's first pull is truncated and
    // its round cannot reach A's tail.
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    for i in 0..(MAX_BATCH + 200) {
        a.engine.insert(&ca, doc! { "_id": format!("d{i}") }).unwrap();
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![a.addr])], SECRET.into(), b.addr);
    config.sync_interval = Duration::from_millis(100);
    config.discovery_interval = Duration::from_millis(100);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&b.engine), config));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut checks = 0usize;
    let mut skips = 0usize;
    while checks == 0 {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the check never ran once the backlog drained"))
            .expect("the loop must keep reporting");
        assert_eq!(report.failed, 0, "the backlog is not a failure: {report:?}");
        if report.divergence_checks > 0 {
            assert!(
                skips >= 1,
                "the truncated round must have been counted as a skip before any check was: \
                 skips={skips}"
            );
        }
        checks += report.divergence_checks;
        skips += report.divergence_skips;
    }

    // And the blindness belongs to the backlog, not to the node: once the
    // pull reaches the tail every round is checked again, and nothing more
    // is skipped.
    let skipped_while_behind = skips;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while checks < 3 {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("a drained cluster stopped being checked"))
            .expect("the loop must keep reporting");
        checks += report.divergence_checks;
        skips += report.divergence_skips;
    }
    assert_eq!(skips, skipped_while_behind, "a drained cluster skips nothing further");
    looping.abort();
}

/// Seed `count` documents into `collection` on `node` as one commit — one
/// oplog entry each, which is what the tests below want, without paying an
/// fsync per document to get them.
fn seed(node: &Node, collection: &kimmy_storage::CollectionMeta, count: usize) {
    let docs = (0..count).map(|i| doc! { "_id": format!("d{i}") }).collect();
    node.engine.insert_many(collection, docs).unwrap();
}

/// Start `replicate` against one peer and report every tick, at `interval`.
/// Discovery is left far shorter than the sync interval so the peer is
/// resolved before the tick that matters, whichever arm of the loop wins the
/// first race.
fn drain_loop(
    from: &Node,
    peer: std::net::SocketAddr,
    interval: Duration,
) -> (tokio::task::JoinHandle<()>, tokio::sync::mpsc::UnboundedReceiver<kimmy_cluster::RoundReport>)
{
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![peer])], SECRET.into(), from.addr);
    config.sync_interval = interval;
    config.discovery_interval = Duration::from_millis(10);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    (tokio::spawn(replicate(Arc::clone(&from.engine), config)), rx)
}

/// [`drain_loop`], with `peer` already a member, so the loop's first tick
/// contacts it whichever arm of the loop wins the first race, and a long
/// interval needs no second tick to find it.
fn drain_loop_knowing(
    from: &Node,
    peer: &Node,
    interval: Duration,
) -> (tokio::task::JoinHandle<()>, tokio::sync::mpsc::UnboundedReceiver<kimmy_cluster::RoundReport>)
{
    use kimmy_cluster::{Members, ReplicationConfig, RoundReport, SeedSource, replicate};

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let members = Members::default();
    members.insert_for_test(peer.addr, peer.engine.node_id());
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![peer.addr])], SECRET.into(), from.addr);
    config.sync_interval = interval;
    config.discovery_interval = Duration::from_millis(10);
    config.members = Some(members);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    (tokio::spawn(replicate(Arc::clone(&from.engine), config)), rx)
}

/// ADR-157's own measure. A member five batches behind catches up inside one
/// tick, rather than one batch per tick with the member idle in between: a
/// tick keeps pulling from a peer while the pull before it came back at the
/// cap. Before this, 5,000 entries were five ticks — twenty-five seconds at
/// the default interval, and the same arithmetic is the seventy minutes a
/// quarter-million-entry backlog took to drain.
///
/// Counted in *contacts* rather than in seconds: a contact is a peer per
/// tick, and `divergence_checks + divergence_skips` is exactly one per
/// contact (ADR-145), so the report says how many ticks the peer was
/// contacted on without the test timing anything. Removing the arm that sends
/// a truncated peer round the queue again (`AfterPull::Again` in `peers.rs`)
/// puts it back at one batch per contact, and this reads five contacts.
/// Deleting only its `draining.push_back(contact)` drops the unfinished
/// contact instead, which counts no check and no skip, so the ticks are
/// counted too, and that reads five ticks.
///
/// The claim is contacts, so the runner's speed is kept out of it. The tick's
/// budget is its interval (ADR-157), and at 2 s a slow runner spent it before
/// the fifth pull and took a second contact. At 20 s no runner does, and the
/// peer is a member before the first tick, so that tick drains the backlog: one
/// contact. Reverted, a tick every 20 s reads five inside the deadline.
#[tokio::test]
async fn a_peer_five_batches_behind_is_drained_inside_one_tick() {
    use kimmy_cluster::protocol::MAX_BATCH;

    let a = node().await;
    let b = node().await;

    let entries = MAX_BATCH * 4 + 904; // 5,000, and five pulls to carry them
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    seed(&a, &ca, entries);

    let (looping, mut rx) = drain_loop_knowing(&b, &a, Duration::from_secs(20));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    let (mut contacts, mut ticks) = (0usize, 0usize);
    while b.engine.count_by_id(ca.id).unwrap() != Some(entries as u64) {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the backlog never drained"))
            .expect("the loop must keep reporting");
        assert_eq!(report.failed, 0, "a backlog is not a failure: {report:?}");
        contacts += report.divergence_checks + report.divergence_skips;
        ticks += 1;
    }
    looping.abort();

    // Both, because the two ways back to one batch per tick read differently:
    // a truncated contact that ends counts a skip, one that is dropped
    // unfinished counts nothing and only the tick count sees it.
    assert_eq!(
        (contacts, ticks),
        (1, 1),
        "five batches must drain in one tick's contact; took {contacts} contacts over {ticks} ticks"
    );
}

/// The other side of the drain, and the one that matters most: the budget is
/// the tick's own interval, so a backlog deeper than a tick can drain leaves
/// the rest for the next tick rather than overrunning the period the loop's
/// timing and ADR-154's overrun warning hang off.
///
/// At an interval of one millisecond the budget is spent by the time the
/// first pull of any tick returns, so every tick makes exactly one pull and
/// three batches take three contacts. The two contacts that end on a
/// truncated pull are counted as exactly one skip each and never as a check:
/// ADR-133's cap-truncation skip is per contact, whatever a contact pulled,
/// and a tick that gave up mid-drain has not earned the check any more than
/// a single truncated round had. Reverting the `Instant::now() < deadline`
/// half of that branch in `peers.rs` drains all three batches inside the
/// first tick, which leaves one contact that ran the check and no truncated
/// contact at all.
#[tokio::test]
async fn a_tick_stops_pulling_when_its_budget_is_spent_and_counts_one_skip() {
    use kimmy_cluster::protocol::MAX_BATCH;

    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "orders").unwrap();
    seed(&a, &ca, MAX_BATCH * 2 + 100);

    let (looping, mut rx) = drain_loop(&b, a.addr, Duration::from_millis(1));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut truncated = 0usize;
    loop {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the check never ran once the backlog drained"))
            .expect("the loop must keep reporting");
        assert_eq!(report.failed, 0, "a backlog is not a failure: {report:?}");
        if report.divergence_checks > 0 {
            assert_eq!(report.divergence_skips, 0, "the tick that drained it: {report:?}");
            break;
        }
        if report.divergence_skips > 0 {
            assert_eq!(
                report.divergence_skips, 1,
                "one contact, one skip, however many pulls it made: {report:?}"
            );
            truncated += 1;
        }
    }
    looping.abort();

    assert!(
        truncated >= 2,
        "a budget of a millisecond leaves a batch per tick, so two batches must have ended a \
         tick still truncated; {truncated} did"
    );
}

/// A drain that gets through the backlog ends on a pull short of the cap,
/// and that pull reached the peer's tail: the tick's *last* contact with the
/// peer decides, so the contact counts one check and no skip, where the same
/// backlog was a skip on every tick until it drained before ADR-157. The
/// pulls before the last one are not checked and not counted — a truncated
/// pull earns the check no more inside a drain than it did on its own.
///
/// Reverting `outcome.truncated = window_truncated;` in `transport.rs`
/// leaves the loop nothing to drain on, and the first contact is a skip.
#[tokio::test]
async fn a_drain_that_ends_short_of_the_cap_is_checked_on_its_last_pull() {
    use kimmy_cluster::protocol::MAX_BATCH;

    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "orders").unwrap();
    let entries = MAX_BATCH + 200;
    seed(&a, &ca, entries);

    let (looping, mut rx) = drain_loop(&b, a.addr, Duration::from_secs(2));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let report = loop {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the peer was never contacted"))
            .expect("the loop must keep reporting");
        if report.divergence_checks + report.divergence_skips > 0 {
            break report;
        }
    };
    looping.abort();

    assert_eq!(report.failed, 0, "{report:?}");
    assert_eq!(report.divergence_checks, 1, "the last pull reached the tail: {report:?}");
    assert_eq!(report.divergence_skips, 0, "and nothing before it was a skip: {report:?}");
    assert_eq!(
        b.engine.count_by_id(ca.id).unwrap(),
        Some(entries as u64),
        "both pulls landed inside the one tick"
    );
}

/// Counts ADR-154's overrun warning, so a test can assert that a tick did
/// not take longer than the interval it owns.
///
/// A hand-written `Subscriber` rather than a subscriber crate: the whole
/// question is whether one particular line was emitted, which is a field
/// visit, and it is not worth a test-only logging dependency. Installed with
/// `set_default`, which scopes it to the thread that installs it — the same
/// thread the current-thread runtime drives the loop's task on — so a test
/// reads back exactly the lines it caused and parallel tests do not see each
/// other's.
#[derive(Clone, Default)]
struct Overruns(Arc<AtomicUsize>);

impl tracing::Subscriber for Overruns {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
        tracing::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut overran = OverrunLine(false);
        event.record(&mut overran);
        if overran.0 {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn enter(&self, _: &tracing::Id) {}

    fn exit(&self, _: &tracing::Id) {}
}

/// Whether an event is the tick-overran warning, by its message.
struct OverrunLine(bool);

impl tracing::field::Visit for OverrunLine {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" && format!("{value:?}").contains("a sync tick took longer") {
            self.0 = true;
        }
    }
}

/// A tick that spends its whole budget draining must not overrun the interval
/// it owns *as a matter of course*, because ADR-154's overrun warning means
/// "this member's sync tick was stuck" and is written to be one line per
/// stall. The budget is therefore spent against a margin: another pull is
/// started only when the pull before it would have fitted in what is left.
///
/// The claim is exactly as strong as the margin is, and no stronger. The
/// margin is an estimate — the slowest pull the contact has made — so a pull
/// slower than every pull before it, by more than the slack left over, can
/// still cross the line, and a few in a run of a dozen ticks do. What must
/// never happen is the systematic case: deciding on "is there any time left
/// at all" makes *every* tick the budget cuts short end past its own period,
/// so the warning fires on each and an operator watching a backlog drain sees
/// nothing else.
///
/// Three assertions, in the order they are made, and it takes all three to
/// pin the budget half of ADR-157:
///
/// 1. **The budget cut a drain short more than once** — a precondition, not
///    a claim: without several such ticks the run says nothing either way.
/// 2. **Fewer of those ticks overran than there were of them.** This is the
///    claim. Reverting `Contact::fits_before` to `Instant::now() < deadline`
///    makes the two numbers equal, and this is what fails.
/// 3. **The tick drained several batches per contact.** Reverting the drain
///    arm in `peers.rs` makes contacts equal batches, and this is what fails.
///
/// **The interval is measured, not chosen**, because what this test needs is
/// one a few pulls wide — wide enough that a tick can make more than one
/// pull, narrow enough that the backlog takes many ticks — and a pull's cost
/// belongs to the machine rather than to the test. A fixed two seconds drained
/// this fixture in two ticks on an idle machine, only one of which the budget
/// cut short, so assertion 1 failed there while a loaded machine passed: the
/// test read as green in the suite and broken to anyone running it alone, and
/// it failed identically against the fix and against the revert, which is the
/// one thing a regression test may not do.
#[tokio::test]
async fn a_tick_that_spends_its_budget_draining_does_not_overrun_its_interval() {
    use kimmy_cluster::protocol::MAX_BATCH;

    let a = node().await;
    let b = node().await;

    const BATCHES: usize = 24;
    let entries = MAX_BATCH * BATCHES;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    seed(&a, &ca, entries);

    let overruns = Overruns::default();
    let counted = Arc::clone(&overruns.0);
    let _recording = tracing::subscriber::set_default(overruns);

    // One round before the loop starts, to price a pull on this machine. It
    // leaves the backlog a batch shorter, which is all it costs.
    let priced = std::time::Instant::now();
    sync_once(&b.engine, a.addr, SECRET, None).await.expect("a first round to price a pull by");
    let interval = priced.elapsed() * 3;

    let (looping, mut rx) = drain_loop(&b, a.addr, interval);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let (mut contacts, mut cut_short) = (0usize, 0usize);
    while b.engine.count_by_id(ca.id).unwrap() != Some(entries as u64) {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the backlog never drained"))
            .expect("the loop must keep reporting");
        assert_eq!(report.failed, 0, "a backlog is not a failure: {report:?}");
        contacts += report.divergence_checks + report.divergence_skips;
        cut_short += report.divergence_skips;
    }
    looping.abort();

    let overran = counted.load(Ordering::Relaxed);
    assert!(
        cut_short >= 2,
        "the budget must have cut a drain short more than once, or this run says nothing: \
         {cut_short} such ticks at an interval of {interval:?}"
    );
    assert!(
        overran < cut_short,
        "a tick must not overrun the interval it owns because it drained: {overran} overruns \
         across {cut_short} ticks the budget cut short, which is every one of them"
    );
    assert!(
        contacts < BATCHES,
        "the ticks must have drained several batches each: {contacts} contacts for {BATCHES} \
         batches"
    );
}

/// A tick's contact with a peer at the pull ceiling, run against a fake peer,
/// and what the tick said about it.
///
/// The peer gets the handshake right and advertises an origin far ahead of
/// this member. Every pull is answered unexhausted with one entry of that
/// origin, below the advertised vector so it is never deferred. `fresh` picks
/// the entry: a new document each pull, which a real drain deeper than the
/// ceiling looks like, or the one document this member already applied, which
/// moves nothing — ADR-157's residual.
///
/// **The interval is priced, not chosen**, for the reason the drain-budget
/// test above gives: a pull's cost belongs to the machine. One pull against
/// the fake is timed, and the interval set to three ceilings' worth of them.
struct CeilingTick {
    pulls: usize,
    failed: usize,
    ceiling_infos: usize,
    ceiling_warns: usize,
}

async fn a_tick_at_the_pull_ceiling(fresh: bool) -> CeilingTick {
    use kimmy_cluster::protocol::prove;
    use kimmy_cluster::{
        MAX_PULLS_PER_CONTACT, ReplicationConfig, RoundReport, SeedSource, replicate,
    };

    let b = node().await;
    let coll = b.engine.create_collection("shop", "orders").unwrap();
    let origin = kimmy_core::NodeId::generate();
    let collection = coll.id;
    let entry = move |n: u64| kimmy_core::OplogEntry {
        stamp: kimmy_core::Stamp::new(Hlc::new(1_000 + n, 0), origin),
        kind: kimmy_core::OpKind::Insert,
        collection,
        doc_id: Some(DocId::String(format!("d{n}"))),
        body: Some(bson::serialize_to_vec(&doc! { "_id": format!("d{n}") }).unwrap()),
    };
    if !fresh {
        b.engine.apply_remote(&coll, &entry(0)).unwrap();
    }
    let mut theirs = kimmy_core::VersionVector::new();
    theirs.insert(origin, Hlc::new(1_000_000, 0));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fake = listener.local_addr().unwrap();
    let pulls = Arc::new(AtomicUsize::new(0));
    tokio::spawn({
        let pulls = Arc::clone(&pulls);
        async move {
            let tls = kimmy_cluster::tls::ClusterTls::new().unwrap();
            while let Ok((tcp, _)) = listener.accept().await {
                let acceptor = tls.acceptor();
                let (theirs, pulls) = (theirs.clone(), Arc::clone(&pulls));
                tokio::spawn(async move {
                    let Ok(mut stream) = acceptor.accept(tcp).await else { return };
                    let binding = kimmy_cluster::tls::binding(stream.get_ref().1).unwrap();
                    let Ok(Message::Hello { nonce, .. }) = read_frame(&mut stream).await else {
                        return;
                    };
                    let welcome = Message::Welcome {
                        node: origin,
                        nonce: vec![7; 32],
                        proof: prove(SECRET, &nonce, &binding),
                    };
                    if write_frame(&mut stream, &welcome).await.is_err() {
                        return;
                    }
                    let Ok(Message::Confirm { .. }) = read_frame(&mut stream).await else {
                        return;
                    };
                    while let Ok(message) = read_frame(&mut stream).await {
                        let answer = match message {
                            Message::AskVersions { .. } => Message::Vectors {
                                servable: theirs.clone(),
                                witnessed: theirs.clone(),
                            },
                            Message::AskEntries { .. } => {
                                let n = pulls.fetch_add(1, Ordering::SeqCst) as u64;
                                let served = entry(if fresh { n } else { 0 });
                                let scanned_to = served.stamp.hlc;
                                Message::Entries {
                                    entries: vec![served],
                                    scanned_to,
                                    exhausted: false,
                                }
                            }
                            _ => return,
                        };
                        if write_frame(&mut stream, &answer).await.is_err() {
                            return;
                        }
                    }
                });
            }
        }
    });

    let priced = std::time::Instant::now();
    let one = sync_once(&b.engine, fake, SECRET, None).await.expect("one pull to price");
    let interval = priced.elapsed() * (3 * MAX_PULLS_PER_CONTACT as u32);
    assert!(one.truncated, "the fake must read as truncated, or this tests nothing: {one:?}");
    let priced_pulls = pulls.load(Ordering::SeqCst);

    let lines = CeilingLines::default();
    let (infos, warns) = (Arc::clone(&lines.info), Arc::clone(&lines.warn));
    let _recording = tracing::subscriber::set_default(lines);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![fake])], SECRET.into(), b.addr);
    config.sync_interval = interval;
    config.discovery_interval = Duration::from_millis(10);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&b.engine), config));

    // The first tick that reached the peer. Nothing else pulls until the next
    // tick, a whole interval later, so what is read at its report is that
    // tick's alone.
    let deadline = tokio::time::Instant::now() + interval * 4 + Duration::from_secs(10);
    let tick = loop {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("no tick reached the peer"))
            .expect("the loop must keep reporting");
        let seen = pulls.load(Ordering::SeqCst) - priced_pulls;
        if seen > 0 {
            break CeilingTick {
                pulls: seen,
                failed: report.failed,
                ceiling_infos: infos.load(Ordering::SeqCst),
                ceiling_warns: warns.load(Ordering::SeqCst),
            };
        }
    };
    looping.abort();
    tick
}

/// The ceiling lines, by level, counted on this test's thread only.
#[derive(Clone, Default)]
struct CeilingLines {
    info: Arc<AtomicUsize>,
    warn: Arc<AtomicUsize>,
}

impl tracing::Subscriber for CeilingLines {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
        tracing::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut line = CeilingLine(false);
        event.record(&mut line);
        if line.0 {
            match *event.metadata().level() {
                tracing::Level::WARN => self.warn.fetch_add(1, Ordering::SeqCst),
                tracing::Level::INFO => self.info.fetch_add(1, Ordering::SeqCst),
                _ => 0,
            };
        }
    }

    fn enter(&self, _: &tracing::Id) {}

    fn exit(&self, _: &tracing::Id) {}
}

/// Whether an event is the pull-ceiling line, by its message.
struct CeilingLine(bool);

impl tracing::field::Visit for CeilingLine {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" && format!("{value:?}").contains("pull ceiling reached") {
            self.0 = true;
        }
    }
}

/// ADR-157's residual, closed by its addendum. A peer answering every pull
/// with a window this member already holds, unexhausted, moves nothing and
/// still reads as truncated, so the drain would pull again until the tick's
/// deadline. The ceiling ends the contact at `MAX_PULLS_PER_CONTACT`, and
/// because nothing was applied in any of those pulls the line is a warning.
/// Reverting the ceiling in `peers.rs` lets the tick pull until its deadline:
/// about three times the ceiling.
#[tokio::test]
async fn a_peer_serving_windows_that_move_nothing_is_held_to_the_pull_ceiling() {
    let tick = a_tick_at_the_pull_ceiling(false).await;
    assert_eq!(tick.failed, 0, "a ceiling is not a failure");
    assert_eq!(
        tick.pulls,
        kimmy_cluster::MAX_PULLS_PER_CONTACT,
        "one contact, held to the ceiling, with budget left for about twice as many more"
    );
    assert_eq!((tick.ceiling_warns, tick.ceiling_infos), (1, 0), "nothing applied: one warning");
}

/// The same ceiling reached by a real drain: every pull applied a new
/// document. The contact still ends at the ceiling — the spill ADR-157's
/// addendum prices — but a drain that is making progress is not a peer to
/// suspect, so the line is at info, not a warning.
#[tokio::test]
async fn a_drain_deeper_than_the_ceiling_spills_to_the_next_tick_without_a_warning() {
    let tick = a_tick_at_the_pull_ceiling(true).await;
    assert_eq!(tick.failed, 0, "a ceiling is not a failure");
    assert_eq!(tick.pulls, kimmy_cluster::MAX_PULLS_PER_CONTACT, "held to the ceiling as well");
    assert_eq!((tick.ceiling_warns, tick.ceiling_infos), (0, 1), "entries applied: one info line");
}

/// A pull that fails ends the tick's contact with that peer. The round goes
/// through the health backoff it always did, and the tick does not spend the
/// rest of its budget dialling a member that has just refused it — a
/// failure is not a truncation, and nothing about it says another pull would
/// do better.
///
/// The relay hands exactly one connection through, so the tick's first pull
/// is a real, cap-truncated round and the pull the drain wants next cannot
/// connect at all. Exactly one failure, and the contact it ends is one skip
/// (ADR-145). Reverting the `contact.finish()` in the `Err` arm of
/// `peers.rs` to a `draining.push_back(contact)` retries the failure until
/// the budget runs out, and this reads a tick of them.
#[tokio::test]
async fn a_pull_that_fails_ends_the_contact_rather_than_being_retried_inside_the_tick() {
    use kimmy_cluster::protocol::MAX_BATCH;

    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "orders").unwrap();
    seed(&a, &ca, MAX_BATCH + 200);
    let relay = relay_one_connection(a.addr).await;

    let (looping, mut rx) = drain_loop(&b, relay, Duration::from_secs(2));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let report = loop {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the second pull never failed"))
            .expect("the loop must keep reporting");
        if report.failed > 0 {
            break report;
        }
    };
    looping.abort();

    assert_eq!(report.failed, 1, "the failure ends the contact, it is not retried: {report:?}");
    assert_eq!(report.divergence_checks, 0, "a failed round has not checked: {report:?}");
    assert_eq!(report.divergence_skips, 1, "and is the contact's one skip: {report:?}");
    assert_eq!(
        report.pulls.contacts[kimmy_cluster::ContactEnd::Failed.slot()],
        1,
        "and is the one contact counted as ended by a failure (ADR-175): {report:?}"
    );
}

/// A byte relay to `target` that hands exactly one connection through and
/// closes every later one: a member whose inbound replication completes
/// once and then never again. TLS runs end to end through it, so the
/// channel binding holds and the one round is a real round — a relay that
/// terminated TLS could not do this (see `man_in_the_middle`). The listener
/// stays bound for the life of the test — dropping it would free the port
/// for another test in the same process to bind — and every connection
/// after the first is accepted and dropped at once, so each later dial
/// fails on a socket the peer closed rather than on anything with a timer
/// in it.
async fn relay_one_connection(target: std::net::SocketAddr) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let Ok((mut inbound, _)) = listener.accept().await else { return };
        let relay = async {
            let Ok(mut upstream) = TcpStream::connect(target).await else { return };
            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut upstream).await;
        };
        let close_the_rest = async {
            while let Ok((later, _)) = listener.accept().await {
                drop(later);
            }
        };
        tokio::join!(relay, close_the_rest);
    });
    addr
}

/// The live finding ADR-145 answers, end to end through two real loops.
/// One member's inbound replication freezes after a single good round;
/// the other keeps writing into a collection both hold by name. Every
/// member holds the same collections, so the existence half agrees
/// everywhere; the frozen member is behind and never advances, so under
/// ADR-133's gate alone the count half would be deferred against it on
/// every contact for as long as the freeze lasts — which on the cluster
/// that found this was some 250 checks reporting clean.
///
/// Two claims, one per side. On the healthy member: the count half defers
/// while the peer's position is unproven, compares once it has stood still
/// for `FROZEN_CONTACTS` checked contacts, and the gauge then names the
/// count-only divergence. On the frozen member: its one check leaves a
/// reading, every round after fails and is counted as a skip, `ran` never
/// moves again, and the age of that reading rises — the number that says
/// the gauge it is holding is old.
#[tokio::test]
async fn a_count_divergence_on_a_frozen_peer_is_found_and_the_frozen_member_reports_its_age() {
    use kimmy_cluster::{FROZEN_CONTACTS, ReplicationConfig, RoundReport, SeedSource, replicate};

    let healthy = node().await;
    let frozen = node().await;

    let orders = healthy.engine.create_collection("shop", "orders").unwrap();
    for i in 0..20 {
        healthy.engine.insert(&orders, doc! { "_id": format!("d{i}") }).unwrap();
    }
    sync(&healthy, &frozen).await;
    sync(&healthy, &frozen).await; // converged: same collection, same count, both ways

    // The member that will freeze pulls through a relay that lets exactly
    // one round through. That round runs the check, so there is a reading
    // to grow old; every round after it fails at once, on a connection the
    // relay accepts and closes.
    let relay = relay_one_connection(healthy.addr).await;
    let (frozen_tx, mut frozen_rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![relay])], SECRET.into(), frozen.addr);
    config.sync_interval = Duration::from_millis(100);
    config.discovery_interval = Duration::from_millis(100);
    config.on_round = Some(Arc::new(move |report| {
        let _ = frozen_tx.send(report);
    }));
    let frozen_loop = tokio::spawn(replicate(Arc::clone(&frozen.engine), config));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let report = tokio::time::timeout_at(deadline, frozen_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the one relayed round never ran the check"))
            .expect("the loop must keep reporting");
        if report.divergence_checks > 0 {
            // The report carries the instant of the check, not an age
            // (ADR-154); the age is the reader's subtraction.
            let checked =
                report.divergence_last_check.unwrap_or_else(|| panic!("just checked: {report:?}"));
            assert!(checked.elapsed() < Duration::from_secs(1), "just checked: {report:?}");
            break;
        }
        assert_eq!(report.divergence_last_check, None, "nothing has run yet: {report:?}");
    }

    // The healthy member keeps writing into the collection both hold. The
    // frozen one will never see these: a count-only divergence on a
    // collection whose name and existence agree on every member.
    for i in 20..40 {
        healthy.engine.insert(&orders, doc! { "_id": format!("d{i}") }).unwrap();
    }

    let (healthy_tx, mut healthy_rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config = ReplicationConfig::new(
        vec![SeedSource::Static(vec![frozen.addr])],
        SECRET.into(),
        healthy.addr,
    );
    config.sync_interval = Duration::from_millis(100);
    config.discovery_interval = Duration::from_millis(100);
    config.on_round = Some(Arc::new(move |report| {
        let _ = healthy_tx.send(report);
    }));
    let healthy_loop = tokio::spawn(replicate(Arc::clone(&healthy.engine), config));

    // The healthy side: deferred while the peer's stillness is unproven,
    // compared once it is, confirmed on the second compared probe.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let (mut checks, mut compared, mut deferred, mut confirmed) = (0usize, 0usize, 0usize, 0usize);
    while confirmed == 0 {
        let report = tokio::time::timeout_at(deadline, healthy_rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the count divergence never confirmed against the frozen peer: \
                     checks={checks} compared={compared} deferred={deferred}"
                )
            })
            .expect("the loop must keep reporting");
        assert_eq!(report.failed, 0, "the frozen peer answers every pull: {report:?}");
        assert_eq!(report.divergence_skips, 0, "nothing truncates a converged pull: {report:?}");
        if compared == 0 && report.divergence_count_compared > 0 {
            assert!(
                deferred >= FROZEN_CONTACTS as usize,
                "a peer that is behind is compared only after standing still for \
                 {FROZEN_CONTACTS} checked contacts, never sooner: deferred={deferred}"
            );
        }
        checks += report.divergence_checks;
        compared += report.divergence_count_compared;
        deferred += report.divergence_count_deferred;
        confirmed = report.divergent_collections;
    }
    assert_eq!(confirmed, 1, "the count-only divergence, on a collection held everywhere by name");
    assert!(compared >= 2, "confirmed on two consecutive probes, never one: compared={compared}");
    assert_eq!(
        checks,
        compared + deferred,
        "one collection in rotation: every checked contact either compared it or deferred it"
    );
    healthy_loop.abort();

    // The frozen side: `ran` stays where its one good round left it, every
    // failed round is a skip, and the age of the reading rises.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let (mut later_checks, mut failed, mut skips) = (0usize, 0usize, 0usize);
    let mut age = 0u64;
    let mut last_check: Option<std::time::Instant> = None;
    while age < 1 {
        let report = tokio::time::timeout_at(deadline, frozen_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the frozen member's check age never rose"))
            .expect("the loop must keep reporting");
        let checked =
            report.divergence_last_check.unwrap_or_else(|| panic!("a reading exists: {report:?}"));
        if let Some(previous) = last_check {
            assert_eq!(checked, previous, "no check ran, so the instant stands: {report:?}");
        }
        assert_eq!(report.divergent_collections, 0, "the reading it is holding: {report:?}");
        later_checks += report.divergence_checks;
        failed += report.failed;
        skips += report.divergence_skips;
        // The age is computed where it is read (ADR-154): here, from the
        // instant the report carries, as `/metrics` does on a scrape.
        age = checked.elapsed().as_secs();
        last_check = Some(checked);
    }
    assert_eq!(later_checks, 0, "no round completed after the freeze, so nothing re-examined");
    assert!(failed >= 1, "the freeze is a run of failed rounds");
    assert_eq!(skips, failed, "every failed round is counted as a skip, and only those");
    frozen_loop.abort();
}

/// The pre-existing defect ADR-146 corrects, end to end through two real
/// loops on a cluster with nothing to do. The gate that decides whether a
/// peer is "behind" compared the peer's *servable* vector against this
/// node's *witnessed* vector. A peer that processed this node's latest entry
/// from some origin without appending it — here, the loser of a concurrent
/// write to one document (ADR-054) — has a servable position on that origin
/// below this node's witnessed position, for ever: under the old gate it
/// read as behind indefinitely, and under ADR-145 as behind-and-still, so
/// the count half deferred against it for `FROZEN_CONTACTS` contacts before
/// comparing — a steady `deferred` trickle on an idle cluster, and the right
/// count for the wrong reason. Gated on what the peer has *processed*, a
/// converged idle cluster defers nothing: `deferred` stays at 0 and
/// `compared` rises on both members.
///
/// The collection and the index are created on one member and replicated
/// to the other; a replicated schema change is appended under its
/// originating stamp, so on its own it leaves no gap between the two
/// vectors. The discarded write is what does.
#[tokio::test]
async fn a_converged_idle_cluster_defers_no_count_probe() {
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let a = node().await;
    let b = node().await;

    let orders = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.create_index("shop", "orders", vec![field("email")], false, None).unwrap();
    sync(&a, &b).await;

    // A concurrent write to one document: the older one loses on the other
    // member, which processes it without appending it (ADR-054).
    a.engine.insert(&orders, doc! { "_id": 1, "email": "first" }).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let orders_b = b.engine.get_collection("shop", "orders").unwrap();
    b.engine.insert(&orders_b, doc! { "_id": 1, "email": "second" }).unwrap();

    sync(&a, &b).await;
    sync(&a, &b).await; // converged, and nothing else is ever written
    assert!(
        a.engine.version_vector().unwrap() != b.engine.version_vector().unwrap()
            || b.engine.witnessed_vector().unwrap() != b.engine.version_vector().unwrap(),
        "the discarded write must leave one member's servable vector below its witnessed one"
    );

    let start = |from: &Node, to: &Node| {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
        let mut config = ReplicationConfig::new(
            vec![SeedSource::Static(vec![to.addr])],
            SECRET.into(),
            from.addr,
        );
        config.sync_interval = Duration::from_millis(100);
        config.discovery_interval = Duration::from_millis(100);
        config.on_round = Some(Arc::new(move |report| {
            let _ = tx.send(report);
        }));
        (tokio::spawn(replicate(Arc::clone(&from.engine), config)), rx)
    };
    let (a_loop, mut a_rx) = start(&a, &b);
    let (b_loop, mut b_rx) = start(&b, &a);

    // Several sync intervals on each member: every checked contact compares
    // the one collection in rotation, and not one defers it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    for (name, rx) in [("a", &mut a_rx), ("b", &mut b_rx)] {
        let (mut checks, mut compared) = (0usize, 0usize);
        while checks < 8 {
            let report = tokio::time::timeout_at(deadline, rx.recv())
                .await
                .unwrap_or_else(|_| panic!("member {name} never ran eight checks"))
                .expect("the loop must keep reporting");
            assert_eq!(report.failed, 0, "{name}: {report:?}");
            assert_eq!(report.divergence_skips, 0, "{name}: {report:?}");
            assert_eq!(
                report.divergence_count_deferred, 0,
                "{name}: a converged idle cluster has nothing to defer: {report:?}"
            );
            assert_eq!(report.divergent_collections, 0, "{name}: {report:?}");
            checks += report.divergence_checks;
            compared += report.divergence_count_compared;
        }
        assert_eq!(compared, checks, "{name}: every checked contact compared the one collection");
    }
    a_loop.abort();
    b_loop.abort();
}

/// One of ADR-132's two remaining rises of `kimmy_sync_ddl_refused_total`,
/// over the wire: a rival definition arrives under a name this node holds
/// with **no creation stamp**, so there is nothing to arbitrate with, and the
/// arrival is skipped and counted exactly as ADR-123 left it. The 0.22.0
/// round read `0` on this counter on every member in every sample, and
/// nothing anywhere drove either of the two cases that still move it.
///
/// The unstamped definition is built the only way a node can genuinely
/// acquire one — restored from a snapshot page carrying no stamp, which is
/// what a definition written before ADR-132 looks like on the wire and on
/// disk.
#[tokio::test]
async fn an_unstamped_rival_definition_is_refused_and_counted_over_the_wire() {
    let a = node().await;
    let b = node().await;
    let source = node().await;

    // A definition with its creation stamp stripped, restored onto B.
    source.engine.create_collection("shop", "orders").unwrap();
    source
        .engine
        .create_index(
            "shop",
            "orders",
            vec![kimmy_core::IndexField::ascending("email")],
            false,
            Some("by_email".into()),
        )
        .unwrap();
    let mut page = source.engine.snapshot_page(None, None).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            index.created = None;
        }
    }
    page.documents.clear();
    // Granting B no coverage of another node's history: this fixture is
    // about the stored shape of the definition, nothing else.
    page.versions = kimmy_core::VersionVector::default();
    b.engine
        .apply_snapshot_page(
            a.engine.node_id(),
            &mut kimmy_storage::SnapshotProgress::whole_database(),
            &page,
        )
        .unwrap();
    assert!(
        b.engine
            .get_collection("shop", "orders")
            .unwrap()
            .index("by_email")
            .unwrap()
            .created
            .is_none(),
        "the fixture must leave B holding a definition with no creation stamp"
    );

    // A holds a rival definition under the same name, stamped.
    a.engine.create_collection("shop", "orders").unwrap();
    a.engine
        .create_index(
            "shop",
            "orders",
            vec![kimmy_core::IndexField::ascending("email")],
            true,
            Some("by_email".into()),
        )
        .unwrap();
    let ca = a.engine.get_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "after" }).unwrap();

    let outcome = sync_once(&b.engine, a.addr, SECRET, None)
        .await
        .expect("a rival it cannot arbitrate must not fail the round");
    assert_eq!(outcome.ddl_refused, 1, "skipped and counted: {outcome:?}");
    assert!(
        !b.engine.get_collection("shop", "orders").unwrap().index("by_email").unwrap().unique,
        "B keeps the definition it cannot arbitrate away"
    );
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    assert!(
        b.engine.get(&cb, &DocId::String("after".into())).unwrap().is_some(),
        "and the refusal does not stop the entries behind it"
    );
}

/// ADR-132's other rise, over the wire, under ADR-139: the arriving definition
/// *wins* the creation-stamp comparison over a document this node holds that
/// the definition cannot key. Before ADR-139 the replacement aborted whole
/// and was counted; now the winner builds, the document is filed unkeyed
/// under it, and the members agree on the later definition. A definition
/// this build cannot *apply* still aborts whole — that case is pinned at the
/// storage level, where an entry no member can mint can be built by hand.
#[tokio::test]
async fn a_winning_definition_builds_over_a_document_it_cannot_key_over_the_wire() {
    let a = node().await;
    let b = node().await;
    let source = node().await;

    // B's definition, stamped older than anything A can mint — pinned to a
    // fixed stamp rather than left to the clock, so which side wins is a
    // fact of the fixture and not of how fast the test ran.
    let ancient = kimmy_core::Stamp::new(Hlc::new(1, 0), kimmy_core::NodeId::from_bytes([0; 16]));
    source.engine.create_collection("shop", "orders").unwrap();
    source
        .engine
        .create_index(
            "shop",
            "orders",
            vec![kimmy_core::IndexField::ascending("tags")],
            false,
            Some("probe".into()),
        )
        .unwrap();
    let mut page = source.engine.snapshot_page(None, None).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            index.created = Some(ancient);
        }
    }
    page.documents.clear();
    page.versions = kimmy_core::VersionVector::default();
    b.engine
        .apply_snapshot_page(
            a.engine.node_id(),
            &mut kimmy_storage::SnapshotProgress::whole_database(),
            &page,
        )
        .unwrap();
    assert_eq!(
        b.engine.get_collection("shop", "orders").unwrap().index("probe").unwrap().created,
        Some(ancient),
        "the fixture must leave B holding the older definition, or A's does not win \
         the comparison and this test proves something else"
    );

    // The document A's compound definition cannot key: two indexed paths
    // both holding arrays.
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    b.engine.insert(&cb, doc! { "_id": "both", "tags": ["x", "y"], "cats": ["p", "q"] }).unwrap();

    // A's rival, which builds there because A holds no such document.
    a.engine.create_collection("shop", "orders").unwrap();
    a.engine
        .create_index(
            "shop",
            "orders",
            vec![
                kimmy_core::IndexField::ascending("tags"),
                kimmy_core::IndexField::ascending("cats"),
            ],
            false,
            Some("probe".into()),
        )
        .expect("accepted on A: no document there holds arrays at both paths");
    let ca = a.engine.get_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "after" }).unwrap();

    let outcome = sync_once(&b.engine, a.addr, SECRET, None)
        .await
        .expect("a definition over a document it cannot key must not wedge the round");
    assert_eq!(outcome.ddl_refused, 0, "built, not refused: {outcome:?}");
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    let index = cb.index("probe").cloned().unwrap();
    assert_eq!(index.fields.len(), 2, "the later definition stands on B, as on A");
    assert_eq!(
        b.engine.unkeyed_count(&cb, index.id).unwrap(),
        1,
        "B's own document, filed unkeyed under the winner"
    );
    assert!(
        b.engine.get(&cb, &DocId::String("after".into())).unwrap().is_some(),
        "and the entries behind it arrive"
    );
}

// ---------------------------------------------------------------------------
// A schema change confirms itself on its peers (ADR-140)
// ---------------------------------------------------------------------------

/// Bind an ephemeral port and serve `engine` on it, with a push hook.
async fn listen_with(
    engine: &Arc<Engine>,
    hook: kimmy_cluster::PushHook,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tls = std::sync::Arc::new(
        kimmy_cluster::tls::ClusterTls::new().expect("cluster TLS for the test listener"),
    );
    let serving =
        tokio::spawn(serve_with(Arc::clone(engine), listener, SECRET.to_string(), Some(hook), tls));
    (addr, serving)
}

#[tokio::test]
async fn a_pushed_schema_change_is_applied_at_once_and_reported() {
    // What the confirmation rides on: the member holds the definition when
    // the push answers, not a sync interval later, and the window is
    // witnessed so anti-entropy does not fetch it again.
    let a = node().await;
    let b = node().await;
    a.engine.create_collection("shop", "orders").unwrap();
    a.engine
        .create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
        .unwrap();
    let entry = newest(&a.engine, kimmy_core::OpKind::CreateIndex);

    let pushed = push_entry(&a.engine, b.addr, SECRET, &entry).await.expect("the push is answered");
    assert_eq!(pushed.node, b.engine.node_id(), "the peer names itself in the handshake");
    assert_eq!(pushed.unreached, None, "{pushed:?}");
    assert_eq!(
        pushed.outcome.ddl, 2,
        "the collection the member lacked, then the index: {pushed:?}"
    );
    assert_eq!(pushed.outcome.ddl_refused, 0, "{pushed:?}");
    assert!(
        b.engine.get_collection("shop", "orders").unwrap().index("by_email").is_some(),
        "held now, not after the next sync interval"
    );
    let second = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(second.total(), 0, "witnessed by the push, so nothing is re-requested: {second:?}");
}

/// The newest entry of `kind` this node holds — the one a confirmation
/// pushes a moment after minting it.
fn newest(engine: &Engine, kind: kimmy_core::OpKind) -> kimmy_core::OplogEntry {
    engine
        .entries_for_peer(Hlc::ZERO, kimmy_cluster::protocol::MAX_BATCH * 2)
        .unwrap()
        .entries
        .into_iter()
        .rev()
        .find(|e| e.kind == kind)
        .expect("an entry of that kind")
}

#[tokio::test]
async fn a_push_carries_everything_the_member_lacks_before_the_change() {
    // The hole the first push opened, closed (ADR-143). A creates a
    // collection, writes to it and adds an index within one sync interval;
    // B has pulled nothing yet. Pushing the index alone raised B's witnessed
    // vector past the collection and the document, so B skipped the index as
    // an unknown collection and was never sent the collection again — a
    // member with a hole in its history for the life of the cluster, which
    // the cluster harness met as a TTL owner that never expired anything.
    // The window a push carries now starts where B's history ends.
    let a = node().await;
    let b = node().await;
    let ca = a.engine.create_collection("shop", "sessions").unwrap();
    a.engine.insert(&ca, doc! { "_id": "s1", "seen": 0 }).unwrap();
    a.engine
        .create_index("shop", "sessions", vec![field("seen")], false, Some("ttl_seen".into()))
        .unwrap();
    let entry = newest(&a.engine, kimmy_core::OpKind::CreateIndex);

    let pushed = push_entry(&a.engine, b.addr, SECRET, &entry).await.unwrap();
    assert_eq!(pushed.unreached, None, "{pushed:?}");
    assert_eq!(pushed.outcome.unknown_collection, 0, "the collection arrived first: {pushed:?}");
    assert_eq!((pushed.outcome.ddl, pushed.outcome.applied), (2, 1), "{pushed:?}");
    let cb = b.engine.get_collection("shop", "sessions").expect("the collection, not a hole");
    assert!(cb.index("ttl_seen").is_some(), "and the index over it");
    assert!(
        b.engine.get(&cb, &DocId::String("s1".into())).unwrap().is_some(),
        "and the document written before it"
    );
    let second = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(second.total(), 0, "nothing left to pull and nothing re-requested: {second:?}");
}

#[tokio::test]
async fn a_member_that_already_holds_the_change_is_confirmed_without_a_window() {
    // A sync round got there first: the member's witnessed position is past
    // the entry, so the push sends nothing and reports it held.
    let a = node().await;
    let b = node().await;
    a.engine.create_collection("shop", "orders").unwrap();
    a.engine
        .create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
        .unwrap();
    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    let entry = newest(&a.engine, kimmy_core::OpKind::CreateIndex);

    let pushed = push_entry(&a.engine, b.addr, SECRET, &entry).await.unwrap();
    assert_eq!(pushed.unreached, None, "{pushed:?}");
    assert_eq!(pushed.outcome.total(), 0, "already held; nothing sent: {pushed:?}");
}

#[tokio::test]
async fn a_member_more_than_a_batch_behind_is_reported_unreached_and_sent_nothing() {
    // The window is derived exactly as a pull's and capped the same way, so
    // a member this far behind cannot be reached in one exchange. It is told
    // nothing — a window that stops short of the entry would only do a sync
    // round's work on a request's clock — and named, and the sync loop
    // brings it up at its own pace, in order, with no hole.
    let a = node().await;
    let b = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    let batch: Vec<_> =
        (0..kimmy_cluster::protocol::MAX_BATCH as i64 + 1).map(|n| doc! { "_id": n }).collect();
    a.engine.insert_many(&ca, batch).unwrap();
    a.engine
        .create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
        .unwrap();
    let entry = newest(&a.engine, kimmy_core::OpKind::CreateIndex);

    let pushed = push_entry(&a.engine, b.addr, SECRET, &entry).await.unwrap();
    let reason = pushed.unreached.clone().expect("named unreached");
    assert!(reason.contains("entries behind"), "{reason}");
    assert_eq!(pushed.outcome.total(), 0, "nothing was sent: {pushed:?}");
    assert!(b.engine.get_collection("shop", "orders").is_err(), "nothing was applied");

    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    assert_eq!(b.engine.count(&cb).unwrap(), kimmy_cluster::protocol::MAX_BATCH as u64 + 1);
    assert!(cb.index("by_email").is_some(), "anti-entropy carried it");
}

#[tokio::test]
async fn a_member_below_the_retention_horizon_is_reported_unreached() {
    // The same horizon check a served pull makes: a member whose gap has
    // been collected is not handed what is left of it (ADR-097). It is named
    // unreached, and the snapshot fallback brings it up on its next round.
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    for i in 0..50i64 {
        a.engine.insert(&ca, doc! { "_id": i }).unwrap();
    }
    a.engine
        .collect_garbage_at(
            kimmy_storage::physical_now_ms() + 1_000_000_000,
            kimmy_storage::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();
    a.engine
        .create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
        .unwrap();
    let b = node().await;
    let entry = newest(&a.engine, kimmy_core::OpKind::CreateIndex);

    let pushed = push_entry(&a.engine, b.addr, SECRET, &entry).await.unwrap();
    assert!(
        pushed.unreached.as_deref().is_some_and(|r| r.contains("retention horizon")),
        "{pushed:?}"
    );
    assert!(b.engine.get_collection("shop", "orders").is_err(), "nothing was applied");

    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    assert_eq!(b.engine.count(&cb).unwrap(), 50, "the snapshot brought it up");
    assert!(cb.index("by_email").is_some(), "index included");
}

#[tokio::test]
async fn a_pushed_schema_change_the_receiver_cannot_apply_is_reported_as_refused() {
    // The answer a confirmation acts on: the member could not apply the
    // definition, skipped it, and counted it — and says so, rather than
    // failing the exchange.
    let (a, b) = sender_with_a_definition_the_receiver_cannot_arbitrate().await;
    let entry = newest(&a.engine, kimmy_core::OpKind::CreateIndex);

    let pushed = push_entry(&a.engine, b.addr, SECRET, &entry).await.unwrap();
    assert_eq!(pushed.unreached, None, "{pushed:?}");
    assert_eq!(pushed.outcome.ddl_refused, 1, "refused and reported: {pushed:?}");
    assert!(
        !b.engine.get_collection("shop", "orders").unwrap().index("by_email").unwrap().unique,
        "B keeps the definition it cannot arbitrate away"
    );
}

#[tokio::test]
async fn the_push_hook_sees_what_the_receiver_refused() {
    // The receiver's own counter must not depend on which way a refusal
    // arrived: the hook is what carries a pushed refusal to it.
    let a = node().await;
    let refused = Arc::new(AtomicUsize::new(0));
    let hook: kimmy_cluster::PushHook = Arc::new({
        let refused = Arc::clone(&refused);
        move |outcome: &kimmy_storage::SyncOutcome| {
            refused.fetch_add(outcome.ddl_refused, Ordering::SeqCst);
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let (addr, _serving) = listen_with(&engine, hook).await;

    // B holds an unstamped definition under the name A's rival carries.
    let source = node().await;
    source.engine.create_collection("shop", "orders").unwrap();
    source
        .engine
        .create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
        .unwrap();
    let mut page = source.engine.snapshot_page(None, None).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            index.created = None;
        }
    }
    page.documents.clear();
    page.versions = kimmy_core::VersionVector::default();
    engine
        .apply_snapshot_page(
            a.engine.node_id(),
            &mut kimmy_storage::SnapshotProgress::whole_database(),
            &page,
        )
        .unwrap();
    a.engine.create_collection("shop", "orders").unwrap();
    a.engine
        .create_index("shop", "orders", vec![field("email")], true, Some("by_email".into()))
        .unwrap();
    let entry = newest(&a.engine, kimmy_core::OpKind::CreateIndex);

    let pushed = push_entry(&a.engine, addr, SECRET, &entry).await.unwrap();
    assert_eq!(pushed.outcome.ddl_refused, 1, "{pushed:?}");
    assert_eq!(refused.load(Ordering::SeqCst), 1, "the hook saw the refusal");
}

// ---------------------------------------------------------------------------
// A drop mints its entry wherever it lands (ADR-141)
// ---------------------------------------------------------------------------

/// A holds an index; B holds the collection and has never heard of the
/// index — the member a front happened to route a drop to.
async fn holder_and_a_member_without_the_index() -> (Node, Node) {
    let a = node().await;
    let b = node().await;
    a.engine.create_collection("shop", "orders").unwrap();
    a.engine
        .create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
        .unwrap();
    b.engine.create_collection("shop", "orders").unwrap();
    // Strictly later wall time than A's create, so B's drop sorts after the
    // creation it removes (ADR-132).
    tokio::time::sleep(Duration::from_millis(2)).await;
    (a, b)
}

#[tokio::test]
async fn a_drop_issued_on_a_member_without_the_index_reaches_the_holder() {
    // The finding, over the wire: the drop is recorded on the non-holder,
    // replicates to the holder on the next round, and removes it there.
    let (a, b) = holder_and_a_member_without_the_index().await;
    let dropped = b.engine.drop_index_stamped("shop", "orders", "by_email").unwrap();
    assert!(!dropped.removed, "B held nothing to remove");

    let outcome = sync_once(&a.engine, b.addr, SECRET, None).await.unwrap();
    assert_eq!(outcome.ddl_refused + outcome.ddl_declined, 0, "{outcome:?}");
    assert!(
        a.engine.get_collection("shop", "orders").unwrap().index("by_email").is_none(),
        "the holder dropped it on B's instruction"
    );
    let outcome = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert_eq!(outcome.ddl_refused + outcome.ddl_declined, 0, "{outcome:?}");
    assert!(
        b.engine.get_collection("shop", "orders").unwrap().index("by_email").is_none(),
        "A's create is older than B's tombstone and reads as history"
    );
}

#[tokio::test]
async fn a_drop_pushed_from_a_member_without_the_index_is_applied_by_the_holder() {
    // With ADR-140's confirmation: the drop is pushed to the holder within
    // the request that issued it, so a DELETE through a front removes the
    // index before its response.
    let (a, b) = holder_and_a_member_without_the_index().await;
    let dropped = b.engine.drop_index_stamped("shop", "orders", "by_email").unwrap();
    let entry = b.engine.oplog_entry(&dropped.stamp.unwrap()).unwrap().expect("the drop entry");

    let pushed = push_entry(&b.engine, a.addr, SECRET, &entry).await.unwrap();
    assert_eq!(pushed.unreached, None, "{pushed:?}");
    assert!(pushed.outcome.ddl >= 1, "applied on the holder: {pushed:?}");
    assert_eq!(pushed.outcome.ddl_declined, 0, "{pushed:?}");
    assert!(a.engine.get_collection("shop", "orders").unwrap().index("by_email").is_none());
}

#[tokio::test]
async fn a_pushed_drop_older_than_the_holders_index_is_reported_as_declined() {
    // The residual, reported: the holder's index was created after the
    // drop, so the holder keeps it and says why, which a confirmation reads
    // as a member that did not apply the change.
    let a = node().await;
    let b = node().await;
    a.engine.create_collection("shop", "orders").unwrap();
    b.engine.create_collection("shop", "orders").unwrap();
    let dropped = b.engine.drop_index_stamped("shop", "orders", "by_email").unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    a.engine
        .create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
        .unwrap();
    let entry = b.engine.oplog_entry(&dropped.stamp.unwrap()).unwrap().expect("the drop entry");

    let pushed = push_entry(&b.engine, a.addr, SECRET, &entry).await.unwrap();
    assert_eq!(pushed.outcome.ddl_declined, 1, "declined and reported: {pushed:?}");
    assert!(a.engine.get_collection("shop", "orders").unwrap().index("by_email").is_some());
}

/// Detection to repair (ADR-148), through the real loop. B's witnessed
/// position on A already sits above twenty documents it never applied —
/// the state an upgrade finds a member in after a hole of the kind this
/// ADR closes, and the state `apply_peer_batch` on an exhausted empty
/// window puts it in here. Anti-entropy alone never asks for them again:
/// the position says B has them, and the lag gauge reads 0. The count half
/// of the divergence check confirms the collection against A, the loop
/// plans a replay from the collection's creation on the next round with A,
/// the replay re-serves A's oplog from there, the documents land, and the
/// gauge returns to 0 on the probe after that.
#[tokio::test]
async fn a_confirmed_count_divergence_is_repaired_through_the_real_loop() {
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let a = node().await;
    let b = node().await;

    let mut collections = Vec::new();
    for name in ["alpha", "beta", "gamma"] {
        let c = a.engine.create_collection("shop", name).unwrap();
        a.engine.insert(&c, doc! { "_id": "0" }).unwrap();
        collections.push(c);
    }
    sync(&a, &b).await;
    sync(&a, &b).await;

    let gamma = &collections[2];
    for i in 1..=20 {
        a.engine.insert(gamma, doc! { "_id": i.to_string() }).unwrap();
    }
    let theirs = a.engine.version_vector().unwrap();
    b.engine.apply_peer_batch(&theirs, &[], Hlc::ZERO, true).unwrap();
    let gamma_on_b = b.engine.get_collection("shop", "gamma").unwrap();
    assert_eq!(b.engine.count(&gamma_on_b).unwrap(), 1, "the hole: witnessed, never applied");
    assert!(
        b.engine.witnessed_vector().unwrap().covers(&theirs),
        "and nothing about the position says so"
    );

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![a.addr])], SECRET.into(), b.addr);
    config.sync_interval = Duration::from_millis(50);
    config.discovery_interval = Duration::from_millis(50);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&b.engine), config));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut confirmed_once = false;
    let mut repair_rounds = 0usize;
    let mut cleared = false;
    while !cleared {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the divergence was not confirmed and repaired in time: confirmed={confirmed_once} \
                     repair_rounds={repair_rounds} gamma on b={}",
                    b.engine.count(&gamma_on_b).unwrap()
                )
            })
            .expect("the loop must keep reporting");
        confirmed_once |= report.divergent_collections > 0;
        repair_rounds += report.repair_rounds;
        cleared = confirmed_once && repair_rounds > 0 && report.divergent_collections == 0;
    }
    looping.abort();

    assert_eq!(
        b.engine.count(&gamma_on_b).unwrap(),
        21,
        "the replay re-served the documents the position had claimed"
    );
    assert!(repair_rounds >= 1, "at least one round was spent repairing: {repair_rounds}");
}

/// A member missing a collection its peer holds — the creation witnessed
/// away, the documents for it arriving every round — stops its batches at
/// the first such document, plans a snapshot from that peer, and holds the
/// collection with every document after the next round (ADR-148). Nothing
/// fails, and nothing past the stop is witnessed until then.
#[tokio::test]
async fn a_member_lacking_a_collection_stops_plans_a_snapshot_and_catches_up() {
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let a = node().await;
    let b = node().await;
    let base = a.engine.create_collection("shop", "base").unwrap();
    a.engine.insert(&base, doc! { "_id": "0" }).unwrap();
    sync(&a, &b).await;

    // Created on A, and B's position on A moved *past* the creation without
    // it: the shape a lost creation leaves behind. Past, not at — a window
    // is inclusive at the stamp it is asked from, so a position level with
    // the creation would simply be served it again.
    let late = a.engine.create_collection("shop", "late").unwrap();
    a.engine.insert(&base, doc! { "_id": "past-the-create" }).unwrap();
    let past = a.engine.version_vector().unwrap();
    b.engine.apply_peer_batch(&past, &[], Hlc::ZERO, true).unwrap();
    assert!(b.engine.get_collection("shop", "late").is_err(), "B never applied the creation");

    // The documents written into it afterwards are what every window from
    // A now carries, and B cannot place any of them.
    for i in 0..5 {
        a.engine.insert(&late, doc! { "_id": i }).unwrap();
    }
    a.engine.insert(&base, doc! { "_id": "after" }).unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![a.addr])], SECRET.into(), b.addr);
    config.sync_interval = Duration::from_millis(50);
    config.discovery_interval = Duration::from_millis(50);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&b.engine), config));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut stopped = 0usize;
    let mut repair_rounds = 0usize;
    let mut failed = 0usize;
    loop {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("never caught up: stopped={stopped} repair_rounds={repair_rounds}")
            })
            .expect("the loop must keep reporting");
        stopped += report.entries_skipped_unknown_collection;
        repair_rounds += report.repair_rounds;
        failed += report.failed;
        if let Ok(on_b) = b.engine.get_collection("shop", "late")
            && b.engine.count(&on_b).unwrap() == 5
            && b.engine.witnessed_vector().unwrap().covers(&a.engine.version_vector().unwrap())
        {
            break;
        }
    }
    looping.abort();

    assert!(stopped >= 1, "the batch stopped at the collection B lacks: {stopped}");
    assert!(repair_rounds >= 1, "and a snapshot was pulled for it: {repair_rounds}");
    assert_eq!(failed, 0, "without a round failing");
    let base_on_b = b.engine.get_collection("shop", "base").unwrap();
    assert!(
        b.engine.get(&base_on_b, &DocId::String("after".into())).unwrap().is_some(),
        "the document behind the stop landed too"
    );
}

/// The finding, reduced to two members: a collection dropped here stays
/// dropped while the peer has not applied the drop yet.
///
/// A and B hold the same three thousand documents. A drops the collection;
/// B is not syncing, so it goes on holding — and serving — the incarnation
/// A buried. Every round A runs against B from then on sees a collection
/// the peer holds and this node does not, which is character for character
/// what the existence half of the check reports for a member that has lost
/// one. A must read it as the life it ended rather than as a hole in
/// itself: nothing reported, nothing confirmed, no repair planned, no
/// snapshot pulled. On the cluster this came from, `DELETE /v1/db/shop/coll/bench`
/// answered `200 {"dropped": true}` and 48,128 documents came back on all
/// three members, minutes later, twice.
///
/// The controls are asserted alongside, because a check that had simply
/// stopped running would satisfy the negative half on its own: the check
/// runs on every round of the watch, no round fails, and B still holds all
/// three thousand documents at the end of it — there was something to pull
/// back for the whole time. Then the drop reaches B by the ordinary route
/// and the two members agree about a collection neither of them has.
#[tokio::test]
async fn a_collection_dropped_here_is_not_pulled_back_from_a_peer_that_has_not_applied_the_drop() {
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    /// Checked rounds watched before the drop is called safe. A confirmed
    /// existence finding takes two, and the repair it plans runs on the
    /// third, so this is comfortably past the point the unfixed code has
    /// already pulled the collection back.
    const CHECKS_WATCHED: usize = 8;
    const DOCUMENTS: u64 = 3_000;

    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "bench").unwrap();
    let batch: Vec<_> = (0..DOCUMENTS as i64).map(|n| doc! { "_id": n }).collect();
    a.engine.insert_many(&ca, batch).unwrap();
    // Several times the batch cap, so converging takes a handful of rounds.
    let mut rounds = 0;
    let cb = loop {
        sync(&a, &b).await;
        rounds += 1;
        match b.engine.get_collection("shop", "bench") {
            Ok(cb) if b.engine.count(&cb).unwrap() == DOCUMENTS => break cb,
            _ => assert!(rounds < 10, "B never caught up with A's {DOCUMENTS} documents"),
        }
    };

    assert!(a.engine.drop_collection("shop", "bench").unwrap(), "dropped here");
    let dropped = a.engine.collection_dropped_at(ca.id).unwrap().expect("a tombstone records it");
    assert!(a.engine.get_collection("shop", "bench").is_err());

    // One round on its own first, so the failure is legible before the loop
    // is involved at all: the check ran, and it found nothing.
    let outcome = sync_once(&a.engine, b.addr, SECRET, None).await.unwrap();
    assert_eq!(
        outcome.divergent,
        Some(BTreeSet::new()),
        "B holds the incarnation A dropped, which is not a divergence: {outcome:?}"
    );
    assert_eq!(outcome.applied, 0, "and nothing was pulled: {outcome:?}");

    // And through the real loop, which is what plans a repair.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![b.addr])], SECRET.into(), a.addr);
    config.sync_interval = Duration::from_millis(50);
    config.discovery_interval = Duration::from_millis(50);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&a.engine), config));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut checks = 0usize;
    let mut repair_rounds = 0usize;
    while checks < CHECKS_WATCHED {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the loop stopped reporting after {checks} checked rounds"))
            .expect("the loop must keep reporting");
        checks += report.divergence_checks;
        repair_rounds += report.repair_rounds;
        assert_eq!(
            report.divergent_collections, 0,
            "confirmed divergent after {checks} checks, against a peer that is merely \
             holding what A dropped"
        );
        assert_eq!(repair_rounds, 0, "a repair was planned for a collection A dropped on purpose");
        assert_eq!(report.failed, 0, "{report:?}");
    }
    looping.abort();

    if let Ok(back) = a.engine.get_collection("shop", "bench") {
        panic!(
            "the drop was undone: {} documents are back on A, pulled from a peer that had \
             not applied the drop",
            a.engine.count(&back).unwrap()
        );
    }
    assert_eq!(
        b.engine.count(&cb).unwrap(),
        DOCUMENTS,
        "B held every document throughout, so there was something to pull back all along"
    );

    // The drop reaches B by the ordinary route, and the two members agree
    // about a collection neither of them now has.
    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert!(b.engine.get_collection("shop", "bench").is_err(), "B applied A's drop");
    assert_eq!(
        b.engine.collection_dropped_at(ca.id).unwrap(),
        Some(dropped),
        "at A's stamp, which is what makes the two tombstones the same fact"
    );
    for (from, to) in [(&a, &b), (&b, &a)] {
        let outcome = sync_once(&from.engine, to.addr, SECRET, None).await.unwrap();
        assert_eq!(outcome.divergent, Some(BTreeSet::new()), "nothing left to find: {outcome:?}");
    }
}

/// A repair whose sender drops the collection brings the drop back instead,
/// and the copy standing here goes with it.
///
/// B holds half of A's copy — the shape a repair exists to complete — when A
/// drops the collection. The snapshot A then serves for it carries no
/// definition and no documents, because there are none left to carry, but it
/// does carry the stamp of the drop. B must apply that drop rather than keep
/// what it had accumulated: a member left advertising a collection the
/// cluster has agreed is deleted re-seeds it onto every member that applied
/// the drop, which is the resurrection the tombstone exists to stop.
///
/// The tombstone lands at *A's* stamp rather than B's clock, and mints no
/// entry here — the pair a replicated `DropCollection` writes — so B does
/// not go on to re-broadcast the drop under a later stamp than it has.
///
/// This is the drop as it stands when the repair opens, which rides the
/// snapshot's first page. The drop that lands *between* two pages of a
/// repair already under way is the test that follows this one.
#[tokio::test]
async fn a_snapshot_repair_whose_sender_has_dropped_the_collection_drops_it_here_too() {
    use kimmy_cluster::{PeerStalls, Repair};

    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "bench").unwrap();
    let first: Vec<_> = (0..600i64).map(|n| doc! { "_id": n }).collect();
    a.engine.insert_many(&ca, first).unwrap();
    sync(&a, &b).await;
    let cb = b.engine.get_collection("shop", "bench").unwrap();
    assert_eq!(b.engine.count(&cb).unwrap(), 600);

    // A writes on, and B's position moves past those writes without them:
    // half a copy, and nothing in the position saying so.
    let second: Vec<_> = (600..1_200i64).map(|n| doc! { "_id": n }).collect();
    a.engine.insert_many(&ca, second).unwrap();
    let past = a.engine.version_vector().unwrap();
    b.engine.apply_peer_batch(&past, &[], Hlc::ZERO, true).unwrap();
    assert_eq!(b.engine.count(&cb).unwrap(), 600, "the partial copy the repair is planned for");

    // A drops it while B is repairing from A.
    assert!(a.engine.drop_collection("shop", "bench").unwrap());
    let dropped = a.engine.collection_dropped_at(ca.id).unwrap().expect("A's tombstone");
    let minted = b.engine.version_vector().unwrap().get(b.engine.node_id());

    let mut stalls = PeerStalls::new();
    assert!(stalls.plan_repair(a.engine.node_id(), ca.id, Repair::Snapshot));
    let outcome = sync_once_with(&b.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
    assert!(outcome.repairing, "the round ran the repair: {outcome:?}");
    assert!(!stalls.repairing(a.engine.node_id()), "and finished it: the sender had nothing left");

    if let Ok(kept) = b.engine.get_collection("shop", "bench") {
        panic!(
            "B kept {} documents of a collection A has dropped, and goes on advertising it",
            b.engine.count(&kept).unwrap()
        );
    }
    assert_eq!(
        b.engine.collection_dropped_at(ca.id).unwrap(),
        Some(dropped),
        "the tombstone is at A's stamp, not B's clock"
    );
    assert_eq!(
        b.engine.version_vector().unwrap().get(b.engine.node_id()),
        minted,
        "a replicated drop mints no entry here"
    );
}

/// A byte relay to `target` that closes the connection it is carrying the
/// moment `cut` is signalled — a peer that goes away mid-round. TLS runs end
/// to end through it, so the round it carries is a real round, for the reason
/// `relay_one_connection` gives. The listener stays bound for the life of the
/// test.
///
/// One signal, one connection: a `Notify` with nothing parked on it keeps the
/// permit, so a connection opened after the signal would take it and be cut
/// at once. The round that resumes the pull dials the peer directly, so
/// nothing here needs otherwise.
async fn relay_until_cut(
    target: std::net::SocketAddr,
    cut: Arc<tokio::sync::Notify>,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut inbound, _)) = listener.accept().await {
            let cut = Arc::clone(&cut);
            tokio::spawn(async move {
                let Ok(mut upstream) = TcpStream::connect(target).await else { return };
                tokio::select! {
                    _ = tokio::io::copy_bidirectional(&mut inbound, &mut upstream) => {}
                    _ = cut.notified() => {}
                }
            });
        }
    });
    addr
}

/// The sender dropping the collection **between two pages** of a repair, over
/// two members and the wire.
///
/// B is pulling a scoped snapshot of a collection it does not hold; the round
/// is cut with part of the copy applied and its cursor kept, and A drops the
/// collection before the round that resumes the pull. The page that comes
/// back carries the drop, so B ends with the collection absent, the partial
/// copy it had accumulated gone, and the tombstone at A's stamp.
///
/// Until every page of a scoped snapshot carried the sender's drop, that
/// resumed page came back with no definition, no documents, no drop and no
/// cursor — the same thing a snapshot that had simply run out looks like — so
/// B called the pull complete and went on advertising a partial copy of an
/// incarnation the cluster had agreed to delete, with no tombstone of its own
/// to stop it being served or re-seeded onto the members that applied the
/// drop. That is the window this test defends, and the one nothing else here
/// reaches: the test above it exercises the drop as it stands on the
/// snapshot's *first* page. The resumed page has a test of its own in
/// `kimmy-storage`; this is the same window through the round that pulls it.
#[tokio::test]
async fn a_repair_whose_sender_drops_the_collection_between_pages_discards_the_partial_copy() {
    use kimmy_cluster::{PeerStalls, Repair};
    use kimmy_storage::SNAPSHOT_PAGE;

    // Wide enough that a page lands long before the last one does, so the
    // cut below always finds the pull part-way through.
    const DOCUMENTS: usize = SNAPSHOT_PAGE * 20;

    let a = node().await;
    let b = node().await;
    let ca = a.engine.create_collection("shop", "bench").unwrap();
    let batch: Vec<_> = (0..DOCUMENTS as i64).map(|n| doc! { "_id": n }).collect();
    a.engine.insert_many(&ca, batch).unwrap();

    // B holds none of it, which is what a snapshot repair is for. Nothing of
    // A's oplog reaches B on either round: a repair asks for the snapshot
    // outright rather than for a window.
    let cut = Arc::new(tokio::sync::Notify::new());
    let relayed = relay_until_cut(a.addr, Arc::clone(&cut)).await;
    let mut stalls = PeerStalls::new();
    assert!(stalls.plan_repair(a.engine.node_id(), ca.id, Repair::Snapshot));

    let engine = Arc::clone(&b.engine);
    let round = tokio::spawn(async move {
        let outcome = sync_once_with(&engine, relayed, SECRET, None, &mut stalls).await;
        (outcome, stalls)
    });

    // Cut the round once a page has landed: the pages applied and the cursor
    // saying where they stopped are what the next round resumes from.
    let mut waited = 0;
    while b.engine.count_by_id(ca.id).unwrap().unwrap_or(0) < SNAPSHOT_PAGE as u64 {
        waited += 1;
        assert!(waited < 30_000, "the snapshot's first page never landed on B");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    cut.notify_one();
    let (outcome, mut stalls) = round.await.unwrap();
    assert!(outcome.is_err(), "the round was cut, not completed: {outcome:?}");
    let held = b.engine.count_by_id(ca.id).unwrap().expect("a partial copy stands here");
    assert!(
        held >= SNAPSHOT_PAGE as u64 && held < DOCUMENTS as u64,
        "part of A's copy and not all of it, so there is a pull to resume: {held}"
    );
    assert!(stalls.repairing(a.engine.node_id()), "and the repair is still under way");

    // Between the pages. The round that resumes the pull is the one that
    // finds out.
    assert!(a.engine.drop_collection("shop", "bench").unwrap());
    let dropped = a.engine.collection_dropped_at(ca.id).unwrap().expect("A's tombstone");
    let minted = b.engine.version_vector().unwrap().get(b.engine.node_id());

    let outcome = sync_once_with(&b.engine, a.addr, SECRET, None, &mut stalls).await.unwrap();
    assert!(outcome.repairing, "the resumed round is the repair: {outcome:?}");
    assert!(!stalls.repairing(a.engine.node_id()), "which ended with the drop");

    if let Ok(kept) = b.engine.get_collection("shop", "bench") {
        panic!(
            "B kept {} documents of the incarnation A dropped between pages, and goes on \
             advertising it",
            b.engine.count(&kept).unwrap()
        );
    }
    assert_eq!(b.engine.count_by_id(ca.id).unwrap(), None, "the partial copy went with the drop");
    assert_eq!(
        b.engine.collection_dropped_at(ca.id).unwrap(),
        Some(dropped),
        "the tombstone is at A's stamp, not B's clock"
    );
    assert_eq!(
        b.engine.version_vector().unwrap().get(b.engine.node_id()),
        minted,
        "a replicated drop mints no entry here"
    );
}

/// A collection genuinely recreated on the peer after the drop is still
/// pulled — the tombstone rule subtracts one incarnation, not the name.
///
/// A drops the collection and B applies the drop; B then creates it again,
/// under a stamp later than the drop, and writes into the new life. The
/// entry that carried that creation is gone — collected on B, and A's
/// position has already moved past it — so the only thing that can find the
/// new collection is the check, and the only thing that can bring it is a
/// snapshot. Both must happen: A's tombstone is older than the incarnation
/// B names, so the id is reported, confirmed, and repaired.
///
/// What arrives is the new life and nothing of the old one: the collection
/// stands at *B's* creation stamp rather than A's apply clock — the
/// incarnation A then advertises, and what a replayed drop is judged
/// against — and its documents are the ones written since, with none of the
/// buried life's coming back with them.
#[tokio::test]
async fn a_collection_recreated_on_the_peer_after_the_drop_is_still_pulled() {
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let a = node().await;
    let b = node().await;

    let ca = a.engine.create_collection("shop", "bench").unwrap();
    let old: Vec<_> = (0..10).map(|n| doc! { "_id": format!("old-{n}") }).collect();
    a.engine.insert_many(&ca, old).unwrap();
    sync(&a, &b).await;
    assert_eq!(b.engine.count(&b.engine.get_collection("shop", "bench").unwrap()).unwrap(), 10);

    // The drop, applied on both members.
    assert!(a.engine.drop_collection("shop", "bench").unwrap());
    let dropped = a.engine.collection_dropped_at(ca.id).unwrap().expect("A's tombstone");
    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert!(b.engine.get_collection("shop", "bench").is_err(), "B applied the drop");

    // And a new life for the name, on B, once B's drop purger has removed what
    // the first one held (ADR-189).
    b.engine.finish_purges_now().unwrap();
    // A's too, or A's pull of the recreation waits for it at the creation.
    a.engine.finish_purges_now().unwrap();
    let recreated = b.engine.create_collection("shop", "bench").unwrap();
    assert!(recreated.created > dropped.hlc, "the recreation is later than the drop it follows");
    let new: Vec<_> = (0..10).map(|n| doc! { "_id": format!("new-{n}") }).collect();
    b.engine.insert_many(&recreated, new).unwrap();

    // A's position has moved past the creation without it, and B has since
    // collected the history that carried it: the entries path can never
    // bring this collection, whichever member asks.
    let past = b.engine.version_vector().unwrap();
    a.engine.apply_peer_batch(&past, &[], Hlc::ZERO, true).unwrap();
    b.engine
        .collect_garbage_at(
            kimmy_storage::physical_now_ms() + 1_000_000_000,
            kimmy_storage::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();
    assert!(a.engine.get_collection("shop", "bench").is_err(), "A knows nothing of the new life");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![b.addr])], SECRET.into(), a.addr);
    config.sync_interval = Duration::from_millis(50);
    config.discovery_interval = Duration::from_millis(50);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&a.engine), config));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut reported = false;
    let mut repair_rounds = 0usize;
    let restored = loop {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the recreation was never pulled: reported={reported} \
                     repair_rounds={repair_rounds}"
                )
            })
            .expect("the loop must keep reporting");
        reported |= report.divergent_collections > 0;
        repair_rounds += report.repair_rounds;
        if let Ok(on_a) = a.engine.get_collection("shop", "bench")
            && a.engine.count(&on_a).unwrap() == 10
        {
            break on_a;
        }
    };
    looping.abort();

    assert!(reported, "the check reported the id A holds a tombstone for");
    assert!(repair_rounds >= 1, "and a round was spent repairing it: {repair_rounds}");
    assert_eq!(
        restored.created, recreated.created,
        "restored under B's creation stamp, not A's apply clock"
    );
    for n in 0..10 {
        let id = DocId::String(format!("new-{n}"));
        assert!(a.engine.get(&restored, &id).unwrap().is_some(), "the new life's documents");
        let buried = DocId::String(format!("old-{n}"));
        assert!(a.engine.get(&restored, &buried).unwrap().is_none(), "and none of the old one's");
    }
}

#[tokio::test]
async fn a_delete_crosses_the_wire_in_a_snapshot_to_a_member_that_held_the_document() {
    // ADR-167 over the real protocol: a page's deletes must survive BSON and
    // the transport. C holds the document, A deletes it and collects its
    // oplog, so C -- below A's horizon -- is served a snapshot, and the delete
    // must reach it there.
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": 1 }).unwrap();
    a.engine.insert(&ca, doc! { "_id": 2 }).unwrap();

    let c = node().await;
    sync_once(&c.engine, a.addr, SECRET, None).await.unwrap();
    let cc = c.engine.get_collection("shop", "orders").unwrap();
    assert_eq!(c.engine.count(&cc).unwrap(), 2, "C must hold the document first");

    assert!(a.engine.delete(&ca, &DocId::Int64(2)).unwrap());
    // Retention keeps an origin's newest entry, so a later write stands in
    // front of the delete before A collects a day behind it.
    let later = kimmy_storage::physical_now_ms() + 36 * HOUR_MS;
    a.engine.apply_batch(&[entry_stamped(ca.id, later)]).unwrap();
    a.engine
        .collect_garbage_at(
            later + HOUR_MS,
            kimmy_storage::RetentionPolicy::new(DAY_SECS, u64::MAX),
        )
        .unwrap();
    assert!(
        !a.engine.can_serve_peer_holding(&c.engine.version_vector().unwrap()).unwrap(),
        "the fixture must put C below A's horizon, or this is the entries route"
    );

    sync_once(&c.engine, a.addr, SECRET, None).await.expect("C catches up from A by snapshot");
    assert!(c.engine.get(&cc, &DocId::Int64(2)).unwrap().is_none(), "the delete must travel");
    assert!(c.engine.get(&cc, &DocId::Int64(1)).unwrap().is_some());
    assert!(
        c.engine.get(&cc, &DocId::String("clock".into())).unwrap().is_some(),
        "the snapshot served"
    );
}

#[tokio::test]
async fn a_delete_does_not_yet_reach_a_member_through_one_that_never_held_the_document() {
    // A KNOWN GAP, pinned over the wire so a change to it is noticed. C holds
    // the document; A deletes it; B, which never held it, catches up from A by
    // snapshot and records nothing; C then catches up from B by snapshot, and
    // B's walk has no tombstone to carry, so C keeps the document. Tracked by
    // the plan "a delete relayed through a member that never held the document
    // does not travel", whose first step is to flip the final assertion.
    //
    // Both catch-ups are snapshots, which takes arranging: a member is served
    // one only when its peer has collected an entry it lacks.
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": 1 }).unwrap();
    a.engine.insert(&ca, doc! { "_id": 2 }).unwrap();

    let c = node().await;
    sync_once(&c.engine, a.addr, SECRET, None).await.unwrap();
    let cc = c.engine.get_collection("shop", "orders").unwrap();

    assert!(a.engine.delete(&ca, &DocId::Int64(2)).unwrap());
    a.engine
        .collect_garbage_at(
            kimmy_storage::physical_now_ms() + 1_000_000_000,
            kimmy_storage::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();

    let b = node().await;
    sync_once(&b.engine, a.addr, SECRET, None).await.expect("B catches up from A by snapshot");
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    assert!(b.engine.get(&cb, &DocId::Int64(2)).unwrap().is_none());

    b.engine.insert(&cb, doc! { "_id": 3 }).unwrap();
    let later = kimmy_storage::physical_now_ms() + 36 * HOUR_MS;
    b.engine.apply_batch(&[entry_stamped(cb.id, later)]).unwrap();
    b.engine
        .collect_garbage_at(
            later + HOUR_MS,
            kimmy_storage::RetentionPolicy::new(DAY_SECS, u64::MAX),
        )
        .unwrap();
    assert!(
        !b.engine.can_serve_peer_holding(&c.engine.version_vector().unwrap()).unwrap(),
        "the fixture must put C below B's horizon, or this is the entries route"
    );

    sync_once(&c.engine, b.addr, SECRET, None).await.expect("C catches up from B by snapshot");
    assert!(
        c.engine.get(&cc, &DocId::Int64(3)).unwrap().is_some(),
        "the snapshot must have served"
    );
    assert!(
        c.engine.get(&cc, &DocId::Int64(2)).unwrap().is_some(),
        "the known gap: C keeps the document -- if this now fails, the gap closed; update the plan"
    );
}

// -----------------------------------------------------------------------
// The count half on both sides (ADR-168)
// -----------------------------------------------------------------------

/// A probe built the way `kimmy-cluster::peers` builds one: this node's
/// witnessed vector first, then its count.
fn probe_of(engine: &Engine, id: kimmy_core::CollectionId) -> Option<DivergenceProbe> {
    let mine_at = Some(engine.witnessed_vector().unwrap());
    let mine_count = engine.count_by_id(id).unwrap();
    Some(DivergenceProbe { id, mine_count, mine_at })
}

#[tokio::test]
async fn a_member_draining_its_peers_writes_does_not_compare_a_count_it_read_before_its_pull() {
    // The defect: a member's count is read once per tick, before its pulls,
    // and the peer's when the probe reaches it, after them. A replica taking
    // in an origin's steady writes compared the two, found its own lag, and
    // after two such contacts confirmed a divergence and ran a repair for
    // it. The gate deferred only when the PEER trailed.
    let a = node().await;
    let b = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": 0 }).unwrap();
    sync(&a, &b).await;
    sync(&a, &b).await;

    let mut stalls = kimmy_cluster::transport::PeerStalls::new();
    for round in 1..=6i64 {
        for i in 0..20 {
            a.engine.insert(&ca, doc! { "_id": round * 100 + i }).unwrap();
        }
        let probe = probe_of(&b.engine, ca.id);
        let outcome = sync_once_with(&b.engine, a.addr, SECRET, probe, &mut stalls).await.unwrap();
        assert!(outcome.divergent.is_some(), "round {round}: the check must have run: {outcome:?}");
        assert_eq!(outcome.count_probe, None, "round {round}: nothing compared: {outcome:?}");
        assert!(outcome.count_probe_deferred, "round {round}: deferred, and counted as such");
    }

    // The writes stop and the replica catches up: its next count is compared,
    // and agrees.
    let probe = probe_of(&b.engine, ca.id);
    let outcome = sync_once_with(&b.engine, a.addr, SECRET, probe, &mut stalls).await.unwrap();
    assert_eq!(outcome.count_probe, Some((ca.id, false)), "compared, and equal: {outcome:?}");
}

#[tokio::test]
async fn a_self_side_held_still_is_compared_and_its_real_difference_found() {
    // The self side's standing-still rule, which is defensive: the sync loop
    // re-reads this node's vector every tick and a checked contact leaves it
    // covering the peer, so the loop never presents the same trailing position
    // twice -- a wedged member is caught by its PEERS' side (ADR-145). This
    // test holds the probe's vector and count still across contacts itself,
    // the one way to reach the rule, and asserts it compares rather than
    // defers for ever: after FROZEN_CONTACTS deferred contacts the count is
    // compared and the difference found.
    let a = node().await;
    let b = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": 1 }).unwrap();
    sync(&a, &b).await;
    sync(&a, &b).await;

    let stuck = probe_of(&b.engine, ca.id);
    a.engine.insert(&ca, doc! { "_id": 2 }).unwrap();

    let mut stalls = kimmy_cluster::transport::PeerStalls::new();
    for contact in 1..=kimmy_cluster::FROZEN_CONTACTS {
        let outcome =
            sync_once_with(&b.engine, a.addr, SECRET, stuck.clone(), &mut stalls).await.unwrap();
        assert!(outcome.count_probe_deferred, "contact {contact}: not yet still: {outcome:?}");
    }
    for contact in 0..2 {
        let outcome =
            sync_once_with(&b.engine, a.addr, SECRET, stuck.clone(), &mut stalls).await.unwrap();
        assert_eq!(
            outcome.count_probe,
            Some((ca.id, true)),
            "still for FROZEN_CONTACTS: compared, and the difference found ({contact}): {outcome:?}"
        );
    }
}

#[tokio::test]
async fn while_one_member_keeps_writing_the_count_half_against_it_stays_quiet() {
    // Pinned, because it is deliberate and must not be discovered: it takes no
    // bulk load. One document a round on one member keeps every contact against
    // it deferred -- this node trails it on its origin and moves -- so no count
    // is compared against that member while it writes. The gate before ADR-168
    // compared here, and found a mismatch, on the first round: this is the
    // consequence of the change, not behaviour it inherited.
    let a = node().await;
    let b = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    sync(&a, &b).await;
    sync(&a, &b).await;

    let mut stalls = kimmy_cluster::transport::PeerStalls::new();
    for round in 0..12i64 {
        a.engine.insert(&ca, doc! { "_id": round }).unwrap();
        let probe = probe_of(&b.engine, ca.id);
        let outcome = sync_once_with(&b.engine, a.addr, SECRET, probe, &mut stalls).await.unwrap();
        assert_eq!(outcome.count_probe, None, "round {round}: {outcome:?}");
        assert!(outcome.count_probe_deferred, "round {round}: deferred, not skipped");
    }
}

// Where a pull's time goes (ADR-175)
//
// Each series is proven by making the one thing it measures slow and seeing
// that series, and not its neighbours, move. A timing that reads small is the
// failure this guards against: it is indistinguishable from a healthy one.

/// How long a slowed phase must read, and how long the others may: far
/// enough apart that a loaded machine does not blur them.
const SLOW: Duration = Duration::from_millis(400);
const FLOOR: Duration = Duration::from_millis(350);

/// A plain TCP relay to `target` that holds every chunk the far side sends
/// back for `delay` before passing it on. The TLS inside is untouched: it is
/// a slow wire, not a party to the conversation.
async fn slow_relay(target: std::net::SocketAddr, delay: Duration) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(server) = TcpStream::connect(target).await else { return };
                let (mut cr, mut cw) = client.into_split();
                let (mut sr, mut sw) = server.into_split();
                let up = async { tokio::io::copy(&mut cr, &mut sw).await.map(drop) };
                let down = async {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = vec![0u8; 64 * 1024];
                    loop {
                        let n = sr.read(&mut buf).await?;
                        if n == 0 {
                            return Ok::<(), std::io::Error>(());
                        }
                        tokio::time::sleep(delay).await;
                        cw.write_all(&buf[..n]).await?;
                    }
                };
                let _ = tokio::join!(up, down);
            });
        }
    });
    addr
}

/// A source with `n` documents written, and an empty member to pull them.
async fn a_source_holding(n: usize) -> (Node, Node) {
    let a = node().await;
    let b = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert_many(&ca, (0..n).map(|i| doc! { "_id": i as i64 }).collect()).unwrap();
    (a, b)
}

/// One pull into `into` from `peer`, and its timing as the replication loop
/// reads it: from the `PeerStalls` the round left it on, the only place a
/// pull's timing is carried (ADR-175).
async fn pull_timed(
    into: &Engine,
    peer: std::net::SocketAddr,
) -> (kimmy_storage::SyncOutcome, kimmy_storage::PullTiming) {
    let mut stalls = PeerStalls::new();
    let outcome = sync_once_with(into, peer, SECRET, None, &mut stalls).await.expect("pull");
    let pull = stalls.take_pull().expect("a window was pulled");
    (outcome, pull)
}

#[tokio::test]
async fn a_pull_that_waits_for_the_writer_says_so_apart_from_applying() {
    let (a, b) = a_source_holding(50).await;

    // Something else holds the puller's writer for twice `SLOW` from before
    // the pull starts, so whatever the handshake costs, the batch still
    // queues for well past the floor.
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let engine = Arc::clone(&b.engine);
    let holder = std::thread::spawn(move || {
        let _hold = engine.hold_writer(kimmy_storage::WriterHolder::Bulk);
        held_tx.send(()).unwrap();
        std::thread::sleep(2 * SLOW);
    });
    held_rx.recv().unwrap();

    let (_, pull) = pull_timed(&b.engine, a.addr).await;
    holder.join().unwrap();

    assert!(pull.wait >= FLOOR, "the wait behind the writer is the wait phase: {pull:?}");
    assert!(pull.apply < FLOOR, "and is not also counted as applying: {pull:?}");
    assert!(pull.serve < FLOOR, "nor as serving: {pull:?}");
    assert_eq!(pull.entries, 51, "the creation and the fifty documents: {pull:?}");
}

#[tokio::test]
async fn a_pull_whose_commit_is_slow_says_so_in_apply() {
    let (a, b) = a_source_holding(50).await;
    // Every commit on the puller waits `SLOW` for the shared flush: work
    // the batch does after taking the writer, not a wait for it.
    b.engine.set_durability(kimmy_storage::DurabilityClass::Coalesced, SLOW);

    let (_, pull) = pull_timed(&b.engine, a.addr).await;

    assert!(pull.apply >= FLOOR, "a slow commit is the apply phase: {pull:?}");
    assert!(pull.wait < FLOOR, "not a wait for the writer: {pull:?}");
    assert!(pull.serve < FLOOR, "nor serving: {pull:?}");
}

#[tokio::test]
async fn a_pull_from_a_slow_peer_says_so_in_serve() {
    let (a, b) = a_source_holding(50).await;
    let slow = slow_relay(a.addr, SLOW).await;

    let (_, pull) = pull_timed(&b.engine, slow).await;

    assert!(pull.serve >= FLOOR, "a slow answer is the serve phase: {pull:?}");
    assert!(pull.wait < FLOOR, "{pull:?}");
    assert!(pull.apply < FLOOR, "{pull:?}");
}

#[tokio::test]
async fn a_pull_says_how_long_the_oldest_entry_it_lacked_had_waited() {
    let (a, b) = a_source_holding(10).await;
    tokio::time::sleep(SLOW).await;

    let (_, pull) = pull_timed(&b.engine, a.addr).await;
    match pull.oldest_lacked {
        Some(kimmy_storage::EntryWait::Waited(waited)) => {
            assert!(waited >= FLOOR, "written {SLOW:?} before the pull: {pull:?}")
        }
        other => panic!("the oldest lacked entry's wait was not taken: {other:?}"),
    }

    // Fresh writes, pulled at once, read a wait far shorter: the reading is
    // the entries' age, not something the round always adds.
    let ca = a.engine.get_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": "fresh" }).unwrap();
    let (_, pull) = pull_timed(&b.engine, a.addr).await;
    match pull.oldest_lacked {
        Some(kimmy_storage::EntryWait::Waited(waited)) => {
            assert!(waited < FLOOR, "written just before the pull: {pull:?}")
        }
        other => panic!("the fresh entry's wait was not taken: {other:?}"),
    }
    assert_eq!(pull.entries, 1, "only what it lacked was served: {pull:?}");
}

/// Run the replication loop on `into` against `peer` until a tick reports
/// what `done` is waiting for, and return every report and lag it saw.
async fn loop_until(
    into: &Node,
    peer: std::net::SocketAddr,
    interval: Duration,
    done: impl Fn(&kimmy_cluster::PullReport) -> bool,
) -> (kimmy_cluster::PullReport, Vec<u64>) {
    use kimmy_cluster::{ReplicationConfig, RoundReport, SeedSource, replicate};

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RoundReport>();
    let (lag_tx, mut lag_rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
    let mut config =
        ReplicationConfig::new(vec![SeedSource::Static(vec![peer])], SECRET.into(), into.addr);
    config.sync_interval = interval;
    config.discovery_interval = Duration::from_millis(50);
    config.on_round = Some(Arc::new(move |report| {
        let _ = tx.send(report);
    }));
    config.on_lag = Some(Arc::new(move |lag| {
        let _ = lag_tx.send(lag);
    }));
    let looping = tokio::spawn(replicate(Arc::clone(&into.engine), config));

    let mut seen = kimmy_cluster::PullReport::default();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !done(&seen) {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("no tick reported it in time; saw {seen:?}"))
            .expect("the loop must keep reporting");
        seen.add(&report.pulls);
    }
    looping.abort();
    let mut lags = Vec::new();
    while let Ok(lag) = lag_rx.try_recv() {
        lags.push(lag);
    }
    (seen, lags)
}

#[tokio::test]
async fn a_drain_the_tick_cannot_finish_is_counted_as_ended_by_the_budget() {
    use kimmy_cluster::ContactEnd;

    // Three full windows behind a peer whose every answer takes `SLOW`: the
    // first pull alone outlasts a one-second tick, so the contact ends with
    // the window truncated and the next pull not started.
    let (a, b) = a_source_holding(3 * kimmy_cluster::protocol::MAX_BATCH).await;
    let slow = slow_relay(a.addr, SLOW).await;

    let (seen, lags) = loop_until(&b, slow, Duration::from_secs(1), |seen| {
        seen.contacts[ContactEnd::Budget.slot()] >= 1
    })
    .await;

    assert_eq!(seen.contacts[ContactEnd::Ceiling.slot()], 0, "{seen:?}");
    assert_eq!(seen.contacts[ContactEnd::Failed.slot()], 0, "{seen:?}");
    assert!(seen.serve.sum_us >= FLOOR.as_micros() as u64, "the tick went to serving: {seen:?}");
    // The lag the budget left, in milliseconds: the entries were written
    // before a pull that took longer than a second, so it is past a
    // thousand, which a reading in whole seconds could not be.
    assert!(
        lags.iter().any(|&lag| (1_000..600_000).contains(&lag)),
        "a truncated contact leaves a lag, reported in milliseconds: {lags:?}"
    );
}

#[tokio::test]
async fn a_drain_the_tick_finishes_is_counted_as_caught_up() {
    use kimmy_cluster::ContactEnd;

    // The same backlog from a peer that answers at once is drained inside a
    // generous tick, and no contact ends any other way.
    let (a, b) = a_source_holding(3 * kimmy_cluster::protocol::MAX_BATCH).await;

    let (seen, _) = loop_until(&b, a.addr, Duration::from_secs(5), |seen| {
        seen.contacts[ContactEnd::CaughtUp.slot()] >= 1
    })
    .await;

    assert_eq!(seen.contacts[ContactEnd::Budget.slot()], 0, "{seen:?}");
    assert_eq!(seen.contacts[ContactEnd::Ceiling.slot()], 0, "{seen:?}");
    assert_eq!(seen.contacts[ContactEnd::Failed.slot()], 0, "{seen:?}");
    assert!(seen.serve.count >= 3, "three windows, each a pull: {seen:?}");
    assert_eq!(seen.entries, 3 * kimmy_cluster::protocol::MAX_BATCH as u64 + 1, "{seen:?}");
}

#[tokio::test]
async fn an_entry_served_below_the_members_position_is_not_read_as_a_wait() {
    // A span held as state is served below the member's position (ADR-172):
    // an entry it already covers, however old, is not one it was waiting for,
    // and reading its age as a wait would put a repair's history into the
    // series meant for the time entries queue.
    let (a, r, _ca, _s) = a_member_holding_a_repaired_entry_below_its_position().await;

    let (outcome, pull) = pull_timed(&r.engine, a.addr).await;
    assert_eq!(outcome.superseded, 1, "the held entry alone is served: {outcome:?}");
    assert_eq!(pull.entries, 1, "{pull:?}");
    assert_eq!(pull.oldest_lacked, None, "nothing served was lacked: {pull:?}");
}

/// A plain TCP relay to `target` that passes one round through until the peer
/// has sent back `served` bytes — a window — and then closes the connection
/// at this member's next request. The round applies the window and fails on
/// what comes after it, the divergence check.
async fn relay_cut_after_a_window(
    target: std::net::SocketAddr,
    served: usize,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let Ok(server) = TcpStream::connect(target).await else { return };
                let (mut cr, mut cw) = client.into_split();
                let (mut sr, mut sw) = server.into_split();
                let sent_back = Arc::new(AtomicUsize::new(0));
                let up = {
                    let sent_back = Arc::clone(&sent_back);
                    async move {
                        let mut buf = vec![0u8; 64 * 1024];
                        loop {
                            let n = cr.read(&mut buf).await.unwrap_or(0);
                            if n == 0 || sent_back.load(Ordering::SeqCst) >= served {
                                return;
                            }
                            if sw.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    }
                };
                let down = async {
                    let mut buf = vec![0u8; 64 * 1024];
                    loop {
                        let n = sr.read(&mut buf).await.unwrap_or(0);
                        if n == 0 || cw.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                        sent_back.fetch_add(n, Ordering::SeqCst);
                    }
                };
                // Whichever side stops first ends the connection both ways.
                tokio::select! { _ = up => {}, _ = down => {} }
            });
        }
    });
    addr
}

#[tokio::test]
async fn a_window_applied_before_its_round_fails_is_still_a_pull() {
    use kimmy_cluster::ContactEnd;

    // Fifty documents of 4 KiB each, so the window is far past the handshake
    // in size and the cut lands after it. The round commits the window, then
    // fails asking the peer for the divergence check.
    let a = node().await;
    let b = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    let body = "x".repeat(4 * 1024);
    a.engine
        .insert_many(&ca, (0..50).map(|i| doc! { "_id": i as i64, "body": body.clone() }).collect())
        .unwrap();
    let cut = relay_cut_after_a_window(a.addr, 64 * 1024).await;

    let (seen, _) = loop_until(&b, cut, Duration::from_secs(5), |seen| {
        seen.contacts[ContactEnd::Failed.slot()] >= 1
    })
    .await;

    let cb = b.engine.get_collection("shop", "orders").expect("the window was applied");
    assert_eq!(b.engine.count(&cb).unwrap(), 50, "and committed before the round failed");
    assert!(seen.serve.count >= 1, "the applied window is a pull: {seen:?}");
    assert!(seen.entries >= 51, "carrying what it applied: {seen:?}");
}

// ---------------------------------------------------------------------------
// ADR-180: a member that caught up by snapshot serves onward the index
// definitions it restored. `m` holds a prefix of `a`'s history and is behind
// the change; `a`'s oplog is then aged out entirely, so `p` joining afresh can
// only catch up by snapshot -- state, with no entry behind it until this
// record. `m` then syncs from `p` alone, through real rounds.
// ---------------------------------------------------------------------------

/// Age `engine`'s oplog out completely, so a peer behind it must snapshot.
fn age_out_fully(engine: &Engine) {
    engine
        .collect_garbage_at(
            kimmy_storage::physical_now_ms() + 1_000_000_000,
            kimmy_storage::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();
}

/// `a` with `shop.orders` and a document `m` has already pulled; then the
/// index `by_x` and a second document, which `m` has not.
/// Whether `engine` still holds the entry for `a`'s index `by_x`: without
/// it, a member that lacks the index can only have it from `engine` by
/// snapshot.
fn holds_the_index_entry(engine: &Engine, a: &Engine) -> bool {
    let stamp = a.get_collection("shop", "orders").unwrap().index("by_x").unwrap().created;
    engine.oplog_entry(&stamp.unwrap()).unwrap().is_some()
}

async fn origin_ahead_of_m_by_an_index() -> (Node, Node) {
    let a = node().await;
    let ca = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&ca, doc! { "_id": 1, "x": 1 }).unwrap();
    let m = node().await;
    sync_once(&m.engine, a.addr, SECRET, None).await.unwrap();

    a.engine.create_index("shop", "orders", vec![field("x")], false, Some("by_x".into())).unwrap();
    a.engine.insert(&ca, doc! { "_id": 2, "x": 2 }).unwrap();
    (a, m)
}

#[tokio::test]
async fn an_index_restored_by_snapshot_relays_to_a_member_that_holds_the_collection() {
    // The reproduction. Before ADR-180 `p` served `m` a window with the
    // document after the index and not the index, `m` witnessed past it, and
    // nothing ever served it again.
    let (a, m) = origin_ahead_of_m_by_an_index().await;
    age_out_fully(&a.engine);
    assert!(
        !holds_the_index_entry(&a.engine, &a.engine),
        "premise: p can only have it by snapshot"
    );

    let p = node().await;
    sync_once(&p.engine, a.addr, SECRET, None).await.unwrap();
    sync_once(&m.engine, p.addr, SECRET, None).await.unwrap();

    let held = m.engine.get_collection("shop", "orders").unwrap();
    assert_eq!(m.engine.count(&held).unwrap(), 2, "m took the window: {:?}", held.indexes);
    assert!(held.index("by_x").is_some(), "and the index in it: {:?}", held.indexes);
    assert_eq!(p.engine.ddl_relogged(), 1, "counted where p re-logged it");
}

#[tokio::test]
async fn control_the_same_relay_without_the_snapshot_hop() {
    // Same topology, `a`'s oplog left intact, so `p` catches up by entries.
    // Passing on main as well: the relay is sound, and the hop is the whole
    // difference.
    let (a, m) = origin_ahead_of_m_by_an_index().await;

    assert!(holds_the_index_entry(&a.engine, &a.engine), "premise: p can have it by entries");
    let p = node().await;
    sync_once(&p.engine, a.addr, SECRET, None).await.unwrap();
    sync_once(&m.engine, p.addr, SECRET, None).await.unwrap();

    let held = m.engine.get_collection("shop", "orders").unwrap();
    assert!(held.index("by_x").is_some(), "control: {:?}", held.indexes);
}

#[tokio::test]
async fn an_index_relays_through_two_snapshot_hops() {
    // `q` catches up from `p` by snapshot too, so what `q` serves `m` is a
    // re-log of a re-log -- at `a`'s stamp, never at `p`'s.
    let (a, m) = origin_ahead_of_m_by_an_index().await;
    let stamp = a.engine.get_collection("shop", "orders").unwrap().index("by_x").unwrap().created;
    age_out_fully(&a.engine);

    let p = node().await;
    sync_once(&p.engine, a.addr, SECRET, None).await.unwrap();
    age_out_fully(&p.engine);
    assert!(
        !holds_the_index_entry(&p.engine, &a.engine),
        "premise: q can only have it by snapshot"
    );
    let q = node().await;
    sync_once(&q.engine, p.addr, SECRET, None).await.unwrap();

    sync_once(&m.engine, q.addr, SECRET, None).await.unwrap();
    let held = m.engine.get_collection("shop", "orders").unwrap();
    assert!(held.index("by_x").is_some(), "m learned the index from q: {:?}", held.indexes);
    let entry = q.engine.oplog_entry(&stamp.unwrap()).unwrap().expect("q holds the entry");
    assert_eq!(entry.stamp.node, a.engine.node_id(), "at a's stamp, not p's");
}

// -----------------------------------------------------------------------
// A large collection drop does not stop a member's replication (ADR-189)

/// Poll `done` every 10 ms for up to ten seconds; one that never holds fails
/// the test rather than hanging it.
async fn within_ten_seconds(what: &str, done: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(std::time::Instant::now() < deadline, "{what} did not happen within ten seconds");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A member applying a peer's drop of a collection larger than a chunk goes
/// on pulling the peer's other databases while the rows are removed. The
/// member's purge is held at its gate for the whole round, so the round can
/// only return if applying the drop does not wait for the purge. Before
/// ADR-189 the apply ran the purge, the round blocked at the gate, and every
/// database waited with it.
///
/// The round runs on a thread of its own, joined with a timeout: an apply
/// that never yields cannot be timed out from inside, so a regression fails
/// here at ten seconds rather than hanging the suite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_members_pulls_of_another_database_go_on_while_a_large_replicated_drop_is_purged() {
    let a = node().await;
    let b = node().await;
    let big = a.engine.create_collection("big", "c").unwrap();
    let rows = kimmy_storage::engine::DROP_PURGE_CHUNK * 2 + 7;
    a.engine.insert_many(&big, (0..rows).map(|i| doc! { "_id": i as i64 }).collect()).unwrap();
    let other = a.engine.create_collection("other", "c").unwrap();
    a.engine.insert(&other, doc! { "_id": "before" }).unwrap();
    // A round carries a capped window, so B catches up over several.
    for _ in 0..100 {
        if sync_once(&b.engine, a.addr, SECRET, None).await.unwrap().exhausted {
            break;
        }
    }
    assert!(b.engine.rows_under(big.id).unwrap() >= rows, "B holds the big collection");

    b.engine.close_purge_gate();
    assert!(a.engine.drop_collection("big", "c").unwrap());
    a.engine.insert(&other, doc! { "_id": "after" }).unwrap();

    let (answered, answer) = std::sync::mpsc::channel();
    let (engine, peer) = (Arc::clone(&b.engine), a.addr);
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _ = answered.send(runtime.block_on(sync_once(&engine, peer, SECRET, None)));
    });
    let round = answer
        .recv_timeout(Duration::from_secs(10))
        .expect("the round returned while B's purge was held");
    round.expect("the round succeeds");

    let other_here = b.engine.get_collection("other", "c").unwrap();
    assert!(
        b.engine.get(&other_here, &DocId::String("after".into())).unwrap().is_some(),
        "the other database's write arrived in the same round"
    );
    assert!(b.engine.get_collection("big", "c").is_err(), "the drop applied");
    assert!(b.engine.rows_under(big.id).unwrap() > 0, "and its rows wait for the purger");

    b.engine.open_purge_gate();
    let purger = tokio::spawn(Arc::clone(&b.engine).run_drop_purger());
    within_ten_seconds("B's purge", || b.engine.rows_under(big.id).unwrap() == 0).await;
    purger.abort();
}

/// A pushed creation of a name whose earlier collection the receiver is
/// still purging is not applied, and the receiver says so in its reply
/// (`Pushed::purge_pending`, ADR-189), so the pusher reports the member as
/// pending rather than as refusing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_stops_at_a_creation_waiting_for_the_receivers_purge_and_says_so() {
    let a = node().await;
    let b = node().await;
    let first = a.engine.create_collection("shop", "orders").unwrap();
    for i in 0..5i64 {
        a.engine.insert(&first, doc! { "_id": i }).unwrap();
    }
    assert!(a.engine.drop_collection("shop", "orders").unwrap());
    // B takes the first life and its drop; with no purger running on B, the
    // rows stay owed.
    sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    assert!(b.engine.get_collection("shop", "orders").is_err());
    assert!(b.engine.rows_under(first.id).unwrap() > 0, "B still owes the first life's rows");

    a.engine.finish_purges_now().unwrap();
    a.engine.create_collection("shop", "orders").unwrap();
    let entry = a
        .engine
        .entries_for_peer(Hlc::ZERO, 1_024)
        .unwrap()
        .entries
        .into_iter()
        .last()
        .expect("the recreation's entry");
    assert_eq!(entry.kind, kimmy_core::OpKind::CreateCollection);

    let pushed = push_entry(&a.engine, b.addr, SECRET, &entry).await.expect("the push is answered");
    assert_eq!(pushed.outcome.purge_pending, 1, "{pushed:?}");
    assert_eq!(pushed.outcome.ddl, 0, "nothing was created: {pushed:?}");
    assert_eq!(pushed.outcome.unknown_collection, 0, "nor is anything missing: {pushed:?}");
    assert!(b.engine.get_collection("shop", "orders").is_err());

    b.engine.finish_purges_now().unwrap();
    let pushed = push_entry(&a.engine, b.addr, SECRET, &entry).await.unwrap();
    assert_eq!(pushed.outcome.purge_pending, 0, "{pushed:?}");
    assert!(b.engine.get_collection("shop", "orders").is_ok(), "created once the purge is done");
}
