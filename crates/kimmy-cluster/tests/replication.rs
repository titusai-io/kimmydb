//! Replication over real TCP sockets.
//!
//! The convergence rules are already tested between engines in one process
//! (`kimmy-storage/src/sync.rs`). These tests exist for what that could not
//! reach: that the wire carries the types faithfully, that the handshake
//! actually gates access, and that a listener survives a peer misbehaving.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

    let entries = a.engine.entries_for_peer(kimmy_core::Hlc::ZERO, 10).unwrap();
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
        sync_once(&b.engine, a.addr, SECRET).await.expect("b should pull from a");
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
    let first = sync_once(&b.engine, a.addr, SECRET).await.expect("the snapshot must encode");
    assert_eq!(first.applied, 50);
    let cb = b.engine.get_collection("shop", &name).expect("the collection must arrive");
    assert_eq!(b.engine.count(&cb).unwrap(), 50);

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
    let err = sync_once(&b.engine, mitm_addr, SECRET)
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
    sync_once(&b.engine, a.addr, SECRET).await.expect("a direct round must succeed");

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
    let result = tokio::time::timeout(Duration::from_secs(30), sync_once(&b.engine, addr, SECRET))
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
    let result = tokio::time::timeout(Duration::from_secs(60), sync_once(&b.engine, addr, SECRET))
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
        provider: kimmy_core::ProviderConfig::Byo,
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
    sync_once(&b.engine, a.addr, SECRET).await.expect("the create must replicate");

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

    sync_once(&b.engine, a.addr, SECRET)
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
    sync_once(&b.engine, a.addr, SECRET).await.expect("the round must succeed");

    assert!(
        b.engine.get_collection("shelf", "doomed").is_err(),
        "a collection dropped on the receiver must not be recreated by replication"
    );
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
        let raw = kimmy_storage::lag_behind_ms(
            &peer.engine.version_vector().unwrap(),
            &a.engine.witnessed_vector().unwrap(),
        );
        assert!(raw > DAY_SECS * 1_000, "the scenario must reproduce the raw gap: {raw} ms");
        assert_eq!(peer.engine.version_vector().unwrap().get(a.engine.node_id()), a_last);

        let outcome = sync_once(&a.engine, peer.addr, SECRET).await.unwrap();
        assert_eq!(
            outcome.behind_ms, 0,
            "a peer that can still be served everything it lacks is not a stale rejoiner: {outcome:?}"
        );
    }
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

    let outcome = sync_once(&b.engine, a.addr, SECRET).await.unwrap();
    // An incremental round re-serves the tail B already holds and reports it
    // superseded; a snapshot reports only what it applied. That is the tell.
    assert!(
        outcome.superseded > 0,
        "the round must be served from the oplog, not a snapshot: {outcome:?}"
    );
    assert_eq!(outcome.applied, 1, "{outcome:?}");
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    assert!(b.engine.get(&cb, &DocId::String("a-after-restart".into())).unwrap().is_some());

    let second = sync_once(&b.engine, a.addr, SECRET).await.unwrap();
    assert_eq!(second.total(), 0, "and B is then caught up: {second:?}");
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
    sync_once(&a.engine, b.addr, SECRET).await.unwrap();

    let later = kimmy_storage::physical_now_ms() + 36 * HOUR_MS;
    a.engine.apply_batch(&[entry_stamped(ca.id, later)]).unwrap();
    a.engine
        .collect_garbage_at(
            later + HOUR_MS,
            kimmy_storage::RetentionPolicy::new(DAY_SECS, DAY_SECS),
        )
        .unwrap();
    a.engine.insert(&ca, doc! { "_id": "a-after" }).unwrap();

    let outcome = sync_once(&a.engine, b.addr, SECRET).await.unwrap();
    assert!(
        outcome.behind_ms > DAY_SECS * 1_000,
        "B lacks a collected write of A's, 36 hours behind: it is stale: {outcome:?}"
    );

    let pulled = sync_once(&b.engine, a.addr, SECRET).await.unwrap();
    assert_eq!(pulled.superseded, 0, "served as a snapshot, not from the oplog: {pulled:?}");
    for id in ["a-missed", "a-after"] {
        assert!(
            b.engine.get(&cb, &DocId::String(id.into())).unwrap().is_some(),
            "{id} must arrive"
        );
    }
    let second = sync_once(&b.engine, a.addr, SECRET).await.unwrap();
    assert_eq!(second.total(), 0, "{second:?}");
}
