//! Replication over real TCP sockets.
//!
//! The convergence rules are already tested between engines in one process
//! (`kimmy-storage/src/sync.rs`). These tests exist for what that could not
//! reach: that the wire carries the types faithfully, that the handshake
//! actually gates access, and that a listener survives a peer misbehaving.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use std::collections::BTreeSet;

use bson::doc;
use kimmy_cluster::protocol::{Message, ProtocolError, read_frame, write_frame};
use kimmy_cluster::transport::{DivergenceProbe, push_entry, serve, serve_with, sync_once};
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
    let mut page = source.engine.snapshot_page(None).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            index.created = None;
        }
    }
    page.documents.clear();
    page.versions = kimmy_core::VersionVector::default();
    b.engine.apply_snapshot_page(&page).unwrap();

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

    let outcome = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
    // An incremental round re-serves the tail B already holds and reports it
    // superseded; a snapshot reports only what it applied. That is the tell.
    assert!(
        outcome.superseded > 0,
        "the round must be served from the oplog, not a snapshot: {outcome:?}"
    );
    assert_eq!(outcome.applied, 1, "{outcome:?}");
    let cb = b.engine.get_collection("shop", "orders").unwrap();
    assert!(b.engine.get(&cb, &DocId::String("a-after-restart".into())).unwrap().is_some());

    let second = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
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

    let pulled = sync_once(&b.engine, a.addr, SECRET, None).await.unwrap();
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

    let probe = Some(DivergenceProbe { id: ca.id, mine_count: None });
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
    let probe = Some(DivergenceProbe { id: ca.id, mine_count });
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

    let probe = Some(DivergenceProbe { id: ca.id, mine_count: None });
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

    let probe = Some(DivergenceProbe { id: ca.id, mine_count: None });
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
    let probe = Some(DivergenceProbe { id: ca.id, mine_count });
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
    let probe = Some(DivergenceProbe { id: ca.id, mine_count });
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

    let a = node().await;
    let b = node().await;

    let stranded = a.engine.create_collection("shop", "stranded").unwrap();
    a.engine.insert(&stranded, doc! { "_id": "1" }).unwrap();
    let busy = a.engine.create_collection("shop", "busy").unwrap();

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

    // A modest busy cluster: a handful of writes between rounds, well under
    // the batch cap, so a round pulling them still reaches the tail.
    let a_engine = Arc::clone(&a.engine);
    let writer = tokio::spawn(async move {
        for i in 0..20 {
            for j in 0..5 {
                a_engine.insert(&busy, doc! { "_id": format!("d{i}-{j}") }).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut confirmed = 0usize;
    while confirmed == 0 {
        let report = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the check never confirmed under sustained write load"))
            .expect("the loop must keep reporting");
        confirmed = report.divergent_collections;
    }
    assert_eq!(confirmed, 1, "the stranded collection, found despite the concurrent writes");
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
            assert_eq!(report.divergence_check_age_secs, Some(0), "just checked: {report:?}");
            break;
        }
        assert_eq!(report.divergence_check_age_secs, None, "nothing has run yet: {report:?}");
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
    let mut age = Some(0u64);
    while age < Some(1) {
        let report = tokio::time::timeout_at(deadline, frozen_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("the frozen member's check age never rose"))
            .expect("the loop must keep reporting");
        assert!(report.divergence_check_age_secs.is_some(), "a reading exists: {report:?}");
        assert!(report.divergence_check_age_secs >= age, "the age does not fall: {report:?}");
        assert_eq!(report.divergent_collections, 0, "the reading it is holding: {report:?}");
        later_checks += report.divergence_checks;
        failed += report.failed;
        skips += report.divergence_skips;
        age = report.divergence_check_age_secs;
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
    let mut page = source.engine.snapshot_page(None).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            index.created = None;
        }
    }
    page.documents.clear();
    // Granting B no coverage of another node's history: this fixture is
    // about the stored shape of the definition, nothing else.
    page.versions = kimmy_core::VersionVector::default();
    b.engine.apply_snapshot_page(&page).unwrap();
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
    let mut page = source.engine.snapshot_page(None).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            index.created = Some(ancient);
        }
    }
    page.documents.clear();
    page.versions = kimmy_core::VersionVector::default();
    b.engine.apply_snapshot_page(&page).unwrap();
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
    let serving =
        tokio::spawn(serve_with(Arc::clone(engine), listener, SECRET.to_string(), Some(hook)));
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
    let mut page = source.engine.snapshot_page(None).unwrap();
    for state in &mut page.collections {
        for index in &mut state.indexes {
            index.created = None;
        }
    }
    page.documents.clear();
    page.versions = kimmy_core::VersionVector::default();
    engine.apply_snapshot_page(&page).unwrap();
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
