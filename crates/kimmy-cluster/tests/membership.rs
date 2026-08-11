//! SWIM membership over real UDP sockets.
//!
//! The unit tests cover identity and the live set. These cover the thing that
//! cannot be checked without a network: that two nodes given nothing but each
//! other's address actually converge on a shared view of who is alive, and
//! notice when one stops answering.

use std::net::SocketAddr;
use std::time::Duration;

use kimmy_cluster::{Members, SeedFeed};

/// Every node in these tests shares one secret, as a real cluster must.
const SECRET: &str = "a-shared-membership-test-secret";
use tokio::net::UdpSocket;

struct Node {
    addr: SocketAddr,
    node_id: kimmy_core::NodeId,
    members: Members,
    announce: tokio::sync::mpsc::Sender<SocketAddr>,
    handle: tokio::task::JoinHandle<()>,
}

async fn node() -> Node {
    node_with_secret(SECRET).await
}

async fn node_with_secret(secret: &str) -> Node {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let members = Members::default();
    let (announce, feed) = SeedFeed::channel();
    // A distinct id per node, as a real node has: it comes from the database
    // file, and it is what peers key on rather than the address.
    let node_id = kimmy_core::NodeId::generate();

    let handle = tokio::spawn(kimmy_cluster::membership::run(
        socket,
        addr,
        node_id,
        secret.to_string(),
        members.clone(),
        feed,
    ));
    Node { addr, node_id, members, announce, handle }
}

/// Wait for a condition, failing rather than hanging.
async fn eventually(label: &str, mut check: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for: {label}");
}

#[tokio::test]
async fn two_nodes_discover_each_other() {
    // One address each, no shared configuration beyond that.
    let a = node().await;
    let b = node().await;

    a.announce.send(b.addr).await.unwrap();

    eventually("a to see b", || a.members.snapshot().contains(&b.addr)).await;
    // And membership is mutual: b learns about a without being told.
    eventually("b to see a", || b.members.snapshot().contains(&a.addr)).await;
}

#[tokio::test]
async fn a_peers_node_id_arrives_with_it_and_survives_gossip() {
    // Webhook ownership hashes node ids, so the id has to reach every node
    // that might have to decide — including one that only ever heard of the
    // peer second-hand. A placeholder surviving here would mean ownership
    // computed over an id that is not anybody's.
    let a = node().await;
    let b = node().await;
    let c = node().await;

    a.announce.send(b.addr).await.unwrap();
    eventually("a and b to meet", || a.members.snapshot().contains(&b.addr)).await;
    c.announce.send(a.addr).await.unwrap();
    eventually("c to learn about b through a", || c.members.snapshot().contains(&b.addr)).await;

    // Direct: a met b by announcing to it.
    assert!(
        a.members.node_ids().contains(&b.node_id),
        "a must know b by id, not only by address: {:?}",
        a.members.node_ids()
    );
    // Second-hand: c never announced to b, and learned it through a.
    assert!(
        c.members.node_ids().contains(&b.node_id),
        "a gossiped id must be the peer's real one, not the announce placeholder"
    );
    assert!(c.members.node_ids().contains(&a.node_id));
    assert!(
        !c.members.node_ids().contains(&c.node_id),
        "the live set is peers only; a node never lists itself"
    );
}

#[tokio::test]
async fn a_third_node_is_learned_by_gossip_not_by_configuration() {
    // The capability discovery cannot provide: C is told only about A, and A
    // never announces B to it — yet C ends up knowing B.
    let a = node().await;
    let b = node().await;
    let c = node().await;

    a.announce.send(b.addr).await.unwrap();
    eventually("a and b to meet", || a.members.snapshot().contains(&b.addr)).await;

    c.announce.send(a.addr).await.unwrap();

    eventually("c to learn about b through a", || c.members.snapshot().contains(&b.addr)).await;
    assert!(c.members.snapshot().contains(&a.addr));
}

#[tokio::test]
async fn a_node_that_stops_answering_is_declared_down() {
    // The other capability: a shared opinion that a peer is gone, rather than
    // each node privately failing to connect forever.
    let a = node().await;
    let b = node().await;

    a.announce.send(b.addr).await.unwrap();
    eventually("a to see b", || a.members.snapshot().contains(&b.addr)).await;

    // Kill b's membership task; its socket closes with it.
    b.handle.abort();

    eventually("a to declare b down", || !a.members.snapshot().contains(&b.addr)).await;
}

#[tokio::test]
async fn a_node_holding_the_wrong_secret_cannot_join() {
    // Found by driving a five-node cluster, not by reading the code: SWIM was
    // unauthenticated, so a node with the wrong `cluster_secret` joined the
    // member set. Replication rejected it correctly and it could read nothing
    // — but webhook ownership is rendezvous-hashed over exactly this set, so
    // it won roughly 1/(N+1) of subscriptions and delivered none of them.
    // Eight of twelve arrived; four vanished, with every real node believing
    // it had correctly stood down. See ADR-053.
    let a = node().await;
    let b = node().await;
    let impostor = node_with_secret("a-completely-different-secret").await;

    // The impostor announces itself to a legitimate node, repeatedly.
    for _ in 0..3 {
        impostor.announce.send(a.addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    // ...and a legitimate node announces to it, so the refusal holds in both
    // directions rather than only for unsolicited traffic.
    a.announce.send(impostor.addr).await.unwrap();

    // Meanwhile the two legitimate nodes must still find each other, or this
    // test would pass on a cluster that simply never formed.
    a.announce.send(b.addr).await.unwrap();
    eventually("a and b to meet", || a.members.snapshot().contains(&b.addr)).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(
        !a.members.snapshot().contains(&impostor.addr),
        "an unauthenticated node must not reach the member set: {:?}",
        a.members.snapshot()
    );
    assert!(!b.members.snapshot().contains(&impostor.addr));
    assert!(
        !impostor.members.node_ids().contains(&a.node_id),
        "and it must not learn the cluster's membership either"
    );
    assert_eq!(a.members.snapshot().len(), 1, "a sees exactly one peer: b");
}

#[tokio::test]
async fn a_node_does_not_list_itself() {
    // Replication filters this too, but a member set that includes the local
    // node would have it probing and syncing with itself.
    let a = node().await;
    let b = node().await;
    a.announce.send(b.addr).await.unwrap();
    eventually("a to see b", || a.members.snapshot().contains(&b.addr)).await;

    assert!(!a.members.snapshot().contains(&a.addr));
}
