//! SWIM membership over UDP.
//!
//! Replication already gossips *state*: each node pulls oplog entries from a
//! few peers and data reaches the cluster transitively. This module gossips
//! *membership* — who is alive — which is the half discovery cannot answer.
//!
//! Discovery reports who was *configured*, and a node's own failed connections
//! report who *it* cannot reach. Neither is a cluster opinion. SWIM makes one:
//! a node that cannot reach a peer asks others to probe it indirectly before
//! declaring anything, so a single bad link does not evict a healthy node, and
//! a genuine failure is agreed rather than rediscovered independently by
//! everyone.
//!
//! ```text
//!   UDP :7900          probes, acks, suspicion, membership updates
//!   TCP :7900          version vectors, oplog entries, snapshots
//! ```
//!
//! Two protocols on one port, which is why the config field is a bind address
//! rather than a port pair.
//!
//! # Shape
//!
//! [`foca`] owns the protocol and knows nothing about sockets or time. One task
//! owns the `Foca` value and is fed everything through a channel — inbound
//! datagrams, expired timers, and requests to announce — so the state is never
//! shared and never locked. What comes *out* is a set of live members, which
//! replication reads.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use foca::{Config, Foca, Identity, Notification, PostcardCodec, Timer};
use kimmy_core::NodeId;

use crate::protocol;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Largest datagram accepted. SWIM messages are small; anything larger is not
/// one of ours.
const MAX_DATAGRAM: usize = 64 * 1024;

/// Cluster size hint used to tune probe intervals and fan-out.
///
/// foca scales its timings by this. Over-estimating costs a little latency in
/// detecting failure; under-estimating costs extra traffic. A small default
/// suits the deployments this is aimed at, and it is not a limit.
const CLUSTER_SIZE_HINT: u32 = 16;

/// The node id an announce carries before the peer's real one is known.
///
/// Discovery yields an *address*; the identity behind it is whatever answers.
/// foca is built for this — it accepts an `Announce` whose `dst` matches only
/// on address — so the placeholder never needs to be right, only distinct.
const UNKNOWN_NODE: NodeId = NodeId::from_bytes([0u8; 16]);

/// A cluster member: which node it is, where it listens, and which incarnation
/// of it this is.
///
/// The incarnation is what lets a node that was wrongly declared down rejoin
/// under the same address. Without it, `Identity::renew` has nothing to change
/// and a node evicted by a transient network fault could never come back.
///
/// The **node id travels here** so that everything downstream can key on the
/// node rather than on where it happens to be listening. It is gossiped for
/// free: foca disseminates identities already, so this needs no second channel
/// and no address-to-node mapping to keep in step. See ADR-051.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Member {
    pub addr: SocketAddr,
    incarnation: u64,
    pub node: NodeId,
}

impl Member {
    /// This node's own identity.
    pub fn identified(addr: SocketAddr, node: NodeId) -> Self {
        Self { addr, incarnation: 0, node }
    }

    /// A discovered address whose node is not known yet.
    pub fn announcing(addr: SocketAddr) -> Self {
        Self { addr, incarnation: 0, node: UNKNOWN_NODE }
    }

    /// Whether this identity is a placeholder rather than a peer's real one.
    fn is_placeholder(&self) -> bool {
        self.node == UNKNOWN_NODE
    }
}

impl Identity for Member {
    type Addr = SocketAddr;

    fn renew(&self) -> Option<Self> {
        // Declared down by the cluster: come back as a later incarnation of the
        // same address, which by `win_addr_conflict` displaces the dead record.
        // The node id is carried over — this is the same node, not a new one.
        Some(Self {
            addr: self.addr,
            incarnation: self.incarnation.wrapping_add(1),
            node: self.node,
        })
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        // A placeholder is what an announce carries before anyone has answered;
        // a real identity must always displace it, whatever the incarnations
        // say, or a node would be remembered as the address it was found at.
        if self.is_placeholder() != adversary.is_placeholder() {
            return !self.is_placeholder();
        }
        if self.incarnation != adversary.incarnation {
            return self.incarnation > adversary.incarnation;
        }
        // Two different nodes claiming one address — a replacement that reused
        // it, most likely. Both start at incarnation 0, so without a tiebreak
        // neither displaces the other and the address stalls on the dead one.
        // Comparing node ids is arbitrary but *agreed*: every node reaches the
        // same answer, which is what matters.
        self.node > adversary.node
    }
}

/// Live members, shared with the replication loop and the webhook dispatcher.
///
/// A snapshot rather than a channel: replication asks "who is up *now*" once
/// per round, and does not care about the transitions in between.
///
/// Keyed by address and valued by node id, because the two consumers want
/// different halves — replication dials an address, ownership hashes a node.
/// Note that this holds **peers only**: SWIM's live set never contains the node
/// holding it, which is the trap that left every clustered webhook undelivered
/// until the harness caught it.
#[derive(Clone, Default)]
pub struct Members(Arc<RwLock<BTreeMap<SocketAddr, NodeId>>>);

impl Members {
    /// Peer addresses, for anything that needs to dial one.
    pub fn snapshot(&self) -> BTreeSet<SocketAddr> {
        self.0.read().keys().copied().collect()
    }

    /// Peer node ids, for anything that needs to name a peer independently of
    /// where it is listening.
    pub fn node_ids(&self) -> BTreeSet<NodeId> {
        self.0.read().values().copied().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.0.read().is_empty()
    }

    fn insert(&self, addr: SocketAddr, node: NodeId) {
        self.0.write().insert(addr, node);
    }

    fn remove(&self, addr: &SocketAddr) {
        self.0.write().remove(addr);
    }

    /// Populate a member set without a running SWIM task.
    ///
    /// For tests in crates that consume this set — ownership in particular,
    /// whose whole point is what happens when an address changes under a node.
    /// Membership itself is driven only by foca notifications.
    pub fn insert_for_test(&self, addr: SocketAddr, node: NodeId) {
        self.insert(addr, node);
    }

    /// Remove a member without a running SWIM task. See [`Self::insert_for_test`].
    pub fn remove_for_test(&self, addr: &SocketAddr) {
        self.remove(addr);
    }
}

/// Everything the membership task reacts to.
enum Input {
    /// A datagram from a peer.
    Data(Vec<u8>),
    /// A timer foca asked us to deliver.
    Timer(Timer<Member>),
    /// Introduce ourselves to a discovered address.
    Announce(SocketAddr),
}

/// Collects foca's outputs so they can be dispatched after the call returns.
///
/// foca's `Runtime` is synchronous, but sending a datagram and sleeping are
/// not, so nothing is performed here — the work is queued and flushed by the
/// caller, which is the only place that can `await`.
struct Collector {
    outgoing: Vec<(SocketAddr, Vec<u8>)>,
    timers: Vec<(Timer<Member>, Duration)>,
    members: Members,
}

impl foca::Runtime<Member> for Collector {
    fn notify(&mut self, notification: Notification<'_, Member>) {
        match notification {
            Notification::MemberUp(member) => {
                info!(peer = %member.addr, node = %member.node, "member up");
                self.members.insert(member.addr, member.node);
            }
            Notification::MemberDown(member) => {
                info!(peer = %member.addr, "member down");
                self.members.remove(&member.addr);
            }
            // A member came back under a new incarnation. The address is
            // unchanged, so the live set already contains it.
            Notification::Rename(old, new) => {
                debug!(from = ?old, to = ?new, "member renewed its identity");
                self.members.insert(new.addr, new.node);
            }
            Notification::Defunct => {
                warn!("this node was declared down by the cluster; rejoining");
            }
            Notification::Rejoin(identity) => {
                info!(as_member = %identity.addr, "rejoined the cluster");
            }
            other => debug!(?other, "membership notification"),
        }
    }

    fn send_to(&mut self, to: Member, data: &[u8]) {
        self.outgoing.push((to.addr, data.to_vec()));
    }

    fn submit_after(&mut self, event: Timer<Member>, after: Duration) {
        self.timers.push((event, after));
    }
}

/// Run SWIM membership until the socket fails.
///
/// `local` is this node's cluster address, and must be the address peers can
/// reach — a bind of `0.0.0.0` would otherwise announce an unroutable identity.
/// `node` is this node's durable id, which travels with the identity so peers
/// can name it independently of where it is listening.
pub async fn run(
    socket: UdpSocket,
    local: SocketAddr,
    node: NodeId,
    secret: String,
    members: Members,
    seeds: SeedFeed,
) {
    let identity = Member::identified(local, node);
    let config =
        Config::new_lan(std::num::NonZeroU32::new(CLUSTER_SIZE_HINT).expect("non-zero literal"));

    // Seeded from the OS. SWIM uses randomness to pick probe targets, so a
    // predictable sequence would make every node probe in the same order.
    let rng: rand::rngs::StdRng = rand::make_rng();
    let mut foca = Foca::new(identity, config, rng, PostcardCodec);
    let socket = Arc::new(socket);
    let (tx, mut rx) = mpsc::channel::<Input>(1024);

    // Inbound datagrams.
    tokio::spawn({
        let socket = Arc::clone(&socket);
        let tx = tx.clone();
        let secret = secret.clone();
        async move {
            let mut buffer = vec![0u8; MAX_DATAGRAM];
            let mut rejected: u64 = 0;
            loop {
                match socket.recv_from(&mut buffer).await {
                    Ok((len, from)) => {
                        // Verified *before* foca sees it. An unauthenticated
                        // node that reached the member set would become a
                        // webhook ownership candidate and deliver nothing,
                        // which is how this was found (ADR-053).
                        let Some(payload) = protocol::untag_datagram(&secret, &buffer[..len])
                        else {
                            rejected += 1;
                            // Rate-limited: an unauthenticated peer probes on
                            // its own schedule forever, and a line per
                            // datagram would bury every other membership log.
                            if rejected.is_power_of_two() {
                                warn!(
                                    peer = %from,
                                    rejected,
                                    "dropping a membership datagram that failed authentication; \
                                     check that every node shares one cluster_secret"
                                );
                            }
                            continue;
                        };
                        if tx.send(Input::Data(payload.to_vec())).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "membership socket read failed");
                        return;
                    }
                }
            }
        }
    });

    // Discovered addresses to announce ourselves to.
    tokio::spawn({
        let tx = tx.clone();
        async move {
            let mut seeds = seeds;
            while let Some(addr) = seeds.next().await {
                if tx.send(Input::Announce(addr)).await.is_err() {
                    return;
                }
            }
        }
    });

    info!(bind = %local, "gossiping membership");

    let mut collector = Collector { outgoing: Vec::new(), timers: Vec::new(), members };

    while let Some(input) = rx.recv().await {
        let result = match input {
            Input::Data(bytes) => foca.handle_data(&bytes, &mut collector),
            Input::Timer(timer) => foca.handle_timer(timer, &mut collector),
            Input::Announce(addr) => foca.announce(Member::announcing(addr), &mut collector),
        };

        if let Err(e) = result {
            // A malformed datagram from one peer must not stop membership for
            // everyone; foca's state is unchanged by a rejected message.
            debug!(error = %e, "membership input rejected");
        }

        // Dispatch what foca asked for. Datagrams are best-effort by design —
        // SWIM assumes loss and probes again — so a send failure is logged at
        // debug rather than treated as an error.
        for (addr, data) in collector.outgoing.drain(..) {
            let data = protocol::tag_datagram(&secret, &data);
            if let Err(e) = socket.send_to(&data, addr).await {
                debug!(peer = %addr, error = %e, "membership send failed");
            }
        }

        for (event, after) in collector.timers.drain(..) {
            let tx = tx.clone();
            // Every submitted timer MUST be delivered — foca tolerates delay
            // but not loss — so this is a task per timer rather than a wheel we
            // could get wrong.
            tokio::spawn(async move {
                tokio::time::sleep(after).await;
                let _ = tx.send(Input::Timer(event)).await;
            });
        }
    }
}

/// A stream of addresses to introduce ourselves to.
///
/// Membership discovers peers by gossip once it has *one* contact, so this only
/// has to supply enough to bootstrap — but it keeps supplying, because a node
/// that starts alone must still find the cluster when it appears.
pub struct SeedFeed {
    rx: mpsc::Receiver<SocketAddr>,
}

impl SeedFeed {
    pub fn channel() -> (mpsc::Sender<SocketAddr>, Self) {
        let (tx, rx) = mpsc::channel(64);
        (tx, Self { rx })
    }

    async fn next(&mut self) -> Option<SocketAddr> {
        self.rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    #[test]
    fn a_renewed_identity_displaces_the_one_it_replaces() {
        // Without this a node wrongly declared down could never rejoin: it
        // would keep offering an identity the cluster has already buried.
        let original = Member::identified(addr(7900), node(1));
        let renewed = original.renew().expect("renewal must be possible");

        assert_eq!(renewed.addr, original.addr, "an address does not change on rejoin");
        assert_eq!(renewed.node, original.node, "nor does the node id: this is the same node");
        assert!(renewed.win_addr_conflict(&original), "the newer incarnation must win");
        assert!(!original.win_addr_conflict(&renewed));
    }

    #[test]
    fn identity_is_addressed_by_socket_not_by_incarnation() {
        // foca keeps memory bound by *nodes*, not by identities, which only
        // works if every incarnation reports the same address.
        let member = Member::identified(addr(7900), node(1));
        assert_eq!(member.addr(), addr(7900));
        assert_eq!(member.renew().unwrap().addr(), addr(7900));
    }

    #[test]
    fn a_real_identity_beats_a_placeholder_even_at_a_lower_incarnation() {
        // This is the case that makes the placeholder branch load-bearing
        // rather than decorative. With equal incarnations the node-id
        // comparison already favours a real identity, because the placeholder
        // is all-zero and therefore minimal — so only a placeholder at a
        // *higher* incarnation distinguishes having the branch from not.
        let mut placeholder = Member::announcing(addr(7900));
        placeholder.incarnation = 5;
        let real = Member::identified(addr(7900), node(7));

        assert!(real.win_addr_conflict(&placeholder), "a real identity must always displace one");
        assert!(!placeholder.win_addr_conflict(&real));
    }

    #[test]
    fn a_real_identity_displaces_the_placeholder_an_announce_carries() {
        // Discovery yields an address; the identity behind it is whatever
        // answers. If the placeholder could win, a peer would be remembered as
        // the address it was found at and never as itself.
        let placeholder = Member::announcing(addr(7900));
        let real = Member::identified(addr(7900), node(7));

        assert!(real.win_addr_conflict(&placeholder), "the real identity must win");
        assert!(!placeholder.win_addr_conflict(&real), "and the placeholder must not");
    }

    #[test]
    fn two_nodes_claiming_one_address_resolve_the_same_way_on_both_sides() {
        // A replacement that reused an address: both start at incarnation 0, so
        // without a tiebreak neither displaces the other and the address stalls
        // on whichever was seen first. The rule is arbitrary but must be
        // *agreed* — exactly one of the two comparisons may be true.
        let a = Member::identified(addr(7900), node(1));
        let b = Member::identified(addr(7900), node(2));

        assert_ne!(
            a.win_addr_conflict(&b),
            b.win_addr_conflict(&a),
            "exactly one must win, or the conflict never resolves"
        );
    }

    #[test]
    fn a_member_round_trips_through_the_wire_codec() {
        // Identities cross the network inside foca's messages; a codec
        // mismatch would look like an unreachable cluster.
        let member = Member { addr: addr(7901), incarnation: 3, node: node(9) };
        let bytes = postcard::to_allocvec(&member).unwrap();
        assert_eq!(postcard::from_bytes::<Member>(&bytes).unwrap(), member);
    }

    #[test]
    fn the_live_set_reflects_up_and_down_and_carries_node_ids() {
        let members = Members::default();
        assert!(members.is_empty());

        members.insert(addr(7901), node(1));
        members.insert(addr(7902), node(2));
        assert_eq!(members.snapshot().len(), 2);
        assert_eq!(members.node_ids(), BTreeSet::from([node(1), node(2)]));

        members.remove(&addr(7901));
        assert_eq!(members.snapshot(), BTreeSet::from([addr(7902)]));
        assert_eq!(members.node_ids(), BTreeSet::from([node(2)]), "the id goes with the address");
    }

    #[test]
    fn a_node_that_moves_address_keeps_its_node_id() {
        // The whole point of task 10: the set changes address, and the identity
        // ownership hashes does not.
        let members = Members::default();
        members.insert(addr(7901), node(1));
        let before = members.node_ids();

        // Rescheduled onto a new address.
        members.remove(&addr(7901));
        members.insert(addr(7999), node(1));

        assert_eq!(members.node_ids(), before, "the node is the same node");
        assert_ne!(members.snapshot(), BTreeSet::from([addr(7901)]));
    }
}
