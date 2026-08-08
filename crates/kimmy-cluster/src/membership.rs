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

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use foca::{Config, Foca, Identity, Notification, PostcardCodec, Timer};
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

/// A cluster member: where it listens, and which incarnation of it this is.
///
/// The incarnation is what lets a node that was wrongly declared down rejoin
/// under the same address. Without it, `Identity::renew` has nothing to change
/// and a node evicted by a transient network fault could never come back.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Member {
    pub addr: SocketAddr,
    incarnation: u64,
}

impl Member {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr, incarnation: 0 }
    }
}

impl Identity for Member {
    type Addr = SocketAddr;

    fn renew(&self) -> Option<Self> {
        // Declared down by the cluster: come back as a later incarnation of the
        // same address, which by `win_addr_conflict` displaces the dead record.
        Some(Self { addr: self.addr, incarnation: self.incarnation.wrapping_add(1) })
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        self.incarnation > adversary.incarnation
    }
}

/// Live members, shared with the replication loop.
///
/// A snapshot rather than a channel: replication asks "who is up *now*" once
/// per round, and does not care about the transitions in between.
#[derive(Clone, Default)]
pub struct Members(Arc<RwLock<BTreeSet<SocketAddr>>>);

impl Members {
    pub fn snapshot(&self) -> BTreeSet<SocketAddr> {
        self.0.read().clone()
    }

    pub fn is_empty(&self) -> bool {
        self.0.read().is_empty()
    }

    fn insert(&self, addr: SocketAddr) {
        self.0.write().insert(addr);
    }

    fn remove(&self, addr: &SocketAddr) {
        self.0.write().remove(addr);
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
                info!(peer = %member.addr, "member up");
                self.members.insert(member.addr);
            }
            Notification::MemberDown(member) => {
                info!(peer = %member.addr, "member down");
                self.members.remove(&member.addr);
            }
            // A member came back under a new incarnation. The address is
            // unchanged, so the live set already contains it.
            Notification::Rename(old, new) => {
                debug!(from = ?old, to = ?new, "member renewed its identity");
                self.members.insert(new.addr);
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
pub async fn run(socket: UdpSocket, local: SocketAddr, members: Members, seeds: SeedFeed) {
    let identity = Member::new(local);
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
        async move {
            let mut buffer = vec![0u8; MAX_DATAGRAM];
            loop {
                match socket.recv_from(&mut buffer).await {
                    Ok((len, _from)) => {
                        if tx.send(Input::Data(buffer[..len].to_vec())).await.is_err() {
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
            Input::Announce(addr) => foca.announce(Member::new(addr), &mut collector),
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

    #[test]
    fn a_renewed_identity_displaces_the_one_it_replaces() {
        // Without this a node wrongly declared down could never rejoin: it
        // would keep offering an identity the cluster has already buried.
        let original = Member::new(addr(7900));
        let renewed = original.renew().expect("renewal must be possible");

        assert_eq!(renewed.addr, original.addr, "an address does not change on rejoin");
        assert!(renewed.win_addr_conflict(&original), "the newer incarnation must win");
        assert!(!original.win_addr_conflict(&renewed));
    }

    #[test]
    fn identity_is_addressed_by_socket_not_by_incarnation() {
        // foca keeps memory bound by *nodes*, not by identities, which only
        // works if every incarnation reports the same address.
        let member = Member::new(addr(7900));
        assert_eq!(member.addr(), addr(7900));
        assert_eq!(member.renew().unwrap().addr(), addr(7900));
    }

    #[test]
    fn a_member_round_trips_through_the_wire_codec() {
        // Identities cross the network inside foca's messages; a codec
        // mismatch would look like an unreachable cluster.
        let member = Member { addr: addr(7901), incarnation: 3 };
        let bytes = postcard::to_allocvec(&member).unwrap();
        assert_eq!(postcard::from_bytes::<Member>(&bytes).unwrap(), member);
    }

    #[test]
    fn the_live_set_reflects_up_and_down() {
        let members = Members::default();
        assert!(members.is_empty());

        members.insert(addr(7901));
        members.insert(addr(7902));
        assert_eq!(members.snapshot().len(), 2);

        members.remove(&addr(7901));
        assert_eq!(members.snapshot(), BTreeSet::from([addr(7902)]));
    }
}
