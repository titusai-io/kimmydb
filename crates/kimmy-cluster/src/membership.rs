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
use std::time::{Duration, Instant};

use foca::{
    Config, Foca, Identity, NoCustomBroadcast, Notification, PostcardCodec, Runtime, Timer,
};
use kimmy_core::{CollectionId, NodeId};

use crate::facts::{
    Facts, FactsSource, LocalFacts, OwnerClass, PeerFacts, PeerState, last_decoded_seq, may_own,
    next_decoded_seq,
};
use crate::protocol;
pub use incarnation::IncarnationFile;
use parking_lot::{Mutex, RwLock};
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
/// **It outlives the process** ([`IncarnationFile`], ADR-242): a start takes one
/// above the last its data directory recorded, so a restarted member displaces
/// whatever identity of it a peer holds in one step, instead of renewing one
/// step at a time up to it.
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
    /// This node's own identity, at `incarnation`
    /// ([`IncarnationFile::current`] for a process's first).
    pub fn identified(addr: SocketAddr, node: NodeId, incarnation: u64) -> Self {
        Self { addr, incarnation, node }
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
        // Two different nodes claiming one address at one incarnation — a
        // replacement that reused it, most likely, since a node with a fresh
        // data directory starts at 0 (ADR-242). Without a tiebreak neither
        // displaces the other and the address stalls on the dead one.
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
pub struct Members(Arc<MembersInner>);

impl std::fmt::Debug for Members {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Members").field("peers", &self.0.live.read().len()).finish()
    }
}

#[derive(Default)]
struct MembersInner {
    live: RwLock<BTreeMap<SocketAddr, NodeId>>,
    /// Each address's generation: the value of `next` when it was last
    /// inserted. Kept when the address is removed, so a generation only ever
    /// moves forward (ADR-191).
    generations: RwLock<BTreeMap<SocketAddr, u64>>,
    next: std::sync::atomic::AtomicU64,
    /// This member's own block, once the daemon has given a source for it.
    local: std::sync::OnceLock<LocalFacts>,
    /// The catching-up marker and its replay, once the daemon has given them: what a
    /// connection served here asks the peer for, and applies, when this member's
    /// replay of its own origin is owed (ADR-212's addendum).
    catch_up: std::sync::OnceLock<Arc<crate::catchup::CatchUp>>,
    /// The last block each peer sent (ADR-201), and the floors that keep an
    /// earlier-decoded block from coming back after a drop (ADR-213).
    ///
    /// **Every writer of this table or of the live set builds and stores the new
    /// [`PeerView`] while holding this lock**, so the order views are published in
    /// is the order the state changed in, and an older view is never stored last.
    peer_facts: RwLock<PeerTable>,
    /// The immutable view the evaluator reads: a load, never a lock.
    view: arc_swap::ArcSwap<PeerView>,
    view_version: std::sync::atomic::AtomicU64,
    /// When the live set last changed: a yield becomes effective only after it has
    /// held still for a lease (ADR-213, C9).
    live_changed: Mutex<Option<Instant>>,
    /// When each live peer came up, for the unheard-peer warning.
    up_since: Mutex<BTreeMap<NodeId, Instant>>,
    /// The sync interval and the fanout the lease is derived from.
    lease_shape: RwLock<(Duration, usize)>,
    /// When each peer's undecodable block was last said, so it is said once per
    /// [`crate::health::WARN_INTERVAL`] and peer.
    undecodable_said: Mutex<BTreeMap<NodeId, Instant>>,
    unheard_said: Mutex<BTreeMap<NodeId, Instant>>,
    /// When each peer was last named in the WARN that a `Decimal128` id exists
    /// here while it cannot hold one (ADR-238).
    decimal128_said: Mutex<BTreeMap<NodeId, Instant>>,
    /// The immediate facts exchanges asked of this set (ADR-247).
    exchanges: Exchanges,
}

/// Where an immediate facts exchange with a member is started: handed the
/// member's address and node id (ADR-247).
pub(crate) type ExchangeSink = Arc<dyn Fn(SocketAddr, NodeId) + Send + Sync>;

/// The asks the replication loop's exchange driver takes, in order.
pub(crate) type ExchangeQueue = mpsc::UnboundedReceiver<(SocketAddr, NodeId)>;

/// The immediate facts exchanges asked of a member set (ADR-247): where an ask
/// goes, the queue the replication loop takes once, and when each member was
/// last asked for.
///
/// **The queue exists from the set's creation**, so a member the membership task
/// sees come up before the replication loop has started (the daemon starts
/// membership first) is queued, not lost.
struct Exchanges {
    sink: RwLock<ExchangeSink>,
    queue: Mutex<Option<ExchangeQueue>>,
    asked: Mutex<BTreeMap<NodeId, Asked>>,
    /// Woken when a trailing ask is scheduled, so the driver's timer takes it.
    scheduled: tokio::sync::Notify,
    /// Exchanges the driver has seen end, for a test that waits on one.
    #[cfg(test)]
    ended: std::sync::atomic::AtomicUsize,
}

/// When a member was last asked for, and what is owed it since (ADR-247).
#[derive(Clone, Copy, Debug)]
struct Asked {
    /// The last ask, or, while `trailing`, when the trailing ask is due.
    at: Instant,
    /// Whether the member's block, or every block, was dropped since that ask:
    /// what makes an ask the debounce holds owe a trailing one.
    dropped: bool,
    /// A trailing ask is pending, due at `at`.
    trailing: bool,
}

impl Default for Exchanges {
    fn default() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let sink: ExchangeSink = Arc::new(move |addr, node| {
            // Unbounded, and bounded by the debounce instead: one ask per
            // member per sync interval. A send fails only once the driver,
            // which holds this set, has gone with the loop.
            let _ = tx.send((addr, node));
        });
        Self {
            sink: RwLock::new(sink),
            queue: Mutex::new(Some(rx)),
            asked: Mutex::default(),
            scheduled: tokio::sync::Notify::new(),
            #[cfg(test)]
            ended: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

/// The blocks held, and where a block must have been decoded after to count.
#[derive(Default)]
struct PeerTable {
    held: BTreeMap<NodeId, PeerFacts>,
    /// The decode sequence at which a peer's slot was dropped (SWIM down, a
    /// rename), and when: a record decoded before it is ignored. Pruned once the
    /// slot holds a block decoded after it, and for a peer that is gone and has
    /// been quiet for many leases, as `held` is.
    dropped: BTreeMap<NodeId, (u64, Instant)>,
    /// The decode sequence at this member's own `Defunct` or `Rejoin`.
    floor: u64,
}

/// One held block, as the evaluator sees it.
#[derive(Clone, Debug)]
pub struct ViewBlock {
    pub facts: Arc<Facts>,
    /// The sender's generation of the block, `None` from a 0.43 sender.
    pub generation: Option<u64>,
    pub received: Instant,
}

/// The live set and every held block, immutable: what the yield evaluator reads
/// without taking a lock any runtime task takes (ADR-213). `version` moves with
/// every change.
#[derive(Clone, Debug, Default)]
pub struct PeerView {
    pub version: u64,
    /// The lease a block is fresh for, derived from the live count.
    pub lease: Duration,
    pub live: BTreeSet<NodeId>,
    pub blocks: BTreeMap<NodeId, ViewBlock>,
}

impl PeerView {
    /// The block held from `node`, if it is live, and whether it is within its lease.
    pub fn live_block(&self, node: &NodeId, now: Instant) -> Option<(&ViewBlock, bool)> {
        if !self.live.contains(node) {
            return None;
        }
        let held = self.blocks.get(node)?;
        Some((held, now.saturating_duration_since(held.received) <= self.lease))
    }
}

impl Members {
    /// Give the connections served from this handle the catching-up state. Set
    /// once, at start.
    pub fn set_catch_up(&self, catch_up: Arc<crate::catchup::CatchUp>) {
        let _ = self.0.catch_up.set(catch_up);
    }

    /// The catching-up state, if the daemon gave it.
    pub fn catch_up(&self) -> Option<&Arc<crate::catchup::CatchUp>> {
        self.0.catch_up.get()
    }

    /// Peer addresses, for anything that needs to dial one.
    pub fn snapshot(&self) -> BTreeSet<SocketAddr> {
        self.0.live.read().keys().copied().collect()
    }

    /// Which time this address last came into the live set: a value that
    /// moves forward every time SWIM brings it (back) up, whether after being
    /// declared down or under a new incarnation, and never moves back, not
    /// even when the address is removed. `None` for an address never seen.
    ///
    /// What a confirmation's back-off after an unanswered push compares
    /// against, so a member that restarts ends it (ADR-191). A per-address
    /// counter reset by removal could land on the value a back-off already
    /// recorded; one global counter cannot.
    pub fn generation(&self, addr: &SocketAddr) -> Option<u64> {
        self.0.generations.read().get(addr).copied()
    }

    /// Peer node ids, for anything that needs to name a peer independently of
    /// where it is listening.
    pub fn node_ids(&self) -> BTreeSet<NodeId> {
        self.0.live.read().values().copied().collect()
    }

    /// Every peer with both its address and its id, for a caller that dials
    /// by address and reports by id — a member that never answered the dial
    /// still has to be named.
    pub fn entries(&self) -> Vec<(SocketAddr, NodeId)> {
        self.0.live.read().iter().map(|(addr, node)| (*addr, *node)).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.0.live.read().is_empty()
    }

    /// The live set and every held block as of the last change: **one atomic
    /// load, no lock** (ADR-213). The yield evaluator's only read of `Members`.
    pub fn view(&self) -> Arc<PeerView> {
        self.0.view.load_full()
    }

    /// Build and store the view. Called with `table` held for write, so the
    /// order views are stored in is the order the state changed in.
    fn publish(&self, table: &PeerTable) {
        let blocks = table
            .held
            .iter()
            .map(|(node, held)| {
                (
                    *node,
                    ViewBlock {
                        facts: Arc::clone(&held.facts),
                        generation: held.generation,
                        received: held.received,
                    },
                )
            })
            .collect();
        let version = self.0.view_version.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        let view = PeerView { version, lease: self.lease(), live: self.node_ids(), blocks };
        self.0.view.store(Arc::new(view));
    }

    fn live_changed_now(&self) {
        *self.0.live_changed.lock() = Some(Instant::now());
    }

    fn insert(&self, addr: SocketAddr, node: NodeId) {
        let table = self.0.peer_facts.write();
        let generation = self.0.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        self.0.generations.write().insert(addr, generation);
        self.0.live.write().insert(addr, node);
        self.live_changed_now();
        self.0.up_since.lock().entry(node).or_insert_with(Instant::now);
        self.publish(&table);
        // Nothing is dropped here: a block that arrived before SWIM's insert
        // (the contact can win the race) is the peer's, and a peer SWIM declared
        // down had its block dropped then, in `remove`.
    }

    fn remove(&self, addr: &SocketAddr) {
        let mut table = self.0.peer_facts.write();
        let gone = self.0.live.write().remove(addr);
        self.live_changed_now();
        // Declared down: what it last said, and what it last read of ours,
        // belong to an incarnation that may be gone. A block it sends after it
        // is back is a new one, and a stale bit stays only until then (ADR-201).
        if let Some(node) = gone {
            self.0.up_since.lock().remove(&node);
            self.drop_peer(&mut table, node);
        }
        self.publish(&table);
    }

    /// A block from `peer` did not decode and was treated as none. Says so, once
    /// per interval per peer, naming it: otherwise a peer whose blocks never
    /// decode looks exactly like an older one that sends none. Answers whether it
    /// said it now.
    pub(crate) fn note_undecodable(&self, peer: NodeId, now: Instant) -> bool {
        let mut said = self.0.undecodable_said.lock();
        let due = said
            .get(&peer)
            .is_none_or(|at| now.saturating_duration_since(*at) >= crate::health::WARN_INTERVAL);
        if due {
            said.insert(peer, now);
            warn!(
                %peer,
                "a peer sent a facts block that does not decode; it is treated as having sent \
                 none, as an older version's peer is"
            );
        }
        due
    }

    /// The live peers that have sent no block for more than a lease, since they
    /// came up. Such a peer is not a candidate for webhooks or embeddings here, so
    /// this member also owns that share (ADR-213).
    pub fn unheard_beyond_lease(&self, now: Instant) -> Vec<NodeId> {
        let lease = self.lease();
        let table = self.0.peer_facts.read();
        let up = self.0.up_since.lock();
        self.node_ids()
            .into_iter()
            .filter(|node| {
                !table.held.contains_key(node)
                    && up
                        .get(node)
                        .is_some_and(|since| now.saturating_duration_since(*since) > lease)
            })
            .collect()
    }

    /// Say, once per interval per peer, that `peer` has been unheard for more than
    /// a lease. Answers whether it said it now.
    pub(crate) fn note_unheard(&self, peer: NodeId, now: Instant) -> bool {
        let live = self.node_ids();
        let mut said = self.0.unheard_said.lock();
        // Kept for live peers and for those named within the interval, the same
        // shape as the echoed map: a peer that left does not stay in it for ever.
        said.retain(|node, at| {
            live.contains(node) || now.saturating_duration_since(*at) < crate::health::WARN_INTERVAL
        });
        let due = said
            .get(&peer)
            .is_none_or(|at| now.saturating_duration_since(*at) >= crate::health::WARN_INTERVAL);
        if due {
            said.insert(peer, now);
            warn!(
                %peer,
                "a live peer has sent no facts block for more than a lease: it is not a candidate \
                 for webhooks or embeddings here, so this member also owns that peer's share until \
                 it is heard"
            );
        }
        due
    }

    /// The live peers that do not advertise `Facts.decimal128 = Some(true)`, a
    /// peer not heard from yet included: what holds back a locally created
    /// `Decimal128` id or bound (ADR-238). One load of the view, no lock.
    pub fn decimal128_blockers(&self) -> Vec<NodeId> {
        crate::facts::decimal128_blockers(&self.view())
    }

    /// The peers to name now in the WARN that this member holds a document
    /// whose `_id` is a `Decimal128` while they cannot hold one (ADR-238): each
    /// blocking peer the first time it is seen, then again every
    /// [`crate::decimal128::DECIMAL128_WARN_INTERVAL`] while it still blocks.
    /// The caller asks only while the marker is set. A peer that stops
    /// blocking (it upgraded, or left the live set) is forgotten, so one that
    /// blocks again is named at once.
    ///
    /// **A live peer this member has not heard from is named only once it has
    /// been unheard for more than a lease**, as the unheard-peer WARN does: it
    /// blocks the gate at once (failing closed), but a peer that came up a
    /// moment ago and has not been contacted yet is not one that cannot hold a
    /// decimal, and saying so at every start would teach an operator to skip
    /// the line.
    pub fn decimal128_warn_due(&self, now: Instant) -> Vec<NodeId> {
        let lease = self.lease();
        let view = self.view();
        let blockers: Vec<NodeId> = {
            let up = self.0.up_since.lock();
            crate::facts::decimal128_blockers(&view)
                .into_iter()
                .filter(|node| {
                    view.blocks.contains_key(node)
                        || up
                            .get(node)
                            .is_none_or(|since| now.saturating_duration_since(*since) > lease)
                })
                .collect()
        };
        let mut said = self.0.decimal128_said.lock();
        said.retain(|node, _| blockers.contains(node));
        blockers
            .into_iter()
            .filter(|node| {
                let due = said.get(node).is_none_or(|at| {
                    now.saturating_duration_since(*at)
                        >= crate::decimal128::DECIMAL128_WARN_INTERVAL
                });
                if due {
                    said.insert(*node, now);
                }
                due
            })
            .collect()
    }

    /// [`Self::decimal128_warn_due`] while the marker is set (ADR-238): the peers
    /// to name now in the WARN. `marker` reads the store's marker, and is asked
    /// only when some live peer blocks, so a cluster that can hold decimals
    /// costs no read; with none blocking, the call forgets who was named.
    pub fn decimal128_warnings(&self, marker: impl FnOnce() -> bool, now: Instant) -> Vec<NodeId> {
        if !self.decimal128_blockers().is_empty() && !marker() {
            return Vec::new();
        }
        self.decimal128_warn_due(now)
    }

    /// Drop what `node` last said, and what it last echoed of ours: it is a new
    /// incarnation, or SWIM declared it down (ADR-201). The slot remembers when it
    /// was dropped, so a block decoded before cannot come back (ADR-213).
    fn drop_peer(&self, table: &mut PeerTable, node: NodeId) {
        table.held.remove(&node);
        table.dropped.insert(node, (last_decoded_seq(), Instant::now()));
        if let Some(local) = self.0.local.get() {
            local.forget(node);
        }
        if let Some(asked) = self.0.exchanges.asked.lock().get_mut(&node) {
            asked.dropped = true;
        }
    }

    fn forget_peer(&self, node: NodeId) {
        let mut table = self.0.peer_facts.write();
        self.drop_peer(&mut table, node);
        self.publish(&table);
    }

    /// This member was declared down by the cluster, or rejoined (ADR-213): its
    /// peers dropped its block, and it may have missed changes to theirs, so it
    /// clears every echo and **drops every peer block**. It then owns everything
    /// it is a candidate for until it hears blocks again: duplicates, never a gap.
    pub(crate) fn clear_for_own_defunct(&self) {
        let mut table = self.0.peer_facts.write();
        table.held.clear();
        table.floor = last_decoded_seq();
        if let Some(local) = self.0.local.get() {
            local.clear_echoes();
        }
        for asked in self.0.exchanges.asked.lock().values_mut() {
            asked.dropped = true;
        }
        self.publish(&table);
    }

    /// Give this member's own block a source. Once: a second call is ignored.
    pub fn set_facts_source(&self, source: FactsSource) {
        let _ = self.0.local.set(LocalFacts::with_source(source));
    }

    /// The sync interval and fanout the lease is derived from.
    pub fn configure_lease(&self, sync_interval: Duration, fanout: usize) {
        let table = self.0.peer_facts.write();
        *self.0.lease_shape.write() = (sync_interval, fanout.max(1));
        self.publish(&table);
    }

    /// How long a peer's block is fresh: **at least `ceil((N - 1) / fanout) + 2`
    /// sync intervals**, derived from the current member count `N` and the
    /// fanout, not a constant. With a fanout below the peer count one round does
    /// not reach every peer, and a fixed lease would expire facts that are still
    /// being refreshed (ADR-201).
    pub fn lease(&self) -> Duration {
        let fanout = self.0.lease_shape.read().1.max(1);
        let peers = self.0.live.read().len();
        self.sync_interval() * (peers.div_ceil(fanout) as u32 + 2)
    }

    /// The sync interval this set was told. Unconfigured (a member set nothing
    /// has told its intervals): the default, so a lease is never zero.
    fn sync_interval(&self) -> Duration {
        let interval = self.0.lease_shape.read().0;
        if interval.is_zero() { crate::peers::DEFAULT_SYNC_INTERVAL } else { interval }
    }

    /// Ask for an immediate facts exchange with `node` at `addr`, as of `at`
    /// (ADR-247): **at most one per member per sync interval**, counted from the
    /// last ask whatever became of it, so a member that flaps or does not answer
    /// costs one exchange an interval and the tick is the fallback. Answers
    /// whether one was started now.
    ///
    /// **An ask the interval holds is not lost when it is owed**: if the member's
    /// block, or every block, was dropped since the last ask (a rename, a down,
    /// this member's own rejoin), one trailing ask is scheduled for the end of the
    /// interval, at most one pending per member, which
    /// [`Self::exchange_due_at`] starts. An ask that drops nothing since (the
    /// `MemberUp` beside a `Rename` in one input) owes none.
    ///
    /// Called after the change that asked for it is applied: an exchange started
    /// before a rename's drop could bring the new block back before the drop
    /// removed it again.
    pub(crate) fn exchange_at(&self, addr: SocketAddr, node: NodeId, at: Instant) -> bool {
        let interval = self.sync_interval();
        {
            let mut asked = self.0.exchanges.asked.lock();
            asked.retain(|_, last| {
                last.trailing || at.saturating_duration_since(last.at) < interval
            });
            if let Some(last) = asked.get_mut(&node) {
                let owed = last.dropped && !last.trailing;
                if owed {
                    last.at += interval;
                    last.trailing = true;
                }
                drop(asked);
                if owed {
                    self.0.exchanges.scheduled.notify_one();
                }
                return false;
            }
            asked.insert(node, Asked { at, dropped: false, trailing: false });
        }
        // Called with no lock of this set held: a sink may read and record blocks.
        let sink = Arc::clone(&self.0.exchanges.sink.read());
        sink(addr, node);
        true
    }

    /// Start every trailing ask due by `now` whose member is still live, at the
    /// address it is live at (ADR-247), and answer when the next one is due. A
    /// trailing ask counts as an ask at its due instant, started or not; **one that
    /// starts nothing (its member is down) keeps the drop it owed**, so the ask
    /// that member's next `Up` makes within the interval owes a trailing one.
    pub(crate) fn exchange_due_at(&self, now: Instant) -> Option<Instant> {
        // Read before the ask record is locked: `live` is never taken inside it.
        let live = self.entries();
        let due: Vec<(SocketAddr, NodeId)> = {
            let mut asked = self.0.exchanges.asked.lock();
            asked
                .iter_mut()
                .filter(|(_, last)| last.trailing && last.at <= now)
                .filter_map(|(node, last)| {
                    last.trailing = false;
                    let (addr, _) = live.iter().find(|(_, live)| live == node)?;
                    last.dropped = false;
                    Some((*addr, *node))
                })
                .collect()
        };
        if !due.is_empty() {
            let sink = Arc::clone(&self.0.exchanges.sink.read());
            for (addr, node) in due {
                sink(addr, node);
            }
        }
        self.next_trailing_exchange()
    }

    /// When the next trailing ask is due, if one is pending.
    pub(crate) fn next_trailing_exchange(&self) -> Option<Instant> {
        let asked = self.0.exchanges.asked.lock();
        asked.values().filter(|last| last.trailing).map(|last| last.at).min()
    }

    /// Wait until a trailing ask is scheduled (or one was since the last wait).
    pub(crate) async fn exchange_scheduled(&self) {
        self.0.exchanges.scheduled.notified().await;
    }

    /// Count an exchange the driver saw end, for a test that waits on one.
    #[cfg(test)]
    pub(crate) fn note_exchange_ended_for_test(&self) {
        self.0.exchanges.ended.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// How many exchanges the driver has seen end.
    #[cfg(test)]
    pub(crate) fn exchanges_ended_for_test(&self) -> usize {
        self.0.exchanges.ended.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// [`Self::exchange_at`] for every live peer: this member's own rejoin
    /// dropped every block it held (ADR-247).
    pub(crate) fn exchange_with_live_at(&self, at: Instant) {
        for (addr, node) in self.entries() {
            self.exchange_at(addr, node, at);
        }
    }

    /// The queue of asked exchanges, for the replication loop's driver. Once:
    /// `None` after the first call.
    pub(crate) fn take_exchange_queue(&self) -> Option<ExchangeQueue> {
        self.0.exchanges.queue.lock().take()
    }

    /// Start exchanges with `sink` instead of the queue: a test that runs them
    /// itself.
    #[cfg(test)]
    pub(crate) fn set_exchange_sink_for_test(&self, sink: ExchangeSink) {
        *self.0.exchanges.sink.write() = sink;
    }

    /// This member's block to send, with its generation, or `None` with no
    /// source. Cached (`facts::LocalFacts`).
    pub fn local_facts(&self) -> Option<(std::sync::Arc<Facts>, u64)> {
        self.local_facts_at(Instant::now())
    }

    pub(crate) fn local_facts_at(&self, now: Instant) -> Option<(std::sync::Arc<Facts>, u64)> {
        self.0.local.get()?.current(now)
    }

    /// This member's own block with `yielding` masked to the **effective** bits
    /// (ADR-213): what ownership consults for `me`. A class counts as yielded
    /// only once every live peer has echoed the generation that set it and the
    /// live set has held still for a lease; until then this member keeps owning
    /// it. The advertised block, which peers read, is [`Self::local_facts`].
    pub fn local_facts_effective(&self) -> Option<Arc<Facts>> {
        self.local_facts_effective_at(Instant::now())
    }

    pub(crate) fn local_facts_effective_at(&self, now: Instant) -> Option<Arc<Facts>> {
        let local = self.0.local.get()?;
        local.effective(now, || (self.node_ids(), self.live_stable_at(now)))
    }

    /// Whether the live set has held still for a lease. A member that has seen
    /// no change since it was created counts from the first time it is asked.
    fn live_stable_at(&self, now: Instant) -> bool {
        let lease = self.lease();
        let mut changed = self.0.live_changed.lock();
        let at = *changed.get_or_insert(now);
        now.saturating_duration_since(at) >= lease
    }

    /// What this member holds of `peer`'s block, to echo on the next frame to it
    /// (ADR-213). **Only what is recorded**: an empty boot and generation zero when
    /// nothing is held, which says "echoing, and confirming nothing".
    pub(crate) fn echo_for(&self, peer: NodeId) -> crate::protocol::Echo {
        self.0
            .peer_facts
            .read()
            .held
            .get(&peer)
            .map(|held| crate::protocol::Echo {
                boot: held.facts.boot.clone(),
                generation: held.generation.unwrap_or(0),
            })
            .unwrap_or_default()
    }

    /// A frame from `peer` carried `echo` (or none): the confirmation of this
    /// member's yield (ADR-213).
    pub(crate) fn note_echo(&self, peer: NodeId, echo: Option<&crate::protocol::Echo>) {
        if let Some(local) = self.0.local.get() {
            local.note_echo(peer, echo);
        }
    }

    /// A block `node` sent, decoded at `decoded_seq` (ADR-213).
    ///
    /// **Blocks are ordered by when they were decoded, and across boots by the
    /// sender's start time.** The held block is replaced only when one of these
    /// holds, and a block that does not replace it does not refresh `received`:
    /// - (a) same boot: decoded later **and** a generation at least the held one
    ///   (a 0.43 sender has none, which skips that half);
    /// - (b) a different boot: decoded later **and** `started_ms` at least the
    ///   held one's (either missing skips that half);
    /// - (c) a different boot, the held block past its lease, and this one decoded
    ///   later: the escape for a sender whose clock stepped back across a restart.
    ///   It is **never** for the same boot: within one process the generation does
    ///   not go backwards, however old the held block is.
    ///
    /// A record decoded before the slot's drop, or before this member's own
    /// `Defunct`, is ignored. Answers whether the block was taken.
    pub(crate) fn record_peer_facts(
        &self,
        node: NodeId,
        facts: Arc<Facts>,
        generation: Option<u64>,
        decoded_seq: u64,
        now: Instant,
    ) -> bool {
        let mut table = self.0.peer_facts.write();
        let floor = table.floor.max(table.dropped.get(&node).map_or(0, |(seq, _)| *seq));
        if decoded_seq <= floor {
            return false;
        }
        let lease = self.lease();
        let mut restarted = false;
        if let Some(held) = table.held.get(&node) {
            let later = decoded_seq > held.decoded_seq;
            let same_boot = held.facts.boot == facts.boot;
            let ordered = if same_boot {
                later && generation.is_none_or(|g| g >= held.generation.unwrap_or(0))
            } else {
                later
                    && match (facts.started_ms, held.facts.started_ms) {
                        (Some(new), Some(old)) => new >= old,
                        _ => true,
                    }
            };
            let escape =
                !same_boot && later && now.saturating_duration_since(held.received) > lease;
            if !(ordered || escape) {
                return false;
            }
            restarted = !same_boot;
        }
        table.held.insert(node, PeerFacts { facts, received: now, generation, decoded_seq });
        if let Some(local) = self.0.local.get() {
            // A new boot id is a new process: what the old one echoed of our
            // block says nothing of it.
            if restarted {
                local.forget(node);
            }
            local.retain(&self.node_ids());
        }
        // A peer that is not live and has been quiet for many leases is
        // forgotten, so the table follows the cluster and does not grow.
        let live: BTreeSet<NodeId> = self.node_ids();
        table.held.retain(|peer, held| {
            live.contains(peer) || now.saturating_duration_since(held.received) < lease * 10
        });
        // A drop floor is needed only until the slot holds a later block (which
        // already refuses anything decoded earlier), and while its peer might
        // still be heard from.
        let PeerTable { held, dropped, floor } = &mut *table;
        dropped.retain(|peer, (seq, at)| {
            *seq > *floor
                && !held.get(peer).is_some_and(|h| h.decoded_seq > *seq)
                && (live.contains(peer) || now.saturating_duration_since(*at) < lease * 10)
        });
        self.publish(&table);
        true
    }

    /// How this member sees each live peer.
    pub fn peer_states(&self) -> BTreeMap<NodeId, PeerState> {
        self.peer_states_at(Instant::now())
    }

    pub(crate) fn peer_states_at(&self, now: Instant) -> BTreeMap<NodeId, PeerState> {
        let lease = self.lease();
        let table = self.0.peer_facts.read();
        self.node_ids()
            .into_iter()
            .map(|node| {
                let state = match table.held.get(&node) {
                    None => PeerState::Unknown,
                    Some(held) if now.saturating_duration_since(held.received) > lease => {
                        PeerState::Stale
                    }
                    Some(held) if held.facts.catching_up => PeerState::IneligibleCatchingUp,
                    Some(held) if held.facts.yielding.any() => PeerState::IneligibleYielding,
                    Some(_) => PeerState::Eligible,
                };
                (node, state)
            })
            .collect()
    }

    /// Live peers per [`PeerState`], in [`PeerState::ALL`] order.
    pub fn peer_state_counts(&self) -> [u64; PeerState::ALL.len()] {
        let mut counts = [0u64; PeerState::ALL.len()];
        for state in self.peer_states().into_values() {
            counts[state.slot()] += 1;
        }
        counts
    }

    /// The candidates for `class` (for TTL, `collection`): the live peers that
    /// may own it by what they last said, plus this member if it may by its own
    /// block. **Never `catching_up`**; an empty set with yielding respected is
    /// tried again ignoring it, since an owner that is slow beats none. Empty
    /// after that means nobody may own it (ADR-201).
    ///
    /// **A peer this member holds no block from is not a candidate** for any class,
    /// in the fallback pass too (ADR-213): never heard, or forgotten on SWIM down,
    /// a rename, a new boot or this member's own `Defunct`. Exclusion only ever
    /// adds owners, so it costs duplicates, never a gap. TTL needs positive
    /// knowledge that the peer holds the index, **and a TTL listing is positive
    /// only while its block is within its lease**. A block past its lease keeps
    /// saying `catching_up` and what it yields.
    ///
    /// `mine` is this member's block with the **effective** yield bits
    /// ([`Self::local_facts_effective`]), not the advertised ones.
    pub fn candidates(
        &self,
        class: OwnerClass,
        collection: Option<CollectionId>,
        me: NodeId,
        mine: &Facts,
        me_holds_ttl: bool,
    ) -> BTreeSet<NodeId> {
        let table = self.0.peer_facts.read();
        let peers = self.node_ids();
        let (now, lease) = (Instant::now(), self.lease());
        for ignore_yielding in [false, true] {
            let mut set: BTreeSet<NodeId> = peers
                .iter()
                .filter(|peer| {
                    let held = table.held.get(*peer);
                    let fresh = held
                        .is_some_and(|held| now.saturating_duration_since(held.received) <= lease);
                    let theirs = held.map(|held| &*held.facts);
                    may_own(theirs, class, collection, None, ignore_yielding, fresh)
                })
                .copied()
                .collect();
            if may_own(Some(mine), class, collection, Some(me_holds_ttl), ignore_yielding, true) {
                set.insert(me);
            }
            if !set.is_empty() {
                return set;
            }
        }
        BTreeSet::new()
    }

    /// Whether every live peer writes this build's vector chunk ids, as its
    /// block says ([`Facts::vector_chunk_ids`], ADR-239): `Some(false)` while a
    /// live peer's block says otherwise (a build before 0.51.0 sends no such
    /// field), `None` while a live peer has not been heard and none says
    /// otherwise, `Some(true)` once every one has said so. The embedding
    /// worker judges again what an older member wrote during the roll when
    /// this turns from `Some(false)` to `Some(true)`.
    pub fn chunk_ids_everywhere(&self) -> Option<bool> {
        let table = self.0.peer_facts.read();
        let mut everywhere = Some(true);
        for peer in self.node_ids() {
            match table.held.get(&peer) {
                Some(held) if held.facts.vector_chunk_ids != Some(kimmy_core::CHUNK_ID_FORMAT) => {
                    return Some(false);
                }
                Some(_) => {}
                None => everywhere = None,
            }
        }
        everywhere
    }

    /// Whether a live peer that lists `collection` among its TTL indexes, and has
    /// expiry on, says it is catching up: what tells "every holder is catching
    /// up, so expiry waits" from "nobody holds the index" (ADR-201). One with
    /// expiry off would not expire it once caught up, so it does not count.
    pub fn holder_catching_up(&self, collection: CollectionId) -> bool {
        let table = self.0.peer_facts.read();
        self.node_ids().iter().any(|peer| {
            table.held.get(peer).is_some_and(|held| {
                held.facts.catching_up
                    && !held.facts.ttl_disabled
                    && held.facts.holds_ttl(collection)
            })
        })
    }

    /// The live peers that have not confirmed this member's yield of `class`:
    /// those that do not echo (a 0.43 peer, or one whose echo did not decode) and
    /// those whose echo is below the generation that set the bit. What a yielding
    /// member waits on before it stops owning (ADR-201, ADR-213). Empty when the
    /// member does not yield the class.
    pub fn unconfirmed_peers(&self, class: OwnerClass) -> Vec<NodeId> {
        let Some(local) = self.0.local.get() else { return Vec::new() };
        if local.current(Instant::now()).is_none() {
            return Vec::new();
        }
        local.unconfirmed(class, &self.node_ids())
    }

    /// The generation that set this member's yield of `class`, `None` while it
    /// does not yield it: what [`Self::unconfirmed_peers`] waits for an echo of.
    pub fn yield_generation(&self, class: OwnerClass) -> Option<u64> {
        self.0.local.get()?.yield_generation(class)
    }

    /// Record a block a peer sent, as if a contact had carried it now. For
    /// tests in crates that consume this set, whose point is what ownership does
    /// with what members say (ADR-201).
    /// `age` back-dates the block, so a test can make it stale.
    pub fn record_peer_facts_for_test(&self, node: NodeId, facts: Facts, age: Duration) {
        let received = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        self.record_peer_facts(node, Arc::new(facts), None, next_decoded_seq(), received);
    }

    /// Hold the table's write lock, as a stalled worker could: a test that the
    /// yield evaluator never waits on it.
    #[cfg(test)]
    pub(crate) fn write_lock_for_test(&self) -> impl Drop + '_ {
        self.0.peer_facts.write()
    }

    /// How many drop floors the table keeps.
    #[cfg(test)]
    pub(crate) fn drop_floors_for_test(&self) -> usize {
        self.0.peer_facts.read().dropped.len()
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

    /// Move `addr`'s generation on, as SWIM bringing it up and declaring it down
    /// again between two looks would, and leave whether it is live as it was.
    #[cfg(test)]
    pub(crate) fn bump_generation_for_test(&self, addr: &SocketAddr) {
        let generation = self.0.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        self.0.generations.write().insert(*addr, generation);
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

/// This crate's SWIM instance.
pub(crate) type Swim = Foca<Member, PostcardCodec, rand::rngs::StdRng, NoCustomBroadcast>;

/// Where [`after_input`] records a renewed incarnation (ADR-242): the data
/// directory's file in the daemon.
pub(crate) trait KeepIncarnation {
    fn record(&mut self, incarnation: u64);
}

impl KeepIncarnation for IncarnationFile {
    fn record(&mut self, incarnation: u64) {
        IncarnationFile::record(self, incarnation);
    }
}

pub(crate) use step::{Outgoing, after_input};

/// The daemon's step after every input, and the queue of what the input sends,
/// which **only that step's answer opens**: a datagram an input queued can be
/// taken only with the `Recorded` that [`after_input`] returns once it has
/// recorded the input's renewal, so no datagram carries a renewed identity
/// before it is recorded (ADR-242). Nothing outside this module can make one.
mod step {
    use super::{KeepIncarnation, Member, Runtime, SocketAddr, Swim, warn};

    /// That an input's renewal, if it made one, is recorded: what its datagrams
    /// wait for. Only [`after_input`] makes one.
    #[must_use]
    pub(crate) struct Recorded(());

    /// The datagrams an input queued, held until its renewal is recorded.
    #[derive(Default)]
    pub(crate) struct Outgoing(Vec<(SocketAddr, Vec<u8>)>);

    impl Outgoing {
        pub(crate) fn push(&mut self, to: SocketAddr, data: Vec<u8>) {
            self.0.push((to, data));
        }

        /// Everything queued, to send now.
        pub(crate) fn take(&mut self, _recorded: Recorded) -> Vec<(SocketAddr, Vec<u8>)> {
            std::mem::take(&mut self.0)
        }
    }

    /// What follows every input foca handles: after one that made this member
    /// rejoin, foca is asked to judge its connection state again.
    ///
    /// **Why (foca 2.0.0).** A member that learns it was declared down by a
    /// `TurnUndead` message (the one a peer sends to a member it declares down,
    /// which a member that was paused or cut off reads first when it is back)
    /// rejoins inside `handle_data` through `attempt_rejoin`, which calls
    /// `change_identity`, which resets the instance to `Disconnected`, bumping its
    /// timer token so every timer already scheduled is ignored. Only `apply_many`
    /// judges the connection state again (its last step makes an instance with
    /// active members `Connected`, and schedules its probe, gossip and announce
    /// timers), and the `TurnUndead` paths (from a sender held active, and from one
    /// held down) rejoin after it, or without it. The member then probes no one,
    /// gossips nothing, announces nothing and acks no ping, while still listing its
    /// peers live: until an active peer happens to send it something its new
    /// identity accepts, which, with every peer it could hear from paused or dead,
    /// is never. `apply_many` with no updates is foca's public way to have the
    /// state judged again; this call can go once foca does it itself on that path.
    ///
    /// `rejoined` is whether the input's handling notified `Rejoin`.
    ///
    /// **The renewed incarnation is recorded first**, in `kept`: the answer is
    /// what opens the input's datagrams ([`Outgoing::take`]), so the identity
    /// is on disk before any datagram carries it, and the next start is above
    /// it (ADR-242).
    pub(crate) fn after_input(
        swim: &mut Swim,
        rejoined: bool,
        runtime: impl Runtime<Member>,
        kept: &mut impl KeepIncarnation,
    ) -> Recorded {
        if rejoined {
            kept.record(swim.identity().incarnation);
            if let Err(e) = swim.apply_many(std::iter::empty(), false, runtime) {
                warn!(error = %e, "membership could not resume after rejoining the cluster");
            }
        }
        Recorded(())
    }
}

/// The addresses to announce this member to when discovery offers `offered`:
/// that address, and, **while this member holds no active member**, every other
/// address in foca's member table, the members it holds down included.
///
/// **Why.** A member with no active member is `Disconnected`, and every timer
/// foca schedules for itself (probe, gossip, announce, announce to the members
/// it holds down) needs it connected: it says nothing until something reaches
/// it. Discovery offers the configured seeds only, so two members that both
/// rejoined after a pause, each holding the other's earlier identity down and
/// unaware of its new one, whose seeds are all gone, would each stay alone for
/// good. Announcing to what the table still knows lets each find the other, as
/// foca's own announce to down members would if it were connected.
///
/// **Cost.** Bounded by the member table (a member held down stays in it for
/// `Config::remove_down_after`, 24 hours), and only while this member is alone;
/// the membership loop sends it at most once every [`ALONE_ANNOUNCE_EVERY`].
pub(crate) fn announce_targets(swim: &Swim, offered: SocketAddr) -> Vec<SocketAddr> {
    let mut targets = vec![offered];
    if swim.num_members() == 0 {
        let own = swim.identity().addr;
        for member in swim.iter_membership_state() {
            let addr = member.id().addr;
            if addr != own && !targets.contains(&addr) {
                targets.push(addr);
            }
        }
    }
    targets
}

/// The least time between two announces to the whole member table by a member
/// that is alone ([`announce_targets`]): discovery offers each seed every
/// interval, and the table is announced to once for all of them.
const ALONE_ANNOUNCE_EVERY: Duration = Duration::from_secs(1);

/// Collects foca's outputs so they can be dispatched after the call returns.
///
/// foca's `Runtime` is synchronous, but sending a datagram and sleeping are
/// not, so nothing is performed here — the work is queued and flushed by the
/// caller, which is the only place that can `await`.
struct Collector {
    outgoing: Outgoing,
    timers: Vec<(Timer<Member>, Duration)>,
    members: Members,
    /// Whether the input being handled made this member rejoin.
    rejoined: bool,
    /// When the input being handled was taken: what the exchanges it asks for
    /// are debounced by (ADR-247).
    at: Instant,
}

/// Apply what `notification` says to `members`, at `at`, and ask for the
/// immediate facts exchanges it calls for (ADR-247). Answers whether it was this
/// member's own `Rejoin`.
///
/// **Each exchange is asked for after the change is applied**: after a rename's
/// drop, so the block the exchange brings is decoded after the drop and kept
/// (ADR-213), and after a rejoin's clear, for the same reason.
fn apply_notification(
    members: &Members,
    notification: Notification<'_, Member>,
    at: Instant,
) -> bool {
    match notification {
        Notification::MemberUp(member) => {
            info!(peer = %member.addr, node = %member.node, "member up");
            members.insert(member.addr, member.node);
            members.exchange_at(member.addr, member.node, at);
        }
        Notification::MemberDown(member) => {
            info!(peer = %member.addr, "member down");
            members.remove(&member.addr);
        }
        // A member came back under a new incarnation. The address is
        // unchanged, so the live set already contains it.
        Notification::Rename(old, new) => {
            debug!(from = ?old, to = ?new, "member renewed its identity");
            // A new incarnation: what the old one said, and read, is not
            // this one's (ADR-201). Only `insert` follows, and it drops
            // nothing, since a block can beat the insert of a peer that is
            // new to this member.
            members.forget_peer(old.node);
            members.forget_peer(new.node);
            members.insert(new.addr, new.node);
            members.exchange_at(new.addr, new.node, at);
        }
        Notification::Defunct => {
            warn!("this node was declared down by the cluster; rejoining");
            // The cluster dropped this member's block, and this member may
            // have missed changes to its peers': clear what it routes by
            // and what it counted as confirmed (ADR-213).
            members.clear_for_own_defunct();
        }
        Notification::Rejoin(identity) => {
            info!(as_member = %identity.addr, "rejoined the cluster");
            members.clear_for_own_defunct();
            members.exchange_with_live_at(at);
            return true;
        }
        other => debug!(?other, "membership notification"),
    }
    false
}

impl foca::Runtime<Member> for Collector {
    fn notify(&mut self, notification: Notification<'_, Member>) {
        if apply_notification(&self.members, notification, self.at) {
            self.rejoined = true;
        }
    }

    fn send_to(&mut self, to: Member, data: &[u8]) {
        self.outgoing.push(to.addr, data.to_vec());
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
/// can name it independently of where it is listening. `incarnation` is where
/// its incarnation is kept between processes, already holding this process's
/// first ([`IncarnationFile::begin`], ADR-242).
#[allow(clippy::too_many_arguments)]
pub async fn run(
    socket: UdpSocket,
    local: SocketAddr,
    node: NodeId,
    mut incarnation: IncarnationFile,
    secret: String,
    members: Members,
    seeds: SeedFeed,
    shutdown: kimmy_task::Shutdown,
) {
    let identity = Member::identified(local, node, incarnation.current());
    let config =
        Config::new_lan(std::num::NonZeroU32::new(CLUSTER_SIZE_HINT).expect("non-zero literal"));

    // Seeded from the OS. SWIM uses randomness to pick probe targets, so a
    // predictable sequence would make every node probe in the same order.
    let rng: rand::rngs::StdRng = rand::make_rng();
    let mut foca: Swim = Foca::new(identity, config, rng, PostcardCodec);
    let socket = Arc::new(socket);
    let (tx, mut rx) = mpsc::channel::<Input>(1024);

    // Inbound datagrams. Supervised in its own right, because supervising
    // `run` does not cover it: if this task dies, `run` goes on looping and
    // foca simply stops hearing anything, so the member set freezes exactly as
    // if membership had died (ADR-184).
    // The handles are kept only to name them. Nothing aborts these: the loop
    // below never ends, and each supervisor stops its own work when shutdown
    // begins.
    let _inbound = kimmy_task::supervise_judged("membership_inbound", shutdown.clone(), {
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
                            // The loop this feeds has gone, so there is nothing
                            // left to receive for. Ordinary, and not a death:
                            // it happens whenever `run` ends or is stopped.
                            return kimmy_task::Ended::Expected(
                                "the membership loop it feeds has gone",
                            );
                        }
                    }
                    Err(e) => {
                        // The socket is the only way membership hears anything,
                        // so this is a death: without it foca stops receiving
                        // and the member set freezes at whatever it last knew,
                        // while `run` goes on looping (ADR-184).
                        warn!(error = %e, "membership socket read failed");
                        return kimmy_task::Ended::Unexpected(
                            "the membership socket could not be read",
                        );
                    }
                }
            }
        }
    });

    // Discovered addresses to announce ourselves to. **One-shot, not endless**:
    // it drains the seed feed and ends, and it also ends when the receiver is
    // gone. Both are expected, so a return here must not read as a death --
    // which is not how it was first written, and the membership tests found it
    // within seconds by stopping the test binary with an exit code. A panic is
    // still fatal: an announcement lost to one is a node that never introduces
    // itself to a seed.
    let _announcing = kimmy_task::supervise_oneshot("membership_announce", shutdown.clone(), {
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

    let mut collector = Collector {
        outgoing: Outgoing::default(),
        timers: Vec::new(),
        members,
        rejoined: false,
        at: Instant::now(),
    };

    let mut announced_alone: Option<Instant> = None;
    while let Some(input) = rx.recv().await {
        collector.at = Instant::now();
        let result = match input {
            Input::Data(bytes) => foca.handle_data(&bytes, &mut collector),
            Input::Timer(timer) => foca.handle_timer(timer, &mut collector),
            Input::Announce(addr) => {
                let alone_due = foca.num_members() == 0
                    && announced_alone.is_none_or(|at| at.elapsed() >= ALONE_ANNOUNCE_EVERY);
                let targets = if alone_due {
                    announced_alone = Some(Instant::now());
                    announce_targets(&foca, addr)
                } else {
                    vec![addr]
                };
                let mut result = Ok(());
                for target in targets {
                    if let Err(e) = foca.announce(Member::announcing(target), &mut collector) {
                        result = Err(e);
                    }
                }
                result
            }
        };

        if let Err(e) = result {
            // A malformed datagram from one peer must not stop membership for
            // everyone; foca's state is unchanged by a rejected message.
            debug!(error = %e, "membership input rejected");
        }
        let rejoined = std::mem::take(&mut collector.rejoined);
        let recorded = after_input(&mut foca, rejoined, &mut collector, &mut incarnation);

        // Dispatch what foca asked for, which only the record opens. Datagrams
        // are best-effort by design — SWIM assumes loss and probes again — so a
        // send failure is logged at debug rather than treated as an error.
        for (addr, data) in collector.outgoing.take(recorded) {
            let data = protocol::tag_datagram(&secret, &data);
            if let Err(e) = socket.send_to(&data, addr).await {
                debug!(peer = %addr, error = %e, "membership send failed");
            }
        }

        for (event, after) in collector.timers.drain(..) {
            let tx = tx.clone();
            // Every submitted timer MUST be delivered — foca tolerates delay
            // but not loss — so this is a task per timer rather than a wheel we
            // could get wrong. And because a *lost* timer is what foca cannot
            // tolerate, this is the one-shot shape: ending is the normal case
            // and must not read as a death, while a panic is fatal, since a
            // timer that unwinds freezes membership as surely as a dead
            // receiver would (ADR-184).
            kimmy_task::supervise_oneshot("membership_timer", shutdown.clone(), async move {
                tokio::time::sleep(after).await;
                let _ = tx.send(Input::Timer(event)).await;
            });
        }
    }

    // **Unreachable, and that is the point.** `run` holds `tx` for its whole
    // body -- the timer arm above clones it -- so `rx.recv()` cannot return
    // `None` and this loop cannot end. `run` is itself the supervised
    // `membership` task, and a task that never returns is exactly what
    // `supervise` wants.
    //
    // Two aborts used to sit here, on `inbound` and `announcing`, with a comment
    // explaining that a child outliving this loop would return and be read as a
    // death. Nothing reached them. They are gone rather than kept as reassuring
    // dead code, and the children are stopped by their own supervisors when
    // shutdown begins -- which is only true since the supervisors started
    // stopping their work instead of detaching it (ADR-184).
    unreachable!("the membership loop holds a sender, so its channel never closes")
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

pub mod incarnation;

#[cfg(test)]
mod yield_tests;

#[cfg(test)]
mod rejoin_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    /// SWIM's `Rename` (a node renewing its identity after being wrongly declared
    /// down) is a new incarnation: the block it sent and what it read of ours are
    /// dropped, where `insert` alone drops nothing (ADR-201).
    #[test]
    fn a_renamed_member_is_a_new_incarnation_and_is_forgotten() {
        use foca::Runtime;
        let members = Members::default();
        members.set_facts_source(std::sync::Arc::new(|| Facts {
            boot: vec![9; 16],
            yielding: crate::facts::Yielding { ttl: true, ..Default::default() },
            ..Facts::default()
        }));
        let old = Member::identified(addr(7900), node(1), 0);
        let new = old.renew().unwrap();
        members.insert_for_test(addr(7900), node(1));
        members.record_peer_facts_for_test(
            node(1),
            Facts { catching_up: true, boot: vec![1; 16], ..Facts::default() },
            Duration::ZERO,
        );
        let (_, generation) = members.local_facts().unwrap();
        members.note_echo(node(1), Some(&protocol::Echo { boot: vec![9; 16], generation }));
        assert_eq!(members.peer_states()[&node(1)], PeerState::IneligibleCatchingUp);
        assert!(members.unconfirmed_peers(OwnerClass::Ttl).is_empty());

        let mut collector = Collector {
            outgoing: Outgoing::default(),
            timers: Vec::new(),
            members: members.clone(),
            rejoined: false,
            at: Instant::now(),
        };
        collector.notify(Notification::Rename(&old, &new));
        assert_eq!(members.peer_states()[&node(1)], PeerState::Unknown, "its block is dropped");
        assert_eq!(
            members.unconfirmed_peers(OwnerClass::Ttl),
            vec![node(1)],
            "and what it read of ours"
        );
    }

    /// An immediate exchange is asked at most once per member per sync interval
    /// (ADR-247), counted from the last ask, and each member on its own: a
    /// second ask within the interval starts none, another member's starts one,
    /// and one at the interval's end starts one again.
    #[test]
    fn an_exchange_is_asked_once_per_member_per_sync_interval() {
        let members = Members::default();
        members.configure_lease(Duration::from_secs(5), 3);
        let started = Arc::new(Mutex::new(Vec::new()));
        members.set_exchange_sink_for_test({
            let started = Arc::clone(&started);
            Arc::new(move |addr, node| started.lock().push((addr, node)))
        });
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        assert!(members.exchange_at(addr(7901), node(1), at(0)));
        assert!(!members.exchange_at(addr(7901), node(1), at(4_999)), "within the interval");
        assert!(members.exchange_at(addr(7902), node(2), at(1_000)), "another member's own");
        assert!(members.exchange_at(addr(7901), node(1), at(5_000)), "the interval is over");
        assert!(!members.exchange_at(addr(7902), node(2), at(5_000)), "from its own last ask");
        assert_eq!(
            *started.lock(),
            vec![(addr(7901), node(1)), (addr(7902), node(2)), (addr(7901), node(1))]
        );
    }

    /// An ask the interval holds owes one trailing ask at the interval's end when
    /// the member's block was dropped since the last ask, and none otherwise; at
    /// most one is pending per member; it starts at the address the member is live
    /// at then, not at all if it is not live, and counts as an ask at its due
    /// instant (ADR-247).
    #[test]
    fn an_ask_the_interval_holds_owes_one_trailing_ask_only_after_a_drop() {
        let members = Members::default();
        members.configure_lease(Duration::from_secs(5), 3);
        let started = Arc::new(Mutex::new(Vec::new()));
        members.set_exchange_sink_for_test({
            let started = Arc::clone(&started);
            Arc::new(move |addr, node| started.lock().push((addr, node)))
        });
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        members.insert_for_test(addr(7901), node(1));
        members.insert_for_test(addr(7902), node(2));
        assert!(members.exchange_at(addr(7901), node(1), at(0)));
        assert!(!members.exchange_at(addr(7901), node(1), at(10)), "nothing dropped since");
        assert_eq!(members.next_trailing_exchange(), None, "so none is owed");

        members.remove_for_test(&addr(7901));
        members.insert_for_test(addr(7901), node(1));
        assert!(!members.exchange_at(addr(7901), node(1), at(1_000)));
        assert_eq!(members.next_trailing_exchange(), Some(at(5_000)), "owed at the interval's end");
        members.remove_for_test(&addr(7901));
        members.insert_for_test(addr(7911), node(1));
        assert!(!members.exchange_at(addr(7911), node(1), at(2_000)));
        assert_eq!(members.next_trailing_exchange(), Some(at(5_000)), "one pending");

        assert_eq!(members.exchange_due_at(at(4_999)), Some(at(5_000)), "not yet due");
        assert_eq!(started.lock().len(), 1);
        assert_eq!(members.exchange_due_at(at(5_000)), None);
        assert_eq!(
            *started.lock(),
            vec![(addr(7901), node(1)), (addr(7911), node(1))],
            "at the address it is live at"
        );
        assert!(!members.exchange_at(addr(7911), node(1), at(9_999)), "an ask at its due instant");
        assert_eq!(members.next_trailing_exchange(), None, "nothing dropped since it");

        // Owed to a member that is no longer live when it is due: not started.
        assert!(members.exchange_at(addr(7902), node(2), at(0)));
        members.remove_for_test(&addr(7902));
        assert!(!members.exchange_at(addr(7902), node(2), at(100)));
        assert_eq!(members.exchange_due_at(at(5_000)), None);
        assert_eq!(started.lock().len(), 3, "the member is not live");
    }

    /// A member down when its trailing ask is due, and up again within the next
    /// interval: the trailing ask started nothing, so the drop it owed is still
    /// owed, and the up's ask, held by the interval, owes a trailing one (ADR-247).
    #[test]
    fn a_trailing_ask_due_while_the_member_is_down_leaves_the_next_drop_owed() {
        let members = Members::default();
        members.configure_lease(Duration::from_secs(5), 3);
        let started = Arc::new(Mutex::new(Vec::new()));
        members.set_exchange_sink_for_test({
            let started = Arc::clone(&started);
            Arc::new(move |addr, node| started.lock().push((addr, node)))
        });
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        members.insert_for_test(addr(7901), node(1));
        assert!(members.exchange_at(addr(7901), node(1), at(0)));
        // Down and up (a restart within the interval): owed, trailing at 5 s.
        members.remove_for_test(&addr(7901));
        members.insert_for_test(addr(7901), node(1));
        assert!(!members.exchange_at(addr(7901), node(1), at(1_000)));
        assert_eq!(members.next_trailing_exchange(), Some(at(5_000)), "premise: owed");
        // Down again before it is due; due while down: nothing starts.
        members.remove_for_test(&addr(7901));
        assert_eq!(members.exchange_due_at(at(5_000)), None);
        assert_eq!(started.lock().len(), 1, "premise: not started, the member is down");
        // Up again within the interval of the due instant: its block is gone.
        members.insert_for_test(addr(7901), node(1));
        assert!(!members.exchange_at(addr(7901), node(1), at(6_000)), "held by the interval");
        assert_eq!(members.next_trailing_exchange(), Some(at(10_000)), "and owes a trailing ask");
        assert_eq!(members.exchange_due_at(at(10_000)), None);
        assert_eq!(started.lock().len(), 2, "started at the end of the interval");
    }

    #[test]
    fn a_renewed_identity_displaces_the_one_it_replaces() {
        // Without this a node wrongly declared down could never rejoin: it
        // would keep offering an identity the cluster has already buried.
        let original = Member::identified(addr(7900), node(1), 0);
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
        let member = Member::identified(addr(7900), node(1), 0);
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
        let real = Member::identified(addr(7900), node(7), 0);

        assert!(real.win_addr_conflict(&placeholder), "a real identity must always displace one");
        assert!(!placeholder.win_addr_conflict(&real));
    }

    #[test]
    fn a_real_identity_displaces_the_placeholder_an_announce_carries() {
        // Discovery yields an address; the identity behind it is whatever
        // answers. If the placeholder could win, a peer would be remembered as
        // the address it was found at and never as itself.
        let placeholder = Member::announcing(addr(7900));
        let real = Member::identified(addr(7900), node(7), 0);

        assert!(real.win_addr_conflict(&placeholder), "the real identity must win");
        assert!(!placeholder.win_addr_conflict(&real), "and the placeholder must not");
    }

    #[test]
    fn two_nodes_claiming_one_address_resolve_the_same_way_on_both_sides() {
        // A replacement that reused an address: both start at incarnation 0, so
        // without a tiebreak neither displaces the other and the address stalls
        // on whichever was seen first. The rule is arbitrary but must be
        // *agreed* — exactly one of the two comparisons may be true.
        let a = Member::identified(addr(7900), node(1), 0);
        let b = Member::identified(addr(7900), node(2), 0);

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
