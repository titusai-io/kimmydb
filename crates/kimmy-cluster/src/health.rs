//! Which peers to talk to this round.
//!
//! Two problems, both of which appear only once a cluster is more than a pair.
//!
//! **A peer that has gone away is retried every interval, forever.** Nothing
//! breaks — anti-entropy is idempotent and a failed round costs a refused
//! connection — but the log fills with the same failure and every round pays
//! for a node that is not coming back.
//!
//! **Every node syncs with every other node.** That is O(n²) connections per
//! interval across the cluster, and the interval is short.
//!
//! Neither needs cluster-wide agreement to fix, which is why this is local
//! bookkeeping rather than gossip. What it does *not* provide is a shared
//! opinion about which nodes are alive, or knowledge of nodes absent from
//! discovery — see [ADR-037](../../../docs/decisions.md) for why that trade was
//! taken.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Peers contacted per round, before backoff is considered.
///
/// Small and constant rather than a fraction of the cluster: anti-entropy is
/// transitive, so a write reaches everyone through intermediate peers without
/// every node having to contact every other. Three keeps propagation quick
/// while making the per-round cost independent of cluster size.
pub const DEFAULT_FANOUT: usize = 3;

/// The longest a failing peer is left alone.
///
/// Capped so a peer that comes back is noticed within a bounded time, however
/// long it was away.
pub const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// What is known about one peer, from this node's own attempts.
#[derive(Clone, Copy, Debug)]
struct PeerState {
    /// Consecutive failures. Reset by any success.
    failures: u32,
    /// Not worth contacting before this.
    next_attempt: Instant,
}

/// Local health bookkeeping and round-robin peer selection.
pub struct PeerHealth {
    state: HashMap<SocketAddr, PeerState>,
    /// Rotates each round so selection is fair without needing randomness.
    ///
    /// Round-robin rather than a random sample: every peer gets contacted
    /// within a bounded number of rounds rather than probably-eventually, and
    /// the behaviour is reproducible in a test. Correlated choices across nodes
    /// would matter for gossip dissemination, where the point is to spread a
    /// message; here each node is pulling for itself.
    cursor: usize,
    fanout: usize,
    base: Duration,
}

impl PeerHealth {
    pub fn new(fanout: usize, base: Duration) -> Self {
        Self { state: HashMap::new(), cursor: 0, fanout: fanout.max(1), base }
    }

    /// Choose which peers to contact now.
    pub fn select(&mut self, peers: &BTreeSet<SocketAddr>, now: Instant) -> Vec<SocketAddr> {
        if peers.is_empty() {
            return Vec::new();
        }

        // Peers in backoff are skipped, not counted against the fanout: three
        // unreachable peers must not starve a reachable fourth.
        let ready: Vec<SocketAddr> = peers
            .iter()
            .copied()
            .filter(|peer| self.state.get(peer).is_none_or(|s| s.next_attempt <= now))
            .collect();
        if ready.is_empty() {
            return Vec::new();
        }

        let take = self.fanout.min(ready.len());
        let start = self.cursor % ready.len();
        let chosen: Vec<SocketAddr> = (0..take).map(|i| ready[(start + i) % ready.len()]).collect();

        // Advance by what was taken, so the next round continues rather than
        // re-covering the same peers.
        self.cursor = start + take;
        chosen
    }

    /// Record a round that worked.
    pub fn succeeded(&mut self, peer: SocketAddr) {
        self.state.remove(&peer);
    }

    /// Record a round that did not, and back off.
    pub fn failed(&mut self, peer: SocketAddr, now: Instant) {
        let entry = self.state.entry(peer).or_insert(PeerState { failures: 0, next_attempt: now });
        entry.failures = entry.failures.saturating_add(1);
        entry.next_attempt = now + backoff(self.base, entry.failures);
    }

    /// Consecutive failures recorded for a peer.
    pub fn failures(&self, peer: SocketAddr) -> u32 {
        self.state.get(&peer).map_or(0, |s| s.failures)
    }

    /// Peers currently being backed off.
    pub fn backing_off(&self, now: Instant) -> usize {
        self.state.values().filter(|s| s.next_attempt > now).count()
    }
}

/// Exponential backoff, capped.
fn backoff(base: Duration, failures: u32) -> Duration {
    // Shift rather than repeated multiply, and saturate: a peer down for a long
    // time would otherwise overflow the exponent into a wrapped, tiny delay —
    // turning a long outage back into a hot loop.
    let factor = 1u32.checked_shl(failures.min(16).saturating_sub(1)).unwrap_or(u32::MAX);
    base.saturating_mul(factor).min(MAX_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peers(n: usize) -> BTreeSet<SocketAddr> {
        (0..n).map(|i| format!("127.0.0.1:{}", 7900 + i).parse().unwrap()).collect()
    }

    fn health() -> PeerHealth {
        PeerHealth::new(DEFAULT_FANOUT, Duration::from_secs(5))
    }

    #[test]
    fn a_round_contacts_at_most_the_fanout() {
        // The whole point: per-round cost independent of cluster size.
        let mut h = health();
        let chosen = h.select(&peers(50), Instant::now());
        assert_eq!(chosen.len(), DEFAULT_FANOUT);
    }

    #[test]
    fn a_small_cluster_contacts_everyone() {
        let mut h = health();
        let chosen = h.select(&peers(2), Instant::now());
        assert_eq!(chosen.len(), 2, "fanout is a cap, not a quota");
    }

    #[test]
    fn every_peer_is_reached_within_a_bounded_number_of_rounds() {
        // Round-robin rather than sampling, so this is "within ceil(n/fanout)"
        // rather than "probably, eventually".
        let all = peers(9);
        let mut h = health();
        let now = Instant::now();

        let mut seen = BTreeSet::new();
        for _ in 0..3 {
            seen.extend(h.select(&all, now));
        }

        assert_eq!(seen, all, "three rounds of three must cover nine peers");
    }

    #[test]
    fn a_failing_peer_is_retried_less_and_less_often() {
        let mut h = health();
        let now = Instant::now();
        let peer = *peers(1).iter().next().unwrap();

        h.failed(peer, now);
        let first = h.select(&peers(1), now);
        assert!(first.is_empty(), "a peer just failed must not be retried immediately");

        // Still backed off well after the sync interval would have come round.
        assert!(h.select(&peers(1), now + Duration::from_secs(4)).is_empty());
        // ...but eventually retried.
        assert!(!h.select(&peers(1), now + Duration::from_secs(6)).is_empty());
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let base = Duration::from_secs(5);
        assert_eq!(backoff(base, 1), Duration::from_secs(5));
        assert_eq!(backoff(base, 2), Duration::from_secs(10));
        assert_eq!(backoff(base, 3), Duration::from_secs(20));
        assert_eq!(backoff(base, 60), MAX_BACKOFF, "a long outage must not delay recovery forever");
    }

    #[test]
    fn a_very_long_outage_does_not_wrap_into_a_hot_loop() {
        // Shifting by more than the width would wrap, turning a long backoff
        // into a tiny one — the opposite of what a long outage should do.
        for failures in [16u32, 31, 32, 33, 1000, u32::MAX] {
            assert_eq!(backoff(Duration::from_secs(5), failures), MAX_BACKOFF, "{failures}");
        }
    }

    #[test]
    fn success_clears_the_backoff() {
        let mut h = health();
        let now = Instant::now();
        let peer = *peers(1).iter().next().unwrap();

        h.failed(peer, now);
        h.failed(peer, now);
        assert_eq!(h.failures(peer), 2);

        h.succeeded(peer);
        assert_eq!(h.failures(peer), 0);
        assert!(
            !h.select(&peers(1), now).is_empty(),
            "a recovered peer is contacted again at once"
        );
    }

    #[test]
    fn unreachable_peers_do_not_starve_a_reachable_one() {
        // Backed-off peers are filtered before the fanout is applied. Counting
        // them would let three dead peers hide a live fourth every round.
        let all = peers(4);
        let mut h = health();
        let now = Instant::now();

        let mut sorted = all.iter().copied();
        let (a, b, c, live) = (
            sorted.next().unwrap(),
            sorted.next().unwrap(),
            sorted.next().unwrap(),
            sorted.next().unwrap(),
        );
        for dead in [a, b, c] {
            h.failed(dead, now);
        }

        assert_eq!(h.select(&all, now), vec![live], "the reachable peer must still be contacted");
    }

    #[test]
    fn no_peers_is_not_an_error() {
        let mut h = health();
        assert!(h.select(&BTreeSet::new(), Instant::now()).is_empty());
    }
}
