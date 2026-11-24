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

/// How often a peer that keeps failing is reported at warning level.
///
/// Bounded in both directions on purpose. Reporting every round turns a log
/// into noise nobody reads; reporting only the first failure — which is what
/// this replaced — hides an outage that never ends. Five minutes is frequent
/// enough that a wedged cluster is visible within one alerting window and rare
/// enough that a peer down overnight costs a handful of lines.
pub const WARN_INTERVAL: Duration = Duration::from_secs(300);

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
    /// When this peer's failure was last reported at warning level.
    ///
    /// Only the first failure used to be reported, and every one after it went
    /// to `debug`, which is below the default level. That is right for a peer
    /// that is rebooting and wrong for one that never comes back: a permanently
    /// failing peer said nothing at all, so a cluster could sit half-converged
    /// indefinitely with a clean log.
    last_warned: Instant,
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
        self.select_excluding(peers, &BTreeSet::new(), now, 0)
    }

    /// [`Self::select`], for a tick that has already given `slots` of this
    /// node's fanout budget to peers carried forward from a draining contact
    /// (ADR-195): picks up to `fanout - slots` more from `peers`,
    /// never one already in `excluded`, so what a tick contacts in total —
    /// carried plus freshly selected — never exceeds `fanout`.
    ///
    /// `slots` rather than deriving it from `excluded.len()`: the caller
    /// passes `carried.len()`, where `carried` is the carried set already
    /// filtered against this tick's live `peers`. A carried peer that filter
    /// dropped (SWIM marked it down since) is in neither `carried` nor
    /// `excluded`, and spends no slot this tick at all — its removal frees
    /// the slot for a fresh pick rather than leaving it stranded on a peer
    /// this tick will not dial.
    pub fn select_excluding(
        &mut self,
        peers: &BTreeSet<SocketAddr>,
        excluded: &BTreeSet<SocketAddr>,
        now: Instant,
        slots: usize,
    ) -> Vec<SocketAddr> {
        let budget = self.fanout.saturating_sub(slots);
        if budget == 0 || peers.is_empty() {
            return Vec::new();
        }

        // Peers in backoff are skipped, not counted against the fanout: three
        // unreachable peers must not starve a reachable fourth. Peers already
        // excluded (carried forward this tick) are skipped the same way, so
        // this never hands out a peer twice in one tick.
        let ready: Vec<SocketAddr> = peers
            .iter()
            .copied()
            .filter(|peer| !excluded.contains(peer))
            .filter(|peer| self.state.get(peer).is_none_or(|s| s.next_attempt <= now))
            .collect();
        if ready.is_empty() {
            return Vec::new();
        }

        let take = budget.min(ready.len());
        let start = self.cursor % ready.len();
        let chosen: Vec<SocketAddr> = (0..take).map(|i| ready[(start + i) % ready.len()]).collect();

        // Advance by what was taken, so the next round continues rather than
        // re-covering the same peers.
        self.cursor = start + take;
        chosen
    }

    /// Up to `n` live peers that are not backing off and not in `excluded`,
    /// without moving the rotation: the ones a tick could add to its contacts when
    /// one of them has set aside its snapshot (ADR-224). Which of them comes first
    /// follows the rotation, so the extras differ from tick to tick as the chosen do.
    pub fn extras(
        &self,
        peers: &BTreeSet<SocketAddr>,
        excluded: &BTreeSet<SocketAddr>,
        now: Instant,
        n: usize,
    ) -> Vec<SocketAddr> {
        let ready: Vec<SocketAddr> = peers
            .iter()
            .copied()
            .filter(|peer| !excluded.contains(peer))
            .filter(|peer| self.state.get(peer).is_none_or(|s| s.next_attempt <= now))
            .collect();
        if ready.is_empty() {
            return Vec::new();
        }
        let start = self.cursor % ready.len();
        (0..n.min(ready.len())).map(|i| ready[(start + i) % ready.len()]).collect()
    }

    /// Record a round that worked.
    pub fn succeeded(&mut self, peer: SocketAddr) {
        self.state.remove(&peer);
    }

    /// Record a round that did not, and back off.
    ///
    /// Returns whether this failure should be reported at warning level: the
    /// first one, and then no more often than [`WARN_INTERVAL`] for as long as
    /// the peer keeps failing.
    pub fn failed(&mut self, peer: SocketAddr, now: Instant) -> bool {
        let entry = self.state.entry(peer).or_insert(PeerState {
            failures: 0,
            next_attempt: now,
            // So the first failure is always due to be reported.
            last_warned: now - WARN_INTERVAL,
        });
        entry.failures = entry.failures.saturating_add(1);
        entry.next_attempt = now + backoff(self.base, entry.failures);

        let due = now.duration_since(entry.last_warned) >= WARN_INTERVAL;
        if due {
            entry.last_warned = now;
        }
        due
    }

    /// Membership lists `peer` as live again, having not listed it at the tick before: it is ready at once and its next failure
    /// backs off from the base, as a peer never seen fails.
    ///
    /// A backoff is this node's own guess that a peer is gone, made from rounds that failed. Membership is the cluster's
    /// better evidence that it is back, and a peer whose rounds failed through a partition was backing off by up to
    /// [`MAX_BACKOFF`] when the partition healed: it was not dialled for as long as the cap even though SWIM listed it. The
    /// warning clock is kept, so a peer that flaps does not warn on every return. Nothing is recorded for a peer with no
    /// failures.
    pub fn came_up(&mut self, peer: SocketAddr, now: Instant) {
        if let Some(entry) = self.state.get_mut(&peer) {
            entry.failures = 0;
            entry.next_attempt = now;
        }
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

/// The peers membership listed as live at the last tick, so that a tick can tell the ones it lists for the first time.
///
/// Empty when membership is empty or absent: the discovery fallback is not a listing, so a peer discovery has been dialling is
/// new to this the tick membership first names it.
#[derive(Debug, Default)]
pub struct ListedLive {
    listed: BTreeSet<SocketAddr>,
}

impl ListedLive {
    /// Record `live` as this tick's listing and return the addresses in it that the tick before did not list.
    pub fn newly_listed(&mut self, live: &BTreeSet<SocketAddr>) -> Vec<SocketAddr> {
        let new = live.difference(&self.listed).copied().collect();
        self.listed = live.clone();
        new
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
    fn carried_peers_take_fanout_slots_first() {
        // ADR-195: a tick that already gave two of its three
        // fanout slots to peers carried forward from a draining contact must
        // select at most one more, never three fresh on top of the two
        // carried -- the total contacted this tick, carried plus selected,
        // never exceeds fanout.
        let mut h = health();
        let all = peers(9);
        let carried: BTreeSet<SocketAddr> = all.iter().take(2).copied().collect();
        let chosen = h.select_excluding(&all, &carried, Instant::now(), carried.len());
        assert_eq!(chosen.len(), DEFAULT_FANOUT - 2, "only one slot left after two carried");
        assert!(chosen.iter().all(|p| !carried.contains(p)), "never re-selects a carried peer");
    }

    #[test]
    fn carried_peers_at_or_above_fanout_leave_nothing_to_select() {
        let mut h = health();
        let all = peers(9);
        let carried: BTreeSet<SocketAddr> = all.iter().take(DEFAULT_FANOUT).copied().collect();
        let chosen = h.select_excluding(&all, &carried, Instant::now(), carried.len());
        assert!(chosen.is_empty(), "fanout is already spent by what was carried");
    }

    #[test]
    fn select_is_select_excluding_with_nothing_carried() {
        // `select` is a thin wrapper; this pins that it still behaves like
        // the un-refactored version, not just that it compiles.
        let all = peers(9);
        let now = Instant::now();
        let mut a = health();
        let mut b = health();
        assert_eq!(a.select(&all, now), b.select_excluding(&all, &BTreeSet::new(), now, 0));
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
    fn a_peer_membership_reports_up_is_ready_at_once_and_backs_off_from_the_base_again() {
        let mut h = health();
        let now = Instant::now();
        let peer = *peers(1).iter().next().unwrap();
        let other = peers(2).into_iter().next_back().unwrap();

        // A partition: eight rounds failed, the first warned, and the peer is backed off by the cap.
        let mut warned = Vec::new();
        for _ in 0..8 {
            warned.push(h.failed(peer, now));
        }
        assert_eq!(
            warned.iter().filter(|w| **w).count(),
            1,
            "premise: only the first failure warned"
        );
        let healed = now + Duration::from_secs(10);
        assert!(h.select(&peers(1), healed).is_empty(), "premise: still backing off at the heal");
        assert_eq!(h.backing_off(healed), 1);

        h.came_up(peer, healed);
        assert_eq!(h.failures(peer), 0, "membership lists it again: the count starts over");
        assert_eq!(h.select(&peers(1), healed), vec![peer], "ready at once, not at the cap");
        assert_eq!(h.backing_off(healed), 0);

        // It fails again: the backoff starts from the base, not from where the partition left it, and the warning clock
        // is the one it had (a peer that flaps does not warn on every return).
        let warn_again = h.failed(peer, healed);
        assert!(!warn_again, "the warning clock is kept: it warned ten seconds ago");
        assert_eq!(h.failures(peer), 1);
        assert!(h.select(&peers(1), healed + Duration::from_secs(4)).is_empty());
        assert_eq!(
            h.select(&peers(1), healed + Duration::from_secs(5)),
            vec![peer],
            "the base: 5 s"
        );

        // A peer with no failures has nothing recorded for it, and a peer never seen fails as a first failure does.
        h.came_up(other, healed);
        assert_eq!(h.backing_off(healed), 1, "only the peer that failed again is backing off");
        assert!(h.failed(other, healed), "a first failure is reported at warning level");
    }

    #[test]
    fn membership_lists_a_peer_as_new_only_at_the_tick_it_first_appears() {
        let all = peers(3);
        let (a, b, c) = {
            let mut it = all.iter().copied();
            (it.next().unwrap(), it.next().unwrap(), it.next().unwrap())
        };
        let set = |xs: &[SocketAddr]| xs.iter().copied().collect::<BTreeSet<_>>();
        let mut listed = ListedLive::default();
        assert_eq!(
            listed.newly_listed(&set(&[a, b])),
            vec![a, b],
            "the first listing names everyone"
        );
        assert!(
            listed.newly_listed(&set(&[a, b])).is_empty(),
            "a peer listed all along is not new"
        );
        assert_eq!(listed.newly_listed(&set(&[a, b, c])), vec![c], "one that joins is");
        assert!(listed.newly_listed(&set(&[a])).is_empty(), "a peer going away is not new");
        assert_eq!(
            listed.newly_listed(&set(&[a, b])),
            vec![b],
            "and one that comes back after a tick without it is"
        );
        assert!(listed.newly_listed(&BTreeSet::new()).is_empty());
        assert_eq!(
            listed.newly_listed(&set(&[a])),
            vec![a],
            "an empty listing (membership empty, or the discovery fallback) forgets everyone"
        );
    }

    #[test]
    fn no_peers_is_not_an_error() {
        let mut h = health();
        assert!(h.select(&BTreeSet::new(), Instant::now()).is_empty());
    }
}
