//! The replication loop: find peers, sync with them, repeat.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_core::NodeId;
use kimmy_storage::Engine;
use tracing::{Instrument, debug, info, warn};

use crate::discovery::SeedSource;
use crate::health::{DEFAULT_FANOUT, PeerHealth};
use crate::membership::Members;
use crate::transport::sync_once;

/// How often to run a round against every known peer.
pub const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);

/// How often to re-resolve the seed sources.
///
/// Separate from the sync interval and deliberately slower: DNS is the
/// expensive half, and a Kubernetes Service does not change pod set every few
/// seconds. But it must happen *repeatedly* — a node that resolved only at
/// startup would never see a peer that joined after it.
pub const DEFAULT_DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);

/// What the loop reports about a peer's staleness after each successful round:
/// the peer, and how far it trails this node when that exceeds tombstone
/// retention (`None` when within the window).
pub type PeerStalenessHook = Arc<dyn Fn(NodeId, Option<u64>) + Send + Sync>;

/// What one sync tick did, for the caller to count (ADR-123).
///
/// Everything here is a fact only the loop can see: which rounds failed,
/// which peers it is leaving alone, and what the rounds that succeeded had
/// to skip. Each is a number that says "something is wrong" when
/// `kimmy_replication_lag_seconds` says nothing — a failed round reports no
/// lag at all, by design (ADR-122), so a cluster wedged on a round that
/// fails every time read 0 lag and every member live for as long as it was
/// wedged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RoundReport {
    /// Rounds in this tick that failed, whatever the cause: a peer that could
    /// not be reached, a handshake that was refused, a batch that could not
    /// be applied. One per peer attempted, so at most the fanout. A caller
    /// adds this to a counter; a counter that keeps rising while the lag
    /// gauge sits at 0 is the shape of the silent wedge.
    pub failed: usize,
    /// Peers this node is currently backing off from after failures, as of
    /// the end of the tick: 0 when every known peer answered its last
    /// round, or when there are no peers. A level, for a gauge.
    pub backing_off: usize,
    /// Replicated schema changes the rounds in this tick could not apply to
    /// this node's state and skipped — `SyncOutcome::ddl_refused`, summed
    /// over the peers reached. Each one is an index this node now lacks and
    /// its peers hold, and nothing will retry it; the `warn!` at the time
    /// names it.
    pub ddl_refused: usize,
}

/// What the loop reports after every sync tick. See [`RoundReport`].
pub type RoundHook = Arc<dyn Fn(RoundReport) + Send + Sync>;

pub struct ReplicationConfig {
    pub seeds: Vec<SeedSource>,
    pub secret: String,
    /// This node's own listener, so it does not sync with itself.
    pub local: SocketAddr,
    pub sync_interval: Duration,
    pub discovery_interval: Duration,
    /// Peers contacted per round.
    ///
    /// A cap rather than a quota: a cluster smaller than this contacts
    /// everyone. Keeping it constant is what makes the per-round cost
    /// independent of cluster size.
    pub fanout: usize,
    /// Where to send discovered addresses so membership can announce to them.
    ///
    /// Membership finds the rest of the cluster by gossip once it has *one*
    /// contact, but a node that starts alone has none — so discovery keeps
    /// feeding it, not only at startup.
    pub announce: Option<tokio::sync::mpsc::Sender<SocketAddr>>,
    /// Live members according to SWIM, when membership is running.
    ///
    /// Preferred over discovery once it knows anyone: discovery reports who was
    /// *configured*, membership reports who is *up*, and only the second can
    /// tell a node that was removed from a Service from one that never
    /// answered. Discovery remains the bootstrap and the fallback.
    pub members: Option<Members>,
    /// Called after each sync round with the round's worst replication lag,
    /// in seconds: how far behind in time this node is against the peers it
    /// reached (ADR-122).
    ///
    /// A callback rather than a metrics handle: the peer's version vector —
    /// the only thing lag can honestly be computed from — exists nowhere but
    /// this loop, and this crate has no business knowing what the caller does
    /// with the number (ADR-043 called this shape out when deferring the
    /// metric). Not called when no peer was reached: an unreachable cluster
    /// has *unknown* lag, and overwriting the last known value with zero
    /// would report the outage as perfect health.
    pub on_lag: Option<std::sync::Arc<dyn Fn(u64) + Send + Sync>>,
    /// This node's `storage.tombstone_retention_secs`: the window past which a
    /// peer that has not caught up may resurrect a delete (ADR-085).
    pub tombstone_retention: Duration,
    /// Called after every successful round with the peer's id and, when it
    /// trails this node by more than tombstone retention, by how much;
    /// `None` when it is within the window. Same shape as `on_lag`, for the
    /// same reason: the peer's vector exists nowhere but this loop.
    pub on_peer_staleness: Option<PeerStalenessHook>,
    /// Called once after every sync tick, reached peers or not, with what
    /// the tick did that `on_lag` cannot say: rounds that failed, peers
    /// being backed off, schema changes refused (ADR-123). Same shape as
    /// `on_lag`, for the same reason — this crate knows nothing about
    /// metrics — and unlike `on_lag` it *is* called when no peer was
    /// reached, because "every round failed" is precisely the report.
    pub on_round: Option<RoundHook>,
}

impl ReplicationConfig {
    pub fn new(seeds: Vec<SeedSource>, secret: String, local: SocketAddr) -> Self {
        Self {
            seeds,
            secret,
            local,
            sync_interval: DEFAULT_SYNC_INTERVAL,
            discovery_interval: DEFAULT_DISCOVERY_INTERVAL,
            fanout: DEFAULT_FANOUT,
            announce: None,
            members: None,
            on_lag: None,
            // The storage default; a caller with a configured window passes
            // its own, and zero disables the check.
            tombstone_retention: Duration::from_secs(24 * 60 * 60),
            on_peer_staleness: None,
            on_round: None,
        }
    }
}

/// Run anti-entropy against known peers, forever.
pub async fn replicate(engine: Arc<Engine>, config: ReplicationConfig) {
    let mut discovered: BTreeSet<SocketAddr> = BTreeSet::new();
    let mut health = PeerHealth::new(config.fanout, config.sync_interval);
    let mut discovery = tokio::time::interval(config.discovery_interval);
    let mut sync = tokio::time::interval(config.sync_interval);

    // Peers currently flagged as stale rejoiners, so the warning fires on the
    // transition and not on every round they stay that way.
    let mut stale_peers: BTreeSet<NodeId> = BTreeSet::new();
    let retention_ms = config.tombstone_retention.as_millis() as u64;

    loop {
        tokio::select! {
            _ = discovery.tick() => {
                discovered = resolve(&config.seeds, config.local).await;
                debug!(count = discovered.len(), "resolved peers");

                // Offer every resolved address to membership. Announcing to one
                // we already know is harmless — foca ignores it — and doing it
                // every interval is what lets a node that started alone join
                // the cluster whenever it appears.
                if let Some(announce) = &config.announce {
                    for peer in &discovered {
                        let _ = announce.try_send(*peer);
                    }
                }
            }
            _ = sync.tick() => {
                // A subset, not everyone: anti-entropy is transitive, so a
                // write reaches the cluster through intermediate peers without
                // every node contacting every other one every interval.
                // Membership when it knows anyone, discovery otherwise. A node
                // that has just started has resolved seeds but not yet gossiped
                // with them, so discovery is what gets the first round out.
                let peers = match &config.members {
                    Some(members) if !members.is_empty() => {
                        let mut live = members.snapshot();
                        live.remove(&config.local);
                        live
                    }
                    _ => discovered.clone(),
                };

                // The round's worst lag across the peers actually reached.
                // `None` when nothing answered, and then nothing is reported:
                // an unreachable cluster has unknown lag, not zero lag.
                let mut round_lag: Option<u64> = None;
                let mut report = RoundReport::default();
                for peer in health.select(&peers, Instant::now()) {
                    // One span per peer per round, not one per round: an
                    // anti-entropy round against three peers is three
                    // conversations with three different outcomes, and folding
                    // them into one span would lose which peer was the slow
                    // one — the only thing anybody opens this trace to find
                    // out. `applied`, `ddl` and `lag_ms` are declared here and
                    // filled from the outcome, so a failed round still leaves a
                    // span with the peer on it rather than nothing at all.
                    let span = tracing::info_span!(
                        "cluster.sync",
                        otel.kind = "client",
                        peer = %peer,
                        applied = tracing::field::Empty,
                        ddl = tracing::field::Empty,
                        lag_ms = tracing::field::Empty,
                    );
                    // Sequential rather than concurrent: a round is cheap when
                    // converged, and syncing with every peer at once would make
                    // a large cluster stampede one node that fell behind.
                    match sync_once(&engine, peer, &config.secret).instrument(span.clone()).await {
                        Ok(outcome) => {
                            // `i64` throughout: `tracing-opentelemetry` has
                            // no `record_u64`, so an unsigned value is
                            // formatted with `Debug` and reaches a collector
                            // as a string nothing can graph.
                            span.record("applied", outcome.applied as i64);
                            span.record("ddl", outcome.ddl as i64);
                            span.record("lag_ms", outcome.lag_ms as i64);
                            health.succeeded(peer);
                            round_lag = Some(round_lag.unwrap_or(0).max(outcome.lag_ms));
                            report.ddl_refused += outcome.ddl_refused;
                            if let Some(node) = outcome.peer {
                                let stale = retention_ms > 0 && outcome.behind_ms > retention_ms;
                                let was = stale_peers.contains(&node);
                                if stale && !was {
                                    stale_peers.insert(node);
                                    warn!(
                                        %peer,
                                        node = %node,
                                        behind_secs = outcome.behind_ms / 1_000,
                                        retention_secs = retention_ms / 1_000,
                                        "peer trails this node by more than tombstone \
                                         retention; deletes it missed may already be \
                                         collected here, so merging it can resurrect them \
                                         — a stale rejoiner should be reset, not merged"
                                    );
                                } else if !stale && was {
                                    stale_peers.remove(&node);
                                    info!(%peer, node = %node, "peer is back within tombstone retention");
                                }
                                if let Some(report) = &config.on_peer_staleness {
                                    report(node, stale.then_some(outcome.behind_ms));
                                }
                            }
                            if outcome.total() > 0 {
                                info!(
                                    %peer,
                                    applied = outcome.applied,
                                    ddl = outcome.ddl,
                                    "merged from peer"
                                );
                            }
                        }
                        // A peer being unreachable is the normal state of a
                        // cluster, not an error worth stopping for — but it is
                        // worth backing off, so a node that is not coming back
                        // stops costing a connection every interval.
                        Err(e) => {
                            let now = Instant::now();
                            // Reported on the first failure and then at a
                            // bounded cadence, not once and never again: a peer
                            // that never recovers has to stay visible, or a
                            // half-converged cluster looks like a healthy one.
                            let due = health.failed(peer, now);
                            let failures = health.failures(peer);
                            report.failed += 1;
                            if due {
                                warn!(%peer, error = %e, failures, "sync round failed; backing off");
                            } else {
                                debug!(%peer, error = %e, failures, "sync round failed");
                            }
                        }
                    }
                }
                if let (Some(on_lag), Some(lag_ms)) = (&config.on_lag, round_lag) {
                    on_lag(lag_ms / 1_000);
                }
                // Reported whether or not anything was reached: the tick in
                // which every round failed is the one an operator most needs
                // to hear about, and it is the one `on_lag` says nothing for.
                if let Some(on_round) = &config.on_round {
                    report.backing_off = health.backing_off(Instant::now());
                    on_round(report);
                }
            }
        }
    }
}

/// Resolve every seed source, dropping this node's own address.
async fn resolve(seeds: &[SeedSource], local: SocketAddr) -> BTreeSet<SocketAddr> {
    let mut out = BTreeSet::new();
    for seed in seeds {
        match seed.resolve().await {
            Ok(addrs) => out.extend(addrs),
            // A name that does not resolve yet is what a cluster looks like
            // while it is starting. Logged, not fatal.
            Err(e) => warn!(seed = %seed.describe(), error = %e, "could not resolve seed"),
        }
    }

    // A headless Service resolves to *every* pod including this one, and a node
    // syncing with itself would do work to learn nothing.
    out.remove(&local);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolution_drops_this_node() {
        // Kubernetes headless DNS returns every pod, including the one asking.
        let local: SocketAddr = "127.0.0.1:7900".parse().unwrap();
        let other: SocketAddr = "127.0.0.1:7901".parse().unwrap();
        let seeds = vec![SeedSource::Static(vec![local, other])];

        let peers = resolve(&seeds, local).await;

        assert_eq!(peers, BTreeSet::from([other]), "a node must not sync with itself");
    }

    #[tokio::test]
    async fn an_unresolvable_seed_does_not_lose_the_others() {
        // One bad DNS name must not blind a node to every peer it can reach.
        let good: SocketAddr = "127.0.0.1:7901".parse().unwrap();
        let seeds = vec![
            SeedSource::Dns { name: "no-such-host.invalid".into(), port: 7900 },
            SeedSource::Static(vec![good]),
        ];

        let peers = resolve(&seeds, "127.0.0.1:7900".parse().unwrap()).await;

        assert!(peers.contains(&good));
    }

    #[tokio::test]
    async fn duplicate_addresses_are_collapsed() {
        // Overlapping seed sources are normal; syncing twice per round is not.
        let a: SocketAddr = "127.0.0.1:7901".parse().unwrap();
        let seeds = vec![SeedSource::Static(vec![a]), SeedSource::Static(vec![a])];

        let peers = resolve(&seeds, "127.0.0.1:7900".parse().unwrap()).await;

        assert_eq!(peers.len(), 1);
    }
}
