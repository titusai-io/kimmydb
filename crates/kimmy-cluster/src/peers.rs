//! The replication loop: find peers, sync with them, repeat.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_storage::Engine;
use tracing::{debug, info, warn};

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
    /// in seconds.
    ///
    /// A callback rather than a metrics handle: the peer's version vector —
    /// the only thing lag can honestly be computed from — exists nowhere but
    /// this loop, and this crate has no business knowing what the caller does
    /// with the number (ADR-043 called this shape out when deferring the
    /// metric). Not called when no peer was reached: an unreachable cluster
    /// has *unknown* lag, and overwriting the last known value with zero
    /// would report the outage as perfect health.
    pub on_lag: Option<std::sync::Arc<dyn Fn(u64) + Send + Sync>>,
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
        }
    }
}

/// Run anti-entropy against known peers, forever.
pub async fn replicate(engine: Arc<Engine>, config: ReplicationConfig) {
    let mut discovered: BTreeSet<SocketAddr> = BTreeSet::new();
    let mut health = PeerHealth::new(config.fanout, config.sync_interval);
    let mut discovery = tokio::time::interval(config.discovery_interval);
    let mut sync = tokio::time::interval(config.sync_interval);

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
                for peer in health.select(&peers, Instant::now()) {
                    // Sequential rather than concurrent: a round is cheap when
                    // converged, and syncing with every peer at once would make
                    // a large cluster stampede one node that fell behind.
                    match sync_once(&engine, peer, &config.secret).await {
                        Ok(outcome) => {
                            health.succeeded(peer);
                            round_lag = Some(round_lag.unwrap_or(0).max(outcome.lag_ms));
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
                            health.failed(peer, now);
                            let failures = health.failures(peer);
                            // Noisy once, then quiet: repeating the same failure
                            // every interval is how a log stops being read.
                            if failures == 1 {
                                warn!(%peer, error = %e, "sync round failed; backing off");
                            } else {
                                debug!(%peer, error = %e, failures, "sync round failed");
                            }
                        }
                    }
                }
                if let (Some(on_lag), Some(lag_ms)) = (&config.on_lag, round_lag) {
                    on_lag(lag_ms / 1_000);
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
