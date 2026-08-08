//! The replication loop: find peers, sync with them, repeat.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kimmy_storage::Engine;
use tracing::{debug, info, warn};

use crate::discovery::SeedSource;
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
}

/// Run anti-entropy against discovered peers, forever.
pub async fn replicate(engine: Arc<Engine>, config: ReplicationConfig) {
    let mut peers: BTreeSet<SocketAddr> = BTreeSet::new();
    let mut discovery = tokio::time::interval(config.discovery_interval);
    let mut sync = tokio::time::interval(config.sync_interval);

    loop {
        tokio::select! {
            _ = discovery.tick() => {
                peers = resolve(&config.seeds, config.local).await;
                debug!(count = peers.len(), "resolved peers");
            }
            _ = sync.tick() => {
                for peer in &peers {
                    // Sequential rather than concurrent: a round is cheap when
                    // converged, and syncing with every peer at once would make
                    // a large cluster stampede one node that fell behind.
                    match sync_once(&engine, *peer, &config.secret).await {
                        Ok(outcome) if outcome.total() > 0 => info!(
                            %peer,
                            applied = outcome.applied,
                            ddl = outcome.ddl,
                            "merged from peer"
                        ),
                        Ok(_) => {}
                        // A peer being unreachable is the normal state of a
                        // cluster, not an error worth stopping for.
                        Err(e) => debug!(%peer, error = %e, "sync round failed"),
                    }
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
