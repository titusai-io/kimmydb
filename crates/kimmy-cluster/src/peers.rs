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
use crate::transport::{DivergenceProbe, sync_once};

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
    /// Collections the cross-member divergence check currently has confirmed
    /// against some peer (ADR-133): held there and not here, or held by
    /// both with disagreeing document counts. A level, like `backing_off` —
    /// but keyed per peer, and for the count half per collection *and* peer,
    /// not per tick: a finding clears only when a later contact with the
    /// specific peer that reported it (and, for a count finding, the same
    /// probe of the same collection against that peer) no longer sees it,
    /// never merely because some tick's union of every peer reached that
    /// tick came up empty. See `DivergenceTracker::observe` for why a
    /// per-tick reading would have left this permanently unable to confirm
    /// past a handful of peers, or past one collection. It moves for exactly
    /// the condition that left every other field in this report at its
    /// healthiest value while the cluster silently lost data.
    pub divergent_collections: usize,
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

    // The cross-member divergence check (ADR-133): which collection this
    // tick probes for a document count, and which findings have recurred
    // often enough to confirm. Owned by the loop, not the engine — like
    // `health` and `stale_peers` below, it is a fact about this process's
    // ticks, not about the data.
    let mut divergence = kimmy_storage::DivergenceTracker::new();

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

                // This tick's turn in the divergence check's rotation
                // (ADR-133): a fresh, cheap read of what this node holds —
                // metadata only — and this node's own count of whichever
                // collection is up next, computed once and reused against
                // every peer this tick rather than once per peer.
                //
                // A failure to read it must not reach `advance_probe` at
                // all, empty set or otherwise: `advance_probe` sweeps its
                // count-side state against whatever it is handed, on the
                // reasoning that a collection absent from that set is
                // *provably* gone (ADR-133, defect 6) — true of a fresh
                // read, and false of a transient error standing in for one.
                // Passing an empty set on error used to be harmless, before
                // that sweep existed; now it would read a storage hiccup as
                // "this node holds nothing" and silently discard every live
                // count finding, the exact silence this check exists to
                // rule out. `advance_probe_on` is where that decision lives,
                // pulled out on its own so it is a function with a test
                // rather than a branch inside this loop.
                let mine_collections = engine.all_collection_ids();
                if let Err(e) = &mine_collections {
                    warn!(error = %e, "divergence check: could not list this node's own \
                          collections; skipping this tick's probe rotation");
                }
                let probe = advance_probe_on(&mut divergence, mine_collections).map(|id| {
                    let mine_count = match engine.count_by_id(id) {
                        Ok(count) => count,
                        Err(e) => {
                            warn!(error = %e, collection = %id, "divergence check: could not \
                                  count the probed collection; comparing existence only this tick");
                            None
                        }
                    };
                    DivergenceProbe { id, mine_count }
                });
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
                    match sync_once(&engine, peer, &config.secret, probe)
                        .instrument(span.clone())
                        .await
                    {
                        Ok(mut outcome) => {
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
                                // Folded in only when the check actually ran
                                // against this peer this contact
                                // (`exhausted`, see `sync_once`) — a peer
                                // whose round had a backlog too deep to
                                // reach its tail must not be read as having
                                // reconciled, and `observe` treats "not
                                // called" and "called with nothing found" as
                                // the two different facts they are
                                // (ADR-133). `divergent` and `count_probe`
                                // are always set together by `sync_once`, so
                                // `divergent`'s presence alone gates both.
                                if let Some(existence) = outcome.divergent.take() {
                                    let findings = kimmy_storage::DivergenceFindings {
                                        existence,
                                        count: outcome.count_probe.take(),
                                    };
                                    divergence.observe(node, findings);
                                }
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
                // Read every tick regardless of whether anything reports it,
                // for the same reason the line above computes `report`
                // unconditionally: the tracker's own state does not depend
                // on whether a caller wired up `on_round`, only `observe`
                // above does, and that already ran per peer contacted.
                report.divergent_collections = divergence.confirmed_count();
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

/// The collection id this tick probes for a count, given the result of
/// reading this node's own collection set — or `None`, without touching
/// `tracker` at all, when that read failed.
///
/// A read failure is *not* an empty set standing in for one:
/// `DivergenceTracker::advance_probe` sweeps its count-side confirmation
/// state against whatever it is handed, on the premise that a collection
/// absent from that set is provably gone this node no longer holds it
/// (ADR-133, defect 6). That premise holds for a genuine read and fails for
/// a read that merely errored — a transient storage hiccup is not evidence
/// this node suddenly holds zero collections, and reading it that way would
/// silently discard every live count finding for as long as the error
/// lasts. So `advance_probe` is not called at all on that path: the
/// rotation's cursor stays exactly where it was, and every finding survives
/// untouched into the next tick, which may read cleanly.
fn advance_probe_on<E>(
    tracker: &mut kimmy_storage::DivergenceTracker,
    mine: Result<BTreeSet<kimmy_core::CollectionId>, E>,
) -> Option<kimmy_core::CollectionId> {
    match mine {
        Ok(ids) => tracker.advance_probe(&ids),
        Err(_) => None,
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

    /// The fanout scaling defect, composed the way the real loop composes
    /// it: `PeerHealth::select`'s own rotation feeding `DivergenceTracker`,
    /// not a hand-written stand-in for either. `DivergenceTracker`'s unit
    /// tests already pin that it confirms across non-adjacent contacts in
    /// isolation; this is the piece that pins the *other* half actually
    /// produces contacts shaped that way at a cluster size the default
    /// fanout cannot cover in one pass, so a future change to either
    /// `select`'s rotation policy or the tracker's keying cannot silently
    /// re-break the composition while each component's own tests stay
    /// green.
    #[test]
    fn a_peer_confirms_despite_a_fanout_smaller_than_the_cluster() {
        use std::collections::HashMap;

        let peers: BTreeSet<SocketAddr> =
            (0..8).map(|i| format!("127.0.0.1:{}", 7900 + i).parse().unwrap()).collect();
        // Stands in for the handshake's introduction in the real protocol,
        // which is where a `SocketAddr` and a `NodeId` are actually paired.
        let ids: HashMap<SocketAddr, NodeId> = peers
            .iter()
            .enumerate()
            .map(|(i, &addr)| (addr, NodeId::from_bytes((i as u128).to_be_bytes())))
            .collect();
        // Past the sixth of eight peers at the default fanout of 3 -- two
        // ticks in a row cannot possibly both reach it by round-robin alone.
        let divergent_peer = *peers.iter().nth(5).unwrap();
        let divergent_collection = kimmy_core::CollectionId(1);

        let mut health = PeerHealth::new(DEFAULT_FANOUT, Duration::from_secs(5));
        let mut tracker = kimmy_storage::DivergenceTracker::new();
        let now = Instant::now();

        for _ in 0..40 {
            for addr in health.select(&peers, now) {
                health.succeeded(addr);
                let existence = if addr == divergent_peer {
                    BTreeSet::from([divergent_collection])
                } else {
                    BTreeSet::new()
                };
                let findings = kimmy_storage::DivergenceFindings { existence, count: None };
                tracker.observe(ids[&addr], findings);
            }
            if tracker.confirmed_count() > 0 {
                break;
            }
        }
        assert_eq!(tracker.confirmed_count(), 1, "confirms at 8 peers despite fanout 3");
    }

    /// Defect 6's sweep introduced a regression of its own: a transient
    /// failure to read this node's own collections must not be handed to
    /// `advance_probe` as an empty set, or it silently discards every live
    /// count finding on the strength of an error rather than a fact.
    #[test]
    fn a_read_failure_leaves_confirmed_count_findings_untouched() {
        let mut tracker = kimmy_storage::DivergenceTracker::new();
        let p = NodeId::from_bytes(1u128.to_be_bytes());
        let mismatched = kimmy_storage::DivergenceFindings {
            existence: BTreeSet::new(),
            count: Some((kimmy_core::CollectionId(7), true)),
        };
        tracker.observe(p, mismatched.clone());
        tracker.observe(p, mismatched);
        assert_eq!(tracker.confirmed_count(), 1, "confirmed before the read failure");

        // Stands in for `engine.all_collection_ids()` failing this tick —
        // not for it succeeding with nothing in it.
        let failed: Result<BTreeSet<kimmy_core::CollectionId>, &str> = Err("transient");
        let probe = advance_probe_on(&mut tracker, failed);

        assert_eq!(probe, None, "nothing to probe when the read itself failed");
        assert_eq!(
            tracker.confirmed_count(),
            1,
            "a read failure must not be read as \"this node holds nothing\""
        );
    }
}
