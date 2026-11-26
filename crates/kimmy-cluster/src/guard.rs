//! The mistaken-restore guard (D13 containment, ADR-274): before a fresh epoch's host dials anyone
//! or completes a handshake, it asks whether its own node id is live at another address, and halts
//! itself if a host answers.
//!
//! **This file is the scaffold of the tests commit**: the function compiles and does nothing, so
//! that the tests in `guard_tests.rs` are red on their designed assertions. The code commit
//! replaces it.

use std::net::SocketAddr;
use std::time::Duration;

use kimmy_core::NodeId;

/// What the guard needs: who this host is, where it listens, the cluster secret, and the facts the
/// decision reads (`containment::guard_step`). `window` and `seed_timeout` override the defaults
/// (twice the suspicion-to-down time; `containment::SEED_TIMEOUT`) so that a test does not wait ten
/// seconds.
#[derive(Clone, Debug)]
pub struct GuardConfig {
    pub own_node: NodeId,
    /// This host's own addresses: a list entry at one of these is this host, not another.
    pub own_addresses: Vec<SocketAddr>,
    pub secret: String,
    /// The seeds name another member.
    pub seed_configured: bool,
    /// One of them answered a dial.
    pub a_seed_answered: bool,
    /// foca's `suspect_to_down_after`, from which the window is derived.
    pub suspect_to_down: Duration,
    pub seed_timeout: Duration,
    pub window: Option<Duration>,
    /// `--replace` was given for this epoch's restore and is honoured: a live host that answers
    /// does not halt this one (the ordinary containment then tells that host).
    pub replace: bool,
}

impl GuardConfig {
    pub fn new(own_node: NodeId, own_addresses: Vec<SocketAddr>, secret: &str) -> Self {
        Self {
            own_node,
            own_addresses,
            secret: secret.to_string(),
            seed_configured: true,
            a_seed_answered: true,
            suspect_to_down: Duration::from_millis(4_800),
            seed_timeout: crate::containment::SEED_TIMEOUT,
            window: None,
            replace: false,
        }
    }
}

/// What the guard decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GuardVerdict {
    /// Nothing answered: the host proceeds.
    Pass,
    /// A host of this node answered a probe at this address: the fresh host halts itself.
    LiveHost(SocketAddr),
}

/// Run the guard. `listed` is SWIM's member list as `(address, node)` pairs, or `None` until the
/// first one has arrived.
///
/// The three cases (ruled 2026-11-25): with no seed, or none that answered, it passes at once; with
/// a seed that answered and no member list yet it waits for the first list, at most `seed_timeout`,
/// and then passes; with a list that names this node at an address other than its own it probes
/// each such address at once and waits at most the window for an answer. A probe that is answered
/// halts the fresh host (unless `--replace`); none, or one that does not verify, passes.
pub async fn run_guard(
    config: &GuardConfig,
    listed: impl Fn() -> Option<Vec<(SocketAddr, NodeId)>>,
) -> GuardVerdict {
    let started = std::time::Instant::now();
    loop {
        let list = listed();
        let elsewhere: Vec<SocketAddr> = list
            .iter()
            .flatten()
            .filter(|(addr, node)| *node == config.own_node && !config.own_addresses.contains(addr))
            .map(|(addr, _)| *addr)
            .collect();
        let step = crate::containment::guard_step(
            config.seed_configured,
            config.a_seed_answered,
            list.is_some(),
            !elsewhere.is_empty(),
            config.suspect_to_down,
        );
        match step {
            crate::containment::GuardStep::Pass => return GuardVerdict::Pass,
            crate::containment::GuardStep::AwaitMemberList(_) => {
                if started.elapsed() >= config.seed_timeout {
                    return GuardVerdict::Pass;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            crate::containment::GuardStep::Probe(window) => {
                let window = config.window.unwrap_or(window);
                return match probe_all(config, &elsewhere, window).await {
                    Some(addr) if !config.replace => GuardVerdict::LiveHost(addr),
                    Some(addr) => {
                        tracing::warn!(
                            %addr,
                            "a live host of this node answered the guard's probe; `--replace` was \
                             given for this epoch, so this host proceeds and the ordinary \
                             containment tells that host"
                        );
                        GuardVerdict::Pass
                    }
                    None => GuardVerdict::Pass,
                };
            }
        }
    }
}

/// Probe every address at once and return the first that answers with a verifying `Welcome` within
/// `window`.
async fn probe_all(
    config: &GuardConfig,
    addresses: &[SocketAddr],
    window: Duration,
) -> Option<SocketAddr> {
    let mut probes = tokio::task::JoinSet::new();
    for addr in addresses.iter().copied() {
        let (own, secret) = (config.own_node, config.secret.clone());
        // UNSUPERVISED: a probe's panic ends that probe only; the join reads it as no answer.
        probes.spawn(async move {
            crate::transport::probe_peer(own, addr, &secret).await.then_some(addr)
        });
    }
    let waiting = async {
        while let Some(done) = probes.join_next().await {
            if let Ok(Some(addr)) = done {
                return Some(addr);
            }
        }
        None
    };
    tokio::time::timeout(window, waiting).await.ok().flatten()
}

/// Whether any of `seeds` (`host:port` names, resolved here) answers a probe within a second and a
/// half: the guard's "a seed answered" fact. With none answering the host proceeds at once, so the
/// first node of a cluster that is starting, whose seeds are down, does not wait for a member list
/// that cannot come.
pub async fn a_seed_answers(config: &GuardConfig, seeds: &[String]) -> bool {
    const WITHIN: Duration = Duration::from_millis(1_500);
    let mut probes = tokio::task::JoinSet::new();
    for seed in seeds {
        let Ok(resolved) = tokio::net::lookup_host(seed.as_str()).await else { continue };
        for addr in resolved.filter(|addr| !config.own_addresses.contains(addr)) {
            let (own, secret) = (config.own_node, config.secret.clone());
            // UNSUPERVISED: a probe's panic ends that probe only; the join reads it as no answer.
            probes.spawn(async move {
                tokio::time::timeout(WITHIN, crate::transport::probe_peer(own, addr, &secret))
                    .await
                    .unwrap_or(false)
            });
        }
    }
    while let Some(done) = probes.join_next().await {
        if done.unwrap_or(false) {
            return true;
        }
    }
    false
}
