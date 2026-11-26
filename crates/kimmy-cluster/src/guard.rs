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
/// first one has arrived. **Scaffold: always passes.**
pub async fn run_guard(
    _config: &GuardConfig,
    _listed: impl Fn() -> Option<Vec<(SocketAddr, NodeId)>>,
) -> GuardVerdict {
    GuardVerdict::Pass
}
