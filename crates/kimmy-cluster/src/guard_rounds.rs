//! The question a restore asks before it joins its cluster, asked in rounds, and the verdict on
//! each round (D13 containment, ADR-274).
//!
//! **Scaffold of the tests commit**: nobody is asked, and every question passes at once.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use kimmy_core::NodeId;

use crate::membership::Members;

/// How long an address that nobody names for the asking node has to answer in a round.
pub const UNNAMED_WITHIN: Duration = Duration::from_millis(1_500);

/// The pause between one round that decided nothing and the next.
pub const BETWEEN_ROUNDS: Duration = Duration::from_secs(1);

/// What asks, and how long it waits.
#[derive(Clone, Debug)]
pub struct Asking {
    /// The node id of the restore that asks.
    pub own_node: NodeId,
    pub secret: String,
    /// How old a member's membership must be for its answer to count, and how long an address
    /// named for the asking node has to answer: twice foca's suspicion-to-down time.
    pub window: Duration,
    pub unnamed_within: Duration,
    pub between_rounds: Duration,
    /// `--replace` was given for this epoch's restore and is honoured.
    pub replace: bool,
}

impl Asking {
    pub fn new(own_node: NodeId, secret: &str, window: Duration, replace: bool) -> Self {
        Self {
            own_node,
            secret: secret.to_string(),
            window,
            unnamed_within: UNNAMED_WITHIN,
            between_rounds: BETWEEN_ROUNDS,
            replace,
        }
    }
}

/// What the rounds came to. A round that comes to neither is followed by another: no passage of
/// time decides.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// A running member answered and no running host of the asking node did, or `--replace` was
    /// given. With every address the deciding round was named, for the member to be offered.
    Pass { named: Vec<SocketAddr> },
    /// A running host of the asking node answered, from this address.
    LiveHost(SocketAddr),
}

/// Ask, round after round, until one decides. `seeds` is resolved again for each round;
/// `members` holds the asker's token and the reports that diallers hand it.
pub async fn ask_until_decided<F, Fut>(_asking: &Asking, _members: &Members, _seeds: F) -> Decision
where
    F: Fn() -> Fut,
    Fut: Future<Output = Vec<SocketAddr>>,
{
    Decision::Pass { named: Vec::new() }
}
