//! What a member says of itself where a restore asks before it joins (D13 containment, ADR-274):
//! the answer in the `Welcome` to a probe and in the `Welcome` of a member that is undecided, and
//! the report a dialler hands an undecided member back over the connection it opened.
//!
//! **Scaffold of the tests commit**: every answer is blank and nothing is kept.

use kimmy_core::NodeId;
use kimmy_storage::Engine;

use crate::membership::Members;
use crate::protocol::{GuardAnswer, Standing};

/// The most listings, and the most members, one answer names.
// Read by the tests alone until the code commit.
#[allow(dead_code)]
pub const MAX_NAMED: usize = 1_024;

impl Members {
    /// This member's membership has begun to gossip: the age an answer gives counts from here.
    /// Later calls do nothing.
    pub fn note_gossiping(&self) {}

    /// This member's answer to a member of `asker`.
    pub fn guard_answer(&self, _engine: &Engine, _asker: NodeId) -> GuardAnswer {
        GuardAnswer {
            token: Vec::new(),
            standing: Standing::Open,
            membership_age_ms: 0,
            founded_unmet: false,
            listings: Vec::new(),
            members: Vec::new(),
        }
    }

    /// The reports diallers have handed this member while it was undecided, the latest of each
    /// dialling process, each with the node id its dialler proved the cluster secret under.
    pub fn guard_reports(&self) -> Vec<(NodeId, GuardAnswer)> {
        Vec::new()
    }
}
