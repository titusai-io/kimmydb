//! Fail closed when this member's own stamps will be refused (U1, the second half).
//!
//! The bound ([`crate::clock_bound`]) keeps a far-future stamp out of every member's clock. It cannot make this
//! member's own stamps acceptable: a member whose logical clock leads its physical clock by more than the bound, or
//! whose stamps a peer refuses because the peer's clock is behind, writes entries that the cluster holds back until
//! they are due. For an ordinary write that is the existing skew contract. For a schema change (and, later, a
//! replacing `$out`) it is a success the client is told about and the cluster has not made, so those fail closed
//! instead, with a retryable error that names the cause.
//!
//! Two local signals decide it, and neither needs the cluster to be reachable:
//!
//! * **b1.** This member's logical clock is past its own physical clock plus the bound, so its next stamp is.
//! * **b2.** A live peer's answer to a push of this member's entries said it stopped at an entry of this origin because
//!   its clock is past the bound ([`Engine::note_own_stamps_refused`], fed by the pusher), and has not yet said that a
//!   later push went through, or come due.
//!
//! Ordinary writes are not refused, and neither is anything the node creates for itself (a name that starts with `__`:
//! the system collections and their indexes, which a member with a leading clock must still make at start).

use std::fmt;

use kimmy_core::NodeId;

use crate::Engine;
use crate::error::{Result, StorageError};

/// Why this member's own stamps would be refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnStampRefusal {
    /// b1: the logical clock leads the physical clock by `lead_ms`, more than the bound `bound_ms`.
    ClockAhead { lead_ms: u64, bound_ms: u64 },
    /// b2: `peer` refuses this member's entries for their clock, for about `due_in_ms` more (on the peer's clock).
    PeerRefused { peer: NodeId, due_in_ms: u64 },
}

impl fmt::Display for OwnStampRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClockAhead { lead_ms, bound_ms } => write!(
                f,
                "this member's clock is ahead of its own clock by {} s, more than the {} s the cluster allows, \
                 so every other member would hold back this change until it is due; retry on another member, \
                 and fix this member's clock",
                lead_ms.div_ceil(1000),
                bound_ms / 1000
            ),
            Self::PeerRefused { peer, due_in_ms } => write!(
                f,
                "member {peer} is refusing this member's changes because their clock is ahead of its own, for \
                 about {} s more, so it would not take this change; retry on another member, and check the \
                 clocks",
                due_in_ms.div_ceil(1000)
            ),
        }
    }
}

/// What a peer's refusal of this origin's entries recorded: the physical time, on this member's clock, at which the
/// peer says they are due.
pub(crate) type PeerRefusals = std::collections::BTreeMap<NodeId, u64>;

impl Engine {
    /// How far this member's logical clock leads its physical clock, in milliseconds (0 when it does not). The gauge
    /// `kimmy_clock_hlc_lead_seconds` and signal b1.
    pub fn clock_lead_ms(&self) -> u64 {
        self.logical_clock_wall_ms().saturating_sub(self.clock_bound_now_ms())
    }

    /// Record that `peer`'s answer to a push said it stopped at an entry of this origin because its clock is past the
    /// bound, and that it is due in `due_in_ms` on the peer's clock. Signal b2. The pusher calls it.
    pub fn note_own_stamps_refused(&self, peer: NodeId, due_in_ms: u64) {
        let due_at = self.clock_bound_now_ms().saturating_add(due_in_ms);
        self.own_refusals.lock().insert(peer, due_at);
    }

    /// Record that a push to `peer` was answered without a clock refusal: its refusal of this origin is over.
    pub fn note_own_stamps_not_refused(&self, peer: NodeId) {
        self.own_refusals.lock().remove(&peer);
    }

    /// The longest time, in milliseconds on this member's clock, that a peer is still known to refuse this origin's
    /// entries (0 when none does): what has not come due and has not been cleared by a later push. The gauge, and
    /// signal b2. A refusal whose time has passed is dropped here.
    pub fn own_stamps_refused_for_ms(&self) -> u64 {
        self.peer_refusing().map_or(0, |(_, remaining)| remaining)
    }

    /// The peer with the longest refusal still standing, and how long (b2).
    fn peer_refusing(&self) -> Option<(NodeId, u64)> {
        let now = self.clock_bound_now_ms();
        let mut standing = self.own_refusals.lock();
        standing.retain(|_, due_at| *due_at > now);
        standing
            .iter()
            .map(|(peer, due_at)| (*peer, due_at.saturating_sub(now)))
            .max_by_key(|(_, remaining)| *remaining)
    }

    /// Why this member's own stamps would be refused now, if they would be (b1, then b2).
    pub fn own_stamps_refusal(&self) -> Option<OwnStampRefusal> {
        let bound_ms = self.clock_bound().skew_ms;
        let lead_ms = self.clock_lead_ms();
        if lead_ms > bound_ms {
            return Some(OwnStampRefusal::ClockAhead { lead_ms, bound_ms });
        }
        self.peer_refusing()
            .map(|(peer, due_in_ms)| OwnStampRefusal::PeerRefused { peer, due_in_ms })
    }

    /// The guard of every schema change a user can make: an error, with nothing written, when this member's own stamps
    /// would be refused. Called first by each of them. A database or collection whose name starts with `__` is the
    /// node's own (system collections, their indexes) and is not guarded.
    pub fn refuse_ddl_if_own_stamps_would_be_refused(
        &self,
        db: &str,
        collection: Option<&str>,
    ) -> Result<()> {
        if db.starts_with("__") || collection.is_some_and(|name| name.starts_with("__")) {
            return Ok(());
        }
        match self.own_stamps_refusal() {
            Some(refusal) => Err(StorageError::OwnStampsRefused(refusal)),
            None => Ok(()),
        }
    }
}
