//! D13 containment (ADR-274), the cluster half: when a peer's claim of an epoch is believed and
//! bounded, when a halt marker applies to a store, and what the mistaken-restore guard waits for.
//!
//! **This file is the scaffold of the tests commit and nothing else**: each function compiles and
//! does nothing, so that every test in `containment_tests.rs` is red on its designed assertion and
//! none on a missing symbol. The code commits replace each body, and none of these stubs survives
//! to the last commit (a source scan asserts it).

use std::time::Duration;

use kimmy_core::{Epoch, NodeId};

/// How long the guard waits for a seed's first member list before it passes (ruled 2026-11-25;
/// above the window, so a slow first list is not read as silence, and below the catch-up wait). A
/// `cfg(test)` override exists in the code commits.
pub const SEED_TIMEOUT: Duration = Duration::from_secs(15);

/// The clock a claimed epoch's start is held against: `max(the physical clock, the HLC's last wall
/// time)`, or a member whose wall clock is behind its own logical clock refuses every legitimate
/// new epoch (gap 4).
pub fn bound_clock_ms(physical_ms: u64, _hlc_last_wall_ms: u64) -> u64 {
    physical_ms
}

/// Whether a claimed epoch is within the bound at `clock_ms`: its start's wall time is at most
/// `clock_ms + skew_ms`, inclusive. `Epoch::MAX`, whose start bytes are `0xFF`, is beyond any
/// bound.
pub fn claim_within_bound(_clock_ms: u64, _skew_ms: u64, _epoch: &Epoch) -> bool {
    true
}

/// What the guard does next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GuardStep {
    /// Proceed: nothing lists this node id elsewhere, or there is nobody to ask.
    Pass,
    /// A seed answered and no member list has arrived: wait for the first one, at most this long,
    /// then pass.
    AwaitMemberList(Duration),
    /// Another address lists this node id: probe it, and wait this long for an answer. An answer
    /// halts the fresh host.
    Probe(Duration),
}

/// The guard's window: twice foca's `suspect_to_down_after`, which is how long a LIVE host has to
/// answer a probe (a stopped host stays listed longer than that, and is not what the window
/// measures).
pub fn guard_window(suspect_to_down: Duration) -> Duration {
    suspect_to_down
}

/// The guard's three cases (and the fourth, nobody reachable). `seed_configured`: the seeds name
/// another member. `a_seed_answered`: one of them answered a dial. `member_list_seen`: SWIM's first
/// member list has arrived. `listed_elsewhere`: that list has this node id at an address other than
/// this member's own.
pub fn guard_step(
    _seed_configured: bool,
    _a_seed_answered: bool,
    _member_list_seen: bool,
    _listed_elsewhere: bool,
    _suspect_to_down: Duration,
) -> GuardStep {
    GuardStep::Pass
}

/// A halt marker's contents: the node and epoch halted, and the newer epoch it was told of (`None`
/// for a halt by the guard, which has no newer epoch to name).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Marker {
    pub node: NodeId,
    pub halted: Epoch,
    pub told: Option<Epoch>,
}

/// Whether `marker` halts the store of `node` on `store_epoch`, as the manager ruled it: a marker
/// that names a newer epoch halts any store of THAT node whose epoch is below the newer epoch; a
/// marker that carries only the guard's reason halts only the store on the epoch it names; a marker
/// for another node is ignored.
pub fn marker_halts(_marker: &Marker, _node: NodeId, _store_epoch: Epoch) -> bool {
    false
}

/// The `--replace` flag of a restore: it names the epoch the restore minted, and is honoured only
/// for a store on that epoch (a marker left in a directory must not authorize the next restore into
/// it).
pub fn replace_honoured(_flag: Option<Epoch>, _store_epoch: Epoch) -> bool {
    false
}
