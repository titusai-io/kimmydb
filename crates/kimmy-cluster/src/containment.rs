//! D13 containment (ADR-274), the cluster half: when a peer's claim of an epoch is believed and
//! bounded, when a halt marker applies to a store, and what the mistaken-restore guard waits for.
//!
//! **This file is the scaffold of the tests commit and nothing else**: each function compiles and
//! does nothing, so that every test in `containment_tests.rs` is red on its designed assertion and
//! none on a missing symbol. The code commits replace each body, and none of these stubs survives
//! to the last commit (a source scan asserts it).

use std::net::SocketAddr;
use std::path::Path;
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

/// Why the mistaken-restore guard halted a fresh host: the epoch that halted itself, and the
/// address that answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuardHalt {
    pub epoch: Epoch,
    pub address: SocketAddr,
}

/// A halt marker's contents (the model row's finding 2): the node; the floor, which is the newest
/// epoch this directory was ever told of (`Epoch::ZERO` for none); and, when the guard halted it,
/// the epoch that halted itself and the address that answered. A later guard-only halt keeps the
/// floor an earlier tell set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Marker {
    pub node: NodeId,
    pub floor: Epoch,
    pub guard: Option<GuardHalt>,
}

/// Whether `marker` halts the store of `node` on `store_epoch`: a store below the floor is halted,
/// a store on the guard's epoch is halted, and another node's marker is ignored.
pub fn marker_halts(_marker: &Marker, _node: NodeId, _store_epoch: Epoch) -> bool {
    false
}

/// The marker after a host of `node` was told of the newer epoch `told`: the floor is the larger of
/// what it was and `told`, and a guard halt already recorded is kept.
pub fn marker_told(_previous: Option<&Marker>, node: NodeId, _told: Epoch) -> Marker {
    Marker { node, floor: Epoch::ZERO, guard: None }
}

/// The marker after the guard halted a host of `node`: the guard's halt is recorded and the floor
/// an earlier tell set is kept (row 4c: a recovery that finds the newer host up is halted by the
/// guard and must still mint above the floor).
pub fn marker_guard_halted(_previous: Option<&Marker>, node: NodeId, guard: GuardHalt) -> Marker {
    Marker { node, floor: Epoch::ZERO, guard: Some(guard) }
}

/// Whether a told epoch is acted on by the told host (the model row's finding 3): it must be
/// strictly newer than the host's own epoch and within the bound of the host's own clock, as a
/// relayed row is. A tell beyond the bound is not acted on.
pub fn tell_acted_on(_clock_ms: u64, _skew_ms: u64, _own: Epoch, _told: Epoch) -> bool {
    true
}

/// Whether a member runs a round with a peer: not when it knows the peer's epoch of its node is
/// older than the newest it has recorded, whether or not the tell was acted on (the model row's
/// finding 3, `round-with-superseded`).
pub fn runs_round(_known_newest: Epoch, _peer_epoch: Epoch) -> bool {
    true
}

/// What the recovery restore (delete the store, keep the marker, `kimmyd restore`) comes to: the
/// recovered host is a third host of the node, so a live newer host halts it through the guard
/// unless `--replace` was honoured; with it the recovered host proceeds, and the newer host is then
/// told and halts (the model row's rows 4b and 4c).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Recovery {
    pub recovered_host_halts: bool,
    pub newer_host_halts: bool,
}

pub fn recovery_outcome(_newer_host_up: bool, _replace_honoured: bool) -> Recovery {
    Recovery { recovered_host_halts: false, newer_host_halts: false }
}

/// The `--replace` flag of a restore: it names the epoch the restore minted, and is honoured only
/// for a store on that epoch (a marker left in a directory must not authorize the next restore into
/// it).
pub fn replace_honoured(_flag: Option<Epoch>, _store_epoch: Epoch) -> bool {
    false
}

/// The containment file's name, in the data directory. It holds the halt marker and the `--replace`
/// flag, and it lives beside a healthy host as often as beside a halted one, so its name does not
/// say "halted".
pub const MARKER_FILE: &str = "kimmy.containment";

/// What the containment file holds: the halt marker (if the host was halted), and the epoch a
/// `--replace` restore minted (if the operator gave the flag). Either may be absent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MarkerFile {
    pub marker: Option<Marker>,
    pub replace: Option<Epoch>,
}

/// Write the containment file: to a temporary name, synced, then renamed, so a crash leaves the old
/// file or the new one and never half of one.
pub fn write_marker_file(_data_dir: &Path, _file: &MarkerFile) -> std::io::Result<()> {
    Ok(())
}

/// Read the containment file: an absent file is an empty one.
pub fn read_marker_file(_data_dir: &Path) -> std::io::Result<MarkerFile> {
    Ok(MarkerFile::default())
}

/// Remove the containment file; removing an absent one is not an error.
pub fn clear_marker_file(_data_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

/// The epoch a restore of `node`'s backup into `data_dir` must mint strictly above: the floor of
/// the marker, the newest epoch the directory was ever told of. `None` for no marker, a marker of
/// another node, a marker whose floor is none, and a file with only a `--replace` flag.
pub fn restore_floor(_data_dir: &Path, _node: NodeId) -> std::io::Result<Option<Epoch>> {
    Ok(None)
}
