//! D13 containment (ADR-274), the cluster half: when a peer's claim of an epoch is believed and
//! bounded, when a halt marker applies to a store, and what the mistaken-restore guard waits for.
//!
//! The pure decisions are real. **The containment file's functions (`write_marker_file`,
//! `read_marker_file`, `clear_marker_file`, `restore_floor`) are still the scaffold of the tests
//! commits**: they compile and do nothing, so that the tests of the file are red on their designed
//! assertions. A later code commit replaces them, and none survives to the last commit (a source
//! scan asserts it).

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
pub fn bound_clock_ms(physical_ms: u64, hlc_last_wall_ms: u64) -> u64 {
    physical_ms.max(hlc_last_wall_ms)
}

/// Whether a claimed epoch is within the bound at `clock_ms`: its start's wall time is at most
/// `clock_ms + skew_ms`, inclusive. `Epoch::MAX`, whose start bytes are `0xFF`, is beyond any
/// bound.
pub fn claim_within_bound(clock_ms: u64, skew_ms: u64, epoch: &Epoch) -> bool {
    *epoch != Epoch::MAX && epoch.start_hlc().wall_ms <= clock_ms.saturating_add(skew_ms)
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
    suspect_to_down * 2
}

/// The guard's three cases (and the fourth, nobody reachable). `seed_configured`: the seeds name
/// another member. `a_seed_answered`: one of them answered a dial. `member_list_seen`: SWIM's first
/// member list has arrived. `listed_elsewhere`: that list has this node id at an address other than
/// this member's own.
pub fn guard_step(
    seed_configured: bool,
    a_seed_answered: bool,
    member_list_seen: bool,
    listed_elsewhere: bool,
    suspect_to_down: Duration,
) -> GuardStep {
    if !seed_configured || !a_seed_answered {
        GuardStep::Pass
    } else if !member_list_seen {
        GuardStep::AwaitMemberList(SEED_TIMEOUT)
    } else if listed_elsewhere {
        GuardStep::Probe(guard_window(suspect_to_down))
    } else {
        GuardStep::Pass
    }
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
pub fn marker_halts(marker: &Marker, node: NodeId, store_epoch: Epoch) -> bool {
    marker.node == node
        && (store_epoch < marker.floor || marker.guard.is_some_and(|g| g.epoch == store_epoch))
}

/// The marker after a host of `node` was told of the newer epoch `told`: the floor is the larger of
/// what it was and `told`, and a guard halt already recorded is kept.
pub fn marker_told(previous: Option<&Marker>, node: NodeId, told: Epoch) -> Marker {
    let previous = previous.filter(|p| p.node == node);
    Marker {
        node,
        floor: previous.map_or(told, |p| p.floor.max(told)),
        guard: previous.and_then(|p| p.guard),
    }
}

/// The marker after the guard halted a host of `node`: the guard's halt is recorded and the floor
/// an earlier tell set is kept (row 4c: a recovery that finds the newer host up is halted by the
/// guard and must still mint above the floor).
pub fn marker_guard_halted(previous: Option<&Marker>, node: NodeId, guard: GuardHalt) -> Marker {
    let previous = previous.filter(|p| p.node == node);
    Marker { node, floor: previous.map_or(Epoch::ZERO, |p| p.floor), guard: Some(guard) }
}

/// Whether a told epoch is acted on by the told host (the model row's finding 3): it must be
/// strictly newer than the host's own epoch and within the bound of the host's own clock, as a
/// relayed row is. A tell beyond the bound is not acted on.
pub fn tell_acted_on(clock_ms: u64, skew_ms: u64, own: Epoch, told: Epoch) -> bool {
    told > own && claim_within_bound(clock_ms, skew_ms, &told)
}

/// Whether a member runs a round with a peer: not when it knows the peer's epoch of its node is
/// older than the newest it has recorded, whether or not the tell was acted on (the model row's
/// finding 3, `round-with-superseded`).
pub fn runs_round(known_newest: Epoch, peer_epoch: Epoch) -> bool {
    peer_epoch >= known_newest
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

pub fn recovery_outcome(newer_host_up: bool, replace_honoured: bool) -> Recovery {
    Recovery {
        recovered_host_halts: newer_host_up && !replace_honoured,
        newer_host_halts: newer_host_up && replace_honoured,
    }
}

/// The `--replace` flag of a restore: it names the epoch the restore minted, and is honoured only
/// for a store on that epoch (a marker left in a directory must not authorize the next restore into
/// it).
pub fn replace_honoured(flag: Option<Epoch>, store_epoch: Epoch) -> bool {
    flag == Some(store_epoch)
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

/// Act on a tell that a newer epoch of `node` exists (the model row's finding 3). Called with a
/// tell from a verified peer, read as the first frame after a handshake or as the reply to a first
/// request. It halts this host only when `node` is this host's own and the told epoch is strictly
/// newer than its own and within the bound of its own clock; anything else is logged and not acted
/// on. The contact ends either way, and no round runs with that peer.
pub(crate) fn act_on_tell(engine: &kimmy_storage::Engine, node: NodeId, epoch: Epoch) {
    if node != engine.node_id() {
        tracing::debug!(?node, "a tell about another node: not acted on");
        return;
    }
    let clock = engine.bound_clock_ms();
    let skew = engine.clock_bound().skew_ms;
    if !tell_acted_on(clock, skew, engine.epoch(), epoch) {
        tracing::warn!(
            told = %epoch.to_hex(),
            own = %engine.epoch().to_hex(),
            "a peer told this host of an epoch of its node that is not newer than its own or is \
             beyond its clock bound: not acted on, and no round runs with that peer"
        );
        return;
    }
    tracing::warn!(
        told = %epoch.to_hex(),
        own = %engine.epoch().to_hex(),
        "this host was told that a newer epoch of its node exists: it halts"
    );
    // Halt FIRST and whatever else happens: the note is a best effort and must not delay the
    // mirror. `halt` takes the writer gate, which is not re-entrant, so this is called only where
    // no write transaction is held (a test pins the call sites).
    if let Err(e) = kimmy_storage::blocking(|| engine.halt(epoch)) {
        tracing::error!(
            error = %e,
            "this host could not be halted; it refuses writes if the halt's mirror was set"
        );
    }
    if let Err(e) = engine.note_epoch(node, epoch) {
        tracing::warn!(error = %e, "the newer epoch could not be noted; a handshake relearns it");
    }
}
