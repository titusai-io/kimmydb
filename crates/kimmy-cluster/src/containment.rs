//! D13 containment (ADR-274), the cluster half: when a peer's claim of an epoch is believed and
//! bounded, when a halt marker applies to a store, and what the mistaken-restore guard waits for.
//!
//! The decisions are pure functions, and the containment file's reading and writing are here too.

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

/// The most newest-epoch rows a member sends on its vectors, and reads from a peer's: one row per
/// node id ever seen is carried, the newest by epoch start. A cut loses no halt: a host learns it
/// is superseded from the peer's own table at the handshake, and a relayed row is the third source.
pub const MAX_RELAYED_EPOCHS: usize = 1_024;

/// What a member does with one row a verified peer relayed (ADR-274).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayedRow {
    /// Nothing: out of the bound, the maximum epoch, no epoch, or about this host's own node and
    /// not newer than its own epoch. Not recorded, not relayed on, not acted on.
    Drop,
    /// Record it: a newer epoch of another node, within the bound.
    Record,
    /// This host is superseded: the row names a strictly newer epoch of its own node, within the
    /// bound of its own clock.
    Halt,
}

/// Classify one relayed row. `own` is this host's node and epoch. A row is bounded as a claim is,
/// and one about this host's own node halts it only when strictly newer than its own epoch.
pub fn relayed_row(
    clock_ms: u64,
    skew_ms: u64,
    own: (NodeId, Epoch),
    row: (NodeId, Epoch),
) -> RelayedRow {
    let ((own_node, own_epoch), (node, epoch)) = (own, row);
    if epoch == Epoch::ZERO || !claim_within_bound(clock_ms, skew_ms, &epoch) {
        RelayedRow::Drop
    } else if node != own_node {
        RelayedRow::Record
    } else if epoch > own_epoch {
        RelayedRow::Halt
    } else {
        RelayedRow::Drop
    }
}

/// Read the rows a verified peer relayed on its vectors: at most [`MAX_RELAYED_EPOCHS`] of them,
/// each classified by [`relayed_row`], recorded or acted on. Returns whether this host was halted
/// by one, in which case the contact ends and no round runs with that peer.
///
/// Called on a verified peer's answer, where no write transaction is held (a halt takes the writer
/// gate), and it records through [`kimmy_storage::Engine::note_epoch`], which opens none.
pub(crate) fn take_relayed(engine: &kimmy_storage::Engine, relayed: &[(NodeId, Epoch)]) -> bool {
    static SAID: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if relayed.len() > MAX_RELAYED_EPOCHS && !SAID.swap(true, std::sync::atomic::Ordering::Relaxed)
    {
        tracing::warn!(
            rows = relayed.len(),
            cap = MAX_RELAYED_EPOCHS,
            "a peer relayed more newest-epoch rows than the cap; the rest are not read (said once)"
        );
    }
    let clock = engine.bound_clock_ms();
    let skew = engine.clock_bound().skew_ms;
    let own = (engine.node_id(), engine.epoch());
    for &(node, epoch) in relayed.iter().take(MAX_RELAYED_EPOCHS) {
        match relayed_row(clock, skew, own, (node, epoch)) {
            RelayedRow::Drop => {}
            RelayedRow::Record => {
                if let Err(e) = engine.note_epoch(node, epoch) {
                    tracing::debug!(error = %e, "a relayed epoch could not be noted");
                }
            }
            RelayedRow::Halt => {
                act_on_tell(engine, node, epoch);
                return engine.halted().is_some();
            }
        }
    }
    false
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

/// A test's hold between the read and the write of one directory's containment file, so that a second writer can be run there.
/// It is taken once, by the next writer of the directory it was set for: tests share a process.
#[cfg(test)]
pub(crate) mod between_read_and_write {
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    type Hold = Box<dyn FnOnce() + Send>;
    static HOLDS: Mutex<Vec<(PathBuf, Hold)>> = Mutex::new(Vec::new());

    pub(crate) fn hold_the_next(dir: &Path, hold: impl FnOnce() + Send + 'static) {
        HOLDS.lock().unwrap().push((dir.to_path_buf(), Box::new(hold)));
    }

    pub(super) fn reached(dir: &Path) {
        let hold = {
            let mut holds = HOLDS.lock().unwrap();
            holds.iter().position(|(held, _)| held == dir).map(|i| holds.swap_remove(i).1)
        };
        if let Some(hold) = hold {
            hold();
        }
    }
}

/// What the containment file holds: the halt marker (if the host was halted), and the epoch a
/// `--replace` restore minted (if the operator gave the flag). Either may be absent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MarkerFile {
    pub marker: Option<Marker>,
    pub replace: Option<Epoch>,
}

/// The file's lines are `key=value`: `node`, `floor` (an epoch's 36 hex digits), `guard_epoch` and
/// `guard_address` (both or neither), and `replace` (an epoch). A key that is absent is none.
fn render(file: &MarkerFile) -> String {
    let mut out = String::new();
    if let Some(marker) = &file.marker {
        out.push_str(&format!("node={}\nfloor={}\n", marker.node, marker.floor.to_hex()));
        if let Some(guard) = &marker.guard {
            out.push_str(&format!(
                "guard_epoch={}\nguard_address={}\n",
                guard.epoch.to_hex(),
                guard.address
            ));
        }
    }
    if let Some(replace) = &file.replace {
        out.push_str(&format!("replace={}\n", replace.to_hex()));
    }
    out
}

fn invalid(what: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("the containment file: {what}"))
}

fn parse(text: &str) -> std::io::Result<MarkerFile> {
    let mut node = None;
    let mut floor = None;
    let mut guard_epoch = None;
    let mut guard_address = None;
    let mut replace = None;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let (key, value) = line.split_once('=').ok_or_else(|| invalid("a line has no `=`"))?;
        let epoch =
            || Epoch::from_hex(value).ok_or_else(|| invalid("an epoch is not 36 hex digits"));
        match key {
            "node" => node = Some(value.parse::<NodeId>().map_err(|_| invalid("a bad node id"))?),
            "floor" => floor = Some(epoch()?),
            "guard_epoch" => guard_epoch = Some(epoch()?),
            "guard_address" => {
                guard_address =
                    Some(value.parse::<SocketAddr>().map_err(|_| invalid("a bad address"))?)
            }
            "replace" => replace = Some(epoch()?),
            other => return Err(invalid(&format!("an unknown key `{other}`"))),
        }
    }
    let guard = match (guard_epoch, guard_address) {
        (Some(epoch), Some(address)) => Some(GuardHalt { epoch, address }),
        (None, None) => None,
        _ => return Err(invalid("the guard's epoch and address come together")),
    };
    let marker = match (node, floor) {
        (Some(node), Some(floor)) => Some(Marker { node, floor, guard }),
        (None, None) if guard.is_none() => None,
        _ => return Err(invalid("a marker has a node and a floor")),
    };
    Ok(MarkerFile { marker, replace })
}

/// Write the containment file: to a temporary name, synced, then renamed, so a crash leaves the old
/// file or the new one and never half of one. An empty file (no marker, no flag) removes it.
pub fn write_marker_file(data_dir: &Path, file: &MarkerFile) -> std::io::Result<()> {
    use std::io::Write;
    let text = render(file);
    if text.is_empty() {
        return clear_marker_file(data_dir);
    }
    let target = data_dir.join(MARKER_FILE);
    let temporary = data_dir.join(format!("{MARKER_FILE}.tmp"));
    {
        let mut out = std::fs::File::create(&temporary)?;
        out.write_all(text.as_bytes())?;
        out.sync_all()?;
    }
    std::fs::rename(&temporary, &target)?;
    // The rename itself is made durable with the directory.
    std::fs::File::open(data_dir)?.sync_all()
}

/// Read the containment file: an absent file is an empty one, and one that does not parse is an
/// error, never an empty file (a marker that cannot be read must not read as no halt).
pub fn read_marker_file(data_dir: &Path) -> std::io::Result<MarkerFile> {
    match std::fs::read_to_string(data_dir.join(MARKER_FILE)) {
        Ok(text) => parse(&text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(MarkerFile::default()),
        Err(e) => Err(e),
    }
}

/// Remove the containment file; removing an absent one is not an error.
pub fn clear_marker_file(data_dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(data_dir.join(MARKER_FILE)) {
        Ok(()) => std::fs::File::open(data_dir)?.sync_all(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// The epoch a restore of `node`'s backup into `data_dir` must mint strictly above: the floor of
/// the marker, the newest epoch the directory was ever told of. `None` for no marker, a marker of
/// another node, a marker whose floor is none, and a file with only a `--replace` flag.
pub fn restore_floor(data_dir: &Path, node: NodeId) -> std::io::Result<Option<Epoch>> {
    let file = read_marker_file(data_dir)?;
    Ok(file.marker.filter(|m| m.node == node && m.floor != Epoch::ZERO).map(|m| m.floor))
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
    if let Err(e) = record_the_tell(engine, node, epoch) {
        tracing::error!(error = %e, "the containment file could not be written beside the store");
    }
}

/// Write the tell into the containment file beside the store, after the halt row has committed: the
/// restart record, and the floor a recovery restore mints above. The halt row is the authority; a
/// crash between the two re-derives the halt from the first contact.
pub(crate) fn record_the_tell(
    engine: &kimmy_storage::Engine,
    node: NodeId,
    epoch: Epoch,
) -> std::io::Result<()> {
    let Some(dir) = engine.path().parent() else { return Ok(()) };
    let held = read_marker_file(dir).unwrap_or_default();
    #[cfg(test)]
    between_read_and_write::reached(dir);
    let marker = marker_told(held.marker.as_ref(), node, epoch);
    write_marker_file(dir, &MarkerFile { marker: Some(marker), ..held })
}

/// Apply the containment file's rule at a start (ruling 7): a store below the marker's floor is
/// halted, and so is the store on the epoch the guard halted, so an older copy put back under a
/// marker takes no writes until its first contact. Returns whether it halted. A file that cannot be
/// read is an error and never an empty file: the start refuses rather than run unmarked.
pub fn apply_marker_at_start(
    engine: &kimmy_storage::Engine,
    data_dir: &Path,
) -> std::io::Result<bool> {
    let Some(marker) = read_marker_file(data_dir)?.marker else { return Ok(false) };
    if !marker_halts(&marker, engine.node_id(), engine.epoch()) {
        return Ok(false);
    }
    let halted = if marker.floor > engine.epoch() {
        engine.halt(marker.floor)
    } else {
        engine.halt_by_guard()
    };
    halted.map_err(std::io::Error::other)?;
    Ok(true)
}

/// Halt this host because the mistaken-restore guard found its node live at `address` (ADR-274):
/// the halt has no newer epoch to name, and the containment file records the guard's halt (the
/// epoch that halted itself and the address that answered), keeping any floor an earlier tell set.
/// The halt row is the authority; a failure to write the file is logged.
pub fn halt_by_guard(engine: &kimmy_storage::Engine, address: SocketAddr) {
    if let Err(e) = engine.halt_by_guard() {
        tracing::error!(
            error = %e,
            "this host could not be halted; it refuses writes if the halt's mirror was set"
        );
    }
    let Some(dir) = engine.path().parent() else { return };
    let held = read_marker_file(dir).unwrap_or_default();
    #[cfg(test)]
    between_read_and_write::reached(dir);
    let guard = GuardHalt { epoch: engine.epoch(), address };
    let marker = marker_guard_halted(held.marker.as_ref(), engine.node_id(), guard);
    if let Err(e) = write_marker_file(dir, &MarkerFile { marker: Some(marker), ..held }) {
        tracing::error!(error = %e, "the containment file could not be written beside the store");
    }
}
