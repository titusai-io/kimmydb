//! The catching-up marker (ADR-202): a member that knows it is behind says so,
//! and stops doing what a stale store must not do.
//!
//! A member cannot tell "the first node of a new cluster" from "a wiped member
//! of an existing one" by looking at itself, so the gate rests on a fact it
//! knows **locally and durably**, and clears on a fact it observes. The fact is
//! a file in the data directory, `kimmy.catching-up`, and not a key in the
//! store: a backup copies the store's meta table, so a restored or copied store
//! would carry the marker, or lose it, for the wrong reason.
//!
//! **While it is set, no owner work of any class runs here** (expiry, embeddings,
//! webhook dispatch): a catching-up member that expires a document from a stale
//! copy mints a delete stamped *now*, which beats a refreshed copy that arrived
//! with an earlier stamp under last-writer-wins, so an update is lost. Its
//! peers stop assigning it work too (`facts::Facts::catching_up`).
//!
//! **Clearing** is judged once per sync tick from what the tick's successful
//! contacts read (see [`CatchUp::evaluate`]):
//! - by **dominance**: this member's witnessed vector covers each counting
//!   peer's servable vector, and no whole-database snapshot cursor remains;
//! - by the **mutual clear**, for a `seeded_empty` member only: every member it
//!   has ever seen since the marker was set has been reached in the window, all
//!   of them are `seeded_empty` themselves, and each pair covers the other, so a
//!   whole-cluster cold start does not wait for a peer that is not coming;
//! - by the **operator**, deleting the file, which is read live every tick.
//!
//! **It cannot hold for ever on a guess**: with no *counting* peer reached for
//! `catch_up_wait_secs`, counted from the member's first sync tick (the marker is
//! set before the store opens, and a slow open must not use the wait up), the
//! gate opens and the state reads `unknown`, with owner work still off and a
//! `WARN` every ten minutes. Once a counting peer is
//! reached and is ahead the gate closes again, and holds for as long as the
//! catch-up takes: that is knowledge, not a guess.
//!
//! **Nor does it clear past a member that may hold what it lost** (ADR-212).
//! Every member present in the run that has not answered the replay of this
//! member's own origin owes it, until it answers or has been gone from the
//! cluster continuously for the dwell; the set is kept on disk with the replay
//! floor. An owing member holds the clear for the wait, and past that the member
//! serves as `unknown` (the owed latch) rather than clearing, until nobody owes.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use kimmy_core::{NodeId, VersionVector};
use parking_lot::Mutex;
use tracing::{info, warn};

use crate::facts::{CatchUpReason, Facts};
use crate::membership::Members;

/// The marker's file name, in the data directory.
pub const FILE: &str = "kimmy.catching-up";

/// The most node ids the marker accumulates. Past it the mutual clear is
/// refused (fail closed) and a `WARN` says so; the dominance clear and the
/// operator clear still work.
pub const MAX_SEEN: usize = 1_024;

/// The states `kimmy_catching_up{reason}` is one-hot over, in the order it
/// renders them: the reasons in [`CatchUpReason::ALL`] order between `none` and
/// `unknown`.
pub const STATES: [&str; 5] = ["none", "seeded_empty", "restored", "snapshot", "unknown"];

/// The index of `state` in [`STATES`]. Panics on a name that is not one: it is
/// asked with literals, and a typo should fail at once and not read as `none`.
pub fn slot_of(state: &str) -> usize {
    STATES.iter().position(|known| *known == state).expect("a known catching-up state")
}

/// How often, past the bound, the marker says it is still set.
const WARN_EVERY: Duration = Duration::from_secs(600);

/// The least time a member that owes the replay must have been gone from the
/// cluster, continuously, before it is treated as having left (ADR-212): the
/// dwell is the longer of this and `cluster.catch_up_wait_secs`. A constant,
/// not a configuration key, so a configuration stays loadable by an older build.
pub const DWELL_FLOOR: Duration = Duration::from_secs(600);

/// Why a member whose marker is set serves as `unknown` (`/readyz`'s
/// `unknown_because`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnknownCause {
    /// No counting peer has been reached for the wait.
    NoCountingPeer,
    /// A member the cluster lists live owes the replay of this member's own
    /// origin past its hold.
    OwedReplay,
}

impl UnknownCause {
    /// The name `/readyz` gives it.
    pub fn label(self) -> &'static str {
        match self {
            Self::NoCountingPeer => "no_counting_peer",
            Self::OwedReplay => "owed_replay",
        }
    }
}

/// What the marker file says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Marker {
    pub reason: CatchUpReason,
    /// When it was set, milliseconds since the epoch.
    pub since_ms: u64,
    /// Every node id seen since it was set (via SWIM, a contact, or as an origin
    /// in a peer's vector), bounded by [`MAX_SEEN`].
    pub seen: BTreeSet<NodeId>,
    /// More than [`MAX_SEEN`] ids were seen: the set is incomplete, and the
    /// mutual clear is refused.
    pub overflowed: bool,
}

impl Marker {
    fn new(reason: CatchUpReason) -> Self {
        Self { reason, since_ms: now_ms(), seen: BTreeSet::new(), overflowed: false }
    }

    /// `key = value` lines: `reason`, `since`, `written_by`, `seen` (one per id)
    /// and `seen_overflow`.
    fn render(&self) -> String {
        let mut out = format!(
            "reason = {}\nsince = {}\nwritten_by = kimmyd {}\n",
            self.reason.label(),
            self.since_ms,
            kimmy_core::build::VERSION
        );
        for id in &self.seen {
            out.push_str(&format!("seen = {id}\n"));
        }
        if self.overflowed {
            out.push_str("seen_overflow = true\n");
        }
        out
    }

    /// Read what a file says. **A file that cannot be read as this build writes
    /// it is a marker still**, with the reason it names or `seeded_empty`: the
    /// conservative side is gated, and an operator who wants it gone deletes it.
    fn parse(text: &str) -> Self {
        let mut marker = Self::new(CatchUpReason::SeededEmpty);
        marker.since_ms = 0;
        for line in text.lines() {
            let Some((key, value)) = line.split_once('=') else { continue };
            let (key, value) = (key.trim(), value.trim());
            match key {
                "reason" => {
                    if let Some(reason) = CatchUpReason::parse(value) {
                        marker.reason = reason;
                    }
                }
                "since" => marker.since_ms = value.parse().unwrap_or(0),
                "seen" => {
                    if let Ok(id) = value.parse::<NodeId>() {
                        if marker.seen.len() < MAX_SEEN {
                            marker.seen.insert(id);
                        } else {
                            marker.overflowed = true;
                        }
                    }
                }
                "seen_overflow" => marker.overflowed = value == "true",
                _ => {}
            }
        }
        marker
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// A peer this tick reached by a successful contact, and what the contact read.
#[derive(Clone, Debug)]
pub struct Reached {
    pub node: NodeId,
    /// What the peer can serve, from this round's `Vectors` frame.
    pub servable: VersionVector,
    /// What the peer has processed, from the same frame; `None` from a peer that
    /// predates the field.
    pub witnessed: Option<VersionVector>,
    /// What the peer said about itself; `None` from a peer that has sent no block
    /// (an older version), which counts.
    pub facts: Option<Arc<Facts>>,
}

/// What one tick gives the marker to judge.
pub struct Tick<'a> {
    pub me: NodeId,
    pub reached: &'a [Reached],
    /// This member's own vectors, read after the contacts.
    pub mine_witnessed: &'a VersionVector,
    pub mine_servable: &'a VersionVector,
    /// A whole-database snapshot pull from a current member is still under way.
    pub snapshot_pending: bool,
    /// The members SWIM lists live, or `None` with no membership.
    pub live: Option<&'a BTreeSet<NodeId>>,
    /// `cluster.expected_members`, when set.
    pub expected_members: Option<usize>,
    pub now: Instant,
}

/// What one tick tells the replay of this member's own origin about who can
/// answer it.
pub struct ReplayTick<'a> {
    pub me: NodeId,
    /// The members SWIM lists, or `None` with no membership.
    pub members: Option<&'a Members>,
    /// The peers this tick reached by a successful contact, with what each
    /// contact read.
    pub reached: &'a [Reached],
    /// `cluster.expected_members`, when set.
    pub expected_members: Option<usize>,
    pub now: Instant,
}

/// What a tick's judgement did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// No marker was set.
    NotSet,
    /// Still set.
    Kept,
    /// Cleared, and by which rule.
    Cleared(&'static str),
}

/// The marker and what the request path and the sync loop need of it.
pub struct CatchUp {
    path: PathBuf,
    wait: Duration,
    /// The origin of every `Instant` stored as milliseconds below.
    epoch: Instant,
    set: AtomicBool,
    reason: AtomicU8,
    /// When a counting peer was last reached, milliseconds since `epoch`; before
    /// the first sync tick, when the marker was set.
    counting_ms: AtomicU64,
    /// The first sync tick has begun: the first point at which this member can
    /// dial a peer. The wait counts from here, and until it the member has not
    /// had a chance to be counted, so it is never `unknown` for want of a
    /// counting peer.
    ticked: AtomicBool,
    /// Bumped whenever the member may have become gated: a marker set, or a
    /// counting peer reached that closes a gate the wait had opened. What an open
    /// change stream waits on to end when the member starts refusing.
    changes: tokio::sync::watch::Sender<u64>,
    /// The replay of this member's own origin (ADR-202): armed at the start of a
    /// clustered member, answered by each peer once per run.
    replay: Mutex<Replay>,
    state: Mutex<State>,
    /// This run created the store, and wrote a `seeded_empty` marker for it.
    created_this_run: AtomicBool,
    /// The owed latch (ADR-212): the marker's clear rule held while members still
    /// owed the replay past their hold, so the member serves as `unknown`.
    owed_open: AtomicBool,
    /// A `snapshot` mark has gated the member since the marker was set: a
    /// snapshot that resumes does not gate it again.
    snapshot_marked: AtomicBool,
}

/// Where the replay of this member's own origin stands in this run.
///
/// Armed at open; each distinct peer is asked once per run, the first time it is
/// reached, and its answer is remembered. **Every member present in this run
/// (listed live by SWIM, or reached) that has not answered owes it** until it
/// answers or has been gone from the cluster, continuously, for the dwell
/// (ADR-212): an owing member holds the clear for the wait, then the member
/// serves as `unknown`, and the floor the replay asks from is kept on disk for as
/// long as anyone owes.
#[derive(Default)]
struct Replay {
    armed: bool,
    /// Where each ask starts: the lowest own-origin position any start since the
    /// last complete replay saw at open, so a start that wrote before it reached a
    /// peer does not lose the entries between.
    floor: kimmy_core::Hlc,
    /// The peers that have answered this run.
    answered: BTreeSet<NodeId>,
    /// How far each peer's replay has read and applied, across rounds.
    cursors: BTreeMap<NodeId, kimmy_core::Hlc>,
    /// How long a peer keeps the oplog: what is older than this can no longer be
    /// read back, and asking for it is answered `BeyondHorizon`. `None` when
    /// retention collection is off.
    retention: Option<Duration>,
    /// The peers whose `BeyondHorizon` answer has been reported.
    horizon_reported: BTreeSet<NodeId>,
    /// When each peer began answering `BeyondHorizon` without an answer between:
    /// the replay gives up on it after the wait.
    horizon_since: BTreeMap<NodeId, Instant>,
    /// When the replay was armed.
    armed_at: Option<Instant>,
    /// The lowest position the replay would have asked from before the clamp to
    /// the retention: what the retention `WARN` compares the horizon with.
    floor_unclamped: kimmy_core::Hlc,
    /// A floor file was there before this run armed the replay.
    floor_existed: bool,
    /// The members that owe the replay. Only members present in this run (or
    /// loaded from the floor file) are here: an origin named only in a vector is
    /// not, since a member that has gone for good keeps its id in every vector.
    owing: BTreeMap<NodeId, Owing>,
    /// When each member was first present in this run (loaded ones: the arming):
    /// its hold runs for the wait from the later of this and `armed_at`. Never
    /// removed during the run, so a member that leaves and comes back gets no
    /// second hold.
    first_seen: BTreeMap<NodeId, Instant>,
    /// The first successful contact of the run: the start-race guard's window
    /// opens here.
    first_contact_at: Option<Instant>,
    /// Every origin named in a reached peer's vectors in this run.
    origins: BTreeSet<NodeId>,
    /// Every member present at some tick of this run.
    present: BTreeSet<NodeId>,
    /// The start-race guard held at the last tick.
    guard_holds: bool,
    guard_warned: bool,
    /// For each peer, the highest own-origin stamp its replay has proven lost in
    /// this run: a proof at or below it is not new evidence.
    proven: BTreeMap<NodeId, kimmy_core::Hlc>,
    /// The owed ids last written to the floor file, or last tried.
    recorded_owed: BTreeSet<NodeId>,
    /// The members that owed at the last tick judged while the owed latch was
    /// open: what [`CatchUp::owed_members`] says while it is, so the list and
    /// `owed_replay` change together, at a tick, and never disagree between two.
    latched_owed: Vec<NodeId>,
    owed_capped: bool,
    retention_warned: bool,
    unmarked_warned: Option<Instant>,
}

/// A member that owes the replay.
#[derive(Clone, Debug, Default)]
struct Owing {
    /// Since when it has been absent without a break, if it is absent.
    absent_since: Option<Instant>,
    /// Its address in SWIM's set when it was last listed.
    addr: Option<std::net::SocketAddr>,
    /// SWIM lists it at this tick.
    listed: bool,
    /// SWIM has listed it in this run.
    ever_listed: bool,
    /// The generation of `addr` at the last tick: one that moved means SWIM
    /// brought it (back) up in between.
    generation: Option<u64>,
}

impl Replay {
    /// Whether `node`'s hold on the clear still runs at `now`: the wait from the
    /// later of the arming and its first being present.
    fn in_hold(&self, node: &NodeId, wait: Duration, now: Instant) -> bool {
        let Some(first) = self.first_seen.get(node) else { return true };
        let from = self.armed_at.map_or(*first, |armed| armed.max(*first));
        now.saturating_duration_since(from) < wait
    }

    /// Where the replay against `peer` asks from.
    fn asks_from(&self, peer: NodeId) -> kimmy_core::Hlc {
        self.cursors
            .get(&peer)
            .copied()
            .unwrap_or_default()
            .max(self.floor)
            .max(oldest_readable(self.retention))
    }

    /// `peer` has answered: it no longer owes.
    fn answered_by(&mut self, peer: NodeId) {
        self.answered.insert(peer);
        if self.owing.remove(&peer).is_some() {
            info!(%peer, from = ?self.asks_from(peer), "a member owing the replay has answered");
        }
    }

    /// The addresses SWIM lists the owing members at.
    fn owed_addresses(&self) -> Vec<String> {
        self.owing
            .values()
            .filter(|owing| owing.listed)
            .filter_map(|owing| owing.addr.map(|addr| addr.to_string()))
            .collect()
    }

    fn owed_ids(&self) -> Vec<String> {
        self.owing.keys().map(ToString::to_string).collect()
    }
}

/// What a `BeyondHorizon` answer to the replay comes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Horizon {
    /// The first from this peer: say so.
    First,
    /// The replay stays owed and asks again next round.
    Again,
    /// The peer has answered `BeyondHorizon` for the whole wait: the replay stops
    /// asking it, and it counts as settled (it cannot serve those entries).
    GaveUp,
}

impl std::fmt::Debug for CatchUp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatchUp")
            .field("reason", &self.reason())
            .field("replay_armed", &self.replay_armed())
            .finish_non_exhaustive()
    }
}

struct State {
    marker: Option<Marker>,
    /// Each peer's last successful contact, when it was and what it read: the
    /// clear rules judge every peer reached **in the window**, not only the ones
    /// this tick happened to reach. One entry per peer, so bounded by the members
    /// the cluster has (a contact replaces the peer's entry), emptied when the
    /// marker clears, and pruned of entries far older than the wait.
    reached: BTreeMap<NodeId, (Instant, Reached)>,
    warned: Option<Instant>,
}

impl CatchUp {
    /// Read the marker file in `data_dir`, if there is one. `wait` is
    /// `cluster.catch_up_wait_secs`.
    pub fn open(data_dir: &Path, wait: Duration) -> Arc<Self> {
        let path = data_dir.join(FILE);
        let marker = match std::fs::read_to_string(&path) {
            Ok(text) => Some(Marker::parse(&text)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            // A file that exists and cannot be read is a marker still.
            Err(e) => {
                warn!(error = %e, path = %path.display(), "could not read the catching-up marker; treating it as set");
                Some(Marker::new(CatchUpReason::SeededEmpty))
            }
        };
        let this = Arc::new(Self {
            path,
            wait,
            epoch: Instant::now(),
            set: AtomicBool::new(false),
            reason: AtomicU8::new(0),
            counting_ms: AtomicU64::new(0),
            ticked: AtomicBool::new(false),
            changes: tokio::sync::watch::channel(0).0,
            replay: Mutex::new(Replay::default()),
            state: Mutex::new(State { marker: None, reached: BTreeMap::new(), warned: None }),
            created_this_run: AtomicBool::new(false),
            owed_open: AtomicBool::new(false),
            snapshot_marked: AtomicBool::new(false),
        });
        if let Some(marker) = marker {
            info!(
                reason = marker.reason.label(),
                since_ms = marker.since_ms,
                "the catching-up marker is set: this member is behind until it has caught up"
            );
            this.install(marker, Instant::now());
        }
        this
    }

    fn install(&self, marker: Marker, now: Instant) {
        self.reason.store(marker.reason.code(), Relaxed);
        self.set.store(true, Relaxed);
        // The bound counts from the moment it is set.
        self.counting_ms.store(self.ms(now), Relaxed);
        self.state.lock().marker = Some(marker);
        self.changes.send_modify(|n| *n += 1);
    }

    /// This run created the store and wrote its `seeded_empty` marker: the
    /// member has no history of its own origin from before this run.
    pub fn note_store_created_this_run(&self) {
        self.created_this_run.store(true, Relaxed);
    }

    /// How long a member that owes the replay must have been gone from the
    /// cluster, continuously, before it no longer owes it: the longer of
    /// [`DWELL_FLOOR`] and the wait.
    pub fn dwell(&self) -> Duration {
        self.wait.max(DWELL_FLOOR)
    }

    /// A receiver that is woken when this member may have become gated (a marker
    /// set, or the gate closed again after the wait had opened it). Whether it is
    /// gated is [`Self::gated`], asked after the wake.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changes.subscribe()
    }

    fn ms(&self, at: Instant) -> u64 {
        u64::try_from(at.saturating_duration_since(self.epoch).as_millis()).unwrap_or(u64::MAX)
    }

    /// Whether the marker is set.
    pub fn is_set(&self) -> bool {
        self.set.load(Relaxed)
    }

    /// Why it is set, when it is.
    pub fn reason(&self) -> Option<CatchUpReason> {
        self.is_set().then(|| CatchUpReason::from_code(self.reason.load(Relaxed))).flatten()
    }

    /// Set with no counting peer reached for the bound: the gate is open, the
    /// state reads `unknown`, and owner work is still off.
    pub fn unknown(&self, now: Instant) -> bool {
        self.is_set() && (self.owed_open.load(Relaxed) || self.no_counting_peer(now))
    }

    /// The first sync tick begins at `now`: the wait counts from here, and not
    /// from the marker being set, which is before the store opens. A store whose
    /// open outlasts the wait must still be gated at its first answer and for a
    /// whole wait of trying peers. Later calls do nothing.
    pub fn first_tick(&self, now: Instant) {
        if !self.ticked.load(Relaxed) {
            self.counting_ms.store(self.ms(now), Relaxed);
            self.ticked.store(true, Relaxed);
            if let Some(reason) = self.reason() {
                info!(
                    reason = reason.label(),
                    wait_s = self.wait.as_secs(),
                    "the catching-up wait starts: this member's first sync tick has begun, and \
                     it serves as unknown only if no counting peer is reached within the wait"
                );
            }
        }
    }

    /// No counting peer has been reached for the wait, as of `now`, counted from
    /// the first sync tick at the earliest.
    fn no_counting_peer(&self, now: Instant) -> bool {
        self.ticked.load(Relaxed)
            && self.ms(now)
                > self
                    .counting_ms
                    .load(Relaxed)
                    .saturating_add(u64::try_from(self.wait.as_millis()).unwrap_or(u64::MAX))
    }

    /// Whether client requests are refused: set, and the bound has not run out.
    pub fn gated(&self, now: Instant) -> bool {
        self.is_set() && !self.unknown(now)
    }

    /// Why the member serves as `unknown` at `now`, when it does.
    /// When both causes hold it is the owed replay: that is the one that governs
    /// what happens next (a peer that is ahead does not gate it again).
    pub fn unknown_because(&self, now: Instant) -> Option<UnknownCause> {
        if !self.is_set() {
            None
        } else if self.owed_open.load(Relaxed) {
            Some(UnknownCause::OwedReplay)
        } else if self.no_counting_peer(now) {
            Some(UnknownCause::NoCountingPeer)
        } else {
            None
        }
    }

    /// The members that owe the replay of this member's own origin, sorted:
    /// every one, inside its hold or past it. While the owed latch is open
    /// (`unknown_because` is `owed_replay`) it is the set as of the tick that
    /// held or last judged the latch, which is never empty; a member that
    /// answers in between is dropped from it, and the latch with it, at the next
    /// tick. Otherwise it is the set as it stands.
    pub fn owed_members(&self) -> Vec<NodeId> {
        let replay = self.replay.lock();
        if self.owed_open.load(Relaxed) {
            return replay.latched_owed.clone();
        }
        replay.owing.keys().copied().collect()
    }

    /// Whether the owed hold and `unknown (owed)` pass this member by: its marker
    /// is `seeded_empty`, written by this run because this run created the store,
    /// and no floor file was there before this run armed the replay. Such a
    /// member has no history of its own origin from before this run to have lost.
    fn exempt(&self) -> bool {
        self.reason() == Some(CatchUpReason::SeededEmpty)
            && self.created_this_run.load(Relaxed)
            && !self.replay.lock().floor_existed
    }

    /// One-hot label for `kimmy_catching_up{reason}`: `none`, `seeded_empty`,
    /// `restored`, `snapshot` or `unknown`.
    pub fn state_label(&self, now: Instant) -> &'static str {
        STATES[self.state_slot(now)]
    }

    /// The index of [`Self::state_label`] in [`STATES`].
    pub fn state_slot(&self, now: Instant) -> usize {
        if !self.is_set() {
            0
        } else if self.unknown(now) {
            STATES.len() - 1
        } else {
            self.reason().map_or(0, |reason| 1 + reason as usize)
        }
    }

    /// Set the marker, unless one is already set. A stronger reason replaces a
    /// weaker one (`restored` over `snapshot` over `seeded_empty`), since it
    /// clears by fewer rules. **Written atomically** (temp file, fsync, rename,
    /// directory fsync) before it is believed.
    pub fn mark(&self, reason: CatchUpReason) -> std::io::Result<()> {
        self.mark_at(reason, Instant::now())
    }

    /// [`Self::mark`], judged at `now`.
    ///
    /// **It gates the member again on new evidence**, whichever state it reads
    /// (ADR-212): a stronger reason; a `restored` mark that names no peer (an
    /// unattributed proof of loss); or the first `snapshot` mark since the marker
    /// was set (a snapshot that has started is knowledge). Gating resets the wait
    /// and the owed latch and wakes what waits on the gate if it was open. The
    /// file is rewritten only for a stronger reason. Anything else does nothing.
    pub fn mark_at(&self, reason: CatchUpReason, now: Instant) -> std::io::Result<()> {
        self.mark_with(reason, None, now)
    }

    /// The replay against `peer` proved entries of this member's own origin
    /// lost, the highest of them at `lost_max`: mark the member for `reason`.
    ///
    /// New evidence is a stronger reason, or entries not already proven by that
    /// peer: `lost_max` above the highest stamp its replay has proven lost in
    /// this run. A peer's replay reads its pages in stamp order, so a proof at or
    /// below that is the same entries again (the replay re-reads them every round
    /// while a collection they name is missing), and gating on it would close
    /// every change stream every tick.
    pub fn mark_proven(
        &self,
        reason: CatchUpReason,
        peer: NodeId,
        lost_max: kimmy_core::Hlc,
        now: Instant,
    ) -> std::io::Result<()> {
        self.mark_with(reason, Some((peer, lost_max)), now)
    }

    fn mark_with(
        &self,
        reason: CatchUpReason,
        proof: Option<(NodeId, kimmy_core::Hlc)>,
        now: Instant,
    ) -> std::io::Result<()> {
        let state = self.state.lock();
        let held = state.marker.clone();
        let had_marker = held.is_some();
        let stronger = held.as_ref().is_none_or(|held| reason.strength() > held.reason.strength());
        let fresh_proof = proof.is_some_and(|(peer, lost_max)| {
            let mut replay = self.replay.lock();
            let fresh = replay.proven.get(&peer).is_none_or(|proven| lost_max > *proven);
            if fresh {
                replay.proven.insert(peer, lost_max);
            }
            fresh
        });
        let first_snapshot =
            reason == CatchUpReason::Snapshot && !self.snapshot_marked.swap(true, Relaxed);
        let evidence = stronger
            || fresh_proof
            || first_snapshot
            || (proof.is_none() && reason == CatchUpReason::Restored);
        if !evidence {
            return Ok(());
        }
        let was_open = self.unknown(now);
        if stronger {
            let marker = match held {
                Some(mut held) => {
                    held.reason = reason;
                    held
                }
                None => Marker::new(reason),
            };
            kimmy_storage::blocking(|| write_atomic(&self.path, &marker.render()))?;
            drop(state);
            info!(reason = reason.label(), regated = was_open, "the catching-up marker was set");
            // A held marker gated again; a new one relies on the clear that
            // removed the last one having dropped the latch.
            if had_marker {
                self.owed_open.store(false, Relaxed);
            }
            self.install(marker, now);
            return Ok(());
        }
        drop(state);
        // The same marker, gated again: the wait restarts, the latch drops, and
        // what waits on the gate is woken if it was open.
        self.owed_open.store(false, Relaxed);
        self.counting_ms.store(self.ms(now), Relaxed);
        if was_open {
            info!(reason = reason.label(), regated = true, "the catching-up marker was set");
            self.changes.send_modify(|n| *n += 1);
        }
        Ok(())
    }

    /// Remove the marker: `how` says which rule cleared it.
    pub fn clear(&self, how: &str) {
        let mut state = self.state.lock();
        if state.marker.take().is_none() {
            return;
        }
        state.reached.clear();
        state.warned = None;
        drop(state);
        kimmy_storage::blocking(|| {
            if let Err(e) = std::fs::remove_file(&self.path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!(error = %e, "could not remove the catching-up marker file");
                }
            } else if let Some(dir) = self.path.parent()
                && let Err(e) = std::fs::File::open(dir).and_then(|d| d.sync_all())
            {
                warn!(error = %e, "could not sync the data directory after removing the marker");
            }
        });
        self.set.store(false, Relaxed);
        self.reason.store(0, Relaxed);
        // By any rule, the operator's included: a later mark must not inherit an
        // open latch, nor a snapshot already counted.
        self.owed_open.store(false, Relaxed);
        self.snapshot_marked.store(false, Relaxed);
        info!(how, "the catching-up marker was cleared");
    }

    /// The operator's clear: deleting the file takes effect live. Called every
    /// tick, and by the gate's refresh, so it is not a start-time check.
    pub fn refresh(&self) {
        if self.is_set() && !kimmy_storage::blocking(|| self.path.exists()) {
            self.clear("the operator (the file was deleted)");
        }
    }

    /// Remove the marker file and forget the marker, when this member has nobody
    /// else to catch up from (clustering is off, or its seeds name only itself): a
    /// file left by a restore, or by a node that left a cluster, must not gate a
    /// standalone one for ever.
    pub fn discard_when_standalone(&self) {
        if self.is_set() {
            info!("this member has no peers, so the catching-up marker is discarded");
            self.clear("no peers");
        }
    }

    /// Arm the replay of this member's own origin for this run (ADR-202): a clustered
    /// member asks each peer it reaches, once, what that peer holds of this
    /// member's origin above the floor. Called right after the store is opened,
    /// before this run writes anything, with the own-origin position the store
    /// had.
    ///
    /// **The floor is persisted** (`kimmy.replay-floor`): the lower of what an
    /// earlier start left and `at_open`. A start that writes (a topology record,
    /// client writes) and stops before it reaches a peer would otherwise open the
    /// next time with a position past the entries it lost, and find nothing to
    /// read back. The file is removed only when every member that can answer has
    /// answered ([`Self::replay_settle`]).
    ///
    /// The floor is **clamped to what the peers can still serve**: no older than
    /// `now − retention`, since entries below the oplog horizon cannot be read
    /// back by any replay (the snapshot brings them). Without the clamp a floor a
    /// member held for longer than the retention would be older than every peer's
    /// horizon, be answered `BeyondHorizon`, and never see a newer loss.
    ///
    /// A floor file that cannot be read counts as the lowest floor, and a floor
    /// that cannot be written is an error, as the marker's is.
    pub fn arm_replay(
        &self,
        at_open: kimmy_core::Hlc,
        retention: Option<Duration>,
    ) -> std::io::Result<()> {
        self.arm_replay_at(at_open, retention, Instant::now())
    }

    /// [`Self::arm_replay`], armed at `now`.
    ///
    /// The members the last run's file names as owing (`owed =` lines, ADR-212)
    /// owe again, as absent from `now` and first seen at `now`: a member that was
    /// down while this one restarted is still waited on for the dwell, and holds
    /// a marked member's clear for the wait. A rewrite of the floor here carries
    /// those lines.
    pub fn arm_replay_at(
        &self,
        at_open: kimmy_core::Hlc,
        retention: Option<Duration>,
        now: Instant,
    ) -> std::io::Result<()> {
        let file = self.replay_floor_path();
        let read = kimmy_storage::blocking(|| read_replay_floor_file(&file));
        let existing = read.as_ref().map(|(floor, _)| *floor);
        let loaded: BTreeSet<NodeId> =
            read.map(|(_, owed)| owed.into_iter().collect()).unwrap_or_default();
        let lowest = existing.map_or(at_open, |held| held.min(at_open));
        let floor = lowest.max(oldest_readable(retention));
        if existing != Some(floor) {
            kimmy_storage::blocking(|| write_replay_floor_with(&file, floor, &loaded))?;
        }
        let mut replay = self.replay.lock();
        replay.armed = true;
        replay.floor = floor;
        replay.floor_unclamped = lowest;
        replay.floor_existed = existing.is_some();
        replay.retention = retention;
        replay.armed_at = Some(now);
        for id in &loaded {
            replay.first_seen.insert(*id, now);
            replay.owing.insert(*id, Owing { absent_since: Some(now), ..Owing::default() });
        }
        replay.recorded_owed = loaded;
        Ok(())
    }

    /// Whether the replay is armed and not yet settled.
    pub fn replay_armed(&self) -> bool {
        self.replay.lock().armed
    }

    /// The persisted floor, as clamped at arm time.
    pub fn replay_floor(&self) -> kimmy_core::Hlc {
        self.replay.lock().floor
    }

    /// Where the replay against `peer` goes on from: the floor, or further if an
    /// earlier round got further, and never older than the oplog the peers keep
    /// (which moves on as the run does: a peer that answered `BeyondHorizon` is
    /// asked from a later position next round).
    pub fn replay_from(&self, peer: NodeId) -> kimmy_core::Hlc {
        self.replay.lock().asks_from(peer)
    }

    /// `peer` answered the replay `BeyondHorizon` at `now`: it cannot tell what it
    /// holds of this member's origin from where the replay asks, so the replay is
    /// **not** answered and is asked again next round, **until the peer has
    /// answered it so for the whole wait**, when the replay gives up on it: it
    /// counts as answered (it cannot serve those entries, and asking again would
    /// never end). The clamp uses this member's retention and the answer depends
    /// on the peer's, so members that keep the oplog for different times reach
    /// this.
    pub fn replay_beyond_horizon(&self, peer: NodeId, now: Instant) -> Horizon {
        let mut replay = self.replay.lock();
        let since = *replay.horizon_since.entry(peer).or_insert(now);
        if now.saturating_duration_since(since) >= self.wait {
            replay.answered_by(peer);
            return Horizon::GaveUp;
        }
        if replay.horizon_reported.insert(peer) { Horizon::First } else { Horizon::Again }
    }

    /// The replay against `peer` has read and applied everything through `hlc`.
    pub fn replay_advanced(&self, peer: NodeId, hlc: kimmy_core::Hlc) {
        let mut replay = self.replay.lock();
        replay.horizon_since.remove(&peer);
        let cursor = replay.cursors.entry(peer).or_default();
        *cursor = (*cursor).max(hlc);
    }

    /// Whether `peer` has answered the replay this run.
    pub fn replay_answered(&self, peer: NodeId) -> bool {
        self.replay.lock().answered.contains(&peer)
    }

    /// `peer` has answered the replay: what it held of this member's origin has
    /// been read (and applied, if it was lost).
    pub fn replay_finished(&self, peer: NodeId) {
        self.replay.lock().answered_by(peer);
    }

    /// What a tick tells the replay (ADR-212), on every tick, before the replay is
    /// settled and the marker judged:
    ///
    /// - **the start-race guard**: the first contact of the run opens its window
    ///   (the wait), and within it the guard holds while an origin named in a
    ///   reached peer's vectors in this run has not been present in this run, or
    ///   while fewer members than `cluster.expected_members` are live (reached,
    ///   without membership), counting this one;
    /// - while the replay is armed, **the owed set**: a member present at this
    ///   tick (listed by SWIM, or reached) that has not answered owes it, and its
    ///   absence is reset; one absent counts its absence, which a move of its
    ///   address's SWIM generation since the last tick also resets; one absent for
    ///   the dwell without a break has left, and no longer owes. The floor file's
    ///   `owed =` lines are rewritten when the set's membership changes.
    pub fn replay_note_members(&self, tick: &ReplayTick<'_>) {
        let me = tick.me;
        let now = tick.now;
        let listed: BTreeMap<NodeId, std::net::SocketAddr> = tick
            .members
            .map(|members| members.entries().into_iter().map(|(addr, node)| (node, addr)).collect())
            .unwrap_or_default();
        let reached_now: BTreeSet<NodeId> =
            tick.reached.iter().map(|peer| peer.node).filter(|node| *node != me).collect();
        let mut present: BTreeSet<NodeId> =
            listed.keys().copied().filter(|node| *node != me).collect();
        present.extend(reached_now.iter().copied());
        // Members counted against `cluster.expected_members`, this one included.
        let counted = match tick.members {
            Some(_) => listed.keys().filter(|node| **node != me).count() + 1,
            None => {
                let mut lately = self.reached_within(now);
                lately.extend(reached_now.iter().copied());
                lately.remove(&me);
                lately.len() + 1
            }
        };
        let marked = self.is_set();
        let dwell = self.dwell();
        let mut replay = self.replay.lock();

        // The start-race guard.
        if !reached_now.is_empty() {
            replay.first_contact_at.get_or_insert(now);
        }
        for peer in tick.reached.iter().filter(|peer| peer.node != me) {
            replay.origins.extend(peer.servable.iter().map(|(node, _)| node));
            if let Some(witnessed) = &peer.witnessed {
                replay.origins.extend(witnessed.iter().map(|(node, _)| node));
            }
        }
        replay.origins.remove(&me);
        replay.present.extend(present.iter().copied());
        let in_window = replay
            .first_contact_at
            .is_some_and(|first| now.saturating_duration_since(first) < self.wait);
        let absent_origins: Vec<String> =
            replay.origins.difference(&replay.present).map(ToString::to_string).collect();
        let short_of = tick.expected_members.filter(|expected| counted < *expected);
        replay.guard_holds = in_window && (!absent_origins.is_empty() || short_of.is_some());
        if replay.guard_holds && marked && !replay.guard_warned {
            replay.guard_warned = true;
            warn!(
                absent_origins = ?absent_origins,
                expected_members = ?tick.expected_members,
                members = counted,
                window_secs = self.wait.as_secs(),
                "this member is catching up and holds its clear until the members its peers' \
                 vectors name have been seen, and cluster.expected_members are live, or until \
                 cluster.catch_up_wait_secs after its first contact: a member not yet listed \
                 may hold writes it lost"
            );
        }
        if !replay.armed {
            return;
        }

        // The owed set.
        for node in &present {
            replay.first_seen.entry(*node).or_insert(now);
            if replay.answered.contains(node) {
                continue;
            }
            let addr = listed.get(node).copied();
            let generation = addr.and_then(|addr| tick.members.and_then(|m| m.generation(&addr)));
            let owing = replay.owing.entry(*node).or_default();
            owing.absent_since = None;
            owing.listed = addr.is_some();
            if let Some(addr) = addr {
                owing.addr = Some(addr);
                owing.ever_listed = true;
                owing.generation = generation;
            }
        }
        let mut left = Vec::new();
        for (node, owing) in replay.owing.iter_mut().filter(|(node, _)| !present.contains(node)) {
            owing.listed = false;
            // SWIM brought it (back) up since the last tick, and it is down again:
            // it was not gone throughout.
            let generation =
                owing.addr.and_then(|addr| tick.members.and_then(|m| m.generation(&addr)));
            if generation != owing.generation {
                owing.generation = generation;
                owing.absent_since = Some(now);
                continue;
            }
            let since = *owing.absent_since.get_or_insert(now);
            let gone_for = now.saturating_duration_since(since);
            if gone_for >= dwell {
                left.push((*node, owing.clone(), gone_for));
            }
        }
        for (node, owing, gone_for) in left {
            replay.owing.remove(&node);
            warn!(
                peer = %node,
                address = %owing.addr.map(|addr| addr.to_string()).unwrap_or_default(),
                gone_for_secs = gone_for.as_secs(),
                dwell_secs = dwell.as_secs(),
                cause = if owing.ever_listed { "swim_down" } else { "unreached" },
                "a member that still owed the replay of this member's own origin has been gone \
                 from the cluster for longer than the dwell, so it is treated as having left and \
                 the replay against it is forfeited: writes this member lost that only it holds \
                 will not be read back"
            );
        }
        let answered = replay.answered.clone();
        replay.owing.retain(|node, _| !answered.contains(node));

        let past_hold: Vec<NodeId> = replay
            .owing
            .keys()
            .filter(|node| !replay.in_hold(node, self.wait, now))
            .copied()
            .collect();
        // Past the retention, the replay asks from the horizon.
        let horizon = oldest_readable(replay.retention);
        if !replay.retention_warned
            && replay.retention.is_some()
            && !past_hold.is_empty()
            && horizon > replay.floor_unclamped
        {
            replay.retention_warned = true;
            warn!(
                owed = ?replay.owed_ids(),
                floor = ?replay.floor_unclamped,
                horizon = ?horizon,
                retention_secs = replay.retention.map_or(0, |r| r.as_secs()),
                "the replay of this member's own origin can no longer read back writes older than \
                 storage.oplog_retention_secs: the members that still owe it will be asked only \
                 from the retention horizon, and writes this member lost before that are not \
                 recovered by the replay"
            );
        }
        // Not marked, and still owed past the hold: unprotected, and said so.
        if !marked
            && !past_hold.is_empty()
            && replay
                .unmarked_warned
                .is_none_or(|at| now.saturating_duration_since(at) >= WARN_EVERY)
        {
            replay.unmarked_warned = Some(now);
            warn!(
                owed = ?replay.owed_ids(),
                owed_addresses = ?replay.owed_addresses(),
                "the replay of this member's own origin is still owed by members the cluster \
                 lists live, past cluster.catch_up_wait_secs; this member is not marked, so it \
                 serves and does owner work from data that may lack writes they hold, and if \
                 one of them answers with writes this member lost it is marked restored again \
                 and refuses requests"
            );
        }

        // The floor file's `owed =` lines, when the set's membership changed.
        let owed: BTreeSet<NodeId> = replay.owing.keys().take(MAX_SEEN).copied().collect();
        if owed == replay.recorded_owed {
            return;
        }
        replay.recorded_owed = owed.clone();
        if replay.owing.len() > MAX_SEEN && !replay.owed_capped {
            replay.owed_capped = true;
            warn!(
                cap = MAX_SEEN,
                owing = replay.owing.len(),
                "more members owe the replay than the replay floor file records; the rest are \
                 kept in memory only, and a restart of this member forgets them"
            );
        }
        let floor = replay.floor;
        drop(replay);
        let file = self.replay_floor_path();
        let written = kimmy_storage::blocking(|| {
            // Never over a file the operator has removed: recreating it would
            // undo their forfeit of the replay at the next start.
            if !file.exists() {
                return Ok(());
            }
            write_replay_floor_with(&file, floor, &owed)
        });
        if let Err(e) = written {
            warn!(
                error = %e,
                "could not record the members that owe the replay in the replay floor file; \
                 they are kept in memory, and a restart of this member before the next \
                 successful write forgets them"
            );
        }
    }

    /// Settle the replay when nobody owes it, at least one member has answered,
    /// and the start-race guard does not hold: the persisted floor is removed
    /// and the replay disarmed. Until then the floor stays, and every newly
    /// reached peer is asked once, even after the marker has cleared: the lost
    /// entries sit below this member's own vector, so no ordinary pull fetches
    /// them.
    pub fn replay_settle(&self) {
        let file = self.replay_floor_path();
        {
            let mut replay = self.replay.lock();
            if !replay.armed
                || replay.answered.is_empty()
                || !replay.owing.is_empty()
                || replay.guard_holds
            {
                return;
            }
            replay.armed = false;
        }
        kimmy_storage::blocking(|| remove_replay_floor(&file));
        info!("every member that can answer has answered the replay of this member's own origin");
    }

    /// Forget the replay altogether, and its persisted floor: a member with
    /// clustering off has no peers to read anything back from.
    pub fn forget_replay(&self) {
        let file = self.replay_floor_path();
        {
            let mut replay = self.replay.lock();
            replay.armed = false;
            replay.owing.clear();
        }
        kimmy_storage::blocking(|| remove_replay_floor(&file));
    }

    fn replay_floor_path(&self) -> PathBuf {
        self.path.parent().unwrap_or_else(|| Path::new(".")).join(REPLAY_FLOOR_FILE)
    }

    /// The peers reached by a successful contact within the wait, as of `now`.
    /// What stands in for the live set of a member without membership: a cursor
    /// for a peer not heard from within the wait cannot gate this member for ever.
    pub fn reached_within(&self, now: Instant) -> BTreeSet<NodeId> {
        self.state
            .lock()
            .reached
            .iter()
            .filter(|(_, (at, _))| now.saturating_duration_since(*at) <= self.wait)
            .map(|(node, _)| *node)
            .collect()
    }

    /// The marker as it stands, for a test or a topology answer.
    pub fn marker(&self) -> Option<Marker> {
        self.state.lock().marker.clone()
    }

    /// Judge one tick (see the module documentation). Records the peers reached
    /// and the ids seen, resets the bound when a counting peer was reached,
    /// clears when a rule allows, and otherwise says at most every ten minutes,
    /// past the bound, that it is still set.
    pub fn evaluate(&self, tick: &Tick<'_>) -> Decision {
        if !self.is_set() {
            return Decision::NotSet;
        }
        // A contact with this member itself is no evidence about the cluster: a
        // seed that names it by an address it does not bind (a wildcard bind, a
        // loopback or a service address) dials it. The transport refuses such a
        // contact; this is the second line.
        if tick.reached.iter().any(|peer| peer.node == tick.me) {
            let others: Vec<Reached> =
                tick.reached.iter().filter(|peer| peer.node != tick.me).cloned().collect();
            return self.evaluate(&Tick { reached: &others, ..*tick });
        }
        self.track_unseen(tick);
        let mut state = self.state.lock();
        let Some(marker) = state.marker.clone() else { return Decision::NotSet };
        for peer in tick.reached {
            state.reached.insert(peer.node, (tick.now, peer.clone()));
        }
        // A peer not heard from for ten waits has left; its entry would only stay.
        let stale_after = self.wait.saturating_mul(10);
        state.reached.retain(|_, (at, _)| tick.now.saturating_duration_since(*at) <= stale_after);

        // Every id seen since the marker was set: members, contacts and the
        // origins named in any reached vector.
        let mut grown = marker.clone();
        let mut seen_now: BTreeSet<NodeId> = tick.reached.iter().map(|p| p.node).collect();
        if let Some(live) = tick.live {
            seen_now.extend(live.iter().copied());
        }
        for peer in tick.reached {
            seen_now.extend(peer.servable.iter().map(|(node, _)| node));
            if let Some(w) = &peer.witnessed {
                seen_now.extend(w.iter().map(|(node, _)| node));
            }
        }
        seen_now.remove(&tick.me);
        for id in seen_now {
            if grown.seen.contains(&id) {
                continue;
            }
            if grown.seen.len() >= MAX_SEEN {
                grown.overflowed = true;
                break;
            }
            grown.seen.insert(id);
        }
        if grown != marker {
            // Rewritten only when it grew, and never over a file the operator
            // has just removed: recreating it would undo their clear.
            if !kimmy_storage::blocking(|| self.path.exists()) {
                drop(state);
                self.clear("the operator (the file was deleted)");
                return Decision::Cleared("operator");
            }
            if let Err(e) = kimmy_storage::blocking(|| write_atomic(&self.path, &grown.render())) {
                warn!(error = %e, "could not record the members seen in the catching-up marker");
            }
            if grown.overflowed && !marker.overflowed {
                warn!(
                    cap = MAX_SEEN,
                    "more than the cap of members have been seen since the catching-up marker \
                     was set; the mutual clear is refused, and the dominance clear and the \
                     operator clear still work"
                );
            }
            state.marker = Some(grown.clone());
        }
        let marker = grown;

        // A counting peer: reached, not itself catching up (a peer that has sent
        // no block counts), and not one that has lost data (it holds less of its
        // own origin than this member holds of it).
        let counts = |peer: &Reached| {
            !peer.facts.as_deref().is_some_and(|f| f.catching_up)
                && tick.mine_witnessed.get(peer.node) <= peer.servable.get(peer.node)
        };
        // The latch drops when nothing is owed (ADR-212): the member is then
        // judged as any marked member is, and gated if no rule clears it.
        if self.owed_open.load(Relaxed) && self.replay.lock().owing.is_empty() {
            let was_open = self.unknown(tick.now);
            self.owed_open.store(false, Relaxed);
            if was_open && !self.unknown(tick.now) {
                self.changes.send_modify(|n| *n += 1);
            }
        }
        if self.owed_open.load(Relaxed) {
            let mut replay = self.replay.lock();
            replay.latched_owed = replay.owing.keys().copied().collect();
        }
        // Reaching one this tick restarts the wait, and wakes what waits on the
        // gate only if that closes it: under the latch it stays open.
        if tick.reached.iter().any(counts) {
            let was_open = self.unknown(tick.now);
            self.counting_ms.store(self.ms(tick.now), Relaxed);
            if was_open && !self.unknown(tick.now) {
                self.changes.send_modify(|n| *n += 1);
            }
        }
        // Every peer reached in the window, by what its last successful contact
        // read (this tick's, for the ones this tick reached). A peer reached
        // earlier and in health back-off now is still a peer this member must
        // not be behind.
        let window: Vec<&Reached> = state
            .reached
            .values()
            .filter(|(at, _)| tick.now.saturating_duration_since(*at) <= self.wait)
            .map(|(_, peer)| peer)
            .collect();

        let armed = self.replay_armed();
        // Nobody reached in the window can say anything.
        if armed && window.is_empty() {
            return Decision::Kept;
        }
        // The start-race guard (ADR-212): within its window, until the members the
        // peers' vectors name have been present and `expected_members` are live.
        if self.replay.lock().guard_holds {
            return Decision::Kept;
        }
        // The owed hold: a member that owes the replay (a peer reached in the
        // window that has not answered is one) holds the clear for the wait from
        // the later of the arming and its first being present.
        let exempt = self.exempt();
        let owed = armed && !exempt && !self.replay.lock().owing.is_empty();
        if owed {
            let replay = self.replay.lock();
            if replay.owing.keys().any(|node| replay.in_hold(node, self.wait, tick.now)) {
                return Decision::Kept;
            }
        }

        // Dominance: a contact this tick, and this member's witnessed vector
        // covers what every counting peer in the window serves, and no
        // whole-database snapshot is under way.
        let counting: Vec<&&Reached> = window.iter().filter(|peer| counts(peer)).collect();
        let dominance = tick.reached.iter().any(counts)
            && !tick.snapshot_pending
            && counting.iter().all(|peer| tick.mine_witnessed.covers(&peer.servable));
        // The mutual clear, for a `seeded_empty` member only.
        let mutual = !dominance
            && marker.reason == CatchUpReason::SeededEmpty
            && !marker.overflowed
            && !tick.snapshot_pending
            && mutual_clear_holds(&marker, tick, &window, &state.reached, self.wait);
        let rule = if dominance {
            Some(("dominance", "dominance"))
        } else if mutual {
            Some(("the mutual clear", "mutual"))
        } else {
            None
        };
        if let Some((how, name)) = rule {
            if !owed {
                drop(state);
                self.clear(how);
                return Decision::Cleared(name);
            }
            // A rule holds, and every member that owes is past its hold: serve as
            // `unknown`, owner work still off, until each has answered or left.
            // The list first, so a reader that sees the latch sees who owes.
            {
                let mut replay = self.replay.lock();
                replay.latched_owed = replay.owing.keys().copied().collect();
            }
            if !self.owed_open.swap(true, Relaxed) {
                state.warned = Some(tick.now);
                let replay = self.replay.lock();
                warn!(
                    reason = marker.reason.label(),
                    owed = ?replay.owed_ids(),
                    owed_addresses = ?replay.owed_addresses(),
                    held_for_secs = replay
                        .armed_at
                        .map_or(0, |at| tick.now.saturating_duration_since(at).as_secs()),
                    "this member is catching up and now serves as unknown: members the cluster \
                     lists live have not answered the replay of its own origin within \
                     cluster.catch_up_wait_secs, and writes it lost may be on them; it serves \
                     reads that may lack those writes, and expires, embeds and delivers nothing, \
                     until each has answered or has been gone from the cluster for the dwell. \
                     Restore this member's cluster connection to them (which recovers the writes \
                     only within storage.oplog_retention_secs of when they were made); deleting \
                     the catching-up marker turns owner work back on from data that may lack \
                     those writes"
                );
                return Decision::Kept;
            }
            // Already serving as `unknown (owed)`: said again every ten minutes,
            // below.
        }

        // Past the bound, or owed, still set: say so, and why.
        if self.unknown(tick.now)
            && state.warned.is_none_or(|at| tick.now.saturating_duration_since(at) >= WARN_EVERY)
        {
            state.warned = Some(tick.now);
            if self.owed_open.load(Relaxed) {
                let replay = self.replay.lock();
                warn!(
                    reason = marker.reason.label(),
                    owed = ?replay.owed_ids(),
                    owed_addresses = ?replay.owed_addresses(),
                    held_for_secs = replay
                        .armed_at
                        .map_or(0, |at| tick.now.saturating_duration_since(at).as_secs()),
                    "this member still serves as unknown: the replay of its own origin is still \
                     owed by members the cluster lists live; it serves reads that may lack writes \
                     they hold, and expires, embeds and delivers nothing, until each has answered \
                     or has been gone from the cluster for the dwell (restoring the link recovers \
                     the writes only within storage.oplog_retention_secs of when they were made)"
                );
                return Decision::Kept;
            }
            let unreached: Vec<String> = marker
                .seen
                .iter()
                .filter(|id| {
                    state
                        .reached
                        .get(id)
                        .is_none_or(|(at, _)| tick.now.saturating_duration_since(*at) > self.wait)
                })
                .map(ToString::to_string)
                .collect();
            warn!(
                reason = marker.reason.label(),
                since_ms = marker.since_ms,
                seen_cap_overflowed = marker.overflowed,
                seen_cap = MAX_SEEN,
                unreached = ?unreached,
                "this member has been catching up for longer than cluster.catch_up_wait_secs \
                 with no counting peer reached, so it serves but expires, embeds and delivers \
                 nothing; delete the catching-up marker file to bring it back when the state is \
                 known to be fine (if seen_cap_overflowed, more than seen_cap members were seen \
                 and the mutual clear is refused)"
            );
        }
        Decision::Kept
    }

    /// Members a tick shows that the replay has not tracked yet: SWIM's live ones
    /// and the peers reached, other than this member. The loop notes every tick
    /// first ([`Self::replay_note_members`]), so this finds nothing there; it keeps
    /// a caller that judges a tick without noting it from clearing past a member
    /// that owes.
    fn track_unseen(&self, tick: &Tick<'_>) {
        let mut replay = self.replay.lock();
        if !replay.armed {
            return;
        }
        let shown =
            tick.live.into_iter().flatten().copied().chain(tick.reached.iter().map(|p| p.node));
        for node in shown.filter(|node| *node != tick.me).collect::<Vec<_>>() {
            if replay.first_seen.contains_key(&node) {
                continue;
            }
            replay.first_seen.insert(node, tick.now);
            if !replay.answered.contains(&node) {
                replay.owing.insert(node, Owing::default());
            }
        }
    }
}

/// The mutual clear's four conditions (R4.1) and `cluster.expected_members`, over
/// every peer reached **in the window** by what its last successful contact read.
fn mutual_clear_holds(
    marker: &Marker,
    tick: &Tick<'_>,
    window: &[&Reached],
    reached: &BTreeMap<NodeId, (Instant, Reached)>,
    wait: Duration,
) -> bool {
    // A contact this tick, and someone to have heard from.
    if tick.reached.is_empty() || window.is_empty() {
        return false;
    }
    let in_window = |id: &NodeId| {
        reached.get(id).is_some_and(|(at, _)| tick.now.saturating_duration_since(*at) <= wait)
    };
    // 1. Every member it must hear from has been reached in the window, and one
    //    SWIM has only just marked down counts as unreached.
    let required_reached = marker
        .seen
        .iter()
        .all(|id| in_window(id) && tick.live.is_none_or(|live| live.contains(id)));
    if !required_reached {
        return false;
    }
    // 2. Every peer reached in the window is itself `seeded_empty`: one that is
    //    not catching up is for the dominance clear, one catching up for another
    //    reason is not this rule's.
    let all_seeded_empty = window.iter().all(|peer| {
        peer.facts.as_deref().is_some_and(|f| {
            f.catching_up && f.catching_up_reason == Some(CatchUpReason::SeededEmpty)
        })
    });
    if !all_seeded_empty {
        return false;
    }
    // 3. Every origin named in any reached vector belongs to a member reached in
    //    the window, or to this one. Held by (1): the marker records every origin
    //    a reached vector names among the ids it must hear from (`seen`), so an
    //    origin of an older cluster, or of a member that has left, is required to
    //    be reached and is not. Kept as a comment and not as a second check: a
    //    predicate that no input can reach alone is one no test can hold.
    // 4. Mutual coverage against each one's last known vectors, both from the same
    //    frame: what each holds is not missing from the other. A peer with no
    //    witnessed vector takes no part in this rule.
    let covered = window.iter().all(|peer| {
        peer.witnessed.as_ref().is_some_and(|theirs_witnessed| {
            tick.mine_witnessed.covers(&peer.servable)
                && theirs_witnessed.covers(tick.mine_servable)
        })
    });
    if !covered {
        return false;
    }
    // `cluster.expected_members`: members reached, plus this one, at least that.
    tick.expected_members.is_none_or(|expected| window.len() + 1 >= expected)
}

/// Write `text` to `path` atomically: a temporary file in the same directory,
/// fsynced, renamed over the target, and the directory fsynced.
///
/// A write that fails after the temporary file was created removes it, so a
/// refused rename leaves no `.tmp` beside the target.
pub fn write_atomic(path: &Path, text: &str) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = tmp_beside(path);
    let mut file = create_fresh(&tmp)?;
    let written = file
        .write_all(text.as_bytes())
        .and_then(|()| file.sync_all())
        .and_then(|()| std::fs::rename(&tmp, path));
    drop(file);
    if let Err(e) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    std::fs::File::open(dir)?.sync_all()
}

/// Create `tmp` as a new file, removing whatever stands there first. A stale
/// temporary file left by a write that never finished goes; a link planted at
/// its name goes too, removed and not followed, so the write can neither be
/// redirected to the link's target nor truncate it.
fn create_fresh(tmp: &Path) -> std::io::Result<std::fs::File> {
    match std::fs::remove_file(tmp) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    std::fs::OpenOptions::new().write(true).create_new(true).open(tmp)
}

/// The temporary file [`write_atomic`] writes `path` through.
fn tmp_beside(path: &Path) -> PathBuf {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or(FILE);
    dir.join(format!("{name}.tmp"))
}

/// Whether the replay floor can be written in `data_dir`, asked before the store
/// is opened: its path is absent or a regular file (judged on what a link points
/// at, which is followed), and the directory takes the temporary file the write
/// goes through. The floor's contents need the open's
/// position and are written right after it; this is what lets a path that can
/// never take them refuse the start before the open rather than after it.
pub fn check_replay_floor_writable(data_dir: &Path) -> std::io::Result<()> {
    let file = data_dir.join(REPLAY_FLOOR_FILE);
    match std::fs::metadata(&file) {
        Ok(found) if !found.is_file() => {
            return Err(std::io::Error::other(format!(
                "{} exists and is not a regular file",
                file.display()
            )));
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let tmp = tmp_beside(&file);
    drop(create_fresh(&tmp)?);
    std::fs::remove_file(&tmp)
}

/// The replay floor's file name, in the data directory.
pub const REPLAY_FLOOR_FILE: &str = "kimmy.replay-floor";

/// The floor in `path`: `None` when there is no file, the lowest floor when there
/// is one that cannot be read or understood (replaying from the start is the
/// conservative side).
fn read_replay_floor(path: &Path) -> Option<kimmy_core::Hlc> {
    read_replay_floor_file(path).map(|(floor, _)| floor)
}

/// The floor in `path` and the members its `owed =` lines name (ADR-212), at most
/// [`MAX_SEEN`] of them. Lines without `=` and unknown keys are skipped, as 0.43
/// skips the `owed` lines; an `owed` value that does not name a node is skipped
/// with one `WARN` for the file, and never voids the floor. A file that cannot be
/// read is the lowest floor, with no owed lines.
fn read_replay_floor_file(path: &Path) -> Option<(kimmy_core::Hlc, Vec<NodeId>)> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            warn!(error = %e, "the replay floor file cannot be read; the replay starts from the beginning");
            return Some((kimmy_core::Hlc::ZERO, Vec::new()));
        }
    };
    let (mut ms, mut counter) = (None, 0u16);
    let mut owed = BTreeSet::new();
    let mut unparsed = Vec::new();
    let mut dropped = BTreeSet::new();
    for (n, line) in text.lines().enumerate() {
        let Some((key, value)) = line.split_once('=') else { continue };
        match key.trim() {
            "floor_ms" => ms = value.trim().parse::<u64>().ok(),
            "floor_counter" => counter = value.trim().parse::<u16>().unwrap_or(0),
            "owed" => match value.trim().parse::<NodeId>() {
                Ok(id) if owed.len() < MAX_SEEN => {
                    owed.insert(id);
                }
                Ok(id) => {
                    if !owed.contains(&id) {
                        dropped.insert(id);
                    }
                }
                Err(_) => unparsed.push(n + 1),
            },
            _ => {}
        }
    }
    if !unparsed.is_empty() {
        warn!(
            lines = ?unparsed,
            "a line of the replay floor file says owed = but does not name a node; it is \
             skipped, and the floor is kept"
        );
    }
    if !dropped.is_empty() {
        warn!(
            cap = MAX_SEEN,
            dropped = dropped.len(),
            "the replay floor file names more owing members than the cap; the rest are not \
             loaded, so this member does not wait on them after the restart"
        );
    }
    // A file that cannot be read as a floor is the lowest one: replaying from the
    // start is the conservative side.
    let floor = kimmy_core::Hlc::new(ms.unwrap_or(0), if ms.is_some() { counter } else { 0 });
    Some((floor, owed.into_iter().collect()))
}

/// The oldest position a peer keeping the oplog for `retention` can still serve.
fn oldest_readable(retention: Option<Duration>) -> kimmy_core::Hlc {
    match retention {
        Some(retention) => kimmy_core::Hlc::new(
            kimmy_storage::physical_now_ms().saturating_sub(retention.as_millis() as u64),
            0,
        ),
        None => kimmy_core::Hlc::ZERO,
    }
}

fn write_replay_floor_file(path: &Path, floor: kimmy_core::Hlc) -> std::io::Result<()> {
    write_replay_floor_with(path, floor, &BTreeSet::new())
}

/// The floor file: the two floor lines first and always, so a file that names
/// owed members never lacks a floor (0.43 would read such a file as a floor of
/// zero), then one `owed = <node id>` line per member, sorted, at most
/// [`MAX_SEEN`].
fn write_replay_floor_with(
    path: &Path,
    floor: kimmy_core::Hlc,
    owed: &BTreeSet<NodeId>,
) -> std::io::Result<()> {
    let mut text = format!("floor_ms = {}\nfloor_counter = {}\n", floor.wall_ms, floor.counter);
    for id in owed.iter().take(MAX_SEEN) {
        text.push_str(&format!("owed = {id}\n"));
    }
    write_atomic(path, &text)
}

fn remove_replay_floor(path: &Path) {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!(error = %e, "could not remove the replay floor file");
    }
}

/// The replay floor on disk in `data_dir`, if there is one.
pub fn replay_floor_on_disk(data_dir: &Path) -> Option<kimmy_core::Hlc> {
    read_replay_floor(&data_dir.join(REPLAY_FLOOR_FILE))
}

/// Write the replay floor a restore leaves: the own-origin position of the
/// backup, so the first start reads back what the member made after it.
pub fn write_replay_floor(data_dir: &Path, floor: kimmy_core::Hlc) -> std::io::Result<()> {
    write_replay_floor_file(&data_dir.join(REPLAY_FLOOR_FILE), floor)
}

/// Write the marker before the store exists (R2.4): the reason, and the time.
pub fn write_marker(data_dir: &Path, reason: CatchUpReason) -> std::io::Result<()> {
    write_atomic(&data_dir.join(FILE), &Marker::new(reason).render())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(n: u8) -> NodeId {
        NodeId::from_bytes([n; 16])
    }

    fn vector(entries: &[(u8, u64)]) -> VersionVector {
        let mut v = VersionVector::new();
        for (n, at) in entries {
            v.insert(node(*n), kimmy_core::Hlc::new(*at, 0));
        }
        v
    }

    fn block(reason: Option<CatchUpReason>) -> Option<Arc<Facts>> {
        Some(Arc::new(Facts {
            boot: vec![1; 16],
            catching_up: reason.is_some(),
            catching_up_reason: reason,
            ..Facts::default()
        }))
    }

    fn addr(n: u8) -> std::net::SocketAddr {
        std::net::SocketAddr::from(([127, 0, 0, 1], 7000 + u16::from(n)))
    }

    /// A member set listing `ids`, each at its own address.
    fn members_of(ids: &[u8]) -> Members {
        let members = Members::default();
        for n in ids {
            members.insert_for_test(addr(*n), node(*n));
        }
        members
    }

    /// A peer reached with nothing in its vectors.
    fn bare(n: u8) -> Reached {
        reached(n, VersionVector::new(), None, None)
    }

    /// Tell the replay what a tick saw: SWIM's `live` members and the peers
    /// `reached` (with nothing in their vectors), at `now`, for member 9.
    fn note(catchup: &CatchUp, live: &[u8], reached_ids: &[u8], now: Instant) {
        let members = members_of(live);
        let reached: Vec<Reached> = reached_ids.iter().map(|n| bare(*n)).collect();
        catchup.replay_note_members(&ReplayTick {
            me: node(9),
            members: Some(&members),
            reached: &reached,
            expected_members: None,
            now,
        });
    }

    fn reached(
        n: u8,
        servable: VersionVector,
        witnessed: Option<VersionVector>,
        facts: Option<Arc<Facts>>,
    ) -> Reached {
        Reached { node: node(n), servable, witnessed, facts }
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        catchup: Arc<CatchUp>,
    }

    fn marked(reason: CatchUpReason) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        write_marker(dir.path(), reason).unwrap();
        let catchup = CatchUp::open(dir.path(), Duration::from_secs(120));
        Fixture { _dir: dir, catchup }
    }

    fn tick<'a>(
        reached: &'a [Reached],
        mine: &'a VersionVector,
        live: Option<&'a BTreeSet<NodeId>>,
        now: Instant,
    ) -> Tick<'a> {
        Tick {
            me: node(9),
            reached,
            mine_witnessed: mine,
            mine_servable: mine,
            snapshot_pending: false,
            live,
            expected_members: None,
            now,
        }
    }

    #[test]
    fn a_marker_file_round_trips_and_an_unreadable_one_is_still_a_marker() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!CatchUp::open(dir.path(), Duration::from_secs(1)).is_set());
        write_marker(dir.path(), CatchUpReason::Restored).unwrap();
        let held = CatchUp::open(dir.path(), Duration::from_secs(1));
        assert_eq!(held.reason(), Some(CatchUpReason::Restored));
        // A file this build cannot make sense of gates on the conservative side.
        std::fs::write(dir.path().join(FILE), "garbage\n\u{1}").unwrap();
        let odd = CatchUp::open(dir.path(), Duration::from_secs(1));
        assert!(odd.is_set());
    }

    #[test]
    fn the_operator_clears_it_by_deleting_the_file_live() {
        let f = marked(CatchUpReason::SeededEmpty);
        assert!(f.catchup.is_set());
        f.catchup.refresh();
        assert!(f.catchup.is_set(), "the file is there");
        std::fs::remove_file(f._dir.path().join(FILE)).unwrap();
        f.catchup.refresh();
        assert!(!f.catchup.is_set(), "deleting it takes effect without a restart");
    }

    #[test]
    fn a_wiped_member_clears_by_dominance_only_when_it_covers_a_counting_peer() {
        let f = marked(CatchUpReason::SeededEmpty);
        let now = Instant::now();
        let peer = reached(1, vector(&[(1, 100)]), Some(vector(&[(1, 100)])), block(None));
        let behind = vector(&[(1, 50)]);
        let one = [peer.clone()];
        assert_eq!(f.catchup.evaluate(&tick(&one, &behind, None, now)), Decision::Kept);
        assert!(f.catchup.is_set(), "behind a counting peer");
        let caught_up = vector(&[(1, 100)]);
        assert_eq!(
            f.catchup.evaluate(&tick(&one, &caught_up, None, now)),
            Decision::Cleared("dominance")
        );
        assert!(!f.catchup.is_set());
        assert!(!f._dir.path().join(FILE).exists(), "and the file is gone");
    }

    #[test]
    fn a_snapshot_still_under_way_holds_the_marker() {
        let f = marked(CatchUpReason::Restored);
        let now = Instant::now();
        let mine = vector(&[(1, 100)]);
        let one = [reached(1, vector(&[(1, 100)]), None, block(None))];
        let mut t = tick(&one, &mine, None, now);
        t.snapshot_pending = true;
        assert_eq!(f.catchup.evaluate(&t), Decision::Kept);
        t.snapshot_pending = false;
        assert_eq!(f.catchup.evaluate(&t), Decision::Cleared("dominance"));
    }

    #[test]
    fn a_peer_that_is_itself_catching_up_or_lost_its_data_does_not_count() {
        let f = marked(CatchUpReason::Restored);
        let now = Instant::now();
        let mine = vector(&[(1, 100)]);
        // Catching up itself: its vector is not a statement of what the cluster holds.
        let gated =
            [reached(1, vector(&[(1, 100)]), None, block(Some(CatchUpReason::SeededEmpty)))];
        assert_eq!(f.catchup.evaluate(&tick(&gated, &mine, None, now)), Decision::Kept);
        // A peer that holds less of its own origin than this member holds of it.
        let lost = [reached(1, vector(&[(1, 10)]), None, block(None))];
        assert_eq!(f.catchup.evaluate(&tick(&lost, &mine, None, now)), Decision::Kept);
    }

    #[test]
    fn a_peer_that_sends_no_block_counts() {
        let f = marked(CatchUpReason::SeededEmpty);
        let mine = vector(&[(1, 100)]);
        let old = [reached(1, vector(&[(1, 100)]), None, None)];
        assert_eq!(
            f.catchup.evaluate(&tick(&old, &mine, None, Instant::now())),
            Decision::Cleared("dominance")
        );
    }

    /// Three fresh members, each `seeded_empty`, with only their own startup
    /// writes: each clears once it has reached the other two.
    #[test]
    fn an_all_fresh_cluster_clears_by_the_mutual_clear() {
        let f = marked(CatchUpReason::SeededEmpty);
        let now = Instant::now();
        let live: BTreeSet<NodeId> = [node(1), node(2)].into();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        // Each has written its own registration only.
        let mine = vector(&[(9, 5)]);
        let both = [
            reached(1, vector(&[(1, 5)]), Some(vector(&[(1, 5), (9, 5)])), seeded.clone()),
            reached(2, vector(&[(2, 5)]), Some(vector(&[(2, 5), (9, 5)])), seeded.clone()),
        ];
        // Mutual coverage needs each side to hold the other's: ours does not yet.
        assert_eq!(f.catchup.evaluate(&tick(&both, &mine, Some(&live), now)), Decision::Kept);
        // Once they have converged on each other's registrations, each holds all three.
        let converged = vector(&[(1, 5), (2, 5), (9, 5)]);
        let both = [
            reached(1, vector(&[(1, 5)]), Some(converged.clone()), seeded.clone()),
            reached(2, vector(&[(2, 5)]), Some(converged.clone()), seeded.clone()),
        ];
        assert_eq!(
            f.catchup.evaluate(&tick(&both, &converged, Some(&live), now)),
            Decision::Cleared("mutual")
        );
    }

    #[test]
    fn a_member_seen_but_not_reached_holds_the_mutual_clear() {
        let f = marked(CatchUpReason::SeededEmpty);
        let now = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let converged = vector(&[(1, 5), (9, 5)]);
        // Node 2 is live and was seen, but only node 1 was reached.
        let live: BTreeSet<NodeId> = [node(1), node(2)].into();
        let only_one = [reached(1, vector(&[(1, 5)]), Some(vector(&[(1, 5), (9, 5)])), seeded)];
        assert_eq!(
            f.catchup.evaluate(&tick(&only_one, &converged, Some(&live), now)),
            Decision::Kept
        );
        assert!(f.catchup.marker().unwrap().seen.contains(&node(2)), "seen, and required");
    }

    #[test]
    fn an_origin_of_an_older_cluster_blocks_the_mutual_clear() {
        let f = marked(CatchUpReason::SeededEmpty);
        let now = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let live: BTreeSet<NodeId> = [node(1)].into();
        // The peer's vector names an origin (7) that no reached member owns.
        let mine = vector(&[(1, 5), (7, 3), (9, 5)]);
        let only = [reached(
            1,
            vector(&[(1, 5), (7, 3)]),
            Some(vector(&[(1, 5), (7, 3), (9, 5)])),
            seeded,
        )];
        assert_eq!(f.catchup.evaluate(&tick(&only, &mine, Some(&live), now)), Decision::Kept);
    }

    #[test]
    fn a_restored_member_never_takes_the_mutual_clear() {
        let f = marked(CatchUpReason::Restored);
        let now = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let live: BTreeSet<NodeId> = [node(1)].into();
        let mine = vector(&[(1, 5), (9, 5)]);
        let only = [reached(1, vector(&[(1, 5)]), Some(vector(&[(1, 5), (9, 5)])), seeded)];
        assert_eq!(f.catchup.evaluate(&tick(&only, &mine, Some(&live), now)), Decision::Kept);
    }

    #[test]
    fn a_reached_peer_that_is_not_seeded_empty_leaves_the_mutual_clear_to_dominance() {
        let f = marked(CatchUpReason::SeededEmpty);
        let now = Instant::now();
        let live: BTreeSet<NodeId> = [node(1), node(2)].into();
        let mine = vector(&[(1, 5), (2, 5), (9, 5)]);
        // One peer is catching up for another reason.
        let both = [
            reached(
                1,
                vector(&[(1, 5)]),
                Some(mine.clone()),
                block(Some(CatchUpReason::SeededEmpty)),
            ),
            reached(2, vector(&[(2, 5)]), Some(mine.clone()), block(Some(CatchUpReason::Restored))),
        ];
        assert_eq!(f.catchup.evaluate(&tick(&both, &mine, Some(&live), now)), Decision::Kept);
    }

    #[test]
    fn expected_members_holds_the_mutual_clear_back_until_enough_are_reached() {
        let now = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let live: BTreeSet<NodeId> = [node(1)].into();
        let mine = vector(&[(1, 5), (9, 5)]);
        let only = [reached(1, vector(&[(1, 5)]), Some(mine.clone()), seeded.clone())];
        // Unset: two members clear against each other, as documented.
        let f = marked(CatchUpReason::SeededEmpty);
        assert_eq!(
            f.catchup.evaluate(&tick(&only, &mine, Some(&live), now)),
            Decision::Cleared("mutual")
        );
        // Set to three: reached plus this member is two.
        let g = marked(CatchUpReason::SeededEmpty);
        let mut t = tick(&only, &mine, Some(&live), now);
        t.expected_members = Some(3);
        assert_eq!(g.catchup.evaluate(&t), Decision::Kept);
        t.expected_members = Some(2);
        assert_eq!(g.catchup.evaluate(&t), Decision::Cleared("mutual"));
    }

    /// With no counting peer reached for the bound the gate opens and the state
    /// reads `unknown`; reaching a counting peer that is ahead closes it again.
    #[test]
    fn the_gate_opens_after_the_bound_and_closes_again_on_reaching_a_peer_that_is_ahead() {
        let dir = tempfile::tempdir().unwrap();
        write_marker(dir.path(), CatchUpReason::SeededEmpty).unwrap();
        let catchup = CatchUp::open(dir.path(), Duration::from_millis(60));
        let now = Instant::now();
        catchup.first_tick(now);
        assert!(catchup.gated(now));
        assert_eq!(catchup.state_label(now), "seeded_empty");
        std::thread::sleep(Duration::from_millis(120));
        let later = Instant::now();
        assert!(!catchup.gated(later) && catchup.unknown(later));
        assert_eq!(catchup.state_label(later), "unknown");
        // A counting peer that is ahead: not cleared, and the bound restarts.
        let behind = vector(&[(1, 1)]);
        let ahead = [reached(1, vector(&[(1, 100)]), None, block(None))];
        assert_eq!(catchup.evaluate(&tick(&ahead, &behind, None, later)), Decision::Kept);
        // Judged at the tick's own instant: a wall-clock read here would flake on a
        // host loaded enough to take the bound between the two lines.
        assert!(catchup.gated(later), "gated again");
    }

    #[test]
    fn the_seen_set_is_capped_and_a_capped_marker_refuses_the_mutual_clear() {
        let f = marked(CatchUpReason::SeededEmpty);
        let now = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        // More origins than the cap, all named in one peer's vector.
        let mut big = VersionVector::new();
        for i in 0..(MAX_SEEN as u64 + 5) {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&(i + 1).to_be_bytes());
            big.insert(NodeId::from_bytes(bytes), kimmy_core::Hlc::new(1, 0));
        }
        let live: BTreeSet<NodeId> = [node(1)].into();
        let peer = [reached(1, big.clone(), Some(big.clone()), seeded)];
        assert_eq!(f.catchup.evaluate(&tick(&peer, &big, Some(&live), now)), Decision::Kept);
        let marker = f.catchup.marker().unwrap();
        assert!(marker.overflowed && marker.seen.len() <= MAX_SEEN);
        // Read back from the file it is still capped and still overflowed.
        let again = CatchUp::open(f._dir.path(), Duration::from_secs(1)).marker().unwrap();
        assert!(again.overflowed);
    }

    #[test]
    fn the_file_is_rewritten_only_when_the_seen_set_grows() {
        let f = marked(CatchUpReason::SeededEmpty);
        let now = Instant::now();
        let mine = vector(&[(9, 5)]);
        let peer = [reached(1, vector(&[(1, 5)]), None, block(None))];
        f.catchup.evaluate(&tick(&peer, &mine, None, now));
        let path = f._dir.path().join(FILE);
        let first = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(30));
        f.catchup.evaluate(&tick(&peer, &mine, None, now));
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            first,
            "no growth, no write"
        );
    }

    #[test]
    fn a_standalone_node_discards_a_stale_marker() {
        let f = marked(CatchUpReason::SeededEmpty);
        f.catchup.discard_when_standalone();
        assert!(!f.catchup.is_set());
        assert!(!f._dir.path().join(FILE).exists());
    }

    #[test]
    fn dominance_needs_every_counting_peer_covered() {
        let now = Instant::now();
        let one = reached(1, vector(&[(1, 100)]), None, block(None));
        let two = reached(2, vector(&[(2, 100)]), None, block(None));
        let both = [one, two];
        // Covers the first peer and not the second.
        let f = marked(CatchUpReason::Restored);
        let mine = vector(&[(1, 100)]);
        assert_eq!(f.catchup.evaluate(&tick(&both, &mine, None, now)), Decision::Kept);
        let g = marked(CatchUpReason::Restored);
        let mine = vector(&[(1, 100), (2, 100)]);
        assert_eq!(
            g.catchup.evaluate(&tick(&both, &mine, None, now)),
            Decision::Cleared("dominance")
        );
    }

    /// A marker whose seen set overflowed refuses the mutual clear even when every
    /// other condition holds, and the same tick clears once it has not overflowed.
    #[test]
    fn an_overflowed_marker_refuses_the_mutual_clear_when_all_else_holds() {
        let now = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let live: BTreeSet<NodeId> = [node(1)].into();
        let mine = vector(&[(1, 5), (9, 5)]);
        let only = [reached(1, vector(&[(1, 5)]), Some(mine.clone()), seeded)];

        for overflowed in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let mut marker = Marker::new(CatchUpReason::SeededEmpty);
            marker.overflowed = overflowed;
            write_atomic(&dir.path().join(FILE), &marker.render()).unwrap();
            let catchup = CatchUp::open(dir.path(), Duration::from_secs(120));
            let decision = catchup.evaluate(&tick(&only, &mine, Some(&live), now));
            let expected = if overflowed { Decision::Kept } else { Decision::Cleared("mutual") };
            assert_eq!(decision, expected, "overflowed: {overflowed}");
        }
    }

    /// A peer SWIM has marked down counts as unreached, however recently a contact
    /// reached it.
    #[test]
    fn a_peer_swim_has_marked_down_is_not_reached() {
        let now = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let mine = vector(&[(1, 5), (9, 5)]);
        let only = [reached(1, vector(&[(1, 5)]), Some(mine.clone()), seeded)];
        let down: BTreeSet<NodeId> = BTreeSet::new();
        let up: BTreeSet<NodeId> = [node(1)].into();
        let f = marked(CatchUpReason::SeededEmpty);
        assert_eq!(f.catchup.evaluate(&tick(&only, &mine, Some(&down), now)), Decision::Kept);
        let g = marked(CatchUpReason::SeededEmpty);
        assert_eq!(
            g.catchup.evaluate(&tick(&only, &mine, Some(&up), now)),
            Decision::Cleared("mutual")
        );
    }

    /// Coverage runs both ways: what this member serves must be held by the peer as
    /// well as the other way round.
    #[test]
    fn the_mutual_clear_needs_the_peer_to_cover_what_this_member_serves() {
        let now = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let live: BTreeSet<NodeId> = [node(1)].into();
        let mine = vector(&[(1, 5), (9, 5)]);
        // The peer's witnessed vector lacks this member's origin: it has not read
        // what this member holds.
        let lacking = [reached(1, vector(&[(1, 5)]), Some(vector(&[(1, 5)])), seeded.clone())];
        let f = marked(CatchUpReason::SeededEmpty);
        assert_eq!(f.catchup.evaluate(&tick(&lacking, &mine, Some(&live), now)), Decision::Kept);
        let covering = [reached(1, vector(&[(1, 5)]), Some(vector(&[(1, 5), (9, 5)])), seeded)];
        let g = marked(CatchUpReason::SeededEmpty);
        assert_eq!(
            g.catchup.evaluate(&tick(&covering, &mine, Some(&live), now)),
            Decision::Cleared("mutual")
        );
    }

    /// A stronger reason replaces a weaker one and never the other way round, and
    /// the file says which.
    #[test]
    fn a_stronger_reason_replaces_a_weaker_one_and_never_the_other_way() {
        use CatchUpReason::{Restored, SeededEmpty, Snapshot};
        let dir = tempfile::tempdir().unwrap();
        let catchup = CatchUp::open(dir.path(), Duration::from_secs(120));
        catchup.mark(SeededEmpty).unwrap();
        assert_eq!(catchup.reason(), Some(SeededEmpty));
        catchup.mark(Snapshot).unwrap();
        assert_eq!(catchup.reason(), Some(Snapshot), "snapshot outranks seeded_empty");
        catchup.mark(Restored).unwrap();
        assert_eq!(catchup.reason(), Some(Restored), "restored outranks snapshot");
        catchup.mark(Snapshot).unwrap();
        catchup.mark(SeededEmpty).unwrap();
        assert_eq!(catchup.reason(), Some(Restored), "a weaker reason changes nothing");
        let file = std::fs::read_to_string(dir.path().join(FILE)).unwrap();
        assert!(file.contains("reason = restored"), "{file}");
    }

    /// The mutual clear judges every peer reached in the window, not the ones this
    /// tick reached: a populated peer reached a moment ago and not now (in health
    /// back-off) still stands between this member and a clear.
    #[test]
    fn the_mutual_clear_judges_a_populated_peer_reached_earlier_in_the_window() {
        let f = marked(CatchUpReason::SeededEmpty);
        let start = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let live: BTreeSet<NodeId> = [node(1), node(2)].into();
        // Peer 1 is populated and not catching up; peer 2 is as fresh as this member.
        let populated = reached(1, vector(&[(1, 1_000)]), Some(vector(&[(1, 1_000)])), block(None));
        let fresh = |witnessed: &[(u8, u64)]| {
            reached(2, vector(&[(2, 5)]), Some(vector(witnessed)), seeded.clone())
        };
        let mine = vector(&[(9, 5)]);
        let both = [populated, fresh(&[(2, 5), (9, 5)])];
        assert_eq!(f.catchup.evaluate(&tick(&both, &mine, Some(&live), start)), Decision::Kept);

        // A second later only the fresh peer is reached, and it and this member hold
        // each other's writes. Peer 1's writes are still missing from this member.
        let converged = vector(&[(2, 5), (9, 5)]);
        let only_fresh = [fresh(&[(2, 5), (9, 5)])];
        let later = start + Duration::from_secs(1);
        assert_eq!(
            f.catchup.evaluate(&tick(&only_fresh, &converged, Some(&live), later)),
            Decision::Kept,
            "a peer reached in the window and not this tick still counts"
        );
        assert!(f.catchup.is_set());
    }

    /// The same for dominance: an ahead peer reached a moment ago and an empty one
    /// reached now do not clear a member that is behind the first.
    #[test]
    fn dominance_judges_an_ahead_peer_reached_earlier_in_the_window() {
        let f = marked(CatchUpReason::Restored);
        let start = Instant::now();
        let ahead = reached(1, vector(&[(1, 1_000)]), None, block(None));
        let empty = reached(2, vector(&[]), None, block(None));
        let mine = vector(&[(9, 5)]);
        let both = [ahead, empty.clone()];
        assert_eq!(f.catchup.evaluate(&tick(&both, &mine, None, start)), Decision::Kept);
        let only_empty = [empty];
        let later = start + Duration::from_secs(1);
        assert_eq!(
            f.catchup.evaluate(&tick(&only_empty, &mine, None, later)),
            Decision::Kept,
            "the ahead peer, reached in the window, is still ahead"
        );
        // Out of the window, it no longer stands in the way: the wait is 120 s.
        let much_later = start + Duration::from_secs(400);
        assert_eq!(
            f.catchup.evaluate(&tick(&only_empty, &mine, None, much_later)),
            Decision::Cleared("dominance")
        );
    }

    /// A snapshot under way holds the mutual clear as it holds the dominance clear.
    #[test]
    fn a_snapshot_under_way_holds_the_mutual_clear() {
        let now = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let live: BTreeSet<NodeId> = [node(1)].into();
        let mine = vector(&[(1, 5), (9, 5)]);
        let only = [reached(1, vector(&[(1, 5)]), Some(mine.clone()), seeded)];
        let f = marked(CatchUpReason::SeededEmpty);
        let mut t = tick(&only, &mine, Some(&live), now);
        t.snapshot_pending = true;
        assert_eq!(f.catchup.evaluate(&t), Decision::Kept);
        t.snapshot_pending = false;
        assert_eq!(f.catchup.evaluate(&t), Decision::Cleared("mutual"));
    }

    /// A contact with this member itself proves nothing: it is filtered out, so a
    /// member that dialled itself is a member that reached nobody.
    #[test]
    fn a_contact_with_this_member_itself_is_no_contact() {
        let f = marked(CatchUpReason::SeededEmpty);
        let now = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let mine = vector(&[(9, 5)]);
        let live = BTreeSet::new();
        let itself = [reached(9, mine.clone(), Some(mine.clone()), seeded)];
        assert_eq!(f.catchup.evaluate(&tick(&itself, &mine, Some(&live), now)), Decision::Kept);
        assert!(f.catchup.is_set());
        // And it does not count towards `expected_members`, or restart the wait.
        assert!(f.catchup.reached_within(now).is_empty());
    }

    /// The operator's deletion of the file is not undone by the write that records
    /// members seen: the file stays gone and the marker is cleared.
    #[test]
    fn an_operators_clear_is_not_undone_by_a_write_of_the_seen_set() {
        let f = marked(CatchUpReason::SeededEmpty);
        let path = f._dir.path().join(FILE);
        std::fs::remove_file(&path).unwrap();
        let now = Instant::now();
        let mine = vector(&[(9, 5)]);
        // A tick that names a member this marker has not seen: the seen set grows.
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let one = [reached(1, vector(&[(1, 5)]), None, seeded)];
        f.catchup.evaluate(&tick(&one, &mine, None, now));
        assert!(!path.exists(), "the file the operator removed was recreated");
        assert!(!f.catchup.is_set());
    }

    /// Each condition of the mutual clear is judged against every peer reached in
    /// the window, on its own: a peer reached a moment ago that is not
    /// `seeded_empty` (though it holds nothing this member lacks and lacks nothing
    /// this member holds) holds the clear back.
    #[test]
    fn a_peer_reached_earlier_that_is_not_seeded_empty_holds_the_mutual_clear() {
        let f = marked(CatchUpReason::SeededEmpty);
        let start = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let live: BTreeSet<NodeId> = [node(1), node(2)].into();
        let mine = vector(&[(9, 5)]);
        // Peer 1 is not catching up, and its vectors are exactly this member's.
        let ordinary = reached(1, vector(&[(9, 5)]), Some(vector(&[(9, 5)])), block(None));
        let fresh = reached(2, vector(&[(9, 5)]), Some(vector(&[(9, 5)])), seeded);
        let both = [ordinary, fresh.clone()];
        // The first tick only records the two (a snapshot is under way, which holds
        // both clears); the second reaches only the fresh one.
        let mut first = tick(&both, &mine, Some(&live), start);
        first.snapshot_pending = true;
        assert_eq!(f.catchup.evaluate(&first), Decision::Kept);
        let later = start + Duration::from_secs(1);
        let only = [fresh];
        assert_eq!(
            f.catchup.evaluate(&tick(&only, &mine, Some(&live), later)),
            Decision::Kept,
            "peer 1, reached in the window, is not seeded_empty"
        );
    }

    /// The same for mutual coverage: a `seeded_empty` peer reached a moment ago
    /// whose writes this member lacks holds the clear back when only another peer
    /// is reached now.
    #[test]
    fn a_peer_reached_earlier_whose_writes_are_missing_holds_the_mutual_clear() {
        let f = marked(CatchUpReason::SeededEmpty);
        let start = Instant::now();
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let live: BTreeSet<NodeId> = [node(1), node(2)].into();
        let mine = vector(&[(2, 5), (9, 5)]);
        // Peer 1 holds a write of its own that this member has not read.
        let ahead =
            reached(1, vector(&[(1, 7)]), Some(vector(&[(1, 7), (2, 5), (9, 5)])), seeded.clone());
        let fresh = reached(2, vector(&[(2, 5)]), Some(vector(&[(2, 5), (9, 5)])), seeded);
        let both = [ahead, fresh.clone()];
        assert_eq!(f.catchup.evaluate(&tick(&both, &mine, Some(&live), start)), Decision::Kept);
        let later = start + Duration::from_secs(1);
        let only = [fresh];
        assert_eq!(
            f.catchup.evaluate(&tick(&only, &mine, Some(&live), later)),
            Decision::Kept,
            "peer 1's write is missing from this member"
        );
    }

    /// While the replay of the member's own origin is owed by a peer reached in the
    /// window, no rule clears the marker, whatever the tick shows: it may yet find
    /// writes this member lost. It clears once every peer reached in the window has
    /// answered, and not on the strength of the first to.
    #[test]
    fn nothing_clears_until_every_peer_reached_has_answered_the_replay() {
        let now = Instant::now();
        let one = [reached(1, vector(&[(1, 100)]), None, block(None))];
        let mine = vector(&[(1, 100)]);
        let f = marked(CatchUpReason::Restored);
        f.catchup.arm_replay(kimmy_core::Hlc::new(50, 0), None).unwrap();
        assert!(f.catchup.replay_armed());
        assert_eq!(f.catchup.evaluate(&tick(&one, &mine, None, now)), Decision::Kept);
        assert!(f.catchup.is_set());
        f.catchup.replay_finished(node(1));
        assert_eq!(
            f.catchup.evaluate(&tick(&one, &mine, None, now)),
            Decision::Cleared("dominance")
        );

        // A second peer reached in the window that has not answered holds it, though
        // the first has.
        let two = [
            reached(1, vector(&[(1, 100)]), None, block(None)),
            reached(2, vector(&[(2, 100)]), None, block(None)),
        ];
        let mine = vector(&[(1, 100), (2, 100)]);
        let g = marked(CatchUpReason::Restored);
        g.catchup.arm_replay(kimmy_core::Hlc::new(50, 0), None).unwrap();
        g.catchup.replay_finished(node(1));
        assert_eq!(g.catchup.evaluate(&tick(&two, &mine, None, now)), Decision::Kept);
        g.catchup.replay_finished(node(2));
        assert_eq!(
            g.catchup.evaluate(&tick(&two, &mine, None, now)),
            Decision::Cleared("dominance")
        );

        // The mutual clear waits for it too.
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let live: BTreeSet<NodeId> = [node(1)].into();
        let mine = vector(&[(1, 5), (9, 5)]);
        let both = [reached(1, vector(&[(1, 5)]), Some(mine.clone()), seeded)];
        let h = marked(CatchUpReason::SeededEmpty);
        h.catchup.arm_replay(kimmy_core::Hlc::new(50, 0), None).unwrap();
        assert_eq!(h.catchup.evaluate(&tick(&both, &mine, Some(&live), now)), Decision::Kept);
        h.catchup.replay_finished(node(1));
        assert_eq!(
            h.catchup.evaluate(&tick(&both, &mine, Some(&live), now)),
            Decision::Cleared("mutual")
        );
    }

    /// The replay against each peer goes on from where it got, and never from behind
    /// the floor; one peer's progress says nothing about another's.
    #[test]
    fn the_replay_goes_on_from_where_it_got_peer_by_peer() {
        let f = marked(CatchUpReason::Restored);
        let floor = kimmy_core::Hlc::new(50, 0);
        f.catchup.arm_replay(floor, None).unwrap();
        assert_eq!(f.catchup.replay_from(node(1)), floor);
        f.catchup.replay_advanced(node(1), kimmy_core::Hlc::new(80, 0));
        assert_eq!(f.catchup.replay_from(node(1)), kimmy_core::Hlc::new(80, 0));
        f.catchup.replay_advanced(node(1), kimmy_core::Hlc::new(60, 0));
        assert_eq!(f.catchup.replay_from(node(1)), kimmy_core::Hlc::new(80, 0), "never back");
        assert_eq!(f.catchup.replay_from(node(2)), floor, "another peer starts at the floor");
    }

    /// The floor is the lowest position any start since the last complete replay
    /// saw at open, and it is on disk: a start that wrote before it reached a peer
    /// does not lose the entries between.
    #[test]
    fn the_replay_floor_is_the_lowest_position_seen_and_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(REPLAY_FLOOR_FILE);
        let first = CatchUp::open(dir.path(), Duration::from_secs(120));
        first.arm_replay(kimmy_core::Hlc::new(50, 3), None).unwrap();
        assert_eq!(first.replay_floor(), kimmy_core::Hlc::new(50, 3));
        assert!(path.exists(), "persisted");
        // The next start opens with the position past what the first run wrote.
        let second = CatchUp::open(dir.path(), Duration::from_secs(120));
        second.arm_replay(kimmy_core::Hlc::new(80, 0), None).unwrap();
        assert_eq!(second.replay_floor(), kimmy_core::Hlc::new(50, 3), "the floor does not rise");
        // A start with a lower position lowers it.
        let third = CatchUp::open(dir.path(), Duration::from_secs(120));
        third.arm_replay(kimmy_core::Hlc::new(20, 0), None).unwrap();
        assert_eq!(third.replay_floor(), kimmy_core::Hlc::new(20, 0));
        assert_eq!(read_replay_floor(&path), Some(kimmy_core::Hlc::new(20, 0)));
    }

    /// The floor is removed when every member that can answer has answered or has
    /// left (ADR-212), and not before: not on the first answer, not while a live
    /// member has not answered, and not while a member that has not answered has
    /// been gone for less than the dwell. Nothing names an absent origin, so the
    /// start-race guard is not what holds it.
    #[test]
    fn the_replay_floor_is_removed_only_when_every_member_that_can_answer_has_answered_or_left() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(REPLAY_FLOOR_FILE);
        let catchup = CatchUp::open(dir.path(), Duration::from_secs(120));
        let t0 = Instant::now();
        catchup.arm_replay_at(kimmy_core::Hlc::new(50, 0), None, t0).unwrap();
        let dwell = catchup.dwell();

        catchup.replay_settle();
        assert!(path.exists() && catchup.replay_armed(), "nobody has answered");
        note(&catchup, &[1, 2, 9], &[1], t0);
        catchup.replay_finished(node(1));
        catchup.replay_settle();
        assert!(path.exists() && catchup.replay_armed(), "member 2 has not answered");

        // Member 2 leaves the live set: it is waited on for the dwell.
        let t1 = t0 + Duration::from_secs(5);
        for at in [t1, t1 + dwell - Duration::from_secs(1)] {
            note(&catchup, &[1, 9], &[], at);
            catchup.replay_settle();
            assert!(path.exists() && catchup.replay_armed(), "member 2 has not been gone long");
        }
        note(&catchup, &[1, 9], &[], t1 + dwell);
        catchup.replay_settle();
        assert!(!path.exists() && !catchup.replay_armed(), "member 2 has left");
    }

    /// A peer reached in the run that has not answered holds the floor after it
    /// has left SWIM's set, for the dwell (ADR-212): it may still hold what was
    /// lost. Past the dwell it has left, and the floor goes.
    #[test]
    fn a_peer_reached_and_never_answered_holds_the_floor_through_the_dwell() {
        let dir = tempfile::tempdir().unwrap();
        let catchup = CatchUp::open(dir.path(), Duration::from_secs(120));
        let t0 = Instant::now();
        catchup.arm_replay_at(kimmy_core::Hlc::new(50, 0), None, t0).unwrap();
        let dwell = catchup.dwell();
        note(&catchup, &[1, 2], &[1, 2], t0);
        catchup.replay_finished(node(1));
        let t1 = t0 + Duration::from_secs(5);
        for at in [t1, t1 + dwell - Duration::from_secs(1)] {
            note(&catchup, &[1], &[], at);
            catchup.replay_settle();
            assert!(catchup.replay_armed(), "peer 2 was reached and has not answered");
        }
        note(&catchup, &[1], &[], t1 + dwell);
        catchup.replay_settle();
        assert!(!catchup.replay_armed(), "gone for the dwell, it has left");
    }

    /// The floor is no older than what a peer keeping the oplog for the retention
    /// can serve, and the clamp is what is persisted; with no retention it is left.
    #[test]
    fn the_replay_floor_is_clamped_to_what_the_peers_still_hold() {
        let dir = tempfile::tempdir().unwrap();
        write_replay_floor(dir.path(), kimmy_core::Hlc::new(1_000, 0)).unwrap();
        let hour = Duration::from_secs(3600);
        let now = kimmy_storage::physical_now_ms();
        let catchup = CatchUp::open(dir.path(), Duration::from_secs(120));
        catchup.arm_replay(kimmy_core::Hlc::new(2_000, 0), Some(hour)).unwrap();
        let floor = catchup.replay_floor();
        assert!(floor.wall_ms + 3_600_000 >= now, "clamped up to the retention: {floor:?}");
        assert!(floor.wall_ms + 3_600_000 <= now + 10_000, "and no further: {floor:?}");
        assert_eq!(read_replay_floor(&dir.path().join(REPLAY_FLOOR_FILE)), Some(floor));
        assert!(catchup.replay_from(node(1)) >= floor);

        let bare = tempfile::tempdir().unwrap();
        write_replay_floor(bare.path(), kimmy_core::Hlc::new(1_000, 0)).unwrap();
        let unclamped = CatchUp::open(bare.path(), Duration::from_secs(120));
        unclamped.arm_replay(kimmy_core::Hlc::new(2_000, 0), None).unwrap();
        assert_eq!(unclamped.replay_floor(), kimmy_core::Hlc::new(1_000, 0));
    }

    /// The position the replay asks from moves on with the retention as the run
    /// does: a peer that answered `BeyondHorizon` is asked from a later position
    /// next round, not from the same one.
    #[test]
    fn the_replay_asks_from_a_position_that_moves_on_with_the_retention() {
        let dir = tempfile::tempdir().unwrap();
        let catchup = CatchUp::open(dir.path(), Duration::from_secs(120));
        let retention = Duration::from_millis(200);
        catchup.arm_replay(kimmy_core::Hlc::ZERO, Some(retention)).unwrap();
        let first = catchup.replay_from(node(1));
        std::thread::sleep(Duration::from_millis(300));
        let later = catchup.replay_from(node(1));
        assert!(later.wall_ms >= first.wall_ms + 250, "{first:?} then {later:?}");
        assert!(catchup.replay_floor() <= first, "the persisted floor does not move");
    }

    /// A floor file that cannot be read is the lowest floor, not no floor; a floor
    /// that cannot be written fails the arm.
    #[test]
    fn an_unreadable_floor_is_the_lowest_and_an_unwritable_one_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        // A directory where the file should be: reading it fails, but not as a
        // missing file does.
        let path = dir.path().join(REPLAY_FLOOR_FILE);
        std::fs::create_dir(&path).unwrap();
        assert_eq!(read_replay_floor(&path), Some(kimmy_core::Hlc::ZERO));
        let catchup = CatchUp::open(dir.path(), Duration::from_secs(120));
        catchup.arm_replay(kimmy_core::Hlc::new(50, 0), None).unwrap();
        assert_eq!(catchup.replay_floor(), kimmy_core::Hlc::ZERO);

        assert_eq!(read_replay_floor(&dir.path().join("absent")), None);
        let missing =
            CatchUp::open(&dir.path().join("no-such-directory"), Duration::from_secs(120));
        assert!(missing.arm_replay(kimmy_core::Hlc::new(50, 0), None).is_err());
        assert!(!missing.replay_armed(), "a floor that could not be written arms nothing");
    }

    /// A member SWIM lists live that has not answered the replay holds the clear,
    /// though every peer reached has: it may be the one that holds what was lost.
    #[test]
    fn a_live_member_that_has_not_answered_holds_the_clear() {
        let now = Instant::now();
        let one = [reached(1, vector(&[(1, 100)]), None, block(None))];
        let mine = vector(&[(1, 100)]);
        let live: BTreeSet<NodeId> = [node(1), node(2), node(9)].into();
        let f = marked(CatchUpReason::Restored);
        f.catchup.arm_replay(kimmy_core::Hlc::new(50, 0), None).unwrap();
        f.catchup.replay_finished(node(1));
        assert_eq!(
            f.catchup.evaluate(&tick(&one, &mine, Some(&live), now)),
            Decision::Kept,
            "member 2 is live and has not answered"
        );
        f.catchup.replay_finished(node(2));
        assert_eq!(
            f.catchup.evaluate(&tick(&one, &mine, Some(&live), now)),
            Decision::Cleared("dominance"),
            "this member is not one it waits on"
        );
    }

    /// A `BeyondHorizon` answer is reported once per peer and is not an answer,
    /// until the peer has answered it so for the whole wait: then the replay gives
    /// up on it once, and it counts as settled. An answer in between starts the
    /// count again.
    #[test]
    fn a_horizon_answer_is_reported_once_and_ends_after_the_wait() {
        let f = marked(CatchUpReason::Restored);
        let t0 = Instant::now();
        let wait = Duration::from_secs(120);
        f.catchup.arm_replay(kimmy_core::Hlc::new(50, 0), None).unwrap();
        assert_eq!(f.catchup.replay_beyond_horizon(node(1), t0), Horizon::First);
        assert_eq!(f.catchup.replay_beyond_horizon(node(1), t0 + wait / 2), Horizon::Again);
        assert!(!f.catchup.replay_answered(node(1)));
        assert_eq!(f.catchup.replay_beyond_horizon(node(2), t0), Horizon::First);
        assert_eq!(f.catchup.replay_beyond_horizon(node(1), t0 + wait), Horizon::GaveUp);
        assert!(f.catchup.replay_answered(node(1)), "given up on: it counts as settled");
        assert!(!f.catchup.replay_answered(node(2)));

        // An answer between two horizons starts the count again.
        f.catchup.replay_advanced(node(2), kimmy_core::Hlc::new(60, 0));
        assert_eq!(f.catchup.replay_beyond_horizon(node(2), t0 + wait), Horizon::Again);
        assert_eq!(
            f.catchup.replay_beyond_horizon(node(2), t0 + wait * 2 - Duration::from_secs(1)),
            Horizon::Again
        );
        assert!(!f.catchup.replay_answered(node(2)));
    }

    /// A peer reached that answers a horizon for ever holds the clear only for the
    /// wait: once the replay gives up on it, the clear is judged as it is for any
    /// other peer.
    #[test]
    fn a_peer_the_replay_has_given_up_on_no_longer_holds_the_clear() {
        let f = marked(CatchUpReason::Restored);
        let t0 = Instant::now();
        let wait = Duration::from_secs(120);
        let one = [reached(1, vector(&[(1, 100)]), None, block(None))];
        let mine = vector(&[(1, 100)]);
        f.catchup.arm_replay(kimmy_core::Hlc::new(50, 0), None).unwrap();
        f.catchup.replay_beyond_horizon(node(1), t0);
        assert_eq!(f.catchup.evaluate(&tick(&one, &mine, None, t0)), Decision::Kept);
        f.catchup.replay_beyond_horizon(node(1), t0 + wait);
        assert_eq!(
            f.catchup.evaluate(&tick(&one, &mine, None, t0 + wait)),
            Decision::Cleared("dominance")
        );
    }

    /// A member serving as `unknown` (no counting peer for the wait) that is then
    /// shown a loss of its own writes refuses requests again, and wakes what waits
    /// on the gate: a proof is knowledge, whatever reason the marker held.
    #[test]
    fn a_proven_loss_while_unknown_gates_again() {
        let dir = tempfile::tempdir().unwrap();
        write_marker(dir.path(), CatchUpReason::Restored).unwrap();
        let catchup = CatchUp::open(dir.path(), Duration::from_millis(1));
        catchup.first_tick(Instant::now());
        std::thread::sleep(Duration::from_millis(5));
        assert!(catchup.unknown(Instant::now()), "premise: the wait has run out");
        let mut changes = catchup.subscribe();
        changes.mark_unchanged();
        // Judged at an instant taken before the mark: the mark restarts the wait
        // from a later one, so a stall here cannot open the gate again.
        let at = Instant::now();
        catchup.mark(CatchUpReason::Restored).unwrap();
        assert!(catchup.gated(at), "a proven loss gates again");
        assert!(changes.has_changed().unwrap(), "and wakes what waits on the gate");
    }

    #[test]
    fn a_replay_floor_path_that_cannot_take_a_file_is_refused_before_the_open() {
        let dir = tempfile::tempdir().unwrap();
        check_replay_floor_writable(dir.path()).expect("an absent floor can be written");
        assert!(!dir.path().join("kimmy.replay-floor.tmp").exists(), "the probe cleans up");
        write_replay_floor(dir.path(), kimmy_core::Hlc::new(1, 0)).unwrap();
        check_replay_floor_writable(dir.path()).expect("a regular file is replaced");

        let floor_is_a_directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(floor_is_a_directory.path().join(REPLAY_FLOOR_FILE)).unwrap();
        let refused = check_replay_floor_writable(floor_is_a_directory.path()).unwrap_err();
        assert!(refused.to_string().contains("not a regular file"), "{refused}");

        let tmp_is_a_directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp_is_a_directory.path().join("kimmy.replay-floor.tmp")).unwrap();
        assert!(check_replay_floor_writable(tmp_is_a_directory.path()).is_err());

        assert!(check_replay_floor_writable(&dir.path().join("absent")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_link_planted_at_the_temporary_name_is_replaced_not_followed() {
        let outside = tempfile::tempdir().unwrap();
        let target = outside.path().join("precious");
        std::fs::write(&target, "not the floor").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("kimmy.replay-floor.tmp");
        std::os::unix::fs::symlink(&target, &tmp).unwrap();

        check_replay_floor_writable(dir.path()).expect("the probe replaces the link");
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "not the floor");
        assert!(!tmp.exists() && tmp.symlink_metadata().is_err(), "the probe removes what it made");

        std::os::unix::fs::symlink(&target, &tmp).unwrap();
        write_replay_floor(dir.path(), kimmy_core::Hlc::new(7, 0)).expect("the write succeeds");
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "not the floor");
        assert_eq!(
            read_replay_floor(&dir.path().join(REPLAY_FLOOR_FILE)),
            Some(kimmy_core::Hlc::new(7, 0))
        );
        assert!(tmp.symlink_metadata().is_err(), "no temporary file is left");
    }

    #[test]
    fn a_write_whose_rename_fails_leaves_no_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        // A directory at the target: the temporary file is written, then the
        // rename over it is refused.
        let floor = dir.path().join(REPLAY_FLOOR_FILE);
        std::fs::create_dir(&floor).unwrap();
        assert!(write_replay_floor_file(&floor, kimmy_core::Hlc::new(1, 0)).is_err());
        assert!(!dir.path().join("kimmy.replay-floor.tmp").exists(), "the floor's temporary file");

        let marker = dir.path().join(FILE);
        std::fs::create_dir(&marker).unwrap();
        assert!(write_atomic(&marker, "reason = \"restored\"\n").is_err());
        assert!(!dir.path().join(format!("{FILE}.tmp")).exists(), "the marker's temporary file");
    }

    /// A member with clustering off forgets the replay and its floor.
    #[test]
    fn a_standalone_start_forgets_the_replay_floor() {
        let dir = tempfile::tempdir().unwrap();
        let catchup = CatchUp::open(dir.path(), Duration::from_secs(120));
        write_replay_floor(dir.path(), kimmy_core::Hlc::new(50, 0)).unwrap();
        catchup.forget_replay();
        assert!(!dir.path().join(REPLAY_FLOOR_FILE).exists());
        assert!(!catchup.replay_armed());
    }

    /// A peer reached earlier that is catching up for another reason (`restored`
    /// or `snapshot`) holds the mutual clear when only a `seeded_empty` peer is
    /// reached now: the rule is for a cluster of members that all started from
    /// nothing, and one that is restored or streaming a snapshot is not one.
    #[test]
    fn a_peer_reached_earlier_that_is_restored_or_snapshotting_holds_the_mutual_clear() {
        for other in [CatchUpReason::Restored, CatchUpReason::Snapshot] {
            let f = marked(CatchUpReason::SeededEmpty);
            let start = Instant::now();
            let seeded = block(Some(CatchUpReason::SeededEmpty));
            let live: BTreeSet<NodeId> = [node(1), node(2)].into();
            let mine = vector(&[(9, 5)]);
            let elsewhere =
                reached(1, vector(&[(9, 5)]), Some(vector(&[(9, 5)])), block(Some(other)));
            let fresh = reached(2, vector(&[(9, 5)]), Some(vector(&[(9, 5)])), seeded);
            let both = [elsewhere, fresh.clone()];
            // The first tick only records them (a snapshot is under way).
            let mut first = tick(&both, &mine, Some(&live), start);
            first.snapshot_pending = true;
            assert_eq!(f.catchup.evaluate(&first), Decision::Kept);
            let later = start + Duration::from_secs(1);
            let only = [fresh];
            assert_eq!(
                f.catchup.evaluate(&tick(&only, &mine, Some(&live), later)),
                Decision::Kept,
                "peer 1 is catching up for {other:?}, and was reached in the window"
            );
        }
    }

    // -----------------------------------------------------------------------
    // ADR-212: the owed set, the latch, the guard, the dwell
    // -----------------------------------------------------------------------

    const WAIT: Duration = Duration::from_secs(120);

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    /// Peer 1 (B): not catching up, with nothing of other origins in its vectors.
    fn peer_b() -> Reached {
        reached(1, vector(&[(1, 100)]), None, block(None))
    }

    /// This member's vectors: it covers B.
    fn covering() -> VersionVector {
        vector(&[(1, 100), (9, 200)])
    }

    /// One tick as the loop judges it (`peers::judge_catch_up_at`): note the
    /// members, settle, evaluate, with SWIM listing `live`.
    fn judge(
        catchup: &CatchUp,
        live: &Members,
        reached: &[Reached],
        mine: &VersionVector,
        now: Instant,
    ) -> Decision {
        judge_expecting(catchup, live, reached, mine, None, now)
    }

    fn judge_expecting(
        catchup: &CatchUp,
        live: &Members,
        reached: &[Reached],
        mine: &VersionVector,
        expected_members: Option<usize>,
        now: Instant,
    ) -> Decision {
        catchup.replay_note_members(&ReplayTick {
            me: node(9),
            members: Some(live),
            reached,
            expected_members,
            now,
        });
        if catchup.replay_armed() {
            catchup.replay_settle();
        }
        catchup.refresh();
        let ids = live.node_ids();
        let mut t = tick(reached, mine, Some(&ids), now);
        t.expected_members = expected_members;
        catchup.evaluate(&t)
    }

    /// The same, with membership off.
    fn judge_without_membership(
        catchup: &CatchUp,
        reached: &[Reached],
        mine: &VersionVector,
        expected_members: Option<usize>,
        now: Instant,
    ) -> Decision {
        catchup.replay_note_members(&ReplayTick {
            me: node(9),
            members: None,
            reached,
            expected_members,
            now,
        });
        if catchup.replay_armed() {
            catchup.replay_settle();
        }
        catchup.refresh();
        let mut t = tick(reached, mine, None, now);
        t.expected_members = expected_members;
        catchup.evaluate(&t)
    }

    /// A member marked `reason` by a file an earlier run left, with the wait
    /// `wait`, its replay armed at `t0` and B having answered it.
    fn armed_with(reason: CatchUpReason, wait: Duration) -> (Fixture, Instant) {
        let dir = tempfile::tempdir().unwrap();
        write_marker(dir.path(), reason).unwrap();
        let catchup = CatchUp::open(dir.path(), wait);
        let t0 = Instant::now();
        catchup.arm_replay_at(kimmy_core::Hlc::new(50, 0), None, t0).unwrap();
        catchup.replay_finished(node(1));
        (Fixture { _dir: dir, catchup }, t0)
    }

    fn restored_armed() -> (Fixture, Instant) {
        armed_with(CatchUpReason::Restored, WAIT)
    }

    fn floor_on_disk(f: &Fixture) -> bool {
        f._dir.path().join(REPLAY_FLOOR_FILE).exists()
    }

    /// Drive a restored member into `unknown (owed)`: B reached and answered, C
    /// (node 2) listed live from `t0 + 5 s` and never answering, judged at
    /// `t0 + 5 s` and `t0 + 130 s`.
    fn latched() -> (Fixture, Instant) {
        let (f, t0) = restored_armed();
        let live = members_of(&[1, 2]);
        let b = [peer_b()];
        assert_eq!(judge(&f.catchup, &live, &b, &covering(), t0 + secs(5)), Decision::Kept);
        assert_eq!(judge(&f.catchup, &live, &b, &covering(), t0 + secs(130)), Decision::Kept);
        assert_eq!(
            f.catchup.unknown_because(t0 + secs(130)),
            Some(UnknownCause::OwedReplay),
            "premise: C owes the replay past its hold"
        );
        (f, t0)
    }

    /// Every event a test's subscriber saw: its level, its message, and its other
    /// fields by name.
    type Event = (tracing::Level, String, BTreeMap<String, String>);

    #[derive(Clone, Default)]
    struct Events(Arc<std::sync::Mutex<Vec<Event>>>);

    impl Events {
        fn matching(&self, text: &str) -> Vec<Event> {
            self.0.lock().unwrap().iter().filter(|(_, m, _)| m.contains(text)).cloned().collect()
        }
    }

    impl tracing::Subscriber for Events {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
            tracing::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            #[derive(Default)]
            struct Fields(String, BTreeMap<String, String>);
            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    } else {
                        self.1.insert(field.name().to_string(), format!("{value:?}"));
                    }
                }
            }
            let mut fields = Fields::default();
            event.record(&mut fields);
            self.0.lock().unwrap().push((*event.metadata().level(), fields.0, fields.1));
        }
        fn enter(&self, _: &tracing::Id) {}
        fn exit(&self, _: &tracing::Id) {}
    }

    /// A member SWIM lists live that never answers the replay holds the
    /// clear for the wait from the later of the arming and its first being seen,
    /// and past that the member serves as `unknown`, owned work still off, until
    /// it answers or has left; the floor is kept.
    #[test]
    fn a_live_member_that_never_answers_holds_the_clear_for_the_wait_and_then_serves_as_unknown() {
        let (f, t0) = restored_armed();
        let b = [peer_b()];
        let live_two = members_of(&[1, 2]);
        for at in [0, 100, 119] {
            let now = t0 + secs(at);
            assert_eq!(judge(&f.catchup, &live_two, &b, &covering(), now), Decision::Kept);
            assert!(f.catchup.gated(now), "{at} s: member 2 is live and has not answered");
        }
        // A member first seen live later holds it for the wait from then.
        let live_three = members_of(&[1, 2, 3]);
        assert_eq!(judge(&f.catchup, &live_three, &b, &covering(), t0 + secs(130)), Decision::Kept);
        assert!(f.catchup.gated(t0 + secs(130)));
        assert_eq!(judge(&f.catchup, &live_three, &b, &covering(), t0 + secs(249)), Decision::Kept);
        assert!(f.catchup.gated(t0 + secs(249)), "member 3 holds it from when it was first seen");

        let past = t0 + WAIT * 2 + secs(11);
        assert_eq!(judge(&f.catchup, &live_three, &b, &covering(), past), Decision::Kept);
        assert!(f.catchup.unknown(past) && !f.catchup.gated(past), "it serves as unknown");
        assert_eq!(f.catchup.unknown_because(past), Some(UnknownCause::OwedReplay));
        assert_eq!(f.catchup.owed_members(), [node(2), node(3)]);
        assert!(f.catchup.is_set(), "owner work stays off");
        assert!(floor_on_disk(&f), "the floor is kept");
        assert!(!f.catchup.replay_answered(node(2)), "and member 2 is still asked when reached");
    }

    /// One tick on which SWIM has an owing member down, inside
    /// its hold or past it, neither settles the replay nor clears: the floor stays,
    /// and once it is back the transport asks it. The guard passes here (no vector
    /// names an absent origin, `expected_members` unset), so it is the owed set
    /// that holds, and not the guard.
    #[test]
    fn one_swim_down_tick_of_an_owing_member_neither_settles_the_replay_nor_clears() {
        let b = [peer_b()];
        for (down_at, inside) in [(10, true), (135, false)] {
            let (f, t0) = restored_armed();
            let members = members_of(&[1, 2]);
            assert_eq!(judge(&f.catchup, &members, &b, &covering(), t0 + secs(5)), Decision::Kept);
            if !inside {
                judge(&f.catchup, &members, &b, &covering(), t0 + secs(130));
                assert_eq!(
                    f.catchup.unknown_because(t0 + secs(130)),
                    Some(UnknownCause::OwedReplay)
                );
            }
            let down = t0 + secs(down_at);
            members.remove_for_test(&addr(2));
            assert_eq!(
                judge(&f.catchup, &members, &b, &covering(), down),
                Decision::Kept,
                "inside the hold: {inside}"
            );
            assert!(floor_on_disk(&f), "inside the hold: {inside}: the floor is kept");
            assert!(f.catchup.replay_armed(), "inside the hold: {inside}");
            assert!(f.catchup.is_set(), "inside the hold: {inside}");
            assert_eq!(f.catchup.gated(down), inside, "gated inside the hold, owed past it");

            let back = down + secs(5);
            members.insert_for_test(addr(2), node(2));
            assert_eq!(judge(&f.catchup, &members, &b, &covering(), back), Decision::Kept);
            assert!(
                f.catchup.replay_armed() && !f.catchup.replay_answered(node(2)),
                "inside the hold: {inside}: the transport asks C when it is back"
            );
        }
    }

    /// An owing member absent for less than the dwell still owes; at the
    /// dwell it has left: it is released, the floor goes (B answered), and the
    /// covering tick clears.
    #[test]
    fn an_owing_member_gone_for_the_dwell_has_left() {
        let (f, t0) = restored_armed();
        let dwell = f.catchup.dwell();
        assert_eq!(dwell, DWELL_FLOOR, "the wait is shorter than the floor");
        let b = [peer_b()];
        let members = members_of(&[1, 2]);
        judge(&f.catchup, &members, &b, &covering(), t0 + secs(5));
        let gone = t0 + secs(10);
        members.remove_for_test(&addr(2));
        for at in [gone, gone + dwell - secs(1)] {
            assert_eq!(judge(&f.catchup, &members, &b, &covering(), at), Decision::Kept);
            assert_eq!(f.catchup.owed_members(), [node(2)], "still owing");
            assert!(floor_on_disk(&f));
        }
        assert_eq!(
            judge(&f.catchup, &members, &b, &covering(), gone + dwell),
            Decision::Cleared("dominance"),
            "released at the dwell, and the covering tick clears"
        );
        assert!(f.catchup.owed_members().is_empty());
        assert!(!floor_on_disk(&f) && !f.catchup.replay_armed(), "the floor goes");
    }

    /// The dwell counts continuous absence: a return restarts it, though
    /// the absences add up to more than the dwell.
    #[test]
    fn a_return_inside_the_dwell_restarts_it() {
        let (f, t0) = restored_armed();
        let b = [peer_b()];
        let members = members_of(&[1, 2]);
        judge(&f.catchup, &members, &b, &covering(), t0 + secs(5));
        let gone = t0 + secs(10);
        members.remove_for_test(&addr(2));
        judge(&f.catchup, &members, &b, &covering(), gone);
        judge(&f.catchup, &members, &b, &covering(), gone + secs(400));
        members.insert_for_test(addr(2), node(2));
        judge(&f.catchup, &members, &b, &covering(), gone + secs(401));
        members.remove_for_test(&addr(2));
        judge(&f.catchup, &members, &b, &covering(), gone + secs(402));
        let later = gone + secs(402) + f.catchup.dwell() - secs(1);
        assert_eq!(judge(&f.catchup, &members, &b, &covering(), later), Decision::Kept);
        assert_eq!(f.catchup.owed_members(), [node(2)], "absent 400 s, back, absent 599 s");
        assert!(floor_on_disk(&f));
    }

    /// A suspect member is still in SWIM's set, so it is present: it owes
    /// for as long as it is listed, however long that is.
    #[test]
    fn a_suspect_member_is_present() {
        let (f, t0) = restored_armed();
        let b = [peer_b()];
        let listed = members_of(&[1, 2]);
        for at in [5, 300, 700, 1_500] {
            assert_eq!(judge(&f.catchup, &listed, &b, &covering(), t0 + secs(at)), Decision::Kept);
        }
        assert_eq!(f.catchup.owed_members(), [node(2)]);
        assert!(floor_on_disk(&f));
    }

    /// A peer reached by a contact that SWIM has not listed owes until it
    /// has been unreached for the dwell; with membership off, presence is being
    /// reached at that tick.
    #[test]
    fn a_reached_member_that_swim_has_not_listed_owes_and_is_released_after_the_dwell_unreached() {
        for membership in [true, false] {
            let (f, t0) = restored_armed();
            let dwell = f.catchup.dwell();
            let listed = members_of(&[1]);
            let with_three = [peer_b(), reached(3, vector(&[(3, 10)]), None, block(None))];
            let b = [peer_b()];
            let mine = vector(&[(1, 100), (3, 10), (9, 200)]);
            let run = |reached: &[Reached], at: Instant| {
                if membership {
                    judge(&f.catchup, &listed, reached, &mine, at)
                } else {
                    judge_without_membership(&f.catchup, reached, &mine, None, at)
                }
            };
            assert_eq!(run(&with_three, t0 + secs(5)), Decision::Kept, "{membership}");
            let unreached = t0 + secs(10);
            assert_eq!(run(&b, unreached), Decision::Kept);
            assert_eq!(run(&b, unreached + dwell - secs(1)), Decision::Kept, "{membership}");
            assert_eq!(f.catchup.owed_members(), [node(3)], "{membership}");
            assert_eq!(
                run(&b, unreached + dwell),
                Decision::Cleared("dominance"),
                "membership {membership}: unreached for the dwell, it has left"
            );
        }
    }

    /// An owing member that answers lets the clear happen at once.
    #[test]
    fn an_owing_member_that_answers_lets_the_clear_happen() {
        let (f, t0) = restored_armed();
        let b = [peer_b()];
        let live = members_of(&[1, 2]);
        assert_eq!(judge(&f.catchup, &live, &b, &covering(), t0 + secs(5)), Decision::Kept);
        f.catchup.replay_finished(node(2));
        assert_eq!(
            judge(&f.catchup, &live, &b, &covering(), t0 + secs(10)),
            Decision::Cleared("dominance")
        );
    }

    /// A member the replay gave up on after `BeyondHorizon` for the whole
    /// wait counts as answered.
    #[test]
    fn a_member_given_up_on_after_beyond_horizon_counts_as_answered() {
        let (f, t0) = restored_armed();
        let live = members_of(&[1, 2]);
        let both = [peer_b(), reached(2, vector(&[(2, 10)]), None, block(None))];
        let mine = vector(&[(1, 100), (2, 10), (9, 200)]);
        assert_eq!(f.catchup.replay_beyond_horizon(node(2), t0), Horizon::First);
        assert_eq!(judge(&f.catchup, &live, &both, &mine, t0 + secs(5)), Decision::Kept);
        assert_eq!(f.catchup.replay_beyond_horizon(node(2), t0 + WAIT), Horizon::GaveUp);
        assert_eq!(
            judge(&f.catchup, &live, &both, &mine, t0 + WAIT + secs(1)),
            Decision::Cleared("dominance")
        );
    }

    /// While latched, a counting peer that is ahead neither closes the gate
    /// nor wakes what waits on it: the latch can hide only lag.
    #[test]
    fn a_counting_peer_ahead_does_not_close_an_owed_unknown_gate_nor_wake_streams() {
        let (f, t0) = latched();
        let mut changes = f.catchup.subscribe();
        changes.mark_unchanged();
        let ahead = [reached(1, vector(&[(1, 500)]), None, block(None))];
        let at = t0 + secs(135);
        assert_eq!(
            judge(&f.catchup, &members_of(&[1, 2]), &ahead, &covering(), at),
            Decision::Kept
        );
        assert!(!f.catchup.gated(at), "the latch holds the gate open");
        assert_eq!(f.catchup.unknown_because(at), Some(UnknownCause::OwedReplay));
        assert!(!changes.has_changed().unwrap(), "and no stream was woken");
    }

    /// A newly reached peer far ahead leaves the member `unknown (owed)`.
    #[test]
    fn a_newly_reached_peer_far_ahead_leaves_owed_unknown_and_readyz_says_owed() {
        let (f, t0) = latched();
        let far = [peer_b(), reached(3, vector(&[(3, 9_000)]), None, block(None))];
        let at = t0 + secs(135);
        assert_eq!(
            judge(&f.catchup, &members_of(&[1, 2, 3]), &far, &covering(), at),
            Decision::Kept
        );
        assert!(!f.catchup.gated(at));
        assert_eq!(f.catchup.unknown_because(at), Some(UnknownCause::OwedReplay));
        assert_eq!(f.catchup.owed_members(), [node(2), node(3)]);
    }

    /// `mark()` gates again in both `unknown` causes, a snapshot under
    /// `restored` gates once, and an equal reason does not rewrite the file.
    #[test]
    fn mark_regates_in_both_unknown_causes() {
        // `owed_replay`.
        let (f, t0) = latched();
        let mut changes = f.catchup.subscribe();
        changes.mark_unchanged();
        let at = t0 + secs(136);
        f.catchup.mark_at(CatchUpReason::Restored, at).unwrap();
        assert!(f.catchup.gated(at), "owed_replay: gated again");
        assert!(changes.has_changed().unwrap(), "owed_replay: streams woken");

        // `no_counting_peer`: the wait has run out at a virtual instant.
        let dir = tempfile::tempdir().unwrap();
        write_marker(dir.path(), CatchUpReason::Restored).unwrap();
        let catchup = CatchUp::open(dir.path(), WAIT);
        catchup.first_tick(Instant::now());
        let past = Instant::now() + WAIT + secs(10);
        assert!(catchup.unknown(past), "premise");
        let path = dir.path().join(FILE);
        #[cfg(unix)]
        let inode = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&path).unwrap());
        catchup.mark_at(CatchUpReason::Restored, past).unwrap();
        assert!(catchup.gated(past), "no_counting_peer: gated again");
        #[cfg(unix)]
        assert_eq!(
            std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(&path).unwrap()),
            inode,
            "an equal reason does not rewrite the file"
        );

        // A snapshot under `restored` gates the first time, and only the first.
        let later = past + WAIT + secs(10);
        assert!(catchup.unknown(later), "premise");
        catchup.mark_at(CatchUpReason::Snapshot, later).unwrap();
        assert!(catchup.gated(later), "a snapshot that has started gates");
        assert_eq!(catchup.reason(), Some(CatchUpReason::Restored), "the reason stays");
        let again = later + WAIT + secs(10);
        catchup.mark_at(CatchUpReason::Snapshot, again).unwrap();
        assert!(catchup.unknown(again), "a resumed snapshot does not gate again");
    }

    /// A repeated proof of the same lost entries does not gate again;
    /// entries not proven before do.
    #[test]
    fn a_repeated_proof_does_not_regate() {
        let (f, t0) = latched();
        let s5 = kimmy_core::Hlc::new(5, 0);
        let mut changes = f.catchup.subscribe();
        changes.mark_unchanged();
        let t = t0 + secs(131);
        f.catchup.mark_proven(CatchUpReason::Restored, node(2), s5, t).unwrap();
        assert!(f.catchup.gated(t), "the first proof gates");
        assert!(changes.has_changed().unwrap());
        changes.mark_unchanged();

        let live = members_of(&[1, 2]);
        assert_eq!(judge(&f.catchup, &live, &[peer_b()], &covering(), t + secs(5)), Decision::Kept);
        assert_eq!(f.catchup.unknown_because(t + secs(5)), Some(UnknownCause::OwedReplay));

        f.catchup.mark_proven(CatchUpReason::Restored, node(2), s5, t + secs(6)).unwrap();
        assert!(!f.catchup.gated(t + secs(6)), "the same proof again does not gate");
        assert!(!changes.has_changed().unwrap(), "nor wake the streams");

        let s6 = kimmy_core::Hlc::new(6, 0);
        f.catchup.mark_proven(CatchUpReason::Restored, node(2), s6, t + secs(7)).unwrap();
        assert!(f.catchup.gated(t + secs(7)), "a loss not proven before gates");
    }

    /// The operator's clear resets the latch: a later mark gates, it does
    /// not inherit the open state.
    #[test]
    fn clear_resets_the_latch() {
        let (f, t0) = latched();
        std::fs::remove_file(f._dir.path().join(FILE)).unwrap();
        f.catchup.refresh();
        assert!(!f.catchup.is_set());
        let at = t0 + secs(140);
        f.catchup.mark_at(CatchUpReason::Restored, at).unwrap();
        assert!(f.catchup.gated(at), "gated, not unknown");
        assert_eq!(f.catchup.unknown_because(at), None);
    }

    /// `unknown (owed)` clears only on a tick on which a clear rule holds,
    /// not when the owed set empties.
    #[test]
    fn owed_unknown_clears_only_on_a_covering_tick() {
        let (f, t0) = latched();
        f.catchup.replay_finished(node(2));
        assert!(f.catchup.is_set(), "an answer alone clears nothing");
        let live = members_of(&[1, 2]);
        let nobody: [Reached; 0] = [];
        assert_eq!(judge(&f.catchup, &live, &nobody, &covering(), t0 + secs(135)), Decision::Kept);
        assert!(f.catchup.is_set(), "a tick that reached nobody clears nothing");
        assert_eq!(
            judge(&f.catchup, &live, &[peer_b()], &covering(), t0 + secs(140)),
            Decision::Cleared("dominance")
        );
    }

    /// A member first seen live while the member is `unknown (owed)` owes
    /// at once, inside its own hold, and does not gate it again.
    #[test]
    fn a_member_first_seen_live_while_unknown_joins_without_regating() {
        let (f, t0) = latched();
        let mut changes = f.catchup.subscribe();
        changes.mark_unchanged();
        let at = t0 + secs(135);
        assert_eq!(
            judge(&f.catchup, &members_of(&[1, 2, 3]), &[peer_b()], &covering(), at),
            Decision::Kept
        );
        assert!(!f.catchup.gated(at), "no re-gate");
        assert_eq!(f.catchup.unknown_because(at), Some(UnknownCause::OwedReplay));
        assert_eq!(f.catchup.owed_members(), [node(2), node(3)]);
        assert!(!changes.has_changed().unwrap());
    }

    /// A peer reached every tick that never finishes the replay
    /// (its entries name a collection not here yet, or the budget runs out) holds
    /// the member gated only for its hold; past it the member serves as unknown.
    #[test]
    fn a_reached_peer_that_never_finishes_the_replay_serves_as_unknown_past_its_hold() {
        let (f, t0) = restored_armed();
        let live = members_of(&[1, 3]);
        let both = [peer_b(), reached(3, vector(&[(3, 10)]), None, block(None))];
        let mine = vector(&[(1, 100), (3, 10), (9, 200)]);
        assert_eq!(judge(&f.catchup, &live, &both, &mine, t0 + secs(5)), Decision::Kept);
        assert!(f.catchup.gated(t0 + secs(5)));
        let past = t0 + secs(130);
        assert_eq!(judge(&f.catchup, &live, &both, &mine, past), Decision::Kept);
        assert_eq!(f.catchup.unknown_because(past), Some(UnknownCause::OwedReplay));
        assert_eq!(f.catchup.owed_members(), [node(3)]);
    }

    /// A window peer that is ahead keeps the clear
    /// rule false while it is in the window, so the member stays gated though a
    /// member owes past its hold; once that peer has left the window the member
    /// serves as unknown.
    #[test]
    fn a_window_peer_ahead_keeps_the_gate() {
        let (f, t0) = restored_armed();
        let live = members_of(&[1, 2, 3]);
        let ahead = reached(3, vector(&[(3, 900)]), None, block(None));
        f.catchup.replay_finished(node(3));
        judge(&f.catchup, &live, &[peer_b()], &covering(), t0 + secs(5));
        judge(&f.catchup, &live, &[peer_b(), ahead], &covering(), t0 + secs(100));
        let past_c_hold = t0 + secs(150);
        assert_eq!(judge(&f.catchup, &live, &[peer_b()], &covering(), past_c_hold), Decision::Kept);
        assert!(f.catchup.gated(past_c_hold), "the peer ahead, reached in the window, gates");
        let out_of_window = t0 + secs(221);
        assert_eq!(
            judge(&f.catchup, &live, &[peer_b()], &covering(), out_of_window),
            Decision::Kept
        );
        assert_eq!(f.catchup.unknown_because(out_of_window), Some(UnknownCause::OwedReplay));
    }

    /// `first_seen` is recorded on every tick, before anything that can
    /// return early: member 2, first live on a tick a window peer holds, has its
    /// hold end a wait after that tick, not later.
    #[test]
    fn first_seen_is_recorded_on_every_tick() {
        let (f, t0) = restored_armed();
        let live = members_of(&[1, 2, 3]);
        let unanswered = [peer_b(), reached(3, vector(&[(3, 10)]), None, block(None))];
        let mine = vector(&[(1, 100), (3, 10), (9, 200)]);
        let tick1 = t0 + secs(5);
        assert_eq!(judge(&f.catchup, &live, &unanswered, &mine, tick1), Decision::Kept);
        f.catchup.replay_finished(node(3));
        assert_eq!(judge(&f.catchup, &live, &unanswered, &mine, t0 + secs(50)), Decision::Kept);
        let after = tick1 + WAIT + secs(1);
        assert_eq!(judge(&f.catchup, &live, &unanswered, &mine, after), Decision::Kept);
        assert_eq!(
            f.catchup.unknown_because(after),
            Some(UnknownCause::OwedReplay),
            "member 2's hold ended a wait after the tick it was first live"
        );
    }

    /// The start race: SWIM lists only B, and B's vector names C. C is
    /// not yet tracked, so the guard holds the clear until C is present; then C
    /// owes like any member.
    #[test]
    fn an_origin_named_in_a_reached_vector_holds_the_clear_until_reached() {
        for membership in [true, false] {
            let (f, t0) = restored_armed();
            let naming_c = [reached(1, vector(&[(1, 100), (2, 50)]), None, block(None))];
            let mine = vector(&[(1, 100), (2, 50), (9, 200)]);
            let only_b = members_of(&[1]);
            let run = |reached: &[Reached], live: &Members, at: Instant| {
                if membership {
                    judge(&f.catchup, live, reached, &mine, at)
                } else {
                    judge_without_membership(&f.catchup, reached, &mine, None, at)
                }
            };
            assert_eq!(run(&naming_c, &only_b, t0 + secs(5)), Decision::Kept, "{membership}");
            assert!(f.catchup.gated(t0 + secs(5)));
            // C is present: SWIM lists it, or (without membership) it is reached.
            let c = reached(2, vector(&[(2, 50)]), None, block(None));
            let both = [naming_c[0].clone(), c];
            assert_eq!(run(&both, &members_of(&[1, 2]), t0 + secs(30)), Decision::Kept);
            assert_eq!(f.catchup.owed_members(), [node(2)], "{membership}: C owes");
            f.catchup.replay_finished(node(2));
            assert_eq!(
                run(&both, &members_of(&[1, 2]), t0 + secs(35)),
                Decision::Cleared("dominance"),
                "{membership}"
            );
        }
    }

    /// `cluster.expected_members` holds the clear within the guard's
    /// window, and not past it.
    #[test]
    fn expected_members_holds_within_the_window() {
        for membership in [true, false] {
            let (f, t0) = restored_armed();
            let b = [peer_b()];
            let only_b = members_of(&[1]);
            let run = |at: Instant| {
                if membership {
                    judge_expecting(&f.catchup, &only_b, &b, &covering(), Some(3), at)
                } else {
                    judge_without_membership(&f.catchup, &b, &covering(), Some(3), at)
                }
            };
            assert_eq!(run(t0 + secs(5)), Decision::Kept, "{membership}: two of three");
            assert_eq!(run(t0 + secs(124)), Decision::Kept, "{membership}");
            assert_eq!(run(t0 + secs(125)), Decision::Cleared("dominance"), "{membership}");
        }
    }

    /// The guard's window runs from the first contact, not the arming: a
    /// resolver slow enough to put the first contact past a wait after the arming
    /// still has the guard hold at that contact.
    #[test]
    fn the_window_is_timed_from_the_first_contact() {
        let (f, t0) = restored_armed();
        let only_b = members_of(&[1]);
        let nobody: [Reached; 0] = [];
        judge(&f.catchup, &members_of(&[]), &nobody, &covering(), t0 + secs(5));
        let naming_c = [reached(1, vector(&[(1, 100), (2, 50)]), None, block(None))];
        let mine = vector(&[(1, 100), (2, 50), (9, 200)]);
        let first_contact = t0 + WAIT + secs(50);
        assert_eq!(judge(&f.catchup, &only_b, &naming_c, &mine, first_contact), Decision::Kept);
        assert!(f.catchup.gated(first_contact), "the guard holds at the first contact");
    }

    /// The guard lapses at the end of its window: an origin that is never
    /// present does not hold the clear for ever.
    #[test]
    fn the_guard_lapses_at_the_end_of_the_window() {
        let (f, t0) = restored_armed();
        let only_b = members_of(&[1]);
        let naming_c = [reached(1, vector(&[(1, 100), (2, 50)]), None, block(None))];
        let mine = vector(&[(1, 100), (2, 50), (9, 200)]);
        let first = t0 + secs(5);
        assert_eq!(judge(&f.catchup, &only_b, &naming_c, &mine, first), Decision::Kept);
        assert_eq!(
            judge(&f.catchup, &only_b, &naming_c, &mine, first + WAIT - secs(1)),
            Decision::Kept
        );
        assert_eq!(
            judge(&f.catchup, &only_b, &naming_c, &mine, first + WAIT),
            Decision::Cleared("dominance")
        );
    }

    /// The guard holds the floor too: on the first tick B has answered
    /// and SWIM lists only B, but B's vector names C, so the floor stays and the
    /// replay stays armed; past the window, with C never present, it settles.
    #[test]
    fn the_guard_holds_the_floor_within_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let catchup = CatchUp::open(dir.path(), WAIT);
        let t0 = Instant::now();
        catchup.arm_replay_at(kimmy_core::Hlc::new(50, 0), None, t0).unwrap();
        catchup.replay_finished(node(1));
        let only_b = members_of(&[1]);
        let naming_c = [reached(1, vector(&[(1, 100), (2, 50)]), None, block(None))];
        let mine = vector(&[(1, 100), (2, 50), (9, 200)]);
        let path = dir.path().join(REPLAY_FLOOR_FILE);
        judge(&catchup, &only_b, &naming_c, &mine, t0 + secs(5));
        assert!(path.exists() && catchup.replay_armed(), "the guard holds the floor");
        judge(&catchup, &only_b, &naming_c, &mine, t0 + secs(5) + WAIT);
        assert!(!path.exists() && !catchup.replay_armed(), "past the window it settles");
    }

    /// A `seeded_empty` member that created its store in this run has no
    /// history of its own to have lost: owing members do not hold its clear.
    #[test]
    fn a_seeded_empty_member_that_created_its_store_this_run_is_not_held_by_owing_members() {
        let dir = tempfile::tempdir().unwrap();
        let catchup = CatchUp::open(dir.path(), WAIT);
        catchup.mark(CatchUpReason::SeededEmpty).unwrap();
        catchup.note_store_created_this_run();
        let t0 = Instant::now();
        catchup.arm_replay_at(kimmy_core::Hlc::new(50, 0), None, t0).unwrap();
        catchup.replay_finished(node(1));
        assert_eq!(
            judge(&catchup, &members_of(&[1, 2]), &[peer_b()], &covering(), t0 + secs(5)),
            Decision::Cleared("dominance")
        );
    }

    /// A `seeded_empty` marker left by an earlier run, or one beside a
    /// floor file left before this run's arming, is held like any other.
    #[test]
    fn a_seeded_empty_marker_from_an_earlier_run_is_held() {
        // From an earlier run.
        let (f, t0) = armed_with(CatchUpReason::SeededEmpty, WAIT);
        let live = members_of(&[1, 2]);
        assert_eq!(
            judge(&f.catchup, &live, &[peer_b()], &covering(), t0 + secs(5)),
            Decision::Kept
        );
        assert_eq!(
            judge(&f.catchup, &live, &[peer_b()], &covering(), t0 + secs(130)),
            Decision::Kept
        );
        assert_eq!(f.catchup.unknown_because(t0 + secs(130)), Some(UnknownCause::OwedReplay));

        // Created this run, but a floor file was there before the arming.
        let dir = tempfile::tempdir().unwrap();
        write_replay_floor(dir.path(), kimmy_core::Hlc::new(40, 0)).unwrap();
        let catchup = CatchUp::open(dir.path(), WAIT);
        catchup.mark(CatchUpReason::SeededEmpty).unwrap();
        catchup.note_store_created_this_run();
        let t0 = Instant::now();
        catchup.arm_replay_at(kimmy_core::Hlc::new(50, 0), None, t0).unwrap();
        catchup.replay_finished(node(1));
        assert_eq!(judge(&catchup, &live, &[peer_b()], &covering(), t0 + secs(5)), Decision::Kept);
        assert_eq!(
            judge(&catchup, &live, &[peer_b()], &covering(), t0 + secs(130)),
            Decision::Kept
        );
        assert_eq!(catchup.unknown_because(t0 + secs(130)), Some(UnknownCause::OwedReplay));
    }

    /// The mutual clear, holding while a member owes the replay past its
    /// hold, serves as unknown and does not clear.
    #[test]
    fn the_mutual_clear_with_an_owed_live_member_serves_as_unknown() {
        let (f, t0) = armed_with(CatchUpReason::SeededEmpty, WAIT);
        let seeded = block(Some(CatchUpReason::SeededEmpty));
        let converged = vector(&[(1, 5), (2, 5), (9, 5)]);
        let both = [
            reached(1, vector(&[(1, 5)]), Some(converged.clone()), seeded.clone()),
            reached(2, vector(&[(2, 5)]), Some(converged.clone()), seeded),
        ];
        let live = members_of(&[1, 2]);
        assert_eq!(judge(&f.catchup, &live, &both, &converged, t0 + secs(5)), Decision::Kept);
        let past = t0 + secs(130);
        assert_eq!(judge(&f.catchup, &live, &both, &converged, past), Decision::Kept);
        assert_eq!(f.catchup.unknown_because(past), Some(UnknownCause::OwedReplay));
        assert_eq!(f.catchup.owed_members(), [node(2)]);
    }

    /// `cluster.expected_members` does not pad the owed set: once the
    /// guard's window is over, a member short of the expected count clears.
    #[test]
    fn expected_members_is_not_used_to_pad_the_owed_set() {
        let (f, t0) = restored_armed();
        let only_b = members_of(&[1]);
        let b = [peer_b()];
        judge_expecting(&f.catchup, &only_b, &b, &covering(), Some(3), t0 + secs(5));
        judge_expecting(&f.catchup, &only_b, &b, &covering(), Some(3), t0 + secs(130));
        assert!(!f.catchup.is_set(), "cleared");
        assert!(f.catchup.owed_members().is_empty(), "nobody stands in for the missing member");
    }

    /// The owed set survives a restart of this member through the floor
    /// file: a loaded member holds the clear for the wait from the arming, is
    /// released after the dwell if it is never seen, and owes on if it is seen.
    #[test]
    fn the_owed_set_survives_a_restart_of_this_member() {
        let b = [peer_b()];
        let only_b = members_of(&[1]);
        let fresh = |owed: &[NodeId]| {
            let dir = tempfile::tempdir().unwrap();
            let mut text = "floor_ms = 50\nfloor_counter = 0\n".to_string();
            for id in owed {
                text.push_str(&format!("owed = {id}\n"));
            }
            std::fs::write(dir.path().join(REPLAY_FLOOR_FILE), text).unwrap();
            write_marker(dir.path(), CatchUpReason::Restored).unwrap();
            let catchup = CatchUp::open(dir.path(), WAIT);
            let t0 = Instant::now();
            catchup.arm_replay_at(kimmy_core::Hlc::new(80, 0), None, t0).unwrap();
            catchup.replay_finished(node(1));
            (Fixture { _dir: dir, catchup }, t0)
        };

        // Never seen: held for the wait, then unknown, then released.
        let (f, t0) = fresh(&[node(2)]);
        assert_eq!(f.catchup.owed_members(), [node(2)], "loaded");
        assert_eq!(judge(&f.catchup, &only_b, &b, &covering(), t0 + secs(5)), Decision::Kept);
        assert!(f.catchup.gated(t0 + secs(5)), "held for the wait from the arming");
        assert_eq!(judge(&f.catchup, &only_b, &b, &covering(), t0 + secs(125)), Decision::Kept);
        assert_eq!(f.catchup.unknown_because(t0 + secs(125)), Some(UnknownCause::OwedReplay));
        let dwell = f.catchup.dwell();
        assert_eq!(
            judge(&f.catchup, &only_b, &b, &covering(), t0 + dwell),
            Decision::Cleared("dominance"),
            "released a dwell after the arming"
        );
        assert!(!floor_on_disk(&f));

        // Seen after the restart: it owes on.
        let (f, t0) = fresh(&[node(2)]);
        judge(&f.catchup, &members_of(&[1, 2]), &b, &covering(), t0 + secs(60));
        judge(&f.catchup, &members_of(&[1, 2]), &b, &covering(), t0 + dwell + secs(5));
        assert_eq!(f.catchup.owed_members(), [node(2)], "seen, so still owing");

        // More lines than the cap: the cap is loaded.
        let many: Vec<NodeId> = (0..MAX_SEEN as u64 + 76)
            .map(|i| {
                let mut bytes = [0u8; 16];
                bytes[..8].copy_from_slice(&(i + 1_000).to_be_bytes());
                NodeId::from_bytes(bytes)
            })
            .collect();
        let (f, _) = fresh(&many);
        assert_eq!(f.catchup.owed_members().len(), MAX_SEEN);
    }

    /// The wait counts from the first sync tick, not from the marker being set
    /// (before the store opens): a member whose open outlasts the wait is still
    /// gated at its first answer, serves as `unknown` only a full wait after its
    /// first tick, and says when the clock starts, once. Instants are virtual.
    #[test]
    fn the_wait_counts_from_the_first_sync_tick_and_not_from_the_marker() {
        let events = Events::default();
        let _recording = tracing::subscriber::set_default(events.clone());
        let f = marked(CatchUpReason::SeededEmpty);
        let opened = Instant::now() + WAIT * 3;
        // The open took three waits; the first answer comes before any tick.
        assert!(f.catchup.gated(opened), "gated at its first answer");
        assert_eq!(f.catchup.unknown_because(opened), None);
        assert_eq!(f.catchup.state_label(opened), "seeded_empty");
        assert!(events.matching("the catching-up wait starts").is_empty());

        f.catchup.first_tick(opened);
        f.catchup.first_tick(opened + secs(5));
        let said = events.matching("the catching-up wait starts");
        assert_eq!(said.len(), 1, "once per marked start: {said:?}");
        assert_eq!(said[0].0, tracing::Level::INFO);
        assert_eq!(said[0].2["reason"], "\"seeded_empty\"");
        assert_eq!(said[0].2["wait_s"], "120");

        assert!(f.catchup.gated(opened + WAIT - secs(1)), "inside the wait from the tick");
        let past = opened + WAIT + secs(1);
        assert!(f.catchup.unknown(past) && !f.catchup.gated(past), "a full wait after it");
        assert_eq!(f.catchup.unknown_because(past), Some(UnknownCause::NoCountingPeer));
    }

    /// A marker set at runtime, long after the first tick and after an earlier
    /// marker was cleared, gates the member and restarts the wait from when it
    /// was set: `unknown` only a full wait after that. Instants are virtual.
    #[test]
    fn a_marker_set_at_runtime_counts_the_wait_from_when_it_was_set() {
        let f = marked(CatchUpReason::SeededEmpty);
        let t0 = Instant::now();
        f.catchup.first_tick(t0);
        f.catchup.clear("the test");
        assert!(!f.catchup.is_set());
        let set_at = t0 + WAIT * 5;
        f.catchup.mark_at(CatchUpReason::Restored, set_at).unwrap();
        assert!(f.catchup.gated(set_at), "gated when it is set");
        assert!(f.catchup.gated(set_at + WAIT - secs(1)), "and for the wait after it");
        let past = set_at + WAIT + secs(1);
        assert!(f.catchup.unknown(past) && !f.catchup.gated(past), "unknown a full wait after");
        assert_eq!(f.catchup.unknown_because(past), Some(UnknownCause::NoCountingPeer));
    }

    /// The list and the state change together, at a tick: a member that answers
    /// between two ticks leaves the latch open (it drops at the next tick), and
    /// the list still names who owed at the last one, so `owed_replay` is never
    /// reported with `owed_members: []`.
    #[test]
    fn owed_members_is_never_empty_while_the_state_is_owed_replay() {
        let (f, t0) = latched();
        let at = t0 + secs(130);
        assert_eq!(f.catchup.owed_members(), [node(2)]);
        // C answers between two ticks.
        f.catchup.replay_finished(node(2));
        assert_eq!(f.catchup.unknown_because(at), Some(UnknownCause::OwedReplay), "latch open");
        assert_eq!(f.catchup.owed_members(), [node(2)], "the list is the last tick's");
        // The next tick drops the latch and the list with it.
        let live = members_of(&[1, 2]);
        let b = [peer_b()];
        let next = t0 + secs(135);
        judge(&f.catchup, &live, &b, &covering(), next);
        assert_eq!(f.catchup.unknown_because(next), None);
        assert!(f.catchup.owed_members().is_empty());
    }

    /// More `owed =` lines than the cap: the rest are dropped on load, and a
    /// `WARN` says how many. A file at the cap says nothing.
    #[test]
    fn the_cap_dropping_owed_lines_on_load_is_said() {
        let events = Events::default();
        let _recording = tracing::subscriber::set_default(events.clone());
        let load = |count: u64| {
            let dir = tempfile::tempdir().unwrap();
            let mut text = "floor_ms = 50\nfloor_counter = 0\n".to_string();
            for i in 0..count {
                let mut bytes = [0u8; 16];
                bytes[..8].copy_from_slice(&(i + 1_000).to_be_bytes());
                text.push_str(&format!("owed = {}\n", NodeId::from_bytes(bytes)));
            }
            let path = dir.path().join(REPLAY_FLOOR_FILE);
            std::fs::write(&path, text).unwrap();
            read_replay_floor_file(&path).unwrap().1
        };
        assert_eq!(load(MAX_SEEN as u64).len(), MAX_SEEN);
        assert!(events.matching("more owing members than the cap").is_empty());
        assert_eq!(load(MAX_SEEN as u64 + 76).len(), MAX_SEEN);
        let said = events.matching("more owing members than the cap");
        assert_eq!(said.len(), 1, "{said:?}");
        assert_eq!(said[0].0, tracing::Level::WARN);
        assert_eq!(said[0].2["dropped"], "76", "{:?}", said[0].2);
        // A repeated id beyond the cap is one dropped member, not two.
        let again = {
            let dir = tempfile::tempdir().unwrap();
            let mut text = "floor_ms = 50\nfloor_counter = 0\n".to_string();
            let id = |i: u64| {
                let mut bytes = [0u8; 16];
                bytes[..8].copy_from_slice(&i.to_be_bytes());
                NodeId::from_bytes(bytes)
            };
            for i in 0..MAX_SEEN as u64 {
                text.push_str(&format!("owed = {}\n", id(i + 1_000)));
            }
            for _ in 0..3 {
                text.push_str(&format!("owed = {}\n", id(1)));
            }
            let path = dir.path().join(REPLAY_FLOOR_FILE);
            std::fs::write(&path, text).unwrap();
            read_replay_floor_file(&path).unwrap().1.len()
        };
        assert_eq!(again, MAX_SEEN);
        let said = events.matching("more owing members than the cap");
        assert_eq!(said.len(), 2, "{said:?}");
        assert_eq!(said[1].2["dropped"], "1", "{:?}", said[1].2);
        assert_eq!(said[0].2["cap"], MAX_SEEN.to_string());
    }

    /// With both causes holding, `unknown_because` names the owed replay:
    /// it is the one that governs what happens next.
    #[test]
    fn unknown_because_names_owed_when_both_causes_hold() {
        let (f, t0) = latched();
        let nobody: [Reached; 0] = [];
        let at = t0 + secs(130) + WAIT + secs(1);
        judge(&f.catchup, &members_of(&[1, 2]), &nobody, &covering(), at);
        assert!(f.catchup.unknown(at));
        assert_eq!(f.catchup.unknown_because(at), Some(UnknownCause::OwedReplay));
    }

    /// Lowering the wait does not shrink the dwell: a restart of an owing
    /// member that takes longer than the wait is not taken for its leaving.
    #[test]
    fn lowering_wait_does_not_shrink_the_dwell() {
        let (f, t0) = armed_with(CatchUpReason::Restored, secs(30));
        let b = [peer_b()];
        let members = members_of(&[1, 2]);
        judge(&f.catchup, &members, &b, &covering(), t0 + secs(5));
        let gone = t0 + secs(10);
        members.remove_for_test(&addr(2));
        judge(&f.catchup, &members, &b, &covering(), gone);
        assert_eq!(judge(&f.catchup, &members, &b, &covering(), gone + secs(40)), Decision::Kept);
        assert_eq!(f.catchup.owed_members(), [node(2)], "a 40 s absence is not leaving");
        assert!(floor_on_disk(&f));
        assert_eq!(
            judge(&f.catchup, &members, &b, &covering(), gone + DWELL_FLOOR),
            Decision::Cleared("dominance")
        );
        assert!(!floor_on_disk(&f));
    }

    /// The release is a `WARN` naming the member, its last address, how
    /// long it was gone, the dwell and the cause.
    #[test]
    fn the_release_is_a_warn_naming_the_member_and_cause() {
        let events = Events::default();
        let _recording = tracing::subscriber::set_default(events.clone());
        let (f, t0) = restored_armed();
        let b = [peer_b()];
        let members = members_of(&[1, 2]);
        judge(&f.catchup, &members, &b, &covering(), t0 + secs(5));
        let gone = t0 + secs(10);
        members.remove_for_test(&addr(2));
        judge(&f.catchup, &members, &b, &covering(), gone);
        judge(&f.catchup, &members, &b, &covering(), gone + f.catchup.dwell());
        let released = events.matching("treated as having left");
        assert_eq!(released.len(), 1, "{released:?}");
        let (level, _, fields) = &released[0];
        assert_eq!(*level, tracing::Level::WARN);
        assert_eq!(fields.get("peer").map(String::as_str), Some(node(2).to_string().as_str()));
        assert_eq!(fields.get("cause").map(String::as_str), Some("\"swim_down\""));
        assert_eq!(fields.get("address").map(String::as_str), Some(addr(2).to_string().as_str()));
        assert_eq!(fields.get("dwell_secs").map(String::as_str), Some("600"));
        assert_eq!(fields.get("gone_for_secs").map(String::as_str), Some("600"));
    }

    /// Past the retention the replay cannot read back what is older: one
    /// `WARN` says so while a member owes, and the replay asks from the horizon.
    #[test]
    fn past_retention_the_replay_warns_once_and_answers_from_the_horizon() {
        let events = Events::default();
        let _recording = tracing::subscriber::set_default(events.clone());
        let dir = tempfile::tempdir().unwrap();
        write_marker(dir.path(), CatchUpReason::Restored).unwrap();
        let wall = kimmy_storage::physical_now_ms();
        write_replay_floor(dir.path(), kimmy_core::Hlc::new(wall - 120_000, 0)).unwrap();
        let catchup = CatchUp::open(dir.path(), WAIT);
        let t0 = Instant::now();
        catchup.arm_replay_at(kimmy_core::Hlc::new(wall, 0), Some(secs(60)), t0).unwrap();
        catchup.replay_finished(node(1));
        let live = members_of(&[1, 2]);
        for at in [5, 130, 135, 140] {
            judge(&catchup, &live, &[peer_b()], &covering(), t0 + secs(at));
        }
        let warned = events.matching("can no longer read back writes older than");
        assert_eq!(warned.len(), 1, "{warned:?}");
        assert_eq!(warned[0].0, tracing::Level::WARN);
        assert!(
            catchup.replay_from(node(2)).wall_ms + 60_000 >= kimmy_storage::physical_now_ms(),
            "the replay asks from the horizon"
        );
    }

    /// A reader of the floor file as 0.43 wrote it: it reads the floor lines and
    /// skips every other key. Kept here, verbatim from that release, to show a
    /// rollback reads the file this build writes.
    fn read_replay_floor_as_0_43(path: &Path) -> Option<kimmy_core::Hlc> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(_) => return Some(kimmy_core::Hlc::ZERO),
        };
        let (mut ms, mut counter) = (None, 0u16);
        for line in text.lines() {
            let Some((key, value)) = line.split_once('=') else { continue };
            match key.trim() {
                "floor_ms" => ms = value.trim().parse::<u64>().ok(),
                "floor_counter" => counter = value.trim().parse::<u16>().unwrap_or(0),
                _ => {}
            }
        }
        Some(kimmy_core::Hlc::new(ms.unwrap_or(0), if ms.is_some() { counter } else { 0 }))
    }

    /// Every write of the floor file carries the floor lines; the arming
    /// rewrite keeps the owed lines it loaded, drops one that does not parse (with
    /// a `WARN`), and never lets it void the floor; and 0.43's reader reads the
    /// same floor from it.
    #[test]
    fn the_floor_file_always_carries_the_floor_and_the_arming_rewrite_keeps_the_owed_lines() {
        let events = Events::default();
        let _recording = tracing::subscriber::set_default(events.clone());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(REPLAY_FLOOR_FILE);
        std::fs::write(
            &path,
            format!("floor_ms = 1000\nfloor_counter = 3\nowed = {}\nowed = garbage\n", node(2)),
        )
        .unwrap();
        let catchup = CatchUp::open(dir.path(), WAIT);
        catchup
            .arm_replay_at(kimmy_core::Hlc::new(2_000, 0), Some(secs(3600)), Instant::now())
            .unwrap();
        let floor = catchup.replay_floor();
        assert!(floor.wall_ms > 1_000, "clamped: {floor:?}");
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines,
            [
                format!("floor_ms = {}", floor.wall_ms).as_str(),
                format!("floor_counter = {}", floor.counter).as_str(),
                format!("owed = {}", node(2)).as_str(),
            ],
            "{text}"
        );
        assert_eq!(read_replay_floor_as_0_43(&path), Some(floor), "0.43 reads the same floor");
        let skipped = events.matching("does not name a node");
        assert_eq!(skipped.len(), 1, "{skipped:?}");
        assert_eq!(skipped[0].0, tracing::Level::WARN);

        // A line that does not parse never voids the floor.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(REPLAY_FLOOR_FILE);
        std::fs::write(&path, "floor_ms = 1000\nfloor_counter = 3\nowed = garbage\n").unwrap();
        let catchup = CatchUp::open(dir.path(), WAIT);
        catchup.arm_replay_at(kimmy_core::Hlc::new(2_000, 0), None, Instant::now()).unwrap();
        assert_eq!(catchup.replay_floor(), kimmy_core::Hlc::new(1_000, 3));
        assert!(catchup.owed_members().is_empty());
    }

    /// The owed lines a tick writes are read by 0.43 as nothing: its reader finds
    /// the same floor in the file, which does carry them.
    #[test]
    fn a_0_43_reader_reads_the_floor_and_ignores_the_owed_lines() {
        let (f, t0) = restored_armed();
        judge(&f.catchup, &members_of(&[1, 2]), &[peer_b()], &covering(), t0 + secs(5));
        let path = f._dir.path().join(REPLAY_FLOOR_FILE);
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains(&format!("owed = {}\n", node(2))), "{text}");
        assert_eq!(read_replay_floor_as_0_43(&path), Some(kimmy_core::Hlc::new(50, 0)));
        assert_eq!(read_replay_floor(&path), Some(kimmy_core::Hlc::new(50, 0)));
    }

    /// A rewrite of the owed lines from a tick that fails says so, keeps
    /// the set in memory, and the tick completes.
    #[test]
    fn a_failed_owed_rewrite_warns_and_keeps_the_set() {
        let events = Events::default();
        let _recording = tracing::subscriber::set_default(events.clone());
        let (f, t0) = restored_armed();
        let path = f._dir.path().join(REPLAY_FLOOR_FILE);
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert_eq!(
            judge(&f.catchup, &members_of(&[1, 2]), &[peer_b()], &covering(), t0 + secs(5)),
            Decision::Kept
        );
        let warned = events.matching("could not record the members that owe the replay");
        assert_eq!(warned.len(), 1, "{warned:?}");
        assert_eq!(warned[0].0, tracing::Level::WARN);
        assert_eq!(f.catchup.owed_members(), [node(2)], "kept in memory");
    }

    /// The latch drops when nothing is owed: with a peer ahead in the
    /// window the member is gated again (streams woken), and a covering tick then
    /// clears it.
    #[test]
    fn the_latch_drops_when_nothing_is_owed() {
        let (f, t0) = latched();
        let mut changes = f.catchup.subscribe();
        changes.mark_unchanged();
        f.catchup.replay_finished(node(2));
        let live = members_of(&[1, 2]);
        let ahead = [reached(1, vector(&[(1, 500)]), None, block(None))];
        let at = t0 + secs(135);
        assert_eq!(judge(&f.catchup, &live, &ahead, &covering(), at), Decision::Kept);
        assert!(f.catchup.gated(at), "gated, as any marked member behind a peer");
        assert_eq!(f.catchup.unknown_because(at), None);
        assert!(changes.has_changed().unwrap(), "and the streams are woken");
        let caught_up = vector(&[(1, 500), (9, 200)]);
        assert_eq!(
            judge(&f.catchup, &live, &ahead, &caught_up, t0 + secs(140)),
            Decision::Cleared("dominance")
        );
    }

    /// SWIM bringing an owing member up and down again between two ticks
    /// moves its address's generation, and that restarts the dwell, though every
    /// tick's sample found it absent.
    #[test]
    fn a_generation_bump_between_ticks_resets_the_dwell() {
        let (f, t0) = restored_armed();
        let dwell = f.catchup.dwell();
        let b = [peer_b()];
        let members = members_of(&[1, 2]);
        judge(&f.catchup, &members, &b, &covering(), t0 + secs(5));
        members.remove_for_test(&addr(2));
        let gone = t0 + secs(10);
        judge(&f.catchup, &members, &b, &covering(), gone);
        members.bump_generation_for_test(&addr(2));
        judge(&f.catchup, &members, &b, &covering(), gone + secs(300));
        members.bump_generation_for_test(&addr(2));
        judge(&f.catchup, &members, &b, &covering(), gone + dwell - secs(1));
        assert_eq!(
            judge(&f.catchup, &members, &b, &covering(), gone + dwell + secs(1)),
            Decision::Kept
        );
        assert_eq!(f.catchup.owed_members(), [node(2)], "the dwell restarted");
    }

    /// The `WARN`s an operator reads while the replay is owed: on entry to
    /// `unknown (owed)`, naming the owing members and their addresses, and every
    /// ten minutes after it; once while the start-race guard holds a marked
    /// member's clear; and, for a member that is not marked, every ten minutes
    /// while its replay is still owed past the hold.
    #[test]
    fn the_owed_replay_is_said_on_entry_every_ten_minutes_and_when_unmarked() {
        let events = Events::default();
        let _recording = tracing::subscriber::set_default(events.clone());
        let (f, t0) = latched();
        let entered = events.matching("now serves as unknown");
        assert_eq!(entered.len(), 1, "{entered:?}");
        let (level, _, fields) = &entered[0];
        assert_eq!(*level, tracing::Level::WARN);
        assert!(fields["owed"].contains(&node(2).to_string()), "{fields:?}");
        assert!(fields["owed_addresses"].contains(&addr(2).to_string()), "{fields:?}");
        let live = members_of(&[1, 2]);
        for at in [300, 729, 730, 900] {
            judge(&f.catchup, &live, &[peer_b()], &covering(), t0 + secs(at));
        }
        let still = events.matching("still serves as unknown");
        assert_eq!(still.len(), 1, "ten minutes after the entry, and not before: {still:?}");
        assert_eq!(still[0].0, tracing::Level::WARN);

        // The start-race guard, once.
        let (g, t0) = restored_armed();
        let naming_c = [reached(1, vector(&[(1, 100), (2, 50)]), None, block(None))];
        let mine = vector(&[(1, 100), (2, 50), (9, 200)]);
        for at in [5, 10] {
            judge(&g.catchup, &members_of(&[1]), &naming_c, &mine, t0 + secs(at));
        }
        let guarded = events.matching("holds its clear until the members its peers");
        assert_eq!(guarded.len(), 1, "{guarded:?}");
        assert!(guarded[0].2["absent_origins"].contains(&node(2).to_string()));

        // Not marked, and owed past the hold.
        let dir = tempfile::tempdir().unwrap();
        let unmarked = CatchUp::open(dir.path(), WAIT);
        let t0 = Instant::now();
        unmarked.arm_replay_at(kimmy_core::Hlc::new(50, 0), None, t0).unwrap();
        unmarked.replay_finished(node(1));
        for at in [5, 100, 130, 200] {
            judge(&unmarked, &live, &[peer_b()], &covering(), t0 + secs(at));
        }
        let said = events.matching("this member is not marked");
        assert_eq!(said.len(), 1, "{said:?}");
        assert_eq!(said[0].0, tracing::Level::WARN);
    }
}
