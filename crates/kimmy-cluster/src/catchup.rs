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
//! `catch_up_wait_secs` the gate opens and the state reads `unknown`, with owner
//! work still off and a `WARN` every ten minutes. Once a counting peer is
//! reached and is ahead the gate closes again, and holds for as long as the
//! catch-up takes: that is knowledge, not a guess.

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
    /// When a counting peer was last reached, milliseconds since `epoch`.
    counting_ms: AtomicU64,
    /// Bumped whenever the member may have become gated: a marker set, or a
    /// counting peer reached that closes a gate the wait had opened. What an open
    /// change stream waits on to end when the member starts refusing.
    changes: tokio::sync::watch::Sender<u64>,
    state: Mutex<State>,
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
            changes: tokio::sync::watch::channel(0).0,
            state: Mutex::new(State { marker: None, reached: BTreeMap::new(), warned: None }),
        });
        if let Some(marker) = marker {
            info!(
                reason = marker.reason.label(),
                since_ms = marker.since_ms,
                "the catching-up marker is set: this member is behind until it has caught up"
            );
            this.install(marker);
        }
        this
    }

    fn install(&self, marker: Marker) {
        self.reason.store(marker.reason.code(), Relaxed);
        self.set.store(true, Relaxed);
        // The bound counts from the moment it is set.
        self.counting_ms.store(self.ms(Instant::now()), Relaxed);
        self.state.lock().marker = Some(marker);
        self.changes.send_modify(|n| *n += 1);
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
        self.is_set()
            && now.saturating_duration_since(self.epoch).as_millis() as u64
                > self.counting_ms.load(Relaxed) + self.wait.as_millis() as u64
    }

    /// Whether client requests are refused: set, and the bound has not run out.
    pub fn gated(&self, now: Instant) -> bool {
        self.is_set() && !self.unknown(now)
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
        let state = self.state.lock();
        let marker = match state.marker.clone() {
            // Already set, and at least as strong: nothing to do.
            Some(held) if held.reason.strength() >= reason.strength() => return Ok(()),
            Some(mut held) => {
                held.reason = reason;
                held
            }
            None => Marker::new(reason),
        };
        kimmy_storage::blocking(|| write_atomic(&self.path, &marker.render()))?;
        drop(state);
        info!(reason = reason.label(), "the catching-up marker was set");
        self.install(marker);
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
        info!(how, "the catching-up marker was cleared");
    }

    /// The operator's clear: deleting the file takes effect live. Called every
    /// tick, and by the gate's refresh, so it is not a start-time check.
    pub fn refresh(&self) {
        if self.is_set() && !kimmy_storage::blocking(|| self.path.exists()) {
            self.clear("the operator (the file was deleted)");
        }
    }

    /// Remove the marker file and forget the marker, when clustering is off: a
    /// stale file from a node that left a cluster must not gate a standalone one
    /// for ever.
    pub fn discard_when_standalone(&self) {
        if self.is_set() {
            info!("clustering is off, so the catching-up marker is discarded");
            self.clear("clustering is off");
        }
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
        // Reaching one this tick restarts the wait.
        if tick.reached.iter().any(counts) {
            let was_open = self.unknown(tick.now);
            self.counting_ms.store(self.ms(tick.now), Relaxed);
            if was_open {
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

        // Dominance: a contact this tick, and this member's witnessed vector
        // covers what every counting peer in the window serves, and no
        // whole-database snapshot is under way.
        let counting: Vec<&&Reached> = window.iter().filter(|peer| counts(peer)).collect();
        if tick.reached.iter().any(counts)
            && !tick.snapshot_pending
            && counting.iter().all(|peer| tick.mine_witnessed.covers(&peer.servable))
        {
            drop(state);
            self.clear("dominance");
            return Decision::Cleared("dominance");
        }

        // The mutual clear, for a `seeded_empty` member only.
        if marker.reason == CatchUpReason::SeededEmpty
            && !marker.overflowed
            && !tick.snapshot_pending
            && mutual_clear_holds(&marker, tick, &window, &state.reached, self.wait)
        {
            drop(state);
            self.clear("the mutual clear");
            return Decision::Cleared("mutual");
        }

        // Past the bound, still set: say so, and why.
        if self.unknown(tick.now)
            && state.warned.is_none_or(|at| tick.now.saturating_duration_since(at) >= WARN_EVERY)
        {
            state.warned = Some(tick.now);
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
pub fn write_atomic(path: &Path, text: &str) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = dir.join(format!("{FILE}.tmp"));
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(dir)?.sync_all()
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
}
