//! The repair loop guard (repair design §4): what stops a repair that cannot
//! change anything from running for ever, and a repair that works from hiding a
//! bug that keeps producing the divergence.
//!
//! A pure state machine. It reads no clock and no engine: the caller passes the
//! time of each event as a [`Duration`] since any fixed origin, and the ledger
//! answers whether a repair may be planned and what its end meant. Nothing here
//! is persisted (R6): a restart forgets all of it, and a crash-looping member
//! costs one more pull per finding per start, bounded by the planner's cooldown.
//!
//! One entry per `(peer, collection, kind)`. The rules, numbered as the design
//! numbers them:
//!
//! * **R1** a transient end changes nothing but the planner's cooldown;
//! * **R2** a repair is judged at the first *compared* probe after its end, and
//!   only there; one never judged by its deadline counts as transient, but every
//!   second consecutive such end climbs one step;
//! * **R3** an ineffective repair backs off, 5 min, 15 min, 45 min, 2 h 15 min,
//!   6 h, and one that made progress lowers the level to 1;
//! * **R4** more than three episodes of clear-and-recur in a rolling hour stop
//!   the collection, and the stop expires;
//! * **R5** only a confirmed, un-stopped, un-backed-off finding plans, and not
//!   while a repair is in flight or awaiting its probe;
//! * **R7** [`RepairLedger::retain`] prunes by the held collections and the
//!   cluster's members.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kimmy_core::{CollectionId, NodeId};

/// Which finding a repair answers. The count half has its own cooldown and no
/// ledger entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LedgerKind {
    /// A confirmed cut-digest finding: a `Converge` pull.
    Digest,
    /// A confirmed definition finding: a definition repair.
    Definition,
}

/// The ledger's key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub peer: NodeId,
    pub collection: CollectionId,
    pub kind: LedgerKind,
}

/// Back-off after the n-th consecutive ineffective repair (n from 1), R3.
pub const BACKOFF: [Duration; 5] = [
    Duration::from_secs(5 * 60),
    Duration::from_secs(15 * 60),
    Duration::from_secs(45 * 60),
    Duration::from_secs(2 * 3600 + 15 * 60),
    Duration::from_secs(6 * 3600),
];

/// The highest level; at it the back-off is the last of [`BACKOFF`].
pub const MAX_LEVEL: u8 = 5;

/// The shortest a repair waits for its probe (R2).
pub const MIN_PROBE_DEADLINE: Duration = Duration::from_secs(30 * 60);

/// The rolling window R4 counts episodes in.
pub const EPISODE_WINDOW: Duration = Duration::from_secs(3600);

/// More than this many episodes in the window stop the collection (R4).
pub const MAX_EPISODES: usize = 3;

/// How long a stopped collection stays stopped at most (R4).
pub const STOP_FOR: Duration = Duration::from_secs(6 * 3600);

/// How long a stopped collection must show no confirmed finding to be re-armed
/// before [`STOP_FOR`] (R4).
pub const STOP_CLEAR_FOR: Duration = Duration::from_secs(3600);

/// The ledger's one tunable: how long the count rotation takes to name every
/// collection once, which sets R2's deadline and R4's de-duplication window.
#[derive(Clone, Copy, Debug)]
pub struct Params {
    pub rotation: Duration,
}

impl Params {
    fn deadline(self) -> Duration {
        (self.rotation * 3).max(MIN_PROBE_DEADLINE)
    }

    fn dedupe(self) -> Duration {
        self.rotation * 3
    }
}

/// Why a repair ended without a result to judge (R1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transient {
    PeerError,
    Stopped,
    PurgePending,
    Abandoned,
    Dropped,
    WholeDatabaseTookOver,
    Gone,
}

/// How a repair ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    /// Ended without a result: changes nothing but the cooldown (R1).
    Transient(Transient),
    /// Ran to its end. `progress` is the kind's definition of progress (R3).
    Finished { progress: bool },
}

/// What the first compared probe after a repair's end said (R2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Judged {
    /// The compared probe matched.
    Effective,
    /// The compared probe differed, or the build refused it (`Explained`).
    Ineffective,
}

/// Whether the ledger lets a repair plan (R5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MayPlan {
    Yes,
    /// A repair is in flight or awaits its judging probe.
    Busy,
    /// Backing off until the time.
    BackOff(Duration),
    /// The collection is stopped for recurring (R4), until the time.
    Stopped(Duration),
}

/// A repair's end as the ledger saw it, for the caller's log and metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Effective,
    Ineffective {
        level: u8,
        not_before: Duration,
    },
    /// Ended transiently, or never judged by its deadline and not climbing.
    Transient,
    /// Never judged by its deadline, and this was the second in a row.
    UnjudgedClimb {
        level: u8,
        not_before: Duration,
    },
}

/// What a call to [`RepairLedger::confirmed`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Confirmed {
    /// This confirmation is a counted episode (a recurrence after a repair that
    /// cleared the finding).
    pub episode: bool,
    /// This episode took the collection over the limit: it is now stopped.
    pub newly_stopped: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    InFlight,
    Awaiting { ended_at: Duration, deadline: Duration, progress: bool },
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    level: u8,
    not_before: Duration,
    phase: Phase,
    /// Consecutive repairs that ended unjudged (R2).
    unjudged_run: u32,
}

impl Entry {
    fn new() -> Self {
        Entry { level: 0, not_before: Duration::ZERO, phase: Phase::Idle, unjudged_run: 0 }
    }
}

#[derive(Clone, Debug, Default)]
struct CollectionState {
    /// Counted episodes, oldest first.
    episodes: Vec<Duration>,
    /// Until when the collection is stopped.
    stopped_until: Option<Duration>,
    /// The last time a finding against the collection was confirmed.
    last_confirmed: Option<Duration>,
    /// When the stop began (for the quiet re-arm).
    stopped_at: Option<Duration>,
}

/// The loop guard's state.
#[derive(Clone, Debug)]
pub struct RepairLedger {
    params: Params,
    entries: BTreeMap<Key, Entry>,
    /// A `(peer, collection)` whose repair cleared the finding, so the next
    /// confirmation of it is a recurrence (R4). The value is when it cleared.
    armed: BTreeMap<(NodeId, CollectionId), Duration>,
    collections: BTreeMap<CollectionId, CollectionState>,
}

impl RepairLedger {
    pub fn new(params: Params) -> Self {
        RepairLedger {
            params,
            entries: BTreeMap::new(),
            armed: BTreeMap::new(),
            collections: BTreeMap::new(),
        }
    }

    /// R5: whether a repair for `key` may be planned at `now`.
    pub fn may_plan(&mut self, key: Key, now: Duration) -> MayPlan {
        self.rearm(key.collection, now);
        if let Some(until) = self.collections.get(&key.collection).and_then(|c| c.stopped_until) {
            return MayPlan::Stopped(until);
        }
        match self.entries.get(&key) {
            None => MayPlan::Yes,
            Some(entry) => match entry.phase {
                Phase::InFlight | Phase::Awaiting { .. } => MayPlan::Busy,
                Phase::Idle if now < entry.not_before => MayPlan::BackOff(entry.not_before),
                Phase::Idle => MayPlan::Yes,
            },
        }
    }

    /// A repair for `key` was handed out. Only after [`Self::may_plan`] said yes.
    pub fn started(&mut self, key: Key, _now: Duration) {
        self.entries.entry(key).or_insert_with(Entry::new).phase = Phase::InFlight;
    }

    /// The repair for `key` ended (R1, R2, R3). Only a repair that was started
    /// ends; anything else is ignored, so a late event cannot invent state.
    pub fn ended(&mut self, key: Key, now: Duration, end: End) {
        let deadline = self.params.deadline();
        let Some(entry) = self.entries.get_mut(&key) else {
            return;
        };
        if entry.phase != Phase::InFlight {
            return;
        }
        entry.phase = match end {
            End::Transient(_) => Phase::Idle,
            End::Finished { progress } => {
                Phase::Awaiting { ended_at: now, deadline: now + deadline, progress }
            }
        };
    }

    /// R2: the first compared probe after the end of `key`'s repair said
    /// `judged`. Ignored unless the repair is awaiting its probe.
    pub fn judge(&mut self, key: Key, now: Duration, judged: Judged) -> Option<Verdict> {
        let entry = self.entries.get_mut(&key)?;
        let Phase::Awaiting { ended_at, progress, .. } = entry.phase else {
            return None;
        };
        entry.phase = Phase::Idle;
        entry.unjudged_run = 0;
        Some(match judged {
            Judged::Effective => {
                entry.level = 0;
                entry.not_before = Duration::ZERO;
                self.armed.insert((key.peer, key.collection), now);
                Verdict::Effective
            }
            Judged::Ineffective => {
                entry.level = if progress { 1 } else { (entry.level + 1).min(MAX_LEVEL) };
                entry.not_before = ended_at + BACKOFF[usize::from(entry.level) - 1];
                Verdict::Ineffective { level: entry.level, not_before: entry.not_before }
            }
        })
    }

    /// A clear of `key`'s finding arrived. Within the deadline of a repair that
    /// is still awaiting its probe, the repair is recorded retroactively as
    /// effective (R2, R4); otherwise nothing.
    pub fn cleared(&mut self, key: Key, now: Duration) -> Option<Verdict> {
        let entry = self.entries.get(&key)?;
        match entry.phase {
            Phase::Awaiting { deadline, .. } if now <= deadline => {
                self.judge(key, now, Judged::Effective)
            }
            _ => None,
        }
    }

    /// R2: end every repair whose deadline has passed unjudged. Returns the
    /// keys and what happened to each.
    pub fn expire(&mut self, now: Duration) -> Vec<(Key, Verdict)> {
        let mut out = Vec::new();
        for (key, entry) in &mut self.entries {
            let Phase::Awaiting { ended_at, deadline, progress } = entry.phase else {
                continue;
            };
            if now < deadline {
                continue;
            }
            entry.phase = Phase::Idle;
            entry.unjudged_run += 1;
            if progress {
                entry.level = entry.level.min(1);
            }
            if entry.unjudged_run.is_multiple_of(2) {
                entry.level = (entry.level + 1).min(MAX_LEVEL);
                entry.not_before = (ended_at + BACKOFF[usize::from(entry.level) - 1]).max(now);
                out.push((
                    *key,
                    Verdict::UnjudgedClimb { level: entry.level, not_before: entry.not_before },
                ));
            } else {
                out.push((*key, Verdict::Transient));
            }
        }
        out
    }

    /// A finding against `key.peer` for `key.collection` is confirmed at `now`
    /// (called every contact it stands). The first one after a repair cleared
    /// it is an episode (R4).
    pub fn confirmed(&mut self, key: Key, now: Duration) -> Confirmed {
        let dedupe = self.params.dedupe();
        let state = self.collections.entry(key.collection).or_default();
        state.last_confirmed = Some(now);
        if self.armed.remove(&(key.peer, key.collection)).is_none() {
            return Confirmed::default();
        }
        // Different peers' episodes within the de-duplication window are one.
        if state.episodes.last().is_some_and(|last| now.saturating_sub(*last) < dedupe) {
            return Confirmed { episode: false, newly_stopped: false };
        }
        state.episodes.retain(|at| now.saturating_sub(*at) < EPISODE_WINDOW);
        state.episodes.push(now);
        let mut newly_stopped = false;
        if state.episodes.len() > MAX_EPISODES && state.stopped_until.is_none() {
            state.stopped_until = Some(now + STOP_FOR);
            state.stopped_at = Some(now);
            newly_stopped = true;
        }
        Confirmed { episode: true, newly_stopped }
    }

    /// R4: a stopped collection re-arms after [`STOP_FOR`], or once no finding
    /// against it has been confirmed for [`STOP_CLEAR_FOR`], whichever is first.
    fn rearm(&mut self, collection: CollectionId, now: Duration) {
        let Some(state) = self.collections.get_mut(&collection) else {
            return;
        };
        let Some(until) = state.stopped_until else {
            return;
        };
        let quiet = state
            .last_confirmed
            .or(state.stopped_at)
            .is_some_and(|at| now.saturating_sub(at) >= STOP_CLEAR_FOR);
        if now >= until || quiet {
            state.stopped_until = None;
            state.stopped_at = None;
            state.episodes.clear();
        }
    }

    /// Whether the collection is stopped at `now`.
    pub fn is_stopped(&mut self, collection: CollectionId, now: Duration) -> bool {
        self.rearm(collection, now);
        self.collections.get(&collection).is_some_and(|c| c.stopped_until.is_some())
    }

    /// R7: drop everything about a peer that is no longer a member and a
    /// collection that is no longer held. A repair still in flight for a key
    /// that is dropped is dropped with it.
    pub fn retain(&mut self, held: &BTreeSet<CollectionId>, members: &BTreeSet<NodeId>) {
        self.entries.retain(|key, _| held.contains(&key.collection) && members.contains(&key.peer));
        self.armed
            .retain(|(peer, collection), _| held.contains(collection) && members.contains(peer));
        self.collections.retain(|collection, _| held.contains(collection));
    }

    /// The number of entries per level, for the gauge (§15.2).
    pub fn levels(&self, kind: LedgerKind) -> [usize; MAX_LEVEL as usize + 1] {
        let mut out = [0; MAX_LEVEL as usize + 1];
        for (key, entry) in &self.entries {
            if key.kind == kind {
                out[usize::from(entry.level)] += 1;
            }
        }
        out
    }

    /// Entries held, for the prune test.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
#[path = "repair_ledger_tests.rs"]
mod tests;
