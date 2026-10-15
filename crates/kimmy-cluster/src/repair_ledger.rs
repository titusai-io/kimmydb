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
//! * **R1** a transient end changes nothing but the planner's cooldown, until three of them
//!   follow one another for one key: that climbs one level, so a failure that repeats for
//!   ever backs off instead of costing a pull at every cooldown;
//! * **R2** a repair is judged at the first *compared* probe after its end, and
//!   only there; one never judged by its deadline counts as transient, but every
//!   second consecutive such end climbs one step;
//! * **R3** an ineffective repair backs off, 5 min, 15 min, 45 min, 2 h 15 min,
//!   6 h, and one that made progress lowers the level to 1;
//! * **R4** more than three episodes of clear-and-recur in a rolling hour stop
//!   the collection (per kind), and the stop expires; only episodes of *different peers*
//!   within the window count as one;
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

/// The longest the de-duplication window of R4 is, whatever the member's collection count: the
/// window is three rotation periods, and a rotation is a tick per held collection, so on a
/// member with many collections three rotations would be longer than the rolling hour the
/// episodes are counted in.
pub const MAX_DEDUPE: Duration = Duration::from_secs(15 * 60);

/// Consecutive transient ends of one key after which it climbs one level (R1).
pub const TRANSIENT_CLIMB_EVERY: u32 = 3;

impl Params {
    fn deadline(self) -> Duration {
        (self.rotation * 3).max(MIN_PROBE_DEADLINE)
    }

    fn dedupe(self) -> Duration {
        (self.rotation * 3).min(MAX_DEDUPE)
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
    /// Cancelled while queued, before it started.
    Cancelled,
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
    /// A third transient end in a row for one key.
    TransientClimb {
        level: u8,
        not_before: Duration,
    },
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
    /// How many episodes the rolling hour holds, this one included.
    pub episodes: usize,
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
    /// Consecutive repairs that ended transient (R1).
    transient_run: u32,
}

impl Entry {
    fn new() -> Self {
        Entry {
            level: 0,
            not_before: Duration::ZERO,
            phase: Phase::Idle,
            unjudged_run: 0,
            transient_run: 0,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct CollectionState {
    /// Counted episodes, oldest first, with the peer each was of.
    episodes: Vec<(Duration, NodeId)>,
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
    /// A `(peer, collection, kind)` whose repair cleared the finding, so the next
    /// confirmation of it is a recurrence (R4). The value is when it cleared. Keyed by kind: a
    /// digest repair that cleared its finding is no episode of a definition finding that stands.
    armed: BTreeMap<Key, Duration>,
    /// R4's state, per collection and kind.
    collections: BTreeMap<(CollectionId, LedgerKind), CollectionState>,
}

impl Default for RepairLedger {
    /// A five-minute rotation, until the caller says what the tick's is
    /// ([`RepairLedger::set_rotation`]).
    fn default() -> Self {
        Self::new(Params { rotation: Duration::from_secs(300) })
    }
}

impl RepairLedger {
    pub fn set_rotation(&mut self, rotation: Duration) {
        self.params.rotation = rotation;
    }

    /// How many collections are stopped for recurring at `now`, for one kind (the gauge).
    pub fn stopped_collections(&mut self, kind: LedgerKind, now: Duration) -> usize {
        let ids: Vec<_> =
            self.collections.keys().filter(|(_, k)| *k == kind).map(|(id, _)| *id).collect();
        ids.into_iter().filter(|id| self.is_stopped(*id, kind, now)).count()
    }

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
        self.rearm(key.collection, key.kind, now);
        if let Some(until) =
            self.collections.get(&(key.collection, key.kind)).and_then(|c| c.stopped_until)
        {
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
    pub fn ended(&mut self, key: Key, now: Duration, end: End) -> Option<Verdict> {
        let deadline = self.params.deadline();
        let entry = self.entries.get_mut(&key)?;
        if entry.phase != Phase::InFlight {
            return None;
        }
        match end {
            End::Transient(_) => {
                entry.phase = Phase::Idle;
                entry.transient_run += 1;
                // A failure that repeats for ever backs off: the third in a row climbs.
                if entry.transient_run.is_multiple_of(TRANSIENT_CLIMB_EVERY) {
                    entry.level = (entry.level + 1).min(MAX_LEVEL);
                    entry.not_before = now + BACKOFF[usize::from(entry.level) - 1];
                    return Some(Verdict::TransientClimb {
                        level: entry.level,
                        not_before: entry.not_before,
                    });
                }
            }
            End::Finished { progress } => {
                entry.transient_run = 0;
                entry.phase = Phase::Awaiting { ended_at: now, deadline: now + deadline, progress };
            }
        }
        None
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
        entry.transient_run = 0;
        Some(match judged {
            Judged::Effective => {
                entry.level = 0;
                entry.not_before = Duration::ZERO;
                self.armed.insert(key, now);
                Verdict::Effective
            }
            Judged::Ineffective => {
                entry.level = if progress { 1 } else { (entry.level + 1).min(MAX_LEVEL) };
                entry.not_before = ended_at + BACKOFF[usize::from(entry.level) - 1];
                Verdict::Ineffective { level: entry.level, not_before: entry.not_before }
            }
        })
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
        let state = self.collections.entry((key.collection, key.kind)).or_default();
        state.last_confirmed = Some(now);
        state.episodes.retain(|(at, _)| now.saturating_sub(*at) < EPISODE_WINDOW);
        let held = state.episodes.len();
        if self.armed.remove(&key).is_none() {
            return Confirmed { episodes: held, ..Confirmed::default() };
        }
        // An episode of a different peer, counted within the de-duplication window, is the
        // same episode: a partition that heals on five members yields one effective repair per
        // peer, and that is one recurrence. The same peer's recurrence always counts: it is a
        // loop, and a member with many collections (a long rotation) must still stop it.
        let same_event = state
            .episodes
            .iter()
            .any(|(at, peer)| *peer != key.peer && now.saturating_sub(*at) < dedupe);
        if same_event {
            return Confirmed { episode: false, newly_stopped: false, episodes: held };
        }
        state.episodes.push((now, key.peer));
        let mut newly_stopped = false;
        if state.episodes.len() > MAX_EPISODES && state.stopped_until.is_none() {
            state.stopped_until = Some(now + STOP_FOR);
            state.stopped_at = Some(now);
            newly_stopped = true;
        }
        Confirmed { episode: true, newly_stopped, episodes: state.episodes.len() }
    }

    /// R4: a stopped collection re-arms after [`STOP_FOR`], or once no finding
    /// against it has been confirmed for [`STOP_CLEAR_FOR`], whichever is first.
    fn rearm(&mut self, collection: CollectionId, kind: LedgerKind, now: Duration) {
        let Some(state) = self.collections.get_mut(&(collection, kind)) else {
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
    pub fn is_stopped(
        &mut self,
        collection: CollectionId,
        kind: LedgerKind,
        now: Duration,
    ) -> bool {
        self.rearm(collection, kind, now);
        self.collections.get(&(collection, kind)).is_some_and(|c| c.stopped_until.is_some())
    }

    /// R7: drop everything about a peer that is no longer a member (`members` is `None`
    /// when the node has no list) and a collection that is no longer held. A repair still in flight for a key
    /// that is dropped is dropped with it.
    pub fn retain(&mut self, held: &BTreeSet<CollectionId>, members: Option<&BTreeSet<NodeId>>) {
        let member = |peer: &NodeId| members.is_none_or(|members| members.contains(peer));
        self.entries.retain(|key, _| held.contains(&key.collection) && member(&key.peer));
        self.armed.retain(|key, _| held.contains(&key.collection) && member(&key.peer));
        self.collections.retain(|(collection, _), state| {
            state.episodes.retain(|(_, peer)| member(peer));
            held.contains(collection)
        });
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
