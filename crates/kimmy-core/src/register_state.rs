//! The register of a logical name, as a pure function (ADR-272, decisions 3, 5, 8, 9 and 7).
//!
//! A replacing `$out` binds a logical name to a physical collection with one replicated entry. What a
//! member serves for the name, which physicals it buries, which document writes are late and whether it
//! refuses writes are all decided here, from the register events **in effect on the member** and the
//! readiness of the physicals it holds, and from nothing else: no storage, no clock, no I/O. The caller
//! decides which events are in effect (rule B: an event takes effect only once the member covers it,
//! [`covers_in_position`]; a binding is valid only at or below its stage's deadline,
//! [`valid_binding`]) and tells the function what each physical is here ([`Ready`]).
//!
//! The function is monotone, which is why the members converge without a leader: more events, or a
//! physical that becomes ready, move what the name serves only to newer stamps; a physical once buried
//! stays buried; a write once late stays late. The same set of events gives the same answers whatever
//! the order they arrived in, because the answers are functions of the set.
//!
//! The model it must agree with is `swapmodel6g.py` (`newest_applicable`, `enter`, `sweep`,
//! `first_superseding`); `tests/register_state/table.rs` holds the model's answers for generated
//! scenarios and the test replays them.

use crate::hlc::{Hlc, Stamp};
use crate::swap::{RegisterEvent, StageInfo};
use crate::version::VersionVector;

/// The longest a replacing `$out` may take, as the protocol states it: the writer's budget for a stage is
/// its request's remaining time, and never more than this. A protocol constant, not configuration, because
/// every member must read the same deadline out of a creation entry whatever its own configuration says.
/// ADR-266's request deadline has no hard maximum; this is the one the swap states for itself.
pub const MAX_OUT_DEADLINE_MS: u64 = 3_600_000;

/// The clock skew a stage's deadline may carry beyond [`MAX_OUT_DEADLINE_MS`], as a protocol constant: the
/// default of `storage.max_clock_skew_secs`. Configuration cannot enter it, for the reason above.
pub const OUT_DEADLINE_SKEW_MS: u64 = 300_000;

impl StageInfo {
    /// The deadline a member reads out of a stage's creation entry, whatever the entry says: a deadline below
    /// the creation's own `hlc` reads as the creation's (a binding of the stage is then valid only at the
    /// creation's `hlc`), and one past the creation plus [`MAX_OUT_DEADLINE_MS`] plus
    /// [`OUT_DEADLINE_SKEW_MS`] reads as that. A clamp and never a refusal: a replicated entry that is refused
    /// stalls the batch, and the clamp is a pure function of the entry, so every member computes the same value.
    pub fn effective_deadline(&self, creation: &Stamp) -> Hlc {
        let cap = Hlc::new(
            creation.hlc.wall_ms.saturating_add(MAX_OUT_DEADLINE_MS + OUT_DEADLINE_SKEW_MS),
            0,
        );
        if self.deadline < creation.hlc {
            creation.hlc
        } else if self.deadline > cap {
            cap
        } else {
            self.deadline
        }
    }
}

/// Whether a binding stamped `bind` of the stage created by `creation` is valid: stamped at or below the stage's
/// effective deadline. A binding above it is invalid, history on every member, by a pure function of the stamps.
pub fn valid_binding(bind: &Stamp, creation: &Stamp, stage: &StageInfo) -> bool {
    bind.hlc <= stage.effective_deadline(creation)
}

/// Whether a member that has witnessed `witnessed` covers the entry stamped `stamp`: it has witnessed its origin
/// (node and epoch) up to the entry's `hlc`. The witnessed vector counts what the member received and what it
/// witnessed through a sender's skip rule, and the entries a completed snapshot's grant covers; the predicate
/// reads it and nothing else. A register event takes effect on a member only once the member covers it.
pub fn covers_in_position(witnessed: &VersionVector, stamp: &Stamp) -> bool {
    witnessed.get(stamp.origin()) >= stamp.hlc
}

/// What a physical collection is on this member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ready {
    /// Held, alive and complete: it can be served.
    Alive,
    /// Held, but the pages that make it complete have not all landed (a join in progress): not served yet.
    Missing,
    /// Buried, or never held here.
    Buried,
}

/// When a physical was born, by the register.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Birth {
    /// No event of the register names it.
    Unknown,
    /// The identity: the physical named like the logical name, bound at minus infinity.
    Identity,
    /// The stamp of the earliest event that names it.
    At(Stamp),
}

/// What the name serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Visible {
    /// No register event is in effect and ready: the identity serves.
    Identity,
    /// A binding is visible: the name answers from `physical`.
    Bound { physical: String, stamp: Stamp },
    /// A recreation is visible: the name answers from the new `physical`.
    Created { physical: String, stamp: Stamp },
    /// A logical drop is visible: the name is absent.
    Dropped { stamp: Stamp },
}

impl Visible {
    /// The stamp of the event that is visible, or `None` for the identity.
    pub fn stamp(&self) -> Option<&Stamp> {
        match self {
            Self::Identity => None,
            Self::Bound { stamp, .. } | Self::Created { stamp, .. } | Self::Dropped { stamp } => {
                Some(stamp)
            }
        }
    }

    /// The physical the name serves, if it serves one.
    pub fn physical<'a>(&'a self, identity: &'a str) -> Option<&'a str> {
        match self {
            Self::Identity => Some(identity),
            Self::Bound { physical, .. } | Self::Created { physical, .. } => Some(physical),
            Self::Dropped { .. } => None,
        }
    }
}

/// The burial of a physical a visible event has left behind (decision 5): unconditional, never kept, never
/// re-dated, and stamped by the event that buried it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Burial {
    pub physical: String,
    pub at: Stamp,
}

/// Why a member refuses writes to the name (decision 7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    /// The event that supersedes the physical the member serves.
    pub event: Stamp,
    /// The event is known but not yet in effect (parked ahead of its position).
    pub parked: bool,
}

/// What [`Timeline::insert`] did with an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Put {
    /// The event is now in effect.
    Stored,
    /// An event under the same stamp was already in effect; nothing changed.
    Duplicate,
    /// A binding stamped above its stage's (clamped) deadline: history on every member, never stored.
    InvalidBinding,
    /// A binding of a stage whose creation the caller does not know: never stored. On every route the stage's creation
    /// comes before the binding that names it.
    UnknownStage,
}

/// The register events of one logical name that are in effect on a member, with the identity physical.
///
/// A binding gets in only with its stage's creation, so that it can be judged by [`valid_binding`]: there is no way to put an
/// invalid binding in effect, and so none to make one visible (the partial flip the design exists to prevent).
#[derive(Clone, Debug)]
pub struct Timeline {
    identity: String,
    events: Vec<(Stamp, RegisterEvent)>,
}

impl Timeline {
    /// An empty register for the name `identity` (the physical named like the logical name).
    pub fn new(identity: impl Into<String>) -> Self {
        Self { identity: identity.into(), events: Vec::new() }
    }

    /// The name's identity physical.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Put an event in effect. A binding is judged first, by [`valid_binding`] against `stage`, the creation stamp and
    /// [`StageInfo`] of the stage it names: with no stage it is [`Put::UnknownStage`], above the stage's deadline it is
    /// [`Put::InvalidBinding`], and neither is stored. `stage` is not read for a recreation or a logical drop. Idempotent:
    /// an event under a stamp already held changes nothing and is [`Put::Duplicate`].
    pub fn insert(
        &mut self,
        stamp: Stamp,
        event: RegisterEvent,
        stage: Option<(&Stamp, &StageInfo)>,
    ) -> Put {
        if let RegisterEvent::Bind { .. } = &event {
            match stage {
                None => return Put::UnknownStage,
                Some((creation, info)) if !valid_binding(&stamp, creation, info) => {
                    return Put::InvalidBinding;
                }
                Some(_) => {}
            }
        }
        match self.events.binary_search_by(|(held, _)| held.cmp(&stamp)) {
            Ok(_) => Put::Duplicate,
            Err(at) => {
                self.events.insert(at, (stamp, event));
                Put::Stored
            }
        }
    }

    /// The events, oldest first.
    pub fn events(&self) -> &[(Stamp, RegisterEvent)] {
        &self.events
    }

    /// When `physical` was born.
    pub fn birth(&self, physical: &str) -> Birth {
        if physical == self.identity {
            return Birth::Identity;
        }
        self.events
            .iter()
            .find(|(_, event)| event.physical() == Some(physical))
            .map_or(Birth::Unknown, |(stamp, _)| Birth::At(*stamp))
    }

    /// What the name serves: the newest event in effect whose physical is ready here (decision 5). A binding or
    /// a recreation whose physical is missing or buried is skipped, never made visible; a logical drop is always
    /// applicable. With no such event, the identity.
    pub fn visible(&self, ready: impl Fn(&str) -> Ready) -> Visible {
        for (stamp, event) in self.events.iter().rev() {
            match event {
                RegisterEvent::LogicalDrop => return Visible::Dropped { stamp: *stamp },
                RegisterEvent::Bind { physical } if ready(physical) == Ready::Alive => {
                    return Visible::Bound { physical: physical.clone(), stamp: *stamp };
                }
                RegisterEvent::Create { physical } if ready(physical) == Ready::Alive => {
                    return Visible::Created { physical: physical.clone(), stamp: *stamp };
                }
                RegisterEvent::Bind { .. } | RegisterEvent::Create { .. } => {}
            }
        }
        Visible::Identity
    }

    /// The physicals `visible` buries (decision 5): each one held here (alive or missing), not the one served, born
    /// before the visible event, the identity counting as born at minus infinity. A physical no event names is
    /// never buried by this rule (a second writer's stage in flight). A visible identity buries nothing.
    pub fn burials(&self, visible: &Visible, held: impl Fn(&str) -> Ready) -> Vec<Burial> {
        let Some(at) = visible.stamp() else { return Vec::new() };
        let served = visible.physical(&self.identity);
        let mut candidates: Vec<&str> = vec![&self.identity];
        for (_, event) in &self.events {
            if let Some(physical) = event.physical()
                && !candidates.contains(&physical)
            {
                candidates.push(physical);
            }
        }
        candidates
            .into_iter()
            .filter(|physical| Some(*physical) != served)
            .filter(|physical| matches!(held(physical), Ready::Alive | Ready::Missing))
            .filter(|physical| match self.birth(physical) {
                Birth::Identity => true,
                Birth::At(born) => born < *at,
                Birth::Unknown => false,
            })
            .map(|physical| Burial { physical: physical.to_string(), at: *at })
            .collect()
    }

    /// The stages `visible` buries by a later event (decision 5, "burial by a later event"; cause `doomed`): a stage this member
    /// holds (alive or missing), that is not the one served, with **no binding in effect here**, whose effective deadline lies
    /// **strictly below** the visible event's `hlc`. Every valid binding of the stage is stamped at or below the deadline, so in
    /// every order of arrival a binding of it is older than the visible event and the stage can never be, or again be, the
    /// visible physical: the burial waits for nothing. `stages` is each stage this member knows the creation of, with its
    /// effective deadline ([`StageInfo::effective_deadline`]). A stage with a binding in effect is the loser rule's
    /// ([`Self::burials`], D4 before DOOM), and a binding of a doomed stage that arrives later still takes effect as an event
    /// (it sets the stage's birth and makes writes late) and is never visible. The burial is stamped by the visible event,
    /// as the loser rule's is. A pure function of the events in effect, the stages' deadlines and what is held.
    pub fn doomed(
        &self,
        visible: &Visible,
        held: impl Fn(&str) -> Ready,
        stages: &[(&str, Hlc)],
    ) -> Vec<Burial> {
        let Some(at) = visible.stamp() else { return Vec::new() };
        let served = visible.physical(&self.identity);
        stages
            .iter()
            .filter(|(physical, _)| Some(*physical) != served)
            .filter(|(physical, _)| matches!(held(physical), Ready::Alive | Ready::Missing))
            .filter(|(physical, _)| self.birth(physical) == Birth::Unknown)
            .filter(|(_, deadline)| at.hlc > *deadline)
            .map(|(physical, _)| Burial { physical: physical.to_string(), at: *at })
            .collect()
    }

    /// The stamp of the earliest event in effect that supersedes `physical`: above its birth, and a logical drop or
    /// naming another physical (decision 8). `None` if there is none or the physical's birth is unknown.
    pub fn superseded_at(&self, physical: &str) -> Option<Stamp> {
        self.supersedes(&self.events, physical)
    }

    /// Whether a write stamped `write` addressed to `physical` is late (decision 8): some event in effect supersedes
    /// the physical between its birth and the write.
    pub fn is_late(&self, physical: &str, write: &Stamp) -> bool {
        self.superseded_at(physical).is_some_and(|event| *write > event)
    }

    /// Why a member serving `served` refuses writes (decision 7): an event in effect supersedes it, or, failing
    /// that, the earliest event of `parked` (known, ahead of its position) that would.
    pub fn refusal(&self, parked: &Timeline, served: &str) -> Option<Refusal> {
        if let Some(event) = self.superseded_at(served) {
            return Some(Refusal { event, parked: false });
        }
        self.supersedes(&parked.events, served).map(|event| Refusal { event, parked: true })
    }

    /// The earliest of `events` above `physical`'s birth that is a logical drop or names another physical.
    fn supersedes(&self, events: &[(Stamp, RegisterEvent)], physical: &str) -> Option<Stamp> {
        let birth = self.birth(physical);
        events
            .iter()
            .find(|(stamp, event)| {
                let above = match birth {
                    Birth::Unknown => return false,
                    Birth::Identity => true,
                    Birth::At(born) => *stamp > born,
                };
                above && event.physical() != Some(physical)
            })
            .map(|(stamp, _)| *stamp)
    }
}
