//! The register of a logical name as a function of its FACTS: what a member must answer for a set of facts about one name and its
//! database, whatever order they reached it in.
//!
//! The function has two layers. **Layer 2** is [`crate::register_state::Timeline`]: register EVENTS, and what each physical is, to what
//! is served, buried, doomed and superseded. **Layer 1 is this module**: from the facts to the events (which facts are history, which
//! drops are part of the name's swap history, the database's drop as an event of its own kind, which stages it ended), and the rules
//! of a collection that was never swapped. What an event makes visible, and what that buries, is layer 2's alone: this module has no
//! rule of its own for it. What it ends itself is what a drop of the database ends.
//!
//! The definition it must agree with is `u14_fold.py`, which applies the facts one at a time in stamp order;
//! `tests/register_function/table.rs` holds its answers for generated sets of facts and the test replays them; `tie_rows.rs` and
//! `tie_table.rs` beside it hold the answers for facts at one `hlc`. The answer on a set that no cluster can write is not specified
//! (a binding of a stage that was not created below it, a recreation with no stage created below it, a touch of a name that was never
//! created).
//!
//! Nothing in storage reads this yet.
//!
//! **Two facts at one `hlc`** (two origins can mint them). The facts are applied in full [`Stamp`] order, and "below" between two facts
//! is that order: a drop is logical when a stage's creation sorts below it, and two events at one `hlc` are both in the register. Three
//! readings are on the `hlc` alone. A drop against the creation of a collection it ends, and the floor against a touch: **the tie is
//! the drop's** (ADR-236). An ordinary creation or a touch against the register: an event at or below it by `hlc` makes it history,
//! since a visible event buries the identity whichever of the two sorts first. And a stage's deadline is an `Hlc`.
//!
//! **A drop of the database** ends every stage and recreation created at or below it, and the collection named like the logical name.
//! That too is a drop against the creation of what it ends, so it is read on the `hlc`: it ends them whichever of two stamps at one
//! `hlc` sorts first. It is an event of the register when a stage of the name was created at or below it, asked of every stage of the
//! set. A binding or a recreation whose physical a database drop ended, with that drop below it in [`Stamp`] order, is a drop at its
//! own stamp: layer 2 is given a logical drop there, and this module says itself that a write to the ended physical above that stamp
//! is late.

use std::collections::{BTreeMap, BTreeSet};

use crate::hlc::{Hlc, Stamp};
use crate::oplog::OpKind;
use crate::register_state::{Put, Ready, Timeline, Visible, valid_binding};
use crate::swap::{RegisterEvent, StageInfo, StageMeta};

/// One fact about a logical name or its database, under the stamp of the entry that made it. No two facts of a set share a stamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FoldFact {
    pub stamp: Stamp,
    pub kind: FoldKind,
}

/// What a [`FoldFact`] says. A stage's withdrawal by its creator is not one: it has no rule yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FoldKind {
    /// An ordinary creation of the collection.
    Create,
    /// A drop of the collection. With `database_drop` it is a database drop's own entry of this collection, which acts as of that
    /// database drop: it is in the set and changes nothing.
    Drop { database_drop: Option<Hlc> },
    /// A drop of the name's database.
    DatabaseDrop,
    /// The creation of a stage of the name.
    StageCreate { physical: String, stage: StageInfo },
    /// A binding of the name to a stage, valid or not: [`crate::register_state::valid_binding`] judges it against the stage's creation.
    Bind { physical: String },
    /// A recreation of a once-swapped name, as the new physical.
    Recreate { physical: String },
    /// Something of the collection: an entry of one of the kinds [`is_a_touch`] names. **The caller must hold a creation of the name
    /// before it folds a touch:** with no creation and no floor a touch makes nothing, so a member that knows a creation was seen,
    /// although it no longer holds the entry, supplies the [`FoldKind::Create`] fact itself.
    Touch,
}

/// An event of the name's register, as the function names it. The database's drop is a kind of its own here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FoldEvent {
    Bind {
        physical: String,
    },
    Recreate {
        physical: String,
    },
    /// A logical drop of the name.
    Drop,
    /// A drop of the name's database that is part of the name's swap history.
    DatabaseDrop,
}

/// What the name serves.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Serves {
    #[default]
    Absent,
    /// The collection named like the logical name.
    Identity,
    Stage(String),
    Recreation(String),
}

/// What the function answers for a set of facts, with every physical ready unless the function itself ends it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Answer {
    pub serves: Serves,
    /// The newest drop of the name, of the collection or of its database, and never a database drop's own entry of the collection:
    /// whether or not the identity stands. A standing identity's life follows it.
    pub floor: Option<Stamp>,
    /// Whether the collection named like the logical name stands. It carries no creation stamp: that is each member's own.
    pub identity_stands: bool,
    /// The stages and recreations left alive, each with the `hlc` of its creation.
    pub alive: BTreeMap<String, Hlc>,
    /// The stages and recreations that are ended, each with the stamp at which a member that applied the facts in stamp order buries
    /// it. The identity is not listed: the stamp it ends at is each member's own.
    pub dead: BTreeMap<String, Stamp>,
    /// The events in effect, oldest first.
    pub register: Vec<(Stamp, FoldEvent)>,
    /// For the identity and each stage and recreation of the facts: the stamp above which a write to it is late, if any.
    pub superseded: BTreeMap<String, Option<Stamp>>,
    /// The stamps of the facts that are history by the pure rules: never in effect, never visible, burying nothing.
    pub invalid: BTreeSet<Stamp>,
}

impl Answer {
    /// Whether a write stamped `write` addressed to `physical` is late, as
    /// [`crate::register_state::Timeline::is_late`] compares it.
    pub fn is_late(&self, physical: &str, write: &Stamp) -> bool {
        self.superseded.get(physical).copied().flatten().is_some_and(|event| *write > event)
    }
}

/// The answer for the set of `facts` about the logical name `identity` and its database. The order of the slice says nothing: the
/// facts are applied in stamp order.
pub fn fold(identity: &str, facts: &[FoldFact]) -> Answer {
    let mut in_order: Vec<&FoldFact> = facts.iter().collect();
    in_order.sort_by_key(|fact| fact.stamp);
    // every stage of the set, by its physical: a binding is judged against its own, and a drop asks whether one was created below it
    let stages: BTreeMap<&str, StageMeta> = facts
        .iter()
        .filter_map(|fact| match &fact.kind {
            FoldKind::StageCreate { physical, stage } => {
                Some((physical.as_str(), StageMeta { created: fact.stamp, info: *stage }))
            }
            _ => None,
        })
        .collect();
    // every drop of the database in the set, in stamp order: one can end a physical whose creation it sorts below
    let database_drops: Vec<Stamp> = in_order
        .iter()
        .filter(|fact| matches!(fact.kind, FoldKind::DatabaseDrop))
        .map(|fact| fact.stamp)
        .collect();
    let mut name = Name {
        stages: &stages,
        database_drops: &database_drops,
        timeline: Timeline::new(identity),
        plain: Plain::default(),
        alive: BTreeMap::new(),
        dead: BTreeMap::new(),
        invalid: BTreeSet::new(),
        register: BTreeMap::new(),
        ended_when_named: BTreeMap::new(),
    };
    for fact in in_order {
        match &fact.kind {
            FoldKind::Create => name.created(&fact.stamp),
            FoldKind::Touch => name.touched(&fact.stamp),
            FoldKind::Drop { database_drop: None } => name.dropped(fact.stamp),
            FoldKind::DatabaseDrop => name.database_dropped(fact.stamp),
            // a database drop's own entry of the collection acts as of that database drop, which is a fact of its own
            FoldKind::Drop { database_drop: Some(_) } => {}
            FoldKind::StageCreate { physical, .. } => name.stage_created(physical, &fact.stamp),
            FoldKind::Bind { physical } => name.bound(physical, fact.stamp),
            FoldKind::Recreate { physical } => name.recreated(physical, fact.stamp),
        }
    }
    name.answer(facts)
}

/// Whether the drop of the database stamped `drop` ends what was created at `created`: read on the `hlc`, so that a tie is the
/// drop's (ADR-236), whichever of the two stamps sorts first.
fn ends(drop: &Stamp, created: &Stamp) -> bool {
    created.hlc <= drop.hlc
}

/// What the facts applied so far say of a name. Which events there are is decided here; what they make visible and what that buries
/// is layer 2's to say.
struct Name<'a> {
    /// Every stage of the set, by its physical, applied or not.
    stages: &'a BTreeMap<&'a str, StageMeta>,
    /// Every drop of the database in the set, oldest first, applied or not.
    database_drops: &'a [Stamp],
    /// The events in effect.
    timeline: Timeline,
    plain: Plain,
    /// The stages and recreations applied and not ended, each with the `hlc` of its creation.
    alive: BTreeMap<String, Hlc>,
    dead: BTreeMap<String, Stamp>,
    invalid: BTreeSet<Stamp>,
    /// What each event in effect is, by its stamp: layer 2 holds a database drop, and a binding or a recreation that is a drop at its
    /// own stamp, as logical drops.
    register: BTreeMap<Stamp, FoldEvent>,
    /// The physicals a database drop had ended when a binding or a recreation named them, each with that event's stamp: a write to one
    /// above it is late, and layer 2, which was given a logical drop there, cannot say so.
    ended_when_named: BTreeMap<String, Stamp>,
}

impl Name<'_> {
    /// Whether the register holds an event stamped at or below `stamp` by its `hlc`: what is stamped there is of a swapped name.
    fn has_an_event_at_or_below(&self, stamp: &Stamp) -> bool {
        self.timeline.events().iter().any(|(event, _)| event.hlc <= stamp.hlc)
    }

    /// An ordinary creation. With an event of the register at or below it, it is history: a once-swapped name is created again only
    /// by a recreation. Otherwise the rules of the plain collection judge it.
    fn created(&mut self, stamp: &Stamp) {
        if !self.has_an_event_at_or_below(stamp) {
            self.plain.created(stamp);
        }
    }

    /// A touch. With an event of the register at or below it, it changes nothing in what the name is: whether the write is late is
    /// [`Answer::superseded`]'s to say.
    fn touched(&mut self, stamp: &Stamp) {
        if self.has_an_event_at_or_below(stamp) {
            return;
        }
        self.plain.touched(stamp);
    }

    /// A drop of the collection. It is logical, an event of the register, when an event is held or a stage of the name was created
    /// below it, by the full stamp; otherwise it ends the plain collection. The floor rises either way.
    fn dropped(&mut self, stamp: Stamp) {
        let logical = !self.timeline.events().is_empty()
            || self.stages.values().any(|stage| stage.created < stamp);
        if logical {
            self.plain.raise_the_floor(stamp);
            self.enter(stamp, RegisterEvent::LogicalDrop, None, FoldEvent::Drop);
        } else {
            self.plain.dropped(stamp);
        }
    }

    /// A drop of the database: it ends every physical that stands and the collection named like the logical name, and raises the
    /// floor. It is an event of the register when a stage of the name was created at or below it.
    fn database_dropped(&mut self, stamp: Stamp) {
        for physical in std::mem::take(&mut self.alive).into_keys() {
            self.dead.insert(physical, stamp);
        }
        self.plain.dropped(stamp);
        let of_the_swap = self.stages.values().any(|stage| ends(&stamp, &stage.created));
        if of_the_swap {
            self.enter(stamp, RegisterEvent::LogicalDrop, None, FoldEvent::DatabaseDrop);
        }
    }

    /// The first drop of the database below `stamp` that ends what was created at `stamp`: only a drop at the creation's own `hlc`,
    /// sorting below it, can be one.
    fn ended_as_made(&self, stamp: &Stamp) -> Option<Stamp> {
        self.database_drops.iter().copied().find(|drop| ends(drop, stamp) && drop < stamp)
    }

    /// A physical made at `stamp`: it stands, unless a drop of the database already ended it.
    fn made(&mut self, physical: &str, stamp: &Stamp) {
        match self.ended_as_made(stamp) {
            Some(drop) => {
                self.dead.insert(physical.to_string(), drop);
            }
            None => {
                self.alive.insert(physical.to_string(), stamp.hlc);
            }
        }
    }

    /// The creation of a stage: it stands, hidden, until an event or a drop of the database ends it.
    fn stage_created(&mut self, physical: &str, stamp: &Stamp) {
        self.made(physical, stamp);
        self.settle();
    }

    /// A binding, valid or not: layer 2's one door judges it against its stage's creation. A valid one of a stage that a drop of the
    /// database ended, the drop below it, is a drop at its own stamp.
    fn bound(&mut self, physical: &str, stamp: Stamp) {
        let stage = self.stages.get(physical).copied();
        let label = FoldEvent::Bind { physical: physical.to_string() };
        let dead = stage.is_some_and(|stage| {
            valid_binding(&stamp, &stage.created, &stage.info)
                && self
                    .database_drops
                    .iter()
                    .any(|drop| ends(drop, &stage.created) && *drop < stamp)
        });
        if dead {
            self.enter_as_a_drop(physical, stamp, label);
        } else {
            self.enter(stamp, RegisterEvent::Bind { physical: physical.to_string() }, stage, label);
        }
    }

    /// A recreation: the new physical stands, and its event is in effect. One that a drop of the database ended as it was made is a
    /// drop at its own stamp.
    fn recreated(&mut self, physical: &str, stamp: Stamp) {
        self.made(physical, &stamp);
        let label = FoldEvent::Recreate { physical: physical.to_string() };
        let dead = self.ended_as_made(&stamp).is_some();
        if dead {
            self.enter_as_a_drop(physical, stamp, label);
        } else {
            let event = RegisterEvent::Create { physical: physical.to_string() };
            self.enter(stamp, event, None, label);
        }
    }

    /// Enter the binding or recreation of an ended physical: layer 2 is given a logical drop at its stamp, and a write to the
    /// physical above that stamp is late.
    fn enter_as_a_drop(&mut self, physical: &str, stamp: Stamp, label: FoldEvent) {
        self.ended_when_named.entry(physical.to_string()).or_insert(stamp);
        self.enter(stamp, RegisterEvent::LogicalDrop, None, label);
    }

    /// Offer an event to the register through layer 2's one door, and apply what is then visible.
    fn enter(
        &mut self,
        stamp: Stamp,
        event: RegisterEvent,
        stage: Option<StageMeta>,
        label: FoldEvent,
    ) {
        let stage = stage.as_ref().map(|stage| (&stage.created, &stage.info));
        match self.timeline.insert(stamp, event, stage) {
            Put::Stored => {
                self.register.insert(stamp, label);
                self.settle();
            }
            Put::InvalidBinding => {
                self.invalid.insert(stamp);
            }
            // only on a set no cluster can write: a binding of a stage that is not among the facts, or two facts under one stamp
            Put::UnknownStage | Put::Duplicate => {}
        }
    }

    /// What `physical` is to layer 2: alive if it stands. The function ends physicals and never leaves one missing.
    fn ready(&self, physical: &str) -> Ready {
        let stands = if physical == self.timeline.identity() {
            self.plain.stands
        } else {
            self.alive.contains_key(physical)
        };
        if stands { Ready::Alive } else { Ready::Buried }
    }

    /// End what layer 2 says the visible event buries: what it left behind, and the unbound stages whose deadline is below it.
    fn settle(&mut self) {
        let visible = self.timeline.visible(|physical| self.ready(physical));
        let deadlines: Vec<(&str, Hlc)> = self
            .stages
            .iter()
            .map(|(physical, stage)| (*physical, stage.info.effective_deadline(&stage.created)))
            .collect();
        let mut buried = self.timeline.burials(&visible, |physical| self.ready(physical));
        let doomed = self.timeline.doomed(&visible, |physical| self.ready(physical), &deadlines);
        buried.extend(doomed);
        for burial in buried {
            if burial.physical == self.timeline.identity() {
                self.plain.stands = false;
            } else if self.alive.remove(&burial.physical).is_some() {
                self.dead.insert(burial.physical, burial.at);
            }
        }
    }

    /// The answer, once every fact of `facts` is applied.
    fn answer(self, facts: &[FoldFact]) -> Answer {
        let serves = match self.timeline.visible(|physical| self.ready(physical)) {
            Visible::Identity if self.plain.stands => Serves::Identity,
            Visible::Identity | Visible::Dropped { .. } => Serves::Absent,
            Visible::Bound { physical, .. } => Serves::Stage(physical),
            Visible::Created { physical, .. } => Serves::Recreation(physical),
        };
        let recreations = facts.iter().filter_map(|fact| match &fact.kind {
            FoldKind::Recreate { physical } => Some(physical.as_str()),
            _ => None,
        });
        let superseded = [self.timeline.identity()]
            .into_iter()
            .chain(self.stages.keys().copied())
            .chain(recreations)
            .map(|physical| {
                let ended = self.ended_when_named.get(physical).copied();
                (physical.to_string(), ended.or_else(|| self.timeline.superseded_at(physical)))
            })
            .collect();
        Answer {
            serves,
            floor: self.plain.floor,
            identity_stands: self.plain.stands,
            alive: self.alive,
            dead: self.dead,
            register: self.register.into_iter().collect(),
            superseded,
            invalid: self.invalid,
        }
    }
}

/// The collection named like the logical name, and the rules of one that was never swapped. Each rule compares a fact with the floor on
/// the `hlc` alone: **a tie in one `hlc` is the drop's** (ADR-236), whichever of the two stamps sorts first.
#[derive(Default)]
struct Plain {
    /// The newest drop applied: of the collection or of its database.
    floor: Option<Stamp>,
    stands: bool,
}

impl Plain {
    /// Whether something stamped `stamp` is of a life a drop ended.
    fn is_at_or_below_the_floor(&self, stamp: &Stamp) -> bool {
        self.floor.is_some_and(|floor| stamp.hlc <= floor.hlc)
    }

    /// An ordinary creation: history at or below the floor, and otherwise the collection stands.
    fn created(&mut self, stamp: &Stamp) {
        if !self.is_at_or_below_the_floor(stamp) {
            self.stands = true;
        }
    }

    /// A drop raises the floor, whether it ends the plain collection or is an event of the register.
    fn raise_the_floor(&mut self, stamp: Stamp) {
        self.floor = Some(stamp);
    }

    /// A drop of a name that was never swapped: the floor rises to it and the collection, whose life is at or below it, ends.
    fn dropped(&mut self, stamp: Stamp) {
        self.raise_the_floor(stamp);
        self.stands = false;
    }

    /// A touch above a floor makes a collection that does not stand stand again. With no floor it makes nothing: a write never makes a
    /// collection that no creation and no drop speaks of.
    fn touched(&mut self, stamp: &Stamp) {
        if self.floor.is_some() && !self.is_at_or_below_the_floor(stamp) {
            self.stands = true;
        }
    }
}

/// Whether an oplog entry of `kind` is a TOUCH of its collection ([`FoldKind::Touch`]): a document write or a delete, or a change of
/// its definitions. A loss record is not one. The match names every kind, so that a new kind does not compile until it is placed.
#[allow(clippy::match_like_matches_macro)]
pub fn is_a_touch(kind: OpKind) -> bool {
    match kind {
        OpKind::Insert
        | OpKind::Update
        | OpKind::Replace
        | OpKind::Delete
        | OpKind::CreateIndex
        | OpKind::DropIndex
        | OpKind::ConfigureValidator
        | OpKind::ConfigureVectors => true,
        OpKind::Collection
        | OpKind::UniqueViolation
        | OpKind::CreateCollection
        | OpKind::DropCollection
        | OpKind::UnkeyedDocument
        | OpKind::LossRecord
        | OpKind::BindCollection
        | OpKind::AbandonStage
        | OpKind::OutLossRecord => false,
    }
}
