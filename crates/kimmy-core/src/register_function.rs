//! The register of a logical name as a function of its FACTS: what a member must answer for a set of facts about one name and its
//! database, whatever order they reached it in.
//!
//! The function has two layers. **Layer 2** is [`crate::register_state::Timeline`]: register EVENTS, and what each physical is, to what
//! is served, buried, doomed and superseded. **Layer 1 is this module**: from the facts to the events (which facts are history, which
//! drops are part of the name's swap history, the database's drop as an event of its own kind, which stages it ended), and the rules
//! of a collection that was never swapped. It writes no visibility or burial rule of its own.
//!
//! The definition it must agree with is `u14_fold.py`, which applies the facts one at a time in stamp order;
//! `tests/register_function/table.rs` holds its answers for generated sets of facts and the test replays them. The answer on a set that
//! no cluster can write is not specified (a binding of a stage that was not created below it, a recreation with no stage created below
//! it, a touch of a name that was never created).
//!
//! Nothing in storage reads this yet.
//!
//! **Built so far:** the rules of a collection that was never swapped. [`fold`] does not read stage creations, bindings or recreations
//! yet, so the tests of the cases that hold one are red.

use std::collections::{BTreeMap, BTreeSet};

use crate::hlc::{Hlc, Stamp};
use crate::oplog::OpKind;
use crate::swap::StageInfo;

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
    let mut plain = Plain::default();
    for fact in in_order {
        match &fact.kind {
            FoldKind::Create => plain.created(&fact.stamp),
            // with no stage fact read, a drop of the database says of this name what a drop of the collection says
            FoldKind::Drop { database_drop: None } | FoldKind::DatabaseDrop => {
                plain.dropped(fact.stamp);
            }
            // a database drop's own entry of the collection acts as of that database drop, which is a fact of its own
            FoldKind::Drop { database_drop: Some(_) } => {}
            FoldKind::Touch => plain.touched(&fact.stamp),
            // not read yet
            FoldKind::StageCreate { .. } | FoldKind::Bind { .. } | FoldKind::Recreate { .. } => {}
        }
    }
    Answer {
        serves: if plain.stands { Serves::Identity } else { Serves::Absent },
        floor: plain.floor,
        identity_stands: plain.stands,
        // with no event in the register no write is late
        superseded: BTreeMap::from([(identity.to_string(), None)]),
        ..Answer::default()
    }
}

/// The collection named like the logical name, by the rules of one that was never swapped. Each rule compares a fact with the floor on
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

    /// A drop: the floor rises to it and the collection, whose life is at or below it, ends.
    fn dropped(&mut self, stamp: Stamp) {
        self.floor = Some(stamp);
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
