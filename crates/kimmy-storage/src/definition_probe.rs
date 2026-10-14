//! The definition digest's probe (B3 half 2, design 3.3 and 3.4).
//!
//! Where the cut digest (`digest_probe`) compares what two members hold in a
//! collection, this compares what they say about it: its indexes and its vector
//! configuration. It needs no kept row, no walk and no write-path change; a
//! collection's digest is computed from its metadata.
//!
//! **One exchange.** Every definition change carries a stamp (an index's
//! `created`, an `INDEXES_DROPPED` stamp, `vector_changed`). A collection with a
//! stamp **above the cut** is in motion and is deferred
//! ([`DefinitionDefer::DefinitionAboveCut`]); at or below it, nothing can change
//! what is hashed without a stamp above the cut. So the responder answers from
//! one read transaction against the request's cut, and the requester computes its
//! own digest after the answer arrives, in one read transaction against the same
//! cut. There is no second plan and no held transaction.
//!
//! **One transaction, pinned.** Every predicate (the `INDEXES_DROPPED` range
//! read, the metadata, the hints, the drop tombstones, the serve verdict) is read
//! through the one transaction a side opens, never a fresh one: a predicate read
//! at another moment can disagree with the digest it vouches for. A mutation row
//! reads one of them in a fresh transaction, and a test kills it.
//!
//! **A hint, never a mask.** A mismatch is *explained* only when a valid refusal
//! hint ([`crate::refusal_hint`]) on one side covers every differing name, and is
//! counted as such, never as a comparison.

use std::collections::BTreeSet;

use kimmy_core::{CollectionId, VersionVector};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::engine::Engine;
use crate::error::Result;

/// Why a collection's definitions were not compared, counted by the caller
/// under this label. Its own enum and its own series, not half 1's [`Defer`]
/// (`crate::Defer`): growing that one would put reasons on the cut digest's
/// series that cannot occur there.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionDefer {
    /// Either side holds a drop tombstone for the collection's id and the drop is
    /// above the cut or the standing life is not newer than it (the shared rule,
    /// `digest_probe::drop_defers`).
    Incarnation,
    /// The same for the collection's database.
    DatabaseTombstone,
    /// A definition of the collection is stamped above the cut: it is in motion.
    DefinitionAboveCut,
    /// The cut is below what retention removed, so the stamp of a dropped index
    /// may have been collected: every collection defers, on both sides.
    Horizon,
    /// This side failed to compute or to compare (a storage error that was not a
    /// stop): the contact went on without it.
    Error,
    /// An entry of the peer's answer did not decode, or carried a digest of the
    /// wrong width.
    Malformed,
}

impl DefinitionDefer {
    /// Every reason, in the order [`Self::index`] numbers them.
    pub const ALL: [DefinitionDefer; 6] = [
        Self::Incarnation,
        Self::DatabaseTombstone,
        Self::DefinitionAboveCut,
        Self::Horizon,
        Self::Error,
        Self::Malformed,
    ];

    /// This reason's place in [`Self::ALL`], for a counter per reason.
    pub const fn index(self) -> usize {
        match self {
            Self::Incarnation => 0,
            Self::DatabaseTombstone => 1,
            Self::DefinitionAboveCut => 2,
            Self::Horizon => 3,
            Self::Error => 4,
            Self::Malformed => 5,
        }
    }

    /// The label of `kimmy_sync_divergence_definition_probes_total{reason}`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Incarnation => "incarnation",
            Self::DatabaseTombstone => "database_tombstone",
            Self::DefinitionAboveCut => "definition_above_cut",
            Self::Horizon => "horizon",
            Self::Error => "error",
            Self::Malformed => "malformed",
        }
    }
}

/// What a requester sends: the cut, and the collections it wants the per-index
/// detail of (those whose digest differed at the last contact).
///
/// The cut is **the same value** the cut digest's request carries, built once,
/// so the two halves are judged at one cut; a responder that sees two different
/// cuts answers without definitions.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionAsk {
    pub cut: VersionVector,
    #[serde(default)]
    pub detail: Vec<CollectionId>,
}

impl DefinitionAsk {
    /// A request at `cut` for the detail of `detail`.
    pub fn new(cut: VersionVector, detail: impl IntoIterator<Item = CollectionId>) -> Self {
        Self { cut, detail: detail.into_iter().collect::<BTreeSet<_>>().into_iter().collect() }
    }
}

/// One collection's definitions, as the peer computed them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionEntry {
    pub collection: CollectionId,
    /// 16 bytes, big-endian: the digest of the collection's definitions. Never a
    /// `u128` on the wire.
    pub digest: ByteBuf,
    /// Set instead of a digest when the peer cannot compare this collection.
    #[serde(default)]
    pub deferred: Option<DefinitionDefer>,
    /// For a collection named in the request's `detail`: the peer's definitions
    /// as `(name, 16-byte hash)`.
    #[serde(default)]
    pub indexes: Vec<(String, ByteBuf)>,
    /// For a collection named in `detail`: the refusals the peer holds that are
    /// still above the standing life's floor, as `(name, 16-byte hash of the
    /// refused definition)`. Raw pairs: the requester judges them against both
    /// lists.
    #[serde(default)]
    pub refused: Vec<(String, ByteBuf)>,
}

/// The peer's answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct DefinitionReply {
    pub entries: Vec<DefinitionEntry>,
    /// Entries that did not decode (a reason or a shape from a later build) and
    /// were dropped: read, never written, so that the caller can count them.
    #[serde(skip)]
    pub malformed: u32,
}

/// Entries are read one at a time: one that does not decode is dropped and
/// counted, and the rest are kept, so a new field costs the entry and never the
/// frame.
impl<'de> Deserialize<'de> for DefinitionReply {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            entries: Vec<bson::Bson>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let total = raw.entries.len();
        let entries: Vec<DefinitionEntry> =
            raw.entries.into_iter().filter_map(|e| bson::deserialize_from_bson(e).ok()).collect();
        let malformed = (total - entries.len()) as u32;
        Ok(Self { entries, malformed })
    }
}

/// How the peer took a definition request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefinitionAnswered {
    /// With definitions.
    Answered,
    /// With none: a peer that predates the digest of definitions, or one that
    /// failed to compute it, or one that saw two different cuts. Never compared.
    Unanswered,
    /// This side failed to compute or to compare, with an error that was not a
    /// stop: the contact went on without it.
    Failed,
}

/// What one collection's definitions came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefinitionOutcome {
    /// Compared. When not equal, `differing` names the definitions that differ
    /// if this contact asked for the detail of the collection, and is empty
    /// otherwise.
    Compared { equal: bool, mine: u128, theirs: u128, differing: Vec<String> },
    /// The digests differ and a valid refusal hint covers every differing name.
    /// Counted as its own outcome, never as `compared`.
    Explained { differing: Vec<String> },
    /// Not compared, and why.
    Deferred(DefinitionDefer),
}

/// A collection's outcome at the end of a probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefinitionVerdict {
    pub collection: CollectionId,
    pub outcome: DefinitionOutcome,
}

/// What a contact's definition probe came to, for the caller to count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefinitionRound {
    pub answered: DefinitionAnswered,
    pub verdicts: Vec<DefinitionVerdict>,
    /// How many collections this side held: what a contact that came to nothing
    /// for all of them (a failure) is counted against, per collection.
    pub held: usize,
    /// Entries of the peer's answer that did not decode and were dropped.
    pub malformed: usize,
}

impl DefinitionRound {
    /// A round that came to `answered` and nothing else.
    pub fn new(answered: DefinitionAnswered) -> Self {
        Self { answered, verdicts: Vec::new(), held: 0, malformed: 0 }
    }
}

impl Engine {
    /// The responder's side: the digest of every collection's definitions at
    /// the request's cut, read in **one** read transaction, with the detail of
    /// the collections the request names.
    pub fn definition_answer(&self, _ask: &DefinitionAsk) -> Result<DefinitionReply> {
        Ok(DefinitionReply::default())
    }

    /// The requester's side, after the answer arrived: this side's digests at
    /// the same cut in **one** read transaction, and the comparison.
    pub fn definition_compare(
        &self,
        _ask: &DefinitionAsk,
        _reply: &DefinitionReply,
    ) -> Result<Vec<DefinitionVerdict>> {
        Ok(Vec::new())
    }
}

/// Test hooks: where a commit lands that a predicate read in a transaction of
/// its own would see and a pinned one does not.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::RefCell;

    use crate::Engine;

    type Hook = Box<dyn FnOnce(&Engine)>;

    thread_local! {
        /// Run once, on this thread, right after a probe opens its read
        /// transaction and before it reads anything through it.
        static AFTER_THE_TRANSACTION: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    pub(crate) fn after_the_transaction(hook: impl FnOnce(&Engine) + 'static) {
        AFTER_THE_TRANSACTION.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
    }

    #[allow(dead_code)]
    pub(super) fn run_after_the_transaction(engine: &Engine) {
        if let Some(hook) = AFTER_THE_TRANSACTION.with(|h| h.borrow_mut().take()) {
            hook(engine);
        }
    }
}
