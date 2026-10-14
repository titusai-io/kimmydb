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

use std::collections::{BTreeMap, BTreeSet};

use kimmy_core::{CollectionId, Hlc, NodeId, Stamp, VersionVector};
use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::codec;
use crate::definition_digest::{
    DefinitionHash, DefinitionKey, DefinitionKind, FORMAT, definition_list,
};
use crate::digest_probe::drop_defers;
use crate::engine::{Engine, PairedShadows};
use crate::error::Result;
use crate::meta::CollectionMeta;
use crate::snapshot::ServeVerdict;
use crate::tables;

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
    /// The peer's answer is of another format of the digest (what is hashed, or
    /// how it is framed, is not what this build's is): the digests cannot be
    /// equal by accident, so none is compared, on any collection, for as long as
    /// the formats differ (a roll across a change of format).
    Format,
}

impl DefinitionDefer {
    /// Every reason, in the order [`Self::index`] numbers them.
    pub const ALL: [DefinitionDefer; 7] = [
        Self::Incarnation,
        Self::DatabaseTombstone,
        Self::DefinitionAboveCut,
        Self::Horizon,
        Self::Error,
        Self::Malformed,
        Self::Format,
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
            Self::Format => 6,
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
            Self::Format => "format",
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
    /// For a collection deferred as [`DefinitionDefer::DefinitionAboveCut`]: the
    /// origin of a stamp above the cut, so the requester can tell a stamp that
    /// is only in motion from one the cut can never cover (h2-N4).
    #[serde(default)]
    pub above: Option<NodeId>,
    /// For a collection named in the request's `detail`: the peer's definitions.
    #[serde(default)]
    pub definitions: Vec<WireDefinition>,
    /// For a collection named in `detail`: the refusals the peer holds that are
    /// still above the standing life's floor **and valid on its own side**, each
    /// with its kind. The requester applies that kind's shape against both lists.
    #[serde(default)]
    pub refused: Vec<WireRefusal>,
}

/// One definition on the wire: its kind, its name (empty for the vector
/// configuration) and its 16-byte hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDefinition {
    pub kind: DefinitionKind,
    #[serde(default)]
    pub name: String,
    pub hash: ByteBuf,
}

/// One refusal on the wire: the kind of hint it is (which says which shape its
/// validity takes), the name and the hash it stored.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireRefusal {
    pub hint: crate::refusal_hint::HintKind,
    #[serde(default)]
    pub name: String,
    pub hash: ByteBuf,
}

/// The peer's answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct DefinitionReply {
    /// The digest's format ([`FORMAT`]) the peer computed under. Absent (0) from
    /// a build that predates it, which is never equal to this one's.
    pub format: u8,
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
            format: u8,
            #[serde(default)]
            entries: Vec<bson::Bson>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let total = raw.entries.len();
        let entries: Vec<DefinitionEntry> =
            raw.entries.into_iter().filter_map(|e| bson::deserialize_from_bson(e).ok()).collect();
        let malformed = (total - entries.len()) as u32;
        Ok(Self { format: raw.format, entries, malformed })
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
    Compared { equal: bool, mine: u128, theirs: u128, differing: Vec<DefinitionKey> },
    /// The digests differ and a valid refusal hint covers every differing name.
    /// Counted as its own outcome, never as `compared`.
    Explained { differing: Vec<DefinitionKey> },
    /// Not compared, and why.
    Deferred(DefinitionDefer),
}

/// A collection's outcome at the end of a probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefinitionVerdict {
    pub collection: CollectionId,
    pub outcome: DefinitionOutcome,
    /// For a collection deferred as [`DefinitionDefer::DefinitionAboveCut`]: the
    /// origin of a stamp above the cut, and the cut's component for it. A stamp
    /// whose origin's component does not advance contact after contact is one
    /// the cut cannot cover (a decommissioned node's last create), which the
    /// caller names after a while (h2-N4).
    pub above: Option<(NodeId, Hlc)>,
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

/// A collection as one side sees it at a cut.
struct Facet {
    /// This side's digest of the collection's definitions.
    digest: DefinitionHash,
    /// The definitions as `(key, hash)`.
    list: Vec<(DefinitionKey, DefinitionHash)>,
    deferred: Option<DefinitionDefer>,
    /// The origin of a definition stamp above the cut, when that is why.
    above: Option<NodeId>,
    /// The standing life's incarnation floor, which a refusal hint must be above.
    floor: Option<Hlc>,
}

/// A 16-byte hash from the wire, if it is one.
fn hash_of(bytes: &[u8]) -> Option<DefinitionHash> {
    <[u8; 16]>::try_from(bytes).ok().map(u128::from_be_bytes)
}

fn bytes_of(hash: DefinitionHash) -> ByteBuf {
    ByteBuf::from(hash.to_be_bytes().to_vec())
}

impl Engine {
    /// Every collection held (shadows hidden) as this side sees it at `cut`,
    /// the digest and every predicate read through `txn` and no other.
    fn assess_definitions(
        &self,
        txn: &redb::ReadTransaction,
        cut: &VersionVector,
    ) -> Result<BTreeMap<CollectionId, Facet>> {
        let metas = Self::collections_in(txn, PairedShadows::Hidden)?;
        // Retention can collect the tombstone of a dropped index, and the
        // collection's definition stamp falls with it: when the cut is below
        // what retention removed, no collection can be judged, on either side.
        let horizon = Self::serve_verdict_in(txn, cut)? != ServeVerdict::Serve;
        let dropped_indexes = txn.open_table(tables::INDEXES_DROPPED)?;
        let mut tombstoned: BTreeMap<String, Option<Stamp>> = BTreeMap::new();
        let mut facets = BTreeMap::new();
        for meta in &metas {
            let mut above = None;
            let list = definition_list(meta);
            let digest = crate::definition_digest::digest_of_list(&list);
            let dropped = Self::collection_dropped_at_in(txn, meta.id)?;
            let deferred = if horizon {
                Some(DefinitionDefer::Horizon)
            } else if drop_defers(dropped, meta.created, cut)
                // A life that owes ADR-231's sweep for this drop (created above
                // it, floor below it) still holds what the life before put in
                // it, however far the cut is past the drop: the same rule as the
                // cut digest's, so the two halves never disagree about it.
                || dropped.is_some_and(|at| crate::late_drop::owes_a_sweep(meta, at.hlc))
            {
                Some(DefinitionDefer::Incarnation)
            } else if drop_defers(
                match tombstoned.get(&meta.db) {
                    Some(known) => *known,
                    None => {
                        let held = Self::collection_dropped_at_in(
                            txn,
                            kimmy_core::database_drop_id(&meta.db),
                        )?;
                        tombstoned.insert(meta.db.clone(), held);
                        held
                    }
                },
                meta.created,
                cut,
            ) {
                Some(DefinitionDefer::DatabaseTombstone)
            } else {
                above = Self::definition_above_cut(&dropped_indexes, meta, cut)?;
                above.map(|_| DefinitionDefer::DefinitionAboveCut)
            };
            facets.insert(
                meta.id,
                Facet { digest, list, deferred, above, floor: meta.incarnation_floor },
            );
        }
        Ok(facets)
    }

    /// Make the definition probe fail with a storage error on its `calls`-th call
    /// from now (a test hook: a fault of this half's own, which must cost this
    /// half and never the other, nor the contact).
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn fail_definition_probe_in_for_test(&self, calls: u32) {
        self.definition_fail_in.store(calls, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn fail_definition_probe_if_armed(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        let armed = self.definition_fail_in.load(Ordering::SeqCst);
        if armed == 0 {
            return Ok(());
        }
        self.definition_fail_in.store(armed - 1, Ordering::SeqCst);
        match armed {
            1 => Err(crate::StorageError::Corrupt("a definition fault injected by a test".into())),
            _ => Ok(()),
        }
    }

    /// Remove the index `name` from `db.collection`'s stored definition with no
    /// stamp and no entry (a test hook: the divergence replication would not
    /// have made, which the definition digest exists to find).
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn lose_an_index_silently_for_test(
        &self,
        db: &str,
        collection: &str,
        name: &str,
    ) -> Result<()> {
        let mut meta = self.get_collection(db, collection)?;
        meta.indexes.retain(|index| index.name != name);
        let txn = self.begin_write(crate::engine::WriterHolder::Ddl)?;
        Self::put_collection_meta(&txn, &meta)?;
        txn.commit()?;
        Ok(())
    }

    /// The origin of a definition of `meta` stamped above the cut, if there is
    /// one: an index created, a vector configuration changed, or an index dropped.
    /// Each stamp is judged against the cut's component for its own origin, so the
    /// greatest of them is not enough: one origin's stamp can be covered while
    /// another's is not. An index or configuration with no stamp cannot defer.
    fn definition_above_cut(
        dropped_indexes: &impl ReadableTable<(u64, u32), &'static [u8]>,
        meta: &CollectionMeta,
        cut: &VersionVector,
    ) -> Result<Option<NodeId>> {
        let above = |stamp: &Stamp| stamp.hlc > cut.get(stamp.node);
        if let Some(stamp) = meta
            .indexes
            .iter()
            .filter_map(|i| i.created.as_ref())
            .chain(meta.vector_changed.as_ref())
            .find(|stamp| above(stamp))
        {
            return Ok(Some(stamp.node));
        }
        for row in dropped_indexes.range((meta.id.0, 0)..=(meta.id.0, u32::MAX))? {
            let (_, raw) = row?;
            let stamp = codec::decode_oplog_key(raw.value())?;
            if above(&stamp) {
                return Ok(Some(stamp.node));
            }
        }
        Ok(None)
    }

    /// The responder's side: the digest of every collection's definitions at
    /// the request's cut, read in **one** read transaction, with the detail of
    /// the collections the request names.
    pub fn definition_answer(&self, ask: &DefinitionAsk) -> Result<DefinitionReply> {
        #[cfg(any(test, feature = "test-hooks"))]
        self.fail_definition_probe_if_armed()?;
        let txn = self.db().begin_read()?;
        #[cfg(test)]
        test_hooks::run_after_the_transaction(self);
        let facets = self.assess_definitions(&txn, &ask.cut)?;
        let detail: BTreeSet<CollectionId> = ask.detail.iter().copied().collect();
        let mut entries = Vec::new();
        for (id, facet) in &facets {
            let mut entry = DefinitionEntry {
                collection: *id,
                digest: bytes_of(facet.digest),
                deferred: facet.deferred,
                above: facet.above,
                definitions: Vec::new(),
                refused: Vec::new(),
            };
            if facet.deferred.is_none() && detail.contains(id) {
                entry.definitions = facet
                    .list
                    .iter()
                    .map(|(key, hash)| WireDefinition {
                        kind: key.kind,
                        name: key.name.clone(),
                        hash: bytes_of(*hash),
                    })
                    .collect();
                let own: BTreeMap<&DefinitionKey, DefinitionHash> =
                    facet.list.iter().map(|(key, hash)| (key, *hash)).collect();
                // The requester cannot evaluate this side's floor or what this
                // side holds, so both clauses are applied here, before a pair is
                // put on the wire: a refusal of a definition is valid only while
                // this side lacks the name or holds another definition under it,
                // and a held-drop only while this side still holds exactly the
                // hash it stored. A stale row explains nothing.
                entry.refused = Self::refusal_hints_in(&txn, *id)?
                    .into_iter()
                    .filter(|hint| facet.floor.is_none_or(|floor| hint.stamp.hlc > floor))
                    .filter(|hint| {
                        let holds = own.get(&hint.key()) == Some(&hint.hash);
                        holds == hint.kind.is_drop()
                    })
                    .map(|hint| WireRefusal {
                        hint: hint.kind,
                        name: hint.name,
                        hash: bytes_of(hint.hash),
                    })
                    .collect();
            }
            entries.push(entry);
        }
        Ok(DefinitionReply { format: FORMAT, entries, malformed: 0 })
    }

    /// The requester's side, after the answer arrived: this side's digests at
    /// the same cut in **one** read transaction, and the comparison.
    pub fn definition_compare(
        &self,
        ask: &DefinitionAsk,
        reply: &DefinitionReply,
    ) -> Result<Vec<DefinitionVerdict>> {
        #[cfg(any(test, feature = "test-hooks"))]
        self.fail_definition_probe_if_armed()?;
        let txn = self.db().begin_read()?;
        #[cfg(test)]
        test_hooks::run_after_the_transaction(self);
        let mine = self.assess_definitions(&txn, &ask.cut)?;
        let detail: BTreeSet<CollectionId> = ask.detail.iter().copied().collect();
        let mut out = Vec::new();
        for entry in &reply.entries {
            let id = entry.collection;
            let Some(here) = mine.get(&id) else {
                // The peer holds it and this side does not: the existence
                // half's business.
                continue;
            };
            let verdict = |outcome| DefinitionVerdict { collection: id, outcome, above: None };
            let reason = if reply.format != FORMAT {
                Some(DefinitionDefer::Format)
            } else {
                entry.deferred.or(here.deferred)
            };
            if let Some(reason) = reason {
                // A stamp above the cut, on whichever side: its origin, and the
                // cut's component for it, so the caller can tell a stamp in
                // motion from one the cut cannot cover.
                let origin = match reason {
                    DefinitionDefer::DefinitionAboveCut => here.above.or(entry.above),
                    _ => None,
                };
                out.push(DefinitionVerdict {
                    collection: id,
                    outcome: DefinitionOutcome::Deferred(reason),
                    above: origin.map(|node| (node, ask.cut.get(node))),
                });
                continue;
            }
            let Some(theirs) = hash_of(entry.digest.as_slice()) else {
                // A digest of the wrong width is no digest.
                out.push(verdict(DefinitionOutcome::Deferred(DefinitionDefer::Malformed)));
                continue;
            };
            if theirs == here.digest {
                out.push(verdict(DefinitionOutcome::Compared {
                    equal: true,
                    mine: here.digest,
                    theirs,
                    differing: Vec::new(),
                }));
                continue;
            }
            let detailed = detail.contains(&id).then(|| self.explain(&txn, id, here, entry));
            let outcome = match detailed {
                Some(Ok(Some((differing, true)))) => DefinitionOutcome::Explained { differing },
                Some(Ok(Some((differing, false)))) => DefinitionOutcome::Compared {
                    equal: false,
                    mine: here.digest,
                    theirs,
                    differing,
                },
                Some(Err(e)) => return Err(e),
                Some(Ok(None)) | None => DefinitionOutcome::Compared {
                    equal: false,
                    mine: here.digest,
                    theirs,
                    differing: Vec::new(),
                },
            };
            out.push(verdict(outcome));
        }
        Ok(out)
    }

    /// The definitions that differ between this side and the peer's, and whether
    /// valid refusal hints cover every one of them. `None` when the peer sent no
    /// detail (an answer from a build that does not, or a malformed list).
    ///
    /// Judged by the requester on both sides' lists, every entry **keyed by its
    /// kind** (an index named like the vector configuration is another entry). A
    /// hint is valid only while the definitions are as it says (a hint, never a
    /// mask):
    /// - *this side's own* refusal of the peer's definition: the peer's hash for
    ///   the definition is the one the hint stored (that this side lacks it or
    ///   holds another under it is the difference itself, and only a definition
    ///   that differs is ever covered);
    /// - *this side's own* refusal of the peer's **drop**: this side still holds
    ///   the definition with the hash the hint stored, and the peer lacks it;
    /// - *the peer's* refusals, as pairs that carry their kind and already passed
    ///   the peer's floor and its own validity: the same two shapes read from the
    ///   other side.
    ///
    /// This side's own hints must also be above the standing life's floor.
    fn explain(
        &self,
        txn: &redb::ReadTransaction,
        id: CollectionId,
        here: &Facet,
        entry: &DefinitionEntry,
    ) -> Result<Option<(Vec<DefinitionKey>, bool)>> {
        let mut theirs: BTreeMap<DefinitionKey, DefinitionHash> = BTreeMap::new();
        for definition in &entry.definitions {
            let Some(hash) = hash_of(definition.hash.as_slice()) else { return Ok(None) };
            let key = DefinitionKey { kind: definition.kind, name: definition.name.clone() };
            theirs.insert(key, hash);
        }
        let mine: BTreeMap<DefinitionKey, DefinitionHash> = here.list.iter().cloned().collect();
        let differing: BTreeSet<DefinitionKey> = mine
            .keys()
            .chain(theirs.keys())
            .filter(|key| mine.get(*key) != theirs.get(*key))
            .cloned()
            .collect();
        let mut covered: BTreeSet<DefinitionKey> = BTreeSet::new();
        for hint in Self::refusal_hints_in(txn, id)? {
            if here.floor.is_some_and(|floor| hint.stamp.hlc <= floor) {
                continue;
            }
            let key = hint.key();
            let valid = if hint.kind.is_drop() {
                mine.get(&key) == Some(&hint.hash) && !theirs.contains_key(&key)
            } else {
                theirs.get(&key) == Some(&hint.hash)
            };
            if valid {
                covered.insert(key);
            }
        }
        for pair in &entry.refused {
            let Some(hash) = hash_of(pair.hash.as_slice()) else { continue };
            let name = match pair.hint.definition() {
                DefinitionKind::Index => pair.name.clone(),
                DefinitionKind::Vector => String::new(),
            };
            let key = DefinitionKey { kind: pair.hint.definition(), name };
            let valid = if pair.hint.is_drop() {
                // The peer holds what this side dropped, and could not drop it.
                theirs.get(&key) == Some(&hash) && !mine.contains_key(&key)
            } else {
                // The peer refused what this side holds.
                mine.get(&key) == Some(&hash)
            };
            if valid {
                covered.insert(key);
            }
        }
        let explained = !differing.is_empty() && differing.is_subset(&covered);
        Ok(Some((differing.into_iter().collect(), explained)))
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

    pub(super) fn run_after_the_transaction(engine: &Engine) {
        if let Some(hook) = AFTER_THE_TRANSACTION.with(|h| h.borrow_mut().take()) {
            hook(engine);
        }
    }
}
