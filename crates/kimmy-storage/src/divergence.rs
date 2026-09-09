//! The cross-member divergence check (ADR-133).
//!
//! Anti-entropy converges by folding entries into a version vector, and
//! nothing on that path can see a divergence *it* produced — which is
//! exactly what let a truncated sync window advance a witness past entries
//! that were never applied, with `kimmy_replication_lag_seconds` at 0 and
//! every sync counter quiet the whole time. This module answers a narrower,
//! independent question, transport-free like the rest of [`crate::sync`]:
//! given what this node holds and what a peer says it holds, do they agree.
//!
//! Two things are compared, deliberately not everything a full
//! reconciliation would check:
//!
//! - **Which collections exist**, on this node and not the peer's advertised
//!   set, *less* the ones this node holds a tombstone for at the incarnation
//!   the peer is advertising. Metadata only — a scan of the database and
//!   collection tables and one of the tombstone table, not a document — so it
//!   costs the same whether a collection holds ten rows or ten million, and it
//!   is what would have caught the two collections finding 14 stranded on one
//!   member. The subtraction is what stops it undoing a drop the cluster has
//!   agreed on, by repairing the collection back from whichever peer has not
//!   applied that drop yet; see [`compare`].
//! - **One collection's live document count**, chosen in turn by
//!   [`next_probe`] so a round pays for at most one collection's scan rather
//!   than the whole database. It is what would have caught the 500- and
//!   517-document losses finding 14 also produced, which a names-only check
//!   would have missed entirely — the more serious half of what was lost.
//!
//! Confirmation for the two halves runs on different rhythms, and
//! [`DivergenceTracker`] tracks them separately because of it: the existence
//! half is re-examined in full every contact with a peer, so two consecutive
//! contacts are also two consecutive checks of it; the count half examines
//! only whichever collection [`next_probe`] rotated onto that contact, so
//! confirming it needs two consecutive *probes of that same collection*
//! against that peer, which are not the same two contacts once more than one
//! collection is in rotation. An earlier version of this tracker conflated
//! the two, which meant a count divergence was detected correctly every time
//! its turn came round and confirmed never, for any node holding more than
//! one collection — see [`DivergenceTracker::observe`] for the full account.
//!
//! What this cannot catch, stated once here rather than scattered in
//! comments: a document present in equal numbers on every member but with
//! different content (a lost update that still counts, rather than a lost
//! document); a count divergence in a collection that has not yet been
//! probed twice running against a given peer, which can take up to two full
//! rotations through this node's collections; anything while the affected
//! member is legitimately behind, which [`compare`] refuses to report by
//! construction (see its own comment) — including the direction that
//! construction does *not* cover on its own, a peer that has not yet pulled
//! *this* node's own recent writes, which the caller must additionally guard
//! (see `divergence_probe_for` in `kimmy-cluster`); and a collection this
//! node holds that a peer does not, which is that peer's own discovery to
//! make.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use kimmy_core::{CollectionId, Hlc, NodeId};
use redb::{ReadableDatabase, ReadableTable};
use tracing::warn;

use crate::codec;
use crate::engine::{Engine, PairedShadows};
use crate::error::Result;
use crate::tables;

impl Engine {
    /// Every collection id this node holds, across every database — **less
    /// the vector shadows whose parent is present** (ADR-138). Not the set to
    /// reconcile the index cache against: [`Self::live_collections`] is, and
    /// the ids hidden here are exactly the ones it must not miss.
    ///
    /// Metadata only: one read transaction over the `collections` table
    /// ([`Self::all_collections`]), never a document, so this costs the same
    /// on a cluster with empty collections as one with full ones and can run
    /// every round without the cost this module exists to bound (see the
    /// module docs).
    ///
    /// A vector shadow collection is excluded **while the collection it serves
    /// is present here** — its own lifecycle trails that collection by design
    /// (only the owning member builds one), and comparing them would flag that
    /// lag as a divergence rather than measure one.
    ///
    /// An **orphaned** shadow — one whose base collection this node does not
    /// hold — is included, because it is not lag. Nothing builds a shadow for a
    /// collection that is not there, so it is residue, and it is what a
    /// database drop racing an in-flight shadow replication leaves behind: the
    /// peer applies its own local drop, the owner's shadow-creation entry
    /// arrives afterwards, and the database comes back holding nothing but the
    /// shadow. Excluding those made a whole database present on one member and
    /// absent on another **structurally invisible** to the existence check —
    /// observed on a live cluster for over a minute with the check running
    /// every round and `divergence_checks_total{skipped}` at zero, which is
    /// exactly the reading ADR-135 added to mean "checked and agreed"
    /// (ADR-138).
    ///
    /// The rule itself is written once, in `Engine::collections`, and this is
    /// that walk with `PairedShadows::Hidden`; [`Self::live_collections`] is
    /// the same walk with `PairedShadows::Included`, and that one argument is
    /// the whole difference between them.
    pub fn all_collection_ids(&self) -> Result<BTreeSet<CollectionId>> {
        Ok(self.collection_incarnations()?.into_keys().collect())
    }

    /// [`Self::all_collection_ids`] with the incarnation each id stands at:
    /// `CollectionMeta.created`, the stamp of the create that produced it at
    /// its origin.
    ///
    /// The set is exactly the one above — the same walk, the same ADR-138
    /// rule — because it is the same answer with the one fact an id cannot
    /// carry. A peer compares it against the tombstones it holds
    /// ([`Self::collection_tombstones`]): the id alone cannot tell a
    /// collection the peer dropped and this node has not heard about yet
    /// from one recreated since, because a name dropped and created again
    /// derives the same id.
    pub fn collection_incarnations(&self) -> Result<BTreeMap<CollectionId, Hlc>> {
        Ok(self
            .collections(PairedShadows::Hidden)?
            .into_iter()
            .map(|c| (c.id, c.created))
            .collect())
    }

    /// Every collection tombstone this node holds, as the id and the `Hlc`
    /// of the drop that wrote it.
    ///
    /// The whole table under one read transaction, one row per dropped
    /// collection — the same walk `gc::collect_dropped_collections` makes,
    /// down to what it does with a row it cannot read, and for the same
    /// reason: this is the only shape in which the answer is complete.
    ///
    /// **No cutoff, deliberately.** Retention is decided in exactly one
    /// place, by the collector, and a second opinion about which tombstones
    /// still count is how the two would drift — which is the shape of the
    /// bug this reading exists to prevent. What the collector has removed is
    /// gone from here too, which is what `tombstone_retention_secs` means.
    pub fn collection_tombstones(&self) -> Result<BTreeMap<CollectionId, Hlc>> {
        let txn = self.db().begin_read()?;
        let dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        let mut out = BTreeMap::new();
        for row in dropped.iter()? {
            let (id, stamp) = row?;
            // Warned and stepped over, as the collector steps over it: one
            // unreadable row must not be able to fail every divergence check
            // this node makes, for ever, against every peer. The id it names
            // is then simply not known to have been dropped, which is the
            // same position a collected tombstone leaves this node in.
            let Ok(stamp) = codec::decode_oplog_key(stamp.value()) else {
                warn!("undecodable collection tombstone skipped by the divergence check");
                continue;
            };
            out.insert(CollectionId(id.value()), stamp.hlc);
        }
        Ok(out)
    }

    /// The live document count of collection `id`, or `None` if this node
    /// does not hold it.
    ///
    /// One collection's scan, no more — the cost [`next_probe`] rotates
    /// around the cluster's collections rather than paying every round.
    pub fn count_by_id(&self, id: CollectionId) -> Result<Option<u64>> {
        match self.collection_by_id(id)? {
            Some(meta) => Ok(Some(self.count(&meta)?)),
            None => Ok(None),
        }
    }
}

/// What this node holds, going into a divergence check against one peer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LocalState {
    /// Every collection id this node holds (`Engine::all_collection_ids`).
    pub collections: BTreeSet<CollectionId>,
    /// The collection this round is probing for a count, and this node's own
    /// live count of it — `None` for the count when this node does not hold
    /// the collection being probed, which [`compare`] then leaves alone: a
    /// probe collection missing here entirely is already the existence
    /// check's business, not the count check's. The whole field is `None`
    /// when the caller has decided the peer's answer cannot be trusted for a
    /// count this round (see `divergence_probe_for` in `kimmy-cluster`) —
    /// the existence half is unaffected either way.
    pub probe: Option<(CollectionId, Option<u64>)>,
    /// Every collection tombstone this node holds
    /// (`Engine::collection_tombstones`), which is what tells a collection
    /// this node never had from one it destroyed on purpose.
    pub dropped: BTreeMap<CollectionId, Hlc>,
}

/// What a peer answered a divergence check with.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerAnswer {
    /// Every collection id the peer reported holding.
    pub collections: BTreeSet<CollectionId>,
    /// The peer's own live count of the probed collection, `None` if it does
    /// not hold it.
    pub probe_count: Option<u64>,
    /// The incarnation the peer holds each of those collections at
    /// (`Engine::collection_incarnations`). Empty from a peer that predates
    /// the field, which [`compare`] reads as "the incarnation I dropped" for
    /// any id this node holds a tombstone for — see its own documentation
    /// for why that direction, and what it costs.
    pub incarnations: BTreeMap<CollectionId, Hlc>,
}

/// What a divergence check found this contact, split by which half found
/// it.
///
/// The split matters beyond bookkeeping: the two halves confirm on
/// different rhythms, and folding them into one set before they reach
/// [`DivergenceTracker`] is what made a count divergence structurally
/// unconfirmable on any node holding more than one collection (see
/// [`DivergenceTracker::observe`]'s own documentation for the mechanism).
/// The existence half is checked in full, every contact; the count half is
/// checked only for the one collection this contact happened to probe.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Findings {
    /// Collections the peer holds that this node does not.
    pub existence: BTreeSet<CollectionId>,
    /// The probed collection and whether its count disagreed. `None` when
    /// no count comparison was made this contact: no collection was named
    /// in the request, the peer's answer did not cover it, or the caller
    /// judged the peer's answer untrustworthy for a count this round (see
    /// `divergence_probe_for` in `kimmy-cluster`).
    pub count: Option<(CollectionId, bool)>,
}

/// Compare what this node holds against what a peer answered.
///
/// **The existence half is one-directional.** A collection *this* node holds
/// that the peer's answer omits is not reported here — that is the peer's
/// own discovery to make when its own replication loop pulls from this node
/// and finds the identical gate open on its side (see [`crate::sync`] —
/// every member runs the same loop against every other, which is what
/// converges a cluster without a push half). Reporting it here too would
/// flag a collection the instant it is created locally, before the peer has
/// had a chance to catch up — precisely the flapping a legitimately-behind
/// member must not produce. The caller only reaches this function at all
/// when its own `VersionVector::behind` already reads `None` against the
/// peer (or a round's pull reached the peer's true tail — see
/// `kimmy-cluster`'s `sync_once`), i.e. when this node believes it has
/// witnessed everything the peer has advertised; a collection the peer holds
/// that is still missing here, under that belief, is the exact shape of the
/// silence finding 14 produced.
///
/// **The count half has no such protection of its own — this function
/// cannot give it one.** `mine.probe`'s count and `peer.probe_count` are
/// compared symmetrically: this function has no way to tell "the peer is
/// genuinely divergent" from "the peer has simply not pulled this node's own
/// recent writes yet". Only the caller, holding the peer's advertised
/// version vector, can tell the two apart — which is why `mine.probe` is
/// `None` whenever the caller has judged the peer's answer untrustworthy for
/// a count this round, and this function trusts that judgement rather than
/// re-deriving it.
///
/// **A collection this node dropped is not a divergence while the peer still
/// holds the incarnation that was dropped.** Anti-entropy repairs what this
/// half reports, so reporting one meant pulling the dropped collection back
/// from whichever peer had not applied the drop yet, and the recreated copy
/// then re-seeded the members that had it right: a deletion the cluster had
/// already agreed on, undone by the machinery meant to converge it. A drop
/// travels through the entries path like any other change; a peer that has
/// not applied it yet is behind, which is the one thing the existence half
/// must never read as divergence (above).
///
/// A peer that predates `PeerAnswer::incarnations` reports no stamp, and for
/// an id this node holds a tombstone for that reads as the incarnation this
/// node dropped: not reported. The cost is a collection genuinely recreated
/// on such a peer, whose `CreateCollection` entry has already aged out of the
/// oplog, which this check leaves alone until the roll completes or the
/// tombstone expires. That is chosen over resurrecting a drop, which is not
/// recoverable by waiting; and the ordinary recreation is unaffected, because
/// its entry still arrives through the entries path and is applied there.
pub fn compare(mine: &LocalState, peer: &PeerAnswer) -> Findings {
    let existence = peer
        .collections
        .difference(&mine.collections)
        .copied()
        .filter(|id| match mine.dropped.get(id) {
            Some(dropped) => {
                peer.incarnations.get(id).is_some_and(|incarnation| incarnation > dropped)
            }
            None => true,
        })
        .collect();
    let count = match mine.probe {
        Some((id, Some(mine_count))) => {
            peer.probe_count.map(|their_count| (id, mine_count != their_count))
        }
        _ => None,
    };
    Findings { existence, count }
}

/// The next collection id to probe for a document count, cycling through
/// `ids` in order and wrapping after the last one.
///
/// `after` is the id probed last tick; passing the id that follows it (or
/// the first, once the end is reached, or the id set has since changed
/// underneath it) is what guarantees every collection gets a turn rather
/// than however many fit before the set that held them changed shape. `None`
/// only when `ids` is empty — nothing to probe.
pub fn next_probe(
    ids: &BTreeSet<CollectionId>,
    after: Option<CollectionId>,
) -> Option<CollectionId> {
    let first = || ids.iter().next().copied();
    match after {
        None => first(),
        Some(after) => ids
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .next()
            .copied()
            .or_else(first),
    }
}

/// Confirms a divergence over two consecutive *contacts with the same peer*
/// before counting it, and clears it the moment a contact with that peer no
/// longer observes it — [`Self::confirmed_count`] is what the exported gauge
/// reads.
///
/// **Keyed per peer, not per tick.** An earlier version folded every peer's
/// finding into one global pending/confirmed pair per tick, which only
/// confirms a divergence that recurs on two *consecutive replication ticks*.
/// `PeerHealth::select` hands each tick a `fanout`-sized window of the known
/// peers and advances it, so two ticks in a row share a peer only while
/// `2 × fanout > (number of peers)` — with the default fanout of 3, five
/// members or fewer. Past that, no peer is ever the one contacted on two
/// consecutive ticks, so a global tracker can never confirm anything: the
/// gauge is structurally pinned at 0 regardless of how badly the cluster has
/// diverged. Keying by peer instead means confirmation only needs the same
/// peer contacted twice *in a row for that peer* — however many other ticks
/// or other peers fall between — which is what `PeerHealth::select`
/// eventually guarantees for every known peer.
///
/// A single contact's finding is not trusted alone even though the caller
/// has already gated it (see [`compare`]): a peer's answer is built from two
/// separate reads a moment apart — its version vector, fetched by the
/// anti-entropy round already under way, then its collection list and probe
/// count, fetched by this check a message or two later on the same
/// connection. A collection created on the peer in that gap can in
/// principle outrun the vector the gate was judged against. A second
/// confirmation, on the very next contact with that same peer, makes that
/// race need to land twice in a row to be counted, which a live divergence
/// does — nothing here repairs it, so it recurs on every contact for ever —
/// and a race between two reads a message apart does not.
///
/// A finding against one peer is only cleared by a later contact with that
/// *same* peer that no longer sees it — never by ticks in which that peer
/// simply was not contacted. Silence about a peer is not evidence it has
/// reconciled.
///
/// **The count half needs a second, independent confirmation axis, keyed by
/// collection as well as by peer.** The existence half is re-examined in
/// full on every contact — `Findings::existence` is the whole comparison,
/// every time — so "two consecutive contacts with the same peer" is also
/// "two consecutive *checks* of any given collection's existence", and the
/// peer-keyed scheme above confirms it correctly. The count half is not:
/// only one collection is probed per contact (`advance_probe` rotates), so
/// on a node holding more than one collection, the *same* collection is
/// probed against a given peer only once every `advance_probe` cycle — the
/// two "most recent contacts" with that peer are very rarely the two most
/// recent *probes of that collection*. Folding the count finding into the
/// same per-contact `seen` set as existence, as an earlier version of this
/// tracker did, meant a count divergence was visible on exactly one contact
/// in every N (N = collection count) and never on two in a row: detected
/// correctly every time its turn came round, confirmed never, for any N
/// greater than one. `count_pending`/`count_confirmed` track each
/// `(peer, collection)` pair's own two-probes-in-a-row independently of how
/// many other collections are probed against that peer in between —
/// confirming across the two most recent contacts *in which that collection
/// was probed*, not the two most recent contacts overall.
///
/// **A collection this node no longer holds is swept from the count state,
/// in [`Self::advance_probe`], the moment that becomes true — not left to
/// clear itself.** The existence half already has this for free:
/// `Self::observe` recomputes `confirmed`'s membership from a fresh
/// `existence` set on every contact, so a collection absent from it is
/// dropped immediately. The count half's state, keyed by `(peer,
/// collection)`, has no equivalent contact-shaped moment — a dropped
/// collection is never named by `advance_probe` again, so no future probe
/// could ever clear it — and confirming that this really is a "provably
/// gone" fact rather than mere silence needs `mine`, which `advance_probe`
/// already receives fresh every tick. Getting this wrong pins a confirmed
/// finding at 1 for the rest of the process the moment an operator does the
/// obvious thing about it (drop and recreate the collection), which is
/// exactly the "alert nobody can clear" failure this ADR argues against
/// elsewhere for a different reason (defect 2).
#[derive(Debug, Default)]
pub struct DivergenceTracker {
    cursor: Option<CollectionId>,
    /// What the most recent contact with each peer found existentially
    /// divergent, whether or not it was enough on its own to confirm.
    pending: HashMap<NodeId, BTreeSet<CollectionId>>,
    /// What has been confirmed existentially divergent against each peer:
    /// found on that peer's two most recent consecutive contacts.
    confirmed: HashMap<NodeId, BTreeSet<CollectionId>>,
    /// `(peer, collection)` pairs whose most recent *probe* — not most
    /// recent contact — found a count mismatch.
    count_pending: std::collections::HashSet<(NodeId, CollectionId)>,
    /// Collections confirmed divergent by count against each peer: found
    /// mismatched on that pair's two most recent consecutive probes.
    count_confirmed: HashMap<NodeId, BTreeSet<CollectionId>>,
}

impl DivergenceTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the rotation and return the collection id this tick probes
    /// for a count, given the full set of collections this node currently
    /// holds. `None` when there is nothing to probe.
    ///
    /// Also sweeps the count half's state against `mine`: a collection this
    /// node no longer holds can never be named by `advance_probe` again, so
    /// no later probe could ever clean up its `count_pending` or
    /// `count_confirmed` entry — unlike a peer simply not being contacted,
    /// which is silence and must not be read as reconciliation (the
    /// existence half's own rule), a dropped collection is *provably* gone
    /// from the one set this method is already given fresh every call.
    /// Without this sweep a confirmed count finding against a since-dropped
    /// collection stays lit for the rest of the process, which is exactly
    /// the alert an operator cannot clear by fixing the thing it reported —
    /// and it also means a later collection that reuses the same id (a
    /// drop-and-recreate derives an identical id from the same name) would
    /// inherit a stale pending mismatch and could confirm on a single probe
    /// of the new incarnation.
    pub fn advance_probe(&mut self, mine: &BTreeSet<CollectionId>) -> Option<CollectionId> {
        self.count_pending.retain(|(_, id)| mine.contains(id));
        for confirmed in self.count_confirmed.values_mut() {
            confirmed.retain(|id| mine.contains(id));
        }
        self.count_confirmed.retain(|_, confirmed| !confirmed.is_empty());

        self.cursor = next_probe(mine, self.cursor);
        self.cursor
    }

    /// Fold in what one contact with `peer` found — [`compare`]'s result.
    /// Call this once per peer the check actually ran against this tick; a
    /// peer not contacted, or contacted but not checked (the round had a
    /// backlog too deep to reach the peer's tail), must not be folded in at
    /// all, or its last confirmed finding would be read as reconciled
    /// rather than simply not re-examined. `findings.count` being `None`
    /// (nothing was probed, or the probe was not trusted this contact)
    /// leaves that peer's count state exactly where its last actual probe
    /// left it, for the same reason.
    pub fn observe(&mut self, peer: NodeId, findings: Findings) {
        let Findings { existence, count } = findings;

        let previously_pending = self.pending.get(&peer).cloned().unwrap_or_default();
        let newly_confirmed: BTreeSet<CollectionId> =
            previously_pending.intersection(&existence).copied().collect();

        let confirmed_for_peer = self.confirmed.entry(peer).or_default();
        confirmed_for_peer.extend(newly_confirmed);
        confirmed_for_peer.retain(|id| existence.contains(id));
        if confirmed_for_peer.is_empty() {
            self.confirmed.remove(&peer);
        }

        if existence.is_empty() {
            self.pending.remove(&peer);
        } else {
            self.pending.insert(peer, existence);
        }

        if let Some((id, mismatched)) = count {
            let key = (peer, id);
            if mismatched {
                if self.count_pending.contains(&key) {
                    self.count_confirmed.entry(peer).or_default().insert(id);
                }
                self.count_pending.insert(key);
            } else {
                self.count_pending.remove(&key);
                if let Some(set) = self.count_confirmed.get_mut(&peer) {
                    set.remove(&id);
                    if set.is_empty() {
                        self.count_confirmed.remove(&peer);
                    }
                }
            }
        }
    }

    /// Collections confirmed divergent against `peer` by either half of
    /// the check, as of its last folded-in contact — what a repair against
    /// that peer is planned from (ADR-148). Empty for a peer with nothing
    /// confirmed, or never checked.
    pub fn confirmed_against(&self, peer: NodeId) -> BTreeSet<CollectionId> {
        let mut out = self.confirmed.get(&peer).cloned().unwrap_or_default();
        if let Some(by_count) = self.count_confirmed.get(&peer) {
            out.extend(by_count.iter().copied());
        }
        out
    }

    /// Distinct collections confirmed divergent against at least one peer,
    /// by either half of the check — what the gauge reports. The same
    /// collection confirmed against two peers, or by both existence and
    /// count, still counts once: the gauge answers "how many collections",
    /// not "how many peers, or which half, reported it".
    pub fn confirmed_count(&self) -> usize {
        self.confirmed
            .values()
            .flatten()
            .chain(self.count_confirmed.values().flatten())
            .collect::<BTreeSet<_>>()
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An engine holding `app.docs` and, optionally, its shadow — the two
    /// shapes `all_collection_ids` has to tell apart (ADR-138).
    fn engine_with(base: bool, shadow: bool) -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        if base {
            engine.create_collection("app", "docs").unwrap();
        }
        if shadow {
            engine
                .create_system_collection("app", &kimmy_core::vector_meta::shadow_name("docs"))
                .unwrap();
        }
        (engine, dir)
    }

    #[test]
    fn a_shadow_beside_its_collection_is_not_compared() {
        // The owning member's ordinary steady state: it alone builds the
        // shadow, and the peers legitimately do not have one. Comparing it
        // would report that by-design difference as a divergence on every
        // round, which is why the exclusion exists (ADR-133).
        let (owner, _d1) = engine_with(true, true);
        let (peer, _d2) = engine_with(true, false);
        assert_eq!(
            owner.all_collection_ids().unwrap(),
            peer.all_collection_ids().unwrap(),
            "an owner and a non-owner of the same collection must compare equal"
        );
    }

    #[test]
    fn an_orphaned_shadow_is_compared() {
        // What a database drop racing an in-flight shadow replication leaves:
        // the base collection is gone and the shadow is not. Nothing builds a
        // shadow for a collection that is not there, so this is residue, not
        // the lifecycle lag the exclusion is for.
        let (stranded, _d1) = engine_with(false, true);
        let ids = stranded.all_collection_ids().unwrap();
        assert_eq!(ids.len(), 1, "an orphaned shadow must be visible to the check");
    }

    #[test]
    fn live_collections_reports_the_paired_shadow_that_all_collection_ids_hides() {
        // The two views are one walk apart on one rule: `all_collection_ids`
        // hides a shadow standing beside its parent (ADR-138), and
        // `live_collections` hides nothing, because the hidden ids are the
        // ones a vector index is keyed by. Everything else — an ordinary
        // collection, an orphaned shadow — they agree on. The hidden set is
        // pinned against the loop `all_collection_ids` used to be, kept here
        // as the oracle, so moving it onto the single walk changed nothing.
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let shadow_of = kimmy_core::vector_meta::shadow_name;
        let docs = engine.create_collection("app", "docs").unwrap();
        let docs_shadow = engine.create_system_collection("app", &shadow_of("docs")).unwrap();
        let notes = engine.create_collection("app", "notes").unwrap();
        // A parent named `docs` in *another* database must not vouch for this
        // orphan: the rule is per database, where the shadow's name derives.
        let orphan = engine.create_system_collection("other", &shadow_of("docs")).unwrap();

        let oracle: BTreeSet<CollectionId> = {
            let mut ids = BTreeSet::new();
            for db in engine.list_databases().unwrap() {
                let colls = engine.list_collections(&db.name).unwrap();
                let present: std::collections::HashSet<&str> =
                    colls.iter().map(|c| c.name.as_str()).collect();
                for coll in &colls {
                    let hidden = kimmy_core::vector_meta::base_name(&coll.name)
                        .is_some_and(|base| present.contains(base));
                    if !hidden {
                        ids.insert(coll.id);
                    }
                }
            }
            ids
        };
        let hidden = engine.all_collection_ids().unwrap();
        assert_eq!(hidden, oracle, "the single walk must hide exactly what the old loop hid");
        assert_eq!(hidden, [docs.id, notes.id, orphan.id].into_iter().collect::<BTreeSet<_>>());
        assert!(!hidden.contains(&docs_shadow.id), "a paired shadow is hidden from the check");

        let live = engine.live_collections().unwrap();
        assert_eq!(
            live.keys().copied().collect::<BTreeSet<_>>(),
            [docs.id, docs_shadow.id, notes.id, orphan.id].into_iter().collect::<BTreeSet<_>>(),
            "live_collections hides nothing"
        );
        assert_eq!(live[&docs_shadow.id], docs_shadow.created, "and carries the incarnation");
        assert_eq!(live[&orphan.id], orphan.created);
        for id in &hidden {
            assert!(live.contains_key(id), "everything the check sees, the cache sees too");
        }
    }

    #[test]
    fn a_database_holding_only_an_orphaned_shadow_diverges_from_an_empty_peer() {
        // The live failure this fixes, end to end through `compare`: one
        // member holds a database containing nothing but a shadow, another
        // holds nothing at all. Before ADR-138 both sides filtered the shadow
        // out, the difference was the empty set, and the gauge read 0 for
        // over a minute on a real cluster while the check ran every round
        // with `skipped` at zero.
        let (stranded, _d1) = engine_with(false, true);
        let (empty, _d2) = engine_with(false, false);

        let mine = LocalState {
            collections: empty.all_collection_ids().unwrap(),
            probe: None,
            ..Default::default()
        };
        let peer = PeerAnswer {
            collections: stranded.all_collection_ids().unwrap(),
            probe_count: None,
            ..Default::default()
        };
        assert_eq!(
            compare(&mine, &peer).existence.len(),
            1,
            "a shadow-only database the peer holds and this node does not must be reported"
        );
    }

    #[test]
    fn the_incarnations_and_the_tombstones_are_the_two_halves_of_one_answer() {
        // What the two sides of a divergence check read. The incarnations
        // are the ids `all_collection_ids` reports with the one fact an id
        // cannot carry, and the tombstones are the whole table — no cutoff,
        // because the collector already decides retention and a second
        // opinion about it is how the two would drift.
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let docs = engine.create_collection("app", "docs").unwrap();
        let notes = engine.create_collection("app", "notes").unwrap();
        engine.drop_collection("app", "notes").unwrap();
        let dropped = engine.collection_dropped_at(notes.id).unwrap().expect("a tombstone");

        let incarnations = engine.collection_incarnations().unwrap();
        assert_eq!(incarnations, [(docs.id, docs.created)].into_iter().collect());
        assert_eq!(
            incarnations.keys().copied().collect::<BTreeSet<_>>(),
            engine.all_collection_ids().unwrap(),
            "the same set, the same walk"
        );
        assert_eq!(
            engine.collection_tombstones().unwrap(),
            [(notes.id, dropped.hlc)].into_iter().collect()
        );
    }

    fn id(n: u64) -> CollectionId {
        CollectionId(n)
    }

    fn set(ids: &[u64]) -> BTreeSet<CollectionId> {
        ids.iter().map(|&n| id(n)).collect()
    }

    fn peer(n: u128) -> NodeId {
        NodeId::from_bytes(n.to_be_bytes())
    }

    /// Build a [`Findings`] with only the existence half set, for tests that
    /// predate the count/existence split and only ever cared about that
    /// half. Named `only` because a bare `Findings { existence, count: None }`
    /// at every call site would bury the point.
    fn only(existence: &[u64]) -> Findings {
        Findings { existence: set(existence), count: None }
    }

    #[test]
    fn a_collection_the_peer_holds_and_this_node_does_not_is_divergent() {
        let mine = LocalState { collections: set(&[1, 2]), probe: None, ..Default::default() };
        let peer =
            PeerAnswer { collections: set(&[1, 2, 3]), probe_count: None, ..Default::default() };
        assert_eq!(compare(&mine, &peer).existence, set(&[3]));
    }

    #[test]
    fn a_collection_only_this_node_holds_is_not_reported_here() {
        // The peer's own loop finds this when it pulls from this node and
        // reaches the same `behind == None` gate on its side.
        let mine = LocalState { collections: set(&[1, 2, 3]), probe: None, ..Default::default() };
        let peer =
            PeerAnswer { collections: set(&[1, 2]), probe_count: None, ..Default::default() };
        assert_eq!(compare(&mine, &peer).existence, BTreeSet::new());
    }

    #[test]
    fn a_converged_cluster_reports_nothing() {
        let mine = LocalState {
            collections: set(&[1, 2]),
            probe: Some((id(1), Some(40))),
            ..Default::default()
        };
        let peer =
            PeerAnswer { collections: set(&[1, 2]), probe_count: Some(40), ..Default::default() };
        let findings = compare(&mine, &peer);
        assert_eq!(findings.existence, BTreeSet::new());
        assert_eq!(findings.count, Some((id(1), false)));
    }

    #[test]
    fn a_disagreeing_probe_count_is_divergent_even_with_matching_names() {
        // Finding 14's more serious half: names alone would have missed 500
        // and 517 missing documents in collections that existed everywhere.
        let mine = LocalState {
            collections: set(&[1]),
            probe: Some((id(1), Some(1518))),
            ..Default::default()
        };
        let peer =
            PeerAnswer { collections: set(&[1]), probe_count: Some(2018), ..Default::default() };
        let findings = compare(&mine, &peer);
        assert_eq!(findings.existence, BTreeSet::new());
        assert_eq!(findings.count, Some((id(1), true)));
    }

    #[test]
    fn a_probe_neither_side_can_answer_is_not_flagged_by_the_count_half() {
        // This node lacks the probed collection outright; that is the
        // existence check's business (it is absent from `mine.collections`
        // too), not a spurious count mismatch.
        let mine =
            LocalState { collections: set(&[]), probe: Some((id(9), None)), ..Default::default() };
        let peer =
            PeerAnswer { collections: set(&[9]), probe_count: Some(3), ..Default::default() };
        let findings = compare(&mine, &peer);
        assert_eq!(findings.existence, set(&[9]), "caught by existence, not double-counted");
        assert_eq!(findings.count, None, "this node cannot compare a count it does not have");
    }

    /// The stamps a tombstone and a peer's answer are compared by, keyed by
    /// the same short ids the other `compare` tests use.
    fn stamps(entries: &[(u64, u64)]) -> BTreeMap<CollectionId, Hlc> {
        entries.iter().map(|&(n, wall)| (id(n), Hlc::new(wall, 0))).collect()
    }

    #[test]
    fn a_collection_this_node_dropped_is_not_divergent_while_the_peer_still_holds_it() {
        // The finding: a drop answered `200`, and minutes later the
        // collection was back on all three members with every document,
        // twice — pulled from whichever peer had not applied the drop yet,
        // then re-seeded onto the members that had it right. A peer that has
        // not applied a change yet is behind, which is the one thing this
        // half must never report.
        //
        // Id 4 is the same case with the peer's incarnation landing in the
        // same millisecond as the drop: at the boundary it is still the life
        // that was dropped.
        let mine = LocalState {
            collections: set(&[1]),
            dropped: stamps(&[(3, 50), (4, 50)]),
            ..Default::default()
        };
        let peer = PeerAnswer {
            collections: set(&[1, 3, 4]),
            incarnations: stamps(&[(1, 10), (3, 20), (4, 50)]),
            ..Default::default()
        };
        assert_eq!(compare(&mine, &peer).existence, BTreeSet::new());
    }

    #[test]
    fn a_collection_recreated_since_this_node_dropped_it_is_divergent() {
        // Why the incarnation travels at all: ids are derived from names, so
        // a collection recreated after the drop wears the id of the one this
        // node buried. Its stamp is the only thing that separates a genuine
        // recreation, which must be pulled, from the copy the drop was aimed
        // at, which must not.
        let mine = LocalState {
            collections: BTreeSet::new(),
            dropped: stamps(&[(3, 50)]),
            ..Default::default()
        };
        let peer = PeerAnswer {
            collections: set(&[3]),
            incarnations: stamps(&[(3, 51)]),
            ..Default::default()
        };
        assert_eq!(compare(&mine, &peer).existence, set(&[3]));
    }

    #[test]
    fn a_peer_that_names_no_incarnation_is_read_as_holding_the_one_this_node_dropped() {
        // The mixed-version rule, and it is deliberate. A peer that predates
        // the field answers with names alone; reading that as a recreation
        // resurrects the drop, and no later round undoes that, where the
        // recreation this refuses to report is pulled as soon as the roll
        // finishes — or through the entries path, which is untouched.
        let mine = LocalState {
            collections: BTreeSet::new(),
            dropped: stamps(&[(3, 50)]),
            ..Default::default()
        };
        assert_eq!(
            compare(&mine, &PeerAnswer { collections: set(&[3]), ..Default::default() }).existence,
            BTreeSet::new()
        );
        // It subtracts; it does not gate. An id with no tombstone here is
        // reported from that same stampless answer exactly as before.
        assert_eq!(
            compare(&mine, &PeerAnswer { collections: set(&[3, 4]), ..Default::default() })
                .existence,
            set(&[4])
        );
    }

    #[test]
    fn a_caller_that_distrusts_the_probe_suppresses_only_the_count_half() {
        // What the caller does when it judges the peer's answer stale on its
        // own recent writes (`mine.probe = None`): the existence half, which
        // does not depend on the probe at all, still runs.
        let mine = LocalState { collections: set(&[1, 2]), probe: None, ..Default::default() };
        let peer = PeerAnswer {
            collections: set(&[1, 2, 3]),
            probe_count: Some(999),
            ..Default::default()
        };
        let findings = compare(&mine, &peer);
        assert_eq!(findings.existence, set(&[3]), "existence still runs with no probe trusted");
        assert_eq!(findings.count, None);
    }

    #[test]
    fn next_probe_cycles_through_every_collection_and_wraps() {
        let ids = set(&[10, 20, 30]);
        let a = next_probe(&ids, None);
        assert_eq!(a, Some(id(10)));
        let b = next_probe(&ids, a);
        assert_eq!(b, Some(id(20)));
        let c = next_probe(&ids, b);
        assert_eq!(c, Some(id(30)));
        let d = next_probe(&ids, c);
        assert_eq!(d, Some(id(10)), "wraps rather than probing nothing forever");
    }

    #[test]
    fn next_probe_recovers_when_the_last_cursor_no_longer_exists() {
        // The set changes shape between ticks (a collection dropped). The
        // cursor from before must not strand the rotation on nothing.
        let ids = set(&[5, 15]);
        assert_eq!(next_probe(&ids, Some(id(999))), Some(id(5)), "past everything: wraps to first");
        assert_eq!(next_probe(&BTreeSet::new(), Some(id(5))), None, "nothing left to probe");
    }

    // -- Existence half: confirmation against a peer, per contact --------

    #[test]
    fn a_single_contact_does_not_confirm_a_divergence() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), only(&[7]));
        assert_eq!(tracker.confirmed_count(), 0, "one sighting is not yet counted");
    }

    #[test]
    fn two_consecutive_contacts_with_the_same_peer_confirm_it() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), only(&[7]));
        tracker.observe(peer(1), only(&[7]));
        assert_eq!(tracker.confirmed_count(), 1, "seen twice running against the same peer");
    }

    #[test]
    fn a_gap_against_the_same_peer_never_confirms() {
        // The shape of the race `DivergenceTracker` exists to filter out: a
        // one-off does not recur on the very next contact with that peer.
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), only(&[7]));
        tracker.observe(peer(1), only(&[])); // gone next contact — a race, not a divergence
        tracker.observe(peer(1), only(&[7]));
        assert_eq!(
            tracker.confirmed_count(),
            0,
            "has to recur back-to-back, not merely twice ever"
        );
    }

    #[test]
    fn a_confirmed_divergence_clears_the_moment_that_peer_no_longer_shows_it() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), only(&[7]));
        tracker.observe(peer(1), only(&[7]));
        assert_eq!(tracker.confirmed_count(), 1);
        tracker.observe(peer(1), only(&[]));
        assert_eq!(tracker.confirmed_count(), 0, "reconciled: the gauge must fall, not stick");
    }

    #[test]
    fn independent_collections_confirm_independently_against_one_peer() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), only(&[1, 2]));
        tracker.observe(peer(1), only(&[2, 3]));
        assert_eq!(tracker.confirmed_count(), 1, "only 2 recurred; 1 and 3 each seen once");
        tracker.observe(peer(1), only(&[2, 3]));
        assert_eq!(tracker.confirmed_count(), 2, "3 now recurs too; 2 stays confirmed");
    }

    /// The fanout scaling defect, fixed: a peer whose contacts are
    /// interleaved with contacts against other peers — the shape of a
    /// cluster larger than twice the fanout, where the same peer is not
    /// reached on two consecutive replication ticks — still confirms, because
    /// confirmation only needs two consecutive *contacts with that peer*,
    /// not two consecutive ticks of the whole loop.
    #[test]
    fn confirmation_survives_ticks_where_the_peer_was_not_contacted_at_all() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), only(&[7])); // tick 1: peer 1 only known-divergent
        tracker.observe(peer(2), only(&[])); // tick 2: a different peer entirely
        tracker.observe(peer(3), only(&[])); // tick 3: another different peer
        assert_eq!(tracker.confirmed_count(), 0, "peer 1 not yet re-contacted");
        tracker.observe(peer(1), only(&[7])); // tick 4: peer 1's second contact
        assert_eq!(tracker.confirmed_count(), 1, "confirmed on its own two contacts, not the tick");
    }

    #[test]
    fn a_peer_reconciling_does_not_clear_a_different_peers_finding() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), only(&[1]));
        tracker.observe(peer(1), only(&[1]));
        tracker.observe(peer(2), only(&[2]));
        tracker.observe(peer(2), only(&[2]));
        assert_eq!(tracker.confirmed_count(), 2, "collections 1 and 2, against peers 1 and 2");

        tracker.observe(peer(1), only(&[])); // peer 1 reconciles
        assert_eq!(tracker.confirmed_count(), 1, "only peer 1's finding clears");
    }

    #[test]
    fn the_same_collection_confirmed_against_two_peers_counts_once() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), only(&[9]));
        tracker.observe(peer(1), only(&[9]));
        tracker.observe(peer(2), only(&[9]));
        tracker.observe(peer(2), only(&[9]));
        assert_eq!(tracker.confirmed_count(), 1, "one collection, however many peers report it");
    }

    // -- Count half: confirmation across probes, not across contacts -----
    //
    // Finding 14's more serious half was 500 and 517 missing documents in
    // collections that existed, correctly named, on every member. P6
    // reproduced it directly: the check found the mismatch every single
    // time its collection came round in the rotation, and the gauge never
    // moved, because the tracker treated a contact that did not probe a
    // given collection the same as a contact that probed it and found it
    // clean. These pin the fix — the count half's own two-in-a-row rule,
    // keyed by which contacts actually *probed* the collection.

    #[test]
    fn a_single_count_probe_does_not_confirm() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), Findings { existence: set(&[]), count: Some((id(7), true)) });
        assert_eq!(tracker.confirmed_count(), 0);
    }

    #[test]
    fn two_consecutive_probes_of_the_same_collection_confirm_a_count_divergence() {
        let mut tracker = DivergenceTracker::new();
        let findings = Findings { existence: set(&[]), count: Some((id(7), true)) };
        tracker.observe(peer(1), findings.clone());
        tracker.observe(peer(1), findings);
        assert_eq!(tracker.confirmed_count(), 1);
    }

    /// The exact composition defect P6 found: `advance_probe` rotates
    /// through every collection this node holds, so a fixed peer is
    /// contacted every tick but the *same collection* is only probed
    /// against it once per lap. A tracker that confirmed on "two
    /// consecutive contacts" regardless of what was probed could never see
    /// the same collection probed twice running once there was more than
    /// one collection — detected on schedule, confirmed never. This drives
    /// the real rotation against the real tracker, the way
    /// `confirmation_survives_ticks_where_the_peer_was_not_contacted_at_all`
    /// does for the peer axis.
    #[test]
    fn a_count_divergence_confirms_despite_rotating_through_other_collections() {
        let mine = set(&[1, 2, 3, 4, 5]);
        let divergent = id(3);
        let p = peer(1);
        let mut tracker = DivergenceTracker::new();

        for _ in 0..(mine.len() * 2) {
            let probed = tracker.advance_probe(&mine).expect("always something to probe");
            let count = Some((probed, probed == divergent));
            tracker.observe(p, Findings { existence: set(&[]), count });
        }
        assert_eq!(
            tracker.confirmed_count(),
            1,
            "one collection out of five confirms despite the other four rotating through"
        );
    }

    #[test]
    fn a_gap_between_probes_of_the_same_collection_never_confirms_a_count() {
        // A clean probe of the *same* collection between two mismatched
        // ones is the race the two-in-a-row rule exists to filter out.
        let mut tracker = DivergenceTracker::new();
        let mismatched = Findings { existence: set(&[]), count: Some((id(7), true)) };
        let clean = Findings { existence: set(&[]), count: Some((id(7), false)) };
        tracker.observe(peer(1), mismatched.clone());
        tracker.observe(peer(1), clean);
        tracker.observe(peer(1), mismatched);
        assert_eq!(
            tracker.confirmed_count(),
            0,
            "has to recur back-to-back at the probe level too"
        );
    }

    #[test]
    fn a_contact_that_does_not_probe_the_collection_leaves_its_count_state_untouched() {
        // Between the two mismatched probes of collection 7, this peer is
        // contacted again but a *different* collection is probed (or
        // nothing is, on an untrusted contact). Confirmation must not need
        // that intervening contact to have probed 7 too.
        let mut tracker = DivergenceTracker::new();
        let mismatched_7 = Findings { existence: set(&[]), count: Some((id(7), true)) };
        let probed_other = Findings { existence: set(&[]), count: Some((id(8), false)) };
        let untrusted = Findings { existence: set(&[]), count: None };
        tracker.observe(peer(1), mismatched_7.clone());
        tracker.observe(peer(1), probed_other);
        tracker.observe(peer(1), untrusted);
        tracker.observe(peer(1), mismatched_7);
        assert_eq!(tracker.confirmed_count(), 1, "7's own two probes still confirm it");
    }

    #[test]
    fn a_confirmed_count_divergence_clears_on_the_next_clean_probe_of_it() {
        let mut tracker = DivergenceTracker::new();
        let mismatched = Findings { existence: set(&[]), count: Some((id(7), true)) };
        tracker.observe(peer(1), mismatched.clone());
        tracker.observe(peer(1), mismatched);
        assert_eq!(tracker.confirmed_count(), 1);
        tracker.observe(peer(1), Findings { existence: set(&[]), count: Some((id(7), false)) });
        assert_eq!(tracker.confirmed_count(), 0, "reconciled: the gauge must fall, not stick");
    }

    #[test]
    fn existence_and_count_findings_on_the_same_collection_count_once() {
        let mut tracker = DivergenceTracker::new();
        let both = Findings { existence: set(&[7]), count: Some((id(7), true)) };
        tracker.observe(peer(1), both.clone());
        tracker.observe(peer(1), both);
        assert_eq!(tracker.confirmed_count(), 1, "one collection, found by both halves at once");
    }

    /// The realistic trigger for this defect: the gauge fires, an operator
    /// investigates, and remediates the way `operations.md` tells them
    /// to — drop the divergent collection and let it be recreated. Without
    /// the sweep in `advance_probe`, this specific alert is the one an
    /// operator's own remediation can never clear.
    #[test]
    fn a_confirmed_count_divergence_clears_when_its_collection_is_dropped() {
        let mut tracker = DivergenceTracker::new();
        let mismatched = Findings { existence: set(&[]), count: Some((id(7), true)) };
        tracker.observe(peer(1), mismatched.clone());
        tracker.observe(peer(1), mismatched);
        assert_eq!(tracker.confirmed_count(), 1);

        // Collection 7 is dropped locally: it no longer appears in this
        // node's own set, so `advance_probe` can never name it again, and
        // no later probe could ever clear it without the sweep.
        tracker.advance_probe(&set(&[8]));
        assert_eq!(tracker.confirmed_count(), 0, "gone locally must not stay confirmed forever");

        // Any number of subsequent clean ticks must not resurrect it either.
        for _ in 0..100 {
            tracker.advance_probe(&set(&[8]));
        }
        assert_eq!(tracker.confirmed_count(), 0);
    }

    /// `CollectionId` is derived from `(db, name)`, so a drop followed by a
    /// recreation of the same name yields the identical id. A *pending*
    /// (not yet confirmed) mismatch against the old incarnation must not
    /// survive to falsely confirm on the new one's very first probe.
    #[test]
    fn a_pending_count_mismatch_does_not_survive_a_drop_and_recreate() {
        let mut tracker = DivergenceTracker::new();
        // One mismatched probe: pending, not yet confirmed.
        tracker.observe(peer(1), Findings { existence: set(&[]), count: Some((id(7), true)) });
        assert_eq!(tracker.confirmed_count(), 0);

        tracker.advance_probe(&set(&[])); // 7 dropped: sweeps the pending entry
        tracker.advance_probe(&set(&[7])); // 7 recreated, same id

        // One probe of the new incarnation, also mismatched (still
        // catching up, say) -- on its own this must not confirm, because
        // it is the *new* incarnation's first probe, not its second.
        tracker.observe(peer(1), Findings { existence: set(&[]), count: Some((id(7), true)) });
        assert_eq!(
            tracker.confirmed_count(),
            0,
            "one probe of the new incarnation must not inherit the old one's pending state"
        );
    }
}
