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
//!   set. Metadata only — a scan of the database and collection tables, not
//!   a document — so it costs the same whether a collection holds ten rows
//!   or ten million, and it is what would have caught the two collections
//!   finding 14 stranded on one member.
//! - **One collection's live document count**, chosen in turn by
//!   [`next_probe`] so a round pays for at most one collection's scan rather
//!   than the whole database. It is what would have caught the 500- and
//!   517-document losses finding 14 also produced, which a names-only check
//!   would have missed entirely — the more serious half of what was lost.
//!
//! What this cannot catch, stated once here rather than scattered in
//! comments: a document present in equal numbers on every member but with
//! different content (a lost update that still counts, rather than a lost
//! document); a count divergence in a collection that has not yet had its
//! turn at the probe; anything while the affected member is legitimately
//! behind, which [`compare`] refuses to report by construction (see its own
//! comment) — including the direction that construction does *not* cover on
//! its own, a peer that has not yet pulled *this* node's own recent writes,
//! which the caller must additionally guard (see `divergence_probe_for` in
//! `kimmy-cluster`); and a collection this node holds that a peer does not,
//! which is that peer's own discovery to make.

use std::collections::{BTreeSet, HashMap};

use kimmy_core::{CollectionId, NodeId};

use crate::engine::Engine;
use crate::error::Result;

impl Engine {
    /// Every collection id this node holds, across every database.
    ///
    /// Metadata only: [`Self::list_databases`] and [`Self::list_collections`]
    /// each scan their own small table, never a document, so this costs the
    /// same on a cluster with empty collections as one with full ones and can
    /// run every round without the cost this module exists to bound (see the
    /// module docs). Vector shadow collections are excluded — their own
    /// lifecycle trails the collection they serve by design, and comparing
    /// them would flag that lag as a divergence rather than measure one.
    pub fn all_collection_ids(&self) -> Result<BTreeSet<CollectionId>> {
        let mut ids = BTreeSet::new();
        for db in self.list_databases()? {
            for coll in self.list_collections(&db.name)? {
                if !kimmy_core::vector_meta::is_shadow(&coll.name) {
                    ids.insert(coll.id);
                }
            }
        }
        Ok(ids)
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
}

/// What a peer answered a divergence check with.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerAnswer {
    /// Every collection id the peer reported holding.
    pub collections: BTreeSet<CollectionId>,
    /// The peer's own live count of the probed collection, `None` if it does
    /// not hold it.
    pub probe_count: Option<u64>,
}

/// Collections a divergence check found disagreeing against one peer.
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
pub fn compare(mine: &LocalState, peer: &PeerAnswer) -> BTreeSet<CollectionId> {
    let mut divergent: BTreeSet<CollectionId> =
        peer.collections.difference(&mine.collections).copied().collect();
    if let Some((probe, Some(mine_count))) = mine.probe
        && let Some(their_count) = peer.probe_count
        && mine_count != their_count
    {
        divergent.insert(probe);
    }
    divergent
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
#[derive(Debug, Default)]
pub struct DivergenceTracker {
    cursor: Option<CollectionId>,
    /// What the most recent contact with each peer found, whether or not it
    /// was enough on its own to confirm.
    pending: HashMap<NodeId, BTreeSet<CollectionId>>,
    /// What has been confirmed against each peer: found on that peer's two
    /// most recent consecutive contacts.
    confirmed: HashMap<NodeId, BTreeSet<CollectionId>>,
}

impl DivergenceTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the rotation and return the collection id this tick probes
    /// for a count, given the full set of collections this node currently
    /// holds. `None` when there is nothing to probe.
    pub fn advance_probe(&mut self, mine: &BTreeSet<CollectionId>) -> Option<CollectionId> {
        self.cursor = next_probe(mine, self.cursor);
        self.cursor
    }

    /// Fold in what one contact with `peer` found — [`compare`]'s result, or
    /// an empty set for a contact that ran the check and found nothing
    /// wrong. Call this once per peer the check actually ran against this
    /// tick; a peer not contacted, or contacted but not checked (the round
    /// had a backlog too deep to reach the peer's tail), must not be folded
    /// in at all, or its last confirmed finding would be read as reconciled
    /// rather than simply not re-examined.
    pub fn observe(&mut self, peer: NodeId, seen: BTreeSet<CollectionId>) {
        let previously_pending = self.pending.get(&peer).cloned().unwrap_or_default();
        let newly_confirmed: BTreeSet<CollectionId> =
            previously_pending.intersection(&seen).copied().collect();

        let confirmed_for_peer = self.confirmed.entry(peer).or_default();
        confirmed_for_peer.extend(newly_confirmed);
        confirmed_for_peer.retain(|id| seen.contains(id));
        if confirmed_for_peer.is_empty() {
            self.confirmed.remove(&peer);
        }

        if seen.is_empty() {
            self.pending.remove(&peer);
        } else {
            self.pending.insert(peer, seen);
        }
    }

    /// Distinct collections confirmed divergent against at least one peer —
    /// what the gauge reports. The same collection confirmed against two
    /// peers still counts once: the gauge answers "how many collections",
    /// not "how many peer pairs".
    pub fn confirmed_count(&self) -> usize {
        self.confirmed.values().flatten().collect::<BTreeSet<_>>().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u64) -> CollectionId {
        CollectionId(n)
    }

    fn set(ids: &[u64]) -> BTreeSet<CollectionId> {
        ids.iter().map(|&n| id(n)).collect()
    }

    fn peer(n: u128) -> NodeId {
        NodeId::from_bytes(n.to_be_bytes())
    }

    #[test]
    fn a_collection_the_peer_holds_and_this_node_does_not_is_divergent() {
        let mine = LocalState { collections: set(&[1, 2]), probe: None };
        let peer = PeerAnswer { collections: set(&[1, 2, 3]), probe_count: None };
        assert_eq!(compare(&mine, &peer), set(&[3]));
    }

    #[test]
    fn a_collection_only_this_node_holds_is_not_reported_here() {
        // The peer's own loop finds this when it pulls from this node and
        // reaches the same `behind == None` gate on its side.
        let mine = LocalState { collections: set(&[1, 2, 3]), probe: None };
        let peer = PeerAnswer { collections: set(&[1, 2]), probe_count: None };
        assert_eq!(compare(&mine, &peer), BTreeSet::new());
    }

    #[test]
    fn a_converged_cluster_reports_nothing() {
        let mine = LocalState { collections: set(&[1, 2]), probe: Some((id(1), Some(40))) };
        let peer = PeerAnswer { collections: set(&[1, 2]), probe_count: Some(40) };
        assert_eq!(compare(&mine, &peer), BTreeSet::new());
    }

    #[test]
    fn a_disagreeing_probe_count_is_divergent_even_with_matching_names() {
        // Finding 14's more serious half: names alone would have missed 500
        // and 517 missing documents in collections that existed everywhere.
        let mine = LocalState { collections: set(&[1]), probe: Some((id(1), Some(1518))) };
        let peer = PeerAnswer { collections: set(&[1]), probe_count: Some(2018) };
        assert_eq!(compare(&mine, &peer), set(&[1]));
    }

    #[test]
    fn a_probe_neither_side_can_answer_is_not_flagged_by_the_count_half() {
        // This node lacks the probed collection outright; that is the
        // existence check's business (it is absent from `mine.collections`
        // too), not a spurious count mismatch.
        let mine = LocalState { collections: set(&[]), probe: Some((id(9), None)) };
        let peer = PeerAnswer { collections: set(&[9]), probe_count: Some(3) };
        assert_eq!(compare(&mine, &peer), set(&[9]), "caught by existence, not double-counted");
    }

    #[test]
    fn a_caller_that_distrusts_the_probe_suppresses_only_the_count_half() {
        // What the caller does when it judges the peer's answer stale on its
        // own recent writes (`mine.probe = None`): the existence half, which
        // does not depend on the probe at all, still runs.
        let mine = LocalState { collections: set(&[1, 2]), probe: None };
        let peer = PeerAnswer { collections: set(&[1, 2, 3]), probe_count: Some(999) };
        assert_eq!(compare(&mine, &peer), set(&[3]), "existence still runs with no probe trusted");
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

    #[test]
    fn a_single_contact_does_not_confirm_a_divergence() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), set(&[7]));
        assert_eq!(tracker.confirmed_count(), 0, "one sighting is not yet counted");
    }

    #[test]
    fn two_consecutive_contacts_with_the_same_peer_confirm_it() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), set(&[7]));
        tracker.observe(peer(1), set(&[7]));
        assert_eq!(tracker.confirmed_count(), 1, "seen twice running against the same peer");
    }

    #[test]
    fn a_gap_against_the_same_peer_never_confirms() {
        // The shape of the race `DivergenceTracker` exists to filter out: a
        // one-off does not recur on the very next contact with that peer.
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), set(&[7]));
        tracker.observe(peer(1), set(&[])); // gone next contact — a race, not a divergence
        tracker.observe(peer(1), set(&[7]));
        assert_eq!(
            tracker.confirmed_count(),
            0,
            "has to recur back-to-back, not merely twice ever"
        );
    }

    #[test]
    fn a_confirmed_divergence_clears_the_moment_that_peer_no_longer_shows_it() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), set(&[7]));
        tracker.observe(peer(1), set(&[7]));
        assert_eq!(tracker.confirmed_count(), 1);
        tracker.observe(peer(1), set(&[]));
        assert_eq!(tracker.confirmed_count(), 0, "reconciled: the gauge must fall, not stick");
    }

    #[test]
    fn independent_collections_confirm_independently_against_one_peer() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), set(&[1, 2]));
        tracker.observe(peer(1), set(&[2, 3]));
        assert_eq!(tracker.confirmed_count(), 1, "only 2 recurred; 1 and 3 each seen once");
        tracker.observe(peer(1), set(&[2, 3]));
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
        tracker.observe(peer(1), set(&[7])); // tick 1: peer 1 only known-divergent
        tracker.observe(peer(2), set(&[])); // tick 2: a different peer entirely
        tracker.observe(peer(3), set(&[])); // tick 3: another different peer
        assert_eq!(tracker.confirmed_count(), 0, "peer 1 not yet re-contacted");
        tracker.observe(peer(1), set(&[7])); // tick 4: peer 1's second contact
        assert_eq!(tracker.confirmed_count(), 1, "confirmed on its own two contacts, not the tick");
    }

    #[test]
    fn a_peer_reconciling_does_not_clear_a_different_peers_finding() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), set(&[1]));
        tracker.observe(peer(1), set(&[1]));
        tracker.observe(peer(2), set(&[2]));
        tracker.observe(peer(2), set(&[2]));
        assert_eq!(tracker.confirmed_count(), 2, "collections 1 and 2, against peers 1 and 2");

        tracker.observe(peer(1), set(&[])); // peer 1 reconciles
        assert_eq!(tracker.confirmed_count(), 1, "only peer 1's finding clears");
    }

    #[test]
    fn the_same_collection_confirmed_against_two_peers_counts_once() {
        let mut tracker = DivergenceTracker::new();
        tracker.observe(peer(1), set(&[9]));
        tracker.observe(peer(1), set(&[9]));
        tracker.observe(peer(2), set(&[9]));
        tracker.observe(peer(2), set(&[9]));
        assert_eq!(tracker.confirmed_count(), 1, "one collection, however many peers report it");
    }
}
