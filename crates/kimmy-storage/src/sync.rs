//! Anti-entropy: deciding what to send a peer, and applying what one sends.
//!
//! Deliberately transport-free. Everything here works between two `Engine`
//! values in one process, which is how it is tested — convergence is a property
//! of the merge rules, not of the network, and mixing the two would make
//! failures ambiguous. The replication transport calls into this; it does not
//! reimplement it.
//!
//! ```text
//!   A                                    B
//!   │  version_vector() ────────────────▶│
//!   │                                    │  behind(theirs) -> Some(from)
//!   │◀──────────── entries_for_peer(from)│
//!   │  apply_batch(entries)              │
//! ```
//!
//! Both directions run the same exchange, which is why one round converges both
//! ways rather than only pushing.

use kimmy_core::{Hlc, NodeId, OpKind, OplogEntry, VersionVector};
use tracing::{debug, warn};

use crate::engine::Engine;
use crate::error::Result;

/// What applying a batch of replicated entries did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncOutcome {
    /// Entries that won and changed a document.
    pub applied: usize,
    /// Entries already covered by an equal or newer local version. Expected,
    /// not an error: peers resend overlapping ranges by design.
    pub superseded: usize,
    /// Schema changes applied: collections, indexes, vector configuration.
    pub ddl: usize,
    /// Entries for a collection this node does not have.
    ///
    /// Should be zero in a healthy cluster now that collection creation
    /// replicates. It stays non-zero when the `CreateCollection` entry has aged
    /// out of the peer's oplog — counted rather than silently dropped, because
    /// that case is a gap in coverage rather than convergence.
    pub unknown_collection: usize,
    /// The peer the round was with, once it has introduced itself.
    pub peer: Option<NodeId>,
    /// How far the peer trails *this* node, in milliseconds — the mirror of
    /// `lag_ms`. Above tombstone retention it names a stale rejoiner
    /// (ADR-085).
    pub behind_ms: u64,
    /// Milliseconds of the peer's history still unapplied after this round.
    ///
    /// Zero when caught up; non-zero when the peer holds more than one batch
    /// of backlog. Measured from the entries' own timestamps — the age span
    /// of undelivered work — not from any cursor. See [`lag_behind_ms`].
    pub lag_ms: u64,
}

/// How far `mine` trails `theirs`, in milliseconds of history.
///
/// For every origin where the peer's coverage is ahead, the gap between the
/// two wall clocks is the span of that origin's entries this node has not
/// applied; the maximum over origins is what an operator alerts on.
///
/// An origin this node has **never** seen contributes nothing: with only the
/// peer's *newest* timestamp to hand, the honest gap would need the oldest,
/// and `newest − zero` is the age of the epoch, not of the backlog. A joining
/// node's lag becomes meaningful with its first applied batch — moments in —
/// rather than starting at a fifty-year lie.
/// Symmetric in its arguments' roles, so `lag_behind_ms(theirs, mine)` is how
/// far the *peer* trails this node — what decides whether it has been away
/// longer than tombstone retention. Origins the trailing side has never seen
/// at all do not count: a brand-new member holds nothing old enough to
/// resurrect, and treating it as stale would flag every join.
pub fn lag_behind_ms(mine: &VersionVector, theirs: &VersionVector) -> u64 {
    theirs
        .iter()
        .filter_map(|(node, hlc)| {
            let held = mine.get(node);
            (held > Hlc::ZERO && hlc > held).then(|| hlc.wall_ms.saturating_sub(held.wall_ms))
        })
        .max()
        .unwrap_or(0)
}

impl SyncOutcome {
    pub fn total(&self) -> usize {
        self.applied + self.superseded + self.ddl + self.unknown_collection
    }
}

/// What a batch from a peer proved this node has seen, beyond the entries
/// themselves.
///
/// The peer serves contiguously in stamp order from the point asked for, so a
/// batch is a *window*: every entry the peer holds inside it was either
/// delivered or deliberately withheld (`UniqueViolation`, ADR-029). Either
/// way this node has now processed that window for **every origin the peer
/// advertised** — not only the origins that happened to appear in it.
///
/// - A **short** batch (fewer than `limit`) is the peer's whole tail, so the
///   window runs to the end of everything it advertised: the answer is
///   `theirs` itself.
/// - A **full** batch was truncated at `limit`, so the window ends at the last
///   delivered stamp. Each advertised origin is raised to the *lower* of that
///   stamp and the peer's own coverage of it: never past what the peer holds,
///   and never past what was served. Ties at the last stamp from origins with
///   a higher node id sort after it and were not served — they are re-served
///   next round, because the range read is inclusive at the stamp asked for.
///
/// Why the full-batch case exists at all: an advertised stamp this node can
/// never receive — a violation, a stamp whose entry sits behind the window —
/// otherwise pins `VersionVector::behind` at that origin's floor, and once the
/// peer holds a full batch after that floor, every round re-serves the same
/// already-witnessed window forever. Observed as a cluster whose sync log read
/// `applied=0, superseded=1021` every five seconds for the life of the
/// cluster, and whose lag gauge read the cluster's age. See ADR-082.
pub fn coverage_after_batch(
    theirs: &VersionVector,
    entries: &[OplogEntry],
    limit: usize,
) -> VersionVector {
    if entries.len() < limit {
        return theirs.clone();
    }
    let Some(last) = entries.last() else {
        return theirs.clone();
    };
    let window_end = last.stamp.hlc;
    let mut covered = VersionVector::new();
    for (node, their_max) in theirs.iter() {
        covered.insert(node, their_max.min(window_end));
    }
    covered
}

impl Engine {
    /// Merge a batch a peer served in answer to `AskEntries`, and record what
    /// the batch proved about coverage.
    ///
    /// The single path the replication transport uses for a served batch:
    /// [`Self::apply_batch`] for the entries, then the witnessed vector raised
    /// by [`coverage_after_batch`]. `limit` is the batch size that was asked
    /// for, which is what decides whether the peer's tail was reached.
    pub fn apply_peer_batch(
        &self,
        theirs: &VersionVector,
        entries: &[OplogEntry],
        limit: usize,
    ) -> Result<SyncOutcome> {
        let outcome = self.apply_batch(entries)?;
        self.absorb_witnessed(&coverage_after_batch(theirs, entries, limit))?;
        Ok(outcome)
    }

    /// Entries at or after `from`, for a peer that asked to catch up.
    ///
    /// Stamp order, not arrival order: a peer's question is "what do you hold
    /// after this logical time", which is about origin stamps. Change streams
    /// ask a different question and use the arrival index instead.
    ///
    /// **Unique-violation entries are excluded.** They record what *this* node
    /// observed when it merged, and every node observes the same collision
    /// independently — shipping them would report one violation once per node.
    /// See [ADR-029](../../../docs/decisions.md).
    pub fn entries_for_peer(&self, from: Hlc, limit: usize) -> Result<Vec<OplogEntry>> {
        Ok(self
            .read_oplog_from(from, limit)?
            .into_iter()
            .filter(|entry| entry.kind != OpKind::UniqueViolation)
            .collect())
    }

    /// Merge a batch of entries received from a peer.
    ///
    /// Each entry goes through `apply_remote`, so last-writer-wins decides
    /// per document and re-delivery is harmless. Ordering within the batch does
    /// not matter — that is the point of LWW, and it is what lets a peer send
    /// a range without coordinating.
    pub fn apply_batch(&self, entries: &[OplogEntry]) -> Result<SyncOutcome> {
        let mut outcome = SyncOutcome::default();
        let mut witnessed = kimmy_core::VersionVector::new();

        for entry in entries {
            self.apply_one(entry, &mut outcome)?;
            // **Every** entry that was processed, on every path — applied,
            // superseded, DDL, or skipped by design. Doing this per branch is
            // exactly how the hole appeared: three of them forgot, and the
            // node then re-requested those entries on every round forever.
            // See ADR-054.
            witnessed.observe(entry.stamp);
        }
        // One transaction for the batch rather than one per entry: a batch is
        // up to `MAX_BATCH` entries and this is on the sync path.
        self.absorb_witnessed(&witnessed)?;

        if outcome.unknown_collection > 0 {
            warn!(
                entries = outcome.unknown_collection,
                "skipped replicated entries for collections this node does not have; \
                 either the collection was dropped here, or its creation has aged out \
                 of the peer's oplog"
            );
        }
        debug!(
            applied = outcome.applied,
            superseded = outcome.superseded,
            ddl = outcome.ddl,
            unknown_collection = outcome.unknown_collection,
            "merged a batch from a peer"
        );
        Ok(outcome)
    }
}

/// Turn "that collection is not here" into `None` rather than an error.
///
/// A replicated schema change names its collection by *name*, so replaying one
/// after the collection has been dropped locally raises `CollectionNotFound`.
/// Before this existed that error travelled all the way out of `apply_batch`,
/// which failed the whole round — and since the offending entry stays in the
/// peer's oplog forever, the position never advanced and every later round
/// died on the same entry. One dropped collection permanently stopped
/// replication between two nodes.
///
/// Skipping is correct, not merely convenient: `apply_one` already treats a
/// *document* for a missing collection this way, and a schema change for a
/// collection that is gone is history for the same reason. It cannot lose a
/// change that still matters, because the only ways to reach here are a drop
/// that already happened on this node — which supersedes anything older — or a
/// creation that aged out of the peer's oplog, which the caller already counts
/// and warns about.
///
/// Deliberately narrow: only `CollectionNotFound` is swallowed. Any other
/// storage error still fails the round, because a round that quietly skips
/// what it cannot understand is how corruption becomes convergence.
fn gone<T>(result: Result<T>) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound { .. })) => Ok(None),
        Err(e) => Err(e),
    }
}

impl Engine {
    /// Apply a replicated schema change.
    ///
    /// Every arm is idempotent, because a peer resending an overlapping range
    /// is the normal case rather than an error. Idempotency is expressed as
    /// "is the world already like this?" rather than "have I seen this entry?"
    /// — the second would need per-entry bookkeeping that the oplog already
    /// provides, and would be wrong after a rebuild.
    ///
    /// The originating entry is appended either way, so this node's version
    /// vector advances and further peers learn of the change from it. That is
    /// the same rule `apply_remote` follows for documents.
    /// Process one replicated entry. Witnessing is the caller's job, so that
    /// no branch here can forget it.
    fn apply_one(&self, entry: &OplogEntry, outcome: &mut SyncOutcome) -> Result<()> {
        // A node's own observation of a broken constraint is not a fact
        // about the data; refuse it even if a peer sends one.
        if entry.kind == OpKind::UniqueViolation {
            return Ok(());
        }

        // Schema changes come first in stamp order, so a collection exists
        // by the time documents for it arrive.
        if entry.kind.is_ddl() {
            if self.apply_ddl(entry)? {
                outcome.ddl += 1;
            } else {
                outcome.unknown_collection += 1;
            }
            return Ok(());
        }

        // A legacy `Collection` entry names nothing and cannot be acted on.
        if !entry.kind.is_document() {
            return Ok(());
        }

        // A drop the sender has not heard about yet must not be undone by
        // the documents it is still replaying. Checked by id, because a
        // node that dropped the collection can no longer resolve that id
        // to a name.
        if let Some(dropped_at) = self.collection_dropped_at(entry.collection)?
            && entry.stamp < dropped_at
        {
            outcome.superseded += 1;
            return Ok(());
        }

        let Some(collection) = self.collection_by_id(entry.collection)? else {
            outcome.unknown_collection += 1;
            return Ok(());
        };

        // A recreated collection derives the *same* id as its predecessor
        // (`CollectionId::derive`), so an entry from the previous incarnation
        // resolves here rather than missing. Its creation recorded the
        // preceding drop's stamp as an incarnation floor; anything at or below
        // that floor is the previous life, however the stamps sort against the
        // drop itself — a peer's last pre-drop write can land in the same
        // millisecond as the drop, where a strict comparison ties and the
        // document slips through into the replacement. Collections created
        // without a tombstone behind them carry no floor: independent creation
        // on two nodes is convergence, not reincarnation.
        if let Some(floor) = collection.incarnation_floor
            && entry.stamp.hlc <= floor
        {
            outcome.superseded += 1;
            return Ok(());
        }

        if self.apply_remote(&collection, entry)? {
            outcome.applied += 1;
        } else {
            outcome.superseded += 1;
        }
        Ok(())
    }

    /// Returns whether the change was applied. `false` means it named a
    /// collection this node no longer has, which is history rather than an
    /// error — see [`gone`].
    fn apply_ddl(&self, entry: &OplogEntry) -> Result<bool> {
        let Some(body) = &entry.body else {
            // A legacy `Collection` entry, which names nothing.
            return Ok(true);
        };

        match entry.kind {
            OpKind::CreateCollection => {
                let target: kimmy_core::CollectionRef = bson::deserialize_from_slice(body)?;

                // A creation older than the drop that removed it is history,
                // not an instruction. Without this a peer partitioned across
                // the drop would recreate the collection on rejoining.
                if let Some(dropped_at) = self.collection_dropped_at(entry.collection)?
                    && entry.stamp < dropped_at
                {
                    debug!(
                        db = %target.db,
                        collection = %target.name,
                        "ignored a creation older than the drop that removed it"
                    );
                    return Ok(true);
                }

                match self.get_collection(&target.db, &target.name) {
                    Ok(_) => {}
                    Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound {
                        ..
                    })) => {
                        self.create_collection_inner(&target.db, &target.name, false)?;
                        debug!(db = %target.db, collection = %target.name, "created a replicated collection");
                    }
                    Err(e) => return Err(e),
                }
            }
            OpKind::DropCollection => {
                let target: kimmy_core::CollectionRef = bson::deserialize_from_slice(body)?;
                // The originating stamp, not a local one: the tombstone has to
                // sort before any recreation that legitimately followed the
                // drop, or the name becomes unusable on this node forever.
                self.drop_collection_inner(&target.db, &target.name, Some(entry.stamp))?;
                // Recorded even when the collection was already gone: the
                // tombstone is what stops a *later* replay from recreating it,
                // and a node that never had the collection still needs it.
                self.record_collection_drop(entry.collection, entry.stamp)?;
            }
            OpKind::CreateIndex => {
                let target: kimmy_core::IndexCreate = bson::deserialize_from_slice(body)?;
                if gone(self.apply_remote_index(&target))?.is_none() {
                    return Ok(false);
                }
            }
            OpKind::DropIndex => {
                let target: kimmy_core::IndexDrop = bson::deserialize_from_slice(body)?;
                let dropped =
                    self.drop_index_inner(&target.db, &target.collection, &target.index, false);
                if gone(dropped)?.is_none() {
                    return Ok(false);
                }
            }
            OpKind::ConfigureVectors => {
                let target: kimmy_core::VectorSet = bson::deserialize_from_slice(body)?;
                let applied = match target.config {
                    Some(config) => gone(self.configure_vectors_inner(
                        &target.db,
                        &target.collection,
                        config,
                        false,
                    ))?
                    .is_some(),
                    // Never `drop_vectors`: discarding a peer's stored
                    // vectors is not something a configuration change from
                    // elsewhere should decide.
                    None => gone(self.disable_vectors_inner(
                        &target.db,
                        &target.collection,
                        false,
                        false,
                    ))?
                    .is_some(),
                };
                if !applied {
                    return Ok(false);
                }
            }
            _ => {}
        }

        // The operations above deliberately logged nothing. Recording the
        // *originating* entry is what lets the change propagate onward with its
        // identity intact, and is what advances the version vector for its
        // origin node — while minting a local entry instead would send the
        // change back to the peer, which would apply it and mint another.
        let txn = self.begin_write()?;
        crate::engine::append_oplog(&txn, entry)?;
        txn.commit()?;
        self.witness(&entry.stamp);
        // Published, like a replicated *document* is (`apply_remote`). Without
        // this, a replicated schema change sat in the arrival index until some
        // unrelated write happened to wake a stream — so a collection dropped
        // on one node ended its watchers there immediately and left the ones
        // on every other node waiting indefinitely for a nudge.
        //
        // Invisible until change streams had a reason to care about DDL: they
        // filter schema entries out, so "delivered late" and "not delivered"
        // looked the same. Found by the cluster harness, which is the only
        // thing that could have: a single node applies its own drop directly.
        self.publish(vec![entry.clone()]);
        Ok(true)
    }

    /// Create a replicated index, tolerating one that is already there.
    fn apply_remote_index(&self, target: &kimmy_core::IndexCreate) -> Result<()> {
        let meta = self.get_collection(&target.db, &target.collection)?;
        if meta.index(&target.index.name).is_some() {
            return Ok(());
        }

        self.create_index_inner(
            &target.db,
            &target.collection,
            target.index.fields.clone(),
            target.index.unique,
            target.index.enforcement,
            Some(target.index.name.clone()),
            target.index.expire_after_secs,
            target.index.partial_filter.clone(),
            false,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use kimmy_core::DocId;

    const BATCH: usize = 1024;
    const DAY: u64 = 24 * 60 * 60;

    use crate::gc::RetentionPolicy;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        (engine, dir)
    }

    /// One direction of an anti-entropy round: pull into `into` from `from`.
    fn pull(into: &Engine, from: &Engine) -> SyncOutcome {
        let mine = into.version_vector().unwrap();
        let theirs = from.version_vector().unwrap();
        match mine.behind(&theirs) {
            Some(start) => {
                let entries = from.entries_for_peer(start, BATCH).unwrap();
                into.apply_batch(&entries).unwrap()
            }
            None => SyncOutcome::default(),
        }
    }

    /// A full round, both directions, as two peers would run it.
    fn sync(a: &Engine, b: &Engine) {
        pull(a, b);
        pull(b, a);
    }

    /// One direction of a *transport-shaped* round: what `sync_once` does,
    /// between two engines in one process. Unlike [`pull`], this asks against
    /// the witnessed vector and records what the batch proved, so it can show
    /// a round making progress — or failing to.
    fn round(into: &Engine, from: &Engine, limit: usize) -> SyncOutcome {
        let mine = into.witnessed_vector().unwrap();
        let theirs = from.version_vector().unwrap();
        match mine.behind(&theirs) {
            Some(start) => {
                let entries = from.entries_for_peer(start, limit).unwrap();
                let outcome = into.apply_peer_batch(&theirs, &entries, limit).unwrap();
                let mine = into.witnessed_vector().unwrap();
                SyncOutcome { lag_ms: lag_behind_ms(&mine, &theirs), ..outcome }
            }
            None => SyncOutcome::default(),
        }
    }

    #[test]
    fn a_short_batch_proves_the_whole_advertised_vector() {
        let mut theirs = VersionVector::new();
        let origin = kimmy_core::NodeId::generate();
        theirs.insert(origin, Hlc::new(5_000, 0));
        let entries = vec![entry_from(origin, Hlc::new(1_000, 0))];
        assert_eq!(coverage_after_batch(&theirs, &entries, 8), theirs);
        assert_eq!(coverage_after_batch(&theirs, &[], 8), theirs, "an empty tail too");
    }

    #[test]
    fn a_full_batch_proves_every_origin_up_to_its_last_stamp() {
        // Three advertised origins: one whose coverage ends before the window
        // does, one inside it, one beyond. Only the last is clipped.
        let early = kimmy_core::NodeId::generate();
        let inside = kimmy_core::NodeId::generate();
        let beyond = kimmy_core::NodeId::generate();
        let mut theirs = VersionVector::new();
        theirs.insert(early, Hlc::new(1_000, 0));
        theirs.insert(inside, Hlc::new(2_500, 0));
        theirs.insert(beyond, Hlc::new(9_000, 0));
        // A full batch of two, ending at 3_000 — from an origin that is not
        // even advertised, which must make no difference.
        let stranger = kimmy_core::NodeId::generate();
        let entries = vec![
            entry_from(stranger, Hlc::new(2_000, 0)),
            entry_from(stranger, Hlc::new(3_000, 0)),
        ];

        let covered = coverage_after_batch(&theirs, &entries, 2);
        assert_eq!(covered.get(early), Hlc::new(1_000, 0), "never past what the peer holds");
        assert_eq!(covered.get(inside), Hlc::new(2_500, 0));
        assert_eq!(covered.get(beyond), Hlc::new(3_000, 0), "clipped to the window end");
        assert_eq!(covered.get(stranger), Hlc::ZERO, "an unadvertised origin is not claimed");
    }

    fn entry_from(node: kimmy_core::NodeId, hlc: Hlc) -> OplogEntry {
        OplogEntry {
            stamp: kimmy_core::Stamp::new(hlc, node),
            kind: OpKind::Insert,
            collection: kimmy_core::CollectionId::derive("db", "c"),
            doc_id: Some(DocId::String("x".into())),
            body: None,
        }
    }

    #[test]
    fn a_stamp_that_never_ships_cannot_pin_a_peer_behind_a_full_batch() {
        // The production livelock (ADR-082), at its smallest.
        //
        // A's newest own stamp is a unique-violation entry: locally stamped,
        // advertised in A's vector, and never shipped (ADR-029). *Before* it,
        // in stamp order, A holds more than one batch of entries replicated
        // from C — which B has already witnessed directly from C. So B trails
        // A at one origin, A itself, from a floor below all of C's entries;
        // every window served from that floor is full of entries B has seen,
        // and the violation that would end the window sits beyond it.
        //
        // The order matters: a violation *inside* the raw window shortens the
        // batch (the limit is applied before the filter) and the short-batch
        // cure fires. The pin needs the unshippable stamp past a full window.
        const LIMIT: usize = 8;
        const DOCS: usize = 3 * LIMIT + 1;

        let (a, _da) = engine();
        let (b, _db) = engine();
        let (c, _dc) = engine();

        // (1) A creates the schema and one document; B witnesses A that far.
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "local", "email": "clash@x" }).unwrap();
        assert!(round(&b, &a, LIMIT).total() > 0);
        let floor = b.witnessed_vector().unwrap().get(a.node_id());
        assert!(floor > Hlc::ZERO);

        // (2) C writes more than a batch; A and B both take all of it from C.
        c.create_collection("shop", "orders").unwrap();
        let cc = c.get_collection("shop", "orders").unwrap();
        for i in 0..DOCS {
            c.insert(&cc, doc! { "_id": format!("c{i}"), "email": format!("{i}@c") }).unwrap();
        }
        for _ in 0..(DOCS / LIMIT + 2) {
            round(&a, &c, LIMIT);
            round(&b, &c, LIMIT);
        }
        assert!(a.witnessed_vector().unwrap().covers(&c.version_vector().unwrap()));
        assert!(b.witnessed_vector().unwrap().covers(&c.version_vector().unwrap()));

        // (3) A records a violation: its newest own stamp, never shipped, and
        // stamped after everything C wrote.
        let clash = OplogEntry {
            stamp: kimmy_core::Stamp::new(Hlc::new(9_000, 0), kimmy_core::NodeId::generate()),
            kind: OpKind::Insert,
            collection: ca.id,
            doc_id: Some(DocId::String("remote".into())),
            body: Some(
                bson::serialize_to_vec(&doc! { "_id": "remote", "email": "clash@x" }).unwrap(),
            ),
        };
        a.apply_remote(&ca, &clash).unwrap();
        assert_eq!(a.unique_violations(), 1);
        let a_adv = a.version_vector().unwrap();
        assert!(a_adv.get(a.node_id()) > c.version_vector().unwrap().get(c.node_id()));
        let shippable = a.entries_for_peer(floor, usize::MAX).unwrap();
        assert!(shippable.len() > LIMIT, "more than one full window lies under the violation");

        // B trails A, and the floor it would resume from is under all of it.
        assert!(b.witnessed_vector().unwrap().behind(&a_adv).is_some());

        // (4) Rounds from A must terminate, each consuming a window.
        let budget = DOCS / LIMIT + 4;
        let mut rounds = 0;
        let mut last = SyncOutcome::default();
        while rounds < budget {
            rounds += 1;
            last = round(&b, &a, LIMIT);
            if b.witnessed_vector().unwrap().covers(&a.version_vector().unwrap()) {
                break;
            }
        }
        assert!(
            b.witnessed_vector().unwrap().covers(&a.version_vector().unwrap()),
            "B never caught up with A in {budget} rounds; last round: {last:?}"
        );
        assert_eq!(last.lag_ms, 0, "and the gauge reads caught-up");
        assert_eq!(
            round(&b, &a, LIMIT),
            SyncOutcome::default(),
            "a further round transfers nothing"
        );
        // Nothing was invented and nothing lost: every document C wrote, A's
        // own, and the clashing remote one — a violation is reported, not
        // rejected (ADR-029), so it is a document on every node.
        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(b.count(&cb).unwrap() as usize, DOCS + 2);
    }

    #[test]
    fn a_fresh_engine_has_an_empty_vector() {
        let (engine, _dir) = engine();
        assert!(engine.version_vector().unwrap().is_empty());
    }

    #[test]
    fn lag_is_the_span_of_unapplied_history() {
        use kimmy_core::{NodeId, Stamp};

        let origin = NodeId::generate();
        let mut mine = VersionVector::new();
        mine.observe(Stamp::new(Hlc::new(10_000, 0), origin));
        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(Hlc::new(17_500, 0), origin));

        assert_eq!(lag_behind_ms(&mine, &theirs), 7_500, "7.5s of that origin is unapplied");
        assert_eq!(lag_behind_ms(&theirs, &mine), 0, "being ahead is not lag");
        assert_eq!(lag_behind_ms(&mine, &mine), 0, "caught up is zero");
    }

    #[test]
    fn lag_takes_the_worst_origin_not_the_sum() {
        use kimmy_core::{NodeId, Stamp};

        let (a, b) = (NodeId::generate(), NodeId::generate());
        let mut mine = VersionVector::new();
        mine.observe(Stamp::new(Hlc::new(1_000, 0), a));
        mine.observe(Stamp::new(Hlc::new(1_000, 0), b));
        let mut theirs = mine.clone();
        theirs.observe(Stamp::new(Hlc::new(2_000, 0), a));
        theirs.observe(Stamp::new(Hlc::new(9_000, 0), b));

        // An alert cares how far behind the worst origin is; summing origins
        // would report a cluster-wide write burst as one enormous lag.
        assert_eq!(lag_behind_ms(&mine, &theirs), 8_000);
    }

    #[test]
    fn a_discarded_entry_still_counts_as_witnessed() {
        // The bug ADR-054 fixes, at its smallest. A losing write is processed
        // correctly and appends nothing, so the *servable* vector cannot move.
        // The *witnessed* vector must, or the node re-requests that entry on
        // every sync round for the rest of its life.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let coll_a = a.create_collection("db", "c").unwrap();
        let coll_b = b.create_collection("db", "c").unwrap();

        // B writes second, so B's stamp wins.
        a.insert(&coll_a, doc! { "_id": 1, "v": "from-a" }).unwrap();
        b.insert(&coll_b, doc! { "_id": 1, "v": "from-b" }).unwrap();

        let losing = a.entries_for_peer(Hlc::ZERO, 100).unwrap();
        let outcome = b.apply_batch(&losing).unwrap();
        assert_eq!(outcome.applied, 0, "A's write must lose");
        assert!(outcome.superseded > 0);

        let a_origin = a.node_id();
        let servable = b.version_vector().unwrap();
        let witnessed = b.witnessed_vector().unwrap();

        // Replicated DDL *is* appended (`apply_ddl` records the originating
        // entry deliberately), so the servable vector moves for that. What it
        // cannot cover is the discarded document, which is strictly newer.
        assert!(
            witnessed.get(a_origin) > servable.get(a_origin),
            "the discarded insert is seen but not servable: servable={:?} witnessed={:?}",
            servable.get(a_origin),
            witnessed.get(a_origin)
        );
        assert!(
            witnessed.get(a_origin) > Hlc::ZERO,
            "but B has seen it, and must not ask for it again"
        );
        assert_eq!(
            witnessed.behind(&a.version_vector().unwrap()),
            None,
            "B is not behind A any more; a second round would be pointless"
        );
        assert_eq!(
            lag_behind_ms(&witnessed, &a.version_vector().unwrap()),
            0,
            "and the gauge must read caught-up, because it is"
        );
    }

    #[test]
    fn replicated_ddl_is_witnessed_even_though_it_is_never_logged() {
        // The universal case: applying a peer's schema change deliberately
        // appends nothing, so before ADR-054 *every* cluster re-requested
        // every DDL entry on every round, forever — an idle three-node cluster
        // merged 40 times in 20 seconds.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("db", "c").unwrap();

        let ddl = a.entries_for_peer(Hlc::ZERO, 100).unwrap();
        assert!(!ddl.is_empty(), "creating a collection logs an entry");
        let outcome = b.apply_batch(&ddl).unwrap();
        assert!(outcome.ddl > 0);

        assert_eq!(
            b.witnessed_vector().unwrap().behind(&a.version_vector().unwrap()),
            None,
            "a second round must find nothing left to ask for"
        );
    }

    #[test]
    fn an_origin_never_seen_contributes_no_lag() {
        use kimmy_core::{NodeId, Stamp};

        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(Hlc::new(1_786_000_000_000, 0), NodeId::generate()));

        // The peer's vector holds only the *newest* stamp per origin, so an
        // origin this node has never seen has no honest gap to report —
        // `newest − zero` would be the age of the epoch, a fifty-year lie a
        // joining node would alert on. Its lag becomes real with the first
        // applied batch.
        assert_eq!(lag_behind_ms(&VersionVector::new(), &theirs), 0);
    }

    #[test]
    fn a_synced_pair_reports_zero_lag() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let coll = a.create_collection("app", "docs").unwrap();
        for i in 0..5 {
            a.insert(&coll, doc! { "_id": i, "n": i }).unwrap();
        }
        sync(&a, &b);
        let (va, vb) = (a.version_vector().unwrap(), b.version_vector().unwrap());
        assert_eq!(lag_behind_ms(&vb, &va), 0, "a caught-up pair must read zero");
        assert_eq!(lag_behind_ms(&va, &vb), 0);
    }

    #[test]
    fn writing_advances_this_nodes_entry() {
        let (engine, _dir) = engine();
        let coll = engine.create_collection("db", "c").unwrap();
        engine.insert(&coll, doc! { "_id": 1 }).unwrap();

        let vector = engine.version_vector().unwrap();
        assert_eq!(vector.len(), 1);
        assert!(vector.get(engine.node_id()) > Hlc::ZERO);
    }

    #[test]
    fn the_vector_is_rebuilt_when_it_disagrees_with_the_oplog() {
        // Derived state, so a database written before it existed is repaired
        // rather than refused.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let (node, expected) = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("db", "c").unwrap();
            engine.insert(&coll, doc! { "_id": 1 }).unwrap();
            (engine.node_id(), engine.version_vector().unwrap())
        };

        {
            let db = redb::Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut versions = txn.open_table(crate::tables::OPLOG_VERSIONS).unwrap();
                versions.retain(|_, _| false).unwrap();
            }
            txn.commit().unwrap();
        }

        let reopened = Engine::open(&path).unwrap();
        assert_eq!(reopened.version_vector().unwrap(), expected);
        assert!(reopened.version_vector().unwrap().get(node) > Hlc::ZERO);
    }

    #[test]
    fn two_engines_converge_after_one_round() {
        let (a, dir_a) = engine();
        let (b, dir_b) = engine();
        // Same names, so both derive the same collection id — the property that
        // makes any of this work at all.
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();

        a.insert(&ca, doc! { "_id": "from-a", "v": 1 }).unwrap();
        b.insert(&cb, doc! { "_id": "from-b", "v": 2 }).unwrap();

        sync(&a, &b);

        for (engine, coll) in [(&a, &ca), (&b, &cb)] {
            assert!(engine.get(coll, &DocId::String("from-a".into())).unwrap().is_some());
            assert!(engine.get(coll, &DocId::String("from-b".into())).unwrap().is_some());
        }
        drop((dir_a, dir_b));
    }

    #[test]
    fn a_second_round_transfers_nothing() {
        // Convergence has to be stable, or peers would ship the same entries
        // forever.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        b.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();

        sync(&a, &b);
        let second = pull(&b, &a);

        assert_eq!(second, SyncOutcome::default(), "a converged pair must exchange nothing");
    }

    #[test]
    fn conflicting_writes_converge_to_the_same_document() {
        // Both nodes write the same _id concurrently. LWW decides, and both
        // sides must land on the same winner.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();

        a.replace(&ca, &DocId::Int64(1), doc! { "_id": 1, "who": "a" }, true).unwrap();
        b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "who": "b" }, true).unwrap();

        sync(&a, &b);
        sync(&a, &b);

        let from_a = a.get(&ca, &DocId::Int64(1)).unwrap().unwrap();
        let from_b = b.get(&cb, &DocId::Int64(1)).unwrap().unwrap();
        assert_eq!(from_a, from_b, "both nodes must agree on the winner");
    }

    #[test]
    fn a_deletion_replicates_as_a_deletion() {
        // The tombstone is what stops the delete being undone by a peer that
        // still holds the document.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();

        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        sync(&a, &b);
        assert!(b.get(&cb, &DocId::Int64(1)).unwrap().is_some());

        a.delete(&ca, &DocId::Int64(1)).unwrap();
        sync(&a, &b);

        assert!(b.get(&cb, &DocId::Int64(1)).unwrap().is_none(), "the delete must replicate");
    }

    #[test]
    fn a_pre_recreation_entry_is_suppressed_even_when_it_ties_with_the_drop() {
        // The deterministic form of the CI flake: the recreated collection
        // derives the same id, and an entry whose stamp ties with the drop
        // must still be filtered — by the incarnation floor (the drop's own
        // stamp, compared inclusively), not by millisecond luck in a strict
        // tombstone comparison.
        let (a, _da) = engine();
        let ca_old = {
            a.create_collection("shop", "orders").unwrap();
            a.get_collection("shop", "orders").unwrap()
        };

        a.drop_collection("shop", "orders").unwrap();
        let ca_new = a.create_collection("shop", "orders").unwrap();
        assert_eq!(ca_old.id, ca_new.id, "the id is derived from db and name");
        let dropped_at = a.collection_dropped_at(ca_new.id).unwrap().unwrap().hlc;
        assert_eq!(
            ca_new.incarnation_floor,
            Some(dropped_at),
            "the recreation carries the drop as its incarnation floor"
        );

        // Stamps at or below the floor are the previous life — including the
        // exact tie with the drop, which is where the old strict tombstone
        // comparison let a peer's final pre-drop write walk into the
        // replacement.
        for hlc in [ca_old.created, dropped_at] {
            let entry = OplogEntry {
                stamp: kimmy_core::Stamp::new(hlc, kimmy_core::NodeId::generate()),
                kind: OpKind::Insert,
                collection: ca_new.id,
                doc_id: Some(DocId::String("ghost".into())),
                body: Some(bson::serialize_to_vec(&doc! { "_id": "ghost" }).unwrap()),
            };
            let outcome = a.apply_batch(&[entry]).unwrap();
            assert_eq!(
                outcome.superseded, 1,
                "hlc {hlc:?}: a pre-recreation entry must not enter the new incarnation"
            );
            assert_eq!(a.count(&ca_new).unwrap(), 0);
        }

        // And genuinely post-recreation writes still apply: the floor is not
        // a lid over the incarnation, only over its past.
        for offset in [1u64, 2] {
            let entry = OplogEntry {
                stamp: kimmy_core::Stamp::new(
                    Hlc::new(dropped_at.wall_ms + offset, 0),
                    kimmy_core::NodeId::generate(),
                ),
                kind: OpKind::Insert,
                collection: ca_new.id,
                doc_id: Some(DocId::String(format!("live-{offset}"))),
                body: Some(
                    bson::serialize_to_vec(&doc! { "_id": format!("live-{offset}") }).unwrap(),
                ),
            };
            let outcome = a.apply_batch(&[entry]).unwrap();
            assert_eq!(outcome.applied, 1, "post-recreation writes must apply (offset {offset})");
        }
        assert_eq!(a.count(&ca_new).unwrap(), 2);
    }

    #[test]
    fn unique_violation_entries_are_never_sent() {
        // They are this node's observation of a collision, and every node makes
        // the same observation when it merges. Sending them would report one
        // violation once per node.
        let (a, _da) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "local", "email": "clash@x" }).unwrap();

        let entry = OplogEntry {
            stamp: kimmy_core::Stamp::new(Hlc::new(9_000, 0), kimmy_core::NodeId::generate()),
            kind: OpKind::Insert,
            collection: ca.id,
            doc_id: Some(DocId::String("remote".into())),
            body: Some(
                bson::serialize_to_vec(&doc! { "_id": "remote", "email": "clash@x" }).unwrap(),
            ),
        };
        a.apply_remote(&ca, &entry).unwrap();
        assert_eq!(a.unique_violations(), 1, "the collision must have been recorded");

        let outgoing = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap();
        assert!(
            outgoing.iter().all(|e| e.kind != OpKind::UniqueViolation),
            "a violation entry must not be replicated"
        );
        assert!(!outgoing.is_empty(), "ordinary entries must still be sent");
    }

    #[test]
    fn a_collection_created_on_one_node_appears_on_the_other() {
        // Schema changes replicate, so a peer no longer has to be told about a
        // collection out of band before documents for it can arrive.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();

        let outcome = pull(&b, &a);

        assert!(outcome.ddl > 0, "the creation must have been applied: {outcome:?}");
        assert_eq!(outcome.unknown_collection, 0, "nothing should be skipped: {outcome:?}");
        let cb = b.get_collection("shop", "orders").expect("the collection must exist on b");
        assert_eq!(cb.id, ca.id, "and address the same storage");
        assert!(b.get(&cb, &DocId::Int64(1)).unwrap().is_some());
    }

    #[test]
    fn a_document_whose_collection_creation_aged_out_is_counted() {
        // The remaining gap, and the reason the counter stays: if the peer's
        // CreateCollection entry has been collected by retention, a document
        // entry arrives for a collection this node cannot learn the name of.
        let (b, _db) = engine();

        let orphan = OplogEntry {
            stamp: kimmy_core::Stamp::new(Hlc::new(1_000, 0), kimmy_core::NodeId::generate()),
            kind: OpKind::Insert,
            collection: kimmy_core::CollectionId::derive("shop", "never-heard-of"),
            doc_id: Some(DocId::Int64(1)),
            body: Some(bson::serialize_to_vec(&doc! { "_id": 1 }).unwrap()),
        };

        let outcome = b.apply_batch(&[orphan]).unwrap();

        assert_eq!(outcome.unknown_collection, 1, "the gap must be counted: {outcome:?}");
        assert_eq!(outcome.applied, 0);
    }

    #[test]
    fn an_index_replicates_with_its_definition() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();

        pull(&b, &a);

        let cb = b.get_collection("shop", "orders").unwrap();
        let index = cb.indexes.iter().find(|i| i.name == "email_1").expect("the index must exist");
        assert!(index.unique, "a unique constraint must replicate as unique");
        assert_eq!(index.id, kimmy_core::IndexMeta::derive_id("email_1"));
    }

    #[test]
    fn concurrent_index_additions_both_survive() {
        // The reason schema changes are separate operations rather than one
        // metadata snapshot: whole-metadata last-writer-wins would keep only
        // the later of these two and silently lose the other.
        let (a, _da) = engine();
        let (b, _db) = engine();
        for engine in [&a, &b] {
            engine.create_collection("shop", "orders").unwrap();
        }

        a.create_index("shop", "orders", vec![field("email")], false, None).unwrap();
        b.create_index("shop", "orders", vec![field("status")], false, None).unwrap();

        sync(&a, &b);
        sync(&a, &b);

        for engine in [&a, &b] {
            let names: Vec<String> = engine
                .get_collection("shop", "orders")
                .unwrap()
                .indexes
                .iter()
                .map(|i| i.name.clone())
                .collect();
            assert!(names.contains(&"email_1".to_string()), "lost email_1: {names:?}");
            assert!(names.contains(&"status_1".to_string()), "lost status_1: {names:?}");
        }
    }

    #[test]
    fn dropping_an_index_replicates() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], false, None).unwrap();
        sync(&a, &b);
        assert!(!b.get_collection("shop", "orders").unwrap().indexes.is_empty());

        a.drop_index("shop", "orders", "email_1").unwrap();
        sync(&a, &b);

        assert!(
            b.get_collection("shop", "orders").unwrap().indexes.is_empty(),
            "the drop must replicate too"
        );
    }

    #[test]
    fn vector_configuration_replicates_and_can_be_turned_off() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.configure_vectors("shop", "orders", vector_config()).unwrap();

        sync(&a, &b);
        assert!(
            b.get_collection("shop", "orders").unwrap().vector.is_some(),
            "embedding settings must replicate, or nodes would disagree about what to embed"
        );

        a.disable_vectors("shop", "orders", false).unwrap();
        sync(&a, &b);

        assert!(
            b.get_collection("shop", "orders").unwrap().vector.is_none(),
            "turning it off must replicate as well"
        );
    }

    #[test]
    fn replicated_schema_changes_are_idempotent() {
        // Peers resend overlapping ranges, so applying the same creation twice
        // must not error or duplicate anything.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], false, None).unwrap();

        let entries = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap();
        b.apply_batch(&entries).unwrap();
        b.apply_batch(&entries).unwrap();

        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(cb.indexes.len(), 1, "a resend must not duplicate the index");
    }

    #[test]
    fn replicating_a_schema_change_does_not_amplify_it() {
        // Applying a replicated DDL entry must not mint a local one. If it did,
        // the peer would pull that back, apply it, mint another, and the two
        // nodes would trade the same change forever — the oplog growing on
        // every round while nothing changed.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], false, None).unwrap();

        sync(&a, &b);
        let after_first = (
            a.read_arrival_from(0, 10_000).unwrap().len(),
            b.read_arrival_from(0, 10_000).unwrap().len(),
        );

        for _ in 0..5 {
            sync(&a, &b);
        }
        let after_five_more = (
            a.read_arrival_from(0, 10_000).unwrap().len(),
            b.read_arrival_from(0, 10_000).unwrap().len(),
        );

        assert_eq!(
            after_first, after_five_more,
            "repeated rounds must transfer nothing new; the oplog grew from {after_first:?} \
             to {after_five_more:?}"
        );
    }

    #[test]
    fn a_replicated_change_keeps_its_originating_stamp() {
        // The entry a peer stores has to be the one that was sent, not a
        // re-stamped copy: version vectors are keyed by originating node, so a
        // local stamp would make the peer look like the author and leave the
        // real origin permanently outstanding.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();

        sync(&a, &b);

        let vector = b.version_vector().unwrap();
        assert!(
            vector.get(a.node_id()) > Hlc::ZERO,
            "b must record coverage of a's writes under a's node id"
        );
        assert!(b.version_vector().unwrap().covers(&a.version_vector().unwrap()));
    }

    #[test]
    fn a_partitioned_peer_cannot_resurrect_a_dropped_collection() {
        // The scenario collection tombstones exist for.
        //
        // The partitioned peer has to keep *writing* for this to arise: only
        // then does the dropper fall behind it, and only then does it request
        // from the beginning and receive the peer's copy of the original
        // CreateCollection entry. Without that, the dropper is ahead of the
        // peer on every node and asks for nothing — which is why an earlier
        // version of this test passed even with the check removed.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1, "v": "original" }).unwrap();
        sync(&a, &b);

        let cb = b.get_collection("shop", "orders").unwrap();
        assert!(b.get(&cb, &DocId::Int64(1)).unwrap().is_some(), "b must start out holding it");

        // A drops the collection while B is unreachable...
        a.drop_collection("shop", "orders").unwrap();
        // ...and B, still partitioned, keeps serving writes to it.
        b.insert(&cb, doc! { "_id": 2, "v": "written during the partition" }).unwrap();

        // The drop ages out of A's oplog, leaving *only* the tombstone.
        // Tombstone retention is deliberately unbounded here: the point is the
        // window in which the oplog has forgotten and the tombstone has not.
        a.collect_garbage_at(
            crate::engine::physical_now_ms() + 1_000_000_000,
            RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();

        // B rejoins. A is now behind on B, so it asks from the beginning and
        // receives B's copy of the creation.
        pull(&a, &b);

        assert!(
            a.get_collection("shop", "orders").is_err(),
            "the dropped collection must not come back"
        );
    }

    #[test]
    fn documents_written_before_a_drop_do_not_return_to_a_recreated_collection() {
        // Recreating the collection is one route back; replaying its documents
        // into a node that has since recreated it is another.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        sync(&a, &b);

        // B keeps writing into the doomed collection while partitioned.
        let cb = b.get_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": 2 }).unwrap();

        a.drop_collection("shop", "orders").unwrap();
        let ca = a.create_collection("shop", "orders").unwrap();

        pull(&a, &b);

        assert_eq!(
            a.count(&ca).unwrap(),
            0,
            "documents written before the drop must not flow back into the recreated collection"
        );
    }

    #[test]
    fn a_drop_still_replicates_to_a_peer_that_never_had_the_collection() {
        // The tombstone has to be recorded even when there is nothing local to
        // remove, or a third node could later reintroduce the collection.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        a.drop_collection("shop", "orders").unwrap();

        pull(&b, &a);

        let id = kimmy_core::CollectionId::derive("shop", "orders");
        assert!(
            b.collection_dropped_at(id).unwrap().is_some(),
            "b must remember the drop even though it never held the collection"
        );
        assert!(b.get_collection("shop", "orders").is_err());
    }

    #[test]
    fn recreating_after_a_drop_beats_the_tombstone() {
        // A tombstone must not make a name permanently unusable: a creation
        // stamped after the drop is a new collection, not a resurrection.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.drop_collection("shop", "orders").unwrap();
        let recreated = a.create_collection("shop", "orders").unwrap();
        a.insert(&recreated, doc! { "_id": "new" }).unwrap();

        pull(&b, &a);

        let cb = b.get_collection("shop", "orders").expect("the recreation must replicate");
        assert!(b.get(&cb, &DocId::String("new".into())).unwrap().is_some());
    }

    #[test]
    fn collection_tombstones_are_collected_on_the_tombstone_window() {
        // They answer the same question as document tombstones over the same
        // window, so they expire on the same setting rather than on the oplog's.
        let (a, _da) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.drop_collection("shop", "orders").unwrap();
        let id = kimmy_core::CollectionId::derive("shop", "orders");
        assert!(a.collection_dropped_at(id).unwrap().is_some());

        let outcome = a
            .collect_garbage_at(
                crate::engine::physical_now_ms() + 1_000_000_000,
                RetentionPolicy::new(DAY, 0),
            )
            .unwrap();

        assert!(outcome.tombstones_removed > 0);
        assert!(a.collection_dropped_at(id).unwrap().is_none());
    }

    fn vector_config() -> kimmy_core::VectorConfig {
        kimmy_core::VectorConfig {
            fields: vec!["text".into()],
            provider: kimmy_core::ProviderConfig::Byo,
            dim: 4,
            metric: Default::default(),
            chunk: Default::default(),
        }
    }

    #[test]
    fn a_node_joining_late_receives_the_whole_history() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        for i in 0..50i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }

        sync(&a, &b);

        assert_eq!(b.count(&cb).unwrap(), 50, "an empty peer must catch up from zero");
    }

    #[test]
    fn three_nodes_converge_through_a_middle_peer() {
        // A and C never talk directly. Convergence has to be transitive, or a
        // partially connected cluster silently diverges.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let (c, _dc) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        let cc = c.create_collection("shop", "orders").unwrap();

        a.insert(&ca, doc! { "_id": "a" }).unwrap();
        c.insert(&cc, doc! { "_id": "c" }).unwrap();

        sync(&a, &b);
        sync(&b, &c);
        sync(&a, &b);

        for (engine, coll) in [(&a, &ca), (&b, &cb), (&c, &cc)] {
            assert!(engine.get(coll, &DocId::String("a".into())).unwrap().is_some());
            assert!(engine.get(coll, &DocId::String("c".into())).unwrap().is_some());
        }
    }

    #[test]
    fn a_node_behind_on_two_peers_at_different_points_receives_both() {
        // The case the simpler convergence tests miss, and the reason the
        // request threshold is the *lowest* deficient point rather than any
        // other: C is behind on both A and B, and what it is missing from B is
        // stamped EARLIER than what it already holds from A.
        //
        // Starting the request at C's position for A would skip B's older entry
        // entirely — silently, since nothing else would notice.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let (c, _dc) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        let cc = c.create_collection("shop", "orders").unwrap();

        // Oldest write in the cluster, and C never hears of it directly.
        b.insert(&cb, doc! { "_id": "b-oldest" }).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));

        // C catches up with A, so its vector holds a *non-zero* mark for A that
        // is later than B's write.
        a.insert(&ca, doc! { "_id": "a-first" }).unwrap();
        sync(&a, &c);
        std::thread::sleep(std::time::Duration::from_millis(5));

        // A writes again, so C is behind on A as well — but only slightly.
        a.insert(&ca, doc! { "_id": "a-second" }).unwrap();

        // B learns everything, so a single peer can serve both histories.
        sync(&a, &b);
        pull(&c, &b);

        for id in ["b-oldest", "a-first", "a-second"] {
            assert!(
                c.get(&cc, &DocId::String(id.into())).unwrap().is_some(),
                "{id} was skipped; the request started later than the earliest gap"
            );
        }
    }

    fn field(path: &str) -> crate::meta::IndexField {
        crate::meta::IndexField { path: path.into(), descending: false }
    }

    #[test]
    fn the_same_lag_measure_reversed_says_how_far_a_peer_trails_us() {
        // What the stale-rejoiner check runs on (ADR-085): how far the peer
        // is behind *this* node, per origin, ignoring origins it has never
        // seen at all.
        use kimmy_core::{NodeId, Stamp};
        let a = NodeId::from_bytes([1; 16]);
        let b = NodeId::from_bytes([2; 16]);
        let c = NodeId::from_bytes([3; 16]);

        let mut mine = VersionVector::default();
        mine.observe(Stamp::new(Hlc::new(100_000, 0), a));
        mine.observe(Stamp::new(Hlc::new(90_000, 0), b));
        mine.observe(Stamp::new(Hlc::new(50_000, 0), c));

        // The peer last saw `a` at 10s, `b` at 90s (level), never `c`.
        let mut theirs = VersionVector::default();
        theirs.observe(Stamp::new(Hlc::new(10_000, 0), a));
        theirs.observe(Stamp::new(Hlc::new(90_000, 0), b));

        assert_eq!(lag_behind_ms(&theirs, &mine), 90_000, "90 s behind on `a`; `c` does not count");
        assert_eq!(lag_behind_ms(&mine, &theirs), 0, "and we trail it by nothing");

        // A fresh member trails nobody: it holds nothing old enough to resurrect.
        assert_eq!(lag_behind_ms(&VersionVector::default(), &mine), 0);
    }
}
