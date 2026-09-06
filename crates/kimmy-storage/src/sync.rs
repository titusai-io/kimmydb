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

use std::collections::HashMap;
use std::sync::Arc;

use kimmy_core::{CollectionId, DocId, Hlc, NodeId, OpKind, OplogEntry, Stamp, VersionVector};
use tracing::{debug, info, warn};

use crate::docs::RemoteApplied;
use crate::engine::{Engine, WriteTxn};
use crate::error::Result;
use crate::index::UniqueViolation;
use crate::meta::CollectionMeta;
use crate::watch::OplogWindow;

/// What applying a batch of replicated entries did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
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
    /// Replicated schema changes this node could not apply to its current
    /// state and skipped: a definition this build cannot apply, or a name
    /// already taken by a different definition it cannot arbitrate
    /// (ADR-123). Not a definition its documents do not fit — that is built,
    /// with those documents filed unkeyed under it (ADR-139).
    ///
    /// Counted rather than failed, because the refusal is a fact about this
    /// node's data and retrying the entry unchanged can never succeed; the
    /// entry is witnessed so it is not re-served, and the divergence is
    /// visible here, in the log, and on `/metrics`.
    pub ddl_refused: usize,
    /// Replicated index drops this node declined because the index standing
    /// under the name here was created *after* the drop (ADR-132): a drop
    /// re-served past the recreation it preceded, which is the rule doing
    /// its job, or a drop from a member whose clock trailed the creator's,
    /// which leaves this member holding an index its peers have dropped.
    /// The witnessed vector keeps the first rare, so a count that keeps
    /// rising is the second (ADR-141).
    pub ddl_declined: usize,
    /// The peer the round was with, once it has introduced itself.
    pub peer: Option<NodeId>,
    /// How far the peer trails *this* node, in milliseconds of history —
    /// the span between its coverage and ours, counted only at origins where
    /// the peer also lacks an entry retention has removed here. Above
    /// tombstone retention it names a stale rejoiner (ADR-085); zero for a
    /// peer that can still be served every entry it lacks, however wide the
    /// gap (ADR-097). A different question from `lag_ms`, and a different
    /// measure. See [`lag_beyond_horizon_ms`].
    pub behind_ms: u64,
    /// How far behind in time this node is after the round, in milliseconds:
    /// the age of the newest entry it has applied from an origin a peer holds
    /// newer entries of, worst origin.
    ///
    /// Zero when caught up. Grows with the wall clock while a backlog is
    /// being drained, which is what an operator alerts on; the span of the
    /// history still missing does not, and read 0 for a bulk insert whose
    /// stamps all lie within a second (ADR-122). See [`lag_behind_ms`].
    pub lag_ms: u64,
    /// Whether this round's pull reached the peer's true tail: the oplog
    /// window ended because the peer's log ran out, not because the batch
    /// cap was spent (ADR-127's `exhausted`, carried through). This is a
    /// convergence claim independent of the clock — the one signal that
    /// would have contradicted `lag_ms == 0` during finding 14, since a
    /// round can be mid-backlog with `lag_ms` still reading low for a bulk
    /// insert whose stamps cluster within a second (ADR-122) while
    /// `exhausted` correctly reads `false`.
    ///
    /// **Broader than [`crate::watch::OplogWindow::exhausted`], which this
    /// field is not always a plain copy of.** `OplogWindow::exhausted`
    /// answers one narrower question — did this particular oplog scan reach
    /// the end — and says nothing when no scan happened at all. This field
    /// is also `true` in two cases `OplogWindow` never covers: when there
    /// was nothing to pull, so no `Entries` message was exchanged at all,
    /// and when a `BeyondHorizon` snapshot pull completed (a full snapshot
    /// is, by construction, everything the peer held as of the pull — the
    /// snapshot's own version of "reached the tail"). Both are set by
    /// `kimmy-cluster`'s `sync_once`, which owns this broader claim; nothing
    /// in this module computes either. `false` on every path exercised by
    /// the network-free tests in this module other than an explicit
    /// exhausted batch.
    pub exhausted: bool,
    /// The existence half of the cross-member divergence check (ADR-133):
    /// collections the peer holds that this node does not, found this
    /// round. `None` when the check did not run this round at all — a round
    /// whose pull did not reach the peer's tail (`exhausted == false`) skips
    /// it, because that is precisely the state a truncated sync window can
    /// fake without being true. `Some` (possibly holding an empty set) once
    /// it ran, which is whenever `exhausted` is `true`. See
    /// [`crate::divergence::compare`] for what it does and does not report,
    /// and why the count half below is carried separately rather than
    /// folded into this set.
    /// `None` on every path exercised by the network-free tests in this
    /// module: the check itself lives in `kimmy-cluster`, which is the only
    /// place a peer's answer exists.
    pub divergent: Option<std::collections::BTreeSet<kimmy_core::CollectionId>>,
    /// The count half: the collection probed for a document count this
    /// round, and whether it disagreed. Carried apart from `divergent`
    /// because the two halves confirm on different rhythms — the existence
    /// half is checked in full every round the check runs at all, while
    /// only one collection is probed per round, so folding a count finding
    /// into the same set as existence findings is what made a count
    /// divergence structurally unconfirmable on any node holding more than
    /// one collection (see [`crate::divergence::DivergenceTracker::observe`]).
    /// `None` under the same conditions as `divergent`, and additionally
    /// whenever no collection was probed or the peer's answer for it was
    /// judged untrustworthy this round (a lagging peer; see
    /// `divergence_probe_for` in `kimmy-cluster`).
    pub count_probe: Option<(kimmy_core::CollectionId, bool)>,
}

/// How far behind in time `mine` is against `theirs`, in milliseconds, as of
/// `now_ms`.
///
/// For every origin where the peer's coverage is ahead, the answer is how
/// long ago the newest entry this node has applied from that origin was
/// written: `now − held`. The maximum over origins is what an operator
/// alerts on. A node thirty seconds into draining a backlog reads thirty
/// seconds; caught up, it reads zero; holding everything but an entry written
/// two seconds ago, it reads two seconds, which is the truth.
///
/// Not the span between the two heads, `theirs − held`, which is what this
/// measured before ADR-122. That is the width of the window of history still
/// missing, and a bulk insert mints all its stamps within a few hundred
/// milliseconds: on a three-member cluster a replica that held 351 of a
/// peer's 1,000 documents for 78 seconds, and later trailed by 4,000 for
/// 492 seconds, read 0 throughout, and only rose once several writers had
/// spread their writes over many minutes. The gauge answered "how wide is
/// the window I lack", and nobody asks that.
///
/// The known limit, which the span had too: an origin quiet for hours that
/// then writes once leaves every peer reading the length of the silence for
/// one round, until the entry is pulled. The peer's head vector carries no
/// oldest-unapplied stamp that would say the gap holds one entry a second
/// old, so nothing here can tell that spike from a real backlog. The
/// stale-rejoiner verdict, which cannot afford the spike, does not use this
/// measure at all; see [`lag_beyond_horizon_ms`].
///
/// The measure crosses clocks: `held.wall_ms` is the origin's HLC wall time,
/// `now_ms` this node's. A peer whose clock runs ahead of this node's makes
/// an origin this node genuinely trails saturate to zero, and the gauge
/// under-reports by the skew; a peer whose clock runs behind adds it. The
/// span did not have this problem, comparing two stamps of one origin. The
/// error is bounded by the skew the HLC already tolerates between members,
/// and under-reporting by a few seconds is a smaller lie than reading zero
/// through a backlog that lasts minutes.
///
/// An origin this node has **never** seen contributes nothing: with only the
/// peer's *newest* timestamp to hand, the honest gap would need the oldest,
/// and `now − zero` is the age of the epoch, not of the backlog. A joining
/// node's lag becomes meaningful with its first applied batch — moments in —
/// rather than starting at a fifty-year lie.
pub fn lag_behind_ms(mine: &VersionVector, theirs: &VersionVector, now_ms: u64) -> u64 {
    theirs
        .iter()
        .filter_map(|(node, hlc)| {
            let held = mine.get(node);
            (held > Hlc::ZERO && hlc > held).then(|| now_ms.saturating_sub(held.wall_ms))
        })
        .max()
        .unwrap_or(0)
}

/// Whether `held` lacks, at any origin `mine` is ahead of it, an entry that
/// retention has removed here — the highest removed per origin being
/// `collected` ([`Engine::oplog_collected`]).
///
/// Only origins the peer trails count. An origin it has caught up on has no
/// gap to hold anything, and an origin whose newest entry sits below a
/// collected stamp of *another* origin is not behind anything.
pub fn lacks_collected(
    held: &VersionVector,
    mine: &VersionVector,
    collected: &VersionVector,
) -> bool {
    mine.iter().any(|(node, newest)| {
        let theirs = held.get(node);
        theirs < newest && theirs < collected.get(node)
    })
}

/// How far `theirs` trails `mine` at the origins where it also lacks an
/// entry retention has removed here; zero when it lacks nothing collected.
///
/// The span between the two heads, roles swapped, is what named a stale
/// rejoiner (ADR-085): a peer more than tombstone retention behind may hold
/// documents whose deletes it never saw and whose tombstones are gone. But
/// the span between two stamps is the age of the *gap*, not of anything in
/// it. An origin that wrote nothing for longer than retention and then wrote
/// once leaves every peer a gap as wide as its silence holding one entry a few
/// seconds old, which each peer pulls on its next round — and a peer that can
/// still be served every entry it lacks has nothing to resurrect. So the
/// verdict requires both: the span, and something in it that retention has
/// removed, which is what `collected` records per origin (ADR-097).
pub fn lag_beyond_horizon_ms(
    theirs: &VersionVector,
    mine: &VersionVector,
    collected: &VersionVector,
) -> u64 {
    mine.iter()
        .filter_map(|(node, newest)| {
            let held = theirs.get(node);
            let lacking = held > Hlc::ZERO && newest > held && held < collected.get(node);
            lacking.then(|| newest.wall_ms.saturating_sub(held.wall_ms))
        })
        .max()
        .unwrap_or(0)
}

impl SyncOutcome {
    pub fn total(&self) -> usize {
        self.applied
            + self.superseded
            + self.ddl
            + self.unknown_collection
            + self.ddl_refused
            + self.ddl_declined
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
/// The window's end is the peer's own answer, not a deduction from the batch:
/// `scanned_to` is the last stamp the peer's scan examined, and `exhausted`
/// says whether it stopped there because the oplog ended (ADR-127).
///
/// - An **exhausted** window is the peer's whole tail, so it runs to the end of
///   everything the peer advertised: the answer is `theirs` itself.
/// - A window that stopped at the batch limit ends at `scanned_to`. Each
///   advertised origin is raised to the *lower* of that stamp and the peer's
///   own coverage of it: never past what the peer holds, and never past what it
///   read. Ties at that stamp from origins with a higher node id sort after it
///   and were not served — they are re-served next round, because the range
///   read is inclusive at the stamp asked for.
///
/// Why the second case exists at all: an advertised stamp this node can
/// never receive — a violation, a stamp whose entry sits behind the window —
/// otherwise pins `VersionVector::behind` at that origin's floor, and once the
/// peer holds a full batch after that floor, every round re-serves the same
/// already-witnessed window forever. Observed as a cluster whose sync log read
/// `applied=0, superseded=1021` every five seconds for the life of the
/// cluster, and whose lag gauge read the cluster's age. See ADR-082.
///
/// Why the peer reports the end rather than the receiver counting entries: the
/// count was a *proxy* for "the tail was reached", and it stopped being a true
/// one the moment anything was dropped from a window after the limit was
/// spent. A `UniqueViolation` inside a window truncated at 1,024 made a
/// 1,019-entry batch, which read as the whole tail, so every entry past the
/// window was witnessed without ever being applied and nothing re-served it —
/// silent, permanent divergence with every health signal green. ADR-126 takes
/// the cap after the filter so the count is honest again; this rule no longer
/// depends on the count at all, so the next filter cannot reopen the hole.
pub fn coverage_after_batch(
    theirs: &VersionVector,
    scanned_to: Hlc,
    exhausted: bool,
) -> VersionVector {
    if exhausted {
        return theirs.clone();
    }
    let mut covered = VersionVector::new();
    for (node, their_max) in theirs.iter() {
        covered.insert(node, their_max.min(scanned_to));
    }
    covered
}

impl Engine {
    /// Merge a batch a peer served in answer to `AskEntries`, and record what
    /// the batch proved about coverage.
    ///
    /// The single path the replication transport uses for a served batch:
    /// [`Self::apply_batch`] for the entries, with the witnessed vector raised
    /// by [`coverage_after_batch`] in the same transaction as the batch's last
    /// run of documents — so a round with no schema changes in it is one
    /// commit and one fsync, not one per entry plus two (ADR-119).
    /// `scanned_to` and `exhausted` are the peer's report of where its window
    /// ended, which is what decides whether its tail was reached (ADR-127).
    ///
    /// **A window that is not a tail is clamped to what it actually carried,
    /// and one that carried nothing claims nothing.** The window's end moved
    /// from something this node computes to something the peer asserts, and an
    /// assertion crossing the wire is checked here or nowhere: a sender that
    /// trimmed a batch in place but reported the end it had scanned to would
    /// witness away every entry it dropped, which is finding 14 from one wrong
    /// field. `Message::BatchTooLarge` tells a sender not to do that, and this
    /// is the same rule as an invariant rather than as prose.
    ///
    /// A correct sender is unaffected, so the clamp is a no-op today and a
    /// floor afterwards. That includes the empty case, which is why it is
    /// clamped rather than exempted: `read_oplog_from_where` stops only after
    /// keeping an entry, so `!exhausted` implies `entries.len() == limit`, and
    /// a correct sender **cannot** emit an empty window that is not a tail. An
    /// empty one that claims a stamp anyway is therefore always a broken or
    /// hostile peer, and letting it through would absorb the peer's whole
    /// vector in exchange for nothing — a worse form of the same defect than
    /// a trimmed batch.
    ///
    /// An *exhausted* window is exempt, and cannot be checked: absorbing the
    /// peer's advertised vector on exhaustion is ADR-082 itself, and this node
    /// holds nothing to test the claim against. A peer that reports
    /// `exhausted` falsely is trusted, necessarily.
    pub fn apply_peer_batch(
        &self,
        theirs: &VersionVector,
        entries: &[OplogEntry],
        scanned_to: Hlc,
        exhausted: bool,
    ) -> Result<SyncOutcome> {
        let scanned_to = if exhausted {
            scanned_to
        } else {
            entries.last().map_or(Hlc::ZERO, |last| scanned_to.min(last.stamp.hlc))
        };
        self.apply_batch_absorbing(
            entries,
            Some(&coverage_after_batch(theirs, scanned_to, exhausted)),
        )
    }

    /// The window at or after `from` a peer that asked to catch up may be
    /// served, with the stamp its scan reached and whether the oplog ran out.
    ///
    /// Stamp order, not arrival order: a peer's question is "what do you hold
    /// after this logical time", which is about origin stamps. Change streams
    /// ask a different question and use the arrival index instead.
    ///
    /// **Unique-violation entries are excluded.** They record what *this* node
    /// observed when it merged, and every node observes the same collision
    /// independently — shipping them would report one violation once per node.
    /// See [ADR-029](../../../docs/decisions.md).
    ///
    /// `limit` counts the entries that survive that exclusion, not the entries
    /// read: a window truncated at the cap holds `limit` shippable entries, so
    /// a shorter one really is the end of the oplog. Spending the cap first and
    /// filtering afterwards is what let a withheld violation disguise a
    /// truncated window as a tail (ADR-126).
    pub fn entries_for_peer(&self, from: Hlc, limit: usize) -> Result<OplogWindow> {
        self.read_oplog_from_where(from, limit, |entry| entry.kind != OpKind::UniqueViolation)
    }

    /// Merge a batch of entries received from a peer.
    ///
    /// Each document entry goes through `apply_remote_in_txn`, so
    /// last-writer-wins decides per document and re-delivery is harmless.
    /// Ordering within the batch does not matter to the *result* — that is the
    /// point of LWW, and it is what lets a peer send a range without
    /// coordinating — but the entries are applied in the order given, which is
    /// stamp order, so every per-entry check sees what the entries before it
    /// did.
    ///
    /// One transaction per **run** of consecutive document entries, and one
    /// commit per run; a batch with no schema change in it is exactly one
    /// commit. See [`Self::apply_batch_absorbing`] for the shape.
    pub fn apply_batch(&self, entries: &[OplogEntry]) -> Result<SyncOutcome> {
        self.apply_batch_absorbing(entries, None)
    }

    /// [`Self::apply_batch`], also raising the witnessed vector by `extra`
    /// inside the batch's final transaction.
    ///
    /// The shape, and why (ADR-119). Before this, every entry in a batch was
    /// its own transaction: a client bulk insert of 1,000 documents was one
    /// commit on the node that accepted it and about 1,000 commits — each an
    /// fsync under `durable` — on every node that replicated it, plus one for
    /// the witnessed vector and one more for the coverage vector. Measured on
    /// a three-member cluster: `kimmy_commits` and `kimmy_fsyncs` rose one per
    /// replicated document, a 1,024-entry batch took about 145 s to apply, and
    /// replication ran at 8–13 documents a second.
    ///
    /// Now consecutive document entries share one write transaction, a *run*.
    /// A DDL entry ends the run — commit, apply the schema change through
    /// [`Self::apply_ddl`], which commits transactions of its own because the
    /// collection, index and vector `_inner` functions do, then start the next
    /// run — so a batch is one commit per run rather than per entry. The
    /// witnessed vector for the whole batch, and `extra` with it, is raised
    /// in the last run's transaction, so nothing is committed separately for
    /// bookkeeping. A run that fails to commit fails the round, as a failed
    /// entry did before; the next round re-delivers it, and re-delivery is
    /// idempotent.
    ///
    /// The per-entry checks are unchanged and still run per entry, in stamp
    /// order: a node's own `UniqueViolation` is refused, a drop tombstone or an
    /// incarnation floor supersedes what predates it, an unknown collection is
    /// counted. They read collection metadata through read transactions,
    /// which see the state *before* the open run — safe, because a run holds
    /// no schema change (a DDL entry ends it) and the one piece of metadata a
    /// document write does touch, the multikey flag, is re-read through the
    /// write transaction by `index::maintain_remote` and `index::mark_multikey`
    /// themselves.
    ///
    /// What waits for the commit: publishing to change streams, and recording
    /// a unique violation, which mints a local entry in a transaction of its
    /// own. Both are done once per run, after it commits, in entry order.
    ///
    /// The per-entry checks resolve their collection through a memo that
    /// lives for the batch: `collection_by_id` scans every database's
    /// collection list in read transactions of its own, and a run holds the
    /// writer while it does, so without the memo a 1,024-entry batch paid
    /// about 1,024 × (2 + databases) read transactions under the writer for
    /// answers that cannot change until the next schema change. The memo is
    /// cleared at every DDL entry, which is the only thing in a batch that can
    /// change them.
    fn apply_batch_absorbing(
        &self,
        entries: &[OplogEntry],
        extra: Option<&VersionVector>,
    ) -> Result<SyncOutcome> {
        let mut outcome = SyncOutcome::default();
        let mut witnessed = extra.cloned().unwrap_or_default();
        let mut run = Run::default();
        let mut memo = Memo::default();

        for entry in entries {
            // **Every** entry the batch takes, on every path — applied,
            // superseded, DDL, or skipped by design — is observed here, once,
            // before `apply_one` branches on it. Doing this per branch is
            // exactly how the hole appeared: three of them forgot, and the
            // node then re-requested those entries on every round forever.
            // Observing before applying is safe: an error from `apply_one`
            // fails the whole batch, and the vector is dropped with it, so no
            // stamp is recorded for an entry that was not applied. See
            // ADR-054.
            witnessed.observe(entry.stamp);
            self.apply_one(entry, &mut run, &mut memo, &mut outcome)?;
        }

        // The witnessed vector rides in the last run's transaction. A batch
        // that ended in a schema change has no open run, so one is opened for
        // the vector alone — that is the one case a batch costs a commit of
        // bookkeeping, and it used to cost it every time.
        if !witnessed.is_empty() {
            let txn = self.run_txn(&mut run)?;
            Engine::absorb_witnessed_in_txn(txn, &witnessed)?;
        }
        self.commit_run(&mut run)?;

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
            ddl_refused = outcome.ddl_refused,
            ddl_declined = outcome.ddl_declined,
            "merged a batch from a peer"
        );
        Ok(outcome)
    }

    /// The run's transaction, opened on first use.
    ///
    /// Opened lazily rather than at the start of the batch so that a batch
    /// which turns out to hold only schema changes, or nothing this node
    /// keeps, does not hold redb's single writer for the duration.
    fn run_txn<'r, 'e>(&'e self, run: &'r mut Run<'e>) -> Result<&'r WriteTxn<'e>> {
        if run.txn.is_none() {
            run.txn = Some(self.begin_write()?);
        }
        Ok(run.txn.as_ref().expect("opened just above"))
    }

    /// Commit the open run, if there is one, then do what had to wait for
    /// the commit: record each applied entry's unique violations and publish
    /// the run to change streams, in entry order.
    ///
    /// A commit that fails propagates and drops the pending work with it —
    /// nothing is published for a run that did not land, and the entries come
    /// again next round. A failure *after* the commit is different: the run
    /// is durable and witnessed, so a re-delivery of its entries is superseded
    /// and silent, and an entry whose report was skipped would never publish
    /// and never mint its `UniqueViolation` — the hole ADR-029 exists to
    /// close. So every pending entry is reported regardless, the run's
    /// publishes go out in full, and the first error is returned afterwards.
    fn commit_run(&self, run: &mut Run<'_>) -> Result<()> {
        let Some(txn) = run.txn.take() else {
            debug_assert!(run.pending.is_empty(), "applied entries without a transaction");
            return Ok(());
        };
        txn.commit()?;
        let mut published = Vec::with_capacity(run.pending.len());
        let mut failed = None;
        for Pending { collection, entry, id, violations } in run.pending.drain(..) {
            match self.report_remote_write(&collection, &entry, &id, &violations) {
                Ok(entries) => published.extend(entries),
                Err(e) => {
                    failed.get_or_insert(e);
                }
            }
        }
        self.publish(published);
        match failed {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// The collection an entry names, as this batch last saw it.
    ///
    /// `None` is memoised too: a batch for a collection this node lacks would
    /// otherwise rescan every database once per entry.
    fn memo_collection(
        &self,
        memo: &mut Memo,
        id: CollectionId,
    ) -> Result<Option<Arc<CollectionMeta>>> {
        if let Some(found) = memo.collections.get(&id) {
            return Ok(found.clone());
        }
        let found = self.collection_by_id(id)?.map(Arc::new);
        memo.collections.insert(id, found.clone());
        Ok(found)
    }

    /// When the collection was dropped, as this batch last saw it.
    fn memo_dropped_at(&self, memo: &mut Memo, id: CollectionId) -> Result<Option<Stamp>> {
        if let Some(found) = memo.dropped.get(&id) {
            return Ok(*found);
        }
        let found = self.collection_dropped_at(id)?;
        memo.dropped.insert(id, found);
        Ok(found)
    }
}

/// Collection metadata as resolved during one batch.
///
/// Valid until the next schema change, which is the only thing in a batch
/// that can create, drop or recreate a collection — a document run cannot —
/// so it is cleared at every DDL entry and nowhere else. An entry's own
/// write may set an index's multikey flag, which the memoised copy will not
/// show; nothing here reads it (the index paths re-read the definition
/// through the write transaction), and `collection_by_id` returned the
/// pre-run copy before the memo existed too.
#[derive(Default)]
struct Memo {
    collections: HashMap<CollectionId, Option<Arc<CollectionMeta>>>,
    dropped: HashMap<CollectionId, Option<Stamp>>,
}

/// A run of consecutive document entries sharing one transaction.
///
/// `txn` is `None` between runs and before the first document entry.
/// `pending` is what each applied entry left for after the commit; it is
/// empty whenever `txn` is.
#[derive(Default)]
struct Run<'e> {
    txn: Option<WriteTxn<'e>>,
    pending: Vec<Pending>,
}

/// An entry applied into the open run, with what its commit owes.
///
/// One copy of the entry per applied entry until the run commits, bounded by
/// the batch size the transport asks for; the collection is shared with the
/// batch's memo rather than copied.
struct Pending {
    collection: Arc<CollectionMeta>,
    entry: OplogEntry,
    id: DocId,
    violations: Vec<UniqueViolation>,
}

/// How one replicated schema change went against this node's state.
pub(crate) enum Ddl<T> {
    /// Applied, or the world was already like this.
    Applied(T),
    /// It named a collection this node no longer has.
    Gone,
    /// The definition cannot be applied to this node's current state.
    Refused(kimmy_core::Error),
}

impl<T> Ddl<T> {
    fn map<U>(self, f: impl FnOnce(T) -> U) -> Ddl<U> {
        match self {
            Ddl::Applied(value) => Ddl::Applied(f(value)),
            Ddl::Gone => Ddl::Gone,
            Ddl::Refused(e) => Ddl::Refused(e),
        }
    }
}

/// What a batch counts a schema change as, once [`Engine::apply_ddl`] has
/// dealt with it.
enum DdlOutcome {
    Applied,
    UnknownCollection,
    Refused,
    /// A drop older than the index standing under its name: recorded as a
    /// tombstone and not applied (ADR-132), counted so a member whose peers
    /// have all dropped an index it keeps is visible (ADR-141).
    Declined,
}

/// Sort a schema change's result into applied, gone, or refused, leaving
/// every other error an error.
///
/// **Gone.** A replicated schema change names its collection by *name*, so
/// replaying one after the collection has been dropped locally raises
/// `CollectionNotFound`. Before this existed that error travelled all the way
/// out of `apply_batch`, which failed the whole round — and since the
/// offending entry stays in the peer's oplog forever, the position never
/// advanced and every later round died on the same entry. One dropped
/// collection permanently stopped replication between two nodes.
///
/// Skipping is correct, not merely convenient: `apply_one` already treats a
/// *document* for a missing collection this way, and a schema change for a
/// collection that is gone is history for the same reason. It cannot lose a
/// change that still matters, because the only ways to reach here are a drop
/// that already happened on this node — which supersedes anything older — or a
/// creation that aged out of the peer's oplog, which the caller already counts
/// and warns about.
///
/// **Refused.** The same wedge, from the other side (ADR-123). An index
/// definition this build cannot apply — a TTL over two fields, a partial
/// filter it cannot parse — raises `InvalidQuery` from the definition checks;
/// a name already taken here by a different definition, because two members
/// created it concurrently, raises `IndexExists`; an enforcement mode this
/// build does not implement raises `Unsupported`. A definition this node's
/// *documents* do not fit used to be the first of these and is now none of
/// them: the backfill files such a document unkeyed and builds (ADR-139). None of these is
/// `CollectionNotFound`, so each failed the round exactly as a dropped
/// collection once did — observed on a three-member cluster running 0.20.0,
/// where a replayed `CreateIndex` re-requested the same window with backoff
/// to 300 s for the life of the process, the `DropIndex` behind it in the
/// window never reached, and the lag gauge read 0 throughout because a failed
/// round reports no lag.
///
/// This class is different from every other error, and that is why it is
/// safe to skip: the refusal is a deterministic function of the definition
/// and this node's state, so re-delivering the entry unchanged can never
/// succeed, and failing the round buys nothing but the wedge. The entry is
/// still witnessed by `apply_batch_absorbing`, so it is not re-served; it is
/// not appended, so this node does not propagate a definition it does not
/// hold; and the skip is counted (`SyncOutcome::ddl_refused`), logged at
/// warning with the reason, and exported, so the divergence is a visible
/// state rather than a silent one. A compound index refused here is usually
/// followed in the same window by the drop that removed it on the origin —
/// the definition was refused *because* the documents that arrived once it
/// was gone are already here — and the drop still lands.
///
/// Deliberately narrow, in both directions. Only `CollectionNotFound` is
/// gone, and only `InvalidQuery`, `IndexExists` and `Unsupported` are
/// refusals: refusals of the *request*, decided by this node's state. A
/// storage error — redb, I/O, a record that will not decode — is a failure of
/// the *node*, may well succeed on retry, and still fails the round, because
/// a round that quietly skips what it cannot understand is how corruption
/// becomes convergence.
pub(crate) fn settle<T>(result: Result<T>) -> Result<Ddl<T>> {
    use kimmy_core::Error as Core;
    match result {
        Ok(value) => Ok(Ddl::Applied(value)),
        Err(crate::StorageError::Core(Core::CollectionNotFound { .. })) => Ok(Ddl::Gone),
        Err(crate::StorageError::Core(
            e @ (Core::InvalidQuery(_) | Core::IndexExists { .. } | Core::Unsupported(_)),
        )) => Ok(Ddl::Refused(e)),
        Err(e) => Err(e),
    }
}

impl Engine {
    /// Process one replicated entry. Witnessing is the caller's job, so that
    /// no branch here can forget it.
    ///
    /// A document entry is written into `run`'s transaction, opening one if
    /// none is open; a schema change first commits the run, because the DDL
    /// path commits transactions of its own and redb has one writer.
    fn apply_one<'e>(
        &'e self,
        entry: &OplogEntry,
        run: &mut Run<'e>,
        memo: &mut Memo,
        outcome: &mut SyncOutcome,
    ) -> Result<()> {
        // A node's own observation of a broken constraint is not a fact
        // about the data; refuse it even if a peer sends one.
        if entry.kind == OpKind::UniqueViolation {
            return Ok(());
        }

        // Schema changes come first in stamp order, so a collection exists
        // by the time documents for it arrive. One ends the current run:
        // the documents before it must be durable before the DDL path opens
        // its own transactions, and the ones after it start a new run.
        if entry.kind.is_ddl() {
            self.commit_run(run)?;
            // Whatever the batch knew about collections may be wrong after
            // this, whether or not the change turns out to apply.
            *memo = Memo::default();
            match self.apply_ddl(entry)? {
                DdlOutcome::Applied => outcome.ddl += 1,
                DdlOutcome::UnknownCollection => outcome.unknown_collection += 1,
                DdlOutcome::Refused => outcome.ddl_refused += 1,
                DdlOutcome::Declined => outcome.ddl_declined += 1,
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
        if let Some(dropped_at) = self.memo_dropped_at(memo, entry.collection)?
            && entry.stamp < dropped_at
        {
            outcome.superseded += 1;
            return Ok(());
        }

        let Some(collection) = self.memo_collection(memo, entry.collection)? else {
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

        let txn = self.run_txn(run)?;
        match self.apply_remote_in_txn(txn, &collection, entry)? {
            RemoteApplied::Applied { id, violations } => {
                outcome.applied += 1;
                run.pending.push(Pending { collection, entry: entry.clone(), id, violations });
            }
            // Nothing was written, so the run's transaction is exactly as it
            // was; the next entry carries on in it.
            RemoteApplied::Superseded => outcome.superseded += 1,
        }
        Ok(())
    }

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
    ///
    /// Commits transactions of its own — the collection, index and vector
    /// `_inner` functions each do — which is why a batch's document run is
    /// committed before one of these is applied (ADR-119).
    ///
    /// Returns what became of the change. `UnknownCollection` means it named
    /// a collection this node no longer has, which is history rather than an
    /// error; `Refused` means this node's current state cannot take the
    /// definition, which is a divergence to count rather than a round to fail
    /// — see [`settle`] for both. Neither appends the originating entry.
    fn apply_ddl(&self, entry: &OplogEntry) -> Result<DdlOutcome> {
        let Some(body) = &entry.body else {
            // A legacy `Collection` entry, which names nothing.
            return Ok(DdlOutcome::Applied);
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
                    return Ok(DdlOutcome::Applied);
                }

                match self.get_collection(&target.db, &target.name) {
                    Ok(_) => {}
                    Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound {
                        ..
                    })) => {
                        self.create_collection_inner(
                            &target.db,
                            &target.name,
                            false,
                            Some(entry.stamp.hlc),
                        )?;
                        debug!(db = %target.db, collection = %target.name, "created a replicated collection");
                    }
                    Err(e) => return Err(e),
                }
            }
            OpKind::DropCollection => {
                let target: kimmy_core::CollectionRef = bson::deserialize_from_slice(body)?;
                // A drop is the one arm that destroys state, so it is the one
                // arm that has to know *which* incarnation it was aimed at. A
                // recreated collection derives the same id as the one that was
                // dropped, and overlapping ranges are re-delivered as a matter
                // of course — so a drop from the previous life arrives again
                // after the recreation, resolves to the current collection,
                // and without this check empties it. Seen on a three-member
                // cluster on 2026-08-28: one member left with 211 of 361
                // documents, every member missing vectors, lag 0 throughout.
                //
                // Stale if it predates the create that produced the current
                // incarnation (origin stamps on both sides), or is at or
                // before the drop that incarnation was created after.
                match self.get_collection(&target.db, &target.name) {
                    Ok(current) => {
                        let predates_create = entry.stamp.hlc < current.created;
                        let at_or_before_floor =
                            current.incarnation_floor.is_some_and(|floor| entry.stamp.hlc <= floor);
                        if predates_create || at_or_before_floor {
                            debug!(
                                db = %target.db,
                                collection = %target.name,
                                "ignored a drop older than the collection's current incarnation"
                            );
                            // The tombstone is still worth remembering (it never
                            // moves backwards), so a straggling pre-drop write
                            // is superseded here as it would be anywhere else.
                            self.record_collection_drop(entry.collection, entry.stamp)?;
                            return Ok(DdlOutcome::Applied);
                        }
                    }
                    Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound {
                        ..
                    })) => {}
                    Err(e) => return Err(e),
                }
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
                match settle(self.apply_remote_index(&target, entry.stamp))? {
                    Ddl::Applied(true) => {}
                    // History: older than the drop that removed the index, or
                    // than the definition this node holds under the same name.
                    // Counted as applied and *not* appended, exactly as a
                    // `CreateCollection` older than its drop is above — this
                    // node does not re-serve onward an entry it has decided
                    // is history. The drop's own entry, which is appended
                    // when applied, is what carries the ordering to a third
                    // member; nothing is lost by withholding the create.
                    Ddl::Applied(false) => return Ok(DdlOutcome::Applied),
                    Ddl::Gone => return Ok(DdlOutcome::UnknownCollection),
                    Ddl::Refused(reason) => {
                        // The operator's signal: this node now lacks an index
                        // its peers hold, and nothing will retry it. Once per
                        // delivery: the entry is witnessed, so it comes round
                        // again only if a window is re-served for some other
                        // reason, and then it is refused and warned again.
                        warn!(
                            db = %target.db,
                            collection = %target.collection,
                            index = %target.index.name,
                            reason = %reason,
                            "skipped a replicated index this node cannot build; the \
                             definition stands on its peers and the divergence is counted \
                             in kimmy_sync_ddl_refused_total"
                        );
                        return Ok(DdlOutcome::Refused);
                    }
                }
            }
            OpKind::DropIndex => {
                let target: kimmy_core::IndexDrop = bson::deserialize_from_slice(body)?;

                // A drop is the one arm that destroys state, so it is the one
                // arm that has to know *which* index it was aimed at — the
                // `DropCollection` arm's incarnation rule, one level down. An
                // index recreated under the same name derives the same id, and
                // overlapping windows are re-served as a matter of course, so
                // a drop from before the recreation arrives again after it and
                // removes an index nobody dropped. Observed on a three-member
                // cluster on 2026-09-03: a collection listed no indexes on any
                // member, though three stood on all three an hour earlier
                // (ADR-132).
                //
                // An index carrying no creation stamp cannot arbitrate, and
                // reads as older than the drop: the drop applies, which is
                // what ADR-123 left and what a caller reading the reference
                // before this expects.
                if let Ok(current) = self.get_collection(&target.db, &target.collection)
                    && let Some(index) = current.index(&target.index)
                    && index.created.is_some_and(|created| entry.stamp < created)
                {
                    // Info, not warn: a re-served window carries a drop past
                    // the recreation it preceded as a matter of course, and
                    // that is the rule doing its job. Counted all the same,
                    // because the other way to reach here is a drop from a
                    // member whose clock trailed the creator's, which leaves
                    // this member holding an index its peers have all
                    // dropped, and nothing else reports that (ADR-141). The
                    // escape hatch is a local drop on this member, which
                    // mints a stamp ahead of the creation.
                    info!(
                        db = %target.db,
                        collection = %target.collection,
                        index = %target.index,
                        drop = ?entry.stamp,
                        created = ?index.created,
                        "declined a drop older than the index it names; counted in \
                         kimmy_sync_ddl_declined_total"
                    );
                    // Still remembered, exactly as the `DropCollection` arm
                    // remembers a superseded drop: a tombstone never moves
                    // backwards, and this one is older than the creation that
                    // beat it, so it cannot touch the index standing here.
                    // It is this node's record that the drop was seen, and a
                    // second line of defence against the earlier index of the
                    // name — the comparison above turns that replay away too,
                    // so removing this would not by itself let the earlier
                    // index back.
                    self.record_index_drop(
                        entry.collection,
                        kimmy_core::IndexMeta::derive_id(&target.index),
                        entry.stamp,
                    )?;
                    return Ok(DdlOutcome::Declined);
                }

                let dropped = self.drop_index_inner(
                    &target.db,
                    &target.collection,
                    &target.index,
                    Some(entry.stamp),
                );
                match settle(dropped)? {
                    Ddl::Applied(_) => {}
                    Ddl::Gone => {
                        // The collection is gone, and with it the index; the
                        // drop is history. The tombstone is still recorded,
                        // for the reason the `DropCollection` arm records
                        // its own on a node that never had the collection: a
                        // replay of the create — after the collection has
                        // itself been recreated, say — must find the index
                        // already history here. The id is derived from the
                        // name, which is all a drop carries.
                        self.record_index_drop(
                            entry.collection,
                            kimmy_core::IndexMeta::derive_id(&target.index),
                            entry.stamp,
                        )?;
                        return Ok(DdlOutcome::UnknownCollection);
                    }
                    Ddl::Refused(reason) => {
                        warn!(
                            db = %target.db,
                            collection = %target.collection,
                            index = %target.index,
                            reason = %reason,
                            "skipped a replicated index drop this node could not apply"
                        );
                        return Ok(DdlOutcome::Refused);
                    }
                }
            }
            OpKind::ConfigureVectors => {
                let target: kimmy_core::VectorSet = bson::deserialize_from_slice(body)?;
                let applied = match target.config {
                    Some(config) => settle(self.configure_vectors_inner(
                        &target.db,
                        &target.collection,
                        config,
                        false,
                    ))?
                    .map(|_| ()),
                    // Never `drop_vectors`: discarding a peer's stored
                    // vectors is not something a configuration change from
                    // elsewhere should decide.
                    None => settle(self.disable_vectors_inner(
                        &target.db,
                        &target.collection,
                        false,
                        false,
                    ))?
                    .map(|_| ()),
                };
                match applied {
                    Ddl::Applied(()) => {}
                    Ddl::Gone => return Ok(DdlOutcome::UnknownCollection),
                    Ddl::Refused(reason) => {
                        warn!(
                            db = %target.db,
                            collection = %target.collection,
                            reason = %reason,
                            "skipped a replicated vector configuration this node cannot apply; \
                             the divergence is counted in kimmy_sync_ddl_refused_total"
                        );
                        return Ok(DdlOutcome::Refused);
                    }
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
        Ok(DdlOutcome::Applied)
    }

    /// Create a replicated index, stamped `stamp` at its origin.
    ///
    /// Idempotent through `create_index_inner`, which returns the existing
    /// definition when it matches. When the name is taken by a *different*
    /// definition — two members created it while they could not see each
    /// other — the later creation stamp wins, which is how two concurrent
    /// writes to one document already settle (ADR-132); the loser is removed
    /// in the transaction that builds the winner. Where either definition
    /// carries no creation stamp there is nothing to compare, and the
    /// arrival is refused with `IndexExists` and counted, as ADR-123 left it.
    ///
    /// A unique index whose backfill finds keys already shared is built in
    /// full and the collisions are recorded after the commit, the way a
    /// merged write's are (`report_remote_write`, ADR-020, ADR-029): count,
    /// warn, mint a `UniqueViolation` entry, publish.
    ///
    /// `Ok(false)` means the creation is history — older than the drop that
    /// removed the index, or older than the definition this node holds under
    /// the same name — and nothing was done; the caller counts it as applied
    /// and does not append it.
    fn apply_remote_index(&self, target: &kimmy_core::IndexCreate, stamp: Stamp) -> Result<bool> {
        let meta = self.get_collection(&target.db, &target.collection)?;

        // A creation older than the drop that removed the index is history,
        // not an instruction — the same rule the `CreateCollection` arm
        // applies against the collection tombstone, for the same reason: a
        // re-served window, or a peer partitioned across the drop, would
        // otherwise rebuild it. And a rebuild is not merely a resurrection
        // here: it backfills over documents written legally once the index
        // was gone, and fails on them (ADR-123).
        let index_id = kimmy_core::IndexMeta::derive_id(&target.index.name);
        if let Some(dropped_at) = self.index_dropped_at(meta.id, index_id)?
            && stamp < dropped_at
        {
            debug!(
                db = %target.db,
                collection = %target.collection,
                index = %target.index.name,
                "ignored an index creation older than the drop that removed it"
            );
            return Ok(false);
        }

        // The entry's stamp is the definition's creation stamp: the origin
        // recorded exactly this one on the index it minted the entry for, and
        // a replicated entry is appended under the stamp it arrived with, so
        // on this route the payload's copy can never differ from it. Read
        // from the payload first all the same, because that is the field the
        // *snapshot* route has to use — a snapshot carries definitions with
        // no entry behind them — and one rule for reading it is better than
        // two. A payload from a build that recorded no stamp falls back to
        // the entry's, which is the same value the origin would have used.
        let created = target.index.created.unwrap_or(stamp);
        let (created, violations) = self.create_index_inner(
            &target.db,
            &target.collection,
            target.index.fields.clone(),
            target.index.unique,
            target.index.enforcement,
            Some(target.index.name.clone()),
            target.index.expire_after_secs,
            target.index.partial_filter.clone(),
            crate::index::CreateOrigin::Replicated(Some(created)),
        )?;
        if !violations.is_empty() {
            self.report_index_backfill_violations(&meta, &violations)?;
        }
        Ok(matches!(created, crate::index::IndexCreated::Built(_)))
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
                let entries = from.entries_for_peer(start, BATCH).unwrap().entries;
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
        round_window(into, from, limit).0
    }

    /// [`round`], also handing back the window the peer served, for tests that
    /// assert on where it ended rather than on what it carried.
    fn round_window(into: &Engine, from: &Engine, limit: usize) -> (SyncOutcome, OplogWindow) {
        let mine = into.witnessed_vector().unwrap();
        let theirs = from.version_vector().unwrap();
        match mine.behind(&theirs) {
            Some(start) => {
                let window = from.entries_for_peer(start, limit).unwrap();
                let outcome = into
                    .apply_peer_batch(&theirs, &window.entries, window.scanned_to, window.exhausted)
                    .unwrap();
                let mine = into.witnessed_vector().unwrap();
                let now = crate::engine::physical_now_ms();
                (SyncOutcome { lag_ms: lag_behind_ms(&mine, &theirs, now), ..outcome }, window)
            }
            None => (SyncOutcome::default(), OplogWindow { exhausted: true, ..Default::default() }),
        }
    }

    /// The highest stamp `into` has witnessed at any origin the peer
    /// advertised — what "the witness has not advanced past the window" is
    /// asserted against.
    ///
    /// Origins the peer never named are excluded deliberately: merging a
    /// colliding write mints the receiver's *own* `UniqueViolation` entry
    /// (ADR-029), stamped now, and that stamp is the receiver's business
    /// rather than anything the peer's window claimed.
    fn witnessed_of(into: &Engine, theirs: &VersionVector) -> Hlc {
        let mine = into.witnessed_vector().unwrap();
        theirs.iter().map(|(node, _)| mine.get(node)).max().unwrap_or(Hlc::ZERO)
    }

    /// A stamp just above everything `engine` holds.
    ///
    /// An injected entry has to sort *after* the collection it belongs to, or
    /// a peer reads it before it has the collection and counts it as an
    /// unknown one instead of applying it — which would make a document go
    /// missing for a reason that has nothing to do with what is under test.
    fn just_above(engine: &Engine) -> Hlc {
        let head =
            engine.version_vector().unwrap().iter().map(|(_, hlc)| hlc).max().unwrap_or(Hlc::ZERO);
        Hlc::new(head.wall_ms + 1, 0)
    }

    #[test]
    fn an_exhausted_window_proves_the_whole_advertised_vector() {
        let mut theirs = VersionVector::new();
        let origin = kimmy_core::NodeId::generate();
        theirs.insert(origin, Hlc::new(5_000, 0));
        assert_eq!(coverage_after_batch(&theirs, Hlc::new(1_000, 0), true), theirs);
        assert_eq!(coverage_after_batch(&theirs, Hlc::ZERO, true), theirs, "an empty tail too");
    }

    #[test]
    fn a_truncated_window_proves_every_origin_up_to_the_stamp_it_reached() {
        // Three advertised origins: one whose coverage ends before the window
        // does, one inside it, one beyond. Only the last is clipped.
        let early = kimmy_core::NodeId::generate();
        let inside = kimmy_core::NodeId::generate();
        let beyond = kimmy_core::NodeId::generate();
        let mut theirs = VersionVector::new();
        theirs.insert(early, Hlc::new(1_000, 0));
        theirs.insert(inside, Hlc::new(2_500, 0));
        theirs.insert(beyond, Hlc::new(9_000, 0));
        let stranger = kimmy_core::NodeId::generate();

        // A window the peer stopped scanning at 3_000 because its batch filled.
        let covered = coverage_after_batch(&theirs, Hlc::new(3_000, 0), false);
        assert_eq!(covered.get(early), Hlc::new(1_000, 0), "never past what the peer holds");
        assert_eq!(covered.get(inside), Hlc::new(2_500, 0));
        assert_eq!(covered.get(beyond), Hlc::new(3_000, 0), "clipped to the window end");
        assert_eq!(covered.get(stranger), Hlc::ZERO, "an unadvertised origin is not claimed");
    }

    #[test]
    fn a_window_short_of_the_tail_claims_only_what_it_scanned() {
        // A window whose end lies far below what the peer advertises. Reading
        // that as "the whole tail" — which counting entries did, for any batch
        // under the limit — is finding 14 in one line, so the rule takes the
        // peer's stated end instead (ADR-127).
        //
        // This is the rule as a pure function, given what it is told. What a
        // *batch* is allowed to tell it is a separate question, decided in
        // `apply_peer_batch` and pinned by
        // `a_peer_that_over_reports_its_window_claims_only_what_it_sent` and
        // `a_window_that_carried_nothing_and_is_not_a_tail_claims_nothing`.
        let origin = kimmy_core::NodeId::generate();
        let mut theirs = VersionVector::new();
        theirs.insert(origin, Hlc::new(9_000, 0));

        let covered = coverage_after_batch(&theirs, Hlc::new(2_000, 0), false);
        assert_eq!(covered.get(origin), Hlc::new(2_000, 0));
        assert_ne!(covered, theirs, "an empty batch must not absorb the peer's vector");
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
        // The order matters for the *shape* of the reproduction, not for the
        // rule any more: the pin needs the unshippable stamp past a window the
        // batch limit truncates. A violation inside the window no longer
        // shortens the batch — the cap is spent after the filter (ADR-126) —
        // and the window's end is the peer's own report rather than a
        // deduction from the count (ADR-127).
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
        let shippable = a.entries_for_peer(floor, usize::MAX).unwrap().entries;
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

    /// A peer holding a withheld `UniqueViolation` near the head of its oplog,
    /// with `docs` ordinary documents behind it: finding 14's preconditions,
    /// at engine scale.
    ///
    /// The oplog it leaves, in stamp order, is the merged colliding write, the
    /// collection, the index, the local document, **the violation**, and then
    /// the documents — so a window of eight or so entries read from the start
    /// contains the violation and is still truncated by the cap.
    fn peer_with_a_violation_in_its_first_window(a: &Engine, docs: usize) -> CollectionMeta {
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "local", "email": "clash@x" }).unwrap();

        // A merged write that collides on the unique index. It is applied like
        // any other — a violation is reported, not refused — and this node
        // mints its own `UniqueViolation` entry for it, which it will never
        // ship to a peer (ADR-029).
        let clash = OplogEntry {
            stamp: kimmy_core::Stamp::new(just_above(a), kimmy_core::NodeId::generate()),
            kind: OpKind::Insert,
            collection: ca.id,
            doc_id: Some(DocId::String("remote".into())),
            body: Some(
                bson::serialize_to_vec(&doc! { "_id": "remote", "email": "clash@x" }).unwrap(),
            ),
        };
        a.apply_remote(&ca, &clash).unwrap();
        assert_eq!(a.unique_violations(), 1, "the collision must have been recorded");

        for i in 0..docs {
            a.insert(&ca, doc! { "_id": format!("d{i}"), "email": format!("{i}@d") }).unwrap();
        }
        ca
    }

    #[test]
    fn a_withheld_violation_does_not_make_a_truncated_window_look_like_a_tail() {
        // Finding 14, at its smallest. The peer holds more than one window of
        // shippable entries and a withheld violation inside the first one.
        //
        // Spending the cap on entries that are then dropped made that first
        // window return `limit - 1`, and a batch shorter than the limit was
        // read as "the peer's whole tail" — so the receiver absorbed the
        // peer's entire vector, witnessing every entry behind the window
        // without applying one of them, and nothing ever re-served them.
        // Nothing errored, so no counter moved and no line was logged.
        const LIMIT: usize = 8;
        const DOCS: usize = 20;

        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = peer_with_a_violation_in_its_first_window(&a, DOCS);
        let theirs = a.version_vector().unwrap();

        // The first window, truncated at the cap.
        let (_, window) = round_window(&b, &a, LIMIT);
        assert_eq!(
            window.entries.len(),
            LIMIT,
            "the cap must be spent on entries that ship, not on one that is withheld"
        );
        assert!(!window.exhausted, "the peer's oplog is nowhere near its end");
        let delivered = window.entries.last().unwrap().stamp.hlc;

        assert!(
            witnessed_of(&b, &theirs) <= delivered,
            "the witness must not advance past the last stamp the window delivered"
        );
        assert!(
            !b.witnessed_vector().unwrap().covers(&theirs),
            "and the peer's tail must still be outstanding"
        );

        // The remainder arrives on the rounds that follow.
        let budget = (DOCS / LIMIT) + 4;
        for _ in 0..budget {
            round(&b, &a, LIMIT);
        }
        assert!(
            b.witnessed_vector().unwrap().covers(&a.version_vector().unwrap()),
            "B never caught up with A in {budget} rounds"
        );
        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(
            b.count(&cb).unwrap() as usize,
            DOCS + 2,
            "every document, the local one and the merged collision included"
        );
        assert_eq!(a.count(&ca).unwrap(), b.count(&cb).unwrap(), "and the members agree");
    }

    #[test]
    fn a_collection_created_inside_a_truncated_window_reaches_every_member() {
        // The end-to-end shape of finding 14 as it was observed: a collection
        // created on a member its peers were more than one batch behind, with
        // a violation already in the window. It existed on its origin holding
        // documents and answered 404 on the other two members forty-five
        // minutes later, which never list it — replication healthy throughout,
        // lag 0, every sync counter unmoved.
        const LIMIT: usize = 8;
        const DOCS: usize = 20;
        const LATE: usize = 3;

        let (a, _da) = engine();
        let (b, _db) = engine();
        peer_with_a_violation_in_its_first_window(&a, DOCS);

        // Created while the peer is behind by more than one window, so the
        // entry lands in the remainder the receiver used to witness away.
        let late = a.create_collection("shop", "late").unwrap();
        for i in 0..LATE {
            a.insert(&late, doc! { "_id": format!("n{i}") }).unwrap();
        }

        let budget = ((DOCS + LATE) / LIMIT) + 6;
        for _ in 0..budget {
            round(&b, &a, LIMIT);
        }

        let on_b = b
            .get_collection("shop", "late")
            .expect("the collection created behind the window must exist on the peer");
        assert_eq!(on_b.id, late.id, "and address the same storage");
        assert_eq!(b.count(&on_b).unwrap() as usize, LATE, "with the documents written into it");
        assert!(
            b.witnessed_vector().unwrap().covers(&a.version_vector().unwrap()),
            "and the round has converged rather than merely got lucky"
        );
    }

    #[test]
    fn a_violation_stamp_does_not_pin_behind_for_ever() {
        // ADR-082's guard, kept honest. The fix above must not be reached by
        // reverting to "absorb only what was delivered": a stamp the peer
        // advertises and can never ship — its own `UniqueViolation` (ADR-029)
        // — would then pin `VersionVector::behind` at that origin's floor, and
        // once a full window of other entries sits after that floor every
        // round re-serves the same already-witnessed window for the life of
        // the cluster. Observed as `applied=0, superseded=1021` every five
        // seconds, with the lag gauge reading the cluster's age.
        //
        // Converging is necessary but not sufficient to show the cure is
        // there, and on its own it does not discriminate: once the scan
        // reaches the violation at the oplog's head, `scanned_to` is the
        // peer's newest stamp, so clipping each origin to it happens to give
        // the same answer as absorbing the peer's vector. The two rules only
        // differ where the peer advertises an origin **above** its own oplog
        // head, which is not a hypothetical — a snapshot grants coverage for
        // entries the node will never hold (ADR-036), and retention collects
        // the log out from under a vector that persists (ADR-097). The last
        // stage below puts the converged round's own window against exactly
        // that vector.
        const LIMIT: usize = 8;
        const DOCS: usize = 3 * LIMIT + 1;

        let (a, _da) = engine();
        let (b, _db) = engine();

        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "local", "email": "clash@x" }).unwrap();
        for i in 0..DOCS {
            a.insert(&ca, doc! { "_id": format!("d{i}"), "email": format!("{i}@d") }).unwrap();
        }

        // A's newest own stamp: minted now, so it sits after every entry above
        // and more than one window of shippable entries lies under it.
        let clash = OplogEntry {
            stamp: kimmy_core::Stamp::new(just_above(&a), kimmy_core::NodeId::generate()),
            kind: OpKind::Insert,
            collection: ca.id,
            doc_id: Some(DocId::String("remote".into())),
            body: Some(
                bson::serialize_to_vec(&doc! { "_id": "remote", "email": "clash@x" }).unwrap(),
            ),
        };
        a.apply_remote(&ca, &clash).unwrap();
        assert_eq!(a.unique_violations(), 1);

        let pinning = a.version_vector().unwrap().get(a.node_id());
        let shippable = a.entries_for_peer(Hlc::ZERO, usize::MAX).unwrap().entries;
        assert!(shippable.len() > LIMIT, "more than one full window lies under the violation");
        assert!(
            shippable.iter().all(|e| e.stamp.hlc < pinning),
            "the stamp that pins is the one A never ships"
        );

        let budget = DOCS / LIMIT + 6;
        let mut rounds = 0;
        let mut last = OplogWindow::default();
        while rounds < budget {
            rounds += 1;
            last = round_window(&b, &a, LIMIT).1;
            if b.witnessed_vector().unwrap().covers(&a.version_vector().unwrap()) {
                break;
            }
        }

        assert_eq!(
            b.witnessed_vector().unwrap().get(a.node_id()),
            pinning,
            "B must absorb the advertised stamp it can never be sent, or it asks for ever"
        );
        assert_eq!(
            round(&b, &a, LIMIT),
            SyncOutcome::default(),
            "a further round transfers nothing"
        );
        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(b.count(&cb).unwrap() as usize, DOCS + 2, "and nothing was lost getting there");

        // The discriminating half. The round that finished the catch-up ran
        // off the end of A's oplog, and *that* is what proves coverage of
        // everything A advertised — not the stamp the scan happened to stop
        // on. Put the same window against a peer advertising an origin above
        // its own log, the shape a snapshot or a retention pass leaves behind:
        // absorbing answers `granted`, and clipping to the window's end
        // answers `w.scanned_to`, which would pin `behind` at that origin's
        // floor for ever — the livelock ADR-082 exists to prevent.
        assert!(last.exhausted, "the round that caught B up must have reached A's tail");
        let mut advertised = a.version_vector().unwrap();
        let granted = kimmy_core::NodeId::generate();
        let beyond = Hlc::new(last.scanned_to.wall_ms + 60_000, 0);
        advertised.insert(granted, beyond);
        assert!(beyond > last.scanned_to, "only above the window's end do the two rules differ");

        let covered = coverage_after_batch(&advertised, last.scanned_to, last.exhausted);
        assert_eq!(
            covered.get(granted),
            beyond,
            "an exhausted window proves every origin the peer advertised, including one \
             whose entries are not in its oplog at all"
        );
        assert_ne!(
            covered.get(granted),
            last.scanned_to,
            "clipping to the window's end instead would leave that origin pinned"
        );
    }

    #[test]
    fn a_peer_that_over_reports_its_window_claims_only_what_it_sent() {
        // ADR-127 moved the window's end from something this node computes to
        // something the peer asserts, and nothing on the wire is checked by
        // being written down. A sender that trimmed a batch in place — the
        // temptation the `Fits::Only` path creates — while reporting the end
        // it had scanned to would hand the receiver finding 14 from one wrong
        // field: the entries it dropped witnessed away, `behind` reporting
        // nothing missing, and no round ever asking again.
        const DOCS: usize = 20;

        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        for i in 0..DOCS {
            a.insert(&ca, doc! { "_id": format!("d{i}") }).unwrap();
        }

        let theirs = a.version_vector().unwrap();
        let whole = a.entries_for_peer(Hlc::ZERO, usize::MAX).unwrap();
        assert!(whole.exhausted);
        let head = whole.scanned_to;

        // Three entries — the collection and two documents — served with the
        // oplog's head as the window's end.
        let trimmed = &whole.entries[..3];
        b.apply_peer_batch(&theirs, trimmed, head, false).unwrap();

        let cb = b.get_collection("shop", "orders").expect("the creation was in the three");
        assert_eq!(b.count(&cb).unwrap(), 2, "only two documents were actually sent");
        assert!(
            !b.witnessed_vector().unwrap().covers(&theirs),
            "so the peer's tail must still be outstanding, whatever the peer claimed"
        );
        assert_eq!(
            witnessed_of(&b, &theirs),
            trimmed.last().unwrap().stamp.hlc,
            "the window is worth exactly what it carried"
        );

        // And the rest is still served, which is the point of refusing the claim.
        let budget = DOCS + 2;
        for _ in 0..budget {
            round(&b, &a, 8);
        }
        assert_eq!(b.count(&cb).unwrap() as usize, DOCS, "every document arrives on a later round");
    }

    #[test]
    fn a_window_that_carried_nothing_and_is_not_a_tail_claims_nothing() {
        // The worse half of the same hole, and the one the exemption used to
        // let through: a peer that hands over *no* entries while naming a
        // window end absorbs the receiver's whole view of it in exchange for
        // nothing, and no later round asks again.
        //
        // A correct sender cannot produce this. `read_oplog_from_where` stops
        // only after keeping an entry, so a window that is not exhausted holds
        // exactly `limit` entries — never zero. So the state is always a
        // broken or hostile peer, which is the argument for clamping it rather
        // than for trusting it.
        const DOCS: usize = 20;

        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        for i in 0..DOCS {
            a.insert(&ca, doc! { "_id": format!("d{i}") }).unwrap();
        }

        let theirs = a.version_vector().unwrap();
        let whole = a.entries_for_peer(Hlc::ZERO, usize::MAX).unwrap();
        assert!(whole.exhausted);

        let outcome = b.apply_peer_batch(&theirs, &[], whole.scanned_to, false).unwrap();

        assert_eq!(outcome, SyncOutcome::default(), "nothing was applied, because nothing came");
        assert_eq!(witnessed_of(&b, &theirs), Hlc::ZERO, "and nothing may be claimed for it");
        assert!(
            !b.witnessed_vector().unwrap().covers(&theirs),
            "the peer's whole vector must not be absorbed in exchange for an empty batch"
        );
        assert!(b.get_collection("shop", "orders").is_err(), "nothing arrived to create it");

        // Still fully servable afterwards, which is the property being bought.
        let budget = DOCS + 2;
        for _ in 0..budget {
            round(&b, &a, 8);
        }
        let cb = b.get_collection("shop", "orders").expect("a later round creates it");
        assert_eq!(b.count(&cb).unwrap() as usize, DOCS, "and every document arrives");
    }

    mod props {
        use proptest::prelude::*;

        use super::*;

        proptest! {
            // Each case builds two real engines and runs a round per window,
            // so the count is modest; the deterministic tests above carry the
            // named shapes.
            #![proptest_config(ProptestConfig::with_cases(16))]

            /// However a peer's oplog mixes shippable entries with withheld
            /// ones, and whatever the batch limit, a window that did **not**
            /// exhaust the peer's oplog must never leave the receiver
            /// witnessing a stamp past the last one it was actually handed.
            ///
            /// That is the invariant finding 14 broke, stated without
            /// reference to how many entries happened to arrive — which is
            /// the whole point of ADR-127.
            #[test]
            fn a_window_that_is_not_a_tail_never_witnesses_past_what_it_delivered(
                collides in prop::collection::vec(any::<bool>(), 1..24),
                limit in 2usize..9,
            ) {
                let (a, _da) = engine();
                let (b, _db) = engine();
                a.create_collection("shop", "orders").unwrap();
                a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
                let ca = a.get_collection("shop", "orders").unwrap();
                a.insert(&ca, doc! { "_id": "seed", "email": "clash@x" }).unwrap();

                // `true` merges a colliding write, which appends the write and
                // then this node's withheld violation entry after it; `false`
                // is an ordinary local insert. So the withheld entries land
                // wherever the generator put them.
                let origin = kimmy_core::NodeId::generate();
                for (i, collides) in collides.iter().enumerate() {
                    if *collides {
                        let id = format!("r{i}");
                        let entry = OplogEntry {
                            stamp: kimmy_core::Stamp::new(just_above(&a), origin),
                            kind: OpKind::Insert,
                            collection: ca.id,
                            doc_id: Some(DocId::String(id.clone())),
                            body: Some(
                                bson::serialize_to_vec(
                                    &doc! { "_id": id, "email": "clash@x" },
                                )
                                .unwrap(),
                            ),
                        };
                        a.apply_batch(&[entry]).unwrap();
                    } else {
                        a.insert(&ca, doc! { "_id": format!("l{i}"), "email": format!("{i}@l") })
                            .unwrap();
                    }
                }

                // A is quiet from here, so the vector it advertises is fixed
                // for every round below.
                let theirs = a.version_vector().unwrap();
                let budget = collides.len() * 2 + 8;
                let mut rounds = 0;
                while b.witnessed_vector().unwrap().behind(&theirs).is_some() {
                    rounds += 1;
                    prop_assert!(rounds <= budget, "B never converged in {budget} rounds");

                    let (_, window) = round_window(&b, &a, limit);
                    if window.exhausted {
                        continue;
                    }
                    let delivered = window
                        .entries
                        .last()
                        .expect("a window that stopped at the cap shipped entries")
                        .stamp
                        .hlc;
                    prop_assert!(
                        witnessed_of(&b, &theirs) <= delivered,
                        "witnessed past the window: {:?} > {:?}",
                        witnessed_of(&b, &theirs),
                        delivered,
                    );
                }

                // And the point of the invariant: nothing was witnessed away.
                let cb = b.get_collection("shop", "orders").unwrap();
                prop_assert_eq!(b.count(&cb).unwrap(), a.count(&ca).unwrap());
            }
        }
    }

    #[test]
    fn a_fresh_engine_has_an_empty_vector() {
        let (engine, _dir) = engine();
        assert!(engine.version_vector().unwrap().is_empty());
    }

    #[test]
    fn lag_is_how_long_ago_the_newest_applied_entry_was_written() {
        use kimmy_core::{NodeId, Stamp};

        let origin = NodeId::generate();
        let mut mine = VersionVector::new();
        mine.observe(Stamp::new(Hlc::new(10_000, 0), origin));
        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(Hlc::new(17_500, 0), origin));

        // The peer holds newer, and the newest we have is 12 s old.
        assert_eq!(lag_behind_ms(&mine, &theirs, 22_000), 12_000, "12 s behind the clock");
        // It keeps growing while nothing arrives: the backlog is the same
        // width, the node is further behind.
        assert_eq!(lag_behind_ms(&mine, &theirs, 40_000), 30_000);
        assert_eq!(lag_behind_ms(&theirs, &mine, 40_000), 0, "being ahead is not lag");
        assert_eq!(lag_behind_ms(&mine, &mine, 40_000), 0, "caught up is zero");
    }

    #[test]
    fn a_bulk_backlog_reads_the_time_since_it_started_not_the_width_of_its_stamps() {
        // The finding ADR-122 fixes: a bulk insert mints its stamps within a
        // few hundred milliseconds, so the span of history a replica lacks
        // is under a second however many minutes it takes to drain. Held at
        // the bulk's first stamp, the peer at its last, thirty seconds on.
        use kimmy_core::{NodeId, Stamp};

        let origin = NodeId::generate();
        let first = Hlc::new(1_000_000, 0);
        let last = Hlc::new(1_000_300, 0);
        let mut mine = VersionVector::new();
        mine.observe(Stamp::new(first, origin));
        let mut theirs = VersionVector::new();
        theirs.observe(Stamp::new(last, origin));
        let now = first.wall_ms + 30_000;

        let span = last.wall_ms - first.wall_ms;
        assert_eq!(span, 300, "the old formula read the width of the stamps: 0 s on the gauge");
        assert_eq!(lag_behind_ms(&mine, &theirs, now), 30_000, "the node is 30 s behind");
    }

    #[test]
    fn lag_takes_the_worst_origin_not_the_sum() {
        use kimmy_core::{NodeId, Stamp};

        let (a, b) = (NodeId::generate(), NodeId::generate());
        let mut mine = VersionVector::new();
        mine.observe(Stamp::new(Hlc::new(1_000, 0), a));
        mine.observe(Stamp::new(Hlc::new(8_000, 0), b));
        let mut theirs = mine.clone();
        theirs.observe(Stamp::new(Hlc::new(2_000, 0), a));
        theirs.observe(Stamp::new(Hlc::new(9_000, 0), b));

        // An alert cares how far behind the worst origin is; summing origins
        // would report a cluster-wide write burst as one enormous lag. At
        // 10 s, `a` was last applied 9 s ago and `b` 2 s ago.
        assert_eq!(lag_behind_ms(&mine, &theirs, 10_000), 9_000);

        // An origin the peer is level on does not count, whatever its age:
        // catch up on `a` and only `b` is left.
        let mut level = mine.clone();
        level.observe(Stamp::new(Hlc::new(2_000, 0), a));
        assert_eq!(lag_behind_ms(&level, &theirs, 10_000), 2_000);
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

        let losing = a.entries_for_peer(Hlc::ZERO, 100).unwrap().entries;
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
            lag_behind_ms(
                &witnessed,
                &a.version_vector().unwrap(),
                crate::engine::physical_now_ms()
            ),
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

        let ddl = a.entries_for_peer(Hlc::ZERO, 100).unwrap().entries;
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
        // `now − zero` would be the age of the epoch, a fifty-year lie a
        // joining node would alert on. Its lag becomes real with the first
        // applied batch.
        assert_eq!(lag_behind_ms(&VersionVector::new(), &theirs, 1_786_000_005_000), 0);
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
        // Long after the writes: caught up is zero however much time passes.
        let later = crate::engine::physical_now_ms() + DAY * 1_000;
        assert_eq!(lag_behind_ms(&vb, &va, later), 0, "a caught-up pair must read zero");
        assert_eq!(lag_behind_ms(&va, &vb, later), 0);
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

        let outgoing = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;
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
    fn a_replicated_batch_is_one_commit_however_many_entries_it_holds() {
        // The replica's half of
        // `a_batch_is_one_commit_however_many_documents_it_holds` (ADR-119). A
        // bulk insert is one commit where it was accepted; before this it was
        // one commit *per document* on every node that replicated it, each an
        // fsync under `durable` — measured on a three-member cluster at 8–13
        // documents a second.
        const N: usize = 200;
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        let batch: Vec<_> = (0..N as i64).map(|n| doc! { "_id": n, "n": n }).collect();
        a.insert_many(&ca, batch).unwrap();

        // Only the documents: the creation entry is a schema change, which
        // ends a run by design and is measured separately below.
        let theirs = a.version_vector().unwrap();
        let entries: Vec<_> = a
            .entries_for_peer(Hlc::ZERO, BATCH)
            .unwrap()
            .entries
            .into_iter()
            .filter(|e| e.kind.is_document())
            .collect();
        assert_eq!(entries.len(), N);

        let (commits, fsyncs) = (b.commits(), b.fsyncs());
        let end = entries.last().unwrap().stamp.hlc;
        let outcome = b.apply_peer_batch(&theirs, &entries, end, true).unwrap();
        assert_eq!(outcome.applied, N);
        assert_eq!(
            b.commits() - commits,
            1,
            "{N} replicated documents, the witnessed vector and the coverage vector \
             must be one commit"
        );
        assert_eq!(b.fsyncs() - fsyncs, 1, "and so one fsync");
        assert_eq!(b.count(&cb).unwrap() as usize, N, "every document landed");
        assert!(
            b.witnessed_vector().unwrap().covers(&theirs),
            "and the round's coverage was recorded in that same commit"
        );
    }

    #[test]
    fn a_schema_change_mid_batch_splits_it_into_runs_that_commit_once_each() {
        // A DDL entry commits transactions of its own, so it cannot share the
        // documents' transaction: the documents before it are one run, the
        // ones after it another, and both sides of it must land.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let orders_a = a.create_collection("shop", "orders").unwrap();
        b.create_collection("shop", "orders").unwrap();
        for n in 0..3_i64 {
            a.insert(&orders_a, doc! { "_id": n }).unwrap();
        }
        let items_a = a.create_collection("shop", "items").unwrap();
        for n in 0..3_i64 {
            a.insert(&items_a, doc! { "_id": n }).unwrap();
        }
        // Stamp order: three documents, the creation of `items`, three more.
        let entries: Vec<_> = a
            .entries_for_peer(Hlc::ZERO, BATCH)
            .unwrap()
            .entries
            .into_iter()
            .filter(|e| e.kind.is_document() || e.collection == items_a.id)
            .collect();
        assert_eq!(entries.len(), 7);
        assert!(entries[3].kind.is_ddl(), "the schema change sits between the two runs");

        // What the schema change costs on its own, on a third engine: its
        // transactions plus one for the witnessed vector, which a batch with
        // no document run has to commit by itself.
        let (c, _dc) = engine();
        let before = c.commits();
        c.apply_batch(&entries[3..4]).unwrap();
        let ddl_alone = c.commits() - before;

        let before = b.commits();
        let outcome = b.apply_batch(&entries).unwrap();
        assert_eq!((outcome.applied, outcome.ddl), (6, 1), "{outcome:?}");
        // Two runs, one commit each; the schema change's own commits are the
        // same as on `c`, and its witnessed-vector commit there stands in for
        // the second run here, which carries the vector.
        assert_eq!(
            b.commits() - before,
            ddl_alone + 1,
            "one commit per run of documents, not per document"
        );

        let orders_b = b.get_collection("shop", "orders").unwrap();
        let items_b = b.get_collection("shop", "items").expect("created mid-batch");
        assert_eq!(b.count(&orders_b).unwrap(), 3, "the run before the schema change landed");
        assert_eq!(b.count(&items_b).unwrap(), 3, "and the run after it");
        assert!(b.witnessed_vector().unwrap().covers(&a.version_vector().unwrap()));
    }

    #[test]
    fn a_superseded_entry_mid_batch_does_not_stop_the_rest_of_the_run() {
        // A losing entry used to abort its own transaction; in a shared one it
        // must simply write nothing, and the entries after it in the same
        // transaction must still land. And it is still witnessed (ADR-054).
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        for n in 1..=3_i64 {
            a.insert(&ca, doc! { "_id": n, "who": "a" }).unwrap();
        }
        // B writes _id 2 later, so A's version of it loses.
        b.insert(&cb, doc! { "_id": 2, "who": "b" }).unwrap();

        let entries: Vec<_> = a
            .entries_for_peer(Hlc::ZERO, BATCH)
            .unwrap()
            .entries
            .into_iter()
            .filter(|e| e.kind.is_document())
            .collect();
        let before = b.commits();
        let outcome = b.apply_batch(&entries).unwrap();
        assert_eq!((outcome.applied, outcome.superseded), (2, 1), "{outcome:?}");
        assert_eq!(b.commits() - before, 1, "still one commit for the run");

        for (id, who) in [(1, "a"), (2, "b"), (3, "a")] {
            let got = b.get(&cb, &DocId::Int64(id)).unwrap().unwrap();
            assert_eq!(got.get_str("who").unwrap(), who, "_id {id}");
        }
        assert!(
            b.witnessed_vector().unwrap().covers(&a.version_vector().unwrap()),
            "the discarded entry is covered, so it is never asked for again"
        );
    }

    #[test]
    fn a_unique_violation_mid_batch_is_still_recorded_and_the_batch_still_commits() {
        // Recording a violation mints a local entry in its own transaction,
        // which cannot happen while the run's transaction is open. It waits
        // for the commit and must still happen — a converged write with an
        // unreported violation is the failure ADR-029 exists to prevent.
        let (b, _db) = engine();
        b.create_collection("shop", "orders").unwrap();
        b.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        let cb = b.get_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "local", "email": "clash@x" }).unwrap();

        let origin = kimmy_core::NodeId::generate();
        let remote = |ms: u64, id: &str, email: &str| OplogEntry {
            stamp: kimmy_core::Stamp::new(Hlc::new(ms, 0), origin),
            kind: OpKind::Insert,
            collection: cb.id,
            doc_id: Some(DocId::String(id.into())),
            body: Some(bson::serialize_to_vec(&doc! { "_id": id, "email": email }).unwrap()),
        };
        let entries = vec![
            remote(9_000, "before", "a@x"),
            remote(9_001, "remote", "clash@x"),
            remote(9_002, "after", "c@x"),
        ];

        let before = b.commits();
        let outcome = b.apply_batch(&entries).unwrap();
        assert_eq!(outcome.applied, 3, "the colliding write is merged, not refused: {outcome:?}");
        assert_eq!(b.commits() - before, 2, "the run, then the violation's own entry after it");
        assert_eq!(b.unique_violations(), 1);
        assert_eq!(b.count(&cb).unwrap(), 4, "every document is present, the collision included");
        let recorded = b
            .read_oplog_from(Hlc::ZERO, BATCH)
            .unwrap()
            .into_iter()
            .filter(|e| e.kind == OpKind::UniqueViolation)
            .count();
        assert_eq!(recorded, 1, "a `UniqueViolation` entry exists for change streams");
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

    /// An entry's kind, for picking a subset of a peer's oplog by hand.
    fn only(entries: &[OplogEntry], kinds: &[OpKind]) -> Vec<OplogEntry> {
        entries.iter().filter(|e| kinds.contains(&e.kind)).cloned().collect()
    }

    /// A compound index over two paths that may each hold an array.
    fn two_array_index(engine: &Engine) {
        engine
            .create_index("shop", "orders", vec![field("tags"), field("cats")], false, None)
            .expect("accepted: no document holds arrays at both paths yet");
    }

    #[test]
    fn a_replayed_index_over_a_document_this_node_cannot_key_is_built_and_the_entries_behind_it_arrive()
     {
        // The finding ADR-123 fixed, replayed under ADR-139. A created a
        // compound index over two array fields on a collection with no
        // document holding both, dropped it, and then took a two-array
        // document, which is legal once the index is gone. B, meanwhile,
        // holds such a document of its own. Replaying A's history on B, the
        // create's backfill meets B's document — and files it unkeyed rather
        // than refusing the definition. Nothing is counted, the drop behind
        // it lands, and so does everything A wrote afterwards.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        two_array_index(&a);
        a.drop_index("shop", "orders", "tags_1_cats_1").unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "both-a", "tags": ["x", "y"], "cats": ["p", "q"] }).unwrap();
        a.insert(&ca, doc! { "_id": "after" }).unwrap();

        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "both-b", "tags": ["x"], "cats": ["p"] }).unwrap();

        let outcome = round(&b, &a, BATCH);
        assert_eq!(outcome.ddl_refused, 0, "nothing to refuse: the index builds: {outcome:?}");
        assert_eq!(outcome.ddl, 3, "the create, the drop, and the collection: {outcome:?}");
        assert!(
            b.get_collection("shop", "orders").unwrap().index("tags_1_cats_1").is_none(),
            "built and then dropped, as on A"
        );
        for id in ["both-a", "after"] {
            assert!(
                b.get(&cb, &DocId::String(id.into())).unwrap().is_some(),
                "{id} must arrive behind the schema changes"
            );
        }
        assert_eq!(round(&b, &a, BATCH), SyncOutcome::default(), "and the window is not re-served");

        // The peers converge on the documents, both ways.
        sync(&a, &b);
        assert_eq!(a.count(&ca).unwrap(), 3);
        assert_eq!(b.count(&cb).unwrap(), 3);
    }

    /// Whether `engine` holds `name` on `shop.orders`, and how many of its
    /// documents that index could not key.
    fn index_state(engine: &Engine, name: &str) -> Option<u64> {
        let coll = engine.get_collection("shop", "orders").unwrap();
        let index = coll.index(name)?;
        Some(engine.unkeyed_count(&coll, index.id).unwrap())
    }

    #[test]
    fn a_document_arriving_after_an_index_that_cannot_key_it_is_stored_unkeyed() {
        // The wedge, at its smallest. A holds a compound index over two
        // paths; B, which has not heard of it, legally accepts a document
        // holding arrays at both; the document replicates to A. Before
        // ADR-139 A could neither apply the entry nor skip it, and every
        // round failed for the life of the process. Now A stores the
        // document, files it unkeyed under the index it holds, and the
        // round is a round like any other.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        two_array_index(&a);
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "fine", "tags": ["x"], "cats": "p" }).unwrap();

        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "both", "tags": ["x", "y"], "cats": ["p", "q"] }).unwrap();
        b.insert(&cb, doc! { "_id": "later" }).unwrap();

        // The documents alone, applied as a transport round would apply
        // them: the collection's creation entry is a schema change, which
        // ends a run by design, and what is measured here is the run
        // (ADR-119). A holds the collection already.
        let theirs = b.version_vector().unwrap();
        let entries: Vec<_> = b
            .entries_for_peer(Hlc::ZERO, BATCH)
            .unwrap()
            .entries
            .into_iter()
            .filter(|e| e.kind.is_document())
            .collect();
        let end = entries.last().unwrap().stamp.hlc;
        let commits = a.commits();
        let outcome = a.apply_peer_batch(&theirs, &entries, end, true).unwrap();
        assert_eq!(outcome.applied, 2, "both of B's documents apply: {outcome:?}");
        assert_eq!(outcome.ddl_refused, 0, "{outcome:?}");
        assert_eq!(a.commits() - commits, 1, "one run, one commit, unkeyed document included");
        assert!(a.get(&ca, &DocId::String("both".into())).unwrap().is_some(), "stored on A");
        assert_eq!(index_state(&a, "tags_1_cats_1"), Some(1), "held, with one unkeyed document");
        assert_eq!(a.unkeyed_writes(), 1);
        assert_eq!(round(&a, &b, BATCH), SyncOutcome::default(), "witnessed, not re-served");

        // And A's own planner finds the document through that index, as a
        // scan would, so the two answer alike.
        let coll = a.get_collection("shop", "orders").unwrap();
        let filter = kimmy_query::filter::parse(&doc! { "tags": "x", "cats": "q" }).unwrap();
        let plan = kimmy_query::plan::choose(&filter, &coll.indexes).expect("the index applies");
        let mut found = Vec::new();
        for (lower, upper) in &plan.ranges {
            for key in a.index_candidates(&coll, plan.index_id, lower, upper).unwrap() {
                if let Some(d) = a.get_by_encoded_key(&coll, &key).unwrap()
                    && kimmy_query::filter::matches(&filter, &d)
                {
                    found.push(d.get_str("_id").unwrap().to_string());
                }
            }
        }
        assert_eq!(found, vec!["both"], "found through the index, by the recheck");
    }

    #[test]
    fn both_arrival_orders_converge_to_the_same_state() {
        // The pair can meet in two orders — definition first on the member
        // that built it, document first on the member that took the write
        // — and ADR-123 handled only one of them. Under ADR-139 both orders
        // land in one state: the index on every member, the document on
        // every member, filed unkeyed under it everywhere, nothing counted
        // as refused, and nothing left to re-serve.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        two_array_index(&a);
        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "both", "tags": ["x", "y"], "cats": ["p", "q"] }).unwrap();

        // A takes the document (definition first, document second) ...
        let to_a = round(&a, &b, BATCH);
        // ... and B takes the definition (document first, definition second).
        let to_b = round(&b, &a, BATCH);
        assert_eq!(to_a.ddl_refused + to_b.ddl_refused, 0, "{to_a:?} / {to_b:?}");

        for (name, engine) in [("a", &a), ("b", &b)] {
            let coll = engine.get_collection("shop", "orders").unwrap();
            assert!(
                engine.get(&coll, &DocId::String("both".into())).unwrap().is_some(),
                "{name} holds the document"
            );
            assert_eq!(
                index_state(engine, "tags_1_cats_1"),
                Some(1),
                "{name} holds the index, with the document filed unkeyed under it"
            );
        }
        assert_eq!(round(&a, &b, BATCH), SyncOutcome::default(), "converged, a's side");
        assert_eq!(round(&b, &a, BATCH), SyncOutcome::default(), "converged, b's side");
    }

    #[test]
    fn an_index_arriving_after_a_document_it_cannot_key_is_built_over_it() {
        // The order ADR-123 answered by refusing the definition. It is now
        // built, over the document it cannot key, so the member ends with
        // what its peers have rather than without an index they hold.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        two_array_index(&a);
        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "both", "tags": ["x", "y"], "cats": ["p", "q"] }).unwrap();

        let outcome = round(&b, &a, BATCH);
        assert_eq!(outcome.ddl_refused, 0, "{outcome:?}");
        assert_eq!(index_state(&b, "tags_1_cats_1"), Some(1));
        assert_eq!(b.unkeyed_writes(), 1, "the backfill counted the document it filed");
    }

    #[test]
    fn a_batch_holding_documents_an_index_cannot_key_applies_the_rest_of_the_run() {
        // ADR-119's property, under ADR-139: a run is one transaction
        // however many of its entries the index could not key, and every
        // entry in it lands.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        two_array_index(&a);
        let cb = b.create_collection("shop", "orders").unwrap();
        for i in 0..20i64 {
            let doc = if i % 3 == 0 {
                doc! { "_id": i, "tags": ["x", "y"], "cats": ["p"] }
            } else {
                doc! { "_id": i, "tags": ["x"], "cats": "p" }
            };
            b.insert(&cb, doc).unwrap();
        }

        // The documents alone, as in `a_document_arriving_after_an_index…`
        // above: the run is what is measured, and A holds the collection.
        let theirs = b.version_vector().unwrap();
        let entries: Vec<_> = b
            .entries_for_peer(Hlc::ZERO, BATCH)
            .unwrap()
            .entries
            .into_iter()
            .filter(|e| e.kind.is_document())
            .collect();
        let end = entries.last().unwrap().stamp.hlc;
        let commits = a.commits();
        let outcome = a.apply_peer_batch(&theirs, &entries, end, true).unwrap();
        assert_eq!(outcome.applied, 20, "{outcome:?}");
        assert_eq!(a.commits() - commits, 1, "one run, one commit");
        let ca = a.get_collection("shop", "orders").unwrap();
        assert_eq!(a.count(&ca).unwrap(), 20);
        assert_eq!(index_state(&a, "tags_1_cats_1"), Some(7), "0, 3, 6, …, 18");
    }

    #[test]
    fn a_dropped_index_never_comes_back_through_a_replayed_create() {
        // The first order: B took the create and the drop in one batch, and
        // then the two-array document written once the index was gone. Peers
        // re-serve overlapping windows as a matter of course, so the same
        // batch arrives again — and without a tombstone the create would
        // rebuild the index over a document it cannot be built for.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        two_array_index(&a);
        a.drop_index("shop", "orders", "tags_1_cats_1").unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "both", "tags": ["x", "y"], "cats": ["p", "q"] }).unwrap();

        let history = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;
        let first = b.apply_batch(&history).unwrap();
        assert_eq!(first.ddl_refused, 0, "nothing to refuse the first time: {first:?}");
        let cb = b.get_collection("shop", "orders").unwrap();
        assert!(cb.index("tags_1_cats_1").is_none());
        let index_id = kimmy_core::IndexMeta::derive_id("tags_1_cats_1");
        assert!(
            b.index_dropped_at(cb.id, index_id).unwrap().is_some(),
            "the drop left a tombstone"
        );

        let again = b.apply_batch(&history).unwrap();
        assert_eq!(again.ddl_refused, 0, "a create older than the drop is history, not a refusal");
        assert!(
            b.get_collection("shop", "orders").unwrap().index("tags_1_cats_1").is_none(),
            "a dropped index must not come back through a replayed create"
        );
    }

    #[test]
    fn a_peer_that_receives_the_drop_before_the_create_never_builds_the_index() {
        // The second order: the drop arrives first, on a node that never held
        // the index. The tombstone has to be recorded even though there was
        // nothing to remove, or the create — arriving later from another
        // peer, or as an aged-out window re-served — builds it.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        two_array_index(&a);
        a.drop_index("shop", "orders", "tags_1_cats_1").unwrap();
        let history = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;

        b.apply_batch(&only(&history, &[OpKind::CreateCollection, OpKind::DropIndex])).unwrap();
        let cb = b.get_collection("shop", "orders").unwrap();
        let index_id = kimmy_core::IndexMeta::derive_id("tags_1_cats_1");
        assert!(
            b.index_dropped_at(cb.id, index_id).unwrap().is_some(),
            "a drop for an index this node never held must still leave its tombstone"
        );

        b.apply_batch(&only(&history, &[OpKind::CreateIndex])).unwrap();
        assert!(
            b.get_collection("shop", "orders").unwrap().index("tags_1_cats_1").is_none(),
            "the create is older than the drop and must not build the index"
        );
    }

    #[test]
    fn a_drop_for_a_collection_this_node_no_longer_has_still_leaves_an_index_tombstone() {
        // The `DropCollection` arm's reasoning applied to indexes: a node
        // that dropped the collection cannot drop the index, but the
        // tombstone is what a later replay of the create checks against.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        b.create_collection("shop", "orders").unwrap();
        two_array_index(&a);
        a.drop_index("shop", "orders", "tags_1_cats_1").unwrap();
        b.drop_collection("shop", "orders").unwrap();

        let outcome = pull(&b, &a);
        assert!(outcome.unknown_collection > 0, "{outcome:?}");
        let id = kimmy_core::CollectionId::derive("shop", "orders");
        let index_id = kimmy_core::IndexMeta::derive_id("tags_1_cats_1");
        assert!(b.index_dropped_at(id, index_id).unwrap().is_some());
    }

    #[test]
    fn a_local_drop_of_an_index_this_member_does_not_hold_mints_the_drop_and_the_tombstone() {
        // ADR-141: a drop is an instruction to the cluster, not a report on
        // this member. Nothing here to remove, and still an entry to
        // replicate and a tombstone to remember it by.
        let (a, _da) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let dropped = a.drop_index_stamped("shop", "orders", "ghost").unwrap();
        assert!(!dropped.removed, "this member held nothing");
        let stamp = dropped.stamp.expect("the drop was recorded all the same");
        let index_id = kimmy_core::IndexMeta::derive_id("ghost");
        assert_eq!(a.index_dropped_at(ca.id, index_id).unwrap(), Some(stamp), "tombstone");
        let history = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;
        let drop = history.iter().find(|e| e.kind == OpKind::DropIndex).expect("the entry");
        assert_eq!(drop.stamp, stamp, "under the stamp the tombstone records");
        assert!(!a.drop_index("shop", "orders", "ghost").unwrap(), "and `drop_index` says so");
        assert_eq!(
            a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries.len(),
            history.len() + 1,
            "a second drop of the same absent name is a second instruction"
        );
    }

    #[test]
    fn a_drop_issued_on_a_member_without_the_index_removes_it_on_the_holder() {
        // The finding, between two engines. A holds the index; B, which has
        // never heard of it, is asked to drop it — the request a front routes
        // to whichever member answers. B's drop replicates to A and removes
        // the index there; A's create, arriving at B afterwards, is older
        // than B's tombstone and reads as history. Both end without it, and
        // nothing is refused.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
            .unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        // Strictly later wall time than A's create: the two engines share a
        // clock but not a counter, and the drop must sort after the creation
        // it removes (ADR-132).
        std::thread::sleep(std::time::Duration::from_millis(2));
        let dropped = b.drop_index_stamped("shop", "orders", "by_email").unwrap();
        assert!(!dropped.removed);

        let to_a = round(&a, &b, BATCH);
        assert_eq!(to_a.ddl_refused + to_a.ddl_declined, 0, "{to_a:?}");
        assert!(
            a.get_collection("shop", "orders").unwrap().index("by_email").is_none(),
            "the holder dropped it on B's instruction"
        );
        let to_b = round(&b, &a, BATCH);
        assert_eq!(to_b.ddl_refused + to_b.ddl_declined, 0, "{to_b:?}");
        assert!(
            b.get_collection("shop", "orders").unwrap().index("by_email").is_none(),
            "A's create is older than B's tombstone: history, not a rebuild"
        );
        assert_eq!(round(&a, &b, BATCH), SyncOutcome::default(), "converged, a's side");
        assert_eq!(round(&b, &a, BATCH), SyncOutcome::default(), "converged, b's side");
        let _ = cb;
    }

    #[test]
    fn a_recreation_after_a_drop_from_a_non_holder_stands_everywhere() {
        // The tombstone the non-holder recorded must not make the name
        // unusable: a creation stamped after it is a new index, and wins on
        // both members.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
            .unwrap();
        b.create_collection("shop", "orders").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        b.drop_index("shop", "orders", "by_email").unwrap();
        sync(&a, &b);
        sync(&a, &b);
        assert!(a.get_collection("shop", "orders").unwrap().index("by_email").is_none());
        assert!(b.get_collection("shop", "orders").unwrap().index("by_email").is_none());

        std::thread::sleep(std::time::Duration::from_millis(2));
        a.create_index("shop", "orders", vec![field("email")], true, Some("by_email".into()))
            .unwrap();
        let outcome = round(&b, &a, BATCH);
        assert_eq!(outcome.ddl_refused + outcome.ddl_declined, 0, "{outcome:?}");
        assert!(
            b.get_collection("shop", "orders").unwrap().index("by_email").is_some_and(|i| i.unique),
            "the re-creation is newer than B's tombstone and stands on B too"
        );
    }

    #[test]
    fn a_replicated_drop_older_than_the_index_is_declined_and_counted() {
        // The residual ADR-141 cannot reach, made visible: a drop whose stamp
        // trails the creation of the index it names is declined, the index
        // stays, and the decline is counted where an operator can see it.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        b.create_collection("shop", "orders").unwrap();
        // B's drop first, then A's create: the create is the newer of the two.
        let dropped = b.drop_index_stamped("shop", "orders", "by_email").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        a.create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
            .unwrap();
        let drop = b.oplog_entry(&dropped.stamp.unwrap()).unwrap().expect("the drop entry");

        let outcome = a.apply_batch(&[drop]).unwrap();
        assert_eq!(outcome.ddl_declined, 1, "declined and counted: {outcome:?}");
        assert_eq!(outcome.ddl_refused, 0, "not a refusal: {outcome:?}");
        assert!(
            a.get_collection("shop", "orders").unwrap().index("by_email").is_some(),
            "the index it names is newer than the drop, and stands"
        );
    }

    #[test]
    fn a_locally_recreated_index_with_the_same_name_beats_the_tombstone() {
        // A tombstone must not make a name permanently unusable: a creation
        // stamped after the drop is a new index, not a resurrection — on the
        // node that recreated it and on every peer that replays the three.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], false, None).unwrap();
        a.drop_index("shop", "orders", "email_1").unwrap();
        let recreated = a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        assert!(recreated.unique, "the recreation is the new definition");
        let ca = a.get_collection("shop", "orders").unwrap();
        assert!(ca.index("email_1").is_some_and(|i| i.unique));

        pull(&b, &a);
        let cb = b.get_collection("shop", "orders").unwrap();
        let index = cb.index("email_1").expect("the recreation must replicate");
        assert!(index.unique, "and it is the recreated definition, not the dropped one");

        // Re-served, the same history reaches the same state.
        pull(&b, &a);
        assert!(b.get_collection("shop", "orders").unwrap().index("email_1").is_some());
    }

    #[test]
    fn a_replayed_drop_does_not_remove_a_newer_index_of_the_same_name() {
        // The shape observed on a three-member cluster on 2026-09-03: a
        // collection listed *no* indexes on any member, though three stood on
        // all three an hour earlier. A name is created, dropped and created
        // again; the drop between the two creations is re-served after the
        // recreation has arrived, resolves to the index standing under the
        // name, and — with no creation stamp to compare — removes it
        // (ADR-132).
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], false, None).unwrap();
        a.drop_index("shop", "orders", "email_1").unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();

        let history = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;
        let kinds: Vec<OpKind> = history.iter().map(|e| e.kind).collect();
        assert_eq!(
            kinds,
            vec![
                OpKind::CreateCollection,
                OpKind::CreateIndex,
                OpKind::DropIndex,
                OpKind::CreateIndex
            ],
            "the fixture is create, drop, recreate"
        );

        // B takes the recreation without the drop that precedes it — the peer
        // served a window that started after it.
        b.apply_batch(&[history[0].clone(), history[3].clone()]).unwrap();
        assert!(
            b.get_collection("shop", "orders").unwrap().index("email_1").is_some_and(|i| i.unique),
            "the recreated definition is the one B holds"
        );

        // Now the older drop arrives, as an overlapping window re-serves it.
        let outcome = b.apply_batch(&[history[2].clone()]).unwrap();
        assert_eq!(outcome.ddl_refused, 0, "declining a stale drop is not a refusal: {outcome:?}");
        assert_eq!(outcome.ddl_declined, 1, "but it is counted as declined (ADR-141): {outcome:?}");
        assert!(
            b.get_collection("shop", "orders").unwrap().index("email_1").is_some_and(|i| i.unique),
            "a drop older than the index it names must not remove it"
        );
        // Declined, but not forgotten: the tombstone is this node's record
        // that the drop was seen, and it stands at the drop's own stamp — the
        // `DropCollection` arm keeps its superseded drops the same way.
        assert_eq!(
            b.index_dropped_at(
                kimmy_core::CollectionId::derive("shop", "orders"),
                kimmy_core::IndexMeta::derive_id("email_1")
            )
            .unwrap(),
            Some(history[2].stamp),
            "a declined drop still leaves its tombstone"
        );

        // And the whole history, in order and repeatedly, reaches the same
        // state: the older creation is history against the tombstone the
        // declined drop still recorded.
        for _ in 0..3 {
            b.apply_batch(&history).unwrap();
            assert!(
                b.get_collection("shop", "orders")
                    .unwrap()
                    .index("email_1")
                    .is_some_and(|i| i.unique),
                "the newer index stands through a re-served window"
            );
        }
    }

    #[test]
    fn a_drop_replayed_after_this_nodes_own_recreation_leaves_it_alone() {
        // The half ADR-123 recorded as not converging on its own: when the
        // newer creation is *this* node's, no peer can re-serve it, so a
        // replayed drop left the index gone here for ever while every peer
        // kept it. The creation stamp settles it locally.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], false, None).unwrap();
        a.drop_index("shop", "orders", "email_1").unwrap();
        let history = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;
        b.apply_batch(&history).unwrap();
        assert!(b.get_collection("shop", "orders").unwrap().index("email_1").is_none());

        // B recreates the name itself, after the drop.
        b.create_index("shop", "orders", vec![field("email")], true, None).unwrap();

        b.apply_batch(&history).unwrap();
        assert!(
            b.get_collection("shop", "orders").unwrap().index("email_1").is_some_and(|i| i.unique),
            "B's own recreation must survive the replay of the drop it followed"
        );
    }

    #[test]
    fn a_drop_that_follows_the_creation_it_names_still_removes_the_index() {
        // The ordinary case, which the rule above must not cost: a drop
        // stamped after the index it names removes it, whether it arrives in
        // the same batch or a later one.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], false, None).unwrap();
        a.drop_index("shop", "orders", "email_1").unwrap();
        let history = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;

        // The creation first, on its own, so the drop meets a live index.
        b.apply_batch(&history[..2]).unwrap();
        let cb = b.get_collection("shop", "orders").unwrap();
        let index = cb.index("email_1").expect("the creation arrived").clone();
        assert!(index.created.is_some(), "a replicated creation records the origin's stamp");

        let outcome = b.apply_batch(&history[2..]).unwrap();
        assert_eq!(outcome.ddl_refused, 0, "{outcome:?}");
        assert!(
            b.get_collection("shop", "orders").unwrap().index("email_1").is_none(),
            "a drop newer than the index it names must still remove it"
        );
        let entries = crate::index::scan_range(
            b.db(),
            cb.id,
            index.id,
            &[],
            None,
            crate::index::Unkeyed::Exclude,
        )
        .unwrap();
        assert!(entries.is_empty(), "and with it every entry it held");
    }

    #[test]
    fn a_drop_still_removes_an_index_that_carries_no_creation_stamp() {
        // The stored-format rule on the drop side. An index written before
        // the stamp existed cannot say whether it predates a drop, and reads
        // as older than every one: the drop applies, which is what ADR-123
        // left and what a caller reading the reference before ADR-132
        // expects.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], false, None).unwrap();
        let history = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;
        b.apply_batch(&history).unwrap();
        restamp_index(&b, "shop", "orders", "email_1", None);

        a.drop_index("shop", "orders", "email_1").unwrap();
        pull(&b, &a);

        assert!(
            b.get_collection("shop", "orders").unwrap().index("email_1").is_none(),
            "an unstamped index is removed by a replicated drop, exactly as before"
        );
    }

    #[test]
    fn two_members_creating_one_identical_definition_converge_on_one_creation_stamp() {
        // The stamp has to converge, not only the definition. After ADR-132
        // it is the sole arbiter of whether a replayed drop applies, so two
        // members holding the same definition under two stamps answer the
        // same drop differently — and nothing re-serves the creations, both
        // having been witnessed, so the split is permanent and silent: the
        // drop is `Applied` on both sides, no counter moves and lag reads 0.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        // Two creations in one millisecond leave no stamp between them for
        // the drop below to occupy — the interesting case is the partition
        // that lasts, so the fixture makes the gap real rather than hoping
        // the scheduler provides one.
        std::thread::sleep(std::time::Duration::from_millis(2));
        b.create_collection("shop", "orders").unwrap();
        b.create_index("shop", "orders", vec![field("email")], true, None).unwrap();

        let stamps = |e: &Engine| {
            e.get_collection("shop", "orders").unwrap().index("email_1").unwrap().created.unwrap()
        };
        let (first, second) = (stamps(&a), stamps(&b));
        assert!(first < second, "the fixture needs two independent creations, A's first");

        // Heal, both ways and to quiescence.
        for _ in 0..3 {
            sync(&a, &b);
        }
        assert_eq!(stamps(&a), second, "the later creation is the one that stands");
        assert_eq!(stamps(&b), second, "on both members");

        // And a drop stamped between the two creations — which is exactly
        // what anti-entropy delivers on heal — now resolves the same way on
        // each of them.
        let between =
            Stamp::new(Hlc::new(first.hlc.wall_ms + 1, 0), kimmy_core::NodeId::from_bytes([9; 16]));
        assert!(between > first && between < second, "the fixture's drop sits between them");
        let drop = crate::engine::ddl_entry(
            between,
            OpKind::DropIndex,
            kimmy_core::CollectionId::derive("shop", "orders"),
            &kimmy_core::IndexDrop {
                db: "shop".into(),
                collection: "orders".into(),
                index: "email_1".into(),
            },
        )
        .unwrap();
        for engine in [&a, &b] {
            engine.apply_batch(std::slice::from_ref(&drop)).unwrap();
        }
        let holds =
            |e: &Engine| e.get_collection("shop", "orders").unwrap().index("email_1").is_some();
        assert_eq!(holds(&a), holds(&b), "the members must answer one drop alike");
        assert!(holds(&a), "and the drop precedes the creation that stands, so it is history");
    }

    #[test]
    fn an_identical_definition_is_not_re_stamped_by_a_local_recreation() {
        // The merge is for definitions arriving from a peer. A client asking
        // again for an index it already has is idempotent and mints no entry,
        // so moving the stamp here would be a decision no peer ever hears of
        // — which is the divergence the merge exists to close, from the other
        // side.
        let (a, _da) = engine();
        a.create_collection("shop", "orders").unwrap();
        let first = a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        let again = a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        assert_eq!(again.created, first.created, "an idempotent local create moves nothing");
        assert_eq!(
            a.get_collection("shop", "orders").unwrap().index("email_1").unwrap().created,
            first.created
        );
    }

    #[test]
    fn an_unstamped_index_learns_its_stamp_from_the_peer_that_has_one() {
        // The other half of the merge: an index stored before the stamp
        // existed is ambiguous, and a peer holding the same definition with a
        // stamp has the answer. Adopting it ends the ambiguity rather than
        // inventing one, and is what stops the two from answering a drop
        // differently for as long as the pair lives.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        let created = a.get_collection("shop", "orders").unwrap().index("email_1").unwrap().created;
        let history = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;
        b.apply_batch(&history).unwrap();
        restamp_index(&b, "shop", "orders", "email_1", None);

        b.apply_batch(&history).unwrap();

        assert_eq!(
            b.get_collection("shop", "orders").unwrap().index("email_1").unwrap().created,
            created,
            "the definition's true creation stamp is learned from the peer that carries it"
        );
    }

    #[test]
    fn a_losing_definition_is_not_re_served_to_a_third_member() {
        // Why `IndexCreated::Older` exists at all. A creation this node has
        // decided is history must not be appended: appending it would
        // propagate through this node a definition this node does not hold,
        // to every member that pulls from it, for ever. ADR-123's rule for a
        // refusal, and ADR-132's for a loser.
        let node = |n: u8| kimmy_core::NodeId::from_bytes([n; 16]);
        let earlier = Stamp::new(Hlc::new(1_000, 0), node(1));
        let later = Stamp::new(Hlc::new(2_000, 0), node(2));
        let id = kimmy_core::CollectionId::derive("shop", "orders");
        let loser = create_index_entry(
            id,
            "shop",
            "orders",
            definition("by_email", vec![field("email")], false, Some(earlier)),
            earlier,
        );
        let winner = create_index_entry(
            id,
            "shop",
            "orders",
            definition("by_email", vec![field("email")], true, Some(later)),
            later,
        );

        let (m, _dm) = engine();
        m.create_collection("shop", "orders").unwrap();
        m.apply_batch(&[winner]).unwrap();
        let outcome = m.apply_batch(&[loser]).unwrap();
        assert_eq!(outcome.ddl_refused, 0, "history, not a refusal: {outcome:?}");

        let served = m.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;
        assert!(
            !served.iter().any(|e| e.kind == OpKind::CreateIndex && e.stamp == earlier),
            "this node must not serve onward a definition it decided was history: {served:?}"
        );
        assert!(
            served.iter().any(|e| e.kind == OpKind::CreateIndex && e.stamp == later),
            "and it must still serve the one it holds"
        );
    }

    #[test]
    fn a_replicated_unique_index_whose_backfill_collides_is_built_with_the_collision_recorded() {
        // ADR-020 for a definition rather than a document: A created a unique
        // index over its one document; B, partitioned, holds a document with
        // the same value. When A's create reaches B the backfill finds the
        // two sharing a key. Refusing the create would leave B without the
        // index for ever — every later write A checks would go unchecked on
        // B, and index-backed queries would answer differently on the two
        // members — so the index is built in full and the collision is
        // recorded, exactly as a merged write's is.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "a", "email": "clash@x" }).unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, None).unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "b", "email": "clash@x" }).unwrap();

        let outcome = pull(&b, &a);
        assert_eq!(outcome.ddl_refused, 0, "built, not refused: {outcome:?}");
        assert_eq!(outcome.ddl, 2, "the collection and the index: {outcome:?}");
        let cb = b.get_collection("shop", "orders").unwrap();
        let index = cb.index("email_1").expect("the unique index must exist on B");
        assert!(index.unique);

        // Built in full: both holders are in the index, so an index-backed
        // query finds both.
        let entries = crate::index::scan_range(
            b.db(),
            cb.id,
            index.id,
            &[],
            None,
            crate::index::Unkeyed::Exclude,
        )
        .unwrap();
        assert_eq!(entries.len(), 2, "every document is indexed, the colliding one included");

        // Recorded the way a merged write's collision is.
        assert_eq!(b.unique_violations(), 1, "counted once per shared key");
        let live = b.live_unique_violations(&cb).unwrap();
        assert_eq!(live.len(), 1, "the violations route reports it: {live:?}");
        assert_eq!(live[0].index, "email_1");
        let mut ids: Vec<String> = live[0].ids.iter().map(|id| id.to_string()).collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"], "naming every holder");
        let recorded = b
            .read_oplog_from(Hlc::ZERO, BATCH)
            .unwrap()
            .into_iter()
            .filter(|e| e.kind == OpKind::UniqueViolation)
            .count();
        assert_eq!(recorded, 1, "a `UniqueViolation` entry exists for change streams");

        // And the constraint is live for what comes next: a local write
        // through it is checked.
        let refused = b.insert(&cb, doc! { "_id": "c", "email": "clash@x" });
        assert!(
            matches!(
                refused,
                Err(crate::StorageError::Core(kimmy_core::Error::UniqueViolation { .. }))
            ),
            "a later local write through the index must be checked: {refused:?}"
        );
        assert!(b.insert(&cb, doc! { "_id": "d", "email": "free@x" }).is_ok());
    }

    /// An index definition as a peer would put it on the wire, stamped where
    /// a test says it was created.
    ///
    /// Hand-built rather than replicated out of a second engine so that the
    /// two definitions of a concurrent creation sit in a *known* order: two
    /// engines creating an index microseconds apart land in the same
    /// millisecond, where the node id breaks the tie and the winner is
    /// whichever `NodeId::generate` happened to produce.
    fn create_index_entry(
        collection: kimmy_core::CollectionId,
        db: &str,
        name: &str,
        index: crate::meta::IndexMeta,
        stamp: Stamp,
    ) -> OplogEntry {
        crate::engine::ddl_entry(
            stamp,
            OpKind::CreateIndex,
            collection,
            &kimmy_core::IndexCreate { db: db.to_string(), collection: name.to_string(), index },
        )
        .unwrap()
    }

    /// A definition under `name`, stamped `created`.
    fn definition(
        name: &str,
        fields: Vec<crate::meta::IndexField>,
        unique: bool,
        created: Option<Stamp>,
    ) -> crate::meta::IndexMeta {
        crate::meta::IndexMeta {
            id: kimmy_core::IndexMeta::derive_id(name),
            name: name.to_string(),
            fields,
            unique,
            enforcement: Default::default(),
            multikey: false,
            expire_after_secs: None,
            partial_filter: None,
            created,
        }
    }

    /// Rewrite an index's creation stamp in place: `None` stands in for one
    /// stored by a build that recorded none, and an explicit stamp puts a
    /// local definition in a known order against an arriving one without
    /// racing the wall clock.
    fn restamp_index(
        engine: &Engine,
        db: &str,
        collection: &str,
        name: &str,
        created: Option<Stamp>,
    ) {
        let mut meta = engine.get_collection(db, collection).unwrap();
        meta.indexes.iter_mut().find(|i| i.name == name).expect("the index is here").created =
            created;
        // Straight at the database, the way a test that means to write what
        // another build would have written does — the engine's own chokepoint
        // is for writes the engine makes.
        let db = engine.db();
        let txn = db.begin_write().unwrap();
        Engine::put_collection_meta(&txn, &meta).unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn concurrent_definitions_under_one_name_settle_on_the_later_stamp() {
        // Two members created one name with different definitions while they
        // could not see each other. ADR-123 left both standing, counted; a
        // schema that stays divergent for ever is not a resting state, and
        // the round of 2026-09-03 watched two collections sit that way. They
        // settle the way two concurrent writes to one document settle: the
        // later stamp wins, on whichever member the entries reach in
        // whichever order (ADR-132).
        let node = |n: u8| kimmy_core::NodeId::from_bytes([n; 16]);
        let earlier = Stamp::new(Hlc::new(1_000, 0), node(1));
        let later = Stamp::new(Hlc::new(2_000, 0), node(2));
        let id = kimmy_core::CollectionId::derive("shop", "orders");
        let loser = definition("by_email", vec![field("email")], false, Some(earlier));
        let winner = definition("by_email", vec![field("email")], true, Some(later));
        let entries = [
            create_index_entry(id, "shop", "orders", loser, earlier),
            create_index_entry(id, "shop", "orders", winner, later),
        ];

        // Both orders, because a member cannot choose which reaches it first.
        for order in [[0, 1], [1, 0]] {
            let (m, _dm) = engine();
            m.create_collection("shop", "orders").unwrap();
            for i in order {
                let outcome = m.apply_batch(&[entries[i].clone()]).unwrap();
                assert_eq!(outcome.ddl_refused, 0, "neither definition is refused: {outcome:?}");
            }

            let index = m
                .get_collection("shop", "orders")
                .unwrap()
                .index("by_email")
                .cloned()
                .expect("the name still holds an index");
            assert!(index.unique, "the later definition wins, arriving {order:?}");
            assert_eq!(index.created, Some(later), "and it keeps the stamp that won");
        }
    }

    #[test]
    fn the_loser_of_a_concurrent_creation_does_not_come_back_through_a_replayed_create() {
        // The resolution has to hold against a re-served window, which
        // anti-entropy produces routinely — otherwise the two definitions
        // would trade places on every round for ever.
        let node = |n: u8| kimmy_core::NodeId::from_bytes([n; 16]);
        let earlier = Stamp::new(Hlc::new(1_000, 0), node(1));
        let later = Stamp::new(Hlc::new(2_000, 0), node(2));
        let id = kimmy_core::CollectionId::derive("shop", "orders");
        let loser = create_index_entry(
            id,
            "shop",
            "orders",
            definition("by_email", vec![field("email")], false, Some(earlier)),
            earlier,
        );
        let winner = create_index_entry(
            id,
            "shop",
            "orders",
            definition("by_email", vec![field("email")], true, Some(later)),
            later,
        );

        let (m, _dm) = engine();
        let cm = m.create_collection("shop", "orders").unwrap();
        m.apply_batch(&[loser.clone(), winner.clone()]).unwrap();
        // The tombstone the replacement records stands at the *winner's*
        // stamp. Above it, the winner's own re-delivery would read as history
        // and the definition could never be rebuilt after a later drop; below
        // it, the loser's replay reaches the comparison instead of being
        // stopped here. Recorded rather than inferred, so neither bound can
        // move unnoticed.
        assert_eq!(
            m.index_dropped_at(cm.id, kimmy_core::IndexMeta::derive_id("by_email")).unwrap(),
            Some(later),
            "the supersede's tombstone is the winner's stamp"
        );
        for _ in 0..3 {
            let outcome = m.apply_batch(&[loser.clone(), winner.clone()]).unwrap();
            assert_eq!(outcome.ddl_refused, 0, "{outcome:?}");
            let index =
                m.get_collection("shop", "orders").unwrap().index("by_email").unwrap().clone();
            assert!(index.unique, "the winner stands through a re-served window");
        }
    }

    #[test]
    fn a_rival_definition_with_no_creation_stamp_is_still_refused_and_counted() {
        // The stored-format rule. An index written before the creation stamp
        // existed carries none, and a stamp invented for it here would decide
        // a comparison this node knows nothing about — so there is nothing to
        // compare, and the arrival is skipped, counted and named exactly as
        // ADR-123 left it. The ambiguity ends the first time the index is
        // recreated.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        b.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("email")], true, Some("by_email".into()))
            .unwrap();
        b.create_index("shop", "orders", vec![field("email")], false, Some("by_email".into()))
            .unwrap();
        restamp_index(&b, "shop", "orders", "by_email", None);
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "after" }).unwrap();

        let outcome = round(&b, &a, BATCH);
        assert_eq!(outcome.ddl_refused, 1, "{outcome:?}");
        assert!(
            !b.get_collection("shop", "orders").unwrap().index("by_email").unwrap().unique,
            "B keeps the definition it cannot arbitrate away"
        );
        let cb = b.get_collection("shop", "orders").unwrap();
        assert!(
            b.get(&cb, &DocId::String("after".into())).unwrap().is_some(),
            "and the refusal does not stop the entries behind it"
        );
        assert_eq!(round(&b, &a, BATCH), SyncOutcome::default(), "witnessed, not re-served");
    }

    #[test]
    fn a_definition_that_wins_the_stamp_but_cannot_be_applied_leaves_the_one_it_would_replace() {
        // ADR-123's guard, kept honest against the rule that replaces its
        // `IndexExists` case. A refusal is still a refusal: the winning
        // definition is one this build cannot apply — a TTL over two fields,
        // which no member can mint but a re-served or hand-built entry can
        // carry — so the whole replacement aborts. B keeps the index it had
        // rather than ending with neither, and the round goes on, skipped
        // and counted, rather than wedging. Under ADR-139 no *document* can
        // make a non-unique definition unbuildable any more, which is why
        // the winner here is unbuildable by shape.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "both", "tags": ["x", "y"], "cats": ["p", "q"] }).unwrap();
        b.create_index("shop", "orders", vec![field("tags")], false, Some("probe".into())).unwrap();
        // B's definition is older than anything A can mint, so a winner's
        // stamp beats it.
        restamp_index(
            &b,
            "shop",
            "orders",
            "probe",
            Some(Stamp::new(Hlc::new(1, 0), kimmy_core::NodeId::from_bytes([0; 16]))),
        );
        let held = b.get_collection("shop", "orders").unwrap().index("probe").unwrap().id;
        let before = crate::index::scan_range(
            b.db(),
            cb.id,
            held,
            &[],
            None,
            crate::index::Unkeyed::Exclude,
        )
        .unwrap();
        assert!(!before.is_empty(), "the fixture gave the index entries to lose");

        let ca = a.get_collection("shop", "orders").unwrap();
        let stamp = a.next_stamp();
        let mut winner =
            definition("probe", vec![field("tags"), field("cats")], false, Some(stamp));
        winner.expire_after_secs = Some(60);
        a.insert(&ca, doc! { "_id": "after" }).unwrap();
        let mut batch = vec![create_index_entry(ca.id, "shop", "orders", winner, stamp)];
        batch.extend(a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries);
        batch.sort_by_key(|e| e.stamp);

        let outcome = b.apply_batch(&batch).unwrap();
        assert_eq!(outcome.ddl_refused, 1, "skipped and counted, not applied: {outcome:?}");
        let index = b.get_collection("shop", "orders").unwrap().index("probe").cloned().unwrap();
        assert_eq!(
            index.fields.len(),
            1,
            "the replacement aborted whole: B keeps its own definition, not neither"
        );
        let after = crate::index::scan_range(
            b.db(),
            cb.id,
            index.id,
            &[],
            None,
            crate::index::Unkeyed::Exclude,
        )
        .unwrap();
        for key in &before {
            assert!(
                after.contains(key),
                "every entry the index held is still there: the transaction that removed them \
                 aborted with the failed build"
            );
        }
        assert!(
            b.get(&cb, &DocId::String("after".into())).unwrap().is_some(),
            "the entries behind the refusal still arrive"
        );
        assert!(
            b.witnessed_vector().unwrap().get(stamp.node) >= stamp.hlc,
            "witnessed, so it would not be re-requested"
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

        let entries = a.entries_for_peer(Hlc::ZERO, BATCH).unwrap().entries;
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
    fn a_replayed_drop_from_the_previous_incarnation_does_not_empty_the_recreation() {
        // The 2026-08-28 data loss. A creates, drops and recreates `orders`
        // and writes into the recreation; B has followed all of it. Then the
        // range containing the old drop is delivered again — the normal case
        // for overlapping batches — and must change nothing.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.drop_collection("shop", "orders").unwrap();
        let recreated = a.create_collection("shop", "orders").unwrap();
        a.insert(&recreated, doc! { "_id": "kept" }).unwrap();
        pull(&b, &a);
        let cb = b.get_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "kept-on-b" }).unwrap();
        pull(&a, &b);
        assert_eq!(a.count(&recreated).unwrap(), 2);
        assert_eq!(b.count(&cb).unwrap(), 2);

        // Re-deliver A's whole history to B, and B's whole history to A,
        // twice: the old drop rides along both times.
        for _ in 0..2 {
            let everything = a.entries_for_peer(Hlc::new(0, 0), BATCH).unwrap().entries;
            assert!(everything.iter().any(|e| e.kind == OpKind::DropCollection));
            b.apply_batch(&everything).unwrap();
            let everything = b.entries_for_peer(Hlc::new(0, 0), BATCH).unwrap().entries;
            a.apply_batch(&everything).unwrap();
        }

        let ca = a.get_collection("shop", "orders").expect("a still has the recreation");
        let cb = b.get_collection("shop", "orders").expect("b still has the recreation");
        assert_eq!(a.count(&ca).unwrap(), 2, "a replayed drop must not empty the recreation");
        assert_eq!(b.count(&cb).unwrap(), 2, "a replayed drop must not empty the recreation");
    }

    #[test]
    fn a_replayed_drop_is_ignored_on_a_peer_that_only_ever_saw_the_recreation() {
        // C never recorded the first drop, so it has no incarnation floor;
        // the guard has to fall back to the create's origin stamp.
        let (a, _da) = engine();
        let (c, _dc) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.drop_collection("shop", "orders").unwrap();
        let recreated = a.create_collection("shop", "orders").unwrap();
        a.insert(&recreated, doc! { "_id": "kept" }).unwrap();

        let everything = a.entries_for_peer(Hlc::new(0, 0), BATCH).unwrap().entries;
        let old_drop =
            everything.iter().find(|e| e.kind == OpKind::DropCollection).cloned().unwrap();
        let after_the_drop: Vec<_> =
            everything.iter().filter(|e| e.stamp > old_drop.stamp).cloned().collect();

        // C learns the recreation and the document first…
        c.apply_batch(&after_the_drop).unwrap();
        let cc = c.get_collection("shop", "orders").unwrap();
        assert_eq!(c.count(&cc).unwrap(), 1);
        // …and the old drop only afterwards.
        c.apply_batch(std::slice::from_ref(&old_drop)).unwrap();
        let cc = c.get_collection("shop", "orders").expect("the stale drop must not apply");
        assert_eq!(c.count(&cc).unwrap(), 1);
    }

    #[test]
    fn a_later_drop_still_drops_a_recreated_collection() {
        // The guard must not turn every drop after a recreation into a no-op.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.drop_collection("shop", "orders").unwrap();
        a.create_collection("shop", "orders").unwrap();
        pull(&b, &a);
        assert!(b.get_collection("shop", "orders").is_ok());

        a.drop_collection("shop", "orders").unwrap();
        pull(&b, &a);
        assert!(b.get_collection("shop", "orders").is_err(), "a genuinely later drop applies");
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
            provider: kimmy_core::ProviderConfig::Byo {},
            dim: 4,
            metric: Default::default(),
            document_prefix: None,
            query_prefix: None,
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
    fn the_same_lag_measure_reversed_says_how_long_ago_a_peer_last_saw_us() {
        // Roles swapped, the measure is how long ago the peer last applied
        // an entry of an origin *this* node is ahead on, per origin, ignoring
        // origins it has never seen at all. Not what the stale-rejoiner
        // verdict runs on — that is `lag_beyond_horizon_ms` — but the same
        // rules about which origins count.
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

        let now = 100_000;
        assert_eq!(
            lag_behind_ms(&theirs, &mine, now),
            90_000,
            "90 s behind on `a`; `c` does not count"
        );
        assert_eq!(lag_behind_ms(&mine, &theirs, now), 0, "and we trail it by nothing");

        // A fresh member trails nobody: it holds nothing old enough to resurrect.
        assert_eq!(lag_behind_ms(&VersionVector::default(), &mine, now), 0);
    }

    const HOUR_MS: u64 = 60 * 60 * 1000;

    #[test]
    fn a_gap_holding_nothing_collected_is_not_a_stale_rejoiner() {
        // The false verdict from the rolling restart (ADR-097). A wrote once,
        // idled 36 hours, wrote again; B holds the first write and has not yet
        // pulled the second. The span is 36 hours, but nothing in it was
        // collected: B can still be served the one entry it lacks.
        let a = kimmy_core::NodeId::generate();
        let first = Hlc::new(1_000, 0);
        let second = Hlc::new(1_000 + 36 * HOUR_MS, 0);
        let mut theirs = VersionVector::new();
        theirs.insert(a, first);
        let mut mine = VersionVector::new();
        mine.insert(a, second);

        // Five seconds after the second write, the age-behind measure reads
        // the whole silence too: the one-round spike ADR-122 keeps, because
        // the head vector cannot say the gap holds one entry five seconds
        // old. The verdict below cannot afford that, and does not use it.
        let now = second.wall_ms + 5_000;
        assert_eq!(lag_behind_ms(&theirs, &mine, now), 36 * HOUR_MS + 5_000);

        let mut collected = VersionVector::new();
        collected.insert(a, first);
        assert_eq!(
            lag_beyond_horizon_ms(&theirs, &mine, &collected),
            0,
            "B holds everything of A's that was collected, so it is not stale"
        );
        assert!(!lacks_collected(&theirs, &mine, &collected), "and it can be served");

        // Had A written in between and had that write been collected, B would
        // lack it, and the same span is then what it says it is.
        collected.insert(a, Hlc::new(1_000 + HOUR_MS, 0));
        assert_eq!(lag_beyond_horizon_ms(&theirs, &mine, &collected), 36 * HOUR_MS);
        assert!(lacks_collected(&theirs, &mine, &collected));

        // An origin the peer has never seen still counts for nothing, as in
        // `lag_behind_ms`: a brand-new member holds nothing to resurrect.
        assert_eq!(lag_beyond_horizon_ms(&VersionVector::default(), &mine, &collected), 0);
        // ...but a peer that holds *nothing* does lack what was collected, so
        // it is served a snapshot rather than a silent gap.
        assert!(lacks_collected(&VersionVector::default(), &mine, &collected));
    }

    #[test]
    fn an_origin_that_wrote_once_after_a_long_silence_is_still_servable() {
        // Two engines, the restart shape without a network. A writes, both
        // converge, B writes twice more, and everything ages past retention on
        // A — everything but B's last entry, which is the tail. Then A writes
        // once. B asks from A's *previous* write, which A collected along
        // with B's first, so by the threshold B is beyond the horizon and gets
        // a snapshot for a gap that holds exactly one servable entry.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "a-1" }).unwrap();
        sync(&a, &b);
        let cb = b.get_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "b-1" }).unwrap();
        b.insert(&cb, doc! { "_id": "b-2" }).unwrap();
        sync(&a, &b);

        let a_previous = b.witnessed_vector().unwrap().get(a.node_id());
        a.collect_garbage_at(
            crate::physical_now_ms() + 365 * 24 * HOUR_MS,
            RetentionPolicy::new(DAY, DAY),
        )
        .unwrap();
        assert!(
            a.oplog_collected_through().unwrap() > a_previous,
            "B's first write was collected after A's, so the coarse horizon is above A's previous write"
        );
        assert_eq!(
            a.oplog_collected().unwrap().get(a.node_id()),
            a_previous,
            "the per-origin record names A's previous write as the last of A's collected"
        );

        a.insert(&ca, doc! { "_id": "a-2" }).unwrap();
        let held = b.witnessed_vector().unwrap();
        let from = held.behind(&a.version_vector().unwrap()).expect("B trails A now");
        assert_eq!(from, a_previous);

        assert!(
            !a.can_serve_from_oplog(from).unwrap(),
            "by the threshold, B is beyond the horizon"
        );
        assert!(
            a.can_serve_peer_holding(&held).unwrap(),
            "per origin, B lacks nothing A collected: it can be served the one entry"
        );

        // And it is exactly what a stale-rejoiner verdict must not fire on.
        let mine = a.witnessed_vector().unwrap();
        let theirs = b.version_vector().unwrap();
        assert_eq!(lag_beyond_horizon_ms(&theirs, &mine, &a.oplog_collected().unwrap()), 0);
    }

    #[test]
    fn a_peer_that_missed_a_collected_entry_is_beyond_the_horizon_per_origin_too() {
        // The control. B holds A's first write but not its second; A collects
        // the second along with the rest. B lacks something gone, at A's
        // origin, and both the threshold and the per-origin check say so.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "a-1" }).unwrap();
        sync(&a, &b);
        a.insert(&ca, doc! { "_id": "a-2" }).unwrap();
        let cb = b.get_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "b-1" }).unwrap();
        b.insert(&cb, doc! { "_id": "b-2" }).unwrap();
        pull(&a, &b);

        a.collect_garbage_at(
            crate::physical_now_ms() + 365 * 24 * HOUR_MS,
            RetentionPolicy::new(DAY, DAY),
        )
        .unwrap();
        a.insert(&ca, doc! { "_id": "a-3" }).unwrap();

        let held = b.witnessed_vector().unwrap();
        let from = held.behind(&a.version_vector().unwrap()).unwrap();
        assert!(!a.can_serve_from_oplog(from).unwrap());
        assert!(!a.can_serve_peer_holding(&held).unwrap(), "B lacks A's collected second write");

        // A fresh peer against a node that has collected: a snapshot, as
        // before. Against one that has not: served, as before.
        assert!(!a.can_serve_peer_holding(&VersionVector::default()).unwrap());
        assert!(b.can_serve_peer_holding(&VersionVector::default()).unwrap());
    }
}
