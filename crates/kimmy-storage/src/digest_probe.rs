//! The cut digest's probe (B3 design 2.4 to 2.7).
//!
//! Two members cannot compare what each holds while either is taking writes,
//! so the comparison is made at a **cut**: `V = min(W_A, W_B)`, the highest
//! stamp per origin both have witnessed. Anything stamped at or below `V` was
//! processed by both before the exchange began and cannot change for the
//! length of it; anything above is *uncovered*, and its keys are excluded from
//! both digests.
//!
//! **The invariant: a comparison is valid only if both sides excluded the same
//! keys.** A collection is *quiet* at a cut when no record in it is stamped
//! above the cut (`maxw[o] <= V[o]` for every origin `o`), so it has no
//! uncovered key and its digest is its kept row's, an O(1) read. A busy
//! collection's uncovered keys are found by one scan of the oplog above the
//! cut, under a wall-time and a byte budget.
//!
//! **Three steps, and nothing held across the network.**
//! 1. [`Engine::digest_plan`] (the requester, `T1`): the cut, and its
//!    uncovered keys per collection.
//! 2. [`Engine::digest_answer`] (the peer): its own uncovered keys, and its
//!    digest with the union of both sets excluded. For a collection quiet on
//!    the peer where the requester has uncovered keys, the peer subtracts
//!    `term(key, its stamp)` for each by point reads; one subtracted twice
//!    would be added back, so the keys are a set.
//! 3. [`Engine::digest_compare`] (the requester, `T2`): every deferral
//!    predicate is evaluated again, the uncovered set recomputed with the same
//!    cut, and a write that landed in between defers the collection rather than
//!    mismatching it.
//!
//! Every outcome that is not a comparison has a [`Defer`] reason and is
//! counted by the caller; none is silent.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use kimmy_core::{CollectionId, Hlc, Stamp, VersionVector};
use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::codec;
use crate::engine::{Engine, PairedShadows};
use crate::error::Result;
use crate::live_digest::{self, DigestRow};
use crate::tables;
use crate::walk::{WalkScope, open_walk_table};

/// The wall time one scan of the oplog may take, a placeholder set from the
/// cold measurements: cold, on mars's disks, this binds first (about 2 MB); a
/// recent tail is normally in the page cache and 8 MiB takes milliseconds.
pub const SCAN_WALL: Duration = Duration::from_millis(200);

/// The bytes one scan of the oplog may read.
pub const SCAN_BYTES: u64 = 8 * 1024 * 1024;

/// The bytes of uncovered keys a request may carry, over which the collection
/// is deferred.
pub const ASK_KEY_BYTES: usize = 256 * 1024;

/// The bytes of key a peer will point-read to subtract a request's uncovered
/// keys, and of its own uncovered keys it will send back.
pub const ANSWER_KEY_BYTES: usize = 1024 * 1024;

/// What one scan may spend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanBudget {
    pub wall: Duration,
    pub bytes: u64,
}

impl Default for ScanBudget {
    fn default() -> Self {
        Self { wall: SCAN_WALL, bytes: SCAN_BYTES }
    }
}

/// Why a collection was not compared, counted by the caller under this label.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Defer {
    /// A bare tombstone (ADR-228) above the cut on either side.
    BareTombstone,
    /// The oplog above the cut cannot be read in full here: retention removed
    /// it or a snapshot gave the state without the entries.
    Horizon,
    /// Either side holds a tombstone for the collection's database.
    DatabaseTombstone,
    /// A purge of the collection's rows is owed.
    BurialPending,
    /// A write landed between the plan and the comparison.
    Moved,
    /// More uncovered keys than the request or the answer may carry.
    UncoveredCap,
    /// The scan spent its wall time or its bytes.
    Budget,
    /// The peer was already answering a digest probe.
    Busy,
    /// The two sides hold the collection at different incarnations.
    Incarnation,
    /// A self-check was already running.
    SelfCheckBusy,
}

impl Defer {
    /// Every reason, in the order [`Self::index`] numbers them.
    pub const ALL: [Defer; 10] = [
        Self::BareTombstone,
        Self::Horizon,
        Self::DatabaseTombstone,
        Self::BurialPending,
        Self::Moved,
        Self::UncoveredCap,
        Self::Budget,
        Self::Busy,
        Self::Incarnation,
        Self::SelfCheckBusy,
    ];

    /// This reason's place in [`Self::ALL`], for a counter per reason.
    pub const fn index(self) -> usize {
        match self {
            Self::BareTombstone => 0,
            Self::Horizon => 1,
            Self::DatabaseTombstone => 2,
            Self::BurialPending => 3,
            Self::Moved => 4,
            Self::UncoveredCap => 5,
            Self::Budget => 6,
            Self::Busy => 7,
            Self::Incarnation => 8,
            Self::SelfCheckBusy => 9,
        }
    }

    /// The label of `kimmy_sync_divergence_digest_probes_total{reason}`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::BareTombstone => "bare_tombstone",
            Self::Horizon => "horizon",
            Self::DatabaseTombstone => "database_tombstone",
            Self::BurialPending => "burial_pending",
            Self::Moved => "moved",
            Self::UncoveredCap => "uncovered_cap",
            Self::Budget => "budget",
            Self::Busy => "busy",
            Self::Incarnation => "incarnation",
            Self::SelfCheckBusy => "self_check_busy",
        }
    }
}

/// The keys one collection holds above the cut on the requester's side.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Uncovered {
    pub collection: CollectionId,
    pub keys: Vec<ByteBuf>,
}

/// What a requester sends: the cut, the keys it excludes, and the one busy
/// collection it asks the peer to answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestAsk {
    pub cut: VersionVector,
    #[serde(default, deserialize_with = "lenient_uncovered")]
    pub uncovered: Vec<Uncovered>,
    /// The busy collection this contact's rotation names: the peer answers
    /// every collection that is quiet on its side, and this one if it is busy.
    #[serde(default)]
    pub busy: Option<CollectionId>,
}

/// The request's uncovered lists, read one at a time as the entries are.
fn lenient_uncovered<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<Uncovered>, D::Error> {
    let raw = Vec::<bson::Bson>::deserialize(deserializer)?;
    Ok(raw.into_iter().filter_map(|entry| bson::deserialize_from_bson(entry).ok()).collect())
}

/// One collection's digest, as the peer computed it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestEntry {
    pub collection: CollectionId,
    /// 16 bytes, big-endian: the digest with the excluded keys taken out.
    pub digest: ByteBuf,
    /// How many of the requester's uncovered keys the peer processed, after
    /// removing duplicates, absent keys and tombstones included: the number the
    /// requester's own count must equal for the comparison to be valid.
    pub applied: u32,
    /// The peer's own uncovered keys, for a collection that was busy there.
    #[serde(default)]
    pub uncovered: Vec<ByteBuf>,
    /// Set instead of a digest when the peer cannot compare this collection.
    #[serde(default)]
    pub deferred: Option<Defer>,
}

/// The peer's answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestReply {
    #[serde(default, deserialize_with = "lenient_entries")]
    pub entries: Vec<DigestEntry>,
}

/// Entries read one at a time: one that does not decode (a reason this build
/// does not know, a digest of the wrong shape) is dropped and the rest are
/// kept, so a new field costs the entry and never the frame.
fn lenient_entries<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<DigestEntry>, D::Error> {
    let raw = Vec::<bson::Bson>::deserialize(deserializer)?;
    Ok(raw.into_iter().filter_map(|entry| bson::deserialize_from_bson(entry).ok()).collect())
}

/// How the peer took a digest request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DigestAnswered {
    /// With digests.
    Answered,
    /// Already answering a probe: distinct from not answering at all.
    Busy,
    /// With none: a peer that predates the digest, or one that failed to
    /// compute it. Never compared.
    Unanswered,
    /// Asked with the cut alone, to learn whether the peer answers digests
    /// before paying for this side's scan (a peer that has not yet answered
    /// one); nothing is compared this contact.
    Probed,
}

/// What a contact's digest probe came to, for the caller to count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigestRound {
    pub answered: DigestAnswered,
    /// Per collection: compared (equal or not) or deferred, with the reason.
    pub verdicts: Vec<DigestVerdict>,
    /// Collections this side could not ask about, with the reason.
    pub plan_deferred: Vec<(CollectionId, Defer)>,
}

/// What one collection came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DigestOutcome {
    /// Compared: whether the digests are equal, and both digests (with the
    /// excluded keys taken out of each), for the log line a finding gets.
    Compared { equal: bool, mine: u128, theirs: u128 },
    /// Not compared, and why.
    Deferred(Defer),
}

/// A collection's outcome at the end of a probe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DigestVerdict {
    pub collection: CollectionId,
    pub outcome: DigestOutcome,
}

/// A collection as one side sees it at a cut.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Facet {
    created: Hlc,
    row: DigestRow,
    quiet: bool,
    deferred: Option<Defer>,
    /// The keys stamped above the cut. Empty for a quiet collection.
    uncovered: BTreeSet<Vec<u8>>,
}

/// The requester's side, kept between the plan and the comparison.
#[derive(Clone, Debug)]
pub struct DigestPlan {
    /// The cut: `min(W_A, W_B)`, fixed by the request.
    pub cut: VersionVector,
    /// What to send.
    pub ask: DigestAsk,
    facets: BTreeMap<CollectionId, Facet>,
}

impl DigestPlan {
    /// Collections this side could not even ask about, and why: counted
    /// whether or not the peer answers.
    pub fn deferred(&self) -> impl Iterator<Item = (CollectionId, Defer)> + '_ {
        self.facets.iter().filter_map(|(id, f)| f.deferred.map(|d| (*id, d)))
    }
}

/// Whether a scan is for every busy collection or for one.
#[derive(Clone, Copy)]
enum ScanFor {
    All,
    Only(Option<CollectionId>),
}

impl Engine {
    /// The requester's `T1`: the cut against `peer_witnessed`, and this side's
    /// view of every collection at it.
    ///
    /// A read of the rows, and, only if some collection is busy, one scan of
    /// the oplog: run under [`crate::blocking`].
    pub fn digest_plan(
        &self,
        peer_witnessed: &VersionVector,
        busy: Option<CollectionId>,
        budget: ScanBudget,
    ) -> Result<DigestPlan> {
        let txn = self.db().begin_read()?;
        let witnessed = Self::read_versions_in(&txn, tables::OPLOG_WITNESSED)?;
        let cut = witnessed.meet(peer_witnessed);
        let mut facets = self.assess(&txn, &cut, ScanFor::All, budget)?;

        let mut uncovered = Vec::new();
        let mut bytes = 0usize;
        for (id, facet) in &mut facets {
            if facet.deferred.is_some() || facet.uncovered.is_empty() {
                continue;
            }
            let size: usize = facet.uncovered.iter().map(Vec::len).sum();
            if bytes + size > ASK_KEY_BYTES {
                facet.deferred = Some(Defer::UncoveredCap);
                continue;
            }
            bytes += size;
            uncovered.push(Uncovered {
                collection: *id,
                keys: facet.uncovered.iter().cloned().map(ByteBuf::from).collect(),
            });
        }
        Ok(DigestPlan { cut: cut.clone(), ask: DigestAsk { cut, uncovered, busy }, facets })
    }

    /// The peer's answer to a request: a digest for every collection quiet
    /// here, and for the one busy collection the request names; each with the
    /// requester's uncovered keys and this side's own excluded.
    ///
    /// `None` when another probe is already being answered: one at a time, so
    /// a requester that is told so counts it as busy and not as unanswered.
    pub fn digest_answer(
        &self,
        ask: &DigestAsk,
        budget: ScanBudget,
    ) -> Result<Option<DigestReply>> {
        let Some(_guard) = Flag::take(&self.digest_probe_busy) else { return Ok(None) };
        let txn = self.db().begin_read()?;
        let facets = self.assess(&txn, &ask.cut, ScanFor::Only(ask.busy), budget)?;
        let theirs: BTreeMap<CollectionId, BTreeSet<Vec<u8>>> = ask
            .uncovered
            .iter()
            .map(|u| (u.collection, u.keys.iter().map(|k| k.to_vec()).collect()))
            .collect();
        let docs = txn.open_table(tables::DOCS)?;
        let mut read = 0usize;
        let mut entries = Vec::new();
        for (id, facet) in &facets {
            if !facet.quiet && Some(*id) != ask.busy {
                // Busy here and not the one asked for: not answered, and not
                // a deferral. The rotation names it another contact.
                continue;
            }
            let none = BTreeSet::new();
            let theirs = theirs.get(id).unwrap_or(&none);
            let mut entry = DigestEntry {
                collection: *id,
                digest: ByteBuf::from(facet.row.digest.to_be_bytes().to_vec()),
                applied: theirs.len() as u32,
                uncovered: Vec::new(),
                deferred: facet.deferred,
            };
            if entry.deferred.is_none() {
                let own: usize = facet.uncovered.iter().map(Vec::len).sum();
                if own > ANSWER_KEY_BYTES {
                    entry.deferred = Some(Defer::UncoveredCap);
                } else {
                    let union: BTreeSet<&Vec<u8>> = theirs.iter().chain(&facet.uncovered).collect();
                    match excluded_digest(
                        &docs,
                        *id,
                        &facet.row,
                        union,
                        &mut read,
                        ANSWER_KEY_BYTES,
                    )? {
                        Some(digest) => {
                            entry.digest = ByteBuf::from(digest.to_be_bytes().to_vec());
                            entry.uncovered =
                                facet.uncovered.iter().cloned().map(ByteBuf::from).collect();
                        }
                        None => entry.deferred = Some(Defer::UncoveredCap),
                    }
                }
            }
            entries.push(entry);
        }
        Ok(Some(DigestReply { entries }))
    }

    /// The requester's `T2`: every predicate again, the uncovered set again
    /// with the same cut, and the comparison.
    ///
    /// `peer_incarnations` is what the peer reported as each collection's
    /// `created`: a collection held at another incarnation is not compared.
    pub fn digest_compare(
        &self,
        plan: &DigestPlan,
        reply: &DigestReply,
        peer_incarnations: &BTreeMap<CollectionId, Hlc>,
        budget: ScanBudget,
    ) -> Result<Vec<DigestVerdict>> {
        let txn = self.db().begin_read()?;
        let now = self.assess(&txn, &plan.cut, ScanFor::All, budget)?;
        let docs = txn.open_table(tables::DOCS)?;
        let mut read = 0usize;
        let mut out = Vec::new();
        for entry in &reply.entries {
            let id = entry.collection;
            let (Some(then), Some(fresh)) = (plan.facets.get(&id), now.get(&id)) else {
                // The peer holds it and this side does not (or the reverse):
                // the existence half's business.
                continue;
            };
            let defer =
                |d: Defer| DigestVerdict { collection: id, outcome: DigestOutcome::Deferred(d) };
            if let Some(d) = entry.deferred {
                out.push(defer(d));
                continue;
            }
            if peer_incarnations.get(&id).is_some_and(|created| *created != then.created) {
                out.push(defer(Defer::Incarnation));
                continue;
            }
            if let Some(d) = then.deferred.or(fresh.deferred) {
                out.push(defer(d));
                continue;
            }
            // Every predicate again, and the uncovered set again: something
            // that landed between the plan and now defers the collection.
            let moved = then.created != fresh.created
                || then.quiet != fresh.quiet
                || !fresh.uncovered.is_subset(&then.uncovered);
            if moved {
                out.push(defer(Defer::Moved));
                continue;
            }
            if entry.applied as usize != then.uncovered.len() {
                // The peer did not subtract the keys this side excluded, so
                // the two digests were not taken over the same set.
                out.push(defer(Defer::UncoveredCap));
                continue;
            }
            let theirs: BTreeSet<Vec<u8>> = entry.uncovered.iter().map(|k| k.to_vec()).collect();
            let union: BTreeSet<&Vec<u8>> = then.uncovered.iter().chain(&theirs).collect();
            let Some(mine) = excluded_digest(&docs, id, &fresh.row, union, &mut read, usize::MAX)?
            else {
                out.push(defer(Defer::UncoveredCap));
                continue;
            };
            let theirs = <[u8; 16]>::try_from(entry.digest.as_slice()).map(u128::from_be_bytes);
            let Ok(theirs) = theirs else {
                // A digest of the wrong width is no digest.
                out.push(defer(Defer::UncoveredCap));
                continue;
            };
            out.push(DigestVerdict {
                collection: id,
                outcome: DigestOutcome::Compared { equal: theirs == mine, mine, theirs },
            });
        }
        Ok(out)
    }

    /// Every collection held (shadows hidden) as this side sees it at `cut`.
    fn assess(
        &self,
        txn: &redb::ReadTransaction,
        cut: &VersionVector,
        scan_for: ScanFor,
        budget: ScanBudget,
    ) -> Result<BTreeMap<CollectionId, Facet>> {
        let metas = self.collections(PairedShadows::Hidden)?;
        let rows = txn.open_table(tables::LIVE_DIGESTS)?;
        let owed: BTreeSet<CollectionId> = self.owed_purges()?.into_iter().collect();
        let mut tombstoned: BTreeMap<String, bool> = BTreeMap::new();
        let mut facets = BTreeMap::new();
        for meta in &metas {
            let row = live_digest::kept_row(&rows, meta.id.0)?.unwrap_or_default();
            let quiet = row.maxw.iter().all(|(node, hlc)| *hlc <= cut.get(*node));
            let deferred = if row.bare_max.iter().any(|(node, hlc)| *hlc > cut.get(*node)) {
                Some(Defer::BareTombstone)
            } else if match tombstoned.get(&meta.db) {
                Some(known) => *known,
                None => {
                    let held = self.database_dropped_at(&meta.db)?.is_some();
                    tombstoned.insert(meta.db.clone(), held);
                    held
                }
            } {
                Some(Defer::DatabaseTombstone)
            } else if owed.contains(&meta.id) {
                Some(Defer::BurialPending)
            } else {
                None
            };
            facets.insert(
                meta.id,
                Facet { created: meta.created, row, quiet, deferred, uncovered: BTreeSet::new() },
            );
        }

        // The oplog above the cut is read for the busy collections only, and
        // only if it can be read in full here.
        let wanted = |id: &CollectionId| match scan_for {
            ScanFor::All => true,
            ScanFor::Only(only) => Some(*id) == only,
        };
        let busy: BTreeSet<CollectionId> = facets
            .iter()
            .filter(|(id, f)| !f.quiet && f.deferred.is_none() && wanted(id))
            .map(|(id, _)| *id)
            .collect();
        if busy.is_empty() {
            return Ok(facets);
        }
        if self.serve_verdict(cut)? != crate::snapshot::ServeVerdict::Serve {
            for id in &busy {
                facets.get_mut(id).expect("a busy collection").deferred = Some(Defer::Horizon);
            }
            return Ok(facets);
        }
        // From the lowest cut among the origins that can hold an uncovered
        // record of a busy collection: an origin whose records are all covered
        // (a frozen or retired one) does not drag the scan back (review N3).
        let from = busy
            .iter()
            .flat_map(|id| facets[id].row.maxw.iter().filter(|(node, hlc)| **hlc > cut.get(**node)))
            .map(|(node, _)| cut.get(*node))
            .min()
            .unwrap_or(Hlc::ZERO);
        match self.scan_uncovered(txn, cut, &busy, from, budget)? {
            Some(found) => {
                for (id, keys) in found {
                    facets.get_mut(&id).expect("a busy collection").uncovered = keys;
                }
            }
            None => {
                for id in &busy {
                    facets.get_mut(id).expect("a busy collection").deferred = Some(Defer::Budget);
                }
            }
        }
        Ok(facets)
    }

    /// The keys of `busy` collections written above `cut`, read from the
    /// oplog starting at `from`. `None` if the budget was spent first.
    ///
    /// Stop-aware at every row (`walks_stop`).
    fn scan_uncovered(
        &self,
        txn: &redb::ReadTransaction,
        cut: &VersionVector,
        busy: &BTreeSet<CollectionId>,
        from: Hlc,
        budget: ScanBudget,
    ) -> Result<Option<BTreeMap<CollectionId, BTreeSet<Vec<u8>>>>> {
        let started = Instant::now();
        let oplog = open_walk_table(txn, tables::OPLOG, self.walk(WalkScope::Background))?;
        let start = codec::oplog_key_lower_bound(from);
        let mut found: BTreeMap<CollectionId, BTreeSet<Vec<u8>>> = BTreeMap::new();
        let mut bytes = 0u64;
        for row in oplog.range::<&[u8]>(start.as_slice()..)? {
            let (key, value) = row?;
            let (key, value) = (key.value(), value.value());
            bytes += (key.len() + value.len()) as u64;
            if bytes > budget.bytes || started.elapsed() > budget.wall {
                return Ok(None);
            }
            let stamp = codec::decode_oplog_key(key)?;
            if stamp.hlc <= cut.get(stamp.node) {
                continue;
            }
            let (_, collection) = codec::decode_oplog_kind_and_collection(value)?;
            if !busy.contains(&collection) {
                continue;
            }
            // A DDL entry has no document id; a unique violation names a
            // document but writes no record, which over-includes, safely.
            if let Some(doc_id) = codec::decode_oplog_entry(value)?.doc_id {
                found.entry(collection).or_default().insert(crate::docs::doc_key(&doc_id)?);
            }
        }
        Ok(Some(found))
    }

    /// Check a collection's kept digest row against its records, and correct it
    /// if it drifted (B3 design 2.7).
    ///
    /// The kept row and the walk are read in **one read transaction**, so they
    /// are one state however the writers move. A difference is corrected by an
    /// XOR delta in a short write transaction: XOR is linear, so a concurrent
    /// commit's own delta survives, and the writer is never held for the walk.
    /// Over the budget, or with another check running: [`SelfCheck::Busy`].
    pub fn verify_digest_row(
        &self,
        collection: CollectionId,
        budget: ScanBudget,
    ) -> Result<SelfCheck> {
        let Some(_guard) = Flag::take(&self.digest_check_busy) else { return Ok(SelfCheck::Busy) };
        let (kept, walked) = {
            let txn = self.db().begin_read()?;
            let rows = txn.open_table(tables::LIVE_DIGESTS)?;
            let kept = live_digest::kept_row(&rows, collection.0)?.unwrap_or_default();
            match self.walk_headers(&txn, collection, budget)? {
                Some(walked) => (kept, walked),
                None => return Ok(SelfCheck::Busy),
            }
        };
        let drifted = kept.digest != walked.digest
            || walked.maxw.iter().any(|(node, hlc)| kept.maxw.get(node).is_none_or(|k| k < hlc));
        if !drifted {
            return Ok(SelfCheck::Exact);
        }
        #[cfg(test)]
        test_hooks::after_the_snapshot(self);
        // `row_now ^ fix`, with `fix = row_at_snapshot ^ walk`: a delta, so
        // what other commits did since the snapshot is kept.
        let fix = kept.digest ^ walked.digest;
        let txn = self.begin_write(crate::engine::WriterHolder::Write)?;
        {
            let mut rows = txn.open_table(tables::LIVE_DIGESTS)?;
            let mut now = live_digest::kept_row(&rows, collection.0)?.unwrap_or_default();
            now.digest ^= fix;
            for (node, hlc) in &walked.maxw {
                now.raise_maxw(&Stamp::new(*hlc, *node));
            }
            rows.insert(collection.0, now.encode().as_slice())?;
        }
        txn.commit()?;
        self.digest_drift.fetch_add(1, Ordering::Relaxed);
        Ok(SelfCheck::Corrected)
    }

    /// The digest row a walk of `collection`'s record headers gives, in `txn`.
    /// `None` if the budget was spent first.
    ///
    /// Stop-aware at every row (`walks_stop`).
    fn walk_headers(
        &self,
        txn: &redb::ReadTransaction,
        collection: CollectionId,
        budget: ScanBudget,
    ) -> Result<Option<DigestRow>> {
        let started = Instant::now();
        let docs = open_walk_table(txn, tables::DOCS, self.walk(WalkScope::Background))?;
        let mut walked = BTreeMap::new();
        let mut bytes = 0u64;
        for row in docs.range(crate::engine::doc_range(collection))? {
            let (key, value) = row?;
            let (_, doc_key) = key.value();
            let value = value.value();
            bytes += (doc_key.len() + value.len()) as u64;
            if bytes > budget.bytes || started.elapsed() > budget.wall {
                return Ok(None);
            }
            // not a row: a record header that does not decode, left out as the rebuild does
            if let Ok((stamp, live)) = codec::doc_record_header(value) {
                live_digest::fold_walked(&mut walked, collection.0, doc_key, &stamp, live);
            }
        }
        // not a row: the walk's own map, read after the walk
        Ok(Some(walked.remove(&collection.0).unwrap_or_default()))
    }

    /// How many times a self-check found a kept digest row that disagreed with
    /// its records and corrected it. Zero on a healthy node.
    pub fn digest_drift_total(&self) -> u64 {
        self.digest_drift.load(Ordering::Relaxed)
    }

    /// The kept digest row of a collection, for the self-check's memo.
    pub fn digest_row(&self, collection: CollectionId) -> Result<Option<DigestRow>> {
        let txn = self.db().begin_read()?;
        let rows = txn.open_table(tables::LIVE_DIGESTS)?;
        live_digest::kept_row(&rows, collection.0)
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::RefCell;

    use crate::Engine;

    /// What runs between a self-check's snapshot and its correction.
    type Hook = Box<dyn FnOnce(&Engine)>;

    thread_local! {
        /// Run between a self-check's snapshot and its correction, on this
        /// thread: where a concurrent commit lands in the field.
        static AFTER_THE_SNAPSHOT: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    pub(crate) fn between_the_snapshot_and_the_correction(f: impl FnOnce(&Engine) + 'static) {
        AFTER_THE_SNAPSHOT.with(|hook| *hook.borrow_mut() = Some(Box::new(f)));
    }

    pub(super) fn after_the_snapshot(engine: &Engine) {
        if let Some(hook) = AFTER_THE_SNAPSHOT.with(|hook| hook.borrow_mut().take()) {
            hook(engine);
        }
    }
}

/// What a self-check found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelfCheck {
    /// The kept row agreed with the records.
    Exact,
    /// It did not, and has been corrected.
    Corrected,
    /// Not run to the end: over its budget, or another check was running.
    Busy,
}

/// `row.digest` with the live records of `keys` taken out, by point reads of
/// `DOCS`. `None` if reading them would pass `cap` bytes of key.
fn excluded_digest<'k>(
    docs: &impl ReadableTable<(u64, &'static [u8]), &'static [u8]>,
    collection: CollectionId,
    row: &DigestRow,
    keys: impl IntoIterator<Item = &'k Vec<u8>>,
    read: &mut usize,
    cap: usize,
) -> Result<Option<u128>> {
    let mut digest = row.digest;
    for key in keys {
        *read += key.len();
        if *read > cap {
            return Ok(None);
        }
        if let Some(record) = docs.get((collection.0, key.as_slice()))? {
            let (stamp, live) = codec::doc_record_header(record.value())?;
            if live {
                digest ^= live_digest::term(key, &stamp);
            }
        }
    }
    Ok(Some(digest))
}

/// A flag held while a thing runs, so a second caller is told so.
struct Flag<'a>(&'a std::sync::atomic::AtomicBool);

impl<'a> Flag<'a> {
    fn take(flag: &'a std::sync::atomic::AtomicBool) -> Option<Self> {
        flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).ok()?;
        Some(Self(flag))
    }
}

impl Drop for Flag<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::{DocId, NodeId};

    use super::*;
    use crate::Engine;

    const BATCH: usize = 1_024;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
    }

    /// One direction of an anti-entropy round.
    fn pull(into: &Engine, from: &Engine) {
        let mine = into.version_vector().unwrap();
        let theirs = from.version_vector().unwrap();
        if let Some(start) = mine.behind(&theirs) {
            let entries =
                from.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
            into.apply_batch(&entries).unwrap();
        }
    }

    fn sync(a: &Engine, b: &Engine) {
        pull(a, b);
        pull(b, a);
    }

    fn incarnations(e: &Engine) -> BTreeMap<CollectionId, Hlc> {
        e.collection_incarnations().unwrap()
    }

    /// A requester `a` probing `b`, all the way through.
    fn exchange(a: &Engine, b: &Engine, busy: Option<CollectionId>) -> Vec<DigestVerdict> {
        let budget = ScanBudget::default();
        let plan = a.digest_plan(&b.witnessed_vector().unwrap(), busy, budget).unwrap();
        let reply = b.digest_answer(&plan.ask, budget).unwrap().expect("answered");
        a.digest_compare(&plan, &reply, &incarnations(b), budget).unwrap()
    }

    fn outcome_of(verdicts: &[DigestVerdict], id: CollectionId) -> Option<DigestOutcome> {
        verdicts.iter().find(|v| v.collection == id).map(|v| v.outcome)
    }

    /// Whether the collection was compared, and found equal.
    fn compared(verdicts: &[DigestVerdict], id: CollectionId) -> Option<bool> {
        match outcome_of(verdicts, id) {
            Some(DigestOutcome::Compared { equal, .. }) => Some(equal),
            _ => None,
        }
    }

    /// Two members holding `shop.orders` and `shop.items`, converged.
    fn converged()
    -> (Engine, Engine, crate::CollectionMeta, crate::CollectionMeta, [tempfile::TempDir; 2]) {
        let (a, da) = engine();
        let (b, db) = engine();
        let orders = a.create_collection("shop", "orders").unwrap();
        let items = a.create_collection("shop", "items").unwrap();
        for i in 0..6i64 {
            a.insert(&orders, doc! { "_id": i, "n": i }).unwrap();
            a.insert(&items, doc! { "_id": i }).unwrap();
        }
        a.delete(&orders, &DocId::Int64(2)).unwrap();
        sync(&a, &b);
        (a, b, orders, items, [da, db])
    }

    fn old_stamp(node: NodeId) -> Stamp {
        Stamp::new(Hlc::new(1, 0), node)
    }

    #[test]
    fn every_reason_has_its_own_place_and_label() {
        for (n, reason) in Defer::ALL.into_iter().enumerate() {
            assert_eq!(reason.index(), n);
            assert_eq!(Defer::ALL.iter().filter(|r| r.label() == reason.label()).count(), 1);
        }
    }

    #[test]
    fn converged_members_compare_equal_in_both_directions() {
        let (a, b, orders, items, _dirs) = converged();
        for (x, y) in [(&a, &b), (&b, &a)] {
            let verdicts = exchange(x, y, None);
            for id in [orders.id, items.id] {
                assert_eq!(compared(&verdicts, id), Some(true), "{verdicts:?}");
            }
        }
    }

    #[test]
    fn a_lost_update_the_witnessed_vector_vouches_for_is_a_mismatch() {
        let (a, b, orders, items, _dirs) = converged();
        // B holds an older version of order 3 than A, though B's vector covers
        // the newer: the shape the count half cannot see.
        let key = crate::docs::doc_key(&DocId::Int64(3)).unwrap();
        {
            let txn = b.begin_write(crate::engine::WriterHolder::Write).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            let body = bson::serialize_to_vec(&doc! { "_id": 3i64, "n": -1 }).unwrap();
            let record = kimmy_core::DocRecord::live(old_stamp(a.node_id()), body);
            crate::live_count::put_record(
                &txn,
                &mut docs,
                orders.id.0,
                &key,
                &codec::encode_doc_record(&record),
            )
            .unwrap();
            drop(docs);
            txn.commit().unwrap();
        }
        let verdicts = exchange(&a, &b, None);
        assert_eq!(compared(&verdicts, orders.id), Some(false));
        assert_eq!(
            compared(&verdicts, items.id),
            Some(true),
            "an unrelated collection still agrees"
        );
    }

    #[test]
    fn a_write_the_peer_has_not_seen_is_excluded_on_both_sides_and_is_no_finding() {
        let (a, b, orders, _items, _dirs) = converged();
        // A new key and an update of an existing one, both above the cut.
        a.insert(&orders, doc! { "_id": 100i64 }).unwrap();
        a.replace(&orders, &DocId::Int64(4), doc! { "_id": 4i64, "n": 400 }, false).unwrap();
        // B is quiet in the collection and holds the old version of key 4
        // (N4): it subtracts it, since A excluded it.
        let verdicts = exchange(&a, &b, None);
        assert_eq!(compared(&verdicts, orders.id), Some(true), "{verdicts:?}");
        // And the other way: the requester quiet, the peer busy.
        let verdicts = exchange(&b, &a, Some(orders.id));
        assert_eq!(compared(&verdicts, orders.id), Some(true), "{verdicts:?}");
    }

    #[test]
    fn a_key_in_both_uncovered_sets_is_subtracted_once() {
        let (a, b, orders, _items, _dirs) = converged();
        // Both write key 5 after the sync: it is uncovered on both sides.
        a.replace(&orders, &DocId::Int64(5), doc! { "_id": 5i64, "n": 1 }, false).unwrap();
        b.replace(&orders, &DocId::Int64(5), doc! { "_id": 5i64, "n": 2 }, false).unwrap();
        let verdicts = exchange(&a, &b, Some(orders.id));
        assert_eq!(
            compared(&verdicts, orders.id),
            Some(true),
            "a key subtracted twice would add its term back and mismatch: {verdicts:?}"
        );
    }

    #[test]
    fn an_answer_that_did_not_apply_the_requesters_keys_is_not_compared() {
        let (a, b, orders, _items, _dirs) = converged();
        a.insert(&orders, doc! { "_id": 100i64 }).unwrap();
        let budget = ScanBudget::default();
        let plan = a.digest_plan(&b.witnessed_vector().unwrap(), None, budget).unwrap();
        // A peer that ignored the request's uncovered keys.
        let mut bare = plan.ask.clone();
        bare.uncovered.clear();
        let reply = b.digest_answer(&bare, budget).unwrap().unwrap();
        let verdicts = a.digest_compare(&plan, &reply, &incarnations(&b), budget).unwrap();
        assert_eq!(
            outcome_of(&verdicts, orders.id),
            Some(DigestOutcome::Deferred(Defer::UncoveredCap)),
            "{verdicts:?}"
        );
    }

    #[test]
    fn a_write_between_the_plan_and_the_comparison_defers_the_collection() {
        let (a, b, orders, items, _dirs) = converged();
        let budget = ScanBudget::default();
        let plan = a.digest_plan(&b.witnessed_vector().unwrap(), None, budget).unwrap();
        let reply = b.digest_answer(&plan.ask, budget).unwrap().unwrap();
        // After the plan: a new key in a quiet collection.
        a.insert(&orders, doc! { "_id": 200i64 }).unwrap();
        let verdicts = a.digest_compare(&plan, &reply, &incarnations(&b), budget).unwrap();
        assert_eq!(outcome_of(&verdicts, orders.id), Some(DigestOutcome::Deferred(Defer::Moved)));
        assert_eq!(
            compared(&verdicts, items.id),
            Some(true),
            "a collection nothing was written to is still compared"
        );
    }

    #[test]
    fn a_new_key_between_the_plan_and_the_comparison_in_a_busy_collection_defers_it() {
        let (a, b, orders, _items, _dirs) = converged();
        a.insert(&orders, doc! { "_id": 100i64 }).unwrap();
        let budget = ScanBudget::default();
        let plan = a.digest_plan(&b.witnessed_vector().unwrap(), None, budget).unwrap();
        let reply = b.digest_answer(&plan.ask, budget).unwrap().unwrap();
        a.insert(&orders, doc! { "_id": 101i64 }).unwrap();
        let verdicts = a.digest_compare(&plan, &reply, &incarnations(&b), budget).unwrap();
        assert_eq!(outcome_of(&verdicts, orders.id), Some(DigestOutcome::Deferred(Defer::Moved)));
    }

    #[test]
    fn a_bare_tombstone_above_the_cut_defers_on_either_side() {
        let (a, b, orders, _items, _dirs) = converged();
        let stamp = Stamp::new(Hlc::new(crate::physical_now_ms() + 90_000, 0), NodeId::generate());
        let key = crate::docs::doc_key(&DocId::Int64(1_000)).unwrap();
        {
            let txn = b.begin_write(crate::engine::WriterHolder::Write).unwrap();
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            crate::live_count::put_bare_tombstone(
                &txn,
                &mut docs,
                orders.id.0,
                &key,
                &codec::encode_doc_record(&kimmy_core::DocRecord::tombstone(stamp)),
            )
            .unwrap();
            drop(docs);
            txn.commit().unwrap();
        }
        // The peer reports its own (busy here, so it is the one asked for).
        let verdicts = exchange(&a, &b, Some(orders.id));
        assert_eq!(
            outcome_of(&verdicts, orders.id),
            Some(DigestOutcome::Deferred(Defer::BareTombstone)),
            "{verdicts:?}"
        );
        // And this side's own.
        let verdicts = exchange(&b, &a, None);
        assert_eq!(
            outcome_of(&verdicts, orders.id),
            Some(DigestOutcome::Deferred(Defer::BareTombstone)),
            "{verdicts:?}"
        );
    }

    #[test]
    fn a_scan_over_its_budget_defers_and_a_frozen_origin_does_not_drag_it_back() {
        let (a, b, orders, items, _dirs) = converged();
        // An origin that wrote once, long ago, on both: it stays in both
        // vectors at its last stamp for ever (a reseeded member's old id).
        let old = NodeId::generate();
        for e in [&a, &b] {
            let entry = kimmy_core::OplogEntry {
                stamp: old_stamp(old),
                kind: kimmy_core::OpKind::Insert,
                collection: items.id,
                doc_id: Some(DocId::Int64(7_000)),
                body: Some(bson::serialize_to_vec(&doc! { "_id": 7000i64 }).unwrap()),
            };
            e.apply_remote(&items, &entry).unwrap();
        }
        // A crowd of covered entries, then one new write above the cut.
        for i in 0..400i64 {
            a.insert(&items, doc! { "_id": 10_000 + i }).unwrap();
        }
        sync(&a, &b);
        a.insert(&orders, doc! { "_id": 100i64 }).unwrap();
        // A scan from the frozen origin's stamp would read the whole crowd and
        // spend this budget; from the busy collection's own cut it reads one
        // entry.
        let tight = ScanBudget { wall: Duration::from_secs(60), bytes: 4 * 1024 };
        let plan = a.digest_plan(&b.witnessed_vector().unwrap(), None, tight).unwrap();
        let reply = b.digest_answer(&plan.ask, tight).unwrap().unwrap();
        let verdicts = a.digest_compare(&plan, &reply, &incarnations(&b), tight).unwrap();
        assert_eq!(
            compared(&verdicts, orders.id),
            Some(true),
            "the frozen origin dragged the scan's start back: {verdicts:?}"
        );
        // And with nothing to spend the same scan defers.
        let none = ScanBudget { wall: Duration::from_secs(60), bytes: 0 };
        let plan = a.digest_plan(&b.witnessed_vector().unwrap(), None, none).unwrap();
        assert!(
            plan.deferred().any(|(id, d)| id == orders.id && d == Defer::Budget),
            "{:?}",
            plan.deferred().collect::<Vec<_>>()
        );
    }

    #[test]
    fn only_one_probe_is_answered_at_a_time() {
        let (a, b, _orders, _items, _dirs) = converged();
        let plan =
            a.digest_plan(&b.witnessed_vector().unwrap(), None, ScanBudget::default()).unwrap();
        let held = Flag::take(&b.digest_probe_busy).expect("free");
        assert!(b.digest_answer(&plan.ask, ScanBudget::default()).unwrap().is_none());
        drop(held);
        assert!(b.digest_answer(&plan.ask, ScanBudget::default()).unwrap().is_some());
    }

    #[test]
    fn a_collection_at_another_incarnation_is_not_compared() {
        let (a, b, orders, _items, _dirs) = converged();
        let budget = ScanBudget::default();
        let plan = a.digest_plan(&b.witnessed_vector().unwrap(), None, budget).unwrap();
        let reply = b.digest_answer(&plan.ask, budget).unwrap().unwrap();
        let mut theirs = incarnations(&b);
        theirs.insert(orders.id, Hlc::new(5, 0));
        let verdicts = a.digest_compare(&plan, &reply, &theirs, budget).unwrap();
        assert_eq!(
            outcome_of(&verdicts, orders.id),
            Some(DigestOutcome::Deferred(Defer::Incarnation))
        );
    }

    #[test]
    fn the_self_check_finds_a_forged_row_corrects_it_by_delta_and_leaves_a_sound_one() {
        let (a, _b, orders, _items, _dirs) = converged();
        assert_eq!(
            a.verify_digest_row(orders.id, ScanBudget::default()).unwrap(),
            SelfCheck::Exact
        );
        assert_eq!(a.digest_drift_total(), 0);
        let truth = a.digest_row(orders.id).unwrap().unwrap();
        {
            let txn = a.db().begin_write().unwrap();
            let mut forged = truth.clone();
            forged.digest ^= 0xabcd;
            txn.open_table(tables::LIVE_DIGESTS)
                .unwrap()
                .insert(orders.id.0, forged.encode().as_slice())
                .unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(
            a.verify_digest_row(orders.id, ScanBudget::default()).unwrap(),
            SelfCheck::Corrected
        );
        assert_eq!(a.digest_row(orders.id).unwrap().unwrap().digest, truth.digest);
        assert_eq!(a.digest_drift_total(), 1);
        assert_eq!(
            a.verify_digest_row(orders.id, ScanBudget::default()).unwrap(),
            SelfCheck::Exact
        );
    }

    #[test]
    fn the_self_check_over_its_budget_or_while_another_runs_is_busy() {
        let (a, _b, orders, _items, _dirs) = converged();
        let none = ScanBudget { wall: Duration::from_secs(60), bytes: 0 };
        assert_eq!(a.verify_digest_row(orders.id, none).unwrap(), SelfCheck::Busy);
        let held = Flag::take(&a.digest_check_busy).expect("free");
        assert_eq!(a.verify_digest_row(orders.id, ScanBudget::default()).unwrap(), SelfCheck::Busy);
        drop(held);
    }

    #[test]
    fn a_digest_message_round_trips_through_bson() {
        let ask = DigestAsk {
            cut: [(NodeId::generate(), Hlc::new(5, 1))].into_iter().collect(),
            uncovered: vec![Uncovered {
                collection: CollectionId(u64::MAX),
                keys: vec![ByteBuf::from(vec![1, 2, 3])],
            }],
            busy: Some(CollectionId(7)),
        };
        let back: DigestAsk =
            bson::deserialize_from_slice(&bson::serialize_to_vec(&ask).unwrap()).unwrap();
        assert_eq!(back, ask);
        let reply = DigestReply {
            entries: vec![DigestEntry {
                collection: CollectionId(9),
                digest: ByteBuf::from(vec![0u8; 16]),
                applied: 3,
                uncovered: vec![ByteBuf::from(vec![4])],
                deferred: Some(Defer::BareTombstone),
            }],
        };
        let back: DigestReply =
            bson::deserialize_from_slice(&bson::serialize_to_vec(&reply).unwrap()).unwrap();
        assert_eq!(back, reply);
    }
    #[test]
    fn the_self_check_keeps_what_a_commit_did_between_its_snapshot_and_its_correction() {
        let (a, _b, orders, _items, _dirs) = converged();
        // A forged row, and a write that lands after the check has read its
        // state and before it corrects: the correction is a delta, so the
        // write's own term survives.
        {
            let truth = a.digest_row(orders.id).unwrap().unwrap();
            let txn = a.db().begin_write().unwrap();
            let mut forged = truth;
            forged.digest ^= 0x5555;
            txn.open_table(tables::LIVE_DIGESTS)
                .unwrap()
                .insert(orders.id.0, forged.encode().as_slice())
                .unwrap();
            txn.commit().unwrap();
        }
        let id = orders.clone();
        test_hooks::between_the_snapshot_and_the_correction(move |engine| {
            engine.insert(&id, doc! { "_id": 500i64 }).unwrap();
        });
        assert_eq!(
            a.verify_digest_row(orders.id, ScanBudget::default()).unwrap(),
            SelfCheck::Corrected
        );
        // Whatever the walk says now, including the write that landed between.
        assert_eq!(
            a.verify_digest_row(orders.id, ScanBudget::default()).unwrap(),
            SelfCheck::Exact,
            "the correction lost what the concurrent commit added"
        );
    }

    #[test]
    fn a_rebuilt_row_defers_on_a_tombstone_where_a_maintained_one_compares() {
        let (a, b, orders, _items, _dirs) = converged();
        // A deletes a document B has not yet received: its tombstone is above
        // the cut. Maintained, the row knows an entry stands behind it.
        a.delete(&orders, &DocId::Int64(4)).unwrap();
        let verdicts = exchange(&a, &b, Some(orders.id));
        assert_eq!(
            compared(&verdicts, orders.id),
            Some(true),
            "a maintained row compares: the delete is uncovered and set aside: {verdicts:?}"
        );
        // Rebuilt from the headers, the row cannot tell whether an entry
        // stands behind a tombstone, so it defers while one is above the cut.
        let path = a.db_path_for_test();
        drop(a);
        {
            let db = redb::Database::open(&path).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_DIGESTS_THROUGH)
                .unwrap()
                .remove(live_digest::THROUGH)
                .unwrap();
            txn.commit().unwrap();
        }
        let a = Engine::open(&path).unwrap();
        let verdicts = exchange(&a, &b, Some(orders.id));
        assert_eq!(
            outcome_of(&verdicts, orders.id),
            Some(DigestOutcome::Deferred(Defer::BareTombstone)),
            "{verdicts:?}"
        );
    }

    #[test]
    fn a_cut_below_what_retention_removed_defers_the_busy_collection_on_the_horizon() {
        let (a, b, orders, _items, _dirs) = converged();
        // A writes what B has not seen, and retention then removes the oplog
        // above the cut: the uncovered keys can no longer be read.
        a.insert(&orders, doc! { "_id": 100i64 }).unwrap();
        // Retention keeps the newest entry of an origin, so there is a later one.
        a.insert(&orders, doc! { "_id": 101i64 }).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        a.collect_garbage(crate::RetentionPolicy::new(0, 3_600)).unwrap();
        let plan =
            a.digest_plan(&b.witnessed_vector().unwrap(), None, ScanBudget::default()).unwrap();
        assert!(
            plan.deferred().any(|(id, d)| id == orders.id && d == Defer::Horizon),
            "{:?}",
            plan.deferred().collect::<Vec<_>>()
        );
    }
}
