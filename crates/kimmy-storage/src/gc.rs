//! Retention: collecting oplog entries and tombstones that are past their age.
//!
//! Both tables grow monotonically until something removes from them. The oplog
//! gains an entry per mutation forever, and a delete leaves a tombstone rather
//! than removing a key. Retention was configurable long before it was enforced;
//! this is the enforcement.
//!
//! # The invariant that makes this safe
//!
//! **The newest oplog entry is never collected, whatever its age.**
//!
//! The logical clock is not persisted separately — it is resumed on startup
//! from the oplog tail ([`Engine::open`]). Collect the last entry and a restart
//! reads an empty oplog, resumes at [`Hlc::ZERO`], and begins minting stamps
//! *below* ones already on disk. Every subsequent write to an existing document
//! would then lose to its own older version under last-writer-wins, and lose
//! silently: no error, no log line, just an update that does not take.
//!
//! An idle node is exactly where this bites — no writes means every entry is
//! eventually older than the retention window, so the naive rule empties the
//! log precisely when nothing is happening to hide the damage.
//!
//! # What collection costs
//!
//! Collecting a tombstone gives up the ability to out-argue a *later-arriving,
//! older* delete-versus-insert: a partitioned peer that never saw the delete
//! can reintroduce the document. That is the documented reason
//! `tombstone_retention_secs` must exceed the longest partition you intend to
//! survive — see [Operations](../../../docs/operations.md).
//!
//! Collecting an oplog prefix expires resume tokens that point into it. Change
//! streams already handle that (`ResumeTokenExpired`, surfaced as HTTP 410), so
//! it is a contract, not a surprise.

use kimmy_core::Hlc;
use redb::{ReadableDatabase, ReadableTable};
use tracing::{debug, warn};

use crate::codec;
use crate::engine::{Engine, WriterHolder, physical_now_ms};
use crate::error::Result;
use crate::tables;

/// How long each kind of garbage is retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub oplog_secs: u64,
    pub tombstone_secs: u64,
}

impl RetentionPolicy {
    pub const fn new(oplog_secs: u64, tombstone_secs: u64) -> Self {
        Self { oplog_secs, tombstone_secs }
    }
}

/// Oplog entries removed per write transaction of a retention pass.
///
/// Each chunk is one commit, and the writer is released between chunks, so
/// this bounds how long one pass can hold it at a stretch while a day's
/// worth of expired entries is collected in as many chunks as it takes
/// (ADR-151).
pub const OPLOG_COLLECT_CHUNK: usize = 1_000;

/// Documents the tombstone scan visits per pass, under a read transaction.
///
/// A pass over a table larger than this covers the rest on the passes that
/// follow, resuming where it stopped; a tombstone is collected within
/// `ceil(documents / budget)` passes of expiring. At the default interval
/// that is a few passes for a million documents, against a retention of a
/// day (ADR-151).
pub const TOMBSTONE_SCAN_BUDGET: usize = 100_000;

/// Tombstones removed per write transaction of a retention pass.
pub const TOMBSTONE_COLLECT_CHUNK: usize = 1_000;

/// A tombstone the scan found expired, as it was when seen.
struct ExpiredTombstone {
    collection: u64,
    key: Vec<u8>,
    stamp: kimmy_core::Stamp,
}

/// What one collection pass removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcOutcome {
    pub oplog_removed: usize,
    pub tombstones_removed: usize,
}

impl GcOutcome {
    pub fn is_empty(&self) -> bool {
        self.oplog_removed == 0 && self.tombstones_removed == 0
    }
}

impl Engine {
    /// Run one retention pass against the current wall clock.
    pub fn collect_garbage(&self, policy: RetentionPolicy) -> Result<GcOutcome> {
        self.collect_garbage_at(physical_now_ms(), policy)
    }

    /// Run one retention pass as though it were `now_ms`.
    ///
    /// Taking the time as a parameter is what makes retention testable at all:
    /// the alternative is a test that sleeps for the retention window.
    pub fn collect_garbage_at(&self, now_ms: u64, policy: RetentionPolicy) -> Result<GcOutcome> {
        // Named, so a transaction this pass holds too long is reported
        // under it (ADR-151).
        let _span = tracing::info_span!("storage.retention").entered();
        let started = std::time::Instant::now();
        let tombstone_cutoff = cutoff(now_ms, policy.tombstone_secs);
        let outcome = GcOutcome {
            oplog_removed: self.collect_oplog(cutoff(now_ms, policy.oplog_secs))?,
            tombstones_removed: self.collect_tombstones(tombstone_cutoff)?
                + self.collect_dropped_collections(tombstone_cutoff)?
                + self.collect_dropped_indexes(tombstone_cutoff)?,
        };

        // Always, not only when something was removed: a pass that collects
        // nothing still reads, and how long it took is the one thing an
        // operator reading the log for a slow member needs to see.
        debug!(
            oplog = outcome.oplog_removed,
            tombstones = outcome.tombstones_removed,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "retention pass"
        );
        Ok(outcome)
    }

    /// Drop oplog entries older than `cutoff`, **except the newest**.
    ///
    /// The oplog is keyed by stamp, so the expired prefix is a key range and
    /// nothing past it is read. It is scanned under a read transaction and
    /// removed in chunks of [`OPLOG_COLLECT_CHUNK`], each its own commit, so
    /// the single writer is held for one chunk's removal at a time and never
    /// for the scan (ADR-151).
    fn collect_oplog(&self, cutoff: Hlc) -> Result<usize> {
        // Resolved before anything is removed so the comparison has a plain
        // value; the tail cannot move below itself while this runs.
        let Some(newest) = self.oplog_tail()? else {
            return Ok(0);
        };
        // Every key below this bound decodes to a stamp older than `cutoff`.
        let bound = codec::oplog_key_lower_bound(cutoff);

        let mut removed = 0usize;
        loop {
            let expired = {
                let txn = self.db().begin_read()?;
                let oplog = txn.open_table(tables::OPLOG)?;
                let mut keys: Vec<Vec<u8>> = Vec::new();
                for row in oplog.range::<&[u8]>(..bound.as_slice())? {
                    let (key, _) = row?;
                    let key = key.value();
                    let Ok(stamp) = codec::decode_oplog_key(key) else {
                        // An entry whose key will not decode cannot be aged,
                        // so it is kept rather than silently dropped. Keeping
                        // unreadable data is recoverable; deleting it is not.
                        warn!("undecodable oplog key retained");
                        continue;
                    };
                    // The tail is load-bearing: it is where the logical clock
                    // resumes from. See this module's documentation.
                    if stamp == newest {
                        continue;
                    }
                    keys.push(key.to_vec());
                    if keys.len() >= OPLOG_COLLECT_CHUNK {
                        break;
                    }
                }
                keys
            };
            if expired.is_empty() {
                break;
            }
            let full = expired.len() >= OPLOG_COLLECT_CHUNK;
            removed += self.remove_oplog_entries(&expired)?;
            if !full {
                break;
            }
        }
        Ok(removed)
    }

    /// Remove `keys` from the oplog and its arrival index, and record the
    /// horizon they leave behind, in one transaction.
    fn remove_oplog_entries(&self, keys: &[Vec<u8>]) -> Result<usize> {
        let txn = self.begin_write(WriterHolder::Retention)?;
        let mut removed = 0usize;
        {
            let mut oplog = txn.open_table(tables::OPLOG)?;
            let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
            let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;

            let mut highest = None;
            // The same high-water mark per origin: the coarse horizon says a
            // peer below it may lack something collected, this says at which
            // origins it actually does (ADR-097).
            let mut per_origin = kimmy_core::VersionVector::new();
            for key in keys {
                // Collected in one transaction with the entries themselves,
                // so the arrival index can never point at something that is
                // gone. A stream reading it mid-collection sees either state,
                // never a mixture.
                if oplog.remove(key.as_slice())?.is_none() {
                    continue;
                }
                removed += 1;
                if let Ok(stamp) = codec::decode_oplog_key(key) {
                    highest = Some(highest.map_or(stamp.hlc, |h: Hlc| h.max(stamp.hlc)));
                    per_origin.observe(stamp);
                }
                if let Some(seq) = by_stamp.remove(key.as_slice())? {
                    arrival.remove(seq.value())?;
                }
            }

            // Recording the horizon is what lets a peer be told it needs a
            // snapshot rather than being served a silent gap. Both records in
            // the same transaction as the removal, so neither can claim more
            // or less was collected than actually was.
            if let Some(highest) = highest {
                let mut meta = txn.open_table(tables::META)?;
                let previous = match meta.get(tables::META_OPLOG_COLLECTED_THROUGH)? {
                    Some(raw) => codec::decode_oplog_key(raw.value()).ok().map(|s| s.hlc),
                    None => None,
                };
                if previous.is_none_or(|p| highest > p) {
                    let marker = kimmy_core::Stamp::new(highest, self.node_id());
                    meta.insert(
                        tables::META_OPLOG_COLLECTED_THROUGH,
                        codec::oplog_key(&marker).as_slice(),
                    )?;
                }

                for (node, hlc) in per_origin.iter() {
                    let stamp = kimmy_core::Stamp::new(hlc, node);
                    crate::engine::raise_version(&txn, tables::OPLOG_COLLECTED, &stamp)?;
                }
            }
        }
        txn.commit()?;
        Ok(removed)
    }

    /// Drop tombstones older than `cutoff`.
    ///
    /// Only tombstones: a live record is data, however old. Index entries were
    /// already removed when the delete was applied, so nothing else refers to
    /// the key being dropped.
    ///
    /// A tombstone is a document record with the deleted flag, in the one
    /// table with every live document, and nothing indexes it by age; finding
    /// them is a walk. So the walk is done under a read transaction, which
    /// holds no writer, visits at most [`TOMBSTONE_SCAN_BUDGET`] documents per
    /// pass, and resumes next pass where it stopped; what it finds is removed
    /// in chunks of [`TOMBSTONE_COLLECT_CHUNK`], each its own short commit
    /// (ADR-151). A tombstone the scan saw is removed only if it is still the
    /// same tombstone when the writer is held: a document re-created at that
    /// key in between is data.
    fn collect_tombstones(&self, cutoff: Hlc) -> Result<usize> {
        self.collect_tombstones_within(cutoff, TOMBSTONE_SCAN_BUDGET)
    }

    /// [`Self::collect_tombstones`] with the scan budget as a parameter, so
    /// a test can see a scan stop and resume without a table of a hundred
    /// thousand documents.
    fn collect_tombstones_within(&self, cutoff: Hlc, budget: usize) -> Result<usize> {
        let mut cursor = self.gc_scan_cursor();
        let mut visited = 0usize;
        let mut removed = 0usize;
        loop {
            let (expired, last, exhausted, seen) = {
                let txn = self.db().begin_read()?;
                let docs = txn.open_table(tables::DOCS)?;
                let start = match &cursor {
                    Some((collection, key)) => {
                        std::ops::Bound::Excluded((*collection, key.as_slice()))
                    }
                    None => std::ops::Bound::Unbounded,
                };
                let mut expired: Vec<ExpiredTombstone> = Vec::new();
                let mut last = None;
                let mut seen = 0usize;
                let mut exhausted = true;
                for row in docs.range::<(u64, &[u8])>((start, std::ops::Bound::Unbounded))? {
                    let (key, value) = row?;
                    let (collection, doc_key) = key.value();
                    last = Some((collection, doc_key.to_vec()));
                    seen += 1;
                    match codec::decode_doc_record(value.value()) {
                        Ok(record) if record.deleted && record.stamp.hlc < cutoff => {
                            expired.push(ExpiredTombstone {
                                collection,
                                key: doc_key.to_vec(),
                                stamp: record.stamp,
                            });
                        }
                        Ok(_) => {}
                        Err(_) => warn!("undecodable document record retained"),
                    }
                    if expired.len() >= TOMBSTONE_COLLECT_CHUNK || visited + seen >= budget {
                        exhausted = false;
                        break;
                    }
                }
                (expired, last, exhausted, seen)
            };
            visited += seen;
            removed += self.remove_tombstones(&expired, cutoff)?;
            if exhausted {
                // The whole table has been walked: the next pass starts over.
                cursor = None;
                break;
            }
            cursor = last;
            if visited >= budget {
                break;
            }
        }
        self.set_gc_scan_cursor(cursor);
        Ok(removed)
    }

    /// Remove the tombstones a scan found, re-checking each under the writer.
    fn remove_tombstones(&self, expired: &[ExpiredTombstone], cutoff: Hlc) -> Result<usize> {
        // Nothing to do, and no transaction: a scan that found nothing must
        // not cost a commit.
        if expired.is_empty() {
            return Ok(0);
        }
        let txn = self.begin_write(WriterHolder::Retention)?;
        let mut removed = 0usize;
        {
            let mut docs = txn.open_table(tables::DOCS)?;
            for tombstone in expired {
                let key = (tombstone.collection, tombstone.key.as_slice());
                let unchanged = match docs.get(key)? {
                    Some(value) => match codec::decode_doc_record(value.value()) {
                        Ok(record) => {
                            record.deleted
                                && record.stamp == tombstone.stamp
                                && record.stamp.hlc < cutoff
                        }
                        Err(_) => false,
                    },
                    None => false,
                };
                if unchanged && docs.remove(key)?.is_some() {
                    removed += 1;
                }
            }
        }
        txn.commit()?;
        Ok(removed)
    }

    /// Drop collection tombstones older than `cutoff`.
    ///
    /// Counted alongside document tombstones because they answer the same
    /// question over the same window: "was this deleted more recently than the
    /// thing a peer is trying to replay?"
    fn collect_dropped_collections(&self, cutoff: Hlc) -> Result<usize> {
        // Found under a read transaction, removed under the writer only when
        // there is something to remove, so a pass with nothing to collect
        // never takes the writer (ADR-151). One row per dropped collection, so the
        // table is walked whole.
        let expired = {
            let txn = self.db().begin_read()?;
            let dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            let mut expired = Vec::new();
            for row in dropped.iter()? {
                let (key, value) = row?;
                let Ok(stamp) = codec::decode_oplog_key(value.value()) else {
                    warn!("undecodable collection tombstone retained");
                    continue;
                };
                if stamp.hlc < cutoff {
                    expired.push((key.value(), value.value().to_vec()));
                }
            }
            expired
        };
        if expired.is_empty() {
            return Ok(0);
        }

        let txn = self.begin_write(WriterHolder::Retention)?;
        let mut removed = 0usize;
        {
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            for (key, seen) in &expired {
                // Still the tombstone the scan saw: a drop recorded again
                // since then carries a newer stamp and is kept.
                let unchanged = dropped.get(*key)?.is_some_and(|v| v.value() == seen);
                if unchanged && dropped.remove(*key)?.is_some() {
                    removed += 1;
                }
            }
        }
        txn.commit()?;
        Ok(removed)
    }

    /// Drop index tombstones older than `cutoff`.
    ///
    /// Counted with the other tombstones for the reason collection tombstones
    /// are: the question is the same — "was this dropped more recently than
    /// the creation a peer is replaying?" — and so is the window in which the
    /// answer can be trusted (ADR-123).
    fn collect_dropped_indexes(&self, cutoff: Hlc) -> Result<usize> {
        // Found under a read transaction, removed under the writer only when
        // there is something to remove, so a pass with nothing to collect
        // never takes the writer (ADR-151). One row per dropped index, so the
        // table is walked whole.
        let expired = {
            let txn = self.db().begin_read()?;
            let dropped = txn.open_table(tables::INDEXES_DROPPED)?;
            let mut expired = Vec::new();
            for row in dropped.iter()? {
                let (key, value) = row?;
                let Ok(stamp) = codec::decode_oplog_key(value.value()) else {
                    warn!("undecodable index tombstone retained");
                    continue;
                };
                if stamp.hlc < cutoff {
                    expired.push((key.value(), value.value().to_vec()));
                }
            }
            expired
        };
        if expired.is_empty() {
            return Ok(0);
        }

        let txn = self.begin_write(WriterHolder::Retention)?;
        let mut removed = 0usize;
        {
            let mut dropped = txn.open_table(tables::INDEXES_DROPPED)?;
            for (key, seen) in &expired {
                // Still the tombstone the scan saw: a drop recorded again
                // since then carries a newer stamp and is kept.
                let unchanged = dropped.get(*key)?.is_some_and(|v| v.value() == seen);
                if unchanged && dropped.remove(*key)?.is_some() {
                    removed += 1;
                }
            }
        }
        txn.commit()?;
        Ok(removed)
    }

    /// The newest stamp in the oplog.
    fn oplog_tail(&self) -> Result<Option<kimmy_core::Stamp>> {
        let txn = self.db().begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        match oplog.last()? {
            Some((key, _)) => Ok(Some(codec::decode_oplog_key(key.value())?)),
            None => Ok(None),
        }
    }
}

/// The timestamp below which a record of this age is expired.
///
/// Saturating, so a retention window longer than the clock's age keeps
/// everything instead of wrapping into a cutoff in the far future — which would
/// collect the entire database.
fn cutoff(now_ms: u64, retention_secs: u64) -> Hlc {
    let age_ms = retention_secs.saturating_mul(1000);
    Hlc::new(now_ms.saturating_sub(age_ms), 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use kimmy_core::DocId;

    const HOUR_MS: u64 = 60 * 60 * 1000;
    const DAY: u64 = 24 * 60 * 60;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        (engine, dir)
    }

    fn policy() -> RetentionPolicy {
        RetentionPolicy::new(DAY, DAY)
    }

    /// A time far enough ahead that everything written "now" is expired.
    fn much_later() -> u64 {
        physical_now_ms() + 365 * 24 * HOUR_MS
    }

    fn oplog_len(engine: &Engine) -> usize {
        let txn = engine.db().begin_read().unwrap();
        let oplog = txn.open_table(tables::OPLOG).unwrap();
        oplog.iter().unwrap().count()
    }

    #[test]
    fn nothing_is_collected_before_its_time() {
        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        engine.insert(&meta, doc! { "_id": "a", "v": 1 }).unwrap();
        engine.delete(&meta, &DocId::String("a".into())).unwrap();

        let before = oplog_len(&engine);
        let outcome = engine.collect_garbage(policy()).unwrap();

        assert_eq!(outcome, GcOutcome::default(), "fresh records must survive");
        assert_eq!(oplog_len(&engine), before);
    }

    #[test]
    fn expired_tombstones_are_collected() {
        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        engine.insert(&meta, doc! { "_id": "a" }).unwrap();
        engine.insert(&meta, doc! { "_id": "b" }).unwrap();
        engine.delete(&meta, &DocId::String("a".into())).unwrap();

        let outcome = engine.collect_garbage_at(much_later(), policy()).unwrap();

        assert_eq!(outcome.tombstones_removed, 1);
        // The live document is untouched, however old it is.
        assert!(engine.get(&meta, &DocId::String("b".into())).unwrap().is_some());
        assert!(engine.get(&meta, &DocId::String("a".into())).unwrap().is_none());
    }

    #[test]
    fn an_index_tombstone_is_collected_on_the_tombstone_window() {
        // Same window as a document's or a collection's tombstone: the
        // creation it exists to outrank ages out of the oplog under
        // `oplog_retention_secs`, and the tombstone has to outlive that.
        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        engine
            .create_index("db", "c", vec![kimmy_core::IndexField::ascending("a")], false, None)
            .unwrap();
        engine.drop_index("db", "c", "a_1").unwrap();
        let index_id = kimmy_core::IndexMeta::derive_id("a_1");
        assert!(engine.index_dropped_at(meta.id, index_id).unwrap().is_some());

        let early = engine.collect_garbage(policy()).unwrap();
        assert_eq!(early.tombstones_removed, 0, "not before its time");
        assert!(engine.index_dropped_at(meta.id, index_id).unwrap().is_some());

        let outcome = engine.collect_garbage_at(much_later(), policy()).unwrap();
        assert_eq!(outcome.tombstones_removed, 1, "{outcome:?}");
        assert!(
            engine.index_dropped_at(meta.id, index_id).unwrap().is_none(),
            "an expired index tombstone must be collected"
        );
    }

    #[test]
    fn a_live_record_is_never_collected_however_old() {
        // Age expires tombstones, not data. Getting this wrong would delete the
        // database.
        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        for i in 0..5 {
            engine.insert(&meta, doc! { "_id": i, "v": i }).unwrap();
        }

        engine.collect_garbage_at(much_later(), policy()).unwrap();

        for i in 0..5 {
            assert!(
                engine.get(&meta, &DocId::Int64(i)).unwrap().is_some(),
                "live document {i} was collected"
            );
        }
    }

    #[test]
    fn the_newest_oplog_entry_survives_collection() {
        // The clock resumes from the oplog tail. Collecting it would reset the
        // clock to zero on the next restart, and every write afterwards would
        // lose to its own older version. See this module's documentation.
        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        for i in 0..10 {
            engine.insert(&meta, doc! { "_id": i }).unwrap();
        }

        engine.collect_garbage_at(much_later(), policy()).unwrap();

        assert_eq!(oplog_len(&engine), 1, "exactly the tail should remain");
    }

    #[test]
    fn the_clock_still_resumes_after_an_aggressive_collection() {
        // The property the previous test protects, stated end to end: a restart
        // after collection must not mint stamps below what is already stored.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let highest = {
            let engine = Engine::open(&path).unwrap();
            let meta = engine.create_collection("db", "c").unwrap();
            for i in 0..10 {
                engine.insert(&meta, doc! { "_id": i }).unwrap();
            }
            // Retention of zero: collect everything collectable.
            engine.collect_garbage_at(much_later(), RetentionPolicy::new(0, 0)).unwrap();
            engine.oplog_tail().unwrap().unwrap().hlc
        };

        let reopened = Engine::open(&path).unwrap();
        let meta = reopened.get_collection("db", "c").unwrap();
        reopened.insert(&meta, doc! { "_id": "after-restart" }).unwrap();

        let next = reopened.oplog_tail().unwrap().unwrap().hlc;
        assert!(
            next > highest,
            "a post-restart write must stamp above the retained tail: {next} <= {highest}"
        );
    }

    #[test]
    fn collecting_an_empty_oplog_is_harmless() {
        let (engine, _dir) = engine();
        assert_eq!(
            engine.collect_garbage_at(much_later(), policy()).unwrap(),
            GcOutcome::default()
        );
    }

    #[test]
    fn collection_is_idempotent() {
        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        engine.insert(&meta, doc! { "_id": "a" }).unwrap();
        engine.delete(&meta, &DocId::String("a".into())).unwrap();

        let first = engine.collect_garbage_at(much_later(), policy()).unwrap();
        let second = engine.collect_garbage_at(much_later(), policy()).unwrap();

        assert!(!first.is_empty());
        assert!(second.is_empty(), "a second pass had nothing left to do: {second:?}");
    }

    #[test]
    fn a_collected_resume_token_is_reported_as_expired() {
        // The contract change streams already document: a token pointing into a
        // collected prefix is 410, not a silent gap.
        use crate::watch::{WatchOptions, WatchScope};

        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        engine.insert(&meta, doc! { "_id": "first" }).unwrap();

        let early = engine.oplog_tail().unwrap().unwrap();

        for i in 0..5 {
            engine.insert(&meta, doc! { "_id": i }).unwrap();
        }
        engine.collect_garbage_at(much_later(), RetentionPolicy::new(0, DAY)).unwrap();

        let token = kimmy_core::ResumeToken::from_stamp(early);
        let err = engine
            .watch(
                WatchScope::Collection(meta.id),
                WatchOptions { resume_after: Some(token), ..Default::default() },
            )
            .err()
            .expect("a token pointing into a collected prefix must be refused");

        assert!(
            matches!(err, crate::StorageError::Core(kimmy_core::Error::ResumeTokenExpired)),
            "expected an expired token, got {err:?}"
        );
    }

    #[test]
    fn a_retention_window_longer_than_the_clock_collects_nothing() {
        // saturating_sub, not wrapping: a cutoff that wrapped into the far
        // future would collect the entire database.
        let far = cutoff(1_000, u64::MAX);
        assert_eq!(far, Hlc::new(0, 0));
    }

    #[test]
    fn the_cutoff_is_the_retention_window_behind_now() {
        assert_eq!(cutoff(10 * HOUR_MS, DAY), Hlc::new(0, 0));
        assert_eq!(cutoff(48 * HOUR_MS, DAY), Hlc::new(24 * HOUR_MS, 0));
    }

    /// Two engines with entries from both origins on `a`, `b` having written
    /// last so its final entry is the tail `a` keeps.
    fn two_origins() -> (Engine, Engine, tempfile::TempDir, tempfile::TempDir) {
        let (a, da) = engine();
        let (b, db) = engine();
        let ca = a.create_collection("db", "c").unwrap();
        a.insert(&ca, doc! { "_id": "a-1" }).unwrap();
        a.insert(&ca, doc! { "_id": "a-2" }).unwrap();
        for entry in a.entries_for_peer(Hlc::ZERO, usize::MAX).unwrap().entries {
            b.apply_batch(&[entry]).unwrap();
        }
        let cb = b.get_collection("db", "c").unwrap();
        b.insert(&cb, doc! { "_id": "b-1" }).unwrap();
        b.insert(&cb, doc! { "_id": "b-2" }).unwrap();
        let start = a.version_vector().unwrap().get(b.node_id());
        let entries = b.entries_for_peer(start, usize::MAX).unwrap().entries;
        a.apply_batch(&entries).unwrap();
        (a, b, da, db)
    }

    #[test]
    fn collection_is_recorded_per_origin_as_well_as_overall() {
        let (a, b, _da, _db) = two_origins();
        let before = a.version_vector().unwrap();

        a.collect_garbage_at(much_later(), policy()).unwrap();

        let collected = a.oplog_collected().unwrap();
        // Everything of A's went; A's record is its newest.
        assert_eq!(collected.get(a.node_id()), before.get(a.node_id()));
        // B's last entry is the tail and survives, so B's record is the one
        // before it — strictly below what A holds of B.
        let b_recorded = collected.get(b.node_id());
        assert!(b_recorded > Hlc::ZERO && b_recorded < before.get(b.node_id()), "{collected:?}");
        // The coarse horizon is the highest of the per-origin records: the two
        // are one fact at two grains, written in one transaction.
        assert_eq!(
            a.oplog_collected_through().unwrap(),
            collected.iter().map(|(_, hlc)| hlc).max().unwrap()
        );
        // Nothing collected on B, so nothing recorded.
        assert!(b.oplog_collected().unwrap().is_empty());
    }

    #[test]
    fn a_database_an_earlier_build_collected_from_is_seeded_with_the_coarse_horizon() {
        // An earlier build moved the horizon without keeping the per-origin
        // record. Reopened by this one, every origin held is given the
        // horizon: coarse below it, as before; exact above it, from now on.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let node;
        let horizon;
        let held;
        {
            let a = Engine::open(&path).unwrap();
            let ca = a.create_collection("db", "c").unwrap();
            a.insert(&ca, doc! { "_id": "a-1" }).unwrap();
            a.insert(&ca, doc! { "_id": "a-2" }).unwrap();
            a.collect_garbage_at(much_later(), policy()).unwrap();
            node = a.node_id();
            horizon = a.oplog_collected_through().unwrap();
            held = a.version_vector().unwrap();
            assert!(horizon > Hlc::ZERO);

            // What the earlier build leaves behind: the horizon, no record.
            let db = a.db();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::OPLOG_COLLECTED).unwrap().retain(|_, _| false).unwrap();
            txn.commit().unwrap();
            assert!(a.oplog_collected().unwrap().is_empty());
        }

        let reopened = Engine::open(&path).unwrap();
        let collected = reopened.oplog_collected().unwrap();
        assert_eq!(collected.len(), held.len(), "every held origin is seeded");
        assert_eq!(collected.get(node), horizon);

        // Reopening a database whose record already vouches for the horizon
        // leaves it alone.
        drop(reopened);
        let again = Engine::open(&path).unwrap();
        assert_eq!(again.oplog_collected().unwrap(), collected);
    }

    #[test]
    fn nothing_is_seeded_where_nothing_was_ever_collected() {
        // The opposite corner: a fresh database has no horizon, so a peer
        // holding nothing must still be served from the oplog rather than
        // handed a snapshot it does not need.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let a = Engine::open(&path).unwrap();
            let ca = a.create_collection("db", "c").unwrap();
            a.insert(&ca, doc! { "_id": "a-1" }).unwrap();
        }
        let reopened = Engine::open(&path).unwrap();
        assert!(reopened.oplog_collected().unwrap().is_empty());
        assert!(reopened.can_serve_peer_holding(&kimmy_core::VersionVector::default()).unwrap());
    }

    /// The expired prefix is removed a chunk per commit, and the writer is
    /// released between chunks (ADR-151): a day of entries costs as many
    /// short transactions as it takes, never one long one.
    #[test]
    fn an_expired_oplog_is_collected_a_chunk_per_commit() {
        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        let batches = 3;
        for batch in 0..batches {
            let docs = (0..OPLOG_COLLECT_CHUNK)
                .map(|i| doc! { "_id": (batch * OPLOG_COLLECT_CHUNK + i) as i64 })
                .collect();
            engine.insert_many(&meta, docs).unwrap();
        }
        let entries = oplog_len(&engine);
        assert!(entries > batches * OPLOG_COLLECT_CHUNK, "{entries} entries to collect");
        let before = engine.commits();

        let outcome = engine.collect_garbage_at(much_later(), policy()).unwrap();

        assert_eq!(outcome.oplog_removed, entries - 1, "everything but the tail");
        assert_eq!(oplog_len(&engine), 1);
        let commits = (engine.commits() - before) as usize;
        let chunks = (entries - 1).div_ceil(OPLOG_COLLECT_CHUNK);
        assert_eq!(commits, chunks, "one commit per chunk of {OPLOG_COLLECT_CHUNK}");
    }

    /// A pass that collects nothing opens no write transaction at all: the
    /// scans run under readers, and the writer is taken only to remove.
    #[test]
    fn a_pass_with_nothing_to_collect_holds_the_writer_for_nothing() {
        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        for i in 0..50 {
            engine.insert(&meta, doc! { "_id": i }).unwrap();
        }
        let before = engine.commits();
        let waits = engine.writer_wait().count;

        let outcome = engine.collect_garbage(policy()).unwrap();

        assert_eq!(outcome, GcOutcome::default());
        assert_eq!(engine.commits(), before, "a no-op pass must not commit");
        assert_eq!(engine.writer_wait().count, waits, "a no-op pass must not take the writer");
    }

    /// The tombstone scan visits a bounded number of documents per pass and
    /// resumes where it stopped, so every expired tombstone is collected
    /// within a few passes without any one pass walking the whole table.
    #[test]
    fn the_tombstone_scan_resumes_where_the_budget_stopped_it() {
        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        for i in 0..10 {
            engine.insert(&meta, doc! { "_id": i }).unwrap();
        }
        // Tombstones spread through the key range, including the last key.
        for i in [1, 4, 9] {
            engine.delete(&meta, &DocId::Int64(i)).unwrap();
        }
        let cutoff = cutoff(much_later(), 0);

        let mut removed = 0;
        let mut passes = 0;
        loop {
            removed += engine.collect_tombstones_within(cutoff, 4).unwrap();
            passes += 1;
            if engine.gc_scan_cursor().is_none() {
                break;
            }
            assert!(passes < 10, "the scan never reached the end of the table");
        }
        assert_eq!(removed, 3, "every expired tombstone, across passes");
        assert_eq!(passes, 3, "ten documents at four per pass");
        for i in 0..10 {
            let held = engine.get(&meta, &DocId::Int64(i)).unwrap().is_some();
            assert_eq!(held, ![1, 4, 9].contains(&i), "document {i}");
        }
    }

    /// A tombstone the scan saw is removed only if it is still the same
    /// record when the writer is held: a document re-created at that key in
    /// between is data, and a key that changed since the scan is left for
    /// the next pass to look at again.
    #[test]
    fn a_tombstone_that_changed_since_the_scan_is_kept() {
        let (engine, _dir) = engine();
        let meta = engine.create_collection("db", "c").unwrap();
        engine.insert(&meta, doc! { "_id": "a" }).unwrap();
        engine.delete(&meta, &DocId::String("a".into())).unwrap();
        let stale = {
            let txn = engine.db().begin_read().unwrap();
            let docs = txn.open_table(tables::DOCS).unwrap();
            let key = crate::docs::doc_key(&DocId::String("a".into())).unwrap();
            let raw = docs.get((meta.id.0, key.as_slice())).unwrap().unwrap();
            let record = codec::decode_doc_record(raw.value()).unwrap();
            assert!(record.deleted);
            ExpiredTombstone { collection: meta.id.0, key, stamp: record.stamp }
        };
        // Re-created after the scan: the key now holds a live document with
        // a newer stamp.
        engine.insert(&meta, doc! { "_id": "a", "back": true }).unwrap();

        let removed = engine.remove_tombstones(&[stale], cutoff(much_later(), 0)).unwrap();

        assert_eq!(removed, 0);
        assert!(engine.get(&meta, &DocId::String("a".into())).unwrap().is_some(), "data kept");
    }
}
