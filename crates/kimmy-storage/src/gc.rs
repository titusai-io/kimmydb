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
use crate::engine::{Engine, physical_now_ms};
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
        let outcome = GcOutcome {
            oplog_removed: self.collect_oplog(cutoff(now_ms, policy.oplog_secs))?,
            tombstones_removed: self.collect_tombstones(cutoff(now_ms, policy.tombstone_secs))?,
        };

        if !outcome.is_empty() {
            debug!(
                oplog = outcome.oplog_removed,
                tombstones = outcome.tombstones_removed,
                "collected expired records"
            );
        }
        Ok(outcome)
    }

    /// Drop oplog entries older than `cutoff`, **except the newest**.
    fn collect_oplog(&self, cutoff: Hlc) -> Result<usize> {
        // Resolved before the write transaction so the retain closure — which
        // cannot fail — has a plain value to compare against.
        let Some(newest) = self.oplog_tail()? else {
            return Ok(0);
        };

        let txn = self.db().begin_write()?;
        let mut removed = 0usize;
        {
            let mut oplog = txn.open_table(tables::OPLOG)?;
            oplog.retain(|key, _| {
                let Ok(stamp) = codec::decode_oplog_key(key) else {
                    // An entry whose key will not decode cannot be aged, so it
                    // is kept rather than silently dropped. Keeping unreadable
                    // data is recoverable; deleting it is not.
                    warn!("undecodable oplog key retained");
                    return true;
                };
                // The tail is load-bearing: it is where the logical clock
                // resumes from. See this module's documentation.
                if stamp == newest {
                    return true;
                }
                let expired = stamp.hlc < cutoff;
                if expired {
                    removed += 1;
                }
                !expired
            })?;
        }
        txn.commit()?;
        Ok(removed)
    }

    /// Drop tombstones older than `cutoff`.
    ///
    /// Only tombstones: a live record is data, however old. Index entries were
    /// already removed when the delete was applied, so nothing else refers to
    /// the key being dropped.
    fn collect_tombstones(&self, cutoff: Hlc) -> Result<usize> {
        let txn = self.db().begin_write()?;
        let mut removed = 0usize;
        {
            let mut docs = txn.open_table(tables::DOCS)?;
            docs.retain(|_, value| {
                let Ok(record) = codec::decode_doc_record(value) else {
                    warn!("undecodable document record retained");
                    return true;
                };
                let expired = record.deleted && record.stamp.hlc < cutoff;
                if expired {
                    removed += 1;
                }
                !expired
            })?;
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
}
