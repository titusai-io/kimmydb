//! The records a replay page carries (ADR-212's replay of a restored member's
//! own origin, B5): B2 of the build review, the records of the page's
//! collections whose request spans the page, and m1's records cursor, which pages
//! records too many for one frame without moving the replay's position.
//!
//! **Why B2.** A replay page's own advance (the rows of the requester's origin
//! inside it) and the records its sender cannot vouch for by its oplog (F3) are
//! not every record a page's entries need. A request whose attempts all ended on
//! other members merges into a record with no row of the requester's origin, and
//! a member that holds the requester's entries of that request and the record
//! ships the entries back without it: the restored member then holds its own
//! effects bare, and a retry there applies again.
//!
//! **The rule.** Every record of a collection a page's entries name whose `last`
//! is above the page's start and whose `first` is at or below its end, live by
//! the window alone, carried at its `first` (the F1 row). A request's effects
//! lie between its `first` and `last`, so every record with an effect on the page
//! is among these. Carried at `first`, a record rides with the first entry of
//! its collection the requester takes, so a batch that stops between an effect
//! and `last` stores it all the same, as B1's lowest row does for a window.

use std::collections::HashSet;

use kimmy_core::{CollectionId, Hlc, ScopeHash, Stamp};
use redb::{ReadTransaction, ReadableDatabase};
use tracing::info;

use super::{
    CarriedRecord, DeltaBudget, IdempotencyRecord, RECORD_DELTA_ROWS, changes_begin,
    changes_high_water, collection_records, records_changed_since,
};
use crate::engine::Engine;
use crate::error::Result;
use crate::snapshot::RecordsBase;
use crate::tables::IDEMPOTENCY;

/// Where a replay's pages of records alone stand at one replay position (m1 of
/// the build review): what the next page at that position goes on after.
///
/// A replay page whose entries cannot cross with the records they need (more
/// than a frame of them, all riding its first entry) carries records alone, in
/// key order, as many as fit, and names the key of the last one here. The
/// replay's position does not move: a page at the same position carries the
/// records after that key, and once what is left fits with its entries, the
/// entries resume and the cursor ends.
///
/// **What it pages is fixed when it is cut**: B2's records of the collections the
/// page's entries named (`collections`) through the page's end (`through`). Every
/// page at the position reads that set again and carries entries no further
/// than `through`, so a record an entry of a later page there needs is in it,
/// whatever the walk of that page reaches. A record outside the set is never
/// held back.
///
/// **The base guards it**, as a snapshot cursor's `records_base` guards its
/// records delta: the sender's change log when the first page of records alone
/// was cut, `{key, epoch}`. Every later page at the position also carries every
/// record the log names above the base, so a record that changed or arrived
/// since is sent again wherever its key lies. The key is compared only within
/// one epoch of the log: a cursor of another epoch (a restore, an identity
/// reset) starts the position's records over, since what the store held before
/// it is unknown.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplayRecords {
    /// The replay position, the page's start, this cursor belongs to: a page
    /// from anywhere else ignores it.
    pub at: Hlc,
    /// The end of the page that cut the cursor: pages at the position carry no
    /// entry past it.
    pub through: Hlc,
    /// The collections that page's entries named.
    pub collections: Vec<CollectionId>,
    /// The key of the last record sent.
    pub collection: CollectionId,
    #[serde(with = "super::scope_bytes")]
    pub scope: ScopeHash,
    /// The sender's change log when the cursor was cut, or where its last delta
    /// stopped.
    pub base: RecordsBase,
}

impl ReplayRecords {
    /// Whether the key `(collection, scope)` is at or before this cursor's.
    pub fn passed(&self, collection: CollectionId, scope: &ScopeHash) -> bool {
        (collection, scope.as_bytes()) <= (self.collection, self.scope.as_bytes())
    }
}

/// What one replay page's records read came to
/// ([`Engine::replay_page_records`]).
#[derive(Debug)]
pub struct ReplayPageRecords {
    /// B2's records ([`Engine::records_for_replay_page`]), in key order, less
    /// those in `passed`: under an honoured cursor, of the cursor's collections
    /// as well as the page's, through the cursor's end.
    pub records: Vec<CarriedRecord>,
    /// Under an honoured cursor: every live record of those collections the
    /// change log names above the cursor's base, carried at its `first`. Empty
    /// otherwise.
    pub changed: Vec<CarriedRecord>,
    /// Under an honoured cursor: the keys of the cursor's set at or before its
    /// key that have not changed since its base, which an earlier page at the
    /// position sent. A page does not send them again.
    pub passed: Vec<(CollectionId, ScopeHash)>,
    /// Whether the cursor that came is honoured: of this page's position and of
    /// this log's epoch.
    pub resumed: bool,
    /// The base a cursor cut by this page carries: the change log's high water as
    /// this read saw it (read before the records, so a change committed after is
    /// above it), or where `changed` stopped.
    pub base: RecordsBase,
    /// Whether `changed` holds everything the log names above the cursor's base:
    /// when it does not, the page carries records alone.
    pub changed_whole: bool,
}

impl Engine {
    /// Remove the record under a key and nothing else, its rows left as a
    /// retention pass stopped between chunks leaves them: for a test of what an
    /// orphan row ships. Whether there was one.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn remove_idempotency_record_for_test(
        &self,
        collection: CollectionId,
        scope: &ScopeHash,
    ) -> Result<bool> {
        let txn = self.db().begin_write()?;
        let removed = txn
            .open_table(IDEMPOTENCY)?
            .remove((collection.0, scope.as_bytes().as_slice()))?
            .is_some();
        txn.commit()?;
        Ok(removed)
    }

    /// B2's records of a replay page whose entries name `collections`, from
    /// `from` (exclusive, the page's start) through `through` (its end): every
    /// record of those collections with `last` above `from` and `first` at or
    /// below `through`, live by the window alone (the life rule is the
    /// receiver's), carried at its `first`, in key order. A record this build
    /// cannot decode carries nothing here. One read of each collection's records.
    pub fn records_for_replay_page(
        &self,
        collections: &[CollectionId],
        from: Hlc,
        through: Hlc,
    ) -> Result<Vec<CarriedRecord>> {
        let txn = self.db().begin_read()?;
        self.replay_records_in(&txn, collections, from, through)
    }

    fn replay_records_in(
        &self,
        txn: &ReadTransaction,
        collections: &[CollectionId],
        from: Hlc,
        through: Hlc,
    ) -> Result<Vec<CarriedRecord>> {
        let records = txn.open_table(IDEMPOTENCY)?;
        let mut ids = collections.to_vec();
        ids.sort();
        ids.dedup();
        let mut out = Vec::new();
        for id in ids {
            for row in records.range(collection_records(id, None))? {
                let (key, value) = row?;
                let (_, scope) = key.value();
                let Some(scope) = ScopeHash::from_slice(scope) else { continue };
                let Ok(Some(record)) = IdempotencyRecord::decode(value.value()) else {
                    continue;
                };
                if record.last.hlc <= from
                    || record.first.hlc > through
                    || self.record_is_past_window(&record)
                {
                    continue;
                }
                out.push(CarriedRecord {
                    collection: id,
                    scope,
                    carrying: Some(record.first),
                    record: value.value().to_vec(),
                });
            }
        }
        Ok(out)
    }

    /// One replay page's records read, in one read transaction: B2's records
    /// ([`Self::records_for_replay_page`]), and with a `cursor` of this page's
    /// position and of this log's epoch ([`ReplayRecords`]), its set read again
    /// with them, the records the change log names above its base (at most
    /// `changed_bytes` of them), and the keys earlier pages at the position sent.
    /// The base a cursor this page cuts carries is read first.
    pub fn replay_page_records(
        &self,
        collections: &[CollectionId],
        from: Hlc,
        through: Hlc,
        cursor: Option<&ReplayRecords>,
        changed_bytes: usize,
    ) -> Result<ReplayPageRecords> {
        let txn = self.db().begin_read()?;
        // Where the change log stands, before the records are read: a change
        // committed after this read is above it, and so in a later page's delta.
        let begun = changes_begin(&txn)?.filter(|begun| begun.at.node == self.node_id());
        let cut = match begun {
            Some(begun) => {
                let high = changes_high_water(&txn)?.map_or(begun.at.hlc, |high| high.hlc);
                RecordsBase { key: Stamp::new(high, self.node_id()), epoch: Some(begun.epoch) }
            }
            None => RecordsBase { key: Stamp::new(Hlc::ZERO, self.node_id()), epoch: None },
        };
        let mut out = ReplayPageRecords {
            records: Vec::new(),
            changed: Vec::new(),
            passed: Vec::new(),
            resumed: false,
            base: cut,
            changed_whole: true,
        };
        // Within one epoch, or from the zero base of a log that had held
        // nothing: keys are compared only there (`RecordsBase`).
        let honoured = cursor.filter(|cursor| cursor.at == from).filter(|cursor| {
            let honoured = match cursor.base.epoch {
                Some(epoch) => begun.is_some_and(|begun| begun.epoch == epoch),
                None => cursor.base.key.hlc == Hlc::ZERO,
            };
            if !honoured {
                info!(
                    base = ?cursor.base,
                    epoch = ?begun.map(|begun| begun.epoch),
                    "a replay's records cursor is of another log; the position's records start \
                     over"
                );
            }
            honoured
        });
        let Some(cursor) = honoured else {
            out.records = self.replay_records_in(&txn, collections, from, through)?;
            return Ok(out);
        };
        let mut ids: Vec<CollectionId> =
            collections.iter().chain(&cursor.collections).copied().collect();
        ids.sort();
        ids.dedup();
        let records = self.replay_records_in(&txn, &ids, from, cursor.through.max(through))?;
        let named: HashSet<u64> = ids.iter().map(|id| id.0).collect();
        let standing = |id: CollectionId| named.contains(&id.0);
        let delta = records_changed_since(
            &txn,
            cursor.base.key,
            &standing,
            DeltaBudget { bytes: changed_bytes, rows: RECORD_DELTA_ROWS },
        )?;
        out.changed = delta
            .records
            .into_iter()
            .filter_map(|mut carried| {
                let record = IdempotencyRecord::decode(&carried.record).ok().flatten()?;
                if self.record_is_past_window(&record) {
                    return None;
                }
                carried.carrying = Some(record.first);
                Some(carried)
            })
            .collect();
        let changed: HashSet<(u64, [u8; 32])> =
            out.changed.iter().map(|record| (record.collection.0, record.scope.0)).collect();
        let in_set: HashSet<u64> = cursor.collections.iter().map(|id| id.0).collect();
        for record in records {
            // Of the cursor's set (its collections, `first` through its end), at or
            // before its key, and unchanged since its base.
            let sent = in_set.contains(&record.collection.0)
                && record.carrying.is_some_and(|first| first.hlc <= cursor.through)
                && cursor.passed(record.collection, &record.scope)
                && !changed.contains(&(record.collection.0, record.scope.0));
            if sent {
                out.passed.push((record.collection, record.scope));
            } else {
                out.records.push(record);
            }
        }
        out.resumed = true;
        if let Some(stopped) = delta.stopped_at {
            out.base = RecordsBase { key: stopped, epoch: cut.epoch };
            out.changed_whole = false;
        }
        Ok(out)
    }
}
