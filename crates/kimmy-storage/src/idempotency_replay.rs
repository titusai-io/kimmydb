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
use crate::walk::{WalkScope, open_walk_table};

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
        let txn = self.begin_write(crate::engine::WriterHolder::Retention)?;
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
    /// below `through`, live by the window alone (a record outlives a drop of
    /// its collection for its window), carried at its `first`, in key order. A record this build
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

    /// Stop-aware: a replay page is served to no client, and a node's stop ends
    /// its read of the page's collections' records between two of them.
    fn replay_records_in(
        &self,
        txn: &ReadTransaction,
        collections: &[CollectionId],
        from: Hlc,
        through: Hlc,
    ) -> Result<Vec<CarriedRecord>> {
        let records = open_walk_table(txn, IDEMPOTENCY, self.walk(WalkScope::Background))?;
        let mut ids = collections.to_vec();
        ids.sort();
        ids.dedup();
        let mut out = Vec::new();
        for id in ids {
            for row in records.range(collection_records(id, None))? {
                let (key, value) = row?;
                let (_, scope) = key.value();
                let Some(scope) = ScopeHash::from_slice(scope) else { continue };
                // not a row: a record's own bytes, which this build may not decode
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

#[cfg(test)]
mod tests {
    use kimmy_core::{Fingerprint, NodeId};

    use super::*;
    use crate::idempotency::{RecordOutcome, RecordState, RecordsVia};
    use crate::meta::CollectionMeta;

    fn open() -> (Engine, CollectionMeta, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        // The window's clock at the stamps the records are planted at, so they live.
        engine.set_idempotency_clock_for_test(Some(at(0).hlc.wall_ms));
        let a = engine.create_collection("shop", "a").unwrap();
        let b = engine.create_collection("shop", "b").unwrap();
        (engine, a, b, dir)
    }

    const NODE: NodeId = NodeId::from_bytes([7; 16]);

    fn at(ms: u64) -> Stamp {
        Stamp::new(Hlc::new(1_000_000 + ms, 0), NODE)
    }

    fn scope(n: u8) -> ScopeHash {
        ScopeHash([n; 32])
    }

    fn record(first: Stamp, last: Stamp) -> IdempotencyRecord {
        IdempotencyRecord {
            fingerprint: Fingerprint([1; 32]),
            first,
            last,
            origin: NODE,
            state: RecordState::Done,
            contested: false,
            outcome: RecordOutcome::Insert { stamp: last },
        }
    }

    /// Store a record as a frame of records alone does.
    fn plant(engine: &Engine, coll: &CollectionMeta, n: u8, first: Stamp, last: Stamp) {
        let carried = CarriedRecord {
            collection: coll.id,
            scope: scope(n),
            carrying: Some(first),
            record: record(first, last).encode(),
        };
        engine.apply_batch_carrying(&[], &[carried], RecordsVia::Pull).unwrap();
        assert!(engine.idempotency_record(coll.id, &scope(n)).unwrap().is_some(), "premise");
    }

    fn keys(records: &[CarriedRecord]) -> Vec<(u64, u8)> {
        records.iter().map(|r| (r.collection.0, r.scope.0[0])).collect()
    }

    /// B2's rule: the records of the named collections with `last` above the
    /// page's start and `first` at or below its end, live by the window, carried
    /// at their `first`, in key order; nothing of another collection, of a request
    /// that ended at or before the start or began after the end, or past its
    /// window.
    #[test]
    fn a_replay_page_takes_the_live_spanning_records_of_its_collections_at_their_first() {
        let (engine, a, b, _dir) = open();
        let (from, through) = (at(10).hlc, at(20).hlc);
        plant(&engine, &a, 1, at(5), at(15)); // spans the start
        plant(&engine, &a, 2, at(12), at(30)); // inside, ends past the end
        plant(&engine, &a, 3, at(2), at(10)); // ends at the start
        plant(&engine, &a, 4, at(21), at(25)); // begins past the end
        plant(&engine, &b, 5, at(5), at(15)); // another collection
        plant(&engine, &a, 6, at(5), at(11)); // past its window, below
        engine.set_idempotency_window(60);
        engine.set_idempotency_clock_for_test(Some(at(11).hlc.wall_ms + 60_000 + 1));
        let page = engine.records_for_replay_page(&[a.id], from, through).unwrap();
        assert_eq!(keys(&page), vec![(a.id.0, 1), (a.id.0, 2)]);
        assert_eq!(page[0].carrying, Some(at(5)), "carried at its first");
        assert_eq!(page[1].carrying, Some(at(12)));
        assert_eq!(
            IdempotencyRecord::decode(&page[0].record).unwrap(),
            engine.idempotency_record(a.id, &scope(1)).unwrap(),
            "the record as stored"
        );
    }

    /// An orphan row (its record gone, as a retention pass stopped between chunks
    /// leaves one) inside a replay page's range is passed over without error and
    /// ships nothing: B2 reads the records, and the window's own rows (F2, F3)
    /// name a record that is not there.
    #[test]
    fn an_orphan_row_inside_a_replay_page_ships_nothing() {
        let (engine, a, _b, _dir) = open();
        plant(&engine, &a, 1, at(15), at(15));
        assert!(engine.remove_idempotency_record_for_test(a.id, &scope(1)).unwrap());
        let page = engine.records_for_replay_page(&[a.id], at(10).hlc, at(20).hlc).unwrap();
        assert!(page.is_empty(), "{page:?}");
        let empty = kimmy_core::VersionVector::new();
        let window = engine
            .entries_for_peer_holding(Hlc::ZERO, 1024, Some(&empty), crate::WalkScope::Background)
            .unwrap();
        assert!(window.records.is_empty(), "the orphan row ships nothing: {:?}", window.records);
    }

    /// A records cursor of this page's position and of this log, read again: its
    /// set at or before its key, unchanged since its base, is passed; a record that
    /// changed since is carried as it stands now, and one outside the set (another
    /// collection than the cursor's, or a `first` past its end) is never passed.
    #[test]
    fn a_records_cursor_passes_its_set_up_to_its_key_and_nothing_that_changed() {
        let (engine, a, b, _dir) = open();
        let (from, through) = (at(10).hlc, at(20).hlc);
        for n in 1..=4 {
            plant(&engine, &a, n, at(5), at(15));
        }
        plant(&engine, &b, 1, at(5), at(15));
        let cut = engine.replay_page_records(&[a.id], from, through, None, 1 << 20).unwrap();
        assert!(!cut.resumed && cut.passed.is_empty());
        assert_eq!(keys(&cut.records).len(), 4);
        let cursor = ReplayRecords {
            at: from,
            through,
            collections: vec![a.id],
            collection: a.id,
            scope: scope(3),
            base: cut.base,
        };
        // Changed since the cut: record 2 merges with a later attempt.
        plant(&engine, &a, 2, at(6), at(18));
        let read = engine
            .replay_page_records(&[a.id, b.id], from, through, Some(&cursor), 1 << 20)
            .unwrap();
        assert!(read.resumed && read.changed_whole);
        let passed: Vec<_> = read.passed.iter().map(|(c, s)| (c.0, s.0[0])).collect();
        assert_eq!(passed, vec![(a.id.0, 1), (a.id.0, 3)], "its set up to its key, unchanged");
        assert_eq!(keys(&read.changed), vec![(a.id.0, 2)], "what changed, as it stands now");
        assert_eq!(read.changed[0].carrying, Some(at(5)), "at its first");
        let mut rest = keys(&read.records);
        rest.sort();
        let mut want = vec![(a.id.0, 2), (a.id.0, 4), (b.id.0, 1)];
        want.sort();
        assert_eq!(rest, want, "the rest, another collection's included");
        // Another position: not honoured.
        let elsewhere = engine
            .replay_page_records(&[a.id], at(11).hlc, through, Some(&cursor), 1 << 20)
            .unwrap();
        assert!(!elsewhere.resumed && elsewhere.passed.is_empty());
        // Another epoch (a restore, an identity reset): the position starts over.
        let other = ReplayRecords {
            base: RecordsBase {
                epoch: cursor.base.epoch.map(|e| e.wrapping_add(1)),
                ..cursor.base
            },
            ..cursor.clone()
        };
        let restarted =
            engine.replay_page_records(&[a.id], from, through, Some(&other), 1 << 20).unwrap();
        assert!(!restarted.resumed && restarted.passed.is_empty());
        assert_eq!(keys(&restarted.records).len(), 4);
    }

    /// What a cursor passes is its own set alone: a record of a collection its
    /// page did not name, or one whose `first` lies past its end, is never passed,
    /// whatever its key.
    #[test]
    fn a_records_cursor_passes_nothing_outside_its_set() {
        let (engine, a, b, _dir) = open();
        let (from, through) = (at(10).hlc, at(20).hlc);
        plant(&engine, &a, 1, at(5), at(15));
        plant(&engine, &a, 2, at(25), at(30)); // its first past the cursor's end
        plant(&engine, &b, 1, at(5), at(15)); // a collection the cursor did not name
        let cut = engine.replay_page_records(&[a.id], from, through, None, 1 << 20).unwrap();
        let cursor = ReplayRecords {
            at: from,
            through,
            collections: vec![a.id],
            collection: CollectionId(u64::MAX),
            scope: ScopeHash([0xff; 32]),
            base: cut.base,
        };
        let read = engine
            .replay_page_records(&[a.id, b.id], from, at(30).hlc, Some(&cursor), 1 << 20)
            .unwrap();
        assert!(read.resumed);
        let passed: Vec<_> = read.passed.iter().map(|(c, s)| (c.0, s.0[0])).collect();
        assert_eq!(passed, vec![(a.id.0, 1)], "its own set alone");
        let mut rest = keys(&read.records);
        rest.sort();
        let mut want = vec![(a.id.0, 2), (b.id.0, 1)];
        want.sort();
        assert_eq!(rest, want);
    }

    /// A delta larger than its budget stops, and the base moves to where it did.
    #[test]
    fn a_records_cursors_delta_stops_at_its_budget_and_moves_the_base() {
        let (engine, a, _b, _dir) = open();
        let (from, through) = (at(10).hlc, at(20).hlc);
        plant(&engine, &a, 1, at(5), at(15));
        let cut = engine.replay_page_records(&[a.id], from, through, None, 1 << 20).unwrap();
        let cursor = ReplayRecords {
            at: from,
            through,
            collections: vec![a.id],
            collection: a.id,
            scope: scope(9),
            base: cut.base,
        };
        for n in 2..=4 {
            plant(&engine, &a, n, at(5), at(15));
        }
        let read = engine.replay_page_records(&[a.id], from, through, Some(&cursor), 300).unwrap();
        assert!(read.resumed && !read.changed_whole, "{read:?}");
        assert_eq!(read.changed.len(), 1, "a budget that holds one record stops after it");
        assert!(read.base.key > cursor.base.key && read.base.epoch == cursor.base.epoch);
    }

    /// A changed record larger than the budget cannot cross on a page holding
    /// nothing else, and is refused by name rather than sent alone, as a
    /// snapshot page's delta refuses one.
    #[test]
    fn a_changed_record_larger_than_the_budget_is_refused_by_name() {
        let (engine, a, _b, _dir) = open();
        let (from, through) = (at(10).hlc, at(20).hlc);
        plant(&engine, &a, 1, at(5), at(15));
        let cut = engine.replay_page_records(&[a.id], from, through, None, 1 << 20).unwrap();
        let cursor = ReplayRecords {
            at: from,
            through,
            collections: vec![a.id],
            collection: a.id,
            scope: scope(9),
            base: cut.base,
        };
        plant(&engine, &a, 2, at(5), at(15));
        let refused = engine.replay_page_records(&[a.id], from, through, Some(&cursor), 1);
        assert!(
            matches!(&refused, Err(crate::StorageError::SnapshotRowTooLarge { .. })),
            "{refused:?}"
        );
    }
}
