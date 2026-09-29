//! The unique-violations table (ADR-200): which oplog entries are this node's
//! `UniqueViolation` records, per collection, so that `/violations` reads its
//! own records and not the whole retained oplog.
//!
//! A `UniqueViolation` entry is local: the node that merged a collision writes
//! it and it is never shipped, so every one in this node's oplog was written
//! here. The route used to walk the oplog to find them, which costs the whole
//! oplog through redb's page cache on every call (a value lives in the leaf
//! page of its key, so reading a header reads the page).
//!
//! **The table** is keyed by `collection id (8 bytes) || stamp key (26)` with an
//! empty value: one row per retained violation entry, no body, so it cannot
//! disagree with the entry about what the violation says. **A sentinel row**,
//! key `[0x00]` (one byte, which sorts before every 34-byte row), holds
//! `through`, a stamp key: **every violation entry at or below it is in the
//! table.** The marker lives in the table it describes so that a backup, which
//! copies only the tables it lists (`verified.rs`), can never carry one without
//! the other.
//!
//! **Maintenance is one way in.** [`crate::engine::append_oplog_at`] inserts the
//! row whenever the entry is a `UniqueViolation`; retention and a rewind remove
//! it with the entry. **Completeness is recovered from the store itself**: at
//! open the table is ready if `through` is at the oplog's tail, and otherwise a
//! background pass ([`Engine::violations_backfill_step`]) scans the oplog from
//! `through`, so whatever a build that does not know the table wrote in between
//! (an older build, after a rollback) is found. Nothing here needs a version
//! marker and nothing is a rollback boundary.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use kimmy_core::{CollectionId, OpKind};
use redb::{ReadableDatabase, ReadableTable};

use crate::codec;
use crate::engine::{Engine, WriterHolder};
use crate::error::Result;
use crate::tables;
use crate::walk::{WalkScope, open_walk_table};
use crate::watch::ExamineBudget;

/// The sentinel row's key: one byte, before every collection's rows.
pub(crate) const SENTINEL: &[u8] = &[0x00];

/// A row's key: the collection, then the entry's stamp key.
pub(crate) fn row_key(collection: CollectionId, stamp_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + stamp_key.len());
    key.extend_from_slice(&collection.0.to_be_bytes());
    key.extend_from_slice(stamp_key);
    key
}

/// The stamp key half of a row's key, or `None` for the sentinel or a key of
/// another length.
pub(crate) fn stamp_of(row_key: &[u8]) -> Option<&[u8]> {
    (row_key.len() == 8 + codec::STAMP_LEN).then(|| &row_key[8..])
}

/// What the table's state is, in process: whether the route may use it, and
/// what the backfill has read and the route has answered from.
#[derive(Default)]
pub(crate) struct ViolationsState {
    ready: AtomicBool,
    backfilled_rows: AtomicU64,
    /// Calls the route answered from the table, and from the oplog walk.
    from_table: AtomicU64,
    from_oplog: AtomicU64,
}

impl ViolationsState {
    pub(crate) fn ready(&self) -> bool {
        self.ready.load(Relaxed)
    }
    pub(crate) fn set_ready(&self) {
        self.ready.store(true, Relaxed);
    }
    #[cfg(test)]
    pub(crate) fn clear_ready_for_test(&self) {
        self.ready.store(false, Relaxed);
    }
    pub(crate) fn note_call(&self, table: bool) {
        if table {
            self.from_table.fetch_add(1, Relaxed);
        } else {
            self.from_oplog.fetch_add(1, Relaxed);
        }
    }
    pub(crate) fn snapshot(&self) -> ViolationsSnapshot {
        ViolationsSnapshot {
            ready: self.ready(),
            backfilled_rows: self.backfilled_rows.load(Relaxed),
            calls_from_table: self.from_table.load(Relaxed),
            calls_from_oplog: self.from_oplog.load(Relaxed),
        }
    }
}

/// The violations table as a scrape reads it (ADR-200).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ViolationsSnapshot {
    /// The route answers from the table (`through` is at the oplog's tail).
    pub ready: bool,
    /// Oplog rows the background pass has read.
    pub backfilled_rows: u64,
    /// `/violations` calls answered from the table, and from the oplog walk.
    pub calls_from_table: u64,
    pub calls_from_oplog: u64,
}

/// What the read half of a backfill step found.
struct Scan {
    found: Vec<Vec<u8>>,
    last: Option<Vec<u8>>,
    scanned: u64,
}

/// What one backfill step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackfillStep {
    /// The table is complete through the oplog's tail: the route may use it.
    pub done: bool,
    /// Oplog rows the step read.
    pub scanned: u64,
    /// Violation rows it found.
    pub found: u64,
}

/// `through`, the stamp key the table is complete through, if any.
pub(crate) fn through_of<T: ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
) -> Result<Option<Vec<u8>>> {
    Ok(table.get(SENTINEL)?.map(|v| v.value().to_vec()))
}

impl Engine {
    /// Whether `/violations` may be answered from the table.
    pub fn violations_table_ready(&self) -> bool {
        self.violations().ready()
    }

    /// The table's state, for a scrape.
    pub fn violations_snapshot(&self) -> ViolationsSnapshot {
        self.violations().snapshot()
    }

    /// Read one budgeted stretch of the oplog from `through`, find the
    /// `UniqueViolation` entries in it from their headers, and record them.
    ///
    /// **The read is outside the writer**: a read transaction, header-only,
    /// budgeted as ADR-194 budgets a walk (time, then rows), so a cold scan
    /// never holds the single writer. **The write is short**: it point-reads
    /// only the violation keys found (one retention collected meanwhile is
    /// skipped), inserts their rows, and advances `through` to the last stamp
    /// scanned, or the tail if that is lower. `Background` scope: a stop ends it.
    pub fn violations_backfill_step(&self, budget: ExamineBudget) -> Result<BackfillStep> {
        if self.violations().ready() {
            return Ok(BackfillStep { done: true, scanned: 0, found: 0 });
        }
        let scan = self.violations_backfill_scan(budget)?;
        #[cfg(test)]
        test_hooks::run(self);
        self.violations().backfilled_rows.fetch_add(scan.scanned, Relaxed);
        let Some(last) = scan.last else {
            // Nothing above `through`: complete, if the oplog's tail is at it.
            return self.finish_violations_backfill(0, 0);
        };
        let kept = self.record_violations_backfill(&scan.found, last)?;
        self.finish_violations_backfill(scan.scanned, kept)
    }

    /// The read half of a step: the stretch of the oplog above `through`, up to
    /// the budget, and the row keys of the violation entries in it.
    fn violations_backfill_scan(&self, budget: ExamineBudget) -> Result<Scan> {
        let mut scan = Scan { found: Vec::new(), last: None, scanned: 0 };
        let txn = self.db().begin_read()?;
        let oplog = open_walk_table(&txn, tables::OPLOG, self.walk(WalkScope::Background))?;
        let table =
            open_walk_table(&txn, tables::UNIQUE_VIOLATIONS, self.walk(WalkScope::Background))?;
        let from = table.get(SENTINEL)?.map(|v| v.value().to_vec());
        let started = Instant::now();
        let range = match &from {
            Some(from) => oplog.range::<&[u8]>((
                std::ops::Bound::Excluded(from.as_slice()),
                std::ops::Bound::Unbounded,
            ))?,
            None => oplog.range::<&[u8]>(..)?,
        };
        for row in range {
            let (key, value) = row?;
            scan.scanned += 1;
            // not a row: a header this build cannot read names no violation record.
            if let Ok((OpKind::UniqueViolation, collection)) =
                codec::decode_oplog_kind_and_collection(value.value())
            {
                scan.found.push(row_key(collection, key.value()));
            }
            scan.last = Some(key.value().to_vec());
            if budget.spent_after(scan.scanned, started.elapsed()) {
                break;
            }
        }
        Ok(scan)
    }

    /// The write half: insert the rows found (skipping an entry retention
    /// collected since the scan) and advance `through` to `last`, or to the tail
    /// if that is lower. Answers the rows kept.
    fn record_violations_backfill(&self, found: &[Vec<u8>], last: Vec<u8>) -> Result<u64> {
        let txn = self.begin_write(WriterHolder::Retention)?;
        let mut kept = 0u64;
        {
            let oplog = txn.open_table(tables::OPLOG)?;
            let mut table = txn.open_table(tables::UNIQUE_VIOLATIONS)?;
            for row in found {
                if let Some(stamp) = stamp_of(row)
                    && oplog.get(stamp)?.is_some()
                {
                    table.insert(row.as_slice(), [].as_slice())?;
                    kept += 1;
                }
            }
            let tail = oplog.last()?.map(|(k, _)| k.value().to_vec());
            let through = match tail {
                Some(tail) if tail.as_slice() < last.as_slice() => tail,
                _ => last,
            };
            let current = through_of(&table)?;
            if current.as_deref().is_none_or(|c| c < through.as_slice()) {
                table.insert(SENTINEL, through.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(kept)
    }

    /// After a step's commit: ready if `through` has reached the tail.
    fn finish_violations_backfill(&self, scanned: u64, found: u64) -> Result<BackfillStep> {
        let txn = self.db().begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        let table = txn.open_table(tables::UNIQUE_VIOLATIONS)?;
        let done = match (oplog.last()?.map(|(k, _)| k.value().to_vec()), through_of(&table)?) {
            (None, _) => true,
            (Some(tail), Some(through)) => through.as_slice() >= tail.as_slice(),
            (Some(_), None) => false,
        };
        if done {
            self.violations().set_ready();
        }
        Ok(BackfillStep { done, scanned, found })
    }

    /// Advance `through` to the oplog's tail, in a retention pass, when the
    /// table is ready: everything up to the tail is in the table, since a
    /// build that maintains it wrote every violation since it became ready.
    /// Not at a clean close: the rescan after a crash is bounded by the
    /// retention interval, and a close gains no write (ADR-200).
    pub(crate) fn advance_violations_through(&self) -> Result<()> {
        if !self.violations().ready() {
            return Ok(());
        }
        let txn = self.begin_write(WriterHolder::Retention)?;
        {
            let oplog = txn.open_table(tables::OPLOG)?;
            let mut table = txn.open_table(tables::UNIQUE_VIOLATIONS)?;
            if let Some((tail, _)) = oplog.last()? {
                let tail = tail.value().to_vec();
                if through_of(&table)?.as_deref() != Some(tail.as_slice()) {
                    table.insert(SENTINEL, tail.as_slice())?;
                }
            }
        }
        txn.commit()?;
        Ok(())
    }
}

/// A test's hand on the moment between a backfill step's read and its write,
/// where retention or a rewind may run.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::RefCell;

    use crate::engine::Engine;

    type Hook = Box<dyn Fn(&Engine)>;

    thread_local! {
        static BETWEEN: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    pub(crate) fn between_read_and_write(hook: impl Fn(&Engine) + 'static) {
        BETWEEN.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
    }

    pub(super) fn run(engine: &Engine) {
        // Taken, so the hook runs once and its own calls do not recurse.
        if let Some(hook) = BETWEEN.with(|h| h.borrow_mut().take()) {
            hook(engine);
        }
    }
}
