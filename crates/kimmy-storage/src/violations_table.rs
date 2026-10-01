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
//! table.** The marker lives in the table it describes, so the two cannot be
//! separated. **A backup does not carry the table** (`verified.rs` names the
//! tables a backup copies, and this is not among them): a restore starts with no
//! table and no `through`, and the backfill completes it from zero.
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

/// `KIMMY_TEST_VIOLATIONS_FROM_OPLOG`'s state: answer `/violations` by walking
/// the oplog even when the table is ready, so one binary and one store can be
/// measured both ways (the allocation test's control).
static TEST_FROM_OPLOG: AtomicBool = AtomicBool::new(false);

/// Force the oplog walk for every `/violations` call, for the whole process.
/// A test switch: it changes what a call reads and never what it answers.
pub fn set_test_violations_from_oplog(on: bool) {
    TEST_FROM_OPLOG.store(on, Relaxed);
}

/// Whether the route must walk the oplog whatever the table says.
pub(crate) fn test_forces_oplog() -> bool {
    TEST_FROM_OPLOG.load(Relaxed)
}

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
    /// The route answers from the table: it is complete through the oplog's
    /// tail, as the open found it or as a backfill scan that reached the end of
    /// its range found it. `through`, the marker on disk, may be behind the tail
    /// meanwhile (a scan that finds nothing makes no commit), and a retention pass
    /// moves it.
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
    /// The scan reached the end of its range rather than its budget.
    exhausted: bool,
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
    /// never holds the single writer. **The write is short** (held as
    /// [`WriterHolder::Violations`]): it point-reads only the violation keys
    /// found (one retention collected meanwhile is skipped), inserts their rows,
    /// and advances `through`. **A scan that used up its range, not its budget,
    /// sets `through` to the tail as the writer sees it and the table is ready**:
    /// every entry appended after the scan's snapshot went through
    /// `append_oplog_at` in this process, which maintains the table, so nothing
    /// above the snapshot is missing, and under steady writes a step that
    /// stopped at its snapshot's tail could never finish (ADR-200). **A scan
    /// that used up its range and found no violation takes no writer at all**: the
    /// table is ready in memory and `through` is left for the retention pass
    /// to advance (the same argument holds, since what was appended since the
    /// snapshot maintains the table). `Background` scope: a stop ends it.
    pub fn violations_backfill_step(&self, budget: ExamineBudget) -> Result<BackfillStep> {
        if self.violations().ready() {
            return Ok(BackfillStep { done: true, scanned: 0, found: 0 });
        }
        let scan = self.violations_backfill_scan(budget)?;
        #[cfg(test)]
        test_hooks::run(self);
        self.violations().backfilled_rows.fetch_add(scan.scanned, Relaxed);
        // A scan that reached the end of its range and found nothing records
        // nothing: the table is ready in memory and no commit is made, so that
        // the start of a store whose tail holds no violation costs a read and
        // no fsync (ADR-200). `through` stays where it is and the next start
        // scans the same tail again; the retention pass advances it, once the
        // table is ready. A scan that found rows, or that stopped at its budget
        // and must keep its place, commits as before.
        let (kept, done) = if scan.exhausted && scan.found.is_empty() {
            (0, true)
        } else {
            self.record_violations_backfill(&scan)?
        };
        if done {
            self.violations().set_ready();
        }
        Ok(BackfillStep { done, scanned: scan.scanned, found: kept })
    }

    /// The read half of a step: the stretch of the oplog above `through`, up to
    /// the budget, and the row keys of the violation entries in it.
    fn violations_backfill_scan(&self, budget: ExamineBudget) -> Result<Scan> {
        let mut scan = Scan { found: Vec::new(), last: None, scanned: 0, exhausted: true };
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
                scan.exhausted = false;
                break;
            }
        }
        Ok(scan)
    }

    /// The write half: insert the rows found (skipping an entry retention
    /// collected since the scan) and advance `through`: to the tail as it is now
    /// when the scan used up its range, otherwise to the last stamp scanned or
    /// the tail if that is lower. Answers the rows kept, and whether the table is
    /// now complete through the tail.
    fn record_violations_backfill(&self, scan: &Scan) -> Result<(u64, bool)> {
        let txn = self.begin_write(WriterHolder::Violations)?;
        let mut kept = 0u64;
        let done;
        {
            let oplog = txn.open_table(tables::OPLOG)?;
            let mut table = txn.open_table(tables::UNIQUE_VIOLATIONS)?;
            for row in &scan.found {
                if let Some(stamp) = stamp_of(row)
                    && oplog.get(stamp)?.is_some()
                {
                    table.insert(row.as_slice(), [].as_slice())?;
                    kept += 1;
                }
            }
            let tail = oplog.last()?.map(|(k, _)| k.value().to_vec());
            let through = match (&tail, &scan.last) {
                // Nothing in the oplog: complete, and there is no stamp to name.
                (None, _) => None,
                (Some(tail), _) if scan.exhausted => Some(tail.clone()),
                (Some(tail), Some(last)) => Some(if tail.as_slice() < last.as_slice() {
                    tail.clone()
                } else {
                    last.clone()
                }),
                (Some(_), None) => None,
            };
            done = tail.is_none() || through.as_deref().is_some_and(|t| Some(t) >= tail.as_deref());
            if let Some(through) = through {
                let current = through_of(&table)?;
                if current.as_deref().is_none_or(|c| c < through.as_slice()) {
                    table.insert(SENTINEL, through.as_slice())?;
                }
            }
        }
        txn.commit()?;
        Ok((kept, done))
    }

    /// Advance `through` to the oplog's tail, in a retention pass, when the
    /// table is ready: everything up to the tail is in the table, since a
    /// build that maintains it wrote every violation since it became ready.
    /// Every pass does it, whether or not it removed entries, and commits only
    /// when `through` is behind the tail: a start whose backfill found nothing
    /// leaves `through` where it was and relies on this to move it, so the next
    /// start's rescan is bounded by one pass's writes and not by the oplog's
    /// retention. Not at a clean close: a close gains no write (ADR-200).
    pub(crate) fn advance_violations_through(&self) -> Result<()> {
        if !self.violations().ready() {
            return Ok(());
        }
        // A read first, so that a pass on a node with nothing written since the
        // last one commits nothing. Another write may land between this read and
        // the writer, and the write below looks again.
        {
            let txn = self.db().begin_read()?;
            let oplog = txn.open_table(tables::OPLOG)?;
            let table = txn.open_table(tables::UNIQUE_VIOLATIONS)?;
            match oplog.last()? {
                None => return Ok(()),
                Some((tail, _)) if through_of(&table)?.as_deref() == Some(tail.value()) => {
                    return Ok(());
                }
                Some(_) => {}
            }
        }
        #[cfg(test)]
        test_hooks::before_advance_write(self);
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
    use std::rc::Rc;

    use crate::engine::Engine;

    type Hook = Rc<dyn Fn(&Engine)>;

    thread_local! {
        static BETWEEN: RefCell<Option<Hook>> = const { RefCell::new(None) };
        static EVERY: RefCell<Option<Hook>> = const { RefCell::new(None) };
        static BEFORE_ADVANCE: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    /// Run `hook` once, between the read that finds `through` behind the tail
    /// and the write that advances it, in the next retention pass on this
    /// thread: a write that lands in that gap.
    pub(crate) fn between_advance_check_and_write(hook: impl Fn(&Engine) + 'static) {
        BEFORE_ADVANCE.with(|h| *h.borrow_mut() = Some(Rc::new(hook)));
    }

    pub(super) fn before_advance_write(engine: &Engine) {
        let once = BEFORE_ADVANCE.with(|h| h.borrow_mut().take());
        if let Some(hook) = once {
            hook(engine);
        }
    }

    /// Run `hook` once, between the next step's read and its write.
    pub(crate) fn between_read_and_write(hook: impl Fn(&Engine) + 'static) {
        BETWEEN.with(|h| *h.borrow_mut() = Some(Rc::new(hook)));
    }

    /// Run `hook` between the read and the write of every step from now on, on
    /// this thread: a writer that overtakes every step.
    pub(crate) fn between_read_and_write_every(hook: impl Fn(&Engine) + 'static) {
        EVERY.with(|h| *h.borrow_mut() = Some(Rc::new(hook)));
    }

    /// Stop running the repeating hook.
    pub(crate) fn clear_every() {
        EVERY.with(|h| *h.borrow_mut() = None);
    }

    pub(super) fn run(engine: &Engine) {
        // Taken, so the once-hook runs once and its own calls do not recurse.
        let once = BETWEEN.with(|h| h.borrow_mut().take());
        if let Some(hook) = once {
            hook(engine);
        }
        let every = EVERY.with(|h| h.borrow().clone());
        if let Some(hook) = every {
            hook(engine);
        }
    }
}
