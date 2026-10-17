//! Storage schema 4 -> 5: every number key is exact (ADR-238, design section 6).
//!
//! Schema 5 changes the bytes of every number key (`kimmy_core::keyenc`: a
//! variable-length mantissa and an exactness mark, where schema 4 had eight
//! fixed mantissa bytes). Two places hold such keys on disk: `DOCS`, whose key
//! is the encoded `_id` (an `Int64` is the only numeric id a schema 4 store can
//! hold), and `INDEX_ENTRIES`, whose keys hold indexed values and document keys.
//! This module moves both, at open, before the node serves anything.
//!
//! **Documents are re-keyed per `DOCS` collection-id range, through a staging
//! table, under a durable phase.** A new key can equal the old key of a
//! *different* id (the schema 5 key of `0x5000008101800000` is the schema 4 key
//! of `0x50007F8081018000`), so a range must never hold both forms: invariant
//! **I-DOCS**. Phase `rekeying` moves each old row into [`DOCS_REKEYED`] under
//! its new key and deletes it from `DOCS`, a chunk per transaction; the commit
//! that drains the range sets `moving_back`. Phase `moving_back` moves the
//! staged rows into `DOCS`, deleting each from the staging table in the same
//! transaction; the commit that empties it sets `done`. Nothing reads `DOCS`
//! for old keys once the phase is `moving_back`: no byte test can tell the two
//! forms apart ([`kimmy_core::legacy_key::decode_v1`] accepts that new key as
//! the other id), so the phase alone says which form a row is in. The unit is
//! the `DOCS` range and not the collection, because a drop leaves its rows under
//! an id nothing resolves until the purger removes them (ADR-189), and those
//! rows are re-keyed too: the purger folds each row's term out of the cut digest
//! as it removes it.
//!
//! **Every index is drained and rebuilt.** Old and new entries can be
//! byte-identical for different values, so an index's range is emptied before
//! any new entry is written: `pending` -> `draining` -> `refilling(cursor)` ->
//! `done`, each step a chunk per transaction, the cursor committed with the
//! chunk it ends. The refill derives entries with the current index code, so it
//! is also where a document's membership is decided again under the exact
//! comparator. A unique index's collisions are found against `INDEX_ENTRIES`
//! itself as each entry is written, so a run killed and resumed still sees the
//! entries of every earlier chunk, and each is recorded in [`COLLISIONS`] in the
//! commit of the chunk that found it, to be reported once the engine exists.
//!
//! **The rebuild changes no definition and no stamp**: it writes
//! `INDEX_ENTRIES`, its own tables and nothing else, except the one-way
//! `multikey` flag, raised through a full `CollectionMeta` round trip in the
//! commit of the chunk that wrote the entry that needs it. Nothing is appended
//! to the oplog, and the live counts and the cut digests are not touched: they
//! fold the legacy (schema 4) form of a key, which re-keying does not change.
//!
//! **Order at open** (design 6.1): the ensure-tables commit with the tombstone
//! horizon's reset; then the refusals that write nothing ([`preflight`] and the
//! unparseable-filter check); then `raise(5)`, from which a 0.50 or 0.49 build
//! refuses the store; then [`begin`], the first commit, which writes schema 5
//! and creates this module's tables and markers. A start refused before
//! `raise(5)` leaves a store 0.50 opens. The markers, not the version, mean
//! unfinished: [`unfinished`] is what a later open asks, and [`finish`]'s commit
//! deletes them.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use bson::Bson;
use kimmy_core::CollectionId;
use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle,
    WriteTransaction,
};
use tracing::{info, warn};

use crate::codec;
use crate::error::{Result, StorageError};
use crate::index::{DocumentKeys, UNDECIDABLE, UNKEYED};
use crate::meta::{CollectionMeta, IndexMeta};
use crate::tables;
use crate::walk::{WalkStop, open_walk_table_in};

/// `(collection id, schema 5 key) -> the record`: the rows of a range between
/// its two phases. Empty, and deleted, once the migration finishes.
pub(crate) const DOCS_REKEYED: TableDefinition<(u64, &[u8]), &[u8]> =
    TableDefinition::new("schema5_docs_rekeyed");

/// `collection id -> phase || rows moved out || rows moved back`, one row per `DOCS` collection-id
/// range, listed in the first commit whether or not a collection stands over
/// the id. Its presence is what "unfinished" means.
pub(crate) const RANGE_PHASES: TableDefinition<u64, &[u8]> =
    TableDefinition::new("schema5_docs_phases");

/// `(collection id, index id) -> the index's step and what its rebuild found`.
pub(crate) const INDEX_STEPS: TableDefinition<(u64, u32), &[u8]> =
    TableDefinition::new("schema5_index_steps");

/// `(collection id, index id, index key) -> flags || holders`: every unique
/// collision the rebuild found, and the schema 3 -> 4 rebuild's unreported
/// ones, carried. **It outlives the migration's last commit** and is deleted
/// only once the engine has reported what it holds
/// ([`crate::migrate::forget_reported_violations`]): a crash between the two
/// would otherwise lose a collision nobody was ever told about.
pub(crate) const COLLISIONS: TableDefinition<(u64, u32, &[u8]), &[u8]> =
    TableDefinition::new("schema5_unique_collisions");

/// The schema 3 -> 4 markers, deleted in the first commit on every store older
/// than 5, finished or not: 4 -> 5 rebuilds every index under the current rule,
/// which subsumes what 3 -> 4 did.
pub(crate) const PARTIAL_REBUILT: TableDefinition<(u64, u32), ()> =
    TableDefinition::new("partial_rebuilt_under_find");

/// What a finished schema 3 -> 4 rebuild found and had not yet reported, carried
/// into [`COLLISIONS`] in the first commit rather than deleted.
pub(crate) const PARTIAL_REBUILT_VIOLATIONS: TableDefinition<(u64, u32), &[u8]> =
    TableDefinition::new("partial_rebuilt_unique_violations");

/// The first byte of a number key, in both schemas: the only `DOCS` keys that
/// change.
const NUMBER_TAG: &[u8] = &[0x20];
/// The byte after it, which bounds the numeric part of a range.
const ABOVE_NUMBER_TAG: &[u8] = &[0x21];

/// Rows per re-keying transaction (design 6.1). Tiny under test, so every test
/// crosses many chunk boundaries.
#[cfg(not(test))]
const REKEY_CHUNK_ROWS: usize = 8_192;
#[cfg(test)]
const REKEY_CHUNK_ROWS: usize = 7;

/// Index-entry bytes per draining or refilling transaction (design 6.2: 8 to 16
/// MiB). Tiny under test, for the same reason.
#[cfg(not(test))]
const INDEX_CHUNK_BYTES: usize = 16 << 20;
#[cfg(test)]
const INDEX_CHUNK_BYTES: usize = 120;

/// How many rows between two progress lines.
#[cfg(not(test))]
const PROGRESS_EVERY: u64 = 100_000;
#[cfg(test)]
const PROGRESS_EVERY: u64 = 10;

/// The bytes a redb region's data pages hold: redb 4.3 caps a region at
/// `MAX_USABLE_REGION_SPACE`, 4 GiB, and at 2^20 pages (`MAX_PAGE_INDEX + 1`) of
/// its page size, 4 KiB by default (`page_manager.rs`, `header.rs`), which is
/// the same 4 GiB. The engine opens every store with the default page size.
pub(crate) const REDB_REGION_BYTES: u64 = 4 << 30;

/// redb's page, which the file's first page (the super-header) occupies.
const REDB_PAGE_BYTES: u64 = 4096;

/// The most the file can grow by in one step, from its length: what the
/// preflight asks for beside the working set, in place of the fixed 2,048 MiB
/// the design first named.
///
/// redb 4.3 grows the file only when an allocation finds no free page
/// (`TransactionalMemory::grow`), by a step that depends on where the file
/// stands against its 4 GiB regions. With `U` the file's usable bytes (its
/// length less the super-header page), `R` a region and `T = U mod R` the part
/// of the last region in use:
///
/// - `U < R`, one partial region: it asks for `max(2U, U + 2a)` bytes, `a` the
///   allocation that did not fit, so the file **doubles**: it grows by `U`
///   (an allocation larger than the whole file aside, which no migration's
///   pages are). Past `R`, the layout becomes a full region and a partial one,
///   which is still `2U` in all;
/// - `U >= R` and `T > 0`: it fills the partial region (`R - T`), or, when the
///   allocation needs more than half of what that region has left, fills it and
///   adds one more region: **`2R - T` at the worst**;
/// - `U >= R` and `T = 0`: it adds one region, `R`.
///
/// **Checked against the measurement at 2,000,000 documents**
/// (`examples/schema4_migration`): a 2,411 MiB file, one partial region, so at
/// most 2,411 MiB of growth to 4,822 MiB. The file was seen at 4,096 MiB and
/// no more: the doubling asked for one full region and 726 MiB beyond it, the
/// allocations stayed in the full region, and the commit that followed gave the
/// untouched partial region back (`try_shrink` removes a trailing region that
/// is wholly free), so +1,685 MiB, inside the bound. At close the file went
/// back to 2,415 MiB.
///
/// One step covers a migration whose working set fits the space that step
/// adds; [`Plan::margin_bytes`] is asked for on top for the rest.
pub(crate) fn worst_growth(file_bytes: u64) -> u64 {
    let usable = file_bytes.saturating_sub(REDB_PAGE_BYTES);
    if usable < REDB_REGION_BYTES {
        return usable;
    }
    match usable % REDB_REGION_BYTES {
        0 => REDB_REGION_BYTES,
        trailing => 2 * REDB_REGION_BYTES - trailing,
    }
}

/// The phase of one `DOCS` collection-id range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Old-form rows are moved into [`DOCS_REKEYED`]; every numeric row left in
    /// `DOCS` under this id is old-form.
    Rekeying,
    /// Staged rows are moved back; no old-form row is left in `DOCS`.
    MovingBack,
    Done,
}

/// A range's marker: its phase, and the rows each phase has moved so far.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RangeMark {
    pub(crate) phase: Phase,
    /// Rows moved into the staging table.
    pub(crate) out: u64,
    /// Rows moved back into `DOCS`.
    pub(crate) back: u64,
}

impl RangeMark {
    fn new(phase: Phase) -> Self {
        Self { phase, out: 0, back: 0 }
    }

    fn encode(&self) -> [u8; 17] {
        let mut out = [0u8; 17];
        out[0] = match self.phase {
            Phase::Rekeying => 1,
            Phase::MovingBack => 2,
            Phase::Done => 3,
        };
        out[1..9].copy_from_slice(&self.out.to_be_bytes());
        out[9..].copy_from_slice(&self.back.to_be_bytes());
        out
    }

    pub(crate) fn decode(raw: &[u8]) -> Result<Self> {
        let phase = match raw.first() {
            Some(1) => Phase::Rekeying,
            Some(2) => Phase::MovingBack,
            Some(3) => Phase::Done,
            _ => return Err(corrupt_marker("a range's phase")),
        };
        let at = |from: usize| {
            raw.get(from..from + 8)
                .and_then(|b| b.try_into().ok())
                .map(u64::from_be_bytes)
                .ok_or_else(|| corrupt_marker("a range's phase"))
        };
        if raw.len() != 17 {
            return Err(corrupt_marker("a range's phase"));
        }
        Ok(Self { phase, out: at(1)?, back: at(9)? })
    }
}

/// The step of one index's rebuild.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    Pending,
    Draining,
    /// Every document up to and including `after` has its entries.
    Refilling {
        after: Option<Vec<u8>>,
    },
    Done,
}

/// An index's marker: its step, and what its rebuild has found so far, carried
/// across a restart so the closing line counts the whole rebuild.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexMark {
    pub(crate) step: Step,
    pub(crate) drained: u64,
    pub(crate) entries: u64,
    pub(crate) unkeyed: u64,
    pub(crate) collisions: u64,
    /// For a TTL index: what its filter's selection did across the change of
    /// comparator (design 2.6), counted before the drain.
    pub(crate) ttl: TtlCount,
}

/// A TTL index's count of documents whose selection by its partial filter the
/// exact comparator changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TtlCount {
    /// Not counted yet, or not a TTL index.
    Pending,
    /// The previous build's selection is not in the store: the index was not
    /// filed under ADR-185's rule (a schema 3 source, or a partial index its
    /// interrupted 3 -> 4 rebuild had not reached), so there is no "before".
    NotRecorded,
    /// `(now selected, no longer selected)`.
    Counted(u64, u64),
}

impl IndexMark {
    fn pending() -> Self {
        Self {
            step: Step::Pending,
            drained: 0,
            entries: 0,
            unkeyed: 0,
            collisions: 0,
            ttl: TtlCount::Pending,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.push(match self.step {
            Step::Pending => 0,
            Step::Draining => 1,
            Step::Refilling { .. } => 2,
            Step::Done => 3,
        });
        for n in [self.drained, self.entries, self.unkeyed, self.collisions] {
            out.extend_from_slice(&n.to_be_bytes());
        }
        match self.ttl {
            TtlCount::Pending => out.push(0),
            TtlCount::Counted(selected, unselected) => {
                out.push(1);
                out.extend_from_slice(&selected.to_be_bytes());
                out.extend_from_slice(&unselected.to_be_bytes());
            }
            TtlCount::NotRecorded => out.push(2),
        }
        if let Step::Refilling { after: Some(after) } = &self.step {
            out.push(1);
            out.extend_from_slice(after);
        } else {
            out.push(0);
        }
        out
    }

    pub(crate) fn decode(raw: &[u8]) -> Result<Self> {
        let bad = || corrupt_marker("an index's step");
        let mut input = raw;
        let mut take = |n: usize| -> Result<&[u8]> {
            if input.len() < n {
                return Err(bad());
            }
            let (head, tail) = input.split_at(n);
            input = tail;
            Ok(head)
        };
        let step = take(1)?[0];
        let mut numbers = [0u64; 4];
        for n in &mut numbers {
            *n = u64::from_be_bytes(take(8)?.try_into().expect("8 bytes"));
        }
        let ttl = match take(1)?[0] {
            0 => TtlCount::Pending,
            1 => TtlCount::Counted(
                u64::from_be_bytes(take(8)?.try_into().expect("8 bytes")),
                u64::from_be_bytes(take(8)?.try_into().expect("8 bytes")),
            ),
            2 => TtlCount::NotRecorded,
            _ => return Err(bad()),
        };
        let after = match take(1)?[0] {
            0 => None,
            1 => Some(input.to_vec()),
            _ => return Err(bad()),
        };
        let step = match step {
            0 => Step::Pending,
            1 => Step::Draining,
            2 => Step::Refilling { after },
            3 => Step::Done,
            _ => return Err(bad()),
        };
        let [drained, entries, unkeyed, collisions] = numbers;
        Ok(Self { step, drained, entries, unkeyed, collisions, ttl })
    }
}

/// One recorded collision: whether it was carried from the schema 3 -> 4
/// rebuild and not found again, and the keys of the documents holding it, the
/// one that revealed it last.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Collision {
    /// Carried from a schema 3 -> 4 rebuild's record: its index key is in the
    /// schema 4 form, and the refill has not found it again under that key.
    pub(crate) carried: bool,
    pub(crate) holders: Vec<Vec<u8>>,
}

impl Collision {
    fn encode(&self) -> Vec<u8> {
        let mut out = vec![u8::from(self.carried)];
        out.extend_from_slice(&(self.holders.len() as u32).to_be_bytes());
        for holder in &self.holders {
            out.extend_from_slice(&(holder.len() as u32).to_be_bytes());
            out.extend_from_slice(holder);
        }
        out
    }

    pub(crate) fn decode(raw: &[u8]) -> Result<Self> {
        let bad = || corrupt_marker("a recorded collision");
        let (&flags, mut rest) = raw.split_first().ok_or_else(bad)?;
        let u32_at = |rest: &mut &[u8]| -> Result<usize> {
            let (head, tail) = rest.split_at_checked(4).ok_or_else(bad)?;
            *rest = tail;
            Ok(u32::from_be_bytes(head.try_into().expect("4 bytes")) as usize)
        };
        let count = u32_at(&mut rest)?;
        let mut holders = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            let len = u32_at(&mut rest)?;
            let (holder, tail) = rest.split_at_checked(len).ok_or_else(bad)?;
            holders.push(holder.to_vec());
            rest = tail;
        }
        if !rest.is_empty() || flags > 1 {
            return Err(bad());
        }
        Ok(Self { carried: flags == 1, holders })
    }

    /// Add `holders` that are not already named, in order.
    fn hold(&mut self, holders: impl IntoIterator<Item = Vec<u8>>) {
        for holder in holders {
            if !self.holders.contains(&holder) {
                self.holders.push(holder);
            }
        }
    }
}

fn corrupt_marker(what: &str) -> StorageError {
    StorageError::Corrupt(format!(
        "{what} in the schema 5 migration's markers will not decode, so the interrupted \
         migration cannot be resumed from it"
    ))
}

/// Whether an interrupted schema 5 migration left its markers: on a store at
/// schema 5, this, and not the version, is what says the migration is
/// unfinished.
pub(crate) fn unfinished(db: &Database) -> Result<bool> {
    let txn = db.begin_read()?;
    let tables: Vec<String> = txn.list_tables()?.map(|t| t.name().to_string()).collect();
    Ok([RANGE_PHASES.name(), INDEX_STEPS.name()].iter().any(|m| tables.iter().any(|t| t == m)))
}

/// What the migration will do, stated before `raise(5)`, and what the free
/// space has to cover.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Plan {
    /// Collections a `COLLECTIONS` row stands over.
    pub collections: u64,
    /// `DOCS` collection-id ranges, purge-pending ones included.
    pub ranges: u64,
    /// Ranges no collection stands over: a drop's rows still being purged.
    pub purge_pending: u64,
    /// Every row of `DOCS`, tombstones included: an upper bound on the rows to
    /// re-key, which are the numeric-id ones, read from redb's own count.
    pub docs_rows: u64,
    /// Indexes to rebuild.
    pub indexes: u64,
    /// Every entry of `INDEX_ENTRIES`, to drain.
    pub index_entries: u64,
    /// The store's file, in bytes.
    pub file_bytes: u64,
    /// The most the file can grow by in one step ([`worst_growth`]).
    pub growth_bytes: u64,
    /// The free space the start requires, in bytes: that growth and
    /// [`Plan::margin_bytes`].
    pub required_bytes: u64,
}

impl Plan {
    /// The working set chunks can add to the file before the pages they free
    /// are reused: every chunk commits durably ([`commit`]), so the pages one
    /// chunk frees are free for the chunk after the next, and at most a few
    /// chunks' pages are in flight. Each chunk's own rows written twice (copy
    /// on write of the tree's pages, and the staging or index pages it adds),
    /// for the larger chunk of the two kinds, four times over. A row's
    /// footprint is estimated as the file's size over its documents, which
    /// over-counts (the file holds the indexes too).
    fn margin_bytes(docs_rows: u64, file_bytes: u64) -> u64 {
        let per_row = file_bytes / docs_rows.max(1);
        let rekey = REKEY_CHUNK_ROWS as u64 * per_row * 2;
        let index = INDEX_CHUNK_BYTES as u64 * 2;
        4 * rekey.max(index)
    }
}

/// Plan the migration and refuse to start, writing nothing, if the free space
/// does not cover it (design 6.1 step 0). Called before `raise(5)`, so a
/// refusal of a store at `found` below 5 leaves a store the previous build
/// opens; a refusal of an interrupted migration (`found` 5) leaves it for this
/// build to resume once there is room.
///
/// **The figure** (design 6.1 step 0): drain-then-refill and move-then-delete
/// reuse the pages they free after each durable commit, so the file needs room
/// for at most one of redb's growth steps, which [`worst_growth`] computes from
/// the file's length, and a small working set, from the tables' own row counts
/// and the file's size, not from the document count times 80 bytes
/// `PartialRebuildPlan` used (which under-counted a multikey index). Every
/// figure is read without a walk: a preflight that read the store would cost a
/// cold store its size in seconds before saying a word.
pub(crate) fn preflight(db: &Database, path: &std::path::Path, found: u8) -> Result<Plan> {
    let plan = plan(db, path)?;
    let free = free_bytes(path);
    info!(
        collections = plan.collections,
        ranges = plan.ranges,
        purge_pending_ranges = plan.purge_pending,
        documents_and_tombstones = plan.docs_rows,
        indexes = plan.indexes,
        index_entries = plan.index_entries,
        file_mib = plan.file_bytes >> 20,
        growth_mib = plan.growth_bytes >> 20,
        required_free_mib = plan.required_bytes >> 20,
        free_mib = free.map(|f| f >> 20),
        estimate_secs = estimate_secs(&plan),
        "storage schema 4 -> 5 (exact number keys): every document with a numeric _id is re-keyed \
         and every index is drained and rebuilt before this node serves anything; the estimate \
         is for the migration alone, and the first start after it also walks the oplog once"
    );
    match free {
        Some(free) if free < plan.required_bytes => {
            let state = if found < crate::migrate::SCHEMA_VERSION {
                format!(
                    "Nothing was written: the store is still schema {found}, and the previous \
                     build opens it"
                )
            } else {
                "Nothing more was written: the interrupted migration is where it stopped, and \
                 only this build or a later one opens the store"
                    .to_string()
            };
            Err(StorageError::NotEnoughSpace {
                needed: plan.required_bytes,
                free,
                detail: format!(
                    "beside {} ({} MiB): at most {} MiB for one of redb's growth steps from that \
                     size, and {} MiB of working set. {state}. Free space, or grow the volume, \
                     and start again",
                    path.display(),
                    plan.file_bytes >> 20,
                    plan.growth_bytes >> 20,
                    (plan.required_bytes - plan.growth_bytes) >> 20,
                ),
            })
        }
        Some(_) => Ok(plan),
        None => {
            warn!(
                path = %path.display(),
                required_free_mib = plan.required_bytes >> 20,
                "could not read the free space beside the store; migrating without the check"
            );
            Ok(plan)
        }
    }
}

/// The plan, read in one transaction with no walk: redb's row counts, one seek
/// per collection-id range, and the collection registry.
fn plan(db: &Database, path: &std::path::Path) -> Result<Plan> {
    let txn = db.begin_read()?;
    let collections = txn.open_table(tables::COLLECTIONS)?;
    let docs = txn.open_table(tables::DOCS)?;
    let entries = txn.open_table(tables::INDEX_ENTRIES)?;
    let mut plan = Plan::default();
    let mut standing = std::collections::HashSet::new();
    for row in collections.iter()? {
        let (_, value) = row?;
        let meta: CollectionMeta = serde_json::from_slice(value.value())?;
        plan.collections += 1;
        plan.indexes += meta.indexes.len() as u64;
        standing.insert(meta.id.0);
    }
    for id in range_ids(&docs)? {
        plan.ranges += 1;
        plan.purge_pending += u64::from(!standing.contains(&id));
    }
    plan.docs_rows = docs.len()?;
    plan.index_entries = entries.len()?;
    plan.file_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    plan.growth_bytes = worst_growth(plan.file_bytes);
    plan.required_bytes = plan.growth_bytes + Plan::margin_bytes(plan.docs_rows, plan.file_bytes);
    Ok(plan)
}

/// Seconds the migration is expected to take, from the measured per-row costs
/// ([`crate::migrate::MICROS_PER_DOCUMENT`] per document per index, and
/// [`MICROS_PER_REKEYED_ROW`] per row re-keyed).
fn estimate_secs(plan: &Plan) -> u64 {
    let per_index = plan.docs_rows * plan.indexes.min(64) / plan.collections.max(1);
    (plan.docs_rows * MICROS_PER_REKEYED_ROW + per_index * crate::migrate::MICROS_PER_DOCUMENT)
        / 1_000_000
}

/// The cost of re-keying one row, both phases, the up-front estimate is made
/// from: a bound, as [`crate::migrate::MICROS_PER_DOCUMENT`] is. Measured with
/// `examples/schema4_migration` on a laptop's SSD, warm, in a release build:
/// both phases took 15 to 31 s for 2,000,000 rows of about 350 bytes, 8 to 16
/// us each, bound by the durable commit of each chunk; a spinning disk is
/// slower, which is why the line says "estimate".
pub const MICROS_PER_REKEYED_ROW: u64 = 16;

/// Every collection id `DOCS` holds a row under, in order: one seek per id,
/// skipping from range to range, so a store of few large collections is a
/// handful of reads however many rows it holds.
fn range_ids<T>(docs: &T) -> Result<Vec<u64>>
where
    T: ReadableTable<(u64, &'static [u8]), &'static [u8]>,
{
    let mut ids = Vec::new();
    let mut from = Some(0u64);
    while let Some(start) = from {
        let Some(row) = docs.range::<(u64, &[u8])>((start, [].as_slice())..)?.next().transpose()?
        else {
            break;
        };
        let id = row.0.value().0;
        ids.push(id);
        from = id.checked_add(1);
    }
    Ok(ids)
}

/// Whether `id`'s range holds a numeric key: one seek.
fn has_numeric<T>(docs: &T, id: u64) -> Result<bool>
where
    T: ReadableTable<(u64, &'static [u8]), &'static [u8]>,
{
    Ok(docs.range::<(u64, &[u8])>((id, NUMBER_TAG)..(id, ABOVE_NUMBER_TAG))?.next().is_some())
}

/// The free space on the volume holding `path`, as an unprivileged writer may
/// use it; `None` if it cannot be read.
fn free_bytes(path: &std::path::Path) -> Option<u64> {
    #[cfg(test)]
    if let Some(free) = hooks::FREE.with(std::cell::Cell::get) {
        return Some(free);
    }
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => std::path::Path::new("."),
    };
    volume_free_bytes(dir)
}

#[cfg(unix)]
fn volume_free_bytes(dir: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    // SAFETY: `c_path` is a valid NUL-terminated string and `stat` a properly
    // sized, writable `statvfs` the call fills in.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return None;
    }
    #[allow(clippy::unnecessary_cast)]
    Some((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

#[cfg(not(unix))]
fn volume_free_bytes(_dir: &std::path::Path) -> Option<u64> {
    None
}

/// The first commit (design 6.1 step 1): schema 5, the 3 -> 4 residue deleted
/// and its unreported collisions carried, this module's tables and every
/// marker, **all in one transaction**, made immediately after `raise(5)`.
///
/// One marker per `DOCS` collection-id range, whether or not a collection
/// stands over the id: a drop's rows still being purged are re-keyed like any
/// other. A range with no numeric key has nothing to re-key and is marked done
/// here, in the transaction that looked; every other starts `rekeying`. One
/// marker per index of every collection, `pending`.
pub(crate) fn begin(db: &Database, found: u8) -> Result<()> {
    let txn = db.begin_write()?;
    let held: Vec<String> = txn.list_tables()?.map(|t| t.name().to_string()).collect();
    for table in [DOCS_REKEYED.name(), RANGE_PHASES.name(), INDEX_STEPS.name()] {
        if held.iter().any(|t| t == table) {
            return Err(StorageError::Corrupt(format!(
                "the store says schema 4, and holds the schema 5 migration's table {table}: a \
                 schema 5 migration's first commit writes both, so this store was changed by \
                 something else; refusing rather than guessing which rows are in which form"
            )));
        }
    }
    {
        txn.open_table(tables::META)?
            .insert(tables::META_FORMAT_VERSION, [crate::migrate::SCHEMA_VERSION].as_slice())?;
        // The 3 -> 4 markers, finished migration or not: read first, since an
        // interrupted 3 -> 4 rebuild left the partial indexes it had not reached
        // under the older rule, with no record of what ADR-185 would select.
        let rebuilt: Option<std::collections::HashSet<(u64, u32)>> =
            if held.iter().any(|t| t == PARTIAL_REBUILT.name()) {
                Some(
                    txn.open_table(PARTIAL_REBUILT)?
                        .iter()?
                        .map(|row| Ok(row?.0.value()))
                        .collect::<Result<_>>()?,
                )
            } else {
                None
            };
        match txn.delete_table(PARTIAL_REBUILT) {
            Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(e) => return Err(e.into()),
        }
        let mut collisions = txn.open_table(COLLISIONS)?;
        carry_partial_rebuilt_violations(&txn, &mut collisions)?;
        let _ = txn.open_table(DOCS_REKEYED)?;
        let docs = txn.open_table(tables::DOCS)?;
        let mut phases = txn.open_table(RANGE_PHASES)?;
        for id in range_ids(&docs)? {
            let phase = if has_numeric(&docs, id)? { Phase::Rekeying } else { Phase::Done };
            phases.insert(id, RangeMark::new(phase).encode().as_slice())?;
        }
        let mut steps = txn.open_table(INDEX_STEPS)?;
        for row in txn.open_table(tables::COLLECTIONS)?.iter()? {
            let (_, value) = row?;
            let meta: CollectionMeta = serde_json::from_slice(value.value())?;
            for index in &meta.indexes {
                let mut mark = IndexMark::pending();
                // A TTL partial index never filed under ADR-185's rule has no
                // previous selection to count a change against.
                let filed_under_185 = found >= 4
                    && rebuilt.as_ref().is_none_or(|done| done.contains(&(meta.id.0, index.id)));
                if index.is_ttl() && index.partial_filter.is_some() && !filed_under_185 {
                    mark.ttl = TtlCount::NotRecorded;
                }
                steps.insert((meta.id.0, index.id), mark.encode().as_slice())?;
            }
        }
    }
    commit(txn)
}

/// Move the schema 3 -> 4 rebuild's unreported collisions into [`COLLISIONS`],
/// their holders converted to schema 5 keys, and delete their table (design
/// 6.1 step 1, review v2.2 M1).
///
/// Carried, not deleted: they exist exactly in the window between that
/// rebuild's last commit and its report, and the invariant they serve is that a
/// rebuilt index's collisions are never lost. Converted here, while every
/// holder key is still genuinely schema 4, where [`kimmy_core::legacy_key::decode_v1`]
/// is exact: left as they were, they would be compared with the refill's
/// schema 5 holders and never match, or, by the pair the module documentation
/// names, name another document. The index key is kept as recorded and the row
/// marked carried; the last commit settles it against what the refill found
/// ([`settle_carried`]).
fn carry_partial_rebuilt_violations(
    txn: &WriteTransaction,
    collisions: &mut redb::Table<'_, (u64, u32, &'static [u8]), &'static [u8]>,
) -> Result<()> {
    let recorded: Vec<((u64, u32), Vec<u8>)> = match txn.open_table(PARTIAL_REBUILT_VIOLATIONS) {
        Ok(table) => table
            .iter()?
            .map(|row| {
                let (key, value) = row?;
                Ok((key.value(), value.value().to_vec()))
            })
            .collect::<Result<_>>()?,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for ((coll, index), value) in recorded {
        let held: crate::migrate::RecordedViolations =
            serde_json::from_slice(&value).map_err(|e| {
                StorageError::Corrupt(format!(
                    "a pending row of the schema 3 -> 4 rebuild's recorded collisions \
                     (partial_rebuilt_unique_violations) will not decode, so the schema 5 \
                     migration cannot carry a collision that rebuild found: {e}"
                ))
            })?;
        for violation in held.violations {
            let holders = violation
                .holders
                .iter()
                .map(|holder| stored_of_v1(holder))
                .collect::<Result<Vec<_>>>()?;
            let key = (coll, index, violation.key.as_slice());
            let mut row = match collisions.get(key)? {
                Some(raw) => Collision::decode(raw.value())?,
                None => Collision { carried: true, holders: Vec::new() },
            };
            row.hold(holders);
            collisions.insert(key, row.encode().as_slice())?;
        }
    }
    match txn.delete_table(PARTIAL_REBUILT_VIOLATIONS) {
        Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// The schema 5 key of a schema 4 document key: a numeric key decoded strictly
/// and encoded by the live encoder, any other key as it is (no number inside,
/// so byte-identical in both schemas). A numeric key that is not the exact
/// schema 4 key of an `Int64` is refused: no build wrote it, and reading it as
/// the nearest id would move a document onto another's key.
fn stored_of_v1(old: &[u8]) -> Result<Vec<u8>> {
    if !old.starts_with(NUMBER_TAG) {
        return Ok(old.to_vec());
    }
    let id = kimmy_core::legacy_key::decode_v1(old).ok_or_else(|| {
        StorageError::Corrupt(format!(
            "a numeric document key {old:02x?} is not the schema 4 key of any Int64 id, so the \
             schema 5 migration cannot tell which document it is; refusing rather than \
             re-keying it as a guess"
        ))
    })?;
    Ok(kimmy_core::keyenc::encode(&Bson::Int64(id))?)
}

/// Where a migration run is, for its progress lines, its timings and its commits.
struct Run {
    started: Instant,
    /// Rows moved out, rows moved back, entries drained, documents refilled.
    rows: [u64; 4],
    /// Time spent in each of those four kinds of chunk.
    spent: [Duration; 4],
    /// The step `/readyz` reports (a range's move out, its move back, an
    /// index's drain, a collection's refill): its kind, how far it is, its
    /// total, whether that total is exact, and where the step began.
    kind: Kind,
    done: u64,
    total: u64,
    exact: bool,
    began: (u64, u64),
    /// Each collection id's live documents, as the kept counts say: the
    /// estimate a document step's total starts from.
    documents: HashMap<u64, u64>,
    /// What a document step's total starts from where a collection has no
    /// kept count.
    fallback: u64,
}

/// The four kinds of chunk, as [`Run::rows`] and [`Run::spent`] index them.
#[derive(Clone, Copy, Debug)]
enum Kind {
    RekeyA = 0,
    RekeyB = 1,
    Drain = 2,
    Refill = 3,
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Kind::RekeyA => "re-keying",
            Kind::RekeyB => "moving back",
            Kind::Drain => "draining an index",
            Kind::Refill => "refilling indexes",
        }
    }
}

impl Kind {
    /// What `/readyz` calls a step of this kind.
    fn phase(self) -> crate::open_progress::OpenPhase {
        use crate::open_progress::OpenPhase;
        match self {
            Kind::RekeyA | Kind::RekeyB => OpenPhase::Migrating,
            Kind::Drain => OpenPhase::Draining,
            Kind::Refill => OpenPhase::Refilling,
        }
    }
}

impl Run {
    /// The live documents of collection `id`, the estimate a document step's
    /// total starts from: read from the kept counts, so with no walk.
    fn documents_of(&self, id: u64) -> u64 {
        self.documents.get(&id).copied().unwrap_or(self.fallback)
    }

    /// Begin a step `/readyz` reports on its own: its phase, its `total` in the
    /// step's own unit, `exact` or an estimate, and how far an earlier run took
    /// it.
    fn step(&mut self, kind: Kind, total: u64, done: u64, exact: bool) {
        self.kind = kind;
        self.exact = exact;
        self.done = done;
        self.total = total.max(self.floor());
        self.began = (self.done, self.total);
        crate::open_progress::set_open_phase_of(kind.phase(), self.total);
        crate::open_progress::set_open_done(self.done);
    }

    /// The least total the step can show: `done` for an exact total, one more
    /// for an estimate, so an estimated step never reads as finished before
    /// it is.
    fn floor(&self) -> u64 {
        if self.exact { self.done } else { self.done + 1 }
    }

    /// A step done: its total is what it did, so `/readyz` reads `done ==
    /// total` from the step's last commit to the next step's start. For an
    /// exact total that is the total the step began with; an estimate is
    /// lowered or raised to it here.
    fn end_step(&mut self, coll: &str, index: Option<&str>) {
        self.total = self.done;
        crate::open_progress::set_open_total(self.total);
        #[cfg(test)]
        hooks::step_ended(hooks::StepSeen {
            kind: self.kind.label(),
            collection: coll.to_owned(),
            index: index.map(str::to_owned),
            exact: self.exact,
            began: self.began,
            done: self.done,
            total: self.total,
        });
        #[cfg(not(test))]
        let _ = (coll, index);
    }

    /// Count `n` rows of `kind`, `spent` on them, in the step's progress, and
    /// say so every [`PROGRESS_EVERY`] rows of the step, with the figures
    /// `/readyz` gives. An estimated total the step has outrun is raised to
    /// stay ahead of it, so `done` keeps moving and never reads as finished
    /// before the step is.
    fn advance(&mut self, kind: Kind, n: u64, spent: Duration, coll: &str, index: Option<&str>) {
        self.rows[kind as usize] += n;
        self.spent[kind as usize] += spent;
        let before = self.done;
        self.done += n;
        if self.total < self.floor() {
            self.total = self.floor();
            crate::open_progress::set_open_total(self.total);
        }
        crate::open_progress::set_open_done(self.done);
        #[cfg(test)]
        hooks::chunk_done(kind.label(), coll, index, n);
        if before / PROGRESS_EVERY != self.done / PROGRESS_EVERY {
            #[cfg(test)]
            hooks::progress();
            info!(
                phase = kind.phase().label(),
                step = kind.label(),
                collection = coll,
                index,
                done = self.done,
                total = self.total,
                elapsed_ms = self.started.elapsed().as_millis() as u64,
                "migrating storage schema 4 -> 5"
            );
        }
    }
}

/// Commit one chunk, durably, and nothing else may: the one place a test can
/// make a commit fail as a kill before it would.
///
/// **Every chunk is durable**, though each is a resumable state on its own and
/// a lost one would only be redone. Measured on redb 4.3 (the measurement
/// harness at the end of `migrate_v5_tests`): with chunks committed
/// non-durably and one in every two seconds durably, the durable commit that
/// follows a run of non-durable ones took 4.4 to 9.5 s for 100,000 rows, the
/// re-key took seven times as long as with every commit durable, and the file
/// doubled, since the pages a non-durable commit frees are not reused until a
/// durable one. Durable, the file did not grow at all.
fn commit(txn: WriteTransaction) -> Result<()> {
    #[cfg(test)]
    if hooks::fails_now() {
        drop(txn);
        return Err(StorageError::Database("a failure injected mid-migration".into()));
    }
    txn.commit()?;
    #[cfg(test)]
    hooks::committed();
    Ok(())
}

/// Whether a stop has been asked for: looked for before every chunk, inside a
/// range and inside an index, so a stop waits for at most one chunk.
fn stop_requested() -> bool {
    #[cfg(test)]
    if hooks::stop_signalled() {
        return true;
    }
    crate::open_progress::open_stop_requested()
}

/// End the open at a stop: every chunk committed so far is durable, and the
/// next start resumes from their markers.
fn stopped() -> Result<()> {
    info!(
        "a stop ended the storage schema 4 -> 5 migration between two chunks; the next start resumes it"
    );
    Err(StorageError::Stopping(crate::StopReason::Shutdown))
}

/// Run the migration from its markers to the end: every range re-keyed, every
/// index rebuilt, then [`finish`]. Resumes wherever an earlier run stopped,
/// since every step reads its state from its marker.
pub(crate) fn run(db: &Database, plan: &Plan) -> Result<()> {
    let documents: HashMap<u64, u64> = {
        let txn = db.begin_read()?;
        txn.open_table(tables::LIVE_COUNTS)?
            .iter()?
            .map(|row| {
                let (id, n) = row?;
                Ok((id.value(), n.value()))
            })
            .collect::<Result<_>>()?
    };
    let mut run = Run {
        started: Instant::now(),
        rows: [0; 4],
        spent: [Duration::ZERO; 4],
        kind: Kind::RekeyA,
        done: 0,
        total: 0,
        exact: true,
        began: (0, 0),
        documents,
        fallback: plan.docs_rows / plan.ranges.max(1),
    };
    crate::open_progress::set_open_phase(crate::open_progress::OpenPhase::Migrating);
    let (ranges, collections, steps) = {
        let txn = db.begin_read()?;
        let ranges: Vec<u64> = txn
            .open_table(RANGE_PHASES)?
            .iter()?
            .map(|row| Ok(row?.0.value()))
            .collect::<Result<_>>()?;
        let mut collections = HashMap::new();
        for row in txn.open_table(tables::COLLECTIONS)?.iter()? {
            let (_, value) = row?;
            let meta: CollectionMeta = serde_json::from_slice(value.value())?;
            collections.insert(meta.id.0, meta);
        }
        let mut steps: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        for row in txn.open_table(INDEX_STEPS)?.iter()? {
            let (coll, index) = row?.0.value();
            steps.entry(coll).or_default().push(index);
        }
        (ranges, collections, steps)
    };
    // Each collection's range first, then its indexes, one collection at a time,
    // so the refill reads documents the re-key has just written.
    let mut ids: Vec<u64> = ranges.iter().copied().chain(steps.keys().copied()).collect();
    ids.sort_unstable();
    ids.dedup();
    for id in ids {
        let meta = collections.get(&id);
        let name = meta.map_or_else(|| format!("(dropped, id {id})"), |m| m.name.clone());
        if ranges.contains(&id) {
            rekey_range(db, &mut run, id, &name)?;
        }
        let Some(indexes) = steps.get(&id) else { continue };
        let Some(meta) = meta else {
            return Err(StorageError::Corrupt(format!(
                "the schema 5 migration holds index steps for collection id {id}, which no \
                 collection stands over; definitions do not change while it runs"
            )));
        };
        rebuild_indexes(db, &mut run, meta, indexes)?;
    }
    finish(db)?;
    let ms = |kind: Kind| run.spent[kind as usize].as_millis() as u64;
    info!(
        rows_rekeyed = run.rows[Kind::RekeyA as usize],
        rows_moved_back = run.rows[Kind::RekeyB as usize],
        entries_drained = run.rows[Kind::Drain as usize],
        documents_refilled = run.rows[Kind::Refill as usize],
        rekey_ms = ms(Kind::RekeyA),
        move_back_ms = ms(Kind::RekeyB),
        drain_ms = ms(Kind::Drain),
        refill_ms = ms(Kind::Refill),
        elapsed_ms = run.started.elapsed().as_millis() as u64,
        "migrated storage schema 4 -> 5"
    );
    #[cfg(test)]
    hooks::record_timings(run.spent, run.rows);
    Ok(())
}

/// Re-key one `DOCS` collection-id range, from its marker to `done`.
fn rekey_range(db: &Database, run: &mut Run, id: u64, name: &str) -> Result<()> {
    // Each phase is its own step on `/readyz`, counted in rows (documents and
    // tombstones): moving out against the collection's live documents as the
    // kept counts say (an estimate: tombstones move too, and a document with
    // a key that is not a number does not move), finalized when the phase
    // ends; moving back against what moved out (exact).
    let mut stepping = None;
    loop {
        if stop_requested() {
            return stopped();
        }
        let started = Instant::now();
        let txn = db.begin_write()?;
        let mark = read_range(&txn, id)?;
        if stepping != Some(mark.phase) {
            match mark.phase {
                Phase::Rekeying => run.step(Kind::RekeyA, run.documents_of(id), mark.out, false),
                Phase::MovingBack => run.step(Kind::RekeyB, mark.out, mark.back, true),
                Phase::Done => {}
            }
            stepping = Some(mark.phase);
        }
        let (next, kind, moved) = match mark.phase {
            Phase::Done => return Ok(()),
            Phase::Rekeying => {
                let (moved, more) = rekey_chunk(&txn, id)?;
                // The commit that drains the range sets the next phase, so a
                // range is never `rekeying` once a staged row could be back in
                // `DOCS`. A range that staged nothing has nothing to move back.
                let next = if more {
                    Phase::Rekeying
                } else if staged_is_empty(&txn, id)? {
                    Phase::Done
                } else {
                    Phase::MovingBack
                };
                (next, Kind::RekeyA, moved)
            }
            Phase::MovingBack => {
                let (moved, more) = move_back_chunk(&txn, id)?;
                (if more { Phase::MovingBack } else { Phase::Done }, Kind::RekeyB, moved)
            }
        };
        let (out, back) = match kind {
            Kind::RekeyA => (mark.out + moved as u64, mark.back),
            _ => (mark.out, mark.back + moved as u64),
        };
        txn.open_table(RANGE_PHASES)?
            .insert(id, RangeMark { phase: next, out, back }.encode().as_slice())?;
        #[cfg(test)]
        hooks::hold(kind.label());
        commit(txn)?;
        run.advance(kind, moved as u64, started.elapsed(), name, None);
        if next != mark.phase {
            run.end_step(name, None);
        }
        if next == Phase::Done {
            info!(
                collection = name,
                collection_id = id,
                rows = out,
                "re-keyed a collection's documents for storage schema 5"
            );
            crate::open_progress::test_step_pause();
            return Ok(());
        }
    }
}

fn read_range(txn: &WriteTransaction, id: u64) -> Result<RangeMark> {
    let phases = txn.open_table(RANGE_PHASES)?;
    let raw = phases.get(id)?.ok_or_else(|| corrupt_marker("a range's phase"))?;
    RangeMark::decode(raw.value())
}

fn staged_is_empty(txn: &WriteTransaction, id: u64) -> Result<bool> {
    let staged = txn.open_table(DOCS_REKEYED)?;
    Ok(staged.range(crate::engine::doc_range(CollectionId(id)))?.next().is_none())
}

/// Phase `rekeying`: up to a chunk of the range's old-form rows, each moved to
/// the staging table under its new key and deleted from `DOCS`. Answers how many
/// it moved and whether any are left.
///
/// No cursor: the rows moved are gone from `DOCS`, so the range's numeric part
/// is exactly what is left to do, and a resumed run starts at its front.
fn rekey_chunk(txn: &WriteTransaction, id: u64) -> Result<(usize, bool)> {
    let mut docs = txn.open_table(tables::DOCS)?;
    let mut staged = txn.open_table(DOCS_REKEYED)?;
    let (rows, more) = {
        let mut rows = Vec::with_capacity(chunk_rows());
        let mut range = docs.range::<(u64, &[u8])>((id, NUMBER_TAG)..(id, ABOVE_NUMBER_TAG))?;
        for row in range.by_ref().take(chunk_rows()) {
            let (key, value) = row?;
            rows.push((key.value().1.to_vec(), value.value().to_vec()));
        }
        (rows, range.next().is_some())
    };
    for (old, value) in &rows {
        let new = stored_of_v1(old)?;
        if staged.insert((id, new.as_slice()), value.as_slice())?.is_some() {
            return Err(StorageError::Corrupt(format!(
                "two schema 4 keys of collection id {id} re-key to {new:02x?}; refusing rather \
                 than keeping one of the two documents"
            )));
        }
        docs.remove((id, old.as_slice()))?;
    }
    Ok((rows.len(), more))
}

/// Phase `moving_back`: up to a chunk of staged rows moved into `DOCS`, each
/// deleted from the staging table in the same transaction. `DOCS` holds no
/// old-form numeric row of this id now, so a row that is already there is a
/// broken I-DOCS, refused rather than overwritten.
fn move_back_chunk(txn: &WriteTransaction, id: u64) -> Result<(usize, bool)> {
    let mut docs = txn.open_table(tables::DOCS)?;
    let mut staged = txn.open_table(DOCS_REKEYED)?;
    let (rows, more) = {
        let mut rows = Vec::with_capacity(chunk_rows());
        let mut range = staged.range(crate::engine::doc_range(CollectionId(id)))?;
        for row in range.by_ref().take(chunk_rows()) {
            let (key, value) = row?;
            rows.push((key.value().1.to_vec(), value.value().to_vec()));
        }
        (rows, range.next().is_some())
    };
    for (key, value) in &rows {
        if docs.insert((id, key.as_slice()), value.as_slice())?.is_some() {
            return Err(StorageError::Corrupt(format!(
                "moving a re-keyed row of collection id {id} back found {key:02x?} already in \
                 docs: the range holds both forms of a key (I-DOCS), refusing to overwrite"
            )));
        }
        staged.remove((id, key.as_slice()))?;
    }
    Ok((rows.len(), more))
}

fn chunk_rows() -> usize {
    #[cfg(test)]
    if let Some(rows) = hooks::REKEY_ROWS.with(std::cell::Cell::get) {
        return rows;
    }
    REKEY_CHUNK_ROWS
}

fn chunk_bytes() -> usize {
    #[cfg(test)]
    if let Some(bytes) = hooks::INDEX_BYTES.with(std::cell::Cell::get) {
        return bytes;
    }
    INDEX_CHUNK_BYTES
}

fn write_index(txn: &WriteTransaction, coll: u64, index: u32, mark: &IndexMark) -> Result<()> {
    txn.open_table(INDEX_STEPS)?.insert((coll, index), mark.encode().as_slice())?;
    Ok(())
}

/// Rebuild every index of one collection whose range is done (design 6.2): each
/// drained in turn, then all refilled together in one walk of the documents.
fn rebuild_indexes(db: &Database, run: &mut Run, meta: &CollectionMeta, ids: &[u32]) -> Result<()> {
    let mut indexes = Vec::with_capacity(ids.len());
    for &id in ids {
        let index = meta.index_by_id(id).ok_or_else(|| {
            StorageError::Corrupt(format!(
                "the schema 5 migration holds a step for index id {id} of {}.{}, which has no \
                 such index; definitions do not change while it runs",
                meta.db, meta.name
            ))
        })?;
        indexes.push(index.clone());
    }
    for index in &indexes {
        drain(db, run, meta, index)?;
    }
    refill(db, run, meta, &mut indexes)
}

/// Empty one index's range, a chunk per transaction; the commit that empties it
/// moves the index to `refilling`. A TTL index's selection changes are counted
/// first, while its old entries are there to be read.
fn drain(db: &Database, run: &mut Run, meta: &CollectionMeta, index: &IndexMeta) -> Result<()> {
    let coll = meta.id.0;
    // Its own step on `/readyz`, in this index's entries: removed against the
    // entries it held, counted before its first chunk (exact, whether the
    // index is partial and holds a handful or multikey and holds many per
    // document).
    let mut stepping = false;
    loop {
        if stop_requested() {
            return stopped();
        }
        let mut mark = {
            let txn = db.begin_read()?;
            let steps = txn.open_table(INDEX_STEPS)?;
            let raw =
                steps.get((coll, index.id))?.ok_or_else(|| corrupt_marker("an index's step"))?;
            IndexMark::decode(raw.value())?
        };
        if !matches!(mark.step, Step::Pending | Step::Draining) {
            return Ok(());
        }
        if !stepping {
            let left = entries_left(db, meta, index)?;
            run.step(Kind::Drain, mark.drained + left, mark.drained, true);
            stepping = true;
        }
        // A TTL index's count, once, before its first chunk: the entries the
        // drain removes are the previous build's selection.
        let counted = if mark.step == Step::Pending && index.is_ttl() {
            Some(match mark.ttl {
                TtlCount::NotRecorded => TtlCount::NotRecorded,
                _ => {
                    let (now_selected, now_unselected) = ttl_selection_changes(db, meta, index)?;
                    TtlCount::Counted(now_selected, now_unselected)
                }
            })
        } else {
            None
        };
        let started = Instant::now();
        let txn = db.begin_write()?;
        let (removed, empty) = {
            let mut entries =
                open_walk_table_in(&txn, tables::INDEX_ENTRIES, WalkStop::in_write())?;
            crate::index::clear_index_entries_upto(
                &mut entries,
                crate::index::index_id_range(meta.id, index.id),
                chunk_bytes(),
            )?
        };
        if let Some(counted) = counted {
            mark.ttl = counted;
        }
        mark.drained += removed as u64;
        mark.step = if empty { Step::Refilling { after: None } } else { Step::Draining };
        write_index(&txn, coll, index.id, &mark)?;
        #[cfg(test)]
        hooks::hold(Kind::Drain.label());
        commit(txn)?;
        run.advance(Kind::Drain, removed as u64, started.elapsed(), &meta.name, Some(&index.name));
        match counted {
            Some(TtlCount::Counted(now_selected, now_unselected)) => info!(
                db = %meta.db,
                collection = %meta.name,
                index = %index.name,
                now_selected,
                now_not_selected = now_unselected,
                "a TTL index's partial filter now decides documents holding a Decimal128 at its \
                 paths by exact value: this many documents it did not select are now selected, \
                 and so expire once due, and this many it selected no longer are (ADR-238)"
            ),
            Some(TtlCount::NotRecorded) => info!(
                db = %meta.db,
                collection = %meta.name,
                index = %index.name,
                before = "not recorded",
                "a TTL index's partial filter now decides documents holding a Decimal128 at its \
                 paths by exact value; what it selected before is not in this store, which was \
                 never filed under ADR-185's rule (it comes from schema 3), so the change is not \
                 counted (ADR-238)"
            ),
            _ => {}
        }
        if empty {
            run.end_step(&meta.name, Some(&index.name));
            return Ok(());
        }
    }
}

/// The entries still in one index's range: what its drain has left to remove,
/// the drain's exact total. One read of the range's keys (an entry has no
/// value), looking for a stop as often as a chunk would.
fn entries_left(db: &Database, meta: &CollectionMeta, index: &IndexMeta) -> Result<u64> {
    let txn = db.begin_read()?;
    let entries = txn.open_table(tables::INDEX_ENTRIES)?;
    let mut left = 0u64;
    for row in entries.range(crate::index::index_id_range(meta.id, index.id))? {
        row?;
        left += 1;
        if left.is_multiple_of(4_096) && stop_requested() {
            return Err(StorageError::Stopping(crate::StopReason::Shutdown));
        }
    }
    Ok(left)
}

/// For a TTL index, the documents whose selection by its partial filter changes
/// with the exact comparator: `(now selected, no longer selected)` (design 2.6,
/// 6.2). Read before the drain, in one read transaction, so the old entries are
/// the index as the previous build left it.
///
/// Only a document holding a `Decimal128` at a path the filter reads can change:
/// the comparator changed for nothing else. For such a document the previous
/// build's answer is in its entries: it filed a document holding a decimal at a
/// filter path that its filter did not select in the undecidable run (ADR-185),
/// and every other one in the index, so "selected before" is "not in that run",
/// looked up under the document's schema 4 key. "Selected now" is the filter's
/// answer under the current comparator. Generic: it reads the run the previous
/// build wrote and the filter's answer today, not how today's build files it,
/// which since design 2.6 is never in that run. This is the one reader of
/// `UNDECIDABLE` left in the product, and it reads a schema 4 store only.
///
/// The test is `a_ttl_partial_index_over_decimals_counts_and_expires_by_exact_value`
/// in `migrate_v5_tests`: on a schema 4 store whose TTL partial indexes 0.50
/// filed decimal-bearing documents undecidable, the line counts a document the
/// exact filter now selects in `now_selected` and the index holds it under its
/// date key, so the next expiry pass deletes it; a document 0.50 selected only
/// by ranking a decimal equal to the bound (a `$gte`) is counted in
/// `now_not_selected` and is no longer in the index at all; and a document
/// whose selection did not change is in neither count.
fn ttl_selection_changes(
    db: &Database,
    meta: &CollectionMeta,
    index: &IndexMeta,
) -> Result<(u64, u64)> {
    let Some(filter) = index.partial() else { return Ok((0, 0)) };
    let filter = filter?;
    let paths: Vec<String> = filter.predicates().map(|(path, _)| path.to_string()).collect();
    let txn = db.begin_read()?;
    let docs = txn.open_table(tables::DOCS)?;
    let entries = txn.open_table(tables::INDEX_ENTRIES)?;
    let (mut now_selected, mut now_unselected) = (0u64, 0u64);
    for (n, row) in docs.range(crate::engine::doc_range(meta.id))?.enumerate() {
        if n % 4_096 == 0 && stop_requested() {
            return Err(StorageError::Stopping(crate::StopReason::Shutdown));
        }
        let (key, value) = row?;
        let record = codec::decode_doc_record(value.value())?;
        let Some(doc) = record.document()? else { continue };
        let holds_decimal = paths.iter().any(|path| {
            kimmy_core::path::resolve(&doc, path).iter().any(|v| kimmy_core::holds_decimal128(v))
        });
        if !holds_decimal {
            continue;
        }
        let legacy = kimmy_core::legacy_key::legacy_of_stored(key.value().1);
        let was_selected =
            entries.get((meta.id.0, index.id, UNDECIDABLE, legacy.as_bytes()))?.is_none();
        match (was_selected, filter.selects(&doc)) {
            (false, true) => now_selected += 1,
            (true, false) => now_unselected += 1,
            _ => {}
        }
    }
    Ok((now_selected, now_unselected))
}

/// Refill every index of the collection still `refilling`, in one walk of its
/// documents in key order, a chunk of entries per transaction, each index's
/// cursor committed with the chunk it ends.
///
/// One walk for all of them, rather than one per index: each document is read
/// and parsed once whatever the number of indexes. An index whose cursor is
/// ahead of the walk (resumed at different points, which this module does not
/// produce but does not assume away) skips the documents it already holds.
fn refill(
    db: &Database,
    run: &mut Run,
    meta: &CollectionMeta,
    indexes: &mut [IndexMeta],
) -> Result<()> {
    let coll = meta.id.0;
    // Read once, where this run takes the refill up: from here the marks are
    // carried across chunks and written in each chunk's commit, so the store
    // always holds the cursor of the last chunk that committed.
    let mut marks = {
        let txn = db.begin_read()?;
        let steps = txn.open_table(INDEX_STEPS)?;
        indexes
            .iter()
            .map(|index| {
                let raw = steps
                    .get((coll, index.id))?
                    .ok_or_else(|| corrupt_marker("an index's step"))?;
                IndexMark::decode(raw.value())
            })
            .collect::<Result<Vec<_>>>()?
    };
    let active: Vec<usize> =
        (0..indexes.len()).filter(|&i| matches!(marks[i].step, Step::Refilling { .. })).collect();
    if active.is_empty() {
        return Ok(());
    }
    // Its own step on `/readyz`, one for all the collection's indexes, in rows
    // walked (documents and tombstones) against the collection's live
    // documents as the kept counts say: an estimate (tombstones are walked
    // too), raised if outrun and finalized when the walk ends. A resumed
    // refill counts from where it resumes.
    run.step(Kind::Refill, run.documents_of(coll), 0, false);
    loop {
        if stop_requested() {
            return stopped();
        }
        let started = Instant::now();
        let txn = db.begin_write()?;
        // The walk starts at the earliest cursor.
        let from: Option<Vec<u8>> = active
            .iter()
            .map(|&i| match &marks[i].step {
                Step::Refilling { after } => after.clone(),
                _ => unreachable!("only refilling indexes are active"),
            })
            .min_by(|a, b| match (a, b) {
                (None, _) => std::cmp::Ordering::Less,
                (_, None) => std::cmp::Ordering::Greater,
                (Some(a), Some(b)) => a.cmp(b),
            })
            .expect("one active index");
        let mut multikey: Vec<u32> = Vec::new();
        let (walked, last, exhausted) = {
            let docs = txn.open_table(tables::DOCS)?;
            let mut entries =
                open_walk_table_in(&txn, tables::INDEX_ENTRIES, WalkStop::in_write())?;
            let mut collisions = txn.open_table(COLLISIONS)?;
            let (mut bytes, mut walked) = (0usize, 0u64);
            let mut last: Option<Vec<u8>> = None;
            let mut exhausted = true;
            for row in docs.range(crate::engine::doc_range_after(meta.id, from.as_deref()))? {
                if bytes >= chunk_bytes() {
                    exhausted = false;
                    break;
                }
                let (key, value) = row?;
                let doc_key = key.value().1;
                walked += 1;
                last = Some(doc_key.to_vec());
                let record = codec::decode_doc_record(value.value())?;
                let Some(doc) = record.document()? else { continue };
                for &i in &active {
                    let index = &indexes[i];
                    let mark = &mut marks[i];
                    if let Step::Refilling { after: Some(after) } = &mark.step
                        && doc_key <= after.as_slice()
                    {
                        continue;
                    }
                    match crate::index::document_keys(index, &doc)? {
                        DocumentKeys::Keyed { keys, multikey: many } => {
                            if many && !index.multikey && !multikey.contains(&index.id) {
                                multikey.push(index.id);
                            }
                            for k in keys {
                                if index.unique {
                                    let held =
                                        crate::index::holders_of(&entries, meta.id, index.id, &k)?;
                                    let others: Vec<Vec<u8>> =
                                        held.into_iter().filter(|h| h != doc_key).collect();
                                    if !others.is_empty() {
                                        let row = (coll, index.id, k.as_slice());
                                        let mut found = match collisions.get(row)? {
                                            Some(raw) => Collision::decode(raw.value())?,
                                            None => {
                                                mark.collisions += 1;
                                                Collision::default()
                                            }
                                        };
                                        found.carried = false;
                                        found.hold(others.into_iter().chain([doc_key.to_vec()]));
                                        collisions.insert(row, found.encode().as_slice())?;
                                    }
                                }
                                bytes += 12 + k.len() + doc_key.len();
                                entries.insert((coll, index.id, k.as_slice(), doc_key), ())?;
                                mark.entries += 1;
                            }
                        }
                        DocumentKeys::Unkeyed { multikey: many, .. } => {
                            if many && !index.multikey && !multikey.contains(&index.id) {
                                multikey.push(index.id);
                            }
                            bytes += 12 + doc_key.len();
                            entries.insert((coll, index.id, UNKEYED, doc_key), ())?;
                            mark.unkeyed += 1;
                        }
                    }
                }
            }
            (walked, last, exhausted)
        };
        for &i in &active {
            let mark = &mut marks[i];
            mark.step = if exhausted {
                Step::Done
            } else {
                let after = match &mark.step {
                    Step::Refilling { after } => after.clone(),
                    _ => None,
                };
                Step::Refilling { after: last.clone().or(after) }
            };
            write_index(&txn, coll, indexes[i].id, mark)?;
        }
        // In the commit of the chunk that wrote the entry: a two-sided range
        // over an index that holds an array's keys and says it does not loses
        // rows, so the flag never lands later than its entries.
        crate::index::mark_multikey(&txn, &meta.db, &meta.name, &multikey)?;
        for index in indexes.iter_mut().filter(|i| multikey.contains(&i.id)) {
            index.multikey = true;
        }
        #[cfg(test)]
        hooks::hold(Kind::Refill.label());
        commit(txn)?;
        run.advance(Kind::Refill, walked, started.elapsed(), &meta.name, None);
        #[cfg(test)]
        hooks::refilled(walked);
        if exhausted {
            for &i in &active {
                let mark = &marks[i];
                info!(
                    db = %meta.db,
                    collection = %meta.name,
                    index = %indexes[i].name,
                    drained = mark.drained,
                    entries = mark.entries,
                    unkeyed = mark.unkeyed,
                    unique_collisions = mark.collisions,
                    "rebuilt an index for storage schema 5"
                );
            }
            run.end_step(&meta.name, None);
            crate::open_progress::test_step_pause();
            return Ok(());
        }
    }
}

/// The last commit (design 6.1 step 5): every marker and the staging table go,
/// the carried collisions are settled ([`settle_carried`]), and the collision
/// table goes too if that leaves nothing to report. The markers' absence is
/// what "finished" means, so this checks first that every range and every index
/// is done, and that the staging table is empty.
fn finish(db: &Database) -> Result<()> {
    let txn = db.begin_write()?;
    {
        for row in txn.open_table(RANGE_PHASES)?.iter()? {
            let (id, raw) = row?;
            if RangeMark::decode(raw.value())?.phase != Phase::Done {
                return Err(StorageError::Corrupt(format!(
                    "the schema 5 migration reached its end with collection id {} not re-keyed",
                    id.value()
                )));
            }
        }
        for row in txn.open_table(INDEX_STEPS)?.iter()? {
            let (key, raw) = row?;
            if IndexMark::decode(raw.value())?.step != Step::Done {
                let (coll, index) = key.value();
                return Err(StorageError::Corrupt(format!(
                    "the schema 5 migration reached its end with index {index} of collection id \
                     {coll} not rebuilt"
                )));
            }
        }
        if txn.open_table(DOCS_REKEYED)?.iter()?.next().is_some() {
            return Err(StorageError::Corrupt(
                "the schema 5 migration reached its end with re-keyed rows still staged".into(),
            ));
        }
    }
    let dissolved = settle_carried(&txn)?;
    txn.delete_table(RANGE_PHASES)?;
    txn.delete_table(INDEX_STEPS)?;
    txn.delete_table(DOCS_REKEYED)?;
    if txn.open_table(COLLISIONS)?.iter()?.next().is_none() {
        txn.delete_table(COLLISIONS)?;
    }
    commit(txn)?;
    for gone in dissolved {
        info!(
            db = %gone.db,
            collection = %gone.collection,
            index = %gone.index,
            holders = ?gone.holders,
            "a collision the schema 3 -> 4 rebuild recorded and never reported is not reported: \
             rebuilt from every document under this build's exact comparator, the index holds no \
             collision among these documents, so the record describes a state that no longer \
             exists; any collision it holds among some of them was found again and is reported \
             on its own (ADR-238)"
        );
    }
    Ok(())
}

/// A carried collision the rebuild did not find again, for its line.
struct Dissolved {
    db: String,
    collection: String,
    index: String,
    /// The holders' `_id`s, or a key's bytes where no document is left.
    holders: Vec<String>,
}

/// Settle every carried row (the schema 3 -> 4 rebuild's unreported
/// collisions), in the last commit, once every index has been refilled.
///
/// The refill checked every entry of every index under the final comparator,
/// so it has found every collision that exists. A carried row whose holders a
/// row the refill recorded in the same index includes is that collision found
/// again under the schema 5 form of its key: dropped, so it is reported once. A
/// carried row no refill row covers names documents that no longer collide
/// (the exact comparator dissolved it, as a decimal `$gte` that ranked equal to
/// every number does): dropped too, since reporting it would report a
/// violation that does not exist, and named in one line each. So a carried row
/// is never reported itself; what carrying it guarantees is that nothing it
/// recorded can be lost before this point.
fn settle_carried(txn: &WriteTransaction) -> Result<Vec<Dissolved>> {
    let mut collisions = txn.open_table(COLLISIONS)?;
    #[allow(clippy::type_complexity)]
    let mut by_index: BTreeMap<(u64, u32), Vec<(Vec<u8>, Collision)>> = BTreeMap::new();
    for row in collisions.iter()? {
        let (key, value) = row?;
        let (coll, index, k) = key.value();
        by_index
            .entry((coll, index))
            .or_default()
            .push((k.to_vec(), Collision::decode(value.value())?));
    }
    let mut metas = HashMap::new();
    for row in txn.open_table(tables::COLLECTIONS)?.iter()? {
        let (_, value) = row?;
        let meta: CollectionMeta = serde_json::from_slice(value.value())?;
        metas.insert(meta.id.0, meta);
    }
    let docs = txn.open_table(tables::DOCS)?;
    let mut dissolved = Vec::new();
    for ((coll, index), rows) in by_index {
        for (key, carried) in rows.iter().filter(|(_, c)| c.carried) {
            collisions.remove((coll, index, key.as_slice()))?;
            let found_again = rows
                .iter()
                .any(|(_, c)| !c.carried && carried.holders.iter().all(|h| c.holders.contains(h)));
            if found_again {
                continue;
            }
            let meta = metas.get(&coll);
            let holders = carried
                .holders
                .iter()
                .map(|holder| {
                    let id = docs
                        .get((coll, holder.as_slice()))?
                        .map(|raw| codec::decode_doc_record(raw.value()))
                        .transpose()?
                        .and_then(|record| record.document().ok().flatten())
                        .and_then(|doc| doc.get("_id").cloned())
                        .and_then(|id| kimmy_core::DocId::try_from_bson(&id).ok());
                    Ok(id.map_or_else(|| format!("{holder:02x?}"), |id| id.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            dissolved.push(Dissolved {
                db: meta.map_or_else(String::new, |m| m.db.clone()),
                collection: meta.map_or_else(|| format!("(id {coll})"), |m| m.name.clone()),
                index: meta
                    .and_then(|m| m.index_by_id(index))
                    .map_or_else(|| format!("(id {index})"), |i| i.name.clone()),
                holders,
            });
        }
    }
    Ok(dissolved)
}

/// Every collision the migration recorded and has not yet reported, by
/// collection, resolved against the metadata as it stands. Read on every open,
/// not only one that migrates: the table outlives the migration's last commit,
/// so a crash before the report leaves rows for the next open. Carried rows are
/// settled by the last commit ([`settle_carried`]) and none is left to read.
pub(crate) fn recorded_collisions(db: &Database) -> Result<crate::migrate::Unreported> {
    let txn = db.begin_read()?;
    let table = match txn.open_table(COLLISIONS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    // `(collection, index) -> (index key, what was recorded under it)`.
    #[allow(clippy::type_complexity)]
    let mut by_index: BTreeMap<(u64, u32), Vec<(Vec<u8>, Collision)>> = BTreeMap::new();
    for row in table.iter()? {
        let (key, value) = row?;
        let (coll, index, k) = key.value();
        by_index
            .entry((coll, index))
            .or_default()
            .push((k.to_vec(), Collision::decode(value.value())?));
    }
    if by_index.is_empty() {
        return Ok(Vec::new());
    }
    let mut metas = HashMap::new();
    for row in txn.open_table(tables::COLLECTIONS)?.iter()? {
        let (_, value) = row?;
        let meta: CollectionMeta = serde_json::from_slice(value.value())?;
        metas.insert(meta.id.0, meta);
    }
    let mut out: Vec<(CollectionMeta, Vec<crate::index::UniqueViolation>)> = Vec::new();
    for ((coll, index), rows) in by_index {
        // Gone since: nothing to report it against, and its entries went with it.
        let Some(meta) = metas.get(&coll) else { continue };
        let Some(index) = meta.index_by_id(index) else { continue };
        let violations: Vec<crate::index::UniqueViolation> = rows
            .iter()
            .filter(|(_, c)| !c.carried)
            .map(|(key, c)| crate::index::UniqueViolation {
                index: index.name.clone(),
                key: key.clone(),
                holders: c.holders.clone(),
            })
            .collect();
        if violations.is_empty() {
            continue;
        }
        match out.last_mut() {
            Some((last, held)) if last.id == meta.id => held.extend(violations),
            _ => out.push((meta.clone(), violations)),
        }
    }
    Ok(out)
}

/// Forget the recorded collisions once they are reported.
pub(crate) fn forget_collisions(db: &Database) -> Result<()> {
    let txn = db.begin_write()?;
    match txn.delete_table(COLLISIONS) {
        Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(e) => return Err(e.into()),
    }
    txn.commit()?;
    Ok(())
}

/// Test-only points in the migration: a commit that fails as a kill before it
/// would, a stop from another thread, a hold inside a chunk, the chunk sizes,
/// the free space, and what the run did.
#[cfg(test)]
pub(crate) mod hooks {
    use std::cell::{Cell, RefCell};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    thread_local! {
        static COMMITS: Cell<usize> = const { Cell::new(0) };
        static FAIL_AT: Cell<Option<usize>> = const { Cell::new(None) };
        static PROGRESS: Cell<usize> = const { Cell::new(0) };
        static REFILLED: Cell<u64> = const { Cell::new(0) };
        static LARGEST_REFILL: Cell<u64> = const { Cell::new(0) };
        static STOP: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
        static HOLD: Cell<Option<Duration>> = const { Cell::new(None) };
        static HELD: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
        static HOLDS: RefCell<std::collections::BTreeMap<&'static str, usize>> =
            const { RefCell::new(std::collections::BTreeMap::new()) };
        static STOP_AT: Cell<Option<(&'static str, usize)>> = const { Cell::new(None) };
        static RAISED: Cell<Option<(std::time::Instant, usize)>> = const { Cell::new(None) };
        pub(crate) static REKEY_ROWS: Cell<Option<usize>> = const { Cell::new(None) };
        pub(crate) static INDEX_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
        pub(crate) static FREE: Cell<Option<u64>> = const { Cell::new(None) };
        static TIMINGS: Cell<Option<([Duration; 4], [u64; 4])>> = const { Cell::new(None) };
        static WATCHED: RefCell<Option<(std::path::PathBuf, u64)>> = const { RefCell::new(None) };
        static CHUNKS: RefCell<Vec<ChunkSeen>> = const { RefCell::new(Vec::new()) };
        static STEPS: RefCell<Vec<StepSeen>> = const { RefCell::new(Vec::new()) };
    }

    /// Migration commits made on this thread so far.
    pub(crate) fn commits() -> usize {
        COMMITS.with(Cell::get)
    }

    /// Fail the `n`th migration commit from now (from 1) on this thread, as a
    /// kill just before it would.
    pub(crate) fn fail_at_commit(n: usize) {
        FAIL_AT.with(|f| f.set(Some(COMMITS.with(Cell::get) + n)));
    }

    pub(super) fn fails_now() -> bool {
        let next = COMMITS.with(Cell::get) + 1;
        FAIL_AT.with(|f| {
            if f.get() == Some(next) {
                f.set(None);
                true
            } else {
                false
            }
        })
    }

    pub(super) fn committed() {
        COMMITS.with(|c| c.set(c.get() + 1));
        WATCHED.with(|w| {
            if let Some((path, largest)) = w.borrow_mut().as_mut() {
                let len = std::fs::metadata(&*path).map_or(0, |m| m.len());
                *largest = (*largest).max(len);
            }
        });
    }

    /// Watch the length of the file at `path` after every migration commit on
    /// this thread, from its length now.
    pub(crate) fn watch_file(path: &std::path::Path) {
        let len = std::fs::metadata(path).map_or(0, |m| m.len());
        WATCHED.with(|w| *w.borrow_mut() = Some((path.to_path_buf(), len)));
    }

    /// The largest the watched file has been after a commit; stops watching.
    pub(crate) fn largest_file() -> Option<u64> {
        WATCHED.with(|w| w.borrow_mut().take().map(|(_, largest)| largest))
    }

    pub(super) fn progress() {
        PROGRESS.with(|p| p.set(p.get() + 1));
    }

    /// One chunk's progress as `/readyz` showed it after the chunk: its kind,
    /// collection, index, rows, and the open's phase, done and total then.
    #[derive(Clone, Debug)]
    pub(crate) struct ChunkSeen {
        pub(crate) kind: &'static str,
        pub(crate) collection: String,
        pub(crate) index: Option<String>,
        pub(crate) rows: u64,
        pub(crate) phase: Option<crate::open_progress::OpenPhase>,
        pub(crate) done: u64,
        pub(crate) total: u64,
    }

    pub(super) fn chunk_done(kind: &'static str, coll: &str, index: Option<&str>, rows: u64) {
        let (phase, total) = crate::open_progress::tests::THIS_THREAD_PHASE
            .with(Cell::get)
            .map_or((None, 0), |(p, t)| (Some(p), t));
        let done = crate::open_progress::tests::THIS_THREAD_DONE.with(Cell::get);
        CHUNKS.with(|c| {
            c.borrow_mut().push(ChunkSeen {
                kind,
                collection: coll.to_string(),
                index: index.map(str::to_string),
                rows,
                phase,
                done,
                total,
            })
        });
    }

    /// Every chunk's progress on this thread since the last call; clears it.
    pub(crate) fn chunks_seen() -> Vec<ChunkSeen> {
        CHUNKS.with(|c| std::mem::take(&mut *c.borrow_mut()))
    }

    /// One step's end: its kind, collection, index, whether its total was
    /// exact, `(done, total)` as it began, and `done` and `total` as `/readyz`
    /// showed them once it ended.
    #[derive(Clone, Debug)]
    pub(crate) struct StepSeen {
        pub(crate) kind: &'static str,
        pub(crate) collection: String,
        pub(crate) index: Option<String>,
        pub(crate) exact: bool,
        pub(crate) began: (u64, u64),
        pub(crate) done: u64,
        pub(crate) total: u64,
    }

    /// Record a step's end, with `/readyz`'s figures read back rather than the
    /// run's.
    pub(super) fn step_ended(mut seen: StepSeen) {
        seen.total = crate::open_progress::tests::THIS_THREAD_PHASE
            .with(Cell::get)
            .map_or(u64::MAX, |(_, total)| total);
        seen.done = crate::open_progress::tests::THIS_THREAD_DONE.with(Cell::get);
        STEPS.with(|s| s.borrow_mut().push(seen));
    }

    /// Every step's end on this thread since the last call; clears it.
    pub(crate) fn steps_seen() -> Vec<StepSeen> {
        STEPS.with(|s| std::mem::take(&mut *s.borrow_mut()))
    }

    /// Progress lines written on this thread so far.
    pub(crate) fn progress_lines() -> usize {
        PROGRESS.with(Cell::get)
    }

    pub(super) fn refilled(n: u64) {
        REFILLED.with(|r| r.set(r.get() + n));
        LARGEST_REFILL.with(|r| r.set(r.get().max(n)));
    }

    /// The most documents one refill chunk has walked on this thread.
    pub(crate) fn largest_refill_chunk() -> u64 {
        LARGEST_REFILL.with(Cell::get)
    }

    /// Documents the refill has walked on this thread so far.
    pub(crate) fn documents_refilled() -> u64 {
        REFILLED.with(Cell::get)
    }

    /// A stop another thread can raise for the migration this thread runs.
    pub(crate) fn stop_on(signal: Option<Arc<AtomicBool>>) {
        STOP.with(|s| *s.borrow_mut() = signal);
    }

    pub(super) fn stop_signalled() -> bool {
        RAISED.with(|r| r.get().is_some())
            || STOP.with(|s| s.borrow().as_ref().is_some_and(|s| s.load(Ordering::SeqCst)))
    }

    /// Hold every chunk's write transaction open for `hold` before its commit,
    /// raising `held` while it does: a writer held inside a chunk.
    pub(crate) fn hold_in_chunks(hold: Option<Duration>, held: Option<Arc<AtomicBool>>) {
        HOLD.with(|h| h.set(hold));
        HELD.with(|h| *h.borrow_mut() = held);
        HOLDS.with(|h| h.borrow_mut().clear());
        RAISED.with(|r| r.set(None));
    }

    /// Ask for a stop inside the `n`th chunk (from 1) of `kind`, while it holds
    /// the writer, before its commit: a stop that arrives mid-chunk.
    pub(crate) fn stop_inside(kind: &'static str, n: usize) {
        STOP_AT.with(|s| s.set(Some((kind, n))));
    }

    /// When the stop [`stop_inside`] asked for was raised, and the commits made
    /// by then; clears it.
    pub(crate) fn stop_raised() -> Option<(std::time::Instant, usize)> {
        RAISED.with(|r| r.take())
    }

    pub(super) fn hold(kind: &'static str) {
        let n = HOLDS.with(|h| {
            let mut h = h.borrow_mut();
            let n = h.entry(kind).or_insert(0);
            *n += 1;
            *n
        });
        if STOP_AT.with(|s| s.get()) == Some((kind, n)) {
            STOP_AT.with(|s| s.set(None));
            RAISED.with(|r| r.set(Some((std::time::Instant::now(), commits()))));
        }
        if let Some(hold) = HOLD.with(Cell::get) {
            HELD.with(|h| {
                if let Some(held) = h.borrow().as_ref() {
                    held.store(true, Ordering::SeqCst);
                }
            });
            std::thread::sleep(hold);
        }
    }

    pub(super) fn record_timings(spent: [Duration; 4], rows: [u64; 4]) {
        TIMINGS.with(|t| t.set(Some((spent, rows))));
    }

    /// The last run's time and rows per kind of chunk: moved out, moved back,
    /// drained, refilled.
    pub(crate) fn timings() -> Option<([Duration; 4], [u64; 4])> {
        TIMINGS.with(Cell::get)
    }
}
