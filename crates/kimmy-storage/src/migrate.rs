//! On-disk schema migrations.
//!
//! The record encoding ([`crate::codec::FORMAT_VERSION`]) and the database
//! *layout* are separate concerns and version separately. A record whose bytes
//! still decode does not need rewriting because the meaning of a key changed
//! around it — which is exactly the situation here.
//!
//! # Schema 2 — derived collection ids
//!
//! Collection ids used to come from a node-local counter. They are now derived
//! from `(database, name)`, so every node computes the same id for the same
//! collection ([`kimmy_core::CollectionId::derive`]).
//!
//! That renumbers every collection, and the id is embedded in three places:
//! document keys, index-entry keys, and the `collection` field of every oplog
//! entry. All three are rewritten here.
//!
//! Refusing to open instead would have been easier and is what the version
//! check does for a *newer* schema — but a database that can be migrated
//! should be, and a user with data has no other route forward.

use std::collections::HashMap;

use kimmy_core::CollectionId;
use redb::{Database, ReadableDatabase, ReadableTable};
use tracing::info;

use crate::codec;
use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;
use crate::tables;

/// The database layout this build writes and understands.
///
/// **4 (ADR-183) is the first bump that moves no bytes.** Schemas 2 and 3
/// renumbered collection and index ids; 4 leaves the layout byte-identical and
/// changes what a partial index's entries mean: its membership is now what
/// `find` selects with its filter. An older build would parse every byte and
/// maintain the index under its own rule, re-corrupting it on every write. So
/// the version guards "this build can correctly **maintain** this data", not
/// only "this build can parse it", and a build that cannot is refused here
/// rather than let loose on it.
pub const SCHEMA_VERSION: u8 = 4;

/// What a migration found that has to be reported once the engine exists:
/// the collisions a rebuilt unique partial index holds (ADR-183).
pub(crate) type Unreported = Vec<(CollectionMeta, Vec<crate::index::UniqueViolation>)>;

/// Bring a database up to [`SCHEMA_VERSION`], or refuse if it cannot be.
/// `raise` records in the store's sidecar that its schema is about to move
/// (ADR-190): called immediately before the first write of any
/// migration, and before a fresh store's version is written, so an older build
/// refuses a store whose migration has begun, and a migration that never
/// began leaves a rollback open.
pub(crate) fn run(db: &Database, raise: &dyn Fn(u8) -> Result<()>) -> Result<Unreported> {
    step(db, raise)?;
    // Read from the store, not carried out of the loop: this run's recorded
    // collisions and any an earlier interrupted run left unreported.
    persisted_violations(db)
}

/// Take the database to [`SCHEMA_VERSION`], or refuse if it cannot be.
fn step(db: &Database, raise: &dyn Fn(u8) -> Result<()>) -> Result<()> {
    let found = stored_version(db)?;

    // Before the migration writes anything, on every path that migrates --
    // including a schema 1 or 2 source, whose id-deriving steps run before the
    // rebuild, and a schema 4 file whose rebuild was interrupted. Any later and
    // the claim the refusal makes, that no version and no entry was changed,
    // would already be false. The open before this point has written, though
    // nothing the previous build reads differently: redb's repair of a dirty
    // file, the commit that ensures the tables exist, and the sidecar's redb
    // fields, with its schema left where it was (ADR-190). So "a refused start
    // writes nothing" holds for a newer store, refused before the open, and
    // not for this refusal of an older one. (What an operator can then do
    // depends on the version it found -- the previous build opens a schema 1,
    // 2 or 3 directory, and no build opens a half-migrated one; the message
    // says which.)
    let resuming = found == Some(SCHEMA_VERSION) && partial_rebuild_owed(db)?;
    if matches!(found, Some(1..SCHEMA_VERSION)) || resuming {
        refuse_unparseable_partial_filters(db, found.expect("a version was read"))?;
    }

    match found {
        // A fresh database: nothing to migrate, just stamp it.
        None => {
            raise(SCHEMA_VERSION)?;
            write_version(db, SCHEMA_VERSION)
        }
        // Schema 4 with markers still present is a migration interrupted
        // part-way, not a finished one: the version is written in the first
        // index's commit precisely so that an older build refuses a file in
        // this state, and the markers say which indexes are still owed. The
        // last commit deletes them, so their absence is what "finished" means.
        Some(SCHEMA_VERSION) if partial_rebuild_owed(db)? => {
            info!("resuming an interrupted storage schema 3 -> 4 migration (partial indexes)");
            raise(SCHEMA_VERSION)?;
            rebuild_partial_indexes(db)
        }
        Some(SCHEMA_VERSION) => Ok(()),
        // Migrations run in sequence, so a schema 1 database steps through 2
        // rather than needing its own path to the latest. The last step
        // writes the version itself, with its own bookkeeping.
        Some(1) => {
            raise(SCHEMA_VERSION)?;
            info!("migrating storage schema 1 -> 2 (derived collection ids)");
            derive_collection_ids(db)?;
            info!("migrating storage schema 2 -> 3 (derived index ids)");
            derive_index_ids(db)?;
            info!("migrating storage schema 3 -> 4 (partial index membership)");
            rebuild_partial_indexes(db)
        }
        Some(2) => {
            raise(SCHEMA_VERSION)?;
            info!("migrating storage schema 2 -> 3 (derived index ids)");
            derive_index_ids(db)?;
            info!("migrating storage schema 3 -> 4 (partial index membership)");
            rebuild_partial_indexes(db)
        }
        Some(3) => {
            raise(SCHEMA_VERSION)?;
            info!("migrating storage schema 3 -> 4 (partial index membership)");
            rebuild_partial_indexes(db)
        }
        // A newer schema means a newer build wrote this directory. Refusing is
        // the right failure: guessing at a layout we do not know would corrupt
        // it, and the version check exists precisely to avoid that.
        Some(other) => {
            Err(StorageError::UnsupportedFormat { found: other, expected: SCHEMA_VERSION })
        }
    }
}

/// The partial indexes rebuilt so far, by `(collection, index)`: node-local,
/// written in each index's own transaction, and deleted in the commit that
/// writes schema 4. Its rows are what a migration interrupted part-way does
/// not do again.
const PARTIAL_REBUILT: redb::TableDefinition<(u64, u32), ()> =
    redb::TableDefinition::new("partial_rebuilt_under_find");

/// What a rebuild of a unique partial index found, by `(collection, index)`,
/// written in the same commit as that index's marker and deleted only once it
/// has been reported (ADR-183).
///
/// In the store rather than carried back in memory. Reporting needs an engine,
/// which does not exist until after the migration returns, so a crash in
/// between used to lose the finding entirely: both duplicates stayed, and
/// nothing counted, logged or recorded them. The invariant is that a marked
/// index's collisions are never lost, which means the record has to be as
/// durable as the marker and committed with it.
const PARTIAL_REBUILT_VIOLATIONS: redb::TableDefinition<(u64, u32), &[u8]> =
    redb::TableDefinition::new("partial_rebuilt_unique_violations");

/// One index's collisions, with the collection they belong to by name, because
/// an id is not enough to find the metadata a report needs.
#[derive(serde::Serialize, serde::Deserialize)]
struct RecordedViolations {
    db: String,
    collection: String,
    violations: Vec<crate::index::UniqueViolation>,
}

/// Every recorded collision not yet reported, resolved against the collection
/// metadata as it now stands.
///
/// Read on every open, not only on one that migrates: the marker table is gone
/// once the migration finishes, so a crash after the last index and before the
/// report would otherwise leave a recorded collision that nothing ever looks
/// at again.
fn persisted_violations(db: &Database) -> Result<Unreported> {
    let txn = db.begin_read()?;
    let recorded = match txn.open_table(PARTIAL_REBUILT_VIOLATIONS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let collections = txn.open_table(tables::COLLECTIONS)?;
    let mut out = Vec::new();
    for row in recorded.iter()? {
        let (_, value) = row?;
        let held: RecordedViolations = serde_json::from_slice(value.value()).map_err(|e| {
            StorageError::Corrupt(format!(
                "a pending row of the recorded-collision table \
                 (partial_rebuilt_unique_violations) will not decode, so this open cannot \
                 report a collision an earlier rebuild found: {e}"
            ))
        })?;
        // Gone since, or replaced: there is nothing to report a collision
        // against, and the entries went with it. Such a row lingers, because the
        // forget below runs only when something was reported -- so a file whose
        // only pending rows name dropped collections keeps them. Unreachable in
        // practice: it needs the collection dropped between the rebuild's commit
        // and the next open, in the window before the engine exists. Named here
        // rather than guarded, since the cost is one dead row.
        let Some(raw) = collections.get((held.db.as_str(), held.collection.as_str()))? else {
            continue;
        };
        out.push((serde_json::from_slice::<CollectionMeta>(raw.value())?, held.violations));
    }
    Ok(out)
}

/// Forget the recorded collisions, once they have been reported.
///
/// Separate from the migration's own last commit, and later than it: until the
/// engine exists they cannot be reported, so deleting them with the markers
/// would be deleting them unread. Reporting is therefore at least once -- a
/// crash between the report and this leaves them to be reported again on the
/// next open, which is the side to err on.
pub(crate) fn forget_reported_violations(db: &Database) -> Result<()> {
    let txn = db.begin_write()?;
    match txn.delete_table(PARTIAL_REBUILT_VIOLATIONS) {
        Ok(_) => {}
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(e) => return Err(e.into()),
    }
    txn.commit()?;
    Ok(())
}

/// Whether the marker table is still there, which is what an interrupted
/// migration leaves behind.
///
/// Its absence is the only thing that means finished, and that is why the last
/// commit deletes it. A migration that found no partial index never creates it.
fn partial_rebuild_owed(db: &Database) -> Result<bool> {
    let txn = db.begin_read()?;
    match txn.open_table(PARTIAL_REBUILT) {
        Ok(_) => Ok(true),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// The cost of a rebuild, per document per index, that the up-front estimate is
/// made from (ADR-183).
///
/// **Not linear in the document count, which is why this is a bound and not a
/// rate.** Measured on the machine the migration was written on: 73 s for 10
/// million documents, some 7.3 µs each, and 5.4 s for 2 million, some 2.7 µs
/// each. The per-document cost grows with the working set, because a larger
/// store spills out of the page cache. 8 covers both sizes measured; above ten
/// million documents it is an extrapolation, and the announcement's estimate
/// says "estimate" for that reason. The documentation quotes this same number,
/// and the manifest in the operations guide allows more than twice the estimate
/// this produces, so the two cannot drift into a probe that kills the node
/// during its own documented example.
pub const MICROS_PER_DOCUMENT: u64 = 8;

/// redb's growth step, which is what the file extends by when its slack does
/// not cover a rebuild (ADR-183): a rebuild needing 166 MiB took the file from
/// 4,096 to 6,199 MiB. Reported beside the largest index's own size, because
/// the space to have free is the two added together, not either alone.
const REDB_GROWTH_STEP_MIB: u64 = 2048;

/// The measured size of an index entry on disk, including the tree's own
/// overhead (ADR-183): 553 MiB for 7.43 million entries. A rebuild needs about
/// this much free inside the file for its largest index.
const BYTES_PER_ENTRY: u64 = 80;

/// How many documents between two progress lines. Small under test, so a
/// small fixture shows that the beat is kept.
#[cfg(not(test))]
const PROGRESS_EVERY: u64 = 100_000;
#[cfg(test)]
const PROGRESS_EVERY: u64 = 10;

/// What the migration is about to do, stated before it starts.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct PartialRebuildPlan {
    /// `(collection, index, documents to scan)`, in the order they run.
    pub indexes: Vec<(CollectionMeta, crate::IndexMeta, u64)>,
    pub documents: u64,
    /// The most documents any one partial index's collection holds: an upper
    /// bound on its entries for an index that is not multikey.
    pub largest_documents: u64,
}

impl PartialRebuildPlan {
    pub(crate) fn estimate_secs(&self) -> u64 {
        self.documents * MICROS_PER_DOCUMENT / 1_000_000
    }

    pub(crate) fn largest_needs_mib(&self) -> u64 {
        self.largest_documents * BYTES_PER_ENTRY / (1 << 20)
    }
}

/// Refuse the migration, naming every partial index whose stored filter this
/// build will not parse, before anything is rebuilt.
///
/// The rebuild derives each index's membership from its filter, so an index
/// whose filter this build refuses has no membership this build can build. A
/// filter can be one an earlier build accepted and this one does not -- one
/// holding a `Decimal128`, from before parsing refused it -- which is stored
/// metadata, a fact about a definition rather than about any document.
///
/// **It refuses rather than skipping**, unlike a TTL pass, which skips such an
/// index and deletes nothing (ADR-181). A pass is a recurring runtime gate
/// where refusing would stop every other index's expiry; an upgrade is a gate
/// where refusing costs nothing irreversible, and skipping would make the
/// condition permanent in a schema 4 file, which is exactly what ADR-181 relies
/// on not happening when it calls the condition transient. A skipped index
/// would also leave a node running for weeks with a collection refusing every
/// write, explained by one line at a start nobody reads again.
///
/// It runs before any write on every path that migrates, the id-deriving steps
/// of a schema 1 or 2 source included, so nothing has changed when it refuses.
///
/// **Every partial index in the file is parsed**, in every database, whether or
/// not its collection holds a document and whether or not an interrupted
/// migration already marked it rebuilt: an index on an empty collection is
/// never handed a document, so the rebuild alone would never parse its filter
/// and would mark it done. **Every offender is named at once**, so one pass
/// with the previous build fixes them all rather than finding the next on the
/// next upgrade attempt.
fn refuse_unparseable_partial_filters(db: &Database, found: u8) -> Result<()> {
    let txn = db.begin_read()?;
    let collections = txn.open_table(tables::COLLECTIONS)?;
    let mut refused = Vec::new();
    for row in collections.iter()? {
        let (_, value) = row?;
        let meta: CollectionMeta = serde_json::from_slice(value.value())?;
        for index in &meta.indexes {
            if let Some(Err(e)) = index.partial() {
                refused.push(format!("{}.{} index {:?}: {e}", meta.db, meta.name, index.name));
            }
        }
    }
    if refused.is_empty() {
        return Ok(());
    }
    Err(StorageError::UnparseablePartialFilter { found, refused })
}

/// Every partial index not yet rebuilt, with what the announcement states.
///
/// Each collection's size comes from its kept live count (ADR-174), one row,
/// when the counts are current. Counting instead -- ten million documents and
/// six million entries -- took 48 s just to announce the job, more than half
/// the job itself. This runs before `Engine::open` rebuilds stale counts,
/// though, and a database restored from a backup carries none: trusted then,
/// the announcement would say nothing was to be done. So when the counts'
/// mark does not match the store, each collection's records are counted, and
/// that open pays the 48 s.
pub(crate) fn partial_rebuild_plan(db: &Database) -> Result<PartialRebuildPlan> {
    let txn = db.begin_read()?;
    let collections = txn.open_table(tables::COLLECTIONS)?;
    let counts = txn.open_table(tables::LIVE_COUNTS)?;
    let current = crate::live_count::counts_are_current(&txn)?;
    let docs = txn.open_table(tables::DOCS)?;
    let done: std::collections::HashSet<(u64, u32)> = match txn.open_table(PARTIAL_REBUILT) {
        Ok(table) => table.iter()?.map(|row| Ok(row?.0.value())).collect::<Result<_>>()?,
        Err(redb::TableError::TableDoesNotExist(_)) => Default::default(),
        Err(e) => return Err(e.into()),
    };
    let mut plan = PartialRebuildPlan::default();
    for row in collections.iter()? {
        let (_, value) = row?;
        let meta: CollectionMeta = serde_json::from_slice(value.value())?;
        let mut documents = None;
        for index in &meta.indexes {
            if index.partial_filter.is_none() || done.contains(&(meta.id.0, index.id)) {
                continue;
            }
            let documents = match documents {
                Some(n) => n,
                None if current => {
                    *documents.insert(counts.get(meta.id.0)?.map_or(0, |n| n.value()))
                }
                None => {
                    *documents.insert(docs.range(crate::engine::doc_range(meta.id))?.count() as u64)
                }
            };
            plan.documents += documents;
            plan.largest_documents = plan.largest_documents.max(documents);
            plan.indexes.push((meta.clone(), index.clone(), documents));
        }
    }
    Ok(plan)
}

/// Rebuild every partial index under `find`'s semantics, then write schema 4
/// (ADR-183).
///
/// One transaction per index: its entries are cleared by key and rebuilt from
/// every document, its `multikey` flag raised if the new membership makes it
/// so, and its marker written, all in that commit. A crash rolls that index
/// back, marker and all, and the next open does it again; the ones already
/// marked are not redone. Nothing reads the index meanwhile: this runs inside
/// `Engine::open` before the engine exists, and redb refuses a second opener.
///
/// A unique index is rebuilt in full whatever its documents share, and the
/// keys two or more of them share are returned for the engine to report as a
/// replicated build's are (ADR-020, ADR-123): a migration cannot refuse, and
/// documents accepted while the constraint was misapplied are real.
fn rebuild_partial_indexes(db: &Database) -> Result<()> {
    let plan = partial_rebuild_plan(db)?;
    let total = plan.indexes.len();
    if total > 0 {
        info!(
            partial_indexes = total,
            documents = plan.documents,
            estimate_secs = plan.estimate_secs(),
            largest_collection_documents = plan.largest_documents,
            largest_needs_free_mib = plan.largest_needs_mib(),
            plus_growth_step_mib = REDB_GROWTH_STEP_MIB,
            "rebuilding every partial index so its membership is what find selects, plus the documents \
             the filter cannot decide because a Decimal128 ranks equal to every number \
             (ADR-183, ADR-185), before this node serves anything; each index is rebuilt in one transaction and \
             needs about its own size free inside the database file, up to the figure for the \
             largest -- and if the file's slack does not cover it, redb extends the file by its \
             growth step, so allow the two added together"
        );
    }
    for (n, (meta, index, documents)) in plan.indexes.iter().enumerate() {
        let started = std::time::Instant::now();
        info!(
            n = n + 1,
            of = total,
            db = %meta.db,
            collection = %meta.name,
            index = %index.name,
            documents,
            "rebuilding a partial index"
        );
        let txn = db.begin_write()?;
        let (built, multikey, violations) = {
            let mut entries = txn.open_table(tables::INDEX_ENTRIES)?;
            crate::index::clear_index_entries(
                &mut entries,
                crate::index::index_id_range(meta.id, index.id),
            )?;
            let docs = txn.open_table(tables::DOCS)?;
            let mut holders: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
            let (mut built, mut scanned, mut multikey) = (0u64, 0u64, false);
            for row in docs.range(crate::engine::doc_range(meta.id))? {
                let (key, value) = row?;
                let record = codec::decode_doc_record(value.value())?;
                let Some(doc) = record.document()? else { continue };
                let (_, doc_key) = key.value();
                match crate::index::document_keys(index, &doc)? {
                    crate::index::DocumentKeys::Keyed { keys, multikey: many } => {
                        multikey |= many;
                        for key in keys {
                            if index.unique {
                                holders.entry(key.clone()).or_default().push(doc_key.to_vec());
                            }
                            entries.insert((meta.id.0, index.id, key.as_slice(), doc_key), ())?;
                            built += 1;
                        }
                    }
                    // The 3 -> 4 rebuild is where option-2 membership is
                    // built for an existing database (ADR-185): a document the
                    // filter cannot decide goes into the undecidable run here,
                    // exactly as a later write would file it.
                    crate::index::DocumentKeys::Undecidable { .. } => {
                        // The 3 -> 4 rebuild writes the new sentinel directly.
                        // Still no schema 5: 4 is unreleased, so no deployed
                        // database holds these entries under the old key
                        // (ADR-185).
                        entries.insert(
                            (meta.id.0, index.id, crate::index::UNDECIDABLE, doc_key),
                            (),
                        )?;
                        built += 1;
                    }
                    crate::index::DocumentKeys::Unkeyed { multikey: many, .. } => {
                        multikey |= many;
                        entries
                            .insert((meta.id.0, index.id, crate::index::UNKEYED, doc_key), ())?;
                        built += 1;
                    }
                }
                scanned += 1;
                if scanned % PROGRESS_EVERY == 0 {
                    #[cfg(test)]
                    hooks::progress();
                    info!(n = n + 1, of = total, index = %index.name, scanned, of_documents = documents, "rebuilding a partial index");
                }
            }
            let mut violations: Vec<crate::index::UniqueViolation> = holders
                .into_iter()
                .filter(|(_, holders)| holders.len() > 1)
                .map(|(key, holders)| crate::index::UniqueViolation {
                    index: index.name.clone(),
                    key,
                    holders,
                })
                .collect();
            violations.sort_by(|a, b| a.key.cmp(&b.key));
            (built, multikey, violations)
        };
        // One-way, like every other write that can make an index multikey:
        // a two-sided range over one that is and says it is not loses rows.
        let mut standing = meta.clone();
        if multikey && !index.multikey {
            let mut collections = txn.open_table(tables::COLLECTIONS)?;
            let fresh = collections
                .get((meta.db.as_str(), meta.name.as_str()))?
                .map(|raw| serde_json::from_slice::<CollectionMeta>(raw.value()))
                .transpose()?;
            if let Some(mut fresh) = fresh {
                for held in fresh.indexes.iter_mut().filter(|i| i.id == index.id) {
                    held.multikey = true;
                }
                collections.insert(
                    (meta.db.as_str(), meta.name.as_str()),
                    serde_json::to_vec(&fresh)?.as_slice(),
                )?;
                standing = fresh;
            }
        }
        txn.open_table(PARTIAL_REBUILT)?.insert((meta.id.0, index.id), ())?;
        // What this index's rebuild found, in the same commit as its marker.
        // Reporting needs an engine and so happens after the migration returns;
        // recorded here, an interrupt in between leaves them to be reported on
        // the next open instead of losing them.
        if !violations.is_empty() {
            let recorded = RecordedViolations {
                db: standing.db.clone(),
                collection: standing.name.clone(),
                violations: violations.clone(),
            };
            txn.open_table(PARTIAL_REBUILT_VIOLATIONS)?
                .insert((meta.id.0, index.id), serde_json::to_vec(&recorded)?.as_slice())?;
        }
        // The version, in the same commit as the first index's marker rather
        // than at the end. Until this change a half-migrated file was still
        // schema 3, so the previous build opened it and maintained the indexes
        // it had not rebuilt under the old rule: entries the new rule does not
        // hold stayed, and a later run marked the rest done and stamped 4 over
        // them for good. A query answered from such an index misses rows and a
        // unique one refuses a key that is free. Stamping it here means an
        // older build refuses the file instead, and a rollback is the wipe and
        // resync the operations guide documents.
        if n == 0 {
            txn.open_table(tables::META)?
                .insert(tables::META_FORMAT_VERSION, [SCHEMA_VERSION].as_slice())?;
        }
        #[cfg(test)]
        if hooks::fails_at(n + 1) {
            drop(txn);
            return Err(StorageError::Database("a failure injected mid-migration".into()));
        }
        txn.commit()?;
        info!(
            n = n + 1,
            of = total,
            index = %index.name,
            entries = built,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "rebuilt a partial index"
        );
    }
    // The end of the bookkeeping. The version is already 4 if any index was
    // rebuilt in this run or an earlier one; it is written here too, for the
    // database that has no partial index to rebuild at all, where the loop
    // above never ran.
    let txn = db.begin_write()?;
    match txn.delete_table(PARTIAL_REBUILT) {
        Ok(_) => {}
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(e) => return Err(e.into()),
    }
    txn.open_table(tables::META)?
        .insert(tables::META_FORMAT_VERSION, [SCHEMA_VERSION].as_slice())?;
    txn.commit()?;
    Ok(())
}

/// Record the redb major.minor in META, if it is not already what is there
/// (ADR-190): run on the raw database during `Engine::open`, as the migrations
/// are, so the read-only check can refuse a clean store with no sidecar that a
/// newer redb wrote. Never lowered: a newer version recorded there is the
/// evidence that check reads, and `refuse_newer` has already stopped the open.
pub(crate) fn record_redb_version(
    db: &Database,
    build: &crate::format::BuildVersions,
) -> Result<()> {
    let ours = format!("{}.{}", build.redb.0, build.redb.1);
    let stored = stored_redb_version(db)?;
    if stored.as_deref() == Some(ours.as_str())
        || stored.as_deref().and_then(crate::format::major_minor).is_some_and(|v| v > build.redb)
    {
        return Ok(());
    }
    let txn = db.begin_write()?;
    txn.open_table(tables::META)?.insert(crate::format::META_REDB_VERSION, ours.as_bytes())?;
    txn.commit()?;
    Ok(())
}

/// Test-only points in the membership migration.
#[cfg(test)]
pub(crate) mod hooks {
    use std::cell::Cell;

    thread_local! {
        static PROGRESS: Cell<usize> = const { Cell::new(0) };
        static FAIL_AT: Cell<Option<usize>> = const { Cell::new(None) };
    }

    pub(crate) fn progress() {
        PROGRESS.with(|p| p.set(p.get() + 1));
    }

    /// Progress lines written on this thread so far.
    pub(crate) fn progress_lines() -> usize {
        PROGRESS.with(|p| p.get())
    }

    /// Fail the next migration on this thread at the `n`th index (from 1),
    /// after its work and before its commit, as a crash there would.
    pub(crate) fn fail_at_index(n: usize) {
        FAIL_AT.with(|f| f.set(Some(n)));
    }

    pub(crate) fn fails_at(n: usize) -> bool {
        FAIL_AT.with(|f| {
            if f.get() == Some(n) {
                f.set(None);
                true
            } else {
                false
            }
        })
    }
}

pub(crate) fn stored_version(db: &Database) -> Result<Option<u8>> {
    let txn = db.begin_read()?;
    let meta = match txn.open_table(tables::META) {
        Ok(meta) => meta,
        // A fresh store, before the tables exist.
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    Ok(meta.get(tables::META_FORMAT_VERSION)?.and_then(|v| v.value().first().copied()))
}

/// Refuse a store a newer build wrote, reading only, before the ensure-tables
/// commit (ADR-190): a newer schema, or a newer redb recorded in META. The
/// check before the open already refused both for every store it could read.
/// What reaches here is a dirty store with no sidecar, which redb has just
/// repaired: the repair wrote, which is the gap ADR-190 documents, but the
/// session stops here, and META still says which redb wrote the store.
pub(crate) fn refuse_newer(
    db: &Database,
    path: &std::path::Path,
    build: &crate::format::BuildVersions,
) -> Result<()> {
    match stored_version(db)? {
        Some(found) if found > SCHEMA_VERSION => {
            return Err(StorageError::UnsupportedFormat { found, expected: SCHEMA_VERSION });
        }
        _ => {}
    }
    if let Some(recorded) = stored_redb_version(db)?
        && crate::format::major_minor(&recorded).is_some_and(|v| v > build.redb)
    {
        return Err(StorageError::RefusedStore(format!(
            "{} was last written by redb {recorded}, newer than this build's {}.{}. It had no \
             kimmy.format and was not shut down cleanly, so redb repaired it before that could be \
             read; nothing else was written. Start the build that wrote it, or restore a backup",
            path.display(),
            build.redb.0,
            build.redb.1
        )));
    }
    Ok(())
}

/// The redb major.minor META records, tolerating a missing META.
fn stored_redb_version(db: &Database) -> Result<Option<String>> {
    let txn = db.begin_read()?;
    let meta = match txn.open_table(tables::META) {
        Ok(meta) => meta,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    Ok(meta
        .get(crate::format::META_REDB_VERSION)?
        .and_then(|v| std::str::from_utf8(v.value()).ok().map(str::to_string)))
}

fn write_version(db: &Database, version: u8) -> Result<()> {
    let txn = db.begin_write()?;
    {
        let mut meta = txn.open_table(tables::META)?;
        meta.insert(tables::META_FORMAT_VERSION, [version].as_slice())?;
    }
    txn.commit()?;
    Ok(())
}

/// Renumber every collection from its counter id to its derived id.
fn derive_collection_ids(db: &Database) -> Result<()> {
    let mut remap: HashMap<u64, u64> = HashMap::new();
    let mut updated: Vec<((String, String), CollectionMeta)> = Vec::new();

    {
        let txn = db.begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        for row in collections.iter()? {
            let (key, value) = row?;
            let (db_name, coll_name) = key.value();
            let mut meta: CollectionMeta = serde_json::from_slice(value.value())?;

            let new_id = CollectionId::derive(db_name, coll_name);
            if new_id != meta.id {
                remap.insert(meta.id.0, new_id.0);
            }
            meta.id = new_id;
            updated.push(((db_name.to_string(), coll_name.to_string()), meta));
        }
    }

    // Two collections whose derived ids collide would be merged by the
    // renumbering. Vanishingly unlikely, and checked anyway, because the
    // alternative is discovering it as interleaved data.
    let mut seen = HashMap::new();
    for ((db_name, coll_name), meta) in &updated {
        if let Some(previous) = seen.insert(meta.id.0, format!("{db_name}.{coll_name}")) {
            return Err(StorageError::Corrupt(format!(
                "cannot migrate: {db_name}.{coll_name} and {previous} derive the same collection \
                 id; rename one of them with the previous build first"
            )));
        }
    }

    if remap.is_empty() {
        return Ok(());
    }

    // One collection at a time, so a large database does not have to hold its
    // whole document set in memory to be migrated.
    for (old, new) in &remap {
        move_documents(db, *old, *new)?;
        move_index_entries(db, *old, *new)?;
    }
    rewrite_oplog(db, &remap)?;

    let txn = db.begin_write()?;
    {
        let mut collections = txn.open_table(tables::COLLECTIONS)?;
        for ((db_name, coll_name), meta) in &updated {
            collections.insert(
                (db_name.as_str(), coll_name.as_str()),
                serde_json::to_vec(meta)?.as_slice(),
            )?;
        }
    }
    txn.commit()?;

    info!(collections = remap.len(), "renumbered collections to derived ids");
    Ok(())
}

/// One collection's indexes, with the id changes they need.
struct IndexRenumber {
    db: String,
    name: String,
    meta: CollectionMeta,
    /// `(old id, new id)` per index that moved.
    remap: Vec<(u32, u32)>,
}

/// Renumber every index from its counter id to its derived id.
fn derive_index_ids(db: &Database) -> Result<()> {
    let mut updated: Vec<IndexRenumber> = Vec::new();

    {
        let txn = db.begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        for row in collections.iter()? {
            let (key, value) = row?;
            let (db_name, coll_name) = key.value();
            let mut meta: CollectionMeta = serde_json::from_slice(value.value())?;

            let mut remap = Vec::new();
            let mut seen: HashMap<u32, String> = HashMap::new();
            for index in &mut meta.indexes {
                let new_id = kimmy_core::IndexMeta::derive_id(&index.name);
                if let Some(other) = seen.insert(new_id, index.name.clone()) {
                    return Err(StorageError::Corrupt(format!(
                        "cannot migrate: indexes {:?} and {other:?} on {db_name}.{coll_name} \
                         derive the same id; drop one with the previous build first",
                        index.name
                    )));
                }
                if new_id != index.id {
                    remap.push((index.id, new_id));
                }
                index.id = new_id;
            }

            if !remap.is_empty() {
                updated.push(IndexRenumber {
                    db: db_name.to_string(),
                    name: coll_name.to_string(),
                    meta,
                    remap,
                });
            }
        }
    }

    if updated.is_empty() {
        return Ok(());
    }

    let mut moved = 0usize;
    for entry in &updated {
        for (old, new) in &entry.remap {
            moved += move_index_ids(db, entry.meta.id, *old, *new)?;
        }

        let txn = db.begin_write()?;
        {
            let mut collections = txn.open_table(tables::COLLECTIONS)?;
            collections.insert(
                (entry.db.as_str(), entry.name.as_str()),
                serde_json::to_vec(&entry.meta)?.as_slice(),
            )?;
        }
        txn.commit()?;
    }

    info!(entries = moved, "renumbered index entries to derived ids");
    Ok(())
}

/// Move one index's entries from `old` to `new` within a collection.
fn move_index_ids(db: &Database, coll: CollectionId, old: u32, new: u32) -> Result<usize> {
    let rows: Vec<(Vec<u8>, Vec<u8>)> = {
        let txn = db.begin_read()?;
        let entries = txn.open_table(tables::INDEX_ENTRIES)?;
        entries
            .range(crate::engine::index_range(coll))?
            .filter_map(|row| match row {
                Ok((key, _)) => {
                    let (_, index_id, value, doc_key) = key.value();
                    (index_id == old).then(|| Ok((value.to_vec(), doc_key.to_vec())))
                }
                Err(e) => Some(Err(e.into())),
            })
            .collect::<Result<_>>()?
    };

    let count = rows.len();
    if count == 0 {
        return Ok(0);
    }

    let txn = db.begin_write()?;
    {
        let mut entries = txn.open_table(tables::INDEX_ENTRIES)?;
        for (value, doc_key) in &rows {
            entries.insert((coll.0, new, value.as_slice(), doc_key.as_slice()), ())?;
            entries.remove((coll.0, old, value.as_slice(), doc_key.as_slice()))?;
        }
    }
    txn.commit()?;
    Ok(count)
}

fn move_documents(db: &Database, old: u64, new: u64) -> Result<()> {
    let rows: Vec<(Vec<u8>, Vec<u8>)> = {
        let txn = db.begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        docs.range(crate::engine::doc_range(CollectionId(old)))?
            .map(|row| {
                let (key, value) = row?;
                Ok((key.value().1.to_vec(), value.value().to_vec()))
            })
            .collect::<Result<_>>()?
    };

    let txn = db.begin_write()?;
    {
        let mut docs = txn.open_table(tables::DOCS)?;
        for (key, value) in &rows {
            docs.insert((new, key.as_slice()), value.as_slice())?;
        }
        // By key, not `retain_in`: see `index::clear_index_entries` for what
        // `retain_in` costs over a range. The rows are all in hand already,
        // and nothing else writes while a migration runs at open.
        for (key, _) in &rows {
            docs.remove((old, key.as_slice()))?;
        }
    }
    txn.commit()?;
    Ok(())
}

fn move_index_entries(db: &Database, old: u64, new: u64) -> Result<()> {
    #[allow(clippy::type_complexity)]
    let rows: Vec<(u32, Vec<u8>, Vec<u8>)> = {
        let txn = db.begin_read()?;
        let entries = txn.open_table(tables::INDEX_ENTRIES)?;
        entries
            .range(crate::engine::index_range(CollectionId(old)))?
            .map(|row| {
                let (key, _) = row?;
                let (_, index_id, value, doc_key) = key.value();
                Ok((index_id, value.to_vec(), doc_key.to_vec()))
            })
            .collect::<Result<_>>()?
    };

    let txn = db.begin_write()?;
    {
        let mut entries = txn.open_table(tables::INDEX_ENTRIES)?;
        for (index_id, value, doc_key) in &rows {
            entries.insert((new, *index_id, value.as_slice(), doc_key.as_slice()), ())?;
        }
        crate::index::clear_index_entries(
            &mut entries,
            crate::engine::index_range(CollectionId(old)),
        )?;
    }
    txn.commit()?;
    Ok(())
}

/// Rewrite the `collection` field of every affected oplog entry.
///
/// Keys are stamps and do not change, so this is a value rewrite in place —
/// which also means the arrival index, which maps sequence to stamp, needs no
/// migration at all.
fn rewrite_oplog(db: &Database, remap: &HashMap<u64, u64>) -> Result<()> {
    let rows: Vec<(Vec<u8>, Vec<u8>)> = {
        let txn = db.begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        let mut out = Vec::new();
        for row in oplog.iter()? {
            let (key, value) = row?;
            let mut entry = codec::decode_oplog_entry(value.value())?;
            let Some(new) = remap.get(&entry.collection.0) else {
                continue;
            };
            entry.collection = CollectionId(*new);
            out.push((key.value().to_vec(), codec::encode_oplog_entry(&entry)));
        }
        out
    };

    if rows.is_empty() {
        return Ok(());
    }

    let txn = db.begin_write()?;
    {
        let mut oplog = txn.open_table(tables::OPLOG)?;
        for (key, value) in &rows {
            oplog.insert(key.as_slice(), value.as_slice())?;
        }
    }
    txn.commit()?;

    info!(entries = rows.len(), "repointed oplog entries to derived collection ids");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;
    use bson::doc;
    use kimmy_core::DocId;

    /// Rewind a database to schema 1: counter ids, and the version to match.
    ///
    /// Builds the old layout from the new one rather than checking in a binary
    /// fixture, so the test keeps working as the rest of the format evolves.
    pub(super) fn rewind_to_schema_1(path: &std::path::Path, assignments: &[(&str, &str, u64)]) {
        let db = Database::create(path).unwrap();

        let mut remap = HashMap::new();
        {
            let txn = db.begin_write().unwrap();
            {
                let mut collections = txn.open_table(tables::COLLECTIONS).unwrap();
                for (db_name, coll_name, old_id) in assignments {
                    let raw = collections.get((*db_name, *coll_name)).unwrap().unwrap();
                    let mut meta: CollectionMeta = serde_json::from_slice(raw.value()).unwrap();
                    drop(raw);
                    remap.insert(meta.id.0, *old_id);
                    meta.id = CollectionId(*old_id);
                    collections
                        .insert(
                            (*db_name, *coll_name),
                            serde_json::to_vec(&meta).unwrap().as_slice(),
                        )
                        .unwrap();
                }
            }
            txn.commit().unwrap();
        }

        for (new, old) in &remap {
            move_documents(&db, *new, *old).unwrap();
            move_index_entries(&db, *new, *old).unwrap();
        }
        rewrite_oplog(&db, &remap).unwrap();
        write_version(&db, 1).unwrap();
    }

    #[test]
    fn a_schema_1_database_is_migrated_rather_than_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("shop", "orders").unwrap();
            engine.create_index("shop", "orders", vec![field("item")], false, None).unwrap();
            let orders = engine.get_collection("shop", "orders").unwrap();
            for i in 0..5i64 {
                engine.insert(&orders, doc! { "_id": i, "item": format!("w{i}") }).unwrap();
            }
        }

        rewind_to_schema_1(&path, &[("shop", "orders", 7)]);

        // Reopening must migrate, not refuse.
        let engine = Engine::open(&path).unwrap();
        let orders = engine.get_collection("shop", "orders").unwrap();

        assert_eq!(orders.id, CollectionId::derive("shop", "orders"), "id must be renumbered");
        assert_eq!(engine.count(&orders).unwrap(), 5, "documents must move with the id");
        assert!(engine.get(&orders, &DocId::Int64(3)).unwrap().is_some());
    }

    #[test]
    fn moving_a_collection_to_its_derived_id_does_not_grow_the_file() {
        // The old range goes by key, not `retain_in`, which at this size made
        // the migration double the file (see `index::clear_index_entries`).
        // A test that only checked the documents arrived passed under both.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        // Two collections, both moved: each one's clear of its old range must
        // leave the other's documents and index entries where they are.
        {
            let engine = Engine::open(&path).unwrap();
            for name in ["orders", "lines"] {
                engine.create_collection("shop", name).unwrap();
                engine.create_index("shop", name, vec![field("item")], false, None).unwrap();
                let coll = engine.get_collection("shop", name).unwrap();
                let docs: Vec<bson::Document> =
                    (0..5_000i64).map(|i| doc! { "_id": i, "item": format!("w{i}") }).collect();
                engine.insert_many(&coll, docs).unwrap();
            }
        }
        rewind_to_schema_1(&path, &[("shop", "orders", 7), ("shop", "lines", 8)]);
        let before = std::fs::metadata(&path).unwrap().len();

        let engine = Engine::open(&path).unwrap();

        for name in ["orders", "lines"] {
            let coll = engine.get_collection("shop", name).unwrap();
            assert_eq!(engine.count(&coll).unwrap(), 5_000, "{name}: every document moved");
            let index = &coll.indexes[0];
            let entries = crate::index::scan_range(
                engine.db(),
                coll.id,
                index.id,
                &[],
                None,
                crate::index::Unkeyed::Include,
            )
            .unwrap()
            .len();
            assert_eq!(entries, 5_000, "{name}: every index entry moved");
        }
        let after = std::fs::metadata(&path).unwrap().len();
        assert!(
            after <= before + before / 4,
            "the migration grew the file from {before} to {after}"
        );
    }

    #[test]
    fn migrated_index_entries_still_answer_queries() {
        // Index keys embed the collection id, so an unmigrated index entry is
        // an index that silently finds nothing.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("shop", "orders").unwrap();
            engine.create_index("shop", "orders", vec![field("item")], false, None).unwrap();
            let orders = engine.get_collection("shop", "orders").unwrap();
            engine.insert(&orders, doc! { "_id": 1, "item": "widget" }).unwrap();
        }

        rewind_to_schema_1(&path, &[("shop", "orders", 9)]);
        let engine = Engine::open(&path).unwrap();
        let orders = engine.get_collection("shop", "orders").unwrap();

        let index = &orders.indexes[0];
        let key = kimmy_core::keyenc::encode(&bson::Bson::String("widget".into())).unwrap();
        let found = engine.index_candidates(&orders, index.id, &key, &key).unwrap();
        assert_eq!(found.len(), 1, "the index must follow the collection to its new id");
    }

    #[test]
    fn migrated_oplog_entries_point_at_the_new_id() {
        // Otherwise a change stream would attribute history to a collection
        // that no longer has that id — and the embedding worker would skip it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        {
            let engine = Engine::open(&path).unwrap();
            let orders = engine.create_collection("shop", "orders").unwrap();
            engine.insert(&orders, doc! { "_id": 1 }).unwrap();
        }

        rewind_to_schema_1(&path, &[("shop", "orders", 42)]);
        let engine = Engine::open(&path).unwrap();

        let expected = CollectionId::derive("shop", "orders");
        let entries = engine.read_arrival_from(0, 100).unwrap();
        assert!(
            entries.iter().all(|e| e.collection != CollectionId(42)),
            "no entry may still name the old id"
        );
        assert!(
            entries.iter().any(|e| e.collection == expected),
            "entries must be repointed at the derived id"
        );
    }

    #[test]
    fn migration_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            let orders = engine.create_collection("shop", "orders").unwrap();
            engine.insert(&orders, doc! { "_id": 1 }).unwrap();
        }
        rewind_to_schema_1(&path, &[("shop", "orders", 5)]);

        for _ in 0..3 {
            let engine = Engine::open(&path).unwrap();
            let orders = engine.get_collection("shop", "orders").unwrap();
            assert_eq!(orders.id, CollectionId::derive("shop", "orders"));
            assert_eq!(engine.count(&orders).unwrap(), 1);
        }
    }

    #[test]
    fn a_newer_schema_is_refused_rather_than_guessed_at() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let _ = Engine::open(&path).unwrap();
        }
        {
            let db = Database::create(&path).unwrap();
            write_version(&db, SCHEMA_VERSION + 1).unwrap();
        }

        let err = Engine::open(&path).err().expect("a future layout must not be opened");
        assert!(
            matches!(err, StorageError::UnsupportedFormat { .. }),
            "expected an unsupported-format refusal, got {err:?}"
        );
    }

    #[test]
    fn index_entries_follow_their_index_to_a_derived_id() {
        // Index-entry keys embed the index id, so an unmigrated entry is an
        // index that silently finds nothing.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("shop", "orders").unwrap();
            engine.create_index("shop", "orders", vec![field("item")], false, None).unwrap();
            let orders = engine.get_collection("shop", "orders").unwrap();
            engine.insert(&orders, doc! { "_id": 1, "item": "widget" }).unwrap();
        }

        rewind_index_ids_to_counters(&path);

        let engine = Engine::open(&path).unwrap();
        let orders = engine.get_collection("shop", "orders").unwrap();
        let index = &orders.indexes[0];

        assert_eq!(index.id, kimmy_core::IndexMeta::derive_id(&index.name));
        let key = kimmy_core::keyenc::encode(&bson::Bson::String("widget".into())).unwrap();
        let found = engine.index_candidates(&orders, index.id, &key, &key).unwrap();
        assert_eq!(found.len(), 1, "entries must follow the index to its new id");
    }

    /// Rewind index ids to counter-allocated values, as schema 2 had them.
    fn rewind_index_ids_to_counters(path: &std::path::Path) {
        let db = Database::create(path).unwrap();
        let mut work = Vec::new();
        {
            let txn = db.begin_read().unwrap();
            let collections = txn.open_table(tables::COLLECTIONS).unwrap();
            for row in collections.iter().unwrap() {
                let (key, value) = row.unwrap();
                let (db_name, coll_name) = key.value();
                let mut meta: CollectionMeta = serde_json::from_slice(value.value()).unwrap();
                let mut remap = Vec::new();
                for (counter, index) in meta.indexes.iter_mut().enumerate() {
                    remap.push((index.id, counter as u32));
                    index.id = counter as u32;
                }
                work.push(((db_name.to_string(), coll_name.to_string()), meta, remap));
            }
        }

        for ((db_name, coll_name), meta, remap) in work {
            for (old, new) in remap {
                move_index_ids(&db, meta.id, old, new).unwrap();
            }
            let txn = db.begin_write().unwrap();
            {
                let mut collections = txn.open_table(tables::COLLECTIONS).unwrap();
                collections
                    .insert(
                        (db_name.as_str(), coll_name.as_str()),
                        serde_json::to_vec(&meta).unwrap().as_slice(),
                    )
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        write_version(&db, 2).unwrap();
    }

    fn field(path: &str) -> crate::meta::IndexField {
        crate::meta::IndexField { path: path.into(), descending: false }
    }
}

/// ADR-183: the schema 3 -> 4 migration rebuilds every partial index so its
/// membership is what `find` selects — and, since ADR-185, the documents whose
/// membership the filter cannot decide.
#[cfg(test)]
mod membership_migration {
    use std::cmp::Ordering;
    use std::collections::BTreeSet;

    use bson::{Bson, Document, doc};
    use kimmy_core::{PartialFilter, PartialOp, canonical_cmp, path};
    use redb::{Database, ReadableDatabase, ReadableTable};

    use super::*;
    use crate::Engine;

    /// Partial-index membership as it was before ADR-183, verbatim: each
    /// element of an array against the operand and never the whole array,
    /// comparisons across type brackets, and a missing field matching
    /// nothing. The fixture needs it to build what a schema 3 node holds.
    fn old_rule(filter: &Document, doc: &Document) -> bool {
        fn holds(op: &PartialOp, value: &Bson) -> bool {
            match op {
                PartialOp::Exists => true,
                PartialOp::Eq(want) => canonical_cmp(value, want) == Ordering::Equal,
                PartialOp::Gt(b) => canonical_cmp(value, b) == Ordering::Greater,
                PartialOp::Gte(b) => canonical_cmp(value, b) != Ordering::Less,
                PartialOp::Lt(b) => canonical_cmp(value, b) == Ordering::Less,
                PartialOp::Lte(b) => canonical_cmp(value, b) != Ordering::Greater,
            }
        }
        let filter = PartialFilter::parse(filter).unwrap();
        filter.predicates().all(|(field, op)| {
            path::resolve(doc, field).iter().any(|value| match value {
                Bson::Array(items) => items.iter().any(|item| holds(op, item)),
                other => holds(op, other),
            })
        })
    }

    /// Whether a filter's membership cannot differ between the old rule and
    /// `find`'s, decided from the filter alone: every predicate an equality
    /// whose operand is neither null nor an array.
    ///
    /// The migration rebuilds these anyway, because its only audience is
    /// databases created before this release (ADR-183). The class is proven
    /// here so that a later migration with a wider audience can skip them.
    fn cannot_differ(filter: &PartialFilter) -> bool {
        filter.predicates().all(
            |(_, op)| matches!(op, PartialOp::Eq(v) if !matches!(v, Bson::Null | Bson::Array(_))),
        )
    }

    /// The find differential's values, every type bracket and its edges.
    fn skip_class_values() -> Vec<Bson> {
        use bson::Binary;
        use bson::spec::BinarySubtype;
        let oid = bson::oid::ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap();
        vec![
            Bson::Null,
            Bson::Undefined,
            Bson::MinKey,
            Bson::MaxKey,
            Bson::Boolean(false),
            Bson::Boolean(true),
            Bson::Int32(0),
            Bson::Int32(-1),
            Bson::Int32(1),
            Bson::Int32(5),
            Bson::Int32(i32::MAX),
            Bson::Int64(5),
            Bson::Int64(i64::MIN),
            Bson::Double(5.0),
            Bson::Double(-0.0),
            Bson::Double(7.5),
            Bson::Double(f64::NAN),
            Bson::Double(f64::INFINITY),
            Bson::Double(f64::NEG_INFINITY),
            Bson::Decimal128(bson::Decimal128::from_bytes([0; 16])),
            Bson::String(String::new()),
            Bson::String("5".into()),
            Bson::String("x".into()),
            Bson::Symbol("x".into()),
            Bson::Document(doc! {}),
            Bson::Document(doc! {"a": 1}),
            Bson::Document(doc! {"a": null}),
            Bson::Document(doc! {"a": [1]}),
            Bson::Array(vec![]),
            Bson::Array(vec![Bson::Array(vec![])]),
            Bson::Array(vec![1.into(), 2.into()]),
            Bson::Array(vec![Bson::Array(vec![1.into(), 2.into()])]),
            Bson::Array(vec![5.into()]),
            Bson::Array(vec![1.into(), "x".into()]),
            Bson::Array(vec![Bson::Null]),
            Bson::Array(vec![Bson::Document(doc! {"a": 1})]),
            Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2] }),
            Bson::Binary(Binary { subtype: BinarySubtype::Uuid, bytes: vec![0; 16] }),
            Bson::ObjectId(oid),
            Bson::DateTime(bson::DateTime::from_millis(1_000)),
            Bson::DateTime(bson::DateTime::from_millis(-1_000)),
            Bson::Timestamp(bson::Timestamp { time: 5, increment: 1 }),
            Bson::RegularExpression(bson::Regex {
                pattern: "^x".try_into().unwrap(),
                options: "".try_into().unwrap(),
            }),
        ]
    }

    /// Every value at `k` and at `k.a`, bare, in an array, beside another
    /// element, nested an array deeper, and through an array of documents;
    /// and the empty shapes, where presence and elements part company.
    fn skip_class_docs() -> Vec<Document> {
        let mut out = vec![
            doc! {},
            doc! {"k": []},
            doc! {"k": {}},
            doc! {"k": [[]]},
            doc! {"k": [{}]},
            doc! {"k": {"a": []}},
            doc! {"k": [{"a": []}]},
            doc! {"k": [[{"a": 1}]]},
            doc! {"k": [{"b": 1}]},
        ];
        for v in skip_class_values() {
            out.push(doc! {"k": v.clone()});
            out.push(doc! {"k": [v.clone()]});
            out.push(doc! {"k": [v.clone(), 1]});
            out.push(doc! {"k": [[v.clone()]]});
            out.push(doc! {"k": {"a": v.clone()}});
            out.push(doc! {"k": {"a": [v.clone()]}});
            out.push(doc! {"k": {"a": [[v.clone()]]}});
            out.push(doc! {"k": [{"a": v.clone()}]});
            out.push(doc! {"k": [{"a": v.clone()}, {"a": 1}]});
            out.push(doc! {"k": [{"a": [v.clone()]}]});
            out.push(doc! {"k": [{"b": 1}, {"a": v.clone()}]});
        }
        out
    }

    /// Every one-predicate filter the language carries, over `k` and `k.a`,
    /// with every value as its operand, in both spellings of equality; then
    /// every two-predicate conjunction of an equality on `k` with any of
    /// those on `k.a`.
    fn skip_class_filters() -> Vec<Document> {
        let mut single = Vec::new();
        for path in ["k", "k.a"] {
            single.push(doc! {path: {"$exists": true}});
            for v in skip_class_values() {
                single.push(doc! {path: v.clone()});
                for op in ["$eq", "$gt", "$gte", "$lt", "$lte"] {
                    single.push(doc! {path: {op: v.clone()}});
                }
            }
        }
        let mut out = single.clone();
        for v in skip_class_values() {
            for other in single.iter().filter(|f| f.contains_key("k.a")) {
                let mut both = doc! {"k": v.clone()};
                both.extend(other.clone());
                out.push(both);
            }
        }
        out
    }

    /// What a filter's one predicate is, for the control's tally.
    fn kind(filter: &PartialFilter) -> &'static str {
        let ops: Vec<_> = filter.predicates().map(|(_, op)| op).collect();
        match ops.as_slice() {
            [PartialOp::Exists] => "$exists",
            [PartialOp::Eq(Bson::Null)] => "equality with null",
            [PartialOp::Eq(Bson::Array(_))] => "equality with an array",
            [PartialOp::Eq(_)] => "equality with anything else",
            [PartialOp::Gt(_)] => "$gt",
            [PartialOp::Gte(_)] => "$gte",
            [PartialOp::Lt(_)] => "$lt",
            [PartialOp::Lte(_)] => "$lte",
            _ => "a conjunction",
        }
    }

    #[test]
    fn a_filter_of_equalities_on_operands_neither_null_nor_array_selects_the_same_under_both_rules()
    {
        let docs = skip_class_docs();
        let mut in_class = (0usize, 0usize);
        let mut differ: std::collections::BTreeMap<&str, usize> = Default::default();
        for f in skip_class_filters() {
            let Ok(partial) = PartialFilter::parse(&f) else { continue };
            let class = cannot_differ(&partial);
            in_class.0 += usize::from(class);
            for d in &docs {
                let (old, new) = (old_rule(&f, d), partial.selects(d));
                if class {
                    assert_eq!(old, new, "{f:?} is in the class and the rules differ on {d:?}");
                    in_class.1 += 1;
                } else if old != new {
                    *differ.entry(kind(&partial)).or_default() += 1;
                }
            }
        }
        eprintln!(
            "SKIP CLASS: {} filters in the class agreed on all {} (filter, document) pairs over {} \
             documents; outside it the rules differed {differ:?}",
            in_class.0,
            in_class.1,
            docs.len()
        );
        // The control: every kind the class leaves out, the corpus can tell
        // apart. A corpus that could not would agree on everything, and so
        // would pass a class that took them in.
        for kind in [
            "$exists",
            "equality with null",
            "equality with an array",
            "$gt",
            "$gte",
            "$lt",
            "$lte",
            "a conjunction",
        ] {
            assert!(
                differ.get(kind).copied().unwrap_or(0) > 0,
                "premise: the corpus separates {kind}"
            );
        }
        assert!(
            !differ.contains_key("equality with anything else"),
            "the class's own kind never differs"
        );
        assert!(in_class.0 > 1_000, "premise: the class is exercised, single and conjoined");
    }

    /// Put a schema 4 database back where a schema 3 node leaves it: every
    /// partial index holding what the old rule selects, and version 3.
    fn as_schema_3(path: &std::path::Path, unique: &[&str]) {
        let db = Database::create(path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut collections = txn.open_table(tables::COLLECTIONS).unwrap();
            let rows: Vec<(String, String, CollectionMeta)> = collections
                .iter()
                .unwrap()
                .map(|row| {
                    let (k, v) = row.unwrap();
                    let (d, c) = k.value();
                    (d.to_string(), c.to_string(), serde_json::from_slice(v.value()).unwrap())
                })
                .collect();
            let docs = txn.open_table(tables::DOCS).unwrap();
            let mut entries = txn.open_table(tables::INDEX_ENTRIES).unwrap();
            for (d, c, mut meta) in rows {
                for index in meta.indexes.iter_mut() {
                    if unique.contains(&index.name.as_str()) {
                        index.unique = true;
                    }
                    let Some(filter) = index.partial_filter.clone() else { continue };
                    crate::index::clear_index_entries(
                        &mut entries,
                        crate::index::index_id_range(meta.id, index.id),
                    )
                    .unwrap();
                    let whole = crate::IndexMeta { partial_filter: None, ..index.clone() };
                    for row in docs.range(crate::engine::doc_range(meta.id)).unwrap() {
                        let (key, value) = row.unwrap();
                        let doc = codec::decode_doc_record(value.value())
                            .unwrap()
                            .document()
                            .unwrap()
                            .unwrap();
                        if !old_rule(&filter, &doc) {
                            continue;
                        }
                        if let crate::index::DocumentKeys::Keyed { keys, .. } =
                            crate::index::document_keys(&whole, &doc).unwrap()
                        {
                            for k in keys {
                                entries
                                    .insert((meta.id.0, index.id, k.as_slice(), key.value().1), ())
                                    .unwrap();
                            }
                        }
                    }
                }
                collections
                    .insert((d.as_str(), c.as_str()), serde_json::to_vec(&meta).unwrap().as_slice())
                    .unwrap();
            }
            txn.open_table(tables::META)
                .unwrap()
                .insert(tables::META_FORMAT_VERSION, [3u8].as_slice())
                .unwrap();
        }
        txn.commit().unwrap();
    }

    /// Rewrite the stored `partialFilterExpression` of each named index to one
    /// this build refuses: a bound on a `Decimal128`, which `PartialFilter::parse`
    /// declines because it ranks equal to every other number (ADR-181).
    ///
    /// Written straight into the metadata, because `create_index_inner` parses
    /// the filter for every origin, so no door of this build stores one. That is
    /// the point of the case: such a definition can only have been stored by an
    /// earlier build whose parser accepted it.
    fn store_unparseable_filter(path: &std::path::Path, named: &[(&str, &str, &str)]) {
        let db = Database::create(path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut collections = txn.open_table(tables::COLLECTIONS).unwrap();
            let rows: Vec<(String, String, CollectionMeta)> = collections
                .iter()
                .unwrap()
                .map(|row| {
                    let (k, v) = row.unwrap();
                    let (d, c) = k.value();
                    (d.to_string(), c.to_string(), serde_json::from_slice(v.value()).unwrap())
                })
                .collect();
            for (d, c, mut meta) in rows {
                let mut touched = false;
                for index in meta.indexes.iter_mut() {
                    if named
                        .iter()
                        .any(|(db, coll, name)| *db == d && *coll == c && *name == index.name)
                    {
                        index.partial_filter = Some(doc! {"size": {"$gt":
                        bson::Bson::Decimal128(bson::Decimal128::from_bytes([0; 16]))}});
                        touched = true;
                    }
                }
                assert!(touched || named.iter().all(|(db, coll, _)| *db != d || *coll != c));
                if touched {
                    collections
                        .insert(
                            (d.as_str(), c.as_str()),
                            serde_json::to_vec(&meta).unwrap().as_slice(),
                        )
                        .unwrap();
                }
            }
        }
        txn.commit().unwrap();
    }

    /// How many index entries the file holds for `db.coll`, over every index.
    fn entry_count(path: &std::path::Path, db_name: &str, coll: &str) -> usize {
        let db = Database::create(path).unwrap();
        let txn = db.begin_read().unwrap();
        let collections = txn.open_table(tables::COLLECTIONS).unwrap();
        let meta: CollectionMeta =
            serde_json::from_slice(collections.get((db_name, coll)).unwrap().unwrap().value())
                .unwrap();
        let entries = txn.open_table(tables::INDEX_ENTRIES).unwrap();
        meta.indexes
            .iter()
            .map(|index| {
                entries.range(crate::index::index_id_range(meta.id, index.id)).unwrap().count()
            })
            .sum()
    }

    /// A schema 3 database holding two partial indexes whose stored filters this
    /// build refuses: one on a collection with documents, one on an empty
    /// collection.
    ///
    /// The empty one is the case the rebuild alone cannot see. It is handed no
    /// document, so `document_keys` never parses its filter, and the migration
    /// would mark it rebuilt and move on.
    fn schema_3_with_unparseable_filters() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            for name in ["held", "empty"] {
                engine.create_collection("shop", name).unwrap();
                engine
                    .create_index_with(
                        "shop",
                        name,
                        vec![crate::meta::IndexField::ascending("x")],
                        false,
                        crate::meta::Enforcement::Local,
                        Some("by_size".into()),
                        None,
                        Some(doc! {"size": {"$gt": 5}}),
                    )
                    .unwrap();
            }
            let held = engine.get_collection("shop", "held").unwrap();
            engine
                .insert_many(
                    &held,
                    (0..5_i64).map(|i| doc! {"_id": i, "size": 10, "x": i}).collect(),
                )
                .unwrap();
        }
        as_schema_3(&path, &[]);
        store_unparseable_filter(
            &path,
            &[("shop", "held", "by_size"), ("shop", "empty", "by_size")],
        );
        (dir, path)
    }

    /// A schema 3 database holding a partial index and three documents: one the
    /// filter decides against, one it selects, and one it cannot decide.
    ///
    /// A function rather than a block, because the engine has to be dropped
    /// before the file can be reopened — and a block did not do it.
    fn schema_3_with_an_undecidable_document() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("shop", "orders").unwrap();
            engine
                .create_index_with(
                    "shop",
                    "orders",
                    vec![crate::meta::IndexField::ascending("x")],
                    false,
                    crate::meta::Enforcement::Local,
                    Some("by_size".into()),
                    None,
                    Some(doc! {"size": {"$gt": 5}}),
                )
                .unwrap();
            let coll = engine.get_collection("shop", "orders").unwrap();
            engine
                .insert_many(
                    &coll,
                    vec![
                        doc! {"_id": 1_i64, "size": 1, "x": 1},
                        doc! {"_id": 2_i64, "size": 10, "x": 2},
                        doc! {"_id": 3_i64, "size": Bson::Decimal128("1".parse().unwrap()), "x": 3},
                    ],
                )
                .unwrap();
        }
        as_schema_3(&path, &[]);
        (dir, path)
    }

    #[test]
    fn the_rebuild_files_a_document_its_filter_cannot_decide() {
        // **The carrier nothing held.** The 3 -> 4 rebuild has its own
        // `Undecidable` arm, and it is the path *every existing database* takes
        // to ADR-185's membership: a live node never re-files these documents,
        // the migration does. Deleting that arm left the whole suite green.
        let (_dir, path) = schema_3_with_an_undecidable_document();
        assert_eq!(version(&path), Some(3), "premise: a schema 3 database");

        let engine = Engine::open(&path).unwrap();
        assert_eq!(
            stored_version(engine.db()).unwrap(),
            Some(4),
            "the open migrated it — read through the open engine, since the file cannot be \
             opened twice"
        );
        let coll = engine.get_collection("shop", "orders").unwrap();
        let id = coll.index("by_size").unwrap().id;
        assert_eq!(
            engine.undecidable_count(&coll, id).unwrap(),
            1,
            "the rebuild must file the document its filter cannot decide, or every database that \
             migrates loses it from this index until something rewrites it"
        );
        assert_eq!(
            engine.unkeyed_count(&coll, id).unwrap(),
            0,
            "and under the reason that is true of it, not the other one"
        );
    }

    #[test]
    fn an_interrupted_rebuild_files_the_documents_its_filters_cannot_decide_when_it_resumes() {
        // The same carrier through the resume. Two partial indexes, the run
        // stopped after the first commits: the second is rebuilt on the next
        // open, from the markers, and must file the undecidable document as the
        // first did — and the first must not be redone.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("shop", "orders").unwrap();
            for (name, field) in [("by_x", "x"), ("by_y", "y")] {
                engine
                    .create_index_with(
                        "shop",
                        "orders",
                        vec![crate::meta::IndexField::ascending(field)],
                        false,
                        crate::meta::Enforcement::Local,
                        Some(name.into()),
                        None,
                        Some(doc! {"size": {"$gt": 5}}),
                    )
                    .unwrap();
            }
            let coll = engine.get_collection("shop", "orders").unwrap();
            engine
                .insert_many(
                    &coll,
                    vec![
                        doc! {"_id": 1_i64, "size": 1, "x": 1, "y": 1},
                        doc! {"_id": 2_i64, "size": 10, "x": 2, "y": 2},
                        doc! {"_id": 3_i64, "size": Bson::Decimal128("1".parse().unwrap()), "x": 3, "y": 3},
                    ],
                )
                .unwrap();
        }
        as_schema_3(&path, &[]);

        hooks::fail_at_index(2);
        assert!(Engine::open(&path).is_err(), "premise: the failure was reached");
        assert!(
            partial_rebuild_owed(&Database::create(&path).unwrap()).unwrap(),
            "premise: the second index is still owed"
        );

        let engine = Engine::open(&path).unwrap();
        let coll = engine.get_collection("shop", "orders").unwrap();
        for name in ["by_x", "by_y"] {
            let id = coll.index(name).unwrap().id;
            assert_eq!(
                engine.undecidable_count(&coll, id).unwrap(),
                1,
                "{name}: the document its filter cannot decide, filed by the run that built it"
            );
            assert_eq!(engine.unkeyed_count(&coll, id).unwrap(), 0, "{name}: and only there");
        }
    }

    #[test]
    fn a_stored_filter_this_build_refuses_stops_the_migration_before_it_writes() {
        let (_dir, path) = schema_3_with_unparseable_filters();
        let before = entry_count(&path, "shop", "held");
        assert!(before > 0, "premise: the index the migration would rebuild holds entries");
        assert_eq!(version(&path), Some(3), "premise: a schema 3 database");

        let err = match Engine::open(&path) {
            Err(e) => e,
            Ok(_) => panic!("the open must refuse a filter it cannot parse"),
        };

        let StorageError::UnparseablePartialFilter { refused, .. } = &err else {
            panic!("the wrong error: {err}");
        };
        // Both offenders in one refusal, so one pass with the previous build
        // fixes them: the one on a collection holding documents, and the one no
        // rebuild would ever have parsed.
        assert_eq!(refused.len(), 2, "{refused:?}");
        for coll in ["held", "empty"] {
            let named = format!("shop.{coll} index \"by_size\"");
            assert!(refused.iter().any(|r| r.contains(&named)), "{coll} unnamed: {refused:?}");
        }
        let message = err.to_string();
        assert!(message.contains("Decimal128"), "no parse error in the message: {message}");
        assert!(
            message.contains("previous build") && message.contains("drop each index"),
            "no remedy in the message: {message}"
        );

        // Nothing written: the previous build still opens this directory, and
        // the entries a rebuild would have cleared are where they were.
        assert_eq!(version(&path), Some(3), "the version is still 3");
        assert_eq!(entry_count(&path, "shop", "held"), before, "no entries were cleared");
    }

    #[test]
    fn a_refused_filter_is_found_on_a_resume_too() {
        // An interrupted migration leaves schema 4 with markers, and resumes
        // through them rather than through the version. The refusal has to run
        // on that path as well, before the rebuild goes any further.
        let (_dir, path) = fixture();
        hooks::fail_at_index(2);
        assert!(Engine::open(&path).is_err(), "premise: the failure was reached");
        assert_eq!(version(&path), Some(SCHEMA_VERSION), "premise: schema 4 already");
        assert!(
            partial_rebuild_owed(&Database::create(&path).unwrap()).unwrap(),
            "premise: with an index still owed"
        );
        store_unparseable_filter(&path, &[("shop", "t", "by_range")]);

        let err = match Engine::open(&path) {
            Err(e) => e,
            Ok(_) => panic!("the resume must refuse a filter it cannot parse"),
        };

        assert!(
            matches!(&err, StorageError::UnparseablePartialFilter { found, refused }
                if *found == SCHEMA_VERSION && refused.len() == 1),
            "the wrong error: {err}"
        );
        assert!(
            partial_rebuild_owed(&Database::create(&path).unwrap()).unwrap(),
            "and the rebuild went no further: the index is still owed"
        );
    }

    #[test]
    fn a_refused_filter_stops_a_schema_1_source_before_its_ids_are_derived() {
        // A schema 1 source steps through the id-deriving migrations before the
        // rebuild. The refusal runs before those, so the claim it makes -- that
        // nothing was changed and the previous build still opens this directory
        // -- is true, and the version it names is the one actually on disk.
        let (_dir, path) = fixture();
        drop(Engine::open(&path).unwrap());
        store_unparseable_filter(&path, &[("shop", "t", "by_range")]);
        super::tests::rewind_to_schema_1(&path, &[("shop", "t", 7)]);
        assert_eq!(version(&path), Some(1), "premise: a schema 1 database");

        let err = match Engine::open(&path) {
            Err(e) => e,
            Ok(_) => panic!("the open must refuse before deriving anything"),
        };

        assert!(
            matches!(&err, StorageError::UnparseablePartialFilter { found, .. } if *found == 1),
            "the message must name the version on disk, not 3: {err}"
        );
        // Not the version alone: the id-deriving steps do not write the version
        // byte, so a refusal that ran *after* them would leave it at 1 and this
        // test would pass while the bytes had already moved. The collection must
        // still be under the counter id the rewind gave it, which is what
        // `derive_collection_ids` would have rewritten along with every document
        // key and index entry belonging to it.
        assert_eq!(version(&path), Some(1), "the version was not written");
        assert_eq!(
            stored_collection_id(&path, "shop", "t"),
            7,
            "the collection is still under its counter id, so no byte was moved"
        );
    }

    /// The id a collection is stored under, read from the file.
    fn stored_collection_id(path: &std::path::Path, db_name: &str, coll: &str) -> u64 {
        let db = Database::create(path).unwrap();
        let txn = db.begin_read().unwrap();
        let collections = txn.open_table(tables::COLLECTIONS).unwrap();
        let meta: CollectionMeta =
            serde_json::from_slice(collections.get((db_name, coll)).unwrap().unwrap().value())
                .unwrap();
        meta.id.0
    }

    #[test]
    fn a_refused_filter_on_an_empty_collection_stops_the_migration_too() {
        // The hole the up-front parse closes, on its own. A rebuild is handed
        // this index no document, so it never parses the filter: without the
        // check the migration completes, marks the index rebuilt and writes
        // schema 4, and the definition this build cannot read is then carried
        // by a schema 4 database for good -- which is what ADR-181 relies on
        // not happening when it calls the condition transient.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("shop", "empty").unwrap();
            engine
                .create_index_with(
                    "shop",
                    "empty",
                    vec![crate::meta::IndexField::ascending("x")],
                    false,
                    crate::meta::Enforcement::Local,
                    Some("by_size".into()),
                    None,
                    Some(doc! {"size": {"$gt": 5}}),
                )
                .unwrap();
        }
        as_schema_3(&path, &[]);
        store_unparseable_filter(&path, &[("shop", "empty", "by_size")]);
        assert_eq!(version(&path), Some(3), "premise: a schema 3 database");

        let err = match Engine::open(&path) {
            Err(e) => e,
            Ok(_) => panic!("the open must refuse, though no document is ever indexed"),
        };

        assert!(
            matches!(&err, StorageError::UnparseablePartialFilter { refused, .. } if refused.len() == 1),
            "the wrong error: {err}"
        );
        assert_eq!(version(&path), Some(3), "the version is still 3, not 4");
    }

    /// The `_id`s an index holds, read from the file.
    fn members(path: &std::path::Path, index: &str) -> BTreeSet<i64> {
        let db = Database::create(path).unwrap();
        let txn = db.begin_read().unwrap();
        let collections = txn.open_table(tables::COLLECTIONS).unwrap();
        let meta: CollectionMeta =
            serde_json::from_slice(collections.get(("shop", "t")).unwrap().unwrap().value())
                .unwrap();
        let id = meta.index(index).unwrap().id;
        let entries = txn.open_table(tables::INDEX_ENTRIES).unwrap();
        let docs = txn.open_table(tables::DOCS).unwrap();
        entries
            .range(crate::index::index_id_range(meta.id, id))
            .unwrap()
            .map(|row| {
                let (k, _) = row.unwrap();
                let doc_key = k.value().3.to_vec();
                let raw = docs.get((meta.id.0, doc_key.as_slice())).unwrap().unwrap();
                let doc =
                    codec::decode_doc_record(raw.value()).unwrap().document().unwrap().unwrap();
                doc.get("_id")
                    .unwrap()
                    .as_i64()
                    .or_else(|| doc.get_i32("_id").ok().map(i64::from))
                    .unwrap()
            })
            .collect()
    }

    fn version(path: &std::path::Path) -> Option<u8> {
        stored_version(&Database::create(path).unwrap()).unwrap()
    }

    /// `shop.t` with two partial indexes on `x`, one over an array operand
    /// and one over a range, and twenty-five other documents.
    fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let engine = Engine::open(&path).unwrap();
        let t = engine.create_collection("shop", "t").unwrap();
        let mut docs = vec![
            doc! {"_id": 1_i64, "k": [1, 2], "x": 1},
            doc! {"_id": 3_i64, "k": [[1, 2]], "x": 3},
            doc! {"_id": 4_i64, "size": 10, "x": 4},
            doc! {"_id": 5_i64, "size": "large", "x": 5},
        ];
        docs.extend((100..125_i64).map(|i| doc! {"_id": i, "x": i}));
        engine.insert_many(&t, docs).unwrap();
        for (name, filter) in
            [("by_array", doc! {"k": [1, 2]}), ("by_range", doc! {"size": {"$gt": 5}})]
        {
            engine
                .create_index_with(
                    "shop",
                    "t",
                    vec![crate::meta::IndexField::ascending("x")],
                    false,
                    crate::meta::Enforcement::Local,
                    Some(name.into()),
                    None,
                    Some(filter),
                )
                .unwrap();
        }
        drop(engine);
        as_schema_3(&path, &[]);
        (dir, path)
    }

    fn assert_the_fixture_is_the_old_membership(path: &std::path::Path) {
        assert_eq!(
            members(path, "by_array"),
            BTreeSet::from([3]),
            "premise: lacks the whole array"
        );
        assert_eq!(members(path, "by_range"), BTreeSet::from([4, 5]), "premise: holds the string");
    }

    #[test]
    fn a_partial_index_built_under_the_old_rule_is_rebuilt_to_what_find_selects() {
        let (_dir, path) = fixture();
        assert_the_fixture_is_the_old_membership(&path);
        let plan = partial_rebuild_plan(&Database::create(&path).unwrap()).unwrap();
        assert_eq!(plan.indexes.len(), 2);
        assert_eq!(plan.documents, 2 * 29, "each index scans the collection's 29 documents");
        assert_eq!(plan.largest_documents, 29);
        let lines_before = hooks::progress_lines();

        drop(Engine::open(&path).unwrap());

        assert_eq!(members(&path, "by_array"), BTreeSet::from([1, 3]));
        assert_eq!(members(&path, "by_range"), BTreeSet::from([4]));
        assert_eq!(version(&path), Some(SCHEMA_VERSION));
        let db = Database::create(&path).unwrap();
        assert!(
            db.begin_read().unwrap().open_table(PARTIAL_REBUILT).is_err(),
            "the markers are gone"
        );
        assert!(hooks::progress_lines() - lines_before >= 4, "every ten documents, on each index");
    }

    /// Rewrites the kept counts of every collection to `n` in place, leaving
    /// their mark alone.
    fn set_kept_counts(path: &std::path::Path, n: Option<u64>) {
        let db = Database::create(path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut counts = txn.open_table(tables::LIVE_COUNTS).unwrap();
            let ids: Vec<u64> = counts.iter().unwrap().map(|row| row.unwrap().0.value()).collect();
            assert!(!ids.is_empty(), "premise: the fixture's counts are kept");
            for id in ids {
                match n {
                    Some(n) => counts.insert(id, n).unwrap(),
                    None => counts.remove(id).unwrap(),
                };
            }
        }
        txn.commit().unwrap();
    }

    #[test]
    fn the_estimate_is_for_the_whole_migration_and_not_for_one_index() {
        // What an operator decides whether to wait on: several large indexes
        // in sequence, not the largest of them.
        let plan = PartialRebuildPlan {
            indexes: Vec::new(),
            documents: 15_000_000,
            largest_documents: 10_000_000,
        };
        assert_eq!(plan.estimate_secs(), 15_000_000 * MICROS_PER_DOCUMENT / 1_000_000);
        assert_ne!(
            plan.estimate_secs(),
            10_000_000 * MICROS_PER_DOCUMENT / 1_000_000,
            "premise: the total and the largest give different estimates"
        );
    }

    #[test]
    fn the_announcement_reads_the_kept_counts_when_they_are_current() {
        let (_dir, path) = fixture();
        set_kept_counts(&path, Some(7));
        let db = Database::create(&path).unwrap();
        assert!(
            crate::live_count::counts_are_current(&db.begin_read().unwrap()).unwrap(),
            "premise: rewriting a count in place leaves its mark matching"
        );
        let plan = partial_rebuild_plan(&db).unwrap();
        // Seven, not the 29 documents there are: the count was read, not
        // walked for.
        assert_eq!((plan.documents, plan.largest_documents), (2 * 7, 7));
    }

    #[test]
    fn the_announcement_counts_the_collection_when_a_restore_left_no_counts() {
        let (_dir, path) = fixture();
        // What a backup restores: neither the counts nor their mark (ADR-174).
        set_kept_counts(&path, None);
        {
            let db = Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_COUNTS_THROUGH)
                .unwrap()
                .remove(crate::live_count::THROUGH)
                .unwrap()
                .expect("premise: the fixture's counts had a mark");
            txn.commit().unwrap();
        }
        let plan = partial_rebuild_plan(&Database::create(&path).unwrap()).unwrap();
        assert_eq!((plan.documents, plan.largest_documents), (2 * 29, 29));
    }

    #[test]
    fn the_announcement_counts_the_collection_when_the_counts_mark_is_behind() {
        let (_dir, path) = fixture();
        // An older build wrote since: the counts stand at 7 and the mark no
        // longer matches the oplog.
        set_kept_counts(&path, Some(7));
        {
            let db = Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(tables::LIVE_COUNTS_THROUGH)
                .unwrap()
                .insert(crate::live_count::THROUGH, &[0u8; 8][..])
                .unwrap();
            txn.commit().unwrap();
        }
        let plan = partial_rebuild_plan(&Database::create(&path).unwrap()).unwrap();
        assert_eq!((plan.documents, plan.largest_documents), (2 * 29, 29));
    }

    #[test]
    fn a_migration_interrupted_between_indexes_finishes_without_redoing_one() {
        let (_dir, path) = fixture();
        assert_the_fixture_is_the_old_membership(&path);

        hooks::fail_at_index(2);
        assert!(Engine::open(&path).is_err(), "premise: the failure was reached");

        // Schema 4 already, with the markers still there. The version goes down
        // in the first index's commit so that a file in this state -- one index
        // rebuilt, one still holding what the old rule selected -- is refused by
        // the previous build rather than opened and maintained under that rule.
        assert_eq!(version(&path), Some(SCHEMA_VERSION), "the version went down with index 1");
        assert!(
            partial_rebuild_owed(&Database::create(&path).unwrap()).unwrap(),
            "the markers say the migration is not finished"
        );
        assert_eq!(members(&path, "by_array"), BTreeSet::from([1, 3]), "the first index committed");
        assert_eq!(members(&path, "by_range"), BTreeSet::from([4, 5]), "the second rolled back");
        let plan = partial_rebuild_plan(&Database::create(&path).unwrap()).unwrap();
        let pending: Vec<&str> = plan.indexes.iter().map(|(_, i, _)| i.name.as_str()).collect();
        assert_eq!(pending, ["by_range"], "only the unfinished index is left to do");

        // Reopening resumes from the markers, finishes, and clears them. The
        // version was already 4, so the resume is reached through the marker
        // table rather than through the version.
        drop(Engine::open(&path).unwrap());
        assert_eq!(members(&path, "by_range"), BTreeSet::from([4]));
        assert_eq!(members(&path, "by_array"), BTreeSet::from([1, 3]), "and index 1 not redone");
        assert_eq!(version(&path), Some(SCHEMA_VERSION));
        assert!(
            !partial_rebuild_owed(&Database::create(&path).unwrap()).unwrap(),
            "the markers are gone, which is what finished means"
        );
    }

    /// ADR-190: the sidecar records the new schema before the first
    /// write of a migration. A migration that fails at its first index, before
    /// that commit, leaves the sidecar ahead of META, so a build of the old
    /// schema refuses the store without touching it rather than opening one
    /// whose migration has begun.
    #[test]
    fn the_sidecar_is_raised_before_a_migration_writes() {
        let (_dir, path) = fixture();
        assert_the_fixture_is_the_old_membership(&path);
        std::fs::remove_file(crate::format::sidecar_path(&path)).unwrap();

        hooks::fail_at_index(1);
        assert!(Engine::open(&path).is_err(), "premise: the first index's commit failed");
        assert_eq!(version(&path), Some(3), "META did not move");
        assert_eq!(
            crate::format::read_sidecar(&path).map(|s| s.schema),
            Some(SCHEMA_VERSION),
            "the sidecar did, before the first write"
        );

        let before = (
            std::fs::read(&path).unwrap(),
            std::fs::read(crate::format::sidecar_path(&path)).unwrap(),
        );
        let schema_3 =
            crate::format::BuildVersions { schema: 3, ..crate::format::BuildVersions::ours() };
        assert!(matches!(
            Engine::open_as(&path, None, &schema_3),
            Err(StorageError::RefusedStore(_))
        ));
        let after = (
            std::fs::read(&path).unwrap(),
            std::fs::read(crate::format::sidecar_path(&path)).unwrap(),
        );
        assert!(before == after, "refused untouched");

        drop(Engine::open(&path).expect("this build resumes the migration"));
        assert_eq!(version(&path), Some(SCHEMA_VERSION));
    }

    #[test]
    fn a_rebuild_that_takes_in_an_array_raises_multikey() {
        // A document the old rule left out now belongs, and it holds an array
        // at the indexed path. Left unraised, the planner would read both ends
        // of a range on an index that is multikey, and lose rows.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            let t = engine.create_collection("shop", "t").unwrap();
            engine.insert(&t, doc! {"_id": 1_i64, "k": [1, 2], "tags": ["a", "b"]}).unwrap();
            engine
                .create_index_with(
                    "shop",
                    "t",
                    vec![crate::meta::IndexField::ascending("tags")],
                    false,
                    crate::meta::Enforcement::Local,
                    Some("tagged".into()),
                    None,
                    Some(doc! {"k": [1, 2]}),
                )
                .unwrap();
        }
        as_schema_3(&path, &[]);
        {
            // What a schema 3 node holds: the flag as its own build left it.
            let db = Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut collections = txn.open_table(tables::COLLECTIONS).unwrap();
                let mut meta: CollectionMeta = serde_json::from_slice(
                    collections.get(("shop", "t")).unwrap().unwrap().value(),
                )
                .unwrap();
                meta.indexes[0].multikey = false;
                collections
                    .insert(("shop", "t"), serde_json::to_vec(&meta).unwrap().as_slice())
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        assert!(members(&path, "tagged").is_empty(), "premise: the old rule left it out");

        let engine = Engine::open(&path).unwrap();

        let index = engine.get_collection("shop", "t").unwrap().index("tagged").unwrap().clone();
        assert!(index.multikey, "the rebuild took in an array, and says so");
        drop(engine);
        assert_eq!(members(&path, "tagged"), BTreeSet::from([1]));
    }

    #[test]
    fn collisions_a_rebuild_found_survive_an_interrupt_and_are_reported_on_the_next_open() {
        // The rebuild finds the shared key, and the migration is interrupted
        // before it finishes. Reporting needs an engine, which does not exist
        // until the migration returns, so until this was recorded in the same
        // commit as the index's marker the finding was simply lost: both
        // duplicates stayed in the rebuilt index, and nothing counted, logged
        // or recorded them. The index stays marked done, so no later run looks
        // again.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            let t = engine.create_collection("shop", "t").unwrap();
            engine
                .insert_many(
                    &t,
                    vec![
                        doc! {"_id": 1_i64, "k": [1, 2], "email": "a", "size": 10},
                        doc! {"_id": 2_i64, "k": [1, 2], "email": "a", "size": 10},
                    ],
                )
                .unwrap();
            // The unique one first, so it is the index that commits, and a
            // second partial index after it, so there is something to fail on.
            for (name, unique, filter) in [
                ("email_once", true, doc! {"k": [1, 2]}),
                ("by_range", false, doc! {"size": {"$gt": 5}}),
            ] {
                engine
                    .create_index_with(
                        "shop",
                        "t",
                        vec![crate::meta::IndexField::ascending(if unique {
                            "email"
                        } else {
                            "size"
                        })],
                        false,
                        crate::meta::Enforcement::Local,
                        Some(name.into()),
                        None,
                        Some(filter),
                    )
                    .unwrap();
            }
        }
        as_schema_3(&path, &["email_once"]);
        assert!(members(&path, "email_once").is_empty(), "premise: neither was a member");

        hooks::fail_at_index(2);
        assert!(Engine::open(&path).is_err(), "premise: the failure was reached");
        assert_eq!(
            members(&path, "email_once"),
            BTreeSet::from([1, 2]),
            "premise: the unique index committed, holding both duplicates"
        );
        assert!(
            partial_rebuild_owed(&Database::create(&path).unwrap()).unwrap(),
            "premise: the migration is unfinished, and email_once is marked done"
        );

        // The next open resumes, and reports what the interrupted run found.
        let engine = Engine::open(&path).unwrap();
        assert_eq!(engine.unique_violations(), 1, "the collision survived the interrupt");
        drop(engine);
        assert_eq!(version(&path), Some(SCHEMA_VERSION));

        // Reported once, not for ever: the record is forgotten afterwards.
        let engine = Engine::open(&path).unwrap();
        assert_eq!(engine.unique_violations(), 0, "already reported, so not reported again");
    }

    #[test]
    fn a_unique_index_the_rebuild_finds_shared_keys_in_reports_them_and_completes() {
        // Two documents sharing an email, outside the old membership of a
        // unique index over an array operand: accepted while the constraint
        // was misapplied. The rebuild takes both in, and a migration cannot
        // refuse -- so they are reported as a replicated build reports them.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            let t = engine.create_collection("shop", "t").unwrap();
            engine
                .insert_many(
                    &t,
                    vec![
                        doc! {"_id": 1_i64, "k": [1, 2], "email": "a"},
                        doc! {"_id": 2_i64, "k": [1, 2], "email": "a"},
                    ],
                )
                .unwrap();
            engine
                .create_index_with(
                    "shop",
                    "t",
                    vec![crate::meta::IndexField::ascending("email")],
                    false,
                    crate::meta::Enforcement::Local,
                    Some("email_once".into()),
                    None,
                    Some(doc! {"k": [1, 2]}),
                )
                .unwrap();
        }
        as_schema_3(&path, &["email_once"]);
        assert!(members(&path, "email_once").is_empty(), "premise: neither was a member");

        let engine = Engine::open(&path).unwrap();

        assert_eq!(engine.unique_violations(), 1, "one shared key, reported");
        drop(engine);
        assert_eq!(members(&path, "email_once"), BTreeSet::from([1, 2]), "built in full");
        assert_eq!(version(&path), Some(SCHEMA_VERSION));
    }
}

/// What the schema 3 -> 4 migration costs on a realistic collection
/// (ADR-183): a harness, not a test, run in release on the machine a figure
/// is quoted from.
///
/// ```text
/// MIGRATION_N=10000000 cargo test --release -p kimmy-storage --lib \
///     migrate::migration_cost -- --ignored --nocapture
/// ```
#[cfg(test)]
mod migration_cost {
    use bson::doc;

    use super::*;
    use crate::Engine;

    #[test]
    #[ignore = "a measurement harness: see the module documentation"]
    fn measure() {
        let n: i64 = std::env::var("MIGRATION_N").map(|s| s.parse().unwrap()).unwrap_or(1_000_000);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            let t = engine.create_collection("shop", "t").unwrap();
            let pad = "p".repeat(200);
            for start in (0..n).step_by(10_000) {
                let docs: Vec<bson::Document> = (start..(start + 10_000).min(n))
                    .map(|i| {
                        let size = if i % 7 == 0 { bson::Bson::from("large") } else { (i % 20).into() };
                        doc! {"_id": i, "size": size, "at": bson::DateTime::from_millis(i), "pad": &pad}
                    })
                    .collect();
                engine.insert_many(&t, docs).unwrap();
            }
            engine
                .create_index_with(
                    "shop",
                    "t",
                    vec![crate::meta::IndexField::ascending("at")],
                    false,
                    crate::meta::Enforcement::Local,
                    Some("ttl".into()),
                    Some(3600),
                    Some(doc! {"size": {"$gt": 5}}),
                )
                .unwrap();
        }
        write_version(&redb::Database::create(&path).unwrap(), 3).unwrap();
        let plan = partial_rebuild_plan(&redb::Database::create(&path).unwrap()).unwrap();
        let before = std::fs::metadata(&path).unwrap().len() >> 20;
        let planned = std::time::Instant::now();
        drop(partial_rebuild_plan(&redb::Database::create(&path).unwrap()).unwrap());
        let planning = planned.elapsed();
        let started = std::time::Instant::now();
        drop(Engine::open(&path).unwrap());
        let migrated = started.elapsed();
        // The control: the same open with nothing to migrate.
        let reopened = std::time::Instant::now();
        drop(Engine::open(&path).unwrap());
        eprintln!(
            "MIGRATION n={n}: planning alone {planning:?}; an ordinary open afterwards {:?}",
            reopened.elapsed()
        );
        eprintln!(
            "MIGRATION n={n}: open with the migration took {migrated:?}; announced {} documents, \
             estimate {} s, largest collection {} documents needing up to {} MiB; file {before} -> \
             {} MiB",
            plan.documents,
            plan.estimate_secs(),
            plan.largest_documents,
            plan.largest_needs_mib(),
            std::fs::metadata(&path).unwrap().len() >> 20
        );
    }
}
