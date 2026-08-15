//! The storage engine.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kimmy_core::{
    CollectionId, Error as CoreError, Hlc, HlcClock, NodeId, OpKind, OplogEntry, Stamp,
};
use parking_lot::Mutex;
use redb::{Database, ReadableDatabase, ReadableTable};
use tokio::sync::broadcast;
use tracing::{debug, info};

use crate::codec;
use crate::error::{Result, StorageError};
use crate::meta::{CollectionMeta, DatabaseMeta};
use crate::tables;

/// How many change events are buffered per subscriber before it is considered
/// too slow. A lagging subscriber is told to resubscribe rather than being
/// allowed to stall writers.
const EVENT_BUFFER: usize = 1024;

pub struct Engine {
    db: Database,
    node_id: NodeId,
    /// Guards the HLC. Every write takes this briefly to mint a stamp, so it
    /// must never be held across a redb commit.
    clock: Mutex<HlcClock>,
    events: broadcast::Sender<Arc<OplogEntry>>,
    /// Bumped whenever a collection's vectors change.
    ///
    /// An in-memory vector index is built from a snapshot and cannot see later
    /// writes. Counting vectors to detect that would be O(n) per query, and a
    /// count misses the case where one document is deleted and another added.
    /// A counter is exact and free.
    vector_generations: Mutex<std::collections::HashMap<CollectionId, u64>>,
    /// Where the database file lives, kept so that size can be reported
    /// without the caller having to remember what it opened.
    path: std::path::PathBuf,
    /// Unique constraints broken by merging replicated writes, since start.
    ///
    /// A counter rather than only a log line so the condition is visible on the
    /// metrics endpoint without anyone having to be watching a stream when it
    /// happens — see [ADR-020](../../../docs/decisions.md).
    unique_violations: std::sync::atomic::AtomicU64,
    /// Durable write transactions committed, since start.
    ///
    /// redb has a single writer and every commit is an fsync, so the number of
    /// commits a piece of work costs is the thing that decides what a write is
    /// worth — not how much of it is CPU. Counting them makes "an insert is one
    /// commit" a property of a running node rather than a claim in a comment,
    /// which is what M11 task 1 needed: the daemon was paying two commits per
    /// insert where a bare engine paid one, and nothing said so.
    commits: std::sync::atomic::AtomicU64,
}

/// A write transaction that counts itself when it commits.
///
/// Aborts are not counted, deliberately: an abort does not fsync, and the
/// question this exists to answer is how many times a write path reaches the
/// disk. Derefs to the redb transaction, so `open_table` and the rest are
/// unchanged at the call sites.
pub(crate) struct WriteTxn<'a> {
    txn: redb::WriteTransaction,
    commits: &'a std::sync::atomic::AtomicU64,
}

impl WriteTxn<'_> {
    pub(crate) fn commit(self) -> std::result::Result<(), redb::CommitError> {
        self.txn.commit()?;
        self.commits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn abort(self) -> std::result::Result<(), redb::StorageError> {
        self.txn.abort()
    }
}

impl std::ops::Deref for WriteTxn<'_> {
    type Target = redb::WriteTransaction;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl Engine {
    /// Open or create the database at `path`.
    ///
    /// Node identity lives in the database file rather than beside it, so that
    /// copying or restoring the file carries the identity with it. Identity
    /// must survive restarts: it is the tiebreak half of every write's stamp.
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path)?;

        // Ensure every table exists up front so that read transactions never
        // have to handle a missing table.
        let txn = db.begin_write()?;
        {
            let _ = txn.open_table(tables::META)?;
            let _ = txn.open_table(tables::DATABASES)?;
            let _ = txn.open_table(tables::COLLECTIONS)?;
            let _ = txn.open_table(tables::DOCS)?;
            let _ = txn.open_table(tables::INDEX_ENTRIES)?;
            let _ = txn.open_table(tables::OPLOG)?;
            let _ = txn.open_table(tables::OPLOG_ARRIVAL)?;
            let _ = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;
            let _ = txn.open_table(tables::OPLOG_VERSIONS)?;
            let _ = txn.open_table(tables::OPLOG_WITNESSED)?;
            let _ = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        }
        txn.commit()?;

        // Before anything reads a collection id: schema 1 allocated them from a
        // counter, schema 2 derives them from the name.
        crate::migrate::run(&db)?;

        // The arrival index is derived from the oplog, so a database written
        // before it existed — or by a build that did not maintain it — is
        // repaired rather than refused. That is why adding it needed no format
        // version bump: there is no state here that the oplog does not already
        // determine.
        Self::rebuild_arrival_index_if_stale(&db)?;
        Self::rebuild_version_vector_if_stale(&db)?;

        let node_id = Self::load_or_create_node_id(&db)?;
        let resumed = Self::last_oplog_hlc(&db)?;

        if resumed != Hlc::ZERO {
            debug!(hlc = %resumed, "resumed logical clock from the oplog tail");
        }

        let (events, _) = broadcast::channel(EVENT_BUFFER);

        info!(node = %node_id, path = %path.display(), "storage engine open");

        Ok(Self {
            db,
            node_id,
            clock: Mutex::new(HlcClock::resuming_from(resumed)),
            events,
            vector_generations: Mutex::new(Default::default()),
            path: path.to_path_buf(),
            unique_violations: std::sync::atomic::AtomicU64::new(0),
            commits: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Size of the database file on disk, or zero if it cannot be read.
    ///
    /// Zero rather than an error: this exists for a metrics endpoint, and a
    /// gauge that fails the whole scrape because one `stat` did is worse than a
    /// gauge that reads zero.
    pub fn storage_bytes(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    /// How many times this collection's vectors have changed.
    ///
    /// Resets to zero on restart, which is correct: an in-memory index does
    /// not survive one either.
    pub fn vector_generation(&self, collection: CollectionId) -> u64 {
        self.vector_generations.lock().get(&collection).copied().unwrap_or(0)
    }

    /// How many merged writes have broken a unique constraint since start.
    pub fn unique_violations(&self) -> u64 {
        self.unique_violations.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many durable write transactions have committed since start.
    ///
    /// One per unit of work is the expectation. Anything that turns one
    /// client-visible write into two commits doubles what that write costs,
    /// and this is where that shows up.
    pub fn commits(&self) -> u64 {
        self.commits.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn count_unique_violation(&self) {
        self.unique_violations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn bump_vector_generation(&self, collection: CollectionId) {
        *self.vector_generations.lock().entry(collection).or_insert(0) += 1;
    }

    /// Subscribe to the live change feed.
    ///
    /// This is only half of a change stream: on its own it starts at "now" and
    /// misses anything already written. [`crate::watch`] combines it with an
    /// oplog replay to deliver a gap-free sequence.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<OplogEntry>> {
        self.events.subscribe()
    }

    fn load_or_create_node_id(db: &Database) -> Result<NodeId> {
        let txn = db.begin_write()?;
        let id = {
            let mut meta = txn.open_table(tables::META)?;

            // Read guards borrow the table, so copy out what we need before
            // any insert.
            let stored_node =
                meta.get(tables::META_NODE_ID)?.map(|v| <[u8; 16]>::try_from(v.value()));

            match stored_node {
                Some(bytes) => NodeId::from_bytes(
                    bytes.map_err(|_| StorageError::Corrupt("node id is not 16 bytes".into()))?,
                ),
                None => {
                    let id = NodeId::generate();
                    meta.insert(tables::META_NODE_ID, id.to_bytes().as_slice())?;
                    info!(node = %id, "generated a new node identity");
                    id
                }
            }
        };
        txn.commit()?;
        Ok(id)
    }

    /// Rebuild the arrival index if it does not cover the oplog exactly.
    ///
    /// Cheap to check — two counts — and only pays the rebuild when something
    /// is actually wrong: a database written before the index existed, or one
    /// an older build appended to after this one had created it. Comparing
    /// counts rather than contents is enough because the index is only ever
    /// written alongside an oplog append and collected alongside an oplog
    /// removal, so a length mismatch is the only way it can diverge.
    ///
    /// Existing history is ordered by stamp, which is correct: everything
    /// written before this index existed was locally originated, and for local
    /// writes arrival order *is* stamp order.
    fn rebuild_arrival_index_if_stale(db: &Database) -> Result<()> {
        {
            let txn = db.begin_read()?;
            let oplog = txn.open_table(tables::OPLOG)?;
            let arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
            if oplog.iter()?.count() == arrival.iter()?.count() {
                return Ok(());
            }
        }

        let txn = db.begin_write()?;
        let rebuilt = {
            let oplog = txn.open_table(tables::OPLOG)?;
            let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
            let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;
            arrival.retain(|_, _| false)?;
            by_stamp.retain(|_, _| false)?;

            let mut seq = 0u64;
            for row in oplog.iter()? {
                let (key, _) = row?;
                arrival.insert(seq, key.value())?;
                by_stamp.insert(key.value(), seq)?;
                seq += 1;
            }
            seq
        };
        txn.commit()?;

        if rebuilt > 0 {
            info!(entries = rebuilt, "rebuilt the oplog arrival index");
        }
        Ok(())
    }

    /// Raise the version vector to cover everything in the oplog.
    ///
    /// **Only ever raises.** The vector was derived state when the oplog was
    /// the sole way to gain coverage; a snapshot transfer grants coverage of
    /// entries this node will never hold, so the oplog is now a *lower bound*
    /// on what has been seen rather than the whole truth. Recomputing from it
    /// would silently undo a completed snapshot and send the node back to
    /// asking for history it has already been given another way.
    ///
    /// What it still does is repair a vector that has fallen behind: a database
    /// written before the vector existed, or one an older build appended to.
    pub(crate) fn rebuild_version_vector_if_stale(db: &Database) -> Result<()> {
        let mut actual = kimmy_core::VersionVector::new();
        {
            let txn = db.begin_read()?;
            let oplog = txn.open_table(tables::OPLOG)?;
            for row in oplog.iter()? {
                let (key, _) = row?;
                actual.observe(codec::decode_oplog_key(key.value())?);
            }
        }

        let mut stored = Self::read_versions(db, tables::OPLOG_VERSIONS)?;
        let before = stored.clone();
        stored.merge(&actual);

        // A database written before the witnessed vector existed has an empty
        // one. Seeding it from the servable vector is the safe lower bound:
        // the next sync round re-fetches once, witnesses what it processes,
        // and goes quiet (ADR-054).
        let mut witnessed = Self::read_versions(db, tables::OPLOG_WITNESSED)?;
        let witnessed_before = witnessed.clone();
        witnessed.merge(&stored);

        if stored == before && witnessed == witnessed_before {
            return Ok(());
        }

        let txn = db.begin_write()?;
        {
            let mut versions = txn.open_table(tables::OPLOG_VERSIONS)?;
            for (node, hlc) in stored.iter() {
                versions.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }
            let mut seen = txn.open_table(tables::OPLOG_WITNESSED)?;
            for (node, hlc) in witnessed.iter() {
                seen.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }
        }
        txn.commit()?;

        info!(nodes = stored.len(), "raised the version vector to cover the oplog");
        Ok(())
    }

    /// Replace the version vector with exactly what the oplog now covers.
    ///
    /// **The only legitimate lowering.** `rebuild_version_vector_if_stale`
    /// merges, so it can only raise: the vector is authoritative, and
    /// recomputing it during normal operation would discard coverage a snapshot
    /// granted that the oplog never held.
    ///
    /// A point-in-time rewind is the exception, because it *removed* oplog
    /// entries on purpose. Leaving the vector high afterwards would have the
    /// node claim history it no longer holds, and no peer would ever send that
    /// range again — the node would be permanently missing writes and would
    /// look caught up.
    pub(crate) fn reset_version_vector_to_oplog(db: &Database) -> Result<()> {
        let mut actual = kimmy_core::VersionVector::new();
        {
            let txn = db.begin_read()?;
            let oplog = txn.open_table(tables::OPLOG)?;
            for row in oplog.iter()? {
                let (key, _) = row?;
                actual.observe(codec::decode_oplog_key(key.value())?);
            }
        }

        let txn = db.begin_write()?;
        {
            // **Both** vectors. If witnessed stayed high, the node would
            // believe it had already seen the entries the rewind removed and
            // would never ask for them again — permanently missing writes
            // while looking caught up, which is the very failure this function
            // exists to prevent (ADR-054).
            let mut seen = txn.open_table(tables::OPLOG_WITNESSED)?;
            seen.retain(|_, _| false)?;
            for (node, hlc) in actual.iter() {
                seen.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }

            let mut versions = txn.open_table(tables::OPLOG_VERSIONS)?;
            // Cleared rather than overwritten: a node that appears in the old
            // vector but no longer in the oplog has to disappear entirely, and
            // inserting over the top would leave its stale entry behind.
            versions.retain(|_, _| false)?;
            for (node, hlc) in actual.iter() {
                versions.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }
        }
        txn.commit()?;

        info!(nodes = actual.len(), "reset the version vector to the rewound oplog");
        Ok(())
    }

    fn read_versions(
        db: &Database,
        table: redb::TableDefinition<&'static [u8], &'static [u8]>,
    ) -> Result<kimmy_core::VersionVector> {
        let txn = db.begin_read()?;
        let versions = txn.open_table(table)?;
        let mut out = kimmy_core::VersionVector::new();
        for row in versions.iter()? {
            let (node, hlc) = row?;
            out.insert(decode_node(node.value())?, decode_hlc(hlc.value())?);
        }
        Ok(out)
    }

    /// When this collection was dropped, if a tombstone still records it.
    ///
    /// `None` means either that it was never dropped or that the tombstone has
    /// been collected — the two are indistinguishable, which is exactly why
    /// `tombstone_retention_secs` must exceed the longest partition you intend
    /// to survive.
    pub fn collection_dropped_at(&self, id: CollectionId) -> Result<Option<Stamp>> {
        let txn = self.db.begin_read()?;
        let dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        match dropped.get(id.0)? {
            Some(raw) => Ok(Some(codec::decode_oplog_key(raw.value())?)),
            None => Ok(None),
        }
    }

    /// Record that a collection was dropped at `stamp`, if that is newer.
    pub(crate) fn record_collection_drop(&self, id: CollectionId, stamp: Stamp) -> Result<()> {
        let txn = self.begin_write()?;
        {
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            let newer = match dropped.get(id.0)? {
                Some(existing) => stamp > codec::decode_oplog_key(existing.value())?,
                None => true,
            };
            if newer {
                dropped.insert(id.0, codec::oplog_key(&stamp).as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Record coverage granted by a snapshot.
    ///
    /// Merged rather than replaced, so writes this node made that the sender
    /// never saw are not claimed to be forgotten.
    pub fn absorb_version_vector(&self, granted: &kimmy_core::VersionVector) -> Result<()> {
        let mut current = Self::read_versions(&self.db, tables::OPLOG_VERSIONS)?;
        current.merge(granted);

        // Both: a snapshot hands over state, which is the strongest form of
        // having processed everything behind it. Raising only the servable
        // vector would leave the node still asking for the history the
        // snapshot replaced.
        let mut witnessed = Self::read_versions(&self.db, tables::OPLOG_WITNESSED)?;
        witnessed.merge(&current);

        let txn = self.begin_write()?;
        {
            let mut seen = txn.open_table(tables::OPLOG_WITNESSED)?;
            for (node, hlc) in witnessed.iter() {
                seen.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }
            let mut versions = txn.open_table(tables::OPLOG_VERSIONS)?;
            for (node, hlc) in current.iter() {
                versions.insert(node.to_bytes().as_slice(), hlc.to_bytes().as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// The highest `Hlc` retention has removed from the oplog.
    ///
    /// [`Hlc::ZERO`] when nothing has ever been collected, which reads
    /// naturally: every peer is at or above it, so every peer can be served
    /// incrementally.
    pub fn oplog_collected_through(&self) -> Result<Hlc> {
        let txn = self.db.begin_read()?;
        let meta = txn.open_table(tables::META)?;
        match meta.get(tables::META_OPLOG_COLLECTED_THROUGH)? {
            Some(raw) => Ok(codec::decode_oplog_key(raw.value())?.hlc),
            None => Ok(Hlc::ZERO),
        }
    }

    /// The oldest stamp still in the oplog, if any.
    ///
    /// A peer asking from a point below this cannot be served incrementally:
    /// the history it is missing has been collected.
    pub fn oldest_retained(&self) -> Result<Option<Stamp>> {
        let txn = self.db.begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        match oplog.first()? {
            Some((key, _)) => Ok(Some(codec::decode_oplog_key(key.value())?)),
            None => Ok(None),
        }
    }

    /// What this node holds, summarized per originating node.
    ///
    /// The half of anti-entropy a peer needs in order to work out what to send.
    pub fn version_vector(&self) -> Result<kimmy_core::VersionVector> {
        Self::read_versions(&self.db, tables::OPLOG_VERSIONS)
    }

    /// What this node has **processed**, per origin — appended or not.
    ///
    /// This is what "am I behind a peer" and "how far behind" must be asked
    /// against. [`Self::version_vector`] answers a different question — what
    /// this node can *serve* — and is what a peer receives. Conflating them
    /// made every cluster re-request the same entries forever (ADR-054).
    pub fn witnessed_vector(&self) -> Result<kimmy_core::VersionVector> {
        Self::read_versions(&self.db, tables::OPLOG_WITNESSED)
    }

    /// Raise the witnessed vector to cover a whole batch, in one transaction.
    ///
    /// Only ever raises, like every other movement of a version vector: a
    /// lowering would send the node back to asking for history it has already
    /// processed.
    pub fn absorb_witnessed(&self, seen: &kimmy_core::VersionVector) -> Result<()> {
        if seen.is_empty() {
            return Ok(());
        }
        let txn = self.begin_write()?;
        {
            let mut table = txn.open_table(tables::OPLOG_WITNESSED)?;
            for (node, hlc) in seen.iter() {
                let key = node.to_bytes();
                let higher = match table.get(key.as_slice())? {
                    Some(current) => hlc > decode_hlc(current.value())?,
                    None => true,
                };
                if higher {
                    table.insert(key.as_slice(), hlc.to_bytes().as_slice())?;
                }
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Record that a stamp has been processed, whatever came of it.
    ///
    /// Durable, unlike [`Self::witness`], which only nudges the in-memory
    /// clock: a hole that survived a restart would restart the re-sync loop
    /// with it.
    pub fn witness_processed(&self, stamp: &Stamp) -> Result<()> {
        self.witness(stamp);
        let txn = self.begin_write()?;
        raise_version(&txn, tables::OPLOG_WITNESSED, stamp)?;
        txn.commit()?;
        Ok(())
    }

    /// The highest stamp in the oplog, used to resume the logical clock.
    ///
    /// Without this, a restart would mint stamps below ones already written,
    /// and a document updated after the restart could lose to its own older
    /// version under last-writer-wins.
    fn last_oplog_hlc(db: &Database) -> Result<Hlc> {
        let txn = db.begin_read()?;
        let oplog = txn.open_table(tables::OPLOG)?;
        match oplog.last()? {
            Some((key, _)) => Ok(codec::decode_oplog_key(key.value())?.hlc),
            None => Ok(Hlc::ZERO),
        }
    }

    /// Mint the next stamp for a local write.
    pub(crate) fn next_stamp(&self) -> Stamp {
        let hlc = self.clock.lock().tick(physical_now_ms());
        Stamp::new(hlc, self.node_id)
    }

    /// Fold a stamp observed from a peer into the local clock.
    pub(crate) fn witness(&self, stamp: &Stamp) {
        self.clock.lock().witness(stamp.hlc);
    }

    pub(crate) fn db(&self) -> &Database {
        &self.db
    }

    /// Begin a counted write transaction.
    ///
    /// Every write an open engine performs goes through here rather than
    /// through [`Database::begin_write`] directly, so that [`Engine::commits`]
    /// counts the engine's durable commits rather than the paths somebody
    /// remembered to instrument — `commits_are_counted_at_one_chokepoint`
    /// fails if a new one appears. The three commits that legitimately do not
    /// pass through here all happen where there is no `Engine` yet to count
    /// them: opening the database, migrating it, and restoring a backup into a
    /// fresh file.
    pub(crate) fn begin_write(&self) -> std::result::Result<WriteTxn<'_>, redb::TransactionError> {
        Ok(WriteTxn { txn: self.db.begin_write()?, commits: &self.commits })
    }

    /// Publish committed events to live subscribers.
    ///
    /// Called only *after* a successful commit. Publishing before the commit
    /// would let a subscriber observe a change that then rolled back.
    pub(crate) fn publish(&self, entries: Vec<OplogEntry>) {
        for entry in entries {
            // An error here just means nobody is listening.
            let _ = self.events.send(Arc::new(entry));
        }
    }

    // -----------------------------------------------------------------------
    // Databases
    // -----------------------------------------------------------------------

    pub fn create_database(&self, name: &str) -> Result<DatabaseMeta> {
        CoreError::validate_name(name)?;
        let stamp = self.next_stamp();
        let meta = DatabaseMeta { name: name.to_string(), created: stamp.hlc };

        let txn = self.begin_write()?;
        {
            let mut dbs = txn.open_table(tables::DATABASES)?;
            if dbs.get(name)?.is_some() {
                // Creating an existing database is a no-op, matching Mongo's
                // implicit-creation feel rather than erroring.
                let existing = dbs.get(name)?.expect("checked above");
                let parsed: DatabaseMeta = serde_json::from_slice(existing.value())?;
                drop(existing);
                drop(dbs);
                txn.abort()?;
                return Ok(parsed);
            }
            dbs.insert(name, serde_json::to_vec(&meta)?.as_slice())?;
        }
        txn.commit()?;
        Ok(meta)
    }

    pub fn list_databases(&self) -> Result<Vec<DatabaseMeta>> {
        let txn = self.db.begin_read()?;
        let dbs = txn.open_table(tables::DATABASES)?;
        let mut out = Vec::new();
        for entry in dbs.iter()? {
            let (_, value) = entry?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    pub fn database_exists(&self, name: &str) -> Result<bool> {
        let txn = self.db.begin_read()?;
        let dbs = txn.open_table(tables::DATABASES)?;
        Ok(dbs.get(name)?.is_some())
    }

    /// Drop a database and every collection in it.
    pub fn drop_database(&self, name: &str) -> Result<bool> {
        let collections = self.list_collections(name)?;
        for collection in &collections {
            self.drop_collection(name, &collection.name)?;
        }

        let txn = self.begin_write()?;
        let existed = {
            let mut dbs = txn.open_table(tables::DATABASES)?;
            dbs.remove(name)?.is_some()
        };
        txn.commit()?;
        Ok(existed)
    }

    // -----------------------------------------------------------------------
    // Collections
    // -----------------------------------------------------------------------

    pub fn create_collection(&self, db: &str, name: &str) -> Result<CollectionMeta> {
        CoreError::validate_name(db)?;
        CoreError::validate_name(name)?;
        self.create_collection_unchecked(db, name)
    }

    /// Create a collection whose name would fail user-facing validation.
    ///
    /// The `__` prefix is reserved precisely so that users cannot create
    /// collections that collide with internal ones — which means the internal
    /// ones have to be created through a path that skips that check.
    pub fn create_system_collection(&self, db: &str, name: &str) -> Result<CollectionMeta> {
        match self.get_collection(db, name) {
            Ok(existing) => Ok(existing),
            Err(StorageError::Core(CoreError::CollectionNotFound { .. })) => {
                self.create_collection_unchecked(db, name)
            }
            Err(e) => Err(e),
        }
    }

    fn create_collection_unchecked(&self, db: &str, name: &str) -> Result<CollectionMeta> {
        self.create_collection_inner(db, name, true)
    }

    /// `log = false` when applying a replicated creation. See
    /// `create_index_inner` for why a replicated change must not mint an entry.
    pub(crate) fn create_collection_inner(
        &self,
        db: &str,
        name: &str,
        log: bool,
    ) -> Result<CollectionMeta> {
        let stamp = self.next_stamp();
        let txn = self.begin_write()?;

        let meta = {
            let mut collections = txn.open_table(tables::COLLECTIONS)?;
            if collections.get((db, name))?.is_some() {
                drop(collections);
                txn.abort()?;
                return Err(CoreError::CollectionExists {
                    db: db.to_string(),
                    collection: name.to_string(),
                }
                .into());
            }

            // Derived, not allocated: every node computes the same id for the
            // same collection, so a replicated oplog entry addresses the same
            // collection everywhere. See `CollectionId::derive`.
            let id = CollectionId::derive(db, name);

            // The derivation is a 64-bit hash, so a collision is possible in
            // principle. Checked rather than trusted, because the failure would
            // be two unrelated collections quietly sharing storage — refusing
            // to create the second one is recoverable, merging them is not.
            let mut collision = None;
            for existing in collections.iter()? {
                let (key, value) = existing?;
                let other: CollectionMeta = serde_json::from_slice(value.value())?;
                if other.id == id {
                    let (other_db, other_name) = key.value();
                    collision = Some(format!("{other_db}.{other_name}"));
                    break;
                }
            }
            if let Some(other) = collision {
                drop(collections);
                txn.abort()?;
                return Err(StorageError::Corrupt(format!(
                    "collection id for {db}.{name} collides with {other}; rename one of them"
                )));
            }

            let meta = CollectionMeta::new(id, db, name, stamp.hlc);
            collections.insert((db, name), serde_json::to_vec(&meta)?.as_slice())?;
            meta
        };

        // Databases are created implicitly by their first collection.
        {
            let mut dbs = txn.open_table(tables::DATABASES)?;
            if dbs.get(db)?.is_none() {
                let db_meta = DatabaseMeta { name: db.to_string(), created: stamp.hlc };
                dbs.insert(db, serde_json::to_vec(&db_meta)?.as_slice())?;
            }
        }

        let logged = if log {
            let entry = ddl_entry(
                stamp,
                OpKind::CreateCollection,
                meta.id,
                &kimmy_core::CollectionRef::new(db, name),
            )?;
            append_oplog(&txn, &entry)?;
            Some(entry)
        } else {
            None
        };

        txn.commit()?;
        if let Some(entry) = logged {
            self.publish(vec![entry]);
        }

        info!(db, collection = name, id = %meta.id, "created collection");
        Ok(meta)
    }

    pub fn get_collection(&self, db: &str, name: &str) -> Result<CollectionMeta> {
        let txn = self.db.begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        match collections.get((db, name))? {
            Some(v) => Ok(serde_json::from_slice(v.value())?),
            None => Err(CoreError::CollectionNotFound {
                db: db.to_string(),
                collection: name.to_string(),
            }
            .into()),
        }
    }

    pub fn list_collections(&self, db: &str) -> Result<Vec<CollectionMeta>> {
        let txn = self.db.begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;
        let mut out = Vec::new();
        // The database name leads the key, so one collection's entries form a
        // contiguous range.
        for entry in collections.range((db, "")..=(db, "\u{10FFFF}"))? {
            let (_, value) = entry?;
            out.push(serde_json::from_slice(value.value())?);
        }
        Ok(out)
    }

    /// Drop a collection along with all its documents and index entries.
    pub fn drop_collection(&self, db: &str, name: &str) -> Result<bool> {
        self.drop_collection_inner(db, name, None)
    }

    /// `replicated` carries the originating stamp when applying a peer's drop.
    ///
    /// It decides two things at once, and they have to agree: whether to log an
    /// entry of our own (a replicated change must not — see
    /// `create_index_inner`), and **which stamp the tombstone records**. Using a
    /// fresh local stamp for a replicated drop would put the tombstone ahead of
    /// a recreation that legitimately followed it, making the name permanently
    /// unusable on that node.
    pub(crate) fn drop_collection_inner(
        &self,
        db: &str,
        name: &str,
        replicated: Option<Stamp>,
    ) -> Result<bool> {
        let meta = match self.get_collection(db, name) {
            Ok(m) => m,
            Err(StorageError::Core(CoreError::CollectionNotFound { .. })) => return Ok(false),
            Err(e) => return Err(e),
        };

        let stamp = replicated.unwrap_or_else(|| self.next_stamp());
        let log = replicated.is_none();
        let txn = self.begin_write()?;
        {
            let mut collections = txn.open_table(tables::COLLECTIONS)?;
            collections.remove((db, name))?;
        }
        {
            // Range-retain rather than collecting keys: a large collection
            // should not have to fit its key set in memory to be dropped.
            let mut docs = txn.open_table(tables::DOCS)?;
            docs.retain_in(doc_range(meta.id), |_, _| false)?;
        }
        {
            let mut indexes = txn.open_table(tables::INDEX_ENTRIES)?;
            indexes.retain_in(index_range(meta.id), |_, _| false)?;
        }
        {
            // Same transaction as the removal, so there is no instant in which
            // the collection is gone with no record that it was dropped.
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            let newer = match dropped.get(meta.id.0)? {
                Some(existing) => stamp > codec::decode_oplog_key(existing.value())?,
                None => true,
            };
            if newer {
                dropped.insert(meta.id.0, codec::oplog_key(&stamp).as_slice())?;
            }
        }

        let logged = if log {
            let entry = ddl_entry(
                stamp,
                OpKind::DropCollection,
                meta.id,
                &kimmy_core::CollectionRef::new(db, name),
            )?;
            append_oplog(&txn, &entry)?;
            Some(entry)
        } else {
            None
        };

        txn.commit()?;
        if let Some(entry) = logged {
            self.publish(vec![entry]);
        }

        info!(db, collection = name, "dropped collection");
        Ok(true)
    }

    /// Persist a modified collection definition (used when adding an index).
    pub(crate) fn put_collection_meta(
        txn: &redb::WriteTransaction,
        meta: &CollectionMeta,
    ) -> Result<()> {
        let mut collections = txn.open_table(tables::COLLECTIONS)?;
        collections
            .insert((meta.db.as_str(), meta.name.as_str()), serde_json::to_vec(meta)?.as_slice())?;
        Ok(())
    }
}

/// Build a DDL oplog entry with a BSON-encoded payload.
///
/// Every schema change names its target by db and collection *name*, not only
/// by the entry's collection id: ids are derived from names by a hash, and a
/// hash cannot be inverted, so a peer meeting a collection for the first time
/// could not otherwise learn what to call it.
pub(crate) fn ddl_entry<T: serde::Serialize>(
    stamp: Stamp,
    kind: OpKind,
    collection: CollectionId,
    payload: &T,
) -> Result<OplogEntry> {
    Ok(OplogEntry {
        stamp,
        kind,
        collection,
        doc_id: None,
        body: Some(bson::serialize_to_vec(payload)?),
    })
}

fn decode_hlc(bytes: &[u8]) -> Result<Hlc> {
    let fixed: [u8; kimmy_core::HLC_ENCODED_LEN] = bytes
        .try_into()
        .map_err(|_| StorageError::Corrupt("version vector entry is not an Hlc".into()))?;
    Ok(Hlc::from_bytes(fixed))
}

fn decode_node(bytes: &[u8]) -> Result<NodeId> {
    let fixed: [u8; 16] = bytes
        .try_into()
        .map_err(|_| StorageError::Corrupt("version vector key is not a node id".into()))?;
    Ok(NodeId::from_bytes(fixed))
}

/// Append one oplog entry inside an existing transaction.
///
/// Always called in the same transaction as the change it describes, so the log
/// and the data can never disagree — there is no window in which a document is
/// updated but unlogged, or logged but not applied.
/// Raise one origin's entry in a version table, never lowering it.
///
/// Shared by both vectors, so they cannot drift in how they compare — and so a
/// vector can only ever move forward, which is the invariant that keeps a
/// rebuild from granting coverage the oplog never held.
fn raise_version(
    txn: &redb::WriteTransaction,
    table: redb::TableDefinition<&'static [u8], &'static [u8]>,
    stamp: &Stamp,
) -> Result<()> {
    let mut versions = txn.open_table(table)?;
    let node = stamp.node.to_bytes();
    let higher = match versions.get(node.as_slice())? {
        Some(current) => stamp.hlc > decode_hlc(current.value())?,
        None => true,
    };
    if higher {
        versions.insert(node.as_slice(), stamp.hlc.to_bytes().as_slice())?;
    }
    Ok(())
}

pub(crate) fn append_oplog(txn: &redb::WriteTransaction, entry: &OplogEntry) -> Result<()> {
    let key = codec::oplog_key(&entry.stamp);
    let mut oplog = txn.open_table(tables::OPLOG)?;
    let existed =
        oplog.insert(key.as_slice(), codec::encode_oplog_entry(entry).as_slice())?.is_some();

    // Re-appending an entry we already hold must not give it a second arrival
    // position. Peers resend overlapping ranges routinely, and a duplicate
    // arrival entry would deliver the same change twice to every stream.
    if existed {
        return Ok(());
    }

    // Same transaction as the entry, so the vector can never claim coverage of
    // something that was rolled back — a peer would then never be sent it.
    //
    // Both vectors: appending is also the strongest form of having seen it, so
    // witnessed stays at or above servable by construction (ADR-054).
    raise_version(txn, tables::OPLOG_VERSIONS, &entry.stamp)?;
    raise_version(txn, tables::OPLOG_WITNESSED, &entry.stamp)?;

    let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
    let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;

    // The counter lives in the index rather than in `meta` so that it cannot
    // drift from the thing it counts: rebuilding the index also rebuilds the
    // counter, and there is no third place for them to disagree.
    let next = arrival.last()?.map_or(0, |(seq, _)| seq.value() + 1);
    arrival.insert(next, key.as_slice())?;
    by_stamp.insert(key.as_slice(), next)?;
    Ok(())
}

/// Key range covering every document in a collection.
/// [`doc_range`], starting strictly after `after` when one is given.
///
/// Exclusive on purpose: a cursor names the last row already delivered, so
/// including it would hand the caller a duplicate at every page boundary.
pub(crate) fn doc_range_after(
    id: CollectionId,
    after: Option<&[u8]>,
) -> impl std::ops::RangeBounds<(u64, &[u8])> {
    use std::ops::Bound;
    let start = match after {
        Some(key) => Bound::Excluded((id.0, key)),
        None => Bound::Included((id.0, [].as_slice())),
    };
    let end = match id.0.checked_add(1) {
        Some(next) => Bound::Excluded((next, [].as_slice())),
        None => Bound::Unbounded,
    };
    (start, end)
}

pub(crate) fn doc_range(id: CollectionId) -> impl std::ops::RangeBounds<(u64, &'static [u8])> {
    use std::ops::Bound;
    let start = Bound::Included((id.0, [].as_slice()));
    let end = match id.0.checked_add(1) {
        Some(next) => Bound::Excluded((next, [].as_slice())),
        None => Bound::Unbounded,
    };
    (start, end)
}

/// Key range covering every index entry in a collection.
pub(crate) fn index_range(
    id: CollectionId,
) -> impl std::ops::RangeBounds<(u64, u32, &'static [u8], &'static [u8])> {
    use std::ops::Bound;
    let start = Bound::Included((id.0, 0u32, [].as_slice(), [].as_slice()));
    let end = match id.0.checked_add(1) {
        Some(next) => Bound::Excluded((next, 0u32, [].as_slice(), [].as_slice())),
        None => Bound::Unbounded,
    };
    (start, end)
}

/// Milliseconds since the Unix epoch.
///
/// The only place the storage layer reads the wall clock. `kimmy-core` takes
/// physical time as a parameter precisely so that this stays isolated and the
/// clock logic remains deterministically testable.
/// Milliseconds since the Unix epoch.
///
/// Public so that tests elsewhere in the workspace can express "far in the
/// future" against the same clock retention uses.
pub fn physical_now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        (engine, dir)
    }

    /// A counter is only worth reading if nothing can write behind its back.
    ///
    /// `Engine::commits` is a claim about the whole engine, and the way that
    /// claim goes quietly false is somebody reaching for `self.db()` in a new
    /// write path — which compiles, works, and undercounts. So the invariant is
    /// checked against the source rather than trusted: every `begin_write` in
    /// this crate is either `Engine::begin_write` or one of the three places
    /// that legitimately has no engine to count against.
    #[test]
    fn commits_are_counted_at_one_chokepoint() {
        // Where a commit happens before or outside an open `Engine`, and so
        // cannot be counted by one. Each is a whole-file exemption because each
        // file's entire job is one of these.
        const NO_ENGINE_YET: [&str; 3] = [
            "migrate.rs", // runs on the raw database during `Engine::open`
            "backup.rs",  // restores into a fresh file that no engine has opened
            "engine.rs",  // `Engine::open` itself, plus the chokepoint
        ];

        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders = Vec::new();

        for entry in std::fs::read_dir(&src).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if path.extension().is_none_or(|e| e != "rs") || NO_ENGINE_YET.contains(&name.as_str())
            {
                continue;
            }

            let body = std::fs::read_to_string(&path).unwrap();
            for (n, line) in body.lines().enumerate() {
                // Tests reach for a raw database on purpose — to prove what an
                // engine does when the file underneath it was written by
                // something else.
                let raw = line.contains(".begin_write()") && !line.contains("self.begin_write()");
                if raw && !line.trim_start().starts_with("let txn = db.begin_write().unwrap()") {
                    offenders.push(format!("{name}:{}: {}", n + 1, line.trim()));
                }
            }
        }

        assert!(
            offenders.is_empty(),
            "these write transactions bypass `Engine::begin_write`, so `Engine::commits` \
             undercounts them and every conclusion drawn from it is wrong:\n  {}",
            offenders.join("\n  ")
        );
    }

    #[test]
    fn node_identity_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let first = Engine::open(&path).unwrap().node_id();
        let second = Engine::open(&path).unwrap().node_id();
        assert_eq!(first, second, "identity must live with the data");
    }

    #[test]
    fn stamps_are_strictly_increasing() {
        let (engine, _dir) = engine();
        let mut previous = Stamp::new(Hlc::ZERO, engine.node_id());
        for _ in 0..1000 {
            let next = engine.next_stamp();
            assert!(next > previous, "{next:?} did not exceed {previous:?}");
            previous = next;
        }
    }

    #[test]
    fn the_clock_resumes_above_the_oplog_tail_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let last = {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("app", "orders").unwrap();
            engine.clock.lock().last()
        };

        // A restart must not mint stamps below what is already on disk, or a
        // document rewritten after the restart could lose to its own older
        // version under last-writer-wins.
        let reopened = Engine::open(&path).unwrap();
        assert!(reopened.next_stamp().hlc > last);
    }

    #[test]
    fn creating_a_collection_creates_its_database() {
        let (engine, _dir) = engine();
        engine.create_collection("app", "orders").unwrap();
        assert!(engine.database_exists("app").unwrap());
        assert_eq!(engine.list_databases().unwrap().len(), 1);
    }

    #[test]
    fn duplicate_collections_are_rejected() {
        let (engine, _dir) = engine();
        engine.create_collection("app", "orders").unwrap();
        assert!(matches!(
            engine.create_collection("app", "orders"),
            Err(StorageError::Core(CoreError::CollectionExists { .. }))
        ));
    }

    #[test]
    fn distinct_collections_get_distinct_ids() {
        let (engine, _dir) = engine();
        let a = engine.create_collection("app", "a").unwrap().id;
        let b = engine.create_collection("app", "b").unwrap().id;
        assert_ne!(a, b);
    }

    #[test]
    fn recreating_a_collection_reuses_its_id_but_not_its_data() {
        // Ids are derived from the name, so a recreated collection necessarily
        // gets the same id — "same name means same id on every node" and
        // "recreating yields a fresh id" cannot both hold.
        //
        // That makes purging on drop load-bearing rather than merely tidy: a
        // surviving document or index entry would be inherited by the new
        // collection.
        let (engine, _dir) = engine();
        let coll = engine.create_collection("app", "a").unwrap();
        engine.insert(&coll, bson::doc! { "_id": 1, "v": "old" }).unwrap();

        engine.drop_collection("app", "a").unwrap();
        let recreated = engine.create_collection("app", "a").unwrap();

        assert_eq!(recreated.id, coll.id, "a derived id is stable across drop and recreate");
        assert_eq!(engine.count(&recreated).unwrap(), 0, "the dropped data must not be inherited");
        assert!(engine.get(&recreated, &kimmy_core::DocId::Int64(1)).unwrap().is_none());
    }

    #[test]
    fn two_nodes_agree_on_a_collection_id_whatever_the_creation_order() {
        // The reason ids are derived at all. A counter makes this depend on
        // creation order, so a replicated write would land in whichever
        // collection happened to hold that number locally.
        let a_dir = tempfile::tempdir().unwrap();
        let b_dir = tempfile::tempdir().unwrap();
        let a = Engine::open(&a_dir.path().join("kimmy.redb")).unwrap();
        let b = Engine::open(&b_dir.path().join("kimmy.redb")).unwrap();

        let a_orders = a.create_collection("shop", "orders").unwrap().id;
        a.create_collection("shop", "customers").unwrap();

        // Deliberately the opposite order on the second node.
        b.create_collection("shop", "customers").unwrap();
        let b_orders = b.create_collection("shop", "orders").unwrap().id;

        assert_eq!(a_orders, b_orders);
    }

    #[test]
    fn collection_ids_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let first = Engine::open(&path).unwrap().create_collection("app", "a").unwrap().id;
        let second = Engine::open(&path).unwrap().create_collection("app", "b").unwrap().id;
        assert_ne!(first, second, "the id counter must be persistent");
    }

    #[test]
    fn listing_collections_is_scoped_to_one_database() {
        let (engine, _dir) = engine();
        engine.create_collection("app", "orders").unwrap();
        engine.create_collection("app", "users").unwrap();
        engine.create_collection("other", "orders").unwrap();

        let mut names: Vec<_> =
            engine.list_collections("app").unwrap().into_iter().map(|c| c.name).collect();
        names.sort();
        assert_eq!(names, ["orders", "users"]);
        assert_eq!(engine.list_collections("other").unwrap().len(), 1);
        assert!(engine.list_collections("missing").unwrap().is_empty());
    }

    #[test]
    fn getting_a_missing_collection_is_an_error() {
        let (engine, _dir) = engine();
        assert!(matches!(
            engine.get_collection("app", "nope"),
            Err(StorageError::Core(CoreError::CollectionNotFound { .. }))
        ));
    }

    #[test]
    fn dropping_a_missing_collection_reports_false_rather_than_erroring() {
        let (engine, _dir) = engine();
        assert!(!engine.drop_collection("app", "nope").unwrap());
    }

    #[test]
    fn dropping_a_database_removes_its_collections() {
        let (engine, _dir) = engine();
        engine.create_collection("app", "a").unwrap();
        engine.create_collection("app", "b").unwrap();
        engine.create_collection("keep", "c").unwrap();

        assert!(engine.drop_database("app").unwrap());
        assert!(engine.list_collections("app").unwrap().is_empty());
        assert!(!engine.database_exists("app").unwrap());
        // An unrelated database must be untouched.
        assert_eq!(engine.list_collections("keep").unwrap().len(), 1);
    }

    #[test]
    fn invalid_names_are_rejected() {
        let (engine, _dir) = engine();
        assert!(engine.create_collection("app", "__system").is_err());
        assert!(engine.create_collection("app", "with/slash").is_err());
        assert!(engine.create_collection("", "x").is_err());
    }

    #[test]
    fn collection_operations_are_logged_to_the_oplog() {
        let (engine, _dir) = engine();
        let mut rx = engine.subscribe();
        let meta = engine.create_collection("app", "orders").unwrap();

        let event = rx.try_recv().expect("a create should publish an event");
        assert_eq!(event.kind, OpKind::CreateCollection);
        assert_eq!(event.collection, meta.id);
    }

    #[test]
    fn a_mismatched_format_version_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let _ = Engine::open(&path).unwrap();
        }

        // Simulate a data directory written by an incompatible build.
        {
            let db = Database::create(&path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut meta = txn.open_table(tables::META).unwrap();
                meta.insert(tables::META_FORMAT_VERSION, [99u8].as_slice()).unwrap();
            }
            txn.commit().unwrap();
        }

        assert!(
            matches!(Engine::open(&path), Err(StorageError::UnsupportedFormat { found: 99, .. })),
            "opening must refuse rather than misread the records"
        );
    }
}
