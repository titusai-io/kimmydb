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
        }
        txn.commit()?;

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
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// How many times this collection's vectors have changed.
    ///
    /// Resets to zero on restart, which is correct: an in-memory index does
    /// not survive one either.
    pub fn vector_generation(&self, collection: CollectionId) -> u64 {
        self.vector_generations.lock().get(&collection).copied().unwrap_or(0)
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
            let stored_version =
                meta.get(tables::META_FORMAT_VERSION)?.map(|v| v.value().first().copied());
            let stored_node =
                meta.get(tables::META_NODE_ID)?.map(|v| <[u8; 16]>::try_from(v.value()));

            // Refuse to open a data directory written by an incompatible build
            // rather than misinterpreting its records.
            match stored_version {
                Some(found) => {
                    let found = found.unwrap_or(0);
                    if found != codec::FORMAT_VERSION {
                        return Err(StorageError::UnsupportedFormat {
                            found,
                            expected: codec::FORMAT_VERSION,
                        });
                    }
                }
                None => {
                    meta.insert(tables::META_FORMAT_VERSION, [codec::FORMAT_VERSION].as_slice())?;
                }
            }

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

        let txn = self.db.begin_write()?;
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

        let txn = self.db.begin_write()?;
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
        let stamp = self.next_stamp();
        let txn = self.db.begin_write()?;

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

            // Allocate the id inside the same transaction as the insert, so a
            // crash between the two cannot hand the same id to two collections.
            let id = {
                let mut meta_table = txn.open_table(tables::META)?;
                let next = match meta_table.get(tables::META_NEXT_COLLECTION_ID)? {
                    Some(v) => u64::from_be_bytes(
                        v.value()
                            .try_into()
                            .map_err(|_| StorageError::Corrupt("collection counter".into()))?,
                    ),
                    None => 1,
                };
                meta_table
                    .insert(tables::META_NEXT_COLLECTION_ID, (next + 1).to_be_bytes().as_slice())?;
                CollectionId(next)
            };

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

        let entry = OplogEntry {
            stamp,
            kind: OpKind::Collection,
            collection: meta.id,
            doc_id: None,
            body: None,
        };
        append_oplog(&txn, &entry)?;

        txn.commit()?;
        self.publish(vec![entry]);

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
        let meta = match self.get_collection(db, name) {
            Ok(m) => m,
            Err(StorageError::Core(CoreError::CollectionNotFound { .. })) => return Ok(false),
            Err(e) => return Err(e),
        };

        let stamp = self.next_stamp();
        let txn = self.db.begin_write()?;
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

        let entry = OplogEntry {
            stamp,
            kind: OpKind::Collection,
            collection: meta.id,
            doc_id: None,
            body: None,
        };
        append_oplog(&txn, &entry)?;

        txn.commit()?;
        self.publish(vec![entry]);

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

/// Append one oplog entry inside an existing transaction.
///
/// Always called in the same transaction as the change it describes, so the log
/// and the data can never disagree — there is no window in which a document is
/// updated but unlogged, or logged but not applied.
pub(crate) fn append_oplog(txn: &redb::WriteTransaction, entry: &OplogEntry) -> Result<()> {
    let mut oplog = txn.open_table(tables::OPLOG)?;
    oplog.insert(
        codec::oplog_key(&entry.stamp).as_slice(),
        codec::encode_oplog_entry(entry).as_slice(),
    )?;
    Ok(())
}

/// Key range covering every document in a collection.
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
pub(crate) fn physical_now_ms() -> u64 {
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
    fn collection_ids_are_unique_and_never_reused() {
        let (engine, _dir) = engine();
        let a = engine.create_collection("app", "a").unwrap().id;
        let b = engine.create_collection("app", "b").unwrap().id;
        assert_ne!(a, b);

        // Dropping and recreating must not hand back the old id, or stale
        // index entries would be attributed to the new collection.
        engine.drop_collection("app", "a").unwrap();
        let c = engine.create_collection("app", "a").unwrap().id;
        assert_ne!(c, a);
        assert_ne!(c, b);
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
        assert_eq!(event.kind, OpKind::Collection);
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
