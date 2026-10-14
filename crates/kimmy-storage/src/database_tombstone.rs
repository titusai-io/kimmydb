//! A database drop is a tombstone (ADR-229).
//!
//! `DELETE /v1/db/{db}` used to drop the collections the member held when it
//! was applied and leave only their tombstones. A collection the dropper had
//! not received, created on a peer shortly before, had none, so its
//! `CreateCollection` entry applied on the dropper and brought the database
//! back, and the cluster converged on a database someone had deleted.
//!
//! The drop now leaves a tombstone of its own, filed in `COLLECTIONS_DROPPED`
//! under the id of a reserved collection name in the database
//! ([`kimmy_core::DATABASE_DROP_NAME`]), with the stamp of the drop, and
//! replicates as an ordinary `DropCollection` entry that names it. Nothing
//! about it is new to a build that predates the rule: it decodes, applies,
//! stores and serves the entry as the drop of a collection it never held.
//!
//! One rule, one comparison, on every route: with a database tombstone at `S`,
//! a collection whose creation was stamped at or before `S.hlc` is buried, and
//! one that is not stands and is given a tombstone of its own at `S`, so that a
//! straggler of the life before it is refused the same way on every member
//! whether or not that member refused the creation itself.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use kimmy_core::{
    CollectionId, CollectionRef, DATABASE_DROP_NAME, Hlc, OpKind, Stamp, database_drop_id,
    vector_meta,
};
use redb::{ReadableDatabase, ReadableTable};
use tracing::{debug, info, warn};

use crate::codec;
use crate::engine::{Continuing, Engine, WriterHolder, append_oplog, ddl_entry};
use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;
use crate::tables;

/// How many of a buried collection's documents are read to say how many of them
/// were written after the drop.
const WARN_SAMPLE: usize = 1_000;

/// A collection buried without an entry of its own, and every id that went
/// with it (its vector shadow's included).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuriedCollection {
    pub db: String,
    pub name: String,
    pub ids: Vec<CollectionId>,
}

/// One notice on [`Engine::subscribe_buried`]: everything one burial, or one
/// sweep, took. One per sweep rather than one per collection, because a sweep
/// of thousands of collections would otherwise burst past the buffer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Buried {
    pub collections: Vec<BuriedCollection>,
}

impl Buried {
    /// Whether `id` is among the ids this notice buried.
    pub fn names(&self, id: CollectionId) -> bool {
        self.collections.iter().any(|c| c.ids.contains(&id))
    }
}

/// What reading a collection's first documents found.
#[derive(Debug, Default)]
pub(crate) struct WrittenAfter {
    /// Documents read.
    pub(crate) checked: usize,
    /// Of those, the live ones stamped after the drop.
    pub(crate) after: usize,
}

/// What a sweep did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Swept {
    /// The collections it buried.
    pub buried: Vec<BuriedCollection>,
    /// The collections it left standing, each given a tombstone of its own.
    pub spared: usize,
}

/// Where a burial that minted no entry announces itself.
pub(crate) enum Notify<'a> {
    /// At once, as a notice of its own.
    Each,
    /// Into a sweep's list, sent as one notice when the sweep is done.
    Collect(&'a mut Vec<BuriedCollection>),
}

impl Notify<'_> {
    pub(crate) fn buried(self, engine: &Engine, db: &str, name: &str, ids: &[CollectionId]) {
        let item =
            BuriedCollection { db: db.to_string(), name: name.to_string(), ids: ids.to_vec() };
        match self {
            Self::Each => engine.send_buried(vec![item]),
            Self::Collect(into) => into.push(item),
        }
    }
}

impl Engine {
    /// When `db`'s drop was recorded, if a tombstone still records it.
    pub fn database_dropped_at(&self, db: &str) -> Result<Option<Stamp>> {
        self.collection_dropped_at(database_drop_id(db))
    }

    /// The stamp of the drop that makes a creation history, if there is one: a
    /// creation whose origin stamped it at `created` is refused iff
    /// `created <= S.hlc`. **One comparison, an `Hlc` against an `Hlc`, on every
    /// route** (the entries path, a snapshot page, the sweep), so the order in
    /// which a creation and the drop arrive cannot change the outcome. A
    /// creation naming no `created` (a page from a sender before the field)
    /// reads as older.
    pub(crate) fn database_refusal(&self, db: &str, created: Option<Hlc>) -> Result<Option<Stamp>> {
        Ok(self
            .database_dropped_at(db)?
            .filter(|dropped| created.is_none_or(|created| created <= dropped.hlc)))
    }

    /// Record, for a creation the database's drop refused, the tombstone of the
    /// collection and of its vector shadow, at the drop's stamp.
    ///
    /// This is how the database tombstone **expands lazily** into the
    /// tombstones every id-only path already understands. A document, an index
    /// or a vector configuration for the collection names it by id or by a name
    /// the receiver cannot resolve; with no collection and no tombstone it
    /// stops the batch as unknown until a snapshot, and a snapshot's coverage
    /// hides the creation that would have recorded it. Both ids, as a burial
    /// records both: a shadow's chunks are judged by the shadow's id.
    pub(crate) fn record_database_refusal(
        &self,
        db: &str,
        name: &str,
        created: Option<Hlc>,
    ) -> Result<()> {
        let Some(dropped) = self.database_refusal(db, created)? else {
            return Ok(());
        };
        self.record_tombstones_at(db, name, dropped)
    }

    /// Tombstones for `db.name` and its shadow at `at`, where none that new is
    /// held. Read first: a page that is already held must cost no writer.
    fn record_tombstones_at(&self, db: &str, name: &str, at: Stamp) -> Result<()> {
        let mut ids = vec![CollectionId::derive(db, name)];
        if !vector_meta::is_shadow(name) {
            ids.push(CollectionId::derive(db, &vector_meta::shadow_name(name)));
        }
        self.record_ids_at(&ids, at)
    }

    /// Tombstones for `ids` at `at`, in one commit, for those that hold none that
    /// new. Read first: nothing newer to write takes no writer.
    pub(crate) fn record_ids_at(&self, ids: &[CollectionId], at: Stamp) -> Result<()> {
        let mut batch = Vec::new();
        for id in ids {
            if self.collection_dropped_at(*id)?.is_none_or(|held| held < at) {
                batch.push((*id, at));
            }
        }
        self.record_collection_drops(&batch)
    }

    /// Announce collections buried without an entry. Nobody listening is not an
    /// error.
    pub(crate) fn send_buried(&self, collections: Vec<BuriedCollection>) {
        if !collections.is_empty() {
            let _ = self.buried.send(Arc::new(Buried { collections }));
        }
    }

    /// [`Self::send_buried`], for a test of a consumer that lags behind it.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn send_buried_for_test(&self, collections: Vec<BuriedCollection>) {
        self.send_buried(collections);
    }

    /// The first commit of a database drop: the tombstone and the entry that
    /// carries it (ADR-229). Minted under the writer (ADR-148), so its stamp
    /// sorts with the commit order and every entry this node serves after it
    /// is contiguous for its origin.
    pub(crate) fn record_database_drop(&self, db: &str) -> Result<Stamp> {
        let txn = self.begin_write(WriterHolder::Ddl)?;
        let stamp = self.next_stamp();
        let id = database_drop_id(db);
        {
            let mut dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            let newer = match dropped.get(id.0)? {
                Some(held) => stamp > codec::decode_oplog_key(held.value())?,
                None => true,
            };
            if newer {
                dropped.insert(id.0, codec::oplog_key(&stamp).as_slice())?;
            }
        }
        let entry = ddl_entry(
            stamp,
            OpKind::DropCollection,
            id,
            &CollectionRef::new(db, DATABASE_DROP_NAME),
        )?;
        append_oplog(&txn, &entry)?;
        txn.commit()?;
        self.publish(vec![entry]);
        info!(db, "recorded a database drop");
        Ok(stamp)
    }

    /// Bury every collection a database tombstone covers and mark every one it
    /// does not, in every database this member holds a tombstone for.
    ///
    /// For each held collection of such a database, with the drop at `S`:
    ///
    /// - `created <= S.hlc`: **buried**, through the replicated path (the
    ///   drop's stamp, no new entry, chunked and handed to the purger as every
    ///   burial is);
    /// - otherwise: **spared, and given its own tombstone at `S`**, with its
    ///   shadow's. Floors are not carried between members, so without this a
    ///   survivor accepted a straggler of the life before it that another
    ///   member, which had refused the creation and so held the floor, did not.
    ///
    /// Idempotent, and cheap when nothing has changed: a database swept at the
    /// same tombstone and the same collections generation is not walked again.
    /// The memo is written only after a sweep succeeds.
    ///
    /// Ended by the node's stop between two burials, and before each walk of a
    /// database's collections (`Err(Stopping)`): inside an apply that leaves the
    /// entry unappended, so the next start redoes it, and every burial that did
    /// land stands. Without that a stop waited on one commit per collection of
    /// the database, up to the drain's deadline.
    pub fn bury_under_database_tombstones(&self) -> Result<Swept> {
        self.sweep(&|| self.check_walk(crate::WalkScope::Background))
    }

    /// [`Self::bury_under_database_tombstones`] at a start, before the node
    /// serves: ended at a safe point by a stop, since every burial is a commit
    /// of its own and the next start repeats what is left.
    pub fn reconcile_database_tombstones(&self) -> Result<Swept> {
        use crate::open_progress::{OpenPhase, open_stop_requested, set_open_phase};

        // Said on `/readyz` only when there is something to do: a start with no
        // database tombstone passes through without a flicker of a new phase.
        let mut owed = 0u64;
        for database in self.list_databases()? {
            if self.database_dropped_at(&database.name)?.is_some() {
                owed += 1;
            }
        }
        if owed == 0 {
            return Ok(Swept::default());
        }
        let before = crate::open_progress::open_snapshot().phase;
        crate::open_progress::set_open_phase_of(OpenPhase::Reconciling, owed);
        let done = std::cell::Cell::new(0u64);
        let swept = self.sweep(&|| {
            if open_stop_requested() {
                return Err(StorageError::Stopping(crate::StopReason::Shutdown));
            }
            self.check_walk(crate::WalkScope::Background)?;
            crate::open_progress::set_open_done(done.get().min(owed));
            done.set(done.get() + 1);
            Ok(())
        });
        set_open_phase(before);
        let swept = swept?;
        if !swept.buried.is_empty() || swept.spared > 0 {
            info!(
                buried = swept.buried.len(),
                spared = swept.spared,
                "buried what a database's drop covers before serving"
            );
        }
        Ok(swept)
    }

    pub(crate) fn sweep(&self, checkpoint: &dyn Fn() -> Result<()>) -> Result<Swept> {
        let mut swept = Swept::default();
        let mut notice = Vec::new();
        let result = self.sweep_databases(checkpoint, &mut swept, &mut notice);
        // What was buried is announced even when a later burial failed: the
        // streams on it are on collections that are gone.
        swept.buried = notice.clone();
        self.send_buried(notice);
        result.map(|()| swept)
    }

    fn sweep_databases(
        &self,
        checkpoint: &dyn Fn() -> Result<()>,
        swept: &mut Swept,
        notice: &mut Vec<BuriedCollection>,
    ) -> Result<()> {
        let databases = self.list_databases()?;
        // The memo holds a database only while it holds a tombstone: one whose
        // database has gone, or whose tombstone retention has collected, is
        // dropped, so it is bounded by the tombstones held and not by every
        // database name that ever had one.
        {
            let present: std::collections::HashSet<&str> =
                databases.iter().map(|d| d.name.as_str()).collect();
            self.swept.lock().retain(|db, _| present.contains(db.as_str()));
        }
        for database in databases {
            checkpoint()?;
            let db = database.name;
            let Some(dropped) = self.database_dropped_at(&db)? else {
                self.swept.lock().remove(&db);
                continue;
            };
            // Read before the walk: the walk's own commits move it, so the next
            // sweep walks once more, finds nothing to do, and memoises.
            let generation = self.collections_generation.load(Ordering::SeqCst);
            if self.swept.lock().get(&db) == Some(&(dropped, generation)) {
                continue;
            }
            // Non-shadows first, so a parent takes its shadow in its own
            // burial; what is left of the shadows afterwards has no parent to
            // take it (ADR-138), or belongs to a collection that stands.
            for collection in self.list_collections(&db)? {
                if !vector_meta::is_shadow(&collection.name) {
                    checkpoint()?;
                    self.sweep_collection(&db, &collection, dropped, swept, notice, checkpoint)?;
                }
            }
            checkpoint()?;
            for collection in self.list_collections(&db)? {
                if vector_meta::is_shadow(&collection.name) {
                    checkpoint()?;
                    self.sweep_collection(&db, &collection, dropped, swept, notice, checkpoint)?;
                }
            }
            self.swept.lock().insert(db, (dropped, generation));
        }
        Ok(())
    }

    fn sweep_collection(
        &self,
        db: &str,
        collection: &CollectionMeta,
        dropped: Stamp,
        swept: &mut Swept,
        notice: &mut Vec<BuriedCollection>,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        if collection.created <= dropped.hlc {
            let written_after = self.documents_written_after(collection, dropped)?;
            let buried = self.bury_collection_as(
                db,
                &collection.name,
                Some(dropped),
                Continuing::No,
                Notify::Collect(notice),
            )?;
            if buried.is_some() {
                // Only a write stamped after the drop is news: the collection's
                // earlier documents are what the drop is for, and the burial is
                // already logged. A write after it was answered `200` by a member
                // that had not yet received the drop.
                if written_after.after > 0 {
                    warn!(
                        db,
                        collection = %collection.name,
                        written_after_the_drop = written_after.after,
                        checked = written_after.checked,
                        "a database drop buried a collection that holds documents written after \
                         the drop; those writes were acknowledged to clients"
                    );
                } else {
                    debug!(db, collection = %collection.name, "a database drop buried a collection");
                }
            }
        } else {
            self.record_tombstones_at(db, &collection.name, dropped)?;
            // A survivor is a life created after the drop; what it took in of
            // the life before, before it learned of the drop, goes (ADR-231).
            self.sweep_late_drop_until(db, &collection.name, dropped, checkpoint)?;
            swept.spared += 1;
        }
        Ok(())
    }

    /// Record a database's tombstone and nothing else, the way a member on the
    /// previous build records the drop of a collection it never held. For a test
    /// of a start that finds one.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn record_database_tombstone_for_test(&self, id: CollectionId, at: Stamp) -> Result<()> {
        self.record_collection_drop(id, at)
    }

    /// For a collection about to be buried: how many of its first [`WARN_SAMPLE`]
    /// documents were read, and how many of those are live and were written
    /// after the drop.
    ///
    /// Each record is judged by its fixed-width header alone, with no copy of
    /// the body, and nothing is read for an empty collection.
    pub(crate) fn documents_written_after(
        &self,
        collection: &CollectionMeta,
        dropped: Stamp,
    ) -> Result<WrittenAfter> {
        let txn = self.db().begin_read()?;
        if crate::live_count::live_count(&txn, collection.id)? == 0 {
            return Ok(WrittenAfter::default());
        }
        let docs = txn.open_table(tables::DOCS)?;
        let mut found = WrittenAfter::default();
        for row in docs.range(crate::engine::doc_range(collection.id))?.take(WARN_SAMPLE) {
            let (_, value) = row?;
            let (stamp, live) = codec::doc_record_header(value.value())?;
            found.checked += 1;
            if live && stamp.hlc > dropped.hlc {
                found.after += 1;
            }
        }
        Ok(found)
    }
}
