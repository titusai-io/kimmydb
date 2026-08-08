//! Document reads and writes.
//!
//! Every mutation does three things in one redb transaction: write the
//! [`DocRecord`], append the [`OplogEntry`] describing it, and (from M1's index
//! work onward) update index entries. Doing them together is what lets the rest
//! of the system trust the log — there is no window in which a document is
//! changed but unlogged, or logged but not applied.
//!
//! Events reach live subscribers only *after* the commit succeeds, so a
//! subscriber can never observe a change that was rolled back.

use bson::Document;
use kimmy_core::{DocId, DocRecord, Error as CoreError, OpKind, OplogEntry, Stamp, keyenc};
use redb::{ReadableDatabase, ReadableTable};
use tracing::warn;

use crate::codec;
use crate::engine::{Engine, append_oplog, doc_range};
use crate::error::{Result, StorageError};
use crate::index;
use crate::meta::CollectionMeta;
use crate::tables;

/// Outcome of a write, so callers can report counts without a second read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct WriteOutcome {
    pub matched: bool,
    pub modified: bool,
    pub upserted: bool,
}

/// The conventional primary-key field name.
pub const ID_FIELD: &str = "_id";

impl Engine {
    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    pub fn get(&self, coll: &CollectionMeta, id: &DocId) -> Result<Option<Document>> {
        let key = doc_key(id)?;
        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        match docs.get((coll.id.0, key.as_slice()))? {
            Some(raw) => {
                let record = codec::decode_doc_record(raw.value())?;
                // A tombstone is present on disk but absent to a reader.
                Ok(record.document()?)
            }
            None => Ok(None),
        }
    }

    /// Visit every live document in the collection, in `_id` order.
    ///
    /// Callback-based rather than iterator-returning because redb's range
    /// borrows its transaction; this keeps the transaction's lifetime contained
    /// and lets a caller stop early without materializing the whole collection.
    /// Return `false` from `f` to stop.
    pub fn for_each_doc<F>(&self, coll: &CollectionMeta, mut f: F) -> Result<()>
    where
        F: FnMut(DocId, Document) -> Result<bool>,
    {
        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        for entry in docs.range(doc_range(coll.id))? {
            let (_, value) = entry?;
            let record = codec::decode_doc_record(value.value())?;
            if record.deleted {
                continue;
            }
            let doc: Document = bson::deserialize_from_slice(&record.body)?;
            let id = extract_id(&doc)?;
            if !f(id, doc)? {
                break;
            }
        }
        Ok(())
    }

    /// The stamp of a live document, or `None` if it is absent or tombstoned.
    ///
    /// Exposed so that derived data can be tagged with the version of the
    /// document it was derived *from* — client-supplied vectors carry the
    /// source document's HLC, which is what makes staleness a comparison
    /// rather than a state machine. A tombstone reads as absent here for the
    /// same reason it does to any other reader.
    pub fn document_stamp(&self, coll: &CollectionMeta, id: &DocId) -> Result<Option<Stamp>> {
        let key = doc_key(id)?;
        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        match docs.get((coll.id.0, key.as_slice()))? {
            Some(raw) => {
                let record = codec::decode_doc_record(raw.value())?;
                Ok(record.is_live().then_some(record.stamp))
            }
            None => Ok(None),
        }
    }

    pub fn count(&self, coll: &CollectionMeta) -> Result<u64> {
        let mut n = 0;
        self.for_each_doc(coll, |_, _| {
            n += 1;
            Ok(true)
        })?;
        Ok(n)
    }

    // -----------------------------------------------------------------------
    // Writes
    // -----------------------------------------------------------------------

    /// Insert a document, failing if its `_id` already exists.
    ///
    /// Generates an ObjectId when `_id` is absent, and returns the id either
    /// way so the caller need not re-read.
    pub fn insert(&self, coll: &CollectionMeta, mut doc: Document) -> Result<DocId> {
        let id = match doc.get(ID_FIELD) {
            Some(value) => DocId::try_from_bson(value)?,
            None => {
                let id = DocId::generate();
                // Store `_id` first so the stored document round-trips with the
                // field present, as clients expect.
                let mut with_id = Document::new();
                with_id.insert(ID_FIELD, id.to_bson());
                with_id.extend(doc);
                doc = with_id;
                id
            }
        };

        let key = doc_key(&id)?;
        let body = bson::serialize_to_vec(&doc)?;
        let stamp = self.next_stamp();

        let txn = self.db().begin_write()?;
        {
            let mut docs = txn.open_table(tables::DOCS)?;
            // A tombstone may still occupy the key; overwriting it is a
            // legitimate resurrection, but a live document is a conflict.
            let occupied = match docs.get((coll.id.0, key.as_slice()))? {
                Some(raw) => codec::decode_doc_record(raw.value())?.is_live(),
                None => false,
            };
            if occupied {
                drop(docs);
                txn.abort()?;
                return Err(CoreError::DuplicateKey(id.to_string()).into());
            }
            let record = DocRecord::live(stamp, body.clone());
            docs.insert((coll.id.0, key.as_slice()), codec::encode_doc_record(&record).as_slice())?;
        }

        // Same transaction as the document write, so the index cannot describe
        // a state that never existed. A unique violation aborts the whole thing.
        if let Err(e) = index::maintain(&txn, coll.id, &coll.indexes, None, Some(&doc), &key) {
            txn.abort()?;
            return Err(e);
        }

        let entry = OplogEntry {
            stamp,
            kind: OpKind::Insert,
            collection: coll.id,
            doc_id: Some(id.clone()),
            body: Some(body),
        };
        append_oplog(&txn, &entry)?;
        txn.commit()?;
        self.publish(vec![entry]);

        Ok(id)
    }

    /// Replace a document wholesale.
    ///
    /// With `upsert`, a missing document is created instead of reported as
    /// unmatched.
    pub fn replace(
        &self,
        coll: &CollectionMeta,
        id: &DocId,
        mut doc: Document,
        upsert: bool,
    ) -> Result<WriteOutcome> {
        // The id is part of the document's identity, not its content: a replace
        // must not be able to move a document to a different key.
        doc.insert(ID_FIELD, id.to_bson());

        let key = doc_key(id)?;
        let body = bson::serialize_to_vec(&doc)?;
        let stamp = self.next_stamp();

        let txn = self.db().begin_write()?;
        let (existed, previous) = {
            let mut docs = txn.open_table(tables::DOCS)?;
            // The previous image is needed to remove the index entries it
            // contributed — they are derived from the old value, not the new.
            let previous = match docs.get((coll.id.0, key.as_slice()))? {
                Some(raw) => codec::decode_doc_record(raw.value())?.document()?,
                None => None,
            };
            let existed = previous.is_some();

            if !existed && !upsert {
                drop(docs);
                txn.abort()?;
                return Ok(WriteOutcome { matched: false, modified: false, upserted: false });
            }

            let record = DocRecord::live(stamp, body.clone());
            docs.insert((coll.id.0, key.as_slice()), codec::encode_doc_record(&record).as_slice())?;
            (existed, previous)
        };

        if let Err(e) =
            index::maintain(&txn, coll.id, &coll.indexes, previous.as_ref(), Some(&doc), &key)
        {
            txn.abort()?;
            return Err(e);
        }

        let entry = OplogEntry {
            stamp,
            // An upsert that created the document is an insert as far as a
            // change-stream subscriber is concerned.
            kind: if existed { OpKind::Replace } else { OpKind::Insert },
            collection: coll.id,
            doc_id: Some(id.clone()),
            body: Some(body),
        };
        append_oplog(&txn, &entry)?;
        txn.commit()?;
        self.publish(vec![entry]);

        Ok(WriteOutcome { matched: existed, modified: existed, upserted: !existed })
    }

    /// Delete a document, leaving a tombstone.
    ///
    /// The tombstone is what lets this delete beat a concurrent insert that
    /// arrives from a peer later; removing the key outright would make that
    /// insert look brand new and silently undo the delete.
    pub fn delete(&self, coll: &CollectionMeta, id: &DocId) -> Result<bool> {
        let key = doc_key(id)?;
        let stamp = self.next_stamp();

        let txn = self.db().begin_write()?;
        let previous = {
            let mut docs = txn.open_table(tables::DOCS)?;
            let previous = match docs.get((coll.id.0, key.as_slice()))? {
                Some(raw) => codec::decode_doc_record(raw.value())?.document()?,
                None => None,
            };
            if previous.is_none() {
                drop(docs);
                txn.abort()?;
                return Ok(false);
            }
            docs.insert(
                (coll.id.0, key.as_slice()),
                codec::encode_doc_record(&DocRecord::tombstone(stamp)).as_slice(),
            )?;
            previous
        };

        // A tombstoned document must leave no index entries behind, or a scan
        // would surface a candidate whose document no longer exists.
        index::maintain(&txn, coll.id, &coll.indexes, previous.as_ref(), None, &key)?;
        let existed = true;

        let entry = OplogEntry {
            stamp,
            kind: OpKind::Delete,
            collection: coll.id,
            doc_id: Some(id.clone()),
            body: None,
        };
        append_oplog(&txn, &entry)?;
        txn.commit()?;
        self.publish(vec![entry]);

        Ok(existed)
    }

    // -----------------------------------------------------------------------
    // Replication
    // -----------------------------------------------------------------------

    /// Apply an oplog entry received from a peer.
    ///
    /// Returns whether the entry won its conflict and was applied. The decision
    /// routes through [`DocRecord::merge`], the single definition of
    /// last-writer-wins, so replication cannot drift from local writes.
    ///
    /// The entry keeps its originating stamp, so it lands in the oplog at its
    /// original position — which may be *behind* the local tail. Change-stream
    /// subscribers that have already read past that point will not see it; the
    /// cluster work in M4 addresses that.
    pub fn apply_remote(&self, coll: &CollectionMeta, entry: &OplogEntry) -> Result<bool> {
        let Some(id) = entry.doc_id.clone() else {
            // Collection-level operations carry no document to merge.
            self.witness(&entry.stamp);
            return Ok(false);
        };

        let key = doc_key(&id)?;
        let incoming = match &entry.body {
            Some(body) => DocRecord::live(entry.stamp, body.clone()),
            None => DocRecord::tombstone(entry.stamp),
        };

        let txn = self.db().begin_write()?;
        let violations;
        let applied = {
            let mut docs = txn.open_table(tables::DOCS)?;
            let existing = match docs.get((coll.id.0, key.as_slice()))? {
                Some(raw) => Some(codec::decode_doc_record(raw.value())?),
                None => None,
            };

            // The incoming entry must win *strictly*. An equal stamp means we
            // have already applied this exact write — peers resend overlapping
            // ranges routinely — and treating that as a win would re-publish a
            // duplicate change-stream event.
            let wins = match &existing {
                Some(current) => incoming.stamp.wins_over(&current.stamp),
                None => true,
            };

            if !wins {
                drop(docs);
                txn.abort()?;
                self.witness(&entry.stamp);
                return Ok(false);
            }

            // The previous image is needed to remove the index entries it
            // produced, exactly as on the local replace path.
            let previous = match &existing {
                Some(current) => current.document()?,
                None => None,
            };

            let winner = match existing {
                Some(current) => current.merge(incoming.clone()),
                None => incoming.clone(),
            };
            debug_assert_eq!(winner.stamp, incoming.stamp, "merge disagreed with wins_over");

            docs.insert((coll.id.0, key.as_slice()), codec::encode_doc_record(&winner).as_slice())?;
            drop(docs);

            // Secondary indexes are maintained here for the same reason they
            // are on the local path: an index that does not see a replicated
            // write leaves an index-backed query unable to find a document that
            // demonstrably exists. Same transaction, so the two cannot disagree.
            let next = winner.document()?;
            violations = index::maintain_remote(
                &txn,
                coll.id,
                &coll.indexes,
                previous.as_ref(),
                next.as_ref(),
                &key,
            )?;
            true
        };

        append_oplog(&txn, entry)?;
        txn.commit()?;

        // Advance the local clock past what we just accepted, so a subsequent
        // local write is ordered after it.
        self.witness(&entry.stamp);

        let mut published = vec![entry.clone()];
        for violation in &violations {
            self.count_unique_violation();
            warn!(
                index = %violation.index,
                holders = violation.holders.len(),
                collection = %coll.name,
                "a merged write broke a unique constraint"
            );
            published.push(self.log_unique_violation(coll, &id, violation)?);
        }

        self.publish(published);

        Ok(applied)
    }
}

impl Engine {
    /// Append the oplog entry that carries a violation to change streams.
    ///
    /// Locally stamped, because this is *this node's* observation rather than a
    /// replicated fact — every node detects the same collision independently
    /// when it merges, so a shared stamp would be wrong and shipping the entry
    /// to peers would double-report.
    ///
    /// A separate transaction from the merge itself, deliberately. The merge
    /// must not fail because reporting failed: a converged write with an
    /// unreported violation is bad, but a *rejected* replicated write is worse,
    /// because the nodes then never agree.
    fn log_unique_violation(
        &self,
        coll: &CollectionMeta,
        merged: &DocId,
        violation: &index::UniqueViolation,
    ) -> Result<OplogEntry> {
        let mut ids = Vec::with_capacity(violation.holders.len());
        for holder in &violation.holders {
            // The holder list is encoded document keys, which do not decode
            // back to ids; read each document to recover its `_id`.
            match self.document_at_key(coll, holder)? {
                Some(id) => ids.push(id),
                None => continue,
            }
        }

        let detail =
            kimmy_core::UniqueViolationDetail::new(violation.index.clone(), merged.clone(), ids);

        let entry = OplogEntry {
            stamp: self.next_stamp(),
            kind: OpKind::UniqueViolation,
            collection: coll.id,
            doc_id: None,
            body: Some(bson::serialize_to_vec(&detail)?),
        };

        let txn = self.db().begin_write()?;
        append_oplog(&txn, &entry)?;
        txn.commit()?;
        Ok(entry)
    }

    /// The `_id` of the document stored under an encoded key.
    fn document_at_key(&self, coll: &CollectionMeta, key: &[u8]) -> Result<Option<DocId>> {
        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        let Some(raw) = docs.get((coll.id.0, key))? else {
            return Ok(None);
        };
        let record = codec::decode_doc_record(raw.value())?;
        match record.document()? {
            Some(doc) => Ok(Some(extract_id(&doc)?)),
            None => Ok(None),
        }
    }
}

/// The storage key for a document id.
pub(crate) fn doc_key(id: &DocId) -> Result<Vec<u8>> {
    Ok(keyenc::encode(&id.to_bson())?)
}

/// Pull the `_id` out of a stored document.
fn extract_id(doc: &Document) -> Result<DocId> {
    match doc.get(ID_FIELD) {
        Some(value) => Ok(DocId::try_from_bson(value)?),
        None => Err(StorageError::Corrupt("stored document has no _id".into())),
    }
}

/// Reconstruct the stamp a stored record carries, for tests and repair.
pub(crate) fn record_stamp(
    engine: &Engine,
    coll: &CollectionMeta,
    id: &DocId,
) -> Result<Option<Stamp>> {
    let key = doc_key(id)?;
    let txn = engine.db().begin_read()?;
    let docs = txn.open_table(tables::DOCS)?;
    match docs.get((coll.id.0, key.as_slice()))? {
        Some(raw) => Ok(Some(codec::decode_doc_record(raw.value())?.stamp)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::{Hlc, NodeId};

    use super::*;

    fn engine() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("app", "docs").unwrap();
        (engine, coll, dir)
    }

    #[test]
    fn insert_and_get_round_trip() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "name": "ada", "age": 36 }).unwrap();

        let found = engine.get(&coll, &id).unwrap().expect("document should exist");
        assert_eq!(found.get_str("name").unwrap(), "ada");
        assert_eq!(found.get_i32("age").unwrap(), 36);
    }

    #[test]
    fn insert_generates_an_id_and_stores_it_in_the_document() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "x": 1 }).unwrap();

        let found = engine.get(&coll, &id).unwrap().unwrap();
        assert!(found.contains_key(ID_FIELD), "the stored document must carry its _id");
        assert_eq!(DocId::try_from_bson(found.get(ID_FIELD).unwrap()).unwrap(), id);
    }

    #[test]
    fn insert_honours_a_supplied_id() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "_id": "custom", "x": 1 }).unwrap();
        assert_eq!(id, DocId::String("custom".into()));
        assert!(engine.get(&coll, &id).unwrap().is_some());
    }

    #[test]
    fn duplicate_ids_are_rejected() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": 1, "v": "first" }).unwrap();
        assert!(matches!(
            engine.insert(&coll, doc! { "_id": 1, "v": "second" }),
            Err(StorageError::Core(CoreError::DuplicateKey(_)))
        ));
        // The original must be untouched by the failed insert.
        let found = engine.get(&coll, &DocId::Int64(1)).unwrap().unwrap();
        assert_eq!(found.get_str("v").unwrap(), "first");
    }

    #[test]
    fn an_id_of_the_wrong_type_is_rejected() {
        let (engine, coll, _dir) = engine();
        assert!(engine.insert(&coll, doc! { "_id": { "nested": 1 }, "x": 1 }).is_err());
    }

    #[test]
    fn delete_leaves_the_document_invisible() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "x": 1 }).unwrap();

        assert!(engine.delete(&coll, &id).unwrap());
        assert!(engine.get(&coll, &id).unwrap().is_none());
        assert_eq!(engine.count(&coll).unwrap(), 0);
        // Deleting again reports nothing was there.
        assert!(!engine.delete(&coll, &id).unwrap());
    }

    #[test]
    fn delete_leaves_a_tombstone_rather_than_removing_the_key() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "_id": 1 }).unwrap();
        engine.delete(&coll, &id).unwrap();

        // The record must still be on disk: without it, a concurrent insert
        // replicated from a peer would look brand new and undo the delete.
        let stamp = record_stamp(&engine, &coll, &id).unwrap();
        assert!(stamp.is_some(), "the tombstone must persist");
    }

    #[test]
    fn a_deleted_id_can_be_reinserted() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "_id": 1, "v": "first" }).unwrap();
        engine.delete(&coll, &id).unwrap();

        engine.insert(&coll, doc! { "_id": 1, "v": "second" }).unwrap();
        let found = engine.get(&coll, &id).unwrap().unwrap();
        assert_eq!(found.get_str("v").unwrap(), "second");
    }

    #[test]
    fn replace_overwrites_and_reports_a_match() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "_id": 1, "a": 1, "b": 2 }).unwrap();

        let outcome = engine.replace(&coll, &id, doc! { "a": 9 }, false).unwrap();
        assert_eq!(outcome, WriteOutcome { matched: true, modified: true, upserted: false });

        let found = engine.get(&coll, &id).unwrap().unwrap();
        assert_eq!(found.get_i32("a").unwrap(), 9);
        assert!(!found.contains_key("b"), "replace is not a merge");
    }

    #[test]
    fn replace_without_upsert_does_not_create() {
        let (engine, coll, _dir) = engine();
        let id = DocId::Int64(404);
        let outcome = engine.replace(&coll, &id, doc! { "a": 1 }, false).unwrap();
        assert_eq!(outcome, WriteOutcome { matched: false, modified: false, upserted: false });
        assert!(engine.get(&coll, &id).unwrap().is_none());
    }

    #[test]
    fn replace_with_upsert_creates() {
        let (engine, coll, _dir) = engine();
        let id = DocId::Int64(7);
        let outcome = engine.replace(&coll, &id, doc! { "a": 1 }, true).unwrap();
        assert!(outcome.upserted && !outcome.matched);

        let found = engine.get(&coll, &id).unwrap().unwrap();
        assert_eq!(DocId::try_from_bson(found.get(ID_FIELD).unwrap()).unwrap(), id);
    }

    #[test]
    fn replace_cannot_move_a_document_to_a_different_id() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "_id": 1, "v": "a" }).unwrap();

        // A hostile or careless client sends a different _id in the body.
        engine.replace(&coll, &id, doc! { "_id": 999, "v": "b" }, false).unwrap();

        let found = engine.get(&coll, &id).unwrap().unwrap();
        assert_eq!(DocId::try_from_bson(found.get(ID_FIELD).unwrap()).unwrap(), id);
        assert!(engine.get(&coll, &DocId::Int64(999)).unwrap().is_none());
    }

    #[test]
    fn scanning_yields_live_documents_in_id_order() {
        let (engine, coll, _dir) = engine();
        for i in [3, 1, 2] {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }
        engine.delete(&coll, &DocId::Int64(2)).unwrap();

        let mut seen = Vec::new();
        engine
            .for_each_doc(&coll, |id, _| {
                seen.push(id);
                Ok(true)
            })
            .unwrap();

        assert_eq!(seen, vec![DocId::Int64(1), DocId::Int64(3)]);
    }

    #[test]
    fn scanning_can_stop_early() {
        let (engine, coll, _dir) = engine();
        for i in 0..10 {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }

        let mut seen = 0;
        engine
            .for_each_doc(&coll, |_, _| {
                seen += 1;
                Ok(seen < 3)
            })
            .unwrap();
        assert_eq!(seen, 3);
    }

    #[test]
    fn documents_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");

        let id = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("app", "docs").unwrap();
            engine.insert(&coll, doc! { "v": "durable" }).unwrap()
        };

        let engine = Engine::open(&path).unwrap();
        let coll = engine.get_collection("app", "docs").unwrap();
        assert_eq!(engine.get(&coll, &id).unwrap().unwrap().get_str("v").unwrap(), "durable");
    }

    #[test]
    fn writes_publish_events_only_after_they_commit() {
        let (engine, coll, _dir) = engine();
        let mut rx = engine.subscribe();

        let id = engine.insert(&coll, doc! { "x": 1 }).unwrap();
        let event = rx.try_recv().expect("insert should publish");
        assert_eq!(event.kind, OpKind::Insert);
        assert_eq!(event.doc_id.as_ref(), Some(&id));

        engine.delete(&coll, &id).unwrap();
        assert_eq!(rx.try_recv().unwrap().kind, OpKind::Delete);

        // A rejected duplicate must publish nothing.
        engine.insert(&coll, doc! { "_id": 5 }).unwrap();
        rx.try_recv().unwrap();
        let _ = engine.insert(&coll, doc! { "_id": 5 });
        assert!(rx.try_recv().is_err(), "a failed write must not publish an event");
    }

    // -----------------------------------------------------------------------
    // Replication
    // -----------------------------------------------------------------------

    fn remote_entry(
        coll: &CollectionMeta,
        id: &DocId,
        ms: u64,
        node: u8,
        body: Option<Document>,
    ) -> OplogEntry {
        OplogEntry {
            stamp: Stamp::new(Hlc::new(ms, 0), NodeId::from_bytes([node; 16])),
            kind: if body.is_some() { OpKind::Replace } else { OpKind::Delete },
            collection: coll.id,
            doc_id: Some(id.clone()),
            body: body.map(|d| bson::serialize_to_vec(&d).unwrap()),
        }
    }

    #[test]
    fn a_newer_remote_write_wins() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "_id": 1, "v": "local" }).unwrap();

        let local_stamp = record_stamp(&engine, &coll, &id).unwrap().unwrap();
        let entry = remote_entry(
            &coll,
            &id,
            local_stamp.hlc.wall_ms + 1000,
            9,
            Some(doc! { "_id": 1, "v": "remote" }),
        );

        assert!(engine.apply_remote(&coll, &entry).unwrap());
        assert_eq!(engine.get(&coll, &id).unwrap().unwrap().get_str("v").unwrap(), "remote");
    }

    #[test]
    fn an_older_remote_write_loses_and_changes_nothing() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "_id": 1, "v": "local" }).unwrap();

        let entry = remote_entry(&coll, &id, 1, 9, Some(doc! { "_id": 1, "v": "stale" }));
        assert!(!engine.apply_remote(&coll, &entry).unwrap());
        assert_eq!(engine.get(&coll, &id).unwrap().unwrap().get_str("v").unwrap(), "local");
    }

    #[test]
    fn applying_the_same_remote_entry_twice_is_idempotent() {
        let (engine, coll, _dir) = engine();
        let mut rx = engine.subscribe();
        let id = DocId::Int64(1);
        let entry = remote_entry(&coll, &id, 5_000, 9, Some(doc! { "_id": 1, "v": "once" }));

        assert!(engine.apply_remote(&coll, &entry).unwrap());
        rx.try_recv().expect("the first application should publish");

        // Replay must not flip the document or re-report as applied: peers
        // resend overlapping ranges routinely.
        assert!(!engine.apply_remote(&coll, &entry).unwrap());
        assert_eq!(engine.get(&coll, &id).unwrap().unwrap().get_str("v").unwrap(), "once");

        // The consequence that actually reaches users: a redelivered entry
        // must not surface as a second change-stream event.
        assert!(rx.try_recv().is_err(), "a replayed entry must not publish a duplicate event");
    }

    #[test]
    fn a_remote_delete_tombstones_a_local_document() {
        let (engine, coll, _dir) = engine();
        let id = engine.insert(&coll, doc! { "_id": 1, "v": "local" }).unwrap();
        let local = record_stamp(&engine, &coll, &id).unwrap().unwrap();

        let entry = remote_entry(&coll, &id, local.hlc.wall_ms + 1000, 9, None);
        assert!(engine.apply_remote(&coll, &entry).unwrap());
        assert!(engine.get(&coll, &id).unwrap().is_none());
    }

    #[test]
    fn applying_a_remote_write_advances_the_local_clock() {
        let (engine, coll, _dir) = engine();
        let id = DocId::Int64(1);
        let far_future = 9_000_000_000_000;
        let entry = remote_entry(&coll, &id, far_future, 9, Some(doc! { "_id": 1 }));
        engine.apply_remote(&coll, &entry).unwrap();

        // A subsequent local write must be ordered after what we accepted, or
        // it would lose to the write it logically follows.
        assert!(engine.next_stamp().hlc > entry.stamp.hlc);
    }

    #[test]
    fn concurrent_writes_converge_regardless_of_arrival_order() {
        // The same two conflicting writes applied in opposite orders on two
        // replicas must produce the same document.
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let a = Engine::open(&dir_a.path().join("a.redb")).unwrap();
        let b = Engine::open(&dir_b.path().join("b.redb")).unwrap();
        let coll_a = a.create_collection("app", "docs").unwrap();
        let coll_b = b.create_collection("app", "docs").unwrap();

        let id = DocId::Int64(1);
        // Same HLC, different nodes: the node id decides the winner.
        let from_1 = remote_entry(&coll_a, &id, 1_000, 1, Some(doc! { "_id": 1, "v": "one" }));
        let from_2 = remote_entry(&coll_a, &id, 1_000, 2, Some(doc! { "_id": 1, "v": "two" }));

        a.apply_remote(&coll_a, &from_1).unwrap();
        a.apply_remote(&coll_a, &from_2).unwrap();
        b.apply_remote(&coll_b, &from_2).unwrap();
        b.apply_remote(&coll_b, &from_1).unwrap();

        let doc_a = a.get(&coll_a, &id).unwrap().unwrap();
        let doc_b = b.get(&coll_b, &id).unwrap().unwrap();
        assert_eq!(doc_a, doc_b, "replicas must converge");
        assert_eq!(doc_a.get_str("v").unwrap(), "two");
    }
    // -----------------------------------------------------------------------
    // Replicated writes and secondary indexes
    // -----------------------------------------------------------------------

    fn field(path: &str) -> crate::meta::IndexField {
        crate::meta::IndexField { path: path.into(), descending: false }
    }

    /// An engine with an empty collection named `db`.`c`.
    fn indexed_engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("db", "c").unwrap();
        (engine, dir)
    }

    fn remote_insert(coll: &CollectionMeta, id: &str, doc: Document, wall_ms: u64) -> OplogEntry {
        OplogEntry {
            stamp: Stamp::new(kimmy_core::Hlc::new(wall_ms, 0), kimmy_core::NodeId::generate()),
            kind: OpKind::Insert,
            collection: coll.id,
            doc_id: Some(DocId::String(id.into())),
            body: Some(bson::serialize_to_vec(&doc).unwrap()),
        }
    }

    #[test]
    fn a_replicated_document_is_visible_to_an_index() {
        // Without index maintenance on the remote path, an index-backed query
        // silently cannot find a document that demonstrably exists.
        let (engine, _dir) = indexed_engine();
        engine.create_index("db", "c", vec![field("email")], false, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();

        let entry =
            remote_insert(&coll, "peer-1", doc! { "_id": "peer-1", "email": "a@x.com" }, 1_000);
        assert!(engine.apply_remote(&coll, &entry).unwrap());

        let index = &engine.get_collection("db", "c").unwrap().indexes[0];
        let key = keyenc::encode(&bson::Bson::String("a@x.com".into())).unwrap();
        let found = engine.index_candidates(&coll, index.id, &key, &key).unwrap();
        assert_eq!(found.len(), 1, "a replicated document must be indexed");
    }

    #[test]
    fn replacing_a_replicated_document_clears_its_old_index_entry() {
        // The previous image has to be removed, or the index accumulates
        // entries for values the document no longer holds.
        let (engine, _dir) = indexed_engine();
        engine.create_index("db", "c", vec![field("email")], false, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();

        engine
            .apply_remote(
                &coll,
                &remote_insert(&coll, "p", doc! { "_id": "p", "email": "old@x" }, 1_000),
            )
            .unwrap();
        engine
            .apply_remote(
                &coll,
                &remote_insert(&coll, "p", doc! { "_id": "p", "email": "new@x" }, 2_000),
            )
            .unwrap();

        let index = &engine.get_collection("db", "c").unwrap().indexes[0];
        let stale = keyenc::encode(&bson::Bson::String("old@x".into())).unwrap();
        let found = engine.index_candidates(&coll, index.id, &stale, &stale).unwrap();
        assert!(found.is_empty(), "the superseded value must not stay indexed");
    }

    #[test]
    fn a_merged_write_may_break_a_unique_constraint_and_is_counted() {
        // The heart of ADR-020. Two documents with different _ids carry the
        // same unique value; last-writer-wins never runs on them because they
        // are different keys, so both survive and the constraint is violated.
        //
        // Refusing the remote write would mean the nodes never converge, which
        // is the availability this design exists to provide. So it is applied,
        // indexed, and reported.
        let (engine, _dir) = indexed_engine();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();

        engine.insert(&coll, doc! { "_id": "local", "email": "clash@x" }).unwrap();
        assert_eq!(engine.unique_violations(), 0);

        let entry =
            remote_insert(&coll, "remote", doc! { "_id": "remote", "email": "clash@x" }, 9_000);
        assert!(engine.apply_remote(&coll, &entry).unwrap(), "the write must be applied");

        assert_eq!(engine.unique_violations(), 1, "the violation must be counted");

        // Both documents exist. Nothing was discarded.
        assert!(engine.get(&coll, &DocId::String("local".into())).unwrap().is_some());
        assert!(engine.get(&coll, &DocId::String("remote".into())).unwrap().is_some());

        // And both are findable through the index, which is the point of adding
        // the entry anyway rather than skipping it.
        let index = &engine.get_collection("db", "c").unwrap().indexes[0];
        let key = keyenc::encode(&bson::Bson::String("clash@x".into())).unwrap();
        let found = engine.index_candidates(&coll, index.id, &key, &key).unwrap();
        assert_eq!(found.len(), 2, "both holders must be reachable through the index");
    }

    #[test]
    fn a_local_write_is_still_rejected_on_a_unique_violation() {
        // The asymmetry is deliberate: a local client is still there to be told.
        let (engine, _dir) = indexed_engine();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();

        engine.insert(&coll, doc! { "_id": "a", "email": "same@x" }).unwrap();
        let err = engine.insert(&coll, doc! { "_id": "b", "email": "same@x" });

        assert!(matches!(err, Err(StorageError::Core(CoreError::UniqueViolation { .. }))));
        assert_eq!(engine.unique_violations(), 0, "a rejected local write is not a violation");
    }

    #[test]
    fn re_applying_a_remote_entry_does_not_double_count_a_violation() {
        // Peers resend overlapping ranges; the same collision must not inflate
        // the metric every time.
        let (engine, _dir) = indexed_engine();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();

        engine.insert(&coll, doc! { "_id": "local", "email": "clash@x" }).unwrap();
        let entry =
            remote_insert(&coll, "remote", doc! { "_id": "remote", "email": "clash@x" }, 9_000);

        engine.apply_remote(&coll, &entry).unwrap();
        engine.apply_remote(&coll, &entry).unwrap();

        assert_eq!(engine.unique_violations(), 1, "a resend must not be counted again");
    }
    #[tokio::test]
    async fn a_merged_violation_reaches_a_change_stream() {
        // ADR-020's commitment: the violation is an event a client can act on,
        // not just a log line. It has to be an oplog entry to be one, because
        // streams read from the oplog.
        use crate::watch::{ChangeEvent, WatchOptions, WatchScope};

        let (engine, _dir) = indexed_engine();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        engine.insert(&coll, doc! { "_id": "local", "email": "clash@x" }).unwrap();

        let mut stream =
            engine.watch(WatchScope::Collection(coll.id), WatchOptions::default()).unwrap();

        let entry =
            remote_insert(&coll, "remote", doc! { "_id": "remote", "email": "clash@x" }, 9_000);
        engine.apply_remote(&coll, &entry).unwrap();

        // The merged insert first, then the violation it revealed.
        let mut kinds = Vec::new();
        let mut detail = None;
        for _ in 0..2 {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.next(&engine))
                    .await
                    .expect("timed out")
                    .expect("stream ended early");
            if let ChangeEvent::Change { entry, .. } = event {
                kinds.push(entry.kind);
                if entry.kind == OpKind::UniqueViolation {
                    detail = Some(
                        bson::deserialize_from_slice::<kimmy_core::UniqueViolationDetail>(
                            entry.body.as_ref().unwrap(),
                        )
                        .unwrap(),
                    );
                }
            }
        }

        assert!(kinds.contains(&OpKind::UniqueViolation), "expected a violation event: {kinds:?}");
        let detail = detail.expect("the violation must carry its detail");
        assert_eq!(detail.index, "email_1", "the event must name the index that broke");
        assert_eq!(detail.merged, DocId::String("remote".into()));
        assert_eq!(detail.ids.len(), 2, "both holders must be named: {:?}", detail.ids);
        assert!(detail.ids.contains(&DocId::String("local".into())));
        assert!(detail.ids.contains(&DocId::String("remote".into())));
    }

    #[test]
    fn a_clean_merge_logs_no_violation_entry() {
        let (engine, _dir) = indexed_engine();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();

        engine
            .apply_remote(
                &coll,
                &remote_insert(&coll, "a", doc! { "_id": "a", "email": "x@y" }, 1_000),
            )
            .unwrap();

        let entries = engine.read_arrival_from(0, 100).unwrap();
        assert!(
            !entries.iter().any(|e| e.kind == OpKind::UniqueViolation),
            "a merge that broke nothing must not report a violation"
        );
    }
}
