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

use bson::{Bson, Document};
use kimmy_core::{
    CollectionId, DocId, DocRecord, Error as CoreError, OpKind, OplogEntry, ResumeToken, Stamp,
    keyenc,
};
use redb::{ReadableDatabase, ReadableTable};
use tracing::warn;

use crate::codec;
use crate::engine::{Engine, Position, WriteTxn, WriterHolder, append_oplog, doc_range_after};
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
    /// The stamp the write produced, when it wrote anything.
    ///
    /// What a caller hands back as `if_stamp` to make its next write
    /// conditional on nothing having happened in between.
    pub stamp: Option<Stamp>,
}

/// A bulk insert's failure, and which document caused it.
///
/// A batch is all-or-nothing, so nothing survives to point at: without the
/// position the caller would know only that one of their documents was bad.
#[derive(Debug)]
pub struct BulkInsertError {
    /// Position in the submitted batch, absent when the transaction itself
    /// failed rather than any one document.
    pub index: Option<usize>,
    pub source: StorageError,
}

impl BulkInsertError {
    /// A failure of the transaction rather than of a document.
    fn transaction(e: impl Into<StorageError>) -> Self {
        Self { index: None, source: e.into() }
    }
}

impl std::fmt::Display for BulkInsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.index {
            Some(i) => write!(f, "document at index {i}: {}", self.source),
            None => write!(f, "{}", self.source),
        }
    }
}

impl std::error::Error for BulkInsertError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// The conventional primary-key field name.
pub const ID_FIELD: &str = "_id";

/// The writes of one [`Engine::write_batch`], and the entries they produced.
///
/// Constructible only by `write_batch`, which is what keeps the writer
/// inside the crate: a caller can compose replaces and deletes into one
/// commit but never hold, commit or abort the transaction itself. Each write
/// mints its own stamp under the writer (ADR-148) and its entry is kept here,
/// in the order written, for the engine to publish after the one commit.
///
/// A write that fails poisons the scope. The in-transaction forms write the
/// document before its index entries and its oplog entry, so a failure part
/// way through leaves the transaction holding a document nothing indexes
/// and nothing logs; every caller inside this crate aborts on that, but a
/// closure outside it may ignore the error and carry on. So the first
/// failure is remembered here: every later write on the scope refuses
/// without writing, and `write_batch` aborts the scope whatever the closure
/// answers. A torn commit is not something a caller can opt into.
///
/// A scope also carries what must happen only *after* its commit. A vector
/// write through [`Self::put_vectors`] or [`Self::delete_vectors`] names
/// the shadow collection it changed, and `write_batch` bumps that
/// collection's vector generation once the commit has landed — never inside
/// the scope, where a bump would let an index build read the new generation
/// against data the commit had not yet made visible, and be served as fresh
/// for it.
pub struct WriteScope<'a> {
    pub(crate) engine: &'a Engine,
    txn: WriteTxn<'a>,
    entries: Vec<OplogEntry>,
    /// Whether anything has landed in the transaction: a write that produced
    /// an entry, or a consumer position, which produces none. A scope that
    /// wrote nothing is aborted, not committed.
    wrote: bool,
    /// Shadow collections whose vectors this scope changed, each once, for
    /// the generation bump that follows the commit.
    vector_generations: Vec<CollectionId>,
    /// The first write that failed, as its error read; `Some` means the
    /// transaction may hold a partial write and can never commit.
    poisoned: Option<String>,
}

impl WriteScope<'_> {
    /// [`Engine::replace`], into this scope's transaction.
    pub fn replace(
        &mut self,
        coll: &CollectionMeta,
        id: &DocId,
        doc: Document,
        upsert: bool,
    ) -> Result<WriteOutcome> {
        self.refuse_if_poisoned()?;
        let (outcome, entry) = self
            .engine
            .replace_in_txn(&self.txn, coll, id, doc, upsert, None)
            .map_err(|e| self.poison(e))?;
        self.wrote |= entry.is_some();
        self.entries.extend(entry);
        Ok(outcome)
    }

    /// [`Engine::delete`], into this scope's transaction.
    pub fn delete(&mut self, coll: &CollectionMeta, id: &DocId) -> Result<bool> {
        self.refuse_if_poisoned()?;
        let entry = self
            .engine
            .delete_in_txn(&self.txn, coll, id, |_, _| Ok(true))
            .map_err(|e| self.poison(e))?;
        let removed = entry.is_some();
        self.wrote |= removed;
        self.entries.extend(entry);
        Ok(removed)
    }

    /// [`Engine::put_consumer_position`], into this scope's transaction.
    ///
    /// The third kind of write a scope takes, and the one with nothing to
    /// publish: a consumer's position is a row in the metadata table, not a
    /// document, so it mints no stamp and yields no entry — ADR-148 is not
    /// engaged — but it is a write all the same, and a scope holding only a
    /// position commits. What it buys is the embedding worker's checkpoint
    /// riding in the same commit as the batch it covers (ADR-125), rather
    /// than in one of its own. Under the same poison rule as `replace` and
    /// `delete`: a failed write here is refused every write after it, and
    /// the scope aborts.
    pub fn put_consumer_position(&mut self, consumer: &str, token: ResumeToken) -> Result<()> {
        self.refuse_if_poisoned()?;
        self.engine
            .put_consumer_position_in_txn(&self.txn, consumer, token)
            .map_err(|e| self.poison(e))?;
        self.wrote = true;
        Ok(())
    }

    /// Note that this scope changed a shadow collection's vectors, so
    /// `write_batch` bumps its generation once the commit has landed.
    pub(crate) fn touch_vector_generation(&mut self, shadow: CollectionId) {
        if !self.vector_generations.contains(&shadow) {
            self.vector_generations.push(shadow);
        }
    }

    /// Remember the first failure and hand it back unchanged: the caller
    /// sees the error it caused, and the scope sees that it can never
    /// commit.
    fn poison(&mut self, e: StorageError) -> StorageError {
        self.poisoned.get_or_insert_with(|| e.to_string());
        e
    }

    fn refuse_if_poisoned(&self) -> Result<()> {
        match &self.poisoned {
            Some(first) => Err(Self::poisoned_error(first)),
            None => Ok(()),
        }
    }

    /// The error a poisoned scope answers with, on every write after the
    /// failure and from `write_batch` itself. A `Transaction` error, because
    /// that is what it is: the transaction cannot be committed.
    fn poisoned_error(first: &str) -> StorageError {
        StorageError::Transaction(format!(
            "a write inside this scope failed and the scope cannot commit: {first}"
        ))
    }
}

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
    pub fn for_each_doc<F>(&self, coll: &CollectionMeta, f: F) -> Result<()>
    where
        F: FnMut(DocId, Document) -> Result<bool>,
    {
        self.for_each_doc_after(coll, None, f)
    }

    /// [`Engine::for_each_doc`], resuming strictly after an encoded key.
    ///
    /// The bound goes to redb rather than being filtered afterwards, which is
    /// what makes paging cost the size of the *page* instead of the size of
    /// everything before it. `keyenc` is order-preserving, so "after these
    /// bytes" and "after this `_id`" are the same statement.
    pub fn for_each_doc_after<F>(
        &self,
        coll: &CollectionMeta,
        after: Option<&[u8]>,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(DocId, Document) -> Result<bool>,
    {
        self.for_each_record_after(coll, after, |id, _, doc| f(id, doc))
    }

    /// [`Engine::for_each_doc_after`], with each document's stamp.
    ///
    /// The one scan behind both: a read that wants versions — `find` with
    /// `stamps: true` — walks exactly the documents a plain read does.
    pub fn for_each_record_after<F>(
        &self,
        coll: &CollectionMeta,
        after: Option<&[u8]>,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(DocId, Stamp, Document) -> Result<bool>,
    {
        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;
        for entry in docs.range(doc_range_after(coll.id, after))? {
            let (_, value) = entry?;
            let record = codec::decode_doc_record(value.value())?;
            if record.deleted {
                continue;
            }
            let doc: Document = bson::deserialize_from_slice(&record.body)?;
            let id = extract_id(&doc)?;
            if !f(id, record.stamp, doc)? {
                break;
            }
        }
        Ok(())
    }

    /// One live document with its stamp, or `None` if absent or tombstoned.
    pub fn get_stamped(
        &self,
        coll: &CollectionMeta,
        id: &DocId,
    ) -> Result<Option<(Stamp, Document)>> {
        self.get_record_by_encoded_key(coll, &doc_key(id)?)
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
    pub fn insert(&self, coll: &CollectionMeta, doc: Document) -> Result<DocId> {
        self.insert_stamped(coll, doc).map(|(id, _)| id)
    }

    /// [`Engine::insert`], also returning the stamp the write produced — the
    /// version a caller needs for a conditional write that follows.
    pub fn insert_stamped(&self, coll: &CollectionMeta, doc: Document) -> Result<(DocId, Stamp)> {
        let txn = self.begin_write(WriterHolder::Write)?;
        let (id, entry) = match self.insert_in_txn(&txn, coll, doc) {
            Ok(pair) => pair,
            Err(e) => {
                txn.abort()?;
                return Err(e);
            }
        };
        txn.commit()?;
        let stamp = entry.stamp;
        self.publish(vec![entry]);

        Ok((id, stamp))
    }

    /// Insert many documents in a single transaction, or none of them.
    ///
    /// The whole point is the commit: one durable commit for the batch instead
    /// of one per document, which is the only win available here — throughput
    /// is flat across concurrent writers because redb has a single writer, so
    /// per-commit overhead is the cost worth amortizing.
    ///
    /// Atomicity falls out of that mechanism rather than being designed for,
    /// but it is promised: any failure aborts the batch entirely, including
    /// every oplog entry, so the version vector does not move for a batch that
    /// did not land. The error carries the offending document's position,
    /// because with nothing written that is the only thing telling the caller
    /// what to fix.
    pub fn insert_many(
        &self,
        coll: &CollectionMeta,
        docs: Vec<Document>,
    ) -> std::result::Result<Vec<DocId>, BulkInsertError> {
        self.insert_many_stamped(coll, docs).map(|v| v.into_iter().map(|(id, _)| id).collect())
    }

    /// [`Engine::insert_many`], also returning each document's stamp — every
    /// document in the batch gets its own, so a caller that wants to follow
    /// up one of them with a conditional write has the version to name.
    pub fn insert_many_stamped(
        &self,
        coll: &CollectionMeta,
        docs: Vec<Document>,
    ) -> std::result::Result<Vec<(DocId, Stamp)>, BulkInsertError> {
        // Nothing to do, and nothing to log: an empty batch must not open a
        // transaction, or it would append an oplog entry for a write that
        // never happened.
        if docs.is_empty() {
            return Ok(Vec::new());
        }

        let txn = self.begin_write(WriterHolder::Bulk).map_err(BulkInsertError::transaction)?;
        let mut ids = Vec::with_capacity(docs.len());
        let mut entries = Vec::with_capacity(docs.len());

        for (index, doc) in docs.into_iter().enumerate() {
            // Each document is checked against this transaction's own
            // uncommitted writes, so two documents colliding *within* the batch
            // are caught by exactly the checks that catch a collision with
            // stored state — redb reads see the writes of their own txn.
            match self.insert_in_txn(&txn, coll, doc) {
                Ok((id, entry)) => {
                    ids.push((id, entry.stamp));
                    entries.push(entry);
                }
                Err(source) => {
                    txn.abort().map_err(BulkInsertError::transaction)?;
                    return Err(BulkInsertError { index: Some(index), source });
                }
            }
        }

        txn.commit().map_err(BulkInsertError::transaction)?;
        self.publish(entries);

        Ok(ids)
    }

    /// The whole of an insert except the transaction's lifecycle.
    ///
    /// The caller owns the transaction, so this returns its failure rather than
    /// aborting: a batch has to abort once for the batch, not once per
    /// document.
    pub(crate) fn insert_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        coll: &CollectionMeta,
        doc: Document,
    ) -> Result<(DocId, OplogEntry)> {
        // The value the client wrote is kept as written, so an `Int32` id
        // stays an `Int32` rather than passing through `DocId` and back.
        let (id, id_value) = match doc.get(ID_FIELD) {
            Some(value) => (DocId::try_from_bson(value)?, value.clone()),
            None => {
                let id = DocId::generate();
                let value = id.to_bson();
                (id, value)
            }
        };
        let doc = with_id_first(doc, id_value);

        let key = doc_key(&id)?;
        let body = bson::serialize_to_vec(&doc)?;
        let stamp = self.next_stamp();

        {
            let mut docs = txn.open_table(tables::DOCS)?;
            // A tombstone may still occupy the key; overwriting it is a
            // legitimate resurrection, but a live document is a conflict.
            let occupied = match docs.get((coll.id.0, key.as_slice()))? {
                Some(raw) => codec::decode_doc_record(raw.value())?.is_live(),
                None => false,
            };
            if occupied {
                return Err(CoreError::DuplicateKey(id.to_string()).into());
            }
            let record = DocRecord::live(stamp, body.clone());
            docs.insert((coll.id.0, key.as_slice()), codec::encode_doc_record(&record).as_slice())?;
        }

        // Same transaction as the document write, so the index cannot describe
        // a state that never existed. A unique violation returns here and the
        // caller aborts, which discards the document write with it.
        let newly_multikey = index::maintain(self, txn, coll, None, Some(&doc), &key)?;
        index::mark_multikey(txn, &coll.db, &coll.name, &newly_multikey)?;

        let entry = OplogEntry {
            stamp,
            kind: OpKind::Insert,
            collection: coll.id,
            doc_id: Some(id.clone()),
            body: Some(body),
        };
        append_oplog(txn, &entry)?;

        Ok((id, entry))
    }

    /// Replace a document wholesale.
    ///
    /// With `upsert`, a missing document is created instead of reported as
    /// unmatched.
    pub fn replace(
        &self,
        coll: &CollectionMeta,
        id: &DocId,
        doc: Document,
        upsert: bool,
    ) -> Result<WriteOutcome> {
        self.replace_if(coll, id, doc, upsert, None)
    }

    /// [`Engine::replace`], conditional on the document's current version.
    ///
    /// With `expected`, the write happens only if the live document carries
    /// exactly that stamp; otherwise — a different version, or no live
    /// document at all — nothing is written and [`StorageError::Stale`]
    /// carries what is there now. The comparison runs inside the write
    /// transaction, so there is no window between the check and the write.
    pub fn replace_if(
        &self,
        coll: &CollectionMeta,
        id: &DocId,
        doc: Document,
        upsert: bool,
        expected: Option<Stamp>,
    ) -> Result<WriteOutcome> {
        let txn = self.begin_write(WriterHolder::Write)?;
        match self.replace_in_txn(&txn, coll, id, doc, upsert, expected) {
            Ok((outcome, Some(entry))) => {
                txn.commit()?;
                self.publish(vec![entry]);
                Ok(outcome)
            }
            // Unmatched and not an upsert: nothing was written, so nothing
            // is committed — a miss must not cost an fsync.
            Ok((outcome, None)) => {
                txn.abort()?;
                Ok(outcome)
            }
            Err(e) => {
                txn.abort()?;
                Err(e)
            }
        }
    }

    /// The whole of a replace except the transaction's lifecycle, the way
    /// [`Self::insert_in_txn`] is for an insert.
    ///
    /// The caller owns the transaction and holds the writer, so the stamp is
    /// minted here, under it (ADR-148): a stamp minted while another
    /// transaction holds the writer sorts below what that transaction
    /// commits first, and a peer reading this node in that interval
    /// witnesses past it unserved.
    ///
    /// Answers with the outcome and the entry to publish once the caller has
    /// committed. An unmatched replace without `upsert` writes nothing and
    /// yields no entry; a version that does not match `expected` is
    /// [`StorageError::Stale`], and the caller aborts. Nothing is aborted
    /// here: a scope holding many writes aborts once for the scope
    /// (ADR-149), not once per write.
    pub(crate) fn replace_in_txn(
        &self,
        txn: &WriteTxn<'_>,
        coll: &CollectionMeta,
        id: &DocId,
        doc: Document,
        upsert: bool,
        expected: Option<Stamp>,
    ) -> Result<(WriteOutcome, Option<OplogEntry>)> {
        // The id is part of the document's identity, not its content: a replace
        // must not be able to move a document to a different key.
        let doc = with_id_first(doc, id.to_bson());

        let key = doc_key(id)?;
        let body = bson::serialize_to_vec(&doc)?;
        let stamp = self.next_stamp();

        let (existed, previous) = {
            let mut docs = txn.open_table(tables::DOCS)?;
            // The previous image is needed to remove the index entries it
            // contributed — they are derived from the old value, not the new.
            let (current, previous) = match docs.get((coll.id.0, key.as_slice()))? {
                Some(raw) => {
                    let record = codec::decode_doc_record(raw.value())?;
                    (record.is_live().then_some(record.stamp), record.document()?)
                }
                None => (None, None),
            };
            let existed = previous.is_some();

            if expected.is_some() && current != expected {
                return Err(StorageError::Stale { current });
            }
            if !existed && !upsert {
                let unmatched =
                    WriteOutcome { matched: false, modified: false, upserted: false, stamp: None };
                return Ok((unmatched, None));
            }

            let record = DocRecord::live(stamp, body.clone());
            docs.insert((coll.id.0, key.as_slice()), codec::encode_doc_record(&record).as_slice())?;
            (existed, previous)
        };

        // Same transaction as the document write, so the index cannot describe
        // a state that never existed. A unique violation returns here and the
        // caller aborts, which discards the document write with it.
        let newly_multikey = index::maintain(self, txn, coll, previous.as_ref(), Some(&doc), &key)?;
        index::mark_multikey(txn, &coll.db, &coll.name, &newly_multikey)?;

        let entry = OplogEntry {
            stamp,
            // An upsert that created the document is an insert as far as a
            // change-stream subscriber is concerned.
            kind: if existed { OpKind::Replace } else { OpKind::Insert },
            collection: coll.id,
            doc_id: Some(id.clone()),
            body: Some(body),
        };
        append_oplog(txn, &entry)?;

        let outcome = WriteOutcome {
            matched: existed,
            modified: existed,
            upserted: !existed,
            stamp: Some(stamp),
        };
        Ok((outcome, Some(entry)))
    }

    /// Delete a document, leaving a tombstone.
    ///
    /// The tombstone is what lets this delete beat a concurrent insert that
    /// arrives from a peer later; removing the key outright would make that
    /// insert look brand new and silently undo the delete.
    pub fn delete(&self, coll: &CollectionMeta, id: &DocId) -> Result<bool> {
        Ok(self.delete_where(WriterHolder::Write, coll, id, |_, _| Ok(true))?.is_some())
    }

    /// [`Engine::delete`], conditional on the document's current version,
    /// answering with the tombstone's stamp.
    ///
    /// The same contract as [`Engine::replace_if`]: a different version, or
    /// no live document, writes nothing and returns [`StorageError::Stale`].
    /// Without a condition a missing document is not an error and the answer
    /// is `None`. The stamp is the version the delete produced — what a
    /// caller reports as `stamp`, the way every other write does (ADR-084);
    /// the filter route already had it from `modify_where`, and this is the
    /// by-id route's way to the same fact.
    pub fn delete_if(
        &self,
        coll: &CollectionMeta,
        id: &DocId,
        expected: Option<Stamp>,
    ) -> Result<Option<Stamp>> {
        let Some(expected) = expected else {
            return self.delete_where(WriterHolder::Write, coll, id, |_, _| Ok(true));
        };
        // The absent case is decided here rather than in the guard, which
        // only ever sees a live document.
        match self.delete_where(WriterHolder::Write, coll, id, |current, _| {
            if current == expected {
                Ok(true)
            } else {
                Err(StorageError::Stale { current: Some(current) })
            }
        })? {
            Some(stamp) => Ok(Some(stamp)),
            None => Err(StorageError::Stale { current: None }),
        }
    }

    /// [`Engine::delete`], but only if `guard` still approves the document it
    /// is about to remove.
    ///
    /// The guard runs **inside the write transaction**, on the image about to
    /// be tombstoned. That placement is the whole point: TTL expiry scans an
    /// index in one transaction and deletes in another, so a document whose
    /// date is bumped in between — a session heartbeat, which is the canonical
    /// reason to have a TTL at all — would otherwise be deleted while live.
    /// Re-reading here means what the guard approves is exactly what the
    /// commit removes.
    pub(crate) fn delete_guarded(
        &self,
        coll: &CollectionMeta,
        id: &DocId,
        guard: impl Fn(&Document) -> bool,
    ) -> Result<bool> {
        // The expiry pass's own hold, not a client's: a TTL delete costs the
        // same transaction as a client's delete, and an operator asking what
        // held the writer needs the two apart (ADR-159).
        Ok(self.delete_where(WriterHolder::Expiry, coll, id, |_, doc| Ok(guard(doc)))?.is_some())
    }

    /// One delete body, shared by `delete`, `delete_if` and `delete_guarded`:
    /// a check added to one must not be added *beside* the others, for the
    /// same reason `insert` and `insert_many` share `insert_in_txn`.
    ///
    /// `guard` sees the live document's stamp and image inside the write
    /// transaction. `Ok(false)` declines quietly — nothing is written and no
    /// oplog entry is minted, so a refused expiry is invisible to replication
    /// and to change streams. An error aborts the same way and is returned.
    ///
    /// Answers with the tombstone's stamp when a document was removed, and
    /// `None` when there was nothing to remove or the guard declined.
    ///
    /// `holder` is the one thing the three callers do not share: the same
    /// delete is a client's write or the expiry pass's, and the hold
    /// histogram has to be told which (ADR-159).
    fn delete_where(
        &self,
        holder: WriterHolder,
        coll: &CollectionMeta,
        id: &DocId,
        guard: impl Fn(Stamp, &Document) -> Result<bool>,
    ) -> Result<Option<Stamp>> {
        let txn = self.begin_write(holder)?;
        match self.delete_in_txn(&txn, coll, id, guard) {
            Ok(Some(entry)) => {
                txn.commit()?;
                let stamp = entry.stamp;
                self.publish(vec![entry]);
                Ok(Some(stamp))
            }
            // Nothing to remove, or the guard declined: nothing was written,
            // so nothing is committed — a refused expiry must not cost an
            // fsync.
            Ok(None) => {
                txn.abort()?;
                Ok(None)
            }
            Err(e) => {
                txn.abort()?;
                Err(e)
            }
        }
    }

    /// The whole of a delete except the transaction's lifecycle, the way
    /// [`Self::insert_in_txn`] is for an insert; `guard` is
    /// [`Self::delete_where`]'s, and runs on the image about to be
    /// tombstoned.
    ///
    /// The stamp is minted here, under the writer the caller holds
    /// (ADR-148). Answers with the tombstone's entry, to publish once the
    /// caller has committed, or `None` when there was nothing to remove or
    /// the guard declined — nothing was written then, and no entry is
    /// minted. Nothing is aborted here, for the reason
    /// [`Self::replace_in_txn`] gives.
    pub(crate) fn delete_in_txn(
        &self,
        txn: &WriteTxn<'_>,
        coll: &CollectionMeta,
        id: &DocId,
        guard: impl Fn(Stamp, &Document) -> Result<bool>,
    ) -> Result<Option<OplogEntry>> {
        let key = doc_key(id)?;
        let stamp = self.next_stamp();

        let previous = {
            let mut docs = txn.open_table(tables::DOCS)?;
            let (current, previous) = match docs.get((coll.id.0, key.as_slice()))? {
                Some(raw) => {
                    let record = codec::decode_doc_record(raw.value())?;
                    (record.stamp, record.document()?)
                }
                None => (stamp, None),
            };
            let Some(image) = previous.as_ref() else {
                return Ok(None);
            };
            if !guard(current, image)? {
                return Ok(None);
            }
            docs.insert(
                (coll.id.0, key.as_slice()),
                codec::encode_doc_record(&DocRecord::tombstone(stamp)).as_slice(),
            )?;
            previous
        };

        // A tombstoned document must leave no index entries behind, or a scan
        // would surface a candidate whose document no longer exists. A delete
        // writes no new image, so it can never flip the multikey flag.
        index::maintain(self, txn, coll, previous.as_ref(), None, &key)?;

        let entry = OplogEntry {
            stamp,
            kind: OpKind::Delete,
            collection: coll.id,
            doc_id: Some(id.clone()),
            body: None,
        };
        append_oplog(txn, &entry)?;

        Ok(Some(entry))
    }

    // -----------------------------------------------------------------------
    // A scoped write
    // -----------------------------------------------------------------------

    /// Compose several writes into one commit, from outside this crate.
    ///
    /// A loop that wraps a public one-commit-per-call method is the shape
    /// ADR-119 and ADR-125 each fixed once — the replica's apply and the
    /// worker's position — and the vector write and the embedding worker's
    /// store are the third and fourth. This is the general form (ADR-149):
    /// the closure gets a [`WriteScope`] whose writes go into one
    /// transaction the engine holds for the length of the closure, and the
    /// writer never leaves the crate. On `Ok` the scope commits once — the
    /// commit is counted and honours the durability class, as every commit
    /// does — and every entry the writes produced is published after it, in
    /// the order written, so nothing reaches a change stream before it is
    /// durable. On `Err` the scope is aborted, nothing is published, and the
    /// error is returned. A scope that wrote nothing is aborted too: no
    /// fsync, no count, and nothing to publish, which is the rule
    /// [`Self::insert_many`] states for an empty batch. "Nothing" is
    /// measured by writes, not by entries: a consumer position
    /// ([`WriteScope::put_consumer_position`]) produces no entry, and a
    /// scope holding only one still commits.
    ///
    /// After the commit, and only then, the vector generation of every
    /// shadow collection the scope changed through
    /// [`WriteScope::put_vectors`] or [`WriteScope::delete_vectors`] is
    /// bumped once — the rule `bump_vector_generation` states, kept here so
    /// that no caller, inside the crate or out, has to.
    ///
    /// Every stamp is minted inside the closure, under the writer, so
    /// ADR-148's contiguity holds for a scope exactly as it does for a bulk
    /// insert.
    ///
    /// A write that fails inside the scope poisons it: the transaction may
    /// hold a document without its index entries or its oplog entry, so
    /// every later write on the scope is refused, and the scope is aborted
    /// with a `Transaction` error even when the closure swallows the
    /// failure and returns `Ok`. A closure cannot commit a torn write by
    /// ignoring an error.
    ///
    /// What a closure must not do while it holds the writer. It must not
    /// call any other write on this engine — an insert, a replace outside
    /// the scope, a schema change, anything that reaches `begin_write` —
    /// because redb has one writer and this thread already holds it, so the
    /// call would wait for itself forever. And nothing slow or network-bound
    /// belongs inside: every other writer on the node waits behind the
    /// scope for as long as the closure runs, so the closure should hold
    /// its inputs ready and do nothing but write them. Reads are fine.
    ///
    /// `holder` names what the scope is for, since every writer on the node
    /// waits behind it and the hold histogram has to say which of them did
    /// (ADR-159).
    pub fn write_batch<T>(
        &self,
        holder: WriterHolder,
        f: impl FnOnce(&mut WriteScope<'_>) -> Result<T>,
    ) -> Result<T> {
        let txn = self.begin_write(holder)?;
        let mut scope = WriteScope {
            engine: self,
            txn,
            entries: Vec::new(),
            wrote: false,
            vector_generations: Vec::new(),
            poisoned: None,
        };
        let value = match f(&mut scope) {
            Ok(value) => value,
            Err(e) => {
                scope.txn.abort()?;
                return Err(e);
            }
        };
        let WriteScope { txn, entries, wrote, vector_generations, poisoned, .. } = scope;
        if let Some(first) = poisoned {
            // The closure answered `Ok` over a failed write. What the
            // transaction holds is not a state that ever existed, and it
            // is not committed on anyone's say-so.
            txn.abort()?;
            return Err(WriteScope::poisoned_error(&first));
        }
        if !wrote {
            txn.abort()?;
            return Ok(value);
        }
        txn.commit()?;
        // Committed, so a build that reads the new generation now reads the
        // new chunks with it; before the commit it would not have.
        for shadow in vector_generations {
            self.bump_vector_generation(shadow);
        }
        self.publish(entries);
        Ok(value)
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
    ///
    /// One entry, one transaction. A sync batch does not come through here:
    /// it applies a whole run of entries into one transaction with
    /// [`Self::apply_remote_in_txn`] and commits once (ADR-119). This is the
    /// single-entry form for snapshot restore and for tests that manufacture
    /// a replicated write by hand.
    pub fn apply_remote(&self, coll: &CollectionMeta, entry: &OplogEntry) -> Result<bool> {
        if entry.doc_id.is_none() {
            // Collection-level operations carry no document to merge, and
            // nothing to write: decided before the writer is taken.
            self.witness(&entry.stamp);
            return Ok(false);
        }
        let txn = self.begin_write(WriterHolder::Replication)?;
        let RemoteApplied::Applied { id, violations } =
            self.apply_remote_in_txn(&txn, coll, entry, Position::Raise)?
        else {
            // Nothing was written, so nothing is committed — a superseded
            // entry must not cost an fsync.
            txn.abort()?;
            return Ok(false);
        };
        txn.commit()?;
        let published =
            self.report_remote_write(WriterHolder::Replication, coll, entry, &id, &violations)?;
        self.publish(published);
        Ok(true)
    }

    /// The body of [`Self::apply_remote`], inside a transaction the caller
    /// owns.
    ///
    /// Writes the document, its index entries and its oplog entry into `txn`
    /// when the entry wins, and writes **nothing** when it loses — an equal
    /// stamp means this exact write is already here, and peers resend
    /// overlapping ranges by design — so the caller can keep applying further
    /// entries into the same transaction either way. The local clock is
    /// advanced past the stamp in both cases, so a subsequent local write is
    /// ordered after it.
    ///
    /// What cannot happen here is anything that needs the write to be
    /// durable: publishing to change streams, and recording a unique
    /// violation, which mints a local entry in a transaction of its own.
    /// Those come back in the result for [`Self::report_remote_write`] to do
    /// once the caller has committed. The split exists so that a sync batch
    /// can be one commit rather than one per entry (ADR-119); on a
    /// three-member cluster the per-entry form replicated at 8–13 documents
    /// a second under `durable`, one fsync each.
    ///
    /// `position` says whether the appended entry moves this node's version
    /// vectors: it does for a window served from this node's own position,
    /// and must not for a snapshot document, whose coverage is granted once
    /// when the snapshot completes (ADR-152; see [`Position`]).
    pub(crate) fn apply_remote_in_txn(
        &self,
        txn: &WriteTxn<'_>,
        coll: &CollectionMeta,
        entry: &OplogEntry,
        position: Position,
    ) -> Result<RemoteApplied> {
        let Some(id) = entry.doc_id.clone() else {
            // Collection-level operations carry no document to merge.
            self.witness(&entry.stamp);
            return Ok(RemoteApplied::Superseded);
        };

        let key = doc_key(&id)?;
        let incoming = match &entry.body {
            Some(body) => DocRecord::live(entry.stamp, body.clone()),
            None => DocRecord::tombstone(entry.stamp),
        };

        let violations = {
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
                // Superseded, but in a window contiguous from this node's
                // position: an entry this node holds as state (ADR-160) has
                // now arrived as history, so its mark goes and the vectors
                // cover it (ADR-169). Nothing is appended and nothing
                // published; the entry already has both.
                if position == Position::InWindow {
                    drop(docs);
                    crate::engine::release_held_in_position(txn, &entry.stamp)?;
                }
                self.witness(&entry.stamp);
                return Ok(RemoteApplied::Superseded);
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
            //
            // Both calls read the index definitions through `txn`, not from
            // `coll`, which is why a run of entries can share a transaction: a
            // multikey flag an earlier entry set is seen by the next one even
            // though `coll` was resolved from the last committed state.
            let next = winner.document()?;
            let (violations, newly_multikey) =
                index::maintain_remote(self, txn, coll, previous.as_ref(), next.as_ref(), &key)?;
            // A replicated array write makes this node's index multikey exactly
            // as a local one would — the planner here answers queries over the
            // merged data, wherever it was written.
            index::mark_multikey(txn, &coll.db, &coll.name, &newly_multikey)?;
            violations
        };

        crate::engine::append_oplog_at(txn, entry, position)?;

        // Advance the local clock past what we just accepted, so a subsequent
        // local write is ordered after it.
        self.witness(&entry.stamp);

        Ok(RemoteApplied::Applied { id, violations })
    }

    /// The part of a replicated write that must follow its commit: count and
    /// record its unique violations, return what to publish, in order —
    /// the entry, then one `UniqueViolation` entry per constraint it broke —
    /// and, for a write into a shadow collection, move the vector generation.
    ///
    /// After the commit and not before, because recording a violation mints a
    /// local entry in its own transaction (see [`Self::log_unique_violation`]
    /// for why it is separate), and redb has one writer. The caller publishes
    /// the result, which keeps the rule that nothing reaches a subscriber
    /// before it is durable.
    ///
    /// The generation bump lives here for the same reason. A chunk that
    /// arrives by replication is a vector write exactly as `put_vectors` is,
    /// and the index cache reads the generation to notice one; on a member
    /// that does not own the collection's embedding every chunk arrives this
    /// way, so without the bump a graph built there was served as fresh
    /// indefinitely. It follows the commit as `put_vectors` bumps after its
    /// own writes commit: a bump inside the transaction opens a window in
    /// which a build reads the new generation from the counter but the data
    /// from a read transaction that predates the commit, and that graph
    /// would then be served as fresh at a generation it does not describe.
    /// Only an applied entry reaches here, so a superseded re-delivery does
    /// not bump; a replicated tombstone (body `None`) does, since removing a
    /// chunk changes the answer as much as adding one — the same rule as
    /// `delete_vectors`. The bump is a counter, not a write: it mints no stamp
    /// and nothing under ADR-148 applies to it.
    pub(crate) fn report_remote_write(
        &self,
        holder: WriterHolder,
        coll: &CollectionMeta,
        entry: &OplogEntry,
        id: &DocId,
        violations: &[index::UniqueViolation],
    ) -> Result<Vec<OplogEntry>> {
        // Ahead of the violation work, which can fail: a report that errors
        // out is still a write that landed, and the index must hear of it.
        if kimmy_core::vector_meta::is_shadow(&coll.name) {
            self.bump_vector_generation(coll.id);
        }
        let mut published = Vec::with_capacity(1 + violations.len());
        published.push(entry.clone());
        for violation in violations {
            self.count_unique_violation();
            warn!(
                index = %violation.index,
                holders = violation.holders.len(),
                collection = %coll.name,
                "a merged write broke a unique constraint"
            );
            published.push(self.log_unique_violation(holder, coll, id, violation)?);
        }
        Ok(published)
    }

    /// The part of a replicated unique index's build that must follow its
    /// commit: count and record each key the existing documents already
    /// shared, and publish the `UniqueViolation` entries minted for them.
    ///
    /// The backfill equivalent of [`Self::report_remote_write`], and the
    /// entries it mints are read by the same route: `live_unique_violations`
    /// groups a record by index and id set and re-checks its members against
    /// the documents as they are now, so a record here names every holder of
    /// the key, exactly as a merged write's does. What that route keeps as
    /// recorded is `merged`, the document whose arrival revealed the
    /// collision. A backfill has no arrival; the holder the scan met last is
    /// named, because it is the one whose presence turned a key with one
    /// holder into a collision — the same role the merged write plays.
    ///
    /// The count and the log are per key, as for a merged write, so a build
    /// over three documents sharing one value is one violation, not three.
    ///
    /// The shape is `commit_run`'s in `sync.rs`, for the hole it documents:
    /// the index is already committed when this runs, so a re-delivery of the
    /// create meets the same-definition short-circuit and never reaches the
    /// backfill again. A failure recording violation *k* that returned at
    /// once would discard the entries already minted for 1..k-1 unpublished
    /// and never record k+1..n — collisions lost for good, since nothing
    /// will find them a second time. So every violation is reported
    /// regardless, everything that was minted is published, and the first
    /// error is returned afterwards. The failure is not injectable from a
    /// test — recording fails only when the store itself does — which is
    /// why this says so rather than proving it.
    pub(crate) fn report_index_backfill_violations(
        &self,
        coll: &CollectionMeta,
        violations: &[index::UniqueViolation],
    ) -> Result<()> {
        let mut published = Vec::with_capacity(violations.len());
        let mut failed = None;
        for violation in violations {
            let Some(last) = violation.holders.last() else { continue };
            let revealed_by = match self.document_at_key(coll, last) {
                Ok(Some(id)) => id,
                Ok(None) => continue,
                Err(e) => {
                    failed.get_or_insert(e);
                    continue;
                }
            };
            self.count_unique_violation();
            warn!(
                index = %violation.index,
                holders = violation.holders.len(),
                collection = %coll.name,
                "a replicated unique index was built over documents that already share a key"
            );
            match self.log_unique_violation(WriterHolder::IndexBuild, coll, &revealed_by, violation)
            {
                Ok(entry) => published.push(entry),
                Err(e) => {
                    failed.get_or_insert(e);
                }
            }
        }
        self.publish(published);
        match failed {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// What [`Engine::apply_remote_in_txn`] decided, and what it left for after
/// the commit.
#[derive(Debug)]
pub(crate) enum RemoteApplied {
    /// The entry won its conflict and was written into the transaction.
    Applied {
        /// The document it wrote — carried here so the post-commit report
        /// does not have to re-derive from the entry what the write already
        /// knew.
        id: DocId,
        /// Unique constraints the merged write broke. Neither counted nor
        /// recorded yet: [`Engine::report_remote_write`] does both once the
        /// write is durable.
        violations: Vec<index::UniqueViolation>,
    },
    /// The entry lost, or named no document. The transaction is exactly as
    /// it was.
    Superseded,
}

/// A group's identity for deduplication: its ids, order-free.
fn id_set(ids: &[DocId]) -> Vec<String> {
    let mut key: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    key.sort();
    key
}

impl Engine {
    /// Unique violations still standing on a collection (ADR-087).
    ///
    /// Every `UniqueViolation` entry in the retained oplog for this
    /// collection, re-evaluated against the documents **as they are now**:
    /// a named document that is gone, or whose current keys under the index
    /// meet none of the others', has resolved its part of the collision and
    /// is left out of the group; a group with fewer than two members left is
    /// not reported; an index that no longer exists (or is no longer unique)
    /// has no constraint left to break. Deduplicated by index and id set —
    /// once on the recorded set, because a collision is recorded once per
    /// node that merged it and a resend must not read as two, and again on
    /// the surviving set, because two records can shrink to the same group.
    /// Costs one pass over the retained oplog and one read plus one key
    /// computation per named document, which retention bounds; a route, not
    /// a hot path.
    pub fn live_unique_violations(
        &self,
        coll: &CollectionMeta,
    ) -> Result<Vec<kimmy_core::UniqueViolationDetail>> {
        const PAGE: usize = 1024;
        let mut out = Vec::new();
        let mut seen: std::collections::BTreeSet<(String, Vec<String>)> = Default::default();
        let mut reported: std::collections::BTreeSet<(String, Vec<String>)> = Default::default();
        let mut from = kimmy_core::Hlc::ZERO;
        let mut last_seen: Option<Stamp> = None;
        loop {
            let page = self.read_oplog_from(from, PAGE)?;
            let Some(last) = page.last() else { break };
            let last_stamp = last.stamp;
            for entry in &page {
                // `read_oplog_from` is inclusive at `from`, so the first
                // entries of a page can repeat the previous page's tail.
                if last_seen.is_some_and(|s| entry.stamp <= s) {
                    continue;
                }
                if entry.kind != OpKind::UniqueViolation || entry.collection != coll.id {
                    continue;
                }
                let Some(body) = &entry.body else { continue };
                let detail: kimmy_core::UniqueViolationDetail = bson::deserialize_from_slice(body)?;
                if detail.ids.is_empty() {
                    continue;
                }
                if !seen.insert((detail.index.clone(), id_set(&detail.ids))) {
                    continue;
                }
                if let Some(standing) = self.standing_members(coll, &detail)?
                    && reported.insert((standing.index.clone(), id_set(&standing.ids)))
                {
                    out.push(standing);
                }
            }
            if page.len() < PAGE {
                break;
            }
            from = last_stamp.hlc;
            last_seen = Some(last_stamp);
        }
        Ok(out)
    }

    /// The members of a recorded collision that still collide.
    ///
    /// The record names the documents that shared a key when the merge
    /// happened; this asks whether they still do. Each named document that
    /// still exists has its keys recomputed under the index as it is defined
    /// now — the same function the write path uses, over a document this
    /// pass had to read anyway to know it exists — and a member stays only if
    /// some other member holds one of its keys. Deleting a member and
    /// rewriting its value are one case here: a document with no key in
    /// common with the rest has left the group. `merged` is kept as recorded,
    /// because it names the arrival that revealed the collision, which no
    /// later write changes.
    ///
    /// `None` when fewer than two members remain, or when the index the record
    /// names is gone or no longer unique.
    fn standing_members(
        &self,
        coll: &CollectionMeta,
        detail: &kimmy_core::UniqueViolationDetail,
    ) -> Result<Option<kimmy_core::UniqueViolationDetail>> {
        let Some(index) = coll.indexes.iter().find(|i| i.unique && i.name == detail.index) else {
            return Ok(None);
        };
        let mut keyed: Vec<(&DocId, Vec<Vec<u8>>)> = Vec::with_capacity(detail.ids.len());
        for id in &detail.ids {
            if let Some(doc) = self.get(coll, id)? {
                // A document the index cannot key holds no key, so it can
                // share none: it has left the group, whatever it shared before.
                let keys = match index::document_keys(index, &doc)? {
                    index::DocumentKeys::Keyed { keys, .. } => keys,
                    index::DocumentKeys::Unkeyed { .. } => Vec::new(),
                };
                keyed.push((id, keys));
            }
        }
        let ids: Vec<DocId> = keyed
            .iter()
            .enumerate()
            .filter(|(i, (_, keys))| {
                keyed
                    .iter()
                    .enumerate()
                    .any(|(j, (_, theirs))| j != *i && keys.iter().any(|key| theirs.contains(key)))
            })
            .map(|(_, (id, _))| (*id).clone())
            .collect();
        if ids.len() < 2 {
            return Ok(None);
        }
        Ok(Some(kimmy_core::UniqueViolationDetail::new(
            detail.index.clone(),
            detail.merged.clone(),
            ids,
        )))
    }

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
    /// because the nodes then never agree. It is attributed to whatever
    /// revealed the collision, not to a holder of its own: the report is part
    /// of that work's cost, and a label nobody could act on would be one more
    /// row on the page (ADR-159).
    fn log_unique_violation(
        &self,
        holder: WriterHolder,
        coll: &CollectionMeta,
        merged: &DocId,
        violation: &index::UniqueViolation,
    ) -> Result<OplogEntry> {
        let mut ids = Vec::with_capacity(violation.holders.len());
        for key in &violation.holders {
            // The holder list is encoded document keys, which do not decode
            // back to ids; read each document to recover its `_id`.
            match self.document_at_key(coll, key)? {
                Some(id) => ids.push(id),
                None => continue,
            }
        }

        let detail =
            kimmy_core::UniqueViolationDetail::new(violation.index.clone(), merged.clone(), ids);

        let txn = self.begin_write(holder)?;
        let entry = OplogEntry {
            // Under the writer, as every stamp is (ADR-148).
            stamp: self.next_stamp(),
            kind: OpKind::UniqueViolation,
            collection: coll.id,
            doc_id: None,
            body: Some(bson::serialize_to_vec(&detail)?),
        };
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

/// The document as it will be stored: `_id` first, carrying `id`, and every
/// other field in the order it arrived.
///
/// BSON keeps field order, and since ADR-120 so does everything between the
/// client and this table, so where `_id` lands is visible. MongoDB stores it
/// first whatever the client wrote, and both write paths here do the same.
/// Before this shared helper the insert path put a generated `_id` first but
/// left a client-supplied one where it was, and `replace_if` appended `_id`
/// to a body that lacked it — so a `PUT` of `{"zeta": 1, "alpha": 2}` read
/// back with `_id` last while the same body inserted read back with it first.
/// A body whose `_id` is already first is patched in place rather than
/// rebuilt; `Document::insert` on a present key keeps its position.
fn with_id_first(mut doc: Document, id: Bson) -> Document {
    if doc.keys().next().is_some_and(|key| key == ID_FIELD) {
        doc.insert(ID_FIELD, id);
        return doc;
    }
    let mut out = Document::new();
    out.insert(ID_FIELD, id);
    out.extend(doc.into_iter().filter(|(key, _)| key != ID_FIELD));
    out
}

/// The storage key for a document id.
pub(crate) fn doc_key(id: &DocId) -> Result<Vec<u8>> {
    Ok(keyenc::encode(&id.to_bson())?)
}

/// Pull the `_id` out of a stored document.
pub(crate) fn extract_id(doc: &Document) -> Result<DocId> {
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

    /// The cost of a write is the number of times it reaches the disk, and
    /// redb's single writer makes each of those a queue position for everybody
    /// else. M11 task 1 found the daemon spending two commits on an insert
    /// where the engine spent one — invisible to every latency measurement,
    /// because the second commit was somebody else's and only showed up as the
    /// *next* write waiting. These pin the engine's half of that.
    #[test]
    fn one_insert_is_one_commit() {
        let (engine, coll, _dir) = engine();

        let before = engine.commits();
        engine.insert(&coll, doc! {"n": 1}).unwrap();
        assert_eq!(engine.commits() - before, 1, "an insert must reach the disk exactly once");
    }

    #[test]
    fn a_batch_is_one_commit_however_many_documents_it_holds() {
        let (engine, coll, _dir) = engine();

        let before = engine.commits();
        let batch: Vec<_> = (0..100).map(|n| doc! {"n": n}).collect();
        engine.insert_many(&coll, batch).unwrap();
        assert_eq!(
            engine.commits() - before,
            1,
            "batching exists so that 100 documents cost one fsync, not 100"
        );
    }

    /// ADR-149's promise, in the shape of the ADR-119 test: a scope is one
    /// commit and one fsync however many writes it holds, and what it wrote
    /// reaches the change feed only after that commit, in the order written.
    /// A loop of replaces and deletes over the public single-call methods is
    /// one commit *each*, which is the defect this exists to end.
    #[test]
    fn a_scoped_write_is_one_commit_however_many_writes_it_holds() {
        let (engine, coll, _dir) = engine();
        // Two to replace over, two to delete; the other three are upserts.
        for n in 0..4 {
            engine.insert(&coll, doc! { "_id": n, "v": "before" }).unwrap();
        }
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let fsyncs = engine.fsyncs();
        let written: Vec<(DocId, OpKind)> = engine
            .write_batch(WriterHolder::Bulk, |scope| {
                let mut written = Vec::new();
                for n in 0..2 {
                    let id = DocId::Int64(n);
                    let outcome = scope.replace(&coll, &id, doc! { "v": "after" }, false)?;
                    assert!(outcome.matched && outcome.modified && !outcome.upserted);
                    written.push((id, OpKind::Replace));
                }
                for n in 10..13 {
                    let id = DocId::Int64(n);
                    let outcome = scope.replace(&coll, &id, doc! { "v": "after" }, true)?;
                    assert!(!outcome.matched && outcome.upserted);
                    written.push((id, OpKind::Insert));
                }
                for n in 2..4 {
                    let id = DocId::Int64(n);
                    assert!(scope.delete(&coll, &id)?);
                    written.push((id, OpKind::Delete));
                }
                // Nothing has reached the disk or the feed while the scope
                // is open: a write inside it is not a commit of its own.
                assert_eq!(
                    engine.commits(),
                    commits,
                    "a write inside a scope committed on its own"
                );
                assert!(
                    rx.try_recv().is_err(),
                    "an entry was published before the scope committed"
                );
                Ok(written)
            })
            .unwrap();

        assert_eq!(
            engine.commits() - commits,
            1,
            "a scope exists so that seven writes cost one commit, not seven"
        );
        assert_eq!(engine.fsyncs() - fsyncs, 1, "and one fsync");

        let mut published = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            published.push((entry.doc_id.clone().unwrap(), entry.kind));
        }
        assert_eq!(published, written, "every entry, once, in the order written");

        for n in [0, 1, 10, 11, 12] {
            let found = engine.get(&coll, &DocId::Int64(n)).unwrap().expect("written");
            assert_eq!(found.get_str("v").unwrap(), "after");
        }
        for n in [2, 3] {
            assert!(engine.get(&coll, &DocId::Int64(n)).unwrap().is_none(), "{n} was deleted");
        }
    }

    /// The other half of one commit is none: a scope whose closure fails
    /// leaves no document, no entry and no event behind, however far it got.
    #[test]
    fn a_scoped_write_that_fails_leaves_nothing_behind() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": 1, "v": "before" }).unwrap();
        engine.insert(&coll, doc! { "_id": 2, "v": "before" }).unwrap();
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let tail = engine.read_arrival_from(0, 100).unwrap().len();
        let err = engine
            .write_batch(WriterHolder::Bulk, |scope| {
                scope.replace(&coll, &DocId::Int64(1), doc! { "v": "after" }, false)?;
                scope.delete(&coll, &DocId::Int64(2))?;
                scope.replace(&coll, &DocId::Int64(3), doc! { "v": "after" }, true)?;
                Err::<(), _>(StorageError::Transaction("the caller changed its mind".into()))
            })
            .unwrap_err();
        assert!(
            matches!(err, StorageError::Transaction(_)),
            "the caller's error comes back: {err}"
        );

        assert_eq!(engine.commits(), commits, "a failed scope must not reach the disk");
        assert!(rx.try_recv().is_err(), "a failed scope must not publish");
        assert_eq!(
            engine.read_arrival_from(0, 100).unwrap().len(),
            tail,
            "the oplog must not move"
        );
        for n in [1, 2] {
            let found = engine.get(&coll, &DocId::Int64(n)).unwrap().expect("still here");
            assert_eq!(found.get_str("v").unwrap(), "before");
        }
        assert!(engine.get(&coll, &DocId::Int64(3)).unwrap().is_none(), "the upsert rolled back");
    }

    /// A scope that wrote nothing is not a commit — the rule `insert_many`
    /// states for an empty batch — and a write that matched nothing is a
    /// write that wrote nothing.
    #[test]
    fn an_empty_scope_does_not_commit() {
        let (engine, coll, _dir) = engine();
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let outcome = engine
            .write_batch(WriterHolder::Bulk, |scope| {
                let outcome = scope.replace(&coll, &DocId::Int64(7), doc! { "v": 1 }, false)?;
                assert!(!scope.delete(&coll, &DocId::Int64(8))?, "nothing to delete");
                Ok(outcome)
            })
            .unwrap();
        assert!(!outcome.matched && outcome.stamp.is_none(), "an unmatched replace wrote nothing");

        assert_eq!(engine.commits(), commits, "an empty scope must not cost an fsync");
        assert!(rx.try_recv().is_err(), "an empty scope has nothing to publish");
        assert!(engine.get(&coll, &DocId::Int64(7)).unwrap().is_none());
    }

    /// The closure decides what its scope returns, and the closure is
    /// outside this crate. A write that fails part way — the document
    /// written, the unique probe refused before the index and the oplog
    /// entry — must not reach the disk because the closure shrugged and
    /// returned `Ok`: two live documents under one unique key, one of them
    /// unlogged and so never replicated, is a state that never existed.
    #[test]
    fn a_swallowed_error_inside_a_scope_commits_nothing() {
        let (engine, _dir) = indexed_engine();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        engine.insert(&coll, doc! { "_id": "a", "email": "same@x" }).unwrap();
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        // The closure shrugs at both answers and reports success; what each
        // write answered is checked afterwards, so that the assertion that
        // matters — nothing committed — is the one a missing poison fails.
        let mut answers = Vec::new();
        let err = engine
            .write_batch(WriterHolder::Bulk, |scope| {
                let b = scope.replace(
                    &coll,
                    &DocId::String("b".into()),
                    doc! { "email": "same@x" },
                    true,
                );
                let c = scope.replace(
                    &coll,
                    &DocId::String("c".into()),
                    doc! { "email": "other@x" },
                    true,
                );
                answers.push(b);
                answers.push(c);
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(err, StorageError::Transaction(_)), "the scope cannot commit: {err}");
        assert!(err.to_string().contains("unique"), "and says why: {err}");
        assert!(
            matches!(answers[0], Err(StorageError::Core(CoreError::UniqueViolation { .. }))),
            "the violation reached the closure: {:?}",
            answers[0]
        );
        assert!(
            matches!(answers[1], Err(StorageError::Transaction(_))),
            "a poisoned scope refuses every later write: {:?}",
            answers[1]
        );

        assert_eq!(engine.commits(), commits, "a poisoned scope must not reach the disk");
        assert!(rx.try_recv().is_err(), "a poisoned scope must not publish");
        assert!(engine.get(&coll, &DocId::String("b".into())).unwrap().is_none(), "b never landed");
        assert!(engine.get(&coll, &DocId::String("c".into())).unwrap().is_none(), "nor c");
        assert!(engine.get(&coll, &DocId::String("a".into())).unwrap().is_some(), "a is untouched");
    }

    #[test]
    fn a_refused_write_does_not_commit() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! {"_id": "same", "n": 1}).unwrap();

        let before = engine.commits();
        engine.insert(&coll, doc! {"_id": "same", "n": 2}).unwrap_err();
        assert_eq!(
            engine.commits() - before,
            0,
            "an aborted transaction never reached the disk and must not be counted as if it had"
        );
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
    fn a_bulk_failure_names_the_document_when_it_can() {
        // The position is the only thing a caller can act on when nothing was
        // written, so it has to survive into the message rather than living
        // only in a field somebody might not read.
        let (engine, coll, _dir) = engine();
        let err = engine
            .insert_many(&coll, vec![doc! { "_id": 1 }, doc! { "_id": 1 }])
            .expect_err("a duplicate must fail the batch");

        let shown = err.to_string();
        assert!(shown.contains("index 1"), "the position must be in the message: {shown}");
        assert!(shown.contains("duplicate"), "and so must the cause: {shown}");
        assert!(std::error::Error::source(&err).is_some(), "the cause stays reachable");

        // A failure of the transaction rather than of a document has no
        // position, and must not invent one.
        let plain =
            BulkInsertError { index: None, source: CoreError::DuplicateKey("x".into()).into() };
        assert!(!plain.to_string().contains("index"), "{}", plain);
    }

    #[test]
    fn insert_many_stores_every_document_and_returns_their_ids() {
        let (engine, coll, _dir) = engine();
        let ids = engine
            .insert_many(
                &coll,
                vec![doc! { "_id": 1, "v": "a" }, doc! { "_id": 2, "v": "b" }, doc! { "v": "c" }],
            )
            .unwrap();

        assert_eq!(ids.len(), 3, "one id per submitted document, in order");
        for (id, expected) in ids.iter().zip(["a", "b", "c"]) {
            let found = engine.get(&coll, id).unwrap().expect("document should exist");
            assert_eq!(found.get_str("v").unwrap(), expected);
        }
        // The third had no `_id`, so one was generated and stored.
        assert!(engine.get(&coll, &ids[2]).unwrap().unwrap().contains_key(ID_FIELD));
    }

    #[test]
    fn insert_many_of_nothing_writes_nothing_and_does_not_move_the_clock() {
        let (engine, coll, _dir) = engine();
        let before = engine.version_vector().unwrap();

        assert!(engine.insert_many(&coll, vec![]).unwrap().is_empty());

        // An empty batch that opened a transaction would append an oplog entry
        // for a write that never happened.
        assert_eq!(engine.version_vector().unwrap(), before);
    }

    #[test]
    fn insert_many_appends_one_oplog_entry_per_document_with_increasing_stamps() {
        let (engine, coll, _dir) = engine();
        engine
            .insert_many(&coll, vec![doc! { "_id": 1 }, doc! { "_id": 2 }, doc! { "_id": 3 }])
            .unwrap();

        let entries = engine.entries_for_peer(Hlc::ZERO, 100).unwrap().entries;
        let inserts: Vec<_> = entries.iter().filter(|e| e.kind == OpKind::Insert).collect();
        assert_eq!(inserts.len(), 3, "the batch is three documents and three log entries");
        for pair in inserts.windows(2) {
            assert!(
                pair[0].stamp.hlc < pair[1].stamp.hlc,
                "each document in a batch takes its own stamp, in submission order"
            );
        }
    }

    #[test]
    fn insert_many_rejects_the_whole_batch_when_a_document_collides_with_a_stored_id() {
        let (engine, coll, _dir) = engine();
        engine.insert(&coll, doc! { "_id": 2, "v": "original" }).unwrap();
        let before = engine.version_vector().unwrap();

        let err = engine
            .insert_many(
                &coll,
                vec![doc! { "_id": 1 }, doc! { "_id": 2, "v": "collides" }, doc! { "_id": 3 }],
            )
            .expect_err("a duplicate _id must fail the batch");

        assert_eq!(err.index, Some(1), "the caller is told which document to fix");
        assert!(matches!(err.source, StorageError::Core(CoreError::DuplicateKey(_))));

        // All-or-nothing: neither the document before the failure nor the one
        // after it may survive, and the stored document is untouched.
        assert!(engine.get(&coll, &DocId::Int64(1)).unwrap().is_none());
        assert!(engine.get(&coll, &DocId::Int64(3)).unwrap().is_none());
        assert_eq!(
            engine.get(&coll, &DocId::Int64(2)).unwrap().unwrap().get_str("v").unwrap(),
            "original"
        );
        assert_eq!(
            engine.version_vector().unwrap(),
            before,
            "an aborted batch leaves no oplog entry, so the version vector must not move"
        );
    }

    #[test]
    fn insert_many_rejects_a_batch_whose_documents_collide_with_each_other() {
        // The collision is *within* the transaction, against a write that has
        // not committed. It is caught because a redb read sees its own txn's
        // writes — the property the batch path depends on.
        let (engine, coll, _dir) = engine();

        let err = engine
            .insert_many(
                &coll,
                vec![doc! { "_id": 7, "v": "first" }, doc! { "_id": 7, "v": "second" }],
            )
            .expect_err("two documents with one _id must fail the batch");

        assert_eq!(err.index, Some(1), "the second occurrence is the offending one");
        assert!(matches!(err.source, StorageError::Core(CoreError::DuplicateKey(_))));
        assert!(engine.get(&coll, &DocId::Int64(7)).unwrap().is_none(), "nothing landed");
    }

    #[test]
    fn insert_many_rejects_a_batch_that_breaks_a_unique_index_within_itself() {
        // Same intra-transaction property, one layer up: the unique probe in
        // index maintenance must see the entry written moments earlier in this
        // very transaction.
        let (engine, _dir) = indexed_engine();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();

        let err = engine
            .insert_many(
                &coll,
                vec![
                    doc! { "_id": "a", "email": "same@x" },
                    doc! { "_id": "b", "email": "same@x" },
                ],
            )
            .expect_err("a unique collision inside the batch must fail it");

        assert_eq!(err.index, Some(1));
        assert!(matches!(err.source, StorageError::Core(CoreError::UniqueViolation { .. })));
        assert!(engine.get(&coll, &DocId::String("a".into())).unwrap().is_none());
        assert_eq!(engine.unique_violations(), 0, "a rejected local write is not a violation");
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
        assert_eq!((outcome.matched, outcome.modified, outcome.upserted), (true, true, false));
        assert!(outcome.stamp.is_some(), "a write reports the stamp it produced");

        let found = engine.get(&coll, &id).unwrap().unwrap();
        assert_eq!(found.get_i32("a").unwrap(), 9);
        assert!(!found.contains_key("b"), "replace is not a merge");
    }

    #[test]
    fn replace_without_upsert_does_not_create() {
        let (engine, coll, _dir) = engine();
        let id = DocId::Int64(404);
        let outcome = engine.replace(&coll, &id, doc! { "a": 1 }, false).unwrap();
        assert_eq!(
            outcome,
            WriteOutcome { matched: false, modified: false, upserted: false, stamp: None }
        );
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

    /// `_id` is stored first on every write path, so a replace and an insert
    /// of the same body read back the same. Before ADR-120 made field order
    /// visible, `replace_if` appended `_id` to a body that lacked it.
    #[test]
    fn every_write_path_stores_id_first() {
        let (engine, coll, _dir) = engine();
        let keys = |doc: &Document| doc.keys().cloned().collect::<Vec<_>>();

        let id = engine.insert(&coll, doc! { "zeta": 1, "alpha": 2 }).unwrap();
        assert_eq!(keys(&engine.get(&coll, &id).unwrap().unwrap()), ["_id", "zeta", "alpha"]);

        // A client-supplied `_id` moves to the front, as MongoDB moves it, and
        // its value is stored as written rather than through `DocId`.
        let id = engine.insert(&coll, doc! { "zeta": 1, "_id": 7_i32, "alpha": 2 }).unwrap();
        let found = engine.get(&coll, &id).unwrap().unwrap();
        assert_eq!(keys(&found), ["_id", "zeta", "alpha"]);
        assert_eq!(found.get(ID_FIELD), Some(&Bson::Int32(7)));

        engine.replace(&coll, &id, doc! { "zeta": 3, "alpha": 4 }, false).unwrap();
        assert_eq!(keys(&engine.get(&coll, &id).unwrap().unwrap()), ["_id", "zeta", "alpha"]);

        let id = DocId::Int64(8);
        engine.replace(&coll, &id, doc! { "zeta": 1, "alpha": 2 }, true).unwrap();
        assert_eq!(keys(&engine.get(&coll, &id).unwrap().unwrap()), ["_id", "zeta", "alpha"]);
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

    #[test]
    fn a_batch_publishes_one_event_per_document_and_an_aborted_one_publishes_none() {
        let (engine, coll, _dir) = engine();
        let mut rx = engine.subscribe();

        let ids = engine.insert_many(&coll, vec![doc! { "_id": 1 }, doc! { "_id": 2 }]).unwrap();
        for id in &ids {
            let event = rx.try_recv().expect("each document in a batch reaches subscribers");
            assert_eq!(event.kind, OpKind::Insert);
            assert_eq!(event.doc_id.as_ref(), Some(id));
        }
        assert!(rx.try_recv().is_err(), "and no more than one event per document");

        // A batch that never commits must be invisible to a subscriber, or a
        // stream would report a change that was rolled back.
        let _ = engine.insert_many(&coll, vec![doc! { "_id": 3 }, doc! { "_id": 1 }]);
        assert!(rx.try_recv().is_err(), "an aborted batch must publish nothing at all");
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

    #[test]
    fn a_standing_violation_is_re_evaluated_against_the_documents_as_they_are() {
        // ADR-087, amended: the record says who collided when the merge
        // happened; the report says who still does. A later state of a
        // member that keeps the value keeps the collision; one that changes
        // the value resolves it, exactly as a delete would.
        let (engine, _dir) = indexed_engine();
        engine.create_index("db", "c", vec![field("email")], true, None).unwrap();
        let coll = engine.get_collection("db", "c").unwrap();
        engine.insert(&coll, doc! { "_id": "local", "email": "clash@x" }).unwrap();
        let entry =
            remote_insert(&coll, "remote", doc! { "_id": "remote", "email": "clash@x" }, 9_000);
        engine.apply_remote(&coll, &entry).unwrap();

        let live = engine.live_unique_violations(&coll).unwrap();
        assert_eq!(live.len(), 1, "{live:?}");
        assert_eq!(live[0].ids.len(), 2);

        // The peer rewrote its document but kept the email: still colliding.
        let later = remote_insert(
            &coll,
            "remote",
            doc! { "_id": "remote", "email": "clash@x", "note": "still here" },
            9_500,
        );
        engine.apply_remote(&coll, &later).unwrap();
        let live = engine.live_unique_violations(&coll).unwrap();
        assert_eq!(live.len(), 1, "a rewrite that keeps the value keeps the collision");
        assert_eq!(live[0].ids.len(), 2);

        // Rewritten to a value of its own: the key is unique again, and the
        // record — still in the oplog, both documents still present — no
        // longer describes a standing violation.
        engine
            .replace(
                &coll,
                &DocId::String("remote".into()),
                doc! { "_id": "remote", "email": "remote@x" },
                false,
            )
            .unwrap();
        assert!(engine.get(&coll, &DocId::String("local".into())).unwrap().is_some());
        assert!(engine.get(&coll, &DocId::String("remote".into())).unwrap().is_some());
        assert!(
            engine.live_unique_violations(&coll).unwrap().is_empty(),
            "a rewrite of the colliding value resolves the violation"
        );
        // The metric counts detections, not standing violations: the peer's
        // second state was merged into an occupied key and detected again,
        // and neither detection is undone by the resolution. The two records
        // name the same ids, which is why the report showed one group.
        assert_eq!(engine.unique_violations(), 2, "the detection count is history, and stays");
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

    // -----------------------------------------------------------------------
    // Conditional writes by id (ADR-084)
    // -----------------------------------------------------------------------

    fn stale_of(err: StorageError) -> Option<Stamp> {
        match err {
            StorageError::Stale { current } => current,
            other => panic!("expected Stale, got {other:?}"),
        }
    }

    #[test]
    fn a_conditional_replace_needs_the_current_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("app", "docs").unwrap();
        let id = DocId::Int64(1);
        let (_, first) = engine.insert_stamped(&coll, doc! {"_id": 1i64, "n": 0}).unwrap();

        let out = engine.replace_if(&coll, &id, doc! {"n": 1}, false, Some(first)).unwrap();
        assert!(out.modified);
        let second = out.stamp.unwrap();
        assert_ne!(second, first);

        let mut rx = engine.subscribe();
        let _ = rx.try_recv();
        let before = engine.commits();
        let err = engine.replace_if(&coll, &id, doc! {"n": 2}, false, Some(first)).unwrap_err();
        assert_eq!(stale_of(err), Some(second), "the refusal names the current stamp");
        assert_eq!(engine.commits() - before, 0, "a refused replace commits nothing");
        assert!(rx.try_recv().is_err(), "a refused replace publishes nothing");
        assert_eq!(engine.get(&coll, &id).unwrap().unwrap().get_i32("n").unwrap(), 1);

        // Upsert does not rescue a stale condition: the caller said "at this
        // version", not "or create it".
        assert!(engine.delete(&coll, &id).unwrap());
        let err = engine.replace_if(&coll, &id, doc! {"n": 3}, true, Some(second)).unwrap_err();
        assert_eq!(stale_of(err), None);
        assert!(engine.get(&coll, &id).unwrap().is_none());
    }

    #[test]
    fn a_conditional_delete_needs_the_current_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let coll = engine.create_collection("app", "docs").unwrap();
        let id = DocId::Int64(1);
        let (_, first) = engine.insert_stamped(&coll, doc! {"_id": 1i64}).unwrap();
        let second = engine.replace(&coll, &id, doc! {"n": 1}, false).unwrap().stamp.unwrap();

        let before = engine.commits();
        let err = engine.delete_if(&coll, &id, Some(first)).unwrap_err();
        assert_eq!(stale_of(err), Some(second));
        assert_eq!(engine.commits() - before, 0);
        assert!(engine.get(&coll, &id).unwrap().is_some(), "a stale delete removes nothing");

        // The answer is the tombstone's version: newer than the one the
        // condition named, and what the by-id route reports as `stamp`.
        let tombstone = engine.delete_if(&coll, &id, Some(second)).unwrap().expect("deleted");
        assert!(tombstone > second, "a delete moves the stamp");
        assert!(engine.get(&coll, &id).unwrap().is_none());

        // Gone now: expecting any version of it is stale, while an
        // unconditional delete of a missing document is an ordinary `None`.
        assert_eq!(stale_of(engine.delete_if(&coll, &id, Some(second)).unwrap_err()), None);
        assert!(engine.delete_if(&coll, &id, None).unwrap().is_none());
    }
}
