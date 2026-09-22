//! Vector configuration lifecycle and shadow collections.
//!
//! Enabling vectors on a collection creates a companion collection named
//! `{collection}.__vectors`. It is an ordinary collection — same durability,
//! same oplog, same eventual replication — rather than a parallel storage
//! mechanism that would need its own correctness argument.
//!
//! The `__` segment prefix is reserved for system objects, so a user cannot
//! create a collection that shadows one.

use std::collections::HashMap;

use kimmy_core::{
    CollectionId, DocId, Error as CoreError, Hlc, ResumeToken, VectorConfig, VectorRecord,
    vector_meta,
};
use tracing::info;

use redb::{ReadableDatabase, ReadableTable};

use crate::docs::WriteScope;
use crate::engine::{WriteTxn, WriterHolder};
use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;

/// One document's chunk set, encoded for its shadow collection ahead of any
/// writer being taken.
///
/// A scope's contract (ADR-149) is to hold its inputs ready and do nothing
/// but write them, since every other writer on the node waits behind it; so
/// the BSON encoding of each chunk record happens here, before
/// [`crate::Engine::write_batch`] is opened, and [`WriteScope::put_vectors`]
/// takes the result. The chunk numbers are kept beside the documents so the
/// scope can decide which stored chunks are a stale tail without decoding
/// anything.
pub struct VectorWrite {
    source: DocId,
    chunks: Vec<u32>,
    docs: Vec<(DocId, bson::Document)>,
}

impl VectorWrite {
    /// Encode `records` — the complete new chunk set of `source` — for the
    /// shadow collection.
    pub fn encode(source: &DocId, records: &[VectorRecord]) -> Result<Self> {
        let docs = records
            .iter()
            .map(|record| {
                let doc = bson::serialize_to_document(record)
                    .map_err(|e| StorageError::Corrupt(format!("encoding vector record: {e}")))?;
                Ok((VectorRecord::id(source, record.chunk), doc))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { source: source.clone(), chunks: records.iter().map(|r| r.chunk).collect(), docs })
    }
}

/// Where a vector configuration comes from, which decides how the shadow
/// collection it needs is created if it is missing (ADR-178).
#[derive(Clone, Copy, Debug)]
pub(crate) enum Configured {
    /// A client's configuration on this member. The shadow is created at a
    /// stamp minted here and logged, before the configuration's own entry,
    /// so a peer applying the window creates it from that entry, at that
    /// stamp, like any collection.
    Locally,
    /// A peer's `ConfigureVectors` entry, stamped so. A shadow missing here is
    /// created **at that stamp, unlogged**, and not at all when this member
    /// holds a newer tombstone for it. Never at a stamp minted here: a stamp
    /// later than the shadow's drop outlived the drop, and the shadow it
    /// created replicated back to the members that had dropped it.
    FromEntry(kimmy_core::Stamp),
    /// A snapshot page's definition. The shadow is its own collection on the
    /// page, restored at its own stamp, and is not created here.
    FromSnapshot,
}

impl crate::Engine {
    /// Enable or replace auto-embedding for a collection.
    ///
    /// Creates the shadow collection if needed. Rejects an invalid
    /// configuration up front, so a typo surfaces here rather than on the first
    /// document write.
    pub fn configure_vectors(
        &self,
        db: &str,
        collection: &str,
        config: VectorConfig,
    ) -> Result<CollectionMeta> {
        self.configure_vectors_inner(db, collection, config, Configured::Locally, &|_| false)
    }

    /// `log = false` when applying a replicated configuration. See
    /// `create_index_inner` for why a replicated change must not mint an entry.
    pub(crate) fn configure_vectors_inner(
        &self,
        db: &str,
        collection: &str,
        config: VectorConfig,
        by: Configured,
        history: &dyn Fn(&CollectionMeta) -> bool,
    ) -> Result<CollectionMeta> {
        // **A loop, not a self-call.** Every retry passes the same arguments, so
        // a lost race is a `continue`. It was a self-call, which made an
        // unbounded retry a stack overflow — and a bound low enough to protect
        // the stack (sixteen) fired under ordinary concurrent DDL, because K
        // changes to one collection queue at the writer gate and the last loses
        // K−1 races with nothing wrong at all. A loop cannot overflow, so the
        // bound below is only there to stop a check that can never pass.
        for _ in 0..crate::Engine::MAX_DEFINITION_RETRIES {
            let log = matches!(by, Configured::Locally);
            config.validate().map_err(|e| StorageError::Core(CoreError::InvalidQuery(e)))?;

            // A shadow collection holds vectors, not documents; configuring
            // embeddings on one would be a recursive absurdity.
            if vector_meta::is_shadow(collection) {
                return Err(StorageError::Core(CoreError::InvalidName {
                    name: collection.to_string(),
                    reason: "vectors cannot be configured on a shadow collection",
                }));
            }

            let mut meta = self.get_collection(db, collection)?;
            let read = meta.clone();
            #[cfg(test)]
            crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::VectorConfiguration);
            // A configuration of a life of the collection that has since been
            // dropped and recreated here is history, and is not applied to the
            // life that stands. Judged on the definition read here, which the
            // writer's `definition_is` below holds unchanged, and before the shadow
            // is created, so history mints no shadow either.
            if history(&meta) {
                #[cfg(test)]
                crate::sync::race_hooks::absorbed(crate::sync::race_hooks::Race::VectorHistory);
                return Ok(meta);
            }

            // A dimension change is safe exactly when the server can rebuild the
            // vectors itself: the embedding worker treats every ConfigureVectors
            // entry as a reindex trigger and re-embeds from the documents, and
            // both search paths skip records whose width does not match, so the
            // old vectors are invisible while the backfill replaces them.
            //
            // For byo the server holds vectors it can never regenerate — the
            // client computed them — so a changed width there still requires an
            // explicit drop, or search would quietly serve from a shrinking
            // remnant of the old width while nothing replaces it.
            if let Some(existing) = &meta.vector
                && existing.dim != config.dim
                && !config.provider.embeds_server_side()
            {
                return Err(StorageError::Core(CoreError::InvalidQuery(format!(
                    "vector dimension cannot change from {} to {} in place for client-supplied \
                 vectors; drop the vector configuration first, which discards them",
                    existing.dim, config.dim
                ))));
            }

            // The shadow is created in the same transaction as the configuration
            // it serves, so neither is ever durable without the other: minted
            // before the configuration's own commit, one that turned out history,
            // or whose collection was dropped in between, left a shadow whose
            // creation replicated; minted after it, a crash between the two left
            // a configuration the embedding worker skips for want of a shadow.
            // What a dropped shadow of this name left behind is purged first,
            // outside the writer, as any creation's is.
            let shadow = vector_meta::shadow_name(collection);
            let shadow_missing = match self.get_collection(db, &shadow) {
                Ok(_) => false,
                Err(StorageError::Core(CoreError::CollectionNotFound { .. })) => true,
                Err(e) => return Err(e),
            };
            if shadow_missing {
                self.purge_dropped_collection(CollectionId::derive(db, &shadow))?;
                #[cfg(test)]
                crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::SystemCreate);
            }

            meta.vector = Some(config.clone());
            let txn = self.begin_write(WriterHolder::Ddl)?;
            #[cfg(test)]
            crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::VectorsWriting);
            // The shadow first, so a local creation is logged before the
            // configuration, the order a peer applying the window meets them in.
            let created = match by {
                Configured::Locally => Some(self.create_collection_in_txn(
                    &txn,
                    db,
                    &shadow,
                    true,
                    None,
                    &|_| false,
                    self.next_stamp(),
                )),
                Configured::FromEntry(stamp) => Some(self.create_collection_in_txn(
                    &txn,
                    db,
                    &shadow,
                    false,
                    Some(stamp.hlc),
                    &|dropped| stamp < dropped,
                    stamp,
                )),
                Configured::FromSnapshot => None,
            };
            let shadow_entry = match created {
                Some(Ok(crate::engine::InTxn::Created(_, entry))) => entry,
                Some(Ok(crate::engine::InTxn::Exists)) => {
                    // Made by a concurrent configuration of this collection since
                    // it was found missing: the shadow this one wanted.
                    #[cfg(test)]
                    if shadow_missing {
                        crate::sync::race_hooks::absorbed(
                            crate::sync::race_hooks::Race::SystemCreate,
                        );
                    }
                    None
                }
                // A peer's configuration older than the shadow's drop here: the
                // configuration still applies, and the drop that came after it
                // stands.
                Some(Ok(crate::engine::InTxn::History)) | None => None,
                Some(Err(e)) => {
                    txn.abort()?;
                    return Err(e);
                }
            };
            if !crate::Engine::definition_is(&txn, &read)? {
                txn.abort()?;
                #[cfg(test)]
                crate::sync::race_hooks::absorbed(
                    crate::sync::race_hooks::Race::VectorConfiguration,
                );
                continue;
            }
            crate::Engine::put_collection_meta(&txn, &meta)?;

            let logged = if log {
                let entry = crate::engine::ddl_entry(
                    self.next_stamp(),
                    kimmy_core::OpKind::ConfigureVectors,
                    meta.id,
                    &kimmy_core::VectorSet {
                        db: db.to_string(),
                        collection: collection.to_string(),
                        config: Some(config),
                    },
                )?;
                crate::engine::append_oplog(&txn, &entry)?;
                Some(entry)
            } else {
                None
            };
            txn.commit()?;
            #[cfg(test)]
            crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::VectorsCommitted);
            let published: Vec<_> = shadow_entry.into_iter().chain(logged).collect();
            if !published.is_empty() {
                self.publish(published);
            }

            info!(db, collection, shadow = %shadow, "configured auto-embedding");
            return Ok(meta);
        }
        // Unreachable under contention: see `MAX_DEFINITION_RETRIES`.
        Err(crate::Engine::retries_exhausted_error(db, collection))
    }

    /// Turn off auto-embedding, optionally discarding the vectors.
    ///
    /// Keeping them by default means re-enabling with the same settings does
    /// not force a full re-embed, which for a remote provider is a real cost.
    pub fn disable_vectors(&self, db: &str, collection: &str, drop_vectors: bool) -> Result<bool> {
        self.disable_vectors_inner(db, collection, drop_vectors, true, &|_| false)
    }

    pub(crate) fn disable_vectors_inner(
        &self,
        db: &str,
        collection: &str,
        drop_vectors: bool,
        log: bool,
        history: &dyn Fn(&CollectionMeta) -> bool,
    ) -> Result<bool> {
        // **A loop, not a self-call.** Every retry passes the same arguments, so
        // a lost race is a `continue`. It was a self-call, which made an
        // unbounded retry a stack overflow — and a bound low enough to protect
        // the stack (sixteen) fired under ordinary concurrent DDL, because K
        // changes to one collection queue at the writer gate and the last loses
        // K−1 races with nothing wrong at all. A loop cannot overflow, so the
        // bound below is only there to stop a check that can never pass.
        for _ in 0..crate::Engine::MAX_DEFINITION_RETRIES {
            let mut meta = self.get_collection(db, collection)?;
            let read = meta.clone();
            #[cfg(test)]
            crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::VectorRemoval);
            // As `configure_vectors_inner`: turning vectors off for a life that
            // has since been dropped and recreated here is history.
            if history(&meta) {
                return Ok(false);
            }
            if meta.vector.is_none() {
                return Ok(false);
            }

            meta.vector = None;
            let txn = self.begin_write(WriterHolder::Ddl)?;
            // The definition written back was read before the writer
            // (`Engine::definition_is`).
            if !crate::Engine::definition_is(&txn, &read)? {
                txn.abort()?;
                #[cfg(test)]
                crate::sync::race_hooks::absorbed(crate::sync::race_hooks::Race::VectorRemoval);
                continue;
            }
            crate::Engine::put_collection_meta(&txn, &meta)?;

            let logged = if log {
                let entry = crate::engine::ddl_entry(
                    self.next_stamp(),
                    kimmy_core::OpKind::ConfigureVectors,
                    meta.id,
                    &kimmy_core::VectorSet {
                        db: db.to_string(),
                        collection: collection.to_string(),
                        config: None,
                    },
                )?;
                crate::engine::append_oplog(&txn, &entry)?;
                Some(entry)
            } else {
                None
            };
            txn.commit()?;
            if let Some(entry) = logged {
                self.publish(vec![entry]);
            }

            // Deliberately after the config change and not replicated: discarding
            // the stored vectors is a local reclamation choice, and the shadow
            // collection is ordinary data that reconciles like any other.
            if drop_vectors {
                self.drop_collection(db, &vector_meta::shadow_name(collection))?;
            }
            info!(db, collection, drop_vectors, "disabled auto-embedding");
            return Ok(true);
        }
        // Unreachable under contention: see `MAX_DEFINITION_RETRIES`.
        Err(crate::Engine::retries_exhausted_error(db, collection))
    }

    /// The shadow collection holding a collection's vectors, if configured.
    pub fn vector_collection(&self, db: &str, collection: &str) -> Result<Option<CollectionMeta>> {
        let meta = self.get_collection(db, collection)?;
        if meta.vector.is_none() {
            return Ok(None);
        }
        Ok(Some(self.get_collection(db, &vector_meta::shadow_name(collection))?))
    }

    /// Collections in a database that have embedding enabled.
    ///
    /// The embedding worker uses this to know what to watch. Shadow
    /// collections are excluded, so it cannot try to embed its own output.
    pub fn vector_enabled_collections(&self, db: &str) -> Result<Vec<CollectionMeta>> {
        Ok(self
            .list_collections(db)?
            .into_iter()
            .filter(|c| c.vector.is_some() && !vector_meta::is_shadow(&c.name))
            .collect())
    }

    /// Find a collection by its internal id.
    ///
    /// Oplog entries carry the id, not the name, so any consumer of the log
    /// needs this to decide whether an entry is interesting. A catalogue walk
    /// in one read transaction, stopping at the first match: a caller that
    /// knows the name should use [`Self::get_collection`], which is a point
    /// read.
    pub fn collection_by_id(&self, id: kimmy_core::CollectionId) -> Result<Option<CollectionMeta>> {
        let txn = self.db().begin_read()?;
        let collections = txn.open_table(crate::tables::COLLECTIONS)?;
        for entry in collections.iter()? {
            let (_, value) = entry?;
            let meta: CollectionMeta = serde_json::from_slice(value.value())?;
            if meta.id == id {
                return Ok(Some(meta));
            }
        }
        Ok(None)
    }

    /// Every collection this node holds, **shadow collections included**, as
    /// its id and the `created` stamp of the incarnation standing under it.
    ///
    /// Deliberately not [`Self::all_collection_ids`], which hides a shadow
    /// whose parent is present (ADR-138) because the cross-member existence
    /// check must not read the owning member's lifecycle lag as divergence.
    /// The ids it hides are exactly the ones a vector index is keyed by, so
    /// reconciling the index cache against it would forget every live graph
    /// on the node. The two are one walk (`Engine::collections`) one argument
    /// apart: `PairedShadows::Included` here, `Hidden` there.
    ///
    /// The stamp rides along because the id alone cannot vouch for a graph or
    /// a snapshot: ids are derived from names, so a name dropped and created
    /// again is live at the same id, and whatever was built for its
    /// predecessor is keyed and stored under it too.
    pub fn live_collections(&self) -> Result<HashMap<CollectionId, Hlc>> {
        Ok(self
            .collections(crate::engine::PairedShadows::Included)?
            .into_iter()
            .map(|c| (c.id, c.created))
            .collect())
    }

    /// Persist a background consumer's position in the oplog.
    ///
    /// Keyed by name so several consumers can track independently. Stored
    /// rather than recomputed, because rebuilding it would mean re-reading the
    /// whole log on every restart.
    pub fn put_consumer_position(&self, consumer: &str, token: ResumeToken) -> Result<()> {
        let txn = self.begin_write(WriterHolder::Embedding)?;
        if let Err(e) = self.put_consumer_position_in_txn(&txn, consumer, token) {
            txn.abort()?;
            return Err(e);
        }
        txn.commit()?;
        Ok(())
    }

    /// The whole of [`Self::put_consumer_position`] except the transaction's
    /// lifecycle, so a scope can carry a position in the commit it makes
    /// anyway ([`WriteScope::put_consumer_position`]).
    ///
    /// A position is one row of the metadata table: it mints no stamp and
    /// appends no oplog entry, so ADR-148's rule about where a stamp is
    /// minted has nothing here to apply to, and there is nothing to publish
    /// after the caller commits.
    pub(crate) fn put_consumer_position_in_txn(
        &self,
        txn: &WriteTxn<'_>,
        consumer: &str,
        token: ResumeToken,
    ) -> Result<()> {
        let mut meta = txn.open_table(crate::tables::META)?;
        meta.insert(consumer_key(consumer).as_str(), token.encode().as_bytes())?;
        Ok(())
    }

    /// Where a background consumer left off, if it has ever recorded a position.
    pub fn consumer_position(&self, consumer: &str) -> Result<Option<ResumeToken>> {
        let txn = self.db().begin_read()?;
        let meta = txn.open_table(crate::tables::META)?;
        let Some(raw) = meta.get(consumer_key(consumer).as_str())? else {
            return Ok(None);
        };
        let text = std::str::from_utf8(raw.value())
            .map_err(|_| StorageError::Corrupt("consumer position is not utf-8".into()))?;
        Ok(Some(ResumeToken::decode(text)?))
    }

    /// Record which configuration a collection's vectors were last *fully*
    /// embedded under.
    ///
    /// Written by the embedding worker only after a completed backfill scan —
    /// the same position-after-work rule every consumer follows, at the scan
    /// level. A stored fingerprint that differs from the live configuration
    /// is how the worker knows a replayed `ConfigureVectors` entry demands a
    /// full re-embed rather than a staleness-checked no-op: the per-document
    /// HLC cannot see a configuration change, because configurations do not
    /// touch documents.
    pub fn put_vector_fingerprint(&self, collection: CollectionId, fingerprint: u64) -> Result<()> {
        let txn = self.begin_write(WriterHolder::Embedding)?;
        {
            let mut meta = txn.open_table(crate::tables::META)?;
            meta.insert(fingerprint_key(collection).as_str(), &fingerprint.to_be_bytes()[..])?;
        }
        txn.commit()?;
        Ok(())
    }

    /// The configuration fingerprint the last completed backfill recorded.
    pub fn vector_fingerprint(&self, collection: CollectionId) -> Result<Option<u64>> {
        let txn = self.db().begin_read()?;
        let meta = txn.open_table(crate::tables::META)?;
        let Some(raw) = meta.get(fingerprint_key(collection).as_str())? else {
            return Ok(None);
        };
        let bytes: [u8; 8] = raw
            .value()
            .try_into()
            .map_err(|_| StorageError::Corrupt("vector fingerprint is not 8 bytes".into()))?;
        Ok(Some(u64::from_be_bytes(bytes)))
    }

    // -----------------------------------------------------------------------
    // Vector records
    // -----------------------------------------------------------------------

    /// Replace every chunk belonging to one source document.
    ///
    /// Writes the new chunks and removes any left over from a longer previous
    /// version. Without that cleanup, shortening a document would leave its
    /// tail chunks searchable forever — matches pointing at text the document
    /// no longer contains.
    ///
    /// The whole replacement is one commit, in one [`Engine::write_batch`]
    /// scope (ADR-149): every chunk write and every tail delete goes into the
    /// same transaction, so a document of N chunks costs one fsync rather
    /// than N, and a failure part way through leaves the previous chunk set
    /// exactly as it was — never a document with some new chunks and some
    /// old. This is the single-document form of [`WriteScope::put_vectors`],
    /// which the embedding worker composes for a whole provider batch; the
    /// records are encoded here, before the writer is taken, and the scope
    /// does nothing but write them.
    ///
    /// The generation moves only after the commit, never inside the scope: a
    /// bump before the commit would let an index build read the new
    /// generation against the old chunks and be served as fresh for it. The
    /// scope records the shadow and `write_batch` bumps it once the commit
    /// has landed. And it moves only when something changed: an empty
    /// `records` over a document with no chunks writes nothing, commits
    /// nothing and leaves the generation where it was, the same rule
    /// [`Engine::delete_vectors`] follows for a document that has nothing to
    /// delete.
    pub fn put_vectors(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
        records: &[VectorRecord],
    ) -> Result<()> {
        let write = VectorWrite::encode(source, records)?;
        self.write_batch(WriterHolder::Embedding, |scope| scope.put_vectors(shadow, write))
    }

    /// Every vector belonging to one source document, in chunk order.
    pub fn get_vectors(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
    ) -> Result<Vec<VectorRecord>> {
        let mut out = Vec::new();
        self.for_each_chunk_doc_of(shadow, source, |_, doc| {
            out.push(decode_vector(doc)?);
            Ok(true)
        })?;
        // Keys order chunk numbers as text — `#10` before `#2` — so the read
        // sorts numerically rather than trusting the scan.
        out.sort_by_key(|r| r.chunk);
        Ok(out)
    }

    /// Visit every vector belonging to one source document.
    ///
    /// The bounded form of [`Engine::for_each_vector`]: it reads one
    /// document's run of chunks rather than the collection, so a search that
    /// already knows which documents it wants can fetch their chunks by key
    /// instead of scanning for them. A record that does not decode is skipped
    /// with a warning, for the reason `for_each_vector` gives.
    pub fn for_each_vector_of<F>(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(VectorRecord) -> Result<bool>,
    {
        self.for_each_chunk_doc_of(shadow, source, |id, doc| match decode_vector(doc) {
            Ok(record) => f(record),
            Err(e) => {
                tracing::warn!(
                    collection = %shadow.name,
                    document = %id,
                    error = %e,
                    "skipping a document in a vector collection that is not a vector record"
                );
                Ok(true)
            }
        })
    }

    /// The raw chunk documents of one source document, in key order.
    ///
    /// Chunk records are keyed `{source}#{chunk}`, and a string key encodes
    /// byte-for-byte in `_id` order, so one document's chunks are a contiguous
    /// run of the shadow collection: the scan starts just past the encoded
    /// `{source}#` prefix — which sorts before every key that extends it — and
    /// stops at the first key that does not carry it. Reading a document's
    /// chunks therefore costs its chunk count, not the collection's.
    ///
    /// The prefix alone is not proof of ownership: a string id may itself
    /// contain `#`, so `a#1#0` (document `a#1`, chunk 0) sits inside `a#`'s
    /// run. Each key is parsed and its source compared, and a key inside the
    /// run that belongs to another document is passed over rather than ending
    /// the scan.
    fn for_each_chunk_doc_of<F>(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(DocId, bson::Document) -> Result<bool>,
    {
        let wanted = source.to_string();
        let prefix = format!("{wanted}#");
        let after = kimmy_core::keyenc::encode(&bson::Bson::String(prefix.clone()))?;
        self.for_each_doc_after(shadow, Some(&after), |id, doc| {
            // Strings sort together, so the first non-string key — or the first
            // string outside the prefix — is the end of the run.
            let DocId::String(raw) = &id else {
                return Ok(false);
            };
            if !raw.starts_with(&prefix) {
                return Ok(false);
            }
            if !VectorRecord::parse_id(&id).is_some_and(|(s, _)| s == wanted) {
                return Ok(true);
            }
            f(id, doc)
        })
    }

    /// Remove every vector belonging to one source document.
    ///
    /// One commit however many chunks the document holds, for the reason
    /// [`Engine::put_vectors`] gives: the chunk numbers are read under the
    /// writer and every delete goes into the same scope, so a failure leaves
    /// the set whole. The generation moves after the commit, and only when a
    /// chunk was removed; a document with no chunks costs no commit.
    pub fn delete_vectors(&self, shadow: &CollectionMeta, source: &DocId) -> Result<usize> {
        self.write_batch(WriterHolder::Embedding, |scope| scope.delete_vectors(shadow, source))
    }

    /// Visit every stored vector. Used by search and by index rebuilds.
    ///
    /// A document that does not decode as a vector record is **skipped, not
    /// fatal**. A shadow collection is an ordinary collection, so a client with
    /// write access can put an arbitrary document in one; failing the scan
    /// would let a single malformed insert turn every subsequent search on that
    /// collection into a 500. Skipping costs one unusable record and keeps
    /// search available.
    ///
    /// It is logged at `warn` rather than passed over silently, because the
    /// other way to arrive here is genuine corruption.
    pub fn for_each_vector<F>(&self, shadow: &CollectionMeta, mut f: F) -> Result<()>
    where
        F: FnMut(VectorRecord) -> Result<bool>,
    {
        self.for_each_doc(shadow, |id, doc| match decode_vector(doc) {
            Ok(record) => f(record),
            Err(e) => {
                tracing::warn!(
                    collection = %shadow.name,
                    document = %id,
                    error = %e,
                    "skipping a document in a vector collection that is not a vector record"
                );
                Ok(true)
            }
        })
    }

    /// Whether a document's vectors are older than the document itself.
    ///
    /// Returns `true` when there are no vectors at all, since a document that
    /// has never been embedded also needs work.
    pub fn vectors_are_stale(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
        current: Hlc,
    ) -> Result<bool> {
        let records = self.get_vectors(shadow, source)?;
        if records.is_empty() {
            return Ok(true);
        }
        Ok(records.iter().any(|r| r.is_stale(current)))
    }

    fn vector_chunk_numbers(&self, shadow: &CollectionMeta, source: &DocId) -> Result<Vec<u32>> {
        let mut out = Vec::new();
        self.for_each_chunk_doc_of(shadow, source, |id, _| {
            if let Some((_, chunk)) = VectorRecord::parse_id(&id) {
                out.push(chunk);
            }
            Ok(true)
        })?;
        Ok(out)
    }
}

impl WriteScope<'_> {
    /// [`crate::Engine::put_vectors`], into this scope's transaction: the
    /// replace-all write of one source document's chunks, with no commit of
    /// its own.
    ///
    /// The one implementation of the chunk loop. `Engine::put_vectors` is
    /// this in a scope of its own; the embedding worker calls it once per
    /// document of a provider batch inside one scope, with its position
    /// beside them (ADR-125, as amended). The write was encoded before the
    /// scope was opened ([`VectorWrite::encode`]), so nothing here but the
    /// writes themselves runs under the writer. The existing chunk numbers
    /// are read inside the scope, under the writer, so no other write can
    /// add or remove a chunk between the read and the replace, and the tail
    /// removed is the tail that is there; the read sees the state the scope
    /// began from, which is the right state to decide a tail against.
    ///
    /// The generation bump belongs to the commit, not to the write: the
    /// shadow is recorded here, when a chunk was written or removed, and
    /// `write_batch` bumps it once after the commit has landed — never
    /// inside the scope, and never for a scope that aborted. An empty chunk
    /// set over a document that has none writes nothing and records
    /// nothing.
    pub fn put_vectors(&mut self, shadow: &CollectionMeta, write: VectorWrite) -> Result<()> {
        let VectorWrite { source, chunks, docs } = write;
        let existing = self.engine.vector_chunk_numbers(shadow, &source)?;

        let written = !docs.is_empty();
        for (id, doc) in docs {
            self.replace(shadow, &id, doc, true)?;
        }

        // Drop the tail of a previously longer document.
        let mut removed = 0;
        for chunk in existing {
            if !chunks.contains(&chunk) && self.delete(shadow, &VectorRecord::id(&source, chunk))? {
                removed += 1;
            }
        }
        if written || removed > 0 {
            self.touch_vector_generation(shadow.id);
        }
        Ok(())
    }

    /// [`crate::Engine::delete_vectors`], into this scope's transaction.
    ///
    /// Every chunk of the document, read under the writer, deleted in this
    /// scope; the shadow is recorded for the post-commit bump only when a
    /// chunk was removed. Returns how many were.
    pub fn delete_vectors(&mut self, shadow: &CollectionMeta, source: &DocId) -> Result<usize> {
        let mut removed = 0;
        for chunk in self.engine.vector_chunk_numbers(shadow, source)? {
            if self.delete(shadow, &VectorRecord::id(source, chunk))? {
                removed += 1;
            }
        }
        if removed > 0 {
            self.touch_vector_generation(shadow.id);
        }
        Ok(removed)
    }
}

/// META key holding one consumer's oplog position.
fn consumer_key(consumer: &str) -> String {
    format!("consumer_position:{consumer}")
}

fn fingerprint_key(collection: CollectionId) -> String {
    format!("vector_config:{}", collection.0)
}

fn decode_vector(doc: bson::Document) -> Result<VectorRecord> {
    bson::deserialize_from_document(doc)
        .map_err(|e| StorageError::Corrupt(format!("decoding vector record: {e}")))
}

#[cfg(test)]
mod tests {
    use kimmy_core::{ProviderConfig, vector_meta};

    use super::*;
    use crate::Engine;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        engine.create_collection("app", "docs").unwrap();
        (engine, dir)
    }

    #[test]
    fn a_recorded_fingerprint_reads_back() {
        // The idempotency mechanism for reindexing: a completed backfill
        // records the configuration it ran under, and a replayed
        // `ConfigureVectors` entry compares against it. If the read always
        // reported "nothing recorded", every replay would re-embed the whole
        // collection — silently, and expensively.
        let (engine, _dir) = engine();
        let coll = engine.get_collection("app", "docs").unwrap().id;

        assert_eq!(engine.vector_fingerprint(coll).unwrap(), None, "nothing recorded yet");

        engine.put_vector_fingerprint(coll, 0xDEAD_BEEF).unwrap();
        assert_eq!(engine.vector_fingerprint(coll).unwrap(), Some(0xDEAD_BEEF));

        // Replaced rather than accumulated, so a reconfigure leaves one answer.
        engine.put_vector_fingerprint(coll, 7).unwrap();
        assert_eq!(engine.vector_fingerprint(coll).unwrap(), Some(7));
    }

    #[test]
    fn fingerprints_do_not_collide_between_collections() {
        // They share one metadata table, so the key has to carry the
        // collection. A constant key would make configuring vectors on one
        // collection mark every other collection as already backfilled.
        let (engine, _dir) = engine();
        engine.create_collection("app", "other").unwrap();
        let a = engine.get_collection("app", "docs").unwrap().id;
        let b = engine.get_collection("app", "other").unwrap().id;

        engine.put_vector_fingerprint(a, 111).unwrap();

        assert_eq!(engine.vector_fingerprint(a).unwrap(), Some(111));
        assert_eq!(
            engine.vector_fingerprint(b).unwrap(),
            None,
            "one collection's fingerprint must not answer for another"
        );

        engine.put_vector_fingerprint(b, 222).unwrap();
        assert_eq!(engine.vector_fingerprint(a).unwrap(), Some(111), "and must not overwrite it");
        assert_eq!(engine.vector_fingerprint(b).unwrap(), Some(222));
    }

    fn config(dim: usize) -> VectorConfig {
        VectorConfig {
            fields: vec!["body".into()],
            provider: ProviderConfig::Byo {},
            dim,
            metric: Default::default(),
            document_prefix: None,
            query_prefix: None,
            chunk: Default::default(),
        }
    }

    #[test]
    fn a_filter_the_order_cannot_compare_does_not_crash_a_vector_change() {
        // The other pair of callers of the definition write-back. A NaN in any
        // partial filter on this collection made `configure_vectors` and
        // `disable_vectors` abort the node, exactly as a drop or a create did:
        // the check compared metadata with `==`, `NaN != NaN`, and the retry was
        // a self-call with nothing counting it.
        let (engine, _dir) = engine();
        engine
            .create_index_with(
                "app",
                "docs",
                vec![kimmy_core::IndexField::ascending("z")],
                false,
                Default::default(),
                Some("z_nan".into()),
                None,
                Some(bson::doc! { "k": f64::NAN }),
            )
            .unwrap();

        engine.configure_vectors("app", "docs", config(8)).unwrap();
        assert!(engine.vector_collection("app", "docs").unwrap().is_some());
        assert!(engine.disable_vectors("app", "docs", true).unwrap());
    }

    #[test]
    fn enabling_vectors_creates_the_shadow_collection() {
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();

        let shadow = engine.get_collection("app", "docs.__vectors").unwrap();
        assert_eq!(shadow.name, vector_meta::shadow_name("docs"));
        assert!(engine.vector_collection("app", "docs").unwrap().is_some());
    }

    #[test]
    fn an_invalid_config_is_rejected_before_anything_is_created() {
        let (engine, _dir) = engine();
        let mut bad = config(8);
        bad.fields.clear();

        assert!(engine.configure_vectors("app", "docs", bad).is_err());
        // Nothing should have been created by the failed attempt.
        assert!(engine.get_collection("app", "docs.__vectors").is_err());
        assert!(engine.get_collection("app", "docs").unwrap().vector.is_none());
    }

    #[test]
    fn the_dimension_cannot_change_in_place() {
        // Mixing widths in one index is meaningless, so this must be explicit.
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();

        let err = engine.configure_vectors("app", "docs", config(16)).unwrap_err();
        assert!(err.to_string().contains("dimension"), "unhelpful error: {err}");

        // Same width is a fine reconfiguration.
        assert!(engine.configure_vectors("app", "docs", config(8)).is_ok());
    }

    #[test]
    fn disabling_keeps_the_vectors_by_default() {
        // Re-enabling should not force a full re-embed, which for a remote
        // provider costs real money.
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();

        assert!(engine.disable_vectors("app", "docs", false).unwrap());
        assert!(engine.get_collection("app", "docs").unwrap().vector.is_none());
        assert!(engine.get_collection("app", "docs.__vectors").is_ok(), "vectors were discarded");

        // Disabling twice reports that there was nothing to do.
        assert!(!engine.disable_vectors("app", "docs", false).unwrap());
    }

    #[test]
    fn disabling_can_discard_the_vectors() {
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();
        engine.disable_vectors("app", "docs", true).unwrap();
        assert!(engine.get_collection("app", "docs.__vectors").is_err());
    }

    #[test]
    fn vectors_cannot_be_configured_on_a_shadow_collection() {
        let (engine, _dir) = engine();
        engine.configure_vectors("app", "docs", config(8)).unwrap();
        assert!(engine.configure_vectors("app", "docs.__vectors", config(8)).is_err());
    }

    #[test]
    fn shadow_collections_are_not_listed_as_embeddable() {
        // Otherwise the worker would try to embed its own output.
        let (engine, _dir) = engine();
        engine.create_collection("app", "other").unwrap();
        engine.configure_vectors("app", "docs", config(8)).unwrap();

        let enabled = engine.vector_enabled_collections("app").unwrap();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].name, "docs");
    }

    #[test]
    fn a_user_cannot_create_a_collection_that_shadows_one() {
        let (engine, _dir) = engine();
        // The reserved `__` prefix is what protects the namespace.
        assert!(engine.create_collection("app", "__vectors").is_err());
    }

    // -----------------------------------------------------------------------
    // Vector records
    // -----------------------------------------------------------------------

    fn record(chunk: u32, hlc_ms: u64, text: &str) -> VectorRecord {
        VectorRecord {
            source: DocId::Int64(1),
            chunk,
            source_hlc: Hlc::new(hlc_ms, 0),
            vector: vec![chunk as f32, 1.0],
            text: text.into(),
        }
    }

    /// An engine with vectors enabled, returning the shadow collection.
    fn with_vectors() -> (Engine, CollectionMeta, tempfile::TempDir) {
        let (engine, dir) = engine();
        engine.configure_vectors("app", "docs", config(2)).unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        (engine, shadow, dir)
    }

    /// A scope holding only a position commits (ADR-149's "nothing written"
    /// rule is measured by writes, not by entries): the embedding worker's
    /// held position with no batch to ride in goes through the same scope
    /// as one with a batch, and must still land.
    #[test]
    fn a_scope_holding_only_a_position_commits_once() {
        let (engine, _dir) = engine();
        let mut rx = engine.subscribe();
        let token = ResumeToken::new(Hlc::new(7, 1), engine.node_id());

        let commits = engine.commits();
        engine
            .write_batch(WriterHolder::Bulk, |scope| {
                scope.put_consumer_position("worker", token.clone())
            })
            .unwrap();
        assert_eq!(engine.commits() - commits, 1, "a position is a write, and a write commits");
        assert_eq!(engine.consumer_position("worker").unwrap(), Some(token), "and it is readable");
        assert!(rx.try_recv().is_err(), "a position is not an entry: nothing to publish");
    }

    /// The scope's vector write bumps the generation after the commit, as
    /// `put_vectors` does, and a scope that aborts bumps nothing: a build
    /// must never read a generation the data has not caught up with.
    #[test]
    fn a_scoped_vector_write_bumps_the_generation_only_once_it_has_committed() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let three: Vec<VectorRecord> = (0..3).map(|n| record(n, 10, "before")).collect();
        engine.put_vectors(&shadow, &source, &three).unwrap();
        let generation = engine.vector_generation(shadow.id);

        let commits = engine.commits();
        let one = VectorWrite::encode(&source, &[record(0, 20, "after")]).unwrap();
        engine
            .write_batch(WriterHolder::Bulk, |scope| {
                scope.put_vectors(&shadow, one)?;
                assert_eq!(
                    engine.vector_generation(shadow.id),
                    generation,
                    "not bumped inside the scope: the commit has not happened yet"
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(engine.commits() - commits, 1);
        assert_eq!(engine.vector_generation(shadow.id), generation + 1, "bumped once, after");
        let chunks: Vec<u32> =
            engine.get_vectors(&shadow, &source).unwrap().iter().map(|r| r.chunk).collect();
        assert_eq!(chunks, vec![0], "the tail was dropped in the same commit");

        let two = VectorWrite::encode(&source, &[record(0, 30, "x"), record(1, 30, "y")]).unwrap();
        let err = engine
            .write_batch(WriterHolder::Bulk, |scope| {
                scope.put_vectors(&shadow, two)?;
                Err::<(), _>(StorageError::Transaction("abandoned".into()))
            })
            .unwrap_err();
        assert!(matches!(err, StorageError::Transaction(_)));
        assert_eq!(engine.vector_generation(shadow.id), generation + 1, "an abort bumps nothing");
    }

    #[test]
    fn vectors_round_trip_in_chunk_order() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        // Written out of order to prove the read sorts them.
        let records = vec![record(1, 10, "second"), record(0, 10, "first")];
        engine.put_vectors(&shadow, &source, &records).unwrap();

        let read = engine.get_vectors(&shadow, &source).unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].chunk, 0);
        assert_eq!(read[0].text, "first");
        assert_eq!(read[1].text, "second");
        assert_eq!(read[0].vector, vec![0.0, 1.0]);
    }

    #[test]
    fn re_embedding_replaces_chunks_rather_than_accumulating() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);

        engine.put_vectors(&shadow, &source, &[record(0, 10, "old")]).unwrap();
        engine.put_vectors(&shadow, &source, &[record(0, 20, "new")]).unwrap();

        let read = engine.get_vectors(&shadow, &source).unwrap();
        assert_eq!(read.len(), 1, "the chunk should be replaced, not duplicated");
        assert_eq!(read[0].text, "new");
        assert_eq!(read[0].source_hlc, Hlc::new(20, 0));
    }

    #[test]
    fn shortening_a_document_removes_its_orphaned_tail_chunks() {
        // Otherwise the removed text stays searchable forever, and a hit points
        // at content the document no longer contains.
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);

        let long = vec![record(0, 10, "a"), record(1, 10, "b"), record(2, 10, "c")];
        engine.put_vectors(&shadow, &source, &long).unwrap();
        assert_eq!(engine.get_vectors(&shadow, &source).unwrap().len(), 3);

        engine.put_vectors(&shadow, &source, &[record(0, 20, "a")]).unwrap();
        let read = engine.get_vectors(&shadow, &source).unwrap();
        assert_eq!(read.len(), 1, "chunks 1 and 2 should be gone");
        assert_eq!(read[0].chunk, 0);
    }

    /// Every entry the change feed holds right now, as `(doc id, kind)`.
    fn drain(
        rx: &mut tokio::sync::broadcast::Receiver<std::sync::Arc<kimmy_core::OplogEntry>>,
    ) -> Vec<(DocId, kimmy_core::OpKind)> {
        let mut out = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            out.push((entry.doc_id.clone().unwrap(), entry.kind));
        }
        out
    }

    /// ADR-149's promise for the vector write, in the shape of the ADR-119
    /// test: a document's chunks are one commit however many it holds, and
    /// they reach the change feed only after that commit. The loop this
    /// replaces committed once per chunk and once per stale tail chunk, and
    /// published each as it went.
    #[test]
    fn a_documents_chunks_are_one_commit_however_many_it_holds() {
        use kimmy_core::OpKind;

        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let five: Vec<VectorRecord> = (0..5).map(|n| record(n, 10, "long")).collect();
        engine.put_vectors(&shadow, &source, &five).unwrap();
        assert_eq!(
            engine.commits() - commits,
            1,
            "five chunks over an empty shadow must be one commit, not five"
        );
        let published = drain(&mut rx);
        assert_eq!(published.len(), 5, "five entries, after the commit: {published:?}");
        assert!(
            published.iter().all(|(_, kind)| *kind == OpKind::Insert),
            "every chunk was new: {published:?}"
        );

        // Shorter now: three chunks replaced in place, two stale tails gone.
        let commits = engine.commits();
        let three: Vec<VectorRecord> = (0..3).map(|n| record(n, 20, "short")).collect();
        engine.put_vectors(&shadow, &source, &three).unwrap();
        assert_eq!(
            engine.commits() - commits,
            1,
            "three replaces and two tail deletes must be one commit, not five"
        );
        let published = drain(&mut rx);
        let replaces = published.iter().filter(|(_, k)| *k == OpKind::Replace).count();
        let deletes = published.iter().filter(|(_, k)| *k == OpKind::Delete).count();
        assert_eq!((replaces, deletes), (3, 2), "exactly 3 replaces and 2 deletes: {published:?}");
        assert_eq!(published.len(), 5, "and nothing else: {published:?}");

        let stored: Vec<u32> =
            engine.get_vectors(&shadow, &source).unwrap().iter().map(|r| r.chunk).collect();
        assert_eq!(stored, vec![0, 1, 2], "the stored chunk set is exactly 0..3");
    }

    /// The twin for the delete: one commit for the whole chunk set, the
    /// generation moved once, and a second delete of a document with no
    /// chunks costs nothing — no commit, no entry, no generation.
    #[test]
    fn deleting_a_documents_vectors_is_one_commit_however_many_it_holds() {
        use kimmy_core::OpKind;

        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let five: Vec<VectorRecord> = (0..5).map(|n| record(n, 10, "text")).collect();
        engine.put_vectors(&shadow, &source, &five).unwrap();
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let generation = engine.vector_generation(shadow.id);
        assert_eq!(engine.delete_vectors(&shadow, &source).unwrap(), 5);
        assert_eq!(engine.commits() - commits, 1, "five deletes must be one commit, not five");
        let published = drain(&mut rx);
        assert_eq!(published.len(), 5, "five delete entries: {published:?}");
        assert!(published.iter().all(|(_, k)| *k == OpKind::Delete), "{published:?}");
        assert_eq!(engine.vector_generation(shadow.id), generation + 1, "bumped once");
        assert!(engine.get_vectors(&shadow, &source).unwrap().is_empty());

        // Nothing left to delete: nothing happens.
        let commits = engine.commits();
        let generation = engine.vector_generation(shadow.id);
        assert_eq!(engine.delete_vectors(&shadow, &source).unwrap(), 0);
        assert_eq!(engine.commits(), commits, "a delete of nothing must not commit");
        assert!(rx.try_recv().is_err(), "a delete of nothing must not publish");
        assert_eq!(engine.vector_generation(shadow.id), generation, "or move the generation");
    }

    /// The correctness half of one commit: a write that fails at the third
    /// of five chunks leaves the previous chunk set exactly as it was, not a
    /// document with two new chunks in front of its old ones. The failure
    /// is a unique violation on the shadow, which is an ordinary collection
    /// and takes an index like any other.
    #[test]
    fn a_failed_chunk_write_leaves_the_previous_chunk_set_intact() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        engine
            .create_index(
                "app",
                &shadow.name,
                vec![crate::meta::IndexField { path: "text".into(), descending: false }],
                true,
                None,
            )
            .unwrap();

        let before: Vec<VectorRecord> =
            (0..3).map(|n| record(n, 10, &format!("old {n}"))).collect();
        engine.put_vectors(&shadow, &source, &before).unwrap();
        let raw_before: Vec<Option<bson::Document>> =
            (0..3).map(|n| engine.get(&shadow, &VectorRecord::id(&source, n)).unwrap()).collect();
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let generation = engine.vector_generation(shadow.id);
        let tail = engine.read_arrival_from(0, 100).unwrap().len();

        // Chunk 2 repeats chunk 0's text, so the third write breaks the
        // unique index against a chunk written earlier in the same scope.
        let torn = vec![
            record(0, 20, "new 0"),
            record(1, 20, "new 1"),
            record(2, 20, "new 0"),
            record(3, 20, "new 3"),
            record(4, 20, "new 4"),
        ];
        let err = engine.put_vectors(&shadow, &source, &torn).unwrap_err();
        assert!(
            matches!(err, StorageError::Core(CoreError::UniqueViolation { .. })),
            "the write fails on the third chunk: {err}"
        );

        assert_eq!(engine.get_vectors(&shadow, &source).unwrap(), before, "the old set is intact");
        for n in 0..3 {
            let raw = engine.get(&shadow, &VectorRecord::id(&source, n)).unwrap();
            assert_eq!(raw, raw_before[n as usize], "chunk {n} is byte-for-byte what it was");
        }
        for n in 3..5 {
            assert!(
                engine.get(&shadow, &VectorRecord::id(&source, n)).unwrap().is_none(),
                "chunk {n} was never committed"
            );
        }
        assert_eq!(engine.commits(), commits, "a failed write must not commit");
        assert!(rx.try_recv().is_err(), "a failed write must not publish");
        assert_eq!(
            engine.read_arrival_from(0, 100).unwrap().len(),
            tail,
            "the oplog must not move"
        );
        assert_eq!(engine.vector_generation(shadow.id), generation, "the generation must not move");
    }

    /// An empty chunk set over a document that has none is not a write: no
    /// commit, and no generation, since nothing an index could see changed.
    #[test]
    fn an_empty_vector_write_over_nothing_costs_nothing() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        let mut rx = engine.subscribe();

        let commits = engine.commits();
        let generation = engine.vector_generation(shadow.id);
        engine.put_vectors(&shadow, &source, &[]).unwrap();
        assert_eq!(engine.commits(), commits, "an empty write over nothing must not commit");
        assert!(rx.try_recv().is_err(), "or publish");
        assert_eq!(engine.vector_generation(shadow.id), generation, "or move the generation");
        assert!(engine.get_vectors(&shadow, &source).unwrap().is_empty());
    }

    #[test]
    fn vectors_are_scoped_to_their_source_document() {
        let (engine, shadow, _dir) = with_vectors();
        let a = DocId::Int64(1);
        let b = DocId::Int64(2);

        engine.put_vectors(&shadow, &a, &[record(0, 10, "from a")]).unwrap();
        let mut for_b = record(0, 10, "from b");
        for_b.source = b.clone();
        engine.put_vectors(&shadow, &b, &[for_b]).unwrap();

        assert_eq!(engine.get_vectors(&shadow, &a).unwrap()[0].text, "from a");
        assert_eq!(engine.get_vectors(&shadow, &b).unwrap()[0].text, "from b");

        // Deleting one must not touch the other.
        assert_eq!(engine.delete_vectors(&shadow, &a).unwrap(), 1);
        assert!(engine.get_vectors(&shadow, &a).unwrap().is_empty());
        assert_eq!(engine.get_vectors(&shadow, &b).unwrap().len(), 1);
    }

    #[test]
    fn staleness_is_derived_from_the_document_version() {
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);

        // Never embedded: work is needed.
        assert!(engine.vectors_are_stale(&shadow, &source, Hlc::new(10, 0)).unwrap());

        engine.put_vectors(&shadow, &source, &[record(0, 10, "text")]).unwrap();
        assert!(!engine.vectors_are_stale(&shadow, &source, Hlc::new(10, 0)).unwrap());
        assert!(engine.vectors_are_stale(&shadow, &source, Hlc::new(11, 0)).unwrap());
    }

    #[test]
    fn one_stale_chunk_marks_the_document_stale() {
        // A partial re-embed that failed halfway must not look complete.
        let (engine, shadow, _dir) = with_vectors();
        let source = DocId::Int64(1);
        engine
            .put_vectors(&shadow, &source, &[record(0, 20, "fresh"), record(1, 10, "stale")])
            .unwrap();
        assert!(engine.vectors_are_stale(&shadow, &source, Hlc::new(20, 0)).unwrap());
    }

    #[test]
    fn every_vector_can_be_visited_for_a_rebuild() {
        let (engine, shadow, _dir) = with_vectors();
        for i in 1..=3i64 {
            let mut r = record(0, 10, "t");
            r.source = DocId::Int64(i);
            engine.put_vectors(&shadow, &DocId::Int64(i), &[r]).unwrap();
        }

        let mut seen = 0;
        engine
            .for_each_vector(&shadow, |_| {
                seen += 1;
                Ok(true)
            })
            .unwrap();
        assert_eq!(seen, 3);
    }

    #[test]
    fn one_documents_chunks_are_read_as_a_run_of_keys() {
        // Ids `1`, `10` and `1#2` all begin with `1`, and `1#2`'s chunk key
        // `1#2#0` sits *inside* `1#`'s run, between `1#2` and `1#3`. The
        // per-document read must return exactly the document's own chunks:
        // pass over the interloper without ending the run, and end the run
        // before `10#0` rather than at the end of the collection.
        let (engine, shadow, _dir) = with_vectors();
        let put = |id: DocId, chunks: &[u32]| {
            let records: Vec<VectorRecord> = chunks
                .iter()
                .map(|&c| {
                    let mut r = record(c, 10, &format!("{id}/{c}"));
                    r.source = id.clone();
                    r
                })
                .collect();
            engine.put_vectors(&shadow, &id, &records).unwrap();
        };
        put(DocId::Int64(1), &[0, 1, 2, 10]);
        put(DocId::Int64(10), &[0]);
        put(DocId::String("1#2".into()), &[0]);
        put(DocId::Int64(2), &[0]);

        let texts = |id: DocId| -> Vec<String> {
            let mut out = Vec::new();
            engine
                .for_each_vector_of(&shadow, &id, |r| {
                    out.push(r.text);
                    Ok(true)
                })
                .unwrap();
            out
        };
        // Key order, which is textual: `#10` before `#2`.
        assert_eq!(texts(DocId::Int64(1)), vec!["1/0", "1/1", "1/10", "1/2"]);
        assert_eq!(texts(DocId::Int64(10)), vec!["10/0"]);
        assert_eq!(texts(DocId::String("1#2".into())), vec!["1#2/0"]);
        assert_eq!(texts(DocId::Int64(2)), vec!["2/0"]);
        assert!(texts(DocId::Int64(3)).is_empty(), "a document with no chunks reads as none");

        // `get_vectors` puts the same run in chunk order.
        let chunks: Vec<u32> = engine
            .get_vectors(&shadow, &DocId::Int64(1))
            .unwrap()
            .iter()
            .map(|r| r.chunk)
            .collect();
        assert_eq!(chunks, vec![0, 1, 2, 10]);

        // The visitor's stop is honoured.
        let mut seen = 0;
        engine
            .for_each_vector_of(&shadow, &DocId::Int64(1), |_| {
                seen += 1;
                Ok(false)
            })
            .unwrap();
        assert_eq!(seen, 1);
    }

    #[test]
    fn a_documents_run_skips_records_that_are_not_vectors() {
        // Same rule as the full scan: a malformed record inside the run is
        // passed over, and a key inside the run that is not a chunk key at all
        // (`1#junk`) neither ends it nor surfaces.
        let (engine, shadow, _dir) = with_vectors();
        engine
            .put_vectors(
                &shadow,
                &DocId::Int64(1),
                &[record(0, 10, "first"), record(9, 10, "last")],
            )
            .unwrap();
        engine.insert(&shadow, bson::doc! { "_id": "1#5", "not": "a vector record" }).unwrap();
        engine.insert(&shadow, bson::doc! { "_id": "1#junk", "not": "a chunk key" }).unwrap();

        let mut seen = Vec::new();
        engine
            .for_each_vector_of(&shadow, &DocId::Int64(1), |r| {
                seen.push(r.text);
                Ok(true)
            })
            .unwrap();
        assert_eq!(seen, vec!["first".to_string(), "last".to_string()]);
    }

    #[test]
    fn a_document_that_is_not_a_vector_record_is_skipped_not_fatal() {
        // A shadow collection is an ordinary collection, so anyone with write
        // access can insert an arbitrary document into one. If that failed the
        // scan, a single bad insert would turn every later search on the
        // collection into a 500 — a client could brick search with one write.
        let (engine, shadow, _dir) = with_vectors();
        engine.put_vectors(&shadow, &DocId::Int64(1), &[record(0, 10, "real")]).unwrap();

        let junk = bson::doc! { "_id": "junk", "not": "a vector record" };
        engine.insert(&shadow, junk).unwrap();

        let mut seen = Vec::new();
        engine
            .for_each_vector(&shadow, |r| {
                seen.push(r.text);
                Ok(true)
            })
            .unwrap();
        assert_eq!(seen, vec!["real".to_string()], "the good record must still be visible");
    }

    #[test]
    fn vectors_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let source = DocId::Int64(1);
        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("app", "docs").unwrap();
            engine.configure_vectors("app", "docs", config(2)).unwrap();
            let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
            engine.put_vectors(&shadow, &source, &[record(0, 10, "durable")]).unwrap();
        }

        let engine = Engine::open(&path).unwrap();
        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let read = engine.get_vectors(&shadow, &source).unwrap();
        assert_eq!(read[0].text, "durable");
        assert_eq!(read[0].vector, vec![0.0, 1.0]);
    }

    #[test]
    fn the_config_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        {
            let engine = Engine::open(&path).unwrap();
            engine.create_collection("app", "docs").unwrap();
            engine.configure_vectors("app", "docs", config(384)).unwrap();
        }

        let engine = Engine::open(&path).unwrap();
        let meta = engine.get_collection("app", "docs").unwrap();
        assert_eq!(meta.vector.expect("config should persist").dim, 384);
    }

    /// A vector-enabled collection holding one document with one chunk.
    fn with_one_embedded_document(engine: &Engine) {
        engine.configure_vectors("app", "docs", config(8)).unwrap();
        let docs = engine.get_collection("app", "docs").unwrap();
        engine
            .insert(
                &docs,
                bson::doc! { "_id": "ghost", "body": "text that is about to be dropped" },
            )
            .unwrap();

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        engine
            .put_vectors(
                &shadow,
                &DocId::String("ghost".into()),
                &[VectorRecord {
                    source: DocId::String("ghost".into()),
                    chunk: 0,
                    source_hlc: Hlc::ZERO,
                    vector: vec![0.5; 8],
                    text: "text that is about to be dropped".into(),
                }],
            )
            .unwrap();
    }

    #[test]
    fn dropping_a_collection_takes_its_vectors_with_it() {
        // The shadow is an ordinary collection with its own id, so removing the
        // parent's rows never touched it. Its chunks outlived the documents they
        // described.
        let (engine, _dir) = engine();
        with_one_embedded_document(&engine);
        assert!(engine.get_collection("app", "docs.__vectors").is_ok());

        engine.drop_collection("app", "docs").unwrap();

        assert!(
            engine.get_collection("app", "docs.__vectors").is_err(),
            "the shadow collection must not outlive the collection it describes"
        );
    }

    #[test]
    fn a_collection_recreated_with_the_same_name_does_not_inherit_old_vectors() {
        // Why the leak mattered rather than merely wasted space. The shadow's
        // name is derived from the parent's, so a new collection with the same
        // name adopted the orphaned chunks -- and they were searchable. On a
        // live cluster a document from the dropped collection came back from
        // `vector_search` scoring 1.0, above the new collection's own document,
        // with an `_id` that resolved to nothing.
        let (engine, _dir) = engine();
        with_one_embedded_document(&engine);
        engine.drop_collection("app", "docs").unwrap();

        engine.create_collection("app", "docs").unwrap();
        engine.configure_vectors("app", "docs", config(8)).unwrap();

        let shadow = engine.vector_collection("app", "docs").unwrap().unwrap();
        let inherited = engine.count(&shadow).unwrap();
        assert_eq!(
            inherited, 0,
            "a new collection must not inherit the vectors of a dropped one with the same name"
        );
    }

    #[test]
    fn dropping_a_shadow_collection_directly_still_works() {
        // The cascade must not recurse: a shadow has no shadow of its own, and
        // `disable_vectors(drop_vectors: true)` drops one by name.
        let (engine, _dir) = engine();
        with_one_embedded_document(&engine);

        engine.drop_collection("app", "docs.__vectors").unwrap();

        assert!(engine.get_collection("app", "docs.__vectors").is_err());
        assert!(
            engine.get_collection("app", "docs").is_ok(),
            "dropping the shadow must not drop the collection it belongs to"
        );
    }
}
