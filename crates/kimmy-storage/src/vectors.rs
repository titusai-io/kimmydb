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

use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;

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
        self.configure_vectors_inner(db, collection, config, true)
    }

    /// `log = false` when applying a replicated configuration. See
    /// `create_index_inner` for why a replicated change must not mint an entry.
    pub(crate) fn configure_vectors_inner(
        &self,
        db: &str,
        collection: &str,
        config: VectorConfig,
        log: bool,
    ) -> Result<CollectionMeta> {
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

        let shadow = vector_meta::shadow_name(collection);
        self.create_system_collection(db, &shadow)?;

        meta.vector = Some(config.clone());
        let txn = self.begin_write()?;
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
        if let Some(entry) = logged {
            self.publish(vec![entry]);
        }

        info!(db, collection, shadow = %shadow, "configured auto-embedding");
        Ok(meta)
    }

    /// Turn off auto-embedding, optionally discarding the vectors.
    ///
    /// Keeping them by default means re-enabling with the same settings does
    /// not force a full re-embed, which for a remote provider is a real cost.
    pub fn disable_vectors(&self, db: &str, collection: &str, drop_vectors: bool) -> Result<bool> {
        self.disable_vectors_inner(db, collection, drop_vectors, true)
    }

    pub(crate) fn disable_vectors_inner(
        &self,
        db: &str,
        collection: &str,
        drop_vectors: bool,
        log: bool,
    ) -> Result<bool> {
        let mut meta = self.get_collection(db, collection)?;
        if meta.vector.is_none() {
            return Ok(false);
        }

        meta.vector = None;
        let txn = self.begin_write()?;
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
        Ok(true)
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
        let txn = self.begin_write()?;
        {
            let mut meta = txn.open_table(crate::tables::META)?;
            meta.insert(consumer_key(consumer).as_str(), token.encode().as_bytes())?;
        }
        txn.commit()?;
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
        let txn = self.begin_write()?;
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
    pub fn put_vectors(
        &self,
        shadow: &CollectionMeta,
        source: &DocId,
        records: &[VectorRecord],
    ) -> Result<()> {
        for record in records {
            let id = VectorRecord::id(source, record.chunk);
            let doc = bson::serialize_to_document(record)
                .map_err(|e| StorageError::Corrupt(format!("encoding vector record: {e}")))?;
            self.replace(shadow, &id, doc, true)?;
        }

        // Drop the tail of a previously longer document.
        for existing in self.vector_chunk_numbers(shadow, source)? {
            if !records.iter().any(|r| r.chunk == existing) {
                self.delete(shadow, &VectorRecord::id(source, existing))?;
            }
        }
        self.bump_vector_generation(shadow.id);
        Ok(())
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
    pub fn delete_vectors(&self, shadow: &CollectionMeta, source: &DocId) -> Result<usize> {
        let chunks = self.vector_chunk_numbers(shadow, source)?;
        for chunk in &chunks {
            self.delete(shadow, &VectorRecord::id(source, *chunk))?;
        }
        if !chunks.is_empty() {
            self.bump_vector_generation(shadow.id);
        }
        Ok(chunks.len())
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
