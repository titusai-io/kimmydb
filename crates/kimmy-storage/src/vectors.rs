//! Vector configuration lifecycle and shadow collections.
//!
//! Enabling vectors on a collection creates a companion collection named
//! `{collection}.__vectors`. It is an ordinary collection — same durability,
//! same oplog, same eventual replication — rather than a parallel storage
//! mechanism that would need its own correctness argument.
//!
//! The `__` segment prefix is reserved for system objects, so a user cannot
//! create a collection that shadows one.

use kimmy_core::{
    DocId, Error as CoreError, Hlc, ResumeToken, VectorConfig, VectorRecord, vector_meta,
};
use tracing::info;

use redb::ReadableDatabase;

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

        // Changing the width would mix incompatible vectors in one index, so
        // it needs an explicit reindex rather than a silent reconfiguration.
        if let Some(existing) = &meta.vector
            && existing.dim != config.dim
        {
            return Err(StorageError::Core(CoreError::InvalidQuery(format!(
                "vector dimension cannot change from {} to {} in place; drop the vector \
                 configuration first, which discards the existing vectors",
                existing.dim, config.dim
            ))));
        }

        let shadow = vector_meta::shadow_name(collection);
        self.create_system_collection(db, &shadow)?;

        meta.vector = Some(config.clone());
        let txn = self.db().begin_write()?;
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
        let txn = self.db().begin_write()?;
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
    /// needs this to decide whether an entry is interesting.
    pub fn collection_by_id(&self, id: kimmy_core::CollectionId) -> Result<Option<CollectionMeta>> {
        for db in self.list_databases()? {
            if let Some(found) = self.list_collections(&db.name)?.into_iter().find(|c| c.id == id) {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    /// Persist a background consumer's position in the oplog.
    ///
    /// Keyed by name so several consumers can track independently. Stored
    /// rather than recomputed, because rebuilding it would mean re-reading the
    /// whole log on every restart.
    pub fn put_consumer_position(&self, consumer: &str, token: ResumeToken) -> Result<()> {
        let txn = self.db().begin_write()?;
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
        let key = source.to_string();
        let mut out = Vec::new();
        self.for_each_doc(shadow, |id, doc| {
            if VectorRecord::parse_id(&id).is_some_and(|(s, _)| s == key) {
                out.push(decode_vector(doc)?);
            }
            Ok(true)
        })?;
        out.sort_by_key(|r| r.chunk);
        Ok(out)
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
        let key = source.to_string();
        let mut out = Vec::new();
        self.for_each_doc(shadow, |id, _| {
            if let Some((s, chunk)) = VectorRecord::parse_id(&id)
                && s == key
            {
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

    fn config(dim: usize) -> VectorConfig {
        VectorConfig {
            fields: vec!["body".into()],
            provider: ProviderConfig::Byo,
            dim,
            metric: Default::default(),
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
}
