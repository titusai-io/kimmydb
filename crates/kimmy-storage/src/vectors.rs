//! Vector configuration lifecycle and shadow collections.
//!
//! Enabling vectors on a collection creates a companion collection named
//! `{collection}.__vectors`. It is an ordinary collection — same durability,
//! same oplog, same eventual replication — rather than a parallel storage
//! mechanism that would need its own correctness argument.
//!
//! The `__` segment prefix is reserved for system objects, so a user cannot
//! create a collection that shadows one.

use kimmy_core::{DocId, Error as CoreError, Hlc, VectorConfig, VectorRecord, vector_meta};
use tracing::info;

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

        meta.vector = Some(config);
        let txn = self.db().begin_write()?;
        crate::Engine::put_collection_meta(&txn, &meta)?;
        txn.commit()?;

        info!(db, collection, shadow = %shadow, "configured auto-embedding");
        Ok(meta)
    }

    /// Turn off auto-embedding, optionally discarding the vectors.
    ///
    /// Keeping them by default means re-enabling with the same settings does
    /// not force a full re-embed, which for a remote provider is a real cost.
    pub fn disable_vectors(&self, db: &str, collection: &str, drop_vectors: bool) -> Result<bool> {
        let mut meta = self.get_collection(db, collection)?;
        if meta.vector.is_none() {
            return Ok(false);
        }

        meta.vector = None;
        let txn = self.db().begin_write()?;
        crate::Engine::put_collection_meta(&txn, &meta)?;
        txn.commit()?;

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
        Ok(chunks.len())
    }

    /// Visit every stored vector. Used by search and by index rebuilds.
    pub fn for_each_vector<F>(&self, shadow: &CollectionMeta, mut f: F) -> Result<()>
    where
        F: FnMut(VectorRecord) -> Result<bool>,
    {
        self.for_each_doc(shadow, |_, doc| f(decode_vector(doc)?))
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
