//! Vector configuration lifecycle and shadow collections.
//!
//! Enabling vectors on a collection creates a companion collection named
//! `{collection}.__vectors`. It is an ordinary collection — same durability,
//! same oplog, same eventual replication — rather than a parallel storage
//! mechanism that would need its own correctness argument.
//!
//! The `__` segment prefix is reserved for system objects, so a user cannot
//! create a collection that shadows one.

use kimmy_core::{Error as CoreError, VectorConfig, vector_meta};
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
