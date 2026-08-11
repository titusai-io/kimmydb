//! Collection and index metadata.
//!
//! Stored as JSON rather than the packed binary used for documents. This is
//! cold data — read on open and on DDL, never in a query hot path — and being
//! able to read it directly when diagnosing a broken data directory is worth
//! more than the space.

pub use kimmy_core::{Enforcement, IndexField, IndexMeta, VectorConfig};
use kimmy_core::{Hlc, ids::CollectionId};
use serde::{Deserialize, Serialize};

/// A database: purely a namespace for collections.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseMeta {
    pub name: String,
    pub created: Hlc,
}

/// Everything the engine knows about a collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionMeta {
    pub id: CollectionId,
    pub db: String,
    pub name: String,
    pub created: Hlc,
    #[serde(default)]
    pub indexes: Vec<IndexMeta>,
    /// Monotonic index-id allocator.
    ///
    /// Kept as a counter rather than derived from `indexes`, because a derived
    /// value would hand a dropped index's id to the next one created — and a
    /// dropped index's entries are removed lazily, so the new index would
    /// inherit them.
    #[serde(default)]
    index_id_counter: u32,
    /// Auto-embedding configuration, when enabled for this collection.
    #[serde(default)]
    pub vector: Option<VectorConfig>,
}

impl CollectionMeta {
    pub fn new(
        id: CollectionId,
        db: impl Into<String>,
        name: impl Into<String>,
        created: Hlc,
    ) -> Self {
        Self {
            id,
            db: db.into(),
            name: name.into(),
            created,
            indexes: Vec::new(),
            index_id_counter: 0,
            vector: None,
        }
    }

    pub fn index(&self, name: &str) -> Option<&IndexMeta> {
        self.indexes.iter().find(|i| i.name == name)
    }

    /// The collection already holding `id`, if any.
    ///
    /// Index ids are derived from names now, so the only way two indexes share
    /// one is a hash collision — checked rather than assumed, because two
    /// indexes sharing entries is unrecoverable.
    pub fn index_by_id(&self, id: u32) -> Option<&IndexMeta> {
        self.indexes.iter().find(|i| i.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> CollectionMeta {
        CollectionMeta::new(CollectionId(1), "app", "orders", Hlc::new(10, 0))
    }

    #[test]
    fn metadata_round_trips_through_json() {
        let mut m = meta();
        m.indexes.push(IndexMeta {
            id: 0,
            name: "age_1".into(),
            fields: vec![IndexField::ascending("age")],
            unique: false,
            enforcement: Default::default(),
            multikey: false,
        });
        let text = serde_json::to_string(&m).unwrap();
        assert_eq!(serde_json::from_str::<CollectionMeta>(&text).unwrap(), m);
    }

    #[test]
    fn index_metadata_written_before_enforcement_existed_defaults_to_local() {
        // The field was added once the cross-node semantics were settled; older
        // metadata must keep loading, and must not silently claim a stronger
        // guarantee than it was created with.
        let json = r#"{"id":0,"name":"email_1","fields":[{"path":"email"}],"unique":true}"#;
        let index: IndexMeta = serde_json::from_str(json).unwrap();
        assert_eq!(index.enforcement, Enforcement::Local);
    }

    #[test]
    fn index_ids_are_derived_from_the_name() {
        // Every node must compute the same id, or a replicated index definition
        // would key entries differently on each of them.
        assert_eq!(IndexMeta::derive_id("email_1"), IndexMeta::derive_id("email_1"));
        assert_ne!(IndexMeta::derive_id("email_1"), IndexMeta::derive_id("status_1"));
    }

    #[test]
    fn derived_index_ids_are_pinned() {
        // Index-entry keys embed these, so the hash is not free to change.
        // Cross-checked against an independent FNV-1a-32 implementation.
        assert_eq!(IndexMeta::derive_id("email_1"), 0xb788_067b);
        assert_eq!(IndexMeta::derive_id(""), 0x811c_9dc5);
    }

    #[test]
    fn default_index_names_follow_mongo_convention() {
        let fields = vec![IndexField::ascending("age"), IndexField::descending("name")];
        assert_eq!(IndexMeta::default_name(&fields), "age_1_name_-1");
    }

    #[test]
    fn older_metadata_without_new_fields_still_loads() {
        // Forward compatibility for the M2 vector config: metadata written
        // before that field existed must still deserialize.
        let json = r#"{"id":1,"db":"app","name":"orders","created":{"wall_ms":10,"counter":0}}"#;
        let m: CollectionMeta = serde_json::from_str(json).unwrap();
        assert!(m.indexes.is_empty());
        assert!(m.vector.is_none());
    }
}
