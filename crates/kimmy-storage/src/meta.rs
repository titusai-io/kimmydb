//! Collection and index metadata.
//!
//! Stored as JSON rather than the packed binary used for documents. This is
//! cold data — read on open and on DDL, never in a query hot path — and being
//! able to read it directly when diagnosing a broken data directory is worth
//! more than the space.

use kimmy_core::{Hlc, ids::CollectionId};
pub use kimmy_core::{IndexField, IndexMeta};
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
    /// Auto-embedding configuration. Populated in M2.
    #[serde(default)]
    pub vector: Option<serde_json::Value>,
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

    /// The id the next index will receive.
    ///
    /// Takes the maximum of the stored counter and one past the highest live
    /// index id. The second term only matters for metadata written before the
    /// counter existed, where trusting the counter alone would reuse an id.
    pub fn next_index_id(&self) -> u32 {
        let derived = self.indexes.iter().map(|i| i.id + 1).max().unwrap_or(0);
        self.index_id_counter.max(derived)
    }

    /// Allocate a fresh index id, never reusing a dropped one.
    pub fn allocate_index_id(&mut self) -> u32 {
        let id = self.next_index_id();
        self.index_id_counter = id + 1;
        id
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
        });
        let text = serde_json::to_string(&m).unwrap();
        assert_eq!(serde_json::from_str::<CollectionMeta>(&text).unwrap(), m);
    }

    fn push_index(m: &mut CollectionMeta, path: &str) -> u32 {
        let id = m.allocate_index_id();
        m.indexes.push(IndexMeta {
            id,
            name: format!("{path}_1"),
            fields: vec![IndexField::ascending(path)],
            unique: false,
        });
        id
    }

    #[test]
    fn index_ids_are_never_reused() {
        let mut m = meta();
        assert_eq!(push_index(&mut m, "a"), 0);
        assert_eq!(push_index(&mut m, "b"), 1);

        // Dropping the newest index must not free its id: a dropped index's
        // entries are removed lazily, so a new index reusing the id would
        // inherit them.
        m.indexes.retain(|i| i.id != 1);
        assert_eq!(push_index(&mut m, "c"), 2);

        // ...and dropping everything still must not restart from zero.
        m.indexes.clear();
        assert_eq!(push_index(&mut m, "d"), 3);
    }

    #[test]
    fn the_id_counter_survives_a_round_trip() {
        let mut m = meta();
        push_index(&mut m, "a");
        push_index(&mut m, "b");
        m.indexes.clear();

        let reloaded: CollectionMeta =
            serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(reloaded.next_index_id(), 2, "the counter must be persisted");
    }

    #[test]
    fn metadata_predating_the_counter_does_not_reuse_ids() {
        // Written before `index_id_counter` existed: the counter defaults to 0,
        // so falling back to the live indexes is what prevents a collision.
        let json = r#"{"id":1,"db":"app","name":"orders",
            "created":{"wall_ms":10,"counter":0},
            "indexes":[{"id":0,"name":"a_1","fields":[{"path":"a"}],"unique":false},
                       {"id":1,"name":"b_1","fields":[{"path":"b"}],"unique":false}]}"#;
        let m: CollectionMeta = serde_json::from_str(json).unwrap();
        assert_eq!(m.next_index_id(), 2);
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
