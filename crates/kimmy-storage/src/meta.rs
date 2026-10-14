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
    /// The stamp of the create that produced this incarnation, at its
    /// origin: this node's clock for a local create, the entry's stamp for a
    /// replicated one. A replayed drop older than this is history.
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
    /// The stamp of the latest change to `vector`: a configuration, a change of it,
    /// or turning it off (ADR-226). `None` on a collection whose vector state was
    /// last written by a build that did not stamp it, which reads as older than any
    /// stamped change.
    ///
    /// It makes "off" a dated fact. A snapshot page carries `vector` as a presence
    /// only, so a configuration turned off could not be told from one never turned
    /// on, and a member that held the configuration kept it; with a stamp on both
    /// sides, the later change wins and a page can say "off at s". The same stamp
    /// makes the entries path last-writer-wins, which arrival order was not.
    ///
    /// Not inside `VectorConfig`, which refuses unknown fields: a build that does not
    /// know this reads a `CollectionMeta` carrying it and ignores it.
    #[serde(default)]
    pub vector_changed: Option<kimmy_core::Stamp>,
    /// The stamp below which an entry targets a **previous incarnation** of
    /// this collection — set on every creation since ADR-081.
    ///
    /// Collections derive their id from `(db, name)`, so a drop-and-recreate
    /// yields the same id, and replicated entries from the old life still
    /// resolve. `created` alone cannot arbitrate that: a *replicated*
    /// creation records the receiver's clock at apply time (which sits after
    /// the entire catch-up backlog), not the moment the incarnation began.
    /// The floor is the originating creation stamp instead, and it is
    /// deliberately absent (`None`) on metas written before ADR-081 — for
    /// those rows the tombstone comparison remains the only guard, exactly as
    /// before, because inventing a floor from a local clock would suppress
    /// legitimate catch-up.
    #[serde(default)]
    pub incarnation_floor: Option<Hlc>,
    /// Set when a replicated drop met writes stamped after it and the life was
    /// **re-dated** instead of buried: `created` was moved to just after the
    /// drop (ADR-231, extended). Holds that `created` and, once the sweep is
    /// done, the arrival position of the first entry the life kept.
    ///
    /// Valid only while its `created` is the meta's own: any later change of
    /// `created` leaves a value that no longer matches, and it is ignored.
    /// Additive like `vector_changed`: a build that does not know it ignores it
    /// and, rewriting the meta, drops it, which costs a stream open the walk it
    /// made before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redated: Option<Redated>,
}

/// See [`CollectionMeta::redated`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redated {
    /// The `created` the life had before the first drop re-dated it: what a
    /// database's drop that arrives later is judged against (ADR-229), since a
    /// life created at or before it is buried with it, writes and all.
    pub from: Hlc,
    /// The `created` this marker belongs to.
    pub created: Hlc,
    /// The lowest arrival position among the entries the life kept, once the
    /// sweep has taken it. `None` while the sweep runs, and when no kept
    /// entry is still held.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_arrival: Option<u64>,
    /// The arrival position the next entry took when the life was re-dated:
    /// nothing written after the first commit sits below it, so the sweep, which
    /// reads the records that are there when it passes them, takes the lower of
    /// this and what it saw. A record that lands in a range already passed
    /// still has its entry at or above this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrivals_from: Option<u64>,
}

impl CollectionMeta {
    /// Whether this life was re-dated by a drop that met writes stamped after
    /// it, and has not been re-created since.
    pub fn is_redated(&self) -> bool {
        self.redated.is_some_and(|r| r.created == self.created)
    }

    /// The arrival position of the first entry a re-dated life kept, if the
    /// marker belongs to this `created` and the sweep recorded one.
    pub fn redated_first_arrival(&self, created: Hlc) -> Option<u64> {
        self.redated.filter(|r| r.created == created && self.created == created)?.first_arrival
    }

    pub fn new(
        id: CollectionId,
        db: impl Into<String>,
        name: impl Into<String>,
        created: Hlc,
        incarnation_floor: Option<Hlc>,
    ) -> Self {
        Self {
            id,
            db: db.into(),
            name: name.into(),
            created,
            indexes: Vec::new(),
            index_id_counter: 0,
            vector: None,
            vector_changed: None,
            incarnation_floor,
            redated: None,
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
        CollectionMeta::new(
            CollectionId(1),
            "app",
            "orders",
            Hlc::new(10, 0),
            Some(Hlc::new(10, 0)),
        )
    }

    /// A marker is read only for the `created` it was written with: a stale one
    /// (the life was re-created, or re-dated again, since) reads as absent, and
    /// a meta written without the field reads as never re-dated.
    #[test]
    fn a_redated_marker_is_read_only_for_its_own_created() {
        let mut m = meta();
        assert!(!m.is_redated());
        m.created = Hlc::new(20, 0);
        m.redated = Some(Redated {
            from: Hlc::new(10, 0),
            created: Hlc::new(20, 0),
            first_arrival: Some(7),
            arrivals_from: None,
        });
        assert!(m.is_redated());
        assert_eq!(m.redated_first_arrival(Hlc::new(20, 0)), Some(7));
        assert_eq!(m.redated_first_arrival(Hlc::new(21, 0)), None, "another created");
        m.created = Hlc::new(30, 0);
        assert!(!m.is_redated(), "re-dated again since");
        assert_eq!(m.redated_first_arrival(Hlc::new(30, 0)), None);
        let text = serde_json::to_string(&meta()).unwrap();
        assert!(!text.contains("redated"), "absent when unset: {text}");
        let stale: CollectionMeta = serde_json::from_str(&text).unwrap();
        assert_eq!(stale.redated, None);
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
            expire_after_secs: None,
            partial_filter: None,
            created: None,
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

    #[test]
    fn an_index_written_before_ttl_existed_still_loads() {
        // The same guarantee one level down, for M9's `expire_after_secs`.
        // Both formats this crosses are self-describing — JSON here and BSON
        // in the replicated `IndexCreate` — so an added optional field is
        // tolerated rather than read as corruption. That is *why* a TTL could
        // be an index option at all: a new `OpKind` would not have been,
        // because `op_kind_from_tag` refuses an unknown tag.
        let json = r#"{"id":1,"db":"app","name":"orders","created":{"wall_ms":10,"counter":0},
            "indexes":[{"id":7,"name":"age_1","fields":[{"path":"age"}],"unique":false}]}"#;
        let m: CollectionMeta = serde_json::from_str(json).unwrap();
        let index = m.index("age_1").expect("the index still loads");
        assert_eq!(index.expire_after_secs, None);
        assert!(!index.is_ttl());
        assert_eq!(index.ttl_path(), None);
    }

    #[test]
    fn a_ttl_index_round_trips_through_json() {
        let mut m = meta();
        m.indexes.push(IndexMeta {
            id: 0,
            name: "ttl_seen".into(),
            fields: vec![IndexField::ascending("seen")],
            unique: false,
            enforcement: Default::default(),
            multikey: false,
            expire_after_secs: Some(3600),
            partial_filter: None,
            created: None,
        });
        let text = serde_json::to_string(&m).unwrap();
        let back: CollectionMeta = serde_json::from_str(&text).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.index("ttl_seen").unwrap().ttl_path(), Some("seen"));
    }
}
