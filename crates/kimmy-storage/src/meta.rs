//! Collection and index metadata.
//!
//! Stored as JSON rather than the packed binary used for documents. This is
//! cold data — read on open and on DDL, never in a query hot path — and being
//! able to read it directly when diagnosing a broken data directory is worth
//! more than the space.

pub use kimmy_core::{
    Enforcement, IndexField, IndexMeta, MergePolicy, ValidatorSpec, VectorConfig,
};
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
    /// How concurrent writes to one document merge. Fixed at creation. No serde default: a
    /// stored definition without it does not decode.
    pub merge_policy: MergePolicy,
    /// The `created` of the creation whose `merge_policy` this is. Two members can create one name
    /// at once under different policies; the policy follows the creation with the later stamp, the
    /// policy text breaking a tie, on every route a creation arrives by (an entry, a snapshot page),
    /// so all members hold the same one. `None` on a definition stored before it was kept: `created`
    /// stands in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_policy_at: Option<Hlc>,
    /// `R`, the join retention of a `field` collection's documents, in milliseconds (U1's companion):
    /// per collection, chosen by the creating member, fixed at creation, replicated with it and immutable,
    /// and it follows the creation whose policy this collection holds. `None` for a `document`
    /// collection, and on a `field` collection written before it was kept (refused as corrupt when a
    /// field state is written or joined: 0.61.0 is a wipe release, nothing migrates it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_retention_ms: Option<u64>,
    /// `K` and the cut of the loss records of a `field` collection's documents (Step 4): per collection, chosen by the
    /// creating member, fixed at creation, replicated with it and immutable, and it follows the creation whose policy
    /// this collection holds, as `R` does. `None` for a `document` collection (and on a `field` collection written before
    /// it was kept: refused as corrupt when a field state is written or joined, a wipe release).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loss_limits: Option<kimmy_core::LossLimits>,
    /// Present on a swap's stage (ADR-272): the deadline its creation carried, which no binding of the stage may
    /// pass. Fixed at creation, replicated with it and immutable, like `R` and `K`. `None` on every other collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<kimmy_core::StageInfo>,
    pub indexes: Vec<IndexMeta>,
    /// Monotonic index-id allocator.
    ///
    /// Kept as a counter rather than derived from `indexes`, because a derived
    /// value would hand a dropped index's id to the next one created — and a
    /// dropped index's entries are removed lazily, so the new index would
    /// inherit them.
    index_id_counter: u32,
    /// Auto-embedding configuration, when enabled for this collection.
    pub vector: Option<VectorConfig>,
    /// The stamp of the latest change to `vector`: a configuration, a change of it,
    /// or turning it off (ADR-226). `None` on a collection whose vector state was
    /// never changed, which reads as older than any stamped change.
    ///
    /// It makes "off" a dated fact. A snapshot page carries `vector` as a presence
    /// only, so a configuration turned off could not be told from one never turned
    /// on, and a member that held the configuration kept it; with a stamp on both
    /// sides, the later change wins and a page can say "off at s". The same stamp
    /// makes the entries path last-writer-wins, which arrival order was not.
    ///
    /// Not inside `VectorConfig`, which refuses unknown fields.
    pub vector_changed: Option<kimmy_core::Stamp>,
    /// The collection's validator, when it has one (ADR-257).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validator: Option<ValidatorSpec>,
    /// The stamp of the latest change to `validator`: setting it, replacing it, or
    /// removing it. As for `vector_changed` (ADR-226), it makes "removed" a dated
    /// fact: the entries path and a snapshot page both apply a change only when it is
    /// newer than the one standing, so two members that changed the validator
    /// concurrently converge on the later stamp and a page can say "off at s".
    /// `None` on a collection whose validator was never changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validator_changed: Option<kimmy_core::Stamp>,
    /// The stamp below which an entry targets a **previous incarnation** of
    /// this collection.
    ///
    /// Collections derive their id from `(db, name)`, so a drop-and-recreate
    /// yields the same id, and replicated entries from the old life still
    /// resolve. `created` alone cannot arbitrate that: a *replicated*
    /// creation records the receiver's clock at apply time (which sits after
    /// the entire catch-up backlog), not the moment the incarnation began.
    /// The floor is the originating creation stamp instead, and it is `None`
    /// for a life with no drop before it, where the tombstone comparison is the
    /// only guard: inventing a floor from a local clock would suppress
    /// legitimate catch-up.
    pub incarnation_floor: Option<Hlc>,
    /// Set when a replicated drop met writes stamped after it and the life was
    /// **re-dated** instead of buried: `created` was moved to just after the
    /// drop (ADR-231, extended). Holds that `created` and, once the sweep is
    /// done, the arrival position of the first entry the life kept.
    ///
    /// Valid only while its `created` is the meta's own: any later change of
    /// `created` leaves a value that no longer matches, and it is ignored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redated: Option<Redated>,
}

/// See [`CollectionMeta::redated`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redated {
    /// The `created` the life had before the first drop re-dated it: where the
    /// life began, which ADR-235's burial compares to tell one life continuing
    /// from a newer one. (0.49.0 also judged a database's drop against it and
    /// buried the life, writes and all; ADR-236 judges `created`, as every drop
    /// does.)
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

    /// What decides between two creations of this name: the stamp of the creation whose policy
    /// this collection holds, then the policy's text (a tie between members that created at the
    /// same instant under different policies is settled the same way everywhere).
    pub fn policy_key(&self) -> (Hlc, &'static str) {
        (self.merge_policy_at.unwrap_or(self.created), self.merge_policy.as_str())
    }

    /// Does a creation at `at` under `policy` take the policy of this collection from the one it
    /// holds? The later creation wins; a creation never takes it from itself.
    pub fn policy_yields_to(&self, at: Hlc, policy: MergePolicy) -> bool {
        (at, policy.as_str()) > self.policy_key()
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
            merge_policy: MergePolicy::Document,
            merge_policy_at: None,
            join_retention_ms: None,
            loss_limits: None,
            stage: None,
            indexes: Vec::new(),
            index_id_counter: 0,
            vector: None,
            vector_changed: None,
            validator: None,
            validator_changed: None,
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

    #[test]
    fn the_merge_policy_is_always_stored_and_required() {
        let mut m = meta();
        m.merge_policy = MergePolicy::Field;
        let json = serde_json::to_vec(&m).unwrap();
        let back: CollectionMeta = serde_json::from_slice(&json).unwrap();
        assert_eq!(back.merge_policy, MergePolicy::Field);
        let mut value: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(value["merge_policy"], "field");
        value.as_object_mut().unwrap().remove("merge_policy");
        assert!(serde_json::from_value::<CollectionMeta>(value).is_err());
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
