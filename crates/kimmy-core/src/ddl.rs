//! Payloads for schema changes that replicate.
//!
//! Every one of these names its target by **database and collection name**
//! rather than relying on the entry's `collection` id. The id is derived from
//! those names ([`crate::CollectionId::derive`]), so a peer *could* recompute
//! it — but not the reverse: a hash cannot be inverted, so a node receiving
//! `CreateCollection` for a collection it has never heard of would have no way
//! to learn what to call it.
//!
//! # Why an operation each, rather than one metadata snapshot
//!
//! Shipping the whole `CollectionMeta` and merging it last-writer-wins would be
//! simpler, and would lose concurrent index additions: two nodes each adding a
//! *different* index during a partition would produce two whole-metadata
//! values, one of which wins entirely, and one index would silently vanish.
//!
//! Separate operations merge independently, so both survive. The cost is that
//! each one needs its own idempotency rule, which is what
//! `Engine::apply_ddl` provides.

use serde::{Deserialize, Serialize};

use crate::index_meta::IndexMeta;
use crate::vector_meta::VectorConfig;

/// Which collection an operation applies to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionRef {
    pub db: String,
    pub name: String,
}

impl CollectionRef {
    pub fn new(db: impl Into<String>, name: impl Into<String>) -> Self {
        Self { db: db.into(), name: name.into() }
    }
}

/// An index being created on a collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCreate {
    pub db: String,
    pub collection: String,
    /// The full definition, including its derived id.
    ///
    /// The id travels rather than being recomputed on arrival so that a
    /// receiving node cannot disagree with the sender about it — if the
    /// derivation ever changed, entries written by the two builds would still
    /// key alike within one replicated definition.
    pub index: IndexMeta,
}

/// An index being dropped from a collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDrop {
    pub db: String,
    pub collection: String,
    /// Index name. Names are unique within a collection and are what the id is
    /// derived from, so this identifies it exactly.
    pub index: String,
}

/// Auto-embedding being configured, or turned off.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorSet {
    pub db: String,
    pub collection: String,
    /// `None` disables embedding.
    ///
    /// Whether the *stored vectors* are discarded is deliberately not
    /// replicated: it is a local reclamation choice, and a peer that kept its
    /// vectors while another dropped them still converges — the shadow
    /// collection is ordinary data and reconciles through the same anti-entropy
    /// as everything else.
    pub config: Option<VectorConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payloads_round_trip_through_bson() {
        let reference = CollectionRef::new("shop", "orders");
        let bytes = bson::serialize_to_vec(&reference).unwrap();
        assert_eq!(bson::deserialize_from_slice::<CollectionRef>(&bytes).unwrap(), reference);

        let drop =
            IndexDrop { db: "shop".into(), collection: "orders".into(), index: "email_1".into() };
        let bytes = bson::serialize_to_vec(&drop).unwrap();
        assert_eq!(bson::deserialize_from_slice::<IndexDrop>(&bytes).unwrap(), drop);
    }

    #[test]
    fn disabling_vectors_is_distinct_from_never_configuring_them() {
        // `None` has to survive the round trip as `None` rather than collapsing
        // into an absent field, or a replicated disable would be a no-op.
        let off = VectorSet { db: "d".into(), collection: "c".into(), config: None };
        let bytes = bson::serialize_to_vec(&off).unwrap();
        let back: VectorSet = bson::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(back.config, None);
        assert_eq!(back, off);
    }
}
