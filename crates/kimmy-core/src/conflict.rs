//! What a merged write broke, when it broke a constraint.
//!
//! Uniqueness is not maintainable without coordination — see
//! [ADR-020](../../../docs/decisions.md) — so a `local` unique index can be
//! violated by replication no matter what the merge does. The design response
//! is not to prevent it, which is provably impossible, but to make it
//! **impossible to miss**: a change-stream event, a durable record, and a
//! metric.
//!
//! This is the payload of that event.

use serde::{Deserialize, Serialize};

use crate::ids::DocId;

/// A unique constraint broken by merging a replicated write.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UniqueViolationDetail {
    /// The index whose constraint no longer holds.
    pub index: String,
    /// Documents now sharing the key, including the one just merged.
    ///
    /// All of them still exist. Nothing was discarded — the two documents have
    /// different `_id`s, so last-writer-wins never ran on them. Only the
    /// constraint is broken, which is why reconciliation is a decision for the
    /// application rather than something the database can make for it.
    pub ids: Vec<DocId>,
    /// The document whose merge revealed the collision.
    ///
    /// Named separately because it is the actionable one: the others were
    /// already here and already visible to clients.
    pub merged: DocId,
}

impl UniqueViolationDetail {
    pub fn new(index: impl Into<String>, merged: DocId, ids: Vec<DocId>) -> Self {
        Self { index: index.into(), ids, merged }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_detail_round_trips_through_bson() {
        let detail = UniqueViolationDetail::new(
            "email_unique",
            DocId::String("remote".into()),
            vec![DocId::String("local".into()), DocId::String("remote".into())],
        );
        let bytes = bson::serialize_to_vec(&detail).unwrap();
        let back: UniqueViolationDetail = bson::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(back, detail);
    }

    #[test]
    fn the_merged_document_is_among_the_holders() {
        // The event is useless for reconciliation if it names a colliding
        // document that is not in the list of who holds the key.
        let detail = UniqueViolationDetail::new(
            "i",
            DocId::Int64(2),
            vec![DocId::Int64(1), DocId::Int64(2)],
        );
        assert!(detail.ids.contains(&detail.merged));
    }
}
