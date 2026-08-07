//! The stored form of a document.

use serde::{Deserialize, Serialize};

use crate::hlc::Stamp;

/// A document as it lives on disk.
///
/// Deletes are represented as records with `deleted = true` rather than by
/// removing the key. A tombstone is required so that a delete can still beat a
/// concurrent insert that arrives from a peer *after* the delete replicated —
/// without one, the insert would look like a brand-new document and the delete
/// would silently undo itself.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct DocRecord {
    /// When and where this version was written. Drives last-writer-wins.
    pub stamp: Stamp,
    /// Tombstone flag.
    pub deleted: bool,
    /// BSON-encoded document body. Empty when `deleted`.
    pub body: Vec<u8>,
}

impl DocRecord {
    pub fn live(stamp: Stamp, body: Vec<u8>) -> Self {
        Self { stamp, deleted: false, body }
    }

    pub fn tombstone(stamp: Stamp) -> Self {
        Self { stamp, deleted: true, body: Vec::new() }
    }

    pub fn is_live(&self) -> bool {
        !self.deleted
    }

    /// Decode the body, or `None` for a tombstone.
    pub fn document(&self) -> crate::Result<Option<bson::Document>> {
        if self.deleted {
            return Ok(None);
        }
        Ok(Some(bson::deserialize_from_slice(&self.body)?))
    }

    /// Resolve a conflict between this record and an incoming one.
    ///
    /// Returns the winner under last-writer-wins. This is the single place the
    /// merge rule lives; replication, local writes, and repair all route
    /// through it so they cannot disagree.
    pub fn merge(self, incoming: DocRecord) -> DocRecord {
        if incoming.stamp.wins_over(&self.stamp) { incoming } else { self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Hlc;
    use crate::ids::NodeId;

    fn stamp(ms: u64, n: u8) -> Stamp {
        Stamp::new(Hlc::new(ms, 0), NodeId::from_bytes([n; 16]))
    }

    fn rec(ms: u64, n: u8, body: &str) -> DocRecord {
        DocRecord::live(stamp(ms, n), body.as_bytes().to_vec())
    }

    #[test]
    fn later_write_wins() {
        let a = rec(100, 1, "old");
        let b = rec(200, 1, "new");
        assert_eq!(a.clone().merge(b.clone()).body, b"new");
        // ...and the same result regardless of which side we start from.
        assert_eq!(b.merge(a).body, b"new");
    }

    #[test]
    fn identical_stamps_do_not_flip() {
        let a = rec(100, 1, "first");
        let b = rec(100, 1, "second");
        // Same stamp means the same logical write; re-applying must not change
        // anything, which is what makes oplog replay idempotent.
        assert_eq!(a.clone().merge(b).body, b"first");
    }

    #[test]
    fn tombstone_beats_an_older_insert() {
        let insert = rec(100, 1, "doc");
        let delete = DocRecord::tombstone(stamp(200, 1));
        assert!(insert.merge(delete).deleted);
    }

    #[test]
    fn tombstone_loses_to_a_newer_insert() {
        let delete = DocRecord::tombstone(stamp(100, 1));
        let insert = rec(200, 1, "resurrected");
        assert!(delete.merge(insert).is_live());
    }

    #[test]
    fn merge_is_commutative_across_nodes() {
        // Concurrent writes in the same millisecond on different nodes: the
        // node id decides, and both replicas must reach the same answer.
        let from_node_1 = rec(100, 1, "one");
        let from_node_2 = rec(100, 2, "two");
        let a = from_node_1.clone().merge(from_node_2.clone());
        let b = from_node_2.merge(from_node_1);
        assert_eq!(a, b);
        assert_eq!(a.body, b"two");
    }
}
