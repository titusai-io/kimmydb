//! The stored form of a document.

use crate::hlc::Stamp;

/// A document as it lives on disk.
///
/// Deletes are represented as records with `deleted = true` rather than by
/// removing the key. A tombstone is required so that a delete can still beat a
/// concurrent insert that arrives from a peer *after* the delete replicated —
/// without one, the insert would look like a brand-new document and the delete
/// would silently undo itself.
/// **Deliberately not `Serialize`/`Deserialize`.** Nothing serialized this — the
/// whole workspace, tests included, compiles without the derives — and having
/// them was a trap rather than a convenience: `body` is a bare `Vec<u8>`, which
/// serde encodes as a BSON array of int32s, so anything that started putting a
/// `DocRecord` on the wire would silently pay twelve bytes per byte. That is the
/// defect just removed from `OplogEntry::body` and `SnapshotDoc::body`.
///
/// Without the derives it is a compile error instead of a quiet cost. If one is
/// ever genuinely needed on the wire, add `#[serde(with = "serde_bytes")]` to
/// `body` in the same change.
///
/// Storage does not want them either: it writes this through its own binary
/// codec (`kimmy_storage::codec::encode_doc_record`), which has always stored
/// the body as raw bytes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DocRecord {
    /// When and where this version was written. Drives last-writer-wins.
    pub stamp: Stamp,
    /// Tombstone flag.
    pub deleted: bool,
    /// BSON-encoded document body. Empty when `deleted`, **except for the tombstone of a `field` collection**, whose
    /// body is exactly the document `{_id: <id>}` ([`DocRecord::field_tombstone`]): the merge state of a deleted
    /// document keeps no `_id` (the join drops the cells of a dead life), and a member that is to name the document
    /// in an entry of its own (a loss record) must be able to, whichever route its state came by. Nothing reads the
    /// body of a tombstone as a document: [`DocRecord::document`] returns `None` for it.
    pub body: Vec<u8>,
    /// The merge state of a `field` collection's document, in the compact form (a tombstone
    /// keeps it: the version vector runs through deletes). `None` for a `document`
    /// collection, whose record is the version 1 bytes it always was. A record decoded by the
    /// body-only reader carries `None` whatever was stored; only the reader that is named
    /// `with_meta` fills it.
    pub meta: Option<Vec<u8>>,
}

impl DocRecord {
    pub fn live(stamp: Stamp, body: Vec<u8>) -> Self {
        Self { stamp, deleted: false, body, meta: None }
    }

    pub fn tombstone(stamp: Stamp) -> Self {
        Self { stamp, deleted: true, body: Vec::new(), meta: None }
    }

    /// A tombstone of a `field` collection: the delete's stamp and a body of exactly `{_id: <id>}`, never more (a
    /// tombstone that kept a document would be read as one by whatever finds a non-empty body).
    pub fn field_tombstone(stamp: Stamp, id: &crate::DocId) -> Self {
        let mut body = bson::Document::new();
        body.insert("_id", id.to_bson());
        let body = bson::serialize_to_vec(&body).expect("a document of one id serializes");
        Self { stamp, deleted: true, body, meta: None }
    }

    /// The `_id` a tombstone keeps, if it keeps one: `None` for a record that is live, for a tombstone of a
    /// `document` collection (its body is empty), and for a body that is not `{_id: ...}`.
    pub fn tombstone_id(&self) -> Option<crate::DocId> {
        if !self.deleted || self.body.is_empty() {
            return None;
        }
        let doc: bson::Document = bson::deserialize_from_slice(&self.body).ok()?;
        if doc.len() != 1 {
            return None;
        }
        crate::DocId::try_from_bson(doc.get("_id")?).ok()
    }

    /// The `_id` of this record whether it is live (read from its document) or a tombstone that keeps one: what a
    /// field tombstone written over it must keep.
    pub fn id_for_tombstone(&self) -> Option<crate::DocId> {
        if self.deleted {
            return self.tombstone_id();
        }
        let doc: bson::Document = bson::deserialize_from_slice(&self.body).ok()?;
        crate::DocId::try_from_bson(doc.get("_id")?).ok()
    }

    /// The record with the merge state `meta` (compact bytes) attached.
    pub fn with_meta(mut self, meta: Vec<u8>) -> Self {
        self.meta = Some(meta);
        self
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
    use crate::Epoch;
    use crate::hlc::Hlc;
    use crate::ids::NodeId;

    fn stamp(ms: u64, n: u8) -> Stamp {
        Stamp::new(Hlc::new(ms, 0), NodeId::from_bytes([n; 16]), Epoch::ZERO)
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

#[cfg(test)]
mod field_tombstone_tests {
    use super::*;
    use crate::{DocId, Hlc, NodeId};

    fn stamp() -> Stamp {
        Stamp::new(Hlc::new(5, 0), NodeId::from_bytes([1; 16]), crate::Epoch::ZERO)
    }

    #[test]
    fn a_field_tombstone_keeps_the_id_and_exactly_that() {
        for id in [DocId::Int64(7), DocId::String("a".into())] {
            let record = DocRecord::field_tombstone(stamp(), &id);

            assert!(record.deleted, "it is a tombstone");
            let body: bson::Document = bson::deserialize_from_slice(&record.body).unwrap();
            assert_eq!(body.len(), 1, "the body is the id and nothing else: {body:?}");
            assert_eq!(record.tombstone_id(), Some(id), "and the id reads back");
            assert_eq!(record.document().unwrap(), None, "a tombstone is never read as a document");
        }
    }

    #[test]
    fn a_tombstone_of_any_other_collection_keeps_no_id_and_its_body_stays_empty() {
        let record = DocRecord::tombstone(stamp());

        assert!(record.body.is_empty(), "a document collection's tombstone body is empty");
        assert_eq!(record.tombstone_id(), None);
        assert_eq!(record.id_for_tombstone(), None);
    }

    #[test]
    fn the_id_a_tombstone_written_over_a_record_must_keep_is_read_from_a_live_document_or_a_tombstone()
     {
        let live = DocRecord::live(
            stamp(),
            bson::serialize_to_vec(&bson::doc! { "_id": 3, "x": 1 }).unwrap(),
        );
        assert_eq!(live.id_for_tombstone(), Some(DocId::Int64(3)));
        assert_eq!(live.tombstone_id(), None, "a live record keeps no tombstone id");
        assert_eq!(
            DocRecord::field_tombstone(stamp(), &DocId::Int64(3)).id_for_tombstone(),
            Some(DocId::Int64(3))
        );
    }

    #[test]
    fn a_body_that_is_not_exactly_an_id_is_no_tombstone_id() {
        let mut record = DocRecord::tombstone(stamp());
        record.body = bson::serialize_to_vec(&bson::doc! { "_id": 3, "x": 1 }).unwrap();
        assert_eq!(
            record.tombstone_id(),
            None,
            "a tombstone that kept a document is not trusted to name one"
        );
    }
}
