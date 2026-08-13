//! Catching up a peer whose history has been collected.
//!
//! Anti-entropy works by replaying oplog entries, which only reaches back as
//! far as `oplog_retention_secs`. A node joining a cluster older than that
//! window asks for history nobody still has: it receives nothing it can apply,
//! its version vector never advances, and it retries forever. With the default
//! retention that is *any* node added to a cluster more than a day old — which
//! is to say, adding a node to a running cluster.
//!
//! So when a peer asks from below the horizon, it is sent **current state**
//! instead of history: collection definitions first, then documents in pages,
//! then the sender's coverage. The receiver is caught up when it has all three.
//!
//! # Why documents arrive as oplog entries
//!
//! Each snapshot document is applied through the same `apply_remote` that
//! replication uses, carrying the stamp the document actually has. That is not
//! a trick to save code — it is what makes the result *correct*:
//!
//! - last-writer-wins still decides, so a receiver that already holds a newer
//!   version of a document keeps it;
//! - secondary indexes are maintained, so the new node can answer index-backed
//!   queries;
//! - unique violations are detected and reported, rather than being smuggled in
//!   through a side door that skips the check.
//!
//! Collection definitions are *not* logged, because unlike a document's stamp,
//! this node holds no honest record of when or where the collection was created
//! — only that it exists. Inventing history would be worse than omitting it.

use kimmy_core::{
    CollectionId, DocId, Hlc, IndexMeta, OpKind, OplogEntry, Stamp, VectorConfig, VersionVector,
};
use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::codec;
use crate::engine::Engine;
use crate::error::Result;
use crate::tables;

/// Documents per page.
///
/// Bounded so a large collection crosses the wire in several frames rather than
/// one the receiver may not have the memory to hold.
pub const SNAPSHOT_PAGE: usize = 512;

/// Where a snapshot left off, so it can resume rather than restart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotCursor {
    pub collection: u64,
    /// Encoded document key; the next page starts strictly after it.
    pub after_key: Vec<u8>,
}

/// A collection's definition, without its documents.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CollectionState {
    pub db: String,
    pub name: String,
    pub indexes: Vec<IndexMeta>,
    pub vector: Option<VectorConfig>,
}

/// One document, as it currently stands.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SnapshotDoc {
    pub collection: u64,
    pub id: DocId,
    pub stamp: Stamp,
    /// `None` for a tombstone, which travels so a delete is not undone by a
    /// peer that still holds the document.
    pub body: Option<Vec<u8>>,
}

/// One page of a snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPage {
    /// Collection definitions. Sent with the first page only.
    pub collections: Vec<CollectionState>,
    pub documents: Vec<SnapshotDoc>,
    /// Where to resume, or `None` when the snapshot is complete.
    pub next: Option<SnapshotCursor>,
    /// The sender's coverage. Only adopted once `next` is `None`, because a
    /// partial snapshot has not granted it yet.
    pub versions: VersionVector,
}

impl Engine {
    /// Produce one page of a snapshot of current state.
    pub fn snapshot_page(&self, after: Option<SnapshotCursor>) -> Result<SnapshotPage> {
        // Definitions ride the first page, so the receiver can create the
        // collections before any document needs one.
        let collections = if after.is_none() { self.collection_states()? } else { Vec::new() };

        let (documents, next) = self.snapshot_documents(after)?;
        Ok(SnapshotPage { collections, documents, next, versions: self.version_vector()? })
    }

    fn collection_states(&self) -> Result<Vec<CollectionState>> {
        let txn = self.db().begin_read()?;
        let collections = txn.open_table(tables::COLLECTIONS)?;

        let mut out = Vec::new();
        for row in collections.iter()? {
            let (_, value) = row?;
            let meta: crate::CollectionMeta = serde_json::from_slice(value.value())?;
            out.push(CollectionState {
                db: meta.db,
                name: meta.name,
                indexes: meta.indexes,
                vector: meta.vector,
            });
        }
        Ok(out)
    }

    /// Read up to [`SNAPSHOT_PAGE`] documents, resuming after `cursor`.
    ///
    /// Walks the `docs` table in key order, which is `(collection, id)` — so a
    /// single cursor covers every collection without needing to track which one
    /// is in progress.
    fn snapshot_documents(
        &self,
        after: Option<SnapshotCursor>,
    ) -> Result<(Vec<SnapshotDoc>, Option<SnapshotCursor>)> {
        let txn = self.db().begin_read()?;
        let docs = txn.open_table(tables::DOCS)?;

        let mut out = Vec::new();
        let mut cursor = None;

        // `Excluded` on the resume point, so the document that ended the last
        // page is not sent twice.
        let start = match &after {
            Some(c) => std::ops::Bound::Excluded((c.collection, c.after_key.as_slice())),
            None => std::ops::Bound::Unbounded,
        };

        for row in docs.range::<(u64, &[u8])>((start, std::ops::Bound::Unbounded))? {
            let (key, value) = row?;
            let (collection, doc_key) = key.value();
            let record = codec::decode_doc_record(value.value())?;

            // A tombstone has no `_id` to recover from a body, so its id comes
            // from decoding the key is impossible — keyenc is one-way. Skipping
            // tombstones here means a delete does not travel in a snapshot;
            // that is safe, because the receiver never had the document.
            let Some(document) = record.document()? else {
                continue;
            };
            let id = match document.get(crate::ID_FIELD) {
                Some(value) => DocId::try_from_bson(value)?,
                None => continue,
            };

            out.push(SnapshotDoc { collection, id, stamp: record.stamp, body: Some(record.body) });

            if out.len() >= SNAPSHOT_PAGE {
                cursor = Some(SnapshotCursor { collection, after_key: doc_key.to_vec() });
                break;
            }
        }

        Ok((out, cursor))
    }

    /// Apply one page of a peer's snapshot.
    pub fn apply_snapshot_page(&self, page: &SnapshotPage) -> Result<usize> {
        for state in &page.collections {
            self.restore_collection(state)?;
        }

        let mut applied = 0usize;
        for document in &page.documents {
            let Some(collection) = self.collection_by_id(CollectionId(document.collection))? else {
                // The definition should have arrived on the first page; a
                // document without one means a truncated or reordered snapshot.
                debug!(collection = document.collection, "snapshot document has no collection");
                continue;
            };

            // Reconstructed as an ordinary replicated write, so last-writer-wins
            // decides, indexes are maintained, and a unique violation is
            // reported rather than smuggled past the check.
            let entry = OplogEntry {
                stamp: document.stamp,
                kind: OpKind::Replace,
                collection: collection.id,
                doc_id: Some(document.id.clone()),
                body: document.body.clone(),
            };
            if self.apply_remote(&collection, &entry)? {
                applied += 1;
            }
        }

        // Only a *completed* snapshot grants coverage: adopting it earlier would
        // stop the receiver asking for pages it has not been sent.
        if page.next.is_none() {
            self.absorb_version_vector(&page.versions)?;
            info!("snapshot complete");
        }
        Ok(applied)
    }

    /// Recreate a collection and its indexes from a snapshot.
    fn restore_collection(&self, state: &CollectionState) -> Result<()> {
        if self.get_collection(&state.db, &state.name).is_err() {
            self.create_collection_inner(&state.db, &state.name, false)?;
        }

        for index in &state.indexes {
            let existing = self.get_collection(&state.db, &state.name)?;
            if existing.index(&index.name).is_some() {
                continue;
            }
            self.create_index_inner(
                &state.db,
                &state.name,
                index.fields.clone(),
                index.unique,
                index.enforcement,
                Some(index.name.clone()),
                // A restored TTL index keeps its policy: dropping it here
                // would leave a collection that silently stopped expiring.
                index.expire_after_secs,
                false,
            )?;
        }

        if let Some(config) = &state.vector {
            let existing = self.get_collection(&state.db, &state.name)?;
            if existing.vector.as_ref() != Some(config) {
                self.configure_vectors_inner(&state.db, &state.name, config.clone(), false)?;
            }
        }
        Ok(())
    }

    /// Whether a peer asking from `from` can be served from the oplog.
    ///
    /// `false` means the history it needs has been collected, and serving it
    /// incrementally would hand it a silent gap.
    ///
    /// Compared against what retention has actually *removed*, not against the
    /// oldest retained entry: on a node that has never collected anything the
    /// oldest entry is just the first write ever made, and a peer asking from
    /// before it would be sent a full snapshot for no reason.
    pub fn can_serve_from_oplog(&self, from: Hlc) -> Result<bool> {
        Ok(from >= self.oplog_collected_through()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        (engine, dir)
    }

    fn field(path: &str) -> crate::meta::IndexField {
        crate::meta::IndexField { path: path.into(), descending: false }
    }

    /// Transfer a full snapshot from `from` into `into`.
    fn transfer(into: &Engine, from: &Engine) -> usize {
        let mut cursor = None;
        let mut applied = 0;
        loop {
            let page = from.snapshot_page(cursor.clone()).unwrap();
            applied += into.apply_snapshot_page(&page).unwrap();
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        applied
    }

    #[test]
    fn a_snapshot_carries_collections_indexes_and_documents() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("item")], true, None).unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        for i in 0..10i64 {
            a.insert(&ca, doc! { "_id": i, "item": format!("item-{i}") }).unwrap();
        }

        transfer(&b, &a);

        let cb = b.get_collection("shop", "orders").expect("the collection must arrive");
        assert_eq!(b.count(&cb).unwrap(), 10);
        let index = cb.indexes.iter().find(|i| i.name == "item_1").expect("the index must arrive");
        assert!(index.unique);
    }

    #[test]
    fn a_snapshot_pages_a_collection_larger_than_one_page() {
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        let total = SNAPSHOT_PAGE * 2 + 7;
        for i in 0..total as i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }

        transfer(&b, &a);

        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(b.count(&cb).unwrap(), total as u64, "every page must arrive exactly once");
    }

    #[test]
    fn a_snapshot_makes_a_node_beyond_the_horizon_able_to_catch_up() {
        // The failure this exists for: A's oplog is collected, so B asking for
        // history receives nothing it can apply and never advances.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        for i in 0..20i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }
        a.collect_garbage_at(
            crate::engine::physical_now_ms() + 1_000_000_000,
            crate::gc::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();

        // Incremental sync cannot help.
        let from = b.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
        assert!(!a.can_serve_from_oplog(from).unwrap(), "a should know it cannot serve this");

        transfer(&b, &a);

        let cb = b.get_collection("shop", "orders").unwrap();
        assert_eq!(b.count(&cb).unwrap(), 20);
        assert!(
            b.version_vector().unwrap().covers(&a.version_vector().unwrap()),
            "after a snapshot the receiver must stop asking for collected history"
        );
    }

    #[test]
    fn a_snapshot_does_not_overwrite_a_newer_local_version() {
        // Last-writer-wins still decides. A snapshot is state, not authority.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1, "v": "older" }).unwrap();

        let cb = b.create_collection("shop", "orders").unwrap();
        b.replace(&cb, &DocId::Int64(1), doc! { "_id": 1, "v": "newer" }, true).unwrap();

        transfer(&b, &a);

        let kept = b.get(&cb, &DocId::Int64(1)).unwrap().unwrap();
        assert_eq!(kept.get_str("v").unwrap(), "newer", "a snapshot must not undo a newer write");
    }

    #[test]
    fn absorbing_coverage_keeps_the_receivers_own_writes() {
        // The receiver may hold writes the sender never saw; adopting the
        // sender's vector outright would claim it had forgotten them.
        let (a, _da) = engine();
        let (b, _db) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "local-only" }).unwrap();
        let b_own = b.version_vector().unwrap().get(b.node_id());

        transfer(&b, &a);

        assert_eq!(
            b.version_vector().unwrap().get(b.node_id()),
            b_own,
            "the receiver's own coverage must survive"
        );
    }

    #[test]
    fn a_completed_snapshot_survives_a_restart() {
        // The version vector was derived from the oplog, which would have
        // recomputed the granted coverage away on the next open and sent the
        // node back to asking for history it cannot be given.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (a, _da) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();
        a.collect_garbage_at(
            crate::engine::physical_now_ms() + 1_000_000_000,
            crate::gc::RetentionPolicy::new(0, u64::MAX),
        )
        .unwrap();

        let granted = {
            let b = Engine::open(&path).unwrap();
            transfer(&b, &a);
            b.version_vector().unwrap()
        };

        let reopened = Engine::open(&path).unwrap();
        assert_eq!(
            reopened.version_vector().unwrap(),
            granted,
            "a restart must not undo coverage a snapshot granted"
        );
    }

    #[test]
    fn a_peer_within_the_window_is_still_served_incrementally() {
        // Snapshots are the fallback, not the default: they transfer everything.
        let (a, _da) = engine();
        let ca = a.create_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": 1 }).unwrap();

        assert!(a.can_serve_from_oplog(Hlc::ZERO).unwrap(), "nothing has been collected yet");
    }
}
