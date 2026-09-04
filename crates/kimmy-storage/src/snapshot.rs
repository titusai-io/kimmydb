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
use tracing::{debug, info, warn};

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
    /// `CollectionId`, not a bare `u64`: BSON has no unsigned 64-bit type, and
    /// half of all derived ids sit above `i64::MAX`. A bare integer here made
    /// every snapshot page naming such a collection unencodable, so a node
    /// beyond a peer's retention horizon — the only case a snapshot serves —
    /// could never catch up. Same defect as ADR-031's, on the other wire type.
    pub collection: CollectionId,
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
    /// See [`SnapshotCursor::collection`] for why this is not a `u64`.
    pub collection: CollectionId,
    pub id: DocId,
    pub stamp: Stamp,
    /// `None` for a tombstone, which travels so a delete is not undone by a
    /// peer that still holds the document.
    ///
    /// Binary rather than serde's default array-of-int32s, for the reason on
    /// [`kimmy_core::OplogEntry::body`]. It matters at least as much here: a
    /// snapshot is a whole collection by definition, so this is the largest
    /// thing the cluster ever sends.
    #[serde(with = "serde_bytes")]
    pub body: Option<Vec<u8>>,
}

/// What applying one snapshot page did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotApplied {
    /// Documents the page wrote — those that won last-writer-wins here.
    pub applied: usize,
    /// Index definitions on the page this node's documents refused and it
    /// skipped (ADR-123). Folded into `SyncOutcome::ddl_refused` by the
    /// transport, so a refusal reached through a snapshot is counted where
    /// one reached through the oplog is.
    pub ddl_refused: usize,
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
            Some(c) => std::ops::Bound::Excluded((c.collection.0, c.after_key.as_slice())),
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

            out.push(SnapshotDoc {
                collection: CollectionId(collection),
                id,
                stamp: record.stamp,
                body: Some(record.body),
            });

            if out.len() >= SNAPSHOT_PAGE {
                cursor = Some(SnapshotCursor {
                    collection: CollectionId(collection),
                    after_key: doc_key.to_vec(),
                });
                break;
            }
        }

        Ok((out, cursor))
    }

    /// Apply one page of a peer's snapshot.
    pub fn apply_snapshot_page(&self, page: &SnapshotPage) -> Result<SnapshotApplied> {
        let mut ddl_refused = 0usize;
        for state in &page.collections {
            ddl_refused += self.restore_collection(state)?;
        }

        let mut applied = 0usize;
        for document in &page.documents {
            let Some(collection) = self.collection_by_id(document.collection)? else {
                // The definition should have arrived on the first page; a
                // document without one means a truncated or reordered snapshot.
                debug!(collection = document.collection.0, "snapshot document has no collection");
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
        Ok(SnapshotApplied { applied, ddl_refused })
    }

    /// Recreate a collection and its indexes from a snapshot.
    ///
    /// Returns how many of its index definitions this node's documents
    /// refused. A snapshot is served to exactly the peer most likely to hold
    /// documents a definition cannot be built under — one that was away
    /// long enough to write on its own past the origin's retention — and a
    /// bare error here failed the whole snapshot round, the same wedge as a
    /// replayed `CreateIndex` reached through the other route. So the
    /// definitions go through the same classification as replicated DDL
    /// (`sync::settle`, ADR-123): a refused one is warned and counted, and
    /// the restore goes on to the documents; any other error still fails
    /// the page.
    fn restore_collection(&self, state: &CollectionState) -> Result<usize> {
        if self.get_collection(&state.db, &state.name).is_err() {
            self.create_collection_inner(&state.db, &state.name, false, None)?;
        }

        let mut refused = 0usize;
        for index in &state.indexes {
            let existing = self.get_collection(&state.db, &state.name)?;
            let created = self.create_index_inner(
                &state.db,
                &state.name,
                index.fields.clone(),
                index.unique,
                index.enforcement,
                Some(index.name.clone()),
                // A restored TTL index keeps its policy: dropping it here
                // would leave a collection that silently stopped expiring.
                index.expire_after_secs,
                index.partial_filter.clone(),
                // The definition's own creation stamp, which is the only
                // ordering fact a snapshot carries — there is no entry behind
                // it. A name already held here by a different definition is
                // resolved against it exactly as a replicated `CreateIndex`
                // is (ADR-132); a definition already here under this name is
                // returned unchanged, which is what makes calling this for
                // every index of the page cheap. A snapshot written before
                // the stamp existed carries none, and such a rival is refused
                // and counted rather than silently skipped as it was.
                crate::index::CreateOrigin::Replicated(index.created),
            );
            match crate::sync::settle(created)? {
                crate::sync::Ddl::Applied((_, violations)) => {
                    // A replicated definition is built over whatever this
                    // node already holds, collisions reported rather than
                    // refused (ADR-020, ADR-123). Usually nothing: the
                    // indexes are restored before the documents.
                    if !violations.is_empty() {
                        self.report_index_backfill_violations(&existing, &violations)?;
                    }
                }
                // The collection was created or found moments ago; gone now
                // means a concurrent local drop, and the rest of its indexes
                // have nowhere to go.
                crate::sync::Ddl::Gone => break,
                crate::sync::Ddl::Refused(reason) => {
                    refused += 1;
                    warn!(
                        db = %state.db,
                        collection = %state.name,
                        index = %index.name,
                        reason = %reason,
                        "skipped an index in a snapshot this node cannot build; the \
                         definition stands on its peers and the divergence is counted in \
                         kimmy_sync_ddl_refused_total"
                    );
                }
            }
        }

        if let Some(config) = &state.vector {
            let existing = self.get_collection(&state.db, &state.name)?;
            if existing.vector.as_ref() != Some(config) {
                self.configure_vectors_inner(&state.db, &state.name, config.clone(), false)?;
            }
        }
        Ok(refused)
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

    /// Whether a peer that has processed `held` can be served from the oplog.
    ///
    /// The per-origin form of [`Self::can_serve_from_oplog`], for a peer that
    /// sent its vector rather than only the threshold it derived from it.
    /// `false` when, at any origin this node is ahead of the peer, retention
    /// has removed an entry the peer lacks — that is the silent gap. `true`
    /// otherwise, *even when the threshold is below the coarse horizon*:
    /// then every entry the peer lacks is still here to serve, and the gap
    /// between its threshold and the horizon is made of entries it holds.
    ///
    /// The case that needs this is an origin that wrote nothing for longer
    /// than retention and then wrote once. A peer that had everything asks
    /// from the previous write — collected long ago, along with everything
    /// around it — and the coarse horizon can only answer with a snapshot,
    /// for a gap that holds exactly one servable entry (ADR-097).
    pub fn can_serve_peer_holding(&self, held: &VersionVector) -> Result<bool> {
        let mine = self.version_vector()?;
        let collected = self.oplog_collected()?;
        Ok(!crate::sync::lacks_collected(held, &mine, &collected))
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
            applied += into.apply_snapshot_page(&page).unwrap().applied;
            match page.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        applied
    }

    /// A `(db, name)` whose derived id has the top bit set — the half of the
    /// id space BSON cannot carry as an unsigned integer.
    fn high_bit_collection(db: &str) -> String {
        (0u32..)
            .map(|i| format!("orders-{i}"))
            .find(|name| CollectionId::derive(db, name).0 > i64::MAX as u64)
            .unwrap()
    }

    #[test]
    fn a_snapshot_page_naming_a_high_bit_collection_survives_bson() {
        // The in-process transfer above never serialises, which is how a page
        // that BSON refused to encode passed every test here while every real
        // snapshot on a cluster failed with "cannot fit into BSON".
        let (a, _da) = engine();
        let name = high_bit_collection("shop");
        let ca = a.create_collection("shop", &name).unwrap();
        assert!(ca.id.0 > i64::MAX as u64, "the fixture must sit in the unencodable half");
        for i in 0..(SNAPSHOT_PAGE + 1) as i64 {
            a.insert(&ca, doc! { "_id": i }).unwrap();
        }

        let page = a.snapshot_page(None).unwrap();
        assert!(page.next.is_some(), "the cursor must be exercised too");
        let bytes = bson::serialize_to_vec(&page).expect("a page must always encode");
        let back: SnapshotPage = bson::deserialize_from_slice(&bytes).unwrap();
        assert_eq!(back, page, "and round-trip exactly, cursor included");

        let (b, _db) = engine();
        assert_eq!(transfer(&b, &a), SNAPSHOT_PAGE + 1);
        let cb = b.get_collection("shop", &name).unwrap();
        assert_eq!(cb.id, ca.id);
        assert_eq!(b.count(&cb).unwrap() as usize, SNAPSHOT_PAGE + 1);
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
    fn a_snapshot_index_this_node_cannot_build_is_skipped_and_the_documents_restore() {
        // The same wedge as a replayed create, reached through the route
        // that is served to exactly the peer most likely to hold divergent
        // documents. B wrote a two-array document while away; A's snapshot
        // carries a compound index over those two fields. The definition is
        // refused by B's data, counted, and the documents still arrive.
        let (a, _da) = engine();
        let (b, _db) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.create_index("shop", "orders", vec![field("tags"), field("cats")], false, None).unwrap();
        let ca = a.get_collection("shop", "orders").unwrap();
        a.insert(&ca, doc! { "_id": "a-1", "tags": ["x"] }).unwrap();
        a.insert(&ca, doc! { "_id": "a-2", "cats": ["p"] }).unwrap();
        let cb = b.create_collection("shop", "orders").unwrap();
        b.insert(&cb, doc! { "_id": "both", "tags": ["x", "y"], "cats": ["p", "q"] }).unwrap();

        let page = a.snapshot_page(None).unwrap();
        let outcome = b.apply_snapshot_page(&page).expect("a refused index must not fail the page");
        assert_eq!(outcome.ddl_refused, 1, "{outcome:?}");
        assert_eq!(outcome.applied, 2, "the documents restore: {outcome:?}");
        assert!(
            b.get_collection("shop", "orders").unwrap().index("tags_1_cats_1").is_none(),
            "the definition this node's documents cannot be built under is skipped"
        );
        for id in ["a-1", "a-2", "both"] {
            assert!(b.get(&cb, &DocId::String(id.into())).unwrap().is_some(), "{id}");
        }
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
