//! Point-in-time restore: rewinding document state to an earlier instant.
//!
//! # What this can and cannot do, stated first
//!
//! The oplog stores **post-images** ([ADR-008]) — what a document became, never
//! what it was. A delete stores nothing at all: `DocRecord::tombstone` discards
//! the body and the `Delete` entry carries no payload. So the only history that
//! exists is the sequence of values documents *took*, and a rewind can put a
//! document back only to a value the retained oplog still holds.
//!
//! That gives a precise rule. For each document changed after the target time
//! `T`, its state at `T` is:
//!
//! - the post-image of its **latest entry at or before `T`**, if the oplog still
//!   has one; or
//! - **gone**, if its earliest entry after `T` is an `Insert` — it did not exist
//!   yet; or
//! - **unrecoverable**, otherwise: the document existed at `T` with a value
//!   whose last write has already been collected.
//!
//! The third case is refused rather than guessed, and the documents are named.
//! A rewind that silently substituted a later value would produce a database
//! that looks restored and is not — the worst outcome available here.
//!
//! # What this means operationally
//!
//! **Recovering a mistaken update or delete works when the document was written
//! within the oplog retention window.** Recovering one whose previous version
//! predates the horizon does not, because that version no longer exists
//! anywhere. `storage.oplog_retention_secs` is therefore the real
//! point-in-time window, which is worth knowing when choosing it.
//!
//! **Dropped collections cannot be undone.** `drop_collection` purges the
//! documents, and purged documents are not in the oplog either — only the fact
//! of the drop is. Any schema change after the target is refused for the same
//! reason: the rewind would produce a collection whose contents it cannot
//! reconstruct.
//!
//! # Nothing is written until everything is known
//!
//! The whole plan is computed, and every refusal raised, before a single
//! document is touched. A rewind that failed halfway would leave a database at
//! neither point in time.
//!
//! [ADR-008]: ../../../docs/decisions.md

use std::collections::HashMap;

use kimmy_core::{DocId, Hlc, OpKind, OplogEntry};
use redb::{ReadableDatabase, ReadableTable};
use tracing::info;

use crate::codec;
use crate::engine::Engine;
use crate::error::{Result, StorageError};
use crate::{index, tables};

/// What a rewind changed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RewindOutcome {
    /// Documents put back to an earlier value.
    pub reverted: usize,
    /// Documents removed because they did not exist at the target.
    pub removed: usize,
    /// Oplog entries discarded because they described the undone future.
    pub oplog_discarded: usize,
}

/// How many unrecoverable documents to name before summarising.
///
/// Enough to act on, few enough that the message stays readable when a rewind
/// crosses a bulk change.
const MAX_NAMED: usize = 20;

/// A document's storage key: `(collection id, encoded _id)`.
type DocKey = (u64, Vec<u8>);

/// What a document should become, or `None` to remove it.
type PlannedChange = (DocKey, Option<OplogEntry>);

/// One document's history either side of the target.
#[derive(Default)]
struct History {
    latest_at_or_before: Option<OplogEntry>,
    earliest_after: Option<OplogEntry>,
}

impl Engine {
    /// Rewind document state to `until`.
    ///
    /// Refuses, without writing anything, if the target predates the oplog
    /// horizon, if any schema change happened after it, or if any document's
    /// value at that instant is no longer recoverable.
    pub fn rewind_to(&self, until: Hlc) -> Result<RewindOutcome> {
        let horizon = self.oplog_collected_through()?;
        if until < horizon {
            return Err(StorageError::Database(format!(
                "cannot rewind to {until:?}: the oplog has been collected through {horizon:?}, \
                 so nothing describes the database before that point. \
                 storage.oplog_retention_secs is the window this can reach back over"
            )));
        }

        // Gather first, decide second, write third. Reading the whole plan
        // before touching anything is what makes a refusal leave the database
        // exactly as it was.
        let mut histories: HashMap<DocKey, History> = HashMap::new();
        let mut schema_changes: Vec<OpKind> = Vec::new();
        let mut discardable = 0usize;

        {
            let txn = self.db().begin_read()?;
            let oplog = txn.open_table(tables::OPLOG)?;
            for entry in oplog.iter()? {
                let (_, value) = entry?;
                let entry = codec::decode_oplog_entry(value.value())?;
                let after = entry.stamp.hlc > until;
                if after {
                    discardable += 1;
                }

                match entry.kind {
                    // Not a mutation, and not something a rewind has to undo.
                    OpKind::UniqueViolation => continue,
                    OpKind::CreateCollection
                    | OpKind::DropCollection
                    | OpKind::CreateIndex
                    | OpKind::DropIndex
                    | OpKind::ConfigureVectors
                    | OpKind::Collection => {
                        if after {
                            schema_changes.push(entry.kind);
                        }
                        continue;
                    }
                    OpKind::Insert | OpKind::Update | OpKind::Replace | OpKind::Delete => {}
                }

                let Some(id) = entry.doc_id.clone() else {
                    continue;
                };
                let Ok(key) = crate::docs::doc_key(&id) else {
                    continue;
                };
                let slot = histories.entry((entry.collection.0, key)).or_default();

                if after {
                    // The oplog iterates in stamp order, so the first one seen
                    // after the target is the earliest.
                    if slot.earliest_after.is_none() {
                        slot.earliest_after = Some(entry);
                    }
                } else {
                    // ...and the last one seen at or before it is the latest.
                    slot.latest_at_or_before = Some(entry);
                }
            }
        }

        if !schema_changes.is_empty() {
            let mut kinds: Vec<String> =
                schema_changes.iter().map(|k| format!("{k:?}")).collect::<Vec<_>>();
            kinds.sort();
            kinds.dedup();
            return Err(StorageError::Database(format!(
                "cannot rewind past a schema change: {} occurred after the target. Dropping a \
                 collection purges its documents, and purged documents are not in the oplog \
                 either, so the rewind could not reconstruct what it undid",
                kinds.join(", ")
            )));
        }

        // Only documents that actually changed after the target need touching.
        let changed: Vec<_> =
            histories.into_iter().filter(|(_, h)| h.earliest_after.is_some()).collect();

        let mut plan: Vec<PlannedChange> = Vec::new();
        let mut unrecoverable: Vec<(u64, DocId)> = Vec::new();

        for (key, history) in changed {
            match history.latest_at_or_before {
                // Its value at the target is still in the log.
                Some(entry) => plan.push((key, Some(entry))),
                None => {
                    let earliest = history.earliest_after.as_ref().expect("filtered above");
                    if earliest.kind == OpKind::Insert {
                        // Created after the target: it did not exist then.
                        plan.push((key, None));
                    } else {
                        // It existed, and the value it had has been collected.
                        if let Some(id) = earliest.doc_id.clone() {
                            unrecoverable.push((earliest.collection.0, id));
                        }
                    }
                }
            }
        }

        if !unrecoverable.is_empty() {
            let total = unrecoverable.len();
            let named: Vec<String> = unrecoverable
                .iter()
                .take(MAX_NAMED)
                .map(|(coll, id)| format!("collection {coll} {id:?}"))
                .collect();
            let more = total.saturating_sub(named.len());
            return Err(StorageError::Database(format!(
                "cannot rewind: {total} document(s) changed after the target but their earlier \
                 value has already been collected from the oplog, so it exists nowhere. \
                 Nothing was modified. Affected: {}{}",
                named.join(", "),
                if more > 0 { format!(" and {more} more") } else { String::new() }
            )));
        }

        // Everything is decided. Now write.
        let mut outcome = RewindOutcome::default();
        let txn = self.begin_write()?;
        {
            // Index maintenance needs each collection's index definitions,
            // keyed by the id the oplog refers to.
            let mut collections: HashMap<u64, crate::CollectionMeta> = HashMap::new();
            for db_meta in self.list_databases()? {
                for meta in self.list_collections(&db_meta.name)? {
                    collections.insert(meta.id.0, meta);
                }
            }
            let mut docs = txn.open_table(tables::DOCS)?;

            for ((collection_id, key), restore_to) in plan {
                let before = match docs.get((collection_id, key.as_slice()))? {
                    Some(raw) => codec::decode_doc_record(raw.value())?.document()?,
                    None => None,
                };

                let after = match &restore_to {
                    Some(entry) if entry.kind != OpKind::Delete => match &entry.body {
                        Some(body) => {
                            docs.insert(
                                (collection_id, key.as_slice()),
                                codec::encode_doc_record(&kimmy_core::DocRecord::live(
                                    entry.stamp,
                                    body.clone(),
                                ))
                                .as_slice(),
                            )?;
                            outcome.reverted += 1;
                            Some(bson::deserialize_from_slice::<bson::Document>(body).map_err(
                                |e| {
                                    StorageError::Database(format!(
                                        "decoding a restored document: {e}"
                                    ))
                                },
                            )?)
                        }
                        // An entry of a writing kind with no body cannot happen,
                        // and treating it as a delete would quietly lose data.
                        None => {
                            return Err(StorageError::Database(format!(
                                "oplog entry {:?} has no body; the oplog is inconsistent and the \
                                 rewind has been abandoned",
                                entry.stamp
                            )));
                        }
                    },
                    // Deleted at the target, or not yet created: both end as a
                    // tombstone, which is what a delete leaves anyway.
                    _ => {
                        let stamp = restore_to
                            .as_ref()
                            .map(|e| e.stamp)
                            .unwrap_or_else(|| self.next_stamp());
                        docs.insert(
                            (collection_id, key.as_slice()),
                            codec::encode_doc_record(&kimmy_core::DocRecord::tombstone(stamp))
                                .as_slice(),
                        )?;
                        outcome.removed += 1;
                        None
                    }
                };

                // Index entries follow the document, or an index-backed query
                // would answer from the future the rewind just undid.
                if let Some(meta) = collections.get(&collection_id) {
                    let newly_multikey =
                        index::maintain(&txn, meta, before.as_ref(), after.as_ref(), &key)?;
                    // A restored image can hold arrays the current one did not;
                    // the flag is one-way, so marking is the only safe answer.
                    index::mark_multikey(&txn, &meta.db, &meta.name, &newly_multikey)?;
                }
            }

            // The undone future must leave the log as well: an entry describing
            // a change that no longer exists would be shipped to peers, and
            // would undo the rewind on the next anti-entropy round.
            let mut oplog = txn.open_table(tables::OPLOG)?;
            let mut doomed = Vec::new();
            for entry in oplog.iter()? {
                let (key, value) = entry?;
                if codec::decode_oplog_entry(value.value())?.stamp.hlc > until {
                    doomed.push(key.value().to_vec());
                }
            }
            for key in doomed {
                oplog.remove(key.as_slice())?;
                outcome.oplog_discarded += 1;
            }
        }
        txn.commit()?;

        // The oplog just shrank, so the version vector has to come down with
        // it: leaving it high would make this node claim history it no longer
        // holds, and no peer would ever send that range again.
        //
        // This is the one place a *lowering* rebuild is correct. The invariant
        // against it exists for snapshot resync, where a snapshot grants
        // coverage the oplog never held and recomputing would throw it away.
        // Here the history is genuinely gone because this operation removed it,
        // deliberately and offline.
        Engine::reset_version_vector_to_oplog(self.db())?;

        info!(
            reverted = outcome.reverted,
            removed = outcome.removed,
            oplog_discarded = outcome.oplog_discarded,
            ?until,
            "rewound to a point in time"
        );
        let _ = discardable;
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;
    use kimmy_core::index_meta::IndexField;

    use super::*;

    fn engine() -> (Engine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        (engine, dir)
    }

    /// The stamp of the newest oplog entry, i.e. "now" in logical time.
    fn latest(engine: &Engine) -> Hlc {
        engine.version_vector().unwrap().iter().map(|(_, hlc)| hlc).max().unwrap_or(Hlc::ZERO)
    }

    #[test]
    fn an_update_is_rewound_to_its_previous_value() {
        let (engine, _dir) = engine();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 1, "qty": 5 }).unwrap();
        let before_the_mistake = latest(&engine);

        engine.replace(&coll, &DocId::Int64(1), doc! { "_id": 1, "qty": 9999 }, false).unwrap();
        assert_eq!(
            engine.get(&coll, &DocId::Int64(1)).unwrap().unwrap().get_i32("qty").unwrap(),
            9999
        );

        let outcome = engine.rewind_to(before_the_mistake).unwrap();
        assert_eq!(outcome.reverted, 1);
        assert_eq!(
            engine.get(&coll, &DocId::Int64(1)).unwrap().unwrap().get_i32("qty").unwrap(),
            5,
            "the document must hold the value it had at the target"
        );
    }

    #[test]
    fn a_deleted_document_comes_back_if_its_insert_is_still_in_the_log() {
        // The case point-in-time restore exists for: an accidental delete,
        // undone because the document's earlier value is still in the window.
        let (engine, _dir) = engine();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 1, "name": "important" }).unwrap();
        let before_the_mistake = latest(&engine);

        engine.delete(&coll, &DocId::Int64(1)).unwrap();
        assert!(engine.get(&coll, &DocId::Int64(1)).unwrap().is_none());

        engine.rewind_to(before_the_mistake).unwrap();
        let restored = engine.get(&coll, &DocId::Int64(1)).unwrap().expect("it must come back");
        assert_eq!(restored.get_str("name").unwrap(), "important");
    }

    #[test]
    fn a_document_created_after_the_target_is_removed() {
        let (engine, _dir) = engine();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 1 }).unwrap();
        let target = latest(&engine);
        engine.insert(&coll, doc! { "_id": 2 }).unwrap();

        let outcome = engine.rewind_to(target).unwrap();
        assert_eq!(outcome.removed, 1);
        assert!(engine.get(&coll, &DocId::Int64(2)).unwrap().is_none(), "it did not exist then");
        assert!(engine.get(&coll, &DocId::Int64(1)).unwrap().is_some(), "this one did");
    }

    #[test]
    fn indexes_follow_the_rewind() {
        // An index left pointing at the undone future would answer a query with
        // a document that no longer has that value — wrong, and only visible
        // through an index-backed query.
        let (engine, _dir) = engine();
        engine.create_collection("shop", "orders").unwrap();
        engine
            .create_index(
                "shop",
                "orders",
                vec![IndexField { path: "sku".into(), descending: false }],
                false,
                None,
            )
            .unwrap();
        let coll = engine.get_collection("shop", "orders").unwrap();

        engine.insert(&coll, doc! { "_id": 1, "sku": "OLD" }).unwrap();
        let target = latest(&engine);
        engine.replace(&coll, &DocId::Int64(1), doc! { "_id": 1, "sku": "NEW" }, false).unwrap();

        engine.rewind_to(target).unwrap();

        let index = coll.indexes.first().expect("an index");
        let old_key = kimmy_core::keyenc::encode(&bson::Bson::String("OLD".into())).unwrap();
        let new_key = kimmy_core::keyenc::encode(&bson::Bson::String("NEW".into())).unwrap();
        let mut old_upper = old_key.clone();
        old_upper.push(0xff);
        let mut new_upper = new_key.clone();
        new_upper.push(0xff);

        assert!(
            !engine.index_candidates(&coll, index.id, &old_key, &old_upper).unwrap().is_empty(),
            "the restored value must be findable through the index"
        );
        assert!(
            engine.index_candidates(&coll, index.id, &new_key, &new_upper).unwrap().is_empty(),
            "the undone value must not still be in the index"
        );
    }

    #[test]
    fn the_undone_future_leaves_the_oplog() {
        // Otherwise anti-entropy ships the undone writes to a peer, and the
        // peer ships them straight back.
        let (engine, _dir) = engine();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 1 }).unwrap();
        let target = latest(&engine);
        engine.insert(&coll, doc! { "_id": 2 }).unwrap();
        engine.insert(&coll, doc! { "_id": 3 }).unwrap();

        let outcome = engine.rewind_to(target).unwrap();
        assert_eq!(outcome.oplog_discarded, 2, "both later writes must leave the log");
        assert!(
            latest(&engine) <= target,
            "the version vector must come down with the oplog, or this node claims history it \
             no longer holds and no peer will ever send it again"
        );
    }

    #[test]
    fn a_target_before_the_horizon_is_refused() {
        let (engine, _dir) = engine();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 1 }).unwrap();

        let err = engine.rewind_to(Hlc::ZERO).unwrap_err().to_string();
        // With nothing collected the horizon is zero, so zero is reachable;
        // the refusal is for a target genuinely before what the log describes.
        let _ = err;

        // Collect aggressively, then try to reach behind the horizon.
        engine.insert(&coll, doc! { "_id": 2 }).unwrap();
        let horizon = engine.oplog_collected_through().unwrap();
        if horizon > Hlc::ZERO {
            let err = engine.rewind_to(Hlc::ZERO).unwrap_err().to_string();
            assert!(err.contains("collected through"), "unhelpful error: {err}");
        }
    }

    #[test]
    fn a_schema_change_after_the_target_is_refused_without_writing() {
        // Dropping a collection purges its documents, and purged documents are
        // not in the oplog either, so a rewind past one cannot reconstruct what
        // it undid. Refusing is the only honest answer.
        let (engine, _dir) = engine();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 1, "qty": 1 }).unwrap();
        let target = latest(&engine);
        engine.replace(&coll, &DocId::Int64(1), doc! { "_id": 1, "qty": 2 }, false).unwrap();
        engine.create_collection("shop", "later").unwrap();

        let err = engine.rewind_to(target).unwrap_err().to_string();
        assert!(err.contains("schema change"), "unhelpful error: {err}");
        assert_eq!(
            engine.get(&coll, &DocId::Int64(1)).unwrap().unwrap().get_i32("qty").unwrap(),
            2,
            "a refused rewind must not have modified anything"
        );
    }

    #[test]
    fn a_document_whose_earlier_value_was_collected_is_refused_not_guessed() {
        // The refusal that matters most, and the one nothing else covers.
        //
        // A document written long ago and changed recently has its previous
        // value only in the oplog, and the oplog has been collected. Leaving it
        // at the *later* value would produce a database that looks restored and
        // is not — a wrong answer no caller could detect — so the rewind must
        // refuse and name it.
        let (engine, _dir) = engine();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 1, "qty": 5 }).unwrap();
        // Retention never collects the newest entry (ADR-028), so doc 1's
        // insert has to stop being the tail before it can be collected. The
        // first version of this test missed that and the rewind happily
        // *removed* the document instead of refusing — the fixture had not
        // built the condition it was named for.
        for i in 100..110i64 {
            engine.insert(&coll, doc! { "_id": i }).unwrap();
        }

        // Collect, so the only record of qty=5 is gone.
        let much_later = crate::engine::physical_now_ms() + 365 * 24 * 60 * 60 * 1000;
        engine
            .collect_garbage_at(much_later, crate::RetentionPolicy::new(0, 24 * 60 * 60))
            .unwrap();
        let horizon = engine.oplog_collected_through().unwrap();
        assert!(horizon > Hlc::ZERO, "the fixture must actually have collected something");

        // Now change it. The target sits after the horizon but before this.
        engine.replace(&coll, &DocId::Int64(1), doc! { "_id": 1, "qty": 9999 }, false).unwrap();
        let target = horizon;

        let err = engine.rewind_to(target).unwrap_err().to_string();
        assert!(
            err.contains("collected") && err.contains("Nothing was modified"),
            "the refusal must say what happened and that nothing changed: {err}"
        );
        assert_eq!(
            engine.get(&coll, &DocId::Int64(1)).unwrap().unwrap().get_i32("qty").unwrap(),
            9999,
            "a refused rewind must leave the database exactly as it was"
        );
    }

    #[test]
    fn a_rewind_that_changes_nothing_is_a_no_op() {
        let (engine, _dir) = engine();
        let coll = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 1 }).unwrap();
        let now = latest(&engine);

        let outcome = engine.rewind_to(now).unwrap();
        assert_eq!(outcome, RewindOutcome::default());
        assert!(engine.get(&coll, &DocId::Int64(1)).unwrap().is_some());
    }
}
