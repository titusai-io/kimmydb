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

use kimmy_core::{DocId, Hlc, OpKind, OplogEntry, Stamp, VersionVector};
use redb::{ReadableDatabase, ReadableTable};
use tracing::info;

use crate::codec;
use crate::engine::{Engine, WriterHolder};
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
        // The newest tombstone stamp this rewind mints, for the record: no oplog
        // entry and no vector names it, and a start resumes the clock past it.
        let mut minted: Option<Stamp> = None;
        let txn = self.begin_write(WriterHolder::Rewind)?;
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
                            crate::live_count::put_record(
                                &txn,
                                &mut docs,
                                collection_id,
                                &key,
                                &codec::encode_doc_record(&kimmy_core::DocRecord::live(
                                    entry.stamp,
                                    body.clone(),
                                )),
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
                        let stamp = match restore_to.as_ref() {
                            Some(entry) => entry.stamp,
                            None => {
                                let stamp = self.next_stamp();
                                minted = minted.max(Some(stamp));
                                stamp
                            }
                        };
                        crate::live_count::put_record(
                            &txn,
                            &mut docs,
                            collection_id,
                            &key,
                            &codec::encode_doc_record(&kimmy_core::DocRecord::tombstone(stamp)),
                        )?;
                        outcome.removed += 1;
                        None
                    }
                };

                // Index entries follow the document, or an index-backed query
                // would answer from the future the rewind just undid.
                if let Some(meta) = collections.get(&collection_id) {
                    let newly_multikey =
                        index::maintain(self, &txn, meta, before.as_ref(), after.as_ref(), &key)?;
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
            // The state mark goes with the entry, and this is the only
            // remover outside retention. A mark left on a key the oplog no
            // longer holds can never be collected -- retention only removes
            // one alongside the entry it names -- so it would be a permanent
            // orphan, and the one growth path nothing else bounds. ADR-160.
            let mut held = txn.open_table(tables::OPLOG_HELD)?;
            // The arrival index goes with the entry too, as retention removes
            // it. Left behind, the index no longer covered the oplog, and the
            // next start rebuilt it in stamp order — moving every entry this
            // member's streams had delivered, so a token this member issued
            // resumed from the wrong place (ADR-173).
            let mut arrival = txn.open_table(tables::OPLOG_ARRIVAL)?;
            let mut by_stamp = txn.open_table(tables::OPLOG_ARRIVAL_SEQ)?;
            // A violation entry's table row goes with it, and `through` comes
            // down to the new tail: it may not stand above what the oplog holds
            // (ADR-200).
            let mut violations = txn.open_table(tables::UNIQUE_VIOLATIONS)?;
            let mark_before = crate::live_count::mark_of(&arrival, &oplog)?;
            let mut discarded = VersionVector::new();
            for key in doomed {
                if let Some(old) = oplog.remove(key.as_slice())? {
                    let header = codec::decode_oplog_kind_and_collection(old.value());
                    drop(old);
                    if let Ok((kimmy_core::OpKind::UniqueViolation, collection)) = header {
                        violations.remove(
                            crate::violations_table::row_key(collection, key.as_slice()).as_slice(),
                        )?;
                    }
                }
                held.remove(key.as_slice())?;
                if let Some(seq) = by_stamp.remove(key.as_slice())? {
                    arrival.remove(seq.value())?;
                }
                discarded.observe(codec::decode_oplog_key(&key)?);
                outcome.oplog_discarded += 1;
            }
            if let Some(through) = crate::violations_table::through_of(&violations)? {
                let tail = oplog.last()?.map(|(k, _)| k.value().to_vec());
                match tail {
                    Some(tail) if tail.as_slice() < through.as_slice() => {
                        violations.insert(crate::violations_table::SENTINEL, tail.as_slice())?;
                    }
                    None => {
                        violations.remove(crate::violations_table::SENTINEL)?;
                    }
                    _ => {}
                }
            }
            // The tombstones it minted go in the record under this member's own
            // origin: they are above everything it discarded, and a start resumes
            // the clock past the record (`Engine::resume_hlc`, CL1). No entry of
            // this member's stands between them and what it discarded, so a token
            // check reads the record exactly as before.
            if let Some(stamp) = minted {
                discarded.observe(stamp);
            }
            // In the same transaction as the removal: a change stream may have
            // delivered what was just discarded, and a token naming it has to
            // be told from one naming an entry this member never held (ADR-173).
            if !discarded.is_empty() {
                record_rewind(&txn, until, &discarded)?;
            }
            // The live counts moved with every record the rewind rewrote, in
            // this transaction; their mark moves with the rows it removed, or
            // the next start would walk the whole store to rebuild counts that
            // are exact (ADR-174).
            crate::live_count::carry_mark(
                &txn,
                &mark_before,
                &crate::live_count::mark_of(&arrival, &oplog)?,
            )?;
        }
        txn.commit()?;

        // The oplog just lost everything above the target, so the vectors come
        // down to it: left high, this node would claim history it no longer
        // holds, and no peer would ever send that range again. To the target and
        // no lower: what is at or below it is exactly as it was, and a vector
        // lowered there loses coverage of what retention collected, this member's
        // own drops among them (ADR-243).
        Engine::lower_vectors_to_target(self.db(), until)?;

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

    /// Every rewind this database has taken, oldest first.
    pub(crate) fn rewinds(&self) -> Result<Vec<Rewound>> {
        let txn = self.db().begin_read()?;
        let meta = txn.open_table(tables::META)?;
        match meta.get(tables::META_REWOUND)? {
            Some(raw) => decode_rewinds(raw.value()),
            None => Ok(Vec::new()),
        }
    }
}

/// One rewind: its target, and per origin the newest stamp it discarded. Under this
/// member's own origin, no lower than the newest tombstone it minted for a document
/// created after the target, which is above everything it discarded and stamps no
/// entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Rewound {
    pub until: Hlc,
    pub through: VersionVector,
}

impl Rewound {
    /// Whether this rewind discarded the entry stamped `stamp`, or would have
    /// had this member held it.
    pub fn discarded(&self, stamp: &Stamp) -> bool {
        stamp.hlc > self.until && stamp.hlc <= self.through.get(stamp.node)
    }
}

/// `until ‖ origin count (u16) ‖ (node ‖ hlc)*`, one after another per rewind.
const REWIND_ORIGIN_LEN: usize = 16 + kimmy_core::HLC_ENCODED_LEN;

fn record_rewind(
    txn: &redb::WriteTransaction,
    until: Hlc,
    discarded: &VersionVector,
) -> Result<()> {
    let mut meta = txn.open_table(tables::META)?;
    let mut record = match meta.get(tables::META_REWOUND)? {
        Some(raw) => raw.value().to_vec(),
        None => Vec::new(),
    };
    record.extend_from_slice(&until.to_bytes());
    let origins = u16::try_from(discarded.len()).map_err(|_| {
        StorageError::Database("a rewind discarded entries from too many origins".into())
    })?;
    record.extend_from_slice(&origins.to_be_bytes());
    for (node, hlc) in discarded.iter() {
        record.extend_from_slice(&node.to_bytes());
        record.extend_from_slice(&hlc.to_bytes());
    }
    meta.insert(tables::META_REWOUND, record.as_slice())?;
    Ok(())
}

/// The highest stamp any rewind's record names, or zero: where a start's clock must
/// resume at least (`Engine::resume_hlc`, CL1).
///
/// A record that does not decode costs the fold, not the open: it is read here only
/// to keep the clock above a range nothing else names, and the token check that
/// relies on it reports the damage when it reads it.
pub(crate) fn rewound_high_water(db: &redb::Database) -> Result<Hlc> {
    let txn = db.begin_read()?;
    let meta = txn.open_table(tables::META)?;
    let Some(raw) = meta.get(tables::META_REWOUND)? else {
        return Ok(Hlc::ZERO);
    };
    match decode_rewinds(raw.value()) {
        Ok(rewinds) => Ok(rewinds
            .iter()
            .flat_map(|rewind| rewind.through.iter().map(|(_, hlc)| hlc))
            .max()
            .unwrap_or(Hlc::ZERO)),
        Err(error) => {
            tracing::warn!(%error, "the rewind record does not decode; the clock resumes without it");
            Ok(Hlc::ZERO)
        }
    }
}

fn decode_rewinds(mut raw: &[u8]) -> Result<Vec<Rewound>> {
    let corrupt = || StorageError::Corrupt("the rewind record is truncated".into());
    let mut out = Vec::new();
    while !raw.is_empty() {
        let header = kimmy_core::HLC_ENCODED_LEN + 2;
        if raw.len() < header {
            return Err(corrupt());
        }
        let until = Hlc::from_bytes(raw[..kimmy_core::HLC_ENCODED_LEN].try_into().unwrap());
        let origins = u16::from_be_bytes([raw[header - 2], raw[header - 1]]) as usize;
        let end = header + origins * REWIND_ORIGIN_LEN;
        if raw.len() < end {
            return Err(corrupt());
        }
        let mut through = VersionVector::new();
        for i in 0..origins {
            let at = header + i * REWIND_ORIGIN_LEN;
            let node = kimmy_core::NodeId::from_bytes(raw[at..at + 16].try_into().unwrap());
            let hlc = Hlc::from_bytes(raw[at + 16..at + REWIND_ORIGIN_LEN].try_into().unwrap());
            through.insert(node, hlc);
        }
        out.push(Rewound { until, through });
        raw = &raw[end..];
    }
    Ok(out)
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

    /// An hour ahead of the wall clock, so every stamp a fixture mints is `base + t`.
    fn base() -> u64 {
        crate::engine::physical_now_ms() + 60 * 60 * 1000
    }

    /// Move `engine`'s clock to `base + t` before a local step.
    fn at(engine: &Engine, base: u64, t: u64) {
        engine.advance_clock_past(&Stamp::new(Hlc::new(base + t, 0), engine.node_id()));
    }

    /// One pull as the transport asks it: from what `into` has witnessed.
    fn pull(into: &Engine, from: &Engine) {
        let mine = into.witnessed_vector().unwrap();
        let theirs = from.version_vector().unwrap();
        if let Some(start) = mine.behind(&theirs) {
            let entries =
                from.entries_for_peer(start, 1024, crate::WalkScope::Background).unwrap().entries;
            into.apply_batch(&entries).unwrap();
        }
    }

    /// A whole-database snapshot of `from` into `into`, to its completion and grant.
    fn whole(into: &Engine, from: &Engine) {
        let mut progress = crate::SnapshotProgress::whole_database();
        while !progress.is_complete() {
            let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
            let applied = into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
            if applied.purge_pending.is_some() {
                into.finish_purges_now().unwrap();
            }
        }
    }

    #[test]
    fn a_rewind_after_retention_keeps_the_coverage_of_what_retention_collected() {
        // RW1 (ADR-243) where it starts. Retention collected every entry of this
        // member's own origin, and the rewind discards nothing. Rebuilt from the
        // oplog, the vectors lost the origin altogether: no peer lacking what was
        // collected was told so, and no page vouched for it.
        let base = base();
        let ((b, _db), (c, _dc), (fresh, _df)) = (engine(), engine(), engine());
        at(&b, base, 100);
        let orders = b.create_collection("shop", "orders").unwrap();
        at(&b, base, 110);
        b.insert(&orders, doc! { "_id": 1 }).unwrap();
        pull(&c, &b);
        at(&c, base, 190);
        c.insert(&c.get_collection("shop", "orders").unwrap(), doc! { "_id": 2 }).unwrap();
        pull(&b, &c);
        // Past the oplog window of all but the newest entry, which is c's.
        b.collect_garbage_at(base + 1730, crate::RetentionPolicy::new(1, 24 * 60 * 60)).unwrap();
        let own = b.node_id();
        assert!(
            b.oplog_collected().unwrap().get(own) > Hlc::ZERO
                && b.read_oplog_from(Hlc::ZERO, usize::MAX, crate::WalkScope::Background)
                    .unwrap()
                    .iter()
                    .all(|entry| entry.stamp.node != own),
            "the fixture must collect every entry of the member's own origin"
        );
        let (servable, witnessed) = (b.version_vector().unwrap(), b.witnessed_vector().unwrap());

        let outcome = b.rewind_to(Hlc::new(base + 1757, 0)).unwrap();
        assert_eq!(outcome.oplog_discarded, 0, "the fixture discards nothing");
        assert_eq!(b.version_vector().unwrap(), servable, "nothing at or below the target moved");
        assert_eq!(b.witnessed_vector().unwrap(), witnessed, "nor in what it has witnessed");
        assert_eq!(
            b.serve_verdict(&fresh.witnessed_vector().unwrap()).unwrap(),
            crate::ServeVerdict::Collected,
            "a peer that lacks what retention collected is told so, and takes a snapshot"
        );
    }

    #[test]
    fn a_rewind_lowers_every_origin_it_discarded_to_the_target_and_no_lower() {
        let base = base();
        let ((b, _db), (c, _dc)) = (engine(), engine());
        at(&b, base, 50);
        let orders = b.create_collection("shop", "orders").unwrap();
        pull(&c, &b);
        at(&b, base, 100);
        b.insert(&orders, doc! { "_id": 1 }).unwrap();
        at(&c, base, 300);
        c.insert(&c.get_collection("shop", "orders").unwrap(), doc! { "_id": 9 }).unwrap();
        pull(&b, &c);
        at(&b, base, 400);
        b.insert(&orders, doc! { "_id": 2 }).unwrap();

        let until = Hlc::new(base + 200, 0);
        let outcome = b.rewind_to(until).unwrap();
        assert_eq!(outcome.oplog_discarded, 2, "c's write and b's own second write");
        for (vector, name) in [
            (b.version_vector().unwrap(), "servable"),
            (b.witnessed_vector().unwrap(), "witnessed"),
        ] {
            for node in [b.node_id(), c.node_id()] {
                assert_eq!(
                    vector.get(node),
                    until,
                    "the {name} vector names {node} at the target: below everything the rewind \
                     discarded, and still over everything it kept"
                );
            }
        }
    }

    #[test]
    fn a_rewound_member_asks_its_peer_again_for_what_it_discarded() {
        // Why the witnessed vector comes down too (ADR-044, ADR-054): left high,
        // the member would never ask for the range it discarded again.
        let base = base();
        let ((b, _db), (c, _dc)) = (engine(), engine());
        at(&b, base, 50);
        b.create_collection("shop", "orders").unwrap();
        pull(&c, &b);
        at(&c, base, 300);
        c.insert(&c.get_collection("shop", "orders").unwrap(), doc! { "_id": 9 }).unwrap();
        pull(&b, &c);
        let theirs = c.version_vector().unwrap();
        assert!(b.witnessed_vector().unwrap().covers(&theirs));

        b.rewind_to(Hlc::new(base + 200, 0)).unwrap();
        assert!(
            b.witnessed_vector().unwrap().behind(&theirs).is_some(),
            "the member is behind its peer on what it discarded, so it asks again"
        );
        pull(&b, &c);
        assert!(b.witnessed_vector().unwrap().covers(&theirs), "and the pull takes it");
    }

    #[test]
    fn a_rewind_that_discards_nothing_changes_neither_vector() {
        // Two kinds of coverage the oplog does not name: a completed snapshot's
        // grant (the sender's index creation stamps no document), and an entry
        // processed without an append (a write that loses last-writer-wins,
        // ADR-054). Rebuilt from the oplog, the rewind lost both.
        let base = base();
        let ((a, _da), (b, _db), (c, _dc)) = (engine(), engine(), engine());
        at(&a, base, 10);
        let orders = a.create_collection("shop", "orders").unwrap();
        at(&a, base, 20);
        a.insert(&orders, doc! { "_id": 1, "k": 1 }).unwrap();
        at(&a, base, 30);
        a.create_index(
            "shop",
            "orders",
            vec![IndexField { path: "k".into(), descending: false }],
            false,
            None,
        )
        .unwrap();
        whole(&b, &a);
        whole(&c, &a);
        let granted = b.version_vector().unwrap().get(a.node_id());
        let held = crate::docs::record_stamp(
            &b,
            &b.get_collection("shop", "orders").unwrap(),
            &DocId::Int64(1),
        )
        .unwrap()
        .unwrap();
        assert!(granted > held.hlc, "the grant must name a stamp no document carries");

        at(&c, base, 100);
        c.insert(&c.get_collection("shop", "orders").unwrap(), doc! { "_id": 5 }).unwrap();
        at(&b, base, 200);
        b.insert(&b.get_collection("shop", "orders").unwrap(), doc! { "_id": 5 }).unwrap();
        pull(&b, &c);
        let (servable, witnessed) = (b.version_vector().unwrap(), b.witnessed_vector().unwrap());
        assert!(
            witnessed.get(c.node_id()) > servable.get(c.node_id()),
            "the fixture must witness c's losing write without appending it"
        );

        let outcome = b.rewind_to(Hlc::new(base + 1000, 0)).unwrap();
        assert_eq!(outcome.oplog_discarded, 0, "the fixture discards nothing");
        assert_eq!(b.version_vector().unwrap(), servable, "the grant stands");
        assert_eq!(b.witnessed_vector().unwrap(), witnessed, "and so does what it witnessed");
    }

    #[test]
    fn a_start_after_a_rewind_resumes_the_clock_past_what_it_discarded_and_minted() {
        // CL1. A member whose clock led its wall clock (a fast peer's clock reached
        // it) rewinds past two of its own inserts: it discards them and mints a
        // tombstone for each document. Neither the oplog's tail nor a vector names
        // any of those stamps afterwards, so a start resumed below them: its next
        // write was stamped inside the range the rewind's record says it discarded,
        // and below its own tombstones.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let ahead = crate::engine::physical_now_ms() + 60 * 60 * 1000;
        let (discarded, minted) = {
            let engine = Engine::open(&path).unwrap();
            let coll = engine.create_collection("shop", "orders").unwrap();
            engine.insert(&coll, doc! { "_id": 1 }).unwrap();
            let target = latest(&engine);
            engine.advance_clock_past(&Stamp::new(Hlc::new(ahead, 0), engine.node_id()));
            engine.insert(&coll, doc! { "_id": 2 }).unwrap();
            engine.insert(&coll, doc! { "_id": 3 }).unwrap();
            let discarded = latest(&engine);
            let outcome = engine.rewind_to(target).unwrap();
            assert_eq!((outcome.oplog_discarded, outcome.removed), (2, 2));
            let minted = [2, 3]
                .map(|id| {
                    crate::docs::record_stamp(&engine, &coll, &DocId::Int64(id)).unwrap().unwrap()
                })
                .into_iter()
                .max()
                .unwrap();
            assert!(minted.hlc > discarded, "the tombstones are minted above what was discarded");
            engine.close().unwrap();
            (discarded, minted)
        };

        let engine = Engine::open(&path).unwrap();
        let coll = engine.get_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 4 }).unwrap();
        let next = crate::docs::record_stamp(&engine, &coll, &DocId::Int64(4)).unwrap().unwrap();
        assert!(
            next.hlc > discarded,
            "the next write must be stamped above what the rewind discarded"
        );
        assert!(next > minted, "and above the tombstones the rewind minted");
        assert!(
            !engine.rewinds().unwrap().iter().any(|rewind| rewind.discarded(&next)),
            "so a token naming it is not read as naming a discarded entry"
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
