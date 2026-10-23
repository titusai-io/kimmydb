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
//!   this member's oplog does not hold, collected, or held only as a snapshot
//!   page's state.
//!
//! Only entries of the collection's **standing life** count, on either side of
//! `T`. A collection's id is derived from its name, so a drop and a re-creation
//! leave one id, and the purge removes the dropped life's documents but not its
//! entries. An entry at or below the collection's tombstone or its standing
//! life's incarnation floor is of a life a drop ended, which the apply path
//! turns away (`Engine::is_history`), and the rewind never puts it back
//! (ADR-248).
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
//! reconstruct. A drop counts whether the oplog holds its entry or this member
//! holds it only as a tombstone or a floor, as a snapshot page leaves it.
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

        // Each collection standing here, keyed by the id the oplog refers to:
        // its incarnation floor judges the history below, and index maintenance
        // needs its definitions.
        let mut collections: HashMap<u64, crate::CollectionMeta> = HashMap::new();
        for db_meta in self.list_databases()? {
            for meta in self.list_collections(&db_meta.name)? {
                collections.insert(meta.id.0, meta);
            }
        }
        // A drop this member knows of only by its tombstone or by a floor, one a
        // snapshot page carried, is a schema change as much as one whose entry
        // the oplog holds, and a drop after the target is refused however it
        // arrived. Strictly after: a drop at the target is part of the state
        // at the target (ADR-248).
        let floors_after = collections
            .values()
            .any(|meta| meta.incarnation_floor.is_some_and(|floor| floor > until));
        if floors_after {
            schema_changes.push(OpKind::DropCollection);
        }
        // Whether an entry is of a life a drop ended: the apply path's own
        // rule, the collection's tombstone and its standing life's floor
        // (`Engine::is_history`), so the rewind puts back only what an entry
        // arriving here would be applied as (ADR-248).
        let mut memo = crate::sync::Memo::default();

        {
            let txn = self.db().begin_read()?;
            let dropped = txn.open_table(tables::COLLECTIONS_DROPPED)?;
            for row in dropped.iter()? {
                let (_, raw) = row?;
                if codec::decode_oplog_key(raw.value())?.hlc > until {
                    schema_changes.push(OpKind::DropCollection);
                    break;
                }
            }
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
                // An entry of a life a drop ended is no part of the document's
                // history in the life standing here, on either side of the
                // target: its collection id is derived from the name, so it is
                // the same in every life, and the purge removes a life's
                // documents but not its entries.
                if self.is_history(
                    &mut memo,
                    collections.get(&entry.collection.0),
                    entry.collection,
                    entry.stamp,
                )? {
                    continue;
                }
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
                "cannot rewind: {total} document(s) changed after the target, and the value each \
                 had at the target is not in this member's oplog: retention has collected it, or \
                 this member holds the document only as a snapshot page's state, or its entries \
                 at or below the target are of a life a drop has ended. Nothing was modified. \
                 Affected: {}{}",
                named.join(", "),
                if more > 0 { format!(" and {more} more") } else { String::new() }
            )));
        }

        // Everything is decided. Now write.
        let mut outcome = RewindOutcome::default();
        // The lowest stamp among the entries the rewind restores: the vectors are
        // bounded by it as well (`Engine::lower_vectors_in`).
        let mut restored_floor: Option<Hlc> = None;
        // Whether the rewind removed a document it holds no entry of at or below
        // the target: the vectors are then bounded by the horizon as well.
        let mut removed_any = false;
        let schema = Engine::schema_for_record(self.db())?;
        let txn = self.begin_write(WriterHolder::Rewind)?;
        {
            let mut docs = txn.open_table(tables::DOCS)?;

            for ((collection_id, key), restore_to) in plan {
                if let Some(entry) = &restore_to {
                    restored_floor = Some(match restored_floor {
                        Some(floor) => floor.min(entry.stamp.hlc),
                        None => entry.stamp.hlc,
                    });
                }
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
                    // Deleted at the target: the delete's own tombstone, at its own
                    // stamp, at or below the target.
                    Some(entry) => {
                        crate::live_count::put_record(
                            &txn,
                            &mut docs,
                            collection_id,
                            &key,
                            &codec::encode_doc_record(&kimmy_core::DocRecord::tombstone(
                                entry.stamp,
                            )),
                        )?;
                        outcome.removed += 1;
                        None
                    }
                    // Not yet created, as far as this member's oplog says: the row
                    // goes, and no tombstone takes its place. The member can have
                    // judged a write of the document at or below the target against
                    // the insert it discards, a write it never appended; a tombstone
                    // would outrank that write wherever it is kept, and any stamp
                    // one could carry either does that or is another write's
                    // (ADR-246).
                    None => {
                        crate::live_count::remove_record(&txn, &mut docs, collection_id, &key)?;
                        removed_any = true;
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
        // The oplog just lost everything above the target, so the vectors come
        // down to it, in this transaction: left high, this node would claim history
        // it no longer holds, and no peer would ever send that range again; lowered
        // in a transaction of their own, a failure between the two would leave them
        // so. To the target, and below it only as far as a restored or removed
        // document asks, and never below what retention collected (ADR-243). A
        // removed document asks for every write of it above the horizon the member
        // may have judged, and no lower: below the horizon a re-fetch can bring
        // back an insert whose delete and tombstone retention collected here, from
        // a peer whose own pass has not run yet (ADR-246).
        let floor = if removed_any {
            Some(restored_floor.map_or(horizon, |floor| floor.min(horizon)))
        } else {
            restored_floor
        };
        Engine::lower_vectors_in(&txn, schema, until, floor)?;
        txn.commit()?;

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

/// One rewind: its target, and per origin the newest stamp it discarded.
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
/// resume at least (`Engine::resume_hlc`).
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

/// A failure injected in the vectors' lowering, to show the rewind is one transaction.
#[cfg(test)]
pub(crate) mod test_support {
    use std::cell::Cell;

    thread_local! {
        static FAIL_THE_LOWERING: Cell<bool> = const { Cell::new(false) };
    }

    pub(crate) fn fail_the_lowering(on: bool) {
        FAIL_THE_LOWERING.with(|f| f.set(on));
    }

    pub(crate) fn fails_the_lowering() -> bool {
        FAIL_THE_LOWERING.with(|f| f.get())
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
        engine.insert(&coll, doc! { "_id": 1, "sku": "KEPT" }).unwrap();
        let target = latest(&engine);
        engine.insert(&coll, doc! { "_id": 2, "sku": "LATER" }).unwrap();

        let outcome = engine.rewind_to(target).unwrap();
        assert_eq!(outcome.removed, 1);
        assert!(engine.get(&coll, &DocId::Int64(2)).unwrap().is_none(), "it did not exist then");
        assert!(engine.get(&coll, &DocId::Int64(1)).unwrap().is_some(), "this one did");
        // No tombstone takes its place: one would outrank a write of the document at
        // or below the target that a peer keeps (ADR-246).
        assert_eq!(
            crate::docs::record_stamp(&engine, &coll, &DocId::Int64(2)).unwrap(),
            None,
            "the removed document leaves no record"
        );
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(1), "the count follows it");
        crate::live_count::test_support::assert_counts_exact(&engine, "a removal");
        let index = coll.indexes.first().expect("an index");
        let key = |v: &str| {
            let low = kimmy_core::keyenc::encode(&bson::Bson::String(v.into())).unwrap();
            let mut high = low.clone();
            high.push(0xff);
            engine.index_candidates(&coll, index.id, &low, &high).unwrap()
        };
        assert!(key("LATER").is_empty(), "the removed document leaves the index");
        assert!(!key("KEPT").is_empty(), "the kept one stays in it");
    }

    /// Rewind is per key: two subtypes of one byte string are two documents, so the
    /// one created after the target is removed, leaving no record, and the other
    /// goes back to its value at the target (ADR-251).
    #[test]
    fn two_subtypes_of_one_byte_string_are_rewound_apart() {
        let (engine, _dir) = engine();
        let coll = engine.create_collection("shop", "bins").unwrap();
        let binary = |subtype: u8| {
            bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::from(subtype),
                bytes: (0u8..16).collect(),
            })
        };
        let id = |subtype: u8| DocId::try_from_bson(&binary(subtype)).unwrap();
        engine.insert(&coll, doc! { "_id": binary(0), "v": 1 }).unwrap();
        let target = latest(&engine);
        engine.insert(&coll, doc! { "_id": binary(4), "v": 1 }).unwrap();
        engine.replace(&coll, &id(0), doc! { "_id": binary(0), "v": 2 }, false).unwrap();

        let outcome = engine.rewind_to(target).unwrap();
        assert_eq!((outcome.removed, outcome.reverted), (1, 1));
        assert!(engine.get(&coll, &id(4)).unwrap().is_none(), "it did not exist then");
        assert_eq!(
            engine.get(&coll, &id(0)).unwrap().unwrap(),
            doc! { "_id": binary(0), "v": 1 },
            "the other subtype holds the value it had at the target"
        );
        assert_eq!(
            crate::docs::record_stamp(&engine, &coll, &id(4)).unwrap(),
            None,
            "the removed document leaves no tombstone"
        );
        assert_eq!(engine.count_by_id(coll.id).unwrap(), Some(1));
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
        // A rewind after a retention pass (ADR-243). Retention collected every
        // entry of this member's own origin, and the rewind discards nothing. Rebuilt from the
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
        // Both origins write document 5 after the target, which is the stamp of C's
        // insert of it: the rewind discards both writes and puts the document back
        // to that insert, so the bound under the restored entry is the target itself.
        let base = base();
        let ((b, _db), (c, _dc)) = (engine(), engine());
        at(&b, base, 50);
        let orders = b.create_collection("shop", "orders").unwrap();
        pull(&c, &b);
        let theirs = c.get_collection("shop", "orders").unwrap();
        at(&c, base, 150);
        c.insert(&theirs, doc! { "_id": 5, "v": "c" }).unwrap();
        let until = c.version_vector().unwrap().get(c.node_id());
        pull(&b, &c);
        at(&b, base, 300);
        b.replace(&orders, &DocId::Int64(5), doc! { "_id": 5, "v": "b" }, false).unwrap();
        pull(&c, &b);
        at(&c, base, 400);
        c.replace(&theirs, &DocId::Int64(5), doc! { "_id": 5, "v": "c2" }, false).unwrap();
        pull(&b, &c);

        let outcome = b.rewind_to(until).unwrap();
        assert_eq!(
            (outcome.oplog_discarded, outcome.reverted),
            (2, 1),
            "b's write and c's second write, and the document put back"
        );
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
        // grant (the sender's own write was overwritten by another origin, so the
        // page carries no entry of the sender's), and an entry processed without
        // an append (a write that loses last-writer-wins, ADR-054). Rebuilt from
        // the oplog, the rewind lost both.
        let base = base();
        let ((a, _da), (b, _db), (c, _dc)) = (engine(), engine(), engine());
        at(&a, base, 10);
        let orders = a.create_collection("shop", "orders").unwrap();
        at(&a, base, 20);
        a.insert(&orders, doc! { "_id": 1 }).unwrap();
        pull(&c, &a);
        at(&c, base, 25);
        let theirs = c.get_collection("shop", "orders").unwrap();
        c.replace(&theirs, &DocId::Int64(1), doc! { "_id": 1, "v": 2 }, false).unwrap();
        pull(&a, &c);
        whole(&b, &a);
        let granted = b.version_vector().unwrap().get(a.node_id());
        let entries =
            b.read_oplog_from(Hlc::ZERO, usize::MAX, crate::WalkScope::Background).unwrap();
        assert!(
            granted > Hlc::ZERO && entries.iter().all(|entry| entry.stamp.node != a.node_id()),
            "the grant must name a stamp of a's that no entry here carries: {entries:?}"
        );

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
    fn a_start_after_a_rewind_resumes_the_clock_past_what_it_discarded() {
        // The clock after a rewind. A member whose clock led its wall clock (a
        // fast peer's clock reached it) rewinds past two of its own inserts: it
        // discards them and removes both documents. Neither the oplog's tail nor a
        // vector names those stamps afterwards, so a start resumed below them: its
        // next write was stamped inside the range the rewind's record says it
        // discarded.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let ahead = crate::engine::physical_now_ms() + 60 * 60 * 1000;
        let discarded = {
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
            for id in [2, 3] {
                assert_eq!(
                    crate::docs::record_stamp(&engine, &coll, &DocId::Int64(id)).unwrap(),
                    None,
                    "a removed document leaves no record, so no stamp of the rewind's own"
                );
            }
            engine.close().unwrap();
            discarded
        };

        let engine = Engine::open(&path).unwrap();
        let coll = engine.get_collection("shop", "orders").unwrap();
        engine.insert(&coll, doc! { "_id": 4 }).unwrap();
        let next = crate::docs::record_stamp(&engine, &coll, &DocId::Int64(4)).unwrap().unwrap();
        assert!(
            next.hlc > discarded,
            "the next write must be stamped above what the rewind discarded"
        );
        assert!(
            !engine.rewinds().unwrap().iter().any(|rewind| rewind.discarded(&next)),
            "so a token naming it is not read as naming a discarded entry"
        );
    }

    #[test]
    fn a_start_after_a_rewind_resumes_past_a_peers_discarded_stamp() {
        // The rewind discards a peer's write stamped far ahead. A start that resumed
        // below it would stamp the member's next write of the document below the
        // peer's, which then wins when anti-entropy brings it back.
        let base = base();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kimmy.redb");
        let (c, _dc) = engine();
        let far = {
            let b = Engine::open(&path).unwrap();
            at(&b, base, 10);
            let orders = b.create_collection("shop", "orders").unwrap();
            at(&b, base, 20);
            b.insert(&orders, doc! { "_id": 1 }).unwrap();
            pull(&c, &b);
            at(&c, base, 100_000);
            c.replace(
                &c.get_collection("shop", "orders").unwrap(),
                &DocId::Int64(1),
                doc! { "_id": 1, "v": "c" },
                false,
            )
            .unwrap();
            let far = c.version_vector().unwrap().get(c.node_id());
            pull(&b, &c);
            let out = b.rewind_to(Hlc::new(base + 50, 0)).unwrap();
            assert_eq!(out.oplog_discarded, 1, "the peer's write");
            b.close().unwrap();
            far
        };
        let b = Engine::open(&path).unwrap();
        let orders = b.get_collection("shop", "orders").unwrap();
        b.replace(&orders, &DocId::Int64(1), doc! { "_id": 1, "v": "b" }, false).unwrap();
        let next = crate::docs::record_stamp(&b, &orders, &DocId::Int64(1)).unwrap().unwrap();
        assert!(
            next.hlc > far,
            "the next write must be stamped above the peer's discarded write: {next:?} vs {far:?}"
        );
    }

    #[test]
    fn a_rewind_that_removes_a_document_asks_again_for_a_write_it_judged() {
        // C's t150 insert of document 1 reaches B after B's own t300 insert of it,
        // loses last-writer-wins and is witnessed without an append. B's oplog holds
        // no entry of the document at or below t200, so the rewind removes it: with no
        // tombstone, which would outrank C's write, and with each vector bounded by
        // what it keeps and collected, so B asks C again by entries and takes the
        // write (ADR-246). Nothing of C's origin, nor of A's, was discarded.
        let base = base();
        let ((a, _da), (b, _db), (c, _dc)) = (engine(), engine(), engine());
        at(&a, base, 100);
        a.create_collection("shop", "orders").unwrap();
        pull(&b, &a);
        pull(&c, &a);
        let (mine, theirs) = (
            b.get_collection("shop", "orders").unwrap(),
            c.get_collection("shop", "orders").unwrap(),
        );
        at(&c, base, 150);
        c.insert(&theirs, doc! { "_id": 1, "v": "c" }).unwrap();
        let judged = crate::docs::record_stamp(&c, &theirs, &DocId::Int64(1)).unwrap().unwrap();
        at(&b, base, 300);
        b.insert(&mine, doc! { "_id": 1, "v": "b" }).unwrap();
        pull(&b, &c);
        assert!(
            b.witnessed_vector().unwrap().get(c.node_id()) >= judged.hlc,
            "premise: B witnessed C's write"
        );
        assert_eq!(
            b.get(&mine, &DocId::Int64(1)).unwrap().unwrap().get_str("v").unwrap(),
            "b",
            "premise: C's write lost on B"
        );
        let created = a.version_vector().unwrap().get(a.node_id());

        let outcome = b.rewind_to(Hlc::new(base + 200, 0)).unwrap();
        assert_eq!((outcome.removed, outcome.oplog_discarded), (1, 1));
        assert_eq!(
            crate::docs::record_stamp(&b, &mine, &DocId::Int64(1)).unwrap(),
            None,
            "no tombstone takes the document's place"
        );
        for (vector, name) in [
            (b.version_vector().unwrap(), "servable"),
            (b.witnessed_vector().unwrap(), "witnessed"),
        ] {
            assert_eq!(vector.get(c.node_id()), Hlc::ZERO, "{name}: C's origin keeps nothing here");
            assert_eq!(vector.get(b.node_id()), Hlc::ZERO, "{name}: B's own insert was discarded");
            assert_eq!(vector.get(a.node_id()), created, "{name}: A's at the entry it keeps");
        }
        pull(&b, &c);
        assert_eq!(
            crate::docs::record_stamp(&b, &mine, &DocId::Int64(1)).unwrap(),
            Some(judged),
            "B takes C's write again"
        );
    }

    #[test]
    fn a_rewind_that_removes_a_document_after_retention_keeps_the_coverage_of_what_retention_collected()
     {
        // Retention collects every entry of B's own origin; B then inserts a document
        // after the target, which the rewind removes. Bounded by what it keeps alone,
        // B's own origin would leave its coverage below what retention collected, and
        // a peer lacking a collected entry would no longer be told so (ADR-243).
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
        b.collect_garbage_at(base + 1730, crate::RetentionPolicy::new(1, 24 * 60 * 60)).unwrap();
        let own = b.node_id();
        let collected = b.oplog_collected().unwrap().get(own);
        assert!(
            collected > Hlc::ZERO
                && b.read_oplog_from(Hlc::ZERO, usize::MAX, crate::WalkScope::Background)
                    .unwrap()
                    .iter()
                    .all(|entry| entry.stamp.node != own),
            "the fixture must collect every entry of the member's own origin"
        );
        at(&b, base, 1760);
        b.insert(&orders, doc! { "_id": 7 }).unwrap();

        let outcome = b.rewind_to(Hlc::new(base + 1757, 0)).unwrap();
        assert_eq!((outcome.removed, outcome.oplog_discarded), (1, 1));
        for (vector, name) in [
            (b.version_vector().unwrap(), "servable"),
            (b.witnessed_vector().unwrap(), "witnessed"),
        ] {
            assert_eq!(
                vector.get(own),
                collected,
                "{name}: no lower than what retention collected"
            );
        }
        assert_eq!(
            b.serve_verdict(&fresh.witnessed_vector().unwrap()).unwrap(),
            crate::ServeVerdict::Collected,
            "a peer that lacks what retention collected is told so"
        );
    }

    #[test]
    fn a_rewind_that_restores_one_document_and_removes_another_is_bounded_by_the_removal() {
        // C inserts document 2 at t105, below A's t110 insert of document 1. B
        // updates document 1 at t300 and inserts document 2 at t310 before C's
        // insert reaches it, which then loses. The rewind to t200 puts document 1
        // back to t110 and removes document 2: bounded by the restored entry alone,
        // B's coverage of C would stay at t105 and C's write would never come again.
        let base = base();
        let ((a, _da), (b, _db), (c, _dc)) = (engine(), engine(), engine());
        at(&a, base, 100);
        let orders = a.create_collection("shop", "orders").unwrap();
        pull(&b, &a);
        pull(&c, &a);
        let (mine, theirs) = (
            b.get_collection("shop", "orders").unwrap(),
            c.get_collection("shop", "orders").unwrap(),
        );
        at(&c, base, 105);
        c.insert(&theirs, doc! { "_id": 2, "v": "c" }).unwrap();
        let judged = crate::docs::record_stamp(&c, &theirs, &DocId::Int64(2)).unwrap().unwrap();
        at(&a, base, 110);
        a.insert(&orders, doc! { "_id": 1 }).unwrap();
        pull(&b, &a);
        at(&b, base, 300);
        b.replace(&mine, &DocId::Int64(1), doc! { "_id": 1, "v": "b" }, false).unwrap();
        at(&b, base, 310);
        b.insert(&mine, doc! { "_id": 2, "v": "b" }).unwrap();
        pull(&b, &c);
        assert!(b.witnessed_vector().unwrap().get(c.node_id()) >= judged.hlc, "premise");

        let outcome = b.rewind_to(Hlc::new(base + 200, 0)).unwrap();
        assert_eq!((outcome.reverted, outcome.removed, outcome.oplog_discarded), (1, 1, 2));
        pull(&b, &c);
        assert_eq!(
            crate::docs::record_stamp(&b, &mine, &DocId::Int64(2)).unwrap(),
            Some(judged),
            "B takes C's write of the removed document again"
        );
    }

    #[test]
    fn a_rewind_of_a_restored_store_after_retention_keeps_the_coverage() {
        // `kimmyd restore --until`: a backup re-derives the witnessed vector and
        // the per-origin retention record at open; the rewind keeps both vectors.
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
        b.collect_garbage_at(base + 1730, crate::RetentionPolicy::new(1, 24 * 60 * 60)).unwrap();
        let (servable, witnessed) = (b.version_vector().unwrap(), b.witnessed_vector().unwrap());
        let mut backup = Vec::new();
        b.backup_to(&mut backup, crate::WalkScope::Request).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let restored = dir.path().join("kimmy.redb");
        crate::backup::restore(&restored, &mut backup.as_slice()).unwrap();
        let r = Engine::open(&restored).unwrap();
        r.rewind_to(Hlc::new(base + 1757, 0)).unwrap();
        assert_eq!(
            r.version_vector().unwrap(),
            servable,
            "servable kept through restore and rewind"
        );
        assert_eq!(r.witnessed_vector().unwrap(), witnessed, "witnessed kept");
        drop(r);
        let r = Engine::open(&restored).unwrap();
        assert_eq!(r.version_vector().unwrap(), servable, "and across a reopen");
        assert_eq!(
            r.serve_verdict(&fresh.witnessed_vector().unwrap()).unwrap(),
            crate::ServeVerdict::Collected
        );
    }

    #[test]
    fn a_rewind_that_fails_while_lowering_the_vectors_writes_nothing() {
        // The vectors are lowered in the rewind's own transaction. Lowered in a
        // second one, a failure between the two left the oplog shrunk and the
        // vectors above the target: the member claimed history it no longer held.
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
        let entries = b.oplog_entries().unwrap();
        let (servable, witnessed) = (b.version_vector().unwrap(), b.witnessed_vector().unwrap());

        test_support::fail_the_lowering(true);
        let failed = b.rewind_to(Hlc::new(base + 200, 0));
        test_support::fail_the_lowering(false);
        assert!(failed.is_err(), "the injected failure");
        assert_eq!(b.oplog_entries().unwrap(), entries, "no entry was discarded");
        assert!(b.get(&orders, &DocId::Int64(9)).unwrap().is_some(), "no document was removed");
        assert_eq!(b.version_vector().unwrap(), servable);
        assert_eq!(b.witnessed_vector().unwrap(), witnessed);

        let outcome = b.rewind_to(Hlc::new(base + 200, 0)).unwrap();
        assert_eq!(outcome.oplog_discarded, 1);
        // The rewind removes c's insert, so c's coverage comes down to what the
        // rewind keeps of it: nothing (ADR-246).
        assert_eq!(b.version_vector().unwrap().get(c.node_id()), Hlc::ZERO);
    }

    #[test]
    fn a_rewind_that_restores_a_document_bounds_the_vectors_by_it_and_by_what_it_keeps() {
        // B holds A's t110 write of document 1. C's t150 write of it reaches B
        // after B's own t300 write, loses last-writer-wins and is witnessed without
        // an append; C's t180 write of document 2 is appended. The rewind to t200
        // puts document 1 back to t110, so each vector comes down to at most
        // t110 (B's own origin, which keeps nothing) or to the newest entry it
        // keeps of the origin (C's t180), never below it (invariant I).
        let base = base();
        let ((a, _da), (b, _db), (c, _dc)) = (engine(), engine(), engine());
        at(&a, base, 100);
        let orders = a.create_collection("shop", "orders").unwrap();
        at(&a, base, 110);
        a.insert(&orders, doc! { "_id": 1 }).unwrap();
        pull(&b, &a);
        pull(&c, &a);
        let theirs = c.get_collection("shop", "orders").unwrap();
        at(&c, base, 150);
        c.replace(&theirs, &DocId::Int64(1), doc! { "_id": 1, "v": "c" }, false).unwrap();
        at(&c, base, 180);
        c.insert(&theirs, doc! { "_id": 2 }).unwrap();
        let mine = b.get_collection("shop", "orders").unwrap();
        at(&b, base, 300);
        b.replace(&mine, &DocId::Int64(1), doc! { "_id": 1, "v": "b" }, false).unwrap();
        pull(&b, &c);
        let restored = crate::docs::record_stamp(&a, &orders, &DocId::Int64(1)).unwrap().unwrap();
        let kept = crate::docs::record_stamp(&c, &theirs, &DocId::Int64(2)).unwrap().unwrap();
        assert!(
            b.witnessed_vector().unwrap().get(c.node_id()) >= kept.hlc,
            "the fixture must witness C's losing write below C's kept one"
        );

        let outcome = b.rewind_to(Hlc::new(base + 200, 0)).unwrap();
        assert_eq!((outcome.reverted, outcome.oplog_discarded), (1, 1));
        for (vector, name) in [
            (b.version_vector().unwrap(), "servable"),
            (b.witnessed_vector().unwrap(), "witnessed"),
        ] {
            assert_eq!(
                vector.get(b.node_id()),
                restored.hlc,
                "{name}: bounded by the restored entry"
            );
            assert_eq!(vector.get(c.node_id()), kept.hlc, "{name}: never below an entry it keeps");
            assert_eq!(
                vector.get(a.node_id()),
                restored.hlc,
                "{name}: A's coverage is its kept entry"
            );
        }
    }

    /// `items/100` written at t120, `items` dropped at t260 and created again at t330,
    /// `items/100` written again at t386; `ended` writes the ended life's history of the
    /// document between the first write and the drop. Returns the base.
    fn a_life_ended_and_begun_again(
        e: &Engine,
        ended: impl Fn(&Engine, &crate::CollectionMeta, u64),
    ) -> u64 {
        let base = base();
        let items = e.create_collection("shop", "items").unwrap();
        at(e, base, 120);
        e.insert(&items, doc! { "_id": 100i64, "v": "old life" }).unwrap();
        ended(e, &items, base);
        at(e, base, 260);
        e.drop_collection("shop", "items").unwrap();
        e.finish_purges_now().unwrap();
        at(e, base, 330);
        let items = e.create_collection("shop", "items").unwrap();
        at(e, base, 386);
        e.insert(&items, doc! { "_id": 100i64, "v": "new life" }).unwrap();
        base
    }

    #[test]
    fn a_rewind_does_not_put_back_a_version_of_a_life_a_drop_ended() {
        // The latest entry of the document at or below t352 is its t120 insert, of the
        // life the drop at t260 ended. In the life standing at t352 the document did not
        // exist: it is removed, with no record (ADR-246, ADR-248).
        let (e, _dir) = engine();
        let base = a_life_ended_and_begun_again(&e, |_, _, _| {});
        let outcome = e.rewind_to(Hlc::new(base + 352, 0)).unwrap();
        assert_eq!((outcome.reverted, outcome.removed, outcome.oplog_discarded), (0, 1, 1));
        let items = e.get_collection("shop", "items").unwrap();
        assert_eq!(e.get(&items, &DocId::Int64(100)).unwrap(), None, "the ended life's version");
        assert_eq!(crate::docs::record_stamp(&e, &items, &DocId::Int64(100)).unwrap(), None);
        crate::live_count::test_support::assert_counts_exact(&e, "a removal over an ended life");
    }

    #[test]
    fn a_rewind_does_not_put_back_the_delete_of_a_life_a_drop_ended() {
        // The ended life's last entry of the document is a delete: put back, it left a
        // tombstone of the ended life in the standing one.
        let (e, _dir) = engine();
        let base = a_life_ended_and_begun_again(&e, |e, items, base| {
            at(e, base, 200);
            e.delete(items, &DocId::Int64(100)).unwrap();
        });
        let outcome = e.rewind_to(Hlc::new(base + 352, 0)).unwrap();
        assert_eq!((outcome.reverted, outcome.removed), (0, 1));
        let items = e.get_collection("shop", "items").unwrap();
        assert_eq!(
            crate::docs::record_stamp(&e, &items, &DocId::Int64(100)).unwrap(),
            None,
            "no tombstone of the ended life's delete"
        );
    }

    #[test]
    fn a_rewind_puts_back_the_standing_lifes_version_over_an_ended_lifes() {
        let base = base();
        let (e, _dir) = engine();
        let items = e.create_collection("shop", "items").unwrap();
        at(&e, base, 120);
        e.insert(&items, doc! { "_id": 100i64, "v": "old life" }).unwrap();
        at(&e, base, 260);
        e.drop_collection("shop", "items").unwrap();
        e.finish_purges_now().unwrap();
        at(&e, base, 330);
        let items = e.create_collection("shop", "items").unwrap();
        at(&e, base, 340);
        e.insert(&items, doc! { "_id": 100i64, "v": "standing" }).unwrap();
        at(&e, base, 386);
        e.replace(&items, &DocId::Int64(100), doc! { "_id": 100i64, "v": "later" }, false).unwrap();
        let outcome = e.rewind_to(Hlc::new(base + 352, 0)).unwrap();
        assert_eq!((outcome.reverted, outcome.removed), (1, 0));
        let held = e.get(&items, &DocId::Int64(100)).unwrap().expect("the standing life's");
        assert_eq!(held.get_str("v").unwrap(), "standing");
    }

    #[test]
    fn an_entry_in_the_drops_own_millisecond_is_of_the_life_it_ended() {
        // C's write of `items/1` and B's drop share one `Hlc`; a tie is the drop's
        // (ADR-236), so C's t260 entry is of the life the drop ended.
        let base = base();
        let ((b, _db), (c, _dc)) = (engine(), engine());
        at(&b, base, 100);
        b.create_collection("shop", "items").unwrap();
        pull(&c, &b);
        at(&c, base, 260);
        let theirs = c.get_collection("shop", "items").unwrap();
        c.insert(&theirs, doc! { "_id": 1i64 }).unwrap();
        at(&b, base, 260);
        b.drop_collection("shop", "items").unwrap();
        let written = crate::docs::record_stamp(&c, &theirs, &DocId::Int64(1)).unwrap().unwrap();
        let dropped = b.collection_dropped_at(theirs.id).unwrap().unwrap();
        assert_eq!(written.hlc, dropped.hlc, "premise: the write and the drop tie on the Hlc");
        pull(&c, &b);
        c.finish_purges_now().unwrap();
        at(&c, base, 330);
        let items = c.create_collection("shop", "items").unwrap();
        at(&c, base, 386);
        c.insert(&items, doc! { "_id": 1i64, "v": "new life" }).unwrap();
        let outcome = c.rewind_to(Hlc::new(base + 352, 0)).unwrap();
        assert_eq!((outcome.reverted, outcome.removed), (0, 1));
        assert_eq!(crate::docs::record_stamp(&c, &items, &DocId::Int64(1)).unwrap(), None);
    }

    #[test]
    fn a_write_after_a_drop_and_before_the_re_creation_is_of_the_standing_life() {
        // A, which never heard of B's drop at t260, writes `items/2` at t290. B creates
        // `items` again at t330 and takes A's write, stamped above the floor: it is of
        // the standing life though it predates the creation (ADR-236), so the rewind
        // puts it back. Judged by `created`, it would be refused.
        let base = base();
        let ((a, _da), (b, _db)) = (engine(), engine());
        at(&b, base, 100);
        b.create_collection("shop", "items").unwrap();
        pull(&a, &b);
        at(&b, base, 260);
        b.drop_collection("shop", "items").unwrap();
        b.finish_purges_now().unwrap();
        at(&a, base, 290);
        let theirs = a.get_collection("shop", "items").unwrap();
        a.insert(&theirs, doc! { "_id": 2i64, "v": "t290" }).unwrap();
        at(&b, base, 330);
        let items = b.create_collection("shop", "items").unwrap();
        pull(&b, &a);
        at(&a, base, 386);
        a.replace(&theirs, &DocId::Int64(2), doc! { "_id": 2i64, "v": "t386" }, false).unwrap();
        pull(&b, &a);
        assert_eq!(
            b.get(&items, &DocId::Int64(2)).unwrap().unwrap().get_str("v").unwrap(),
            "t386",
            "premise: B holds A's writes in the standing life"
        );
        let outcome = b.rewind_to(Hlc::new(base + 352, 0)).unwrap();
        assert_eq!((outcome.reverted, outcome.removed), (1, 0));
        assert_eq!(b.get(&items, &DocId::Int64(2)).unwrap().unwrap().get_str("v").unwrap(), "t290");
    }

    /// C takes B's t120 insert and t200 replacement of `items/1` by entries, and B's
    /// drop at t260 by a whole-database page, which leaves a tombstone and no entry.
    fn a_drop_carried_by_a_page() -> (Engine, Engine, u64, Vec<tempfile::TempDir>) {
        let base = base();
        let ((b, db), (c, dc)) = (engine(), engine());
        at(&b, base, 100);
        let items = b.create_collection("shop", "items").unwrap();
        pull(&c, &b);
        at(&b, base, 120);
        b.insert(&items, doc! { "_id": 1i64, "v": "a" }).unwrap();
        at(&b, base, 200);
        b.replace(&items, &DocId::Int64(1), doc! { "_id": 1i64, "v": "b" }, false).unwrap();
        pull(&c, &b);
        at(&b, base, 260);
        b.drop_collection("shop", "items").unwrap();
        b.finish_purges_now().unwrap();
        whole(&c, &b);
        c.finish_purges_now().unwrap();
        let id = kimmy_core::CollectionId::derive("shop", "items");
        assert!(c.collection_dropped_at(id).unwrap().is_some(), "premise: the tombstone");
        assert!(
            c.read_oplog_from(Hlc::ZERO, usize::MAX, crate::WalkScope::Background)
                .unwrap()
                .iter()
                .all(|entry| entry.kind != OpKind::DropCollection),
            "premise: no drop entry"
        );
        (b, c, base, vec![db, dc])
    }

    /// The rows the store holds under `items`' id, live or not.
    fn rows_under_items(e: &Engine) -> usize {
        let id = kimmy_core::CollectionId::derive("shop", "items").0;
        let txn = e.db().begin_read().unwrap();
        let docs = txn.open_table(tables::DOCS).unwrap();
        docs.range((id, &[][..])..=(id, &[0xffu8; 64][..])).unwrap().count()
    }

    #[test]
    fn a_rewind_past_a_drop_a_snapshot_page_carried_is_refused_without_writing() {
        // A drop after the target is a schema change after it however it arrived. Taken
        // by a page, it left no entry, and the rewind went ahead: it put the t120 version
        // back as a row under the id of a collection that no longer exists (ADR-248).
        let (_b, c, base, _dirs) = a_drop_carried_by_a_page();
        let entries = c.oplog_entries().unwrap();
        let err = c.rewind_to(Hlc::new(base + 150, 0)).unwrap_err().to_string();
        assert!(err.contains("schema change") && err.contains("DropCollection"), "{err}");
        assert_eq!(c.oplog_entries().unwrap(), entries, "no entry was discarded");
        assert_eq!(rows_under_items(&c), 0, "no row was written");
    }

    #[test]
    fn a_rewind_to_the_drop_itself_is_not_refused() {
        // A drop at the target is part of the state at the target.
        let (_b, c, _base, _dirs) = a_drop_carried_by_a_page();
        let id = kimmy_core::CollectionId::derive("shop", "items");
        let dropped = c.collection_dropped_at(id).unwrap().unwrap();
        c.rewind_to(dropped.hlc).expect("a drop at the target is not after it");
        assert_eq!(rows_under_items(&c), 0);
    }

    #[test]
    fn a_rewind_past_a_drop_named_only_by_a_floor_is_refused() {
        // A life created after a drop carries the drop as its floor; tombstone retention
        // can collect the tombstone and leave the floor, which still says the drop
        // happened.
        let base = base();
        let (e, _dir) = engine();
        at(&e, base, 100);
        e.create_collection("shop", "items").unwrap();
        at(&e, base, 260);
        e.drop_collection("shop", "items").unwrap();
        e.finish_purges_now().unwrap();
        at(&e, base, 330);
        let items = e.create_collection("shop", "items").unwrap();
        assert!(items.incarnation_floor.is_some_and(|floor| floor > Hlc::new(base + 150, 0)));
        // The tombstone collected, as tombstone retention does. Written on the raw
        // database: the state is the premise, not something the engine writes.
        let db = e.db();
        let txn = db.begin_write().unwrap();
        txn.open_table(tables::COLLECTIONS_DROPPED).unwrap().remove(items.id.0).unwrap();
        txn.commit().unwrap();
        // Every entry after the target collected as well, so no entry names the drop.
        let txn = db.begin_write().unwrap();
        {
            let mut oplog = txn.open_table(tables::OPLOG).unwrap();
            let after: Vec<Vec<u8>> = oplog
                .iter()
                .unwrap()
                .map(|row| row.unwrap())
                .filter(|(_, value)| {
                    codec::decode_oplog_entry(value.value()).unwrap().stamp.hlc
                        > Hlc::new(base + 150, 0)
                })
                .map(|(key, _)| key.value().to_vec())
                .collect();
            for key in after {
                oplog.remove(key.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        let err = e.rewind_to(Hlc::new(base + 150, 0)).unwrap_err().to_string();
        assert!(err.contains("schema change") && err.contains("DropCollection"), "{err}");
        // A floor at the target is a drop at the target, which is part of the state there.
        let floor = items.incarnation_floor.unwrap();
        e.rewind_to(floor).expect("a floor at the target is not after it");
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
