//! The join of a physical's definitions into the physical that becomes visible (ADR-272, decision 6, rule 3; Q6.5-2).
//!
//! When the register's pass makes a physical `N` visible, by a binding or by a recreation, every physical it buries as bound out that is the
//! identity, or has its own binding or recreation in effect, held definitions that must not be lost with it: its indexes, the tombstones of
//! the indexes it dropped, and its validator. They are merged into `N` in the transaction of the flip, with their ORIGINAL stamps, by the one
//! rule a definition entry is judged by everywhere (the higher full stamp wins; a tie goes to the drop):
//!
//! - an index `N` lacks and the merge says is present is built over `N`'s documents by the one backfill a client's or a peer's creation runs;
//! - an index `N` holds and the merge says is dropped has its entries and its definition removed, and the tombstone is written under `N`'s id;
//! - an index both hold under one name with different definitions goes to the later creation stamp, as ADR-132 rules for a replicated create;
//! - every tombstone of the buried physical is written under `N`'s id (an index's id is derived from its name, so only the collection half of
//!   the key changes), and the validator goes by its stamp.
//!
//! What the buried physical held is READ BEFORE its burial: the burial removes the definition and leaves the tombstones, so a join that read
//! after it would still carry tombstones and lose every present definition. The merge is commutative, so the order in which one pass buries
//! two physicals does not matter. A flip never fails on a definition: a unique index the documents violate is built in full and the
//! violations are reported once the commit has landed, never minted inside a transaction that can still abort.

use kimmy_core::{CollectionId, Hlc, OplogEntry, Stamp};
use redb::ReadableTable;

use crate::WalkScope;
use crate::engine::{Engine, WriteTxn};
use crate::error::Result;
use crate::index::{self, UniqueViolation};
use crate::meta::{CollectionMeta, IndexMeta};
use crate::tables;
use crate::walk::open_walk_table_in;

/// What a flip's joins leave for after the commit: the violations their builds found, the `unkeyedDocument` entries they appended (durable
/// with the flip, published once it is), and how many documents they filed unkeyed.
#[derive(Default)]
pub(crate) struct JoinedPost {
    pub(crate) violations: Vec<(CollectionMeta, Vec<UniqueViolation>)>,
    pub(crate) unkeyed: Vec<OplogEntry>,
    pub(crate) unkeyed_count: u64,
}

/// What a physical held, as read before it is buried: its metadata (the definitions and the validator) and its index tombstones.
pub(crate) struct Held {
    pub(crate) meta: CollectionMeta,
    pub(crate) tombstones: Vec<(u32, Stamp)>,
}

impl Engine {
    /// The index tombstones recorded under the collection `id`, as `(index id, stamp of the drop)`.
    pub(crate) fn tombstones_of(txn: &WriteTxn<'_>, id: CollectionId) -> Result<Vec<(u32, Stamp)>> {
        let dropped = txn.open_table(tables::INDEXES_DROPPED)?;
        let mut out = Vec::new();
        for row in dropped.range((id.0, 0u32)..=(id.0, u32::MAX))? {
            let (key, value) = row?;
            out.push((key.value().1, crate::codec::decode_oplog_key(value.value())?));
        }
        Ok(out)
    }

    /// Read what `meta`'s physical holds, before it is buried.
    pub(crate) fn read_held(txn: &WriteTxn<'_>, meta: CollectionMeta) -> Result<Held> {
        let tombstones = Self::tombstones_of(txn, meta.id)?;
        Ok(Held { meta, tombstones })
    }

    /// Merge what `held` held into the physical named `into` (the one that just became visible), in `txn`. `floor` is the newest logical drop
    /// in effect, if any: a definition stamped at or below its `hlc` belongs to the dropped life and is not carried.
    pub(crate) fn join_definitions_in_txn(
        &self,
        txn: &WriteTxn<'_>,
        into: &str,
        held: &Held,
        floor: Option<Hlc>,
    ) -> Result<()> {
        let db = held.meta.db.as_str();
        let mut n: CollectionMeta = {
            let collections = txn.open_table(tables::COLLECTIONS)?;
            let Some(row) = collections.get((db, into))? else { return Ok(()) };
            serde_json::from_slice(row.value())?
        };
        if n.id == held.meta.id {
            return Ok(());
        }
        let above = |stamp: &Stamp| floor.is_none_or(|f| stamp.hlc > f);
        let n_tombs = Self::tombstones_of(txn, n.id)?;
        let tomb_of =
            |tombs: &[(u32, Stamp)], id: u32| tombs.iter().find(|(i, _)| *i == id).map(|(_, s)| *s);

        // every index id either physical holds or has a tombstone for
        let mut ids: Vec<u32> = held.meta.indexes.iter().map(|i| i.id).collect();
        ids.extend(n.indexes.iter().map(|i| i.id));
        ids.extend(held.tombstones.iter().map(|(i, _)| *i));
        ids.extend(n_tombs.iter().map(|(i, _)| *i));
        ids.sort_unstable();
        ids.dedup();

        let fill = self.walk(WalkScope::Background);
        let mut post = JoinedPost::default();
        let mut built: Vec<(IndexMeta, Vec<UniqueViolation>)> = Vec::new();
        for id in ids {
            let q_def = held
                .meta
                .index_by_id(id)
                .filter(|i| i.created.as_ref().is_none_or(&above))
                .cloned();
            let q_tomb = tomb_of(&held.tombstones, id).filter(|s| above(s));
            let n_def = n.index_by_id(id).cloned();
            let n_tomb = tomb_of(&n_tombs, id);
            let drop = match (q_tomb, n_tomb) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
            // which definition stands, if any: the same definition keeps the later stamp; two that differ go to the later creation
            // stamp; one with no stamp cannot arbitrate, and the physical's own is kept
            let winner: Option<(IndexMeta, bool)> = match (&q_def, &n_def) {
                (Some(q), Some(n_def)) if q.differences(n_def).is_empty() => {
                    let mut kept = n_def.clone();
                    if let (Some(qc), Some(nc)) = (q.created, n_def.created)
                        && qc > nc
                    {
                        kept.created = Some(qc);
                    }
                    Some((kept, false))
                }
                (Some(q), Some(n_def)) => match (q.created, n_def.created) {
                    (Some(qc), Some(nc)) if qc > nc => Some((q.clone(), true)),
                    _ => Some((n_def.clone(), false)),
                },
                (Some(q), None) => Some((q.clone(), true)),
                (None, Some(n_def)) => Some((n_def.clone(), false)),
                (None, None) => None,
            };
            // a drop at or above the winning creation, or one that meets a creation with no stamp, ends it
            let ended = match (&winner, drop) {
                (Some((w, _)), Some(d)) => w.created.is_none_or(|c| d >= c),
                (None, Some(_)) => true,
                (_, None) => false,
            };
            if ended {
                if n_def.is_some() {
                    {
                        let mut entries = open_walk_table_in(txn, tables::INDEX_ENTRIES, fill)?;
                        index::clear_index_entries(&mut entries, index::index_id_range(n.id, id))?;
                    }
                    n.indexes.retain(|i| i.id != id);
                }
                if let Some(d) = drop {
                    Self::record_index_drop_in_txn(txn, n.id, id, d)?;
                }
                continue;
            }
            // the tombstones below the standing creation are carried all the same: the merge keeps the newer under `N`'s id
            if let Some(t) = q_tomb {
                Self::record_index_drop_in_txn(txn, n.id, id, t)?;
            }
            let Some((winning, needs_build)) = winner else { continue };
            if !needs_build {
                if let Some(slot) = n.indexes.iter_mut().find(|i| i.id == id) {
                    slot.created = winning.created;
                }
                continue;
            }
            if n_def.is_some() {
                {
                    let mut entries = open_walk_table_in(txn, tables::INDEX_ENTRIES, fill)?;
                    index::clear_index_entries(&mut entries, index::index_id_range(n.id, id))?;
                }
                n.indexes.retain(|i| i.id != id);
            }
            let mut index = winning;
            let (observed, violations, unkeyed) =
                self.backfill_index_in_txn(txn, fill, n.id, &index, false)?;
            index.multikey = observed;
            if !unkeyed.is_empty() {
                post.unkeyed_count += unkeyed.len() as u64;
                post.unkeyed.extend(
                    self.append_unkeyed(
                        txn,
                        unkeyed
                            .iter()
                            .map(|(doc, cause)| {
                                let detail = kimmy_core::UnkeyedDocumentDetail {
                                    index: index.name.clone(),
                                    id: doc.clone(),
                                    reason: cause.as_str().to_string(),
                                };
                                (n.id, detail)
                            })
                            .collect(),
                    )?,
                );
            }
            Self::clear_refusal_hints_for_in(
                txn,
                n.id,
                &crate::definition_digest::DefinitionKey::index(&index.name),
            )?;
            n.indexes.push(index.clone());
            if !violations.is_empty() {
                built.push((index, violations));
            }
        }

        // the validator goes by the later stamp, a removal included
        if let Some(q_changed) = held.meta.validator_changed.filter(|s| above(s))
            && n.validator_changed.is_none_or(|c| q_changed > c)
        {
            n.validator = held.meta.validator.clone();
            n.validator_changed = Some(q_changed);
        }

        Self::put_collection_meta(txn, &n)?;
        for (_, violations) in built {
            post.violations.push((n.clone(), violations));
        }
        let mut joined = txn.joined.lock();
        joined.violations.extend(post.violations);
        joined.unkeyed.extend(post.unkeyed);
        joined.unkeyed_count += post.unkeyed_count;
        Ok(())
    }
}
