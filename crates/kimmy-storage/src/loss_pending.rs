//! The loss records parked for a `field` document that has nothing to hold them (the Step 4 design, v4).
//!
//! A loss entry that reaches a member is kept in its oplog (always keep) and applied to the state of its
//! document when the document has a row with a life or a delete. When it has not (no row: the tombstone was
//! collected, the collection purged, or the document's first entry has not come; a row of nothing but records,
//! which is bare), the record is **parked** here, in the transaction that keeps the entry, and folded into the
//! state by the next write that gives the document a row. A row is never built from records alone: a row of that
//! kind has no version vector, so no horizon, and a record in it would never expire.
//!
//! A parked row expires with the emission window, `R` from the loser's own wall time, by the member's clock: the
//! fold drops an expired record itself, and the retention pass deletes it. The table is node-local and outside
//! the replicated state, the digest and the etag, and a backup does not carry it.

use kimmy_core::Hlc;
use kimmy_core::merge_state::{JoinOptions, LossKey, MergeState};
use redb::ReadableTable;

use crate::error::{Result, StorageError};
use crate::tables;

/// The width of the expiry a row's value starts with.
const EXPIRY: usize = 8;

fn corrupt(what: &str) -> StorageError {
    StorageError::Corrupt(format!("a parked loss record {what}"))
}

/// `expires_ms || body`.
fn value_of(expires_ms: u64, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(EXPIRY + body.len());
    out.extend_from_slice(&expires_ms.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn split(value: &[u8]) -> Result<(u64, &[u8])> {
    let (expires, body) = value.split_at_checked(EXPIRY).ok_or_else(|| corrupt("has no expiry"))?;
    Ok((u64::from_be_bytes(expires.try_into().expect("eight bytes")), body))
}

/// When a record whose loser was stamped at `loser` leaves the window: `R` after it.
fn expires_at(loser: Hlc, retention_ms: u64) -> u64 {
    loser.wall_ms.saturating_add(retention_ms)
}

/// Park the record of `entry` for the document `key` of `collection`, unless it is already out of the window at
/// `now_ms`. Written under the entry's oplog key, so one entry parks once.
pub(crate) fn park(
    txn: &redb::WriteTransaction,
    collection: u64,
    key: &[u8],
    entry_key: &[u8],
    body: &[u8],
    retention_ms: u64,
    now_ms: u64,
) -> Result<bool> {
    let (record, _) = MergeState::decode_loss_record(body)
        .map_err(|e| StorageError::Corrupt(format!("the body of a loss record: {e:?}")))?;
    let expires = expires_at(record.stamp.hlc, retention_ms);
    if expires <= now_ms {
        return Ok(false);
    }
    let mut table = txn.open_table(tables::LOSS_PENDING)?;
    table.insert((collection, key, entry_key), value_of(expires, body).as_slice())?;
    Ok(true)
}

/// Whether anything is parked for the document: one range read, the cost every write of a field row pays.
pub(crate) fn any(txn: &redb::WriteTransaction, collection: u64, key: &[u8]) -> Result<bool> {
    let table = txn.open_table(tables::LOSS_PENDING)?;
    Ok(table
        .range((collection, key, &[][..])..=(collection, key, &[0xff; 64][..]))?
        .next()
        .is_some())
}

/// Fold what is parked for the document into `state` and delete it all: each record that is still in the window
/// at `now_ms` goes through [`MergeState::receive_loss`] (the horizon, the floor, the known key). Returns the
/// records that entered the state. The caller writes the state, in this same transaction.
pub(crate) fn fold_into(
    txn: &redb::WriteTransaction,
    collection: u64,
    key: &[u8],
    state: &mut MergeState,
    options: &JoinOptions,
    floor: Option<Hlc>,
    now_ms: u64,
) -> Result<Vec<LossKey>> {
    let mut table = txn.open_table(tables::LOSS_PENDING)?;
    let mut rows: Vec<(Vec<u8>, u64, Vec<u8>)> = Vec::new();
    for row in table.range((collection, key, &[][..])..=(collection, key, &[0xff; 64][..]))? {
        let (k, v) = row?;
        let (expires, body) = split(v.value())?;
        rows.push((k.value().2.to_vec(), expires, body.to_vec()));
    }
    let mut entered = Vec::new();
    for (entry_key, expires, body) in rows {
        table.remove((collection, key, entry_key.as_slice()))?;
        if expires <= now_ms {
            continue;
        }
        let (record, value) = MergeState::decode_loss_record(&body)
            .map_err(|e| StorageError::Corrupt(format!("a parked loss record: {e:?}")))?;
        if floor.is_some_and(|floor| record.stamp.hlc <= floor) {
            continue;
        }
        if state.receive_loss(record.clone(), value, options) {
            entered.push(record);
        }
    }
    Ok(entered)
}

/// Whether the row with this value is out of the window at `now_ms`.
pub(crate) fn is_expired(value: &[u8], now_ms: u64) -> Result<bool> {
    Ok(split(value)?.0 <= now_ms)
}

/// The retention pass: delete every parked record whose window has closed at `now_ms`. Returns how many.
pub(crate) fn expire(txn: &redb::WriteTransaction, now_ms: u64) -> Result<usize> {
    let mut table = txn.open_table(tables::LOSS_PENDING)?;
    let mut gone: Vec<(u64, Vec<u8>, Vec<u8>)> = Vec::new();
    for row in table.iter()? {
        let (k, v) = row?;
        if split(v.value())?.0 <= now_ms {
            let (c, d, e) = k.value();
            gone.push((c, d.to_vec(), e.to_vec()));
        }
    }
    for (c, d, e) in &gone {
        table.remove((*c, d.as_slice(), e.as_slice()))?;
    }
    Ok(gone.len())
}

/// A rewind to `until`: delete the parked records whose entry was stamped above it, as the rewind discards the
/// entries themselves. The entry's oplog key is the third part of the row's key. Returns how many.
pub(crate) fn remove_above(txn: &redb::WriteTransaction, until: Hlc) -> Result<usize> {
    let mut table = txn.open_table(tables::LOSS_PENDING)?;
    let mut gone: Vec<(u64, Vec<u8>, Vec<u8>)> = Vec::new();
    for row in table.iter()? {
        let (k, _) = row?;
        let (c, d, e) = k.value();
        if crate::codec::decode_oplog_key(e)?.hlc > until {
            gone.push((c, d.to_vec(), e.to_vec()));
        }
    }
    for (c, d, e) in &gone {
        table.remove((*c, d.as_slice(), e.as_slice()))?;
    }
    Ok(gone.len())
}

/// The purge of a dropped collection: delete up to `limit` of its parked records whose entry was stamped at or below
/// the drop's `dropped` (a collection created again under the name has the same id, and what is parked after the drop
/// belongs to the new incarnation, which the fold's floor check judges). Returns how many.
pub(crate) fn remove_collection_chunk(
    txn: &redb::WriteTransaction,
    collection: u64,
    dropped: Hlc,
    limit: usize,
) -> Result<usize> {
    if limit == 0 {
        return Ok(0);
    }
    let mut table = txn.open_table(tables::LOSS_PENDING)?;
    let mut gone: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for row in table.range((collection, &[][..], &[][..])..)? {
        let (k, _) = row?;
        let (c, d, e) = k.value();
        if c != collection || gone.len() >= limit {
            break;
        }
        if crate::codec::decode_oplog_key(e)?.hlc <= dropped {
            gone.push((d.to_vec(), e.to_vec()));
        }
    }
    for (d, e) in &gone {
        table.remove((collection, d.as_slice(), e.as_slice()))?;
    }
    Ok(gone.len())
}

impl crate::Engine {
    /// Build `LOSS_PENDING` from the oplog if this store has not (`META_LOSS_PENDING_BUILT` says so): a restore
    /// carries the loss entries and not the table, and skips the marker. A fresh store has the marker from its
    /// first commit (it has nothing to park), so a start of one makes no commit here; this runs at the end of the
    /// open, after every check that can refuse the store, and writes only for a store a restore made. One bounded walk at open, before the node
    /// serves: the oplog from the start of the largest emission window of any `field` collection (that collection's
    /// `R` before now) upward, judging each entry on its nine-byte header. A loss record still inside its window,
    /// whose document has no row or a bare one, is parked; the marker is written in the same transaction.
    pub(crate) fn rebuild_loss_pending_if_needed(&self) -> Result<()> {
        use redb::{ReadableDatabase, ReadableTable};
        let (built, collections) = {
            let txn = self.db().begin_read()?;
            let built =
                txn.open_table(tables::META)?.get(tables::META_LOSS_PENDING_BUILT)?.is_some();
            let collections = if built { Vec::new() } else { Self::all_collections_in(&txn)? };
            (built, collections)
        };
        if built {
            return Ok(());
        }
        let mut windows = std::collections::HashMap::new();
        for meta in collections.iter().filter(|c| c.merge_policy == kimmy_core::MergePolicy::Field)
        {
            windows.insert(meta.id.0, self.field_writer(meta)?.options().retention);
        }
        let now_ms = self.clock_bound_now_ms();
        let widest = windows.values().copied().max().unwrap_or(0);
        let from = kimmy_core::Stamp::new(
            Hlc::new(now_ms.saturating_sub(widest), 0),
            kimmy_core::NodeId::from_bytes([0; 16]),
            kimmy_core::Epoch::ZERO,
        );
        let txn = self.begin_write(crate::engine::WriterHolder::Ddl)?;
        if !windows.is_empty() {
            let mut found: Vec<(Vec<u8>, kimmy_core::OplogEntry)> = Vec::new();
            {
                let oplog = txn.open_table(tables::OPLOG)?;
                for row in oplog.range(crate::codec::oplog_key(&from).as_slice()..)? {
                    let (key, value) = row?;
                    let Ok((kind, collection)) =
                        crate::codec::decode_oplog_kind_and_collection(value.value())
                    else {
                        continue;
                    };
                    if kind == kimmy_core::OpKind::LossRecord && windows.contains_key(&collection.0)
                    {
                        found.push((
                            key.value().to_vec(),
                            crate::codec::decode_oplog_entry(key.value(), value.value())?,
                        ));
                    }
                }
            }
            let docs = txn.open_table(tables::DOCS)?;
            for (entry_key, entry) in found {
                let (Some(id), Some(body)) = (entry.doc_id.as_ref(), entry.body.as_deref()) else {
                    continue;
                };
                let key = crate::docs::doc_key(id)?;
                let held =
                    docs.get((entry.collection.0, key.as_slice()))?.map(|raw| raw.value().to_vec());
                let has_a_state = match held
                    .map(|raw| crate::codec::decode_doc_record_with_meta(&raw))
                    .transpose()?
                {
                    Some(record) => match record.meta.as_deref() {
                        Some(meta) => !crate::field_write::load(Some(meta))?.is_bare(),
                        None => false,
                    },
                    None => false,
                };
                if !has_a_state {
                    park(
                        &txn,
                        entry.collection.0,
                        &key,
                        &entry_key,
                        body,
                        windows[&entry.collection.0],
                        now_ms,
                    )?;
                }
            }
        }
        txn.open_table(tables::META)?.insert(tables::META_LOSS_PENDING_BUILT, [1u8].as_slice())?;
        txn.commit()?;
        Ok(())
    }
}
