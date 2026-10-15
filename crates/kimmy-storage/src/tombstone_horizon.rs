//! The collected-through horizon: the highest tombstone cutoff a retention pass has
//! applied (design v3.4, section 13.1).
//!
//! A tombstone is collected when its stamp is below the pass's cutoff, so this is the
//! stamp below which this member may have collected one. It is the premise of the
//! document repair's young-stamp rule: a live row whose stamp is above the horizon of
//! both members of a pair cannot be a resurrection of a delete either of them applied
//! and then collected.
//!
//! **Monotone** by construction: every writer stores `max(stored, cutoff)`. **It moves
//! only when a collector removes something**, in the transaction that removes it: a pass
//! that removes nothing writes nothing (ADR-151), so a member that collects nothing keeps
//! its horizon, and it is not `now - retention`. **Kept in `META`**, one optional key,
//! not a table and not a format change; a 0.49 binary ignores it and does not maintain
//! it, which is what the reset at open (below) is for.
//!
//! The reset: a start decides from the **prior** sidecar whether the key can be trusted.
//! A store last written by another build (or by none that left a sidecar), a store with
//! no key, and a restored store (which drops the key) start at their start time,
//! persisted as `max(stored, start)` in the open's ensure-tables commit. Nothing older
//! than that start is admitted by the young-stamp rule until the horizon passes it
//! honestly.

use kimmy_core::Hlc;
use redb::{ReadableDatabase, ReadableTable};

use crate::engine::{Engine, WriteTxn};
use crate::error::Result;
use crate::tables;

/// Decode the stored horizon, or `None` for a missing or undecodable value (which reads
/// as an absent key: the start time at the next open).
fn decode(raw: &[u8]) -> Option<Hlc> {
    let bytes: [u8; kimmy_core::HLC_ENCODED_LEN] = raw.try_into().ok()?;
    Some(Hlc::from_bytes(bytes))
}

/// The horizon, read through a write transaction the caller holds. A caller that decides
/// on it (the young-stamp rule) reads it here, in its own write transaction, so a
/// collection that commits between a plan and the page cannot leave it judging with a
/// horizon that has not moved while the tombstone is gone.
pub(crate) fn read_in_txn(txn: &WriteTxn<'_>) -> Result<Option<Hlc>> {
    let meta = txn.open_table(tables::META)?;
    Ok(meta.get(tables::META_TOMBSTONE_HORIZON)?.and_then(|raw| decode(raw.value())))
}

/// Store `max(stored, cutoff)` in the transaction that removes the tombstones collected
/// under `cutoff`. Every collector that removes from `DOCS`, `COLLECTIONS_DROPPED` or
/// `INDEXES_DROPPED` calls this before it commits (a source guard in the tests fails the
/// build if one does not).
pub(crate) fn raise_tombstone_horizon_in_txn(txn: &WriteTxn<'_>, cutoff: Hlc) -> Result<()> {
    set_at_least_in_txn(txn, cutoff)
}

/// `max(stored, at)`, written only when it raises the stored value.
pub(crate) fn set_at_least_in_txn(txn: &WriteTxn<'_>, at: Hlc) -> Result<()> {
    let mut meta = txn.open_table(tables::META)?;
    let stored = meta.get(tables::META_TOMBSTONE_HORIZON)?.and_then(|raw| decode(raw.value()));
    if stored.is_none_or(|stored| at > stored) {
        meta.insert(tables::META_TOMBSTONE_HORIZON, at.to_bytes().as_slice())?;
    }
    Ok(())
}

/// The reset at open, on the raw transaction of the ensure-tables commit: `untrusted` (the
/// prior sidecar was another build's or none) or an absent key stores
/// `max(stored, start)`; otherwise the stored value stands. Never lowers a stored value, and
/// never reads an absent key as zero.
pub(crate) fn reset_at_open(
    txn: &redb::WriteTransaction,
    untrusted: bool,
    start: Hlc,
) -> Result<()> {
    let mut meta = txn.open_table(tables::META)?;
    let stored = meta.get(tables::META_TOMBSTONE_HORIZON)?.and_then(|raw| decode(raw.value()));
    let new = match stored {
        Some(stored) if !untrusted => stored,
        Some(stored) => stored.max(start),
        None => start,
    };
    if stored != Some(new) {
        meta.insert(tables::META_TOMBSTONE_HORIZON, new.to_bytes().as_slice())?;
    }
    Ok(())
}

impl Engine {
    /// The collected-through horizon, or `None` before the first open has recorded one.
    pub fn tombstone_horizon(&self) -> Result<Option<Hlc>> {
        let txn = self.db().begin_read()?;
        let meta = txn.open_table(tables::META)?;
        Ok(meta.get(tables::META_TOMBSTONE_HORIZON)?.and_then(|raw| decode(raw.value())))
    }
}

#[cfg(test)]
impl Engine {
    /// Overwrite the stored horizon, as a build that did not maintain it would leave it.
    pub(crate) fn test_set_tombstone_horizon(&self, at: Hlc) -> Result<()> {
        let txn = self.begin_write(crate::engine::WriterHolder::Ddl)?;
        {
            let mut meta = txn.open_table(tables::META)?;
            meta.insert(tables::META_TOMBSTONE_HORIZON, at.to_bytes().as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Remove the stored key, as a restore or a store older than the key has it.
    pub(crate) fn test_remove_tombstone_horizon(&self) -> Result<()> {
        let txn = self.begin_write(crate::engine::WriterHolder::Ddl)?;
        {
            let mut meta = txn.open_table(tables::META)?;
            meta.remove(tables::META_TOMBSTONE_HORIZON)?;
        }
        txn.commit()?;
        Ok(())
    }
}
