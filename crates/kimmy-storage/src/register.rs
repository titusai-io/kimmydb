//! The register of a logical name, in storage (ADR-272, decisions 3, 5 and 7).
//!
//! `kimmy_core::register_state` decides, as a pure function, what a name serves and which events are valid; this module is
//! where a member keeps the events it applies it to. Three tables: [`tables::REGISTER`] holds the events in effect,
//! [`tables::REGISTER_INFO`] those known ahead of their position (rule B), and [`tables::REGISTER_STAGES`] the creation of
//! every stage, which outlives the stage's collection because a later binding's validity reads it.
//!
//! **There is one door.** Every event reaches a table through [`put`] or [`park`], which build the name's
//! [`Timeline`] and call `Timeline::insert` with the stage the event names: a binding above its stage's deadline, or of a
//! stage whose creation is not known, is never stored, so no route can make one visible by forgetting the check. This
//! module is the only code that opens the three tables (`only_this_module_opens_the_register_tables`).

use kimmy_core::register_state::{Put, Refusal, Timeline};
use kimmy_core::{CollectionId, HLC_ENCODED_LEN, Hlc, RegisterEvent, StageMeta, Stamp};
use redb::ReadableTable;

use crate::error::{Result, StorageError};
use crate::tables;

/// `logical id (8) || stamp key (44)`.
const ROW_KEY_LEN: usize = 8 + Stamp::KEY_LEN;

fn row_key(logical: CollectionId, stamp: &Stamp) -> [u8; ROW_KEY_LEN] {
    let mut key = [0u8; ROW_KEY_LEN];
    key[..8].copy_from_slice(&logical.0.to_be_bytes());
    key[8..].copy_from_slice(&stamp.to_key_bytes());
    key
}

fn corrupt(what: &str) -> StorageError {
    StorageError::Corrupt(format!("the register: {what}"))
}

/// Create the three tables, so a read of them never meets a missing table.
pub(crate) fn open_tables(txn: &redb::WriteTransaction) -> Result<()> {
    let _ = txn.open_table(tables::REGISTER)?;
    let _ = txn.open_table(tables::REGISTER_INFO)?;
    let _ = txn.open_table(tables::REGISTER_STAGES)?;
    Ok(())
}

/// Keep a stage's creation. Written in the transaction of the creation itself and never removed.
pub(crate) fn write_stage_row(
    txn: &redb::WriteTransaction,
    physical: CollectionId,
    stage: &StageMeta,
) -> Result<()> {
    let mut value = Vec::with_capacity(Stamp::KEY_LEN + HLC_ENCODED_LEN);
    value.extend_from_slice(&stage.created.to_key_bytes());
    value.extend_from_slice(&stage.info.deadline.to_bytes());
    txn.open_table(tables::REGISTER_STAGES)?
        .insert(physical.0.to_be_bytes().as_slice(), value.as_slice())?;
    Ok(())
}

/// What the register keeps of the creation of the stage `physical`, if it ever held one.
pub(crate) fn stage_row(
    txn: &redb::WriteTransaction,
    physical: CollectionId,
) -> Result<Option<StageMeta>> {
    let table = txn.open_table(tables::REGISTER_STAGES)?;
    let Some(raw) = table.get(physical.0.to_be_bytes().as_slice())? else {
        return Ok(None);
    };
    let bytes = raw.value();
    if bytes.len() != Stamp::KEY_LEN + HLC_ENCODED_LEN {
        return Err(corrupt("a stage row of the wrong length"));
    }
    let created: [u8; Stamp::KEY_LEN] = bytes[..Stamp::KEY_LEN].try_into().expect("checked length");
    let deadline: [u8; HLC_ENCODED_LEN] =
        bytes[Stamp::KEY_LEN..].try_into().expect("checked length");
    Ok(Some(StageMeta {
        created: Stamp::from_key_bytes(created),
        info: kimmy_core::StageInfo { deadline: Hlc::from_bytes(deadline) },
    }))
}

/// The events of `logical` held in `table`, as a [`Timeline`]. Each stored binding is judged again by the same door; one that
/// no longer passes means the table was written around it, which is corruption.
fn timeline_of(
    txn: &redb::WriteTransaction,
    table: redb::TableDefinition<&'static [u8], &'static [u8]>,
    db: &str,
    logical: &str,
) -> Result<Timeline> {
    let id = CollectionId::derive(db, logical);
    let mut timeline = Timeline::new(logical);
    let rows = txn.open_table(table)?;
    let lo = row_key(id, &Stamp::from_key_bytes([0; Stamp::KEY_LEN]));
    let hi = row_key(id, &Stamp::from_key_bytes([0xFF; Stamp::KEY_LEN]));
    let mut held = Vec::new();
    for row in rows.range(lo.as_slice()..=hi.as_slice())? {
        let (key, value) = row?;
        let stamp = crate::codec::decode_oplog_key(&key.value()[8..])?;
        let event = RegisterEvent::decode(value.value()).map_err(|e| corrupt(&e.to_string()))?;
        held.push((stamp, event));
    }
    drop(rows);
    for (stamp, event) in held {
        let stage = stage_of(txn, db, &event)?;
        match timeline.insert(stamp, event, stage.as_ref().map(|s| (&s.created, &s.info))) {
            Put::Stored => {}
            other => return Err(corrupt(&format!("a stored event is now {other:?}"))),
        }
    }
    Ok(timeline)
}

fn stage_of(
    txn: &redb::WriteTransaction,
    db: &str,
    event: &RegisterEvent,
) -> Result<Option<StageMeta>> {
    match event {
        RegisterEvent::Bind { physical } => stage_row(txn, CollectionId::derive(db, physical)),
        RegisterEvent::Create { .. } | RegisterEvent::LogicalDrop => Ok(None),
    }
}

/// The register events of `logical` in effect on this member.
pub(crate) fn timeline(txn: &redb::WriteTransaction, db: &str, logical: &str) -> Result<Timeline> {
    timeline_of(txn, tables::REGISTER, db, logical)
}

/// The register events of `logical` this member knows and that are not yet in effect (ahead of their position).
pub(crate) fn parked(txn: &redb::WriteTransaction, db: &str, logical: &str) -> Result<Timeline> {
    timeline_of(txn, tables::REGISTER_INFO, db, logical)
}

/// Put an event in effect, through the one door: [`Timeline::insert`] with the stage the event names.
pub(crate) fn put(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    stamp: Stamp,
    event: RegisterEvent,
) -> Result<Put> {
    store(txn, tables::REGISTER, db, logical, stamp, event)
}

/// Record an event that arrived ahead of its position: known, not in effect. The same door, so a binding that could never
/// take effect is not parked either.
pub(crate) fn park(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    stamp: Stamp,
    event: RegisterEvent,
) -> Result<Put> {
    if timeline(txn, db, logical)?.events().iter().any(|(held, _)| *held == stamp) {
        return Ok(Put::Duplicate);
    }
    store(txn, tables::REGISTER_INFO, db, logical, stamp, event)
}

fn store(
    txn: &redb::WriteTransaction,
    table: redb::TableDefinition<&'static [u8], &'static [u8]>,
    db: &str,
    logical: &str,
    stamp: Stamp,
    event: RegisterEvent,
) -> Result<Put> {
    let mut timeline = timeline_of(txn, table, db, logical)?;
    let stage = stage_of(txn, db, &event)?;
    let put = timeline.insert(stamp, event.clone(), stage.as_ref().map(|s| (&s.created, &s.info)));
    if put == Put::Stored {
        let key = row_key(CollectionId::derive(db, logical), &stamp);
        txn.open_table(table)?.insert(key.as_slice(), event.encode().as_slice())?;
    }
    Ok(put)
}

/// An event ahead of its position is now covered: it leaves the information table and takes effect. `None` if no such
/// event is parked.
pub(crate) fn release(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    stamp: &Stamp,
) -> Result<Option<Put>> {
    let key = row_key(CollectionId::derive(db, logical), stamp);
    let event = {
        let mut info = txn.open_table(tables::REGISTER_INFO)?;
        let removed = info.remove(key.as_slice())?;
        let Some(raw) = removed else { return Ok(None) };
        RegisterEvent::decode(raw.value()).map_err(|e| corrupt(&e.to_string()))?
    };
    put(txn, db, logical, *stamp, event).map(Some)
}

/// Build item (a), FIX-4: after the mark of the entry at `stamp` is released, no register event of that stamp is still only
/// information. (A stub: it checks nothing yet.)
pub(crate) fn check_released_took_effect(
    _txn: &redb::WriteTransaction,
    _stamp: &Stamp,
) -> Result<()> {
    Ok(())
}

/// Why a member serving `served` refuses writes to `logical` (decision 7): an event in effect supersedes it or, failing
/// that, one known ahead of its position would.
pub(crate) fn refusal(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    served: &str,
) -> Result<Option<Refusal>> {
    let in_effect = timeline(txn, db, logical)?;
    let ahead = parked(txn, db, logical)?;
    Ok(in_effect.refusal(&ahead, served))
}
