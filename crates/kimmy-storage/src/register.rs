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
    stage_in(&txn.open_table(tables::REGISTER_STAGES)?, physical)
}

/// [`stage_row`] over any readable table (a write or a read transaction's).
fn stage_in(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    physical: CollectionId,
) -> Result<Option<StageMeta>> {
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
    let held = rows_in(&txn.open_table(table)?, CollectionId::derive(db, logical))?;
    timeline_from(held, logical, |event| stage_of(txn, db, event))
}

/// The events of one name held in a register table, oldest first.
fn rows_in(
    rows: &impl ReadableTable<&'static [u8], &'static [u8]>,
    id: CollectionId,
) -> Result<Vec<(Stamp, RegisterEvent)>> {
    let lo = row_key(id, &Stamp::from_key_bytes([0; Stamp::KEY_LEN]));
    let hi = row_key(id, &Stamp::from_key_bytes([0xFF; Stamp::KEY_LEN]));
    let mut held = Vec::new();
    for row in rows.range(lo.as_slice()..=hi.as_slice())? {
        let (key, value) = row?;
        let stamp = crate::codec::decode_oplog_key(&key.value()[8..])?;
        let event = RegisterEvent::decode(value.value()).map_err(|e| corrupt(&e.to_string()))?;
        held.push((stamp, event));
    }
    Ok(held)
}

/// The timeline of `held`, each stored binding judged again by the one door with the stage `stage_for` finds for it.
fn timeline_from(
    held: Vec<(Stamp, RegisterEvent)>,
    logical: &str,
    stage_for: impl Fn(&RegisterEvent) -> Result<Option<StageMeta>>,
) -> Result<Timeline> {
    let mut timeline = Timeline::new(logical);
    for (stamp, event) in held {
        let stage = stage_for(&event)?;
        match timeline.insert(stamp, event, stage.as_ref().map(|s| (&s.created, &s.info))) {
            Put::Stored => {}
            other => return Err(corrupt(&format!("a stored event is now {other:?}"))),
        }
    }
    Ok(timeline)
}

/// The register events of `logical` in effect on this member, read in a read transaction (the resolution of a name, Q6.4 item 0).
pub(crate) fn timeline_read(
    txn: &redb::ReadTransaction,
    db: &str,
    logical: &str,
) -> Result<Timeline> {
    timeline_read_of(txn, tables::REGISTER, db, logical)
}

/// The register events of `logical` known ahead of their position, read in a read transaction.
pub(crate) fn parked_read(
    txn: &redb::ReadTransaction,
    db: &str,
    logical: &str,
) -> Result<Timeline> {
    timeline_read_of(txn, tables::REGISTER_INFO, db, logical)
}

/// Why a member serving `served` refuses writes to `logical` (decision 7), read in a read transaction: the in-effect events,
/// then those known ahead of their position, exactly as [`refusal`] judges them under the writer.
pub(crate) fn refusal_read(
    txn: &redb::ReadTransaction,
    db: &str,
    logical: &str,
    served: &str,
) -> Result<Option<Refusal>> {
    let in_effect = timeline_read(txn, db, logical)?;
    let ahead = parked_read(txn, db, logical)?;
    Ok(in_effect.refusal(&ahead, served))
}

fn timeline_read_of(
    txn: &redb::ReadTransaction,
    table: redb::TableDefinition<&'static [u8], &'static [u8]>,
    db: &str,
    logical: &str,
) -> Result<Timeline> {
    let held = rows_in(&txn.open_table(table)?, CollectionId::derive(db, logical))?;
    let stages = txn.open_table(tables::REGISTER_STAGES)?;
    timeline_from(held, logical, |event| match event {
        RegisterEvent::Bind { physical } => stage_in(&stages, CollectionId::derive(db, physical)),
        RegisterEvent::Create { .. } | RegisterEvent::LogicalDrop => Ok(None),
    })
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
    seed_from_the_tombstone(txn, db, logical)?;
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
    seed_from_the_tombstone(txn, db, logical)?;
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
    // not `put`: the name is known already (this event was), so there is nothing left to seed
    store(txn, tables::REGISTER, db, logical, *stamp, event).map(Some)
}

/// The stamps of the information rows (events known ahead of their position) that are among `stamps`.
fn parked_among(txn: &redb::WriteTransaction, stamps: &[Stamp]) -> Result<Vec<Stamp>> {
    let info = txn.open_table(tables::REGISTER_INFO)?;
    let mut found = Vec::new();
    for row in info.iter()? {
        let (key, _) = row?;
        let stamp = crate::codec::decode_oplog_key(&key.value()[8..])?;
        if stamps.contains(&stamp) {
            found.push(stamp);
        }
    }
    Ok(found)
}

/// Marks of held entries were just released at `released` (a grant covered them): every binding among them that was only
/// information takes effect now, in the same transaction (build item (a)). The table holds only the events ahead of their
/// position, so with none parked this reads one empty table. Returns the `(db, logical)` of each that took effect, for the caller
/// to settle (the bound-out burial, Q6.4 item 1).
pub(crate) fn release_covered(
    txn: &redb::WriteTransaction,
    released: &[Stamp],
) -> Result<Vec<(String, String)>> {
    if released.is_empty() {
        return Ok(Vec::new());
    }
    let waiting = parked_among(txn, released)?;
    let mut took_effect = Vec::new();
    for stamp in &waiting {
        let key = crate::codec::oplog_key(stamp);
        let entry = {
            let oplog = txn.open_table(tables::OPLOG)?;
            let Some(raw) = oplog.get(key.as_slice())? else {
                return Err(corrupt(
                    "an event known ahead of its position whose entry is not held",
                ));
            };
            crate::codec::decode_oplog_entry(key.as_slice(), raw.value())?
        };
        let Some(body) = &entry.body else {
            return Err(corrupt("a register entry without a body"));
        };
        let Some((db, logical)) = event_target(&entry.kind, body)? else {
            return Err(corrupt(
                "an event known ahead of its position whose entry is no register event",
            ));
        };
        release(txn, &db, &logical, stamp)?;
        took_effect.push((db, logical));
    }
    Ok(took_effect)
}

/// Build item (a), FIX-4: once the mark of the entry at `stamp` is released, no register event of that stamp is still only
/// information. A release that raised the coverage and left the event parked would serve a window the member does not act on.
pub(crate) fn check_released_took_effect(
    txn: &redb::WriteTransaction,
    stamp: &Stamp,
) -> Result<()> {
    if parked_among(txn, std::slice::from_ref(stamp))?.is_empty() {
        Ok(())
    } else {
        Err(corrupt("a released register event is still only information"))
    }
}

/// The stamp of the earliest binding of `physical` this member knows, in effect or ahead of its position: the stamp rule S
/// compares a staged row against (ADR-272, decision 6). `None` while the stage has no binding here.
pub(crate) fn binding_stamp_of(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    physical: &str,
) -> Result<Option<Stamp>> {
    let known = |timeline: Timeline| {
        timeline
            .events()
            .iter()
            .filter(
                |(_, event)| matches!(event, RegisterEvent::Bind { physical: p } if p == physical),
            )
            .map(|(stamp, _)| *stamp)
            .min()
    };
    let in_effect = known(timeline(txn, db, logical)?);
    let ahead = known(parked(txn, db, logical)?);
    Ok(match (in_effect, ahead) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    })
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

/// The name a register entry is addressed to: a binding's `(db, logical)`, a drop's, or the logical name a recreation's physical
/// was made from. `None` for a creation that is not a recreate (an ordinary creation is no register event).
pub(crate) fn event_target(
    kind: &kimmy_core::OpKind,
    body: &[u8],
) -> Result<Option<(String, String)>> {
    match kind {
        kimmy_core::OpKind::BindCollection => {
            let bind: kimmy_core::CollectionBind = bson::deserialize_from_slice(body)?;
            Ok(Some((bind.db, bind.name)))
        }
        kimmy_core::OpKind::DropCollection => {
            let target: kimmy_core::CollectionRef = bson::deserialize_from_slice(body)?;
            Ok(Some((target.db, target.name)))
        }
        kimmy_core::OpKind::CreateCollection => {
            // a recreate: the creation of a physical named by its own event, addressed to the logical name it was made from
            let target: kimmy_core::CollectionCreate = bson::deserialize_from_slice(body)?;
            Ok(kimmy_core::logical_of_recreation(&target.name)
                .map(|logical| (target.db.clone(), logical.to_string())))
        }
        other => Err(corrupt(&format!("a {other:?} entry is not a register event"))),
    }
}

/// The first event of a name seeds the register from the identity's drop (Q6.4 item 1b): a member that applied a drop of the
/// name before it knew of any swap holds that drop only as the identity's tombstone, and the binding that arrives afterwards
/// has to be judged against it, as it is on a member that knew of the swap when the drop arrived. With no event of the name
/// known, in effect or ahead, and the identity not standing, a tombstone is a logical drop at its stamp. A binding stamped
/// above it is not touched by it, and one below it is buried by it, as on every other member.
fn seed_from_the_tombstone(txn: &redb::WriteTransaction, db: &str, logical: &str) -> Result<()> {
    let id = CollectionId::derive(db, logical);
    if !rows_in(&txn.open_table(tables::REGISTER)?, id)?.is_empty()
        || !rows_in(&txn.open_table(tables::REGISTER_INFO)?, id)?.is_empty()
    {
        return Ok(());
    }
    if txn.open_table(tables::COLLECTIONS)?.get((db, logical))?.is_some() {
        return Ok(());
    }
    let dropped = {
        let table = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        match table.get(id.0)? {
            Some(raw) => crate::codec::decode_oplog_key(raw.value())?,
            None => return Ok(()),
        }
    };
    store(txn, tables::REGISTER, db, logical, dropped, RegisterEvent::LogicalDrop)?;
    Ok(())
}

/// Whether the name has a register event on this member, in effect or ahead of its position.
pub(crate) fn has_events(txn: &redb::ReadTransaction, db: &str, logical: &str) -> Result<bool> {
    let id = CollectionId::derive(db, logical);
    Ok(!rows_in(&txn.open_table(tables::REGISTER)?, id)?.is_empty()
        || !rows_in(&txn.open_table(tables::REGISTER_INFO)?, id)?.is_empty())
}

/// Whether this member holds a stage of the name.
pub(crate) fn stage_is_held(txn: &redb::ReadTransaction, db: &str, logical: &str) -> Result<bool> {
    stage_of_the_name_is_held(&txn.open_table(tables::COLLECTIONS)?, db, logical)
}

/// Whether the name has been swapped as far as this member can tell: it has a register event, in effect or ahead of its
/// position, or it holds a stage of the name (Q6.4 item 1b). The drop of such a name is a logical drop.
pub(crate) fn is_swapped(txn: &redb::ReadTransaction, db: &str, logical: &str) -> Result<bool> {
    Ok(has_events(txn, db, logical)? || stage_is_held(txn, db, logical)?)
}

/// [`is_swapped`] under the writer.
pub(crate) fn is_swapped_in(txn: &redb::WriteTransaction, db: &str, logical: &str) -> Result<bool> {
    let id = CollectionId::derive(db, logical);
    if !rows_in(&txn.open_table(tables::REGISTER)?, id)?.is_empty()
        || !rows_in(&txn.open_table(tables::REGISTER_INFO)?, id)?.is_empty()
    {
        return Ok(true);
    }
    stage_of_the_name_is_held(&txn.open_table(tables::COLLECTIONS)?, db, logical)
}

fn stage_of_the_name_is_held(
    collections: &impl ReadableTable<(&'static str, &'static str), &'static [u8]>,
    db: &str,
    logical: &str,
) -> Result<bool> {
    let prefix = format!("{logical}{}", kimmy_core::swap::STAGE_NAME_MARK);
    let mut rows = collections.range((db, prefix.as_str())..=(db, "\u{10FFFF}"))?;
    Ok(match rows.next() {
        Some(row) => row?.0.value().1.starts_with(&prefix),
        None => false,
    })
}

/// The name of a register entry that is held ahead of its position here (its event is only information), or `None`: a drop
/// entry that is held for any other reason, and the old rule's, is not a register event.
pub(crate) fn parked_target(
    txn: &redb::WriteTransaction,
    stamp: &Stamp,
    kind: &kimmy_core::OpKind,
    body: &[u8],
) -> Result<Option<(String, String)>> {
    let Some((db, logical)) = event_target(kind, body)? else { return Ok(None) };
    // A bind is always a register event; a drop or a recreate is one only if its event is parked
    if *kind != kimmy_core::OpKind::BindCollection
        && parked_among(txn, std::slice::from_ref(stamp))?.is_empty()
    {
        return Ok(None);
    }
    Ok(Some((db, logical)))
}

/// [`has_events`] under the writer.
pub(crate) fn has_events_in(txn: &redb::WriteTransaction, db: &str, logical: &str) -> Result<bool> {
    let id = CollectionId::derive(db, logical);
    Ok(!rows_in(&txn.open_table(tables::REGISTER)?, id)?.is_empty()
        || !rows_in(&txn.open_table(tables::REGISTER_INFO)?, id)?.is_empty())
}
