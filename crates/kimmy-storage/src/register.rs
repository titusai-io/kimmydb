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

/// Create the register's tables, so a read of them never meets a missing table.
pub(crate) fn open_tables(txn: &redb::WriteTransaction) -> Result<()> {
    let _ = txn.open_table(tables::REGISTER)?;
    let _ = txn.open_table(tables::REGISTER_INFO)?;
    let _ = txn.open_table(tables::REGISTER_STAGES)?;
    let _ = txn.open_table(tables::REGISTER_POST)?;
    Ok(())
}

/// The post-drop row of a logical name (ADR-272, decision 6; Q6.5-2): the definitions stamped above the name's newest logical drop, kept
/// while the name is absent. Merged by stamp (the higher wins, a tie goes to the drop), joined into the next physical that becomes visible
/// and deleted in that transaction.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PostRow {
    /// The indexes, each with its creation stamp.
    pub(crate) indexes: Vec<kimmy_core::IndexMeta>,
    /// The index tombstones: `(index id, stamp of the drop)`.
    pub(crate) tombstones: Vec<(u32, Stamp)>,
    /// The validator and the stamp of its latest change.
    pub(crate) validator: Option<kimmy_core::ValidatorSpec>,
    pub(crate) validator_changed: Option<Stamp>,
    /// The definition floor of the physicals the drop buried, the greatest of them: the next physical takes it
    /// ([`crate::meta::CollectionMeta::definition_floor`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) definition_floor: Option<Hlc>,
}

fn post_key(db: &str, logical: &str) -> [u8; 8] {
    CollectionId::derive(db, logical).0.to_be_bytes()
}

impl PostRow {
    /// Whether the row holds nothing: no index, no tombstone, no validator, no definition floor.
    pub(crate) fn is_empty(&self) -> bool {
        self.indexes.is_empty()
            && self.tombstones.is_empty()
            && self.validator.is_none()
            && self.validator_changed.is_none()
            && self.definition_floor.is_none()
    }

    /// Remove what is stamped at or below `floor` (a logical drop's hlc): the dropped life's.
    pub(crate) fn floor(&mut self, floor: Hlc) {
        self.indexes.retain(|i| i.created.is_some_and(|c| c.hlc > floor));
        self.tombstones.retain(|(_, s)| s.hlc > floor);
        if self.validator_changed.is_none_or(|s| s.hlc <= floor) {
            self.validator = None;
            self.validator_changed = None;
        }
    }

    /// Merge definitions into the row by stamp, carrying only what is stamped above `floor`: per index name the higher stamp wins and a tie
    /// goes to the drop; two definitions that differ go to the later creation stamp; the validator goes by its stamp. REPLACES per name and
    /// stamp and never appends, so delivering the same definition twice leaves the row as after one delivery.
    pub(crate) fn merge(
        &mut self,
        indexes: &[kimmy_core::IndexMeta],
        tombstones: &[(u32, Stamp)],
        validator: Option<(&Option<kimmy_core::ValidatorSpec>, Stamp)>,
        floor: Option<Hlc>,
    ) {
        let above = |s: &Stamp| floor.is_none_or(|f| s.hlc > f);
        let mut ids: Vec<u32> = indexes.iter().map(|i| i.id).collect();
        ids.extend(self.indexes.iter().map(|i| i.id));
        ids.extend(tombstones.iter().map(|(i, _)| *i));
        ids.extend(self.tombstones.iter().map(|(i, _)| *i));
        ids.sort_unstable();
        ids.dedup();
        let tomb =
            |tombs: &[(u32, Stamp)], id: u32| tombs.iter().find(|(i, _)| *i == id).map(|(_, s)| *s);
        let mut next_indexes = Vec::new();
        let mut next_tombs = Vec::new();
        for id in ids {
            let src_def = indexes
                .iter()
                .find(|i| i.id == id)
                .filter(|i| i.created.as_ref().is_none_or(&above));
            let dst_def = self.indexes.iter().find(|i| i.id == id);
            let drop = [tomb(tombstones, id).filter(|s| above(s)), tomb(&self.tombstones, id)]
                .into_iter()
                .flatten()
                .max();
            let winner = match (src_def, dst_def) {
                (Some(s), Some(d)) if s.differences(d).is_empty() => {
                    let mut kept = d.clone();
                    if let (Some(sc), Some(dc)) = (s.created, d.created)
                        && sc > dc
                    {
                        kept.created = Some(sc);
                    }
                    Some(kept)
                }
                (Some(s), Some(d)) => match (s.created, d.created) {
                    (Some(sc), Some(dc)) if sc > dc => Some(s.clone()),
                    _ => Some(d.clone()),
                },
                (Some(s), None) => Some(s.clone()),
                (None, Some(d)) => Some(d.clone()),
                (None, None) => None,
            };
            let ended = match (&winner, drop) {
                (Some(w), Some(d)) => w.created.is_none_or(|c| d >= c),
                (None, Some(_)) => true,
                (_, None) => false,
            };
            if let Some(d) = drop {
                next_tombs.push((id, d));
            }
            if !ended && let Some(w) = winner {
                next_indexes.push(w);
            }
        }
        self.indexes = next_indexes;
        self.tombstones = next_tombs;
        if let Some((spec, changed)) = validator
            && above(&changed)
            && self.validator_changed.is_none_or(|c| changed > c)
        {
            self.validator = spec.clone();
            self.validator_changed = Some(changed);
        }
    }
}

/// Write the post-drop row of `logical`, or delete it when it holds nothing.
pub(crate) fn write_post_row(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    row: &PostRow,
) -> Result<()> {
    let mut table = txn.open_table(tables::REGISTER_POST)?;
    if row.is_empty() {
        table.remove(post_key(db, logical).as_slice())?;
    } else {
        table.insert(post_key(db, logical).as_slice(), serde_json::to_vec(row)?.as_slice())?;
    }
    Ok(())
}

/// Delete the post-drop row of `logical`, in the transaction that joins it into the next physical.
pub(crate) fn delete_post_row(txn: &redb::WriteTransaction, db: &str, logical: &str) -> Result<()> {
    txn.open_table(tables::REGISTER_POST)?.remove(post_key(db, logical).as_slice())?;
    Ok(())
}

/// The post-drop row of `logical`, if it has one.
pub(crate) fn post_row(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
) -> Result<Option<PostRow>> {
    let table = txn.open_table(tables::REGISTER_POST)?;
    match table.get(post_key(db, logical).as_slice())? {
        Some(raw) => Ok(Some(serde_json::from_slice(raw.value())?)),
        None => Ok(None),
    }
}

/// The post-drop row of `logical`, read outside any write transaction. Test builds only: the one-module guard lets nothing but this file
/// open the table, so a test reads it through here.
#[cfg(test)]
pub(crate) fn post_row_for_tests(
    engine: &crate::Engine,
    db: &str,
    logical: &str,
) -> Result<Option<PostRow>> {
    use redb::ReadableDatabase;
    let txn = engine.db().begin_read()?;
    let table = txn.open_table(tables::REGISTER_POST)?;
    match table.get(post_key(db, logical).as_slice())? {
        Some(raw) => Ok(Some(serde_json::from_slice(raw.value())?)),
        None => Ok(None),
    }
}

/// What the register keeps of a stage's creation: the creation's stamp and deadline ([`StageMeta`]) and the names of the stage's
/// collection. The names are part of the row so that a stage the register knows and this member holds no collection for can still be
/// addressed by them: a staging write for it is judged by its creation's origin and its bindings (decision 4, rule S), and the logical
/// name it belongs to follows from the physical.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StageRow {
    pub meta: StageMeta,
    pub db: String,
    pub physical: String,
}

/// Bytes of a stage row before its names: the creation's stamp key and the deadline.
const STAGE_ROW_FIXED: usize = Stamp::KEY_LEN + HLC_ENCODED_LEN;

/// Keep a stage's creation. Written in the transaction of the creation itself and never removed. Whatever rebuilds a stage's row from
/// its creation entry rebuilds the names with it: they are the entry's own `db` and `name`.
pub(crate) fn write_stage_row(
    txn: &redb::WriteTransaction,
    db: &str,
    physical: &str,
    stage: &StageMeta,
) -> Result<()> {
    let mut value = Vec::with_capacity(STAGE_ROW_FIXED + 2 + db.len() + physical.len());
    value.extend_from_slice(&stage.created.to_key_bytes());
    value.extend_from_slice(&stage.info.deadline.to_bytes());
    // `db` is length-prefixed; `physical` is the rest
    value.extend_from_slice(&(db.len() as u16).to_be_bytes());
    value.extend_from_slice(db.as_bytes());
    value.extend_from_slice(physical.as_bytes());
    txn.open_table(tables::REGISTER_STAGES)?
        .insert(CollectionId::derive(db, physical).0.to_be_bytes().as_slice(), value.as_slice())?;
    Ok(())
}

/// What the register keeps of the creation of the stage `physical`, if it ever held one.
pub(crate) fn stage_row(
    txn: &redb::WriteTransaction,
    physical: CollectionId,
) -> Result<Option<StageMeta>> {
    stage_in(&txn.open_table(tables::REGISTER_STAGES)?, physical)
}

/// [`stage_row`] with the names of the stage: the row a staging write for a stage this member holds no collection of is judged by.
pub(crate) fn stage_row_named(
    txn: &redb::WriteTransaction,
    physical: CollectionId,
) -> Result<Option<StageRow>> {
    named_in(&txn.open_table(tables::REGISTER_STAGES)?, physical)
}

/// [`stage_row_named`] in a read transaction: the judgement made before the writer is taken.
pub(crate) fn stage_row_named_read(
    txn: &redb::ReadTransaction,
    physical: CollectionId,
) -> Result<Option<StageRow>> {
    named_in(&txn.open_table(tables::REGISTER_STAGES)?, physical)
}

fn named_in(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    physical: CollectionId,
) -> Result<Option<StageRow>> {
    let Some(meta) = stage_in(table, physical)? else { return Ok(None) };
    let raw = table.get(physical.0.to_be_bytes().as_slice())?.expect("read just above");
    let rest = &raw.value()[STAGE_ROW_FIXED..];
    let db_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
    let text = |bytes: &[u8]| {
        std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|_| corrupt("a stage row with a name that is not text"))
    };
    Ok(Some(StageRow {
        meta,
        db: text(&rest[2..2 + db_len])?,
        physical: text(&rest[2 + db_len..])?,
    }))
}

impl StageRow {
    /// The logical name of the stage if `entry` is a staging write at `position` (ADR-272, decision 4, rule S; and decision 9, burial
    /// by a later event): a write of the origin of the stage's creation, the job that stages the rows, and never a snapshot document,
    /// which is state and not history. **The one place the rule's trigger and its `Hold` test are written**: rule S for a stage this
    /// member holds and the staging write for a stage it holds no collection of both ask it, over the same row. The binding the write
    /// must stay below, and whether the entry is owed, are asked of the transaction by the caller.
    pub(crate) fn staging_logical(
        &self,
        entry: &kimmy_core::OplogEntry,
        position: crate::engine::Position,
    ) -> Option<&str> {
        if position == crate::engine::Position::Hold {
            return None;
        }
        if entry.stamp.origin() != self.meta.created.origin() {
            return None;
        }
        kimmy_core::swap::logical_of_stage(&self.physical)
    }
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
    // The fixed part, the length of the database name, and the names (a database name is never empty, and neither is a physical's). A row of
    // exactly the fixed part is the form before the row named its stage: a development store of schema 12 written by an earlier build, which
    // 0.62.0 does not upgrade (the schema 12 rule for development stores), refused by name and not read as damage of unknown cause.
    if bytes.len() == STAGE_ROW_FIXED {
        return Err(StorageError::Corrupt(format!(
            "the register holds a stage row of the form before it named its stage: this store was written by a development build of storage schema 12. {}",
            crate::migrate::no_upgrade()
        )));
    }
    if bytes.len() < STAGE_ROW_FIXED + 2 {
        return Err(corrupt("a stage row of the wrong length"));
    }
    let db_len = u16::from_be_bytes([bytes[STAGE_ROW_FIXED], bytes[STAGE_ROW_FIXED + 1]]) as usize;
    if bytes.len() < STAGE_ROW_FIXED + 2 + db_len {
        return Err(corrupt("a stage row of the wrong length"));
    }
    let created: [u8; Stamp::KEY_LEN] = bytes[..Stamp::KEY_LEN].try_into().expect("checked length");
    let deadline: [u8; HLC_ENCODED_LEN] =
        bytes[Stamp::KEY_LEN..STAGE_ROW_FIXED].try_into().expect("checked length");
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

/// The stamp of the newest logical drop of `logical` in effect on this member (a logical drop names no physical): the boundary a
/// definition of the name must be stamped above to belong to its present life, whichever physical stands (ADR-272, decision 6, rule 4).
pub(crate) fn newest_logical_drop_read(
    txn: &redb::ReadTransaction,
    db: &str,
    logical: &str,
) -> Result<Option<Stamp>> {
    Ok(timeline_read(txn, db, logical)?
        .events()
        .iter()
        .filter(|(_, event)| event.physical().is_none())
        .map(|(stamp, _)| *stamp)
        .max())
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
    Ok(put_seeding(txn, db, logical, stamp, event)?.0)
}

/// [`put`], and whether the name's register was seeded from the identity's tombstone on the way (the caller settles the name when it
/// was: a seeded drop is in effect whatever became of the event itself).
pub(crate) fn put_seeding(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    stamp: Stamp,
    event: RegisterEvent,
) -> Result<(Put, bool)> {
    let seeded = seed_from_the_tombstone(txn, db, logical, stage_created_by(txn, db, &event)?)?;
    Ok((store(txn, tables::REGISTER, db, logical, stamp, event)?, seeded))
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
    Ok(park_seeding(txn, db, logical, stamp, event)?.0)
}

/// [`park`], and whether the name's register was seeded on the way, as [`put_seeding`].
pub(crate) fn park_seeding(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    stamp: Stamp,
    event: RegisterEvent,
) -> Result<(Put, bool)> {
    let seeded = seed_from_the_tombstone(txn, db, logical, stage_created_by(txn, db, &event)?)?;
    if timeline(txn, db, logical)?.events().iter().any(|(held, _)| *held == stamp) {
        return Ok((Put::Duplicate, seeded));
    }
    Ok((store(txn, tables::REGISTER_INFO, db, logical, stamp, event)?, seeded))
}

/// The stamp of the creation of the stage `event` names: `None` for an event that names none (a drop, a recreation) and for a
/// binding of a stage whose creation this member does not know.
fn stage_created_by(
    txn: &redb::WriteTransaction,
    db: &str,
    event: &RegisterEvent,
) -> Result<Option<Stamp>> {
    Ok(stage_of(txn, db, event)?.map(|stage| stage.created))
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
    // The name's drop may have been judged ordinary while this event was the only one it had (a drop is logical when an event is in effect
    // or a stage was created below it, and a parked binding of a stage above it is neither): it left its tombstone and nothing in the
    // register, so the event takes effect with no drop below it and the physical it makes visible would have no floor (F3). The seed asks
    // whether an event is in effect, which this one is about to be: once, here, for both of the ways a parked event is released (a window
    // that carries it, `release_ddl_mark_with_effect`, and a grant that covers it, `release_covered`), whose callers settle the name after.
    seed_from_the_tombstone(txn, db, logical, stage_created_by(txn, db, &event)?)?;
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
) -> Result<Vec<(String, String, Stamp)>> {
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
        took_effect.push((db, logical, *stamp));
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
/// in effect (one that is only ahead of its position does not count, F3), a tombstone is a logical drop at its stamp, and a binding stamped above it is not touched by it
/// while one below it is buried by it, as on every other member. Returns whether it wrote the row, so that the caller settles
/// the name: the seeded drop is in effect, and what it ends is buried in the same transaction, whatever became of the event
/// that brought the seed (a first event that is only parked, or a binding judged invalid, included).
///
/// The rule behind it: **a past drop of a name is a logical drop only if a stage of the name was created below it.** The
/// identity not standing, the drop took it whole and the first event that arrives is judged against the drop as it is (no life
/// is left for the seed to bury). The identity standing, a tombstone is also what an ordinary re-create leaves behind (the
/// creation reads it for its floor and never removes it) and what a database's drop leaves on a life it spared, so the
/// seed buries a standing life only when it is the remains this very drop re-dated (it keeps writes stamped after the drop,
/// ADR-231, and its `created` is the drop's successor: a life created after the drop is not what the drop ended) and
/// the swap began below the drop (`stage_created`, the creation of the stage the first event names, is below it). `None`
/// is an event that names no stage, which never seeds on a standing identity.
pub(crate) fn seed_from_the_tombstone(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    stage_created: Option<Stamp>,
) -> Result<bool> {
    let id = CollectionId::derive(db, logical);
    // only an event IN EFFECT keeps the seed away: an event that is still ahead of its position judged no drop, and a drop that arrived
    // while it was the name's only one was judged without it (F3). The drop seeded here is below every event that is parked: it was
    // judged ordinary only because every parked binding names a stage above it
    if !rows_in(&txn.open_table(tables::REGISTER)?, id)?.is_empty() {
        return Ok(false);
    }
    let dropped = {
        let table = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        match table.get(id.0)? {
            Some(raw) => crate::codec::decode_oplog_key(raw.value())?,
            None => return Ok(false),
        }
    };
    let standing = match txn.open_table(tables::COLLECTIONS)?.get((db, logical))? {
        Some(raw) => Some(serde_json::from_slice::<crate::meta::CollectionMeta>(raw.value())?),
        None => None,
    };
    if let Some(meta) = standing {
        let remains_of_this_drop = meta.is_redated() && meta.created == dropped.hlc.successor();
        let began_below = stage_created.is_some_and(|created| created < dropped);
        if !(remains_of_this_drop && began_below) {
            return Ok(false);
        }
    }
    Ok(store(txn, tables::REGISTER, db, logical, dropped, RegisterEvent::LogicalDrop)?
        == Put::Stored)
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

/// Whether the name has a stage created below `stamp`: one this member holds, or one that a binding known ahead of its position names.
/// An event known ahead of its position that names no stage (a recreation, a drop) counts as well: it is the swap's history, and
/// what it is judged against is a drop it was stamped before. A stage whose creation row is missing counts: the member cannot say it
/// is not older, and takes the drop as it always did.
///
/// The test of a replicated drop of a name (ADR-272, decision 6): **a past drop of a name is a logical drop only if a stage of the
/// name was created below it.** A stage created above the drop is one the drop never concerned, whatever order the two arrived in.
pub(crate) fn a_stage_is_older_than(
    txn: &redb::ReadTransaction,
    db: &str,
    logical: &str,
    stamp: &Stamp,
) -> Result<bool> {
    let stages = txn.open_table(tables::REGISTER_STAGES)?;
    let older = |physical: &str| -> Result<bool> {
        Ok(stage_in(&stages, CollectionId::derive(db, physical))?
            .is_none_or(|stage| stage.created < *stamp))
    };
    let collections = txn.open_table(tables::COLLECTIONS)?;
    let prefix = format!("{logical}{}", kimmy_core::swap::STAGE_NAME_MARK);
    for row in collections.range((db, prefix.as_str())..=(db, "\u{10FFFF}"))? {
        let physical = row?.0.value().1.to_string();
        if !physical.starts_with(&prefix) {
            break;
        }
        if older(&physical)? {
            return Ok(true);
        }
    }
    let parked =
        rows_in(&txn.open_table(tables::REGISTER_INFO)?, CollectionId::derive(db, logical))?;
    for (_, event) in parked {
        match event {
            RegisterEvent::Bind { physical } => {
                if older(&physical)? {
                    return Ok(true);
                }
            }
            RegisterEvent::Create { .. } | RegisterEvent::LogicalDrop => return Ok(true),
        }
    }
    Ok(false)
}

/// The latest effective deadline among the stages of the name this member holds, if one has not passed on this member's clock
/// (`now_ms`): the name is mid-`$out` here (ADR-272, decision 12; Q6.5-1). A stage past its deadline is an orphan whose writer vanished,
/// held until a later event of the name removes it, and it is not asked.
pub(crate) fn held_stage_deadline_inside(
    txn: &redb::ReadTransaction,
    db: &str,
    logical: &str,
    now_ms: u64,
) -> Result<Option<Hlc>> {
    let stages = txn.open_table(tables::REGISTER_STAGES)?;
    let collections = txn.open_table(tables::COLLECTIONS)?;
    let prefix = format!("{logical}{}", kimmy_core::swap::STAGE_NAME_MARK);
    let mut latest: Option<Hlc> = None;
    for row in collections.range((db, prefix.as_str())..=(db, "\u{10FFFF}"))? {
        let physical = row?.0.value().1.to_string();
        if !physical.starts_with(&prefix) {
            break;
        }
        // A held stage whose creation row is missing (a restore) has no deadline to read: it is not asked.
        let Some(stage) = stage_in(&stages, CollectionId::derive(db, &physical))? else { continue };
        let deadline = stage.info.effective_deadline(&stage.created);
        if deadline.wall_ms > now_ms && latest.is_none_or(|held| deadline > held) {
            latest = Some(deadline);
        }
    }
    Ok(latest)
}

/// Whether the name has a register event in effect on this member: not the events known ahead of their position.
pub(crate) fn has_events_in_effect(
    txn: &redb::ReadTransaction,
    db: &str,
    logical: &str,
) -> Result<bool> {
    Ok(!rows_in(&txn.open_table(tables::REGISTER)?, CollectionId::derive(db, logical))?.is_empty())
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

/// Test only: remove a stage's creation row, as a restore leaves a member (`REGISTER_STAGES` is not carried by a backup).
#[cfg(test)]
pub(crate) fn remove_stage_row_for_test(engine: &crate::Engine, physical: CollectionId) {
    let txn = engine.begin_write(crate::WriterHolder::Ddl).unwrap();
    txn.open_table(tables::REGISTER_STAGES)
        .unwrap()
        .remove(physical.0.to_be_bytes().as_slice())
        .unwrap();
    txn.commit().unwrap();
}

/// Test only: write a stage row in the form before the row named its stage (the creation's stamp key and the deadline only), as a development store of
/// schema 12 written by an earlier build holds it.
#[cfg(test)]
pub(crate) fn write_old_form_stage_row_for_test(engine: &crate::Engine, physical: CollectionId) {
    let txn = engine.begin_write(crate::WriterHolder::Ddl).unwrap();
    txn.open_table(tables::REGISTER_STAGES)
        .unwrap()
        .insert(
            physical.0.to_be_bytes().as_slice(),
            [0u8; Stamp::KEY_LEN + HLC_ENCODED_LEN].as_slice(),
        )
        .unwrap();
    txn.commit().unwrap();
}
