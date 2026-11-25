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

/// What the settle pass decided for a name, as the stored visibility row keeps it (ADR-272; Q6.6, the resolve row): which kind of answer, the
/// stamp of the event that is visible, the physical it names, the newest logical drop in effect (what rule 4 needs), and the newest event
/// stored for the name when the pass decided (what a reader checks the row is still current against).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VisibleKind {
    Identity,
    Bound,
    Created,
    Dropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VisibleRow {
    pub(crate) kind: VisibleKind,
    pub(crate) event: Option<Stamp>,
    pub(crate) physical: String,
    pub(crate) newest_drop: Option<Stamp>,
    pub(crate) newest_event: Option<Stamp>,
}

impl VisibleRow {
    /// The row for what the pass decided: `visible` over the name's events, `newest_drop` in effect and `newest_event` stored.
    pub(crate) fn decided(
        visible: &kimmy_core::register_state::Visible,
        newest_drop: Option<Stamp>,
        newest_event: Option<Stamp>,
    ) -> Self {
        use kimmy_core::register_state::Visible;
        let (kind, event, physical) = match visible {
            Visible::Identity => (VisibleKind::Identity, None, String::new()),
            Visible::Bound { physical, stamp } => {
                (VisibleKind::Bound, Some(*stamp), physical.clone())
            }
            Visible::Created { physical, stamp } => {
                (VisibleKind::Created, Some(*stamp), physical.clone())
            }
            Visible::Dropped { stamp } => (VisibleKind::Dropped, Some(*stamp), String::new()),
        };
        Self { kind, event, physical, newest_drop, newest_event }
    }

    fn encode(&self) -> Vec<u8> {
        let kind = match self.kind {
            VisibleKind::Identity => 0u8,
            VisibleKind::Bound => 1,
            VisibleKind::Created => 2,
            VisibleKind::Dropped => 3,
        };
        let flags = u8::from(self.event.is_some())
            | (u8::from(self.newest_drop.is_some()) << 1)
            | (u8::from(self.newest_event.is_some()) << 2);
        let mut out = vec![kind, flags];
        for stamp in [self.event, self.newest_drop, self.newest_event].into_iter().flatten() {
            out.extend_from_slice(&stamp.to_key_bytes());
        }
        out.extend_from_slice(self.physical.as_bytes());
        out
    }

    /// `None` for bytes that are not a row: a derived row that does not decode is a row that is not there.
    fn decode(bytes: &[u8]) -> Option<Self> {
        let (&kind, rest) = bytes.split_first()?;
        let (&flags, mut rest) = rest.split_first()?;
        let kind = match kind {
            0 => VisibleKind::Identity,
            1 => VisibleKind::Bound,
            2 => VisibleKind::Created,
            3 => VisibleKind::Dropped,
            _ => return None,
        };
        let mut stamp = |present: bool| -> Option<Option<Stamp>> {
            if !present {
                return Some(None);
            }
            let (head, tail) = rest.split_at_checked(Stamp::KEY_LEN)?;
            rest = tail;
            Some(Some(Stamp::from_key_bytes(head.try_into().ok()?)))
        };
        let event = stamp(flags & 1 != 0)?;
        let newest_drop = stamp(flags & 2 != 0)?;
        let newest_event = stamp(flags & 4 != 0)?;
        let physical = String::from_utf8(rest.to_vec()).ok()?;
        Some(Self { kind, event, physical, newest_drop, newest_event })
    }
}

fn visible_key(db: &str, logical: &str) -> [u8; 8] {
    CollectionId::derive(db, logical).0.to_be_bytes()
}

/// The stored visibility row of `logical`, if there is one that decodes.
pub(crate) fn visible_row_read(
    txn: &redb::ReadTransaction,
    db: &str,
    logical: &str,
) -> Result<Option<VisibleRow>> {
    let table = txn.open_table(tables::REGISTER_VISIBLE)?;
    let found = table.get(visible_key(db, logical).as_slice())?;
    Ok(found.and_then(|raw| VisibleRow::decode(raw.value())))
}

/// The stamp of the newest event stored for `logical` in `REGISTER`, by one reverse range read: what a stored row is checked current against.
pub(crate) fn newest_stored_event_read(
    txn: &redb::ReadTransaction,
    db: &str,
    logical: &str,
) -> Result<Option<Stamp>> {
    #[cfg(test)]
    REGISTER_OPENED.with(|n| n.set(n.get() + 1));
    newest_event_of(&txn.open_table(tables::REGISTER)?, CollectionId::derive(db, logical))
}

/// [`newest_stored_event_read`] inside a write transaction (the pass).
pub(crate) fn newest_stored_event(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
) -> Result<Option<Stamp>> {
    newest_event_of(&txn.open_table(tables::REGISTER)?, CollectionId::derive(db, logical))
}

fn newest_event_of(
    rows: &impl ReadableTable<&'static [u8], &'static [u8]>,
    id: CollectionId,
) -> Result<Option<Stamp>> {
    let lo = row_key(id, &Stamp::from_key_bytes([0; Stamp::KEY_LEN]));
    let hi = row_key(id, &Stamp::from_key_bytes([0xFF; Stamp::KEY_LEN]));
    match rows.range(lo.as_slice()..=hi.as_slice())?.next_back() {
        Some(row) => Ok(Some(crate::codec::decode_oplog_key(&row?.0.value()[8..])?)),
        None => Ok(None),
    }
}

/// Keep what was decided for `logical` (the settle pass in its own transaction, a restore from what it computes). A row that is already
/// what was decided is not written again, so a pass that decides nothing new dirties no page. **A name with no stored event has no row**:
/// a row whose `newest_event` is `None` is not written and removes any row there, so every writer obeys the rule and none can leave a
/// row for a name that has nothing to cache (a stage's creation runs the pass with no event; a restore names every nameable name).
pub(crate) fn write_visible_row(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    row: &VisibleRow,
) -> Result<()> {
    let mut table = txn.open_table(tables::REGISTER_VISIBLE)?;
    let key = visible_key(db, logical);
    if row.newest_event.is_none() {
        table.remove(key.as_slice())?;
        return Ok(());
    }
    let bytes = row.encode();
    let same = table.get(key.as_slice())?.is_some_and(|held| held.value() == bytes.as_slice());
    if !same {
        table.insert(key.as_slice(), bytes.as_slice())?;
    }
    Ok(())
}

/// Delete the visibility row of the name a collection named `physical` belongs to (the name itself, or the logical name of a stage or a
/// recreation), in the transaction that makes the collection arrive or leave: a row that may be stale is never left. A missing row is a
/// counted fallback to the computation, which is always right; the pass, where it runs, writes the row again.
pub(crate) fn forget_visible_row(
    txn: &redb::WriteTransaction,
    db: &str,
    physical: &str,
) -> Result<()> {
    let logical = kimmy_core::swap::logical_of_stage(physical)
        .or_else(|| kimmy_core::logical_of_recreation(physical))
        .unwrap_or(physical);
    txn.open_table(tables::REGISTER_VISIBLE)?.remove(visible_key(db, logical).as_slice())?;
    Ok(())
}

/// The stored visibility row of `logical`, read outside any write transaction. Test builds only.
#[cfg(test)]
pub(crate) fn visible_row_for_tests(
    engine: &crate::Engine,
    db: &str,
    logical: &str,
) -> Result<Option<VisibleRow>> {
    use redb::ReadableDatabase;
    let txn = engine.db().begin_read()?;
    visible_row_read(&txn, db, logical)
}

#[cfg(test)]
thread_local! {
    static ROWS_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many `REGISTER` rows this thread has read through [`rows_in`], for a test of what a resolution costs.
#[cfg(test)]
pub(crate) fn rows_read_for_tests() -> usize {
    ROWS_READ.with(std::cell::Cell::get)
}

#[cfg(test)]
thread_local! {
    static REGISTER_OPENED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many times this thread has opened `REGISTER` to read one name's events (the newest-event probe and the timeline), for a test of what
/// a resolution costs.
#[cfg(test)]
pub(crate) fn register_opened_for_tests() -> usize {
    REGISTER_OPENED.with(std::cell::Cell::get)
}

/// Create the register's tables, so a read of them never meets a missing table.
pub(crate) fn open_tables(txn: &redb::WriteTransaction) -> Result<()> {
    let _ = txn.open_table(tables::REGISTER)?;
    let _ = txn.open_table(tables::REGISTER_INFO)?;
    let _ = txn.open_table(tables::REGISTER_STAGES)?;
    let _ = txn.open_table(tables::REGISTER_POST)?;
    let _ = txn.open_table(tables::REGISTER_VISIBLE)?;
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
    #[cfg(test)]
    ROWS_READ.with(|n| n.set(n.get() + held.len()));
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
    #[cfg(test)]
    REGISTER_OPENED.with(|n| n.set(n.get() + 1));
    timeline_read_of(txn, tables::REGISTER, db, logical)
}

/// The stamp of the newest logical drop of `logical` in effect on this member (a logical drop names no physical): the boundary a
/// definition of the name must be stamped above to belong to its present life, whichever physical stands (ADR-272, decision 6, rule 4).
///
/// Read from the name's stored visibility row when it is current, that is when the newest event of the name is the one the row was decided
/// against (ADR-272; Q6.6, the resolve row), and computed from the events when it is not. The second answer says whether the row was stale
/// (the name has events and the row is missing or not current), so the caller counts it; the answer is right either way. A name with no
/// event has no drop.
pub(crate) fn newest_logical_drop_read(
    txn: &redb::ReadTransaction,
    db: &str,
    logical: &str,
) -> Result<(Option<Stamp>, bool)> {
    let Some(newest) = newest_stored_event_read(txn, db, logical)? else {
        return Ok((None, false));
    };
    if let Some(row) = visible_row_read(txn, db, logical)?
        && row.newest_event == Some(newest)
    {
        return Ok((row.newest_drop, false));
    }
    let computed = timeline_read(txn, db, logical)?
        .events()
        .iter()
        .filter(|(_, event)| event.physical().is_none())
        .map(|(stamp, _)| *stamp)
        .max();
    Ok((computed, true))
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
    let seeded = seed_for_event(txn, db, logical, &event)?;
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
    let seeded = seed_for_event(txn, db, logical, &event)?;
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

/// The seed an event asks for: a binding asks as a binding, with the creation of the stage it names; a recreation or a drop names no
/// stage and asks as such ([`SeedAsk`]).
fn seed_for_event(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    event: &RegisterEvent,
) -> Result<bool> {
    let ask = match event {
        RegisterEvent::Bind { .. } => SeedAsk::Binding,
        RegisterEvent::Create { .. } | RegisterEvent::LogicalDrop => SeedAsk::NoStage,
    };
    seed_from_the_tombstone(txn, db, logical, stage_created_by(txn, db, event)?, ask)
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
    seed_for_event(txn, db, logical, &event)?;
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

/// Who asks the seed (ADR-272, decision 6): the creation of a stage, a binding, or an event that names no stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SeedAsk {
    /// The creation of a stage of the name: an entry in a window, or a stage a snapshot page made.
    StageCreation,
    /// A binding (put, parked or released), with the creation of the stage it names.
    Binding,
    /// A recreation's `Create`, a logical drop, or a restart's seed: no stage is named.
    NoStage,
}

/// The first event of a name seeds the register from the identity's drop (Q6.4 item 1b): a member that applied a drop of the
/// name before it knew of any swap holds that drop only as the identity's tombstone, and the event that arrives afterwards
/// has to be judged against it, as it is on a member that knew of the swap when the drop arrived. With no event of the name
/// in effect (one that is only ahead of its position does not count, F3), a tombstone is a logical drop at its stamp, and a binding stamped above it is not touched by it
/// while one below it is buried by it, as on every other member. Returns whether it wrote the row, so that the caller settles
/// the name: the seeded drop is in effect, and what it ends is buried in the same transaction, whatever became of the event
/// that brought the seed (a first event that is only parked, or a binding judged invalid, included).
///
/// The rule behind it: **a past drop of a name is a logical drop only if a stage of the name was created below it.** What the
/// stage's stamp alone cannot answer is a binding of a stage created ABOVE the drop on a member whose identity is gone: the drop has
/// to be in the register below the binding all the same (it floors what the binding makes visible, F3), and the binding does not
/// hide a later life, because the binding buries the identity whatever its age.
///
/// - **The identity absent** (the drop took it whole, and no life is left for the seed to bury): a stage's creation seeds only when
///   the stage was created below the drop (a stage above it is one the drop never concerned, and a later life that a member made
///   without knowing of the swap would be hidden by a drop no member that met the creation after it would hold); a binding seeds
///   whatever the stage's stamp (F3); an event that names no stage seeds (the recreation's `Create`, Q6.6 unit C2e).
/// - **The identity standing** (a life created after the drop, or the remains the drop kept): judged by the stage's stamp alone, for a
///   stage's creation and for a binding: the stage created below the drop, the swap began below it and the life is the one the drop
///   hides on every member that knew of the swap. An event that names no stage never seeds on a standing identity. A life lost this way
///   is a life a member made without knowing of the swap (lost and reported).
/// - **The database guard.** The tombstone a database's drop leaves on a collection (its stamp is the database's drop) is not a drop of a
///   life made after it: such a life stands over the tombstone, the drop never ended it, and no stage met late makes it the swap's. It is
///   never seeded over. What the database's drop DID end and keep (the remains it re-dated, `created` its successor) is still the
///   drop's, and is buried by a swap that began below it as it is by a collection's drop.
pub(crate) fn seed_from_the_tombstone(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    stage_created: Option<Stamp>,
    ask: SeedAsk,
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
    let began_below = stage_created.is_some_and(|created| created < dropped);
    let seeds = match &standing {
        Some(meta) => {
            // the database guard: over the tombstone of a database's drop only the remains that very drop re-dated are its to bury
            let by_the_database = txn
                .open_table(tables::COLLECTIONS_DROPPED)?
                .get(kimmy_core::database_drop_id(db).0)?
                .map(|held| crate::codec::decode_oplog_key(held.value()))
                .transpose()?
                .is_some_and(|database| database == dropped);
            let remains_of_this_drop = meta.is_redated() && meta.created == dropped.hlc.successor();
            began_below && (remains_of_this_drop || !by_the_database)
        }
        None => match ask {
            SeedAsk::StageCreation => began_below,
            SeedAsk::Binding | SeedAsk::NoStage => true,
        },
    };
    if !seeds {
        return Ok(false);
    }
    Ok(store(txn, tables::REGISTER, db, logical, dropped, RegisterEvent::LogicalDrop)?
        == Put::Stored)
}

/// Whether the register owns the collection id `id`: a stage row is kept under it (a stage this member has the creation of, whether or
/// not it still holds the collection), or register rows are kept under it (the identity of a name this member has events of, in effect
/// or ahead of their position). By id and never by name, because a register row does not carry its name. A stage this member has no
/// creation row for is not recognised by id; the snapshot's stage rows (Q6.6, unit D) cover it.
pub(crate) fn owns_id(txn: &redb::ReadTransaction, id: CollectionId) -> Result<bool> {
    if txn.open_table(tables::REGISTER_STAGES)?.get(id.0.to_be_bytes().as_slice())?.is_some() {
        return Ok(true);
    }
    let events = rows_in(&txn.open_table(tables::REGISTER)?, id)?;
    let parked = rows_in(&txn.open_table(tables::REGISTER_INFO)?, id)?;
    Ok(!(events.is_empty() && parked.is_empty()))
}

/// The collection ids the register owns, for the drops a sender carries on a whole-database page (ADR-272, decisions 4 and 6; Q6.6, unit
/// C3): the id of every stage row (a stage whose creation this member has, alive or buried), the identity's id of every name that has a row
/// in REGISTER (an event in effect, a binding or a drop: the burial of the identity is the register's to decide, and the events carry it),
/// and the id of each recreation a `Create` event names, whose database is found through the name's stage rows. **Not** the identity of a name
/// whose events are only parked (REGISTER_INFO): its drop, if it has one, is only in the tombstone, and leaving that out would leave a
/// receiver holding the identity for good. A recreation of a name that has no stage row cannot be named here (its database is not in
/// the row); the receiver's own deferral is the second line for it.
pub(crate) fn owned_ids(
    txn: &redb::ReadTransaction,
) -> Result<std::collections::HashSet<CollectionId>> {
    let mut owned = std::collections::HashSet::new();
    let mut dbs: std::collections::HashMap<CollectionId, String> = Default::default();
    let stages = txn.open_table(tables::REGISTER_STAGES)?;
    for row in stages.iter()? {
        let (key, _) = row?;
        let key: [u8; 8] =
            key.value().try_into().map_err(|_| corrupt("a stage row key is not eight bytes"))?;
        let id = CollectionId(u64::from_be_bytes(key));
        owned.insert(id);
        if let Some(named) = named_in(&stages, id)?
            && let Some(logical) = kimmy_core::swap::logical_of_stage(&named.physical)
        {
            dbs.insert(CollectionId::derive(&named.db, logical), named.db.clone());
        }
    }
    let events = txn.open_table(tables::REGISTER)?;
    for row in events.iter()? {
        let (key, value) = row?;
        let key = key.value();
        if key.len() != ROW_KEY_LEN {
            return Err(corrupt("a register row key is the wrong length"));
        }
        let logical = CollectionId(u64::from_be_bytes(key[..8].try_into().expect("8 bytes")));
        owned.insert(logical);
        let event = RegisterEvent::decode(value.value()).map_err(|e| corrupt(&e.to_string()))?;
        if let RegisterEvent::Create { physical } = event
            && let Some(db) = dbs.get(&logical)
        {
            owned.insert(CollectionId::derive(db, &physical));
        }
    }
    Ok(owned)
}

/// What a snapshot's register carry did (Q6.6, unit C2b): the names it touched, and whether it wrote
/// anything at all. A page this member already holds writes nothing, and the page's transaction
/// then commits nothing.
pub(crate) struct CarryApplied {
    pub(crate) names: Vec<CarriedName>,
    pub(crate) wrote: bool,
}

/// An event of a carry, held to the grant: its stamp, the decoded event, and the raw row it came in (key and value).
type CarriedEvent<'a> = (Stamp, RegisterEvent, &'a [u8], &'a [u8]);

/// What a snapshot's register carry did for one name it could name (Q6.6, unit C2b): the names, and the stamps of the events it put in
/// effect, which the settle pass is told.
pub(crate) struct CarriedName {
    pub(crate) db: String,
    pub(crate) logical: String,
    pub(crate) stored: Vec<Stamp>,
}

/// The names and the key of a stage row as a page carries it: the same shape check a restore makes, so a damaged carry is refused by name.
fn stage_row_names(
    key: &[u8],
    value: &[u8],
) -> std::result::Result<(String, String), &'static str> {
    if key.len() != 8 || value.len() < STAGE_ROW_FIXED + 2 {
        return Err("a stage row of the wrong shape");
    }
    let db_len = u16::from_be_bytes([value[STAGE_ROW_FIXED], value[STAGE_ROW_FIXED + 1]]) as usize;
    if value.len() < STAGE_ROW_FIXED + 2 + db_len {
        return Err("a stage row of the wrong shape");
    }
    let text = |bytes: &[u8]| {
        std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|_| "a stage row with a name that is not text")
    };
    let db = text(&value[STAGE_ROW_FIXED + 2..STAGE_ROW_FIXED + 2 + db_len])?;
    let physical = text(&value[STAGE_ROW_FIXED + 2 + db_len..])?;
    if CollectionId::derive(&db, &physical).0.to_be_bytes().as_slice() != key {
        return Err("a stage row whose names derive another key than its own");
    }
    Ok((db, physical))
}

/// Seed the name's drop from the receiver's own tombstone for the identity, but only when that tombstone is ABOVE the grant for its
/// origin (Q6.6, unit C2b; D1): one the sender could not have judged. A tombstone at or below the grant is the sender's to have used, and
/// seeding from it would put in this register a drop the sender's register does not hold.
fn seed_above_the_grant(
    txn: &redb::WriteTransaction,
    db: &str,
    logical: &str,
    granted: &kimmy_core::VersionVector,
) -> Result<bool> {
    let id = CollectionId::derive(db, logical);
    let own = {
        let table = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        match table.get(id.0)? {
            Some(raw) => Some(crate::codec::decode_oplog_key(raw.value())?),
            None => None,
        }
    };
    match own {
        Some(stamp) if stamp.hlc > granted.get(stamp.origin()) => {
            seed_from_the_tombstone(txn, db, logical, None, SeedAsk::NoStage)
        }
        _ => Ok(false),
    }
}

/// The ids of the stage rows this member holds whose collection is NOT alive here (a stage it doomed or buried, or never held): what a
/// whole-database snapshot's last page lists (ADR-272, decision 9; Q6.6, unit D). Read in the same read transaction as the carry's tables, so
/// that a stage buried between two reads is not listed with its row and flagged alive.
pub(crate) fn not_alive_stage_ids(txn: &redb::ReadTransaction) -> Result<Vec<Vec<u8>>> {
    let stages = txn.open_table(tables::REGISTER_STAGES)?;
    let collections = txn.open_table(tables::COLLECTIONS)?;
    let mut out = Vec::new();
    for row in stages.iter()? {
        let (key, value) = row?;
        let (db, physical) = stage_row_names(key.value(), value.value()).map_err(corrupt)?;
        if collections.get((db.as_str(), physical.as_str()))?.is_none() {
            out.push(key.value().to_vec());
        }
    }
    Ok(out)
}

/// D1 for an id that stays unnamed (Q6.6, unit C2e): when the receiver holds a tombstone for it ABOVE the grant and `REGISTER` holds no row
/// for it, the tombstone is the name's drop (one the sender could not have judged), stored by id as `seed_above_the_grant` stores it for a
/// name: no name is needed to write the row.
fn seed_by_id_above_the_grant(
    txn: &redb::WriteTransaction,
    id: u64,
    granted: &kimmy_core::VersionVector,
) -> Result<bool> {
    if !rows_in(&txn.open_table(tables::REGISTER)?, CollectionId(id))?.is_empty() {
        return Ok(false);
    }
    let own = {
        let table = txn.open_table(tables::COLLECTIONS_DROPPED)?;
        match table.get(id)? {
            Some(raw) => Some(crate::codec::decode_oplog_key(raw.value())?),
            None => None,
        }
    };
    match own {
        Some(stamp) if stamp.hlc > granted.get(stamp.origin()) => {
            let key = row_key(CollectionId(id), &stamp);
            let row = RegisterEvent::LogicalDrop.encode();
            txn.open_table(tables::REGISTER)?.insert(key.as_slice(), row.as_slice())?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Name the ids in `unnamed` that a collection this member holds belongs to: a hidden collection (a stage, a recreation), whose logical
/// name is the part of its physical name before the mark, or the collection itself, which IS the name (the identity of a swapped name that
/// a stale member still holds). The id derives from the database and the name (a register row does not carry its name).
fn name_from_held_collections(
    txn: &redb::WriteTransaction,
    unnamed: &std::collections::BTreeSet<u64>,
    names: &mut std::collections::BTreeMap<u64, (String, String)>,
) -> Result<()> {
    let collections = txn.open_table(tables::COLLECTIONS)?;
    for row in collections.iter()? {
        let (key, _) = row?;
        let (db, physical) = key.value();
        // the collection IS the name: the identity of a swapped name, which a stale member still holds
        let plain = CollectionId::derive(db, physical).0;
        if unnamed.contains(&plain) {
            names.insert(plain, (db.to_string(), physical.to_string()));
        }
        let Some(logical) = kimmy_core::swap::logical_of_stage(physical)
            .or_else(|| kimmy_core::logical_of_recreation(physical))
        else {
            continue;
        };
        let id = CollectionId::derive(db, logical).0;
        if unnamed.contains(&id) {
            names.insert(id, (db.to_string(), logical.to_string()));
        }
    }
    Ok(())
}

/// Put what a snapshot's last page carries of the sender's register into effect, in the last page's transaction (ADR-272, decisions 4
/// and 6; Q6.6, unit C2b). The stage rows first (the door judges every binding by them); then, for each name, the events through the
/// one door with no seed of their own (one seed per name, before them, from the receiver's own tombstone, and only above the grant: D1),
/// a timeline built once per name; then the post-drop rows, only for a name that reads dropped (D3). Every event and every definition of a
/// post-drop row is held to the grant the page's first vector gave: above it, it is dropped and arrives by windows. A binding the door
/// refuses (an unknown stage, one above its deadline) is a damaged page and fails it. A name the receiver cannot name (no stage row) has its
/// rows stored by id and nothing else. The caller absorbs the grant and settles the returned names.
pub(crate) fn apply_carried(
    txn: &redb::WriteTransaction,
    carry: &crate::snapshot::RegisterCarry,
    granted: &kimmy_core::VersionVector,
) -> Result<CarryApplied> {
    let damaged = |what: &str| {
        StorageError::Corrupt(format!("a snapshot page's register is damaged: {what}"))
    };
    let mut wrote = false;
    let mut fresh: std::collections::BTreeSet<u64> = Default::default();
    let mut names: std::collections::BTreeMap<u64, (String, String)> = Default::default();
    {
        let mut stages = txn.open_table(tables::REGISTER_STAGES)?;
        for (key, value) in &carry.stages {
            let (db, physical) = stage_row_names(key, value).map_err(damaged)?;
            let held = stages.get(key.as_slice())?.map(|raw| raw.value().to_vec());
            if held.is_none() {
                stages.insert(key.as_slice(), value.as_slice())?;
                wrote = true;
            }
            if let Some(logical) = kimmy_core::swap::logical_of_stage(&physical) {
                let id = CollectionId::derive(&db, logical).0;
                if held.is_none() {
                    fresh.insert(id);
                }
                names.insert(id, (db, logical.to_string()));
            }
        }
    }
    let mut events: std::collections::BTreeMap<u64, Vec<CarriedEvent<'_>>> = Default::default();
    for (key, value) in &carry.events {
        if key.len() != ROW_KEY_LEN {
            return Err(damaged("a register row key of the wrong length"));
        }
        let id = u64::from_be_bytes(key[..8].try_into().expect("8 bytes"));
        let stamp = crate::codec::decode_oplog_key(&key[8..])?;
        let event = RegisterEvent::decode(value).map_err(|e| damaged(&e.to_string()))?;
        // above the grant: not taken, it arrives by windows; the sender is not trusted to filter
        if stamp.hlc > granted.get(stamp.origin()) {
            continue;
        }
        events.entry(id).or_default().push((stamp, event, key.as_slice(), value.as_slice()));
    }
    // A name the carry's stage rows do not name may still be named by a hidden collection this member holds (Q6.6, unit C2e): a stage or a
    // recreation of it that a page from an older sender made with no stage row. The member's own collections are what the join's earlier
    // pages created, so this covers a join of several pages as the page's own collection states would, and a name found here goes
    // through the same door, pass and row as any other.
    let unnamed: std::collections::BTreeSet<u64> =
        events.keys().filter(|id| !names.contains_key(id)).copied().collect();
    if !unnamed.is_empty() {
        name_from_held_collections(txn, &unnamed, &mut names)?;
    }
    let mut named = Vec::new();
    for (id, mut group) in events {
        group.sort_by_key(|(stamp, ..)| *stamp);
        let Some((db, logical)) = names.get(&id) else {
            // an id nothing names: its events are stored by id, and D1 seeds its drop by id from the receiver's own tombstone
            if seed_by_id_above_the_grant(txn, id, granted)? {
                wrote = true;
            }
            let mut table = txn.open_table(tables::REGISTER)?;
            for (_, _, key, value) in &group {
                if table.insert(*key, *value)?.is_none_or(|old| old.value() != *value) {
                    wrote = true;
                }
            }
            continue;
        };
        let seeded = seed_above_the_grant(txn, db, logical, granted)?;
        wrote |= seeded;
        let mut timeline = timeline_of(txn, tables::REGISTER, db, logical)?;
        let mut stored = Vec::new();
        let mut table = txn.open_table(tables::REGISTER)?;
        for (stamp, event, key, value) in &group {
            let stage = stage_of(txn, db, event)?;
            let stage = stage.as_ref().map(|s| (&s.created, &s.info));
            match timeline.insert(*stamp, event.clone(), stage) {
                Put::Stored => {
                    table.insert(*key, *value)?;
                    stored.push(*stamp);
                    wrote = true;
                }
                Put::Duplicate => {}
                Put::UnknownStage => {
                    let what = format!("a binding of a stage with no row: {event:?}");
                    return Err(damaged(&what));
                }
                Put::InvalidBinding => {
                    let what = format!("a binding above its stage's deadline: {event:?}");
                    return Err(damaged(&what));
                }
            }
        }
        drop(table);
        // a name the carry changed nothing of needs no pass: a page this member holds commits nothing
        if seeded || !stored.is_empty() || fresh.contains(&id) {
            named.push(CarriedName { db: db.clone(), logical: logical.clone(), stored });
        }
    }
    for (key, value) in &carry.posts {
        if key.len() != 8 {
            return Err(damaged("a post-drop row key of the wrong length"));
        }
        let id = CollectionId(u64::from_be_bytes(key[..8].try_into().expect("8 bytes")));
        // only for a name that reads dropped: its newest event in effect is a logical drop, which is always applicable
        let held_events = rows_in(&txn.open_table(tables::REGISTER)?, id)?;
        let Some((drop_at, RegisterEvent::LogicalDrop)) = held_events.last().cloned() else {
            continue;
        };
        let mut row: PostRow = serde_json::from_slice(value)
            .map_err(|_| damaged("a post-drop row that does not decode"))?;
        row.indexes.retain(|i| i.created.is_none_or(|c| c.hlc <= granted.get(c.origin())));
        row.tombstones.retain(|(_, s)| s.hlc <= granted.get(s.origin()));
        if row.validator_changed.is_some_and(|c| c.hlc > granted.get(c.origin())) {
            row.validator = None;
            row.validator_changed = None;
        }
        let mut table = txn.open_table(tables::REGISTER_POST)?;
        let held = table.get(key.as_slice())?.map(|raw| raw.value().to_vec());
        let before = held.clone();
        let merged = match held {
            Some(held) => {
                let mut held: PostRow = serde_json::from_slice(&held)?;
                let floor = Some(drop_at.hlc);
                held.merge(
                    &row.indexes,
                    &row.tombstones,
                    row.validator_changed.map(|changed| (&row.validator, changed)),
                    floor,
                );
                held.definition_floor =
                    [held.definition_floor, row.definition_floor].into_iter().flatten().max();
                held
            }
            None => row,
        };
        let bytes = serde_json::to_vec(&merged)?;
        if before.as_deref() != Some(bytes.as_slice()) {
            table.insert(key.as_slice(), bytes.as_slice())?;
            wrote = true;
        }
    }
    Ok(CarryApplied { names: named, wrote })
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

// -- The backup (ADR-272, decisions 4 and 13; Q6.6, unit A) -------------------------------------------------------------------------------

/// The four tables a backup carries: no other module opens them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Carried {
    Register,
    Info,
    Stages,
    Post,
}

impl Carried {
    fn table(self) -> redb::TableDefinition<'static, &'static [u8], &'static [u8]> {
        match self {
            Carried::Register => tables::REGISTER,
            Carried::Info => tables::REGISTER_INFO,
            Carried::Stages => tables::REGISTER_STAGES,
            Carried::Post => tables::REGISTER_POST,
        }
    }
}

/// The rows of one carried table, as stored (key and value): a backup copies them whole. The tables are small (a row for each event of a
/// swapped name, and a stage row for each stage the member has met).
pub(crate) fn carried_rows(
    txn: &redb::ReadTransaction,
    which: Carried,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let table = match txn.open_table(which.table()) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut rows = Vec::new();
    for row in table.iter()? {
        let (key, value) = row?;
        rows.push((key.value().to_vec(), value.value().to_vec()));
    }
    Ok(rows)
}

/// One row of a backup, written as it was read. The restore transcribes and never judges: [`check_restored`] judges the whole.
pub(crate) fn restore_row(
    txn: &redb::WriteTransaction,
    which: Carried,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    txn.open_table(which.table())?.insert(key, value)?;
    Ok(())
}

fn damaged(what: &str) -> StorageError {
    StorageError::Database(format!(
        "the backup is refused as damaged: {what}; nothing was restored"
    ))
}

fn raw_rows(txn: &redb::WriteTransaction, which: Carried) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let table = txn.open_table(which.table())?;
    let mut rows = Vec::new();
    for row in table.iter()? {
        let (key, value) = row?;
        rows.push((key.value().to_vec(), value.value().to_vec()));
    }
    Ok(rows)
}

/// The consistency check of a restore, in the restore's own transaction and by id (a register row does not carry its name). A backup is one
/// read transaction, so its register and its collections agree by construction: this is for a damaged or hand-made backup, and a future
/// build's mistake. It refuses by the check's own message, in this order: every row decodes; a stage row's names derive its own key; every
/// binding has its stage row; every hidden collection is known to the register (a recreation with no `Create` event is known when its
/// stamp, which its name carries, is above the carried vector); no row of REGISTER is above the carried vector; no row of
/// REGISTER_INFO is at or below it, and each has its entry; and what a flip leaves cannot be below its own floor (check 4).
/// It also writes the visibility row of every name it can name, from what check 4 computes (the row is derived and not carried).
pub(crate) fn check_restored(txn: &redb::WriteTransaction) -> Result<()> {
    use kimmy_core::register_state::{Ready, Visible};

    // the vector the backup carries
    let vector = {
        let mut vector = kimmy_core::VersionVector::new();
        let versions = txn.open_table(tables::OPLOG_VERSIONS)?;
        for row in versions.iter()? {
            let (node, hlc) = row?;
            let origin: [u8; kimmy_core::Origin::LEN] = node
                .value()
                .try_into()
                .map_err(|_| damaged("a version vector key is not an origin"))?;
            let hlc: [u8; HLC_ENCODED_LEN] = hlc
                .value()
                .try_into()
                .map_err(|_| damaged("a version vector value is not a stamp"))?;
            vector.insert(kimmy_core::Origin::from_bytes(origin), Hlc::from_bytes(hlc));
        }
        vector
    };

    // every row decodes
    let mut events: Vec<(bool, CollectionId, Stamp, RegisterEvent)> = Vec::new();
    for (parked, which) in [(false, Carried::Register), (true, Carried::Info)] {
        for (key, value) in raw_rows(txn, which)? {
            if key.len() != ROW_KEY_LEN {
                return Err(damaged("a register row does not decode: its key is the wrong length"));
            }
            let id = CollectionId(u64::from_be_bytes(key[..8].try_into().expect("8 bytes")));
            let stamp = crate::codec::decode_oplog_key(&key[8..])
                .map_err(|_| damaged("a register row does not decode: its stamp"))?;
            let event = RegisterEvent::decode(&value)
                .map_err(|e| damaged(&format!("a register row does not decode: {e}")))?;
            events.push((parked, id, stamp, event));
        }
    }

    // a stage row's database and name derive its own key (and a row of the wrong shape does not decode)
    let mut stages: Vec<(String, String)> = Vec::new();
    for (key, value) in raw_rows(txn, Carried::Stages)? {
        if key.len() != 8 || value.len() < STAGE_ROW_FIXED + 2 {
            return Err(damaged("a stage row does not decode"));
        }
        let db_len =
            u16::from_be_bytes([value[STAGE_ROW_FIXED], value[STAGE_ROW_FIXED + 1]]) as usize;
        if value.len() < STAGE_ROW_FIXED + 2 + db_len {
            return Err(damaged("a stage row does not decode"));
        }
        let text = |bytes: &[u8]| {
            std::str::from_utf8(bytes)
                .map(str::to_string)
                .map_err(|_| damaged("a stage row does not decode: a name that is not text"))
        };
        let db = text(&value[STAGE_ROW_FIXED + 2..STAGE_ROW_FIXED + 2 + db_len])?;
        let physical = text(&value[STAGE_ROW_FIXED + 2 + db_len..])?;
        if CollectionId::derive(&db, &physical).0.to_be_bytes().as_slice() != key.as_slice() {
            return Err(damaged(&format!(
                "a stage row whose names derive another key than its own: {db}.{physical}"
            )));
        }
        stages.push((db, physical));
    }

    // every binding has its stage row: found by the physical's name, and its names must derive the binding's own logical id
    for (_, id, _, event) in &events {
        if let RegisterEvent::Bind { physical } = event {
            let found = stages.iter().any(|(db, name)| {
                name == physical
                    && kimmy_core::swap::logical_of_stage(name)
                        .is_some_and(|logical| CollectionId::derive(db, logical) == *id)
            });
            if !found {
                return Err(damaged(&format!("a binding without its stage row: {physical}")));
            }
        }
    }

    // every hidden collection alive in the backup is known to the register; the names of the swapped names are collected on the way
    let mut names: std::collections::BTreeSet<(String, String)> = std::collections::BTreeSet::new();
    for (db, physical) in &stages {
        if let Some(logical) = kimmy_core::swap::logical_of_stage(physical) {
            names.insert((db.clone(), logical.to_string()));
        }
    }
    {
        let collections = txn.open_table(tables::COLLECTIONS)?;
        for row in collections.iter()? {
            let (key, _) = row?;
            let (db, name) = key.value();
            if !kimmy_core::is_hidden_name(name) {
                continue;
            }
            let known = if kimmy_core::swap::is_stage_name(name) {
                stages.iter().any(|(d, p)| d == db && p == name)
            } else {
                // a recreation is known by its `Create` event, or by a stamp above the carried vector:
                // made after it, its event arrives by a window (a backup taken mid-way, a join).
                // One the vector covers has its event, or it is damaged.
                let has_its_event = events.iter().any(|(_, _, _, event)| {
                    matches!(event, RegisterEvent::Create { physical } if physical == name)
                });
                let made_after_the_vector = kimmy_core::recreation_stamp(name)
                    .is_some_and(|stamp| stamp.hlc > vector.get(stamp.origin()));
                has_its_event || made_after_the_vector
            };
            if !known {
                return Err(damaged(&format!(
                    "a hidden collection unknown to the register: {db}.{name}"
                )));
            }
            if let Some(logical) = kimmy_core::logical_of_recreation(name) {
                names.insert((db.to_string(), logical.to_string()));
            }
        }
    }

    // against the vector the backup carries, then against its oplog
    {
        let oplog = txn.open_table(tables::OPLOG)?;
        for (parked, _, stamp, _) in &events {
            let covered = stamp.hlc <= vector.get(stamp.origin());
            if !*parked && !covered {
                return Err(damaged(&format!(
                    "a REGISTER row above the carried vector: {stamp:?}"
                )));
            }
            if *parked {
                if covered {
                    return Err(damaged(&format!(
                        "a REGISTER_INFO row at or below the carried vector, which nothing would release: {stamp:?}"
                    )));
                }
                if oplog.get(crate::codec::oplog_key(stamp).as_slice())?.is_none() {
                    return Err(damaged(&format!(
                        "a REGISTER_INFO row has no entry in the carried oplog: {stamp:?}"
                    )));
                }
            }
        }
    }

    // check 4: what a flip leaves cannot be below its own floor, and a post-drop row does not stay beside a visible physical
    let post = raw_rows(txn, Carried::Post)?;
    for (db, logical) in &names {
        let timeline = timeline_of(txn, tables::REGISTER, db, logical)?;
        let visible = {
            let collections = txn.open_table(tables::COLLECTIONS)?;
            let mut ready: std::collections::HashMap<String, Ready> = Default::default();
            for (_, event) in timeline.events() {
                if let Some(physical) = event.physical() {
                    let alive = collections.get((db.as_str(), physical))?.is_some();
                    ready.insert(
                        physical.to_string(),
                        if alive { Ready::Alive } else { Ready::Buried },
                    );
                }
            }
            timeline.visible(|physical| ready.get(physical).copied().unwrap_or(Ready::Buried))
        };
        // The name's visibility row is derived and not carried, so it is written here, from what has just been computed: a restored
        // member answers a swapped name from the row, as the member it came from did (ADR-272; Q6.6, the resolve row)
        let newest_drop = timeline
            .events()
            .iter()
            .filter(|(_, event)| event.physical().is_none())
            .map(|(stamp, _)| *stamp)
            .max();
        let newest_event = newest_stored_event(txn, db, logical)?;
        let row = VisibleRow::decided(&visible, newest_drop, newest_event);
        write_visible_row(txn, db, logical, &row)?;
        let physical = match &visible {
            Visible::Bound { physical, .. } | Visible::Created { physical, .. } => physical,
            Visible::Identity | Visible::Dropped { .. } => continue,
        };
        let meta = {
            let collections = txn.open_table(tables::COLLECTIONS)?;
            let raw = collections.get((db.as_str(), physical.as_str()))?;
            match raw {
                Some(raw) => {
                    Some(serde_json::from_slice::<crate::meta::CollectionMeta>(raw.value())?)
                }
                None => None,
            }
        };
        if let Some(meta) = meta
            && let Some(floor) = meta.definition_floor
        {
            if meta.indexes.iter().any(|i| i.created.is_none_or(|c| c.hlc <= floor)) {
                return Err(damaged(&format!(
                    "a visible physical holds a definition at or below its own floor: {db}.{physical}"
                )));
            }
            if meta.validator.is_some() && meta.validator_changed.is_none_or(|c| c.hlc <= floor) {
                return Err(damaged(&format!(
                    "a visible physical holds a validator changed at or below its own floor: {db}.{physical}"
                )));
            }
        }
        if post.iter().any(|(key, _)| key.as_slice() == post_key(db, logical).as_slice()) {
            return Err(damaged(&format!(
                "a post-drop row left while a newer physical is visible: {db}.{logical}"
            )));
        }
    }
    Ok(())
}
