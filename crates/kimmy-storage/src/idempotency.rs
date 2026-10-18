//! Idempotency records (B5): a keyed write remembered in its own transaction.
//!
//! A client may send `Idempotency-Key` with a document write. The member that
//! applies it keeps a **record** of it in the write's own redb transaction, so
//! the record survives exactly what the write survives, under either
//! durability class. A retry with the same key and the same request gets the
//! first answer back, and nothing is written again.
//!
//! # The engine API
//!
//! Each document write path has a keyed twin, beside the unkeyed one and
//! sharing its body, so no existing call site changes:
//!
//! | Unkeyed | Keyed |
//! |---|---|
//! | [`Engine::insert_stamped`] | [`Engine::insert_keyed`] |
//! | [`Engine::insert_many_stamped`] | [`Engine::insert_many_keyed`] |
//! | [`Engine::replace_if`] | [`Engine::replace_keyed`] |
//! | [`Engine::delete_if`] | [`Engine::delete_keyed`] |
//! | [`Engine::modify_where`] | [`Engine::modify_where_keyed`] |
//! | [`Engine::find_and_modify`] | [`Engine::find_and_modify_keyed`] |
//!
//! Each takes a [`KeyedWrite`] (the scope hash and the request's fingerprint,
//! both computed by the caller with `kimmy_core::idempotency`) and answers
//! [`Keyed::Fresh`] with what the unkeyed method answers, or
//! [`Keyed::Replayed`] with the record of the first attempt, from which the
//! caller renders the first answer. It refuses with
//! [`StorageError::IdempotencyKeyReused`] when the key's record is of another
//! request (another fingerprint), and with
//! [`StorageError::IdempotencyKeyInFlight`] while a multi-chunk request with the
//! key runs in this process. Neither writes anything.
//!
//! Inside the write transaction each path already opens, under the writer:
//!
//! 1. **Look the record up**, with the life rule below. The same fingerprint
//!    replays; another is refused.
//! 2. **Write as today.**
//! 3. **If it wrote**, put the record and its [`IDEMPOTENCY_BY_STAMP`] row at
//!    the record's `last`, which for a local commit is the stamp of the first
//!    entry the transaction wrote.
//! 4. **Commit.** Under `durable` the commit fsyncs before the writer is let
//!    go, so a later lookup can only find a durable record.
//! 5. **Publish.**
//!
//! A request that changed nothing (refused, nothing matched, unchanged under
//! ADR-208) writes no record.
//!
//! **A replay under `coalesced`** waits for the flush that makes the record
//! durable before it answers: it takes a coalescing ticket while it still holds
//! the writer, as a committer does, and a failed flush answers
//! [`StorageError::OutcomeUnknown`], as the first attempt's does
//! ([`crate::engine::WriteTxn::abort_for_replay`]).
//!
//! **A keyed insert without `_id`** is given the derived `_id` of its position
//! ([`KeyedWrite::derived_id`]), stored as a string; a client's `_id` is used
//! as it is. A `find_and_modify` upsert's seed is position 0.
//!
//! # The life rule
//!
//! A record is judged by its `last`. It is absent on lookup when it is
//! **history**: [`Engine::is_history`] says the collection's drop tombstone or
//! incarnation floor covers `last`, the rule documents are judged by. And it is
//! absent once it is **older than the window**, `storage.oplog_retention_secs`
//! measured on `last`'s own wall time ([`Engine::set_idempotency_window`]); the
//! cutoff is the retention pass's own, so a record is never replayed past the
//! window retention removes it at. An absent record is overwritten by the next
//! store, never merged with.
//!
//! **A keyed delete by id** is replayed only while its own tombstone stands:
//! the document's stored row carries a stamp at or above the delete's (the
//! tombstone, or a later version over it). Once the tombstone is collected the
//! record is absent for that request, which then runs afresh.
//!
//! # Multi-chunk requests
//!
//! A keyed update or delete by filter commits in chunks (ADR-086). Its first
//! chunk writes the record `applying`, each later one updates it in its own
//! transaction, and the chunk that ends the request writes `done`. When the
//! end is found by a chunk that writes nothing, and the record exists, one
//! record-only commit marks it `done`. While it runs, its `(collection, scope)`
//! is in the in-flight set ([`InFlightGuard`]), and a duplicate here is
//! refused. A retry of an `applying` record that is not running here
//! **resumes**: after the record's cursor only when the record's origin is this
//! member, else from the beginning, and writing a matching document only when
//! its stored stamp is below the record's merged `first`; the matches it
//! leaves are counted in `skipped`. Every chunk after a record exists applies
//! that rule, a fresh attempt's later chunks included: another attempt of the
//! request may have merged its record and its writes in between.
//!
//! # For replication (phase R)
//!
//! [`records_at_stamps`], [`records_for_window`] and
//! [`records_for_collection_page`] read what a sender ships;
//! [`Engine::store_record`] merges what a receiver is sent, in the caller's
//! transaction, writing the rows at the merged `last` and at the carrying
//! stamp, and witnessing the record's stamps. [`Engine::record_is_history`] is
//! the life rule for a record a peer sent.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use bson::{Bson, Document};
use kimmy_core::{CollectionId, Fingerprint, Hlc, NodeId, ScopeHash, Stamp};
use redb::{ReadTransaction, ReadableTable};
use tracing::{debug, info, warn};

use crate::codec;
use crate::engine::{Engine, WriteTxn, WriterHolder};
use crate::error::{Result, StorageError};
use crate::meta::CollectionMeta;
use crate::sync::Memo;
use crate::tables::{self, IDEMPOTENCY, IDEMPOTENCY_BY_STAMP};
use crate::walk::{WalkScope, open_walk_table};

/// Records removed per write transaction of a retention pass, and per chunk of
/// a late drop's sweep of them: the bound the oplog step and the drop purge
/// hold the writer to (ADR-151).
pub const RECORD_COLLECT_CHUNK: usize = 1_000;

/// Records on one snapshot page's records phase.
pub const RECORD_PAGE: usize = 512;

/// The window a bare engine remembers a key for: `storage.oplog_retention_secs`'
/// default. The daemon sets its own ([`Engine::set_idempotency_window`]).
pub const DEFAULT_WINDOW_SECS: u64 = 24 * 60 * 60;

/// The record codec's version byte. A record that leads with a higher one was
/// written by a newer build: it is skipped, never misread.
pub const RECORD_FORMAT: u8 = 1;

/// What a keyed write carries: where its record lives and what request it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyedWrite {
    /// The principal's and the key's hash ([`kimmy_core::IdempotencyKey::scope_hash`]).
    pub scope: ScopeHash,
    /// The request's fingerprint, computed by the API.
    pub fingerprint: Fingerprint,
}

impl KeyedWrite {
    pub fn new(scope: ScopeHash, fingerprint: Fingerprint) -> Self {
        Self { scope, fingerprint }
    }

    /// The `_id` a document without one is given at `position` (its index in
    /// a bulk, else 0): the same on every member and every attempt of the
    /// request, and another for another request under the same key.
    pub fn derived_id(&self, collection: CollectionId, position: u64) -> String {
        kimmy_core::idempotency::derived_id(collection, &self.scope, &self.fingerprint, position)
    }
}

/// What a keyed write answers: the write it made, or the record of the attempt
/// that made it first.
// The record is moved once, to the caller that renders it; boxing it would
// cost every caller a deref for nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum Keyed<T> {
    Fresh(T),
    Replayed(IdempotencyRecord),
}

impl<T> Keyed<T> {
    /// The fresh answer, if it is one.
    pub fn fresh(self) -> Option<T> {
        match self {
            Self::Fresh(value) => Some(value),
            Self::Replayed(_) => None,
        }
    }

    /// The first attempt's record, if this is a replay.
    pub fn replayed(&self) -> Option<&IdempotencyRecord> {
        match self {
            Self::Fresh(_) => None,
            Self::Replayed(record) => Some(record),
        }
    }
}

/// Which image a `find_and_modify` answers with, so its record keeps that one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReturnedImage {
    Before,
    After,
}

/// Whether the request a record describes has finished. Ordered so that at an
/// equal `last`, `Done` is the greater, which is how records merge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecordState {
    /// A multi-chunk request that has not finished.
    Applying,
    Done,
}

/// A filtered write's answer, cumulative over every attempt of the request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModifyRecorded {
    pub matched: u64,
    pub modified: u64,
    /// Matches a resumed attempt (or a later chunk) left because they changed
    /// after the request's first write.
    pub skipped: u64,
    /// Chunks that wrote documents.
    pub commits: u64,
    /// The stamp of the last document written.
    pub stamp: Option<Stamp>,
    /// The last document key a chunk reached, a stored `DOCS` key: where this
    /// member resumes the scan, and never another member.
    pub cursor: Option<Vec<u8>>,
    /// Whether an attempt resumed.
    pub resumed: bool,
}

/// What the first attempt answered, enough to render it again through the
/// function that rendered it. Never the response bytes, which would tie a
/// record to one build's rendering.
#[derive(Clone, Debug, PartialEq)]
pub enum RecordOutcome {
    /// An insert: its stamp. The `_id` is the body's, or derived again.
    Insert { stamp: Stamp },
    /// A bulk insert: each document's stamp, in order. No ids: the body is the
    /// same on a replay (the fingerprint says so), and a derived id is derived
    /// again, so a large-id bulk's record stays small.
    Bulk { stamps: Vec<Stamp> },
    /// A replace by id.
    Replace { matched: bool, modified: bool, upserted: bool, stamp: Option<Stamp> },
    /// A delete by id.
    Delete { deleted: u64, stamp: Option<Stamp> },
    /// An update or delete by filter.
    Modify(ModifyRecorded),
    /// A `find_and_modify`: its flags, the upserted `_id` as the stored body
    /// holds it (a rendered BSON value, never a `DocId`), and the document it
    /// answered with.
    FindAndModify {
        matched: bool,
        modified: bool,
        /// The matched document was removed.
        removed: bool,
        upserted_id: Option<Bson>,
        stamp: Option<Stamp>,
        document: Option<Document>,
    },
}

/// Which kind of request a record is of; a record is replayed only to its own
/// kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutcomeKind {
    Insert,
    Bulk,
    Replace,
    Delete,
    Modify,
    FindAndModify,
}

impl RecordOutcome {
    pub(crate) fn kind(&self) -> OutcomeKind {
        match self {
            Self::Insert { .. } => OutcomeKind::Insert,
            Self::Bulk { .. } => OutcomeKind::Bulk,
            Self::Replace { .. } => OutcomeKind::Replace,
            Self::Delete { .. } => OutcomeKind::Delete,
            Self::Modify(_) => OutcomeKind::Modify,
            Self::FindAndModify { .. } => OutcomeKind::FindAndModify,
        }
    }
}

/// The record of a keyed write.
#[derive(Clone, Debug, PartialEq)]
pub struct IdempotencyRecord {
    pub fingerprint: Fingerprint,
    /// The stamp of the first entry the request wrote; after a merge, the
    /// smallest of its attempts' `first`.
    pub first: Stamp,
    /// The carrying stamp of the latest commit that changed the record.
    pub last: Stamp,
    /// The member that wrote it.
    pub origin: NodeId,
    pub state: RecordState,
    pub outcome: RecordOutcome,
}

// -- The codec ------------------------------------------------------------------------------
//
//   version              u8 (RECORD_FORMAT)
//   fingerprint          32
//   first                26 (an oplog key)
//   last                 26
//   origin               16
//   state                u8: 0 applying, 1 done
//   outcome              tag u8, then its fields:
//     1 insert           stamp
//     2 bulk             u32 count, stamps
//     3 replace          flags u8 (matched 1, modified 2, upserted 4), opt stamp
//     4 delete           u64 deleted, opt stamp
//     5 modify           u64 matched, modified, skipped, commits; flags u8 (resumed 1);
//                        opt stamp; opt bytes cursor
//     6 find-and-modify  flags u8 (matched 1, modified 2, removed 4); opt stamp;
//                        opt bytes upserted id ({"v": value}); opt bytes document
//
// An optional stamp is a presence byte (0, 1) and the stamp; optional bytes are a
// u32 length, `u32::MAX` for none. Nothing may follow the last field.

const STATE_APPLYING: u8 = 0;
const STATE_DONE: u8 = 1;
const TAG_INSERT: u8 = 1;
const TAG_BULK: u8 = 2;
const TAG_REPLACE: u8 = 3;
const TAG_DELETE: u8 = 4;
const TAG_MODIFY: u8 = 5;
const TAG_FIND_AND_MODIFY: u8 = 6;
const NONE_LEN: u32 = u32::MAX;

fn corrupt(what: &str) -> StorageError {
    StorageError::Corrupt(format!("idempotency record: {what}"))
}

fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if input.len() < n {
        return Err(corrupt("ended early"));
    }
    let (head, tail) = input.split_at(n);
    *input = tail;
    Ok(head)
}

fn take_u8(input: &mut &[u8]) -> Result<u8> {
    Ok(take(input, 1)?[0])
}

fn take_u32(input: &mut &[u8]) -> Result<u32> {
    Ok(u32::from_be_bytes(take(input, 4)?.try_into().expect("4 bytes")))
}

fn take_u64(input: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(take(input, 8)?.try_into().expect("8 bytes")))
}

fn take_stamp(input: &mut &[u8]) -> Result<Stamp> {
    codec::decode_oplog_key(take(input, codec::STAMP_LEN)?)
}

fn put_opt_stamp(stamp: Option<Stamp>, out: &mut Vec<u8>) {
    match stamp {
        Some(stamp) => {
            out.push(1);
            out.extend_from_slice(&codec::oplog_key(&stamp));
        }
        None => out.push(0),
    }
}

fn take_opt_stamp(input: &mut &[u8]) -> Result<Option<Stamp>> {
    match take_u8(input)? {
        0 => Ok(None),
        1 => Ok(Some(take_stamp(input)?)),
        _ => Err(corrupt("an optional stamp's presence byte")),
    }
}

fn put_opt_bytes(bytes: Option<&[u8]>, out: &mut Vec<u8>) {
    match bytes {
        Some(bytes) => {
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(bytes);
        }
        None => out.extend_from_slice(&NONE_LEN.to_be_bytes()),
    }
}

fn take_opt_bytes<'a>(input: &mut &'a [u8]) -> Result<Option<&'a [u8]>> {
    let len = take_u32(input)?;
    if len == NONE_LEN {
        return Ok(None);
    }
    Ok(Some(take(input, len as usize)?))
}

fn flag(byte: u8, bit: u8) -> bool {
    byte & bit != 0
}

/// A lone BSON value has no document form, so the upserted `_id` is kept as
/// `{"v": value}`.
fn encode_value(value: &Bson) -> Vec<u8> {
    let mut doc = Document::new();
    doc.insert("v", value.clone());
    bson::serialize_to_vec(&doc).expect("a one-field document always encodes")
}

fn decode_value(bytes: &[u8]) -> Result<Bson> {
    let doc: Document = bson::deserialize_from_slice(bytes)?;
    doc.get("v").cloned().ok_or_else(|| corrupt("an upserted id without its value"))
}

impl IdempotencyRecord {
    /// The record's bytes, as stored and as shipped.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(128);
        out.push(RECORD_FORMAT);
        out.extend_from_slice(self.fingerprint.as_bytes());
        out.extend_from_slice(&codec::oplog_key(&self.first));
        out.extend_from_slice(&codec::oplog_key(&self.last));
        out.extend_from_slice(&self.origin.to_bytes());
        out.push(match self.state {
            RecordState::Applying => STATE_APPLYING,
            RecordState::Done => STATE_DONE,
        });
        match &self.outcome {
            RecordOutcome::Insert { stamp } => {
                out.push(TAG_INSERT);
                out.extend_from_slice(&codec::oplog_key(stamp));
            }
            RecordOutcome::Bulk { stamps } => {
                out.push(TAG_BULK);
                out.extend_from_slice(&(stamps.len() as u32).to_be_bytes());
                for stamp in stamps {
                    out.extend_from_slice(&codec::oplog_key(stamp));
                }
            }
            RecordOutcome::Replace { matched, modified, upserted, stamp } => {
                out.push(TAG_REPLACE);
                out.push(u8::from(*matched) | u8::from(*modified) << 1 | u8::from(*upserted) << 2);
                put_opt_stamp(*stamp, &mut out);
            }
            RecordOutcome::Delete { deleted, stamp } => {
                out.push(TAG_DELETE);
                out.extend_from_slice(&deleted.to_be_bytes());
                put_opt_stamp(*stamp, &mut out);
            }
            RecordOutcome::Modify(m) => {
                out.push(TAG_MODIFY);
                for count in [m.matched, m.modified, m.skipped, m.commits] {
                    out.extend_from_slice(&count.to_be_bytes());
                }
                out.push(u8::from(m.resumed));
                put_opt_stamp(m.stamp, &mut out);
                put_opt_bytes(m.cursor.as_deref(), &mut out);
            }
            RecordOutcome::FindAndModify {
                matched,
                modified,
                removed,
                upserted_id,
                stamp,
                document,
            } => {
                out.push(TAG_FIND_AND_MODIFY);
                out.push(u8::from(*matched) | u8::from(*modified) << 1 | u8::from(*removed) << 2);
                put_opt_stamp(*stamp, &mut out);
                put_opt_bytes(upserted_id.as_ref().map(encode_value).as_deref(), &mut out);
                let document = document
                    .as_ref()
                    .map(|doc| bson::serialize_to_vec(doc).expect("a stored document encodes"));
                put_opt_bytes(document.as_deref(), &mut out);
            }
        }
        out
    }

    /// Read a record. `Ok(None)` for one a newer build wrote, which leads with
    /// a higher version byte: skipped, as a peer's newer record is dropped
    /// rather than failing what carries it. An error for one this format
    /// cannot be.
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>> {
        let mut input = bytes;
        let version = take_u8(&mut input)?;
        if version > RECORD_FORMAT {
            return Ok(None);
        }
        if version != RECORD_FORMAT {
            return Err(corrupt("an unknown version byte"));
        }
        let fingerprint =
            Fingerprint::from_slice(take(&mut input, 32)?).expect("32 bytes were taken");
        let first = take_stamp(&mut input)?;
        let last = take_stamp(&mut input)?;
        let origin = NodeId::from_bytes(take(&mut input, 16)?.try_into().expect("16 bytes"));
        let state = match take_u8(&mut input)? {
            STATE_APPLYING => RecordState::Applying,
            STATE_DONE => RecordState::Done,
            _ => return Err(corrupt("an unknown state")),
        };
        let outcome = match take_u8(&mut input)? {
            TAG_INSERT => RecordOutcome::Insert { stamp: take_stamp(&mut input)? },
            TAG_BULK => {
                let n = take_u32(&mut input)? as usize;
                // Bounded by what is there, never by the count a damaged
                // record claims.
                if input.len() < n.saturating_mul(codec::STAMP_LEN) {
                    return Err(corrupt("a bulk's stamps ended early"));
                }
                let stamps = (0..n).map(|_| take_stamp(&mut input)).collect::<Result<_>>()?;
                RecordOutcome::Bulk { stamps }
            }
            TAG_REPLACE => {
                let flags = take_u8(&mut input)?;
                RecordOutcome::Replace {
                    matched: flag(flags, 1),
                    modified: flag(flags, 2),
                    upserted: flag(flags, 4),
                    stamp: take_opt_stamp(&mut input)?,
                }
            }
            TAG_DELETE => RecordOutcome::Delete {
                deleted: take_u64(&mut input)?,
                stamp: take_opt_stamp(&mut input)?,
            },
            TAG_MODIFY => {
                let matched = take_u64(&mut input)?;
                let modified = take_u64(&mut input)?;
                let skipped = take_u64(&mut input)?;
                let commits = take_u64(&mut input)?;
                let resumed = flag(take_u8(&mut input)?, 1);
                let stamp = take_opt_stamp(&mut input)?;
                let cursor = take_opt_bytes(&mut input)?.map(<[u8]>::to_vec);
                RecordOutcome::Modify(ModifyRecorded {
                    matched,
                    modified,
                    skipped,
                    commits,
                    stamp,
                    cursor,
                    resumed,
                })
            }
            TAG_FIND_AND_MODIFY => {
                let flags = take_u8(&mut input)?;
                let stamp = take_opt_stamp(&mut input)?;
                let upserted_id = take_opt_bytes(&mut input)?.map(decode_value).transpose()?;
                let document = take_opt_bytes(&mut input)?
                    .map(bson::deserialize_from_slice::<Document>)
                    .transpose()?;
                RecordOutcome::FindAndModify {
                    matched: flag(flags, 1),
                    modified: flag(flags, 2),
                    removed: flag(flags, 4),
                    upserted_id,
                    stamp,
                    document,
                }
            }
            _ => return Err(corrupt("an unknown outcome")),
        };
        if !input.is_empty() {
            return Err(corrupt("bytes after its last field"));
        }
        Ok(Some(Self { fingerprint, first, last, origin, state, outcome }))
    }

    /// The record of a write committed here in one transaction: `first` and
    /// `last` are both the stamp of the first entry it wrote, and it is done.
    pub(crate) fn written(
        keyed: &KeyedWrite,
        first: Stamp,
        origin: NodeId,
        outcome: RecordOutcome,
    ) -> Self {
        Self {
            fingerprint: keyed.fingerprint,
            first,
            last: first,
            origin,
            state: RecordState::Done,
            outcome,
        }
    }

    /// Everything but `first`, the bytes the merge breaks a tie by.
    fn rest(&self) -> Vec<u8> {
        let mut bytes = self.encode();
        // `first` is the 26 bytes after the version and the fingerprint.
        bytes.drain(1 + 32..1 + 32 + codec::STAMP_LEN);
        bytes
    }

    /// Two records of one key, merged as a join, so the result does not depend
    /// on the order they arrive in (commutative, associative, idempotent):
    /// `first` is the smaller, `last` the greater, and state, outcome, cursor,
    /// origin and fingerprint come from the record with the greater `last`; at
    /// an equal `last`, `done` beats `applying`, and two of the same state are
    /// told apart by their bytes, so that every member picks the same one.
    ///
    /// `first` is the smaller and not the winner's: each attempt of a request
    /// writes at or above its own `first`, so a resume that writes only below
    /// the merged one never writes what any attempt wrote.
    pub fn merge(&self, other: &Self) -> Self {
        let first = self.first.min(other.first);
        let ours = (self.last, self.state);
        let theirs = (other.last, other.state);
        let winner = match ours.cmp(&theirs) {
            std::cmp::Ordering::Greater => self,
            std::cmp::Ordering::Less => other,
            std::cmp::Ordering::Equal if self.rest() >= other.rest() => self,
            std::cmp::Ordering::Equal => other,
        };
        Self { first, ..winner.clone() }
    }
}

// -- The tables ------------------------------------------------------------------------------

/// An `IDEMPOTENCY_BY_STAMP` row's value: `collection id ‖ scope hash`.
fn row_value(collection: CollectionId, scope: &ScopeHash) -> [u8; 40] {
    let mut out = [0u8; 40];
    out[..8].copy_from_slice(&collection.0.to_be_bytes());
    out[8..].copy_from_slice(scope.as_bytes());
    out
}

fn parse_row_value(value: &[u8]) -> Option<(CollectionId, ScopeHash)> {
    let (id, scope) = value.split_at_checked(8)?;
    Some((CollectionId(u64::from_be_bytes(id.try_into().ok()?)), ScopeHash::from_slice(scope)?))
}

/// A record as stored, if there is one: `Ok(None)` for none, or for one a newer
/// build wrote.
fn read_record(
    table: &impl ReadableTable<(u64, &'static [u8]), &'static [u8]>,
    collection: CollectionId,
    scope: &ScopeHash,
) -> Result<Option<IdempotencyRecord>> {
    match table.get((collection.0, scope.as_bytes().as_slice()))? {
        Some(raw) => IdempotencyRecord::decode(raw.value()),
        None => Ok(None),
    }
}

/// Write `record` under its key, and its row at its `last`, in `txn`.
fn put_rows(
    txn: &redb::WriteTransaction,
    collection: CollectionId,
    scope: &ScopeHash,
    record: &IdempotencyRecord,
    carrying: Option<Stamp>,
) -> Result<()> {
    txn.open_table(IDEMPOTENCY)?
        .insert((collection.0, scope.as_bytes().as_slice()), record.encode().as_slice())?;
    let mut rows = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
    let value = row_value(collection, scope);
    rows.insert(codec::oplog_key(&record.last).as_slice(), value.as_slice())?;
    if let Some(carrying) = carrying {
        rows.insert(codec::oplog_key(&carrying).as_slice(), value.as_slice())?;
    }
    Ok(())
}

/// A record on the wire: its key, the stamp of the entry that carries it, and
/// its bytes as stored (B5, "How a record travels").
///
/// The `records` of a frame (`Entries`, `ReplayEntries`, `Push`) and of a
/// snapshot page are lists of these. The record's bytes are opaque to the
/// transport, which passes them through: only [`IdempotencyRecord::decode`]
/// reads them, on the member that stores them. They carry no document key a
/// peer reads (a multi request's cursor inside is a stored key read back only on
/// its origin), so the wire's legacy-key rule (Decimal128 design 7.1) has
/// nothing to convert here. `carrying` is absent on a snapshot page, whose
/// records travel ahead of the collection's documents and not with an entry.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CarriedRecord {
    /// See `SnapshotCursor::collection` for why this is not a `u64`.
    pub collection: CollectionId,
    #[serde(with = "scope_bytes")]
    pub scope: ScopeHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carrying: Option<Stamp>,
    #[serde(with = "serde_bytes")]
    pub record: Vec<u8>,
}

/// A scope hash on the wire: its 32 bytes as a BSON binary. Any other length
/// fails the one record, never the frame (the frame's list is read leniently).
mod scope_bytes {
    use kimmy_core::ScopeHash;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(scope: &ScopeHash, serializer: S) -> Result<S::Ok, S::Error> {
        serde_bytes::serialize(scope.as_bytes().as_slice(), serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ScopeHash, D::Error> {
        let bytes: serde_bytes::ByteBuf = Deserialize::deserialize(deserializer)?;
        ScopeHash::from_slice(&bytes)
            .ok_or_else(|| serde::de::Error::custom("a scope hash is 32 bytes"))
    }
}

/// A frame's or a page's `records`, read leniently: a list that does not decode
/// is none, and an element that does not decode is dropped, so a field of
/// another shape costs the records and never the frame. A record whose own bytes
/// do not decode is dropped later, by the member storing it, and counted
/// (`kimmy_idempotency_records_undecodable_total`).
pub fn lenient_records<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<CarriedRecord>, D::Error> {
    use serde::Deserialize;
    let raw = Option::<bson::Bson>::deserialize(deserializer)?;
    let Some(bson::Bson::Array(items)) = raw else {
        return Ok(Vec::new());
    };
    Ok(items.into_iter().filter_map(|item| bson::deserialize_from_bson(item).ok()).collect())
}

/// The records that ride with the entries stamped `stamps`, read in the read
/// transaction that read them: one point read of [`IDEMPOTENCY_BY_STAMP`] per
/// stamp, for a window that came from the arrival-index walk. A row that names
/// no record (its record was purged or collected) carries nothing.
pub fn records_at_stamps(txn: &ReadTransaction, stamps: &[Stamp]) -> Result<Vec<CarriedRecord>> {
    let rows = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
    let records = txn.open_table(IDEMPOTENCY)?;
    let mut out = Vec::new();
    for stamp in stamps {
        let Some(value) = rows.get(codec::oplog_key(stamp).as_slice())? else { continue };
        let Some((collection, scope)) = parse_row_value(value.value()) else { continue };
        if let Some(record) = records.get((collection.0, scope.as_bytes().as_slice()))? {
            out.push(CarriedRecord {
                collection,
                scope,
                carrying: Some(*stamp),
                record: record.value().to_vec(),
            });
        }
    }
    Ok(out)
}

/// [`records_at_stamps`] for a window read in stamp order: one range read of
/// [`IDEMPOTENCY_BY_STAMP`] from `first` to `last`, both shipped, keeping the
/// rows at stamps `shipped` says the frame carries. Bounded by the window, which
/// the caller has already read.
pub fn records_for_window(
    txn: &ReadTransaction,
    first: Stamp,
    last: Stamp,
    shipped: &dyn Fn(&Stamp) -> bool,
) -> Result<Vec<CarriedRecord>> {
    let rows = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
    let records = txn.open_table(IDEMPOTENCY)?;
    let (from, to) = (codec::oplog_key(&first), codec::oplog_key(&last));
    let mut out = Vec::new();
    for row in rows.range::<&[u8]>(from.as_slice()..=to.as_slice())? {
        let (key, value) = row?;
        let Ok(carrying) = codec::decode_oplog_key(key.value()) else { continue };
        if !shipped(&carrying) {
            continue;
        }
        let Some((collection, scope)) = parse_row_value(value.value()) else { continue };
        if let Some(record) = records.get((collection.0, scope.as_bytes().as_slice()))? {
            out.push(CarriedRecord {
                collection,
                scope,
                carrying: Some(carrying),
                record: record.value().to_vec(),
            });
        }
    }
    Ok(out)
}

/// One page of a collection's records for a snapshot's records phase: up to
/// `limit` (at most [`RECORD_PAGE`]) records after the scope `after`, in scope
/// order, and the scope to go on after, `None` once the collection's records
/// are all sent.
#[allow(clippy::type_complexity)]
pub fn records_for_collection_page(
    txn: &ReadTransaction,
    collection: CollectionId,
    after: Option<&ScopeHash>,
    limit: usize,
) -> Result<(Vec<(ScopeHash, Vec<u8>)>, Option<ScopeHash>)> {
    let limit = limit.clamp(1, RECORD_PAGE);
    let records = txn.open_table(IDEMPOTENCY)?;
    let mut page = Vec::new();
    let mut more = false;
    for row in
        records.range(collection_records(collection, after.map(|s| s.as_bytes().as_slice())))?
    {
        let (key, value) = row?;
        let (_, scope) = key.value();
        if page.len() == limit {
            more = true;
            break;
        }
        let Some(scope) = ScopeHash::from_slice(scope) else { continue };
        page.push((scope, value.value().to_vec()));
    }
    let next = if more { page.last().map(|(scope, _)| *scope) } else { None };
    Ok((page, next))
}

// -- In the engine ---------------------------------------------------------------------------

/// The engine's idempotency state in process.
pub(crate) struct IdempotencyState {
    /// `(collection, scope)` of every keyed multi-chunk request running here.
    in_flight: parking_lot::Mutex<HashSet<(u64, [u8; 32])>>,
    /// The key's memory, `storage.oplog_retention_secs`.
    window_secs: AtomicU64,
    /// Records retention removed, since start.
    collected: AtomicU64,
    /// Records stored from peers, since start, by [`RecordsVia`].
    received: [AtomicU64; RecordsVia::ALL.len()],
    /// Records a peer sent that this build could not decode, since start.
    undecodable: AtomicU64,
    /// The wall clock the life rule reads, set by a test that moves time; 0 is
    /// the real one.
    #[cfg(any(test, feature = "test-hooks"))]
    now_ms: AtomicU64,
}

impl Default for IdempotencyState {
    fn default() -> Self {
        Self {
            in_flight: Default::default(),
            window_secs: AtomicU64::new(DEFAULT_WINDOW_SECS),
            collected: AtomicU64::new(0),
            received: Default::default(),
            undecodable: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            now_ms: AtomicU64::new(0),
        }
    }
}

/// How a record reached this member from a peer: the label of
/// `kimmy_idempotency_records_received_total{via}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordsVia {
    /// A window this member pulled.
    Pull,
    /// A window a peer pushed (ADR-140).
    Push,
    /// The replay of this member's own origin (ADR-212).
    Replay,
    /// A page of a whole-database snapshot.
    Snapshot,
    /// A page of a scoped repair: a count repair's snapshot, or a converge page.
    Repair,
}

impl RecordsVia {
    /// Every way, in the order of [`Engine::idempotency_records_received`] and of
    /// the API's `RECORDS_RECEIVED_VIA`.
    pub const ALL: [Self; 5] = [Self::Pull, Self::Push, Self::Replay, Self::Snapshot, Self::Repair];

    /// The metric label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Pull => "pull",
            Self::Push => "push",
            Self::Replay => "replay",
            Self::Snapshot => "snapshot",
            Self::Repair => "repair",
        }
    }

    const fn slot(self) -> usize {
        self as usize
    }
}

/// A frame's records, decoded once for the batch that applies its entries, and
/// each stored at most once: in the run that takes the first entry of its
/// collection (B5, "On the receiving side").
pub(crate) struct FrameRecords {
    via: RecordsVia,
    /// `(collection, scope, carrying stamp, record, stored yet)`.
    records: Vec<(CollectionId, ScopeHash, Option<Stamp>, IdempotencyRecord, bool)>,
}

impl FrameRecords {
    /// None: an unkeyed frame, or one from a sender that carries no records.
    pub(crate) fn none() -> Self {
        Self { via: RecordsVia::Pull, records: Vec::new() }
    }

    /// Whether any of the frame's records is of collection `id` and not stored
    /// yet.
    pub(crate) fn owes(&self, id: CollectionId) -> bool {
        self.records.iter().any(|(collection, _, _, _, stored)| *collection == id && !stored)
    }
}

impl Engine {
    /// Decode a frame's records, dropping and counting those this build cannot
    /// read: a newer format is skipped, never fatal to what carries it.
    pub(crate) fn frame_records(&self, carried: &[CarriedRecord], via: RecordsVia) -> FrameRecords {
        let mut records = Vec::with_capacity(carried.len());
        for wire in carried {
            match IdempotencyRecord::decode(&wire.record) {
                Ok(Some(record)) => {
                    records.push((wire.collection, wire.scope, wire.carrying, record, false))
                }
                Ok(None) | Err(_) => {
                    self.idempotency.undecodable.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        FrameRecords { via, records }
    }

    /// Store, in the run's transaction, every record of the frame for the
    /// collection `id` that is not stored yet: called when the run takes (applies
    /// or supersedes) an entry of that collection, on both outcomes of the
    /// merge. Each through [`Self::store_record`]: refused if history by the life
    /// rule, merged with the live record, rows at the merged `last` and at the
    /// carrying stamp, its stamps witnessed. Marked stored either way, so a later
    /// entry of the collection asks nothing again. Returns how many were stored.
    pub(crate) fn store_frame_records(
        &self,
        txn: &WriteTxn<'_>,
        memo: &mut Memo,
        standing: Option<&CollectionMeta>,
        id: CollectionId,
        frame: &mut FrameRecords,
    ) -> Result<usize> {
        let mut stored = 0usize;
        for (collection, scope, carrying, record, done) in &mut frame.records {
            if *collection != id || *done {
                continue;
            }
            if self.store_record(txn, memo, standing, id, scope, record, *carrying)?.is_some() {
                stored += 1;
            }
            *done = true;
        }
        Ok(stored)
    }

    /// Whether any of the frame's records of collection `id` not stored yet is
    /// not history by the life rule: what makes an entry judged history worth a
    /// transaction for the frame's records.
    pub(crate) fn frame_holds_a_live_record(
        &self,
        memo: &mut Memo,
        standing: Option<&CollectionMeta>,
        id: CollectionId,
        frame: &FrameRecords,
    ) -> Result<bool> {
        for (collection, _, _, record, stored) in &frame.records {
            if *collection == id
                && !stored
                && !self.record_is_history(memo, standing, id, record)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Store a snapshot or converge page's records in the page's transaction
    /// (B5, "Snapshots carry records"): each of a collection that stands here,
    /// through [`Self::store_record`] with no carrying stamp (its row at its own
    /// `last`), refused if history by the life rule. A record of a collection
    /// absent here is not stored, as the page's documents of it are not. Those
    /// this build cannot decode are dropped and counted. Returns how many it
    /// stored, to count once the page's transaction commits.
    pub(crate) fn store_page_records(
        &self,
        txn: &WriteTxn<'_>,
        memo: &mut Memo,
        records: &[CarriedRecord],
    ) -> Result<usize> {
        let mut stored = 0usize;
        for wire in records {
            let record = match IdempotencyRecord::decode(&wire.record) {
                Ok(Some(record)) => record,
                Ok(None) | Err(_) => {
                    self.idempotency.undecodable.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            let Some(standing) = self.memo_collection(memo, wire.collection)? else {
                continue;
            };
            if self
                .store_record(
                    txn,
                    memo,
                    Some(&standing),
                    wire.collection,
                    &wire.scope,
                    &record,
                    None,
                )?
                .is_some()
            {
                stored += 1;
            }
        }
        Ok(stored)
    }

    /// Count `n` records stored from peers by `via`, once the transaction that
    /// stored them has committed.
    pub(crate) fn count_records_received(&self, via: RecordsVia, n: usize) {
        if n > 0 {
            self.idempotency.received[via.slot()].fetch_add(n as u64, Ordering::Relaxed);
        }
    }

    /// The frame's way.
    pub(crate) fn frame_via(frame: &FrameRecords) -> RecordsVia {
        frame.via
    }

    /// Records stored from peers since start, by [`RecordsVia::ALL`]: pull,
    /// push, replay, snapshot, repair (`kimmy_idempotency_records_received_total`).
    pub fn idempotency_records_received(&self) -> [u64; RecordsVia::ALL.len()] {
        std::array::from_fn(|slot| self.idempotency.received[slot].load(Ordering::Relaxed))
    }

    /// Records a peer sent that this build could not decode, and dropped
    /// (`kimmy_idempotency_records_undecodable_total`).
    pub fn idempotency_records_undecodable(&self) -> u64 {
        self.idempotency.undecodable.load(Ordering::Relaxed)
    }
}

/// A keyed multi-chunk request's place in the in-flight set: removed when this
/// drops, on every way out, a panic or an early return included. Left behind, it
/// would answer `in flight` to the key until a restart.
pub struct InFlightGuard<'e> {
    engine: &'e Engine,
    key: (u64, [u8; 32]),
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.engine.idempotency.in_flight.lock().remove(&self.key);
    }
}

/// Whether a lookup is made by the request that holds the key's in-flight
/// place, which must not refuse itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Owner {
    Yes,
    No,
}

/// What a lookup under the writer found of the key's record.
#[derive(Debug)]
pub(crate) enum Found {
    /// None, or none that is alive: the request is new.
    Absent,
    /// A finished request with this fingerprint: replay it.
    Done(IdempotencyRecord),
    /// A multi-chunk request with this fingerprint that is not running here:
    /// resume it.
    Applying(IdempotencyRecord),
}

impl Engine {
    /// Remember keys for `secs` after their record's `last`: the daemon sets
    /// `storage.oplog_retention_secs`, the window its retention pass collects
    /// records on. A bare engine remembers for [`DEFAULT_WINDOW_SECS`].
    pub fn set_idempotency_window(&self, secs: u64) {
        self.idempotency.window_secs.store(secs, Ordering::Relaxed);
    }

    /// How long a key is remembered, in seconds.
    pub fn idempotency_window_secs(&self) -> u64 {
        self.idempotency.window_secs.load(Ordering::Relaxed)
    }

    /// Records retention has removed since start
    /// (`kimmy_idempotency_records_collected_total`).
    pub fn idempotency_records_collected(&self) -> u64 {
        self.idempotency.collected.load(Ordering::Relaxed)
    }

    /// Records held (`kimmy_idempotency_records`): the table's length, which
    /// redb keeps in the tree's root, so O(1).
    pub fn idempotency_records(&self) -> Result<u64> {
        use redb::{ReadableDatabase, ReadableTableMetadata};
        let txn = self.db().begin_read()?;
        Ok(txn.open_table(IDEMPOTENCY)?.len()?)
    }

    /// The record stored under a key, as stored, with no life rule: for tests
    /// and for an operator's look at one key.
    pub fn idempotency_record(
        &self,
        collection: CollectionId,
        scope: &ScopeHash,
    ) -> Result<Option<IdempotencyRecord>> {
        use redb::ReadableDatabase;
        let txn = self.db().begin_read()?;
        read_record(&txn.open_table(IDEMPOTENCY)?, collection, scope)
    }

    /// Set the wall clock the life rule reads, for a test whose clocks run
    /// ahead of it; `None` for the real one.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_idempotency_clock_for_test(&self, now_ms: Option<u64>) {
        self.idempotency.now_ms.store(now_ms.unwrap_or(0), Ordering::SeqCst);
    }

    fn idempotency_now_ms(&self) -> u64 {
        #[cfg(any(test, feature = "test-hooks"))]
        {
            let set = self.idempotency.now_ms.load(Ordering::SeqCst);
            if set != 0 {
                return set;
            }
        }
        crate::engine::physical_now_ms()
    }

    /// Take the key's place in the in-flight set, for a multi-chunk request,
    /// before its first chunk waits for the writer. Refused with
    /// [`StorageError::IdempotencyKeyInFlight`] while another holds it.
    pub(crate) fn enter_in_flight(
        &self,
        collection: CollectionId,
        scope: &ScopeHash,
    ) -> Result<InFlightGuard<'_>> {
        let key = (collection.0, scope.0);
        if !self.idempotency.in_flight.lock().insert(key) {
            return Err(StorageError::IdempotencyKeyInFlight);
        }
        Ok(InFlightGuard { engine: self, key })
    }

    /// Whether a record is past the window: its `last` below the cutoff the
    /// retention pass would collect it at.
    pub(crate) fn record_is_past_window(&self, record: &IdempotencyRecord) -> bool {
        record.last.hlc
            < crate::gc::cutoff(self.idempotency_now_ms(), self.idempotency_window_secs())
    }

    /// The life rule's history half, for a record a peer sent or one read
    /// here: [`Engine::is_history`] on its `last`. A record whose `last` is at
    /// or below the collection's drop tombstone or its incarnation floor
    /// belongs to an earlier life.
    ///
    /// By `last` and not `first`: a drop that falls between two chunks of one
    /// request keeps the later chunks' documents (ADR-231, ADR-236), and by
    /// `first` their record would be ignored, so a retry would write them a
    /// second time.
    pub(crate) fn record_is_history(
        &self,
        memo: &mut Memo,
        standing: Option<&CollectionMeta>,
        collection: CollectionId,
        record: &IdempotencyRecord,
    ) -> Result<bool> {
        self.is_history(memo, standing, collection, record.last)
    }

    /// The key's record if it is alive: stored, readable by this build, not
    /// history and not past the window. `standing` is the collection as the
    /// transaction sees it. A record a newer build wrote reads as none.
    fn live_record_in(
        &self,
        txn: &redb::WriteTransaction,
        memo: &mut Memo,
        standing: Option<&CollectionMeta>,
        collection: CollectionId,
        scope: &ScopeHash,
    ) -> Result<Option<IdempotencyRecord>> {
        let Some(record) = read_record(&txn.open_table(IDEMPOTENCY)?, collection, scope)? else {
            return Ok(None);
        };
        if self.record_is_history(memo, standing, collection, &record)?
            || self.record_is_past_window(&record)
        {
            return Ok(None);
        }
        Ok(Some(record))
    }

    /// The collection under `coll`'s name as `txn` sees it: the life rule reads
    /// the floor that stands under the writer, not the one the caller read.
    fn standing_in(
        txn: &redb::WriteTransaction,
        coll: &CollectionMeta,
    ) -> Result<Option<CollectionMeta>> {
        let collections = txn.open_table(tables::COLLECTIONS)?;
        collections
            .get((coll.db.as_str(), coll.name.as_str()))?
            .map(|raw| serde_json::from_slice(raw.value()).map_err(Into::into))
            .transpose()
    }

    /// The lookup a keyed write makes first, under the writer, in its own
    /// transaction: the authoritative one, so two duplicates on one member
    /// serialise here.
    ///
    /// Refuses a key whose multi-chunk request runs here (unless `owner` is
    /// that request), and a key whose live record has another fingerprint.
    /// A record it uses has its stamps witnessed: a resume must write above
    /// its `first`, and a new record must merge above its `last`.
    pub(crate) fn keyed_lookup(
        &self,
        txn: &WriteTxn<'_>,
        coll: &CollectionMeta,
        keyed: &KeyedWrite,
        owner: Owner,
    ) -> Result<Found> {
        if owner == Owner::No
            && self.idempotency.in_flight.lock().contains(&(coll.id.0, keyed.scope.0))
        {
            return Err(StorageError::IdempotencyKeyInFlight);
        }
        let standing = Self::standing_in(txn, coll)?;
        let Some(record) = self.live_record_in(
            txn,
            &mut Memo::default(),
            standing.as_ref(),
            coll.id,
            &keyed.scope,
        )?
        else {
            return Ok(Found::Absent);
        };
        self.witness(&record.first);
        self.witness(&record.last);
        if record.fingerprint != keyed.fingerprint {
            return Err(StorageError::IdempotencyKeyReused);
        }
        Ok(match record.state {
            RecordState::Done => Found::Done(record),
            RecordState::Applying => Found::Applying(record),
        })
    }

    /// The key's live record, read by a later chunk of the request that owns
    /// it, under that chunk's writer: another attempt's record may have merged
    /// in since the chunk before. Witnessed, as every record used is.
    pub(crate) fn keyed_live_record(
        &self,
        txn: &WriteTxn<'_>,
        coll: &CollectionMeta,
        keyed: &KeyedWrite,
    ) -> Result<Option<IdempotencyRecord>> {
        let standing = Self::standing_in(txn, coll)?;
        let record = self.live_record_in(
            txn,
            &mut Memo::default(),
            standing.as_ref(),
            coll.id,
            &keyed.scope,
        )?;
        if let Some(record) = &record {
            self.witness(&record.first);
            self.witness(&record.last);
        }
        Ok(record)
    }

    /// Answer a keyed write from the record it found: abort its transaction
    /// behind the barrier ([`WriteTxn::abort_for_replay`]) and hand the record
    /// back. A record of another kind of request is a key reused, whatever its
    /// fingerprint says.
    pub(crate) fn replay<T>(
        &self,
        txn: WriteTxn<'_>,
        coll: &CollectionMeta,
        record: IdempotencyRecord,
        kind: OutcomeKind,
    ) -> Result<Keyed<T>> {
        if record.outcome.kind() != kind {
            txn.abort()?;
            return Err(StorageError::IdempotencyKeyReused);
        }
        txn.abort_for_replay()?;
        debug!(
            db = %coll.db,
            collection = %coll.name,
            origin = %record.origin,
            age_ms = crate::engine::physical_now_ms().saturating_sub(record.last.hlc.wall_ms),
            "replayed a keyed write"
        );
        Ok(Keyed::Replayed(record))
    }

    /// Put a local commit's record, merged into the live one if there is one
    /// (a resumed or continuing request's), with its row at the merged `last`.
    /// What stands under the key and is not alive is overwritten, never merged
    /// with: an earlier life's record, or one past the window, belongs to
    /// another request.
    pub(crate) fn put_record(
        &self,
        txn: &WriteTxn<'_>,
        collection: CollectionId,
        scope: &ScopeHash,
        live: Option<&IdempotencyRecord>,
        record: &IdempotencyRecord,
    ) -> Result<IdempotencyRecord> {
        let merged = match live {
            Some(live) => live.merge(record),
            None => record.clone(),
        };
        put_rows(txn, collection, scope, &merged, None)?;
        Ok(merged)
    }

    /// Store a record a peer sent, in the transaction that takes its entries
    /// (or a snapshot or converge page's): merged into the live one, with a
    /// row at the merged `last` and, when the frame named one, at the carrying
    /// stamp, so the record travels onward with that entry. The record's stamps
    /// are witnessed: a resume here must write above its `first`.
    ///
    /// Not stored when it is history by the life rule; what stands under the
    /// key and is not alive (history, past the window, unreadable by this
    /// build) is overwritten. Returns the record as stored, or `None`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn store_record(
        &self,
        txn: &WriteTxn<'_>,
        memo: &mut Memo,
        standing: Option<&CollectionMeta>,
        collection: CollectionId,
        scope: &ScopeHash,
        record: &IdempotencyRecord,
        carrying: Option<Stamp>,
    ) -> Result<Option<IdempotencyRecord>> {
        if self.record_is_history(memo, standing, collection, record)? {
            return Ok(None);
        }
        let live = match self.live_record_in(txn, memo, standing, collection, scope) {
            Ok(live) => live,
            // A local record this format cannot be: the peer's is good data,
            // and refusing it would fail the run for good.
            Err(StorageError::Corrupt(why)) => {
                warn!(%why, "an idempotency record here does not decode; a peer's replaces it");
                None
            }
            Err(e) => return Err(e),
        };
        let merged = match &live {
            Some(live) => live.merge(record),
            None => record.clone(),
        };
        put_rows(txn, collection, scope, &merged, carrying)?;
        self.witness(&merged.first);
        self.witness(&merged.last);
        Ok(Some(merged))
    }

    /// Retention's records step: every [`IDEMPOTENCY_BY_STAMP`] row stamped
    /// below `cutoff`, found under a read transaction and removed
    /// [`RECORD_COLLECT_CHUNK`] at a time, each chunk its own commit with the
    /// writer released between them (ADR-151). Each row is re-checked under
    /// the writer: its record goes only when the record's `last` is at or below
    /// the row's stamp, so a record a later commit changed is kept and only the
    /// row goes. Returns how many records it removed.
    ///
    /// At or below, and not only at: a record whose `last` is below a row's
    /// stamp that is below the cutoff is past its window, and its own row at
    /// `last` is the only other thing that removes it. A record this build
    /// cannot decode has no `last` to age by: it goes with the first of its rows
    /// to expire, which is at least one window after its first commit. It was
    /// absent to every lookup here all along, so that costs only a newer build
    /// rolled forward to, and a rollback already reopens that gap. A stop ends
    /// the read of the next chunk with its error, between two chunks.
    pub(crate) fn collect_idempotency_records(&self, cutoff: Hlc) -> Result<usize> {
        use redb::ReadableDatabase;
        let bound = codec::oplog_key_lower_bound(cutoff);
        let mut collected = 0usize;
        loop {
            let expired: Vec<(Vec<u8>, Vec<u8>)> = {
                let txn = self.db().begin_read()?;
                let rows =
                    open_walk_table(&txn, IDEMPOTENCY_BY_STAMP, self.walk(WalkScope::Background))?;
                let mut expired = Vec::new();
                for row in rows.range::<&[u8]>(..bound.as_slice())? {
                    let (key, value) = row?;
                    expired.push((key.value().to_vec(), value.value().to_vec()));
                    if expired.len() >= RECORD_COLLECT_CHUNK {
                        break;
                    }
                }
                expired
            };
            if expired.is_empty() {
                break;
            }
            let full = expired.len() >= RECORD_COLLECT_CHUNK;
            let removed = self.remove_expired_records(&expired)?;
            // Counted per chunk, once its commit has landed: a stop before the
            // next chunk leaves the count true.
            self.idempotency.collected.fetch_add(removed as u64, Ordering::Relaxed);
            collected += removed;
            if !full {
                break;
            }
        }
        Ok(collected)
    }

    /// One chunk of [`Self::collect_idempotency_records`], under the writer.
    fn remove_expired_records(&self, rows: &[(Vec<u8>, Vec<u8>)]) -> Result<usize> {
        let txn = self.begin_write(WriterHolder::Retention)?;
        let mut removed = 0usize;
        {
            let mut by_stamp = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
            let mut records = txn.open_table(IDEMPOTENCY)?;
            for (key, _) in rows {
                // Read again under the writer: the row is what stands now.
                let Some(value) = by_stamp.remove(key.as_slice())? else { continue };
                let named = parse_row_value(value.value());
                drop(value);
                let (Some((collection, scope)), Ok(stamp)) = (named, codec::decode_oplog_key(key))
                else {
                    continue;
                };
                let record_key = (collection.0, scope.as_bytes().as_slice());
                let stale = match records.get(record_key)? {
                    Some(raw) => match IdempotencyRecord::decode(raw.value()) {
                        Ok(Some(record)) => record.last <= stamp,
                        // A record this build cannot read (a newer format, or
                        // damage) has no `last` it can age by, and is absent to
                        // every lookup and overwritten by every store here. It
                        // goes with the first of its rows to expire: kept past
                        // that, nothing would ever find it again.
                        Ok(None) | Err(_) => true,
                    },
                    None => false,
                };
                if stale {
                    records.remove(record_key)?;
                    removed += 1;
                }
            }
        }
        txn.commit()?;
        Ok(removed)
    }

    /// The drop purger's share: up to `budget` of collection `id`'s records,
    /// removed in `txn`, which has already found no collection standing under
    /// the id. Their rows are left to expire: they name nothing now.
    pub(crate) fn purge_records_in(
        txn: &WriteTxn<'_>,
        id: CollectionId,
        budget: usize,
    ) -> Result<usize> {
        let mut records = txn.open_table(IDEMPOTENCY)?;
        let keys: Vec<Vec<u8>> = {
            let mut keys = Vec::new();
            for row in records.range(collection_records(id, None))?.take(budget) {
                let (key, _) = row?;
                keys.push(key.value().1.to_vec());
            }
            keys
        };
        for scope in &keys {
            records.remove((id.0, scope.as_slice()))?;
        }
        Ok(keys.len())
    }

    /// Whether collection `id` has any record stored. Not part of whether a
    /// name can be created again: records never hold that up, since an earlier
    /// life's are never read.
    pub(crate) fn records_under(&self, id: CollectionId) -> Result<bool> {
        use redb::ReadableDatabase;
        let txn = self.db().begin_read()?;
        let records = txn.open_table(IDEMPOTENCY)?;
        Ok(records.range(collection_records(id, None))?.next().transpose()?.is_some())
    }

    /// A late drop's share (ADR-231): the records of collection `standing`
    /// whose `last` is at or before `dropped`, [`RECORD_COLLECT_CHUNK`]
    /// examined per commit, with `stop` asked between chunks. For space only:
    /// once the sweep has taken the floor up to the drop, they are history
    /// whether they stay or go. Returns how many it removed.
    pub(crate) fn sweep_late_drop_records(
        &self,
        id: CollectionId,
        dropped: Stamp,
        stop: crate::late_drop::StopCheck<'_>,
    ) -> Result<usize> {
        let mut removed = 0usize;
        let mut after: Option<Vec<u8>> = None;
        loop {
            stop()?;
            let txn = self.begin_write(WriterHolder::Drop)?;
            let (gone, last, exhausted) = {
                let mut records = txn.open_table(IDEMPOTENCY)?;
                let mut gone = Vec::new();
                let mut last = None;
                let mut exhausted = true;
                for (examined, row) in
                    records.range(collection_records(id, after.as_deref()))?.enumerate()
                {
                    if examined == RECORD_COLLECT_CHUNK {
                        exhausted = false;
                        break;
                    }
                    let (key, value) = row?;
                    let (_, scope) = key.value();
                    last = Some(scope.to_vec());
                    if let Ok(Some(record)) = IdempotencyRecord::decode(value.value())
                        && record.last.hlc <= dropped.hlc
                    {
                        gone.push(scope.to_vec());
                    }
                }
                for scope in &gone {
                    records.remove((id.0, scope.as_slice()))?;
                }
                (gone.len(), last, exhausted)
            };
            if gone == 0 {
                txn.abort()?;
            } else {
                txn.commit()?;
            }
            removed += gone;
            if exhausted {
                break;
            }
            after = last;
        }
        Ok(removed)
    }

    /// A rewind's share (`kimmyd restore --until`), in its transaction: a
    /// record of a request whose every write the rewind discards (`first` after
    /// `until`) goes, since nothing of the request stands and a retry must apply
    /// it afresh. One the rewind cuts part-way (`first` at or before `until`,
    /// `last` after it) is a multi-chunk request that was still applying at
    /// `until`: it becomes `applying`, with no cursor, so a retry resumes from
    /// the beginning and the stamp rule writes what the rewind undid and nothing
    /// the request still holds. Its counts are left as they were, an
    /// overcount. Returns how many records it removed and reopened.
    pub(crate) fn rewind_records_in(txn: &WriteTxn<'_>, until: Hlc) -> Result<(usize, usize)> {
        let mut records = txn.open_table(IDEMPOTENCY)?;
        let mut gone = Vec::new();
        let mut reopened = Vec::new();
        // A stopped node's offline rewind: the whole table, as the rewind reads
        // the whole oplog.
        for row in records.iter()? {
            let (key, value) = row?;
            let (id, scope) = key.value();
            let Ok(Some(record)) = IdempotencyRecord::decode(value.value()) else { continue };
            if record.first.hlc > until {
                gone.push((id, scope.to_vec()));
            } else if record.last.hlc > until {
                let mut open = record;
                open.state = RecordState::Applying;
                if let RecordOutcome::Modify(m) = &mut open.outcome {
                    m.cursor = None;
                }
                reopened.push((id, scope.to_vec(), open.encode()));
            }
        }
        for (id, scope) in &gone {
            records.remove((*id, scope.as_slice()))?;
        }
        for (id, scope, bytes) in &reopened {
            records.insert((*id, scope.as_slice()), bytes.as_slice())?;
        }
        Ok((gone.len(), reopened.len()))
    }

    /// The highest stamp any record carries: the last key of
    /// [`IDEMPOTENCY_BY_STAMP`], one read. Part of where the clock resumes, so
    /// that a record taken from a sender whose clock is ahead is never resumed
    /// below its `first` after a restart (the witness that folded it in is in
    /// memory only).
    pub(crate) fn records_high_water(db: &redb::Database) -> Result<Hlc> {
        use redb::ReadableDatabase;
        let txn = db.begin_read()?;
        let rows = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
        Ok(match rows.last()? {
            Some((key, _)) => codec::decode_oplog_key(key.value()).map_or(Hlc::ZERO, |s| s.hlc),
            None => Hlc::ZERO,
        })
    }

    /// The `INFO` line a resume writes.
    pub(crate) fn log_resume(&self, coll: &CollectionMeta, record: &IdempotencyRecord) {
        if let RecordOutcome::Modify(m) = &record.outcome {
            info!(
                db = %coll.db,
                collection = %coll.name,
                origin = %record.origin,
                matched = m.matched,
                modified = m.modified,
                skipped = m.skipped,
                "resumed a keyed write that had stopped"
            );
        }
    }
}

/// The key range of collection `id`'s records, strictly after the scope `after`
/// when one is given. Its own and not `doc_range`'s, though the key's shape is
/// the same: a record's key holds a scope hash, never a document key.
fn collection_records(
    id: CollectionId,
    after: Option<&[u8]>,
) -> impl std::ops::RangeBounds<(u64, &[u8])> {
    use std::ops::Bound;
    let start = match after {
        Some(scope) => Bound::Excluded((id.0, scope)),
        None => Bound::Included((id.0, [].as_slice())),
    };
    let end = match id.0.checked_add(1) {
        Some(next) => Bound::Excluded((next, [].as_slice())),
        None => Bound::Unbounded,
    };
    (start, end)
}

/// The answer of a write made without a key, which has no record to replay.
pub(crate) fn unkeyed<T>(answer: Keyed<T>) -> T {
    match answer {
        Keyed::Fresh(value) => value,
        Keyed::Replayed(_) => unreachable!("a write made without a key has no record to replay"),
    }
}
