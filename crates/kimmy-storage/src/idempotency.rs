//! Idempotency records (ADR-240): a keyed write remembered in its own transaction.
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
//! 1. **Look the record up**, alive by its window below. The same fingerprint
//!    replays; another, or a contested record, is refused.
//! 2. **Write as today.**
//! 3. **If it wrote**, put the record and its [`IDEMPOTENCY_BY_STAMP`] rows at
//!    the record's merged `first` and `last`, which for a first local commit are
//!    one stamp, the stamp of the first entry the transaction wrote.
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
//! # A record lives its window
//!
//! A record is absent on lookup once it is **older than the window**,
//! `storage.oplog_retention_secs` measured on `last`'s own wall time
//! ([`Engine::set_idempotency_window`]); the cutoff is the retention pass's
//! own, so a record is never replayed past the window retention removes it at.
//! An absent record is overwritten by the next store, never merged with.
//!
//! **A record outlives a drop of its collection** for its window. A drop, a
//! database's drop and a late drop's sweep remove no record, and a record of a
//! dropped life is stored and served as any other: a retry after a drop and the
//! name created again replays its first answer and writes nothing to the new
//! life, and another request under the key is refused for the window (until
//! the name is created again, the API answers `404` as for any missing
//! collection). Retention is the only remover.
//!
//! **A keyed delete by id** is replayed while the request's own tombstone
//! stands: the document's stored row carries a stamp at or above the record's
//! `first`, the request's first write (an attempt's tombstone, or a later
//! version over it), or the row is gone and the collection's drop or
//! incarnation floor is at or above that stamp (the drop took the tombstone).
//! Once retention has collected the tombstone with no drop over it, the record
//! is absent for that request, which then runs afresh.
//!
//! **A contested record** is refused for every retry: two members accepted the
//! key for two requests (two fingerprints), and their records met in a merge
//! ([`IdempotencyRecord::contested`]). A multi-chunk request whose record turns
//! contested between its chunks stops, partially applied.
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
//! # For replication: records travel with coverage
//!
//! A record's **rows** ([`IDEMPOTENCY_BY_STAMP`], keyed origin-major, `node ‖
//! hlc`, so the rows of one origin between two stamps are one range) say which
//! stamps vouch for it: its merged `first` and `last`, and every stamp a frame
//! carried it at. A peer window ships, with whatever entries it holds
//! ([`records_for_coverage`]):
//!
//! - **F2**, every live record with a row inside the coverage the window grants
//!   the requester: above what it holds of that origin, and at or below what the
//!   window's end and this node's servable vector reach;
//! - **F3**, every live record with a row this node's own servable vector does
//!   not reach and the requester does not hold: a row a page or a lost entry
//!   left, which no window of this node would otherwise ever vouch for.
//!
//! A receiver stores every record a frame carries in the transaction that
//! raises its coverage, whether or not an entry of its collection was taken
//! and whether or not the collection stands ([`Engine::store_record`], merged,
//! rows at the merged `first` and `last` and at the sender's row, its stamps
//! witnessed, nothing written when it holds all of that already). So a member whose coverage of an
//! origin passes a row holds the record with `first` at or below it, and a
//! member that takes a request's effect under any origin takes its record with
//! it. [`records_for_collection_page`] reads a snapshot's records phase.
//!
//! # The records delta on snapshot pages
//!
//! A snapshot, scoped or converge page carries each collection's records once,
//! in its records phase, ahead of its documents; a keyed write on the sender
//! after that phase and before the page that holds its document would reach the
//! receiver without its record. So this node keeps a **change log**
//! ([`IDEMPOTENCY_CHANGES`]): one row per store that changes a record, on every
//! path, in the store's transaction ([`put_rows`], and a rewind's reopen),
//! keyed by a stamp minted under the writer, so in commit order. A store that
//! changes nothing writes no row. The log is never shipped by a window and not
//! carried by a backup; retention drops rows older than the record window.
//!
//! Every page after the first carries a **records delta**
//! ([`records_changed_since`]): the records the log names above the base its
//! cursor carries (`SnapshotCursor::records_base`), the log's high water as the
//! page before read it in its documents' transaction ([`changes_high_water`]).
//! One range, each change sent once, nothing for a store without keys. A page
//! carries documents only with the whole of the log above its base; otherwise
//! it is a records-only page and its base moves to the last key it examined.
//!
//! Keys are compared only within one **epoch** ([`changes_begin`]): a random
//! number the log is given when it begins, with its first change, or at an open
//! that finds records and no log of this node (a restore, since a backup carries
//! no log; an identity reset: [`begin_log_over_held_records`]). A base of another
//! epoch restarts the cursor's collection
//! from its records phase, whatever the keys say: a sender restored onto a host
//! whose clock is behind logs keys below a base cut before the restore.

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
use crate::tables::{self, IDEMPOTENCY, IDEMPOTENCY_BY_STAMP, IDEMPOTENCY_CHANGES};
use crate::walk::{WalkScope, open_walk_table};

/// Records removed per write transaction of a retention pass: the bound the
/// oplog step and the drop purge hold the writer to (ADR-151).
pub const RECORD_COLLECT_CHUNK: usize = 1_000;

/// Records on one snapshot page's records phase.
pub const RECORD_PAGE: usize = 512;

/// The change-log rows one page's records delta examines at most, whatever they
/// come to: the bound on one page's read, so a delta from an old base (a resumed
/// pull) or of a scoped pull over a busy store is read over several pages.
pub const RECORD_DELTA_ROWS: usize = 64 * 1024;

/// What a carried record costs on the wire beyond its bytes: the collection id,
/// the scope hash and BSON's framing, a bound and not a measure.
pub(crate) const CARRIED_OVERHEAD: usize = 96;

/// What a record of `len` bytes costs a page's bytes
/// (`snapshot::SNAPSHOT_PAGE_BYTES`).
pub(crate) fn carried_size(len: usize) -> usize {
    len + CARRIED_OVERHEAD
}

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
    /// smallest of its attempts' `first`. What the stamp rule judges a resume
    /// by: a resume writes a match only when it is stored below this.
    pub first: Stamp,
    /// **The stamp of the last entry the latest commit that changed the record
    /// wrote** (L1): for a commit of one entry, that entry's; for a bulk insert
    /// or a chunk of a filtered write, the highest stamp it wrote; the
    /// record-only commit that marks a request `done` keeps the standing
    /// record's (G8's residual). After a merge, the larger of the two. The
    /// record's row moves with it, and the window is measured on it: retention
    /// collects the record one window after `last`.
    pub last: Stamp,
    /// The member that wrote it.
    pub origin: NodeId,
    pub state: RecordState,
    pub outcome: RecordOutcome,
    /// **Two requests met under the key** (contested): two members accepted it for
    /// requests with two fingerprints, and their records merged. Every retry of
    /// either is refused ([`StorageError::IdempotencyKeyReused`]), for the
    /// record's window. Set by [`Self::merge`], beside `first` and never by the
    /// tie-break; a local store never sets it, since a lookup refuses another
    /// fingerprint before anything is written.
    pub contested: bool,
}

// -- The codec ------------------------------------------------------------------------------
//
//   version              u8 (RECORD_FORMAT)
//   fingerprint          32
//   first                26 (an oplog key)
//   last                 26
//   origin               16
//   state                u8: 0 applying, 1 done; 0x80 set when contested
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
/// The state byte's contested bit ([`IdempotencyRecord::contested`]). A record
/// written before the bit reads as not contested.
const STATE_CONTESTED: u8 = 0x80;
/// Where the state byte is: after the version, the fingerprint, `first`,
/// `last` and the origin.
const STATE_AT: usize = 1 + 32 + 2 * codec::STAMP_LEN + 16;
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
        let state = match self.state {
            RecordState::Applying => STATE_APPLYING,
            RecordState::Done => STATE_DONE,
        };
        out.push(if self.contested { state | STATE_CONTESTED } else { state });
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
        let state_byte = take_u8(&mut input)?;
        let contested = flag(state_byte, STATE_CONTESTED);
        let state = match state_byte & !STATE_CONTESTED {
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
        Ok(Some(Self { fingerprint, first, last, origin, state, outcome, contested }))
    }

    /// The record of a write committed here in one transaction: `first` and
    /// `last` are both the stamp of the first entry it wrote, and it is done.
    pub(crate) fn written(
        keyed: &KeyedWrite,
        first: Stamp,
        origin: NodeId,
        outcome: RecordOutcome,
    ) -> Self {
        Self::written_through(keyed, first, first, origin, outcome)
    }

    /// [`Self::written`] for a commit of several entries: `first` the stamp of
    /// the first it wrote, `last` the stamp of the last (L1).
    pub(crate) fn written_through(
        keyed: &KeyedWrite,
        first: Stamp,
        last: Stamp,
        origin: NodeId,
        outcome: RecordOutcome,
    ) -> Self {
        Self {
            fingerprint: keyed.fingerprint,
            first,
            last,
            origin,
            state: RecordState::Done,
            outcome,
            contested: false,
        }
    }

    /// Everything but `first` and `contested`, the bytes the merge breaks a tie
    /// by: the two the merge computes rather than picks.
    fn rest(&self) -> Vec<u8> {
        let mut bytes = self.encode();
        bytes[STATE_AT] &= !STATE_CONTESTED;
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
    ///
    /// `contested` is set when either side is, or when the two fingerprints
    /// differ: whether the requests merged into the result are two or more. A
    /// function of the union, so it keeps the join's three properties; computed
    /// beside `first`, after the winner is picked, and never part of the
    /// tie-break.
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
        let contested = self.contested || other.contested || self.fingerprint != other.fingerprint;
        Self { first, contested, ..winner.clone() }
    }
}

// -- The tables ------------------------------------------------------------------------------

/// The length of an [`IDEMPOTENCY_BY_STAMP`] key: `node (16) ‖ hlc`.
pub(crate) const ROW_KEY_LEN: usize = 16 + kimmy_core::HLC_ENCODED_LEN;

/// An [`IDEMPOTENCY_BY_STAMP`] key: **origin-major**, the node and then the
/// hlc, so that the rows of one origin between two stamps are one range (the
/// coverage rule's reads, [`records_for_coverage`]) and retention seeks each
/// origin's expired prefix. Not the oplog's `hlc ‖ node`.
pub(crate) fn row_key(stamp: &Stamp) -> [u8; ROW_KEY_LEN] {
    row_key_at(stamp.node, stamp.hlc)
}

fn row_key_at(node: NodeId, hlc: Hlc) -> [u8; ROW_KEY_LEN] {
    let mut key = [0u8; ROW_KEY_LEN];
    key[..16].copy_from_slice(&node.to_bytes());
    key[16..].copy_from_slice(&hlc.to_bytes());
    key
}

/// The stamp of an [`IDEMPOTENCY_BY_STAMP`] key, or `None` for a key of
/// another length.
pub(crate) fn decode_row_key(key: &[u8]) -> Option<Stamp> {
    if key.len() != ROW_KEY_LEN {
        return None;
    }
    let node: [u8; 16] = key[..16].try_into().ok()?;
    let hlc: [u8; kimmy_core::HLC_ENCODED_LEN] = key[16..].try_into().ok()?;
    Some(Stamp::new(Hlc::from_bytes(hlc), NodeId::from_bytes(node)))
}

/// A rewind's removal of the rows by stamp of the records it removed
/// (`named`), in its transaction: every row of a record is at or above its
/// `first`, so every such row is at or above `from`, the lowest `first` among
/// them (above the rewind's target for a record whose every write it
/// discarded). One range per origin, from `from`. A reopened record's rows,
/// the one at its `last` included, name a record that stays, and stay. Returns
/// how many rows it removed.
fn remove_rows_naming(
    txn: &WriteTxn<'_>,
    from: Hlc,
    named: &HashSet<(CollectionId, ScopeHash)>,
) -> Result<usize> {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    if named.is_empty() {
        return Ok(0);
    }
    let mut rows = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
    let mut doomed: Vec<Vec<u8>> = Vec::new();
    let mut past: Option<Vec<u8>> = None;
    loop {
        let next = match &past {
            None => rows.range::<&[u8]>(..)?.next().transpose()?,
            Some(past) => {
                rows.range::<&[u8]>((Excluded(past.as_slice()), Unbounded))?.next().transpose()?
            }
        };
        let Some((key, _)) = next else { break };
        let key = key.value().to_vec();
        let Some(at) = decode_row_key(&key) else {
            // A key of another length names nothing; retention removes it.
            past = Some(key);
            continue;
        };
        let (start, end) = (row_key_at(at.node, from), row_key_at(at.node, Hlc::MAX));
        for row in rows.range::<&[u8]>((Included(start.as_slice()), Included(end.as_slice())))? {
            let (key, value) = row?;
            if parse_row_value(value.value()).is_some_and(|row| named.contains(&row)) {
                doomed.push(key.value().to_vec());
            }
        }
        past = Some(end.to_vec());
    }
    for key in &doomed {
        rows.remove(key.as_slice())?;
    }
    Ok(doomed.len())
}

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

/// The stamps a stored record has rows at: its `first` and its `last` (one
/// stamp for a record of one commit), and the carrying stamp a frame named.
fn row_stamps(record: &IdempotencyRecord, carrying: Option<Stamp>) -> Vec<Stamp> {
    let mut stamps = vec![record.first, record.last];
    stamps.extend(carrying);
    stamps.sort();
    stamps.dedup();
    stamps
}

/// Write `record` under its key, and its rows ([`row_stamps`]), in `txn`, and
/// log the change ([`log_change`]): every store that writes comes here, and a
/// store that would change nothing does not.
///
/// **The row at `first` (F1)** is what lets a member that holds the record vouch
/// for it at the stamp a resume is judged by: a member whose coverage of
/// `first`'s origin passes it, through any window of a member that holds the
/// record, is sent the record (the coverage rule, [`records_for_coverage`]).
fn put_rows(
    engine: &Engine,
    txn: &redb::WriteTransaction,
    collection: CollectionId,
    scope: &ScopeHash,
    record: &IdempotencyRecord,
    carrying: Option<Stamp>,
) -> Result<()> {
    log_change(engine, txn, collection, scope)?;
    txn.open_table(IDEMPOTENCY)?
        .insert((collection.0, scope.as_bytes().as_slice()), record.encode().as_slice())?;
    let mut rows = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
    let value = row_value(collection, scope);
    for stamp in row_stamps(record, carrying) {
        rows.insert(row_key(&stamp).as_slice(), value.as_slice())?;
    }
    Ok(())
}

/// Whether `stored` is exactly what is under the key, and every row a store of
/// it would write is there already: a store that would write nothing.
fn store_is_a_no_op(
    txn: &redb::WriteTransaction,
    collection: CollectionId,
    scope: &ScopeHash,
    stored: &IdempotencyRecord,
    carrying: Option<Stamp>,
) -> Result<bool> {
    let encoded = stored.encode();
    let same = txn
        .open_table(IDEMPOTENCY)?
        .get((collection.0, scope.as_bytes().as_slice()))?
        .is_some_and(|raw| raw.value() == encoded.as_slice());
    if !same {
        return Ok(false);
    }
    let rows = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
    let value = row_value(collection, scope);
    for stamp in row_stamps(stored, carrying) {
        let present = rows
            .get(row_key(&stamp).as_slice())?
            .is_some_and(|row| row.value() == value.as_slice());
        if !present {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A record on the wire: its key, the stamp of the entry that carries it, and
/// its bytes as stored (ADR-240, "How a record travels").
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

/// The records a peer window ships, read in the window's own read transaction
/// (ADR-240, records travel with coverage). For each origin `o`, with `floor[o]` what
/// the requester holds of it:
///
/// - **F3 first**: every record with a row of `o` above both `floor[o]` and
///   this node's `servable[o]`, a stamp no window of this node can vouch for by
///   its oplog ([`records_unanchored`]);
/// - **F2**: every record with a row of `o` in `(floor[o], reach[o]]`, where
///   `reach` is the coverage the window grants ([`records_in_advance`]).
///
/// One per key, **carried at the lowest row found** across F3 and F2, and only a
/// record that stands under its key (a row whose record was purged or collected
/// ships nothing). The lowest, and not the first found: F3 is read first and F2
/// origin by origin in node order, so the first row found can lie above another
/// row the same advance covers, and a batch that stops, or splits into runs,
/// between the two would take the effect at the lower row without the record
/// (the receiver stores a record by its carrying row). Returns them with the F3
/// records leading, and how many those are: what the transport fits first.
pub fn records_for_coverage(
    txn: &ReadTransaction,
    floor: &dyn Fn(NodeId) -> Hlc,
    reach: &kimmy_core::VersionVector,
    servable: &kimmy_core::VersionVector,
) -> Result<(Vec<CarriedRecord>, usize)> {
    let rows = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
    let records = txn.open_table(IDEMPOTENCY)?;
    let mut shipping = Shipping { seen: Default::default(), out: Vec::new() };
    records_unanchored(&rows, &records, floor, servable, &mut shipping)?;
    let unanchored = shipping.out.len();
    records_in_advance(&rows, &records, floor, reach, &mut shipping)?;
    Ok((shipping.out, unanchored))
}

/// The records a window has found so far, one per key.
struct Shipping {
    /// Per key: its place in `out`, and the standing record's `first` (`None`
    /// for a record this build cannot decode).
    seen: std::collections::HashMap<(u64, [u8; 32]), (usize, Option<Stamp>)>,
    out: Vec<CarriedRecord>,
}

impl Shipping {
    /// Ship the record the row `(key, value)` names, unless nothing stands under
    /// its key or the row is **below the record's `first`**; a record shipped
    /// already is carried at this row instead when it is lower than the one it
    /// was carried at (see [`records_for_coverage`]).
    ///
    /// Every row a record has is at or above its merged `first` (its `first`, its
    /// `last`, and the sender's rows it was stored with, each at or above that
    /// sender's `first`). A row below it was left by another record under the same
    /// key, one past its window that a new request's record replaced: it vouches
    /// for nothing of the record standing now. A row whose record is gone (a
    /// retention pass stopped between chunks leaves one until the next pass)
    /// names nothing and ships nothing.
    fn row(
        &mut self,
        records: &impl ReadableTable<(u64, &'static [u8]), &'static [u8]>,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let (Some(carrying), Some((collection, scope))) =
            (decode_row_key(key), parse_row_value(value))
        else {
            return Ok(());
        };
        if let Some(&(at, first)) = self.seen.get(&(collection.0, *scope.as_bytes())) {
            let shipped = &mut self.out[at];
            if shipped.carrying.is_some_and(|row| carrying < row)
                && first.is_none_or(|first| carrying >= first)
            {
                shipped.carrying = Some(carrying);
            }
            return Ok(());
        }
        let Some(record) = records.get((collection.0, scope.as_bytes().as_slice()))? else {
            return Ok(());
        };
        let first = IdempotencyRecord::decode(record.value()).ok().flatten().map(|r| r.first);
        if first.is_some_and(|first| carrying < first) {
            return Ok(());
        }
        self.seen.insert((collection.0, *scope.as_bytes()), (self.out.len(), first));
        self.out.push(CarriedRecord {
            collection,
            scope,
            carrying: Some(carrying),
            record: record.value().to_vec(),
        });
        Ok(())
    }
}

/// F3 of [`records_for_coverage`]: for each origin with a row, one seek to that
/// origin's rows above `max(floor, servable)` and a range over them. Bounded by
/// the origins that have rows and the rows this node holds above its own
/// servable vector: rows a page or a lost entry left, which its next pull of
/// their entries anchors.
fn records_unanchored(
    rows: &impl ReadableTable<&'static [u8], &'static [u8]>,
    records: &impl ReadableTable<(u64, &'static [u8]), &'static [u8]>,
    floor: &dyn Fn(NodeId) -> Hlc,
    servable: &kimmy_core::VersionVector,
    shipping: &mut Shipping,
) -> Result<()> {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    let mut past: Option<[u8; ROW_KEY_LEN]> = None;
    loop {
        // not a row: the next origin with a row, one seek.
        let next = match &past {
            None => rows.range::<&[u8]>(..)?.next(),
            Some(past) => rows.range::<&[u8]>((Excluded(past.as_slice()), Unbounded))?.next(),
        };
        let Some(next) = next.transpose()? else { return Ok(()) };
        let key = next.0.value().to_vec();
        drop(next);
        let Some(at) = decode_row_key(&key) else {
            // A key of another length: passed over, as a row naming nothing.
            past = key.as_slice().try_into().ok();
            if past.is_none() {
                return Ok(());
            }
            continue;
        };
        let above = floor(at.node).max(servable.get(at.node));
        let (from, to) = (row_key_at(at.node, above), row_key_at(at.node, Hlc::MAX));
        for row in rows.range::<&[u8]>((Excluded(from.as_slice()), Included(to.as_slice())))? {
            let (key, value) = row?;
            shipping.row(records, key.value(), value.value())?;
        }
        past = Some(to);
    }
}

/// F2 of [`records_for_coverage`]: for each origin the window's coverage
/// reaches past what the requester holds, one range over that origin's rows in
/// `(floor, reach]`. Bounded by the rows inside the advance the window grants:
/// at most a row per stamp the requester's coverage passes, entries this node
/// holds or not.
fn records_in_advance(
    rows: &impl ReadableTable<&'static [u8], &'static [u8]>,
    records: &impl ReadableTable<(u64, &'static [u8]), &'static [u8]>,
    floor: &dyn Fn(NodeId) -> Hlc,
    reach: &kimmy_core::VersionVector,
    shipping: &mut Shipping,
) -> Result<()> {
    use std::ops::Bound::{Excluded, Included};
    for (node, reached) in reach.iter() {
        let held = floor(node);
        if reached <= held {
            continue;
        }
        let (from, to) = (row_key_at(node, held), row_key_at(node, reached));
        for row in rows.range::<&[u8]>((Excluded(from.as_slice()), Included(to.as_slice())))? {
            let (key, value) = row?;
            shipping.row(records, key.value(), value.value())?;
        }
    }
    Ok(())
}

/// One row of this node's change log ([`IDEMPOTENCY_CHANGES`]) for a store of
/// the record under `(collection, scope)`, in the store's transaction, keyed by a
/// stamp minted for it now. The caller holds the writer, so keys are in commit
/// order.
///
/// **The log begins** with the first change it holds, and begins again, with a
/// new epoch, at the first change after an open that found no beginning, or one
/// this node's identity did not write, over a store that held no record. One
/// that held records began it at the open ([`begin_log_over_held_records`]).
fn log_change(
    engine: &Engine,
    txn: &redb::WriteTransaction,
    collection: CollectionId,
    scope: &ScopeHash,
) -> Result<()> {
    let at = engine.next_stamp();
    let key = codec::oplog_key(&at);
    let mut log = txn.open_table(IDEMPOTENCY_CHANGES)?;
    let begun = log
        .get(LOG_BEGINS)?
        .and_then(|raw| LogBegin::decode(raw.value()))
        .is_some_and(|begun| begun.at.node == engine.node_id());
    if !begun {
        let begin = LogBegin { at, epoch: new_epoch() };
        log.insert(LOG_BEGINS, begin.encode().as_slice())?;
    }
    log.insert(key.as_slice(), row_value(collection, scope).as_slice())?;
    Ok(())
}

/// At open: **begin the change log, with a new epoch, over the records this
/// store holds** when it holds any and no log of this node's identity has
/// begun ([`IDEMPOTENCY_CHANGES`]'s empty key absent, or another identity's).
/// Returns whether it began one.
///
/// Such records were written by no change this log names: a restore (a backup
/// carries records and no log), an identity reset, a store from before the log.
/// Without a beginning, this node would cut the **zero base** (`RecordsBase`,
/// "a log that has held nothing") on its next page, and answer a zero base cut
/// by an earlier store with "nothing changed": a sender restored once from a
/// backup with records serves a page with the zero base, is restored again from
/// another, and the next page brings that backup's effect without its record.
/// Begun here, a base this node cuts names the epoch, and a base cut before
/// the next restore is of another epoch, which restarts its collection from its
/// records phase. A zero base cut by a sender that held no records, answered
/// after it was restored with some, is the requester's to refuse
/// (`SnapshotPage::records_epoch`).
///
/// Before the engine serves, so no page is cut before it, in a transaction of
/// its own (metadata alone, as a schema change's); the beginning's key is a
/// stamp of this node's clock minted under the writer, above every key a page of
/// this store cut before the open (`resume_hlc` covers the log).
pub(crate) fn begin_log_over_held_records(engine: &Engine) -> Result<bool> {
    use redb::ReadableDatabase;
    {
        let txn = engine.db().begin_read()?;
        // not a row of the walk: whether the store holds any record, one seek.
        let holds = txn.open_table(IDEMPOTENCY)?.first()?.is_some();
        let begun = changes_begin(&txn)?.is_some_and(|begun| begun.at.node == engine.node_id());
        if !holds || begun {
            return Ok(false);
        }
    }
    let txn = engine.begin_write(WriterHolder::Ddl)?;
    // Minted under the writer, as every log key is.
    let begin = LogBegin { at: engine.next_stamp(), epoch: new_epoch() };
    txn.open_table(IDEMPOTENCY_CHANGES)?.insert(LOG_BEGINS, begin.encode().as_slice())?;
    txn.commit()?;
    info!(
        epoch = begin.epoch,
        "this store holds idempotency records and no change log of this node: the log begins \
         again here, so a snapshot cursor cut before (a restore, an identity reset) restarts its \
         collection from its records"
    );
    Ok(true)
}

/// Begin this node's change log now, with a new epoch and no change in it: the
/// state of a sender whose log began and whose records retention has collected
/// since, so its pages cut bases that name an epoch. For a test of a delta from
/// such a base, which a pull from a log that has never begun (the zero base)
/// cannot show past its first keyed write (`SnapshotPage::records_epoch`).
#[cfg(test)]
pub(crate) fn begin_log_for_test(engine: &Engine) -> Result<LogBegin> {
    let txn = engine.begin_write(WriterHolder::Ddl)?;
    let begin = LogBegin { at: engine.next_stamp(), epoch: new_epoch() };
    txn.open_table(IDEMPOTENCY_CHANGES)?.insert(LOG_BEGINS, begin.encode().as_slice())?;
    txn.commit()?;
    Ok(begin)
}

/// A new log's epoch: random, never 0.
fn new_epoch() -> i64 {
    loop {
        let (high, _) = uuid::Uuid::new_v4().as_u64_pair();
        if high != 0 {
            return high as i64;
        }
    }
}

/// Where this node's change log began ([`IDEMPOTENCY_CHANGES`]'s empty key): the
/// key of its first change, and its epoch (`snapshot::RecordsBase`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LogBegin {
    pub at: Stamp,
    pub epoch: i64,
}

impl LogBegin {
    /// `key (26) ‖ epoch (8, big-endian)`.
    fn encode(&self) -> [u8; codec::STAMP_LEN + 8] {
        let mut out = [0u8; codec::STAMP_LEN + 8];
        out[..codec::STAMP_LEN].copy_from_slice(&codec::oplog_key(&self.at));
        out[codec::STAMP_LEN..].copy_from_slice(&self.epoch.to_be_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let (at, epoch) = bytes.split_at_checked(codec::STAMP_LEN)?;
        Some(Self {
            at: codec::decode_oplog_key(at).ok()?,
            epoch: i64::from_be_bytes(epoch.try_into().ok()?),
        })
    }
}

/// The change log's empty key: where it begins.
const LOG_BEGINS: &[u8] = &[];

/// Where this node's change log began, and its epoch, as `txn` sees it; `None`
/// while it has held no change.
pub(crate) fn changes_begin(txn: &ReadTransaction) -> Result<Option<LogBegin>> {
    let log = txn.open_table(IDEMPOTENCY_CHANGES)?;
    let Some(raw) = log.get(LOG_BEGINS)? else { return Ok(None) };
    Ok(LogBegin::decode(raw.value()))
}

/// The change log's high water as `txn` sees it: its last key, or where it began
/// when retention has dropped every row; `None` while it has held none.
pub(crate) fn changes_high_water(txn: &ReadTransaction) -> Result<Option<Stamp>> {
    use std::ops::Bound::{Excluded, Unbounded};
    let log = txn.open_table(IDEMPOTENCY_CHANGES)?;
    // not a row: the log's last key, one seek.
    let last = log.range::<&[u8]>((Excluded(LOG_BEGINS), Unbounded))?.next_back();
    if let Some(last) = last.transpose()?
        && let Ok(stamp) = codec::decode_oplog_key(last.0.value())
    {
        return Ok(Some(stamp));
    }
    Ok(changes_begin(txn)?.map(|begun| begun.at))
}

/// [`changes_high_water`]'s time, at open, for where the clock resumes.
pub(crate) fn changes_high_water_at_open(db: &redb::Database) -> Result<Hlc> {
    use redb::ReadableDatabase;
    let txn = db.begin_read()?;
    Ok(changes_high_water(&txn)?.map_or(Hlc::ZERO, |stamp| stamp.hlc))
}

/// What one page's records delta may carry and read ([`records_changed_since`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct DeltaBudget {
    /// The bytes of its records: what the page has for its rows
    /// (`snapshot::SNAPSHOT_PAGE_BYTES` less its definitions and drops), of
    /// which the records phase and the documents have what the delta leaves.
    pub bytes: usize,
    /// The log rows it examines, at most [`RECORD_DELTA_ROWS`].
    pub rows: usize,
}

impl DeltaBudget {
    /// A page's.
    pub(crate) const PAGE: Self =
        Self { bytes: crate::snapshot::SNAPSHOT_PAGE_BYTES, rows: RECORD_DELTA_ROWS };
}

/// One page's records delta.
#[derive(Debug)]
pub(crate) struct Delta {
    /// The records, each once, as stored, with no carrying stamp.
    pub records: Vec<CarriedRecord>,
    /// `None` when the delta holds **the whole remainder**: every log row above
    /// the base, read to the log's end. Otherwise the last key it examined, every
    /// row up to which it carries: the base the next page goes on from.
    pub stopped_at: Option<Stamp>,
}

/// One page's **records delta** (ADR-240, "Snapshots carry records"): the records the
/// change log names above `base`, of a collection `standing` names, each once,
/// read in the page's own read transaction, so a page never holds a document
/// whose record changed after its collection's records phase without the
/// record. One range of the log, in key order; a log key is taken under the
/// writer, so every change committed after the read that cut `base` is above it.
///
/// Stops before the budget is passed ([`DeltaBudget`]), naming the last key it
/// examined; a record larger than the whole budget, which a page holding nothing
/// else cannot carry, is refused ([`StorageError::SnapshotRowTooLarge`]). A row whose record
/// is gone (purged or collected) carries nothing. No keyed change since `base` is
/// an empty read.
pub(crate) fn records_changed_since(
    txn: &ReadTransaction,
    base: Stamp,
    standing: &dyn Fn(CollectionId) -> bool,
    budget: DeltaBudget,
) -> Result<Delta> {
    use std::ops::Bound::{Excluded, Unbounded};
    let log = txn.open_table(IDEMPOTENCY_CHANGES)?;
    let records = txn.open_table(IDEMPOTENCY)?;
    let mut out = Vec::new();
    let mut seen: HashSet<(u64, [u8; 32])> = HashSet::new();
    let (mut bytes, mut examined) = (0usize, 0usize);
    let mut last: Option<Stamp> = None;
    let rows_cap = budget.rows.max(1);
    let from = codec::oplog_key(&base);
    for row in log.range::<&[u8]>((Excluded(from.as_slice()), Unbounded))? {
        let (key, value) = row?;
        let Ok(at) = codec::decode_oplog_key(key.value()) else { continue };
        if examined == rows_cap {
            return Ok(Delta { records: out, stopped_at: last });
        }
        examined += 1;
        let named = parse_row_value(value.value())
            .filter(|(collection, scope)| {
                standing(*collection) && !seen.contains(&(collection.0, scope.0))
            })
            .map(|(collection, scope)| {
                records
                    .get((collection.0, scope.as_bytes().as_slice()))
                    .map(|raw| raw.map(|raw| (collection, scope, raw.value().to_vec())))
            })
            .transpose()?
            .flatten();
        if let Some((collection, scope, record)) = named {
            let size = carried_size(record.len());
            if out.is_empty() && size > budget.bytes {
                // Larger than a page holding nothing else: it cannot cross on one.
                return Err(StorageError::SnapshotRowTooLarge {
                    what: format!("the idempotency record of a request on collection {collection}"),
                    bytes: size,
                    room: budget.bytes,
                });
            }
            if bytes + size > budget.bytes {
                // This row is the next page's first.
                return Ok(Delta { records: out, stopped_at: last });
            }
            seen.insert((collection.0, scope.0));
            bytes += size;
            out.push(CarriedRecord { collection, scope, carrying: None, record });
        }
        last = Some(at);
    }
    Ok(Delta { records: out, stopped_at: None })
}

/// One page of a collection's records for a snapshot's records phase: up to
/// `limit` (at most [`RECORD_PAGE`]) records after the scope `after`, in scope
/// order, within `room` bytes ([`carried_size`] each), and whether more of the
/// collection's records are left: one that does not fit is the next page's
/// first. With `refuse`, the page holds nothing else, and a first record larger
/// than `room` cannot cross on a page: it is refused
/// ([`StorageError::SnapshotRowTooLarge`], `snapshot::SNAPSHOT_PAGE_BYTES`).
#[allow(clippy::type_complexity)]
pub fn records_for_collection_page(
    txn: &ReadTransaction,
    collection: CollectionId,
    after: Option<&ScopeHash>,
    limit: usize,
    room: usize,
    refuse: bool,
) -> Result<(Vec<(ScopeHash, Vec<u8>)>, bool)> {
    let limit = limit.clamp(1, RECORD_PAGE);
    let records = txn.open_table(IDEMPOTENCY)?;
    let mut page = Vec::new();
    let mut used = 0usize;
    for row in
        records.range(collection_records(collection, after.map(|s| s.as_bytes().as_slice())))?
    {
        let (key, value) = row?;
        let (_, scope) = key.value();
        if page.len() == limit {
            return Ok((page, true));
        }
        let Some(scope) = ScopeHash::from_slice(scope) else { continue };
        let size = carried_size(value.value().len());
        if used + size > room {
            if refuse && page.is_empty() {
                return Err(StorageError::SnapshotRowTooLarge {
                    what: format!(
                        "the idempotency record of a request on collection {collection} (a \
                         find_and_modify's document)"
                    ),
                    bytes: size,
                    room,
                });
            }
            return Ok((page, true));
        }
        used += size;
        page.push((scope, value.value().to_vec()));
    }
    Ok((page, false))
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
    /// The wall clock the window reads, set by a test that moves time; 0 is
    /// the real one.
    #[cfg(any(test, feature = "test-hooks"))]
    now_ms: AtomicU64,
    /// A test's budget for a page's records delta; `None` is the real one.
    #[cfg(test)]
    delta_budget: parking_lot::Mutex<Option<DeltaBudget>>,
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
            #[cfg(test)]
            delta_budget: parking_lot::Mutex::new(None),
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
/// collection **at or after the record's row** (`carrying`), or else in the
/// batch's last transaction, the one that raises the witnessed vector (ADR-240,
/// records travel with coverage).
///
/// Not in the run of an entry before the row: a record is stored with the
/// coverage that vouches for it, never ahead of it. (Before records outlived a
/// drop of their collection, this also kept a later life's record out of the
/// run of an earlier life's entry, where it met that life's record under the
/// key; a record now lives its window whatever the drops, and the merge is the
/// join in any order.)
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

    /// Whether any of the frame's records is of collection `id`, rides at or
    /// before the entry stamped `at` (see [`Self::rides_by`]), and is not stored
    /// yet.
    pub(crate) fn owes(&self, id: CollectionId, at: Stamp) -> bool {
        self.records.iter().any(|(collection, _, carrying, _, stored)| {
            *collection == id && !stored && Self::rides_by(*carrying, at)
        })
    }

    /// Whether a record carried at `carrying` is stored with the entry stamped
    /// `at`: its row is at or before the entry (a record with no row rides with
    /// any). One whose row is past the entry waits for an entry at or after it,
    /// or for the batch's last transaction. With a window carrying each record
    /// at the lowest of its own rows it found, riding by that row stores it
    /// no later than the run that takes the effect there; an effect below it
    /// lies in coverage the receiver held already, for which it holds a version
    /// of the record (INV-C).
    fn rides_by(carrying: Option<Stamp>, at: Stamp) -> bool {
        carrying.is_none_or(|row| row <= at)
    }

    /// Whether any of the frame's records is not stored yet.
    pub(crate) fn owes_any(&self) -> bool {
        self.records.iter().any(|(_, _, _, _, stored)| !stored)
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
    /// collection `id` that is not stored yet and rides by the entry stamped `at`
    /// ([`FrameRecords::rides_by`]): called when the run takes (applies or
    /// supersedes, history included) that entry of that collection, on every
    /// outcome. Each through [`Self::store_record`]: merged with the live
    /// record, rows at the merged `last` and at the carrying stamp, its stamps
    /// witnessed. Marked stored, so a later entry of the collection asks nothing
    /// again. Returns how many were stored.
    pub(crate) fn store_frame_records(
        &self,
        txn: &WriteTxn<'_>,
        id: CollectionId,
        at: Stamp,
        frame: &mut FrameRecords,
    ) -> Result<usize> {
        let mut stored = 0usize;
        for (collection, scope, carrying, record, done) in &mut frame.records {
            if *collection != id || *done || !FrameRecords::rides_by(*carrying, at) {
                continue;
            }
            let (_, wrote) = self.store_record_noting(txn, id, scope, record, *carrying)?;
            stored += usize::from(wrote);
            *done = true;
        }
        Ok(stored)
    }

    /// Store, in the transaction that raises this node's coverage for the frame,
    /// every record of the frame no run stored: **whether or not an entry of its
    /// collection was taken, and whether or not the collection stands here**. A
    /// window grants coverage per origin, not per collection, and a stamp it
    /// covers is never served here again, so the record it vouched for at that
    /// stamp is stored now or never. Returns how many it wrote: a record held
    /// already writes nothing.
    ///
    /// **Not one whose row is at or past `stop`**, the entry a batch ended at: the
    /// window grants no coverage there, so the record is served again with what
    /// follows, and the entries between the stop and its row are not taken: the
    /// coverage that vouches for it is not granted ([`FrameRecords`]).
    pub(crate) fn store_owed_records(
        &self,
        txn: &WriteTxn<'_>,
        frame: &mut FrameRecords,
        stop: Option<Stamp>,
    ) -> Result<usize> {
        let mut stored = 0usize;
        for (collection, scope, carrying, record, done) in &mut frame.records {
            if *done || carrying.is_some_and(|row| stop.is_some_and(|stop| row >= stop)) {
                continue;
            }
            let (_, wrote) =
                self.store_record_noting(txn, *collection, scope, record, *carrying)?;
            stored += usize::from(wrote);
            *done = true;
        }
        Ok(stored)
    }

    /// Store a snapshot or converge page's records in the page's transaction
    /// (ADR-240, "Snapshots carry records"): each through
    /// [`Self::store_record_noting`] with no carrying stamp (rows at its merged
    /// `first` and `last`, and a row of this node's change log), whether or not
    /// its collection stands here: a record outlives a drop of its collection
    /// for its window. Those this build cannot decode are dropped and counted. Returns how many it **changed**, to
    /// count once the page's transaction commits: a page whose records change
    /// nothing here writes nothing and costs no commit, as a page whose documents
    /// all lose costs none (ADR-152).
    pub(crate) fn store_page_records(
        &self,
        txn: &WriteTxn<'_>,
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
            let (_, wrote) =
                self.store_record_noting(txn, wire.collection, &wire.scope, &record, None)?;
            stored += usize::from(wrote);
        }
        Ok(stored)
    }

    /// [`Self::store_page_records`] in a transaction of their own, counted once it
    /// commits: a page whose documents are not applied here (a converge page from
    /// a sender that dropped the collection, or one of a life this member has not
    /// buried) still leaves its records, which outlive a drop for their window.
    /// Nothing is committed when they change nothing.
    pub(crate) fn store_page_records_alone(
        &self,
        records: &[CarriedRecord],
        via: RecordsVia,
    ) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }
        let txn = self.begin_write(WriterHolder::Repair)?;
        let stored = match self.store_page_records(&txn, records) {
            Ok(stored) => stored,
            Err(e) => {
                txn.abort()?;
                return Err(e);
            }
        };
        if stored == 0 {
            txn.abort()?;
        } else {
            txn.commit()?;
            self.count_records_received(via, stored);
        }
        Ok(stored)
    }

    /// The budget of one page's records delta: [`DeltaBudget::PAGE`], or a test's.
    pub(crate) fn record_delta_budget(&self) -> DeltaBudget {
        #[cfg(test)]
        if let Some(budget) = *self.idempotency.delta_budget.lock() {
            return budget;
        }
        DeltaBudget::PAGE
    }

    /// Set the room of a snapshot page's rows in bytes (its delta's, records
    /// phase's and documents' together, `snapshot::SNAPSHOT_PAGE_BYTES`) and the
    /// log rows its delta examines, for a test that pages without writing 63 MiB
    /// or 65,536 rows; `None` for the real ones.
    #[cfg(test)]
    pub(crate) fn set_record_delta_budget_for_test(&self, bytes_and_rows: Option<(usize, usize)>) {
        *self.idempotency.delta_budget.lock() =
            bytes_and_rows.map(|(bytes, rows)| DeltaBudget { bytes, rows });
        self.set_snapshot_room_for_test(bytes_and_rows.map(|(bytes, _)| bytes));
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

    /// The record stored under a key, as stored, window or not: for tests
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

    /// Set the wall clock the window reads, for a test whose clocks run
    /// ahead of it; `None` for the real one.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_idempotency_clock_for_test(&self, now_ms: Option<u64>) {
        self.idempotency.now_ms.store(now_ms.unwrap_or(0), Ordering::SeqCst);
    }

    /// Store `record` under a key as a peer's frame stores it
    /// ([`Self::store_record`]: merged with the live record, its rows written,
    /// its stamps witnessed), in a transaction of its own. For a test outside
    /// this crate that needs what only replication brings, such as a record
    /// two members met in (a contested record). Returns the record as stored.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn store_peer_record_for_test(
        &self,
        collection: CollectionId,
        scope: &ScopeHash,
        record: &IdempotencyRecord,
    ) -> Result<Option<IdempotencyRecord>> {
        let txn = self.begin_write(WriterHolder::Replication)?;
        let stored = self.store_record(&txn, collection, scope, record, Some(record.last))?;
        txn.commit()?;
        Ok(stored)
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

    /// The key's record if it is alive: stored, readable by this build and not
    /// past the window, whether or not a drop of its collection came after it.
    /// A record a newer build wrote reads as none.
    fn live_record_in(
        &self,
        txn: &redb::WriteTransaction,
        collection: CollectionId,
        scope: &ScopeHash,
    ) -> Result<Option<IdempotencyRecord>> {
        let Some(record) = read_record(&txn.open_table(IDEMPOTENCY)?, collection, scope)? else {
            return Ok(None);
        };
        if self.record_is_past_window(&record) {
            return Ok(None);
        }
        Ok(Some(record))
    }

    /// The collection under `coll`'s name as `txn` sees it: a keyed delete's
    /// replay reads the floor that stands under the writer, not the one the
    /// caller read.
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

    /// Whether a keyed delete's tombstone, stamped at or above `deleted` (the
    /// record's `first`) and gone from `coll`'s rows, went with a drop: the
    /// collection's drop tombstone, or the floor of the life standing under the
    /// writer, at or above it ([`Engine::is_history`], a tie going to the drop).
    pub(crate) fn delete_taken_by_a_drop(
        &self,
        txn: &WriteTxn<'_>,
        coll: &CollectionMeta,
        deleted: Stamp,
    ) -> Result<bool> {
        let standing = Self::standing_in(txn, coll)?;
        self.is_history(&mut Memo::default(), standing.as_ref(), coll.id, deleted)
    }

    /// The lookup a keyed write makes first, under the writer, in its own
    /// transaction: the authoritative one, so two duplicates on one member
    /// serialise here.
    ///
    /// Refuses a key whose multi-chunk request runs here (unless `owner` is
    /// that request), and a key whose live record has another fingerprint or
    /// is contested: before replay, resume, or a delete's tombstone test, so
    /// every keyed call refuses it alike. A record it uses has its stamps
    /// witnessed: a resume must write above its `first`, and a new record must
    /// merge above its `last`.
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
        let Some(record) = self.live_record_in(txn, coll.id, &keyed.scope)? else {
            return Ok(Found::Absent);
        };
        self.witness(&record.first);
        self.witness(&record.last);
        if record.contested || record.fingerprint != keyed.fingerprint {
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
    ///
    /// **Refused when it has turned contested** (or another request's), with
    /// [`StorageError::IdempotencyKeyReused`]: another request's record met this
    /// one's, and its `first` is no bound for this request's stamp rule. The
    /// caller has committed chunks, so the request ends partially applied
    /// (ADR-192), and every later retry is refused at its lookup.
    pub(crate) fn keyed_live_record(
        &self,
        txn: &WriteTxn<'_>,
        coll: &CollectionMeta,
        keyed: &KeyedWrite,
    ) -> Result<Option<IdempotencyRecord>> {
        let record = self.live_record_in(txn, coll.id, &keyed.scope)?;
        if let Some(record) = &record {
            self.witness(&record.first);
            self.witness(&record.last);
            if record.contested || record.fingerprint != keyed.fingerprint {
                return Err(StorageError::IdempotencyKeyReused);
            }
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
    /// with: a record past the window, or one this build cannot read, belongs to
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
        put_rows(self, txn, collection, scope, &merged, None)?;
        Ok(merged)
    }

    /// Store a record a peer sent, in the transaction that raises this node's
    /// coverage (or a snapshot or converge page's): merged into the live one,
    /// with rows at the merged `first` and `last` and, when the frame named one,
    /// at the sender's row (`carrying`), so this node vouches for it onward at
    /// the stamps it was vouched for to it. The record's stamps are witnessed: a
    /// resume here must write above its `first`.
    ///
    /// Stored whether or not a drop of its collection came after it, or the
    /// collection stands: a record outlives a drop for its window. **Not stored
    /// when it is past its window**, the one test of a record's life, **unless
    /// the live record here is of the same request** (the same fingerprint) **and
    /// it is not contested** (a contested record stands for two or more
    /// requests, and merged, its flag would contest the live one):
    /// merged into the live record of a new request under the key, it would
    /// lower that record's `first` to the old request's, and mark it contested;
    /// merged into one of its own request's later attempts, it lowers `first` to
    /// its own attempt's, so the stamp rule leaves what that attempt wrote, which
    /// this member may take from a sender still serving it. What stands under the
    /// key and is not alive (past the window, unreadable by this build) is
    /// overwritten. Returns the record as stored, or `None`.
    pub(crate) fn store_record(
        &self,
        txn: &WriteTxn<'_>,
        collection: CollectionId,
        scope: &ScopeHash,
        record: &IdempotencyRecord,
        carrying: Option<Stamp>,
    ) -> Result<Option<IdempotencyRecord>> {
        let (stored, _) = self.store_record_noting(txn, collection, scope, record, carrying)?;
        Ok(stored)
    }

    /// [`Self::store_record`], also saying whether it wrote anything. **A store
    /// that would change nothing writes nothing** (the merge is what stands and
    /// its rows are there): a sender ships a record it cannot vouch for in every
    /// window until it can (F3), and a window of records a receiver holds already
    /// must not cost it a write, nor a commit where nothing else is written. The
    /// check is against the record as stored, alive or past its window.
    pub(crate) fn store_record_noting(
        &self,
        txn: &WriteTxn<'_>,
        collection: CollectionId,
        scope: &ScopeHash,
        record: &IdempotencyRecord,
        carrying: Option<Stamp>,
    ) -> Result<(Option<IdempotencyRecord>, bool)> {
        let stored = match read_record(&txn.open_table(IDEMPOTENCY)?, collection, scope) {
            Ok(stored) => stored,
            // A local record this format cannot be: the peer's is good data,
            // and refusing it would fail the run for good.
            Err(StorageError::Corrupt(why)) => {
                warn!(%why, "an idempotency record here does not decode; a peer's replaces it");
                None
            }
            Err(e) => return Err(e),
        };
        let live = stored.as_ref().filter(|stored| !self.record_is_past_window(stored));
        // A past-window record merges only as the live record's own request, and
        // only when it stands for one: a contested record stands for two or more,
        // and its flag would carry to the live request.
        let own_request =
            !record.contested && live.is_some_and(|live| live.fingerprint == record.fingerprint);
        if self.record_is_past_window(record) && !own_request {
            return Ok((None, false));
        }
        let merged = match live {
            Some(live) => live.merge(record),
            None => record.clone(),
        };
        self.witness(&merged.first);
        self.witness(&merged.last);
        if stored.as_ref() == Some(&merged)
            && store_is_a_no_op(txn, collection, scope, &merged, carrying)?
        {
            return Ok((Some(merged), false));
        }
        put_rows(self, txn, collection, scope, &merged, carrying)?;
        Ok((Some(merged), true))
    }

    /// Retention's records step: every [`IDEMPOTENCY_BY_STAMP`] row stamped
    /// below `cutoff`, found under a read transaction, one seek per origin to
    /// that origin's rows below the cutoff (the table is keyed origin-major),
    /// and removed [`RECORD_COLLECT_CHUNK`] at a time, each chunk its own commit
    /// with the writer released between them (ADR-151) and the next chunk read
    /// on from where the last one ended. Each row is re-checked under the
    /// writer ([`Self::remove_expired_records`]). Returns how many records it
    /// removed.
    ///
    /// A record goes once its `last` is below the cutoff, with the first of its
    /// rows at or above `last` met. **A row at the `first` of a record still
    /// inside the window is kept** (F1): it is the stamp the record is vouched
    /// for at when its request's first write is covered, and the coverage rule
    /// reads it. A record this build cannot decode has no `last` to age by: it
    /// goes with the first of its rows to expire, which is at least one window
    /// after its first commit. It was absent to every lookup here all along, so
    /// that costs only a newer build rolled forward to, and a rollback already
    /// reopens that gap. A stop ends the read of the next chunk with its error,
    /// between two chunks.
    pub(crate) fn collect_idempotency_records(&self, cutoff: Hlc) -> Result<usize> {
        use redb::ReadableDatabase;
        use std::ops::Bound::{Excluded, Unbounded};
        let mut collected = 0usize;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let expired: Vec<(Vec<u8>, Vec<u8>)> = {
                let txn = self.db().begin_read()?;
                let rows =
                    open_walk_table(&txn, IDEMPOTENCY_BY_STAMP, self.walk(WalkScope::Background))?;
                let mut expired = Vec::new();
                let mut past = after.clone();
                'origins: loop {
                    let next = match &past {
                        None => rows.range::<&[u8]>(..)?.next().transpose()?,
                        Some(past) => {
                            let mut on =
                                rows.range::<&[u8]>((Excluded(past.as_slice()), Unbounded))?;
                            on.next().transpose()?
                        }
                    };
                    let Some(next) = next else { break };
                    let key = next.0.value().to_vec();
                    drop(next);
                    let Some(at) = decode_row_key(&key) else {
                        // A key of another length names nothing: it goes.
                        expired.push((key.clone(), Vec::new()));
                        past = Some(key);
                        if expired.len() >= RECORD_COLLECT_CHUNK {
                            break;
                        }
                        continue;
                    };
                    let below = row_key_at(at.node, cutoff);
                    let (from, resumed) = match &past {
                        Some(past) => (past.clone(), true),
                        None => (key.clone(), false),
                    };
                    let lower = if resumed {
                        Excluded(from.as_slice())
                    } else {
                        std::ops::Bound::Included(from.as_slice())
                    };
                    for row in rows.range::<&[u8]>((lower, Excluded(below.as_slice())))? {
                        let (key, value) = row?;
                        expired.push((key.value().to_vec(), value.value().to_vec()));
                        if expired.len() >= RECORD_COLLECT_CHUNK {
                            break 'origins;
                        }
                    }
                    past = Some(row_key_at(at.node, Hlc::MAX).to_vec());
                }
                expired
            };
            if expired.is_empty() {
                break;
            }
            let full = expired.len() >= RECORD_COLLECT_CHUNK;
            after = expired.last().map(|(key, _)| key.clone());
            let removed = self.remove_expired_records(&expired, cutoff)?;
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

    /// Retention's share of the change log ([`IDEMPOTENCY_CHANGES`]): its rows
    /// keyed below `cutoff`, [`RECORD_COLLECT_CHUNK`] per commit with the writer
    /// released between them (ADR-151). Looked for first under a read
    /// transaction, one seek, so a pass with nothing to drop takes no writer.
    /// Where the log begins is kept. A base older than what is kept loses
    /// nothing: a record changed only before the cutoff is past its window.
    /// Returns how many rows it dropped.
    pub(crate) fn trim_idempotency_changes(&self, cutoff: Hlc) -> Result<usize> {
        use redb::ReadableDatabase;
        use std::ops::Bound::Excluded;
        let below = codec::oplog_key(&Stamp::new(cutoff, NodeId::from_bytes([0; 16])));
        let bounds = (Excluded(LOG_BEGINS), Excluded(below.as_slice()));
        {
            let txn = self.db().begin_read()?;
            let log = txn.open_table(IDEMPOTENCY_CHANGES)?;
            // not a row: whether any row is below the cutoff, one seek.
            if log.range::<&[u8]>(bounds)?.next().transpose()?.is_none() {
                return Ok(0);
            }
        }
        let mut trimmed = 0usize;
        loop {
            let txn = self.begin_write(WriterHolder::Retention)?;
            let n = {
                let mut log = txn.open_table(IDEMPOTENCY_CHANGES)?;
                let mut keys = Vec::new();
                for row in log.range::<&[u8]>(bounds)?.take(RECORD_COLLECT_CHUNK) {
                    let (key, _) = row?;
                    keys.push(key.value().to_vec());
                }
                for key in &keys {
                    log.remove(key.as_slice())?;
                }
                keys.len()
            };
            if n == 0 {
                txn.abort()?;
                return Ok(trimmed);
            }
            txn.commit()?;
            trimmed += n;
            if n < RECORD_COLLECT_CHUNK {
                return Ok(trimmed);
            }
        }
    }

    /// One chunk of [`Self::collect_idempotency_records`], under the writer: each
    /// row re-read, and removed unless it is the `first` of a record still
    /// inside the window; the record removed with it when the record's `last` is
    /// at or below the row's stamp.
    fn remove_expired_records(&self, rows: &[(Vec<u8>, Vec<u8>)], cutoff: Hlc) -> Result<usize> {
        let txn = self.begin_write(WriterHolder::Retention)?;
        let mut removed = 0usize;
        {
            let mut by_stamp = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
            let mut records = txn.open_table(IDEMPOTENCY)?;
            for (key, _) in rows {
                // Read again under the writer: the row is what stands now.
                let named = match by_stamp.get(key.as_slice())? {
                    Some(value) => parse_row_value(value.value()),
                    None => continue,
                };
                let (Some((collection, scope)), Some(stamp)) = (named, decode_row_key(key)) else {
                    by_stamp.remove(key.as_slice())?;
                    continue;
                };
                let record_key = (collection.0, scope.as_bytes().as_slice());
                let (stale, keep_row) = match records.get(record_key)? {
                    Some(raw) => match IdempotencyRecord::decode(raw.value()) {
                        Ok(Some(record)) => (
                            record.last <= stamp,
                            // F1: the `first` of a record still inside the window.
                            record.first == stamp && record.last.hlc >= cutoff,
                        ),
                        // A record this build cannot read (a newer format, or
                        // damage) has no `last` it can age by, and nothing here
                        // finds its other rows without decoding it. It is aged
                        // by the stamp of this row, the first of its rows to
                        // expire, which is never after its `last`: it does not
                        // outlive the window. Here it is absent to every lookup
                        // and overwritten by every store anyway.
                        Ok(None) | Err(_) => (true, false),
                    },
                    None => (false, false),
                };
                if keep_row {
                    continue;
                }
                by_stamp.remove(key.as_slice())?;
                if stale {
                    records.remove(record_key)?;
                    removed += 1;
                }
            }
        }
        txn.commit()?;
        Ok(removed)
    }

    /// A rewind's share (`kimmyd restore --until`), in its transaction: a
    /// record of a request whose every write the rewind discards (`first` after
    /// `until`) goes, since nothing of the request stands and a retry must apply
    /// it afresh, **with its rows by stamp** ([`remove_rows_naming`]): left
    /// behind, they would be above the rewound servable vector, read as F3
    /// candidates by every frame until retention collected them. One the rewind
    /// cuts part-way (`first` at or before `until`, `last` after it) is of a
    /// request only some of whose writes stand: a filtered write past one chunk,
    /// a bulk insert minted across the millisecond the rewind is to (`--until`
    /// is a millisecond), or a record of any kind two attempts merged into, the
    /// earlier attempt's stamp its `first` and the later one's its `last`:
    ///
    /// - **a bulk insert goes**, as one discarded whole does, with its rows:
    ///   nothing resumes one, and kept, its replay would answer every document
    ///   inserted while only a prefix stands. Gone, a retry is a fresh attempt,
    ///   which meets the first document that stands as a duplicate and writes
    ///   nothing;
    /// - **every other kind becomes `applying`**, its rows kept. An update or
    ///   delete by filter (`Modify`) loses its cursor, so a retry resumes from the
    ///   beginning and the stamp rule writes what the rewind undid and nothing the
    ///   request still holds; its counts are left as they were, an overcount. An
    ///   insert, a replace, a delete by id and a `find_and_modify` replay an
    ///   applying record: an attempt's effect stands at or below `until`, and a
    ///   fresh retry would apply the request on top of it.
    ///
    /// A reopened record is a change, logged as every store of one is
    /// ([`IDEMPOTENCY_CHANGES`]). Returns how many records it removed and
    /// reopened.
    pub(crate) fn rewind_records_in(
        &self,
        txn: &WriteTxn<'_>,
        until: Hlc,
    ) -> Result<(usize, usize)> {
        let mut records = txn.open_table(IDEMPOTENCY)?;
        let mut gone = Vec::new();
        let mut reopened = Vec::new();
        // A stopped node's offline rewind: the whole table, as the rewind reads
        // the whole oplog.
        for row in records.iter()? {
            let (key, value) = row?;
            let (id, scope) = key.value();
            let Ok(Some(record)) = IdempotencyRecord::decode(value.value()) else { continue };
            let cut = record.last.hlc > until;
            let bulk = matches!(record.outcome, RecordOutcome::Bulk { .. });
            if record.first.hlc > until || (cut && bulk) {
                gone.push((id, scope.to_vec()));
            } else if cut {
                let mut open = record;
                open.state = RecordState::Applying;
                if let RecordOutcome::Modify(m) = &mut open.outcome {
                    m.cursor = None;
                }
                reopened.push((id, scope.to_vec(), open.encode()));
            }
        }
        // The lowest `first` of what goes: where its rows begin.
        let mut from: Option<Hlc> = None;
        for (id, scope) in &gone {
            if let Some(old) = records.remove((*id, scope.as_slice()))?
                && let Ok(Some(old)) = IdempotencyRecord::decode(old.value())
            {
                from = Some(from.map_or(old.first.hlc, |from| from.min(old.first.hlc)));
            }
        }
        for (id, scope, bytes) in &reopened {
            records.insert((*id, scope.as_slice()), bytes.as_slice())?;
        }
        drop(records);
        let named: HashSet<(CollectionId, ScopeHash)> = gone
            .iter()
            .filter_map(|(id, scope)| Some((CollectionId(*id), ScopeHash::from_slice(scope)?)))
            .collect();
        if let Some(from) = from {
            remove_rows_naming(txn, from, &named)?;
        }
        for (id, scope, _) in &reopened {
            if let Some(scope) = ScopeHash::from_slice(scope) {
                log_change(self, txn, CollectionId(*id), &scope)?;
            }
        }
        Ok((gone.len(), reopened.len()))
    }

    /// The highest stamp any record carries: the largest of each origin's last
    /// key of [`IDEMPOTENCY_BY_STAMP`] (keyed origin-major), two seeks per
    /// origin with a row. Part of where the clock resumes, so that a record
    /// taken from a sender whose clock is ahead is never resumed below its
    /// `first` after a restart (the witness that folded it in is in memory
    /// only).
    pub(crate) fn records_high_water(db: &redb::Database) -> Result<Hlc> {
        use redb::ReadableDatabase;
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let txn = db.begin_read()?;
        let rows = txn.open_table(IDEMPOTENCY_BY_STAMP)?;
        let mut high = Hlc::ZERO;
        let mut past: Option<Vec<u8>> = None;
        loop {
            // not a row: the next origin with a row, one seek.
            let next = match &past {
                None => rows.range::<&[u8]>(..)?.next(),
                Some(past) => rows.range::<&[u8]>((Excluded(past.as_slice()), Unbounded))?.next(),
            };
            let Some(next) = next.transpose()? else { return Ok(high) };
            let key = next.0.value().to_vec();
            drop(next);
            let Some(at) = decode_row_key(&key) else {
                past = Some(key);
                continue;
            };
            let (from, to) = (row_key_at(at.node, Hlc::ZERO), row_key_at(at.node, Hlc::MAX));
            // not a row: the origin's last key, one seek.
            let mut last =
                rows.range::<&[u8]>((Included(from.as_slice()), Included(to.as_slice())))?;
            if let Some(last) = last.next_back().transpose()?
                && let Some(stamp) = decode_row_key(last.0.value())
            {
                high = high.max(stamp.hlc);
            }
            past = Some(to.to_vec());
        }
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

#[path = "idempotency_replay.rs"]
mod replay;
pub use replay::{ReplayPageRecords, ReplayRecords};
