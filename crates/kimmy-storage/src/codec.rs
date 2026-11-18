//! On-disk binary formats for the hot-path records.
//!
//! These are hand-rolled rather than derived. A derive-based codec ties the
//! on-disk layout to a dependency's internal versioning, and the oplog format
//! is also the replication wire format — two reasons the bytes need to be
//! something we specify explicitly and can evolve deliberately. Every record
//! leads with a format version so a future change is detectable rather than
//! silently misparsed.
//!
//! Cold metadata (collection definitions) uses JSON instead; see
//! [`crate::meta`]. It is read rarely and being human-inspectable during
//! debugging is worth more than the bytes.

use kimmy_core::{
    DocId, DocRecord, HLC_ENCODED_LEN, Hlc, OpKind, OplogEntry, Stamp, ids::CollectionId,
};

use crate::error::{Result, StorageError};

/// Bumped only for incompatible layout changes.
pub const FORMAT_VERSION: u8 = 1;

/// A stamp's stored width: `hlc ‖ node ‖ epoch`, 44 bytes ([`Stamp::KEY_LEN`]).
pub(crate) const STAMP_LEN: usize = Stamp::KEY_LEN;
/// Sentinel length meaning "this optional field is absent".
const NONE_LEN: u32 = u32::MAX;

fn corrupt(what: &str) -> StorageError {
    StorageError::Corrupt(what.to_string())
}

// ---------------------------------------------------------------------------
// Primitives
// ---------------------------------------------------------------------------

fn put_stamp(stamp: &Stamp, out: &mut Vec<u8>) {
    out.extend_from_slice(&stamp.to_key_bytes());
}

fn take_stamp(input: &mut &[u8]) -> Result<Stamp> {
    let bytes = take(input, STAMP_LEN)?;
    let key: [u8; STAMP_LEN] = bytes.try_into().expect("STAMP_LEN bytes");
    Ok(Stamp::from_key_bytes(key))
}

fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if input.len() < n {
        return Err(corrupt("record ended early"));
    }
    let (head, tail) = input.split_at(n);
    *input = tail;
    Ok(head)
}

fn take_u32(input: &mut &[u8]) -> Result<u32> {
    let bytes = take(input, 4)?;
    Ok(u32::from_be_bytes(bytes.try_into().expect("4 bytes")))
}

fn put_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn take_bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = take_u32(input)? as usize;
    take(input, len)
}

fn put_opt_bytes(bytes: Option<&[u8]>, out: &mut Vec<u8>) {
    match bytes {
        Some(b) => put_bytes(b, out),
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

fn check_version(input: &mut &[u8]) -> Result<()> {
    let version = take(input, 1)?[0];
    if version != FORMAT_VERSION {
        return Err(StorageError::UnsupportedFormat { found: version, expected: FORMAT_VERSION });
    }
    Ok(())
}

/// The format version of a record that carries the merge state of a `field` collection's
/// document: `version ‖ stamp ‖ deleted ‖ varint meta_len ‖ meta ‖ body` for a document
/// record, and the same flag for an oplog entry. A `document` collection never writes it.
pub const META_FORMAT_VERSION: u8 = 2;

/// The version byte of a document record or an oplog entry: 1, or 2 when it carries `meta`.
/// Returns whether `meta` follows the header.
fn check_version_meta(input: &mut &[u8]) -> Result<bool> {
    let version = take(input, 1)?[0];
    match version {
        FORMAT_VERSION => Ok(false),
        META_FORMAT_VERSION => Ok(true),
        found => Err(StorageError::UnsupportedFormat { found, expected: META_FORMAT_VERSION }),
    }
}

fn put_varint(mut n: usize, out: &mut Vec<u8>) {
    while n >= 0x80 {
        out.push((n as u8 & 0x7f) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn take_varint(input: &mut &[u8]) -> Result<usize> {
    let mut n = 0usize;
    for shift in (0..35).step_by(7) {
        let b = take(input, 1)?[0];
        n |= usize::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(n);
        }
    }
    Err(corrupt("a length is longer than five bytes"))
}

// ---------------------------------------------------------------------------
// DocId
// ---------------------------------------------------------------------------

// Unlike an index key, a document id must be *decodable* — replication has to
// reconstruct it — so this is a separate encoding from `keyenc`, which is
// one-way by design.
const DOC_ID_OBJECT_ID: u8 = 1;
const DOC_ID_STRING: u8 = 2;
const DOC_ID_INT64: u8 = 3;
const DOC_ID_BINARY: u8 = 4;
// Tag 5 was `Uuid` and is retired; it decodes as an unknown tag.
/// A `Decimal128` id: its 19 canonical bytes (`DocId`'s `Decimal128Id`).
const DOC_ID_DECIMAL128: u8 = 6;

pub fn encode_doc_id(id: &DocId, out: &mut Vec<u8>) {
    match id {
        DocId::ObjectId(oid) => {
            out.push(DOC_ID_OBJECT_ID);
            out.extend_from_slice(&oid.bytes());
        }
        DocId::String(s) => {
            out.push(DOC_ID_STRING);
            put_bytes(s.as_bytes(), out);
        }
        DocId::Int64(v) => {
            out.push(DOC_ID_INT64);
            out.extend_from_slice(&v.to_be_bytes());
        }
        DocId::Binary { subtype, bytes } => {
            out.push(DOC_ID_BINARY);
            out.push(*subtype);
            put_bytes(bytes, out);
        }
        DocId::Decimal128(d) => {
            out.push(DOC_ID_DECIMAL128);
            out.extend_from_slice(&d.to_bytes());
        }
    }
}

pub fn decode_doc_id(input: &mut &[u8]) -> Result<DocId> {
    let tag = take(input, 1)?[0];
    Ok(match tag {
        DOC_ID_OBJECT_ID => {
            let bytes: [u8; 12] = take(input, 12)?.try_into().expect("12 bytes");
            DocId::ObjectId(bson::oid::ObjectId::from_bytes(bytes))
        }
        DOC_ID_STRING => {
            let bytes = take_bytes(input)?;
            DocId::String(
                String::from_utf8(bytes.to_vec())
                    .map_err(|_| corrupt("document id is not valid utf-8"))?,
            )
        }
        DOC_ID_INT64 => {
            let bytes: [u8; 8] = take(input, 8)?.try_into().expect("8 bytes");
            DocId::Int64(i64::from_be_bytes(bytes))
        }
        DOC_ID_BINARY => {
            let subtype = take(input, 1)?[0];
            DocId::Binary { subtype, bytes: take_bytes(input)?.to_vec() }
        }
        DOC_ID_DECIMAL128 => {
            let bytes = take(input, kimmy_core::DECIMAL128_ID_LEN)?;
            DocId::Decimal128(
                kimmy_core::Decimal128Id::from_bytes(bytes)
                    .ok_or_else(|| corrupt("document id is not a canonical Decimal128"))?,
            )
        }
        other => return Err(corrupt(&format!("unknown document id tag {other}"))),
    })
}

// ---------------------------------------------------------------------------
// DocRecord
// ---------------------------------------------------------------------------

pub fn encode_doc_record(record: &DocRecord) -> Vec<u8> {
    let meta_len = record.meta.as_ref().map_or(0, Vec::len);
    let mut out = Vec::with_capacity(1 + STAMP_LEN + 1 + 5 + meta_len + record.body.len());
    out.push(if record.meta.is_some() { META_FORMAT_VERSION } else { FORMAT_VERSION });
    put_stamp(&record.stamp, &mut out);
    out.push(u8::from(record.deleted));
    if let Some(meta) = &record.meta {
        put_varint(meta.len(), &mut out);
        out.extend_from_slice(meta);
    }
    out.extend_from_slice(&record.body);
    out
}

/// The record without its merge state: the body-only reader, for every site that reads and
/// does not write the record back. `meta` is skipped, not copied, and is `None` in the result.
pub fn decode_doc_record(input: &[u8]) -> Result<DocRecord> {
    decode_doc_record_inner(input, false)
}

/// The record with its merge state, for a site that reads a record and writes it back (a
/// body-only read there would drop the state without a sound).
pub fn decode_doc_record_with_meta(input: &[u8]) -> Result<DocRecord> {
    decode_doc_record_inner(input, true)
}

fn decode_doc_record_inner(mut input: &[u8], keep_meta: bool) -> Result<DocRecord> {
    let has_meta = check_version_meta(&mut input)?;
    let stamp = take_stamp(&mut input)?;
    let deleted = take(&mut input, 1)?[0] != 0;
    let mut meta = None;
    if has_meta {
        let len = take_varint(&mut input)?;
        let bytes = take(&mut input, len)?;
        if keep_meta {
            meta = Some(bytes.to_vec());
        }
    }
    // The body is the remainder; no length prefix needed since it is last.
    Ok(DocRecord { stamp, deleted, body: input.to_vec(), meta })
}

/// The hash of the merged state an encoded document record carries, read from the stored bytes
/// without copying or decoding the state: `None` for a version 1 record (no state) or bytes
/// that are not a record.
pub fn doc_record_state_hash(input: &[u8]) -> Option<[u8; 16]> {
    kimmy_core::merge_state::MergeState::compact_hash_of(doc_record_meta(input)?).ok()
}

/// The merged-state bytes of an encoded record, borrowed: `None` for a version 1 record.
fn doc_record_meta(mut input: &[u8]) -> Option<&[u8]> {
    if !check_version_meta(&mut input).ok()? {
        return None;
    }
    take(&mut input, STAMP_LEN + 1).ok()?;
    let len = take_varint(&mut input).ok()?;
    take(&mut input, len).ok()
}

/// What a read of the record answers besides the document: its version identity and, for each
/// write in `asked`, whether the record has seen it. Under `field` (a record with a merged state)
/// a write is seen when the state's version vector holds its origin at or above its HLC; under
/// `document` when the stored stamp is at or above it. The state is decoded only when something
/// is asked.
pub fn doc_record_seen(
    record: &DocRecord,
    raw: &[u8],
    asked: &[Stamp],
) -> Result<crate::docs::Seen> {
    let version = doc_record_version(record, raw);
    let covers = match (asked.is_empty(), doc_record_meta(raw)) {
        (true, _) => Vec::new(),
        (false, None) => asked.iter().map(|s| record.stamp >= *s).collect(),
        (false, Some(meta)) => {
            let state = kimmy_core::merge_state::MergeState::decode_compact(meta)
                .map_err(|e| corrupt(&format!("the merge state of a record: {e:?}")))?;
            asked
                .iter()
                .map(|s| {
                    let writer = kimmy_core::merge_state::WriterId { node: s.node, epoch: s.epoch };
                    state.vv.get(&writer).is_some_and(|seen| *seen >= s.hlc)
                })
                .collect()
        }
    };
    Ok(crate::docs::Seen { version, covers })
}

/// The version identity of a record read from `raw`: its stamp, and the hash of the merged state
/// its bytes carry when they carry one (an etag), else the stamp alone.
pub fn doc_record_version(record: &DocRecord, raw: &[u8]) -> kimmy_core::DocVersion {
    match doc_record_state_hash(raw) {
        Some(hash) => kimmy_core::DocVersion::Etag(record.stamp, hash),
        None => kimmy_core::DocVersion::Stamp(record.stamp),
    }
}

/// Whether an encoded document record is live, read from its fixed-width header
/// without touching the body.
///
/// For a walk that only counts: every record is judged on its first
/// `1 + STAMP_LEN + 1` bytes, and nothing is copied or decoded. The bytes read
/// from disk are the same — redb keeps a value in the leaf page that holds its
/// key — but the copy [`decode_doc_record`] makes of every body, and the BSON
/// parse a document read adds to it, are not (ADR-133's addendum).
pub fn doc_record_is_live(mut input: &[u8]) -> Result<bool> {
    check_version_meta(&mut input)?;
    take(&mut input, STAMP_LEN)?;
    Ok(take(&mut input, 1)?[0] == 0)
}

/// The stamp of an encoded document record and whether it is live, read from
/// its fixed-width header with no copy of the body.
///
/// For a walk that asks only whether any record was written after a point, as
/// [`doc_record_is_live`] is for one that only counts.
pub fn doc_record_header(mut input: &[u8]) -> Result<(Stamp, bool)> {
    check_version_meta(&mut input)?;
    let stamp = take_stamp(&mut input)?;
    Ok((stamp, take(&mut input, 1)?[0] == 0))
}

/// What the digest folds of an encoded record: its newest stamp, whether it is live and, for a
/// record that carries a merged state (a `field` collection's), the hash of that state and the
/// version vector of the state. A record without a merged state is its stamp alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordView {
    pub stamp: Stamp,
    pub live: bool,
    /// The hash of the merged state's compact bytes; `None` for a `document` record.
    pub state: Option<[u8; 16]>,
    /// Every origin the merged state has seen, with its newest write: what the record vouches for
    /// beyond the origin of its own newest stamp. Empty for a `document` record.
    pub origins: Vec<(kimmy_core::Origin, Hlc)>,
}

impl RecordView {
    /// A record of a `document` collection: the stamp and liveness, no state.
    pub fn plain(stamp: Stamp, live: bool) -> Self {
        Self { stamp, live, state: None, origins: Vec::new() }
    }
}

/// [`RecordView`] of an encoded record. A version 1 record is read from its header alone, as
/// [`doc_record_header`] does; a version 2 record's merged state is decoded.
pub fn doc_record_view(input: &[u8]) -> Result<RecordView> {
    let (stamp, live) = doc_record_header(input)?;
    let Some(meta) = doc_record_meta(input) else {
        return Ok(RecordView::plain(stamp, live));
    };
    let state = kimmy_core::merge_state::MergeState::decode_compact(meta)
        .map_err(|e| StorageError::Corrupt(format!("the merge state of a record: {e:?}")))?;
    let hash = kimmy_core::merge_state::MergeState::compact_hash_of(meta)
        .map_err(|e| StorageError::Corrupt(format!("the merge state of a record: {e:?}")))?;
    let origins = state
        .vv
        .iter()
        .map(|(w, h)| (kimmy_core::Origin { node: w.node, epoch: w.epoch }, *h))
        .collect();
    Ok(RecordView { stamp, live, state: Some(hash), origins })
}

// ---------------------------------------------------------------------------
// OplogEntry
// ---------------------------------------------------------------------------

fn op_kind_tag(kind: OpKind) -> u8 {
    match kind {
        OpKind::Insert => 1,
        OpKind::Update => 2,
        OpKind::Replace => 3,
        OpKind::Delete => 4,
        OpKind::Collection => 5,
        OpKind::UniqueViolation => 6,
        OpKind::CreateCollection => 7,
        OpKind::DropCollection => 8,
        OpKind::CreateIndex => 9,
        OpKind::DropIndex => 10,
        OpKind::ConfigureVectors => 11,
        OpKind::ConfigureValidator => 12,
        OpKind::UnkeyedDocument => 13,
    }
}

fn op_kind_from_tag(tag: u8) -> Result<OpKind> {
    Ok(match tag {
        1 => OpKind::Insert,
        2 => OpKind::Update,
        3 => OpKind::Replace,
        4 => OpKind::Delete,
        5 => OpKind::Collection,
        6 => OpKind::UniqueViolation,
        7 => OpKind::CreateCollection,
        8 => OpKind::DropCollection,
        9 => OpKind::CreateIndex,
        10 => OpKind::DropIndex,
        11 => OpKind::ConfigureVectors,
        12 => OpKind::ConfigureValidator,
        13 => OpKind::UnkeyedDocument,
        other => return Err(corrupt(&format!("unknown op kind {other}"))),
    })
}

/// An oplog entry as the `OPLOG` table stores it: `FORMAT_VERSION ‖ kind ‖ collection ‖ …`.
///
/// **The stamp is not in the value.** It is the row's key ([`oplog_key`]), and a copy in the
/// value was 44 bytes of the same thing; [`decode_oplog_entry`] takes the key and puts the
/// stamp back. The entry on the wire (serde) keeps its stamp.
pub fn encode_oplog_entry(entry: &OplogEntry) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(if entry.meta.is_some() { META_FORMAT_VERSION } else { FORMAT_VERSION });
    out.push(op_kind_tag(entry.kind));
    out.extend_from_slice(&entry.collection.0.to_be_bytes());

    match &entry.doc_id {
        Some(id) => {
            let mut buf = Vec::with_capacity(20);
            encode_doc_id(id, &mut buf);
            put_bytes(&buf, &mut out);
        }
        None => out.extend_from_slice(&NONE_LEN.to_be_bytes()),
    }

    put_opt_bytes(entry.body.as_deref(), &mut out);
    // version 2 only: the merge state follows the body
    if let Some(meta) = &entry.meta {
        put_bytes(meta, &mut out);
    }
    out
}

/// Whether an encoded oplog entry carries a merge state: the entry of a `field` collection does, and one of a
/// `document` collection does not, which its version byte says before anything else is read.
pub fn oplog_entry_carries_state(input: &[u8]) -> bool {
    input.first() == Some(&META_FORMAT_VERSION)
}

/// The kind and collection of an encoded oplog entry, read from its fixed-width
/// header without touching the document id or the body.
///
/// For a walk that wants one kind of entry for one collection: every other
/// entry is judged on these nine bytes, and only a match pays for
/// [`decode_oplog_entry`] and the copy of its body.
pub fn decode_oplog_kind_and_collection(mut input: &[u8]) -> Result<(OpKind, CollectionId)> {
    check_version_meta(&mut input)?;
    let kind = op_kind_from_tag(take(&mut input, 1)?[0])?;
    let collection =
        CollectionId(u64::from_be_bytes(take(&mut input, 8)?.try_into().expect("8 bytes")));
    Ok((kind, collection))
}

/// The entry stored under `key` with value `input`: the stamp is the key's
/// ([`decode_oplog_key`]), the rest is the value ([`encode_oplog_entry`]).
pub fn decode_oplog_entry(key: &[u8], mut input: &[u8]) -> Result<OplogEntry> {
    let stamp = decode_oplog_key(key)?;
    let has_meta = check_version_meta(&mut input)?;
    let kind = op_kind_from_tag(take(&mut input, 1)?[0])?;
    let collection =
        CollectionId(u64::from_be_bytes(take(&mut input, 8)?.try_into().expect("8 bytes")));

    let doc_id = match take_opt_bytes(&mut input)? {
        Some(mut bytes) => Some(decode_doc_id(&mut bytes)?),
        None => None,
    };
    let body = take_opt_bytes(&mut input)?.map(<[u8]>::to_vec);
    let meta = if has_meta { Some(take_bytes(&mut input)?.to_vec()) } else { None };

    Ok(OplogEntry { stamp, kind, collection, doc_id, body, meta })
}

/// An entry encoded for the `OPLOG` table and read back under its own key: the round trip a
/// test wants.
#[cfg(test)]
pub(crate) fn oplog_round_trip(entry: &OplogEntry) -> Result<OplogEntry> {
    decode_oplog_entry(&oplog_key(&entry.stamp), &encode_oplog_entry(entry))
}

/// The oplog's primary key: `(hlc, node, epoch)` encoded so that `memcmp` yields the
/// total write order. Scanning the oplog forwards is therefore just a redb
/// range scan.
pub fn oplog_key(stamp: &Stamp) -> [u8; STAMP_LEN] {
    stamp.to_key_bytes()
}

/// The lowest possible oplog key at or after `hlc`.
pub fn oplog_key_lower_bound(hlc: Hlc) -> [u8; STAMP_LEN] {
    let mut key = [0u8; STAMP_LEN];
    key[..HLC_ENCODED_LEN].copy_from_slice(&hlc.to_bytes());
    key
}

pub fn decode_oplog_key(key: &[u8]) -> Result<Stamp> {
    let mut input = key;
    take_stamp(&mut input)
}

#[cfg(test)]
mod tests {
    use kimmy_core::ids::CollectionId;
    use kimmy_core::{Epoch, NodeId};

    use super::*;

    fn stamp(ms: u64, n: u8) -> Stamp {
        Stamp::new(Hlc::new(ms, 3), NodeId::from_bytes([n; 16]), Epoch::ZERO)
    }

    fn meta_of(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn a_record_without_meta_is_the_version_1_bytes_it_always_was() {
        let r = DocRecord::live(stamp(100, 1), b"body".to_vec());
        let bytes = encode_doc_record(&r);
        let mut want = vec![FORMAT_VERSION];
        put_stamp(&r.stamp, &mut want);
        want.push(0);
        want.extend_from_slice(b"body");
        assert_eq!(bytes, want, "a document record's bytes do not change");
        assert_eq!(bytes.len(), 1 + STAMP_LEN + 1 + 4);
        let t = encode_doc_record(&DocRecord::tombstone(stamp(5, 2)));
        assert_eq!((t[0], t[1 + STAMP_LEN], t.len()), (1, 1, 1 + STAMP_LEN + 1));
    }

    #[test]
    fn a_record_with_meta_round_trips_live_tombstone_empty_and_large() {
        for meta in [meta_of(0), meta_of(1), meta_of(127), meta_of(128), meta_of(65_536)] {
            for base in [
                DocRecord::live(stamp(100, 1), b"body-bytes".to_vec()),
                DocRecord::live(stamp(0, 0), Vec::new()),
                DocRecord::tombstone(stamp(200, 2)),
            ] {
                let record = base.with_meta(meta.clone());
                let bytes = encode_doc_record(&record);
                assert_eq!(bytes[0], META_FORMAT_VERSION);
                assert_eq!(decode_doc_record_with_meta(&bytes).unwrap(), record);
                // the body-only reader: the same stamp, flag and body, and no meta
                let body_only = decode_doc_record(&bytes).unwrap();
                assert_eq!(body_only.meta, None);
                assert_eq!(
                    (body_only.stamp, body_only.deleted, &body_only.body),
                    (record.stamp, record.deleted, &record.body)
                );
                // the headers read the same two facts from either version
                assert_eq!(doc_record_header(&bytes).unwrap(), (record.stamp, record.is_live()));
                assert_eq!(doc_record_is_live(&bytes).unwrap(), record.is_live());
            }
        }
    }

    #[test]
    fn the_with_meta_reader_reads_a_version_1_record_as_no_meta() {
        let r = DocRecord::live(stamp(9, 9), b"x".to_vec());
        assert_eq!(decode_doc_record_with_meta(&encode_doc_record(&r)).unwrap(), r);
    }

    #[test]
    fn a_meta_length_past_the_end_is_corrupt_and_version_3_is_unsupported() {
        let good =
            encode_doc_record(&DocRecord::live(stamp(1, 1), Vec::new()).with_meta(meta_of(10)));
        // cut inside the meta
        let cut = &good[..good.len() - 3];
        for f in [decode_doc_record, decode_doc_record_with_meta] {
            assert!(matches!(f(cut), Err(StorageError::Corrupt(_))), "{:?}", f(cut));
        }
        // a length of 2^31 and a length that never ends
        let at = 1 + STAMP_LEN + 1;
        let mut big = good[..at].to_vec();
        big.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x08]);
        assert!(decode_doc_record(&big).is_err());
        let mut endless = good[..at].to_vec();
        endless.extend_from_slice(&[0xff; 8]);
        assert!(decode_doc_record(&endless).is_err());
        let mut v3 = good.clone();
        v3[0] = 3;
        for r in [decode_doc_record(&v3), decode_doc_record_with_meta(&v3)] {
            assert!(matches!(r, Err(StorageError::UnsupportedFormat { found: 3, .. })));
        }
        assert!(doc_record_header(&v3).is_err() && doc_record_is_live(&v3).is_err());
    }

    #[test]
    fn doc_record_round_trips() {
        for record in [
            DocRecord::live(stamp(100, 1), b"body-bytes".to_vec()),
            DocRecord::tombstone(stamp(200, 2)),
            DocRecord::live(stamp(0, 0), Vec::new()),
        ] {
            let decoded = decode_doc_record(&encode_doc_record(&record)).unwrap();
            assert_eq!(decoded, record);
        }
    }

    #[test]
    fn doc_id_round_trips_every_variant() {
        let ids = [
            DocId::ObjectId(bson::oid::ObjectId::new()),
            DocId::String("hello".into()),
            DocId::String(String::new()),
            DocId::Int64(i64::MIN),
            DocId::Int64(0),
            DocId::Binary { subtype: 0, bytes: vec![0, 1, 2, 255] },
            DocId::Binary { subtype: 0, bytes: Vec::new() },
            DocId::Binary { subtype: 4, bytes: vec![7; 16] },
            DocId::Binary { subtype: 0x80, bytes: vec![1] },
            DocId::Binary { subtype: 0xFF, bytes: Vec::new() },
            DocId::try_from_bson(&bson::Bson::Decimal128("-1.5".parse().unwrap())).unwrap(),
            DocId::try_from_bson(&bson::Bson::Decimal128("1E+6144".parse().unwrap())).unwrap(),
        ];
        assert!(
            ids.iter().filter(|id| matches!(id, DocId::Decimal128(_))).count() == 2,
            "the fixture must carry decimal ids"
        );
        for id in ids {
            let mut buf = Vec::new();
            encode_doc_id(&id, &mut buf);
            let mut slice = buf.as_slice();
            assert_eq!(decode_doc_id(&mut slice).unwrap(), id);
            assert!(slice.is_empty(), "decoder left trailing bytes for {id:?}");
        }
    }

    #[test]
    fn binary_doc_id_keeps_its_subtype_and_tag_5_is_refused() {
        let mut buf = Vec::new();
        encode_doc_id(&DocId::Binary { subtype: 4, bytes: vec![9, 8] }, &mut buf);
        assert_eq!(buf[0], 4);
        assert_eq!(buf[1], 4);
        let mut slice = buf.as_slice();
        assert_eq!(
            decode_doc_id(&mut slice).unwrap(),
            DocId::Binary { subtype: 4, bytes: vec![9, 8] }
        );
        let mut other = Vec::new();
        encode_doc_id(&DocId::Binary { subtype: 0, bytes: vec![9, 8] }, &mut other);
        assert_ne!(buf, other);

        let mut retired = vec![5u8];
        retired.extend_from_slice(&[0; 16]);
        assert!(decode_doc_id(&mut retired.as_slice()).is_err());
    }

    #[test]
    fn an_oplog_entry_without_meta_is_the_version_1_bytes_and_one_with_meta_round_trips() {
        let base = OplogEntry {
            stamp: stamp(100, 1),
            kind: OpKind::Insert,
            collection: CollectionId(7),
            doc_id: Some(DocId::String("k".into())),
            body: Some(b"doc".to_vec()),
            meta: None,
        };
        let v1 = encode_oplog_entry(&base);
        assert_eq!(v1[0], FORMAT_VERSION);
        // version 1: kind, collection, id (length-prefixed), body (length-prefixed), nothing after
        let id_len = 1 + 4 + 1; // tag, length, "k"
        assert_eq!(v1.len(), 1 + 1 + 8 + 4 + id_len + 4 + 3);
        for meta in [Vec::new(), vec![1, 2, 3], vec![9; 70_000]] {
            let with = OplogEntry { meta: Some(meta), ..base.clone() };
            let bytes = encode_oplog_entry(&with);
            assert_eq!(bytes[0], META_FORMAT_VERSION);
            assert_eq!(
                &bytes[1..v1.len()],
                &v1[1..],
                "the rest of the entry is the version 1 layout"
            );
            assert_eq!(oplog_round_trip(&with).unwrap(), with);
            assert_eq!(
                decode_oplog_kind_and_collection(&bytes).unwrap(),
                (OpKind::Insert, CollectionId(7))
            );
            let mut cut = bytes.clone();
            cut.truncate(bytes.len() - 1);
            assert!(decode_oplog_entry(&oplog_key(&with.stamp), &cut).is_err());
        }
        assert_eq!(oplog_round_trip(&base).unwrap(), base);
    }

    #[test]
    fn the_wire_form_of_an_entry_without_meta_has_no_meta_key_and_one_with_meta_is_binary() {
        let base = OplogEntry {
            stamp: stamp(100, 1),
            kind: OpKind::Insert,
            collection: CollectionId(7),
            doc_id: Some(DocId::String("k".into())),
            body: Some(b"doc".to_vec()),
            meta: None,
        };
        let plain = bson::serialize_to_vec(&base).unwrap();
        assert!(!plain.windows(5).any(|w| w == b"meta\0"), "no meta key without meta");
        let with = OplogEntry { meta: Some(vec![7; 1000]), ..base.clone() };
        let wire = bson::serialize_to_vec(&with).unwrap();
        assert!(wire.len() < plain.len() + 1100, "binary, not an int32 per byte: {}", wire.len());
        let back: OplogEntry = bson::deserialize_from_slice(&wire).unwrap();
        assert_eq!(back, with);
        let back: OplogEntry = bson::deserialize_from_slice(&plain).unwrap();
        assert_eq!(back, base);
    }

    #[test]
    fn oplog_entry_round_trips() {
        let entries = [
            OplogEntry {
                stamp: stamp(100, 1),
                kind: OpKind::Insert,
                collection: CollectionId(7),
                doc_id: Some(DocId::String("k".into())),
                body: Some(b"doc".to_vec()),
                meta: None,
            },
            // A delete carries no body.
            OplogEntry {
                stamp: stamp(101, 1),
                kind: OpKind::Delete,
                collection: CollectionId(7),
                doc_id: Some(DocId::Int64(42)),
                body: None,
                meta: None,
            },
            // A collection-level op carries neither id nor body.
            OplogEntry {
                stamp: stamp(102, 1),
                kind: OpKind::Collection,
                collection: CollectionId(u64::MAX),
                doc_id: None,
                body: None,
                meta: None,
            },
        ];
        for entry in entries {
            let decoded = oplog_round_trip(&entry).unwrap();
            assert_eq!(decoded, entry);
        }
    }

    #[test]
    fn an_empty_body_is_distinct_from_no_body() {
        // `Some(vec![])` and `None` must not collapse, or a replicated replace
        // with an empty document would be applied as a delete.
        let base = OplogEntry {
            stamp: stamp(1, 1),
            kind: OpKind::Replace,
            collection: CollectionId(1),
            doc_id: Some(DocId::Int64(1)),
            body: Some(Vec::new()),
            meta: None,
        };
        let none = OplogEntry { body: None, ..base.clone() };
        assert_ne!(encode_oplog_entry(&base), encode_oplog_entry(&none));
        assert_eq!(oplog_round_trip(&base).unwrap().body, Some(Vec::new()));
        assert_eq!(oplog_round_trip(&none).unwrap().body, None);
    }

    #[test]
    fn oplog_keys_sort_in_write_order() {
        let mut keys =
            [oplog_key(&stamp(200, 1)), oplog_key(&stamp(100, 9)), oplog_key(&stamp(100, 1))];
        keys.sort();
        assert_eq!(keys[0], oplog_key(&stamp(100, 1)));
        assert_eq!(keys[1], oplog_key(&stamp(100, 9)));
        assert_eq!(keys[2], oplog_key(&stamp(200, 1)));
    }

    #[test]
    fn oplog_key_round_trips() {
        let s = stamp(12345, 7);
        assert_eq!(decode_oplog_key(&oplog_key(&s)).unwrap(), s);
    }

    #[test]
    fn lower_bound_sorts_at_or_before_every_key_with_that_hlc() {
        let hlc = Hlc::new(500, 2);
        let bound = oplog_key_lower_bound(hlc);
        // Node id 0 is the smallest possible, so the bound must not exceed it.
        assert!(bound <= oplog_key(&Stamp::new(hlc, NodeId::from_bytes([0; 16]), Epoch::ZERO)));
        assert!(bound < oplog_key(&Stamp::new(hlc, NodeId::from_bytes([255; 16]), Epoch::ZERO)));
    }

    #[test]
    fn a_future_format_version_is_rejected() {
        let mut bytes = encode_doc_record(&DocRecord::tombstone(stamp(1, 1)));
        bytes[0] = META_FORMAT_VERSION + 1;
        assert!(matches!(decode_doc_record(&bytes), Err(StorageError::UnsupportedFormat { .. })));
    }

    #[test]
    fn truncated_records_error_rather_than_panic() {
        let entry = OplogEntry {
            stamp: stamp(1, 1),
            kind: OpKind::Insert,
            collection: CollectionId(1),
            doc_id: Some(DocId::String("abc".into())),
            body: Some(b"xyz".to_vec()),
            meta: None,
        };
        let key = oplog_key(&entry.stamp);
        let full = encode_oplog_entry(&entry);
        for cut in 1..full.len() {
            // Every truncation must be a clean error; a panic here would take
            // down the server on a single corrupt page.
            assert!(decode_oplog_entry(&key, &full[..cut]).is_err(), "no error at cut {cut}");
        }
        // and so is a key that is not a stamp's
        assert!(decode_oplog_entry(&key[..43], &full).is_err());
    }
}
