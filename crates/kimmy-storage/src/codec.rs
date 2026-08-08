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
    DocId, DocRecord, HLC_ENCODED_LEN, Hlc, NodeId, OpKind, OplogEntry, Stamp, ids::CollectionId,
};

use crate::error::{Result, StorageError};

/// Bumped only for incompatible layout changes.
pub const FORMAT_VERSION: u8 = 1;

const STAMP_LEN: usize = HLC_ENCODED_LEN + 16;
/// Sentinel length meaning "this optional field is absent".
const NONE_LEN: u32 = u32::MAX;

fn corrupt(what: &str) -> StorageError {
    StorageError::Corrupt(what.to_string())
}

// ---------------------------------------------------------------------------
// Primitives
// ---------------------------------------------------------------------------

fn put_stamp(stamp: &Stamp, out: &mut Vec<u8>) {
    out.extend_from_slice(&stamp.hlc.to_bytes());
    out.extend_from_slice(&stamp.node.to_bytes());
}

fn take_stamp(input: &mut &[u8]) -> Result<Stamp> {
    let bytes = take(input, STAMP_LEN)?;
    let mut hlc = [0u8; HLC_ENCODED_LEN];
    hlc.copy_from_slice(&bytes[..HLC_ENCODED_LEN]);
    let mut node = [0u8; 16];
    node.copy_from_slice(&bytes[HLC_ENCODED_LEN..]);
    Ok(Stamp::new(Hlc::from_bytes(hlc), NodeId::from_bytes(node)))
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
const DOC_ID_UUID: u8 = 5;

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
        DocId::Binary(b) => {
            out.push(DOC_ID_BINARY);
            put_bytes(b, out);
        }
        DocId::Uuid(u) => {
            out.push(DOC_ID_UUID);
            out.extend_from_slice(u.as_bytes());
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
        DOC_ID_BINARY => DocId::Binary(take_bytes(input)?.to_vec()),
        DOC_ID_UUID => {
            let bytes: [u8; 16] = take(input, 16)?.try_into().expect("16 bytes");
            DocId::Uuid(uuid::Uuid::from_bytes(bytes))
        }
        other => return Err(corrupt(&format!("unknown document id tag {other}"))),
    })
}

// ---------------------------------------------------------------------------
// DocRecord
// ---------------------------------------------------------------------------

pub fn encode_doc_record(record: &DocRecord) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + STAMP_LEN + 1 + record.body.len());
    out.push(FORMAT_VERSION);
    put_stamp(&record.stamp, &mut out);
    out.push(u8::from(record.deleted));
    out.extend_from_slice(&record.body);
    out
}

pub fn decode_doc_record(mut input: &[u8]) -> Result<DocRecord> {
    check_version(&mut input)?;
    let stamp = take_stamp(&mut input)?;
    let deleted = take(&mut input, 1)?[0] != 0;
    // The body is the remainder; no length prefix needed since it is last.
    Ok(DocRecord { stamp, deleted, body: input.to_vec() })
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
        other => return Err(corrupt(&format!("unknown op kind {other}"))),
    })
}

pub fn encode_oplog_entry(entry: &OplogEntry) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(FORMAT_VERSION);
    put_stamp(&entry.stamp, &mut out);
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
    out
}

pub fn decode_oplog_entry(mut input: &[u8]) -> Result<OplogEntry> {
    check_version(&mut input)?;
    let stamp = take_stamp(&mut input)?;
    let kind = op_kind_from_tag(take(&mut input, 1)?[0])?;
    let collection =
        CollectionId(u64::from_be_bytes(take(&mut input, 8)?.try_into().expect("8 bytes")));

    let doc_id = match take_opt_bytes(&mut input)? {
        Some(mut bytes) => Some(decode_doc_id(&mut bytes)?),
        None => None,
    };
    let body = take_opt_bytes(&mut input)?.map(<[u8]>::to_vec);

    Ok(OplogEntry { stamp, kind, collection, doc_id, body })
}

/// The oplog's primary key: `(hlc, node)` encoded so that `memcmp` yields the
/// total write order. Scanning the oplog forwards is therefore just a redb
/// range scan.
pub fn oplog_key(stamp: &Stamp) -> [u8; STAMP_LEN] {
    let mut key = [0u8; STAMP_LEN];
    key[..HLC_ENCODED_LEN].copy_from_slice(&stamp.hlc.to_bytes());
    key[HLC_ENCODED_LEN..].copy_from_slice(&stamp.node.to_bytes());
    key
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

    use super::*;

    fn stamp(ms: u64, n: u8) -> Stamp {
        Stamp::new(Hlc::new(ms, 3), NodeId::from_bytes([n; 16]))
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
            DocId::Binary(vec![0, 1, 2, 255]),
            DocId::Binary(Vec::new()),
            DocId::Uuid(uuid::Uuid::new_v4()),
        ];
        for id in ids {
            let mut buf = Vec::new();
            encode_doc_id(&id, &mut buf);
            let mut slice = buf.as_slice();
            assert_eq!(decode_doc_id(&mut slice).unwrap(), id);
            assert!(slice.is_empty(), "decoder left trailing bytes for {id:?}");
        }
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
            },
            // A delete carries no body.
            OplogEntry {
                stamp: stamp(101, 1),
                kind: OpKind::Delete,
                collection: CollectionId(7),
                doc_id: Some(DocId::Int64(42)),
                body: None,
            },
            // A collection-level op carries neither id nor body.
            OplogEntry {
                stamp: stamp(102, 1),
                kind: OpKind::Collection,
                collection: CollectionId(u64::MAX),
                doc_id: None,
                body: None,
            },
        ];
        for entry in entries {
            let decoded = decode_oplog_entry(&encode_oplog_entry(&entry)).unwrap();
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
        };
        let none = OplogEntry { body: None, ..base.clone() };
        assert_ne!(encode_oplog_entry(&base), encode_oplog_entry(&none));
        assert_eq!(decode_oplog_entry(&encode_oplog_entry(&base)).unwrap().body, Some(Vec::new()));
        assert_eq!(decode_oplog_entry(&encode_oplog_entry(&none)).unwrap().body, None);
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
        assert!(bound <= oplog_key(&Stamp::new(hlc, NodeId::from_bytes([0; 16]))));
        assert!(bound < oplog_key(&Stamp::new(hlc, NodeId::from_bytes([255; 16]))));
    }

    #[test]
    fn a_future_format_version_is_rejected() {
        let mut bytes = encode_doc_record(&DocRecord::tombstone(stamp(1, 1)));
        bytes[0] = FORMAT_VERSION + 1;
        assert!(matches!(decode_doc_record(&bytes), Err(StorageError::UnsupportedFormat { .. })));
    }

    #[test]
    fn truncated_records_error_rather_than_panic() {
        let full = encode_oplog_entry(&OplogEntry {
            stamp: stamp(1, 1),
            kind: OpKind::Insert,
            collection: CollectionId(1),
            doc_id: Some(DocId::String("abc".into())),
            body: Some(b"xyz".to_vec()),
        });
        for cut in 1..full.len() {
            // Every truncation must be a clean error; a panic here would take
            // down the server on a single corrupt page.
            assert!(decode_oplog_entry(&full[..cut]).is_err(), "no error at cut {cut}");
        }
    }
}
