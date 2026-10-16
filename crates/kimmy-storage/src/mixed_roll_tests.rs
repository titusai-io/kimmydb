//! The wire half of the mixed 0.50/0.51 roll (Decimal128 design 7.1 to 7.3; test plan
//! item 8, its v2.3 and v2.4 additions, and review Q7).
//!
//! Schema 5 changed the stored bytes of every number key. Every key that crosses between
//! members (a carried delete's key, a snapshot cursor, a cut digest's uncovered list) and
//! every key the cut digest hashes is therefore the frozen schema-4 form, so a 0.50 member
//! and this build exchange and compare exactly as two 0.50 members do. The conversions live
//! at the engine's wire API, and these tests call that API as the transport does.
//!
//! **No 0.50 binary runs here.** A *v1-shaped fixture* is the other end of the wire, written
//! in this file the way 0.50 behaves: pages and digest answers built, and pages applied, by
//! schema-4 key. Its encoder ([`v1`]) is a third copy of the schema-4 layout, sharing no
//! code with the product's frozen form (`kimmy_core::legacy_key`) or the live encoder, and
//! held to the bytes v0.50.0 wrote by a golden test.
//!
//! **The A2 pair** (design 6.1). The schema-5 key of [`A`] is byte for byte the schema-4 key
//! of [`B`] (`esc` puts `FF` after a zero byte). Every receiver and digest test holds both in
//! one collection: that is what turns a missed conversion from "a delete is lost" into "the
//! delete lands on another document", the failure this half exists to rule out.

use std::collections::{BTreeMap, BTreeSet};

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, NodeId, Stamp, VersionVector};
use redb::{ReadableDatabase, ReadableTable};
use serde_bytes::ByteBuf;

use crate::digest_probe::{DigestAsk, DigestEntry, DigestOutcome, DigestReply, ScanBudget};
use crate::{
    CollectionMeta, Engine, SNAPSHOT_PAGE, SnapshotCursor, SnapshotDoc, SnapshotPage,
    SnapshotPolicy, SnapshotProgress, SnapshotTombstone, Uncovered, WalkScope, codec, tables,
};

/// The id whose schema-5 key is the schema-4 key of [`B`].
pub(crate) const A: i64 = 0x5000_0081_0180_0000;
/// The id whose schema-4 key is the schema-5 key of [`A`].
pub(crate) const B: i64 = 0x5000_7F80_8101_8000;

// -- The schema-4 encoder of the fixtures -------------------------------------------------

/// The schema-4 key of a document id, and the cut digest's term over it, written here from
/// the layout and nothing else.
pub(crate) mod v1 {
    use kimmy_core::{DocId, Stamp};

    const TAG_NUMBER: u8 = 0x20;
    const TAG_STRING: u8 = 0x30;
    const TAG_BINARY: u8 = 0x60;
    const TAG_OBJECT_ID: u8 = 0x70;

    /// The schema-4 key of `id`. A `Decimal128` id has none.
    pub(crate) fn key(id: &DocId) -> Vec<u8> {
        match id {
            DocId::Int64(v) => int64(*v),
            DocId::String(s) => {
                let mut out = vec![TAG_STRING];
                for &byte in s.as_bytes() {
                    match byte {
                        0 => out.extend_from_slice(&[0x00, 0xFF]),
                        b => out.push(b),
                    }
                }
                out.extend_from_slice(&[0x00, 0x00]);
                out
            }
            DocId::ObjectId(oid) => {
                let mut out = vec![TAG_OBJECT_ID];
                out.extend_from_slice(&oid.bytes());
                out
            }
            DocId::Binary(bytes) => binary(0x00, bytes),
            DocId::Uuid(uuid) => binary(0x04, uuid.as_bytes()),
            DocId::Decimal128(_) => panic!("a Decimal128 id has no schema-4 key"),
        }
    }

    fn binary(subtype: u8, bytes: &[u8]) -> Vec<u8> {
        let mut out = vec![TAG_BINARY];
        out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        out.push(subtype);
        out.extend_from_slice(bytes);
        out
    }

    /// Tag, sign (`02` negative, `03` zero, `04` positive), then the exponent of the
    /// leading bit biased by 32,768 (2 bytes) and the magnitude shifted until its top bit
    /// is set (8 bytes), all ten inverted for a negative number.
    fn int64(v: i64) -> Vec<u8> {
        if v == 0 {
            return vec![TAG_NUMBER, 0x03];
        }
        let magnitude = v.unsigned_abs();
        let exp = 63 - magnitude.leading_zeros() as i32;
        let mut body = ((exp + 32_768) as u16).to_be_bytes().to_vec();
        body.extend_from_slice(&(magnitude << magnitude.leading_zeros()).to_be_bytes());
        if v < 0 {
            for byte in &mut body {
                *byte = !*byte;
            }
        }
        let mut out = vec![TAG_NUMBER, if v < 0 { 0x02 } else { 0x04 }];
        out.extend(body);
        out
    }

    /// The `i64` whose schema-4 key `key` is, if it is exactly one.
    pub(crate) fn int64_of(key: &[u8]) -> Option<i64> {
        if key == [TAG_NUMBER, 0x03] {
            return Some(0);
        }
        if key.len() != 12 || key[0] != TAG_NUMBER || !matches!(key[1], 0x02 | 0x04) {
            return None;
        }
        let mut body: [u8; 10] = key[2..].try_into().ok()?;
        if key[1] == 0x02 {
            for byte in &mut body {
                *byte = !*byte;
            }
        }
        let exp = i32::from(u16::from_be_bytes([body[0], body[1]])) - 32_768;
        let mantissa = u64::from_be_bytes(body[2..].try_into().ok()?);
        if !(0..=63).contains(&exp) {
            return None;
        }
        let magnitude = mantissa >> (63 - exp);
        let v = if key[1] == 0x02 {
            if magnitude == 1 << 63 { i64::MIN } else { -i64::try_from(magnitude).ok()? }
        } else {
            i64::try_from(magnitude).ok()?
        };
        // Canonical only: the bytes this encoder writes for the value.
        (int64(v) == key).then_some(v)
    }

    /// The cut digest's term: `xxh3_128(key || hlc || node)`.
    pub(crate) fn term(key: &[u8], stamp: &Stamp) -> u128 {
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        hasher.update(key);
        hasher.update(&stamp.hlc.to_bytes());
        hasher.update(&stamp.node.to_bytes());
        hasher.digest128()
    }
}

/// The stored (schema-5) key of the document a schema-4 key names, as a 0.50 sender's key
/// names it: an integer key decoded to its id, any other key (a string, an `ObjectId`) the
/// same bytes in both schemas. `None` for a number key that is not a canonical schema-4
/// one: a key that left the engine unconverted.
fn stored_of_v1(key: &[u8]) -> Option<Vec<u8>> {
    match key.first() {
        Some(0x20) => v1::int64_of(key).map(|v| crate::docs::doc_key(&DocId::Int64(v)).unwrap()),
        Some(0xFF) | None => None,
        Some(_) => Some(key.to_vec()),
    }
}

/// Every record of `collection` as stored: `(stored key, stamp, the id if live)`.
fn records(engine: &Engine, collection: CollectionId) -> Vec<(Vec<u8>, Stamp, Option<DocId>)> {
    let txn = engine.db().begin_read().unwrap();
    let docs = txn.open_table(tables::DOCS).unwrap();
    let mut out = Vec::new();
    for row in docs.range(crate::engine::doc_range(collection)).unwrap() {
        let (key, value) = row.unwrap();
        let record = codec::decode_doc_record(value.value()).unwrap();
        let id = record
            .document()
            .unwrap()
            .map(|body| DocId::try_from_bson(body.get(crate::ID_FIELD).unwrap()).unwrap());
        out.push((key.value().1.to_vec(), record.stamp, id));
    }
    out
}

/// The digest a 0.50 member keeps for `collection` over the same records: the XOR of the
/// schema-4 term of every live document, its key taken from the body's `_id`.
pub(crate) fn v1_digest(engine: &Engine, collection: CollectionId) -> u128 {
    records(engine, collection)
        .into_iter()
        .filter_map(|(_, stamp, id)| id.map(|id| v1::term(&v1::key(&id), &stamp)))
        .fold(0, |digest, term| digest ^ term)
}

// -- The model oracles (review Q7) ---------------------------------------------------------

/// The two oracles the model runs carry: a mixed pair behaves as a pair of this build
/// exactly when what this build puts on the wire is 0.50's bytes and its digests are 0.50's.
pub(crate) mod oracle {
    use super::*;

    /// (a), on a page `sender` served: every carried delete's key is the schema-4 key of the
    /// document the sender holds that tombstone for, and the cursor is the schema-4 key of
    /// the page's last row.
    pub(crate) fn page(sender: &Engine, page: &SnapshotPage) -> Result<(), String> {
        // Every row the page passed, by its schema-4 key: the cursor names the greatest.
        let mut passed: Vec<(CollectionId, Vec<u8>)> =
            page.documents.iter().map(|d| (d.collection, v1::key(&d.id))).collect();
        let txn = sender.db().begin_read().unwrap();
        let docs = txn.open_table(tables::DOCS).unwrap();
        for tombstone in &page.deleted_documents {
            let Some(stored) = stored_of_v1(&tombstone.key) else {
                return Err(format!(
                    "a carried delete's key is not schema 4: {:02x?}",
                    tombstone.key
                ));
            };
            let held = docs
                .get((tombstone.collection.0, stored.as_slice()))
                .unwrap()
                .map(|raw| codec::decode_doc_record(raw.value()).unwrap());
            match held {
                Some(record) if !record.is_live() && record.stamp == tombstone.stamp => {}
                other => {
                    return Err(format!(
                        "the carried delete {:02x?} at {:?} names no tombstone of the sender's \
                         ({other:?})",
                        tombstone.key, tombstone.stamp
                    ));
                }
            }
            passed.push((tombstone.collection, tombstone.key.clone()));
        }
        if let Some(next) = &page.next {
            let last = passed.into_iter().max();
            let at = Some((next.collection, next.after_key.clone()));
            if at != last {
                return Err(format!(
                    "the cursor {next} is not the schema-4 key of the page's last row {last:02x?}"
                ));
            }
        }
        Ok(())
    }

    /// (a), on an uncovered list `owner` sent for `collection` at `cut`: every key is the
    /// schema-4 key of a document an entry of `owner`'s above the cut names.
    pub(crate) fn uncovered(
        owner: &Engine,
        cut: &VersionVector,
        collection: CollectionId,
        keys: &[ByteBuf],
    ) -> Result<(), String> {
        let named: BTreeSet<Vec<u8>> = owner
            .read_oplog_from(Hlc::ZERO, usize::MAX, WalkScope::Background)
            .unwrap()
            .into_iter()
            .filter(|e| e.collection == collection && e.stamp.hlc > cut.get(e.stamp.node))
            .filter_map(|e| e.doc_id.map(|id| v1::key(&id)))
            .collect();
        for key in keys {
            if !named.contains(key.as_slice()) {
                return Err(format!(
                    "an uncovered key {:02x?} is not the schema-4 key of an entry above the cut",
                    key.as_slice()
                ));
            }
        }
        Ok(())
    }

    /// (a), on a digest request and the answer to it.
    pub(crate) fn exchange(
        requester: &Engine,
        ask: &DigestAsk,
        answerer: &Engine,
        reply: &DigestReply,
    ) -> Result<(), String> {
        for list in &ask.uncovered {
            uncovered(requester, &ask.cut, list.collection, &list.keys)?;
        }
        for entry in &reply.entries {
            uncovered(answerer, &ask.cut, entry.collection, &entry.uncovered)?;
        }
        Ok(())
    }

    /// (b): every kept digest row of a standing collection equals the schema-4 digest of the
    /// member's own documents.
    pub(crate) fn digests(engine: &Engine) -> Result<(), String> {
        let ids: Vec<CollectionId> = {
            let txn = engine.db().begin_read().unwrap();
            let collections = txn.open_table(tables::COLLECTIONS).unwrap();
            collections
                .iter()
                .unwrap()
                .map(|row| {
                    let (_, value) = row.unwrap();
                    serde_json::from_slice::<CollectionMeta>(value.value()).unwrap().id
                })
                .collect()
        };
        for id in ids {
            let kept = engine.digest_row(id).unwrap().map_or(0, |row| row.digest);
            let want = v1_digest(engine, id);
            if kept != want {
                return Err(format!(
                    "the digest of {id} is {kept:032x}; the schema-4 digest of its documents is \
                     {want:032x}"
                ));
            }
        }
        Ok(())
    }
}

// -- The 0.50-shaped ends of the wire -------------------------------------------------------

/// One record as stored, with what a 0.50 sender would put on a page for it.
struct Row {
    stored: Vec<u8>,
    v1: Vec<u8>,
    stamp: Stamp,
    /// The id and body of a live record.
    live: Option<(DocId, Vec<u8>)>,
}

/// `collection`'s records in schema-4 key order. A tombstone keeps no `_id`, so its id is
/// the one of `ids` whose stored key it is.
fn rows(engine: &Engine, collection: CollectionId, ids: &[DocId]) -> Vec<Row> {
    let txn = engine.db().begin_read().unwrap();
    let docs = txn.open_table(tables::DOCS).unwrap();
    let mut out = Vec::new();
    for row in docs.range(crate::engine::doc_range(collection)).unwrap() {
        let (key, value) = row.unwrap();
        let stored = key.value().1.to_vec();
        let record = codec::decode_doc_record(value.value()).unwrap();
        let live = record.document().unwrap().map(|body| {
            let id = DocId::try_from_bson(body.get(crate::ID_FIELD).unwrap()).unwrap();
            (id, record.body.clone())
        });
        let id = match &live {
            Some((id, _)) => id.clone(),
            None => ids
                .iter()
                .find(|id| crate::docs::doc_key(id).unwrap() == stored)
                .unwrap_or_else(|| panic!("a tombstone of no fixture id: {stored:02x?}"))
                .clone(),
        };
        out.push(Row { stored, v1: v1::key(&id), stamp: record.stamp, live });
    }
    out.sort_by(|a, b| a.v1.cmp(&b.v1));
    out
}

/// A scoped page of `collection` as a 0.50 sender cuts it from `sender`'s records: the
/// rows after `after` (a schema-4 cursor) in schema-4 key order, at most `limit`, each
/// carried delete by its schema-4 key, and the cursor the schema-4 key of the last row.
fn v1_page(
    sender: &Engine,
    collection: CollectionId,
    ids: &[DocId],
    after: Option<&SnapshotCursor>,
    limit: usize,
) -> SnapshotPage {
    let all: Vec<Row> = rows(sender, collection, ids)
        .into_iter()
        .filter(|row| after.is_none_or(|c| row.v1.as_slice() > c.after_key.as_slice()))
        .collect();
    let more = all.len() > limit;
    let taken: Vec<Row> = all.into_iter().take(limit).collect();
    let next = match (more, taken.last()) {
        (true, Some(last)) => Some(SnapshotCursor { collection, after_key: last.v1.clone() }),
        _ => None,
    };
    let mut page = SnapshotPage {
        collections: match after {
            None => vec![crate::CollectionState::from(
                sender.collection_by_id(collection).unwrap().expect("the collection"),
            )],
            Some(_) => Vec::new(),
        },
        documents: Vec::new(),
        next,
        versions: sender.version_vector().unwrap(),
        dropped: None,
        dropped_collections: Vec::new(),
        deleted_documents: Vec::new(),
        dropped_indexes: Vec::new(),
        tombstone_horizon: sender.tombstone_horizon().unwrap(),
    };
    for row in taken {
        match row.live {
            Some((id, body)) => page.documents.push(SnapshotDoc {
                collection,
                id,
                stamp: row.stamp,
                body: Some(body),
            }),
            None => page.deleted_documents.push(SnapshotTombstone {
                collection,
                key: row.v1,
                stamp: row.stamp,
            }),
        }
    }
    page
}

/// A 0.50 member's `DOCS` for one collection, keyed by schema-4 key, and what 0.50 does with
/// a scoped page (`apply_carried_document_deletes` and the document arms at v0.50.0, in
/// miniature).
#[derive(Clone, Debug, Default)]
struct V1Member {
    /// Schema-4 key to the record's stamp, and its id when live.
    rows: BTreeMap<Vec<u8>, (Stamp, Option<DocId>)>,
    /// The cursor as received, sent back as it is.
    after: Option<SnapshotCursor>,
    /// Every row a page carried, by key, in the order they came.
    passed: Vec<Vec<u8>>,
}

impl V1Member {
    /// What `engine` holds of `collection`, as a 0.50 member holds it.
    fn holding(engine: &Engine, collection: CollectionId, ids: &[DocId]) -> Self {
        let rows = rows(engine, collection, ids)
            .into_iter()
            .map(|row| (row.v1, (row.stamp, row.live.map(|(id, _)| id))))
            .collect();
        V1Member { rows, ..Default::default() }
    }

    /// Apply one page under `All` or, with `converge`, under `Converge`, where a row held
    /// nowhere is written only when stamped above `young_above`.
    fn apply(&mut self, page: &SnapshotPage, converge: bool, young_above: Hlc) {
        for document in &page.documents {
            let key = v1::key(&document.id);
            self.passed.push(key.clone());
            let write = match self.rows.get(&key) {
                Some((held, _)) => document.stamp.wins_over(held),
                None => !converge || document.stamp.hlc > young_above,
            };
            if write {
                self.rows.insert(key, (document.stamp, Some(document.id.clone())));
            }
        }
        for tombstone in &page.deleted_documents {
            self.passed.push(tombstone.key.clone());
            if tombstone.stamp.hlc > page.versions.get(tombstone.stamp.node) {
                continue;
            }
            let write = match self.rows.get(&tombstone.key) {
                // The bare tombstone, `All` only.
                None => !converge,
                Some((held, _)) if !tombstone.stamp.wins_over(held) => false,
                // Over an older tombstone, both policies.
                Some((_, None)) => true,
                // Over a live document whose own key is the one carried.
                Some((_, Some(id))) => v1::key(id) == tombstone.key,
            };
            if write {
                self.rows.insert(tombstone.key.clone(), (tombstone.stamp, None));
            }
        }
        self.after = page.next.clone();
    }

    /// Pull every page of `collection` from `sender`, sending each cursor back as received.
    fn pull(&mut self, sender: &Engine, collection: CollectionId, converge: bool, young: Hlc) {
        self.after = None;
        for _ in 0..64 {
            let page = sender.snapshot_page(self.after.clone(), Some(collection)).unwrap();
            oracle::page(sender, &page).unwrap();
            self.apply(&page, converge, young);
            if self.after.is_none() {
                return;
            }
        }
        panic!("a pull that never ended");
    }

    fn live(&self, id: i64) -> Option<Stamp> {
        match self.rows.get(&v1::key(&DocId::Int64(id))) {
            Some((stamp, Some(_))) => Some(*stamp),
            _ => None,
        }
    }

    fn tombstone(&self, id: i64) -> Option<Stamp> {
        match self.rows.get(&v1::key(&DocId::Int64(id))) {
            Some((stamp, None)) => Some(*stamp),
            _ => None,
        }
    }
}

// -- Engines --------------------------------------------------------------------------------

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

/// Everything `from` holds, by entries.
fn pull(into: &Engine, from: &Engine) {
    let entries = from.entries_for_peer(Hlc::ZERO, 100_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

/// A record of `collection` under the stored key of `id`: its stamp and whether it is live.
fn held(engine: &Engine, collection: CollectionId, id: i64) -> Option<(Stamp, bool)> {
    held_at(engine, collection, &crate::docs::doc_key(&DocId::Int64(id)).unwrap())
}

fn held_at(engine: &Engine, collection: CollectionId, key: &[u8]) -> Option<(Stamp, bool)> {
    let txn = engine.db().begin_read().unwrap();
    let docs = txn.open_table(tables::DOCS).unwrap();
    docs.get((collection.0, key)).unwrap().map(|raw| codec::doc_record_header(raw.value()).unwrap())
}

fn count_rows(engine: &Engine, collection: CollectionId) -> usize {
    let txn = engine.db().begin_read().unwrap();
    let docs = txn.open_table(tables::DOCS).unwrap();
    docs.range(crate::engine::doc_range(collection)).unwrap().count()
}

/// The stamp the latest write left on `engine`'s clock, for a peer to be moved past.
fn now_on(engine: &Engine) -> Stamp {
    Stamp::new(engine.clock_last(), engine.node_id())
}

fn int(id: i64) -> DocId {
    DocId::Int64(id)
}

// -- The golden bytes -----------------------------------------------------------------------

/// FNV-1a over each key's length and bytes, in order: a checksum of a run of keys.
fn fnv(keys: impl Iterator<Item = Vec<u8>>) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for key in keys {
        for byte in std::iter::once(key.len() as u8).chain(key) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

/// The fixtures' encoder writes what v0.50.0's `keyenc::encode` wrote, for every id the
/// model runs and these tests use. The bytes were printed by a build of that tag and are
/// checked in: each named id's, and a checksum of every integer from -1,100 to 2,100 (the
/// model ids, the young-stamp filler and their neighbours).
#[test]
fn the_fixtures_schema_4_encoder_writes_what_v0_50_0_wrote() {
    let named: [(DocId, &str); 24] = [
        (int(0), "2003"),
        (int(1), "200480008000000000000000"),
        (int(3), "20048001c000000000000000"),
        (int(5), "20048002a000000000000000"),
        (int(-1), "20027fff7fffffffffffffff"),
        (int(-1_100), "20027ff5767fffffffffffff"),
        (int(255), "20048007ff00000000000000"),
        (int(1_000), "20048009fa00000000000000"),
        (int(2_000), "2004800afa00000000000000"),
        (int(A), "2004803ea000010203000000"),
        (int(B), "2004803ea000ff0102030000"),
        (int(A + 1), "2004803ea000010203000002"),
        (int(B - 1), "2004803ea000ff010202fffe"),
        (int(i64::MIN), "20027fc07fffffffffffffff"),
        (int(i64::MIN + 1), "20027fc10000000000000001"),
        (int(i64::MAX), "2004803efffffffffffffffe"),
        (int(i64::MAX - 1), "2004803efffffffffffffffc"),
        (int(1 << 53), "200480358000000000000000"),
        (int((1 << 53) + 1), "200480358000000000000400"),
        (int(-(1 << 53) - 1), "20027fca7ffffffffffffbff"),
        (DocId::String("a".into()), "30610000"),
        (DocId::String("a\0b".into()), "306100ff620000"),
        (DocId::ObjectId(bson::oid::ObjectId::from_bytes([7; 12])), "70070707070707070707070707"),
        (DocId::Binary(vec![0, 1, 255]), "600000000000000003000001ff"),
    ];
    for (id, hex) in named {
        assert_eq!(v1::key(&id), unhex(hex), "{id:?}");
    }
    let range = fnv((-1_100i64..=2_100).map(|v| v1::key(&int(v))));
    assert_eq!(range, 0xec55_ebef_91c7_bca1, "the keys of -1,100..=2,100 moved");
}

/// The product's frozen form (`kimmy_core::legacy_key`) is the fixtures' too, and the A2
/// pair is what design 6.1 says: the schema-5 key of `A` is the schema-4 key of `B`.
#[test]
fn the_products_frozen_form_is_the_fixtures_and_the_a2_pair_is_crossed() {
    let mut ids: Vec<i64> = (-1_100..=2_100).collect();
    ids.extend([A, B, A + 1, B - 1, i64::MIN, i64::MAX, 1 << 53, -(1 << 53) - 1]);
    for v in ids {
        let id = int(v);
        assert_eq!(kimmy_core::keyenc::legacy_key(&id).as_bytes(), v1::key(&id), "{v}");
        let stored = crate::docs::doc_key(&id).unwrap();
        assert_eq!(kimmy_core::keyenc::legacy_of_stored(&stored).as_bytes(), v1::key(&id), "{v}");
        assert_eq!(kimmy_core::keyenc::stored_of_legacy(&v1::key(&id)), stored, "{v}");
        assert_eq!(v1::int64_of(&v1::key(&id)), Some(v), "{v}");
    }
    assert_eq!(crate::docs::doc_key(&int(A)).unwrap(), v1::key(&int(B)), "the A2 pair");
    assert_ne!(crate::docs::doc_key(&int(B)).unwrap(), v1::key(&int(A)));
    assert_eq!(v1::int64_of(&crate::docs::doc_key(&int(A)).unwrap()), Some(B));
}

// -- 7.1, the sender: what `snapshot_page` puts on the wire --------------------------------

/// A collection holding `ids`, with `deleted` of them deleted again: live rows and
/// tombstones interleaved in key order.
fn holding(engine: &Engine, name: &str, ids: &[i64], deleted: &[i64]) -> CollectionMeta {
    let coll = engine.create_collection("shop", name).unwrap();
    let docs = ids.iter().map(|id| doc! { "_id": *id, "v": 0 }).collect();
    engine.insert_many(&coll, docs).unwrap();
    for id in deleted {
        assert!(engine.delete(&coll, &int(*id)).unwrap());
    }
    coll
}

/// Every carried delete and the cursor are schema-4 keys, on a scoped page and on a
/// whole-database one, and the cursor names the page's last row.
#[test]
fn a_page_carries_its_deletes_and_its_cursor_in_the_schema_4_form() {
    let (s, _ds) = engine();
    let mut ids: Vec<i64> = (-20..600).collect();
    ids.extend([A, A + 1, B - 1, B, B + 1, i64::MAX, i64::MIN]);
    let deleted: Vec<i64> = ids
        .iter()
        .copied()
        .filter(|id| id % 3 == 0)
        .chain([B - 1, B])
        .collect::<BTreeSet<i64>>()
        .into_iter()
        .collect();
    let coll = holding(&s, "orders", &ids, &deleted);

    for scope in [Some(coll.id), None] {
        let mut after = None;
        let mut carried = Vec::new();
        for pages in 1.. {
            assert!(pages <= 16, "a walk that does not end");
            let page = s.snapshot_page(after.clone(), scope).unwrap();
            oracle::page(&s, &page).unwrap();
            carried.extend(page.deleted_documents.iter().map(|t| t.key.clone()));
            match page.next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        let mut want: Vec<Vec<u8>> = deleted.iter().map(|id| v1::key(&int(*id))).collect();
        want.sort();
        assert_eq!(carried, want, "every delete, once, by its schema-4 key ({scope:?})");
    }
    let first = s.snapshot_page(None, Some(coll.id)).unwrap();
    let next = first.next.expect("more than a page");
    assert!(v1::int64_of(&next.after_key).is_some(), "a schema-4 integer key: {next}");
}

/// The sender reads the request's cursor as the schema-4 key it is: the page resumes right
/// after the row it names, at an A2 key too, whose schema-4 key sorts below its stored one.
#[test]
fn the_sender_resumes_after_the_row_a_schema_4_cursor_names() {
    let (s, _ds) = engine();
    let ids = [1, 2, 3, A, A + 1, B - 1, B, B + 1];
    let coll = holding(&s, "orders", &ids, &[]);
    for (i, id) in ids.iter().enumerate() {
        let after = SnapshotCursor { collection: coll.id, after_key: v1::key(&int(*id)) };
        let page = s.snapshot_page(Some(after), Some(coll.id)).unwrap();
        let served: Vec<DocId> = page.documents.iter().map(|d| d.id.clone()).collect();
        let want: Vec<DocId> = ids[i + 1..].iter().map(|id| int(*id)).collect();
        assert_eq!(served, want, "after {id:#x}");
    }
}

/// Three ids per row past one page, so the first page ends at `A`: the rows between `A` and
/// `B` are exactly what a cursor read in the wrong form repeats or skips.
fn across_an_a2_boundary(engine: &Engine) -> (CollectionMeta, Vec<i64>, Vec<i64>) {
    let mut ids: Vec<i64> = (1..SNAPSHOT_PAGE as i64).collect();
    ids.extend([A, A + 1, B - 1, B, B + 1]);
    let deleted = vec![5, 100, B - 1];
    let coll = holding(engine, "orders", &ids, &deleted);
    (coll, ids, deleted)
}

/// A pull across a page boundary at `A`, in every direction: each row arrives once, the
/// requester keeps the cursor as it received it and sends it back unchanged, and the sender
/// reads it as schema 4.
#[test]
fn a_pull_across_an_a2_boundary_takes_every_row_once_in_every_direction() {
    let (s, _ds) = engine();
    let (coll, ids, deleted) = across_an_a2_boundary(&s);
    let all: Vec<DocId> = ids.iter().map(|id| int(*id)).collect();
    let mut keys: Vec<Vec<u8>> = all.iter().map(v1::key).collect();
    keys.sort();
    let first = s.snapshot_page(None, Some(coll.id)).unwrap();
    assert_eq!(first.next.as_ref().unwrap().after_key, v1::key(&int(A)), "premise: ends at A");

    // This build to this build, under `All`: the cursor is persisted as received.
    let (r, _dr) = engine();
    let mut progress = SnapshotProgress::of_collection(coll.id);
    let mut passed = Vec::new();
    while !progress.is_complete() {
        let page = s.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        oracle::page(&s, &page).unwrap();
        passed.extend(page.documents.iter().map(|d| v1::key(&d.id)));
        passed.extend(page.deleted_documents.iter().map(|t| t.key.clone()));
        r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
        if let Some(next) = &page.next {
            assert_eq!(progress.after(), Some(next), "kept as received");
            let stored = r.snapshots_to_resume().unwrap();
            assert_eq!(stored.len(), 1);
            assert_eq!(stored[0].1.after(), Some(next), "persisted as received");
        }
    }
    passed.sort();
    assert_eq!(passed, keys, "every row once");
    assert_eq!(count_rows(&r, coll.id), ids.len());
    for id in &ids {
        assert_eq!(held(&r, coll.id, *id).map(|(_, live)| live), Some(!deleted.contains(id)));
    }

    // This build to a 0.50 member.
    let mut old = V1Member::default();
    old.pull(&s, coll.id, false, Hlc::ZERO);
    let mut passed = old.passed.clone();
    let before = passed.len();
    passed.sort();
    passed.dedup();
    assert_eq!((before, passed), (keys.len(), keys.clone()), "every row once, to 0.50");

    // A 0.50 member to this build: the cursor goes back as it came.
    let (r, _dr) = engine();
    let mut progress = SnapshotProgress::of_collection(coll.id);
    while !progress.is_complete() {
        let page = v1_page(&s, coll.id, &all, progress.after(), SNAPSHOT_PAGE);
        r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
        assert_eq!(progress.after(), page.next.as_ref(), "kept as received");
    }
    assert_eq!(count_rows(&r, coll.id), ids.len());
    for id in &ids {
        assert_eq!(held(&r, coll.id, *id).map(|(_, live)| live), Some(!deleted.contains(id)));
    }
    oracle::digests(&r).unwrap();
}

// -- 7.1, the receiver: every held state, both policies, every direction --------------------

/// The receiver matrix's collections, one per state the receiver holds. Each holds the A2
/// pair, so a carried delete of `B` read unconverted meets `A`.
const LIVE: &str = "held_live";
const TOMBSTONE: &str = "held_tombstone";
const NOTHING: &str = "held_nothing";
const ROWS: &str = "held_rows";

/// The ids the matrix's tombstones may be of.
fn matrix_ids() -> Vec<DocId> {
    [A, B, 7, 9, A + 1].map(int).to_vec()
}

/// A sender `s` and a receiver `r` that agreed, after which `r` deleted and `s` moved on:
/// - `LIVE`: `r` holds `A`, `B` and 7 live, and `s` deleted `B` and 7 (a delete over a held
///   live row);
/// - `TOMBSTONE`: `r` holds `A` live and deleted `B` and 7 itself, and `s` deleted them
///   later (a delete over a held older tombstone);
/// - `NOTHING`: `r` holds `A` live and never saw `B` or 7, which `s` wrote and deleted (a
///   delete where nothing is held: `All`'s bare tombstone);
/// - `ROWS`: `r` holds `A`, `B` and 7 as first written, and `s` replaced them and wrote 9
///   and `A + 1`, which `r` lacks (a newer row over a held one; a young row created).
fn matrix() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (s, ds) = engine();
    let (r, dr) = engine();
    for name in [LIVE, TOMBSTONE, NOTHING, ROWS] {
        let coll = s.create_collection("shop", name).unwrap();
        let ids: &[i64] = if name == NOTHING { &[A] } else { &[A, B, 7] };
        s.insert_many(&coll, ids.iter().map(|id| doc! { "_id": *id, "v": 0 }).collect()).unwrap();
    }
    pull(&r, &s);
    let tombstone = r.get_collection("shop", TOMBSTONE).unwrap();
    for id in [B, 7] {
        assert!(r.delete(&tombstone, &int(id)).unwrap());
    }
    s.advance_clock_past(&now_on(&r));
    let coll = |name| s.get_collection("shop", name).unwrap();
    for id in [B, 7] {
        assert!(s.delete(&coll(LIVE), &int(id)).unwrap());
        assert!(s.delete(&coll(TOMBSTONE), &int(id)).unwrap());
        s.insert(&coll(NOTHING), doc! { "_id": id, "v": 1 }).unwrap();
        assert!(s.delete(&coll(NOTHING), &int(id)).unwrap());
    }
    for id in [A, B, 7, 9, A + 1] {
        s.replace(&coll(ROWS), &int(id), doc! { "_id": id, "v": 1 }, true).unwrap();
    }
    (s, r, [ds, dr])
}

/// What a receiver holds, whichever build it is: a record's stamp, live or a tombstone.
trait Holds {
    fn record(&self, name: &str, id: i64) -> Option<(Stamp, bool)>;
    fn rows(&self, name: &str) -> usize;
}

struct ThisBuild<'a>(&'a Engine);

impl Holds for ThisBuild<'_> {
    fn record(&self, name: &str, id: i64) -> Option<(Stamp, bool)> {
        held(self.0, CollectionId::derive("shop", name), id)
    }
    fn rows(&self, name: &str) -> usize {
        count_rows(self.0, CollectionId::derive("shop", name))
    }
}

impl Holds for BTreeMap<&'static str, V1Member> {
    fn record(&self, name: &str, id: i64) -> Option<(Stamp, bool)> {
        let member = &self[name];
        member.live(id).map(|s| (s, true)).or_else(|| member.tombstone(id).map(|s| (s, false)))
    }
    fn rows(&self, name: &str) -> usize {
        self[name].rows.len()
    }
}

/// What every direction must leave: the delete on the document whose key it carries and on
/// no other, the bare tombstone under `All` only, the newer rows applied.
fn assert_matrix(
    after: &dyn Holds,
    s: &Engine,
    a_before: &BTreeMap<&str, Stamp>,
    converge: bool,
    how: &str,
) {
    let sent = |name: &str, id: i64| held(s, CollectionId::derive("shop", name), id).unwrap();
    for name in [LIVE, TOMBSTONE, NOTHING] {
        let untouched = Some((a_before[name], true));
        assert_eq!(after.record(name, A), untouched, "{how}: {name}: A untouched");
    }
    for name in [LIVE, TOMBSTONE] {
        for id in [B, 7] {
            assert_eq!(after.record(name, id), Some(sent(name, id)), "{how}: {name}: {id:#x}");
        }
        assert_eq!(after.rows(name), 3, "{how}: {name}: no row under another key");
    }
    for id in [B, 7] {
        let want = if converge { None } else { Some(sent(NOTHING, id)) };
        assert_eq!(after.record(NOTHING, id), want, "{how}: {NOTHING}: {id:#x}");
    }
    assert_eq!(after.rows(NOTHING), if converge { 1 } else { 3 }, "{how}: {NOTHING}");
    for id in [A, B, 7, 9, A + 1] {
        assert_eq!(after.record(ROWS, id), Some(sent(ROWS, id)), "{how}: {ROWS}: {id:#x}");
    }
    assert_eq!(after.rows(ROWS), 5, "{how}: {ROWS}");
}

/// The stamp of `A` the receiver holds in each collection before the pull.
fn a_held(r: &Engine) -> BTreeMap<&'static str, Stamp> {
    [LIVE, TOMBSTONE, NOTHING]
        .into_iter()
        .map(|name| (name, held(r, CollectionId::derive("shop", name), A).unwrap().0))
        .collect()
}

fn progress_for(id: CollectionId, converge: bool) -> SnapshotProgress {
    if converge {
        SnapshotProgress::of_collection_converging(id, VersionVector::new())
    } else {
        SnapshotProgress::of_collection(id)
    }
}

/// Every held state, under `All` and `Converge`, in each direction: this build to this
/// build, a 0.50 sender to this build, and this build to a 0.50 receiver.
#[test]
fn a_carried_delete_lands_on_the_document_its_key_names_in_every_direction() {
    for converge in [false, true] {
        let policy = if converge { "Converge" } else { "All" };

        let (s, r, _dirs) = matrix();
        let a_before = a_held(&r);
        for name in [LIVE, TOMBSTONE, NOTHING, ROWS] {
            let id = CollectionId::derive("shop", name);
            let mut progress = progress_for(id, converge);
            assert_eq!(progress.policy() == SnapshotPolicy::Converge, converge);
            while !progress.is_complete() {
                let page = s.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
                oracle::page(&s, &page).unwrap();
                r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
            }
        }
        assert_matrix(&ThisBuild(&r), &s, &a_before, converge, &format!("{policy}, 0.51 to 0.51"));
        oracle::digests(&r).unwrap();

        let (s, r, _dirs) = matrix();
        let a_before = a_held(&r);
        for name in [LIVE, TOMBSTONE, NOTHING, ROWS] {
            let id = CollectionId::derive("shop", name);
            let mut progress = progress_for(id, converge);
            while !progress.is_complete() {
                let page = v1_page(&s, id, &matrix_ids(), progress.after(), SNAPSHOT_PAGE);
                r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
            }
        }
        assert_matrix(&ThisBuild(&r), &s, &a_before, converge, &format!("{policy}, 0.50 to 0.51"));
        oracle::digests(&r).unwrap();

        let (s, r, _dirs) = matrix();
        let a_before = a_held(&r);
        let young = r.tombstone_horizon().unwrap().max(s.tombstone_horizon().unwrap()).unwrap();
        let mut old = BTreeMap::new();
        for name in [LIVE, TOMBSTONE, NOTHING, ROWS] {
            let id = CollectionId::derive("shop", name);
            let mut member = V1Member::holding(&r, id, &matrix_ids());
            member.pull(&s, id, converge, young);
            old.insert(name, member);
        }
        assert_matrix(&old, &s, &a_before, converge, &format!("{policy}, 0.51 to 0.50"));
    }
}

// -- 7.3, the cut digest --------------------------------------------------------------------

/// The ids the digest tests hold: the A2 pair, both ends of `i64`, zero and both signs, and
/// ids with no number in them, whose keys are the same in both schemas.
fn digest_ids() -> Vec<DocId> {
    let mut ids: Vec<DocId> = [A, B, A + 1, B - 1, 0, -1, 5, i64::MIN, i64::MAX, 1 << 53]
        .into_iter()
        .chain(10..60)
        .map(int)
        .collect();
    ids.extend([
        DocId::String("a".into()),
        DocId::String("a\0b".into()),
        DocId::ObjectId(bson::oid::ObjectId::from_bytes([7; 12])),
        DocId::Binary(vec![0, 1, 255]),
    ]);
    ids
}

/// Write `record` under `key` of `collection` as a replicated write does, through
/// `put_record` (or `put_bare_tombstone` with `bare`).
fn put(
    engine: &Engine,
    collection: CollectionId,
    key: &[u8],
    record: &kimmy_core::DocRecord,
    bare: bool,
) {
    let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
    {
        let mut docs = txn.open_table(tables::DOCS).unwrap();
        let encoded = codec::encode_doc_record(record);
        match bare {
            true => {
                crate::live_count::put_bare_tombstone(&txn, &mut docs, collection.0, key, &encoded)
            }
            false => crate::live_count::put_record(&txn, &mut docs, collection.0, key, &encoded),
        }
        .unwrap();
    }
    txn.commit().unwrap();
}

/// The same data, the digest a 0.50 member keeps: from the fold of every write
/// (`put_record`, `put_bare_tombstone`, `remove_record`), from the walk at an open that
/// rebuilds the rows, and from the self-check, which finds the row exact and corrects a
/// drifted one back to it.
#[test]
fn the_cut_digest_is_the_schema_4_digest_at_every_fold_site() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let engine = Engine::open(&path).unwrap();
    let coll = engine.create_collection("shop", "orders").unwrap();
    let docs = digest_ids().iter().map(|id| doc! { "_id": id.to_bson(), "v": 0 }).collect();
    engine.insert_many(&coll, docs).unwrap();
    let check = |engine: &Engine, after: &str| {
        let kept = engine.digest_row(coll.id).unwrap().expect("a row").digest;
        assert_eq!(kept, v1_digest(engine, coll.id), "after {after}");
        assert_ne!(kept, 0, "premise: live documents");
        oracle::digests(engine).unwrap();
    };
    check(&engine, "the inserts");

    for id in [A, 5, i64::MIN] {
        engine.replace(&coll, &int(id), doc! { "_id": id, "v": 1 }, false).unwrap();
    }
    for id in [B, 7, 11] {
        engine.delete(&coll, &int(id)).ok();
    }
    engine.delete(&coll, &DocId::String("a".into())).unwrap();
    engine.insert(&coll, doc! { "_id": 7i64, "v": 2 }).unwrap();
    check(&engine, "replaces, deletes and an insert over a tombstone");

    // A tombstone with no entry behind it, over nothing and over a live document.
    let far =
        Stamp::new(Hlc::new(crate::physical_now_ms() + 60_000, 0), NodeId::from_bytes([9; 16]));
    for id in [B - 1, 12] {
        let key = crate::docs::doc_key(&int(id)).unwrap();
        put(&engine, coll.id, &key, &kimmy_core::DocRecord::tombstone(far), true);
    }
    put(
        &engine,
        coll.id,
        &crate::docs::doc_key(&int(70)).unwrap(),
        &kimmy_core::DocRecord::tombstone(far),
        true,
    );
    check(&engine, "bare tombstones");

    // A live record taken out by `remove_record`, as the drop purger takes them.
    {
        let txn = engine.begin_write(crate::engine::WriterHolder::Write).unwrap();
        {
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            for id in [A + 1, 13] {
                let key = crate::docs::doc_key(&int(id)).unwrap();
                assert!(
                    crate::live_count::remove_record(&txn, &mut docs, coll.id.0, &key).unwrap()
                );
            }
        }
        txn.commit().unwrap();
    }
    check(&engine, "remove_record");
    let kept = engine.digest_row(coll.id).unwrap().unwrap().digest;
    engine.close().unwrap();

    // The walk at an open that rebuilds the rows, whatever their marks say.
    let engine =
        Engine::open_with(&path, &crate::OpenOptions::new().force_rebuild("a test")).unwrap();
    assert_eq!(engine.digest_row(coll.id).unwrap().unwrap().digest, kept, "the rebuild");
    check(&engine, "a rebuild at open");

    // The self-check.
    assert_eq!(
        engine.verify_digest_row(coll.id, ScanBudget::self_check()).unwrap(),
        crate::SelfCheck::Exact
    );
    engine.corrupt_digest_row_for_test(coll.id, 0xD1F7).unwrap();
    assert_eq!(
        engine.verify_digest_row(coll.id, ScanBudget::self_check()).unwrap(),
        crate::SelfCheck::Corrected
    );
    assert_eq!(engine.digest_row(coll.id).unwrap().unwrap().digest, kept, "corrected to it");
    check(&engine, "the self-check's correction");
}

/// `legacy_term` is the schema-4 term of a stored key, and never fails: bytes that are not a
/// canonical stored key, a schema-4 key among them, are hashed as they are.
#[test]
fn legacy_term_hashes_a_stored_key_as_schema_4_and_anything_else_as_it_is() {
    let stamp = Stamp::new(Hlc::new(5, 1), NodeId::from_bytes([3; 16]));
    for id in digest_ids() {
        let stored = crate::docs::doc_key(&id).unwrap();
        let want = v1::term(&v1::key(&id), &stamp);
        assert_eq!(crate::live_digest::legacy_term(&stored, &stamp), want, "{id:?}");
        let legacy = kimmy_core::keyenc::legacy_key(&id);
        assert_eq!(crate::live_digest::term(&legacy, &stamp), want, "{id:?}");
    }
    // A schema-4 integer key is not a stored key (12 bytes where a stored one is shorter,
    // or another id's): fed to it, it is hashed as it is, or as the stored key it happens to
    // be, and never refused.
    let five = v1::key(&int(5));
    assert_eq!(crate::live_digest::legacy_term(&five, &stamp), v1::term(&five, &stamp));
    let junk: [&[u8]; 7] =
        [&[], &[0x20], &[0x20, 0x04], &[0x20, 0x04, 0x80], &[0xFF], &[0x20, 0x07, 1], &[0x00]];
    for bytes in junk {
        assert_eq!(
            crate::live_digest::legacy_term(bytes, &stamp),
            v1::term(bytes, &stamp),
            "{bytes:02x?}"
        );
    }
    for v in [-3i64, 0, 5, 1_000, A, B, i64::MIN] {
        // Never panics, whatever the schema-4 key happens to read as.
        let _ = crate::live_digest::legacy_term(&v1::key(&int(v)), &stamp);
    }
}

/// The schema-4 keys of the documents `member`'s entries above `cut` name in `collection`:
/// the uncovered list a 0.50 member sends.
fn v1_uncovered(
    member: &Engine,
    cut: &VersionVector,
    collection: CollectionId,
) -> BTreeSet<Vec<u8>> {
    member
        .read_oplog_from(Hlc::ZERO, usize::MAX, WalkScope::Background)
        .unwrap()
        .into_iter()
        .filter(|e| e.collection == collection && e.stamp.hlc > cut.get(e.stamp.node))
        .filter_map(|e| e.doc_id.map(|id| v1::key(&id)))
        .collect()
}

/// The digest a 0.50 member takes of `collection` with `keys` set aside: its kept digest,
/// less the schema-4 term of each key it holds a live document under.
fn v1_excluded(member: &Engine, collection: CollectionId, keys: &BTreeSet<Vec<u8>>) -> u128 {
    let mut digest = v1_digest(member, collection);
    for key in keys {
        let stored = stored_of_v1(key).expect("a schema-4 key");
        if let Some((stamp, true)) = held_at(member, collection, &stored) {
            digest ^= v1::term(key, &stamp);
        }
    }
    digest
}

/// A 0.50 member's answer to `ask` for `collection`, as `digest_answer` built it there.
fn v1_answer(member: &Engine, ask: &DigestAsk, collection: CollectionId) -> DigestReply {
    let theirs: BTreeSet<Vec<u8>> = ask
        .uncovered
        .iter()
        .filter(|u| u.collection == collection)
        .flat_map(|u| u.keys.iter().map(|k| k.to_vec()))
        .collect();
    let own = v1_uncovered(member, &ask.cut, collection);
    let union: BTreeSet<Vec<u8>> = theirs.union(&own).cloned().collect();
    let entry = DigestEntry {
        collection,
        digest: ByteBuf::from(v1_excluded(member, collection, &union).to_be_bytes().to_vec()),
        applied: theirs.len() as u32,
        uncovered: own.into_iter().map(ByteBuf::from).collect(),
        deferred: None,
    };
    DigestReply { entries: vec![entry], malformed: 0 }
}

fn compared(verdicts: &[crate::DigestVerdict], id: CollectionId) -> Option<bool> {
    verdicts.iter().find(|v| v.collection == id).and_then(|v| match v.outcome {
        DigestOutcome::Compared { equal, .. } => Some(equal),
        DigestOutcome::Deferred(_) => None,
    })
}

/// Two members that agree up to a cut and each wrote above it: the same id on both (it is
/// set aside once, not twice), ids the other lacks, and the A2 pair split between them. A
/// probe between this build and a 0.50 member compares equal in both directions, and so
/// does one between two of this build.
#[test]
fn a_probe_with_integer_ids_above_the_cut_on_both_sides_compares_equal_in_both_directions() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let coll = a.create_collection("shop", "orders").unwrap();
    let ids: Vec<i64> = (1..=20).chain([A, B, A + 1]).collect();
    a.insert_many(&coll, ids.iter().map(|id| doc! { "_id": *id, "v": 0 }).collect()).unwrap();
    pull(&b, &a);
    pull(&a, &b);
    let bc = b.get_collection("shop", "orders").unwrap();
    // Above the cut: `a` is this build, `b` the 0.50 member's data.
    a.replace(&coll, &int(5), doc! { "_id": 5i64, "v": 1 }, false).unwrap();
    b.replace(&bc, &int(5), doc! { "_id": 5i64, "v": 2 }, false).unwrap();
    a.insert(&coll, doc! { "_id": 100i64 }).unwrap();
    b.insert(&bc, doc! { "_id": 200i64 }).unwrap();
    a.replace(&coll, &int(A), doc! { "_id": A, "v": 1 }, false).unwrap();
    b.replace(&bc, &int(B), doc! { "_id": B, "v": 1 }, false).unwrap();
    assert!(a.delete(&coll, &int(7)).unwrap());
    assert!(b.delete(&bc, &int(A + 1)).unwrap());
    let cut = a.witnessed_vector().unwrap().meet(&b.witnessed_vector().unwrap());
    let budget = ScanBudget::default();

    // This build asks, a 0.50 member answers.
    let plan = a.digest_plan(&b.witnessed_vector().unwrap(), Some(coll.id), budget).unwrap();
    assert_eq!(plan.cut, cut);
    let reply = v1_answer(&b, &plan.ask, coll.id);
    oracle::exchange(&a, &plan.ask, &b, &reply).unwrap();
    let sent: BTreeSet<Vec<u8>> = plan.ask.uncovered[0].keys.iter().map(|k| k.to_vec()).collect();
    assert_eq!(sent, v1_uncovered(&a, &cut, coll.id), "the request's keys are 0.50's");
    let verdicts = a.digest_compare(&plan, &reply, budget).unwrap();
    assert_eq!(compared(&verdicts, coll.id), Some(true), "0.51 asks 0.50: {verdicts:?}");

    // A 0.50 member asks, this build answers.
    let own = v1_uncovered(&b, &cut, coll.id);
    let ask = DigestAsk {
        cut: cut.clone(),
        uncovered: vec![Uncovered {
            collection: coll.id,
            keys: own.iter().cloned().map(ByteBuf::from).collect(),
        }],
        busy: Some(coll.id),
    };
    let reply = a.digest_answer(&ask, budget).unwrap().expect("answered");
    oracle::exchange(&b, &ask, &a, &reply).unwrap();
    let entry = reply.entries.iter().find(|e| e.collection == coll.id).expect("an entry");
    assert_eq!(entry.deferred, None);
    assert_eq!(entry.applied as usize, own.len(), "every key processed");
    let theirs: BTreeSet<Vec<u8>> = entry.uncovered.iter().map(|k| k.to_vec()).collect();
    assert_eq!(theirs, v1_uncovered(&a, &cut, coll.id), "the answer's keys are 0.50's");
    let union: BTreeSet<Vec<u8>> = own.union(&theirs).cloned().collect();
    let mine = v1_excluded(&b, coll.id, &union);
    let answered = u128::from_be_bytes(entry.digest.as_slice().try_into().unwrap());
    assert_eq!(answered, mine, "0.50 asks 0.51");

    // And between two of this build, each way.
    for (x, y) in [(&a, &b), (&b, &a)] {
        let plan = x.digest_plan(&y.witnessed_vector().unwrap(), Some(coll.id), budget).unwrap();
        let reply = y.digest_answer(&plan.ask, budget).unwrap().unwrap();
        oracle::exchange(x, &plan.ask, y, &reply).unwrap();
        let verdicts = x.digest_compare(&plan, &reply, budget).unwrap();
        assert_eq!(compared(&verdicts, coll.id), Some(true), "0.51 and 0.51: {verdicts:?}");
    }
}

/// The caps on uncovered keys count the bytes the peer reads: twelve a schema-4 integer key,
/// where the stored key is seven or eight.
#[test]
fn the_uncovered_caps_count_the_bytes_the_peer_reads() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let coll = a.create_collection("shop", "orders").unwrap();
    pull(&b, &a);
    let bc = b.get_collection("shop", "orders").unwrap();
    for id in 1_000..1_010i64 {
        a.insert(&coll, doc! { "_id": id }).unwrap();
        b.insert(&bc, doc! { "_id": id + 100 }).unwrap();
    }
    let wire = 10 * 12;
    let stored: usize =
        (1_000..1_010i64).map(|id| crate::docs::doc_key(&int(id)).unwrap().len()).sum();
    assert!(stored < wire, "premise: the stored keys are shorter ({stored})");
    let witnessed = b.witnessed_vector().unwrap();

    let budget = |ask_keys, answer_keys| ScanBudget { ask_keys, answer_keys, ..Default::default() };
    let plan = a.digest_plan(&witnessed, Some(coll.id), budget(wire - 1, wire)).unwrap();
    assert!(plan.ask.uncovered.is_empty(), "one byte under the wire bytes: not carried");
    assert_eq!(plan.deferred().collect::<Vec<_>>(), vec![(coll.id, crate::Defer::UncoveredCap)]);
    let plan = a.digest_plan(&witnessed, Some(coll.id), budget(wire, wire)).unwrap();
    assert_eq!(plan.ask.uncovered[0].keys.len(), 10, "exactly the wire bytes: carried");

    // The answer's own keys, the same way: `b` is busy with ten keys of its own.
    let ask = DigestAsk { cut: plan.cut.clone(), uncovered: Vec::new(), busy: Some(coll.id) };
    let reply = b.digest_answer(&ask, budget(wire, wire - 1)).unwrap().unwrap();
    assert_eq!(reply.entries[0].deferred, Some(crate::Defer::UncoveredCap));
    let reply = b.digest_answer(&ask, budget(wire, wire)).unwrap().unwrap();
    assert_eq!(reply.entries[0].deferred, None);
    assert_eq!(reply.entries[0].uncovered.len(), 10);
}

/// A drop stopped part-way through its purge, over `Int64` live rows and tombstones, with
/// rows a schema-4 store left keyed in schema 4 beside them (the totality of `legacy_term`,
/// design 7.3): a forced open walk over it completes, the purge finishes, the count and
/// digest rows go with the last chunk, and the name can be created again.
#[test]
fn a_drop_stopped_mid_purge_over_schema_4_rows_walks_and_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    let engine = Engine::open(&path).unwrap();
    let ids: Vec<i64> = (0..2_500).chain([A, B]).collect();
    let deleted: Vec<i64> = ids.iter().copied().filter(|id| id % 7 == 0).collect();
    let coll = holding(&engine, "orders", &ids, &deleted);
    let stamp = Stamp::new(Hlc::new(crate::physical_now_ms(), 0), NodeId::from_bytes([5; 16]));
    for id in 10_000..10_300i64 {
        let record = match id % 2 {
            0 => kimmy_core::DocRecord::live(
                stamp,
                bson::serialize_to_vec(&doc! { "_id": id }).unwrap(),
            ),
            _ => kimmy_core::DocRecord::tombstone(stamp),
        };
        put(&engine, coll.id, &v1::key(&int(id)), &record, false);
    }
    let all = ids.len() + 300;
    assert_eq!(count_rows(&engine, coll.id), all);
    engine.bury_collection("shop", "orders", None).unwrap().expect("dropped");
    assert_eq!(engine.purge_chunk(coll.id).unwrap(), crate::engine::DROP_PURGE_CHUNK);
    let left = count_rows(&engine, coll.id);
    assert!(left > 0 && left < all, "stopped part-way: {left}");
    engine.close().unwrap();

    let engine =
        Engine::open_with(&path, &crate::OpenOptions::new().force_rebuild("a test")).unwrap();
    assert_eq!(count_rows(&engine, coll.id), left, "the walk removed nothing");
    assert!(engine.digest_row(coll.id).unwrap().is_some(), "and rebuilt the row over them");
    engine.finish_purges_now().unwrap();
    assert_eq!(count_rows(&engine, coll.id), 0, "the purge finished");
    assert_eq!(engine.digest_row(coll.id).unwrap(), None, "the digest row is forgotten");
    {
        let txn = engine.db().begin_read().unwrap();
        let counts = txn.open_table(tables::LIVE_COUNTS).unwrap();
        assert!(counts.get(coll.id.0).unwrap().is_none(), "the count row is forgotten");
    }
    let again = engine.create_collection("shop", "orders").unwrap();
    assert_eq!(again.id, coll.id);
    assert_eq!(engine.count(&again, WalkScope::Request).unwrap(), 0);
}

// -- Where the conversions are --------------------------------------------------------------

/// The product source of every file of this crate, as `(file, function, body)`: test modules
/// and test files left out, split at every `fn` item.
fn product_functions() -> Vec<(String, String, String)> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "rs"))
        .filter(|path| !path.to_string_lossy().ends_with("_tests.rs"))
        .collect();
    files.sort();
    let mut out = Vec::new();
    for path in files {
        let file = path.file_name().unwrap().to_string_lossy().into_owned();
        let text = std::fs::read_to_string(&path).unwrap();
        let product = text.split("\n#[cfg(test)]\nmod tests").next().unwrap();
        let mut name = String::from("(file scope)");
        let mut body = String::new();
        for line in product.lines() {
            let item = line.trim_start();
            let item = ["pub(crate) ", "pub(super) ", "pub "]
                .iter()
                .find_map(|vis| item.strip_prefix(vis))
                .unwrap_or(item);
            if let Some(rest) = item.strip_prefix("fn ") {
                out.push((file.clone(), std::mem::take(&mut name), std::mem::take(&mut body)));
                name = rest.split(['(', '<']).next().unwrap().to_string();
            }
            // Comments say what the code does; only the code is held to the rule.
            let code = line.split("//").next().unwrap();
            body.push_str(code);
            body.push('\n');
        }
        out.push((file, name, body));
    }
    out
}

/// The functions of this crate's product code whose code contains `needle`.
fn using(needle: &str) -> BTreeSet<(String, String)> {
    product_functions()
        .into_iter()
        .filter(|(_, _, body)| body.contains(needle))
        .map(|(file, name, _)| (file, name))
        .collect()
}

fn sites(list: &[(&str, &str)]) -> BTreeSet<(String, String)> {
    list.iter().map(|(file, name)| (file.to_string(), name.to_string())).collect()
}

/// Every conversion between the stored and the legacy form, and every read of a received
/// key, is where design 7.1 and 7.3 put it, and nowhere else: a new reader of a page's
/// carried deletes, a cursor converted on receipt, or a fold site that hashes another form
/// fails here before it fails in a mixed roll.
#[test]
fn the_conversions_are_at_the_wire_api_and_the_fold_sites_and_nowhere_else() {
    // The received keys, read once, at the receiver's first act.
    assert_eq!(using(".deleted_documents"), sites(&[("snapshot.rs", "of_page")]));
    // Legacy to stored: the request's cursor, the carried deletes, an uncovered key's point
    // read.
    assert_eq!(
        using("stored_of_legacy("),
        sites(&[
            ("snapshot.rs", "snapshot_page"),
            ("snapshot.rs", "of_page"),
            ("digest_probe.rs", "excluded_digest"),
        ])
    );
    // Stored to legacy: what a page carries, and every fold site.
    assert_eq!(
        using("legacy_of_stored("),
        sites(&[
            ("snapshot.rs", "snapshot_page"),
            ("live_count.rs", "wrote"),
            ("live_count.rs", "assert_digests_exact"),
            ("live_walk.rs", "run"),
            ("digest_probe.rs", "walk_headers"),
            ("live_digest.rs", "legacy_term"),
        ])
    );
    // The uncovered keys, legacy from the id itself.
    assert_eq!(using("legacy_key("), sites(&[("digest_probe.rs", "scan_uncovered")]));
    // Received uncovered keys, legacy by definition.
    assert_eq!(
        using("LegacyKey::from_wire("),
        sites(&[("digest_probe.rs", "digest_answer"), ("digest_probe.rs", "digest_compare")])
    );
    // The term is taken in its own module and nowhere else.
    assert_eq!(using("live_digest::term("), sites(&[]));
    assert_eq!(
        using("legacy_term("),
        sites(&[("digest_probe.rs", "excluded_digest"), ("live_digest.rs", "legacy_term")])
    );
}

/// Neither digest's format moves (design 7.3 and 8, review m6): the cut digest's rows a 0.50
/// member kept are the rows this build keeps, since every term is over the legacy key, and a
/// definition digest hashes no key at all. A bump would defer every comparison (`format`)
/// between a 0.50 and a 0.51 member for the whole roll, and rebuild every kept row at the
/// first start of 0.51 for nothing.
#[test]
fn neither_digest_format_moves() {
    assert_eq!(crate::live_digest::FORMAT, 1, "the cut digest's format");
    assert_eq!(crate::definition_digest::FORMAT, 2, "the definition digest's format");
}

/// A `Decimal128` id has no schema-4 key (design 7.1): its carried delete and a cursor that
/// ends at it travel as `FF` and the stored key, the receiver lands the delete on it, the
/// sender resumes after it, and the digest hashes the same marked form.
#[test]
fn a_decimal_id_travels_marked_and_lands_on_its_own_document() {
    let decimal =
        |text: &str| DocId::try_from_bson(&bson::Bson::Decimal128(text.parse().unwrap())).unwrap();
    let (s, _ds) = engine();
    let (r, _dr) = engine();
    let coll = s.create_collection("shop", "orders").unwrap();
    let ids = [int(1), decimal("1.5"), decimal("2.5"), int(3)];
    for id in &ids {
        s.insert(&coll, doc! { "_id": id.to_bson() }).unwrap();
    }
    pull(&r, &s);
    assert!(s.delete(&coll, &decimal("1.5")).unwrap());
    let marked = |id: &DocId| {
        let mut out = vec![0xFF];
        out.extend(crate::docs::doc_key(id).unwrap());
        out
    };

    let page = s.snapshot_page(None, Some(coll.id)).unwrap();
    let carried: Vec<Vec<u8>> = page.deleted_documents.iter().map(|t| t.key.clone()).collect();
    assert_eq!(carried, vec![marked(&decimal("1.5"))]);
    let mut progress = SnapshotProgress::of_collection(coll.id);
    r.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
    let rc = r.get_collection("shop", "orders").unwrap();
    assert!(r.get(&rc, &decimal("1.5")).unwrap().is_none(), "the delete landed");
    for id in [int(1), decimal("2.5"), int(3)] {
        assert!(r.get(&rc, &id).unwrap().is_some(), "{id:?} untouched");
    }

    // A cursor ending at a decimal id is marked, and the sender resumes after it.
    let after = SnapshotCursor { collection: coll.id, after_key: marked(&decimal("2.5")) };
    let rest = s.snapshot_page(Some(after), Some(coll.id)).unwrap();
    assert_eq!(rest.documents.iter().map(|d| d.id.clone()).collect::<Vec<_>>(), vec![int(3)]);

    // The digest's term of a decimal id is over the marked form.
    let stamp =
        |e: &Engine, id: &DocId| held_at(e, coll.id, &crate::docs::doc_key(id).unwrap()).unwrap().0;
    let want = [int(1), decimal("2.5"), int(3)].iter().fold(0u128, |digest, id| {
        let key = match id {
            DocId::Decimal128(_) => marked(id),
            other => v1::key(other),
        };
        digest ^ v1::term(&key, &stamp(&s, id))
    });
    assert_eq!(s.digest_row(coll.id).unwrap().unwrap().digest, want);
    assert_eq!(r.digest_row(rc.id).unwrap().unwrap().digest, want);
}
