//! The schema 5 migration's tests (ADR-238, design section 6 and 10 item 7).
//!
//! **The fixture is a schema 4 store built by hand**, with a frozen schema 4
//! encoder written here ([`v1`]) and raw redb writes: the store's documents are
//! first written by this build's engine, which is the *reference* every
//! migrated store is compared with, and a copy is then taken back to what a
//! 0.50 node leaves -- every numeric document key in its schema 4 form, every
//! index's entries derived as 0.50 derived them (a `Decimal128` unkeyable, a
//! decimal at a partial filter's path compared as equal to every number),
//! schema 4 in META and in the sidecar, written by 0.50.0. Neither the live
//! encoder nor `kimmy_core::legacy_key` builds a byte of it; the two are
//! checked against the frozen one instead.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use bson::{Bson, Document, doc};
use kimmy_core::{CollectionId, DocId, PartialFilter, PartialOp, canonical_cmp, path};
use redb::{Database, ReadableDatabase, ReadableTable};

use crate::Engine;
use crate::codec;
use crate::meta::{CollectionMeta, Enforcement, IndexField, IndexMeta};
use crate::tables;

/// The schema 4 key encoding, frozen: `keyenc` and `cmp::decompose` as v0.50.0
/// has them, ported line for line and sharing nothing with the live encoder.
pub(crate) mod v1 {
    use bson::{Bson, Document};

    use super::*;

    const TAG_MIN_KEY: u8 = 0x01;
    const TAG_NULL: u8 = 0x10;
    const TAG_NUMBER: u8 = 0x20;
    const TAG_STRING: u8 = 0x30;
    const TAG_DOCUMENT: u8 = 0x40;
    const TAG_ARRAY: u8 = 0x50;
    const TAG_BINARY: u8 = 0x60;
    const TAG_OBJECT_ID: u8 = 0x70;
    const TAG_BOOL: u8 = 0x80;
    const TAG_DATE: u8 = 0x90;
    const TAG_TIMESTAMP: u8 = 0xA0;
    const TAG_REGEX: u8 = 0xB0;
    const TAG_DB_POINTER: u8 = 0xC0;
    const TAG_JS_CODE: u8 = 0xD0;
    const TAG_JS_CODE_WITH_SCOPE: u8 = 0xE0;
    const TAG_MAX_KEY: u8 = 0xF0;

    enum Numeric {
        Nan,
        NegInfinity,
        Finite { negative: bool, exp: i16, mantissa: u64 },
        Zero,
        PosInfinity,
    }

    fn from_i64(v: i64) -> Numeric {
        if v == 0 {
            return Numeric::Zero;
        }
        let magnitude = v.unsigned_abs();
        let shift = magnitude.leading_zeros();
        Numeric::Finite { negative: v < 0, exp: 63 - shift as i16, mantissa: magnitude << shift }
    }

    fn from_f64(v: f64) -> Numeric {
        if v.is_nan() {
            return Numeric::Nan;
        }
        if v == f64::INFINITY {
            return Numeric::PosInfinity;
        }
        if v == f64::NEG_INFINITY {
            return Numeric::NegInfinity;
        }
        if v == 0.0 {
            return Numeric::Zero;
        }
        let bits = v.to_bits();
        let negative = bits >> 63 == 1;
        let exponent_field = ((bits >> 52) & 0x7FF) as i32;
        let fraction = bits & ((1u64 << 52) - 1);
        let (significand, power) = if exponent_field == 0 {
            (fraction, -1074i32)
        } else {
            ((1u64 << 52) | fraction, exponent_field - 1075)
        };
        let shift = significand.leading_zeros();
        Numeric::Finite {
            negative,
            exp: (power - shift as i32 + 63) as i16,
            mantissa: significand << shift,
        }
    }

    fn encode_numeric(n: Numeric, out: &mut Vec<u8>) {
        match n {
            Numeric::Nan => out.push(0x00),
            Numeric::NegInfinity => out.push(0x01),
            Numeric::Zero => out.push(0x03),
            Numeric::PosInfinity => out.push(0x05),
            Numeric::Finite { negative, exp, mantissa } => {
                out.push(if negative { 0x02 } else { 0x04 });
                let biased = (exp as i32 + 32_768) as u16;
                let mut body = [0u8; 10];
                body[..2].copy_from_slice(&biased.to_be_bytes());
                body[2..].copy_from_slice(&mantissa.to_be_bytes());
                if negative {
                    for byte in &mut body {
                        *byte = !*byte;
                    }
                }
                out.extend_from_slice(&body);
            }
        }
    }

    fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
        for &byte in bytes {
            if byte == 0x00 {
                out.extend_from_slice(&[0x00, 0xFF]);
            } else {
                out.push(byte);
            }
        }
        out.extend_from_slice(&[0x00, 0x00]);
    }

    /// `None` where 0.50 refused: a `Decimal128` anywhere in the value.
    fn encode_into(value: &Bson, out: &mut Vec<u8>) -> Option<()> {
        match value {
            Bson::MinKey => out.push(TAG_MIN_KEY),
            Bson::MaxKey => out.push(TAG_MAX_KEY),
            Bson::Null | Bson::Undefined => out.push(TAG_NULL),
            Bson::Int32(v) => {
                out.push(TAG_NUMBER);
                encode_numeric(from_i64(i64::from(*v)), out);
            }
            Bson::Int64(v) => {
                out.push(TAG_NUMBER);
                encode_numeric(from_i64(*v), out);
            }
            Bson::Double(v) => {
                out.push(TAG_NUMBER);
                encode_numeric(from_f64(*v), out);
            }
            Bson::Decimal128(_) => return None,
            Bson::String(s) | Bson::Symbol(s) => {
                out.push(TAG_STRING);
                encode_bytes(s.as_bytes(), out);
            }
            Bson::Document(d) => {
                out.push(TAG_DOCUMENT);
                for (key, val) in d {
                    out.push(0x01);
                    encode_bytes(key.as_bytes(), out);
                    encode_into(val, out)?;
                }
                out.push(0x00);
            }
            Bson::Array(a) => {
                out.push(TAG_ARRAY);
                for element in a {
                    out.push(0x01);
                    encode_into(element, out)?;
                }
                out.push(0x00);
            }
            Bson::Binary(b) => {
                out.push(TAG_BINARY);
                out.extend_from_slice(&(b.bytes.len() as u64).to_be_bytes());
                out.push(u8::from(b.subtype));
                out.extend_from_slice(&b.bytes);
            }
            Bson::ObjectId(oid) => {
                out.push(TAG_OBJECT_ID);
                out.extend_from_slice(&oid.bytes());
            }
            Bson::Boolean(v) => {
                out.push(TAG_BOOL);
                out.push(u8::from(*v));
            }
            Bson::DateTime(dt) => {
                out.push(TAG_DATE);
                out.extend_from_slice(
                    &((dt.timestamp_millis() as u64) ^ (1u64 << 63)).to_be_bytes(),
                );
            }
            Bson::Timestamp(ts) => {
                out.push(TAG_TIMESTAMP);
                out.extend_from_slice(&ts.time.to_be_bytes());
                out.extend_from_slice(&ts.increment.to_be_bytes());
            }
            Bson::RegularExpression(re) => {
                out.push(TAG_REGEX);
                encode_bytes(re.pattern.as_str().as_bytes(), out);
                encode_bytes(re.options.as_str().as_bytes(), out);
            }
            Bson::DbPointer(_) => out.push(TAG_DB_POINTER),
            Bson::JavaScriptCode(code) => {
                out.push(TAG_JS_CODE);
                encode_bytes(code.as_bytes(), out);
            }
            Bson::JavaScriptCodeWithScope(cws) => {
                out.push(TAG_JS_CODE_WITH_SCOPE);
                encode_bytes(cws.code.as_bytes(), out);
                encode_into(&Bson::Document(cws.scope.clone()), out)?;
            }
        }
        Some(())
    }

    pub(crate) fn encode(value: &Bson) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        encode_into(value, &mut out)?;
        Some(out)
    }

    fn encode_compound_ordered(components: &[(Bson, bool)]) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        for (value, descending) in components {
            let start = out.len();
            encode_into(value, &mut out)?;
            if *descending {
                for byte in &mut out[start..] {
                    *byte = !*byte;
                }
            }
        }
        Some(out)
    }

    /// The schema 4 key of an `Int64` id.
    pub(crate) fn int64(v: i64) -> Vec<u8> {
        encode(&Bson::Int64(v)).expect("an integer encodes")
    }

    /// The schema 5 key of an `Int64` id, from the design's layout (4.1) and
    /// written here, so the fixture can find rows without the live encoder.
    pub(crate) fn v2_int64(v: i64) -> Vec<u8> {
        if v == 0 {
            return vec![0x20, 0x03];
        }
        let magnitude = v.unsigned_abs();
        let exp = 63 - magnitude.leading_zeros() as i32;
        let mut m = ((magnitude as u128) << (127 - exp)).to_be_bytes().to_vec();
        while m.last() == Some(&0) {
            m.pop();
        }
        let mut body = ((exp + 32_768) as u16).to_be_bytes().to_vec();
        for b in m {
            body.push(b);
            if b == 0 {
                body.push(0xFF);
            }
        }
        body.extend_from_slice(&[0x00, 0x00]);
        if v < 0 {
            for b in &mut body {
                *b = !*b;
            }
        }
        let mut out = vec![0x20, if v < 0 { 0x02 } else { 0x04 }];
        out.extend(body);
        out
    }

    /// How 0.50 compared two values a partial filter met: a `Decimal128`
    /// against any number was `Equal`, which is the defect ADR-238 fixes.
    fn old_cmp(a: &Bson, b: &Bson) -> Ordering {
        let number = |v: &Bson| {
            matches!(v, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_))
        };
        let decimal = |v: &Bson| matches!(v, Bson::Decimal128(_));
        if number(a) && number(b) && (decimal(a) || decimal(b)) {
            Ordering::Equal
        } else {
            canonical_cmp(a, b)
        }
    }

    fn group(v: &Bson) -> u8 {
        match v {
            Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Decimal128(_) => 1,
            Bson::String(_) | Bson::Symbol(_) => 2,
            Bson::Document(_) => 3,
            Bson::Array(_) => 4,
            Bson::Binary(_) => 5,
            Bson::ObjectId(_) => 6,
            Bson::Boolean(_) => 7,
            Bson::DateTime(_) => 8,
            Bson::Timestamp(_) => 9,
            Bson::Null | Bson::Undefined => 10,
            _ => 11,
        }
    }

    fn any_element(values: &[&Bson], predicate: impl Fn(&Bson) -> bool) -> bool {
        values.iter().any(|value| {
            predicate(value) || matches!(value, Bson::Array(items) if items.iter().any(&predicate))
        })
    }

    /// What a 0.50 partial filter selected, for the scalar decimals the
    /// fixture puts at filter paths (it refuses a decimal inside a composite
    /// there, which this does not model).
    pub(crate) fn old_selects(filter: &PartialFilter, doc: &Document) -> bool {
        filter.predicates().all(|(field, op)| {
            let values = path::resolve(doc, field);
            for v in &values {
                if matches!(v, Bson::Array(_) | Bson::Document(_)) {
                    assert!(
                        !kimmy_core::holds_decimal128(v),
                        "the fixture keeps decimals at a filter path scalar"
                    );
                }
            }
            let compares = |bound: &Bson, accept: &[Ordering]| {
                any_element(&values, |v| {
                    group(v) == group(bound) && accept.contains(&old_cmp(v, bound))
                })
            };
            match op {
                PartialOp::Exists => !values.is_empty(),
                PartialOp::Eq(Bson::Null) => {
                    values.is_empty() || any_element(&values, |v| matches!(v, Bson::Null))
                }
                PartialOp::Eq(want) => {
                    any_element(&values, |v| old_cmp(v, want) == Ordering::Equal)
                }
                PartialOp::Gt(b) => compares(b, &[Ordering::Greater]),
                PartialOp::Gte(b) => compares(b, &[Ordering::Greater, Ordering::Equal]),
                PartialOp::Lt(b) => compares(b, &[Ordering::Less]),
                PartialOp::Lte(b) => compares(b, &[Ordering::Less, Ordering::Equal]),
            }
        })
    }

    /// What a 0.50 index derived from a document.
    #[derive(Debug, PartialEq, Eq)]
    pub(crate) enum Keys {
        Keyed(Vec<Vec<u8>>, bool),
        Unkeyed(bool),
        Undecidable,
    }

    /// `index::document_keys` as v0.50.0 has it, over the frozen encoder.
    pub(crate) fn document_keys(index: &IndexMeta, doc: &Document) -> Keys {
        if let Some(filter) = index.partial() {
            let filter = filter.expect("the fixture's filters parse");
            if !old_selects(&filter, doc) {
                let undecidable = filter.predicates().any(|(field, _)| {
                    path::resolve(doc, field).iter().any(|v| kimmy_core::holds_decimal128(v))
                });
                return if undecidable {
                    Keys::Undecidable
                } else {
                    Keys::Keyed(Vec::new(), false)
                };
            }
        }
        let mut per_field: Vec<Vec<Bson>> = Vec::new();
        let (mut array_fields, mut multikey) = (0, false);
        for field in &index.fields {
            let resolved = path::resolve(doc, &field.path);
            let mut values: Vec<Bson> = Vec::new();
            multikey |= resolved.len() > 1;
            if resolved.is_empty() {
                values.push(Bson::Null);
            } else {
                for value in resolved {
                    if let Bson::Array(items) = value {
                        array_fields += 1;
                        multikey = true;
                        values.extend(items.iter().cloned());
                        values.push(value.clone());
                    } else {
                        values.push(value.clone());
                    }
                }
            }
            values.dedup_by(|a, b| old_cmp(a, b) == Ordering::Equal);
            per_field.push(values);
        }
        if array_fields > 1 && index.fields.len() > 1 {
            return Keys::Unkeyed(multikey);
        }
        let mut keys: Vec<Vec<(Bson, bool)>> = vec![Vec::new()];
        for (field, values) in index.fields.iter().zip(per_field) {
            let mut next = Vec::new();
            for prefix in &keys {
                for value in &values {
                    let mut combined = prefix.clone();
                    combined.push((value.clone(), field.descending));
                    next.push(combined);
                }
            }
            keys = next;
            if keys.len() > 1_000 {
                return Keys::Unkeyed(multikey);
            }
        }
        let mut encoded = Vec::new();
        for key in &keys {
            match encode_compound_ordered(key) {
                Some(bytes) => encoded.push(bytes),
                None => return Keys::Unkeyed(multikey),
            }
        }
        encoded.sort();
        encoded.dedup();
        Keys::Keyed(encoded, multikey)
    }
}

/// The `i64` a schema 5 integer key encodes, read from the design's layout
/// (4.1) by this module alone: what the I-DOCS check reads a staged or
/// moved-back key with. `None` for anything that is not exactly such a key.
fn v2_int64_of(key: &[u8]) -> Option<i64> {
    let (sub, body) = match key {
        [0x20, 0x03] => return Some(0),
        [0x20, sub @ (0x02 | 0x04), body @ ..] => (*sub, body),
        _ => return None,
    };
    let negative = sub == 0x02;
    let body: Vec<u8> = body.iter().map(|b| if negative { !b } else { *b }).collect();
    let exp = i32::from(u16::from_be_bytes([*body.first()?, *body.get(1)?])) - 32_768;
    let (mut mantissa, mut i) = (Vec::new(), 2);
    loop {
        match (*body.get(i)?, body.get(i + 1).copied()) {
            (0, Some(0)) => break,
            (0, Some(0xFF)) => {
                mantissa.push(0);
                i += 2;
            }
            (0, _) => return None,
            (b, _) => {
                mantissa.push(b);
                i += 1;
            }
        }
    }
    if i + 2 != body.len() || mantissa.is_empty() || mantissa.len() > 16 {
        return None;
    }
    if !(0..=63).contains(&exp) || mantissa[0] >> 7 == 0 {
        return None;
    }
    let mut padded = [0u8; 16];
    padded[..mantissa.len()].copy_from_slice(&mantissa);
    let m = u128::from_be_bytes(padded);
    let fraction = (127 - exp) as u32;
    if m & ((1u128 << fraction) - 1) != 0 {
        return None;
    }
    let magnitude = m >> fraction;
    if negative {
        if magnitude == 1u128 << 63 {
            Some(i64::MIN)
        } else {
            i64::try_from(magnitude).ok().map(|v| -v)
        }
    } else {
        i64::try_from(magnitude).ok()
    }
}

/// The two ids whose keys collide across the schemas (design 6.1): the schema 5
/// key of the first is the canonical schema 4 key of the second.
const A2: (i64, i64) = (0x5000_0081_0180_0000, 0x5000_7F80_8101_8000);

/// A store and its sidecar, copied.
fn copy_store(from: &Path, to: &Path) {
    std::fs::copy(from, to).unwrap();
    std::fs::copy(crate::format::sidecar_path(from), crate::format::sidecar_path(to)).unwrap();
}

/// The schema 4 store this module migrates, and the schema 5 store the same
/// writes made with this build's engine, which the migrated one must equal.
pub(crate) struct Fixture {
    dir: tempfile::TempDir,
    /// Written by this build's engine.
    pub(crate) reference: PathBuf,
    /// The same store, taken back to schema 4 by hand. Never opened by an
    /// engine: each test migrates a copy.
    v1: PathBuf,
    /// Every `Int64` id written, live or deleted, by collection id.
    pub(crate) ids: BTreeMap<u64, BTreeSet<i64>>,
    /// The collection a drop left rows under.
    pub(crate) gone: CollectionId,
    /// Each index's `multikey` as 0.50 would have set it.
    old_multikey: BTreeMap<(u64, u32), bool>,
    copies: std::cell::Cell<usize>,
}

impl Fixture {
    /// A fresh copy of the schema 4 store.
    pub(crate) fn copy(&self) -> PathBuf {
        let n = self.copies.get() + 1;
        self.copies.set(n);
        let path = self.dir.path().join(format!("copy{n}.redb"));
        copy_store(&self.v1, &path);
        path
    }
}

fn index(engine: &Engine, coll: &str, name: &str, fields: Vec<IndexField>, unique: bool) {
    engine
        .create_index_with(
            "shop",
            coll,
            fields,
            unique,
            Enforcement::Local,
            Some(name.into()),
            None,
            None,
        )
        .unwrap();
}

fn partial(
    engine: &Engine,
    coll: &str,
    name: &str,
    fields: Vec<IndexField>,
    unique: bool,
    ttl: Option<i64>,
    filter: Document,
) {
    engine
        .create_index_with(
            "shop",
            coll,
            fields,
            unique,
            Enforcement::Local,
            Some(name.into()),
            ttl,
            Some(filter),
        )
        .unwrap();
}

fn dec(s: &str) -> Bson {
    Bson::Decimal128(s.parse().unwrap())
}

/// The ids of `shop.items`: the whole `i64` range's edges, the A2 pair, and a
/// run long enough to cross many chunks.
fn item_ids() -> Vec<i64> {
    let mut ids = vec![
        i64::MIN,
        i64::MIN + 1,
        -(1 << 62),
        -257,
        -1,
        0,
        1,
        2,
        5,
        255,
        256,
        257,
        65_535,
        65_536,
        (1 << 53) + 1,
        1 << 62,
        (1 << 62) + 1,
        i64::MAX - 1,
        i64::MAX,
        A2.0,
        A2.1,
    ];
    ids.extend(1_000..1_060);
    ids
}

/// One document of `shop.items`.
fn item(id: i64) -> Document {
    let doubles = [-0.0, 0.5, 1e300, -1e-300, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 5e-324];
    let at = bson::DateTime::from_millis(1_000_000 + id.rem_euclid(1_000));
    let mut d = doc! {
        "_id": id,
        "n": id.rem_euclid(17),
        "d": doubles[id.rem_euclid(doubles.len() as i64) as usize],
        "nested": {"a": {"b": id.rem_euclid(13) as f64 / 4.0}},
        "tags": [id.rem_euclid(3), id.rem_euclid(5), 2.5],
        "email": format!("e{id}"),
        "s": format!("s{}", id.rem_euclid(7)),
        "at": at,
        "at2": at,
        "tier": 1 + id.rem_euclid(4),
    };
    match id {
        // An unkeyed document on 0.50: a decimal at indexed paths.
        1_001 => {
            d.insert("n", dec("2.5"));
        }
        // The partial unique index: a decimal at its filter's path that 0.50
        // could not decide, one the exact comparator selects and one it does not.
        1_020 => {
            d.insert("email2", "solo");
            d.insert("balance", dec("10"));
        }
        1_021 => {
            d.insert("email2", "neg");
            d.insert("balance", dec("-5"));
        }
        1_022 => {
            d.insert("email2", "plain");
            d.insert("balance", 7);
        }
        // TTL: 0.50 ranked a decimal equal to 3, so `$lt: 3` did not select it
        // and `$gte: 3` did; exactly, both answers turn round.
        1_030 => {
            d.insert("tier", dec("1"));
        }
        // Unchanged: 5 is not below 3 and is at least 3, both ways.
        1_031 => {
            d.insert("tier", dec("5"));
        }
        _ => {}
    }
    d
}

/// Build the fixture: the reference with this build's engine, then a copy taken
/// back to schema 4.
pub(crate) fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let reference = dir.path().join("reference.redb");
    let mut ids: BTreeMap<u64, BTreeSet<i64>> = BTreeMap::new();
    let gone;
    {
        let engine = Engine::open(&reference).unwrap();
        let items = engine.create_collection("shop", "items").unwrap();
        index(&engine, "items", "by_n", vec![IndexField::ascending("n")], false);
        index(&engine, "items", "by_d", vec![IndexField::ascending("d")], false);
        index(&engine, "items", "by_nested", vec![IndexField::ascending("nested.a.b")], false);
        index(&engine, "items", "by_obj", vec![IndexField::ascending("nested")], false);
        index(&engine, "items", "by_tags", vec![IndexField::ascending("tags")], false);
        index(&engine, "items", "email_once", vec![IndexField::ascending("email")], true);
        index(
            &engine,
            "items",
            "by_n_s",
            vec![IndexField::ascending("n"), IndexField::descending("s")],
            false,
        );
        partial(
            &engine,
            "items",
            "money_once",
            vec![IndexField::ascending("email2")],
            true,
            None,
            doc! {"balance": {"$gt": 0}},
        );
        partial(
            &engine,
            "items",
            "ttl_lt",
            vec![IndexField::ascending("at")],
            false,
            Some(3_600),
            doc! {"tier": {"$lt": 3}},
        );
        partial(
            &engine,
            "items",
            "ttl_gte",
            vec![IndexField::ascending("at2")],
            false,
            Some(3_600),
            doc! {"tier": {"$gte": 3}},
        );
        let all = item_ids();
        engine.insert_many(&items, all.iter().map(|&id| item(id)).collect()).unwrap();
        for id in [i64::MIN + 1, 256, 1_003, 1_010, 1_050] {
            assert!(engine.delete(&items, &DocId::Int64(id)).unwrap());
        }
        ids.insert(items.id.0, all.into_iter().collect());

        // A drop stopped mid-purge: its rows under an id nothing resolves.
        let doomed = engine.create_collection("shop", "gone").unwrap();
        index(&engine, "gone", "by_n", vec![IndexField::ascending("n")], false);
        engine
            .insert_many(&doomed, (1..40_i64).map(|i| doc! {"_id": i, "n": i * 3}).collect())
            .unwrap();
        for id in [3, 7] {
            engine.delete(&doomed, &DocId::Int64(id)).unwrap();
        }
        assert!(engine.drop_collection("shop", "gone").unwrap());
        assert!(engine.rows_under(doomed.id).unwrap() > 0, "premise: the drop left its rows");
        ids.insert(doomed.id.0, (1..40).collect());
        gone = doomed.id;

        // String ids: untouched by the re-key, their index rebuilt.
        let strs = engine.create_collection("shop", "strs").unwrap();
        index(&engine, "strs", "by_n", vec![IndexField::ascending("n")], false);
        engine
            .insert_many(&strs, (0..9_i64).map(|i| doc! {"_id": format!("k{i}"), "n": i}).collect())
            .unwrap();

        // An index over no documents.
        engine.create_collection("shop", "empty").unwrap();
        index(&engine, "empty", "by_x", vec![IndexField::ascending("x")], false);
    }
    let v1 = dir.path().join("kimmy.redb");
    copy_store(&reference, &v1);
    let old_multikey = to_schema_4(&v1, &ids);
    Fixture { dir, reference, v1, ids, gone, old_multikey, copies: std::cell::Cell::new(0) }
}

/// Take a store this build wrote back to what a 0.50 node leaves, by hand, and
/// answer each index's `multikey` as 0.50 would have set it.
///
/// Every numeric row of `DOCS` goes out and comes back under its schema 4 key
/// (all out first, so no schema 4 key lands on a schema 5 row it equals); every
/// standing collection's indexes are derived again as 0.50 derived them; a
/// dropped collection's entries keep their values and take the schema 4 form of
/// their document key, since nothing but a range removal ever reads them.
fn to_schema_4(path: &Path, ids: &BTreeMap<u64, BTreeSet<i64>>) -> BTreeMap<(u64, u32), bool> {
    let by_v2: BTreeMap<(u64, Vec<u8>), i64> = ids
        .iter()
        .flat_map(|(c, set)| set.iter().map(move |&id| ((*c, v1::v2_int64(id)), id)))
        .collect();
    let mut old_multikey = BTreeMap::new();
    let db = Database::create(path).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut docs = txn.open_table(tables::DOCS).unwrap();
        let rows: Vec<(u64, Vec<u8>, Vec<u8>)> = docs
            .iter()
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value().0, k.value().1.to_vec(), v.value().to_vec())
            })
            .collect();
        let mut converted = 0;
        for (c, key, _) in &rows {
            if key[0] == 0x20 {
                docs.remove((*c, key.as_slice())).unwrap();
            }
        }
        for (c, key, value) in &rows {
            if key[0] == 0x20 {
                let id = by_v2[&(*c, key.clone())];
                docs.insert((*c, v1::int64(id).as_slice()), value.as_slice()).unwrap();
                converted += 1;
            }
        }
        assert_eq!(
            converted,
            ids.values().map(BTreeSet::len).sum::<usize>(),
            "premise: every numeric row is a fixture id, deleted or not"
        );
        let v1_key = |c: u64, key: &[u8]| -> Vec<u8> {
            if key[0] == 0x20 { v1::int64(by_v2[&(c, key.to_vec())]) } else { key.to_vec() }
        };
        let mut collections = txn.open_table(tables::COLLECTIONS).unwrap();
        let metas: Vec<CollectionMeta> = collections
            .iter()
            .unwrap()
            .map(|r| serde_json::from_slice(r.unwrap().1.value()).unwrap())
            .collect();
        let mut entries = txn.open_table(tables::INDEX_ENTRIES).unwrap();
        let standing: BTreeSet<u64> = metas.iter().map(|m| m.id.0).collect();
        // A dropped collection's entries: the document key's schema 4 form.
        let orphans: Vec<(u64, u32, Vec<u8>, Vec<u8>)> = entries
            .iter()
            .unwrap()
            .map(|r| {
                let (key, _) = r.unwrap();
                let (c, i, k, d) = key.value();
                (c, i, k.to_vec(), d.to_vec())
            })
            .filter(|(c, ..)| !standing.contains(c))
            .collect();
        for (c, i, k, d) in &orphans {
            entries.remove((*c, *i, k.as_slice(), d.as_slice())).unwrap();
        }
        for (c, i, k, d) in &orphans {
            entries.insert((*c, *i, k.as_slice(), v1_key(*c, d).as_slice()), ()).unwrap();
        }
        for mut meta in metas {
            let all: Vec<(u64, u32, Vec<u8>, Vec<u8>)> = entries
                .range(crate::engine::index_range(meta.id))
                .unwrap()
                .map(|r| {
                    let (key, _) = r.unwrap();
                    let (c, i, k, d) = key.value();
                    (c, i, k.to_vec(), d.to_vec())
                })
                .collect();
            for (c, i, k, d) in &all {
                entries.remove((*c, *i, k.as_slice(), d.as_slice())).unwrap();
            }
            let docs_of: Vec<(Vec<u8>, Document)> = docs
                .range(crate::engine::doc_range(meta.id))
                .unwrap()
                .filter_map(|r| {
                    let (k, v) = r.unwrap();
                    let doc = codec::decode_doc_record(v.value()).unwrap().document().unwrap()?;
                    Some((k.value().1.to_vec(), doc))
                })
                .collect();
            for index in &mut meta.indexes {
                let mut multikey = false;
                for (key, doc) in &docs_of {
                    // Already schema 4: the rows were converted above.
                    match v1::document_keys(index, doc) {
                        v1::Keys::Keyed(keys, many) => {
                            multikey |= many;
                            for k in keys {
                                entries
                                    .insert((meta.id.0, index.id, k.as_slice(), key.as_slice()), ())
                                    .unwrap();
                            }
                        }
                        v1::Keys::Unkeyed(many) => {
                            multikey |= many;
                            entries
                                .insert(
                                    (meta.id.0, index.id, crate::index::UNKEYED, key.as_slice()),
                                    (),
                                )
                                .unwrap();
                        }
                        v1::Keys::Undecidable => {
                            entries
                                .insert(
                                    (
                                        meta.id.0,
                                        index.id,
                                        crate::index::UNDECIDABLE,
                                        key.as_slice(),
                                    ),
                                    (),
                                )
                                .unwrap();
                        }
                    }
                }
                index.multikey = multikey;
                old_multikey.insert((meta.id.0, index.id), multikey);
            }
            collections
                .insert(
                    (meta.db.as_str(), meta.name.as_str()),
                    serde_json::to_vec(&meta).unwrap().as_slice(),
                )
                .unwrap();
        }
        txn.open_table(tables::META)
            .unwrap()
            .insert(tables::META_FORMAT_VERSION, [4u8].as_slice())
            .unwrap();
        // What 0.50 recorded: its walk verified the vector for schema 4.
        let mut verified = txn.open_table(tables::VECTOR_VERIFIED).unwrap();
        let records: Vec<(String, Vec<u8>)> = verified
            .iter()
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value().to_string(), v.value().to_vec())
            })
            .collect();
        assert!(!records.is_empty(), "premise: the reference's open recorded its walk");
        for (k, mut v) in records {
            v[1] = 4;
            verified.insert(k.as_str(), v.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    drop(db);
    crate::format::write_sidecar(path, &v050_sidecar()).unwrap();
    old_multikey
}

/// The sidecar a 0.50.0 node writes.
fn v050_sidecar() -> crate::format::Sidecar {
    let mut sidecar = crate::format::stamp(4, &v050());
    sidecar.written_by = "0.50.0".into();
    sidecar
}

/// The versions a 0.50.0 build carries.
pub(crate) fn v050() -> crate::format::BuildVersions {
    crate::format::BuildVersions {
        schema: 4,
        written_by: "0.50.0".into(),
        ..crate::format::BuildVersions::ours()
    }
}

/// Every row of a table, as bytes, or none if the table does not exist.
fn rows<D, K, V>(db: &D, def: redb::TableDefinition<K, V>) -> BTreeMap<Vec<u8>, Vec<u8>>
where
    D: ReadableDatabase,
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    let txn = db.begin_read().unwrap();
    let table = match txn.open_table(def) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return BTreeMap::new(),
        Err(e) => panic!("{e}"),
    };
    table
        .iter()
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            (K::as_bytes(&k.value()).as_ref().to_vec(), V::as_bytes(&v.value()).as_ref().to_vec())
        })
        .collect()
}

/// A closed store, read without writing to it.
fn read_only(path: &Path) -> redb::ReadOnlyDatabase {
    redb::Builder::new().open_read_only(path).unwrap()
}

/// Whether a table exists in the store.
fn has_table(path: &Path, name: &str) -> bool {
    read_only(path).begin_read().unwrap().list_tables().unwrap().any(|t| {
        use redb::TableHandle;
        t.name() == name
    })
}

/// `DOCS` as `(collection id, key) -> record`.
fn docs_of<D: ReadableDatabase>(db: &D) -> BTreeMap<(u64, Vec<u8>), Vec<u8>> {
    let txn = db.begin_read().unwrap();
    let docs = txn.open_table(tables::DOCS).unwrap();
    docs.iter()
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            ((k.value().0, k.value().1.to_vec()), v.value().to_vec())
        })
        .collect()
}

/// `INDEX_ENTRIES` of the collections in `only`.
fn entries_of<D: ReadableDatabase>(
    db: &D,
    only: &BTreeSet<u64>,
) -> BTreeSet<(u64, u32, Vec<u8>, Vec<u8>)> {
    let txn = db.begin_read().unwrap();
    let entries = txn.open_table(tables::INDEX_ENTRIES).unwrap();
    entries
        .iter()
        .unwrap()
        .map(|r| {
            let (key, _) = r.unwrap();
            let (c, i, k, d) = key.value();
            (c, i, k.to_vec(), d.to_vec())
        })
        .filter(|(c, ..)| only.contains(c))
        .collect()
}

/// Every collection's metadata, by id.
fn metas_of<D: ReadableDatabase>(db: &D) -> BTreeMap<u64, CollectionMeta> {
    let txn = db.begin_read().unwrap();
    let collections = txn.open_table(tables::COLLECTIONS).unwrap();
    collections
        .iter()
        .unwrap()
        .map(|r| {
            let meta: CollectionMeta = serde_json::from_slice(r.unwrap().1.value()).unwrap();
            (meta.id.0, meta)
        })
        .collect()
}

fn version<D: ReadableDatabase>(db: &D) -> Option<u8> {
    let txn = db.begin_read().unwrap();
    let meta = txn.open_table(tables::META).unwrap();
    meta.get(tables::META_FORMAT_VERSION).unwrap().and_then(|v| v.value().first().copied())
}

/// The ids a query yields through the planner's index, and each one's stamp.
fn query(engine: &Engine, coll: &CollectionMeta, filter: &Document) -> Option<Vec<(Bson, String)>> {
    let parsed = kimmy_query::filter::parse(filter).unwrap();
    let plan = kimmy_query::plan::choose(&parsed, &coll.indexes)?;
    let scan = crate::IndexScan {
        index_id: plan.index_id,
        ranges: &plan.ranges,
        both_bounds: plan.both_bounds,
        exact: plan.exact,
    };
    let mut out = Vec::new();
    engine
        .visit_index_candidates(
            coll,
            &scan,
            crate::CandidateOrder::ById { after: None, want: None },
            crate::WalkScope::Request,
            |_, stamp, doc| {
                if kimmy_query::filter::matches(&parsed, &doc).unwrap() {
                    out.push((doc.get("_id").unwrap().clone(), format!("{stamp:?}")));
                }
                Ok(true)
            },
        )
        .unwrap()
        .expect("the scan was not refused");
    Some(out)
}

/// The queries the migrated store must answer as the reference does, each
/// through the index the planner picks.
fn queries() -> Vec<Document> {
    vec![
        doc! {"n": {"$gte": 3}},
        doc! {"n": {"$gt": 2, "$lt": 3}},
        doc! {"n": 2.5},
        doc! {"d": {"$lt": 1}},
        doc! {"d": {"$gte": 0}},
        doc! {"nested.a.b": {"$gt": 1}},
        doc! {"nested": {"a": {"b": 0.5}}},
        doc! {"tags": 2},
        doc! {"tags": {"$gt": 1, "$lt": 4}},
        doc! {"email": "e1005"},
        doc! {"email2": "solo", "balance": {"$gt": 0}},
        doc! {"n": {"$gte": 1, "$lt": 9}, "s": "s3"},
        doc! {"at": {"$lt": bson::DateTime::from_millis(1_000_500)}, "tier": {"$lt": 3}},
        doc! {"at2": {"$gte": bson::DateTime::from_millis(0)}, "tier": {"$gte": 3}},
    ]
}

#[test]
fn the_frozen_encoders_agree_with_the_live_ones_and_the_legacy_module() {
    // The fixture's bytes come from `v1` alone; this is the cross-check, not
    // the source: the frozen schema 4 encoder against the product's frozen
    // legacy form, and the layout-built schema 5 key against the live encoder.
    for set in fixture_ids() {
        let id = set;
        assert_eq!(
            v1::int64(id),
            kimmy_core::legacy_key::legacy_key(&DocId::Int64(id)).into_bytes(),
            "{id}"
        );
        assert_eq!(kimmy_core::legacy_key::decode_v1(&v1::int64(id)), Some(id), "{id}");
        let live = kimmy_core::keyenc::encode(&Bson::Int64(id)).unwrap();
        assert_eq!(v1::v2_int64(id), live, "{id}");
        assert_eq!(v2_int64_of(&live), Some(id), "{id}");
    }
    // A schema 4 key of every value the fixture indexes, against the golden
    // bytes of v0.50.0 (`kimmy_core::legacy_key`'s test holds the integers').
    assert_eq!(
        v1::encode(&Bson::Double(0.5)).unwrap(),
        [0x20, 0x04, 0x7F, 0xFF, 0x80, 0, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(v1::encode(&Bson::String("e5".into())).unwrap(), [0x30, b'e', b'5', 0, 0]);
    assert_eq!(v1::encode(&dec("1")), None, "0.50 could not key a decimal");
    // The pair whose keys cross between the schemas.
    assert_eq!(v1::v2_int64(A2.0), v1::int64(A2.1));
}

fn fixture_ids() -> Vec<i64> {
    let mut ids = item_ids();
    ids.extend(1..40);
    ids
}

#[test]
fn a_migrated_store_equals_the_store_this_build_writes() {
    let f = fixture();
    let path = f.copy();
    {
        let before = read_only(&path);
        assert_eq!(version(&before), Some(4), "premise: schema 4");
        let items = f.ids.keys().next().copied().unwrap();
        assert!(
            docs_of(&before).contains_key(&(items, v1::int64(1_000))),
            "premise: schema 4 keys"
        );
    }
    let reference = Engine::open(&f.reference).unwrap();
    let (migrated, logs) = crate::log_capture::logged(|| Engine::open(&path).unwrap());

    assert_eq!(version(migrated.db()), Some(5));
    // Every row of `DOCS`, the dropped collection's included: the same keys and
    // the same records, so every `(id, stamp)` and every tombstone.
    assert_eq!(docs_of(migrated.db()), docs_of(reference.db()), "DOCS");
    // Every standing collection's index entries, byte for byte.
    let standing: BTreeSet<u64> = metas_of(reference.db()).keys().copied().collect();
    assert_eq!(
        entries_of(migrated.db(), &standing),
        entries_of(reference.db(), &standing),
        "INDEX_ENTRIES"
    );
    // The definitions: unchanged but for `multikey`, which is 0.50's or this
    // build's, whichever is raised.
    let (got, want) = (metas_of(migrated.db()), metas_of(reference.db()));
    for (id, meta) in &got {
        let reference = &want[id];
        for (index, theirs) in meta.indexes.iter().zip(&reference.indexes) {
            let old = f.old_multikey[&(*id, index.id)];
            assert_eq!(index.multikey, theirs.multikey || old, "{}: multikey", index.name);
            assert_eq!(
                IndexMeta { multikey: false, ..index.clone() },
                IndexMeta { multikey: false, ..theirs.clone() },
                "{}",
                index.name
            );
        }
    }
    // The queries, through the planner and the executor.
    let items_meta = migrated.get_collection("shop", "items").unwrap();
    let items_ref = reference.get_collection("shop", "items").unwrap();
    let mut planned = 0;
    for filter in queries() {
        let mine = query(&migrated, &items_meta, &filter);
        planned += usize::from(mine.is_some());
        assert_eq!(mine, query(&reference, &items_ref, &filter), "{filter}");
    }
    assert!(planned >= 10, "premise: the queries use indexes ({planned})");
    for &id in f.ids[&items_meta.id.0].iter() {
        let id = DocId::Int64(id);
        // As bytes: a document holding a NaN is not equal to itself.
        let bytes = |doc: Option<Document>| doc.map(|d| bson::serialize_to_vec(&d).unwrap());
        assert_eq!(
            bytes(migrated.get(&items_meta, &id).unwrap()),
            bytes(reference.get(&items_ref, &id).unwrap()),
            "{id:?}"
        );
    }
    // The decimals 0.50 could not key or decide are keyed and decided.
    let by_n = items_meta.index("by_n").unwrap().id;
    assert_eq!(migrated.unkeyed_count(&items_meta, by_n).unwrap(), 0, "a decimal is keyed now");
    // The TTL indexes: one line each, counting what changes.
    assert!(
        logs.contains("index=ttl_lt now_selected=1 now_not_selected=0"),
        "the TTL line for ttl_lt: {logs}"
    );
    assert!(
        logs.contains("index=ttl_gte now_selected=0 now_not_selected=1"),
        "the TTL line for ttl_gte: {logs}"
    );
    // The first start on schema 5 walks the oplog; the next one does not.
    assert!(logs.contains("checked the version vector against the oplog"), "{logs}");
    assert!(logs.contains("migrated storage schema 4 -> 5"), "{logs}");
    drop(migrated);
    let (again, logs) = crate::log_capture::logged(|| Engine::open(&path).unwrap());
    assert!(logs.contains("skipped the oplog walk"), "{logs}");
    assert!(!logs.contains("schema 4 -> 5"), "a finished migration does nothing: {logs}");
    drop(again);
    for table in ["schema5_docs_phases", "schema5_index_steps", "schema5_docs_rekeyed"] {
        assert!(!has_table(&path, table), "{table} is gone with the last commit");
    }
    assert!(!has_table(&path, "partial_rebuilt_under_find"));
    assert_eq!(read_only(&path).begin_read().map(|_| ()).ok(), Some(()));
    assert_eq!(crate::format::read_sidecar(&path).unwrap().schema, 5);
}

/// Put every numeric document key a store holds back in its schema 4 form, in
/// `DOCS` and as the document part of every index entry, inside `txn`; index
/// values are left as they are (a migration drains them unread). For the
/// fixtures of the chain's older steps, which an engine of this build writes
/// and then rewinds.
pub(crate) fn rewind_keys_to_v1(txn: &redb::WriteTransaction) {
    let mut docs = txn.open_table(tables::DOCS).unwrap();
    let rows: Vec<(u64, Vec<u8>, Vec<u8>)> = docs
        .iter()
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.value().0, k.value().1.to_vec(), v.value().to_vec())
        })
        .filter(|(_, k, _)| k[0] == 0x20)
        .collect();
    for (c, k, _) in &rows {
        docs.remove((*c, k.as_slice())).unwrap();
    }
    let v1_of = |k: &[u8]| v1::int64(v2_int64_of(k).expect("an Int64 key of this build"));
    for (c, k, v) in &rows {
        docs.insert((*c, v1_of(k).as_slice()), v.as_slice()).unwrap();
    }
    let mut entries = txn.open_table(tables::INDEX_ENTRIES).unwrap();
    let held: Vec<(u64, u32, Vec<u8>, Vec<u8>)> = entries
        .iter()
        .unwrap()
        .map(|r| {
            let (key, _) = r.unwrap();
            let (c, i, k, d) = key.value();
            (c, i, k.to_vec(), d.to_vec())
        })
        .filter(|(.., d)| d.first() == Some(&0x20))
        .collect();
    for (c, i, k, d) in &held {
        entries.remove((*c, *i, k.as_slice(), d.as_slice())).unwrap();
    }
    for (c, i, k, d) in &held {
        entries.insert((*c, *i, k.as_slice(), v1_of(d).as_slice()), ()).unwrap();
    }
}
