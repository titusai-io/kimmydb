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
            // Selected now, and an array at `arr_when_rich`'s path: the
            // rebuild makes that index multikey, which 0.50's never was.
            d.insert("arr", vec![1, 2]);
        }
        1_021 => {
            d.insert("email2", "neg");
            d.insert("balance", dec("-5"));
        }
        1_022 => {
            d.insert("email2", "plain");
            d.insert("balance", 7);
            d.insert("arr", 5);
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
            "arr_when_rich",
            vec![IndexField::ascending("arr")],
            false,
            None,
            doc! {"balance": {"$gt": 0}},
        );
        // A dropped index: its tombstone is one of the tables the rebuild leaves
        // alone.
        index(&engine, "items", "dropme", vec![IndexField::ascending("s")], false);
        assert!(engine.drop_index("shop", "items", "dropme").unwrap());
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

/// What a 0.50.0 build calls itself in a sidecar. A build of the 0.50.0
/// release and not this one, whatever this branch's own version says until the
/// release bumps it: the horizon's reset is decided by the two differing.
const V050: &str = "0.50.0+release";

/// The sidecar a 0.50.0 node writes.
fn v050_sidecar() -> crate::format::Sidecar {
    crate::format::stamp(4, &v050())
}

/// The versions a 0.50.0 build carries.
pub(crate) fn v050() -> crate::format::BuildVersions {
    crate::format::BuildVersions {
        schema: 4,
        written_by: V050.into(),
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

use crate::migrate_v5::{
    Collision, DOCS_REKEYED, INDEX_STEPS, IndexMark, Phase, RANGE_PHASES, RangeMark, Step, hooks,
};

/// Chunk sizes for the migrations this thread runs: rows per re-key chunk and
/// entry bytes per drain or refill chunk.
fn chunks(rows: usize, bytes: usize) {
    hooks::REKEY_ROWS.with(|c| c.set(Some(rows)));
    hooks::INDEX_BYTES.with(|c| c.set(Some(bytes)));
}

/// The markers a killed migration left, read without writing.
fn phases(path: &Path) -> (BTreeMap<u64, RangeMark>, BTreeMap<(u64, u32), IndexMark>) {
    let db = read_only(path);
    let ranges = rows(&db, RANGE_PHASES)
        .into_iter()
        .map(|(k, v)| (u64::from_le_bytes(k.try_into().unwrap()), RangeMark::decode(&v).unwrap()))
        .collect();
    let txn = db.begin_read().unwrap();
    let indexes = match txn.open_table(INDEX_STEPS) {
        Ok(t) => t
            .iter()
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.value(), IndexMark::decode(v.value()).unwrap())
            })
            .collect(),
        Err(_) => BTreeMap::new(),
    };
    (ranges, indexes)
}

/// **I-DOCS**, read from a store a migration was killed in: in every range the
/// rows in `DOCS` and in the staging table are each in the form the range's
/// phase says -- schema 4 in `DOCS` while `rekeying` (and before the first
/// commit), schema 5 everywhere after -- every fixture id is held exactly once
/// across the two, and each row holds that id's own record. A key is matched
/// only as the exact key of a known id in the expected form, so the pair whose
/// keys cross between the schemas (A2) cannot pass in the wrong form: its row
/// would hold the other document's record.
fn assert_i_docs(
    path: &Path,
    f: &Fixture,
    reference: &BTreeMap<(u64, Vec<u8>), Vec<u8>>,
    at: usize,
) {
    let db = read_only(path);
    let docs = docs_of(&db);
    let staged: BTreeMap<(u64, Vec<u8>), Vec<u8>> = {
        let txn = db.begin_read().unwrap();
        match txn.open_table(DOCS_REKEYED) {
            Ok(t) => t
                .iter()
                .unwrap()
                .map(|r| {
                    let (k, v) = r.unwrap();
                    ((k.value().0, k.value().1.to_vec()), v.value().to_vec())
                })
                .collect(),
            Err(_) => BTreeMap::new(),
        }
    };
    let (ranges, _) = phases(path);
    for (&coll, ids) in &f.ids {
        let as_v1: BTreeMap<Vec<u8>, i64> = ids.iter().map(|&id| (v1::int64(id), id)).collect();
        let as_v2: BTreeMap<Vec<u8>, i64> = ids.iter().map(|&id| (v1::v2_int64(id), id)).collect();
        let phase = ranges.get(&coll).map(|m| m.phase);
        let (in_docs, in_staging) = match phase {
            None | Some(Phase::Rekeying) => (&as_v1, &as_v2),
            Some(Phase::MovingBack) | Some(Phase::Done) => (&as_v2, &as_v2),
        };
        let mut seen = BTreeSet::new();
        let mut check = |form: &BTreeMap<Vec<u8>, i64>, key: &[u8], record: &[u8], where_: &str| {
            let id = *form.get(key).unwrap_or_else(|| {
                panic!("killed at {at}: {where_} of {coll} holds {key:02x?}, not in the form {phase:?} says")
            });
            assert!(seen.insert(id), "killed at {at}: {id} held twice");
            assert_eq!(
                record,
                reference[&(coll, v1::v2_int64(id))].as_slice(),
                "killed at {at}: the row of {id} holds another record"
            );
        };
        for ((c, key), record) in docs.range((coll, vec![0x20])..(coll, vec![0x21])) {
            assert_eq!(*c, coll);
            check(in_docs, key, record, "docs");
        }
        for ((c, key), record) in staged.range((coll, Vec::new())..(coll + 1, Vec::new())) {
            assert_eq!(*c, coll);
            assert_ne!(phase, Some(Phase::Done), "killed at {at}: a done range has staged rows");
            check(in_staging, key, record, "the staging table");
        }
        assert_eq!(&seen, ids, "killed at {at}: rows of {coll} lost");
    }
}

/// What a killed migration was in the middle of, for the coverage the kill
/// test claims.
fn where_killed(path: &Path, f: &Fixture) -> BTreeSet<&'static str> {
    let (ranges, indexes) = phases(path);
    let mut out = BTreeSet::new();
    for (coll, mark) in &ranges {
        let total = f.ids.get(coll).map_or(0, |ids| ids.len()) as u64;
        match mark.phase {
            Phase::Rekeying if mark.out > 0 => {
                out.insert("mid re-key");
            }
            Phase::MovingBack if mark.back == 0 => {
                assert_eq!(mark.out, total, "every row moved out before any moves back");
                out.insert("between the last re-key chunk and the first move-back chunk");
            }
            Phase::MovingBack => {
                out.insert("mid move-back");
            }
            _ => {}
        }
    }
    for mark in indexes.values() {
        match &mark.step {
            Step::Draining => {
                out.insert("mid drain");
            }
            Step::Refilling { after: Some(_) } => {
                out.insert("mid refill");
            }
            Step::Refilling { after: None } => {
                out.insert("between the drain and the refill");
            }
            _ => {}
        }
    }
    out
}

#[test]
fn killed_at_every_commit_the_migration_resumes_to_the_store_this_build_writes() {
    // The guard on resumability: the migration killed before each of its
    // commits in turn, I-DOCS read from what each kill left, and the next open
    // finishing to the reference, byte for byte. Small chunks, so every phase
    // is cut part-way. The cursor must be the one in the store: the work the
    // two runs do together is the clean run's and at most one chunk.
    chunks(9, 1_500);
    let f = fixture();
    let reference = read_only(&f.reference);
    let reference_docs = docs_of(&reference);
    let standing: BTreeSet<u64> = metas_of(&reference).keys().copied().collect();
    let reference_entries = entries_of(&reference, &standing);
    drop(reference);
    let (commits, refilled) = {
        let path = f.copy();
        let (before, walked) = (hooks::commits(), hooks::documents_refilled());
        drop(Engine::open(&path).unwrap());
        (hooks::commits() - before, hooks::documents_refilled() - walked)
    };
    assert!(commits > 40, "premise: many commits ({commits})");
    let mut seen = BTreeSet::new();
    for at in 1..=commits {
        let path = f.copy();
        let walked = hooks::documents_refilled();
        hooks::fail_at_commit(at);
        assert!(Engine::open(&path).is_err(), "premise: the failure was reached at {at}");
        seen.extend(where_killed(&path, &f));
        assert_i_docs(&path, &f, &reference_docs, at);
        assert_multikey_with_its_entries(&path, at);
        let engine = Engine::open(&path).unwrap();
        assert_eq!(docs_of(engine.db()), reference_docs, "killed at {at}: DOCS");
        assert_eq!(
            entries_of(engine.db(), &standing),
            reference_entries,
            "killed at {at}: INDEX_ENTRIES"
        );
        assert!(!crate::migrate_v5::unfinished(engine.db()).unwrap(), "killed at {at}: finished");
        drop(engine);
        let both = hooks::documents_refilled() - walked;
        assert!(
            both <= refilled + hooks::largest_refill_chunk(),
            "killed at {at}: the two runs refilled {both} documents, the clean run {refilled}: the \
             resume did not start at the cursor its last commit left"
        );
        std::fs::remove_file(&path).unwrap();
    }
    for wanted in [
        "mid re-key",
        "between the last re-key chunk and the first move-back chunk",
        "mid move-back",
        "mid drain",
        "between the drain and the refill",
        "mid refill",
    ] {
        assert!(seen.contains(wanted), "no kill landed {wanted}: {seen:?}");
    }
}

#[test]
fn the_a2_pair_keeps_both_documents() {
    // The schema 5 key of A2.0 is the schema 4 key of A2.1. Re-keyed in place,
    // the first to move (A2.0 sorts first) would land on the second's row and
    // replace it. Staged, both survive, each under its own new key.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = Engine::open(&path).unwrap();
        let pair = engine.create_collection("shop", "pair").unwrap();
        engine
            .insert_many(
                &pair,
                vec![doc! {"_id": A2.0, "who": "a"}, doc! {"_id": A2.1, "who": "b"}],
            )
            .unwrap();
    }
    {
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        rewind_keys_to_v1(&txn);
        txn.open_table(tables::META)
            .unwrap()
            .insert(tables::META_FORMAT_VERSION, [4u8].as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    let docs = docs_of(&read_only(&path));
    let coll = CollectionId::derive("shop", "pair").0;
    assert_eq!(
        docs.keys().filter(|(c, _)| *c == coll).cloned().collect::<BTreeSet<_>>(),
        BTreeSet::from([(coll, v1::int64(A2.0)), (coll, v1::int64(A2.1))]),
        "premise: both under their schema 4 keys"
    );
    chunks(1, 1_000);
    let engine = Engine::open(&path).unwrap();
    let pair = engine.get_collection("shop", "pair").unwrap();
    for (id, who) in [(A2.0, "a"), (A2.1, "b")] {
        let doc = engine.get(&pair, &DocId::Int64(id)).unwrap().expect("both survive");
        assert_eq!(doc.get_str("who").unwrap(), who, "{id:#x} holds its own document");
    }
    assert_eq!(engine.count(&pair, crate::WalkScope::Request).unwrap(), 2);
}

/// No index holds a document's two or more keyed entries while its definition
/// says it is not multikey: the flag lands in the commit of the chunk that
/// wrote the entries, never later (design 6.2, review v2.2 m3). Sound one way
/// only (an empty array is multikey with one entry), which is the way that
/// loses rows.
fn assert_multikey_with_its_entries(path: &Path, at: usize) {
    let db = read_only(path);
    let metas = metas_of(&db);
    let standing: BTreeSet<u64> = metas.keys().copied().collect();
    let mut keyed: BTreeMap<(u64, u32, Vec<u8>), usize> = BTreeMap::new();
    for (c, i, k, d) in entries_of(&db, &standing) {
        if !crate::index::is_sentinel(&k) {
            *keyed.entry((c, i, d)).or_default() += 1;
        }
    }
    for ((c, i, d), n) in keyed {
        let index = metas[&c].index_by_id(i).unwrap();
        assert!(
            n < 2 || index.multikey,
            "killed at {at}: {} holds {n} entries of {d:02x?} and says it is not multikey",
            index.name
        );
    }
}

/// Every `Int64` id a store this build wrote holds, by collection id.
fn ids_of(path: &Path) -> BTreeMap<u64, BTreeSet<i64>> {
    let mut out: BTreeMap<u64, BTreeSet<i64>> = BTreeMap::new();
    for (c, key) in docs_of(&read_only(path)).into_keys() {
        if let Some(id) = v2_int64_of(&key) {
            out.entry(c).or_default().insert(id);
        }
    }
    out
}

/// A schema 4 store: written by this build's engine with `build`, then each
/// `(collection, id, body)` of `edit` put in place of that document's body by a
/// raw write, keeping its stamp -- what a replicated write can leave on 0.50
/// that a local one would be refused -- and taken back to schema 4.
fn small_schema_4(
    build: impl FnOnce(&Engine),
    edit: &[(&str, i64, Document)],
) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = Engine::open(&path).unwrap();
        build(&engine);
    }
    {
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            for (coll, id, body) in edit {
                let c = CollectionId::derive("shop", coll).0;
                let key = v1::v2_int64(*id);
                let mut record = codec::decode_doc_record(
                    docs.get((c, key.as_slice())).unwrap().expect("the document to edit").value(),
                )
                .unwrap();
                record.body = bson::serialize_to_vec(body).unwrap();
                docs.insert((c, key.as_slice()), codec::encode_doc_record(&record).as_slice())
                    .unwrap();
            }
        }
        txn.commit().unwrap();
    }
    let ids = ids_of(&path);
    to_schema_4(&path, &ids);
    (dir, path)
}

/// A copy of a closed store beside it.
fn copy_beside(path: &Path, name: &str) -> PathBuf {
    let copy = path.with_file_name(format!("{name}.redb"));
    copy_store(path, &copy);
    copy
}

/// How many migration commits a clean run of a copy of `path` makes.
fn clean_commits(path: &Path) -> usize {
    let before = hooks::commits();
    drop(Engine::open(&copy_beside(path, "clean")).unwrap());
    hooks::commits() - before
}

/// The store and its sidecar, as bytes.
fn snapshot(path: &Path) -> (Vec<u8>, Option<Vec<u8>>) {
    (std::fs::read(path).unwrap(), std::fs::read(crate::format::sidecar_path(path)).ok())
}

/// An older build refuses the store, and nothing in it changes.
fn assert_refused_untouched(path: &Path, build: &crate::format::BuildVersions, what: &str) {
    let before = snapshot(path);
    match crate::format::check_before_open_with(path, build) {
        Err(crate::StorageError::RefusedStore { .. }) => {}
        other => panic!("{what}: the check did not refuse: {other:?}"),
    }
    assert!(Engine::open_as(path, None, build).is_err(), "{what}: the open did not refuse");
    assert!(snapshot(path) == before, "{what}: the store or its sidecar changed");
}

/// The versions a 0.49 build carries.
fn v049() -> crate::format::BuildVersions {
    crate::format::BuildVersions { written_by: "0.49.3".into(), ..v050() }
}

#[test]
fn a_050_or_049_build_refuses_the_store_from_raise_5() {
    // The rollback boundary is `raise(5)`, immediately before the first commit
    // (design 6.1, 6.5 item 1): from it the sidecar refuses an older build;
    // from the first commit META does too, which is what refuses a copy whose
    // sidecar is gone.
    let f = fixture();
    let items = CollectionId::derive("shop", "items").0;
    for (name, build) in [("0.50", v050()), ("0.49", v049())] {
        // Killed after `raise(5)` and before the first commit.
        let path = f.copy();
        hooks::fail_at_commit(1);
        assert!(Engine::open(&path).is_err(), "premise: the first commit failed");
        assert_eq!(crate::format::read_sidecar(&path).unwrap().schema, 5, "raised");
        assert_eq!(version(&read_only(&path)), Some(4), "premise: before the first commit");
        assert_refused_untouched(&path, &build, &format!("{name}, raised, with the sidecar"));
        // With no sidecar, META says 4 and nothing of the migration was
        // written: it is the store the previous build left, and it opens it.
        std::fs::remove_file(crate::format::sidecar_path(&path)).unwrap();
        assert!(crate::format::check_before_open_with(&path, &build).is_ok(), "{name}");
        assert!(!has_table(&path, "schema5_docs_phases"));
        assert!(docs_of(&read_only(&path)).contains_key(&(items, v1::int64(1_000))));

        // Killed after the first commit: refused with the sidecar, and without.
        let path = f.copy();
        hooks::fail_at_commit(2);
        assert!(Engine::open(&path).is_err(), "premise: the second commit failed");
        assert_eq!(version(&read_only(&path)), Some(5), "the version went down first");
        assert_refused_untouched(&path, &build, &format!("{name}, first commit, sidecar"));
        std::fs::remove_file(crate::format::sidecar_path(&path)).unwrap();
        assert_refused_untouched(&path, &build, &format!("{name}, first commit, no sidecar"));
        // And this build resumes it.
        drop(Engine::open(&path).unwrap());
    }
}

#[test]
fn the_preflight_refuses_with_the_figures_and_leaves_a_store_0_50_opens() {
    let f = fixture();
    let path = f.copy();
    let items = CollectionId::derive("shop", "items").0;
    // A few KiB: below any requirement, however small the test's chunks.
    hooks::FREE.with(|c| c.set(Some(4_096)));
    let (err, logs) = crate::log_capture::logged(|| Engine::open(&path).err());
    hooks::FREE.with(|c| c.set(None));
    let err = err.expect("refused");
    let file = std::fs::metadata(&path).unwrap().len();
    match &err {
        crate::StorageError::NotEnoughSpace { needed, free, .. } => {
            assert_eq!(*free, 4_096, "the free space it was told");
            assert!(*needed >= crate::migrate_v5::growth_step(file, 4_096), "{needed}");
        }
        other => panic!("the wrong refusal: {other}"),
    }
    let message = err.to_string();
    for wanted in [
        "needs",
        "0 MiB is free",
        "redb's next growth step",
        "documents and tombstones re-keyed through a staging table",
        "index chunks in flight",
        "MiB margin",
        "still schema 4",
        "previous build",
    ] {
        assert!(message.contains(wanted), "{wanted:?} not in: {message}");
    }
    // The plan was announced before the refusal: what it would do.
    for wanted in
        ["collections=3", "ranges=3", "purge_pending_ranges=1", "indexes=", "required_free_mib="]
    {
        assert!(logs.contains(wanted), "{wanted:?} not in the announcement: {logs}");
    }
    // Nothing of the migration was written: the version, the keys, the
    // sidecar's schema and author are where 0.50 left them, and its check
    // passes.
    let db = read_only(&path);
    assert_eq!(version(&db), Some(4));
    assert!(docs_of(&db).contains_key(&(items, v1::int64(1_000))), "still schema 4 keys");
    drop(db);
    for table in [
        "schema5_docs_phases",
        "schema5_index_steps",
        "schema5_docs_rekeyed",
        "schema5_unique_collisions",
    ] {
        assert!(!has_table(&path, table), "{table} exists after a refusal");
    }
    let sidecar = crate::format::read_sidecar(&path).unwrap();
    assert_eq!((sidecar.schema, sidecar.written_by.as_str()), (4, V050));
    assert!(crate::format::check_before_open_with(&path, &v050()).is_ok());
    // With the space, the next start migrates.
    hooks::FREE.with(|c| c.set(Some(1 << 40)));
    drop(Engine::open(&path).unwrap());
    hooks::FREE.with(|c| c.set(None));
    assert_eq!(version(&read_only(&path)), Some(5));
}

#[test]
fn the_requirement_is_the_growth_step_the_staged_rows_the_index_chunks_and_a_margin() {
    // B1 and M3 of the 0.51 build review: the requirement is redb's next growth
    // step for the file, the rows the re-key stages, the index chunks in flight
    // and a fixed margin, and nothing priced as the file's size over its
    // documents.
    use crate::migrate_v5::{MARGIN_BYTES, growth_step};
    let f = fixture();
    let path = f.copy();
    chunks(8_192, 16 << 20);
    let plan = {
        let db = Database::create(&path).unwrap();
        crate::migrate_v5::preflight(&db, &path, 4).unwrap()
    };
    assert_eq!(plan.collections, 3, "items, strs and empty: gone was dropped");
    assert_eq!(plan.purge_pending, 1, "the dropped collection's rows");
    assert_eq!(plan.ranges, 3, "items, gone and strs hold rows; empty holds none");
    let numeric: usize = f.ids.values().map(BTreeSet::len).sum();
    assert_eq!(plan.docs_rows, numeric as u64 + 9, "and strs' nine string ids");
    assert_eq!(plan.staged.rows, numeric as u64, "the rows under a number key, purge-pending too");
    // Every numeric row's leaf bytes, as a key-ordered staging table holds them.
    let db = read_only(&path);
    let leaf: u64 = docs_of(&db)
        .iter()
        .filter(|((_, k), _)| k.first() == Some(&0x20))
        .map(|((_, k), v)| {
            let pair = k.len() as u64 + v.len() as u64 + 20;
            4096u64.div_ceil(4092 / pair)
        })
        .sum();
    drop(db);
    assert_eq!(plan.staged.bytes, leaf);
    assert_eq!(plan.need.growth, growth_step(plan.file_bytes, plan.staged.largest_allocation));
    // The fixture's rows are fewer than a chunk: the chunks in flight are the
    // rows themselves, four times over.
    assert_eq!(plan.need.staged_docs, leaf * 5);
    // Its entries are less than one chunk of 16 MiB: twice their bytes, four
    // times over.
    assert!(plan.index_bytes > 0 && plan.index_bytes < 16 << 20, "{}", plan.index_bytes);
    assert_eq!(plan.need.staged_index, 8 * plan.index_bytes);
    assert_eq!(plan.need.margin, MARGIN_BYTES);
    assert_eq!(
        plan.required_bytes,
        plan.need.growth + plan.need.staged_docs + plan.need.staged_index + MARGIN_BYTES
    );
}

#[test]
fn the_requirement_pins_the_shipped_chunks_against_0_2_and_2_000_000_rows() {
    // The requirement as a function of what the tables hold, at the shipped
    // chunk sizes: 8,192 rows and 16 MiB.
    use crate::migrate_v5::{MARGIN_BYTES, Staged, requirement};
    const MIB: u64 = 1 << 20;
    let shipped =
        |file: u64, staged: Staged, index: u64| requirement(file, &staged, index, 8_192, 16 << 20);
    // No rows, no entries: the growth step and the margin.
    let empty = shipped(MIB, Staged::default(), 0);
    assert_eq!((empty.staged_docs, empty.staged_index), (0, 0));
    assert_eq!(empty.total, MIB - 4096 + MARGIN_BYTES);
    // Two rows of a kilobyte in a 45 MiB file of oplog: their own bytes, not
    // 8,192 rows priced at the file over two, which was 1.5 TiB.
    let two = Staged { rows: 2, bytes: 2 * 1366, largest_allocation: 4096 };
    let churned = shipped(45 * MIB, two, 0);
    assert_eq!(churned.staged_docs, 2 * 1366 * 5);
    assert_eq!(churned.total, 45 * MIB - 4096 + 2 * 1366 * 5 + MARGIN_BYTES);
    // 2,000,000 rows of about 410 bytes (nine to a page, 456 bytes each) in a
    // 2,411 MiB file with three indexes: the rows once, four chunks of 8,192 of
    // them, and eight times 16 MiB of entries.
    let big = Staged { rows: 2_000_000, bytes: 2_000_000 * 456, largest_allocation: 4096 };
    let fixture = shipped(2_411 * MIB, big, 1 << 30);
    assert_eq!(fixture.growth, 2_411 * MIB - 4096);
    assert_eq!(fixture.staged_docs, 2_000_000 * 456 + 4 * 8_192 * 456);
    assert_eq!(fixture.staged_index, 8 * (16 << 20));
    // The chunk sizes are what the in-flight terms are made of.
    let smaller = requirement(2_411 * MIB, &big, 1 << 30, 1_024, 1 << 20);
    assert_eq!(smaller.staged_docs, 2_000_000 * 456 + 4 * 1_024 * 456);
    assert_eq!(smaller.staged_index, 8 * (1 << 20));
}

#[test]
fn the_growth_step_is_redbs() {
    // `TransactionalMemory::grow` in redb 4.3: below one region the file
    // doubles; with full regions and a trailing one it fills the trailing one,
    // and adds a region only when the allocation needs more than half of what
    // the trailing one has left; with full regions only it adds one.
    use crate::migrate_v5::{REDB_REGION_BYTES as R, growth_step};
    const MIB: u64 = 1 << 20;
    let page = 4096;
    // One partial region: the step is the file, or twice an allocation larger
    // than half of it.
    assert_eq!(growth_step(128 * MIB + page, page), 128 * MIB);
    assert_eq!(growth_step(page + page, page), 2 * page);
    assert_eq!(growth_step(MIB + page, 4 * MIB), 8 * MIB);
    // The measurement at 2,000,000 documents: a 2,411 MiB file grew to 4,096
    // MiB during its migration, inside the doubling.
    assert_eq!(growth_step(2_411 * MIB, page), 2_411 * MIB - page);
    // Past one region: the rest of the trailing region (`R - T`), the step a
    // probe of redb 4.3 measured (a 6,673 MiB file grew to 8,192 MiB, with
    // 1 MiB allocations), not `2R - T`.
    assert_eq!(growth_step(6_673 * MIB, MIB), 8_192 * MIB - 6_673 * MIB + page);
    assert_eq!(growth_step(R + MIB + page, page), R - MIB);
    assert_eq!(growth_step(5 * (R / 4) + page, MIB), R - R / 4);
    // `2R - T` only when the trailing region has no more than twice the
    // largest allocation left.
    assert_eq!(growth_step(2 * R - MIB + page, MIB / 2 - page), MIB);
    assert_eq!(growth_step(2 * R - MIB + page, MIB / 2), R + MIB);
    // A whole number of regions grows by one.
    assert_eq!(growth_step(2 * R + page, page), R);
    assert_eq!(growth_step(R + page, page), R);
    // Never more than two regions.
    for len in [0, page, R - 1, R, 3 * R + 1, 10 * R - page] {
        assert!(growth_step(len, 16 * MIB) <= 2 * R, "{len}");
    }
}

/// The requirement of `path` at the shipped chunk sizes, as the real open plans
/// it: a copy refused with no free space says the figure, a second copy is
/// refused with one byte less than it, and the store migrates with exactly it.
fn requirement_holds_on_open(path: &Path) -> crate::migrate_v5::Plan {
    chunks(8_192, 16 << 20);
    let refused = |free: u64, name: &str| {
        let copy = copy_beside(path, name);
        hooks::FREE.with(|c| c.set(Some(free)));
        let opened = Engine::open(&copy).map(drop);
        hooks::FREE.with(|c| c.set(None));
        match opened {
            Err(crate::StorageError::NotEnoughSpace { needed, .. }) => needed,
            other => panic!("not refused with {free} bytes free: {other:?}"),
        }
    };
    let needed = refused(0, "probe");
    let plan = hooks::LAST_PLAN.with(|p| p.borrow_mut().take()).expect("the open planned");
    assert_eq!(needed, plan.required_bytes, "the refusal names the plan's figure");
    assert_eq!(refused(needed - 1, "short"), needed, "one byte short");
    hooks::FREE.with(|c| c.set(Some(needed)));
    let opened = Engine::open(path).map(drop);
    hooks::FREE.with(|c| c.set(None));
    opened.unwrap();
    assert_eq!(version(&read_only(path)), Some(5));
    plan
}

/// A store this build writes with `build`, taken back to schema 4 by the
/// product's own helper.
fn schema_4_store(build: impl FnOnce(&Engine)) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = Engine::open(&path).unwrap();
        build(&engine);
    }
    crate::migrate::make_schema_4_for_test(&path).unwrap();
    (dir, path)
}

/// What a store's requirement is made of: the growth step, the rows' own bytes,
/// the index chunks and the margin, whatever else the file holds.
fn assert_bounded_by_its_rows(plan: &crate::migrate_v5::Plan, what: &str) {
    use crate::migrate_v5::{MARGIN_BYTES, growth_step};
    let growth = growth_step(plan.file_bytes, plan.staged.largest_allocation);
    assert_eq!(plan.need.growth, growth, "{what}");
    assert_eq!(plan.need.margin, MARGIN_BYTES, "{what}");
    assert!(plan.need.staged_docs <= 5 * plan.staged.bytes, "{what}: {plan:?}");
    assert!(plan.need.staged_index <= 8 * plan.index_bytes, "{what}: {plan:?}");
    assert_eq!(
        plan.required_bytes,
        growth + plan.need.staged_docs + plan.need.staged_index + MARGIN_BYTES,
        "{what}"
    );
    eprintln!(
        "PREFLIGHT {what}: file {} KiB, {} rows to re-key in {} KiB, requires {} KiB (growth {} \
         KiB, documents {} KiB, index {} KiB, margin {} KiB)",
        plan.file_bytes >> 10,
        plan.staged.rows,
        plan.staged.bytes >> 10,
        plan.required_bytes >> 10,
        plan.need.growth >> 10,
        plan.need.staged_docs >> 10,
        plan.need.staged_index >> 10,
        plan.need.margin >> 10
    );
}

#[test]
fn the_preflight_asks_an_empty_store_for_its_growth_step_and_the_margin() {
    // The review's 12,032 MiB for a store with one empty collection: a full
    // chunk of 8,192 rows priced at the file over no documents.
    for (what, collection) in [("an empty store", false), ("an empty collection", true)] {
        let (_dir, path) = schema_4_store(|engine| {
            if collection {
                engine.create_collection("app", "c").unwrap();
            }
        });
        let plan = requirement_holds_on_open(&path);
        assert_bounded_by_its_rows(&plan, what);
        assert_eq!((plan.staged.rows, plan.need.staged_docs, plan.need.staged_index), (0, 0, 0));
        assert!(plan.required_bytes < 66 << 20, "{what}: {} MiB", plan.required_bytes >> 20);
    }
}

/// The heavier preflight cases, in release. CI runs this one (about 1 s); the
/// churned store (about 70 s) and the one shaped like kimmy1-3 (about 45 s) are
/// run by hand:
///
/// ```text
/// cargo test --release -p kimmy-storage --lib -- --include-ignored \
///     migrate_v5_tests::the_preflight_of_a_store_
/// ```
#[test]
#[ignore = "heavy: 60,000 documents dropped and purged; CI runs it in release"]
fn the_preflight_of_a_store_whose_collection_was_dropped_and_purged() {
    // The review's 6,029,660 MiB: the file holds the purged collection's freed
    // pages and no document.
    let (_dir, path) = schema_4_store(|engine| {
        let tmp = engine.create_collection("app", "tmp").unwrap();
        for start in (0..60_000_i64).step_by(10_000) {
            let docs = (start..start + 10_000).map(|i| doc! {"_id": i, "pad": "p".repeat(400)});
            engine.insert_many(&tmp, docs.collect()).unwrap();
        }
        assert!(engine.drop_collection("app", "tmp").unwrap());
        engine.purge_dropped_collection(tmp.id).unwrap();
        engine.create_collection("app", "c").unwrap();
    });
    let plan = requirement_holds_on_open(&path);
    assert_bounded_by_its_rows(&plan, "a dropped and purged collection");
    assert!(plan.file_bytes > 32 << 20, "premise: the file kept the purged pages");
    assert_eq!(plan.staged.rows, 0);
    assert_eq!(plan.required_bytes, plan.need.growth + plan.need.margin);
}

#[test]
#[ignore = "heavy: 20,000 replacements, about 70 s in release; run by hand"]
fn the_preflight_of_a_store_of_two_documents_churned_20_000_times() {
    // The review's 1,506,989 MiB: two documents, and an oplog of 20,000
    // replacements of a kilobyte.
    let (_dir, path) = schema_4_store(|engine| {
        let c = engine.create_collection("app", "c").unwrap();
        let two = vec![doc! {"_id": 1_i64, "n": 0}, doc! {"_id": 2_i64, "n": 0}];
        engine.insert_many(&c, two).unwrap();
        for i in 0..20_000_i64 {
            let id = 1 + i % 2;
            let body = doc! {"_id": id, "n": i, "pad": "p".repeat(1_000)};
            engine.replace(&c, &DocId::Int64(id), body, false).unwrap();
        }
    });
    let plan = requirement_holds_on_open(&path);
    assert_bounded_by_its_rows(&plan, "two documents churned 20,000 times");
    assert_eq!(plan.staged.rows, 2);
    assert!(plan.staged.bytes <= 2 * 4096, "two rows of a kilobyte: {}", plan.staged.bytes);
    assert!(plan.file_bytes > 16 << 20, "premise: the oplog fills the file");
}

#[test]
#[ignore = "heavy: an oplog of 10,000 entries of 16 KiB, about 45 s in release; run by hand"]
fn the_preflight_of_a_store_shaped_like_kimmy1_3() {
    // kimmy1 to kimmy3 in the lab: about 1.5 GB of file, mostly oplog, and four
    // documents. Scaled down, with the ratio of file bytes to document bytes
    // kept high: four documents of 16 KiB, each rewritten 2,500 times.
    let (_dir, path) = schema_4_store(|engine| {
        let c = engine.create_collection("app", "c").unwrap();
        let body = |id: i64, n: i64| doc! {"_id": id, "n": n, "pad": "q".repeat(16_000)};
        engine.insert_many(&c, (1..=4).map(|id| body(id, 0)).collect()).unwrap();
        for n in 0..2_500_i64 {
            for id in 1..=4 {
                engine.replace(&c, &DocId::Int64(id), body(id, n), false).unwrap();
            }
        }
    });
    let plan = requirement_holds_on_open(&path);
    assert_bounded_by_its_rows(&plan, "shaped like kimmy1-3");
    assert_eq!(plan.staged.rows, 4);
    let ratio = plan.file_bytes / plan.staged.bytes.max(1);
    assert!(ratio > 1_000, "premise: the file is {ratio} times its documents' bytes");
    // The old figure priced 8,192 rows at the file over four documents, twice,
    // four times over: hundreds of GiB here. This one is the step and a few
    // pages more than the margin.
    let old = 4 * 8_192 * (plan.file_bytes / 4) * 2;
    assert!(plan.required_bytes < plan.need.growth + (65 << 20), "{plan:?}");
    assert!(plan.required_bytes * 100 < old, "{} against {old}", plan.required_bytes);
}

#[test]
fn a_4_to_10_gib_store_on_a_10_gib_volume_passes_where_redbs_step_fits() {
    // M3: `2R - T` refused every store of 4 to 10 GiB on the documented 10 GiB
    // volume. With redb's real step each passes where the step fits in what is
    // left of the volume, and is refused where redb would need more.
    use crate::migrate_v5::REDB_REGION_BYTES as R;
    const GIB: u64 = 1 << 30;
    let f = fixture();
    let volume = 10 * GIB;
    for (file, fits) in [
        (4 * GIB - (1 << 20), true),
        (4 * GIB, true),
        (5 * GIB, true),
        (6 * GIB, true),
        (7 * GIB, true),
        (8 * GIB - (128 << 20), true),
        // A whole number of regions: redb adds a region, 4 GiB, and 2 are left.
        (2 * R + 4096, false),
        // 9 GiB: the trailing region needs 3 GiB more, and 1 is left.
        (9 * GIB, false),
    ] {
        let path = f.copy();
        chunks(8_192, 16 << 20);
        hooks::FILE_BYTES.with(|c| c.set(Some(file)));
        hooks::FREE.with(|c| c.set(Some(volume - file)));
        let opened = Engine::open(&path).map(drop);
        hooks::FILE_BYTES.with(|c| c.set(None));
        hooks::FREE.with(|c| c.set(None));
        match (fits, opened) {
            (true, Ok(())) => assert_eq!(version(&read_only(&path)), Some(5)),
            (false, Err(crate::StorageError::NotEnoughSpace { needed, free, .. })) => {
                assert!(needed > free, "{file}");
                assert_eq!(version(&read_only(&path)), Some(4), "nothing written");
            }
            (fits, other) => panic!("{} MiB, fits {fits}: {:?}", file >> 20, other),
        }
    }
}

/// The requirement of the 2,000,000-document acceptance store's shape (about
/// 350-byte documents, integer `_id`s, three indexes), which must be about the
/// old figure: at this size that was the growth step and 128 MiB. With
/// `PREFLIGHT_STORE` naming a schema 4 store, that store is planned instead (a
/// copy of it).
///
/// ```text
/// cargo test --release -p kimmy-storage --lib -- --include-ignored --exact \
///     migrate_v5_tests::the_preflight_of_the_2_000_000_document_store
/// ```
#[test]
#[ignore = "heavy: 2,000,000 documents; a measurement, run by hand in release"]
fn the_preflight_of_the_2_000_000_document_store() {
    let n: i64 = std::env::var("PREFLIGHT_N").map(|s| s.parse().unwrap()).unwrap_or(2_000_000);
    let dir = tempfile::tempdir().unwrap();
    let path = match std::env::var("PREFLIGHT_STORE") {
        Ok(store) => {
            let path = dir.path().join("kimmy.redb");
            std::fs::copy(store, &path).unwrap();
            path
        }
        Err(_) => shaped_schema_4(dir.path(), n, 300, 3),
    };
    let file = std::fs::metadata(&path).unwrap().len();
    chunks(8_192, 16 << 20);
    let started = std::time::Instant::now();
    let plan = {
        let db = Database::create(&path).unwrap();
        crate::migrate_v5::preflight(&db, &path, 4).unwrap()
    };
    let planned = started.elapsed();
    let old = crate::migrate_v5::growth_step(file, 4096)
        + 4 * (8_192 * (file / plan.docs_rows.max(1)) * 2).max(2 * (16 << 20));
    eprintln!(
        "PREFLIGHT 2M: {} rows, file {} MiB; requires {} MiB (growth {}, documents {}, index {}, \
         margin {}); the old figure {} MiB; planned in {planned:?}",
        plan.docs_rows,
        file >> 20,
        plan.required_bytes >> 20,
        plan.need.growth >> 20,
        plan.need.staged_docs >> 20,
        plan.need.staged_index >> 20,
        plan.need.margin >> 20,
        old >> 20
    );
    assert!(
        plan.required_bytes >= old * 3 / 4 && plan.required_bytes <= old * 3 / 2,
        "about the old figure: {} MiB against {} MiB",
        plan.required_bytes >> 20,
        old >> 20
    );
}

#[test]
fn a_numeric_key_no_build_wrote_is_refused() {
    // `decode_v1` is strict: a key with the number tag that is not exactly the
    // schema 4 key of an `Int64` refuses the migration, loudly, rather than
    // being re-keyed as the nearest id.
    let f = fixture();
    let path = f.copy();
    let items = CollectionId::derive("shop", "items").0;
    {
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            let record =
                docs.get((items, v1::int64(1_000).as_slice())).unwrap().unwrap().value().to_vec();
            // 5 with its mantissa's top bit clear: no encoder writes it.
            let bad = [0x20, 0x04, 0x80, 0x02, 0x50, 0, 0, 0, 0, 0, 0, 0];
            docs.insert((items, bad.as_slice()), record.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    chunks(1_000, 1 << 20);
    match Engine::open(&path) {
        Err(crate::StorageError::Corrupt(why)) => {
            assert!(why.contains("not the schema 4 key of any Int64 id"), "{why}")
        }
        Err(other) => panic!("the wrong refusal: {other}"),
        Ok(_) => panic!("re-keyed a key no build wrote"),
    }
    assert!(crate::migrate_v5::unfinished(&Database::create(&path).unwrap()).unwrap());
}

#[test]
fn a_schema_4_store_with_3_to_4_markers_loses_them_in_the_first_commit() {
    // The 3 -> 4 markers on a store that never finished that migration: the
    // first 4 -> 5 commit deletes them, so no schema 5 store holds them.
    let f = fixture();
    let path = f.copy();
    let items = CollectionId::derive("shop", "items").0;
    {
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(crate::migrate_v5::PARTIAL_REBUILT)
            .unwrap()
            .insert((items, 7u32), ())
            .unwrap();
        txn.commit().unwrap();
    }
    assert!(has_table(&path, "partial_rebuilt_under_find"), "premise");
    hooks::fail_at_commit(2);
    assert!(Engine::open(&path).is_err());
    assert_eq!(version(&read_only(&path)), Some(5));
    assert!(!has_table(&path, "partial_rebuilt_under_find"), "gone with the first commit");
    drop(Engine::open(&path).unwrap());
    assert!(!has_table(&path, "partial_rebuilt_under_find"));
}

/// Build `shop.t`, with the given unique indexes created before the documents.
fn unique_store(
    indexes: &[(&str, &str, Option<Document>)],
    docs: Vec<Document>,
    edit: &[(&str, i64, Document)],
) -> (tempfile::TempDir, PathBuf) {
    small_schema_4(
        |engine| {
            let t = engine.create_collection("shop", "t").unwrap();
            for (name, path, filter) in indexes {
                match filter {
                    Some(filter) => partial(
                        engine,
                        "t",
                        name,
                        vec![IndexField::ascending(*path)],
                        true,
                        None,
                        filter.clone(),
                    ),
                    None => index(engine, "t", name, vec![IndexField::ascending(*path)], true),
                }
            }
            engine.insert_many(&t, docs).unwrap();
        },
        edit,
    )
}

#[test]
fn a_finished_3_to_4_rebuilds_unreported_collisions_are_carried_and_settled() {
    // A 0.50 store a crash left between its 3 -> 4 rebuild's last commit and
    // the report: the recorded collisions are pending, and carried. One of them
    // the refill finds again under the schema 5 form of its key: reported once,
    // not twice. The other the exact comparator dissolves (the documents'
    // decimal `tier` was "at least 3" only by ranking equal to 3, so the rebuilt
    // index holds neither): not reported, since it is no violation now, and
    // named in one line.
    let t = CollectionId::derive("shop", "t").0;
    let (_dir, path) = unique_store(
        &[("email_once", "email", Some(doc! {"tier": {"$gte": 3}})), ("num_once", "num", None)],
        vec![
            doc! {"_id": 1_i64, "email": "x", "tier": dec("1"), "num": 1},
            doc! {"_id": 2_i64, "email": "y", "tier": dec("1"), "num": 2},
            doc! {"_id": 5_i64, "email": "z", "num": 7},
            doc! {"_id": 6_i64, "email": "w", "num": 8},
        ],
        &[
            ("t", 2, doc! {"_id": 2_i64, "email": "x", "tier": dec("1"), "num": 2}),
            ("t", 6, doc! {"_id": 6_i64, "email": "w", "num": 7}),
        ],
    );
    let metas = metas_of(&read_only(&path));
    let (email, num) =
        (metas[&t].index("email_once").unwrap().id, metas[&t].index("num_once").unwrap().id);
    {
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut recorded =
                txn.open_table(crate::migrate_v5::PARTIAL_REBUILT_VIOLATIONS).unwrap();
            for (index, name, key, holders) in [
                (email, "email_once", v1::encode(&Bson::String("x".into())).unwrap(), [1, 2]),
                (num, "num_once", v1::encode(&Bson::Int32(7)).unwrap(), [5, 6]),
            ] {
                let row = crate::migrate::RecordedViolations {
                    db: "shop".into(),
                    collection: "t".into(),
                    violations: vec![crate::index::UniqueViolation {
                        index: name.into(),
                        key,
                        holders: holders.iter().map(|&id| v1::int64(id)).collect(),
                    }],
                };
                recorded.insert((t, index), serde_json::to_vec(&row).unwrap().as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }
    // The report reads what the migration recorded: stop just before it.
    crate::migrate::hooks::fail_before_the_report();
    let (opened, logs) = crate::log_capture::logged(|| Engine::open(&path).map(drop));
    assert!(opened.is_err(), "premise: stopped before the report");
    assert!(!has_table(&path, "partial_rebuilt_unique_violations"), "carried, not kept");
    let lines: Vec<&str> = logs.lines().filter(|l| l.contains("is not reported")).collect();
    assert_eq!(lines.len(), 1, "one line for the dissolved collision: {logs}");
    assert!(
        lines[0].contains("index=email_once") && lines[0].contains(r#"holders=["1", "2"]"#),
        "it names the index and its holders by _id: {}",
        lines[0]
    );
    let recorded =
        crate::migrate_v5::recorded_collisions(&Database::create(&path).unwrap()).unwrap();
    let mut named: Vec<(String, Vec<i64>)> = recorded
        .iter()
        .flat_map(|(_, violations)| violations.iter())
        .map(|v| {
            (
                v.index.clone(),
                v.holders.iter().map(|h| v2_int64_of(h).expect("schema 5 holders")).collect(),
            )
        })
        .collect();
    named.sort();
    assert_eq!(
        named,
        vec![("num_once".to_string(), vec![5, 6])],
        "the collision that stands, once, its holders in their schema 5 form"
    );
    let engine = Engine::open(&path).unwrap();
    assert_eq!(engine.unique_violations(), 1, "the one that stands, reported");
    drop(engine);
    let engine = Engine::open(&path).unwrap();
    assert_eq!(engine.unique_violations(), 0, "and forgotten once reported");
    drop(engine);
    assert!(!has_table(&path, "schema5_unique_collisions"));
}

#[test]
fn a_collision_is_recorded_in_its_chunks_commit_wherever_the_migration_is_killed() {
    // A replication-merged duplicate on a 0.50 store: two documents share a
    // unique key, which 0.50 already reported. The refill finds it again,
    // whichever side of a resumed cursor each holder is on, and it is reported
    // once more, once.
    let docs: Vec<Document> = (1..=40_i64)
        .map(|i| doc! {"_id": i, "email": format!("e{i}"), "pad": "p".repeat(40)})
        .collect();
    let (_dir, path) = unique_store(
        &[("email_once", "email", None)],
        docs,
        &[
            ("t", 5, doc! {"_id": 5_i64, "email": "dup", "pad": "p".repeat(40)}),
            ("t", 30, doc! {"_id": 30_i64, "email": "dup", "pad": "p".repeat(40)}),
        ],
    );
    let t = CollectionId::derive("shop", "t").0;
    let email = metas_of(&read_only(&path))[&t].index("email_once").unwrap().id;
    chunks(4, 160);
    let commits = clean_commits(&path);
    let dup = kimmy_core::keyenc::encode(&Bson::String("dup".into())).unwrap();
    let mut sides = BTreeSet::new();
    for at in 1..=commits {
        let run = copy_beside(&path, &format!("run{at}"));
        hooks::fail_at_commit(at);
        assert!(Engine::open(&run).is_err(), "premise: killed at {at}");
        let (_, steps) = phases(&run);
        let recorded = rows(&read_only(&run), crate::migrate_v5::COLLISIONS);
        let past = |id: i64| match steps.get(&(t, email)).map(|m| &m.step) {
            Some(Step::Done) => true,
            Some(Step::Refilling { after: Some(after) }) => *after >= v1::v2_int64(id),
            _ => false,
        };
        // Where the committed cursor stood: before both, between, past both.
        sides.insert((past(5), past(30)));
        let row: Option<Collision> = recorded
            .iter()
            .find(|(k, _)| k.ends_with(&dup))
            .map(|(_, v)| Collision::decode(v).unwrap());
        if past(30) {
            let row = row.unwrap_or_else(|| panic!("killed at {at}: past both, and not recorded"));
            assert_eq!(row.holders, vec![v1::v2_int64(5), v1::v2_int64(30)], "killed at {at}");
        } else {
            assert!(row.is_none(), "killed at {at}: recorded before the refill reached it");
        }
        let engine = Engine::open(&run).unwrap();
        assert_eq!(engine.unique_violations(), 1, "killed at {at}: reported once more");
        drop(engine);
        let engine = Engine::open(&run).unwrap();
        assert_eq!(engine.unique_violations(), 0, "killed at {at}: and once");
        drop(engine);
    }
    for side in [(false, false), (true, false), (true, true)] {
        assert!(sides.contains(&side), "no kill left the cursor at {side:?}: {sides:?}");
    }
}

#[test]
fn the_money_field_collision_is_reported() {
    // A partial unique index whose filter path holds a decimal: 0.50 filed both
    // documents undecidable and enforced nothing, so it accepted both. Decided
    // exactly (design 2.6), both are selected, keyed under one email, and
    // collide: the migration reports it through the existing path, and from
    // then on a write that would add or keep a third holder is refused.
    let (_dir, path) = unique_store(
        &[("money_once", "email", Some(doc! {"balance": {"$gt": 0}}))],
        vec![
            doc! {"_id": 1_i64, "email": "x", "balance": dec("10")},
            doc! {"_id": 2_i64, "email": "y", "balance": dec("10")},
            doc! {"_id": 3_i64, "email": "z", "balance": dec("-10")},
        ],
        &[("t", 2, doc! {"_id": 2_i64, "email": "x", "balance": dec("10")})],
    );
    let t = CollectionId::derive("shop", "t").0;
    let money = metas_of(&read_only(&path))[&t].index("money_once").unwrap().id;
    let run = |path: &Path, key: &[u8]| {
        entries_of(&read_only(path), &BTreeSet::from([t]))
            .into_iter()
            .filter(|(_, i, k, _)| *i == money && k.as_slice() == key)
            .count()
    };
    assert_eq!(run(&path, crate::index::UNDECIDABLE), 3, "premise: 0.50 decided none of them");
    let engine = Engine::open(&path).unwrap();
    assert_eq!(engine.unique_violations(), 1, "the collision 0.50 never enforced");
    let coll = engine.get_collection("shop", "t").unwrap();
    let x = kimmy_core::keyenc::encode(&Bson::String("x".into())).unwrap();
    let held = |engine: &Engine| {
        crate::index::scan_range(
            engine,
            crate::WalkScope::Request,
            coll.id,
            money,
            &x,
            Some(&x),
            crate::index::Unkeyed::Exclude,
        )
        .unwrap()
    };
    assert_eq!(held(&engine), vec![v1::v2_int64(1), v1::v2_int64(2)], "both keyed under x");
    assert_eq!(
        entries_of(engine.db(), &BTreeSet::from([t]))
            .iter()
            .filter(|(_, i, ..)| *i == money)
            .count(),
        2,
        "the negative balance is not selected and the index holds nothing for it: no run"
    );
    // A live write after 2.6: a third decimal holder of x is refused.
    let err = engine
        .insert(&coll, doc! {"_id": 4_i64, "email": "x", "balance": dec("10.00")})
        .unwrap_err();
    assert!(
        matches!(err, crate::StorageError::Core(kimmy_core::Error::UniqueViolation { .. })),
        "{err}"
    );
    // A standing holder's update that keeps the key is refused too, which 0.50
    // allowed; one that leaves the filter's selection is accepted.
    let err = engine
        .replace(
            &coll,
            &DocId::Int64(1),
            doc! {"_id": 1_i64, "email": "x", "balance": dec("11")},
            false,
        )
        .unwrap_err();
    assert!(err.to_string().contains("already holds this value"), "{err}");
    engine
        .replace(
            &coll,
            &DocId::Int64(1),
            doc! {"_id": 1_i64, "email": "x", "balance": dec("0")},
            false,
        )
        .unwrap();
    assert_eq!(held(&engine), vec![v1::v2_int64(2)], "the one that left the filter is unfiled");
    // And the document 0.50 did not select stays outside the constraint.
    engine
        .replace(
            &coll,
            &DocId::Int64(3),
            doc! {"_id": 3_i64, "email": "x", "balance": dec("-1")},
            false,
        )
        .unwrap();
}

/// The schema 4 store of the TTL test: two TTL partial indexes over `tier`,
/// `$lt: 3` on `at` and `$gte: 3` on `at2`, and documents whose `tier` 0.50
/// compared as equal to 3 whenever it was a decimal.
fn ttl_decimal_store() -> (tempfile::TempDir, PathBuf) {
    let (expired, fresh) =
        (bson::DateTime::from_millis(1_000), bson::DateTime::from_millis(10_000_000_000_000));
    let docs: Vec<Document> = [
        // Newly expired: `$lt` selects it now, and `at` is due.
        (1_i64, dec("1"), expired, fresh),
        // Newly kept: 0.50's `$gte` selected it and `at2` is due, so 0.50
        // expired it; now `$gte` does not select it, and `$lt`'s `at` is fresh.
        (2, dec("1"), fresh, expired),
        // A decimal whose selection did not change, both ways: untouched.
        (3, dec("5"), expired, fresh),
        // Not a decimal: what 0.50 did, this build does.
        (4, Bson::Int32(1), expired, expired),
        // Equal to the bound: `$lt` never, `$gte` always, so `ttl_gte`
        // expires it on both builds.
        (5, dec("3.0"), expired, expired),
        // Below the bound by 10^-33: `$lt` now, `$gte` no longer.
        (6, dec("2.999999999999999999999999999999999"), expired, expired),
        // Selected now by `$lt`, and not due.
        (7, dec("-Infinity"), fresh, fresh),
        // Not a decimal, not due.
        (8, Bson::Int32(5), fresh, fresh),
    ]
    .into_iter()
    .map(|(id, tier, at, at2)| doc! {"_id": id, "tier": tier, "at": at, "at2": at2})
    .collect();
    small_schema_4(
        |engine| {
            let t = engine.create_collection("shop", "t").unwrap();
            for (name, field, filter) in [
                ("ttl_lt", "at", doc! {"tier": {"$lt": 3}}),
                ("ttl_gte", "at2", doc! {"tier": {"$gte": 3}}),
            ] {
                partial(
                    engine,
                    "t",
                    name,
                    vec![IndexField::ascending(field)],
                    false,
                    Some(3_600),
                    filter,
                );
            }
            engine.insert_many(&t, docs).unwrap();
        },
        &[],
    )
}

#[test]
fn a_ttl_partial_index_over_decimals_counts_and_expires_by_exact_value() {
    // Design 2.6 and the v2.3 test plan's (b), Chris's ruling: a TTL partial
    // index decides a decimal at its filter path by exact value, in both
    // directions. On 0.50 a decimal the `$lt: 3` filter did not select sat in
    // the undecidable run and a TTL index never expired it, while `$gte: 3`
    // selected every decimal (ranked equal to 3) and expired it once due. The
    // migration counts, per TTL index, what changes, from that run; the first
    // expiry pass after it deletes exactly the documents now selected and due.
    let (_dir, path) = ttl_decimal_store();
    let t = CollectionId::derive("shop", "t").0;
    let metas = metas_of(&read_only(&path));
    let (lt, gte) = (metas[&t].index("ttl_lt").unwrap().id, metas[&t].index("ttl_gte").unwrap().id);
    let in_run = |index: u32| -> BTreeSet<i64> {
        entries_of(&read_only(&path), &BTreeSet::from([t]))
            .into_iter()
            .filter(|(_, i, k, _)| *i == index && k.as_slice() == crate::index::UNDECIDABLE)
            .map(|(.., d)| kimmy_core::legacy_key::decode_v1(&d).expect("a schema 4 key"))
            .collect()
    };
    assert_eq!(
        in_run(lt),
        BTreeSet::from([1, 2, 3, 5, 6, 7]),
        "premise: 0.50 filed every decimal `$lt` did not select undecidable"
    );
    assert!(in_run(gte).is_empty(), "premise: 0.50's `$gte` selected every decimal");

    let (engine, logs) = crate::log_capture::logged(|| Engine::open(&path).unwrap());
    // Now selected under `$lt`: 1, 2, 6, 7; under `$gte` no longer: 1, 2, 6,
    // 7. 3 and 5 are decimals whose selection did not change, and 4 and 8 are
    // not decimals: in neither count.
    for (index, line) in [
        ("ttl_lt", "index=ttl_lt now_selected=4 now_not_selected=0"),
        ("ttl_gte", "index=ttl_gte now_selected=0 now_not_selected=4"),
    ] {
        assert!(logs.contains(line), "{index}: {logs}");
    }
    let coll = engine.get_collection("shop", "t").unwrap();
    let ids = |index: u32| -> BTreeSet<i64> {
        crate::index::scan_range(
            &engine,
            crate::WalkScope::Request,
            coll.id,
            index,
            &[],
            None,
            crate::index::Unkeyed::Include,
        )
        .unwrap()
        .iter()
        .map(|k| v2_int64_of(k).expect("a schema 5 key"))
        .collect()
    };
    assert_eq!(ids(lt), BTreeSet::from([1, 2, 4, 6, 7]), "ttl_lt holds what `$lt: 3` selects");
    assert_eq!(ids(gte), BTreeSet::from([3, 5, 8]), "ttl_gte holds what `$gte: 3` selects");

    // The first pass: past `at`'s and `at2`'s due time for the expired dates.
    let now = 1_000 + 3_600_000 + 1;
    let mut deleted = BTreeSet::new();
    for index in crate::expiry::ttl_indexes(&coll) {
        let before: BTreeSet<i64> =
            (1..=8).filter(|&id| engine.get(&coll, &DocId::Int64(id)).unwrap().is_some()).collect();
        let out = engine.expire_documents(&coll, index, now).unwrap();
        assert_eq!(out.skipped_filter, 0, "{}: holds only what its filter selects", index.name);
        let after: BTreeSet<i64> =
            (1..=8).filter(|&id| engine.get(&coll, &DocId::Int64(id)).unwrap().is_some()).collect();
        assert_eq!(out.deleted as usize, before.len() - after.len(), "{}", index.name);
        deleted.extend(before.difference(&after).copied());
    }
    assert_eq!(
        deleted,
        BTreeSet::from([1, 4, 5, 6]),
        "exactly the documents now selected and due: 1 newly expired (0.50 never expired it), 2 \
         newly kept (0.50 expired it), 3 untouched, 4 and 5 as on 0.50, 6 by the other index"
    );
}

#[test]
fn a_dropped_collections_rows_are_re_keyed_and_the_purge_finishes() {
    // A drop stopped mid-purge: rows under an id no collection stands over.
    // Re-keyed with the rest, they are what the purger removes, folding each
    // one's term out; it finishes, the name can be created again, the count
    // and digest rows go, and a forced open walk completes over the result.
    let f = fixture();
    let path = f.copy();
    let gone = f.gone;
    let engine = Engine::open(&path).unwrap();
    let held: Vec<Vec<u8>> =
        docs_of(engine.db()).into_keys().filter(|(c, _)| *c == gone.0).map(|(_, k)| k).collect();
    assert_eq!(held.len(), f.ids[&gone.0].len(), "premise: every row, tombstones too");
    for key in &held {
        assert!(v2_int64_of(key).is_some(), "{key:02x?} is not a schema 5 key");
    }
    assert!(engine.purge_dropped_collection(gone).unwrap() > 0);
    assert_eq!(engine.rows_under(gone).unwrap(), 0, "the purge finished");
    {
        let txn = engine.db().begin_read().unwrap();
        assert!(txn.open_table(tables::LIVE_COUNTS).unwrap().get(gone.0).unwrap().is_none());
        assert!(txn.open_table(tables::LIVE_DIGESTS).unwrap().get(gone.0).unwrap().is_none());
    }
    engine.create_collection("shop", "gone").unwrap();
    drop(engine);
    drop(Engine::open_with(&path, &crate::OpenOptions::new().force_rebuild("a test")).unwrap());
}

#[test]
fn the_rebuild_changes_no_definition_stamp_tombstone_or_entry_of_the_oplog() {
    // Review m3: the rebuild writes INDEX_ENTRIES and the multikey bit, and
    // nothing else a peer compares. A creation path that minted a stamp would
    // put an index above the drop tombstones that refused it.
    let f = fixture();
    let path = f.copy();
    // A snapshot pull's persisted cursor, which design 5.1 keeps as it is: a
    // 0.51 sender converts it on the wire.
    {
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(tables::SNAPSHOT_PROGRESS)
            .unwrap()
            .insert([7u8; 16].as_slice(), br#"{"a cursor":"0.50 persisted"}"#.as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    let before = read_only(&path);
    let (metas, digests): (BTreeMap<u64, CollectionMeta>, BTreeMap<u64, _>) = {
        let metas = metas_of(&before);
        let digests = metas
            .iter()
            .map(|(id, m)| (*id, crate::definition_digest::definition_digest(m)))
            .collect();
        (metas, digests)
    };
    let tables_before = (
        rows(&before, tables::OPLOG),
        rows(&before, tables::OPLOG_VERSIONS),
        rows(&before, tables::OPLOG_WITNESSED),
        rows(&before, tables::OPLOG_HELD),
        rows(&before, tables::COLLECTIONS_DROPPED),
        rows(&before, tables::INDEXES_DROPPED),
        rows(&before, tables::LIVE_COUNTS),
        rows(&before, tables::LIVE_DIGESTS),
        rows(&before, tables::LIVE_DIGESTS_THROUGH),
        rows(&before, tables::LIVE_COUNTS_THROUGH),
        rows(&before, tables::SNAPSHOT_PROGRESS),
    );
    assert!(!tables_before.4.is_empty() && !tables_before.5.is_empty(), "premise: tombstones");
    assert!(!tables_before.10.is_empty(), "premise: a persisted snapshot cursor");
    let meta_rows = |db: &redb::ReadOnlyDatabase| -> BTreeMap<Vec<u8>, Vec<u8>> {
        rows(db, tables::META)
            .into_iter()
            .filter(|(k, _)| {
                ![tables::META_FORMAT_VERSION, tables::META_TOMBSTONE_HORIZON]
                    .iter()
                    .any(|m| k.as_slice() == m.as_bytes())
            })
            .collect()
    };
    let meta_before = meta_rows(&before);
    drop(before);
    drop(Engine::open(&path).unwrap());
    let after = read_only(&path);
    let tables_after = (
        rows(&after, tables::OPLOG),
        rows(&after, tables::OPLOG_VERSIONS),
        rows(&after, tables::OPLOG_WITNESSED),
        rows(&after, tables::OPLOG_HELD),
        rows(&after, tables::COLLECTIONS_DROPPED),
        rows(&after, tables::INDEXES_DROPPED),
        rows(&after, tables::LIVE_COUNTS),
        rows(&after, tables::LIVE_DIGESTS),
        rows(&after, tables::LIVE_DIGESTS_THROUGH),
        rows(&after, tables::LIVE_COUNTS_THROUGH),
        rows(&after, tables::SNAPSHOT_PROGRESS),
    );
    assert!(
        tables_after == tables_before,
        "the oplog, the vectors, the tombstones, the counts, the digests or a snapshot cursor \
         moved"
    );
    assert_eq!(meta_rows(&after), meta_before, "META, the refusal hints included");
    for (id, meta) in metas_of(&after) {
        let was = &metas[&id];
        let unflag = |m: &CollectionMeta| {
            let mut m = m.clone();
            for index in &mut m.indexes {
                index.multikey = false;
            }
            m
        };
        assert_eq!(unflag(&meta), unflag(was), "{}: only multikey may move", meta.name);
        assert_eq!(
            crate::definition_digest::definition_digest(&meta),
            digests[&id],
            "{}",
            meta.name
        );
    }
}

#[test]
fn the_horizon_reset_lands_before_raise_5() {
    // Design 6.1, order at open: the ensure-tables commit resets the tombstone
    // horizon (0.50.0 wrote the sidecar, so it is not this build's evidence),
    // and only then does `raise(5)` name this build in the sidecar. Killed
    // between `raise(5)` and the first commit, the reset is already there.
    let f = fixture();
    let path = f.copy();
    let horizon = |path: &Path| {
        let db = read_only(path);
        let raw = rows(&db, tables::META);
        raw.get(tables::META_TOMBSTONE_HORIZON.as_bytes())
            .map(|v| kimmy_core::Hlc::from_bytes(v.as_slice().try_into().expect("an hlc")))
    };
    let was = horizon(&path).expect("premise: the reference kept a horizon");
    let started = kimmy_core::Hlc::new(crate::physical_now_ms(), 0);
    hooks::fail_at_commit(1);
    assert!(Engine::open(&path).is_err());
    let sidecar = crate::format::read_sidecar(&path).unwrap();
    assert_eq!((sidecar.schema, sidecar.written_by), (5, kimmy_core::build::VERSION.to_string()));
    let reset = horizon(&path).unwrap();
    assert!(reset >= started && reset > was, "the reset landed before raise(5): {was} -> {reset}");
}

/// Writers on another store in the same directory, as long as `stop` is clear:
/// the disk and the scheduler busy beside a migration.
fn background_writers(
    dir: &Path,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Vec<std::thread::JoinHandle<u64>> {
    (0..2)
        .map(|w| {
            let path = dir.join(format!("beside{w}.redb"));
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                let engine = Engine::open(&path).unwrap();
                let coll = engine.create_collection("load", "w").unwrap();
                let mut written = 0u64;
                let mut next = 0i64;
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    let docs: Vec<Document> = (next..next + 50)
                        .map(|i| doc! {"_id": i, "pad": "x".repeat(200)})
                        .collect();
                    next += 50;
                    written += engine.insert_many(&coll, docs).unwrap().len() as u64;
                }
                written
            })
        })
        .collect()
}

#[test]
fn a_stop_ends_the_migration_within_one_chunk_while_a_writer_holds_it() {
    // The stop-path rule: a stop asked for during the migration waits for at
    // most the chunk it arrived in, in every kind of chunk, timed, with
    // writers busy beside it; the next start resumes to the reference. Each
    // chunk holds the writer for HOLD before its commit, and the stop arrives
    // inside one of them.
    const HOLD: std::time::Duration = std::time::Duration::from_millis(60);
    let f = fixture();
    let reference = read_only(&f.reference);
    let reference_docs = docs_of(&reference);
    let standing: BTreeSet<u64> = metas_of(&reference).keys().copied().collect();
    let reference_entries = entries_of(&reference, &standing);
    drop(reference);
    let load = tempfile::tempdir().unwrap();
    let stop_load = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers = background_writers(load.path(), std::sync::Arc::clone(&stop_load));
    chunks(16, 2_000);
    for kind in ["re-keying", "moving back", "draining an index", "refilling indexes"] {
        let path = f.copy();
        hooks::hold_in_chunks(Some(HOLD), None);
        hooks::stop_inside(kind, 2);
        let result = Engine::open(&path);
        let returned = std::time::Instant::now();
        let (raised, commits_then) = hooks::stop_raised().expect("the stop was raised");
        let after = hooks::commits() - commits_then;
        hooks::hold_in_chunks(None, None);
        let phase = crate::open_progress::tests::THIS_THREAD_PHASE.with(std::cell::Cell::get);
        let want = match kind {
            "draining an index" => crate::open_progress::OpenPhase::Draining,
            "refilling indexes" => crate::open_progress::OpenPhase::Refilling,
            _ => crate::open_progress::OpenPhase::Migrating,
        };
        assert!(
            matches!(phase, Some((p, total)) if p == want && total > 0),
            "{kind}: the open's progress said {phase:?}, not {want:?} with a total"
        );
        assert!(
            matches!(result, Err(crate::StorageError::Stopping(_))),
            "{kind}: the open did not stop: {:?}",
            result.map(|_| ())
        );
        let waited = returned - raised;
        eprintln!("STOP {kind}: answered {waited:?} after it was asked for, {after} commits");
        assert!(
            waited < HOLD + std::time::Duration::from_millis(1_500),
            "{kind}: the stop waited {waited:?}, more than the chunk it arrived in"
        );
        // The chunk it arrived in, and nothing after it.
        assert!(after <= 1, "{kind}: {after} commits after the stop");
        assert!(crate::migrate_v5::unfinished(&Database::create(&path).unwrap()).unwrap());
        let engine = Engine::open(&path).unwrap();
        assert_eq!(docs_of(engine.db()), reference_docs, "{kind}: resumed");
        assert_eq!(entries_of(engine.db(), &standing), reference_entries, "{kind}: resumed");
    }
    stop_load.store(true, std::sync::atomic::Ordering::SeqCst);
    let written: u64 = writers.into_iter().map(|w| w.join().unwrap()).sum();
    assert!(written > 0, "premise: the writers wrote beside the migration");
}

/// A schema 4 store of `n` documents in one collection with one index over a
/// number, written by hand with the frozen encoder in large transactions: the
/// size a drain has to be measured at, built in seconds.
fn indexed_schema_4(dir: &Path, n: i64) -> PathBuf {
    let path = dir.join("kimmy.redb");
    let coll = CollectionId::derive("shop", "t");
    let mut meta = CollectionMeta::new(coll, "shop", "t", kimmy_core::Hlc::new(1, 0), None);
    meta.indexes.push(IndexMeta {
        id: IndexMeta::derive_id("by_n"),
        name: "by_n".into(),
        fields: vec![IndexField::ascending("n")],
        unique: false,
        enforcement: Enforcement::Local,
        multikey: false,
        expire_after_secs: None,
        partial_filter: None,
        created: None,
    });
    let index = meta.indexes[0].id;
    let node = kimmy_core::NodeId::generate();
    let db = Database::create(&path).unwrap();
    for start in (0..n).step_by(100_000) {
        let txn = db.begin_write().unwrap();
        {
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            let mut entries = txn.open_table(tables::INDEX_ENTRIES).unwrap();
            for i in start..(start + 100_000).min(n) {
                let key = v1::int64(i);
                let record = kimmy_core::DocRecord {
                    stamp: kimmy_core::Stamp::new(kimmy_core::Hlc::new(10 + i as u64, 0), node),
                    deleted: false,
                    body: bson::serialize_to_vec(&doc! {"_id": i, "n": i * 7}).unwrap(),
                };
                docs.insert((coll.0, key.as_slice()), codec::encode_doc_record(&record).as_slice())
                    .unwrap();
                let value = v1::encode(&Bson::Int64(i * 7)).unwrap();
                entries.insert((coll.0, index, value.as_slice(), key.as_slice()), ()).unwrap();
            }
        }
        txn.commit().unwrap();
    }
    let txn = db.begin_write().unwrap();
    txn.open_table(tables::COLLECTIONS)
        .unwrap()
        .insert(("shop", "t"), serde_json::to_vec(&meta).unwrap().as_slice())
        .unwrap();
    txn.open_table(tables::META)
        .unwrap()
        .insert(tables::META_FORMAT_VERSION, [4u8].as_slice())
        .unwrap();
    txn.commit().unwrap();
    path
}

/// A schema 4 store of `n` documents of about `pad` bytes more than their
/// fields in one collection with `indexes` indexes (one to three, over `n`,
/// `m` and `k`), written by hand with the frozen encoder in large
/// transactions: the acceptance store's shape, built in seconds.
fn shaped_schema_4(dir: &Path, n: i64, pad: usize, indexes: usize) -> PathBuf {
    let path = dir.join("kimmy.redb");
    let coll = CollectionId::derive("shop", "t");
    let mut meta = CollectionMeta::new(coll, "shop", "t", kimmy_core::Hlc::new(1, 0), None);
    let fields = ["n", "m", "k"];
    for field in &fields[..indexes.clamp(1, 3)] {
        let name = format!("by_{field}");
        meta.indexes.push(IndexMeta {
            id: IndexMeta::derive_id(&name),
            name,
            fields: vec![IndexField::ascending(*field)],
            unique: false,
            enforcement: Enforcement::Local,
            multikey: false,
            expire_after_secs: None,
            partial_filter: None,
            created: None,
        });
    }
    let node = kimmy_core::NodeId::generate();
    let padding = "p".repeat(pad);
    let db = Database::create(&path).unwrap();
    for start in (0..n).step_by(100_000) {
        let txn = db.begin_write().unwrap();
        {
            let mut docs = txn.open_table(tables::DOCS).unwrap();
            let mut entries = txn.open_table(tables::INDEX_ENTRIES).unwrap();
            for i in start..(start + 100_000).min(n) {
                let key = v1::int64(i);
                let doc = doc! {"_id": i, "n": i * 7, "m": i % 1_000, "k": format!("k{i}"), "pad": &padding};
                let record = kimmy_core::DocRecord {
                    stamp: kimmy_core::Stamp::new(kimmy_core::Hlc::new(10 + i as u64, 0), node),
                    deleted: false,
                    body: bson::serialize_to_vec(&doc).unwrap(),
                };
                docs.insert((coll.0, key.as_slice()), codec::encode_doc_record(&record).as_slice())
                    .unwrap();
                for index in &meta.indexes {
                    let field = index.fields[0].path.as_str();
                    let value = v1::encode(doc.get(field).unwrap()).unwrap();
                    entries
                        .insert((coll.0, index.id, value.as_slice(), key.as_slice()), ())
                        .unwrap();
                }
            }
        }
        txn.commit().unwrap();
    }
    let txn = db.begin_write().unwrap();
    txn.open_table(tables::COLLECTIONS)
        .unwrap()
        .insert(("shop", "t"), serde_json::to_vec(&meta).unwrap().as_slice())
        .unwrap();
    txn.open_table(tables::META)
        .unwrap()
        .insert(tables::META_FORMAT_VERSION, [4u8].as_slice())
        .unwrap();
    txn.commit().unwrap();
    path
}

/// **A bound on the file's growth across a drain and a refill of hundreds of
/// thousands of entries**, at the shipped chunk sizes: the file must stay near
/// its size, and well inside the one growth step the preflight asks room for
/// ([`crate::migrate_v5::growth_step`]).
///
/// It was first written to fail a drain written with redb's `retain_in`, which
/// once grew a file from 2.1 to 12 GiB over 742,858 entries. On redb 4.3 it
/// cannot: `retain_in` drains faster than removing by key and grows the file no
/// more (see `index::clear_index_entries`), so that mutant is equivalent here,
/// and the drain is by key for its stop-and-resume chunks, not for the file.
///
/// ```text
/// cargo test --release -p kimmy-storage --lib -- --include-ignored --exact \
///     migrate_v5_tests::draining_keeps_the_file_near_its_size
/// ```
#[test]
#[ignore = "heavy: 300,000 entries, about 20 s in a debug build; CI runs it in release"]
fn draining_keeps_the_file_near_its_size() {
    let n: i64 = std::env::var("DRAIN_N").map(|s| s.parse().unwrap()).unwrap_or(300_000);
    let dir = tempfile::tempdir().unwrap();
    let path = indexed_schema_4(dir.path(), n);
    let entries = {
        let db = read_only(&path);
        let txn = db.begin_read().unwrap();
        redb::ReadableTableMetadata::len(&txn.open_table(tables::INDEX_ENTRIES).unwrap()).unwrap()
    };
    assert!(entries >= n as u64, "premise: {entries} entries");
    let before = std::fs::metadata(&path).unwrap().len();
    chunks(8_192, 16 << 20);
    hooks::watch_file(&path);
    let engine = Engine::open(&path).unwrap();
    let largest = hooks::largest_file().unwrap();
    let ([rekey, back, drain, refill], rows) = hooks::timings().unwrap();
    drop(engine);
    eprintln!(
        "DRAIN n={n}: {entries} entries; file {} MiB before, {} MiB at its largest; re-key \
         {rekey:?}, move back {back:?}, drain {drain:?} ({} entries), refill {refill:?}",
        before >> 20,
        largest >> 20,
        rows[2]
    );
    assert_eq!(rows[2], entries, "every entry drained");
    assert!(
        largest - before <= crate::migrate_v5::growth_step(before, 4096),
        "the file grew by more than the step the preflight asks room for"
    );
    assert!(
        largest <= before + before / 2 + (64 << 20),
        "the migration grew the file from {} to {} MiB",
        before >> 20,
        largest >> 20
    );
}

/// A store with one TTL partial index over a decimal-bearing collection, at
/// schema `version`, its keys in schema 4 form.
fn ttl_store(version: u8, rebuilt: Option<bool>) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let engine = Engine::open(&path).unwrap();
        let t = engine.create_collection("shop", "t").unwrap();
        partial(
            &engine,
            "t",
            "ttl_lt",
            vec![IndexField::ascending("at")],
            false,
            Some(3_600),
            doc! {"tier": {"$lt": 3}},
        );
        let at = bson::DateTime::from_millis(1_000);
        engine
            .insert_many(
                &t,
                vec![
                    doc! {"_id": 1_i64, "at": at, "tier": dec("1")},
                    doc! {"_id": 2_i64, "at": at, "tier": 1},
                ],
            )
            .unwrap();
    }
    let db = Database::create(&path).unwrap();
    let txn = db.begin_write().unwrap();
    rewind_keys_to_v1(&txn);
    txn.open_table(tables::META)
        .unwrap()
        .insert(tables::META_FORMAT_VERSION, [version].as_slice())
        .unwrap();
    if let Some(rebuilt) = rebuilt {
        // An interrupted 3 -> 4 rebuild's markers: this index done, or not.
        let mut markers = txn.open_table(crate::migrate_v5::PARTIAL_REBUILT).unwrap();
        if rebuilt {
            let id = IndexMeta::derive_id("ttl_lt");
            markers.insert((CollectionId::derive("shop", "t").0, id), ()).unwrap();
        } else {
            markers.insert((0u64, 0u32), ()).unwrap();
        }
    }
    txn.commit().unwrap();
    drop(db);
    (dir, path)
}

#[test]
fn a_ttl_index_never_filed_under_adr_185_has_no_before_to_count_against() {
    // A schema 3 store never filed a partial index's undecidable documents, so
    // what its TTL index selected before is not in the store: the line says
    // so, and gives no count, rather than a count of 0 it did not make. So
    // does an index an interrupted 3 -> 4 rebuild had not reached; one it had
    // reached is counted like any schema 4 index.
    for (version, rebuilt, counted) in
        [(3, None, false), (4, Some(false), false), (4, Some(true), true), (4, None, true)]
    {
        let (_dir, path) = ttl_store(version, rebuilt);
        let (engine, logs) = crate::log_capture::logged(|| Engine::open(&path).unwrap());
        drop(engine);
        let line = logs
            .lines()
            .find(|l| l.contains("a TTL index's partial filter") && l.contains("index=ttl_lt"))
            .unwrap_or_else(|| panic!("schema {version}, {rebuilt:?}: no TTL line: {logs}"));
        if counted {
            assert!(
                line.contains("now_selected=") && !line.contains("not recorded"),
                "schema {version}, {rebuilt:?}: {line}"
            );
        } else {
            assert!(
                line.contains(r#"before="not recorded""#) && !line.contains("now_selected"),
                "schema {version}, {rebuilt:?}: {line}"
            );
        }
    }
}

/// The verified record's epoch and schema bytes, read without writing.
fn verified_record(path: &Path) -> Option<(u8, u8)> {
    let rows = rows(&read_only(path), tables::VECTOR_VERIFIED);
    rows.get("verified".as_bytes()).map(|v| (v[0], v[1]))
}

#[test]
fn i_epoch_and_check_store_answer_for_a_store_mid_migration_and_after() {
    // Design 6.3 (m5): the migration leaves `I_EPOCH` where it is and writes
    // no verified record; the record moves to schema 5 only with the first
    // start's walk. And `kimmyd check-store`: this build checks a store
    // mid-migration and after it alike (redb's checksums, which a migration's
    // chunks keep, being commits like any other); the previous build answers
    // `not_checked` for both, refusing, before it reads anything, a store a
    // newer build wrote.
    let f = fixture();
    let previous = |path: &Path| {
        crate::format::check_reading_only(path, &v050()).map(|_| ()).map_err(|e| e.to_string())
    };
    let path = f.copy();
    assert_eq!(verified_record(&path), Some((crate::verified::I_EPOCH, 4)), "premise: 0.50's");
    assert_eq!(crate::verified::I_EPOCH, 1, "the migration does not move the epoch");

    // Mid-migration: killed half-way through its commits.
    let half = {
        let probe = f.copy();
        let before = hooks::commits();
        drop(Engine::open(&probe).unwrap());
        (hooks::commits() - before) / 2
    };
    hooks::fail_at_commit(half);
    assert!(Engine::open(&path).is_err());
    assert!(crate::migrate_v5::unfinished(&Database::create(&path).unwrap()).unwrap());
    assert_eq!(
        verified_record(&path),
        Some((1, 4)),
        "mid-migration: the record is untouched, epoch 1 for schema 4, so it counts for nothing"
    );
    let ours = crate::check::check_store(&path, &Default::default());
    assert_eq!(ours.verdict, crate::check::Verdict::Clean, "mid-migration, this build: {ours:?}");
    let theirs = previous(&path).expect_err("0.50 refuses a store mid-migration");
    assert!(theirs.contains("schema 5"), "{theirs}");

    // Finished: the first start walks the oplog and records the walk for
    // schema 5 under the same epoch.
    drop(Engine::open(&path).unwrap());
    assert_eq!(verified_record(&path), Some((1, 5)), "after: epoch 1, schema 5");
    let ours = crate::check::check_store(&path, &Default::default());
    assert_eq!(ours.verdict, crate::check::Verdict::Clean, "after, this build: {ours:?}");
    let theirs = previous(&path).expect_err("0.50 refuses a migrated store");
    assert!(theirs.contains("schema 5"), "{theirs}");
    eprintln!("CHECK-STORE: this build: {}; 0.50: refused: {theirs}", ours.json_line());
}

#[test]
fn progress_advances_with_every_chunk_in_every_phase() {
    // `/readyz` while the migration runs: every step reports its own phase
    // (`migrating` for a range's two re-key passes, `draining` for each
    // index, `refilling` for each collection), a total in the step's own unit,
    // and a `done` that moves with every chunk that did work and reads `done
    // == total` once the step ends. The mars run of 2,000,000 documents sat at
    // one `done == total` for 565 s through the drain and the refill before
    // per-step progress, and then showed a drain of 2,000,000 (the
    // collection's documents) for an index that held fewer entries, its last
    // `done` far below that total.
    //
    // Units: the re-key's two passes and the refill count rows of `DOCS`
    // (documents and tombstones); a drain counts its own index's entries,
    // exactly, counted before its first chunk. Moving back is exact too (what
    // moved out); moving out and the refill start from the kept live count, an
    // estimate the end of the step finalizes.
    use crate::open_progress::OpenPhase;
    let f = fixture();
    let path = f.copy();
    // What each step has to do, read from the schema 4 store before the open:
    // each index's entries, each collection's rows of `DOCS`, and its kept
    // live count.
    let (entries_of, rows_of, live_of) = {
        let db = Database::create(&path).unwrap();
        let txn = db.begin_read().unwrap();
        let mut entries_of: BTreeMap<(u64, u32), u64> = BTreeMap::new();
        for row in txn.open_table(tables::INDEX_ENTRIES).unwrap().iter().unwrap() {
            let (coll, index, _, _) = row.unwrap().0.value();
            *entries_of.entry((coll, index)).or_default() += 1;
        }
        let mut rows_of: BTreeMap<u64, u64> = BTreeMap::new();
        for row in txn.open_table(tables::DOCS).unwrap().iter().unwrap() {
            *rows_of.entry(row.unwrap().0.value().0).or_default() += 1;
        }
        let live_of: BTreeMap<u64, u64> = txn
            .open_table(tables::LIVE_COUNTS)
            .unwrap()
            .iter()
            .unwrap()
            .map(|row| {
                let (id, n) = row.unwrap();
                (id.value(), n.value())
            })
            .collect();
        (entries_of, rows_of, live_of)
    };
    chunks(7, 300);
    hooks::chunks_seen();
    hooks::steps_seen();
    let (engine, logs) = crate::log_capture::logged(|| Engine::open(&path).unwrap());
    let items = engine.get_collection("shop", "items").unwrap();
    drop(engine);
    let live_items = live_of[&items.id.0];
    let seen = hooks::chunks_seen();
    let ended = hooks::steps_seen();
    type StepId = (&'static str, String, Option<String>);
    let id_of = |kind: &'static str, c: &str, i: &Option<String>| (kind, c.to_owned(), i.clone());
    let ends: BTreeMap<StepId, &hooks::StepSeen> =
        ended.iter().map(|e| (id_of(e.kind, &e.collection, &e.index), e)).collect();
    assert_eq!(ends.len(), ended.len(), "a step ended twice: {ended:#?}");
    let mut previous: Option<&hooks::ChunkSeen> = None;
    let mut steps: BTreeSet<StepId> = BTreeSet::new();
    for chunk in &seen {
        let want = match chunk.kind {
            "re-keying" | "moving back" => OpenPhase::Migrating,
            "draining an index" => OpenPhase::Draining,
            _ => OpenPhase::Refilling,
        };
        assert_eq!(chunk.phase, Some(want), "{chunk:?}");
        let end = ends
            .get(&id_of(chunk.kind, &chunk.collection, &chunk.index))
            .unwrap_or_else(|| panic!("a step with chunks never ended: {chunk:?}"));
        if end.exact {
            // An exact total holds from the step's first chunk to its last.
            assert_eq!(chunk.total, end.total, "an exact total moved: {chunk:?} {end:?}");
            assert!(chunk.done <= chunk.total, "{chunk:?}");
        } else {
            assert!(chunk.done < chunk.total, "an estimate read finished early: {chunk:?}");
        }
        let same_step = previous.is_some_and(|p| {
            p.kind == chunk.kind && p.collection == chunk.collection && p.index == chunk.index
        });
        if chunk.rows > 0 {
            match previous.filter(|_| same_step) {
                Some(p) => assert!(chunk.done > p.done, "no advance: {p:?} then {chunk:?}"),
                None => assert!(chunk.done >= chunk.rows, "{chunk:?}"),
            }
        }
        steps.insert((chunk.kind, chunk.collection.clone(), chunk.index.clone()));
        previous = Some(chunk);
    }
    // Every kind of step was seen, and a drain of every index of `items`.
    for kind in ["re-keying", "moving back", "refilling indexes"] {
        assert!(steps.iter().any(|(k, c, _)| *k == kind && c == "items"), "no {kind} of items");
    }
    for index in &items.indexes {
        assert!(
            steps.iter().any(|(k, c, i)| *k == "draining an index"
                && c == "items"
                && i.as_deref() == Some(index.name.as_str())),
            "no drain step for {}",
            index.name
        );
    }
    let multi_chunk = |kind: &str| {
        seen.iter().filter(|c| c.kind == kind && c.collection == "items" && c.rows > 0).count() > 2
    };
    for kind in ["re-keying", "moving back", "draining an index", "refilling indexes"] {
        assert!(multi_chunk(kind), "premise: {kind} takes several chunks");
    }
    // Every step ends at `done == total` on `/readyz`: re-key out, re-key back,
    // each TTL partial index's count, each index's drain, each collection's
    // refill. An exact total is what the step did from its start; an estimate
    // is finalized to it. A count has no chunks: it moves with every document.
    const COUNT: &str = "counting a TTL index's selection";
    assert_eq!(
        steps,
        ends.keys().filter(|(k, _, _)| *k != COUNT).cloned().collect(),
        "steps with chunks, steps that ended"
    );
    for end in &ended {
        assert_eq!(end.done, end.total, "a step ended with done != total: {end:?}");
        if end.exact {
            assert_eq!(end.began.1, end.done, "an exact total was not the step's work: {end:?}");
        }
    }
    let end = |kind: &'static str, index: Option<&str>| {
        ends[&id_of(kind, "items", &index.map(str::to_owned))]
    };
    // Each drain's total is its own index's entries, counted, whatever the
    // collection holds: `money_once` (partial, `balance > 0`) holds a handful
    // of `items`' documents, as the mars fixture's partial index held 14 of
    // 2,000,000.
    for index in &items.indexes {
        let drain = end("draining an index", Some(&index.name));
        let held = entries_of.get(&(items.id.0, index.id)).copied().unwrap_or(0);
        assert!(drain.exact, "{drain:?}");
        assert_eq!((drain.began.1, drain.done), (held, held), "{}: {drain:?}", index.name);
    }
    let money = end("draining an index", Some("money_once"));
    assert!(
        (1..=3).contains(&money.total) && money.total * 10 < live_items,
        "premise: a partial index over a handful of {live_items} documents: {money:?}"
    );
    // Moving out and the refill walk every row of `items`, tombstones too: the
    // live count they start from is raised to that, and moving back's exact
    // total is what moved out.
    let rows = rows_of[&items.id.0];
    assert!(rows > live_items, "premise: items holds tombstones ({rows} rows, {live_items} live)");
    for kind in ["re-keying", "refilling indexes"] {
        let step = end(kind, None);
        assert!(!step.exact, "{step:?}");
        assert_eq!((step.began.1, step.done), (live_items, rows), "{kind}: {step:?}");
    }
    let back = end("moving back", None);
    assert!(back.exact && back.done == rows, "{back:?}");
    // Each TTL partial index's count scans every row of `items`, one at a time.
    for ttl in ["ttl_lt", "ttl_gte"] {
        let count = end(COUNT, Some(ttl));
        assert!(!count.exact, "{count:?}");
        assert_eq!((count.began, count.done, count.total), ((0, live_items), rows, rows), "{ttl}");
    }
    let counted = ends.keys().filter(|(k, _, _)| *k == COUNT).count();
    assert_eq!(counted, 2, "a count for each TTL partial index, and none other");
    // An estimate above the work is lowered to it: a collection of 20 number
    // ids and 30 string ids starts its move out from its 50 live documents and
    // moves the 20, the string keys being the same in both schemas.
    let (_dir, mixed) = small_schema_4(
        |engine| {
            let c = engine.create_collection("shop", "mixed").unwrap();
            index(engine, "mixed", "by_n", vec![IndexField::ascending("n")], false);
            let ints = (1..=20_i64).map(|i| doc! {"_id": i, "n": i});
            let strs = (0..30_i64).map(|i| doc! {"_id": format!("s{i}"), "n": i});
            engine.insert_many(&c, ints.chain(strs).collect()).unwrap();
        },
        &[],
    );
    hooks::steps_seen();
    drop(Engine::open(&mixed).unwrap());
    let ended = hooks::steps_seen();
    let step = |kind: &str| ended.iter().find(|e| e.kind == kind && e.collection == "mixed");
    let out = step("re-keying").expect("a move out of mixed");
    assert_eq!((out.began.1, out.done, out.total), (50, 20, 20), "{out:?}");
    let refill = step("refilling indexes").expect("a refill of mixed");
    assert_eq!((refill.done, refill.total), (50, 50), "{refill:?}");
    // The progress lines say the same, phase by phase, with the same figures.
    for phase in ["migrating", "draining", "refilling"] {
        let lines: Vec<&str> = logs
            .lines()
            .filter(|l| {
                l.contains("migrating storage schema 4 -> 5")
                    && l.contains(&format!("phase=\"{phase}\""))
            })
            .collect();
        assert!(lines.len() >= 2, "{phase}: no progress lines: {logs}");
        assert!(lines.iter().all(|l| l.contains("done=") && l.contains("total=")), "{lines:?}");
    }
    assert!(
        logs.lines().any(|l| l.contains("phase=\"draining\"") && l.contains("index=")),
        "a drain's line names its index"
    );
}

/// `shop.t` of `n` documents under a partial index that selects none of them,
/// and `shop.deleted` of `n` documents, every one deleted: a refill that writes
/// nothing for a whole collection, twice over.
fn sparse_refill_store(n: i64) -> (tempfile::TempDir, PathBuf) {
    small_schema_4(
        |engine| {
            let t = engine.create_collection("shop", "t").unwrap();
            partial(
                engine,
                "t",
                "rare",
                vec![IndexField::ascending("x")],
                false,
                None,
                doc! {"flag": {"$gt": 0}},
            );
            let docs = (0..n).map(|i| doc! {"_id": i, "x": i, "pad": "p".repeat(200)});
            engine.insert_many(&t, docs.collect()).unwrap();
            let deleted = engine.create_collection("shop", "deleted").unwrap();
            index(engine, "deleted", "by_x", vec![IndexField::ascending("x")], false);
            engine
                .insert_many(&deleted, (0..n).map(|i| doc! {"_id": i, "x": i}).collect())
                .unwrap();
            for i in 0..n {
                assert!(engine.delete(&deleted, &DocId::Int64(i)).unwrap());
            }
        },
        &[],
    )
}

#[test]
fn a_sparse_partial_index_or_a_run_of_tombstones_refills_in_bounded_chunks() {
    // M2 of the 0.51 build review: a refill chunk ended only on the bytes of
    // entries it wrote, so a partial index that selects nothing, or a run of
    // tombstones, walked its whole collection in one transaction, with no stop
    // looked for and no progress shown. A chunk now also ends on the documents
    // it walked, and on the time it worked.
    let n: i64 = 400;
    let (_dir, path) = sparse_refill_store(n);
    chunks(7, 300);
    hooks::chunks_seen();
    drop(Engine::open(&path).unwrap());
    let seen = hooks::chunks_seen();
    for coll in ["t", "deleted"] {
        let refills: Vec<&hooks::ChunkSeen> =
            seen.iter().filter(|c| c.kind == "refilling indexes" && c.collection == coll).collect();
        let walked: Vec<u64> = refills.iter().map(|c| c.rows).collect();
        assert!(walked.iter().all(|&r| r <= 7), "{coll}: chunks walked {walked:?}");
        assert_eq!(walked.iter().sum::<u64>(), n as u64, "{coll}: every document walked once");
        // Progress moves with every chunk.
        assert!(refills.windows(2).all(|w| w[1].done > w[0].done), "{coll}: {refills:?}");
    }
}

#[test]
fn a_refill_chunk_ends_on_its_time_bound() {
    // The documents and the bytes out of reach: the time bound alone ends each
    // chunk. At zero, every chunk works on one document.
    let (_dir, path) = sparse_refill_store(30);
    chunks(1_000_000, 1 << 30);
    hooks::REFILL_TIME.with(|c| c.set(Some(std::time::Duration::ZERO)));
    hooks::chunks_seen();
    let opened = Engine::open(&path).map(drop);
    hooks::REFILL_TIME.with(|c| c.set(None));
    opened.unwrap();
    let walked: Vec<u64> = hooks::chunks_seen()
        .into_iter()
        .filter(|c| c.kind == "refilling indexes" && c.collection == "t")
        .map(|c| c.rows)
        .collect();
    assert_eq!(walked, vec![1; 30], "one document a chunk");
}

#[test]
fn a_stop_inside_a_sparse_refill_ends_it_within_one_chunk() {
    // The stop-path rule over a refill that writes nothing: the stop asked for
    // inside its second chunk is answered after that chunk, and the next start
    // resumes to what a clean run leaves.
    const HOLD: std::time::Duration = std::time::Duration::from_millis(40);
    let (_dir, path) = sparse_refill_store(400);
    let clean = copy_beside(&path, "clean");
    chunks(7, 300);
    drop(Engine::open(&clean).unwrap());
    let want = docs_of(&read_only(&clean));
    let standing: BTreeSet<u64> = metas_of(&read_only(&clean)).keys().copied().collect();
    let want_entries = entries_of(&read_only(&clean), &standing);
    hooks::hold_in_chunks(Some(HOLD), None);
    hooks::stop_inside("refilling indexes", 2);
    hooks::chunks_seen();
    let result = Engine::open(&path).map(drop);
    let returned = std::time::Instant::now();
    let (raised, commits_then) = hooks::stop_raised().expect("a second refill chunk to stop in");
    hooks::hold_in_chunks(None, None);
    let after = hooks::commits() - commits_then;
    assert!(matches!(result, Err(crate::StorageError::Stopping(_))), "{result:?}");
    assert!(after <= 1, "{after} commits after the stop");
    assert!(returned - raised < HOLD + std::time::Duration::from_millis(1_500));
    let refilled: u64 =
        hooks::chunks_seen().iter().filter(|c| c.kind == "refilling indexes").map(|c| c.rows).sum();
    assert!(refilled <= 14, "stopped after its second chunk: {refilled} documents walked");
    let engine = Engine::open(&path).unwrap();
    assert_eq!(docs_of(engine.db()), want);
    assert_eq!(entries_of(engine.db(), &standing), want_entries);
}

/// The sparse refill at the shipped chunk sizes, in release (about 7 s; CI runs
/// it):
///
/// ```text
/// cargo test --release -p kimmy-storage --lib -- --include-ignored --exact \
///     migrate_v5_tests::a_sparse_refill_at_the_shipped_chunk_sizes_commits_every_8192_documents
/// ```
#[test]
#[ignore = "heavy: 300,000 documents; CI runs it in release"]
fn a_sparse_refill_at_the_shipped_chunk_sizes_commits_every_8192_documents() {
    // The review measured one refill chunk of 300,000 documents here.
    let n: i64 = 300_000;
    let (_dir, path) = small_schema_4(
        |engine| {
            let t = engine.create_collection("shop", "t").unwrap();
            partial(
                engine,
                "t",
                "rare",
                vec![IndexField::ascending("x")],
                true,
                None,
                doc! {"flag": {"$gt": 0}},
            );
            for start in (0..n).step_by(20_000) {
                let docs = (start..start + 20_000)
                    .map(|i| doc! {"_id": i, "x": i, "pad": "p".repeat(300)});
                engine.insert_many(&t, docs.collect()).unwrap();
            }
        },
        &[],
    );
    chunks(8_192, 16 << 20);
    hooks::chunks_seen();
    drop(Engine::open(&path).unwrap());
    let walked: Vec<u64> = hooks::chunks_seen()
        .into_iter()
        .filter(|c| c.kind == "refilling indexes")
        .map(|c| c.rows)
        .collect();
    eprintln!(
        "SPARSE REFILL at shipped sizes: {} chunks, the largest {:?}",
        walked.len(),
        walked.iter().max()
    );
    assert!(walked.iter().all(|&r| r <= 8_192), "{walked:?}");
    assert!(walked.len() as i64 >= n / 8_192, "{} chunks", walked.len());
    assert_eq!(walked.iter().sum::<u64>(), n as u64);
}

#[test]
fn a_carried_row_with_the_same_key_bytes_does_not_absorb_a_dissolved_holder() {
    // Minor 6 of the 0.51 build review: a string key's bytes are the same in
    // both schemas, so a carried row (holders 1, 2, 3 under "x") and the
    // refill's row for the collision that stands now (1, 2) were one row of
    // one table, and document 3, which the exact comparator took out of the
    // index (0.50 ranked its decimal tier equal to 3), was reported with them.
    let t = CollectionId::derive("shop", "t").0;
    let (_dir, path) = unique_store(
        &[("email_once", "email", Some(doc! {"tier": {"$gte": 3}}))],
        vec![
            doc! {"_id": 1_i64, "email": "x", "tier": dec("5")},
            doc! {"_id": 2_i64, "email": "x2", "tier": dec("4")},
            doc! {"_id": 3_i64, "email": "x3", "tier": dec("1")},
        ],
        &[
            ("t", 2, doc! {"_id": 2_i64, "email": "x", "tier": dec("4")}),
            ("t", 3, doc! {"_id": 3_i64, "email": "x", "tier": dec("1")}),
        ],
    );
    let email = metas_of(&read_only(&path))[&t].index("email_once").unwrap().id;
    carry(&path, t, email, "email_once", Bson::String("x".into()), &[1, 2, 3]);
    crate::migrate::hooks::fail_before_the_report();
    let (opened, logs) = crate::log_capture::logged(|| Engine::open(&path).map(drop));
    assert!(opened.is_err(), "premise: stopped before the report");
    assert_eq!(recorded(&path), vec![("email_once".to_string(), vec![1, 2])]);
    let line = logs
        .lines()
        .find(|l| l.contains("rebuilt an index") && l.contains("email_once"))
        .expect("the index's line");
    assert!(line.contains("unique_collisions=1"), "the refill's collision, counted: {line}");
    let gone: Vec<&str> = logs.lines().filter(|l| l.contains("is not reported")).collect();
    assert_eq!(gone.len(), 1, "the carried row, dissolved as recorded: {logs}");
    assert!(gone[0].contains(r#"holders=["1", "2", "3"]"#), "{}", gone[0]);
    assert!(!has_table(&path, "schema5_carried_collisions"), "settled by the last commit");
}

#[test]
fn a_carried_collision_under_a_string_key_found_again_is_reported_once() {
    // Minor 7: the re-found test of the carried rows went through an Int32 key,
    // whose bytes change across the schemas. Under a string key, the same bytes:
    // the collision stands, the refill finds it again under the key it was
    // carried under, and it is reported once, with no line saying it is not.
    let t = CollectionId::derive("shop", "t").0;
    let (_dir, path) = unique_store(
        &[("email_once", "email", None)],
        vec![doc! {"_id": 1_i64, "email": "x"}, doc! {"_id": 2_i64, "email": "x2"}],
        &[("t", 2, doc! {"_id": 2_i64, "email": "x"})],
    );
    let email = metas_of(&read_only(&path))[&t].index("email_once").unwrap().id;
    carry(&path, t, email, "email_once", Bson::String("x".into()), &[1, 2]);
    crate::migrate::hooks::fail_before_the_report();
    let (opened, logs) = crate::log_capture::logged(|| Engine::open(&path).map(drop));
    assert!(opened.is_err(), "premise: stopped before the report");
    assert_eq!(recorded(&path), vec![("email_once".to_string(), vec![1, 2])]);
    assert!(!logs.contains("is not reported"), "found again, not dissolved: {logs}");
    assert!(!has_table(&path, "schema5_carried_collisions"), "settled by the last commit");
    let engine = Engine::open(&path).unwrap();
    assert_eq!(engine.unique_violations(), 1, "reported once");
    drop(engine);
    assert!(!has_table(&path, "schema5_unique_collisions"), "forgotten once reported");
}

#[test]
fn the_collision_table_is_forgotten_after_a_report_pass_that_reported_nothing() {
    // Minor 7: rows whose index is gone are not reported, and the table was
    // forgotten only when something was; it stayed for good.
    let f = fixture();
    let path = f.copy();
    drop(Engine::open(&path).unwrap());
    {
        let db = Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(crate::migrate_v5::COLLISIONS)
            .unwrap()
            .insert((7u64, 9u32, b"k".as_slice()), Collision::default().encode().as_slice())
            .unwrap();
        txn.commit().unwrap();
    }
    let engine = Engine::open(&path).unwrap();
    assert_eq!(engine.unique_violations(), 0, "nothing to report it against");
    drop(engine);
    assert!(!has_table(&path, "schema5_unique_collisions"), "forgotten all the same");
}

/// Record a pending schema 3 -> 4 collision on `index` of collection `coll`:
/// `key`'s schema 4 form, held by `holders`.
fn carry(path: &Path, coll: u64, index: u32, name: &str, key: Bson, holders: &[i64]) {
    let db = Database::create(path).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut recorded = txn.open_table(crate::migrate_v5::PARTIAL_REBUILT_VIOLATIONS).unwrap();
        let row = crate::migrate::RecordedViolations {
            db: "shop".into(),
            collection: "t".into(),
            violations: vec![crate::index::UniqueViolation {
                index: name.into(),
                key: v1::encode(&key).unwrap(),
                holders: holders.iter().map(|&id| v1::int64(id)).collect(),
            }],
        };
        recorded.insert((coll, index), serde_json::to_vec(&row).unwrap().as_slice()).unwrap();
    }
    txn.commit().unwrap();
}

/// What the migration recorded to report, by index, holders as `_id`s.
fn recorded(path: &Path) -> Vec<(String, Vec<i64>)> {
    let recorded =
        crate::migrate_v5::recorded_collisions(&Database::create(path).unwrap()).unwrap();
    let mut named: Vec<(String, Vec<i64>)> = recorded
        .iter()
        .flat_map(|(_, violations)| violations.iter())
        .map(|v| {
            let mut ids: Vec<i64> =
                v.holders.iter().map(|h| v2_int64_of(h).expect("schema 5 holders")).collect();
            ids.sort_unstable();
            (v.index.clone(), ids)
        })
        .collect();
    named.sort();
    named
}

#[test]
fn a_ttl_index_without_a_partial_filter_is_not_counted() {
    // Minor 8: only a partial filter can decide a document differently under
    // the exact comparator. A TTL index without one has nothing to count: no
    // scan of the collection, no step for it, and no line about a partial
    // filter.
    let (_dir, path) = small_schema_4(
        |engine| {
            let t = engine.create_collection("shop", "t").unwrap();
            engine
                .create_index_with(
                    "shop",
                    "t",
                    vec![IndexField::ascending("at")],
                    false,
                    Enforcement::Local,
                    Some("ttl_all".into()),
                    Some(3_600),
                    None,
                )
                .unwrap();
            let docs =
                (0..20_i64).map(|i| doc! {"_id": i, "at": bson::DateTime::now(), "tier": dec("1")});
            engine.insert_many(&t, docs.collect()).unwrap();
        },
        &[],
    );
    hooks::steps_seen();
    let (opened, logs) = crate::log_capture::logged(|| Engine::open(&path).map(drop));
    opened.unwrap();
    let steps = hooks::steps_seen();
    assert!(steps.iter().all(|s| s.kind != "counting a TTL index's selection"), "{steps:#?}");
    assert!(!logs.contains("partial filter now decides"), "{logs}");
    assert!(!logs.contains("before=\"not recorded\""), "{logs}");
}
