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
    hooks::FREE.with(|c| c.set(Some(100 << 20)));
    let (err, logs) = crate::log_capture::logged(|| Engine::open(&path).err());
    hooks::FREE.with(|c| c.set(None));
    let err = err.expect("refused");
    match &err {
        crate::StorageError::Io(e) if e.kind() == std::io::ErrorKind::StorageFull => {}
        other => panic!("the wrong refusal: {other}"),
    }
    let message = err.to_string();
    for wanted in
        ["needs", "100 MiB is free", "growth step of 2048 MiB", "still schema 4", "previous build"]
    {
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
fn the_requirement_is_the_growth_step_and_a_working_set_from_the_tables() {
    // Read without a walk: the tables' own counts and the file's size.
    let f = fixture();
    let path = f.copy();
    let plan = {
        let db = Database::create(&path).unwrap();
        crate::migrate_v5::preflight(&db, &path, 4).unwrap()
    };
    assert_eq!(plan.collections, 3, "items, strs and empty: gone was dropped");
    assert_eq!(plan.purge_pending, 1, "the dropped collection's rows");
    assert_eq!(plan.ranges, 3, "items, gone and strs hold rows; empty holds none");
    let rows: usize = f.ids.values().map(BTreeSet::len).sum::<usize>() + 9;
    assert_eq!(plan.docs_rows, rows as u64);
    assert!(plan.required_bytes > crate::migrate_v5::REDB_GROWTH_STEP_MIB << 20);
    assert!(
        plan.required_bytes < (crate::migrate_v5::REDB_GROWTH_STEP_MIB << 20) + (1 << 30),
        "a working set, not a copy of the store: {}",
        plan.required_bytes
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
fn a_finished_3_to_4_rebuilds_unreported_collisions_are_carried_and_reported_once() {
    // A 0.50 store a crash left between its 3 -> 4 rebuild's last commit and
    // the report: the recorded collisions are pending. One of them the exact
    // comparator dissolves (the documents' decimal `tier` was "at least 3" only
    // by ranking equal to 3), so only the carried row names it; the other the
    // refill finds again under the schema 5 form of its key, and it is reported
    // once, not twice.
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
    assert!(Engine::open(&path).is_err(), "premise: stopped before the report");
    assert!(!has_table(&path, "partial_rebuilt_unique_violations"), "carried, not kept");
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
        vec![("email_once".to_string(), vec![1, 2]), ("num_once".to_string(), vec![5, 6])],
        "each collision once, its holders in their schema 5 form"
    );
    let engine = Engine::open(&path).unwrap();
    assert_eq!(engine.unique_violations(), 2, "both reported");
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
    // documents undecidable and enforced nothing. Decided exactly, both are
    // selected, keyed under one email, and collide.
    let (_dir, path) = unique_store(
        &[("money_once", "email", Some(doc! {"balance": {"$gt": 0}}))],
        vec![
            doc! {"_id": 1_i64, "email": "x", "balance": dec("10")},
            doc! {"_id": 2_i64, "email": "y", "balance": dec("10")},
        ],
        &[("t", 2, doc! {"_id": 2_i64, "email": "x", "balance": dec("10")})],
    );
    let t = CollectionId::derive("shop", "t").0;
    let money = metas_of(&read_only(&path))[&t].index("money_once").unwrap().id;
    let undecided = entries_of(&read_only(&path), &BTreeSet::from([t]))
        .into_iter()
        .filter(|(_, i, k, _)| *i == money && k.as_slice() == crate::index::UNDECIDABLE)
        .count();
    assert_eq!(undecided, 2, "premise: 0.50 decided neither");
    let engine = Engine::open(&path).unwrap();
    assert_eq!(engine.unique_violations(), 1, "the collision 0.50 never enforced");
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
    );
    assert!(!tables_before.4.is_empty() && !tables_before.5.is_empty(), "premise: tombstones");
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
    );
    assert!(
        tables_after == tables_before,
        "the oplog, the vectors, the tombstones, the counts or the digests moved"
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
        assert!(
            matches!(phase, Some((crate::open_progress::OpenPhase::Migrating, total)) if total > 0),
            "{kind}: the open's progress said {phase:?}, not migrating with a total"
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
/// number, built by this build's engine and its keys put back by hand: the
/// size a drain has to be measured at.
fn indexed_schema_4(dir: &Path, n: i64) -> PathBuf {
    let path = dir.join("kimmy.redb");
    {
        let engine = Engine::open(&path).unwrap();
        let t = engine.create_collection("shop", "t").unwrap();
        index(&engine, "t", "by_n", vec![IndexField::ascending("n")], false);
        for start in (0..n).step_by(20_000) {
            let docs: Vec<Document> =
                (start..(start + 20_000).min(n)).map(|i| doc! {"_id": i, "n": i * 7}).collect();
            engine.insert_many(&t, docs).unwrap();
        }
    }
    let db = Database::create(&path).unwrap();
    let txn = db.begin_write().unwrap();
    rewind_keys_to_v1(&txn);
    txn.open_table(tables::META)
        .unwrap()
        .insert(tables::META_FORMAT_VERSION, [4u8].as_slice())
        .unwrap();
    txn.commit().unwrap();
    path
}

/// **The guard on draining by key** (`index::clear_index_entries`, never
/// redb's `retain_in`): the migration drains every index of a store holding
/// hundreds of thousands of entries, at the shipped chunk sizes, and the file
/// must stay near its size. `retain_in` with a predicate that keeps nothing
/// grew a file from 2.1 to 12 GiB over 742,858 entries (see
/// `clear_index_entries`); a drain written with it fails here.
///
/// ```text
/// cargo test --release -p kimmy-storage --lib -- --ignored --exact \
///     migrate_v5_tests::draining_by_key_keeps_the_file_near_its_size
/// ```
#[test]
#[ignore = "heavy: 300,000 entries; run it in release, see the doc comment"]
fn draining_by_key_keeps_the_file_near_its_size() {
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
        largest <= before + before / 2 + (64 << 20),
        "the migration grew the file from {} to {} MiB",
        before >> 20,
        largest >> 20
    );
}
