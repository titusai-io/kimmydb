//! The frozen schema-4 form of a document key.
//!
//! Schema 5 changes the bytes of every number key (`crate::keyenc`: a variable
//! length mantissa with an exactness mark). A mixed roll still has members that
//! speak schema 4 on the wire and hash schema-4 keys into the cut digest, so a
//! key that leaves the engine, or feeds the digest, keeps the schema-4 bytes for
//! every id that form can represent. This module is that form, **frozen**: it
//! shares no code with the live encoder, so a change to the live encoder cannot
//! move it.
//!
//! The form is a type, [`LegacyKey`], so that a stored (schema-5) key cannot be
//! handed to something expecting a legacy one. It is constructed by [`legacy_key`]
//! (from a `DocId`) and [`legacy_of_stored`] (from a stored key; total), and by
//! nothing else: there is no constructor from raw bytes, so a stored key passed
//! where a legacy one is wanted does not compile. Bytes received from a peer are
//! read back through both conversions, `legacy_of_stored(&stored_of_legacy(b))`,
//! which is the identity on every legacy key a 0.50 or a 0.51 member sends.
//!
//! A `Decimal128` id has no schema-4 form. It travels as `0xFF` followed by its
//! stored key, which no schema-4 key can begin with (the highest tag is
//! `MaxKey`, `0xF0`), and which only a schema-5 member can receive: the gate that
//! keeps such an id out of a cluster with an older member is separate.

use bson::Bson;

use crate::ids::DocId;
use crate::keyenc;

/// The first byte of a legacy key that carries a stored key it has no schema-4
/// form for (a `Decimal128` id).
pub const STORED_FORM_MARKER: u8 = 0xFF;

const TAG_NUMBER: u8 = 0x20;
const NUM_NEGATIVE: u8 = 0x02;
const NUM_ZERO: u8 = 0x03;
const NUM_POSITIVE: u8 = 0x04;
const EXPONENT_BIAS: i32 = 32_768;

/// A document key in the frozen schema-4 form.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct LegacyKey(Vec<u8>);

impl LegacyKey {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for LegacyKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// The schema-4 key of a document id.
pub fn legacy_key(id: &DocId) -> LegacyKey {
    match id {
        DocId::Int64(v) => LegacyKey(v1_int64(*v)),
        DocId::Decimal128(_) => {
            let mut out = vec![STORED_FORM_MARKER];
            out.extend_from_slice(&keyenc::encode(&id.to_bson()).expect("a decimal id encodes"));
            LegacyKey(out)
        }
        // No number inside: byte-identical in both schemas, and the live encoder
        // is the only encoder there is for them.
        other => LegacyKey(keyenc::encode(&other.to_bson()).expect("a non-decimal id encodes")),
    }
}

/// The legacy form of a stored (schema-5) key. **Total**: bytes that are not a
/// stored number key are returned as they are, so a caller that folds or ships
/// the result can never be refused by a malformed row.
pub fn legacy_of_stored(stored: &[u8]) -> LegacyKey {
    match stored_number(stored) {
        Some(StoredNumber::Int64(v)) => LegacyKey(v1_int64(v)),
        Some(StoredNumber::Other) => {
            let mut out = Vec::with_capacity(1 + stored.len());
            out.push(STORED_FORM_MARKER);
            out.extend_from_slice(stored);
            LegacyKey(out)
        }
        None => LegacyKey(stored.to_vec()),
    }
}

/// The stored (schema-5) key of a legacy key. **Total**, as [`legacy_of_stored`]:
/// bytes that are not a schema-4 integer key and not marked are returned as they
/// are.
pub fn stored_of_legacy(legacy: &[u8]) -> Vec<u8> {
    if legacy.first() == Some(&STORED_FORM_MARKER) {
        return legacy[1..].to_vec();
    }
    match v1_int64_of(legacy) {
        Some(v) => keyenc::encode(&Bson::Int64(v)).expect("an integer encodes"),
        None => legacy.to_vec(),
    }
}

/// The `i64` whose schema-4 key `key` is, or `None` for any other bytes: the
/// strict decoder the schema-5 migration re-keys a stored `DOCS` key with.
///
/// **Strict means canonical**: `Some(v)` is returned only when `key` is exactly
/// the schema-4 key of `v`, byte for byte, so a key this encoder never wrote is
/// refused rather than read as some nearby id. **It does not tell a schema-5 key
/// from a schema-4 one**: the schema-5 key of `0x5000008101800000` is the
/// canonical schema-4 key of `0x50007F8081018000`, and this returns the second.
/// Only the migration's durable phases say which form a key is in.
pub fn decode_v1(key: &[u8]) -> Option<i64> {
    v1_int64_of(key)
}

/// The schema-4 key of an `i64`: tag, sign, then a biased 2-byte exponent and the
/// 8-byte normalised mantissa, the whole body inverted when negative.
fn v1_int64(v: i64) -> Vec<u8> {
    if v == 0 {
        return vec![TAG_NUMBER, NUM_ZERO];
    }
    let magnitude = v.unsigned_abs();
    let shift = magnitude.leading_zeros();
    let exp = 63 - shift as i32;
    let biased = (exp + EXPONENT_BIAS) as u16;
    let mut body = [0u8; 10];
    body[..2].copy_from_slice(&biased.to_be_bytes());
    body[2..].copy_from_slice(&(magnitude << shift).to_be_bytes());
    let negative = v < 0;
    if negative {
        for byte in &mut body {
            *byte = !*byte;
        }
    }
    let mut out = Vec::with_capacity(12);
    out.push(TAG_NUMBER);
    out.push(if negative { NUM_NEGATIVE } else { NUM_POSITIVE });
    out.extend_from_slice(&body);
    out
}

/// The `i64` a schema-4 integer key encodes, if `key` is exactly one.
fn v1_int64_of(key: &[u8]) -> Option<i64> {
    match key {
        [TAG_NUMBER, NUM_ZERO] => Some(0),
        [TAG_NUMBER, sub @ (NUM_NEGATIVE | NUM_POSITIVE), body @ ..] if body.len() == 10 => {
            let negative = *sub == NUM_NEGATIVE;
            let mut body: [u8; 10] = body.try_into().ok()?;
            if negative {
                for byte in &mut body {
                    *byte = !*byte;
                }
            }
            let exp = i32::from(u16::from_be_bytes([body[0], body[1]])) - EXPONENT_BIAS;
            let mantissa = u64::from_be_bytes(body[2..].try_into().ok()?);
            // The mantissa's top bit is set by construction; one without it is not
            // a key this encoder wrote.
            if mantissa >> 63 == 0 || !(0..=63).contains(&exp) {
                return None;
            }
            // value = mantissa × 2^(exp − 63): an integer iff the low (63 − exp)
            // bits are clear.
            let fraction_bits = (63 - exp) as u32;
            if fraction_bits > 0 && mantissa & ((1u64 << fraction_bits) - 1) != 0 {
                return None;
            }
            let magnitude = mantissa >> fraction_bits;
            if negative {
                // `i64::MIN` is the one magnitude that does not negate.
                if magnitude == 1u64 << 63 {
                    Some(i64::MIN)
                } else {
                    i64::try_from(magnitude).ok().map(|m| -m)
                }
            } else {
                i64::try_from(magnitude).ok()
            }
        }
        _ => None,
    }
}

enum StoredNumber {
    Int64(i64),
    /// A finite number that is not an `i64`: a `Decimal128` id.
    Other,
}

/// Read a whole stored key as a finite number, if it is exactly one.
///
/// Layout (design §4.1): tag, sign, 2-byte biased exponent, the first 128 bits of
/// the binary expansion with trailing zero bytes stripped and `00` escaped as
/// `00 FF`, then `00 00` (exact) or `00 01` (a nonzero tail beyond 128 bits). A
/// negative number has its whole body inverted.
fn stored_number(stored: &[u8]) -> Option<StoredNumber> {
    let (&tag, rest) = stored.split_first()?;
    if tag != TAG_NUMBER {
        return None;
    }
    let (&sub, body) = rest.split_first()?;
    match sub {
        NUM_ZERO if body.is_empty() => return Some(StoredNumber::Int64(0)),
        NUM_POSITIVE | NUM_NEGATIVE => {}
        _ => return None,
    }
    let negative = sub == NUM_NEGATIVE;
    let byte = |i: usize| body.get(i).map(|b| if negative { !*b } else { *b });
    let exp = i32::from(u16::from_be_bytes([byte(0)?, byte(1)?])) - EXPONENT_BIAS;

    // Unescape M up to its terminator.
    let mut mantissa = Vec::with_capacity(16);
    let mut i = 2;
    let exact = loop {
        match byte(i)? {
            0x00 => match byte(i + 1)? {
                0x00 => break true,
                0x01 => break false,
                0xFF => {
                    mantissa.push(0);
                    i += 2;
                }
                _ => return None,
            },
            b => {
                mantissa.push(b);
                i += 1;
            }
        }
    };
    // The terminator must be the last thing in the key, M must be non-empty with
    // its top bit set and be at most 16 bytes, and a stripped M ends in a
    // nonzero byte.
    if i + 2 != body.len()
        || mantissa.is_empty()
        || mantissa.len() > 16
        || mantissa[0] >> 7 == 0
        || *mantissa.last()? == 0
    {
        return None;
    }
    if !exact || !(0..=63).contains(&exp) {
        return Some(StoredNumber::Other);
    }
    // Pad M to 128 bits. value = M × 2^(exp − 127): an integer iff the bits below
    // the 2^0 place are clear.
    let mut padded = [0u8; 16];
    padded[..mantissa.len()].copy_from_slice(&mantissa);
    let m = u128::from_be_bytes(padded);
    let fraction_bits = (127 - exp) as u32;
    if m & ((1u128 << fraction_bits) - 1) != 0 {
        return Some(StoredNumber::Other);
    }
    let magnitude = m >> fraction_bits;
    let value = if negative {
        if magnitude == 1u128 << 63 {
            Some(i64::MIN)
        } else {
            i64::try_from(magnitude).ok().map(|v| -v)
        }
    } else {
        i64::try_from(magnitude).ok()
    };
    Some(value.map_or(StoredNumber::Other, StoredNumber::Int64))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The schema-5 key of an `i64`, built here without the live encoder, so the
    /// conversions are tested against the layout of the design and not against
    /// whatever the encoder happens to emit.
    fn v2_int64(v: i64) -> Vec<u8> {
        if v == 0 {
            return vec![TAG_NUMBER, NUM_ZERO];
        }
        let magnitude = v.unsigned_abs();
        let exp = 63 - magnitude.leading_zeros() as i32;
        let mut m = ((magnitude as u128) << (127 - exp)).to_be_bytes().to_vec();
        while m.last() == Some(&0) {
            m.pop();
        }
        let mut body = ((exp + EXPONENT_BIAS) as u16).to_be_bytes().to_vec();
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
        let mut out = vec![TAG_NUMBER, if v < 0 { NUM_NEGATIVE } else { NUM_POSITIVE }];
        out.extend(body);
        out
    }

    fn edges() -> Vec<i64> {
        let mut out = vec![0, 1, -1, 2, 5, -5, 255, 256, 257, 65_535, 65_536, i64::MAX, i64::MIN];
        for k in 1..63 {
            for d in [-1i64, 0, 1] {
                out.push((1i64 << k) + d);
                out.push(-((1i64 << k) + d));
            }
        }
        out.push(i64::MIN + 1);
        out.push(i64::MAX - 1);
        out
    }

    #[test]
    fn the_schema_4_bytes_of_an_integer_are_frozen() {
        // Golden bytes of the 0.50.0 encoder.
        assert_eq!(v1_int64(0), [0x20, 0x03]);
        assert_eq!(
            v1_int64(5),
            [0x20, 0x04, 0x80, 0x02, 0xA0, 0, 0, 0, 0, 0, 0, 0],
            "the mantissa is the whole 8 bytes in schema 4"
        );
        assert_eq!(
            v1_int64(-5),
            [0x20, 0x02, 0x7F, 0xFD, 0x5F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
        assert_eq!(
            v1_int64(i64::MIN),
            [0x20, 0x02, 0x7F, 0xC0, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
    }

    #[test]
    fn the_legacy_bytes_equal_what_0_50_0_wrote() {
        // `keyenc::encode(&Bson::Int64(v))` at v0.50.0, printed by a build of that
        // tag and checked in. The frozen encoder must never drift from it.
        let golden: [(i64, &str); 21] = [
            (i64::MIN, "20027fc07fffffffffffffff"),
            (-9_223_372_036_854_775_807, "20027fc10000000000000001"),
            (-4_611_686_018_427_387_904, "20027fc17fffffffffffffff"),
            (-1_099_511_627_776, "20027fd77fffffffffffffff"),
            (-5, "20027ffd5fffffffffffffff"),
            (-1, "20027fff7fffffffffffffff"),
            (0, "2003"),
            (1, "200480008000000000000000"),
            (2, "200480018000000000000000"),
            (5, "20048002a000000000000000"),
            (255, "20048007ff00000000000000"),
            (256, "200480088000000000000000"),
            (257, "200480088080000000000000"),
            (65_535, "2004800fffff000000000000"),
            (65_536, "200480108000000000000000"),
            (1_099_511_627_776, "200480288000000000000000"),
            (9_007_199_254_740_993, "200480358000000000000400"),
            (4_611_686_018_427_387_904, "2004803e8000000000000000"),
            (4_611_686_018_427_387_905, "2004803e8000000000000002"),
            (9_223_372_036_854_775_806, "2004803efffffffffffffffc"),
            (i64::MAX, "2004803efffffffffffffffe"),
        ];
        for (v, hex) in golden {
            let want: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            assert_eq!(legacy_key(&DocId::Int64(v)).as_bytes(), want, "{v}");
        }
    }

    #[test]
    fn a_legacy_integer_key_round_trips_through_its_value() {
        for v in edges() {
            assert_eq!(v1_int64_of(&v1_int64(v)), Some(v), "{v}");
        }
    }

    #[test]
    fn a_stored_integer_converts_to_its_legacy_key_and_back() {
        for v in edges() {
            let stored = v2_int64(v);
            let legacy = legacy_of_stored(&stored);
            assert_eq!(legacy.as_bytes(), v1_int64(v), "{v}");
            assert_eq!(legacy, legacy_key(&DocId::Int64(v)), "{v}");
            assert_eq!(stored_of_legacy(legacy.as_bytes()), stored, "{v}");
        }
    }

    #[test]
    fn a_key_with_no_number_in_it_is_the_same_in_both_forms() {
        let ids = [
            DocId::String("hello".into()),
            DocId::String(String::new()),
            DocId::String("a\0b".into()),
            DocId::ObjectId(bson::oid::ObjectId::new()),
            DocId::Binary(vec![0, 1, 255]),
            DocId::Uuid(uuid::Uuid::new_v4()),
        ];
        for id in ids {
            let stored = keyenc::encode(&id.to_bson()).unwrap();
            assert_eq!(legacy_key(&id).as_bytes(), stored, "{id:?}");
            assert_eq!(legacy_of_stored(&stored).as_bytes(), stored, "{id:?}");
            assert_eq!(stored_of_legacy(&stored), stored, "{id:?}");
        }
    }

    #[test]
    fn both_conversions_are_total() {
        // Bytes that are neither form come back as they are, whatever they are.
        let junk: [&[u8]; 8] = [
            &[],
            &[0x20],
            &[0x20, 0x04],
            &[0x20, 0x04, 0x80],
            &[0x20, 0x07, 1, 2, 3],
            &[0x20, 0x04, 0x80, 0x02, 0xA0, 0x00, 0x07],
            &[0x00],
            &[0x30, 0x41, 0x00],
        ];
        for key in junk {
            assert_eq!(legacy_of_stored(key).as_bytes(), key);
            assert_eq!(stored_of_legacy(key), key);
        }
        // A schema-4 key handed to `legacy_of_stored` is not a stored key; it is
        // hashed as it is.
        let v1 = v1_int64(5);
        assert_eq!(legacy_of_stored(&v1).as_bytes(), v1);
    }

    #[test]
    fn decode_v1_answers_only_for_the_exact_bytes_of_an_integer_key() {
        // Every shape a schema-4 integer key cannot have: the wrong length, a
        // mantissa without its top bit, a fraction, an exponent out of range, a
        // magnitude past `i64`, a sub-tag that is not a sign, a zero with a body.
        let refused: [&[u8]; 12] = [
            &[],
            &[0x20],
            &[0x20, 0x03, 0x00],
            &[0x20, 0x04, 0x80, 0x02, 0xA0, 0, 0, 0, 0, 0, 0],
            &[0x20, 0x04, 0x80, 0x02, 0xA0, 0, 0, 0, 0, 0, 0, 0, 0],
            &[0x20, 0x04, 0x80, 0x02, 0x50, 0, 0, 0, 0, 0, 0, 0],
            &[0x20, 0x04, 0x80, 0x02, 0xA0, 0, 0, 0, 0, 0, 0, 0x01],
            &[0x20, 0x04, 0x80, 0x40, 0x80, 0, 0, 0, 0, 0, 0, 0],
            &[0x20, 0x04, 0x7F, 0xFF, 0x80, 0, 0, 0, 0, 0, 0, 0],
            &[0x20, 0x04, 0x80, 0x3F, 0x80, 0, 0, 0, 0, 0, 0, 0],
            &[0x20, 0x05, 0x80, 0x02, 0xA0, 0, 0, 0, 0, 0, 0, 0],
            &[0x30, 0x04, 0x80, 0x02, 0xA0, 0, 0, 0, 0, 0, 0, 0],
        ];
        for key in refused {
            assert_eq!(decode_v1(key), None, "{key:02x?}");
        }
        // And whatever it does accept is the canonical key of what it returns:
        // every 12-byte string over a few varied bodies, both signs.
        let mut accepted = 0;
        for sub in [NUM_NEGATIVE, NUM_POSITIVE] {
            for exp in [0u16, 1, 31, 62, 63, 64, 0x7FFF, 0x8000, 0x8001, 0x803E, 0x803F, 0x8040] {
                for mantissa in [0u64, 1, 0x8000_0000_0000_0000, 0xA000_0000_0000_0000, u64::MAX] {
                    let mut key = vec![TAG_NUMBER, sub];
                    key.extend_from_slice(&exp.to_be_bytes());
                    key.extend_from_slice(&mantissa.to_be_bytes());
                    if let Some(v) = decode_v1(&key) {
                        accepted += 1;
                        assert_eq!(v1_int64(v), key, "{key:02x?} read as {v}");
                    }
                }
            }
        }
        assert!(accepted > 0, "premise: some of these are keys");
        // The A2 pair: a schema-5 key that is also a canonical schema-4 key, read
        // as the other id. Strictness is not detection.
        let a = 0x5000_0081_0180_0000_i64;
        let b = 0x5000_7F80_8101_8000_i64;
        assert_eq!(v2_int64(a), v1_int64(b), "premise: the pair");
        assert_eq!(decode_v1(&v2_int64(a)), Some(b));
    }

    #[test]
    fn a_non_integer_number_travels_marked_with_its_stored_key() {
        // 1.5: exp 0, M = 1.1b = C0, exact.
        let stored = [0x20, 0x04, 0x80, 0x00, 0xC0, 0x00, 0x00];
        let legacy = legacy_of_stored(&stored);
        assert_eq!(legacy.as_bytes()[0], STORED_FORM_MARKER);
        assert_eq!(&legacy.as_bytes()[1..], stored);
        assert_eq!(stored_of_legacy(legacy.as_bytes()), stored);
    }
}
