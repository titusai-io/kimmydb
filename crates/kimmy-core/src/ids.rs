//! Identifier types.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::decimal::{self, Decimal};

/// Stable identity of a node in the cluster.
///
/// Generated once on first start and persisted, so a restarting node keeps its
/// identity — losing it would make the node a stranger to its own prior writes
/// and break last-writer-wins tiebreaks.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(Uuid);

/// Serialized as its hyphenated string form, always.
///
/// `Uuid`'s own serde picks between a string and raw bytes based on whether the
/// format calls itself human-readable, and BSON answers that question
/// differently when writing than when reading — so a `NodeId` written into a
/// BSON frame could not be read back out of one.
///
/// A string is also what makes a `NodeId` usable as a **map key**: BSON
/// document keys must be strings, and [`crate::VersionVector`] is keyed by node.
/// Fixing the representation here rather than at each use site means there is
/// one answer rather than one per format.
impl Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl NodeId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Node ids appear in nearly every log line; the full UUID is noise.
        write!(f, "Node({:.8})", self.0.simple())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for NodeId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

/// Internal handle for a collection.
///
/// Collections are addressed by `(database, name)` at the API edge, but keyed
/// by this dense integer on disk so that renames stay cheap and index keys stay
/// short.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct CollectionId(pub u64);

/// Serialized as a signed 64-bit integer, reinterpreting the bits.
///
/// **BSON has no unsigned 64-bit type.** The id is a hash, so it uses the whole
/// `u64` range, and roughly half of all collection names derive one above
/// `i64::MAX` — which the derived `Serialize` refuses to encode, with
/// `Unsigned integer N cannot fit into BSON`.
///
/// That made every oplog entry naming such a collection unsendable, so the
/// collection and its documents simply never replicated. The write succeeded
/// locally and the peer's log carried one warning per round, which is about as
/// quiet as a distributed-systems bug gets. Whether a given collection worked
/// depended on its name hashing below the halfway point.
///
/// Reinterpreting the bits is lossless and round-trips exactly. Ids that
/// already encoded — the ones below `i64::MAX` — keep the identical
/// representation, so this widens what works without changing what worked.
///
/// The **on-disk** form is untouched: [`crate::ids`] values are persisted by
/// the hand-rolled codec as raw big-endian bytes, not through serde, so no
/// migration is involved.
///
/// Same lesson as [`NodeId`] above: a type that crosses a format boundary needs
/// one fixed representation chosen here, not one inferred per format.
impl Serialize for CollectionId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.0 as i64)
    }
}

impl<'de> Deserialize<'de> for CollectionId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = CollectionId;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a 64-bit collection id")
            }

            // The representation this type writes.
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(CollectionId(v as u64))
            }

            // Accepted so that a self-describing format which can hold the
            // value unsigned — JSON, or a debug dump — reads back correctly
            // rather than failing on a number it represented faithfully.
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(CollectionId(v))
            }
        }

        deserializer.deserialize_i64(Visitor)
    }
}

impl CollectionId {
    /// The id of `db.name`, on every node, without coordination.
    ///
    /// Derived rather than allocated. A counter is node-local, so two nodes
    /// that create the same collection in a different order end up with
    /// different ids for it — and since every oplog entry names its collection
    /// by id, a replicated write would then be applied to whichever collection
    /// happened to hold that number locally. Silently, and only for peers whose
    /// creation order differed, which is the worst way for it to fail.
    ///
    /// Deriving the id from the name removes the problem rather than
    /// coordinating it away: no agreement round, no leader, and a node that has
    /// never met a peer computes the same answer.
    ///
    /// # Why FNV-1a
    ///
    /// The hash has to be **stable forever** — it is baked into every on-disk
    /// key. `DefaultHasher` is explicitly not guaranteed stable across Rust
    /// releases, so an upgrade could silently repoint every collection. FNV-1a
    /// is a handful of lines, has no dependency, and is fully specified.
    ///
    /// Collisions are checked at creation time rather than trusted; see
    /// `Engine::create_collection`.
    pub fn derive(db: &str, name: &str) -> Self {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        let mut hash = OFFSET;
        // The NUL separator keeps `("a", "bc")` and `("ab", "c")` distinct.
        // Without it they hash identically, which would merge two collections.
        for byte in db.as_bytes().iter().chain(b"\0").chain(name.as_bytes()) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }

        // Zero is reserved so that an uninitialised or defaulted id cannot
        // silently address a real collection.
        Self(if hash == 0 { 1 } else { hash })
    }

    pub const fn to_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl fmt::Display for CollectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "coll#{}", self.0)
    }
}

/// A document's `_id`.
///
/// Mongo permits any BSON type here, and so do we — but the value must be
/// encodable as an order-preserving key, which rules out documents and arrays.
/// [`DocId::try_from_bson`] enforces that.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub enum DocId {
    ObjectId(bson::oid::ObjectId),
    String(String),
    Int64(i64),
    Binary(Vec<u8>),
    Uuid(Uuid),
    /// A `Decimal128` that is not an integer in `i64` range. An integer one is
    /// [`DocId::Int64`]. Appended last: serde numbers variants by position.
    Decimal128(Decimal128Id),
}

impl DocId {
    /// Mint a fresh identifier. Defaults to ObjectId for Mongo familiarity.
    pub fn generate() -> Self {
        Self::ObjectId(bson::oid::ObjectId::new())
    }

    /// Convert from a user-supplied `_id` value, rejecting types that cannot
    /// serve as a primary key.
    ///
    /// A `Decimal128` that is an integer in `i64` range is the `Int64` of that
    /// value — `_id: Decimal128("5")`, `Int32(5)` and `Int64(5)` are one document,
    /// as the exact order says they are one value. Any other finite `Decimal128`
    /// is [`DocId::Decimal128`]; NaN and the infinities are refused (legal keys,
    /// poor identifiers, easy to relax later).
    pub fn try_from_bson(value: &bson::Bson) -> crate::Result<Self> {
        match value {
            bson::Bson::ObjectId(oid) => Ok(Self::ObjectId(*oid)),
            bson::Bson::String(s) => Ok(Self::String(s.clone())),
            bson::Bson::Int32(i) => Ok(Self::Int64(i64::from(*i))),
            bson::Bson::Int64(i) => Ok(Self::Int64(*i)),
            bson::Bson::Binary(b) => Ok(Self::Binary(b.bytes.clone())),
            bson::Bson::Decimal128(d) => match decimal::decode(d) {
                Decimal::Zero => Ok(Self::Int64(0)),
                finite @ Decimal::Finite { .. } => {
                    let id = Decimal128Id::from_normalized(finite.normalized());
                    Ok(id.as_i64().map_or(Self::Decimal128(id), Self::Int64))
                }
                Decimal::Nan | Decimal::NegInfinity | Decimal::PosInfinity => {
                    Err(crate::Error::InvalidDocumentId { found: "Decimal128 (not finite)".into() })
                }
            },
            other => Err(crate::Error::InvalidDocumentId {
                found: format!("{:?}", other.element_type()),
            }),
        }
    }

    pub fn to_bson(&self) -> bson::Bson {
        match self {
            Self::ObjectId(oid) => bson::Bson::ObjectId(*oid),
            Self::String(s) => bson::Bson::String(s.clone()),
            Self::Int64(i) => bson::Bson::Int64(*i),
            Self::Binary(b) => bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: b.clone(),
            }),
            Self::Uuid(u) => bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Uuid,
                bytes: u.as_bytes().to_vec(),
            }),
            Self::Decimal128(d) => bson::Bson::Decimal128(d.to_decimal128()),
        }
    }
}

impl fmt::Display for DocId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObjectId(oid) => write!(f, "{oid}"),
            Self::String(s) => write!(f, "{s}"),
            Self::Int64(i) => write!(f, "{i}"),
            Self::Binary(b) => write!(f, "bin({} bytes)", b.len()),
            Self::Uuid(u) => write!(f, "{u}"),
            Self::Decimal128(d) => write!(f, "{d}"),
        }
    }
}

/// A finite, non-zero `Decimal128` in canonical form, as a document id.
///
/// The value is `±coefficient × 10^exponent` with the coefficient stripped of
/// trailing decimal zeros and the exponent raised to match, so **equal values are
/// equal ids** (`1.0` and `1.00` are one id). The exponent may therefore leave
/// BID's range (`1E+6144` is `1E6144`); [`Decimal128Id::to_decimal128`] pads it
/// back. Zero cannot occur (it is `Int64(0)`), nor can an integer that fits
/// `i64`.
///
/// The byte form (the stored codec and the serde form) is 19 bytes: the sign, the
/// exponent as a big-endian `i16`, the coefficient as a big-endian `u128`. BSON
/// has no 128-bit integer, so the coefficient is never a BSON integer.
///
/// The derived order is the order of those bytes' fields, **not** numeric order.
/// Nothing may order ids by it; the numeric order is the key's.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Decimal128Id {
    negative: bool,
    exponent: i16,
    coefficient: u128,
}

/// The width of [`Decimal128Id::to_bytes`].
pub const DECIMAL128_ID_LEN: usize = 19;

impl Decimal128Id {
    fn from_normalized(d: Decimal) -> Self {
        let Decimal::Finite { negative, coefficient, exponent } = d else {
            unreachable!("only finite values are ids")
        };
        Self { negative, exponent, coefficient }
    }

    /// The canonical bytes: `sign (1) ‖ exponent (2) ‖ coefficient (16)`.
    pub fn to_bytes(self) -> [u8; DECIMAL128_ID_LEN] {
        let mut out = [0u8; DECIMAL128_ID_LEN];
        out[0] = u8::from(self.negative);
        out[1..3].copy_from_slice(&self.exponent.to_be_bytes());
        out[3..].copy_from_slice(&self.coefficient.to_be_bytes());
        out
    }

    /// Read the canonical bytes, refusing anything [`Self::to_bytes`] would not
    /// have written: another length, a sign other than 0 or 1, a zero or
    /// out-of-range coefficient, a coefficient with a trailing zero digit, an
    /// exponent below BID's minimum or above what the coefficient can reach, or a
    /// value that is an `i64` (that is `DocId::Int64`).
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; DECIMAL128_ID_LEN] = bytes.try_into().ok()?;
        let negative = match bytes[0] {
            0 => false,
            1 => true,
            _ => return None,
        };
        let exponent = i16::from_be_bytes([bytes[1], bytes[2]]);
        let coefficient = u128::from_be_bytes(bytes[3..].try_into().ok()?);
        let id = Self { negative, exponent, coefficient };
        let canonical = coefficient != 0
            && coefficient <= decimal::MAX_COEFFICIENT
            && coefficient % 10 != 0
            && exponent >= decimal::MIN_EXPONENT
            // The largest exponent any value can normalise to: 1E+6144.
            && i32::from(exponent) <= i32::from(decimal::MAX_EXPONENT) + 33
            && id.as_i64().is_none();
        canonical.then_some(id)
    }

    /// The `Decimal128` this id denotes, in canonical BID (the exponent padded
    /// back into range, which always fits because the original did).
    pub fn to_decimal128(self) -> bson::Decimal128 {
        decimal::pack_padded(self.negative, self.coefficient, self.exponent)
    }

    /// The value as an `i64`, if it is an integer in range.
    fn as_i64(self) -> Option<i64> {
        let mut magnitude = self.coefficient;
        if self.exponent < 0 {
            // Normalised: no trailing zeros to shed, so a negative exponent is a
            // fraction.
            return None;
        }
        for _ in 0..self.exponent {
            magnitude = magnitude.checked_mul(10)?;
            if magnitude > 1u128 << 63 {
                return None;
            }
        }
        if self.negative {
            if magnitude == 1u128 << 63 {
                Some(i64::MIN)
            } else {
                i64::try_from(magnitude).ok().map(|m| -m)
            }
        } else {
            i64::try_from(magnitude).ok()
        }
    }
}

/// A bounded canonical scientific form that is also injective: the normalised
/// digits and the adjusted exponent, never the plain decimal (about 6,000
/// characters at either extreme) and never truncated digits. A vector chunk id is
/// `{source}#{chunk}`, so two ids that printed alike would share chunks, and a
/// duplicate-key message must name the right id.
impl fmt::Display for Decimal128Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digits = self.coefficient.to_string();
        let adjusted = i32::from(self.exponent) + digits.len() as i32 - 1;
        if self.negative {
            f.write_str("-")?;
        }
        f.write_str(&digits[..1])?;
        if digits.len() > 1 {
            write!(f, ".{}", &digits[1..])?;
        }
        write!(f, "E{adjusted:+}")
    }
}

impl Serialize for Decimal128Id {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde_bytes::serialize(&self.to_bytes()[..], serializer)
    }
}

impl<'de> Deserialize<'de> for Decimal128Id {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        Self::from_bytes(&bytes)
            .ok_or_else(|| serde::de::Error::custom("not a canonical Decimal128 document id"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_id_rejects_unkeyable_types() {
        let doc = bson::Bson::Document(bson::doc! { "nested": 1 });
        assert!(DocId::try_from_bson(&doc).is_err());
        let arr = bson::Bson::Array(vec![bson::Bson::Int32(1)]);
        assert!(DocId::try_from_bson(&arr).is_err());
    }

    #[test]
    fn doc_id_normalizes_int32_to_int64() {
        let id = DocId::try_from_bson(&bson::Bson::Int32(7)).unwrap();
        assert_eq!(id, DocId::Int64(7));
        // Otherwise `{_id: 7}` inserted as int32 and looked up as int64 would miss.
        assert_eq!(DocId::try_from_bson(&bson::Bson::Int64(7)).unwrap(), id);
    }

    fn dec(s: &str) -> bson::Bson {
        bson::Bson::Decimal128(s.parse().unwrap())
    }

    fn id(s: &str) -> DocId {
        DocId::try_from_bson(&dec(s)).unwrap()
    }

    #[test]
    fn an_integral_decimal_in_i64_range_is_the_int64_of_its_value() {
        for (text, want) in [
            ("5", 5i64),
            ("5.00", 5),
            ("50E-1", 5),
            ("0.5E+1", 5),
            ("-0", 0),
            ("0", 0),
            ("0E-100", 0),
            ("0E+6111", 0),
            ("9223372036854775807", i64::MAX),
            ("-9223372036854775808", i64::MIN),
            ("-92233720368547758080E-1", i64::MIN),
            ("1E+18", 1_000_000_000_000_000_000),
        ] {
            assert_eq!(id(text), DocId::Int64(want), "{text}");
        }
        // Int32, Int64 and an equal decimal are one id.
        assert_eq!(id("7"), DocId::try_from_bson(&bson::Bson::Int32(7)).unwrap());
    }

    #[test]
    fn a_non_zero_non_i64_decimal_is_a_decimal_id() {
        for text in [
            "1.5",
            "-1.5",
            "0.1",
            "9223372036854775808",
            "-9223372036854775809",
            "1E+19",
            "1E+6112",
            "1E+6144",
            "9.999999999999999999999999999999999E+6144",
            "1E-6176",
            "1.000000000000000000000000000000001E-6143",
        ] {
            assert!(matches!(id(text), DocId::Decimal128(_)), "{text}");
        }
    }

    #[test]
    fn equal_decimals_are_one_id_and_unequal_ones_are_not() {
        assert_eq!(id("1.5"), id("1.50"));
        assert_eq!(id("1.5"), id("15E-1"));
        assert_eq!(id("1.5").to_string(), id("1.500").to_string());
        assert_ne!(id("1.5"), id("-1.5"));
        assert_ne!(id("1.5"), id("1.6"));
        assert_ne!(id("1E+6144"), id("1E+6143"));
    }

    #[test]
    fn nan_and_infinity_are_refused_as_ids() {
        for text in ["NaN", "-NaN", "Infinity", "-Infinity"] {
            assert!(DocId::try_from_bson(&dec(text)).is_err(), "{text}");
        }
    }

    #[test]
    fn to_bson_repads_an_exponent_above_the_bid_range() {
        // Normalised `1E+6144` is c = 1, e = 6144, which BID cannot hold: packed
        // straight in it would read back as a non-canonical zero and key like
        // `_id: 0`.
        for text in ["1E+6112", "1E+6144", "-1E+6144", "9.999999999999999999999999999999999E+6144"]
        {
            let original = dec(text);
            let back = id(text).to_bson();
            assert_eq!(back, original, "{text}");
            let bson::Bson::Decimal128(d) = back else { panic!("decimal") };
            assert_ne!(decimal::decode(&d), Decimal::Zero, "{text} read back as zero");
        }
    }

    #[test]
    fn the_byte_form_round_trips_and_is_canonical() {
        for text in ["1.5", "-1.5", "1E+6144", "1E-6176", "0.1", "1E+19", "123456789.987654321"] {
            let DocId::Decimal128(d) = id(text) else { panic!("{text}") };
            let bytes = d.to_bytes();
            assert_eq!(bytes.len(), DECIMAL128_ID_LEN);
            assert_eq!(Decimal128Id::from_bytes(&bytes), Some(d), "{text}");
        }
        let DocId::Decimal128(d) = id("1.5") else { panic!() };
        let mut bytes = d.to_bytes();
        // Everything to_bytes would not write is refused.
        assert_eq!(Decimal128Id::from_bytes(&bytes[..18]), None);
        bytes[0] = 2;
        assert_eq!(Decimal128Id::from_bytes(&bytes), None, "a sign other than 0 or 1");
        let mut zero = d.to_bytes();
        zero[3..].fill(0);
        assert_eq!(Decimal128Id::from_bytes(&zero), None, "a zero coefficient");
        let mut trailing = d.to_bytes();
        trailing[3..].copy_from_slice(&150u128.to_be_bytes());
        assert_eq!(Decimal128Id::from_bytes(&trailing), None, "a coefficient ending in 0");
        let mut integer = d.to_bytes();
        integer[1..3].copy_from_slice(&0i16.to_be_bytes());
        integer[3..].copy_from_slice(&5u128.to_be_bytes());
        assert_eq!(Decimal128Id::from_bytes(&integer), None, "an i64 is DocId::Int64");
    }

    #[test]
    fn the_serde_form_is_the_19_bytes() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct W {
            id: DocId,
        }
        for text in ["1.5", "-0.1", "1E+6144", "1E-6176"] {
            let w = W { id: id(text) };
            let doc = bson::serialize_to_document(&w).unwrap();
            // An enum is `{ variant: payload }`; the payload is binary.
            let inner = doc.get_document("id").unwrap();
            let bson::Bson::Binary(b) = inner.get("Decimal128").expect("variant") else {
                panic!("the payload is binary: {doc:?}")
            };
            assert_eq!(b.bytes.len(), DECIMAL128_ID_LEN, "{text}");
            let back: W = bson::deserialize_from_document(doc).unwrap();
            assert_eq!(back, w, "{text}");
        }
        // Appended last: the earlier variants keep their serde position.
        let doc = bson::serialize_to_document(&W { id: DocId::Int64(3) }).unwrap();
        assert!(doc.get_document("id").unwrap().contains_key("Int64"));
    }

    #[test]
    fn a_corrupt_decimal_id_is_refused_on_decode_not_trusted() {
        #[derive(Deserialize, Debug)]
        struct W {
            #[allow(dead_code)]
            id: DocId,
        }
        let doc = bson::doc! { "id": { "Decimal128": bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![0u8; DECIMAL128_ID_LEN],
        } } };
        assert!(bson::deserialize_from_document::<W>(doc).is_err());
    }

    #[test]
    fn display_is_bounded_canonical_and_injective() {
        let texts = [
            "1.5",
            "-1.5",
            "0.15",
            "15",
            "1.50",
            "150E-2",
            "1E+19",
            "1E+6144",
            "1E+6112",
            "9.999999999999999999999999999999999E+6144",
            "1E-6176",
            "1.5E+19",
            "1.5E-19",
        ];
        let mut seen = std::collections::HashMap::new();
        for text in texts {
            let shown = id(text).to_string();
            assert!(shown.len() <= 48, "{text} prints {} chars", shown.len());
            if let Some(prior) = seen.insert(shown.clone(), text) {
                let a: bson::Decimal128 = prior.parse().unwrap();
                let b: bson::Decimal128 = text.parse().unwrap();
                assert_eq!(
                    decimal::decode(&a).normalized(),
                    decimal::decode(&b).normalized(),
                    "{prior} and {text} print alike ({shown}) but are different values"
                );
            }
        }
        assert_eq!(id("1.5").to_string(), "1.5E+0");
        assert_eq!(id("-0.15").to_string(), "-1.5E-1");
        assert_eq!(id("1E+19").to_string(), "1E+19");
        assert_eq!(id("1.50").to_string(), id("1.5").to_string());
    }

    #[test]
    fn node_id_round_trips_through_bytes() {
        let id = NodeId::generate();
        assert_eq!(NodeId::from_bytes(id.to_bytes()), id);
    }
    #[test]
    fn a_collection_id_is_the_same_on_every_node() {
        // The whole point: no coordination, no creation-order dependence.
        assert_eq!(CollectionId::derive("shop", "orders"), CollectionId::derive("shop", "orders"));
    }

    #[test]
    fn different_collections_get_different_ids() {
        let ids = [
            CollectionId::derive("shop", "orders"),
            CollectionId::derive("shop", "customers"),
            CollectionId::derive("hr", "orders"),
            CollectionId::derive("", "orders"),
        ];
        for (i, a) in ids.iter().enumerate() {
            for b in &ids[i + 1..] {
                assert_ne!(a, b, "distinct collections must not share an id");
            }
        }
    }

    #[test]
    fn the_separator_keeps_split_points_distinct() {
        // Without a separator, ("a","bc") and ("ab","c") hash the same input
        // and two unrelated collections would silently become one.
        assert_ne!(CollectionId::derive("a", "bc"), CollectionId::derive("ab", "c"));
        assert_ne!(CollectionId::derive("ab", ""), CollectionId::derive("a", "b"));
    }

    #[test]
    fn zero_is_never_produced() {
        // Reserved, so a defaulted id cannot address a real collection.
        assert_ne!(CollectionId::derive("", "").0, 0);
    }

    #[test]
    fn ids_are_pinned_to_exact_values() {
        // These are on-disk keys. If this test fails, every existing database
        // has just been repointed — the hash is not free to change, and a
        // "harmless refactor" of `derive` is not harmless.
        //
        // Cross-checked against an independent FNV-1a implementation rather
        // than recorded from this one, so the test would also catch this
        // implementation being wrong in a self-consistent way.
        assert_eq!(CollectionId::derive("shop", "orders").0, 0x53ad_8d42_0376_3a3a);
        assert_eq!(CollectionId::derive("__kimmy", "__users").0, 0x1f65_9d82_1893_00f2);
        assert_eq!(CollectionId::derive("a", "bc").0, 0xab40_f682_0d40_b523);
        assert_eq!(CollectionId::derive("ab", "c").0, 0xfd61_c083_ef20_0867);
    }

    /// A collection id above `i64::MAX`, which BSON cannot hold unsigned.
    ///
    /// `("c", "t")` derives `17808914187510290470`. Picked from a real failure
    /// rather than constructed: it is the collection that would not replicate
    /// between three containers.
    const HIGH: (&str, &str) = ("c", "t");

    #[test]
    fn a_high_collection_id_survives_bson() {
        // The bug this was written for: the derived `Serialize` refused any id
        // above `i64::MAX` with "Unsigned integer N cannot fit into BSON", so
        // every oplog entry naming such a collection was unsendable and the
        // collection silently never replicated.
        let id = CollectionId::derive(HIGH.0, HIGH.1);
        assert!(id.0 > i64::MAX as u64, "the fixture must actually exercise the high half");

        let bytes = bson::serialize_to_vec(&Wrapper { collection: id })
            .expect("a collection id must encode into BSON");
        let back: Wrapper = bson::deserialize_from_slice(&bytes).expect("and must decode back");

        assert_eq!(back.collection, id, "the id must survive the round trip exactly");
    }

    /// A struct rather than `bson::doc!`, because the macro wants
    /// `Into<Bson>` while the replication path goes through serde — and serde
    /// is where the bug lived.
    #[derive(Serialize, Deserialize)]
    struct Wrapper {
        collection: CollectionId,
    }

    #[test]
    fn every_derived_id_encodes_regardless_of_which_half_it_lands_in() {
        // The failure was a coin flip per collection name — roughly half of all
        // names derive an id above `i64::MAX`. A test using one hard-coded name
        // therefore proves nothing about the other half, which is exactly how
        // this reached main: the replication tests use "shop"."orders", and it
        // happens to hash low.
        let mut high = 0;
        for i in 0..2_000 {
            let name = format!("collection-{i}");
            let id = CollectionId::derive("db", &name);
            if id.0 > i64::MAX as u64 {
                high += 1;
            }
            let bytes = bson::serialize_to_vec(&Wrapper { collection: id })
                .unwrap_or_else(|e| panic!("db.{name} (id {}) failed to encode: {e}", id.0));
            let back: Wrapper = bson::deserialize_from_slice(&bytes).expect("decode");
            assert_eq!(back.collection, id, "db.{name} did not round-trip");
        }
        assert!(high > 100, "expected both halves to be exercised; only {high} were high");
    }

    #[test]
    fn the_wire_form_of_a_low_id_is_unchanged() {
        // The fix must widen what works without altering what already worked,
        // or every node would have to upgrade in lockstep.
        let id = CollectionId::derive("shop", "orders");
        assert!(id.0 < i64::MAX as u64, "fixture must be in the low half");

        let doc = bson::serialize_to_document(&Wrapper { collection: id }).expect("encode");
        assert_eq!(
            doc.get_i64("collection").expect("encoded as a signed 64-bit integer"),
            id.0 as i64,
            "a low id must keep the representation it had before the fix"
        );
    }
}
