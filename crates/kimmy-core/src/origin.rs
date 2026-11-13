//! The identity of a writer incarnation: [`Epoch`] and [`Origin`].
//!
//! An [`Origin`] is a node and one of its epochs. Everything that orders, covers or keys writes
//! by "who wrote them" does so by origin, never by node alone: a restored node takes a fresh
//! epoch, and its new writes must not be mistaken for the writes the restore threw away.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::hlc::Hlc;
use crate::ids::NodeId;

/// The incarnation part of a writer identity: 18 bytes, in production the
/// 10-byte HLC at the writer's start followed by 8 random bytes. A restored
/// node takes a fresh epoch, so its new writes never collide with the writes
/// the restore threw away. Tests build epochs from integers.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Epoch(pub [u8; Epoch::LEN]);

impl Epoch {
    pub const LEN: usize = 18;

    /// The smallest epoch: all zero bytes.
    pub const ZERO: Epoch = Epoch([0; Self::LEN]);

    /// The largest epoch: all `0xFF` bytes.
    pub const MAX: Epoch = Epoch([0xFF; Self::LEN]);

    /// Length of the text form: two lowercase hex digits per byte.
    pub const HEX_LEN: usize = 2 * Self::LEN;

    /// A start HLC followed by 8 random bytes.
    pub fn new(start: Hlc, random: [u8; 8]) -> Self {
        let mut b = [0u8; Self::LEN];
        b[..10].copy_from_slice(&start.to_bytes());
        b[10..].copy_from_slice(&random);
        Self(b)
    }

    /// A fresh epoch: `start`, the HLC of the operation that begins the incarnation, followed
    /// by 64 random bits from the operating system's generator (a version-4 UUID's bytes that
    /// carry no version or variant bits). Never derived from anything read from a store: two
    /// restores of one backup at the same HLC still get different epochs.
    pub fn generate(start: Hlc) -> Self {
        let uuid = *uuid::Uuid::new_v4().as_bytes();
        // bytes 6 (version) and 8 (variant) hold fixed bits; the other bytes are random
        let random = [uuid[0], uuid[1], uuid[2], uuid[3], uuid[4], uuid[5], uuid[7], uuid[9]];
        Self::new(start, random)
    }

    /// A small epoch for tests and models: `n` in the low 8 bytes.
    pub fn from_u64(n: u64) -> Self {
        let mut b = [0u8; Self::LEN];
        b[10..].copy_from_slice(&n.to_be_bytes());
        Self(b)
    }

    /// The start HLC an epoch was minted with: its first ten bytes. No entry of the epoch is
    /// older (a restore mints its epoch at the HLC of the restore's open). An epoch built from
    /// an integer ([`Epoch::from_u64`]) or [`Epoch::ZERO`] starts at [`Hlc::ZERO`].
    ///
    /// Read in one place only: the threshold of `VersionVector::behind`, as an optimisation
    /// (see there). A coverage claim never reads it.
    pub fn start_hlc(self) -> Hlc {
        let mut bytes = [0u8; 10];
        bytes.copy_from_slice(&self.0[..10]);
        Hlc::from_bytes(bytes)
    }

    /// The text form: 36 lowercase hex digits. The one printer; [`Epoch::from_hex`] is the one
    /// parser.
    pub fn to_hex(self) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(Self::HEX_LEN);
        for byte in self.0 {
            out.push(char::from(DIGITS[usize::from(byte >> 4)]));
            out.push(char::from(DIGITS[usize::from(byte & 0x0F)]));
        }
        out
    }

    /// The inverse of [`Epoch::to_hex`]: exactly 36 lowercase hex digits, nothing else.
    pub fn from_hex(text: &str) -> Option<Self> {
        let raw = text.as_bytes();
        if raw.len() != Self::HEX_LEN {
            return None;
        }
        let digit = |c: u8| match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        };
        let mut out = [0u8; Self::LEN];
        for (byte, pair) in out.iter_mut().zip(raw.as_chunks::<2>().0) {
            *byte = (digit(pair[0])? << 4) | digit(pair[1])?;
        }
        Some(Self(out))
    }
}

/// Serialized as its 36-character lowercase hex text where the format is human-readable (JSON,
/// the API, logs), and as its 18 raw bytes otherwise (BSON binary). Read back from either form,
/// whatever the format says about itself: the BSON writers disagree with the BSON readers about
/// being human-readable (see [`NodeId`]'s serde form), so a reader accepts the text and the bytes
/// both. A text that is not 36 lowercase hex digits, and bytes that are not 18, are refused.
impl Serialize for Epoch {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_hex())
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

struct EpochVisitor;

impl serde::de::Visitor<'_> for EpochVisitor {
    type Value = Epoch;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an epoch: 36 lowercase hex digits, or 18 bytes")
    }

    fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Epoch, E> {
        Epoch::from_hex(text).ok_or_else(|| E::custom("an epoch is 36 lowercase hex digits"))
    }

    fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Epoch, E> {
        let bytes: [u8; Epoch::LEN] =
            bytes.try_into().map_err(|_| E::invalid_length(bytes.len(), &"18 bytes"))?;
        Ok(Epoch(bytes))
    }
}

impl<'de> Deserialize<'de> for Epoch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(EpochVisitor)
    }
}

/// One writer incarnation: the node and its epoch. Part of every ordering.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Origin {
    pub node: NodeId,
    pub epoch: Epoch,
}

impl Origin {
    /// Width of the byte form: the node's 16 bytes, then the epoch's 18.
    pub const LEN: usize = 16 + Epoch::LEN;

    pub const fn new(node: NodeId, epoch: Epoch) -> Self {
        Self { node, epoch }
    }

    /// The byte form `node ‖ epoch`, whose `memcmp` order is the order of [`Origin`].
    pub fn to_bytes(self) -> [u8; Self::LEN] {
        let mut out = [0u8; Self::LEN];
        out[..16].copy_from_slice(&self.node.to_bytes());
        out[16..].copy_from_slice(&self.epoch.0);
        out
    }

    /// The inverse of [`Origin::to_bytes`].
    pub fn from_bytes(bytes: [u8; Self::LEN]) -> Self {
        let mut node = [0u8; 16];
        node.copy_from_slice(&bytes[..16]);
        let mut epoch = [0u8; Epoch::LEN];
        epoch.copy_from_slice(&bytes[16..]);
        Self { node: NodeId::from_bytes(node), epoch: Epoch(epoch) }
    }
}

/// A text that is not an origin's wire form.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "not an origin: expected <node uuid, 36 lowercase hex and hyphens>/<epoch, 36 lowercase hex digits>"
)]
pub struct ParseOriginError;

/// The wire form of an origin, the one printer: `<node uuid>/<epoch as 36 lowercase hex>`, 73
/// characters. It is a string so that a map keyed by origin stays a BSON document, and it has
/// one parser ([`FromStr`]); no other code splits it.
impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.node, self.epoch.to_hex())
    }
}

impl FromStr for Origin {
    type Err = ParseOriginError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (node, epoch) = text.split_once('/').ok_or(ParseOriginError)?;
        // `Uuid` parsing also takes uppercase, braced and simple forms; the wire form is one
        // text, so only the printer's own output is accepted.
        let parsed: NodeId = node.parse().map_err(|_| ParseOriginError)?;
        if node.len() != 36 || parsed.to_string() != node {
            return Err(ParseOriginError);
        }
        let epoch = Epoch::from_hex(epoch).ok_or(ParseOriginError)?;
        Ok(Self { node: parsed, epoch })
    }
}

/// Serialized as the wire string where the format is human-readable, and as its 34 raw bytes
/// otherwise; read from either, like [`Epoch`]. **A map keyed by origin must not rely on this**:
/// a BSON key is a string whatever the writer says about itself, so such a map writes each key as
/// the wire string itself (`VersionVector` does).
impl Serialize for Origin {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            serializer.serialize_bytes(&self.to_bytes())
        }
    }
}

struct OriginVisitor;

impl serde::de::Visitor<'_> for OriginVisitor {
    type Value = Origin;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an origin: <node uuid>/<36 lowercase hex digits>, or 34 bytes")
    }

    fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Origin, E> {
        text.parse().map_err(E::custom)
    }

    fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Origin, E> {
        let bytes: [u8; Origin::LEN] =
            bytes.try_into().map_err(|_| E::invalid_length(bytes.len(), &"34 bytes"))?;
        Ok(Origin::from_bytes(bytes))
    }
}

impl<'de> Deserialize<'de> for Origin {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(OriginVisitor)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn origin(node: u8, epoch: u64) -> Origin {
        Origin { node: NodeId::from_bytes([node; 16]), epoch: Epoch::from_u64(epoch) }
    }

    fn any_origin() -> impl Strategy<Value = Origin> {
        (any::<[u8; 16]>(), any::<[u8; 18]>())
            .prop_map(|(node, epoch)| Origin::new(NodeId::from_bytes(node), Epoch(epoch)))
    }

    #[test]
    fn the_wire_form_is_the_node_a_slash_and_36_lowercase_hex_digits() {
        let o = Origin::new(
            NodeId::from_bytes([0xAB; 16]),
            Epoch::new(Hlc::new(0x0102_0304_0506_0708, 0x090A), [0xCD; 8]),
        );
        let text = o.to_string();
        assert_eq!(
            text,
            "abababab-abab-abab-abab-abababababab/0102030405060708090acdcdcdcdcdcdcdcd"
        );
        assert_eq!(text.len(), 73);
        assert_eq!(text.parse::<Origin>().unwrap(), o);
    }

    #[test]
    fn the_wire_form_refuses_every_other_text() {
        let good = origin(0xAB, 0x9A).to_string();
        let (node, epoch) = good.split_once('/').unwrap();
        let refused = [
            String::new(),
            "/".to_string(),
            node.to_string(),                              // no slash
            format!("{node}{epoch}"),                      // missing slash
            format!("{node}/{}", &epoch[1..]),             // epoch one digit short
            format!("{node}/{epoch}0"),                    // epoch one digit long
            format!("{node}/{}", epoch.replace('0', "g")), // non-hex
            format!("{node}/{}", epoch.replace('0', "O")), // non-hex, look-alike
            format!("{node}/{}", "A".repeat(36)),          // uppercase hex
            format!("{}/{epoch}", node.to_uppercase()),    // uppercase node
            format!("{}/{epoch}", node.replace('-', "")),  // the simple uuid form
            format!("{{{node}}}/{epoch}"),                 // the braced uuid form
            format!("{node}/{epoch}/{epoch}"),             // a third part
            format!("{good} "),                            // trailing space
            format!(" {good}"),
            format!("{node}/{}+", &epoch[1..]),
            format!("{node}/{}é", &epoch[2..]), // 36 bytes, but not 36 hex digits
        ];
        for text in &refused {
            assert!(text.parse::<Origin>().is_err(), "accepted {text:?}");
        }
        assert_ne!(node.to_uppercase(), node);
        assert_ne!(epoch.to_uppercase(), epoch);
        assert!(format!("{node}/{}", epoch.to_uppercase()).parse::<Origin>().is_err());
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Holder {
        epoch: Epoch,
        origin: Origin,
    }

    #[test]
    fn json_writes_the_text_forms_and_reads_them_back() {
        let o = origin(3, 0xDEAD_BEEF);
        let json = serde_json::to_string(&o).unwrap();
        assert_eq!(json, format!("\"{o}\""));
        assert_eq!(serde_json::from_str::<Origin>(&json).unwrap(), o);
        let e = Epoch::from_u64(77);
        let json = serde_json::to_string(&e).unwrap();
        assert_eq!(json, format!("\"{}\"", e.to_hex()));
        assert_eq!(serde_json::from_str::<Epoch>(&json).unwrap(), e);
        // a wrong length or a non-hex text is refused
        assert!(serde_json::from_str::<Epoch>("\"00\"").is_err());
        assert!(serde_json::from_str::<Epoch>(&format!("\"{}\"", "G".repeat(36))).is_err());
        assert!(serde_json::from_str::<Origin>("\"nope\"").is_err());
        assert!(serde_json::from_str::<Epoch>("[1,2,3]").is_err());
    }

    /// `bson::serialize_to_vec` reports `is_human_readable() == false` (bson 3.1.0
    /// src/ser/raw.rs:72, `human_readable: false`), so an epoch and an origin are BSON binary
    /// there; `serialize_to_document` reports true (src/ser/serde.rs:426, `unwrap_or(true)`),
    /// so they are strings. This pins both, and that each reads back from either.
    #[test]
    fn bson_writes_binary_through_the_raw_writer_and_text_through_the_document_writer() {
        let h = Holder { epoch: Epoch::new(Hlc::new(5, 6), [9; 8]), origin: origin(0xAB, 11) };

        let bytes = bson::serialize_to_vec(&h).unwrap();
        let doc: bson::Document = bson::deserialize_from_slice(&bytes).unwrap();
        match (doc.get("epoch"), doc.get("origin")) {
            (Some(bson::Bson::Binary(e)), Some(bson::Bson::Binary(o))) => {
                assert_eq!(e.bytes, h.epoch.0.to_vec());
                assert_eq!(o.bytes, h.origin.to_bytes().to_vec());
            }
            other => panic!("expected two binaries, got {other:?}"),
        }
        assert_eq!(bson::deserialize_from_slice::<Holder>(&bytes).unwrap(), h);

        let doc = bson::serialize_to_document(&h).unwrap();
        assert_eq!(doc.get_str("epoch").unwrap(), h.epoch.to_hex());
        assert_eq!(doc.get_str("origin").unwrap(), h.origin.to_string());
        assert_eq!(bson::deserialize_from_document::<Holder>(doc).unwrap(), h);

        // each form is read whichever way it was written
        let from_text = bson::doc! { "epoch": h.epoch.to_hex(), "origin": h.origin.to_string() };
        assert_eq!(
            bson::deserialize_from_slice::<Holder>(&bson::serialize_to_vec(&from_text).unwrap())
                .unwrap(),
            h
        );
        let binary = |b: &[u8]| {
            bson::Bson::Binary(bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: b.to_vec(),
            })
        };
        let from_binary =
            bson::doc! { "epoch": binary(&h.epoch.0), "origin": binary(&h.origin.to_bytes()) };
        assert_eq!(bson::deserialize_from_document::<Holder>(from_binary).unwrap(), h);

        // bytes of the wrong length are refused, for each type
        let short_epoch =
            bson::doc! { "epoch": binary(&[0; 17]), "origin": binary(&h.origin.to_bytes()) };
        assert!(bson::deserialize_from_document::<Holder>(short_epoch).is_err());
        let long_epoch =
            bson::doc! { "epoch": binary(&[0; 19]), "origin": binary(&h.origin.to_bytes()) };
        assert!(bson::deserialize_from_document::<Holder>(long_epoch).is_err());
        let short_origin = bson::doc! { "epoch": binary(&h.epoch.0), "origin": binary(&[0; 33]) };
        assert!(bson::deserialize_from_document::<Holder>(short_origin).is_err());
        let long_origin = bson::doc! { "epoch": binary(&h.epoch.0), "origin": binary(&[0; 35]) };
        assert!(bson::deserialize_from_document::<Holder>(long_origin).is_err());
        // the raw reader refuses too
        let raw = bson::serialize_to_vec(
            &bson::doc! { "epoch": binary(&[0; 17]), "origin": binary(&h.origin.to_bytes()) },
        )
        .unwrap();
        assert!(bson::deserialize_from_slice::<Holder>(&raw).is_err());
    }

    #[test]
    fn a_generated_epoch_is_its_start_hlc_and_64_random_bits() {
        let start = Hlc::new(1_700_000_000_123, 7);
        let one = Epoch::generate(start);
        let other = Epoch::generate(start);
        assert_eq!(one.start_hlc(), start);
        assert_eq!(&one.0[..10], &start.to_bytes());
        assert_ne!(one, other, "two mints at one HLC differ in their random half");
        // the random half is not a constant and not the version bits of a UUID alone
        let halves: std::collections::BTreeSet<[u8; 8]> =
            (0..32).map(|_| Epoch::generate(start).0[10..].try_into().unwrap()).collect();
        assert_eq!(halves.len(), 32);
        // a later start sorts later whatever the random half: the epoch order is start-first
        assert!(Epoch::generate(Hlc::new(start.wall_ms + 1, 0)) > Epoch::generate(start));
        // a test epoch starts at zero
        assert_eq!(Epoch::from_u64(9).start_hlc(), Hlc::ZERO);
        assert_eq!(Epoch::ZERO.start_hlc(), Hlc::ZERO);
    }

    #[test]
    fn the_extremes_are_the_all_zero_and_all_one_epochs() {
        assert_eq!(Epoch::ZERO, Epoch::default());
        assert!(Epoch::ZERO < Epoch::from_u64(1));
        assert!(Epoch::from_u64(u64::MAX) < Epoch::MAX);
        assert_eq!(Epoch::MAX.to_hex(), "f".repeat(36));
    }

    proptest! {
        #[test]
        fn an_origin_round_trips_through_its_bytes_and_its_text(o in any_origin()) {
            prop_assert_eq!(Origin::from_bytes(o.to_bytes()), o);
            prop_assert_eq!(o.to_string().parse::<Origin>().unwrap(), o);
        }

        /// The order of origins is the byte order of their 34-byte form: the storage layer
        /// keys tables by those bytes and the coverage tests compare origins by `Ord`.
        #[test]
        fn origin_order_is_the_byte_order_of_its_form(a in any_origin(), b in any_origin()) {
            prop_assert_eq!(a.cmp(&b), a.to_bytes().cmp(&b.to_bytes()));
        }

        #[test]
        fn origin_order_is_node_then_epoch(a in any_origin(), b in any_origin()) {
            prop_assert_eq!(a.cmp(&b), (a.node, a.epoch).cmp(&(b.node, b.epoch)));
        }
    }
}
