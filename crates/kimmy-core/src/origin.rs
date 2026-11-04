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

    /// A small epoch for tests and models: `n` in the low 8 bytes.
    pub fn from_u64(n: u64) -> Self {
        let mut b = [0u8; Self::LEN];
        b[10..].copy_from_slice(&n.to_be_bytes());
        Self(b)
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

/// Serialized as its 36-character lowercase hex text, in every format, so an epoch reads back
/// out of JSON and BSON alike (the same reasoning as [`NodeId`]'s serde form).
impl Serialize for Epoch {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Epoch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::from_hex(&text)
            .ok_or_else(|| serde::de::Error::custom("an epoch is 36 lowercase hex digits"))
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

impl Serialize for Origin {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Origin {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
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

    #[test]
    fn an_epoch_is_hex_text_in_json_and_bson_and_an_origin_is_its_wire_string() {
        let o = origin(3, 0xDEAD_BEEF);
        let json = serde_json::to_string(&o).unwrap();
        assert_eq!(json, format!("\"{o}\""));
        assert_eq!(serde_json::from_str::<Origin>(&json).unwrap(), o);
        let e = Epoch::from_u64(77);
        let json = serde_json::to_string(&e).unwrap();
        assert_eq!(json, format!("\"{}\"", e.to_hex()));
        assert_eq!(serde_json::from_str::<Epoch>(&json).unwrap(), e);
        assert!(serde_json::from_str::<Epoch>("\"00\"").is_err());
        assert!(serde_json::from_str::<Origin>("\"nope\"").is_err());

        // BSON writes and reads it back too, and an origin works as a document key
        let doc = bson::doc! { "o": o.to_string(), "e": e.to_hex() };
        let back: std::collections::BTreeMap<String, String> =
            bson::deserialize_from_document(doc).unwrap();
        assert_eq!(back["o"].parse::<Origin>().unwrap(), o);
        assert_eq!(Epoch::from_hex(&back["e"]).unwrap(), e);
        let mut map = std::collections::BTreeMap::new();
        map.insert(o, 5u32);
        let doc = bson::serialize_to_document(&map).unwrap();
        assert_eq!(doc.get_i64(o.to_string()).unwrap(), 5);
        let back: std::collections::BTreeMap<Origin, u32> =
            bson::deserialize_from_document(doc).unwrap();
        assert_eq!(back, map);
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
