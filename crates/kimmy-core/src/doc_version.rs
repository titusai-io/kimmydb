//! The version identity of one stored document, as a client sees it.
//!
//! Under the `document` policy a document's version is its stamp: the latest write wins as a
//! whole, so the stamp names the state. Under `field` the stamp is the highest entry of the
//! document's version vector, and two different merged states can share it (a concurrent write
//! that sorts lower is joined without moving it), so the identity adds the hash of the merged
//! state: an **etag**. A token is opaque to the client: it compares tokens for equality and hands
//! one back as `if_stamp`.
//!
//! * stamp token: `0x01 ‖ stamp key form` (45 bytes), what [`Stamp::encode`] writes;
//! * etag token: `0x02 ‖ stamp key form ‖ state hash` (61 bytes), base64url without padding.

use base64::Engine as _;

use crate::error::{Error, Result};
use crate::hlc::Stamp;

const ETAG_FORMAT: u8 = 0x02;

/// The hash of a merged state (`MergeState::compact_hash`): 16 bytes.
pub type StateHash = [u8; 16];

/// A document's version identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocVersion {
    /// A `document` collection's document: its stamp.
    Stamp(Stamp),
    /// A `field` collection's document: `max(vv)` and the hash of the merged state.
    Etag(Stamp, StateHash),
}

impl DocVersion {
    /// Length of an etag token's raw bytes.
    pub const ETAG_LEN: usize = 1 + Stamp::KEY_LEN + 16;

    /// The stamp in the identity: the write that is the newest in it.
    pub fn stamp(&self) -> Stamp {
        match self {
            Self::Stamp(s) | Self::Etag(s, _) => *s,
        }
    }

    /// The state hash, for an etag.
    pub fn state_hash(&self) -> Option<StateHash> {
        match self {
            Self::Stamp(_) => None,
            Self::Etag(_, h) => Some(*h),
        }
    }

    /// The opaque token.
    pub fn encode(&self) -> String {
        match self {
            Self::Stamp(s) => s.encode(),
            Self::Etag(s, hash) => {
                let mut raw = Vec::with_capacity(Self::ETAG_LEN);
                raw.push(ETAG_FORMAT);
                raw.extend_from_slice(&s.to_key_bytes());
                raw.extend_from_slice(hash);
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
            }
        }
    }

    /// The inverse of [`DocVersion::encode`]: a stamp token or an etag token, nothing else.
    pub fn decode(token: &str) -> Result<Self> {
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| Error::MalformedStamp)?;
        match raw.first() {
            Some(&ETAG_FORMAT) if raw.len() == Self::ETAG_LEN => {
                let key: [u8; Stamp::KEY_LEN] =
                    raw[1..1 + Stamp::KEY_LEN].try_into().map_err(|_| Error::MalformedStamp)?;
                let hash: StateHash =
                    raw[1 + Stamp::KEY_LEN..].try_into().map_err(|_| Error::MalformedStamp)?;
                Ok(Self::Etag(Stamp::from_key_bytes(key), hash))
            }
            Some(&ETAG_FORMAT) => Err(Error::MalformedStamp),
            _ => Stamp::decode(token).map(Self::Stamp),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Hlc;
    use crate::ids::NodeId;
    use crate::origin::Epoch;

    fn stamp(ms: u64) -> Stamp {
        Stamp::new(Hlc::new(ms, 1), NodeId::from_bytes([7; 16]), Epoch::ZERO)
    }

    #[test]
    fn a_stamp_token_is_what_stamp_encode_writes_and_an_etag_is_61_bytes() {
        let s = stamp(100);
        assert_eq!(DocVersion::Stamp(s).encode(), s.encode());
        let e = DocVersion::Etag(s, [9; 16]);
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(e.encode()).unwrap();
        assert_eq!((raw.len(), raw[0]), (61, 0x02));
        assert_eq!(&raw[1..45], &s.to_key_bytes());
        assert_eq!(&raw[45..], &[9; 16]);
    }

    #[test]
    fn both_tokens_round_trip_and_anything_else_is_malformed() {
        for v in [DocVersion::Stamp(stamp(5)), DocVersion::Etag(stamp(6), [1; 16])] {
            assert_eq!(DocVersion::decode(&v.encode()).unwrap(), v);
            assert_eq!(v.stamp().hlc.wall_ms >= 5, true);
        }
        let e = DocVersion::Etag(stamp(6), [1; 16]).encode();
        for bad in ["", "!!", &e[..e.len() - 4], &format!("{e}AAAA")] {
            assert!(DocVersion::decode(bad).is_err(), "{bad:?}");
        }
        // an etag differs from another state at the same stamp
        assert_ne!(
            DocVersion::Etag(stamp(6), [1; 16]).encode(),
            DocVersion::Etag(stamp(6), [2; 16]).encode()
        );
        assert_eq!(DocVersion::Etag(stamp(6), [1; 16]).state_hash(), Some([1; 16]));
        assert_eq!(DocVersion::Stamp(stamp(6)).state_hash(), None);
    }
}
