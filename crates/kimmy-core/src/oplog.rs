//! Oplog entries and change-stream resume tokens.
//!
//! The oplog is the spine of KimmyDB. Every mutation appends exactly one entry,
//! and three independent subsystems consume the same log:
//!
//! 1. change streams (WebSocket subscribers, resumable by token),
//! 2. the auto-embedding pipeline (an internal subscriber), and
//! 3. cluster anti-entropy (peers pull ranges they are missing).
//!
//! Building the log once and reusing it three times is why single-instance
//! change streams work here at all — the log exists whether or not the node is
//! part of a cluster, rather than being a byproduct of replication.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::hlc::{HLC_ENCODED_LEN, Hlc, Stamp};
use crate::ids::{CollectionId, DocId, NodeId};
use crate::version::VersionVector;

/// What kind of mutation an oplog entry describes.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OpKind {
    Insert,
    /// A partial modification via update operators.
    Update,
    /// A whole-document overwrite.
    Replace,
    Delete,
    /// **Legacy.** A collection was created, dropped, or reconfigured, with no
    /// payload saying which or naming it.
    ///
    /// Still decoded so that oplogs written before schema changes replicated
    /// keep loading, but never written again — and it cannot be applied by a
    /// peer, because it identifies nothing. Superseded by the operations below.
    Collection,
    /// Merging a replicated write broke a unique constraint.
    ///
    /// Not a mutation — it describes something that *happened to* the data
    /// rather than a change to it. It lives in the oplog anyway because that is
    /// the only path to a change stream, and because being durable and
    /// resumable is the whole point: a violation nobody was connected to
    /// witness is barely better than a silent one.
    ///
    /// Carries no `doc_id`; the colliding ids are in the body. **Must not be
    /// replicated** — every node detects its own violations at its own merge
    /// time, so shipping these to peers would double-report. See
    /// [`crate::UniqueViolationDetail`].
    UniqueViolation,
    /// A collection was created. Body: [`crate::CollectionRef`].
    CreateCollection,
    /// A collection was dropped. Body: [`crate::CollectionRef`].
    DropCollection,
    /// An index was created. Body: [`crate::IndexCreate`].
    CreateIndex,
    /// An index was dropped. Body: [`crate::IndexDrop`].
    DropIndex,
    /// Auto-embedding was configured or turned off. Body: [`crate::VectorSet`].
    ConfigureVectors,
}

impl OpKind {
    /// Whether this entry describes a schema change rather than a document.
    pub fn is_ddl(self) -> bool {
        matches!(
            self,
            Self::CreateCollection
                | Self::DropCollection
                | Self::CreateIndex
                | Self::DropIndex
                | Self::ConfigureVectors
        )
    }

    /// Whether this entry carries a document change to merge.
    pub fn is_document(self) -> bool {
        matches!(self, Self::Insert | Self::Update | Self::Replace | Self::Delete)
    }
}

/// One durable, totally-ordered record of a mutation.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct OplogEntry {
    pub stamp: Stamp,
    pub kind: OpKind,
    pub collection: CollectionId,
    /// Absent only for [`OpKind::Collection`] entries.
    pub doc_id: Option<DocId>,
    /// Full post-image, BSON-encoded. Absent for deletes.
    ///
    /// We store the whole document rather than a diff: it makes replication
    /// application idempotent and order-independent (just compare stamps and
    /// overwrite), and it lets change-stream subscribers get `fullDocument`
    /// without a second read.
    ///
    /// **`serde_bytes` is load-bearing, not tidiness.** Serde encodes a bare
    /// `Vec<u8>` as an array of int32s — one BSON element, with its own index
    /// key, per byte — which made a replicated document about twelve times its
    /// own size. A 1 MiB document became a 12.5 MiB entry, so a batch reached
    /// the 64 MiB frame limit at roughly 5 MiB of real data. As binary it is
    /// the document's own length plus a few bytes.
    #[serde(with = "serde_bytes")]
    pub body: Option<Vec<u8>>,
}

impl OplogEntry {
    pub fn resume_token(&self) -> ResumeToken {
        ResumeToken::from_stamp(self.stamp)
    }

    pub fn document(&self) -> Result<Option<bson::Document>> {
        match &self.body {
            Some(bytes) => Ok(Some(bson::deserialize_from_slice(bytes)?)),
            None => Ok(None),
        }
    }
}

/// An opaque cursor into a change stream.
///
/// Clients treat this as a blob. It names the last delivered entry by stamp,
/// and, on a token a 0.30.0 or later member issued, also carries
/// [`Issued`]: which member delivered it, and how far that member's stream had
/// delivered each origin (ADR-173).
///
/// Both are needed because a stream follows a member's *arrival* order, and
/// every member orders the same writes differently. On the member that issued
/// it, the stamp names a position in that order, which is exact. Anywhere else
/// that position means nothing, so a member resumes from the vector instead.
///
/// A token without `issued` is the single stamp every member issued before
/// 0.30.0, still accepted for one minor release.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ResumeToken {
    pub hlc: Hlc,
    pub node: NodeId,
    pub issued: Option<Issued>,
}

/// Who issued a resume token, and what their stream had delivered.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Issued {
    /// The member whose stream delivered the token's entry.
    pub by: NodeId,
    /// Per origin, a stamp at or below which the issuing member's stream had
    /// taken every entry that member holds — delivered it, or passed over it
    /// as out of scope — when the token was issued (ADR-173).
    pub delivered: VersionVector,
}

/// A single-stamp token: `hlc ‖ node`, as issued before 0.30.0.
const SINGLE_STAMP_LEN: usize = HLC_ENCODED_LEN + 16;

/// The first byte of an issued token. A single-stamp token has no version
/// byte, and is told apart by its length, which no issued token can have.
const ISSUED_VERSION: u8 = 2;

/// `version ‖ hlc ‖ node ‖ issued-by ‖ origin count (u16, big-endian)`.
const ISSUED_HEADER_LEN: usize = 1 + HLC_ENCODED_LEN + 16 + 16 + 2;

/// One origin of the delivered vector: `node ‖ hlc`.
const ORIGIN_LEN: usize = 16 + HLC_ENCODED_LEN;

/// The most origins a token may name. A cluster's vector has one per member
/// that ever wrote; the bound keeps a token a string a query parameter can
/// carry and refuses a forged one that asks for an unbounded decode.
pub const MAX_TOKEN_ORIGINS: usize = 1024;

impl ResumeToken {
    /// A single-stamp token, with no issuing member.
    pub fn new(hlc: Hlc, node: NodeId) -> Self {
        Self { hlc, node, issued: None }
    }

    /// A single-stamp token naming `stamp`, with no issuing member.
    pub fn from_stamp(stamp: Stamp) -> Self {
        Self::new(stamp.hlc, stamp.node)
    }

    /// The token a member's stream issues for the entry stamped `stamp`.
    pub fn issued(stamp: Stamp, by: NodeId, delivered: VersionVector) -> Self {
        Self { hlc: stamp.hlc, node: stamp.node, issued: Some(Issued { by, delivered }) }
    }

    pub fn to_stamp(&self) -> Stamp {
        Stamp::new(self.hlc, self.node)
    }

    /// The exclusive lower bound for a `resume_after` scan.
    ///
    /// Returning the *successor* rather than the token itself is what makes
    /// resumption deliver each event exactly once — resuming at the token would
    /// redeliver the last event the client already saw.
    pub fn exclusive_start(&self) -> Hlc {
        self.hlc.successor()
    }

    pub fn encode(&self) -> String {
        let Some(issued) = &self.issued else {
            let mut buf = [0u8; SINGLE_STAMP_LEN];
            buf[..HLC_ENCODED_LEN].copy_from_slice(&self.hlc.to_bytes());
            buf[HLC_ENCODED_LEN..].copy_from_slice(&self.node.to_bytes());
            return URL_SAFE_NO_PAD.encode(buf);
        };
        let origins = issued.delivered.len().min(MAX_TOKEN_ORIGINS);
        let mut buf = Vec::with_capacity(ISSUED_HEADER_LEN + origins * ORIGIN_LEN);
        buf.push(ISSUED_VERSION);
        buf.extend_from_slice(&self.hlc.to_bytes());
        buf.extend_from_slice(&self.node.to_bytes());
        buf.extend_from_slice(&issued.by.to_bytes());
        // `origins` fits: MAX_TOKEN_ORIGINS is below u16::MAX.
        buf.extend_from_slice(&(origins as u16).to_be_bytes());
        // A vector iterates in node order, which is what `decode` requires.
        for (node, hlc) in issued.delivered.iter().take(origins) {
            buf.extend_from_slice(&node.to_bytes());
            buf.extend_from_slice(&hlc.to_bytes());
        }
        URL_SAFE_NO_PAD.encode(buf)
    }

    pub fn decode(s: &str) -> Result<Self> {
        let raw = URL_SAFE_NO_PAD.decode(s).map_err(|_| Error::MalformedResumeToken)?;
        if raw.len() == SINGLE_STAMP_LEN {
            return Ok(Self::new(hlc_at(&raw, 0), node_at(&raw, HLC_ENCODED_LEN)));
        }
        if raw.len() < ISSUED_HEADER_LEN || raw[0] != ISSUED_VERSION {
            return Err(Error::MalformedResumeToken);
        }
        let hlc = hlc_at(&raw, 1);
        let node = node_at(&raw, 1 + HLC_ENCODED_LEN);
        let by = node_at(&raw, 1 + HLC_ENCODED_LEN + 16);
        let count_at = ISSUED_HEADER_LEN - 2;
        let origins = u16::from_be_bytes([raw[count_at], raw[count_at + 1]]) as usize;
        if origins > MAX_TOKEN_ORIGINS || raw.len() != ISSUED_HEADER_LEN + origins * ORIGIN_LEN {
            return Err(Error::MalformedResumeToken);
        }
        let mut delivered = VersionVector::new();
        let mut previous: Option<NodeId> = None;
        for i in 0..origins {
            let at = ISSUED_HEADER_LEN + i * ORIGIN_LEN;
            let origin = node_at(&raw, at);
            // Strictly ascending, as `encode` writes them: a repeated origin
            // would be a token that says two things about one member.
            if previous.is_some_and(|p| p >= origin) {
                return Err(Error::MalformedResumeToken);
            }
            previous = Some(origin);
            delivered.insert(origin, hlc_at(&raw, at + 16));
        }
        Ok(Self { hlc, node, issued: Some(Issued { by, delivered }) })
    }
}

fn hlc_at(raw: &[u8], at: usize) -> Hlc {
    let mut bytes = [0u8; HLC_ENCODED_LEN];
    bytes.copy_from_slice(&raw[at..at + HLC_ENCODED_LEN]);
    Hlc::from_bytes(bytes)
}

fn node_at(raw: &[u8], at: usize) -> NodeId {
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&raw[at..at + 16]);
    NodeId::from_bytes(bytes)
}

impl std::fmt::Display for ResumeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.encode())
    }
}

impl From<ResumeToken> for String {
    fn from(t: ResumeToken) -> Self {
        t.encode()
    }
}

impl TryFrom<String> for ResumeToken {
    type Error = Error;

    fn try_from(s: String) -> Result<Self> {
        ResumeToken::decode(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token() -> ResumeToken {
        ResumeToken::new(Hlc::new(1_700_000_000_000, 42), NodeId::from_bytes([9; 16]))
    }

    #[test]
    fn token_round_trips() {
        let t = token();
        assert_eq!(ResumeToken::decode(&t.encode()).unwrap(), t);
    }

    #[test]
    fn token_is_url_safe() {
        let encoded = token().encode();
        assert!(
            encoded.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "token {encoded} must survive a query string unescaped"
        );
    }

    #[test]
    fn malformed_tokens_are_rejected() {
        assert!(ResumeToken::decode("not base64!!").is_err());
        // Right alphabet, wrong length.
        assert!(ResumeToken::decode(&URL_SAFE_NO_PAD.encode([0u8; 8])).is_err());
    }

    #[test]
    fn resume_is_exclusive_of_the_token_itself() {
        let t = token();
        assert!(t.exclusive_start() > t.hlc, "resuming must not redeliver the last event");
    }

    fn issued_token() -> ResumeToken {
        let mut delivered = VersionVector::new();
        delivered.insert(NodeId::from_bytes([3; 16]), Hlc::new(1_700_000_000_500, 1));
        delivered.insert(NodeId::from_bytes([9; 16]), Hlc::new(1_700_000_000_000, 42));
        ResumeToken::issued(
            Stamp::new(Hlc::new(1_700_000_000_000, 42), NodeId::from_bytes([9; 16])),
            NodeId::from_bytes([5; 16]),
            delivered,
        )
    }

    #[test]
    fn an_issued_token_round_trips_with_its_issuer_and_vector() {
        let t = issued_token();
        let back = ResumeToken::decode(&t.encode()).unwrap();
        assert_eq!(back, t);
        assert_eq!(back.issued.unwrap().delivered.len(), 2);
    }

    /// What every member issued before 0.30.0: `hlc ‖ node`, 26 bytes, no
    /// version byte. Built here from the layout rather than through `encode`,
    /// so a change to how a single-stamp token is written cannot also change
    /// what this test expects of one.
    #[test]
    fn a_single_stamp_token_issued_before_0_30_still_decodes() {
        let hlc = Hlc::new(1_700_000_000_000, 42);
        let node = NodeId::from_bytes([9; 16]);
        let mut raw = Vec::new();
        raw.extend_from_slice(&hlc.to_bytes());
        raw.extend_from_slice(&node.to_bytes());
        let old = URL_SAFE_NO_PAD.encode(&raw);

        let token = ResumeToken::decode(&old).unwrap();
        assert_eq!(token.to_stamp(), Stamp::new(hlc, node));
        assert_eq!(token.issued, None, "a single-stamp token names no issuing member");
        assert_eq!(token.encode(), old, "and is written back unchanged");
    }

    #[test]
    fn malformed_issued_tokens_are_rejected() {
        let raw = URL_SAFE_NO_PAD.decode(issued_token().encode()).unwrap();

        let mut wrong_version = raw.clone();
        wrong_version[0] = 3;
        assert!(ResumeToken::decode(&URL_SAFE_NO_PAD.encode(wrong_version)).is_err());

        let mut truncated = raw.clone();
        truncated.pop();
        assert!(ResumeToken::decode(&URL_SAFE_NO_PAD.encode(truncated)).is_err());

        // The two origins swapped: a vector names each member once, in order.
        let mut unsorted = raw.clone();
        let first = ISSUED_HEADER_LEN..ISSUED_HEADER_LEN + ORIGIN_LEN;
        let second = ISSUED_HEADER_LEN + ORIGIN_LEN..ISSUED_HEADER_LEN + 2 * ORIGIN_LEN;
        let (a, b) = (raw[first.clone()].to_vec(), raw[second.clone()].to_vec());
        unsorted[first].copy_from_slice(&b);
        unsorted[second].copy_from_slice(&a);
        assert!(ResumeToken::decode(&URL_SAFE_NO_PAD.encode(unsorted)).is_err());

        // A count past the bound, with the bytes to match it.
        let mut oversized = raw[..ISSUED_HEADER_LEN].to_vec();
        let count = (MAX_TOKEN_ORIGINS + 1) as u16;
        oversized[ISSUED_HEADER_LEN - 2..].copy_from_slice(&count.to_be_bytes());
        oversized.resize(ISSUED_HEADER_LEN + (MAX_TOKEN_ORIGINS + 1) * ORIGIN_LEN, 0);
        assert!(ResumeToken::decode(&URL_SAFE_NO_PAD.encode(oversized)).is_err());
    }

    #[test]
    fn tokens_serde_as_plain_strings() {
        // Clients see an opaque string, not a nested object.
        let json = serde_json::to_string(&token()).unwrap();
        assert!(json.starts_with('"'), "expected a JSON string, got {json}");
        let back: ResumeToken = serde_json::from_str(&json).unwrap();
        assert_eq!(back, token());
    }
}
