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
    pub body: Option<Vec<u8>>,
}

impl OplogEntry {
    pub fn resume_token(&self) -> ResumeToken {
        ResumeToken { hlc: self.stamp.hlc, node: self.stamp.node }
    }

    pub fn document(&self) -> Result<Option<bson::Document>> {
        match &self.body {
            Some(bytes) => Ok(Some(bson::deserialize_from_slice(bytes)?)),
            None => Ok(None),
        }
    }
}

/// An opaque cursor into the oplog.
///
/// Clients treat this as a blob. Internally it is just the `(hlc, node)` of the
/// last delivered entry, encoded so that resuming means "scan from the
/// successor of this position".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ResumeToken {
    pub hlc: Hlc,
    pub node: NodeId,
}

const TOKEN_LEN: usize = HLC_ENCODED_LEN + 16;

impl ResumeToken {
    pub fn new(hlc: Hlc, node: NodeId) -> Self {
        Self { hlc, node }
    }

    pub fn from_stamp(stamp: Stamp) -> Self {
        Self { hlc: stamp.hlc, node: stamp.node }
    }

    pub fn to_stamp(self) -> Stamp {
        Stamp::new(self.hlc, self.node)
    }

    /// The exclusive lower bound for a `resume_after` scan.
    ///
    /// Returning the *successor* rather than the token itself is what makes
    /// resumption deliver each event exactly once — resuming at the token would
    /// redeliver the last event the client already saw.
    pub fn exclusive_start(self) -> Hlc {
        self.hlc.successor()
    }

    pub fn encode(self) -> String {
        let mut buf = [0u8; TOKEN_LEN];
        buf[..HLC_ENCODED_LEN].copy_from_slice(&self.hlc.to_bytes());
        buf[HLC_ENCODED_LEN..].copy_from_slice(&self.node.to_bytes());
        URL_SAFE_NO_PAD.encode(buf)
    }

    pub fn decode(s: &str) -> Result<Self> {
        let raw = URL_SAFE_NO_PAD.decode(s).map_err(|_| Error::MalformedResumeToken)?;
        if raw.len() != TOKEN_LEN {
            return Err(Error::MalformedResumeToken);
        }
        let mut hlc_bytes = [0u8; HLC_ENCODED_LEN];
        hlc_bytes.copy_from_slice(&raw[..HLC_ENCODED_LEN]);
        let mut node_bytes = [0u8; 16];
        node_bytes.copy_from_slice(&raw[HLC_ENCODED_LEN..]);
        Ok(Self { hlc: Hlc::from_bytes(hlc_bytes), node: NodeId::from_bytes(node_bytes) })
    }
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

    #[test]
    fn tokens_serde_as_plain_strings() {
        // Clients see an opaque string, not a nested object.
        let json = serde_json::to_string(&token()).unwrap();
        assert!(json.starts_with('"'), "expected a JSON string, got {json}");
        let back: ResumeToken = serde_json::from_str(&json).unwrap();
        assert_eq!(back, token());
    }
}
