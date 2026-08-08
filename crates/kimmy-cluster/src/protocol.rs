//! The node-to-node wire protocol.
//!
//! Length-prefixed BSON frames over TCP. BSON because the payload is oplog
//! entries, which are already BSON-shaped, and because it round-trips the exact
//! types storage uses — a JSON hop would have to re-derive `Hlc`, `DocId` and
//! binary bodies from a representation that cannot hold them.
//!
//! # Authentication
//!
//! Both sides prove they hold `cluster_secret` before anything else is
//! exchanged, and neither sends it:
//!
//! ```text
//!   Hello   { node, nonce_a }              ───▶
//!           ◀───  Welcome { node, nonce_b, HMAC(secret, nonce_a) }
//!   Confirm { HMAC(secret, nonce_b) }      ───▶
//! ```
//!
//! Three messages, not two, because a challenge has to be *received* before it
//! can be answered — an initiator cannot prove a nonce the responder has not
//! chosen yet. Each side signs a value the other picked, so a proof captured
//! from one handshake is useless in the next.
//!
//! **Mutual**, because a one-sided check would let anything that can open a
//! socket read the entire oplog by simply never asking for proof in return.
//!
//! This is authentication, not confidentiality: frames are plaintext, so anyone
//! on the path can read replicated documents. TLS is M5. The secret's job today
//! is to stop an unrelated process — a misconfigured node pointed at the wrong
//! cluster, most likely — from joining and merging its data in.

use std::io;

use kimmy_core::{Hlc, NodeId, OplogEntry, VersionVector};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Frames larger than this are refused rather than allocated.
///
/// A length prefix read from the network is attacker-controlled, so trusting it
/// enough to allocate is how a single malformed frame becomes an out-of-memory
/// kill. 64 MiB is far above a full oplog batch and far below anything that
/// threatens a node.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// Entries a peer will send in one response.
///
/// Bounded so a node that is far behind catches up over several rounds rather
/// than in one frame it may not have the memory to hold.
pub const MAX_BATCH: usize = 1024;

/// What one side says to the other.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// Opens a connection: who I am, and a challenge for you to answer.
    Hello { node: NodeId, nonce: Vec<u8> },
    /// Answers the challenge, and issues one of my own.
    Welcome { node: NodeId, nonce: Vec<u8>, proof: Vec<u8> },
    /// Answers the responder's challenge. The handshake is complete after this.
    Confirm { proof: Vec<u8> },
    /// "What do you hold?"
    ///
    /// A struct variant with no fields rather than a unit variant: BSON has no
    /// representation for a bare value at the top level, and a unit variant
    /// serializes to a string. Every message on this wire must be a document.
    AskVersions {},
    /// The answer.
    Versions(VersionVector),
    /// "Send me everything at or after this point."
    AskEntries { from: Hlc, limit: usize },
    /// The answer, in stamp order.
    Entries(Vec<OplogEntry>),
    /// Something went wrong; the sender is closing.
    Fault(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("malformed frame: {0}")]
    Malformed(String),
    #[error("frame of {size} bytes exceeds the {MAX_FRAME} byte limit")]
    TooLarge { size: usize },
    #[error("peer failed authentication")]
    Unauthenticated,
    #[error("peer reported: {0}")]
    Fault(String),
    #[error("peer closed the connection")]
    Closed,
}

/// Write one length-prefixed frame.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &Message,
) -> Result<(), ProtocolError> {
    let body = bson::serialize_to_vec(message)
        .map_err(|e| ProtocolError::Malformed(format!("encoding: {e}")))?;
    if body.len() > MAX_FRAME {
        return Err(ProtocolError::TooLarge { size: body.len() });
    }

    writer.write_all(&(body.len() as u32).to_be_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

/// Read one length-prefixed frame.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Message, ProtocolError> {
    let mut len = [0u8; 4];
    match reader.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Err(ProtocolError::Closed),
        Err(e) => return Err(e.into()),
    }

    // Checked *before* allocating: the length comes from the network.
    let size = u32::from_be_bytes(len) as usize;
    if size > MAX_FRAME {
        return Err(ProtocolError::TooLarge { size });
    }

    let mut body = vec![0u8; size];
    reader.read_exact(&mut body).await?;
    bson::deserialize_from_slice(&body)
        .map_err(|e| ProtocolError::Malformed(format!("decoding: {e}")))
}

/// `HMAC-SHA256(secret, nonce)`.
pub fn prove(secret: &str, nonce: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(nonce);
    mac.finalize().into_bytes().to_vec()
}

/// Check a proof in constant time.
///
/// A byte-by-byte comparison leaks how much of a forged proof was correct,
/// which is enough to recover the rest one byte at a time.
pub fn proof_is_valid(secret: &str, nonce: &[u8], proof: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    let expected = prove(secret, nonce);
    expected.ct_eq(proof).into()
}

/// A fresh nonce.
///
/// Derived from the node id and the clock rather than a CSPRNG: its only job is
/// to be unlikely to repeat, so that a proof captured from one handshake cannot
/// be replayed into the next. It is not a secret and guessing it gains nothing
/// without the key.
pub fn nonce(node: NodeId) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&node.to_bytes());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    out.extend_from_slice(&now.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A vector with entries in it.
    ///
    /// An *empty* one round trips under any encoding, so testing with one
    /// proves nothing about whether node ids survive the wire — which they
    /// did not.
    fn populated_vector() -> VersionVector {
        let mut v = VersionVector::new();
        v.observe(kimmy_core::Stamp::new(Hlc::new(42, 3), NodeId::generate()));
        v
    }

    #[tokio::test]
    async fn frames_round_trip() {
        let messages = [
            Message::AskVersions {},
            Message::AskEntries { from: Hlc::new(7, 1), limit: 10 },
            Message::Versions(populated_vector()),
            Message::Entries(Vec::new()),
            Message::Hello { node: NodeId::generate(), nonce: vec![1, 2, 3] },
            Message::Confirm { proof: vec![9, 9] },
            Message::Fault("nope".into()),
        ];

        for message in messages {
            let mut buffer = Vec::new();
            write_frame(&mut buffer, &message).await.unwrap();
            let back = read_frame(&mut buffer.as_slice()).await.unwrap();
            assert_eq!(back, message);
        }
    }

    #[tokio::test]
    async fn several_frames_share_one_stream() {
        // The length prefix is what separates them; without it the second read
        // would consume the tail of the first message.
        let mut buffer = Vec::new();
        write_frame(&mut buffer, &Message::AskVersions {}).await.unwrap();
        write_frame(&mut buffer, &Message::AskEntries { from: Hlc::ZERO, limit: 5 }).await.unwrap();

        let mut stream = buffer.as_slice();
        assert_eq!(read_frame(&mut stream).await.unwrap(), Message::AskVersions {});
        assert_eq!(
            read_frame(&mut stream).await.unwrap(),
            Message::AskEntries { from: Hlc::ZERO, limit: 5 }
        );
    }

    #[tokio::test]
    async fn a_closed_stream_is_reported_as_closed_not_as_corruption() {
        // A peer going away is ordinary; it must not look like an attack.
        let empty: &[u8] = &[];
        let err = read_frame(&mut { empty }).await.unwrap_err();
        assert!(matches!(err, ProtocolError::Closed), "got {err:?}");
    }

    #[tokio::test]
    async fn an_oversized_length_prefix_is_refused_before_allocating() {
        // The prefix comes from the network. Trusting it enough to allocate is
        // how one malformed frame becomes an out-of-memory kill.
        let mut framed = (u32::MAX).to_be_bytes().to_vec();
        framed.extend_from_slice(b"not actually this long");

        let err = read_frame(&mut framed.as_slice()).await.unwrap_err();
        assert!(matches!(err, ProtocolError::TooLarge { .. }), "got {err:?}");
    }

    #[test]
    fn a_proof_verifies_only_against_its_own_secret_and_nonce() {
        let n = nonce(NodeId::generate());
        let proof = prove("shared-secret", &n);

        assert!(proof_is_valid("shared-secret", &n, &proof));
        assert!(!proof_is_valid("different-secret", &n, &proof), "a wrong key must not verify");
        assert!(!proof_is_valid("shared-secret", b"another nonce", &proof), "replay must not work");
        assert!(!proof_is_valid("shared-secret", &n, b"forged"), "a forged proof must not verify");
    }

    #[test]
    fn the_secret_never_appears_in_a_proof() {
        let secret = "a-very-recognizable-cluster-secret";
        let proof = prove(secret, &nonce(NodeId::generate()));
        assert!(
            !proof.windows(secret.len()).any(|w| w == secret.as_bytes()),
            "the proof must not carry the key it was made with"
        );
    }

    #[test]
    fn nonces_do_not_repeat() {
        let node = NodeId::generate();
        let a = nonce(node);
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert_ne!(a, nonce(node), "a repeated nonce makes a captured proof replayable");
    }
}
