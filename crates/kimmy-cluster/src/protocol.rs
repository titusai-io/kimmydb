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

use kimmy_core::{CollectionId, Hlc, NodeId, OplogEntry, VersionVector};
use kimmy_storage::{SnapshotCursor, SnapshotPage};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Frames larger than this are refused rather than allocated.
///
/// A length prefix read from the network is attacker-controlled, so trusting it
/// enough to allocate is how a single malformed frame becomes an out-of-memory
/// kill. 64 MiB is far below anything that threatens a node.
///
/// It is **not** above every full oplog batch, which this comment used to claim.
/// [`MAX_BATCH`] bounds a response by entry count and this bounds it by bytes, so
/// entries averaging over 64 KiB make a full batch exceed the frame. See
/// [`Message::BatchTooLarge`] for what happens then.
///
/// Documents now travel as binary rather than as arrays of int32s, so an entry
/// costs about the document's own size and 64 KiB each is a genuinely large
/// document rather than an 5 KiB one. Reachable, but no longer routine.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

/// Entries a peer will send in one response.
///
/// Bounded so a node that is far behind catches up over several rounds rather
/// than in one frame it may not have the memory to hold. A *count*, so it does
/// not on its own keep a batch inside [`MAX_FRAME`] — see [`Message::BatchTooLarge`].
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
    ///
    /// `held` is the requester's witnessed vector — the one `from` was derived
    /// from — so the sender can judge the horizon per origin rather than by
    /// the single threshold: a requester below the coarse horizon is served
    /// when nothing it lacks has been collected, and told `BeyondHorizon`
    /// only when something has (ADR-097). Optional on the wire, because the
    /// handshake negotiates no version: a sender that predates the field
    /// ignores it and judges by `from` as before, and a requester that
    /// predates it sends none, which a sender reads the same way. Neither
    /// direction of a mixed-version cluster loses anything it had.
    AskEntries {
        from: Hlc,
        limit: usize,
        #[serde(default)]
        held: Option<VersionVector>,
    },
    /// The answer, in stamp order, and where the window it came from ended.
    ///
    /// `scanned_to` is the last stamp the sender's scan examined — an entry it
    /// withheld as readily as one it shipped — and `exhausted` says it stopped
    /// there because the oplog ended rather than because the batch filled.
    /// Together they are the receiver's coverage rule
    /// (`kimmy_storage::sync::coverage_after_batch`).
    ///
    /// **The sender reports the window's end; the receiver never infers it.**
    /// It used to be deduced from the batch's length, on the reasoning that a
    /// batch shorter than the limit had to be the sender's whole tail. That
    /// held only while nothing shortened a batch for another reason, and a
    /// withheld `UniqueViolation` (ADR-029) inside a window truncated at the
    /// limit does exactly that — the receiver then witnesses every entry past
    /// the window without ever being sent it, and nothing re-serves them
    /// (ADR-126, ADR-127). Stating the fact costs one stamp and one bool per
    /// batch and cannot be reopened by whatever the next filter is.
    Entries { entries: Vec<OplogEntry>, scanned_to: Hlc, exhausted: bool },
    /// "That many entries will not fit in a frame; ask for this many."
    ///
    /// [`MAX_BATCH`] bounds a response by entry count and [`MAX_FRAME`] bounds it
    /// by bytes, so entries averaging over 64 KiB make a full batch too large to
    /// send. Answering with the count that *does* fit lets the requester retry once
    /// rather than probing.
    ///
    /// **The sender must not simply serve fewer entries.** The entries it drops
    /// would sit inside a window it reported having scanned past, so the
    /// receiver would witness them without ever seeing them — the silent gap
    /// ADR-082, ADR-127 and `BeyondHorizon` all exist to prevent. Asking again
    /// for `fits` re-reads the window at the smaller limit, so the end it
    /// reports matches the entries it sends. `Engine::apply_peer_batch` clamps
    /// a non-exhausted window to its last delivered stamp, so a sender that
    /// ignores this loses the claim rather than the receiver's data — but the
    /// batch it trimmed is still short of what it said, so ask again instead.
    ///
    /// `fits` is zero when one entry alone exceeds the frame, which no limit can
    /// carry; the requester reports that rather than probing forever.
    BatchTooLarge { fits: usize },
    /// "I have collected the history you asked for; ask for a snapshot."
    ///
    /// Sent instead of `Entries` when the requester is below the sender's
    /// retention horizon. Serving it entries anyway would hand it a silent gap:
    /// it would apply what still exists, advance its version vector, and never
    /// learn what it missed.
    ///
    /// "Below the horizon" is judged per origin when the requester sent its
    /// vector (`AskEntries::held`), and by the threshold alone when it did
    /// not. The difference is a requester that trails one origin by a long
    /// silence and one entry: by the threshold it is beyond the horizon and
    /// pulls a snapshot; per origin, nothing it lacks was collected and it is
    /// served the entry (ADR-097).
    BeyondHorizon {},
    /// "Send me current state instead of history."
    AskSnapshot { after: Option<SnapshotCursor> },
    /// One page of it.
    Snapshot(Box<SnapshotPage>),
    /// "Which collections do you hold, and — for one of them — how many
    /// documents?"
    ///
    /// Sent only when the requester's own round found nothing left to pull
    /// (ADR-133): that belief rests on the version vector `AskVersions`
    /// already answered, and a truncated sync window is exactly what can
    /// make the belief false without anything on the anti-entropy path able
    /// to see it. `probe` names the one collection this round wants a live
    /// document count for — rotated by the caller so no round pays for more
    /// than one collection's scan — and is `None` when the requester holds
    /// none at all.
    AskDivergence { probe: Option<CollectionId> },
    /// The answer: every collection id this node currently holds, across
    /// every database, and the live document count of `probe` if this node
    /// holds it too.
    ///
    /// `collections` costs a metadata scan, not a document read — the same
    /// bound `Engine::all_collection_ids` states. `probe_count` is the one
    /// piece of this exchange that reads documents, and it reads exactly one
    /// collection's worth.
    Divergence { collections: Vec<CollectionId>, probe_count: Option<u64> },
    /// "What have you processed?"
    ///
    /// The receiver's *witnessed* vector — what it has processed per origin,
    /// appended or not — where `AskVersions` answers with what a node can
    /// *serve*. The one caller is a push (ADR-143): the pusher derives the
    /// window the member lacks from this exactly as the member would derive
    /// it for itself, so a push never carries an entry out of order.
    AskWitnessed {},
    /// The answer.
    Witnessed(VersionVector),
    /// "Here is the window you would have pulled from me; apply it now and
    /// tell me what became of it."
    ///
    /// The one message in this protocol that moves entries *toward* a peer
    /// rather than pulling them, and it exists for exactly one caller: a
    /// schema change confirming itself on every live member before its
    /// request answers (ADR-140). **A push is a pull the sender starts**
    /// (ADR-143): `entries`, `scanned_to` and `exhausted` are what
    /// `AskEntries` would have answered had the receiver asked from its own
    /// witnessed position, and `versions` is what `AskVersions` would have
    /// answered first — so the receiver accounts for the window through the
    /// same coverage rule a pulled one goes through, and its witnessed
    /// vector is raised only over entries it was sent. Pushing a lone entry
    /// through `apply_batch`, as this message first did, raised the vector
    /// past every earlier entry from the same origin the receiver had not
    /// yet pulled, and nothing re-served them: a member could be handed an
    /// index for a collection it was never sent.
    Push { entries: Vec<OplogEntry>, scanned_to: Hlc, exhausted: bool, versions: VersionVector },
    /// What the pushed window became on the receiver: the fields of its
    /// `SyncOutcome` a pusher can act on. `ddl_declined` is a drop the
    /// receiver turned away as older than the index it holds (ADR-141).
    Pushed {
        applied: usize,
        ddl: usize,
        ddl_refused: usize,
        unknown_collection: usize,
        ddl_declined: usize,
    },
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

/// `HMAC-SHA256(secret, nonce || binding)`.
///
/// `binding` is the TLS session's exported keying material ([`crate::tls`]).
/// Including it is what makes a relayed proof useless: a man-in-the-middle runs
/// two TLS sessions, their exporters differ, so a proof captured from one does
/// not validate on the other — and recomputing it needs the secret.
///
/// The two inputs are length-prefixed rather than concatenated. Without that,
/// a nonce of `A` with binding `BC` and a nonce of `AB` with binding `C` hash
/// the same bytes, and an attacker who can influence one could shift the
/// boundary. The same reasoning as the separator in `CollectionId::derive`.
pub fn prove(secret: &str, nonce: &[u8], binding: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(&(nonce.len() as u64).to_be_bytes());
    mac.update(nonce);
    mac.update(&(binding.len() as u64).to_be_bytes());
    mac.update(binding);
    mac.finalize().into_bytes().to_vec()
}

/// Check a proof in constant time.
///
/// A byte-by-byte comparison leaks how much of a forged proof was correct,
/// which is enough to recover the rest one byte at a time.
pub fn proof_is_valid(secret: &str, nonce: &[u8], binding: &[u8], proof: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    let expected = prove(secret, nonce, binding);
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

/// Bytes an authentication tag adds to every membership datagram.
pub const TAG_LEN: usize = 32;

/// Tag a membership datagram: `HMAC-SHA256(secret, payload) || payload`.
///
/// SWIM is connectionless by design — which is what makes failure detection
/// cheap — so there is no session to authenticate once, and every datagram
/// carries its own proof. See [ADR-053](../../../docs/decisions.md).
pub fn tag_datagram(secret: &str, payload: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(payload);

    let mut out = Vec::with_capacity(TAG_LEN + payload.len());
    out.extend_from_slice(&mac.finalize().into_bytes());
    out.extend_from_slice(payload);
    out
}

/// The payload of a datagram whose tag verifies, or `None`.
///
/// Constant-time, for the reason [`proof_is_valid`] gives: a byte-by-byte
/// comparison leaks how much of a forged tag was correct, which is enough to
/// recover the rest one byte at a time.
///
/// This proves the sender holds the cluster secret. It does **not** prevent a
/// captured datagram being replayed — see ADR-053 for why that is out of
/// scope here rather than overlooked.
pub fn untag_datagram<'a>(secret: &str, datagram: &'a [u8]) -> Option<&'a [u8]> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    use subtle::ConstantTimeEq;

    if datagram.len() < TAG_LEN {
        return None;
    }
    let (tag, payload) = datagram.split_at(TAG_LEN);

    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(payload);
    let expected = mac.finalize().into_bytes();

    bool::from(expected.ct_eq(tag)).then_some(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// RFC 4231 test case 2, against the primitive these functions are built
    /// on. A dependency bump that changed what `Hmac<Sha256>` computes would
    /// re-tag every datagram on the wire and invalidate every webhook
    /// signature a receiver has stored — silently, because both sides of a
    /// single-version cluster would agree with each other and with nothing
    /// else. This is the check that does not move when the crates do.
    #[test]
    fn hmac_sha256_matches_rfc_4231() {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;

        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(b"Jefe").expect("any key length");
        mac.update(b"what do ya want for nothing?");
        assert_eq!(
            hex(&mac.finalize().into_bytes()),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    /// The framing, not just the primitive: length prefixes in [`prove`] and
    /// the tag layout in [`tag_datagram`]. Recorded from the implementation
    /// these values were first produced by, so a refactor that reorders an
    /// `update` or drops a prefix is caught even though the HMAC itself is
    /// still correct.
    #[test]
    fn the_wire_tags_are_what_they_have_always_been() {
        assert_eq!(
            hex(&prove("cluster-secret", b"nonce-bytes", b"binding-bytes")),
            "20f4bb5890196f25e48f02fb57c95b75b3c1b20be30e9b67140d30a8443e193c"
        );
        assert_eq!(
            hex(&tag_datagram("cluster-secret", b"payload-bytes")[..TAG_LEN]),
            "8bd9e1ea949555c5dd98a9831e2f79e90717175ac246fb8ce42de1927f6a9796"
        );
    }

    #[test]
    fn a_tagged_datagram_round_trips_under_the_right_secret() {
        let payload = b"a swim probe".as_slice();
        let wire = tag_datagram("shared-secret", payload);

        assert_eq!(wire.len(), TAG_LEN + payload.len());
        assert_eq!(&wire[TAG_LEN..], payload, "the payload rides in clear; only origin is proven");
        assert_eq!(untag_datagram("shared-secret", &wire), Some(payload));
    }

    #[test]
    fn a_datagram_from_the_wrong_secret_is_refused() {
        // The whole point. A node configured with a different secret joined a
        // real cluster's member set, became a webhook ownership candidate, and
        // delivered nothing — found by driving a five-node cluster (ADR-053).
        let wire = tag_datagram("their-secret", b"a swim probe");
        assert_eq!(untag_datagram("our-secret", &wire), None);
    }

    #[test]
    fn a_tampered_payload_is_refused() {
        let mut wire = tag_datagram("shared-secret", b"a swim probe");
        let last = wire.len() - 1;
        wire[last] ^= 0xFF;
        assert_eq!(untag_datagram("shared-secret", &wire), None, "the tag covers the payload");
    }

    #[test]
    fn a_truncated_or_untagged_datagram_is_refused() {
        // An old node sends the bare payload with no tag at all. It must be
        // refused rather than misread as a tag followed by a short body —
        // which is why this is a wire break and a stop-the-cluster upgrade.
        assert_eq!(untag_datagram("shared-secret", b"a swim probe"), None);
        assert_eq!(untag_datagram("shared-secret", b""), None);
        assert_eq!(untag_datagram("shared-secret", &[0u8; TAG_LEN - 1]), None);

        // Exactly a tag and nothing else is a legitimate empty payload.
        let empty = tag_datagram("shared-secret", b"");
        assert_eq!(empty.len(), TAG_LEN);
        assert_eq!(untag_datagram("shared-secret", &empty), Some(b"".as_slice()));
    }

    #[test]
    fn the_tag_is_not_the_handshake_proof() {
        // Two constructions over one secret. If they ever coincided, a
        // datagram captured off the wire would be a usable handshake proof.
        let payload = b"a swim probe".as_slice();
        let tagged = tag_datagram("shared-secret", payload);
        assert_ne!(&tagged[..TAG_LEN], prove("shared-secret", payload, b"").as_slice());
    }

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
            Message::AskEntries { from: Hlc::new(7, 1), limit: 10, held: None },
            Message::AskEntries { from: Hlc::new(7, 1), limit: 10, held: Some(populated_vector()) },
            Message::Versions(populated_vector()),
            Message::Entries { entries: Vec::new(), scanned_to: Hlc::new(11, 2), exhausted: false },
            Message::Entries { entries: Vec::new(), scanned_to: Hlc::ZERO, exhausted: true },
            Message::Hello { node: NodeId::generate(), nonce: vec![1, 2, 3] },
            Message::Confirm { proof: vec![9, 9] },
            Message::AskDivergence { probe: Some(CollectionId(42)) },
            Message::AskDivergence { probe: None },
            Message::Divergence {
                collections: vec![CollectionId(1), CollectionId(2)],
                probe_count: Some(7),
            },
            Message::Divergence { collections: Vec::new(), probe_count: None },
            Message::AskWitnessed {},
            Message::Witnessed(populated_vector()),
            Message::Push {
                entries: Vec::new(),
                scanned_to: Hlc::new(11, 2),
                exhausted: false,
                versions: populated_vector(),
            },
            Message::Pushed {
                applied: 1,
                ddl: 2,
                ddl_refused: 0,
                unknown_collection: 0,
                ddl_declined: 3,
            },
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
        write_frame(&mut buffer, &Message::AskEntries { from: Hlc::ZERO, limit: 5, held: None })
            .await
            .unwrap();

        let mut stream = buffer.as_slice();
        assert_eq!(read_frame(&mut stream).await.unwrap(), Message::AskVersions {});
        assert_eq!(
            read_frame(&mut stream).await.unwrap(),
            Message::AskEntries { from: Hlc::ZERO, limit: 5, held: None }
        );
    }

    #[tokio::test]
    async fn ask_entries_crosses_a_version_boundary_in_both_directions() {
        // The handshake negotiates no protocol version, so a field added to a
        // request has to be one an older peer can ignore and a newer peer can
        // do without. Both halves, as frames: what a requester before `held`
        // sends, and what a sender before `held` sees.
        let from = Hlc::new(7, 1);

        // A frame from a requester that predates the field: no `held` at all.
        let old_request = bson::doc! { "AskEntries": { "from": bson::serialize_to_bson(&from).unwrap(), "limit": 10i64 } };
        let mut buffer = Vec::new();
        let body = bson::serialize_to_vec(&old_request).unwrap();
        buffer.extend_from_slice(&(body.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&body);
        assert_eq!(
            read_frame(&mut buffer.as_slice()).await.unwrap(),
            Message::AskEntries { from, limit: 10, held: None },
            "a request without the field must read as one that did not send it"
        );

        // A frame carrying a field this build does not know, standing in for
        // what a sender that predates `held` sees when a newer requester
        // writes one: it must be ignored, not refused.
        let future = bson::doc! { "AskEntries": { "from": bson::serialize_to_bson(&from).unwrap(), "limit": 10i64, "somethingNewer": true } };
        let mut buffer = Vec::new();
        let body = bson::serialize_to_vec(&future).unwrap();
        buffer.extend_from_slice(&(body.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&body);
        assert_eq!(
            read_frame(&mut buffer.as_slice()).await.unwrap(),
            Message::AskEntries { from, limit: 10, held: None },
            "a field this build does not know must not fail the frame"
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

    const BINDING: &[u8] = b"a-tls-exporter-value-32-bytes-ok";

    #[test]
    fn a_proof_verifies_only_against_its_own_secret_and_nonce() {
        let n = nonce(NodeId::generate());
        let proof = prove("shared-secret", &n, BINDING);

        assert!(proof_is_valid("shared-secret", &n, BINDING, &proof));
        assert!(
            !proof_is_valid("different-secret", &n, BINDING, &proof),
            "a wrong key must not verify"
        );
        assert!(
            !proof_is_valid("shared-secret", b"another nonce", BINDING, &proof),
            "replay must not work"
        );
        assert!(
            !proof_is_valid("shared-secret", &n, BINDING, b"forged"),
            "a forged proof must not verify"
        );
    }

    #[test]
    fn a_proof_does_not_verify_against_a_different_channel() {
        // This is the whole of the man-in-the-middle defence. A relay holds two
        // TLS sessions with different exporters, so a proof captured on one
        // must not validate on the other. If this ever passes, the TLS between
        // nodes is confidentiality against a passive listener and nothing more
        // — and it would still *look* like it was working.
        let n = nonce(NodeId::generate());
        let proof = prove("shared-secret", &n, BINDING);

        assert!(
            !proof_is_valid("shared-secret", &n, b"a-different-tls-session-exporter!", &proof),
            "a proof relayed onto another TLS session must be rejected"
        );
    }

    #[test]
    fn the_nonce_and_binding_boundary_cannot_be_shifted() {
        // Without length prefixes, nonce=`AB` binding=`C` and nonce=`A`
        // binding=`BC` hash identical bytes, so an attacker able to influence
        // one field could move the boundary and reuse a proof.
        let a = prove("s", b"AB", b"C");
        let b = prove("s", b"A", b"BC");
        assert_ne!(a, b, "the two fields must not be able to trade bytes");
    }

    #[test]
    fn the_secret_never_appears_in_a_proof() {
        let secret = "a-very-recognizable-cluster-secret";
        let proof = prove(secret, &nonce(NodeId::generate()), BINDING);
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
