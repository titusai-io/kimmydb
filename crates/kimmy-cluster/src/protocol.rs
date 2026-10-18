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

/// What a member holds of the sender's block (ADR-213): the sender's boot id and
/// the generation of the block recorded from that boot. **Confirmation is the
/// reader's**: a member's yield counts as read by a peer only when a frame from
/// the peer carries an echo naming the member's current boot and a generation at
/// least the one that set the bit. A member that holds nothing sends an empty
/// boot and generation zero, which is *echoing, and confirming nothing*; a build
/// that predates the field sends no echo at all and never confirms.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Echo {
    #[serde(default, with = "serde_bytes")]
    pub boot: Vec<u8>,
    /// A per-process counter: it encodes as BSON `Int64` and never nears
    /// `i64::MAX`.
    #[serde(default)]
    pub generation: u64,
}

/// A generation on a frame, read leniently: anything but a non-negative integer
/// is `None`, so a field of another shape costs the field and never the frame.
fn lenient_generation<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    crate::facts::lenient_millis(deserializer)
}

/// A position on a frame, read leniently: anything that is not one reads as
/// absent, so a field of another shape costs the field and never the frame.
fn lenient_hlc<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<Hlc>, D::Error> {
    let raw = Option::<bson::Bson>::deserialize(deserializer)?;
    Ok(raw.and_then(|raw| bson::deserialize_from_bson::<Hlc>(raw).ok()))
}

/// A digest request, read leniently (B3): one that does not decode is none, so a
/// field of another shape costs the field and never the frame.
fn lenient_digest_ask<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<kimmy_storage::DigestAsk>, D::Error> {
    let raw = Option::<bson::Bson>::deserialize(deserializer)?;
    Ok(raw.and_then(|raw| bson::deserialize_from_bson(raw).ok()))
}

/// A digest answer, read leniently, as the request is.
fn lenient_digest_reply<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<kimmy_storage::DigestReply>, D::Error> {
    let raw = Option::<bson::Bson>::deserialize(deserializer)?;
    Ok(raw.and_then(|raw| bson::deserialize_from_bson(raw).ok()))
}

/// A definition request, read leniently (B3 half 2), as the digest's is.
fn lenient_definition_ask<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<kimmy_storage::DefinitionAsk>, D::Error> {
    let raw = Option::<bson::Bson>::deserialize(deserializer)?;
    Ok(raw.and_then(|raw| bson::deserialize_from_bson(raw).ok()))
}

/// A definition answer, read leniently, as the request is.
fn lenient_definition_reply<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<kimmy_storage::DefinitionReply>, D::Error> {
    let raw = Option::<bson::Bson>::deserialize(deserializer)?;
    Ok(raw.and_then(|raw| bson::deserialize_from_bson(raw).ok()))
}

/// An echo, read leniently: one that does not decode is none, which reads as a
/// peer that does not echo.
fn lenient_echo<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Echo>, D::Error> {
    let raw = Option::<bson::Bson>::deserialize(deserializer)?;
    Ok(raw.and_then(|raw| bson::deserialize_from_bson::<Echo>(raw).ok()))
}

/// Why a replay page could not be applied yet, and is not a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayStall {
    /// A page named a collection this member does not hold yet.
    UnknownCollection,
    /// A page named a collection whose drop this member is still purging.
    PurgePending,
}

/// What one side says to the other.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// Opens a connection: who I am, and a challenge for you to answer.
    Hello { node: NodeId, nonce: Vec<u8> },
    /// Answers the challenge, and issues one of my own.
    Welcome { node: NodeId, nonce: Vec<u8>, proof: Vec<u8> },
    /// Answers the responder's challenge. The handshake is complete after this.
    Confirm { proof: Vec<u8> },
    /// "What do you hold?" — and, when `witnessed` is set, "what have you
    /// processed?"
    ///
    /// A struct variant rather than a unit variant: BSON has no
    /// representation for a bare value at the top level, and a unit variant
    /// serializes to a string. Every message on this wire must be a document.
    ///
    /// `witnessed` asks for the receiver's witnessed vector beside its
    /// servable one, answered as [`Message::Vectors`] (ADR-146): the count
    /// half of the divergence check gates on what the peer has *processed*,
    /// which its servable vector understates for ever after an entry it
    /// processed without appending. Optional on the wire, for the reason
    /// `AskEntries::held` is: the handshake negotiates no version, so a
    /// receiver that predates the field ignores it and answers
    /// [`Message::Versions`] as before, which the requester reads as "no
    /// witnessed vector" and gates on the servable one for that contact; a
    /// requester that predates it sends none and is answered as it always
    /// was. Neither direction of a mixed-version cluster fails a round.
    AskVersions {
        #[serde(default)]
        witnessed: bool,
        /// The requester's own block (ADR-201), so a member that is only ever
        /// contacted and never contacts still hands it over. Optional on the
        /// wire: a receiver that predates it ignores the key, and a requester
        /// that predates it sends none, which reads as unknown.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::facts::lenient"
        )]
        facts: Option<std::sync::Arc<crate::facts::Facts>>,
        /// The generation of the requester's own block (ADR-213), so the server
        /// can order blocks from one process. Optional and lenient.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "lenient_generation"
        )]
        facts_gen: Option<u64>,
        /// The server's block as the requester **holds it now** (ADR-213): the
        /// confirmation of the server's yield, on the serve path.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "lenient_echo"
        )]
        echo: Option<Echo>,
    },
    /// The answer: what the receiver can serve.
    Versions(VersionVector),
    /// The answer to an `AskVersions` that set `witnessed`: what the receiver
    /// can serve, and what it has processed (ADR-146). `servable` is exactly
    /// what [`Message::Versions`] carries; `witnessed` is what
    /// [`Message::Witnessed`] carries. One frame rather than two requests,
    /// because every sync round asks the first question and a checked
    /// contact needs the second.
    Vectors {
        servable: VersionVector,
        witnessed: VersionVector,
        /// The answerer's own block (ADR-201), with the same compatibility as
        /// `AskVersions::facts`: an older requester drops the key, and an older
        /// answerer sends none.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::facts::lenient"
        )]
        facts: Option<std::sync::Arc<crate::facts::Facts>>,
        /// The generation of the answerer's block in this reply (ADR-213).
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "lenient_generation"
        )]
        facts_gen: Option<u64>,
        /// The requester's block as the server **holds it after recording** the
        /// request's (ADR-213): the confirmation of the requester's yield, on the
        /// sync path. A reply's `facts_gen` confirms nothing.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "lenient_echo"
        )]
        echo: Option<Echo>,
        /// This member's replay of its own origin is armed and the requester has
        /// not answered it: the position the replay asks from, so the requester
        /// serves it on this connection (ADR-212's addendum). Absent when there
        /// is nothing to ask, and from an older answerer, which an older
        /// requester reads by ignoring the key.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "lenient_hlc"
        )]
        replay_from: Option<Hlc>,
    },
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
        /// Spans of history the requester holds as state at or below `held`,
        /// which the skip would otherwise never serve it again (ADR-172):
        /// the sender serves the entries inside them as well. Written on
        /// every request, empty when there are none, so a mixed-version
        /// cluster exercises the boundary on every pull. Optional on the wire
        /// for the reason `held` is: a sender that predates the field ignores
        /// it and serves as ADR-171 does, and a requester that predates it
        /// sends none, which reads as no spans.
        #[serde(default)]
        marked: Vec<kimmy_storage::MarkedRange>,
        /// The requester can take a partial window (ADR-194): the sender may
        /// end its walk at a budget, however few entries it has kept, and say
        /// where in `Entries::passed_through`. A sender that predates the
        /// field ignores it and serves the whole window, as before; a
        /// requester that predates it sends none, which reads as `false`.
        #[serde(default)]
        partial: bool,
    },
    /// The answer, in stamp order, and where the window it came from ended.
    ///
    /// `scanned_to` is the last stamp the sender's scan examined — an entry it
    /// withheld as readily as one it shipped — and `exhausted` says it stopped
    /// there because the oplog ended rather than because the batch filled.
    /// Together they are the receiver's coverage rule
    /// (`kimmy_storage::sync::coverage_up_to`).
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
    ///
    /// `passed_through` is the full stamp of the last row the sender
    /// examined, when its budget ended the window rather than the batch or
    /// the oplog (ADR-194): only in answer to an `AskEntries` that set
    /// `partial`, with `scanned_to` its timestamp and `exhausted` false, and
    /// the window then covers everything through it, however few entries it
    /// carries. A requester that predates the field ignores it; a sender that
    /// predates it sends none.
    Entries {
        entries: Vec<OplogEntry>,
        scanned_to: Hlc,
        exhausted: bool,
        #[serde(default)]
        passed_through: Option<kimmy_core::Stamp>,
        /// The idempotency records the shipped entries carry (B5, "How a
        /// record travels"): `{collection, scope hash, carrying stamp, record
        /// bytes}`, read in the read transaction that read the window and only
        /// for the stamps shipped. The bytes are opaque here and pass through.
        /// Optional on the wire and read leniently: an older receiver ignores
        /// the field, and an older sender sends none, which reads as no records.
        #[serde(
            default,
            skip_serializing_if = "Vec::is_empty",
            deserialize_with = "kimmy_storage::lenient_records"
        )]
        records: Vec<kimmy_storage::CarriedRecord>,
    },
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
    /// A page of the entries the requester holds of the **answerer's own origin**
    /// above `from`, sent on a connection whose `Vectors` asked for them
    /// (`replay_from`). `exhausted` says the sender holds nothing more above this
    /// page; `horizon` says it has collected the oplog below `from` and cannot
    /// say. Only ever sent to a peer that asked, so an older build never meets it.
    ReplayEntries {
        from: Hlc,
        entries: Vec<OplogEntry>,
        exhausted: bool,
        #[serde(default)]
        horizon: bool,
        /// How far the sender's walk got. It is at least the last entry's stamp (a
        /// page that says less is refused); a value below `from` is read as `from`.
        /// On an empty page that is not the end, the requester moves its cursor
        /// here, as `Entries::scanned_to` moves a pull's. A walk the sender's budget
        /// cut short before it reached the requester's entries therefore still
        /// makes progress at every contact.
        scanned_to: Hlc,
        /// The idempotency records the shipped entries carry (B5, "How a
        /// record travels"): `{collection, scope hash, carrying stamp, record
        /// bytes}`, read in the read transaction that read the window and only
        /// for the stamps shipped. The bytes are opaque here and pass through.
        /// Optional on the wire and read leniently: an older receiver ignores
        /// the field, and an older sender sends none, which reads as no records.
        #[serde(
            default,
            skip_serializing_if = "Vec::is_empty",
            deserialize_with = "kimmy_storage::lenient_records"
        )]
        records: Vec<kimmy_storage::CarriedRecord>,
    },
    /// The answer to [`Message::ReplayEntries`]: where the replay stands now.
    /// `next_from` is the position it asks from next; `done` that the sender has
    /// answered it completely; `stalled` that a page named something this member
    /// must first receive by the ordinary pull, so the replay stops for this
    /// contact and goes on at the next, with nothing answered.
    ReplayAck {
        next_from: Hlc,
        done: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stalled: Option<ReplayStall>,
    },
    /// "Send me current state instead of history."
    ///
    /// `collection` scopes the snapshot to one collection (ADR-152): a
    /// repair is planned for one (ADR-148), and the whole database was what
    /// it pulled. `None` is the whole-database snapshot a member below the
    /// sender's retention horizon needs. `after` is where to resume — the
    /// requester carries it between rounds, so a snapshot that does not fit
    /// one round goes on from its last page rather than page one.
    ///
    /// Optional on the wire, for the reason `AskEntries::held` is: the
    /// handshake negotiates no version. A sender that predates the field
    /// ignores it and serves the whole database, which the requester
    /// applies as it always did — a repair then costs what it cost before
    /// this field, and nothing is misread; a requester that predates it
    /// sends none and is served the whole database as before.
    AskSnapshot {
        after: Option<SnapshotCursor>,
        #[serde(default)]
        collection: Option<CollectionId>,
    },
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
    ///
    /// `digest` (B3) asks the cut digest as well: the cut, and the keys the
    /// requester excludes. Optional on the wire and read leniently, so an
    /// answering peer that predates it ignores it, and a field of another
    /// shape costs the field and never the frame. Absent on a requester that
    /// predates it or has nothing to ask.
    AskDivergence {
        probe: Option<CollectionId>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "lenient_digest_ask"
        )]
        digest: Option<kimmy_storage::DigestAsk>,
        /// The definition digest (B3 half 2) asked as well: the same cut the
        /// digest carries, and the collections whose per-index detail the
        /// requester wants. Optional on the wire and read leniently, so an
        /// answering peer that predates it ignores it, and a field of another
        /// shape costs the field and never the frame.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "lenient_definition_ask"
        )]
        definitions: Option<kimmy_storage::DefinitionAsk>,
    },
    /// The answer: every collection id this node currently holds, across
    /// every database, and the live document count of `probe` if this node
    /// holds it too.
    ///
    /// `collections` costs a metadata scan, not a document read — the same
    /// bound `Engine::all_collection_ids` states. `probe_count` is the one
    /// piece of this exchange that reads documents, and it reads exactly one
    /// collection's worth.
    ///
    /// `incarnations` is the `created` stamp of each of those collections,
    /// which is what lets the requester tell a collection it dropped and this
    /// node has not heard about yet from one recreated since: a name dropped
    /// and created again derives the same id, so `collections` alone cannot.
    /// A `Vec` on the wire like `collections`, made a map at the boundary.
    ///
    /// Optional on the wire, for the reason `AskEntries::held` is: the
    /// handshake negotiates no version. A requester that predates the field
    /// ignores it and compares names alone as before; a sender that predates
    /// it writes none, which the requester reads as "the incarnation I
    /// dropped" for anything it holds a tombstone for
    /// (`kimmy_storage::compare_divergence`, which states what that costs).
    /// `collections` is untouched, so an older receiver reads the answer
    /// exactly as it always did.
    Divergence {
        collections: Vec<CollectionId>,
        probe_count: Option<u64>,
        #[serde(default)]
        incarnations: Vec<(CollectionId, Hlc)>,
        /// The cut digest of every collection quiet at the requester's cut,
        /// and of the one busy collection it named (B3). `None` from a peer
        /// that predates it, which a requester counts as unanswered and never
        /// compares; absent from the frame when there is nothing to say, so
        /// the answer to an older requester is byte for byte what it was.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "lenient_digest_reply"
        )]
        digests: Option<kimmy_storage::DigestReply>,
        /// The peer was already answering a digest probe and answered this one
        /// without digests: distinct from `None`, which is what a peer that
        /// predates the digest sends.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        digest_busy: bool,
        /// The definition digests (B3 half 2): **present iff the request carried
        /// `definitions`** (possibly with no entries), absent otherwise, so an
        /// answer to an older requester is byte for byte what it was and an
        /// empty answer is never read as a peer that predates the digest.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "lenient_definition_reply"
        )]
        definitions: Option<kimmy_storage::DefinitionReply>,
    },
    /// "What have you processed?"
    ///
    /// The receiver's *witnessed* vector — what it has processed per origin,
    /// appended or not — where `AskVersions` answers with what a node can
    /// *serve*. The one caller is a push (ADR-143): the pusher derives the
    /// window the member lacks from this exactly as the member would derive
    /// it for itself, so a push never carries an entry out of order. A sync
    /// round asks the same question through `AskVersions { witnessed: true }`
    /// (ADR-146), on the frame it already spends.
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
    Push {
        entries: Vec<OplogEntry>,
        scanned_to: Hlc,
        exhausted: bool,
        versions: VersionVector,
        /// The idempotency records the shipped entries carry (B5, "How a
        /// record travels"): `{collection, scope hash, carrying stamp, record
        /// bytes}`, read in the read transaction that read the window and only
        /// for the stamps shipped. The bytes are opaque here and pass through.
        /// Optional on the wire and read leniently: an older receiver ignores
        /// the field, and an older sender sends none, which reads as no records.
        #[serde(
            default,
            skip_serializing_if = "Vec::is_empty",
            deserialize_with = "kimmy_storage::lenient_records"
        )]
        records: Vec<kimmy_storage::CarriedRecord>,
    },
    /// What the pushed window became on the receiver: the fields of its
    /// `SyncOutcome` a pusher can act on. `ddl_declined` is a drop the
    /// receiver turned away as older than the index it holds (ADR-141).
    Pushed {
        applied: usize,
        /// Schema changes the receiver applied, not counting those whose
        /// entry it already held as sent: those are `ddl_held`.
        ddl: usize,
        /// Schema changes the window carried that the receiver already held,
        /// entry and all, and so did not apply again or append. Absent from a
        /// receiver on a version before the field, which counted them in
        /// `ddl`.
        #[serde(default)]
        ddl_held: usize,
        ddl_refused: usize,
        unknown_collection: usize,
        ddl_declined: usize,
        /// Entries the receiver left for its next pull rather than
        /// witnessing (ADR-148). Absent from a receiver on a version before
        /// the field, which never left any.
        #[serde(default)]
        deferred: usize,
        /// Batches the receiver stopped at a creation that waits for its drop
        /// purger (ADR-189). Absent from a receiver on a version before the
        /// field, which never stopped there.
        #[serde(default)]
        purge_pending: usize,
        /// The entries in the window the receiver refused or declined, by
        /// stamp (ADR-191): what lets a pusher whose window carried several
        /// changes say which of them a member could not take. Absent from a
        /// receiver on a version before the field; the pusher then finds it
        /// does not account for the counts above and says so rather than
        /// guessing (ADR-191).
        #[serde(default)]
        refused: Vec<kimmy_core::Stamp>,
        /// The entry the batch stopped at, for an unknown collection
        /// (ADR-148) or a creation waiting for the drop purger (ADR-189): it
        /// and everything after it in the window were not taken. Absent from
        /// a receiver on a version before the field, like `refused`.
        #[serde(default)]
        stopped_at: Option<kimmy_core::Stamp>,
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
    /// A bounded wait ran out: the handshake, a push, or a whole sync round.
    ///
    /// Separate from [`Self::Malformed`] because the two send an operator to
    /// opposite places. A malformed frame is a WIRE problem — a version skew,
    /// a capability neither side negotiated, a corrupted or truncated frame —
    /// and the things to look at are peer builds and the negotiation. A
    /// timeout says the peer did not answer in time, which is load, a stalled
    /// disk, a saturated link or a peer that is wedged, and none of that is
    /// visible in a frame. Reporting one as the other sends whoever is
    /// holding the pager to read protocol code for a problem that is not
    /// there.
    #[error("{0} timed out")]
    TimedOut(String),
    #[error("frame of {size} bytes exceeds the {MAX_FRAME} byte limit")]
    TooLarge { size: usize },
    #[error("peer failed authentication")]
    Unauthenticated,
    #[error("peer reported: {0}")]
    Fault(String),
    #[error("peer closed the connection")]
    Closed,
    /// This node's own storage failed while answering the peer: a read to
    /// serve it, or a write applying a window it pushed. Separate from
    /// [`Self::Malformed`] because a full disk is not a wire problem, and
    /// sending whoever reads the log to the peer's frames for it wastes
    /// their time.
    #[error("local storage: {0}")]
    Local(String),
    /// This node has closed its storage to writes at the end of a shutdown
    /// (ADR-192), so a pushed window was not applied. Not a failure: the
    /// window stays the pusher's, and anti-entropy carries it to this node
    /// once it runs again.
    #[error("this node is shutting down: {0}")]
    Stopping(String),
    /// The peer at the far end of the connection is this node itself (the
    /// handshake named this node's own id): a seed that names it by an address
    /// it does not bind, a wildcard bind reached by a loopback address. No round
    /// is run with it (ADR-202): what it reads is this node's own state, which
    /// says nothing about the cluster.
    #[error("the peer is this node itself")]
    SelfContact,
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
    decode(&read_body(reader).await?)
}

/// [`read_frame`], whether this frame carried a `facts` block that did not
/// decode (ADR-201), and the **decode sequence** taken as it decoded (ADR-213).
/// The flag is cleared and taken with **no await between**, right around the
/// decode, so another task decoding on the same thread while this one waited for
/// its bytes cannot set it: it names this frame's peer or none. The sequence
/// travels with the block to wherever it is recorded, however late.
pub(crate) async fn read_frame_noting_facts<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(Message, bool, u64), ProtocolError> {
    let body = read_body(reader).await?;
    crate::facts::take_undecodable();
    let message = decode(&body)?;
    let seq = crate::facts::next_decoded_seq();
    Ok((message, crate::facts::take_undecodable(), seq))
}

fn decode(body: &[u8]) -> Result<Message, ProtocolError> {
    bson::deserialize_from_slice(body)
        .map_err(|e| ProtocolError::Malformed(format!("decoding: {e}")))
}

/// The bytes of one length-prefixed frame.
async fn read_body<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, ProtocolError> {
    // Closed only at a frame boundary, before the first byte of a prefix. A
    // connection that ends inside the prefix ended mid-frame, which is an
    // I/O failure like one that ends inside the body.
    //
    // At the boundary, TLS's end without `close_notify` is a close too:
    // rustls reports it as `UnexpectedEof` rather than as the end, but every
    // exchange before it was whole, since frames carry their own length, so
    // nothing was cut short. It is how a peer before 0.41 ended every round,
    // and how any peer's connection ends when its process goes.
    let mut len = [0u8; 4];
    let mut got = 0;
    while got < len.len() {
        let read = match reader.read(&mut len[got..]).await {
            Err(e) if got == 0 && e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(ProtocolError::Closed);
            }
            read => read?,
        };
        match read {
            0 if got == 0 => return Err(ProtocolError::Closed),
            0 => {
                return Err(ProtocolError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("the connection closed after {got} of a frame's 4 length bytes"),
                )));
            }
            n => got += n,
        }
    }

    // Checked *before* allocating: the length comes from the network.
    let size = u32::from_be_bytes(len) as usize;
    if size > MAX_FRAME {
        return Err(ProtocolError::TooLarge { size });
    }

    let mut body = vec![0u8; size];
    reader.read_exact(&mut body).await?;
    Ok(body)
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
            Message::AskVersions { witnessed: false, facts: None, facts_gen: None, echo: None },
            Message::AskVersions { witnessed: true, facts: None, facts_gen: None, echo: None },
            Message::AskEntries {
                from: Hlc::new(7, 1),
                limit: 10,
                held: None,
                marked: Vec::new(),
                partial: false,
            },
            Message::AskEntries {
                from: Hlc::new(7, 1),
                limit: 10,
                held: Some(populated_vector()),
                marked: Vec::new(),
                partial: false,
            },
            Message::AskEntries {
                from: Hlc::new(7, 1),
                limit: 10,
                held: Some(populated_vector()),
                marked: vec![kimmy_storage::MarkedRange {
                    origin: NodeId::generate(),
                    from: Hlc::new(3, 0),
                    through: Hlc::new(5, 2),
                }],
                partial: false,
            },
            Message::Versions(populated_vector()),
            Message::Vectors {
                servable: populated_vector(),
                witnessed: populated_vector(),
                facts: None,
                facts_gen: None,
                echo: None,
                replay_from: None,
            },
            Message::Entries {
                entries: Vec::new(),
                scanned_to: Hlc::new(11, 2),
                exhausted: false,
                passed_through: None,
                records: Vec::new(),
            },
            Message::Entries {
                entries: Vec::new(),
                scanned_to: Hlc::ZERO,
                exhausted: true,
                passed_through: None,
                records: Vec::new(),
            },
            Message::Hello { node: NodeId::generate(), nonce: vec![1, 2, 3] },
            Message::Confirm { proof: vec![9, 9] },
            Message::AskDivergence {
                probe: Some(CollectionId(42)),
                digest: None,
                definitions: None,
            },
            Message::AskDivergence { probe: None, digest: None, definitions: None },
            Message::AskSnapshot { after: None, collection: None },
            // The unencodable half of the id space, and a cursor: the shape
            // every scoped repair of such a collection sends.
            Message::AskSnapshot { after: None, collection: Some(CollectionId(u64::MAX)) },
            Message::AskSnapshot {
                after: Some(SnapshotCursor {
                    collection: CollectionId(u64::MAX),
                    after_key: vec![1, 2],
                    records: None,
                    // A records delta's base and where a records-only page
                    // stopped (B5).
                    records_base: Some({
                        let mut base = VersionVector::new();
                        base.insert(NodeId::from_bytes([3; 16]), Hlc::new(77, 1));
                        base
                    }),
                    records_from: Some(kimmy_core::Stamp::new(
                        Hlc::new(70, 2),
                        NodeId::from_bytes([3; 16]),
                    )),
                }),
                collection: Some(CollectionId(u64::MAX)),
            },
            Message::Divergence {
                collections: vec![CollectionId(1), CollectionId(2)],
                probe_count: Some(7),
                // An incarnation rides beside its id, so the pair has to
                // survive BSON — the unencodable half of the id space
                // included, as the ids themselves already are.
                incarnations: vec![
                    (CollectionId(1), Hlc::new(3, 1)),
                    (CollectionId(u64::MAX), Hlc::new(4, 0)),
                ],
                digests: None,
                digest_busy: false,
                definitions: None,
            },
            Message::Divergence {
                collections: Vec::new(),
                probe_count: None,
                incarnations: Vec::new(),
                digests: None,
                digest_busy: false,
                definitions: None,
            },
            Message::AskWitnessed {},
            Message::Witnessed(populated_vector()),
            Message::Push {
                entries: Vec::new(),
                scanned_to: Hlc::new(11, 2),
                exhausted: false,
                versions: populated_vector(),
                records: Vec::new(),
            },
            Message::Pushed {
                applied: 1,
                ddl: 2,
                ddl_held: 6,
                ddl_refused: 0,
                unknown_collection: 0,
                ddl_declined: 3,
                deferred: 4,
                purge_pending: 5,
                refused: vec![kimmy_core::Stamp::new(Hlc::new(3, 1), NodeId::from_bytes([7; 16]))],
                stopped_at: Some(kimmy_core::Stamp::new(
                    Hlc::new(4, 0),
                    NodeId::from_bytes([8; 16]),
                )),
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
        write_frame(
            &mut buffer,
            &Message::AskVersions { witnessed: false, facts: None, facts_gen: None, echo: None },
        )
        .await
        .unwrap();
        write_frame(
            &mut buffer,
            &Message::AskEntries {
                from: Hlc::ZERO,
                limit: 5,
                held: None,
                marked: Vec::new(),
                partial: false,
            },
        )
        .await
        .unwrap();

        let mut stream = buffer.as_slice();
        assert_eq!(
            read_frame(&mut stream).await.unwrap(),
            Message::AskVersions { witnessed: false, facts: None, facts_gen: None, echo: None }
        );
        assert_eq!(
            read_frame(&mut stream).await.unwrap(),
            Message::AskEntries {
                from: Hlc::ZERO,
                limit: 5,
                held: None,
                marked: Vec::new(),
                partial: false
            }
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
            Message::AskEntries { from, limit: 10, held: None, marked: Vec::new(), partial: false },
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
            Message::AskEntries { from, limit: 10, held: None, marked: Vec::new(), partial: false },
            "a field this build does not know must not fail the frame"
        );
    }

    /// The same boundary for `AskEntries::partial` and `Entries::
    /// passed_through` (ADR-194), in both directions. A sender before
    /// `partial` reads a request that sets it as the request it knew, and
    /// serves a whole window; a requester before `passed_through` reads a
    /// partial window's frame as the window it knew. A frame from a build
    /// before either reads as `false` and `None`.
    #[tokio::test]
    async fn the_partial_window_fields_cross_a_version_boundary_in_both_directions() {
        /// `AskEntries` as a sender before ADR-194 declares it.
        #[derive(Debug, Deserialize)]
        struct AskEntriesBeforePartial {
            from: Hlc,
            limit: usize,
            #[serde(default)]
            held: Option<VersionVector>,
            #[serde(default)]
            marked: Vec<kimmy_storage::MarkedRange>,
        }
        /// `Entries` as a requester before ADR-194 declares it.
        #[derive(Debug, Deserialize)]
        struct EntriesBeforePassedThrough {
            entries: Vec<OplogEntry>,
            scanned_to: Hlc,
            exhausted: bool,
        }
        async fn body_of(message: &Message, variant: &str) -> bson::Document {
            let mut written = Vec::new();
            write_frame(&mut written, message).await.unwrap();
            let body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
            body.get_document(variant).unwrap().clone()
        }
        async fn read_back(variant: &str, fields: bson::Document) -> Message {
            let bytes = bson::serialize_to_vec(&bson::doc! { variant: fields }).unwrap();
            let mut frame = (bytes.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(&bytes);
            read_frame(&mut frame.as_slice()).await.unwrap()
        }

        let held = populated_vector();
        let ask = Message::AskEntries {
            from: Hlc::new(7, 1),
            limit: 10,
            held: Some(held.clone()),
            marked: Vec::new(),
            partial: true,
        };
        let sent = body_of(&ask, "AskEntries").await;
        assert_eq!(
            sent.get_bool("partial").ok(),
            Some(true),
            "the frame carries the field: {sent}"
        );
        let older: AskEntriesBeforePartial = bson::deserialize_from_document(sent.clone())
            .expect("a sender that predates the field reads the request");
        assert_eq!((older.from, older.limit), (Hlc::new(7, 1), 10));
        assert_eq!(older.held, Some(held.clone()));
        assert!(older.marked.is_empty());
        let mut before = sent;
        before.remove("partial");
        assert_eq!(
            read_back("AskEntries", before).await,
            Message::AskEntries {
                from: Hlc::new(7, 1),
                limit: 10,
                held: Some(held),
                marked: Vec::new(),
                partial: false,
            },
            "a request from before the field asks for a whole window"
        );

        let p = kimmy_core::Stamp::new(Hlc::new(11, 2), NodeId::generate());
        let entries = Message::Entries {
            entries: Vec::new(),
            scanned_to: p.hlc,
            exhausted: false,
            passed_through: Some(p),
            records: Vec::new(),
        };
        let sent = body_of(&entries, "Entries").await;
        assert!(sent.contains_key("passed_through"), "the frame carries the field: {sent}");
        let older: EntriesBeforePassedThrough = bson::deserialize_from_document(sent.clone())
            .expect("a requester that predates the field reads the window");
        assert!(older.entries.is_empty());
        assert_eq!((older.scanned_to, older.exhausted), (p.hlc, false));
        let mut before = sent;
        before.remove("passed_through");
        assert_eq!(
            read_back("Entries", before).await,
            Message::Entries {
                entries: Vec::new(),
                scanned_to: p.hlc,
                exhausted: false,
                passed_through: None,
                records: Vec::new(),
            },
            "a window from before the field names no end of its own"
        );
    }

    /// C7 of the B5 design: the idempotency records cross a version boundary in
    /// both directions on every frame that carries them (`Entries`,
    /// `ReplayEntries`, `Push`, a snapshot page, and the records position of a
    /// snapshot cursor). A receiver before the field reads a frame carrying
    /// records as the frame it knew; a frame from a sender before the field
    /// reads with no records; a record element this build cannot read is
    /// dropped and the frame stands. The facts field is the gate's half, and
    /// is tested with the facts codec (`codec_tests.rs`,
    /// `this_build_reads_idempotency_records_leniently_and_an_older_block_as_not_carrying`
    /// and `a_051_decoder_reads_a_block_carrying_idempotency_records`).
    #[tokio::test]
    async fn the_idempotency_records_cross_a_version_boundary_in_both_directions() {
        /// `Entries` as a requester before B5 declares it.
        #[derive(Debug, Deserialize)]
        struct EntriesBeforeRecords {
            entries: Vec<OplogEntry>,
            scanned_to: Hlc,
            exhausted: bool,
            #[serde(default)]
            passed_through: Option<kimmy_core::Stamp>,
        }
        /// `ReplayEntries` as a receiver before B5 declares it.
        #[derive(Debug, Deserialize)]
        struct ReplayBeforeRecords {
            from: Hlc,
            entries: Vec<OplogEntry>,
            exhausted: bool,
            #[serde(default)]
            horizon: bool,
            scanned_to: Hlc,
        }
        /// `Push` as a receiver before B5 declares it.
        #[derive(Debug, Deserialize)]
        struct PushBeforeRecords {
            entries: Vec<OplogEntry>,
            scanned_to: Hlc,
            exhausted: bool,
            versions: VersionVector,
        }
        /// `SnapshotCursor` as a receiver before B5 declares it.
        #[derive(Debug, Deserialize, Serialize)]
        struct CursorBeforeRecords {
            collection: CollectionId,
            after_key: Vec<u8>,
        }
        /// `SnapshotPage`, as far as a receiver before B5 reads it.
        #[derive(Debug, Deserialize)]
        struct PageBeforeRecords {
            documents: Vec<kimmy_storage::SnapshotDoc>,
            next: Option<CursorBeforeRecords>,
        }
        async fn body_of(message: &Message, variant: &str) -> bson::Document {
            let mut written = Vec::new();
            write_frame(&mut written, message).await.unwrap();
            let body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
            body.get_document(variant).unwrap().clone()
        }
        async fn read_back(variant: &str, fields: bson::Document) -> Message {
            let bytes = bson::serialize_to_vec(&bson::doc! { variant: fields }).unwrap();
            let mut frame = (bytes.len() as u32).to_be_bytes().to_vec();
            frame.extend_from_slice(&bytes);
            read_frame(&mut frame.as_slice()).await.unwrap()
        }

        let node = NodeId::from_bytes([5; 16]);
        let at = kimmy_core::Stamp::new(Hlc::new(11, 2), node);
        // A collection id above `i64::MAX`, as half of all derived ids are.
        let high = CollectionId(u64::MAX - 7);
        let entry = OplogEntry {
            stamp: at,
            kind: kimmy_core::OpKind::Insert,
            collection: high,
            doc_id: Some(kimmy_core::DocId::Int64(1)),
            body: Some(bson::serialize_to_vec(&bson::doc! { "_id": 1i64 }).unwrap()),
        };
        let record = kimmy_storage::CarriedRecord {
            collection: high,
            scope: kimmy_core::ScopeHash([9; 32]),
            carrying: Some(at),
            record: vec![1, 2, 3, 4],
        };
        let records = vec![record.clone()];
        // A record element a later build might send and this one cannot read.
        let unreadable = bson::Bson::String("a record from the future".into());

        // Entries.
        let message = Message::Entries {
            entries: vec![entry.clone()],
            scanned_to: at.hlc,
            exhausted: true,
            passed_through: None,
            records: records.clone(),
        };
        let sent = body_of(&message, "Entries").await;
        assert!(sent.get_array("records").is_ok_and(|r| r.len() == 1), "carried: {sent}");
        let older: EntriesBeforeRecords = bson::deserialize_from_document(sent.clone())
            .expect("a requester that predates the field reads the window");
        assert_eq!(
            (older.entries, older.scanned_to, older.exhausted),
            (vec![entry.clone()], at.hlc, true)
        );
        assert_eq!(older.passed_through, None);
        assert_eq!(read_back("Entries", sent.clone()).await, message, "this build reads it whole");
        let mut lenient = sent.clone();
        lenient.get_array_mut("records").unwrap().push(unreadable.clone());
        assert_eq!(
            read_back("Entries", lenient).await,
            message,
            "an unreadable element is dropped"
        );
        let mut before = sent;
        before.remove("records");
        let Message::Entries { records: none, entries, .. } = read_back("Entries", before).await
        else {
            panic!("not a window")
        };
        assert!(none.is_empty() && entries.len() == 1, "a window from before carries none");

        // ReplayEntries.
        let message = Message::ReplayEntries {
            from: Hlc::new(3, 0),
            entries: vec![entry.clone()],
            exhausted: false,
            horizon: false,
            scanned_to: at.hlc,
            records: records.clone(),
        };
        let sent = body_of(&message, "ReplayEntries").await;
        assert!(sent.contains_key("records"), "carried: {sent}");
        let older: ReplayBeforeRecords = bson::deserialize_from_document(sent.clone())
            .expect("a receiver that predates the field reads the page");
        assert_eq!((older.from, older.entries.len(), older.exhausted), (Hlc::new(3, 0), 1, false));
        assert_eq!((older.horizon, older.scanned_to), (false, at.hlc));
        assert_eq!(read_back("ReplayEntries", sent.clone()).await, message);
        let mut lenient = sent.clone();
        lenient.insert("records", bson::Bson::Int32(7));
        let Message::ReplayEntries { records: none, .. } =
            read_back("ReplayEntries", lenient).await
        else {
            panic!("not a replay page")
        };
        assert!(none.is_empty(), "a field this build cannot read is no records");
        let mut before = sent;
        before.remove("records");
        let Message::ReplayEntries { records: none, .. } = read_back("ReplayEntries", before).await
        else {
            panic!("not a replay page")
        };
        assert!(none.is_empty());

        // Push.
        let mut versions = VersionVector::new();
        versions.insert(node, at.hlc);
        let message = Message::Push {
            entries: vec![entry.clone()],
            scanned_to: at.hlc,
            exhausted: true,
            versions: versions.clone(),
            records: records.clone(),
        };
        let sent = body_of(&message, "Push").await;
        assert!(sent.contains_key("records"), "carried: {sent}");
        let older: PushBeforeRecords = bson::deserialize_from_document(sent.clone())
            .expect("a receiver that predates the field reads the push");
        assert_eq!((older.entries.len(), older.scanned_to, older.exhausted), (1, at.hlc, true));
        assert_eq!(older.versions, versions);
        assert_eq!(read_back("Push", sent.clone()).await, message);
        let mut before = sent;
        before.remove("records");
        let Message::Push { records: none, .. } = read_back("Push", before).await else {
            panic!("not a push")
        };
        assert!(none.is_empty());

        // A snapshot page with records, and a cursor in a records phase.
        let page = SnapshotPage {
            collections: Vec::new(),
            documents: Vec::new(),
            next: Some(SnapshotCursor {
                collection: high,
                after_key: Vec::new(),
                records: Some(vec![9; 32]),
                // And the records delta's base, with where a records-only page
                // stopped.
                records_base: Some(versions.clone()),
                records_from: Some(at),
            }),
            versions: versions.clone(),
            dropped: None,
            dropped_collections: Vec::new(),
            deleted_documents: Vec::new(),
            dropped_indexes: Vec::new(),
            tombstone_horizon: None,
            records: vec![kimmy_storage::CarriedRecord { carrying: None, ..record }],
        };
        let message = Message::Snapshot(Box::new(page.clone()));
        let sent = body_of(&message, "Snapshot").await;
        assert!(sent.contains_key("records"), "carried: {sent}");
        let older: PageBeforeRecords = bson::deserialize_from_document(sent.clone())
            .expect("a receiver that predates the field reads the page");
        assert!(older.documents.is_empty());
        let echoed = older.next.expect("the cursor");
        assert_eq!((echoed.collection, echoed.after_key.clone()), (high, Vec::new()));
        let cut = sent.get_document("next").unwrap();
        assert!(
            cut.contains_key("records_base") && cut.contains_key("records_from"),
            "the delta's base rides the cursor: {cut}"
        );
        assert_eq!(read_back("Snapshot", sent.clone()).await, message, "this build reads it whole");
        let mut before = sent;
        before.remove("records");
        let Message::Snapshot(read) = read_back("Snapshot", before).await else {
            panic!("not a page")
        };
        assert!(read.records.is_empty(), "a page from before carries none");
        // The older receiver echoes `{collection, after_key}`: read as the
        // documents phase of the collection, which is where its sender goes.
        let ask = bson::doc! {
            "after": bson::serialize_to_bson(&echoed).unwrap(),
            "collection": bson::Bson::Null,
        };
        let Message::AskSnapshot { after: Some(after), .. } = read_back("AskSnapshot", ask).await
        else {
            panic!("not a snapshot request")
        };
        assert_eq!(
            after,
            SnapshotCursor {
                collection: high,
                after_key: Vec::new(),
                records: None,
                records_base: None,
                records_from: None,
            },
            "an echo without the position asks for the documents, and without the base for no \
             records delta"
        );
        // A base of another shape is none: the cursor still reads.
        let ask = bson::doc! {
            "after": {
                "collection": bson::serialize_to_bson(&high).unwrap(),
                "after_key": bson::Bson::Array(Vec::new()),
                "records_base": "not a vector",
                "records_from": 7i32,
            },
            "collection": bson::Bson::Null,
        };
        let Message::AskSnapshot { after: Some(after), .. } = read_back("AskSnapshot", ask).await
        else {
            panic!("not a snapshot request")
        };
        assert_eq!((after.records_base, after.records_from), (None, None));
    }

    /// The same boundary for `AskEntries::marked` (ADR-172). A receiver
    /// before the field reads a request carrying spans as the request it
    /// knew; a requester before the field sends none, which reads as a
    /// request naming no spans and is served as ADR-171 serves it.
    #[tokio::test]
    async fn ask_entries_marked_crosses_a_version_boundary_in_both_directions() {
        /// `AskEntries` as a receiver before ADR-172 declares it.
        #[derive(Debug, Deserialize)]
        struct AskEntriesBeforeMarked {
            from: Hlc,
            limit: usize,
            #[serde(default)]
            held: Option<VersionVector>,
        }

        let held = populated_vector();
        let request = Message::AskEntries {
            from: Hlc::new(7, 1),
            limit: 10,
            held: Some(held.clone()),
            marked: vec![kimmy_storage::MarkedRange {
                origin: NodeId::generate(),
                from: Hlc::new(3, 0),
                through: Hlc::new(5, 2),
            }],
            partial: false,
        };
        let mut written = Vec::new();
        write_frame(&mut written, &request).await.unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
        let sent = body.get_document("AskEntries").unwrap().clone();
        assert!(
            sent.contains_key("marked"),
            "the frame must carry the field for this to test anything: {sent}"
        );

        let older: AskEntriesBeforeMarked = bson::deserialize_from_document(sent.clone())
            .expect("a receiver that predates the field must still read the request");
        assert_eq!(older.from, Hlc::new(7, 1));
        assert_eq!(older.limit, 10);
        assert_eq!(older.held, Some(held.clone()));

        let mut before = sent;
        before.remove("marked");
        let frame = {
            let bytes = bson::serialize_to_vec(&bson::doc! { "AskEntries": before }).unwrap();
            let mut buffer = (bytes.len() as u32).to_be_bytes().to_vec();
            buffer.extend_from_slice(&bytes);
            buffer
        };
        assert_eq!(
            read_frame(&mut frame.as_slice()).await.unwrap(),
            Message::AskEntries {
                from: Hlc::new(7, 1),
                limit: 10,
                held: Some(held),
                marked: Vec::new(),
                partial: false,
            },
            "a request without the field must read as one naming no spans"
        );
    }

    /// `Pushed::purge_pending` (ADR-189) is a backward-compatible addition: a
    /// receiver before the field sends a `Pushed` without it, which reads as
    /// no stop at a pending creation, which is what that receiver never made.
    #[tokio::test]
    async fn pushed_without_purge_pending_reads_as_none() {
        let mut written = Vec::new();
        let current = Message::Pushed {
            applied: 1,
            ddl: 1,
            ddl_held: 0,
            ddl_refused: 0,
            unknown_collection: 0,
            ddl_declined: 0,
            deferred: 0,
            purge_pending: 1,
            refused: Vec::new(),
            stopped_at: None,
        };
        write_frame(&mut written, &current).await.unwrap();
        let mut body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
        let fields = body.get_document_mut("Pushed").expect("a struct variant");
        assert!(fields.remove("purge_pending").is_some(), "the field is on the wire: {fields:?}");

        let bytes = bson::serialize_to_vec(&body).unwrap();
        let mut old = (bytes.len() as u32).to_be_bytes().to_vec();
        old.extend_from_slice(&bytes);
        assert_eq!(
            read_frame(&mut old.as_slice()).await.unwrap(),
            Message::Pushed {
                applied: 1,
                ddl: 1,
                ddl_held: 0,
                ddl_refused: 0,
                unknown_collection: 0,
                ddl_declined: 0,
                deferred: 0,
                purge_pending: 0,
                refused: Vec::new(),
                stopped_at: None,
            },
            "an older receiver's reply must read as no stop"
        );
    }

    /// `Pushed::ddl_held` is a backward-compatible addition: a receiver
    /// before the field sends a `Pushed` without it, which reads as none
    /// held -- that receiver counted them in `ddl`.
    #[tokio::test]
    async fn pushed_without_ddl_held_reads_as_none_held() {
        let pushed = |ddl_held| Message::Pushed {
            applied: 0,
            ddl: 1,
            ddl_held,
            ddl_refused: 0,
            unknown_collection: 0,
            ddl_declined: 0,
            deferred: 0,
            purge_pending: 0,
            refused: Vec::new(),
            stopped_at: None,
        };
        let mut written = Vec::new();
        write_frame(&mut written, &pushed(2)).await.unwrap();
        let mut body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
        let fields = body.get_document_mut("Pushed").expect("a struct variant");
        assert!(fields.remove("ddl_held").is_some(), "the field is on the wire: {fields:?}");

        let bytes = bson::serialize_to_vec(&body).unwrap();
        let mut old = (bytes.len() as u32).to_be_bytes().to_vec();
        old.extend_from_slice(&bytes);
        assert_eq!(
            read_frame(&mut old.as_slice()).await.unwrap(),
            pushed(0),
            "an older receiver's reply must read as none held"
        );
    }

    /// `Pushed::refused` and `Pushed::stopped_at` (ADR-191) are
    /// backward-compatible additions: a receiver before them sends a
    /// `Pushed` without either, which reads as none named. The pusher tells
    /// that apart from "none happened" by the counts, not by the fields.
    #[tokio::test]
    async fn pushed_without_the_per_change_fields_reads_as_none_named() {
        let stamp = kimmy_core::Stamp::new(Hlc::new(9, 0), NodeId::from_bytes([3; 16]));
        let mut written = Vec::new();
        let current = Message::Pushed {
            applied: 0,
            ddl: 1,
            ddl_held: 0,
            ddl_refused: 1,
            unknown_collection: 1,
            ddl_declined: 0,
            deferred: 0,
            purge_pending: 0,
            refused: vec![stamp],
            stopped_at: Some(stamp),
        };
        write_frame(&mut written, &current).await.unwrap();
        let mut body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
        let fields = body.get_document_mut("Pushed").expect("a struct variant");
        assert!(fields.remove("refused").is_some(), "the field is on the wire: {fields:?}");
        assert!(fields.remove("stopped_at").is_some(), "the field is on the wire: {fields:?}");

        let bytes = bson::serialize_to_vec(&body).unwrap();
        let mut old = (bytes.len() as u32).to_be_bytes().to_vec();
        old.extend_from_slice(&bytes);
        assert_eq!(
            read_frame(&mut old.as_slice()).await.unwrap(),
            Message::Pushed {
                applied: 0,
                ddl: 1,
                ddl_held: 0,
                ddl_refused: 1,
                unknown_collection: 1,
                ddl_declined: 0,
                deferred: 0,
                purge_pending: 0,
                refused: Vec::new(),
                stopped_at: None,
            },
            "an older receiver's reply must read as naming nothing"
        );
    }

    /// The same boundary for `AskVersions::witnessed` (ADR-146), which is
    /// what keeps a sync round from failing against a peer on either side
    /// of the upgrade: a requester before the field sends an empty
    /// document, which reads as not asking; a receiver before the field
    /// sees a field it does not know on an empty struct variant, which it
    /// must ignore rather than refuse.
    #[tokio::test]
    async fn ask_versions_crosses_a_version_boundary_in_both_directions() {
        let frame = |body: bson::Document| {
            let mut buffer = Vec::new();
            let bytes = bson::serialize_to_vec(&body).unwrap();
            buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&bytes);
            buffer
        };

        let old_request = frame(bson::doc! { "AskVersions": {} });
        assert_eq!(
            read_frame(&mut old_request.as_slice()).await.unwrap(),
            Message::AskVersions { witnessed: false, facts: None, facts_gen: None, echo: None },
            "a request without the field must read as one that did not ask"
        );

        let future = frame(bson::doc! { "AskVersions": { "somethingNewer": true } });
        assert_eq!(
            read_frame(&mut future.as_slice()).await.unwrap(),
            Message::AskVersions { witnessed: false, facts: None, facts_gen: None, echo: None },
            "a field this build does not know must not fail the frame"
        );

        // And the frame this build writes when it asks is exactly the shape
        // an old receiver is handed above: an unknown field on `AskVersions`.
        let mut asked = Vec::new();
        write_frame(
            &mut asked,
            &Message::AskVersions { witnessed: true, facts: None, facts_gen: None, echo: None },
        )
        .await
        .unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&asked[4..]).unwrap();
        assert_eq!(body, bson::doc! { "AskVersions": { "witnessed": true } });
    }

    /// Another task decoding an undecodable block on the same thread while this
    /// one waits for its bytes must not make this frame look undecodable: the
    /// flag belongs to the frame that was decoded, with no await between clearing
    /// and taking it.
    #[tokio::test]
    async fn a_frame_is_not_blamed_for_a_block_another_task_could_not_decode() {
        use tokio::io::AsyncWriteExt;
        let frame = |body: bson::Document| {
            let mut buffer = Vec::new();
            let bytes = bson::serialize_to_vec(&body).unwrap();
            buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&bytes);
            buffer
        };
        let good = frame(bson::doc! {
            "Vectors": { "servable": {}, "witnessed": {}, "facts": { "catching_up": true } }
        });
        let bad = frame(bson::doc! {
            "Vectors": { "servable": {}, "witnessed": {}, "facts": { "ttl": "not a list" } }
        });

        let (mut waiting_end, mut waiting_writer) = tokio::io::duplex(4096);
        // The first task reads and has nothing yet: it is parked on its bytes.
        let waiting = tokio::spawn(async move {
            read_frame_noting_facts(&mut waiting_end).await.map(|(_, flagged, _)| flagged)
        });
        tokio::task::yield_now().await;

        // Meanwhile a second connection, on this thread, decodes a bad block.
        let (mut other_end, mut other_writer) = tokio::io::duplex(4096);
        other_writer.write_all(&bad).await.unwrap();
        // A read site that does not look at the flag, as most do.
        let other = read_frame(&mut other_end).await.unwrap();
        assert!(matches!(other, Message::Vectors { facts: None, .. }), "the block was dropped");

        // Now the first one's bytes arrive, and they are good.
        waiting_writer.write_all(&good).await.unwrap();
        assert!(!waiting.await.unwrap().unwrap(), "and not on the frame that was waiting");
    }

    /// The boundary for `facts` (ADR-201), in both directions and on both
    /// messages. An older build reads a message carrying the key and drops it;
    /// this build reads one without the key as unknown; a block with fewer fields
    /// than this build's decodes; and a member with no block writes no key at all,
    /// so its frame is byte for byte the frame an older build wrote.
    #[tokio::test]
    async fn facts_cross_a_version_boundary_in_both_directions() {
        use serde::Deserialize;
        // What the message was before the field, as an older build reads it.
        #[derive(Debug, PartialEq, Deserialize)]
        enum Before {
            AskVersions {
                #[serde(default)]
                witnessed: bool,
            },
            Vectors {
                servable: VersionVector,
                witnessed: VersionVector,
            },
        }
        let frame = |body: bson::Document| {
            let mut buffer = Vec::new();
            let bytes = bson::serialize_to_vec(&body).unwrap();
            buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&bytes);
            buffer
        };
        let facts =
            crate::facts::Facts { boot: vec![9; 16], catching_up: true, ..Default::default() }
                .with_ttl(vec![crate::facts::TtlHeld {
                    collection: kimmy_core::CollectionId(u64::MAX - 1),
                    digest: vec![0xFF; 8],
                }]);

        // New writes, old reads.
        for message in [
            Message::AskVersions {
                witnessed: true,
                facts: Some(std::sync::Arc::new(facts.clone())),
                facts_gen: None,
                echo: None,
            },
            Message::Vectors {
                servable: populated_vector(),
                witnessed: populated_vector(),
                facts: Some(std::sync::Arc::new(facts.clone())),
                facts_gen: None,
                echo: None,
                replay_from: None,
            },
        ] {
            let mut written = Vec::new();
            write_frame(&mut written, &message).await.unwrap();
            let old: Before = bson::deserialize_from_slice(&written[4..]).unwrap();
            match (message, old) {
                (Message::AskVersions { .. }, Before::AskVersions { witnessed }) => {
                    assert!(witnessed);
                }
                (Message::Vectors { servable, .. }, Before::Vectors { servable: seen, .. }) => {
                    assert_eq!(seen, servable);
                }
                other => panic!("the same variant either way: {other:?}"),
            }
        }

        // Old writes, new reads: no key means no block.
        let old_vectors = frame(bson::doc! { "Vectors": { "servable": {}, "witnessed": {} } });
        match read_frame(&mut old_vectors.as_slice()).await.unwrap() {
            Message::Vectors { facts, .. } => assert_eq!(facts, None),
            other => panic!("{other:?}"),
        }
        let old_ask = frame(bson::doc! { "AskVersions": { "witnessed": true } });
        assert_eq!(
            read_frame(&mut old_ask.as_slice()).await.unwrap(),
            Message::AskVersions { witnessed: true, facts: None, facts_gen: None, echo: None }
        );

        // A block from a build with fewer fields, on the wire in a reply.
        let fewer = frame(bson::doc! {
            "Vectors": { "servable": {}, "witnessed": {}, "facts": { "catching_up": true } }
        });
        match read_frame(&mut fewer.as_slice()).await.unwrap() {
            Message::Vectors { facts: Some(facts), .. } => {
                assert!(facts.catching_up && facts.ttl.is_empty());
            }
            other => panic!("{other:?}"),
        }

        // A block that does not decode is no block, never a failed exchange: a
        // wrong type for a field, and a partial `yielding` (the missing flags
        // read as false).
        let wrong = frame(bson::doc! {
            "Vectors": { "servable": {}, "witnessed": {}, "facts": { "ttl": "not a list" } }
        });
        match read_frame(&mut wrong.as_slice()).await.unwrap() {
            Message::Vectors { facts, servable, .. } => {
                assert_eq!(facts, None);
                assert_eq!(servable, VersionVector::new(), "the rest of the message stands");
            }
            other => panic!("{other:?}"),
        }
        let partial = frame(bson::doc! {
            "AskVersions": { "witnessed": true, "facts": { "yielding": { "ttl": true } } }
        });
        match read_frame(&mut partial.as_slice()).await.unwrap() {
            Message::AskVersions { facts: Some(facts), .. } => {
                assert!(
                    facts.yielding.ttl && !facts.yielding.webhooks && !facts.yielding.embeddings
                );
            }
            other => panic!("{other:?}"),
        }

        // This build's own frames round trip, block included, and a member with
        // no block writes none.
        let mut wire = Vec::new();
        let full = Message::Vectors {
            servable: populated_vector(),
            witnessed: populated_vector(),
            facts: Some(std::sync::Arc::new(facts)),
            facts_gen: None,
            echo: None,
            replay_from: None,
        };
        write_frame(&mut wire, &full).await.unwrap();
        assert_eq!(read_frame(&mut wire.as_slice()).await.unwrap(), full);
        let mut bare = Vec::new();
        write_frame(
            &mut bare,
            &Message::Vectors {
                servable: populated_vector(),
                witnessed: populated_vector(),
                facts: None,
                facts_gen: None,
                echo: None,
                replay_from: None,
            },
        )
        .await
        .unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&bare[4..]).unwrap();
        assert!(!body.get_document("Vectors").unwrap().contains_key("facts"));
        let mut ask = Vec::new();
        write_frame(
            &mut ask,
            &Message::AskVersions { witnessed: true, facts: None, facts_gen: None, echo: None },
        )
        .await
        .unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&ask[4..]).unwrap();
        assert!(!body.get_document("AskVersions").unwrap().contains_key("facts"));
    }

    /// The same boundary for `AskSnapshot::collection` (ADR-152). A
    /// requester before the field sends `after` alone, which reads as a
    /// whole-database snapshot; a sender before the field sees a field it
    /// does not know and must ignore it, serving the whole database — the
    /// fallback the ADR names for the minutes a rolling upgrade has mixed
    /// versions — rather than fail the frame.
    #[tokio::test]
    async fn ask_snapshot_crosses_a_version_boundary_in_both_directions() {
        let frame = |body: bson::Document| {
            let mut buffer = Vec::new();
            let bytes = bson::serialize_to_vec(&body).unwrap();
            buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&bytes);
            buffer
        };

        let old_request = frame(bson::doc! { "AskSnapshot": { "after": bson::Bson::Null } });
        assert_eq!(
            read_frame(&mut old_request.as_slice()).await.unwrap(),
            Message::AskSnapshot { after: None, collection: None },
            "a request without the field is a whole-database snapshot"
        );

        let future = frame(
            bson::doc! { "AskSnapshot": { "after": bson::Bson::Null, "somethingNewer": true } },
        );
        assert_eq!(
            read_frame(&mut future.as_slice()).await.unwrap(),
            Message::AskSnapshot { after: None, collection: None },
            "a field this build does not know must not fail the frame"
        );

        // What this build writes for a scoped request is exactly that shape
        // to an old sender: `after` it knows, and one field it does not.
        let mut asked = Vec::new();
        let scoped = Message::AskSnapshot { after: None, collection: Some(CollectionId(7)) };
        write_frame(&mut asked, &scoped).await.unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&asked[4..]).unwrap();
        assert_eq!(
            body,
            bson::doc! { "AskSnapshot": { "after": bson::Bson::Null, "collection": 7i64 } }
        );
    }

    /// The older-receiver half for `SnapshotPage`'s later fields, beside the
    /// two boundaries above and below. A page carrying `dropped_collections`
    /// (ADR-162) and `deleted_documents` (ADR-167) must decode through the page
    /// as a receiver predating both declares it, ignoring what it cannot read;
    /// and a page carrying a field newer than this build must still decode
    /// here. The older-sender half is `kimmy-storage`'s
    /// `a_page_that_predates_the_field_denies_nothing_and_breaks_nothing`.
    /// ADR-231 (extended): the definition of a re-dated life names the creation it
    /// was re-dated from. A receiver that predates the field reads the page and
    /// ignores it; a page from a sender that predates it reads with none.
    #[tokio::test]
    async fn a_definition_names_the_creation_it_was_re_dated_from_across_a_version_boundary() {
        /// `CollectionState` as a receiver before the field declares it.
        #[derive(Debug, Deserialize)]
        struct StateBeforeRedated {
            db: String,
            name: String,
            created: Option<Hlc>,
        }

        fn carried_has_the_floor(state: &kimmy_storage::CollectionState) -> bool {
            bson::serialize_to_document(state).unwrap().contains_key("incarnation_floor")
        }

        let state = kimmy_storage::CollectionState {
            db: "shop".into(),
            name: "orders".into(),
            indexes: Vec::new(),
            vector: None,
            created: Some(Hlc::new(900, 0)),
            vector_changed: None,
            redated_from: Some(Hlc::new(100, 0)),
            incarnation_floor: Some(Hlc::new(200, 0)),
        };
        let mut written = Vec::new();
        write_frame(
            &mut written,
            &Message::Snapshot(Box::new(SnapshotPage {
                collections: vec![state.clone()],
                documents: Vec::new(),
                next: None,
                versions: VersionVector::new(),
                dropped: None,
                dropped_collections: Vec::new(),
                deleted_documents: Vec::new(),
                dropped_indexes: Vec::new(),
                tombstone_horizon: None,
                records: Vec::new(),
            })),
        )
        .await
        .unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
        let sent = body.get_document("Snapshot").unwrap().clone();
        let carried = sent.get_array("collections").unwrap()[0].as_document().unwrap().clone();
        assert!(carried.contains_key("redated_from"), "the frame must carry the field: {carried}");
        let older: StateBeforeRedated = bson::deserialize_from_document(carried.clone())
            .expect("a receiver that predates the field must still read the definition");
        assert_eq!((older.db.as_str(), older.name.as_str()), ("shop", "orders"));
        assert_eq!(older.created, Some(Hlc::new(900, 0)));

        // The other direction: a sender that predates it writes none, and it reads as none.
        let mut before = carried;
        before.remove("redated_from");
        let read: kimmy_storage::CollectionState = bson::deserialize_from_document(before).unwrap();
        assert_eq!(read.redated_from, None);
        // And an ordinary definition does not carry it at all.
        let plain = kimmy_storage::CollectionState {
            redated_from: None,
            incarnation_floor: None,
            ..state.clone()
        };
        let doc = bson::serialize_to_document(&plain).unwrap();
        assert!(!doc.contains_key("redated_from"), "absent when unset: {doc}");
        assert!(!doc.contains_key("incarnation_floor"), "absent when unset: {doc}");

        // The sender's incarnation floor (ADR-235): carried, read by an older receiver
        // without trouble (it declares no such field), and none from a sender that
        // predates it.
        assert!(carried_has_the_floor(&state), "the frame must carry the floor");
        let mut without = bson::serialize_to_document(&state).unwrap();
        without.remove("incarnation_floor");
        let read: kimmy_storage::CollectionState =
            bson::deserialize_from_document(without).unwrap();
        assert_eq!(read.incarnation_floor, None);
    }

    #[tokio::test]
    async fn snapshot_page_crosses_a_version_boundary_to_an_older_receiver() {
        /// `SnapshotPage` as a receiver before ADR-162 and ADR-167 declares it.
        #[derive(Debug, Deserialize)]
        struct PageBeforeDropsAndDeletes {
            collections: Vec<kimmy_storage::CollectionState>,
            documents: Vec<kimmy_storage::SnapshotDoc>,
            next: Option<SnapshotCursor>,
            versions: VersionVector,
            #[serde(default)]
            dropped: Option<kimmy_core::Stamp>,
        }

        let node = NodeId::from_bytes([3; 16]);
        let stamp = |ms| kimmy_core::Stamp::new(Hlc::new(ms, 0), node);
        let mut versions = VersionVector::new();
        versions.insert(node, Hlc::new(900, 0));
        let page = SnapshotPage {
            collections: Vec::new(),
            documents: Vec::new(),
            next: Some(SnapshotCursor {
                collection: CollectionId(7),
                after_key: vec![1, 2, 3],
                records: None,
                records_base: None,
                records_from: None,
            }),
            versions: versions.clone(),
            dropped: None,
            dropped_collections: vec![(CollectionId(9), stamp(500))],
            deleted_documents: vec![kimmy_storage::SnapshotTombstone {
                collection: CollectionId(7),
                key: vec![4, 5],
                stamp: stamp(600),
            }],
            dropped_indexes: vec![(CollectionId(7), 2_032_551_883, stamp(550))],
            tombstone_horizon: Some(Hlc::new(777, 3)),
            records: Vec::new(),
        };

        let mut written = Vec::new();
        write_frame(&mut written, &Message::Snapshot(Box::new(page.clone()))).await.unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
        let sent = body.get_document("Snapshot").unwrap().clone();
        assert!(
            sent.contains_key("dropped_collections")
                && sent.contains_key("deleted_documents")
                && sent.contains_key("dropped_indexes"),
            "the frame must carry both fields for this to test anything: {sent}"
        );

        let older: PageBeforeDropsAndDeletes = bson::deserialize_from_document(sent.clone())
            .expect("a receiver that predates the fields must still read the page");
        assert!(older.collections.is_empty() && older.documents.is_empty());
        assert_eq!(older.next, page.next);
        assert_eq!(older.versions, versions);
        assert_eq!(older.dropped, None);

        // The other direction (ADR-225): a sender that predates `dropped_indexes` writes
        // none, and the page reads with an empty list and everything else as sent.
        let mut before_indexes = sent.clone();
        before_indexes.remove("dropped_indexes");
        let frame = {
            let bytes = bson::serialize_to_vec(&bson::doc! { "Snapshot": before_indexes }).unwrap();
            let mut buffer = (bytes.len() as u32).to_be_bytes().to_vec();
            buffer.extend_from_slice(&bytes);
            buffer
        };
        let mut without = page.clone();
        without.dropped_indexes = Vec::new();
        assert_eq!(
            read_frame(&mut frame.as_slice()).await.unwrap(),
            Message::Snapshot(Box::new(without)),
            "a sender that predates the index drops writes none, and the page still reads"
        );

        // The collected-through horizon (repair design 13.1): carried when set, and a sender that
        // predates it writes none, which reads as none and never as a zero horizon.
        assert!(sent.contains_key("tombstone_horizon"), "the frame must carry the horizon: {sent}");
        let mut before_horizon = sent.clone();
        before_horizon.remove("tombstone_horizon");
        let frame = {
            let bytes = bson::serialize_to_vec(&bson::doc! { "Snapshot": before_horizon }).unwrap();
            let mut buffer = (bytes.len() as u32).to_be_bytes().to_vec();
            buffer.extend_from_slice(&bytes);
            buffer
        };
        let mut no_horizon = page.clone();
        no_horizon.tombstone_horizon = None;
        assert_eq!(
            read_frame(&mut frame.as_slice()).await.unwrap(),
            Message::Snapshot(Box::new(no_horizon)),
            "a sender that predates the horizon writes none, and the page reads with none"
        );

        let mut future = sent;
        future.insert("somethingNewer", true);
        let frame = {
            let bytes = bson::serialize_to_vec(&bson::doc! { "Snapshot": future }).unwrap();
            let mut buffer = (bytes.len() as u32).to_be_bytes().to_vec();
            buffer.extend_from_slice(&bytes);
            buffer
        };
        assert_eq!(
            read_frame(&mut frame.as_slice()).await.unwrap(),
            Message::Snapshot(Box::new(page)),
            "a field this build does not know must not fail the page"
        );
    }

    /// The same boundary for `Divergence::incarnations`. The answer is what
    /// crosses the version line here rather than the request: a requester
    /// that predates the field ignores it and compares names alone; an
    /// answering peer that predates it writes none, which a requester that
    /// knows the field reads as "the incarnation you dropped" for every id it
    /// holds a tombstone for (`kimmy_storage::compare_divergence`).
    /// `collections` is untouched either way, so nothing an older receiver
    /// already read changes shape.
    #[tokio::test]
    async fn divergence_crosses_a_version_boundary_in_both_directions() {
        let frame = |body: bson::Document| {
            let mut buffer = Vec::new();
            let bytes = bson::serialize_to_vec(&body).unwrap();
            buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&bytes);
            buffer
        };
        let answered = |collections| Message::Divergence {
            collections,
            probe_count: Some(7),
            incarnations: Vec::new(),
            digests: None,
            digest_busy: false,
            definitions: None,
        };

        let old_answer =
            frame(bson::doc! { "Divergence": { "collections": [1i64], "probe_count": 7i64 } });
        assert_eq!(
            read_frame(&mut old_answer.as_slice()).await.unwrap(),
            answered(vec![CollectionId(1)]),
            "an answer without the field must read as one that named no incarnation"
        );

        let future = frame(
            bson::doc! { "Divergence": { "collections": [1i64], "probe_count": 7i64, "somethingNewer": true } },
        );
        assert_eq!(
            read_frame(&mut future.as_slice()).await.unwrap(),
            answered(vec![CollectionId(1)]),
            "a field this build does not know must not fail the frame"
        );

        // And the frame this build answers with is exactly that shape to an
        // older requester: the two fields it knows, and one it ignores.
        let mut written = Vec::new();
        let held = Message::Divergence {
            collections: vec![CollectionId(1)],
            probe_count: Some(7),
            incarnations: vec![(CollectionId(1), Hlc::new(9, 2))],
            digests: None,
            digest_busy: false,
            definitions: None,
        };
        write_frame(&mut written, &held).await.unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
        assert_eq!(
            body,
            bson::doc! { "Divergence": {
                "collections": [1i64],
                "probe_count": 7i64,
                "incarnations": [[1i64, { "wall_ms": 9i64, "counter": 2i32 }]],
            } }
        );
    }

    /// B3: the cut digest crosses the version line in both directions, for the
    /// request and for the answer.
    #[tokio::test]
    async fn the_digest_fields_cross_a_version_boundary_in_both_directions() {
        let frame = |body: bson::Document| {
            let bytes = bson::serialize_to_vec(&body).unwrap();
            let mut buffer = Vec::new();
            buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&bytes);
            buffer
        };
        let ask = kimmy_storage::DigestAsk {
            cut: [(NodeId::generate(), Hlc::new(5, 1))].into_iter().collect(),
            uncovered: vec![kimmy_storage::Uncovered {
                collection: CollectionId(u64::MAX),
                keys: vec![serde_bytes::ByteBuf::from(vec![1, 2])],
            }],
            busy: Some(CollectionId(3)),
        };

        // A request from a build before the digest: no field, and the answering
        // build reads it as no digest asked.
        let old_request = frame(bson::doc! { "AskDivergence": { "probe": 7i64 } });
        assert_eq!(
            read_frame(&mut old_request.as_slice()).await.unwrap(),
            Message::AskDivergence {
                probe: Some(CollectionId(7)),
                digest: None,
                definitions: None
            }
        );

        // And the request this build sends with nothing to ask is exactly that
        // frame to an older responder: the digest field is not on the wire.
        let mut written = Vec::new();
        write_frame(
            &mut written,
            &Message::AskDivergence {
                probe: Some(CollectionId(7)),
                digest: None,
                definitions: None,
            },
        )
        .await
        .unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
        assert_eq!(body, bson::doc! { "AskDivergence": { "probe": 7i64 } });

        // A request that carries one round-trips, and an older responder, which
        // reads only `probe`, ignores the rest of it (derived struct variants
        // without `deny_unknown_fields`).
        let asked =
            Message::AskDivergence { probe: None, digest: Some(ask.clone()), definitions: None };
        let mut written = Vec::new();
        write_frame(&mut written, &asked).await.unwrap();
        assert_eq!(read_frame(&mut written.as_slice()).await.unwrap(), asked);
        #[derive(serde::Deserialize, Debug)]
        enum Old {
            AskDivergence { probe: Option<CollectionId> },
        }
        let old: Old = bson::deserialize_from_slice(&written[4..]).expect("an older reader");
        assert!(matches!(old, Old::AskDivergence { probe: None }), "{old:?}");

        // An answer from a build before the digest: no field, read as none.
        let old_answer =
            frame(bson::doc! { "Divergence": { "collections": [1i64], "probe_count": 7i64 } });
        let Message::Divergence { digests, digest_busy, .. } =
            read_frame(&mut old_answer.as_slice()).await.unwrap()
        else {
            panic!("a Divergence")
        };
        assert_eq!((digests, digest_busy), (None, false), "unanswered, not busy");

        // The busy marker is its own thing, not a missing field.
        let busy = frame(bson::doc! { "Divergence": {
        "collections": [1i64], "probe_count": 7i64, "digest_busy": true } });
        let Message::Divergence { digests, digest_busy, .. } =
            read_frame(&mut busy.as_slice()).await.unwrap()
        else {
            panic!("a Divergence")
        };
        assert_eq!((digests, digest_busy), (None, true));

        // An answer with digests round-trips.
        let reply = kimmy_storage::DigestReply {
            malformed: 0,
            entries: vec![kimmy_storage::DigestEntry {
                collection: CollectionId(u64::MAX),
                digest: serde_bytes::ByteBuf::from(vec![7; 16]),
                applied: 2,
                uncovered: vec![serde_bytes::ByteBuf::from(vec![9])],
                deferred: None,
            }],
        };
        let answered = Message::Divergence {
            collections: vec![CollectionId(1)],
            probe_count: Some(7),
            incarnations: Vec::new(),
            digests: Some(reply.clone()),
            digest_busy: false,
            definitions: None,
        };
        let mut written = Vec::new();
        write_frame(&mut written, &answered).await.unwrap();
        assert_eq!(read_frame(&mut written.as_slice()).await.unwrap(), answered);
        // Read by an older requester, the extra field is ignored.
        #[derive(serde::Deserialize, Debug)]
        enum OldAnswer {
            Divergence { collections: Vec<CollectionId>, probe_count: Option<u64> },
        }
        let old: OldAnswer = bson::deserialize_from_slice(&written[4..]).expect("an older reader");
        assert!(matches!(old, OldAnswer::Divergence { probe_count: Some(7), .. }), "{old:?}");
    }

    /// B3 half 2: the definition digest crosses the version line in both
    /// directions, for the request and for the answer. The answer's field is
    /// present iff the request carried one, so an answer to an older requester is
    /// byte for byte what it was, and an empty answer is never read as a build
    /// that predates the field.
    #[tokio::test]
    async fn the_definition_fields_cross_a_version_boundary_in_both_directions() {
        let frame = |body: bson::Document| {
            let bytes = bson::serialize_to_vec(&body).unwrap();
            let mut buffer = Vec::new();
            buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&bytes);
            buffer
        };
        // A request from a build before the field reads as none asked.
        let old_request = frame(bson::doc! { "AskDivergence": { "probe": 7i64 } });
        assert_eq!(
            read_frame(&mut old_request.as_slice()).await.unwrap(),
            Message::AskDivergence {
                probe: Some(CollectionId(7)),
                digest: None,
                definitions: None
            }
        );
        // A request that carries one round-trips, and an older responder, which
        // reads only `probe`, ignores it.
        let ask = kimmy_storage::DefinitionAsk::new(
            VersionVector::new(),
            [CollectionId(3), CollectionId(1)],
        );
        let asked = Message::AskDivergence {
            probe: Some(CollectionId(7)),
            digest: None,
            definitions: Some(ask.clone()),
        };
        let mut written = Vec::new();
        write_frame(&mut written, &asked).await.unwrap();
        assert_eq!(read_frame(&mut written.as_slice()).await.unwrap(), asked);
        #[derive(serde::Deserialize, Debug)]
        enum Old {
            AskDivergence { probe: Option<CollectionId> },
        }
        let old: Old = bson::deserialize_from_slice(&written[4..]).expect("an older reader");
        assert!(matches!(old, Old::AskDivergence { probe: Some(CollectionId(7)) }), "{old:?}");
        // And when it is not asked, the key is not on the wire at all.
        let mut written = Vec::new();
        write_frame(
            &mut written,
            &Message::AskDivergence { probe: None, digest: None, definitions: None },
        )
        .await
        .unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
        assert!(!body.get_document("AskDivergence").unwrap().contains_key("definitions"));

        // An answer with none, to a requester that asked for none: no key.
        let mut written = Vec::new();
        let plain = Message::Divergence {
            collections: vec![CollectionId(1)],
            probe_count: Some(7),
            incarnations: Vec::new(),
            digests: None,
            digest_busy: false,
            definitions: None,
        };
        write_frame(&mut written, &plain).await.unwrap();
        let body = bson::deserialize_from_slice::<bson::Document>(&written[4..]).unwrap();
        assert!(!body.get_document("Divergence").unwrap().contains_key("definitions"));
        // An answer from a build before the field: no key, read as none.
        let Message::Divergence { definitions, .. } =
            read_frame(&mut written.as_slice()).await.unwrap()
        else {
            panic!("a Divergence")
        };
        assert_eq!(definitions, None, "a peer that predates it");
        // An answer that is present and empty is present, not absent.
        let empty = Message::Divergence {
            collections: Vec::new(),
            probe_count: None,
            incarnations: Vec::new(),
            digests: None,
            digest_busy: false,
            definitions: Some(kimmy_storage::DefinitionReply::default()),
        };
        let mut written = Vec::new();
        write_frame(&mut written, &empty).await.unwrap();
        let Message::Divergence { definitions, .. } =
            read_frame(&mut written.as_slice()).await.unwrap()
        else {
            panic!("a Divergence")
        };
        assert_eq!(definitions, Some(kimmy_storage::DefinitionReply::default()));
        // An answer with entries round-trips, and an older requester ignores it.
        let reply = kimmy_storage::DefinitionReply {
            format: kimmy_storage::definition_digest::FORMAT,
            malformed: 0,
            entries: vec![kimmy_storage::DefinitionEntry {
                collection: CollectionId(u64::MAX),
                digest: serde_bytes::ByteBuf::from(vec![7; 16]),
                deferred: None,
                above: None,
                definitions: vec![kimmy_storage::WireDefinition {
                    kind: kimmy_storage::DefinitionKind::Index,
                    name: "x".into(),
                    hash: serde_bytes::ByteBuf::from(vec![1; 16]),
                }],
                refused: vec![kimmy_storage::WireRefusal {
                    hint: kimmy_storage::HintKind::DropIndex,
                    name: "y".into(),
                    hash: serde_bytes::ByteBuf::from(vec![2; 16]),
                }],
            }],
        };
        let answered = Message::Divergence {
            collections: vec![CollectionId(1)],
            probe_count: Some(7),
            incarnations: Vec::new(),
            digests: None,
            digest_busy: false,
            definitions: Some(reply),
        };
        let mut written = Vec::new();
        write_frame(&mut written, &answered).await.unwrap();
        assert_eq!(read_frame(&mut written.as_slice()).await.unwrap(), answered);
        #[derive(serde::Deserialize, Debug)]
        enum OldAnswer {
            Divergence { collections: Vec<CollectionId>, probe_count: Option<u64> },
        }
        let old: OldAnswer = bson::deserialize_from_slice(&written[4..]).expect("an older reader");
        assert!(matches!(old, OldAnswer::Divergence { probe_count: Some(7), .. }), "{old:?}");
    }

    /// The answer's format number crosses the wire, and an answer without one (a
    /// build that predates it) reads as format 0, which is never this build's, so
    /// it is deferred and never compared.
    #[test]
    fn the_definition_answers_format_crosses_the_wire_and_its_absence_reads_as_zero() {
        let sent = kimmy_storage::DefinitionReply {
            format: kimmy_storage::definition_digest::FORMAT,
            entries: Vec::new(),
            malformed: 0,
        };
        let doc = bson::serialize_to_bson(&sent).unwrap();
        assert!(doc.as_document().unwrap().contains_key("format"), "{doc}");
        let read: kimmy_storage::DefinitionReply = bson::deserialize_from_bson(doc).unwrap();
        assert_eq!(read.format, kimmy_storage::definition_digest::FORMAT);
        let without = bson::bson!({ "entries": [] });
        let read: kimmy_storage::DefinitionReply = bson::deserialize_from_bson(without).unwrap();
        assert_eq!(read.format, 0);
        assert_ne!(read.format, kimmy_storage::definition_digest::FORMAT);
    }

    /// B3 half 2: a definition field of another shape costs the field and never
    /// the frame, and one bad entry costs the entry, counted.
    #[tokio::test]
    async fn a_definition_field_of_another_shape_costs_the_field_and_not_the_frame() {
        let frame = |body: bson::Document| {
            let bytes = bson::serialize_to_vec(&body).unwrap();
            let mut buffer = Vec::new();
            buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&bytes);
            buffer
        };
        let garbled_request = frame(
            bson::doc! { "AskDivergence": { "probe": 7i64, "definitions": "not a request" } },
        );
        assert_eq!(
            read_frame(&mut garbled_request.as_slice()).await.unwrap(),
            Message::AskDivergence {
                probe: Some(CollectionId(7)),
                digest: None,
                definitions: None
            },
        );
        let garbled_answer = frame(bson::doc! { "Divergence": {
        "collections": [1i64], "probe_count": 7i64, "definitions": [1, 2, 3] } });
        let Message::Divergence { collections, probe_count, definitions, .. } =
            read_frame(&mut garbled_answer.as_slice()).await.unwrap()
        else {
            panic!("a Divergence")
        };
        assert_eq!((collections, probe_count, definitions), (vec![CollectionId(1)], Some(7), None));

        // One entry with a reason this build does not know, one that is not an
        // entry at all, beside a good one.
        let good = bson::serialize_to_bson(&kimmy_storage::DefinitionEntry {
            collection: CollectionId(5),
            digest: serde_bytes::ByteBuf::from(vec![1; 16]),
            deferred: None,
            above: None,
            definitions: Vec::new(),
            refused: Vec::new(),
        })
        .unwrap();
        let unknown_reason = bson::bson!({
            "collection": 6i64,
            "digest": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0; 16] },
            "deferred": "a_reason_from_a_later_build",
        });
        let mixed = frame(bson::doc! { "Divergence": {
        "collections": [1i64],
        "definitions": { "entries": [unknown_reason, good, "garbage"] } } });
        let Message::Divergence { definitions, .. } =
            read_frame(&mut mixed.as_slice()).await.unwrap()
        else {
            panic!("a Divergence")
        };
        let reply = definitions.expect("the answer still stands");
        assert_eq!(reply.entries.len(), 1);
        assert_eq!(reply.entries[0].collection, CollectionId(5));
        assert_eq!(reply.malformed, 2, "the two that did not decode are counted");
    }

    /// B3 (N11): a new field of another shape costs the field and never the
    /// frame, and one bad entry costs the entry.
    #[tokio::test]
    async fn a_digest_field_of_another_shape_costs_the_field_and_not_the_frame() {
        let frame = |body: bson::Document| {
            let bytes = bson::serialize_to_vec(&body).unwrap();
            let mut buffer = Vec::new();
            buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&bytes);
            buffer
        };
        let garbled_request =
            frame(bson::doc! { "AskDivergence": { "probe": 7i64, "digest": "not a request" } });
        assert_eq!(
            read_frame(&mut garbled_request.as_slice()).await.unwrap(),
            Message::AskDivergence {
                probe: Some(CollectionId(7)),
                digest: None,
                definitions: None
            },
            "a request of another shape is no request, and the probe is still read"
        );
        let garbled_answer = frame(bson::doc! { "Divergence": {
        "collections": [1i64], "probe_count": 7i64, "digests": [1, 2, 3] } });
        let Message::Divergence { collections, probe_count, digests, .. } =
            read_frame(&mut garbled_answer.as_slice()).await.unwrap()
        else {
            panic!("a Divergence")
        };
        assert_eq!((collections, probe_count, digests), (vec![CollectionId(1)], Some(7), None));

        // One entry with a reason this build does not know, beside a good one.
        let good = bson::serialize_to_bson(&kimmy_storage::DigestEntry {
            collection: CollectionId(5),
            digest: serde_bytes::ByteBuf::from(vec![1; 16]),
            applied: 0,
            uncovered: Vec::new(),
            deferred: None,
        })
        .unwrap();
        let unknown_reason = bson::bson!({
            "collection": 6i64,
            "digest": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: vec![0; 16] },
            "applied": 0,
            "deferred": "a_reason_from_a_later_build",
        });
        let mixed = frame(bson::doc! { "Divergence": {
        "collections": [5i64, 6i64], "probe_count": null,
        "digests": { "entries": [unknown_reason, good] } } });
        let Message::Divergence { digests, .. } = read_frame(&mut mixed.as_slice()).await.unwrap()
        else {
            panic!("a Divergence")
        };
        let reply = digests.expect("the answer still stands");
        assert_eq!(
            reply.entries.len(),
            1,
            "the entry with a reason this build does not know is dropped"
        );
        assert_eq!(reply.entries[0].collection, CollectionId(5));
        assert_eq!(reply.malformed, 1, "and counted, so that it is not silent");

        // The request's uncovered lists are read the same way (N6d).
        let good = bson::serialize_to_bson(&kimmy_storage::Uncovered {
            collection: CollectionId(5),
            keys: vec![serde_bytes::ByteBuf::from(vec![1])],
        })
        .unwrap();
        let ask = frame(bson::doc! { "AskDivergence": { "probe": null, "digest": {
        "cut": {}, "uncovered": [ "not an entry", good ] } } });
        let Message::AskDivergence { digest, .. } = read_frame(&mut ask.as_slice()).await.unwrap()
        else {
            panic!("an AskDivergence")
        };
        let uncovered = digest.expect("the request stands").uncovered;
        assert_eq!(uncovered.len(), 1, "the bad list is dropped and the good one kept");
    }

    #[tokio::test]
    async fn a_closed_stream_is_reported_as_closed_not_as_corruption() {
        // A peer going away is ordinary; it must not look like an attack.
        let empty: &[u8] = &[];
        let err = read_frame(&mut { empty }).await.unwrap_err();
        assert!(matches!(err, ProtocolError::Closed), "got {err:?}");
    }

    #[tokio::test]
    async fn a_stream_that_ends_inside_a_length_prefix_is_an_io_failure_not_a_close() {
        // Two of the four prefix bytes, then nothing: the peer went away
        // mid-frame, which is not the clean close at a frame boundary.
        let partial: &[u8] = &[0, 0];
        let err = read_frame(&mut { partial }).await.unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof),
            "got {err:?}"
        );
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

    #[test]
    fn a_timeout_does_not_render_as_a_malformed_frame() {
        // Round 0270 logged `malformed frame: sync round timed out`. The two
        // send an operator to opposite places -- a wire problem is peer builds
        // and capability negotiation, a timeout is load, a stalled disk or a
        // wedged peer -- so a round that ran out of time must not name the
        // wire at all.
        let timed_out = ProtocolError::TimedOut("sync round".into()).to_string();
        assert_eq!(timed_out, "sync round timed out");
        assert!(
            !timed_out.contains("malformed"),
            "a timeout that says `malformed` sends the pager to read protocol code: {timed_out}"
        );
        assert!(!timed_out.contains("frame"), "nothing about a frame is known to be wrong");

        // And a genuine wire problem still says so.
        let malformed = ProtocolError::Malformed("expected Pushed, got Fault".into()).to_string();
        assert_eq!(malformed, "malformed frame: expected Pushed, got Fault");
    }
}
