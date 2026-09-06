//! Serving peers, and syncing with them.
//!
//! One TCP listener answers questions about what this node holds; one task
//! periodically asks the same questions of its peers. Both sides drive the
//! anti-entropy already in `kimmy-storage` — nothing here decides what wins,
//! which entries are missing, or how a merge resolves. That was built and
//! tested without a network on purpose, and this layer only moves bytes.
//!
//! ```text
//!   serve()                          sync_once()
//!     accept                            connect + handshake
//!     handshake                         AskVersions  ─────▶
//!            ◀───── AskVersions         ◀───── Vectors
//!     Vectors ─────▶                    behind(theirs)?
//!            ◀───── AskEntries          AskEntries  ─────▶
//!     Entries ─────▶                    ◀───── Entries
//!                                       apply_batch
//! ```
//!
//! A round is one-directional: it pulls. Both peers running it against each
//! other is what converges them, and that falls out of every node running the
//! same loop rather than needing a push half.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kimmy_core::{Hlc, NodeId, VersionVector};
use kimmy_storage::{Engine, SyncOutcome};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use kimmy_core::oplog::OplogEntry;

use crate::protocol::{
    MAX_BATCH, MAX_FRAME, Message, ProtocolError, nonce, proof_is_valid, prove, read_frame,
    write_frame,
};

/// How much of a frame the entries themselves may occupy.
///
/// The rest is the `Entries` envelope and BSON's per-element overhead in an array,
/// both small; a mebibyte of slack is far more than either needs and costs one
/// entry's worth of throughput in the rare case this matters at all.
const ENTRY_BUDGET: usize = MAX_FRAME - (1024 * 1024);

/// Whether a batch fits in one frame, and how much of it does if not.
enum Fits {
    All,
    Only(usize),
}

/// How many leading entries fit inside [`ENTRY_BUDGET`].
///
/// Sizes each entry once and takes a running total, rather than serializing the
/// whole batch to find out it is too big and then doing it again for a smaller one.
/// The common case is a single pass that says `All`.
fn how_many_fit(entries: &[OplogEntry]) -> Fits {
    let mut total = 0usize;
    for (i, entry) in entries.iter().enumerate() {
        let size = match bson::serialize_to_vec(entry) {
            Ok(bytes) => bytes.len(),
            // Unencodable here means unencodable in the batch too, so stopping short
            // hands the caller the prefix that can be sent and lets the real error
            // surface where it is reported properly.
            Err(_) => return Fits::Only(i),
        };
        if total + size > ENTRY_BUDGET {
            return Fits::Only(i);
        }
        total += size;
    }
    Fits::All
}

/// How long a peer has to complete the handshake.
///
/// Without it a connection that opens and then says nothing holds a task
/// forever, which is a denial of service that costs the other side nothing.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long one request may take.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How long dialling a peer may take, covering the TCP connect and the TLS
/// handshake that follows it.
///
/// Both were previously unbounded, and the cost of that was not paid by the
/// unreachable peer. A sync round walks its peers sequentially and awaits each
/// one, so a peer whose host drops packets rather than refusing them blocks the
/// round for the kernel's connect timeout — around two minutes at the usual six
/// SYN retries — and the round's own tick cannot fire while it is blocked. One
/// stopped node therefore slowed replication to every *healthy* node behind it
/// in the round, turning a six-second convergence into a four-minute one.
///
/// Short, because this bounds a dial to a peer on the same network rather than
/// a request against it: a peer that cannot complete both handshakes in this
/// long is one the round is better off recording as failed and backing off
/// from, which is exactly what [`crate::PeerHealth`] then does.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Called on the serving side with what a pushed batch became (ADR-140), so
/// the receiver's own counters see a refusal that arrived by push exactly as
/// one that arrived by pull.
pub type PushHook = Arc<dyn Fn(&SyncOutcome) + Send + Sync>;

/// Serve peer requests until the listener fails.
pub async fn serve(engine: Arc<Engine>, listener: TcpListener, secret: String) {
    serve_with(engine, listener, secret, None).await
}

/// [`serve`], reporting each pushed batch's outcome to `on_pushed`.
pub async fn serve_with(
    engine: Arc<Engine>,
    listener: TcpListener,
    secret: String,
    on_pushed: Option<PushHook>,
) {
    let local = listener.local_addr().ok();

    // Generated once per process, not per connection: the certificate proves
    // nothing on its own (see `crate::tls`), so the only cost of reusing it is
    // none, and generating a keypair per peer would be a denial-of-service
    // lever anyone who can open a socket could pull.
    let tls = match crate::tls::ClusterTls::new() {
        Ok(tls) => Arc::new(tls),
        Err(e) => {
            // Fatal rather than a fall back to plaintext. Falling back would
            // mean an operator who configured a cluster expecting encrypted
            // replication silently got none.
            warn!(error = %e, "cannot start cluster TLS; replication will not serve");
            return;
        }
    };
    info!(bind = ?local, "serving cluster replication over TLS");

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!(error = %e, "cluster listener failed to accept");
                continue;
            }
        };

        let engine = Arc::clone(&engine);
        let secret = secret.clone();
        let on_pushed = on_pushed.clone();
        let acceptor = tls.acceptor();
        // One task per peer: a slow or hostile peer must not stall the others,
        // and a panic in one connection must not take the listener down. The
        // TLS handshake happens inside the task for the same reason — it is
        // the first thing an attacker can make slow.
        tokio::spawn(async move {
            let tls_stream = match acceptor.accept(stream).await {
                Ok(s) => s,
                Err(e) => {
                    debug!(%peer, error = %e, "TLS handshake failed");
                    return;
                }
            };
            let binding = {
                let (_, conn) = tls_stream.get_ref();
                match crate::tls::binding(conn) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(%peer, error = %e, "no channel binding; refusing the peer");
                        return;
                    }
                }
            };
            if let Err(e) =
                serve_peer(&engine, tls_stream, &secret, &binding, on_pushed.as_ref()).await
            {
                match e {
                    ProtocolError::Closed => debug!(%peer, "peer disconnected"),
                    other => warn!(%peer, error = %other, "peer connection failed"),
                }
            }
        });
    }
}

async fn serve_peer<S>(
    engine: &Engine,
    mut stream: S,
    secret: &str,
    binding: &[u8],
    on_pushed: Option<&PushHook>,
) -> Result<(), ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let peer = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        accept_handshake(engine, &mut stream, secret, binding),
    )
    .await
    .map_err(|_| ProtocolError::Malformed("handshake timed out".into()))??;
    debug!(?peer, "peer authenticated");

    loop {
        match read_frame(&mut stream).await? {
            Message::AskVersions { witnessed } => {
                let servable =
                    engine.version_vector().map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                if !witnessed {
                    // A requester that predates the flag, answered as it
                    // always was (ADR-146).
                    write_frame(&mut stream, &Message::Versions(servable)).await?;
                    continue;
                }
                // Read after the servable vector, never before: the
                // witnessed vector stays at or above the servable one by
                // construction (ADR-054), and reading it second keeps that
                // true of the pair on the wire, so the requester's gate can
                // only ever read this node as further along, never as short
                // of what it can serve.
                let witnessed = engine
                    .witnessed_vector()
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                write_frame(&mut stream, &Message::Vectors { servable, witnessed }).await?;
            }
            Message::AskWitnessed {} => {
                let witnessed = engine
                    .witnessed_vector()
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                write_frame(&mut stream, &Message::Witnessed(witnessed)).await?;
            }
            Message::AskEntries { from, limit, held } => {
                // Tell a peer below the horizon rather than serving it what is
                // left: it would apply that, advance its version vector, and
                // never learn what had been collected. Judged per origin when
                // the peer said what it holds — a threshold below the horizon
                // is not a gap if everything under it is the peer's own
                // coverage — and by the threshold alone for a peer that did
                // not (ADR-097).
                let servable = match &held {
                    Some(held) => engine.can_serve_peer_holding(held),
                    None => engine.can_serve_from_oplog(from),
                }
                .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                if !servable {
                    write_frame(&mut stream, &Message::BeyondHorizon {}).await?;
                    continue;
                }

                // The peer's limit is a request, not an instruction: honouring
                // an arbitrary one would let it ask for the whole oplog in a
                // single frame.
                let limit = limit.min(MAX_BATCH);
                let window = engine
                    .entries_for_peer(from, limit)
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;

                // Large entries can put a full batch over the frame limit. Failing the
                // write would drop the connection, and the same oversized batch is the
                // next thing to send on every round — so replication would never
                // recover. Answer with the count that fits instead, and let the
                // requester ask again; see `Message::BatchTooLarge` for why serving
                // fewer entries unasked would be a silent gap rather than a kindness.
                match how_many_fit(&window.entries) {
                    Fits::All => {
                        write_frame(
                            &mut stream,
                            &Message::Entries {
                                entries: window.entries,
                                scanned_to: window.scanned_to,
                                exhausted: window.exhausted,
                            },
                        )
                        .await?
                    }
                    Fits::Only(fits) => {
                        warn!(%limit, %fits, "batch does not fit in a frame; asking the peer for fewer");
                        write_frame(&mut stream, &Message::BatchTooLarge { fits }).await?;
                    }
                }
            }
            Message::AskSnapshot { after } => {
                let page = engine
                    .snapshot_page(after)
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                write_frame(&mut stream, &Message::Snapshot(Box::new(page))).await?;
            }
            Message::AskDivergence { probe } => {
                // Metadata only, whatever `probe` is — see
                // `Engine::all_collection_ids`.
                let collections = engine
                    .all_collection_ids()
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                // The one document read in this exchange, bounded to the
                // single collection the requester named (ADR-133).
                let probe_count = match probe {
                    Some(id) => engine
                        .count_by_id(id)
                        .map_err(|e| ProtocolError::Malformed(e.to_string()))?,
                    None => None,
                };
                write_frame(
                    &mut stream,
                    &Message::Divergence {
                        collections: collections.into_iter().collect(),
                        probe_count,
                    },
                )
                .await?;
            }
            Message::Push { entries, scanned_to, exhausted, versions } => {
                // The same cap a pull is served under: a peer must not be able
                // to hand this node an unbounded batch to apply in one go.
                if entries.len() > MAX_BATCH {
                    let reason = format!(
                        "a push of {} entries exceeds the {MAX_BATCH} entry batch limit",
                        entries.len()
                    );
                    let _ = write_frame(&mut stream, &Message::Fault(reason.clone())).await;
                    return Err(ProtocolError::Malformed(reason));
                }
                // Through `apply_peer_batch`, exactly as a pulled window is
                // (ADR-143): the coverage rule raises the witnessed vector only
                // over what the window carried, so this node never witnesses
                // past an entry it was not sent; a schema change it applies is
                // appended onward; one it cannot apply is refused, counted and
                // reported (ADR-123), which is the answer the pusher is waiting
                // for (ADR-140).
                let outcome = engine
                    .apply_peer_batch(&versions, &entries, scanned_to, exhausted)
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                if let Some(hook) = on_pushed {
                    hook(&outcome);
                }
                write_frame(
                    &mut stream,
                    &Message::Pushed {
                        applied: outcome.applied,
                        ddl: outcome.ddl,
                        ddl_refused: outcome.ddl_refused,
                        unknown_collection: outcome.unknown_collection,
                        ddl_declined: outcome.ddl_declined,
                    },
                )
                .await?;
            }
            Message::Fault(reason) => return Err(ProtocolError::Fault(reason)),
            // Anything else is a peer talking out of turn.
            other => {
                let reason = format!("unexpected message: {other:?}");
                let _ = write_frame(&mut stream, &Message::Fault(reason.clone())).await;
                return Err(ProtocolError::Malformed(reason));
            }
        }
    }
}

/// Answer an inbound handshake: prove ourselves, then demand proof.
async fn accept_handshake<S>(
    engine: &Engine,
    stream: &mut S,
    secret: &str,
    binding: &[u8],
) -> Result<kimmy_core::NodeId, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Message::Hello { node, nonce: their_nonce } = read_frame(stream).await? else {
        return Err(ProtocolError::Malformed("expected Hello".into()));
    };

    // Answer their challenge and issue ours in the same frame.
    let ours = nonce(engine.node_id());
    write_frame(
        stream,
        &Message::Welcome {
            node: engine.node_id(),
            nonce: ours.clone(),
            proof: prove(secret, &their_nonce, binding),
        },
    )
    .await?;

    let Message::Confirm { proof } = read_frame(stream).await? else {
        return Err(ProtocolError::Malformed("expected Confirm".into()));
    };
    if !proof_is_valid(secret, &ours, binding, &proof) {
        // Deliberately terse: telling a caller *why* their proof failed helps
        // them iterate towards a valid one.
        let _ = write_frame(stream, &Message::Fault("authentication failed".into())).await;
        return Err(ProtocolError::Unauthenticated);
    }
    Ok(node)
}

/// The one collection this round wants a peer's document count for, and this
/// node's own count of it — computed once by the caller so a tick contacting
/// several peers pays for that collection's scan once, not once per peer
/// (ADR-133).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DivergenceProbe {
    pub id: kimmy_core::CollectionId,
    pub mine_count: Option<u64>,
}

/// Run one anti-entropy round against `peer`, pulling what this node lacks.
///
/// `probe` drives the cross-member divergence check (ADR-133): a message or
/// two is spent on it only in the branch where the round finds nothing left
/// to pull, since that is the one state a truncated sync window can fake —
/// see `kimmy_storage::divergence` for what is compared and why only that
/// branch is safe to check without flapping during ordinary catch-up.
///
/// A round with no memory of the peer: the count half of the check is gated
/// as ADR-133 first had it, on the vectors ADR-146 corrected it to — dropped
/// while the peer has not processed everything this node has — because a
/// peer can only be read as *frozen* rather than catching up across
/// contacts, and this call has seen none. The loop, which has, calls
/// [`sync_once_with`] and carries a [`PeerStalls`] between rounds.
pub async fn sync_once(
    engine: &Engine,
    peer: SocketAddr,
    secret: &str,
    probe: Option<DivergenceProbe>,
) -> Result<SyncOutcome, ProtocolError> {
    sync_once_with(engine, peer, secret, probe, &mut PeerStalls::new()).await
}

/// [`sync_once`], remembering where each peer stood behind this node on the
/// contacts before this one (ADR-145).
///
/// `stalls` is read and updated in the round's own check branch, since that
/// is where the peer's vector exists: the count half of the divergence
/// check runs against a peer that is behind this node only once its
/// processed position behind this node has come back unchanged on
/// [`FROZEN_CONTACTS`] consecutive checked contacts — see
/// `divergence_probe_for` for why a moving position and a still one are
/// different facts, and [`PeerStalls::gate_vector`] for which of the peer's
/// two vectors "behind" is judged on.
pub async fn sync_once_with(
    engine: &Engine,
    peer: SocketAddr,
    secret: &str,
    probe: Option<DivergenceProbe>,
    stalls: &mut PeerStalls,
) -> Result<SyncOutcome, ProtocolError> {
    let (mut stream, their_node) = dial(engine, peer, secret).await?;
    sync_over(engine, &mut stream, peer, their_node, probe, stalls).await
}

/// What a push to one member became (ADR-140, ADR-143).
#[derive(Clone, Debug, PartialEq)]
pub struct PushOutcome {
    /// The member, as it named itself in the handshake.
    pub node: kimmy_core::NodeId,
    /// What the window became there: `ddl_refused` above zero is a schema
    /// change the member could not apply and skipped, counted there. Empty
    /// when the member had already processed the entry and nothing was sent.
    pub outcome: SyncOutcome,
    /// Why the window could not carry the entry, when it could not: the
    /// member is more than a batch behind this node, or below its retention
    /// horizon. Nothing was sent, and anti-entropy carries the entry.
    pub unreached: Option<String>,
}

/// Hand `peer` the window it lacks from this node, ending in `entry`, and
/// wait for what became of it.
///
/// **A push is a pull the sender starts** (ADR-143). The peer is asked what
/// it has processed, the window is derived from that exactly as the peer
/// would derive it for itself — same threshold, same horizon check, same
/// batch and frame limits — and the peer accounts for it through the same
/// coverage rule a pulled window goes through. So a push never carries an
/// entry out of order, and a member's witnessed vector is raised only over
/// entries it was sent. The first form of this call pushed the entry alone
/// through `apply_batch`, which raised the vector past every earlier entry
/// the member had not yet pulled; nothing re-served them.
///
/// A peer that has already processed `entry` is confirmed without anything
/// being sent. A peer the window cannot reach — more than a batch behind, or
/// below this node's retention horizon — is sent nothing and reported
/// `unreached`: a window that stops short of the entry would only do a sync
/// round's work on a request's clock, and the sync loop is already doing
/// that work at its own pace.
///
/// One exchange on a fresh connection, bounded like a sync round.
pub async fn push_entry(
    engine: &Engine,
    peer: SocketAddr,
    secret: &str,
    entry: &OplogEntry,
) -> Result<PushOutcome, ProtocolError> {
    let (mut stream, their_node) = dial(engine, peer, secret).await?;
    let nothing_sent = |unreached: Option<String>| PushOutcome {
        node: their_node,
        outcome: SyncOutcome { peer: Some(their_node), ..SyncOutcome::default() },
        unreached,
    };
    let exchange = async {
        write_frame(&mut stream, &Message::AskWitnessed {}).await?;
        let held = match read_frame(&mut stream).await? {
            Message::Witnessed(held) => held,
            Message::Fault(reason) => return Err(ProtocolError::Fault(reason)),
            other => {
                return Err(ProtocolError::Malformed(format!("expected Witnessed, got {other:?}")));
            }
        };
        if held.get(entry.stamp.node) >= entry.stamp.hlc {
            // A sync round got there first; the member holds or has refused
            // the entry already, and counted whichever it was.
            return Ok(nothing_sent(None));
        }

        // What this node can serve, read *before* the window so the vector
        // never claims more than the window could carry — the order a served
        // pull has, where `AskVersions` precedes `AskEntries`.
        let mine = engine.version_vector().map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        let Some(from) = held.behind(&mine) else {
            return Ok(nothing_sent(None));
        };
        let servable = engine
            .can_serve_peer_holding(&held)
            .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        if !servable {
            return Ok(nothing_sent(Some(
                "the member is below this node's retention horizon; anti-entropy will hand it \
                 a snapshot"
                    .into(),
            )));
        }
        let mut window = engine
            .entries_for_peer(from, MAX_BATCH)
            .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        if let Fits::Only(fits) = how_many_fit(&window.entries) {
            if fits == 0 {
                return Err(ProtocolError::Malformed(format!(
                    "a single oplog entry at or after {from:?} exceeds the {MAX_FRAME} byte \
                     frame limit and cannot replicate"
                )));
            }
            // Re-read at the smaller limit rather than trim: the end the
            // window reports must match the entries it carries (ADR-127).
            window = engine
                .entries_for_peer(from, fits)
                .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        }
        if !window.entries.iter().any(|e| e.stamp == entry.stamp) {
            return Ok(nothing_sent(Some(format!(
                "the member is more than {} entries behind this node; anti-entropy will carry \
                 the change",
                window.entries.len()
            ))));
        }

        write_frame(
            &mut stream,
            &Message::Push {
                entries: window.entries,
                scanned_to: window.scanned_to,
                exhausted: window.exhausted,
                versions: mine,
            },
        )
        .await?;
        match read_frame(&mut stream).await? {
            Message::Pushed { applied, ddl, ddl_refused, unknown_collection, ddl_declined } => {
                Ok(PushOutcome {
                    node: their_node,
                    outcome: SyncOutcome {
                        applied,
                        ddl,
                        ddl_refused,
                        unknown_collection,
                        ddl_declined,
                        peer: Some(their_node),
                        ..SyncOutcome::default()
                    },
                    unreached: None,
                })
            }
            Message::Fault(reason) => Err(ProtocolError::Fault(reason)),
            other => Err(ProtocolError::Malformed(format!("expected Pushed, got {other:?}"))),
        }
    };
    tokio::time::timeout(REQUEST_TIMEOUT, exchange)
        .await
        .map_err(|_| ProtocolError::Malformed("push timed out".into()))?
}

/// Dial `peer`, complete TLS and the handshake, and hand back the stream and
/// the peer's proven node id. The prelude every client-side exchange shares.
async fn dial(
    engine: &Engine,
    peer: SocketAddr,
    secret: &str,
) -> Result<(tokio_rustls::client::TlsStream<TcpStream>, kimmy_core::NodeId), ProtocolError> {
    let tcp =
        tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(peer)).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("connecting to {peer} timed out after {CONNECT_TIMEOUT:?}"),
            )
        })??;

    // Built per round rather than held: a sync round is seconds apart, and a
    // dialling node has no accept loop to amortise it against.
    let tls = crate::tls::ClusterTls::new()
        .map_err(|e| ProtocolError::Malformed(format!("cluster TLS: {e}")))?;

    // Bounded for the same reason as the connect above, and separately from it:
    // a host that completes a TCP handshake and then says nothing leaves the
    // TLS handshake waiting, which stalls the round just as effectively as an
    // unroutable address does.
    let mut stream = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tls.connector().connect(crate::tls::ClusterTls::server_name(), tcp),
    )
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("TLS handshake with {peer} timed out after {CONNECT_TIMEOUT:?}"),
        )
    })?
    .map_err(|e| ProtocolError::Malformed(format!("TLS handshake with {peer}: {e}")))?;

    let binding = {
        let (_, conn) = stream.get_ref();
        crate::tls::binding(conn).map_err(ProtocolError::Malformed)?
    };

    let their_node = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        open_handshake(engine, &mut stream, secret, &binding),
    )
    .await
    .map_err(|_| ProtocolError::Malformed("handshake timed out".into()))??;
    Ok((stream, their_node))
}

/// The body of [`sync_once`], over a connection [`dial`] opened.
async fn sync_over<S>(
    engine: &Engine,
    mut stream: S,
    peer: SocketAddr,
    their_node: kimmy_core::NodeId,
    probe: Option<DivergenceProbe>,
    stalls: &mut PeerStalls,
) -> Result<SyncOutcome, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let round = async {
        // Both of the peer's vectors, on the one frame every round spends
        // anyway (ADR-146): what it can serve drives the pull, and what it
        // has processed drives the count half's gate below. A peer that
        // predates the flag answers with the first alone.
        write_frame(&mut stream, &Message::AskVersions { witnessed: true }).await?;
        let (theirs, their_witnessed) = match read_frame(&mut stream).await? {
            Message::Vectors { servable, witnessed } => (servable, Some(witnessed)),
            Message::Versions(servable) => (servable, None),
            Message::Fault(reason) => return Err(ProtocolError::Fault(reason)),
            other => {
                return Err(ProtocolError::Malformed(format!("expected Vectors, got {other:?}")));
            }
        };
        let processed = stalls.gate_vector(peer, their_node, &theirs, their_witnessed.as_ref());

        // What we have *seen*, not what we could serve. Asking against the
        // servable vector re-requests everything a node processed without
        // appending — last-writer-wins losers, DDL it refused, declined or
        // judged history, and stamps covered by a batch's window that it was
        // never sent, such as a withheld `UniqueViolation` — on every round,
        // forever (ADR-054). Replicated DDL that applies is *not* among them:
        // `apply_ddl` appends the originating entry.
        let mine =
            engine.witnessed_vector().map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        let Some(from) = mine.behind(&theirs) else {
            // Nothing to pull, but the peer's own position is still news:
            // how far *it* trails *us* is what says whether it has been
            // away longer than tombstone retention.
            //
            // It is also the one belief a truncated sync window can hold
            // falsely without anything on the anti-entropy path noticing
            // (ADR-133): `mine` already claims to cover every origin `theirs`
            // advertised, which is exactly finding 14's signature. Asking the
            // peer what it actually holds is cheap precisely because this
            // branch is common on a converged cluster — though not, on its
            // own, on a busy one; see the `exhausted` branch below for the
            // other place this check runs.
            let gate = divergence_probe_for(probe, stalls.observe(their_node, processed, &mine));
            let findings = ask_divergence(engine, &mut stream, gate.probe).await?;
            return Ok(SyncOutcome {
                peer: Some(their_node),
                behind_ms: behind_beyond_horizon(engine, &theirs, &mine)?,
                divergent: Some(findings.existence),
                count_probe: findings.count,
                count_probe_deferred: gate.deferred,
                exhausted: true,
                ..SyncOutcome::default()
            });
        };

        // Ask for a full batch; if the peer says that will not fit, ask again for the
        // number it named. At most one retry, because the peer answers with a count
        // rather than a refusal. The retry re-reads the window at the smaller limit,
        // so the window end the peer reports matches the entries it sends and the
        // coverage rules are untouched.
        //
        // The vector `from` came from travels with it, so the peer can judge
        // its horizon per origin rather than by the threshold alone.
        let mut limit = MAX_BATCH;
        let held = Some(mine.clone());
        write_frame(&mut stream, &Message::AskEntries { from, limit, held: held.clone() }).await?;
        let mut answer = read_frame(&mut stream).await?;

        if let Message::BatchTooLarge { fits } = answer {
            if fits == 0 {
                // One entry alone exceeds the frame, so no limit can carry it. Name it
                // rather than probing: this cannot replicate until the entry is gone.
                return Err(ProtocolError::Malformed(format!(
                    "a single oplog entry at or after {from:?} exceeds the {MAX_FRAME} \
                     byte frame limit and cannot replicate"
                )));
            }
            limit = fits;
            warn!(%peer, %limit, "peer cannot fit a full batch; asking for what it offered");
            write_frame(&mut stream, &Message::AskEntries { from, limit, held }).await?;
            answer = read_frame(&mut stream).await?;
        }

        // Whether this round's own pull reached the peer's true tail — the
        // fact that lets the divergence check also run on a round that
        // pulled something, rather than only on a round that found nothing
        // left to pull (ADR-133). A completed snapshot pull earns the same
        // reading: `pull_snapshot` does not return until `page.next` is
        // `None`, which is every page the peer had as of the pull, the
        // snapshot's own version of "reached the tail".
        let mut window_exhausted = false;
        let mut outcome = match answer {
            // The batch, and what it proved: an exhausted window is the peer's
            // whole tail, any other ends at the stamp the peer says it scanned
            // to. Either way the witnessed vector is raised for every origin
            // the peer advertised, including stamps it holds but never ships —
            // a `UniqueViolation` (ADR-029) — because otherwise such a stamp
            // pins `behind` at its floor and the same window is re-served
            // every round for the life of the cluster (ADR-082). The peer
            // reports where its window ended rather than leaving it to be
            // deduced from how many entries arrived, which a withheld entry
            // could make a lie (ADR-127). The decision lives in storage
            // (`coverage_after_batch`), where it is tested between engines
            // without a network.
            Message::Entries { entries, scanned_to, exhausted } => {
                // A correct sender cannot produce an empty, non-exhausted
                // window: `read_oplog_from_where` only stops short of the
                // limit by reaching the true end of the oplog, and it never
                // breaks without having pushed at least one kept entry
                // first when it does not (U1's proof, over every withheld
                // arrangement and limit). This combination is therefore not
                // an ordinary capped pull with nothing new to offer — it is
                // a peer claiming both "nothing here" and "more exists",
                // which nothing downstream can safely read as convergence.
                // Failed as a malformed round rather than silently treated
                // as "not exhausted, do not check": that would fold a
                // genuine signal into the same bucket as an unremarkable
                // capped pull, and a failed round is exactly the shape
                // `kimmy_sync_failures_total` exists to make visible.
                if is_unreachable_from_a_correct_sender(entries.len(), exhausted) {
                    // Logged unconditionally, not left to the caller's
                    // generic per-peer failure debounce: that debounce is
                    // right for the ordinary noise of a peer going up and
                    // down, and wrong here, because it can route this
                    // occurrence's very first sighting to `debug` if the
                    // same peer already had an unrelated failure recently —
                    // leaving nothing but a bare counter increment for a
                    // condition whose whole point is that it should never
                    // occur at all. The counter says something is wrong;
                    // this is what says what.
                    warn!(
                        %peer,
                        ?from,
                        "peer answered an empty batch while reporting its tail was not \
                         reached; a correct sender cannot produce this, refusing the round"
                    );
                    return Err(ProtocolError::Malformed(format!(
                        "peer at {peer} answered an empty batch from {from:?} while reporting \
                         its tail was not reached — a correct sender cannot produce this"
                    )));
                }
                window_exhausted = exhausted;
                engine
                    .apply_peer_batch(&theirs, &entries, scanned_to, exhausted)
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))
            }
            // The peer has collected what we need. Fall back to current state.
            Message::BeyondHorizon {} => {
                warn!(%peer, "behind the peer's retention horizon; falling back to a snapshot");
                window_exhausted = true;
                pull_snapshot(engine, &mut stream).await
            }
            other => Err(ProtocolError::Malformed(format!("expected Entries, got {other:?}"))),
        }?;

        // How far behind in time this node is after the round: the age of
        // the newest entry it holds from any origin the peer, as of the vector
        // it opened with, holds newer entries of. Zero in the caught-up steady
        // state; while a backlog wider than one batch drains, it grows with
        // the clock — which is what an operator wants a gauge for, and what
        // the span of missing history did not do for a bulk insert whose
        // stamps all lay within a second (ADR-122). `theirs` is a round old
        // by now, so this is a floor — a peer that raced ahead during the
        // round shows up next round.
        let mine =
            engine.witnessed_vector().map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        outcome.lag_ms =
            kimmy_storage::lag_behind_ms(&mine, &theirs, kimmy_storage::physical_now_ms());
        outcome.exhausted = window_exhausted;
        // Only when the pull reached the peer's tail: a round still working
        // through a backlog deeper than one batch has not earned the belief
        // the check depends on, and must not spend a message finding out
        // (ADR-133). This is what lets a busy cluster — many small rounds,
        // each comfortably under the batch cap — still get checked on
        // nearly every round, unlike a single global "nothing to pull" gate,
        // which a continuous trickle of new writes can starve indefinitely.
        if window_exhausted {
            let gate = divergence_probe_for(probe, stalls.observe(their_node, processed, &mine));
            let findings = ask_divergence(engine, &mut stream, gate.probe).await?;
            outcome.divergent = Some(findings.existence);
            outcome.count_probe = findings.count;
            outcome.count_probe_deferred = gate.deferred;
        }
        // The other direction is a different question: how far the peer
        // trails this node, in history it can no longer be served. A peer
        // that is more than tombstone retention behind may be holding
        // documents this node has deleted and already collected the
        // tombstones for — the resurrection case (ADR-085). Reported, not
        // acted on: the loop decides what to say about it.
        outcome.behind_ms = behind_beyond_horizon(engine, &theirs, &mine)?;
        outcome.peer = Some(their_node);
        Ok(outcome)
    };

    tokio::time::timeout(REQUEST_TIMEOUT, round)
        .await
        .map_err(|_| ProtocolError::Malformed("sync round timed out".into()))?
}

/// How far the peer trails this node where it can no longer catch up
/// incrementally — the measure a stale-rejoiner verdict is made on.
///
/// Not the bare span between the two vectors: an origin that wrote nothing
/// for longer than tombstone retention and then wrote once leaves every peer
/// trailing it by the whole silence until their next round, and a restarted
/// member does exactly that when it re-registers itself. Measured on a
/// converged three-member cluster: the first round after a member came back
/// named *both* peers stale — behind by the time since the previous restart
/// — and withdrew it five seconds later. The retention record says whether
/// the gap holds anything the peer cannot be served, and only then does the
/// span mean what the verdict says it means (ADR-097).
fn behind_beyond_horizon(
    engine: &Engine,
    theirs: &kimmy_core::VersionVector,
    mine: &kimmy_core::VersionVector,
) -> Result<u64, ProtocolError> {
    let collected =
        engine.oplog_collected().map_err(|e| ProtocolError::Malformed(e.to_string()))?;
    Ok(kimmy_storage::lag_beyond_horizon_ms(theirs, mine, &collected))
}

/// Whether to trust the peer's document count this round, given where the
/// peer stands: its *witnessed* vector, fetched at the top of this round,
/// against this node's own witnessed vector as of just before asking
/// (ADR-133, on the vectors ADR-146 corrected it to).
///
/// The gate that gets a caller into a divergence check at all —
/// `mine.behind(&theirs).is_none()`, or the analogous `exhausted` check —
/// only protects *this node's* belief that it is not behind the peer. It
/// says nothing about the reverse: whether the *peer* is behind this node.
/// A peer that has simply not yet pulled this node's own recent writes will
/// answer a probe with a stale, lower count for any collection those writes
/// touched — a real difference, but ordinary replication lag, not a
/// divergence. Measured on the round that produced finding 14's cluster:
/// peers lagged 130–260 s behind each other in steady operation, which
/// would otherwise have kept the count half of this check reporting a
/// mismatch, and the two-tick confirmation does not filter it out, because
/// a lagging peer reproduces the same mismatch on every consecutive contact.
///
/// `theirs.behind(&mine)` asks the reverse question directly: is there
/// anything `mine` holds, at any origin, that `theirs` has not yet
/// witnessed? `Some` means the peer is behind this node and its answer
/// cannot be trusted for a count this round, so the probe is dropped
/// (`None`) — the existence half is unaffected, since it never depends on
/// the peer being caught up on anything of *this* node's.
///
/// **Both sides of that question are witnessed vectors (ADR-146).** The
/// gate first compared the peer's *servable* vector — what `AskVersions`
/// answered, what the peer can hand onward — against this node's
/// *witnessed* one, and the two are not the same measure. A peer that
/// processed this node's latest entry from some origin without appending
/// it — the loser of a concurrent write (ADR-054), a schema change it
/// refused or declined, a `UniqueViolation` stamp it witnessed through a
/// batch's coverage without ever being sent the entry (`entries_for_peer`
/// withholds one from a pull and a push alike, ADR-029) — has a servable
/// position on that origin below this node's witnessed
/// position, for ever. Under the servable gate it read as behind
/// indefinitely: before ADR-145 the count half was silently never compared
/// against it, and after, it read as behind-and-still and was compared for
/// the wrong reason after [`FROZEN_CONTACTS`] deferred contacts — on a
/// converged idle cluster, a steady trickle of `deferred` from a peer that
/// had nothing to catch up on. "Behind" means what ADR-133 says only when
/// it is asked of what the peer has *processed*, which is the vector
/// `AskVersions { witnessed: true }` now carries beside the servable one.
///
/// **Behind and still is not behind and catching up (ADR-145).** The rule
/// above reads every peer that is behind as a peer that is catching up, and
/// a peer whose inbound replication has wedged is behind for good: on a
/// three-member cluster where one member's every round failed for half an
/// hour, the two healthy members ran the check some 250 times each and the
/// count half never once compared against the wedged member, while four
/// collections sat with different document counts on it and the same fifty
/// names everywhere — the existence half agreed, the count half never
/// looked, and the gauge said 0 with `ran` climbing. What tells the two
/// apart is not how far behind the peer is but whether it is *moving*: a
/// backlog draining advances the peer's position on some origin it trails
/// this node on every round, and a wedge leaves every such position exactly
/// where it was. So the probe is dropped only for a peer that is behind
/// **and advancing** — [`PeerPosition::Advancing`] — and compared once its
/// position behind this node has come back unchanged on
/// [`FROZEN_CONTACTS`] consecutive checked contacts. The defect-2 argument
/// still holds in full for the advancing case, which is every healthy
/// lagging peer; the frozen case is the one it was never about.
///
/// The dropped probe is reported as `deferred` so a caller can count it.
/// Without that a count half that never runs against a peer permanently
/// behind is indistinguishable from one that runs and agrees, which is
/// what the wedged cluster above read as.
fn divergence_probe_for(probe: Option<DivergenceProbe>, position: PeerPosition) -> CountGate {
    match position {
        PeerPosition::Advancing => CountGate { probe: None, deferred: probe.is_some() },
        PeerPosition::CaughtUp | PeerPosition::Frozen => CountGate { probe, deferred: false },
    }
}

/// What [`divergence_probe_for`] decided: the probe to send, if any, and
/// whether a probe the rotation named was held back this contact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CountGate {
    probe: Option<DivergenceProbe>,
    deferred: bool,
}

/// How many consecutive checked contacts a peer's processed position behind
/// this node must come back unchanged on before the peer is read as frozen
/// rather than catching up (ADR-145).
///
/// A named constant rather than a setting: this is the number of sightings
/// a stall needs before it stops being a coincidence, not a knob an
/// operator tunes per deployment. Three, because a peer with a backlog
/// moves on every round it completes, so even one unchanged sighting is
/// unusual, and three in a row is a peer that has stopped — at the default
/// interval, fifteen seconds of standing still on a cluster whose healthy
/// members converge in five. A larger value would only delay the count half
/// against a peer that is already wedged; a smaller one would trust a peer
/// that merely missed one round's pull from this node, which the fanout
/// rotation on a cluster larger than a few members makes routine.
pub const FROZEN_CONTACTS: u32 = 3;

/// Where a peer stands against this node, as of the contact just opened
/// (ADR-145). The count half of the divergence check trusts a peer's
/// document count in the first and third states and not the second.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerPosition {
    /// The peer has processed everything this node has — its witnessed
    /// vector is not behind this node's at any origin. ADR-133's own
    /// trusted case, on the vector ADR-146 corrected it to.
    CaughtUp,
    /// The peer has not processed something this node has, on some origin,
    /// and its processed position there moved since the last checked
    /// contact, or this node has too few sightings of it to say. Ordinary
    /// catch-up: its count is stale and must not be compared (ADR-133,
    /// defect 2).
    Advancing,
    /// The peer is behind this node and its processed position on every
    /// origin it trails this node on has come back unchanged on
    /// [`FROZEN_CONTACTS`] consecutive checked contacts. Not catching up:
    /// whatever its count says is what it holds, and will keep holding.
    Frozen,
}

/// Where each peer last stood behind this node, and for how many contacts
/// it has stood there (ADR-145).
///
/// Kept here, beside the gate that reads it, rather than in `PeerHealth` or
/// `DivergenceTracker`. `PeerHealth` is failure bookkeeping keyed by
/// address, and it *forgets* a peer on every successful round — the exact
/// rounds this memo is built from, since a frozen peer answers every one of
/// this node's pulls perfectly. `DivergenceTracker` is `kimmy-storage`'s,
/// transport-free by design, and keyed by what a check *found*; this is
/// keyed by what the wire said before the check ran, which is transport's
/// own business and the one place the peer's vector exists. Keyed by node
/// id, as the tracker is, because the handshake has already paired the
/// address with one by the time the round reads the vector, and it is the
/// id a restarted member keeps.
///
/// Only the origins the peer *trails this node on* are remembered, at the
/// peer's *processed* position for each — its witnessed vector against this
/// node's (ADR-146), so a position only ever means "has not processed", and
/// an entry the peer processed without appending does not read as a
/// trailing position that never moves. The peer's own origin is never among
/// them — nothing has processed more of a node's writes than the node — so
/// a wedged member that is still taking local writes reads as still, which
/// it is on every origin that matters here. An origin this node itself
/// moved ahead on between two contacts joins the map without breaking the
/// run, because that is this node advancing, not the peer; an origin the
/// peer moved on breaks it, whether it caught up on that origin entirely or
/// only got closer.
///
/// The invariant the memo and the gate hold together: the count probe is
/// deferred only for a peer whose witnessed vector trails this node's
/// witnessed vector on some origin *and* is still advancing there.
#[derive(Debug, Default)]
pub struct PeerStalls {
    by_peer: HashMap<NodeId, Stall>,
    /// Peers whose last answer carried no witnessed vector — a version
    /// before ADR-146 — so the gate is judged on their servable one, and
    /// the log says so once rather than every round.
    without_witnessed: HashSet<NodeId>,
}

/// One peer's remembered position and how long it has held it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Stall {
    /// The peer's processed position, as of the last checked contact, on
    /// every origin it then trailed this node on.
    trailing: BTreeMap<NodeId, Hlc>,
    /// Consecutive checked contacts on which every remembered position came
    /// back unchanged.
    unchanged: u32,
}

impl PeerStalls {
    pub fn new() -> Self {
        Self::default()
    }

    /// Which of the peer's two vectors this contact's gate is judged on
    /// (ADR-146): what it has processed, when it said; what it can serve,
    /// from a peer that answered `AskVersions` without the witnessed vector
    /// — a version before the flag — which is ADR-133's gate exactly as it
    /// was, for that contact only, rather than a failed round. A peer that
    /// processed an entry without appending it reads as behind under that
    /// gate, and the count half defers or, after [`FROZEN_CONTACTS`]
    /// contacts, compares it for the wrong reason, which is the state this
    /// node was in before the peer is upgraded. Logged once per peer each
    /// way, at info, because a rolling upgrade is bounded and ends, and a
    /// `/metrics` series for it would read 0 for ever after: the line
    /// names the member, and the operations guide names what a `deferred`
    /// trickle on an idle cluster means.
    pub fn gate_vector<'a>(
        &mut self,
        peer: SocketAddr,
        node: NodeId,
        servable: &'a VersionVector,
        witnessed: Option<&'a VersionVector>,
    ) -> &'a VersionVector {
        match witnessed {
            Some(witnessed) => {
                if self.without_witnessed.remove(&node) {
                    info!(
                        %peer,
                        node = %node,
                        "peer now says what it has processed; the count half of the \
                         divergence check is gated on it again"
                    );
                }
                witnessed
            }
            None => {
                if self.without_witnessed.insert(node) {
                    info!(
                        %peer,
                        node = %node,
                        "peer answered without saying what it has processed (a version before \
                         the field); the count half of the divergence check is gated on what \
                         it can serve until it does, which reads an entry it processed without \
                         appending as a position it is behind on"
                    );
                }
                servable
            }
        }
    }

    /// Fold in the vector this checked contact with `peer` opened with, and
    /// say where the peer stands. `theirs` is the peer's witnessed vector as
    /// fetched at the top of the round — or its servable one, from a peer
    /// that did not say ([`Self::gate_vector`]); `mine` is this node's
    /// witnessed vector as of just before the check.
    ///
    /// Called once per contact the check runs on, and not on a contact
    /// ADR-133's cap-truncation skip applies to: "consecutive" is
    /// consecutive *checked* contacts, and a truncated pull in between says
    /// nothing about whether the peer moved.
    pub fn observe(
        &mut self,
        peer: NodeId,
        theirs: &VersionVector,
        mine: &VersionVector,
    ) -> PeerPosition {
        let trailing: BTreeMap<NodeId, Hlc> = mine
            .iter()
            .filter(|(origin, mine_at)| theirs.get(*origin) < *mine_at)
            .map(|(origin, _)| (origin, theirs.get(origin)))
            .collect();
        if trailing.is_empty() {
            self.by_peer.remove(&peer);
            return PeerPosition::CaughtUp;
        }

        let stall = self.by_peer.entry(peer).or_default();
        let still = !stall.trailing.is_empty()
            && stall.trailing.iter().all(|(origin, at)| theirs.get(*origin) == *at);
        stall.unchanged = if still { stall.unchanged.saturating_add(1) } else { 0 };
        stall.trailing = trailing;
        if stall.unchanged >= FROZEN_CONTACTS {
            PeerPosition::Frozen
        } else {
            PeerPosition::Advancing
        }
    }
}

/// Whether a peer's `Entries` answer combines a claim no correct sender can
/// produce: an empty batch while also reporting its tail was not reached.
///
/// `read_oplog_from_where` only stops short of the batch limit by reaching
/// the true end of the peer's oplog, and it never returns having kept zero
/// entries without doing so — so `!exhausted` implies at least one entry,
/// over every arrangement of withheld entries and every limit (U1's proof).
/// Pulled out as its own function because the one-line condition it replaces
/// is exactly what an incautious edit is likely to simplify: `!exhausted`
/// alone reads as "the same thing, shorter" and is not — it would refuse
/// every ordinary capped pull on a busy cluster, which is the overwhelming
/// common case this function must leave alone. See the truth table in this
/// function's own tests for the one cell that is actually the error.
fn is_unreachable_from_a_correct_sender(entries_len: usize, exhausted: bool) -> bool {
    entries_len == 0 && !exhausted
}

/// Ask the peer what it holds, and compare against what this node holds
/// (ADR-133). One message each way, on the connection already open for this
/// round.
async fn ask_divergence<S>(
    engine: &Engine,
    stream: &mut S,
    probe: Option<DivergenceProbe>,
) -> Result<kimmy_storage::DivergenceFindings, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Read fresh per peer contacted this tick, unlike `probe`'s count: this
    // is a metadata scan, not a document read, so paying it once per peer
    // rather than caching it across the tick's peer loop is not the cost
    // this module bounds (see the module docs on `Engine::all_collection_ids`).
    let mine_collections =
        engine.all_collection_ids().map_err(|e| ProtocolError::Malformed(e.to_string()))?;

    write_frame(stream, &Message::AskDivergence { probe: probe.map(|p| p.id) }).await?;
    let (peer_collections, probe_count) = match read_frame(stream).await? {
        Message::Divergence { collections, probe_count } => (collections, probe_count),
        Message::Fault(reason) => return Err(ProtocolError::Fault(reason)),
        other => {
            return Err(ProtocolError::Malformed(format!("expected Divergence, got {other:?}")));
        }
    };

    let mine = kimmy_storage::DivergenceLocalState {
        collections: mine_collections,
        probe: probe.map(|p| (p.id, p.mine_count)),
    };
    let peer = kimmy_storage::DivergencePeerAnswer {
        collections: peer_collections.into_iter().collect(),
        probe_count,
    };
    Ok(kimmy_storage::compare_divergence(&mine, &peer))
}

/// Pull a full snapshot, page by page, until the peer says it is complete.
///
/// Returns what applying it changed, so a caller sees a snapshot the same way
/// it sees an incremental round.
async fn pull_snapshot<S>(engine: &Engine, stream: &mut S) -> Result<SyncOutcome, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut cursor = None;
    let mut outcome = SyncOutcome::default();

    loop {
        write_frame(stream, &Message::AskSnapshot { after: cursor.clone() }).await?;
        let page = match read_frame(stream).await? {
            Message::Snapshot(page) => page,
            Message::Fault(reason) => return Err(ProtocolError::Fault(reason)),
            other => {
                return Err(ProtocolError::Malformed(format!("expected Snapshot, got {other:?}")));
            }
        };

        let page_outcome = engine
            .apply_snapshot_page(&page)
            .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        outcome.applied += page_outcome.applied;
        // Counted where a refusal reached through the oplog is, so the
        // metric and the round report do not depend on the route (ADR-123).
        outcome.ddl_refused += page_outcome.ddl_refused;

        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    info!(documents = outcome.applied, "caught up from a snapshot");
    Ok(outcome)
}

/// Open a handshake: challenge them, check the answer, then answer theirs.
///
/// Returns the peer's node id, as it introduced itself and then proved it
/// holds the cluster secret — the name a sync outcome is reported under.
async fn open_handshake<S>(
    engine: &Engine,
    stream: &mut S,
    secret: &str,
    binding: &[u8],
) -> Result<kimmy_core::NodeId, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let ours = nonce(engine.node_id());
    write_frame(stream, &Message::Hello { node: engine.node_id(), nonce: ours.clone() }).await?;

    let (their_node, their_nonce, proof) = match read_frame(stream).await? {
        Message::Welcome { node, nonce, proof } => (node, nonce, proof),
        Message::Fault(reason) => return Err(ProtocolError::Fault(reason)),
        other => {
            return Err(ProtocolError::Malformed(format!("expected Welcome, got {other:?}")));
        }
    };

    // Checked *before* answering their challenge: proving ourselves to an
    // unauthenticated peer would tell it whether its guess at the secret was
    // close, and hands it a valid proof for a nonce it chose.
    if !proof_is_valid(secret, &ours, binding, &proof) {
        return Err(ProtocolError::Unauthenticated);
    }

    write_frame(stream, &Message::Confirm { proof: prove(secret, &their_nonce, binding) }).await?;
    Ok(their_node)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The truth table `is_unreachable_from_a_correct_sender` decides. Only
    /// one of the four cells is the error; the other three are all ordinary
    /// traffic, and the one an edit is most likely to break by accident —
    /// a non-empty, non-exhausted batch, the shape of every capped pull on
    /// a busy cluster — is asserted explicitly rather than left implied.
    #[test]
    fn only_an_empty_non_exhausted_batch_is_unreachable_from_a_correct_sender() {
        assert!(
            is_unreachable_from_a_correct_sender(0, false),
            "the one cell that is the error: nothing shipped, tail not reached"
        );
        assert!(
            !is_unreachable_from_a_correct_sender(0, true),
            "an empty batch because the peer's whole tail really was empty"
        );
        assert!(
            !is_unreachable_from_a_correct_sender(50, false),
            "an ordinary capped pull -- the regression a `!exhausted` shortcut would introduce"
        );
        assert!(
            !is_unreachable_from_a_correct_sender(50, true),
            "a full batch that happened to reach the tail on its last entry"
        );
    }

    fn node(n: u128) -> NodeId {
        NodeId::from_bytes(n.to_be_bytes())
    }

    fn vector(entries: &[(NodeId, u64)]) -> VersionVector {
        entries.iter().map(|&(origin, wall)| (origin, Hlc::new(wall, 0))).collect()
    }

    fn probe() -> Option<DivergenceProbe> {
        Some(DivergenceProbe { id: kimmy_core::CollectionId(7), mine_count: Some(10) })
    }

    /// The truth table `divergence_probe_for` decides over `PeerStalls`
    /// (ADR-133 defect 2, as amended by ADR-145 and ADR-146), on the
    /// vectors the gate is judged on: the peer's witnessed vector against
    /// this node's. Three rows: a peer that is behind and moving is
    /// deferred, every time; a peer that is behind and has stood still for
    /// `FROZEN_CONTACTS` checked contacts is compared; a peer that is not
    /// behind is compared at once. The gate's `deferred` flag — what the
    /// `deferred` outcome is counted from — is asserted alongside the probe
    /// on every row, so the counter cannot say one thing while the wire
    /// does another.
    #[test]
    fn the_count_probe_is_deferred_for_a_moving_peer_and_compared_for_a_still_one() {
        let peer = node(1);
        let me = node(2);
        let mut stalls = PeerStalls::new();

        // Behind and advancing: the peer's processed position on this
        // node's origin moves on every contact, the shape of a backlog
        // draining.
        let mine = vector(&[(me, 100), (peer, 5)]);
        for their_wall in [10u64, 20, 30, 40, 50] {
            let theirs = vector(&[(me, their_wall), (peer, 5)]);
            let position = stalls.observe(peer, &theirs, &mine);
            assert_eq!(position, PeerPosition::Advancing, "moved since last contact");
            let gate = divergence_probe_for(probe(), position);
            assert_eq!(gate, CountGate { probe: None, deferred: true }, "at {their_wall}");
        }

        // Behind and frozen: the same processed position comes back on
        // contact after contact. The first sighting is the memo being
        // written, the next `FROZEN_CONTACTS` are it standing still, and
        // the probe goes out on the last of those.
        let mut stalls = PeerStalls::new();
        let theirs = vector(&[(me, 50), (peer, 5)]);
        for sighting in 1..=FROZEN_CONTACTS {
            let position = stalls.observe(peer, &theirs, &mine);
            assert_eq!(position, PeerPosition::Advancing, "sighting {sighting}: not yet frozen");
            assert!(divergence_probe_for(probe(), position).deferred, "sighting {sighting}");
        }
        let position = stalls.observe(peer, &theirs, &mine);
        assert_eq!(position, PeerPosition::Frozen, "unchanged for FROZEN_CONTACTS contacts");
        assert_eq!(
            divergence_probe_for(probe(), position),
            CountGate { probe: probe(), deferred: false },
            "a still peer's count is what it holds and will keep holding"
        );
        // And stays compared while it stays still.
        assert_eq!(stalls.observe(peer, &theirs, &mine), PeerPosition::Frozen);

        // Not behind: compared at once, no memory needed, and a memo that
        // existed is dropped rather than left to count a peer that has
        // since caught up.
        let caught_up = vector(&[(me, 100), (peer, 5)]);
        let position = stalls.observe(peer, &caught_up, &mine);
        assert_eq!(position, PeerPosition::CaughtUp);
        assert_eq!(
            divergence_probe_for(probe(), position),
            CountGate { probe: probe(), deferred: false }
        );
        assert!(stalls.by_peer.is_empty(), "nothing to remember about a peer that is level");

        // No probe named this tick is neither compared nor deferred,
        // whatever the peer's position.
        assert_eq!(
            divergence_probe_for(None, PeerPosition::Advancing),
            CountGate { probe: None, deferred: false },
            "nothing in rotation is not a deferral"
        );
    }

    /// The two movements that must not break a stall's run, and the one that
    /// must — all on witnessed positions (ADR-146). This node moving ahead
    /// on its own origin, or gaining a new origin from a third member, is
    /// this node advancing, not the peer; the peer's own writes never enter
    /// the map at all, because nothing has processed more of a node's
    /// writes than the node. The peer processing more of an origin it
    /// trails — even without catching up on it — is the peer advancing, and
    /// resets the run.
    #[test]
    fn a_stall_survives_this_nodes_own_progress_and_breaks_on_the_peers() {
        let peer = node(1);
        let me = node(2);
        let third = node(3);
        let mut stalls = PeerStalls::new();

        // What the peer has processed, on each contact.
        let theirs = vector(&[(me, 50), (peer, 5)]);
        stalls.observe(peer, &theirs, &vector(&[(me, 100), (peer, 5)]));
        // This node writes more: its own origin moves, the peer's position
        // there does not.
        stalls.observe(peer, &theirs, &vector(&[(me, 200), (peer, 5)]));
        // This node pulls a third member's writes the peer has none of: a
        // new trailing origin appears at the peer's position of zero.
        stalls.observe(peer, &theirs, &vector(&[(me, 300), (peer, 5), (third, 40)]));
        // The peer takes a local write: its own origin moves, which is never
        // in the map.
        let theirs_wrote = vector(&[(me, 50), (peer, 9)]);
        let mine = vector(&[(me, 300), (peer, 5), (third, 40)]);
        assert_eq!(
            stalls.observe(peer, &theirs_wrote, &mine),
            PeerPosition::Frozen,
            "three unchanged sightings despite everything this node and the peer did locally"
        );

        // The peer processes some of this node's writes — closer, still
        // behind.
        let theirs_moved = vector(&[(me, 120), (peer, 9)]);
        assert_eq!(
            stalls.observe(peer, &theirs_moved, &mine),
            PeerPosition::Advancing,
            "closer on an origin it trails is movement, and the run restarts"
        );
        assert_eq!(stalls.by_peer[&peer].unchanged, 0);
    }

    /// The defect ADR-146 corrects, at the gate. A peer that processed this
    /// node's latest entry without appending it — the loser of a concurrent
    /// write, say — has a servable position below this node's witnessed
    /// one on that origin and a witnessed position level with it. Judged
    /// on what it has processed, it is not behind and is compared at once;
    /// judged on what it can serve, as the gate was, it is behind for ever
    /// and reads as advancing, then frozen. A peer whose witnessed vector
    /// does trail is behind, whatever it can serve.
    #[test]
    fn a_peer_that_processed_without_appending_is_not_behind() {
        let peer = node(1);
        let me = node(2);
        let mine = vector(&[(me, 100), (peer, 5)]);
        let their_servable = vector(&[(me, 80), (peer, 5)]);
        let their_witnessed = vector(&[(me, 100), (peer, 5)]);

        let mut stalls = PeerStalls::new();
        for _ in 0..=FROZEN_CONTACTS {
            let position = stalls.observe(peer, &their_witnessed, &mine);
            assert_eq!(position, PeerPosition::CaughtUp, "processed everything this node has");
            assert_eq!(
                divergence_probe_for(probe(), position),
                CountGate { probe: probe(), deferred: false }
            );
        }
        assert!(stalls.by_peer.is_empty(), "nothing to remember about a peer that is level");

        // The same peer on the servable vector: what the gate did before,
        // and what it still does for a peer that did not say what it has
        // processed.
        let mut stalls = PeerStalls::new();
        for _ in 0..FROZEN_CONTACTS {
            let position = stalls.observe(peer, &their_servable, &mine);
            assert_eq!(position, PeerPosition::Advancing, "behind on what it can serve");
            assert!(divergence_probe_for(probe(), position).deferred);
        }
        assert_eq!(
            stalls.observe(peer, &their_servable, &mine),
            PeerPosition::Frozen,
            "and then compared for the wrong reason: the position never moves"
        );

        // A peer that has genuinely not processed everything is behind,
        // however much it can serve.
        let mut stalls = PeerStalls::new();
        let trailing = vector(&[(me, 90), (peer, 5)]);
        let position = stalls.observe(peer, &trailing, &mine);
        assert_eq!(position, PeerPosition::Advancing);
        assert_eq!(
            divergence_probe_for(probe(), position),
            CountGate { probe: None, deferred: true }
        );
    }

    /// Which vector the gate is judged on, and the one line each way. A
    /// peer that said what it has processed is judged on that; one that
    /// did not is judged on what it can serve, and the log line fires on
    /// the first such answer and on the first answer that says again,
    /// never in between.
    #[test]
    fn the_gate_is_judged_on_what_the_peer_processed_or_on_what_it_serves_if_it_did_not_say() {
        let peer = node(1);
        let addr: SocketAddr = "127.0.0.1:7900".parse().unwrap();
        let servable = vector(&[(peer, 5)]);
        let witnessed = vector(&[(peer, 5), (node(2), 100)]);
        let mut stalls = PeerStalls::new();

        assert_eq!(stalls.gate_vector(addr, peer, &servable, Some(&witnessed)), &witnessed);
        assert!(stalls.without_witnessed.is_empty());

        assert_eq!(stalls.gate_vector(addr, peer, &servable, None), &servable);
        assert!(stalls.without_witnessed.contains(&peer), "remembered, so the line fires once");
        assert_eq!(stalls.gate_vector(addr, peer, &servable, None), &servable);

        assert_eq!(stalls.gate_vector(addr, peer, &servable, Some(&witnessed)), &witnessed);
        assert!(stalls.without_witnessed.is_empty(), "upgraded: forgotten, so the line fires once");
    }

    /// A peer on one side of the upgrade or the other, over the wire this
    /// round runs on. Both fake peers hold nothing this node lacks and
    /// answer the same count; one answers `AskVersions` with both vectors
    /// and a witnessed position level with this node's, the other — a
    /// version before the flag — with its servable vector alone, which
    /// trails. The first is compared; the second falls back to the old
    /// gate and is deferred, and the round completes either way rather
    /// than failing.
    #[tokio::test]
    async fn a_peer_that_answers_without_its_witnessed_vector_is_gated_on_its_servable_one() {
        use tokio::io::DuplexStream;

        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let orders = engine.create_collection("shop", "orders").unwrap();
        engine.insert(&orders, bson::doc! { "_id": 1 }).unwrap();
        let mine = engine.witnessed_vector().unwrap();
        let me = engine.node_id();
        assert!(mine.get(me) > Hlc::ZERO, "this node has something a peer can trail");
        let probe = Some(DivergenceProbe { id: orders.id, mine_count: Some(1) });
        let their_node = node(9);
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        // A peer that has processed everything this node has but can serve
        // less of it: the entry it processed without appending.
        let mut trailing = VersionVector::new();
        trailing.insert(me, Hlc::new(1, 0));
        let processed = mine.clone();

        async fn fake_peer(mut stream: DuplexStream, answer: Message) {
            match read_frame(&mut stream).await.unwrap() {
                Message::AskVersions { witnessed: true } => {}
                other => panic!("the round opens by asking for both vectors, got {other:?}"),
            }
            write_frame(&mut stream, &answer).await.unwrap();
            match read_frame(&mut stream).await.unwrap() {
                Message::AskDivergence { .. } => {}
                other => panic!("nothing to pull, so the check comes next, got {other:?}"),
            }
            let divergence = Message::Divergence { collections: Vec::new(), probe_count: Some(1) };
            write_frame(&mut stream, &divergence).await.unwrap();
        }

        // Upgraded: both vectors, gated on what it has processed.
        let (ours, theirs) = tokio::io::duplex(MAX_FRAME);
        let answer = Message::Vectors { servable: trailing.clone(), witnessed: processed };
        let peer = tokio::spawn(fake_peer(theirs, answer));
        let mut stalls = PeerStalls::new();
        let outcome = sync_over(&engine, ours, addr, their_node, probe, &mut stalls).await.unwrap();
        peer.await.unwrap();
        assert_eq!(
            outcome.count_probe,
            Some((orders.id, false)),
            "compared, and equal: {outcome:?}"
        );
        assert!(!outcome.count_probe_deferred, "{outcome:?}");

        // Not yet upgraded: the servable vector alone, and the old gate
        // reads the same peer as behind. The round completes.
        let (ours, theirs) = tokio::io::duplex(MAX_FRAME);
        let peer = tokio::spawn(fake_peer(theirs, Message::Versions(trailing)));
        let outcome = sync_over(&engine, ours, addr, their_node, probe, &mut stalls).await.unwrap();
        peer.await.unwrap();
        assert_eq!(outcome.count_probe, None, "deferred under the old gate: {outcome:?}");
        assert!(outcome.count_probe_deferred, "{outcome:?}");
        assert!(stalls.without_witnessed.contains(&their_node), "and noted as answering without");
        assert_eq!(outcome.divergent, Some(std::collections::BTreeSet::new()), "the check ran");
    }
}
