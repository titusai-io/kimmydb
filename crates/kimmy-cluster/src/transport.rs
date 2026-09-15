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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kimmy_core::{CollectionId, Hlc, NodeId, VersionVector};
use kimmy_storage::{Engine, SnapshotPage, SnapshotProgress, SyncOutcome};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;
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

/// How much of a round's [`REQUEST_TIMEOUT`] a snapshot pull leaves unspent
/// before it stops asking for pages (ADR-152).
///
/// The timeout wraps the whole round and cancels it at whatever await point
/// it reaches, which for a snapshot was the middle of a frame: the page in
/// flight was lost, and the next round asked for page one again. A pull now
/// checks the round's deadline *between* pages and asks for no more once it
/// is this close. A page in flight when the deadline passes is still applied
/// — it is one transaction — and the cursor after it recorded before the
/// next frame; what the reserve buys is that the round ends on a page
/// boundary of its own choosing rather than wherever the timeout fell. Five
/// seconds is several pages' worth on a loaded member and a small fraction
/// of the round.
const SNAPSHOT_PAGE_RESERVE: Duration = Duration::from_secs(5);

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
    .map_err(|_| ProtocolError::TimedOut("handshake".into()))??;
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
            Message::AskEntries { from, limit, held, marked } => {
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
                // What the peer has already processed of each origin is not
                // served again: the threshold is one stamp for every origin,
                // and without this a caught-up peer is re-served the whole
                // oplog above its oldest position (ADR-171).
                // Off the worker: passing over what the peer holds can walk the
                // retained oplog to reach the first entry it lacks, in one read
                // transaction (ADR-153).
                let window = kimmy_storage::blocking(|| {
                    engine.entries_for_peer_marked(from, limit, held.as_ref(), &marked)
                })
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
            Message::AskSnapshot { after, collection } => {
                // The whole database, or the one collection a repair named
                // (ADR-152); resumed from wherever the requester says.
                let page = engine
                    .snapshot_page(after, collection)
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                write_frame(&mut stream, &Message::Snapshot(Box::new(page))).await?;
            }
            Message::AskDivergence { probe } => {
                // Metadata only, whatever `probe` is — see
                // `Engine::all_collection_ids`. One walk answers both halves
                // of what this node holds: the ids, and the incarnation each
                // stands at, which is what the requester's tombstones are
                // compared against.
                let held = engine
                    .collection_incarnations()
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                // The one document read in this exchange, bounded to the
                // single collection the requester named (ADR-133).
                let probe_count = match probe {
                    // A walk of that collection, so off the worker (ADR-153).
                    Some(id) => kimmy_storage::blocking(|| engine.count_by_id(id))
                        .map_err(|e| ProtocolError::Malformed(e.to_string()))?,
                    None => None,
                };
                write_frame(
                    &mut stream,
                    &Message::Divergence {
                        collections: held.keys().copied().collect(),
                        probe_count,
                        incarnations: held.into_iter().collect(),
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
                        deferred: outcome.deferred,
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DivergenceProbe {
    pub id: kimmy_core::CollectionId,
    pub mine_count: Option<u64>,
    /// This node's witnessed vector, read in the same snapshot as
    /// `mine_count` — what that count can have seen (ADR-168).
    ///
    /// The count is read once per tick, before the tick's pulls, and the
    /// peer's count is read when the probe reaches it, after them. So this
    /// node's count can predate entries the peer's already covers, and a
    /// comparison of the two on a member draining a backlog reads its own
    /// lag as a divergence. The gate judges that against this vector, not
    /// against the fresher one the round reads after its pull. `None` from a
    /// caller that did not read it, which leaves the gate as it was before
    /// ADR-168: only the peer's side is judged.
    pub mine_at: Option<VersionVector>,
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
/// [`sync_once_with`] and carries a [`PeerStalls`] between rounds — which is
/// also where a snapshot that did not fit the round is left to resume
/// (ADR-152); this call, with no memory, starts one over.
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
        // The window a pull from the member's position would be served, what
        // it has processed passed over (ADR-171).
        // Off the worker, for the reason the served arm gives (ADR-153).
        let mut window = kimmy_storage::blocking(|| {
            engine.entries_for_peer_holding(from, MAX_BATCH, Some(&held))
        })
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
            window = kimmy_storage::blocking(|| {
                engine.entries_for_peer_holding(from, fits, Some(&held))
            })
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
            Message::Pushed {
                applied,
                ddl,
                ddl_refused,
                unknown_collection,
                ddl_declined,
                deferred,
            } => Ok(PushOutcome {
                node: their_node,
                outcome: SyncOutcome {
                    applied,
                    ddl,
                    ddl_refused,
                    unknown_collection,
                    ddl_declined,
                    deferred,
                    peer: Some(their_node),
                    ..SyncOutcome::default()
                },
                unreached: None,
            }),
            Message::Fault(reason) => Err(ProtocolError::Fault(reason)),
            other => Err(ProtocolError::Malformed(format!("expected Pushed, got {other:?}"))),
        }
    };
    tokio::time::timeout(REQUEST_TIMEOUT, exchange)
        .await
        .map_err(|_| ProtocolError::TimedOut("push".into()))?
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
    .map_err(|_| ProtocolError::TimedOut("handshake".into()))??;
    Ok((stream, their_node))
}

/// The body of [`sync_once`], over a connection [`dial`] opened: one round,
/// bounded by [`REQUEST_TIMEOUT`].
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
    // What a snapshot pull checks between pages (ADR-152): the round's own
    // deadline, less the reserve, so the round ends on a page boundary
    // rather than wherever the timeout below cuts it.
    let snapshot_deadline = Instant::now() + REQUEST_TIMEOUT - SNAPSHOT_PAGE_RESERVE;
    tokio::time::timeout(
        REQUEST_TIMEOUT,
        sync_round(engine, &mut stream, peer, their_node, probe, stalls, snapshot_deadline),
    )
    .await
    .map_err(|_| ProtocolError::TimedOut("sync round".into()))?
}

/// One round with `their_node` over `stream`, unbounded: [`sync_over`] puts
/// the timeout around it and hands it `snapshot_deadline`, the instant past
/// which a snapshot pull asks for no more pages this round (ADR-152). Separate
/// so a test can hand it a deadline of its own.
async fn sync_round<S>(
    engine: &Engine,
    stream: &mut S,
    peer: SocketAddr,
    their_node: kimmy_core::NodeId,
    probe: Option<DivergenceProbe>,
    stalls: &mut PeerStalls,
    snapshot_deadline: Instant,
) -> Result<SyncOutcome, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Both of the peer's vectors, on the one frame every round spends
    // anyway (ADR-146): what it can serve drives the pull, and what it
    // has processed drives the count half's gate below. A peer that
    // predates the flag answers with the first alone.
    write_frame(stream, &Message::AskVersions { witnessed: true }).await?;
    let (theirs, their_witnessed) = match read_frame(stream).await? {
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
    let mine = engine.witnessed_vector().map_err(|e| ProtocolError::Malformed(e.to_string()))?;

    // A repair planned against this peer (ADR-148) asks from below
    // this node's position — from the divergent collection's creation,
    // or for the peer's snapshot — where the position alone would say
    // there is nothing to pull. A replay is judged against the peer's
    // horizon by the threshold it asks from rather than by the vector
    // it holds, so a floor below what the peer retains becomes the
    // snapshot it would have been anyway.
    let repair = stalls.repair_due(their_node);
    let replay_floor = match repair {
        Some((_, Repair::Replay { from })) => Some(from),
        _ => None,
    };
    // What this node holds as state at or below its own position (ADR-172).
    // The peer passes over everything that position covers (ADR-171), so
    // these entries would never be served again and their marks would stay
    // until retention. Named as spans, so the peer serves them and ADR-169
    // releases them on arrival: only the part of a span the peer advertises
    // reaching, and not beside a repair, whose replay serves its range whole.
    // Each span resumes past what this peer already served of it, so a span
    // whose bottom the peer cannot serve is walked once rather than re-served
    // from the same place on every pull.
    let marked = if repair.is_none() {
        let spans = kimmy_storage::blocking(|| engine.held_marks_covered_by(&mine))
            .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        let reachable = spans
            .into_iter()
            .filter_map(|(span, marks)| {
                let through = span.through.min(theirs.get(span.origin));
                (through >= span.from)
                    .then_some((kimmy_storage::MarkedRange { through, ..span }, marks))
            })
            .collect();
        stalls.marks_to_ask(their_node, reachable, &theirs, std::time::Instant::now())
    } else {
        Vec::new()
    };
    if stalls.marks_named_changed(their_node, &marked) && !marked.is_empty() {
        info!(
            %peer,
            spans = marked.len(),
            "asking the peer to serve entries this node holds as state below its own position"
        );
    }
    let from = match (mine.behind(&theirs), repair) {
        (behind, Some((_, Repair::Replay { from: floor }))) => {
            Some(behind.map_or(floor, |behind| behind.min(floor)))
        }
        (behind, Some((_, Repair::Snapshot))) => Some(behind.unwrap_or(Hlc::ZERO)),
        (_, None) => entries_threshold(&mine, &theirs, &marked),
    };
    let Some(from) = from else {
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
        //
        // A whole-database snapshot left to resume with this peer is moot:
        // the position says there is nothing below the horizon to pull.
        stalls.snapshot_forgotten(their_node, None);
        let position = stalls.position(their_node, processed, &mine, &theirs, probe.as_ref());
        let gate = divergence_probe_for(probe, position);
        let findings = ask_divergence(engine, stream, gate.probe).await?;
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
    let held = if replay_floor.is_some() { None } else { Some(mine.clone()) };
    let mut answer = match repair {
        Some((collection, Repair::Snapshot)) => {
            if stalls.snapshot_resumes(their_node, Some(collection)) {
                info!(
                    %peer,
                    collection = %collection,
                    "repairing: resuming the peer's snapshot of the collection from where \
                     the last round left it"
                );
            } else {
                warn!(
                    %peer,
                    collection = %collection,
                    "repairing: pulling the peer's snapshot of a collection this node lacks \
                     or holds a confirmed divergence in"
                );
            }
            Message::BeyondHorizon {}
        }
        _ => {
            write_frame(
                stream,
                &Message::AskEntries { from, limit, held: held.clone(), marked: marked.clone() },
            )
            .await?;
            read_frame(stream).await?
        }
    };

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
        write_frame(stream, &Message::AskEntries { from, limit, held, marked: marked.clone() })
            .await?;
        answer = read_frame(stream).await?;
    }

    // Whether this round's own pull reached the peer's true tail — the
    // fact that lets the divergence check also run on a round that
    // pulled something, rather than only on a round that found nothing
    // left to pull (ADR-133). A *completed* snapshot pull earns the same
    // reading — every page the peer had as of the pull, the snapshot's own
    // version of "reached the tail" — and one left to resume for the next
    // round (ADR-152) does not.
    let mut window_exhausted = false;
    // Whether pulling again from this peer at once would carry more
    // (ADR-157): the fact the loop drains a backlog on rather than waiting
    // a whole interval per batch. Narrower than `!window_exhausted` — see
    // where it is set below, and `SyncOutcome::truncated`.
    let mut window_truncated = false;
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
        // (`coverage_up_to`), where it is tested between engines
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
            // The peer serves this node from its oplog, so a whole-database
            // snapshot left to resume with it is moot; a cursor kept past
            // this point would resume a walk the position has moved on from.
            stalls.snapshot_forgotten(their_node, None);
            let last = entries.last().map(|entry| entry.stamp.hlc);
            let outcome = engine
                .apply_peer_batch(&theirs, &entries, scanned_to, exhausted)
                .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
            // The peer's tail was reached if the batch took the whole
            // window up to the vector the peer advertised. An entry
            // deferred above that vector does not change that: it lies
            // past the tail the peer announced, and the divergence check
            // compares against that announced vector, so the check may
            // run. Gating it on nothing deferred would silence the check
            // for as long as writes keep landing between the peer's two
            // reads — exactly the busy cluster the check exists for. A
            // batch stopped at a collection this node lacks did not reach
            // the tail (ADR-148).
            window_exhausted = exhausted && outcome.unknown_collection == 0;
            // And whether the loop should spend another of this tick's
            // pulls here (ADR-157). `exhausted` is the peer's own statement
            // that its scan stopped at the limit with more log behind it,
            // which is what makes a second pull worth making — never the
            // entry count, which is the inference ADR-126 removed. The two
            // conditions beside it are what makes the second pull *move*:
            // a batch that stopped at a collection this node lacks is
            // re-served from the same place and stops at the same entry
            // until the repair planned above brings the collection, and a
            // window whose every entry sat above the vector the peer
            // introduced it with left this node's position exactly where it
            // was. Pulling again on either would spend the tick's budget
            // asking the same question.
            window_truncated =
                !exhausted && outcome.unknown_collection == 0 && outcome.deferred < entries.len();
            // Where each span this request named resumes against this peer
            // (ADR-172). A batch stopped at a collection this node lacks did
            // not take what it carried past the stop, so it moves nothing.
            if !marked.is_empty() && outcome.unknown_collection == 0 {
                stalls.marks_served(
                    their_node,
                    entries.last().map(|entry| entry.stamp),
                    exhausted,
                    std::time::Instant::now(),
                );
            }
            match (repair, &outcome.unknown) {
                // A replay under way: done when it reached the tail,
                // escalated to a snapshot if it stopped at a collection
                // this node lacks — the peer's oplog cannot supply the
                // creation either — and otherwise continued from where
                // the window ended.
                (Some((collection, Repair::Replay { .. })), unknown) => {
                    if unknown.is_some() {
                        stalls.repair_continues(their_node, Repair::Snapshot);
                    } else if window_exhausted {
                        stalls.repair_finished(their_node);
                        info!(
                            %peer,
                            collection = %collection,
                            "repair complete: the peer's oplog was re-served to the tail"
                        );
                    } else {
                        let next = last.unwrap_or(from);
                        stalls.repair_continues(their_node, Repair::Replay { from: next });
                    }
                }
                // A batch stopped at a collection this node does not
                // hold: its creation was witnessed here without being
                // applied, or has aged out of the peer's oplog, and
                // only the peer's snapshot brings it. Planned here, on
                // the strength of the stop itself, rather than left to
                // the divergence check — which cannot run on a round
                // that did not reach the tail, and this one never will
                // until the collection is here.
                (None, Some(unknown))
                    if stalls.plan_repair(their_node, unknown.id, Repair::Snapshot) =>
                {
                    warn!(
                        %peer,
                        collection = %unknown.id,
                        name = unknown.name.as_deref().unwrap_or("unknown"),
                        "planned a snapshot from this peer to repair a collection this \
                         node lacks; the next round with it pulls the snapshot"
                    );
                }
                _ => {}
            }
            Ok(outcome)
        }
        // The peer has collected what we need — or a repair asked for
        // its snapshot outright. Fall back to current state: the whole
        // database for the horizon, the one collection for the repair
        // (ADR-152). A pull that does not finish inside the round leaves
        // its pages applied and its cursor with `stalls`, and the next
        // round with this peer goes on from there.
        Message::BeyondHorizon {} => {
            let scope = match repair {
                Some((collection, Repair::Snapshot)) => Some(collection),
                _ => None,
            };
            if repair.is_none() && !stalls.snapshot_resumes(their_node, None) {
                warn!(%peer, "behind the peer's retention horizon; falling back to a snapshot");
            }
            let pulled =
                pull_snapshot(engine, stream, peer, their_node, scope, stalls, snapshot_deadline)
                    .await?;
            window_exhausted = pulled.complete;
            if let Some((collection, _)) = repair
                && pulled.complete
            {
                stalls.repair_finished(their_node);
                info!(%peer, collection = %collection, "repair complete: snapshot pulled");
            }
            Ok(pulled.outcome)
        }
        other => Err(ProtocolError::Malformed(format!("expected Entries, got {other:?}"))),
    }?;
    outcome.repairing = repair.is_some();

    // How far behind in time this node is after the round: the age of
    // the newest entry it holds from any origin the peer, as of the vector
    // it opened with, holds newer entries of. Zero in the caught-up steady
    // state; while a backlog wider than one batch drains, it grows with
    // the clock — which is what an operator wants a gauge for, and what
    // the span of missing history did not do for a bulk insert whose
    // stamps all lay within a second (ADR-122). `theirs` is a round old
    // by now, so this is a floor — a peer that raced ahead during the
    // round shows up next round.
    let mine = engine.witnessed_vector().map_err(|e| ProtocolError::Malformed(e.to_string()))?;
    outcome.lag_ms = kimmy_storage::lag_behind_ms(&mine, &theirs, kimmy_storage::physical_now_ms());
    outcome.exhausted = window_exhausted;
    outcome.truncated = window_truncated;
    // Only when the pull reached the peer's tail: a round still working
    // through a backlog deeper than one batch has not earned the belief
    // the check depends on, and must not spend a message finding out
    // (ADR-133). This is what lets a busy cluster — many small rounds,
    // each comfortably under the batch cap — still get checked on
    // nearly every round, unlike a single global "nothing to pull" gate,
    // which a continuous trickle of new writes can starve indefinitely.
    if window_exhausted {
        let position = stalls.position(their_node, processed, &mine, &theirs, probe.as_ref());
        let gate = divergence_probe_for(probe, position);
        let findings = ask_divergence(engine, stream, gate.probe).await?;
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

/// The count half's position for a contact, from both sides (ADR-168).
///
/// Either side behind and moving defers: a count read while one side was
/// still taking in entries the other's count had seen is a comparison of two
/// moments, not of two members. Otherwise either side standing still
/// compares — the standing-still rule of ADR-145, now on both sides, so a
/// member that is itself wedged is compared too — and neither side behind
/// compares at once.
fn combine(peer_side: PeerPosition, self_side: PeerPosition) -> PeerPosition {
    use PeerPosition::{Advancing, CaughtUp, Frozen};
    match (peer_side, self_side) {
        (Advancing, _) | (_, Advancing) => Advancing,
        (Frozen, _) | (_, Frozen) => Frozen,
        (CaughtUp, CaughtUp) => CaughtUp,
    }
}

/// What [`divergence_probe_for`] decided: the probe to send, if any, and
/// whether a probe the rotation named was held back this contact.
#[derive(Clone, Debug, PartialEq, Eq)]
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
///
/// Since ADR-148 it also carries the repairs planned against each peer:
/// what to ask the peer for on the next round that the position alone
/// would not ask for, keyed by the collection the repair is on behalf of.
/// Kept here for the same reason as the stall memo — it is read and written
/// inside the round, against the peer the round is with.
#[derive(Debug, Default)]
pub struct PeerStalls {
    by_peer: HashMap<NodeId, Stall>,
    /// Where this node stood behind each peer when its probe count was read,
    /// on the origins it then trailed that peer on (ADR-168) — the mirror of
    /// `by_peer`, so a member that is itself draining a backlog is not read
    /// as holding a different count from a member that has more of it.
    by_self: HashMap<NodeId, Stall>,
    /// Peers whose last answer carried no witnessed vector — a version
    /// before ADR-146 — so the gate is judged on their servable one, and
    /// the log says so once rather than every round.
    without_witnessed: HashSet<NodeId>,
    repairs: HashMap<NodeId, Repairs>,
    /// Snapshot pulls left to resume, one per peer (ADR-152): where the next
    /// page begins and what the completed snapshot grants, carried across
    /// rounds so a snapshot that does not fit one round goes on from its
    /// last page rather than page one. Beside the repairs for the reason
    /// they are here — written inside the round, against the peer the round
    /// is with — and written after every page, before the next frame, so a
    /// round the timeout cancels leaves the pages it applied recorded.
    snapshots: HashMap<NodeId, SnapshotProgress>,
    /// Where each held span resumes against each peer (ADR-172), keyed by
    /// peer and origin: see [`PeerStalls::marks_to_ask`].
    marks_asked: HashMap<(NodeId, NodeId), SpanAsked>,
    /// The spans the last request to each peer named, with how many marks
    /// each covered, until [`PeerStalls::marks_served`] records the answer.
    marks_pending: HashMap<NodeId, Vec<(kimmy_storage::MarkedRange, usize, Hlc)>>,
    /// The spans each peer was last named in the log, by origin and upper
    /// bound, so the line is written when they change and not on every pull.
    marks_logged: HashMap<NodeId, Vec<(NodeId, Hlc)>>,
}

/// Where one origin's held span resumes against one peer (ADR-172).
#[derive(Clone, Copy, Debug)]
struct SpanAsked {
    /// Where the next request for this origin's span starts: past every entry
    /// of it the peer has served, or past its top once a window reached the
    /// peer's tail.
    resume: Hlc,
    /// The marks the span covered when this was recorded. More marks since
    /// can mean a held entry below `resume`, so the record is dropped.
    marks: usize,
    /// What the peer advertised of the span's origin when this was recorded.
    /// A peer that has moved on that origin since may now hold what it
    /// lacked, so the record is dropped.
    theirs: Hlc,
    /// When the record last moved.
    at: std::time::Instant,
}

/// How long a span's resume point stands before the span is asked from its
/// bottom again (ADR-172), when nothing else has reopened it. A peer that
/// answered a span to its tail without holding one of its entries reopens it
/// by moving on the span's origin, which is how most peers come to hold such
/// an entry. One that takes the entry below its own position on an origin
/// that has gone quiet moves nothing this node can see; five minutes, the
/// repair cooldown's length at the default interval, bounds how long that
/// release waits and how often a span no peer can serve is walked.
pub const MARKS_REASK_AFTER: Duration = Duration::from_secs(300);

/// The stamp a pull asks from, when no repair is planned (ADR-171, ADR-172).
///
/// This node's threshold on the peer, as `VersionVector::behind` gives it.
/// **Never lowered for a span**: a sender that reads `marked` lowers its own
/// scan start to the spans, and one that predates the field would serve every
/// entry from a lowered stamp, the drain ADR-171 removed. When this node is
/// behind on nothing but names spans, the highest stamp the peer advertises:
/// a sender that reads `marked` serves the spans, and one that does not
/// serves the entries at or above that stamp, which this node already holds.
/// `None` when there is nothing to ask.
pub fn entries_threshold(
    mine: &VersionVector,
    theirs: &VersionVector,
    marked: &[kimmy_storage::MarkedRange],
) -> Option<Hlc> {
    mine.behind(theirs).or_else(|| {
        (!marked.is_empty()).then(|| theirs.iter().map(|(_, hlc)| hlc).max().unwrap_or(Hlc::ZERO))
    })
}

/// How a round repairs a hole against one peer (ADR-148).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Repair {
    /// Ask the peer for its oplog from this stamp — below this node's
    /// position — window by window until a window reaches the peer's tail.
    /// Everything this node already holds is superseded on the way; what it
    /// lacks lands. A floor the peer no longer retains becomes a snapshot.
    Replay { from: Hlc },
    /// Pull the peer's snapshot: current state, collections included. What
    /// a collection this node lacks outright needs, since the oplog window
    /// that carries its documents has already lost its creation.
    Snapshot,
}

/// The repairs planned against one peer.
#[derive(Debug, Default)]
struct Repairs {
    /// The repair in progress, and the collection it is for.
    active: Option<(CollectionId, Repair)>,
    /// Consecutive rounds the repair in progress has been handed out for
    /// without making progress — a round that failed, or a snapshot round
    /// that applied no page. Counted when the next round opens, from
    /// `advanced`; reset by a round that advanced. At [`REPAIR_ATTEMPTS`]
    /// the repair is abandoned and takes the cooldown, so a repair that
    /// cannot complete costs the same as one that can rather than retrying
    /// every round for the life of the process.
    stalled: u32,
    /// Whether the round last handed the repair advanced it — a replay's
    /// window moved, a snapshot's page landed (ADR-152). Read and cleared
    /// when the next round opens, so that round is the first of a new run
    /// rather than the second: the count is incremented before a round
    /// runs, because a round that fails never returns here, and a reset
    /// inside the round would otherwise leave the advancing round counted.
    advanced: bool,
    /// Planned behind it, one per collection.
    queued: BTreeMap<CollectionId, Repair>,
    /// Collections repaired against this peer, with the contacts since: not
    /// repaired again until the check reports them clear, or
    /// [`REPAIR_COOLDOWN_ROUNDS`] contacts have passed — so a divergence a
    /// repair cannot close costs one repair per cooldown, not one per
    /// contact.
    done: BTreeMap<CollectionId, u32>,
    /// Whether this tick's contact with the peer has already been counted
    /// against the cooldowns above (ADR-157). Cleared by
    /// [`PeerStalls::tick_opened`].
    ///
    /// A tick makes several pulls at a peer while it is draining a backlog,
    /// and each one asks [`PeerStalls::repair_due`] what to run — but the
    /// cooldown is a wall-clock measure wearing a round's clothing, sixty
    /// rounds being five minutes at the default interval. Counted per pull
    /// it would expire in seconds during exactly the backlog a repair must
    /// not be competing with for the tick's budget.
    counted: bool,
}

/// Contacts with a peer after which a collection repaired against it may be
/// repaired again without the check having reported it clear in between
/// (ADR-148). A contact is a peer per sync tick, however many pulls the tick
/// made at it (ADR-157). Sixty is five minutes at the default interval: long
/// enough
/// that a divergence a repair cannot close — a definition this build
/// refuses, say — does not cost a replay every round, short enough that a
/// hole reopened after a repair is not left for the life of the process.
pub const REPAIR_COOLDOWN_ROUNDS: u32 = 60;

/// Rounds a repair may be handed out for without advancing before it is
/// abandoned and takes the cooldown (ADR-148).
///
/// A replay advances every round that completes — its window moves — and a
/// snapshot advances on every page it applies, across rounds (ADR-152), so
/// a repair that does not advance is one whose round keeps failing before a
/// page lands. Before ADR-152 a snapshot advanced only by completing inside
/// `REQUEST_TIMEOUT`, and one of a large database was exactly the repair
/// that could keep not finishing; now it is abandoned only when three
/// rounds in a row apply nothing.
/// Three, for the reason [`FROZEN_CONTACTS`] is three: one is a peer
/// having a bad moment, three in a row is a repair that is not going to
/// complete on this route, and the cooldown then keeps it to one attempt
/// every few minutes instead of one every round.
pub const REPAIR_ATTEMPTS: u32 = 3;

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

    /// The held spans to name to `peer` on this pull (ADR-172), from `current`,
    /// the spans this node holds with how many marks each covers.
    ///
    /// Each span starts where it resumes against this peer: past what the
    /// peer's earlier windows served of it. A span resumed past its top is
    /// left out. That is the peer having served everything it retains inside
    /// it, and asking again would re-walk its oplog to find the rest absent.
    /// The record is dropped, and the span asked from its bottom, when this
    /// node now holds more marks on that origin than when it was recorded,
    /// when the peer now advertises more of that origin than it did then, or
    /// [`MARKS_REASK_AFTER`] after the record last moved. Origins no longer
    /// held are forgotten.
    pub fn marks_to_ask(
        &mut self,
        peer: NodeId,
        current: Vec<(kimmy_storage::MarkedRange, usize)>,
        theirs: &VersionVector,
        now: std::time::Instant,
    ) -> Vec<kimmy_storage::MarkedRange> {
        self.marks_asked.retain(|(asked_of, origin), _| {
            *asked_of != peer || current.iter().any(|(span, _)| span.origin == *origin)
        });
        let mut named = Vec::new();
        for (span, marks) in &current {
            let key = (peer, span.origin);
            if self.marks_asked.get(&key).is_some_and(|asked| {
                *marks > asked.marks
                    || theirs.get(span.origin) > asked.theirs
                    || now.saturating_duration_since(asked.at) >= MARKS_REASK_AFTER
            }) {
                self.marks_asked.remove(&key);
            }
            let from = match self.marks_asked.get_mut(&key) {
                Some(asked) => {
                    asked.marks = *marks;
                    asked.resume.max(span.from)
                }
                None => span.from,
            };
            if from <= span.through {
                named.push(kimmy_storage::MarkedRange { from, ..*span });
            }
        }
        let pending = named
            .iter()
            .map(|span| {
                let marks =
                    current.iter().find(|(c, _)| c.origin == span.origin).map_or(0, |c| c.1);
                (*span, marks, theirs.get(span.origin))
            })
            .collect();
        self.marks_pending.insert(peer, pending);
        named
    }

    /// Record where the spans the last request to `peer` named resume, from
    /// the window it answered with (ADR-172): past the window's last entry,
    /// for a window truncated at the cap, and past each span's top for one
    /// that reached the peer's tail. The last entry's full stamp decides the
    /// tie, as the coverage rule's does (ADR-148): an origin that sorts after
    /// it at the same timestamp was not examined, and resumes at it.
    pub fn marks_served(
        &mut self,
        peer: NodeId,
        last: Option<kimmy_core::Stamp>,
        exhausted: bool,
        now: std::time::Instant,
    ) {
        let Some(pending) = self.marks_pending.remove(&peer) else {
            return;
        };
        for (span, marks, theirs) in pending {
            let resume = if exhausted {
                span.through.successor()
            } else {
                match last {
                    Some(last) if span.origin <= last.node => last.hlc.successor().max(span.from),
                    Some(last) => last.hlc.max(span.from),
                    None => span.from,
                }
            };
            self.marks_asked
                .insert((peer, span.origin), SpanAsked { resume, marks, theirs, at: now });
        }
    }

    /// Whether the spans named to `peer` differ, by origin and upper bound,
    /// from the last ones this answered for (ADR-172). What the log line is
    /// written on, so a span walked a window at a time is logged once.
    pub fn marks_named_changed(
        &mut self,
        peer: NodeId,
        named: &[kimmy_storage::MarkedRange],
    ) -> bool {
        let now: Vec<(NodeId, Hlc)> =
            named.iter().map(|span| (span.origin, span.through)).collect();
        let before = self.marks_logged.insert(peer, now.clone()).unwrap_or_default();
        before != now
    }

    /// Take up the snapshot pulls the engine recorded before this process
    /// started (ADR-161), so one interrupted by a restart resumes at its last
    /// recorded page rather than at page one.
    ///
    /// Only pulls that are unfinished and have applied a page are taken: a
    /// completed one has nothing to resume, and one that recorded no page is
    /// the same as no record at all. `snapshot_progress` already drops a
    /// resumed pull whose scope is not the one a round wants, so a stale
    /// record cannot redirect a round -- the worst it costs is being
    /// discarded.
    pub fn resume_snapshots(&mut self, recorded: Vec<(NodeId, SnapshotProgress)>) {
        for (peer, progress) in recorded {
            if progress.is_complete() || progress.pages() == 0 {
                continue;
            }
            info!(
                %peer,
                pages = progress.pages(),
                documents = progress.documents(),
                cursor = ?progress.after().map(ToString::to_string),
                "resuming a snapshot pull this node was part-way through"
            );
            self.snapshots.insert(peer, progress);
        }
    }

    /// A sync tick has begun, so the next pull at each peer opens that
    /// peer's contact for this tick (ADR-157).
    ///
    /// What [`Self::repair_due`] counts against [`REPAIR_COOLDOWN_ROUNDS`]
    /// is contacts, and a tick's several pulls at one peer are one contact;
    /// this is what tells them apart. The loop calls it once per tick,
    /// before the tick's first pull.
    pub fn tick_opened(&mut self) {
        for repairs in self.repairs.values_mut() {
            repairs.counted = false;
        }
    }

    /// Plan `repair` against `peer` on behalf of `collection` (ADR-148).
    /// `false` when one is already planned or under way for it, or one ran
    /// and the finding has not cleared or cooled down since.
    pub fn plan_repair(&mut self, peer: NodeId, collection: CollectionId, repair: Repair) -> bool {
        let repairs = self.repairs.entry(peer).or_default();
        if repairs.done.contains_key(&collection)
            || repairs.active.is_some_and(|(id, _)| id == collection)
            || repairs.queued.contains_key(&collection)
        {
            return false;
        }
        repairs.queued.insert(collection, repair);
        true
    }

    /// The repair the round with `peer` now opening runs, if any: the one
    /// under way, or the next planned. Also counts the round against every
    /// repair done for this peer, for the cooldown, and against the repair
    /// under way, which is abandoned to the cooldown after
    /// [`REPAIR_ATTEMPTS`] rounds that did not advance it.
    pub fn repair_due(&mut self, peer: NodeId) -> Option<(CollectionId, Repair)> {
        let repairs = self.repairs.get_mut(&peer)?;
        // Once per contact, not once per pull: a draining tick asks this
        // several times, and the cooldown below is a constant argued in
        // minutes (ADR-157). The stall accounting under it stays per pull,
        // because a pull *is* a round the repair was handed out for.
        if !std::mem::replace(&mut repairs.counted, true) {
            repairs.done.retain(|_, contacts| {
                *contacts += 1;
                *contacts < REPAIR_COOLDOWN_ROUNDS
            });
        }
        // A repair handed out on the previous round and neither advanced
        // nor finished since: the round failed, or the snapshot landed no
        // page. Counted here rather than at the failure, because a round
        // that fails never returns to this module at all; a round that
        // advanced says so and starts the run again, so exactly
        // `REPAIR_ATTEMPTS` rounds landing nothing abandon a repair whether
        // or not it had advanced before them.
        if repairs.active.is_some() {
            if std::mem::take(&mut repairs.advanced) {
                repairs.stalled = 0;
            } else {
                repairs.stalled += 1;
            }
            if repairs.stalled >= REPAIR_ATTEMPTS
                && let Some((collection, _)) = repairs.active.take()
            {
                warn!(
                    node = %peer,
                    collection = %collection,
                    attempts = REPAIR_ATTEMPTS,
                    "a repair against this peer has not advanced; abandoning it until the \
                     divergence check reports the collection again or the cooldown passes"
                );
                repairs.done.insert(collection, 0);
                repairs.stalled = 0;
                repairs.advanced = false;
                // Whatever of its snapshot was pulled is kept — the pages
                // are applied — but not resumed: the next repair of the
                // collection, after the cooldown, starts over.
                if self.snapshots.get(&peer).is_some_and(|p| p.scope() == Some(collection)) {
                    self.snapshots.remove(&peer);
                }
            }
        }
        if repairs.active.is_none() {
            repairs.active = repairs.queued.pop_first();
            repairs.stalled = 0;
            repairs.advanced = false;
        }
        repairs.active
    }

    /// Whether a snapshot pull scoped to `scope` was left to resume with
    /// `peer`: one that applied at least a page and has not completed.
    fn snapshot_resumes(&self, peer: NodeId, scope: Option<CollectionId>) -> bool {
        self.snapshots
            .get(&peer)
            .is_some_and(|p| p.scope() == scope && p.pages() > 0 && !p.is_complete())
    }

    /// The snapshot pull with `peer` scoped to `scope`: the one left to
    /// resume, or a fresh one. A pull left over for another scope is
    /// dropped — a round pulls one snapshot, and the one it wants is this.
    fn snapshot_progress(
        &mut self,
        peer: NodeId,
        scope: Option<CollectionId>,
    ) -> &mut SnapshotProgress {
        let fresh = || match scope {
            Some(id) => SnapshotProgress::of_collection(id),
            None => SnapshotProgress::whole_database(),
        };
        let progress = self.snapshots.entry(peer).or_insert_with(fresh);
        if progress.scope() != scope || progress.is_complete() {
            *progress = fresh();
        }
        progress
    }

    /// A page of the snapshot with `peer` was applied: the repair it
    /// serves, if it serves one, advanced, and its stall run starts again —
    /// exactly as a replay's does when its window moves (ADR-152).
    fn snapshot_advanced(&mut self, peer: NodeId) {
        if let Some(repairs) = self.repairs.get_mut(&peer)
            && matches!(repairs.active, Some((_, Repair::Snapshot)))
        {
            repairs.advanced = true;
        }
    }

    /// The snapshot pull with `peer` is complete: nothing to resume.
    fn snapshot_done(&mut self, peer: NodeId) {
        self.snapshots.remove(&peer);
    }

    /// Forget a snapshot pull scoped to `scope` left to resume with `peer`,
    /// if there is one. The pages it applied stay applied; what is dropped
    /// is the cursor, for a round that showed the pull is no longer the
    /// one wanted.
    fn snapshot_forgotten(&mut self, peer: NodeId, scope: Option<CollectionId>) {
        if self.snapshots.get(&peer).is_some_and(|p| p.scope() == scope) {
            self.snapshots.remove(&peer);
        }
    }

    /// The repair under way against `peer` continues next round as `next`.
    fn repair_continues(&mut self, peer: NodeId, next: Repair) {
        if let Some(repairs) = self.repairs.get_mut(&peer)
            && let Some(active) = &mut repairs.active
        {
            active.1 = next;
            // It advanced, so the stall run starts again at the next round.
            repairs.advanced = true;
        }
    }

    /// The repair under way against `peer` is done.
    fn repair_finished(&mut self, peer: NodeId) {
        if let Some(repairs) = self.repairs.get_mut(&peer)
            && let Some((collection, _)) = repairs.active.take()
        {
            repairs.done.insert(collection, 0);
            repairs.stalled = 0;
            // The page that finished it must not be read as advancing the
            // next repair planned for this peer.
            repairs.advanced = false;
            if self.snapshots.get(&peer).is_some_and(|p| p.scope() == Some(collection)) {
                self.snapshots.remove(&peer);
            }
        }
    }

    /// Forget repairs done against `peer` for collections the check no
    /// longer reports against it, so a finding that comes back is repaired
    /// again rather than waiting out the cooldown.
    pub fn retain_repaired(&mut self, peer: NodeId, still_reported: &BTreeSet<CollectionId>) {
        if let Some(repairs) = self.repairs.get_mut(&peer) {
            repairs.done.retain(|collection, _| still_reported.contains(collection));
        }
    }

    /// Whether a repair is planned or under way against `peer`.
    pub fn repairing(&self, peer: NodeId) -> bool {
        self.repairs
            .get(&peer)
            .is_some_and(|repairs| repairs.active.is_some() || !repairs.queued.is_empty())
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

    /// Where this checked contact with `peer` stands for the count half, on
    /// both sides (ADR-168): the peer's position behind this node
    /// ([`Self::observe`]) and this node's position behind the peer when its
    /// count was read ([`Self::observe_self`]), folded by [`combine`].
    ///
    /// `processed` is the vector the peer's side is judged on
    /// ([`Self::gate_vector`]); `mine` is this node's witnessed vector just
    /// before the check; `servable` is what the peer said it can serve when
    /// the contact opened; `probe` carries the vector this node's count was
    /// read at.
    pub fn position(
        &mut self,
        peer: NodeId,
        processed: &VersionVector,
        mine: &VersionVector,
        servable: &VersionVector,
        probe: Option<&DivergenceProbe>,
    ) -> PeerPosition {
        let theirs = self.observe(peer, processed, mine);
        let ours = self.observe_self(peer, servable, probe.and_then(|p| p.mine_at.as_ref()));
        combine(theirs, ours)
    }

    /// Fold in where this node stood when its probe count was read, against
    /// what the peer could serve when this contact opened, and say whether
    /// this node was behind the peer and still moving (ADR-168).
    ///
    /// **The mirror of [`Self::observe`], on the other pair of vectors.** The
    /// peer's side asks whether the peer has *processed* everything this node
    /// has, so it is judged witnessed against witnessed (ADR-146). This side
    /// asks whether this node's count had seen everything the peer's count
    /// can have — whether this node had processed everything the peer can
    /// *serve* — which is the pull's own question, `mine.behind(&theirs)`:
    /// this node's witnessed vector against the peer's servable one. Judged
    /// on the peer's *witnessed* vector instead, an entry the peer processed
    /// without appending — which it can never send, so this node processes it
    /// only by pulling from its origin — would read as a position this node
    /// trails on until then, and
    /// an idle cluster would defer the count against that peer for
    /// [`FROZEN_CONTACTS`] contacts after every such entry: the trickle
    /// ADR-146 removed, reintroduced in reverse.
    ///
    /// The peer's local writes enter the map: they are exactly what this node
    /// trails a writing peer on. So can this node's own origin, when a local
    /// write made after `mine_at` was read has already reached the peer; that
    /// defers, which is right, since the count predates that write too.
    ///
    /// A still position is judged as the peer's is — unchanged on
    /// [`FROZEN_CONTACTS`] consecutive checked contacts compares — and the sync
    /// loop does not reach it: a checked contact leaves this node covering the
    /// peer's servable vector, and the next tick re-reads `mine_at`, so the run
    /// restarts. A member whose inbound replication has stopped is caught by
    /// its peers' side (ADR-145). The rule is kept so a self side reading the
    /// same position on consecutive contacts compares rather than defers
    /// indefinitely; it compares only where the gate before ADR-168 did. `None` —
    /// no vector read with the count — forgets any memo and answers
    /// [`PeerPosition::CaughtUp`], which leaves the decision to the peer's
    /// side alone, as before ADR-168.
    pub fn observe_self(
        &mut self,
        peer: NodeId,
        servable: &VersionVector,
        mine_at: Option<&VersionVector>,
    ) -> PeerPosition {
        let Some(mine_at) = mine_at else {
            self.by_self.remove(&peer);
            return PeerPosition::CaughtUp;
        };
        let trailing: BTreeMap<NodeId, Hlc> = servable
            .iter()
            .filter(|(origin, theirs_at)| mine_at.get(*origin) < *theirs_at)
            .map(|(origin, _)| (origin, mine_at.get(origin)))
            .collect();
        if trailing.is_empty() {
            self.by_self.remove(&peer);
            return PeerPosition::CaughtUp;
        }

        let stall = self.by_self.entry(peer).or_default();
        let still = !stall.trailing.is_empty()
            && stall.trailing.iter().all(|(origin, at)| mine_at.get(*origin) == *at);
        stall.unchanged = if still { stall.unchanged.saturating_add(1) } else { 0 };
        stall.trailing = trailing;
        // Defensive: the sync loop does not reach `Frozen` here. `peers` re-reads
        // `mine_at` every tick, and a checked contact leaves this node covering
        // the peer's servable vector, so on the next checked contact every
        // trailing origin has moved and the run starts again. A member whose
        // inbound replication has stopped makes no checked contacts at all; its
        // PEERS' side catches it (ADR-145). Kept so a self side read at the same
        // position contact after contact compares rather than defers for ever.
        if stall.unchanged >= FROZEN_CONTACTS {
            PeerPosition::Frozen
        } else {
            PeerPosition::Advancing
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
    // Read on the same rhythm and for the same reason: one row per dropped
    // collection, no document. Without it a collection this node destroyed
    // on purpose reads as one it is merely missing, and the repair the
    // finding plans pulls it back from whichever peer has not applied the
    // drop yet.
    let mine_dropped =
        engine.collection_tombstones().map_err(|e| ProtocolError::Malformed(e.to_string()))?;

    write_frame(stream, &Message::AskDivergence { probe: probe.as_ref().map(|p| p.id) }).await?;
    let (peer_collections, probe_count, peer_incarnations) = match read_frame(stream).await? {
        Message::Divergence { collections, probe_count, incarnations } => {
            (collections, probe_count, incarnations)
        }
        Message::Fault(reason) => return Err(ProtocolError::Fault(reason)),
        other => {
            return Err(ProtocolError::Malformed(format!("expected Divergence, got {other:?}")));
        }
    };

    let mine = kimmy_storage::DivergenceLocalState {
        collections: mine_collections,
        probe: probe.map(|p| (p.id, p.mine_count)),
        dropped: mine_dropped,
    };
    let peer = kimmy_storage::DivergencePeerAnswer {
        collections: peer_collections.into_iter().collect(),
        probe_count,
        incarnations: peer_incarnations.into_iter().collect(),
    };
    Ok(kimmy_storage::compare_divergence(&mine, &peer))
}

/// What one round's snapshot pull came to (ADR-152).
struct SnapshotPulled {
    /// What applying the pages changed, so a caller sees a snapshot the
    /// same way it sees an incremental round.
    outcome: SyncOutcome,
    /// Whether the final page landed. `false` is a pull left to resume: its
    /// pages stay applied, its cursor is with the [`PeerStalls`], and the
    /// next round with the peer goes on from it.
    complete: bool,
}

/// Whether a page answering a snapshot scoped to `id` carries anything of
/// another collection — what a sender that predates the scope serves.
fn served_beyond_scope(page: &SnapshotPage, id: CollectionId) -> bool {
    page.documents.iter().any(|d| d.collection != id)
        || page.collections.iter().any(|c| CollectionId::derive(&c.db, &c.name) != id)
}

/// Pull a snapshot, page by page, until the peer says it is complete or the
/// round's budget is spent (ADR-152).
///
/// `scope` is the one collection to pull, for a repair (ADR-148), or `None`
/// for the whole database a member below the peer's retention horizon
/// needs. Where the pull stands is kept in `stalls`, against `node`, and is
/// moved after every page *before* the next is asked for: a page applied is
/// one transaction, so a round the timeout cancels at the next frame leaves
/// every page it applied recorded, and the next round asks for the page
/// after them rather than page one. `deadline` is checked between pages,
/// never inside one — a page in flight when it passes is finished and
/// recorded. Every page applied is the repair advancing
/// ([`PeerStalls::snapshot_advanced`]), so a slow-but-moving snapshot is
/// never abandoned as stalled; [`REPAIR_ATTEMPTS`] counts rounds that
/// applied nothing.
async fn pull_snapshot<S>(
    engine: &Engine,
    stream: &mut S,
    peer: SocketAddr,
    node: NodeId,
    scope: Option<CollectionId>,
    stalls: &mut PeerStalls,
    deadline: Instant,
) -> Result<SnapshotPulled, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut outcome = SyncOutcome::default();
    let mut pages = 0usize;
    // Documents the pages carried that were history here (`is_history`), or
    // whose collection this node holds a tombstone for. Kept for the log
    // rather than folded into the round's outcome: it says a repair that
    // wrote nothing did so because everything on it was already buried, not
    // because it stalled, and reading it as the round's `superseded` would
    // count on this route what only part of the entries path counts on that
    // one.
    let mut superseded = 0usize;
    let resumed = stalls.snapshot_resumes(node, scope);
    let mut whole_from_older_peer = false;

    loop {
        let after = stalls.snapshot_progress(node, scope).after().cloned();
        write_frame(stream, &Message::AskSnapshot { after, collection: scope }).await?;
        let page = match read_frame(stream).await? {
            Message::Snapshot(page) => page,
            Message::Fault(reason) => return Err(ProtocolError::Fault(reason)),
            other => {
                return Err(ProtocolError::Malformed(format!("expected Snapshot, got {other:?}")));
            }
        };

        // A sender before the field ignores the scope and serves its whole
        // database. Applied as it comes: that is the snapshot a repair
        // pulled before ADR-152, correct and only dearer, and it grants no
        // coverage under a scope either. Said once a round, because the log
        // is where an operator learns the roll is not finished.
        if let Some(id) = scope
            && !whole_from_older_peer
            && served_beyond_scope(&page, id)
        {
            whole_from_older_peer = true;
            info!(
                %peer,
                collection = %id,
                "the peer answered a snapshot of one collection with its whole database (a \
                 version before the field); applying it as it comes"
            );
        }

        // Applied, and the progress moved, before anything else is awaited:
        // this is the point a cancelled round resumes from.
        let progress = stalls.snapshot_progress(node, scope);
        let before = progress.pages();
        let applied = engine.apply_snapshot_page(node, progress, &page);
        let advanced = progress.pages() > before;
        let complete = progress.is_complete();
        let (total_pages, total_documents) = (progress.pages(), progress.documents());
        let cursor = progress.after().map(ToString::to_string);
        if advanced {
            stalls.snapshot_advanced(node);
            pages += 1;
        }
        let applied = applied.map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        outcome.applied += applied.applied;
        // Counted where a refusal reached through the oplog is, so the
        // metric and the round report do not depend on the route (ADR-123).
        outcome.ddl_refused += applied.ddl_refused;
        superseded += applied.superseded;

        if complete {
            stalls.snapshot_done(node);
            info!(
                %peer,
                documents = outcome.applied,
                superseded,
                pages,
                total_pages,
                total_documents,
                resumed,
                "caught up from a snapshot"
            );
            return Ok(SnapshotPulled { outcome, complete: true });
        }
        if Instant::now() >= deadline {
            info!(
                %peer,
                pages,
                documents = outcome.applied,
                superseded,
                total_pages,
                total_documents,
                cursor = cursor.as_deref().unwrap_or("-"),
                "snapshot left to resume on the next round with this peer: the round's \
                 budget is spent"
            );
            return Ok(SnapshotPulled { outcome, complete: false });
        }
    }
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
        Some(DivergenceProbe {
            id: kimmy_core::CollectionId(7),
            mine_count: Some(10),
            mine_at: None,
        })
    }

    /// The resume points `sync_round` names held spans through (ADR-172). A
    /// span is named from its bottom; after a truncated window it resumes past
    /// the window's last entry, with the full stamp deciding a tie; after a
    /// window that reached the tail it is left out; and it is named from its
    /// bottom again when more marks are held on its origin, when the record is
    /// older than `MARKS_REASK_AFTER`, and of a different peer. Origins no
    /// longer held are forgotten, and a request answered by anything but a
    /// window moves nothing.
    #[test]
    fn peer_stalls_resume_each_span_past_what_the_peer_served() {
        let peer = node(1);
        let other = node(2);
        let origin = node(3);
        let at = |wall: u64| kimmy_storage::MarkedRange {
            origin,
            from: Hlc::new(wall, 0),
            through: Hlc::new(100, 0),
        };
        let stamp = |wall: u64, n: u128| kimmy_core::Stamp::new(Hlc::new(wall, 0), node(n));
        let t0 = std::time::Instant::now();
        let theirs = vector(&[(origin, 100)]);
        let mut stalls = PeerStalls::new();

        assert_eq!(
            stalls.marks_to_ask(peer, vec![(at(10), 2)], &theirs, t0),
            vec![at(10)],
            "first"
        );
        // A window truncated at the cap, ending at an entry of this origin.
        stalls.marks_served(peer, Some(stamp(40, 3)), false, t0);
        let resumed = stalls.marks_to_ask(peer, vec![(at(10), 2)], &theirs, t0);
        assert_eq!(
            resumed,
            vec![at(40)]
                .into_iter()
                .map(|s| kimmy_storage::MarkedRange { from: Hlc::new(40, 0).successor(), ..s })
                .collect::<Vec<_>>(),
            "resumes past the last entry"
        );
        // Truncated at an entry of an origin that sorts before this one, at
        // the same timestamp: this origin's entry there was not examined.
        stalls.marks_served(peer, Some(stamp(60, 2)), false, t0);
        assert_eq!(
            stalls.marks_to_ask(peer, vec![(at(10), 2)], &theirs, t0),
            vec![at(60)],
            "a tie resumes at it"
        );
        // Answered by a snapshot or a failed round: nothing recorded, so the
        // pending request is simply named again.
        assert_eq!(stalls.marks_to_ask(peer, vec![(at(10), 2)], &theirs, t0), vec![at(60)]);
        // A window that reached the peer's tail.
        stalls.marks_served(peer, Some(stamp(200, 3)), true, t0);
        assert!(
            stalls.marks_to_ask(peer, vec![(at(10), 2)], &theirs, t0).is_empty(),
            "answered: left out"
        );
        assert_eq!(
            stalls.marks_to_ask(peer, vec![(at(10), 2)], &vector(&[(origin, 101)]), t0),
            vec![at(10)],
            "the peer moved on the span's origin: named from the bottom again"
        );
        stalls.marks_served(peer, None, true, t0);
        assert!(
            stalls.marks_to_ask(peer, vec![(at(10), 1)], &theirs, t0).is_empty(),
            "a mark released since is still answered"
        );
        assert_eq!(
            stalls.marks_to_ask(peer, vec![(at(10), 3)], &theirs, t0),
            vec![at(10)],
            "more marks than recorded: named from the bottom again"
        );
        stalls.marks_served(peer, None, true, t0);
        assert!(stalls.marks_to_ask(peer, vec![(at(10), 3)], &theirs, t0).is_empty());
        assert_eq!(
            stalls.marks_to_ask(peer, vec![(at(10), 3)], &theirs, t0 + MARKS_REASK_AFTER),
            vec![at(10)],
            "and after the record expires"
        );
        assert_eq!(
            stalls.marks_to_ask(other, vec![(at(10), 3)], &theirs, t0),
            vec![at(10)],
            "another peer"
        );
        stalls.marks_served(peer, None, true, t0);
        assert!(stalls.marks_to_ask(peer, Vec::new(), &theirs, t0).is_empty(), "nothing held");
        assert_eq!(
            stalls.marks_to_ask(peer, vec![(at(10), 3)], &theirs, t0),
            vec![at(10)],
            "and a span held again after that is named from its bottom"
        );
    }

    /// The log line for held spans is written when they change by origin and
    /// upper bound, not as a span walks a window at a time (ADR-172).
    #[test]
    fn the_spans_named_are_logged_when_they_change() {
        let peer = node(1);
        let span = |from: u64, through: u64| kimmy_storage::MarkedRange {
            origin: node(3),
            from: Hlc::new(from, 0),
            through: Hlc::new(through, 0),
        };
        let mut stalls = PeerStalls::new();
        assert!(stalls.marks_named_changed(peer, &[span(10, 100)]));
        assert!(!stalls.marks_named_changed(peer, &[span(40, 100)]), "resumed, same span");
        assert!(stalls.marks_named_changed(peer, &[span(40, 90)]), "a new top");
        assert!(stalls.marks_named_changed(peer, &[]));
        assert!(!stalls.marks_named_changed(peer, &[]));
    }

    /// The request's `from` is never lowered for a span (ADR-172): the
    /// threshold when behind, the peer's newest stamp when behind on nothing
    /// and naming spans, and nothing otherwise.
    #[test]
    fn a_pull_asks_from_its_threshold_and_never_from_a_span() {
        let me = node(1);
        let peer = node(2);
        let span = [kimmy_storage::MarkedRange {
            origin: me,
            from: Hlc::new(1, 0),
            through: Hlc::new(2, 0),
        }];
        let mine = vector(&[(me, 50), (peer, 30)]);
        let behind = vector(&[(me, 50), (peer, 40)]);
        assert_eq!(entries_threshold(&mine, &behind, &span), Some(Hlc::new(30, 0)));
        assert_eq!(entries_threshold(&mine, &behind, &[]), Some(Hlc::new(30, 0)));
        let level = vector(&[(me, 45), (peer, 30)]);
        assert_eq!(entries_threshold(&mine, &level, &span), Some(Hlc::new(45, 0)));
        assert_eq!(entries_threshold(&mine, &level, &[]), None);
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

    /// ADR-168's mirror of the table above, on this node's side: this node
    /// behind the peer when its count was read, and still moving, defers;
    /// standing still for `FROZEN_CONTACTS` checked contacts compares, so a
    /// member whose own inbound replication has stopped is still checked;
    /// level compares at once. Judged on the vector the count was read at
    /// against what the peer can serve.
    #[test]
    fn this_node_behind_and_moving_defers_and_standing_still_compares() {
        let peer = node(1);
        let me = node(2);
        let their_servable = vector(&[(me, 50), (peer, 100)]);

        // Draining the peer's writes: this node's position on the peer's
        // origin moves on every contact.
        let mut stalls = PeerStalls::new();
        for my_wall in [10u64, 20, 30, 40, 50, 60, 70] {
            let mine_at = vector(&[(me, 50), (peer, my_wall)]);
            let position = stalls.observe_self(peer, &their_servable, Some(&mine_at));
            assert_eq!(position, PeerPosition::Advancing, "at {my_wall}");
            let gate = divergence_probe_for(probe(), combine(PeerPosition::CaughtUp, position));
            assert_eq!(gate, CountGate { probe: None, deferred: true }, "at {my_wall}");
        }

        // Wedged: the same position comes back.
        let mut stalls = PeerStalls::new();
        let stuck = vector(&[(me, 50), (peer, 40)]);
        for sighting in 1..=FROZEN_CONTACTS {
            let position = stalls.observe_self(peer, &their_servable, Some(&stuck));
            assert_eq!(position, PeerPosition::Advancing, "sighting {sighting}: not yet frozen");
        }
        let position = stalls.observe_self(peer, &their_servable, Some(&stuck));
        assert_eq!(position, PeerPosition::Frozen, "unchanged for FROZEN_CONTACTS contacts");
        assert_eq!(
            divergence_probe_for(probe(), combine(PeerPosition::CaughtUp, position)),
            CountGate { probe: probe(), deferred: false },
            "a member that has stopped taking in its peer's writes is compared"
        );

        // Level: compared at once, and the memo goes.
        let level = vector(&[(me, 50), (peer, 100)]);
        assert_eq!(
            stalls.observe_self(peer, &their_servable, Some(&level)),
            PeerPosition::CaughtUp
        );
        assert!(stalls.by_self.is_empty());

        // This node's own writes are never a position it trails on.
        let ahead_on_me = vector(&[(me, 500), (peer, 100)]);
        assert_eq!(
            stalls.observe_self(peer, &their_servable, Some(&ahead_on_me)),
            PeerPosition::CaughtUp
        );

        // No vector read with the count: this side says nothing.
        assert_eq!(stalls.observe_self(peer, &their_servable, None), PeerPosition::CaughtUp);
    }

    /// Why this node's side is judged against what the peer can SERVE. A
    /// peer that processed an entry without appending it has a witnessed
    /// position this node can never reach, because the entry is never sent.
    /// Judged on that, this node would trail for ever -- deferred, then
    /// frozen and compared -- on a cluster with nothing to catch up on: the
    /// trickle ADR-146 removed, in reverse. `position` must hand the
    /// servable vector to this side and the witnessed one to the peer's.
    #[test]
    fn this_node_is_judged_on_what_the_peer_can_serve_not_on_what_it_processed() {
        let peer = node(1);
        let me = node(2);
        let their_servable = vector(&[(me, 50), (peer, 80)]);
        let their_witnessed = vector(&[(me, 50), (peer, 100)]);
        let mine = vector(&[(me, 50), (peer, 80)]);
        let probe = Some(DivergenceProbe {
            id: kimmy_core::CollectionId(7),
            mine_count: Some(10),
            mine_at: Some(mine.clone()),
        });

        let mut stalls = PeerStalls::new();
        for contact in 0..=FROZEN_CONTACTS {
            let position =
                stalls.position(peer, &their_witnessed, &mine, &their_servable, probe.as_ref());
            assert_eq!(
                position,
                PeerPosition::CaughtUp,
                "contact {contact}: nothing to catch up on"
            );
            assert!(!divergence_probe_for(probe.clone(), position).deferred);
        }

        // The same reading on the witnessed vector: what the wrong pairing does.
        let mut stalls = PeerStalls::new();
        assert_eq!(
            stalls.observe_self(peer, &their_witnessed, Some(&mine)),
            PeerPosition::Advancing,
            "behind for ever on an entry it can never be sent"
        );
    }

    /// `combine`'s table: either side moving defers; otherwise either side
    /// standing still compares; neither behind compares.
    #[test]
    fn either_side_moving_defers_and_otherwise_a_still_side_compares() {
        use PeerPosition::{Advancing, CaughtUp, Frozen};
        for (peer_side, self_side, expected) in [
            (CaughtUp, CaughtUp, CaughtUp),
            (Advancing, CaughtUp, Advancing),
            (CaughtUp, Advancing, Advancing),
            (Advancing, Frozen, Advancing),
            (Frozen, Advancing, Advancing),
            (Frozen, CaughtUp, Frozen),
            (CaughtUp, Frozen, Frozen),
            (Frozen, Frozen, Frozen),
            (Advancing, Advancing, Advancing),
        ] {
            assert_eq!(combine(peer_side, self_side), expected, "{peer_side:?} {self_side:?}");
        }
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
        let probe = Some(DivergenceProbe { id: orders.id, mine_count: Some(1), mine_at: None });
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
            let divergence = Message::Divergence {
                collections: Vec::new(),
                probe_count: Some(1),
                incarnations: Vec::new(),
            };
            write_frame(&mut stream, &divergence).await.unwrap();
        }

        // Upgraded: both vectors, gated on what it has processed.
        let (ours, theirs) = tokio::io::duplex(MAX_FRAME);
        let answer = Message::Vectors { servable: trailing.clone(), witnessed: processed };
        let peer = tokio::spawn(fake_peer(theirs, answer));
        let mut stalls = PeerStalls::new();
        let outcome =
            sync_over(&engine, ours, addr, their_node, probe.clone(), &mut stalls).await.unwrap();
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
    /// ADR-148 at the wire: a peer whose window carries entries above the
    /// vector it advertised for their origin. The receiver applies nothing
    /// above that vector, witnesses exactly what the vector promised, and
    /// still runs the divergence check, because the tail it needs is the one
    /// the peer announced and a deferred entry lies past it. What it never
    /// does is what it did: observe the entries and claim everything of
    /// that origin below them.
    #[tokio::test]
    async fn entries_above_the_advertised_vector_are_neither_applied_nor_witnessed() {
        use tokio::io::DuplexStream;

        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let orders = engine.create_collection("shop", "orders").unwrap();
        let origin = node(3);
        let advertised = Hlc::new(5_000, 0);
        let mut theirs = VersionVector::new();
        theirs.insert(origin, advertised);
        let entry = |wall: u64, id: i32| OplogEntry {
            stamp: kimmy_core::Stamp::new(Hlc::new(wall, 0), origin),
            kind: kimmy_core::OpKind::Insert,
            collection: orders.id,
            doc_id: Some(kimmy_core::DocId::Int64(id.into())),
            body: Some(bson::serialize_to_vec(&bson::doc! { "_id": id }).unwrap()),
        };
        let window = vec![entry(4_000, 1), entry(7_000, 2), entry(8_000, 3)];

        async fn fake_peer(
            mut stream: DuplexStream,
            theirs: VersionVector,
            window: Vec<OplogEntry>,
        ) {
            match read_frame(&mut stream).await.unwrap() {
                Message::AskVersions { witnessed: true } => {}
                other => panic!("expected AskVersions, got {other:?}"),
            }
            let answer = Message::Vectors { servable: theirs.clone(), witnessed: theirs };
            write_frame(&mut stream, &answer).await.unwrap();
            match read_frame(&mut stream).await.unwrap() {
                Message::AskEntries { .. } => {}
                other => panic!("expected AskEntries, got {other:?}"),
            }
            let scanned_to = window.last().unwrap().stamp.hlc;
            let entries = Message::Entries { entries: window, scanned_to, exhausted: true };
            write_frame(&mut stream, &entries).await.unwrap();
            // The window was taken whole up to the advertised vector, so the
            // round reached the tail the peer announced and runs the check —
            // the entries it left lie past that tail, not short of it.
            match read_frame(&mut stream).await.unwrap() {
                Message::AskDivergence { .. } => {}
                other => panic!("expected AskDivergence, got {other:?}"),
            }
            let answer = Message::Divergence {
                collections: Vec::new(),
                probe_count: None,
                incarnations: Vec::new(),
            };
            write_frame(&mut stream, &answer).await.unwrap();
        }

        let (ours, peer_end) = tokio::io::duplex(MAX_FRAME);
        let peer = tokio::spawn(fake_peer(peer_end, theirs.clone(), window));
        let mut stalls = PeerStalls::new();
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let outcome = sync_over(&engine, ours, addr, node(9), None, &mut stalls).await.unwrap();
        peer.await.unwrap();

        assert_eq!(outcome.applied, 1, "the entry the vector covered: {outcome:?}");
        assert_eq!(outcome.deferred, 2, "the two above it were left: {outcome:?}");
        assert!(outcome.exhausted, "the window was taken whole up to the advertised vector");
        assert_eq!(
            outcome.divergent,
            Some(std::collections::BTreeSet::new()),
            "so the check ran on a round that deferred: {outcome:?}"
        );
        assert_eq!(engine.count(&orders).unwrap(), 1);
        let mine = engine.witnessed_vector().unwrap();
        assert_eq!(mine.get(origin), advertised, "witnessed exactly what was advertised");
        assert_eq!(mine.behind(&theirs), None);
        // The peer advertises them next round, and this node asks from its
        // own position, below them.
        let mut later = VersionVector::new();
        later.insert(origin, Hlc::new(8_000, 0));
        assert_eq!(mine.behind(&later), Some(advertised));
    }

    /// A planned replay (ADR-148) asks from the floor it was planned with,
    /// judged by the threshold rather than the vector, though the position
    /// says there is nothing to pull; it continues from where a truncated
    /// window ended and finishes on an exhausted one, after which the round
    /// is an ordinary round again.
    #[tokio::test]
    async fn a_planned_replay_asks_below_the_position_until_the_tail_is_reached() {
        use tokio::io::DuplexStream;

        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let orders = engine.create_collection("shop", "orders").unwrap();
        let origin = node(3);
        let mut theirs = VersionVector::new();
        theirs.insert(origin, Hlc::new(9_000, 0));
        // The hole: the position claims the origin through 9_000 while
        // nothing of it was ever applied.
        engine.absorb_witnessed(&theirs).unwrap();
        let entry = |wall: u64, id: i32| OplogEntry {
            stamp: kimmy_core::Stamp::new(Hlc::new(wall, 0), origin),
            kind: kimmy_core::OpKind::Insert,
            collection: orders.id,
            doc_id: Some(kimmy_core::DocId::Int64(id.into())),
            body: Some(bson::serialize_to_vec(&bson::doc! { "_id": id }).unwrap()),
        };

        /// One round of the peer's side: answer the vectors, expect
        /// `AskEntries` from `expect_from` with no vector, serve `window`.
        async fn fake_peer(
            mut stream: DuplexStream,
            theirs: VersionVector,
            expect_from: Hlc,
            window: Vec<OplogEntry>,
            exhausted: bool,
        ) {
            match read_frame(&mut stream).await.unwrap() {
                Message::AskVersions { witnessed: true } => {}
                other => panic!("expected AskVersions, got {other:?}"),
            }
            let answer = Message::Vectors { servable: theirs.clone(), witnessed: theirs };
            write_frame(&mut stream, &answer).await.unwrap();
            match read_frame(&mut stream).await.unwrap() {
                Message::AskEntries { from, held, .. } => {
                    assert_eq!(from, expect_from, "a replay asks from its floor");
                    assert_eq!(held, None, "and is judged by the threshold, not the vector");
                }
                other => panic!("expected AskEntries, got {other:?}"),
            }
            let scanned_to = window.last().unwrap().stamp.hlc;
            let entries = Message::Entries { entries: window, scanned_to, exhausted };
            write_frame(&mut stream, &entries).await.unwrap();
            if exhausted {
                match read_frame(&mut stream).await.unwrap() {
                    Message::AskDivergence { .. } => {}
                    other => panic!("the tail was reached, so the check follows, got {other:?}"),
                }
                let divergence = Message::Divergence {
                    collections: Vec::new(),
                    probe_count: None,
                    incarnations: Vec::new(),
                };
                write_frame(&mut stream, &divergence).await.unwrap();
            }
        }

        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let mut stalls = PeerStalls::new();
        assert!(stalls.plan_repair(
            node(9),
            orders.id,
            Repair::Replay { from: Hlc::new(1_000, 0) }
        ));
        assert!(!stalls.plan_repair(node(9), orders.id, Repair::Snapshot), "one per collection");

        // Round one: a truncated window from the floor.
        let (ours, peer_end) = tokio::io::duplex(MAX_FRAME);
        let window = vec![entry(2_000, 1), entry(3_000, 2)];
        let peer =
            tokio::spawn(fake_peer(peer_end, theirs.clone(), Hlc::new(1_000, 0), window, false));
        let outcome = sync_over(&engine, ours, addr, node(9), None, &mut stalls).await.unwrap();
        peer.await.unwrap();
        assert_eq!(outcome.applied, 2, "{outcome:?}");
        assert!(outcome.repairing, "{outcome:?}");
        assert!(stalls.repairing(node(9)), "continues next round");

        // Round two: from where the window ended, to the tail.
        let (ours, peer_end) = tokio::io::duplex(MAX_FRAME);
        let window = vec![entry(3_000, 2), entry(9_000, 3)];
        let peer =
            tokio::spawn(fake_peer(peer_end, theirs.clone(), Hlc::new(3_000, 0), window, true));
        let outcome = sync_over(&engine, ours, addr, node(9), None, &mut stalls).await.unwrap();
        peer.await.unwrap();
        assert_eq!(outcome.applied, 1, "{outcome:?}");
        assert!(outcome.repairing && outcome.exhausted, "{outcome:?}");
        assert!(!stalls.repairing(node(9)), "done");
        assert_eq!(engine.count(&orders).unwrap(), 3, "the hole is closed");
        assert!(
            !stalls.plan_repair(node(9), orders.id, Repair::Snapshot),
            "not repaired again until the finding clears or the cooldown passes"
        );
        let mut still = BTreeSet::new();
        still.insert(orders.id);
        stalls.retain_repaired(node(9), &still);
        assert!(!stalls.plan_repair(node(9), orders.id, Repair::Snapshot), "still reported");
        stalls.retain_repaired(node(9), &BTreeSet::new());
        assert!(stalls.plan_repair(node(9), orders.id, Repair::Snapshot), "cleared, so again");
    }

    /// A snapshot page as a fake peer serves it: one collection, documents
    /// with `origin`'s stamps at `walls`, the cursor and the vector given.
    fn snapshot_page(
        origin: NodeId,
        walls: &[u64],
        next: Option<kimmy_storage::SnapshotCursor>,
        versions: VersionVector,
        first: bool,
    ) -> SnapshotPage {
        let collection = CollectionId::derive("shop", "orders");
        let collections = if first {
            vec![kimmy_storage::CollectionState {
                db: "shop".into(),
                name: "orders".into(),
                indexes: Vec::new(),
                vector: None,
                // The incarnation the sender holds, older than every
                // document it carries, as a real sender's would be.
                created: Some(Hlc::ZERO),
            }]
        } else {
            Vec::new()
        };
        let documents = walls
            .iter()
            .map(|&wall| kimmy_storage::SnapshotDoc {
                collection,
                id: kimmy_core::DocId::Int64(wall as i64),
                stamp: kimmy_core::Stamp::new(Hlc::new(wall, 0), origin),
                body: Some(bson::serialize_to_vec(&bson::doc! { "_id": wall as i64 }).unwrap()),
            })
            .collect();
        SnapshotPage {
            collections,
            documents,
            next,
            versions,
            dropped: None,
            dropped_collections: Vec::new(),
            deleted_documents: Vec::new(),
        }
    }

    fn cursor(after: u64) -> kimmy_storage::SnapshotCursor {
        kimmy_storage::SnapshotCursor {
            collection: CollectionId::derive("shop", "orders"),
            after_key: after.to_be_bytes().to_vec(),
        }
    }

    /// A deadline a snapshot pull has already passed, and one it never
    /// reaches within a test.
    fn spent() -> Instant {
        Instant::now() - Duration::from_secs(1)
    }
    fn ample() -> Instant {
        Instant::now() + Duration::from_secs(600)
    }

    /// ADR-152 on the horizon path, over the wire. A whole-database snapshot
    /// whose round budget is spent after one page leaves that page applied
    /// and its cursor with the `PeerStalls`; the next round asks for the
    /// page after it, not page one, and the coverage adopted when the final
    /// page lands is the vector served with the *first* page — the peer's
    /// later vector, served with the last page, is not covered, so the next
    /// round still asks for what the peer wrote in between. Nothing is
    /// witnessed before the end: the pages' documents move no vector.
    #[tokio::test]
    async fn a_snapshot_cut_short_resumes_from_its_cursor_and_grants_the_first_pages_vector() {
        use tokio::io::DuplexStream;

        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let origin = node(3);
        let first_vector = vector(&[(origin, 3_000)]);
        let last_vector = vector(&[(origin, 9_000)]);
        let their_node = node(9);
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        /// One round of the peer's side: vectors, `BeyondHorizon` to the
        /// pull, then the snapshot pages it is asked for, in order, each
        /// asked for from the cursor expected.
        async fn fake_peer(
            mut stream: DuplexStream,
            theirs: VersionVector,
            pages: Vec<(Option<kimmy_storage::SnapshotCursor>, SnapshotPage)>,
            then_check: bool,
        ) {
            match read_frame(&mut stream).await.unwrap() {
                Message::AskVersions { witnessed: true } => {}
                other => panic!("expected AskVersions, got {other:?}"),
            }
            let answer = Message::Vectors { servable: theirs.clone(), witnessed: theirs };
            write_frame(&mut stream, &answer).await.unwrap();
            match read_frame(&mut stream).await.unwrap() {
                Message::AskEntries { .. } => {}
                other => panic!("expected AskEntries, got {other:?}"),
            }
            write_frame(&mut stream, &Message::BeyondHorizon {}).await.unwrap();
            for (expect_after, page) in pages {
                match read_frame(&mut stream).await.unwrap() {
                    Message::AskSnapshot { after, collection: None } => {
                        assert_eq!(after, expect_after, "asked from where the last page ended");
                    }
                    other => panic!("expected a whole-database AskSnapshot, got {other:?}"),
                }
                write_frame(&mut stream, &Message::Snapshot(Box::new(page))).await.unwrap();
            }
            if then_check {
                match read_frame(&mut stream).await.unwrap() {
                    Message::AskDivergence { .. } => {}
                    other => panic!("a completed snapshot reached the tail, got {other:?}"),
                }
                let divergence = Message::Divergence {
                    collections: Vec::new(),
                    probe_count: None,
                    incarnations: Vec::new(),
                };
                write_frame(&mut stream, &divergence).await.unwrap();
            }
            // Nothing more must be asked: the round's budget is spent, or
            // the round is over.
            assert!(
                matches!(read_frame(&mut stream).await, Err(ProtocolError::Closed)),
                "the requester asked for more than the round allowed"
            );
        }

        let mut stalls = PeerStalls::new();
        let orders = CollectionId::derive("shop", "orders");

        // Round one: budget already spent, so one page and no more.
        let (ours, peer_end) = tokio::io::duplex(MAX_FRAME);
        let page1 =
            snapshot_page(origin, &[1_000, 2_000], Some(cursor(2_000)), first_vector.clone(), true);
        let peer =
            tokio::spawn(fake_peer(peer_end, last_vector.clone(), vec![(None, page1)], false));
        let mut ours = ours;
        let outcome = sync_round(&engine, &mut ours, addr, their_node, None, &mut stalls, spent())
            .await
            .unwrap();
        drop(ours);
        peer.await.unwrap();
        assert_eq!(outcome.applied, 2, "the page landed: {outcome:?}");
        assert!(!outcome.exhausted, "left to resume is not the tail: {outcome:?}");
        assert!(stalls.snapshot_resumes(their_node, None), "the cursor is kept");
        assert_eq!(stalls.snapshots[&their_node].after(), Some(&cursor(2_000)));
        let meta = engine.collection_by_id(orders).unwrap().expect("the definition arrived");
        assert_eq!(engine.count(&meta).unwrap(), 2);
        assert_eq!(
            engine.witnessed_vector().unwrap().get(origin),
            Hlc::ZERO,
            "nothing is witnessed until the snapshot completes"
        );

        // Round two: from the cursor, with the budget intact, to the end.
        // The peer has written on: the final page's vector is higher.
        let (ours, peer_end) = tokio::io::duplex(MAX_FRAME);
        let page2 =
            snapshot_page(origin, &[4_000], Some(cursor(4_000)), last_vector.clone(), false);
        let page3 = snapshot_page(origin, &[9_000], None, last_vector.clone(), false);
        let peer = tokio::spawn(fake_peer(
            peer_end,
            last_vector.clone(),
            vec![(Some(cursor(2_000)), page2), (Some(cursor(4_000)), page3)],
            true,
        ));
        let mut ours = ours;
        let outcome = sync_round(&engine, &mut ours, addr, their_node, None, &mut stalls, ample())
            .await
            .unwrap();
        drop(ours);
        peer.await.unwrap();
        assert_eq!(outcome.applied, 2, "{outcome:?}");
        assert!(outcome.exhausted, "a completed snapshot is the tail: {outcome:?}");
        assert!(outcome.divergent.is_some(), "so the check ran: {outcome:?}");
        assert!(!stalls.snapshot_resumes(their_node, None), "nothing left to resume");
        assert_eq!(engine.count(&meta).unwrap(), 4, "every page, each exactly once");
        let mine = engine.witnessed_vector().unwrap();
        assert!(mine.covers(&first_vector), "the first page's vector is adopted: {mine:?}");
        assert!(
            !mine.covers(&last_vector),
            "not the last page's: what the peer wrote behind the cursor since is still asked \
             for — {mine:?}"
        );
        assert_eq!(mine.behind(&last_vector), Some(Hlc::new(3_000, 0)));
    }

    /// ADR-152 on the repair path. A snapshot repair asks for the one
    /// collection it was planned for, resumes from its cursor on the next
    /// round, is not abandoned while pages keep landing — one page per round
    /// for more rounds than `REPAIR_ATTEMPTS` — finishes when the final
    /// page lands, and grants no coverage on the way or at the end: the
    /// position carries the rest. Three rounds that apply nothing still
    /// abandon it.
    #[tokio::test]
    async fn a_snapshot_repair_pulls_one_collection_resumes_and_is_abandoned_only_when_nothing_lands()
     {
        use tokio::io::DuplexStream;

        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let origin = node(3);
        let theirs = vector(&[(origin, 9_000)]);
        let their_node = node(9);
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let orders = CollectionId::derive("shop", "orders");
        // The hole: the position already claims the origin, so nothing
        // says the collection is missing except the repair.
        engine.absorb_witnessed(&theirs).unwrap();

        /// One round of the peer's side of a snapshot repair: vectors, then
        /// straight to a scoped `AskSnapshot` — no `AskEntries`, the repair
        /// asks for the snapshot outright — answered with `page`; a
        /// completed page is followed by the check.
        async fn fake_peer(
            mut stream: DuplexStream,
            theirs: VersionVector,
            expect_after: Option<kimmy_storage::SnapshotCursor>,
            page: Option<SnapshotPage>,
        ) {
            match read_frame(&mut stream).await.unwrap() {
                Message::AskVersions { witnessed: true } => {}
                other => panic!("expected AskVersions, got {other:?}"),
            }
            let answer = Message::Vectors { servable: theirs.clone(), witnessed: theirs };
            write_frame(&mut stream, &answer).await.unwrap();
            // A round that fails before a page lands: the peer goes away.
            let Some(page) = page else { return };
            match read_frame(&mut stream).await.unwrap() {
                Message::AskSnapshot { after, collection } => {
                    assert_eq!(collection, Some(CollectionId::derive("shop", "orders")));
                    assert_eq!(after, expect_after, "resumed from the last page applied");
                }
                other => panic!("expected a scoped AskSnapshot, got {other:?}"),
            }
            let complete = page.next.is_none();
            write_frame(&mut stream, &Message::Snapshot(Box::new(page))).await.unwrap();
            if complete {
                match read_frame(&mut stream).await.unwrap() {
                    Message::AskDivergence { .. } => {}
                    other => panic!("expected AskDivergence, got {other:?}"),
                }
                let divergence = Message::Divergence {
                    collections: Vec::new(),
                    probe_count: None,
                    incarnations: Vec::new(),
                };
                write_frame(&mut stream, &divergence).await.unwrap();
            }
            assert!(matches!(read_frame(&mut stream).await, Err(ProtocolError::Closed)));
        }

        let mut stalls = PeerStalls::new();
        assert!(stalls.plan_repair(their_node, orders, Repair::Snapshot));

        // More rounds than a stalled repair survives, each landing one page
        // and stopping on a spent budget.
        let rounds = REPAIR_ATTEMPTS as u64 * 2;
        for round in 0..rounds {
            let wall = 1_000 * (round + 1);
            let after = (round > 0).then(|| cursor(wall - 1_000));
            let page =
                snapshot_page(origin, &[wall], Some(cursor(wall)), theirs.clone(), round == 0);
            let (ours, peer_end) = tokio::io::duplex(MAX_FRAME);
            let peer = tokio::spawn(fake_peer(peer_end, theirs.clone(), after, Some(page)));
            let mut ours = ours;
            let outcome =
                sync_round(&engine, &mut ours, addr, their_node, None, &mut stalls, spent())
                    .await
                    .unwrap();
            drop(ours);
            peer.await.unwrap();
            assert_eq!(outcome.applied, 1, "round {round}: {outcome:?}");
            assert!(outcome.repairing && !outcome.exhausted, "round {round}: {outcome:?}");
            assert!(stalls.repairing(their_node), "round {round}: a moving repair is kept");
            assert!(stalls.snapshot_resumes(their_node, Some(orders)), "round {round}");
        }

        // The final page, with the budget intact: done, and the check runs.
        let last = snapshot_page(origin, &[9_000], None, theirs.clone(), false);
        let (ours, peer_end) = tokio::io::duplex(MAX_FRAME);
        let peer = tokio::spawn(fake_peer(
            peer_end,
            theirs.clone(),
            Some(cursor(1_000 * rounds)),
            Some(last),
        ));
        let mut ours = ours;
        let outcome = sync_round(&engine, &mut ours, addr, their_node, None, &mut stalls, ample())
            .await
            .unwrap();
        drop(ours);
        peer.await.unwrap();
        assert!(outcome.repairing && outcome.exhausted, "{outcome:?}");
        assert!(outcome.divergent.is_some(), "{outcome:?}");
        assert!(!stalls.repairing(their_node), "done");
        assert!(!stalls.snapshot_resumes(their_node, Some(orders)));
        let meta = engine.collection_by_id(orders).unwrap().expect("the collection arrived");
        assert_eq!(engine.count(&meta).unwrap(), rounds + 1, "every page exactly once");
        assert_eq!(
            engine.witnessed_vector().unwrap(),
            theirs,
            "a scoped snapshot grants nothing: the position is what it was"
        );

        // A second repair whose every round fails before a page lands is
        // abandoned after `REPAIR_ATTEMPTS` of them, snapshot or not.
        stalls.retain_repaired(their_node, &BTreeSet::new());
        assert!(stalls.plan_repair(their_node, orders, Repair::Snapshot));
        for attempt in 0..REPAIR_ATTEMPTS {
            let (ours, peer_end) = tokio::io::duplex(MAX_FRAME);
            let peer = tokio::spawn(fake_peer(peer_end, theirs.clone(), None, None));
            let mut ours = ours;
            let failed =
                sync_round(&engine, &mut ours, addr, their_node, None, &mut stalls, ample()).await;
            drop(ours);
            peer.await.unwrap();
            assert!(failed.is_err(), "attempt {attempt}: the peer went away");
        }
        assert_eq!(
            stalls.repair_due(their_node),
            None,
            "abandoned after three rounds that landed nothing"
        );
        assert!(!stalls.repairing(their_node));
    }

    /// The rolling-upgrade half of the tombstone rule, over the wire. A
    /// peer running a version before `incarnations` answers without the
    /// field, which arrives here as an empty one (`protocol.rs` pins that
    /// on the bytes), and so says nothing about which life of a collection
    /// it holds. For an id this node has a tombstone for, that silence is
    /// read as the incarnation this node dropped: not reported, so nothing
    /// plans a repair that would pull the buried life back from the member
    /// that has not applied the drop yet. Deliberate, and what it costs is
    /// stated on `divergence::compare`.
    ///
    /// The controls sit against the same engine and the same answer, so the
    /// rule cannot pass by quietly reporting nothing at all: a collection
    /// this node holds no tombstone for is reported from that very answer,
    /// and the same peer naming an incarnation *after* the drop is reported
    /// too — which is also what pins the field's journey from the wire into
    /// the comparison, rather than being read as absent whatever arrives.
    /// Level with the drop is the same-millisecond tie, and is the life
    /// that was dropped.
    #[tokio::test]
    async fn a_peer_that_names_no_incarnation_does_not_reopen_a_collection_dropped_here() {
        use tokio::io::DuplexStream;

        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
        let bench = CollectionId::derive("shop", "bench");
        let ledger = CollectionId::derive("shop", "ledger");
        engine.create_collection("shop", "bench").unwrap();
        assert!(engine.drop_collection("shop", "bench").unwrap());
        let dropped = engine.collection_dropped_at(bench).unwrap().expect("the tombstone");

        /// The peer's side: the vectors, then an answer naming both
        /// collections and whatever it knows of their incarnations.
        async fn fake_peer(
            mut stream: DuplexStream,
            theirs: VersionVector,
            incarnations: Vec<(CollectionId, Hlc)>,
        ) {
            match read_frame(&mut stream).await.unwrap() {
                Message::AskVersions { witnessed: true } => {}
                other => panic!("expected AskVersions, got {other:?}"),
            }
            let answer = Message::Vectors { servable: theirs.clone(), witnessed: theirs };
            write_frame(&mut stream, &answer).await.unwrap();
            match read_frame(&mut stream).await.unwrap() {
                Message::AskDivergence { .. } => {}
                other => panic!("expected AskDivergence, got {other:?}"),
            }
            let divergence = Message::Divergence {
                collections: vec![
                    CollectionId::derive("shop", "bench"),
                    CollectionId::derive("shop", "ledger"),
                ],
                probe_count: None,
                incarnations,
            };
            write_frame(&mut stream, &divergence).await.unwrap();
            assert!(matches!(read_frame(&mut stream).await, Err(ProtocolError::Closed)));
        }

        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let their_node = node(9);
        // Nothing to pull, so the round is the check and nothing else.
        let theirs = VersionVector::new();
        let mut stalls = PeerStalls::new();

        for (incarnations, expected, what) in [
            (
                Vec::new(),
                vec![ledger],
                "a peer that names no incarnation holds the one this node dropped",
            ),
            (
                vec![(bench, dropped.hlc), (ledger, Hlc::ZERO)],
                vec![ledger],
                "an incarnation level with the drop is the life the drop ended",
            ),
            (
                vec![(bench, dropped.hlc.successor()), (ledger, Hlc::ZERO)],
                vec![bench, ledger],
                "an incarnation after the drop is a genuine recreation",
            ),
        ] {
            let (ours, peer_end) = tokio::io::duplex(MAX_FRAME);
            let peer = tokio::spawn(fake_peer(peer_end, theirs.clone(), incarnations));
            let outcome =
                sync_over(&engine, ours, addr, their_node, None, &mut stalls).await.unwrap();
            peer.await.unwrap();
            assert_eq!(
                outcome.divergent,
                Some(expected.into_iter().collect()),
                "{what}: {outcome:?}"
            );
        }
    }

    /// A repair that never completes — every round with the peer failing,
    /// or a snapshot too large to finish inside the request timeout — is
    /// abandoned after `REPAIR_ATTEMPTS` rounds that did not advance it,
    /// and takes the cooldown exactly as a completed one does. Without
    /// this it is retried on every round for the life of the process, and
    /// a full-database snapshot every five seconds is a wedge of its own.
    #[test]
    fn a_repair_that_never_advances_is_abandoned_and_takes_the_cooldown() {
        let peer = node(1);
        let collection = CollectionId(7);
        let mut stalls = PeerStalls::new();
        assert!(stalls.plan_repair(peer, collection, Repair::Snapshot));

        // Handed out exactly `REPAIR_ATTEMPTS` times, the number the
        // warning names.
        for attempt in 0..REPAIR_ATTEMPTS {
            assert_eq!(
                stalls.repair_due(peer),
                Some((collection, Repair::Snapshot)),
                "attempt {attempt}"
            );
        }
        // The round after that abandons it rather than asking again.
        assert_eq!(stalls.repair_due(peer), None, "abandoned");
        assert!(!stalls.repairing(peer));
        assert!(!stalls.plan_repair(peer, collection, Repair::Snapshot), "and cooling down");

        // A repair that *does* advance keeps its place, however long it
        // takes: the run restarts on every window it moves.
        let mut stalls = PeerStalls::new();
        assert!(stalls.plan_repair(peer, collection, Repair::Replay { from: Hlc::new(1, 0) }));
        for wall in 1..=(REPAIR_ATTEMPTS as u64 * 4) {
            assert!(stalls.repair_due(peer).is_some(), "still going at {wall}");
            stalls.repair_continues(peer, Repair::Replay { from: Hlc::new(wall, 0) });
        }
        assert!(stalls.repairing(peer), "a repair making progress is never abandoned");
    }

    /// The count after progress (ADR-152). A repair that advanced — a page
    /// landed, a window moved — is handed out for exactly `REPAIR_ATTEMPTS`
    /// rounds that land nothing before it is abandoned, the same as one
    /// that never advanced: the round that advanced is not the first of
    /// the run. The count was incremented when a round opened and reset
    /// inside the round that advanced, so the next round opened at one
    /// and two empty rounds abandoned a repair that had just made
    /// progress.
    #[test]
    fn a_repair_that_advanced_survives_three_rounds_landing_nothing_and_not_a_fourth() {
        let peer = node(1);
        let collection = CollectionId(7);

        // A snapshot repair: a page landed in the round it was handed to.
        let mut stalls = PeerStalls::new();
        assert!(stalls.plan_repair(peer, collection, Repair::Snapshot));
        assert_eq!(stalls.repair_due(peer), Some((collection, Repair::Snapshot)));
        stalls.snapshot_advanced(peer);
        for empty in 1..=REPAIR_ATTEMPTS {
            assert_eq!(
                stalls.repair_due(peer),
                Some((collection, Repair::Snapshot)),
                "empty round {empty} after the page: still handed out"
            );
        }
        assert_eq!(stalls.repair_due(peer), None, "and abandoned on the round after");
        assert!(!stalls.repairing(peer));

        // A replay, and a run that restarts: two empty rounds, then the
        // window moves, then three more before the fourth abandons it.
        let mut stalls = PeerStalls::new();
        let replay = Repair::Replay { from: Hlc::new(1, 0) };
        assert!(stalls.plan_repair(peer, collection, replay));
        assert_eq!(stalls.repair_due(peer), Some((collection, replay)));
        stalls.repair_continues(peer, Repair::Replay { from: Hlc::new(2, 0) });
        assert!(stalls.repair_due(peer).is_some(), "first empty round");
        assert!(stalls.repair_due(peer).is_some(), "second empty round");
        stalls.repair_continues(peer, Repair::Replay { from: Hlc::new(3, 0) });
        for empty in 1..=REPAIR_ATTEMPTS {
            assert!(stalls.repair_due(peer).is_some(), "empty round {empty} after the move");
        }
        assert_eq!(stalls.repair_due(peer), None, "abandoned on the fourth");
    }

    /// The cooldown: a repair done for a collection is not planned again
    /// for `REPAIR_COOLDOWN_ROUNDS` contacts with that peer unless the check
    /// reports it clear, and is again after. One `tick_opened` per round
    /// here, because a contact is a peer per tick (ADR-157) and this is the
    /// one-pull-per-tick shape a converged cluster has.
    #[test]
    fn a_repaired_collection_waits_out_the_cooldown_before_repairing_again() {
        let peer = node(1);
        let collection = CollectionId(7);
        let mut stalls = PeerStalls::new();
        assert!(stalls.plan_repair(peer, collection, Repair::Snapshot));
        assert_eq!(stalls.repair_due(peer), Some((collection, Repair::Snapshot)));
        stalls.repair_finished(peer);
        assert!(!stalls.repairing(peer));
        for _ in 1..REPAIR_COOLDOWN_ROUNDS {
            stalls.tick_opened();
            assert_eq!(stalls.repair_due(peer), None);
            assert!(!stalls.plan_repair(peer, collection, Repair::Snapshot), "cooling down");
        }
        stalls.tick_opened();
        assert_eq!(stalls.repair_due(peer), None);
        assert!(stalls.plan_repair(peer, collection, Repair::Snapshot), "cooled down");
    }

    /// A tick that pulls from a peer a dozen times while draining a backlog
    /// spends **one** contact of the cooldown, not a dozen (ADR-157). The
    /// constant is sixty because sixty is five minutes at the default
    /// interval; counted per pull, a drain would spend it in seconds and a
    /// divergence the repair cannot close would replay the collection's
    /// whole history every few ticks, against the very backlog the tick's
    /// budget is there to clear.
    #[test]
    fn a_tick_that_pulls_many_times_spends_one_contact_of_the_repair_cooldown() {
        let peer = node(1);
        let collection = CollectionId(7);
        let mut stalls = PeerStalls::new();
        assert!(stalls.plan_repair(peer, collection, Repair::Snapshot));
        stalls.tick_opened();
        assert_eq!(stalls.repair_due(peer), Some((collection, Repair::Snapshot)));
        stalls.repair_finished(peer);

        // One tick, a hundred pulls: far past the cooldown's count, and it
        // has not moved.
        stalls.tick_opened();
        for _ in 0..(REPAIR_COOLDOWN_ROUNDS as usize * 2) {
            assert_eq!(stalls.repair_due(peer), None);
        }
        assert!(
            !stalls.plan_repair(peer, collection, Repair::Snapshot),
            "a drained tick is one contact, so the cooldown has barely started"
        );

        // And the ticks after it do move it.
        for _ in 1..REPAIR_COOLDOWN_ROUNDS {
            stalls.tick_opened();
            assert_eq!(stalls.repair_due(peer), None);
        }
        assert!(stalls.plan_repair(peer, collection, Repair::Snapshot), "cooled down");
    }
}
