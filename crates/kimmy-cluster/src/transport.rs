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
//!            ◀───── AskVersions         ◀───── Versions
//!     Versions ────▶                    behind(theirs)?
//!            ◀───── AskEntries          AskEntries  ─────▶
//!     Entries ─────▶                    ◀───── Entries
//!                                       apply_batch
//! ```
//!
//! A round is one-directional: it pulls. Both peers running it against each
//! other is what converges them, and that falls out of every node running the
//! same loop rather than needing a push half.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

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

/// Serve peer requests until the listener fails.
pub async fn serve(engine: Arc<Engine>, listener: TcpListener, secret: String) {
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
            if let Err(e) = serve_peer(&engine, tls_stream, &secret, &binding).await {
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
            Message::AskVersions {} => {
                let versions =
                    engine.version_vector().map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                write_frame(&mut stream, &Message::Versions(versions)).await?;
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
#[derive(Clone, Copy, Debug)]
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
pub async fn sync_once(
    engine: &Engine,
    peer: SocketAddr,
    secret: &str,
    probe: Option<DivergenceProbe>,
) -> Result<SyncOutcome, ProtocolError> {
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

    let round = async {
        write_frame(&mut stream, &Message::AskVersions {}).await?;
        let Message::Versions(theirs) = read_frame(&mut stream).await? else {
            return Err(ProtocolError::Malformed("expected Versions".into()));
        };

        // What we have *seen*, not what we could serve. Asking against the
        // servable vector re-requests everything a node processed without
        // appending — replicated DDL, last-writer-wins losers — on every round,
        // forever (ADR-054).
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
            let probe = divergence_probe_for(probe, &theirs, &mine);
            let divergent = ask_divergence(engine, &mut stream, probe).await?;
            return Ok(SyncOutcome {
                peer: Some(their_node),
                behind_ms: behind_beyond_horizon(engine, &theirs, &mine)?,
                divergent: Some(divergent),
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
            let probe = divergence_probe_for(probe, &theirs, &mine);
            outcome.divergent = Some(ask_divergence(engine, &mut stream, probe).await?);
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

/// Whether to trust the peer's document count this round, given `theirs` —
/// the peer's version vector, fetched at the top of this round — and
/// `mine`, this node's own vector as of just before asking (ADR-133).
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
fn divergence_probe_for(
    probe: Option<DivergenceProbe>,
    theirs: &kimmy_core::VersionVector,
    mine: &kimmy_core::VersionVector,
) -> Option<DivergenceProbe> {
    if theirs.behind(mine).is_some() { None } else { probe }
}

/// Ask the peer what it holds, and compare against what this node holds
/// (ADR-133). One message each way, on the connection already open for this
/// round.
async fn ask_divergence<S>(
    engine: &Engine,
    stream: &mut S,
    probe: Option<DivergenceProbe>,
) -> Result<std::collections::BTreeSet<kimmy_core::CollectionId>, ProtocolError>
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
