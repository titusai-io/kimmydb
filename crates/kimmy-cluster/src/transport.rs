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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kimmy_storage::{Engine, SyncOutcome};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::protocol::{
    MAX_BATCH, Message, ProtocolError, nonce, proof_is_valid, prove, read_frame, write_frame,
};

/// How long a peer has to complete the handshake.
///
/// Without it a connection that opens and then says nothing holds a task
/// forever, which is a denial of service that costs the other side nothing.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long one request may take.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
            Message::AskEntries { from, limit } => {
                // Tell a peer below the horizon rather than serving it what is
                // left: it would apply that, advance its version vector, and
                // never learn what had been collected.
                let servable = engine
                    .can_serve_from_oplog(from)
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                if !servable {
                    write_frame(&mut stream, &Message::BeyondHorizon {}).await?;
                    continue;
                }

                // The peer's limit is a request, not an instruction: honouring
                // an arbitrary one would let it ask for the whole oplog in a
                // single frame.
                let limit = limit.min(MAX_BATCH);
                let entries = engine
                    .entries_for_peer(from, limit)
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                write_frame(&mut stream, &Message::Entries(entries)).await?;
            }
            Message::AskSnapshot { after } => {
                let page = engine
                    .snapshot_page(after)
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                write_frame(&mut stream, &Message::Snapshot(Box::new(page))).await?;
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

/// Run one anti-entropy round against `peer`, pulling what this node lacks.
pub async fn sync_once(
    engine: &Engine,
    peer: SocketAddr,
    secret: &str,
) -> Result<SyncOutcome, ProtocolError> {
    let tcp = TcpStream::connect(peer).await?;

    // Built per round rather than held: a sync round is seconds apart, and a
    // dialling node has no accept loop to amortise it against.
    let tls = crate::tls::ClusterTls::new()
        .map_err(|e| ProtocolError::Malformed(format!("cluster TLS: {e}")))?;
    let mut stream = tls
        .connector()
        .connect(crate::tls::ClusterTls::server_name(), tcp)
        .await
        .map_err(|e| ProtocolError::Malformed(format!("TLS handshake with {peer}: {e}")))?;

    let binding = {
        let (_, conn) = stream.get_ref();
        crate::tls::binding(conn).map_err(ProtocolError::Malformed)?
    };

    tokio::time::timeout(HANDSHAKE_TIMEOUT, open_handshake(engine, &mut stream, secret, &binding))
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
            return Ok(SyncOutcome::default());
        };

        write_frame(&mut stream, &Message::AskEntries { from, limit: MAX_BATCH }).await?;
        let mut outcome = match read_frame(&mut stream).await? {
            Message::Entries(entries) => {
                // A short batch means the peer sent everything it is willing to
                // send from `from` onward, so this node has now seen all of
                // `theirs` — including entries the peer holds but deliberately
                // never ships. A `UniqueViolation` is the standing example
                // (ADR-029): it is in the sender's oplog and therefore in the
                // vector it advertised, but `entries_for_peer` excludes it, so
                // a receiver could never cover that stamp by receiving it and
                // would ask again every round forever.
                //
                // Only on a *short* batch. A full one was truncated at the
                // limit, and there is more to come.
                let exhausted = entries.len() < MAX_BATCH;
                let applied = engine
                    .apply_batch(&entries)
                    .map_err(|e| ProtocolError::Malformed(e.to_string()));
                if applied.is_ok() && exhausted {
                    engine
                        .absorb_witnessed(&theirs)
                        .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                }
                applied
            }
            // The peer has collected what we need. Fall back to current state.
            Message::BeyondHorizon {} => {
                warn!(%peer, "behind the peer's retention horizon; falling back to a snapshot");
                pull_snapshot(engine, &mut stream).await
            }
            other => Err(ProtocolError::Malformed(format!("expected Entries, got {other:?}"))),
        }?;

        // What still trails after the round, measured against the vector the
        // peer opened with. Zero in the caught-up steady state; non-zero
        // exactly when the backlog exceeded one batch, which is the condition
        // an operator wants a gauge for. `theirs` is a round old by now, so
        // this is a floor — a peer that raced ahead during the round shows up
        // next round.
        let mine =
            engine.witnessed_vector().map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        outcome.lag_ms = kimmy_storage::lag_behind_ms(&mine, &theirs);
        Ok(outcome)
    };

    tokio::time::timeout(REQUEST_TIMEOUT, round)
        .await
        .map_err(|_| ProtocolError::Malformed("sync round timed out".into()))?
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

        let applied = engine
            .apply_snapshot_page(&page)
            .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        outcome.applied += applied;

        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    info!(documents = outcome.applied, "caught up from a snapshot");
    Ok(outcome)
}

/// Open a handshake: challenge them, check the answer, then answer theirs.
async fn open_handshake<S>(
    engine: &Engine,
    stream: &mut S,
    secret: &str,
    binding: &[u8],
) -> Result<(), ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let ours = nonce(engine.node_id());
    write_frame(stream, &Message::Hello { node: engine.node_id(), nonce: ours.clone() }).await?;

    let (their_nonce, proof) = match read_frame(stream).await? {
        Message::Welcome { nonce, proof, .. } => (nonce, proof),
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
    Ok(())
}
