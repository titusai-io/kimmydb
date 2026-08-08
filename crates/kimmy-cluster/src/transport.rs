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
    info!(bind = ?local, "serving cluster replication");

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
        // One task per peer: a slow or hostile peer must not stall the others,
        // and a panic in one connection must not take the listener down.
        tokio::spawn(async move {
            if let Err(e) = serve_peer(&engine, stream, &secret).await {
                match e {
                    ProtocolError::Closed => debug!(%peer, "peer disconnected"),
                    other => warn!(%peer, error = %other, "peer connection failed"),
                }
            }
        });
    }
}

async fn serve_peer(
    engine: &Engine,
    mut stream: TcpStream,
    secret: &str,
) -> Result<(), ProtocolError> {
    let peer =
        tokio::time::timeout(HANDSHAKE_TIMEOUT, accept_handshake(engine, &mut stream, secret))
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
                // The peer's limit is a request, not an instruction: honouring
                // an arbitrary one would let it ask for the whole oplog in a
                // single frame.
                let limit = limit.min(MAX_BATCH);
                let entries = engine
                    .entries_for_peer(from, limit)
                    .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
                write_frame(&mut stream, &Message::Entries(entries)).await?;
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
async fn accept_handshake(
    engine: &Engine,
    stream: &mut TcpStream,
    secret: &str,
) -> Result<kimmy_core::NodeId, ProtocolError> {
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
            proof: prove(secret, &their_nonce),
        },
    )
    .await?;

    let Message::Confirm { proof } = read_frame(stream).await? else {
        return Err(ProtocolError::Malformed("expected Confirm".into()));
    };
    if !proof_is_valid(secret, &ours, &proof) {
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
    let mut stream = TcpStream::connect(peer).await?;

    tokio::time::timeout(HANDSHAKE_TIMEOUT, open_handshake(engine, &mut stream, secret))
        .await
        .map_err(|_| ProtocolError::Malformed("handshake timed out".into()))??;

    let round = async {
        write_frame(&mut stream, &Message::AskVersions {}).await?;
        let Message::Versions(theirs) = read_frame(&mut stream).await? else {
            return Err(ProtocolError::Malformed("expected Versions".into()));
        };

        let mine = engine.version_vector().map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        let Some(from) = mine.behind(&theirs) else {
            return Ok(SyncOutcome::default());
        };

        write_frame(&mut stream, &Message::AskEntries { from, limit: MAX_BATCH }).await?;
        let Message::Entries(entries) = read_frame(&mut stream).await? else {
            return Err(ProtocolError::Malformed("expected Entries".into()));
        };

        engine.apply_batch(&entries).map_err(|e| ProtocolError::Malformed(e.to_string()))
    };

    tokio::time::timeout(REQUEST_TIMEOUT, round)
        .await
        .map_err(|_| ProtocolError::Malformed("sync round timed out".into()))?
}

/// Open a handshake: challenge them, check the answer, then answer theirs.
async fn open_handshake(
    engine: &Engine,
    stream: &mut TcpStream,
    secret: &str,
) -> Result<(), ProtocolError> {
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
    if !proof_is_valid(secret, &ours, &proof) {
        return Err(ProtocolError::Unauthenticated);
    }

    write_frame(stream, &Message::Confirm { proof: prove(secret, &their_nonce) }).await?;
    Ok(())
}
