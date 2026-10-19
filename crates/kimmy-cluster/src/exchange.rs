//! The immediate facts exchange (ADR-247): a member that sees a peer come up or
//! renamed, or that rejoins itself, exchanges facts blocks with that peer at
//! once, instead of at its next sync contact, up to an interval later.
//!
//! An exchange is the opening frame pair of a sync round and nothing after it:
//! `AskVersions`, carrying this member's block and its echo of the peer's, and
//! the peer's `Vectors`, carrying the peer's block and its echo of this member's.
//! The peer records the request's block before it answers, as it does on every
//! contact, so both blocks cross in one round trip. **No frame is new.** A
//! replay the peer asks for on its `Vectors` stays owed to its next round.
//!
//! It runs beside the replication loop, not in it: a tick that drains holds the
//! loop for up to an interval, and a failed round backs its peer off, which
//! would lengthen the window an exchange is for. A failed exchange is one
//! `debug` line and changes nothing else; the tick is the fallback.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_core::NodeId;
use kimmy_storage::Engine;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, error};

use crate::facts::Facts;
use crate::membership::{ExchangeQueue, Members};
use crate::protocol::{Message, ProtocolError, read_frame_noting_facts, write_frame};
use crate::transport::{close, dial};

/// How long an exchange waits for the peer's answer once it has dialled and
/// authenticated it; the dial has bounds of its own.
pub(crate) const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);

/// Run each exchange asked on `queue`, each in a task of its own, for as long as
/// the replication loop that spawned this runs: the loop aborts this when it
/// ends, and every exchange in flight with it.
///
/// This member's block is read here, before an exchange's task starts, so an
/// exchange takes no storage step and an abort can end one anywhere.
pub(crate) async fn run(
    engine: Arc<Engine>,
    secret: String,
    members: Members,
    mut queue: ExchangeQueue,
) {
    let mut running = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            asked = queue.recv() => {
                // The sender lives in `members`, which this holds, so the queue
                // never closes while this runs.
                let Some((addr, node)) = asked else { return };
                let ours = members.local_facts();
                let (engine, secret, members) =
                    (Arc::clone(&engine), secret.clone(), members.clone());
                // UNSUPERVISED: one exchange, owned by this driver and aborted with it; a
                // panic in one is logged below and ends nothing else, and the next sync
                // contact carries the blocks.
                running.spawn(async move {
                    match exchange(&engine, addr, &secret, &members, ours).await {
                        Ok(peer) => {
                            debug!(%addr, %peer, "exchanged facts blocks with a member at once")
                        }
                        Err(e) => debug!(
                            %addr,
                            %node,
                            error = %e,
                            "an immediate facts exchange failed; the next sync contact carries \
                             the blocks"
                        ),
                    }
                });
            }
            Some(done) = running.join_next(), if !running.is_empty() => {
                if let Err(e) = done
                    && e.is_panic()
                {
                    error!(error = %e, "an immediate facts exchange panicked");
                }
            }
        }
    }
}

/// One exchange with `peer`, sending `ours` (ADR-247): dial, `AskVersions`, the
/// peer's `Vectors`, close. Answers the node that answered, whose block and echo
/// are recorded in `members`, ordered by when the answer decoded (ADR-213).
pub(crate) async fn exchange(
    engine: &Engine,
    peer: SocketAddr,
    secret: &str,
    members: &Members,
    ours: Option<(Arc<Facts>, u64)>,
) -> Result<NodeId, ProtocolError> {
    let (mut stream, their_node) = dial(engine, peer, secret).await?;
    let answered =
        tokio::time::timeout(EXCHANGE_TIMEOUT, frames(&mut stream, their_node, members, ours))
            .await
            .unwrap_or_else(|_| Err(ProtocolError::TimedOut("facts exchange".into())));
    close(&mut stream).await;
    answered.map(|()| their_node)
}

/// The frame pair of an exchange, over a dialled and authenticated stream.
async fn frames<S>(
    stream: &mut S,
    their_node: NodeId,
    members: &Members,
    ours: Option<(Arc<Facts>, u64)>,
) -> Result<(), ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (facts, facts_gen) =
        ours.map_or((None, None), |(facts, generation)| (Some(facts), Some(generation)));
    // What this member holds of the peer's block, read now (after a rename,
    // nothing): the confirmation of the peer's yield, as on every request.
    let echo = Some(members.echo_for(their_node));
    write_frame(stream, &Message::AskVersions { witnessed: true, facts, facts_gen, echo }).await?;
    let (frame, undecodable, decoded_seq) = read_frame_noting_facts(stream).await?;
    match frame {
        Message::Vectors { facts, facts_gen, echo, .. } => {
            if undecodable {
                members.note_undecodable(their_node, Instant::now());
            }
            if let Some(facts) = facts {
                members.record_peer_facts(
                    their_node,
                    facts,
                    facts_gen,
                    decoded_seq,
                    Instant::now(),
                );
            }
            members.note_echo(their_node, echo.as_ref());
            Ok(())
        }
        // A peer older than the witnessed flag answers with its vector alone,
        // and no block.
        Message::Versions(_) => Ok(()),
        Message::Fault(reason) => Err(ProtocolError::Fault(reason)),
        other => Err(ProtocolError::Malformed(format!("expected Vectors, got {other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::facts::{OwnerClass, Yielding};
    use tokio::net::TcpListener;

    const SECRET: &str = "an-exchange-test-secret";

    /// A member: its engine, its set with a block of `boot` (yielding webhooks
    /// when `yields`), and its replication listener, served.
    struct Peer {
        engine: Arc<Engine>,
        members: Members,
        addr: SocketAddr,
        serving: tokio::task::JoinHandle<()>,
        _dir: tempfile::TempDir,
    }

    async fn peer(boot: u8, yields: bool) -> Peer {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let members = Members::default();
        members.configure_lease(Duration::from_secs(5), 3);
        members.set_facts_source(Arc::new(move || Facts {
            boot: vec![boot; 16],
            started_ms: Some(1_000),
            yielding: Yielding { webhooks: yields, ..Yielding::default() },
            ..Facts::default()
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serving = tokio::spawn(crate::transport::serve_with(
            Arc::clone(&engine),
            listener,
            SECRET.into(),
            None,
            None,
            None,
            Arc::new(crate::tls::ClusterTls::new().unwrap()),
            Some(members.clone()),
        ));
        Peer { engine, members, addr, serving, _dir: dir }
    }

    fn held_boot(members: &Members, node: NodeId) -> Option<Vec<u8>> {
        members.view().blocks.get(&node).map(|held| held.facts.boot.clone())
    }

    /// One exchange: each side holds the other's block afterwards, and the
    /// asker reads the answerer's echo of its own block, which confirms its yield.
    /// Neither held anything of the other before (as after a rename).
    #[tokio::test]
    async fn an_exchange_carries_both_blocks_and_the_answerers_echo() {
        let (a, b) = (peer(1, true).await, peer(2, false).await);
        let (a_node, b_node) = (a.engine.node_id(), b.engine.node_id());
        a.members.insert_for_test(b.addr, b_node);
        b.members.insert_for_test(a.addr, a_node);
        assert_eq!(a.members.unconfirmed_peers(OwnerClass::Webhooks), vec![b_node], "premise");

        let ours = a.members.local_facts();
        let answered = exchange(&a.engine, b.addr, SECRET, &a.members, ours).await.unwrap();

        assert_eq!(answered, b_node);
        assert_eq!(held_boot(&a.members, b_node), Some(vec![2; 16]), "the answer's block");
        assert_eq!(held_boot(&b.members, a_node), Some(vec![1; 16]), "the request's block");
        assert!(
            a.members.unconfirmed_peers(OwnerClass::Webhooks).is_empty(),
            "B recorded A's block before it answered, and echoed it"
        );
        a.serving.abort();
        b.serving.abort();
    }

    /// A member that does not answer: the exchange fails at the dial, at once,
    /// and records nothing.
    #[tokio::test]
    async fn an_exchange_with_a_member_not_serving_fails_and_records_nothing() {
        let a = peer(1, false).await;
        let closed = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let ours = a.members.local_facts();
        assert!(exchange(&a.engine, closed, SECRET, &a.members, ours).await.is_err());
        assert!(a.members.view().blocks.is_empty());
        a.serving.abort();
    }

    /// The driver runs what the set asks for: an ask is an exchange, with no
    /// tick anywhere.
    #[tokio::test]
    async fn the_driver_runs_each_asked_exchange() {
        let (a, b) = (peer(1, false).await, peer(2, false).await);
        let b_node = b.engine.node_id();
        let queue = a.members.take_exchange_queue().expect("the queue, once");
        assert!(a.members.take_exchange_queue().is_none(), "once");
        let driver =
            tokio::spawn(run(Arc::clone(&a.engine), SECRET.into(), a.members.clone(), queue));
        a.members.insert_for_test(b.addr, b_node);
        assert!(a.members.exchange_at(b.addr, b_node, Instant::now()));
        let deadline = Instant::now() + Duration::from_secs(30);
        while held_boot(&a.members, b_node).is_none() {
            assert!(Instant::now() < deadline, "the asked exchange never ran");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(held_boot(&a.members, b_node), Some(vec![2; 16]));
        driver.abort();
        a.serving.abort();
        b.serving.abort();
    }
}
