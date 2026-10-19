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

/// The driver of a replication loop's exchanges: [`run`] in a task of its own,
/// aborted when this is dropped, which the loop does when it ends, and with it
/// every exchange in flight.
pub(crate) struct Driver(tokio::task::JoinHandle<()>);

impl Driver {
    pub(crate) fn spawn(
        engine: Arc<Engine>,
        secret: String,
        members: Members,
        queue: ExchangeQueue,
    ) -> Self {
        // UNSUPERVISED: owned by the replication loop and aborted with it; each exchange
        // is a task of its own whose panic it logs and outlives, and a lost exchange
        // leaves its blocks to the next sync contact.
        Self(tokio::spawn(run(engine, secret, members, queue)))
    }

    /// The driver's task, for a test that watches it end.
    #[cfg(test)]
    fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.0.abort_handle()
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Run each exchange asked on `queue`, each in a task of its own, and start each
/// trailing ask when it is due ([`Members::exchange_due_at`]), which puts it on
/// the queue.
///
/// This member's block is read here, at each ask, before the exchange's task
/// starts, so an exchange takes no storage step and an abort can end one
/// anywhere.
pub(crate) async fn run(
    engine: Arc<Engine>,
    secret: String,
    members: Members,
    mut queue: ExchangeQueue,
) {
    let mut running = tokio::task::JoinSet::new();
    loop {
        let next = members.next_trailing_exchange();
        let trailing = async move {
            match next {
                Some(due) => tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await,
                None => std::future::pending().await,
            }
        };
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
                    #[cfg(test)]
                    members.note_exchange_ended_for_test();
                });
            }
            () = trailing => {
                members.exchange_due_at(Instant::now());
            }
            // A trailing ask scheduled since `next` was read: read it again.
            () = members.exchange_scheduled() => {}
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
    exchange_within(engine, peer, secret, members, ours, EXCHANGE_TIMEOUT).await
}

/// [`exchange`], waiting `limit` for the answer once dialled and authenticated.
async fn exchange_within(
    engine: &Engine,
    peer: SocketAddr,
    secret: &str,
    members: &Members,
    ours: Option<(Arc<Facts>, u64)>,
    limit: Duration,
) -> Result<NodeId, ProtocolError> {
    let (mut stream, their_node) = dial(engine, peer, secret).await?;
    let answered = tokio::time::timeout(limit, frames(&mut stream, their_node, members, ours))
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
        let yields = Arc::new(parking_lot::Mutex::new(yields));
        peer_yielding(boot, yields).await
    }

    /// [`peer`], whose block yields webhooks while `yields` says so.
    async fn peer_yielding(boot: u8, yields: Arc<parking_lot::Mutex<bool>>) -> Peer {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let members = Members::default();
        members.configure_lease(Duration::from_secs(5), 3);
        members.set_facts_source(Arc::new(move || Facts {
            boot: vec![boot; 16],
            started_ms: Some(1_000),
            yielding: Yielding { webhooks: *yields.lock(), ..Yielding::default() },
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

    /// Wait (within 30 s) until `done` holds.
    async fn until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !done() {
            assert!(Instant::now() < deadline, "never: {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn driver(member: &Peer) -> Driver {
        let queue = member.members.take_exchange_queue().expect("the queue, once");
        Driver::spawn(Arc::clone(&member.engine), SECRET.into(), member.members.clone(), queue)
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

    /// An ask made before the driver has taken the queue (the daemon starts
    /// membership before replication) is run once it does.
    #[tokio::test]
    async fn an_ask_made_before_the_driver_starts_runs_when_it_does() {
        let (a, b) = (peer(1, false).await, peer(2, false).await);
        let b_node = b.engine.node_id();
        a.members.insert_for_test(b.addr, b_node);
        assert!(a.members.exchange_at(b.addr, b_node, Instant::now()));
        let _driver = driver(&a);
        until("the early ask's exchange", || held_boot(&a.members, b_node).is_some()).await;
        a.serving.abort();
        b.serving.abort();
    }

    /// A failed exchange counts as the member's last ask as a good one does: a
    /// member that does not answer costs one exchange an interval.
    #[tokio::test]
    async fn a_failed_exchange_still_holds_the_next_ask_for_the_interval() {
        let a = peer(1, false).await;
        let closed = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let node = NodeId::from_bytes([7; 16]);
        let _driver = driver(&a);
        assert!(a.members.exchange_at(closed, node, Instant::now()));
        until("the failed exchange to end", || a.members.exchanges_ended_for_test() >= 1).await;
        assert!(!a.members.exchange_at(closed, node, Instant::now()), "held for the interval");
        a.serving.abort();
    }

    /// Each ask carries this member's block as it is at the ask: one asked after
    /// the block changed carries the change.
    #[tokio::test]
    async fn each_exchange_carries_the_block_as_it_is_at_its_ask() {
        let yields = Arc::new(parking_lot::Mutex::new(false));
        let (a, b) = (peer_yielding(1, Arc::clone(&yields)).await, peer(2, false).await);
        let (a_node, b_node) = (a.engine.node_id(), b.engine.node_id());
        let held_yield =
            || b.members.view().blocks.get(&a_node).map(|held| held.facts.yielding.webhooks);
        let _driver = driver(&a);
        let t0 = Instant::now();
        assert!(a.members.exchange_at(b.addr, b_node, t0));
        until("the first exchange", || a.members.exchanges_ended_for_test() >= 1).await;
        assert_eq!(held_yield(), Some(false));
        // The block changes, and is rebuilt (its cache holds a block for a second).
        *yields.lock() = true;
        let _ = a.members.local_facts_at(Instant::now() + Duration::from_secs(2));
        assert!(a.members.exchange_at(b.addr, b_node, t0 + Duration::from_secs(10)));
        until("the second exchange", || a.members.exchanges_ended_for_test() >= 2).await;
        assert_eq!(held_yield(), Some(true), "the block as it was at the second ask");
        a.serving.abort();
        b.serving.abort();
    }

    /// A member that completes the handshake and never answers: the exchange
    /// ends at its bound.
    #[tokio::test]
    async fn an_exchange_with_a_member_that_never_answers_ends_at_its_bound() {
        let a = peer(1, false).await;
        let dir = tempfile::tempdir().unwrap();
        let silent = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tls = crate::tls::ClusterTls::new().unwrap();
        let serving = tokio::spawn({
            let silent = Arc::clone(&silent);
            async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut stream = tls.acceptor().accept(tcp).await.unwrap();
                let binding = crate::tls::binding(stream.get_ref().1).unwrap();
                crate::transport::accept_handshake(&silent, &mut stream, SECRET, &binding)
                    .await
                    .unwrap();
                let _asked = crate::protocol::read_frame(&mut stream).await.unwrap();
                std::future::pending::<()>().await;
            }
        });
        let ours = a.members.local_facts();
        let limit = Duration::from_millis(300);
        let answered = tokio::time::timeout(
            Duration::from_secs(20),
            exchange_within(&a.engine, addr, SECRET, &a.members, ours, limit),
        )
        .await
        .expect("the exchange ends at its bound");
        assert!(matches!(answered, Err(ProtocolError::TimedOut(_))), "{answered:?}");
        serving.abort();
        a.serving.abort();
    }

    /// Dropping the driver, as the replication loop does when it ends, ends it.
    #[tokio::test]
    async fn dropping_the_driver_ends_it() {
        let a = peer(1, false).await;
        let driver = driver(&a);
        let task = driver.abort_handle();
        assert!(!task.is_finished(), "premise: it runs");
        drop(driver);
        until("the driver to end", || task.is_finished()).await;
        a.serving.abort();
    }

    /// The driver starts a trailing ask when it is due: the member's block was
    /// dropped (declared down and up again) within the interval of the last ask,
    /// whose own ask the interval holds.
    #[tokio::test]
    async fn the_driver_starts_a_trailing_ask_when_it_is_due() {
        let (a, b) = (peer(1, false).await, peer(2, false).await);
        let b_node = b.engine.node_id();
        a.members.configure_lease(Duration::from_secs(2), 3);
        a.members.insert_for_test(b.addr, b_node);
        let _driver = driver(&a);
        assert!(a.members.exchange_at(b.addr, b_node, Instant::now()));
        until("the first exchange", || a.members.exchanges_ended_for_test() >= 1).await;
        assert!(held_boot(&a.members, b_node).is_some());
        a.members.remove_for_test(&b.addr);
        a.members.insert_for_test(b.addr, b_node);
        assert!(held_boot(&a.members, b_node).is_none(), "premise: dropped");
        assert!(!a.members.exchange_at(b.addr, b_node, Instant::now()), "held by the interval");
        assert!(a.members.next_trailing_exchange().is_some(), "a trailing ask is owed");
        until("the trailing exchange", || held_boot(&a.members, b_node).is_some()).await;
        assert_eq!(a.members.exchanges_ended_for_test(), 2);
        assert!(a.members.next_trailing_exchange().is_none());
        a.serving.abort();
        b.serving.abort();
    }
}
