//! Confirmation is the reader's (ADR-213): the serve and sync paths over the real
//! transport code, with the requester's round held after it decodes, a scripted
//! 0.43 peer, a scripted peer that echoes what it chooses, and a block that does not
//! decode.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_core::NodeId;
use kimmy_storage::Engine;

use super::*;
use crate::facts::{Facts, OwnerClass, Yielding};
use crate::peers::apply_facts_read;
use crate::protocol::Echo;
use tokio::io::AsyncWriteExt;

const SECRET: &str = "a-yield-test-secret";
const BINDING: &[u8] = b"a-yield-test-binding";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn addr(n: u16) -> SocketAddr {
    format!("127.0.0.1:{}", 7100 + n).parse().unwrap()
}

/// A member's `Members`, whose own block yields webhooks when `yielding` says so,
/// with `boot` and `started_ms`, and one live peer.
struct Member {
    members: Members,
    yielding: Arc<parking_lot::Mutex<Yielding>>,
    /// A virtual clock for the block's one-second cache: every rebuild this test
    /// asks for is a moment later than the last, so none waits.
    clock: parking_lot::Mutex<Instant>,
}

fn member(boot: u8, started_ms: u64, peer: NodeId, peer_addr: SocketAddr) -> Member {
    let members = Members::default();
    members.configure_lease(Duration::from_secs(5), 3);
    members.insert_for_test(peer_addr, peer);
    let yielding = Arc::new(parking_lot::Mutex::new(Yielding::default()));
    members.set_facts_source({
        let yielding = Arc::clone(&yielding);
        Arc::new(move || Facts {
            boot: vec![boot; 16],
            started_ms: Some(started_ms),
            yielding: *yielding.lock(),
            ..Facts::default()
        })
    });
    Member { members, yielding, clock: parking_lot::Mutex::new(Instant::now()) }
}

impl Member {
    fn yield_webhooks(&self) {
        *self.yielding.lock() = Yielding { webhooks: true, ..Yielding::default() };
    }

    /// A later moment than any this member was asked at.
    fn later(&self, by: Duration) -> Instant {
        let mut clock = self.clock.lock();
        *clock += by;
        *clock
    }

    /// The block as it would be sent, rebuilt from the source now.
    fn block(&self) -> (Arc<Facts>, u64) {
        self.members.local_facts_at(self.later(Duration::from_secs(2))).unwrap()
    }

    /// Whether this member's own view counts the webhooks yield as effective,
    /// asked far enough ahead that the live set has held still for a lease.
    fn yields_effectively(&self) -> bool {
        let later = self.later(Duration::from_secs(600));
        self.members.local_facts_effective_at(later).unwrap().yielding.webhooks
    }

    fn unconfirmed(&self) -> Vec<NodeId> {
        self.members.unconfirmed_peers(OwnerClass::Webhooks)
    }
}

/// One round: `client` asks `server` over the real serve and sync code.
async fn round(
    server: &Engine,
    server_members: &Members,
    client: &Engine,
    stalls: &mut PeerStalls,
) -> Result<SyncOutcome, ProtocolError> {
    let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
    let serving = async {
        let _ = serve_peer(
            server,
            far,
            SECRET,
            BINDING,
            None,
            ServeBudgets::serving(),
            Some(server_members),
        )
        .await;
    };
    let asking = async {
        open_handshake(client, &mut ours, SECRET, BINDING).await.unwrap();
        let outcome = sync_over(client, &mut ours, addr(1), server.node_id(), None, stalls).await;
        drop(ours);
        outcome
    };
    let ((), outcome) = tokio::join!(serving, asking);
    outcome
}

fn stalls_for(member: &Member) -> PeerStalls {
    let mut stalls = PeerStalls::new();
    let (block, generation) = member.block();
    stalls.set_local_facts(Some(block), Some(generation));
    stalls.set_members(Some(member.members.clone()));
    stalls
}

/// A1: the yielder's reply is written, and the requester has decoded it but its
/// round is held before it applies. The yielder **still owns**: a frame written is
/// not a block read. Once the requester applies it and asks again, the request
/// echoes the block, and then, and only then, the yielder stops owning.
#[tokio::test]
async fn a_yield_is_not_confirmed_by_a_frame_written_but_by_the_next_echo() {
    let (a_engine, _a) = engine();
    let (p_engine, _p) = engine();
    let (a_node, p_node) = (a_engine.node_id(), p_engine.node_id());
    // A yields webhooks; P is its one live peer. P's own view of A is the member
    // table of the requester.
    let a = member(0xA, 1_000, p_node, addr(2));
    a.yield_webhooks();
    a.block();
    let p = member(0xB, 1_000, a_node, addr(1));
    let mut stalls = stalls_for(&p);

    // P's round to A: A's reply carries its yielding block, which P decodes.
    round(&a_engine, &a.members, &p_engine, &mut stalls).await.unwrap();
    let read = stalls.take_facts_read();
    assert_eq!(read.len(), 1, "the reply's block was read");
    assert!(read[0].facts.yielding.webhooks && read[0].generation.is_some());
    // The round is held here: P has not applied it. Under the old rule the write
    // counted as a read.
    assert!(!a.yields_effectively(), "a frame written is not a block read");
    assert_eq!(a.unconfirmed(), vec![p_node]);
    // The request carried the generation of P's own block, which A recorded with it.
    let (_, p_generation) = p.block();
    assert_eq!(
        a.members.view().blocks[&p_node].generation,
        Some(p_generation),
        "ordering needs the generation on the request too"
    );
    // P's request carried an echo of what it held of A at that moment: nothing.
    assert!(stalls.take_echoes_read().iter().all(|(node, _)| *node == a_node));

    // P applies it, and its next request echoes the block.
    for read in read {
        p.members.record_peer_facts(
            read.node,
            read.facts,
            read.generation,
            read.decoded_seq,
            Instant::now(),
        );
    }
    round(&a_engine, &a.members, &p_engine, &mut stalls).await.unwrap();
    assert!(a.unconfirmed().is_empty(), "the echo confirmed it");
    assert!(a.yields_effectively());
}

/// A reply without `facts_gen` or an echo (a 0.43 peer, emulated) never confirms,
/// and a block with a `facts_gen` and no echo does not either.
#[tokio::test]
async fn the_sync_path_confirms_only_on_an_echo() {
    let (a_engine, _a) = engine();
    let (p_engine, _p) = engine();
    let p_node = p_engine.node_id();
    let a = member(0xA, 1_000, p_node, addr(2));
    a.yield_webhooks();
    let (block, generation) = a.block();
    let vector = a_engine.version_vector().unwrap();

    // The scripted peer answers with the reply it is told to.
    async fn answer(mut stream: tokio::io::DuplexStream, reply: Message) {
        match read_frame(&mut stream).await.unwrap() {
            Message::AskVersions { witnessed: true, .. } => {}
            other => panic!("{other:?}"),
        }
        write_frame(&mut stream, &reply).await.unwrap();
    }
    let mut cases = Vec::new();
    // 0.43: no block, no generation, no echo.
    cases.push((None, None, None, false));
    // 0.44 with a block and a generation and no echo.
    cases.push((Some(Facts { boot: vec![9; 16], ..Facts::default() }), Some(5), None, false));
    // An echo of a previous process of A at a higher generation: another boot.
    let old = Echo { boot: vec![0x0F; 16], generation: 500 };
    cases.push((Some(Facts { boot: vec![9; 16], ..Facts::default() }), Some(5), Some(old), false));
    // An echo of A's own boot below the generation that set the bit: not enough,
    // when the bit was set after generation 1.
    let below = Echo { boot: block.boot.clone(), generation: generation.saturating_sub(1) };
    // The echo of A's current boot at the generation it holds.
    let right = Echo { boot: block.boot.clone(), generation };
    cases.push((Some(Facts { boot: vec![9; 16], ..Facts::default() }), Some(5), Some(right), true));
    let _ = below;
    for (i, (facts, facts_gen, echo, confirms)) in cases.into_iter().enumerate() {
        let reply = Message::Vectors {
            servable: vector.clone(),
            witnessed: vector.clone(),
            facts: facts.map(Arc::new),
            facts_gen,
            echo,
            replay_from: None,
        };
        let mut stalls = stalls_for(&a);
        let (ours, theirs) = tokio::io::duplex(MAX_FRAME);
        // UNSUPERVISED: a scripted peer in a test, awaited below, whose panic fails the test
        let far = tokio::spawn(answer(theirs, reply));
        let _ = sync_over(&a_engine, ours, addr(2), p_node, None, &mut stalls).await;
        far.await.unwrap();
        apply_facts_read(&a.members, &mut stalls);
        assert_eq!(a.unconfirmed().is_empty(), confirms, "case {i}");
        a.members.note_echo(p_node, None);
    }
}

/// C6(a): P holds A's previous process at generation 500 and A restarts and yields
/// at generation 3: P's request echoes `(A_old.boot, 500)`, and its reply on A's
/// sync contact echoes the same. A must not confirm P on either path.
#[tokio::test]
async fn an_echo_of_the_previous_process_confirms_nothing_on_either_path() {
    let (a_engine, _a) = engine();
    let (p_engine, _p) = engine();
    let (a_node, p_node) = (a_engine.node_id(), p_engine.node_id());
    let a = member(0xA2, 2_000, p_node, addr(2));
    a.yield_webhooks();
    let (_, generation) = a.block();
    assert!(generation < 500);
    let old = Echo { boot: vec![0xA1; 16], generation: 500 };

    // The serve path: a scripted requester, standing in for P, asks A and echoes
    // the previous process.
    let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
    let serving = async {
        let _ = serve_peer(
            &a_engine,
            far,
            SECRET,
            BINDING,
            None,
            ServeBudgets::serving(),
            Some(&a.members),
        )
        .await;
    };
    let asking = async {
        open_handshake(&p_engine, &mut ours, SECRET, BINDING).await.unwrap();
        let ask = Message::AskVersions {
            witnessed: true,
            facts: Some(Arc::new(Facts { boot: vec![0xB; 16], ..Facts::default() })),
            facts_gen: Some(1),
            echo: Some(old.clone()),
        };
        write_frame(&mut ours, &ask).await.unwrap();
        let _ = read_frame(&mut ours).await.unwrap();
        drop(ours);
    };
    tokio::join!(serving, asking);
    assert_eq!(a.unconfirmed(), vec![p_node], "serve path: another boot at 500 confirms nothing");

    // The sync path: a scripted P answers A's request echoing the same.
    let vector = a_engine.version_vector().unwrap();
    let reply = Message::Vectors {
        servable: vector.clone(),
        witnessed: vector,
        facts: Some(Arc::new(Facts { boot: vec![0xB; 16], ..Facts::default() })),
        facts_gen: Some(500),
        echo: Some(old),
        replay_from: None,
    };
    let mut stalls = stalls_for(&a);
    let (ours, mut theirs) = tokio::io::duplex(MAX_FRAME);
    // UNSUPERVISED: a scripted peer in a test, awaited below, whose panic fails the test
    let far = tokio::spawn(async move {
        let _ = read_frame(&mut theirs).await.unwrap();
        write_frame(&mut theirs, &reply).await.unwrap();
    });
    let _ = sync_over(&a_engine, ours, addr(2), p_node, None, &mut stalls).await;
    far.await.unwrap();
    apply_facts_read(&a.members, &mut stalls);
    assert_eq!(a.unconfirmed(), vec![p_node], "sync path: the same");
    let _ = a_node;
}

/// P cannot decode A's request block (a shape the lenient decoder rejects, injected
/// through the `facts_undecodable` path): P keeps A's previous block and its reply
/// echoes the previous generation, so A never confirms P even though the reply
/// carries P's own `facts_gen`.
#[tokio::test]
async fn a_peer_that_could_not_decode_the_block_echoes_what_it_held_and_never_confirms() {
    let (a_engine, _a) = engine();
    let (p_engine, _p) = engine();
    let (a_node, p_node) = (a_engine.node_id(), p_engine.node_id());
    // P serves, and holds A's pre-yield block (generation 1) from an earlier contact.
    let p = member(0xB, 1_000, a_node, addr(1));
    let a = member(0xA, 1_000, p_node, addr(2));
    let (pre, pre_gen) = a.block();
    p.members.record_peer_facts(
        a_node,
        pre,
        Some(pre_gen),
        crate::facts::next_decoded_seq(),
        Instant::now(),
    );
    // A yields: its block is now generation pre_gen + 1.
    a.yield_webhooks();
    let (yielding_block, yielding_gen) = a.block();
    assert!(yielding_block.yielding.webhooks && yielding_gen > pre_gen);

    // The request, as it reaches P: a facts key that does not decode.
    let (mut ours, far) = tokio::io::duplex(MAX_FRAME);
    let before = crate::facts::facts_undecodable_total();
    let serving = async {
        let _ = serve_peer(
            &p_engine,
            far,
            SECRET,
            BINDING,
            None,
            ServeBudgets::serving(),
            Some(&p.members),
        )
        .await;
    };
    let asking = async {
        open_handshake(&a_engine, &mut ours, SECRET, BINDING).await.unwrap();
        let body = bson::doc! { "AskVersions": {
            "witnessed": true,
            "facts": { "ttl": "not a list" },
            "facts_gen": yielding_gen as i64,
        } };
        let bytes = bson::serialize_to_vec(&body).unwrap();
        ours.write_all(&(bytes.len() as u32).to_be_bytes()).await.unwrap();
        ours.write_all(&bytes).await.unwrap();
        let reply = read_frame(&mut ours).await.unwrap();
        drop(ours);
        reply
    };
    let ((), reply) = tokio::join!(serving, asking);
    assert!(crate::facts::facts_undecodable_total() > before, "the block did not decode");
    let Message::Vectors { facts_gen, echo, .. } = reply else { panic!("{reply:?}") };
    assert!(facts_gen.is_some(), "the reply carries P's own generation");
    let echo = echo.expect("a 0.44 peer always echoes");
    assert_eq!(echo, Echo { boot: vec![0xA; 16], generation: pre_gen }, "what P held");
    // A applies the reply: P is not confirmed.
    a.members.note_echo(p_node, Some(&echo));
    assert_eq!(a.unconfirmed(), vec![p_node]);
}

/// C2: P's contact with A's previous process decodes its block and then hangs. A
/// restarts under a new boot and yields at its first contacts, and P records and
/// confirms the new block. When P's old contact is finally applied its earlier-decoded
/// block is ignored, so P keeps the yielding one.
#[tokio::test]
async fn a_late_apply_of_the_previous_process_does_not_undo_a_yield() {
    let (a_engine, _a) = engine();
    let (p_engine, _p) = engine();
    let (a_node, p_node) = (a_engine.node_id(), p_engine.node_id());
    let p = member(0xB, 1_000, a_node, addr(1));
    let a_old = member(0xA1, 1_000, p_node, addr(2));
    let mut p_stalls = stalls_for(&p);

    // 1. P's contact with the old A decodes its block, and is held.
    round(&a_engine, &a_old.members, &p_engine, &mut p_stalls).await.unwrap();
    let held_contact = p_stalls.take_facts_read();
    assert_eq!(held_contact.len(), 1);
    assert_eq!(held_contact[0].facts.boot, vec![0xA1; 16]);

    // 2. A restarts as a new process and yields at its first contact: its request
    // reaches P, which records it, and the reply echoes it.
    let a_new = member(0xA2, 2_000, p_node, addr(2));
    a_new.yield_webhooks();
    let mut a_stalls = stalls_for(&a_new);
    round(&p_engine, &p.members, &a_engine, &mut a_stalls).await.unwrap();
    apply_facts_read(&a_new.members, &mut a_stalls);
    assert!(a_new.unconfirmed().is_empty(), "P holds A's new block and says so");
    assert!(p.members.view().blocks[&a_node].facts.yielding.webhooks);

    // 3. P's old contact is applied at last.
    for read in held_contact {
        p.members.record_peer_facts(
            read.node,
            read.facts,
            read.generation,
            read.decoded_seq,
            Instant::now(),
        );
    }
    let held = &p.members.view().blocks[&a_node];
    assert_eq!(held.facts.boot, vec![0xA2; 16], "the old process did not come back");
    assert!(held.facts.yielding.webhooks);
}

/// The same sequence as the first test, over the real listener, TLS and loopback
/// TCP: the yielder's reply is read by the requester and not yet applied, and the
/// yielder still owns; once the requester applies the block, its next request
/// carries the echo and the yield becomes effective.
#[tokio::test]
async fn a_yield_over_loopback_tls_is_confirmed_by_the_next_request_and_not_the_reply() {
    let (a_engine, _a) = engine();
    let a_engine = Arc::new(a_engine);
    let (p_engine, _p) = engine();
    let (a_node, p_node) = (a_engine.node_id(), p_engine.node_id());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a_addr = listener.local_addr().unwrap();
    let a = member(0xA, 1_000, p_node, addr(2));
    a.yield_webhooks();
    a.block();
    let tls = Arc::new(crate::tls::ClusterTls::new().unwrap());
    // UNSUPERVISED: a listener in a test, aborted below; a panic in it fails the round the test makes
    let serving = tokio::spawn(serve_with(
        Arc::clone(&a_engine),
        listener,
        SECRET.to_string(),
        None,
        None,
        None,
        tls,
        Some(a.members.clone()),
    ));
    let p = member(0xB, 1_000, a_node, a_addr);
    let mut stalls = stalls_for(&p);

    sync_once_with(&p_engine, a_addr, SECRET, None, &mut stalls).await.unwrap();
    let read = stalls.take_facts_read();
    assert_eq!(read.len(), 1, "the reply's yielding block was read");
    assert!(read[0].facts.yielding.webhooks);
    assert!(!a.yields_effectively(), "read by the requester's code, not yet applied: still owned");
    assert_eq!(a.unconfirmed(), vec![p_node]);

    for read in read {
        p.members.record_peer_facts(
            read.node,
            read.facts,
            read.generation,
            read.decoded_seq,
            Instant::now(),
        );
    }
    sync_once_with(&p_engine, a_addr, SECRET, None, &mut stalls).await.unwrap();
    assert!(a.unconfirmed().is_empty(), "the next request echoed it");
    assert!(a.yields_effectively());
    serving.abort();
}
