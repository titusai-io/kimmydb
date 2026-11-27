//! The mistaken-restore guard against real members (D13 containment, ADR-274): what its probe is
//! answered, what the probed side records of it, and what its rounds make of a running host. The
//! rounds' own rows, against a member made up for the test, are in `guard_rounds_tests.rs`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kimmy_core::NodeId;
use kimmy_storage::Engine;
use tokio::net::TcpListener;

use crate::guard_rounds::{Asking, Decision, ask_until_decided};
use crate::membership::Members;
use crate::transport::probe_answer;

const SECRET: &str = "a-guard-test-secret";
const WINDOW: Duration = Duration::from_millis(800);

/// A live member serving the cluster listener under `secret`: its engine, its address, and its
/// task.
struct Live {
    engine: Arc<Engine>,
    addr: SocketAddr,
    serving: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

async fn live(secret: &str) -> Live {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = tokio::spawn(crate::transport::serve_with(
        Arc::clone(&engine),
        listener,
        secret.into(),
        None,
        None,
        None,
        Arc::new(crate::tls::ClusterTls::new().unwrap()),
        None,
    ));
    Live { engine, addr, serving, _dir: dir }
}

/// An address nothing listens on.
async fn a_stopped_hosts_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// What a restore of `node` whose seeds are `seeds` has decided after `within`, if anything.
async fn decided(
    node: NodeId,
    seeds: &[SocketAddr],
    replace: bool,
    within: Duration,
) -> Option<Decision> {
    let asking = Asking {
        between_rounds: Duration::from_millis(100),
        ..Asking::new(node, SECRET, WINDOW, replace)
    };
    let members = Members::default();
    let rounds = ask_until_decided(&asking, &members, || std::future::ready(seeds.to_vec()));
    tokio::time::timeout(within, rounds).await.ok()
}

const SOON: Duration = Duration::from_secs(10);

/// A live member answers a probe with a `Welcome` whose proof verifies.
#[tokio::test]
async fn a_live_member_answers_a_probe() {
    let host = live(SECRET).await;
    let answered = probe_answer(host.engine.node_id(), host.addr, SECRET).await;
    assert_eq!(answered.map(|(node, _)| node), Some(host.engine.node_id()));
    host.serving.abort();
}

/// An answer that does not verify, and an address that answers nothing, are not answers (9d, 9c).
#[tokio::test]
async fn a_wrong_secret_and_a_closed_port_are_not_answers() {
    let host = live("another-secret-entirely").await;
    let node = host.engine.node_id();
    assert!(probe_answer(node, host.addr, SECRET).await.is_none(), "a bad proof");
    assert!(probe_answer(node, a_stopped_hosts_address().await, SECRET).await.is_none());
    host.serving.abort();
}

/// 9a: a running host of the node halts the fresh one, with the address that answered.
#[tokio::test]
async fn a_live_host_of_the_node_halts_the_fresh_one() {
    let old = live(SECRET).await;
    let verdict = decided(old.engine.node_id(), &[old.addr], false, SOON).await;
    assert_eq!(verdict, Some(Decision::LiveHost(old.addr)));
    old.serving.abort();
}

/// 9a, 9d: the probe records nothing and acts on nothing on the probed side.
#[tokio::test]
async fn the_probed_host_records_nothing_and_is_not_halted() {
    let old = live(SECRET).await;
    let node = old.engine.node_id();
    let own_epoch = old.engine.epoch();
    let _ = decided(node, &[old.addr], false, SOON).await;
    assert_eq!(old.engine.newest_epoch(node).unwrap(), own_epoch, "its table is unchanged");
    assert!(old.engine.halted().is_none(), "and it is not halted");
    assert!(!old.engine.has_met_a_peer(), "and it has met nobody");
    old.serving.abort();
}

/// 9d: a host that answers with a proof that does not verify is not a live host: it halts nobody,
/// and it is no answer to pass on.
#[tokio::test]
async fn an_answer_with_a_bad_proof_does_not_halt_the_fresh_host() {
    let impostor = live("a-different-cluster-secret").await;
    let node = impostor.engine.node_id();
    let verdict = decided(node, &[impostor.addr], false, Duration::from_secs(2)).await;
    assert_eq!(verdict, None);
    impostor.serving.abort();
}

/// 9e: with `--replace` honoured a running host of the node does not halt the fresh one.
#[tokio::test]
async fn replace_proceeds_although_a_live_host_answers() {
    let old = live(SECRET).await;
    let verdict = decided(old.engine.node_id(), &[old.addr], true, SOON).await;
    assert!(matches!(verdict, Some(Decision::Pass { .. })), "{verdict:?}");
    old.serving.abort();
}
