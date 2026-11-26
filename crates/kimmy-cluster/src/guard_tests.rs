//! The mistaken-restore guard (D13 containment, ADR-274; tests 9a to 9h of the plan): a fresh
//! epoch's host asks, before it dials anyone, whether its own node id is live at another address.
//! The tests commit of the guard: each is red on its designed assertion against the scaffold in
//! `guard.rs`, and the controls that the scaffold also satisfies are the mutants' guards.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kimmy_core::NodeId;
use kimmy_storage::Engine;
use tokio::net::TcpListener;

use crate::guard::{GuardConfig, GuardVerdict, a_seed_answers, run_guard};
use crate::transport::probe_peer;

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

/// An address nothing listens on: a stopped host that SWIM still lists.
async fn a_stopped_hosts_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

fn own_address() -> SocketAddr {
    "127.0.0.1:1".parse().unwrap()
}

/// The fresh host's guard: it is a restore of `node`, listens at `own_address()`, and a window and
/// a seed timeout short enough for a test.
fn config(node: NodeId, secret: &str) -> GuardConfig {
    GuardConfig {
        window: Some(WINDOW),
        seed_timeout: Duration::from_millis(500),
        ..GuardConfig::new(node, vec![own_address()], secret)
    }
}

// -- the probe (9a, 9d) -------------------------------------------------------------------------

/// A live member answers a probe with a `Welcome` whose proof verifies, and the probe says so.
#[tokio::test]
async fn a_live_member_answers_a_probe() {
    let host = live(SECRET).await;
    assert!(
        probe_peer(host.engine.node_id(), host.addr, SECRET).await,
        "the probe was answered by a Welcome whose proof verified"
    );
    host.serving.abort();
}

/// An answer that does not verify, and an address that answers nothing, are not answers (9d, 9c).
#[tokio::test]
async fn a_wrong_secret_and_a_closed_port_are_not_answers() {
    let host = live("another-secret-entirely").await;
    assert!(!probe_peer(host.engine.node_id(), host.addr, SECRET).await, "a bad proof");
    assert!(!probe_peer(host.engine.node_id(), a_stopped_hosts_address().await, SECRET).await);
    host.serving.abort();
}

// -- the guard (9a, 9c to 9h) -------------------------------------------------------------------

/// 9a: a live old host of the node, listed at another address, answers: the fresh host halts itself
/// with the address that answered.
#[tokio::test]
async fn a_live_host_of_the_node_halts_the_fresh_one() {
    let old = live(SECRET).await;
    let node = old.engine.node_id();
    let listed = vec![(old.addr, node)];
    let verdict = run_guard(&config(node, SECRET), || Some(listed.clone())).await;
    assert_eq!(verdict, GuardVerdict::LiveHost(old.addr));
    old.serving.abort();
}

/// 9a, 9d: the probe records nothing and acts on nothing on the probed side.
#[tokio::test]
async fn the_probed_host_records_nothing_and_is_not_halted() {
    let old = live(SECRET).await;
    let node = old.engine.node_id();
    let own_epoch = old.engine.epoch();
    let listed = vec![(old.addr, node)];
    let _ = run_guard(&config(node, SECRET), || Some(listed.clone())).await;
    assert_eq!(old.engine.newest_epoch(node).unwrap(), own_epoch, "its table is unchanged");
    assert!(old.engine.halted().is_none(), "and it is not halted");
    old.serving.abort();
}

/// 9c: a host that SWIM still lists but that is stopped does not answer, and the legitimate
/// replacement proceeds once the window has passed.
#[tokio::test]
async fn a_listed_host_that_is_stopped_does_not_halt_the_replacement() {
    let node = NodeId::generate();
    let listed = vec![(a_stopped_hosts_address().await, node)];
    let started = Instant::now();
    let verdict = run_guard(&config(node, SECRET), || Some(listed.clone())).await;
    assert_eq!(verdict, GuardVerdict::Pass);
    assert!(
        started.elapsed() < WINDOW + Duration::from_secs(2),
        "it waited no more than the window"
    );
}

/// 9d: a host that answers with a proof that does not verify is not a live host.
#[tokio::test]
async fn an_answer_with_a_bad_proof_does_not_halt_the_fresh_host() {
    let impostor = live("a-different-cluster-secret").await;
    let node = impostor.engine.node_id();
    let listed = vec![(impostor.addr, node)];
    assert_eq!(run_guard(&config(node, SECRET), || Some(listed.clone())).await, GuardVerdict::Pass);
    impostor.serving.abort();
}

/// 9e: with `--replace` honoured a live host that answers does not halt the fresh one.
#[tokio::test]
async fn replace_proceeds_although_a_live_host_answers() {
    let old = live(SECRET).await;
    let node = old.engine.node_id();
    let listed = vec![(old.addr, node)];
    let config = GuardConfig { replace: true, ..config(node, SECRET) };
    assert_eq!(run_guard(&config, || Some(listed.clone())).await, GuardVerdict::Pass);
    old.serving.abort();
}

/// 9h: no seed configured, or none reachable: nobody to ask, and no wait.
#[tokio::test]
async fn no_seed_and_no_reachable_seed_pass_at_once() {
    let node = NodeId::generate();
    for (configured, answered) in [(false, false), (true, false)] {
        let config = GuardConfig {
            seed_configured: configured,
            a_seed_answered: answered,
            ..config(node, SECRET)
        };
        let started = Instant::now();
        assert_eq!(run_guard(&config, || None).await, GuardVerdict::Pass);
        assert!(started.elapsed() < Duration::from_millis(300), "no wait: {:?}", started.elapsed());
    }
}

/// A seed answered and the first member list has not come: the guard waits for it, and probes what
/// it names.
#[tokio::test]
async fn a_seed_that_answered_waits_for_the_first_list_and_then_probes() {
    let old = live(SECRET).await;
    let node = old.engine.node_id();
    let listed = vec![(old.addr, node)];
    let born = Instant::now();
    let arrives = Duration::from_millis(250);
    let verdict =
        run_guard(&config(node, SECRET), || (born.elapsed() >= arrives).then(|| listed.clone()))
            .await;
    assert_eq!(verdict, GuardVerdict::LiveHost(old.addr));
    old.serving.abort();
}

/// A member list that never comes ends at the seed timeout, and the host proceeds.
#[tokio::test]
async fn a_member_list_that_never_comes_ends_at_the_seed_timeout() {
    let node = NodeId::generate();
    let started = Instant::now();
    let verdict = run_guard(&config(node, SECRET), || None).await;
    assert_eq!(verdict, GuardVerdict::Pass);
    assert!(
        started.elapsed() >= Duration::from_millis(450),
        "it waited for the first list: {:?}",
        started.elapsed()
    );
}

/// A list that does not name this node at another address passes at once: this host's own entry is
/// not another.
#[tokio::test]
async fn a_list_that_does_not_name_the_node_elsewhere_passes_at_once() {
    let old = live(SECRET).await;
    let node = NodeId::generate();
    let someone_else = vec![(old.addr, old.engine.node_id())];
    let own = vec![(own_address(), node)];
    for listed in [someone_else, own] {
        let started = Instant::now();
        assert_eq!(
            run_guard(&config(node, SECRET), || Some(listed.clone())).await,
            GuardVerdict::Pass
        );
        assert!(started.elapsed() < Duration::from_millis(300), "no probe, no wait");
    }
    old.serving.abort();
}

// -- a seed that answers (the guard's "a seed answered" fact) -----------------------------------

/// A seed that is up answers a probe; one that is down, or does not resolve, does not.
#[tokio::test]
async fn a_seed_that_is_up_answers_and_one_that_is_down_does_not() {
    let up = live(SECRET).await;
    let down = a_stopped_hosts_address().await;
    let guard = config(NodeId::generate(), SECRET);
    assert!(a_seed_answers(&guard, &[up.addr.to_string()]).await, "a seed that is up");
    assert!(
        !a_seed_answers(&guard, &[down.to_string(), "no-such-host.invalid:1".into()]).await,
        "a seed that is down, and a name that does not resolve"
    );
    assert!(!a_seed_answers(&guard, &[]).await, "no seeds");
    up.serving.abort();
}
