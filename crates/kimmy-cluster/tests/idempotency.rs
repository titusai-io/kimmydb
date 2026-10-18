//! Idempotency keys across members over real TCP sockets (B5, "Replication and
//! the cross-member retry", tests C1 to C9). Each member is an engine serving
//! the real replication listener; a keyed request is the engine's keyed write,
//! which is what the API's keyed routes call once the gate lets the key in.
//!
//! These are the cluster rows in process. The same rows run again over HTTP
//! against real `kimmyd` processes (`crates/kimmyd/tests/cluster.rs`, with
//! `cluster.idempotency_keys = true` on every member) once the API takes the
//! header there; the names match, `c1_` to `c9_`. C4 needs a hook between a
//! keyed write's commit and anything after it, which only a storage test can
//! place: `kimmy-storage`'s `c4_a_pull_right_after_the_commit_carries_the_record`.
//! C7, the frames across a version boundary, is a codec test:
//! `protocol.rs`'s `the_idempotency_records_cross_a_version_boundary_in_both_directions`.

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bson::{Document, doc};
use kimmy_cluster::transport::{serve_with, set_test_strip_records, sync_once};
use kimmy_cluster::{Facts, IdempotencyClosed, IdempotencyGate, Members};
use kimmy_core::{DocId, Fingerprint, ScopeHash};
use kimmy_storage::{
    Candidates, CollectionMeta, Engine, IdempotencyRecord, Keyed, KeyedWrite, ModifySpec,
    RecordOutcome, WalkScope,
};
use tokio::net::TcpListener;

const SECRET: &str = "an-idempotency-cluster-secret";

struct Node {
    engine: Arc<Engine>,
    addr: std::net::SocketAddr,
    serving: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

async fn node() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(&dir.path().join("kimmy.redb")).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tls = Arc::new(kimmy_cluster::tls::ClusterTls::new().expect("cluster TLS"));
    let serving = tokio::spawn(serve_with(
        Arc::clone(&engine),
        listener,
        SECRET.to_string(),
        None,
        None,
        None,
        tls,
        None,
    ));
    Node { engine, addr, serving, _dir: dir }
}

impl Node {
    /// The member stops: it serves no one from here on.
    fn stop(&self) {
        self.serving.abort();
    }

    fn orders(&self) -> CollectionMeta {
        self.engine.get_collection("shop", "orders").unwrap()
    }

    fn n(&self, id: i64) -> i64 {
        self.engine.get(&self.orders(), &DocId::Int64(id)).unwrap().unwrap().get_i64("n").unwrap()
    }

    fn count(&self) -> u64 {
        self.engine.count(&self.orders(), WalkScope::Request).unwrap()
    }

    fn record(&self, k: &KeyedWrite) -> Option<IdempotencyRecord> {
        self.engine.idempotency_record(self.orders().id, &k.scope).unwrap()
    }

    /// `$inc` of `n` on document `id` under `k`.
    fn keyed_inc(&self, id: i64, k: &KeyedWrite) -> Keyed<kimmy_storage::ModifyManyOutcome> {
        self.engine
            .modify_where_keyed(&self.orders(), &Candidates::Scan, &Inc(id), Some(1), k)
            .unwrap()
    }
}

/// `into` pulls one round from `from`.
async fn pull(into: &Node, from: &Node) -> kimmy_storage::SyncOutcome {
    sync_once(&into.engine, from.addr, SECRET, None).await.expect("a pull")
}

async fn sync(a: &Node, b: &Node) {
    pull(a, b).await;
    pull(b, a).await;
}

fn keyed(n: u8) -> KeyedWrite {
    KeyedWrite::new(ScopeHash([n; 32]), Fingerprint([n; 32]))
}

fn replayed<T: std::fmt::Debug>(answer: Keyed<T>) -> IdempotencyRecord {
    match answer {
        Keyed::Replayed(record) => record,
        Keyed::Fresh(value) => panic!("a replay was expected; the retry applied again: {value:?}"),
    }
}

fn fresh<T: std::fmt::Debug>(answer: Keyed<T>) -> T {
    match answer {
        Keyed::Fresh(value) => value,
        Keyed::Replayed(record) => panic!("a fresh write was expected; replayed {record:?}"),
    }
}

/// `$inc` of `n` on the document whose `_id` is the one named.
struct Inc(i64);

impl ModifySpec for Inc {
    fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
        Ok(doc.get_i64("_id").is_ok_and(|id| id == self.0) && doc.contains_key("n"))
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.insert("n", doc.get_i64("n").map_err(|e| e.to_string())? + 1);
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<Result<Document, String>> {
        None
    }
}

/// A and B, with `shop.orders` on both, holding `{_id: 1, n: 0}`.
async fn pair() -> (Node, Node) {
    let (a, b) = (node().await, node().await);
    let orders = a.engine.create_collection("shop", "orders").unwrap();
    a.engine.insert(&orders, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    sync(&a, &b).await;
    assert_eq!(b.n(1), 0, "premise: B holds the document");
    (a, b)
}

/// C1. A keyed write replicated to B, and the retry sent to B: B answers it from
/// A's record, a replay whose origin is A (the API counts it in
/// `peer_replays`), and the write is applied once.
#[tokio::test]
async fn c1_a_retry_on_the_member_the_write_replicated_to_replays() {
    let (a, b) = pair().await;
    let k = keyed(1);
    let (id, first) = fresh(a.engine.insert_keyed(&a.orders(), doc! { "v": 1 }, &k).unwrap());
    let pulled = b.engine.idempotency_records_received()[0];
    pull(&b, &a).await;
    assert_eq!(b.engine.idempotency_records_received()[0], pulled + 1, "by pull");
    let again = replayed(b.engine.insert_keyed(&b.orders(), doc! { "v": 1 }, &k).unwrap());
    assert_ne!(again.origin, b.engine.node_id(), "a peer replay");
    assert_eq!(again.origin, a.engine.node_id());
    assert_eq!(again.outcome, RecordOutcome::Insert { stamp: first });
    assert!(b.engine.get(&b.orders(), &id).unwrap().is_some());
    assert_eq!(b.count(), 2, "applied once");

    // A keyed `$inc`, the same way: B's retry leaves it applied once.
    let k = keyed(11);
    fresh(a.keyed_inc(1, &k));
    pull(&b, &a).await;
    replayed(b.keyed_inc(1, &k));
    assert_eq!(b.n(1), 1);
}

/// C1 by push: the record rides the window a confirmation pushes, and is
/// counted by push.
#[tokio::test]
async fn c1_a_pushed_window_carries_the_record() {
    let (a, b) = pair().await;
    // B's listener with a member set, so the confirmation can push to it.
    let k = keyed(2);
    fresh(a.keyed_inc(1, &k));
    let entry = a
        .engine
        .entries_for_peer(kimmy_core::Hlc::ZERO, 1024, WalkScope::Background)
        .unwrap()
        .entries
        .last()
        .cloned()
        .unwrap();
    let pushed = b.engine.idempotency_records_received()[1];
    let members = Members::default();
    members.insert_for_test(b.addr, b.engine.node_id());
    let confirmer = kimmy_cluster::Confirmer::new(
        Arc::clone(&a.engine),
        SECRET.into(),
        members,
        kimmy_cluster::ConfirmConfig::new(Duration::from_secs(1)),
    );
    confirmer.confirm(b.addr, b.engine.node_id(), entry, Duration::from_secs(10)).await;
    assert_eq!(b.engine.idempotency_records_received()[1], pushed + 1, "by push");
    replayed(b.keyed_inc(1, &k));
    assert_eq!(b.n(1), 1);
}

/// C2. A commits a keyed insert and stops before anyone pulls it; the retry
/// goes to B, which has no record and applies it. When A returns, both hold one
/// document under the key's derived `_id`, and one record: B's outcome, the
/// later application's, with A's `first`. The cut digests compare equal.
#[tokio::test]
async fn c2_a_retry_applied_elsewhere_converges_to_one_document_and_one_record() {
    let (a, b) = pair().await;
    let k = keyed(3);
    let (id_a, first_a) = fresh(a.engine.insert_keyed(&a.orders(), doc! { "v": 1 }, &k).unwrap());
    // A stops here: nothing pulls from it. The retry goes to B.
    tokio::time::sleep(Duration::from_millis(2)).await;
    let (id_b, first_b) = fresh(b.engine.insert_keyed(&b.orders(), doc! { "v": 1 }, &k).unwrap());
    assert_eq!(id_a, id_b, "the derived `_id`");
    // A returns.
    sync(&a, &b).await;
    sync(&a, &b).await;
    for node in [&a, &b] {
        assert_eq!(node.count(), 2, "one document of the request, beside the base");
        let r = node.record(&k).expect("a record");
        assert_eq!(r.first, first_a, "A's first");
        assert_eq!(r.last, first_b);
        assert_eq!(r.outcome, RecordOutcome::Insert { stamp: first_b }, "B's outcome");
    }
    assert_eq!(a.record(&k), b.record(&k), "the records converge");
    assert_eq!(
        a.engine.digest_row(a.orders().id).unwrap().map(|r| r.digest),
        b.engine.digest_row(b.orders().id).unwrap().map(|r| r.digest),
        "the cut digests compare equal"
    );
}

/// C3. The same keyed `$inc` applied on A and on B from the same version, before
/// either receives the other's: once they have, both hold base + 1, not base +
/// 2, one record, and a retry on either replays.
#[tokio::test]
async fn c3_one_keyed_inc_applied_on_two_members_ends_at_base_plus_one() {
    let (a, b) = pair().await;
    let k = keyed(4);
    fresh(a.keyed_inc(1, &k));
    tokio::time::sleep(Duration::from_millis(2)).await;
    fresh(b.keyed_inc(1, &k));
    sync(&a, &b).await;
    sync(&a, &b).await;
    assert_eq!((a.n(1), b.n(1)), (1, 1), "base + 1 on both");
    assert_eq!(a.record(&k), b.record(&k));
    replayed(a.keyed_inc(1, &k));
    replayed(b.keyed_inc(1, &k));
    assert_eq!((a.n(1), b.n(1)), (1, 1));
}

/// C5. A's keyed entry loses on B to a newer independent write of the same
/// document, so B does not append it; B stores its record all the same, and the
/// retry on B is answered from it rather than applied after the version that
/// beat it.
#[tokio::test]
async fn c5_a_retry_replays_where_the_keyed_entry_lost() {
    let (a, b) = pair().await;
    let k = keyed(5);
    fresh(a.keyed_inc(1, &k));
    tokio::time::sleep(Duration::from_millis(2)).await;
    b.engine.replace(&b.orders(), &DocId::Int64(1), doc! { "n": 10i64 }, false).unwrap();
    let outcome = pull(&b, &a).await;
    assert_eq!(outcome.superseded, 1, "premise: A's entry lost on B: {outcome:?}");
    assert!(b.record(&k).is_some(), "stored on a loss");
    let again = replayed(b.keyed_inc(1, &k));
    assert_eq!(again.origin, a.engine.node_id());
    assert_eq!(b.n(1), 10, "not applied after the version that beat it");
}

/// C6. A member behind A's horizon catches up by snapshot, and takes the
/// records with the documents: a retry there replays.
#[tokio::test]
async fn c6_a_member_that_caught_up_by_snapshot_replays() {
    let a = node().await;
    let orders = a.engine.create_collection("shop", "orders").unwrap();
    for i in 0..20i64 {
        a.engine.insert(&orders, doc! { "_id": i, "n": 0i64 }).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let k = keyed(6);
    let (_, at) = fresh(a.engine.insert_keyed(&orders, doc! { "v": 1 }, &k).unwrap());
    // A has collected its history below the keyed write; the write and its
    // record are inside the window.
    let pass = a
        .engine
        .collect_garbage_at(at.hlc.wall_ms - 1, kimmy_storage::RetentionPolicy::new(0, u64::MAX))
        .unwrap();
    assert!(pass.oplog_removed > 0 && pass.records_removed == 0, "premise: {pass:?}");

    let b = node().await;
    let snapshots = b.engine.idempotency_records_received()[3];
    let outcome = pull(&b, &a).await;
    assert!(outcome.applied >= 21, "caught up: {outcome:?}");
    assert_eq!(b.engine.idempotency_records_received()[3], snapshots + 1, "by snapshot");
    let again = replayed(b.engine.insert_keyed(&b.orders(), doc! { "v": 1 }, &k).unwrap());
    assert_eq!(again.origin, a.engine.node_id());
    assert_eq!(b.count(), 21);
}

/// C8. A key used in one life of a collection, dropped and created again: the
/// same key applies afresh in the new life, on every member, and the old
/// life's record is never brought back.
#[tokio::test]
async fn c8_a_key_from_before_a_drop_and_recreate_applies_afresh() {
    let (a, b) = pair().await;
    let k = keyed(8);
    fresh(a.engine.insert_keyed(&a.orders(), doc! { "_id": 50i64, "v": 1 }, &k).unwrap());
    sync(&a, &b).await;
    assert!(b.record(&k).is_some(), "premise");
    a.engine.drop_collection("shop", "orders").unwrap();
    a.engine.finish_purges_now().unwrap();
    a.engine.create_collection("shop", "orders").unwrap();
    sync(&a, &b).await;
    b.engine.finish_purges_now().unwrap();
    sync(&a, &b).await;
    assert_eq!(b.count(), 0, "premise: B holds the new life");
    fresh(b.engine.insert_keyed(&b.orders(), doc! { "_id": 50i64, "v": 1 }, &k).unwrap());
    sync(&a, &b).await;
    assert_eq!((a.count(), b.count()), (1, 1));
    replayed(a.engine.insert_keyed(&a.orders(), doc! { "_id": 50i64, "v": 1 }, &k).unwrap());
}

// -- C9: a member that does not carry records -----------------------------------------------

/// What a member of a release before 0.52 says about itself: nothing of records.
fn older_block() -> Facts {
    Facts { boot: vec![9; 16], ..Facts::default() }
}

fn carrying_block() -> Facts {
    Facts { boot: vec![9; 16], idempotency_records: Some(true), ..Facts::default() }
}

/// A gate over a member set that lists each of `peers` live, with the block it
/// sends.
fn gate(peers: &[(&Node, Facts)]) -> (Arc<IdempotencyGate>, Members) {
    let members = Members::default();
    for (peer, facts) in peers {
        members.insert_for_test(peer.addr, peer.engine.node_id());
        members.record_peer_facts_for_test(peer.engine.node_id(), facts.clone(), Duration::ZERO);
    }
    let gate = IdempotencyGate::new(true, true, true);
    gate.set_members(members.clone());
    (gate, members)
}

/// A client's keyed `$inc` through a member's gate: refused when it is closed,
/// and then nothing is written.
fn through_gate(
    gate: &IdempotencyGate,
    node: &Node,
    id: i64,
    k: &KeyedWrite,
) -> Result<Keyed<kimmy_storage::ModifyManyOutcome>, IdempotencyClosed> {
    gate.check()?;
    Ok(node.keyed_inc(id, k))
}

/// C9. Member C runs a release before 0.52: it serves every frame and page
/// without records (the test switch strips its sender) and its block omits the
/// field. While C is live, the gate refuses a key on A and on B, naming C, and
/// the schedule "keyed `$inc` on A; C pulls from A; B pulls from C before A; A
/// stops; retry on B" writes nothing. Taken past the gate, the same schedule
/// ends at base + 2, which is what the net prevents. With C on 0.52 it replays
/// and ends at base + 1.
#[tokio::test]
async fn c9_a_live_member_that_does_not_carry_records_closes_the_gate() {
    let (a, b) = pair().await;
    // One document for each part of the test: through the gates, past them, and
    // with C on 0.52.
    let orders = a.orders();
    a.engine.insert(&orders, doc! { "_id": 2i64, "n": 0i64 }).unwrap();
    a.engine.insert(&orders, doc! { "_id": 3i64, "n": 0i64 }).unwrap();
    sync(&a, &b).await;
    let c = node().await;
    sync(&c, &a).await;
    assert_eq!((b.n(3), c.n(3)), (0, 0), "premise: B and C hold the documents");
    set_test_strip_records(vec![c.engine.node_id()]);

    let (gate_a, _) = gate(&[(&b, carrying_block()), (&c, older_block())]);
    let (gate_b, members_b) = gate(&[(&a, carrying_block()), (&c, older_block())]);
    let named = IdempotencyClosed::Peers(vec![c.engine.node_id().to_string()]);
    assert_eq!(gate_a.check(), Err(named.clone()), "A refuses, naming C");
    assert_eq!(gate_b.check(), Err(named.clone()), "B refuses, naming C");

    // The schedule, through the gates: nothing is written.
    let k = keyed(9);
    assert!(through_gate(&gate_a, &a, 1, &k).is_err());
    pull(&c, &a).await;
    pull(&b, &c).await;
    members_b.remove_for_test(&a.addr);
    assert!(through_gate(&gate_b, &b, 1, &k).is_err(), "A stopped, C still live");
    assert_eq!(b.n(1), 0, "nothing applied: base");

    // The same schedule taken past the gate (what the net exists to prevent): B
    // takes the write from C without its record, and applies the retry again.
    let k = keyed(19);
    fresh(a.keyed_inc(2, &k));
    pull(&c, &a).await;
    assert!(c.record(&k).is_some(), "C stores what it receives");
    pull(&b, &c).await;
    assert_eq!(b.n(2), 1, "premise: B took the write from C");
    assert_eq!(b.record(&k), None, "and no record with it");
    fresh(b.keyed_inc(2, &k));
    assert_eq!(b.n(2), 2, "base + 2: applied twice");

    // C runs 0.52: it carries records and says so. The gates open, and the
    // schedule replays.
    set_test_strip_records(Vec::new());
    let (gate_a, _) = gate(&[(&b, carrying_block()), (&c, carrying_block())]);
    let (gate_b, members_b) = gate(&[(&a, carrying_block()), (&c, carrying_block())]);
    assert_eq!((gate_a.check(), gate_b.check()), (Ok(()), Ok(())));
    let k = keyed(29);
    fresh(through_gate(&gate_a, &a, 3, &k).unwrap());
    pull(&c, &a).await;
    pull(&b, &c).await;
    a.stop();
    members_b.remove_for_test(&a.addr);
    replayed(through_gate(&gate_b, &b, 3, &k).unwrap());
    assert_eq!(b.n(3), 1, "base + 1");
}
