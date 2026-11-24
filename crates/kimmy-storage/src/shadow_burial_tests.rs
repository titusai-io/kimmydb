//! The shadow burial and its notice (ADR-272, decisions 5 and 12; Q6.5-2, series B3): a physical the settle pass buries takes its vector
//! shadow with it, and every burial the pass makes is announced once, by the commit that landed it, naming the BURIED PHYSICAL (its
//! database, its own name and every id that went with it).
//!
//! Two consumers wait for the notice: the vector graph cache of kimmy-api (which derives the shadow from the notice's database and name,
//! and ignores the ids) and a stream scoped to one collection. A notice that names the logical name or the physical that is visible now
//! makes the first reconcile the wrong shadow, so the tests name the physical in a first flip (the identity is buried) and in a second
//! flip (a stage is buried), where the three names differ.
//!
//! The fixture is the swap of `out_counter_tests`: `orders`, two long-lived stages and a stage that can never be visible.

use std::time::Duration;

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, DocId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VectorRecord,
    VersionVector, vector_meta,
};

use crate::{
    BuriedCollection, ChangeEvent, Engine, InvalidateReason, PeerWindow, SyncOutcome, WalkScope,
    WatchOptions, WatchScope, physical_now_ms,
};

const DB: &str = "app";
/// A stage with a short deadline: doomed by any event stamped above it that makes another physical visible.
const SHORT: &str = "orders.__stg.s1";
/// The stage the tests bind first.
const FIRST: &str = "orders.__stg.s2";
/// The stage the tests bind second.
const SECOND: &str = "orders.__stg.s3";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    (Engine::open(&dir.path().join("kimmy.redb")).expect("an engine opens"), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .expect("the oplog is readable")
        .entries
}

fn try_window(b: &Engine, entries: &[OplogEntry]) -> crate::Result<SyncOutcome> {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector()?;
    let mut outcome = SyncOutcome::default();
    b.apply_peer_window_into(
        &theirs,
        Some(&mine),
        entries,
        PeerWindow {
            scanned_to: entries.last().map_or(Hlc::ZERO, |e| e.stamp.hlc),
            exhausted: true,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )?;
    Ok(outcome)
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    try_window(b, entries).expect("the window applies")
}

fn creation_of(a: &Engine, physical: &str) -> OplogEntry {
    everything(a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, physical)
        })
        .expect("the stage's creation is in the author's oplog")
}

fn bind_of(a: &Engine, physical: &str, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: physical.into(),
            })
            .expect("the binding encodes"),
        ),
        meta: None,
    }
}

/// A drop of `orders`, as the author `a` would mint it at `wall_ms`.
fn drop_of(a: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::DropCollection,
        collection: orders_id(),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders"))
                .expect("the drop encodes"),
        ),
        meta: None,
    }
}

/// The author of the stages and of the bindings of the two long-lived ones, the second stamped above the first.
struct World {
    short: OplogEntry,
    first: OplogEntry,
    second: OplogEntry,
    bind_first: OplogEntry,
    bind_second: OplogEntry,
    /// A logical drop of the name, stamped above both bindings.
    drop: OplogEntry,
    _author: (Engine, tempfile::TempDir),
}

fn world() -> World {
    let (a, dir) = engine();
    let now = physical_now_ms();
    a.create_stage(DB, SHORT, MergePolicy::Document, Hlc::new(now + 5_000, 0))
        .expect("the short-lived stage is created");
    a.create_stage(DB, FIRST, MergePolicy::Document, Hlc::new(now + 120_000, 0))
        .expect("the first long-lived stage is created");
    a.create_stage(DB, SECOND, MergePolicy::Document, Hlc::new(now + 121_000, 0))
        .expect("the second long-lived stage is created");
    let (short, first, second) =
        (creation_of(&a, SHORT), creation_of(&a, FIRST), creation_of(&a, SECOND));
    let bind_first = bind_of(&a, FIRST, now + 20_000);
    let bind_second = bind_of(&a, SECOND, now + 30_000);
    let drop = drop_of(&a, now + 40_000);
    World { short, first, second, bind_first, bind_second, drop, _author: (a, dir) }
}

fn vector_config() -> kimmy_core::VectorConfig {
    kimmy_core::VectorConfig {
        fields: vec!["text".into()],
        provider: kimmy_core::ProviderConfig::Byo {},
        dim: 4,
        metric: Default::default(),
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

/// A member holding `orders` with a document, its vector configuration (made before any stage exists, as the door requires) and one
/// chunk in the shadow, and the creations of `stages`.
fn member_with_vectors(stages: &[&OplogEntry]) -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").expect("the identity is created");
    m.insert(&orders, doc! { "_id": 1, "text": "old" }).expect("the identity takes a document");
    m.configure_vectors(DB, "orders", vector_config()).expect("vectors are configured");
    let shadow = m.vector_collection(DB, "orders").unwrap().expect("the shadow exists");
    let source = DocId::Int64(1);
    m.put_vectors(
        &shadow,
        &source,
        &[VectorRecord {
            source: source.clone(),
            chunk: 0,
            source_hlc: Hlc::new(1, 0),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            text: "old".into(),
            config: None,
        }],
    )
    .expect("the shadow takes a chunk");
    for creation in stages {
        window(&m, std::slice::from_ref(*creation));
    }
    (m, d)
}

/// A member holding `orders` with a document and no vectors, and the creations of `stages`.
fn member_plain(stages: &[&OplogEntry]) -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").expect("the identity is created");
    m.insert(&orders, doc! { "_id": 1, "text": "old" }).expect("the identity takes a document");
    for creation in stages {
        window(&m, std::slice::from_ref(*creation));
    }
    (m, d)
}

fn orders_id() -> CollectionId {
    CollectionId::derive(DB, "orders")
}

fn shadow_id() -> CollectionId {
    CollectionId::derive(DB, &vector_meta::shadow_name("orders"))
}

fn physical_served(e: &Engine) -> String {
    e.resolve(DB, "orders").expect("the name resolves").physical
}

/// Every collection the notices queued on `rx` named, in the order they came, whatever the notices that carried them.
fn notices(
    rx: &mut tokio::sync::broadcast::Receiver<std::sync::Arc<crate::Buried>>,
) -> Vec<BuriedCollection> {
    let mut named = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(notice) => named.extend(notice.collections.iter().cloned()),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => return named,
            Err(e) => panic!("the subscriber lost a notice: {e:?}"),
        }
    }
}

fn named(db: &str, name: &str, ids: &[CollectionId]) -> BuriedCollection {
    BuriedCollection { db: db.into(), name: name.into(), ids: ids.to_vec() }
}

// -- Test 12: the shadow ---------------------------------------------------------------------------------------------------------------

#[test]
fn the_flip_buries_the_vector_shadow_of_the_identity_with_a_tombstone_at_the_binding_and_defers_its_rows_with_the_parents()
 {
    let w = world();
    let (m, _d) = member_with_vectors(&[&w.first]);
    m.set_purge_deferral(Duration::from_secs(60));
    let shadow_name = vector_meta::shadow_name("orders");
    // premises: the shadow is alive with a row, and no tombstone stands for it
    let shadow = m.get_collection(DB, &shadow_name).expect("premise: the shadow is alive");
    assert_eq!(shadow.id, shadow_id(), "premise: the shadow's id is the one the name derives");
    assert_eq!(
        m.get_vectors(&shadow, &DocId::Int64(1)).expect("the shadow reads").len(),
        1,
        "premise: the shadow holds a chunk"
    );
    assert!(m.rows_under(shadow_id()).unwrap() > 0, "premise: the shadow has rows");
    assert_eq!(m.collection_dropped_at(shadow_id()).unwrap(), None, "premise: no tombstone yet");

    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(physical_served(&m), FIRST, "premise: the binding is visible");
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the identity is buried");

    assert!(
        m.get_collection(DB, &shadow_name).is_err(),
        "the identity's vector shadow is not left standing with no parent"
    );
    assert_eq!(
        m.collection_dropped_at(shadow_id()).unwrap(),
        Some(w.bind_first.stamp),
        "the shadow carries a tombstone at the binding's stamp, as its parent does"
    );
    assert_eq!(
        m.collection_dropped_at(orders_id()).unwrap(),
        Some(w.bind_first.stamp),
        "premise: the parent's tombstone is the binding's"
    );
    assert!(
        m.purge_pending(shadow_id()).unwrap(),
        "the shadow's rows are owed to the purger with the parent's"
    );
    assert_eq!(m.purge_chunk(shadow_id()).unwrap(), 0, "and deferred as the parent's are");
    assert!(m.rows_under(shadow_id()).unwrap() > 0, "so they are still there");
    m.set_clock_offset_for_tests(61_000);
    assert!(m.purge_chunk(shadow_id()).unwrap() > 0, "after the instant the purger takes them");
    assert_eq!(m.rows_under(shadow_id()).unwrap(), 0);
}

#[test]
fn the_notice_of_the_first_flip_names_the_identity_and_its_shadow() {
    let w = world();
    let (m, _d) = member_with_vectors(&[&w.first]);
    let mut rx = m.subscribe_buried();
    assert!(notices(&mut rx).is_empty(), "premise: nothing announced yet");
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(physical_served(&m), FIRST, "premise: the binding is visible");
    assert_eq!(
        notices(&mut rx),
        [named(DB, "orders", &[orders_id(), shadow_id()])],
        "one burial, announced under the physical's own name with the shadow's id beside the parent's"
    );
}

#[test]
fn the_notice_of_a_second_flip_names_the_stage_it_buries_and_neither_the_logical_name_nor_the_stage_now_visible()
 {
    let w = world();
    let (m, _d) = member_with_vectors(&[&w.first, &w.second]);
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(physical_served(&m), FIRST, "premise: the first flip");
    let mut rx = m.subscribe_buried();
    assert!(
        notices(&mut rx).is_empty(),
        "premise: the first flip's notice was taken before this subscription"
    );
    window(&m, std::slice::from_ref(&w.bind_second));
    assert_eq!(physical_served(&m), SECOND, "premise: the second flip");
    assert!(
        m.get_collection(DB, FIRST).is_err(),
        "premise: the stage the first flip made visible is buried"
    );
    assert_eq!(
        notices(&mut rx),
        [named(DB, FIRST, &[CollectionId::derive(DB, FIRST)])],
        "the notice names the buried stage: not `orders`, which was buried before, and not the stage that is visible now"
    );
}

#[test]
fn every_burial_a_pass_makes_is_announced_whatever_its_cause() {
    // the identity is buried by the binding (`bound_out`) and the short-lived stage with it (`doomed`): two burials, two announcements;
    // then a logical drop of the name buries the stage that became visible (`drop`)
    let w = world();
    let (m, _d) = member_with_vectors(&[&w.short, &w.first]);
    assert!(m.get_collection(DB, SHORT).is_ok(), "premise: the short-lived stage is held");
    let mut rx = m.subscribe_buried();
    window(&m, std::slice::from_ref(&w.bind_first));
    assert!(
        m.get_collection(DB, SHORT).is_err(),
        "premise: the doomed stage was buried with the binding"
    );
    assert_eq!(
        m.out_buried(),
        [("bound_out", 1), ("doomed", 1), ("orphan", 0), ("drop", 0)],
        "premise: two burials, two causes"
    );
    let mut heard = notices(&mut rx);
    heard.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(
        heard,
        [
            named(DB, "orders", &[orders_id(), shadow_id()]),
            named(DB, SHORT, &[CollectionId::derive(DB, SHORT)]),
        ],
        "each burial the pass made is named once"
    );
    window(&m, std::slice::from_ref(&w.drop));
    assert!(m.get_collection(DB, FIRST).is_err(), "premise: the drop buried the visible stage");
    assert_eq!(
        m.out_buried(),
        [("bound_out", 1), ("doomed", 1), ("orphan", 0), ("drop", 1)],
        "premise: the third burial is a drop's"
    );
    assert_eq!(
        notices(&mut rx),
        [named(DB, FIRST, &[CollectionId::derive(DB, FIRST)])],
        "a burial by a logical drop is announced as the others are"
    );
}

#[test]
fn a_flip_that_fails_after_its_pass_announces_nothing_and_the_redelivery_announces_once() {
    let w = world();
    let (m, _d) = member_with_vectors(&[&w.first]);
    let mut rx = m.subscribe_buried();
    let passes = crate::sync::race_hooks::settle_passes();
    crate::sync::race_hooks::fail_after_the_next_settle();
    let attempt = try_window(&m, std::slice::from_ref(&w.bind_first));
    assert!(attempt.is_err(), "premise: the transaction failed: {attempt:?}");
    assert_eq!(
        crate::sync::race_hooks::settle_passes(),
        passes + 1,
        "premise: the pass ran, and buried, before it failed"
    );
    assert_eq!(physical_served(&m), "orders", "premise: nothing of the binding was committed");
    assert!(
        m.get_collection(DB, &vector_meta::shadow_name("orders")).is_ok(),
        "premise: the shadow stands, so no burial landed"
    );
    assert!(notices(&mut rx).is_empty(), "an aborted transaction announces nothing");
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(physical_served(&m), FIRST, "the redelivery made the binding visible");
    assert_eq!(
        notices(&mut rx),
        [named(DB, "orders", &[orders_id(), shadow_id()])],
        "and announces its burial once"
    );
}

// -- Test 14: a stream ends at the flip ---------------------------------------------------------------------------------------------

/// What a stream said, in order, until it was quiet for `quiet` or ended with the invalidation of a burial: the schema changes it carried on
/// the way (kind, collection, stamp), and whether the invalidation came. Anything else it says is a failure of the test.
async fn until_a_burial(
    stream: &mut crate::ChangeStream,
    engine: &Engine,
    quiet: Duration,
) -> (Vec<(OpKind, CollectionId, Stamp)>, bool) {
    let mut passed = Vec::new();
    loop {
        match tokio::time::timeout(quiet, stream.next(engine)).await {
            Err(_) => return (passed, false),
            Ok(Some(ChangeEvent::Change { entry, .. })) if !entry.kind.is_document() => {
                passed.push((entry.kind, entry.collection, entry.stamp));
            }
            Ok(Some(ChangeEvent::Invalidate { reason: InvalidateReason::CollectionDropped })) => {
                return (passed, true);
            }
            Ok(other) => panic!("the stream said something else: {other:?}"),
        }
    }
}

// This pins today's INTERIM end of a stream on a bound-out physical: the schema change of the binding is passed on, and then the stream
// ends with `CollectionDropped`. It is not the end the design wants: a client stream must never be handed a register entry, and the end of
// the stream is one event, the `CollectionReplaced` invalidate. The invalidate-reason unit (Q6.7, unit 8) amends this test on purpose.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_collection_stream_on_the_bound_out_physical_ends_at_the_flip_and_one_on_the_visible_stage_goes_on()
 {
    let w = world();
    let (m, _d) = member_plain(&[&w.first]);
    let mut on_identity = m
        .watch(WatchScope::Collection(orders_id()), WatchOptions::default())
        .expect("a stream opens on the identity");
    let mut on_stage = m
        .watch(WatchScope::Collection(CollectionId::derive(DB, FIRST)), WatchOptions::default())
        .expect("a stream opens on the stage");
    window(&m, std::slice::from_ref(&w.bind_first));
    assert_eq!(physical_served(&m), FIRST, "premise: the binding is visible");
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the identity is buried");

    let (passed, ended) = until_a_burial(&mut on_identity, &m, Duration::from_secs(3)).await;
    assert!(ended, "the stream of the physical the flip buried ends, as at any burial");
    assert_eq!(
        passed,
        [(OpKind::BindCollection, orders_id(), w.bind_first.stamp)],
        "before it ended the stream carried the schema change of this binding and nothing else"
    );
    let after = tokio::time::timeout(Duration::from_secs(3), on_identity.next(&m)).await;
    assert!(
        matches!(after, Ok(None)),
        "the stream is over once it has ended, and says nothing more: {after:?}"
    );
    // control: the stage the flip made visible was not buried, so its stream is still open and has nothing to say
    let (passed, ended) = until_a_burial(&mut on_stage, &m, Duration::from_millis(300)).await;
    assert!(!ended, "the stream of the visible stage went on");
    assert!(passed.is_empty(), "and carried nothing the flip did not mean for it: {passed:?}");
}
