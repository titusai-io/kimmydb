//! Unit S, the vector shadow of a swapped name: a snapshot page is judged by its parent's name (ADR-272, decisions 6 and 12). Tests first.
//!
//! - S1: a page whose shadow is newer than the buried shadow waits on nothing, and makes neither the shadow nor the identity;
//! - S2: after the purge, a page makes no shadow beside the swapped name and reports it orphaned;
//! - S3 (a guard): a page of documents of the swapped identity is superseded and leaves the held rows untouched.

use std::time::Duration;

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, DocId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VectorRecord,
    VersionVector, vector_meta,
};

use crate::{Engine, PeerWindow, SnapshotProgress, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
/// The stage the tests bind.
const FIRST: &str = "orders.__stg.s2";

// ---- fixtures copied from shadow_burial_tests.rs -------------------------------------------------------------------------------------------

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

/// The author of the stage `FIRST` and of its binding, and the clock of the entries addressed to it.
struct World {
    first: OplogEntry,
    bind_first: OplogEntry,
    author: Engine,
    _dir: tempfile::TempDir,
}

fn world() -> World {
    let (a, dir) = engine();
    let now = physical_now_ms();
    a.create_stage(DB, FIRST, MergePolicy::Document, Hlc::new(now + 120_000, 0))
        .expect("the stage is created");
    let first = creation_of(&a, FIRST);
    let bind_first = bind_of(&a, FIRST, now + 20_000);
    World { first, bind_first, author: a, _dir: dir }
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

fn chunk_of(source: i64) -> VectorRecord {
    VectorRecord {
        source: DocId::Int64(source),
        chunk: 0,
        source_hlc: Hlc::new(1, 0),
        vector: vec![1.0, 0.0, 0.0, 0.0],
        text: "old".into(),
        config: None,
    }
}

/// A member holding `orders` with a document, its vector configuration and one chunk in the shadow, and the creations of `stages`.
fn member_with_vectors(stages: &[&OplogEntry]) -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let orders = m.create_collection(DB, "orders").expect("the identity is created");
    m.insert(&orders, doc! { "_id": 1, "text": "old" }).expect("the identity takes a document");
    m.configure_vectors(DB, "orders", vector_config()).expect("vectors are configured");
    let shadow = m.vector_collection(DB, "orders").unwrap().expect("the shadow exists");
    m.put_vectors(&shadow, &DocId::Int64(1), &[chunk_of(1)]).expect("the shadow takes a chunk");
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

fn shadow_name() -> String {
    vector_meta::shadow_name("orders")
}

fn shadow_id() -> CollectionId {
    CollectionId::derive(DB, &shadow_name())
}

// ---- fixtures from join_tests.rs (:2436-2464, :2136-2152) ----------------------------------------------------------------------------------

/// The tombstone of each of `ids` set by hand to a stamp far below any sender a test can build: a burial leaves it at the binding's stamp, which is above every sender (`a_newer_life_page_creates_no_identity_...`).
fn lower_the_tombstones(m: &Engine, by: &Engine, ids: &[CollectionId]) -> Stamp {
    let low = Stamp::new(Hlc::new(1, 0), by.node_id(), by.epoch());
    for id in ids {
        let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
        txn.open_table(crate::tables::COLLECTIONS_DROPPED)
            .expect("the table opens")
            .insert(id.0, crate::codec::oplog_key(&low).as_slice())
            .expect("the tombstone is lowered");
        txn.commit().expect("the commit lands");
    }
    low
}

/// A sender that holds `orders` with `documents` and, when `vectors`, its configuration and a chunk of document 1 in the shadow: a life made now, above the lowered tombstones and below the binding.
fn a_sender(documents: &[i64], vectors: bool) -> (Engine, tempfile::TempDir) {
    let (a, d) = engine();
    let orders = a.create_collection(DB, "orders").expect("the sender's collection");
    for id in documents {
        a.insert(&orders, doc! { "_id": *id, "text": "new" }).expect("the sender's document");
    }
    if vectors {
        a.configure_vectors(DB, "orders", vector_config()).expect("the sender's configuration");
        let shadow = a.vector_collection(DB, "orders").unwrap().expect("the sender's shadow");
        a.put_vectors(&shadow, &DocId::Int64(1), &[chunk_of(1)]).expect("the sender's chunk");
    }
    (a, d)
}

/// Apply the whole-database pages of `sender` to `m`, bounded (a route that refuses every page whole never advances its progress). A page that waits on a purge ends the loop: its progress is not moved, so the same page would come again.
fn apply_the_pages(m: &Engine, sender: &Engine) -> (SnapshotProgress, Vec<crate::SnapshotApplied>) {
    let mut progress = SnapshotProgress::whole_database();
    let mut applied = Vec::new();
    for _ in 0..50 {
        if progress.is_complete() {
            break;
        }
        let page = sender
            .snapshot_page(progress.after().cloned(), progress.scope())
            .expect("the sender's page");
        let done = m
            .apply_snapshot_page(sender.node_id(), &mut progress, &page)
            .expect("the page applies");
        let waits = done.purge_pending.is_some();
        applied.push(done);
        if waits {
            break;
        }
    }
    (progress, applied)
}

/// The member after the flip: the identity is buried and its rows are held for the purger, a minute away.
fn flipped(m: &Engine, w: &World) {
    flipped_with(m, w, Duration::from_secs(60));
}

/// [`flipped`] with the purger's deferral of the buried physicals set to `deferral`: the register's burial records the instant before which the
/// purger leaves their rows alone, and `purge_dropped_collection` (like the purger) removes nothing until it has passed.
fn flipped_with(m: &Engine, w: &World, deferral: Duration) {
    m.set_purge_deferral(deferral);
    window(m, std::slice::from_ref(&w.bind_first));
    assert_eq!(
        m.resolve(DB, "orders").expect("the name resolves").physical,
        FIRST,
        "premise: the binding is visible"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the identity is buried");
}

// ---- S1: shadow, held ----------------------------------------------------------------------------------------------------------------------

/// A member with vectors on `orders` and a purge deferral; the flip (the shadow is buried with the identity, its rows held); a sender whose `orders` AND shadow are above the burial stamp (the tombstones lowered by hand, the shadow's too). The
/// first page. RED TODAY by hardproblems' read: the purge gate asks `name_is_swapped(db, "orders.__vectors")`, which is false (no stage carries that prefix), falls through to the tombstone test, finds the shadow's creation ABOVE the burial and its rows
/// still filed, and the page waits whole (snapshot.rs:1524-1539): `purge_pending` names the shadow's id and the progress does not move. WANTED: the page applies, no shadow and no identity are made, the rows under both ids are unchanged.
/// Mutant: the rule only in `parent_buried_since` (not in the gate): the page still waits.
#[test]
fn s1_a_page_whose_shadow_is_newer_than_the_buried_shadow_waits_on_nothing_and_makes_neither_the_shadow_nor_the_identity()
 {
    let w = world();
    let (m, _d) = member_with_vectors(&[&w.first]);
    flipped(&m, &w);
    assert!(
        m.get_collection(DB, &shadow_name()).is_err(),
        "premise: the identity's shadow is buried with it"
    );
    assert!(
        m.purge_pending(orders_id()).unwrap(),
        "premise: the identity's rows are held for the purger"
    );
    assert!(
        m.purge_pending(shadow_id()).unwrap(),
        "premise: the shadow's rows are held for the purger"
    );
    let rows = (m.rows_under(orders_id()).unwrap(), m.rows_under(shadow_id()).unwrap());
    assert!(rows.0 > 0 && rows.1 > 0, "premise: rows stand under both ids");
    lower_the_tombstones(&m, &w.author, &[orders_id(), shadow_id()]);
    let (a, _da) = a_sender(&[1], true);
    assert!(
        a.get_collection(DB, &shadow_name()).is_ok(),
        "premise: the sender holds `orders` with a shadow, both made now"
    );

    let (progress, applied) = apply_the_pages(&m, &a);

    assert!(
        applied.iter().all(|done| done.purge_pending.is_none()),
        "the page waits on no purge (it waited on {:?}: the shadow's id is {:?})",
        applied.iter().find_map(|done| done.purge_pending),
        shadow_id()
    );
    assert!(progress.is_complete(), "the pull completes: its progress moved");
    assert!(m.get_collection(DB, "orders").is_err(), "no identity is made beside the swapped name");
    assert!(
        m.get_collection(DB, &shadow_name()).is_err(),
        "no shadow is made: its parent's name is swapped here"
    );
    assert_eq!(
        (m.rows_under(orders_id()).unwrap(), m.rows_under(shadow_id()).unwrap()),
        rows,
        "the rows under both held ids are what they were"
    );
}

// ---- S2: shadow, nothing held ----------------------------------------------------------------------------------------------------------

/// The same after the purge ran (nothing filed under either id). RED TODAY by hardproblems' read: with nothing to wait for, the page's shadow is not history and not orphaned (`parent_buried_since` compares only stamps, snapshot.rs:4105-4117), so it
/// is created with no parent, under a name whose vector configuration the same page has just discarded. WANTED: no shadow is made, and `shadows_orphaned` names it (the transport then cancels a planned pull of it).
/// Mutant: the shadow is not made but not reported orphaned.
#[test]
fn s2_after_the_purge_a_page_makes_no_shadow_beside_the_swapped_name_and_reports_it_orphaned() {
    let w = world();
    let (m, _d) = member_with_vectors(&[&w.first]);
    // a deferral that passes at once: the purge below must remove the rows, which it does not while the instant is ahead
    flipped_with(&m, &w, Duration::from_millis(1));
    std::thread::sleep(Duration::from_millis(50));
    m.purge_dropped_collection(orders_id()).expect("the identity's purge runs");
    m.purge_dropped_collection(shadow_id()).expect("the shadow's purge runs");
    assert!(
        !m.purge_pending(orders_id()).unwrap() && !m.purge_pending(shadow_id()).unwrap(),
        "premise: nothing is filed under either id"
    );
    lower_the_tombstones(&m, &w.author, &[orders_id(), shadow_id()]);
    let (a, _da) = a_sender(&[1], true);

    let (progress, applied) = apply_the_pages(&m, &a);

    assert!(progress.is_complete(), "premise: the pull completes");
    assert!(m.get_collection(DB, "orders").is_err(), "no identity is made beside the swapped name");
    assert!(
        m.get_collection(DB, &shadow_name()).is_err(),
        "no shadow stands without its parent (the same page discarded the parent's configuration)"
    );
    assert!(
        applied.iter().any(|done| done.shadows_orphaned.contains(&shadow_id())),
        "the page reports the shadow orphaned ({:?})",
        applied.iter().map(|done| done.shadows_orphaned.clone()).collect::<Vec<_>>()
    );
}

// ---- S3: the document half for the identity (a GUARD) --------------------------------------------------------------------------------

/// The document half for the identity, which no product test pins (`a_snapshot_page_of_a_life_newer_...` runs with the rows pending and asserts only that no identity is made and the index lands on the stage): the same page with DOCUMENTS of `orders`.
/// The page completes, reports no purge wait, counts the documents superseded, and the rows under the held id are what they were. GREEN TODAY by hardproblems' read (snapshot.rs:1856-1890, sync.rs:1705-1723: "gone, and a tombstone says so" is history).
/// It is the property unit P's remake (a fact above the floor that arrives where the collection is gone makes it again) and F2's move of the burial mark must not break for a SWAPPED name: a GUARD, not a red test. "Byte for byte" is the row count and the
/// tombstone; there is no API that dumps the rows, so a document that changed in place is not caught (ASSUMPTION: the count plus the tombstone are what the finding's property needs; strengthen with a row read if one exists).
#[test]
fn s3_a_page_of_documents_of_the_swapped_identity_is_superseded_and_leaves_the_held_rows_untouched()
{
    let w = world();
    let (m, _d) = member_plain(&[&w.first]);
    flipped(&m, &w);
    assert!(
        m.purge_pending(orders_id()).unwrap(),
        "premise: the identity's rows are held for the purger"
    );
    let rows = m.rows_under(orders_id()).unwrap();
    assert!(rows > 0, "premise: rows stand under the held id");
    let low = lower_the_tombstones(&m, &w.author, &[orders_id()]);
    let (a, _da) = a_sender(&[1, 2, 3], false);

    let (progress, applied) = apply_the_pages(&m, &a);

    assert!(progress.is_complete(), "the page completes");
    assert!(applied.iter().all(|done| done.purge_pending.is_none()), "and reports no purge wait");
    assert!(
        applied.iter().map(|done| done.superseded).sum::<usize>() >= 3,
        "the three documents are counted superseded ({:?})",
        applied.iter().map(|done| done.superseded).collect::<Vec<_>>()
    );
    assert!(m.get_collection(DB, "orders").is_err(), "no identity is made");
    assert_eq!(
        m.rows_under(orders_id()).unwrap(),
        rows,
        "the rows under the held id are what they were"
    );
    assert_eq!(m.collection_dropped_at(orders_id()).unwrap(), Some(low), "and so is its tombstone");
}

// ---- S13: before the flip, the identity's own shadow is legitimate ---------------------------------------------------------------------------

/// A member BEFORE the flip: it holds `orders` with its vector configuration and a stage of the name, unbound, and it lacks the shadow (the
/// shadow's definition is removed and its rows purged: what a holder whose shadow has not arrived looks like). The name is swapped as far as
/// the register can tell (a stage is held), but its identity still stands with its vectors, so the shadow is the identity's own and
/// legitimate: refusing it would leave the identity with no vectors until the flip.
fn before_the_flip_missing_its_shadow(w: &World) -> (Engine, tempfile::TempDir) {
    let (m, d) = member_with_vectors(&[]);
    m.forget_collection_leaving_rows(DB, &shadow_name()).expect("the shadow's definition goes");
    m.purge_dropped_collection(shadow_id()).expect("its rows are purged");
    window(&m, std::slice::from_ref(&w.first));
    assert!(m.get_collection(DB, "orders").is_ok(), "premise: the identity stands");
    assert!(
        m.get_collection(DB, "orders").unwrap().vector.is_some(),
        "premise: it holds its vector configuration"
    );
    assert!(m.get_collection(DB, FIRST).is_ok(), "premise: the stage is held, unbound");
    assert!(m.get_collection(DB, &shadow_name()).is_err(), "premise: the shadow is missing");
    (m, d)
}

/// A whole page, whose shadow is the identity's own, makes it before the flip, and does not report it orphaned.
#[test]
fn s13a_before_the_flip_a_page_makes_the_identitys_shadow_and_reports_no_orphan() {
    let w = world();
    let (m, _d) = before_the_flip_missing_its_shadow(&w);
    let (a, _da) = a_sender(&[1], true);

    let (progress, applied) = apply_the_pages(&m, &a);

    assert!(progress.is_complete(), "the pull completes");
    assert!(
        m.get_collection(DB, &shadow_name()).is_ok(),
        "the identity's shadow is made: its parent stands here"
    );
    assert!(
        applied.iter().all(|done| done.shadows_orphaned.is_empty()),
        "and is not reported orphaned ({:?})",
        applied.iter().map(|done| done.shadows_orphaned.clone()).collect::<Vec<_>>()
    );
}

/// A shadow the identity lost (its definition dropped, its rows awaiting the purger) and the page re-creates waits on that purge as any
/// recreation does, before the flip as the purge gate judges it by the parent's name AND the identity: held, it is not a swapped shadow.
#[test]
fn s13c_before_the_flip_a_page_whose_shadow_is_newer_than_the_lost_one_waits_on_its_purge() {
    let w = world();
    let (m, _d) = member_with_vectors(&[]);
    m.forget_collection_leaving_rows(DB, &shadow_name()).expect("the shadow's definition goes");
    window(&m, std::slice::from_ref(&w.first));
    lower_the_tombstones(&m, &w.author, &[shadow_id()]);
    assert!(
        m.get_collection(DB, "orders").unwrap().vector.is_some(),
        "premise: the identity stands with its vectors"
    );
    assert!(m.get_collection(DB, FIRST).is_ok(), "premise: the stage is held, unbound");
    assert!(m.get_collection(DB, &shadow_name()).is_err(), "premise: the shadow is missing");
    assert!(m.purge_pending(shadow_id()).unwrap(), "premise: its rows are held for the purger");
    let (a, _da) = a_sender(&[1], true);
    let others: Vec<String> = (0..3).map(|n| format!("a-before-{n}")).collect();
    for name in &others {
        a.create_collection(DB, name).expect("the sender's other collection");
    }
    let first_page = a.snapshot_page(None, None).expect("the sender's first page");
    let names: Vec<&str> = first_page.collections.iter().map(|state| state.name.as_str()).collect();
    let at_shadow =
        names.iter().position(|name| *name == shadow_name()).expect("the page defines the shadow");
    assert!(
        names[..at_shadow].iter().any(|name| others.iter().any(|other| other == name)),
        "premise: a collection the member lacks is defined before the shadow in the page ({names:?})"
    );

    let (_progress, applied) = apply_the_pages(&m, &a);

    assert_eq!(
        applied.iter().find_map(|done| done.purge_pending),
        Some(shadow_id()),
        "the page waits on the purge of the shadow it would re-create"
    );
    assert!(
        others.iter().all(|name| m.get_collection(DB, name).is_err()),
        "and waits whole (ADR-189): no definition of it is applied before the wait ends"
    );
}

/// A shadow the identity lost AFTER the page's creation is history here, and is not made; it is not an orphan of a swapped name either, as
/// the identity stands: the report judges the parent's name AND its identity (the pull planned for the shadow is not cancelled by it).
#[test]
fn s13d_before_the_flip_a_shadow_older_than_its_drop_is_history_and_is_not_reported_orphaned() {
    let w = world();
    let (a, _da) = a_sender(&[1], true);
    let (m, _d) = member_with_vectors(&[]);
    m.forget_collection_leaving_rows(DB, &shadow_name()).expect("the shadow's definition goes");
    // its drop, stamped far above the sender's creation of it
    let drop = Stamp::new(Hlc::new(4_000_000_000_000, 0), a.node_id(), a.epoch());
    let txn = m.begin_write(crate::WriterHolder::Ddl).expect("a writer");
    txn.open_table(crate::tables::COLLECTIONS_DROPPED)
        .expect("the table opens")
        .insert(shadow_id().0, crate::codec::oplog_key(&drop).as_slice())
        .expect("the tombstone is written");
    txn.commit().expect("the commit lands");
    window(&m, std::slice::from_ref(&w.first));
    assert!(
        m.get_collection(DB, "orders").unwrap().vector.is_some(),
        "premise: the identity stands with its vectors"
    );
    assert!(m.get_collection(DB, FIRST).is_ok(), "premise: the stage is held, unbound");
    assert!(m.get_collection(DB, &shadow_name()).is_err(), "premise: the shadow is missing");

    let (_progress, applied) = apply_the_pages(&m, &a);

    assert!(
        applied.iter().all(|done| done.purge_pending.is_none()),
        "premise: a creation the drop makes history waits on no purge"
    );
    assert!(
        m.get_collection(DB, &shadow_name()).is_err(),
        "the page's shadow is older than the drop here: history, not made"
    );
    assert!(
        applied.iter().all(|done| done.shadows_orphaned.is_empty()),
        "and it is not reported orphaned: its parent stands ({:?})",
        applied.iter().map(|done| done.shadows_orphaned.clone()).collect::<Vec<_>>()
    );
}

/// The same by the scoped pull `shadows_missing` plans for a shadow the member lacks.
#[test]
fn s13b_before_the_flip_the_scoped_pull_of_the_shadow_makes_it() {
    let w = world();
    let (m, _d) = before_the_flip_missing_its_shadow(&w);
    let (a, _da) = a_sender(&[1], true);
    let page = a.snapshot_page(None, Some(shadow_id())).expect("the sender's scoped page");
    assert!(
        page.collections.iter().any(|state| state.name == shadow_name()),
        "premise: the page defines the shadow"
    );

    let mut progress = SnapshotProgress::of_collection(shadow_id());
    let done = m.apply_snapshot_page(a.node_id(), &mut progress, &page).expect("the page applies");

    assert!(
        m.get_collection(DB, &shadow_name()).is_ok(),
        "the identity's shadow is made by the scoped pull"
    );
    assert!(done.shadows_orphaned.is_empty(), "and is not reported orphaned");
}
