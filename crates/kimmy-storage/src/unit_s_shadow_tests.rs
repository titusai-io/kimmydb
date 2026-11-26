//! Unit S, the vector shadow of a swapped name: a snapshot page is judged by its parent's name (ADR-272, decisions 6 and 12). Tests first.
//!
//! - S1: a page whose shadow is newer than the buried shadow waits on nothing, and makes neither the shadow nor the identity;
//! - S2: after the purge, a page makes no shadow beside the swapped name and reports it orphaned;
//! - S3 (a guard): a page of documents of the swapped identity is superseded and leaves the held rows untouched;
//! - S6-S8: an entry that changes the schema of a stage (an index dropped, a vector configuration, a validator) is refused, by the member that holds the stage and by the one that does not (S5, the index creation, lands with Q6.7 unit 1: below);
//! - S9, S10: a page whose state is a stage does not configure vectors on it (S9) and still builds its index (S10, a guard);
//! - S11: a client cannot create a database or a collection whose name ends with the shadow suffix, by any creation door (a guard beside them: a name that only resembles the suffix stays valid).

use std::time::Duration;

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, DocId, Enforcement, Hlc, IndexCreate, IndexDrop, IndexField,
    MergePolicy, OpKind, OplogEntry, Stamp, ValidatorSet, ValidatorSpec, VectorRecord, VectorSet,
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

// ---- S12: the writer's own judgement ---------------------------------------------------------------------------------------------------------

/// The creation of a shadow whose parent is ABSENT is judged again under the writer by the parent's name (the catalogue is open there, hence
/// `register::is_swapped_with`): between the restore's own check and the writer, a competitor makes the parent's name swapped (the stage's
/// creation and its binding arrive in a window), and the shadow must not be made beside it. The page carries the shadow's state alone, so the
/// parent is absent and unswapped at the restore's check; the race point is the one before the creation.
#[test]
fn s12_a_shadow_whose_parent_name_is_swapped_before_the_writer_is_not_made() {
    let w = world();
    let (m, _d) = engine();
    let m = std::sync::Arc::new(m);
    let (a, _da) = a_sender(&[1], true);
    let mut page = a.snapshot_page(None, None).expect("the sender's page");
    page.collections.retain(|state| state.name == shadow_name());
    page.carried.clear();
    assert_eq!(page.collections.len(), 1, "premise: the page defines the shadow alone");
    let (rival, first, bind) = (std::sync::Arc::clone(&m), w.first.clone(), w.bind_first.clone());
    crate::sync::race_hooks::at(crate::sync::race_hooks::Race::Restore, move || {
        window(&rival, std::slice::from_ref(&first));
        window(&rival, std::slice::from_ref(&bind));
    });

    m.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .expect("the page applies");

    assert_eq!(
        m.resolve(DB, "orders").expect("the name resolves").physical,
        FIRST,
        "premise: the competitor swapped the name before the writer"
    );
    assert!(
        m.get_collection(DB, &shadow_name()).is_err(),
        "the shadow is not made beside the swapped name"
    );
}

// ---- S5-S8: S's third commit, one test per entry arm ------------------------------------------------------------------------------------

/// The four entry arms a definition reaches the stage by. Decision 15: a schema change addressed to a stage is refused on the name alone; the honest traffic addresses the LOGICAL name (the coder's addendum, item 3), so the refusal is not a carve-out.
#[derive(Clone, Copy, Debug)]
enum Arm {
    #[allow(dead_code, reason = "built by S5, which lands with Q6.7 unit 1 (below)")]
    CreateIndex,
    DropIndex,
    ConfigureVectors,
    ConfigureValidator,
}

/// The entry for `arm`, addressed to the stage `FIRST` by the stage's author, stamped above the stage's creation and inside the clock-skew bound.
fn entry_to_the_stage(w: &World, arm: Arm) -> OplogEntry {
    entry_addressed_to(w, arm, FIRST)
}

/// The entry for `arm` addressed to the hidden physical `physical` (a stage's or a recreation's name), stamped as above.
fn entry_addressed_to(w: &World, arm: Arm, physical: &str) -> OplogEntry {
    let stamp =
        Stamp::new(Hlc::new(physical_now_ms() + 25_000, 0), w.author.node_id(), w.author.epoch());
    let at = CollectionId::derive(DB, physical);
    let made = match arm {
        Arm::CreateIndex => crate::engine::ddl_entry(
            stamp,
            OpKind::CreateIndex,
            at,
            &IndexCreate {
                db: DB.into(),
                collection: physical.into(),
                index: kimmy_core::IndexMeta {
                    id: kimmy_core::IndexMeta::derive_id("by_t"),
                    name: "by_t".into(),
                    fields: vec![IndexField::ascending("t")],
                    unique: false,
                    enforcement: Enforcement::Local,
                    multikey: false,
                    expire_after_secs: None,
                    partial_filter: None,
                    created: None,
                },
            },
        ),
        Arm::DropIndex => crate::engine::ddl_entry(
            stamp,
            OpKind::DropIndex,
            at,
            &IndexDrop { db: DB.into(), collection: physical.into(), index: "by_t".into() },
        ),
        Arm::ConfigureVectors => crate::engine::ddl_entry(
            stamp,
            OpKind::ConfigureVectors,
            at,
            &VectorSet {
                db: DB.into(),
                collection: physical.into(),
                config: Some(vector_config()),
            },
        ),
        Arm::ConfigureValidator => crate::engine::ddl_entry(
            stamp,
            OpKind::ConfigureValidator,
            at,
            &ValidatorSet {
                db: DB.into(),
                collection: physical.into(),
                validator: Some(ValidatorSpec {
                    schema: doc! { "bsonType": "object", "title": "to the stage" },
                    action: Default::default(),
                    level: Default::default(),
                }),
            },
        ),
    };
    made.expect("the entry encodes")
}

/// What the stage must NOT have gained, per arm. The stage holds no index, no configuration, no validator and no shadow before the entry; after it, still none (for a drop: no tombstone under the stage's id).
fn assert_the_stage_gained_nothing(m: &Engine, arm: Arm, what: &str) {
    let stage = m.get_collection(DB, FIRST).expect("the stage is held");
    assert!(stage.index("by_t").is_none(), "{what}: the stage gains no index ({arm:?})");
    assert!(stage.vector.is_none(), "{what}: the stage gains no vector configuration ({arm:?})");
    assert!(m.validator_of(&stage).is_none(), "{what}: the stage gains no validator ({arm:?})");
    assert!(
        m.get_collection(DB, &vector_meta::shadow_name(FIRST)).is_err(),
        "{what}: the stage gains no shadow ({arm:?})"
    );
    assert!(
        m.index_dropped_at(stage.id, kimmy_core::IndexMeta::derive_id("by_t"))
            .expect("the tombstone is readable")
            .is_none(),
        "{what}: no index tombstone is recorded under the stage ({arm:?})"
    );
}

/// One arm, both members: a member that HOLDS the stage (the entry is refused, counted, not applied: the stage gains nothing) and one that does NOT (it answers Refused too, not `UnknownCollection`: the window is not stopped, so a member that lacks the stage does not
/// stall behind the entry and does not differ from one that holds it: decision 15's "an answer that depends on whether the stage is held would differ between members"). RED TODAY by the coder's read (sync.rs:3004, :3101, :3254, :3322; resolve_in
/// finds no register event under a physical name's id and answers `resolve_identity` with `visible: None`, so the target is the stage and the entry is applied): the member that holds it applies the entry; the member without it stops with `UnknownCollection`
/// (sync.rs:3052-3060, :3330-3335).
/// Mutant: the check only in two of the four arms (the other two stay red); the check after the stage is resolved (the held member still applies); the refusal as `UnknownCollection` for the member without the stage.
fn an_entry_addressed_to_a_hidden_physical_is_refused_and_changes_nothing(arm: Arm) {
    let w = world();
    let entry = entry_to_the_stage(&w, arm);

    let (held, _dh) = member_plain(&[&w.first]);
    assert!(held.get_collection(DB, FIRST).is_ok(), "premise: this member holds the stage");
    assert_the_stage_gained_nothing(&held, arm, "premise");
    let outcome = window(&held, std::slice::from_ref(&entry));
    assert_eq!(
        outcome.ddl_refused, 1,
        "the member that holds the stage refuses the entry ({arm:?}; {outcome:?})"
    );
    assert_eq!(outcome.unknown_collection, 0, "and is not stopped by it ({arm:?})");
    assert_the_stage_gained_nothing(&held, arm, "the member that holds the stage");

    let (without, _dw) = member_plain(&[]);
    assert!(
        without.get_collection(DB, FIRST).is_err(),
        "premise: this member does not hold the stage"
    );
    let outcome = window(&without, std::slice::from_ref(&entry));
    assert_eq!(
        outcome.unknown_collection, 0,
        "the member WITHOUT the stage is not stopped (UnknownCollection) by an entry the member that holds it refuses ({arm:?}; {outcome:?})"
    );
    assert_eq!(outcome.ddl_refused, 1, "it answers Refused ({arm:?}; {outcome:?})");
    assert!(without.get_collection(DB, FIRST).is_err(), "and holds no stage afterwards ({arm:?})");
}

/// The same refusal for a RECREATION's physical name (`orders.__rc.<stamp>`): hidden and made by the register alone, as a stage is, and no honest entry names it. A member that does not hold it answers Refused, not `UnknownCollection`, and creates nothing: the predicate is the hidden name, not the stage's.
fn an_entry_addressed_to_a_recreation_is_refused_and_changes_nothing(arm: Arm) {
    let w = world();
    let stamp = Stamp::new(Hlc::new(physical_now_ms(), 0), w.author.node_id(), w.author.epoch());
    let recreation = kimmy_core::recreation_name("orders", &stamp);
    assert!(kimmy_core::is_recreation_name(&recreation), "premise: a recreation's name");
    let entry = entry_addressed_to(&w, arm, &recreation);

    let (m, _d) = member_plain(&[]);
    assert!(
        m.get_collection(DB, &recreation).is_err(),
        "premise: this member holds no such collection"
    );
    let outcome = window(&m, std::slice::from_ref(&entry));

    assert_eq!(
        outcome.unknown_collection, 0,
        "an entry addressed to a recreation does not stop the window ({arm:?}; {outcome:?})"
    );
    assert_eq!(outcome.ddl_refused, 1, "it answers Refused ({arm:?}; {outcome:?})");
    assert!(
        m.get_collection(DB, &recreation).is_err(),
        "and holds nothing under that name ({arm:?})"
    );
}

/// Q6.7 unit 1: lands with the creation entry carrying the definitions. A stage's indexes are copied into it by the writer's creation entry (Q67-prebuild
/// unit 1, point 1, which rules separate stage-addressed CreateIndex entries out), and until unit 1 builds that, the join and floor tests make the copy
/// by hand as a replicated CreateIndex addressed to the stage (23 of them). The refusal of that arm, and so S5, land with it, and with them the test
/// that pins today's UnknownCollection for a physical the member never held (`join_tests::a_definition_entry_addressed_to_a_physical_the_member_never_held_...`).
#[cfg(any())]
mod q67_unit_1 {
    use super::*;

    #[test]
    fn s5_a_create_index_addressed_to_a_stage_is_refused_by_the_member_that_holds_it_and_by_the_one_that_does_not()
     {
        an_entry_addressed_to_a_hidden_physical_is_refused_and_changes_nothing(Arm::CreateIndex);
        an_entry_addressed_to_a_recreation_is_refused_and_changes_nothing(Arm::CreateIndex);
    }
}

#[test]
fn s6_a_drop_index_addressed_to_a_stage_is_refused_by_the_member_that_holds_it_and_by_the_one_that_does_not()
 {
    an_entry_addressed_to_a_hidden_physical_is_refused_and_changes_nothing(Arm::DropIndex);
    an_entry_addressed_to_a_recreation_is_refused_and_changes_nothing(Arm::DropIndex);
}

#[test]
fn s7_a_configure_vectors_addressed_to_a_stage_is_refused_by_the_member_that_holds_it_and_by_the_one_that_does_not()
 {
    an_entry_addressed_to_a_hidden_physical_is_refused_and_changes_nothing(Arm::ConfigureVectors);
    an_entry_addressed_to_a_recreation_is_refused_and_changes_nothing(Arm::ConfigureVectors);
}

#[test]
fn s8_a_configure_validator_addressed_to_a_stage_is_refused_by_the_member_that_holds_it_and_by_the_one_that_does_not()
 {
    an_entry_addressed_to_a_hidden_physical_is_refused_and_changes_nothing(Arm::ConfigureValidator);
    an_entry_addressed_to_a_recreation_is_refused_and_changes_nothing(Arm::ConfigureValidator);
}

// ---- S9, S10: the page route ------------------------------------------------------------------------------------------------------------

/// The stage information the member holds for `FIRST`, which a page whose state is that stage carries.
fn held_stage(m: &Engine) -> kimmy_core::StageMeta {
    m.get_collection(DB, FIRST)
        .expect("the stage is held")
        .stage
        .expect("premise: the held stage carries its stage information")
}

/// A whole-database page of a sender that holds `orders` with the index `by_t` and (when `vector`) its vector configuration, in which the state of `orders` is RENAMED to the stage `FIRST`: what a page carries when its sender's visible physical is a stage
/// (the page applies a state by `state.name`). HAND-FORGED. ASSUMPTIONS: `SnapshotPage::collections` is a public `Vec<CollectionState>`, and `CollectionState` has public `name` and `vector` fields (snapshot.rs:406 copies `vector` from the meta; the stamps
/// `vector_changed` and the index `created` come from the sender and are named by the page's `versions`, so the coverage check at snapshot.rs:3955 passes).
fn a_page_whose_state_is_the_stage(
    vector: bool,
    stage: kimmy_core::StageMeta,
) -> (crate::SnapshotPage, Engine, tempfile::TempDir) {
    let (a, d) = engine();
    a.create_collection(DB, "orders").expect("the sender's collection");
    a.create_index(DB, "orders", vec![IndexField::ascending("t")], false, Some("by_t".into()))
        .expect("the sender's index");
    if vector {
        a.configure_vectors(DB, "orders", vector_config()).expect("the sender's configuration");
    }
    let mut page = a.snapshot_page(None, None).expect("the sender's page");
    let state = page
        .collections
        .iter_mut()
        .find(|state| state.name == "orders")
        .expect("the page defines `orders`");
    state.name = FIRST.into();
    // what a sender whose visible physical is a stage carries: the stage's own information (a stage name with none is refused outright, and
    // nothing of the state is applied)
    state.stage = Some(stage);
    (page, a, d)
}

fn apply_one_page(m: &Engine, from: &Engine, page: &crate::SnapshotPage) -> crate::SnapshotApplied {
    let mut progress = SnapshotProgress::whole_database();
    m.apply_snapshot_page(from.node_id(), &mut progress, page).expect("the page applies")
}

/// The page's vector step on a HIDDEN state: a swapped name keeps no vector configuration and a stage never has one (decision 12; the coder's addendum, item 4: snapshot.rs:3955-4001 calls `configure_vectors_inner` on the physical name with no hidden-name check).
/// WANTED: the step is refused through `settled_vector_step`'s Refused path: skipped, warned, counted in `ddl_refused`; the stage gains no configuration; the page completes. RED TODAY: the stage is configured and `ddl_refused` is 0. The "and no shadow" assert is
/// a GUARD and not a red reason: a configuration applied from a page makes no shadow (vectors.rs, `Configured::FromSnapshot` => None).
/// Mutant: the check placed before `settled_vector_step` raises instead of skipping (the page fails and is asked again every round).
#[test]
fn s9_a_page_whose_state_is_a_stage_does_not_configure_vectors_on_it_and_counts_the_refusal() {
    let w = world();
    let (m, _d) = member_plain(&[&w.first]);
    let (page, a, _da) = a_page_whose_state_is_the_stage(true, held_stage(&m));
    assert!(
        page.collections.iter().any(|state| state.name == FIRST && state.vector.is_some()),
        "premise: the forged page carries a vector configuration for the stage"
    );

    let done = apply_one_page(&m, &a, &page);

    assert_eq!(done.ddl_refused, 1, "the vector step is refused and counted once ({done:?})");
    let stage = m.get_collection(DB, FIRST).expect("the stage is held");
    assert!(stage.vector.is_none(), "the stage gains no vector configuration");
    assert!(
        m.get_collection(DB, &vector_meta::shadow_name(FIRST)).is_err(),
        "and no shadow (a guard: a page's configuration makes none)"
    );
}

/// The same for a state that TURNS vectors off (no configuration, a stamp the sender's coverage names): the step is skipped for a hidden physical whatever the state says, so the turn-off is not recorded on the stage either, and it is counted as the configuration is.
#[test]
fn s9b_a_page_whose_state_is_a_stage_and_turns_vectors_off_records_nothing_and_counts_the_refusal()
{
    let w = world();
    let (m, _d) = member_plain(&[&w.first]);
    let (mut page, a, _da) = a_page_whose_state_is_the_stage(false, held_stage(&m));
    let covered = page.versions.get(Stamp::new(Hlc::ZERO, a.node_id(), a.epoch()).origin());
    assert!(covered > Hlc::ZERO, "premise: the page's coverage names the sender's own writes");
    let turned_off = Stamp::new(covered, a.node_id(), a.epoch());
    let state = page
        .collections
        .iter_mut()
        .find(|state| state.name == FIRST)
        .expect("the page defines the stage");
    assert!(state.vector.is_none(), "premise: the state carries no configuration");
    state.vector_changed = Some(turned_off);

    let done = apply_one_page(&m, &a, &page);

    assert_eq!(done.ddl_refused, 1, "the turn-off is refused and counted once ({done:?})");
    let stage = m.get_collection(DB, FIRST).expect("the stage is held");
    assert!(stage.vector.is_none(), "the stage holds no vector configuration");
    assert_eq!(stage.vector_changed, None, "and the turn-off is not recorded on it");
}

/// The refusal must stay on the VECTOR step: a page from a sender whose visible physical is a stage carries that physical's indexes and validator (the join gave them), and the receiver applies them by `state.name`. GUARD, GREEN TODAY (the coder's read; the
/// index of a page lands on the stage in `a_snapshot_page_of_a_life_newer_...`'s second half through the name). It goes red if S's refusal is widened from the vector step to the whole hidden state.
#[test]
fn s10_a_page_whose_state_is_a_stage_still_builds_its_index_on_the_stage() {
    let w = world();
    let (m, _d) = member_plain(&[&w.first]);
    let (page, a, _da) = a_page_whose_state_is_the_stage(false, held_stage(&m));

    let done = apply_one_page(&m, &a, &page);

    assert_eq!(done.ddl_refused, 0, "the index is not refused ({done:?})");
    let stage = m.get_collection(DB, FIRST).expect("the stage is held");
    assert!(stage.index("by_t").is_some(), "the page's index is built on the stage");
}

// ---- S11: a client cannot create a collection named like a shadow ---------------------------------------------------------------------------

/// RULED INTO S (the manager, build-order addendum "`is_shadow` and the look-alike name"): the shadow suffix `.__vectors` (`vector_meta::VECTOR_SUFFIX`) is refused by `validate_name`, where the recreation mark is refused. The product already treats such a name as a
/// shadow (its policy is forced, it is hidden, skipped and swept as chunks), and vector_meta.rs says the segment is reserved, so a collection a client made under such a name never worked as an ordinary one: refusing it loses no working behaviour.
/// RED TODAY: `validate_name` refuses only a leading `__`, so each creation door below SUCCEEDS on "a.b.__vectors". WANTED: `Error::InvalidName` from each door. A mid-name suffix ("orders.__vectors.x") is NOT pinned either way: it is unruled.
/// The entries and page routes validate no names (the manager's ruling): honest peers create only through validated paths, and S's arm refusals cover the hidden physicals, so no row here for them; the product makes the shadow in `configure_vectors_inner` through `create_collection_in_txn`, which
/// validates no name and stays as it is (a test makes one by `create_system_collection`; names_and_options_tests.rs pins the replicated route). The pure rows of `validate_name` are kimmy-core's own (error.rs).
const LOOKS_LIKE_A_SHADOW: &str = "a.b.__vectors";

#[test]
fn s11_create_collection_refuses_a_name_that_ends_with_the_shadow_suffix() {
    let (m, _d) = engine();
    let Err(err) = m.create_collection(DB, LOOKS_LIKE_A_SHADOW) else {
        panic!("create_collection accepted {LOOKS_LIKE_A_SHADOW:?}");
    };
    assert!(
        matches!(err, crate::StorageError::Core(kimmy_core::Error::InvalidName { .. })),
        "{err:?}"
    );
    assert!(m.get_collection(DB, LOOKS_LIKE_A_SHADOW).is_err(), "and nothing was created");
}

#[test]
fn s11_create_collection_with_policy_refuses_a_name_that_ends_with_the_shadow_suffix() {
    let (m, _d) = engine();
    let Err(err) =
        m.create_collection_with_policy(DB, LOOKS_LIKE_A_SHADOW, MergePolicy::Document, None)
    else {
        panic!("create_collection_with_policy accepted {LOOKS_LIKE_A_SHADOW:?}");
    };
    assert!(
        matches!(err, crate::StorageError::Core(kimmy_core::Error::InvalidName { .. })),
        "{err:?}"
    );
    assert!(m.get_collection(DB, LOOKS_LIKE_A_SHADOW).is_err(), "and nothing was created");
}

#[test]
fn s11_create_collection_with_validator_refuses_a_name_that_ends_with_the_shadow_suffix() {
    let (m, _d) = engine();
    let validator = ValidatorSpec {
        schema: doc! { "bsonType": "object" },
        action: Default::default(),
        level: Default::default(),
    };
    let Err(err) = m.create_collection_with_validator(DB, LOOKS_LIKE_A_SHADOW, validator) else {
        panic!("create_collection_with_validator accepted {LOOKS_LIKE_A_SHADOW:?}");
    };
    assert!(
        matches!(err, crate::StorageError::Core(kimmy_core::Error::InvalidName { .. })),
        "{err:?}"
    );
    assert!(m.get_collection(DB, LOOKS_LIKE_A_SHADOW).is_err(), "and nothing was created");
}

#[test]
fn s11_create_database_refuses_a_name_that_ends_with_the_shadow_suffix() {
    let (m, _d) = engine();
    let Err(err) = m.create_database(LOOKS_LIKE_A_SHADOW) else {
        panic!("create_database accepted {LOOKS_LIKE_A_SHADOW:?}");
    };
    assert!(
        matches!(err, crate::StorageError::Core(kimmy_core::Error::InvalidName { .. })),
        "{err:?}"
    );
}

/// GUARDS, apart from the refusals so that none sits behind a red assert: GREEN TODAY and after the fix. Only the exact suffix is reserved: names that merely resemble it stay valid, and a collection is created under each.
#[test]
fn s11_names_that_only_resemble_the_shadow_suffix_stay_valid_and_create() {
    let (m, _d) = engine();
    for name in ["orders.vectors", "orders__vectors", "orders.__vectorsx"] {
        assert!(kimmy_core::Error::validate_name(name).is_ok(), "{name:?} stays a valid name");
        assert!(m.create_collection(DB, name).is_ok(), "create_collection succeeds on {name:?}");
    }
}
