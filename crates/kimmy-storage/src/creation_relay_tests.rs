//! A replicated creation is appended and served onward whatever becomes of it (ADR-236): made, adopted, or refused by its
//! collection's drop or its database's. A member that refused one and withheld it still named the creation's origin past it, once
//! it served a later entry of that origin, so no member that pulled the relay was ever offered the creation. What every creation
//! SEEN decides then differed between members for good: the options row of the name, and through it a collection's merge policy
//! (ADR-272, decision 16). The rows for a creation the DATABASE's drop refuses, and the plain relay row for one its collection's
//! drop refuses, are in `database_drop_tests`.
//!
//! A child of the convergence model, so that the schedules it found run on its cluster.

use std::time::Duration;

use bson::doc;
use kimmy_core::{
    CollectionId, DocId, Epoch, Hlc, IndexField, MergePolicy, NodeId, OpKind, OplogEntry, Stamp,
    VersionVector,
};
use redb::ReadableDatabase;

use super::{Cluster, DB, Route, base, parse};
use crate::register::BaseOptions;
use crate::{
    ChangeEvent, ChangeStream, Engine, PeerWindow, SyncOutcome, WalkScope, WatchOptions, WatchScope,
};

const BATCH: usize = 10_000;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(&dir.path().join("kimmy.redb")).unwrap();
    (engine, dir)
}

/// Long enough that the next stamp is on a later millisecond than any minted before it.
fn later() {
    std::thread::sleep(Duration::from_millis(5));
}

fn orders() -> CollectionId {
    CollectionId::derive(DB, "orders")
}

fn entries_of(e: &Engine) -> Vec<OplogEntry> {
    e.entries_for_peer(Hlc::ZERO, BATCH, WalkScope::Background).unwrap().entries
}

/// The last entry of `kind` for `orders` that `e` itself wrote.
fn own(e: &Engine, kind: OpKind) -> OplogEntry {
    entries_of(e)
        .into_iter()
        .rfind(|entry| {
            entry.kind == kind && entry.collection == orders() && entry.stamp.node == e.node_id()
        })
        .expect("the member logged it")
}

fn options_row(e: &Engine) -> Option<BaseOptions> {
    crate::register::base_options_of(&e.db().begin_read().unwrap(), orders()).unwrap()
}

/// One window to `to`, in position, then its purger.
fn window(
    to: &Engine,
    theirs: &VersionVector,
    entries: &[OplogEntry],
    scanned_to: Hlc,
    exhausted: bool,
) -> SyncOutcome {
    let mine = to.witnessed_vector().unwrap();
    let mut outcome = SyncOutcome::default();
    to.apply_peer_window_into(
        theirs,
        Some(&mine),
        entries,
        PeerWindow { scanned_to, exhausted, passed_through: None, asked_partial: false },
        &mut outcome,
    )
    .unwrap();
    to.finish_purges_now().unwrap();
    outcome
}

/// `to` takes one entry, in position.
fn take(to: &Engine, entry: &OplogEntry) {
    let mut theirs = VersionVector::default();
    theirs.observe(entry.stamp);
    let outcome = window(to, &theirs, std::slice::from_ref(entry), entry.stamp.hlc, true);
    assert_eq!(outcome.purge_pending, 0, "premise: the entry was taken: {outcome:?}");
}

/// `to` pulls from `from` alone, in position, until nothing of `from`'s is left to ask for.
fn pull_from(to: &Engine, from: &Engine) {
    for _ in 0..8 {
        let theirs = from.version_vector().unwrap();
        let mine = to.witnessed_vector().unwrap();
        let Some(start) = mine.behind(&theirs) else { return };
        let served = from.entries_for_peer(start, BATCH, WalkScope::Background).unwrap();
        let outcome = window(to, &theirs, &served.entries, served.scanned_to, served.exhausted);
        assert!(
            outcome.unknown.is_none(),
            "premise: nothing of an unknown collection: {outcome:?}"
        );
    }
    panic!("a pull that never ended");
}

// -- The schedules the equality test found -----------------------------------------------------

/// The model's `schedule`, quiesced by entries: every member's options row of `orders` is the one of the newest creation, which is
/// stamped `newest` ms past the schedule's base.
fn every_options_row_is_the_newest_creations(schedule: &str, newest: u64) {
    let base = base();
    let mut cluster = Cluster::new(base);
    cluster.run(&parse(schedule)).unwrap();
    assert!(cluster.quiesce(Route::Entries).unwrap(), "premise: the members settle");
    // its author holds its own entry, whatever became of the creation elsewhere
    let created = (0..3)
        .flat_map(|m| entries_of(cluster.e(m)))
        .filter(|e| e.kind == OpKind::CreateCollection && e.collection == orders())
        .map(|e| e.stamp)
        .max()
        .expect("premise: a creation");
    assert_eq!(created.hlc.wall_ms, base + newest, "premise: the newest creation of the schedule");
    let rows: Vec<Option<BaseOptions>> = (0..3).map(|m| options_row(cluster.e(m))).collect();
    let at: Vec<Option<u64>> =
        rows.iter().map(|row| row.as_ref().map(|row| row.at.wall_ms - base)).collect();
    assert!(
        rows.iter().all(|row| row.as_ref().map(|row| row.at) == Some(created.hlc)),
        "the options rows of A, B and C name creations at {at:?}; the newest is at {newest}"
    );
    assert!(rows[1..].iter().all(|row| *row == rows[0]), "{rows:?}");
}

/// Seed 235 of the equality test: B was relayed the drop by a member that refused the creation, and held no row.
#[test]
fn seed_235_a_member_relayed_the_drop_alone_holds_the_creations_options_row() {
    every_options_row_is_the_newest_creations(
        "purger off; C.create(orders)@110; C.drop(orders)@120; A.dropdb@140; \
         A<-C scoped(orders); A<-B whole; C<-A entries; B.write(items/3)@150; A.drop(orders)@190",
        110,
    );
}

/// Seed 259: B joined with the older creation, dropped it, and never met the newer one.
#[test]
fn seed_259_a_member_that_dropped_the_older_creation_holds_the_newer_ones_options_row() {
    every_options_row_is_the_newest_creations(
        "C.create(orders)@120; C.create(orders)@130; A.create(orders)@110; A.write(orders/2)@120; \
         B<-A whole; C.index(orders,k_1:asc)@170; B.drop(orders)@170; B<-A entries; \
         A.index(orders,j_1:asc)@180; C<-A entries; C.dropindex(orders,k_1)@230",
        120,
    );
}

/// Seed 953: the refusing member's check of the collection's own drop comes before the database's, which would have relayed it.
#[test]
fn seed_953_a_creation_refused_by_both_drops_reaches_every_options_row() {
    every_options_row_is_the_newest_creations(
        "B.create(orders)@110; B<-C entries; A.drop(orders)@140; B.drop(orders)@140; \
         C.create(orders)@130; C.create(orders)@140; B.index(orders,k_1:asc)@170; \
         B.create(items)@180; C.drop(orders)@170; B.dropdb@200; B.write(orders/1)@210",
        130,
    );
}

// -- Two creations of different policies -------------------------------------------------------

/// Two creations of `orders` that never met, of different merge policies, the `field` one the later, and a drop above both, by a
/// member that held the first.
struct TwoPolicies {
    document: OplogEntry,
    field: OplogEntry,
    dropped: OplogEntry,
    /// The author of the later creation, which never hears of the drop.
    later_author: Engine,
    _dirs: [tempfile::TempDir; 3],
}

fn two_policies() -> TwoPolicies {
    let (x, dx) = engine();
    let (y, dy) = engine();
    let (z, dz) = engine();
    x.create_collection(DB, "orders").unwrap();
    let document = own(&x, OpKind::CreateCollection);
    later();
    y.create_collection_with_policy(DB, "orders", MergePolicy::Field, None).unwrap();
    let field = own(&y, OpKind::CreateCollection);
    later();
    take(&z, &document);
    assert!(z.drop_collection(DB, "orders").unwrap());
    let dropped = own(&z, OpKind::DropCollection);
    later();
    assert!(
        document.stamp.hlc < field.stamp.hlc && field.stamp.hlc < dropped.stamp.hlc,
        "premise: both creations are below the drop, the field one the later"
    );
    TwoPolicies { document, field, dropped, later_author: y, _dirs: [dx, dy, dz] }
}

/// An index of `orders` made by `by`, stamped above `floor`.
fn index_above(by: &Engine, name: &str, floor: &Stamp) -> OplogEntry {
    by.create_index(DB, "orders", vec![IndexField::ascending(name)], false, Some(name.into()))
        .unwrap();
    let entry = own(by, OpKind::CreateIndex);
    assert!(entry.stamp.hlc > floor.hlc, "premise: the fact is above the floor");
    entry
}

/// The merge policy and the indexes of `orders` on `e`.
fn definition(e: &Engine) -> (MergePolicy, Vec<String>) {
    let meta = e.get_collection(DB, "orders").expect("the member holds the collection");
    let mut indexes: Vec<String> = meta.indexes.iter().map(|i| i.name.clone()).collect();
    indexes.sort();
    (meta.merge_policy, indexes)
}

/// Both creations are history under the drop, and a fact above the floor makes the collection again, from each member's options
/// row. R holds the drop when the later creation reaches it. M takes the older creation and the drop, and the rest from R alone.
#[test]
fn two_creations_of_different_policies_below_a_drop_make_one_collection_of_one_policy() {
    let world = two_policies();
    let fact = index_above(&world.later_author, "ky", &world.dropped.stamp);
    let (r, _dr) = engine();
    let (m, _dm) = engine();
    for entry in [&world.document, &world.dropped, &world.field, &fact] {
        take(&r, entry);
    }
    assert_eq!(definition(&r), (MergePolicy::Field, vec!["ky".into()]), "premise: R made it again");
    take(&m, &world.document);
    take(&m, &world.dropped);
    pull_from(&m, &r);
    assert_eq!(definition(&m), definition(&r), "M made the collection again from another creation");
    assert_eq!(options_row(&m), options_row(&r));
}

/// The same with remains that stand: R keeps a life of the older creation, because an index above the drop keeps it, and the
/// later creation, history under the drop, reaches R LAST. The standing life adopts its policy. M hears the older creation from
/// its author and the rest from R alone.
#[test]
fn a_later_history_creation_arriving_last_at_kept_remains_reaches_the_next_member() {
    let world = two_policies();
    let (writer, _dw) = engine();
    take(&writer, &world.document);
    let keeps = index_above(&writer, "kw", &world.dropped.stamp);
    let after = index_above(&world.later_author, "ky", &world.dropped.stamp);
    let (r, _dr) = engine();
    let (m, _dm) = engine();
    for entry in [&world.document, &keeps, &world.dropped] {
        take(&r, entry);
    }
    assert_eq!(
        definition(&r),
        (MergePolicy::Document, vec!["kw".into()]),
        "premise: the drop kept the life"
    );
    take(&r, &world.field);
    assert_eq!(definition(&r).0, MergePolicy::Field, "premise: the standing life adopted it");
    take(&r, &after);
    take(&m, &world.document);
    pull_from(&m, &r);
    assert_eq!(definition(&m), definition(&r), "M never met the later creation");
    assert_eq!(options_row(&m), options_row(&r));
}

// -- Change streams ----------------------------------------------------------------------------

/// A member R that holds the tombstone of a drop of `orders` and has never met the creation, stamped below the drop, that is about
/// to reach it.
struct Refuser {
    r: Engine,
    creation: OplogEntry,
    dropped: Stamp,
    _dirs: [tempfile::TempDir; 3],
}

fn refuser() -> Refuser {
    let (x, dx) = engine();
    let (z, dz) = engine();
    let (r, dr) = engine();
    x.create_collection(DB, "orders").unwrap();
    let creation = own(&x, OpKind::CreateCollection);
    later();
    take(&z, &creation);
    assert!(z.drop_collection(DB, "orders").unwrap());
    let dropped = own(&z, OpKind::DropCollection);
    take(&r, &dropped);
    later();
    assert!(r.get_collection(DB, "orders").is_err());
    Refuser { r, creation, dropped: dropped.stamp, _dirs: [dx, dz, dr] }
}

/// R takes `creation`, which must then be in its log: what a stream or a peer can be served.
fn r_takes_and_holds(r: &Engine, creation: &OplogEntry) {
    take(r, creation);
    assert!(
        entries_of(r).iter().any(|e| e.stamp == creation.stamp),
        "the refused creation is not in the refusing member's log, so it is served to no one"
    );
}

/// The stamps of the creations `stream` delivers, up to the marker: the write of `marker` into `into`, made after what the stream
/// is asked about, which the stream must deliver.
async fn creations_before_a_marker(
    engine: &Engine,
    stream: &mut ChangeStream,
    into: &str,
    marker: i64,
) -> Vec<Stamp> {
    let coll = engine.get_collection(DB, into).unwrap();
    engine.insert(&coll, doc! { "_id": marker }).unwrap();
    let mut creations = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next(engine))
            .await
            .expect("the marker arrives within five seconds")
            .expect("the stream goes on");
        let entry = match event {
            ChangeEvent::Change { entry, .. } => entry,
            other => panic!("the stream did not go on: {other:?}"),
        };
        if entry.kind == OpKind::CreateCollection {
            creations.push(entry.stamp);
        }
        if entry.doc_id == Some(DocId::Int64(marker)) {
            return creations;
        }
    }
}

#[tokio::test]
async fn a_cluster_stream_delivers_a_creation_the_collections_drop_refused() {
    let w = refuser();
    w.r.create_collection(DB, "other").unwrap();
    let mut stream = w.r.watch(WatchScope::Cluster, WatchOptions::default()).unwrap();
    r_takes_and_holds(&w.r, &w.creation);
    let delivered = creations_before_a_marker(&w.r, &mut stream, "other", 1).await;
    assert_eq!(delivered, vec![w.creation.stamp]);
}

#[tokio::test]
async fn a_database_stream_does_not_deliver_it_where_the_name_does_not_stand() {
    let w = refuser();
    w.r.create_collection(DB, "other").unwrap();
    let mut stream = w.r.watch(WatchScope::Database(DB.into()), WatchOptions::default()).unwrap();
    r_takes_and_holds(&w.r, &w.creation);
    assert!(w.r.get_collection(DB, "orders").is_err(), "premise: the name does not stand");
    let delivered = creations_before_a_marker(&w.r, &mut stream, "other", 1).await;
    assert_eq!(delivered, Vec::<Stamp>::new());
}

#[tokio::test]
async fn a_database_stream_delivers_it_where_a_later_life_of_the_name_stands() {
    let w = refuser();
    let life = w.r.create_collection(DB, "orders").unwrap();
    assert!(life.created > w.dropped.hlc, "premise: a later life");
    let mut stream = w.r.watch(WatchScope::Database(DB.into()), WatchOptions::default()).unwrap();
    r_takes_and_holds(&w.r, &w.creation);
    let delivered = creations_before_a_marker(&w.r, &mut stream, "orders", 1).await;
    assert_eq!(delivered, vec![w.creation.stamp]);
}

#[tokio::test]
async fn the_later_lifes_collection_stream_passes_over_a_relayed_creation() {
    let w = refuser();
    let life = w.r.create_collection(DB, "orders").unwrap();
    assert!(life.created > w.dropped.hlc, "premise: a later life");
    let mut stream = w.r.watch(WatchScope::Collection(life.id), WatchOptions::default()).unwrap();
    r_takes_and_holds(&w.r, &w.creation);
    let delivered = creations_before_a_marker(&w.r, &mut stream, "orders", 1).await;
    assert_eq!(delivered, Vec::<Stamp>::new(), "a creation of the life before");
}

/// A creation in the drop's own `Hlc` is of the life the drop ended (ADR-236: the tie is the drop's), whichever of the two stamps
/// sorts first. Real members cannot mint two stamps at one `Hlc`, so the creation's stamp is written here: the drop's `Hlc`, from
/// a node that sorts above every other.
#[tokio::test]
async fn the_later_lifes_collection_stream_passes_over_a_creation_tied_with_the_drop() {
    let w = refuser();
    let life = w.r.create_collection(DB, "orders").unwrap();
    assert!(life.created > w.dropped.hlc, "premise: a later life");
    let mut tied = w.creation.clone();
    tied.stamp = Stamp::new(w.dropped.hlc, NodeId::from_bytes([0xff; 16]), Epoch::ZERO);
    assert!(tied.stamp > w.dropped, "premise: it sorts above the drop, in the drop's Hlc");
    let mut stream = w.r.watch(WatchScope::Collection(life.id), WatchOptions::default()).unwrap();
    r_takes_and_holds(&w.r, &tied);
    let delivered = creations_before_a_marker(&w.r, &mut stream, "orders", 1).await;
    assert_eq!(delivered, Vec::<Stamp>::new(), "a creation tied with the drop");
}
