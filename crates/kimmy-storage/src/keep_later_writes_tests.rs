//! A drop keeps the writes stamped after it (ADR-231, extended): the decision,
//! the two commits, the vector shadow and the database's guard, one test per
//! clause of the design.

use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, IndexField, Stamp, VectorRecord};

use crate::late_drop::hooks;
use crate::live_count::test_support::assert_counts_exact;
use crate::sync::race_hooks;
use crate::{Engine, SnapshotProgress, WalkScope, tables};

const BATCH: usize = 1024;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn pull(into: &Engine, from: &Engine) {
    let mine = into.version_vector().unwrap();
    let theirs = from.version_vector().unwrap();
    if let Some(start) = mine.behind(&theirs) {
        let entries = from.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
        into.apply_batch(&entries).unwrap();
    }
}

fn sync(a: &Engine, b: &Engine) {
    pull(a, b);
    pull(b, a);
}

fn quiesce(engines: &[&Engine]) {
    for _ in 0..4 {
        for (i, x) in engines.iter().enumerate() {
            for y in &engines[i + 1..] {
                sync(x, y);
            }
        }
        for e in engines {
            e.finish_purges_now().unwrap();
        }
    }
}

/// Long enough that the next stamp is on a later millisecond than any minted
/// before it, so "after the drop" is a fact about stamps and not luck.
fn later() {
    std::thread::sleep(Duration::from_millis(5));
}

fn docs_of(e: &Engine, name: &str, ids: &[i64]) -> Vec<i64> {
    let coll = e.get_collection("shop", name).unwrap();
    ids.iter().copied().filter(|n| e.get(&coll, &DocId::Int64(*n)).unwrap().is_some()).collect()
}

fn holds(e: &Engine, name: &str) -> bool {
    e.get_collection("shop", name).is_ok()
}

/// A creates `shop.orders` with document 1 and D takes it. Nothing else yet.
fn a_and_d_holding_the_old_life()
-> (Engine, Engine, tempfile::TempDir, tempfile::TempDir, CollectionId) {
    let (a, da) = engine();
    let (d, dd) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
    sync(&a, &d);
    assert_eq!(docs_of(&d, "orders", &[1]), vec![1], "premise");
    (a, d, da, dd, on_a.id)
}

fn byo_config() -> kimmy_core::VectorConfig {
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

fn chunk_of(source: i64, at: Hlc) -> VectorRecord {
    VectorRecord {
        source: DocId::Int64(source),
        chunk: 0,
        source_hlc: at,
        vector: vec![0.1, 0.2, 0.3, 0.4],
        text: "t".into(),
        config: None,
    }
}

// -- D-2: the decision ------------------------------------------------------

/// A replicated drop that meets nothing stamped after it is the burial it
/// always was: the survivors path is not taken.
#[test]
fn a_drop_with_nothing_after_it_takes_the_fast_path() {
    let (a, d, _da, _dd, id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    let before = hooks::survivors_paths();
    quiesce(&[&a, &d]);
    assert_eq!(hooks::survivors_paths(), before, "the fast path walks nothing");
    assert!(!holds(&d, "orders"));
    assert_eq!(d.rows_under(id).unwrap(), 0);
    assert_counts_exact(&d, "a buried collection");
}

/// An absent digest row is *unknown*, not "nothing above": the drop takes the
/// survivors path. The sweep of a collection with nothing after the drop ends
/// buried, so the end state is the same and the walk was cheap.
#[test]
fn an_absent_digest_row_is_unknown_and_takes_the_survivors_path() {
    let (a, d, _da, _dd, id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    {
        let txn = d.begin_write(crate::WriterHolder::Ddl).unwrap();
        txn.open_table(tables::LIVE_DIGESTS).unwrap().remove(id.0).unwrap();
        txn.commit().unwrap();
    }
    assert!(d.digest_row(id).unwrap().is_none(), "premise");
    let before = hooks::survivors_paths();
    quiesce(&[&a, &d]);
    assert_eq!(
        hooks::survivors_paths(),
        before + 1,
        "an absent row must not read as nothing-above"
    );
    assert!(!holds(&d, "orders"), "nothing was left, so it is buried");
    assert_eq!(d.rows_under(id).unwrap(), 0);
    assert_counts_exact(&d, "an unknown row");
}

/// An index created after the drop on the member that had not heard of it is
/// a write after the drop too: the fast path is not taken and the index stands.
#[test]
fn an_index_stamped_after_the_drop_keeps_the_collection() {
    let (a, d, _da, _dd, _id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    d.create_index("shop", "orders", vec![IndexField::ascending("k")], false, Some("k_1".into()))
        .unwrap();
    let before = hooks::survivors_paths();
    quiesce(&[&a, &d]);
    assert_eq!(hooks::survivors_paths(), before + 1);
    let kept = d.get_collection("shop", "orders").unwrap();
    assert!(kept.index("k_1").is_some(), "the index made after the drop went with the life");
    assert_eq!(docs_of(&d, "orders", &[1]), Vec::<i64>::new(), "document 1 is the drop's");
    assert_counts_exact(&d, "a definition kept");
}

/// A local write that lands between the drop's read of the collection and its
/// burying transaction is stamped above the drop, and is not lost though it
/// was acknowledged: the decision is made under the writer.
#[test]
fn a_write_landing_before_the_burial_takes_the_writer_is_kept() {
    let (a, d, _da, _dd, _id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    let d = Arc::new(d);
    let start = d.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
    later();
    let competing = Arc::clone(&d);
    let before = hooks::survivors_paths();
    let ((), ()) = race_hooks::race(
        race_hooks::Race::Burial,
        move || {
            let on_d = competing.get_collection("shop", "orders").unwrap();
            competing.insert(&on_d, doc! { "_id": 4i64 }).unwrap();
        },
        || {
            d.apply_batch(&entries).unwrap();
        },
    );
    assert_eq!(hooks::survivors_paths(), before + 1, "the write was seen under the writer");
    assert_eq!(docs_of(&d, "orders", &[1, 4]), vec![4]);
    assert_counts_exact(&d, "a write racing the burial");
}

// -- D-1 / M-1: the two commits ----------------------------------------------

/// A life the sweep leaves with nothing live (the write after the drop was
/// itself deleted) is buried in the last commit and purged: the incarnation
/// rule that would turn the same drop away from a life re-dated above it does
/// not stand in the way.
#[test]
fn a_life_left_with_nothing_by_the_sweep_is_buried_and_purged() {
    let (a, d, _da, _dd, id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
    assert!(d.delete(&on_d, &DocId::Int64(3)).unwrap());
    let before = hooks::survivors_paths();
    quiesce(&[&a, &d]);
    assert_eq!(hooks::survivors_paths(), before + 1, "a tombstone stamped after the drop counts");
    assert!(!holds(&d, "orders"), "buried by the last commit");
    assert_eq!(d.rows_under(id).unwrap(), 0, "and purged");
    assert!(d.collection_dropped_at(id).unwrap().is_some());
    assert_counts_exact(&d, "an emptied life");
}

/// The log line says what happened. The survivors path is taken whenever a
/// collection might hold something stamped after the drop, and the sweep often
/// finds nothing, so "kept them" must not be logged for a collection buried whole.
#[test]
fn the_survivors_path_logs_kept_only_when_something_survived() {
    let (a, d, _da, _dd, _id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
    let logs = crate::log_capture::logs_of(|| quiesce(&[&a, &d]));
    assert!(holds(&d, "orders"), "premise: a live write after the drop survived");
    assert!(logs.contains("kept them"), "{logs}");
    assert!(!logs.contains("the collection was buried"), "{logs}");
}

#[test]
fn the_survivors_path_logs_buried_when_the_sweep_left_nothing() {
    let (a, d, _da, _dd, _id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
    assert!(d.delete(&on_d, &DocId::Int64(3)).unwrap());
    let logs = crate::log_capture::logs_of(|| quiesce(&[&a, &d]));
    assert!(!holds(&d, "orders"), "premise: buried by the last commit");
    assert!(logs.contains("the collection was buried"), "{logs}");
    assert!(!logs.contains("kept them"), "a collection buried whole was reported as kept: {logs}");
}

// -- A database's drop keeps as a collection's does (ADR-236) ---------------

/// The database's drop reaches the life it covers: what was written into it
/// after the drop stands, re-dated, and what was written before goes. The
/// member that never held the life takes it from the one that kept it, as the
/// existence half's scoped repair brings it (ADR-231, extended).
#[test]
fn a_database_drop_keeps_a_write_stamped_after_it_in_the_life_it_covers() {
    let (a, d, _da, _dd, id) = a_and_d_holding_the_old_life();
    a.create_collection("shop", "items").unwrap();
    sync(&a, &d);
    a.drop_database("shop").unwrap();
    a.finish_purges_now().unwrap();
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
    let before = hooks::survivors_paths();
    quiesce(&[&a, &d]);
    assert!(hooks::survivors_paths() > before, "premise: the survivors path");
    let kept = d.get_collection("shop", "orders").expect("the covered life keeps the write");
    let database_drop = a.database_dropped_at("shop").unwrap().unwrap();
    assert!(kept.is_redated() && kept.created > database_drop.hlc, "{kept:?}");
    assert_eq!(docs_of(&d, "orders", &[1, 3]), vec![3]);
    assert!(!holds(&d, "items"), "nothing after: buried");
    assert_counts_exact(&d, "a database drop that kept");
    // The repair the existence half plans brings the kept life to the dropper.
    assert!(!holds(&a, "orders"), "premise: A has nothing to put it in");
    let page = d.snapshot_page(None, Some(id)).unwrap();
    a.apply_snapshot_page(d.node_id(), &mut SnapshotProgress::of_collection(id), &page).unwrap();
    assert_eq!(docs_of(&a, "orders", &[1, 3]), vec![3], "the dropper takes the kept write");
}

// -- D-3 / M-2 / M-4: the database's guard, as ADR-236 leaves it -------------

/// A database that was dropped and made again has a tombstone, and a
/// collection created after it is not covered by it: that collection's own
/// drop keeps the writes stamped after it.
#[test]
fn a_collection_in_a_database_made_again_keeps_a_write_after_its_own_drop() {
    let (a, d, _da, _dd, _id) = a_and_d_holding_the_old_life();
    a.drop_database("shop").unwrap();
    a.finish_purges_now().unwrap();
    later();
    let again = a.create_collection("shop", "orders").unwrap();
    a.insert(&again, doc! { "_id": 1i64 }).unwrap();
    quiesce(&[&a, &d]);
    assert!(d.database_dropped_at("shop").unwrap().is_some(), "premise: a tombstone is held");
    later();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
    let before = hooks::survivors_paths();
    quiesce(&[&a, &d]);
    assert_eq!(hooks::survivors_paths(), before + 1);
    assert_eq!(docs_of(&d, "orders", &[1, 3]), vec![3], "a database tombstone is not the guard");
}

/// The database's tombstone arrives after the collection's drop kept a write
/// made before the database's drop: the database's drop, judged by the kept
/// life's `created` like any drop (ADR-236), removes that write, and the life,
/// left with nothing, is buried.
#[test]
fn a_database_tombstone_that_comes_after_a_kept_life_removes_a_write_made_before_it() {
    let (a, d, _da, _dd, _id) = a_and_d_holding_the_old_life();
    a.create_collection("shop", "items").unwrap();
    sync(&a, &d);
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
    later();
    a.drop_database("shop").unwrap();
    let before = hooks::survivors_paths();
    quiesce(&[&a, &d]);
    assert_eq!(hooks::survivors_paths(), before + 1, "the collection's drop came first and kept");
    assert!(!holds(&d, "orders"), "the database's drop buried what the collection's kept");
    assert!(!holds(&d, "items"));
    assert_counts_exact(&d, "a database drop after a kept life");
}

/// A whole page of the life the receiver's floor says has ended brings the definitions
/// that life made after the drop, an index and a vector configuration, into the life
/// standing here (ADR-236): the drop keeps them, and the sender's life will once it hears
/// the drop. Refused with the life, as they were, the receiver took the coverage of their
/// entries and never held them.
#[test]
fn a_whole_page_brings_the_definitions_its_drop_keeps_into_the_newer_life() {
    let (a, d, _da, _dd, _id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    a.finish_purges_now().unwrap();
    later();
    d.create_index("shop", "orders", vec![IndexField::ascending("k")], false, Some("by_k".into()))
        .unwrap();
    d.configure_vectors("shop", "orders", byo_config()).unwrap();
    later();
    let newer = a.create_collection("shop", "orders").unwrap();
    assert!(newer.incarnation_floor.is_some(), "premise: floored at the drop");
    let page = d.snapshot_page(None, None).unwrap();
    a.apply_snapshot_page(d.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    let standing = a.get_collection("shop", "orders").unwrap();
    assert_eq!(standing.created, newer.created, "premise: the newer life stands");
    assert!(standing.index("by_k").is_some(), "the index was refused: {:?}", standing.indexes);
    assert!(standing.vector.is_some(), "the vector configuration was refused");
    assert_eq!(
        docs_of(&a, "orders", &[1]),
        Vec::<i64>::new(),
        "the life before's document stays out"
    );
}

/// F15 (ADR-236). A drop that reaches a life created before it, holding a write after it,
/// re-dates the life, sweeps the definitions of the life before, then its records a chunk
/// at a time, and takes the floor in the last chunk. A `CreateIndex` of the life before
/// that arrives between two chunks (a re-served window, a count-half replay) was judged by
/// the floor alone, which was not there yet: built, and kept once the floor landed over it,
/// an index every other member refused. It is judged by the drop's tombstone too.
fn a_life_kept_by_a_drop_with_the_old_lifes_index_arriving_between_two_chunks(
    arrives: impl FnOnce(&Engine, &[kimmy_core::OplogEntry], &Engine) + 'static,
) -> (Engine, Engine, Engine, Stamp, [tempfile::TempDir; 3]) {
    let (a, da) = engine();
    let (d, dd) = engine();
    let (x, dx) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
    pull(&d, &a);
    later();
    a.create_index("shop", "orders", vec![IndexField::ascending("v")], false, Some("old_i".into()))
        .unwrap();
    later();
    // More than a chunk of the life before, so the sweep takes two.
    let docs: Vec<_> = (1_000..2_400i64).map(|i| doc! { "_id": i }).collect();
    a.insert_many(&on_a, docs).unwrap();
    pull(&x, &a);
    let entries = a.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    let create: Vec<_> =
        entries.iter().filter(|e| e.kind == kimmy_core::OpKind::CreateIndex).cloned().collect();
    let rest: Vec<_> =
        entries.iter().filter(|e| e.kind != kimmy_core::OpKind::CreateIndex).cloned().collect();
    // D holds the life before without its index.
    d.apply_batch(&rest).unwrap();
    later();
    assert!(a.drop_collection("shop", "orders").unwrap());
    let dropped = a.collection_dropped_at(on_a.id).unwrap().expect("the drop's tombstone");
    later();
    // D makes an index after the drop, so the drop keeps its life.
    d.create_index("shop", "orders", vec![IndexField::ascending("k")], false, Some("k_1".into()))
        .unwrap();
    let d = Arc::new(d);
    let x = Arc::new(x);
    let (on_d, from_x) = (Arc::clone(&d), Arc::clone(&x));
    hooks::run_after_the_next_chunk(move || arrives(&on_d, &create, &from_x));
    let start = d.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let window = a.entries_for_peer(start, 10_000, WalkScope::Background).unwrap().entries;
    d.apply_batch(&window).unwrap();
    let d = Arc::try_unwrap(d).ok().expect("the hook ran and let go");
    let x = Arc::try_unwrap(x).ok().expect("the hook ran and let go");
    (a, d, x, dropped, [da, dd, dx])
}

fn indexes_of_the_life_before(e: &Engine, dropped: Stamp) -> Vec<String> {
    let kept = e.get_collection("shop", "orders").unwrap();
    assert_eq!(kept.incarnation_floor, Some(dropped.hlc), "premise: the sweep finished");
    kept.indexes
        .iter()
        .filter(|i| i.created.is_some_and(|c| c.hlc <= dropped.hlc))
        .map(|i| i.name.clone())
        .collect()
}

#[test]
fn a_replicated_index_of_the_life_before_between_two_sweep_chunks_is_history() {
    let (_a, d, _x, dropped, _dirs) =
        a_life_kept_by_a_drop_with_the_old_lifes_index_arriving_between_two_chunks(
            |d, create, _| {
                d.apply_batch(create).unwrap();
            },
        );
    assert_eq!(indexes_of_the_life_before(&d, dropped), Vec::<String>::new());
    let names: Vec<_> =
        d.get_collection("shop", "orders").unwrap().indexes.into_iter().map(|i| i.name).collect();
    assert_eq!(names, vec!["k_1".to_string()], "the life's own index stands");
}

/// The same by a page: X, still holding the life before with its index, serves D a whole
/// page between two chunks of D's sweep.
#[test]
fn a_pages_index_of_the_life_before_between_two_sweep_chunks_is_history() {
    let (_a, d, _x, dropped, _dirs) =
        a_life_kept_by_a_drop_with_the_old_lifes_index_arriving_between_two_chunks(|d, _, x| {
            let page = x.snapshot_page(None, None).unwrap();
            assert!(
                page.collections.iter().any(|c| c.indexes.iter().any(|i| i.name == "old_i")),
                "premise: the page carries the index"
            );
            let mut progress = SnapshotProgress::whole_database();
            d.apply_snapshot_page(x.node_id(), &mut progress, &page).unwrap();
        });
    assert_eq!(indexes_of_the_life_before(&d, dropped), Vec::<String>::new());
}

/// A definition in the drop's own `Hlc`, from a node above the dropper's, arriving in the
/// same window, is the drop's too (ADR-236: a tie is the drop's).
#[test]
fn a_replicated_index_in_the_drops_hlc_between_two_sweep_chunks_is_history() {
    let (_a, d, _x, dropped, _dirs) =
        a_life_kept_by_a_drop_with_the_old_lifes_index_arriving_between_two_chunks(
            |d, create, _| {
                let held = d.collection_dropped_at(CollectionId::derive("shop", "orders"));
                let dropped = held.unwrap().expect("the tombstone is recorded first");
                let mut tied = create[0].clone();
                tied.stamp = Stamp::new(dropped.hlc, kimmy_core::NodeId::from_bytes([0xff; 16]));
                d.apply_batch(std::slice::from_ref(&tied)).unwrap();
            },
        );
    assert_eq!(indexes_of_the_life_before(&d, dropped), Vec::<String>::new());
    let names: Vec<_> =
        d.get_collection("shop", "orders").unwrap().indexes.into_iter().map(|i| i.name).collect();
    assert_eq!(names, vec!["k_1".to_string()], "only the life's own index stands");
}

/// The same window for a vector configuration of the life before: judged by the drop's
/// tombstone, not by the floor alone (ADR-236).
#[test]
fn a_replicated_vector_configuration_of_the_life_before_between_two_sweep_chunks_is_history() {
    let (a, _da) = engine();
    let (d, _dd) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
    pull(&d, &a);
    later();
    a.configure_vectors("shop", "orders", byo_config()).unwrap();
    later();
    let docs: Vec<_> = (1_000..2_400i64).map(|i| doc! { "_id": i }).collect();
    a.insert_many(&on_a, docs).unwrap();
    let entries = a.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    let configure: Vec<_> = entries
        .iter()
        .filter(|e| e.kind == kimmy_core::OpKind::ConfigureVectors)
        .cloned()
        .collect();
    assert_eq!(configure.len(), 1, "premise");
    let rest: Vec<_> = entries
        .iter()
        .filter(|e| e.kind != kimmy_core::OpKind::ConfigureVectors)
        .cloned()
        .collect();
    d.apply_batch(&rest).unwrap();
    assert!(d.get_collection("shop", "orders").unwrap().vector.is_none(), "premise: withheld");
    later();
    assert!(a.drop_collection("shop", "orders").unwrap());
    let dropped = a.collection_dropped_at(on_a.id).unwrap().expect("the drop's tombstone");
    later();
    d.create_index("shop", "orders", vec![IndexField::ascending("k")], false, Some("k_1".into()))
        .unwrap();
    let d = Arc::new(d);
    let on_d = Arc::clone(&d);
    hooks::run_after_the_next_chunk(move || {
        on_d.apply_batch(&configure).unwrap();
    });
    let start = d.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let window = a.entries_for_peer(start, 10_000, WalkScope::Background).unwrap().entries;
    d.apply_batch(&window).unwrap();
    let kept = d.get_collection("shop", "orders").unwrap();
    assert_eq!(kept.incarnation_floor, Some(dropped.hlc), "premise: the sweep finished");
    assert!(kept.vector.is_none(), "the life before's configuration: {:?}", kept.vector_changed);
}

/// An explicit collection drop that reaches a member in a whole-database page
/// keeps a write stamped after it, as the entry would, in whichever order the
/// page lists its tombstones.
#[test]
fn a_whole_page_carrying_an_explicit_collection_drop_keeps_a_later_write() {
    for reversed in [false, true] {
        let (a, d, _da, _dd, id) = a_and_d_holding_the_old_life();
        a.create_collection("shop", "items").unwrap();
        sync(&a, &d);
        assert!(a.drop_collection("shop", "orders").unwrap());
        a.finish_purges_now().unwrap();
        later();
        let on_d = d.get_collection("shop", "orders").unwrap();
        d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
        let mut page = a.snapshot_page(None, None).unwrap();
        assert!(page.dropped_collections.iter().any(|(c, _)| *c == id), "premise");
        if reversed {
            page.dropped_collections.reverse();
        }
        d.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
        assert_eq!(docs_of(&d, "orders", &[1, 3]), vec![3], "reversed: {reversed}");
        assert_counts_exact(&d, "a whole page that kept");
    }
}

/// And a database's drop in the same page, listed before or after its
/// collections, keeps the later write too (ADR-236), and buries the collection
/// that holds nothing after it.
#[test]
fn a_whole_page_carrying_a_database_drop_keeps_a_later_write_whichever_order_its_list_is_in() {
    for reversed in [false, true] {
        let (a, d, _da, _dd, _id) = a_and_d_holding_the_old_life();
        a.create_collection("shop", "items").unwrap();
        sync(&a, &d);
        a.drop_database("shop").unwrap();
        a.finish_purges_now().unwrap();
        later();
        let on_d = d.get_collection("shop", "orders").unwrap();
        d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
        let mut page = a.snapshot_page(None, None).unwrap();
        if reversed {
            page.dropped_collections.reverse();
        }
        let before = hooks::survivors_paths();
        d.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
        d.finish_purges_now().unwrap();
        assert_eq!(
            hooks::survivors_paths(),
            before + 1,
            "one life re-dated, the one with the later write (reversed: {reversed})"
        );
        assert_eq!(docs_of(&d, "orders", &[1, 3]), vec![3], "reversed: {reversed}");
        assert!(!holds(&d, "items"), "reversed: {reversed}");
        assert_counts_exact(&d, "a whole page carrying a database drop");
    }
}

// -- D-4 / M-3: the vector shadow follows its parent -------------------------

fn a_and_d_holding_a_vector_collection() -> (Engine, Engine, tempfile::TempDir, tempfile::TempDir) {
    let (a, da) = engine();
    let (d, dd) = engine();
    let on_a = a.create_collection("shop", "orders").unwrap();
    a.configure_vectors("shop", "orders", byo_config()).unwrap();
    a.insert(&on_a, doc! { "_id": 1i64 }).unwrap();
    let shadow_a =
        a.get_collection("shop", &kimmy_core::vector_meta::shadow_name("orders")).unwrap();
    a.put_vectors(&shadow_a, &DocId::Int64(1), &[chunk_of(1, on_a.created)]).unwrap();
    sync(&a, &d);
    assert!(holds(&d, &kimmy_core::vector_meta::shadow_name("orders")), "premise");
    (a, d, da, dd)
}

/// A chunk is kept only while the document it embeds is live. A document of the
/// life before goes with its chunk (by stamp), and a chunk stamped after the drop
/// for a document that was deleted after it (the worker removes chunks later) is
/// no reason for the shadow to keep a row with no parent; the chunk of a
/// document that survives stays.
#[test]
fn a_chunk_without_a_live_parent_is_removed_and_a_survivors_chunk_stays() {
    let (a, d, _da, _dd) = a_and_d_holding_a_vector_collection();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    let (_, kept) = d.insert_stamped(&on_d, doc! { "_id": 5i64 }).unwrap();
    let (_, gone) = d.insert_stamped(&on_d, doc! { "_id": 6i64 }).unwrap();
    let shadow = d.get_collection("shop", &kimmy_core::vector_meta::shadow_name("orders")).unwrap();
    d.put_vectors(&shadow, &DocId::Int64(5), &[chunk_of(5, kept.hlc)]).unwrap();
    d.put_vectors(&shadow, &DocId::Int64(6), &[chunk_of(6, gone.hlc)]).unwrap();
    assert!(d.delete(&on_d, &DocId::Int64(6)).unwrap());
    assert_eq!(d.get_vectors(&shadow, &DocId::Int64(6)).unwrap().len(), 1, "premise: orphaned");
    quiesce(&[&a, &d]);
    assert_eq!(docs_of(&d, "orders", &[1, 5, 6]), vec![5]);
    let shadow = d.get_collection("shop", &kimmy_core::vector_meta::shadow_name("orders")).unwrap();
    assert!(d.get_vectors(&shadow, &DocId::Int64(1)).unwrap().is_empty(), "the life before's");
    assert!(d.get_vectors(&shadow, &DocId::Int64(6)).unwrap().is_empty(), "a parentless chunk");
    assert_eq!(d.get_vectors(&shadow, &DocId::Int64(5)).unwrap().len(), 1);
    assert_counts_exact(&d, "a shadow that follows its parent");
}

/// A life left with nothing is buried with its shadow, in the one commit.
#[test]
fn an_emptied_life_is_buried_with_its_shadow() {
    let (a, d, _da, _dd) = a_and_d_holding_a_vector_collection();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 5i64 }).unwrap();
    assert!(d.delete(&on_d, &DocId::Int64(5)).unwrap());
    quiesce(&[&a, &d]);
    assert!(!holds(&d, "orders"));
    assert!(!holds(&d, &kimmy_core::vector_meta::shadow_name("orders")), "the shadow went too");
    assert_counts_exact(&d, "a life emptied with its shadow");
}

/// The shadow is swept after its parent whatever order a start reaches them
/// in: a start that finds both owing does the parent first.
#[test]
fn a_start_sweeps_the_parent_before_its_shadow() {
    let (a, d, _da, _dd) = a_and_d_holding_a_vector_collection();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 5i64 }).unwrap();
    let shadow = d.get_collection("shop", &kimmy_core::vector_meta::shadow_name("orders")).unwrap();
    d.put_vectors(&shadow, &DocId::Int64(5), &[chunk_of(5, Hlc::ZERO)]).unwrap();
    // Stop after the first commit of the survivors path: both lives are re-dated
    // and owe a sweep; nothing is swept yet.
    let asked = std::cell::Cell::new(0);
    let dropped = {
        let start = d.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
        let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
        entries.iter().find(|e| e.kind == kimmy_core::OpKind::DropCollection).unwrap().stamp
    };
    let result = d.drop_keeping_later_writes(
        "shop",
        "orders",
        dropped,
        &|| {
            asked.set(asked.get() + 1);
            Err(crate::StorageError::Stopping(crate::StopReason::Shutdown))
        },
        crate::database_tombstone::Notify::Each,
    );
    assert!(matches!(result, Err(crate::StorageError::Stopping(_))));
    for name in ["orders", shadow.name.as_str()] {
        let standing = d.get_collection("shop", name).unwrap();
        assert!(standing.is_redated(), "{name}: re-dated in the first commit");
        assert!(crate::late_drop::owes_a_sweep(&standing, dropped.hlc), "{name}");
    }
    // A start that reaches the shadow's turn first still sweeps the parent first.
    d.sweep_late_drop("shop", &shadow.name, dropped).unwrap();
    let parent = d.get_collection("shop", "orders").unwrap();
    assert_eq!(
        parent.incarnation_floor,
        Some(dropped.hlc),
        "the parent was swept by the shadow's turn"
    );
    assert_eq!(docs_of(&d, "orders", &[1, 5]), vec![5]);
    assert_counts_exact(&d, "a shadow swept after its parent");
}

// -- The fast path's premise ---------------------------------------------------

/// The fast path trusts `maxw`: the highest stamp per origin of any record,
/// live or tombstone, however it arrived. One test per way a record reaches a
/// collection, so a route that stopped raising it would send a write after the
/// drop to the burial.
#[test]
fn maxw_is_raised_by_every_way_a_record_reaches_a_collection() {
    let (a, d, _da, _dd, id) = a_and_d_holding_the_old_life();
    let reaches = |e: &Engine, node: kimmy_core::NodeId, at: Hlc, how: &str| {
        let row = e.digest_row(id).unwrap().unwrap_or_else(|| panic!("{how}: no row"));
        let held = row.maxw.get(&node).copied().unwrap_or(Hlc::ZERO);
        assert!(held >= at, "{how}: maxw {held:?} is below the record's {at:?}");
    };
    // A local insert and a local delete (a tombstone).
    let on_d = d.get_collection("shop", "orders").unwrap();
    later();
    let (_, inserted) = d.insert_stamped(&on_d, doc! { "_id": 7i64 }).unwrap();
    reaches(&d, d.node_id(), inserted.hlc, "a local insert");
    later();
    assert!(d.delete(&on_d, &DocId::Int64(7)).unwrap());
    let deleted = d.version_vector().unwrap().get(d.node_id());
    reaches(&d, d.node_id(), deleted, "a local delete");
    // A replicated insert from a peer.
    later();
    let on_a = a.get_collection("shop", "orders").unwrap();
    let (_, replicated) = a.insert_stamped(&on_a, doc! { "_id": 8i64 }).unwrap();
    sync(&a, &d);
    reaches(&d, a.node_id(), replicated.hlc, "a replicated insert");
    // A record a snapshot page carries.
    later();
    let (_, carried) = a.insert_stamped(&on_a, doc! { "_id": 9i64 }).unwrap();
    let page = a.snapshot_page(None, None).unwrap();
    d.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
    reaches(&d, a.node_id(), carried.hlc, "a record from a snapshot page");
    // The tombstones a sweep writes with no entry behind them: at the drop's stamp.
    let drop_at = Hlc::new(carried.hlc.wall_ms + 50, 0);
    let dropper = kimmy_core::Stamp::new(drop_at, a.node_id());
    let sweeper = d.get_collection("shop", "orders").unwrap();
    let mut newer = sweeper.clone();
    newer.created = Hlc::new(drop_at.wall_ms + 1, 0);
    {
        let txn = d.begin_write(crate::WriterHolder::Ddl).unwrap();
        Engine::put_collection_meta(&txn, &newer).unwrap();
        txn.commit().unwrap();
    }
    d.record_collection_drop(id, dropper).unwrap();
    d.sweep_late_drop("shop", "orders", dropper).unwrap();
    reaches(&d, a.node_id(), drop_at, "a sweep's tombstone at the drop");
}

// -- The owed window ----------------------------------------------------------

/// From the first commit until the floor the life holds pre-drop documents and
/// is dated above the drop. A member that never heard of the drop and holds the
/// life before is not replaced by it, whether it pulls entries from or a
/// scoped snapshot of the member in the window.
#[test]
fn a_peer_holding_the_older_life_is_not_replaced_by_one_in_the_owed_window() {
    let (a, d, _da, _dd, id) = a_and_d_holding_the_old_life();
    let (p, _dp) = engine();
    sync(&a, &p);
    assert_eq!(docs_of(&p, "orders", &[1]), vec![1], "premise: p holds the older life");
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
    let start = d.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
    let dropped =
        entries.iter().find(|e| e.kind == kimmy_core::OpKind::DropCollection).unwrap().stamp;
    let stopped = d.drop_keeping_later_writes(
        "shop",
        "orders",
        dropped,
        &|| Err(crate::StorageError::Stopping(crate::StopReason::Shutdown)),
        crate::database_tombstone::Notify::Each,
    );
    assert!(matches!(stopped, Err(crate::StorageError::Stopping(_))));
    let window = d.get_collection("shop", "orders").unwrap();
    assert!(window.created > dropped.hlc && window.incarnation_floor.is_none(), "premise");

    let before = p.get_collection("shop", "orders").unwrap();
    sync(&p, &d);
    let page = d.snapshot_page(None, Some(id)).unwrap();
    p.apply_snapshot_page(d.node_id(), &mut SnapshotProgress::of_collection(id), &page).unwrap();
    let after = p.get_collection("shop", "orders").unwrap();
    assert_eq!(after.created, before.created, "p's life was replaced");
    assert!(docs_of(&p, "orders", &[1]) == vec![1], "p's older life lost its documents");
    assert_counts_exact(&p, "the older life through the window");
}

// -- A re-dated life must not outlive the database's drop that covers it --------

/// A database's drop `S` is followed by a drop of each of its collections, at
/// stamps above `S`. M, which is told of the collection's drop and not of `S`
/// (it was behind and caught up by a page), keeps a write and dates the life
/// just above that drop, so above `S`. A peer repairs the collection from M, and
/// learns `S` before or after. Either way, and whichever order M learns `S` in,
/// the write stands on every member (ADR-236): the life holds nothing stamped at
/// or before its `created`, which is above `S`, so `S` has nothing of it to
/// remove. (0.49.0 judged it by where it began, `redated.from`, and buried it,
/// the write with it, ADR-231 D-3.)
#[test]
fn a_life_re_dated_above_the_databases_drop_keeps_its_write_on_every_member() {
    for peer_holds_the_database_drop_first in [true, false] {
        let (a, m, _da, _dm, id) = a_and_d_holding_the_old_life();
        a.create_collection("shop", "items").unwrap();
        sync(&a, &m);
        later();
        let before = a.version_vector().unwrap();
        a.drop_database("shop").unwrap();
        a.finish_purges_now().unwrap();
        let from = m.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
        let entries = a.entries_for_peer(from, BATCH, WalkScope::Background).unwrap().entries;
        let _ = before;
        let database_drop = entries
            .iter()
            .find(|e| e.collection == kimmy_core::database_drop_id("shop"))
            .unwrap()
            .clone();
        let collection_drop = entries
            .iter()
            .find(|e| e.kind == kimmy_core::OpKind::DropCollection && e.collection == id)
            .unwrap()
            .clone();
        assert!(collection_drop.stamp > database_drop.stamp, "premise: a drop above S");
        later();
        let on_m = m.get_collection("shop", "orders").unwrap();
        m.insert(&on_m, doc! { "_id": 3i64 }).unwrap();
        // M is told of the collection's drop and not of S.
        m.apply_batch(std::slice::from_ref(&collection_drop)).unwrap();
        let kept = m.get_collection("shop", "orders").unwrap();
        assert!(kept.is_redated(), "premise: M kept the write");
        assert!(kept.created > database_drop.stamp.hlc, "premise: dated above S");
        assert!(kept.redated.unwrap().from <= database_drop.stamp.hlc, "premise: began before S");

        // P learns of the database's drop by a whole-database page, or not yet.
        let (p, _dp) = engine();
        if peer_holds_the_database_drop_first {
            let page = a.snapshot_page(None, None).unwrap();
            p.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
                .unwrap();
            assert!(p.database_dropped_at("shop").unwrap().is_some(), "premise: P holds S");
        }
        // P repairs the collection from M.
        let page = m.snapshot_page(None, Some(id)).unwrap();
        assert_eq!(
            page.collections.first().and_then(|c| c.redated_from),
            Some(kept.redated.unwrap().from),
            "the definition says where the life began"
        );
        p.apply_snapshot_page(m.node_id(), &mut SnapshotProgress::of_collection(id), &page)
            .unwrap();
        assert_eq!(docs_of(&p, "orders", &[1, 3]), vec![3], "installed with the write");
        if !peer_holds_the_database_drop_first {
            p.apply_batch(std::slice::from_ref(&database_drop)).unwrap();
            assert_eq!(docs_of(&p, "orders", &[1, 3]), vec![3], "and S, after, keeps it");
        }
        // And M learns of S last.
        m.apply_batch(std::slice::from_ref(&database_drop)).unwrap();
        m.finish_purges_now().unwrap();
        p.finish_purges_now().unwrap();
        // A, which dropped it, takes it from M as the existence half's repair would.
        let page = m.snapshot_page(None, Some(id)).unwrap();
        a.apply_snapshot_page(m.node_id(), &mut SnapshotProgress::of_collection(id), &page)
            .unwrap();
        for (who, e) in [("a", &a), ("m", &m), ("p", &p)] {
            let peer_first = peer_holds_the_database_drop_first;
            assert_eq!(docs_of(e, "orders", &[1, 3]), vec![3], "{who}, peer first: {peer_first}");
            assert!(!holds(e, "items"), "{who}");
            assert_counts_exact(e, who);
        }
    }
}

// -- m-1: an interrupted drop still appends its entry -------------------------

/// A drop interrupted after its first commit is delivered again, finds the life
/// re-dated above it and is "ignored" as aimed at a life already over; the sweep
/// continues, and the entry is appended all the same, so this member holds the
/// drop to serve onward.
#[test]
fn a_drop_interrupted_after_its_first_commit_and_delivered_again_appends_its_entry() {
    let (a, d, _da, _dd, _id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
    let start = d.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
    let drop = entries.iter().find(|e| e.kind == kimmy_core::OpKind::DropCollection).unwrap();
    let interrupted = d.drop_keeping_later_writes(
        "shop",
        "orders",
        drop.stamp,
        &|| Err(crate::StorageError::Stopping(crate::StopReason::Shutdown)),
        crate::database_tombstone::Notify::Each,
    );
    assert!(matches!(interrupted, Err(crate::StorageError::Stopping(_))));
    assert!(d.oplog_entry(&drop.stamp).unwrap().is_none(), "premise: not appended yet");

    d.apply_batch(&entries).unwrap();
    assert!(d.oplog_entry(&drop.stamp).unwrap().is_some(), "the entry is held to serve onward");
    let standing = d.get_collection("shop", "orders").unwrap();
    assert_eq!(standing.incarnation_floor, Some(drop.stamp.hlc), "and the sweep finished");
    assert_eq!(docs_of(&d, "orders", &[1, 3]), vec![3]);
    assert_counts_exact(&d, "a redelivered interrupted drop");
}

// -- Review fixes -------------------------------------------------------------

/// m-2. A stream open on the collection ends at the first commit, as at a
/// burial: a client that saw the documents of the life before must be told they
/// are going, and the collection standing does not say so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_open_on_the_collection_ends_when_the_drop_reaches_the_survivors_path() {
    use crate::{ChangeEvent, InvalidateReason, WatchOptions, WatchScope};

    let (a, d, _da, _dd, id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
    let mut stream = d.watch(WatchScope::Collection(id), WatchOptions::default()).unwrap();
    let before = hooks::survivors_paths();
    let start = d.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
    d.apply_batch(&entries).unwrap();
    assert_eq!(hooks::survivors_paths(), before + 1, "premise: the survivors path");
    let heard = tokio::time::timeout(Duration::from_millis(1_000), stream.next(&d))
        .await
        .expect("the stream hears the drop");
    assert!(
        matches!(
            heard,
            Some(ChangeEvent::Invalidate { reason: InvalidateReason::CollectionDropped })
        ),
        "{heard:?}"
    );
}

/// n3. A write that lands in a range of the collection the sweep has already
/// passed, between two chunks, has its entry at or above the position the first
/// commit read; the minimum the sweep records cannot sit above it. Here the only
/// kept record the sweep sees is stamped after that write.
#[test]
fn a_write_landing_behind_the_sweeps_cursor_is_not_below_the_recorded_first_arrival() {
    let (a, d, _da, _dd, id) = a_and_d_holding_the_old_life();
    // More than a chunk of the life before, so the sweep takes two.
    let on_a = a.get_collection("shop", "orders").unwrap();
    let docs: Vec<_> = (1_000..2_400i64).map(|i| doc! { "_id": i }).collect();
    a.insert_many(&on_a, docs).unwrap();
    sync(&a, &d);
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    // The only thing the life keeps at the first commit is a definition.
    d.create_index("shop", "orders", vec![IndexField::ascending("k")], false, Some("k_1".into()))
        .unwrap();
    let on_d = d.get_collection("shop", "orders").unwrap();
    // After the first chunk: W lands in the range already passed, then K in the
    // range ahead. K's entry arrives after W's.
    let landing = Arc::new(d);
    let writer = Arc::clone(&landing);
    let (w_stamp, k_stamp) = {
        let cell = std::rc::Rc::new(std::cell::RefCell::new(None));
        let into = std::rc::Rc::clone(&cell);
        let on_d = on_d.clone();
        hooks::run_after_the_next_chunk(move || {
            let (_, w) = writer.insert_stamped(&on_d, doc! { "_id": 0i64 }).unwrap();
            let (_, k) = writer.insert_stamped(&on_d, doc! { "_id": 99_999i64 }).unwrap();
            *into.borrow_mut() = Some((w, k));
        });
        let start = landing.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
        let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
        landing.apply_batch(&entries).unwrap();
        cell.take().expect("the sweep ran more than one chunk")
    };
    let kept = landing.get_collection("shop", "orders").unwrap();
    assert!(kept.is_redated(), "premise");
    let first = kept.redated_first_arrival(kept.created).expect("recorded");
    let arrival_of = |at: kimmy_core::Stamp| landing.arrival_of(&at).unwrap().unwrap();
    assert!(arrival_of(w_stamp) < arrival_of(k_stamp), "premise: W arrived before K");
    assert!(
        first <= arrival_of(w_stamp),
        "first arrival {first} is above the entry of the write that landed behind the cursor"
    );
    let _ = id;
}

/// n4. Installing a re-dated definition from a page costs no commit more than
/// installing an ordinary one: the marker is written with the creation, so a
/// stop cannot leave the life standing without it.
#[test]
fn a_re_dated_definition_is_installed_with_its_marker_in_the_one_commit() {
    let (a, m, _da, _dm, id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_m = m.get_collection("shop", "orders").unwrap();
    m.insert(&on_m, doc! { "_id": 3i64 }).unwrap();
    let start = m.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
    let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
    m.apply_batch(&entries).unwrap();
    let kept = m.get_collection("shop", "orders").unwrap();
    assert!(kept.is_redated(), "premise");

    let ordinary = |e: &Engine| {
        let page = e.snapshot_page(None, Some(CollectionId::derive("shop", "other"))).unwrap();
        (e.commits(), page)
    };
    let (p, _dp) = engine();
    let (q, _dq) = engine();
    let plain = q.create_collection("shop", "other").unwrap();
    q.insert(&plain, doc! { "_id": 1i64 }).unwrap();
    let (_, plain_page) = ordinary(&q);
    let redated_page = m.snapshot_page(None, Some(id)).unwrap();
    assert!(redated_page.collections[0].redated_from.is_some(), "premise: the page says so");
    assert!(plain_page.collections[0].redated_from.is_none(), "premise");

    let (r, _dr) = engine();
    let before = r.commits();
    r.apply_snapshot_page(
        q.node_id(),
        &mut SnapshotProgress::of_collection(CollectionId::derive("shop", "other")),
        &plain_page,
    )
    .unwrap();
    let ordinary_cost = r.commits() - before;
    let before = p.commits();
    p.apply_snapshot_page(m.node_id(), &mut SnapshotProgress::of_collection(id), &redated_page)
        .unwrap();
    let marked_cost = p.commits() - before;
    assert_eq!(marked_cost, ordinary_cost, "the marker took a commit of its own");
    let installed = p.get_collection("shop", "orders").unwrap();
    assert_eq!(installed.redated.map(|r| r.from), kept.redated.map(|r| r.from));
    assert!(installed.is_redated());
}

/// m-1/B: a vector configuration made after the drop is a write after it: the
/// collection stands with its configuration and its shadow even with no document
/// stamped after the drop.
#[test]
fn a_vector_configuration_made_after_the_drop_keeps_the_collection_and_its_shadow() {
    let (a, m, _da, _dm) = a_and_d_holding_a_vector_collection();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    m.configure_vectors("shop", "orders", byo_config()).unwrap();
    let before = hooks::survivors_paths();
    quiesce(&[&a, &m]);
    assert_eq!(
        hooks::survivors_paths(),
        before + 1,
        "a configuration after the drop is not buried"
    );
    assert!(m.get_collection("shop", "orders").unwrap().vector.is_some());
    assert!(holds(&m, &kimmy_core::vector_meta::shadow_name("orders")), "and its shadow");
    assert_eq!(docs_of(&m, "orders", &[1]), Vec::<i64>::new());
    assert_counts_exact(&m, "a configuration kept");
}

/// B: a kept parent whose vector configuration stands keeps its shadow when the
/// sweep leaves it empty.
#[test]
fn the_shadow_of_a_configured_parent_stands_empty() {
    let (a, m, _da, _dm) = a_and_d_holding_a_vector_collection();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    m.configure_vectors("shop", "orders", byo_config()).unwrap();
    let on_m = m.get_collection("shop", "orders").unwrap();
    m.insert(&on_m, doc! { "_id": 5i64 }).unwrap();
    quiesce(&[&a, &m]);
    assert!(m.get_collection("shop", "orders").unwrap().vector.is_some());
    assert!(holds(&m, &kimmy_core::vector_meta::shadow_name("orders")));
    assert_counts_exact(&m, "an empty shadow of a configured parent");
}

/// A chunk whose document is not here is undecided: one made from a version
/// after the drop may be on its way from another origin and stays; one made from
/// a version at or before the drop can never arrive live in this life and goes.
#[test]
fn a_chunk_whose_document_is_not_here_stays_if_made_after_the_drop_and_goes_if_not() {
    let (a, d, _da, _dd) = a_and_d_holding_a_vector_collection();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    // Something after the drop, so the survivors path is taken.
    d.insert(&on_d, doc! { "_id": 9i64 }).unwrap();
    let shadow = d.get_collection("shop", &kimmy_core::vector_meta::shadow_name("orders")).unwrap();
    let after_the_drop = d.version_vector().unwrap().get(d.node_id());
    // Documents 7 and 8 are not in the parent here. 7's chunk was made from a
    // version before the drop, 8's from one after it.
    d.put_vectors(&shadow, &DocId::Int64(7), &[chunk_of(7, on_d.created)]).unwrap();
    d.put_vectors(&shadow, &DocId::Int64(8), &[chunk_of(8, after_the_drop)]).unwrap();
    quiesce(&[&a, &d]);
    let shadow = d.get_collection("shop", &kimmy_core::vector_meta::shadow_name("orders")).unwrap();
    assert!(d.get_vectors(&shadow, &DocId::Int64(7)).unwrap().is_empty(), "history never arrives");
    assert_eq!(d.get_vectors(&shadow, &DocId::Int64(8)).unwrap().len(), 1, "may still arrive");
    assert_counts_exact(&d, "undecided chunks");
}

/// The same by a snapshot page, which carries the drop with no oplog entry for a
/// stream to end on: the first commit's notice is the only thing that tells it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_open_on_the_collection_ends_when_a_snapshot_carries_the_drop() {
    use crate::{ChangeEvent, InvalidateReason, WatchOptions, WatchScope};

    let (a, d, _da, _dd, id) = a_and_d_holding_the_old_life();
    assert!(a.drop_collection("shop", "orders").unwrap());
    a.finish_purges_now().unwrap();
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 3i64 }).unwrap();
    let mut stream = d.watch(WatchScope::Collection(id), WatchOptions::default()).unwrap();
    let page = a.snapshot_page(None, Some(id)).unwrap();
    assert!(page.dropped.is_some(), "premise: the page carries the drop and no entry");
    let before = hooks::survivors_paths();
    d.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::of_collection(id), &page).unwrap();
    assert_eq!(hooks::survivors_paths(), before + 1, "premise: the survivors path");
    let heard = tokio::time::timeout(Duration::from_millis(1_000), stream.next(&d))
        .await
        .expect("the stream hears the drop");
    assert!(
        matches!(
            heard,
            Some(ChangeEvent::Invalidate { reason: InvalidateReason::CollectionDropped })
        ),
        "{heard:?}"
    );
}
