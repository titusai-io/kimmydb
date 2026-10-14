//! A drop keeps the writes stamped after it (ADR-231, extended): the decision,
//! the two commits, the vector shadow and the database's guard, one test per
//! clause of the design.

use std::sync::Arc;
use std::time::Duration;

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, IndexField, VectorRecord};

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

fn chunk_of(source: i64) -> VectorRecord {
    VectorRecord {
        source: DocId::Int64(source),
        chunk: 0,
        source_hlc: Hlc::ZERO,
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

// -- D-3 / M-2 / M-4: the database's guard ----------------------------------

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

/// The database's tombstone arrives after the collection's drop kept a write:
/// the life was created at or before the database's drop, so the database
/// buries it with the write (ADR-229), whichever of the two came first.
#[test]
fn a_database_tombstone_that_comes_after_a_kept_life_buries_it() {
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
/// collections, buries them whole.
#[test]
fn a_whole_page_carrying_a_database_drop_buries_whichever_order_its_list_is_in() {
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
        d.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();
        d.finish_purges_now().unwrap();
        assert!(!holds(&d, "orders"), "reversed: {reversed}");
        assert!(!holds(&d, "items"), "reversed: {reversed}");
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
    a.put_vectors(&shadow_a, &DocId::Int64(1), &[chunk_of(1)]).unwrap();
    sync(&a, &d);
    assert!(holds(&d, &kimmy_core::vector_meta::shadow_name("orders")), "premise");
    (a, d, da, dd)
}

/// A chunk the embedding worker wrote after the drop for a document the sweep
/// removes does not keep the shadow alive; the chunk of a document that
/// survives does stay.
#[test]
fn a_late_chunk_of_a_swept_parent_is_removed_and_a_survivors_chunk_stays() {
    let (a, d, _da, _dd) = a_and_d_holding_a_vector_collection();
    assert!(a.drop_collection("shop", "orders").unwrap());
    later();
    let on_d = d.get_collection("shop", "orders").unwrap();
    d.insert(&on_d, doc! { "_id": 5i64 }).unwrap();
    let shadow = d.get_collection("shop", &kimmy_core::vector_meta::shadow_name("orders")).unwrap();
    d.put_vectors(&shadow, &DocId::Int64(1), &[chunk_of(1)]).unwrap();
    d.put_vectors(&shadow, &DocId::Int64(5), &[chunk_of(5)]).unwrap();
    quiesce(&[&a, &d]);
    assert_eq!(docs_of(&d, "orders", &[1, 5]), vec![5]);
    let shadow = d.get_collection("shop", &kimmy_core::vector_meta::shadow_name("orders")).unwrap();
    assert!(d.get_vectors(&shadow, &DocId::Int64(1)).unwrap().is_empty(), "a parentless chunk");
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
    d.put_vectors(&shadow, &DocId::Int64(5), &[chunk_of(5)]).unwrap();
    // Stop after the first commit of the survivors path: both lives are re-dated
    // and owe a sweep; nothing is swept yet.
    let asked = std::cell::Cell::new(0);
    let dropped = {
        let start = d.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
        let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
        entries.iter().find(|e| e.kind == kimmy_core::OpKind::DropCollection).unwrap().stamp
    };
    let result = d.drop_keeping_later_writes("shop", "orders", dropped, &|| {
        asked.set(asked.get() + 1);
        Err(crate::StorageError::Stopping(crate::StopReason::Shutdown))
    });
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
    let stopped = d.drop_keeping_later_writes("shop", "orders", dropped, &|| {
        Err(crate::StorageError::Stopping(crate::StopReason::Shutdown))
    });
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

/// M keeps a write through a collection's drop before it has heard of the
/// database's drop `S`, and a peer repairs the collection from M. Whether the
/// peer holds `S` already (it refuses the definition) or learns it after (it
/// buries what it installed), and whether M learns it last, the write is gone
/// everywhere, the collection is absent everywhere (ADR-229), and nothing loops.
#[test]
fn a_life_re_dated_before_the_databases_drop_is_known_does_not_outlive_it() {
    for peer_holds_the_database_drop_first in [true, false] {
        let (a, m, _da, _dm, id) = a_and_d_holding_the_old_life();
        a.create_collection("shop", "items").unwrap();
        sync(&a, &m);
        assert!(a.drop_collection("shop", "orders").unwrap());
        later();
        let on_m = m.get_collection("shop", "orders").unwrap();
        m.insert(&on_m, doc! { "_id": 3i64 }).unwrap();
        // M takes the collection's drop alone: the database's is not yet known to it.
        let start = m.version_vector().unwrap().behind(&a.version_vector().unwrap()).unwrap();
        let entries = a.entries_for_peer(start, BATCH, WalkScope::Background).unwrap().entries;
        m.apply_batch(&entries).unwrap();
        let kept = m.get_collection("shop", "orders").unwrap();
        assert!(kept.is_redated(), "premise: M kept the write");
        later();
        a.drop_database("shop").unwrap();
        a.finish_purges_now().unwrap();

        // P learns of the database's drop by a whole-database page.
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
            "the definition says it was re-dated"
        );
        p.apply_snapshot_page(m.node_id(), &mut SnapshotProgress::of_collection(id), &page)
            .unwrap();
        if peer_holds_the_database_drop_first {
            assert!(!holds(&p, "orders"), "refused at install: it began before S");
        } else {
            assert!(holds(&p, "orders"), "premise: P had nothing to refuse it with");
            let page = a.snapshot_page(None, None).unwrap();
            p.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page)
                .unwrap();
        }
        // And M learns of it last.
        quiesce(&[&a, &m, &p]);
        for (who, e) in [("a", &a), ("m", &m), ("p", &p)] {
            assert!(!holds(e, "orders"), "{who}, peer first: {peer_holds_the_database_drop_first}");
            assert!(!holds(e, "items"), "{who}");
            assert_counts_exact(e, who);
        }
    }
}
