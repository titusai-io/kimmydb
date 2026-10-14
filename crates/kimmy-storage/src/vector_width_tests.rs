//! A client-supplied vector configuration's width, changed through an off, on
//! the entries route and on a snapshot page (ADR-227, B4 item 4).
//!
//! The sender cannot change a `byo` width in place; it turns the configuration
//! off between the two. A dated page applies that history as one change. An
//! entry that arrives without the off (the off lost on its stamp to a change made
//! here, or not yet applied) was refused instead, so the same history ended in
//! two states depending on the route. A dated change from elsewhere now applies
//! the implied off on both; a local change is refused as it always was.

use kimmy_core::{Hlc, ProviderConfig, VectorConfig};

use crate::{Engine, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn byo(dim: usize) -> VectorConfig {
    VectorConfig {
        fields: vec!["body".into()],
        provider: ProviderConfig::Byo {},
        dim,
        metric: kimmy_core::Metric::Cosine,
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn pull(into: &Engine, from: &Engine) -> crate::SyncOutcome {
    let entries = from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap()
}

fn transfer(into: &Engine, from: &Engine) {
    let mut progress = SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
    }
}

fn width(e: &Engine) -> Option<usize> {
    e.get_collection("shop", "orders").unwrap().vector.map(|v| v.dim)
}

/// `a` and `b` both hold width 8; then `a` turns it off (`s_off`), `b` configures
/// width 8 again (`s_8 > s_off`) and `a` configures width 16 (`s_16 > s_8`): one
/// origin's width 8 at a stamp the receiver holds, the other's off below it and a
/// width above it.
fn concurrent_width_change() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    a.create_collection("shop", "orders").unwrap();
    a.configure_vectors("shop", "orders", byo(8)).unwrap();
    let (b, db) = engine();
    pull(&b, &a);
    assert_eq!(width(&b), Some(8), "premise");
    later();
    assert!(a.disable_vectors("shop", "orders", false).unwrap());
    later();
    b.configure_vectors("shop", "orders", byo(8)).unwrap();
    later();
    a.configure_vectors("shop", "orders", byo(16)).unwrap();
    let held = b.get_collection("shop", "orders").unwrap().vector_changed.unwrap();
    let wide = a.get_collection("shop", "orders").unwrap().vector_changed.unwrap();
    assert!(held < wide, "premise: the receiver's stamp is below `a`'s width 16");
    (a, b, [da, db])
}

#[test]
fn the_entries_route_and_a_page_end_in_the_same_width() {
    let (a, via_page, _dirs) = concurrent_width_change();
    transfer(&via_page, &a);
    let (a, via_entries, _dirs) = concurrent_width_change();
    let outcome = pull(&via_entries, &a);
    assert_eq!(width(&via_entries), width(&via_page), "refused {}", outcome.ddl_refused);
    assert_eq!(width(&via_entries), Some(16));
}

#[test]
fn the_entry_is_applied_not_refused_and_the_stamp_is_the_origins() {
    let (a, b, _dirs) = concurrent_width_change();
    let outcome = pull(&b, &a);
    assert_eq!(outcome.ddl_refused, 0, "applied, not refused");
    assert_eq!(
        b.get_collection("shop", "orders").unwrap().vector_changed,
        a.get_collection("shop", "orders").unwrap().vector_changed,
        "at the origin's stamp"
    );
}

#[test]
fn the_stored_vectors_of_the_old_width_are_kept_as_for_an_off() {
    let (a, b, _dirs) = concurrent_width_change();
    let shadow = b.get_collection("shop", "orders.__vectors").unwrap();
    b.put_vectors(
        &shadow,
        &kimmy_core::DocId::Int64(1),
        &[kimmy_core::VectorRecord {
            source: kimmy_core::DocId::Int64(1),
            chunk: 0,
            source_hlc: Hlc::new(1, 0),
            vector: vec![0.5; 8],
            text: "eight".into(),
            config: None,
        }],
    )
    .unwrap();
    pull(&b, &a);
    // A burial of the shadow is purged in the background; finish it, so a regression
    // that starts deleting the old-width rows cannot hide behind the purger.
    b.finish_purges_now().unwrap();
    let kept: Vec<usize> = b
        .get_vectors(&shadow, &kimmy_core::DocId::Int64(1))
        .unwrap()
        .iter()
        .map(|r| r.vector.len())
        .collect();
    assert_eq!(kept, vec![8], "an entry from a peer never deletes data");
}

#[test]
fn an_entry_that_loses_on_its_stamp_changes_nothing_whatever_its_width() {
    let (a, b, _dirs) = {
        let (a, da) = engine();
        a.create_collection("shop", "orders").unwrap();
        a.configure_vectors("shop", "orders", byo(8)).unwrap();
        let (b, db) = engine();
        pull(&b, &a);
        later();
        a.disable_vectors("shop", "orders", false).unwrap();
        a.configure_vectors("shop", "orders", byo(16)).unwrap();
        later();
        // The receiver's change is the newest.
        b.configure_vectors("shop", "orders", byo(8)).unwrap();
        (a, b, [da, db])
    };
    let before = b.get_collection("shop", "orders").unwrap();
    pull(&b, &a);
    assert_eq!(b.get_collection("shop", "orders").unwrap(), before);
}

#[test]
fn a_local_change_of_width_is_still_refused() {
    let (a, _da) = engine();
    a.create_collection("shop", "orders").unwrap();
    a.configure_vectors("shop", "orders", byo(8)).unwrap();
    let err = a.configure_vectors("shop", "orders", byo(16)).unwrap_err();
    assert!(err.to_string().contains("dimension cannot change"), "{err}");
    assert_eq!(width(&a), Some(8));
}

#[test]
fn an_entry_of_an_older_life_with_another_width_changes_nothing() {
    // The exemption sits below the history check: an entry at or below the standing
    // life's incarnation floor is the previous life's and is not applied, whatever
    // its width. The standing configuration is left unstamped so only the history
    // check, not the last-writer-wins guard, can turn the entry away.
    let (b, _db) = engine();
    let low = kimmy_core::NodeId::from_bytes([1; 16]);
    let id = kimmy_core::CollectionId::derive("shop", "orders");
    b.record_collection_drop(id, kimmy_core::Stamp::new(Hlc::new(1_000, 0), low)).unwrap();
    b.create_collection("shop", "orders").unwrap();
    b.configure_vectors("shop", "orders", byo(8)).unwrap();
    {
        let mut meta = b.get_collection("shop", "orders").unwrap();
        assert_eq!(meta.incarnation_floor, Some(Hlc::new(1_000, 0)), "premise");
        meta.vector_changed = None;
        let txn = b.begin_write(crate::WriterHolder::Ddl).unwrap();
        Engine::put_collection_meta(&txn, &meta).unwrap();
        txn.commit().unwrap();
    }
    let entry = crate::engine::ddl_entry(
        kimmy_core::Stamp::new(Hlc::new(500, 0), low),
        kimmy_core::OpKind::ConfigureVectors,
        id,
        &kimmy_core::VectorSet {
            db: "shop".into(),
            collection: "orders".into(),
            config: Some(byo(16)),
        },
    )
    .unwrap();
    let outcome = b.apply_batch(std::slice::from_ref(&entry)).unwrap();
    assert_eq!(width(&b), Some(8), "an older life's change was applied");
    assert_eq!(outcome.ddl_refused, 0, "history is skipped, not refused");
}
