//! Step 3 item (3.4b): a sweep of a late drop that stops after a chunk thins the rest when it resumes. Kept
//! apart from the tests of the sweep's result because it needs the size of a chunk, which the commit that
//! makes the sweep thin makes visible to the crate's tests.

use bson::doc;
use kimmy_core::{Hlc, MergePolicy, OpKind, Stamp};

use crate::Engine;
use crate::live_count::test_support::{assert_counts_exact, assert_digests_exact};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

const MESSAGE: &str =
    "the cells of the life before the drop do not stay in a record stamped after it";

/// More records than a chunk: the sweep stops after the first chunk and resumes. The records of the first
/// chunk are thinned once and the rest wait; the resume finishes them and rewrites none of the first.
#[test]
fn a_sweep_stopped_after_a_chunk_thins_the_rest_when_it_resumes_and_none_twice() {
    use std::cell::Cell;
    let n = (crate::late_drop::SWEEP_CHUNK + 20) as i64;
    let (a, _da) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    for i in 0..n {
        a.insert(&ca, doc! { "_id": i, "a": 1 }).unwrap();
    }
    let last_insert = a
        .entries_for_peer(Hlc::ZERO, 100_000, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.kind == OpKind::Insert)
        .map(|e| e.stamp)
        .max()
        .unwrap();
    later();
    for i in 0..n {
        let id = kimmy_core::DocId::Int64(i);
        a.replace(&ca, &id, doc! { "_id": i, "a": 1, "b": 2 }, false).unwrap();
    }
    let writes: Vec<_> = a
        .entries_for_peer(Hlc::ZERO, 100_000, crate::WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.kind == OpKind::Replace)
        .collect();
    assert_eq!(writes.len() as i64, n, "premise: a write for each document");
    later();
    let (b, _db) = engine();
    let cb = b.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    b.apply_batch(&writes).unwrap();
    let ids: Vec<kimmy_core::DocId> = (0..n).map(kimmy_core::DocId::Int64).collect();
    let mixed = |e: &Engine| {
        ids.iter().filter(|id| e.get(&cb, id).unwrap().is_some_and(|d| d.contains_key("a"))).count()
    };
    assert_eq!(
        mixed(&b) as i64,
        n,
        "premise: B holds every document with the cell of the life before"
    );
    let dropped = Stamp::new(last_insert.hlc, b.node_id(), b.epoch());
    assert!(
        writes.iter().all(|w| w.stamp.hlc > dropped.hlc),
        "premise: every write is after the drop"
    );
    b.record_collection_drop(cb.id, dropped).unwrap();

    let asked = Cell::new(0);
    let one_chunk = || {
        asked.set(asked.get() + 1);
        match asked.get() > 1 {
            true => Err(crate::StorageError::Stopping(crate::StopReason::Shutdown)),
            false => Ok(()),
        }
    };
    assert!(
        matches!(
            b.sweep_late_drop_until("shop", "orders", dropped, &one_chunk),
            Err(crate::StorageError::Stopping(_))
        ),
        "the sweep stops after the first chunk"
    );
    let thinned = n as usize - mixed(&b);
    assert_eq!(
        thinned,
        crate::late_drop::SWEEP_CHUNK,
        "the first chunk's records are thinned and the rest wait"
    );
    let first = b.get_versioned(&cb, &ids[0]).unwrap();

    b.sweep_late_drop("shop", "orders", dropped).unwrap();
    assert_eq!(mixed(&b), 0, "{MESSAGE}: the resume thinned the rest");
    assert_eq!(
        b.get_versioned(&cb, &ids[0]).unwrap(),
        first,
        "a record of the first chunk is not rewritten again"
    );
    assert_counts_exact(&b, "a sweep resumed after a chunk");
    assert_digests_exact(&b, "a sweep resumed after a chunk");
}
