//! The later-life stop a page holding no life answers (F7), and a relayed drop of an earlier
//! life in a collection stream (F4).

use std::collections::BTreeSet;
use std::time::Duration;

use bson::doc;
use kimmy_core::{CollectionId, IndexField};

use crate::sync::SyncOutcome;
use crate::watch::{ChangeEvent, WatchOptions, WatchScope};
use crate::{Engine, SnapshotProgress, WalkScope};

const BATCH: usize = 1024;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

/// A transport-shaped round, as `sync::tests::round` runs it.
fn round(into: &Engine, from: &Engine) -> SyncOutcome {
    let mine = into.witnessed_vector().unwrap();
    let theirs = from.version_vector().unwrap();
    match mine.behind(&theirs) {
        Some(start) => {
            let window = from.entries_for_peer(start, BATCH, WalkScope::Background).unwrap();
            into.apply_peer_batch(&theirs, &window.entries, window.scanned_to, window.exhausted)
                .unwrap()
        }
        None => SyncOutcome::default(),
    }
}

/// The ADR-148 scoped page an `unknown` stop asks for.
fn scoped(into: &Engine, from: &Engine, id: CollectionId) {
    let mut progress = SnapshotProgress::of_collection(id);
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
    }
}

/// Rounds of `into <- from` with the protocol's answer to a stop; how many stopped.
fn settle(into: &Engine, from: &Engine) -> usize {
    let mut stops = 0;
    for _ in 0..6 {
        let outcome = round(into, from);
        if let Some(stopped) = outcome.unknown {
            stops += 1;
            scoped(into, from, stopped.id);
        }
    }
    stops
}

fn ids_on(engine: &Engine, db: &str, name: &str) -> BTreeSet<String> {
    let Ok(coll) = engine.get_collection(db, name) else {
        return BTreeSet::new();
    };
    engine
        .snapshot_page(None, Some(coll.id))
        .unwrap()
        .documents
        .iter()
        .map(|d| d.id.to_string())
        .collect()
}

fn field() -> Vec<IndexField> {
    vec![IndexField { path: "n".into(), descending: false }]
}

/// F7, all members on this build. An index created after a concurrent collection drop
/// and dropped again before the drop arrives leaves nothing stamped after the drop, so
/// the creating member buries the life, and still holds and serves the `CreateIndex`.
/// A member that lacks the collection stops at it (`defines_a_later_life`), asks for the
/// scoped page, gets the same tombstone back, and stops again on every round: nothing
/// past the index in that peer's window, in any collection, reaches it by entries.
#[test]
fn finding_f7_an_index_created_and_dropped_after_a_drop_wedges_the_window() {
    let (a, _da) = engine();
    let (c, _dc) = engine();
    a.create_collection("shop", "orders").unwrap();
    let items = a.create_collection("shop", "items").unwrap();
    round(&c, &a);
    assert!(c.drop_collection("shop", "orders").unwrap());
    std::thread::sleep(Duration::from_millis(2));
    let index = a.create_index("shop", "orders", field(), false, None).unwrap();
    assert!(a.drop_index("shop", "orders", &index.name).unwrap());
    round(&a, &c);
    assert!(a.get_collection("shop", "orders").is_err(), "premise: A buried the life");
    a.insert(&items, doc! { "_id": 1i64 }).unwrap();

    let stops = settle(&c, &a);
    assert_eq!(
        ids_on(&c, "shop", "items"),
        ids_on(&a, "shop", "items"),
        "C never took A's acknowledged write behind the index ({stops} stops in 6 rounds)"
    );
    assert!(c.witnessed_vector().unwrap().covers(&a.version_vector().unwrap()));
}

/// F7 in a mixed roll. A 0.49.x member applies a database's drop to a life it holds by
/// burying it writes and all (ADR-229's D-3 guard), an index stamped after the drop
/// with it, and still serves the `CreateIndex`. Simulated here with the burial 0.49.x
/// makes (`bury_collection_as` at the drop's stamp). A 0.50.0 member that lacks the
/// collection stops at the index on every round, as above.
#[test]
fn finding_f7_a_049_burial_of_a_later_definition_wedges_a_050_puller() {
    let (s, _ds) = engine(); // stands for a 0.49.x member
    let (r, _dr) = engine(); // 0.50.0
    s.create_collection("shop", "orders").unwrap();
    round(&r, &s);
    assert!(r.drop_database("shop").unwrap());
    std::thread::sleep(Duration::from_millis(2));
    s.create_index("shop", "orders", field(), false, None).unwrap();
    // What 0.49.x does on the drop's arrival: the database's tombstone, then the
    // covered life buried at the drop's stamp whatever is stamped after it.
    let dropped = r.database_dropped_at("shop").unwrap().expect("the database's tombstone");
    s.record_database_tombstone_for_test(kimmy_core::database_drop_id("shop"), dropped).unwrap();
    s.bury_collection_as(
        "shop",
        "orders",
        Some(dropped),
        crate::engine::Continuing::No,
        crate::database_tombstone::Notify::Each,
    )
    .unwrap()
    .expect("premise: buried");
    round(&s, &r);
    assert!(s.get_collection("shop", "orders").is_err(), "premise: S holds no life");
    let things = s.create_collection("other", "things").unwrap();
    s.insert(&things, doc! { "_id": 1i64 }).unwrap();

    let stops = settle(&r, &s);
    assert_eq!(
        ids_on(&r, "other", "things"),
        ids_on(&s, "other", "things"),
        "R never took S's acknowledged write behind the index ({stops} stops in 6 rounds)"
    );
}

/// F4. A drop this member ignores (its own life is newer) is now appended and published,
/// and a collection stream open on the live collection takes it as the collection's drop:
/// `Invalidate{CollectionDropped}` for a collection that stands, holding its documents.
/// Before ADR-236 the ignored drop was neither appended nor published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finding_f4_a_relayed_ignored_drop_ends_a_stream_on_the_live_collection() {
    let (c, _dc) = engine();
    let (m, _dm) = engine();
    c.create_collection("shop", "orders").unwrap();
    assert!(c.drop_collection("shop", "orders").unwrap());
    std::thread::sleep(Duration::from_millis(2));
    let live = m.create_collection("shop", "orders").unwrap();
    m.insert(&live, doc! { "_id": 1i64 }).unwrap();
    let mut stream = m.watch(WatchScope::Collection(live.id), WatchOptions::default()).unwrap();
    round(&m, &c);
    let standing = m.get_collection("shop", "orders").expect("premise: the drop is ignored");
    assert_eq!(standing.created, live.created, "premise: the same life");
    m.insert(&live, doc! { "_id": 2i64 }).unwrap();
    loop {
        let heard = tokio::time::timeout(Duration::from_secs(2), stream.next(&m))
            .await
            .expect("the stream answered");
        match heard {
            Some(ChangeEvent::Change { entry, .. }) if entry.kind.is_document() => break,
            Some(ChangeEvent::Change { .. }) => continue,
            other => panic!("a stream on a live collection ended: {other:?}"),
        }
    }
}
