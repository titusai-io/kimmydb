//! Unit P, P10b: a page that shows its life stands above the floor makes the standing life KEPT, not buried. A member that holds the life from before
//! a drop with nothing above it takes a sender's life that the drop re-dated (it holds writes made after the drop). The page names the drop's floor,
//! which the member has not taken, so the member applies it as a carried drop; the keep test finds nothing above the drop, and the burial it used to
//! take left the old rows to the purger and the member without the collection until the purge finished and the page came again (`NOT FOUND` for a
//! client, and a stream on it ended). Now the page's decision keeps the life: re-dated to the drop's successor with the marker, its rows at or below the
//! floor swept in place, and the page's documents applied into it: the same end state, the member is never absent, and no purge is owed.

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, OpKind, OplogEntry};

use crate::{Engine, SnapshotProgress, WalkScope};

const DB: &str = "shop";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn pull(into: &Engine, from: &Engine) {
    let entries = from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    into.apply_batch(&entries).unwrap();
}

fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn id() -> CollectionId {
    CollectionId::derive(DB, "orders")
}

/// Never NOT FOUND, and the end state unchanged. `x` made `orders` with document 1; `z` took it and dropped it (D); `m` took it before the drop and
/// holds nothing above it; `s` took it, wrote more than a page of documents after the drop (not having heard it) and then took the drop: its life is
/// kept, re-dated, with those documents. `m` pulls `s`'s collection by a scoped snapshot, a page at a time. After EVERY page the collection answers
/// on `m`; the pull never waits on a purge; and it ends with the sender's life: its documents, no document 1, the drop as its floor, and the
/// sender's `created`. Mutants (P10b): the page's decision ignored (the life is buried: the collection is absent after the first page, and the
/// page waits for the purge); the sweep's last chunk buries the life it leaves empty.
#[test]
fn a_member_holding_the_life_before_the_drop_is_never_without_the_collection_while_it_takes_the_re_dated_life()
 {
    let (x, _dx) = engine();
    let first = x.create_collection(DB, "orders").unwrap();
    x.insert(&first, doc! { "_id": 1 }).unwrap();
    let (z, _dz) = engine();
    pull(&z, &x);
    let (m, _dm) = engine();
    pull(&m, &x);
    let (s, _ds) = engine();
    pull(&s, &x);
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    let drops: Vec<OplogEntry> = z
        .entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background)
        .unwrap()
        .entries
        .into_iter()
        .filter(|e| e.kind == OpKind::DropCollection)
        .collect();
    pause();
    let on_s = s.get_collection(DB, "orders").unwrap();
    let n = crate::SNAPSHOT_PAGE as i64 + 8;
    s.insert_many(&on_s, (2..2 + n).map(|i| doc! { "_id": i }).collect()).unwrap();
    s.apply_batch(&drops).unwrap();
    let kept = s.get_collection(DB, "orders").expect("the sender keeps its life");
    assert!(kept.is_redated(), "premise: the sender's life is re-dated by the drop: {kept:?}");
    let drop_hlc = drops[0].stamp.hlc;
    assert!(m.get(&m.get_collection(DB, "orders").unwrap(), &DocId::Int64(1)).unwrap().is_some());
    assert_eq!(
        m.get_collection(DB, "orders").unwrap().incarnation_floor,
        None,
        "premise: the receiver has not taken the drop"
    );

    let mut progress = SnapshotProgress::of_collection(id());
    let mut pages = 0;
    while !progress.is_complete() {
        let page = s.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        let applied = m.apply_snapshot_page(s.node_id(), &mut progress, &page).unwrap();
        assert_eq!(applied.purge_pending, None, "no purge is owed: {applied:?}");
        assert!(
            m.get_collection(DB, "orders").is_ok(),
            "the collection answers on the member after page {pages}"
        );
        pages += 1;
        assert!(pages < 20, "the pull does not end");
    }
    assert!(pages >= 2, "premise: the pull took more than one page");

    let held = m.get_collection(DB, "orders").unwrap();
    assert_eq!(held.created, kept.created, "the sender's life");
    assert_eq!(held.incarnation_floor, Some(drop_hlc), "with the drop as its floor");
    assert_eq!(m.count(&held, WalkScope::Background).unwrap(), n as u64, "its documents");
    assert!(m.get(&held, &DocId::Int64(1)).unwrap().is_none(), "and not the one the drop ended");
}
