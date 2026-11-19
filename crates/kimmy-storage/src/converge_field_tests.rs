//! The `Converge` policy over a `field` collection (Step 3, item 3.5): a row of a `field` document is judged
//! by what its state holds, not by the stamp of the record, which is only its newest write.
//!
//! The young-stamp rule asks whether a row could be one a collected tombstone killed. A delete kills the
//! lives whose creator had not seen it, so the question is about the lives of the state: a row is young when
//! every life of it that is not dead was made above the horizon.

use bson::doc;
use kimmy_core::merge_state::MergeState;
use kimmy_core::{CollectionId, Hlc, MergePolicy, VersionVector};

use crate::{Engine, SnapshotApplied, SnapshotProgress, WalkScope};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn orders() -> CollectionId {
    CollectionId::derive("shop", "orders")
}

fn field_collection(e: &Engine) -> crate::CollectionMeta {
    e.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap()
}

fn coll(e: &Engine) -> crate::CollectionMeta {
    e.get_collection("shop", "orders").unwrap()
}

fn everything(from: &Engine) -> Vec<kimmy_core::OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// A sender and a requester of one `field` collection, opened before any row is written so that the
/// horizons they reset to at open sit below every row of the test.
fn field_pair() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, da) = engine();
    let (b, db) = engine();
    field_collection(&a);
    b.apply_batch(&everything(&a)).unwrap();
    later();
    (a, b, [da, db])
}

/// The lives of a state that are not dead, read from its fields: the tests of this file hold over a tree
/// without `MergeState::alive_lives`.
fn alive(state: &MergeState) -> impl Iterator<Item = kimmy_core::merge_state::MStamp> + '_ {
    state.lives.keys().filter(|life| !state.dead.contains(*life)).copied()
}

/// The state the sender's scoped page carries for its one document.
fn state_of_the_row(from: &Engine) -> MergeState {
    let page = from.snapshot_page(None, Some(orders())).unwrap();
    assert_eq!(page.documents.len(), 1, "premise: one row");
    crate::field_write::load(page.documents[0].meta.as_deref()).unwrap()
}

/// Apply the sender's whole scoped snapshot to `into` under `Converge`, with `cut` as the finding's, page by
/// page, summing what every page reported.
fn converge(into: &Engine, from: &Engine, cut: VersionVector) -> SnapshotApplied {
    converge_with(into, from, cut, |_| {})
}

/// [`converge`] over pages `tweak` has changed.
fn converge_with(
    into: &Engine,
    from: &Engine,
    cut: VersionVector,
    tweak: impl Fn(&mut crate::SnapshotPage),
) -> SnapshotApplied {
    let mut progress = SnapshotProgress::of_collection_converging(orders(), cut);
    let mut total = SnapshotApplied::default();
    while !progress.is_complete() {
        let mut page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        tweak(&mut page);
        let done = into
            .apply_snapshot_page(from.node_id(), &mut progress, &page)
            .expect("a page is applied, or its rows are left");
        total.applied += done.applied;
        total.left += done.left;
        total.created += done.created;
        total.applied_at_cut += done.applied_at_cut;
        total.created_at_cut += done.created_at_cut;
    }
    total
}

/// The horizon of the receiver, set to now: what was written before it is at or below it, and what is
/// written after the pause is above it.
fn collected_up_to_now(e: &Engine) -> Hlc {
    let horizon = Hlc::new(crate::physical_now_ms(), 0);
    e.test_set_tombstone_horizon(horizon).unwrap();
    later();
    horizon
}

#[test]
fn a_row_whose_life_is_at_or_below_the_horizon_is_left_though_its_newest_write_is_above_it() {
    let (a, b, _d) = field_pair();
    let ca = coll(&a);
    let x = a.insert(&ca, doc! { "_id": 3, "a": 1 }).unwrap();
    later();
    let horizon = collected_up_to_now(&b);
    a.replace(&ca, &x, doc! { "_id": 3, "a": 1, "b": 2 }, false).unwrap();
    let page = a.snapshot_page(None, Some(orders())).unwrap();
    assert!(
        page.documents[0].stamp.hlc > horizon,
        "premise: the newest write is above the horizon"
    );
    assert!(
        alive(&state_of_the_row(&a)).any(|life| life.hlc <= horizon),
        "premise: the life the row carries was made at or below it"
    );
    let done = converge(&b, &a, VersionVector::new());
    assert_eq!(
        b.get(&coll(&b), &x).unwrap(),
        None,
        "a life a collected delete may have killed is not brought back by a newer write in it"
    );
    assert_eq!((done.created, done.left), (0, 1));
}

#[test]
fn a_row_whose_lives_are_all_above_the_horizon_is_created() {
    let (a, b, _d) = field_pair();
    let ca = coll(&a);
    collected_up_to_now(&b);
    let x = a.insert(&ca, doc! { "_id": 3, "a": 1 }).unwrap();
    later();
    a.replace(&ca, &x, doc! { "_id": 3, "a": 1, "b": 2 }, false).unwrap();
    let done = converge(&b, &a, VersionVector::new());
    assert_eq!(b.get(&coll(&b), &x).unwrap(), Some(doc! { "_id": 3, "a": 1, "b": 2 }));
    assert_eq!((done.created, done.left), (1, 0));
}

#[test]
fn a_life_the_state_holds_dead_does_not_keep_a_row_from_being_young() {
    // A made X, deleted it, and made it again: the row is the second life, and carries the first as dead.
    let (a, b, _d) = field_pair();
    let ca = coll(&a);
    let x = a.insert(&ca, doc! { "_id": 3, "a": 1 }).unwrap();
    later();
    a.delete(&ca, &x).unwrap();
    later();
    let horizon = collected_up_to_now(&b);
    a.insert(&ca, doc! { "_id": 3, "a": 2 }).unwrap();
    let state = state_of_the_row(&a);
    assert_eq!(state.lives.len(), 2, "premise: the row carries both lives");
    assert!(
        state.dead.iter().all(|life| life.hlc <= horizon) && state.dead.len() == 1,
        "premise: one of them is dead, and made at or below the horizon"
    );
    let done = converge(&b, &a, VersionVector::new());
    assert_eq!(
        b.get(&coll(&b), &x).unwrap(),
        Some(doc! { "_id": 3, "a": 2 }),
        "a dead life renders nothing and is not brought back, so it does not block the life beside it"
    );
    assert_eq!((done.created, done.left), (1, 0));
}

/// Two lives of one document alive at once, which an honest history makes only by two concurrent re-inserts after
/// a delete (a first insert of one `_id` has one winner, and the others are dead). S and C hold the field
/// collection and X; S deletes it, both take the delete. The caller makes X again at S, and then at C, from a
/// state that has seen the delete, and S takes C's insert: S holds the life it made and C's alive, and the
/// life the delete killed dead.
fn s_and_c_that_hold_a_deleted_x()
-> (Engine, Engine, crate::CollectionMeta, crate::CollectionMeta, [tempfile::TempDir; 2]) {
    let (s, ds) = engine();
    let (c, dc) = engine();
    let cs = field_collection(&s);
    let x = s.insert(&cs, doc! { "_id": 3, "a": 1 }).unwrap();
    c.apply_batch(&everything(&s)).unwrap();
    let cc = coll(&c);
    later();
    s.delete(&cs, &x).unwrap();
    c.apply_batch(&everything(&s)).unwrap();
    later();
    (s, c, cs, cc, [ds, dc])
}

#[test]
fn a_life_only_the_floor_made_history_does_not_keep_a_row_from_being_young() {
    // S made X again before B's drop; C made it after, and S holds both lives alive. B stands over the floor of
    // the drop: the first life is history there, and what the row would leave is the second.
    let (s, c, cs, cc, _d) = s_and_c_that_hold_a_deleted_x();
    let (b, _db) = engine();
    field_collection(&b);
    let x = s.insert(&cs, doc! { "_id": 3, "a": 2 }).unwrap();
    later();
    assert!(b.drop_collection("shop", "orders").unwrap());
    b.finish_purges_now().unwrap();
    later();
    let cb = field_collection(&b);
    let floor = cb.incarnation_floor.expect("premise: B's collection stands over a floor");
    later();
    let horizon = collected_up_to_now(&b);
    assert!(horizon > floor, "premise: the horizon is above the floor");
    c.insert(&cc, doc! { "_id": 3, "b": 2 }).unwrap();
    s.apply_batch(&everything(&c)).unwrap();
    let state = state_of_the_row(&s);
    assert_eq!(alive(&state).count(), 2, "premise: S holds two lives alive");
    assert!(
        alive(&state).any(|life| life.hlc <= floor),
        "premise: one of them is at or below the floor, and so at or below the horizon"
    );
    let done = converge(&b, &s, VersionVector::new());
    assert_eq!(
        b.get(&coll(&b), &x).unwrap(),
        Some(doc! { "_id": 3, "b": 2 }),
        "the life before the drop is history here and does not block the life after it"
    );
    assert_eq!((done.created, done.left), (1, 0));
}

#[test]
fn a_row_with_one_life_at_or_below_the_horizon_and_one_above_is_left_whole() {
    // S made X again before the horizon and C made it after; S holds both lives alive. One of them may have
    // been killed by a delete B has collected, and the row is not applied in part: a life is not brought back
    // without the lives beside it.
    let (s, c, cs, cc, _d) = s_and_c_that_hold_a_deleted_x();
    let (b, _db) = engine();
    field_collection(&b);
    let x = s.insert(&cs, doc! { "_id": 3, "a": 2 }).unwrap();
    later();
    let horizon = collected_up_to_now(&b);
    c.insert(&cc, doc! { "_id": 3, "b": 2 }).unwrap();
    s.apply_batch(&everything(&c)).unwrap();
    let state = state_of_the_row(&s);
    assert_eq!(alive(&state).count(), 2, "premise: S holds two lives alive");
    assert!(
        alive(&state).any(|life| life.hlc <= horizon)
            && alive(&state).any(|life| life.hlc > horizon),
        "premise: one of them is at or below the horizon and one above it"
    );
    let done = converge(&b, &s, VersionVector::new());
    assert_eq!(
        b.get(&coll(&b), &x).unwrap(),
        None,
        "a row is left whole while any life of it may have been killed: it is not applied in part"
    );
    assert_eq!((done.created, done.left), (0, 1));
}

#[test]
fn a_row_whose_state_does_not_decode_is_left() {
    let (a, b, _d) = field_pair();
    let ca = coll(&a);
    let x = a.insert(&ca, doc! { "_id": 3, "a": 1 }).unwrap();
    later();
    let done = converge_with(&b, &a, VersionVector::new(), |page| {
        for document in &mut page.documents {
            document.meta = Some(vec![0xFF; 3]);
        }
    });
    assert_eq!(
        b.get(&coll(&b), &x).unwrap(),
        None,
        "a row whose state does not decode is left, not applied"
    );
    assert_eq!((done.created, done.left), (0, 1));
}

// -- at_cut: what a pull counts as progress ----------------------------------------------

/// The cut of a finding that stands at `stamp` for the origin that made it.
fn cut_at(stamp: kimmy_core::Stamp) -> VersionVector {
    let mut cut = VersionVector::new();
    cut.observe(stamp);
    cut
}

/// A made X and wrote to it again: the row carries both writes, under the stamp of the second. The cut is
/// between them, so the first is at the cut and the stamp of the row is above it.
fn a_row_with_a_write_at_the_cut_under_a_stamp_above_it(
    a: &Engine,
) -> (kimmy_core::DocId, VersionVector) {
    let ca = coll(a);
    let x = a.insert(&ca, doc! { "_id": 3, "a": 1 }).unwrap();
    let first = everything(a).into_iter().find(|e| e.kind == kimmy_core::OpKind::Insert).unwrap();
    later();
    a.replace(&ca, &x, doc! { "_id": 3, "a": 1, "b": 2 }, false).unwrap();
    let page = a.snapshot_page(None, Some(orders())).unwrap();
    let cut = cut_at(first.stamp);
    assert!(
        page.documents[0].stamp.hlc > cut.get(page.documents[0].stamp.origin()),
        "premise: the stamp of the row is above the cut"
    );
    (x, cut)
}

#[test]
fn a_row_applied_with_a_write_at_the_cut_under_a_stamp_above_it_counts_at_the_cut() {
    let (a, b, _d) = field_pair();
    let ca = coll(&a);
    let x = a.insert(&ca, doc! { "_id": 3, "a": 1 }).unwrap();
    let first = everything(&a).into_iter().find(|e| e.kind == kimmy_core::OpKind::Insert).unwrap();
    b.apply_batch(&everything(&a)).unwrap();
    later();
    a.replace(&ca, &x, doc! { "_id": 3, "a": 1, "b": 2 }, false).unwrap();
    let done = converge(&b, &a, cut_at(first.stamp));
    assert_eq!(b.get(&coll(&b), &x).unwrap(), Some(doc! { "_id": 3, "a": 1, "b": 2 }));
    assert_eq!(done.applied, 1, "premise: the row was applied");
    assert_eq!(
        done.applied_at_cut, 1,
        "the row carries a write at the cut, which ordinary replication would not have delivered"
    );
}

#[test]
fn a_row_created_with_a_write_at_the_cut_under_a_stamp_above_it_counts_at_the_cut() {
    let (a, b, _d) = field_pair();
    let (x, cut) = a_row_with_a_write_at_the_cut_under_a_stamp_above_it(&a);
    let done = converge(&b, &a, cut);
    assert_eq!(b.get(&coll(&b), &x).unwrap(), Some(doc! { "_id": 3, "a": 1, "b": 2 }));
    assert_eq!(done.created, 1, "premise: the row was created");
    assert_eq!(done.created_at_cut, 1, "the row carries a write at the cut");
}

#[test]
fn a_row_with_every_write_above_the_cut_does_not_count_at_the_cut() {
    let (a, b, _d) = field_pair();
    a_row_with_a_write_at_the_cut_under_a_stamp_above_it(&a);
    let long_ago = Hlc::new(crate::physical_now_ms() - 3_600_000, 0);
    let done = converge(&b, &a, cut_at(kimmy_core::Stamp::new(long_ago, a.node_id(), a.epoch())));
    assert_eq!(done.created, 1, "premise: the row was created");
    assert_eq!(
        (done.created_at_cut, done.applied_at_cut),
        (0, 0),
        "a row of tail writes is one ordinary replication also brings"
    );
}

// ---- a state-only move is a write (the page that moves only a document's stored state) -------------------

/// Two members of one `field` collection that hold the same document, `{x: 7}`, with different states:
/// both wrote `x = 7` concurrently, and only `a` has taken the other's write. `a`'s state holds both writes
/// (its version vector names both writers and its records name the clash); `b`'s holds its own. The document
/// either renders is the same, so what a pull from `a` has to move into `b` is the state and nothing else.
fn a_richer_and_a_poorer_member() -> (Engine, Engine, kimmy_core::DocId, [tempfile::TempDir; 2]) {
    let (a, b, dirs) = field_pair();
    let (ca, cb) = (coll(&a), coll(&b));
    let id = a.insert(&ca, doc! { "_id": 1, "x": 0 }).unwrap();
    later();
    b.apply_batch(&everything(&a)).unwrap();
    a.replace(&ca, &id, doc! { "_id": 1, "x": 7 }, false).unwrap();
    b.replace(&cb, &id, doc! { "_id": 1, "x": 7 }, false).unwrap();
    later();
    a.apply_batch(&everything(&b)).unwrap();
    assert_eq!(
        a.get(&ca, &id).unwrap(),
        Some(doc! { "_id": 1, "x": 7 }),
        "premise: the richer member"
    );
    assert_eq!(
        b.get(&cb, &id).unwrap(),
        Some(doc! { "_id": 1, "x": 7 }),
        "premise: the poorer member"
    );
    assert_ne!(
        state_of_the_row(&a),
        state_of_the_row(&b),
        "premise: the same document over different states"
    );
    (a, b, id, dirs)
}

#[test]
fn a_converge_pull_that_moves_only_a_documents_state_writes_it() {
    let (a, b, id, _d) = a_richer_and_a_poorer_member();

    let done = converge(&b, &a, VersionVector::new());

    assert_eq!(
        b.get(&coll(&b), &id).unwrap(),
        Some(doc! { "_id": 1, "x": 7 }),
        "the document is as it was"
    );
    assert_eq!(
        state_of_the_row(&b),
        state_of_the_row(&a),
        "the stored state is the richer member's"
    );
    assert_eq!(done.applied, 1, "and the pull counts it, or the repair is judged ineffective");
}

#[test]
fn a_page_of_a_whole_database_snapshot_that_is_not_the_last_and_moves_only_a_state_writes_it() {
    let (a, b, id, _d) = a_richer_and_a_poorer_member();
    let mut page = a.snapshot_page(None, None).unwrap();
    // Not the last page: no grant rides it, so the only thing that can commit it is a write.
    page.next = Some(crate::SnapshotCursor {
        collection: orders(),
        after_key: vec![0xff],
        until_key: None,
    });

    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

    assert_eq!(
        b.get(&coll(&b), &id).unwrap(),
        Some(doc! { "_id": 1, "x": 7 }),
        "the document is as it was"
    );
    assert_eq!(
        state_of_the_row(&b),
        state_of_the_row(&a),
        "the stored state is the richer member's"
    );
}

#[test]
fn a_window_that_moves_only_a_documents_state_writes_it() {
    // The other two routes already kept the move: a run's transaction commits whole. Pinned so the new outcome
    // cannot change it.
    let (a, b, id, _d) = a_richer_and_a_poorer_member();

    b.apply_batch(&everything(&a)).unwrap();

    assert_eq!(b.get(&coll(&b), &id).unwrap(), Some(doc! { "_id": 1, "x": 7 }));
    assert_eq!(state_of_the_row(&b), state_of_the_row(&a));
}

#[test]
fn a_single_entry_whose_state_moves_only_the_stored_state_is_committed() {
    let (a, b, id, _d) = a_richer_and_a_poorer_member();
    let before = state_of_the_row(&b);
    let replace = everything(&a)
        .into_iter()
        .filter(|entry| {
            entry.stamp.node == a.node_id() && entry.kind == kimmy_core::OpKind::Replace
        })
        .max_by_key(|entry| entry.stamp)
        .expect("the richer member's own write");

    let applied = b.apply_remote(&coll(&b), &replace).unwrap();

    assert!(!applied, "the document did not move, so no entry won");
    assert_eq!(b.get(&coll(&b), &id).unwrap(), Some(doc! { "_id": 1, "x": 7 }));
    assert_ne!(
        state_of_the_row(&b),
        before,
        "the stored state moved: a rollback would have left it as it was"
    );
}

// ---- the same for the state a carried delete brings (a tombstone's) --------------------------------------

/// Two members that both deleted the one document: `a` has taken the other's delete and so holds a tombstone whose
/// state keeps both; `b` holds the tombstone of its own delete. No document is rendered on either side, so what a
/// pull from `a` has to move into `b` is the tombstone's state and nothing else.
fn a_richer_and_a_poorer_tombstone() -> (Engine, Engine, [tempfile::TempDir; 2]) {
    let (a, b, dirs) = field_pair();
    let (ca, cb) = (coll(&a), coll(&b));
    let id = a.insert(&ca, doc! { "_id": 1, "x": 0 }).unwrap();
    later();
    b.apply_batch(&everything(&a)).unwrap();
    assert!(a.delete(&ca, &id).unwrap());
    assert!(b.delete(&cb, &id).unwrap());
    later();
    a.apply_batch(&everything(&b)).unwrap();
    assert_ne!(
        state_of_the_tombstone(&a),
        state_of_the_tombstone(&b),
        "premise: the same absence over different states"
    );
    (a, b, dirs)
}

/// The state the sender's scoped page carries for its one tombstone.
fn state_of_the_tombstone(from: &Engine) -> MergeState {
    let page = from.snapshot_page(None, Some(orders())).unwrap();
    assert_eq!(page.deleted_documents.len(), 1, "premise: one tombstone");
    crate::field_write::load(page.deleted_documents[0].meta.as_deref()).unwrap()
}

#[test]
fn a_converge_pull_that_moves_only_a_tombstones_state_writes_it() {
    let (a, b, _d) = a_richer_and_a_poorer_tombstone();

    converge(&b, &a, VersionVector::new());

    assert_eq!(
        state_of_the_tombstone(&b),
        state_of_the_tombstone(&a),
        "the stored state is the richer member's"
    );
}

#[test]
fn a_page_of_a_whole_database_snapshot_that_is_not_the_last_and_moves_only_a_tombstones_state_writes_it()
 {
    let (a, b, _d) = a_richer_and_a_poorer_tombstone();
    let mut page = a.snapshot_page(None, None).unwrap();
    page.next = Some(crate::SnapshotCursor {
        collection: orders(),
        after_key: vec![0xff],
        until_key: None,
    });

    b.apply_snapshot_page(a.node_id(), &mut SnapshotProgress::whole_database(), &page).unwrap();

    assert_eq!(
        state_of_the_tombstone(&b),
        state_of_the_tombstone(&a),
        "the stored state is the richer member's"
    );
}

// ---- an entry the member covers but does not hold is kept by the single-entry apply (always keep) --------------

#[test]
fn a_single_entry_a_member_covers_but_does_not_hold_is_appended_to_its_oplog() {
    let (a, b, _d) = field_pair();
    let (ca, cb) = (coll(&a), coll(&b));
    let id = a.insert(&ca, doc! { "_id": 1, "x": 0 }).unwrap();
    later();
    a.replace(&ca, &id, doc! { "_id": 1, "x": 1 }, false).unwrap();
    let mut writes: Vec<_> =
        everything(&a).into_iter().filter(|entry| entry.doc_id.is_some()).collect();
    writes.sort_by_key(|entry| entry.stamp);
    assert_eq!(writes.len(), 2, "premise: an insert and a replace");
    assert!(b.apply_remote(&cb, &writes[1]).unwrap(), "premise: the later entry is applied");
    let held = |e: &Engine, stamp| {
        e.read_oplog_from(Hlc::ZERO, 10_000, WalkScope::Background)
            .unwrap()
            .iter()
            .any(|entry| entry.stamp == stamp)
    };
    assert!(!held(&b, writes[0].stamp), "premise: the earlier entry is not in the member's oplog");

    let applied = b.apply_remote(&cb, &writes[0]).unwrap();

    assert!(!applied, "the state already covers it, so no entry won");
    assert!(
        held(&b, writes[0].stamp),
        "an entry owed to the oplog is appended, and the single-entry apply commits what it appended"
    );
}
