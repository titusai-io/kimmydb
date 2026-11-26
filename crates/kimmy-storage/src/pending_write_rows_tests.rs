//! "The pending write" rows of unit P (Q6-build-order.md, hardproblems and the manager, Nov 25): a write above the floor of a collection the member cannot make yet is a WAIT, not a loss and not a
//! divergence. Compiling today: w1a and w1b (the retry of a fetch ends in exactly one of two states), w2 (serve only what was taken), w5 (behind, not different). w4 (no repair cooldown) is a unit test of
//! the repair planner and lives in `pending-unknownfetch-w4-tests.rs` (kimmy-cluster). w1c (the answer carries the N row and both floors) and w3 (N's life) need the names table N and are the `cfg(any())` blocks
//! at the end of this file: red-first, with their expectations; the API they name is an ASSUMPTION to adapt when unit N exists.
//! w1d and w1e (added Thu Nov 26, from P-plan.md section 8, the gone-collection case traced): a member that holds the DATABASE's tombstone and nothing else of the collection (no collection, no tombstone of its own, no options row), met by a write of it.
//! NOT COMMITTED: pending, with the other unit P tests.

use bson::doc;
use kimmy_core::{CollectionId, DocId, Hlc, MergePolicy, OpKind, OplogEntry, VersionVector};

use crate::{Engine, PeerWindow, SnapshotProgress, SyncOutcome, WalkScope};

const DB: &str = "app";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().unwrap();
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
    )
    .unwrap();
    outcome
}

fn id_of(name: &str) -> CollectionId {
    CollectionId::derive(DB, name)
}

/// The writes `m` minted into `name`, oldest first.
fn writes(m: &Engine, name: &str) -> Vec<OplogEntry> {
    everything(m)
        .into_iter()
        .filter(|e| {
            e.doc_id.is_some() && e.collection == id_of(name) && e.stamp.node == m.node_id()
        })
        .collect()
}

/// `author` makes `orders` and writes two documents (W1, then W2). Returns the author and its two writes.
fn author_with_two_writes() -> (Engine, tempfile::TempDir, OplogEntry, OplogEntry) {
    let (a, d) = engine();
    let orders = a.create_collection(DB, "orders").unwrap();
    a.insert(&orders, doc! { "_id": 1, "v": "first" }).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    a.insert(&orders, doc! { "_id": 2, "v": "second" }).unwrap();
    let w = writes(&a, "orders");
    assert_eq!(w.len(), 2, "premise: two writes");
    (a, d, w[0].clone(), w[1].clone())
}

/// w2, serve only what was taken: a member that cannot make the collection leaves a write untaken (`Step::Unknown`) and does not serve it; a third member pulling from it does not receive that entry.
#[test]
fn w2_a_member_that_left_a_write_untaken_does_not_serve_it() {
    let (_a, _da, w1, w2) = author_with_two_writes();
    let (x, _dx) = engine(); // holds nothing of orders: no collection, no tombstone
    let outcome = window(&x, &[w1.clone(), w2.clone()]);
    let mut red = Vec::new();
    if outcome.unknown_collection == 0 {
        red.push("the window was not stopped at the write (unknown_collection is 0): the premise of the row does not hold".to_string());
    }
    let served = everything(&x);
    for (name, w) in [("W1", &w1), ("W2", &w2)] {
        if served.iter().any(|e| e.stamp == w.stamp) {
            red.push(format!("x serves {name}, which it has not taken"));
        }
    }
    // a third member pulling from x
    let (y, _dy) = engine();
    window(&y, &served);
    if y.get_collection(DB, "orders").is_ok() {
        red.push(
            "a third member received the collection from a member that had not taken the write"
                .to_string(),
        );
    }
    assert!(red.is_empty(), "w2: {}", red.join("; "));
}

/// w5, behind and not different: the member stopped at the first write has taken nothing stamped after it from that window. Its state is the function of the entries it has taken (none of the author's
/// entries): no collection, no document.
#[test]
fn w5_a_member_stopped_at_a_write_has_taken_nothing_stamped_after_it_from_that_window() {
    let (_a, _da, w1, w2) = author_with_two_writes();
    let (x, _dx) = engine();
    let outcome = window(&x, &[w1.clone(), w2.clone()]);
    let mut red = Vec::new();
    if outcome.applied != 0 {
        red.push(format!(
            "the window applied {} entries past the write it could not take",
            outcome.applied
        ));
    }
    if x.get_collection(DB, "orders").is_ok() {
        red.push("the member made a collection from a write".to_string());
    }
    if everything(&x).iter().any(|e| e.stamp == w2.stamp) {
        red.push("the member took the entry stamped after the one it stopped at".to_string());
    }
    // `OpKind` is used for the premise that nothing but writes were offered
    assert!(w1.kind != OpKind::CreateCollection && w2.kind != OpKind::CreateCollection);
    assert!(red.is_empty(), "w5: {}", red.join("; "));
}

/// w1a, the fetch's answer from a sender that holds the collection STANDING: the retry ends with the collection made and the write applied (one of the two states the retry may end in).
#[test]
fn w1a_a_fetch_answered_by_a_sender_that_holds_the_collection_ends_with_it_made_and_the_write_applied()
 {
    let (a, _da, w1, w2) = author_with_two_writes();
    let (x, _dx) = engine();
    let first = window(&x, &[w1.clone(), w2.clone()]);
    assert!(first.unknown_collection > 0, "premise: x is stopped at the write");
    let identity = id_of("orders");
    let mut progress = SnapshotProgress::of_collection(identity);
    let page = a.snapshot_page(None, Some(identity)).unwrap();
    x.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    let retry = window(&x, &[w1, w2]);
    let held = match x.get_collection(DB, "orders") {
        Ok(c) => (true, x.count(&c, WalkScope::Background).unwrap()),
        Err(_) => (false, 0),
    };
    assert_eq!(
        (held, retry.unknown_collection),
        ((true, 2), 0),
        "w1a: after the fetch the member holds the collection with both writes and is no longer stopped"
    );
}

/// w1b, the fetch's answer from a sender that holds the collection GONE (dropped, only its tombstone): the retry ends with the write history: no collection, and the member is not stopped any more.
#[test]
fn w1b_a_fetch_answered_by_a_sender_that_holds_the_collection_gone_ends_with_the_write_history() {
    let (a, _da, w1, w2) = author_with_two_writes();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(a.drop_collection(DB, "orders").unwrap());
    let identity = id_of("orders");
    a.purge_dropped_collection(identity).unwrap();
    let (x, _dx) = engine();
    let first = window(&x, &[w1.clone(), w2.clone()]);
    assert!(first.unknown_collection > 0, "premise: x is stopped at the write");
    let mut progress = SnapshotProgress::of_collection(identity);
    let page = a.snapshot_page(None, Some(identity)).unwrap();
    x.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    let retry = window(&x, &[w1, w2]);
    let held = x.get_collection(DB, "orders").is_ok();
    assert_eq!(
        (held, retry.unknown_collection),
        (false, 0),
        "w1b: the writes are at or below the sender's drop: history; the member holds no collection and is not stopped"
    );
}

/// A member `d`'s drop of the database (D) and the entry that carries it: `d` makes another collection first, so the database exists for it to drop. Stamped after every entry the caller made before calling this.
fn the_databases_drop() -> (Engine, tempfile::TempDir, OplogEntry) {
    let (d, dd) = engine();
    std::thread::sleep(std::time::Duration::from_millis(5));
    d.create_collection(DB, "keep").unwrap();
    assert!(d.drop_database(DB).unwrap());
    let drop = everything(&d)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::DropCollection
                && e.collection == kimmy_core::database_drop_id(DB)
                && e.stamp.node == d.node_id()
        })
        .expect("the database's drop entry");
    (d, dd, drop)
}

/// A member that holds the database's tombstone D and nothing else of `orders` (no collection, no tombstone of its own, and so no options row): the case P-plan.md section 8 traces. A member that has taken only D.
fn holds_only_the_databases_tombstone(drop: &OplogEntry) -> (Engine, tempfile::TempDir) {
    let (m, dm) = engine();
    window(&m, std::slice::from_ref(drop));
    assert!(
        m.database_dropped_at(DB).unwrap().is_some(),
        "premise: the member holds the database's tombstone"
    );
    assert!(
        m.collection_dropped_at(id_of("orders")).unwrap().is_none(),
        "premise: and none of the collection's own"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: and no collection");
    (m, dm)
}

/// w1d, a write ABOVE the database's drop, the sender holding the collection STANDING: after ONE fetch the member makes the collection from the page's options row and applies the write. `orders` is a `field` collection, so the remake has to read the ROW (a remake that
/// took the default policy would make a `document` collection). The creation and the first document are below D, and do not come back.
/// RED today: the write meets a gone collection; the fetch's page refuses the creation (it is at or below D, `record_database_refusal`) and records the collection's tombstone at D; the write, above D, is then judged against that tombstone and found to be history (sync.rs:2234-2238): the
/// member ends with nothing. GREEN at P7 (the gone arm reads BOTH floors: a fact above the later of them, with an options row for the id and no swap history, makes the collection from the row, dated above the floor, and applies the fact; the same for the page route's documents).
/// Mutants (P7): the write above the floor is history after the fetch (today's); the remake takes the default policy and not the row's; the remake does not apply the write; a remade collection holds the creation's document that is below D.
/// This row also pins, end to end on the STANDING path, that the page the fetch answers with carries the options row (w1c pins the page itself, in the n-api file).
#[test]
fn w1d_a_write_above_the_databases_drop_makes_the_collection_from_the_fetched_row_after_one_fetch()
{
    let (a, _da) = engine();
    let orders = a.create_collection_with_policy(DB, "orders", MergePolicy::Field, None).unwrap();
    a.insert(&orders, doc! { "_id": 1, "v": "below the drop" }).unwrap();
    let (_d, _dd, drop) = the_databases_drop();
    std::thread::sleep(std::time::Duration::from_millis(5));
    // `a` has not heard of the drop: it writes again, stamped above D
    a.insert(&orders, doc! { "_id": 2, "v": "above the drop" }).unwrap();
    let above: Vec<OplogEntry> =
        writes(&a, "orders").into_iter().filter(|e| e.stamp.hlc > drop.stamp.hlc).collect();
    assert_eq!(above.len(), 1, "premise: one write stamped above the drop");

    let (m, _dm) = holds_only_the_databases_tombstone(&drop);
    let first = window(&m, &above);
    assert!(
        first.unknown_collection > 0,
        "premise: the member is stopped at the write (it holds no row to make the collection from)"
    );
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "premise: nothing is made by the stopped window"
    );

    // the fetch: a scoped snapshot of the one collection from the sender that holds it standing, then the retry
    let identity = id_of("orders");
    let mut progress = SnapshotProgress::of_collection(identity);
    let page = a.snapshot_page(None, Some(identity)).unwrap();
    m.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    let retry = window(&m, &above);

    let held = m.get_collection(DB, "orders");
    assert_eq!(
        (held.is_ok(), retry.unknown_collection),
        (true, 0),
        "w1d: after one fetch the write above the database's drop has made the collection, and the member is not stopped"
    );
    let held = held.unwrap();
    assert_eq!(
        held.merge_policy,
        MergePolicy::Field,
        "w1d: made from the row's options (field), not the default"
    );
    assert_eq!(
        m.count(&held, WalkScope::Background).unwrap(),
        1,
        "w1d: with the one document above the drop, and not the one below it"
    );
    assert!(
        m.get(&held, &DocId::Int64(2)).unwrap().is_some(),
        "w1d: the write above the drop is applied"
    );
    assert!(
        m.get(&held, &DocId::Int64(1)).unwrap().is_none(),
        "w1d: and the document below the drop does not come back"
    );
}

/// A drop of `orders` by `by`, stamped `wall_ms` by hand, and the drop of the whole database (hardproblems' correction below needs stamps the test chooses).
fn hand_drop_of(by: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: kimmy_core::Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        kind: OpKind::DropCollection,
        collection: id_of("orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&kimmy_core::CollectionRef::new(DB, "orders"))
                .expect("the drop encodes"),
        ),
        meta: None,
    }
}

fn hand_database_drop(by: &Engine, wall_ms: u64) -> OplogEntry {
    crate::engine::ddl_entry(
        kimmy_core::Stamp::new(Hlc::new(wall_ms, 0), by.node_id(), by.epoch()),
        OpKind::DropCollection,
        kimmy_core::database_drop_id(DB),
        &kimmy_core::CollectionRef::new(DB, kimmy_core::DATABASE_DROP_NAME),
    )
    .expect("the database drop encodes")
}

/// w1e (a), the CORRECTED row (hardproblems' P review, adopted 12:25 PM: "a document entry carries only the collection id, so a member with no row cannot read D"): a member that HOLDS the options row (and so knows the collection's database), whose own tombstone has been
/// collected and which keeps the database's tombstone D, judges a write at or below D history AT ONCE: the window is not stopped, so no fetch is planned, and no collection is made. The state is built as w1c's third leg builds it (row4's collector pass between the two drops).
/// RED today: the gone arm reads only the collection's OWN tombstone (sync.rs:2234), which the member no longer holds, so the window stops at the write (`unknown_collection` is not 0) and the transport plans a fetch (transport.rs:3583). GREEN at P7 (the gone arm reads BOTH floors).
/// Mutant (P7): the gone arm reads the collection's own tombstone only (the database legs). Not asserted: WHERE the floor is read (the gone arm, or `is_history`, ruling 5).
#[test]
fn w1e_a_member_holding_the_row_judges_a_write_at_or_below_the_databases_drop_history_with_no_fetch()
 {
    let (_a, _da, w1, w2) = author_with_two_writes();
    let (m, _dm) = engine();
    let life = m.create_collection(DB, "orders").expect("the member's own life");
    let t0 = life.created.wall_ms;
    let (x, _dx) = engine();
    let (own_drop, database_drop) =
        (hand_drop_of(&x, t0 + 10_000), hand_database_drop(&x, t0 + 20_000));
    window(&m, std::slice::from_ref(&own_drop));
    window(&m, std::slice::from_ref(&database_drop));
    m.finish_purges_now().expect("the purges finish");
    m.collect_garbage_at(
        own_drop.stamp.hlc.wall_ms + 1_000 + 5_000,
        crate::RetentionPolicy::new(31_536_000, 1),
    )
    .expect("the retention pass runs");
    assert_eq!(
        m.collection_dropped_at(id_of("orders")).unwrap(),
        None,
        "premise: the collection's own tombstone is collected"
    );
    assert_eq!(
        m.database_dropped_at(DB).unwrap(),
        Some(database_drop.stamp),
        "premise: the database's tombstone is kept"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "premise: the collection is gone");
    assert!(
        w1.stamp.hlc < database_drop.stamp.hlc && w2.stamp.hlc < database_drop.stamp.hlc,
        "premise: both writes are stamped below the database's drop"
    );
    let outcome = window(&m, &[w1.clone(), w2.clone()]);
    assert_eq!(
        outcome.unknown_collection, 0,
        "w1e (a): a member that holds the row judges a write below the database's drop history at once: the window is not stopped, so no fetch is planned"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "w1e (a): and no collection is made from it");
}

/// w1e (b), a member with NO row: it holds D only (no collection, no tombstone of its own, no options row), and a document entry carries only the collection id, so it cannot tell which database the write belongs to and CANNOT read D. The write STOPS the window (premise), the
/// fetch is planned, and after ONE fetch (a scoped page from the author, which holds the collection standing) the write is history: no collection is made and the member is not stopped. A GUARD: GREEN today (the page's refusal of the creation at or below D records the collection's
/// tombstone at D, and the write is below it) and at P7 (the row the page carries is c1's, below D: no remake). Mutant (P7): a remake whenever a row exists.
#[test]
fn w1e_a_member_with_no_row_judges_a_write_at_or_below_the_databases_drop_history_after_one_fetch()
{
    let (a, _da, w1, w2) = author_with_two_writes();
    let (_d, _dd, drop) = the_databases_drop();
    assert!(
        w1.stamp.hlc < drop.stamp.hlc && w2.stamp.hlc < drop.stamp.hlc,
        "premise: both writes are stamped below the database's drop"
    );
    let (m, _dm) = holds_only_the_databases_tombstone(&drop);
    let first = window(&m, &[w1.clone(), w2.clone()]);
    assert!(
        first.unknown_collection > 0,
        "premise: without a row the member cannot read D, so the write stops the window and a fetch is planned"
    );
    let identity = id_of("orders");
    let mut progress = SnapshotProgress::of_collection(identity);
    let page = a.snapshot_page(None, Some(identity)).unwrap();
    m.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    let retry = window(&m, &[w1, w2]);
    assert_eq!(
        (m.get_collection(DB, "orders").is_ok(), retry.unknown_collection),
        (false, 0),
        "w1e (b): after one fetch the write is history: no collection is made and the member is not stopped"
    );
}

/// w1f, the APPEND side of History (P-plan.md section 8, step 3: the coder's argument that no sender can serve a write it judged history): a member that holds the collection's own drop takes a write BELOW it. The write is judged history, the window is not stopped, and the write is NOT in
/// the member's served oplog (the drop it took is, which shows the oplog read works). EXPECTED: green at 30f3fc04 if the coder's read ("a plain collection's entry judged history is not appended") is right; it changes at NO P commit (P7 remakes only for a fact ABOVE the floors),
/// and must stay green through P7 and P8. If it is RED at the first run, that is a FINDING for P (a sender CAN serve a write it judged history, and section 8's third shape is reachable), not a test bug. Mutant (P7/P8): a fact judged history is appended.
#[test]
fn w1f_a_write_judged_history_is_not_served_onward_and_does_not_stop_the_window() {
    let (a, _da, w1, w2) = author_with_two_writes();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(a.drop_collection(DB, "orders").unwrap());
    let drop = everything(&a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::DropCollection
                && e.collection == id_of("orders")
                && e.stamp.node == a.node_id()
        })
        .expect("the collection's drop entry");
    assert!(
        w1.stamp.hlc < drop.stamp.hlc && w2.stamp.hlc < drop.stamp.hlc,
        "premise: both writes are below the drop"
    );
    let (m, _dm) = engine();
    window(&m, std::slice::from_ref(&drop));
    assert!(
        m.collection_dropped_at(id_of("orders")).unwrap().is_some(),
        "premise: the member holds the collection's own tombstone"
    );
    let outcome = window(&m, &[w1.clone(), w2.clone()]);
    assert_eq!(
        outcome.unknown_collection, 0,
        "w1f: a write below a tombstone the member holds is history, and the window is not stopped"
    );
    assert!(m.get_collection(DB, "orders").is_err(), "w1f: and no collection is made from it");
    let served = everything(&m);
    assert!(
        served.iter().any(|e| e.stamp == drop.stamp),
        "premise: the member serves the drop it took"
    );
    for (name, w) in [("W1", &w1), ("W2", &w2)] {
        assert!(
            !served.iter().any(|e| e.stamp == w.stamp),
            "w1f: the member serves {name}, a write it judged history: it holds an entry no other member can need"
        );
    }
}
