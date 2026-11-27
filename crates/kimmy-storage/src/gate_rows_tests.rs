//! The remake gate: a held STAGE with no register event must not decide whether a fact above the floor remakes a gone collection (the function table's rule: events only).
//!
//! The product's gate (`gone_plan` -> `register::is_swapped`, register.rs ~2301) is "a register event OR a held stage of the name"; the table's touch rule is events only. They differ when a stage was created ABOVE the identity's tombstone (the drop
//! was then a plain drop, not a logical one) and no binding has arrived: 11 of the table's 1,023 cases. Ruled: the gate is wrong; members that take the same facts in different orders differ (the probe at 7f48e27d showed it in the order D, sc, W).
//!
//! THE FACTS (`world(true)`): `orders` held with its first document (the prefix); a drop D by a member that took the prefix; a stage created at sc ABOVE D (no binding); a write W above D by a member that had not heard D. In every order the facts
//! meet a member that starts alike (it took the prefix and holds no stage), and the purger runs after each fact (a creation or a remake that meets rows not yet purged waits, and these tests do not re-offer: the probe's "D, W, sc" ended gone for that reason alone).
//! pq1a, pq1b, pq1c: the three orders W, D, sc / D, sc, W / D, W, sc all end STANDING with W: (true, 1, true) = `orders` stands with one document and the stage is held. Expected at 7f48e27d: pq1a GREEN (W meets the standing collection and D re-dates it,
//! the stage comes after); pq1b RED, (false, 0, true) (the gate sees the held stage at W and refuses the remake); pq1c GREEN by reading (W arrives before the stage, so the gate does not apply, and the stage created above D seeds nothing). Each records what
//! the member holds after EACH fact and prints it, so a red says at which fact the collection is lost. GREEN for pq1b when the gate reads register events only.
//! pq2 (a GUARD against an over-wide fix): the stage created BELOW D (`world(false)`): all six orders of {sc, D, W} end ABSENT with the stage held, (false, 0, true): the stage below the drop makes D a logical drop (or seeds one from the tombstone), the register
//! buries the identity, and a write above D changes nothing (the seed buries a remade life). Expected GREEN at 7f48e27d BY READING ONLY (the C2a seed, the logical-drop rule and the gate that now applies); a first run confirms it. It goes red if a fix makes
//! the remake ignore a register event.
//! pq3 (the page route): D, sc, then a page from a member that holds the life with W: the receiver ends standing with W, (true, 1, true). Expected RED at 7f48e27d, (false, 0, true): the page's remake of a document above the floor meets the same gate.
//!
//! Mutants: the gate reads "a held stage" again (pq1b, pq3 red); the remake ignores a register event (pq2 red).

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, DocId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};
use redb::ReadableDatabase;

use crate::{Engine, PeerWindow, SnapshotProgress, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
const STAGE: &str = "orders.__stg.g1";

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

fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn id_of(name: &str) -> CollectionId {
    CollectionId::derive(DB, name)
}

fn own(m: &Engine, kind: OpKind, name: &str) -> Vec<OplogEntry> {
    everything(m)
        .into_iter()
        .filter(|e| e.kind == kind && e.collection == id_of(name) && e.stamp.node == m.node_id())
        .collect()
}

/// What a member holds: whether `orders` stands, how many documents it holds, and whether the stage is held.
type Holds = (bool, u64, bool);

fn holds(m: &Engine) -> Holds {
    let stage = m.get_collection(DB, STAGE).is_ok();
    match m.get_collection(DB, "orders") {
        Ok(c) => (true, m.count(&c, WalkScope::Background).unwrap(), stage),
        Err(_) => (false, 0, stage),
    }
}

/// The facts of one world, and the members that made them (kept alive: `y` is the author of W, which has not heard D, and holds the life with W).
struct World {
    prefix: Vec<OplogEntry>,
    d: OplogEntry,
    sc: OplogEntry,
    w: OplogEntry,
    y: Engine,
    /// the member that made the drop: it holds D and has lost `orders` (what a page carrying D comes from)
    z: Engine,
    _dirs: Vec<tempfile::TempDir>,
}

/// `stage_above_the_drop`: the stage is created at sc ABOVE D (sc after D); otherwise BELOW it (sc before D). W is above D either way.
fn world(stage_above_the_drop: bool) -> World {
    let (y, dy) = engine();
    let orders = y.create_collection(DB, "orders").unwrap();
    y.insert(&orders, doc! { "_id": 1, "v": "old" }).unwrap();
    let prefix = everything(&y);
    let (x, dx) = engine();
    let (z, dz) = engine();
    window(&z, &prefix);
    let make_stage = || {
        x.create_stage(DB, STAGE, MergePolicy::Document, Hlc::new(physical_now_ms() + 60_000, 0))
            .expect("the stage");
        everything(&x)
            .into_iter()
            .find(|e| e.kind == OpKind::CreateCollection && e.collection == id_of(STAGE))
            .expect("the stage's creation")
    };
    let (sc, d) = if stage_above_the_drop {
        pause();
        assert!(z.drop_collection(DB, "orders").unwrap());
        let d = own(&z, OpKind::DropCollection, "orders").pop().expect("the drop");
        pause();
        (make_stage(), d)
    } else {
        pause();
        let sc = make_stage();
        pause();
        assert!(z.drop_collection(DB, "orders").unwrap());
        (sc, own(&z, OpKind::DropCollection, "orders").pop().expect("the drop"))
    };
    pause();
    // `y` has not heard of the drop: it writes again, stamped above D
    y.insert(&orders, doc! { "_id": 2, "v": "above the drop" }).unwrap();
    let w = everything(&y)
        .into_iter()
        .find(|e| e.doc_id == Some(DocId::Int64(2)) && e.collection == id_of("orders"))
        .expect("the write");
    if stage_above_the_drop {
        assert!(d.stamp.hlc < sc.stamp.hlc && sc.stamp.hlc < w.stamp.hlc, "premise: D < sc < W");
    } else {
        assert!(sc.stamp.hlc < d.stamp.hlc && d.stamp.hlc < w.stamp.hlc, "premise: sc < D < W");
    }
    World { prefix, d, sc, w, y, z, _dirs: vec![dy, dx, dz] }
}

/// A fresh member that took the prefix (it holds `orders` with its first document and no stage), then the facts in `order`, the purger run after each. Returns what it holds at the end and after each fact.
fn run(world: &World, order: &[&OplogEntry]) -> (Holds, Vec<Holds>) {
    let (m, _dm) = engine();
    for entry in &world.prefix {
        window(&m, std::slice::from_ref(entry));
    }
    assert_eq!(
        holds(&m),
        (true, 1, false),
        "premise: the member holds orders and no stage before the facts"
    );
    let mut trace = Vec::new();
    for entry in order {
        window(&m, std::slice::from_ref(entry));
        m.finish_purges_now().unwrap();
        trace.push(holds(&m));
    }
    (holds(&m), trace)
}

fn label(world: &World, entry: &OplogEntry) -> &'static str {
    if entry.stamp == world.d.stamp {
        "D"
    } else if entry.stamp == world.sc.stamp {
        "sc"
    } else {
        "W"
    }
}

/// Every order of the three facts ends standing with W: `orders` stands, one document (W), the stage held.
fn ends_standing_with_w(world: &World, order: [&OplogEntry; 3], what: &str) {
    let (end, trace) = run(world, &order);
    assert_eq!(
        end,
        (true, 1, true),
        "{what}: orders stands with W and the stage is held; after each fact {:?}: {:?}",
        order.iter().map(|e| label(world, e)).collect::<Vec<_>>(),
        trace
    );
}

/// pq1a: W, D, sc. W meets the standing collection, D re-dates its life (W is kept), the stage arrives after. Expected GREEN at 7f48e27d.
#[test]
fn pq1a_w_then_d_then_a_stage_above_the_drop_ends_standing_with_w() {
    let w = world(true);
    ends_standing_with_w(&w, [&w.w, &w.d, &w.sc], "pq1a (W, D, sc)");
}

/// pq1b: D, sc, W. The stage is held when W arrives. RED at 7f48e27d, (false, 0, true): the gate sees the held stage and refuses the remake. GREEN when the gate reads register events only.
#[test]
fn pq1b_d_then_a_stage_above_the_drop_then_w_ends_standing_with_w() {
    let w = world(true);
    ends_standing_with_w(&w, [&w.d, &w.sc, &w.w], "pq1b (D, sc, W)");
}

/// pq1c: D, W, sc, the purger run after D (and after every fact). W arrives before the stage, so the gate does not apply, and a stage created above D seeds nothing. Expected GREEN at 7f48e27d.
#[test]
fn pq1c_d_then_w_then_a_stage_above_the_drop_ends_standing_with_w() {
    let w = world(true);
    ends_standing_with_w(&w, [&w.d, &w.w, &w.sc], "pq1c (D, W, sc)");
}

/// pq2, a GUARD against an over-wide fix: the stage is created BELOW D, and every one of the six orders of {sc, D, W} ends ABSENT with the stage held, (false, 0, true). The stage below the drop makes D a logical drop (or seeds one from the tombstone), the
/// register buries the identity, and a write above D changes nothing. Expected GREEN at 7f48e27d by reading only (a first run confirms it). Mutant: the remake ignores a register event.
#[test]
fn pq2_a_stage_created_below_the_drop_ends_absent_in_every_order() {
    let w = world(false);
    let facts = [&w.sc, &w.d, &w.w];
    let orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
    let mut red = Vec::new();
    for order in orders {
        let order: Vec<&OplogEntry> = order.iter().map(|&i| facts[i]).collect();
        let (end, trace) = run(&w, &order);
        if end != (false, 0, true) {
            red.push(format!(
                "{:?} ends {end:?}, after each fact {trace:?}",
                order.iter().map(|e| label(&w, e)).collect::<Vec<_>>()
            ));
        }
    }
    assert!(
        red.is_empty(),
        "pq2: a stage created below the drop ends absent in every order; {} of 6 do not:\n{}",
        red.len(),
        red.join("\n")
    );
}

/// pq3, the PAGE route: the receiver took the prefix, D and then the stage above D (it holds the stage and has lost the collection), and then pulls a whole-database snapshot from `y`, a member that has not heard D and holds the life with W. The page's
/// documents above the floor remake the collection, and the receiver ends standing with W, (true, 1, true). Expected RED at 7f48e27d, (false, 0, true): the page's remake meets the same gate (a held stage).
#[test]
fn pq3_a_page_from_a_member_that_holds_the_life_with_w_makes_the_collection_stand_after_d_and_a_stage()
 {
    let w = world(true);
    let (r, _dr) = engine();
    for entry in &w.prefix {
        window(&r, std::slice::from_ref(entry));
    }
    for entry in [&w.d, &w.sc] {
        window(&r, std::slice::from_ref(entry));
        r.finish_purges_now().unwrap();
    }
    assert_eq!(
        holds(&r),
        (false, 0, true),
        "premise: the receiver has lost the collection and holds the stage"
    );
    let mut progress = SnapshotProgress::whole_database();
    for _ in 0..50 {
        if progress.is_complete() {
            break;
        }
        let page = w.y.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        r.apply_snapshot_page(w.y.node_id(), &mut progress, &page).unwrap();
        r.finish_purges_now().unwrap();
    }
    assert!(progress.is_complete(), "premise: the pull completed");
    assert_eq!(
        holds(&r),
        (true, 1, true),
        "pq3: after the page orders stands with W and the stage is held"
    );
}

/// A whole-database pull of `from` into `to`, the purger run after each page.
fn pull(to: &Engine, from: &Engine) {
    let mut progress = SnapshotProgress::whole_database();
    for _ in 0..50 {
        if progress.is_complete() {
            break;
        }
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        to.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
        to.finish_purges_now().unwrap();
    }
    assert!(progress.is_complete(), "premise: the pull completed");
}

/// pq4 (the purger's completion, `remake_is_wanted` at remake.rs:155): a member took D and a stage created ABOVE D (no binding: no register event), and then a later
/// creation of `orders` above D, which waits for the drop's rows still filed; nothing offers it again. The purge completes: the collection is made from the row
/// (empty: the dropped rows are gone), and the stage is still held. RED at 7f48e27d, (false, 0, true): the completion's gate reads the held stage as a swap.
/// Mutants: the plan's gate reads "a held stage" again.
#[test]
fn pq4_a_purge_that_completes_makes_the_collection_beside_a_held_stage_with_no_event() {
    let w = world(true);
    let (m, _dm) = engine();
    for entry in &w.prefix {
        window(&m, std::slice::from_ref(entry));
    }
    window(&m, std::slice::from_ref(&w.d));
    assert!(
        m.rows_under(id_of("orders")).unwrap() > 0,
        "premise: the dropped life's rows are not purged"
    );
    window(&m, std::slice::from_ref(&w.sc));
    pause();
    let (y2, _dy2) = engine();
    y2.create_collection(DB, "orders").unwrap();
    let c = everything(&y2)
        .into_iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == id_of("orders"))
        .expect("the later creation");
    assert!(w.d.stamp.hlc < c.stamp.hlc, "premise: the creation is above the drop");
    window(&m, std::slice::from_ref(&c));
    assert!(
        m.get_collection(DB, "orders").is_err(),
        "premise: no collection is made over the rows"
    );
    m.purge_dropped_collection(id_of("orders")).unwrap();
    assert_eq!(
        holds(&m),
        (true, 0, true),
        "pq4: the purge's completion made the collection from the row, and the stage is held"
    );
}

/// pq5 (a page waits for a purge, snapshot.rs:2067): a member took D and a stage created above D, and the drop's rows are still filed. A page from `y`, which holds the
/// life with W, defines `orders`: the name is plain (no register event), so the page waits whole for the purge, as for any plain name, and `progress` does not move.
/// RED at 7f48e27d: the held stage reads as a swap, the wait is skipped and nothing waits. Mutant: the plain branch of the wait reads "a held stage" again.
#[test]
fn pq5_a_page_waits_for_the_purge_of_a_name_that_holds_a_stage_and_no_event() {
    let w = world(true);
    let (m, _dm) = engine();
    for entry in &w.prefix {
        window(&m, std::slice::from_ref(entry));
    }
    window(&m, std::slice::from_ref(&w.d));
    window(&m, std::slice::from_ref(&w.sc));
    assert!(
        m.rows_under(id_of("orders")).unwrap() > 0,
        "premise: the dropped life's rows are not purged"
    );
    let mut progress = SnapshotProgress::whole_database();
    let page = w.y.snapshot_page(None, None).unwrap();
    let applied = m.apply_snapshot_page(w.y.node_id(), &mut progress, &page).unwrap();
    assert_eq!(
        applied.purge_pending,
        Some(id_of("orders")),
        "pq5: the page waits for the purge of `orders`, whole"
    );
    assert_eq!(progress.pages(), 0, "and the progress did not move");
}

/// pq6 (the carried drop, snapshot.rs:3720): a member holds `orders` (the old life) and a stage created ABOVE D, and no event. A whole-database page from `z`, which
/// holds D, carries the drop of `orders`. It is a plain drop (the stage is above it), so it is applied and the old life ends, as it does for a member that took D as an
/// ENTRY: both end (false, 0, true), and the deferral counter reads 0. RED at 7f48e27d: `carried_burial_is_the_registers` reads the held stage as a swap, the burial is
/// deferred, and the page's member keeps the old life (true, 1, true). Mutant: the predicate reads "a held stage" again.
#[test]
fn pq6_a_carried_drop_of_a_name_that_holds_a_stage_above_it_ends_as_the_entry_does() {
    let w = world(true);
    let (m, _dm) = engine();
    for entry in &w.prefix {
        window(&m, std::slice::from_ref(entry));
    }
    window(&m, std::slice::from_ref(&w.sc));
    assert_eq!(holds(&m), (true, 1, true), "premise: the old life stands and the stage is held");
    pull(&m, &w.z);
    let (by_entries, _) = run(&w, &[&w.sc, &w.d]);
    assert_eq!(
        by_entries,
        (false, 0, true),
        "premise: the member that took D as an entry has lost the old life"
    );
    assert_eq!(
        holds(&m),
        by_entries,
        "pq6: the member that took D on a page ends as the one that took it as an entry"
    );
    assert_eq!(m.register_carried_drops_deferred(), 0, "and no carried drop was deferred");
}

/// pq6b, a GUARD for the precise rule at 3720: the stage is created BELOW D, so the drop is a logical drop and the register's: the page's burial is deferred (counted),
/// and the old life stays. GREEN at 7f48e27d and after P14. NOT a claim that it converges: a member that took D as an entry ends differently, and F2 (the receiver runs the
/// function on a carried drop) replaces this deferral. Mutant: the predicate ignores a stage older than the drop.
#[test]
fn pq6b_a_carried_drop_of_a_name_that_holds_a_stage_below_it_is_the_registers_to_judge() {
    let w = world(false);
    let (m, _dm) = engine();
    for entry in &w.prefix {
        window(&m, std::slice::from_ref(entry));
    }
    window(&m, std::slice::from_ref(&w.sc));
    assert_eq!(holds(&m), (true, 1, true), "premise: the old life stands and the stage is held");
    pull(&m, &w.z);
    assert!(
        m.register_carried_drops_deferred() >= 1,
        "pq6b: the burial of a name with a stage below the drop is deferred to the register"
    );
    assert_eq!(holds(&m), (true, 1, true), "and the old life stands");
}

// ---- pq7: a binding known AHEAD of its position stops the remake only until it is released --------------------------------------------------------------

/// A binding of the stage to `orders`, by the stage's creator (the origin of `sc`), stamped one millisecond after the creation: valid (the creator's, inside the stage's deadline).
fn binding_of_the_stage(sc: &OplogEntry) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(sc.stamp.hlc.wall_ms + 1, 0), sc.stamp.node, sc.stamp.epoch),
        kind: OpKind::BindCollection,
        collection: id_of("orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: STAGE.into(),
            })
            .expect("the binding encodes"),
        ),
        meta: None,
    }
}

/// The register events of `orders` that are in effect, and those known ahead of their position (information, held in REGISTER_INFO).
fn events_of_orders(m: &Engine) -> (usize, usize) {
    let txn = m.db().begin_read().unwrap();
    let in_effect = crate::register::timeline_read(&txn, DB, "orders").unwrap().events().len();
    let parked = crate::register::parked_read(&txn, DB, "orders").unwrap().events().len();
    (in_effect, parked)
}

/// A fresh member that took the prefix, then the steps in `order`: "sc" and "B" through `apply_batch` (the stage's creation, never parked; the binding with no window behind it, so information, as rule_b_tests makes it), "D" and "W" in a window
/// (the purger run after D). Returns the member (and its directory) with what it held after each step.
fn member_with_a_parked_binding(
    world: &World,
    binding: &OplogEntry,
    order: &[&str],
) -> (Engine, tempfile::TempDir, Vec<Holds>) {
    let (m, dm) = engine();
    for entry in &world.prefix {
        window(&m, std::slice::from_ref(entry));
    }
    let mut trace = Vec::new();
    for step in order {
        match *step {
            "sc" => {
                m.apply_batch(std::slice::from_ref(&world.sc)).unwrap();
            }
            "B" => {
                m.apply_batch(std::slice::from_ref(binding)).unwrap();
            }
            "D" => {
                window(&m, std::slice::from_ref(&world.d));
                m.finish_purges_now().unwrap();
            }
            "W" => {
                window(&m, std::slice::from_ref(&world.w));
            }
            other => panic!("no step {other}"),
        }
        trace.push(holds(&m));
    }
    (m, dm, trace)
}

/// pq7: a binding known AHEAD of its position (parked: information, held out of position, not in effect) still makes the name swapped for the remake, but ONLY UNTIL IT IS RELEASED. The stage is created above D, so D is a plain drop; the name's only register fact is the
/// parked binding. Member A takes sc, B (parked), D, W: W must NOT remake the identity while B is parked, so A holds no `orders` (the stage is held). Member Bm takes sc, W, B (parked), D: W met the standing collection first, so Bm keeps `orders` with W until the
/// release (its state is recorded, not asserted). The release: B arrives in a window and takes effect; the visible binding buries the identity and the stage is served, and BOTH end alike, `orders` absent and the stage served, (false, 0, true).
/// Pins has_events' "ahead of its position" half. Expected GREEN at 7f48e27d (is_swapped covers events today, and a held stage as well); after P14's gate reads register events only it is the parked binding alone that blocks W. Mutant: `has_events` reads REGISTER only (A then
/// remakes at W while B is parked: red after the gate fix; at 7f48e27d the held stage still blocks it).
#[test]
fn pq7_a_binding_known_ahead_of_its_position_stops_the_remake_until_it_is_released() {
    let w = world(true);
    let b = binding_of_the_stage(&w.sc);
    assert!(
        w.d.stamp.hlc < w.sc.stamp.hlc && b.stamp.hlc < w.w.stamp.hlc,
        "premise: D < sc < B < W (the stage is above the drop)"
    );
    let (a, _da, a_trace) = member_with_a_parked_binding(&w, &b, &["sc", "B", "D", "W"]);
    let (bm, _dbm, bm_trace) = member_with_a_parked_binding(&w, &b, &["sc", "W", "B", "D"]);
    for (who, m) in [("A", &a), ("Bm", &bm)] {
        assert_eq!(
            events_of_orders(m),
            (0, 1),
            "premise: {who}'s only register fact is the binding known ahead of its position"
        );
    }
    assert_eq!(
        holds(&a),
        (false, 0, true),
        "pq7: A (sc, B parked, D, W): W does not remake the identity while the binding is parked; after each step {a_trace:?}"
    );
    // the release: the same binding arrives in a window and takes effect in the transaction that releases its hold
    for m in [&a, &bm] {
        window(m, std::slice::from_ref(&b));
    }
    // What each member's register holds after the release is NOT asserted exactly. A holds two events in effect (the binding and the LogicalDrop at D that the binding SEEDS over the gone identity's tombstone: `seed_from_the_tombstone`,
    // "a binding seeds whatever the stage's stamp", F3); Bm holds one (its identity stands as the drop's remains and the stage is above the drop, so nothing is seeded). Whether the two registers should then hold the same events is F1's
    // question, not this row's: here the binding must be in effect and nothing may be left parked.
    let after: Vec<(&str, (usize, usize))> =
        [("A", &a), ("Bm", &bm)].iter().map(|(who, m)| (*who, events_of_orders(m))).collect();
    for (who, (in_effect, parked)) in &after {
        assert!(
            *in_effect >= 1 && *parked == 0,
            "premise: {who}'s binding took effect when it was released (events in effect, parked): {after:?}"
        );
    }
    let served =
        |m: &Engine| m.resolve(DB, "orders").map(|r| r.physical).map_err(|e| e.to_string());
    assert_eq!(
        (holds(&a), served(&a)),
        (holds(&bm), served(&bm)),
        "pq7: after the release both end alike (A after {a_trace:?}, Bm after {bm_trace:?})"
    );
    assert_eq!(
        holds(&a),
        (false, 0, true),
        "pq7: and the visible binding has buried the identity on both: orders absent, the stage held"
    );
    assert_eq!(served(&a), Ok(STAGE.to_string()), "pq7: and the stage is served");
}

/// pq8 (the shared rule of a drop, `register::a_drop_is_logical`): the name's only register fact is a binding known AHEAD of its position, of a stage created ABOVE D. A drop that a
/// whole-database page carries (from `z`, which holds D) is a plain drop for such a name, as it is for a replicated drop entry (no event in effect, and no stage below the drop: the
/// parked binding's stage is above it), so the old life ends and the page's member ends as the member that took D as an entry: both (false, 0, true), and no carried drop was deferred.
/// RED at 7f48e27d: the parked event makes the name swapped, the burial is deferred and the page's member keeps the old life. Mutant: the carried drop's predicate reads "swapped" again.
#[test]
fn pq8_a_carried_drop_of_a_name_with_a_binding_ahead_of_its_position_ends_as_the_entry_does() {
    let w = world(true);
    let b = binding_of_the_stage(&w.sc);
    let (m, _dm, m_trace) = member_with_a_parked_binding(&w, &b, &["sc", "B"]);
    assert_eq!(events_of_orders(&m), (0, 1), "premise: the binding is known ahead of its position");
    assert_eq!(holds(&m), (true, 1, true), "premise: the old life stands and the stage is held");
    pull(&m, &w.z);
    let (by_entries, _dn, e_trace) = member_with_a_parked_binding(&w, &b, &["sc", "B", "D"]);
    assert_eq!(
        holds(&by_entries),
        (false, 0, true),
        "premise: the member that took D as an entry has lost the old life; after each step {e_trace:?}"
    );
    assert_eq!(
        holds(&m),
        holds(&by_entries),
        "pq8: the member that took D on a page ends as the one that took it as an entry (after {m_trace:?})"
    );
    assert_eq!(m.register_carried_drops_deferred(), 0, "and no carried drop was deferred");
}
