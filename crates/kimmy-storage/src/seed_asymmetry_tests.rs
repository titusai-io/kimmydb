//! The seed of a drop, on members that meet the same entries in different orders (ADR-272, decision 6): the shapes a model of the rule
//! found divergent, and that the rule as ruled ends equal on every member.
//!
//! **Probe 1**: M drops `orders` (ordinary: no swap is known), A creates a stage of `orders` ABOVE the drop, and S, which holds `orders`,
//! meets the stage and then the drop: ordinary there too (the stage is above it). S's client makes `orders` again, and M meets the stage
//! first. A stage created above a drop is one the drop never concerned, so M seeds no drop for it, and the life S made is served on M as
//! it is on S.
//!
//! **Probe 2/3**: the stage is created BELOW the drop. M drops (ordinary: M does not know the stage), T meets the drop (ordinary) and makes
//! `orders` again, and then M meets the stage (it seeds the drop: the swap began below it) while T, whose identity stands as a later life,
//! meets the stage last. The swap began below the drop, so the drop is the swap's on every member: T's life is buried as M's would be, and
//! the two registers hold the same drop. The life T made is lost (Chris, 2026-11-23: lost and reported), by the seed, which counts it.

use kimmy_core::{
    CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, RegisterEvent, Stamp, VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
const STAGE1: &str = "orders.__stg.a1";

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

fn with_orders() -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    m.create_collection(DB, "orders").unwrap();
    (m, d)
}

/// The creation of the stage `physical`, as its author `a` logged it.
fn creation_of(a: &Engine, physical: &str) -> OplogEntry {
    everything(a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, physical)
        })
        .unwrap()
}

/// A stage of `orders` created now by a member of its own, with a deadline far above any test.
fn a_stage(a: &Engine) -> OplogEntry {
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    a.create_stage(DB, STAGE1, MergePolicy::Document, deadline).unwrap();
    creation_of(a, STAGE1)
}

/// The drop of the identity `orders` that `m` logged for its client's drop.
fn the_drop_of_orders(m: &Engine) -> OplogEntry {
    everything(m)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::DropCollection && e.collection == CollectionId::derive(DB, "orders")
        })
        .expect("premise: the member logged a drop of orders")
}

/// The ordinary creation of the identity `orders` that `m` logged after `drop`.
fn the_creation_of_orders_after(m: &Engine, drop: &OplogEntry) -> OplogEntry {
    everything(m)
        .into_iter()
        .rfind(|e| {
            e.kind == OpKind::CreateCollection
                && e.collection == CollectionId::derive(DB, "orders")
                && e.stamp.hlc > drop.stamp.hlc
        })
        .expect("premise: the member logged a creation of orders after the drop")
}

fn the_events_of_orders(m: &Engine) -> Vec<(Stamp, RegisterEvent)> {
    use redb::ReadableDatabase;
    let txn = m.db().begin_read().unwrap();
    crate::register::timeline_read(&txn, DB, "orders").unwrap().events().to_vec()
}

/// The stamps of the register events of `orders` on `m`, each of which must be a logical drop.
fn the_logical_drops_of_orders(m: &Engine) -> Vec<Stamp> {
    the_events_of_orders(m)
        .into_iter()
        .map(|(stamp, event)| {
            assert!(
                matches!(event, RegisterEvent::LogicalDrop),
                "premise: the only events these members hold are drops"
            );
            stamp
        })
        .collect()
}

/// What the name answers on `m`, as a value an assertion can compare.
fn served_by(m: &Engine) -> Result<String, String> {
    m.resolve(DB, "orders").map(|resolved| resolved.physical).map_err(|e| e.to_string())
}

/// What `e` has counted as standing lives the register buried by the route `by` (`seed` or `drop`).
pub(crate) fn life_burials(e: &Engine, by: &str) -> u64 {
    e.register_life_burials()
        .into_iter()
        .find_map(|(label, n)| (label == by).then_some(n))
        .expect("the route is a label of the count")
}

/// What `e` holds: every collection, hidden ones included, as (database, name, id), in order. Two members that agree on what the name
/// answers must also agree on this, or anti-entropy keeps planning repairs between them.
fn held(e: &Engine) -> Vec<(String, String, u64)> {
    let mut held: Vec<_> = e
        .all_collections()
        .unwrap()
        .into_iter()
        .map(|meta| (meta.db, meta.name, meta.id.0))
        .collect();
    held.sort();
    held
}

fn a_pause() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

/// How two members that met the same entries in different orders end.
struct Ends {
    drop: OplogEntry,
    /// M, which dropped `orders` and met the stage after the life was made again.
    m_served: Result<String, String>,
    m_drops: Vec<Stamp>,
    /// The member that made `orders` again (S in probe 1, T in probe 2).
    other_served: Result<String, String>,
    other_drops: Vec<Stamp>,
    /// The standing lives the seed buried on each (`life_burials(_, "seed")`).
    m_burials: u64,
    other_burials: u64,
    /// What each holds (`held`).
    m_held: Vec<(String, String, u64)>,
    other_held: Vec<(String, String, u64)>,
}

/// Probe 1: the drop, then a stage above it, then a life made after it.
fn drop_first_stage_above_then_a_create() -> Ends {
    let (s, _ds) = with_orders();
    let (m, _dm) = engine();
    window(&m, &everything(&s));
    assert!(m.get_collection(DB, "orders").is_ok(), "premise: M follows S");
    assert!(m.drop_collection(DB, "orders").unwrap(), "premise: M's client drops orders");
    let drop = the_drop_of_orders(&m);
    assert!(the_events_of_orders(&m).is_empty(), "premise: M's drop is ordinary, no event");
    a_pause();
    let (a, _da) = engine();
    let creation = a_stage(&a);
    assert!(creation.stamp.hlc > drop.stamp.hlc, "premise: the stage is created above the drop");
    // M meets the creation after its drop; S meets it first and then the drop
    window(&m, std::slice::from_ref(&creation));
    window(&s, std::slice::from_ref(&creation));
    window(&s, std::slice::from_ref(&drop));
    assert!(
        the_events_of_orders(&s).is_empty(),
        "premise: S judged the drop ordinary: the stage is above it"
    );
    assert!(s.get_collection(DB, "orders").is_err(), "premise: the drop buried S's life");
    // S's client makes the name again (the dropped life's rows go first, on both)
    let identity = CollectionId::derive(DB, "orders");
    s.purge_dropped_collection(identity).expect("S's purge runs");
    m.purge_dropped_collection(identity).expect("M's purge runs");
    s.create_collection(DB, "orders").unwrap();
    assert_eq!(served_by(&s), Ok("orders".to_string()), "premise: S serves the life it made");
    let create = the_creation_of_orders_after(&s, &drop);
    window(&m, std::slice::from_ref(&create));
    Ends {
        m_served: served_by(&m),
        m_drops: the_logical_drops_of_orders(&m),
        other_served: served_by(&s),
        other_drops: the_logical_drops_of_orders(&s),
        m_burials: life_burials(&m, "seed"),
        other_burials: life_burials(&s, "seed"),
        m_held: held(&m),
        other_held: held(&s),
        drop,
    }
}

/// Probe 2/3: a stage below the drop, the drop met by a member that does not know the stage, a life made after it, and then the stage.
fn stage_below_then_a_drop_and_a_create() -> Ends {
    let (m, _dm) = with_orders();
    a_pause();
    let (a, _da) = engine();
    let creation = a_stage(&a);
    a_pause();
    assert!(m.drop_collection(DB, "orders").unwrap(), "premise: M's client drops orders");
    let drop = the_drop_of_orders(&m);
    assert!(creation.stamp.hlc < drop.stamp.hlc, "premise: the stage is created below the drop");
    assert!(the_events_of_orders(&m).is_empty(), "premise: M's drop is ordinary, no event");
    // T meets the drop (ordinary: it does not know the stage) and makes the name again
    let (t, _dt) = engine();
    window(&t, &everything(&m));
    assert!(t.get_collection(DB, "orders").is_err(), "premise: the drop buried T's life");
    assert!(the_events_of_orders(&t).is_empty(), "premise: T judged the drop ordinary");
    let identity = CollectionId::derive(DB, "orders");
    t.purge_dropped_collection(identity).expect("T's purge runs");
    m.purge_dropped_collection(identity).expect("M's purge runs");
    t.create_collection(DB, "orders").unwrap();
    assert_eq!(served_by(&t), Ok("orders".to_string()), "premise: T serves the life it made");
    let create = the_creation_of_orders_after(&t, &drop);
    // M meets the stage (it knows the swap began below its drop) and then T's creation; T meets the stage last
    window(&m, std::slice::from_ref(&creation));
    assert_eq!(
        the_logical_drops_of_orders(&m),
        vec![drop.stamp],
        "premise: M seeded the drop when it met the stage created below it"
    );
    window(&m, std::slice::from_ref(&create));
    window(&t, std::slice::from_ref(&creation));
    Ends {
        m_served: served_by(&m),
        m_drops: the_logical_drops_of_orders(&m),
        other_served: served_by(&t),
        other_drops: the_logical_drops_of_orders(&t),
        m_burials: life_burials(&m, "seed"),
        other_burials: life_burials(&t, "seed"),
        m_held: held(&m),
        other_held: held(&t),
        drop,
    }
}

#[test]
fn a_stage_above_a_drop_does_not_hide_the_life_made_after_it_on_the_member_that_dropped() {
    let e = drop_first_stage_above_then_a_create();
    assert_eq!(e.m_served, Ok("orders".to_string()), "M serves the life S made");
    assert_eq!(e.other_served, Ok("orders".to_string()), "S serves it");
    assert!(e.m_drops.is_empty(), "no drop was seeded: the stage began above it");
    assert!(e.other_drops.is_empty(), "and S holds none");
    assert_eq!((e.m_burials, e.other_burials), (0, 0), "and no life was buried by a seed");
    assert_eq!(e.m_held, e.other_held, "and M and S hold the same collections, with the same ids");
}

#[test]
fn a_stage_below_a_drop_ends_the_life_made_after_it_on_every_member() {
    let e = stage_below_then_a_drop_and_a_create();
    assert!(e.m_served.is_err(), "M reads the name as not found: {:?}", e.m_served);
    assert!(
        e.other_served.is_err(),
        "and so does T, whose later life the drop ends: {:?}",
        e.other_served
    );
    assert_eq!(e.m_drops, vec![e.drop.stamp], "M holds the drop");
    assert_eq!(e.other_drops, vec![e.drop.stamp], "and T holds the same drop");
    // What each holds differs by one collection, and it is accepted for 0.62 (ruled): T's life was buried by its seed, so T holds none; M met
    // the stage first, and T's creation of `orders` arrived after M's seed put the drop in effect, so M holds an empty identity that its name
    // answers NOT FOUND for (the register's drop hides it, and nothing settles the name after a creation of the identity). Both answer NOT
    // FOUND; apart from that collection the two hold the same, with the same ids. Anti-entropy does not heal it, and the check found how it
    // behaves: T, the member without the collection, reads `divergent_collections` 1 on every check from its second tick and plans one count
    // repair per cooldown (`REPAIR_COOLDOWN_ROUNDS`, 60 contacts) that never clears it; M reports nothing; nothing is client-visible. It is
    // fixed with the database-drop unit after 0.62.
    assert!(
        e.m_held.iter().any(|(_, name, _)| name == "orders"),
        "M holds the hidden, empty identity: {:?}",
        e.m_held
    );
    assert!(
        !e.other_held.iter().any(|(_, name, _)| name == "orders"),
        "T holds none: {:?}",
        e.other_held
    );
    let apart: Vec<_> = e.m_held.iter().filter(|(_, name, _)| name != "orders").cloned().collect();
    assert_eq!(apart, e.other_held, "and apart from it M and T hold the same, with the same ids");
}

#[test]
fn the_life_a_seed_buries_is_counted_on_the_member_that_made_it_and_only_there() {
    let e = stage_below_then_a_drop_and_a_create();
    assert!(
        e.other_served.is_err(),
        "premise: the seed buried the life T made: {:?}",
        e.other_served
    );
    assert_eq!(e.other_burials, 1, "T counts the life its seed buried");
    assert_eq!(
        e.m_burials, 0,
        "M's identity was absent when its seed was made: no life was buried"
    );
}

/// What each member answers for the name, and what it holds (`held`), in the order A, B, L.
type MemberAnswers = Vec<(Result<String, String>, Vec<(String, String, u64)>)>;

/// Three members and the entries each would send: A made a stage of `orders`; B, which held `orders` and did not know the stage, dropped
/// the name after it; and L, which had heard neither, dropped the DATABASE after that and made the name again. `meets[m]` is the order in
/// which member `m` (A, B, L) meets the other two, and then every member meets every other once more, as anti-entropy would. What each
/// answers for the name, and what each holds, in the order A, B, L.
fn three_members_that_meet_in(meets: [[usize; 2]; 3]) -> MemberAnswers {
    let (b, _db) = with_orders();
    let (a, _da) = engine();
    let (l, _dl) = engine();
    window(&a, &everything(&b));
    window(&l, &everything(&b));
    assert!(
        a.get_collection(DB, "orders").is_ok() && l.get_collection(DB, "orders").is_ok(),
        "premise: A and L follow B"
    );
    a_pause();
    let creation = a_stage(&a);
    a_pause();
    assert!(
        b.drop_collection(DB, "orders").unwrap(),
        "premise: B's client drops orders, not knowing the stage"
    );
    let drop = the_drop_of_orders(&b);
    assert!(creation.stamp.hlc < drop.stamp.hlc, "premise: the stage is created below B's drop");
    a_pause();
    assert!(l.drop_database(DB).unwrap(), "premise: L drops the database, having heard neither");
    a_pause();
    l.create_collection(DB, "orders").unwrap();
    assert_eq!(served_by(&l), Ok("orders".to_string()), "premise: L serves the life it made");
    let members = [&a, &b, &l];
    let sent = [everything(&a), everything(&b), everything(&l)];
    for (member, order) in meets.iter().enumerate() {
        for other in order {
            window(members[member], &sent[*other]);
        }
    }
    for member in 0..3 {
        for other in (0..3).filter(|other| *other != member) {
            window(members[member], &everything(members[other]));
        }
    }
    members.iter().map(|m| (served_by(m), held(m))).collect()
}

// A KNOWN LIMITATION for 0.62 (ADR-272, decision 5), pre-existing (it diverges the same at 10317639, before the seed rule): a database drop
// that races a swap. A member that holds the stage when the name's ordinary drop arrives takes it as a logical drop, and that drop (and the
// ordinary drop of the name that the database's drop writes just above itself) hides a life made after the database's drop; a member whose
// database drop reached it first refuses the stage as history, never learns of the swap, and serves that life. The members disagree for
// good, and the one that answers NOT FOUND still holds the collection, so anti-entropy has nothing to find. The body asserts convergence,
// so that the unit that fixes it, after 0.62, is told when it has: remove the `ignore` there. The disagreement is counted where it
// happens (`kimmy_register_hidden_creations_total`).
#[test]
#[ignore = "known limitation for 0.62, a database drop racing a swap (decision 5): fixed as its own unit after 0.62"]
fn three_members_that_met_a_drop_a_database_drop_and_a_new_life_agree_in_every_order() {
    const ORDERS: [[usize; 2]; 2] = [[0, 1], [1, 0]];
    // A meets B and L, B meets A and L, L meets A and B: each in either order, so eight runs
    for run in 0..8usize {
        let pick = |member: usize| -> [usize; 2] {
            let others: Vec<usize> = (0..3).filter(|o| *o != member).collect();
            let order = ORDERS[(run >> member) & 1];
            [others[order[0]], others[order[1]]]
        };
        let meets = [pick(0), pick(1), pick(2)];
        let ends = three_members_that_meet_in(meets);
        for (member, (answer, holds)) in ends.iter().enumerate().skip(1) {
            assert_eq!(
                answer, &ends[0].0,
                "member {member} answers as A does, meeting in {meets:?}"
            );
            assert_eq!(
                holds, &ends[0].1,
                "member {member} holds what A holds, meeting in {meets:?}"
            );
        }
    }
}
