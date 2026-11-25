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
}
