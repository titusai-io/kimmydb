//! A binding the register refuses writes nothing, the seed included (ADR-272, decision 6; unit F1).
//!
//! The seed turns the tombstone of a drop a member applied before it knew of any swap into a logical drop, when the name's first event
//! arrives. For a binding on a member whose identity is gone it does so whatever the stage's stamp, because what the binding makes
//! visible needs that drop below it. A binding the register refuses makes nothing visible, so a seed it brought would leave a drop in
//! the register of a member that met the binding after the drop and in none that met it before: the two would answer differently for
//! the same entries, for good.
//!
//! The case: `orders` is created, then dropped; a stage of it is created ABOVE the drop; a binding of the stage is refused (above the
//! stage's deadline, or from another origin than the stage's creation); and a write stamped above the drop, by a member that had not
//! heard of it, makes the name stand again. One member takes the drop before the stage and the binding, one after. Both must hold no
//! register event and serve `orders` with the write. The binding arrives in a window (it would take effect) or with no window behind
//! it (it would wait as information): the two ways into the register.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, DocId, Hlc, MergePolicy, OpKind, OplogEntry, RegisterEvent,
    Stamp, VersionVector,
};
use redb::ReadableDatabase;

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

const DB: &str = "app";
const STAGE: &str = "orders.__stg.a1";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn entry_of(from: &Engine, kind: OpKind, name: &str) -> OplogEntry {
    everything(from)
        .into_iter()
        .find(|e| e.kind == kind && e.collection == CollectionId::derive(DB, name))
        .unwrap_or_else(|| panic!("the author's oplog holds the {kind:?} of {name}"))
}

fn window(m: &Engine, entry: &OplogEntry) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    theirs.observe(entry.stamp);
    let mine = m.witnessed_vector().unwrap();
    let mut outcome = SyncOutcome::default();
    m.apply_peer_window_into(
        &theirs,
        Some(&mine),
        std::slice::from_ref(entry),
        PeerWindow {
            scanned_to: entry.stamp.hlc,
            exhausted: true,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )
    .unwrap();
    outcome
}

fn bind_at(stamp: Stamp) -> OplogEntry {
    OplogEntry {
        stamp,
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: STAGE.into(),
            })
            .unwrap(),
        ),
        meta: None,
    }
}

/// The entries of the case, each from the member that made it.
struct Case {
    /// `orders` created, and then dropped, by one member.
    created: OplogEntry,
    dropped: OplogEntry,
    /// The stage's creation, by another member, above the drop; and the deadline it carries.
    stage: OplogEntry,
    deadline: Hlc,
    /// A document written to `orders`, above the drop, by a member that holds the creation and never heard of the drop.
    write: OplogEntry,
    stage_writer: Engine,
    /// A member that made none of the above.
    stranger: Engine,
    _dirs: Vec<tempfile::TempDir>,
}

fn tick() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn case() -> Case {
    let (a, da) = engine();
    a.create_collection(DB, "orders").unwrap();
    let created = entry_of(&a, OpKind::CreateCollection, "orders");
    a.drop_collection(DB, "orders").unwrap();
    let dropped = entry_of(&a, OpKind::DropCollection, "orders");
    tick();
    let (w, dw) = engine();
    w.apply_batch(std::slice::from_ref(&created)).unwrap();
    let orders = w.get_collection(DB, "orders").unwrap();
    w.insert(&orders, doc! { "_id": 1, "v": "after the drop" }).unwrap();
    let write = entry_of(&w, OpKind::Insert, "orders");
    let (b, db) = engine();
    let deadline = Hlc::new(physical_now_ms() + 60_000, 0);
    b.create_stage(DB, STAGE, MergePolicy::Document, deadline).unwrap();
    let stage = entry_of(&b, OpKind::CreateCollection, STAGE);
    let (stranger, ds) = engine();
    assert!(
        created.stamp.hlc < dropped.stamp.hlc
            && dropped.stamp.hlc < stage.stamp.hlc
            && dropped.stamp.hlc < write.stamp.hlc,
        "premise: the stage and the write are stamped above the drop"
    );
    Case {
        created,
        dropped,
        stage,
        deadline,
        write,
        stage_writer: b,
        stranger,
        _dirs: vec![da, dw, db, ds],
    }
}

/// From the stage's own writer, one millisecond above the stage's deadline.
fn above_the_deadline(c: &Case) -> OplogEntry {
    let b = &c.stage_writer;
    bind_at(Stamp::new(Hlc::new(c.deadline.wall_ms + 1, 0), b.node_id(), b.epoch()))
}

/// Inside the deadline, from a member that did not create the stage.
fn from_another_origin(c: &Case) -> OplogEntry {
    let other = &c.stranger;
    let hlc = Hlc::new(c.stage.stamp.hlc.wall_ms + 1, 0);
    bind_at(Stamp::new(hlc, other.node_id(), other.epoch()))
}

#[derive(Clone, Copy, Debug)]
enum Delivery {
    /// In a window: a binding the register took would take effect.
    Window,
    /// With no window behind it: a binding the register took would wait as information.
    Bare,
}

#[derive(Clone, Copy, Debug)]
enum Order {
    /// The drop, then the stage and the binding: the identity is gone when the binding arrives.
    DropFirst,
    /// The stage and the binding, then the drop: the identity stands when the binding arrives.
    BindingFirst,
}

/// A name's register events, each under its stamp.
type Events = Vec<(Stamp, RegisterEvent)>;

/// What a member came to.
#[derive(Debug, PartialEq)]
struct End {
    in_effect: Events,
    ahead_of_position: Events,
    served: Result<String, String>,
    holds_the_write: bool,
}

fn register_of(m: &Engine) -> (Events, Events) {
    let txn = m.db().begin_read().unwrap();
    let in_effect = crate::register::timeline_read(&txn, DB, "orders").unwrap();
    let ahead = crate::register::parked_read(&txn, DB, "orders").unwrap();
    (in_effect.events().to_vec(), ahead.events().to_vec())
}

fn end_of(m: &Engine) -> End {
    let (in_effect, ahead_of_position) = register_of(m);
    let resolved = m.resolve(DB, "orders");
    let holds_the_write =
        resolved.as_ref().is_ok_and(|r| m.get(&r.meta, &DocId::Int64(1)).unwrap().is_some());
    let served = resolved.map(|r| r.physical).map_err(|e| e.to_string());
    End { in_effect, ahead_of_position, served, holds_the_write }
}

/// A member takes the case's entries in `order`, the binding by `delivery`, and the write last.
fn member(c: &Case, binding: &OplogEntry, order: Order, delivery: Delivery) -> End {
    let (m, _dir) = engine();
    window(&m, &c.created);
    let the_drop = || {
        window(&m, &c.dropped);
        m.finish_purges_now().unwrap();
        assert!(m.get_collection(DB, "orders").is_err(), "premise: the drop took the identity");
    };
    let the_stage_and_the_binding = || {
        window(&m, &c.stage);
        match delivery {
            Delivery::Window => {
                window(&m, binding);
            }
            Delivery::Bare => {
                m.apply_batch(std::slice::from_ref(binding)).unwrap();
            }
        }
    };
    match order {
        Order::DropFirst => {
            the_drop();
            the_stage_and_the_binding();
        }
        Order::BindingFirst => {
            the_stage_and_the_binding();
            assert!(m.get_collection(DB, "orders").is_ok(), "premise: the identity stands");
            the_drop();
        }
    }
    window(&m, &c.write);
    end_of(&m)
}

fn both_orders_end_with_the_name_served(binding: fn(&Case) -> OplogEntry, delivery: Delivery) {
    let c = case();
    let binding = binding(&c);
    let stands = End {
        in_effect: Vec::new(),
        ahead_of_position: Vec::new(),
        served: Ok("orders".to_string()),
        holds_the_write: true,
    };
    // the member that met the binding while the identity stood first: it is the one the other must agree with
    let binding_first = member(&c, &binding, Order::BindingFirst, delivery);
    assert_eq!(binding_first, stands, "the binding, then the drop ({delivery:?})");
    let drop_first = member(&c, &binding, Order::DropFirst, delivery);
    assert_eq!(drop_first, stands, "the drop, then the binding ({delivery:?})");
}

#[test]
fn a_binding_above_the_deadline_taken_in_a_window_leaves_both_orders_alike() {
    both_orders_end_with_the_name_served(above_the_deadline, Delivery::Window);
}

#[test]
fn a_binding_from_another_origin_taken_in_a_window_leaves_both_orders_alike() {
    both_orders_end_with_the_name_served(from_another_origin, Delivery::Window);
}

#[test]
fn a_binding_above_the_deadline_with_no_window_behind_it_leaves_both_orders_alike() {
    both_orders_end_with_the_name_served(above_the_deadline, Delivery::Bare);
}

#[test]
fn a_binding_from_another_origin_with_no_window_behind_it_leaves_both_orders_alike() {
    both_orders_end_with_the_name_served(from_another_origin, Delivery::Bare);
}

/// The control: a VALID binding of a stage created above the drop still seeds on a member whose identity is gone, so that the drop is
/// in the register below what the binding makes visible.
#[test]
fn a_valid_binding_still_seeds_the_drop_below_what_it_makes_visible() {
    let c = case();
    let b = &c.stage_writer;
    let hlc = Hlc::new(c.stage.stamp.hlc.wall_ms + 1, 0);
    let binding = bind_at(Stamp::new(hlc, b.node_id(), b.epoch()));
    let (m, _dir) = engine();
    window(&m, &c.created);
    window(&m, &c.dropped);
    m.finish_purges_now().unwrap();
    window(&m, &c.stage);
    assert_eq!(register_of(&m).0, Vec::new(), "premise: the stage, above the drop, seeds nothing");
    window(&m, &binding);
    // storage's rule today: the register function answers the binding alone here (no stage was created below the drop), and F2 flips this
    assert_eq!(
        register_of(&m).0,
        vec![
            (c.dropped.stamp, RegisterEvent::LogicalDrop),
            (binding.stamp, RegisterEvent::Bind { physical: STAGE.into() }),
        ],
        "the drop is seeded below the binding"
    );
    assert_eq!(m.resolve(DB, "orders").unwrap().physical, STAGE);
}
