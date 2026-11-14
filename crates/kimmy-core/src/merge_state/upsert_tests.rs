//! The upsert entries of the effects layer: `upsert_create` (every shape), `upsert_cells` (the
//! creation cells in list order) and `UpsertEffects::apply`.

use super::tests::{f, fl, i, opts, p, s, wid};
use super::*;
use crate::effects::{CellTag, CreationCell, UpsertEffects};
use crate::hlc::Hlc;
use crate::ids::NodeId;

fn cell(name: &str, v: Value, tag: CellTag) -> CreationCell {
    CreationCell { path: p(name), value: v, tag }
}

fn inc(name: &str, by: Value) -> CreationCell {
    cell(name, by, CellTag::Inc)
}

fn names(w: &Writer) -> Vec<String> {
    w.state
        .ordered_document(&opts())
        .expect("a life is alive")
        .into_iter()
        .map(|(n, _)| n)
        .collect()
}

fn writer() -> Writer {
    Writer::new(wid('A', 0), opts())
}

/// The creation entry takes the cells as ONE list in the order given, whatever the tags:
/// filter {a: 1, b: 2} with $set {a: 5} is the cells [a (plain, 5), b (creation, 2)] and the
/// document reads `a`, `b`, not `b`, `a`.
#[test]
fn the_creation_cells_are_written_in_list_order_whatever_their_tags() {
    let fx = UpsertEffects {
        from_filter: vec![(p("a"), i(1)), (p("b"), i(2))],
        set: vec![(p("a"), i(5))],
        ..Default::default()
    };
    let cells = fx.creation_cells().unwrap();
    assert_eq!(cells.iter().map(|c| c.path.to_string()).collect::<Vec<_>>(), ["a", "b"]);
    let mut w = writer();
    w.upsert_cells(&cells, &[], 0).unwrap();
    assert_eq!(names(&w), ["a", "b"]);
    let vis = w.visible().unwrap();
    assert_eq!((vis.get(&p("a")), vis.get(&p("b"))), (Some(&i(5)), Some(&i(2))));
    // the same through the effects entry
    let mut w2 = writer();
    fx.apply(&mut w2, &[], 0).unwrap();
    assert_eq!(w2.state, w.state);
    // and the tags do not order: a tagged cell after a plain one, then the reverse
    for (first, second) in
        [(CellTag::Plain, CellTag::Creation), (CellTag::Creation, CellTag::Plain)]
    {
        let mut w = writer();
        w.upsert_cells(&[cell("x", i(1), first), cell("y", i(2), second)], &[], 0).unwrap();
        assert_eq!(names(&w), ["x", "y"], "{first:?} then {second:?}");
        let mut w = writer();
        w.upsert_cells(&[cell("y", i(2), second), cell("x", i(1), first)], &[], 0).unwrap();
        assert_eq!(names(&w), ["y", "x"], "{second:?} then {first:?}");
    }
}

/// The two-list entry is the ordered one with the tagged cells first: it kept its behaviour.
#[test]
fn the_two_list_upsert_is_the_ordered_one_with_the_tagged_cells_first() {
    let (mut old, mut new) = (writer(), writer());
    old.upsert_soi_create(&[(p("b"), i(2))], &p("n"), &[(p("a"), i(5))], 0).unwrap();
    new.upsert_cells(
        &[cell("b", i(2), CellTag::Creation), cell("a", i(5), CellTag::Plain)],
        &[&p("n")],
        0,
    )
    .unwrap();
    assert_eq!(old.state, new.state);
    assert_eq!(names(&old), ["b", "a"]);
}

#[test]
fn increments_alone_are_one_write_with_typed_operands() {
    let mut w = writer();
    let incs = vec![inc("n", Value::Int32(2)), inc("big", i(5)), inc("d", f(1.5))];
    let (created, second) = w.upsert_create(&incs, 0).unwrap();
    assert!(second.is_none(), "one write");
    assert_eq!(w.state.lives.len(), 1);
    assert_eq!(w.state.vv[&w.id], created.stamp.hlc);
    let vis = w.visible().unwrap();
    assert_eq!(vis.get(&p("n")), Some(&Value::Int32(2)));
    assert_eq!(vis.get(&p("big")), Some(&i(5)));
    // a double on an absent field is a register, not a counter
    assert_eq!(vis.get(&p("d")), Some(&f(1.5)));
    assert_eq!(w.state.slots.len(), 2);
    assert!(w.state.regs.keys().any(|(_, q)| *q == p("d")));
}

#[test]
fn creation_cells_with_increments_are_two_writes_and_the_second_is_one_stamp_for_all() {
    let mut w = writer();
    let list =
        vec![cell("a", s("x"), CellTag::Creation), inc("n", i(1)), inc("m", Value::Int32(3))];
    let (created, second) = w.upsert_create(&list, 0).unwrap();
    let second = second.expect("the increment write");
    assert!(second.stamp.hlc > created.stamp.hlc);
    assert_eq!(w.state.vv[&w.id], second.stamp.hlc);
    assert_eq!(w.state.slots.len(), 2);
    assert!(w.state.slots.values().all(|sl| sl.stamp == second.stamp));
    assert_eq!(names(&w), ["a", "n", "m"]);
}

#[test]
fn an_empty_upsert_creates_the_empty_document() {
    let mut w = writer();
    let (created, second) = w.upsert_create(&[], 0).unwrap();
    assert!(second.is_none());
    assert_eq!(w.state.lives.len(), 1);
    assert!(w.state.regs.is_empty() && w.state.slots.is_empty());
    assert_eq!(w.visible().unwrap().len(), 0);
    assert_eq!(w.state.vv[&w.id], created.stamp.hlc);
}

/// Every refusal is whole: no stamp, no clock move, no life.
#[test]
fn a_refused_upsert_changes_nothing() {
    let a = cell("a", i(1), CellTag::Plain);
    let cases: Vec<(Vec<CreationCell>, &str)> = vec![
        (vec![inc("n", s("x"))], "not a number"),
        (
            vec![
                a.clone(),
                CreationCell {
                    path: Path::new(["a", "b"]).unwrap(),
                    value: i(1),
                    tag: CellTag::Inc,
                },
            ],
            "overlap",
        ),
        (vec![a.clone(), inc("a", i(1))], "same path"),
        (vec![inc("n", i(1)), inc("n", i(2))], "twice"),
        (vec![inc("n", i(1)), inc("k", s("z"))], "bad operand after a good one"),
    ];
    for (cells, what) in cases {
        let mut w = writer();
        let before = (w.state.clone(), w.clock);
        assert!(w.upsert_create(&cells, 0).is_err(), "{what}");
        assert_eq!((w.state.clone(), w.clock), before, "{what}: nothing moved");
    }
    // a life that is alive refuses the upsert
    let mut w = writer();
    w.upsert_create(std::slice::from_ref(&a), 0).unwrap();
    let before = (w.state.clone(), w.clock);
    assert_eq!(w.upsert_create(&[a], 0).unwrap_err(), UpdateRefused::LifeExists);
    assert_eq!((w.state, w.clock), before);
}

#[test]
fn stamps_of_a_caller_are_both_checked_before_the_creation_is_written() {
    let mk = |ms: u64| MStamp { hlc: Hlc::new(ms, 0), writer: wid('A', 0) };
    let list = [cell("a", i(1), CellTag::Plain), inc("n", i(1))];
    // a second stamp that is not above the first
    let mut w = writer();
    let before = (w.state.clone(), w.clock);
    let mut stamps = vec![mk(10), mk(10)].into_iter();
    let r = w.upsert_create_stamped(&list, &mut || stamps.next().unwrap());
    assert!(matches!(r, Err(UpdateError::Stamp(_))), "{r:?}");
    assert_eq!((w.state.clone(), w.clock), before);
    // another writer's stamp
    let other = MStamp { hlc: Hlc::new(30, 0), writer: wid('B', 0) };
    let mut stamps = vec![mk(20), other].into_iter();
    let r = w.upsert_create_stamped(&list, &mut || stamps.next().unwrap());
    assert!(matches!(r, Err(UpdateError::Stamp(StampError::OtherWriter { .. }))), "{r:?}");
    assert_eq!((w.state.clone(), w.clock), before);
    // good stamps are used as given, in order
    let mut stamps = vec![mk(20), mk(30)].into_iter();
    let (c, second) = w.upsert_create_stamped(&list, &mut || stamps.next().unwrap()).unwrap();
    assert_eq!((c.stamp, second.unwrap().stamp), (mk(20), mk(30)));
}

/// The increments are cells of the ONE ordered list: a field written by `$inc` takes its place in
/// the post-image order among the cells, not after all of them. Filter {a: 1}, `$set {b: 2}`,
/// `$inc {c: 1, a0: 1}`, post-image order a, a0, b, c.
#[test]
fn an_increment_field_takes_its_place_in_the_creation_order() {
    let list = [
        cell("a", i(1), CellTag::Creation),
        inc("a0", i(1)),
        cell("b", i(2), CellTag::Plain),
        inc("c", i(1)),
    ];
    let mut w = writer();
    let (_, second) = w.upsert_create(&list, 0).unwrap();
    assert!(second.is_some());
    assert_eq!(names(&w), ["a", "a0", "b", "c"]);
    let vis = w.visible().unwrap();
    assert_eq!((vis.get(&p("a0")), vis.get(&p("c"))), (Some(&i(1)), Some(&i(1))));
    // and another interleaving
    let mut w = writer();
    w.upsert_create(&[inc("c", i(1)), cell("b", i(2), CellTag::Plain), inc("a0", i(1))], 0)
        .unwrap();
    assert_eq!(names(&w), ["c", "b", "a0"]);
    // the same list through the effects layer, with the post-image order named by the caller
    let fx = UpsertEffects {
        from_filter: vec![(p("a"), i(1))],
        set: vec![(p("b"), i(2))],
        inc: vec![(p("c"), i(1)), (p("a0"), i(1))],
        ..Default::default()
    };
    let order = [p("a"), p("a0"), p("b"), p("c")];
    let listed = fx.creation_list(&order).unwrap();
    assert_eq!(
        listed.iter().map(|c| (c.path.to_string(), c.tag)).collect::<Vec<_>>(),
        [
            ("a".to_string(), CellTag::Creation),
            ("a0".to_string(), CellTag::Inc),
            ("b".to_string(), CellTag::Plain),
            ("c".to_string(), CellTag::Inc),
        ]
    );
    let mut w = writer();
    fx.apply(&mut w, &order, 0).unwrap();
    assert_eq!(names(&w), ["a", "a0", "b", "c"]);
    // no order: the default sequence, the increments last
    assert_eq!(
        fx.creation_list(&[]).unwrap().iter().map(|c| c.path.to_string()).collect::<Vec<_>>(),
        ["a", "b", "c", "a0"]
    );
    // an order that leaves out a cell is refused
    assert!(matches!(fx.creation_list(&[p("b")]), Err(UpdateRefused::BadPath(_))));
}

/// The creation order is the FULL post-image order of the plan, the filter's cells included: a
/// filter field that a `$set` overrides keeps its place (filter {a: 1, b: 2} with $set {a: 5}
/// is `a`, `b`, as the reference has it), and an order that leaves out ANY cell is refused,
/// a filter cell as much as a `$set` one.
#[test]
fn the_creation_order_names_every_cell_the_filter_cells_included() {
    let fx = UpsertEffects {
        from_filter: vec![(p("a"), i(1)), (p("b"), i(2))],
        set: vec![(p("a"), i(5))],
        ..Default::default()
    };
    let order = [p("a"), p("b")];
    let listed = fx.creation_list(&order).unwrap();
    assert_eq!(listed.iter().map(|c| c.path.to_string()).collect::<Vec<_>>(), ["a", "b"]);
    let mut w = writer();
    fx.apply(&mut w, &order, 0).unwrap();
    assert_eq!(names(&w), ["a", "b"]);
    let vis = w.visible().unwrap();
    assert_eq!((vis.get(&p("a")), vis.get(&p("b"))), (Some(&i(5)), Some(&i(2))));
    // a filter cell left out of the order is refused
    assert!(matches!(fx.creation_list(&[p("a")]), Err(UpdateRefused::BadPath(_))));
    // the order puts a filter cell after a new one when the post-image does
    let fx = UpsertEffects {
        from_filter: vec![(p("b"), i(1))],
        set_on_insert: vec![(p("a"), i(2))],
        ..Default::default()
    };
    let listed = fx.creation_list(&[p("a"), p("b")]).unwrap();
    assert_eq!(listed.iter().map(|c| c.path.to_string()).collect::<Vec<_>>(), ["a", "b"]);
}

#[test]
fn the_effects_apply_helper_returns_both_writes() {
    let fx = UpsertEffects {
        set_on_insert: vec![(p("created"), i(1))],
        inc: vec![(p("n"), i(2))],
        ..Default::default()
    };
    let mut w = writer();
    let written = fx.apply(&mut w, &[], 0).unwrap();
    assert!(written.incremented.is_some());
    // overlapping lists are refused at the effects layer, nothing written
    let bad = UpsertEffects {
        set_on_insert: vec![(p("n"), i(1))],
        inc: vec![(p("n"), i(2))],
        ..Default::default()
    };
    let mut w = writer();
    assert!(matches!(bad.apply(&mut w, &[], 0), Err(UpdateRefused::Overlap(..))));
    assert!(w.state.lives.is_empty());
}

/// A state with `MAX_WRITERS` writers and no life alive, and a writer that is none of them.
fn full_of_writers() -> (MergeState, Writer) {
    let mut state = MergeState::default();
    let mut last = None;
    for k in 0..MAX_WRITERS {
        let id = WriterId {
            node: NodeId::from_bytes([(k % 200) as u8 + 1; 16]),
            epoch: Epoch::from_u64((k / 200) as u64),
        };
        let mut w = Writer::new(id, opts());
        w.receive(&state);
        if k == 0 {
            w.insert(&fl(&[("n", i(0))]), 0).unwrap();
        }
        w.write_set(&p(&format!("f{k}")), i(k as i64), 0).unwrap();
        state = w.state.clone();
        last = Some(w);
    }
    // the document is deleted: no life is alive, so the cap is what refuses the next writer
    let mut last = last.expect("some writers");
    last.write_delete(0).expect("the delete");
    let state = last.state;
    assert!(state.alive(&opts()).is_empty());
    let late_id = WriterId { node: NodeId::from_bytes([250; 16]), epoch: Epoch::from_u64(9) };
    let mut late = Writer::new(late_id, opts());
    late.receive(&state);
    (state, late)
}

/// A caller's stamps are asked for once the upsert is known to be writable: a writer past the
/// writer cap is refused BEFORE any stamp is pulled, in the two-write shape as in the one-write
/// one ("never for a refusal").
#[test]
fn the_writer_cap_refuses_before_a_caller_stamp_is_pulled() {
    let shapes = [
        vec![cell("a", s("x"), CellTag::Creation), inc("n", i(1))], // two writes
        vec![inc("n", i(1))],                                       // one write
        vec![cell("a", s("x"), CellTag::Creation)],                 // creation only
    ];
    for list in shapes {
        let (_, mut late) = full_of_writers();
        let before = (late.state.clone(), late.clock);
        let id = late.id;
        let pulled = std::cell::Cell::new(0u64);
        let mut stamp = || {
            pulled.set(pulled.get() + 1);
            MStamp { hlc: Hlc::new(10_000_000 + pulled.get(), 0), writer: id }
        };
        let r = late.upsert_create_stamped(&list, &mut stamp);
        assert!(matches!(r, Err(UpdateError::Refused(UpdateRefused::WriterCap))), "{r:?}");
        assert_eq!(pulled.get(), 0, "a stamp was pulled for a refusal");
        assert_eq!((late.state.clone(), late.clock), before);
    }
}

/// A `Decimal128` operand through the creation entry: one write for increments alone, and the
/// two-write shape beside a creation cell; the counter is created in the operand's type.
#[test]
fn a_decimal_operand_creates_a_decimal_counter_through_upsert_create() {
    let dec = |t: &str| Value::Decimal128(t.parse::<bson::Decimal128>().unwrap().bytes());
    let text = |v: &Value| match v {
        Value::Decimal128(b) => bson::Decimal128::from_bytes(*b).to_string(),
        v => panic!("not a decimal: {v:?}"),
    };
    let mut w = writer();
    let (_, second) =
        w.upsert_create(&[inc("d", dec("1.5")), inc("n", Value::Int32(2))], 0).unwrap();
    assert!(second.is_none(), "increments alone are one write");
    let vis = w.visible().unwrap();
    assert_eq!(text(&vis[&p("d")]), "1.5");
    assert_eq!(vis[&p("n")], Value::Int32(2));
    // beside a creation cell: two writes, the decimal in the second
    let mut w = writer();
    let (_, second) = w
        .upsert_create(&[cell("a", s("x"), CellTag::Creation), inc("d", dec("-0.25"))], 0)
        .unwrap();
    assert!(second.is_some());
    let vis = w.visible().unwrap();
    assert_eq!(text(&vis[&p("d")]), "-0.25");
    // and the same increment after the creation adds in Decimal128
    w.write_update(&[UpdateOp::Inc(p("d"), Value::Int32(1))], 0).unwrap();
    assert_eq!(text(&w.visible().unwrap()[&p("d")]), "0.75");
}
