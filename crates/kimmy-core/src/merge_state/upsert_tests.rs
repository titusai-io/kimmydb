//! The upsert entries of the effects layer: `upsert_create` (every shape), `upsert_cells` (the
//! creation cells in list order) and `UpsertEffects::apply`.

use super::tests::{f, i, opts, p, s, wid};
use super::*;
use crate::effects::{CellTag, CreationCell, UpsertEffects};
use crate::hlc::Hlc;

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
    let order = [p("a0"), p("b"), p("c")];
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
    // an order that leaves out a new field is refused
    assert!(matches!(fx.creation_list(&[p("b")]), Err(UpdateRefused::BadPath(_))));
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
