//! A register or cell that an alive explicit insert suppresses (the `$setOnInsert` cells of an upsert that raced it,
//! V6m2) takes part in no `clash` and no `typeClash`: the write that made it is reported once, as the `insertLost` of
//! the creation cell, and the insert's own values, which are what the document shows, are never reported as the loser
//! of a conflict with a cell that is not in the document (Step 4 design 7.1).

use std::collections::BTreeSet;

use super::tests::{Mem, all_orders, h, i, opts, s};
use super::*;

fn np(dotted: &str) -> Path {
    Path::new(dotted.split('.')).unwrap()
}

fn sub(f: &[(&str, Value)]) -> Value {
    Value::Document(f.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect())
}

/// An upsert at `B` whose `$setOnInsert` is `fields` (dotted paths) and which increments `views`: `(life, inc stamp)`.
fn upsert_at(b: &mut Mem, fields: &[(&str, Value)]) -> (MStamp, MStamp) {
    let f: Vec<(Path, Value)> = fields.iter().map(|(k, v)| (np(k), v.clone())).collect();
    let (c, inc) = b.w.upsert_soi(&f, &np("views"), 1, &[], 0).unwrap();
    let (life, at) = (c.stamp, inc.stamp);
    b.log(Some(c));
    b.log(Some(inc));
    (life, at)
}

/// An explicit insert at `A` and an upsert at `B`, concurrent, the insert's clock `ins_at` and the upsert's `ups_at`:
/// every delivery order shows `visible` and ends holding exactly the records `records` (kind, dotted path), all of the
/// upsert's creation write.
fn insert_against_upsert(
    insert: &[(&str, Value)],
    upsert: &[(&str, Value)],
    (ins_at, ups_at): (u64, u64),
    visible: &[(&str, Value)],
    records: &[(LossKind, &str)],
) {
    let o = opts();
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    a.w.clock = h(ins_at);
    b.w.clock = h(ups_at);
    let ia = a.ins(insert, 0).unwrap();
    let (life, at) = upsert_at(&mut b, upsert);
    let want: BTreeSet<_> =
        records.iter().map(|(k, path)| (*k, LossAt::Field(np(path)), life)).collect();
    let shown: Option<std::collections::BTreeMap<Path, Value>> =
        Some(visible.iter().map(|(k, v)| (np(k), v.clone())).collect());
    for (ord, m) in all_orders(&o, &[a.ent(ia), b.ent(at)], &[]) {
        assert_eq!(m.visible(), shown, "{ord:?}");
        assert_eq!(m.loss_set(), want, "{ord:?}");
    }
}

#[test]
fn a_newer_suppressed_cell_does_not_make_the_inserts_own_value_the_loser_of_a_clash() {
    // A inserts theme "light"; B's upsert, newer, would set it "dark" on insert. The document shows A's value; the
    // upsert's cell is the one that was lost and is reported once, as an insertLost, and no clash names "light".
    insert_against_upsert(
        &[("theme", s("light")), ("name", s("A"))],
        &[("theme", s("dark"))],
        (1, 6),
        &[("name", s("A")), ("theme", s("light")), ("views", i(1))],
        &[(LossKind::InsertLost, "theme")],
    );
}

#[test]
fn an_older_suppressed_cell_is_reported_as_an_insert_lost_and_not_also_as_a_clash() {
    insert_against_upsert(
        &[("theme", s("light")), ("name", s("A"))],
        &[("theme", s("dark"))],
        (6, 1),
        &[("name", s("A")), ("theme", s("light")), ("views", i(1))],
        &[(LossKind::InsertLost, "theme")],
    );
}

#[test]
fn a_suppressed_scalar_over_the_inserts_map_is_no_clash_and_no_type_clash() {
    // The insert's `a` is a map holding `b`; the upsert's `a = 5` on insert is suppressed.
    insert_against_upsert(
        &[("a", sub(&[("b", i(1))]))],
        &[("a", i(5))],
        (1, 6),
        &[("a", Value::Map), ("a.b", i(1)), ("views", i(1))],
        &[(LossKind::InsertLost, "a")],
    );
}

#[test]
fn a_suppressed_cell_below_the_inserts_scalar_is_no_type_clash_in_either_order() {
    for at in [(1, 6), (6, 1)] {
        insert_against_upsert(
            &[("a", i(5))],
            &[("a.b", i(1))],
            at,
            &[("a", i(5)), ("views", i(1))],
            &[(LossKind::InsertLost, "a.b")],
        );
    }
}

#[test]
fn two_upserts_that_set_one_field_on_insert_still_clash() {
    // Nothing is suppressed (no explicit insert is alive): the older cell lost to the newer, and it is reported.
    let o = opts();
    let (mut a, mut b) = (Mem::new('A', &o), Mem::new('B', &o));
    a.w.clock = h(1);
    b.w.clock = h(6);
    let (la, ea) = upsert_at(&mut a, &[("theme", s("light"))]);
    let (_, eb) = upsert_at(&mut b, &[("theme", s("dark"))]);
    for (ord, m) in all_orders(&o, &[a.ent(ea), b.ent(eb)], &[]) {
        assert!(
            m.loss_set().contains(&(LossKind::Clash, LossAt::Field(np("theme")), la)),
            "{ord:?}: {:?}",
            m.loss_set()
        );
    }
}

#[test]
fn two_upserts_and_an_insert_report_each_suppressed_cell_at_least_once_and_never_the_inserts_value()
{
    // The two upserts may meet before the insert reaches a member: then the older one's cell is a true clash for
    // as long as nothing suppresses it (and stays, a union), and the insertLost follows. Either way every
    // suppressed cell is reported as an insertLost, and no record names the insert's own value as a loser.
    let o = opts();
    let (mut a, mut b, mut c) = (Mem::new('A', &o), Mem::new('B', &o), Mem::new('C', &o));
    a.w.clock = h(1);
    b.w.clock = h(4);
    c.w.clock = h(8);
    let (la, ea) = upsert_at(&mut a, &[("theme", s("a"))]);
    let (lb, eb) = upsert_at(&mut b, &[("theme", s("b"))]);
    let ic = c.ins(&[("theme", s("c")), ("name", s("C"))], 0).unwrap();
    let shown: Option<std::collections::BTreeMap<Path, Value>> = Some(
        [("theme", s("c")), ("name", s("C")), ("views", i(2))]
            .into_iter()
            .map(|(k, v)| (np(k), v))
            .collect(),
    );
    for (ord, m) in all_orders(&o, &[a.ent(ea), b.ent(eb), c.ent(ic)], &[]) {
        assert_eq!(m.visible(), shown, "{ord:?}");
        let records = m.loss_set();
        for life in [la, lb] {
            assert!(
                records.contains(&(LossKind::InsertLost, LossAt::Field(np("theme")), life)),
                "{ord:?}: every suppressed cell is reported: {records:?}"
            );
        }
        assert!(
            records.iter().all(|(kind, _, stamp)| {
                *stamp != ic || !matches!(kind, LossKind::Clash | LossKind::TypeClash)
            }),
            "{ord:?}: the insert's own value is never the loser of a conflict: {records:?}"
        );
    }
}

#[test]
fn an_insert_that_a_delete_killed_does_not_suppress_the_cells_of_an_upsert_that_saw_the_delete() {
    // The insert is dead, so no explicit insert is alive: the upsert's creation cell is the document's value and
    // nothing is reported (the predicate is asked of the alive lives, not of every life).
    let o = opts();
    let (mut i0, mut d, mut u) = (Mem::new('I', &o), Mem::new('D', &o), Mem::new('U', &o));
    let ins = i0.ins(&[("theme", s("i"))], 0).unwrap();
    d.receive_entry(&i0.ent(ins));
    let del = d.del().unwrap();
    u.receive_entry(&i0.ent(ins));
    u.receive_entry(&d.ent(del));
    let (_, at) = upsert_at(&mut u, &[("theme", s("u"))]);
    let shown: Option<std::collections::BTreeMap<Path, Value>> =
        Some([("theme", s("u")), ("views", i(1))].into_iter().map(|(k, v)| (np(k), v)).collect());
    for (ord, m) in all_orders(&o, &[i0.ent(ins), d.ent(del), u.ent(at)], &[]) {
        assert_eq!(m.visible(), shown, "{ord:?}");
        assert!(m.loss_set().is_empty(), "{ord:?}: {:?}", m.loss_set());
    }
}
