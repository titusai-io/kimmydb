//! Tests of the order state that need the codecs and the property tests' runs: the join laws, both
//! codecs and the ordered read on states that carry order entries.
//!
//! The runs of the property tests are given the order state after every local write by
//! [`feed_orders`], a test-only stand-in for item O2 (the writer does not feed it yet). The
//! semantics of the feed and the read are checked against the reference model in `ord_tests`.

use std::collections::{BTreeMap, BTreeSet};

use super::ord_tests::{at, flatten, u, w};
use super::prop_tests::{Rng, check_laws_on, nested_ord_pools, opts, ord_pools};
use super::*;
use crate::hlc::Hlc;

// ---- the feed on the states of the property tests ---------------------------------------

/// Feed the order state of a writer after a local write, the way a writer that feeds it would:
/// the cells this writer stamped since `before` (its vector before the op) are the writes; each
/// stamp is one write into the life its cells are in, naming its cells (an unset for a register
/// that holds `Unset`, a write for the rest) and the maps above them. One write in three that
/// names two cells or more is a replace (the root's level floor, every cell a reset), a map
/// marker is a document-valued write (a write at the map, the level mark, the cells below it
/// resets). `frng` is a stream of its own: the run the writer is in draws the same numbers with
/// the feed and without it. This is a test-only stand-in for item O2.
pub(super) fn feed_orders(w: &mut Writer, before: &Vv, frng: &mut Rng, o: &JoinOptions) {
    let id = w.id;
    let old = before.get(&id).copied().unwrap_or(Hlc::ZERO);
    let known = before.values().copied().max().unwrap_or(Hlc::ZERO);
    // per write stamp: the life, and the cells it wrote as (path, is an unset, is a map marker)
    type Written = (MStamp, Vec<(Path, bool, bool)>);
    let mut cells: BTreeMap<MStamp, Written> = BTreeMap::new();
    for ((l, p), r) in &w.state.regs {
        if r.stamp.writer == id && r.stamp.hlc > old {
            let e = cells.entry(r.stamp).or_insert_with(|| (*l, Vec::new()));
            e.1.push((p.clone(), r.value == Value::Unset, r.value == Value::Map));
        }
    }
    for ((l, p, _, _), s) in &w.state.slots {
        if s.stamp.writer == id && s.stamp.hlc > old {
            cells.entry(s.stamp).or_insert_with(|| (*l, Vec::new())).1.push((
                p.clone(),
                false,
                false,
            ));
        }
    }
    for (stamp, (life, mut cs)) in cells {
        // a cell and the slot of the same path in one write name the field once
        cs.sort();
        cs.dedup_by(|a, b| a.0 == b.0);
        let replace = cs.len() >= 2 && frng.below(3) == 0;
        let mut items: Vec<(OrdAt, OrdMode)> = Vec::new();
        if replace {
            items.push((OrdAt::Root, OrdMode::Level));
        }
        let mut anc = BTreeSet::new();
        for (p, unset, map) in &cs {
            let at = OrdAt::Field(p.clone());
            if *unset {
                items.push((at, OrdMode::Unset));
                continue;
            }
            for a in p.ancestors() {
                if anc.insert(a.clone()) && !cs.iter().any(|c| c.0 == a) {
                    items.push((OrdAt::Field(a), OrdMode::Write));
                }
            }
            let below_a_map = cs.iter().any(|(q, _, m)| *m && p.is_below(q));
            items.push((
                at.clone(),
                if replace || below_a_map { OrdMode::Reset } else { OrdMode::Write },
            ));
            if *map {
                items.push((at, OrdMode::Level));
            }
        }
        let events = ord_events(&items, o);
        w.state.ord_apply(life, stamp, known, &events, o);
    }
}

// ---- the property tests' states with the order state fed -----------------------------

/// What a pool of states with order entries holds, by shape.
#[derive(Default, Debug)]
struct Seen {
    states: usize,
    with_ords: usize,
    entries: usize,
    root_entries: usize,
    floor_u: usize,
    floor_u_bit: usize,
    floor_r: usize,
    lvl: usize,
    last: usize,
    firsts_2_plus: usize,
}

fn tally(pools: &[Vec<MergeState>]) -> Seen {
    let mut t = Seen::default();
    for s in pools.iter().flatten() {
        t.states += 1;
        t.with_ords += usize::from(!s.ords.is_empty());
        for ((_, at), x) in &s.ords {
            t.entries += 1;
            t.root_entries += usize::from(*at == OrdAt::Root);
            match x.floor {
                Some(Floor::U { others: true, .. }) => t.floor_u_bit += 1,
                Some(Floor::U { .. }) => t.floor_u += 1,
                Some(Floor::R { .. }) => t.floor_r += 1,
                None => {}
            }
            t.lvl += usize::from(x.lvl.is_some());
            t.last += usize::from(x.last.is_some());
            t.firsts_2_plus += usize::from(x.firsts.len() >= 2);
        }
    }
    t
}

/// The join laws, the codecs and the ordered read on states that carry order entries: the flat and
/// the nested runs of the property tests with the order state fed after every local write (a
/// stand-in for item O2: `feed_orders`), at a retention beyond any clock, at 20, and at 6 where the
/// exact associativity is not claimed.
#[test]
fn states_with_order_entries_obey_the_laws_and_both_codecs() {
    let mut total = Seen::default();
    for (o, exact) in [(opts(1_000_000_000), true), (opts(20), true), (opts(6), false)] {
        let sets = [("flat", ord_pools(40, 24, &o)), ("nested", nested_ord_pools(40, 26, &o))];
        for (name, ps) in sets {
            let seen = tally(&ps);
            assert!(seen.with_ords * 2 > seen.states, "{name}: {seen:?}");
            check_laws_on(&o, exact, &ps, 400);
            for st in ps.iter().flatten() {
                assert!(
                    st.check_decoded().is_ok(),
                    "{name}: the states writers build are accepted"
                );
                let c = st.encode_compact();
                let back = MergeState::decode_compact(&c).expect("decode_compact");
                assert_eq!(&back, st);
                assert_eq!(back.encode_compact(), c, "canonical");
                assert_eq!(back.compact_hash(), st.compact_hash());
                assert_eq!(c[3], if st.ords.is_empty() { 2 } else { 3 }, "the version byte");
                if !st.ords.is_empty() {
                    assert!(!st.v1_can_hold(), "version 1 cannot spell an order state");
                    assert_eq!(st.try_encode(), Err(V1CannotHold));
                    assert_eq!(st.try_canonical_bytes(), Err(V1CannotHold));
                }
                // the ordered read: the same fields and values as the flat one, a document for
                // exactly the states that have a live life
                let flat_view = st.visible(&o);
                let ordered = st.ordered_document(&o);
                assert_eq!(flat_view.is_some(), ordered.is_some());
                if let (Some(v), Some(d)) = (flat_view, ordered) {
                    let f = flatten(&d);
                    assert_eq!(f.len(), v.len());
                    assert!(f.iter().all(|(p, x)| v.get(p) == Some(x)));
                    assert_eq!(
                        OrderedValue::paths(&d),
                        f.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>()
                    );
                }
            }
            if o.retention == 1_000_000_000 {
                total.states += seen.states;
                total.with_ords += seen.with_ords;
                total.entries += seen.entries;
                total.root_entries += seen.root_entries;
                total.floor_u += seen.floor_u;
                total.floor_u_bit += seen.floor_u_bit;
                total.floor_r += seen.floor_r;
                total.lvl += seen.lvl;
                total.last += seen.last;
                total.firsts_2_plus += seen.firsts_2_plus;
            }
        }
    }
    eprintln!("states with order entries: {total:?}");
    assert!(total.floor_u > 100 && total.floor_u_bit > 100 && total.floor_r > 100, "{total:?}");
    assert!(total.lvl > 100 && total.root_entries > 100 && total.firsts_2_plus > 100, "{total:?}");
}

/// The same document in the same order for every order of the joins: the ordered read is a
/// function of the state. Triples of pooled states joined both ways (the state, the document).
#[test]
fn the_ordered_document_does_not_depend_on_the_join_order() {
    let o = opts(1_000_000_000);
    let ps = nested_ord_pools(30, 26, &o);
    let mut rng = Rng(0x00DD_BA11);
    let (mut differing_from_flat_order, mut compared) = (0, 0);
    for _ in 0..600 {
        let p = &ps[rng.below(ps.len())];
        let (a, b, c) = (&p[rng.below(p.len())], &p[rng.below(p.len())], &p[rng.below(p.len())]);
        let l = join(&join(a, b, &o).0, c, &o).0;
        let r = join(c, &join(b, a, &o).0, &o).0;
        assert_eq!(l.ordered_document(&o), r.ordered_document(&o));
        if let Some(d) = l.ordered_document(&o) {
            compared += 1;
            let by_path: Vec<Path> = l.visible(&o).unwrap().into_keys().collect();
            differing_from_flat_order += usize::from(OrderedValue::paths(&d) != by_path);
        }
    }
    // the order is not the path order, or the test would prove nothing about the order state
    assert!(
        compared > 300 && differing_from_flat_order > 50,
        "{compared} {differing_from_flat_order}"
    );
}

// ---- what decode refuses is what the join is not idempotent on -------------------------------

/// The two invariants of the order state that decode checks because the join relies on them
/// (`MergeState::check_decoded`, items 10 and 11): on a state that breaks one the join is not
/// idempotent, so the replicas would differ by the order of their receives.
#[test]
fn the_join_is_not_idempotent_where_decode_refuses_an_order_entry() {
    let o = opts(1_000_000_000);
    let a = w(b'A');
    let life = at(1, b'A');
    let mut s = MergeState::default();
    s.lives.insert(life, Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Insert });
    s.vv.insert(a, Hlc::new(9, 0));
    // an entry with a candidate its own floor voids
    let bad = FieldOrd {
        floor: Some(u(5, b'A', 4, false)),
        firsts: [(a, (at(2, b'A'), 0))].into(),
        ..FieldOrd::default()
    };
    s.ords.insert((life, OrdAt::Field(Path::flat("x").unwrap())), bad);
    assert_ne!(join(&s, &s, &o).0, s, "the join drops the voided candidate");
    assert_eq!(MergeState::decode_compact(&s.encode_compact()), Err(DecodeError::OrdNotNormal));
    // an entry of a life the state does not list
    let mut t = MergeState::default();
    t.vv.insert(a, Hlc::new(9, 0));
    t.ords.insert((life, OrdAt::Root), FieldOrd { lvl: Some(at(2, b'A')), ..FieldOrd::default() });
    assert_ne!(
        join(&t, &MergeState::default(), &o).0,
        t,
        "the join drops the entry of a life that is not alive"
    );
    assert_eq!(MergeState::decode_compact(&t.encode_compact()), Err(DecodeError::OrdLifeNotListed));
    // a version vector behind a stamp of an entry
    let mut v = s.clone();
    v.ords.clear();
    v.ords.insert((life, OrdAt::Root), FieldOrd { lvl: Some(at(20, b'A')), ..FieldOrd::default() });
    assert_eq!(
        MergeState::decode_compact(&v.encode_compact()),
        Err(DecodeError::VersionVectorBehind)
    );
}
