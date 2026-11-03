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
                assert_eq!(&c[..4], b"QMS\x02", "the version byte is 2 for every state");
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

// ---- one state, one spelling: the empty order state is no order state ----------------------

/// A state of the life `(1, A)` with a register at `x` and an order entry for it.
fn with_entry(life: MStamp, x: FieldOrd) -> MergeState {
    let mut s = MergeState::default();
    s.lives.insert(life, Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Insert });
    s.vv.insert(life.writer, Hlc::new(life.hlc.wall_ms.max(9), 0));
    let p = Path::flat("x").unwrap();
    let reg = Reg::new(life, Value::Int64(1), BTreeMap::new(), None, false).unwrap();
    s.regs.insert((life, p.clone()), reg);
    s.ords.insert((life, OrdAt::Field(p)), x);
    s
}

fn some_entry(life: MStamp) -> FieldOrd {
    FieldOrd {
        firsts: [(life.writer, (life, 0))].into(),
        last: Some((life, 0)),
        ..FieldOrd::default()
    }
}

/// Everything that makes two states the same, and the bytes and hashes that follow from it.
fn assert_same_state(a: &MergeState, b: &MergeState, what: &str) {
    assert_eq!(a, b, "{what}");
    assert_eq!(a.encode_compact(), b.encode_compact(), "{what}: bytes");
    assert_eq!(a.compact_bytes(), b.compact_bytes(), "{what}: body");
    assert_eq!(a.compact_hash(), b.compact_hash(), "{what}: hash");
}

/// A state whose order state EMPTIES in a join encodes byte for byte like one that never had an
/// order state: a delete that kills the life, a concurrent first insert that supersedes it, and
/// an empty entry (which no writer makes) that the join removes.
#[test]
fn a_state_whose_order_state_empties_encodes_like_one_that_never_had_it() {
    let o = opts(1_000_000_000);
    let life = at(1, b'A');
    let with = with_entry(life, some_entry(life));
    let mut never = with.clone();
    never.ords.clear();
    assert!(!with.ords.is_empty() && with.encode_compact() != never.encode_compact());

    // 1. a delete that the life did not see kills it: its cells and its order entries go
    let mut d = MergeState::default();
    let del = at(5, b'B');
    d.deletes.insert(del, [(w(b'B'), Hlc::new(5, 0))].into());
    d.vv.insert(w(b'B'), Hlc::new(5, 0));
    let (killed, _) = join(&with, &d, &o);
    let (killed_never, _) = join(&never, &d, &o);
    assert!(
        killed.ords.is_empty() && killed.regs.is_empty(),
        "the life is dead: no cells, no entries"
    );
    assert_same_state(&killed, &killed_never, "after a delete");
    assert_eq!(killed.encode_compact()[3], 2);
    // and the other way round
    assert_same_state(&join(&d, &with, &o).0, &killed_never, "after a delete, the other way");

    // 2. a newer concurrent first insert supersedes the life: it is dead, and its entries go
    let newer = at(7, b'B');
    let rival = with_entry(newer, some_entry(newer));
    let mut rival_never = rival.clone();
    rival_never.ords.clear();
    let (sup, _) = join(&with, &rival, &o);
    let mut twin = join(&never, &rival_never, &o).0;
    assert_eq!(
        sup.ords.keys().map(|k| k.0).collect::<BTreeSet<_>>(),
        [newer].into(),
        "only the survivor's entry"
    );
    twin.ords.insert((newer, OrdAt::Field(Path::flat("x").unwrap())), some_entry(newer));
    assert_same_state(&sup, &twin, "after a supersession");
    assert!(sup.losses.keys().any(|k| k.kind == LossKind::InsertLost));

    // 3. an empty entry, which no writer makes, does not survive a join with anything (itself too)
    let mut empty = with.clone();
    empty.ords.clear();
    empty.ords.insert((life, OrdAt::Root), FieldOrd::default());
    empty.ords.insert((life, OrdAt::Field(Path::flat("x").unwrap())), FieldOrd::default());
    assert_ne!(empty, never, "as built, the empty entries are in the state");
    for other in [&never, &empty, &MergeState::default()] {
        let joined = join(&empty, other, &o).0;
        assert!(joined.ords.is_empty(), "the join removes the empty entries");
        let flipped = join(other, &empty, &o).0;
        assert_eq!(joined.ords, flipped.ords);
    }
    assert_same_state(
        &join(&empty, &never, &o).0,
        &join(&never, &never, &o).0,
        "after an empty entry",
    );
    // a join that leaves the order state as it was does not touch the bytes of one that has it
    assert_same_state(&join(&with, &with, &o).0, &with, "idempotent");
}

/// The decoder and the checks refuse an empty entry, in both spellings of the state.
#[test]
fn an_empty_entry_is_refused() {
    let life = at(1, b'A');
    let mut s = with_entry(life, some_entry(life));
    s.ords.insert((life, OrdAt::Root), FieldOrd::default());
    assert_eq!(s.check_decoded(), Err(DecodeError::OrdEntryEmpty));
    assert_eq!(MergeState::decode_compact(&s.encode_compact()), Err(DecodeError::OrdEntryEmpty));
    // a state that holds one is not idempotent under join, which is why it is refused
    assert_ne!(join(&s, &s, &opts(1_000_000_000)).0, s);
    // the single empty entry, spelled by hand: floor byte 0, no firsts, flags 0
    let mut only = with_entry(life, FieldOrd::default());
    only.regs.clear();
    assert_eq!(MergeState::decode_compact(&only.encode_compact()), Err(DecodeError::OrdEntryEmpty));
    // no order state at all is the state without a section
    only.ords.clear();
    let enc = only.encode_compact();
    assert_eq!(MergeState::decode_compact(&enc), Ok(only));
}

// ---- the join laws on what decode accepts -------------------------------------------------

/// A state of the life `(1, A)` with one order entry, at the root or at the path `x`, and a
/// version vector that covers every stamp of the universe below.
fn entry_state(root: bool, x: FieldOrd) -> MergeState {
    let life = at(1, b'A');
    let mut s = MergeState::default();
    s.lives.insert(life, Life { seen: BTreeSet::new(), order: Vec::new(), kind: LifeKind::Insert });
    for k in *b"ABC" {
        s.vv.insert(w(k), Hlc::new(40, 0));
    }
    let at_ = if root { OrdAt::Root } else { OrdAt::Field(Path::flat("x").unwrap()) };
    s.ords.insert((life, at_), x);
    s
}

/// An arbitrary order entry over three writers and stamps 1 to 12, honest in each part with a
/// probability and wrong otherwise: a first under another writer's stamp, a `last` before a
/// first, a `prev` not below the stamp, a position that differs between writes, a root with more
/// than a level mark, a level mark with no write behind it. It need not be normal.
fn arb_entry(rng: &mut Rng, root: bool) -> FieldOrd {
    let writer = |rng: &mut Rng| *rng.pick(b"ABC");
    let stamp = |rng: &mut Rng| at(1 + rng.below(12) as u64, writer(rng));
    let pos = |rng: &mut Rng, s: MStamp| {
        if rng.below(4) == 0 { rng.below(4) as u64 } else { super::hand_built::pos_of(s) }
    };
    let mut x = FieldOrd::default();
    if root && rng.below(10) < 7 {
        x.lvl = Some(stamp(rng));
        return x;
    }
    x.floor = match rng.below(4) {
        0 => {
            let s = stamp(rng);
            let prev = if rng.below(5) == 0 {
                rng.below(14) as u64
            } else {
                rng.below(s.hlc.wall_ms as usize) as u64
            };
            Some(Floor::U { stamp: s, prev: Hlc::new(prev, 0), others: rng.below(2) == 0 })
        }
        1 => Some(Floor::R { stamp: stamp(rng) }),
        _ => None,
    };
    for _ in 0..rng.below(4) {
        let key = writer(rng);
        let s = stamp(rng);
        let s = if rng.below(4) == 0 { s } else { MStamp { writer: w(key), ..s } };
        let p = pos(rng, s);
        x.firsts.insert(w(key), (s, p));
    }
    if rng.below(5) != 0 {
        let top = x.firsts.values().map(|c| c.0).max();
        let s = stamp(rng);
        let s = if rng.below(4) != 0 { s.max(top.unwrap_or(s)) } else { s };
        let p = pos(rng, s);
        x.last = Some((s, p));
    }
    if rng.below(3) == 0 {
        x.lvl = Some(stamp(rng));
    }
    if rng.below(10) < 7 {
        x = ord_join(&x, &x, &JoinOptions::default()); // normal, as a join leaves it
    }
    x
}

/// The pairwise conditions between the entries of one key that no two states of one history
/// break (item 22 of the inventory), each an instance of "one stamp is one write" or "a writer has
/// one history":
///
/// * where both hold a first for a writer at different stamps, the floor of the later voids the
///   earlier;
/// * a stamp is one kind of write: the same floor stamp is the same floor (kind, `prev`, bit), and
///   an unset's stamp is never a candidate (an unset names no position) in the other entry;
/// * a stamp names the path once: candidates at one stamp share one position.
fn states_agree(a: &FieldOrd, b: &FieldOrd, o: &JoinOptions) -> bool {
    let histories = a.firsts.iter().all(|(wr, ca)| match b.firsts.get(wr) {
        Some(cb) if cb > ca => !b.valid(ca, o),
        Some(cb) if cb < ca => !a.valid(cb, o),
        _ => true,
    });
    let floors = match (a.floor, b.floor) {
        (Some(fa), Some(fb)) if fa.stamp() == fb.stamp() => fa == fb,
        _ => true,
    };
    let cands = |x: &FieldOrd| x.firsts.values().copied().chain(x.last).collect::<Vec<_>>();
    let unset_is_not_a_candidate = |u: &FieldOrd, x: &FieldOrd| match u.floor {
        Some(Floor::U { stamp, .. }) => cands(x).iter().all(|c| c.0 != stamp),
        _ => true,
    };
    let positions =
        cands(a).iter().all(|ca| cands(b).iter().all(|cb| ca.0 != cb.0 || ca.1 == cb.1));
    histories
        && floors
        && unset_is_not_a_candidate(a, b)
        && unset_is_not_a_candidate(b, a)
        && positions
}

#[derive(Default, Debug)]
struct Laws {
    entries: usize,
    groups: usize,
    pairs: usize,
    triples: usize,
    /// triples whose three pairs agree on every writer's history
    agreeing: usize,
    not_associative: usize,
    not_associative_agreeing: usize,
}

/// Commutative and idempotent on every pair of the entries of a key, associative on every triple;
/// returns the counts, with the failures of associativity split by whether the three pairs agree
/// on the writers' histories. The first triple that fails while agreeing is returned (a real
/// bug of the join).
fn check_group(es: &[FieldOrd], o: &JoinOptions, laws: &mut Laws) -> Option<(usize, usize, usize)> {
    laws.groups += 1;
    laws.entries += es.len();
    let mut bad = None;
    for a in es {
        assert_eq!(ord_join(a, a, o), *a, "idempotent on a normal entry");
        for b in es {
            laws.pairs += 1;
            assert_eq!(ord_join(a, b, o), ord_join(b, a, o), "commutative");
        }
    }
    for (i, a) in es.iter().enumerate() {
        for (j, b) in es.iter().enumerate() {
            for (k, c) in es.iter().enumerate() {
                if i == j || j == k || i == k {
                    continue;
                }
                laws.triples += 1;
                let agree = states_agree(a, b, o) && states_agree(b, c, o) && states_agree(a, c, o);
                laws.agreeing += usize::from(agree);
                if ord_join(&ord_join(a, b, o), c, o) != ord_join(a, &ord_join(b, c, o), o) {
                    laws.not_associative += 1;
                    if agree {
                        laws.not_associative_agreeing += 1;
                        bad.get_or_insert((i, j, k));
                    }
                }
            }
        }
    }
    bad
}

/// The join laws on the order entries that DECODE ACCEPTS. Candidates: arbitrary entries (wrong in
/// each part with a probability), written as states in the compact form and decoded; damaged
/// encodings of states that carry an order state; the hand-built closed states; the causal
/// simulation. Counted: the candidates, the refusals by error, the accepted entries, the pairs and
/// triples. Commutative and idempotent on all of them. Associative on all that come from ONE
/// history (the closed hand-built chains, the simulation) and on every accepted triple whose pairs
/// agree on each writer's history; the accepted triples that fail are the cross-state case of item
/// 22 (`a_writer_cannot_have_two_histories...`), and none of them agrees.
#[test]
fn the_join_laws_hold_on_what_decode_accepts() {
    let o = JoinOptions::default();
    let mut rng = Rng(0x00AC_CE97_ED00);
    // arbitrary entries, through the codec
    let mut refused: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_key: BTreeMap<bool, Vec<FieldOrd>> = BTreeMap::new();
    let (mut candidates, mut accepted) = (0usize, 0usize);
    for _ in 0..60_000usize {
        let root = rng.below(5) == 0;
        let x = arb_entry(&mut rng, root);
        candidates += 1;
        match MergeState::decode_compact(&entry_state(root, x).encode_compact()) {
            Err(e) => *refused.entry(format!("{e:?}")).or_default() += 1,
            Ok(s) => {
                accepted += 1;
                let e = s.ords.into_values().next().unwrap();
                let g = by_key.entry(root).or_default();
                if !g.contains(&e) {
                    g.push(e);
                }
            }
        }
    }
    // damaged encodings of states that carry an order state
    let corpus = super::compact_tests::ord_corpus();
    let mut fuzz_by_key: BTreeMap<(MStamp, OrdAt), Vec<FieldOrd>> = BTreeMap::new();
    let (mut fuzz_inputs, mut fuzz_accepted) = (0usize, 0usize);
    for _ in 0..40_000 {
        let mut b = corpus[rng.below(corpus.len())].clone();
        for _ in 0..1 + rng.below(3) {
            let i = rng.below(b.len());
            match rng.below(4) {
                0 => b[i] ^= 1 << rng.below(8),
                1 => b[i] = *rng.pick(&[0u8, 1, 2, 3, 5]),
                2 => b[i] = b[i].wrapping_add(1),
                _ => {
                    let j = rng.below(b.len());
                    b.swap(i, j);
                }
            }
        }
        fuzz_inputs += 1;
        if let Ok(s) = MergeState::decode_compact(&b) {
            fuzz_accepted += 1;
            for (k, e) in s.ords {
                let g = fuzz_by_key.entry(k).or_default();
                if !g.contains(&e) {
                    g.push(e);
                }
            }
        }
    }
    eprintln!("candidates {candidates}: {accepted} accepted, refused {refused:?}");
    eprintln!("damaged encodings {fuzz_inputs}: {fuzz_accepted} accepted");
    // the arbitrary entries: laws, with the residual
    let mut arb = Laws::default();
    for es in by_key.values() {
        let es: Vec<FieldOrd> = es.iter().take(60).cloned().collect();
        if let Some(t) = check_group(&es, &o, &mut arb) {
            panic!(
                "an accepted triple whose pairs agree is not associative:\n a {}\n b {}\n c {}",
                brief(&es[t.0]),
                brief(&es[t.1]),
                brief(&es[t.2])
            );
        }
    }
    eprintln!("arbitrary accepted: {arb:?}");
    // the damaged encodings
    let mut fz = Laws::default();
    for es in fuzz_by_key.values().filter(|e| e.len() >= 2) {
        let es: Vec<FieldOrd> = es.iter().take(16).cloned().collect();
        assert!(check_group(&es, &o, &mut fz).is_none(), "{es:?}");
    }
    eprintln!("damaged accepted: {fz:?}");
    // one history: the closed hand-built states and the simulation
    use super::hand_built::{self, BUILDERS, Mode};
    let mut one = Laws::default();
    let mut hb: BTreeMap<(MStamp, OrdAt), Vec<FieldOrd>> = BTreeMap::new();
    let mut hrng = Rng(0x00C1_05ED_0001);
    for _ in 0..4000 {
        let c = hand_built::case(&mut hrng, Mode::Closed, BUILDERS.len() - 1, 1_000_000_000);
        for s in [c.s, c.t] {
            assert_eq!(s.check_decoded(), Ok(()));
            for (k, e) in s.ords {
                let g = hb.entry(k).or_default();
                if !g.contains(&e) {
                    g.push(e);
                }
            }
        }
    }
    let hb_groups = hb.values().filter(|e| e.len() >= 2).count();
    for es in hb.values() {
        assert_eq!(check_group(es, &o, &mut one), None, "{es:?}");
    }
    let mut sim = Laws::default();
    for seed in 1..=40 {
        let mut by_key: BTreeMap<(MStamp, OrdAt), Vec<FieldOrd>> = BTreeMap::new();
        for s in super::ord_tests::ord_sim(seed, 40, &o) {
            assert_eq!(s.check_decoded(), Ok(()), "a simulated state is accepted");
            for (k, e) in s.ords {
                let g = by_key.entry(k).or_default();
                if !g.contains(&e) {
                    g.push(e);
                }
            }
        }
        for es in by_key.values() {
            let es: Vec<FieldOrd> = es.iter().take(40).cloned().collect();
            assert_eq!(check_group(&es, &o, &mut sim), None);
            assert_eq!(sim.not_associative, 0, "one history is associative");
        }
    }
    eprintln!("hand-built closed: {one:?} ({hb_groups} keys with several)");
    eprintln!("simulation: {sim:?}");
    assert_eq!(one.not_associative + sim.not_associative, 0);
    // non-vacuity, and the counts pinned (the streams are seeded)
    let refused_total: usize = refused.values().sum();
    assert_eq!((candidates, accepted, refused_total), (60_000, 31_795, 28_205), "{refused:?}");
    assert_eq!(refused.len(), 8, "every clause that one state can show is reached: {refused:?}");
    assert_eq!((fuzz_inputs, fuzz_accepted), (40_000, 10_550));
    assert_eq!((arb.triples, arb.agreeing, arb.not_associative), (248_160, 195_960, 5_684));
    assert_eq!(
        arb.not_associative_agreeing, 0,
        "the accepted triples that fail are the cross-state ones"
    );
    assert_eq!((fz.triples, fz.agreeing, fz.not_associative), (159_204, 129_420, 508));
    assert_eq!((one.triples, one.not_associative), (4_266, 0));
    assert_eq!((sim.triples, sim.not_associative), (108_870, 0));
}

/// An entry in a line: `u5C/2` an unset floor at (5, C) remembering 2, `r` a replace, `F A8.2` the first of
/// A at (8, A) position 2, `L` last, `V` level mark.
fn brief(x: &FieldOrd) -> String {
    let st = |s: &MStamp| format!("{}{}", s.hlc.wall_ms, (s.writer.node.to_bytes()[0]) as char);
    let mut out = String::new();
    match x.floor {
        Some(Floor::U { stamp, prev, others }) => {
            out += &format!("u{}/{}{} ", st(&stamp), prev.wall_ms, if others { "+" } else { "" })
        }
        Some(Floor::R { stamp }) => out += &format!("r{} ", st(&stamp)),
        None => {}
    }
    for (wr, c) in &x.firsts {
        out += &format!("F{}:{}.{} ", wr.node.to_bytes()[0] as char, st(&c.0), c.1);
    }
    if let Some(c) = x.last {
        out += &format!("L{}.{} ", st(&c.0), c.1);
    }
    if let Some(l) = x.lvl {
        out += &format!("V{} ", st(&l));
    }
    out
}

/// THE CROSS-STATE CASE (item 22 of the inventory of `check_decoded`), kept as a pinned counterexample.
/// Three states hold the entries `{first of W at 5}`, `{first of W at 9}` and `{an unset at 7}` of one
/// key. Each alone is accepted by `decode`, and no single state shows what is wrong: the first two
/// cannot be states of ONE writer W (W's state at 9 holds W's write at 5, or a floor over it), so
/// they are two histories of one writer, which honest stamping and the transfer of whole states
/// rule out (one stamp is one write). The two ways of joining them differ. The test asserts
/// the associativity that holds for every triple of ONE history, so it fails, and is ignored.
#[test]
#[ignore = "cross-state: two histories of one writer; no single state shows it (inventory item 22)"]
fn a_writer_cannot_have_two_histories_but_decode_cannot_tell() {
    let o = JoinOptions::default();
    let a = FieldOrd {
        firsts: [(w(b'A'), (at(5, b'A'), 0))].into(),
        last: Some((at(5, b'A'), 0)),
        ..FieldOrd::default()
    };
    let b = FieldOrd {
        firsts: [(w(b'A'), (at(9, b'A'), 0))].into(),
        last: Some((at(9, b'A'), 0)),
        ..FieldOrd::default()
    };
    let c = FieldOrd { floor: Some(u(7, b'B', 6, false)), ..FieldOrd::default() };
    for x in [&a, &b, &c] {
        let enc = entry_state(false, x.clone()).encode_compact();
        assert!(MergeState::decode_compact(&enc).is_ok(), "each entry alone is accepted");
    }
    assert!(!states_agree(&a, &b, &o), "the pair that no history makes");
    assert_eq!(
        ord_join(&ord_join(&a, &b, &o), &c, &o),
        ord_join(&a, &ord_join(&b, &c, &o), &o),
        "associativity, which one history has"
    );
}

/// O1-3: the reviewer's example of a single-state shape that breaks associativity against reachable
/// partners: `{W: (5, W)}`, `{U(7, X)}` and a first KEYED by W but STAMPED by V, `{W: (9, V)}`.
/// Decode refuses the third (`OrdFirstNotByItsWriter`); were it accepted, the groupings would
/// disagree: `(a + c) + b` keeps no first for W, `a + (c + b)` keeps `(9, V)`.
#[test]
fn a_first_keyed_by_one_writer_and_stamped_by_another_is_refused() {
    let o = JoinOptions::default();
    let (wr, x, v) = (w(b'A'), b'B', b'C');
    let a = FieldOrd {
        firsts: [(wr, (at(5, b'A'), 0))].into(),
        last: Some((at(5, b'A'), 0)),
        ..FieldOrd::default()
    };
    let b = FieldOrd { floor: Some(u(7, x, 6, false)), ..FieldOrd::default() };
    let c = FieldOrd {
        firsts: [(wr, (at(9, v), 0))].into(),
        last: Some((at(9, v), 0)),
        ..FieldOrd::default()
    };
    for ok in [&a, &b] {
        assert!(
            MergeState::decode_compact(&entry_state(false, ok.clone()).encode_compact()).is_ok()
        );
    }
    assert_eq!(
        MergeState::decode_compact(&entry_state(false, c.clone()).encode_compact()),
        Err(DecodeError::OrdFirstNotByItsWriter)
    );
    assert_eq!(
        c.shape_error(&OrdAt::Field(Path::flat("x").unwrap())),
        Some(DecodeError::OrdFirstNotByItsWriter)
    );
    // what the refusal prevents
    let left = ord_join(&ord_join(&a, &c, &o), &b, &o);
    let right = ord_join(&a, &ord_join(&c, &b, &o), &o);
    assert!(left.firsts.is_empty(), "{left:?}");
    assert_eq!(right.firsts, [(wr, (at(9, v), 0))].into());
}

// ---- no panic from decoded input ----------------------------------------------------------

/// O1-8: the version 1 entry points are fallible and refuse a state with an order state, and nothing
/// a decoded state is given to panics: the reads, the join, every local write of a writer that holds
/// the state, and the receive of it. The states are the encodings of the corpus (the model's
/// scenarios and pooled runs with the order state) and those that damage of them still decodes to.
#[test]
fn nothing_panics_on_a_decoded_state_and_version_one_refuses_an_order_state() {
    let o = JoinOptions::default();
    let corpus = super::compact_tests::ord_corpus();
    let mut rng = Rng(0x00DE_C0DE_0001);
    let mut states: Vec<MergeState> =
        corpus.iter().map(|b| MergeState::decode_compact(b).unwrap()).collect();
    for _ in 0..4_000 {
        let mut b = corpus[rng.below(corpus.len())].clone();
        for _ in 0..1 + rng.below(3) {
            let i = rng.below(b.len());
            b[i] = if rng.below(2) == 0 {
                b[i] ^ (1 << rng.below(8))
            } else {
                *rng.pick(&[0u8, 1, 2, 3])
            };
        }
        if let Ok(s) = MergeState::decode_compact(&b) {
            states.push(s);
        }
    }
    let x = Path::flat("x").unwrap();
    let (mut with_ords, mut ops) = (0usize, 0usize);
    for (k, st) in states.iter().enumerate() {
        if !st.ords.is_empty() {
            with_ords += 1;
            assert_eq!(st.try_canonical_bytes(), Err(V1CannotHold));
            assert_eq!(st.try_state_hash(), Err(V1CannotHold));
            assert_eq!(st.try_encode(), Err(V1CannotHold));
        } else if st.v1_can_hold() {
            assert!(st.try_state_hash().is_ok() && st.try_encode().is_ok());
        }
        // the reads
        let _ = (
            st.visible(&o),
            st.ordered_document(&o),
            st.hidden_all(&o),
            st.over_caps(&o),
            st.excess(&o),
        );
        let _ =
            (st.value(&x, &o), st.alive(&o), st.writers(), st.compact_hash(), st.encode_compact());
        let _ = join(st, st, &o);
        // a writer that holds the state: every local write, then the receive of the state
        if k % 3 == 0 {
            let id = WriterId {
                node: crate::ids::NodeId::from_bytes([9; 16]),
                epoch: Epoch::from_u64(7),
            };
            let mut wr = Writer::new(id, o.clone());
            wr.state = st.clone();
            wr.clock = st.vv.values().copied().max().unwrap_or(Hlc::ZERO);
            let doc = [
                (Path::flat("x").unwrap(), Value::Int64(1)),
                (Path::flat("y").unwrap(), Value::Null),
            ];
            wr.write_set(&x, Value::Int64(3), 0);
            wr.write_set(&Path::new(["x", "z"]).unwrap(), Value::Int64(3), 0);
            wr.write_unset(&x, 0);
            wr.write_inc(&x, 5, 0);
            wr.write_replace(&doc, 0);
            wr.upsert_inc(&x, 1, 0);
            wr.write_delete(0);
            wr.insert(&doc, 0);
            let _ = wr.receive(st);
            ops += 1;
        }
    }
    eprintln!(
        "{} decoded states, {with_ords} with an order state, {ops} writers driven",
        states.len()
    );
    assert!(with_ords > 1_000 && ops > 300, "{with_ords} {ops}");
}

/// O1-2 of the O1 review, the honest case beyond retention. The model's nested run seed 1421 (60 steps,
/// retention 10, restore and snapshots on, the covered-entry skip off) reaches, at the key `((1, A),
/// a.d.g)`, three entries that are each an honest state's and not associative: `{first of C at 3}`,
/// `{first of C at 31}` and `{an unset of C at 8, prev 3}`. The trace: C writes the path at 3 and
/// unsets it at 8; a delete kills the life (its entries go with it); a join jumps the receiver's
/// horizon from -2 to 9, more than R ahead, which collects the delete and the dead mark, and an old
/// entry that is re-sent brings the life back with no entry at the path; C writes the path again at 31, a
/// first with no floor over its earlier one. This is the ADR-085 limit (the same mechanism makes the
/// state itself non-associative at a small retention: the NOTE of `prop_tests::check_laws`), and decode
/// cannot tell it from an honest state: each entry passes every check. The boundary of the order
/// join's associativity is therefore "within retention", not "reachable by a causal history".
/// `q7-logs/ord-goldens/o1_2_triple.py` (in the review files) prints the three entries from the model.
#[test]
fn an_honest_history_beyond_retention_reaches_the_non_associative_triple() {
    let o = JoinOptions::default();
    let c = b'C';
    let first = |hlc: u64| FieldOrd {
        firsts: [(w(c), (at(hlc, c), 0))].into(),
        last: Some((at(hlc, c), 0)),
        ..FieldOrd::default()
    };
    let (a, b) = (first(3), first(31));
    let unset = FieldOrd { floor: Some(u(8, c, 3, false)), ..FieldOrd::default() };
    for x in [&a, &b, &unset] {
        assert!(
            MergeState::decode_compact(&entry_state(false, x.clone()).encode_compact()).is_ok()
        );
    }
    assert!(!states_agree(&a, &b, &o), "the pair no history within retention makes");
    let left = ord_join(&ord_join(&a, &b, &o), &unset, &o);
    let right = ord_join(&a, &ord_join(&b, &unset, &o), &o);
    assert_ne!(left, right, "not associative");
    assert!(left.firsts.is_empty() && right.firsts == [(w(c), (at(31, c), 0))].into());
}
