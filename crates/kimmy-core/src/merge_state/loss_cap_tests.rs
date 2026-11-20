//! The cap and the cut of a document's loss records (Step 4 design 3.1 to 3.4, claims 1 and 2 of section 4): the rank
//! order, the cap as a semilattice operation that commutes with the horizon, what the join and `receive_loss` do with it
//! and count, the cut of a value, and the bounds the codecs refuse past.

use std::collections::{BTreeMap, BTreeSet};

use super::loss_cut::{cap_losses, cut_fields, cut_value, encoded_len, within_cut};
use super::prop_tests::Rng;
use super::tests::{h, i, wid};
use super::*;

fn key(kind: LossKind, path: &str, wall: u64, who: char) -> LossKey {
    LossKey::field(kind, Path::flat(path).unwrap(), MStamp { hlc: h(wall), writer: wid(who, 0) })
}

fn rec(wall: u64) -> (LossKey, LossValue) {
    (key(LossKind::Clash, "x", wall, 'A'), LossValue::Value(i(wall as i64)))
}

fn limits(cap: usize) -> JoinOptions {
    JoinOptions::default().with_loss_limits_unchecked(cap, 4096)
}

fn walls(m: &BTreeMap<LossKey, LossValue>) -> Vec<u64> {
    let mut w: Vec<u64> = m.keys().map(|k| k.stamp.hlc.wall_ms).collect();
    w.sort_unstable();
    w
}

// ---- the rank --------------------------------------------------------------------------------------------

#[test]
fn the_rank_is_the_losers_stamp_first_then_what_it_is_about_then_the_kind() {
    use std::cmp::Ordering::{Greater, Less};
    // the stamp first: a later stamp outranks whatever the rest says (the derived order would say the kind first)
    assert_eq!(
        key(LossKind::Clash, "z", 2, 'A').rank_cmp(&key(LossKind::ArrayClash, "a", 1, 'A')),
        Greater
    );
    // the writer breaks a tie of the clock
    assert_eq!(
        key(LossKind::Clash, "x", 5, 'A').rank_cmp(&key(LossKind::Clash, "x", 5, 'B')),
        Less
    );
    // one losing write's records: the path, then the kind
    assert_eq!(
        key(LossKind::Clash, "a", 5, 'A').rank_cmp(&key(LossKind::Clash, "b", 5, 'A')),
        Less
    );
    assert_eq!(
        key(LossKind::Clash, "a", 5, 'A').rank_cmp(&key(LossKind::TypeClash, "a", 5, 'A')),
        Less
    );
    assert_eq!(
        key(LossKind::Clash, "a", 5, 'A').rank_cmp(&key(LossKind::Clash, "a", 5, 'A')),
        std::cmp::Ordering::Equal
    );
}

#[test]
fn the_cap_keeps_the_greatest_by_rank_and_names_what_it_dropped() {
    let mut m: BTreeMap<_, _> = (1..=6).map(rec).collect();
    let dropped = cap_losses(&mut m, 4);
    assert_eq!(walls(&m), vec![3, 4, 5, 6]);
    assert_eq!(dropped.iter().map(|k| k.stamp.hlc.wall_ms).collect::<Vec<_>>(), vec![1, 2]);
    assert!(cap_losses(&mut m, 4).is_empty(), "under the cap, nothing");
}

// ---- claims 1 and 2, as properties -------------------------------------------------------------------------

fn random_records(rng: &mut Rng, n: usize) -> BTreeMap<LossKey, LossValue> {
    let kinds = [LossKind::Clash, LossKind::DeleteWon, LossKind::InsertLost, LossKind::TypeClash];
    let paths = ["a", "b", "c"];
    (0..n)
        .map(|_| {
            let k = key(
                *rng.pick(&kinds),
                rng.pick(&paths),
                1 + rng.below(12) as u64,
                *rng.pick(&['A', 'B']),
            );
            // one key, one value (the premise of the design: two members that hold one key hold one value)
            let v = LossValue::Value(i(k.stamp.hlc.wall_ms as i64 * 10 + k.kind as i64));
            (k, v)
        })
        .collect()
}

fn cap(mut m: BTreeMap<LossKey, LossValue>, k: usize) -> BTreeMap<LossKey, LossValue> {
    cap_losses(&mut m, k);
    m
}

fn union(
    a: &BTreeMap<LossKey, LossValue>,
    b: &BTreeMap<LossKey, LossValue>,
) -> BTreeMap<LossKey, LossValue> {
    a.iter().chain(b).map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn horizon_filter(m: &BTreeMap<LossKey, LossValue>, wall: u64) -> BTreeMap<LossKey, LossValue> {
    m.iter()
        .filter(|(k, _)| k.stamp.hlc.wall_ms >= wall)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Claim 1: `cap(cap(A) ∪ cap(B)) = cap(A ∪ B)`: capped sets joined in any order, with repeats, agree, so a dropped record
/// is never needed again.
#[test]
fn claim_1_the_cap_is_a_semilattice_operation() {
    let mut rng = Rng(0x00c1_a1b5);
    for k in 1..=6 {
        for _ in 0..400 {
            let (a, b) = (random_records(&mut rng, 8), random_records(&mut rng, 8));
            let whole = cap(union(&a, &b), k);
            assert_eq!(cap(union(&cap(a.clone(), k), &cap(b.clone(), k)), k), whole, "K={k}");
            // commutative, idempotent, and a repeat changes nothing
            assert_eq!(cap(union(&cap(b.clone(), k), &cap(a.clone(), k)), k), whole, "K={k}");
            assert_eq!(cap(union(&whole, &cap(a.clone(), k)), k), whole, "K={k}");
        }
    }
}

/// Claim 2: `cap(F_h(S)) = F_h(cap(S))`: the horizon removes a prefix of the rank order (its first component is the loser's
/// wall time), so a record the cap dropped because `K` greater ones exist does not come back when those age out.
#[test]
fn claim_2_the_horizon_commutes_with_the_cap() {
    let mut rng = Rng(0x00c1_a1b6);
    for k in 1..=6 {
        for _ in 0..400 {
            let s = random_records(&mut rng, 10);
            let wall = rng.below(14) as u64;
            assert_eq!(
                cap(horizon_filter(&s, wall), k),
                horizon_filter(&cap(s.clone(), k), wall),
                "K={k} h={wall}"
            );
        }
    }
}

/// The two claims at the join: states that hold only records, joined in any order and with repeats, with the horizon
/// applied at each join, end holding `cap(F_H(U))`, and no state ever holds more than `K`.
#[test]
fn the_join_of_capped_states_converges_whatever_the_order() {
    let mut rng = Rng(0x00c1_a1b7);
    for k in 1..=6 {
        let o = limits(k).with_retention(6);
        for _ in 0..150 {
            let states: Vec<MergeState> = (0..4)
                .map(|n| {
                    let mut st =
                        MergeState { losses: random_records(&mut rng, 6), ..MergeState::default() };
                    // the version vector names the newest write each state has seen: the horizon is its max minus `R`
                    st.vv.insert(wid('A', 0), h(12 + n));
                    st
                })
                .collect();
            let mut results = Vec::new();
            for order in
                [vec![0, 1, 2, 3], vec![3, 2, 1, 0], vec![1, 3, 0, 2], vec![2, 2, 0, 1, 3, 3]]
            {
                let mut acc = MergeState::default();
                for n in order {
                    acc = join(&acc, &states[n], &o).0;
                    assert!(acc.losses.len() <= k, "K={k}: never more than K");
                }
                // every member ends with the same horizon, as a last join with every state shows
                for st in &states {
                    acc = join(&acc, st, &o).0;
                }
                results.push(acc.losses);
            }
            assert!(results.windows(2).all(|w| w[0] == w[1]), "K={k}: {results:?}");
            // and it is cap(F_H(U))
            let all: BTreeMap<_, _> = states.iter().flat_map(|st| st.losses.clone()).collect();
            let horizon = (12 + 3) - 6;
            assert_eq!(results[0], cap(horizon_filter(&all, horizon), k), "K={k}");
        }
    }
}

// ---- what the join and receive_loss count ------------------------------------------------------------------

#[test]
fn the_join_counts_what_the_cap_refused_and_what_it_evicted_and_fresh_excludes_both() {
    let o = limits(2);
    let mut held = MergeState::default();
    held.losses.extend([rec(5), rec(6)]);
    // a newer record displaces a held one: evicted 1; fresh is the newer one
    let mut newer = MergeState::default();
    newer.losses.extend([rec(9)]);
    let (j, fresh, counts) = join_counted(&held, &newer, &o);
    assert_eq!((walls(&j.losses), counts.refused, counts.evicted), (vec![6, 9], 0, 1));
    assert_eq!(fresh, [rec(9).0].into_iter().collect());
    // an older record is refused on arrival: refused 1, and it is not fresh
    let mut older = MergeState::default();
    older.losses.extend([rec(2)]);
    let (j, fresh, counts) = join_counted(&held, &older, &o);
    assert_eq!((walls(&j.losses), counts.refused, counts.evicted), (vec![5, 6], 1, 0));
    assert!(fresh.is_empty(), "a record that never held a place is not fresh: {fresh:?}");
}

#[test]
fn receive_loss_says_why_it_did_not_add_a_record_and_how_many_it_displaced() {
    let o = limits(2).with_retention(100);
    let mut st = MergeState::default();
    st.vv.insert(wid('A', 0), h(200));
    assert_eq!(st.receive_loss_counted(rec(50).0, rec(50).1, &o), (LossReceive::Horizon, 0));
    assert_eq!(st.receive_loss_counted(rec(150).0, rec(150).1, &o), (LossReceive::Added, 0));
    assert_eq!(st.receive_loss_counted(rec(150).0, rec(150).1, &o), (LossReceive::Known, 0));
    assert_eq!(st.receive_loss_counted(rec(160).0, rec(160).1, &o), (LossReceive::Added, 0));
    // full: an older record is refused by the cap, a newer one is added and displaces one
    assert_eq!(st.receive_loss_counted(rec(120).0, rec(120).1, &o), (LossReceive::Cap, 0));
    assert_eq!(st.receive_loss_counted(rec(170).0, rec(170).1, &o), (LossReceive::Added, 1));
    assert_eq!(walls(&st.losses), vec![160, 170]);
    // a value over the cut is refused, not cut
    let big = LossValue::Value(Value::String("x".repeat(500)));
    let small = JoinOptions::default().with_loss_limits_unchecked(4, 64).with_retention(100);
    assert_eq!(
        st.receive_loss_counted(key(LossKind::Clash, "y", 190, 'A'), big, &small),
        (LossReceive::Oversize, 0)
    );
    // and the plain bool is "added"
    assert!(!st.receive_loss(rec(10).0, rec(10).1, &o));
}

#[test]
fn a_record_that_comes_in_over_the_cut_is_refused_by_the_join_and_counted() {
    let o = JoinOptions::default().with_loss_limits_unchecked(4, 64);
    let mut t = MergeState::default();
    t.losses.insert(
        key(LossKind::Clash, "x", 5, 'A'),
        LossValue::Value(Value::String("y".repeat(300))),
    );
    t.losses.insert(key(LossKind::Clash, "x", 6, 'A'), LossValue::Value(i(1)));
    let (j, fresh, counts) = join_counted(&MergeState::default(), &t, &o);
    assert_eq!((j.losses.len(), fresh.len(), counts.oversize), (1, 1, 1));
}

#[test]
fn the_limits_of_a_collection_are_refused_outside_their_bounds() {
    let ok = |c, u| JoinOptions::default().with_loss_limits(c, u).is_ok();
    assert!(ok(4, 64) && ok(64, 4096) && ok(10, 100));
    assert!(!ok(3, 64) && !ok(65, 64) && !ok(4, 63) && !ok(4, 4097));
    let o = JoinOptions::default().with_loss_limits(4, 64).unwrap();
    assert_eq!((o.loss_cap(), o.loss_cut()), (4, 64));
}

// ---- the cut -------------------------------------------------------------------------------------------------

fn text(n: usize) -> Value {
    Value::String("é".repeat(n))
}

#[test]
fn a_value_that_fits_is_kept_whole_and_one_that_does_not_is_cut_to_what_fits() {
    let cut = 64;
    assert_eq!(cut_value(&i(7), cut), LossValue::Value(i(7)));
    assert_eq!(
        cut_value(&Value::String("short".into()), cut),
        LossValue::Value(Value::String("short".into()))
    );
    for v in [
        text(500),
        Value::String("x".repeat(500)),
        Value::Binary { subtype: 0, bytes: vec![7; 500] },
        Value::Regex { pattern: "p".repeat(500), options: "i".into() },
        Value::Array((0..100).map(i).collect()),
        Value::Array(vec![Value::String("a".repeat(40)), Value::String("b".repeat(300))]),
    ] {
        let cutv = cut_value(&v, cut);
        assert!(matches!(cutv, LossValue::CutValue(_)), "{v:?} is cut");
        assert!(encoded_len(&cutv) <= cut, "{v:?}: {} bytes", encoded_len(&cutv));
        assert!(within_cut(&cutv, cut));
        // a function of the value and the cut only: two members cut it to equal bytes
        assert_eq!(cut_value(&v, cut), cutv);
        // and the cut value is a prefix of the value, never something else
        match (&v, &cutv) {
            (Value::String(a), LossValue::CutValue(Value::String(b))) => {
                assert!(a.starts_with(b.as_str()))
            }
            (
                Value::Binary { bytes: a, .. },
                LossValue::CutValue(Value::Binary { bytes: b, .. }),
            ) => assert!(a.starts_with(b)),
            (Value::Array(a), LossValue::CutValue(Value::Array(b))) => {
                assert_eq!(&a[..b.len()], &b[..])
            }
            (
                Value::Regex { pattern: a, options: oa },
                LossValue::CutValue(Value::Regex { pattern: b, options: ob }),
            ) => {
                assert!(a.starts_with(b.as_str()) && oa == ob)
            }
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn a_string_is_cut_on_a_char_boundary_as_long_as_it_can_be() {
    // 'é' is two bytes: the cut must never split one, and takes as many as fit
    let LossValue::CutValue(Value::String(kept)) = cut_value(&text(500), 64) else { panic!() };
    assert!(kept.chars().all(|c| c == 'é'));
    assert!(encoded_len(&LossValue::CutValue(Value::String(kept.clone()))) <= 64);
    let one_more = Value::String("é".repeat(kept.chars().count() + 1));
    assert!(encoded_len(&LossValue::CutValue(one_more)) > 64, "one more char would not fit");
}

#[test]
fn an_array_is_cut_to_whole_elements_and_the_empty_array_if_the_first_is_too_big() {
    let LossValue::CutValue(Value::Array(kept)) =
        cut_value(&Value::Array((0..100).map(i).collect()), 64)
    else {
        panic!()
    };
    assert!(!kept.is_empty() && kept.len() < 100);
    assert_eq!(kept, (0..kept.len() as i64).map(i).collect::<Vec<_>>());
    let huge_first = Value::Array(vec![Value::String("z".repeat(900)), i(1)]);
    assert_eq!(cut_value(&huge_first, 64), LossValue::CutValue(Value::Array(vec![])));
}

#[test]
fn a_field_order_is_cut_to_its_leading_paths() {
    let fields: Vec<Path> =
        (0..200).map(|n| Path::flat(&format!("field{n:03}")).unwrap()).collect();
    assert!(matches!(cut_fields(&fields[..3], 64), LossValue::Fields(_)));
    let LossValue::CutFields(kept) = cut_fields(&fields, 64) else { panic!() };
    assert!(!kept.is_empty() && kept.len() < 200 && kept == fields[..kept.len()]);
    assert!(encoded_len(&LossValue::CutFields(kept)) <= 64);
}

#[test]
fn the_join_cuts_the_value_of_a_record_it_makes() {
    // A's long string loses to B's: the clash record keeps A's string cut to the collection's cut
    let o = JoinOptions::default().with_loss_limits_unchecked(4, 64);
    let long = Value::String("w".repeat(400));
    let (mut a, mut b) = (super::tests::Mem::new('A', &o), super::tests::Mem::new('B', &o));
    let i0 = a.ins(&[("x", i(0))], 0).unwrap();
    b.receive_entry(&a.ent(i0));
    let wa = a.set("x", long.clone(), 0).unwrap();
    b.w.clock = h(9);
    let wb = b.set("x", i(2), 0).unwrap();
    let mut obs = super::tests::fresh_observer(&o, &[a.ent(i0), a.ent(wa), b.ent(wb)]);
    obs.receive_entry(&b.ent(wb));
    let st = &obs.w.state;
    let (k, v) = st.losses.iter().next().expect("a clash record");
    assert_eq!(k.kind, LossKind::Clash);
    assert!(matches!(v, LossValue::CutValue(Value::String(s)) if s.len() < 400), "{v:?}");
}

// ---- the codecs ----------------------------------------------------------------------------------------------

fn with_records(n: usize, value: impl Fn(usize) -> LossValue) -> MergeState {
    let mut st = MergeState::default();
    for k in 0..n {
        st.losses.insert(key(LossKind::Clash, "x", 1 + k as u64, 'A'), value(k));
    }
    st
}

#[test]
fn the_cut_variants_round_trip_in_both_codecs_and_a_whole_document_order_stays_whole() {
    let mut st = MergeState::default();
    st.losses
        .insert(key(LossKind::Clash, "a", 1, 'A'), LossValue::CutValue(Value::String("ab".into())));
    st.losses.insert(
        key(LossKind::TypeClash, "b", 2, 'A'),
        LossValue::CutValue(Value::Array(vec![i(1), i(2)])),
    );
    st.losses.insert(
        LossKey::document(LossKind::InsertLost, MStamp { hlc: h(3), writer: wid('A', 0) }),
        LossValue::CutFields(vec![Path::flat("f").unwrap()]),
    );
    let compact = MergeState::decode_compact(&st.encode_compact()).unwrap();
    assert_eq!(compact.losses, st.losses);
    let v1 = MergeState::decode(&st.encode()).unwrap();
    assert_eq!(v1.losses, st.losses);
    // the body of a loss entry carries it too
    for (k, v) in &st.losses {
        assert_eq!(
            MergeState::decode_loss_record(&MergeState::encode_loss_record(k, v)).unwrap(),
            (k.clone(), v.clone())
        );
    }
}

#[test]
fn a_decode_refuses_more_records_than_a_document_keeps_and_a_value_longer_than_a_cut_can_leave() {
    let ok = with_records(MAX_LOSS_CAP, |k| LossValue::Value(i(k as i64)));
    assert!(MergeState::decode_compact(&ok.encode_compact()).is_ok());
    assert!(MergeState::decode(&ok.encode()).is_ok());
    let many = with_records(MAX_LOSS_CAP + 1, |k| LossValue::Value(i(k as i64)));
    assert_eq!(
        MergeState::decode_compact(&many.encode_compact()),
        Err(DecodeError::TooManyLossRecords)
    );
    assert_eq!(MergeState::decode(&many.encode()), Err(DecodeError::TooManyLossRecords));
    let big = with_records(1, |_| LossValue::Value(Value::String("b".repeat(MAX_LOSS_CUT + 10))));
    assert_eq!(
        MergeState::decode_compact(&big.encode_compact()),
        Err(DecodeError::LossValueTooLarge)
    );
    assert_eq!(MergeState::decode(&big.encode()), Err(DecodeError::LossValueTooLarge));
    let at_the_bound =
        with_records(1, |_| cut_value(&Value::String("b".repeat(9000)), MAX_LOSS_CUT));
    assert!(MergeState::decode_compact(&at_the_bound.encode_compact()).is_ok());
}

#[test]
fn a_state_with_a_cut_record_hashes_loss_free_as_one_without_it() {
    // the loss-free hash excludes the records, whatever their value shape (the C3b header is unchanged)
    let bare = MergeState::default();
    let mut cut = bare.clone();
    cut.losses
        .insert(key(LossKind::Clash, "a", 1, 'A'), LossValue::CutValue(Value::String("ab".into())));
    assert_eq!(cut.loss_free_hash(), bare.loss_free_hash());
}

#[test]
fn the_cap_and_the_cut_are_a_function_of_the_collection_not_of_the_state_that_holds_the_records() {
    // two members, one set of records, one cap: the same records whichever joined them first
    let o = limits(3);
    let mut a = MergeState::default();
    a.losses.extend((1..=5).map(rec));
    let mut b = MergeState::default();
    b.losses.extend((4..=8).map(rec));
    let ab = join(&a, &b, &o).0.losses;
    let ba = join(&b, &a, &o).0.losses;
    assert_eq!(ab, ba);
    assert_eq!(walls(&ab), vec![6, 7, 8]);
    let _ = BTreeSet::<u8>::new();
}
