//! Two contracts of the loss records that the design note pins (Step 4, v4, 7.2 and 7.4): a record can name a write
//! that is alive and is kept, and `retiredWriter` is reserved: nothing emits it.

use super::hand_built::{self, BUILDERS, Mode};
use super::prop_tests::Rng;
use super::tests::{Mem, h, i, opts};
use super::*;

fn p(x: &str) -> Path {
    Path::flat(x).unwrap()
}

/// 7.2: a converged state can hold a live register and an `insertLost` record that names it (a join before the floor
/// was known, then the filter). The record is kept, not dropped: deleting it would make the record set non-monotone
/// (the cap and the horizon are prefix removals), and suppressing it at delivery needs a rule about when the loser's
/// cell is alive that the model does not have. A `conflictResolved` can therefore name a write that is alive.
#[test]
fn a_record_that_names_a_live_register_is_kept_by_the_state_the_join_and_the_codecs() {
    let o = opts();
    let mut a = Mem::new('A', &o);
    a.w.clock = h(5);
    let write = a.ins(&[("x", i(1))], 0).unwrap();
    let key = LossKey::field(LossKind::InsertLost, p("x"), write);
    let mut stale = a.w.state.clone();
    assert!(
        stale.regs.iter().any(|((_, at), r)| *at == p("x") && r.stamp == write),
        "premise: x is live"
    );
    assert!(
        stale.receive_loss(key.clone(), LossValue::Value(i(1)), &o),
        "the record is accepted beside it"
    );

    // the document still shows the register, and the record is still there
    let visible = |s: &MergeState| s.visible(&o);
    assert_eq!(visible(&stale).unwrap()[&p("x")], i(1));
    assert!(stale.losses.contains_key(&key));
    // a join with the state the record was never in (either way round) keeps it and does not move the document
    let (joined, _) = join(&stale, &a.w.state, &o);
    assert!(joined.losses.contains_key(&key), "the join keeps it");
    assert_eq!(visible(&joined), visible(&stale));
    let (back, _) = join(&a.w.state, &stale, &o);
    assert!(back.losses.contains_key(&key), "and in the other order");
    // both codecs keep it
    let compact = MergeState::decode_compact(&stale.encode_compact()).unwrap();
    assert!(compact.losses.contains_key(&key), "the compact form keeps it");
    let v1 = MergeState::decode(&stale.encode()).unwrap();
    assert!(v1.losses.contains_key(&key), "the version 1 form keeps it");
}

/// 7.4: `retiredWriter` (kind tag 3) is reserved and emitted by nothing. The join of any two states, whatever they
/// hold (the hand-built corpus in all three modes, with the reserved kind taken out of the inputs, which the corpus
/// adds on purpose to the coherent states), makes no record of that kind, in its result or among the records it
/// reports as new.
#[test]
fn nothing_emits_a_retired_writer_record() {
    let mut rng = Rng(0x07e7_12ed_f00d);
    let (mut joins, mut records) = (0usize, 0usize);
    for mode in [Mode::Any, Mode::Coherent, Mode::Closed] {
        for builder in 0..BUILDERS.len() {
            for ret in [1_000_000_000u64, 40] {
                for _ in 0..40 {
                    let c = hand_built::case(&mut rng, mode, builder, ret);
                    let (mut s, mut t) = (c.s, c.t);
                    for x in [&mut s, &mut t] {
                        x.losses.retain(|k, _| k.kind != LossKind::RetiredWriter);
                    }
                    let o = JoinOptions::new(ret, i64::MAX).unwrap();
                    let (joined, fresh) = join(&s, &t, &o);
                    joins += 1;
                    records += joined.losses.len();
                    assert!(
                        joined
                            .losses
                            .keys()
                            .chain(fresh.iter())
                            .all(|k| k.kind != LossKind::RetiredWriter),
                        "mode {mode:?}, builder {builder}, retention {ret}: the join made a retiredWriter record"
                    );
                }
            }
        }
    }
    assert!(
        joins > 1000 && records > 500,
        "premise: the corpus made records to judge ({joins} joins, {records} records)"
    );
}
