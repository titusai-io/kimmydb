//! The body of a loss-record entry ([`MergeState::encode_loss_record`]): the compact form of a state that holds
//! one record and nothing else, so it round-trips what a state spells and refuses what a state refuses.

use super::counter::Num;
use super::prop_tests::opts;
use super::tests::{fl, h, i, p, s, wid};
use super::*;

fn stamp(wall: u64) -> MStamp {
    MStamp { hlc: h(wall), writer: wid('A', 3) }
}

fn every_kind() -> [LossKind; 6] {
    [
        LossKind::Clash,
        LossKind::DeleteWon,
        LossKind::InsertLost,
        LossKind::RetiredWriter,
        LossKind::TypeClash,
        LossKind::ArrayClash,
    ]
}

fn every_value() -> Vec<LossValue> {
    vec![
        LossValue::Value(i(7)),
        LossValue::Value(s("lost")),
        LossValue::Total(Num::Int64(-5)),
        LossValue::Fields(vec![p("a"), p("b")]),
    ]
}

#[test]
fn every_kind_place_and_value_round_trips_through_the_body() {
    for kind in every_kind() {
        for at in [LossAt::Document, LossAt::Field(p("x")), LossAt::Field(p("a.b"))] {
            for value in every_value() {
                let key = LossKey { kind, at: at.clone(), stamp: stamp(40) };
                let bytes = MergeState::encode_loss_record(&key, &value);
                assert_eq!(
                    MergeState::decode_loss_record(&bytes),
                    Ok((key.clone(), value.clone())),
                    "{kind:?} at {at:?} holding {value:?}"
                );
            }
        }
    }
}

#[test]
fn a_whole_document_and_a_field_named_underscore_are_two_bodies() {
    // The version 1 form spells both as the segment `_`; the body does not.
    let whole = LossKey::document(LossKind::InsertLost, stamp(1));
    let field = LossKey::field(LossKind::InsertLost, p("_"), stamp(1));
    let value = LossValue::Fields(vec![p("x")]);
    assert_ne!(
        MergeState::encode_loss_record(&whole, &value),
        MergeState::encode_loss_record(&field, &value)
    );
}

#[test]
fn bytes_that_are_not_exactly_one_record_are_refused() {
    let key = LossKey::field(LossKind::Clash, p("x"), stamp(40));
    let value = LossValue::Value(i(1));
    let good = MergeState::encode_loss_record(&key, &value);
    assert!(MergeState::decode_loss_record(&good).is_ok(), "premise");

    for cut in 0..good.len() {
        assert!(MergeState::decode_loss_record(&good[..cut]).is_err(), "a body cut at {cut} bytes");
    }
    let mut trailing = good.clone();
    trailing.push(0);
    assert!(MergeState::decode_loss_record(&trailing).is_err(), "a trailing byte");

    assert!(
        MergeState::decode_loss_record(&MergeState::default().encode_compact()).is_err(),
        "a state with no record"
    );
    let mut two = MergeState::default();
    two.losses.insert(key.clone(), value.clone());
    two.losses.insert(LossKey::field(LossKind::Clash, p("y"), stamp(41)), value.clone());
    assert!(MergeState::decode_loss_record(&two.encode_compact()).is_err(), "two records");

    let mut writer = Writer::new(wid('A', 3), opts(10));
    writer.insert(&fl(&[("x", i(1))]), 0).unwrap();
    let mut with_a_life = writer.state.clone();
    with_a_life.losses.insert(key, value);
    assert!(
        MergeState::decode_loss_record(&with_a_life.encode_compact()).is_err(),
        "a record beside a document"
    );
}

#[test]
fn a_total_no_writer_produces_is_refused_in_a_body_as_in_a_state() {
    let key = LossKey::field(LossKind::DeleteWon, p("n"), stamp(40));
    let past = LossValue::Total(Num::Int64(super::state::MAX_TOTAL + 1));
    let mut bytes = MergeState::default();
    bytes.losses.insert(key, past);
    assert!(
        MergeState::decode_loss_record(&bytes.encode_compact()).is_err(),
        "the decode refuses a total past the bound, and so does the body"
    );
}
