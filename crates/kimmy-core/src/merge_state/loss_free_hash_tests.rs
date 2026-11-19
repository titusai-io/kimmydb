//! The state hash without its loss records ([`MergeState::loss_free_hash`]): what the digest term and the etag of
//! a record hash, so that a loss record neither moves the digest nor fails a conditional write.

use super::prop_tests::opts;
use super::tests::{fl, i, p, wid};
use super::*;

/// A state with a live document, and the same one with a loss record.
fn with_and_without_a_record() -> (MergeState, MergeState) {
    let mut writer = Writer::new(wid('A', 0), opts(1_000));
    writer.insert(&fl(&[("x", i(1))]), 0).unwrap();
    let bare = writer.state.clone();
    let mut recorded = bare.clone();
    let loser = *recorded.lives.keys().next().unwrap();
    let key = LossKey::field(LossKind::Clash, p("x"), loser);
    assert!(
        recorded.receive_loss(key, LossValue::Value(i(9)), &opts(1_000)),
        "premise: a record was added"
    );
    (bare, recorded)
}

#[test]
fn states_that_differ_only_in_their_loss_records_have_one_loss_free_hash() {
    let (bare, recorded) = with_and_without_a_record();

    assert_ne!(
        bare.compact_hash(),
        recorded.compact_hash(),
        "premise: the full hash sees the record"
    );
    assert_eq!(bare.loss_free_hash(), recorded.loss_free_hash(), "the loss-free hash does not");
    assert_eq!(
        MergeState::loss_free_hash_of(&bare.encode_compact()),
        MergeState::loss_free_hash_of(&recorded.encode_compact()),
        "nor does the one read from the bytes"
    );
}

#[test]
fn the_loss_free_hash_still_sees_everything_else() {
    let (bare, recorded) = with_and_without_a_record();
    let mut writer = Writer::from_state(wid('A', 0), recorded.clone(), opts(1_000));
    writer.write_set(&p("x"), i(2), 0).unwrap();

    assert_ne!(writer.state.loss_free_hash(), recorded.loss_free_hash(), "a write moves it");
    let mut moved_vv = bare.clone();
    moved_vv.vv.insert(wid('B', 0), crate::hlc::Hlc::new(50, 0));
    assert_ne!(
        moved_vv.loss_free_hash(),
        bare.loss_free_hash(),
        "a version vector component moves it"
    );
}

#[test]
fn the_hash_read_from_the_bytes_is_the_hash_of_the_state() {
    let (bare, recorded) = with_and_without_a_record();
    for state in [bare.clone(), recorded, MergeState::default()] {
        assert_eq!(
            MergeState::loss_free_hash_of(&state.encode_compact()),
            Ok(state.loss_free_hash())
        );
    }
    assert_eq!(
        bare.loss_free_hash(),
        bare.compact_hash(),
        "with no record there is nothing to leave out"
    );
    assert!(MergeState::loss_free_hash_of(&[1, 2, 3]).is_err(), "bytes that are not a state");
    let mut cut = bare.encode_compact();
    cut.pop();
    assert!(MergeState::loss_free_hash_of(&cut).is_err(), "a state cut short");
}
