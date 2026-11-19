//! The state hash without its loss records ([`MergeState::loss_free_hash`]): what the digest term and the etag of
//! a record hash, so that a loss record neither moves the digest nor fails a conditional write.

use super::codec::DecodeError;
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
    assert!(MergeState::decode_compact(&cut).is_err(), "a state cut short is not a state");
}

#[test]
fn a_state_with_no_record_is_written_as_version_2_exactly_as_before_and_one_with_records_as_version_3()
 {
    let (bare, recorded) = with_and_without_a_record();

    let plain = bare.encode_compact();
    assert_eq!(plain[3], 2, "no record: version 2");
    assert_eq!(
        &plain[4..],
        bare.compact_bytes().as_slice(),
        "and the body follows the version byte, as it always did"
    );

    let with = recorded.encode_compact();
    assert_eq!(with[3], 3, "records: version 3");
    assert_eq!(
        &with[4..20],
        recorded.loss_free_hash().as_slice(),
        "the loss-free hash is in the header"
    );
    assert_eq!(
        &with[20..],
        recorded.compact_bytes().as_slice(),
        "and the body is the one version 2 has"
    );
    assert_eq!(
        MergeState::compact_hash_of(&with),
        Ok(recorded.compact_hash()),
        "the full hash is of the body alone"
    );
    assert_eq!(MergeState::compact_hash_of(&plain), Ok(bare.compact_hash()));
    assert_eq!(MergeState::decode_compact(&with), Ok(recorded));
}

#[test]
fn the_loss_free_hash_of_bytes_is_read_with_no_decode() {
    let (bare, recorded) = with_and_without_a_record();
    // A header that is right over a body that is not a state: the hash is answered, which a decode could not.
    let mut plain = bare.encode_compact()[..4].to_vec();
    plain.extend_from_slice(&[0xff; 9]);
    assert!(MergeState::decode_compact(&plain).is_err(), "premise: the body is not a state");
    assert!(
        MergeState::loss_free_hash_of(&plain).is_ok(),
        "version 2: the hash of the body, with no decode"
    );

    let mut with = recorded.encode_compact()[..20].to_vec();
    let stated = with[4..20].to_vec();
    with.extend_from_slice(&[0xff; 9]);
    assert!(MergeState::decode_compact(&with).is_err(), "premise: the body is not a state");
    assert_eq!(
        MergeState::loss_free_hash_of(&with).unwrap().as_slice(),
        stated.as_slice(),
        "version 3: the header's hash, with no decode"
    );
}

#[test]
fn a_decode_refuses_a_version_that_disagrees_with_its_records() {
    let (bare, recorded) = with_and_without_a_record();

    // Version 2 over a state that holds a record: the header's hash taken out and the byte set back.
    let mut v2_with_records = recorded.encode_compact();
    v2_with_records.drain(4..20);
    v2_with_records[3] = 2;
    assert_eq!(
        MergeState::decode_compact(&v2_with_records),
        Err(DecodeError::VersionDisagreesWithRecords),
        "version 2 over records"
    );

    // Version 3 over a state that holds none.
    let mut v3_without = bare.encode_compact();
    v3_without[3] = 3;
    for byte in bare.loss_free_hash().iter().rev() {
        v3_without.insert(4, *byte);
    }
    assert_eq!(
        MergeState::decode_compact(&v3_without),
        Err(DecodeError::VersionDisagreesWithRecords),
        "version 3 over no record"
    );
}

#[test]
fn a_decode_refuses_a_header_hash_that_is_not_the_hash_of_the_state_without_its_records() {
    let (_, recorded) = with_and_without_a_record();
    let mut lying = recorded.encode_compact();
    lying[4] ^= 1;

    assert_eq!(
        MergeState::decode_compact(&lying),
        Err(DecodeError::LossFreeHashMismatch),
        "a peer that sends a state names the etag of the document: it may not name a false one"
    );
}
