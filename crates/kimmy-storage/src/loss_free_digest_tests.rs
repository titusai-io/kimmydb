//! A loss record moves neither the digest term nor the etag of a record: both hash the state without its records.

use kimmy_core::merge_state::{
    LossKey, LossKind, LossValue, MStamp, MergeState, Path, Value, Writer, WriterId,
};
use kimmy_core::{DocRecord, DocVersion, Hlc, NodeId, Stamp};

use crate::codec;
use crate::live_digest;

fn id(n: u8) -> NodeId {
    NodeId::from_bytes([n; 16])
}

/// Two stored forms of one document, the second with a loss record in its state.
fn a_record_and_the_same_with_a_loss() -> (Vec<u8>, Vec<u8>, Stamp) {
    let writer_id = WriterId { node: id(1), epoch: kimmy_core::Epoch::ZERO };
    let opts = kimmy_core::merge_state::JoinOptions::new(1_000_000, i64::MAX).unwrap();
    let mut writer = Writer::new(writer_id, opts.clone());
    writer.insert(&[(Path::flat("x").unwrap(), Value::Int64(1))], 0).unwrap();
    let bare = writer.state.clone();
    let mut recorded = bare.clone();
    let loser = MStamp { hlc: Hlc::new(1, 0), writer: writer_id };
    assert!(recorded.receive_loss(
        LossKey::field(LossKind::Clash, Path::flat("x").unwrap(), loser),
        LossValue::Value(Value::Int64(5)),
        &opts
    ));
    let stamp = Stamp::new(Hlc::new(2, 0), id(1), kimmy_core::Epoch::ZERO);
    let encode = |state: &MergeState| {
        let mut record = DocRecord::live(
            stamp,
            bson::serialize_to_vec(&bson::doc! { "_id": 1, "x": 1 }).unwrap(),
        );
        record.meta = Some(state.encode_compact());
        codec::encode_doc_record(&record)
    };
    (encode(&bare), encode(&recorded), stamp)
}

#[test]
fn the_digest_term_of_a_record_does_not_see_its_loss_records() {
    let (bare, recorded, _) = a_record_and_the_same_with_a_loss();
    let key = b"k";

    let (a, b) =
        (codec::doc_record_view(&bare).unwrap(), codec::doc_record_view(&recorded).unwrap());

    assert_eq!(a.state, b.state, "the state the view hashes leaves the records out");
    assert_eq!(live_digest::term_of(key, &a), live_digest::term_of(key, &b), "so the term is one");
}

#[test]
fn the_etag_of_a_record_does_not_see_its_loss_records() {
    let (bare, recorded, stamp) = a_record_and_the_same_with_a_loss();
    let version = |raw: &[u8]| {
        codec::doc_record_version(&codec::decode_doc_record_with_meta(raw).unwrap(), raw)
    };

    assert_eq!(
        version(&bare),
        version(&recorded),
        "a client holding the one etag may write over the other"
    );
    assert!(
        matches!(version(&bare), DocVersion::Etag(s, _) if s == stamp),
        "and it is an etag of the state"
    );
    assert_eq!(codec::doc_record_state_hash(&bare), codec::doc_record_state_hash(&recorded));
}

#[test]
fn the_digest_format_is_the_one_that_leaves_records_out() {
    // The terms of rows written under the format before, which hashed the records, are not trusted: the mark moves.
    assert_eq!(live_digest::FORMAT, 4, "the digest format moved when what a term hashes did");
}

#[test]
fn the_check_of_a_conditional_write_and_the_version_of_an_entry_do_not_see_loss_records() {
    let (bare, recorded, stamp) = a_record_and_the_same_with_a_loss();
    let held = |raw: &[u8]| codec::decode_doc_record_with_meta(raw).unwrap();

    assert_eq!(
        crate::docs::state_hash_of(Some(&held(&bare))),
        crate::docs::state_hash_of(Some(&held(&recorded))),
        "what an If-Match compares with the etag it was given"
    );
    let entry = |raw: &[u8]| kimmy_core::OplogEntry {
        stamp,
        kind: kimmy_core::OpKind::Replace,
        collection: kimmy_core::CollectionId(1),
        doc_id: Some(kimmy_core::DocId::Int64(1)),
        body: None,
        meta: held(raw).meta,
    };
    assert_eq!(
        crate::docs::version_of_entry(&entry(&bare)),
        crate::docs::version_of_entry(&entry(&recorded)),
        "the version an entry's write produced"
    );
}
