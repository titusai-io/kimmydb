//! The caps a client write is held to where it is stored (ADR-250): the final
//! document at most 16 MiB, an `_id` at most 4 KiB, on insert, replace and every
//! update's result. A write that changes nothing is never refused.

use std::cmp::Ordering;

use bson::{Document, doc};
use kimmy_core::limits::{MAX_DOCUMENT_BYTES, MAX_ID_BYTES};
use kimmy_core::{DocId, Error as CoreError};

use crate::{Engine, ModifySpec, StorageError};

fn open(dir: &tempfile::TempDir) -> Engine {
    Engine::open(&dir.path().join("kimmy.redb")).unwrap()
}

fn core(err: StorageError) -> CoreError {
    match err {
        StorageError::Core(inner) => inner,
        other => panic!("not a core refusal: {other:?}"),
    }
}

/// Sets `pad` to a string of `len` bytes on every document.
struct Pad(usize);

impl ModifySpec for Pad {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        Ok(true)
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.insert("pad", "x".repeat(self.0));
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

#[test]
fn an_id_over_the_cap_is_refused_on_insert_and_replace_and_one_at_the_cap_is_stored() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(&dir);
    let c = e.create_collection("shop", "c").unwrap();

    let at_cap = "a".repeat(MAX_ID_BYTES);
    e.insert(&c, doc! { "_id": at_cap.clone() }).unwrap();
    let over = "a".repeat(MAX_ID_BYTES + 1);
    let refused = core(e.insert(&c, doc! { "_id": over.clone() }).unwrap_err());
    assert!(matches!(refused, CoreError::IdTooLarge { bytes, .. } if bytes == MAX_ID_BYTES + 1));
    let refused = e.insert_many(&c, vec![doc! { "_id": "fine" }, doc! { "_id": over.clone() }]);
    assert!(refused.is_err());
    assert!(
        e.get(&c, &DocId::String("fine".into())).unwrap().is_none(),
        "the batch is refused whole"
    );
    // Replace, as an upsert, names the id by its argument.
    let err = e.replace(&c, &DocId::String(over), doc! { "v": 1 }, true).unwrap_err();
    assert!(matches!(core(err), CoreError::IdTooLarge { .. }));
    assert!(e.get(&c, &DocId::String(at_cap)).unwrap().is_some());
}

#[test]
fn a_document_over_the_cap_is_refused_on_insert_replace_and_update_and_a_shrinking_write_passes() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(&dir);
    let c = e.create_collection("shop", "c").unwrap();
    let id = DocId::Int64(1);

    let err =
        e.insert(&c, doc! { "_id": 1i64, "pad": "x".repeat(MAX_DOCUMENT_BYTES) }).unwrap_err();
    assert!(
        matches!(core(err), CoreError::DocumentTooLarge { max, .. } if max == MAX_DOCUMENT_BYTES)
    );
    assert!(e.get(&c, &id).unwrap().is_none());

    e.insert(&c, doc! { "_id": 1i64, "pad": "x".repeat(1024) }).unwrap();
    let err =
        e.replace(&c, &id, doc! { "pad": "x".repeat(MAX_DOCUMENT_BYTES) }, false).unwrap_err();
    assert!(matches!(core(err), CoreError::DocumentTooLarge { .. }));
    let err =
        e.modify_where(&c, &crate::Candidates::Scan, &Pad(MAX_DOCUMENT_BYTES), None).unwrap_err();
    assert!(matches!(core(err), CoreError::DocumentTooLarge { .. }));
    assert_eq!(e.get(&c, &id).unwrap().unwrap().get_str("pad").unwrap().len(), 1024, "unchanged");

    // Just under the cap is stored; a write that shrinks it, and one that sends
    // it again unchanged, are not refused.
    let room = MAX_DOCUMENT_BYTES - 128;
    e.modify_where(&c, &crate::Candidates::Scan, &Pad(room), None).unwrap();
    e.modify_where(&c, &crate::Candidates::Scan, &Pad(room), None).unwrap();
    e.modify_where(&c, &crate::Candidates::Scan, &Pad(10), None).unwrap();
    assert_eq!(e.get(&c, &id).unwrap().unwrap().get_str("pad").unwrap().len(), 10);
}

/// The length of `pad` that makes `{_id, pad}` encode to exactly `target` bytes.
fn pad_for(id: bson::Bson, target: usize) -> usize {
    let probe = bson::serialize_to_vec(&doc! { "_id": id, "pad": "" }).unwrap().len();
    target - probe
}

/// The boundary is the exact stored encoding on all three checked functions:
/// a document of exactly 16 MiB is stored, one byte more is refused naming
/// that size.
#[test]
fn the_document_boundary_is_exact_on_insert_replace_and_update() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(&dir);
    let c = e.create_collection("shop", "c").unwrap();
    let n = pad_for(bson::Bson::Int64(1), MAX_DOCUMENT_BYTES);
    // insert
    e.insert(&c, doc! { "_id": 1i64, "pad": "x".repeat(n) }).unwrap();
    let stored = e.get(&c, &DocId::Int64(1)).unwrap().unwrap();
    assert_eq!(bson::serialize_to_vec(&stored).unwrap().len(), MAX_DOCUMENT_BYTES);
    let err = core(e.insert(&c, doc! { "_id": 2i64, "pad": "x".repeat(n + 1) }).unwrap_err());
    assert!(
        matches!(err, CoreError::DocumentTooLarge { bytes, .. } if bytes == MAX_DOCUMENT_BYTES + 1),
        "{err:?}"
    );
    // replace (over an existing row, and as an upsert that creates one)
    e.insert(&c, doc! { "_id": 3i64 }).unwrap();
    e.replace(&c, &DocId::Int64(3), doc! { "pad": "y".repeat(n) }, false).unwrap();
    let err = core(
        e.replace(&c, &DocId::Int64(3), doc! { "pad": "z".repeat(n + 1) }, false).unwrap_err(),
    );
    assert!(
        matches!(err, CoreError::DocumentTooLarge { bytes, .. } if bytes == MAX_DOCUMENT_BYTES + 1),
        "{err:?}"
    );
    // An upsert stores a small integer id as an Int32 (`replace_id_value`).
    let m = pad_for(bson::Bson::Int32(4), MAX_DOCUMENT_BYTES);
    e.replace(&c, &DocId::Int64(4), doc! { "pad": "y".repeat(m) }, true).unwrap();
    let err =
        core(e.replace(&c, &DocId::Int64(5), doc! { "pad": "y".repeat(m + 1) }, true).unwrap_err());
    assert!(
        matches!(err, CoreError::DocumentTooLarge { bytes, .. } if bytes == MAX_DOCUMENT_BYTES + 1),
        "{err:?}"
    );
    // update result
    let d = tempfile::tempdir().unwrap();
    let e = open(&d);
    let c = e.create_collection("shop", "c").unwrap();
    e.insert(&c, doc! { "_id": 1i64, "pad": "" }).unwrap();
    e.modify_where(&c, &crate::Candidates::Scan, &Pad(n), None).unwrap();
    let err = core(e.modify_where(&c, &crate::Candidates::Scan, &Pad(n + 1), None).unwrap_err());
    assert!(
        matches!(err, CoreError::DocumentTooLarge { bytes, .. } if bytes == MAX_DOCUMENT_BYTES + 1),
        "{err:?}"
    );
}

/// A binary id and a multi-byte string id are measured in bytes, 4096 passes.
#[test]
fn binary_and_multibyte_ids_are_measured_in_bytes_at_the_cap() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(&dir);
    let c = e.create_collection("shop", "c").unwrap();
    let bin = |len: usize| {
        bson::Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![7; len],
        })
    };
    e.insert(&c, doc! { "_id": bin(MAX_ID_BYTES) }).unwrap();
    let err = core(e.insert(&c, doc! { "_id": bin(MAX_ID_BYTES + 1) }).unwrap_err());
    assert!(
        matches!(err, CoreError::IdTooLarge { kind: "binary", bytes, .. } if bytes == MAX_ID_BYTES + 1),
        "{err:?}"
    );
    e.insert(&c, doc! { "_id": "\u{e9}".repeat(MAX_ID_BYTES / 2) }).unwrap();
    let err = core(
        e.insert(&c, doc! { "_id": format!("{}a", "\u{e9}".repeat(MAX_ID_BYTES / 2)) })
            .unwrap_err(),
    );
    assert!(
        matches!(err, CoreError::IdTooLarge { kind: "string", bytes, .. } if bytes == MAX_ID_BYTES + 1),
        "{err:?}"
    );
}

/// The vectors of a document whose `_id` is within the cap can be stored: the
/// cap is client admission only (ADR-250), and a chunk id is derived, not sent.
#[test]
fn the_vectors_of_a_document_whose_id_is_within_the_cap_can_be_stored() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(&dir);
    let shadow = e.create_collection("shop", "c.__vectors").unwrap();
    let record = |source: DocId| kimmy_core::VectorRecord {
        source,
        chunk: 0,
        source_hlc: kimmy_core::Hlc::new(1, 0),
        vector: vec![1.0, 0.0, 0.0],
        text: "t".into(),
        config: None,
    };
    let mut failures = Vec::new();
    for source in [
        DocId::String("a".repeat(MAX_ID_BYTES)),
        DocId::String("a".repeat(MAX_ID_BYTES - 1)),
        DocId::Binary { subtype: 0, bytes: vec![1; 3000] },
        DocId::Binary { subtype: 4, bytes: vec![1; 2045] },
    ] {
        let label = match &source {
            DocId::String(s) => format!("string {}", s.len()),
            DocId::Binary { bytes, .. } => format!("binary {}", bytes.len()),
            _ => unreachable!(),
        };
        if let Err(err) = e.put_vectors(&shadow, &source, &[record(source.clone())]) {
            failures.push(format!("{label}: {err}"));
        }
    }
    assert!(failures.is_empty(), "vectors refused for ids within the cap: {failures:#?}");
}

/// A multi update that crosses the cap in a later chunk: the earlier chunk stands
/// and the answer is `PartiallyApplied` carrying `DocumentTooLarge`.
#[test]
fn a_multi_update_crossing_the_cap_in_a_later_chunk_is_partially_applied() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(&dir);
    e.set_multi_chunk_docs(2);
    let c = e.create_collection("shop", "c").unwrap();
    let near = pad_for(bson::Bson::Int64(3), MAX_DOCUMENT_BYTES) - 100;
    e.insert(&c, doc! { "_id": 1i64, "pad": "" }).unwrap();
    e.insert(&c, doc! { "_id": 2i64, "pad": "" }).unwrap();
    e.insert(&c, doc! { "_id": 3i64, "pad": "x".repeat(near), "more": "" }).unwrap();
    // Sets `more` to 1 KiB on every document: only the third crosses.
    struct More;
    impl ModifySpec for More {
        fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
            Ok(true)
        }
        fn compare(&self, _: &Document, _: &Document) -> Ordering {
            Ordering::Equal
        }
        fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
            let mut next = doc.clone();
            next.insert("more", "m".repeat(1024));
            Ok(Some(next))
        }
        fn upsert(&self) -> Option<std::result::Result<Document, String>> {
            None
        }
    }
    let err = e.modify_where(&c, &crate::Candidates::Scan, &More, None).unwrap_err();
    match err {
        StorageError::PartiallyApplied { applied, cause } => {
            assert!(
                matches!(
                    applied,
                    crate::Applied::Modify { matched: 2, modified: 2, commits: 1, in_doubt: 0 }
                ),
                "{applied:?}"
            );
            assert!(
                matches!(*cause, StorageError::Core(CoreError::DocumentTooLarge { .. })),
                "{cause:?}"
            );
        }
        other => panic!("not partially applied: {other:?}"),
    }
    for id in [1i64, 2] {
        assert_eq!(
            e.get(&c, &DocId::Int64(id)).unwrap().unwrap().get_str("more").unwrap().len(),
            1024
        );
    }
    assert_eq!(e.get(&c, &DocId::Int64(3)).unwrap().unwrap().get_str("more").unwrap(), "");
}
