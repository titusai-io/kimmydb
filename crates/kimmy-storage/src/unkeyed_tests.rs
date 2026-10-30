//! A document an index cannot key (ADR-258): refused on the member that takes the
//! write, filed unkeyed and announced when it arrives any other way.

use std::cmp::Ordering;

use bson::{Bson, Document, doc};
use kimmy_core::{
    DocId, Error as CoreError, Hlc, NodeId, OpKind, OplogEntry, Stamp, UnkeyedDocumentDetail,
};

use crate::index::CreateOrigin;
use crate::merge::{SinkMode, WhenMatched};
use crate::meta::{Enforcement, IndexField};
use crate::{Candidates, CollectionMeta, Engine, ModifySpec, StorageError};

fn open() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn fields(paths: &[&str]) -> Vec<IndexField> {
    paths.iter().map(|p| IndexField::ascending(*p)).collect()
}

/// A collection `c` holding one keyable document, under a compound index `ab`.
fn indexed(e: &Engine) -> CollectionMeta {
    e.create_collection("shop", "c").unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    e.insert(&c, doc! { "_id": 1i64, "a": 1, "b": 1 }).unwrap();
    e.create_index("shop", "c", fields(&["a", "b"]), false, Some("ab".into())).unwrap();
    e.get_collection("shop", "c").unwrap()
}

fn bad() -> Document {
    doc! { "a": [1, 2], "b": [3, 4] }
}

fn with_id(id: i64, mut d: Document) -> Document {
    d.insert("_id", id);
    d
}

fn refusal(err: StorageError) -> String {
    match err {
        StorageError::Core(CoreError::InvalidQuery(message)) => message,
        other => panic!("not a 400: {other:?}"),
    }
}

fn events(e: &Engine) -> Vec<UnkeyedDocumentDetail> {
    e.read_oplog_from(Hlc::ZERO, 10_000, crate::WalkScope::Request)
        .unwrap()
        .into_iter()
        .filter(|entry| entry.kind == OpKind::UnkeyedDocument)
        .map(|entry| bson::deserialize_from_slice(&entry.body.unwrap()).unwrap())
        .collect()
}

fn unkeyed_of(e: &Engine, c: &CollectionMeta) -> u64 {
    e.unkeyed_count(c, c.index("ab").unwrap().id).unwrap()
}

/// Sets `a` and `b` to two arrays.
struct SetBoth;

impl ModifySpec for SetBoth {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        Ok(true)
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.insert("a", vec![1, 2]);
        next.insert("b", vec![3, 4]);
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

/// Matches nothing and upserts a document with two arrays.
struct UpsertBoth;

impl ModifySpec for UpsertBoth {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        Ok(false)
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        Ok(Some(doc.clone()))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        Some(Ok(with_id(77, bad())))
    }
}

fn remote(c: &CollectionMeta, id: i64, wall: u64, body: Document) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall, 0), NodeId::generate()),
        kind: OpKind::Insert,
        collection: c.id,
        doc_id: Some(DocId::Int64(id)),
        body: Some(bson::serialize_to_vec(&with_id(id, body)).unwrap()),
    }
}

#[test]
fn every_local_write_path_refuses_parallel_arrays() {
    let (e, _d) = open();
    let c = indexed(&e);

    let message = refusal(e.insert(&c, with_id(2, bad())).unwrap_err());
    assert!(message.starts_with("cannot index parallel arrays [b] [a]"), "{message}");
    assert!(message.contains("\"ab\"") && message.contains("_id 2"), "{message}");

    let err = e.insert_many(&c, vec![with_id(3, doc! { "a": 1 }), with_id(4, bad())]).unwrap_err();
    assert_eq!(err.index, Some(1));
    refusal(err.source);
    refusal(e.replace(&c, &DocId::Int64(1), bad(), false).unwrap_err());
    refusal(e.replace(&c, &DocId::Int64(9), bad(), true).unwrap_err());
    refusal(e.modify_where(&c, &Candidates::Scan, &SetBoth, None).unwrap_err());
    refusal(e.find_and_modify(&c, &Candidates::Scan, &SetBoth).unwrap_err());
    refusal(e.find_and_modify(&c, &Candidates::Keys(vec![]), &UpsertBoth).unwrap_err());
    refusal(e.write_documents(&c, vec![with_id(8, bad())], SinkMode::Insert).unwrap_err());
    let merge = SinkMode::Merge { when_matched: WhenMatched::Merge, insert_unmatched: true };
    refusal(e.write_documents(&c, vec![with_id(9, bad())], merge).unwrap_err());
    // A merge whose result is the unkeyable shape: the stored document has `a`
    // and `b` scalar, the incoming one lays arrays over both.
    refusal(e.write_documents(&c, vec![with_id(1, bad())], merge).unwrap_err());

    assert_eq!(e.count(&c, crate::WalkScope::Request).unwrap(), 1, "nothing was stored");
    assert_eq!(e.get(&c, &DocId::Int64(1)).unwrap().unwrap().get_i32("a").unwrap(), 1);
    assert_eq!(unkeyed_of(&e, &c), 0);
    assert_eq!(e.unkeyed_writes(), 0, "a refusal files nothing");
    assert_eq!(e.unkeyed_refusals(), 10);
    assert!(events(&e).is_empty(), "a refused write is not announced");

    // And the keyable shapes pass on each path.
    e.insert(&c, doc! { "_id": 2i64, "a": [1, 2], "b": 3 }).unwrap();
    e.replace(&c, &DocId::Int64(1), doc! { "a": 1, "b": [5, 6] }, false).unwrap();
    e.write_documents(&c, vec![doc! { "_id": 8i64, "a": 1 }], SinkMode::Insert).unwrap();
}

#[test]
fn the_key_cap_is_refused_and_one_under_it_is_not() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    e.create_index("shop", "c", fields(&["a"]), false, Some("a_1".into())).unwrap();
    let c = e.get_collection("shop", "c").unwrap();

    let big: Vec<Bson> = (0..1_000).map(Bson::Int32).collect();
    let message = refusal(e.insert(&c, doc! { "_id": 1i64, "a": big.clone() }).unwrap_err());
    assert!(message.contains("more than 1000 index keys"), "{message}");
    assert!(message.contains("\"a_1\"") && message.contains("_id 1"), "{message}");
    refusal(e.replace(&c, &DocId::Int64(2), doc! { "a": big.clone() }, true).unwrap_err());
    let under: Vec<Bson> = (0..998).map(Bson::Int32).collect();
    e.insert(&c, doc! { "_id": 3i64, "a": under }).unwrap();
    assert_eq!(e.unkeyed_refusals(), 2);
}

#[test]
fn a_unique_index_keeps_its_own_refusal() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    e.create_index("shop", "c", fields(&["a", "b"]), true, Some("ab".into())).unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    let message = refusal(e.insert(&c, with_id(1, bad())).unwrap_err());
    assert!(message.contains("a unique index must be able to key every document"), "{message}");
}

#[test]
fn a_partial_index_is_asked_only_about_the_documents_it_selects() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    e.create_index_with(
        "shop",
        "c",
        fields(&["a", "b"]),
        false,
        Enforcement::Local,
        Some("ab".into()),
        None,
        Some(doc! { "k": 1 }),
    )
    .unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    let mut outside = bad();
    outside.insert("k", 2);
    e.insert(&c, with_id(1, outside)).unwrap();
    let mut inside = bad();
    inside.insert("k", 1);
    refusal(e.insert(&c, with_id(2, inside)).unwrap_err());
}

#[test]
fn a_replicated_write_is_accepted_filed_and_announced_once() {
    let (e, _d) = open();
    let c = indexed(&e);
    let mut rx = e.subscribe();

    let first = remote(&c, 5, 100, bad());
    assert!(e.apply_remote(&c, &first).unwrap(), "accepted, not refused");
    assert_eq!(unkeyed_of(&e, &c), 1);
    assert!(e.get(&c, &DocId::Int64(5)).unwrap().is_some());
    assert_eq!(
        events(&e),
        vec![UnkeyedDocumentDetail {
            index: "ab".into(),
            id: DocId::Int64(5),
            reason: "parallel_arrays".into()
        }]
    );
    // Published to a live watcher after the entry itself.
    let kinds: Vec<OpKind> = std::iter::from_fn(|| rx.try_recv().ok()).map(|x| x.kind).collect();
    assert_eq!(kinds, vec![OpKind::Insert, OpKind::UnkeyedDocument]);
    assert_eq!(e.unkeyed_refusals(), 0);

    // The event is this member's own: it is never served to a peer.
    let served = e.entries_for_peer(Hlc::ZERO, 1_000, crate::WalkScope::Request).unwrap().entries;
    assert!(served.iter().all(|x| x.kind != OpKind::UnkeyedDocument), "{served:?}");
    assert!(served.iter().any(|x| x.kind == OpKind::Insert));

    // The same entry again is superseded: no second event.
    assert!(!e.apply_remote(&c, &first).unwrap());
    assert_eq!(events(&e).len(), 1);
}

#[test]
fn the_event_is_sent_on_the_move_into_the_unkeyed_run_and_not_again() {
    let (e, _d) = open();
    let c = indexed(&e);
    let mut wall = 100;
    let mut put = |e: &Engine, id: i64, body: Document| {
        wall += 10;
        let mut entry = remote(&c, id, wall, body);
        entry.kind = OpKind::Replace;
        assert!(e.apply_remote(&c, &entry).unwrap());
    };

    // A keyed document arriving announces nothing.
    put(&e, 5, doc! { "a": [1, 2], "b": 3 });
    assert!(events(&e).is_empty());
    // Keyed to unkeyed: announced.
    put(&e, 5, bad());
    assert_eq!(events(&e).len(), 1);
    // Unkeyed to unkeyed, by a re-save or by another field: not again, though
    // filed and counted again.
    let counted = e.unkeyed_writes();
    put(&e, 5, bad());
    put(&e, 5, doc! { "a": [1, 2], "b": [3, 4], "note": "x" });
    assert_eq!(events(&e).len(), 1);
    assert_eq!(e.unkeyed_writes(), counted + 2);
    assert_eq!(unkeyed_of(&e, &c), 1, "one document, one entry in the run");
    // Unkeyed to keyed to unkeyed: the second move is announced.
    put(&e, 5, doc! { "a": 1, "b": 1 });
    assert_eq!(unkeyed_of(&e, &c), 0);
    put(&e, 5, bad());
    assert_eq!(events(&e).len(), 2);
    // A different document is its own event.
    put(&e, 6, bad());
    let ids: Vec<DocId> = events(&e).into_iter().map(|d| d.id).collect();
    assert_eq!(ids, vec![DocId::Int64(5), DocId::Int64(5), DocId::Int64(6)]);
}

#[test]
fn a_document_is_announced_once_for_each_index_that_cannot_key_it() {
    let (e, _d) = open();
    indexed(&e);
    e.create_index("shop", "c", fields(&["b", "a"]), false, Some("ba".into())).unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    assert!(e.apply_remote(&c, &remote(&c, 5, 100, bad())).unwrap());
    let mut named: Vec<String> = events(&e).into_iter().map(|d| d.index).collect();
    named.sort();
    assert_eq!(named, vec!["ab", "ba"]);
}

#[test]
fn the_key_cap_has_its_own_reason() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    e.create_index("shop", "c", fields(&["a"]), false, Some("a_1".into())).unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    let big: Vec<Bson> = (0..1_000).map(Bson::Int32).collect();
    assert!(e.apply_remote(&c, &remote(&c, 1, 100, doc! { "a": big })).unwrap());
    assert_eq!(events(&e)[0].reason, "too_many_keys");
}

#[test]
fn a_local_create_over_documents_it_cannot_key_lists_them_and_creates_nothing() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    for i in 0..13 {
        e.insert(&c, with_id(i, bad())).unwrap();
    }
    e.insert(&c, doc! { "_id": 100i64, "a": 1, "b": 1 }).unwrap();

    let message = refusal(
        e.create_index("shop", "c", fields(&["a", "b"]), false, Some("ab".into())).unwrap_err(),
    );
    assert!(message.contains("13 existing documents"), "{message}");
    assert!(message.contains("cannot index parallel arrays"), "{message}");
    assert!(message.contains("_ids: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 and 3 more"), "{message}");
    assert!(message.contains("no index was created"), "{message}");
    assert!(e.get_collection("shop", "c").unwrap().index("ab").is_none());
    assert_eq!(e.unkeyed_writes(), 0);
    let oplog = e.read_oplog_from(Hlc::ZERO, 1_000, crate::WalkScope::Request).unwrap();
    assert!(oplog.iter().all(|x| x.kind != OpKind::CreateIndex), "nothing was logged");

    // With the offenders gone the same create succeeds.
    for i in 0..13 {
        e.delete(&c, &DocId::Int64(i)).unwrap();
    }
    e.create_index("shop", "c", fields(&["a", "b"]), false, Some("ab".into())).unwrap();
}

#[test]
fn a_replicated_definition_is_accepted_over_documents_it_cannot_key() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    e.insert(&c, with_id(1, bad())).unwrap();
    e.insert(&c, with_id(2, bad())).unwrap();
    e.insert(&c, doc! { "_id": 3i64, "a": 1, "b": 1 }).unwrap();

    let created = Stamp::new(Hlc::new(5, 0), NodeId::generate());
    let (built, _) = e
        .create_index_inner(
            "shop",
            "c",
            fields(&["a", "b"]),
            false,
            Enforcement::Local,
            Some("ab".into()),
            None,
            None,
            CreateOrigin::Replicated { created: Some(created), logged: None },
            &|_, _| Ok(false),
        )
        .unwrap();
    assert!(matches!(built, crate::index::IndexCreated::Built(_)));
    let c = e.get_collection("shop", "c").unwrap();
    assert_eq!(unkeyed_of(&e, &c), 2);
    assert_eq!(e.unkeyed_writes(), 2);
    let mut seen = events(&e);
    seen.sort_by_key(|d| format!("{:?}", d.id));
    assert_eq!(
        seen,
        vec![
            UnkeyedDocumentDetail {
                index: "ab".into(),
                id: DocId::Int64(1),
                reason: "parallel_arrays".into()
            },
            UnkeyedDocumentDetail {
                index: "ab".into(),
                id: DocId::Int64(2),
                reason: "parallel_arrays".into()
            },
        ]
    );
    assert_eq!(e.unkeyed_refusals(), 0);
}

#[test]
fn the_unkeyed_list_pages_and_names_the_reason_of_each_document() {
    let (e, _d) = open();
    let c = indexed(&e);
    for id in 10..15 {
        assert!(e.apply_remote(&c, &remote(&c, id, 100 + id as u64, bad())).unwrap());
    }
    let index = c.index("ab").unwrap();

    let first = e.unkeyed_documents(&c, index, None, 2).unwrap();
    assert_eq!(first.rows.len(), 2);
    assert!(first.next.is_some());
    let second = e.unkeyed_documents(&c, index, first.next.as_deref(), 2).unwrap();
    let third = e.unkeyed_documents(&c, index, second.next.as_deref(), 2).unwrap();
    assert_eq!(third.rows.len(), 1);
    assert!(third.next.is_none());
    let ids: Vec<Bson> = [first, second, third]
        .into_iter()
        .flat_map(|p| p.rows)
        .map(|(id, why)| {
            assert_eq!(why, "parallel_arrays");
            id
        })
        .collect();
    assert_eq!(ids.len(), 5, "every unkeyed document, once, and the keyed one not at all");
    assert!(!ids.contains(&Bson::Int64(1)));

    // A document reshaped out of the run leaves the list.
    let mut fixed = remote(&c, 12, 500, doc! { "a": 1, "b": 1 });
    fixed.kind = OpKind::Replace;
    assert!(e.apply_remote(&c, &fixed).unwrap());
    assert_eq!(e.unkeyed_documents(&c, index, None, 100).unwrap().rows.len(), 4);
}

/// Applies replicated replaces of document 5, stamped a known distance after
/// the index was created, so a rewind can be aimed between them.
struct Replays {
    base: u64,
}

impl Replays {
    fn new() -> Self {
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
                as u64;
        Self { base: now + 1_000_000 }
    }

    fn put(&self, e: &Engine, c: &CollectionMeta, offset: u64, body: Document) {
        let mut entry = remote(c, 5, self.base + offset, body);
        entry.kind = OpKind::Replace;
        assert!(e.apply_remote(c, &entry).unwrap());
    }

    /// A target between the writes at offsets 10 and 20.
    fn between(&self) -> Hlc {
        Hlc::new(self.base + 15, 0)
    }
}

#[test]
fn a_rewind_that_puts_back_an_unkeyed_document_announces_it() {
    let (e, _d) = open();
    let c = indexed(&e);
    let mut rx = e.subscribe();
    let at = Replays::new();
    at.put(&e, &c, 10, bad());
    at.put(&e, &c, 20, doc! { "a": 1, "b": 1 });
    assert_eq!(events(&e).len(), 1, "the arrival of the unkeyed document");
    assert_eq!(unkeyed_of(&e, &c), 0);
    while rx.try_recv().is_ok() {}

    let counted = e.unkeyed_writes();
    e.rewind_to(at.between()).unwrap();

    // Filed unkeyed again, counted, and announced a second time in the
    // rewind's own transaction.
    assert_eq!(unkeyed_of(&e, &c), 1);
    assert_eq!(e.unkeyed_writes(), counted + 1);
    let named: Vec<(DocId, String)> = events(&e).into_iter().map(|d| (d.id, d.reason)).collect();
    assert_eq!(
        named,
        vec![
            (DocId::Int64(5), "parallel_arrays".to_string()),
            (DocId::Int64(5), "parallel_arrays".to_string())
        ]
    );
    let kinds: Vec<OpKind> = std::iter::from_fn(|| rx.try_recv().ok()).map(|x| x.kind).collect();
    assert_eq!(kinds, vec![OpKind::UnkeyedDocument]);
    // Still this member's own report.
    let served = e.entries_for_peer(Hlc::ZERO, 1_000, crate::WalkScope::Request).unwrap().entries;
    assert!(served.iter().all(|x| x.kind != OpKind::UnkeyedDocument), "{served:?}");
}

#[test]
fn a_rewind_that_leaves_a_document_unkeyed_announces_nothing() {
    let (e, _d) = open();
    let c = indexed(&e);
    let at = Replays::new();
    at.put(&e, &c, 10, bad());
    at.put(&e, &c, 20, doc! { "a": [1, 2], "b": [3, 4], "note": "x" });
    assert_eq!(events(&e).len(), 1);

    e.rewind_to(at.between()).unwrap();

    assert_eq!(unkeyed_of(&e, &c), 1);
    assert_eq!(events(&e).len(), 1, "unkeyed before and after: not a move into the run");
}

#[test]
fn a_rewind_that_makes_an_unkeyed_document_keyed_announces_nothing_and_drops_the_old_report() {
    let (e, _d) = open();
    let c = indexed(&e);
    let at = Replays::new();
    at.put(&e, &c, 10, doc! { "a": 1, "b": 1 });
    at.put(&e, &c, 20, bad());
    assert_eq!(events(&e).len(), 1);

    e.rewind_to(at.between()).unwrap();

    assert_eq!(unkeyed_of(&e, &c), 0);
    assert!(events(&e).is_empty(), "the report of the undone write left with it");
}

#[test]
fn write_documents_refuses_an_unkeyable_output_before_its_first_chunk() {
    let (e, _d) = open();
    e.create_collection("shop", "t").unwrap();
    e.create_index("shop", "t", fields(&["a", "b"]), false, Some("ab".into())).unwrap();
    let c = e.get_collection("shop", "t").unwrap();
    e.set_multi_chunk_docs(2);
    let output = || {
        vec![
            doc! { "_id": 1i64, "a": 1, "b": 1 },
            doc! { "_id": 2i64, "a": 2, "b": 2 },
            with_id(3, bad()),
        ]
    };
    for mode in [
        SinkMode::Insert,
        SinkMode::Merge { when_matched: WhenMatched::Replace, insert_unmatched: true },
        SinkMode::Merge { when_matched: WhenMatched::Merge, insert_unmatched: true },
    ] {
        let message = refusal(e.write_documents(&c, output(), mode).unwrap_err());
        assert!(message.contains("cannot index parallel arrays"), "{message}");
        assert!(message.contains("_id 3") && message.ends_with("output document 2"), "{message}");
        assert_eq!(
            e.count(&c, crate::WalkScope::Request).unwrap(),
            0,
            "the first chunk was not written"
        );
    }
    // An output document with no `_id` is named by its place.
    let message = refusal(e.write_documents(&c, vec![bad()], SinkMode::Insert).unwrap_err());
    assert!(message.ends_with("output document 0"), "{message}");

    // A document the stage would not write is not asked about.
    e.insert(&c, doc! { "_id": 3i64, "a": 3, "b": 3 }).unwrap();
    let keep = SinkMode::Merge { when_matched: WhenMatched::KeepExisting, insert_unmatched: true };
    assert_eq!(e.write_documents(&c, output(), keep).unwrap().counts.kept, 1);
}

/// An output document repeating the `_id` of an earlier one that the write
/// inserts is matched by that insert inside the write. `keepExisting` keeps it,
/// never writing it, so it is not checked (ADR-258).
#[test]
fn a_kept_output_document_repeating_an_earlier_id_is_not_checked() {
    let (e, _d) = open();
    e.create_collection("shop", "t").unwrap();
    e.create_index("shop", "t", fields(&["a", "b"]), false, Some("ab".into())).unwrap();
    let c = e.get_collection("shop", "t").unwrap();
    let output = vec![with_id(7, doc! { "a": 1, "b": 1 }), with_id(7, bad())];
    let keep = SinkMode::Merge { when_matched: WhenMatched::KeepExisting, insert_unmatched: true };

    // What the write alone does, a call each.
    let (alone, _d2) = open();
    alone.create_collection("shop", "t").unwrap();
    alone.create_index("shop", "t", fields(&["a", "b"]), false, Some("ab".into())).unwrap();
    let ac = alone.get_collection("shop", "t").unwrap();
    assert_eq!(
        alone.write_documents(&ac, vec![output[0].clone()], keep).unwrap().counts.inserted,
        1
    );
    assert_eq!(alone.write_documents(&ac, vec![output[1].clone()], keep).unwrap().counts.kept, 1);

    let counts = e.write_documents(&c, output, keep).unwrap().counts;
    assert_eq!((counts.inserted, counts.kept), (1, 1));
    assert_eq!(e.count(&c, crate::WalkScope::Request).unwrap(), 1);
}

/// Under the default merge the repeat is matched by the earlier insert, and is
/// stored in its final form, known only inside the write: the up-front check
/// leaves it to the write, whose refusal names no output position. `replace`
/// stores the incoming document as it stands, and is refused up front.
#[test]
fn a_merged_output_document_repeating_an_earlier_id_is_left_to_the_write() {
    let (e, _d) = open();
    e.create_collection("shop", "t").unwrap();
    e.create_index("shop", "t", fields(&["a", "b"]), false, Some("ab".into())).unwrap();
    let c = e.get_collection("shop", "t").unwrap();
    let merge = SinkMode::Merge { when_matched: WhenMatched::Merge, insert_unmatched: true };
    let output = vec![with_id(7, doc! { "a": 1, "b": 1 }), with_id(7, bad())];
    let message = refusal(e.write_documents(&c, output.clone(), merge).unwrap_err());
    assert!(message.contains("cannot index parallel arrays"), "{message}");
    assert!(!message.contains("output document"), "refused by the write, not ahead: {message}");
    assert_eq!(e.count(&c, crate::WalkScope::Request).unwrap(), 0, "one chunk, rolled back");

    let replace = SinkMode::Merge { when_matched: WhenMatched::Replace, insert_unmatched: true };
    let message = refusal(e.write_documents(&c, output, replace).unwrap_err());
    assert!(message.ends_with("output document 1"), "{message}");
}

/// A document the stage discards as unmatched is never written, so a later
/// document with its `_id` is unmatched too, and is not matched by it.
#[test]
fn a_discarded_output_document_does_not_match_a_later_one() {
    let (e, _d) = open();
    e.create_collection("shop", "t").unwrap();
    e.create_index("shop", "t", fields(&["a", "b"]), false, Some("ab".into())).unwrap();
    let c = e.get_collection("shop", "t").unwrap();
    let discard = SinkMode::Merge { when_matched: WhenMatched::Merge, insert_unmatched: false };
    let output = vec![with_id(7, doc! { "a": 1, "b": 1 }), with_id(7, bad())];
    assert_eq!(e.write_documents(&c, output, discard).unwrap().counts.discarded, 2);
}

#[test]
fn write_documents_refuses_an_output_a_unique_index_cannot_key_before_its_first_chunk() {
    let (e, _d) = open();
    e.create_collection("shop", "t").unwrap();
    e.create_index("shop", "t", fields(&["a", "b"]), true, Some("ab".into())).unwrap();
    let c = e.get_collection("shop", "t").unwrap();
    e.set_multi_chunk_docs(2);
    let output = || {
        vec![
            doc! { "_id": 1i64, "a": 1, "b": 1 },
            doc! { "_id": 2i64, "a": 2, "b": 2 },
            with_id(3, bad()),
        ]
    };
    for mode in [
        SinkMode::Insert,
        SinkMode::Merge { when_matched: WhenMatched::Replace, insert_unmatched: true },
    ] {
        let message = refusal(e.write_documents(&c, output(), mode).unwrap_err());
        assert!(message.contains("a unique index must be able to key every document"), "{message}");
        assert!(message.ends_with("output document 2"), "{message}");
        assert_eq!(e.count(&c, crate::WalkScope::Request).unwrap(), 0, "nothing was written");
    }
}

#[test]
fn a_rewind_keeps_the_report_of_a_filing_that_still_stands() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    e.create_index("shop", "c", fields(&["a", "b"]), false, Some("ab".into())).unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    let now =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
            as u64;
    std::thread::sleep(std::time::Duration::from_millis(200));
    // Written elsewhere at now + 10, arriving here 200 ms later: its report is
    // stamped above the target, the document's own stamp below it.
    assert!(e.apply_remote(&c, &remote(&c, 5, now + 10, bad())).unwrap());
    assert_eq!(events(&e).len(), 1);
    let mut rx = e.subscribe();

    e.rewind_to(Hlc::new(now + 50, 0)).unwrap();

    let c = e.get_collection("shop", "c").unwrap();
    assert_eq!(unkeyed_of(&e, &c), 1, "the document stands, unkeyed");
    let named: Vec<(DocId, String)> = events(&e).into_iter().map(|d| (d.id, d.reason)).collect();
    assert_eq!(named, vec![(DocId::Int64(5), "parallel_arrays".to_string())]);
    let kinds: Vec<OpKind> = std::iter::from_fn(|| rx.try_recv().ok()).map(|x| x.kind).collect();
    assert_eq!(kinds, vec![OpKind::UnkeyedDocument]);

    // A second rewind to the same point does not pile reports up.
    e.rewind_to(Hlc::new(now + 50, 0)).unwrap();
    assert_eq!(events(&e).len(), 1);
}

#[test]
fn a_rewind_does_not_re_announce_a_report_whose_document_is_gone_or_keyed() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    e.create_index("shop", "c", fields(&["a", "b"]), false, Some("ab".into())).unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    let now =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
            as u64;
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert!(e.apply_remote(&c, &remote(&c, 5, now + 10, bad())).unwrap());
    assert!(e.apply_remote(&c, &remote(&c, 6, now + 10, bad())).unwrap());
    assert_eq!(events(&e).len(), 2);
    // Document 5 is fixed and document 6 deleted, both at or below the target:
    // neither is in the rewind's plan, and neither stands unkeyed.
    let mut fixed = remote(&c, 5, now + 20, doc! { "a": 1, "b": 1 });
    fixed.kind = OpKind::Replace;
    assert!(e.apply_remote(&c, &fixed).unwrap());
    let mut gone = remote(&c, 6, now + 30, doc! {});
    gone.kind = OpKind::Delete;
    gone.body = None;
    assert!(e.apply_remote(&c, &gone).unwrap());
    assert_eq!(unkeyed_of(&e, &c), 0);

    e.rewind_to(Hlc::new(now + 50, 0)).unwrap();

    let c = e.get_collection("shop", "c").unwrap();
    assert_eq!(unkeyed_of(&e, &c), 0);
    assert!(events(&e).is_empty(), "{:?}", events(&e));
}

#[test]
fn the_parallel_arrays_message_names_the_path_when_one_path_holds_both_arrays() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    e.create_index("shop", "c", fields(&["x.a", "c"]), false, Some("xa_c".into())).unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    let message = refusal(
        e.insert(&c, doc! { "_id": 1i64, "x": [{ "a": [1] }, { "a": [2] }], "c": 1 }).unwrap_err(),
    );
    assert!(message.contains("cannot index parallel arrays [x.a] [x.a]"), "{message}");
    assert!(!message.contains('?'), "{message}");
    // Two paths still name both, the later field first.
    let (e, _d) = open();
    let c = indexed(&e);
    let message = refusal(e.insert(&c, with_id(2, bad())).unwrap_err());
    assert!(message.contains("cannot index parallel arrays [b] [a]"), "{message}");
}

#[test]
fn a_unique_refusal_and_a_refused_create_are_counted() {
    let (e, _d) = open();
    e.create_collection("shop", "u").unwrap();
    e.create_index("shop", "u", fields(&["a", "b"]), true, Some("ab".into())).unwrap();
    let u = e.get_collection("shop", "u").unwrap();
    refusal(e.insert(&u, with_id(1, bad())).unwrap_err());
    assert_eq!(e.unkeyed_refusals(), 1, "a unique index's refusal of a write");

    // A refused local create is one refusal, however many documents it names.
    e.create_collection("shop", "v").unwrap();
    let v = e.get_collection("shop", "v").unwrap();
    for i in 0..3 {
        e.insert(&v, with_id(i, bad())).unwrap();
    }
    refusal(
        e.create_index("shop", "v", fields(&["a", "b"]), false, Some("ab".into())).unwrap_err(),
    );
    assert_eq!(e.unkeyed_refusals(), 2, "a refused create of a non-unique index");
    refusal(
        e.create_index("shop", "v", fields(&["a", "b"]), true, Some("abu".into())).unwrap_err(),
    );
    assert_eq!(e.unkeyed_refusals(), 3, "a refused create of a unique index");

    // A create that succeeds, and a rewind that files, count nothing.
    e.create_collection("shop", "w").unwrap();
    e.create_index("shop", "w", fields(&["a", "b"]), false, Some("ab".into())).unwrap();
    assert_eq!(e.unkeyed_refusals(), 3);
}
