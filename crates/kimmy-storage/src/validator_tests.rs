//! The collection validator (ADR-257): what it holds, which writes it refuses, and
//! that nothing replicated ever meets it.

use std::cmp::Ordering;

use bson::{Document, doc};
use kimmy_core::{
    DocId, Error as CoreError, Hlc, OpKind, Stamp, ValidationAction, ValidationLevel, ValidatorSpec,
};

use crate::merge::{SinkMode, WhenMatched};
use crate::{Candidates, Engine, ModifySpec, StorageError};

fn open() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn spec(action: ValidationAction, level: ValidationLevel) -> ValidatorSpec {
    ValidatorSpec {
        schema: doc! {
            "bsonType": "object",
            "required": ["qty"],
            "properties": { "qty": { "bsonType": "int", "minimum": 0 } },
        },
        action,
        level,
    }
}

fn strict_error() -> ValidatorSpec {
    spec(ValidationAction::Error, ValidationLevel::Strict)
}

fn invalid(err: StorageError) -> String {
    match err {
        StorageError::Core(CoreError::DocumentInvalid { reason }) => reason,
        other => panic!("not a validation refusal: {other:?}"),
    }
}

/// Sets `qty` on every document.
struct SetQty(i32);

impl ModifySpec for SetQty {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        Ok(true)
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.insert("qty", self.0);
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

/// Matches nothing and upserts `doc`.
struct Upsert(Document);

impl ModifySpec for Upsert {
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
        Some(Ok(self.0.clone()))
    }
}

/// A collection `c` with the validator set and one conforming document.
fn collection(e: &Engine, validator: ValidatorSpec) -> crate::CollectionMeta {
    e.create_collection("shop", "c").unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    e.insert(&c, doc! { "_id": 1i64, "qty": 5 }).unwrap();
    e.set_validator("shop", "c", Some(validator)).unwrap()
}

fn qty_of(e: &Engine, c: &crate::CollectionMeta, id: i64) -> Option<Bson> {
    e.get(c, &DocId::Int64(id)).unwrap().and_then(|d| d.get("qty").cloned())
}

use bson::Bson;

#[test]
fn a_validator_is_stamped_and_removing_it_is_a_later_dated_change() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    assert_eq!(e.get_collection("shop", "c").unwrap().validator_changed, None);

    let on = e.set_validator("shop", "c", Some(strict_error())).unwrap();
    assert_eq!(on.validator, Some(strict_error()));
    let set_at = on.validator_changed.expect("a change is stamped");

    let off = e.set_validator("shop", "c", None).unwrap();
    assert_eq!(off.validator, None);
    assert!(off.validator_changed > Some(set_at), "removing it is a change, later");

    // A removal of what is not there is a change too: stamped and logged.
    let again = e.set_validator("shop", "c", None).unwrap();
    assert!(again.validator_changed > off.validator_changed);
    let kinds: Vec<OpKind> = e
        .entries_for_peer(Hlc::ZERO, 100, crate::WalkScope::Request)
        .unwrap()
        .entries
        .iter()
        .map(|entry| entry.kind)
        .collect();
    assert_eq!(kinds.iter().filter(|k| **k == OpKind::ConfigureValidator).count(), 3, "{kinds:?}");
}

#[test]
fn a_schema_that_does_not_parse_is_refused_when_it_is_set() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    let bad = ValidatorSpec {
        schema: doc! { "$ref": "#/definitions/x" },
        action: ValidationAction::Error,
        level: ValidationLevel::Strict,
    };
    let err = e.set_validator("shop", "c", Some(bad)).unwrap_err();
    assert!(matches!(err, StorageError::Core(CoreError::InvalidQuery(_))), "{err:?}");
    assert_eq!(e.get_collection("shop", "c").unwrap().validator, None);
}

#[test]
fn a_shadow_collection_takes_no_validator() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    let err = e.set_validator("shop", "c.__vectors", Some(strict_error())).unwrap_err();
    assert!(matches!(err, StorageError::Core(CoreError::InvalidName { .. })), "{err:?}");
}

#[test]
fn every_client_write_path_is_refused_under_error() {
    let (e, _d) = open();
    let c = collection(&e, strict_error());

    // insert
    let reason = invalid(e.insert(&c, doc! { "_id": 2i64, "qty": -1 }).unwrap_err());
    assert!(reason.contains("qty"), "the failing keyword path is named: {reason}");
    assert!(e.get(&c, &DocId::Int64(2)).unwrap().is_none());
    // insert_many: whole batch refused, and the position is named
    let err = e
        .insert_many(&c, vec![doc! { "_id": 3i64, "qty": 1 }, doc! { "_id": 4i64, "qty": "x" }])
        .unwrap_err();
    assert_eq!(err.index, Some(1));
    invalid(err.source);
    assert!(e.get(&c, &DocId::Int64(3)).unwrap().is_none(), "the batch is refused whole");
    // replace, and an upsert that inserts
    invalid(e.replace(&c, &DocId::Int64(1), doc! { "qty": -5 }, false).unwrap_err());
    invalid(e.replace(&c, &DocId::Int64(9), doc! { "qty": -5 }, true).unwrap_err());
    // update (multi)
    invalid(e.modify_where(&c, &Candidates::Scan, &SetQty(-1), None).unwrap_err());
    // find-and-modify: the update, and the upsert's insert
    invalid(e.find_and_modify(&c, &Candidates::Scan, &SetQty(-1)).unwrap_err());
    let upsert = Upsert(doc! { "_id": 7i64, "qty": -1 });
    let nothing = Candidates::Keys(vec![]);
    invalid(e.find_and_modify(&c, &nothing, &upsert).unwrap_err());
    // $out / $merge
    invalid(
        e.write_documents(&c, vec![doc! { "_id": 8i64, "qty": -1 }], SinkMode::Insert).unwrap_err(),
    );

    assert_eq!(qty_of(&e, &c, 1), Some(Bson::Int32(5)), "the stored document never changed");
    assert_eq!(e.count(&c, crate::WalkScope::Request).unwrap(), 1);
    let counts = e.validation_counts();
    assert_eq!(counts.len(), 1);
    assert!(counts[0].refusals >= 8 && counts[0].warnings == 0, "{counts:?}");

    // And conforming writes pass on each of them.
    e.insert(&c, doc! { "_id": 2i64, "qty": 1 }).unwrap();
    e.replace(&c, &DocId::Int64(1), doc! { "qty": 6 }, false).unwrap();
    e.modify_where(&c, &Candidates::Scan, &SetQty(7), None).unwrap();
    e.write_documents(&c, vec![doc! { "_id": 8i64, "qty": 2 }], SinkMode::Insert).unwrap();
}

#[test]
fn a_refused_output_writes_nothing_before_the_first_chunk() {
    let (e, _d) = open();
    let c = collection(&e, strict_error());
    e.set_multi_chunk_docs(2);
    let docs = vec![
        doc! { "_id": 10i64, "qty": 1 },
        doc! { "_id": 11i64, "qty": 1 },
        doc! { "_id": 12i64, "qty": 1 },
        doc! { "_id": 13i64, "qty": -1 },
    ];
    invalid(e.write_documents(&c, docs.clone(), SinkMode::Insert).unwrap_err());
    assert_eq!(e.count(&c, crate::WalkScope::Request).unwrap(), 1, "no chunk was written");
    let replace_all =
        SinkMode::Merge { when_matched: WhenMatched::Replace, insert_unmatched: true };
    invalid(e.write_documents(&c, docs, replace_all).unwrap_err());
    assert_eq!(e.count(&c, crate::WalkScope::Request).unwrap(), 1);
}

#[test]
fn a_merge_is_validated_in_its_final_form() {
    let (e, _d) = open();
    let c = collection(&e, strict_error());
    let merge = SinkMode::Merge { when_matched: WhenMatched::Merge, insert_unmatched: true };
    // The incoming document has no `qty`; merged over the stored one it has.
    e.write_documents(&c, vec![doc! { "_id": 1i64, "note": "hi" }], merge).unwrap();
    assert_eq!(qty_of(&e, &c, 1), Some(Bson::Int32(5)));
    // The same document, unmatched, is an insert that fails the schema.
    invalid(e.write_documents(&c, vec![doc! { "_id": 2i64, "note": "hi" }], merge).unwrap_err());
    // A merge whose result fails is refused in the transaction.
    invalid(e.write_documents(&c, vec![doc! { "_id": 1i64, "qty": -4 }], merge).unwrap_err());
    assert_eq!(qty_of(&e, &c, 1), Some(Bson::Int32(5)));
}

#[test]
fn warn_accepts_the_write_and_counts_it() {
    let (e, _d) = open();
    let c = collection(&e, spec(ValidationAction::Warn, ValidationLevel::Strict));
    e.insert(&c, doc! { "_id": 2i64, "qty": -1 }).unwrap();
    e.insert_many(&c, vec![doc! { "_id": 3i64 }, doc! { "_id": 4i64, "qty": "x" }]).unwrap();
    e.replace(&c, &DocId::Int64(1), doc! { "qty": -2 }, false).unwrap();
    e.modify_where(&c, &Candidates::Scan, &SetQty(-3), None).unwrap();
    e.write_documents(&c, vec![doc! { "_id": 9i64, "qty": -9 }], SinkMode::Insert).unwrap();
    let counts = e.validation_counts();
    assert_eq!(counts[0].refusals, 0);
    assert!(counts[0].warnings >= 6, "{counts:?}");
    assert_eq!(e.count(&c, crate::WalkScope::Request).unwrap(), 5);
}

#[test]
fn moderate_checks_inserts_and_updates_only_of_conforming_documents() {
    let (e, _d) = open();
    collection(&e, spec(ValidationAction::Error, ValidationLevel::Moderate));
    // A document that does not conform, stored before the validator was in force.
    let unvalidated = e.set_validator("shop", "c", None).unwrap();
    e.insert(&unvalidated, doc! { "_id": 2i64, "qty": -1 }).unwrap();
    let c = e
        .set_validator("shop", "c", Some(spec(ValidationAction::Error, ValidationLevel::Moderate)))
        .unwrap();

    // Inserts are checked.
    invalid(e.insert(&c, doc! { "_id": 3i64, "qty": -1 }).unwrap_err());
    // An update of a conforming document is checked.
    invalid(e.replace(&c, &DocId::Int64(1), doc! { "qty": -1 }, false).unwrap_err());
    // An update of one that did not conform is free, to anything.
    e.replace(&c, &DocId::Int64(2), doc! { "qty": -7, "note": 1 }, false).unwrap();
    assert_eq!(qty_of(&e, &c, 2), Some(Bson::Int32(-7)));
    // The same through the update path.
    let only_two = Candidates::Keys(vec![crate::docs::doc_key(&DocId::Int64(2)).unwrap()]);
    e.modify_where(&c, &only_two, &SetQty(-8), None).unwrap();
    assert_eq!(qty_of(&e, &c, 2), Some(Bson::Int32(-8)));

    // Strict refuses the same updates.
    let c = e.set_validator("shop", "c", Some(strict_error())).unwrap();
    invalid(e.replace(&c, &DocId::Int64(2), doc! { "qty": -9 }, false).unwrap_err());
    invalid(e.modify_where(&c, &only_two, &SetQty(-9), None).unwrap_err());
}

#[test]
fn deleting_and_an_unchanged_write_are_not_validated() {
    let (e, _d) = open();
    collection(&e, strict_error());
    let unvalidated = e.set_validator("shop", "c", None).unwrap();
    e.insert(&unvalidated, doc! { "_id": 2i64, "qty": -1 }).unwrap();
    let c = e.set_validator("shop", "c", Some(strict_error())).unwrap();
    // Sent again unchanged: nothing is written, so nothing is judged.
    e.replace(&c, &DocId::Int64(2), doc! { "qty": -1 }, false).unwrap();
    assert!(e.delete(&c, &DocId::Int64(2)).unwrap());
}

/// `b` holding `a`'s collection by entries, then `b` made to hold the validator.
fn replica_with_validator(a: &Engine) -> (Engine, tempfile::TempDir) {
    let (b, dir) = open();
    let entries = a.entries_for_peer(Hlc::ZERO, 100, crate::WalkScope::Request).unwrap().entries;
    b.apply_batch(&entries).unwrap();
    b.set_validator("shop", "c", Some(strict_error())).unwrap();
    (b, dir)
}

#[test]
fn a_replicated_write_is_never_refused_and_nothing_held_is_rechecked() {
    let (a, _da) = open();
    a.create_collection("shop", "c").unwrap();
    let ca = a.get_collection("shop", "c").unwrap();
    let (b, _db) = replica_with_validator(&a);
    let cb = b.get_collection("shop", "c").unwrap();

    // `a` has no validator, so it accepts what `b`'s would refuse.
    a.insert(&ca, doc! { "_id": 1i64, "qty": -1 }).unwrap();
    a.insert(&ca, doc! { "_id": 2i64, "note": "no qty" }).unwrap();
    let entries = a.entries_for_peer(Hlc::ZERO, 100, crate::WalkScope::Request).unwrap().entries;
    b.apply_batch(&entries).unwrap();
    assert_eq!(qty_of(&b, &cb, 1), Some(Bson::Int32(-1)), "kept");
    assert!(b.get(&cb, &DocId::Int64(2)).unwrap().is_some(), "kept");
    assert!(b.validation_counts().is_empty(), "and never judged");

    // The same member refuses the same document from a client.
    invalid(b.insert(&cb, doc! { "_id": 3i64, "qty": -1 }).unwrap_err());

    // Setting a validator does not recheck what is held; the report lists it.
    let page = b.validator_violations(&cb, None, 100).unwrap();
    let ids: Vec<Bson> = page.rows.iter().map(|(id, _)| id.clone()).collect();
    assert_eq!(ids, vec![Bson::Int64(1), Bson::Int64(2)]);
    assert_eq!(page.scanned, 2);
}

#[test]
fn a_snapshot_page_is_never_refused_either() {
    let (a, _da) = open();
    a.create_collection("shop", "c").unwrap();
    let ca = a.get_collection("shop", "c").unwrap();
    a.insert(&ca, doc! { "_id": 1i64, "qty": -1 }).unwrap();
    let (b, _db) = replica_with_validator(&a);
    let cb = b.get_collection("shop", "c").unwrap();
    transfer(&b, &a);
    assert_eq!(qty_of(&b, &cb, 1), Some(Bson::Int32(-1)));
}

fn transfer(into: &Engine, from: &Engine) {
    let mut progress = crate::SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        let page: crate::SnapshotPage =
            bson::deserialize_from_slice(&bson::serialize_to_vec(&page).unwrap()).unwrap();
        into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
    }
}

#[test]
fn the_validator_reaches_a_replica_by_entries_and_removal_follows() {
    let (a, _da) = open();
    a.create_collection("shop", "c").unwrap();
    let (b, _db) = open();
    let sync = |from: &Engine, into: &Engine| {
        let entries =
            from.entries_for_peer(Hlc::ZERO, 100, crate::WalkScope::Request).unwrap().entries;
        into.apply_batch(&entries).unwrap();
    };
    sync(&a, &b);

    let on = a.set_validator("shop", "c", Some(strict_error())).unwrap();
    sync(&a, &b);
    let held = b.get_collection("shop", "c").unwrap();
    assert_eq!(held.validator, Some(strict_error()));
    assert_eq!(held.validator_changed, on.validator_changed);
    invalid(b.insert(&held, doc! { "_id": 1i64, "qty": -1 }).unwrap_err());

    let off = a.set_validator("shop", "c", None).unwrap();
    sync(&a, &b);
    let held = b.get_collection("shop", "c").unwrap();
    assert_eq!(held.validator, None);
    assert_eq!(held.validator_changed, off.validator_changed);
    b.insert(&held, doc! { "_id": 1i64, "qty": -1 }).unwrap();
}

#[test]
fn the_later_stamp_wins_whatever_order_the_changes_arrive_in() {
    let (a, _da) = open();
    a.create_collection("shop", "c").unwrap();
    let first = a.set_validator("shop", "c", Some(strict_error())).unwrap();
    let warn = spec(ValidationAction::Warn, ValidationLevel::Moderate);
    let second = a.set_validator("shop", "c", Some(warn.clone())).unwrap();
    assert!(second.validator_changed > first.validator_changed);

    let changes: Vec<_> = a
        .entries_for_peer(Hlc::ZERO, 100, crate::WalkScope::Request)
        .unwrap()
        .entries
        .into_iter()
        .filter(|entry| entry.kind == OpKind::ConfigureValidator)
        .collect();
    assert_eq!(changes.len(), 2);
    let creation: Vec<_> = a
        .entries_for_peer(Hlc::ZERO, 100, crate::WalkScope::Request)
        .unwrap()
        .entries
        .into_iter()
        .filter(|entry| entry.kind == OpKind::CreateCollection)
        .collect();

    for order in [[0, 1], [1, 0]] {
        let (b, _db) = open();
        b.apply_batch(&creation).unwrap();
        for i in order {
            b.apply_batch(std::slice::from_ref(&changes[i])).unwrap();
        }
        let held = b.get_collection("shop", "c").unwrap();
        assert_eq!(held.validator, Some(warn.clone()), "order {order:?}");
        assert_eq!(held.validator_changed, second.validator_changed);
    }
}

#[test]
fn a_snapshot_carries_the_validator_and_says_when_it_was_removed() {
    let (a, _da) = open();
    a.create_collection("shop", "c").unwrap();
    let on = a.set_validator("shop", "c", Some(strict_error())).unwrap();

    let (b, _db) = open();
    transfer(&b, &a);
    let held = b.get_collection("shop", "c").unwrap();
    assert_eq!(held.validator, Some(strict_error()));
    assert_eq!(held.validator_changed, on.validator_changed);

    // "Off at s": the page carries the removal's stamp, and the receiver that held
    // the validator drops it.
    let off = a.set_validator("shop", "c", None).unwrap();
    let page = a.snapshot_page(None, None).unwrap();
    let state = page.collections.iter().find(|c| c.name == "c").unwrap();
    assert!(state.validator.is_none() && state.validator_changed == off.validator_changed);
    transfer(&b, &a);
    let held = b.get_collection("shop", "c").unwrap();
    assert_eq!(held.validator, None);
    assert_eq!(held.validator_changed, off.validator_changed);
}

#[test]
fn a_page_older_than_the_standing_change_does_not_override_it() {
    let (a, _da) = open();
    a.create_collection("shop", "c").unwrap();
    a.set_validator("shop", "c", None).unwrap();
    let (b, _db) = open();
    transfer(&b, &a);
    // `b` changes it after `a`'s removal; `a`'s page is now the older fact.
    let b_set = b.set_validator("shop", "c", Some(strict_error())).unwrap();
    transfer(&b, &a);
    let held = b.get_collection("shop", "c").unwrap();
    assert_eq!(held.validator, Some(strict_error()));
    assert_eq!(held.validator_changed, b_set.validator_changed);
}

#[test]
fn a_page_whose_stamp_its_senders_coverage_does_not_name_is_ignored() {
    let (a, _da) = open();
    a.create_collection("shop", "c").unwrap();
    a.set_validator("shop", "c", Some(strict_error())).unwrap();
    let (b, _db) = open();
    let mut progress = crate::SnapshotProgress::whole_database();
    let mut page = a.snapshot_page(None, None).unwrap();
    for state in &mut page.collections {
        if let Some(stamp) = &mut state.validator_changed {
            *stamp = Stamp::new(Hlc::new(stamp.hlc.wall_ms + 10_000_000, 0), stamp.node);
        }
    }
    b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();
    assert_eq!(b.get_collection("shop", "c").unwrap().validator, None);
}

#[test]
fn the_validator_is_part_of_the_definition_digest() {
    let (e, _d) = open();
    let none = e.create_collection("shop", "c").unwrap();
    let on = e.set_validator("shop", "c", Some(strict_error())).unwrap();
    let warn = e
        .set_validator("shop", "c", Some(spec(ValidationAction::Warn, ValidationLevel::Strict)))
        .unwrap();
    let digest = crate::definition_digest::definition_digest;
    assert_ne!(digest(&none), digest(&on));
    assert_ne!(digest(&on), digest(&warn), "the policies are hashed");
    let again = e.set_validator("shop", "c", Some(strict_error())).unwrap();
    assert_eq!(digest(&on), digest(&again), "and the stamp is not");
    assert_eq!(
        crate::definition_digest::definition_list(&on).last().map(|(k, _)| k.clone()),
        Some(crate::definition_digest::DefinitionKey::validator())
    );
}

#[test]
fn a_stored_schema_that_no_longer_parses_leaves_the_collection_writable() {
    let (e, _d) = open();
    let c = collection(&e, strict_error());
    // A build that changed the dialect: the stored schema carries a keyword this one
    // refuses. Set past the parse that a client's change is held to.
    let mut meta = c.clone();
    meta.validator = Some(ValidatorSpec {
        schema: doc! { "$ref": "#/x" },
        action: ValidationAction::Error,
        level: ValidationLevel::Strict,
    });
    e.insert(&meta, doc! { "_id": 5i64, "qty": -1 }).expect("not validated, not wedged");
    e.insert(&meta, doc! { "_id": 6i64, "qty": -1 }).unwrap();
    assert_eq!(e.validators_unparseable(), 1, "counted once per change of it, not per write");
    assert!(e.validator_violations(&meta, None, 10).unwrap().rows.is_empty());
}

#[test]
fn the_report_pages_through_a_scan_and_names_why() {
    let (e, _d) = open();
    collection(&e, strict_error());
    let c = e.set_validator("shop", "c", None).unwrap();
    for i in 2..=7i64 {
        let doc = if i % 2 == 0 {
            doc! { "_id": i, "qty": -1 }
        } else {
            doc! { "_id": i, "qty": 1 }
        };
        e.insert(&c, doc).unwrap();
    }
    let c = e.set_validator("shop", "c", Some(strict_error())).unwrap();

    let first = e.validator_violations(&c, None, 2).unwrap();
    assert_eq!(
        first.rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
        vec![Bson::Int64(2), Bson::Int64(4)]
    );
    assert!(first.rows[0].1.contains("qty"), "{:?}", first.rows[0]);
    let next = first.next.clone().expect("more to scan");
    let second = e.validator_violations(&c, Some(&next), 2).unwrap();
    assert_eq!(second.rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>(), vec![Bson::Int64(6)]);
    assert_eq!(second.next, None, "the scan reached the end");

    // With no validator the report is empty.
    let c = e.set_validator("shop", "c", None).unwrap();
    assert!(e.validator_violations(&c, None, 10).unwrap().rows.is_empty());
}

#[test]
fn a_meta_written_before_the_validator_existed_reads_without_one() {
    // The store written by 0.57.0 holds no `validator` and no `validator_changed`:
    // both are optional with defaults, so it opens as it is and no migration exists.
    let (e, _d) = open();
    let current = e.create_collection("shop", "c").unwrap();
    let mut json: serde_json::Value = serde_json::to_value(&current).unwrap();
    let object = json.as_object_mut().unwrap();
    assert!(!object.contains_key("validator"), "absent when unset");
    assert!(!object.contains_key("validator_changed"), "absent when unset");
    object.remove("validator");
    object.remove("validator_changed");
    let old: crate::CollectionMeta = serde_json::from_value(json).unwrap();
    assert_eq!(old, current);
    assert_eq!(old.validator, None);
    assert_eq!(old.validator_changed, None);
}

#[test]
fn a_snapshot_page_from_before_the_validator_existed_carries_none() {
    let (a, _da) = open();
    a.create_collection("shop", "c").unwrap();
    let page = a.snapshot_page(None, None).unwrap();
    let mut doc = bson::serialize_to_document(&page.collections[0]).unwrap();
    assert!(!doc.contains_key("validator") && !doc.contains_key("validator_changed"));
    doc.remove("validator");
    let state: crate::CollectionState = bson::deserialize_from_document(doc).unwrap();
    assert!(state.validator.is_none() && state.validator_changed.is_none());
}

#[test]
fn the_oplog_kind_round_trips_through_its_tag() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    e.set_validator("shop", "c", Some(strict_error())).unwrap();
    let entry = e
        .entries_for_peer(Hlc::ZERO, 100, crate::WalkScope::Request)
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.kind == OpKind::ConfigureValidator)
        .unwrap();
    let back = crate::codec::decode_oplog_entry(&crate::codec::encode_oplog_entry(&entry)).unwrap();
    assert_eq!(back, entry);
    assert!(entry.kind.is_ddl() && !entry.kind.is_document());
    let body: kimmy_core::ValidatorSet =
        bson::deserialize_from_slice(&entry.body.unwrap()).unwrap();
    assert_eq!(body.validator, Some(strict_error()));
}

// -- the stored schema keeps its types (ADR-182's rule, ADR-257) ----------------

/// A schema holding a value of every type the relaxed JSON form loses or blurs:
/// a generic binary, a `NaN`, both infinities, an `Int64` beside an `Int32` that
/// reads the same, a `Decimal128`, a date and an `ObjectId`.
fn typed_schema() -> Document {
    use bson::{Binary, Decimal128, spec::BinarySubtype};
    doc! {
        "bsonType": "object",
        "properties": {
            "bin": { "enum": [Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 3] })] },
            "low": { "bsonType": "int", "minimum": f64::NEG_INFINITY },
            "high": { "maximum": f64::INFINITY },
            "nan": { "enum": [f64::NAN, 1] },
            "wide": { "enum": [Bson::Int64(5), Bson::Int32(6)] },
            "dec": { "multipleOf": "1.50".parse::<Decimal128>().unwrap() },
            "date": { "enum": [bson::DateTime::from_millis(1_700_000_000_123)] },
            "oid": { "enum": [bson::oid::ObjectId::parse_str("0123456789abcdef01234567").unwrap()] },
        },
        "required": ["bin"],
    }
}

fn typed_spec() -> ValidatorSpec {
    ValidatorSpec {
        schema: typed_schema(),
        action: ValidationAction::Error,
        level: ValidationLevel::Strict,
    }
}

/// The schema `held` carries is exactly `typed_schema`, every type kept: compared
/// by canonical Extended JSON, where `Int64(5)` and `Int32(5)`, a binary and its
/// bytes, and a `NaN` and a `null` are all different.
fn assert_typed(held: &crate::CollectionMeta, what: &str) {
    let validator = held.validator.as_ref().unwrap_or_else(|| panic!("{what}: no validator"));
    assert_eq!(
        kimmy_core::validator::stored_schema::canonical(&validator.schema),
        kimmy_core::validator::stored_schema::canonical(&typed_schema()),
        "{what}: the schema read back is not the schema set"
    );
    assert_eq!(validator, &typed_spec(), "{what}");
}

/// What the typed schema must still decide, whichever path it came by.
fn assert_enforced(e: &Engine, what: &str) {
    let c = e.get_collection("shop", "c").unwrap();
    let bin = Bson::Binary(bson::Binary {
        subtype: bson::spec::BinarySubtype::Generic,
        bytes: vec![1, 2, 3],
    });
    let refused = |r: crate::Result<DocId>| {
        assert!(
            matches!(r, Err(StorageError::Core(CoreError::DocumentInvalid { .. }))),
            "{what}: {r:?}"
        );
    };
    e.insert(&c, doc! { "_id": 1i64, "bin": bin.clone() })
        .unwrap_or_else(|x| panic!("{what}: {x:?}"));
    // The listed binary is accepted and its bytes as an array are not.
    refused(e.insert(&c, doc! { "_id": 2i64, "bin": [1, 2, 3] }));
    // `required` and `bsonType` stand: a validator that did not parse would let both by.
    refused(e.insert(&c, doc! { "_id": 3i64 }));
    refused(e.insert(&c, doc! { "_id": 4i64, "bin": bin.clone(), "low": "text" }));
    // A `NaN` in an `enum` is listed; a `null` is not.
    e.insert(&c, doc! { "_id": 5i64, "bin": bin.clone(), "nan": f64::NAN }).unwrap();
    refused(e.insert(&c, doc! { "_id": 6i64, "bin": bin.clone(), "nan": Bson::Null }));
    // `-Infinity` as a floor is no floor.
    e.insert(&c, doc! { "_id": 7i64, "bin": bin.clone(), "low": i32::MIN }).unwrap();
    assert_eq!(e.validators_unparseable(), 0, "{what}: the stored schema parses");
}

#[test]
fn the_stored_schema_keeps_every_type_through_the_store_and_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let e = Engine::open(&path).unwrap();
        e.create_collection("shop", "c").unwrap();
        e.set_validator("shop", "c", Some(typed_spec())).unwrap();
        // Read back from the store, not the value `set_validator` returned.
        assert_typed(&e.get_collection("shop", "c").unwrap(), "same session");
        assert_enforced(&e, "same session");
    }
    let e = Engine::open(&path).unwrap();
    assert_typed(&e.get_collection("shop", "c").unwrap(), "after a restart");
    // The documents the first session accepted are held, so the enforcement is
    // checked against a fresh collection of the same validator.
    e.create_collection("shop", "d").unwrap();
    e.set_validator("shop", "d", Some(typed_spec())).unwrap();
    drop(e);
    let e = Engine::open(&path).unwrap();
    assert_typed(&e.get_collection("shop", "d").unwrap(), "a second restart");
    let c = e.get_collection("shop", "d").unwrap();
    let r = e.insert(&c, doc! { "_id": 1i64, "bin": [1, 2, 3] });
    assert!(matches!(r, Err(StorageError::Core(CoreError::DocumentInvalid { .. }))), "{r:?}");
}

#[test]
fn a_typed_schema_enforces_what_it_says_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let e = Engine::open(&path).unwrap();
        e.create_collection("shop", "c").unwrap();
        e.set_validator("shop", "c", Some(typed_spec())).unwrap();
    }
    let e = Engine::open(&path).unwrap();
    assert_enforced(&e, "after a restart");
}

#[test]
fn a_typed_schema_survives_an_entry_a_snapshot_page_and_a_restart_on_a_replica() {
    let (a, _da) = open();
    a.create_collection("shop", "c").unwrap();
    a.set_validator("shop", "c", Some(typed_spec())).unwrap();

    // By oplog entries, each through its stored byte form and BSON on the wire.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let b = Engine::open(&path).unwrap();
        let entries: Vec<_> = a
            .entries_for_peer(Hlc::ZERO, 100, crate::WalkScope::Request)
            .unwrap()
            .entries
            .iter()
            .map(|entry| {
                crate::codec::decode_oplog_entry(&crate::codec::encode_oplog_entry(entry)).unwrap()
            })
            .collect();
        let body = entries
            .iter()
            .find(|entry| entry.kind == OpKind::ConfigureValidator)
            .and_then(|entry| entry.body.clone())
            .unwrap();
        let set: kimmy_core::ValidatorSet = bson::deserialize_from_slice(&body).unwrap();
        assert_eq!(set.validator, Some(typed_spec()), "the entry body");
        b.apply_batch(&entries).unwrap();
        assert_typed(&b.get_collection("shop", "c").unwrap(), "by entries");
    }
    let b = Engine::open(&path).unwrap();
    assert_typed(&b.get_collection("shop", "c").unwrap(), "by entries, after a restart");
    assert_enforced(&b, "by entries, after a restart");

    // By snapshot page.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let b = Engine::open(&path).unwrap();
        let page = a.snapshot_page(None, None).unwrap();
        let page: crate::SnapshotPage =
            bson::deserialize_from_slice(&bson::serialize_to_vec(&page).unwrap()).unwrap();
        let state = page.collections.iter().find(|c| c.name == "c").unwrap();
        assert_eq!(state.validator, Some(typed_spec()), "the page");
        transfer(&b, &a);
        assert_typed(&b.get_collection("shop", "c").unwrap(), "by page");
    }
    let b = Engine::open(&path).unwrap();
    assert_typed(&b.get_collection("shop", "c").unwrap(), "by page, after a restart");
    assert_enforced(&b, "by page, after a restart");
}

#[test]
fn identical_typed_definitions_are_not_a_conflict_with_themselves() {
    // A `NaN` in the schema made the derived `==` of the metadata false against
    // itself; the definition check compares encodings, and so does the spec.
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    e.set_validator("shop", "c", Some(typed_spec())).unwrap();
    // The same change again, and a retry over it, complete rather than loop.
    e.set_validator("shop", "c", Some(typed_spec())).unwrap();
    let held = e.get_collection("shop", "c").unwrap();
    assert_eq!(held.validator, Some(typed_spec()));
    assert_eq!(
        crate::definition_digest::validator_hash(held.validator.as_ref().unwrap()),
        crate::definition_digest::validator_hash(&typed_spec())
    );
}

// -- a collection created with its validator (ADR-257) -------------------------

fn plain(schema: Document) -> ValidatorSpec {
    ValidatorSpec { schema, action: ValidationAction::Error, level: ValidationLevel::Strict }
}

fn entries_of(e: &Engine) -> Vec<kimmy_core::OplogEntry> {
    e.entries_for_peer(Hlc::ZERO, 1000, crate::WalkScope::Request).unwrap().entries
}

#[test]
fn creating_a_collection_with_a_validator_is_one_commit_and_one_entry() {
    let (plain_engine, _dp) = open();
    let before = plain_engine.commits();
    plain_engine.create_collection("shop", "c").unwrap();
    let one_creation = plain_engine.commits() - before;

    let (e, _d) = open();
    let before = e.commits();
    let made = e.create_collection_with_validator("shop", "c", strict_error()).unwrap();
    assert_eq!(e.commits() - before, one_creation, "the collection and its validator, one commit");

    // Standing, read back from the store, with the validator stamped by the creation.
    let held = e.get_collection("shop", "c").unwrap();
    assert_eq!(held.validator, Some(strict_error()));
    assert_eq!(held.validator_changed.map(|s| s.hlc), Some(held.created));
    assert_eq!(held, made);

    // One entry: the creation, carrying the validator. No `ConfigureValidator` follows it.
    let kinds: Vec<OpKind> = entries_of(&e).iter().map(|entry| entry.kind).collect();
    assert_eq!(kinds, [OpKind::CreateCollection], "{kinds:?}");
    let create = &entries_of(&e)[0];
    assert_eq!(create.stamp.hlc, held.created);
    let body: kimmy_core::CollectionCreate =
        bson::deserialize_from_slice(create.body.as_ref().unwrap()).unwrap();
    assert_eq!(body.validator, Some(strict_error()));

    // It enforces from the first write.
    invalid(e.insert(&held, doc! { "_id": 1i64, "qty": -1 }).unwrap_err());
    e.insert(&held, doc! { "_id": 2i64, "qty": 1 }).unwrap();

    // A change after it is a later one, and wins.
    let off = e.set_validator("shop", "c", None).unwrap();
    assert!(off.validator_changed > held.validator_changed);
}

#[test]
fn a_refused_create_with_a_validator_leaves_nothing_behind() {
    let (e, _d) = open();
    // A schema that does not parse: no collection, no entry.
    let err = e
        .create_collection_with_validator("shop", "c", plain(doc! { "$ref": "#/definitions/x" }))
        .unwrap_err();
    assert!(matches!(err, StorageError::Core(CoreError::InvalidQuery(_))), "{err:?}");
    assert!(matches!(
        e.get_collection("shop", "c"),
        Err(StorageError::Core(CoreError::CollectionNotFound { .. }))
    ));
    assert!(entries_of(&e).is_empty());

    // A name that exists keeps what it has: no validator is laid over it.
    e.create_collection("shop", "d").unwrap();
    let err = e.create_collection_with_validator("shop", "d", strict_error()).unwrap_err();
    assert!(matches!(err, StorageError::Core(CoreError::CollectionExists { .. })), "{err:?}");
    assert_eq!(e.get_collection("shop", "d").unwrap().validator, None);
    assert_eq!(entries_of(&e).len(), 1, "the one plain creation");

    // A shadow name cannot take a validator, created or set.
    assert!(e.create_collection_with_validator("shop", "__vec_c", strict_error()).is_err());
}

#[test]
fn a_member_never_holds_a_collection_created_with_a_validator_without_it() {
    let (a, _da) = open();
    a.create_collection_with_validator("shop", "c", typed_spec()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kimmy.redb");
    {
        let b = Engine::open(&path).unwrap();
        // The entry alone, each through its stored byte form, as a peer is served it.
        let entries: Vec<_> = entries_of(&a)
            .iter()
            .map(|entry| {
                crate::codec::decode_oplog_entry(&crate::codec::encode_oplog_entry(entry)).unwrap()
            })
            .collect();
        assert_eq!(entries.len(), 1);
        // What applying a plain creation costs, for comparison.
        let plain_apply = {
            let (src, _ds) = open();
            src.create_collection("shop", "c").unwrap();
            let (fresh, _df) = open();
            let at = fresh.commits();
            fresh.apply_batch(&entries_of(&src)).unwrap();
            fresh.commits() - at
        };
        let at = b.commits();
        b.apply_batch(&entries).unwrap();
        assert_eq!(b.commits() - at, plain_apply, "the applied creation is the same commits");
        assert_typed(&b.get_collection("shop", "c").unwrap(), "applied");
        assert_eq!(
            b.get_collection("shop", "c").unwrap().validator_changed,
            Some(entries[0].stamp),
            "stamped by the entry that carried it"
        );
    }
    let b = Engine::open(&path).unwrap();
    assert_typed(&b.get_collection("shop", "c").unwrap(), "after a restart");
    assert_enforced(&b, "after a restart");
}

#[test]
fn a_creation_met_again_does_not_bring_back_a_validator_that_was_removed() {
    let (a, _da) = open();
    a.create_collection_with_validator("shop", "c", strict_error()).unwrap();
    let (b, _db) = open();
    let creation = entries_of(&a);
    b.apply_batch(&creation).unwrap();
    assert_eq!(b.get_collection("shop", "c").unwrap().validator, Some(strict_error()));

    // Removed here, later: the creation met again (a re-served window) is the older change.
    a.set_validator("shop", "c", None).unwrap();
    b.apply_batch(&entries_of(&a)).unwrap();
    assert_eq!(b.get_collection("shop", "c").unwrap().validator, None);
    b.apply_batch(&creation).unwrap();
    assert_eq!(b.get_collection("shop", "c").unwrap().validator, None, "last writer wins");
}

#[test]
fn a_creation_carrying_a_validator_reaches_a_member_that_made_the_name_itself() {
    let (a, _da) = open();
    a.create_collection_with_validator("shop", "c", strict_error()).unwrap();
    // The name was made here, plain, before the creation arrived.
    let (b, _db) = open();
    b.create_collection("shop", "c").unwrap();
    b.apply_batch(&entries_of(&a)).unwrap();
    assert_eq!(b.get_collection("shop", "c").unwrap().validator, Some(strict_error()));
}

// -- the reviewer's probes, adopted ----------------------------------------------

#[test]
fn an_upsert_that_inserts_is_checked_under_moderate() {
    let (e, _d) = open();
    e.create_collection("shop", "c").unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    e.insert(&c, doc! { "_id": 1i64, "n": "bad" }).unwrap();
    e.insert(&c, doc! { "_id": 2i64, "n": 1 }).unwrap();
    e.set_validator(
        "shop",
        "c",
        Some(ValidatorSpec {
            schema: doc! { "properties": { "n": { "bsonType": "int" } } },
            action: ValidationAction::Error,
            level: ValidationLevel::Moderate,
        }),
    )
    .unwrap();
    let c = e.get_collection("shop", "c").unwrap();
    // Replace of the document that did not conform: free.
    e.replace(&c, &DocId::Int64(1), doc! { "n": "still bad" }, false).unwrap();
    // Replace of the one that did, with a bad image: refused.
    invalid(e.replace(&c, &DocId::Int64(2), doc! { "n": "now bad" }, false).unwrap_err());
    // An upsert that inserts is an insert, and is checked.
    invalid(e.replace(&c, &DocId::Int64(3), doc! { "n": "bad" }, true).unwrap_err());
    e.replace(&c, &DocId::Int64(3), doc! { "n": 3 }, true).unwrap();
}

#[test]
fn an_old_lifes_validator_entry_does_not_reach_the_new_life() {
    let (a, _da) = open();
    a.create_collection("shop", "c").unwrap();
    a.set_validator("shop", "c", Some(plain(doc! { "required": ["old"] }))).unwrap();
    a.drop_collection("shop", "c").unwrap();
    a.create_collection("shop", "c").unwrap();
    let all = entries_of(&a);
    let (late, rest): (Vec<_>, Vec<_>) =
        all.iter().cloned().partition(|e| e.kind == OpKind::ConfigureValidator);
    assert_eq!(late.len(), 1, "premise: the first life's validator entry");
    // The validator entry delivered last, after the drop and the new life, and in order.
    for (name, batches) in
        [("validator last", vec![rest.clone(), late.clone()]), ("in order", vec![all.clone()])]
    {
        let (b, _db) = open();
        for batch in batches {
            b.apply_batch(&batch).unwrap();
        }
        let held = b.get_collection("shop", "c").unwrap();
        assert_eq!(held.validator, None, "{name}: the first life's validator reached the second");
        b.insert(&held, doc! { "_id": 1i64, "x": 1 }).expect("the new life has no validator");
    }
    assert_eq!(a.get_collection("shop", "c").unwrap().validator, None);
}

#[test]
fn a_reserved_validator_of_a_dropped_life_does_not_reach_the_next() {
    let (a, _da) = open();
    let (b, _db) = open();
    a.create_collection("shop", "c").unwrap();
    a.set_validator("shop", "c", Some(plain(doc! { "required": ["old"] }))).unwrap();
    a.drop_collection("shop", "c").unwrap();
    a.create_collection("shop", "c").unwrap();
    let theirs = a.version_vector().unwrap();
    let whole = a.entries_for_peer(Hlc::ZERO, 1000, crate::WalkScope::Background).unwrap();
    b.apply_peer_batch(&theirs, &whole.entries, whole.scanned_to, whole.exhausted).unwrap();
    assert!(b.get_collection("shop", "c").unwrap().validator.is_none());
    // The first life's validator entry served again, alone, to a member holding the
    // drop and the new life.
    let at = whole.entries.iter().position(|e| e.kind == OpKind::ConfigureValidator).unwrap();
    let one = &whole.entries[at..at + 1];
    b.apply_peer_batch(&theirs, one, one[0].stamp.hlc, false).unwrap();
    assert!(
        b.get_collection("shop", "c").unwrap().validator.is_none(),
        "the first life's validator reached the second"
    );
}
