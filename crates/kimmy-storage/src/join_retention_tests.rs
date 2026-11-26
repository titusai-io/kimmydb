//! The join retention `R` of a `field` collection (the companion of U1): per collection, chosen by the creating
//! member (its tombstone retention), fixed at creation, replicated with the creation (the entry and the snapshot
//! page) and immutable, so every member computes the same horizon `H = max(vv) - R`. Never read from a local
//! setting after the creation.

use std::time::Duration;

use std::cmp::Ordering;

use bson::{Bson, Document, doc};
use kimmy_core::{Hlc, MergePolicy, OplogEntry};

use crate::{Engine, ModifySpec, SnapshotProgress, WalkScope};

const HOUR_MS: u64 = 3_600_000;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

/// Adds one to `n` on every match that has one.
struct Bump;

impl ModifySpec for Bump {
    fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
        Ok(doc.contains_key("n"))
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.insert("n", Bson::Int64(doc.get_i64("n").map_err(|e| e.to_string())? + 1));
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

/// A whole-database snapshot of `from`, applied to `into`, page by page.
fn take_a_snapshot(into: &Engine, from: &Engine) {
    let mut progress = SnapshotProgress::whole_database();
    let mut after = None;
    for _ in 0..50 {
        let page = from.snapshot_page(after.take(), None).unwrap();
        into.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
        after = page.next.clone();
        if after.is_none() {
            return;
        }
    }
    panic!("the snapshot did not end");
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

#[test]
fn a_collection_created_elsewhere_holds_the_creators_join_retention_and_not_its_own() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.set_default_join_retention(Duration::from_millis(HOUR_MS));
    b.set_default_join_retention(Duration::from_millis(48 * HOUR_MS));
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    assert_eq!(ca.join_retention_ms, Some(HOUR_MS), "premise: the creator's own value");

    b.apply_batch(&everything(&a)).unwrap();

    let cb = b.get_collection("shop", "orders").unwrap();
    assert_eq!(
        cb.join_retention_ms,
        Some(HOUR_MS),
        "B holds A's join retention, not its own ({} h)",
        48
    );
}

#[test]
fn a_collection_taken_by_snapshot_holds_the_creators_join_retention() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.set_default_join_retention(Duration::from_millis(HOUR_MS));
    b.set_default_join_retention(Duration::from_millis(48 * HOUR_MS));
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let page = a.snapshot_page(None, None).unwrap();

    let mut progress = SnapshotProgress::whole_database();
    b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();

    let cb = b.get_collection("shop", "orders").unwrap();
    assert_eq!(cb.join_retention_ms, Some(HOUR_MS), "the snapshot carries R");
}

#[test]
fn the_join_retention_does_not_change_after_creation() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.set_default_join_retention(Duration::from_millis(HOUR_MS));
    a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let creation = everything(&a);
    b.apply_batch(&creation).unwrap();

    // The creator's own setting moves, and the creation is served again: neither changes the collection.
    a.set_default_join_retention(Duration::from_millis(99 * HOUR_MS));
    b.set_default_join_retention(Duration::from_millis(7 * HOUR_MS));
    b.apply_batch(&creation).unwrap();

    for engine in [&a, &b] {
        let held = engine.get_collection("shop", "orders").unwrap();
        assert_eq!(held.join_retention_ms, Some(HOUR_MS), "R does not change after creation");
    }
}

#[test]
fn the_horizon_of_a_collection_uses_its_own_join_retention() {
    let (a, _da) = engine();
    a.set_default_join_retention(Duration::from_millis(HOUR_MS));
    let short = a.create_collection_with_policy("shop", "short", MergePolicy::Field, None).unwrap();
    a.set_default_join_retention(Duration::from_millis(48 * HOUR_MS));
    let long = a.create_collection_with_policy("shop", "long", MergePolicy::Field, None).unwrap();

    for (coll, expected) in [(&short, HOUR_MS), (&long, 48 * HOUR_MS)] {
        let id = a.insert(coll, doc! { "_id": 1 }).unwrap();
        let record = a.get_versioned(coll, &id).unwrap().expect("the document");
        drop(record);
        let writer = a.field_writer(coll).unwrap();
        assert_eq!(writer.retention_ms, expected, "the writer carries the collection's R");
        let raw = {
            use redb::ReadableDatabase;
            let txn = a.db().begin_read().unwrap();
            let docs = txn.open_table(crate::tables::DOCS).unwrap();
            let key = crate::docs::doc_key(&id).unwrap();
            docs.get((coll.id.0, key.as_slice())).unwrap().unwrap().value().to_vec()
        };
        let record = crate::codec::decode_doc_record_with_meta(&raw).unwrap();
        let state = crate::field_write::load(record.meta.as_deref()).unwrap();
        let newest = state.vv.values().map(|h| h.wall_ms).max().unwrap();
        assert_eq!(
            state.horizon(&writer.options()).wall_ms,
            newest.saturating_sub(expected),
            "the horizon of a collection uses its own R"
        );
    }
}

/// A creation of a `field` collection that carries no `R` is not a valid creation: refused as corrupt by name,
/// before anything is written, whether it arrives as an entry or on a page.
#[test]
fn a_creation_of_a_field_collection_that_carries_no_join_retention_is_refused_as_corrupt() {
    let (a, _da) = engine();
    a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();

    let (b, _db) = engine();
    let mut creation = everything(&a).last().unwrap().clone();
    let mut body: kimmy_core::CollectionCreate =
        bson::deserialize_from_slice(creation.body.as_deref().unwrap()).unwrap();
    body.join_retention_ms = None;
    creation.body = Some(bson::serialize_to_vec(&body).unwrap());
    match b.apply_batch(std::slice::from_ref(&creation)) {
        Err(crate::StorageError::Corrupt(why)) => {
            assert!(why.contains("holds no join retention"), "{why}");
        }
        other => panic!("an entry that carries no R is refused as corrupt, not {other:?}"),
    }
    assert!(b.get_collection("shop", "orders").is_err(), "nothing was created");

    let (c, _dc) = engine();
    let mut page = a.snapshot_page(None, None).unwrap();
    page.collections[0].join_retention_ms = None;
    let mut progress = SnapshotProgress::whole_database();
    match c.apply_snapshot_page(a.node_id(), &mut progress, &page) {
        Err(crate::StorageError::Corrupt(why)) => {
            assert!(why.contains("holds no join retention"), "{why}");
        }
        other => panic!("a page that carries no R is refused as corrupt, not {other:?}"),
    }
    assert!(c.get_collection("shop", "orders").is_err(), "nothing was created");
}

/// The same refusal where the creation meets a collection this member holds already and would take its policy
/// (the later creation wins): a creation of a `field` collection that carries no `R`, met by an entry or a page,
/// is refused as corrupt and the held collection keeps what it had.
#[test]
fn a_later_creation_that_carries_no_join_retention_is_refused_where_it_would_be_adopted() {
    let (b, _db) = engine();
    b.create_collection("shop", "orders").unwrap();
    std::thread::sleep(Duration::from_millis(10));
    let (a, _da) = engine();
    a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();

    let mut creation = everything(&a).last().unwrap().clone();
    let mut body: kimmy_core::CollectionCreate =
        bson::deserialize_from_slice(creation.body.as_deref().unwrap()).unwrap();
    body.join_retention_ms = None;
    creation.body = Some(bson::serialize_to_vec(&body).unwrap());
    match b.apply_batch(std::slice::from_ref(&creation)) {
        Err(crate::StorageError::Corrupt(why)) => {
            assert!(why.contains("holds no join retention"), "{why}");
        }
        other => panic!("a later entry that carries no R is refused as corrupt, not {other:?}"),
    }

    let mut page = a.snapshot_page(None, None).unwrap();
    page.collections[0].join_retention_ms = None;
    let mut progress = SnapshotProgress::whole_database();
    match b.apply_snapshot_page(a.node_id(), &mut progress, &page) {
        Err(crate::StorageError::Corrupt(why)) => {
            assert!(why.contains("holds no join retention"), "{why}");
        }
        other => panic!("a later page that carries no R is refused as corrupt, not {other:?}"),
    }
    let held = b.get_collection("shop", "orders").unwrap();
    assert_eq!(held.merge_policy, MergePolicy::Document, "the held collection keeps its policy");
    assert_eq!(held.join_retention_ms, None);
}

/// Every route that creates a `field` collection gives it an `R`: the local create, a creation entry from
/// another member and a snapshot page; a collection of any other policy holds none.
#[test]
fn every_route_that_creates_a_field_collection_gives_it_a_join_retention() {
    let (a, _da) = engine();
    a.set_default_join_retention(Duration::from_millis(HOUR_MS));
    let local =
        a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    assert_eq!(local.join_retention_ms, Some(HOUR_MS), "the local create takes the default");
    let document = a.create_collection("shop", "plain").unwrap();
    assert_eq!(document.join_retention_ms, None, "a document collection has none");
    let system = a.create_system_collection("shop", "__system").unwrap();
    assert_eq!(system.join_retention_ms, None, "nor does a system collection");

    let (b, _db) = engine();
    b.apply_batch(&everything(&a)).unwrap();
    assert_eq!(
        b.get_collection("shop", "orders").unwrap().join_retention_ms,
        Some(HOUR_MS),
        "the entry route carries the creator's R"
    );

    let (c, _dc) = engine();
    take_a_snapshot(&c, &a);
    assert_eq!(
        c.get_collection("shop", "orders").unwrap().join_retention_ms,
        Some(HOUR_MS),
        "the snapshot route carries the creator's R"
    );
}

/// A write to a collection of any other policy asks for no writer and so needs no `R`: an update, a delete
/// and a replace of a `document` collection are as they were.
#[test]
fn a_document_collection_is_written_and_updated_without_a_join_retention() {
    let (a, _da) = engine();
    let c = a.create_collection("shop", "plain").unwrap();
    let id = a.insert(&c, doc! { "_id": 1, "n": 1i64 }).unwrap();
    a.replace(&c, &id, doc! { "_id": 1, "n": 2i64 }, false).unwrap();
    a.modify_where(&c, &crate::Candidates::Scan, &Bump, None).unwrap();
    assert!(a.delete(&c, &id).unwrap());
}

/// The tripwire of "never read from a local setting after the creation": the join options of a field write
/// are built in one place from the collection's own `R`, so no production code names the library default.
#[test]
fn no_production_code_builds_join_options_from_a_local_source() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if !name.ends_with(".rs") || name.ends_with("_tests.rs") || name == "field_write.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let production = text.split("#[cfg(test)]\nmod ").next().unwrap();
        assert!(
            !production.contains("JoinOptions::default()")
                && !production.contains("JoinOptions::new("),
            "no local-config source of R: {name} builds join options"
        );
    }
}

/// Unit P (the 11:36 AM ruling): the same creation met again with another `R` is no longer refused. The options row orders the two (the
/// later creation, and at one `Hlc` and one policy the greater encoding of the whole row, as text), and the definition follows the row.
#[test]
fn a_creation_met_again_with_another_join_retention_is_settled_by_the_options_row_and_not_refused()
{
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.set_default_join_retention(Duration::from_millis(HOUR_MS));
    a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let creation = everything(&a);
    b.apply_batch(&creation).unwrap();

    let mut again = creation.last().unwrap().clone();
    let mut body: kimmy_core::CollectionCreate =
        bson::deserialize_from_slice(again.body.as_deref().unwrap()).unwrap();
    body.join_retention_ms = Some(2 * HOUR_MS);
    again.body = Some(bson::serialize_to_vec(&body).unwrap());
    let outcome = b.apply_batch(std::slice::from_ref(&again)).unwrap();

    assert_eq!(outcome.ddl_refused, 0, "no refusal is counted: {outcome:?}");
    let held = b.get_collection("shop", "orders").unwrap();
    assert_eq!(
        held.join_retention_ms,
        Some(2 * HOUR_MS),
        "the greater encoding of the row stands (7200000 over 3600000, as text)"
    );
}

/// The same for a snapshot page: it is settled by the options row, and is no error of the pull.
#[test]
fn a_page_carrying_another_join_retention_for_the_same_creation_is_settled_by_the_options_row() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.set_default_join_retention(Duration::from_millis(HOUR_MS));
    a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    let mut page = a.snapshot_page(None, None).unwrap();
    let mut progress = SnapshotProgress::whole_database();
    b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();

    page.collections[0].join_retention_ms = Some(2 * HOUR_MS);
    let mut progress = SnapshotProgress::whole_database();
    b.apply_snapshot_page(a.node_id(), &mut progress, &page).expect("the page applies");

    let held = b.get_collection("shop", "orders").unwrap();
    assert_eq!(held.join_retention_ms, Some(2 * HOUR_MS), "the greater encoding of the row stands");
}

/// Two creations of one name under the same policy and different `R` settle on the later creation's `R`, on every
/// member, whichever creation reaches it first.
#[test]
fn the_join_retention_follows_the_later_creation_on_every_member() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    a.set_default_join_retention(Duration::from_millis(HOUR_MS));
    b.set_default_join_retention(Duration::from_millis(48 * HOUR_MS));
    a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    std::thread::sleep(Duration::from_millis(10));
    b.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();

    let (from_a, from_b) = (everything(&a), everything(&b));
    a.apply_batch(&from_b).unwrap();
    b.apply_batch(&from_a).unwrap();

    for engine in [&a, &b] {
        let held = engine.get_collection("shop", "orders").unwrap();
        assert_eq!(
            held.join_retention_ms,
            Some(48 * HOUR_MS),
            "every member ends with the later creation's R"
        );
    }
}
