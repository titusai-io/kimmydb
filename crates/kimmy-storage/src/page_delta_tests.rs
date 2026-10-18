//! The records delta on every snapshot, scoped and converge page (B5, "Snapshots
//! carry records"), and the fresh row a page store writes.
//!
//! The residual this closes: a keyed write on the sender after its collection's
//! records phase and before the page that holds its document. Without the delta
//! that page brings the effect without the record, and a retry on the receiver
//! applies the request a second time. R1, R1b and R1c are the G1 review's
//! reproductions of it (scoped, a retry before the pull's end, whole-database).

use std::cmp::Ordering;
use std::sync::Arc;

use bson::{Bson, Document, doc};
use kimmy_core::{DocId, Fingerprint, Hlc, NodeId, ScopeHash, Stamp, VersionVector};

use crate::idempotency::{Keyed, KeyedWrite, RecordState, RecordsVia};
use crate::{
    Candidates, CollectionMeta, Engine, ModifySpec, PeerWindow, SNAPSHOT_PAGE, ServeVerdict,
    SnapshotCursor, SnapshotPage, SnapshotProgress, SyncOutcome,
};

struct Inc;

impl ModifySpec for Inc {
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

fn open(dir: &tempfile::TempDir, name: &str) -> Engine {
    Engine::open(&dir.path().join(format!("{name}.redb"))).unwrap()
}

fn key(n: u8) -> KeyedWrite {
    KeyedWrite::new(ScopeHash([n; 32]), Fingerprint([n; 32]))
}

/// A key of its own for each `i`, all of one request shape.
fn key_of(i: u64) -> KeyedWrite {
    let mut scope = [0u8; 32];
    scope[..8].copy_from_slice(&i.to_be_bytes());
    KeyedWrite::new(ScopeHash(scope), Fingerprint([1; 32]))
}

fn fresh<T: std::fmt::Debug>(answer: Keyed<T>) -> T {
    match answer {
        Keyed::Fresh(value) => value,
        other => panic!("expected a fresh write, got {other:?}"),
    }
}

fn count(engine: &Engine, coll: &CollectionMeta) -> u64 {
    engine.count(coll, crate::walk::WalkScope::Request).unwrap()
}

fn n_of(engine: &Engine, coll: &CollectionMeta, id: i64) -> i64 {
    engine.get(coll, &DocId::Int64(id)).unwrap().unwrap().get_i64("n").unwrap()
}

/// One pull of `to` from `from`, as the listener serves it: the verdict first,
/// the requester's held marks named, the window from its witnessed vector, and
/// the records it carries.
fn pull(to: &Engine, from: &Engine) {
    for _ in 0..16 {
        let theirs = from.version_vector().unwrap();
        let mine = to.witnessed_vector().unwrap();
        assert_eq!(from.serve_verdict(&mine).unwrap(), ServeVerdict::Serve);
        let Some(start) = mine.behind(&theirs) else { return };
        let marked = to.held_ranges_covered_by(&mine).unwrap();
        let window = from.serve_entries_to_peer(start, 1024, Some(&mine), &marked, None).unwrap();
        let mut outcome = SyncOutcome::default();
        to.apply_peer_window_carrying_into(
            &theirs,
            Some(&mine),
            &window.entries,
            &window.records,
            RecordsVia::Pull,
            PeerWindow {
                scanned_to: window.scanned_to,
                exhausted: window.exhausted,
                passed_through: None,
                asked_partial: false,
            },
            &mut outcome,
        )
        .unwrap();
        if window.exhausted {
            return;
        }
    }
}

/// The next page of `progress` from `from`, applied on `to`.
fn take(to: &Engine, from: &Engine, progress: &mut SnapshotProgress) -> SnapshotPage {
    let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
    to.apply_snapshot_page(from.node_id(), progress, &page).unwrap();
    page
}

fn finish(to: &Engine, from: &Engine, progress: &mut SnapshotProgress) -> Vec<SnapshotPage> {
    let mut pages = Vec::new();
    while !progress.is_complete() {
        pages.push(take(to, from, progress));
    }
    pages
}

/// The assertion the residual's tests end with: `m` holds the request's effect,
/// so a retry there must replay and leave the counter as it is.
fn retry_must_replay(m: &Engine, cm: &CollectionMeta, k: &KeyedWrite, id: i64) {
    let before = n_of(m, cm, id);
    let held = m.idempotency_record(cm.id, &k.scope).unwrap();
    let retry = m.modify_where_keyed(cm, &Candidates::Scan, &Inc, Some(1), k).unwrap();
    let after = n_of(m, cm, id);
    assert!(
        held.is_some() && matches!(retry, Keyed::Replayed(_)) && after == before,
        "the receiver holds the effect (n = {before}) without the record ({held:?}); the \
         retry answered {retry:?} and n is now {after}"
    );
}

/// `engine`'s record rows at its own origin.
fn own_rows(engine: &Engine) -> usize {
    use redb::ReadableDatabase;
    let txn = engine.db().begin_read().unwrap();
    let rows = txn.open_table(crate::tables::IDEMPOTENCY_BY_STAMP).unwrap();
    let own = engine.node_id().to_bytes();
    let mut n = 0;
    for row in redb::ReadableTable::iter(&rows).unwrap() {
        let (key, _) = row.unwrap();
        if key.value().starts_with(&own) {
            n += 1;
        }
    }
    n
}

/// Whether a page is records-only: records and no documents, the cursor moved
/// only in its delta fields.
fn records_only(page: &SnapshotPage, asked: &SnapshotCursor) -> bool {
    let Some(next) = &page.next else { return false };
    page.documents.is_empty()
        && page.deleted_documents.is_empty()
        && next.records_from.is_some()
        && (next.collection, &next.after_key, &next.records)
            == (asked.collection, &asked.after_key, &asked.records)
}

/// A collection on `y` whose documents take three pages, the one at `target`
/// a counter.
fn three_pages(y: &Engine, target: i64) -> CollectionMeta {
    let cy = y.create_collection("shop", "orders").unwrap();
    let docs = (1..(3 * SNAPSHOT_PAGE as i64))
        .map(|i| {
            if i == target {
                doc! { "_id": i, "n": 10i64 }
            } else {
                doc! { "_id": i, "v": i }
            }
        })
        .collect();
    y.insert_many(&cy, docs).unwrap();
    cy
}

// -- The residual -----------------------------------------------------------------------------

/// R1: a scoped pull (the count half's repair of a collection X lacks) takes the
/// collection's records phase and first page; Y's keyed `$inc` lands on a
/// document of the second page; X, serving, takes that page. The page's delta
/// carries the record, stored before the document in the page's transaction.
#[test]
fn r1_a_keyed_write_between_the_records_phase_and_its_page_reaches_the_receiver_with_its_record() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let cy = y.create_collection("shop", "orders").unwrap();
    let last = SNAPSHOT_PAGE as i64 + 40;
    y.insert_many(&cy, (1..last).map(|i| doc! { "_id": i, "v": i }).collect()).unwrap();
    y.insert(&cy, doc! { "_id": last, "n": 10i64 }).unwrap();
    let mut progress = SnapshotProgress::of_collection(cy.id);
    let page = take(&x, &y, &mut progress);
    assert!(page.next.is_some(), "premise: the document is on a later page");
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let pages = finish(&x, &y, &mut progress);
    assert_eq!(pages[0].records.len(), 1, "the next page's delta: {:?}", pages[0].records);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&x, &cx, last), 11, "premise: the page brought the effect");
    retry_must_replay(&x, &cx, &k, last);
}

/// R1b: the same, with the retry between the page that brings the effect and
/// the pull's end: a delta only at completion would leave this open.
#[test]
fn r1b_a_retry_between_the_effects_page_and_the_pulls_end_replays() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    take(&x, &y, &mut progress);
    assert!(!progress.is_complete(), "premise: the pull is not over");
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&x, &cx, target), 11, "premise: this page brought the effect");
    retry_must_replay(&x, &cx, &k, target);
}

/// R1c: the whole-database route. The collection's records phase (it holds an
/// earlier key's record) and its first page of documents go; the keyed write
/// lands on a document of its second page.
#[test]
fn r1c_a_whole_database_page_brings_an_effect_with_a_record_that_came_after_the_phase() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let cy = y.create_collection("shop", "orders").unwrap();
    let last = SNAPSHOT_PAGE as i64 + 40;
    y.insert_many(&cy, (1..last).map(|i| doc! { "_id": i, "v": i }).collect()).unwrap();
    y.insert(&cy, doc! { "_id": last, "n": 10i64 }).unwrap();
    fresh(y.insert_keyed(&cy, doc! { "_id": 0i64, "v": 0i64 }, &key(8)).unwrap());
    let mut progress = SnapshotProgress::whole_database();
    let page = take(&x, &y, &mut progress);
    assert!(!page.records.is_empty() && page.next.is_some(), "premise: phase, then more");
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    finish(&x, &y, &mut progress);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&x, &cx, last), 11, "premise: the snapshot brought the effect");
    retry_must_replay(&x, &cx, &k, last);
}

/// The next page's base is read in the page's own transaction, the documents'.
/// A write committed after the page's read and before its cursor is cut is not
/// on the page and not under its base, so the next page's delta carries it.
#[test]
fn the_next_pages_base_is_read_in_the_pages_own_transaction() {
    use crate::sync::race_hooks::{Race, race};
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (Arc::new(open(&dir, "y")), open(&dir, "x"));
    let target = 2 * SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let k = key(7);
    let writer = Arc::clone(&y);
    let coll = cy.clone();
    let (page, ()) = race(
        Race::PageRead,
        move || {
            fresh(writer.modify_where_keyed(&coll, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
        },
        || y.snapshot_page(progress.after().cloned(), progress.scope()).unwrap(),
    );
    assert!(page.records.is_empty(), "premise: the write is not on this page");
    x.apply_snapshot_page(y.node_id(), &mut progress, &page).unwrap();
    let pages = finish(&x, &y, &mut progress);
    assert_eq!(pages[0].records.len(), 1, "the next page's delta carries it");
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&x, &cx, target), 11, "premise: the pull brought the effect");
    retry_must_replay(&x, &cx, &k, target);
}

/// A page's records commit with its documents, in its one transaction: right
/// after the commit that brings an effect, a retry on the receiver replays.
#[test]
fn a_pages_records_commit_with_its_documents() {
    use crate::sync::race_hooks::{Race, race};
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), Arc::new(open(&dir, "x")));
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let page = y.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
    let receiver = Arc::clone(&x);
    let retry = move || {
        let cx = receiver.get_collection("shop", "orders").unwrap();
        let held = receiver.idempotency_record(cx.id, &k.scope).unwrap();
        let n = n_of(&receiver, &cx, target);
        let retry = receiver.modify_where_keyed(&cx, &Candidates::Scan, &Inc, Some(1), &k).unwrap();
        (held.is_some(), n, matches!(retry, Keyed::Replayed(_)), n_of(&receiver, &cx, target))
    };
    let (applied, (held, before, replayed, after)) = race(Race::PageCommitted, retry, || {
        x.apply_snapshot_page(y.node_id(), &mut progress, &page)
    });
    applied.unwrap();
    assert_eq!(before, 11, "premise: the commit brought the effect");
    assert!(held && replayed && after == before, "held {held}, replayed {replayed}, n {after}");
}

// -- Paging and resuming ----------------------------------------------------------------------

/// A delta larger than [`crate::idempotency::RECORD_PAGE`] goes as records-only
/// pages, each leaving the cursor's document position as it came, and then the
/// documents; one larger than the byte budget the same, and a record larger than
/// the byte budget goes alone. Every record and every document arrives.
#[test]
fn a_delta_larger_than_a_page_goes_as_records_only_pages() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let cy = three_pages(&y, 1);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    // More records than a page, written between two pages.
    let n = crate::idempotency::RECORD_PAGE as u64 + 60;
    for i in 0..n {
        let id = 10_000 + i as i64;
        fresh(y.insert_keyed(&cy, doc! { "_id": id, "v": 1 }, &key_of(i)).unwrap());
    }
    let asked = progress.after().cloned().unwrap();
    let page = take(&x, &y, &mut progress);
    assert!(records_only(&page, &asked), "a records-only page: {:?}", page.next);
    assert_eq!(page.records.len(), crate::idempotency::RECORD_PAGE);
    let asked = progress.after().cloned().unwrap();
    let page = take(&x, &y, &mut progress);
    assert_eq!(page.records.len(), 60, "the rest of the delta");
    assert!(!page.documents.is_empty(), "then the documents, on the page that ends the delta");
    assert_eq!(
        page.documents[0].id,
        DocId::Int64(SNAPSHOT_PAGE as i64 + 1),
        "from where the cursor stood before the delta: {asked}"
    );
    assert_eq!(page.next.as_ref().and_then(|next| next.records_from), None);

    // By bytes: a budget of three records, then of less than one.
    let one = page.records[0].record.len() + 96;
    y.set_record_delta_budget_for_test(Some((3 * one, crate::idempotency::RECORD_DELTA_ROWS)));
    for i in n..n + 7 {
        let id = 10_000 + i as i64;
        fresh(y.insert_keyed(&cy, doc! { "_id": id, "v": 1 }, &key_of(i)).unwrap());
    }
    let mut sizes = Vec::new();
    loop {
        let asked = progress.after().cloned().unwrap();
        let page = take(&x, &y, &mut progress);
        sizes.push(page.records.len());
        if !records_only(&page, &asked) {
            break;
        }
    }
    assert_eq!(sizes, vec![3, 3, 1], "by bytes");
    y.set_record_delta_budget_for_test(Some((1, crate::idempotency::RECORD_DELTA_ROWS)));
    for i in n + 7..n + 9 {
        let id = 10_000 + i as i64;
        fresh(y.insert_keyed(&cy, doc! { "_id": id, "v": 1 }, &key_of(i)).unwrap());
    }
    let mut sizes = Vec::new();
    loop {
        let asked = progress.after().cloned().unwrap();
        let page = take(&x, &y, &mut progress);
        sizes.push(page.records.len());
        if !records_only(&page, &asked) {
            break;
        }
    }
    assert_eq!(sizes, vec![1, 1], "a record larger than the budget goes alone");
    y.set_record_delta_budget_for_test(None);
    finish(&x, &y, &mut progress);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(count(&x, &cx), count(&y, &cy), "every document, once");
    assert_eq!(x.idempotency_records().unwrap(), n + 9, "every record");
}

/// The rows one page's delta reads are bounded: a scoped delta over rows of
/// other collections goes on over pages, records-only pages that may carry
/// nothing, and still finds its own collection's record behind them.
#[test]
fn a_delta_reads_a_bounded_number_of_rows_a_page() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    let other = y.create_collection("shop", "other").unwrap();
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    y.set_record_delta_budget_for_test(Some((crate::idempotency::RECORD_DELTA_BYTES, 4)));
    for i in 0..10u64 {
        fresh(y.insert_keyed(&other, doc! { "_id": i as i64 }, &key_of(i)).unwrap());
    }
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let mut shapes = Vec::new();
    loop {
        let asked = progress.after().cloned().unwrap();
        let page = take(&x, &y, &mut progress);
        let only = records_only(&page, &asked);
        shapes.push((only, page.records.len()));
        if !only {
            break;
        }
    }
    assert_eq!(shapes, vec![(true, 0), (true, 0), (false, 1)], "4 rows a page, 11 rows");
    y.set_record_delta_budget_for_test(None);
    finish(&x, &y, &mut progress);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(x.idempotency_records().unwrap(), 1, "only the scoped collection's");
    retry_must_replay(&x, &cx, &k, target);
}

/// The base rides the cursor the receiver persists (`SNAPSHOT_PROGRESS`): a pull
/// resumed after a restart is first sent every record that changed since, as
/// records-only pages when it is more than a page, a restart part-way through
/// those included, with the document position as it was; then the documents.
#[test]
fn a_resumed_pull_is_first_sent_every_record_that_changed_since() {
    let dir = tempfile::tempdir().unwrap();
    let y = open(&dir, "y");
    let x_path = dir.path().join("x.redb");
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    {
        let x = Engine::open(&x_path).unwrap();
        take(&x, &y, &mut progress);
    }
    // The receiver stops; meanwhile the sender takes more keyed writes than a
    // page holds, one of them on a document still to come.
    let n = crate::idempotency::RECORD_PAGE as u64 + 30;
    for i in 0..n {
        let id = 10_000 + i as i64;
        fresh(y.insert_keyed(&cy, doc! { "_id": id, "v": 1 }, &key_of(i)).unwrap());
    }
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let x = Engine::open(&x_path).unwrap();
    let mut resumed = x.snapshots_to_resume().unwrap();
    assert_eq!(resumed.len(), 1);
    let (peer, mut progress) = resumed.pop().unwrap();
    assert_eq!(peer, y.node_id());
    let asked = progress.after().cloned().unwrap();
    assert!(asked.records_base.is_some(), "the base was persisted: {asked:?}");
    let page = take(&x, &y, &mut progress);
    assert!(records_only(&page, &asked), "records-only first: {:?}", page.next);
    drop(x);
    // And again, part-way through the delta.
    let x = Engine::open(&x_path).unwrap();
    let (_, mut progress) = x.snapshots_to_resume().unwrap().pop().unwrap();
    let at = progress.after().cloned().unwrap();
    assert!(at.records_from.is_some(), "where the delta stopped was persisted: {at:?}");
    assert_eq!(
        (at.collection, &at.after_key, &at.records),
        (asked.collection, &asked.after_key, &asked.records)
    );
    let page = take(&x, &y, &mut progress);
    assert!(!page.documents.is_empty() && !page.records.is_empty(), "the rest, then documents");
    finish(&x, &y, &mut progress);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(x.idempotency_records().unwrap(), n + 1, "every record");
    assert_eq!(count(&x, &cx), count(&y, &cy), "every document, once");
    retry_must_replay(&x, &cx, &k, target);
}

// -- Compatibility and the wire ---------------------------------------------------------------

/// A cursor without a base, the first page's or one a 0.51 receiver echoes
/// having dropped the field, carries no delta; with the base the same page
/// carries it. The base survives the wire's BSON and the JSON the receiver
/// persists its progress in, and a value of another shape reads as none.
#[test]
fn a_cursor_without_a_base_carries_no_delta_and_the_base_crosses_bson_and_json() {
    let dir = tempfile::tempdir().unwrap();
    let y = open(&dir, "y");
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    let first = y.snapshot_page(None, Some(cy.id)).unwrap();
    assert!(first.records.is_empty(), "the first page carries no delta");
    let next = first.next.clone().unwrap();
    assert!(next.records_base.is_some(), "its cursor carries the next page's base");
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &key(7)).unwrap());
    let with = y.snapshot_page(Some(next.clone()), Some(cy.id)).unwrap();
    assert_eq!(with.records.len(), 1);

    // A 0.51 receiver echoes `{collection, after_key}` (and the records
    // position, as it never knew it, is none).
    #[derive(serde::Serialize, serde::Deserialize)]
    struct OlderCursor {
        collection: kimmy_core::CollectionId,
        after_key: Vec<u8>,
    }
    let older: OlderCursor =
        bson::deserialize_from_slice(&bson::serialize_to_vec(&next).unwrap()).unwrap();
    let echoed: SnapshotCursor =
        bson::deserialize_from_slice(&bson::serialize_to_vec(&older).unwrap()).unwrap();
    assert_eq!((echoed.records_base.clone(), echoed.records_from), (None, None));
    let without = y.snapshot_page(Some(echoed), Some(cy.id)).unwrap();
    assert!(without.records.is_empty(), "no base, no delta");
    assert_eq!(without.documents, with.documents, "the same page otherwise");

    // Both fields, through BSON and JSON, alone and inside the persisted progress.
    let node = NodeId::from_bytes([3; 16]);
    let mut base = VersionVector::new();
    base.insert(node, Hlc::new(1_700_000_000_000, 4));
    base.insert(NodeId::from_bytes([9; 16]), Hlc::new(12, 0));
    let cursor = SnapshotCursor {
        records_base: Some(base),
        records_from: Some(Stamp::new(Hlc::new(1_700_000_000_000, 2), node)),
        ..next
    };
    let wire: SnapshotCursor =
        bson::deserialize_from_slice(&bson::serialize_to_vec(&cursor).unwrap()).unwrap();
    assert_eq!(wire, cursor, "BSON");
    let persisted: SnapshotCursor =
        serde_json::from_slice(&serde_json::to_vec(&cursor).unwrap()).unwrap();
    assert_eq!(persisted, cursor, "JSON");
    let none = SnapshotCursor { records_base: None, records_from: None, ..cursor.clone() };
    let text = serde_json::to_string(&none).unwrap();
    assert!(!text.contains("records_base") && !text.contains("records_from"), "{text}");

    // A value of another shape is none, in either.
    let mut json = serde_json::to_value(&cursor).unwrap();
    json["records_base"] = serde_json::json!([1, 2, 3]);
    json["records_from"] = serde_json::json!("later");
    let read: SnapshotCursor = serde_json::from_value(json).unwrap();
    assert_eq!((read.records_base, read.records_from), (None, None));
    let mut wire = bson::serialize_to_document(&cursor).unwrap();
    wire.insert("records_base", 5i32);
    wire.insert("records_from", doc! { "hlc": "no" });
    let read: SnapshotCursor = bson::deserialize_from_document(wire).unwrap();
    assert_eq!((read.records_base, read.records_from), (None, None));
}

// -- Page stores ------------------------------------------------------------------------------

/// A page store that changes a record writes it, its rows and a row at a fresh
/// stamp of the receiver's own; one that changes nothing writes nothing, and the
/// page commits nothing.
#[test]
fn a_page_store_writes_a_fresh_row_when_it_changes_a_record_and_nothing_when_it_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let cy = y.create_collection("shop", "orders").unwrap();
    fresh(y.insert_keyed(&cy, doc! { "_id": 1i64, "n": 0i64 }, &key(7)).unwrap());
    let page = y.snapshot_page(None, Some(cy.id)).unwrap();
    assert_eq!(page.records.len(), 1);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    x.apply_snapshot_page(y.node_id(), &mut progress, &page).unwrap();
    assert_eq!(own_rows(&x), 1, "the fresh row");
    let commits = x.commits();
    let mut again = SnapshotProgress::of_collection(cy.id);
    x.apply_snapshot_page(y.node_id(), &mut again, &page).unwrap();
    assert_eq!(x.commits(), commits, "a page that changes nothing commits nothing");
    assert_eq!(own_rows(&x), 1, "and writes no fresh row");
    assert_eq!(x.idempotency_records_received()[4], 1, "counted once, when it changed");
}

/// The page-store edge: a member serving a snapshot of a collection takes a
/// converge page of it, and the page changes a record whose rows are all at
/// stamps the member had witnessed when it cut its last page. Here the record
/// becomes `done` by a record-only commit, which keeps its `last`: the member
/// pulled the request's last chunk before that commit. The fresh row puts the
/// changed record in the next page's delta, so the snapshot's receiver ends
/// with `done`, and a retry there replays instead of resuming.
#[test]
fn a_record_a_member_takes_by_page_while_serving_a_snapshot_rides_its_next_page() {
    use crate::sync::race_hooks::{Race, at};
    let dir = tempfile::tempdir().unwrap();
    let (y, s, m) = (Arc::new(open(&dir, "y")), Arc::new(open(&dir, "s")), open(&dir, "m"));
    let cy = y.create_collection("shop", "orders").unwrap();
    y.insert(&cy, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    y.insert(&cy, doc! { "_id": 2i64, "n": 0i64 }).unwrap();
    y.insert_many(
        &cy,
        (100..(100 + SNAPSHOT_PAGE as i64)).map(|i| doc! { "_id": i, "v": i }).collect(),
    )
    .unwrap();
    pull(&s, &y);
    // A two-chunk request; S pulls it after its last writing chunk and before
    // the record-only commit that marks it done.
    y.set_multi_chunk_docs(1);
    let k = key(7);
    let (puller, from) = (Arc::clone(&s), Arc::clone(&y));
    at(Race::BetweenChunks, move || {
        at(Race::BetweenChunks, move || pull(&puller, &from));
    });
    let out = fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!(out.commits, 2);
    crate::sync::race_hooks::clear();
    let cs = s.get_collection("shop", "orders").unwrap();
    let held = s.idempotency_record(cs.id, &k.scope).unwrap().unwrap();
    assert_eq!(held.state, RecordState::Applying, "premise: S pulled it before `done`");
    assert_eq!(y.idempotency_record(cy.id, &k.scope).unwrap().unwrap().last, held.last);

    // M takes S's first page: the record as S holds it, and both documents.
    let mut progress = SnapshotProgress::of_collection(cs.id);
    let first = take(&m, &s, &mut progress);
    assert_eq!(first.records.len(), 1);
    assert!(first.next.is_some(), "premise: more pages");
    // S takes a converge page of the collection from Y: the record, done.
    let mut converge = SnapshotProgress::of_collection_converging(cs.id, Default::default());
    let page = y.snapshot_page(None, Some(cy.id)).unwrap();
    s.apply_snapshot_page(y.node_id(), &mut converge, &page).unwrap();
    assert_eq!(s.idempotency_record(cs.id, &k.scope).unwrap().unwrap().state, RecordState::Done);

    finish(&m, &s, &mut progress);
    let cm = m.get_collection("shop", "orders").unwrap();
    let record = m.idempotency_record(cm.id, &k.scope).unwrap().unwrap();
    assert_eq!(record.state, RecordState::Done, "S's next page carried the done record");
    assert_eq!((n_of(&m, &cm, 1), n_of(&m, &cm, 2)), (1, 1));
    let retry = m.modify_where_keyed(&cm, &Candidates::Scan, &Inc, None, &k).unwrap();
    assert!(matches!(retry, Keyed::Replayed(_)), "a done request replays: {retry:?}");
}

/// No keyed rows: every page's delta is an empty read, and the pages are what
/// they were without the delta, but for the base on the cursor.
#[test]
fn a_store_without_keys_pages_as_before() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let cy = three_pages(&y, 1);
    let mut progress = SnapshotProgress::whole_database();
    let pages = finish(&x, &y, &mut progress);
    assert_eq!(pages.len(), 3);
    assert!(pages.iter().all(|page| page.records.is_empty()));
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(count(&x, &cx), count(&y, &cy));
    assert_eq!(own_rows(&x), 0);
}

// -- The next base ----------------------------------------------------------------------------

/// Each origin's next base is the vector of the page that began reading its
/// rows: the page's own for a delta that ended on it; for one that stopped, the
/// base it came with for the origin it stopped in when an earlier page began
/// that one, and for every origin it has not reached.
#[test]
fn next_base_takes_each_origin_from_the_page_that_began_its_rows() {
    use crate::idempotency::next_base;
    let (a, b, c) =
        (NodeId::from_bytes([1; 16]), NodeId::from_bytes([2; 16]), NodeId::from_bytes([3; 16]));
    let vector = |hlcs: [u64; 3]| {
        let mut v = VersionVector::new();
        for (node, ms) in [a, b, c].into_iter().zip(hlcs) {
            v.insert(node, Hlc::new(ms, 0));
        }
        v
    };
    let (base, now) = (vector([10, 20, 30]), vector([11, 21, 31]));
    let get = |v: &VersionVector| [a, b, c].map(|node| v.get(node).wall_ms);
    assert_eq!(get(&next_base(&base, None, None, &now)), [11, 21, 31], "one page");
    assert_eq!(get(&next_base(&base, None, Some(b), &now)), [11, 21, 30], "stopped in b");
    assert_eq!(get(&next_base(&base, Some(b), None, &now)), [10, 20, 31], "went on in b");
    assert_eq!(get(&next_base(&base, Some(a), Some(a), &now)), [10, 20, 30], "still in a");
    assert_eq!(get(&next_base(&base, Some(a), Some(c), &now)), [10, 21, 31], "a to c");
}
