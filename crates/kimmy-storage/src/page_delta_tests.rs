//! The records delta on every snapshot, scoped and converge page (B5, "Snapshots
//! carry records"), read from the sender's change log of idempotency records.
//!
//! The residual this closes: a keyed write on the sender after its collection's
//! records phase and before the page that holds its document. Without the delta
//! that page brings the effect without the record, and a retry on the receiver
//! applies the request a second time. R1, R1b and R1c are the G1 review's
//! reproductions of it (scoped, a retry before the pull's end, whole-database);
//! R2 is a record the sender stores below its own witnessed vector, by the replay
//! path, which only a log of changes, and not a vector of stamps, sees.

use std::cmp::Ordering;
use std::sync::Arc;

use bson::{Bson, Document, doc};
use kimmy_core::{DocId, Fingerprint, Hlc, NodeId, OpKind, OplogEntry, ScopeHash, Stamp};

use crate::idempotency::{
    CarriedRecord, IdempotencyRecord, Keyed, KeyedWrite, ModifyRecorded, RecordOutcome,
    RecordState, RecordsVia,
};
use crate::{
    Candidates, CollectionMeta, Engine, ModifySpec, PeerWindow, RecordsBase, SNAPSHOT_PAGE,
    ServeVerdict, SnapshotCursor, SnapshotPage, SnapshotProgress, SyncOutcome,
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

/// Begin `y`'s change log, no change in it: a sender whose records retention
/// has collected since its log began (`idempotency::begin_log_for_test`).
fn begun(y: &Engine) {
    crate::idempotency::begin_log_for_test(y).unwrap();
}

/// The rows of `engine`'s change log, where it begins not counted.
fn log_rows(engine: &Engine) -> usize {
    use redb::ReadableDatabase;
    let txn = engine.db().begin_read().unwrap();
    let log = txn.open_table(crate::tables::IDEMPOTENCY_CHANGES).unwrap();
    let mut n = 0;
    for row in redb::ReadableTable::iter(&log).unwrap() {
        let (key, _) = row.unwrap();
        n += usize::from(!key.value().is_empty());
    }
    n
}

/// `pull`, counting the records the windows carried.
fn pull_counting(to: &Engine, from: &Engine) -> usize {
    let mut carried = 0;
    for _ in 0..16 {
        let theirs = from.version_vector().unwrap();
        let mine = to.witnessed_vector().unwrap();
        let Some(start) = mine.behind(&theirs) else { return carried };
        let marked = to.held_ranges_covered_by(&mine).unwrap();
        let window = from.serve_entries_to_peer(start, 1024, Some(&mine), &marked, None).unwrap();
        carried += window.records.len();
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
            return carried;
        }
    }
    carried
}

/// Whether a page is records-only: no documents, the cursor moved only in its
/// base.
fn records_only(page: &SnapshotPage, asked: &SnapshotCursor) -> bool {
    let Some(next) = &page.next else { return false };
    page.documents.is_empty()
        && page.deleted_documents.is_empty()
        && next.records_base.is_some()
        && next.records_base != asked.records_base
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
    // Its log has begun, so the base names an epoch and the next page's delta
    // answers it (a zero base would be refused at the first keyed write).
    begun(&y);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    take(&x, &y, &mut progress);
    assert!(!progress.is_complete(), "premise: the pull is not over");
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&x, &cx, target), 11, "premise: this page brought the effect");
    retry_must_replay(&x, &cx, &k, target);
    // Each change rides one page: the next base is above it.
    let rest = finish(&x, &y, &mut progress);
    assert!(rest.iter().all(|page| page.records.is_empty()), "sent once");
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
    // Its log has begun, so the base names an epoch and the next page's delta
    // answers it (a zero base would be refused at the first keyed write).
    begun(&y);
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

/// A delta is bounded by the frame budget, not by a page of records: more than
/// [`crate::idempotency::RECORD_PAGE`] changed records ride one page with its
/// documents. One larger than the byte budget goes as records-only pages, each
/// leaving the cursor's document position as it came and moving its base, and
/// then the documents with the rest; a record no page can carry is refused,
/// naming it. Every other record and every document arrives.
#[test]
fn a_delta_larger_than_the_byte_budget_goes_as_records_only_pages() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let cy = three_pages(&y, 1);
    // Its log has begun, so the base names an epoch and the next page's delta
    // answers it (a zero base would be refused at the first keyed write).
    begun(&y);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let n = crate::idempotency::RECORD_PAGE as u64 + 60;
    for i in 0..n {
        let id = 10_000 + i as i64;
        fresh(y.insert_keyed(&cy, doc! { "_id": id, "v": 1 }, &key_of(i)).unwrap());
    }
    let page = take(&x, &y, &mut progress);
    assert_eq!(page.records.len(), n as usize, "more than a page of records, on one page");
    assert_eq!(page.documents[0].id, DocId::Int64(SNAPSHOT_PAGE as i64 + 1));

    // By bytes: a budget of three records, then of less than one.
    let one = page.records[0].record.len() + 96;
    y.set_record_delta_budget_for_test(Some((3 * one, crate::idempotency::RECORD_DELTA_ROWS)));
    for i in n..n + 7 {
        let id = 10_000 + i as i64;
        fresh(y.insert_keyed(&cy, doc! { "_id": id, "v": 1 }, &key_of(i)).unwrap());
    }
    let mut sizes = Vec::new();
    let mut position = None;
    loop {
        let asked = progress.after().cloned().unwrap();
        let page = take(&x, &y, &mut progress);
        sizes.push(page.records.len());
        if !records_only(&page, &asked) {
            assert!(!page.documents.is_empty(), "the page with the rest has documents");
            break;
        }
        let at = (asked.collection, asked.after_key.clone(), asked.records.clone());
        assert!(position.get_or_insert(at.clone()) == &at, "the document position stays");
    }
    assert_eq!(sizes, vec![3, 3, 1], "by bytes");
    // Less room than one record: no page can carry it, and it is refused, naming
    // it, rather than sent in a frame too large to cross.
    y.set_record_delta_budget_for_test(Some((1, crate::idempotency::RECORD_DELTA_ROWS)));
    fresh(y.insert_keyed(&cy, doc! { "_id": 20_000i64, "v": 1 }, &key_of(n + 7)).unwrap());
    let refused = y.snapshot_page(progress.after().cloned(), progress.scope());
    assert!(
        matches!(&refused, Err(crate::StorageError::SnapshotRowTooLarge { what, .. })
            if what.contains("idempotency record")),
        "the delta's record refused: {refused:?}"
    );
    y.set_record_delta_budget_for_test(None);
    finish(&x, &y, &mut progress);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(count(&x, &cx), count(&y, &cy), "every document, once");
    assert_eq!(x.idempotency_records().unwrap(), n + 8, "every record");
}

/// **A page carries documents only with the whole remainder** of the log above
/// its base. Keyed changes before and during a delta of several pages, one on a
/// document of the page that ends it: that page carries every record changed up
/// to its own read, so a retry on the receiver right after it replays.
#[test]
fn a_page_carries_documents_only_with_the_whole_remainder() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let (target, during) = (SNAPSHOT_PAGE as i64 + 40, SNAPSHOT_PAGE as i64 + 80);
    let cy = y.create_collection("shop", "orders").unwrap();
    let docs = (1..(3 * SNAPSHOT_PAGE as i64))
        .map(|i| {
            if i == target || i == during {
                doc! { "_id": i, "n": 10i64 }
            } else {
                doc! { "_id": i, "v": i }
            }
        })
        .collect();
    y.insert_many(&cy, docs).unwrap();
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    for i in 0..7u64 {
        fresh(y.insert_keyed(&cy, doc! { "_id": 10_000 + i as i64 }, &key_of(i)).unwrap());
    }
    let k = key(7);
    let on = |id: i64| Candidates::Keys(vec![crate::docs::doc_key(&DocId::Int64(id)).unwrap()]);
    fresh(y.modify_where_keyed(&cy, &on(target), &Inc, Some(1), &k).unwrap());
    let one = y.snapshot_page(None, Some(cy.id)).unwrap().records.len();
    assert_eq!(one, 8, "premise: eight records");
    let size = {
        let any = y.idempotency_record(cy.id, &key_of(0).scope).unwrap().unwrap();
        any.encode().len() + 96
    };
    y.set_record_delta_budget_for_test(Some((3 * size, crate::idempotency::RECORD_DELTA_ROWS)));
    let asked = progress.after().cloned().unwrap();
    let page = take(&x, &y, &mut progress);
    assert!(records_only(&page, &asked), "a records-only page first");
    // A keyed change during the delta, on a document of the page that ends it.
    let k2 = key(9);
    fresh(y.modify_where_keyed(&cy, &on(during), &Inc, Some(1), &k2).unwrap());
    loop {
        let asked = progress.after().cloned().unwrap();
        let page = take(&x, &y, &mut progress);
        if !records_only(&page, &asked) {
            break;
        }
    }
    y.set_record_delta_budget_for_test(None);
    let cx = x.get_collection("shop", "orders").unwrap();
    // The documents, at the real budget, up to the page that brings both effects.
    let holds = |id: i64| x.get(&cx, &DocId::Int64(id)).unwrap().is_some();
    while !(holds(target) && holds(during)) {
        take(&x, &y, &mut progress);
    }
    assert_eq!((n_of(&x, &cx, target), n_of(&x, &cx, during)), (11, 11), "premise: the page");
    assert!(!progress.is_complete(), "premise: the pull goes on");
    retry_must_replay(&x, &cx, &k, target);
    retry_must_replay(&x, &cx, &k2, during);
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
    // Its log has begun, so the base names an epoch and the next page's delta
    // answers it (a zero base would be refused at the first keyed write).
    begun(&y);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    y.set_record_delta_budget_for_test(Some((crate::SNAPSHOT_PAGE_BYTES, 4)));
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
/// records-only pages when it is more than a page holds, a restart part-way
/// through those included, with the document position as it was; then the
/// documents.
#[test]
fn a_resumed_pull_is_first_sent_every_record_that_changed_since() {
    let dir = tempfile::tempdir().unwrap();
    let y = open(&dir, "y");
    let x_path = dir.path().join("x.redb");
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    // Its log has begun, so the base names an epoch and the next page's delta
    // answers it (a zero base would be refused at the first keyed write).
    begun(&y);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    {
        let x = Engine::open(&x_path).unwrap();
        take(&x, &y, &mut progress);
    }
    // The receiver stops; meanwhile the sender takes keyed writes, one of them
    // on a document still to come, more than a page holds at the budget below.
    let n = 30u64;
    for i in 0..n {
        let id = 10_000 + i as i64;
        fresh(y.insert_keyed(&cy, doc! { "_id": id, "v": 1 }, &key_of(i)).unwrap());
    }
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let size = y.idempotency_record(cy.id, &key_of(0).scope).unwrap().unwrap().encode().len();
    y.set_record_delta_budget_for_test(Some((
        20 * (size + 96),
        crate::idempotency::RECORD_DELTA_ROWS,
    )));
    let x = Engine::open(&x_path).unwrap();
    let mut resumed = x.snapshots_to_resume().unwrap();
    assert_eq!(resumed.len(), 1);
    let (peer, mut progress) = resumed.pop().unwrap();
    assert_eq!(peer, y.node_id());
    let asked = progress.after().cloned().unwrap();
    assert!(asked.records_base.is_some(), "the base was persisted: {asked:?}");
    let page = take(&x, &y, &mut progress);
    assert!(records_only(&page, &asked), "records-only first: {:?}", page.next);
    assert_eq!(page.records.len(), 20);
    drop(x);
    // And again, part-way through the delta.
    let x = Engine::open(&x_path).unwrap();
    let (_, mut progress) = x.snapshots_to_resume().unwrap().pop().unwrap();
    let at = progress.after().cloned().unwrap();
    assert!(
        at.records_base.unwrap().key > asked.records_base.unwrap().key,
        "the base moved and was persisted: {at:?}"
    );
    assert_eq!(
        (at.collection, &at.after_key, &at.records),
        (asked.collection, &asked.after_key, &asked.records)
    );
    let page = take(&x, &y, &mut progress);
    assert_eq!(page.records.len(), 11, "the rest");
    assert!(!page.documents.is_empty(), "then documents");
    y.set_record_delta_budget_for_test(None);
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
    assert_eq!(echoed.records_base, None);
    let without = y.snapshot_page(Some(echoed), Some(cy.id)).unwrap();
    assert!(without.records.is_empty(), "no base, no delta");
    assert_eq!(without.documents, with.documents, "the same page otherwise");

    // The base, through BSON and JSON, alone and inside the persisted progress.
    let node = NodeId::from_bytes([3; 16]);
    let cursor = SnapshotCursor {
        records_base: Some(RecordsBase {
            key: Stamp::new(Hlc::new(1_700_000_000_000, 4), node),
            epoch: Some(-7_000_000_000_000_000_000),
        }),
        ..next
    };
    let wire: SnapshotCursor =
        bson::deserialize_from_slice(&bson::serialize_to_vec(&cursor).unwrap()).unwrap();
    assert_eq!(wire, cursor, "BSON");
    let persisted: SnapshotCursor =
        serde_json::from_slice(&serde_json::to_vec(&cursor).unwrap()).unwrap();
    assert_eq!(persisted, cursor, "JSON");
    let zero = SnapshotCursor {
        records_base: Some(RecordsBase { key: Stamp::new(Hlc::ZERO, node), epoch: None }),
        ..cursor.clone()
    };
    let wire: SnapshotCursor =
        bson::deserialize_from_slice(&bson::serialize_to_vec(&zero).unwrap()).unwrap();
    assert_eq!(wire, zero, "the zero base, through BSON");
    let none = SnapshotCursor { records_base: None, ..cursor.clone() };
    let text = serde_json::to_string(&none).unwrap();
    assert!(!text.contains("records_base"), "{text}");
    let mut progress = SnapshotProgress::of_collection(cy.id);
    let x = open(&dir, "x");
    x.apply_snapshot_page(y.node_id(), &mut progress, &first).unwrap();
    let (_, persisted) = x.snapshots_to_resume().unwrap().pop().expect("persisted");
    assert_eq!(persisted.after(), first.next.as_ref(), "the base in SNAPSHOT_PROGRESS's JSON");

    // A value of another shape is none, in either.
    let mut json = serde_json::to_value(&cursor).unwrap();
    json["records_base"] = serde_json::json!([1, 2, 3]);
    let read: SnapshotCursor = serde_json::from_value(json).unwrap();
    assert_eq!(read.records_base, None);
    let mut wire = bson::serialize_to_document(&cursor).unwrap();
    wire.insert("records_base", doc! { "hlc": "no" });
    let read: SnapshotCursor = bson::deserialize_from_document(wire).unwrap();
    assert_eq!(read.records_base, None);
}

// -- Page stores ------------------------------------------------------------------------------

/// Every store that changes a record logs the change, a page store's included,
/// and one that changes nothing logs nothing and writes nothing: a page or a
/// frame carrying records already held costs no commit and leaves the log as it
/// was.
#[test]
fn a_store_that_changes_a_record_logs_it_and_one_that_changes_nothing_logs_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x, s) = (open(&dir, "y"), open(&dir, "x"), open(&dir, "s"));
    let cy = y.create_collection("shop", "orders").unwrap();
    let k = key(7);
    fresh(y.insert_keyed(&cy, doc! { "_id": 1i64, "n": 0i64 }, &k).unwrap());
    assert_eq!(log_rows(&y), 1, "a local commit logs");
    let page = y.snapshot_page(None, Some(cy.id)).unwrap();
    assert_eq!(page.records.len(), 1);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    x.apply_snapshot_page(y.node_id(), &mut progress, &page).unwrap();
    assert_eq!(log_rows(&x), 1, "a page store logs");
    let commits = x.commits();
    let mut again = SnapshotProgress::of_collection(cy.id);
    x.apply_snapshot_page(y.node_id(), &mut again, &page).unwrap();
    assert_eq!(x.commits(), commits, "a page that changes nothing commits nothing");
    assert_eq!(log_rows(&x), 1, "and logs nothing");
    assert_eq!(x.idempotency_records_received()[4], 1, "counted once, when it changed");

    pull(&s, &y);
    assert_eq!(log_rows(&s), 1, "a frame store logs");
    // The same entries and record again: superseded, and the record held.
    let window = y.entries_for_peer(Hlc::ZERO, 1_024, crate::WalkScope::Background).unwrap();
    let record = y.idempotency_record(cy.id, &k.scope).unwrap().unwrap();
    let carried = CarriedRecord {
        collection: cy.id,
        scope: k.scope,
        carrying: Some(record.first),
        record: record.encode(),
    };
    let commits = s.commits();
    s.apply_batch_carrying(&window.entries, &[], RecordsVia::Pull).unwrap();
    let bare = s.commits() - commits;
    let commits = s.commits();
    s.apply_batch_carrying(&window.entries, &[carried], RecordsVia::Pull).unwrap();
    assert_eq!(log_rows(&s), 1, "a frame of what is held logs nothing");
    assert_eq!(s.commits() - commits, bare, "and its records cost no commit of their own");
}

/// The log is node-local: a record a member stores is not shipped again by its
/// windows. Once a peer holds it, a later window between them carries no record
/// (no row without an entry is written to `IDEMPOTENCY_BY_STAMP`, which F3
/// would send on every window).
#[test]
fn a_record_crosses_each_link_once() {
    let dir = tempfile::tempdir().unwrap();
    let (y, s, m) = (open(&dir, "y"), open(&dir, "s"), open(&dir, "m"));
    let cy = y.create_collection("shop", "orders").unwrap();
    fresh(y.insert_keyed(&cy, doc! { "_id": 1i64, "n": 0i64 }, &key(7)).unwrap());
    pull(&s, &y);
    assert_eq!(pull_counting(&m, &s), 1, "the record crosses once");
    // Later traffic between them carries it no more.
    y.insert(&cy, doc! { "_id": 2i64 }).unwrap();
    pull(&s, &y);
    assert_eq!(pull_counting(&m, &s), 0, "and not again");
    let other = s.create_collection("shop", "other").unwrap();
    s.insert(&other, doc! { "_id": 1i64 }).unwrap();
    assert_eq!(pull_counting(&m, &s), 0, "nor with the member's own writes");
}

/// The page-store edge: a member serving a snapshot of a collection takes a
/// converge page of it, and the page changes a record the member had already
/// sent the snapshot's receiver, at stamps the member had witnessed. Here the
/// record becomes `done` by a record-only commit on its origin (G8's residual:
/// the last writing chunk's lookahead met a match the stamp rule leaves, a
/// document written between two chunks), after the member pulled the request's
/// last writing chunk. The page store logs the change, so the snapshot's next
/// page carries it, the receiver ends with `done`, and a retry there replays
/// instead of resuming.
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
    // A request of two writing chunks of one. Between them a third counter is
    // written, above the request's `first`, so the second chunk's lookahead
    // meets a match the stamp rule leaves and cannot call the end; a later
    // chunk finds it and commits the record alone, `done`, at the same `last`.
    // S pulls after the last writing chunk, before that commit.
    y.set_multi_chunk_docs(1);
    let k = key(7);
    let (puller, from, writer, coll) = (Arc::clone(&s), Arc::clone(&y), Arc::clone(&y), cy.clone());
    at(Race::BetweenChunks, move || {
        writer.insert(&coll, doc! { "_id": 3i64, "n": 0i64 }).unwrap();
        at(Race::BetweenChunks, move || pull(&puller, &from));
    });
    let out = fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, None, &k).unwrap());
    assert_eq!((out.commits, out.skipped), (2, 1), "premise: two writing chunks, one left");
    crate::sync::race_hooks::clear();
    let done = y.idempotency_record(cy.id, &k.scope).unwrap().unwrap();
    assert_eq!(done.state, RecordState::Done);
    let cs = s.get_collection("shop", "orders").unwrap();
    let held = s.idempotency_record(cs.id, &k.scope).unwrap().unwrap();
    assert_eq!(held.state, RecordState::Applying, "premise: S pulled it before `done`");
    assert_eq!(held.last, done.last, "premise: the record-only commit kept its `last`");

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
    assert_eq!(log_rows(&x), 0);
}

// -- A record stored below the sender's witnessed vector -----------------------------------

/// R2 (the G1 review's section 10): a record the sender stores during the pull
/// below its own witnessed vector, as a replay page or a frame requested before a
/// concurrent pull raised that vector does. Origin Q's later entry raised Y's
/// coverage of Q before the first page; Q's earlier entry, which carries a keyed
/// request's effect and record, is applied on Y after it, by the path a replay
/// page takes. The store is logged like any other, so the next page carries it.
#[test]
fn r2_a_record_stored_below_the_senders_witnessed_vector_still_rides_the_next_page() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let cy = y.create_collection("shop", "orders").unwrap();
    let last = SNAPSHOT_PAGE as i64 + 40;
    y.insert_many(&cy, (1..=last).map(|i| doc! { "_id": i, "v": i }).collect()).unwrap();
    let q = NodeId::from_bytes([0xC1; 16]);
    let now = crate::physical_now_ms();
    let entry = |at: Stamp, id: i64, body: Document| OplogEntry {
        stamp: at,
        kind: OpKind::Replace,
        collection: cy.id,
        doc_id: Some(DocId::Int64(id)),
        body: Some(bson::serialize_to_vec(&body).unwrap()),
    };
    let (early, late) =
        (Stamp::new(Hlc::new(now + 10, 0), q), Stamp::new(Hlc::new(now + 50, 0), q));
    y.apply_batch_carrying(
        &[entry(late, 1, doc! { "_id": 1i64, "v": -1i64 })],
        &[],
        RecordsVia::Pull,
    )
    .unwrap();
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let k = key(7);
    let record = IdempotencyRecord {
        fingerprint: k.fingerprint,
        first: early,
        last: early,
        origin: q,
        state: RecordState::Done,
        outcome: RecordOutcome::Modify(ModifyRecorded {
            matched: 1,
            modified: 1,
            skipped: 0,
            commits: 1,
            stamp: Some(early),
            cursor: None,
            resumed: false,
        }),
        contested: false,
    };
    let carried = CarriedRecord {
        collection: cy.id,
        scope: k.scope,
        carrying: Some(early),
        record: record.encode(),
    };
    y.apply_batch_carrying(
        &[entry(early, last, doc! { "_id": last, "n": 11i64 })],
        &[carried],
        RecordsVia::Replay,
    )
    .unwrap();
    assert!(y.idempotency_record(cy.id, &k.scope).unwrap().is_some(), "premise: Y stored it");
    assert!(y.witnessed_vector().unwrap().get(q) > early.hlc, "premise: below Y's vector");
    finish(&x, &y, &mut progress);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&x, &cx, last), 11, "premise: the page brought the effect");
    retry_must_replay(&x, &cx, &k, last);
}

/// A record a member pulls while it serves a snapshot rides the snapshot's next
/// page: the frame store is logged like a local commit, whatever stamps its rows
/// are at.
#[test]
fn a_record_a_member_pulls_while_serving_a_snapshot_rides_its_next_page() {
    let dir = tempfile::tempdir().unwrap();
    let (y, s, m) = (open(&dir, "y"), open(&dir, "s"), open(&dir, "m"));
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    pull(&s, &y);
    let cs = s.get_collection("shop", "orders").unwrap();
    let mut progress = SnapshotProgress::of_collection(cs.id);
    take(&m, &s, &mut progress);
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    pull(&s, &y);
    let pages = finish(&m, &s, &mut progress);
    assert_eq!(pages[0].records.len(), 1, "the next page's delta");
    let cm = m.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&m, &cm, target), 11, "premise: the pull brought the effect");
    retry_must_replay(&m, &cm, &k, target);
}

// -- A base of another epoch --------------------------------------------------------------

/// Whether `page` is its collection again from its records phase: all `records`
/// of it, then its documents from the first, `first`.
fn restarted(page: &SnapshotPage, records: usize, first: i64) -> bool {
    page.records.len() == records
        && page.documents.first().map(|d| &d.id) == Some(&DocId::Int64(first))
}

/// A cursor's base with another epoch.
fn of_epoch(cursor: &SnapshotCursor, epoch: Option<i64>) -> SnapshotCursor {
    let base = cursor.records_base.expect("a base");
    SnapshotCursor { records_base: Some(RecordsBase { epoch, ..base }), ..cursor.clone() }
}

/// A base of another epoch than the sender's log has (a cursor cut from a log
/// this node no longer has): what changed since it is unknown, so the cursor's
/// collection restarts from its records phase, which carries the record of a
/// keyed write made since the page before. A key without an epoch, while the log
/// has one, the same.
#[test]
fn a_base_of_another_epoch_restarts_its_collection_from_its_records() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    fresh(y.insert_keyed(&cy, doc! { "_id": 0i64 }, &key(3)).unwrap());
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let asked = progress.after().cloned().unwrap();
    let epoch = asked.records_base.unwrap().epoch.expect("premise: the log has begun");
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let page = y.snapshot_page(Some(of_epoch(&asked, None)), Some(cy.id)).unwrap();
    assert!(restarted(&page, 2, 0), "a key without an epoch restarts: {:?}", page.next);
    let page = y.snapshot_page(Some(of_epoch(&asked, Some(epoch ^ 1))), Some(cy.id)).unwrap();
    assert!(restarted(&page, 2, 0), "restarted: {:?}", page.next);
    x.apply_snapshot_page(y.node_id(), &mut progress, &page).unwrap();
    finish(&x, &y, &mut progress);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&x, &cx, target), 11, "premise: the pull brought the effect");
    retry_must_replay(&x, &cx, &k, target);
}

/// The zero base, which a log that had held nothing cuts, is answered by the
/// sender with every change the log holds since, whatever its epoch: no restart
/// on the sender. The receiver refuses that page all the same, since the page
/// names an epoch the base was not cut under (guard ii of the review's m4), and
/// the restart it asks for brings the record: a retry replays.
#[test]
fn the_zero_base_is_sent_every_change_the_log_holds() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let asked = progress.after().cloned().unwrap();
    assert_eq!(asked.records_base.unwrap().epoch, None, "premise: the zero base");
    assert_eq!(asked.records_base.unwrap().key.hlc, Hlc::ZERO);
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let page = y.snapshot_page(Some(asked), Some(cy.id)).unwrap();
    assert_eq!(page.records.len(), 1, "the change, as a delta");
    assert_eq!(page.documents[0].id, DocId::Int64(SNAPSHOT_PAGE as i64 + 1), "no restart");
    assert!(page.records_epoch.is_some(), "the page names the epoch begun since");
    let applied = x.apply_snapshot_page(y.node_id(), &mut progress, &page).unwrap();
    assert!(applied.records_restarted, "the receiver refuses it");
    assert_eq!(finish_noting(&x, &y, &mut progress), 0, "and the restart is answered");
    let cx = x.get_collection("shop", "orders").unwrap();
    retry_must_replay(&x, &cx, &k, target);
}

/// A sender restored from a backup (which carries no log) begins its log again
/// with a new epoch, and a base cut before the restore restarts the cursor's
/// collection, before the restored log holds anything and after. And a sender
/// whose identity was reset begins it again the same way.
#[test]
fn a_restore_or_an_identity_reset_begins_the_log_again_and_restarts_older_bases() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    fresh(y.insert_keyed(&cy, doc! { "_id": 0i64 }, &key(3)).unwrap());
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let asked = progress.after().cloned().unwrap();
    let page = y.snapshot_page(Some(asked.clone()), Some(cy.id)).unwrap();
    assert!(!restarted(&page, 1, 0), "premise: its own base goes on: {:?}", page.next);
    // Y is replaced by a restore of its backup.
    let mut backup = Vec::new();
    y.backup_to(&mut backup, crate::WalkScope::Background).unwrap();
    drop(y);
    let restored_path = dir.path().join("y2.redb");
    crate::backup::restore(&restored_path, &mut backup.as_slice()).unwrap();
    let y = Engine::open(&restored_path).unwrap();
    assert_eq!(log_rows(&y), 0, "a backup carries no log");
    let page = y.snapshot_page(Some(asked.clone()), Some(cy.id)).unwrap();
    assert!(restarted(&page, 1, 0), "restarted on a restored log with nothing in it");
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &key(7)).unwrap());
    let page = y.snapshot_page(Some(asked.clone()), Some(cy.id)).unwrap();
    assert!(restarted(&page, 2, 0), "restarted after a restore: {:?}", page.next);
    // Its identity is reset: the log begins again, at the open, over the
    // records it holds.
    let page = y.snapshot_page(Some(asked), Some(cy.id)).unwrap();
    let before = page.next.unwrap();
    y.close().unwrap();
    {
        let db = redb::Database::open(&restored_path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(crate::tables::META).unwrap().remove(crate::tables::META_NODE_ID).unwrap();
        txn.commit().unwrap();
    }
    let y = Engine::open(&restored_path).unwrap();
    let page = y.snapshot_page(Some(before.clone()), Some(cy.id)).unwrap();
    assert!(restarted(&page, 2, 0), "restarted on a log another identity began");
    fresh(y.insert_keyed(&cy, doc! { "_id": 1_000_000i64 }, &key(9)).unwrap());
    let page = y.snapshot_page(Some(before.clone()), Some(cy.id)).unwrap();
    assert!(restarted(&page, 3, 0), "restarted after an identity reset: {:?}", page.next);
    let epoch = |cursor: &SnapshotCursor| cursor.records_base.and_then(|base| base.epoch);
    let renewed = epoch(page.next.as_ref().unwrap());
    assert!(renewed.is_some() && renewed != epoch(&before), "the log began again: {renewed:?}");
}

/// The clock-behind restore (the G1 review, 10.7's follow-up): the sender is
/// restored from a backup older than the receiver's base, onto a host whose
/// clock is behind it, so the keyed change it then takes is logged **below** that
/// base; later, its clock past the base, it logs another above it. Read by key
/// values alone the base looks current (between where the log begins and its
/// high water), and the change below it would be missed: the receiver would take
/// its effect without its record. The epoch tells the base is of another log, and
/// the cursor's collection restarts from its records.
#[test]
fn a_base_from_before_a_restore_onto_a_clock_behind_restarts_its_collection() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);
    fresh(y.insert_keyed(&cy, doc! { "_id": 0i64 }, &key(3)).unwrap());
    let mut backup = Vec::new();
    y.backup_to(&mut backup, crate::WalkScope::Background).unwrap();
    // After the backup, Y's log goes on, and the receiver's base with it.
    std::thread::sleep(std::time::Duration::from_millis(30));
    fresh(y.insert_keyed(&cy, doc! { "_id": 1_000_000i64 }, &key(4)).unwrap());
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let base = progress.after().unwrap().records_base.unwrap();
    drop(y);
    // Restored onto a host whose clock is an hour behind.
    let restored_path = dir.path().join("y2.redb");
    crate::backup::restore(&restored_path, &mut backup.as_slice()).unwrap();
    let y = Engine::open_with_clock_behind_for_test(&restored_path, 3_600_000).unwrap();
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let below = y.idempotency_record(cy.id, &k.scope).unwrap().unwrap().first;
    assert!(below.hlc < base.key.hlc, "premise: logged below the base");
    // The clock catches up, and the log goes on above the base.
    y.set_clock_behind_for_test(0);
    fresh(y.insert_keyed(&cy, doc! { "_id": 1_000_001i64 }, &key(5)).unwrap());
    {
        use redb::ReadableDatabase;
        let txn = y.db().begin_read().unwrap();
        let begun = crate::idempotency::changes_begin(&txn).unwrap().unwrap();
        let high = crate::idempotency::changes_high_water(&txn).unwrap().unwrap();
        assert!(begun.at.hlc < base.key.hlc && base.key.hlc < high.hlc, "premise: inside");
        assert_ne!(Some(begun.epoch), base.epoch, "a new epoch");
    }
    let page = take(&x, &y, &mut progress);
    // The restored records: the one from the backup and the two since.
    assert!(restarted(&page, 3, 0), "restarted: {:?}", page.next);
    finish(&x, &y, &mut progress);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&x, &cx, target), 11, "premise: the pull brought the effect");
    retry_must_replay(&x, &cx, &k, target);
}

// -- Two restores during one pull (the B5 build review's m4) -------------------------------

/// Where `engine`'s change log begins, and its epoch, as it stands.
fn log_begin(engine: &Engine) -> Option<crate::idempotency::LogBegin> {
    use redb::ReadableDatabase;
    let txn = engine.db().begin_read().unwrap();
    crate::idempotency::changes_begin(&txn).unwrap()
}

/// A store restored from `backup`, opened, at a path of its own.
fn restored(dir: &tempfile::TempDir, name: &str, backup: &[u8]) -> Engine {
    let path = dir.path().join(format!("{name}.redb"));
    crate::backup::restore(&path, &mut &backup[..]).unwrap();
    Engine::open(&path).unwrap()
}

/// `y`'s backup, as `kimmyd backup` writes it.
fn backup_of(y: &Engine) -> Vec<u8> {
    let mut out = Vec::new();
    y.backup_to(&mut out, crate::WalkScope::Background).unwrap();
    out
}

/// An open over records and no log of this node begins the log, with a new
/// epoch, before any page is cut: after a restore (a backup carries no log) and
/// after an identity reset. A store with no records begins none at open, and a
/// store whose own log has begun keeps it across a restart.
#[test]
fn an_open_over_records_and_no_log_begins_the_log_with_a_new_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let y_path = dir.path().join("y.redb");
    let y = Engine::open(&y_path).unwrap();
    let cy = y.create_collection("shop", "orders").unwrap();
    y.insert(&cy, doc! { "_id": 1i64 }).unwrap();
    let bare = backup_of(&y);
    assert!(log_begin(&y).is_none(), "premise: no keyed write, no log");
    fresh(y.insert_keyed(&cy, doc! { "_id": 2i64 }, &key(3)).unwrap());
    let own = log_begin(&y).expect("premise: begun by its first change");
    let keyed = backup_of(&y);
    y.close().unwrap();

    // A restart keeps the log it has.
    let y = Engine::open(&y_path).unwrap();
    assert_eq!(log_begin(&y), Some(own), "a restart keeps its log");
    // A restore of a store with no records begins none.
    let r0 = restored(&dir, "r0", &bare);
    assert!(log_begin(&r0).is_none(), "no records, no log");
    // A restore of one with records begins it at open, of this node, epoch new.
    let r1 = restored(&dir, "r1", &keyed);
    let begun = log_begin(&r1).expect("begun at open over the restored records");
    assert_eq!(begun.at.node, r1.node_id());
    assert_ne!(begun.epoch, own.epoch, "a new epoch");
    assert_eq!(log_rows(&r1), 0, "a beginning, and no change logged");
    // An identity reset: the log another identity began is not this node's.
    let r1_path = dir.path().join("r1.redb");
    r1.close().unwrap();
    {
        let db = redb::Database::open(&r1_path).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(crate::tables::META).unwrap().remove(crate::tables::META_NODE_ID).unwrap();
        txn.commit().unwrap();
    }
    let reset = Engine::open(&r1_path).unwrap();
    let again = log_begin(&reset).expect("begun again at open");
    assert_eq!(again.at.node, reset.node_id());
    assert_ne!(again.epoch, begun.epoch, "a new epoch after an identity reset");
}

/// The review's two-restore schedule (m4, `d1`): a sender restored from a
/// backup that holds records serves the first page; it is restored again, from
/// a backup that also holds a keyed `$inc` on a document of a later page, and
/// the pull goes on. The first restore's open began the log with an epoch, so
/// the first page's base names it; the second's began another, so the next page
/// restarts the collection from its records phase, which carries the `$inc`'s
/// record, and a retry on the receiver replays.
#[test]
fn a_pull_across_two_restores_of_a_sender_with_records_takes_every_record() {
    let dir = tempfile::tempdir().unwrap();
    let y0 = open(&dir, "y0");
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y0, target);
    // b1: a record (key 3), the target untouched.
    fresh(y0.insert_keyed(&cy, doc! { "_id": 0i64 }, &key(3)).unwrap());
    let b1 = backup_of(&y0);
    // b2: and the keyed $inc on the target.
    let k = key(7);
    fresh(y0.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let b2 = backup_of(&y0);
    drop(y0);

    let y1 = restored(&dir, "y1", &b1);
    let x = open(&dir, "x");
    let mut progress = SnapshotProgress::of_collection(cy.id);
    let first = take(&x, &y1, &mut progress);
    assert_eq!(first.records.len(), 1, "premise: b1's record in the records phase");
    let base = progress.after().unwrap().records_base.unwrap();
    assert!(base.epoch.is_some(), "the first restore's log names the base: {base:?}");
    drop(y1);

    // The sender is restored again, from b2.
    let y2 = restored(&dir, "y2", &b2);
    let pages = finish(&x, &y2, &mut progress);
    assert!(restarted(&pages[0], 2, 0), "restarted from its records: {:?}", pages[0].next);
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(n_of(&x, &cx, target), 11, "premise: the page brought b2's effect");
    retry_must_replay(&x, &cx, &k, target);
}

/// The next page of `progress` from `from`, applied on `to`, and whether `to`
/// refused it for its zero base (`SnapshotApplied::records_restarted`).
fn take_noting(
    to: &Engine,
    from: &Engine,
    progress: &mut SnapshotProgress,
) -> (SnapshotPage, bool) {
    let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
    let applied = to.apply_snapshot_page(from.node_id(), progress, &page).unwrap();
    (page, applied.records_restarted)
}

/// The pull to its end, and how many of its pages `to` refused for their zero
/// base.
fn finish_noting(to: &Engine, from: &Engine, progress: &mut SnapshotProgress) -> usize {
    let mut refused = 0;
    for _ in 0..64 {
        if progress.is_complete() {
            return refused;
        }
        refused += usize::from(take_noting(to, from, progress).1);
    }
    panic!("the pull did not end in 64 pages ({refused} refused)");
}

/// Guard (ii) on a schedule of two restores during one pull: the sender holds no
/// records when the first page is cut, so the page carries the zero base; it is
/// restored, from a backup that holds a keyed `$inc` on a later page's document,
/// whose open begins its log over the restored records with an epoch and logs
/// none of them. The sender answers the zero base with an empty delta and the
/// `$inc`'s effect; the receiver refuses that page, since the page names an
/// epoch the zero base was not cut under, and asks for the collection from its
/// records phase, which carries the record.
#[test]
fn a_zero_base_answered_after_a_restore_with_records_is_refused_and_restarts_the_records() {
    let dir = tempfile::tempdir().unwrap();
    let y0 = open(&dir, "y0");
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y0, target);
    let b0 = backup_of(&y0);
    let k = key(7);
    fresh(y0.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    let b2 = backup_of(&y0);
    drop(y0);

    let y1 = restored(&dir, "y1", &b0);
    let x = open(&dir, "x");
    let mut progress = SnapshotProgress::of_collection(cy.id);
    let (first, _) = take_noting(&x, &y1, &mut progress);
    assert!(first.records.is_empty() && first.records_epoch.is_none(), "premise: no records");
    let base = progress.after().unwrap().records_base.unwrap();
    assert_eq!((base.key.hlc, base.epoch), (Hlc::ZERO, None), "premise: the zero base");
    drop(y1);

    // The sender is restored again, from b2, whose open begins its log.
    let y2 = restored(&dir, "y2", &b2);
    let epoch = log_begin(&y2).expect("premise: begun at open").epoch;
    let asked = progress.after().cloned().unwrap();
    let cx = x.get_collection("shop", "orders").unwrap();
    let held = count(&x, &cx);
    let (answer, refused) = take_noting(&x, &y2, &mut progress);
    assert!(answer.records.is_empty(), "premise: the sender answers the zero base, empty");
    assert!(answer.documents.iter().any(|d| d.id == DocId::Int64(target)), "with the effect");
    assert_eq!(answer.records_epoch, Some(epoch), "naming the epoch begun since");
    assert!(refused, "the receiver refuses it");
    assert_eq!(count(&x, &cx), held, "nothing of it applied");
    let restart = progress.after().cloned().unwrap();
    assert_eq!(
        restart,
        SnapshotCursor {
            collection: asked.collection,
            after_key: Vec::new(),
            records: Some(Vec::new()),
            records_base: None,
        },
        "and asks for the collection from its records phase, with no base"
    );
    assert_eq!(finish_noting(&x, &y2, &mut progress), 0, "no page refused again");
    assert_eq!(n_of(&x, &cx, target), 11, "premise: the pull brought b2's effect");
    retry_must_replay(&x, &cx, &k, target);
}

/// The cost of guard (ii) on an ordinary pull: the sender holds no records when
/// the pull begins, and its first keyed write lands mid-pull. Its log begins
/// with that write and names it, so the page answering the zero base would have
/// been right, but the receiver cannot tell it from a restore and refuses it:
/// **exactly one** records-phase restart, and the pull completes, on a scoped
/// pull and on a converge pull. Keyed writes after the log has begun cost none.
#[test]
fn a_first_keyed_write_mid_pull_costs_exactly_one_records_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let target = SNAPSHOT_PAGE as i64 + 40;
    let cy = three_pages(&y, target);

    // A scoped pull.
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    assert_eq!(progress.after().unwrap().records_base.unwrap().epoch, None, "premise: zero");
    let k = key(7);
    fresh(y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    // The sender's own answer is right: the change, as a delta, no restart.
    let answer = y.snapshot_page(progress.after().cloned(), Some(cy.id)).unwrap();
    assert_eq!(answer.records.len(), 1, "the change, as a delta");
    assert_eq!(answer.documents[0].id, DocId::Int64(SNAPSHOT_PAGE as i64 + 1), "no restart");
    let (_, refused) = take_noting(&x, &y, &mut progress);
    assert!(refused, "refused all the same");
    // A keyed write on a later page now, under the log's epoch: no restart.
    let late = 2 * SNAPSHOT_PAGE as i64 + 7;
    let on = Candidates::Keys(vec![crate::docs::doc_key(&DocId::Int64(late)).unwrap()]);
    y.replace(&cy, &DocId::Int64(late), doc! { "_id": late, "n": 0i64 }, false).unwrap();
    let k2 = key(8);
    let refused = {
        let mut refused = 0;
        // One page into the restart, then the second keyed write, then the rest.
        refused += usize::from(take_noting(&x, &y, &mut progress).1);
        fresh(y.modify_where_keyed(&cy, &on, &Inc, Some(1), &k2).unwrap());
        refused + finish_noting(&x, &y, &mut progress)
    };
    assert_eq!(refused, 0, "after the one restart, no page is refused");
    let cx = x.get_collection("shop", "orders").unwrap();
    assert_eq!(count(&x, &cx), count(&y, &cy), "the pull completed");
    retry_must_replay(&x, &cx, &k, target);
    retry_must_replay(&x, &cx, &k2, late);

    // A converge pull, from a second sender whose log has not begun, by a
    // member that holds the collection, as a converge pull's receiver does.
    let (w, c) = (open(&dir, "w"), open(&dir, "c"));
    let cw = three_pages(&w, target);
    pull(&c, &w);
    let cc = c.get_collection("shop", "orders").unwrap();
    let mut converge = SnapshotProgress::of_collection_converging(cc.id, Default::default());
    let (first, refused) = take_noting(&c, &w, &mut converge);
    assert!(!refused && first.records_epoch.is_none(), "premise: the zero base");
    let k3 = key(9);
    fresh(w.modify_where_keyed(&cw, &Candidates::Scan, &Inc, Some(1), &k3).unwrap());
    let (_, refused) = take_noting(&c, &w, &mut converge);
    assert!(refused, "a converge page is refused the same way");
    assert_eq!(finish_noting(&c, &w, &mut converge), 0, "once, then the pull completes");
    assert_eq!(n_of(&c, &cc, target), 11, "premise: the converge pull brought the effect");
    retry_must_replay(&c, &cc, &k3, target);
}

// -- The records phase within the page's records budget ------------------------------------

/// A collection `name` on `y` of `n` counters with a `pad`-byte string each, and
/// a keyed `find_and_modify` on each answering with its document after: `n`
/// records of about `pad` bytes each.
fn large_records(y: &Engine, name: &str, n: i64, pad: usize) -> (CollectionMeta, Vec<KeyedWrite>) {
    let coll = y.create_collection("shop", name).unwrap();
    let text = "x".repeat(pad);
    y.insert_many(
        &coll,
        (0..n).map(|i| doc! { "_id": i, "n": 0i64, "pad": text.clone() }).collect(),
    )
    .unwrap();
    let keys: Vec<KeyedWrite> = (0..n as u64).map(|i| key_of(1_000 + i)).collect();
    for (i, k) in keys.iter().enumerate() {
        let on = Candidates::Keys(vec![crate::docs::doc_key(&DocId::Int64(i as i64)).unwrap()]);
        let out = y
            .find_and_modify_keyed(&coll, &on, &Inc, k, crate::idempotency::ReturnedImage::After)
            .unwrap();
        fresh(out);
    }
    (coll, keys)
}

/// What a page's records cost its rows' room.
fn records_bytes(page: &SnapshotPage) -> usize {
    page.records.iter().map(|r| crate::idempotency::carried_size(r.record.len())).sum()
}

/// What a page's rows cost its room: its records, documents and tombstones.
fn rows_bytes(page: &SnapshotPage) -> usize {
    let documents: usize = page
        .documents
        .iter()
        .map(|d| {
            d.body.as_ref().map_or(0, Vec::len)
                + crate::docs::doc_key(&d.id).unwrap().len()
                + crate::snapshot::DOCUMENT_OVERHEAD
        })
        .sum();
    let tombstones: usize = page
        .deleted_documents
        .iter()
        .map(|t| t.key.len() + crate::snapshot::TOMBSTONE_OVERHEAD)
        .sum();
    records_bytes(page) + documents + tombstones
}

/// One record's cost, of a record of `coll` on `y`.
fn one_record(y: &Engine, coll: &CollectionMeta, k: &KeyedWrite) -> usize {
    crate::idempotency::carried_size(
        y.idempotency_record(coll.id, &k.scope).unwrap().unwrap().encode().len(),
    )
}

/// A records phase larger than the page's records budget spans pages, each
/// within it, the cursor's records position moving on; a receiver restarted in
/// the middle resumes from it; every record arrives and a retry replays. The
/// cursor in the middle of the phase crosses BSON and JSON as before, and a 0.51
/// receiver's echo, without the position, goes to the documents.
#[test]
fn a_records_phase_larger_than_the_records_budget_spans_pages() {
    let dir = tempfile::tempdir().unwrap();
    let y = open(&dir, "y");
    let (big, keys) = large_records(&y, "big", 12, 10_000);
    let budget = 3 * one_record(&y, &big, &keys[0]) + 50;
    y.set_record_delta_budget_for_test(Some((budget, crate::idempotency::RECORD_DELTA_ROWS)));
    let x_path = dir.path().join("x.redb");
    let mut progress = SnapshotProgress::whole_database();
    let mut positions = Vec::new();
    {
        let x = Engine::open(&x_path).unwrap();
        for _ in 0..2 {
            let page = take(&x, &y, &mut progress);
            assert_eq!(page.records.len(), 3, "three records' worth");
            assert!(rows_bytes(&page) <= budget, "within the budget");
            assert!(page.documents.is_empty(), "records first");
            positions.push(page.next.clone().unwrap().records.expect("a records position"));
        }
    }
    assert!(positions[0] < positions[1], "the position moves on");
    let x = Engine::open(&x_path).unwrap();
    let (_, mut progress) = x.snapshots_to_resume().unwrap().pop().expect("persisted");
    let at = progress.after().cloned().unwrap();
    assert_eq!(at.records.as_ref(), Some(&positions[1]), "resumed from the records position");
    let wire: SnapshotCursor =
        bson::deserialize_from_slice(&bson::serialize_to_vec(&at).unwrap()).unwrap();
    assert_eq!(wire, at, "BSON");
    let json: SnapshotCursor = serde_json::from_slice(&serde_json::to_vec(&at).unwrap()).unwrap();
    assert_eq!(json, at, "JSON");
    let older = SnapshotCursor { records: None, records_base: None, ..at.clone() };
    let skipped = y.snapshot_page(Some(older), None).unwrap();
    assert!(skipped.records.is_empty(), "a 0.51 echo: no records");
    assert_eq!(skipped.documents.first().map(|d| d.id.clone()), Some(DocId::Int64(0)));
    for page in finish(&x, &y, &mut progress) {
        assert!(rows_bytes(&page) <= budget, "within the budget");
    }
    let cx = x.get_collection("shop", "big").unwrap();
    assert_eq!(x.idempotency_records().unwrap(), 12, "every record");
    let on = Candidates::Keys(vec![crate::docs::doc_key(&DocId::Int64(11)).unwrap()]);
    let again = x.find_and_modify_keyed(
        &cx,
        &on,
        &Inc,
        &keys[11],
        crate::idempotency::ReturnedImage::After,
    );
    assert!(matches!(again.unwrap(), Keyed::Replayed(_)), "a retry replays");
}

/// The same on a scoped repair's pages and on a converge pull's.
#[test]
fn a_scoped_and_a_converge_records_phase_span_pages_within_the_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x, c) = (open(&dir, "y"), open(&dir, "x"), open(&dir, "c"));
    let (big, keys) = large_records(&y, "big", 10, 10_000);
    let budget = 4 * one_record(&y, &big, &keys[0]) + 50;
    y.set_record_delta_budget_for_test(Some((budget, crate::idempotency::RECORD_DELTA_ROWS)));
    let mut progress = SnapshotProgress::of_collection(big.id);
    let pages = finish(&x, &y, &mut progress);
    let counts: Vec<usize> =
        pages.iter().map(|page| page.records.len()).filter(|n| *n > 0).collect();
    assert_eq!(counts, vec![4, 4, 2], "a scoped repair");
    assert!(pages.iter().all(|page| rows_bytes(page) <= budget));
    assert!(pages.len() > 3, "and its documents, by bytes too");
    assert_eq!(x.idempotency_records().unwrap(), 10);

    // C holds the documents and has lost the records: a converge pull brings them.
    pull(&c, &y);
    let cc = c.get_collection("shop", "big").unwrap();
    {
        let db = c.db();
        let txn = db.begin_write().unwrap();
        {
            let mut records = txn.open_table(crate::tables::IDEMPOTENCY).unwrap();
            for k in &keys {
                records.remove((cc.id.0, k.scope.as_bytes().as_slice())).unwrap();
            }
        }
        txn.commit().unwrap();
    }
    let mut converge = SnapshotProgress::of_collection_converging(cc.id, Default::default());
    let pages = finish(&c, &y, &mut converge);
    let counts: Vec<usize> =
        pages.iter().map(|page| page.records.len()).filter(|n| *n > 0).collect();
    assert_eq!(counts, vec![4, 4, 2], "a converge pull");
    assert!(pages.iter().all(|page| rows_bytes(page) <= budget));
    assert_eq!(c.idempotency_records().unwrap(), 10);
}

/// The delta and the records phase share the page's records budget: a page whose
/// delta is large leaves its records phase what is left, and the phase goes on
/// on the next page.
#[test]
fn the_delta_and_the_records_phase_share_the_records_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    // Two collections with records, walked in id order; the first page ends at
    // the boundary into the second.
    let (first_name, second_name) = if kimmy_core::CollectionId::derive("shop", "a")
        < kimmy_core::CollectionId::derive("shop", "b")
    {
        ("a", "b")
    } else {
        ("b", "a")
    };
    let (a, a_keys) = large_records(&y, first_name, 2, 10_000);
    let (b, _) = large_records(&y, second_name, 6, 10_000);
    assert!(a.id < b.id, "premise: a is walked first");
    let one = one_record(&y, &a, &a_keys[0]);
    let budget = 5 * one + 50;
    y.set_record_delta_budget_for_test(Some((budget, crate::idempotency::RECORD_DELTA_ROWS)));
    let mut progress = SnapshotProgress::whole_database();
    let first = take(&x, &y, &mut progress);
    assert_eq!(first.records.len(), 2, "a's records, then its documents");
    // Keyed changes since: a delta of three large records.
    for (i, k) in [(0i64, 7u64), (1, 8)].into_iter().chain([(0, 9)]) {
        let on = Candidates::Keys(vec![crate::docs::doc_key(&DocId::Int64(i)).unwrap()]);
        fresh(
            y.find_and_modify_keyed(
                &a,
                &on,
                &Inc,
                &key_of(k),
                crate::idempotency::ReturnedImage::After,
            )
            .unwrap(),
        );
    }
    let second = take(&x, &y, &mut progress);
    assert!(rows_bytes(&second) <= budget, "the delta and the phase within one budget");
    let of_b = second.records.iter().filter(|r| r.collection == b.id).count();
    assert_eq!((second.records.len() - of_b, of_b), (3, 2), "three of the delta, two of b's phase");
    let rest = finish(&x, &y, &mut progress);
    assert!(rest.iter().all(|page| rows_bytes(page) <= budget));
    assert_eq!(x.idempotency_records().unwrap(), 2 + 6 + 3);
}

/// A row that does not fit what a page has left goes first on the next page: a
/// page whose delta is large leaves the records phase's next record to the next.
/// A row that does not fit a page holding nothing else cannot cross on a page:
/// it is refused, naming it, a record or a document alike; the first page, which
/// carries the definitions, leaves it to the second instead.
#[test]
fn a_row_goes_first_on_the_next_page_and_one_no_page_can_carry_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let (big, keys) = large_records(&y, "big", 3, 10_000);
    let one = one_record(&y, &big, &keys[0]);
    y.set_record_delta_budget_for_test(Some((
        one + one / 2,
        crate::idempotency::RECORD_DELTA_ROWS,
    )));
    let mut progress = SnapshotProgress::of_collection(big.id);
    let first = take(&x, &y, &mut progress);
    assert_eq!(first.records.len(), 1, "one record's worth");
    // A large keyed change: the next page's delta.
    let on = Candidates::Keys(vec![crate::docs::doc_key(&DocId::Int64(2)).unwrap()]);
    fresh(
        y.find_and_modify_keyed(
            &big,
            &on,
            &Inc,
            &key(42),
            crate::idempotency::ReturnedImage::After,
        )
        .unwrap(),
    );
    let second = take(&x, &y, &mut progress);
    assert_eq!(second.records.len(), 1, "the delta's record only");
    assert_eq!(second.records[0].scope, key(42).scope);
    let third = take(&x, &y, &mut progress);
    assert_eq!(third.records.len(), 1, "then the phase's next, first on its page");
    assert_ne!(third.records[0].scope, first.records[0].scope, "from where the phase stood");
    finish(&x, &y, &mut progress);
    assert_eq!(x.idempotency_records().unwrap(), 4);

    // Less room than one record: the first page leaves it, the second refuses it.
    y.set_record_delta_budget_for_test(Some((one / 2, crate::idempotency::RECORD_DELTA_ROWS)));
    let page = y.snapshot_page(None, Some(big.id)).unwrap();
    assert!(page.records.is_empty() && page.documents.is_empty(), "behind the definitions");
    let refused = y.snapshot_page(page.next.clone(), Some(big.id));
    match refused {
        Err(crate::StorageError::SnapshotRowTooLarge { what, bytes, room }) => {
            assert!(what.contains("idempotency record") && bytes > room, "{what}");
        }
        other => panic!("expected the record refused, got {other:?}"),
    }
    // A document no page can carry, likewise.
    let plain = y.create_collection("shop", "plain").unwrap();
    y.insert(&plain, doc! { "_id": 7i64, "pad": "y".repeat(one) }).unwrap();
    let page = y.snapshot_page(None, Some(plain.id)).unwrap();
    assert!(page.documents.is_empty(), "behind the definitions");
    match y.snapshot_page(page.next.clone(), Some(plain.id)) {
        Err(e @ crate::StorageError::SnapshotRowTooLarge { .. }) => {
            assert!(e.to_string().contains("document Int64(7)"), "{e}");
        }
        other => panic!("expected the document refused, got {other:?}"),
    }
}

/// At the real budget: a page of documents and records that would pass a frame
/// is cut by bytes, each page's encoding within a frame (`SNAPSHOT_PAGE_BYTES`
/// and its 1 MiB), the cursor going on from the last row sent: 600 documents
/// of 130 KiB, the shape that stalled a v0.51.0 pull for good, whole-database
/// and scoped; and records holding megabyte documents, a records phase over a
/// page.
#[test]
fn pages_of_large_rows_stay_within_a_frame_at_the_real_budget() {
    let frame = crate::SNAPSHOT_PAGE_BYTES + 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let (y, x, z) = (open(&dir, "y"), open(&dir, "x"), open(&dir, "z"));
    let wide = y.create_collection("shop", "wide").unwrap();
    let pad = "x".repeat(130 * 1024);
    for chunk in 0..6i64 {
        let docs =
            (0..100i64).map(|i| doc! { "_id": chunk * 100 + i, "pad": pad.clone() }).collect();
        y.insert_many(&wide, docs).unwrap();
    }
    for (receiver, scope) in [(&x, None), (&z, Some(wide.id))] {
        let mut progress = match scope {
            None => SnapshotProgress::whole_database(),
            Some(id) => SnapshotProgress::of_collection(id),
        };
        let pages = finish(receiver, &y, &mut progress);
        for page in &pages {
            let encoded = bson::serialize_to_vec(page).unwrap().len();
            assert!(encoded <= frame, "a page of {encoded} bytes, over {frame}");
        }
        assert!(pages[0].documents.len() < SNAPSHOT_PAGE, "cut by bytes, not by count");
        let cw = receiver.get_collection("shop", "wide").unwrap();
        assert_eq!(count(receiver, &cw), 600, "every document, once");
    }

    let (big, _) = large_records(&y, "big", 70, 1024 * 1024);
    let m = open(&dir, "m");
    let mut progress = SnapshotProgress::of_collection(big.id);
    let pages = finish(&m, &y, &mut progress);
    for page in &pages {
        let encoded = bson::serialize_to_vec(page).unwrap().len();
        assert!(encoded <= frame, "a page of {encoded} bytes, over {frame}");
    }
    assert!(pages[0].records.len() < 70 && pages[0].documents.is_empty(), "the phase spans pages");
    assert_eq!(m.idempotency_records().unwrap(), 70);
    let cm = m.get_collection("shop", "big").unwrap();
    assert_eq!(count(&m, &cm), 70);
}

// -- The build review's d3 and d4 (m5): pages within a frame at the real budget -------------

/// The frame a page must fit: `SNAPSHOT_PAGE_BYTES` and the 1 MiB it leaves.
const FRAME: usize = crate::SNAPSHOT_PAGE_BYTES + 1024 * 1024;

/// Removes `pad`: the before image is large, the after image small.
struct Shrink;

impl ModifySpec for Shrink {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        Ok(true)
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.remove("pad");
        next.insert("t", Bson::Int64(1));
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

/// Sets `t` to a new value on every match.
struct Touch;

impl ModifySpec for Touch {
    fn matches(&self, _: &Document) -> kimmy_core::Result<bool> {
        Ok(true)
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, doc: &Document) -> std::result::Result<Option<Document>, String> {
        let mut next = doc.clone();
        next.insert("t", Bson::Int64(doc.get_i64("t").unwrap_or(0) + 1));
        Ok(Some(next))
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

/// D3 (the review's m5, "separately confirmed"): a records phase of 512
/// `find_and_modify` records, each holding a 140 KiB before image, was one page of
/// 73,629,363 bytes, over a frame. It spans pages now, each within a frame, and
/// every record arrives: a retry of any key replays.
#[test]
fn d3_a_records_phase_of_find_and_modify_records_stays_within_a_frame() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let cy = y.create_collection("shop", "big").unwrap();
    let pad = "x".repeat(140 * 1024);
    y.insert_many(&cy, (0..512i64).map(|i| doc! { "_id": i, "pad": pad.clone() }).collect())
        .unwrap();
    let image = crate::idempotency::ReturnedImage::Before;
    for i in 0..512u64 {
        let on = Candidates::Keys(vec![crate::docs::doc_key(&DocId::Int64(i as i64)).unwrap()]);
        fresh(y.find_and_modify_keyed(&cy, &on, &Shrink, &key_of(i), image).unwrap());
    }
    let mut progress = SnapshotProgress::of_collection(cy.id);
    let pages = finish(&x, &y, &mut progress);
    for page in &pages {
        let bytes = bson::serialize_to_vec(page).unwrap().len();
        assert!(bytes <= FRAME, "a page of {bytes} bytes, over a frame of {FRAME}");
    }
    assert!(pages.iter().filter(|page| !page.records.is_empty()).count() > 1, "spans pages");
    assert_eq!(x.idempotency_records().unwrap(), 512, "every record");
    let cx = x.get_collection("shop", "big").unwrap();
    let on = Candidates::Keys(vec![crate::docs::doc_key(&DocId::Int64(511)).unwrap()]);
    let again = x.find_and_modify_keyed(&cx, &on, &Shrink, &key_of(511), image).unwrap();
    assert!(matches!(again, Keyed::Replayed(_)), "a retry replays: {again:?}");
}

/// D4 (the review's m5): a page whose documents alone fit a frame (512 of 66 KiB,
/// 33 MiB) and a delta of 32 `find_and_modify` records of 1,000 KiB documents that
/// land between pages, 32 MiB, within the delta's old budget of its own, made a
/// page of 67,478,180 bytes, and the pull stalled on it. The delta, the records
/// phase and the documents share one budget now: that page stays within a frame,
/// and the pull completes with every record.
#[test]
fn d4_a_whole_delta_and_a_page_of_documents_stay_within_a_frame() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let cy = y.create_collection("shop", "mid").unwrap();
    let pad = "x".repeat(66 * 1024);
    let docs = (1..=(2 * SNAPSHOT_PAGE as i64 + 10)).map(|i| doc! { "_id": i, "pad": pad.clone() });
    y.insert_many(&cy, docs.collect()).unwrap();
    // Big documents past the second page, for the find_and_modify records.
    let big = "y".repeat(1000 * 1024);
    let first_big = 10_000i64;
    for i in 0..32i64 {
        y.insert(&cy, doc! { "_id": first_big + i, "pad": big.clone() }).unwrap();
    }
    // Its log has begun, so the base names an epoch and the next page's delta
    // answers it (a zero base would be refused at the first keyed write).
    begun(&y);
    let mut progress = SnapshotProgress::of_collection(cy.id);
    take(&x, &y, &mut progress);
    let image = crate::idempotency::ReturnedImage::After;
    for i in 0..32u64 {
        let id = DocId::Int64(first_big + i as i64);
        let on = Candidates::Keys(vec![crate::docs::doc_key(&id).unwrap()]);
        fresh(y.find_and_modify_keyed(&cy, &on, &Touch, &key_of(i), image).unwrap());
    }
    let second = y.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
    let bytes = bson::serialize_to_vec(&second).unwrap().len();
    assert!(bytes <= FRAME, "a page of {bytes} bytes, over a frame of {FRAME}");
    assert!(!second.records.is_empty(), "premise: the delta rides it");
    let applied = x.apply_snapshot_page(y.node_id(), &mut progress, &second).unwrap();
    assert!(!applied.records_restarted, "premise: answered by the log");
    for page in finish(&x, &y, &mut progress) {
        let bytes = bson::serialize_to_vec(&page).unwrap().len();
        assert!(bytes <= FRAME, "a page of {bytes} bytes, over a frame of {FRAME}");
    }
    assert_eq!(x.idempotency_records().unwrap(), 32, "every record");
    let cx = x.get_collection("shop", "mid").unwrap();
    assert_eq!(count(&x, &cx), count(&y, &cy), "every document, once");
    let on = Candidates::Keys(vec![crate::docs::doc_key(&DocId::Int64(first_big + 31)).unwrap()]);
    let again = x.find_and_modify_keyed(&cx, &on, &Touch, &key_of(31), image).unwrap();
    assert!(matches!(again, Keyed::Replayed(_)), "a retry replays: {again:?}");
}

/// What a page's other parts cost (its definitions, drops, vector and epoch):
/// the page encoded with no rows and no cursor, as `snapshot_page` measures it.
fn fixed_bytes(page: &SnapshotPage) -> usize {
    let bare = SnapshotPage {
        documents: Vec::new(),
        deleted_documents: Vec::new(),
        records: Vec::new(),
        next: None,
        tombstone_horizon: None,
        records_epoch: Some(0),
        ..page.clone()
    };
    bson::serialize_to_vec(&bare).unwrap().len()
}

/// A page's other parts are counted against its budget with its rows: a
/// whole-database snapshot of a sender holding sixty dropped collections, whose
/// drops ride every page, keeps each page's drops and rows within the budget
/// together, and every document arrives once.
#[test]
fn a_pages_definitions_and_drops_are_counted_against_its_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    for i in 0..60 {
        let name = format!("gone{i}");
        y.create_collection("shop", &name).unwrap();
        assert!(y.drop_collection("shop", &name).unwrap());
    }
    let cy = y.create_collection("shop", "kept").unwrap();
    let pad = "x".repeat(1024);
    y.insert_many(&cy, (0..200i64).map(|i| doc! { "_id": i, "pad": pad.clone() }).collect())
        .unwrap();
    let budget = 24 * 1024;
    y.set_snapshot_page_bytes_for_test(Some(budget));
    let mut progress = SnapshotProgress::whole_database();
    let pages = finish(&x, &y, &mut progress);
    y.set_snapshot_page_bytes_for_test(None);
    assert!(pages.len() > 2, "premise: paged by bytes");
    for page in &pages {
        let (fixed, rows) = (fixed_bytes(page), rows_bytes(page));
        assert!(fixed > 2 * 1024, "premise: the drops ride every page: {fixed}");
        assert!(fixed + rows <= budget, "{fixed} + {rows} over {budget}");
    }
    let cx = x.get_collection("shop", "kept").unwrap();
    assert_eq!(count(&x, &cx), 200, "every document, once");
}

// -- The records of a collection dropped and not created again -----------------------------

/// Two collection names whose ids put the first before the second in the walk.
fn walked_in_order(first: &'static str, second: &'static str) -> (&'static str, &'static str) {
    let id = |name| kimmy_core::CollectionId::derive("shop", name);
    if id(first) < id(second) { (first, second) } else { (second, first) }
}

/// A key outlives a drop of its collection for its window (B5): a snapshot of a
/// sender that dropped a collection and did not create it again still carries
/// its records, whole-database and scoped, and none of its documents.
#[test]
fn a_snapshot_after_a_drop_carries_the_dropped_collections_records() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let gone = y.create_collection("shop", "gone").unwrap();
    for i in 0..3u64 {
        fresh(y.insert_keyed(&gone, doc! { "_id": i as i64 }, &key_of(500 + i)).unwrap());
    }
    let kept = y.create_collection("shop", "kept").unwrap();
    y.insert(&kept, doc! { "_id": 1i64 }).unwrap();
    assert!(y.drop_collection("shop", "gone").unwrap());
    let mut progress = SnapshotProgress::whole_database();
    let pages = finish(&x, &y, &mut progress);
    let of_gone =
        |page: &SnapshotPage| page.records.iter().filter(|r| r.collection == gone.id).count();
    assert_eq!(pages.iter().map(of_gone).sum::<usize>(), 3, "whole-database: its records");
    assert!(
        pages.iter().all(|page| page.documents.iter().all(|d| d.collection != gone.id)),
        "and none of its documents"
    );
    let scoped = y.snapshot_page(None, Some(gone.id)).unwrap();
    assert!(scoped.dropped.is_some(), "premise: the scoped page carries the drop");
    assert_eq!(of_gone(&scoped), 3, "scoped: its records");
    assert!(scoped.documents.is_empty());
}

/// A page's delta carries a record of a collection that no longer stands: a
/// keyed write on a collection the walk has passed, and then its drop, between
/// two pages.
#[test]
fn a_pages_delta_carries_a_record_of_a_dropped_collection() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let (gone_name, big_name) = walked_in_order("gone", "big");
    let gone = y.create_collection("shop", gone_name).unwrap();
    y.insert(&gone, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    let big = y.create_collection("shop", big_name).unwrap();
    y.insert_many(&big, (0..(2 * SNAPSHOT_PAGE as i64)).map(|i| doc! { "_id": i }).collect())
        .unwrap();
    let mut progress = SnapshotProgress::whole_database();
    take(&x, &y, &mut progress);
    assert_eq!(progress.after().unwrap().collection, big.id, "premise: the walk is past it");
    let k = key(7);
    fresh(y.modify_where_keyed(&gone, &Candidates::Scan, &Inc, Some(1), &k).unwrap());
    assert!(y.drop_collection("shop", gone_name).unwrap());
    let page = take(&x, &y, &mut progress);
    assert!(
        page.records.iter().any(|r| r.collection == gone.id && r.scope == k.scope),
        "the delta carries it: {:?}",
        page.records
    );
}

/// End to end, with the receiver half (ruling A): a member rebuilt by
/// snapshot after a drop, the collection not created again, replays a retry of
/// the old key once the name is created again.
#[test]
fn a_member_rebuilt_by_snapshot_after_a_drop_replays_a_retry_of_the_old_key() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let gone = y.create_collection("shop", "gone").unwrap();
    let k = key(7);
    fresh(y.insert_keyed(&gone, doc! { "_id": 1i64 }, &k).unwrap());
    assert!(y.drop_collection("shop", "gone").unwrap());
    let mut progress = SnapshotProgress::whole_database();
    finish(&x, &y, &mut progress);
    assert!(x.get_collection("shop", "gone").is_err(), "premise: dropped there too");
    let again = x.create_collection("shop", "gone").unwrap();
    let retry = x.insert_keyed(&again, doc! { "_id": 1i64 }, &k).unwrap();
    assert!(matches!(retry, Keyed::Replayed(_)), "the old key replays: {retry:?}");
}
