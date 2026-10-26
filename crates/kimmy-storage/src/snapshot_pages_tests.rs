//! Snapshot, scoped and converge pages bounded by bytes (ADR-249): a row that does
//! not fit what a page has left goes first on the next; one no page can carry is
//! refused by name; every page's encoding stays within a frame; a converge pull
//! counts its rows, tombstones with documents.

use std::cmp::Ordering;

use bson::{Bson, Document, doc};
use kimmy_core::{DocId, Hlc, NodeId, Stamp};

use crate::{
    CollectionMeta, Engine, ModifySpec, SNAPSHOT_PAGE, SnapshotCursor, SnapshotPage,
    SnapshotProgress,
};

/// Removes every match that has `gone`.
struct Del;

impl ModifySpec for Del {
    fn matches(&self, doc: &Document) -> kimmy_core::Result<bool> {
        Ok(doc.contains_key("gone"))
    }
    fn compare(&self, _: &Document, _: &Document) -> Ordering {
        Ordering::Equal
    }
    fn apply(&self, _: &Document) -> std::result::Result<Option<Document>, String> {
        Ok(None)
    }
    fn upsert(&self) -> Option<std::result::Result<Document, String>> {
        None
    }
}

/// Adds one to `n` on every match that has one.
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

/// An engine that takes any row: the tests store ids and documents a peer could carry
/// but no client could write under the caps of ADR-250.
fn open(dir: &tempfile::TempDir, name: &str) -> Engine {
    let engine = Engine::open(&dir.path().join(format!("{name}.redb"))).unwrap();
    engine.lift_admission_caps_for_test();
    engine
}

fn count(engine: &Engine, coll: &CollectionMeta) -> u64 {
    engine.count(coll, crate::walk::WalkScope::Request).unwrap()
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

/// What a page's rows cost its room: its documents and tombstones.
fn rows_bytes(page: &SnapshotPage) -> usize {
    let documents: usize = page
        .documents
        .iter()
        .map(|d| {
            d.body.as_ref().map_or(0, Vec::len)
                + crate::snapshot::id_wire_len(&d.id)
                + crate::snapshot::DOCUMENT_OVERHEAD
        })
        .sum();
    let tombstones: usize = page
        .deleted_documents
        .iter()
        .map(|t| t.key.len() + crate::snapshot::TOMBSTONE_OVERHEAD)
        .sum();
    documents + tombstones
}

// -- A scoped snapshot ends where the collection ended when it began --------------------

/// **A scoped snapshot stops at the last key the collection held when its first page was
/// served**, however much is written after it. The rows after it reach the receiver by
/// entries. Three pages' worth at the start, two pages' worth added after every page: the
/// pull completes after three pages and carries only what stood at the start. A collection
/// that stood at exactly a page's worth ends on that page, with no empty one after it.
#[test]
fn a_scoped_snapshot_stops_at_the_collection_as_it_stood_when_it_began() {
    let dir = tempfile::tempdir().unwrap();
    let (from, to) = (open(&dir, "from"), open(&dir, "to"));
    let page = crate::snapshot::SNAPSHOT_PAGE as i64;
    for (name, standing, expected_pages) in [("grows", 3 * page, 3usize), ("exact", page, 1)] {
        let coll = from.create_collection("shop", name).unwrap();
        let rows = |first: i64, n: i64| -> Vec<bson::Document> {
            (first..first + n).map(|i| bson::doc! { "_id": i }).collect()
        };
        from.insert_many(&coll, rows(0, standing)).unwrap();
        let mut progress = SnapshotProgress::of_collection(coll.id);
        let mut next = standing;
        let mut pages = 0usize;
        while !progress.is_complete() {
            assert!(pages < 20, "{name}: it chased the tail for {pages} pages");
            take(&to, &from, &mut progress);
            pages += 1;
            from.insert_many(&coll, rows(next, 2 * page)).unwrap();
            next += 2 * page;
        }
        assert_eq!(pages, expected_pages, "{name}");
        let received = to.get_collection("shop", name).unwrap();
        assert_eq!(count(&to, &received), standing as u64, "{name}: only what stood at the start");
    }
}

/// A whole-database walk is never cut at an `until_key`: it grants coverage, so the rows past a
/// cut would be lost for good. Only a snapshot of one collection ends at a key; a whole-database
/// cursor that carries one (it should not) is walked to the end all the same.
#[test]
fn a_whole_database_walk_ignores_an_until_key() {
    let dir = tempfile::tempdir().unwrap();
    let from = open(&dir, "from");
    let page = crate::snapshot::SNAPSHOT_PAGE as i64;
    let coll = from.create_collection("shop", "wide").unwrap();
    let rows: Vec<bson::Document> = (0..3 * page).map(|i| bson::doc! { "_id": i }).collect();
    from.insert_many(&coll, rows).unwrap();
    let first = from.snapshot_page(None, None).unwrap();
    let mut cursor = first.next.clone().expect("more than a page");
    assert_eq!(cursor.until_key, None, "a whole-database page names no end");
    // A cursor with an end that cuts everything after the first page.
    cursor.until_key = Some(vec![0]);
    let mut documents = first.documents.len();
    let mut after = Some(cursor);
    while let Some(cursor) = after {
        let page = from.snapshot_page(Some(cursor), None).unwrap();
        documents += page.documents.len();
        after = page.next;
    }
    assert_eq!(documents as i64, 3 * page, "every row, whatever the cursor says");
}

/// A row that does not fit what a page has left goes first on the next page, and
/// every row arrives. A row that does not fit a page holding nothing else cannot
/// cross on a page: it is refused, naming it; the first page, which carries the
/// definitions, leaves it to the second instead.
#[test]
fn a_row_goes_first_on_the_next_page_and_one_no_page_can_carry_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let plain = y.create_collection("shop", "plain").unwrap();
    let pad = "y".repeat(10_000);
    y.insert_many(&plain, (0..3i64).map(|i| doc! { "_id": i, "pad": pad.clone() }).collect())
        .unwrap();
    // Room for one such document with its cursor, not for two.
    y.set_snapshot_room_for_test(Some(15_000));
    let mut progress = SnapshotProgress::of_collection(plain.id);
    let pages = finish(&x, &y, &mut progress);
    assert_eq!(
        pages.iter().map(|page| page.documents.len()).collect::<Vec<_>>(),
        vec![1, 1, 1],
        "each document first on its page, in order"
    );
    assert!(pages.iter().all(|page| rows_bytes(page) <= 15_000));
    let cx = x.get_collection("shop", "plain").unwrap();
    assert_eq!(count(&x, &cx), 3, "every document, once");

    // Less room than one document: the first page leaves it, the second refuses it.
    y.set_snapshot_room_for_test(Some(5_000));
    let page = y.snapshot_page(None, Some(plain.id)).unwrap();
    assert!(page.documents.is_empty(), "behind the definitions");
    match y.snapshot_page(page.next.clone(), Some(plain.id)) {
        Err(crate::StorageError::SnapshotRowTooLarge { what, bytes, room }) => {
            assert!(what.contains("document with _id Int64(0)"), "{what}");
            assert!(bytes > room, "{bytes} bytes in {room}");
        }
        other => panic!("expected the document refused, got {other:?}"),
    }

    // A tombstone, with a room smaller than its own overhead, is refused likewise.
    y.set_snapshot_room_for_test(None);
    let gone = y.create_collection("shop", "gone").unwrap();
    y.insert(&gone, doc! { "_id": 1i64, "gone": true }).unwrap();
    y.modify_where(&gone, &crate::Candidates::Scan, &Del, None).unwrap();
    y.set_snapshot_room_for_test(Some(100));
    let page = y.snapshot_page(None, Some(gone.id)).unwrap();
    assert!(page.deleted_documents.is_empty(), "behind the definitions");
    match y.snapshot_page(page.next.clone(), Some(gone.id)) {
        Err(e @ crate::StorageError::SnapshotRowTooLarge { .. }) => {
            assert!(e.to_string().contains("a tombstone of collection"), "{e}");
        }
        other => panic!("expected the tombstone refused, got {other:?}"),
    }
}

/// The refusal of a row whose `_id` is too large to cross with its cursor names
/// the id by its kind, its length and its first bytes, not by the id: the text is
/// logged and sent to the requester every round, and a 3.5 MiB binary `_id` once
/// made it 11 MB. (A binary id crosses as bytes, so it takes one over about 6 MiB to
/// leave no room for its cursor.)
#[test]
fn the_refusal_of_a_large_id_names_it_by_its_length_and_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let y = open(&dir, "y");
    let c = y.create_collection("shop", "wideid").unwrap();
    let id = bson::Binary {
        subtype: bson::spec::BinarySubtype::Generic,
        bytes: vec![7u8; 7 * 1024 * 1024],
    };
    y.insert(&c, doc! { "_id": Bson::Binary(id), "n": 1i64 }).unwrap();
    let mut after: Option<SnapshotCursor> = None;
    let mut refused = None;
    for _ in 0..6 {
        match y.snapshot_page(after.clone(), Some(c.id)) {
            Err(e @ crate::StorageError::SnapshotRowTooLarge { .. }) => {
                refused = Some(e.to_string());
                break;
            }
            Ok(page) => match page.next {
                Some(next) => after = Some(next),
                None => panic!("premise: the page was served ({} documents)", page.documents.len()),
            },
            Err(e) => panic!("premise: {e}"),
        }
    }
    let text = refused.expect("premise: the document is refused by name");
    assert!(text.len() < 512, "a refusal of {} bytes, logged and sent every round", text.len());
    assert!(
        text.contains(
            "binary _id of subtype 0 and 7340032 bytes starting 070707070707070707070707"
        ),
        "{text}"
    );
}

/// An `_id` is named by its kind, its length and at most 24 leading bytes, the
/// same name every time, whatever its size; a kind that is small by nature is
/// named whole.
#[test]
fn an_id_is_named_by_its_kind_length_and_a_short_prefix() {
    use crate::snapshot::id_name;
    let long = DocId::Binary { subtype: 0, bytes: (0..200u8).collect() };
    assert_eq!(
        id_name(&long),
        "binary _id of subtype 0 and 200 bytes starting 000102030405060708090a0b0c0d0e0f1011121314151617..."
    );
    assert_eq!(id_name(&long), id_name(&long.clone()));
    assert_eq!(
        id_name(&DocId::Binary { subtype: 4, bytes: vec![1, 2] }),
        "binary _id of subtype 4 and 2 bytes starting 0102"
    );
    assert_eq!(
        id_name(&DocId::Binary { subtype: 0, bytes: Vec::new() }),
        "binary _id of subtype 0 and 0 bytes starting "
    );
    let text = DocId::String(format!("order-{}", "é".repeat(100)));
    let name = id_name(&text);
    // Cut at a character boundary: 6 bytes of ASCII and 9 two-byte characters.
    assert_eq!(name, format!("string _id of 206 bytes starting \"order-{}\"...", "é".repeat(9)));
    assert_eq!(id_name(&DocId::String("abc".into())), "string _id of 3 bytes starting \"abc\"");
    assert_eq!(id_name(&DocId::Int64(7)), "_id Int64(7)");
    for id in [long, text] {
        assert!(id_name(&id).len() < 128, "{}", id_name(&id));
    }
}

/// At the real budget: a page of documents that would pass a frame is cut by
/// bytes, each page's encoding within a frame (`SNAPSHOT_PAGE_BYTES` and its
/// 1 MiB), the cursor going on from the last row sent: 600 documents of 130 KiB,
/// the shape that stalled a v0.51.0 pull for good, whole-database and scoped.
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
}

/// What a page's other parts cost (its definitions, drops and vector):
/// the page encoded with no rows and no cursor, as `snapshot_page` measures it.
fn fixed_bytes(page: &SnapshotPage) -> usize {
    let bare = SnapshotPage {
        documents: Vec::new(),
        deleted_documents: Vec::new(),
        next: None,
        tombstone_horizon: None,
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

// -- The page budget against the encoding: `_id`s and cursor keys --------------------------

/// The bounds the budget counts rows and the cursor at hold of the BSON encoding,
/// row by row, for the shapes that maximise them: the widest collection id and
/// stamp, and `_id`s of every kind, binary ones as
/// large as 4 KiB, which cross as a BSON binary with their subtype; and the
/// cursor, whose key crosses as an array of `int32`s.
#[test]
fn the_row_overheads_bound_the_encoding() {
    use crate::snapshot::{
        DOCUMENT_OVERHEAD, SnapshotDoc, SnapshotTombstone, TOMBSTONE_OVERHEAD, cursor_reserve,
        id_wire_len,
    };
    let node = NodeId::from_bytes([0xab; 16]);
    let stamp = Stamp::new(Hlc { wall_ms: u64::from(u32::MAX) * 1000, counter: u16::MAX }, node);
    let bare = SnapshotPage {
        collections: Vec::new(),
        documents: Vec::new(),
        next: None,
        versions: Default::default(),
        dropped: None,
        dropped_collections: Vec::new(),
        deleted_documents: Vec::new(),
        dropped_indexes: Vec::new(),
        tombstone_horizon: None,
    };
    let size = |page: &SnapshotPage| bson::serialize_to_vec(page).unwrap().len();
    let base = size(&bare);
    let decimal = DocId::try_from_bson(&Bson::Decimal128("1.5".parse().unwrap())).unwrap();
    for (name, id) in [
        ("int64", DocId::Int64(i64::MIN)),
        ("objectid", DocId::ObjectId(bson::oid::ObjectId::new())),
        ("decimal", decimal),
        ("string-1k", DocId::String("s".repeat(1024))),
        ("binary-0-0", DocId::Binary { subtype: 0, bytes: vec![9; 0] }),
        ("binary-0-32", DocId::Binary { subtype: 0, bytes: vec![9; 32] }),
        ("binary-0-256", DocId::Binary { subtype: 0, bytes: vec![9; 256] }),
        ("binary-0-4096", DocId::Binary { subtype: 0, bytes: vec![9; 4096] }),
        ("binary-4-0", DocId::Binary { subtype: 4, bytes: vec![9; 0] }),
        ("binary-4-32", DocId::Binary { subtype: 4, bytes: vec![9; 32] }),
        ("binary-4-256", DocId::Binary { subtype: 4, bytes: vec![9; 256] }),
        ("binary-4-4096", DocId::Binary { subtype: 4, bytes: vec![9; 4096] }),
    ] {
        let key = crate::docs::doc_key(&id).unwrap();
        let body =
            bson::serialize_to_vec(&doc! { "_id": id.to_bson(), "pad": "p".repeat(100) }).unwrap();
        let documents: Vec<SnapshotDoc> = (0..SNAPSHOT_PAGE)
            .map(|_| SnapshotDoc {
                collection: kimmy_core::CollectionId(u64::MAX),
                id: id.clone(),
                stamp,
                body: Some(body.clone()),
            })
            .collect();
        let page = SnapshotPage { documents, ..bare.clone() };
        let counted = SNAPSHOT_PAGE * (body.len() + id_wire_len(&id) + DOCUMENT_OVERHEAD);
        let actual = size(&page) - base;
        assert!(actual <= counted, "documents {name}: {actual} bytes, counted {counted}");
        let tombstones: Vec<SnapshotTombstone> = (0..SNAPSHOT_PAGE)
            .map(|_| SnapshotTombstone {
                collection: kimmy_core::CollectionId(u64::MAX),
                key: key.clone(),
                stamp,
            })
            .collect();
        let page = SnapshotPage { deleted_documents: tombstones, ..bare.clone() };
        let counted = SNAPSHOT_PAGE * (key.len() + TOMBSTONE_OVERHEAD);
        let actual = size(&page) - base;
        assert!(actual <= counted, "tombstones {name}: {actual} bytes, counted {counted}");
        // The cursor a page cuts at such a row.
        let cursor = SnapshotCursor {
            collection: kimmy_core::CollectionId(u64::MAX),
            after_key: key.clone(),
            until_key: Some(key.clone()),
        };
        let page = SnapshotPage { next: Some(cursor), ..bare.clone() };
        let actual = size(&page) - base;
        let reserved = cursor_reserve(key.len(), key.len());
        assert!(actual <= reserved, "the cursor at {name}: {actual} bytes, reserved {reserved}");
    }
}

/// A full page of documents with 4 KiB binary `_id`s (512 of 110,000 bytes was
/// counted at 60.6 MB and crossed at 78.9 MB, over a frame): each page within a
/// frame, and every document arrives once.
#[test]
fn a_page_of_large_binary_ids_stays_within_a_frame() {
    let dir = tempfile::tempdir().unwrap();
    let y = open(&dir, "y");
    let cy = y.create_collection("shop", "bin").unwrap();
    let pad = "x".repeat(110_000);
    for chunk in 0..8u32 {
        let docs = (0..64u32)
            .map(|i| {
                let mut bytes = vec![0u8; 4096];
                bytes[..4].copy_from_slice(&(chunk * 64 + i).to_be_bytes());
                let id = bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes };
                doc! { "_id": Bson::Binary(id), "pad": pad.clone() }
            })
            .collect();
        y.insert_many(&cy, docs).unwrap();
    }
    let x = open(&dir, "x");
    let mut progress = SnapshotProgress::of_collection(cy.id);
    let pages = finish(&x, &y, &mut progress);
    let frame = crate::SNAPSHOT_PAGE_BYTES + 1024 * 1024;
    for page in &pages {
        let encoded = bson::serialize_to_vec(page).unwrap().len();
        assert!(encoded <= frame, "a page of {encoded} bytes, over a frame of {frame}");
    }
    let cx = x.get_collection("shop", "bin").unwrap();
    assert_eq!(count(&x, &cx), 512, "every document, once");
}

/// A page whose rows fill it ends at a document with a 1.9 MiB string `_id`: the
/// cursor the page cuts names that key, which crosses as an array of integers,
/// about 19 MB. It is reserved with the row, so the page stays within a frame.
#[test]
fn a_full_page_ending_at_a_large_id_stays_within_a_frame() {
    let dir = tempfile::tempdir().unwrap();
    let y = open(&dir, "y");
    let cy = y.create_collection("shop", "wide").unwrap();
    let pad = "x".repeat(1024 * 1024);
    for i in 0..59 {
        y.insert(&cy, doc! { "_id": format!("a{i:03}"), "pad": pad.clone() }).unwrap();
    }
    let big = format!("b{}", "z".repeat(1900 * 1024));
    y.insert(&cy, doc! { "_id": big, "n": 1i64 }).unwrap();
    y.insert(&cy, doc! { "_id": "c", "pad": pad.clone() }).unwrap();
    let x = open(&dir, "x");
    let mut progress = SnapshotProgress::of_collection(cy.id);
    let pages = finish(&x, &y, &mut progress);
    let frame = crate::SNAPSHOT_PAGE_BYTES + 1024 * 1024;
    for page in &pages {
        let encoded = bson::serialize_to_vec(page).unwrap().len();
        assert!(encoded <= frame, "a page of {encoded} bytes, over a frame of {frame}");
    }
    let cx = x.get_collection("shop", "wide").unwrap();
    assert_eq!(count(&x, &cx), 61, "every document, once");
}

/// A converge pull counts the rows its pages carried for its bound, tombstones
/// with documents: a collection of 512 documents and 512 tombstones, both held
/// stale by the receiver, comes to 1,024 rows.
#[test]
fn a_converge_pull_counts_its_tombstones_with_its_documents() {
    let dir = tempfile::tempdir().unwrap();
    let (y, c) = (open(&dir, "y"), open(&dir, "c"));
    let cy = y.create_collection("shop", "orders").unwrap();
    let page = SNAPSHOT_PAGE as i64;
    let docs = (1..=2 * page).map(|i| {
        if i > page {
            doc! { "_id": i, "gone": true }
        } else {
            doc! { "_id": i, "n": 0i64 }
        }
    });
    y.insert_many(&cy, docs.collect()).unwrap();
    finish(&c, &y, &mut SnapshotProgress::whole_database());
    // Every document rewritten and every other deleted, after C pulled them.
    y.modify_where(&cy, &crate::Candidates::Scan, &Inc, None).unwrap();
    y.modify_where(&cy, &crate::Candidates::Scan, &Del, None).unwrap();
    let cc = c.get_collection("shop", "orders").unwrap();
    let mut converge = SnapshotProgress::of_collection_converging(cc.id, Default::default());
    let pages = finish(&c, &y, &mut converge);
    let (documents, tombstones): (usize, usize) = pages
        .iter()
        .map(|p| (p.documents.len(), p.deleted_documents.len()))
        .fold((0, 0), |(d, t), (pd, pt)| (d + pd, t + pt));
    assert_eq!((documents, tombstones), (512, 512), "premise: the pages' rows");
    assert_eq!(converge.rows(), 1024, "documents and tombstones, every one counted");
    // And a whole-database pull counts none: the bound is a converge pull's.
    let z = open(&dir, "z");
    let mut whole = SnapshotProgress::whole_database();
    finish(&z, &y, &mut whole);
    assert_eq!(whole.rows(), 0);
}

/// A page's cursor names a key that crosses as an array of integers, which can be
/// large: under a test page budget of 8 MiB, a pull of documents with 200 KiB
/// string `_id`s (about 2.5 MB as each of the cursor's two keys crosses, so about
/// 5 MB for the cursor that names one) reserves the cursor with each row, so every
/// page's encoding stays within the budget and every document arrives once.
#[test]
fn a_page_reserves_the_large_key_of_the_cursor_it_cuts() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x) = (open(&dir, "y"), open(&dir, "x"));
    let cy = y.create_collection("shop", "keys").unwrap();
    let wide = |i: u8| format!("{}{}", char::from(b'a' + i), "k".repeat(200 * 1024));
    let pad = "x".repeat(400 * 1024);
    for i in 0..6 {
        y.insert(&cy, doc! { "_id": wide(i), "n": 0i64, "pad": pad.clone() }).unwrap();
    }
    // The cursor carries the key twice now (where to resume, and where the snapshot ends),
    // each as an array of `int32`s, so a page of these keys needs room for both.
    let budget = 8 * 1024 * 1024;
    y.set_snapshot_page_bytes_for_test(Some(budget));
    let mut progress = SnapshotProgress::of_collection(cy.id);
    let pages = finish(&x, &y, &mut progress);
    y.set_snapshot_page_bytes_for_test(None);
    assert!(pages.len() > 1, "premise: cut by bytes: {} pages", pages.len());
    for page in &pages {
        let encoded = bson::serialize_to_vec(page).unwrap().len();
        assert!(encoded <= budget + 4096, "a page of {encoded} bytes, over {budget}");
    }
    let cx = x.get_collection("shop", "keys").unwrap();
    assert_eq!(count(&x, &cx), 6, "every document, once");
}
