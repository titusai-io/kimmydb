//! Records travel with coverage (B5, finding G1 and its siblings): schedules
//! through the storage halves of the real paths, with the listener's serve
//! verdict (ADR-224) applied before every window and a whole-database snapshot
//! taken where the real loop would take one. Each ends on a member that holds a
//! keyed request's effect, where a retry must be answered from the record and
//! leave the counter as it is. They come from the independent analysis of G1
//! (`g1rev_tests.rs`), which ran each against the code before the fix (every one
//! but the first failed there) and against a prototype of it.

use std::cmp::Ordering;

use bson::{Bson, Document, doc};
use kimmy_core::{DocId, Fingerprint, Hlc, ScopeHash, Stamp};

use crate::idempotency::{Keyed, KeyedWrite, RecordsVia};
use crate::{
    Candidates, CollectionMeta, Engine, ModifySpec, PeerWindow, ServeVerdict, SnapshotProgress,
    SyncOutcome,
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

/// What a pull round did.
#[derive(Debug, PartialEq)]
enum Pulled {
    Served,
    Refused(ServeVerdict),
}

/// One pull of `to` from `from`, as the listener serves it: the verdict first
/// (`transport.rs` `AskEntries`), the requester's held marks named, the window
/// from its witnessed vector, and the records the window carries.
fn pull(to: &Engine, from: &Engine) -> Pulled {
    for _ in 0..16 {
        let theirs = from.version_vector().unwrap();
        let mine = to.witnessed_vector().unwrap();
        let verdict = from.serve_verdict(&mine).unwrap();
        if verdict != ServeVerdict::Serve {
            return Pulled::Refused(verdict);
        }
        let Some(start) = mine.behind(&theirs) else { return Pulled::Served };
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
            return Pulled::Served;
        }
    }
    Pulled::Served
}

/// A pull, and the whole-database snapshot the loop takes when the peer answers
/// `BeyondHorizon`.
fn pull_or_snapshot(to: &Engine, from: &Engine) -> Pulled {
    let pulled = pull(to, from);
    if let Pulled::Refused(_) = pulled {
        snapshot(to, from);
    }
    pulled
}

fn snapshot(to: &Engine, from: &Engine) {
    let mut progress = SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let page = from.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        to.apply_snapshot_page(from.node_id(), &mut progress, &page).unwrap();
    }
}

fn n_of(engine: &Engine, coll: &CollectionMeta, id: i64) -> i64 {
    engine.get(coll, &DocId::Int64(id)).unwrap().unwrap().get_i64("n").unwrap()
}

fn key() -> KeyedWrite {
    KeyedWrite::new(ScopeHash([7; 32]), Fingerprint([7; 32]))
}

/// Move `engine`'s clock `ms` past `stamp`: a later physical time, which is all a
/// write made "at about the same time" on another member needs.
fn later_than(engine: &Engine, stamp: Stamp, ms: u64) {
    engine.advance_clock_past(&Stamp::new(Hlc::new(stamp.hlc.wall_ms + ms, 0), engine.node_id()));
}

/// The assertion every test ends with: `m` holds the request's effect, so a
/// retry there must replay and leave the counter as it is.
fn retry_must_replay(m: &Engine, cm: &CollectionMeta, k: &KeyedWrite, id: i64, single: bool) {
    let before = n_of(m, cm, id);
    let held = m.idempotency_record(cm.id, &k.scope).unwrap();
    let stop = if single { Some(1) } else { None };
    let retry = m.modify_where_keyed(cm, &Candidates::Scan, &Inc, stop, k).unwrap();
    let after = n_of(m, cm, id);
    assert!(
        held.is_some() && matches!(retry, Keyed::Replayed(_)) && after == before,
        "M holds the effect (n = {before}) without the record ({held:?}); the retry \
         answered {retry:?} and n is now {after}"
    );
}

/// G1 exactly as reported, but through the real verdict: X took the database by
/// snapshot, so it refuses M's window (`StateHeld`, ADR-224) and M takes a snapshot
/// of X, whose records phase carries the record. It passed before the fix too: the
/// reported route is closed in production by ADR-224, and the reproducer that
/// reported it served a window without the verdict.
#[test]
fn g1_as_reported_is_refused_by_the_state_held_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x, m) = (open(&dir, "y"), open(&dir, "x"), open(&dir, "m"));
    let cy = y.create_collection("shop", "orders").unwrap();
    y.insert(&cy, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    assert_eq!(pull(&m, &y), Pulled::Served);
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = key();
    assert!(matches!(
        y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap(),
        Keyed::Fresh(_)
    ));
    y.replace(&cy, &DocId::Int64(1), doc! { "n": 11i64 }, false).unwrap();
    snapshot(&x, &y);
    assert_eq!(pull_or_snapshot(&m, &x), Pulled::Refused(ServeVerdict::StateHeld));
    pull_or_snapshot(&m, &y);
    assert_eq!(n_of(&m, &cm, 1), 11);
    retry_must_replay(&m, &cm, &k, 1, true);
}

/// G2, frames only, three members, no snapshot or repair: X writes the document
/// at about the time Y's keyed `$inc` lands, so the `$inc`'s entry loses on X
/// (stored, not appended); Y then writes on top of its own `$inc`, which wins on X.
/// X's oplog holds the later version and not the `$inc`; M pulls X, takes the
/// later version, and its coverage of Y passes the `$inc`, so Y never serves it.
#[test]
fn g2_a_loser_relay_drops_the_record() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x, m) = (open(&dir, "y"), open(&dir, "x"), open(&dir, "m"));
    let cy = y.create_collection("shop", "orders").unwrap();
    y.insert(&cy, doc! { "_id": 1i64, "n": 10i64 }).unwrap();
    assert_eq!(pull(&x, &y), Pulled::Served);
    assert_eq!(pull(&m, &y), Pulled::Served);
    let (cx, cm) =
        (x.get_collection("shop", "orders").unwrap(), m.get_collection("shop", "orders").unwrap());
    let k = key();
    assert!(matches!(
        y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap(),
        Keyed::Fresh(_)
    ));
    let s1 = y.idempotency_record(cy.id, &k.scope).unwrap().unwrap().first;
    later_than(&x, s1, 10);
    x.replace(&cx, &DocId::Int64(1), doc! { "n": 100i64 }, false).unwrap();
    later_than(&y, s1, 20);
    y.replace(&cy, &DocId::Int64(1), doc! { "n": 11i64, "tag": "y" }, false).unwrap();
    assert_eq!(pull(&x, &y), Pulled::Served);
    assert!(x.idempotency_record(cx.id, &k.scope).unwrap().is_some(), "premise: X stored it");
    assert_eq!(n_of(&x, &cx, 1), 11, "premise: Y's later version won on X");
    assert_eq!(pull(&m, &x), Pulled::Served);
    assert_eq!(pull(&m, &y), Pulled::Served);
    assert_eq!(n_of(&m, &cm, 1), 11);
    retry_must_replay(&m, &cm, &k, 1, true);
}

/// G2b: the same with nothing written on top. One keyed update-many writes two
/// documents in one transaction; its record rides the first entry. X's own write
/// to the first document beats that entry on X, and the second entry wins there,
/// so X relays the request's own effect without its record.
#[test]
fn g2b_a_multi_document_write_whose_first_entry_lost_is_relayed_bare() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x, m) = (open(&dir, "y"), open(&dir, "x"), open(&dir, "m"));
    let cy = y.create_collection("shop", "orders").unwrap();
    y.insert(&cy, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    y.insert(&cy, doc! { "_id": 2i64, "n": 0i64 }).unwrap();
    assert_eq!(pull(&x, &y), Pulled::Served);
    assert_eq!(pull(&m, &y), Pulled::Served);
    let (cx, cm) =
        (x.get_collection("shop", "orders").unwrap(), m.get_collection("shop", "orders").unwrap());
    let k = key();
    assert!(matches!(
        y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, None, &k).unwrap(),
        Keyed::Fresh(_)
    ));
    let s1 = y.idempotency_record(cy.id, &k.scope).unwrap().unwrap().first;
    later_than(&x, s1, 10);
    x.replace(&cx, &DocId::Int64(1), doc! { "n": 100i64 }, false).unwrap();
    assert_eq!(pull(&x, &y), Pulled::Served);
    assert_eq!(n_of(&x, &cx, 2), 1, "premise: the second entry won on X");
    assert_eq!(pull(&m, &x), Pulled::Served);
    assert_eq!(pull(&m, &y), Pulled::Served);
    assert_eq!(n_of(&m, &cm, 2), 1);
    retry_must_replay(&m, &cm, &k, 2, false);
}

/// G1b, the snapshot route that the verdict does not refuse: Y's keyed `$inc` and
/// the write on top land while X's snapshot of Y is in flight, after its first page
/// (whose vector is the grant) and before the collection's records phase. X stores
/// the record from the page and holds the later version above the grant; it then
/// pulls Y, where the `$inc`'s entry loses to that version. M, as far as the grant,
/// pulls X and is served (no `StateHeld`): the later version, without the record.
#[test]
fn g1b_a_write_during_a_snapshot_is_relayed_bare_by_its_receiver() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x, m) = (open(&dir, "y"), open(&dir, "x"), open(&dir, "m"));
    // A first collection big enough to fill the first page.
    let filler = y.create_collection("shop", "aaa").unwrap();
    for i in 0..(crate::SNAPSHOT_PAGE as i64 + 8) {
        y.insert(&filler, doc! { "_id": i, "v": i }).unwrap();
    }
    let cy = y.create_collection("shop", "orders").unwrap();
    y.insert(&cy, doc! { "_id": 1i64, "n": 10i64 }).unwrap();
    assert_eq!(pull(&m, &y), Pulled::Served);
    let cm = m.get_collection("shop", "orders").unwrap();
    // Order the walk: the collection ids decide which goes first.
    assert!(filler.id < cy.id, "premise: the filler is walked first");
    let mut progress = SnapshotProgress::whole_database();
    let page = y.snapshot_page(None, None).unwrap();
    assert!(page.next.is_some(), "premise: more than one page");
    x.apply_snapshot_page(y.node_id(), &mut progress, &page).unwrap();
    // The writes, between the first page and the collection's records phase.
    let k = key();
    assert!(matches!(
        y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap(),
        Keyed::Fresh(_)
    ));
    y.replace(&cy, &DocId::Int64(1), doc! { "n": 11i64, "tag": "y" }, false).unwrap();
    while !progress.is_complete() {
        let page = y.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        x.apply_snapshot_page(y.node_id(), &mut progress, &page).unwrap();
    }
    let cx = x.get_collection("shop", "orders").unwrap();
    assert!(x.idempotency_record(cx.id, &k.scope).unwrap().is_some(), "premise: page record");
    assert_eq!(pull(&x, &y), Pulled::Served);
    assert_eq!(pull(&m, &x), Pulled::Served, "M is at the grant: no StateHeld");
    assert_eq!(pull(&m, &y), Pulled::Served);
    assert_eq!(n_of(&m, &cm, 1), 11);
    retry_must_replay(&m, &cm, &k, 1, true);
}

/// T2, the transient class: the effect reaches M under another origin while the
/// relay cannot vouch for the record's stamp. Y's keyed `$inc` loses on X to X's
/// own write; Z took the `$inc` and wrote on top of it; that version wins on X. X
/// appended nothing of Y's past the insert, so its window does not take M's
/// coverage of Y past the `$inc`: M holds Z's version, which holds the `$inc`, and
/// no record, until it pulls Y or Z. With Y and Z down, a retry on M doubles.
#[test]
fn t2_a_cross_origin_relay_leaves_the_effect_bare_while_its_origin_is_down() {
    let dir = tempfile::tempdir().unwrap();
    let (y, z, x, m) = (open(&dir, "y"), open(&dir, "z"), open(&dir, "x"), open(&dir, "m"));
    let cy = y.create_collection("shop", "orders").unwrap();
    y.insert(&cy, doc! { "_id": 1i64, "n": 10i64 }).unwrap();
    for member in [&z, &x, &m] {
        assert_eq!(pull(member, &y), Pulled::Served);
    }
    let cz = z.get_collection("shop", "orders").unwrap();
    let cx = x.get_collection("shop", "orders").unwrap();
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = key();
    assert!(matches!(
        y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap(),
        Keyed::Fresh(_)
    ));
    let s1 = y.idempotency_record(cy.id, &k.scope).unwrap().unwrap().first;
    later_than(&x, s1, 10);
    x.replace(&cx, &DocId::Int64(1), doc! { "n": 100i64 }, false).unwrap();
    assert_eq!(pull(&z, &y), Pulled::Served);
    later_than(&z, s1, 20);
    z.replace(&cz, &DocId::Int64(1), doc! { "n": 11i64, "tag": "z" }, false).unwrap();
    assert_eq!(pull(&x, &y), Pulled::Served);
    assert_eq!(pull(&x, &z), Pulled::Served);
    assert_eq!(n_of(&x, &cx, 1), 11, "premise: Z's version won on X");
    // Y and Z go down; M hears only X.
    assert_eq!(pull(&m, &x), Pulled::Served);
    assert_eq!(n_of(&m, &cm, 1), 11);
    retry_must_replay(&m, &cm, &k, 1, true);
}

/// G3, a coverage-only window: M's coverage of Y passes the `$inc` in a window that
/// carries no entry of its collection (X lost the `$inc` to its own write, which M
/// already holds, and then appended a later write of Y's to another collection).
/// Nothing of the request's effect reaches M then, but Y never serves M the `$inc`
/// again, so when Y's later write on top of its `$inc` reaches M, the record must
/// already be there: the receiver stores a record the window carries even when no
/// entry of its collection is taken.
#[test]
fn g3_a_coverage_only_window_passes_the_request_without_its_record() {
    let dir = tempfile::tempdir().unwrap();
    let (y, x, m) = (open(&dir, "y"), open(&dir, "x"), open(&dir, "m"));
    let cy = y.create_collection("shop", "orders").unwrap();
    let other = y.create_collection("shop", "zzz").unwrap();
    y.insert(&cy, doc! { "_id": 1i64, "n": 10i64 }).unwrap();
    assert_eq!(pull(&x, &y), Pulled::Served);
    assert_eq!(pull(&m, &y), Pulled::Served);
    let (cx, cm) =
        (x.get_collection("shop", "orders").unwrap(), m.get_collection("shop", "orders").unwrap());
    let k = key();
    assert!(matches!(
        y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap(),
        Keyed::Fresh(_)
    ));
    let s1 = y.idempotency_record(cy.id, &k.scope).unwrap().unwrap().first;
    later_than(&x, s1, 10);
    x.replace(&cx, &DocId::Int64(1), doc! { "n": 100i64 }, false).unwrap();
    assert_eq!(pull(&m, &x), Pulled::Served, "M takes X's write first");
    assert_eq!(pull(&x, &y), Pulled::Served, "the $inc loses on X");
    later_than(&y, s1, 20);
    y.insert(&other, doc! { "_id": 1i64 }).unwrap();
    assert_eq!(pull(&x, &y), Pulled::Served);
    assert_eq!(pull(&m, &x), Pulled::Served, "coverage of Y passes the $inc, nothing of orders");
    // Y, which never heard X's write, writes on top of its own `$inc`.
    y.replace(&cy, &DocId::Int64(1), doc! { "n": 11i64, "tag": "y" }, false).unwrap();
    assert_eq!(pull(&m, &y), Pulled::Served);
    assert_eq!(n_of(&m, &cm, 1), 11);
    retry_must_replay(&m, &cm, &k, 1, true);
}

/// G8: the record-only `done` commit mints a `last` of its own. M pulls Y between
/// the last chunk that wrote and the chunk that finds nothing and only marks the
/// record `done`, so it holds the record `applying` and covers its `last` as it
/// stood. The `done` record's new row lies above that, so the next window Y
/// serves M carries it (one comes once Y writes again: a member that is not
/// behind its peer's vector asks it for no window), and a retry on M replays
/// rather than resumes. With the `last` kept, no window would ever carry it.
#[test]
fn g8_a_member_that_pulled_before_the_done_commit_replays_a_retry() {
    let dir = tempfile::tempdir().unwrap();
    let (y, m) = (std::sync::Arc::new(open(&dir, "y")), std::sync::Arc::new(open(&dir, "m")));
    let cy = y.create_collection("shop", "orders").unwrap();
    y.set_multi_chunk_docs(1);
    y.insert_many(&cy, vec![doc! { "_id": 1i64, "n": 0i64 }, doc! { "_id": 2i64, "n": 0i64 }])
        .unwrap();
    assert_eq!(pull(&m, &y), Pulled::Served);
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = key();
    // Before the second chunk, arm a pull before the third: the chunk that finds
    // nothing and commits the record alone.
    let (y1, m1) = (std::sync::Arc::clone(&y), std::sync::Arc::clone(&m));
    y.before_next_continuing_write(move || {
        let (y2, m2) = (std::sync::Arc::clone(&y1), std::sync::Arc::clone(&m1));
        y1.before_next_continuing_write(move || {
            assert_eq!(pull(&m2, &y2), Pulled::Served);
        });
    });
    let out = y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, None, &k).unwrap();
    assert!(matches!(out, Keyed::Fresh(_)), "{out:?}");
    let held = m.idempotency_record(cm.id, &k.scope).unwrap().expect("premise: M pulled it");
    assert_eq!(held.state, crate::idempotency::RecordState::Applying, "premise: before `done`");
    assert_eq!(n_of(&m, &cm, 2), 1, "premise: M holds both chunks");
    // Y writes on, unkeyed and elsewhere, and M pulls.
    let other = y.create_collection("shop", "other").unwrap();
    y.insert(&other, doc! { "_id": 1i64 }).unwrap();
    assert_eq!(pull(&m, &y), Pulled::Served);
    assert_eq!(
        m.idempotency_record(cm.id, &k.scope).unwrap().map(|r| r.state),
        Some(crate::idempotency::RecordState::Done)
    );
    retry_must_replay(&m, &cm, &k, 2, false);
}
