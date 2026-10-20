//! Records travel with coverage (ADR-240): schedules
//! through the storage halves of the real paths, with the listener's serve
//! verdict (ADR-224) applied before every window and a whole-database snapshot
//! taken where the real loop would take one. Each ends on a member that holds a
//! keyed request's effect, where a retry must be answered from the record and
//! leave the counter as it is. They come from an independent analysis of how an
//! effect could be relayed without its record, which ran each against the code
//! before the fix (every one but the first failed there) and against a prototype
//! of it.

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

/// A relay after a snapshot, through the real verdict: X took the database by
/// snapshot, so it refuses M's window (`StateHeld`, ADR-224) and M takes a
/// snapshot of X, whose records phase carries the record. It passed before the
/// fix too: the reported route is closed in production by ADR-224, and the
/// reproducer that reported it served a window without the verdict.
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

/// A losing entry relayed, frames only, three members, no snapshot or repair: X
/// writes the document at about the time Y's keyed `$inc` lands, so the
/// `$inc`'s entry loses on X (stored, not appended); Y then writes on top of
/// its own `$inc`, which wins on X. X's oplog holds the later version and not
/// the `$inc`; M pulls X, takes the later version, and its coverage of Y passes
/// the `$inc`, so Y never serves it.
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

/// The same with nothing written on top. One keyed update-many writes two
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

/// The snapshot route that the verdict does not refuse: Y's keyed `$inc` and
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

/// A cross-origin relay, the transient class: the effect reaches M under
/// another origin while the relay cannot vouch for the record's stamp. Y's
/// keyed `$inc` loses on X to X's own write; Z took the `$inc` and wrote on top
/// of it; that version wins on X. X appended nothing of Y's past the insert, so
/// its window does not take M's coverage of Y past the `$inc`: M holds Z's
/// version, which holds the `$inc`, and no record, until it pulls Y or Z. With
/// Y and Z down, a retry on M doubles.
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

/// A coverage-only window: M's coverage of Y passes the `$inc` in a window that
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

/// G8: a keyed multi-chunk request writes `done` in the chunk whose lookahead
/// finds its end, so a member fully caught up on its sender gets `done` with the
/// last chunk's entry and replays a retry. Nothing is written after the end. M
/// pulls Y after the first chunk, and again once the request has answered; a
/// pull is also armed before a third chunk, which runs only when the lookahead
/// is off (that chunk finds nothing and marks the record `done` alone, keeping
/// its `last`, so a member that pulled before it would never receive it).
#[test]
fn g8_a_caught_up_member_gets_done_with_the_last_chunk_and_replays() {
    let dir = tempfile::tempdir().unwrap();
    let (y, m) = (std::sync::Arc::new(open(&dir, "y")), std::sync::Arc::new(open(&dir, "m")));
    let cy = y.create_collection("shop", "orders").unwrap();
    y.set_multi_chunk_docs(1);
    y.insert_many(&cy, vec![doc! { "_id": 1i64, "n": 0i64 }, doc! { "_id": 2i64, "n": 0i64 }])
        .unwrap();
    assert_eq!(pull(&m, &y), Pulled::Served);
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = key();
    // Before the second chunk, M pulls the first; and a pull is armed before a
    // third chunk, which only a request without the lookahead makes.
    let (y1, m1) = (std::sync::Arc::clone(&y), std::sync::Arc::clone(&m));
    y.before_next_continuing_write(move || {
        assert_eq!(pull(&m1, &y1), Pulled::Served);
        let (y2, m2) = (std::sync::Arc::clone(&y1), std::sync::Arc::clone(&m1));
        y1.before_next_continuing_write(move || {
            assert_eq!(pull(&m2, &y2), Pulled::Served);
        });
    });
    let out = y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, None, &k).unwrap();
    let Keyed::Fresh(out) = out else { panic!("a fresh request: {out:?}") };
    assert_eq!((out.modified, out.commits), (2, 2), "{out:?}");
    // Nothing written after the end: M pulls once more, caught up or not.
    pull(&m, &y);
    assert_eq!(n_of(&m, &cm, 2), 1, "premise: M holds both chunks");
    assert_eq!(
        m.idempotency_record(cm.id, &k.scope).unwrap().map(|r| r.state),
        Some(crate::idempotency::RecordState::Done),
        "M holds the request's end"
    );
    retry_must_replay(&m, &cm, &k, 2, false);
}

/// G8's residual: when the last writing chunk's lookahead spends its budget on
/// matches the stamp rule leaves, a later chunk finds the end having written
/// nothing and marks the record `done` alone, keeping its `last`. A member that
/// pulled the last writing chunk resumes a retry: `resumed`, every match left
/// by the stamp rule, nothing written a second time.
#[test]
fn g8_residual_a_tail_of_left_matches_past_the_lookahead_resumes_without_a_double() {
    let dir = tempfile::tempdir().unwrap();
    let (y, m) = (std::sync::Arc::new(open(&dir, "y")), std::sync::Arc::new(open(&dir, "m")));
    let cy = y.create_collection("shop", "orders").unwrap();
    y.set_multi_chunk_docs(1);
    let docs: Vec<Document> = (1..=3i64).map(|i| doc! { "_id": i, "n": 0i64 }).collect();
    y.insert_many(&cy, docs).unwrap();
    assert_eq!(pull(&m, &y), Pulled::Served);
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = key();
    // After the first chunk (whose lookahead saw document 2 writable), another
    // writer rewrites documents 2 and 3, so the stamp rule leaves both; M pulls
    // all of it before the next chunk.
    let (y1, m1) = (std::sync::Arc::clone(&y), std::sync::Arc::clone(&m));
    y.before_next_continuing_write(move || {
        let coll = y1.get_collection("shop", "orders").unwrap();
        for id in [2i64, 3] {
            y1.replace(&coll, &DocId::Int64(id), doc! { "n": 50i64 }, false).unwrap();
        }
        assert_eq!(pull(&m1, &y1), Pulled::Served);
    });
    let Keyed::Fresh(out) = y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, None, &k).unwrap()
    else {
        panic!("a fresh request")
    };
    assert_eq!((out.modified, out.skipped, out.commits), (1, 2, 1), "{out:?}");
    let stored = y.idempotency_record(cy.id, &k.scope).unwrap().unwrap();
    assert_eq!(stored.state, crate::idempotency::RecordState::Done);
    pull(&m, &y);
    let held = m.idempotency_record(cm.id, &k.scope).unwrap().unwrap();
    assert_eq!(held.state, crate::idempotency::RecordState::Applying, "premise: the residual");
    let before: Vec<i64> = (1..=3).map(|id| n_of(&m, &cm, id)).collect();
    let Keyed::Fresh(again) = m.modify_where_keyed(&cm, &Candidates::Scan, &Inc, None, &k).unwrap()
    else {
        panic!("a resume")
    };
    assert!(again.resumed, "{again:?}");
    assert_eq!((again.modified, again.skipped), (1, 3), "cumulative, everything left: {again:?}");
    assert_eq!((1..=3).map(|id| n_of(&m, &cm, id)).collect::<Vec<_>>(), before, "no double");
}

/// G8: the lookahead finds the end past matches the stamp rule leaves. A
/// two-document chunk writes documents 3 and 4; past them only document 5 is
/// left, rewritten by another writer after the request's first write, and then
/// the scan ends: the chunk writes `done`, counting document 5 in `skipped`. A
/// member that pulled before a further chunk would have run (one runs only if
/// the lookahead took document 5 for a match it could write) gets `done` and
/// replays.
#[test]
fn g8_the_lookahead_ends_past_matches_the_stamp_rule_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let (y, m) = (std::sync::Arc::new(open(&dir, "y")), std::sync::Arc::new(open(&dir, "m")));
    let cy = y.create_collection("shop", "orders").unwrap();
    y.set_multi_chunk_docs(2);
    let docs: Vec<Document> = (1..=5i64).map(|i| doc! { "_id": i, "n": 0i64 }).collect();
    y.insert_many(&cy, docs).unwrap();
    assert_eq!(pull(&m, &y), Pulled::Served);
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = key();
    let (y1, m1) = (std::sync::Arc::clone(&y), std::sync::Arc::clone(&m));
    y.before_next_continuing_write(move || {
        let coll = y1.get_collection("shop", "orders").unwrap();
        y1.replace(&coll, &DocId::Int64(5), doc! { "n": 50i64 }, false).unwrap();
        let (y2, m2) = (std::sync::Arc::clone(&y1), std::sync::Arc::clone(&m1));
        y1.before_next_continuing_write(move || {
            assert_eq!(pull(&m2, &y2), Pulled::Served);
        });
    });
    let Keyed::Fresh(out) = y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, None, &k).unwrap()
    else {
        panic!("a fresh request")
    };
    assert_eq!((out.modified, out.skipped, out.commits), (4, 1, 2), "{out:?}");
    pull(&m, &y);
    assert_eq!(
        m.idempotency_record(cm.id, &k.scope).unwrap().map(|r| r.state),
        Some(crate::idempotency::RecordState::Done)
    );
    retry_must_replay(&m, &cm, &k, 4, false);
    assert_eq!(n_of(&m, &cm, 5), 50, "left as the other writer left it");
}

/// One pull of `to` from `from`, the window as served but with an entry of a
/// collection `to` has never heard of inserted at `stop`: the batch stops there
/// (ADR-123), as it does at an unknown collection or a creation waiting for the
/// purger (ADR-189).
fn pull_stopping_at(to: &Engine, from: &Engine, stop: Stamp) -> SyncOutcome {
    let theirs = from.version_vector().unwrap();
    let mine = to.witnessed_vector().unwrap();
    let start = mine.behind(&theirs).unwrap();
    let marked = to.held_ranges_covered_by(&mine).unwrap();
    let mut window = from.serve_entries_to_peer(start, 1024, Some(&mine), &marked, None).unwrap();
    window.entries.push(kimmy_core::OplogEntry {
        stamp: stop,
        kind: kimmy_core::OpKind::Insert,
        collection: kimmy_core::CollectionId(0x5EED_5EED),
        doc_id: Some(DocId::Int64(1)),
        body: Some(bson::serialize_to_vec(&doc! { "_id": 1i64 }).unwrap()),
    });
    window.entries.sort_by_key(|e| e.stamp);
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
    outcome
}

/// B's keyed `$inc` (r_b), then the retry on A, which lacks B's record (r_a,
/// later). X holds both, merged, with rows at r_b and r_a. M pulls X, and its
/// batch stops between the two rows. A window carries a record at the
/// **lowest** row it finds: carried at r_a, as it was when A sorts first and
/// its row was found first, the stopped batch stores no record whose row is at
/// or past the stop, though its coverage passes r_b and it took r_b's effect;
/// B, the only member M then reaches, ships nothing, since M covers its rows,
/// and a retry on M applies again.
#[test]
fn a_stopped_batch_covers_a_row_whose_record_rode_a_later_one() {
    a_stopped_batch(true);
}

/// The same schedule with B sorting first, whose row was found first already.
#[test]
fn a_stopped_batch_with_the_lower_row_found_first() {
    a_stopped_batch(false);
}

fn a_stopped_batch(a_sorts_first: bool) {
    let dir = tempfile::tempdir().unwrap();
    let b = open(&dir, "b");
    let mut n = 0;
    let a = loop {
        let a = open(&dir, &format!("a{n}"));
        n += 1;
        if (a.node_id() < b.node_id()) == a_sorts_first {
            break a;
        }
    };
    let (x, m) = (open(&dir, "x"), open(&dir, "m"));
    let cb = b.create_collection("shop", "orders").unwrap();
    b.insert(&cb, doc! { "_id": 1i64, "n": 10i64 }).unwrap();
    for member in [&a, &x, &m] {
        assert_eq!(pull(member, &b), Pulled::Served);
    }
    let ca = a.get_collection("shop", "orders").unwrap();
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = key();
    assert!(matches!(
        b.modify_where_keyed(&cb, &Candidates::Scan, &Inc, Some(1), &k).unwrap(),
        Keyed::Fresh(_)
    ));
    let r_b = b.idempotency_record(cb.id, &k.scope).unwrap().unwrap().first;
    later_than(&a, r_b, 10);
    assert!(matches!(
        a.modify_where_keyed(&ca, &Candidates::Scan, &Inc, Some(1), &k).unwrap(),
        Keyed::Fresh(_)
    ));
    let r_a = a.idempotency_record(ca.id, &k.scope).unwrap().unwrap().first;
    assert_eq!(pull(&x, &b), Pulled::Served);
    assert_eq!(pull(&x, &a), Pulled::Served);
    let stop = Stamp::new(Hlc::new(r_b.hlc.wall_ms + 1, 0), a.node_id());
    assert!(r_b < stop && stop < r_a, "premise: the stop lies between the two");
    let out = pull_stopping_at(&m, &x, stop);
    assert_eq!(out.unknown_collection, 1, "premise: the batch stopped: {out:?}");
    assert_eq!(n_of(&m, &cm, 1), 11, "premise: M took B's application");
    assert!(m.witnessed_vector().unwrap().get(b.node_id()) >= r_b.hlc, "premise: covers r_b");
    // X and A go down; M hears only B.
    assert_eq!(pull(&m, &b), Pulled::Served);
    retry_must_replay(&m, &cm, &k, 1, true);
}

/// The same carried row, without a stop. X's window holds B's keyed `$inc`
/// (r_b), a schema change, and A's retry (r_a, A sorting first). The schema
/// change ends M's first run, which commits r_b's effect; the batch then fails
/// before its last commit (a crash, an I/O error). Carried at r_a, the record
/// would ride with an entry at or after r_a and the first run would hold the
/// effect without it, its own append covering r_b; carried at r_b, the lowest
/// row, it rides with r_b's entry in that run.
#[test]
fn a_run_committed_before_the_carried_row_holds_the_effect_bare() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(&dir, "b");
    let mut n = 0;
    let a = loop {
        let a = open(&dir, &format!("a{n}"));
        n += 1;
        if a.node_id() < b.node_id() {
            break a;
        }
    };
    let (x, m) = (open(&dir, "x"), open(&dir, "m"));
    let cb = b.create_collection("shop", "orders").unwrap();
    b.insert(&cb, doc! { "_id": 1i64, "n": 10i64 }).unwrap();
    for member in [&a, &x, &m] {
        assert_eq!(pull(member, &b), Pulled::Served);
    }
    let ca = a.get_collection("shop", "orders").unwrap();
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = key();
    assert!(matches!(
        b.modify_where_keyed(&cb, &Candidates::Scan, &Inc, Some(1), &k).unwrap(),
        Keyed::Fresh(_)
    ));
    let r_b = b.idempotency_record(cb.id, &k.scope).unwrap().unwrap().first;
    later_than(&a, r_b, 5);
    a.create_collection("shop", "zzz").unwrap();
    later_than(&a, r_b, 10);
    assert!(matches!(
        a.modify_where_keyed(&ca, &Candidates::Scan, &Inc, Some(1), &k).unwrap(),
        Keyed::Fresh(_)
    ));
    assert_eq!(pull(&x, &b), Pulled::Served);
    assert_eq!(pull(&x, &a), Pulled::Served);
    // M pulls X; the batch fails before its last commit.
    let theirs = x.version_vector().unwrap();
    let mine = m.witnessed_vector().unwrap();
    let start = mine.behind(&theirs).unwrap();
    let window = x.serve_entries_to_peer(start, 1024, Some(&mine), &[], None).unwrap();
    crate::sync::count_hooks::fail_next(crate::sync::count_hooks::Fail::BeforeLastCommit);
    let mut outcome = SyncOutcome::default();
    let failed = m.apply_peer_window_carrying_into(
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
    );
    assert!(failed.is_err(), "premise: the batch failed before its last commit");
    assert_eq!(n_of(&m, &cm, 1), 11, "premise: M's first run committed B's application");
    // X and A go down; M hears only B.
    assert_eq!(pull(&m, &b), Pulled::Served);
    retry_must_replay(&m, &cm, &k, 1, true);
}

// -- A sender that purged a drop, records outliving it ---------------------------------------

/// One window of at most `limit` entries, through the verdict, as `pull` serves it.
fn pull_limited(to: &Engine, from: &Engine, limit: usize) {
    let theirs = from.version_vector().unwrap();
    let mine = to.witnessed_vector().unwrap();
    assert_eq!(from.serve_verdict(&mine).unwrap(), ServeVerdict::Serve);
    let Some(start) = mine.behind(&theirs) else { return };
    let marked = to.held_ranges_covered_by(&mine).unwrap();
    let window = from.serve_entries_to_peer(start, limit, Some(&mine), &marked, None).unwrap();
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
}

/// A purged sender relays a dropped life. A sender that has applied a
/// collection's drop and run its purge still serves the dropped life's entries
/// from its oplog. A member behind the drop takes the keyed `$inc`'s entry,
/// and, records outliving a drop, the record with it, since the purge leaves
/// records: a retry there replays. Before, the purge had removed the record,
/// the entry came bare, and the retry applied on top of its own effect, a
/// version stamped after the drop that the next life keeps (ADR-236).
#[test]
fn d1_a_purged_senders_old_life_entry_reaches_a_member_behind_the_drop_with_its_record() {
    let dir = tempfile::tempdir().unwrap();
    let (a, m) = (open(&dir, "a"), open(&dir, "m"));
    let ca = a.create_collection("shop", "orders").unwrap();
    a.insert(&ca, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    assert_eq!(pull(&m, &a), Pulled::Served);
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = key();
    assert!(matches!(
        a.modify_where_keyed(&ca, &Candidates::Scan, &Inc, Some(1), &k).unwrap(),
        Keyed::Fresh(_)
    ));
    assert!(a.drop_collection("shop", "orders").unwrap());
    a.finish_purges_now().unwrap();
    // One entry: the `$inc`'s, ahead of the drop.
    pull_limited(&m, &a, 1);
    assert_eq!(n_of(&m, &cm, 1), 1, "premise: M took the `$inc` and not yet the drop");
    retry_must_replay(&m, &cm, &k, 1, true);
}
