//! The idempotency model test (B5, design "Tests > Model test"), and its findings.
//!
//! **Finding G1**: a member that took a collection by a whole-database snapshot
//! relays the later versions it holds as state without the record of the request
//! whose effect they contain. A page's record is rowed at its `last`, and a window
//! carries only the rows at the stamps it ships; the relay's coverage then passes the
//! request's own entries, so the receiver never gets the record. A retry there applies
//! on top of the effect.

use std::cmp::Ordering;

use bson::{Bson, Document, doc};
use kimmy_core::{DocId, Fingerprint, ScopeHash};

use crate::idempotency::{Keyed, KeyedWrite, RecordsVia};
use crate::{Candidates, Engine, ModifySpec, PeerWindow, SnapshotProgress, SyncOutcome, WalkScope};

const BATCH: usize = 1024;

/// `$inc` of `n` by one.
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

/// A pull of everything `from` has for `to`, with its records, as the transport's
/// pull makes it: from the requester's witnessed vector.
fn pull_all(to: &Engine, from: &Engine) {
    for _ in 0..8 {
        let theirs = from.version_vector().unwrap();
        let mine = to.witnessed_vector().unwrap();
        let Some(start) = mine.behind(&theirs) else { return };
        let window = from
            .entries_for_peer_holding(start, BATCH, Some(&mine), WalkScope::Background)
            .unwrap();
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

/// Finding G1, minimal: Y applies a keyed `$inc` and then writes the counter again;
/// X takes the database by snapshot (the record with it, rowed at its `last`); M
/// pulls from X and is sent the later version without the record, and its coverage
/// then passes the `$inc`'s own entry, so Y never serves it the record either, Y
/// serving throughout. A retry on M applies the `$inc` a second time (n 11 -> 12).
/// Asserts guarantee 2 (a member holding any effect of the write holds its record),
/// so it fails until the design closes the gap.
#[test]
#[ignore = "finding G1: a snapshot-held version is relayed without its record (design change pending)"]
fn finding_g1_a_relay_of_a_snapshot_held_version_drops_the_record() {
    let dir = tempfile::tempdir().unwrap();
    let y = Engine::open(&dir.path().join("y.redb")).unwrap();
    let x = Engine::open(&dir.path().join("x.redb")).unwrap();
    let m = Engine::open(&dir.path().join("m.redb")).unwrap();
    let cy = y.create_collection("shop", "orders").unwrap();
    y.insert(&cy, doc! { "_id": 1i64, "n": 0i64 }).unwrap();
    pull_all(&m, &y);
    let cm = m.get_collection("shop", "orders").unwrap();
    let k = KeyedWrite::new(ScopeHash([1; 32]), Fingerprint([1; 32]));
    let first = y.modify_where_keyed(&cy, &Candidates::Scan, &Inc, Some(1), &k).unwrap();
    assert!(matches!(first, Keyed::Fresh(_)));
    // Another write on top: the version holds the `$inc`'s effect.
    y.replace(&cy, &DocId::Int64(1), doc! { "n": 11i64 }, false).unwrap();
    let mut progress = SnapshotProgress::whole_database();
    while !progress.is_complete() {
        let page = y.snapshot_page(progress.after().cloned(), progress.scope()).unwrap();
        x.apply_snapshot_page(y.node_id(), &mut progress, &page).unwrap();
    }
    assert!(x.idempotency_record(cy.id, &k.scope).unwrap().is_some(), "premise: X holds it");
    pull_all(&m, &x);
    pull_all(&m, &y);
    assert_eq!(m.get(&cm, &DocId::Int64(1)).unwrap().unwrap().get_i64("n").unwrap(), 11);
    let held = m.idempotency_record(cm.id, &k.scope).unwrap();
    let retry = m.modify_where_keyed(&cm, &Candidates::Scan, &Inc, Some(1), &k).unwrap();
    let n = m.get(&cm, &DocId::Int64(1)).unwrap().unwrap().get_i64("n").unwrap();
    assert!(
        held.is_some() && matches!(retry, Keyed::Replayed(_)) && n == 11,
        "M holds the `$inc`'s effect without its record ({held:?}), and a retry there applied \
         it again: {retry:?}, n = {n}"
    );
}
