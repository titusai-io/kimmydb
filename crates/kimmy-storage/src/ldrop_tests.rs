//! L-DROP, the receiver half (ADR-272 decision 4, ADR-169 widened): a held mark that the servable vector covers is dropped when its
//! entry is superseded at its key (ADR-169's 2026-11-12 amendment) OR is a document entry addressed to a physical that is not alive on
//! this member. One predicate, `engine::physical_is_alive`, the same question the sender will ask of the entries it serves.
//!
//! Why: a member that holds an entry as state (a snapshot's page, a repair) for a physical that is later dropped or buried here would
//! keep the mark, ask its peers for the entry in every pull until retention collects it (ADR-172), and the vector would vouch for an
//! entry the member does not serve. The entry is history on this member: its effect is gone with the physical and nothing is owed.
//! It is NOT the late-write predicate of decision 8: a write to a physical that is later buried is not late by being buried.

use bson::doc;
use kimmy_core::{Hlc, OplogEntry, VersionVector};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope};

const DB: &str = "shop";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

/// `entries` as one window served contiguously from the receiver's own position.
fn window(b: &Engine, entries: &[OplogEntry]) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().unwrap();
    let mut outcome = SyncOutcome::default();
    b.apply_peer_window_into(
        &theirs,
        Some(&mine),
        entries,
        PeerWindow {
            scanned_to: entries.last().map_or(Hlc::ZERO, |e| e.stamp.hlc),
            exhausted: true,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )
    .unwrap();
    outcome
}

/// A makes `orders` and `other`, writes one document to `orders` (the entry C will hold as state) and then one to `other` (the
/// entry that, applied in a window, raises C's servable vector over the first). C holds `orders`' entry under Hold.
struct Case {
    c: Engine,
    _dc: tempfile::TempDir,
    _da: tempfile::TempDir,
    held: OplogEntry,
    cover: OplogEntry,
}

fn case() -> Case {
    let (a, da) = engine();
    let (c, dc) = engine();
    a.create_collection(DB, "orders").unwrap();
    a.create_collection(DB, "other").unwrap();
    let creations = everything(&a);
    c.apply_batch(&creations).unwrap();
    let orders = a.get_collection(DB, "orders").unwrap();
    let other = a.get_collection(DB, "other").unwrap();
    later();
    a.insert(&orders, doc! { "_id": 1, "v": 1 }).unwrap();
    let held = everything(&a).into_iter().last().unwrap();
    later();
    a.insert(&other, doc! { "_id": 1 }).unwrap();
    let cover = everything(&a).into_iter().last().unwrap();
    let coll_c = c.get_collection(DB, "orders").unwrap();
    c.apply_held_for_test(&coll_c, &held).unwrap();
    assert_eq!(c.held_len().unwrap(), 1, "premise: C holds the entry as state");
    Case { c, _dc: dc, _da: da, held, cover }
}

#[test]
fn a_covered_mark_of_a_document_entry_for_a_physical_dropped_here_is_dropped() {
    let k = case();
    k.c.drop_collection(DB, "orders").unwrap();
    assert_eq!(k.c.held_len().unwrap(), 1, "premise: the drop leaves the mark");
    window(&k.c, std::slice::from_ref(&k.cover));
    let servable = k.c.version_vector().unwrap().get(k.held.stamp.origin());
    assert!(servable >= k.held.stamp.hlc, "premise: the window's vector now covers the held entry");
    assert_eq!(k.c.held_len().unwrap(), 0, "its physical is not alive here: the mark is dropped");
}

#[test]
fn a_covered_mark_of_a_document_entry_for_a_physical_that_is_alive_is_kept() {
    let k = case();
    window(&k.c, std::slice::from_ref(&k.cover));
    let servable = k.c.version_vector().unwrap().get(k.held.stamp.origin());
    assert!(servable >= k.held.stamp.hlc, "premise: the window's vector covers the held entry");
    assert_eq!(
        k.c.held_len().unwrap(),
        1,
        "a genuine gap on a live physical: kept, and still asked for"
    );
}

#[test]
fn an_uncovered_mark_of_a_dropped_physical_is_kept() {
    // The sweep runs after a window; this window carries an entry of ANOTHER origin, so the servable vector for the held entry's
    // origin does not move and the mark stays uncovered: the coverage test comes first, and the dead physical alone does not drop it.
    let k = case();
    k.c.drop_collection(DB, "orders").unwrap();
    assert_eq!(k.c.held_len().unwrap(), 1);
    let (b, _db) = engine();
    b.create_collection(DB, "third").unwrap();
    let other = everything(&b);
    assert!(
        other.iter().all(|e| e.stamp.origin() != k.held.stamp.origin()),
        "premise: another origin"
    );
    window(&k.c, &other);
    let servable = k.c.version_vector().unwrap().get(k.held.stamp.origin());
    assert!(
        servable < k.held.stamp.hlc,
        "premise: the held entry is still above the servable vector"
    );
    assert_eq!(k.c.held_len().unwrap(), 1, "not covered by the vector: left alone");
}

#[test]
fn a_sender_keeps_serving_the_entries_of_a_collection_it_dropped() {
    // The ADR-169 amendment states the sender's omission as permitted, not done: the ordinary window of a dropped collection is
    // what 0.61.0 serves, and the serve walk is not changed.
    let (a, _da) = engine();
    a.create_collection(DB, "orders").unwrap();
    let orders = a.get_collection(DB, "orders").unwrap();
    later();
    a.insert(&orders, doc! { "_id": 1 }).unwrap();
    let insert = everything(&a).into_iter().last().unwrap();
    a.drop_collection(DB, "orders").unwrap();
    assert!(
        everything(&a).iter().any(|e| e.stamp == insert.stamp),
        "the document entry of the dropped collection is still in the window the sender serves"
    );
}

#[test]
fn the_receivers_sweep_calls_the_one_predicate_and_nothing_else_decides_the_drop() {
    // One predicate at both ends in the text (ADR-169 amendment); in the build only the receiver calls it, because the sender's
    // omission is permitted and not built. Counted over the non-test sources: the definition and one call, inside the sweep.
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut uses = Vec::new();
    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if path.extension().is_none_or(|e| e != "rs") || name.ends_with("_tests.rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        for (i, line) in text.lines().enumerate() {
            let code = line.trim_start();
            if !code.starts_with("//") && code.contains("physical_is_alive(") {
                uses.push(format!("{name}:{}", i + 1));
            }
        }
    }
    assert_eq!(uses.len(), 2, "the definition and the one call: {uses:?}");
    assert!(uses.iter().all(|u| u.starts_with("engine.rs:")), "{uses:?}");
    let engine = std::fs::read_to_string(src.join("engine.rs")).unwrap();
    let sweep_at = engine.find("pub(crate) fn drop_superseded_covered_marks").unwrap();
    let sweep_end = engine[sweep_at..].find("\n}\n").unwrap() + sweep_at;
    assert!(
        engine[sweep_at..sweep_end].contains("physical_is_alive("),
        "the sweep of covered marks asks the predicate"
    );
}
