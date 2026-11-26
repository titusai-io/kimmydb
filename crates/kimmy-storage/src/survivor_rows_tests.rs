//! Unit P's survivor rows s1 to s4 and the shadow row (the tester, Thu Nov 26 2026, against 30f3fc04; P6 of P-plan.md).
//!
//! WHAT THE ROWS ARE. Build-order "For the tester", item 1 (added Wed Nov 25, 10:55 PM CST; ruled in FINDING-write-after-a-drop-is-judged-on-arrival.md): a plain collection STANDS when anything of it is stamped above its
//! floor (a creation, a write, a delete, a definition), the same on every member in either order of delivery, and what the drop removed does not come back.
//!   s1: a drop, then a write stamped above it by a member that had not heard, no creation after: the collection stands on every member with that one document.
//!   s2: the same with a DELETE in place of the write.   s3: the same with a DEFINITION (an index created after the drop).
//!   s4: a write stamped AT OR BELOW the drop that arrives after it is history: the collection stays gone.
//!   shadow: a chunk stamped above a GONE vector shadow's floor is NOT remade by P (decided by F2's rule 2): the shadow stays gone. THIS ROW FLIPS AT F2 (ruling B2, 12:25 PM): it is a guard through P and is inverted, not deleted, when F2 gives the shadow its own remake.
//! s1 to s4 were written on Nov 25 for the u14 harness (pending-harness-rows-tests.rs, ran once as u14-harness-1 in wt-q6t: s1, s2, s3, s5 red in the DROP-FIRST order, s4 green). This file is their P form: every row is split by the order of
//! delivery (so a red set names the order, and P8's deletion of the `created` readers cannot flip a write-first guard unseen), each with premises, and module-local helpers. NOT compiled, NOT run, NOT queued; install as
//! crates/kimmy-storage/src/survivor_rows_tests.rs with `#[cfg(test)] mod survivor_rows_tests;`.
//!
//! s5 IS NOT HERE, ON PURPOSE. Its harness shape (a member that never held the collection, holding only the tombstone, takes the write) has no options row for the remake, so it could never go green: N keeps the base options, and a member that
//! holds none finds them by the fetch. Its two halves are rows already: row3 (the n-api file: the row is held, the remake is dated above the floor) and w1a/w1b (the pending-write file: the member holds none, the fetch answers with the N row and both floors).
//!
//! EXPECTED AT P6 (30f3fc04, reads, not runs) AND THE P7 FIX THAT CHANGES IT. The remake is P7: the gone arm of `judge_document` (sync.rs:2225-2240) reads BOTH tombstones, and a fact above the later of them, with an N row for the id and no swap history, makes the
//! collection from the row's options (`document`, `field`: the row's), dated just above the floor, and applies the fact; the same for a deleted document and a `CreateIndex` (and a `ConfigureValidator`), not for a `ConfigureVectors`.
//! - RED, the drop-first order (the drop is applied, then a fact above it meets the gone collection and is lost today): s1 (write), s2 (delete), s3 (definition). GREEN at P7. The write-first order is the ADR-231/235 survivors path and is GREEN today: the guards below must STAY green through P7 and P8.
//! - GREEN guards: s4 in both orders (a fact at or below the floor is history), and the shadow row. They must stay green through P7 (mutants: a fact at or below the floor remakes; a shadow remade by its zero row) and P8.
//!
//! NOTE s1: P-plan.md maps s1 to the plain-life `one_insert_...` row. This s1 is the plain three-member shape of the same rule and goes green at P7 (the remake), where `one_insert_...` goes green at P8 (its creations and drops need the deleted `created` readers).
//!
//! DEPENDS ON THE COMMIT PLAN'S RULINGS (the manager will say if one goes the other way):
//! - ruling 3 (which facts remake): s2 and s3 assume a delete and a `CreateIndex` remake. If a definition does not, s3 is a guard (stays gone) instead of a red.
//! - ruling 2 (shadows are not remade in P): the shadow row assumes it. If P remakes a shadow, the row is inverted.
//! - ruling 4 (dated above the floor) and 5 (`is_history`): no row here reads either; row3 does.
//!
//! The mutants these rows are for: P7 "reading the collection's own tombstone only" (the database legs are the n-api and plain-life files'), "a write at or below the floor remakes" (s4), "a shadow remade by its zero row" (the shadow row),
//! "the remake without the swap-history gate" (row1a, row1b, not here).

use bson::doc;
use kimmy_core::{
    CollectionId, DocId, Hlc, IndexField, OpKind, OplogEntry, VectorRecord, VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope};

const DB: &str = "app";
const SHADOW: &str = "orders.__vectors";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

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

/// A stamp taken after the drop's, whatever the wall clock's resolution.
fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn id_of(name: &str) -> CollectionId {
    CollectionId::derive(DB, name)
}

/// The entries `m` minted itself of one kind for one collection.
fn own(m: &Engine, kind: OpKind, name: &str) -> Vec<OplogEntry> {
    everything(m)
        .into_iter()
        .filter(|e| e.kind == kind && e.collection == id_of(name) && e.stamp.node == m.node_id())
        .collect()
}

/// What a member holds of `name`: whether it stands and how many documents.
fn holds(m: &Engine, name: &str) -> (bool, u64) {
    match m.get_collection(DB, name) {
        Ok(c) => (true, m.count(&c, WalkScope::Background).unwrap()),
        Err(_) => (false, 0),
    }
}

/// A member holding `orders` with one document (`_id` 1, "old").
fn with_orders() -> (Engine, tempfile::TempDir) {
    let (m, d) = engine();
    let c = m.create_collection(DB, "orders").unwrap();
    m.insert(&c, doc! { "_id": 1, "v": "old" }).unwrap();
    (m, d)
}

/// The sender `s` and three members (a, b, c) that took everything of it: a drops, b has not heard, c is the member the row is about.
fn four() -> [(Engine, tempfile::TempDir); 4] {
    let (s, ds) = with_orders();
    let (a, da) = engine();
    let (b, db) = engine();
    let (c, dc) = engine();
    for m in [&a, &b, &c] {
        window(m, &everything(&s));
        assert_eq!(
            holds(m, "orders"),
            (true, 1),
            "premise: the member took the collection and its document"
        );
    }
    [(s, ds), (a, da), (b, db), (c, dc)]
}

// ---- s1, s2, s3: a fact above the drop keeps the collection -----------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Touch {
    Write,
    Delete,
    Definition,
}

/// `a` drops `orders` (D) and purges it; `b`, which has not heard of the drop, touches the collection stamped above D (a write, a delete of document 1, or an index). TWO members that took everything of the sender take D and the one entry: the first in the order
/// the drop first, the second the fact first. Returns (the drop first, the fact first), of ONE world, so that their stamps are the same.
fn survivor_both(touch: Touch) -> [(Engine, tempfile::TempDir); 2] {
    let [(s, _ds), (a, _da), (b, _db), (c, dc)] = four();
    let (c2, dc2) = engine();
    window(&c2, &everything(&s));
    assert_eq!(
        holds(&c2, "orders"),
        (true, 1),
        "premise: the second member took the collection and its document"
    );
    assert!(a.drop_collection(DB, "orders").unwrap());
    a.purge_dropped_collection(id_of("orders")).unwrap();
    let d = own(&a, OpKind::DropCollection, "orders").pop().expect("the drop");
    pause();
    let meta = b.get_collection(DB, "orders").unwrap();
    let above_the_drop = |e: &OplogEntry| e.stamp.node == b.node_id() && e.stamp.hlc > d.stamp.hlc;
    let touched: Vec<OplogEntry> = match touch {
        Touch::Write => {
            b.insert(&meta, doc! { "_id": 7, "v": "after the drop" }).unwrap();
            everything(&b)
                .into_iter()
                .filter(|e| {
                    above_the_drop(e) && e.doc_id.is_some() && e.collection == id_of("orders")
                })
                .collect()
        }
        Touch::Delete => {
            assert!(b.delete(&meta, &DocId::Int64(1)).unwrap());
            everything(&b)
                .into_iter()
                .filter(|e| {
                    above_the_drop(e) && e.doc_id.is_some() && e.collection == id_of("orders")
                })
                .collect()
        }
        Touch::Definition => {
            b.create_index(DB, "orders", vec![IndexField::ascending("v")], false, Some("x".into()))
                .unwrap();
            everything(&b)
                .into_iter()
                .filter(|e| {
                    above_the_drop(e)
                        && e.kind == OpKind::CreateIndex
                        && e.collection == id_of("orders")
                })
                .collect()
        }
    };
    assert_eq!(touched.len(), 1, "premise: one entry of {touch:?} stamped above the drop");
    let take_the_drop = |m: &Engine| {
        window(m, std::slice::from_ref(&d));
        let _ = m.purge_dropped_collection(id_of("orders"));
    };
    take_the_drop(&c);
    assert_eq!(holds(&c, "orders"), (false, 0), "premise: the drop took the collection");
    window(&c, &touched);
    window(&c2, &touched);
    assert!(holds(&c2, "orders").0, "premise: the {touch:?} applied to the standing collection");
    take_the_drop(&c2);
    [(c, dc), (c2, dc2)]
}

/// One of the two members of `survivor_both`.
fn survivor(touch: Touch, drop_first: bool) -> (Engine, tempfile::TempDir) {
    let [first, second] = survivor_both(touch);
    if drop_first { first } else { second }
}

/// The row: `c` stands holding `documents` documents (and, for a definition, the index), whatever the order.
fn the_survivor_row(touch: Touch, drop_first: bool, documents: u64) {
    let (c, _dc) = survivor(touch, drop_first);
    let order = if drop_first { "the drop first" } else { "the fact first" };
    assert_eq!(
        holds(&c, "orders"),
        (true, documents),
        "{touch:?}, {order}: the collection stands on what is stamped above the drop and nothing the drop removed comes back"
    );
    if let Touch::Definition = touch {
        let meta = c.get_collection(DB, "orders").unwrap();
        assert!(
            meta.index("x").is_some(),
            "{touch:?}, {order}: the index created above the drop is held"
        );
    }
}

/// s1, the drop first. RED today: the write meets a gone collection (its tombstone is D) and is lost. GREEN at P7 (the remake from N's row, dated above D, applies the write; document 1, below D, does not come back).
#[test]
fn s1_a_write_above_a_drop_keeps_the_collection_with_that_one_document_when_the_drop_arrives_first()
{
    the_survivor_row(Touch::Write, true, 1);
}

/// s1, the write first: the survivors path (ADR-231/235) keeps it today. A GUARD through P7 and P8.
#[test]
fn s1_a_write_above_a_drop_keeps_the_collection_with_that_one_document_when_the_write_arrives_first()
 {
    the_survivor_row(Touch::Write, false, 1);
}

/// s2, the drop first. RED today, GREEN at P7 (the same remake for a deleted document: the collection stands with the delete applied, no document). Depends on ruling 3.
#[test]
fn s2_a_delete_above_a_drop_keeps_the_collection_when_the_drop_arrives_first() {
    the_survivor_row(Touch::Delete, true, 0);
}

/// s2, the delete first. A GUARD through P7 and P8.
#[test]
fn s2_a_delete_above_a_drop_keeps_the_collection_when_the_delete_arrives_first() {
    the_survivor_row(Touch::Delete, false, 0);
}

/// s3, the drop first. RED today (the index creation meets a gone collection: `Unknown`, or history by `defines_a_later_life`, sync.rs:1810), GREEN at P7 (the remake applies the definition). Depends on ruling 3.
#[test]
fn s3_an_index_created_above_a_drop_keeps_the_collection_when_the_drop_arrives_first() {
    the_survivor_row(Touch::Definition, true, 0);
}

/// s3, the index first. A GUARD through P7 and P8 (a definition made after the drop is not the ended life's: the drop keeps it).
#[test]
fn s3_an_index_created_above_a_drop_keeps_the_collection_when_the_index_arrives_first() {
    the_survivor_row(Touch::Definition, false, 0);
}

// ---- s4: a fact at or below the drop is history ----------------------------------------------------------------------------------------------

/// `b`, which has not heard of the drop, writes document 7 BELOW D (the drop is stamped after the write); `a` drops. `c` takes D and the write in the given order and ends without the collection, whatever the order. GUARDS (green today): they
/// must stay green through P7 (mutant: a write at or below the floor remakes) and P8 (mutant: the standing life keeps a write below the drop).
fn s4(drop_first: bool) {
    let [(_s, _ds), (a, _da), (b, _db), (c, _dc)] = four();
    let meta = b.get_collection(DB, "orders").unwrap();
    b.insert(&meta, doc! { "_id": 7, "v": "before the drop" }).unwrap();
    pause();
    assert!(a.drop_collection(DB, "orders").unwrap());
    a.purge_dropped_collection(id_of("orders")).unwrap();
    let d = own(&a, OpKind::DropCollection, "orders").pop().expect("the drop");
    let below: Vec<OplogEntry> = everything(&b)
        .into_iter()
        .filter(|e| {
            e.stamp.node == b.node_id()
                && e.doc_id.is_some()
                && e.collection == id_of("orders")
                && e.stamp.hlc < d.stamp.hlc
        })
        .collect();
    assert_eq!(below.len(), 1, "premise: one write stamped below the drop");
    let take_the_drop = |m: &Engine| {
        window(m, std::slice::from_ref(&d));
        let _ = m.purge_dropped_collection(id_of("orders"));
    };
    if drop_first {
        take_the_drop(&c);
        assert_eq!(holds(&c, "orders"), (false, 0), "premise: the drop took the collection");
        window(&c, &below);
    } else {
        window(&c, &below);
        assert_eq!(
            holds(&c, "orders"),
            (true, 2),
            "premise: the write applied to the standing collection"
        );
        take_the_drop(&c);
    }
    let order = if drop_first { "the drop first" } else { "the write first" };
    assert_eq!(
        holds(&c, "orders"),
        (false, 0),
        "s4, {order}: a write stamped below the drop is history, and the collection stays gone"
    );
}

#[test]
fn s4_a_write_below_a_drop_is_history_when_the_drop_arrives_first() {
    s4(true);
}

#[test]
fn s4_a_write_below_a_drop_is_history_when_the_write_arrives_first() {
    s4(false);
}

// ---- the shadow row ----------------------------------------------------------------------------------------------------------------------------

fn vector_config() -> kimmy_core::VectorConfig {
    kimmy_core::VectorConfig {
        fields: vec!["text".into()],
        provider: kimmy_core::ProviderConfig::Byo {},
        dim: 4,
        metric: Default::default(),
        document_prefix: None,
        query_prefix: None,
        chunk: Default::default(),
    }
}

fn chunk_of(source: i64) -> VectorRecord {
    VectorRecord {
        source: DocId::Int64(source),
        chunk: 0,
        source_hlc: Hlc::new(1, 0),
        vector: vec![1.0, 0.0, 0.0, 0.0],
        text: "old".into(),
        config: None,
    }
}

/// FLIPS AT F2 (ruling B2: P remakes NO shadow; the shadow's remake is F2's rule 2). A chunk stamped above a GONE vector shadow's floor is NOT remade by P: the shadow stays gone, and so does its parent (the chunk is a fact of the shadow, which has no facts of its own; a configuration makes a shadow). DEPENDS ON RULING 2 (shadows are not remade
/// in P; the 12:26 AM ruling gives a shadow's remake to F2's rule 2): if P remakes a shadow this row is inverted. A GUARD, green today and through P7 (mutant: a shadow remade by its zero row, which is at or below every floor and keeps nothing standing by itself).
/// Only the drop-first order is asserted: what the write-first order does to a chunk on a shadow dropped with its parent is the survivors path's, which this row does not claim.
#[test]
fn shadow_a_chunk_above_a_gone_shadows_floor_is_not_remade_when_the_drop_arrives_first() {
    let (s, _ds) = engine();
    let orders = s.create_collection(DB, "orders").unwrap();
    s.insert(&orders, doc! { "_id": 1, "text": "old" }).unwrap();
    s.configure_vectors(DB, "orders", vector_config()).expect("vectors are configured");
    let shadow = s.vector_collection(DB, "orders").unwrap().expect("the shadow exists");
    s.put_vectors(&shadow, &DocId::Int64(1), &[chunk_of(1)]).expect("the shadow takes a chunk");
    let (a, _da) = engine();
    let (b, _db) = engine();
    let (c, _dc) = engine();
    for m in [&a, &b, &c] {
        window(m, &everything(&s));
        assert!(m.get_collection(DB, SHADOW).is_ok(), "premise: the member holds the shadow");
    }
    assert!(a.drop_collection(DB, "orders").unwrap());
    let d = own(&a, OpKind::DropCollection, "orders").pop().expect("the drop of the parent");
    pause();
    // b has not heard of the drop: it writes a chunk into its shadow, stamped above D
    let shadow_b = b.get_collection(DB, SHADOW).unwrap();
    b.put_vectors(&shadow_b, &DocId::Int64(2), &[chunk_of(2)]).expect("the chunk is written");
    let chunk: Vec<OplogEntry> = everything(&b)
        .into_iter()
        .filter(|e| {
            e.stamp.node == b.node_id()
                && e.collection == id_of(SHADOW)
                && e.stamp.hlc > d.stamp.hlc
        })
        .collect();
    assert!(!chunk.is_empty(), "premise: b minted an entry into the shadow stamped above the drop");
    // c takes the drop, which takes the parent and its shadow, and then the chunk
    window(&c, std::slice::from_ref(&d));
    let _ = c.purge_dropped_collection(id_of("orders"));
    let _ = c.purge_dropped_collection(id_of(SHADOW));
    assert!(c.get_collection(DB, "orders").is_err(), "premise: the parent is gone");
    assert!(c.get_collection(DB, SHADOW).is_err(), "premise: the shadow went with its parent");
    window(&c, &chunk);
    assert!(
        c.get_collection(DB, SHADOW).is_err(),
        "a chunk above a gone shadow's floor does not make the shadow again in P"
    );
    assert!(c.get_collection(DB, "orders").is_err(), "and does not make its parent");
}

// ---- the Redated marker row (hardproblems' 1:10 PM ruling) --------------------------------------------------------------------------------------------

/// s1 in BOTH orders ends with the same `is_redated()` and the same own tombstone: the write-first order is the survivors path (a drop that met a write stamped after it re-dates the life and records the marker), the drop-first order is the remake (dated the floor's successor).
/// The marker is a property of the life, so the two orders must agree on it, whichever value the ruling gives it (the coder reads whether the remake sets it); the own tombstone is the drop's in both. Both members must stand (the row compares the two lives, so a member that lost the
/// collection fails on `stands`). RED today (the drop-first member loses the collection), GREEN at P7; if it is red at P7 on the marker alone, that is the finding the ruling is about.
#[test]
fn s1_both_orders_end_with_the_same_redated_marker_and_the_same_own_tombstone() {
    let [(first, _d1), (second, _d2)] = survivor_both(Touch::Write);
    let life = |m: &Engine| {
        let meta = m.get_collection(DB, "orders").ok();
        (
            meta.is_some(),
            meta.map(|c| c.is_redated()),
            m.collection_dropped_at(id_of("orders")).unwrap(),
        )
    };
    let (a, b) = (life(&first), life(&second));
    assert!(a.0 && b.0, "both members stand: the drop first {a:?}, the write first {b:?}");
    assert_eq!(
        a, b,
        "the two orders end with the same life: the same marker and the same own tombstone"
    );
}
