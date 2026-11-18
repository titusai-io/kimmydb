//! Step 3, the frame check: what a peer says it has processed is believed up to the clock bound
//! and no further (`max_clock_skew_secs`, the same bound as an entry's).
//!
//! The receiver's clock is moved back (`set_clock_offset_for_tests`, per engine) so that a vector
//! made from the real clock is far-future to it, and forward again to make it due: no test waits.

use bson::doc;
use kimmy_core::{Epoch, Hlc, MergePolicy, NodeId, Origin, VersionVector};

use crate::{Engine, WalkScope, physical_now_ms};

const SKEW_MS: u64 = 300_000;

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

/// A donor that holds a field collection with one document: its entries, its vector, its origin.
struct Donor {
    entries: Vec<kimmy_core::OplogEntry>,
    theirs: VersionVector,
    origin: Origin,
    _engine: Engine,
    _dir: tempfile::TempDir,
}

fn donor() -> Donor {
    let (a, dir) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let entries = a.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    let theirs = a.version_vector().unwrap();
    let origin = a.origin();
    Donor { entries, theirs, origin, _engine: a, _dir: dir }
}

/// A window that carries nothing and says it reached the peer's end: the vector is the whole claim.
fn empty_window(b: &Engine, theirs: &VersionVector) {
    b.apply_peer_batch(theirs, &[], Hlc::ZERO, true).unwrap();
}

#[test]
fn a_window_claim_past_the_bound_is_believed_only_up_to_it() {
    // An origin whose epoch starts at zero, so that it is the claim that is past the bound and not the origin's
    // mint (an origin minted past the bound is left out, which the test below it covers).
    let origin = Origin { node: NodeId::generate(), epoch: Epoch::ZERO };
    let claimed = Hlc::new(physical_now_ms() + 3_600_000, 0);
    let mut theirs = VersionVector::new();
    theirs.insert(origin, claimed);
    let (b, _db) = engine();
    let before = b.clock_bound_now_ms() + SKEW_MS;

    empty_window(&b, &theirs);

    let after = b.clock_bound_now_ms() + SKEW_MS;
    let believed = b.witnessed_vector().unwrap().get(origin);
    assert!(
        believed.wall_ms >= before && believed.wall_ms <= after,
        "believed up to the bound ({before}..={after}) and not to the claim: {believed:?}"
    );
    assert!(believed < claimed, "less than the peer claimed");
}

#[test]
fn what_was_claimed_past_the_bound_is_believed_once_it_is_due() {
    let Donor { entries, theirs, origin, .. } = donor();
    let (b, _db) = engine();
    b.set_clock_offset_for_tests(-((SKEW_MS + 60_000) as i64));
    empty_window(&b, &theirs);
    assert!(b.witnessed_vector().unwrap().get(origin) < theirs.get(origin), "premise");

    b.set_clock_offset_for_tests(0);
    let scanned_to = entries.last().unwrap().stamp.hlc;
    b.apply_peer_batch(&theirs, &entries, scanned_to, true).unwrap();

    assert_eq!(
        b.witnessed_vector().unwrap().get(origin),
        theirs.get(origin),
        "the entries below the claim were not lost to it: they apply and the claim is whole"
    );
    assert!(b.get_collection("shop", "orders").is_ok(), "and the collection is here");
}

#[test]
fn a_claim_within_the_bound_is_believed_whole() {
    let Donor { theirs, origin, .. } = donor();
    let (b, _db) = engine();

    empty_window(&b, &theirs);

    assert_eq!(b.witnessed_vector().unwrap().get(origin), theirs.get(origin));
    assert!(b.clock_refusals().is_empty(), "nothing was refused: {:?}", b.clock_refusals());
}

#[test]
fn an_origin_minted_past_the_bound_is_not_claimed_at_all() {
    let Donor { mut theirs, .. } = donor();
    let (b, _db) = engine();
    let late = Origin {
        node: NodeId::generate(),
        epoch: Epoch::new(Hlc::new(b.clock_bound_now_ms() + 10 * SKEW_MS, 0), [4; 8]),
    };
    theirs.insert(late, Hlc::new(5, 0));

    empty_window(&b, &theirs);

    assert_eq!(b.witnessed_vector().unwrap().get(late), Hlc::ZERO, "left out");
}

#[test]
fn a_clamped_claim_is_counted_under_the_origin_that_made_it() {
    let Donor { theirs, origin, .. } = donor();
    let (b, _db) = engine();
    b.set_clock_offset_for_tests(-((SKEW_MS + 60_000) as i64));

    empty_window(&b, &theirs);
    empty_window(&b, &theirs);

    assert_eq!(
        b.clock_refusals(),
        vec![(origin.node.to_string(), 2)],
        "once per window that was clamped, named by the origin"
    );
}
