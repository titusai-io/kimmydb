//! Step 3, the frame check: what a peer says it has processed is believed up to the clock bound
//! and no further (`max_clock_skew_secs`, the same bound as an entry's).
//!
//! The receiver's clock is moved back (`set_clock_offset_for_tests`, per engine) so that a vector
//! made from the real clock is far-future to it, and forward again to make it due: no test waits.

use bson::doc;
use kimmy_core::{DocId, Epoch, Hlc, MergePolicy, NodeId, Origin, Stamp, VersionVector};

use crate::{Engine, SnapshotProgress, WalkScope, physical_now_ms};

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
    engine: Engine,
    _dir: tempfile::TempDir,
}

fn donor() -> Donor {
    let (a, dir) = engine();
    let ca = a.create_collection_with_policy("shop", "orders", MergePolicy::Field, None).unwrap();
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let entries = a.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    let theirs = a.version_vector().unwrap();
    let origin = a.origin();
    Donor { entries, theirs, origin, engine: a, _dir: dir }
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

/// A receiver that holds the donor's collection (it took the creation), and the donor's whole-database page.
fn a_page_for(donor: &Donor, b: &Engine) -> crate::SnapshotPage {
    b.apply_batch(&donor.entries).unwrap();
    donor.engine.snapshot_page(None, None).unwrap()
}

#[test]
fn a_snapshot_grant_past_the_bound_is_believed_only_up_to_it() {
    let d = donor();
    let (b, _db) = engine();
    let mut page = a_page_for(&d, &b);
    let far = Hlc::new(physical_now_ms() + 3_600_000, 0);
    page.versions.insert(d.origin, far);

    b.apply_snapshot_page(d.engine.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .unwrap();

    let limit = b.clock_bound_now_ms() + SKEW_MS;
    for (what, vector) in
        [("witnessed", b.witnessed_vector().unwrap()), ("servable", b.version_vector().unwrap())]
    {
        let believed = vector.get(d.origin);
        assert!(
            believed.wall_ms <= limit && believed < far,
            "the {what} vector took the grant of an hour ahead as it stands: {believed:?}"
        );
    }
}

#[test]
fn a_snapshot_horizon_ahead_of_this_clock_is_adopted_only_up_to_it() {
    let d = donor();
    let (b, _db) = engine();
    let mut page = a_page_for(&d, &b);
    assert!(page.next.is_none(), "premise: one page, so it completes and the horizon is adopted");
    let far = Hlc::new(physical_now_ms() + 3_600_000, 0);
    page.tombstone_horizon = Some(far);

    b.apply_snapshot_page(d.engine.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .unwrap();

    let horizon = b.tombstone_horizon().unwrap().unwrap();
    assert!(horizon.wall_ms <= b.clock_bound_now_ms(), "adopted as it stands: {horizon:?}");
    assert_eq!(
        b.clock_refusals(),
        vec![(d.engine.node_id().to_string(), 1)],
        "counted, under the sender"
    );
}

#[test]
fn a_horizon_within_this_clock_is_adopted_whole() {
    let d = donor();
    let (b, _db) = engine();
    let own = b.tombstone_horizon().unwrap().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let mut page = a_page_for(&d, &b);
    let behind = Hlc::new(own.wall_ms + 1, 7);
    page.tombstone_horizon = Some(behind);

    b.apply_snapshot_page(d.engine.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .unwrap();

    assert_eq!(b.tombstone_horizon().unwrap().unwrap(), behind);
    assert!(b.clock_refusals().is_empty(), "nothing was refused: {:?}", b.clock_refusals());
}

/// The young-stamp rule reads the sender's horizon as it was sent. Adoption lowers a horizon ahead of this clock to the
/// clock, which is the safe direction there (tombstones are kept longer); for the young rule a lower value is the unsafe
/// one (more rows count as young and a row whose delete the sender collected is written back), so it must not see it.
#[test]
fn the_young_rule_reads_a_senders_horizon_ahead_of_this_clock_as_it_was_sent() {
    let (a, _da) = engine();
    let (b, _db) = engine();
    let ca =
        a.create_collection_with_policy("shop", "orders", MergePolicy::Document, None).unwrap();
    let creation = a.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries;
    b.apply_batch(&creation).unwrap();
    let cb = b.get_collection("shop", "orders").unwrap();
    // A row stamped inside the bound and ahead of this member's clock, below the horizon the sender names.
    a.advance_clock_past(&Stamp::new(
        Hlc::new(physical_now_ms() + 30_000, 0),
        a.node_id(),
        a.epoch(),
    ));
    a.insert(&ca, doc! { "_id": 1 }).unwrap();
    let mut page = a.snapshot_page(None, Some(ca.id)).unwrap();
    page.tombstone_horizon = Some(Hlc::new(physical_now_ms() + 3_600_000, 0));
    let mut progress = SnapshotProgress::of_collection_converging(ca.id, VersionVector::new());

    let applied = b.apply_snapshot_page(a.node_id(), &mut progress, &page).unwrap();

    assert_eq!(
        applied.created, 0,
        "a row at or below the sender's horizon is not young, whatever this member's clock reads: {applied:?}"
    );
    assert_eq!(b.get(&cb, &DocId::Int64(1)).unwrap(), None, "and it was not written");
}

#[test]
fn a_drop_past_the_bound_that_the_pages_own_vector_names_is_not_believed() {
    let d = donor();
    let (b, _db) = engine();
    let mut page = a_page_for(&d, &b);
    let victim = b.get_collection("shop", "orders").unwrap();
    let far = Stamp::new(
        Hlc::new(physical_now_ms() + 3_600_000, 0),
        d.engine.node_id(),
        d.engine.epoch(),
    );
    page.dropped_collections = vec![(victim.id, far)];
    page.versions.insert(far.origin(), far.hlc);

    b.apply_snapshot_page(d.engine.node_id(), &mut SnapshotProgress::whole_database(), &page)
        .unwrap();

    assert!(
        b.get_collection("shop", "orders").is_ok(),
        "the denial was believed and took the collection"
    );
    assert!(
        b.collections_dropped().unwrap().is_empty(),
        "a tombstone was recorded at the far stamp"
    );
    assert!(b.clock_last() < far.hlc, "and the clock moved to it");
}
