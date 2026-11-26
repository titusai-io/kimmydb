//! Unit P, P8 (i): the keep test of a drop reads the options row of the id. A standing life that a drop would bury (nothing of it is stamped above
//! the drop) is KEPT when the row's `at` is above the drop: a creation of the name stands above the drop, so the life is the CREATION's, dated by
//! it, and not the remains of the life the drop ended (ruled 3:33 PM). It carries no Redated marker, the drop's floor is taken, and what the drop
//! ended of it is swept.

use bson::doc;
use kimmy_core::{CollectionId, Hlc, OpKind, OplogEntry, VersionVector};

use crate::{Engine, PeerWindow, WalkScope};

const DB: &str = "app";

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

fn window(b: &Engine, entries: &[OplogEntry]) {
    let mut theirs = VersionVector::default();
    for entry in entries {
        theirs.observe(entry.stamp);
    }
    let mine = b.witnessed_vector().unwrap();
    let mut outcome = crate::SyncOutcome::default();
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
}

fn pause() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn id() -> CollectionId {
    CollectionId::derive(DB, "orders")
}

/// A member holding the first life (c2, document 1) takes a creation of the name made AFTER the drop (c4, from a member that never held the
/// first life), and then the drop (d3, between the two creations, from a member that held the first life only). The life it holds is the
/// creation's: it stands, empty, dated by c4's stamp, with no marker and the drop as its floor.
/// Mutants (P8): the keep test without the options row (the life is buried and nothing remakes it); a life kept for its row is dated as the
/// remains of the life the drop ended (the drop's successor, marked).
#[test]
fn a_life_whose_row_is_above_the_drop_is_the_creations_life_dated_by_it_and_unmarked() {
    let (x, _dx) = engine();
    let first = x.create_collection(DB, "orders").unwrap();
    x.insert(&first, doc! { "_id": 1, "v": "old" }).unwrap();
    let (z, _dz) = engine();
    window(&z, &everything(&x));
    pause();
    assert!(z.drop_collection(DB, "orders").unwrap());
    let d3 = everything(&z)
        .into_iter()
        .find(|e| e.kind == OpKind::DropCollection && e.collection == id())
        .expect("the drop");
    pause();
    let (y, _dy) = engine();
    y.create_collection(DB, "orders").unwrap();
    let c4 = everything(&y)
        .into_iter()
        .find(|e| e.kind == OpKind::CreateCollection && e.collection == id())
        .expect("the later creation");
    assert!(d3.stamp.hlc < c4.stamp.hlc, "premise: the drop is below the later creation");

    let (m, _dm) = engine();
    window(&m, &everything(&x));
    assert_eq!(
        m.count(&m.get_collection(DB, "orders").unwrap(), WalkScope::Background).unwrap(),
        1
    );
    window(&m, std::slice::from_ref(&c4));
    window(&m, std::slice::from_ref(&d3));

    let held = m.get_collection(DB, "orders").expect("the creation's life stands");
    assert_eq!(held.created, c4.stamp.hlc, "dated by the creation");
    assert!(!held.is_redated(), "and not marked as the remains of the life the drop ended");
    assert_eq!(held.incarnation_floor, Some(d3.stamp.hlc), "with the drop as its floor");
    assert_eq!(
        m.count(&held, WalkScope::Background).unwrap(),
        0,
        "what the drop ended of it is swept"
    );
}
