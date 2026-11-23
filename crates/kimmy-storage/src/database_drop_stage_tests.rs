//! A replacing `$out`'s stage that meets a drop of its database (ADR-272, decisions 4 and 9; ADR-229, ADR-236).
//!
//! Two defects in the stage's life under a database drop, both reachable by ordinary concurrency (A starts a replacing `$out`, B drops
//! the database, C hears the drop first):
//!
//! 1a. A stage's creation stamped at or below the drop, arriving after it, was refused as history and wrote no creation row in the
//! register, so the author's binding was `UnknownStage` and the batch stopped at it for good: the creation never comes again. The
//! same held when the stage's own id already had a tombstone above the creation, which also returned without appending the entry.
//!
//! 1b. A stage alive when the drop arrived and holding a row stamped after it took the survivors path and kept its stage meta; the
//! author's valid binding then made a partial output visible, and divergence repair spread it. A stage is buried whole at the drop.

use bson::doc;
use kimmy_core::{
    CollectionBind, CollectionId, Hlc, MergePolicy, OpKind, OplogEntry, Stamp, VersionVector,
};

use crate::{Engine, PeerWindow, SyncOutcome, WalkScope, WriterHolder, physical_now_ms};

const DB: &str = "app";
const S1: &str = "orders.__stg.s1";

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

/// Long enough that the next stamp is on a later millisecond than any minted before it, so "after the drop" is a fact about stamps.
fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

fn creation_of(a: &Engine) -> OplogEntry {
    everything(a)
        .into_iter()
        .find(|e| {
            e.kind == OpKind::CreateCollection && e.collection == CollectionId::derive(DB, S1)
        })
        .expect("the stage's creation")
}

/// The author's binding of `orders` to the stage, stamped just above the creation: at or below the deadline, so valid.
fn bind_of(a: &Engine, wall_ms: u64) -> OplogEntry {
    OplogEntry {
        stamp: Stamp::new(Hlc::new(wall_ms, 0), a.node_id(), a.epoch()),
        kind: OpKind::BindCollection,
        collection: CollectionId::derive(DB, "orders"),
        doc_id: None,
        body: Some(
            bson::serialize_to_vec(&CollectionBind {
                db: DB.into(),
                name: "orders".into(),
                physical: S1.into(),
            })
            .unwrap(),
        ),
        meta: None,
    }
}

/// The creation row the register keeps for the stage, if any.
fn has_creation_row(e: &Engine) -> bool {
    let txn = e.begin_write(WriterHolder::Ddl).unwrap();
    let row = crate::register::stage_row(&txn, CollectionId::derive(DB, S1)).unwrap();
    txn.abort().unwrap();
    row.is_some()
}

fn name_is_absent(e: &Engine) -> bool {
    matches!(
        e.resolve(DB, "orders"),
        Err(crate::StorageError::Core(kimmy_core::Error::CollectionNotFound { .. }))
    )
}

fn held_at(e: &Engine, stamp: &Stamp) -> usize {
    everything(e).iter().filter(|x| x.stamp == *stamp).count()
}

/// The author of a stage and the member that drops the database after the stage's creation: `d` holds `orders` and never meets the stage.
struct Race {
    a: Engine,
    d: Engine,
    creation: OplogEntry,
    _dirs: [tempfile::TempDir; 2],
}

fn race() -> Race {
    let (a, da) = engine();
    let (d, dd) = engine();
    d.create_collection(DB, "orders").unwrap();
    a.create_stage(DB, S1, MergePolicy::Document, Hlc::new(physical_now_ms() + 60_000, 0)).unwrap();
    let creation = creation_of(&a);
    later();
    assert!(d.drop_database(DB).unwrap(), "premise: the database is dropped");
    Race { a, d, creation, _dirs: [da, dd] }
}

#[test]
fn a_stage_creation_older_than_the_database_drop_writes_its_row_and_does_not_stall_the_batch() {
    let r = race();
    let stage = r.a.get_collection(DB, S1).unwrap();
    r.a.insert(&stage, doc! { "_id": 9, "v": "staged" }).unwrap();
    let row = everything(&r.a).into_iter().find(|e| e.kind == OpKind::Insert).unwrap();
    let bind = bind_of(&r.a, row.stamp.hlc.wall_ms + 1);
    let (m, _dm) = engine();
    window(&m, &everything(&r.d)); // C hears the drop first
    assert!(m.database_dropped_at(DB).unwrap().is_some(), "premise: the database's drop is taken");
    let outcome = window(&m, &[r.creation.clone(), row, bind.clone()]);
    assert_eq!(
        outcome.unknown_collection, 0,
        "the binding is not an unknown stage: the creation was met, and the batch does not stop at it for good ({outcome:?})"
    );
    assert!(
        m.witnessed_vector().unwrap().get(bind.stamp.origin()) >= bind.stamp.hlc,
        "the window went past the binding"
    );
    assert!(
        has_creation_row(&m),
        "a stage's creation writes its row whenever it is taken, whether or not it makes the collection"
    );
    assert_eq!(
        held_at(&m, &r.creation.stamp),
        1,
        "and the creation is held and served onward, once"
    );
    assert!(
        m.get_collection(DB, S1).is_err(),
        "the stage is not made: the database's drop covers it"
    );
    assert!(name_is_absent(&m), "and the name stays absent");
}

#[test]
fn a_stage_creation_older_than_the_stages_own_tombstone_is_appended_and_writes_its_row() {
    let r = race();
    // `d` buried the stage's own id with an entry of its own: the drop of a collection, which is how a database's drop reaches a stage
    // another member holds (here `d` took the creation before dropping)
    let (d2, _dd2) = engine();
    window(&d2, std::slice::from_ref(&r.creation));
    later();
    assert!(d2.drop_database(DB).unwrap(), "premise: the database holding the stage is dropped");
    let drop_of_stage = everything(&d2)
        .into_iter()
        .find(|e| e.kind == OpKind::DropCollection && e.collection == CollectionId::derive(DB, S1))
        .expect("the drop of the stage, as the database's drop writes it");
    let (m, _dm) = engine();
    window(&m, std::slice::from_ref(&drop_of_stage));
    assert!(
        m.collection_dropped_at(CollectionId::derive(DB, S1)).unwrap().is_some(),
        "premise: the stage's own id has a tombstone above its creation"
    );
    window(&m, std::slice::from_ref(&r.creation));
    assert_eq!(
        held_at(&m, &r.creation.stamp),
        1,
        "the creation older than the stage's own tombstone is held and served onward, as one the database refuses is"
    );
    assert!(has_creation_row(&m), "and its row is written");
    assert!(m.get_collection(DB, S1).is_err(), "the stage is not made");
}

#[test]
fn a_stage_holding_a_row_stamped_after_the_drop_is_buried_whole_and_the_binding_makes_nothing_visible()
 {
    let r = race();
    later();
    let stage = r.a.get_collection(DB, S1).unwrap();
    r.a.insert(&stage, doc! { "_id": 9, "v": "staged after the drop" }).unwrap();
    let row = everything(&r.a).into_iter().find(|e| e.kind == OpKind::Insert).unwrap();
    let bind = bind_of(&r.a, row.stamp.hlc.wall_ms + 1);
    let (m, _dm) = engine();
    window(&m, &[r.creation.clone(), row.clone()]); // the stage is alive here and holds a row stamped after the drop
    assert!(m.get_collection(DB, S1).is_ok(), "premise: the stage is alive");
    window(&m, &everything(&r.d)); // and now the database's drop arrives
    assert!(
        m.get_collection(DB, S1).is_err(),
        "a stage never takes the survivors path: it is buried whole at the drop, not kept for the row stamped after it"
    );
    window(&m, std::slice::from_ref(&bind));
    assert!(
        name_is_absent(&m),
        "the author's valid binding makes nothing visible: no partial output"
    );
}

/// One direction of an anti-entropy round: `into` pulls what `from` holds that it lacks.
fn pull(into: &Engine, from: &Engine) {
    let theirs = from.version_vector().unwrap();
    if let Some(start) = into.version_vector().unwrap().behind(&theirs) {
        let entries = from.entries_for_peer(start, 1024, WalkScope::Background).unwrap().entries;
        into.apply_batch(&entries).unwrap();
    }
}

/// The exact order of hardproblems' 1b: the author's staging rows are stamped after the drop D, and its binding after the rows; the dropper
/// and a third member hear the drop first, a fourth holds the stage and its row when the drop reaches it and the binding arrives after
/// it. On the old code the member that held the row keeps a partial stage, the binding makes it visible, and the repair rounds spread it.
#[test]
fn members_converge_on_an_absent_name_after_a_stage_with_rows_after_the_drop_is_bound() {
    let r = race();
    later();
    let stage = r.a.get_collection(DB, S1).unwrap();
    r.a.insert(&stage, doc! { "_id": 9, "v": "staged after the drop" }).unwrap();
    let row = everything(&r.a).into_iter().find(|e| e.kind == OpKind::Insert).unwrap();
    let bind = bind_of(&r.a, row.stamp.hlc.wall_ms + 1);
    // C hears the drop first, then everything the author wrote
    let (c, _dc) = engine();
    window(&c, &everything(&r.d));
    window(&c, &[r.creation.clone(), row.clone(), bind.clone()]);
    // B holds the stage and its row when the drop reaches it, and the binding arrives after
    let (b, _db) = engine();
    window(&b, &[r.creation.clone(), row.clone()]);
    assert!(
        b.get_collection(DB, S1).is_ok(),
        "premise: the stage is alive on B, holding a row stamped after the drop"
    );
    window(&b, &everything(&r.d));
    window(&b, std::slice::from_ref(&bind));
    // anti-entropy among the three, twice over
    for _ in 0..2 {
        for (x, y) in [(&b, &c), (&c, &b), (&b, &r.d), (&r.d, &b), (&c, &r.d), (&r.d, &c)] {
            pull(x, y);
        }
    }
    for (who, member) in [
        ("B, which held the row first", &b),
        ("C, which heard the drop first", &c),
        ("D, the dropper", &r.d),
    ] {
        assert!(
            name_is_absent(member),
            "{who}: the name is absent after the repair rounds: the drop wins over the output, and no partial output spreads"
        );
        assert!(member.get_collection(DB, S1).is_err(), "{who}: no stage stands");
    }
}
