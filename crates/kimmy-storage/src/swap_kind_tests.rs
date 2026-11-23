//! The swap's entry kinds go through the clock bound (ADR-272, decision 14): `BindCollection` and `AbandonStage` are
//! schema changes, decided by `apply_ddl` whose first step is the bound; `OutLossRecord` is a record kind of its own whose
//! arm of the batch step has the bound first. Each, at exactly the bound ahead of this member's physical clock, is taken
//! and appended; one millisecond past it, it is refused until due by name, not applied and not witnessed.
//!
//! The clock the bound reads is held still (`freeze_clock_bound_for_tests`): a stamp exactly the bound ahead of a clock
//! that moves is a coin toss, and the boundary is what these tests are about.

use kimmy_core::{
    CollectionBind, CollectionId, DocId, Epoch, Hlc, MergePolicy, NodeId, OpKind, OplogEntry,
    OutLoss, StageAbandon, Stamp, VersionVector,
};

use crate::{ClockRefusal, Engine, PeerWindow, SyncOutcome, WalkScope, physical_now_ms};

fn engine() -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Engine::open(&dir.path().join("kimmy.redb")).unwrap(), dir)
}

fn everything(from: &Engine) -> Vec<OplogEntry> {
    from.entries_for_peer(Hlc::ZERO, 10_000, WalkScope::Background).unwrap().entries
}

const STAGE: &str = "orders.__stg.x";

/// An entry of each swap kind, stamped `wall_ms` by a member this one has never heard of.
fn the_swap_entries(wall_ms: u64) -> Vec<(&'static str, OplogEntry)> {
    let stamp = Stamp::new(Hlc::new(wall_ms, 0), NodeId::from_bytes([9; 16]), Epoch::ZERO);
    let entry = |kind, collection, doc_id, body: Vec<u8>| OplogEntry {
        stamp,
        kind,
        collection,
        doc_id,
        body: Some(body),
        meta: None,
    };
    vec![
        (
            "BindCollection",
            entry(
                OpKind::BindCollection,
                CollectionId::derive("shop", "orders"),
                None,
                bson::serialize_to_vec(&CollectionBind {
                    db: "shop".into(),
                    name: "orders".into(),
                    physical: STAGE.into(),
                })
                .unwrap(),
            ),
        ),
        (
            "AbandonStage",
            entry(
                OpKind::AbandonStage,
                CollectionId::derive("shop", STAGE),
                None,
                bson::serialize_to_vec(&StageAbandon { db: "shop".into(), physical: STAGE.into() })
                    .unwrap(),
            ),
        ),
        (
            "OutLossRecord",
            entry(
                OpKind::OutLossRecord,
                CollectionId::derive("shop", "orders"),
                Some(DocId::Int64(1)),
                bson::serialize_to_vec(&OutLoss {
                    db: "shop".into(),
                    name: "orders".into(),
                    loser: Stamp::new(
                        Hlc::new(wall_ms.saturating_sub(1_000), 0),
                        NodeId::from_bytes([8; 16]),
                        Epoch::ZERO,
                    ),
                    image: Some(vec![1, 2, 3]),
                    cut: false,
                })
                .unwrap(),
            ),
        ),
    ]
}

/// A member whose bound clock is held at the real time now, and that time.
fn member() -> (Engine, tempfile::TempDir, u64) {
    let (e, dir) = engine();
    let now = physical_now_ms();
    e.freeze_clock_bound_for_tests(Some(now));
    (e, dir, now)
}

/// `entry` as a window served contiguously from the receiver's own position: a register event takes effect only in position
/// (rule B), and the witnessed vector moves over it only then.
fn window(e: &Engine, entry: &OplogEntry) -> SyncOutcome {
    let mut theirs = VersionVector::default();
    theirs.observe(entry.stamp);
    let mine = e.witnessed_vector().unwrap();
    let mut outcome = SyncOutcome::default();
    e.apply_peer_window_into(
        &theirs,
        Some(&mine),
        std::slice::from_ref(entry),
        PeerWindow {
            scanned_to: entry.stamp.hlc,
            exhausted: true,
            passed_through: None,
            asked_partial: false,
        },
        &mut outcome,
    )
    .unwrap();
    outcome
}

#[test]
fn a_swap_entry_exactly_the_bound_ahead_is_taken_and_appended() {
    for (name, _) in the_swap_entries(0) {
        let (e, _dir, now) = member();
        // The binding's stage is held here, with a deadline above the stamp the binding carries.
        e.create_stage("shop", STAGE, MergePolicy::Document, Hlc::new(now + 600_000, 0)).unwrap();
        let limit = now + e.clock_bound().skew_ms;
        let entry = the_swap_entries(limit).into_iter().find(|(n, _)| *n == name).unwrap().1;
        let outcome = window(&e, &entry);
        assert!(outcome.clock_refused_at.is_none(), "{name} exactly at the bound: {outcome:?}");
        assert_eq!(outcome.clock_refused, 0, "{name}: {outcome:?}");
        assert!(
            everything(&e).iter().any(|held| held.stamp == entry.stamp && held.kind == entry.kind),
            "{name} is appended"
        );
        assert_eq!(
            e.witnessed_vector().unwrap().get(entry.stamp.origin()),
            entry.stamp.hlc,
            "{name} is witnessed"
        );
    }
}

#[test]
fn a_swap_entry_one_millisecond_past_the_bound_is_refused_by_name_and_not_witnessed() {
    for (name, _) in the_swap_entries(0) {
        let (e, _dir, now) = member();
        let past = now + e.clock_bound().skew_ms + 1;
        let entry = the_swap_entries(past).into_iter().find(|(n, _)| *n == name).unwrap().1;
        let outcome = e.apply_batch(std::slice::from_ref(&entry)).unwrap();
        let stop = outcome.clock_refused_at.as_ref().unwrap_or_else(|| {
            panic!("{name} one ms past the bound is refused by name: {outcome:?}")
        });
        assert_eq!(stop.reason, ClockRefusal::Stamp, "{name}");
        assert_eq!(stop.due_in_ms, 1, "{name}: due one millisecond from now");
        assert_eq!((outcome.applied, outcome.ddl, outcome.clock_refused), (0, 0, 1), "{name}");
        assert!(
            !everything(&e).iter().any(|held| held.kind == entry.kind),
            "{name} is not appended"
        );
        assert_eq!(
            e.witnessed_vector().unwrap().get(entry.stamp.origin()),
            Hlc::ZERO,
            "{name} is not witnessed: the vector did not move"
        );
    }
}

#[test]
fn a_far_swap_entry_does_not_move_the_logical_clock() {
    for (name, _) in the_swap_entries(0) {
        let (e, _dir) = engine();
        let entry = the_swap_entries(physical_now_ms() + 86_400_000)
            .into_iter()
            .find(|(n, _)| *n == name)
            .unwrap()
            .1;
        e.apply_batch(std::slice::from_ref(&entry)).unwrap();
        let lead = e.clock_last().wall_ms.saturating_sub(physical_now_ms());
        assert!(lead <= e.clock_bound().skew_ms, "{name} moved the logical clock {lead} ms ahead");
    }
}

#[test]
fn a_swap_entry_whose_body_does_not_decode_stops_the_round() {
    for (name, _) in the_swap_entries(0) {
        let (e, _dir, now) = member();
        let mut entry = the_swap_entries(now).into_iter().find(|(n, _)| *n == name).unwrap().1;
        entry.body = Some(vec![1, 2, 3]);
        assert!(
            e.apply_batch(&[entry]).is_err(),
            "{name}: a body that is no payload fails the round"
        );
    }
}

/// Q7's consumers read a loss record's bytes as merge state. A swap's loss record is a kind of its own, so none of them
/// takes it for one: it is not a Q7 loss entry in the oplog and it is not renderable as a `conflictResolved`.
#[test]
fn a_q7_consumer_does_not_take_a_swaps_loss_record_for_one_of_its_own() {
    let (e, _dir, now) = member();
    let entry = the_swap_entries(now).into_iter().find(|(n, _)| *n == "OutLossRecord").unwrap().1;
    e.apply_batch(std::slice::from_ref(&entry)).unwrap();
    let held = everything(&e);
    assert!(held.iter().any(|x| x.kind == OpKind::OutLossRecord));
    assert!(held.iter().all(|x| x.kind != OpKind::LossRecord), "no Q7 loss entry was made of it");
    assert!(!entry.kind.is_loss_record());
}

/// The document apply path never takes a swap's loss record for a write of the document it names.
#[test]
fn a_swaps_loss_record_is_refused_by_the_document_apply_path() {
    let (e, _dir, now) = member();
    let orders = e.create_collection("shop", "orders").unwrap();
    let entry = the_swap_entries(now).into_iter().find(|(n, _)| *n == "OutLossRecord").unwrap().1;
    assert!(e.apply_remote(&orders, &entry).is_err());
    assert!(e.get(&orders, &DocId::Int64(1)).unwrap().is_none(), "no document was written");
    assert!(
        !everything(&e).iter().any(|x| x.kind == OpKind::OutLossRecord),
        "nothing was appended"
    );
}
