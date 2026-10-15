//! The document repair's planner and its loop guard together (repair design sections 3 and
//! 4): what a confirmed digest finding plans, what each way a repair ends does to the
//! ledger, and what the counters say.

use std::time::Duration;

use kimmy_core::{CollectionId, NodeId};
use kimmy_storage::SnapshotProgress;

use super::*;

fn node(n: u128) -> NodeId {
    NodeId::from_bytes(n.to_be_bytes())
}

const C: CollectionId = CollectionId(7);

fn mins(n: u64) -> Duration {
    Duration::from_secs(60 * n)
}

fn stalls_at(now: Duration) -> PeerStalls {
    let mut stalls = PeerStalls::new();
    stalls.set_repair_clock(now, mins(5));
    stalls
}

/// Plan, promote to active, and end as finished: a repair that ran to its end.
fn run_to_the_end(stalls: &mut PeerStalls, peer: NodeId) {
    assert_eq!(stalls.plan_digest_repair(peer, C), DigestPlan::Planned);
    assert_eq!(stalls.repair_due(peer), Some((C, Repair::Converge)));
    stalls.repair_finished(peer);
}

#[test]
fn a_confirmed_finding_plans_one_converge_repair_and_the_ledger_holds_the_rest() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned);
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Held(MayPlan::Busy), "queued");
    assert_eq!(stalls.repair_due(p), Some((C, Repair::Converge)));
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Held(MayPlan::Busy), "active");
    // Another peer's repair of the same collection is its own.
    assert_eq!(stalls.plan_digest_repair(node(2), C), DigestPlan::Planned);
}

#[test]
fn a_count_repair_of_the_collection_wins_and_is_counted() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert!(stalls.plan_repair(p, C, Repair::Snapshot));
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::SkippedCountRepair);
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::SkippedCountRepair.index()], 1);
    // The ledger was not asked to hold anything.
    assert!(stalls.ledger.is_empty());
}

#[test]
fn a_whole_database_pull_pending_for_the_peer_holds_the_repair() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    let mut progress = SnapshotProgress::whole_database();
    progress.test_mark_paged();
    stalls.snapshots.insert(p, progress);
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::WholeDatabasePending);
    // Another peer is not held by it.
    assert_eq!(stalls.plan_digest_repair(node(2), C), DigestPlan::Planned);
}

#[test]
fn an_ineffective_repair_backs_off_by_the_table_and_a_judged_match_resets_it() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    run_to_the_end(&mut stalls, p);
    // Awaiting its probe: held.
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Held(MayPlan::Busy));
    let verdict = stalls.judge_digest_repair(p, C, Judged::Ineffective).unwrap();
    assert_eq!(
        verdict,
        Verdict::Ineffective { level: 1, not_before: mins(1) + crate::repair_ledger::BACKOFF[0] }
    );
    // The cooldown the planner keeps clears when the finding stops being reported.
    stalls.retain_digest(p, &BTreeSet::new());
    assert!(matches!(stalls.plan_digest_repair(p, C), DigestPlan::Held(MayPlan::BackOff(_))));
    stalls.set_repair_clock(mins(7), mins(5));
    run_to_the_end(&mut stalls, p);
    assert!(matches!(
        stalls.judge_digest_repair(p, C, Judged::Effective),
        Some(Verdict::Effective)
    ));
    stalls.retain_digest(p, &BTreeSet::new());
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned, "level back to 0");
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::Applied.index()], 2);
    assert_eq!(counts.digest_judgements[Judgement::Ineffective.index()], 1);
    assert_eq!(counts.digest_judgements[Judgement::Effective.index()], 1);
}

#[test]
fn a_repair_that_wrote_a_row_lowers_the_level_and_one_that_wrote_none_does_not() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    // Level 2 first: two repairs that wrote nothing, judged ineffective.
    for (i, at) in [1, 10].into_iter().enumerate() {
        stalls.set_repair_clock(mins(at), mins(5));
        stalls.retain_digest(p, &BTreeSet::new());
        run_to_the_end(&mut stalls, p);
        let v = stalls.judge_digest_repair(p, C, Judged::Ineffective).unwrap();
        assert!(matches!(v, Verdict::Ineffective { level, .. } if usize::from(level) == i + 1));
    }
    // The third wrote a row.
    stalls.set_repair_clock(mins(100), mins(5));
    stalls.retain_digest(p, &BTreeSet::new());
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned);
    stalls.repair_due(p);
    let applied = kimmy_storage::SnapshotApplied { applied: 3, ..Default::default() };
    stalls.converge_page_applied(p, &applied);
    stalls.repair_finished(p);
    let v = stalls.judge_digest_repair(p, C, Judged::Ineffective).unwrap();
    assert!(matches!(v, Verdict::Ineffective { level: 1, .. }), "{v:?}");
    let rows = stalls.take_repair_counts().digest_rows;
    assert_eq!(rows[RowResult::Applied.index()], 3);
}

#[test]
fn the_tally_is_the_active_repairs_own_and_starts_over_with_the_next() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned);
    stalls.repair_due(p);
    let page = kimmy_storage::SnapshotApplied { created: 2, left: 1, ..Default::default() };
    stalls.converge_page_applied(p, &page);
    assert_eq!(stalls.repairs[&p].tally.created, 2);
    stalls.repair_finished(p);
    assert_eq!(stalls.repairs[&p].tally, Tally::default());
    let rows = stalls.take_repair_counts().digest_rows;
    assert_eq!((rows[RowResult::Created.index()], rows[RowResult::Left.index()]), (2, 1));
}

#[test]
fn a_whole_database_pull_taking_over_ends_a_queued_and_an_active_repair_as_transient() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    let other = CollectionId(8);
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned);
    stalls.repair_due(p); // C is active
    assert!(stalls.plan_repair(p, other, Repair::Converge)); // `other` queued
    stalls
        .ledger
        .started(LedgerKey { peer: p, collection: other, kind: LedgerKind::Digest }, mins(1));
    stalls.whole_database_takes_over(p);
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::Transient.index()], 2);
    // R1: the ledger is idle for both, at level 0, and planning is not held by it.
    stalls.retain_digest(p, &BTreeSet::new());
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned);
    assert_eq!(stalls.plan_digest_repair(p, other), DigestPlan::Planned);
}

#[test]
fn a_repair_cancelled_while_queued_is_transient_and_holds_nothing() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned);
    assert_eq!(stalls.repairs_cancelled(C), 1);
    assert_eq!(
        stalls.take_repair_counts().digest_repairs[DigestRepairOutcome::Transient.index()],
        1
    );
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned);
}

#[test]
fn a_repair_whose_sender_dropped_the_collection_is_transient_not_applied() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned);
    stalls.repair_due(p);
    stalls.repair_ended_transient(p, Transient::Dropped);
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::Transient.index()], 1);
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::Applied.index()], 0);
    // Not awaiting a probe: the ledger would let it plan again once the cooldown passes.
    stalls.retain_digest(p, &BTreeSet::new());
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned);
}

#[test]
fn a_repair_that_advances_nothing_for_the_attempts_is_abandoned_and_counted() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned);
    for _ in 0..=REPAIR_ATTEMPTS {
        stalls.repair_due(p);
    }
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::Abandoned.index()], 1);
    assert!(!stalls.repairing(p));
    stalls.retain_digest(p, &BTreeSet::new());
    assert_eq!(stalls.plan_digest_repair(p, C), DigestPlan::Planned, "R1: level unchanged");
}

#[test]
fn a_repair_never_judged_by_its_deadline_is_counted_unjudged() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    run_to_the_end(&mut stalls, p);
    stalls.set_repair_clock(mins(40), mins(5));
    let expired = stalls.expire_repairs();
    assert_eq!(expired.len(), 1);
    assert_eq!(stalls.take_repair_counts().digest_judgements[Judgement::Unjudged.index()], 1);
}

#[test]
fn recurring_episodes_stop_the_collection_and_the_stop_is_counted_and_gauged() {
    let mut stalls = stalls_at(mins(0));
    let p = node(1);
    let mut stopped = false;
    for i in 0..4u64 {
        stalls.set_repair_clock(mins(i * 16), mins(5));
        stalls.retain_digest(p, &BTreeSet::new());
        run_to_the_end(&mut stalls, p);
        stalls.judge_digest_repair(p, C, Judged::Effective).unwrap();
        stalls.set_repair_clock(mins(i * 16 + 1), mins(5));
        stopped |= stalls.digest_finding_confirmed(p, C).newly_stopped;
    }
    assert!(stopped);
    assert_eq!(stalls.stopped_collections(), 1);
    stalls.retain_digest(p, &BTreeSet::new());
    assert!(matches!(stalls.plan_digest_repair(p, C), DigestPlan::Held(MayPlan::Stopped(_))));
    assert_eq!(stalls.take_repair_counts().digest_judgements[Judgement::Recurring.index()], 1);
}

#[test]
fn the_ledger_is_pruned_with_the_members_and_the_collections() {
    let mut stalls = stalls_at(mins(1));
    for peer in [node(1), node(2)] {
        run_to_the_end(&mut stalls, peer);
    }
    assert_eq!(stalls.ledger.len(), 2);
    stalls.retain_ledger(&BTreeSet::from([C]), Some(&BTreeSet::from([node(1)])));
    assert_eq!(stalls.ledger.len(), 1);
    stalls.retain_ledger(&BTreeSet::new(), None);
    assert!(stalls.ledger.is_empty());
}
