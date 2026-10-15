//! The document repair's planner and its loop guard together (repair design sections 3 and
//! 4): what a confirmed digest finding plans, what each way a repair ends does to the
//! ledger, and what the counters say.

use std::time::Duration;

use kimmy_core::{CollectionId, NodeId, VersionVector};
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
    assert_eq!(stalls.plan_digest_repair(peer, C, VersionVector::default()), DigestPlan::Planned);
    assert_eq!(stalls.repair_due(peer), Some((C, Repair::Converge)));
    stalls.repair_finished(peer);
}

#[test]
fn a_confirmed_finding_plans_one_converge_repair_and_the_ledger_holds_the_rest() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
    assert_eq!(
        stalls.plan_digest_repair(p, C, VersionVector::default()),
        DigestPlan::Held(MayPlan::Busy),
        "queued"
    );
    assert_eq!(stalls.repair_due(p), Some((C, Repair::Converge)));
    assert_eq!(
        stalls.plan_digest_repair(p, C, VersionVector::default()),
        DigestPlan::Held(MayPlan::Busy),
        "active"
    );
    // Another peer's repair of the same collection is its own.
    assert_eq!(
        stalls.plan_digest_repair(node(2), C, VersionVector::default()),
        DigestPlan::Planned
    );
}

#[test]
fn a_count_repair_of_the_collection_wins_and_is_counted() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert!(stalls.plan_repair(p, C, Repair::Snapshot));
    assert_eq!(
        stalls.plan_digest_repair(p, C, VersionVector::default()),
        DigestPlan::SkippedCountRepair
    );
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
    assert_eq!(
        stalls.plan_digest_repair(p, C, VersionVector::default()),
        DigestPlan::WholeDatabasePending
    );
    // Another peer is not held by it.
    assert_eq!(
        stalls.plan_digest_repair(node(2), C, VersionVector::default()),
        DigestPlan::Planned
    );
}

#[test]
fn an_ineffective_repair_backs_off_by_the_table_and_a_judged_match_resets_it() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    run_to_the_end(&mut stalls, p);
    // Awaiting its probe: held.
    assert_eq!(
        stalls.plan_digest_repair(p, C, VersionVector::default()),
        DigestPlan::Held(MayPlan::Busy)
    );
    let verdict = stalls.judge_digest_repair(p, C, Judged::Ineffective).unwrap();
    assert_eq!(
        verdict,
        Verdict::Ineffective { level: 1, not_before: mins(1) + crate::repair_ledger::BACKOFF[0] }
    );
    // The cooldown the planner keeps clears when the finding stops being reported.
    stalls.retain_digest(p, &BTreeSet::new());
    assert!(matches!(
        stalls.plan_digest_repair(p, C, VersionVector::default()),
        DigestPlan::Held(MayPlan::BackOff(_))
    ));
    stalls.set_repair_clock(mins(7), mins(5));
    run_to_the_end(&mut stalls, p);
    assert!(matches!(
        stalls.judge_digest_repair(p, C, Judged::Effective),
        Some(Verdict::Effective)
    ));
    stalls.retain_digest(p, &BTreeSet::new());
    assert_eq!(
        stalls.plan_digest_repair(p, C, VersionVector::default()),
        DigestPlan::Planned,
        "level back to 0"
    );
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
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
    stalls.repair_due(p);
    let applied =
        kimmy_storage::SnapshotApplied { applied: 3, applied_at_cut: 3, ..Default::default() };
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
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
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
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
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
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
    assert_eq!(stalls.plan_digest_repair(p, other, VersionVector::default()), DigestPlan::Planned);
}

#[test]
fn a_repair_cancelled_while_queued_is_transient_and_holds_nothing() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
    assert_eq!(stalls.repairs_cancelled(C), 1);
    assert_eq!(
        stalls.take_repair_counts().digest_repairs[DigestRepairOutcome::Transient.index()],
        1
    );
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
}

#[test]
fn a_repair_whose_sender_dropped_the_collection_is_transient_not_applied() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
    stalls.repair_due(p);
    stalls.repair_ended_transient(p, Transient::Dropped);
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::Transient.index()], 1);
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::Applied.index()], 0);
    // Not awaiting a probe: the ledger would let it plan again once the cooldown passes.
    stalls.retain_digest(p, &BTreeSet::new());
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
}

#[test]
fn a_repair_that_advances_nothing_for_the_attempts_is_abandoned_and_counted() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
    // Contacts as the loop makes them: the Converge pull and an ordinary one alternate, and
    // only a Converge round that landed nothing counts.
    for _ in 0..(2 * REPAIR_ATTEMPTS + 2) {
        contact(&mut stalls, p);
    }
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.digest_repairs[DigestRepairOutcome::Abandoned.index()], 1);
    assert!(!stalls.repairing(p));
    stalls.retain_digest(p, &BTreeSet::new());
    assert_eq!(
        stalls.plan_digest_repair(p, C, VersionVector::default()),
        DigestPlan::Planned,
        "R1: level unchanged"
    );
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
    assert!(matches!(
        stalls.plan_digest_repair(p, C, VersionVector::default()),
        DigestPlan::Held(MayPlan::Stopped(_))
    ));
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

/// The loop R3 exists to stop. A collection under live writes: every Converge pull applies
/// tail rows above the finding's cut, which ordinary replication would also deliver, and a
/// standing ambiguous id never clears. That counts as no progress, so the level climbs to
/// the cap and the repair runs about four times a day, not about every quarter hour.
#[test]
fn a_written_collection_with_a_standing_ambiguous_id_climbs_to_the_cap() {
    let mut stalls = stalls_at(mins(0));
    let p = node(1);
    let mut now = mins(0);
    let mut levels = Vec::new();
    for _ in 0..7 {
        stalls.set_repair_clock(now, mins(5));
        stalls.retain_digest(p, &BTreeSet::new());
        assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
        stalls.repair_due(p);
        // Five tail rows applied, none at or below the cut.
        let page = kimmy_storage::SnapshotApplied { applied: 5, ..Default::default() };
        stalls.converge_page_applied(p, &page);
        stalls.repair_finished(p);
        let Verdict::Ineffective { level, not_before } =
            stalls.judge_digest_repair(p, C, Judged::Ineffective).unwrap()
        else {
            panic!("not ineffective");
        };
        levels.push(level);
        now = not_before;
    }
    assert_eq!(levels, [1, 2, 3, 4, 5, 5, 5]);
}

/// And the same collection when a pull does write a row at the cut: real progress lowers the
/// level to 1, so a new genuine divergence waits minutes and not six hours.
#[test]
fn a_row_at_the_cut_is_progress_and_lowers_the_level() {
    let mut stalls = stalls_at(mins(0));
    let p = node(1);
    let mut now = mins(0);
    for _ in 0..3 {
        stalls.set_repair_clock(now, mins(5));
        stalls.retain_digest(p, &BTreeSet::new());
        stalls.plan_digest_repair(p, C, VersionVector::default());
        stalls.repair_due(p);
        stalls.repair_finished(p);
        let Some(Verdict::Ineffective { not_before, .. }) =
            stalls.judge_digest_repair(p, C, Judged::Ineffective)
        else {
            panic!("not ineffective");
        };
        now = not_before;
    }
    stalls.set_repair_clock(now, mins(5));
    stalls.retain_digest(p, &BTreeSet::new());
    stalls.plan_digest_repair(p, C, VersionVector::default());
    stalls.repair_due(p);
    let page =
        kimmy_storage::SnapshotApplied { applied: 5, applied_at_cut: 1, ..Default::default() };
    stalls.converge_page_applied(p, &page);
    stalls.repair_finished(p);
    let v = stalls.judge_digest_repair(p, C, Judged::Ineffective).unwrap();
    assert!(matches!(v, Verdict::Ineffective { level: 1, .. }), "{v:?}");
}

/// The cut of the finding that planned a repair reaches the pull that applies its pages, and
/// is the next repair's own, not the last one's.
#[test]
fn the_findings_cut_reaches_the_pull_of_its_own_repair() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    let mut cut = VersionVector::new();
    cut.insert(node(9), kimmy_core::Hlc::new(5_000, 0));
    assert_eq!(stalls.plan_digest_repair(p, C, cut.clone()), DigestPlan::Planned);
    assert_eq!(stalls.repair_due(p), Some((C, Repair::Converge)));
    let progress = stalls.snapshot_progress(p, Some(C), kimmy_storage::SnapshotPolicy::Converge);
    assert_eq!(progress.cut(), &cut);
    stalls.repair_finished(p);
    // The next repair of another collection, planned for another cut.
    let other = CollectionId(8);
    assert_eq!(stalls.plan_digest_repair(p, other, VersionVector::default()), DigestPlan::Planned);
    assert_eq!(stalls.repair_due(p), Some((other, Repair::Converge)));
    let progress =
        stalls.snapshot_progress(p, Some(other), kimmy_storage::SnapshotPolicy::Converge);
    assert_eq!(progress.cut(), &VersionVector::default());
}

// -- The definition repair (repair design section 12) -----------------------------------

fn run_definition_to_the_end(stalls: &mut PeerStalls, peer: NodeId, built: usize) {
    assert_eq!(stalls.plan_definition_repair(peer, C, 1_000), DigestPlan::Planned);
    assert_eq!(stalls.repair_due(peer), Some((C, Repair::Definitions)));
    let applied = kimmy_storage::SnapshotApplied {
        definitions: kimmy_storage::DefinitionRestore { built, ..Default::default() },
        ..Default::default()
    };
    stalls.definitions_page_applied(peer, &applied);
    stalls.repair_finished(peer);
}

#[test]
fn a_definition_repair_is_independent_of_a_count_repair_of_the_collection() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert!(stalls.plan_repair(p, C, Repair::Snapshot));
    assert_eq!(stalls.plan_definition_repair(p, C, 10), DigestPlan::Planned);
    // And a digest repair beside a definition one is the digest's own.
    assert_eq!(
        stalls.plan_digest_repair(node(2), C, VersionVector::default()),
        DigestPlan::Planned
    );
}

#[test]
fn a_whole_database_pull_pending_for_the_peer_holds_a_definition_repair() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    let mut progress = SnapshotProgress::whole_database();
    progress.test_mark_paged();
    stalls.snapshots.insert(p, progress);
    assert_eq!(stalls.plan_definition_repair(p, C, 10), DigestPlan::WholeDatabasePending);
}

#[test]
fn a_definition_repair_that_built_an_index_lowers_the_level_and_one_that_built_none_does_not() {
    let mut stalls = stalls_at(mins(0));
    let p = node(1);
    let mut now = mins(0);
    for expected in [1u8, 2] {
        stalls.set_repair_clock(now, mins(5));
        stalls.retain_definition(p, &BTreeSet::new());
        run_definition_to_the_end(&mut stalls, p, 0);
        let v = stalls.judge_definition_repair(p, C, Judged::Ineffective, false).unwrap();
        let Verdict::Ineffective { level, not_before } = v else { panic!("{v:?}") };
        assert_eq!(level, expected);
        now = not_before;
    }
    stalls.set_repair_clock(now, mins(5));
    stalls.retain_definition(p, &BTreeSet::new());
    run_definition_to_the_end(&mut stalls, p, 1);
    let v = stalls.judge_definition_repair(p, C, Judged::Ineffective, false).unwrap();
    assert!(matches!(v, Verdict::Ineffective { level: 1, .. }), "{v:?}");
}

#[test]
fn an_explained_comparison_is_not_effective_and_is_counted_apart() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    run_definition_to_the_end(&mut stalls, p, 0);
    let v = stalls.judge_definition_repair(p, C, Judged::Ineffective, true).unwrap();
    assert!(matches!(v, Verdict::Ineffective { .. }));
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.definition_judgements[DefinitionJudgement::Explained.index()], 1);
    assert_eq!(counts.definition_judgements[DefinitionJudgement::Effective.index()], 0);
    assert_eq!(counts.definition_repairs[DigestRepairOutcome::Applied.index()], 1);
}

#[test]
fn the_pages_index_results_are_counted_by_result() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_definition_repair(p, C, 10), DigestPlan::Planned);
    stalls.repair_due(p);
    let applied = kimmy_storage::SnapshotApplied {
        definitions: kimmy_storage::DefinitionRestore {
            built: 1,
            unique: 2,
            ambiguous: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    stalls.definitions_page_applied(p, &applied);
    let counts = stalls.take_repair_counts();
    let at = |label: &str| {
        counts.definition_indexes
            [kimmy_storage::DEFINITION_RESULTS.iter().position(|l| *l == label).unwrap()]
    };
    assert_eq!((at("built"), at("unique"), at("ambiguous"), at("ttl")), (1, 2, 3, 0));
}

#[test]
fn a_whole_database_pull_taking_over_ends_a_definition_repair_as_transient() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_definition_repair(p, C, 10), DigestPlan::Planned);
    stalls.repair_due(p);
    stalls.whole_database_takes_over(p);
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.definition_repairs[DigestRepairOutcome::Transient.index()], 1);
    stalls.retain_definition(p, &BTreeSet::new());
    assert_eq!(stalls.plan_definition_repair(p, C, 10), DigestPlan::Planned);
}

#[test]
fn the_size_gate_reaches_the_pull_of_its_own_repair() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_definition_repair(p, C, 4_321), DigestPlan::Planned);
    assert_eq!(stalls.repair_due(p), Some((C, Repair::Definitions)));
    let progress = stalls.snapshot_progress(p, Some(C), kimmy_storage::SnapshotPolicy::Definitions);
    assert_eq!(progress.max_index_documents(), 4_321);
}

#[test]
fn a_definition_repair_not_judged_by_its_deadline_is_counted_unjudged() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    run_definition_to_the_end(&mut stalls, p, 1);
    stalls.set_repair_clock(mins(40), mins(5));
    assert_eq!(stalls.expire_repairs().len(), 1);
    let counts = stalls.take_repair_counts();
    assert_eq!(counts.definition_judgements[DefinitionJudgement::Unjudged.index()], 1);
    assert_eq!(counts.digest_judgements[Judgement::Unjudged.index()], 0);
}

#[test]
fn a_definition_and_a_digest_repair_of_one_pair_have_ledgers_of_their_own() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    run_definition_to_the_end(&mut stalls, p, 0);
    // Awaiting the definition probe: the digest repair is not held by it.
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
}

// -- The lines that report a repair's end (repair design section 15.2) -------------------

/// A subscriber that keeps each event as `LEVEL message {field=value ...}`.
#[derive(Clone, Default)]
struct Lines(Arc<std::sync::Mutex<Vec<String>>>);

impl tracing::Subscriber for Lines {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?} {}", self.0);
                } else {
                    self.0.push_str(&format!("{}={value:?} ", field.name()));
                }
            }
        }
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(format!("{} {}", event.metadata().level(), fields.0));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

fn logged(run: impl FnOnce()) -> Vec<String> {
    let lines = Lines::default();
    let seen = Arc::clone(&lines.0);
    {
        let _recording = tracing::subscriber::set_default(lines);
        run();
    }
    let lines = seen.lock().unwrap().clone();
    lines
}

#[test]
fn a_document_repair_that_left_rows_says_so_and_names_the_first_ids() {
    let lines = logged(|| {
        let mut stalls = stalls_at(mins(1));
        let p = node(1);
        assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
        stalls.repair_due(p);
        let page = kimmy_storage::SnapshotApplied {
            applied: 2,
            applied_at_cut: 2,
            left: 3,
            left_ids: vec![kimmy_core::DocId::Int64(7), kimmy_core::DocId::Int64(9)],
            ..Default::default()
        };
        stalls.converge_page_applied(p, &page);
        stalls.repair_finished(p);
    });
    let ended = lines.iter().find(|l| l.contains("a divergence repair ended")).unwrap();
    assert!(ended.starts_with("INFO "), "{ended}");
    for want in ["kind=\"digest\"", "outcome=\"applied\"", "applied=2", "left=3", "\"7\"", "\"9\""]
    {
        assert!(ended.contains(want), "{want}: {ended}");
    }
    let warn = lines.iter().find(|l| l.contains("it will not write")).expect("the WARN");
    assert!(warn.starts_with("WARN ") && warn.contains("left=3") && warn.contains("operations.md"));
}

#[test]
fn a_document_repair_that_left_nothing_raises_no_warning() {
    let lines = logged(|| {
        let mut stalls = stalls_at(mins(1));
        let p = node(1);
        stalls.plan_digest_repair(p, C, VersionVector::default());
        stalls.repair_due(p);
        stalls.repair_finished(p);
    });
    assert!(lines.iter().any(|l| l.contains("a divergence repair ended")));
    assert!(!lines.iter().any(|l| l.starts_with("WARN ") && l.contains("will not write")));
}

#[test]
fn a_definition_repair_ends_with_what_it_built_and_what_it_skipped_in_one_line() {
    let lines = logged(|| {
        let mut stalls = stalls_at(mins(1));
        let p = node(1);
        stalls.plan_definition_repair(p, C, 10);
        stalls.repair_due(p);
        let page = kimmy_storage::SnapshotApplied {
            definitions: kimmy_storage::DefinitionRestore {
                built: 1,
                unique: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        stalls.definitions_page_applied(p, &page);
        stalls.repair_finished(p);
    });
    let ended = lines.iter().find(|l| l.contains("a divergence repair ended")).unwrap();
    for want in ["kind=\"definition\"", "built=1", "unique=1", "skipped=1"] {
        assert!(ended.contains(want), "{want}: {ended}");
    }
}

#[test]
fn a_transient_end_is_reported_as_transient() {
    let lines = logged(|| {
        let mut stalls = stalls_at(mins(1));
        let p = node(1);
        stalls.plan_digest_repair(p, C, VersionVector::default());
        stalls.repair_due(p);
        stalls.repair_ended_transient(p, Transient::Dropped);
    });
    let ended = lines.iter().find(|l| l.contains("a divergence repair ended")).unwrap();
    assert!(ended.contains("outcome=\"transient\""), "{ended}");
}

// -- A Converge round is bounded and alternates with an ordinary one (design section 2.6) ----

/// One contact with the peer: the loop's `tick_opened`, then the pull's `repair_due`.
fn contact(stalls: &mut PeerStalls, peer: NodeId) -> Option<(CollectionId, Repair)> {
    stalls.tick_opened();
    stalls.repair_due(peer)
}

#[test]
fn a_converge_repair_is_handed_out_every_second_contact_and_never_twice_in_one() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert_eq!(stalls.plan_digest_repair(p, C, VersionVector::default()), DigestPlan::Planned);
    stalls.tick_opened();
    assert_eq!(stalls.repair_due(p), Some((C, Repair::Converge)), "the first contact");
    // A draining tick pulls again at the same peer: that pull is an ordinary one.
    assert_eq!(stalls.repair_due(p), None, "a second pull of the contact");
    assert_eq!(stalls.repair_due(p), None);
    assert_eq!(contact(&mut stalls, p), None, "the contact after a Converge pull is ordinary");
    assert_eq!(stalls.repair_due(p), None);
    assert_eq!(contact(&mut stalls, p), Some((C, Repair::Converge)), "and then it resumes");
    assert!(stalls.repairing(p), "the repair is still under way throughout");
}

#[test]
fn alternation_does_not_count_a_resting_contact_as_a_stalled_round() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    stalls.plan_digest_repair(p, C, VersionVector::default());
    // Twenty contacts of a repair that advances every Converge round: never abandoned.
    for _ in 0..20 {
        if contact(&mut stalls, p).is_some() {
            stalls.snapshot_advanced(p);
        }
    }
    assert!(stalls.repairing(p), "a repair that advances was abandoned");
    assert_eq!(
        stalls.take_repair_counts().digest_repairs[DigestRepairOutcome::Abandoned.index()],
        0
    );
}

#[test]
fn three_converge_rounds_that_land_nothing_abandon_the_repair_and_one_failed_round_does_not() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    stalls.plan_digest_repair(p, C, VersionVector::default());
    let mut handed = 0;
    let mut contacts = 0;
    while stalls.repairing(p) && contacts < 40 {
        contacts += 1;
        // Advance on the first Converge round, then land nothing.
        if contact(&mut stalls, p).is_some() {
            handed += 1;
            if handed == 1 {
                stalls.snapshot_advanced(p);
            }
        }
    }
    assert!(!stalls.repairing(p), "never abandoned");
    // One that advanced, then REPAIR_ATTEMPTS that did not (the abandon fires when the next is
    // asked for), so the repair was handed out REPAIR_ATTEMPTS + 1 times at most.
    assert!((REPAIR_ATTEMPTS..=REPAIR_ATTEMPTS + 2).contains(&handed), "handed {handed} times");
    assert_eq!(
        stalls.take_repair_counts().digest_repairs[DigestRepairOutcome::Abandoned.index()],
        1
    );
}

#[test]
fn a_count_repair_is_not_alternated() {
    let mut stalls = stalls_at(mins(1));
    let p = node(1);
    assert!(stalls.plan_repair(p, C, Repair::Snapshot));
    assert_eq!(contact(&mut stalls, p), Some((C, Repair::Snapshot)));
    assert_eq!(stalls.repair_due(p), Some((C, Repair::Snapshot)), "a second pull of the contact");
    assert_eq!(contact(&mut stalls, p), Some((C, Repair::Snapshot)), "and the next contact");
}
