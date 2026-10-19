# ADR-245: the mutation rows, and the test that kills each

This is the table [ADR-245](decisions.md) refers to. Each row is a change to the source that must make a test fail: it was applied to the post-format text of a `git archive` export of this change's code, the named tests were run by exact name under an 1,800 s timeout and each had to run, and the change was reset. **25 rows, 25 killed.** Rows W are the change's own, and rows R were added after review. ADR-241's rows are in [its own table](adr-241-mutation-rows.md).

## The builders, the queue and the stop

| # | Mutation | Killed by |
|---|---|---|
| W1 | a collection's build queued once per search, not once | `concurrent_accesses_to_one_collection_share_a_single_build` |
| W2 | the build concurrency ignored: a builder per build | `a_slow_build_on_one_collection_does_not_delay_a_search_on_another` |
| W3 | a build's panic not caught: the builder dies with it | `a_build_that_panics_costs_that_build_and_not_the_builder` |
| R4 | a collection's pending mark never cleared after its build | `a_build_that_panics_costs_that_build_and_not_the_builder`, `a_caller_arriving_mid_rebuild_is_served_the_previous_graph` |
| R1 | install without the epoch fence | `a_build_that_outlives_a_drop_installs_nothing`, `a_drop_while_the_build_is_queued_installs_nothing` |
| R5 | the fence epoch read when the build starts, not when it was queued | `a_drop_while_the_build_is_queued_installs_nothing` |
| W16 | a reconfiguration leaves a queued build of another shape unfenced when nothing is resident | `a_reconfiguration_fences_a_queued_build_with_nothing_resident` |
| W19 | a re-delivered configuration fences the queued build of the shape in force | `a_re_delivered_configuration_keeps_the_queued_build_of_its_shape` |
| W12 | a builder holds the store through the save | `a_stop_during_a_save_finds_the_store_free` |
| W17 | the bridge's pass on a builder looks at the request scope, which the stop does not end | `a_stop_during_the_bridge_pass_ends_the_build` |
| W20 | the linking looks at the stop only once a round | `a_stop_during_a_linking_round_ends_it` |
| W10 | a build concurrency of 0 accepted | `graph_builds_run_one_at_a_time_unless_told_otherwise_and_never_none` |

## What a search is answered by meanwhile

| # | Mutation | Killed by |
|---|---|---|
| W7 | a snapshot a previous process saved trusted without judgement | `a_snapshot_saved_before_a_move_of_chunk_ids_is_rebuilt` |
| W14 | a previous process's snapshot judged at the builder's generation, not the process's start | `a_write_between_adopting_a_snapshot_and_judging_it_leaves_it_behind`, `an_edit_before_the_first_search_after_a_restart_leaves_the_snapshot_behind` |
| W18 | a previous process's snapshot judged at a generation past the process's start | `a_write_between_adopting_a_snapshot_and_judging_it_leaves_it_behind`, `an_edit_before_the_first_search_after_a_restart_leaves_the_snapshot_behind` |
| W11 | a snapshot 0.51 saved adopted as fresh: the graph marker ignored | `a_snapshot_0_51_saved_serves_once_and_is_rebuilt` |
| W8 | the stale gauge leaves out graphs not yet judged | `the_stale_gauge_counts_graphs_served_while_they_are_rebuilt` |
| W15 | the pending gauge counts only stale resident graphs | `the_pending_gauge_counts_builds_the_stale_gauge_cannot` |

## The exact scan while a graph is built

| # | Mutation | Killed by |
|---|---|---|
| W4 | no bound on a collection's exact scans | `a_collections_exact_scans_take_turns_until_their_deadline` |
| W6 | a search waits for a scan turn past its deadline | `a_collections_exact_scans_take_turns_until_their_deadline` |
| R2 | the scan turn given back before the scan runs | `a_scan_holds_its_turn_while_it_reads` |
| R2b | the scan turn given back just before the scan reads | `a_scan_holds_its_turn_while_it_reads` |
| W5 | the exact scan's deadline ignored | `an_exact_scan_stops_at_its_deadline` |
| R3 | the scan reads its deadline only after the whole walk | `an_exact_scan_stops_at_its_deadline` |

## What ships

| # | Mutation | Killed by |
|---|---|---|
| W9 | settle compiled into every build | `settle_is_in_no_build_that_ships` |
