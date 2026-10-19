# ADR-245: the mutation rows, and the test that kills each

This is the table [ADR-245](decisions.md) refers to. Each row is a change to the source that must make a test fail: it was applied to the post-format text of a `git archive` export, the named tests were run by exact name under an 1,800 s timeout and each had to run, and the change was reset. W1 and W11 ran on the code of `f0def264`; W2 to W10, M2 and M10 on the same code one test assertion earlier, the one that gave W1 its kill. **13 rows, 13 killed.** ADR-241's row M9 (the graph mark ignored) is W11 here, since the mark is read in the code this change moved; its other rows are in [ADR-241's table](adr-241-mutation-rows.md), and M2 and M10 were re-run here because the tests that kill them changed or sit beside what changed.

| # | Mutation | Killed by |
|---|---|---|
| W1 | a collection's build queued once per search, not once | `concurrent_accesses_to_one_collection_share_a_single_build` |
| W2 | the build concurrency ignored: a builder per build | `a_slow_build_on_one_collection_does_not_delay_a_search_on_another` |
| W3 | a build's panic not caught: the builder dies with it | `a_build_that_panics_costs_that_build_and_not_the_builder` |
| W4 | no bound on a collection's exact scans | `a_collections_exact_scans_take_turns_until_their_deadline` |
| W5 | the exact scan's deadline ignored | `an_exact_scan_stops_at_its_deadline` |
| W6 | a search waits for a scan turn past its deadline | `a_search_with_no_turn_at_a_scan_by_its_deadline_answers_503_timeout` |
| W7 | a snapshot a previous process saved trusted without judgement | `a_snapshot_saved_before_a_move_of_chunk_ids_is_rebuilt` |
| W8 | the stale gauge leaves out graphs not yet judged | `the_stale_gauge_counts_graphs_served_while_they_are_rebuilt` |
| W9 | settle compiled into every build | `settle_is_in_no_build_that_ships` |
| W10 | a build concurrency of 0 accepted | `graph_builds_run_one_at_a_time_unless_told_otherwise_and_never_none` |
| W11 | a snapshot 0.51 saved adopted as fresh: the graph marker ignored | `a_snapshot_0_51_saved_serves_once_and_is_rebuilt` |
| M2 | dump check: the format check removed (ADR-241's row, re-run on this branch) | `a_dump_that_would_end_the_process_is_refused_and_rebuilt_and_the_process_survives`, `every_spoiled_dump_is_refused_by_what_spoiled_it` |
| M10 | the bridge removed (a stranded component gets no edge) (ADR-241's row, re-run on this branch) | `the_linking_bridges_a_component_no_search_reaches` |
