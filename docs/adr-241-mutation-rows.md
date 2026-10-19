# ADR-241: the mutation rows, and the test that kills each

This is the table [ADR-241](decisions.md) refers to. Each row is a change to the source that must make a test fail: it was applied to the post-format text of a `git archive 42628f70` export, the named tests were run by exact name under an 1,800 s timeout and each had to run, and the change was reset. A row recorded as *equivalent* cannot be killed, and a row recorded as *defensive* guards something no test can tell apart; each says why. **29 rows: 27 killed, one defensive (O5), one equivalent (O19).** Rows M are the change's own, O1, O2 and O4 to O9 the review's, the rest added with the fixes after it. M1 builds against hnsw_rs 0.3.4 carrying the same added function, so that it compiles.

## The library, the linking and the build's retry

| # | Mutation | Result | Killed by |
|---|---|---|---|
| M1 | hnsw_rs pinned back to 0.3.4 (with the same add_layer0_edges patch so it compiles) | killed | `every_vector_is_reachable_in_every_build` |
| M3 | every attempt inserts in attempt 0's order | killed | `a_build_left_cut_off_is_retried_in_each_attempts_order`, `an_attempt_builds_one_graph_and_each_retry_another` |
| M4 | layer-0 linking removed | killed | `a_build_links_every_point_its_insertion_cut_off` |
| M5 | structural retry removed | killed | `a_build_left_cut_off_is_retried_in_each_attempts_order` |
| M10 | the bridge removed (a stranded component gets no edge) | killed | `the_linking_bridges_a_component_no_search_reaches` |
| O4 | linking ignores points outside the entry point's component | killed | `the_linking_reaches_a_component_the_entry_point_does_not` |
| O5 | linking accepts cut-off candidates | survives (defensive) | an edge from a cut-off candidate reaches nothing, and the next round, of at most four, links the point again; every fixture tried links fully within four rounds without the filter, which saves rounds, and a per-edge assertion cannot be made from outside the loop, since later rounds take edges from points earlier rounds linked |
| O6 | the entry point taken as the last top-level id | killed | `the_linking_reaches_a_component_the_entry_point_does_not` |
| O20 | the bridge enters a component and does not leave it | killed | `the_linking_bridges_a_component_no_search_reaches` |

## The dump check and the graph mark

| # | Mutation | Result | Killed by |
|---|---|---|---|
| M2 | dump check: the format check removed | killed | `every_spoiled_dump_is_refused_by_what_spoiled_it`, `a_dump_that_would_end_the_process_is_refused_and_rebuilt_and_the_process_survives` |
| O1 | dump: a neighbour rank at its layer's size accepted | killed | `every_spoiled_dump_is_refused_by_what_spoiled_it` |
| O2 | dump: an entry rank at its layer's size accepted | killed | `every_spoiled_dump_is_refused_by_what_spoiled_it` |
| O12 | the dump check's squared-length test removed | killed | `every_spoiled_dump_is_refused_by_what_spoiled_it` |
| M9 | the graph marker ignored (a 0.51 snapshot adopted as fresh) | killed | `a_snapshot_0_51_saved_serves_once_and_is_rebuilt` |

## The filtered walk and the keyed join's deadline

| # | Mutation | Result | Killed by |
|---|---|---|---|
| M6 | adaptive first width removed (the first filtered walk is the fixed 320) | killed | `a_filtered_search_returns_k_at_low_selectivity` |
| M7 | the whole adaptive walk removed: a fixed first walk, answered short | killed | `a_filtered_walk_short_at_its_widest_reads_the_admitted_documents`, `a_first_walk_short_of_k_is_retried_wider_and_answers_k`, `a_filtered_search_returns_k_at_low_selectivity` |
| M8 | keyed-join fallback removed (a short widest walk is answered short) | killed | `the_keyed_join_stops_at_its_deadline`, `a_filtered_walk_short_at_its_widest_reads_the_admitted_documents` |
| O7 | FILTERED_MARGIN 2 -> 1 | killed | `a_filtered_search_returns_k_at_low_selectivity` |
| O8 | no retry walk | killed | `a_first_walk_short_of_k_is_retried_wider_and_answers_k` |
| O9 | a walk of half the hits wanted accepted | killed | `a_first_walk_short_of_k_is_retried_wider_and_answers_k` |
| O10 | the retry widened by four times the first only, not by what the walk saw | killed | `a_retry_is_widened_by_what_the_first_walk_saw` |
| O11 | the keyed join's deadline ignored | killed | `the_keyed_join_stops_at_its_deadline` |
| O15 | a search past its deadline answered as a snapshot error, not a timeout | killed | `a_search_past_its_deadline_answers_as_a_timeout` |

## A vector too large to square, where it enters

| # | Mutation | Result | Killed by |
|---|---|---|---|
| O13 | a client's chunk with no finite squared length accepted | killed | `a_vector_too_large_to_score_is_refused_as_a_chunk_and_as_a_query` |
| O14 | a query vector with no finite squared length accepted | killed | `a_vector_too_large_to_score_is_refused_as_a_chunk_and_as_a_query` |
| O16 | a provider's query vector with no finite squared length accepted | killed | `a_provider_query_vector_too_large_to_score_or_missing_answers_502` |
| O17 | a provider's vector with no finite squared length stored | killed | `a_provider_vector_too_large_to_score_stores_nothing_of_its_batch` |
| O18 | the provider's count of what it returned not checked | killed | `a_provider_query_vector_too_large_to_score_or_missing_answers_502` |
| O19 | the query's own check for a missing vector removed (equivalent: the provider's count refuses first) | equivalent | the provider counts what it returned against its inputs and refuses a missing vector first (`expected 1 vectors, got 0`, row O18), so the query's own check is never reached through any provider |
