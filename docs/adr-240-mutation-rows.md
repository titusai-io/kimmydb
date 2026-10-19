# ADR-240: the mutation rows, and the test that kills each

This is the table [ADR-240](decisions.md#adr-240--idempotency-keys-the-record-is-written-with-the-write-and-travels-with-it-and-a-member-takes-keys-only-while-every-member-carries-records) refers to: every mutation row of the eight pieces of B5, run again together on the merged head. Each row is a change to the source that must make a test fail. Each piece ran its own harness against its own tree as it was built; here every row was run on one tree, a `git archive 9922b699` export, the integration of storage (S), the front (F), the executor (E), replication (R), the HTTP cluster tests (C), coverage with L1 and the G8 lookahead (G), the page records delta (P), the model (M) and the docs.

**How a row was run.** A row is one or more exact replacements of the post-format source, applied in order, each of which must occur exactly once at its turn, or the row is not run. It was applied to the export, never to a worktree; its piece's own suites ran under a timeout that kills the suite's whole process group (a hang is a kill: an engine write inside `write_batch` can deadlock), stopping at the first suite that failed; and the files it touched were copied back from a second, pristine export and compared with it. Every suite passed on the unmutated export first, each having run at least one test, and each export was whole and equal to its pristine copy at the end. Two exports ran side by side, each with its own target directory, at `nice -n 10`. A merge mutant that failed a proptest left its seed in the export's `proptest-regressions`, so later rows of that export ran it as one more case, which the unmutated tree passes. *Killed by* names the tests that failed, by function name.

**Rows that had moved.** 35 of the 265 rows no longer applied to the merged text. 32 are re-pointed at it with the same meaning, and the Notes say what moved; three are superseded, their target removed or replaced, and say by what. G's eight re-runs of C's rows are the same edit and the same test as C's, and ran once, as C's. One test R's cluster suite named was renamed since, and the filter follows it.

**Survivors.** A row its piece's suites did not kill was run again against the head's idempotency suites across the crates, the wider suites: kimmy-storage's `idempotency`, `page_delta`, `modify::`, `gc::`, `rewind` and `backup` tests, kimmy-cluster's `--test idempotency` and its `idempotency`, `replay_tests` and `transport::tests` lib tests (less `a_replay_of_many_pages_is_reported_once_not_once_per_page`, whose scoped log subscriber missed a line in three of five shared-process runs of the unmutated tree), kimmy-api's lib tests, `--test idempotency` and `--test api`, and kimmy-mcp's `--test mcp`. The model's delta rows, which run the model alone by design, were run again with P's suite for the same patch. A row still standing after that is killed by a test added on `idempotency-h`, run on a `git archive 595ee612` export with that test's own filter (*killed (test added)*), or is recorded as equivalent, with the argument. No production code changed: no survivor showed a fault in the code, and each one killed was a test missing.

## Storage (S)

58 rows: the key, its scope and the derived `_id` (kimmy-core); the record's codec and merge; the keyed write paths; the life rule; the coalesced replay barrier; the multi-chunk record and its resume; the receiver's store; retention, the purge, the sweep, the rewind and the backup. Suites: `cargo test -p kimmy-core --lib idempotency::` for C1 to C6, `cargo test -p kimmy-storage --lib idempotency_tests::` for the rest, 900 s.

| Row | What it plants | Result | Killed by | Notes |
|---|---|---|---|---|
| C1-derived-id-without-fingerprint | derived id without fingerprint | killed | `every_input_moves_the_derived_id`, `the_derived_id_is_pinned` |  |
| C2-scope-without-principal | scope without principal | killed | `the_derived_id_is_pinned`, `the_scope_hash_is_pinned` |  |
| C3-no-version-nibble | no version nibble | killed | `the_construction_is_rfc_9562_appendix_b2`, `the_derived_id_is_pinned` |  |
| C4-any-escape-accepted | any escape accepted | killed | `everything_but_a_lone_quoted_string_is_refused` |  |
| C5-parameters-ignored | parameters ignored | killed | `everything_but_a_lone_quoted_string_is_refused` |  |
| C6-min-length-15 | min length 15 | killed | `escapes_unescape_and_count_as_one_character`, `the_raw_form_holds_the_same_rule_and_reaches_the_same_scope` |  |
| R1-merge-keeps-winners-first | merge keeps winners first | killed | 8: `a_later_chunk_leaves_what_another_attempt_wrote_meanwhile`, `a_multi_chunk_record_follows_its_chunks`, `a_receiver_merges_and_witnesses_what_it_is_sent`, `a_resume_of_another_members_attempt_writes_each_document_once` and 4 more |  |
| R2-merge-by-first-arrival | merge by first arrival | killed | 14: `a_backup_carries_the_records`, `a_duplicate_while_a_multi_chunk_request_runs_is_in_flight`, `a_multi_chunk_record_follows_its_chunks`, `a_multi_chunk_request_across_crashes_writes_each_document_once` and 10 more |  |
| R3-applying-beats-done | applying beats done | killed | `the_merged_first_is_the_earliest_attempts` |  |
| R4-newer-record-refused | newer record refused | killed | `a_newer_record_is_skipped_and_a_malformed_one_refused` |  |
| R5-trailing-bytes-accepted | trailing bytes accepted | killed | `a_newer_record_is_skipped_and_a_malformed_one_refused` |  |
| P1-record-in-a-later-transaction | record in a later transaction | killed | `a_keyed_insert_replays_the_same_stamp_and_leaves_one_document`, `f1_a_keyed_insert_whose_fsync_fails_replays_after_the_repair`, `f3_a_coalesced_replay_answers_only_after_the_flush`, `f3_a_coalesced_replay_shares_its_records_failed_flush` |  |
| P2-insert-writes-no-record | insert writes no record | killed | 16: `a_backup_carries_the_records`, `a_drops_purge_removes_the_collections_records`, `a_durable_replay_pays_no_fsync`, `a_key_reused_for_another_request_is_refused_and_writes_nothing` and 12 more |  |
| P3-fingerprint-not-compared | fingerprint not compared | killed | `a_key_reused_for_another_request_is_refused_and_writes_nothing` |  |
| P4-kind-not-compared | kind not compared | killed | `a_key_reused_for_another_request_is_refused_and_writes_nothing` |  |
| P5-random-id-for-keyed-insert | random id for keyed insert | killed | `a_keyed_insert_replays_the_same_stamp_and_leaves_one_document`, `the_derived_id_is_the_same_on_two_engines` |  |
| P6-bulk-derives-position-0 | bulk derives position 0 | killed | `a_keyed_bulk_replays_its_stamps_and_derives_each_position`, `l1_a_bulk_inserts_record_runs_from_its_first_entry_to_its_last`, `the_derived_id_is_the_same_on_two_engines` |  |
| P7-bulk-record-at-last-stamp | bulk record at last stamp | killed | `a_keyed_bulk_replays_its_stamps_and_derives_each_position`, `l1_a_bulk_inserts_record_runs_from_its_first_entry_to_its_last` | re-pointed: L1 made the bulk record run first..last (`written_through`); the record's first moved to the batch's last stamp |
| P8-upsert-seed-not-derived | upsert seed not derived | killed | `a_keyed_find_and_modify_records_its_answer` |  |
| P9-upserted-id-as-docid | upserted id as docid | killed | `a_keyed_upsert_records_the_stored_id_value` |  |
| P10-delete-tombstone-check-off | delete tombstone check off | killed | `a_keyed_delete_replays_only_while_its_tombstone_stands` |  |
| L1-history-off | history off | killed | `a_record_of_an_earlier_life_is_never_replayed` |  |
| L2-window-off | window off | killed | `a_record_past_its_window_is_absent` |  |
| L3-history-by-first | history by first | killed | `a_record_is_judged_by_its_last`, `l1_a_drop_at_a_commits_first_entry_leaves_the_record_of_its_later_entry_standing` |  |
| B1-replay-without-barrier | replay without barrier | killed | `f3_a_coalesced_replay_answers_only_after_the_flush`, `f3_a_coalesced_replay_shares_its_records_failed_flush` |  |
| B2-flush-failure-ignored | flush failure ignored | killed | `f3_a_coalesced_replay_shares_its_records_failed_flush` |  |
| B3-ticket-after-release | ticket after release | equivalent in safety | | Equivalent in what a replay may claim; no test can see the rest without a hook between two adjacent statements. The ticket has to name a flush that starts after the record's commit, and that commit landed before the replay's transaction opened (the replay read the record), so a ticket taken after the writer is let go is still taken after it: the flush that covers it takes the writer later still and syncs the record's pages with everything before them, and a replay is answered only once one has. What differs is a flush that takes the writer between `self.release()` and the ticket on another thread: it covers the original's ticket and not this one, so this replay waits one window more for the next flush, and if the disk fails at that next flush it answers `OutcomeUnknown` where the original replays (the record was in fact synced). That is an in-doubt answer where a definite one was possible, never a claim beyond what is durable, and after the failure redb fails every later operation (`PreviousIo`), so a retry is answered by another member either way. A test cannot place that flush without a hook added to `abort_for_replay` between the two statements. |
| M1-in-flight-not-entered | in flight not entered | killed | `a_duplicate_while_a_multi_chunk_request_runs_is_in_flight` |  |
| M2-in-flight-never-left | in flight never left | killed | 9: `a_chunks_record_runs_from_its_first_entry_to_its_last`, `a_duplicate_while_a_multi_chunk_request_runs_is_in_flight`, `a_multi_chunk_record_follows_its_chunks`, `a_multi_chunk_request_across_crashes_writes_each_document_once` and 5 more |  |
| M3-in-flight-not-checked | in flight not checked | killed | `a_duplicate_while_a_multi_chunk_request_runs_is_in_flight` |  |
| M4-multi-refuses-itself | multi refuses itself | killed | 19: `a_backup_carries_the_records`, `a_chunks_record_runs_from_its_first_entry_to_its_last`, `a_duplicate_while_a_multi_chunk_request_runs_is_in_flight`, `a_key_reused_for_another_request_is_refused_and_writes_nothing` and 15 more |  |
| M5-resume-from-any-cursor | resume from any cursor | killed | `a_resume_of_another_members_attempt_writes_each_document_once` |  |
| M6-resume-without-stamp-rule | resume without stamp rule | killed | `a_later_chunk_leaves_what_another_attempt_wrote_meanwhile`, `a_resume_of_another_members_attempt_writes_each_document_once`, `a_rewind_removes_or_reopens_the_records_it_cuts` |  |
| M7-stamp-rule-on-resume-only | stamp rule on resume only | killed | `a_later_chunk_leaves_what_another_attempt_wrote_meanwhile` |  |
| M8-later-chunk-reads-no-record | later chunk reads no record | killed | `a_later_chunk_leaves_what_another_attempt_wrote_meanwhile`, `a_multi_chunk_record_follows_its_chunks`, `a_rewind_removes_or_reopens_the_records_it_cuts`, `f7_retention_collects_a_record_one_window_after_its_last` |  |
| M9-never-applying | never applying | killed | 5: `a_duplicate_while_a_multi_chunk_request_runs_is_in_flight`, `a_multi_chunk_request_across_crashes_writes_each_document_once`, `a_resume_of_another_members_attempt_writes_each_document_once`, `f4_a_three_chunk_inc_that_failed_at_chunk_two_resumes` and 1 more |  |
| M10-never-ends | never ends | killed (test added) | `a_resume_whose_short_last_chunk_writes_nothing_marks_the_record_done` |  |
| M11-no-record-only-done | no record only done | killed (wider suites) | `a_record_a_member_takes_by_page_while_serving_a_snapshot_rides_its_next_page`, `g8_residual_a_tail_of_left_matches_past_the_lookahead_resumes_without_a_double` |  |
| M12-resume-counts-not-folded | resume counts not folded | killed | `a_multi_chunk_request_across_crashes_writes_each_document_once`, `f4_a_three_chunk_inc_that_failed_at_chunk_two_resumes`, `f5_a_keyed_request_stopped_between_chunks_resumes` |  |
| M13-lookup-witness-off | lookup witness off | equivalent | | The lookup's witness can never raise the clock. Every record reaches the table with its stamps already witnessed: a local write's are minted by this clock, a frame's or a page's are witnessed by `store_record_noting`, which witnesses the merged `first` and `last` before it writes, and a reopened or restored store's clock resumes above every row of `IDEMPOTENCY_BY_STAMP` (`the_clock_resumes_above_every_record_after_a_restart`), where a standing record keeps rows at its `first` (F1) and its `last` (retention removes the record with that row). An HLC never goes back. `keyed_live_record`, which the later chunks read through, witnesses as well and is unchanged. |
| M14-local-store-overwrites | local store overwrites | killed | 5: `a_later_chunk_leaves_what_another_attempt_wrote_meanwhile`, `a_multi_chunk_record_follows_its_chunks`, `a_resume_of_another_members_attempt_writes_each_document_once`, `a_rewind_removes_or_reopens_the_records_it_cuts` and 1 more |  |
| S1-store-without-witness | store without witness | killed | `a_receiver_merges_and_witnesses_what_it_is_sent`, `the_clock_resumes_above_every_record_after_a_restart` |  |
| S2-store-history | store history | killed | `a_receiver_merges_and_witnesses_what_it_is_sent` | re-pointed: `store_record_noting` returns `(None, false)` now; the history refusal removed |
| S3-store-overwrites | store overwrites | killed | `a_receiver_merges_and_witnesses_what_it_is_sent` |  |
| S4-no-row-at-last | no row at last | killed | 11: `a_chunks_record_runs_from_its_first_entry_to_its_last`, `a_drops_purge_removes_the_collections_records`, `a_keyed_insert_replays_the_same_stamp_and_leaves_one_document`, `a_multi_chunk_record_follows_its_chunks` and 7 more | re-pointed: rows are written from `row_stamps` (first, last, carrying) since F1; the row at `last` is left out, even where last = first |
| S5-window-filter-ignored | window filter ignored | superseded | | superseded by the coverage rule (`records_for_coverage`, fix (e)): the filter it removed (a window ships the records of the entries it ships) is gone from the design, and is now itself a fault, planted back by M:records-at-shipped-stamps-only; a window's records are held by G01/G02 (F2, F3 off) and G10/G11 (where its reach comes from) |
| S6-resume-hlc-without-records | resume hlc without records | killed | `the_clock_resumes_above_every_record_after_a_restart` |  |
| G1-records-step-skipped | records step skipped | killed | `f7_retention_collects_a_record_one_window_after_its_last`, `the_records_step_runs_when_the_oplog_step_has_nothing_to_do` |  |
| G2-records-step-only-after-oplog-removal | records step only after oplog removal | killed | `f7_retention_collects_a_record_one_window_after_its_last`, `the_records_step_runs_when_the_oplog_step_has_nothing_to_do` |  |
| G3-recheck-off | recheck off | killed (test added) | `a_record_stays_when_a_row_between_its_first_and_last_expires` | re-pointed: the re-check is the first of a `(stale, keep_row)` pair since F1 |
| G4-recheck-equal-only | recheck equal only | killed | `a_record_whose_row_at_last_is_missing_is_still_collected` | re-pointed: as G3 |
| G5-count-after-the-loop | count after the loop | killed | `a_stop_ends_the_records_step_between_chunks` |  |
| G6-purge-leaves-records | purge leaves records | killed | `a_database_drop_removes_every_collections_records`, `a_drops_purge_leaves_a_record_of_the_life_after_it`, `a_drops_purge_removes_the_collections_records`, `a_record_of_an_earlier_life_is_never_replayed` |  |
| G7-sweep-strictly-before | sweep strictly before | killed | `a_late_drops_sweep_removes_the_earlier_lifes_records` |  |
| G8-rewind-ignores-records | rewind ignores records | killed | `a_rewind_removes_or_reopens_the_records_it_cuts` | re-pointed: `rewind_records_in` is a method now (`self.`) |
| G9-backup-without-records | backup without records | killed | `a_backup_carries_the_records`, `the_clock_resumes_above_every_record_after_a_restart` |  |
| G11-records-walk-not-stop-aware | records walk not stop aware | killed | `a_stop_ends_the_records_step_between_chunks` |  |
| G10-tables-not-ensured | tables not ensured | killed | `a_request_that_changed_nothing_writes_no_record`, `f2_a_keyed_insert_whose_first_write_fails_applies_once_after`, `f3_a_coalesced_replay_answers_only_after_the_flush`, `f3_a_coalesced_replay_shares_its_records_failed_flush` |  |

**58 rows: 55 killed, 2 equivalent, 1 superseded, 0 run once elsewhere, 0 survived.**

## The front (F)

49 rows: the operator switch, the live-peer net and the facts field; the header's syntax and the MCP argument's; the route table and its guard; the fingerprint; the outcome bodies; the error codes; the version's window; the metrics and the resume's log line. Each row runs the kimmy-cluster, kimmy-api, kimmy-mcp or kimmyd tests F named for it, 1200 s. Since F ran, phase E replaced the API's key parser with kimmy-core's, moved the gate after authentication into the keyed extractors and `tool_key`, and moved the resume's log line into the engine: those rows are re-pointed at the new sites, with F's suites.

| Row | What it plants | Result | Killed by | Notes |
|---|---|---|---|---|
| G1 | switch ignored | killed | `the_switch_decides_first_and_is_named` |  |
| G2 | net removed | killed | `a_peer_that_is_not_live_is_not_asked`, `the_net_refuses_with_an_older_or_unknown_live_peer_and_accepts_without`, `the_predicate_answers_as_the_gate_does_and_reads_it_live` | to re-point: the gate split moved the net into `facts::idempotency_records_net`, as M's `live-peer-net-removed` now patches it |
| G3 | unheard live peer passes | killed | `the_net_refuses_with_an_older_or_unknown_live_peer_and_accepts_without`, `the_predicate_answers_as_the_gate_does_and_reads_it_live` | to re-point: the unheard set is now `facts::unheard_peers`, which GS's `gs-unheard-passes` patches |
| G4 | membership off passes | killed | `without_a_membership_view_the_net_fails_closed` |  |
| G5 | facts field not advertised | killed | `the_block_says_this_build_carries_idempotency_records` |  |
| G6 | facts field not lenient | killed | `this_build_reads_idempotency_records_leniently_and_an_older_block_as_not_carrying` |  |
| G7 | guard does not ask the gate | killed | `an_older_or_unheard_live_peer_refuses_naming_it_until_it_carries_records`, `with_the_switch_off_a_key_is_refused_naming_the_switch` | re-pointed: the gate moved from the route guard into the keyed extractor, after authentication (phase E); the same text as E:A4, run with F's wider suite. The gate split replaced the first test with `an_older_live_peer_refuses_400_naming_it_whoever_else_is_unheard` and `a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken` |
| G8 | MCP argument not gated | killed | `the_argument_is_gated_as_the_header_is` | re-pointed: `tool_key` asks the gate through `refuse_unless_open` |
| G9 | capability ignores the gate | killed | `idempotency_keys_is_listed_only_while_the_gate_is_open` |  |
| G10 | clustered default is true | killed | `check_config_prints_the_idempotency_keys_in_effect`, `idempotency_keys_defaults_by_clustering_and_parses` |  |
| G11 | check-config does not settle the switch | killed | `check_config_prints_the_idempotency_keys_in_effect` |  |
| G12 | node does not install the gate | killed | `the_idempotency_gate_is_installed_from_the_switch_before_anything_serves` |  |
| G13 | node does not hand the gate its members | killed | `the_idempotency_gate_is_installed_from_the_switch_before_anything_serves` |  |
| H1 | length counted on the escaped value | killed | `the_length_is_the_unescaped_values_and_its_bounds_are_16_and_255`, `the_mcp_argument_is_the_same_rule_without_quotes` | re-pointed: the API's parser was replaced by kimmy-core's `IdempotencyKey::parse_header_value` (phase E); the escape kept in the value |
| H2 | minimum 15 | killed | `the_header_is_read_from_one_field_line_and_refused_by_name`, `the_length_is_the_unescaped_values_and_its_bounds_are_16_and_255`, `the_mcp_argument_is_the_same_rule_without_quotes`, `the_published_bounds_are_the_cores` | re-pointed: `MIN_KEY_CHARS` is kimmy-core's `KEY_MIN_LEN` (the same text as S:C6, run with F's suite) |
| H3 | maximum 256 | killed | `the_header_is_read_from_one_field_line_and_refused_by_name`, `the_length_is_the_unescaped_values_and_its_bounds_are_16_and_255`, `the_mcp_argument_is_the_same_rule_without_quotes`, `the_published_bounds_are_the_cores` | re-pointed: `MAX_KEY_CHARS` is kimmy-core's `KEY_MAX_LEN` |
| H4 | anything after the string accepted | killed | `a_value_that_is_not_one_quoted_string_is_refused` | re-pointed: kimmy-core's parser |
| H5 | parameters not named | killed | `a_value_that_is_not_one_quoted_string_is_refused` | re-pointed: kimmy-core's parser |
| H6 | two field lines accepted | killed | `the_header_is_read_from_one_field_line_and_refused_by_name` |  |
| H7 | unquoted value read | killed | `a_value_that_is_not_one_quoted_string_is_refused` | re-pointed: kimmy-core's parser; an unquoted value is read as the key |
| H8 | non-ASCII accepted | killed | `a_character_outside_printable_ascii_is_refused` | re-pointed: kimmy-core's parser |
| H9 | MCP argument alphabet unchecked | killed | `the_mcp_argument_is_the_same_rule_without_quotes` | re-pointed: the MCP argument is kimmy-core's `IdempotencyKey::from_raw` |
| R1 | table entry removed (bulk) | killed | `the_routes_that_take_an_idempotency_key_are_the_ones_the_specification_gives_one` |  |
| R2 | guard not over /mcp (layered beside the query-string guard) | killed | `every_other_route_refuses_the_header_by_name` | re-pointed: the guard is one layer over the merged table, so removing it is no longer 'not over /mcp' but no guard at all; `/mcp` let through by the guard instead (the same text as M:header-guard-not-over-mcp), run with F's suite |
| R3 | guard ignores the table | killed | `every_other_route_refuses_the_header_by_name`, `the_route_refusal_comes_before_authentication`, `with_the_switch_off_a_key_is_refused_naming_the_switch` |  |
| R4 | guard checks no syntax before authentication | killed | `the_route_refusal_comes_before_authentication` |  |
| R5 | explain with a key allowed (update) | superseded | | superseded by E:A6: the explain refusal moved from the update route's handler into `exec` (phase E); E:A6 plants the same fault there and runs the same suite |
| F1 | body not hashed | killed | `one_body_byte_moves_the_fingerprint`, `the_fingerprint_moves_with_every_part_of_the_request_as_sent` |  |
| F2 | query not hashed | killed | `the_fingerprint_is_taken_over_the_request_as_sent` |  |
| F3 | no length prefix | killed | `the_fingerprint_moves_with_every_part_of_the_request_as_sent` |  |
| F4 | tool fingerprint keeps the key | killed | `a_tool_fingerprint_is_the_name_and_the_arguments_without_the_key` |  |
| F5 | keyed body skips the 415 | killed | `a_keyed_body_is_refused_exactly_as_an_unkeyed_one_is` |  |
| O1 | resumed fields always rendered | killed | `a_keyed_request_adds_skipped_and_resumed_after_everything_else`, `update_and_delete_render_what_their_routes_answered` | re-pointed: `resumed` is a flag rendered by `append_keyed` since phase E |
| O2 | replace renders modified as matched | killed | `replace_renders_what_the_replace_route_answered` |  |
| O3 | find_and_modify reports a removal's stamp | killed | `find_and_modify_reports_a_stamp_only_for_a_document_that_still_exists` |  |
| O4 | update reports a multi write's stamp | killed | `filtered_writes_report_a_stamp_only_when_one_document_was_written` | re-pointed: the counts come from `run_filtered` (fresh or the record's) |
| O5 | delete counts matched as deleted | equivalent | | A filtered delete writes every match it does not skip: `write_chosen` returns no entry only when the new body equals the stored one, and a delete has no new body, so each chunk's entries are its matches and `modified` equals `matched` on every path, fresh, replayed from the record's counts (the same two counts), and resumed with the record's counts folded in. The record-only `done` commit adds matches only from a chunk that wrote nothing, which for a delete is a chunk whose every match was skipped, so it adds none. re-pointed: the counts come from `run_filtered` |
| O6 | bulk reports the stored id, not the sent one | killed | `insert_and_insert_many_echo_the_id_they_stored` |  |
| E1 | in_flight without Retry-After | killed | `the_two_key_answers_carry_their_status_retry_and_wait` |  |
| E2 | in_flight retry elsewhere | killed | `the_two_key_answers_carry_their_status_retry_and_wait` |  |
| E3 | key codes logged at WARN | killed | `operations_publishes_the_level_of_every_code_the_server_logs` |  |
| V1 | window field not this node's | killed | `the_version_publishes_the_window` |  |
| V2 | node publishes the default window | killed | `the_idempotency_gate_is_installed_from_the_switch_before_anything_serves` |  |
| M1 | gated refusal not counted | killed | `with_the_switch_off_a_key_is_refused_naming_the_switch` | re-pointed: the gated refusal is counted in `refuse_unless_open` since the gate moved after authentication |
| M2 | outcome rendered from slot 0 | killed | `the_render_is_byte_for_byte_what_a_scrape_receives`, `the_snapshot_reads_the_same_atomics_the_render_does` |  |
| M3 | records gauge not in the snapshot | killed | `the_snapshot_reads_the_same_atomics_the_render_does` |  |
| M4 | bridge reads another outcome's slot | killed | `each_idempotency_instrument_reads_the_slot_of_its_own_label` |  |
| M5 | a series off the bridge | killed | `every_metrics_series_reaches_the_bridge` |  |
| L1 | resume logged at DEBUG | killed (test added) | `a_resume_logs_at_info_and_a_replay_below_it` | re-pointed: the resume line is the engine's (`Engine::log_resume`) since phase E |

**49 rows: 47 killed, 1 equivalent, 1 superseded, 0 run once elsewhere, 0 survived.**

## The executor (E)

30 rows: each route's keyed call, the replay and its marker, the replay's answer per route, the two key refusals and their counts, authorization before the record, the gate's place, the counters, `skipped` and `resumed`, and the derived `_id`'s echo. Suites as E named them (kimmy-api `--test idempotency`, kimmy-mcp `--test mcp`), 1200 s. Every row applied unchanged.

| Row | What it plants | Result | Killed by | Notes |
|---|---|---|---|---|
| W1 | insert's keyed call dropped | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| W2 | bulk's keyed call dropped | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| W3 | replace's keyed call dropped | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| W4 | delete by id's keyed call dropped | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| W5 | update's and delete's keyed call dropped | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| W6 | find_and_modify's keyed call dropped | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| W7 | MCP tools hand no key to exec | killed | `a_keyed_tool_call_sent_again_is_its_first_result_marked_as_a_replay` |  |
| R1 | replay not marked | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| R2 | MCP replay not marked | killed | `a_keyed_tool_call_sent_again_is_its_first_result_marked_as_a_replay` |  |
| R3 | delete by id replays the current state, not the record | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| R4 | filtered replay's counts not the record's | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| R5 | find_and_modify replay's stamp not filtered by removal | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| R6 | find_and_modify replay ignores the projection | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| K1 | 422 and 409 swapped | killed | `a_different_request_under_a_remembered_key_is_refused_and_writes_nothing` |  |
| K2 | reused not counted | killed | `a_different_request_under_a_remembered_key_is_refused_and_writes_nothing` |  |
| A1 | keyed requests skip authorization (the record answers first) | killed | `a_revoked_grant_is_refused_before_the_record_is_read` |  |
| A2 | scope not per principal | killed | `the_same_key_from_another_principal_applies_independently` |  |
| A3 | gate before authentication (in the route layer) | killed | `with_the_switch_off_a_key_is_refused_naming_the_switch` |  |
| A4 | HTTP extractor does not ask the gate | killed | `with_the_switch_off_a_key_is_refused_naming_the_switch` |  |
| A5 | DELETE by id does not ask the gate | killed | `with_the_switch_off_a_key_is_refused_naming_the_switch` |  |
| A6 | explain with a key allowed (update) | killed | `explain_with_a_key_is_refused` |  |
| M1 | peer replays not counted | killed | `a_replay_of_another_members_record_is_a_peer_replay` |  |
| M2 | a resume counted as applied | killed | `a_stopped_multi_update_resumes_with_its_key_and_counts_what_it_left` |  |
| M3 | records gauge not read from the engine | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| S1 | resumed never rendered | killed | `a_keyed_request_adds_skipped_and_resumed_after_everything_else` |  |
| S2 | skipped rendered only on a resume | killed | `a_keyed_request_adds_skipped_and_resumed_after_everything_else` |  |
| S3 | skipped not carried from the engine | killed | `a_stopped_multi_update_resumes_with_its_key_and_counts_what_it_left` |  |
| D1 | insert replay echoes another position's derived id | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| D2 | bulk replay echoes position 0's id for every document | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |
| D3 | a replay echoes the derived id over the client's own | killed | `a_replay_of_every_keyed_route_is_the_first_answer_byte_for_byte` |  |

**30 rows: 30 killed, 0 equivalent, 0 superseded, 0 run once elsewhere, 0 survived.**

## Records across members (R)

19 rows: the receiver's store, a window's records and the frame's fit, the replay, the snapshot's records phase, the converge page, the stamp rule on a fresh attempt and the live-peer net. Suites: `cargo test -p kimmy-storage --lib idempotency_repl_tests::`; kimmy-cluster's `--test idempotency` with the lib tests of R; or C9 alone; 1200 s. G's coverage rule replaced the window readers W1 to W3 moved.

| Row | What it plants | Result | Killed by | Notes |
|---|---|---|---|---|
| V1-receiver-stores-only-on-win | receiver stores only on win | killed (test added) | `a_record_lands_with_the_run_that_takes_its_entry_when_the_entry_loses` | re-pointed: `frame.owes` takes the entry's stamp since the records ride by stamp (the same text as M:store-only-on-a-win) |
| V2-record-stored-only-with-its-carrying-entry | record stored only with its carrying entry | killed (test added) | `a_record_whose_entry_is_not_in_the_frame_rides_with_a_later_entry` | re-pointed: a record rides with an entry at or after its row (`rides_by`); stored only with the entry at its row |
| V3-record-stored-while-history | record stored while history | killed | `a_history_record_is_not_stored_and_an_undecodable_one_is_dropped` | re-pointed: G's re-point of the same row (the store returns `(None, false)`) |
| V4-undecodable-record-kept-past-its-row | undecodable record kept past its row | killed | `an_undecodable_record_goes_when_its_row_expires` | re-pointed: G's re-point (the retention re-check is a `(stale, keep_row)` pair) |
| W1-records-outside-the-windows-txn-keys | records outside the windows txn keys | killed | `a_windows_records_are_read_in_its_own_transaction` | re-pointed: one reader, `records_for_coverage`, replaced both `records_at_stamps` (W1) and `records_for_window` (W2); the same text as G13 |
| W2-records-outside-the-windows-txn-linear | records outside the windows txn linear | superseded | | superseded: `records_for_window`, the linear reader it moved out of the window's transaction, is gone; the one reader left is W1's (re-pointed, = G13) |
| W3-frame-fit-ignores-records | frame fit ignores records | killed | `a_frame_counts_every_record_its_window_ships` | re-pointed: the fit counts a record as a rider of the entry it rides with (`riders`); the same text as G14; suite filter `a_frame_counts_the_records` renamed `a_frame_counts_every_record` |
| W4-pull-serves-no-records | pull serves no records | killed | 7: `a_fresh_multi_chunk_request_skips_what_unrelated_writers_wrote_between_its_chunks`, `c1_a_retry_on_the_member_the_write_replicated_to_replays`, `c2_a_retry_applied_elsewhere_converges_to_one_document_and_one_record`, `c3_one_keyed_inc_applied_on_two_members_ends_at_base_plus_one` and 3 more | suite filter `a_frame_counts_the_records` renamed `a_frame_counts_every_record` |
| W5-push-carries-no-records | push carries no records | killed | `c1_a_pushed_window_carries_the_record` | suite filter `a_frame_counts_the_records` renamed `a_frame_counts_every_record` |
| W6-replay-applies-without-records | replay applies without records | killed | `a_replay_brings_back_the_records_of_the_keyed_writes_it_restores` | suite filter `a_frame_counts_the_records` renamed `a_frame_counts_every_record` |
| S1-snapshot-without-records | snapshot without records | killed | `a_records_phase_spans_pages_and_an_older_receiver_skips_it`, `a_repair_and_a_converge_page_carry_the_records`, `c6_a_snapshot_carries_each_collections_records_before_its_documents` |  |
| S2-snapshot-page-records-not-stored | snapshot page records not stored | killed | `a_records_phase_spans_pages_and_an_older_receiver_skips_it`, `a_repair_and_a_converge_page_carry_the_records`, `c6_a_snapshot_carries_each_collections_records_before_its_documents` |  |
| S3-snapshot-records-after-documents | snapshot records after documents | killed | `a_records_phase_spans_pages_and_an_older_receiver_skips_it`, `a_repair_and_a_converge_page_carry_the_records`, `c6_a_snapshot_carries_each_collections_records_before_its_documents` |  |
| S4-records-page-not-ending-at-a-collection-boundary | records page not ending at a collection boundary | killed | `c6_a_snapshot_carries_each_collections_records_before_its_documents` | re-pointed: the page test counts the records phase's own records (`phased`) since the page delta |
| S5-cursor-position-not-echoed | cursor position not echoed | killed | `a_records_phase_spans_pages_and_an_older_receiver_skips_it` |  |
| S6-cursor-position-not-written | cursor position not written | killed | `a_records_phase_spans_pages_and_an_older_receiver_skips_it`, `c6_a_snapshot_carries_each_collections_records_before_its_documents` |  |
| S7-converge-page-without-records | converge page without records | killed | `a_repair_and_a_converge_page_carry_the_records` |  |
| M7-stamp-rule-on-resume-only | stamp rule on resume only | killed | `a_fresh_multi_chunk_request_skips_what_unrelated_writers_wrote_between_its_chunks` | suite filter `a_frame_counts_the_records` renamed `a_frame_counts_every_record` |
| G1-live-peer-net-removed | live peer net removed | killed | `c9_a_live_member_that_does_not_carry_records_closes_the_gate` |  |

**19 rows: 18 killed, 0 equivalent, 1 superseded, 0 run once elsewhere, 0 survived.**

## The HTTP cluster tests (C)

8 rows against the cluster tests on real kimmyd processes: `cargo test -p kimmyd --features lab-hooks --test cluster -- --ignored --test-threads=1 <test>`, 1200 s. Every row applied unchanged.

| Row | What it plants | Result | Killed by | Notes |
|---|---|---|---|---|
| K1-live-peer-net-removed | live peer net removed | killed | `lab_hook_c9_a_live_member_that_does_not_carry_records_closes_the_gate` |  |
| K2-strip-hook-does-not-strip | strip hook does not strip | killed | `lab_hook_c9_a_live_member_that_does_not_carry_records_closes_the_gate` |  |
| K3-strip-hook-keeps-the-facts-field | strip hook keeps the facts field | killed | `lab_hook_c9_a_live_member_that_does_not_carry_records_closes_the_gate` |  |
| K4-peer-replay-not-counted | peer replay not counted | killed | `c1_a_retry_on_the_member_a_keyed_write_replicated_to_is_answered_from_its_record` |  |
| K5-fresh-answer-marked-as-a-replay | fresh answer marked as a replay | killed | `c2_a_retry_applied_while_the_first_member_was_stopped_converges_to_one_document` |  |
| K6-stamp-rule-on-resume-only | stamp rule on resume only | killed | `lab_hook_a_fresh_keyed_multi_update_skips_what_others_wrote_between_its_chunks` |  |
| K7-pull-serves-no-records | pull serves no records | killed | `c1_a_retry_on_the_member_a_keyed_write_replicated_to_is_answered_from_its_record` |  |
| K8-snapshot-without-records | snapshot without records | killed | `c6_a_member_that_caught_up_by_snapshot_answers_a_retry_from_the_record` |  |

**8 rows: 8 killed, 0 equivalent, 0 superseded, 0 run once elsewhere, 0 survived.**

## Records travel with coverage (G)

42 rows: G's own 18 (F1 to F3, store on coverage, the frame's fit of riders, the no-op store, the G8 lookahead, where a window's reach comes from, L1), its re-runs of R's 16 rows on its head with a wider suite, and of C's eight, which ran once, as C's. Storage rows run all of kimmy-storage's `idempotency` tests, the model's CI sweep among them, 1500 s.

| Row | What it plants | Result | Killed by | Notes |
|---|---|---|---|---|
| G01-F2-off | F2 off | killed | 46: `a_batch_that_ends_early_leaves_the_records_past_its_end`, `a_frames_record_rides_with_the_entry_at_its_row_not_an_earlier_one`, `a_record_lands_with_the_run_that_takes_its_collections_entry`, `a_sender_finds_the_records_of_the_coverage_it_grants` and 42 more |  |
| G02-F3-off | F3 off | killed | `a_sender_finds_the_records_of_the_coverage_it_grants`, `row_t2_a_cross_origin_relay_while_the_origin_is_down`, `t2_a_cross_origin_relay_leaves_the_effect_bare_while_its_origin_is_down` |  |
| G02b-F3-off-cluster-t2 | F3 off cluster t2 | killed | `t2_a_cross_origin_relay_carries_the_record_while_its_origin_is_down` |  |
| G03-store-on-coverage-off | store on coverage off | killed | `a_frames_records_are_stored_whether_or_not_an_entry_of_their_collection_is_taken`, `g3_a_coverage_only_window_passes_the_request_without_its_record` |  |
| G04-F1-off | F1 off | killed | `a_chunks_record_runs_from_its_first_entry_to_its_last`, `l1_a_bulk_inserts_record_runs_from_its_first_entry_to_its_last`, `row_f1_a_window_cut_inside_a_commit` |  |
| G05-retention-drops-the-first-row | retention drops the first row | killed | `f7_retention_collects_a_record_one_window_after_its_last` |  |
| G06-replay-pages-filtered-by-records_of | replay pages filtered by records of | killed | `g5_a_replay_from_a_peer_whose_copy_of_the_keyed_entry_lost_carries_its_record` |  |
| G07-fit-ignores-riderless-records | fit ignores riderless records | killed | `a_frame_counts_every_record_its_window_ships` |  |
| G08-no-op-store-commits | no op store commits | killed | `a_frames_records_are_stored_whether_or_not_an_entry_of_their_collection_is_taken` |  |
| G09-G8-lookahead-off | G8 lookahead off | killed | 6: `g8_a_caught_up_member_gets_done_with_the_last_chunk_and_replays`, `g8_the_lookahead_ends_past_matches_the_stamp_rule_leaves`, `row_g8_a_member_that_pulls_the_last_chunk_gets_done_with_it`, `row_g8_the_lookahead_ends_past_a_left_match` and 2 more |  |
| G15-lookahead-counts-writable-matches-as-left | lookahead counts writable matches as left | killed | `g8_the_lookahead_ends_past_matches_the_stamp_rule_leaves`, `generated_schedules_short`, `the_lookahead_does_not_end_a_request_with_a_match_it_could_write`, `the_lookahead_reads_one_match_where_the_request_goes_on` |  |
| G16-lookahead-counts-left-matches-as-writable | lookahead counts left matches as writable | killed | `g8_the_lookahead_ends_past_matches_the_stamp_rule_leaves`, `row_g8_the_lookahead_ends_past_a_left_match` |  |
| G17-lookahead-reads-a-full-budget-ahead | lookahead reads a full budget ahead | killed | `the_lookahead_reads_one_match_where_the_request_goes_on` |  |
| G10-reach-from-the-requesters-vector | reach from the requesters vector | killed | 45: `a_batch_that_ends_early_leaves_the_records_past_its_end`, `a_frames_record_rides_with_the_entry_at_its_row_not_an_earlier_one`, `a_record_lands_with_the_run_that_takes_its_collections_entry`, `a_window_ships_a_record_at_its_own_row_not_at_one_an_earlier_life_left` and 41 more |  |
| G11-reach-from-a-read-outside-the-window | reach from a read outside the window | equivalent | | The vector read outside the window is the same table read later, so it is at or above the window's own `servable`, and `window_reach` caps each origin by the window's end the same way: where the two reaches differ, the window's own is that origin's `servable` and the other lies above it. Every row above `servable` is F3's (`records_unanchored` ships each origin's rows above `max(floor, servable)`, run first), and `Shipping` takes each key once, so the wider F2 range finds no record F3 has not already shipped, in the same order: the window's records and its `unanchored` count are the same. |
| G12-L1-off | L1 off | killed | `a_chunks_record_runs_from_its_first_entry_to_its_last`, `finding_l1_a_drop_in_the_millisecond_of_a_commit_buries_its_record`, `l1_a_drop_at_a_commits_first_entry_leaves_the_record_of_its_later_entry_standing` |  |
| G13-records-outside-the-windows-txn | records outside the windows txn | killed | `a_windows_records_are_read_in_its_own_transaction` |  |
| G14-fit-ignores-riders | fit ignores riders | killed | `a_frame_counts_every_record_its_window_ships` |  |
| R-V1-receiver-stores-only-on-win | receiver stores only on win | killed (test added) | `a_record_lands_with_the_run_that_takes_its_entry_when_the_entry_loses` | re-pointed as R:V1 |
| R-V2-record-stored-only-with-its-carrying-entry | record stored only with its carrying entry | killed (test added) | `a_record_whose_entry_is_not_in_the_frame_rides_with_a_later_entry` | re-pointed as R:V2 |
| R-V3-record-stored-while-history | record stored while history | killed | `a_history_record_is_not_stored_and_an_undecodable_one_is_dropped`, `a_receiver_merges_and_witnesses_what_it_is_sent` |  |
| R-V4-undecodable-record-kept-past-its-row | undecodable record kept past its row | killed | `an_undecodable_record_goes_when_its_row_expires` |  |
| R-W4-pull-serves-no-records | pull serves no records | killed | 7: `a_fresh_multi_chunk_request_skips_what_unrelated_writers_wrote_between_its_chunks`, `c1_a_retry_on_the_member_the_write_replicated_to_replays`, `c2_a_retry_applied_elsewhere_converges_to_one_document_and_one_record`, `c3_one_keyed_inc_applied_on_two_members_ends_at_base_plus_one` and 3 more |  |
| R-W5-push-carries-no-records | push carries no records | killed | `c1_a_pushed_window_carries_the_record` |  |
| R-W6-replay-applies-without-records | replay applies without records | killed | `a_replay_brings_back_the_records_of_the_keyed_writes_it_restores`, `g5_a_replay_from_a_peer_whose_copy_of_the_keyed_entry_lost_carries_its_record` |  |
| R-S1-snapshot-without-records | snapshot without records | killed | 16: `a_records_phase_spans_pages_and_an_older_receiver_skips_it`, `a_repair_and_a_converge_page_carry_the_records`, `c6_a_snapshot_carries_each_collections_records_before_its_documents`, `finding_g1_a_relay_of_a_snapshot_held_version_drops_the_record` and 12 more |  |
| R-S2-snapshot-page-records-not-stored | snapshot page records not stored | killed | 17: `a_records_phase_spans_pages_and_an_older_receiver_skips_it`, `a_repair_and_a_converge_page_carry_the_records`, `c6_a_snapshot_carries_each_collections_records_before_its_documents`, `finding_g1_a_relay_of_a_snapshot_held_version_drops_the_record` and 13 more |  |
| R-S3-snapshot-records-after-documents | snapshot records after documents | killed | `a_records_phase_spans_pages_and_an_older_receiver_skips_it`, `a_repair_and_a_converge_page_carry_the_records`, `c6_a_snapshot_carries_each_collections_records_before_its_documents`, `generated_schedules_short` |  |
| R-S4-records-page-not-ending-at-a-collection-boundary | records page not ending at a collection boundary | killed | `c6_a_snapshot_carries_each_collections_records_before_its_documents` | re-pointed as R:S4 |
| R-S5-cursor-position-not-echoed | cursor position not echoed | killed | `a_records_phase_spans_pages_and_an_older_receiver_skips_it`, `generated_schedules_short`, `row_g1b_a_write_between_snapshot_pages_then_a_relay` |  |
| R-S6-cursor-position-not-written | cursor position not written | killed | `a_records_phase_spans_pages_and_an_older_receiver_skips_it`, `c6_a_snapshot_carries_each_collections_records_before_its_documents`, `generated_schedules_short`, `row_g1b_a_write_between_snapshot_pages_then_a_relay` |  |
| R-S7-converge-page-without-records | converge page without records | killed | `a_repair_and_a_converge_page_carry_the_records`, `generated_schedules_short` |  |
| R-M7-stamp-rule-on-resume-only | stamp rule on resume only | killed | `a_fresh_multi_chunk_request_skips_what_unrelated_writers_wrote_between_its_chunks` |  |
| R-G1-live-peer-net-removed | live peer net removed | killed | `c9_a_live_member_that_does_not_carry_records_closes_the_gate` |  |
| C-K1-live-peer-net-removed | live peer net removed | identical | | identical to C:K1-live-peer-net-removed (G re-ran C's rows on its head: the same edit, the same cluster test); run once, as C:K1-live-peer-net-removed |
| C-K2-strip-hook-does-not-strip | strip hook does not strip | identical | | identical to C:K2-strip-hook-does-not-strip (G re-ran C's rows on its head: the same edit, the same cluster test); run once, as C:K2-strip-hook-does-not-strip |
| C-K3-strip-hook-keeps-the-facts-field | strip hook keeps the facts field | identical | | identical to C:K3-strip-hook-keeps-the-facts-field (G re-ran C's rows on its head: the same edit, the same cluster test); run once, as C:K3-strip-hook-keeps-the-facts-field |
| C-K4-peer-replay-not-counted | peer replay not counted | identical | | identical to C:K4-peer-replay-not-counted (G re-ran C's rows on its head: the same edit, the same cluster test); run once, as C:K4-peer-replay-not-counted |
| C-K5-fresh-answer-marked-as-a-replay | fresh answer marked as a replay | identical | | identical to C:K5-fresh-answer-marked-as-a-replay (G re-ran C's rows on its head: the same edit, the same cluster test); run once, as C:K5-fresh-answer-marked-as-a-replay |
| C-K6-stamp-rule-on-resume-only | stamp rule on resume only | identical | | identical to C:K6-stamp-rule-on-resume-only (G re-ran C's rows on its head: the same edit, the same cluster test); run once, as C:K6-stamp-rule-on-resume-only |
| C-K7-pull-serves-no-records | pull serves no records | identical | | identical to C:K7-pull-serves-no-records (G re-ran C's rows on its head: the same edit, the same cluster test); run once, as C:K7-pull-serves-no-records |
| C-K8-snapshot-without-records | snapshot without records | identical | | identical to C:K8-snapshot-without-records (G re-ran C's rows on its head: the same edit, the same cluster test); run once, as C:K8-snapshot-without-records |

**42 rows: 33 killed, 1 equivalent, 0 superseded, 8 run once elsewhere, 0 survived.**

## The page records delta (P)

20 rows: the page records delta's 17 mutants, M01 to M13, and the narrowed re-runs P made of the two whose whole suite hangs (M03x of M03; M12zx and M12zy of M12z). Suite: `cargo test -p kimmy-storage --lib -- <the row's filters>`, 900 s (P ran them at 300 s with the build inside it).

| Row | What it plants | Result | Killed by | Notes |
|---|---|---|---|---|
| M01 | delta off: no page carries a delta | killed | 17: `a_base_from_before_a_restore_onto_a_clock_behind_restarts_its_collection`, `a_base_of_another_epoch_restarts_its_collection_from_its_records`, `a_cursor_without_a_base_carries_no_delta_and_the_base_crosses_bson_and_json`, `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages` and 13 more |  |
| M02 | delta only on the completing page (non-final pages carry none, base frozen at the first page's) | killed | 12: `a_base_from_before_a_restore_onto_a_clock_behind_restarts_its_collection`, `a_base_of_another_epoch_restarts_its_collection_from_its_records`, `a_cursor_without_a_base_carries_no_delta_and_the_base_crosses_bson_and_json`, `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages` and 8 more |  |
| M03 | base from the wrong page: the cursor keeps the base it came with (never advances) | killed (hung) | `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages`, `a_restore_or_an_identity_reset_begins_the_log_again_and_restarts_older_bases`, `r1b_a_retry_between_the_effects_page_and_the_pulls_end_replays` |  |
| M04 | delta stored after the documents, in a transaction of its own after the page's commit | killed | 7: `a_base_from_before_a_restore_onto_a_clock_behind_restarts_its_collection`, `a_base_of_another_epoch_restarts_its_collection_from_its_records`, `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages`, `a_page_carries_documents_only_with_the_whole_remainder` and 3 more |  |
| M05 | next base read outside the documents' transaction (after the page's read) | killed | 5: `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages`, `a_delta_reads_a_bounded_number_of_rows_a_page`, `a_page_carries_documents_only_with_the_whole_remainder`, `a_resumed_pull_is_first_sent_every_record_that_changed_since` and 1 more |  |
| M06 | page stores write no log row | killed | `a_record_a_member_takes_by_page_while_serving_a_snapshot_rides_its_next_page`, `a_store_that_changes_a_record_logs_it_and_one_that_changes_nothing_logs_nothing` |  |
| M07 | frame stores (pull, push, replay, F2/F3) write no log row | killed | `a_record_a_member_pulls_while_serving_a_snapshot_rides_its_next_page`, `a_store_that_changes_a_record_logs_it_and_one_that_changes_nothing_logs_nothing`, `r2_a_record_stored_below_the_senders_witnessed_vector_still_rides_the_next_page` |  |
| M08 | local commits write no log row | killed | 15: `a_base_from_before_a_restore_onto_a_clock_behind_restarts_its_collection`, `a_base_of_another_epoch_restarts_its_collection_from_its_records`, `a_cursor_without_a_base_carries_no_delta_and_the_base_crosses_bson_and_json`, `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages` and 11 more |  |
| M09 | log rows also written to idempotency_by_stamp | killed | `a_record_crosses_each_link_once` |  |
| M10 | a no-op store logs | killed | `a_store_that_changes_a_record_logs_it_and_one_that_changes_nothing_logs_nothing` |  |
| M11 | a document page without the whole remainder (a delta capped at a page, the rest on the next) | killed | `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages`, `a_delta_reads_a_bounded_number_of_rows_a_page`, `a_page_carries_documents_only_with_the_whole_remainder`, `a_resumed_pull_is_first_sent_every_record_that_changed_since` |  |
| M12 | epoch check off: every base is answered | killed | `a_base_from_before_a_restore_onto_a_clock_behind_restarts_its_collection`, `a_base_of_another_epoch_restarts_its_collection_from_its_records`, `a_restore_or_an_identity_reset_begins_the_log_again_and_restarts_older_bases` |  |
| M12v | the value-compare guard alone (no epoch): a base between where the log begins and its high water is answered | killed | `a_base_from_before_a_restore_onto_a_clock_behind_restarts_its_collection`, `a_base_of_another_epoch_restarts_its_collection_from_its_records` |  |
| M12z | the zero base is not answered (a base without an epoch always restarts) | killed (hung) | 8: `a_cursor_without_a_base_carries_no_delta_and_the_base_crosses_bson_and_json`, `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages`, `a_delta_reads_a_bounded_number_of_rows_a_page`, `a_page_carries_documents_only_with_the_whole_remainder` and 4 more |  |
| M12n | a key without an epoch is answered | killed | `a_base_of_another_epoch_restarts_its_collection_from_its_records` |  |
| M12r | the log does not begin again after an identity reset | killed | `a_restore_or_an_identity_reset_begins_the_log_again_and_restarts_older_bases` |  |
| M13 | a records-only page moves the cursor's document position | killed | `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages`, `a_delta_reads_a_bounded_number_of_rows_a_page`, `a_page_carries_documents_only_with_the_whole_remainder`, `a_resumed_pull_is_first_sent_every_record_that_changed_since` |  |
| M03x | base from the wrong page: the cursor keeps the base it came with (never advances) | killed | `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages`, `r1b_a_retry_between_the_effects_page_and_the_pulls_end_replays` | M03 again, its filters narrowed to tests that fail rather than hang (P's own re-run) |
| M12zx | the zero base is not answered (a base without an epoch always restarts) | killed (hung) | `the_zero_base_is_sent_every_change_the_log_holds` | M12z again, its filters narrowed (P's own re-run, which still hung at P's 300 s) |
| M12zy | the zero base is not answered (a base without an epoch always restarts) | killed | `the_zero_base_is_sent_every_change_the_log_holds` | M12z again, narrowed to the one test that fails rather than hangs (P's own re-run) |

**20 rows: 20 killed, 0 equivalent, 0 superseded, 0 run once elsewhere, 0 survived.**

## The model's mutants (M)

39 rows, the table in the repository, `scripts/idempotency-mutants/mutants.py`, by group (design, fix (e), lives, delta, elsewhere), with the suites and timeouts `run.sh` gives them (`IDEM_CI_SEEDS=24`, `--test-threads 4`). The delta group is P's mutants against the model alone.

| Row | What it plants | Result | Killed by | Notes |
|---|---|---|---|---|
| record-in-a-later-txn | The record written in a transaction after the write (insert and filtered write) | killed | 11: `a_keyed_insert_replays_the_same_stamp_and_leaves_one_document`, `a_multi_chunk_request_across_crashes_writes_each_document_once`, `f1_a_keyed_insert_whose_fsync_fails_replays_after_the_repair`, `f3_a_coalesced_replay_answers_only_after_the_flush` and 7 more |  |
| store-only-on-a-win | The receiver stores a frame's record only when the entry wins | killed (test added) | `a_record_lands_with_the_run_that_takes_its_entry_when_the_entry_loses` | the same fault as R:V1, which no test of the head killed |
| resume-without-the-stamp-rule | A resumed attempt writes every match: no `first` stamp rule | killed | 6: `a_resume_of_another_members_attempt_writes_each_document_once`, `a_rewind_removes_or_reopens_the_records_it_cuts`, `g8_residual_a_tail_of_left_matches_past_the_lookahead_resumes_without_a_double`, `generated_schedules_short` and 2 more |  |
| stamp-rule-only-on-resume | The stamp rule applied only to resumed attempts, not to a fresh attempt's later chunks (Chris) | killed | `a_later_chunk_leaves_what_another_attempt_wrote_meanwhile`, `g8_residual_a_tail_of_left_matches_past_the_lookahead_resumes_without_a_double`, `row_concurrent_fresh_attempts_on_two_members` |  |
| page-record-not-witnessed | A snapshot or converge page's record stored without witnessing its stamps | killed | `the_clock_resumes_above_every_record_after_a_restart` |  |
| snapshot-without-records | A snapshot (scoped, converge or whole) stores none of its records | killed | 17: `a_records_phase_spans_pages_and_an_older_receiver_skips_it`, `a_repair_and_a_converge_page_carry_the_records`, `c6_a_snapshot_carries_each_collections_records_before_its_documents`, `finding_g1_a_relay_of_a_snapshot_held_version_drops_the_record` and 13 more |  |
| merge-by-first-arrival | A receiver keeps the record it holds: records merged by first arrival | killed | 10: `a_receiver_merges_and_witnesses_what_it_is_sent`, `c2_two_applications_converge_to_one_document_and_one_record`, `g8_a_caught_up_member_gets_done_with_the_last_chunk_and_replays`, `generated_schedules_short` and 6 more |  |
| merge-keeps-the-winners-first | The merge keeps the winning record's `first` | killed | 17: `a_later_chunk_leaves_what_another_attempt_wrote_meanwhile`, `a_multi_chunk_record_follows_its_chunks`, `a_receiver_merges_and_witnesses_what_it_is_sent`, `a_resume_of_another_members_attempt_writes_each_document_once` and 13 more |  |
| resume-hlc-without-records | `resume_hlc` without the records' high water | killed | `the_clock_resumes_above_every_record_after_a_restart` |  |
| row-at-carrying-only | A received record rowed at its carrying stamp only, not at its `last` (a page store gets no row) | killed | 6: `a_receiver_merges_and_witnesses_what_it_is_sent`, `a_stop_ends_the_records_step_between_chunks`, `f7_retention_collects_a_record_one_window_after_its_last`, `finding_g1_a_relay_of_a_snapshot_held_version_drops_the_record` and 2 more |  |
| random-id-for-keyed-inserts | A keyed insert without `_id` gets a random ObjectId (insert and bulk) | killed | 6: `a_keyed_bulk_replays_its_stamps_and_derives_each_position`, `a_keyed_insert_replays_the_same_stamp_and_leaves_one_document`, `c2_two_applications_converge_to_one_document_and_one_record`, `generated_schedules_short` and 2 more |  |
| derivation-without-the-fingerprint | The derived `_id` hashes no fingerprint | killed | `row_a_reused_key_on_a_member_without_the_record` |  |
| life-rule-off | No record is ever history: the life rule off | killed | 5: `a_history_record_is_not_stored_and_an_undecodable_one_is_dropped`, `a_receiver_merges_and_witnesses_what_it_is_sent`, `a_record_of_an_earlier_life_is_never_replayed`, `generated_schedules_short` and 1 more |  |
| records-at-shipped-stamps-only | A window ships only the records whose row is one of its entries' stamps (before fix (e)) | killed | 14: `finding_g1_a_relay_of_a_snapshot_held_version_drops_the_record`, `g1b_a_write_during_a_snapshot_is_relayed_bare_by_its_receiver`, `g2_a_loser_relay_drops_the_record`, `g2b_a_multi_document_write_whose_first_entry_lost_is_relayed_bare` and 10 more |  |
| f2-without-store-on-coverage | A frame's record is stored only in a run that takes an entry of its collection | killed | `a_frames_records_are_stored_whether_or_not_an_entry_of_their_collection_is_taken`, `g3_a_coverage_only_window_passes_the_request_without_its_record`, `generated_schedules_short` |  |
| no-f3 | No record a sender cannot vouch for by its oplog is shipped (F3 off) | killed | `a_sender_finds_the_records_of_the_coverage_it_grants`, `row_t2_a_cross_origin_relay_while_the_origin_is_down`, `t2_a_cross_origin_relay_leaves_the_effect_bare_while_its_origin_is_down` |  |
| no-row-at-first | A record is rowed at its last (and a carrying stamp), never at its first (F1 off) | killed | `a_chunks_record_runs_from_its_first_entry_to_its_last`, `l1_a_bulk_inserts_record_runs_from_its_first_entry_to_its_last`, `row_f1_a_window_cut_inside_a_commit` |  |
| retention-drops-a-live-first-row | Retention removes the row at the first of a record still inside its window | killed | `f7_retention_collects_a_record_one_window_after_its_last` |  |
| lookahead-off | No lookahead: a request whose end follows a full chunk is marked done by a record-only commit (G8) | killed | 6: `g8_a_caught_up_member_gets_done_with_the_last_chunk_and_replays`, `g8_the_lookahead_ends_past_matches_the_stamp_rule_leaves`, `row_g8_a_member_that_pulls_the_last_chunk_gets_done_with_it`, `row_g8_the_lookahead_ends_past_a_left_match` and 2 more |  |
| lookahead-counts-left-matches-as-writable | The lookahead takes a match the stamp rule leaves for one the request could write | killed | `g8_the_lookahead_ends_past_matches_the_stamp_rule_leaves`, `row_g8_the_lookahead_ends_past_a_left_match` |  |
| last-is-the-commits-first-entry | A record's last is its latest commit's first entry, not its last (L1 reverted) | killed | `a_chunks_record_runs_from_its_first_entry_to_its_last`, `finding_l1_a_drop_in_the_millisecond_of_a_commit_buries_its_record`, `l1_a_bulk_inserts_record_runs_from_its_first_entry_to_its_last`, `l1_a_drop_at_a_commits_first_entry_leaves_the_record_of_its_later_entry_standing` |  |
| replay-pages-filtered-by-shipped-entries | A replay page carries only the records whose carrying stamp is one of its entries (records_of) | killed | `g5_a_replay_from_a_peer_whose_copy_of_the_keyed_entry_lost_carries_its_record` |  |
| replay-without-the-barrier | A coalesced replay answers without waiting for the flush that makes its record durable (F3) | killed | `f3_a_coalesced_replay_answers_only_after_the_flush`, `f3_a_coalesced_replay_shares_its_records_failed_flush` |  |
| purge-removes-every-record-under-the-id | The drop purger removes every record under the collection's id, a later life's too (P1 reverted) | killed | `a_drops_purge_leaves_a_record_of_the_life_after_it`, `row_a_record_of_the_life_after_a_drop_survives_its_purge` |  |
| ships-a-record-at-an-earlier-lifes-row | A sender ships a record at a row below its first, one an earlier life under its key left | killed | `a_frames_record_rides_with_the_entry_at_its_row_not_an_earlier_one`, `a_window_ships_a_record_at_its_own_row_not_at_one_an_earlier_life_left`, `row_a_retry_after_a_drop_and_a_recreation_applies_afresh` |  |
| stores-a-record-with-an-earlier-entry | A receiver stores a frame's record in the run of its collection's first entry, before its row | killed | `a_frames_record_rides_with_the_entry_at_its_row_not_an_earlier_one`, `row_a_retry_after_a_drop_and_a_recreation_applies_afresh` |  |
| stores-records-past-the-stop | A batch that ends at an entry stores the frame's records whose row is past it | killed | `a_batch_that_ends_early_leaves_the_records_past_its_end` |  |
| delta-off | P's M01: delta off: no page carries a delta | killed | 7: `row_r1_a_keyed_write_between_the_records_phase_and_its_page`, `row_r1b_a_peer_takes_a_page_from_the_receiver_mid_pull`, `row_r1b_a_retry_between_the_effects_page_and_the_pulls_end`, `row_r1c_a_whole_database_retry_at_completion` and 3 more |  |
| delta-only-on-the-completing-page | P's M02: delta only on the completing page (non-final pages carry none, base frozen at the first page's) | killed | `row_r1b_a_peer_takes_a_page_from_the_receiver_mid_pull`, `row_r1b_a_retry_between_the_effects_page_and_the_pulls_end` |  |
| delta-base-never-advances | P's M03x: base from the wrong page: the cursor keeps the base it came with (never advances) | killed (wider suites) | `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages`, `r1b_a_retry_between_the_effects_page_and_the_pulls_end_replays` | the model alone does not see it; the same patch as P:M03x, run again with P:M03x's suite, whose page delta tests kill it |
| delta-base-read-outside-the-documents-txn | P's M05: next base read outside the documents' transaction (after the page's read) | killed (wider suites) | 5: `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages`, `a_delta_reads_a_bounded_number_of_rows_a_page`, `a_page_carries_documents_only_with_the_whole_remainder`, `a_resumed_pull_is_first_sent_every_record_that_changed_since` and 1 more | the model alone does not see it; the same patch as P:M05, run again with P:M05's suite, whose page delta tests kill it |
| page-stores-log-no-change | P's M06: page stores write no log row | killed | `row_r2_a_record_taken_by_page_rides_the_next_page_its_member_serves` |  |
| frame-stores-log-no-change | P's M07: frame stores (pull, push, replay, F2/F3) write no log row | killed | `row_r2_a_record_taken_by_frame_rides_the_next_page_its_member_serves`, `row_r2_a_restart_between_the_pages_a_member_serves` |  |
| local-commits-log-no-change | P's M08: local commits write no log row | killed | `row_r1_a_keyed_write_between_the_records_phase_and_its_page`, `row_r1b_a_peer_takes_a_page_from_the_receiver_mid_pull`, `row_r1b_a_retry_between_the_effects_page_and_the_pulls_end`, `row_r1c_a_whole_database_retry_at_completion` |  |
| records-only-page-moves-the-document-position | P's M13: a records-only page moves the cursor's document position | killed (wider suites) | `a_delta_larger_than_the_byte_budget_goes_as_records_only_pages`, `a_delta_reads_a_bounded_number_of_rows_a_page`, `a_page_carries_documents_only_with_the_whole_remainder`, `a_resumed_pull_is_first_sent_every_record_that_changed_since` | the model alone does not see it; the same patch as P:M13, run again with P:M13's suite, whose page delta tests kill it |
| resume-hlc-without-the-change-log | `resume_hlc` without the change log's high water (its keys are minted stamps) | killed | `the_clock_resumes_above_the_change_logs_keys_after_a_restart` |  |
| header-guard-not-over-mcp | The header guard lets `/mcp` through | killed | `every_other_route_refuses_the_header_by_name` |  |
| operator-switch-ungated | `cluster.idempotency_keys` ignored | killed | `with_the_switch_off_a_key_is_refused_naming_the_switch` |  |
| live-peer-net-removed | The live-peer net never blocks | killed | `c9_a_live_member_that_does_not_carry_records_closes_the_gate` | re-pointed by the gate split at `match crate::facts::idempotency_records_net(…)`, and killed there again (GS below) |

**39 rows: 39 killed, 0 equivalent, 0 superseded, 0 run once elsewhere, 0 survived.**

## The build review's fixes (Y): B1, B2, M1, m1 and m3

27 rows, on `git archive` exports of the fix branch `idempotency-y` (`100b8bec` for the first fifteen, `ae544eb0` for the rest), each with its own target directory and a 900 s timeout (1,500 s for the outbound cursor row, whose replay loops to its deadline): B1 (a window carries a record at the lowest of its own rows), B2 (a replay page's records of its collections, both ways), the five tests M1 of the build review found missing, m1 (records too many for one frame: a pull's partial window of records alone, a push not sent, a replay's pages of records alone under a records cursor) and m3 (a rewind removes a cut bulk's record). Suites: the named tests alone, by exact name, each row required to run them. Two rows survived their first run because the outbound B2 test then ran a whole round, whose ordinary pull brought Q's coverage and the record with it; the test now runs the replay alone, and both rows are killed against it.

| Row | What it plants | Result | Killed by | Notes |
|---|---|---|---|---|
| Y-B1a-carried-at-the-first-row-found | A record found again at a lower row keeps the row it was first found at | killed | `a_stopped_batch_covers_a_row_whose_record_rode_a_later_one`, `a_run_committed_before_the_carried_row_holds_the_effect_bare` | the control `a_stopped_batch_with_the_lower_row_found_first` passes under it |
| Y-B1b-lowered-below-first | A lower row found later lowers the carrying row even below the record's `first` | killed | `a_row_below_the_records_first_found_later_does_not_lower_its_carrying_row` |  |
| Y-m3-rewind-reopens-a-bulk | A rewind reopens a cut bulk insert's record as it reopens a filter's | killed | `a_rewind_that_cuts_a_bulk_part_way_removes_its_record` |  |
| Y-B2-no-collection-records-inbound | A replay page carries no record of its collections (B2 off), inbound | killed | `b2_an_inbound_replay_carries_a_record_with_no_row_of_the_requesters_origin` |  |
| Y-B2-no-collection-records-outbound | The same, outbound | killed | `b2_an_outbound_replay_carries_a_record_with_no_row_of_the_requesters_origin` | survived the first run (see above) |
| Y-B2-no-collection-records-storage | The same, in storage | killed | `a_replay_page_takes_the_live_spanning_records_of_its_collections_at_their_first` |  |
| Y-B2-carried-at-last | B2's records carried at `last`, not `first` | killed | `a_replay_page_that_stops_between_the_effects_and_last_stores_the_record` |  |
| Y-B2-outbound-asks-no-replay | The outbound replay's `AskEntries` does not set `replay` | killed | `b2_an_outbound_replay_carries_a_record_with_no_row_of_the_requesters_origin` | survived the first run (see above) |
| Y-m1-cursor-dropped-inbound | A replay page ignores the records cursor it is given | killed | `the_records_at_an_inbound_replays_start_cross_on_several_pages` |  |
| Y-m1-cursor-dropped-outbound | The same, outbound | killed | `the_records_at_an_outbound_replays_start_cross_on_several_pages` |  |
| Y-m1-records-alone-not-stored | A replay page with no lost entries stores none of its records | killed | `the_records_at_an_inbound_replays_start_cross_on_several_pages` |  |
| Y-m1-pull-no-records-alone | A pull whose first entry cannot cross with its riders is answered `fits: 0` as before | killed | `a_pull_whose_first_entry_cannot_cross_with_its_records_takes_them_alone_first` |  |
| Y-m1-push-fails | A push that fits nothing fails as an entry that cannot replicate | killed | `a_push_whose_first_entry_cannot_cross_with_its_records_sends_nothing` |  |
| Y-m1-changed-passed | A record the change log names as changed since the cursor's base is passed with the rest | killed | `a_records_cursor_passes_its_set_up_to_its_key_and_nothing_that_changed` |  |
| Y-m1-epoch-unguarded | A cursor of another epoch is honoured | killed | `a_records_cursor_passes_its_set_up_to_its_key_and_nothing_that_changed` |  |
| Y-m1-set-any-collection | A cursor passes records of collections outside its set | killed | `a_records_cursor_passes_nothing_outside_its_set` |  |
| Y-m1-set-any-first | A cursor passes records whose `first` is past its end | killed | `a_records_cursor_passes_nothing_outside_its_set` |  |
| Y-m1-delta-stop-ignored | A delta that stopped at its budget reads as whole | killed | `a_records_cursors_delta_stops_at_its_budget_and_moves_the_base` |  |
| Y-m1-inbound-cursor-forgotten | The inbound serve starts every contact without the cursor its peer acknowledged | killed | `the_records_at_an_inbound_replays_start_cross_over_several_contacts` |  |
| Y-m1-outbound-cursor-forgotten | The outbound replay starts every contact without the cursor it stored | killed | `the_records_at_an_outbound_replays_start_cross_over_several_contacts` |  |
| M1-X07-retention-first-origin-only | Retention reads only the first origin that has rows | killed | `everything_expires_and_nothing_is_left` | the build review's M1 |
| M1-X05-change-log-never-trimmed | The change log's trim returns at once | killed | `everything_expires_and_nothing_is_left`, `retention_trims_the_change_log_below_the_window_and_keeps_its_beginning` | the build review's M1 (X05) |
| M1-N3-trim-not-called | The retention pass never calls the change log's trim | killed | `retention_trims_the_change_log_below_the_window_and_keeps_its_beginning` | the build review's M1 (N3) |
| M1-N1-whole-log-trimmed | Every retention pass trims the whole change log | killed | `a_retention_pass_during_a_pull_keeps_the_log_the_next_page_reads` | the build review's M1 |
| M1-M13a-records-only-page-drops-the-phase | A records-only page cut inside a records phase drops the phase position | killed | `a_records_only_page_inside_a_records_phase_keeps_the_phase` | the build review's M1 |
| M1-X4-replay-drops-unanchored | A replay page drops its F3 records | killed | `a_replay_page_carries_a_record_its_sender_cannot_vouch_for` | the build review's M1 |
| M1-X4-with-B2-off | X4 with B2's records off as well | killed | `a_replay_page_carries_a_record_its_sender_cannot_vouch_for` | the F3 test holds without B2 |

**27 rows: 27 killed, 0 equivalent, 0 superseded, 0 survived.**

## The gate split (GS)

21 rows, the `gate-split` group of `scripts/idempotency-mutants/mutants.py`, with M's `live-peer-net-removed` re-pointed at the net's new call, on a `git archive 1d3612b0` export of `idempotency-gw` with its own target directory and a 1,500 s timeout per suite: the two gates' split of a live member not heard from yet (`503 peer_unheard`, `Retry-After` of one sync interval, for at most the unheard lease, which is floored at the time SWIM takes to declare a silent peer down plus one sync interval) from one that does not say it has the capability (`400`: its block says no or does not decode, or no block within the lease). Each row names the tests that must fail, and counts as killed only when every one of them ran and failed in its log; every suite passed on the unmutated export first, with every named test run.

| Row | What it plants | Result | Killed by | Notes |
|---|---|---|---|---|
| gs-unheard-counted-as-refusing | A live peer with no block held counts as one that refuses, within its lease too | killed | `a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides` (idempotency), `a_restarted_member_answers_503_until_it_has_heard_its_peers_and_never_400` | the cluster test over real processes: a restarted member answered `400` in its window |
| gs-idempotency-gate-unheard-as-peers | The idempotency gate reports an unheard peer as one that says no | killed | `a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides` (idempotency), `a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken` |  |
| gs-decimal128-gate-unheard-as-peers | The `Decimal128` gate reports an unheard peer as one that cannot | killed | `a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides` (decimal128), `the_live_peer_net_holds_back_a_decimal_id_and_bound_until_every_peer_can_hold_one` |  |
| gs-keyed-edge-unheard-as-400 | The keyed edge answers the gate's transient refusal `400` | killed | `a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken` |  |
| gs-decimal128-edge-unheard-as-400 | The API answers a `Decimal128` refusal for an unheard peer `400` | killed | `a_decimal128_refusal_is_503_while_a_peer_is_unheard_and_400_otherwise`, `the_live_peer_net_holds_back_a_decimal_id_and_bound_until_every_peer_can_hold_one` |  |
| gs-retry-after-dropped | `peer_unheard` carries no `Retry-After` | killed | `a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken`, `the_argument_is_gated_as_the_header_is` | REST's header and MCP's `retry_after_secs` |
| gs-retry-after-zero-allowed | `peer_unheard`'s `Retry-After` may be 0 | killed | `a_decimal128_refusal_is_503_while_a_peer_is_unheard_and_400_otherwise` |  |
| gs-wait-longer | The wait of a refusal for a peer not heard from yet is 5 seconds, not 1 | killed | `the_wait_is_a_fixed_second_whatever_the_sync_interval` |  |
| gs-wait-zero | The wait of a refusal for a peer not heard from yet is 0 | killed | `the_wait_is_a_fixed_second_whatever_the_sync_interval` | `ApiError::peer_unheard`'s own floor, which `gs-retry-after-zero-allowed` kills, would answer 1 |
| gs-precedence-swapped | A peer not heard from decides over one whose block says no | killed | `a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides` (idempotency and decimal128), `an_older_live_peer_refuses_400_naming_it_whoever_else_is_unheard`, `the_live_peer_net_holds_back_a_decimal_id_and_bound_until_every_peer_can_hold_one` |  |
| gs-unheard-passes | A live peer not heard from passes the net (fails open) | killed | `a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides` (idempotency), `a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken` |  |
| gs-capability-listed-while-unheard | `idempotency-keys` listed while an unheard peer closes the gate | killed | `idempotency_keys_is_not_listed_while_a_member_is_unheard_or_says_no`, `a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken` |  |
| gs-retry-class-no | `peer_unheard`'s retry class is `no` | killed | `a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken` |  |
| gs-lease-never-expires | A peer not heard from never passes its lease: `503` for as long as it stays unheard | killed | `a_peer_not_heard_from_is_unheard_within_its_lease_and_refuses_after_it`, `a_peer_not_heard_from_refuses_after_its_lease_and_an_undecodable_one_at_once`, `a_peer_unheard_past_its_lease_or_whose_block_does_not_decode_is_400` |  |
| gs-past-the-lease-passes | A peer past its lease is not counted by the gate at all (fails open) | killed | `a_peer_not_heard_from_is_unheard_within_its_lease_and_refuses_after_it`, `a_peer_unheard_past_its_lease_or_whose_block_does_not_decode_is_400` |  |
| gs-undecodable-counted-as-unheard | A block that does not decode counts as no block: `503` within the lease | killed | `a_peer_whose_block_does_not_decode_refuses_at_once`, `a_peer_not_heard_from_refuses_after_its_lease_and_an_undecodable_one_at_once`, `a_peer_unheard_past_its_lease_or_whose_block_does_not_decode_is_400` |  |
| gs-rename-keeps-the-clock | A SWIM rename leaves the peer's unheard clock where it was | killed | `a_rename_or_a_rejoin_starts_the_unheard_clock_again` |  |
| gs-rejoin-keeps-the-clock | This member's rejoin leaves every peer's unheard clock where it was | killed | `a_rename_or_a_rejoin_starts_the_unheard_clock_again` |  |
| gs-unheard-lease-without-floor | The unheard lease is the lease alone, without SWIM's floor | killed | `the_unheard_lease_is_floored_at_the_time_swim_takes_to_declare_a_peer_down`, `a_frozen_peer_is_declared_down_before_its_unheard_lease_runs_out` | the real-process test: a frozen peer is answered `400` while live |
| gs-floor-without-detection | The floor is the suspect-to-down time alone, without the probe periods SWIM takes to suspect | killed | `the_unheard_lease_is_floored_at_the_time_swim_takes_to_declare_a_peer_down`, `a_frozen_peer_is_declared_down_before_its_unheard_lease_runs_out` | the real-process test: `400` at 5.8 s, the peer declared down at about 6.8 s |
| gs-floor-not-configured | The running membership never gives the member set foca's timers | killed | `a_frozen_peer_is_declared_down_before_its_unheard_lease_runs_out` |  |
| live-peer-net-removed (re-pointed) | The live-peer net never blocks | killed | `c9_a_live_member_that_does_not_carry_records_closes_the_gate` | M's row; counted there |

**21 rows: 20 killed, 1 equivalent, 0 superseded, 0 survived.**

## Totals

| Piece | Rows | Killed | Equivalent | Superseded | Run once elsewhere | Survived |
|---|---|---|---|---|---|---|
| Storage (S) | 58 | 55 | 2 | 1 | 0 | 0 |
| The front (F) | 49 | 47 | 1 | 1 | 0 | 0 |
| The executor (E) | 30 | 30 | 0 | 0 | 0 | 0 |
| Records across members (R) | 19 | 18 | 0 | 1 | 0 | 0 |
| The HTTP cluster tests (C) | 8 | 8 | 0 | 0 | 0 | 0 |
| Records travel with coverage (G) | 42 | 33 | 1 | 0 | 8 | 0 |
| The page records delta (P) | 20 | 20 | 0 | 0 | 0 | 0 |
| The model's mutants (M) | 39 | 39 | 0 | 0 | 0 | 0 |
| The build review's fixes (Y) | 27 | 27 | 0 | 0 | 0 | 0 |
| The gate split (GS) | 21 | 20 | 1 | 0 | 0 | 0 |
| **All** | **313** | **297** | **5** | **3** | **8** | **0** |

Of the 250 rows killed before the build review, 238 were killed by their piece's own suites (3 of them by a hang, with the tests that failed before it), 4 by the wider suites and 8 by the five tests added on `idempotency-h`: `a_resume_logs_at_info_and_a_replay_below_it`, `a_resume_whose_short_last_chunk_writes_nothing_marks_the_record_done` and `a_record_stays_when_a_row_between_its_first_and_last_expires` in `idempotency_tests.rs`, `a_record_lands_with_the_run_that_takes_its_entry_when_the_entry_loses` and `a_record_whose_entry_is_not_in_the_frame_rides_with_a_later_entry` in `idempotency_repl_tests.rs`. The five equivalent rows are B3 and M13 of S, O5 of F, G11 of G and `gs-wait-may-be-zero` of GS.
