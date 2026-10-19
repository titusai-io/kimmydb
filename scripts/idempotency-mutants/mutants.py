#!/usr/bin/env python3
"""The idempotency keys' mutation rows (ADR-240): each is an exact-text patch of the post-format
source, applied once to a `git archive` export, run against its suites under a timeout,
and reverted by copying the pristine file back. A row whose text does not occur exactly
once fails `check` and is never run.

    mutants.py list [group]            names of the rows (group: design, fix-e, lives, delta, elsewhere, fixes, gate-split, all)
    mutants.py check <tree>            every row applies to <tree>, exactly once
    mutants.py apply <name> <tree>     patch <tree> in place
    mutants.py files <name>            the files a row touches
    mutants.py timeout <name>          the row's own suite timeout in seconds, if it has one
    mutants.py kills <name>            the tests that must fail under the row, one per line, if it names them
    mutants.py suites <name>           the suites a row runs, one per line:
                                       <crate> <target> <filter> <ignored: yes|no>
"""
import sys

# The storage crate's idempotency tests: the model's CI sweep, its named rows and
# reproducers, and the unit, replication and coverage tests. What is ignored (the long
# run, rows waiting for a fix not on the tree) is left out.
MODEL = [("kimmy-storage", "--lib", "idempotency", "no")]
# The model alone: what the generated sweep and the named rows see of a mutant, without
# the unit tests that kill it elsewhere (P's page delta rows: its own table has those).
MODEL_ONLY = [("kimmy-storage", "--lib", "idempotency_model_tests", "no")]

# The gate split (the gate window; Chris's ruling): a live member not heard from yet is
# `503 peer_unheard` with `Retry-After`, a held block that says no is `400`, and the block
# that says no decides. Each row names the tests that must fail (`kills`), and `check-kills`
# reads a row's log for them.
GATE_UNIT = ("kimmy-cluster", "--lib", "idempotency::tests::a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides", "no")
DECIMAL_UNIT = ("kimmy-cluster", "--lib", "decimal128::tests::a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides", "no")
API_UNHEARD = ("kimmy-api", "--test idempotency", "a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken", "no")
API_OLDER = ("kimmy-api", "--test idempotency", "an_older_live_peer_refuses_400_naming_it_whoever_else_is_unheard", "no")
API_DECIMAL = ("kimmy-api", "--test api", "the_live_peer_net_holds_back_a_decimal_id_and_bound_until_every_peer_can_hold_one", "no")
CLUSTER_WINDOW = ("kimmyd", "--test cluster", "a_restarted_member_answers_503_until_it_has_heard_its_peers_and_never_400", "yes")
LEASE_UNIT = ("kimmy-cluster", "--lib", "idempotency::tests::a_peer_not_heard_from_is_unheard_within_its_lease_and_refuses_after_it", "no")
DECIMAL_LEASE_UNIT = ("kimmy-cluster", "--lib", "decimal128::tests::a_peer_not_heard_from_refuses_after_its_lease_and_an_undecodable_one_at_once", "no")
UNDECODABLE_UNIT = ("kimmy-cluster", "--lib", "idempotency::tests::a_peer_whose_block_does_not_decode_refuses_at_once", "no")
CLOCK_UNIT = ("kimmy-cluster", "--lib", "facts::tests::a_rename_or_a_rejoin_starts_the_unheard_clock_again", "no")
API_LEASE = ("kimmy-api", "--test idempotency", "a_peer_unheard_past_its_lease_or_whose_block_does_not_decode_is_400", "no")
FLOOR_UNIT = ("kimmy-cluster", "--lib", "facts::tests::the_unheard_lease_is_floored_at_the_time_swim_takes_to_declare_a_peer_down", "no")
FROZEN_CLUSTER = ("kimmyd", "--test cluster", "a_frozen_peer_is_declared_down_before_its_unheard_lease_runs_out", "yes")

GATE_SPLIT = [
    {
        "name": "gs-unheard-counted-as-refusing",
        "group": "gate-split",
        "doc": "Collapse in the net: a live peer with no block held counts as one that says no (400)",
        "patches": [
            ("crates/kimmy-cluster/src/facts.rs",
             "            None => view.undecodable.contains(node) || view.unheard_beyond_lease(node, now),\n",
             "            None => true,\n"),
        ],
        "suites": [GATE_UNIT, CLUSTER_WINDOW],
        "kills": ["idempotency::tests::a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides",
                  "a_restarted_member_answers_503_until_it_has_heard_its_peers_and_never_400"],
    },
    {
        "name": "gs-idempotency-gate-unheard-as-peers",
        "group": "gate-split",
        "doc": "Collapse in the idempotency gate: an unheard peer is reported as one that says no",
        "patches": [
            ("crates/kimmy-cluster/src/idempotency.rs",
             "            PeerNet::Unheard(peers) => Err(IdempotencyClosed::Unheard {\n"
             "                peers: names(peers),\n"
             "                retry_after_secs: crate::membership::UNHEARD_RETRY_AFTER_SECS,\n"
             "            }),\n",
             "            PeerNet::Unheard(peers) => Err(IdempotencyClosed::Peers(names(peers))),\n"),
        ],
        "suites": [GATE_UNIT, API_UNHEARD],
        "kills": ["idempotency::tests::a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides",
                  "a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken"],
    },
    {
        "name": "gs-decimal128-gate-unheard-as-peers",
        "group": "gate-split",
        "doc": "Collapse in the Decimal128 gate: an unheard peer is reported as one that cannot",
        "patches": [
            ("crates/kimmy-cluster/src/decimal128.rs",
             "            PeerNet::Unheard(peers) => Err(Decimal128Closed::Unheard {\n"
             "                peers: names(peers),\n"
             "                retry_after_secs: crate::membership::UNHEARD_RETRY_AFTER_SECS,\n"
             "            }),\n",
             "            PeerNet::Unheard(peers) => Err(Decimal128Closed::Peers(names(peers))),\n"),
        ],
        "suites": [DECIMAL_UNIT, API_DECIMAL],
        "kills": ["decimal128::tests::a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides",
                  "the_live_peer_net_holds_back_a_decimal_id_and_bound_until_every_peer_can_hold_one"],
    },
    {
        "name": "gs-keyed-edge-unheard-as-400",
        "group": "gate-split",
        "doc": "Collapse at the keyed edge: the gate's transient refusal answered 400 bad_request",
        "patches": [
            ("crates/kimmy-api/src/idempotency.rs",
             "                Some(secs) => ApiError::peer_unheard(text, secs),\n",
             "                Some(_) => ApiError::bad_request(text),\n"),
        ],
        "suites": [API_UNHEARD],
        "kills": ["a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken"],
    },
    {
        "name": "gs-decimal128-edge-unheard-as-400",
        "group": "gate-split",
        "doc": "Collapse at the API's mapping: a Decimal128 refusal for an unheard peer answered 400",
        "patches": [
            ("crates/kimmy-api/src/error.rs",
             "                Some(secs) => ApiError::peer_unheard(e.to_string(), secs),\n",
             "                Some(_) => ApiError::bad_request(e.to_string()),\n"),
        ],
        "suites": [("kimmy-api", "--lib", "error::tests::a_decimal128_refusal_is_503_while_a_peer_is_unheard_and_400_otherwise", "no"), API_DECIMAL],
        "kills": ["error::tests::a_decimal128_refusal_is_503_while_a_peer_is_unheard_and_400_otherwise",
                  "the_live_peer_net_holds_back_a_decimal_id_and_bound_until_every_peer_can_hold_one"],
    },
    {
        "name": "gs-retry-after-dropped",
        "group": "gate-split",
        "doc": "peer_unheard carries no Retry-After",
        "patches": [
            ("crates/kimmy-api/src/error.rs",
             "            retry_after_secs: Some(retry_after_secs.max(1)),\n",
             "            retry_after_secs: { let _ = retry_after_secs; None },\n"),
        ],
        "suites": [API_UNHEARD, ("kimmy-mcp", "--test mcp", "the_argument_is_gated_as_the_header_is", "no")],
        "kills": ["a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken",
                  "the_argument_is_gated_as_the_header_is"],
    },
    {
        "name": "gs-retry-after-zero-allowed",
        "group": "gate-split",
        "doc": "peer_unheard's Retry-After may be 0",
        "patches": [
            ("crates/kimmy-api/src/error.rs",
             "            retry_after_secs: Some(retry_after_secs.max(1)),\n",
             "            retry_after_secs: Some(retry_after_secs),\n"),
        ],
        "suites": [("kimmy-api", "--lib", "error::tests::a_decimal128_refusal_is_503_while_a_peer_is_unheard_and_400_otherwise", "no")],
        "kills": ["error::tests::a_decimal128_refusal_is_503_while_a_peer_is_unheard_and_400_otherwise"],
    },
    {
        "name": "gs-wait-longer",
        "group": "gate-split",
        "doc": "The wait of a refusal for a peer not heard from yet is 5 seconds, not 1",
        "patches": [
            ("crates/kimmy-cluster/src/membership.rs",
             "pub const UNHEARD_RETRY_AFTER_SECS: u64 = 1;\n",
             "pub const UNHEARD_RETRY_AFTER_SECS: u64 = 5;\n"),
        ],
        "suites": [("kimmy-cluster", "--lib", "idempotency::tests::the_wait_is_a_fixed_second_whatever_the_sync_interval", "no")],
        "kills": ["idempotency::tests::the_wait_is_a_fixed_second_whatever_the_sync_interval"],
    },
    {
        "name": "gs-wait-zero",
        "group": "gate-split",
        "doc": "The wait of a refusal for a peer not heard from yet is 0",
        "patches": [
            ("crates/kimmy-cluster/src/membership.rs",
             "pub const UNHEARD_RETRY_AFTER_SECS: u64 = 1;\n",
             "pub const UNHEARD_RETRY_AFTER_SECS: u64 = 0;\n"),
        ],
        "suites": [("kimmy-cluster", "--lib", "idempotency::tests::the_wait_is_a_fixed_second_whatever_the_sync_interval", "no")],
        "kills": ["idempotency::tests::the_wait_is_a_fixed_second_whatever_the_sync_interval"],
    },
    {
        "name": "gs-precedence-swapped",
        "group": "gate-split",
        "doc": "Precedence swapped: a peer not heard from decides over one whose block says no",
        "patches": [
            ("crates/kimmy-cluster/src/facts.rs",
             "        let refused = refusing_peers(view, now, has);\n"
             "        if !refused.is_empty() {\n"
             "            return Self::Refused(refused);\n"
             "        }\n"
             "        let unheard = unheard_peers(view, now);\n"
             "        if !unheard.is_empty() {\n"
             "            return Self::Unheard(unheard);\n"
             "        }\n",
             "        let unheard = unheard_peers(view, now);\n"
             "        if !unheard.is_empty() {\n"
             "            return Self::Unheard(unheard);\n"
             "        }\n"
             "        let refused = refusing_peers(view, now, has);\n"
             "        if !refused.is_empty() {\n"
             "            return Self::Refused(refused);\n"
             "        }\n"),
        ],
        "suites": [GATE_UNIT, DECIMAL_UNIT, API_OLDER, API_DECIMAL],
        "kills": ["idempotency::tests::a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides",
                  "decimal128::tests::a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides",
                  "an_older_live_peer_refuses_400_naming_it_whoever_else_is_unheard",
                  "the_live_peer_net_holds_back_a_decimal_id_and_bound_until_every_peer_can_hold_one"],
    },
    {
        "name": "gs-unheard-passes",
        "group": "gate-split",
        "doc": "A live peer not heard from passes the net (fails open)",
        "patches": [
            ("crates/kimmy-cluster/src/facts.rs",
             "            !view.blocks.contains_key(node)\n"
             "                && !view.undecodable.contains(node)\n"
             "                && !view.unheard_beyond_lease(node, now)\n",
             "            let _ = (node, now);\n"
             "            false\n"),
        ],
        "suites": [GATE_UNIT, API_UNHEARD],
        "kills": ["idempotency::tests::a_peer_not_heard_from_is_transient_and_a_peer_that_says_no_decides",
                  "a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken"],
    },
    {
        "name": "gs-capability-listed-while-unheard",
        "group": "gate-split",
        "doc": "idempotency-keys listed while the gate is closed for an unheard peer",
        "patches": [
            ("crates/kimmy-api/src/version.rs",
             "            Self::IdempotencyKeys => state.idempotency_gate().is_ok(),\n",
             "            Self::IdempotencyKeys => state.idempotency_gate().map_or_else(|c| c.retry_after_secs().is_some(), |()| true),\n"),
        ],
        "suites": [("kimmy-api", "--lib", "version::tests::idempotency_keys_is_not_listed_while_a_member_is_unheard_or_says_no", "no"), API_UNHEARD],
        "kills": ["version::tests::idempotency_keys_is_not_listed_while_a_member_is_unheard_or_says_no",
                  "a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken"],
    },
    {
        "name": "gs-retry-class-no",
        "group": "gate-split",
        "doc": "peer_unheard's retry class is no",
        "patches": [
            ("crates/kimmy-api/src/error.rs",
             "            Self::PeerUnheard => Retry::Wait,\n",
             "            Self::PeerUnheard => Retry::No,\n"),
        ],
        "suites": [API_UNHEARD],
        "kills": ["a_live_peer_not_heard_from_is_answered_503_and_the_same_key_is_then_taken"],
    },
    {
        "name": "gs-lease-never-expires",
        "group": "gate-split",
        "doc": "A peer not heard from is never past its lease: the gate answers 503 for as long as it stays unheard",
        "patches": [
            ("crates/kimmy-cluster/src/membership.rs",
             "            .is_some_and(|since| now.saturating_duration_since(*since) > self.unheard_lease)\n",
             "            .is_some_and(|since| {\n"
             "                let _ = (since, now);\n"
             "                false\n"
             "            })\n"),
        ],
        "suites": [LEASE_UNIT, DECIMAL_LEASE_UNIT, API_LEASE],
        "kills": ["idempotency::tests::a_peer_not_heard_from_is_unheard_within_its_lease_and_refuses_after_it",
                  "decimal128::tests::a_peer_not_heard_from_refuses_after_its_lease_and_an_undecodable_one_at_once",
                  "a_peer_unheard_past_its_lease_or_whose_block_does_not_decode_is_400"],
    },
    {
        "name": "gs-past-the-lease-passes",
        "group": "gate-split",
        "doc": "A peer past its lease is no longer counted by the gate at all (fails open)",
        "patches": [
            ("crates/kimmy-cluster/src/facts.rs",
             "            None => view.undecodable.contains(node) || view.unheard_beyond_lease(node, now),\n",
             "            None => view.undecodable.contains(node),\n"),
        ],
        "suites": [LEASE_UNIT, API_LEASE],
        "kills": ["idempotency::tests::a_peer_not_heard_from_is_unheard_within_its_lease_and_refuses_after_it",
                  "a_peer_unheard_past_its_lease_or_whose_block_does_not_decode_is_400"],
    },
    {
        "name": "gs-undecodable-counted-as-unheard",
        "group": "gate-split",
        "doc": "A block that does not decode counts as no block: 503 within the lease instead of 400 at once",
        "patches": [
            ("crates/kimmy-cluster/src/facts.rs",
             "            None => view.undecodable.contains(node) || view.unheard_beyond_lease(node, now),\n",
             "            None => view.unheard_beyond_lease(node, now),\n"),
            ("crates/kimmy-cluster/src/facts.rs",
             "            !view.blocks.contains_key(node)\n"
             "                && !view.undecodable.contains(node)\n"
             "                && !view.unheard_beyond_lease(node, now)\n",
             "            !view.blocks.contains_key(node) && !view.unheard_beyond_lease(node, now)\n"),
        ],
        "suites": [UNDECODABLE_UNIT, DECIMAL_LEASE_UNIT, API_LEASE],
        "kills": ["idempotency::tests::a_peer_whose_block_does_not_decode_refuses_at_once",
                  "decimal128::tests::a_peer_not_heard_from_refuses_after_its_lease_and_an_undecodable_one_at_once",
                  "a_peer_unheard_past_its_lease_or_whose_block_does_not_decode_is_400"],
    },
    {
        "name": "gs-rename-keeps-the-clock",
        "group": "gate-split",
        "doc": "A SWIM rename drops the block and leaves the unheard clock where it was",
        "patches": [
            ("crates/kimmy-cluster/src/membership.rs",
             "        self.drop_peer(&mut table, node);\n"
             "        if let Some(since) = self.0.up_since.lock().get_mut(&node) {\n"
             "            *since = Instant::now();\n"
             "        }\n",
             "        self.drop_peer(&mut table, node);\n"),
        ],
        "suites": [CLOCK_UNIT],
        "kills": ["facts::tests::a_rename_or_a_rejoin_starts_the_unheard_clock_again"],
    },
    {
        "name": "gs-rejoin-keeps-the-clock",
        "group": "gate-split",
        "doc": "This member's own rejoin drops every block and leaves the unheard clocks where they were",
        "patches": [
            ("crates/kimmy-cluster/src/membership.rs",
             "        let now = Instant::now();\n"
             "        for since in self.0.up_since.lock().values_mut() {\n"
             "            *since = now;\n"
             "        }\n",
             ""),
        ],
        "suites": [CLOCK_UNIT],
        "kills": ["facts::tests::a_rename_or_a_rejoin_starts_the_unheard_clock_again"],
    },
    {
        "name": "gs-unheard-lease-without-floor",
        "group": "gate-split",
        "doc": "The unheard lease is the lease alone, without SWIM's floor",
        "patches": [
            ("crates/kimmy-cluster/src/membership.rs",
             "        self.lease().max(floor)\n",
             "        let _ = floor;\n"
             "        self.lease()\n"),
        ],
        "suites": [FLOOR_UNIT, FROZEN_CLUSTER],
        "kills": ["facts::tests::the_unheard_lease_is_floored_at_the_time_swim_takes_to_declare_a_peer_down",
                  "a_frozen_peer_is_declared_down_before_its_unheard_lease_runs_out"],
    },
    {
        "name": "gs-floor-without-detection",
        "group": "gate-split",
        "doc": "The floor is SWIM's suspect-to-down time alone, without the probe periods it takes to suspect",
        "patches": [
            ("crates/kimmy-cluster/src/membership.rs",
             "        timers.suspect_to_down_after + timers.probe_period * (2 * peers)\n",
             "        let _ = (timers.probe_period, peers);\n"
             "        timers.suspect_to_down_after\n"),
        ],
        "suites": [FLOOR_UNIT, FROZEN_CLUSTER],
        "kills": ["facts::tests::the_unheard_lease_is_floored_at_the_time_swim_takes_to_declare_a_peer_down",
                  "a_frozen_peer_is_declared_down_before_its_unheard_lease_runs_out"],
    },
    {
        "name": "gs-floor-not-configured",
        "group": "gate-split",
        "doc": "The running membership never gives the member set foca's timers: no floor",
        "patches": [
            ("crates/kimmy-cluster/src/membership.rs",
             "    members.configure_swim(&config);\n",
             "    let _ = &config;\n"),
        ],
        "suites": [FROZEN_CLUSTER],
        "kills": ["a_frozen_peer_is_declared_down_before_its_unheard_lease_runs_out"],
    },
]

ROWS = [
    {
        "name": "record-in-a-later-txn",
        "group": "design",
        "doc": "The record written in a transaction after the write (insert and filtered write)",
        "patches": [
            ("crates/kimmy-storage/src/docs.rs",
             """        let stamp = entry.stamp;
        // In the write's own transaction: the record survives exactly what the
        // write survives (ADR-240).
        if let Some(keyed) = keyed {
            let outcome = RecordOutcome::Insert { stamp };
            let record = IdempotencyRecord::written(keyed, stamp, self.node_id(), outcome);
            if let Err(e) = self.put_record(&txn, coll.id, &keyed.scope, None, &record) {
                txn.abort()?;
                return Err(e);
            }
        }
        txn.commit()?;
""",
             """        let stamp = entry.stamp;
        txn.commit()?;
        if let Some(keyed) = keyed {
            let outcome = RecordOutcome::Insert { stamp };
            let record = IdempotencyRecord::written(keyed, stamp, self.node_id(), outcome);
            let txn = self.begin_write(WriterHolder::Write)?;
            self.put_record(&txn, coll.id, &keyed.scope, None, &record)?;
            txn.commit()?;
        }
"""),
            ("crates/kimmy-storage/src/modify.rs",
             """            if let Err(e) =
                self.put_record(&txn, coll.id, &run.keyed.scope, run.live.as_ref(), &record)
            {
                txn.abort().map_err(none)?;
                return Err(none(e));
            }
        }

        match txn.commit() {
""",
             """            let live = run.live.clone();
            let scope = run.keyed.scope;
            return match txn.commit() {
                Ok(()) => {
                    let txn = self.begin_write(holder).map_err(none)?;
                    self.put_record(&txn, coll.id, &scope, live.as_ref(), &record).map_err(none)?;
                    txn.commit().map_err(none)?;
                    Ok(found(entries))
                }
                Err(e) => Err(none(e)),
            };
        }

        match txn.commit() {
"""),
        ],
        "suites": MODEL,
    },
    {
        "name": "store-only-on-a-win",
        "group": "design",
        "doc": "The receiver stores a frame's record only when the entry wins",
        "patches": [
            ("crates/kimmy-storage/src/sync.rs",
             "        if frame.owes(collection.id, entry.stamp) {\n",
             "        if frame.owes(collection.id, entry.stamp) && matches!(applied, RemoteApplied::Applied { .. }) {\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "resume-without-the-stamp-rule",
        "group": "design",
        "doc": "A resumed attempt writes every match: no `first` stamp rule",
        "patches": [
            ("crates/kimmy-storage/src/modify.rs",
             "        let first = run.as_deref().and_then(|run| run.live.as_ref()).map(|live| live.first);\n",
             "        let first = if outcome.resumed { None } else { run.as_deref().and_then(|run| run.live.as_ref()).map(|live| live.first) };\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "stamp-rule-only-on-resume",
        "group": "design",
        "doc": "The stamp rule applied only to resumed attempts, not to a fresh attempt's later chunks (Chris)",
        "patches": [
            ("crates/kimmy-storage/src/modify.rs",
             "        let first = run.as_deref().and_then(|run| run.live.as_ref()).map(|live| live.first);\n",
             "        let first = if outcome.resumed { run.as_deref().and_then(|run| run.live.as_ref()).map(|live| live.first) } else { None };\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "page-record-not-witnessed",
        "group": "design",
        "doc": "A snapshot or converge page's record stored without witnessing its stamps",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             """            None => record.clone(),
        };
        self.witness(&merged.first);
        self.witness(&merged.last);
        if stored.as_ref() == Some(&merged)
""",
             """            None => record.clone(),
        };
        if carrying.is_some() {
            self.witness(&merged.first);
            self.witness(&merged.last);
        }
        if stored.as_ref() == Some(&merged)
"""),
        ],
        "suites": MODEL,
    },
    {
        "name": "snapshot-without-records",
        "group": "design",
        "doc": "A snapshot (scoped, converge or whole) stores none of its records",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "        let mut stored = 0usize;\n        for wire in records {\n",
             "        let mut stored = 0usize;\n        for wire in records.iter().take(0) {\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "merge-by-first-arrival",
        "group": "design",
        "doc": "A receiver keeps the record it holds: records merged by first arrival",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             """        let merged = match live {
            Some(live) => live.merge(record),
            None => record.clone(),
        };
        self.witness(&merged.first);""",
             """        let merged = match live {
            Some(live) => live.clone(),
            None => record.clone(),
        };
        self.witness(&merged.first);"""),
        ],
        "suites": MODEL,
    },
    {
        "name": "merge-keeps-the-winners-first",
        "group": "design",
        "doc": "The merge keeps the winning record's `first`",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "        Self { first, contested, ..winner.clone() }\n",
             "        let _ = first;\n        Self { contested, ..winner.clone() }\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "resume-hlc-without-records",
        "group": "design",
        "doc": "`resume_hlc` without the records' high water",
        "patches": [
            ("crates/kimmy-storage/src/engine.rs",
             "            .max(Self::records_high_water(db)?)\n            .max(crate::idempotency::changes_high_water_at_open(db)?)\n",
             "            .max(crate::idempotency::changes_high_water_at_open(db)?)\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "row-at-carrying-only",
        "group": "design",
        "doc": "A received record rowed at its carrying stamp only, not at its `last` (a page store gets no row)",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             """        put_rows(self, txn, collection, scope, &merged, carrying)?;
        Ok((Some(merged), true))
""",
             """        log_change(self, txn, collection, scope)?;
        txn.open_table(IDEMPOTENCY)?
            .insert((collection.0, scope.as_bytes().as_slice()), merged.encode().as_slice())?;
        if let Some(carrying) = carrying {
            txn.open_table(IDEMPOTENCY_BY_STAMP)?
                .insert(row_key(&carrying).as_slice(), row_value(collection, scope).as_slice())?;
        }
        Ok((Some(merged), true))
"""),
        ],
        "suites": MODEL,
    },
    {
        "name": "random-id-for-keyed-inserts",
        "group": "design",
        "doc": "A keyed insert without `_id` gets a random ObjectId (insert and bulk)",
        "patches": [
            ("crates/kimmy-storage/src/docs.rs",
             """            if !doc.contains_key(ID_FIELD) {
                doc.insert(ID_FIELD, keyed.derived_id(coll.id, 0));
            }
""",
             """            let _ = keyed;
"""),
            ("crates/kimmy-storage/src/docs.rs",
             """            if let Some(keyed) = keyed
                && !doc.contains_key(ID_FIELD)
            {
                doc.insert(ID_FIELD, keyed.derived_id(coll.id, index as u64));
            }
""",
             """            let _ = (keyed, index);
"""),
        ],
        "suites": MODEL,
    },
    {
        "name": "derivation-without-the-fingerprint",
        "group": "design",
        "doc": "The derived `_id` hashes no fingerprint",
        "patches": [
            ("crates/kimmy-core/src/idempotency.rs",
             "    hash.update(scope.0);\n    hash.update(fingerprint.0);\n",
             "    hash.update(scope.0);\n    let _ = fingerprint;\n"),
        ],
        "suites": MODEL,
    },
    # life-rule-off: retired with the life rule (records outlive a drop): no record is history now,
    # by design; the row A1 plants the life rule back at the lookup, its inverse.
    {
        "name": "records-at-shipped-stamps-only",
        "group": "fix-e",
        "doc": "A window ships only the records whose row is one of its entries' stamps (before fix (e))",
        "patches": [
            ("crates/kimmy-storage/src/sync.rs",
             "        crate::idempotency::records_for_coverage(txn, &floor, &reach, &servable)\n",
             """        let (records, _) =
            crate::idempotency::records_for_coverage(txn, &floor, &reach, &servable)?;
        let shipped: std::collections::HashSet<Stamp> =
            window.entries.iter().map(|entry| entry.stamp).collect();
        let records =
            records.into_iter().filter(|r| r.carrying.is_some_and(|at| shipped.contains(&at))).collect();
        Ok((records, 0))
"""),
        ],
        "suites": MODEL,
    },
    {
        "name": "f2-without-store-on-coverage",
        "group": "fix-e",
        "doc": "A frame's record is stored only in a run that takes an entry of its collection",
        "patches": [
            ("crates/kimmy-storage/src/sync.rs",
             "        if frame.owes_any() {\n",
             "        if false && frame.owes_any() {\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "no-f3",
        "group": "fix-e",
        "doc": "No record a sender cannot vouch for by its oplog is shipped (F3 off)",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "    records_unanchored(&rows, &records, floor, servable, &mut shipping)?;\n",
             "    let _ = servable;\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "no-row-at-first",
        "group": "fix-e",
        "doc": "A record is rowed at its last (and a carrying stamp), never at its first (F1 off)",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "    let mut stamps = vec![record.first, record.last];\n",
             "    let mut stamps = vec![record.last];\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "retention-drops-a-live-first-row",
        "group": "fix-e",
        "doc": "Retention removes the row at the first of a record still inside its window",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "                            record.first == stamp && record.last.hlc >= cutoff,\n",
             "                            false,\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "lookahead-off",
        "group": "fix-e",
        "doc": "No lookahead: a request whose end follows a full chunk is marked done by a record-only commit (G8)",
        "patches": [
            ("crates/kimmy-storage/src/modify.rs",
             "        if !ends && let Some(run) = run.as_deref() {\n",
             "        if false && !ends && let Some(run) = run.as_deref() {\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "lookahead-counts-left-matches-as-writable",
        "group": "fix-e",
        "doc": "The lookahead takes a match the stamp rule leaves for one the request could write",
        "patches": [
            ("crates/kimmy-storage/src/modify.rs",
             "        let writable = |found: &Match| first.is_none_or(|first| found.stamp < first);\n",
             "        let writable = |found: &Match| {\n            let _ = (first, found);\n            true\n        };\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "last-is-the-commits-first-entry",
        "group": "fix-e",
        "doc": "A record's last is its latest commit's first entry, not its last (L1 reverted)",
        "patches": [
            ("crates/kimmy-storage/src/modify.rs",
             "            let (first_entry, last_entry) = (entries[0].stamp, entries[entries.len() - 1].stamp);\n",
             "            let (first_entry, last_entry) = (entries[0].stamp, entries[0].stamp);\n"),
            ("crates/kimmy-storage/src/docs.rs",
             "            let (first, last) = (stamps[0], stamps[stamps.len() - 1]);\n",
             "            let (first, last) = (stamps[0], stamps[0]);\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "replay-pages-filtered-by-shipped-entries",
        "group": "fix-e",
        "doc": "A replay page carries only the records whose carrying stamp is one of its entries (records_of)",
        "patches": [
            ("crates/kimmy-cluster/src/transport.rs",
             '    let unanchored = lead.len();\n    let mut all = lead;\n    all.extend(rest.into_values());\n',
             '    let shipped: HashSet<kimmy_core::Stamp> = entries.iter().map(|entry| entry.stamp).collect();\n    let mut all: Vec<_> = lead\n        .into_iter()\n        .chain(rest.into_values())\n        .filter(|record| record.carrying.is_some_and(|at| shipped.contains(&at)))\n        .collect();\n    let unanchored = 0;\n'),
        ],
        "suites": [("kimmy-cluster", "--lib", "replay_tests", "no")],
    },
    {
        "name": "replay-without-the-barrier",
        "group": "design",
        "doc": "A coalesced replay answers without waiting for the flush that makes its record durable (F3)",
        "patches": [
            ("crates/kimmy-storage/src/engine.rs",
             """        if waits {
            blocking(|| engine.wait_for_flush(ticket))
                .map_err(|e| StorageError::OutcomeUnknown(e.to_string()))?;
        }
""",
             """        let _ = (waits, ticket);
"""),
        ],
        "suites": [("kimmy-storage", "--lib", "idempotency_tests::f3", "no")],
    },
    # purge-removes-every-record-under-the-id: retired with P1's purge of records (records
    # outlive a drop): a drop's purge leaves every record; the row A2 plants a purge that removes them.
    {
        "name": "ships-a-record-at-an-earlier-lifes-row",
        "group": "lives",
        "doc": "A sender ships a record at a row below its first, one an earlier record under its key (past its window, overwritten) left",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "        if first.is_some_and(|first| carrying < first) {\n            return Ok(());\n        }\n",
             ""),
        ],
        "suites": MODEL,
    },
    # stores-a-record-with-an-earlier-entry: retired as equivalent under ruling A. Storing
    # a frame's record with an earlier entry of its collection than the one at its row puts
    # the record on the receiver before its effect, which ruling A allows (I2 is effect =>
    # record), so the mutant changes no observable behaviour; its rationale, a record judged
    # by the life of a drop between the two runs, went with the life rule.
    {
        "name": "stores-records-past-the-stop",
        "group": "lives",
        "doc": "A batch that ends at an entry stores the frame's records whose row is past it",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "            if *done || carrying.is_some_and(|row| stop.is_some_and(|stop| row >= stop)) {\n",
             "            if *done || (carrying.is_some() && stop.is_some() && false) {\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": 'delta-off',
        "group": "delta",
        "doc": "P's M01: delta off: no page carries a delta",
        "patches": [
            ('crates/kimmy-storage/src/snapshot.rs',
             '            && let Some(base) = cursor.records_base\n        {\n',
             '            && let Some(base) = cursor.records_base.filter(|_| false)\n        {\n'),
        ],
        "suites": MODEL_ONLY,
        "timeout": 600,
    },
    {
        "name": 'delta-only-on-the-completing-page',
        "group": "delta",
        "doc": "P's M02: delta only on the completing page (non-final pages carry none, base frozen at the first page's)",
        "patches": [
            ('crates/kimmy-storage/src/snapshot.rs',
             '        let (records, documents, mut deleted_documents, next) = self.snapshot_rows(\n            after,\n            scope,\n            self.snapshot_rows_room(fixed),\n            first,\n            &mut records_epoch,\n        )?;\n',
             '        let room = self.snapshot_rows_room(fixed);\n        let (records, documents, mut deleted_documents, next) = {\n            let whole = self.snapshot_rows(after.clone(), scope, room, first, &mut records_epoch)?;\n            if whole.3.is_some() {\n                let frozen = after.as_ref().and_then(|a| a.records_base);\n                let stripped = after.clone().map(|c| SnapshotCursor { records_base: None, ..c });\n                let mut part = self.snapshot_rows(stripped, scope, room, first, &mut records_epoch)?;\n                part.3 = part.3.map(|c| SnapshotCursor { records_base: frozen.or(c.records_base), ..c });\n                part\n            } else {\n                whole\n            }\n        };\n'),
        ],
        "suites": MODEL_ONLY,
        "timeout": 600,
    },
    {
        "name": 'delta-base-never-advances',
        "group": "delta",
        "doc": "P's M03x: base from the wrong page: the cursor keeps the base it came with (never advances)",
        "patches": [
            ('crates/kimmy-storage/src/snapshot.rs',
             '        let delta_len = records.len();\n',
             '        let delta_len = records.len();\n        let frozen = after.as_ref().and_then(|a| a.records_base);\n'),
            ('crates/kimmy-storage/src/snapshot.rs',
             '            cursor.map(|cursor| SnapshotCursor { records_base: Some(cut_base), ..cursor })',
             '            cursor.map(|cursor| SnapshotCursor { records_base: frozen.or(Some(cut_base)), ..cursor })'),
        ],
        "suites": MODEL_ONLY,
        "timeout": 600,
    },
    {
        "name": 'delta-base-read-outside-the-documents-txn',
        "group": "delta",
        "doc": "P's M05: next base read outside the documents' transaction (after the page's read)",
        "patches": [
            ('crates/kimmy-storage/src/snapshot.rs',
             '        crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::PageRead);\n',
             '        crate::sync::race_hooks::reach(crate::sync::race_hooks::Race::PageRead);\n        let later = {\n            let t = self.db().begin_read()?;\n            let begun = crate::idempotency::changes_begin(&t)?;\n            let high = crate::idempotency::changes_high_water(&t)?.map_or(Hlc::ZERO, |high| high.hlc);\n            RecordsBase { key: Stamp::new(high, self.node_id()), epoch: begun.map(|begun| begun.epoch) }\n        };\n        let next = next.map(|cursor| SnapshotCursor {\n            records_base: cursor.records_base.and(Some(later)),\n            ..cursor\n        });\n'),
        ],
        "suites": MODEL_ONLY,
        "timeout": 600,
    },
    {
        "name": 'page-stores-log-no-change',
        "group": "delta",
        "doc": "P's M06: page stores write no log row",
        "patches": [
            ('crates/kimmy-storage/src/idempotency.rs',
             '    log_change(engine, txn, collection, scope)?;',
             '    if carrying.is_some() || record.origin == engine.node_id() {\n        log_change(engine, txn, collection, scope)?;\n    }'),
        ],
        "suites": MODEL_ONLY,
        "timeout": 600,
    },
    {
        "name": 'frame-stores-log-no-change',
        "group": "delta",
        "doc": "P's M07: frame stores (pull, push, replay, F2/F3) write no log row",
        "patches": [
            ('crates/kimmy-storage/src/idempotency.rs',
             '    log_change(engine, txn, collection, scope)?;',
             '    if carrying.is_none() {\n        log_change(engine, txn, collection, scope)?;\n    }'),
        ],
        "suites": MODEL_ONLY,
        "timeout": 600,
    },
    {
        "name": 'local-commits-log-no-change',
        "group": "delta",
        "doc": "P's M08: local commits write no log row",
        "patches": [
            ('crates/kimmy-storage/src/idempotency.rs',
             '    log_change(engine, txn, collection, scope)?;',
             '    if carrying.is_some() || record.origin != engine.node_id() {\n        log_change(engine, txn, collection, scope)?;\n    }'),
        ],
        "suites": MODEL_ONLY,
        "timeout": 600,
    },
    {
        "name": 'records-only-page-moves-the-document-position',
        "group": "delta",
        "doc": "P's M13: a records-only page moves the cursor's document position",
        "patches": [
            ('crates/kimmy-storage/src/snapshot.rs',
             '                    let cursor = SnapshotCursor { records_base: Some(base), ..cursor.clone() };',
             '                    let cursor = SnapshotCursor {\n                        records_base: Some(base),\n                        after_key: Vec::new(),\n                        records: None,\n                        ..cursor.clone()\n                    };'),
        ],
        "suites": MODEL_ONLY,
        "timeout": 600,
    },
    {
        "name": "resume-hlc-without-the-change-log",
        "group": "delta",
        "doc": "`resume_hlc` without the change log's high water (its keys are minted stamps)",
        "patches": [
            ("crates/kimmy-storage/src/engine.rs",
             "            .max(Self::records_high_water(db)?)\n            .max(crate::idempotency::changes_high_water_at_open(db)?)\n",
             "            .max(Self::records_high_water(db)?)\n"),
        ],
        "suites": MODEL,
        "timeout": 600,
    },
    {
        "name": "header-guard-not-over-mcp",
        "group": "elsewhere",
        "doc": "The header guard lets `/mcp` through",
        "patches": [
            ("crates/kimmy-api/src/routes.rs",
             """    if !request.headers().contains_key(crate::idempotency::IDEMPOTENCY_KEY) {
        return next.run(request).await;
    }
""",
             """    if !request.headers().contains_key(crate::idempotency::IDEMPOTENCY_KEY)
        || request.uri().path() == "/mcp"
    {
        return next.run(request).await;
    }
"""),
        ],
        "suites": [("kimmy-api", "--test idempotency", "every_other_route_refuses_the_header_by_name", "no")],
    },
    {
        "name": "operator-switch-ungated",
        "group": "elsewhere",
        "doc": "`cluster.idempotency_keys` ignored",
        "patches": [
            ("crates/kimmy-cluster/src/idempotency.rs",
             "        if !self.switch {\n            return Err(IdempotencyClosed::Undeclared);\n        }\n",
             "        let _ = self.switch;\n"),
        ],
        "suites": [
            ("kimmy-api", "--test idempotency", "with_the_switch_off_a_key_is_refused_naming_the_switch", "no"),
            ("kimmy-cluster", "--lib", "idempotency", "no"),
        ],
    },
    {
        "name": "live-peer-net-removed",
        "group": "elsewhere",
        "doc": "The live-peer net never blocks",
        "patches": [
            ("crates/kimmy-cluster/src/idempotency.rs",
             "        match crate::facts::idempotency_records_net(&members.view(), now) {\n",
             "        match { let _ = (members, now); PeerNet::Open } {\n"),
        ],
        "suites": [
            ("kimmy-cluster", "--test idempotency", "c9_a_live_member_that_does_not_carry_records_closes_the_gate", "no"),
        ],
        "kills": ["c9_a_live_member_that_does_not_carry_records_closes_the_gate"],
    },
] + GATE_SPLIT

# The fixes after the review of the merged head, and records outliving a drop, as the model sees them: each row's killer
# is a model row or the generated sweep, run against the model alone.
FIXES = [
    ("contested-without-the-fingerprints", "A merge of two fingerprints is not contested (the contested flag's fingerprint half)",
     "crates/kimmy-storage/src/idempotency.rs",
     "        let contested = self.contested || other.contested || self.fingerprint != other.fingerprint;\n",
     "        let contested = self.contested || other.contested;\n"),
    ("contested-from-the-winner", "A merged record is contested only when its winner is (the flag in the tie-break)",
     "crates/kimmy-storage/src/idempotency.rs",
     "        let contested = self.contested || other.contested || self.fingerprint != other.fingerprint;\n",
     "        let contested = winner.contested;\n"),
    ("lookup-ignores-contested", "The lookup replays or resumes a contested record of its own fingerprint",
     "crates/kimmy-storage/src/idempotency.rs",
     "        self.witness(&record.last);\n        if record.contested || record.fingerprint != keyed.fingerprint {\n",
     "        self.witness(&record.last);\n        if record.fingerprint != keyed.fingerprint {\n"),
    ("later-chunk-ignores-contested", "A later chunk writes on a record that turned contested since the chunk before",
     "crates/kimmy-storage/src/idempotency.rs",
     "            self.witness(&record.last);\n            if record.contested || record.fingerprint != keyed.fingerprint {\n",
     "            self.witness(&record.last);\n            if record.fingerprint != keyed.fingerprint {\n"),
    ("past-window-record-stored", "A receiver stores a record past its window, merged into a new request's",
     "crates/kimmy-storage/src/idempotency.rs",
     "        if self.record_is_past_window(record)\n            && !live.is_some_and(|live| live.fingerprint == record.fingerprint)\n        {\n            return Ok((None, false));\n        }\n",
     ""),
    ("past-window-same-request-refused", "A record past its window of the live record's own request is refused, not merged (as first built)",
     "crates/kimmy-storage/src/idempotency.rs",
     "        if self.record_is_past_window(record)\n            && !live.is_some_and(|live| live.fingerprint == record.fingerprint)\n        {\n",
     "        if self.record_is_past_window(record) {\n"),
    ("zero-base-guard-off", "A page asked with the zero records base is applied though its sender's log began since (the zero-base guard)",
     "crates/kimmy-storage/src/snapshot.rs",
     "    (zero && page.records_epoch.is_some()).then(|| SnapshotCursor {\n",
     "    (zero && page.records_epoch.is_some() && base.key.hlc != Hlc::ZERO).then(|| SnapshotCursor {\n"),
    ("no-log-begun-over-restored-records", "An open over records with no log of this node begins none (the epoch begun at open)",
     "crates/kimmy-storage/src/engine.rs",
     "        crate::idempotency::begin_log_over_held_records(&engine)?;\n",
     ""),
    ("replay-page-records-off", "A replay page carries no record of its collections (the spanning records off, in storage)",
     "crates/kimmy-storage/src/idempotency_replay.rs",
     "                if record.last.hlc <= from\n",
     "                if record.last.hlc <= Hlc::MAX\n"),
    ("replay-records-carried-at-last", "The spanning records carried at their last, not their first",
     "crates/kimmy-storage/src/idempotency_replay.rs",
     "                    carrying: Some(record.first),\n",
     "                    carrying: Some(record.last),\n"),
    ("replay-cursor-epoch-unguarded", "A replay's records cursor of another epoch of the sender's log is honoured",
     "crates/kimmy-storage/src/idempotency_replay.rs",
     "                Some(epoch) => begun.is_some_and(|begun| begun.epoch == epoch),\n",
     "                Some(_) => begun.is_some(),\n"),
    ("replay-cursor-passes-changed", "A replay's records cursor passes a record the log names as changed since its base",
     "crates/kimmy-storage/src/idempotency_replay.rs",
     "                && cursor.passed(record.collection, &record.scope)\n                && !changed.contains(&(record.collection.0, record.scope.0));\n",
     "                && cursor.passed(record.collection, &record.scope);\n"),
    ("rewind-leaves-the-rows", "A rewind removes the records of what it discards and leaves their rows by stamp",
     "crates/kimmy-storage/src/idempotency.rs",
     "        if let Some(from) = from {\n            remove_rows_naming(txn, from, &named)?;\n        }\n",
     "        let _ = (from, &named);\n"),
    ("rewind-reopens-a-bulk", "A rewind reopens a cut bulk insert's record as it reopens every other kind's",
     "crates/kimmy-storage/src/idempotency.rs",
     "            if record.first.hlc > until || (cut && bulk) {\n",
     "            if record.first.hlc > until {\n"),
    ("rewind-removes-every-cut-kind", "A rewind removes a cut record of every kind but a filter's (the rule before G narrowed it to a bulk)",
     "crates/kimmy-storage/src/idempotency.rs",
     "            if record.first.hlc > until || (cut && bulk) {\n",
     "            if record.first.hlc > until || (cut && !matches!(record.outcome, RecordOutcome::Modify(_))) {\n"),
    ("restore-marks-no-held-rows", "A restore marks none of the rows its backup's source held above its vectors (BR1, before #211)",
     "crates/kimmy-storage/src/backup.rs",
     "                if stamp.hlc > vector.get(stamp.node) {\n",
     "                if false && stamp.hlc > vector.get(stamp.node) {\n"),
    ("rewind-lowers-to-the-oplog", "A rewind lowers each vector to the oplog's newest entry of its origin, below what retention collected (RW1, before #209)",
     "crates/kimmy-storage/src/engine.rs",
     "                let mut after = before.min(until);\n",
     "                let mut after = before.min(until).min(kept.get(node));\n"),
    ("delete-judged-by-its-own-stamp", "A keyed delete by id is judged by its outcome's stamp, not the record's first (before G)",
     "crates/kimmy-storage/src/docs.rs",
     "        let first = record.first;\n        let key = doc_key(id)?;\n",
     "        let first = match &record.outcome {\n            RecordOutcome::Delete { stamp: Some(s), .. } => *s,\n            _ => record.first,\n        };\n        let key = doc_key(id)?;\n"),
    ("rewind-keeps-the-cursor", "A rewind reopens a filter's record with its cursor (a resume skips what it undid)",
     "crates/kimmy-storage/src/idempotency.rs",
     "                    m.cursor = None;\n",
     "                    let _ = m;\n"),
]
for name, doc, path, old, new in FIXES:
    ROWS.append({"name": name, "group": "fixes", "doc": doc, "patches": [(path, old, new)], "suites": MODEL_ONLY})


def row(name):
    for r in ROWS:
        if r["name"] == name:
            return r
    sys.exit(f"no row {name}")


def apply(r, tree, write):
    texts = {}
    for path, old, new in r["patches"]:
        full = f"{tree}/{path}"
        text = texts.get(full)
        if text is None:
            text = open(full).read()
        n = text.count(old)
        if n != 1:
            return f"{r['name']}: {path}: the text occurs {n} times"
        texts[full] = text.replace(old, new)
    if write:
        for full, text in texts.items():
            open(full, "w").write(text)
    return None


def main():
    cmd = sys.argv[1]
    if cmd == "list":
        group = sys.argv[2] if len(sys.argv) > 2 else "all"
        for r in ROWS:
            if group in ("all", r["group"]):
                print(r["name"])
    elif cmd == "check":
        bad = [e for e in (apply(r, sys.argv[2], False) for r in ROWS) if e]
        for e in bad:
            print(e)
        sys.exit(1 if bad else 0)
    elif cmd == "apply":
        e = apply(row(sys.argv[2]), sys.argv[3], True)
        if e:
            sys.exit(e)
    elif cmd == "files":
        for path in sorted({p for p, _, _ in row(sys.argv[2])["patches"]}):
            print(path)
    elif cmd == "suites":
        for crate, target, filt, ignored in row(sys.argv[2])["suites"]:
            print(f"{crate}|{target}|{filt}|{ignored}")
    elif cmd == "timeout":
        print(row(sys.argv[2]).get("timeout", ""))
    elif cmd == "doc":
        print(row(sys.argv[2])["doc"])
    elif cmd == "kills":
        for name in row(sys.argv[2]).get("kills", []):
            print(name)
    else:
        sys.exit(f"unknown command {cmd}")


main()
