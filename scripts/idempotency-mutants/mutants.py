#!/usr/bin/env python3
"""The B5 idempotency mutation rows: each is an exact-text patch of the post-format
source, applied once to a `git archive` export, run against its suites under a timeout,
and reverted by copying the pristine file back. A row whose text does not occur exactly
once fails `check` and is never run.

    mutants.py list [group]            names of the rows (group: design, fix-e, lives, delta, elsewhere, all)
    mutants.py check <tree>            every row applies to <tree>, exactly once
    mutants.py apply <name> <tree>     patch <tree> in place
    mutants.py files <name>            the files a row touches
    mutants.py timeout <name>          the row's own suite timeout in seconds, if it has one
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

ROWS = [
    {
        "name": "record-in-a-later-txn",
        "group": "design",
        "doc": "The record written in a transaction after the write (insert and filtered write)",
        "patches": [
            ("crates/kimmy-storage/src/docs.rs",
             """        let stamp = entry.stamp;
        // In the write's own transaction: the record survives exactly what the
        // write survives (B5).
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
        if live.as_ref() == Some(&merged)
""",
             """            None => record.clone(),
        };
        if carrying.is_some() {
            self.witness(&merged.first);
            self.witness(&merged.last);
        }
        if live.as_ref() == Some(&merged)
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
             """        let merged = match &live {
            Some(live) => live.merge(record),
            None => record.clone(),
        };
        self.witness(&merged.first);""",
             """        let merged = match &live {
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
             "        Self { first, ..winner.clone() }\n",
             "        let _ = first;\n        winner.clone()\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "resume-hlc-without-records",
        "group": "design",
        "doc": "`resume_hlc` without the records' high water",
        "patches": [
            ("crates/kimmy-storage/src/engine.rs",
             "            .max(Self::records_high_water(db)?)\n            .max(crate::idempotency::changes_high_water_at_open(db)?))\n",
             "            .max(crate::idempotency::changes_high_water_at_open(db)?))\n"),
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
    {
        "name": "life-rule-off",
        "group": "design",
        "doc": "No record is ever history: the life rule off",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "        self.is_history(memo, standing, collection, record.last)\n",
             "        let _ = (memo, standing, collection, record);\n        Ok(false)\n"),
        ],
        "suites": MODEL,
    },
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
             """                    through.max(window.scanned_to),
                    records_to_send(engine, window.records),
                    window.unanchored,
                );
""",
             """                    through.max(window.scanned_to),
                    {
                        let shipped: std::collections::HashSet<kimmy_core::Stamp> =
                            entries.iter().map(|entry| entry.stamp).collect();
                        records_to_send(engine, window.records)
                            .into_iter()
                            .filter(|r| r.carrying.is_some_and(|at| shipped.contains(&at)))
                            .collect()
                    },
                    0,
                );
"""),
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
    {
        "name": "purge-removes-every-record-under-the-id",
        "group": "lives",
        "doc": "The drop purger removes every record under the collection's id, a later life's too (P1 reverted)",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "                    Ok(Some(record)) => dropped.is_none_or(|at| record.last.hlc <= at.hlc),\n",
             "                    Ok(Some(_)) => true,\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "ships-a-record-at-an-earlier-lifes-row",
        "group": "lives",
        "doc": "A sender ships a record at a row below its first, one an earlier life under its key left",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "            && carrying < standing.first\n",
             "            && carrying < standing.first\n            && carrying.hlc == Hlc::MAX\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "stores-a-record-with-an-earlier-entry",
        "group": "lives",
        "doc": "A receiver stores a frame's record in the run of its collection's first entry, before its row",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             "        carrying.is_none_or(|row| row <= at)\n",
             "        let _ = (carrying, at);\n        true\n"),
        ],
        "suites": MODEL,
    },
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
             '        let (records, documents, mut deleted_documents, next) = self.snapshot_rows(after, scope)?;',
             '        let (records, documents, mut deleted_documents, next) = {\n            let whole = self.snapshot_rows(after.clone(), scope)?;\n            if whole.3.is_some() {\n                let frozen = after.as_ref().and_then(|a| a.records_base);\n                let stripped = after.clone().map(|c| SnapshotCursor { records_base: None, ..c });\n                let mut part = self.snapshot_rows(stripped, scope)?;\n                part.3 = part.3.map(|c| SnapshotCursor { records_base: frozen.or(c.records_base), ..c });\n                part\n            } else {\n                whole\n            }\n        };'),
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
             "            .max(Self::records_high_water(db)?)\n            .max(crate::idempotency::changes_high_water_at_open(db)?))\n",
             "            .max(Self::records_high_water(db)?))\n"),
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
             "        let blockers = crate::facts::idempotency_records_blockers(&members.view());\n",
             "        let blockers: Vec<kimmy_core::NodeId> = { let _ = members; Vec::new() };\n"),
        ],
        "suites": [
            ("kimmy-cluster", "--test idempotency", "c9_a_live_member_that_does_not_carry_records_closes_the_gate", "no"),
        ],
    },
]


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
    else:
        sys.exit(f"unknown command {cmd}")


main()
