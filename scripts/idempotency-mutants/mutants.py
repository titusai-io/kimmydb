#!/usr/bin/env python3
"""The B5 idempotency mutation rows: each is an exact-text patch of the post-format
source, applied once to a `git archive` export, run against its suites under a timeout,
and reverted by copying the pristine file back. A row whose text does not occur exactly
once fails `check` and is never run.

    mutants.py list [group]            names of the rows (group: design, fix-e, elsewhere, all)
    mutants.py check <tree>            every row applies to <tree>, exactly once
    mutants.py apply <name> <tree>     patch <tree> in place
    mutants.py files <name>            the files a row touches
    mutants.py suites <name>           the suites a row runs, one per line:
                                       <crate> <target> <filter> <ignored: yes|no>
"""
import sys

# The storage crate's idempotency tests: the model's CI sweep, its named rows and
# reproducers, and the unit, replication and coverage tests. What is ignored (the long
# run, rows waiting for a fix not on the tree) is left out.
MODEL = [("kimmy-storage", "--lib", "idempotency", "no")]

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
             "        if frame.owes(collection.id) {\n",
             "        if frame.owes(collection.id) && matches!(applied, RemoteApplied::Applied { .. }) {\n"),
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
             "        Ok(tail.max(named).max(Self::metadata_high_water(db)?).max(Self::records_high_water(db)?))\n",
             "        Ok(tail.max(named).max(Self::metadata_high_water(db)?))\n"),
        ],
        "suites": MODEL,
    },
    {
        "name": "row-at-carrying-only",
        "group": "design",
        "doc": "A received record rowed at its carrying stamp only, not at its `last` (a page store gets no row)",
        "patches": [
            ("crates/kimmy-storage/src/idempotency.rs",
             """        put_rows(txn, collection, scope, &merged, carrying)?;
        Ok((Some(merged), true))
""",
             """        txn.open_table(IDEMPOTENCY)?
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
             "        if ahead.iter().any(|found| first.is_none_or(|first| found.stamp < first)) {\n",
             "        let _ = first;\n        if !ahead.is_empty() {\n"),
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
    elif cmd == "doc":
        print(row(sys.argv[2])["doc"])
    else:
        sys.exit(f"unknown command {cmd}")


main()
