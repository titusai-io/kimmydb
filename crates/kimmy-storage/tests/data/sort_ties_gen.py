#!/usr/bin/env python3
"""Rebuild sort_ties.tsv from the reference recordings q7s3n, q7s3n2 and q7s3n3 (3 `index` records each).

    python3 -I data/sort_ties_gen.py <reference-6.0.5 directory> > data/sort_ties.tsv

The tie order of a sort with no `_id` key, over 14 documents of tied keys. One row per `index` record
(9 rows: the same script in three fresh containers). The fields read from a record:
`label`; `command.noIndex` (present on the first record: no index exists, so there is no hinted run);
`command.preDocs` or `command.docs` (the documents, in the order recorded; `before` and
`beforeOrdered` are not read); `reply.sorted[]` (`sort`, `idsUnhinted`, `idsHinted`) and
`reply.aggs[]` (`pipeline`, `idsUnhinted`, `idsHinted`). `plan` and `hintedPlan` are not read (the
engine has no planner output to compare). `idsHinted` is absent from the record without an index.

Columns: `label`, `indexed` (0 or 1), the documents, and the expected outcome as JSON:
`sorted[]` and `aggs[]`, each `{"spec": the sort, "unhinted": ids, "hinted": ids or null, "grouped": bool}`
(for an aggregation `spec` is the `$sort` of its one-stage pipeline, and `pipeline` the pipeline).
`grouped` is true for a descending sort with an index and no `_id` as its last key: the reference
reads the index backwards and reverses the ties, which the engine does not (docs/deviations.md, ties
in a sort with no unique key); the test compares such a row group by group. Every other row is exact.

q7s3n2 and q7s3n3 are checked against q7s3n here: every field of every record is equal except `set`
and `label`, or this script stops.
"""
import json
import sys

ROWS = 0


def ids(v):
    return None if v is None else [int(x["$numberInt"]) for x in v]


def grouped(indexed, spec):
    keys = list(spec)
    return indexed and keys[-1] != "_id" and int(spec[keys[0]]["$numberInt"]) == -1


base = sys.argv[1].rstrip("/") + "/"
sets = {name: [json.loads(line) for line in open(base + name + ".jsonl")] for name in ("q7s3n", "q7s3n2", "q7s3n3")}
for name in ("q7s3n2", "q7s3n3"):
    for a, b in zip(sets["q7s3n"], sets[name]):
        for k in a:
            if k not in ("set", "label") and a[k] != b[k]:
                sys.exit(f"{name} differs from q7s3n in `{k}` of {a['label']}")
for name, records in sets.items():
    for r in records:
        c, rep = r["command"], r["reply"]
        indexed = not c.get("noIndex", False)
        docs = c.get("preDocs", c.get("docs"))
        sorted_rows = [
            {"spec": s["sort"], "unhinted": ids(s["idsUnhinted"]), "hinted": ids(s.get("idsHinted")),
             "grouped": grouped(indexed, s["sort"])}
            for s in rep["sorted"]
        ]
        agg_rows = []
        for a in rep["aggs"]:
            assert len(a["pipeline"]) == 1 and list(a["pipeline"][0]) == ["$sort"], a["pipeline"]
            spec = a["pipeline"][0]["$sort"]
            agg_rows.append({"pipeline": a["pipeline"], "spec": spec, "unhinted": ids(a["idsUnhinted"]),
                             "hinted": ids(a.get("idsHinted")), "grouped": grouped(indexed, spec)})
        ROWS += 1
        print("\t".join([r["label"].split()[0], "1" if indexed else "0", json.dumps(docs),
                         json.dumps({"sorted": sorted_rows, "aggs": agg_rows})]))
print(f"sort_ties: {ROWS} cases", file=sys.stderr)
