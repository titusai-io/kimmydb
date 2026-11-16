#!/usr/bin/env python3
"""Rebuild q7s2ab.tsv from the reference recording q7s2ab (102 updates).

    python3 -I data/q7s2ab_gen.py <reference-6.0.5 directory> > data/q7s2ab.tsv

The positional `$` under `$or`, `$in`, ranges, `$and`, branches on other fields, a nested `$or`,
`$nor` and `$elemMatch`: 34 filters over `arr = [5, 1, 6, 2, 7, 6, 5]` (or a short one with a field
`b`), each as `$set`, `$inc` and `$unset` of `arr.$`. One row per record, in the format of
`update_table.tsv`: `label` (`q7s2ab` and the case number), `kind` (`update`), the documents before
(`beforeOrdered`), the command (`commandOrdered`: `q` and `u`) and the recorded outcome as canonical
extended JSON in the order recorded: `{"error": code}` where `reply.writeErrors[0]` holds one
(code 2, "the positional operator did not find the match needed from the query"), else
`{"n", "nModified", "after"}` (`reply.n`, `reply.nModified`, `afterOrdered`). The answer of a
refusal is the code only.
"""
import json
import sys

ROWS = 0  # the rows written: the test asserts this count


def typed(v):
    if isinstance(v, dict):
        if list(v) == ["$ordered"]:
            return {k: typed(x) for k, x in v["$ordered"]}
        return {k: typed(x) for k, x in v.items()}
    if isinstance(v, list):
        return [typed(x) for x in v]
    return v


base = sys.argv[1].rstrip("/") + "/"
for n, line in enumerate(open(base + "q7s2ab.jsonl"), 1):
    r = json.loads(line)
    assert r["kind"] == "update" and r["label"].startswith(f"ab{n:04d} "), r["label"]
    reply = r["reply"]
    if reply.get("writeErrors"):
        outcome = {"error": reply["writeErrors"][0]["code"]}
    else:
        outcome = {"n": reply["n"], "nModified": reply["nModified"], "after": typed(r["afterOrdered"])}
    ROWS += 1
    print("\t".join([f"q7s2ab{n:04d}", "update", json.dumps(typed(r["beforeOrdered"])),
                     json.dumps(typed(r["commandOrdered"])), json.dumps(outcome)]))
print(f"q7s2ab: {ROWS} cases", file=sys.stderr)
