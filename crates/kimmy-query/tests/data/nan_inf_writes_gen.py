#!/usr/bin/env python3
"""Rebuild nan_inf_writes.tsv from the reference recording q7s3k (the write cases, lines 9 to 66).

    python3 -I data/nan_inf_writes_gen.py <reference-6.0.5 directory> > data/nan_inf_writes.tsv

One row per `update`, `delete` and `findAndModify` record whose FILTER holds a NaN, a number or an
infinity: `label`, `kind`, the documents before (`beforeOrdered`), the command (`commandOrdered`)
and the outcome as canonical extended JSON in the order recorded: `n` (`reply.n`, or
`reply.lastErrorObject.n` of a findAndModify) and `after` (`afterOrdered`, the collection
afterwards). The `_id` of a document an upsert makes is a generated ObjectId and is not compared.
`reply` carries no returned document for these findAndModify records.
"""
import json
import sys

ROWS = 0  # the rows written: the test asserts this count


def flat(v):
    if isinstance(v, dict):
        if list(v) == ["$ordered"]:
            return {k: flat(x) for k, x in v["$ordered"]}
        return {k: flat(x) for k, x in v.items()}
    if isinstance(v, list):
        return [flat(x) for x in v]
    return v


base = sys.argv[1].rstrip("/") + "/"
for n, line in enumerate(open(base + "q7s3k.jsonl"), 1):
    r = json.loads(line)
    if r["kind"] not in ("update", "delete", "findAndModify"):
        continue
    rep = r["reply"]
    count = rep["lastErrorObject"]["n"] if r["kind"] == "findAndModify" else rep["n"]
    outcome = {"n": count, "after": flat(r["afterOrdered"])}
    ROWS += 1
    print(
        "\t".join(
            [
                r["label"].split()[0],
                r["kind"],
                json.dumps(flat(r["beforeOrdered"])),
                json.dumps(flat(r["commandOrdered"])),
                json.dumps(outcome),
            ]
        )
    )
print(f"nan_inf_writes: {ROWS} cases", file=sys.stderr)
