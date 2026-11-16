#!/usr/bin/env python3
"""Rebuild projection_included.tsv from the reference recordings q7s4q, q7s3d and q7s4g.

    python3 -I data/projection_included_gen.py <reference-6.0.5 directory> > data/projection_included.tsv

An INCLUDED dotted path keeps its parent whatever it finds inside, for a `find` projection and for a
`$project` stage alike: a sub-document stays a document holding what was selected (empty if nothing
was), an array stays an array with one projected document per document element, scalar elements
dropped and nested arrays walked the same way; only a parent that is a scalar, null or missing is
left out.

Rows: q7s4q 1 to 12 (12 parent shapes: find and aggregate, with and without `_id: 0`), q7s3d 23, 25,
32, 35 and 41 (finds) and q7s4g 35, 97, 98 and 99 (aggregates). One row per record: `label`, `kind`
(`find` or `aggregate`), the documents (`beforeOrdered`, typed), the command (`commandOrdered`: the
`projection` of a find, the `pipeline` of an aggregate) and the answer (`reply.docs`, typed, in the
order recorded: a find's sort and an aggregate's `$sort` are on `_id`, and the documents are given in
that order). `reply.ids` of a find is not read (it is null when `_id` is excluded). Canonical
extended JSON, in the order recorded.
"""
import json
import sys

ROWS = 0  # the rows written: the test asserts this count
WANT = {"q7s4q": range(1, 13), "q7s3d": (23, 25, 32, 35, 41), "q7s4g": (35, 97, 98, 99)}


def flat(v):
    if isinstance(v, dict):
        if list(v) == ["$ordered"]:
            return {k: flat(x) for k, x in v["$ordered"]}
        return {k: flat(x) for k, x in v.items()}
    if isinstance(v, list):
        return [flat(x) for x in v]
    return v


base = sys.argv[1].rstrip("/") + "/"
for name, lines in WANT.items():
    records = [json.loads(line) for line in open(base + name + ".jsonl")]
    for n in lines:
        r = records[n - 1]
        assert r["label"].startswith(f"{name}{n:04d} "), r["label"]
        c = flat(r["commandOrdered"])
        spec = c["projection"] if r["kind"] == "find" else c["pipeline"]
        ROWS += 1
        print("\t".join([r["label"].split()[0], r["kind"], json.dumps(flat(r["beforeOrdered"])),
                         json.dumps(spec), json.dumps(flat(r["reply"])["docs"])]))
print(f"projection_included: {ROWS} cases", file=sys.stderr)
