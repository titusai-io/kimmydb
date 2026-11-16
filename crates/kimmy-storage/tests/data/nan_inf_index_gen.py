#!/usr/bin/env python3
"""Rebuild nan_inf_index.tsv from the reference recording q7s3k (the `index` cases, lines 1-8 and 67-93).

    python3 -I data/nan_inf_index_gen.py <reference-6.0.5 directory> > data/nan_inf_index.tsv

One row per `index` record: `label`, `mode` (`pre`: the documents exist when the index is built;
`docs`: they are inserted one by one after it), `unique` (0 or 1), the partial filter (or `-`), the
documents, the queries and the sorts, and the expected outcome. The documents, queries and sorts are
read from `commandOrdered` and the answers from `reply`: `reply.create` (the error code of a failed
build, or 0), `reply.inserts` (the code of each insert, 0 or 11000), `reply.queries[]` (`ids`,
`idsUnhinted`, `countUnhinted`: `null` where the recording holds an error) and `reply.sorted[]`
(`idsHinted`, `idsUnhinted`, and `tied`, set from the sort spec: true when its last key is not
`_id`, so that the test compares the groups of equal keys as sets). `before` is not read. Canonical extended JSON, in the order recorded.
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


def ids(v):
    return None if v is None else [int(x["$numberInt"]) for x in v]


base = sys.argv[1].rstrip("/") + "/"
for n, line in enumerate(open(base + "q7s3k.jsonl"), 1):
    r = json.loads(line)
    if r["kind"] != "index":
        continue
    c, rep = flat(r["commandOrdered"]), flat(r["reply"])
    mode = "pre" if "preDocs" in c else "docs"
    docs = c.get("preDocs", c.get("docs"))
    opts = c["index"]["options"]
    create = rep["create"].get("error", {}).get("code", 0)
    expected = {
        "create": create,
        "inserts": rep.get("inserts", []),
        "queries": [
            {
                "ids": ids(q.get("ids")),
                "idsUnhinted": ids(q.get("idsUnhinted")),
                "countUnhinted": q.get("countUnhinted"),
            }
            for q in rep.get("queries", [])
        ],
        "sorted": [
            {
                "idsHinted": ids(s.get("idsHinted")),
                "idsUnhinted": ids(s.get("idsUnhinted")),
                # a sort whose last key is not `_id` has no unique last key: its ties are compared as sets
                "tied": list(sort)[-1] != "_id",
            }
            for s, sort in zip(rep.get("sorted", []), c.get("sorts", []))
        ],
    }
    ROWS += 1
    print(
        "\t".join(
            [
                r["label"].split()[0],
                mode,
                "1" if opts.get("unique") else "0",
                json.dumps(opts["partialFilterExpression"]) if "partialFilterExpression" in opts else "-",
                json.dumps(docs),
                json.dumps(c.get("queries", [])),
                json.dumps(c.get("sorts", [])),
                json.dumps(expected),
            ]
        )
    )
print(f"nan_inf_index: {ROWS} cases", file=sys.stderr)
