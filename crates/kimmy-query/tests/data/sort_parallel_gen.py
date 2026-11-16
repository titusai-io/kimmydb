#!/usr/bin/env python3
"""Rebuild sort_parallel.tsv from the reference recording q7s4k (cases 1 to 128).

    python3 -I sort_parallel_gen.py <reference-6.0.5 directory> > sort_parallel.tsv

One row per recorded `find` and `$sort` aggregation: `case` (q7s4k, the line of the recording),
`kind` (`find` or `aggregate`), the documents, the command (`sort`, `filter`, `skip`, `limit` of a
find, or the pipeline) and the answer: `ERR` with the end of the message when the reference
refused it (code 2), or the `_id`s it answered in order.
"""
import json, sys

ROWS = 0  # the rows written: the test asserts this count

def plain(v):
    if isinstance(v, dict):
        if list(v) == ["$ordered"]:
            return {k: plain(x) for k, x in v["$ordered"]}
        if len(v) == 1:
            (k, x), = v.items()
            if k in ("$numberInt", "$numberLong"):
                return int(x)
        return {k: plain(x) for k, x in v.items()}
    if isinstance(v, list):
        return [plain(x) for x in v]
    return v

base = sys.argv[1].rstrip("/") + "/"
for n, line in enumerate(open(base + "q7s4k.jsonl"), 1):
    if not line.strip():
        continue
    r = json.loads(line)
    cmd = plain(r["commandOrdered"])
    if "u" in cmd:
        continue
    reply = r["reply"]
    if "error" in reply:
        err = reply["error"]
        assert err["code"] == 2 and err["message"].endswith("cannot sort with keys that are parallel arrays"), err
        answer = "ERR"
    else:
        docs = reply.get("ids") if "ids" in reply else [d["_id"] for d in plain(reply["docs"])]
        answer = json.dumps(plain(docs))
    ROWS += 1
    print("\t".join([f"{n:04d}", r["kind"], json.dumps(plain(r["beforeOrdered"]), ensure_ascii=False),
                     json.dumps(cmd, ensure_ascii=False), answer]))
print(f"sort_parallel: {ROWS} cases", file=sys.stderr)
