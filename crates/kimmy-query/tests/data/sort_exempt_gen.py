#!/usr/bin/env python3
"""Rebuild sort_exempt.tsv from the reference recording q7s4l (52 cases).

    python3 -I sort_exempt_gen.py <reference-6.0.5 directory> > sort_exempt.tsv

One row per recorded `find` and `$sort` aggregation: `case` (q7s4l, the line of the recording),
`kind` (`find`, `aggregate` or `findAndModify`), the documents, the command (a find's `sort`, `filter`, `skip`, `limit` and `projection`, the pipeline, or a findAndModify's `query`, `sort` and the rest) and the answer: `ERR` with the end of the message when the reference
refused it (code 2), or the `_id`s it answered in order (`OK` for a findAndModify that answered).
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
for n, line in enumerate(open(base + "q7s4l.jsonl"), 1):
    if not line.strip():
        continue
    r = json.loads(line)
    cmd = plain(r["commandOrdered"])
    reply = r["reply"]
    if "error" in reply:
        err = reply["error"]
        assert err["code"] == 2 and err["message"].endswith("cannot sort with keys that are parallel arrays"), err
        assert (r["kind"] == "findAndModify") == err["message"].startswith("Plan executor error during findAndModify"), err
        answer = "ERR"
    else:
        if r["kind"] == "findAndModify":
            answer = "OK"
        else:
            docs = reply.get("ids") if "ids" in reply else [d["_id"] for d in plain(reply["docs"])]
            answer = json.dumps(plain(docs))
    ROWS += 1
    print("\t".join([f"{n:04d}", r["kind"], json.dumps(plain(r["beforeOrdered"]), ensure_ascii=False),
                     json.dumps(cmd, ensure_ascii=False), answer]))
print(f"sort_exempt: {ROWS} cases", file=sys.stderr)
