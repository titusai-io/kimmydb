#!/usr/bin/env python3
"""Rebuild sort_exempt_types.tsv from the reference recordings q7s4m (72 cases) and q7s2z 13-30
(an array as the `_id` literal, `{}` and `{$eq: {}}`, with a control).

    python3 -I sort_exempt_types_gen.py <reference-6.0.5 directory> > sort_exempt_types.tsv

One row per recorded `find`, `$sort` aggregation and `findAndModify`: `case` (the set and the line
of the recording), `kind`, the documents, the command and the answer, as in `sort_exempt_gen.py`,
but with the values typed (canonical extended JSON): the filter's `_id` is of every type the
recordings tried. The answer is `ERR` when the reference refused it (code 2, parallel arrays), `OK`
for a findAndModify of q7s4m that answered, or the `_id`s it answered in order. A findAndModify of
q7s2z that answered holds the document it RETURNED instead (`{"value": ...}`, the record's `value`, `null`
where nothing matched, the keys in the order recorded): the test runs the update and compares it.
"""
import json, sys

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
for name, wanted in (("q7s4m", None), ("q7s2z", range(13, 31))):
    for n, line in enumerate(open(base + name + ".jsonl"), 1):
        if not line.strip() or (wanted is not None and n not in wanted):
            continue
        r = json.loads(line)
        reply = r["reply"]
        if "error" in reply:
            err = reply["error"]
            assert err["code"] == 2 and err["message"].endswith("cannot sort with keys that are parallel arrays"), err
            answer = "ERR"
        elif r["kind"] == "findAndModify" and name == "q7s2z":
            # the returned document: the recording's top-level `value` (null where nothing matched),
            # typed, its keys in the order recorded
            answer = json.dumps({"value": typed(r["value"])}, ensure_ascii=False)
        elif r["kind"] == "findAndModify":
            answer = "OK"
        else:
            docs = reply["ids"] if "ids" in reply else [d["_id"] for d in typed(reply["docs"])]
            answer = json.dumps(typed(docs), ensure_ascii=False)
        ROWS += 1
        print("\t".join([f"{name}{n:04d}", r["kind"], json.dumps(typed(r["beforeOrdered"]), ensure_ascii=False),
                         json.dumps(typed(r["commandOrdered"]), ensure_ascii=False), answer]))
print(f"sort_exempt_types: {ROWS} cases", file=sys.stderr)
