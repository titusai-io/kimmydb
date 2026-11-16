#!/usr/bin/env python3
"""Rebuild update_table.tsv from the reference recordings.

    python3 -I update_table_gen.py <reference-6.0.5 directory> > update_table.tsv

Rows: every update and find case of q7s2q and q7s2l, and the listed cases of q7s2r, q7s2t, q7s3b,
q7s3g, q7s3j, q7s2aa, q7s2y, q7s2u and q7s2v (see LISTED). One row per case: `label` (the set and the line of its jsonl), `kind`
(`update` or `find`), the documents before, the command, and the recorded outcome, as canonical
extended JSON in the order recorded. The outcome is `{"error": code}`, or for an update
`{"n", "nModified", "after"}`, or for a find `{"ids"}`. A case that sets a clock value
(`$currentDate`, `$$NOW`) is left out: its value is the time of the recording.
"""
import json, sys

ROWS = 0  # the rows written: the test asserts this count

LISTED = {
    "q7s2r": {40},
    "q7s2t": {282},
    "q7s3b": {155, 156, 159, 162, 164, 166, 167, 169, 170, 171, 172},
    "q7s3g": {323, 324, 325},
    "q7s3j": {201},
    # aa 1-10: `$pull` of an `$elemMatch` with an `$all`, and of an `$all`, over [1, [1, 2], [1], 2,
    # [[1, 2]], [2, 1]]; the answer is `afterOrdered`, the stored array (the update side of q7s3l).
    "q7s2aa": set(range(1, 11)),
    # y 1-16 and 25-68: a digit segment spelled two ways, over an array (the reference aborts),
    # a document and a missing container; y 17-24 name one key twice and are the client's, not
    # the server's; y 69-98 are the `$pull` and `$elemMatch` neighbours of q7s3g0243.
    "q7s2y": set(range(1, 17)) | set(range(25, 99)),
    # The 22 rows of q7s2u and the 24 of q7s2v that write `o.01` beside `o.1` and the like.
    "q7s2u": {16, 17, 19, 22, 23, 25, 28, 29, 31, 34, 35, 37, 40, 41, 43, 46, 47, 49, 52, 53, 55, 62},
    "q7s2v": set(range(121, 145)),
    # z 1-12: `$pull` of a document, a number and an array as a condition, over an array that
    # holds documents of two field orders, a number and a nested array.
    "q7s2z": set(range(1, 13)),
}
WHOLE = ("q7s2q", "q7s2l")

def typed(v):
    if isinstance(v, dict):
        if list(v) == ["$ordered"]:
            return {k: typed(x) for k, x in v["$ordered"]}
        return {k: typed(x) for k, x in v.items()}
    if isinstance(v, list):
        return [typed(x) for x in v]
    return v

base = sys.argv[1].rstrip("/") + "/"
skipped = []
for name in WHOLE + tuple(LISTED):
    for n, line in enumerate(open(base + name + ".jsonl"), 1):
        if not line.strip():
            continue
        if name not in WHOLE and n not in LISTED[name]:
            continue
        r = json.loads(line)
        if r["kind"] not in ("update", "find"):
            continue
        text = json.dumps(r["commandOrdered"])
        if "$currentDate" in text or "$$NOW" in text:
            skipped.append(f"{name}{n:04d}")
            continue
        cmd = typed(r["commandOrdered"])
        reply = r["reply"]
        if "error" in reply:
            outcome = {"error": reply["error"]["code"]}
        elif r["kind"] == "update" and reply.get("writeErrors"):
            outcome = {"error": reply["writeErrors"][0]["code"]}
        elif r["kind"] == "update":
            outcome = {"n": reply["n"], "nModified": reply["nModified"],
                       "after": typed(r["afterOrdered"])}
            if reply.get("upserted"):
                outcome["upserted"] = True
        elif "ids" in reply:
            outcome = {"ids": reply["ids"]}
        else:
            continue
        ROWS += 1
        print("\t".join([f"{name}{n:04d}", r["kind"], json.dumps(typed(r["beforeOrdered"]), ensure_ascii=False),
                         json.dumps(cmd, ensure_ascii=False), json.dumps(outcome, ensure_ascii=False)]))
print("# left out (a clock value): " + " ".join(skipped), file=sys.stderr)
print(f"update_table: {ROWS} cases", file=sys.stderr)
