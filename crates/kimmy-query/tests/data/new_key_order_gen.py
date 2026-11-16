#!/usr/bin/env python3
"""Rebuild new_key_order.tsv from the reference recordings q7s2u, q7s2v, q7s2w and the updates
(cases 129 to 140, a name led by 1 with a multi-byte character) of q7s4k.

    python3 -I new_key_order_gen.py <reference-6.0.5 directory> > new_key_order.tsv

One row per recorded update that was accepted and whose update document is operators over simple
values: `case`, `upsert` (0 or 1), the document the update ran over (the seed of the filter for an
upsert), the update, and the document the reference stored, each as JSON in the order recorded.
`case` is the file letter and the line of the recording (`u0002`, `v0001`, `w0157`, `k0129`).
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

def simple(v):
    if isinstance(v, dict):
        return all(isinstance(k, str) and simple(x) for k, x in v.items())
    if isinstance(v, list):
        return all(simple(x) for x in v)
    return v is None or isinstance(v, (int, str)) and not isinstance(v, bool)

OPS = {"$set", "$setOnInsert", "$inc", "$min", "$max", "$mul"}
base = sys.argv[1].rstrip("/") + "/"
for letter, name in (("u", "q7s2u"), ("v", "q7s2v"), ("w", "q7s2w"), ("k", "q7s4k")):
    for n, line in enumerate(open(base + name + ".jsonl"), 1):
        if not line.strip():
            continue
        r = json.loads(line)
        reply = r["reply"]
        if "error" in reply or reply.get("writeErrors") or reply.get("ok") != 1:
            continue
        cmd = plain(r["commandOrdered"])
        update = cmd.get("u")
        if not isinstance(update, dict) or not update or not set(update) <= OPS:
            continue
        upsert = bool(cmd.get("upsert"))
        before = [plain(d) for d in r["beforeOrdered"]]
        after = [plain(d) for d in r["afterOrdered"]]
        if upsert:
            if before or cmd["q"] != {"_id": 1} or len(after) != 1:
                continue
            seed = {"_id": 1}
        else:
            if len(before) != 1 or len(after) != 1:
                continue
            seed = before[0]
        if not (simple(seed) and simple(update) and simple(after[0])):
            continue
        ROWS += 1
        print("\t".join([f"{letter}{n:04d}", "1" if upsert else "0", json.dumps(seed, ensure_ascii=False),
                         json.dumps(update, ensure_ascii=False), json.dumps(after[0], ensure_ascii=False)]))
print(f"new_key_order: {ROWS} cases", file=sys.stderr)
