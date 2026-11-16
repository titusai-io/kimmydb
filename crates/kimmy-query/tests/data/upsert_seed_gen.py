#!/usr/bin/env python3
"""Rebuild upsert_seed.tsv from the reference recording q7s2x (308 upserts).

    python3 -I upsert_seed_gen.py <reference-6.0.5 directory> > upsert_seed.tsv

One row per upsert: `case` (`x` and the line of the recording), the filter, the update and the
document the reference inserted, each as JSON in the order recorded.
"""
import json, sys

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
for n, line in enumerate(open(base + "q7s2x.jsonl"), 1):
    if not line.strip():
        continue
    r = json.loads(line)
    reply = r["reply"]
    if "error" in reply or reply.get("writeErrors") or reply.get("ok") != 1:
        continue
    cmd = plain(r["commandOrdered"])
    after = [plain(d) for d in r["afterOrdered"]]
    assert cmd.get("upsert") and len(after) == 1 and not r["beforeOrdered"], n
    print("\t".join([f"x{n:04d}", json.dumps(cmd["q"], ensure_ascii=False),
                     json.dumps(cmd["u"], ensure_ascii=False), json.dumps(after[0], ensure_ascii=False)]))
