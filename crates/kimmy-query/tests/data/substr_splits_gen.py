#!/usr/bin/env python3
"""Rebuild substr_splits.tsv from the reference recordings q7s4r (27 aggregations), q7s4s (60) and q7s4u (60).

    python3 -I data/substr_splits_gen.py <reference-6.0.5 directory> > data/substr_splits.tsv

q7s4r: `$substr` (1 to 9), `$substrBytes` (10 to 18) and `$substrCP` (19 to 27) over nine ranges
each: a start inside a character, an end inside one, an end inside a 4-byte one, whole-character
controls, a negative length, a negative start and a length of 1.5.

q7s4s: `$substr` (1 to 20), `$substrBytes` (21 to 40) and `$substrCP` (41 to 60) over "abcdef", the
start or the length a Double 1.5, 1.0 or 4294967296, a Decimal128 1.5 or 2, an Int64 2 or
4294967296, a string, null or a missing field, and a start beyond or at the end.

q7s4u: `$substr` (1 to 30) and `$substrBytes` (31 to 60) over "abcdef" with a start or a length that is
a Decimal128 (0.5, 1.4, 1.5, 1.6, 2.5, 3.5, -0.5 and neighbours), a Double (1.5, 1.6, 3.5), a negative length
or a negative start.

One row per record: `label`, the function (the one key of `commandOrdered.pipeline[0].$project.r`),
its three arguments (typed, in the order recorded), and the outcome: `{"error": code}` for
`reply.error.code`, else `{"r": value}` for `reply.docs[0].r` (typed). `before` and
`beforeOrdered` (one document, `{_id: 1}`) are not read.
"""
import json
import sys

COUNTS = {"q7s4r": 27, "q7s4s": 60, "q7s4u": 60}
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
for set_name, want in COUNTS.items():
    n = 0
    for n, line in enumerate(open(base + set_name + ".jsonl"), 1):
        r = json.loads(line)
        assert r["label"].startswith(f"{set_name}{n:04d} "), r["label"]
        expr = flat(r["commandOrdered"])["pipeline"][0]["$project"]["r"]
        (name, args), = expr.items()
        reply = flat(r["reply"])
        outcome = {"error": reply["error"]["code"]} if "error" in reply else {"r": reply["docs"][0]["r"]}
        ROWS += 1
        print("\t".join([f"{set_name}{n:04d}", name, json.dumps(args, ensure_ascii=False), json.dumps(outcome, ensure_ascii=False)]))
    assert n == want, (set_name, n)
print(f"substr_splits: {ROWS} cases ({', '.join(f'{k} {v}' for k, v in COUNTS.items())})", file=sys.stderr)
