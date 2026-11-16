#!/usr/bin/env python3
"""Rebuild doc_order.tsv from the reference recordings q7s4t (18 cases) and q7s4v (12 `$densify` cases); q7s4t2 and
q7s4v2 are the same scripts in a second container.

    python3 -I data/doc_order_gen.py <reference-6.0.5 directory> > data/doc_order.tsv

Documents as ordering values: `k` = {a:1}, {a:"x"}, {b:0}, {a:1,b:2}, {b:2,a:1}, {}, {a:null}, {a:[1]} (ids 1 to
8), ordered by a find sort and a `$sort` (1 to 4), by `$setWindowFields` `$rank`, `$documentNumber` and
`$denseRank` over `sortBy` (5 to 10), partitioned by (11, 12), by the sort of `$group` keys (13, 14) and in
the partitions of `$densify` (15 to 18). Ascending: {} {a:null} {a:1} {a:1,b:2} {b:0} {b:2,a:1} {a:"x"} {a:[1]}
(ids 6 7 1 4 3 5 2 8), descending the reverse; no two documents tie, and {a:1,b:2} and {b:2,a:1} are two values.

q7s4v: `$densify` over `n` with a null, a missing and a numeric value per partition `p`, with bounds "partition",
"full" and [0, 5], with and without `partitionByFields` (1 to 12): the order of the output.
With bounds "partition" the partitions come in the order of their keys and, inside one, the documents with no
numeric value (absent or null) come first in stored order and then the values ascending with the generated rows
between them (q7s4v0001, 0007, q7s4t0016, 0018). With bounds "full" or explicit bounds the reference's own order is
NOT reliable (q7s4t0015 and q7s4t0017 differed between two recordings, and are in `unstable.tsv`; the rows of q7s4v
agreed by chance and are not in it): those rows are compared as a SET, by rule, whatever `unstable.tsv` says. Bounds
"partition" without `partitionByFields` is refused (code 5733408: q7s4v0002, 0008).

One row per record: `label`, `kind` (`find` or `aggregate`), the documents (`beforeOrdered`, typed), the command
(`commandOrdered`: a find's `sort`, or the `pipeline`), the answer (a find's `reply.ids`, an aggregation's
`reply.docs`, typed, in the order recorded; `{"error": code, "message": text}` for `reply.error.code` and the text of `reply.error.message` after the last `:: `) and `set` (`1` when the order
of the answer is not a promise: the label is in `unstable.tsv`, as q7s4t0015 and q7s4t0017 are, or the command is a
`$densify` whose bounds are not "partition"; the test compares such an answer as a set of rows). q7s4t2 and q7s4v2
are read only to check that the two containers agree on every row but the ones in `unstable.tsv`.
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
unstable = {line.split("\t")[0] for line in open(base + "unstable.tsv") if line.startswith("q7s4")}


def densify_not_by_partition(command):
    if not isinstance(command, list) or "$densify" not in command[0]:
        return False
    return command[0]["$densify"]["range"]["bounds"] != "partition"


for set_name, second_name, want in (("q7s4t", "q7s4t2", 18), ("q7s4v", "q7s4v2", 12)):
    first = [json.loads(line) for line in open(base + set_name + ".jsonl")]
    second = [json.loads(line) for line in open(base + second_name + ".jsonl")]
    assert len(first) == len(second) == want, (set_name, len(first), len(second))
    for n, (r, r2) in enumerate(zip(first, second), 1):
        label = f"{set_name}{n:04d}"
        assert r["label"].startswith(label + " "), r["label"]
        same = flat(r["reply"]) == flat(r2["reply"])
        assert same != (label in unstable), (label, same)
        command = flat(r["commandOrdered"])
        reply = flat(r["reply"])
        if r["kind"] == "find":
            spec, answer = command["sort"], reply["ids"]
        else:
            spec = command["pipeline"]
            answer = ({"error": reply["error"]["code"], "message": reply["error"]["message"].split(":: ")[-1]}
                      if "error" in reply else reply["docs"])
        as_set = label in unstable or densify_not_by_partition(spec)
        ROWS += 1
        print("\t".join([label, r["kind"], json.dumps(flat(r["beforeOrdered"]), ensure_ascii=False),
                         json.dumps(spec, ensure_ascii=False), json.dumps(answer, ensure_ascii=False),
                         "1" if as_set else "0"]))
print(f"doc_order: {ROWS} cases (q7s4t 18, q7s4v 12)", file=sys.stderr)
