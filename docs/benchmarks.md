# Benchmarks

[← Documentation index](README.md)

What has been measured, what it changed, and how to reproduce it.

This file exists because three constants in the code were chosen by reasoning
and labelled as guesses. A guess that is never checked becomes a fact by
repetition — these are the checks.

---

## How to run

```bash
cargo bench -p kimmy-vector                  # vector search and index build
cargo bench -p kimmy-storage                 # the write path
cargo bench -p kimmy-vector -- hnsw_build    # one group
```

**Recorded, not gated.** Numbers land here by hand rather than failing CI.
Criterion on a shared runner is noisy enough that a threshold gate produces
false failures, and a check people learn to ignore is worse than no check —
the same reasoning that keeps rate limiting off routes nobody has measured.

Results below are from one machine and are useful as *ratios*, not absolutes.
The conclusions drawn are all order-of-magnitude, which is what survives moving
to different hardware.

| | |
|---|---|
| Machine | Linux 7.0.11, `cargo bench` release profile |
| Date | 2026-08-08 |
| Fixture | 384 dimensions — the width of `all-MiniLM-L6-v2` |
| Metric | Cosine |
| Query | `k = 10`, one chunk per document |

Dimension matters: the cost of one comparison scales with width, so a toy width
would flatter the graph. 384 is the smallest width anyone realistically uses.

---

## Vector search: exact scan vs HNSW

| Vectors | Exact scan | HNSW | Saved per query | One build | Queries to repay the build |
|---:|---:|---:|---:|---:|---:|
| 250 | 7.6 ms | **1.41 ms** | 6.2 ms | 50 ms | 8 |
| 500 | 15.5 ms | **1.54 ms** | 14.0 ms | 161 ms | 12 |
| 1,000 | 31.3 ms | **1.86 ms** | 29.4 ms | 498 ms | 17 |
| 2,000 | 63.0 ms | **2.41 ms** | 60.5 ms | 1,678 ms | 28 |
| 4,000 | 126.2 ms | **3.10 ms** | 123.2 ms | 5,393 ms | 44 |

### There is no crossover

The threshold was set to 2,000 on the reasoning that "below this, scanning beats
building *and* walking a graph". The graph is faster at **every size measured**,
including the smallest. The premise was wrong, not just the number.

### Why — and this is the more useful finding

An exact scan costs **~31 µs per vector**, flat across the whole range. That is
far too slow for 384 floating-point multiply-adds, which is nanoseconds. The
cost is the storage read and the record decode: `vector_search` walks every
stored record through `for_each_vector`.

That single number explains both columns:

- the exact path is linear in collection size, because it loads every record;
- the graph path is nearly flat, because it loads only the ~40 candidates it
  over-fetches — 40 × 31 µs ≈ 1.3 ms, which is exactly the floor observed at
  250 vectors.

So the two paths are not "arithmetic vs graph traversal". They are **"load
everything" vs "load forty things"**, and the graph wins as soon as the
collection is bigger than the over-fetch.

**The optimisation this points at** is not the threshold at all: it is that
scoring does not need the whole record. Storing vectors so they can be scanned
without decoding text and metadata would move the exact path by an order of
magnitude and change where every line above sits. Not done — recorded because
the measurement is what makes it visible.

---

## What changed

| Constant | Was | Now | Why |
|---|---|---|---|
| `MIN_VECTORS_FOR_INDEX` | 2,000 | **500** | No crossover exists; the graph wins from 250 up. 500 keeps a build from being paid by a collection that is barely queried — below it a scan is ≤ 15 ms, which is not worth 161 ms of build to improve |
| `MAX_STALENESS` | 30 s | **30 s** | Unchanged, but now for a reason. A rebuild is 1.7 s at 2,000 vectors and 5.4 s at 4,000, so on a continuously written collection this window is what caps rebuild cost at ~18% of a core rather than exceeding 100% |

`MIN_VECTORS_FOR_INDEX` was not lowered further because the build is real work
that grows faster than linearly, and a collection queried a handful of times
between rebuilds never repays it. The break-even column above is the number to
re-read if that judgement needs revisiting.

---

## Not yet measured

| | Why it matters |
|---|---|
| `MAX_LIMIT = 10_000` in `kimmy_api::exec` | The cap on `find`. Still a guess |
| Index-backed vs scanned `find` | The planner's whole premise is unmeasured — and now the more interesting gap, since the write-path cost of an index turns out to be nil |
| Dimensions other than 384 | 768 and 1536 are both common, and the crossover depends on width |
| Batched writes | Nothing commits more than one mutation per transaction, and the commit is the entire cost — see [The write path](#the-write-path) |
| Concurrent writers | Every number here is single-threaded; redb allows one writer, so the ceiling under contention is unknown |

**Recall was the gap, and it is now closed.** Lowering the threshold routes more
collections through the graph, so the ≥ 90% recall claim covers more traffic
than before. The existing test measured recall at **16 dimensions**, which is
the flattering case — approximate search gets harder as width grows, because
distances concentrate and a greedy walk has less signal to follow.
`recall_holds_at_a_realistic_embedding_width` now pins it at 384 dimensions and
exactly 500 vectors: the boundary case, the smallest collection the graph is
trusted to serve. It passes.

---

## The write path

| Operation | Cost | Rate |
|---|---:|---:|
| `insert`, no secondary index | 3.52 ms | 284/s |
| `insert`, one secondary index | 3.37 ms | 297/s |
| `insert`, two secondary indexes | 3.53 ms | 283/s |
| `replace` | 3.38 ms | 296/s |
| `delete` + `insert` | 6.81 ms | 147/s |
| `put_vectors`, 1 chunk | 5.67 ms | 176/s |
| `put_vectors`, 4 chunks | 18.51 ms | 54/s |

### Secondary indexes are free on the write path

Zero, one and two indexes all cost the same within noise. That is worth stating
plainly because the usual intuition — and the usual advice — is that indexes
make writes slower, which shapes how people design schemas.

Here they do not, and the reason is the row below.

### Everything costs one durable commit

Every mutation lands in a single transaction that also appends its oplog entry
([ADR-008](decisions.md)), and that transaction is committed durably. At ~3.4 ms
apiece, the commit *is* the write: index maintenance, document size and record
shape all disappear underneath it. `delete` + `insert` is 6.8 ms because it is
two commits, not because deleting is expensive.

So the lever for ingest throughput is **batching mutations into one
transaction**, not tuning anything inside a write. Nothing in the API offers
that today — every route commits per operation — which makes it the obvious
next thing to look at if ingest rate ever matters. It also sets the ceiling
the embedding worker runs against, since it stores vectors one document at a
time.

---

## A number published here was wrong

The first version of this document recorded, under "an observation worth a real
benchmark", that `put_vectors` cost ~50–65 ms and implied vector ingest of
**15–20 documents per second**. That was wrong by roughly six times. It is
**5.67 ms** for a single chunk, or ~176 documents per second.

The error is instructive. The figure was not measured; it was *inferred* from
how long a test took, divided by the number of writes in it. That test ran in a
**debug** binary, while every benchmark here runs in release — and the timing
also contained a graph build and ten searches that were quietly attributed to
the writes.

**A timing taken as a by-product of measuring something else is an anecdote, not
a measurement.** It inherits the other thing's build profile, its warm-up, and
whatever else shared the stopwatch. The rule this file now follows: anything
quoted as a rate lives in a harness that states its own conditions, or it does
not get quoted.

Left in rather than quietly deleted, because the wrong number was acted on — it
went into a handoff and a summary — and the record of why it was wrong is worth
more than a clean page.

---

## Method notes

**Fixtures are deterministic.** Vectors come from a fixed pseudo-random
sequence, so a re-run compares against the same data. A benchmark whose input
changes between runs measures the input.

**Setup is excluded from measurement** but dominates wall-clock: building a
fixture is one storage write per vector. That is why the sweep stops at 4,000 —
8,000 pushed a full run past twenty minutes, and a benchmark too slow to rerun
is one nobody reruns. The exact path is linear, so larger sizes are
extrapolation rather than information.

**Build and query are measured separately** because they amortise differently:
a rebuild is paid once per staleness window, a query on every request. A single
blended number would answer neither question.

---

## Next

- [Vectors](vectors.md) — what the index is for
- [Decisions](decisions.md) — ADR-022 on why a stale graph is safe
- [Testing](testing.md) — the recall invariants these numbers sit beside
