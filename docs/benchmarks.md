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
cargo bench -p kimmy-storage                 # write path and query path
cargo bench -p kimmy-vector -- hnsw_build    # one group
cargo bench -p kimmyd --bench http           # what a client gets, over a socket
```

The last one is not a Criterion benchmark. It **spawns the shipped `kimmyd`
binary** and drives it with concurrent HTTP clients for a fixed duration per
cell, because throughput under contention is not a shape Criterion measures.
`KIMMY_BENCH_MS` and `KIMMY_BENCH_CONCURRENCY` change the duration and the
client counts.

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
| `MAX_LIMIT` | 10,000 | **10,000** | Unchanged, now checked: a full scan of exactly 10,000 documents is ~8 ms, so the cap bounds an unindexed query at single-digit milliseconds |
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
| Dimensions other than 384 | 768 and 1536 are both common, and the crossover depends on width |
| Larger-than-memory collections | Every figure here fits in page cache |
| A cluster under load | These are single-node numbers; replication's cost to the write path is unmeasured |

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

## Index-backed lookup vs a collection scan

10,000 documents, one equality filter, selectivity as the dial.

| Matching documents | Indexed | Scan | |
|---:|---:|---:|---|
| 1 | **0.003 ms** | 8.085 ms | index ~2,700× faster |
| 100 | **0.171 ms** | 8.133 ms | index 48× faster |
| 1,000 | **1.670 ms** | 7.905 ms | index 4.7× faster |
| 5,000 | 8.288 ms | **7.911 ms** | scan wins |

### The scan is flat; the index is not

A scan costs the same regardless of how many documents match — it reads all
10,000 either way, at **~0.8 µs per document**. The indexed path costs
**~1.66 µs per candidate** and reads only the candidates.

A random read is therefore about **twice** a sequential one, which is the whole
story: an index wins exactly when it eliminates more than half the collection.
The measured crossover sits right at 50% selectivity, which is what that ratio
predicts.

### What this means alongside the write-path numbers

Maintaining a secondary index costs nothing on a write, and reading through one
is enormously faster on any selective query. So an index is close to free in
both directions, and the remaining costs are disk and the
[one-bound range limitation](deviations.md).

The exception is real but narrow: a filter that matches most of a collection is
better off scanning, and the planner does not know selectivity — it has no
statistics, so it will use an index whenever one applies. On a filter matching
most documents that is a modest loss (5,000 of 10,000 cost 8.3 ms instead of
7.9 ms), which is why statistics have not been worth building.

### `MAX_LIMIT = 10_000` is defensible

The cap on `find` was a guess. A full scan of exactly 10,000 documents is
**~8 ms**, so the cap bounds an unindexed query at single-digit milliseconds of
storage work. That is a reasonable ceiling — big enough not to surprise, small
enough that a pathological query cannot occupy a core. Not changed; now checked.

---

## Concurrent writers — flat, which is the answer

The two measurements M5 left open, taken 2026-08-11 on the same machine class
as the rest of this page (`cargo bench -p kimmy-storage -- writers`; a small
three-field document, one durable commit per insert).

| Writers | Aggregate throughput |
|---:|---:|
| 1 | 296 docs/sec |
| 2 | 299 docs/sec |
| 4 | 303 docs/sec |
| 8 | 304 docs/sec |

**Throughput is flat from one writer to eight.** redb has a single writer, so
commits serialize — the question this run answers is whether concurrent
clients merely *share* that writer's throughput or actively lose some of it
to contention. They share it cleanly: within noise, eight writers cost
nothing over one.

What that decides:

- **Parallelizing a bulk load buys nothing and costs nothing.** An operator
  with a slow ingest should not reach for more connections.
- **The single-writer rate is the sustained-ingest baseline**: ~300 small
  documents/sec, ~3.4 ms per durable commit (the larger write-path fixture
  above runs ~5–6 ms — document size is most of the difference).
- **A bulk-insert API's win is per-commit overhead**, not concurrency — the
  number that shapes M8's bulk-insert design when it lands. It landed; the
  next section is what it was worth.

---

## Batching into one commit — 176×

Taken 2026-08-11 on the same machine (`cargo bench -p kimmy-storage --bench
write_path -- bulk`; Criterion medians over 30 samples, ~200-byte documents,
no secondary indexes). The `bulk` group inserts N documents in one
transaction; the first row is the same document through the ordinary
one-commit-each path, for the comparison.

| Batch | Total | Per document | Throughput |
|---:|---:|---:|---:|
| 1, own commit | 3.43 ms | 3.43 ms | 291 docs/sec |
| `bulk` 1 | 3.41 ms | 3.41 ms | 293 docs/sec |
| `bulk` 10 | 4.68 ms | 0.468 ms | 2,137 docs/sec |
| `bulk` 100 | 7.66 ms | 0.077 ms | 13,060 docs/sec |
| `bulk` 1000 | 19.48 ms | 0.019 ms | **51,320 docs/sec** |

**The commit was almost the entire cost.** Fitting the 100- and 1000-document
points puts the marginal document at ~13 µs against a fixed per-commit cost of
several milliseconds — a ratio of roughly 260:1. That is why the concurrent
writer curve was flat and why this one is not: the previous section measured
more clients sharing one commit rate, and this one measures needing far fewer
commits.

Two things worth reading off it directly:

- **`bulk` at batch size 1 lands on the single-insert number.** The batch path
  costs nothing when there is nothing to amortize, so it is not a trade.
- **The returns flatten but do not stop.** 1→10 is 7.3×, 10→100 another 6.1×,
  100→1000 another 3.9×. The 1000-document cap sits where the curve has mostly
  levelled, and in practice the 2 MB request body limit binds first for any
  document over ~2 KB ([ADR-048](decisions.md)).

**End to end, over HTTP, it holds.** Driving a debug node over loopback — not
a release build, so the absolute numbers are slower than the table above —
500 documents took **0.16 s** in one bulk request against **11.6 s** as 500
separate requests. A 72× gap where the storage measurement predicts ~90×, the
difference being per-request HTTP and auth work the batch pays once.

---

## Over a socket: what a client actually gets

**Every other number on this page is taken at the storage engine.** This one is
taken at the client's end of a TCP connection, which is the only place a
question like "how many writes a second" has an answer someone can act on. It
includes JSON and Extended JSON conversion both ways, per-request token
verification, HTTP framing, TLS when on, and the contention of several clients
at once.

| | |
|---|---|
| Machine | Linux 7.0.11, same as every table above |
| Date | 2026-08-14 |
| Build | `cargo bench` profile — the **shipped `kimmyd` binary**, spawned as a child process |
| Fixture | 10,000 documents of six fields, the same shape the write-path benchmarks use |
| Method | 3 s per cell after 200 discarded warm-up requests; the load generator shares the machine with the server |

### Reads

| Scenario | Clients | Plaintext req/s | TLS req/s | p50 ms | p99 ms |
|---|---:|---:|---:|---:|---:|
| point read by `_id` | 1 | 8,001 | 7,651 | 0.09 | 0.31 |
| point read by `_id` | 8 | 42,665 | 37,928 | 0.17 | 0.43 |
| point read by `_id` | 32 | **70,660** | 63,401 | 0.41 | 1.14 |
| `find`, page of 100 | 1 | 1,276 | 1,119 | 0.71 | 1.98 |
| `find`, page of 100 | 8 | 5,016 | 4,710 | 1.52 | 3.05 |
| `find`, page of 100 | 32 | **7,149** | 6,308 | 4.34 | 8.54 |
| `count`, whole collection | 1 | 30 | 29 | 32.38 | 61.91 |
| `count`, whole collection | 32 | 154 | 148 | 203.26 | 342.29 |

### Writes

| Scenario | Clients | Plaintext req/s | TLS req/s | p50 ms | p99 ms |
|---|---:|---:|---:|---:|---:|
| insert one | 1 | 143 | 138 | 7.00 | 10.01 |
| insert one | 8 | 383 | 322 | 23.15 | 82.12 |
| insert one | 32 | 602 | 529 | 1.62 | 246.06 |
| bulk of 100 | 1 | 73 (7,300 docs/s) | 71 | 12.20 | 33.88 |
| bulk of 100 | 32 | 244 (**24,400 docs/s**) | 248 | 27.91 | 482.08 |

### What it says

**TLS is close to free.** Within noise at one client and about 10% at
thirty-two. Whatever the reason to terminate TLS elsewhere, throughput is not
it.

**The protocol costs about 0.1 ms per request.** A point read — HTTP framing,
token verification, a storage read, BSON to Extended JSON and back out — has a
p50 of 0.09 ms. That is the honest number for "what does going through the API
cost", and it is small.

**Reads scale with clients and writes do not.** Reads go from 8,001/s to
70,660/s across thirty-two clients. Writes go from 143/s to 602/s — better than
flat, because commits batch opportunistically, but nothing like linear, and the
tail pays for it: p99 rises from 10 ms to 246 ms. redb has one writer, and
concurrency queues rather than parallelizes ([ADR-001](decisions.md)). This is
the socket-level confirmation of what
[Concurrent writers](#concurrent-writers--flat-which-is-the-answer) measured at
the engine.

**Batching is still the answer, and by more than the engine numbers suggest.**
Through the same socket, one client gets 143 documents a second inserting one
at a time and **7,300 a second in batches of 100** — a 51× difference that
costs a client nothing but a loop. At thirty-two clients it is 24,400/s.

**`count` is a collection scan.** 30 requests a second over 10,000 documents,
and it is the one read that barely scales. A client that polls a count is
asking the server to read everything, every time.

### One thing measured here and not explained

A single insert takes **7.0 ms** through the API against **~3.4 ms** for the
same insert at the engine ([The write path](#the-write-path), re-measured on
this machine while writing this section) — the write costs about twice as much
through the daemon as in a bare harness.

**It is not protocol overhead**: the read numbers bound that at ~0.1 ms. It is
not per-document encoding either, since a batch of 100 costs 12.2 ms against
6.9 ms at the engine, so the gap is roughly fixed per *request* rather than
growing with the documents in it. Candidates, none of them verified: the background oplog consumers the
daemon runs and a bare `Engine` does not — the embedding worker and the webhook
dispatcher both wake on every write — or a commit's fsync landing on a runtime
worker thread rather than a dedicated one.

**Recorded as a question rather than an answer**, which is the rule this file
was rewritten under: a cause that has not been measured is not a cause.

---

## The baseline

`scripts/bench-baseline.py` records every Criterion median to
`scripts/bench-baseline.json` and compares a later run against it:

```bash
cargo bench -p kimmy-storage -p kimmy-vector
scripts/bench-baseline.py check     # or `record`, to reset it
```

The tolerance is a deliberate ±50%: durable commits on a development machine
jitter tens of percent between runs, and the baseline exists to catch a
*shape* change — a 2×, an accidental O(n) — not a five-percent wobble.
Still **recorded, not gated**, for the reason at the top of this page; what
the script changes is that "check the branch didn't regress anything" is one
command on the machine the baseline was recorded on, instead of an eyeball
diff against these tables. Numbers from a different machine compare the
machines.

---

## A retracted figure

An earlier revision of this file carried a `put_vectors` cost and an ingest rate
that were **not measured** — they were inferred from how long a test took,
divided by the writes inside it. That test was a debug binary while every
benchmark here runs in release, and its timing also contained a graph build and
ten searches. The inferred figures were about six times too slow.

**The measured values are the ones in [The write path](#the-write-path) above.
Nothing else on this page is derived from that estimate.**

The retraction is noted rather than silently dropped because the figure had
already been quoted elsewhere, and because of the rule it produced:

> A timing taken as a by-product of measuring something else inherits the other
> thing's build profile and whatever else shared the clock. It is an anecdote,
> not a measurement. Anything quoted as a rate lives in a harness that states
> its own conditions.

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
