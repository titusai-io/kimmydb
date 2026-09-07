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

### What a resident graph costs, and what parallel insertion would buy

Measured on 2026-08-30 for [ADR-103](decisions.md), with a scratch harness
around `hnsw_rs` 0.3.4 alone — pseudo-random vectors, the graph parameters
`index.rs` uses, a counting global allocator — on a ten-core host in the
release profile. Three rounds each; the ranges are the spread.

| Vectors × dim | Sequential build | `parallel_insert`, 4 threads | `parallel_insert`, all cores | Bytes per node |
|---:|---:|---:|---:|---:|
| 4,000 × 384 | 4.3–7.0 s | 1.4–2.2 s | 1.1–2.1 s | 6,491–6,591 |
| 20,000 × 384 | 40–53 s | 12.5–15 s | 9–14 s | 6,477–6,518 |
| 20,000 × 64 | 11.6–13.5 s | 7.6–10.9 s | 5.8–9.8 s | 5,280–5,346 |

Two things fall out. **The per-node cost barely depends on width**: take the
vector out (1,536 bytes at 384 dimensions, 256 at 64) and about 5,000 bytes
remain either way — `hnsw_rs`'s neighbour lists and per-layer tables. That is
the `dim × 4 + 5,000` the index cache budgets by, and it means a
384-dimensional chunk costs 6.5 KB resident, twice what the vector alone
suggests. **Parallel insertion is a 3–4× win that saturates at four
threads**: contention inside the graph means ten cores buy little over four.
Recall@10 against a brute-force scan and the 128-probe reachability score were
indistinguishable between the three variants at every size. It is not adopted
— the reasoning is beside the insertion loop in `index.rs` — and the numbers
are here so that decision can be reopened with a rebuild backlog in hand
rather than re-measured.

The recall figures themselves — 0.72–0.81 at 4,000 and 0.50–0.58 at 20,000
*uniform random* 384-dimensional vectors, at the `ef = 50` a `k = 10` search
uses — are a property of uniform random data at that width, where distances
concentrate, not of real embeddings, which cluster; the recall tests in
`index.rs` hold ≥ 0.90 on the fixtures they define. They are recorded because
they were seen, and because at 20,000 such vectors the reachability score ran
7–14 against a threshold of 8, so a synthetic load test at that scale would
see some builds discarded and rebuilt. Real data has not shown this; if a
synthetic one looks slow, that is where to look first.

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

### The build-cost regression this page caught

Worth recording as a use of the baseline rather than as a number. Running
`scripts/bench-baseline.py check` during unrelated work reported
`hnsw_build/4000` at **3.00×** and `hnsw_build/2000` at **2.03×** against the
M8 baseline — and 3.00× is exactly `MAX_BUILD_ATTEMPTS + 1`, the signature of a
graph being discarded and rebuilt the maximum number of times, every time.

The reachability check added with the vector data-loss fix was counting a
search that ran out of budget as a point the graph had lost. Its threshold was
sized on a 400-vector, 16-dimensional fixture; at 384 dimensions the count
climbs with collection size, so at 4,000 vectors it fired on essentially every
build. [ADR-061](decisions.md) has the split that fixed it.

| | before | after | baseline |
|---|---:|---:|---:|
| `hnsw_build/2000` | 3,342 ms | **1,702 ms** | 1.03× |
| `hnsw_build/4000` | 15,955 ms | **5,354 ms** | 1.01× |

The residual 1.28× at 250 vectors and 1.14× at 500 are the check itself — 128
probes against a build measured in tens of milliseconds — and are the cost the
data-loss fix was always going to carry.

**The point is that a tolerance nobody was gating on still caught it.** The
±50% band is loose on purpose, to catch a change of *shape* rather than a
wobble, and a 3× is a shape. Nothing else would have: every recall measurement
was fine, because every rebuilt graph was fine, and only the wasted work
differed.

---

## Not yet measured

| | Why it matters |
|---|---|
| Dimensions other than 384 | 768 and 1536 are both common, and the crossover depends on width |
| Larger-than-memory collections | Every figure here fits in page cache |
| A cluster under load | These are single-node numbers; replication's cost to the write path is unmeasured |
| Peak memory of a read against collection size | `count`, an index-backed `find` with a small `limit`, and a sorted `find` are bounded by what they return rather than by what they scan ([ADR-098](decisions.md)). The bound is argued from the code and tested for what is held; resident memory under those requests has not been measured |

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

`put_vectors` used to have two rows here, 5.67 ms for one chunk and 18.51 ms
for four, from the same 2026-08-08 run. They were taken when each chunk was a
commit of its own; the re-measurement after that changed is
["Storing a document's vectors"](#storing-a-documents-vectors-is-one-commit-however-many-chunks-it-holds)
below, on a different machine, so it is not a row in this table.

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
transaction**, not tuning anything inside a write. When this was first
written nothing in the API offered that; `POST .../bulk` (`insert_many`) now
does, and "Batching into one commit" below is what it was worth. The other
lever, sharing one fsync between concurrent commits, is the `coalesced`
durability class measured further down — and it is for concurrent writers
only; a lone writer is slower under it, which is spelled out there. The
embedding worker used to run against a ceiling of one commit per chunk of
every document it stored, plus one each time it checkpointed its position; a
document's chunks are now one commit however many there are
([ADR-149](decisions.md), the section below), and the worker commits once per
provider batch of about 32 documents with its position checkpoint folded into
the same transaction (the amendment to [ADR-125](decisions.md)), so the
ceiling it runs against is one commit per batch.

### Storing a document's vectors is one commit, however many chunks it holds

Re-measured 2026-09-07 on a different machine from the table above (an
Apple-silicon laptop, `cargo bench -p kimmy-storage --bench write_path --
put_vectors`; Criterion medians over 30 samples, two runs of each tree on a
quiet machine, the mean of the two; 384-dimensional vectors), so read the
two columns against each other rather than against the rows above. "Before"
is the tree in which `put_vectors` wrote each chunk, and removed each stale
tail chunk, as a commit of its own; "after" is the tree as shipped, where the
whole replacement is one scoped write ([ADR-149](decisions.md)). The 32-chunk
case was added to the bench for this measurement, because one chunk cannot
show a per-chunk cost and four barely can.

| Chunks | Before: one commit per chunk | After: one commit per document | Ratio |
|---:|---:|---:|---:|
| 1 | 6.08 ms, 164 docs/s | 6.30 ms, 159 docs/s | 1.0× |
| 4 | 25.20 ms, 40 docs/s | 7.76 ms, 129 docs/s | 3.2× |
| 32 | 183.8 ms, 5.4 docs/s | 13.07 ms, 77 docs/s | **14×** |

**The old rows hid a multiplier of one commit per chunk.** Before, the
marginal chunk cost 5.7 ms — a durable commit on this machine — so a
document's cost was its chunk count times the commit, and the 3.3× step from
the one-chunk row to the four-chunk row was that multiplier without a name.
After, the marginal chunk is ~0.22 ms: the record and its oplog entry, with
the commit paid once; 32 chunks land at 2,400 chunks a second against 170
before.

**The one-chunk row does not move, and is not supposed to.** With one chunk
there is nothing to amortise, the same way `bulk` at batch size 1 lands on the
single-insert number. On a corpus of about one chunk per document — the last
real one embedded was 12,789 documents in 12,829 chunks — this change saves
nothing per document; what it saves there is at the worker, whose batch of
~32 documents plus its position is now one commit rather than 33.

`scripts/bench-baseline.json` still holds the one- and four-chunk medians
from before the change, recorded on its own machine, and has no 32-chunk
entry; a `check` there will report the four-chunk case as `FASTER`, beyond
the tolerance band, and the 32-chunk case as new, until the next `record` on
that machine replaces them.

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

### `find` on `_id` is a primary-key read, not a scan

Measured 2026-08-14, release build, 10,000 documents, over HTTP — the same
question asked three ways on the same data, first sample discarded.

| | `find({_id})` p50 | p99 | examined | strategy |
|---|---:|---:|---:|---|
| before | 7.328 ms | 20.188 ms | 10,000 | `collectionScan` |
| after | **0.540 ms** | **1.212 ms** | **1** | `idLookup` |
| `GET /docs/{id}` | 0.306 ms | 0.703 ms | 1 | — |
| unindexed filter (control) | 7.065 ms | 19.733 ms | 10,000 | `collectionScan` |

**13.6× faster, and the control is unchanged**, which is what makes this the
fast path rather than a warmer machine. The planner consults *secondary*
indexes, and the primary key is not among them, so `find({_id: n})` read the
whole collection while `GET /docs/{id}` answered the same question with one
read.

The residual **1.77×** against the point route is honest overhead rather than a
mystery: `find` parses a filter, plans, re-applies the filter to the candidate
and wraps the result in a documents array. Worth knowing that the point route
is still the cheaper way to ask, and no longer worth restructuring an
application around.

`update`, `delete` and `count` inherit it, because all three go through
`collect_matching`.

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

## Coalesced durability — the flat line bends

Taken 2026-08-26 on a different machine from the rest of this page (an
Apple-silicon laptop, `cargo bench -p kimmy-storage --bench
concurrent_writes`; Criterion medians over 10 samples, the same three-field
document, ten inserts per writer per iteration), so compare the two columns
with each other rather than with the tables above. `durable` is every commit
fsyncing for itself; `coalesced` is `storage.durability = "coalesced"` with
a 5 ms window ([ADR-088](decisions.md)).

| Writers | `durable` | `coalesced` | Ratio |
|---:|---:|---:|---:|
| 1 | 170 docs/s | 79 docs/s | 0.46× |
| 2 | 177 docs/s | 137 docs/s | 0.77× |
| 4 | 176 docs/s | 327 docs/s | 1.9× |
| 8 | 177 docs/s | 536 docs/s | 3.0× |
| 16 | 182 docs/s | 1,114 docs/s | 6.1× |

Two things to read off it, and the unflattering one first. **A lone writer
is slower under `coalesced`** — each commit waits a full window for company
that never comes, so it pays the fsync *and* the wait. The class is for
concurrency; a single ingest loop should stay on `durable` or batch. **From
four writers up, the fsync is shared**: sixteen concurrent writers land
1,114 documents a second through the same single writer that gives 182 with
one fsync each. Every one of those documents was on disk when its call
returned — the class changes who pays for the fsync, not whether it happens.

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

### The write gap, explained

The tables above were published with an open question: a single insert took
**7.0 ms** through the API against **~3.4 ms** for the same insert at the
engine, and neither protocol overhead nor per-document encoding accounted for
it. M11 task 1 measured it.

**The daemon spends two durable commits on an insert where the engine spends
one.** The second is the embedding worker's. It records its oplog position
after *every* entry — including entries it has nothing to do with, in
collections with no vector configuration — and `put_consumer_position` is its
own write transaction and therefore its own fsync. redb has a single writer, so
that commit is not merely extra work on the same disk: it is a queue position
in front of the *next* write.

That last part is why the gap looked fixed per request. A request waits behind
at most one in-flight commit however many documents it carried, so a batch of
100 paid the same penalty as an insert of one — which is exactly the shape
recorded above, and which had made per-document encoding look like the wrong
answer for the right reason.

> **The second commit is gone.** The worker holds its position
> and writes it by deadline — with a batch, or after at most one second —
> rather than after every entry (ADR-125). The numbers in this section are
> left as they were measured against the per-entry write; what replaces it is
> one position checkpoint per second while entries arrive, however many
> arrive, so a burst of N skipped entries is N commits plus one.

#### The evidence

**A running node reports it.** `kimmy_commits` on `/metrics`, over 200 inserts
into a collection with no vector configuration:

| | inserts/s | ms each | commits per insert |
|---|---:|---:|---:|
| as shipped | 156 | 6.39 | **2.00** |
| embedding worker not started | 226 | 4.42 | **1.00** |
| webhook dispatcher not started | 156 | 6.40 | 2.00 |
| neither started | 215 | 4.65 | 1.00 |

**The HTTP benchmark agrees, and more strongly**, one client, plaintext,
`insert one`: **54 req/s at p50 11.00 ms** as shipped against **236 req/s at
p50 4.05 ms** with the worker not started — 4.4× the throughput. It is a larger
effect here than in the table above because that cell runs after 10,000
documents have been seeded, and the worker is still working through them: its
backfill competes with foreground writes for the same single writer.

The dispatcher was a named candidate and is **not** implicated: it wakes on a
two-second tick rather than on a write, and removing it changes nothing.

**And it reproduces at the engine, with no HTTP anywhere.** `cargo bench -p
kimmy-storage --bench write_path -- insert_with_consumer` runs the same insert
loop with and without a thread that records an oplog position after every
entry:

| `insert_with_consumer` | median |
|---|---:|
| `consumers/0` | 3.51 ms |
| `consumers/1` | **6.58 ms** |

An iteration there ends when the consumer has caught up, not when the insert
returns — which is the only honest unit, and worth stating because the first
version of this benchmark did not do it. Timing the insert alone reported
**+0.2 ms** for a cost that is **+3.1 ms**: the consumer's commit simply fell
outside the window. A consumer that lags has not made the write cheaper.

**Scale, for what the commit costs.** On this machine a raw redb commit of one
small row is **3.00 ms** p50 and an engine insert is **3.17 ms** — so an insert
is very nearly all fsync, and a second commit is close to a second insert. The
per-request work the daemon does and a bare `Engine` call does not is
negligible beside it: `get_collection`'s read transaction is **0.001 ms**,
Extended JSON → BSON is **0.001 ms**, and authentication is bounded by the
0.07 ms point read that includes it.

**The remaining candidate is not eliminated, only shown not to apply here.** A
commit's fsync does land on a runtime worker thread rather than a dedicated
one — there is no `spawn_blocking` on this path — but at one client the runtime
has nothing else to schedule, so it has no victim, and the gap is present at one
client. Whether it costs anything at concurrency is a different question and
has not been measured.

#### The part that is worse than the gap

The penalty a client sees is one commit, because it only ever waits behind one.
The work is not one commit. **The worker commits once per document, so a bulk
request is amplified by its batch size** — and the amplification is deferred,
which is why no latency figure showed it:

> 4,000 documents in 40 bulk requests: **0.39 s**, 84 commits while the
> requests were in flight. The node then went on committing for another
> **12.3 s**, reaching **4,041** — one per document, plus one per request.

A third of a second of ingest bought twelve seconds of saturated writer. The
bulk API exists so that 100 documents cost one fsync rather than 100
([Batching into one commit](#batching-into-one-commit--176)); behind it, the worker
pays the 100 anyway. Sustained ingest is bounded by the worker's per-document
commit rate, not by the batch path — and this holds on every node, whether or
not any collection uses vectors.

**No fix was included here.** Making a consumer's position writes cheaper trades
a crash replaying a few idempotent entries for an fsync per write, which is a
change to the oplog-consumer contract and was reserved for a decision. What
this section changed is that the trade became one between two measured numbers.

> **The decision was taken in ADR-125**, after the cluster form of the same
> cost was measured on a three-member cluster running 0.20.0: a 1,000-document
> bulk converged everywhere in 3–5 s and was followed by about 75 s of
> committing at ~18/s on every member, the writer included — the worker
> checkpointing once per replicated entry. The position is now held and
> written by deadline, at most once a second, so the 4,000-document ingest
> above would leave the node committing for about a second rather than twelve.
> The figures in this section are not re-measured.

Worth knowing before that decision: **the replication path already made the
other choice.** `Sync::apply_batch` in `kimmy-storage/src/sync.rs` witnesses a
whole batch in one transaction — "one transaction for the batch rather than one
per entry", with the reasoning in a comment beside it — and it keeps the
after-the-work ordering that makes a replay safe. So the shape a fix would take
already exists in this codebase, applied to the same problem, by an author who
was thinking about batch size at the time.

#### How to reproduce it

```bash
# The mechanism, at the engine, with no HTTP involved:
cargo bench -p kimmy-storage --bench write_path -- insert_with_consumer
# The cost, on a node: read kimmy_commits off /metrics either side of N inserts.
```

`kimmy_commits` is on `/metrics` for this reason — see
[Operations](operations.md). Commits per client-visible write is the number
that matters, and it is not derivable from a latency figure.

---

## What tracing costs

Telemetry is off unless `telemetry.endpoint` is set, so the tables above are
what a node without it does. This one is the same benchmark run twice on the
same machine, minutes apart: once with no endpoint, once pointed at an
OpenTelemetry Collector on loopback with `sample_ratio = 1.0` — **every span
exported, which is the worst case**.

| | |
|---|---|
| Machine | Darwin 25.6.0, Apple silicon, 10 cores |
| Date | 2026-08-23 |
| Build | `cargo bench` profile — the shipped `kimmyd` binary, spawned as a child process |
| Fixture | 10,000 documents of six fields |
| Method | 2 s per cell after 200 discarded warm-up requests |
| Collector | `docker run --rm -p 4318:4318 otel/opentelemetry-collector:latest`, default config, **on the same machine** |

```bash
cargo bench -p kimmyd --bench http                                      # off
KIMMY_OTLP_ENDPOINT=http://127.0.0.1:4318 cargo bench -p kimmyd --bench http
```

### Reads, plaintext

| Scenario | Clients | Off req/s | On req/s | Change | p99 off → on |
|---|---:|---:|---:|---:|---|
| point read by `_id` | 1 | 23,723 | 16,373 | −31% | 0.08 → 0.21 ms |
| point read by `_id` | 8 | 64,048 | 54,784 | −14% | 0.32 → 0.30 ms |
| point read by `_id` | 32 | 93,297 | 77,343 | −17% | 1.15 → 1.39 ms |
| `find`, page of 100 | 1 | 2,505 | 2,367 | −6% | 0.50 → 0.60 ms |
| `find`, page of 100 | 32 | 13,993 | 12,506 | −11% | 4.63 → 5.87 ms |
| `count`, whole collection | 32 | 360 | 302 | −16% | 149.60 → 217.90 ms |

### Writes, plaintext

| Scenario | Clients | Off req/s | On req/s | Change |
|---|---:|---:|---:|---:|
| insert one | 1 | 90 | 74 | −18% |
| insert one | 32 | 369 | 328 | −11% |
| bulk of 100 | 1 | 72 | 64 | −11% |
| bulk of 100 | 32 | 290 | 334 | +15% |

### What it says

**The cost is real and it is span construction, not export.** Nothing on the
request path waits for the collector — spans go to a batch processor on its own
threads — so what these numbers measure is building three to five spans per
request and handing them to a queue. A point read is ~0.04 ms of work, so
attaching spans to it is a large *fraction* of a very small number; the same
absolute cost is 6% of a `find` and invisible against a `count`.

**Read the −31% at one client as the ceiling, not the expectation.** The
collector shares ten cores with both the server and the load generator, and the
single-client cell is the one where the server is otherwise idle enough to be
dominated by that. The write rows move by roughly the run-to-run jitter this
benchmark already has — the `bulk of 100` row went *up* by 15%, which is not a
finding about tracing.

**`sample_ratio` is the dial, and 1.0 is not the recommended setting for a node
serving 90,000 reads a second.** Sampling is parent-based, so lowering it still
records whole traces rather than fragments of them
([ADR-069](decisions.md)) — you get fewer traces, not worse ones.

**An unreachable collector costs nothing measurable.** Pointed at a closed port,
2,000 point reads measured p50 0.153 ms / p99 0.311 ms against 0.159 / 0.315 with
telemetry off, and the node kept serving throughout. Export failures are logged
from the exporter's own threads and never reach the request path.

---

## The allocator: musl, glibc and mimalloc

Every release binary is a static musl build ([ADR-063](decisions.md)), and
since 0.17.0 the container ships that same file ([ADR-107](decisions.md))
rather than a glibc build of its own. ADR-107 named the one thing an operator
could observe in that change — musl's malloc where glibc's had been — and said
a measurement would decide whether it mattered. This is that measurement,
taken for [ADR-117](decisions.md), which set the allocator. **Every number
above this section came from a glibc or macOS build**, which is why none of
them showed what follows.

| | |
|---|---|
| Machine | A Linux arm64 container on Apple silicon, Docker Desktop's `linux/arm64` daemon, `--cpus 4 --memory 4g`; the load generator shares those four cores with the server |
| Date | 2026-09-01 |
| Image | `rust:1-slim-trixie` with `musl-tools`; rustc 1.98.0; targets `aarch64-unknown-linux-gnu` and `aarch64-unknown-linux-musl` |
| Build | `cargo bench` profile — the release profile plus debug info; `mimalloc` 0.1.52 (mimalloc 3.3.2), default features |
| Fixture | 10,000 documents of six fields, as every socket table above |
| Method | 3 s per cell after 200 discarded warm-up requests; 1, 8, 32 and 64 clients; plaintext (TLS was run too and orders the three the same way in every cell) |
| Runs | Two of each configuration, interleaved; the better run is shown. Spread — the two runs' difference as a share of the better — was 1–11% for glibc, 1–23% for musl and 0–10% for mimalloc on plaintext at eight clients and up |
| Load generator | One binary, the harness built for the glibc target, driving all three servers with the server binary swapped underneath it — so the client's own allocator does not move between columns |
| Peak RSS | `VmHWM` from `/proc/<pid>/status`, sampled every 200 ms while the node ran |

Three servers, one commit:

- **glibc** — `aarch64-unknown-linux-gnu`, no allocator set: what the
  container shipped before 0.17.0.
- **musl** — `aarch64-unknown-linux-musl`, no allocator set: every release
  tarball, and the container from 0.17.0 through 0.19.0.
- **musl + mimalloc** — the same musl target with `mimalloc::MiMalloc` as the
  global allocator: what ships from the next release.

### Reads, plaintext

| Scenario | Clients | glibc req/s | musl req/s | mimalloc req/s | p50 ms, glibc / musl / mimalloc | p99 ms, glibc / musl / mimalloc |
|---|---:|---:|---:|---:|---|---|
| point read by `_id` | 1 | 10,718 | 9,714 | 10,779 | 0.09 / 0.10 / 0.09 | 0.15 / 0.18 / 0.16 |
| point read by `_id` | 8 | 45,116 | **18,345** | 48,217 | 0.15 / 0.44 / 0.15 | 0.28 / 0.78 / 0.28 |
| point read by `_id` | 32 | 52,303 | **18,930** | 57,588 | 0.33 / 1.63 / 0.31 | 0.71 / 4.24 / 0.66 |
| point read by `_id` | 64 | 51,956 | **19,682** | 54,686 | 0.65 / 3.22 / 0.65 | 45.25 / 6.20 / 39.23 |
| `find`, page of 100 | 1 | 1,576 | 1,111 | 2,012 | 0.62 / 0.87 / 0.48 | 0.77 / 1.19 / 0.70 |
| `find`, page of 100 | 8 | 6,381 | **477** | 8,289 | 1.17 / 16.44 / 0.89 | 6.12 / 31.37 / 2.12 |
| `find`, page of 100 | 32 | 6,369 | **483** | 7,951 | 4.67 / 62.43 / 3.46 | 13.55 / 138.71 / 16.40 |
| `find`, page of 100 | 64 | 6,257 | **537** | 8,304 | 9.32 / 116.02 / 6.78 | 22.96 / 239.23 / 21.38 |
| `count`, whole collection | 1 | 49 | 43 | 59 | 20.38 / 23.40 / 16.80 | 21.95 / 25.24 / 18.18 |
| `count`, whole collection | 8 | 171 | **19** | 201 | 46.95 / 415.61 / 39.75 | 70.18 / 546.95 / 59.25 |
| `count`, whole collection | 32 | 175 | **22** | 207 | 164.49 / 1,282.92 / 146.82 | 326.42 / 2,129.66 / 305.71 |
| `count`, whole collection | 64 | 174 | **20** | 211 | 337.88 / 2,638.86 / 277.90 | 728.31 / 5,223.35 / 540.21 |

### Writes, plaintext

| Scenario | Clients | glibc req/s | musl req/s | mimalloc req/s | p50 ms, glibc / musl / mimalloc | p99 ms, glibc / musl / mimalloc |
|---|---:|---:|---:|---:|---|---|
| insert one | 1 | 1,016 | 984 | 1,076 | 0.86 / 0.87 / 0.80 | 2.13 / 2.67 / 2.47 |
| insert one | 8 | 2,439 | 2,214 | 2,450 | 4.85 / 4.72 / 4.46 | 7.54 / 9.04 / 8.66 |
| insert one | 32 | 3,626 | 3,283 | 3,756 | 1.63 / 3.24 / 1.51 | 31.73 / 30.47 / 26.27 |
| insert one | 64 | 4,766 | 4,104 | 5,056 | 2.79 / 7.60 / 2.22 | 54.23 / 52.69 / 47.43 |
| bulk of 100 | 1 | 230 | 216 | 257 | 3.80 / 4.12 / 3.36 | 6.28 / 6.98 / 5.99 |
| bulk of 100 | 8 | 508 | **360** | 567 | 23.51 / 18.89 / 22.19 | 33.89 / 48.91 / 29.69 |
| bulk of 100 | 32 | 771 | **418** | 808 | 5.84 / 67.69 / 4.15 | 114.73 / 170.60 / 123.66 |
| bulk of 100 | 64 | 992 | **462** | 1,058 | 11.14 / 125.76 / 6.72 | 325.29 / 344.28 / 208.14 |

### Resident memory

| | glibc | musl | musl + mimalloc |
|---|---:|---:|---:|
| Peak `VmHWM`, two runs | 273–300 MB | 125–147 MB | 546–576 MB |

The peak is reached in the write cells in every column — sixty-four clients
each sending hundred-document batches — and until then all three sit between
35 and 80 MB.

### What it says

**musl's malloc is the regression, and it is large.** At one client the musl
binary is within 10–30% of glibc. At eight, point reads run at 0.4× the rate,
a page of 100 documents at 0.07× and `count` at 0.11×, and the ratio follows
how many allocations a request makes rather than how many requests arrive: a
point read allocates a handful of times, decoding and re-encoding a hundred
documents allocates hundreds. musl's allocator serialises every allocation on
one lock, and four tokio workers stand in one queue for it. The tail says the
same: a paged `find` at eight clients has a p99 of 31 ms on musl and 6 ms on
glibc, and a `count` at sixty-four clients waits five seconds where glibc
waits 0.7.

**mimalloc recovers it and passes glibc.** Reads are 8–30% above glibc at
every client count, one included — the allocator is on the single-request path
as well as the contended one — and the p99 on a paged `find` at eight clients
is 2 ms, a third of glibc's.

**Writes barely move**, as they should: an insert is mostly fsync ([The write
gap, explained](#the-write-gap-explained)), and no allocator changes that. The
bulk path is the exception, because decoding a hundred-document batch
allocates, and at sixty-four clients musl does 462 batches a second to glibc's
992.

**The cost is resident memory.** mimalloc's peak is about twice glibc's and
four times musl's. That is retained heap rather than live data — musl's figure
bounds what was in use — and it is not the purge timer: a run with
`MIMALLOC_PURGE_DELAY=0` peaked at 608 MB. Where it goes was not chased
further; mimalloc keeps a heap per thread and hands memory back in whole
segments, and the burst has sixty-four requests in flight across every worker.
[Operations](operations.md#capacity) carries the figure, because a container
sized to the old peak wants headroom.

**The 40 ms p99 on point reads at sixty-four clients** appears with glibc and
mimalloc alike and not with musl, which never gets that far. It is the load
generator — sixty-four tasks on four cores — and not the server: a server that
answers fast enough exposes the client's own scheduling.

### How to reproduce it

Inside a Linux container with `musl-tools` and the musl target installed:

```bash
cargo bench -p kimmyd --bench http --no-run                       # the harness, glibc, and target/release/kimmyd
cargo build --profile bench --target aarch64-unknown-linux-musl -p kimmyd --bin kimmyd
# The harness embeds the path of the binary it was built beside. Copy the
# musl kimmyd over target/release/kimmyd and run the harness executable that
# `--no-run` printed, then again with the glibc one; the load generator is
# then the same binary for every row.
KIMMY_BENCH_CONCURRENCY=1,8,32,64 KIMMY_BENCH_MS=3000 target/release/deps/http-<hash> --bench
```

To measure the musl binary without mimalloc, remove the `#[global_allocator]`
in `crates/kimmyd/src/main.rs` for that build. Peak memory is `VmHWM` in
`/proc/<pid>/status` of the spawned node, read before the harness kills it.

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
