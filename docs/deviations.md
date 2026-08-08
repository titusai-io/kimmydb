# Deviations Register

[← Documentation index](README.md)

Where the implementation differs from what was planned or requested, why, and
what it would take to close.

Individual notes live near the code they affect, but scattered notes are not
reviewable — this is the single place to see the whole picture. **Every entry
here is a debt, not a decision that has been quietly retired.**

Status meanings:

- 🔴 **Open drift** — differs from an explicit request, not yet agreed
- 🟡 **Agreed deferral** — differs from the plan, explicitly accepted
- 🟢 **Superseded** — the plan changed for a recorded reason

---

## 🟢 HNSW implemented (was an open drift)

**Requested.** During planning the choice was *"HNSW from the start"*. It was
then deferred three times across sessions, each time to land something else
end to end — no single decision, which is exactly how this kind of drift
happens.

**Now built.** `HnswIndex` over `hnsw_rs`, with recall measured against the
exact path rather than assumed: the tests assert ≥ 90% recall at k=10 and that
the nearest neighbour agrees with an exact scan exactly.

**Two findings from building it:**

- The crate the roadmap originally named, `hnswlib-rs`, **requires nightly
  Rust** — its `corenn-kernels` dependency uses `#![feature(f16)]`. Naming it
  without checking was my error. `hnsw_rs` builds on stable.
- **The `Dot` metric has no approximate index.** `anndists::DistDot` computes
  `1 - dot` and *asserts the result is non-negative*, which only holds for
  unit-length vectors; a real embedding would abort the process. Dot-product
  collections use the exact scan. Normalizing on the way in would make it work
  but would silently change what a dot-product search means.

---

## 🟢 HNSW is wired into search (was an open drift)

**Was.** The index existed and was tested, but `vector_search` always ran the
exact scan — nothing chose the approximate path. Wiring it needed a
cache-and-invalidate policy that no component owned.

**Now.** `kimmy_vector::IndexCache` owns that decision, held in `AppState` so
one graph is shared across requests. `access()` returns `Approximate` or
`Exact`, and both search endpoints dispatch through it.

The policy, and why each part of it is what it is:

| Question | Answer | Because |
|---|---|---|
| When is a graph worth building? | ≥ 2000 vectors | Below that, scanning beats building *and* walking a graph |
| How is staleness detected? | A per-collection generation counter, bumped on every vector write and delete | Counting is O(n); a count also cannot see a delete-then-add that leaves the total unchanged |
| When does a stale graph rebuild? | After 30s | Rebuilding per write would rebuild continuously under load, and each rebuild is O(n log n) |
| What happens if a build fails? | Fall back to the exact scan | An optimisation that cannot be built must not fail the query |

**Why bounded staleness is safe here, when a stale secondary index is not.**
The graph only supplies *candidates*. Scores are recomputed from the currently
stored vector, and a candidate whose record no longer exists is skipped. So a
deleted document cannot surface and an updated one scores by its new vector —
the only effect of staleness is that a document written in the last 30 seconds
may not be found yet. That is bounded recall loss on new data, never incorrect
data. `the_approximate_path_agrees_with_the_exact_one` asserts the two paths
return the same nearest neighbour with a byte-identical score.

**Still open:** no on-disk snapshot persistence — see below.

---

## 🟡 Vector indexes are rebuilt from scratch after a restart

The plan called for snapshotting the graph to `hnsw_snapshots` and replaying
newer vector-oplog entries on startup. Not built: the cache is in-memory only.

**Consequence.** The first search of a large collection after a restart pays a
full O(n log n) build, and until then queries are served by the exact scan —
slower, never wrong. Correctness does not depend on it, which is why it was
deferred rather than blocking the wiring.

---

## 🟢 `byo` now has an ingest route (was an open drift)

**Was.** `byo` — "the client supplies the vectors" — is the default provider,
and there was **no endpoint for supplying them**. Search on a `byo` collection
returned nothing, always. The only working path was writing raw records into
the shadow collection using the internal `VectorRecord` serde shape: the `#`
chunk separator in `_id`, the externally-tagged `DocId`, the internal HLC
shape. An implementation detail acting as a public contract.

**Now.** `PUT /v1/db/{db}/coll/{coll}/docs/{id}/vectors` taking
`[{chunk, vector, text}]`, with `GET` and `DELETE` alongside it. Replace-all
per document, matching what the embedding worker does — the only semantics that
stops a shortened document from leaving orphan chunks. The server supplies
`source` and `source_hlc` from the document it already holds, so no internal
shape crosses the boundary and staleness detection keeps working.

Requires `write` on the collection, and the document must exist: without it
there is no HLC for staleness to compare against.

**And the failure is no longer silent.** Searching a collection with no vectors
stored at all now returns `409 no_vectors` naming the remedy, rather than an
empty result that reads as "nothing matched". That was the more damaging half:
M3 exposed `vector_search` to agents, which would retry a query forever against
a collection that could never answer it.

**Shape chosen by the maintainer** rather than unilaterally, since it is public API.

---

## 🟢 A malformed shadow document no longer breaks search (was a live bug)

**Was.** A shadow collection is an ordinary collection, so anyone with write
access could insert a document into one. Any document that did not decode as a
`VectorRecord` made `for_each_vector` fail — turning **every subsequent search
on that collection into a 500**. One insert could brick search.

**Now.** Undecodable documents are skipped and logged at `warn`. Search stays
available; the operator still sees the problem. Same principle as the index
skipping records that no longer exist.

**Found by** the same manual verification as the entry above.

---

## 🔴 Index ranges use only one bound

**Cause.** A real bug, found by property testing: `{a: [2, 0]}` matches
`{$gte: 1, $lte: 1}` because *different array elements* satisfy each bound.
Intersecting both into one key range excluded the document.

**Fix applied.** Use one bound only, keeping the range a superset.

**Consequence.** `{qty: {$gte: 5, $lte: 9}}` scans the index from 5 upward
rather than stopping at 9. Correctness is intact; selectivity is not.

**To close.** Track multikey-ness per index, as MongoDB does, and use both
bounds for fields that never hold arrays. Touches the write path.

**Detail.** [Indexes](indexes.md).

---

## 🟡 `$in` does not use an index

Needs a *union* of point lookups rather than a single range. A query using
`$in` falls back to a collection scan. Common enough to be worth doing.

---

## 🟡 Ranges on descending index fields are ignored

The planner falls back to the equality prefix. Inverted encoding swaps which
end each bound belongs to, and getting it backwards produces a range that is
too **narrow** — silently wrong. Deliberately slow rather than possibly wrong.

**To close.** Implement the swap with its own property test.

---

## 🟢 Local embeddings are feature-gated, not the default

**Planned.** `fastembed` local ONNX as the zero-config default provider.

**Built.** Behind a `local-embeddings` cargo feature, off by default.

**Why.** Its dependencies pull native ONNX Runtime *and* OpenSSL, which would
undo the pure-Rust property that motivated choosing redb over RocksDB
(ADR-001) and `rust_crypto` over `aws_lc_rs` (ADR-016), and roughly triple the
image. Raised and agreed before building.

**Consequence.** Out of the box, embedding needs a remote provider or
client-supplied vectors. `--features local-embeddings` restores it.

---

## 🟢 Unique indexes are single-node only by default

Uniqueness is a global invariant and provably not maintainable without
coordination. `local` enforcement is the default; `coordinated` is reserved and
refused until M4. Raised and agreed. See [ADR-020](decisions.md).

---

## 🟡 Not yet implemented, and known

| Gap | Consequence | Milestone |
|---|---|---|
| TLS | Tokens and passwords cross the wire in plaintext without a proxy | M5 |
| Rate limiting | `/v1/auth/login` is brute-forceable at network speed | M5 |
| Token revocation | Deleting a user does not invalidate issued tokens | not planned |
| Aggregation pipeline | `$group`, `$unwind`, etc. absent — including the `$vectorSearch` stage, so search is endpoint-only, **and the planned MCP `aggregate` tool, which has nothing to expose** | M5 |
| Backup / restore | Cold file copy only | M5 |
| Multi-document atomicity | A batch update can be partially applied | by design |
| Benchmarks | No performance regression baseline exists — including the 2000-vector index threshold, which is a guess, not a measurement | M5 |
| Vector reindex operation | Changing model or dimension needs a disable-with-`drop_vectors` and re-enable, which backfills from the oplog | M5 |

---

## 🟢 Closed

**Oplog and tombstone GC.** Was the most serious 🟡 in this register —
retention was configured but not enforced, so both tables grew without bound.
Enforced now by a background pass every `storage.gc_interval_secs`. The design
constraint that mattered was not the collection itself but that **the newest
oplog entry must never be collected**: the logical clock resumes from the oplog
tail, so an aged-out tail would reset the clock on restart and make every later
write lose to its own older version, silently. [ADR-028](decisions.md).

---

## 🟢 Deliberate departures in M3

**`kimmy-mcp` depends on `kimmy-api`, not the reverse.** The crate graph
carried a placeholder arrow from M0 pointing the other way. Inverted so both
edges share one executor with the authorization check inside it; the
alternative was duplicating Extended JSON conversion, the query planner path,
and vector search dispatch into a second crate. [ADR-024](decisions.md).

**Tools are not filtered by grant.** A read-only token sees every write tool and
is refused when it calls one. Hiding is not an enforcement boundary, and a
filtered list makes refusals unexplainable. [ADR-025](decisions.md).

**`rmcp`'s `Host` allow-list is off by default.** It is DNS-rebinding
protection for unauthenticated local servers; `/mcp` verifies a bearer token
before the transport runs. The SDK default would have rejected every client
connecting by a real hostname. Operators can re-enable it via
`server.mcp_allowed_hosts`. [ADR-026](decisions.md).

**MCP resources exclude `__kimmy` and `.__vectors`.** A resource is material an
agent attaches to its context, and the user store is a column of password
hashes. Tools still reach them under the ordinary access check, so this is a
default rather than a control. [ADR-027](decisions.md).

**Sessions are disabled.** Stateless, so a token that expires mid-conversation
stops working rather than riding an already-open session. The cost is that a
long-running agent must re-authenticate.

---

## 🟡 Simplifications inside working features

**Keyword search is term overlap, not BM25.** It exists to give hybrid search a
lexical signal, and RRF only uses the *ordering*, so absolute scores need not
be principled. A real BM25 would rank better on its own.

**Chunking counts characters, not tokens.** A token count depends on the
model's tokenizer, which the storage layer has no business knowing. The default
(2000 chars ≈ 512 tokens) is conservative and can overshoot for dense text.

**No minimum score threshold on search.** k-NN returns the `k` nearest even
when nothing is genuinely similar, so a query against unrelated content still
returns results with near-zero scores. Callers must threshold themselves.

**`skip` is O(n)** even with an index. Deep paging stays expensive.

**Result order without an explicit `sort` is unspecified**, and differs between
an index-backed query and a scan. Matches MongoDB, still a footgun.

---

## 🔴 Known problem deferred to M4

**Replicated writes land behind the change-stream position.** An applied remote
entry keeps its originating stamp, so it enters the oplog *behind* the local
tail. A subscriber past that point never sees it.

Single-node streams are unaffected. Three candidate resolutions are recorded in
[Roadmap](roadmap.md); none is chosen.

---

## How to use this document

When something here is closed, move it to 🟢 with a note on what changed —
don't delete it. The record of *why* a thing was once wrong is worth more than
a clean list.

When a new deferral is made, add it **at the time**, not later. Every 🔴 above
became one because it was recorded somewhere local and never surfaced.
