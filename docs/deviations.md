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

## 🔴 HNSW not implemented; search is exact

**Requested.** During planning the choice was *"HNSW from the start"*
over *"flat first, HNSW behind a trait"*.

**Built.** Exact search: every stored vector is scored, best `k` kept.

**Why it drifted.** Deferred three times across separate sessions, each time to
land something else end to end. No single decision — which is exactly how this
kind of drift happens.

**Consequence.** O(n) per query. Fine into the low tens of thousands of
vectors; degrades linearly past that. No recall loss, and no index to keep
consistent.

**To close.** `VectorIndex` trait with an HNSW implementation, snapshot
persistence, tombstoned deletes, and recall measured against the exact path
(which is a genuinely useful oracle — the one real benefit of the ordering).

**Tracked as** task 18.

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
| Oplog and tombstone GC | **Unbounded disk growth.** Retention is configured but not enforced | M5 |
| TLS | Tokens and passwords cross the wire in plaintext without a proxy | M5 |
| Rate limiting | `/v1/auth/login` is brute-forceable at network speed | M5 |
| Token revocation | Deleting a user does not invalidate issued tokens | not planned |
| Aggregation pipeline | `$group`, `$unwind`, etc. absent | M5 |
| Backup / restore | Cold file copy only | M5 |
| Multi-document atomicity | A batch update can be partially applied | by design |
| Benchmarks | No performance regression baseline exists | M5 |

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
