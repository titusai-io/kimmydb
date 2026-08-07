# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-07 — end of M2

**Branch:** `m2-index-wiring` (ready to merge)
**Gate:** 449 tests passing · `cargo fmt --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · vector endpoints driven
manually against a running server

### What this branch did

Closed the last 🔴 drift from M2 — HNSW existed and was tested but nothing
chose it — and completed the M2 documentation.

1. **`IndexCache`** (`crates/kimmy-vector/src/cache.rs`) owns the
   exact-versus-approximate decision. Held in `AppState`, so one graph is
   shared across requests rather than rebuilt per query. Both search endpoints
   dispatch through `knn()` in `crates/kimmy-api/src/vectors.rs`.

2. **A per-collection vector generation counter** on `Engine`, bumped by
   `put_vectors` and `delete_vectors`. Staleness detection that is exact and
   free, where counting would be O(n) and would still miss a delete-then-add.

3. **Fixed: a malformed shadow document broke search permanently.** A shadow
   collection is an ordinary collection, so any client with write access could
   insert into one; anything that did not decode as a `VectorRecord` made
   `for_each_vector` fail and turned *every subsequent search on that
   collection* into a 500. Now skipped and logged at `warn`.

4. **Docs:** new [Vectors](vectors.md); ADR-021/022/023; roadmap M2 marked
   complete with a planned-versus-built table; testing updated with a sixth
   load-bearing invariant; README corrected (it still claimed indexes were
   unimplemented, and its vector example did not deserialize).

### Next: M3 — the built-in MCP server

Planned shape, from [Roadmap](roadmap.md): `rmcp` with
`transport-streamable-http-server`, mounted at `/mcp` on the same axum router,
sharing storage handles directly — no separate process, no loopback hop.

The load-bearing constraint: **every tool call runs as the authenticated
principal, through the same `Principal::can()` the REST routes use.** Write
tools exist but fail authorization for a read-only token. There must not be a
second, weaker enforcement point — that is the whole reason MCP lives in-process.

Worth knowing before starting:

- `Auth` is an axum extractor (`crates/kimmy-api/src/state.rs`), deliberately
  so a route cannot forget it. Whatever MCP does should reuse it rather than
  re-deriving a principal.
- `search` is already its own action, grantable without `read` — so an agent
  can be given semantic search over a collection without raw document access.
- `describe_collection` (sampled schema inference) is the high-value tool for
  an agent that does not know the data. It does not exist yet.

### Open, and needing your decision

**`byo` has no ingest route.** `byo` is the default embedding provider, and
there is no endpoint for supplying vectors — so search on a `byo` collection
returns nothing, always. The only working path is writing raw records into the
shadow collection using internal serde shapes, which should not be a public
contract.

Proposed: `PUT /v1/db/{db}/coll/{coll}/docs/{id}/vectors` taking
`[{chunk, vector, text}]`, with the server supplying `source` and `source_hlc`
from the document it already has. **Not built unilaterally — it is a public API
surface.** Detail in [Deviations](deviations.md).

Everything else open is implementation, not decision: one-bound index ranges,
`$in` not using an index, descending-field ranges, HNSW snapshot persistence.
The M4 change-stream problem below will force a decision when M4 starts.

### The trap waiting in M4

**Replicated writes land behind the change-stream position.** An applied remote
entry keeps its originating stamp, so it enters the oplog *behind* the local
tail — and a subscriber already past that point never sees it. Single-node
streams are unaffected, which is why it has not bitten yet. Three candidate
resolutions are in [Roadmap](roadmap.md); none is chosen.

---

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
