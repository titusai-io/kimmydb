# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-07 — end of M3

**Branch:** `m3-mcp-server` (ready to merge)
**Gate:** 480 tests passing · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · `/mcp` driven manually against
a running server, including a real scoped user

### What this branch did

Built M3: the in-process MCP server. Full detail in [MCP](mcp.md).

1. **`kimmy_api::exec`** — one module both edges call, with the authorization
   check *inside* each operation rather than beside it. The REST handlers
   collapsed into thin adapters over it, which is the evidence the extraction
   was real rather than shaped around MCP.

2. **`kimmy-mcp`** — `rmcp` 3.1 streamable HTTP at `/mcp`, merged onto the same
   router in `kimmyd`. Twelve tools, collections as resources. **Stateless**: no
   MCP session, so an expired token stops working immediately.

3. **The dependency arrow was inverted.** `kimmy-mcp` now depends on
   `kimmy-api`, not the reverse — the M0 placeholder said otherwise. Rationale
   in [ADR-024](decisions.md); the short version is that co-location makes one
   enforcement point *possible* and a shared executor makes it *unavoidable*.

4. **Sampled schema inference** (`kimmy-api/src/schema.rs`), behind both the
   MCP `describe_collection` tool and a new `GET …/describe` REST route. Dotted
   paths, `path[]` for array elements, per-document presence, bounded recursion.

5. **Two bugs found by driving a real server, not by tests.** Schema inference
   counted array *elements* rather than documents, reporting a `presence` of
   2.0. And the first `resources/list` offered `kimmy://__kimmy/__users` — the
   password-hash collection — as attachable agent context. Both fixed, both now
   have tests, both recorded in [Testing](testing.md).

6. **Docs:** new [MCP](mcp.md); ADR-024 through ADR-027; roadmap M3 marked
   complete with a planned-versus-built table; a seventh load-bearing invariant
   in testing; http-api gained `/describe` and `/mcp`.

### Next: M4 — gossip clustering

The largest remaining piece, and the first one where the design has a known
unsolved problem rather than just unwritten code. Membership via
[`foca`](https://github.com/caio/foca) over UDP; `apply_remote`, conflict
resolution, and discovery parsing already exist and are tested. **Missing: the
transport.**

Before writing any of it, two decisions have to be made — see below.

### Open, and needing your decision

**Two carried forward, one new.**

1. **`byo` has no ingest route.** Unchanged from M2 and still the most
   user-visible gap: `byo` is the default embedding provider and there is no
   endpoint for supplying vectors, so search on a `byo` collection returns
   nothing, always. **This now also affects MCP** — an agent given
   `vector_search` on a `byo` collection gets empty results with no indication
   why. Proposed shape unchanged: `PUT /v1/db/{db}/coll/{coll}/docs/{id}/vectors`
   taking `[{chunk, vector, text}]`. Not built unilaterally; it is a public API
   surface. Detail in [Deviations](deviations.md).

2. **Replicated writes land behind the change-stream position (M4).** An applied
   remote entry keeps its originating stamp, so it enters the oplog *behind* the
   local tail and a subscriber past that point never sees it. Three candidate
   resolutions are in [Roadmap](roadmap.md); none is chosen. **This will block
   M4 on day one.**

3. **New: unique-violation surfacing is committed but unspecified.** M4 must
   convert a silent last-writer-wins discard into a `uniqueViolation` change
   event ([ADR-020](decisions.md)). What the event carries, and where the losing
   document is recorded so it can be reconciled, is not decided.

Everything else open is implementation, not decision: `$in` not using an index,
descending-field ranges, one-bound index ranges, HNSW snapshot persistence, and
the aggregation pipeline that would give MCP its `aggregate` tool.

### Worth knowing before starting M4

- **`kimmy_api::exec` is now the single authorization point**, and M4 adds a
  third writer to the engine — the replication transport. It applies remote
  entries rather than serving a principal, so it goes through `apply_remote`,
  *not* through `exec`. Keep that boundary clear: `exec` is for things a
  principal asked for.
- **MCP is stateless by choice**, partly so that clustering does not later have
  to make sessions follow a node.
- The `uniqueViolation` work touches change streams, which the embedding worker
  and MCP both sit downstream of.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
