# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-07 — M3 done, retention landed

**Branches:** `m3-mcp-server` (ready to merge) → `m5-retention-gc` (stacked on
it, ready to merge)
**Gate:** 493 tests passing · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · both branches driven manually
against a running server

### `m3-mcp-server` — the in-process MCP server

Full detail in [MCP](mcp.md). The short version:

1. **`kimmy_api::exec`** — one module both edges call, with the authorization
   check *inside* each operation. `kimmy-mcp` depends on `kimmy-api`, not the
   reverse; the M0 placeholder had it backwards ([ADR-024](decisions.md)).
2. **`kimmy-mcp`** — `rmcp` 3.1 streamable HTTP at `/mcp`, stateless, twelve
   tools, collections as resources.
3. **Sampled schema inference** behind `describe_collection` and a new
   `GET …/describe` REST route.
4. Two bugs found by driving a real server: presence counted array elements
   rather than documents, and `resources/list` offered the password-hash
   collection as agent context.

### `m5-retention-gc` — the largest remaining operational gap

Retention was configurable since M0 and enforced by nothing, so the oplog and
tombstones grew without bound. That was the most serious entry in
[Deviations](deviations.md); it is now closed.

- `kimmy_storage::gc` — `Engine::collect_garbage(policy)`, plus a
  `collect_garbage_at(now_ms, …)` that takes the time as a parameter so
  retention is testable without sleeping for a day.
- A background pass in `kimmyd` every `storage.gc_interval_secs` (default
  10 min, `0` disables). Validation refuses an interval longer than the oplog
  window, and refuses zero for either retention.

**The part worth remembering: the newest oplog entry is never collected.** The
logical clock is not persisted separately — `Engine::open` resumes it from the
oplog tail. Collect that entry and a restart resumes at `Hlc::ZERO`, minting
stamps below what is already stored, and every later write to an existing
document loses to its own older version *silently*. An idle node is where it
would have bitten, since a busy one always has a fresh tail.
[ADR-028](decisions.md). A mutant removing the guard fails three tests.

**Measured, because the guess was wrong.** A collection pass grows the file
before it shrinks it — redb is copy-on-write. 2,000 documents of 4 KB: 52.7 MB
before, 105.4 MB immediately after, 53.3 MB once later writes reused the space.
Operators need free space equal to what one pass will collect, and the first
pass on an uncollected database is the biggest. Recorded in
[Operations](operations.md).

### Next: M4 — gossip clustering

The largest remaining piece, and the first where the design has a known unsolved
problem rather than just unwritten code. `apply_remote`, conflict resolution,
and discovery parsing exist and are tested. **Missing: the transport.**

Two decisions have to be made before writing it — see below.

### Open, and needing your decision

1. **`byo` has no ingest route.** Carried since M2 and the most user-visible
   gap: `byo` is the default embedding provider with no endpoint for supplying
   vectors, so search returns nothing, always. **M3 made it worse** — an agent
   given `vector_search` on such a collection gets empty results with no
   indication why. Proposed: `PUT /v1/db/{db}/coll/{coll}/docs/{id}/vectors`
   taking `[{chunk, vector, text}]`. Not built unilaterally; public API surface.

2. **Replicated writes land behind the change-stream position (M4).** An applied
   remote entry keeps its originating stamp, so it enters the oplog *behind* the
   local tail and a subscriber past that point never sees it. Three candidates
   in [Roadmap](roadmap.md); none chosen. **Blocks M4 on day one.**

3. **Unique-violation surfacing is committed but unspecified (M4).**
   [ADR-020](decisions.md) commits to a `uniqueViolation` change event. What it
   carries, and where the losing document is recorded for reconciliation, is
   undecided.

Everything else open is implementation, not decision: `$in` not using an index,
descending-field ranges, one-bound index ranges, HNSW snapshot persistence, and
the aggregation pipeline that would give MCP its `aggregate` tool.

### Worth knowing before starting M4

- **`kimmy_api::exec` is the single authorization point**, and M4 adds a third
  writer — the replication transport. It applies remote entries rather than
  serving a principal, so it goes through `apply_remote`, *not* `exec`. Keep
  that boundary: `exec` is for things a principal asked for.
- **Retention now interacts with replication.** `oplog_retention_secs` bounds
  how far a peer can fall behind and still catch up incrementally; a peer
  further behind than the window needs a full resync, which does not exist yet.
  Collection also cannot empty the log, so there is always a tail to compare
  version vectors against.
- **MCP is stateless by choice**, partly so clustering need not make sessions
  follow a node.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
