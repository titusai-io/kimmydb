# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-07 — M3 done, and all three M4 blockers resolved

**Branches, each ready to merge, stacked in this order:**

| Branch | What |
|---|---|
| `m3-mcp-server` | The in-process MCP server |
| `m5-retention-gc` | Oplog and tombstone retention, enforced |
| `m4-oplog-arrival-order` | Change streams follow arrival, not origin stamp |
| `m4-remote-index-maintenance` | Secondary indexes on the merge path |
| `m2-byo-vector-ingest` | The `byo` ingest route, and the silent-search fix |

**Gate:** 513 tests · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · each branch driven against a
running server.

### The three blockers, and what was chosen

**1. `byo` had no ingest route.** Now `PUT
/v1/db/{db}/coll/{coll}/docs/{id}/vectors` taking `[{chunk, vector, text}]`,
replace-all per document, server supplying `source` and `source_hlc`. Searching
a collection with no vectors at all is `409 no_vectors` with the remedy in the
message, rather than an empty result that reads as "nothing matched".

**2. Replicated writes landed behind the change-stream position.** Resolved with
a second ordering — `oplog_arrival` — over local arrival sequence, with the
oplog still keyed by origin stamp for conflict resolution and anti-entropy.
Resume tokens unchanged. No format bump: the index is derived from the oplog and
rebuilt on open when it does not cover it. [Oplog](oplog.md).

**3. Unique violations on merge.** `OpKind::UniqueViolation` oplog entries, so
they reach change streams durably and resumably, plus a `kimmy_unique_violations`
metric. Both colliding documents survive and both are indexed.
[ADR-029](decisions.md).

### Bugs found and fixed along the way

- **`apply_remote` never maintained secondary indexes.** A replicated document
  would have been invisible to every index-backed query — present in a scan,
  missing from a `find` the planner chose an index for.
- **Streams de-duplicated by comparing stamps**, discarding exactly the
  replicated entries the arrival index exists to deliver.
- **Streams trusted publication order**, which can differ from commit order
  under concurrency — latent, intermittent, unrelated to replication. Both
  dissolved once the broadcast became a wake-up ([ADR-030](decisions.md)).
- **`ConsumerLagged` was never emitted**, so a stream whose range retention had
  collected skipped it silently.
- **Schema inference counted array elements, not documents**, reporting a
  presence above 1.0.
- **MCP published the password-hash collection as an attachable resource.**

### Next: M4 — gossip clustering

`apply_remote`, conflict resolution, index maintenance on merge, violation
reporting, and discovery parsing all exist and are tested. **Missing: the
transport.** Membership via [`foca`](https://github.com/caio/foca) over UDP,
TCP for oversized payloads.

Nothing is blocking it now.

### Worth knowing before starting

- **Exclude `OpKind::UniqueViolation` from anti-entropy.** These are a node's
  own observation; every node detects the same collision when it merges, so
  shipping them would report one violation once per node.
- **`kimmy_api::exec` is the single authorization point**, and replication is a
  third writer. It serves no principal, so it goes through `apply_remote`, not
  `exec`. Keep that boundary.
- **Anti-entropy uses `read_oplog_from`** (stamp order), streams use
  `read_arrival_from` (arrival order). They are different questions; do not
  merge them.
- **`oplog_retention_secs` now bounds peer catch-up.** A peer further behind
  than the window needs a full resync, which does not exist yet.
- **Retention never collects the newest oplog entry** — the clock resumes from
  it ([ADR-028](decisions.md)).

### Still open, none blocking

Implementation only, no decisions needed: `$in` not using an index;
descending-field ranges; one-bound index ranges; HNSW snapshot persistence; the
aggregation pipeline that would give MCP its `aggregate` tool.

Two design questions M4 will raise but has not yet: whether `drop_collection`
should replicate as a tombstone, and how to handle a node whose tombstones were
collected while it was partitioned.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
