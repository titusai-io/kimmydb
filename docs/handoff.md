# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M4: replication core done, DDL replication next

**Branches, stacked, both ready to merge:**

| Branch | What |
|---|---|
| `m4-version-vectors` | Version vectors and anti-entropy, transport-free |
| `m4-derived-index-ids` | Index ids derived from names, schema 3 |

**Gate:** 548 tests · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · migration driven on a real
server with an index-backed query before and after.

### `m4-version-vectors`

`VersionVector` (`{node → max_hlc}`), the `oplog_versions` table maintained on
append, `entries_for_peer`, and `apply_batch`. Tested between engines in one
process: two-node convergence, three-node transitive convergence, conflicting
writes agreeing on a winner, deletes replicating, a stable second round.

A mutation check changed a *test* rather than the code — see
[Testing](testing.md).

### `m4-derived-index-ids`

The prerequisite for DDL replication, and the same bug as ADR-031 one level
down. Index ids came from a per-collection counter while index-entry keys embed
them, so node A's index 1 and node B's index 1 would key the same storage while
describing different indexes. A `CreateIndex` payload carrying a node-local id
is not something you can send anywhere.

Now `FNV-1a-32(name)`, with schema 3 renumbering existing entries.
[ADR-032](decisions.md).

### Next: DDL replication

**the maintainer chose operation-specific entries.** New op kinds, each with its own
payload, so two nodes adding *different* indexes during a partition both keep
theirs:

```
OpKind::CreateCollection   body: { db, name }
OpKind::DropCollection     body: { db, name }
OpKind::CreateIndex        body: IndexMeta
OpKind::DropIndex          body: { name }
OpKind::ConfigureVectors   body: Option<VectorConfig>   // None disables
```

What still needs building:

1. The op kinds and their codec tags. The existing payload-free
   `OpKind::Collection` stays decodable for old oplogs but is never written
   again; `apply_batch` cannot act on one, since it names nothing.
2. Logging from `create_collection`, `drop_collection`, `create_index`,
   `drop_index`, `configure_vectors`, `disable_vectors` — **five of those six
   currently write no oplog entry at all**.
3. Applying them idempotently in `apply_batch`, in stamp order.

**The open sub-question:** a `DropCollection` entry acts as a tombstone only for
as long as the oplog retains it. Past `oplog_retention_secs`, a partitioned peer
rejoining could resurrect a dropped collection. That is the same trade as
document tombstones, but it is the roadmap's flagged open question and worth
deciding explicitly rather than inheriting.

### Then

- **The transport** — `foca` membership over UDP, TCP for oversized payloads.
- **Full resync** for a peer further behind than `oplog_retention_secs`.

### Worth knowing

- **Anti-entropy uses `read_oplog_from`** (stamp order); change streams use
  `read_arrival_from` (arrival order).
- **`apply_remote` is not `exec`** — it serves no principal.
- **Retention never collects the newest oplog entry** ([ADR-028](decisions.md)).
- **Both collection and index ids are derived**, which is what makes a
  replicated entry address the same thing everywhere.

### Open, none blocking

`$in` not using an index; descending-field ranges; one-bound index ranges; HNSW
snapshot persistence; the aggregation pipeline that would give MCP `aggregate`.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
