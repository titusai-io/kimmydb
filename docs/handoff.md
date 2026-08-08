# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M4: everything but the transport

**Branch:** `m4-ddl-replication` (ready to merge)
**Gate:** 563 tests · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · DDL driven on a real server

### What this branch did

Two things, both closing correctness gaps before the network.

**1. Schema changes replicate.** Five entry kinds, each with its own payload and
idempotency rule — operations rather than one metadata snapshot, so two nodes
adding *different* indexes during a partition both keep theirs.
[ADR-033](decisions.md).

```
CreateCollection   { db, name }
DropCollection     { db, name }
CreateIndex        IndexMeta (with its derived id)
DropIndex          { db, collection, index }
ConfigureVectors   { db, collection, config }   // config: null disables
```

Before this, **five of the six DDL operations wrote no oplog entry at all**, and
the two that did carried no payload.

**2. Dropped collections leave a tombstone.** Deleting a document was protected
twice — the oplog entry *and* a tombstone in `docs` under
`tombstone_retention_secs`. Dropping a collection was protected once, by the
oplog entry alone, so past `oplog_retention_secs` a rejoining partitioned peer
brought the collection back with every document in it.
`collections_dropped` now records the drop's stamp, keyed by collection id and
collected on the tombstone window. [ADR-034](decisions.md).

### Two bugs found, both by tests that first passed for the wrong reason

**Amplification.** Applying a replicated DDL originally ran the ordinary local
operation, which logged an entry under *this* node's stamp; the peer pulled it
back, applied it, minted another. The same change traded forever, the oplog
growing every round. Each DDL operation now has a non-logging path.

**The tombstone landed under the wrong stamp.** Applying a peer's drop recorded
it with a fresh *local* stamp. Local clocks have already witnessed the peer's
stamps, so the tombstone outranked a recreation that legitimately followed the
drop — making the name permanently unusable on that node.
`drop_collection_inner` now takes the originating stamp.

The resurrection test itself was wrong twice before it caught anything: it never
attempted the resurrection, then it collected the tombstone it was testing. Both
written up in [Testing](testing.md).

### Next: the transport — the only thing left

`foca` (SWIM + suspicion) membership over UDP, TCP for oversized payloads and
oplog range transfer. Everything it calls exists and is tested between engines
in one process:

```rust
let mine = engine.version_vector()?;
// exchange vectors with the peer
if let Some(from) = mine.behind(&theirs) {
    let entries = peer.entries_for_peer(from, limit)?;   // over the wire
    engine.apply_batch(&entries)?;
}
```

`kimmy-cluster` has discovery parsing and nothing else. Config already carries
`cluster.enabled`, `cluster.bind`, `cluster.seeds`, `cluster.cluster_secret`,
and validation refuses the unsafe combinations.

### Then

- **Full resync** for a peer further behind than `oplog_retention_secs`.

### Worth knowing

- **Applying replicated DDL must not log**, and must carry the originating
  stamp into any tombstone it records. Both were bugs; both have tests.
- **Anti-entropy excludes `OpKind::UniqueViolation`** — a node's own observation.
- **Anti-entropy uses `read_oplog_from`** (stamp order); change streams use
  `read_arrival_from` (arrival order).
- **DDL entries are not rendered on change streams** — clients see data, not
  schema.
- **Collection and index ids are both derived**, which is what lets a replicated
  entry address the same thing everywhere.
- **`tombstone_retention_secs` now governs dropped collections too**, not only
  deleted documents.

### Open, none blocking

`$in` not using an index; descending-field ranges; one-bound index ranges; HNSW
snapshot persistence; the aggregation pipeline that would give MCP `aggregate`.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
