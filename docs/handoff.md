# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M4: everything but the transport

**Branch:** `m4-ddl-replication` (ready to merge)
**Gate:** 558 tests · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · DDL driven on a real server

### What this branch did

Schema changes replicate, which was the last correctness gap before the
network. Five entry kinds, each with its own payload and idempotency rule:

```
CreateCollection   { db, name }
DropCollection     { db, name }
CreateIndex        IndexMeta (with its derived id)
DropIndex          { db, collection, index }
ConfigureVectors   { db, collection, config }   // config: null disables
```

Operations rather than one metadata snapshot, as chosen — whole-metadata LWW
would lose an index whenever two nodes added different ones during a partition.
A test pins that both survive. [ADR-033](decisions.md).

Before this, **five of the six DDL operations wrote no oplog entry at all**, and
the two that did carried no payload.

**A real bug found on the way.** Applying a replicated DDL originally ran the
ordinary local operation, which logged an entry under *this* node's stamp. The
peer pulled that back, applied it, minted another — the same change traded
forever, the oplog growing every round. Each DDL operation now has a
non-logging path used only when applying a replicated change; the originating
entry is appended instead. Two convergence tests failed on this before it was
understood, and it now has one of its own that fails under a reintroduced
double-log.

### Next: the transport — the only thing left

`foca` (SWIM + suspicion) membership over UDP, TCP for oversized payloads and
oplog range transfer. Everything it needs to call already exists and is tested
between engines in one process:

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
and validation already refuses the unsafe combinations.

### Then

- **Full resync** for a peer further behind than `oplog_retention_secs`.
- **Collection tombstones**, if you want them — see below.

### Open question worth deciding

A `DropCollection` entry acts as a tombstone only while the oplog retains it.
Past `oplog_retention_secs`, a rejoining partitioned peer could resurrect a
dropped collection. Same trade as document tombstones, and the roadmap has
flagged it since M2 — but it is inherited rather than decided.

### Worth knowing

- **Anti-entropy excludes `OpKind::UniqueViolation`** — a node's own observation.
- **Applying replicated DDL must not log** — see the amplification bug above.
- **Anti-entropy uses `read_oplog_from`** (stamp order); change streams use
  `read_arrival_from` (arrival order).
- **DDL entries are not rendered on change streams**, matching what collection
  entries always did — clients see data, not schema.
- **Collection and index ids are both derived**, which is what lets a replicated
  entry address the same thing everywhere.

### Open, none blocking

`$in` not using an index; descending-field ranges; one-bound index ranges; HNSW
snapshot persistence; the aggregation pipeline that would give MCP `aggregate`.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
