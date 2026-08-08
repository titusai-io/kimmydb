# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — a node can join a running cluster

**Branch:** `m4-snapshot-resync` (ready to merge)
**Gate:** 596 tests · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · **an empty node joined a live
cluster whose history had been collected**

### What this branch did

I probed the retention horizon before starting SWIM, and found that "full
resync" was not a deferred optimisation but a hard limit:

```
A documents:                      20
entries A could still offer:       1   (the retained tail)
outcome:  unknown_collection: 1        (the CreateCollection entry is gone)
B documents:  collection missing entirely
B still considers itself behind:  Some(Hlc(0.0))
```

**A node could not join a cluster older than `oplog_retention_secs`** — at the
default, any node added to a cluster more than a day old. It received nothing it
could apply and retried forever. Honest (it never falsely believed it was caught
up) but hard-stuck.

Now a peer below the horizon is told `BeyondHorizon` and asks for a **snapshot**:
collection definitions, documents in pages, then the sender's coverage.
[ADR-036](decisions.md).

Three things worth remembering:

- **The horizon is recorded** (`oplog_collected_through`), not inferred from the
  oldest retained entry — which on a node that has collected nothing is just the
  first write ever made.
- **Snapshot documents are applied through `apply_remote`**, so LWW still
  decides, indexes are maintained, and unique violations are still detected.
- **The version vector stopped being derived state.** It was rebuilt from the
  oplog on open, which would have recomputed a completed snapshot's coverage
  away. Opening now only ever *raises* it.

### Verified on real daemons

An empty node joined a cluster whose oplog had been collected: it detected the
horizon, snapshotted, and received all 30 documents and the unique index. Then
**exactly one** snapshot across many subsequent rounds, followed by ordinary
incremental replication in both directions.

### Next: SWIM membership — and a licensing question first

`foca` is the crate the roadmap names, and it is **MPL-2.0**. Depending on it
without modification is permitted and common, but it is a licensing
consideration rather than a purely technical call — and dependency choices here
have been deliberate (redb over RocksDB for pure Rust, `jsonwebtoken` with
`rust_crypto` to avoid a C toolchain).

What SWIM would buy, now that resync works:

- **Failure detection.** Today a node learns a peer is gone by failing to
  connect, every round.
- **Membership beyond seeds.** Lower value on the primary target: a Kubernetes
  headless Service already resolves to the full member set.
- **Scale.** Every node currently syncs with every other node each interval,
  which is O(n²) connections.

### Then

- **SRV discovery**, which needs a DNS resolver crate.
- **TLS between nodes** (M5) — `cluster_secret` authenticates peers but frames
  are plaintext.

### Worth knowing

- **The transport moves bytes and nothing else.** Convergence is tested without
  a network in `kimmy-storage`; keep it that way.
- **The version vector is authoritative, not derived.** Do not reintroduce a
  rebuild that lowers it.
- **Applying replicated DDL must not log**, and must carry the originating stamp
  into any tombstone it records.
- **`tombstone_retention_secs` governs dropped collections too.**

### Open, none blocking

`$in` not using an index; descending-field ranges; one-bound index ranges; HNSW
snapshot persistence; the aggregation pipeline that would give MCP `aggregate`.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
