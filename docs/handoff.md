# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — clustering works

**Branch:** `m4-replication-transport` (ready to merge)
**Gate:** 585 tests · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · **two real daemons formed a
cluster and converged**

### What this branch did

The replication transport, which was the last thing between the anti-entropy
core and a working cluster.

- **`protocol`** — length-prefixed BSON frames, and a three-message mutual
  handshake proving both sides hold `cluster_secret` without either sending it.
- **`transport`** — `serve` answers peers; `sync_once` runs one pull round.
- **`peers`** — resolves seeds periodically and syncs with what it finds.
- **`discovery::resolve`** — `static:`, `dns:` and `k8s:` resolve;
  `dns-srv:` still only parses.
- Wired into `kimmyd` behind `cluster.enabled`, with `sync_interval_secs` and
  `discovery_interval_secs` and validation for both.

The transport decides nothing: `kimmy-storage` still owns what wins and what is
missing. [ADR-035](decisions.md).

### Verified on two real daemons, not only in tests

- A collection, a **unique index** and a document replicated to a node that was
  told nothing but one seed address.
- Writes on each node converged in both directions.
- A partition (`SIGSTOP`) healed to the same document count on both sides.
- The same unique value written on each side of a partition left **both
  documents in place** and **both nodes reporting the violation** — ADR-020
  working end to end on real hardware rather than in a unit test.

### Two mistakes worth knowing about

**The handshake was wrong first time.** It had the initiator signing a nonce it
generated itself, which the responder checked against a different one — no valid
handshake existed. A challenge has to be received before it can be answered,
which is why it is three messages.

**`NodeId` could be written to BSON but not read back.** `uuid`'s serde picks
its representation from `is_human_readable()`, which BSON answers differently
when writing than when reading. `NodeId` now has one fixed representation. The
protocol round-trip test had used an *empty* `VersionVector`, so it never
encoded a node id at all — written up in [Testing](testing.md).

### Next

- **SWIM membership** via `foca`. Without it there is no failure detection or
  suspicion: a node syncs with every resolved address and learns a peer is gone
  by failing to connect. Workable; noisier than it should be at scale.
- **Full resync** for a peer further behind than `oplog_retention_secs`.
- **SRV discovery**, which needs a DNS resolver crate.
- **TLS between nodes** (M5) — `cluster_secret` authenticates peers but the
  frames are plaintext.

### Worth knowing

- **The transport moves bytes and nothing else.** Convergence is tested without
  a network in `kimmy-storage/src/sync.rs`; keep it that way, or a merge bug and
  a dropped packet become indistinguishable.
- **Applying replicated DDL must not log**, and must carry the originating stamp
  into any tombstone it records. Both were bugs; both have tests.
- **Anti-entropy excludes `OpKind::UniqueViolation`** — a node's own observation.
- **`tombstone_retention_secs` governs dropped collections too.**

### Open, none blocking

`$in` not using an index; descending-field ranges; one-bound index ranges; HNSW
snapshot persistence; the aggregation pipeline that would give MCP `aggregate`.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
