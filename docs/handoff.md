# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M4 complete, M5 next

**Branch:** `main`, everything merged (PRs #13–#15 landed SWIM, peer health and
snapshot resync).
**Gate on merged main:** 614 tests · `cargo fmt --all -- --check` clean ·
`cargo clippy --workspace --all-targets -- -D warnings` clean.

### Where the project is

M0–M4 are complete. Clustering works end to end and has been driven on real
daemons, not only in tests:

- two nodes forming a cluster from one seed address, converging both ways;
- a partition healing;
- a cross-node unique violation leaving both documents in place while both nodes
  reported it (ADR-020, on real hardware);
- an empty node joining a cluster whose oplog history had been collected;
- a third node learning a peer **by gossip** that it was never configured with,
  and both survivors agreeing within milliseconds when a node was killed.

### M4 turned up four things the plan did not anticipate

All fixed, all with tests, all in [Decisions](decisions.md):

| Found | ADR |
|---|---|
| Collection ids were node-local counters — replicated writes would land in the wrong collection | 031 |
| Index ids were the same bug one level down | 032 |
| DDL did not replicate at all; five of six operations logged nothing | 033 |
| A node could not join a cluster older than `oplog_retention_secs` | 036 |

### Next: M5 — hardening

Nothing in it is blocked. Full list in [Roadmap](roadmap.md); the shape of it:

| Item | Note |
|---|---|
| Rate limiting | `/v1/auth/login` is brute-forceable at network speed |
| TLS | Two fronts: client↔server, and node↔node (`cluster_secret` authenticates but does not encrypt) |
| Benchmarks | Several tuning constants are guesses — the 2000-vector index threshold especially |
| Aggregation pipeline | Biggest single feature; also unblocks the MCP `aggregate` tool and `$vectorSearch` |
| Backup / restore | Cold file copy only today |
| `kimmy` CLI, audit log, richer metrics | |

Suggested order: rate limiting and TLS first (they separate "runs" from "can be
exposed"), then benchmarks (M5 is where guessed constants stop being
acceptable). Aggregation is self-contained and can slot anywhere.

### Carried debt, none blocking

One 🔴 in [Deviations](deviations.md): index ranges use only one bound —
correct but less selective; closing it means tracking multikey-ness per index on
the write path. Then `$in` not using an index, descending-field ranges, HNSW
snapshot persistence, SRV discovery (needs a DNS resolver crate), and a vector
reindex operation.

### Invariants a change must not break

- **The transport moves bytes and nothing else.** Convergence is tested without
  a network in `kimmy-storage/src/sync.rs`; keep it that way, or a merge bug and
  a dropped packet become indistinguishable.
- **The version vector is authoritative, not derived.** Never reintroduce a
  rebuild that lowers it — a snapshot grants coverage the oplog never held.
- **Applying replicated DDL must not log**, and must carry the originating stamp
  into any tombstone it records. Both were bugs; both have tests.
- **Retention never collects the newest oplog entry** — the clock resumes from
  it (ADR-028).
- **Anti-entropy excludes `OpKind::UniqueViolation`** — a node's own observation.
- **Collection and index ids are derived from names**, which is what lets a
  replicated entry address the same thing on every node.
- **`kimmy_api::exec` is the single authorization point** for anything a
  principal asked for. Replication goes through `apply_remote`, not `exec`.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
