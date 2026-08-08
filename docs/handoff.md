# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M4 is functionally complete

**Branches, stacked, both ready to merge:**

| Branch | What |
|---|---|
| `m4-snapshot-resync` | A peer past the retention horizon catches up from state |
| `m4-peer-health` | Backoff for failing peers, fanout instead of all-to-all |

**Gate:** 606 tests · `cargo fmt --all -- --check` clean · `cargo clippy
--workspace --all-targets -- -D warnings` clean · both driven on real daemons

### `m4-snapshot-resync`

A node could not join a cluster older than `oplog_retention_secs` — it received
nothing it could apply and retried forever. Probed rather than assumed. It now
detects the horizon and pulls current state instead. [ADR-036](decisions.md).

The subtle part: **the version vector stopped being derived from the oplog.** It
was rebuilt on every open, which would have recomputed a completed snapshot's
coverage away. Opening now only ever *raises* it.

### `m4-peer-health`

Local failure tracking with exponential backoff, and a fixed number of peers
contacted per round in rotation — **instead of** SWIM, not before it.
[ADR-037](decisions.md).

By the time the transport existed, what a gossip layer would have added had
narrowed to two things, neither of them correctness: retrying dead peers
forever, and O(n²) connections per round. Both are local problems with local
fixes. The third thing SWIM gives — learning about nodes absent from your seeds
— is nearly free on Kubernetes, where a headless Service already resolves to
every ready pod.

**This was not decided on licence.** `foca` and `memberlist` are MPL-2.0;
depending on either unmodified is permitted and unremarkable, and `chitchat`
(MIT) was available. It was decided because the remaining benefit did not
justify the subsystem.

**What it gives up:** no shared opinion about which nodes are alive. Nothing
depends on that today, because anti-entropy is transitive and idempotent. It
becomes worth revisiting if membership is ever needed for something that *must*
be agreed — `coordinated` unique enforcement ([ADR-020](decisions.md)) being the
obvious candidate, since it routes a value to the node that owns it.

### M4 status

Everything the milestone set out to do is built, with one substitution
(peer health for SWIM) and one addition nobody planned (snapshot resync,
which turned out to be load-bearing).

Verified on real daemons across the last few branches: two nodes forming a
cluster from one seed address; convergence in both directions; a partition
healing; a cross-node unique violation leaving both documents in place while
both nodes reported it; an empty node joining a cluster whose history had been
collected; and a dead seed producing one warning rather than one per round.

### Remaining, none blocking

- **SRV discovery** — `dns-srv:` parses but does not resolve; SRV records need a
  DNS resolver crate. `dns:` and `k8s:` both work.
- **TLS between nodes** (M5) — `cluster_secret` authenticates peers, but frames
  are plaintext.
- `$in` not using an index; descending-field ranges; one-bound index ranges;
  HNSW snapshot persistence; the aggregation pipeline that would give MCP its
  `aggregate` tool.

### Worth knowing

- **The transport moves bytes and nothing else.** Convergence is tested without
  a network in `kimmy-storage`; keep it that way.
- **The version vector is authoritative, not derived.** Do not reintroduce a
  rebuild that lowers it.
- **Applying replicated DDL must not log**, and must carry the originating stamp
  into any tombstone it records.
- **Anti-entropy excludes `OpKind::UniqueViolation`** — a node's own observation.
- **`tombstone_retention_secs` governs dropped collections too.**

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
