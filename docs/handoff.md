# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M5: aggregation shipped; docs audited

**Branch:** `m5-doc-accuracy-and-planner`, off `main` (PRs #16–#20 merged).
Not merged. Carries three things: the documentation audit, the planner
measurement, and the aggregation pipeline.
**Gate:** 662 tests (640 + 22) · fmt clean · clippy clean · driven on a live
server.

### Aggregation

Nine stages: `$match`, `$project`, `$sort`, `$skip`, `$limit`, `$unwind`,
`$group`, `$count`, `$lookup`. Eight accumulators. `POST …/aggregate`, plus the
MCP `aggregate` tool that had been advertised as planned since M3.
[Aggregation](aggregation.md) is the reference.

**Three decisions worth carrying:**

1. **Blocking stages refuse rather than truncate.** A hard 100,000-document
   ceiling, checked on every stage's output, erroring with the stage's name. A
   `$group` over 90% of the input looks identical to one over all of it, so a
   partial answer is worse than none. The maintainer chose this over spill-to-disk.
2. **`$lookup` is authorized against the collection it joins.** Without that a
   caller with `read` on one collection could pull any other through a join —
   a privilege escalation shaped like a query, routing around the single
   authorization point. Tested at both edges, and a mutation removing the check
   turns the test red.
3. **The pure pipeline refuses `$lookup` rather than passing input through.**
   `kimmy-query` has no storage dependency, so the executor runs joins. A join
   that silently does nothing is a wrong answer shaped like a right one.

`$lookup` scans the foreign collection **once**, keyed in memory — a
per-document join is O(n·m). It sees the foreign collection as of when the stage
runs; there is no cross-collection snapshot in a store without multi-document
transactions.

Five mutations injected, five caught, with a no-op control first.

### The documentation audit

the maintainer asked for stale and incorrect documentation to be fixed rather than left.
More was wrong than the number that prompted it: `docs/README.md` still listed
clustering as planned and said nothing transported replication;
`docs/change-streams.md` documented a bug fixed by ADR-030 as a live
limitation; five places still cited the 2,000-vector threshold; ADR-021 and six
code comments still claimed a pure-Rust build; and the `coordinated` enforcement
error told users it "lands in M4" — user-facing, and wrong.

**The pattern:** every one was true when written. Nothing catches a document
that goes false as code moves under it, which is the ADR-016 drift again and
argues for the same fix — checks that fail, not claims that are asserted.

### The planner, measured

A scan is flat (~0.8 µs/document); the indexed path is ~1.66 µs/candidate. **A
random read costs about twice a sequential one, so an index wins exactly when it
eliminates more than half the collection**, and the measured crossover sits
there. With index maintenance already free on writes, an index is close to free
in both directions. `MAX_LIMIT = 10_000` is now checked, not guessed: a full
10,000-document scan is ~8 ms.

### What is left in M5

| Item | Note |
|---|---|
| TLS between nodes | The last security gap. Needs a trust decision before code: `cluster_secret` already authenticates with a mutual HMAC challenge, so TLS would add confidentiality — the question is operator certificates versus binding the channel to the existing secret |
| Backup / restore | Cold file copy only |
| `kimmy` CLI | Still a stub that points at the HTTP API |
| Audit log, richer metrics | |
| A CI check for native build dependencies | What would have caught the ADR-016 drift, and the audit above |

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
- **The login rate limit is consulted before the password is verified**, or it
  stops bounding the Argon2 work that is half its purpose (ADR-038).
- **Both serving paths use `into_make_service_with_connect_info`.** Without it
  there is no peer address, and every caller silently shares one rate-limit
  bucket. There are now two stacks — `axum::serve` for plaintext, `axum-server`
  for TLS — and a new one is how this would regress.
- **Certificates are read before the socket is bound**, so a bad one stops the
  node rather than failing for whoever connects first (ADR-039).
- **Do not add a second native crypto stack.** `ring` is already in the build;
  anything selecting `aws-lc-rs` adds CMake for the same primitives.
- **A type that crosses a format boundary needs a chosen representation, not an
  inherited one.** `NodeId` and `CollectionId` have both cost a replication
  outage by deriving serde and letting BSON decide. Anything new on the wire
  gets the same scrutiny — particularly a `u64`, which BSON cannot hold above
  `i64::MAX`.
- **A fixture that is a hash is a sample, not a constant.** Test both halves of
  the range, and assert the fixture still has the property the test needs.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
