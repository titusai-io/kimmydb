# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M5: docs audited for accuracy, planner measured

**Branch:** `m5-doc-accuracy-and-planner`, off `main` (PRs #16–#20 merged).
Not merged.
**Gate:** 640 tests · fmt clean · clippy clean at `-D warnings`.

### The documentation was audited, and it was wrong in several places

the maintainer asked for stale and incorrect documentation to be fixed rather than left.
A sweep found more than the one number that prompted it:

| Was | Reality |
|---|---|
| `docs/README.md`: "Gossip clustering — 📋 Planned (M4)" | M4 shipped three PRs ago |
| `docs/README.md`: "nothing transports [replication] between nodes yet" | It has been driven on real daemons and in containers |
| `docs/change-streams.md`: "replicated writes may land behind the stream position" | **Solved** by the arrival index (ADR-030) — this documented a fixed bug as a live limitation |
| `docs/oplog.md`: "full resync needed (M4)" | Snapshot resync is built (ADR-036) |
| Five places still citing the 2,000-vector threshold | It is 500 since the benchmark branch |
| ADR-021 and six code comments citing the "pure-Rust property" | Corrected in ADR-016 — the build has carried `ring` since M2 |
| `index.rs` error text: "coordinated … lands in M4" | User-facing, and wrong: M4 landed, coordinated did not |
| `docs/operations.md`: "clustering lands in M4, run replicas: 1" | Replaced with the `KIMMY_CLUSTER_BIND` requirement that actually matters |

The retracted `put_vectors` figure is now a short note that does not repeat the
wrong numbers, since a reader skimming it could otherwise carry them away.

**The pattern worth noting:** every one of these was true when written. Nothing
in the process catches a doc that quietly becomes false because the code moved
under it — which is the same failure mode as the ADR-016 drift, and it argues
for the same fix: checks that fail, not claims that are asserted.

### The planner's premise, measured

10,000 documents, one equality filter, selectivity as the dial:

| Matching | Indexed | Scan |
|---:|---:|---:|
| 1 | 0.003 ms | 8.085 ms |
| 100 | 0.171 ms | 8.133 ms |
| 1,000 | 1.670 ms | 7.905 ms |
| 5,000 | 8.288 ms | 7.911 ms |

A scan is flat at ~0.8 µs per document; the indexed path is ~1.66 µs per
candidate. **A random read costs about twice a sequential one, so an index wins
exactly when it eliminates more than half the collection** — and the measured
crossover sits there.

Together with "indexes are free on the write path", an index is close to free in
both directions. The planner has no statistics and will use one whenever it
applies, including where a scan would be marginally faster; the worst case
measured is 8.3 ms against 7.9 ms, which is why statistics are not worth
building.

**`MAX_LIMIT = 10_000` is now checked rather than guessed** — a full scan of
exactly 10,000 documents is ~8 ms, so the cap bounds an unindexed query at
single-digit milliseconds. Unchanged.

### Next: aggregation, the last big M5 feature

Nothing is blocked. `$match`, `$group`, `$unwind`, `$project`, `$sort`,
`$limit` — it also unblocks the MCP `aggregate` tool, which has been advertised
as planned since M3, and the `$vectorSearch` stage.

Then: node↔node TLS (needs a trust decision first), backup/restore, the `kimmy`
CLI, audit log, and a CI check for native build dependencies.

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
