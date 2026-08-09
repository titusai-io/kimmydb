# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M5: the write path measured, and a wrong number corrected

**Branch:** `m5-write-benchmarks`, off `main` (PRs #16-#19 all merged).
Not merged.
**Gate:** 640 tests · fmt clean · clippy clean at `-D warnings`.

### First: a correction

The previous handoff recorded that `put_vectors` costs ~50-65 ms, implying
vector ingest of **15-20 documents per second**. **That was wrong by roughly six
times.** It is 5.67 ms for a single chunk — about 176 documents per second.

The figure was never measured. It was inferred from how long a *test* took
divided by the writes inside it — and that test ran in a **debug** binary while
every benchmark runs in release, with a graph build and ten searches also inside
the same stopwatch. A timing taken as a by-product of measuring something else
inherits the other thing's build profile and everything else sharing the clock.
It is an anecdote, not a measurement. Kept in
[Benchmarks](benchmarks.md#a-number-published-here-was-wrong) rather than quietly
deleted, because the wrong number was acted on.

### What the write path actually costs

| Operation | Cost |
|---|---:|
| `insert`, no secondary index | 3.52 ms |
| `insert`, one secondary index | 3.37 ms |
| `insert`, two secondary indexes | 3.53 ms |
| `replace` | 3.38 ms |
| `put_vectors`, 1 chunk | 5.67 ms |
| `put_vectors`, 4 chunks | 18.51 ms |

**Two findings worth carrying forward:**

1. **Secondary indexes are free on the write path.** Zero, one and two cost the
   same within noise, which contradicts the usual intuition that indexes make
   writes slower — and that intuition shapes how people design schemas.
2. **Everything costs exactly one durable commit** (~3.4 ms). The mutation and
   its oplog entry share one transaction ([ADR-008](decisions.md)), and that
   commit swamps index maintenance, document size and record shape. `delete` +
   `insert` is 6.8 ms because it is two commits.

So the lever for ingest throughput is **batching mutations into one
transaction**, and nothing in the API offers that — every route commits per
operation. That is the obvious next thing if ingest rate ever matters, and it is
the ceiling the embedding worker runs against.

### Next in M5

| Item | Note |
|---|---|
| Index-backed vs scanned `find` | The planner's premise is still unmeasured, and now the more interesting gap given indexes cost nothing to maintain |
| `MAX_LIMIT = 10_000` | Still a guess |
| TLS between nodes | Needs a trust decision before code — `cluster_secret` authenticates but does not encrypt |
| Aggregation pipeline | Biggest single feature; unblocks the MCP `aggregate` tool and `$vectorSearch` |
| Backup / restore, `kimmy` CLI, audit log | |
| A CI check for native build dependencies | What would have caught the ADR-016 drift |

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
