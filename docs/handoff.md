# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M5 in progress; a replication bug found by running the image

**Branch:** `m5-container-fixes`, off `main` (PRs #16 rate limiting, #17 TLS
both merged). Not merged.
**Gate:** 639 tests (635 + 4) · `cargo fmt --all -- --check` clean ·
`cargo clippy --workspace --all-targets -- -D warnings` clean · driven as a
built Docker image, single node and a three-node cluster.

### The thing to know

**Collection ids above `i64::MAX` could not be encoded, so those collections
never replicated.** Ids are hashes over the full `u64` range, BSON has no
unsigned 64-bit type, and `CollectionId` used a derived `Serialize`. About
**48% of collection names** were affected — a coin flip per name. The write
succeeded locally and the peer logged one `malformed frame` warning per round,
so nothing surfaced to a client.

The suite passed throughout, because all thirteen replication tests use
`"shop"."orders"`, which hashes into the low half. Fixed with one fixed
representation for `CollectionId`, matching the `NodeId` precedent; on-disk form
untouched, since ids persist through the hand-rolled codec rather than serde.
[ADR-031](decisions.md) carries the full account.

Also fixed: `docker-compose.yml` never set a per-node `cluster.bind`, so every
containerized node advertised `127.0.0.1` and **SWIM never formed** — replication
still converged via discovery, which is exactly what made it look healthy.

### Why this was found now and not earlier

Nothing in this project's process was skipped; the gap was that "drive a real
server" had always meant a `cargo run` binary on loopback, never the artefact
users actually deploy. Building the image and running three of them found two
issues in an hour, one of them severe. **Worth repeating before any release.**

### Previously, on this milestone

Login rate limiting (PR #16, [ADR-038](decisions.md)) and native client TLS
(PR #17, [ADR-039](decisions.md)). Node↔node replication is still plaintext and
needs its own trust decision before code.

### What this branch did

1. **Fixed the id-serialization bug above.** Regression tests at both levels: a
   2,000-name sweep asserting *both* halves of the range are exercised, and an
   end-to-end replication test over a real socket that asserts its own fixture
   still lands in the high half.
2. **`docker-compose.yml`** pins a subnet and a per-node `KIMMY_CLUSTER_BIND`,
   so the shipped cluster demo actually gossips. Its header comment also still
   claimed clustering lands in M4.
3. **Documented two container gotchas** that cost real time: the cluster-bind
   requirement (with a Kubernetes downward-API snippet), and that the image runs
   as uid 10001, so a TLS key at `0600` owned by the operator stops the node.

### Verification

Both fixes were proven where they failed rather than only in tests: `c.t` — the
collection that would not converge — now replicates to both peers in 8 s with
zero malformed-frame warnings; and both survivors declare a killed node down
within 17 ms of each other, where previously nothing was ever declared down.

Reverting the id fix turns both new tests red, checked in each direction.

### Next in M5

Nothing is blocked. Full list in [Roadmap](roadmap.md):

| Item | Note |
|---|---|
| TLS between nodes | The remaining half. Needs a trust decision before code — see above |
| Benchmarks | Several tuning constants are guesses — the 2000-vector index threshold especially. Also what should decide any further rate limits |
| Aggregation pipeline | Biggest single feature; also unblocks the MCP `aggregate` tool and `$vectorSearch` |
| Backup / restore | Cold file copy only today |
| `kimmy` CLI, audit log, richer metrics | |
| A CI check for native build dependencies | What would have caught the ADR-016 drift |

Suggested next: benchmarks, since they are what several deferred decisions are
waiting on. Node↔node TLS is the natural pair with this branch if you would
rather finish TLS first.

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
