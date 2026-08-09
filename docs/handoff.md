# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M5: replication is encrypted; TLS complete on both fronts

**Branch:** `m5-cluster-tls`, off `main` (PRs #16–#21 merged). Not merged.
**Gate:** 668 tests (662 + 6) · fmt clean · clippy clean · native-dep check
clean · driven on two real daemons.

### What this branch did

Node-to-node replication runs over TLS, always. Each node generates a
self-signed certificate at startup and **neither side verifies the other's** —
what secures the channel is that the existing mutual HMAC handshake also signs
the TLS session's exported keying material ([RFC 5705]):

```
proof = HMAC(cluster_secret, len(nonce) || nonce || len(exporter) || exporter)
```

A man-in-the-middle holds two TLS sessions with different exporters, so the
proof it relays is over the wrong bytes and it cannot recompute one without the
secret. Confidentiality and MITM resistance, with no PKI to run.
[ADR-040](decisions.md).

### The test that carries this

`a_man_in_the_middle_cannot_relay_the_handshake` stands up a relay that
genuinely terminates TLS on both sides and genuinely can read the frames — that
is the point, since it demonstrates why unverified TLS alone would be
insufficient — and requires the handshake to fail.

**Removing the binding from the proof makes that test fail while its control
still passes.** The control matters more than usual here: a bug breaking *all*
replication would make the MITM test pass for entirely the wrong reason, which
is a trap this suite has fallen into before.

All 13 pre-existing replication tests now run over TLS unchanged. Two of them
speak raw TCP and are refused one layer earlier than before; their comments were
corrected rather than left describing what they used to prove.

### Consequences worth knowing

- **No switch.** `cluster_secret` must already match cluster-wide; a second
  setting that must also match is another way to misconfigure a cluster, and its
  failure mode would be silent plaintext.
- **A cluster cannot be upgraded node by node across this version.** TLS and
  plaintext nodes cannot talk. Stop the cluster, upgrade all nodes, restart —
  no data is lost, each node holds a full copy and anti-entropy reconciles.
  Recorded in [Operations](operations.md).
- **Node identity is still only "holds the cluster secret."** Unchanged, and
  what the secret always meant.

### What is left in M5

One branch and PR each, as agreed:

| Item | Note |
|---|---|
| Backup / restore | Cold file copy only today. The most operationally significant gap left |
| Audit log, richer metrics | Both small and mostly mechanical |
| `kimmy` CLI | Still a stub that points at the HTTP API |

[RFC 5705]: https://www.rfc-editor.org/rfc/rfc5705

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
