# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-09 — M5: online backup and restore

**Branch:** `m5-backup-restore`, off `main` (PRs #16–#22 merged). Not merged.
**Gate:** 677 tests (668 + 9) · fmt clean · clippy clean · native-dep check
clean · driven end to end against a serving node.

### What this branch did

`GET /v1/admin/backup` streams a consistent backup **while the node serves**;
`kimmyd restore --from <file>` writes one into a data directory that does not
already hold a database. [ADR-041](decisions.md).

Before this, the only answer was stopping the node and copying `kimmy.redb` —
and copying it from a *running* node captures a torn file, because redb is
rewriting pages underneath the copy.

**Why it is an endpoint, not a command.** redb allows one process to hold a
database, so a separate backup process cannot open a live one. The node has to
take its own backup. The whole walk happens in one read transaction, so every
table is read as of the same instant and writers are neither blocked nor
affected — asserted by a test that writes concurrently while a backup runs.

**Buffered, not streamed as produced.** Streaming would hold the read
transaction open for as long as a client took to read, pinning MVCC pages to a
slow socket. Memory is bounded by the database rather than by the caller.

**`admin` over `*` is required** — a backup is every document, so a
database-scoped admin must not be able to take one. There is no grant-filtered
backup, because a partial backup that looks whole is a restore that silently
loses data.

### The identity question, and why there is no flag

A backup carries the node id and a restore keeps it: the id is the tiebreak half
of every write's stamp, so restoring under a fresh identity makes a node a
stranger to its own history.

The edge is that restoring one backup onto **two** nodes gives them one
identity, and the cluster then cannot tell them apart. Restore is for replacing
a node, not cloning one; to add a node, start an empty one and let anti-entropy
fill it. A `--new-identity` flag is deliberately absent — one keystroke between
recovering and corrupting a cluster's identity space. The CLI prints the warning
on every restore.

### A test that improved an error rather than being weakened

`a_file_that_is_not_a_backup_is_refused_by_name` failed at first because a short
file hit `failed to fill whole buffer` before the magic was ever checked —
accurate and useless, when the likeliest mistake is pointing at the wrong file.
The magic is now read and checked on its own, first.

### What is left in M5

One branch and PR each:

| Item | Note |
|---|---|
| Audit log, richer metrics | Both small and mostly mechanical. The audit log wants to hang off the single authorization point rather than each route |
| `kimmy` CLI | Still a stub that points at the HTTP API. The largest remaining piece and the least load-bearing |

Point-in-time restore from the oplog is **not** built and is not currently
planned — a backup is a whole-node snapshot. Worth a decision if you want it.

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
