# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-09 — M5: point-in-time restore

**Branch:** `m5-point-in-time-restore`, off `main` (PRs #16–#24 merged). Not
merged.
**Gate:** 697 tests (687 + 10) · fmt clean · clippy clean · native-dep check
clean · driven end to end through a simulated incident.

### What this branch did

`kimmyd restore --from <backup> --until <ms>` restores a backup and rewinds
document state to that instant using the oplog the backup carries.
[ADR-044](decisions.md).

**The shape of the feature is set by what the oplog stores.** Post-images only
([ADR-008](decisions.md)) — what a document *became*, never what it was — and a
delete stores nothing at all, since `DocRecord::tombstone` discards the body. So
a rewind can put a document back only to a value the retained oplog still holds,
and `oplog_retention_secs` is the real point-in-time window.

Each document changed after the target is either restored from its latest entry
at or before it, removed if its earliest later entry is an `Insert`, or
**refused** — it existed then, with a value that has been collected. Refusing is
the point: leaving such a document at its later value produces a database that
looks restored and is not.

Nothing is written until every check passes, so a refusal leaves the file
exactly as the restore wrote it.

**The undone future leaves the oplog and the version vector comes down with
it** — otherwise a peer ships the undone writes straight back, or the node
claims history it no longer holds and is permanently missing writes while
looking caught up. That lowering is the one legitimate exception to "the version
vector is never rebuilt downwards", and it has its own function rather than
relaxing the existing one.

### The mutation check that earned its keep

Five faults injected; **the most important one escaped on the first run** —
silently skipping unrecoverable documents. Nothing covered that path, because
every existing test used documents whose history was still in the window.

Writing the missing test then hit the classic trap: the first version collected
the oplog and expected a refusal, but retention never collects the newest entry
(ADR-028), so the document's insert was still the tail and the rewind removed
the document instead. The fixture had not built the condition it was named for.
Both are recorded in [Testing](testing.md).

### One usability bug found by running it

`restore` demanded `KIMMY_ROOT_PASSWORD`. It writes a file and exits and never
authenticates anybody, so an operator recovering from an incident was being
asked to invent a password first. It now skips the serving configuration.

### What is left in M5

| Item | Note |
|---|---|
| `kimmy` CLI | The last item. Still a stub printing a pointer to the HTTP API — the largest remaining piece and the least load-bearing, since everything it would do is already reachable over HTTP. Worth deciding the shape first: interactive shell, one-shot command wrapper, or both |

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
