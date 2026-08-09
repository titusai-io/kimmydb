# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-09 — M5 complete

**Branch:** `m5-kimmy-cli`, off `main` (PRs #16–#25 merged). Not merged.
**Gate:** 702 tests · fmt clean · clippy clean · native-dep check clean ·
every command driven against a live node.

**This finishes M5.** M0–M5 are all complete.

### What this branch did

`kimmy` is a real client: one-shot subcommands over the ordinary HTTP API,
JSON on stdout, diagnostics on stderr, non-zero exit on failure.

Both shape decisions were deliberate. **One-shot rather than a shell**, because
a REPL is this same command surface plus a terminal UI, so the commands come
first and a shell could sit on top later. **Over HTTP rather than opening the
file**, because redb allows one process to hold a database — a file-opening
client could not run while a node was running, and would bypass authentication
and RBAC.

**There is no `--password` flag,** deliberately: it would land in shell history
and in `ps` for every user on the machine. The password comes from stdin or
`KIMMY_PASSWORD`, and `login` prints the token rather than writing it to disk, so
there is no file whose permissions, lifetime and cleanup have to be defended.

### Two tests that were wrong before they were right

`there_is_no_password_flag` first searched the rendered help text — which
*explains* why the flag does not exist, so the search found the explanation and
passed for the wrong reason. Rewritten to walk clap's arguments, it still passed
under mutation, because clap does not propagate subcommand arguments until
`build()` is called. Only after both fixes does adding a flag turn it red.

Worth remembering: the mutation that *seemed* to escape twice was also failing to
compile the first time, so the "passed" line being read came from the restored
run further down the same command. **Check that a mutation compiled before
concluding a test missed it.**

### M5 in one place

| | |
|---|---|
| Login rate limiting | ADR-038 |
| TLS, clients | ADR-039 |
| Benchmarks | Vector index, write path, planner. Two guessed constants measured, one changed |
| Aggregation pipeline | ADR-044's neighbour: nine stages, `$lookup` authorized separately |
| Container fixes | A replication bug affecting ~48% of collection names |
| Native-dependency CI check | The enforceable half of ADR-016 |
| TLS, node↔node | ADR-040 — channel binding rather than PKI |
| Backup and restore | ADR-041 |
| Audit log, metrics | ADR-042, ADR-043 |
| Point-in-time restore | ADR-044 |
| `kimmy` CLI | This branch |

### Where to go next

Nothing is blocked, and there is no M6 defined. The register's remaining debt is
below. Candidates, in rough order of how often they would be felt:

| | |
|---|---|
| `$in` does not use an index | Common query shape falling back to a scan |
| Index ranges use one bound | Correct but less selective; needs multikey tracking on the write path |
| HNSW snapshot persistence | A restart pays a full rebuild on first search |
| Interactive shell | Now cheap: the command surface exists |
| SRV discovery | Needs a DNS resolver crate |
| Latency histograms, oplog lag | Both deliberately skipped in ADR-043 rather than guessed |

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
