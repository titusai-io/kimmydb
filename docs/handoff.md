# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-09 — M5: audit log and richer metrics

**Branch:** `m5-audit-and-metrics`, off `main` (PRs #16–#23 merged). Not merged.
**Gate:** 687 tests (677 + 10) · fmt clean · clippy clean · native-dep check
clean · driven on a live node in two audit modes.

### The audit log

Authorization decisions are recorded inside `Auth::require` — the one function
every check funnels through — at the `kimmy::audit` tracing target.
`audit.mode` selects `off`, `denials` (default), `writes` or `all`.
[ADR-042](decisions.md).

**Emitted from the check, not from the routes.** A log each handler has to
remember to write is a log with holes, and nothing about a missing audit line
says it is missing. A new route is audited by virtue of being authorized at all.

`denials` is the default because `all` writes one line per authorized operation
— on a read-heavy node, one per request. A denial is rare and is the event
someone is watching for. An unknown mode is refused at startup: a typo would
produce a server recording nothing, which looks exactly like a server nobody has
attacked.

Verified live at `mode=writes`: two admin actions and a write recorded, a
refusal recorded, and a `find` correctly absent. At `mode=denials` the same
traffic produced exactly one record.

### Richer metrics

Uptime, `kimmy_requests_total`, `kimmy_responses_total{class}`,
`kimmy_storage_bytes`, and counters for authorization denials, authentication
failures, rate limiting and backups. [ADR-043](decisions.md).

**The three specific counters are derived from the response status in one
middleware**, not incremented at the refusal. Each status has exactly one source
— 401 credentials, 403 `ApiError::forbidden`, 429 the limiter — so counting in
one layer means a new route is counted by existing. Verified live:
`kimmy_authz_denied_total 1` matched the single 403.

**Two things are deliberately missing and documented as such:** latency
histograms need buckets from measurements that do not exist for end-to-end
requests, and oplog lag needs a peer's version vector the API layer does not
hold. Both are worth doing; neither is worth guessing.

### What is left in M5

| Item | Note |
|---|---|
| `kimmy` CLI | The last item. Still a stub printing a pointer to the HTTP API — the largest remaining piece and the least load-bearing, since everything it would do is already reachable over HTTP |

Point-in-time restore from the oplog remains unbuilt and unplanned; a backup is
a whole-node snapshot. Worth a decision if you want it.

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
