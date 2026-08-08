# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M5 in progress, client TLS done

**Branch:** `m5-tls`, off `main` (which has login rate limiting merged, PR #16).
Not merged.
**Gate:** 635 tests (629 + 6) · `cargo fmt --all -- --check` clean ·
`cargo clippy --workspace --all-targets -- -D warnings` clean · driven against a
running daemon over real TLS.

### What this branch did

Native TLS termination for the HTTP, WebSocket and MCP listener —
`axum-server` over `rustls`. [ADR-039](decisions.md) has the reasoning. Enabled
by naming `server.tls.cert_file` and `server.tls.key_file`; there is no separate
toggle, because "enabled with no certificate" can only ever be a startup
failure.

**Node↔node replication is still plaintext.** That is the remaining TLS work and
it needs its own trust decision — `cluster_secret` already authenticates peers
with a mutual HMAC challenge that never sends the secret, so what TLS would add
there is confidentiality, and the interesting question is whether to require
operator-supplied certificates or bind the channel to the secret that already
exists.

### The finding that shaped it

**The "no C toolchain" property has not held since M2**, and ADR-016 plus
[Deviations](deviations.md) claimed it did. `kimmy-vector` depends on `reqwest`
with `rustls-tls`, non-optionally, which pulls `rustls → ring`; `ring` ships C
and assembly. Found by running `cargo tree -i ring` while planning TLS rather
than trusting the register.

The maintainer chose to accept the cost and correct the record. The operative rule now is
**do not add a second native crypto stack** — which is why TLS uses
`tls-rustls-no-provider` with `ring` installed explicitly, rather than
`axum-server`'s default, which would have added `aws-lc-rs` and CMake for the
same primitives. It is logged 🔴 rather than 🟢 in the register because what
closes it is a CI check, not code: nothing today would catch the next such drift.

### Invariants this branch put under test

There are now two serving stacks, and the property they must agree on fails
silently: if `into_make_service_with_connect_info` is dropped, requests still
succeed and the only symptom is every caller sharing one rate-limit bucket.
Asserted on both paths.

Four injected mutations, four caught — this time with a deliberate no-op
mutation first as a control, since the previous branch's harness had silently
been running a nonexistent cargo target. A fifth bug was caught without being
injected: `set_nonblocking(false)` on the listener handed to `axum-server`
panics tokio at the *first TLS connection*, not at startup, so the process would
have come up healthy and died on first contact.

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

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
