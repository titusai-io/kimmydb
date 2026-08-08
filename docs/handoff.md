# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-08 — M5 started, login rate limiting done

**Branch:** `m5-rate-limiting`, off `main`. Not merged.
**Gate:** 629 tests (614 + 15) · `cargo fmt --all -- --check` clean ·
`cargo clippy --workspace --all-targets -- -D warnings` clean · driven against a
running daemon.

### What this branch did

`/v1/auth/login` now carries a token bucket per caller. [ADR-038](decisions.md)
has the reasoning; the parts that matter for anything built on top:

- **The check precedes `authenticate`.** Every attempt runs a full Argon2id
  verification, including for a user that does not exist — that is deliberate,
  and is what stops timing revealing whether an account exists. So the endpoint
  was a ~19 MB-per-request amplifier for an anonymous caller, and a limit
  applied *after* the hash would have done all the work it exists to prevent.
- **Only failures are recorded.** A correct client is never throttled for
  succeeding.
- **`kimmy_api::Limiter` is route-agnostic** and takes the clock as a parameter
  (ADR-007). Adding another route is a field on `RateLimits`, a config knob and
  a `check_at` call.
- **Three defaults are deliberate trades**, each with a 🟡 in
  [Deviations](deviations.md): per-username limiting off (it is also a lockout),
  `trusted_proxy_header` off (a forwarded header is client-supplied), and a
  shared egress sharing a budget.

Scope was agreed with the maintainer: build the mechanism reusable, apply it where a
limit is a *security* property, and let the benchmark work decide where a
*capacity* limit belongs rather than guessing numbers now.

Also corrected: README still listed M4 as "Next".

### Verification worth repeating

Seven mutations were injected into the limiter and all seven turned a named test
red. Two initially looked like escapes — the mutation *harness* was passing
`--test api` as two arguments where one was expected, so cargo ran a target that
does not exist and printed no failure line. Recorded in
[Testing](testing.md#mutation-testing) because it is the same trap from the other
side: a green result whose work never ran.

One honest gap: nothing proves the limit runs *before* Argon2 rather than after.
The only observable difference is latency. Structure and a comment defend it;
closing it properly needs a counter on the authentication path.

### Next in M5

Nothing is blocked. Full list in [Roadmap](roadmap.md):

| Item | Note |
|---|---|
| TLS | Two fronts: client↔server, and node↔node (`cluster_secret` authenticates but does not encrypt). `rustls` keeps ADR-016's pure-Rust property |
| Benchmarks | Several tuning constants are guesses — the 2000-vector index threshold especially. Also what should decide any further rate limits |
| Aggregation pipeline | Biggest single feature; also unblocks the MCP `aggregate` tool and `$vectorSearch` |
| Backup / restore | Cold file copy only today |
| `kimmy` CLI, audit log, richer metrics | |

Suggested next: TLS, then benchmarks. Aggregation is self-contained and can slot
anywhere.

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
- **The router is served with `into_make_service_with_connect_info`.** Without
  it there is no peer address, and every caller silently shares one rate-limit
  bucket. The server warns once if it happens; a new `axum::serve` call site is
  the way it would.

## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
