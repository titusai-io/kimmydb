# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-11 — M8 begun: the cluster harness, and what it caught

**M0–M7 are complete; M8 (prove, persist, polish) is underway.** Twelve
tasks, planned in [Roadmap](roadmap.md); three decisions reserved for the
maintainer before their branches start: bulk insert's API shape, certificate
reload's trigger, and token revocation's semantics.

### State of the branches

| | |
|---|---|
| `main` | PRs #16–#42 merged: the M8 plan, the cluster harness, observability |
| `m8-benchmarks` | **Not merged.** The `writers` bench (throughput flat from 1 to 8 concurrent writers — the single redb writer is shared cleanly, so a bulk API's win is per-commit overhead, not parallelism) and `scripts/bench-baseline.py` with the recorded baseline |

### The bug the harness caught on its first run

**No webhook was ever delivered in any clustered deployment.** SWIM's live
set contains *peers only* — foca's `MemberUp` never fires for the node
holding the set — so rendezvous ownership computed an owner that could never
be `me`, and every node stood down for every subscription. Single-node
worked (empty set → own everything), which is why all of M6's tests, its
mutation runs, and every live drive passed. Fixed in `ownership::owns`:
candidates are the live peers **plus this node**, which also dissolves the
empty-set special case. Full account in [Deviations](deviations.md).

### What the harness asserts

Real `kimmyd` processes on scratch ports: gossip formation read from the new
`kimmy_cluster_members` gauge (peers-only — a formed three-node cluster
reads 2 everywhere); an unseeded member learned through gossip; `SIGSTOP`
suspicion and `SIGCONT` recovery without restart; kill detection;
replication through gossip-discovered peers using a collection name that
hashes above `i64::MAX`; and one webhook per node's ownership share, all
delivered, with a killed owner's subscription taken over by a survivor.
Ignored by default; CI runs them serialized (`--ignored --test-threads=1`).

### State of the branches

| | |
|---|---|
| `main` | PRs #16–#38 merged |
| `m7-mutation-and-closeout` | **Not merged.** The M7 mutation-testing pass and this docs close-out. Test-and-docs only — no production code changed |

**Gate on it:** fmt · clippy `-D warnings` ·
`./scripts/check-native-deps.sh` · full suite. Nothing behavioural to drive:
the branch adds tests and documentation.

### What M7 was

Four branches: the M6 review fixes (webhook payload names, backoff pruning,
the egress DNS TOCTOU), multikey tracking per index (closing the register's
last 🔴 — both range bounds on scalar-only indexes, snapshot-checked scans,
and the stale-handle maintenance fix), ranges on descending fields (the bound
swap), and `$in` as a union of point probes. The full accounts live in
[Roadmap](roadmap.md), [Deviations](deviations.md) and [Indexes](indexes.md).

### The mutation pass, and what it teaches

`cargo-mutants` is now installed and replaces the hand-rolled harness. 131
mutants over the planner, the key encoding, and everything M7 changed: ten
escapes, nine killed with new tests, one proven equivalent. The account is in
[Testing](testing.md); the lesson in one line: **every killable escape lived
in a routing or rendering layer above the one the strong tests guard** —
`exec`'s choice of which engine call to make, `explain`'s rendering, the
dispatcher's outer loop. When a well-tested layer gains a caller, the caller
needs its own tests.

### What webhooks are, in one paragraph

A client registers a URL against a collection; the cluster pushes change events
to it. Same events as a [change stream](change-streams.md), opposite direction —
for consumers that cannot hold a WebSocket. [Webhooks](webhooks.md) is the user
documentation and is accurate; read it before the code.

### The design, so it is not re-derived

Read [ADR-045](decisions.md) for the full reasoning. The load-bearing ideas:

1. **The dispatcher is an ordinary oplog consumer**, like the embedding worker.
   Nothing about it is special machinery.
2. **Delivery progress is replicated state.** Each node writes *only its own*
   record — `__kimmy.__webhook_progress`, `_id = {subscription}:{node}`, holding
   a `VersionVector` — so there are no write conflicts, and any node reads the
   union. This is what makes a node dying survivable.
3. **Ownership is derived, not elected.** `owner = rendezvous_hash(subscription,
   live SWIM members)`, a pure function every node computes independently. No
   vote, no term, no coordinator.
4. **A pass plans serially, delivers concurrently under a bound, applies
   serially.** Only the network call overlaps. Everything touching the engine is
   on one thread.

Two earlier designs were tried on paper and rejected, so do not re-propose them:
the *originating* node delivering (loses events when that node dies) and *every*
node delivering (N duplicates per write).

### Decisions already taken — do not re-litigate

| | |
|---|---|
| Delivery guarantee | **At-least-once.** Every event carries a stable `X-Kimmy-Event-Id` so receivers deduplicate |
| Who may register | The **`webhook`** action, independent of `watch`. Only `admin` implies it |
| Egress | Private ranges blocked by default, `webhooks.allowed_hosts` to override. Resolved address checked, every address, redirects refused |
| Where a subscription starts | **Now**, not the beginning of the oplog. Seeded at registration |
| Signing | `HMAC-SHA256(secret, timestamp + "." + body)`, timestamp inside the signature |
| Delivery concurrency | Bounded, `webhooks.max_concurrent_deliveries`, default 8 |
| An oversized document | Delivered with `fullDocument` omitted, never dropped |
| MCP tool | Deliberately not built. Registering an egress path is not a reading act |

### Where the code is

| | |
|---|---|
| `kimmy-api/src/webhooks.rs` | Registry, registration API, the `__kimmy.__webhooks` collection |
| `kimmy-api/src/dispatch.rs` | The worker, progress, delivery, signing, backoff, invalidation, limits |
| `kimmy-api/src/ownership.rs` | Rendezvous hashing over the SWIM member set |
| `kimmy-api/src/egress.rs` | Which URLs may be dialled |
| `kimmy-api/tests/webhooks.rs` | Delivery against a receiver on a real socket |
| `kimmyd/src/node.rs` | Spawns the dispatcher with the live `Members` handle |

### How this work is verified

Beyond the suite: a real receiver on a socket, verifying the HMAC exactly as a
consumer would. It has found what tests did not — per-subscription secrets, the
history-replay behaviour, and the empty `database`/`collection` fields, which
every test missed because every test asserted fields that were present rather
than reading the body a receiver actually gets. **Assert payload shape on the
wire.**

**Mutation-test anything touching delivery, egress or the planner.** Run a
no-op control first — a mutation that fails to compile produces no
`test result:` line and reads exactly like an escape — and check the mutation
actually applied before concluding a test missed it.

### Carried debt, none blocking

**The register holds zero 🔴 — M7 closed the last one, and both 🟡 planner
gaps with it.** What remains, all 🟡 in [Deviations](deviations.md): HNSW
snapshot persistence (a restart pays a rebuild on first search), SRV discovery
(needs a DNS resolver crate), a vector reindex operation, webhook ownership
hashing `SocketAddr` rather than node ids (a re-addressed node reshuffles its
subscriptions), rate limiting beyond login (waits on benchmarks), latency
histograms and oplog lag metrics, certificate reload without a restart, and —
noted while driving the server, not yet in the register — no bulk insert
endpoint.

### Invariants a change must not break

- **The multikey flag is one-way and set in the same transaction as the index
  entries.** A flag that cleared, or lagged its entries by even one commit,
  licenses a two-sided range that silently loses documents.
- **A both-bounds plan is validated in the snapshot that scans it.** A `false`
  read in an earlier transaction proves nothing about this one.
- **Index maintenance reads its definitions inside the write's transaction**,
  never from the caller's handle — a stale handle once skipped a just-created
  index entirely, unique constraint included.

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
  bucket.
- **Certificates are read before the socket is bound**, so a bad one stops the
  node rather than failing for whoever connects first (ADR-039).
- **Do not add a second native crypto stack.** `ring` is already in the build;
  anything selecting `aws-lc-rs` adds CMake for the same primitives.
- **A type that crosses a format boundary needs a chosen representation, not an
  inherited one.** `NodeId` and `CollectionId` have both cost a replication
  outage by deriving serde and letting BSON decide — particularly a `u64`, which
  BSON cannot hold above `i64::MAX`.
- **A fixture that is a hash is a sample, not a constant.** Test both halves of
  the range, and assert the fixture still has the property the test needs.
- **Webhook progress is recorded only after an endpoint accepts.** Recording
  first turns a failed delivery into a silently skipped event.
- **A node writes only its own progress record.** The moment two nodes write
  one record, last-writer-wins starts discarding delivery history.
- **A dispatch pass applies progress serially, after the concurrent join.**
  Recording inside the concurrent block would race two subscriptions' writes.
- **The resume point moves even when nothing is delivered**, on a heartbeat.
  Without it the retention horizon overtakes every healthy subscription; without
  the heartbeat an idle node writes to the oplog every tick.
- **An event is never dropped for being large.** `fullDocument` comes off; the
  event still goes.
- **The egress policy is checked before *every* delivery**, not only at
  registration, and every resolved address is checked rather than the first. A
  hostname is not a destination.
- **The delivery client resolves through `CheckedResolver`** — the egress check
  and the dial share one resolution, or a zero-TTL name gets a window between
  them. And never fall back to a default client: it follows redirects and
  resolves unchecked, which is both egress protections gone at once.
- **Webhook ownership hashes with FNV-1a, never `DefaultHasher`**, which is not
  stable between Rust versions — ownership shifting under a compiler upgrade
  would reshuffle every subscription on a rolling restart.
- **Ownership candidates are the live peers plus this node.** SWIM's live set
  never contains the node holding it, so an owner computed over it alone can
  never be `me` — the bug that silently undelivered every clustered webhook.
  Any new consumer of `Members` must know it is reading *peers*, not the
  cluster.
- **A cluster feature is not verified until the harness has run it on real
  nodes.** Transport-free tests and single-node drives both passed while
  clustered delivery was entirely broken.


## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
