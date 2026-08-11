# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-11 — M8 in progress: Polish under way

**M0–M7 complete. M8 (prove, persist, polish — [Roadmap](roadmap.md)) is eight
of twelve tasks in.** The open PR is task 8; tasks 9–12 remain, and one of them
is blocked on a maintainer decision (below).

### How work runs here — read this first

The rhythm:

- **One branch per task, always off fresh `main`.** `git checkout main &&
  git pull && git checkout -b m8-<task>`.
- **Every branch lands through a PR (`gh pr create`)**, which the maintainer
  reviews and merges. Pushing a branch is not finishing it; the PR is.
- **The gate, before every commit:** `cargo fmt --all -- --check` ·
  `cargo clippy --workspace --all-targets -- -D warnings` ·
  `./scripts/check-native-deps.sh` · `cargo test --workspace`. Then **drive
  the change against a running node** — the live drive has caught what the
  suite could not on nearly every branch (empty webhook fields, the reindex
  that embedded nothing, the `open_ai` wire tag). CI additionally runs the
  cluster harness (`cargo test -p kimmyd --test cluster -- --ignored
  --test-threads=1`).
- **Every deviation from plan gets a `docs/deviations.md` entry at the time
  it is made**, 🔴 (open drift) / 🟡 (agreed deferral) / 🟢 (superseded/closed).
  Design decisions get an ADR in `docs/decisions.md` (next number: **ADR-050**).
  Task 7 found the register can silently lose an entry — the handoff's debt
  table pointed at a 🟡 for bulk insert that had never been written down.
  Check the register itself, not the summary of it.

### The one structural idea, if you read nothing else

**The oplog is the spine.** Every mutation appends exactly one durable,
HLC-ordered entry *in the same redb transaction as the change itself*. Four
independent subsystems consume that same log: change streams (WebSocket, and
resumable by token), the embedding worker, cluster anti-entropy, and the
webhook dispatcher. Two consequences that keep mattering:

- A new background consumer is "subscribe to the log", not new machinery —
  and backfill is not a special case, the consumer just starts earlier.
- **Consumers record their position after doing the work, never before.**
  Crashing replays the entry; idempotency makes the replay a no-op. Recording
  first silently skips work — the failure mode behind three separate bugs so
  far.

This is why single-instance change streams work here when they don't in
MongoDB: there the log is a byproduct of replication, so it needs a replica
set. Here clustering is a *consumer* of the log, not its cause.
[Architecture](architecture.md) and [Oplog](oplog.md) have the detail.

### State of the branches

| | |
|---|---|
| `main` | PRs #16–#47 merged: through M8 task 7 (bulk insert) |
| `m8-cert-reload` (open PR) | **Task 8.** SIGHUP *and* a 60-second mtime poll, because neither covers both deployments. Almost no new machinery: `axum-server` already held the certificate behind a per-handshake handle and already exposed a reload. A bad new certificate is refused and the old one keeps serving — the opposite of the startup rule, on purpose ([ADR-049](decisions.md)). **This handoff commit rides on it.** |

### The M8 task board

Prove and Persist are complete; Polish is under way.

| # | Task | Status |
|---|---|---|
| 1 | Cluster verification harness | ✅ #41 |
| 2 | Latency histograms + oplog lag | ✅ #42 (ADR-046) |
| 3 | Benchmark baseline + concurrent writers | ✅ #43 |
| 4 | HNSW snapshot persistence | ✅ #44 |
| 5 | Vector reindex | ✅ #45 |
| 6 | Provider dialect audit | ✅ #46 (ADR-047) |
| 7 | Bulk insert | ✅ #47 (ADR-048) |
| 8 | Certificate reload | 🔵 open PR (ADR-049) |
| 9 | SRV discovery | `dns-srv:` parses but does not resolve; needs a resolver crate. **Must not add a second native crypto stack** — keep DNSSEC features off; `check-native-deps.sh` is the arbiter |
| 10 | Webhook ownership by node id | Rendezvous hashes `SocketAddr` today, so re-addressing reshuffles subscriptions. Gossip the node id as member metadata and hash that. Verify with the harness; a mixed-version cluster produces duplicates, which at-least-once tolerates |
| 11 | **Token revocation** | ⏸ **needs a decision** — semantics. Leading candidate: a per-user token version bumped to invalidate all outstanding tokens (no per-request lookup that can miss), over a replicated deny-list. ADR first |
| 12 | Mutation pass + docs closeout | `cargo-mutants` diff-scoped over everything M8 changed; the M7 lesson says escapes hide in new callers, not old layers |

**Recommended next branch: task 9 (SRV discovery).** No decision needed; the
constraint is the whole difficulty — a resolver crate that can read SRV records
**must not add a second native crypto stack**. Keep DNSSEC features off and let
`./scripts/check-native-deps.sh` arbitrate before the work is called done.

### What the completed M8 branches did, and the bugs they found

- **Task 1 — cluster harness (`kimmyd/tests/cluster.rs`).** Spawns real
  `kimmyd` processes. Its first run found **no webhook had ever been delivered
  in any clustered deployment**: SWIM's live set holds *peers only* (foca's
  `MemberUp` never fires for the node holding it), so rendezvous ownership
  computed an owner that could never be `me`. Single-node worked (empty set →
  own everything), which is why all of M6 passed. Fixed in `ownership::owns`:
  candidates are the live peers **plus this node**. Added `kimmy_cluster_members`
  gauge (peers-only: a formed three-node cluster reads 2). `SIGSTOP`/`SIGCONT`
  stand in for a partition.
- **Task 2 — observability (ADR-046).** `kimmy_request_duration_seconds`
  histogram with **measured** buckets (the first draft was written from
  expectation and the measurement corrected it), and
  `kimmy_replication_lag_seconds` pushed from the replication loop via
  `ReplicationConfig::on_lag` — the only place a peer's version vector exists.
  Found while measuring: **`find {_id}` is a collection scan** (the planner
  never consults the primary key; `GET /docs/{id}` is the point path) — 🟡 in
  the register.
- **Task 3 — benchmarks.** Concurrent writers flat 1→8 (~300 docs/s); the
  single redb writer is shared cleanly. `scripts/bench-baseline.py record|check`
  over Criterion medians, ±50% advisory tolerance, still recorded-not-gated.
- **Task 4 — HNSW snapshots.** Graphs persist at `data_dir/hnsw/<id>/`
  (staged-and-renamed), loaded before any rebuild on the first access after a
  restart, validity checked by vector *count* (the generation counter is
  in-memory). Found: **`hnsw_rs` panics on a corrupt graph file** → the load
  runs under `catch_unwind`.
- **Task 5 — vector reindex.** Every `ConfigureVectors` oplog entry now
  triggers a collection-scan backfill in the embedding worker. Revealed the old
  "re-enable backfills from the oplog" claim was **always false** — a
  long-lived worker's position is past old entries, so enabling embedding on
  existing documents embedded nothing. Also fixed: the provider cache never
  evicted on reconfigure. Idempotency is layered: a config fingerprint per scan
  (written after completion) + the HLC staleness check per document.
- **Task 6 — provider audit (ADR-047).** Voyage is OpenAI-compatible (the
  `open_ai` dialect — note the wire tag is `open_ai`, snake_case). Cohere and
  Gemini needed dialects. Verified against **documented** shapes with fixtures,
  not live endpoints (the bar every dialect has met since M2 — a live call
  needs a paid key and egresses text to a third party).
- **Task 7 — bulk insert (ADR-048).** `POST .../coll/{coll}/bulk`, a bare
  array, one transaction. `insert` and `insert_many` share a new
  `insert_in_txn` that does everything but `begin_write`/`commit`/`abort`, so
  the batch reuses the single-document checks rather than reimplementing them.
  **176× per document at batch 1000** (291 → ~51,300 docs/sec): the marginal
  document is ~13 µs against a several-millisecond commit, so the commit was
  very nearly the *whole* cost — precisely what task 3's flat writer curve
  implied. End to end on a live debug node, 500 documents took 0.16 s in one
  request against 11.6 s as 500. Found while writing it: **the register never
  held the bulk-insert debt** the handoff's own table pointed at. Also note the
  path is `/bulk`, not `/docs/bulk`, which would shadow the document whose
  `_id` is `"bulk"`.
- **Task 8 — certificate reload (ADR-049).** SIGHUP *and* a 60-second mtime
  poll. Almost nothing was built: `axum-server` already held the certificate
  behind a handle the acceptor reads per handshake and already exposed
  `reload_from_pem_file`, so the branch is about *triggers* and the ADR is
  mostly about why there are two — there is no way to signal PID 1 of a
  Kubernetes pod, and a poll alone makes an operator wait out the interval.
  The reload parses before it stores, which is what lets a bad certificate be
  refused rather than fatal, and what absorbs the window between writing a new
  certificate and writing its key. Left undone on purpose:
  `kimmy_tls_cert_expiry_seconds` needs `x509-parser` as a new runtime
  dependency — 🟡 in the register.

### Where the code is

| | |
|---|---|
| `kimmy-storage/src/` | The engine: `docs.rs` (CRUD + oplog), `index.rs` (secondary indexes + multikey), `sync.rs` (transport-free anti-entropy + `lag_behind_ms`), `vectors.rs` (shadow collections, config, fingerprint), `rewind.rs` (point-in-time restore) |
| `kimmy-query/src/plan.rs` | The rule-based planner: equality prefix, both-bounds ranges, `$in` unions |
| `kimmy-vector/src/` | `worker.rs` (embedding + reindex backfill), `provider.rs` (the dialects + `Auth`), `index.rs` (HNSW + snapshot save/load), `cache.rs` (`IndexCache`, snapshot adoption) |
| `kimmy-api/src/` | `exec.rs` (the single authz + query executor both edges call), `webhooks.rs` / `dispatch.rs` / `ownership.rs` / `egress.rs` (webhooks), `metrics.rs`, `routes.rs` |
| `kimmy-cluster/src/` | `membership.rs` (SWIM/foca, `Members`), `peers.rs` (`replicate`, `ReplicationConfig`), `transport.rs` (TCP framing), `health.rs` |
| `kimmyd/src/node.rs` | Wires everything: spawns cluster tasks, embedding worker, webhook dispatcher, GC |
| `kimmyd/tests/cluster.rs` | The multi-node harness |

### How to run and verify things

- **Scratch server:** `KIMMY_ROOT_PASSWORD=pw ./target/debug/kimmyd --config
  <toml>` with a non-default port and scratch `data_dir` (7878 may collide).
  Log in at `POST /v1/auth/login`; the bootstrap user is `root`.
- **Cluster harness:** `cargo test -p kimmyd --test cluster -- --ignored
  --test-threads=1`. A node with `cluster.enabled` and no seeds refuses to
  start, so the harness pre-allocates ports and cross-seeds.
- **Benchmarks:** `cargo bench -p kimmy-storage -p kimmy-vector`, then
  `scripts/bench-baseline.py check`.
- **Mutation testing:** `cargo mutants --file <f> -o <outdir> -- -p <pkgs>`
  (installed). `--in-diff <diff>` scopes to a diff. No `--no-fail-fast` flag —
  that habit belonged to the retired hand-rolled harness; a non-compiling
  mutant reports `unviable`. Some escapes are *equivalent mutants* no test can
  kill — prove it, don't chase it.
- **Live provider drive:** a fake HTTP endpoint speaking a dialect's shape;
  set the key env var (`OPENAI_API_KEY`, `COHERE_API_KEY`, `GEMINI_API_KEY`)
  before launching the node.

### Carried debt, none blocking

**The register holds zero 🔴.** M7 closed the last one. What remains, all 🟡
in [Deviations](deviations.md), with the M8 tasks that would close them:

| Debt | |
|---|---|
| SRV discovery parses but does not resolve | M8 task 9 |
| Webhook ownership hashes `SocketAddr`, not node ids | M8 task 10 |
| Deleting a user does not invalidate issued tokens | M8 task 11 |
| `find {_id}` is a collection scan — the planner never consults the primary key | not in M8; found during task 2 |
| Rate limiting covers login only | waits on a capacity decision, not on measurement any more |
| `update` and `delete` still apply document by document and can stop partway — bulk *insert* is atomic, they are not | by design |
| Keyword search is term overlap, not BM25; chunking counts characters, not tokens; `skip` is O(n); no minimum score threshold | simplifications inside working features |
| No `$vectorSearch` pipeline stage; no mTLS; no computed pipeline expressions | not planned |

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
  node rather than failing for whoever connects first (ADR-039). **At reload
  the rule inverts**: a bad certificate is refused and the one already serving
  stays. Both are right — startup has nothing to fall back to and a serving
  node does — and the reload half is what makes a botched rotation survivable
  rather than an outage (ADR-049).
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
- **Correctness never depends on an HNSW snapshot.** A corrupt one is deleted
  and rebuilt (the load runs under `catch_unwind` because `hnsw_rs` panics on
  bad magic), a behind one serves once and rebuilds, a missing one builds, and
  a restore carries none. A snapshot whose metric or dimension disagrees with
  the live config is refused before the graph is read.
- **Vector backfill scans the collection, never the oplog.** The oplog may
  have been collected; the documents are the durable source. The config
  fingerprint is written **after** the scan completes — recording it first
  would leave the remainder embedded under the old model with nothing to
  notice.
- **A provider is cached against the configuration that built it.** A
  reconfigured collection must not keep embedding through the old provider.
- **Replication lag is pushed from the replication loop, never computed in the
  API layer** — that loop is the only place a peer's version vector exists. An
  unreachable cluster reports its *last* value, not zero: an outage has
  unknown lag, and zero would read as perfect health.
- **Health and metrics routes stay out of the latency histogram** but stay in
  the request counter. They fire every few seconds forever and would crowd the
  buckets real traffic lands in.
- **A metric's buckets or thresholds are measured, not chosen.** ADR-046's
  first draft was written from expectation and the measurement corrected it.
- **A bulk insert is one transaction and one commit, and a failure anywhere
  aborts all of it** — every document, and every oplog entry with them. The
  version vector must not move for a batch that did not land, and nothing may
  be published. Events go out once, after the commit, one per document.
- **A batch is validated against itself, not only against stored state.** Two
  documents sharing an `_id`, or colliding on a unique index, must fail the
  batch. This works only because a redb read sees its own transaction's
  uncommitted writes — the property the whole reuse of the single-document
  checks rests on, so both paths have a test.
- **`Engine::insert` and `insert_many` share `insert_in_txn`.** A check added
  to one must not be added *beside* the other; the point of the helper is that
  a batch cannot drift into being more permissive than a single insert.


## Conventions for this file

Replace the section above when a branch lands; keep only the current state.
The historical record lives in [Deviations](deviations.md) and
[Decisions](decisions.md), which are append-mostly by design.
