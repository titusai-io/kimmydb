# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-13 — **M9 complete**; M10 planned, nothing started

**M0–M9 complete.** M9 finished on 2026-08-13 with all five tasks merged, plus
two unplanned fixes found while doing them.

**M10 is planned and no branch exists.** Start at task 1 — [Roadmap](roadmap.md)
has the board and the six reserved decisions.

**Sharding is the one thing still deliberately deferred — do not re-open it
unasked.** Replicated-not-partitioned is the current position and is considered
correct for now; the maintainer wants to decide from experience of running
KimmyDB, not from a feature comparison. The *client story* half of that old
deferral was settled by [ADR-055](decisions.md) and is what M10 carries out.

### How work runs here — read this first

The rhythm:

- **One branch per task, always off fresh `main`.** `git checkout main &&
  git pull && git checkout -b m10-<task>`.
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
  Design decisions get an ADR in `docs/decisions.md` (next number: **ADR-056**).
  M8 task 7 found the register can silently lose an entry — this file's debt
  table pointed at a 🟡 for bulk insert that had never been written down.
  Check the register itself, not the summary of it.
- **CI caches are written only from `main`.** A cache is scoped to the ref that
  wrote it and a PR can already read the base branch's, so `save-if` keeps PR
  branches from writing byte-identical copies. Before this was fixed the repo
  sat at the 10 GiB ceiling with 87% duplicates, and eviction was removing
  *main's* caches to make room for copies of them. `cache-cleanup.yml` deletes
  a PR's caches when it closes. Do not remove `save-if` to "make CI faster".

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
| `main` | PRs #16–#63 merged: all of M8 and all of M9, SWIM authentication (ADR-053), the witnessed vector (ADR-054), ADR-055 and the M10 board |
| — | No branch open. M10 task 1 is next, and its reserved decision comes first. |

### The M8 and M9 boards — all seventeen done

M8 (twelve tasks, PRs #41–#51 plus the closeout) built the cluster harness,
observability, benchmarks, HNSW snapshots, vector reindex, the provider
dialects, bulk insert, certificate reload, SRV discovery, webhook ownership by
node id and token revocation. Its ADRs are 046–052 and its lessons are in the
invariants below; the per-task narrative was retired from this file once the
milestone was two behind. [Decisions](decisions.md) and
[Deviations](deviations.md) hold the record.

| # | M9 task | |
|---|---|---|
| 1 | Computed expressions + `$addFields`/`$set` + `$replaceRoot` | ✅ #57 |
| 2 | TTL / expiring documents | ✅ #58 |
| 3 | `findAndModify` | ✅ #59 |
| 4 | Partial indexes | ✅ #61 |
| 5 | Cursors / efficient pagination | ✅ #63 |
| — | `update`/`delete` use the planner (drift found during task 3) | ✅ #60 |
| — | CI cache hygiene | ✅ #62 |

**M9 wrote no ADRs.** Every decision was recorded as a 🟢 entry in
[Deviations](deviations.md) instead, because each was a feature decision with
its reasoning rather than an architectural choice with alternatives. Next ADR
number is still **ADR-056**. If an M10 decision supersedes something — task 1's
spec approach or task 3's versioning policy plausibly will — that is an ADR.

### What the M9 branches did, and what they found

Read this before touching the query engine or the index path.

- **Task 1 — computed expressions (#57).** `kimmy-query/src/expr.rs` is a
  recursive tree: arithmetic, strings, conditionals, comparison, boolean, date
  parts, `$dateToString`, and `$literal`. Because `$group`'s `_id` and every
  accumulator argument were *already* typed as `Expr`, they gained the whole
  operator set by construction. **Found a latent `$sum` bug**: `finish()`
  carried an `all_int` flag and a comment citing ADR-002 about not losing
  precision above 2^53, while accumulating in `f64` and casting back — so a sum
  of 2^53+1 and 1 returned 9007199254740992. The comment was right and the code
  was wrong and nothing failed when they disagreed. **Deliberate behaviour
  change**: a document-valued expression now computes, so `{_id: {c: "$city"}}`
  groups by city where it used to put every input in one bucket. `$literal` was
  added beyond the agreed operator list because the list is incomplete without
  it — once `$`-strings are field paths and documents are expressions, there
  must be a way to produce the literal string `"$city"`.
- **Task 2 — TTL (#58).** A TTL index (`expireAfterSeconds` on `IndexMeta`),
  not a collection setting: the pass must *find* expired documents every tick,
  and an index range scan costs ~1.66 µs per expired document against ~8 ms per
  pass for a 10k collection scan. **One node expires a given collection**, by
  rendezvous hashing through `kimmy-api/src/expiry.rs` — every node expiring
  independently converges but produces N deletes per document. An expiry is an
  ordinary `OpKind::Delete`, because `op_kind_from_tag` rejects an unknown tag
  as *corruption* and a new variant would be a stop-the-cluster upgrade.
  **The near-miss worth knowing**: marking an expiry by putting a payload in the
  delete's body looks harmless, and `apply_remote` decodes any delete carrying a
  body as a **live document** — every marked expiry would have resurrected
  itself on every node. `kimmy_ttl_expired_total` exists so "one document, one
  delete" is measured; the cluster harness sums it across three nodes and
  requires exactly 1.
- **Task 3 — `findAndModify` (#59).** One `/find_and_modify` route, and **the
  match happens inside the write transaction**: redb has a single writer, so a
  match found inside the write cannot be taken before the commit. Atomic by
  construction, no retry loop. `MAX_CANDIDATES` (10,000) bounds the writer hold
  as a *refusal*, since choosing from a prefix would return a document the sort
  did not pick. **The crate boundary held and improved the design**:
  `kimmy-query` is a dev-only dependency of `kimmy-storage`, so query semantics
  arrive as `ModifySpec` — pure functions the engine calls inside its
  transaction, the same shape `delete_guarded` took for TTL.
- **Task 4 — partial indexes (#61).** Partial only; a sparse index is
  `{field: {$exists: true}}`. The filter language in `kimmy-core/src/partial.rs`
  is **deliberately bounded** — `$exists: true`, equality, four comparisons,
  conjunction — because general implication between filters is undecidable, and
  a best-effort containment check returns a *subset* with nothing to indicate
  it. The refusal lands at index creation. **Three things were verified rather
  than assumed**, each of which would have been silent: `bson::Document`
  round-trips losslessly through the metadata JSON as canonical Extended JSON
  (checked with a date and 2^53+1); a comparison never matches an absent field,
  which is what makes `$gt`/`$lt` safe to treat as proving existence, **except
  `{a: null}` which matches missing fields** and so contributes nothing to
  containment; and `impl Eq for Bson` exists despite the `f64`, which is what
  lets `IndexMeta` keep its derives.
- **Task 5 — cursors (#63).** An opaque token that is *the encoded document key
  of the page's last row and nothing else*. `keyenc` is order-preserving, so
  byte order is `_id` order and "next page" is a range bound storage already
  takes — no new comparison logic, no sorting, no server state, and node
  portability falls out because the token is a pure function of the `_id`.
  Measured: 100,000 documents walked in 1001 pages in 1.09 s, flat ~1 ms per
  page, against `skip` growing to 89 ms by page 500. **A design fault was found
  by writing the test**: the first draft returned `nextCursor` only when a
  cursor had been *sent*, so the test needed a magic first-page constant that no
  client could have discovered. `nextCursor` is now offered whenever the page
  filled and the query is one a cursor can continue.
- **The drift (#60).** `exec::update` and `exec::delete` never called
  `plan::choose` — they used `for_each_doc`, a straight collection scan, so an
  index never sped up a filtered update or delete. The register *had* said
  "found by a scan", but inside a row about atomicity where it read as being
  about partial application. Both now go through `collect_matching`, and both
  take `explain`. Matching alone on 20,000 documents: 17.09 ms → 0.61 ms.
  A second bug went with it — `update` counted `modified` by incrementing per
  target rather than reading the write's answer.

**The pattern across all five:** every task turned up something the code
claimed but did not do, and in four of five cases the claim was in a comment
sitting directly above the code contradicting it. Read the comment, then check
the code does what it says.

### The M10 board — nothing started, and it is next

**Theme: KimmyDB has a protocol and has never written it down.** HTTP framing,
Extended JSON v2, bearer tokens, a typed error envelope and WebSocket streaming
answer every question a wire protocol has to answer. Nothing specifies,
versions or tests it — so every client would be hand-written and nothing fails
when a route drifts. [ADR-055](decisions.md) settles the direction; this is the
work.

| # | Task | Reserved decision to settle first |
|---|---|---|
| 1 | **Protocol specification** (OpenAPI 3.1) + a drift test | Hand-written and checked, vs. generated from routes |
| 2 | **Error taxonomy as public surface** | Which errors are declared retryable |
| 3 | **Versioning and compatibility policy** | The shape of the promise |
| 4 | **Token refresh** | Refresh token vs. sliding re-issue |
| 5 | **Client-visible topology / discovery** | Static config vs. live SWIM membership |
| 6 | **Cursors at the protocol level** | — (M9 task 5 settled the shape) |
| 7 | **HTTP-level benchmark harness** | — |
| 8 | **Rust client**, `kimmy-client`; the CLI becomes its consumer | HTTP/async stack |
| 9 | **Python client** | HTTP/async stack |
| 10 | **Go client** | HTTP stack |
| 11 | **Conformance suite all three clients pass** | — |
| 12 | **One example app per language** + mutation pass and closeout | — |

**M9 is done, so nothing blocks M10.** Task 6 inherits the cursor design
directly: an opaque `_id`-order token, already node-portable, which is most of
what "cursors at the protocol level" has to decide.

**Only task 5 carries distributed-systems risk**, and it carries two known
traps at once: `Members` holds *peers only*, and the set must contain only
authenticated peers (ADR-053).

**Task 7 is the one that answers a question currently unanswerable.** Every
figure in [Benchmarks](benchmarks.md) is taken at the storage engine — nothing
measures the socket, so JSON conversion, per-request token verification, TLS
and concurrent HTTP clients are all outside the published numbers. Until it
exists there is no honest answer to "what throughput can a client expect", and
that file's own retracted-figure note is what happens when one gets quoted
anyway.

### How to size the next thing after M10

The material, if a milestone is ever needed for its own sake:

- **The carried debt below**, none of it blocking.
- **The gaps in [Testing](testing.md)** — nothing runs for long or at scale,
  nothing kills a node mid-write, multi-node tests are pairwise and
  short-lived. Every serious bug of the last two sessions lived in exactly
  those blind spots.
- **The evidence M8 and M9 both produced:** every claim that mattered and had
  no mechanism behind it turned out to be false. Four M8 branches found one,
  the two post-M8 fixes were two more, and M9 found one in every task. Prefer
  work that turns another standing assertion into something that fails when it
  stops being true.
- **Index-ordered scans**, the one piece M9 named and did not build.
  `scan_range_in` ends with `out.sort()` over document keys, so index
  candidates arrive in `_id` order rather than index order. That is why sorted
  paging still uses `skip`, and why every sorted `find` materialises its whole
  match set before sorting. Fixing it would make sorted paging constant-work
  per page *and* make every sorted query cheaper — the largest single
  performance item known to be outstanding.

### What driving a real cluster established

Beyond the suite, on 3- and 5-node clusters from a release build — worth
knowing so it is not re-derived:

- Formation, one JWT cluster-wide, DDL/document/bulk replication, LWW
  convergence under 1000 concurrent writes (~7s) and under 100 conflicting
  writes to a single key.
- Kill/rejoin, `SIGSTOP`/`SIGCONT`, webhook failover, snapshot resync past the
  retention horizon, partition heal with conflicting unique values (both
  documents survive, both nodes count the violation).
- Change-stream resume tokens are **portable across nodes** — a token from one
  node resumes correctly on any other.
- **The auto-embedding pipeline end to end**: document written with no client
  vectors → worker calls the provider → shadow collection → replicated to every
  node in ~2s → semantic search by *text* returns the right document with exact
  cosine scores. Each document is embedded **exactly once cluster-wide**, so an
  N-node cluster does not pay N× for embeddings.
- A 90-second soak with a node cycling down/up/STOP/CONT: 5,351 writes, none
  lost, converged in 42s.

**Three cautions for whoever writes the next drive script.** `PUT /docs/{id}`
without `?upsert=true` returns `200 {"matched":0}` and writes nothing — a
conflict test built on it wrote nothing at all and passed, because five nodes
agreed on an error. Shadow-collection vectors take ~2s to replicate; a check
that runs immediately reports a broken pipeline that is merely young. And a
**first request against a fresh node is slow** for reasons that are not the
feature — the cursor drive measured 20.7 ms on page zero and sub-millisecond
on every page after it. Discard the first sample or say plainly that you did.

### Where the code is

| | |
|---|---|
| `kimmy-core/src/` | Shared types no crate may own alone: `keyenc.rs` (order-preserving key encoding — the property cursors and TTL both rest on), `partial.rs` (the bounded partial-index filter language), `cursor.rs` (the paging token), `ids.rs`, `hlc.rs`, `oplog.rs` |
| `kimmy-storage/src/` | The engine: `docs.rs` (CRUD + oplog + `delete_guarded`), `index.rs` (secondary indexes, multikey, partial membership), `modify.rs` (`find_and_modify`, matching inside the write transaction), `expiry.rs` (the TTL scan), `sync.rs` (transport-free anti-entropy + `lag_behind_ms`), `vectors.rs`, `rewind.rs` |
| `kimmy-query/src/` | `plan.rs` (the rule-based planner: equality prefix, both-bounds ranges, `$in` unions, and partial-index containment), `expr.rs` (computed expressions), `aggregate.rs` (stages), `filter.rs`, `update.rs`, `shape.rs` |
| `kimmy-vector/src/` | `worker.rs` (embedding + reindex backfill), `provider.rs` (the dialects + `Auth`), `index.rs` (HNSW + snapshot save/load), `cache.rs` (`IndexCache`, snapshot adoption) |
| `kimmy-api/src/` | `exec.rs` (the single authz + query executor both edges call), `expiry.rs` (which node expires a collection), `webhooks.rs` / `dispatch.rs` / `ownership.rs` / `egress.rs` (webhooks), `metrics.rs`, `routes.rs`, `json.rs` (Extended JSON v2 at the boundary) |
| `kimmy-cluster/src/` | `membership.rs` (SWIM/foca, `Members`), `peers.rs` (`replicate`, `ReplicationConfig`), `transport.rs` (TCP framing), `health.rs` |
| `kimmyd/src/node.rs` | Wires everything: spawns cluster tasks, embedding worker, webhook dispatcher, GC, TTL expiry |
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
- **Driving a change, in general.** Every M9 branch used the same shape: build,
  start a scratch node on a spare port with a scratch `data_dir`, log in, seed
  with `/bulk` (one commit — seeding 100k documents one at a time takes
  minutes), then exercise the change *and its refusals* with `curl` or a short
  Python script. Where a claim is about cost, measure it against a **release**
  build; a debug build's numbers are anecdotes. Check the node log for
  warnings before stopping it, then `pkill -f "kimmyd --config <path>"`.
- **The suite is ~1,042 tests** across the workspace, plus 5 ignored cluster
  tests. A full `cargo test --workspace` is about two minutes.

### Carried debt, none blocking

**The register holds zero 🔴.** M7 closed the last one. What remains is all 🟡
in [Deviations](deviations.md):

| Debt | |
|---|---|
| `find {_id}` is a collection scan — the planner never consults the primary key. `GET /docs/{id}` is the point path | found during M8 task 2; still open |
| **Index scans return `_id` order, not index order** — so sorted paging still uses `skip`, and every sorted `find` materialises its whole match set | the largest known performance item; see "how to size the next thing" |
| Rate limiting covers login only | waits on a capacity decision |
| `update` and `delete` apply document by document and can stop partway — bulk *insert* is atomic, they are not. They *do* use the planner now (#60) | by design |
| Keyword search is term overlap, not BM25; chunking counts characters, not tokens; no minimum score threshold | simplifications inside working features |
| Array/set expression operators, variable binding (`$$ROOT`, `$map`, `$filter`, `$reduce`, `$let`), type conversion | outside M9 task 1's agreed list; variable binding needs an evaluation *scope*, not another operator |
| No `$vectorSearch` pipeline stage; no mTLS | not planned |
| No published protocol spec; no token refresh; clients cannot discover the cluster | **M10 tasks 1, 4, 5** |
| Client-facing throughput is unmeasured — every benchmark is engine-level | **M10 task 7** |
| Sharding | **deferred by decision** until there is operational experience |

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
- **There are two vectors, and they answer different questions.** The
  oplog-derived one is *what I can serve* and is what a peer receives from
  `AskVersions`. The witnessed one is *what I have processed* and is what
  `behind` and `lag_behind_ms` must read. Swapping them either way breaks
  something: comparing against servable re-requests everything a node
  processed without appending, forever; advertising witnessed makes peers stop
  sending entries nobody will then send (ADR-054).
- **Anything that processes a replicated entry must witness it**, on every
  branch — applied, superseded, skipped. That is why the per-entry work lives
  in `apply_one` and the witnessing sits in the caller: a `continue` that skips
  the bookkeeping is exactly how this bug existed since M4.
- **Applying replicated DDL must not log**, and must carry the originating stamp
  into any tombstone it records. Both were bugs; both have tests.
- **Retention never collects the newest oplog entry** — the clock resumes from
  it (ADR-028).
- **Anti-entropy excludes `OpKind::UniqueViolation`** — a node's own observation.
- **Collection and index ids are derived from names**, which is what lets a
  replicated entry address the same thing on every node.
- **`kimmy_api::exec` is the single authorization point** for anything a
  principal asked for. Replication goes through `apply_remote`, not `exec`.
- **A signature is not an authorization.** `TokenIssuer::verify` proves a token
  was issued here and has not expired, and nothing else — the account may be
  gone, disabled, or logged out since. The `Auth` extractor checks that, and it
  must stay there: `verify` has no engine on purpose, which is what keeps it a
  pure function (ADR-052).
- **Both things that evict cached token state are load-bearing.** Admin routes
  evict synchronously so a single node is correct with no background task; the
  oplog consumer evicts so a *replicated* edit reaches this node at all. Drop
  the first and revocation depends on remembering to spawn a task — which the
  integration tests proved is easy to forget.
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
  anything selecting `aws-lc-rs` adds CMake for the same primitives. This is
  now mostly a *feature-flag* discipline rather than a crate-choice one:
  `hickory-resolver` and `axum-server` both ship aws-lc-rs variants of features
  that look innocuous, so `default-features = false` plus an explicit list is
  the pattern. `./scripts/check-native-deps.sh` is the arbiter, and it should
  keep reporting `cc` alone.
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
- **Webhook ownership hashes node ids, never addresses**, and with FNV-1a,
  never `DefaultHasher` — which is not stable between Rust versions. Either
  mistake reshuffles every subscription for a reason that is not a membership
  change: a compiler upgrade, or a node moving without going anywhere. The
  hashed form is `NodeId`'s hyphenated string, which core fixes deliberately
  rather than inheriting from `Uuid`'s serde.
- **The SWIM identity is a wire format.** `Member` is encoded with postcard,
  which is not self-describing, so adding or reordering a field breaks
  membership across versions outright — a new node rejects an old identity
  rather than tolerating it. Any change here is a stop-the-cluster upgrade and
  needs a note in [Operations](operations.md), as ADR-040 and ADR-051 both did.
- **Ownership candidates are the live peers plus this node.** SWIM's live set
  never contains the node holding it, so an owner computed over it alone can
  never be `me` — the bug that silently undelivered every clustered webhook.
  Any new consumer of `Members` must know it is reading *peers*, not the
  cluster.
- **The SWIM member set must contain only authenticated peers.** Every
  membership datagram carries an HMAC over `cluster_secret`, verified before
  foca sees it. This is not defence in depth on top of the replication
  handshake — ownership is computed over the member set, so an unauthenticated
  peer in it wins a share of the webhook subscriptions and delivers none of
  them (ADR-053). Anything new that reads `Members` inherits this.
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
- **`Engine::insert` and `insert_many` share `insert_in_txn`**, and
  `Engine::delete` and TTL expiry share `delete_guarded`. A check added to one
  must not be added *beside* the other; the point of the helper is that one
  path cannot drift into being more permissive than its sibling.

**From M9 — the query engine and the index path:**

- **`keyenc` is order-preserving, and three features now depend on it.** Index
  ranges, TTL's expired-document scan, and cursor paging all rest on "byte
  order of encoded keys is canonical BSON order". Breaking that does not fail
  loudly; it silently returns the wrong documents.
- **A guard that decides whether to write runs *inside* the write transaction.**
  TTL's `delete_guarded` re-reads the document before tombstoning it, because
  the scan and the delete are separate transactions and a session refreshed in
  between must survive. `find_and_modify` goes further and does the whole match
  inside the write, which is what makes it atomic on a single writer.
- **A partial index may answer only a query provably contained by its filter**,
  and the filter language is bounded (`$exists: true`, equality, four
  comparisons, conjunction) precisely so containment is a decision rather than
  a guess. **Do not widen that language** without also solving general
  implication — a wrong containment check returns a subset with nothing to
  indicate it, which is the multikey failure again.
- **`{a: null}` matches an explicit null *and* a missing field**, so it can
  never prove a field exists and must contribute nothing to partial-index
  containment. Comparisons are safe to treat as proving existence because
  `condition_matches` evaluates over resolved values, which are empty when the
  path is absent.
- **A document outside a partial filter contributes no index keys at all**,
  which is what makes membership fall out of ordinary maintenance —
  `apply_entries` derives removals and insertions from the same function — and
  what keeps such a document from flipping the multikey flag.
- **`$cond` and `$ifNull` evaluate lazily.** A guard like
  `{$cond: [{$gt: ["$n", 0]}, {$divide: [100, "$n"]}, null]}` must work, not
  fail on exactly the inputs it exists to protect.
- **In expressions, null propagates but a type violation refuses.**
  `{$add: ["$typo", 1]}` is null; `{$add: ["text", 1]}` is a 400. Collapsing
  both to null makes a typo and a type error indistinguishable in the output.
- **Integer arithmetic is exact.** `$add`/`$subtract`/`$multiply` compute in
  `i64` while every operand is integral, promoting only on overflow or a
  double. `$sum` accumulates the same way. Accumulating in `f64` and casting
  back loses precision above 2^53 — that was a real bug, sitting under a
  comment saying it must not happen.
- **A TTL index is single-field and dates-only**, and a document whose indexed
  field holds a string or nothing is never expired. That is what stops a policy
  added to a heterogeneous collection from deleting everything lacking the
  field, and type ordering in the key encoding gives it for free.
- **Expiry is owned by one node per collection**, rendezvous-hashed like
  webhooks. Every node expiring independently converges but produces N deletes
  per document. `kimmy_ttl_expired_total` is what keeps that measured rather
  than asserted, and the cluster harness requires it to sum to exactly 1.
- **An expiry is an ordinary `OpKind::Delete`.** `op_kind_from_tag` rejects an
  unknown tag as *corruption*, so a new variant is a stop-the-cluster upgrade.
  And **a delete carrying a body is decoded as a live document** by
  `apply_remote` — never put a payload on one.
- **A cursor carries no server state.** It is the encoded document key of the
  page's last row, which is why it is portable between nodes. Anything that
  makes it node-specific breaks round-robin clients (ADR-055).
- **A query a cursor cannot page gets no `nextCursor` at all** — not a token
  that would silently page in `_id` order when a different sort was asked for.
- **`kimmy-query` is a dev-only dependency of `kimmy-storage`.** The engine
  does storage, not semantics. When the engine needs query behaviour it takes
  it as caller-supplied pure functions (`ModifySpec`, `delete_guarded`'s
  guard), evaluated inside its own transaction. Do not add the production
  dependency; the boundary has improved every design that met it.


## Reading order for someone new

1. **This file's first three sections** — the status, how work runs, and the
   oplog-as-spine idea. Nothing else makes sense without them.
2. **[Roadmap](roadmap.md)'s M10 board** and its reserved decisions.
3. **[Deviations](deviations.md)** — the register, not a summary of it. Every
   decision M9 made is a 🟢 entry there with its reasoning.
4. **The invariants above**, before touching the query engine, the index path
   or anything that writes.
5. **[Architecture](architecture.md)** and **[Testing](testing.md)** when you
   need the shape of a subsystem or the state of its coverage.

The two things most likely to bite someone new: a claim in a comment that the
code beneath it does not honour (M9 found one in every task), and a cluster
behaviour that passes every transport-free test while being entirely broken on
real nodes (M8 task 1 found exactly that). The harness and the live drive exist
because neither is hypothetical.

---

## Conventions for this file

Replace the sections above when a branch lands; keep only the current state.
A milestone's per-task narrative is worth keeping while it is the most recent
one and worth retiring once it is two behind — its durable lessons should have
become invariants by then. The historical record lives in
[Deviations](deviations.md) and [Decisions](decisions.md), which are
append-mostly by design.
