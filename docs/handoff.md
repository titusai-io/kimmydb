# Handoff — where development stands

[← Documentation index](README.md)

A running note for picking work back up. Updated at the end of each branch.

---

## As of 2026-08-14 — **M10 complete**; the closeout is on a branch awaiting review

**M0–M10 complete — every milestone that was ever planned.** Tasks 1–11 are
merged (#65–#76); task 12 — the examples, the mutation pass and this closeout —
is on `m10-examples-closeout` as **PR #77**, open and unmerged.

**KimmyDB now has a written, versioned and tested client protocol, and three
first-party clients held to one set of scenarios.** What to do next is an open
question rather than a plan: see "How to size the next thing" below, which is
now the most useful section of this file.

Every reserved decision so far went to the maintainer first, as every M8 and M9
one did. Task 1: hand-written specification, checked by a contract test that
validates **live responses**, not only the route inventory ([ADR-056](decisions.md)).
Task 2: a typed `ErrorCode` enum, retryability **three-valued** — `no` / `wait`
/ `elsewhere` — because leaderless replication makes "ask a different node" a
real answer ([ADR-057](decisions.md)). Task 3: `/v1` does not break, and a node
advertises **capabilities** rather than expecting clients to map versions to
features ([ADR-058](decisions.md)).

Task 4: **sliding re-issue** of the access token rather than a second
credential, with no grace for an expired one ([ADR-059](decisions.md)).

Task 5: **addresses from a replicated registry, liveness from SWIM**, because
reading client addresses out of membership was not available at any acceptable
price ([ADR-060](decisions.md)).

Task 6: the paging contract, and **node portability tested on three real nodes**
rather than argued from the encoding.

Task 7: the HTTP benchmark harness, and the numbers it produced.

Task 8: `kimmy-client`, and `kimmy-cli` converted into a consumer of it.

Task 9: `clients/python`, package `kimmydb`, synchronous on `httpx` and
`websockets`, with its own CI job driving a real node.

Task 10: `clients/go`, package `kimmydb`, one dependency, with its own CI job.

Task 12: `shelf`, one application written three times and run in CI, plus the
mutation pass and this closeout.

**There is no M11.** The milestone that has just finished was the last one
planned, and choosing the next thing is a decision for the maintainer rather
than something to infer. The material for that choice is in
[Roadmap](roadmap.md) and in "How to size the next thing" below — the carried
debt, the gaps in [Testing](testing.md), and the one performance item M10
measured and did not chase.

**Sharding is the one thing still deliberately deferred — do not re-open it
unasked.** Replicated-not-partitioned is the current position and is considered
correct for now; the maintainer wants to decide from experience of running
KimmyDB, not from a feature comparison. The *client story* half of that old
deferral was settled by [ADR-055](decisions.md) and is what M10 carries out.

### Getting oriented

#### 1. Find out where the tree actually is

```bash
cd /path/to/kimmydb
git checkout main && git pull
gh pr list --state open            # is #77 still open?
gh pr view 77 --json state,title
```

**If #77 is still open**, the tree you want is that branch and the work is
waiting on review.

**If #77 is merged**, `main` holds all of M0–M10 and there is **no next task
queued**. That is the real state: the project has finished everything that was
ever planned, and what happens next is a decision the maintainer makes.

#### 2. What this project is, in one paragraph

KimmyDB is a document and vector database in Rust — MongoDB-shaped query
surface, leaderless replication, HNSW vector search with server-side embedding,
change streams, webhooks, and a full HTTP/WebSocket protocol. Eleven crates in
one workspace, three first-party clients (Rust, Python, Go), ~1,100 tests, and
a specification (`docs/openapi.yaml`) that a contract test holds to the running
server. **[The oplog is the spine](#the-one-structural-idea-if-you-read-nothing-else)**
— read that section, because nearly every subsystem is a consumer of one log
and the design only makes sense once that lands.

#### 3. Verify the tree is healthy before you believe anything

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
./scripts/check-native-deps.sh          # must report `cc` alone, nothing more
cargo test --workspace                  # ~2 minutes, ~1,103 tests
```

Then, if the change is anything but documentation, **drive a real node** — the
live drive has caught what the suite could not on nearly every branch. "How to
run and verify things" below has the recipes, including the cluster harness,
the three client suites, the conformance runner and the examples.

#### 4. What is already decided — do not re-open these unasked

- **Sharding.** Replicated-not-partitioned is the position, deliberately, until
  there is operational experience. Not an oversight.
- **The 60 ADRs in [Decisions](decisions.md).** M10 added ADR-056 through
  ADR-060: the hand-written specification, the three-valued retry taxonomy,
  path-major versioning with capability discovery, sliding token re-issue, and
  the replicated node registry. Each was put to the maintainer first.
- **The register in [Deviations](deviations.md)** holds **zero 🔴**. Everything
  open is a 🟡 that was agreed. Read the register itself, not a summary — M8
  found a debt that existed only there.

#### 5. The two failure modes that keep catching people here

**A claim with no mechanism behind it is usually false.** M8 found one in four
branches, M9 in every task, M10 in four more — including a sentence in a PR
description that turned out to describe a system that does not exist, and a
specification paragraph about creating a collection twice that no test had ever
done. When you find an assertion in a comment, a doc or a spec, the useful
question is "what fails if this stops being true?" If the answer is "nothing",
that is the next bug.

**Transport-free tests agree with each other and lie about clusters.** Three
separate defects passed every single-node test and were only ever visible on
real nodes — most recently a replicated schema change that was appended to the
oplog and never published, so a dropped collection ended its own node's
watchers and left every other node's hanging. Use `cargo test -p kimmyd --test
cluster -- --ignored --test-threads=1`.

#### 6. Picking the next milestone

"How to size the next thing after M10" below is written for exactly that, and
the strongest single candidate named in it is **index-ordered scans** —
`scan_range_in` sorts by document key, so sorted paging still uses `skip` and
every sorted `find` materialises its whole match set. It is the largest known
performance item and it makes two things better at once.

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
  Design decisions get an ADR in `docs/decisions.md` (next number: **ADR-061**).
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
| `m10-protocol-spec` | ✅ Merged as #65. M10 task 1: `docs/openapi.yaml`, its contract test, ADR-056 |
| `m10-error-taxonomy` | ✅ Merged as #66. M10 task 2: `ErrorCode` as an enum, the three-valued retry class, `JsonBody`, ADR-057 |
| `m10-versioning` | ✅ Merged as #67. M10 task 3: `docs/compatibility.md`, `GET /v1/version`, the `Capability` enum, ADR-058 |
| `m10-token-refresh` | ✅ Merged as #68. M10 task 4: `POST /v1/auth/refresh`, `expiresIn`, ADR-059 |
| `m10-topology` | ✅ Merged as #69. M10 task 5: `GET /v1/topology`, the node registry, `server.advertise`, ADR-060 |
| `m10-protocol-cursors` | ✅ Merged as #70. M10 task 6: the paging contract, and cross-node paging in the harness |
| `m10-http-bench` | ✅ Merged as #71. M10 task 7: the HTTP benchmark, and the numbers in [Benchmarks](benchmarks.md) |
| `m10-rust-client` | ✅ Merged as #72. M10 task 8: `kimmy-client`, the CLI converted, [Clients](clients.md) |
| `m10-python-client` | ✅ Merged as #73. M10 task 9: `clients/python`, the `kimmydb` package, a CI job |
| `drop-invalidates-change-streams` | ✅ Merged as #74. Three change-stream defects: the drop invalidate, the unpublished replicated DDL, and a recreated collection serving the dead one's history |
| `m10-go-client` | ✅ Merged as #75. M10 task 10: `clients/go`, package `kimmydb`, a CI job |
| `m10-conformance` | ✅ Merged as #76. M10 task 11: `clients/conformance`, three drivers, a CI job |
| `m10-examples-closeout` | **Open as PR #77, not merged.** M10 task 12: `shelf` in three languages, the client mutation pass, the closeout. The server-side mutation pass is *not* in it — see the debt table |

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
its reasoning rather than an architectural choice with alternatives. **M10 in
turn wrote five** — ADR-056 through ADR-060 — because each of its reserved
decisions had real alternatives to weigh rather than being a feature choice
with a reason. Next ADR number is **ADR-061**.

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

### The M10 board — all twelve done

**Theme: KimmyDB has a protocol and has never written it down.** HTTP framing,
Extended JSON v2, bearer tokens, a typed error envelope and WebSocket streaming
answer every question a wire protocol has to answer. Nothing specifies,
versions or tests it — so every client would be hand-written and nothing fails
when a route drifts. [ADR-055](decisions.md) settles the direction; this is the
work.

| # | Task | Reserved decision to settle first |
|---|---|---|
| 1 | ✅ **Protocol specification** (OpenAPI 3.1) + a contract test | Settled: hand-written, checked by inventory *and* live responses (ADR-056) |
| 2 | ✅ **Error taxonomy as public surface** | Settled: three-valued `no`/`wait`/`elsewhere`, closed by an enum (ADR-057) |
| 3 | ✅ **Versioning and compatibility policy** | Settled: path-major, `/v1` never breaks, capabilities over version numbers (ADR-058) |
| 4 | ✅ **Token refresh** | Settled: sliding re-issue, no grace for an expired token (ADR-059) |
| 5 | ✅ **Client-visible topology / discovery** | Settled: replicated registry for addresses, SWIM for liveness (ADR-060) |
| 6 | ✅ **Cursors at the protocol level** | — (M9 settled the shape; this settled the wire contract) |
| 7 | ✅ **HTTP-level benchmark harness** | — |
| 8 | ✅ **Rust client**, `kimmy-client`; the CLI is its consumer | Settled: `reqwest`, already in the tree |
| 9 | ✅ **Python client** | Settled: `httpx` + `websockets`, sync first |
| 10 | ✅ **Go client** | Settled: stdlib `net/http` + `coder/websocket` |
| 11 | ✅ **Conformance suite all three clients pass** | — |
| 12 | ✅ **One example app per language** + mutation pass and closeout | — |

**All twelve landed, in twelve PRs (#65–#77) plus one unplanned fix (#74).**
Task 12 is the only one not yet merged. The protocol is now written down
(`docs/openapi.yaml`), versioned, and held to the running server by a contract
test — so a route that drifts fails a build instead of silently breaking a
client.

### What task 1 did, and what it found

`docs/openapi.yaml` describes every route the router registers — the auth flow,
Extended JSON v2, the error envelope, cursors, and the WebSocket frame shapes
that OpenAPI has nowhere else to put. `/mcp` is deliberately absent: a
different protocol, config-gated, specified by MCP itself.

**The document is not the deliverable; `crates/kimmy-api/tests/openapi.rs` is.**
It checks inventory both directions, then drives every documented operation
against a real server and validates each response against the declared schema,
and **ends by asserting every documented operation was exercised** — so a route
cannot be added to the router and the spec without also being driven. That
last assertion is what every later task pays: changing the wire now means
editing the document and the scenario together.

Three findings, all from the live half, all in the same shape as M9's:

- **`PUT /docs/{id}` returned `matched` and `modified` as booleans** while
  `/update` and `/find_and_modify` return them as counts — one field name,
  two types, on one protocol, because the route serialized `WriteOutcome`'s
  bools straight through. **The route had no integration test at all**, and
  this file described it as `{"matched": 0}`, which was wrong in a way nothing
  could contradict. Normalized to counts by the maintainer's decision, at the
  cheapest possible moment: no client exists and nothing in-tree reads them.
  `upserted` stays a boolean because it is one.
- **`GET /v1/users` returns names, not user objects** — the spec's first draft
  was written from the handler rather than the store.
- **The M8 inventory test had a hole.** It matched `.route("` at the start of a
  line, so it skipped the three registrations rustfmt breaks across lines,
  including `/docs/{id}`. Green while never checking the busiest route on the
  API. There is one scanner now, in the new test, and it covers `http-api.md`
  too.

**Verified beyond the suite**, as every branch is: a release node on port 7911,
driven by a Python script that parses `docs/openapi.yaml` itself and checks 32
real responses against it — a second reader, independently implemented, of the
same document. It also walked a two-page cursor and confirmed the three
`PUT` shapes on the wire. No disagreements, and the node log was clean.

### What task 2 did, and what it found

The seventeen error codes are a closed set: `ErrorCode` is an enum, and both the
wire string and the **retry class** come from exhaustive matches on it, so a new
code does not compile until both are answered. The class is `no`, `wait` or
`elsewhere`, and it **rides in the envelope** — `{"error", "message", "retry"}`
— so a client acts on it without a table of codes compiled at release time.
That is what has to be true for task 3 to call "adding a code" additive.

`elsewhere` exists because this is a leaderless cluster. `internal`,
`misconfigured` and `snapshot` are conditions of the node that answered, and a
peer holds the same data; a boolean `retryable` would tell a client to retry the
machine that just failed it. [ADR-057](decisions.md) has the full division.

Six findings, and **four of them came from the live drive, not the suite**:

- **`no_vectors` was in neither document** — the enumeration's first catch, and
  the reason the roadmap's "enumerate it" had to be exhaustive over the code
  rather than over `error.rs`.
- **422 was in the prose reference and specified nowhere.** Seventeen
  operations can return it; all seventeen declare it now.
- **Sixteen routes were outside the taxonomy entirely.** Axum's body rejection
  is bare text, and the M5 mapping that fixes it is only reached by a handler
  taking `Result<Json<T>, JsonRejection>` — **one handler of nineteen did**. It
  now lives in an extractor, `json::JsonBody<T>`, which cannot be used without
  it. The conformance test had only ever driven a wrong-shaped body against
  `/bulk`, the one route that was right.
- **`/watch` refused non-upgrade requests with no envelope at all**, same
  reason, same fix.
- **The specification had the OpenAI provider tag as `openai`.** It is
  `open_ai`; `openai` is the *display* name from `ProviderConfig::name()`, and
  the spec was written from it. This project had already been caught by that
  exact distinction once.
- **`no` is not a string in YAML 1.1.** `enum: [no, wait, elsewhere]` reads as
  `[False, "wait", "elsewhere"]` in PyYAML and as the string in Rust's reader,
  so two readers of the specification disagreed about a value that goes on the
  wire. Quoted now. Nothing inside the Rust test could have seen it — this is
  the argument for the drive being an *independently written* second reader,
  not a rerun of the same logic.

**Verified beyond the suite**: two release nodes, two scripts. The task 1 drive
re-run unchanged (32 responses, no disagreements) as a regression against the
new envelope, and a second that parses the retry table out of the specification
and checks it against the wire — seven codes observed, including `misconfigured`
as `elsewhere`, provoked by pointing a collection at an API key environment
variable the node does not have.

### What task 3 did, and the shape it sets for tasks 4–12

[Compatibility](compatibility.md) is the policy: the path carries the major,
`/v1` does not break, additive changes ship in it without ceremony, and
anything breaking mints `/v2` served alongside for at least one minor line and
six months. Date-versioned requests were rejected — a shim layer per released
version has to be *exercised* to mean anything, and that is a permanent tax
taken on before the first client exists.

**`GET /v1/version` is the load-bearing half, and it advertises capabilities
rather than a number.** Nodes are upgraded one at a time, so a client that
round-robins meets nodes of different ages; "does this node have the feature I
am about to use" is not a question a version number answers unless the client
also carries a version→feature table, which is the table that goes stale in
every client independently. `Capability` is an enum checked against the
specification, like `ErrorCode`.

**Every client-visible feature from here on owes a capability**, and tasks 4
and 5 are the first two — refresh and topology are exactly the things a client
must detect rather than assume.

**Four claims became mechanism**, which is the part worth keeping:

- Every versioned route is under `/v1/`, and the prefix agrees with the
  server's reported protocol *and* `info.version` in the specification.
- The advertised capabilities are the documented ones, each with an
  explanation rather than only a name.
- **No response schema forbids unknown properties.** Without it, "a new
  response field is additive" is false for any client that validates — it would
  break on the next field added, silently, and only for them.
- The default build must not advertise `local-embeddings`, which is what proves
  the list is answered per build rather than asserted.

**Two things stay prose and are marked as such** in the policy: the six-month
window, which is a promise about calendar time, and "changing what a route
means is breaking", which nothing reading shapes can detect. Naming them is the
point — the failure this milestone keeps finding is a claim that reads like a
mechanism and is not one.

### What task 4 did

`POST /v1/auth/refresh` exchanges a valid token for a fresh one — **sliding
re-issue, not a second credential**. Nothing new for a client to store, no
second lifetime, nothing kept server-side. A stored rotating refresh token was
rejected for an architectural reason worth remembering: rotation is a
compare-and-set on a replicated record, which a leaderless store does not
offer, and two concurrent rotations resolve by last-writer-wins — discarding a
credential a client is holding ([ADR-059](decisions.md)).

**The security half is structural rather than written.** The route takes the
`Auth` extractor, so a token whose account was deleted, disabled, or had its
password or grants changed is refused *before the handler runs*. Refresh cannot
launder a revoked session by construction, not by remembering to check — which
matters, because the remembering version is one deleted line away from being an
indefinite session-laundering endpoint.

**Three deliberate non-features**, all now written down and tested:

- The old token keeps working until it expires. A stateless token cannot be
  recalled; ending a session early is what the version bump is for.
- A grant change stops refresh along with everything else, because grants live
  in the token. That is the cost of carrying them rather than looking them up.
- No grace for an expired token, so `exp` means one thing on every route. A
  client idle past the lifetime logs in again — a thing a library may ask of an
  application, where re-sending credentials hourly is not.

**`expiresIn` is reported at login and refresh**, so a client never decodes a
token it is told to treat as opaque. It is also the first spend of the
compatibility promise written one task earlier: a new response field, shipped
in `/v1` without ceremony.

### What task 5 did — the one with distributed-systems risk

`GET /v1/topology` lists the cluster. **Addresses come from a replicated
registry** each node writes itself into; **liveness comes from SWIM**. Two
sources because neither answers alone, and because the single-source version
was not available: `Member` carries the *gossip* address and is postcard-
encoded, so putting a client address in it is a stop-the-cluster upgrade — the
fourth in three milestones — and inferring one from the gossip address is a
guess that breaks on separate interfaces, TLS termination and container
networking ([ADR-060](decisions.md)).

**Both inherited traps were handled on purpose, not survived by luck:**

- `Members` holds **peers only**, so the answering node is added explicitly and
  listed first. A list derived from membership alone would tell a client the
  cluster does not include the node it is talking to.
- The set holds **only authenticated peers** (ADR-053), and that invariant now
  guards something new: an unauthenticated peer would be advertised to clients
  as a node to send credentials to.

**`status` is `live` or `unknown`, never `down`**, because a node whose gossip
is partitioned while its HTTP works is a real state here. Hiding it removes an
option exactly when a client wants one.

**The registry is an address book, not a heartbeat** — written at startup and
only when the content changes, so an idle cluster appends nothing. Freshness of
liveness is SWIM's job.

**Verified in the cluster harness**, which is the only thing that could verify
it: three real nodes, each listing all three as `live` with real addresses —
including a node whose seed list never named it, which only replication
explains — then a token from one node used at every advertised address, then a
node killed and required to read `unknown` rather than vanish. All six harness
tests pass.

### What task 6 did

M9 built cursors and documented them well — as *engine* behaviour. The wire
carried three claims nothing checked, and the specification said nothing about
page size at all.

**Node portability is now tested, not argued.** A harness test walks a
collection across three nodes, changing node every page, and requires the walk
to see every document exactly once and in order. The claim was sound — a token
is a pure function of the `_id` — but the protocol now *tells* clients to
round-robin, so paging that broke when they did would be a data bug reached by
following the protocol's own advice.

**Two silent behaviours were in no specification**, both of the kind a client
author meets in production:

- **An unlimited `find` returns 100 documents, not all of them.** The prose
  said so; the machine-readable document a client is generated from did not.
  `count` has no cap and is the honest source for a total.
- **A `limit` over 10,000 is clamped rather than refused** — the request
  succeeds and returns less than was asked for.

**And one real trap, now stated and tested:** a final page that is exactly full
still carries a token, because the server cannot know it is the last without
looking further. **A client ends its walk on a short or empty page, not on a
token no longer being offered.**

**One property is documented rather than enforced, on purpose.** A token is a
*position*, not a query: sent with a different filter it resumes that filter
after the same key. Enforcing it would mean putting the query inside the token,
which makes it large and gives clients structure to depend on in something they
are told to treat as opaque.

### What task 7 did, and the one thing it could not explain

`cargo bench -p kimmyd --bench http` spawns the **shipped binary** and drives it
with concurrent clients over a real socket — plaintext and TLS, reads and
writes, warm-up discarded, percentiles rather than a mean. Not a Criterion
bench: throughput under contention is not a shape Criterion measures. Recorded,
not gated, like everything else in [Benchmarks](benchmarks.md).

- **TLS is close to free** — within noise at one client, ~10% at thirty-two.
- **The protocol costs ~0.1 ms per request**, measured as a point read's p50.
- **Reads scale, writes do not**: 8,001/s → 70,660/s against 143/s → 602/s,
  with write p99 going 10 ms → 246 ms. One redb writer, experienced by a client
  as tail latency.
- **`count` is a collection scan** at 30/s over 10,000 documents, and barely
  scales. Worth knowing before a client polls one.

**And a gap it could not explain.** A single insert is 7.0 ms over HTTP against
~3.4 ms at the engine. It is *not* protocol overhead — the read numbers bound
that at 0.1 ms — and not per-document encoding, since the gap is fixed per
request rather than growing with batch size. Candidates: the background oplog
consumers a daemon runs and a bare `Engine` does not, or a commit's fsync on a
runtime worker thread. **Recorded as a 🟡 question**, because a cause that has
not been measured is not a cause. Halving it would double single-write
throughput for every client that does not batch.

### What task 8 did — the first client, and what it found

`kimmy-client` holds a token and refreshes it, fails over between nodes
discovered from `/v1/topology`, pages with cursors, returns typed errors
carrying the retry class, and resumes change streams. Each of those is a server
promise from an earlier task, which is what tasks 1–7 were for.

**It depends on no `kimmy-*` crate, and a test keeps it that way.** That is the
property that makes it a *check* rather than only a convenience: it sees what
the Python and Go clients will see. A shared type would make this the one
client that works for a reason the others cannot have.

**Retries are deliberately conservative.** A read moves to another node on
`elsewhere`; a write does not, because `elsewhere` says *this node* did not
answer, not that the work did not happen — and no status distinguishes an
insert that failed before its commit from one that failed after. A caller who
knows the request is idempotent says so, and an insert carrying its own `_id`
qualifies while one without does not.

**Converting the CLI found three defects**, which is exactly why the roadmap
made it the first consumer:

- **`Client::request` took a `reqwest::Method`**, putting the HTTP stack in the
  public API — every consumer had to depend on `reqwest` to name a verb. The
  crate has its own `Method` now.
- **Login did not fail over.** It tried only the first endpoint, so a client
  handed a list whose first address was dead could not authenticate at all —
  the one failure that makes every other endpoint useless. Found by a test
  putting a dead address in front of a live one.
- **The CLI could not create a collection.** On a fresh database the first
  `insert` failed with "collection not found" and offered nowhere to go but
  `curl`. Found by driving the converted binary.

`kimmy watch` and `kimmy topology` came along with the conversion, because the
client made them a few lines each.

### What task 9 did, and the two things it found

`clients/python`, package `kimmydb`, synchronous, on `httpx` and `websockets`.
Both were chosen for the same reason: each has a sync *and* an async API behind
nearly the same surface, so "sync first" costs nothing when async is wanted.
The stdlib was rejected for a measured reason rather than a taste — `urllib`
opens a connection per request, and against a ~0.1 ms request a handshake per
call would dominate everything.

**It shares no code with the Rust client, and passes the same scenario list.**
That is the arrangement task 11 depends on: two independent readers of one
specification, so a disagreement between them means something.

**The surface is Python rather than a translation** — iteration where a Python
caller expects it, `documents()` for the shape most callers want, exceptions
rather than returned errors, and `.code` as a plain string because codes are
additive.

Two findings:

- **A lazy change stream misses events.** Python's natural shape is a
  generator, which would open the socket at the first `next()` — so anything
  written between `watch()` and that read is lost silently. It connects when it
  is asked for now. Found by a test that hung for ten minutes.
- **A dropped collection leaves a stream open and silent.** No event, no close,
  no error: the stream waits for changes to something that no longer exists.
  This is a *server* behaviour — change streams carry data, not DDL, and the
  only invalidate reasons are `ConsumerLagged` and `ResumeTokenExpired` — and
  it surprises anyone arriving from MongoDB, where dropping a collection
  invalidates its streams. Recorded as a 🟡 and asserted in both clients'
  tests, so the day it changes something fails. **Whether it should change is
  worth a decision**, and it is a change-stream question rather than a client
  one.

**CI runs the Python tests against a real `kimmyd`**, on the same reasoning as
every other client test here: a mocked server asserts only what the client
already believes.

### What task 10 did

`clients/go`, package `kimmydb`, **one dependency** — Go's `net/http` pools
connections, so the reasoning that ruled out Python's standard library does not
apply, and the only thing missing is WebSocket framing.

`coder/websocket` rather than `gorilla/websocket` for a reason specific to this
design: it handshakes through an ordinary `*http.Client`, so a change stream
inherits the same client, TLS configuration, proxy and timeouts as every other
request. `gorilla` dials with its own `Dialer` — two configurations that can
drift, which is a split this project has paid for before.

**Idioms rather than a translation.** Paging and streaming are
range-over-function iterators, so the error is the second loop variable rather
than something a caller has to remember; everything takes a `context.Context`,
including the change stream, and cancelling it is how watching stops.

**It found nothing new, and that is the result.** The roadmap put Go third
because it was least likely to surface a gap the other two had missed. Two
clients and a specification had already taken the surprises, and the third
agreeing is the evidence task 11 rests on.

### What task 11 did, and the defect it exposed

`clients/conformance` holds all three clients to **one** set of scenarios. A
declared list with the observations a correct client must produce, a small
driver per client that runs a named scenario and prints what it saw, and a
runner that starts a fresh node per scenario and compares. Sixteen scenarios,
three clients, forty-eight runs.

**A driver reports; it never judges.** Three clients that each decided whether
they had passed would be three opinions rather than one oracle and three
answers. Coverage is checked separately from behaviour, because a client that
stops implementing a scenario must fail rather than fall silent.

**Shown to go red.** The Python driver, made to stop one page early, produced
`documents_seen: expected 250, observed 200` while the other two passed. A
suite that has never failed is a suite nobody has tested.

**And it found a real defect on its first full run.** The specification had
said since M10 task 1 that collection creation is idempotent — "Created, or
already present" — and the server has always returned `409 conflict`. Both
clients repeated the claim in a comment.

**Nothing caught it because nothing ever created a collection twice.** The
contract test's coverage assertion checks that every *operation* is exercised,
not that every documented *outcome* is, so a false sentence about the second
call went unchallenged through four tasks. It is now corrected in the
specification, in both clients, in the contract test, and as a conformance
scenario — four places, because the claim was in four places.

**Worth carrying forward:** a documented outcome that no test produces is the
next place to look for this kind of error.

### The change-stream fix, and the second defect it exposed

The 🟡 above lasted a day. A dropped collection now ends the streams watching
it — `InvalidateReason::CollectionDropped`, decided **in storage** rather than
at the HTTP edge, because that is where the other two reasons live and where
`finished` is set. Scoped: only a stream watching that collection ends, so a
`Cluster` stream — the embedding worker's — is untouched, and a test says so.

The sharp part was never the stall. **Ids are derived from `(database,
name)`**, so a collection recreated under the same name has the same id, and
the old stream would silently resume delivering for it — one stream spanning
two different collections with nothing in between.

**Then fixing it exposed a second defect.** A replicated schema change was
appended to the receiving node's oplog but **never published**, while a
replicated document was. So a drop ended its own node's watchers immediately
and left every other node's waiting for an unrelated write to nudge them.
Invisible for as long as streams filtered DDL out, because "delivered late" and
"not delivered" looked identical — and **only the cluster harness could have
found it**, since a single node applies its own drop directly. That is the
third time the harness has caught something every transport-free test agreed
was fine.

**One representation fixed on the way past.** The invalidate reason went onto
the wire through `{:?}`, so renaming a variant would have silently renamed a
value clients branch on. It has an `as_str` now, with the existing two names
kept exactly as `Debug` rendered them — the invariant `NodeId` and
`CollectionId` each cost a replication outage to learn.

**And a third defect, older than both, found by checking a claim.** A pull
request description said a client could resume past an invalidate and would
replay to it again. Probing a real node showed that was not what happened —
and what *did* happen was worse. Because ids are derived from
`(database, name)`, a recreated collection reuses its id, so the oplog still
held the dead incarnation's entries and streams still matched them:
`from_start` on a healthy recreated collection replayed a dead collection's
documents and then invalidated immediately, never showing the live data.

A stream now never reads across a drop, and a resume token from before one is
**refused** with `resume_token_expired` rather than moved forward silently —
between that token and this collection's first event is a gap, and a silent gap
is what the invalidate machinery exists to prevent.

**The pattern across all three:** each was found by asking a running node what
it did, not by reading what it was supposed to do. The third one came from
verifying a sentence I had written in a PR description, which turned out to
describe a system that does not exist.

**Task 5 carried the milestone's distributed-systems risk and is done.** Both
traps were handled deliberately — the answering node is added explicitly because
`Members` holds *peers only*, and the authenticated-peers invariant (ADR-053)
now also stops an unauthenticated node being advertised to clients as somewhere
to send credentials. Anything new reading `Members` still inherits both.

**Task 7 answered the question that had been unanswerable**, and the numbers
are in [Benchmarks](benchmarks.md): TLS is close to free, the protocol costs
~0.1 ms per request, reads scale with clients (8,001/s → 70,660/s) and writes
do not (143/s → 602/s, p99 10 ms → 246 ms). It also found something no
engine-level benchmark could: a single write costs about **twice** as much
through the daemon as at the engine, which is neither protocol overhead nor
encoding, and is recorded as an open question rather than explained.

### What task 12 did, and what the mutation pass found

`shelf`, a small library catalogue, **written three times** — Rust, Python, Go.
Not three snippets: one application that stocks a shelf in a single commit,
groups it by decade, pages it five at a time, searches it by vector, and
watches it change while another thread writes to it. All three print the same
three nearest titles with the same scores to three decimals, which is a
stronger statement about the clients than any of them passing its own tests.
They run in CI against a real node, because an example nobody runs decays into
a document that used to be true.

The embedding is a **deliberate toy** — a normalized bag-of-words FNV-1a hash,
sixteen wide, identical in all three languages — and says so in every file. It
buys a real pipeline (configure vectors, store per document, search, score)
with no API key, no model and no network. Swapping in a real provider does not
change the application code above the search.

**The mutation pass, split by scope.** Running every client mutant against the
workspace suite was a nine-hour job; running the client's mutants against the
client's tests is twenty minutes. The client's first pass caught 101 of 167
viable mutants, and the misses were not marginal:

- **Seven convenience methods had no test at all** — `find`, `update`,
  `delete`, `aggregate`, `replace_document`, `delete_document`, `download`.
  Each could be stubbed out entirely and the suite stayed green, because the
  tests reached the server through `pages`, `insert`, `count` and `request`.
  One-line wrappers are exactly where a wrong verb or a wrong path hides.
- **Topology filtering was untested**, because a one-node harness has nothing
  to filter: every comparison in `refresh_topology` could be inverted with no
  observable effect. It has three nodes to choose between now.
- **Thirteen of the seventeen error codes were never produced by any test**,
  so any of them could have been renamed silently — on public surface that
  callers branch on.

Five new tests took it to **133 caught of 167 viable** (190 mutants: 34
missed, 20 unviable, 3 timeouts). What is left is classified rather than
absorbed, in [Testing](testing.md): reconnect-backoff internals that need
fault injection, and a handful of provably equivalent mutants. **The `retry:
wait` row was closed on 2026-08-14** — and closing it found the branch was not
merely untested but wrong; see the section below.

**The rule from M7 held again**: the value was not the score, it was that
mutation testing asked "which of these lines could I delete?" and the answer
included seven public methods.

**The server-side half of the pass was abandoned, and that is a debt.** The 90
mutants in `kimmy-api`, `kimmy-storage` and `kimmy-auth` are unclassified: the
run re-ran all three suites for every mutant at a parallelism that pushed each
past its timeout, and produced 3 caught and 15 timeouts out of 47 before it was
stopped. Those crates had full passes in M7 and M8 and their share of this diff
is small, so it is not alarming — but the new code in them is branch-heavy and
deserves it. One pass per crate with tests scoped to that crate closes it in
about an hour; it is in [Deviations](deviations.md) so it does not get lost.

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
without `?upsert=true` returns `200 {"matched": 0}` and writes nothing — a
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
| `kimmy-client/src/` | The Rust client: `lib.rs` (`Client`, `Builder`, `Safety`, `Method`, failover and token renewal), `error.rs` (the mirrored `ErrorCode`), `page.rs` (`Query`, `Pages`), `watch.rs` (`ChangeStream` and its reconnect) |
| `kimmy-api/src/` (M10) | `error.rs` (the 17-code taxonomy and three-valued `Retry`), `version.rs` (`GET /v1/version`, `Capability`), `topology.rs` (`GET /v1/topology`, the node registry in `__kimmy.__nodes`), `json.rs` (`JsonBody`, so typed bodies get the envelope) |

**And outside `crates/`, all of it new in M10:**

| | |
|---|---|
| `docs/openapi.yaml` | **The protocol specification, and the authority.** ~2,100 hand-written lines. If code and this disagree, one of them is a bug — decide which, don't paper over it |
| `crates/kimmy-api/tests/openapi.rs` | The contract test that holds it true: inventory in *both* directions, live response validation, a coverage assertion, and the error-code/retry cross-check |
| `clients/python/` | Package `kimmydb`, synchronous, `httpx` + `websockets`, built with `uv`/hatchling. 19 tests |
| `clients/go/` | Module `github.com/titusai-io/kimmydb/clients/go`, package `kimmydb`, one dependency (`coder/websocket`). 18 tests |
| `clients/conformance/` | `scenarios.json` (16 scenarios), a shared oracle, `run.py`, and one driver per client. **The only test that compares the clients to each other** |
| `examples/` | `shelf` — one application in three languages, plus `run-all.sh`. The Rust one lives at `crates/kimmy-client/examples/shelf.rs` because Cargo wants it there |

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
  **Scope the test command to the mutant's own crate** (`--file
  'crates/kimmy-client/src/**' -- -p kimmy-client`). Unscoped, every client
  mutant re-runs the storage and cluster suites: a nine-hour job instead of
  twenty minutes. Set `--timeout` from a *contended* baseline, not an idle one
  — M10 lost most of a day to a run at `-j 8` with a 300s cap that produced 15
  timeouts and 3 caught out of 47, where every "timeout" was `cargo test` still
  linking. And note the binary is `cargo-mutants`: **`pkill -f "cargo mutants"`
  does not match it**, so an abandoned run survives and competes with its
  replacement.
- **The Python client:** `cd clients/python && uv run --extra dev pytest`.
  Needs a `kimmyd` binary; `conftest.py` starts one. Timeout is 60s per test,
  because a change-stream bug once hung a test for ten minutes.
- **The Go client:** `cd clients/go && go test ./...`. Same arrangement.
- **The conformance suite:** `python3 clients/conformance/run.py` — starts a
  node, runs all three drivers over `scenarios.json`, compares each to the
  shared oracle *and* to the others. This is the test that catches a client
  quietly disagreeing with its siblings.
- **The examples:** `./examples/run-all.sh` — builds nothing, starts a fresh
  node, runs all three `shelf` programs against it in sequence. The second and
  third exercise the "already stocked" path. Requires `target/debug/kimmyd`.
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
- **Driving the protocol specifically.** `docs/openapi.yaml` is machine
  readable, so a drive can validate against it rather than eyeballing: task 1's
  script parsed the document with `yaml` and checked required fields and
  declared types on 32 real responses from a release node. Python has `yaml`
  here but **not** `jsonschema`, so a full validator has to be the Rust test —
  the shallow check is still worth having, because it is a second reader of the
  same document.
- **The suite is ~1,103 Rust tests** across the workspace, plus 8 ignored
  cluster tests, 19 Python and 18 Go client tests and 16 conformance scenarios.
  A full `cargo test --workspace` is about two minutes.

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
| **The M10 server-side mutation pass — `kimmy-api`'s 76 mutants are still unclassified** | narrowed 2026-08-14: `kimmy-auth` (2) and `kimmy-storage` (12) are done, and found `InvalidateReason::as_str` unpinned. Run `kimmy-api` on an **idle** machine — contention is what ruined it twice |
| ~~The client's `retry: wait` path has no test that reaches it~~ | **Closed 2026-08-14.** The test found the branch was *wrong*: `wait` failed over instead of waiting, so it behaved as `elsewhere` with a delay. Fixed, 9/9 mutants caught |
| A single write costs ~2× as much through the daemon as at the engine — not protocol overhead, not encoding | found by M10 task 7; **recorded as an open question rather than explained** |
| Sharding | **deferred by decision** until there is operational experience |

Three rows left this table in M10 and are noted here so nobody re-opens them:
token refresh (task 4), cluster discovery for clients (task 5), and
client-facing throughput, which is now measured and in [Benchmarks](benchmarks.md).

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

**From M10 — the protocol as a contract:**

- **`docs/openapi.yaml` is the protocol's authority, and changing the wire
  means changing it in the same commit.** `crates/kimmy-api/tests/openapi.rs`
  fails when the router and the document disagree about which operations
  exist, when a real response does not match its declared schema, **and when a
  documented operation is not driven by the test at all**. That last one is
  what stops the document becoming prose again; it is also what every new route
  now costs (ADR-056).
- **One concept, one type, across every route that reports a write.**
  `matched` and `modified` are counts everywhere, including on `PUT
  /docs/{id}`, which touches at most one document. `upserted` is a boolean
  because it is one. This was not true until the specification's test drove
  both routes and compared them.
- **A route inventory built by scanning source must handle multi-line
  registrations.** The check that missed them was green for two milestones
  while never looking at `/docs/{id}`. If a scan can silently match nothing,
  assert that it matched something.
- **Every refusal carries the envelope, and the mapping lives in the extractor
  rather than in each handler.** `json::JsonBody<T>` is the only way a typed
  body enters, and `/watch` maps the upgrade rejection the same way. A mapping a
  handler has to remember to reach is a mapping eighteen of nineteen handlers
  did not reach.
- **The error code set is closed by the compiler, and the retry class is part
  of adding a code.** `ErrorCode` is an enum with exhaustive matches for the
  wire string and the class; the class goes on the wire so a client handles a
  code released after it was written (ADR-057).
- **`elsewhere` is only meaningful because every node accepts writes.** Anything
  that reclassifies `internal`, `misconfigured` or `snapshot` is claiming a
  peer cannot answer, which for a full-copy cluster needs a reason.
- **A specification is a wire format, so its own encoding has to be checked.**
  `no` is boolean `false` in YAML 1.1. Quote scalars that could be read as
  something else, and keep the live drive an *independently written* reader —
  a second run of the same parser proves nothing about the document.


## Reading order for someone new

1. **"Getting oriented"** above, then how work runs and the
   oplog-as-spine idea. Nothing else makes sense without them.
2. **[Roadmap](roadmap.md)** — every milestone through M10 is closed, so read
   it for what was built and what was ruled out, not for a task to pick up.
   There is no queued work; the next milestone is the maintainer's call.
3. **[Deviations](deviations.md)** — the register, not a summary of it. Every
   decision M9 and M10 made is a 🟢 entry there with its reasoning, and the
   two debts M10 created are 🟡 entries.
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
