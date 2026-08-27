# Decisions

[← Documentation index](README.md)

An architecture decision record. Each entry captures what was chosen, what was
rejected, and — most importantly — **why**, so the reasoning survives even when
the person who made the call does not remember it.

---

## ADR-001 — redb as the storage engine

**Decision.** [redb](https://github.com/cberner/redb) 4.x.

**Alternatives.** RocksDB, a custom LSM/B-tree, `fjall`, `sled`.

**Why.** Pure Rust, so no C++ toolchain in the build or the container. ACID with
MVCC snapshots, giving many concurrent readers against one writer for free.
Typed table definitions with `memcmp` key ordering, which is exactly the
primitive an order-preserving key encoding wants. RocksDB would have been the
safe battle-tested pick but brings a C++ dependency and substantial tuning
surface; writing a storage engine would have delayed a usable database by
months; `sled` is effectively unmaintained.

**Cost.** One handle per database file, per process — discovered when a test
tried to open a second `Engine` for a writer thread. Share an `Arc<Engine>`.
Also less operational literature than RocksDB when something goes wrong.

---

## ADR-002 — BSON on disk, Extended JSON at the edge

**Decision.** Store BSON. Convert at the HTTP boundary using Extended JSON v2.

**Why.** Mongo-style comparison needs typed values and a defined cross-type
order. JSON has one number type and no dates, binary, or object ids. Storing
JSON would mean either losing types or inventing a parallel type system on top
of it — and then the query semantics people expect would not work.

Extended JSON keeps plain JSON working for everything expressible in it, so a
caller who does not care never meets the distinction.

**Consequence.** Whole numbers stay integers rather than widening to double.
Widening would break `$type` queries and lose precision above 2^53; `2^53 + 1`
round-trips exactly and there is a test pinning it.

---

## ADR-003 — Hand-rolled binary codec for hot records

**Decision.** Explicit byte layouts for `DocRecord` and `OplogEntry`, each with
a leading format version. JSON for collection metadata.

**Why.** A derive-based codec ties the on-disk layout to a dependency's internal
versioning — bincode 1.x and 2.x are mutually incompatible, and a database
cannot have its file format change because a dependency did. The oplog format is
*also* the replication wire format for M4, which doubles the reason to specify it
explicitly.

Metadata went the other way: it is read on open and on DDL, never in a hot path,
and reading it directly while diagnosing a broken data directory is worth more
than the bytes.

**Cost.** More code, and encode/decode symmetry has to be maintained by hand —
so every record type has a round-trip test, and truncation is tested at every
offset.

---

## ADR-004 — Two independent ordering implementations

**Decision.** `cmp::canonical_cmp` (semantics) and `keyenc::encode` (bytes) are
written independently and cross-checked by property tests.

**Why.** The invariant `encode(a).cmp(encode(b)) == canonical_cmp(a, b)` is what
makes indexes correct, and breaking it does not crash — it silently returns
wrong query results. Having the encoder delegate to the comparator would be less
code and strictly worse: a single implementation cannot catch its own
bit-manipulation errors. An oracle is the entire point.

**Follow-up.** The two are *not* independent for numbers, because `keyenc`
encodes through `cmp::decompose`. A bug there would corrupt both identically.
So numeric ordering is additionally checked against a third implementation
sharing no code with either. Both paths were mutation-tested to confirm the
tests actually detect injected faults.

---

## ADR-005 — Exact mantissa/exponent numeric encoding

**Decision.** Encode every number as `±mantissa × 2^(exp − 63)` rather than
converting to `f64`.

**Why.** Encoding through a double collapses `2^53` and `2^53 + 1` into the same
bytes, so two distinct `i64` values would share one index entry. Anyone using
large integer ids gets silently wrong results.

**Cost.** More complex encoder; `Decimal128` cannot be represented exactly and
is **refused** as an index key or `_id` rather than encoded approximately.
Refusing is honest; an approximate index key is a wrong answer waiting to
happen.

---

## ADR-006 — HLC with node-id tiebreak, whole-document LWW

**Decision.** Hybrid logical clocks; conflicts resolve by
`(wall_ms, counter, node_id)`, at whole-document granularity.

**Alternatives.** Per-field LWW, JSON CRDTs (Automerge-style), vector clocks.

**Why.** LWW is simple, predictable, and has bounded metadata — 26 bytes per
record. The node id makes ties deterministic, so every replica picks the same
winner regardless of arrival order. Per-field LWW preserves more but multiplies
metadata and complicates delete semantics; CRDTs preserve the most but are a
large subsystem with real storage overhead and are hard to make fast.

**Cost.** Concurrent edits to *different fields* of the same document lose one
of them. This is a real limitation, not a rough edge — documented prominently in
[Time & Conflicts](time-and-conflicts.md). `merge_policy` on `CollectionMeta` is
the intended extension point if the semantics need to change.

---

## ADR-007 — Physical time as a parameter

**Decision.** `kimmy-core` never reads the clock. `HlcClock::tick(physical_ms)`
takes time as an argument.

**Why.** Clock skew, backwards NTP jumps, stalls, and counter exhaustion are
exactly what an HLC exists to survive — and are nearly impossible to test
against a real clock. As parameters they become ordinary unit tests. This is not
purism; it is what made "NTP yanks the clock back a second" a two-line test.

---

## ADR-008 — The oplog is written unconditionally

**Decision.** Every mutation appends an oplog entry, in the same transaction,
whether or not the node is clustered.

**Why.** This is the central bet. It gives change streams on a single node — the
motivating requirement — because the log exists whether or not a peer ever
existed. It also made the embedding pipeline a plain change-stream
subscriber rather than a scheduler, and gives M4 replication a ready-made
source.

**Cost.** Write amplification: every write stores the document twice, since
entries carry full post-images. Bounded by retention (📋 not yet implemented).
Full images were chosen over diffs because they make replication idempotent and
order-independent.

---

## ADR-009 — Subscribe before replaying

**Decision.** A change stream subscribes to the live broadcast **first**, then
replays the oplog, then deduplicates the overlap.

**Why.** The reverse order leaves a window between the read and the subscription
in which committed events reach nobody. That gap is silent, intermittent, and
load-dependent — it only manifests when a write lands in exactly the wrong
microsecond, which is to say: in production, not in testing.

It gets a dedicated test that writes concurrently across a simulated disconnect
and asserts the delivered sequence is exactly complete.

---

## ADR-010 — Lag recovers from the oplog

**Decision.** A subscriber that falls behind the in-memory buffer rewinds and
replays from disk rather than being invalidated.

**Why.** The plan originally called for `invalidate`. Writing the test made it
obvious that the same events are on disk, so invalidation throws away a
capability the design already has. Recovery is bounded by a resume floor so it
cannot resurrect history the client deliberately skipped.

**Consequence.** `ChangeEvent::Invalidate` is currently unreachable, and is
documented as such rather than left looking live. It becomes reachable when
oplog collection can remove a stream's replay range underneath it.

---

## ADR-011 — HTTP/JSON + WebSocket, not the MongoDB wire protocol

**Decision.** REST-ish HTTP with WebSocket change streams.

**Alternatives.** MongoDB wire protocol (existing drivers would just work),
gRPC, a custom binary protocol.

**Why.** Wire-protocol compatibility is a large surface — opcodes, cursors,
SCRAM, the `hello` handshake — and it would lock the data model to Mongo's
semantics permanently. HTTP works from curl and every language without a driver,
and the MCP layer (M3) is HTTP anyway.

**Cost.** No existing Mongo tooling — Compass, `mongosh`, existing drivers — and
higher per-request overhead than a binary protocol.

---

## ADR-012 — JWT with embedded grants

**Decision.** HS256 tokens carrying grants, signed with a cluster-wide secret.

**Why.** In a leaderless cluster a request may land on any node. A per-node key
would produce intermittent 401s that only appear under load balancing.
Embedding grants keeps verification a pure function of the token — no store
lookup on the hot path, no cross-node consistency requirement for authorization.

**Cost.** **No revocation.** Deleting a user or narrowing grants only takes
effect when the token expires — hence the one-hour default. Cutting off access
immediately requires rotating the secret, which invalidates every token. This is
stated plainly in [Security](security.md) rather than left to be discovered.

---

## ADR-013 — One authorization decision point

**Decision.** `Principal::can()` is the only place that answers "may this
principal do this?", and authorization is an axum *extractor*, not middleware.

**Why.** A second enforcement path is exactly how an MCP tool (M3) ends up
quietly more permissive than the REST route beside it. As an extractor, a route
that needs a principal takes one and a route that does not is visibly public —
with middleware, "which routes are protected?" is a question you answer by
reading a registration list.

**Related.** Authorization runs *before* the collection is resolved, so a denied
request cannot distinguish "forbidden" from "does not exist". A 404 there would
let a caller probe for collections they cannot access.

---

## ADR-014 — `write` implies `read`; `read` does not imply `watch`

**Decision.** Action implication is `admin ⊃ {write, watch, read, search}`,
`write ⊃ read`, `read ⊃ search`. `watch` stands alone.

**Why.** An update must read the document it modifies, so requiring both
separately would make every writer role wrong by default. Vector search is a
read. But a change-stream subscriber sees every change to a collection
*continuously*, which is a materially different exposure from point reads — so
it must be granted deliberately.

---

## ADR-015 — Callback-based collection scans

**Decision.** `for_each_doc(coll, |id, doc| -> Result<bool>)` rather than
returning an iterator or a `Vec`.

**Why.** redb's range borrows its transaction. Returning an iterator would leak
that lifetime into every caller; returning a `Vec` would materialize whole
collections in memory. The callback returns `false` to stop early, which keeps
paging cheap for unsorted queries.

---

## ADR-016 — Pure-Rust cryptography

**Decision.** `jsonwebtoken` with the `rust_crypto` feature; `argon2` pinned to
stable 0.5 rather than the 0.6 release candidate.

**Why.** `aws_lc_rs` would reintroduce a C toolchain dependency, undoing the
reason redb was chosen over RocksDB. And password hashing is not the place to
run ahead of a stable release.

> **Corrected 2026-08-08.** The stated goal — a build needing no C toolchain —
> **has not held since M2**, and this ADR claimed otherwise for two milestones.
> `kimmy-vector` depends on `reqwest` with `rustls-tls`, non-optionally, for the
> remote embedding providers, which pulls `rustls → ring`. `ring` ships C and
> assembly and builds with `cc`. Found while planning TLS, by checking
> `cargo tree -i ring` rather than trusting the register.
>
> The choices above are still the right ones and still stand — picking a pure
> option where one exists costs nothing. What is no longer true is the *claim*
> that the whole build is free of a native toolchain. The maintainer chose to
> accept the cost and correct the record rather than gate `reqwest` behind a
> feature.
>
> The practical rule going forward: **do not add a second native crypto stack.**
> `ring` is paid for; `aws-lc-rs` would add CMake on top of it for the same
> primitives. That is what decides the provider in [ADR-039](#adr-039--tls-terminates-natively-on-the-provider-already-in-the-build).

---

## ADR-017 — Reserved `__` prefix, with an internal escape hatch

**Decision.** User-facing name validation rejects the `__` prefix. System
objects are created through `create_system_collection`, which skips that check.

**Why.** The user store lives in an ordinary collection
(`__kimmy.__users`), so it gets the same durability, oplog, and eventual
replication as any other data — rather than a parallel storage mechanism that
would need its own correctness argument. Reserving the prefix is what stops a
user from shadowing it.

---

## ADR-018 — Monotonic id counters, never reused

**Decision.** Collection and index ids come from persistent counters, allocated
in the same transaction as the object they identify.

**Why.** Deriving the next index id from `max(existing) + 1` reuses a dropped
index's id. A dropped index's entries are removed lazily, so a new index
inheriting the id would also inherit its stale entries — and return wrong
results. A test caught the contradiction between the implementation and its own
doc comment.

---

## ADR-019 — `find` is paged by default

**Decision.** 100 documents by default, 10,000 maximum.

**Why.** An unbounded `find` on a large collection would pull it entirely into
memory. A default cap makes the failure mode "you got fewer results than you
expected" rather than "the server fell over".

**Cost.** `skip` is O(n) and deep paging is expensive — and stays so until
indexes land.

---

## ADR-020 — Uniqueness reaches only as far as coordination does

**Decision.** Unique indexes carry an explicit `enforcement` mode. `local` (the
default) enforces on the accepting node and **detects** cross-node violations
after merge. `coordinated` — real cluster-wide enforcement — is reserved and
refused until M4.

**Why this is not an implementation gap.** Uniqueness is a *global* invariant:
deciding whether a write is legal requires knowing what every other node is
concurrently doing. Bailis et al. (*Coordination Avoidance in Database
Systems*, VLDB 2014) formalize this as **I-confluence** — an invariant is
maintainable without coordination iff merging any two valid states yields a
valid state. Uniqueness fails: node A holding `email=a@x.com` and node B
holding `email=a@x.com` on a *different* document are each valid alone and
invalid merged.

So no merge function fixes this. During a partition each side must either
accept the write (breaking uniqueness) or refuse it (breaking availability).
There is no third option, and a leaderless design has already chosen
availability.

**`_id` is exempt, and that covers most of the demand.** Two nodes inserting
the same `_id` collide on the same key, and last-writer-wins converges them to
a single document — primary-key uniqueness holds by construction. The residue
is that the losing insert's *content* is discarded silently, where a client
might have expected a `409`. That is a lost-update problem, not a uniqueness
one, but it shares the silent-failure smell and is documented as such.

**Alternatives considered.**

| | Guarantee | Cost |
|---|---|---|
| Drop unique indexes | — | Loses a genuinely useful single-node feature |
| Detect after merge *(chosen default)* | None enforced; violations reported | Almost nothing |
| Coordinate per value *(reserved)* | Real | Those writes become CP; needs value-ownership routing |
| Consensus per write | Linearizable | A Raft/Paxos subsystem, plus latency on every constrained write |

**Note on "leaderless".** Coordinated enforcement would route a value to the
node owning `hash(value)`. That is per-key coordination, **not** a cluster
leader — no elections, no primary, every node owns some slice. It stays
compatible with the project's leaderless goal; what it costs is availability
for that one value while its owner is unreachable.

**Consequence.** The default is honest rather than convenient: a `local` unique
index does less in a cluster than its name suggests, and says so. Making that
a visible, per-index, opt-in decision is preferable to either silently weakening
the guarantee or refusing the feature outright.

---

## ADR-021 — Local embeddings are opt-in, not the default

**Decision.** The default provider is `byo` (client-supplied vectors). In-process
ONNX inference lives behind a `local-embeddings` cargo feature, off by default,
and is *rejected at configuration time* in a build that lacks it.

**Why.** `fastembed` pulls native ONNX Runtime **and**, by default, OpenSSL, and
roughly triples the image. A zero-config default that quietly costs
cross-compilation and hundreds of megabytes is not zero-cost.

> **Corrected 2026-08-08.** This originally argued the feature would "undo the
> pure-Rust property" behind ADR-001 and ADR-016. That property was already
> gone — `reqwest` has pulled `ring` into every build since M2, as recorded in
> the correction on [ADR-016](#adr-016--pure-rust-cryptography). The decision
> stands on the remaining reason, which is the stronger one anyway: ONNX Runtime
> is hundreds of megabytes of binaries and a separate runtime, a different order
> of cost from a crate that builds some C with `cc`.

**Why rejection happens at configuration time.** A `local` provider in a build
without the feature fails identically forever. Failing when the configuration is
written surfaces it to the person who can fix it; failing on the first document
write surfaces it to traffic.

**Consequence.** Out of the box, embedding needs a remote provider or
client-supplied vectors. `--features local-embeddings` restores the planned
behaviour. Recorded as a deliberate departure in [Deviations](deviations.md).

---

## ADR-022 — The vector index is a candidate source, not a source of truth

**Decision.** The HNSW graph supplies *candidates only*. Every candidate is
re-scored from the vector currently in storage, and a candidate whose record no
longer exists is skipped. The graph is never consulted for a score.

**Why this matters more than it looks.** It is the single property that makes a
*cached, deliberately stale* index safe. A stale secondary index returns wrong
documents, because it is authoritative for what matched. A stale vector index
cannot: a deleted document is skipped, and an updated one scores by its new
vector. The only residue is that a very recently added document may not be found
yet — bounded recall loss on new data, never incorrect data.

**What it buys.** The index can be rebuilt on an interval rather than maintained
transactionally on the write path. Maintaining an HNSW graph inside the write
transaction would put O(log n) graph mutation — and a lock — in front of every
insert, for a structure that is only ever an optimisation.

**Consequence.** A rebuild interval (30 s) and a size threshold (2000 vectors)
are tunable policy, not correctness parameters. Getting them wrong makes search
slower or less fresh; it cannot make search wrong. `IndexCache` owns both, in
one place, rather than letting the decision spread through the search path.

---

## ADR-023 — No approximate index for the `dot` metric

**Decision.** Dot-product collections always take the exact scan.

**Why.** `anndists::DistDot` — the distance implementation behind `hnsw_rs` —
computes `1 - dot` and asserts the result is non-negative. That holds only for
unit-length vectors. A real embedding would trip the assertion and **abort the
process**, turning a search into a server crash.

**Why not normalize on the way in.** It would work, and it would silently change
what the collection means: a dot-product search over normalized vectors *is*
cosine similarity. Redefining a user's chosen metric to make an optimisation
available is the wrong trade.

**Consequence.** `dot` collections are O(n) per query, with no recall loss. The
limitation is in the type system's reach — `HnswIndex::supports(metric)` — not a
runtime surprise.

**Note.** This was found by testing the metric rather than trusting the crate. It
is the strongest argument in the project so far for the measure-don't-assume rule
in [Testing](testing.md).

---

## ADR-024 — MCP shares the API's execution path, not just its process

**Decision.** `kimmy-mcp` depends on `kimmy-api`. Both edges call one module,
`kimmy_api::exec`, which performs the authorization check *inside* each
operation rather than beside it. `kimmyd` merges the two routers onto one
listener.

**What was planned.** The crate graph had the arrow the other way — `kimmy-api`
depending on `kimmy-mcp` — from a placeholder written at M0 before either
existed.

**Why it was inverted.** M3's stated constraint is that "there must not be a
second, weaker enforcement point." In-process co-location does not achieve that
on its own; it only makes it *possible*. Two crates sharing an `Engine` can
still drift, because each writes its own `auth.require(...)` before touching it,
and a tool added later can simply omit one. Sharing the executor makes the
check unskippable: there is no path to the engine that does not pass through a
function that already performed it.

Inverting the dependency was the cheap way to get that. The alternative —
`kimmy-api` depending on `kimmy-mcp` — would have required duplicating Extended
JSON conversion, the index-planning query executor, and the vector search
dispatch into the MCP crate, which is three opportunities for exactly the drift
the milestone exists to prevent.

**Cost.** `kimmy-mcp` now pulls in axum and `ApiError`. The layering diagram
gains an edge between two crates that are conceptually peers. Worth it: the
alternative was three copies of logic whose divergence would be silent.

**Consequence.** The REST routes became thin adapters in the same change. That
was not the goal, but it is the check that the extraction was real — if the
executor were shaped around MCP, the HTTP handlers would not have collapsed into
one-liners over it.

---

## ADR-025 — Tools are always advertised; the role decides what runs

**Decision.** Every tool appears in `tools/list` for every authenticated
caller. A read-only token sees `insert`, `delete`, and `create_index`, and gets
an authorization error if it calls one.

**Alternative rejected.** Filtering the tool list by the caller's grants.

**Why.** Hiding is not a boundary — the enforcement is the `Principal::can`
check, which runs either way — so filtering would buy no safety. It would cost
two things. An agent that cannot see a tool cannot be told *why* it was refused,
so "you lack write access to this collection" degrades into "no such tool",
which is not actionable. And a filtered list makes the surface depend on the
token, so two agents against the same server would disagree about what the
server is.

**Consequence.** The server's `instructions` say this outright, so a model reads
the refusal as a permission fact about itself rather than a malfunction.

---

## ADR-026 — No `Host` allow-list on `/mcp` by default

**Decision.** `server.mcp_allowed_hosts` defaults to empty, which disables the
`Host` header check that `rmcp` enables by default.

**Why.** The check is DNS-rebinding protection, designed for an MCP server
running on a developer's laptop with no authentication — where a malicious web
page can make the victim's browser issue requests the server will honour.
Neither half applies here. KimmyDB binds to a network address by design, and
`/mcp` requires a bearer token that is verified by axum middleware *before*
`rmcp` sees the request. A rebinding attack cannot forge that token.

Keeping `rmcp`'s default would have rejected every client that reached the
server by its real hostname — which is the normal deployment — and the failure
mode is an opaque refusal that looks like a bug rather than a policy.

**Cost.** One layer of defence in depth is off by default. It is a layer that
protects against an attack the bearer token already stops, and operators who
want it can list their hostnames.

**Consequence.** The bearer-token check must stay ahead of the MCP transport. If
`/mcp` were ever mounted without its middleware, this decision would become
wrong — which is why `principal()` fails closed and logs rather than defaulting
to an anonymous caller.

---

## ADR-027 — MCP resources exclude KimmyDB's own internals

**Decision.** `resources/list` omits the `__kimmy` system database and any
`__`-prefixed or `.__vectors` collection. The `find` tool does not: a superuser
can read them, exactly as through the REST API.

**Why the two differ.** They are different acts. A tool call is a caller asking
a specific question. A resource is *material an agent attaches to its context* —
and the highest-value thing in `__kimmy.__users` is a column of Argon2id
password hashes. Offering those for attachment is wrong regardless of whether
the caller is authorized to read them, because the authorization answers "may
this principal see it", not "should this be pasted into a language model".

Shadow collections are excluded for a duller reason: they hold float arrays that
would consume an enormous amount of context and describe nothing the source
collection does not.

**Not a security control.** It is a default. The access decision remains
`Principal::can`, in one place, as everywhere else.

**Found by driving the server.** The first `resources/list` against a real node
returned `kimmy://__kimmy/__users`. No test would have caught it, because no
test would have thought to look.

---

## ADR-028 — The newest oplog entry is never collected

**Decision.** Retention collects oplog entries older than its window, with one
exception that overrides age entirely: the single newest entry always survives.

**Why.** The logical clock is not persisted separately. `Engine::open` resumes
it by reading the oplog tail — that is the *only* record of how far the clock
has advanced. An empty oplog resumes at `Hlc::ZERO`.

So a retention rule that collects purely by age is a data-loss bug on a delay.
Collect the last entry, restart, and the node begins minting stamps below ones
already on disk. Every subsequent write to an existing document loses to its own
older version under last-writer-wins — and loses *silently*: the write returns
200, the oplog entry is appended, and the document does not change.

**Where it would have bitten.** An idle node. No writes means every entry
eventually ages past the window, so the naive rule empties the log precisely
when there is no activity to make the damage visible. A busy node always has a
fresh entry and would never have shown the bug in testing.

**Alternative considered.** Persist the clock high-water mark in the `meta`
table on every write. Correct, and it would decouple the clock from retention
entirely — but it puts an extra write in every transaction to remove a
one-line special case. Worth revisiting if something else ever needs the clock
independent of the log; not worth it for this.

**Consequence.** An oplog can never be fully empty once anything has been
written, so "collect everything" is not an expressible state. Three tests pin
it, including one that restarts the engine after collecting with zero retention
and asserts the next write stamps above the retained tail. Removing the
exception fails all three.

---

## ADR-029 — A violation is an oplog entry, because that is what a change stream is

**Decision.** `OpKind::UniqueViolation` — a real oplog entry, locally stamped,
carrying the index name and every colliding id. Not a document change; it
describes something that happened *to* the data.

**Why it had to be an entry.** [Roadmap](roadmap.md) committed M4 to "a
`uniqueViolation` change-stream event". When that was written, streams read from
the in-memory broadcast, so an in-memory event would have worked. It no longer
would: streams now read from the oplog and discard the broadcast payload
([ADR-030](#adr-030--the-broadcast-channel-is-a-wake-up-not-a-data-path)). An
event outside the log cannot be delivered at all.

That turned out to be the better outcome anyway. As an entry it is durable,
ordered, and resumable, and it reaches subscribers through machinery that
already exists — a violation nobody happened to be connected to witness is
barely better than a silent one, and this one survives being missed.

**Alternative considered: a `{coll}.__conflicts` collection.** An ordinary
document write, so it would also produce a stream event, and it would outlive
`oplog_retention_secs` because it would be data. Rejected for now as more
surface area than the commitment requires — a second shadow collection per
collection, with its own retention story — but it remains the right answer if
violations ever need to be reconcilable weeks later.

**Locally stamped, and must not replicate.** Every node detects the same
collision independently when it merges, so the entry is *this* node's
observation. Shipping it to peers would report one violation once per node. M4's
anti-entropy has to exclude the kind explicitly; this is recorded in the roadmap
rather than left to be rediscovered.

**Written in a separate transaction from the merge.** Deliberate: reporting must
not be able to fail the write. A converged write with an unreported violation is
bad, but a *rejected* replicated write is worse — the nodes then never agree,
which is the availability the whole design is protecting.

---

## ADR-030 — The broadcast channel is a wake-up, not a data path

**Decision.** Change streams read every entry from the oplog's arrival index.
The broadcast channel's payload is discarded; only the fact that a message
arrived is used, as a signal that there may be more on disk.

**What forced it.** The old stream de-duplicated the replay/live overlap by
dropping anything stamped at or below its high-water mark. A replicated entry
carries an older stamp by definition, so that check discarded exactly the
entries the arrival index exists to deliver.

**What it also fixed.** Publication happens *after* the commit that assigns an
arrival position, so two concurrent writers can publish in the opposite order
from the one they committed in. A stream trusting publication order would have
delivered those reversed — and the stamp comparison would then have dropped the
second one permanently. That bug was latent, unrelated to replication, and would
have been load-dependent and intermittent.

**What it bought.** Falling behind the channel buffer stopped being an error
condition. The data is on disk either way, so a `Lagged` receiver is a late
wake-up and nothing more. `InvalidateReason::ConsumerLagged` now has exactly one
cause: retention collected the range the stream was about to read.

**Cost.** A read per wake-up rather than delivery straight from memory. Under
load the reads batch, so the cost falls as throughput rises — the opposite of
the shape that would matter.

---

## ADR-031 — Collection ids are derived from the name, not allocated

**Decision.** `CollectionId = FNV-1a-64(db || 0x00 || name)`. Every node computes
the same id for the same collection, with no coordination.

**The bug this fixes.** Ids came from a node-local counter, and every oplog
entry names its collection by id. Two nodes creating the same collections in a
different order therefore disagreed about what an entry referred to:

```
Node A: orders, then customers   →  shop.orders = 1
Node B: customers, then orders   →  shop.orders = 2
```

A replicated write for `shop.orders` from A would have been applied to whatever
collection held id 1 on B. Verified empirically before designing around it, not
inferred from reading.

The failure mode is what makes it serious: it *works* whenever both nodes happen
to create collections in the same order, which is exactly what a two-node smoke
test does. It would have passed early testing and corrupted data later.

**Why derivation rather than agreement.** The alternatives both required
coordination or rewriting. Replicating metadata and letting a first writer win
means the loser must renumber — rewriting every document, index entry and vector
for that collection, racily. Carrying `(db, name)` in every oplog entry avoids
ids entirely but pays two strings per entry forever. Derivation costs one
migration and then nothing, and it works on a node that has never met a peer,
which is the leaderless property the whole design rests on.

**Why FNV-1a specifically.** The hash is baked into on-disk keys, so it must be
stable *forever*. `DefaultHasher` is explicitly not guaranteed stable across
Rust releases — using it would mean a compiler upgrade could silently repoint
every collection. FNV-1a is fully specified, dependency-free, and a few lines.
The pinned values are cross-checked in tests against an independent
implementation, so the test would also catch this implementation being wrong in
a self-consistent way.

**Collisions are checked, not assumed.** 64 bits over a realistic number of
collections makes one vanishingly unlikely, but the consequence — two unrelated
collections sharing storage — is unrecoverable, so creation refuses on collision
and the migration refuses to merge.

**The consequence worth knowing.** Dropping and recreating a collection now
reuses its id. That is not a choice; "same name means same id everywhere" and
"recreating yields a fresh id" are contradictory. It makes purging on drop
load-bearing rather than tidy — a surviving document or index entry would be
inherited by the new collection. `drop_collection` already purges both, and a
test now pins that rather than pinning the old id-uniqueness property it
replaced.

**Migration, not refusal.** Schema 1 databases are renumbered on open:
documents, index entries, and the `collection` field of every oplog entry. The
arrival index needs no migration because it maps sequence to stamp. Refusing
would have been easier, but a database that *can* be migrated should be — a user
with data has no other route forward. A *newer* schema is still refused, because
guessing at an unknown layout is how you corrupt it.

**The bill for a hash-shaped id, paid 2026-08-08.** Deriving the id made it a
hash, and a hash uses the whole `u64` range — while **BSON has no unsigned
64-bit type**. `CollectionId` derived `Serialize`, so any id above `i64::MAX`
failed to encode with `Unsigned integer N cannot fit into BSON`, and every
oplog entry naming that collection was unsendable. The collection and its
documents simply never replicated.

Roughly **48% of collection names** are affected — it is a coin flip per name.
The suite stayed green because the replication tests use `"shop"."orders"`,
which derives `0x53ad…`, in the low half. Found by running three containers and
noticing that `c.t` would not converge while `repl.items` had.

Fixed by giving `CollectionId` one fixed representation — a bit-cast to `i64` —
exactly as [`NodeId`](#adr-006--hlc-with-node-id-tiebreak-whole-document-lww)
needed when the same class of bug broke the *first* two-node sync. Ids below
`i64::MAX` encode identically to before, so the fix widens what works without
changing what worked, and the on-disk form is untouched because ids are
persisted by the hand-rolled codec as raw bytes rather than through serde.

The lesson is not about BSON. It is that **a derived id is a value with a
range**, and the moment a type crosses a format boundary its representation has
to be chosen rather than inherited. Two bugs of this exact shape have now been
paid for.

---

## ADR-032 — Index ids are derived too

**Decision.** `IndexMeta.id = FNV-1a-32(name)`, the same treatment
[ADR-031](#adr-031--collection-ids-are-derived-from-the-name-not-allocated)
gave collections, one level down.

**Why it follows.** An index definition has to replicate — a unique index is a
*constraint*, and a constraint that exists on one node and not another is not a
constraint. But index-entry keys embed the index id, and that id came from a
per-collection counter. Node A's index 1 and node B's index 1 would key the same
storage while describing different indexes, so replicating the definition would
have corrupted the entries.

Found while designing DDL replication rather than by a failure: the
`CreateIndex` payload carries an `IndexMeta`, and an `IndexMeta` whose id is
node-local is not a thing you can send anywhere.

**32 bits, not 64.** That is the width index-entry keys already use, and the
population is far smaller — collisions are between indexes *on one collection*,
where a handful is typical rather than thousands. Checked at creation and at
migration regardless, since two indexes sharing entries is unrecoverable.

**Same consequence as collections.** Recreating an index under the same name
reuses its id, so purging on drop is load-bearing. `drop_index` already removed
its entries in the same transaction as the metadata change; the comment claiming
ids were "not returned to the pool" because entries were "removed lazily" was
describing a hazard that the code did not actually have.

---

## ADR-033 — Schema changes replicate as operations, not as a snapshot

**Decision.** Five oplog entry kinds — `CreateCollection`, `DropCollection`,
`CreateIndex`, `DropIndex`, `ConfigureVectors` — each carrying its own payload,
each applied independently and idempotently.

**What was broken.** Five of the six DDL operations wrote **no oplog entry at
all**, and the two that did (`create_collection`, `drop_collection`) carried no
payload — byte-identical to each other, naming nothing. A peer receiving one
learned only that *something* happened to a collection id it might not
recognise. So no schema change could replicate, and documents could only flow
between collections that already existed on both sides.

**Alternative rejected: one metadata snapshot, merged last-writer-wins.**
Simpler, and it loses index additions. Two nodes each adding a *different* index
during a partition produce two whole-`CollectionMeta` values; one wins entirely
and the other's index silently vanishes. Separate operations merge
independently, so both survive — which a test pins.

**Every payload names its target by db and collection *name*.** Ids are derived
from names by a hash ([ADR-031](#adr-031--collection-ids-are-derived-from-the-name-not-allocated)),
and a hash cannot be inverted, so a node meeting a collection for the first time
could not otherwise learn what to call it.

**Idempotency is "is the world already like this?", not "have I seen this
entry?"** Peers resend overlapping ranges by design. Asking about the world
needs no per-entry bookkeeping and stays correct after an index rebuild.

**The bug this surfaced.** Applying a replicated DDL originally ran the ordinary
local operation, which logged an entry of its own under *this* node's stamp. The
peer then pulled that back, applied it, and minted another — the same change
traded forever, the oplog growing on every round. Fixed by giving each DDL
operation a non-logging path used only when applying a replicated change; the
*originating* entry is appended instead, so the change propagates onward with
its identity intact and the version vector advances for the right node.

Two tests failed on this before it was understood, and it now has one of its own
that fails under a deliberately reintroduced double-log.

**Not replicated: discarding stored vectors.** `disable_vectors` takes a
`drop_vectors` flag; the flag is a local reclamation choice and stays local. The
shadow collection is ordinary data and reconciles through the same anti-entropy
as everything else.

**Resolved separately.** Dropped collections now leave a tombstone —
[ADR-034](#adr-034--dropping-a-collection-leaves-a-tombstone).

---

## ADR-034 — Dropping a collection leaves a tombstone

**Decision.** `collections_dropped` records the stamp of every drop, keyed by
collection id, collected under `tombstone_retention_secs` alongside document
tombstones.

**The asymmetry it fixes.** Deleting a document was protected twice: the oplog
entry replicates the delete, and a tombstone stays in `docs` under
`tombstone_retention_secs`, so a rejoining peer re-sending its old insert loses
on stamp comparison even after the oplog entry is collected.

Dropping a collection was protected once. `drop_collection` removed the
`collections` row outright, leaving nothing, so the `DropCollection` oplog entry
was the only record — bounded by `oplog_retention_secs`. Once it aged out, a
peer partitioned across that window rejoined still holding the collection, and
anti-entropy recreated it along with every document in it. Nothing compared
stamps, because nothing was left to compare against.

Two settings governed two scopes, and the one that governed *collections* was
not the one anyone would guess.

**Keyed by id, not name.** A replicated document entry names its collection by
id, and a node that has dropped the collection can no longer resolve that id to
a name — so a name-keyed tombstone could not be consulted on the path that
needs it most.

**Two checks, not one.** A creation older than the drop is ignored, and a
document entry older than the drop is discarded. The second matters because
recreating the collection is not the only way back: replaying its documents into
a node that has since recreated it under the same name would refill it with
data the drop removed.

**Recreation still works.** The tombstone stores a stamp, not a prohibition, so
a creation stamped *after* the drop wins and the name stays usable. That is what
makes it a tombstone rather than a blocklist.

**The bug this design produced, and the test that found it.** Applying a peer's
drop initially recorded the tombstone under a *fresh local* stamp rather than
the originating one. Local clocks have witnessed the peer's stamps and moved
past them, so the tombstone landed ahead of a recreation that legitimately
followed the drop — making the name permanently unusable on that node.
`drop_collection_inner` now takes the originating stamp explicitly, and the
parameter that decides whether to log is the same one that decides which stamp
to record, because the two must agree.

---

## ADR-035 — Replication pulls over TCP; peers authenticate mutually

**Decision.** A TCP listener answers two questions — "what do you hold?" and
"send me everything after this point" — and every node periodically asks them of
every peer discovery resolves. Length-prefixed BSON frames. No push half.

**Pull, not push.** A round is one-directional, and both peers running the same
loop against each other is what converges them. A push half would need its own
retry, ordering and back-pressure story to say nothing a pull does not already
say, and a node that is behind is the one with the information about *how* far
behind it is.

**BSON, not JSON.** The payload is oplog entries, which are already BSON-shaped.
A JSON hop would have to re-derive `Hlc`, `DocId` and binary bodies from a
representation that cannot hold them — the same reason storage is BSON and only
the HTTP edge is JSON.

**The transport decides nothing.** It moves bytes; `kimmy-storage` decides what
wins, what is missing, and how a merge resolves. That half was built and tested
without a network deliberately, so a convergence failure and a dropped packet
cannot be confused for one another.

### Mutual authentication, in three messages

```text
Hello   { node, nonce_a }              ───▶
        ◀───  Welcome { node, nonce_b, HMAC(secret, nonce_a) }
Confirm { HMAC(secret, nonce_b) }      ───▶
```

**Three, not two.** A challenge has to be *received* before it can be answered;
an initiator cannot prove a nonce the responder has not chosen yet. The first
version of this got that wrong — it had the initiator signing a nonce it
generated itself, which the responder then checked against a different one, so
no valid handshake existed.

**Mutual, not one-sided.** A server-only check would let anything that can open
a socket read the entire oplog by simply never demanding proof in return. The
initiator therefore verifies the responder *before* answering its challenge —
answering first would hand an unauthenticated peer a valid proof for a nonce it
chose.

**Constant-time comparison.** A byte-by-byte check leaks how much of a forged
proof was correct, which recovers the rest one byte at a time.

**This is authentication, not confidentiality.** Frames are plaintext; anyone on
the path reads replicated documents. TLS is M5. The secret's job today is to
stop an unrelated process — a node pointed at the wrong cluster, most likely —
from joining and merging its data in.

**The frame limit is checked before allocating.** A length prefix read from the
network is attacker-controlled, and trusting it enough to allocate is how one
malformed frame becomes an out-of-memory kill.

---

## ADR-036 — A peer past the retention horizon gets state, not history

**Decision.** When a peer asks for oplog entries from a point retention has
already collected, it is told so and sent **current state** instead: collection
definitions, then documents in pages, then the sender's version vector.

**The failure this fixes was not an optimisation.** Anti-entropy replays oplog
entries, which reach back only as far as `oplog_retention_secs`. A node joining
an older cluster asks for history nobody holds. Probed rather than assumed:

```
A documents:                      20
entries A could still offer:       1   (the retained tail)
outcome:  unknown_collection: 1        (the CreateCollection entry is gone)
B documents:  collection missing entirely
B still considers itself behind:  Some(Hlc(0.0))
```

With the default retention this is *any* node added to a cluster more than a day
old — which is to say, adding a node to a running cluster.

**It was at least honest.** B never falsely believed it was caught up, so there
was no silent divergence, only an infinite retry. That is why the fix is a
fallback rather than a correction: nothing was wrong, something was missing.

**The horizon is recorded, not inferred.** `oplog_collected_through` holds what
retention has actually removed. The oldest *retained* entry cannot stand in for
it: on a node that has never collected anything, that is simply the first write
ever made, and a peer asking from before it would be sent a full snapshot it did
not need.

**Documents arrive as oplog entries.** Each is applied through the same
`apply_remote` replication uses, carrying the stamp the document actually holds.
Not to save code — it is what keeps the result correct: last-writer-wins still
decides, so a receiver holding a newer version keeps it; indexes are maintained,
so the new node can answer index-backed queries; and unique violations are
detected rather than smuggled past the check through a side door.

**Collection definitions are not logged.** Unlike a document's stamp, a snapshot
carries no honest record of when or where a collection was created — only that
it exists. Inventing history would be worse than omitting it, and a node that
joined by snapshot can pass the collection on by snapshotting in turn.

**Coverage is merged, not adopted.** The receiver may hold writes the sender has
never seen; taking the sender's vector outright would claim it had forgotten
them.

**The version vector stopped being derived state.** It was rebuilt from the
oplog on every open, which would have recomputed a completed snapshot's coverage
away and sent the node straight back to asking for history it cannot be given.
The oplog is now a *lower bound* on coverage: opening raises the vector to cover
it and never lowers it.

---

## ADR-037 — Local peer health, and SWIM membership above it

**Decision.** Two layers, built in that order. Each node keeps private
bookkeeping — exponential backoff for peers that fail it, and a fixed fanout
contacted per round in rotation. Above that, SWIM via [`foca`](https://github.com/caio/foca)
over UDP gives the cluster a *shared* opinion about who is alive.

**Why both.** They answer different questions and neither subsumes the other.

Backoff and fanout are about *this node's* costs: not retrying a peer that is
not coming back, and not opening O(n²) connections per interval across the
cluster. Those are local problems with local fixes, and they still apply with
membership running — SWIM tells you a peer is up, not that your last three
attempts to reach it timed out.

SWIM is about *agreement*: a node that cannot reach a peer asks others to probe
it indirectly before anything is declared, so one bad link does not evict a
healthy node, and a genuine failure is agreed rather than rediscovered
independently by everyone. It also learns members that were never configured —
verified with three daemons where the third was told only about the first and
learned the second by gossip.

**The order was wrong at first, and worth recording.** I built the local layer
and wrote this ADR as *instead of* SWIM, having judged that discovery on
Kubernetes covered enough. That reasoning held for the deployment shape and
missed the point: gossiping state between peers with no leader is a stated goal
of the project, not an implementation detail to be optimised away. The local
layer was the right thing to build; presenting it as a replacement was not.

**Membership is on by default and can be turned off.** With `membership = false`
peers come from discovery alone and each node forms its own private opinion —
which is exactly the earlier behaviour, kept because it is a reasonable
single-datacentre configuration and because it is what runs if the UDP port is
blocked.

**Two protocols share one port.** UDP carries probes and membership, TCP carries
version vectors, oplog entries and snapshots. One address to configure, one
firewall rule to get wrong instead of two.

**A wildcard bind is not an identity.** `0.0.0.0` is a listening instruction;
announcing it would tell the cluster to probe an address that routes nowhere.
The advertised address falls back to loopback with a warning, which keeps a
single-host cluster working and makes the misconfiguration visible.

**Identities carry an incarnation.** Without one, `Identity::renew` has nothing
to change and a node evicted by a transient fault could never rejoin — it would
keep offering an identity the cluster had already buried.

**On the licence.** `foca` is MPL-2.0. For an unmodified dependency the
obligation amounts to keeping the notice; MPL § 3.3 explicitly contemplates use
within a larger work under other terms. Raised before adding it rather than
after, because dependency choices here have been deliberate — and recorded here
so a later licence audit finds the reasoning rather than a surprise.

---

## ADR-038 — Login is rate-limited before the password is checked

**Decision.** `/v1/auth/login` carries a token bucket per client address. The
budget is consulted **before** `UserStore::authenticate` runs, and a token is
spent only when authentication **fails**.

**Why before, not after.** Every login attempt runs a full Argon2id
verification, including for a user that does not exist — deliberately, since
equalising that cost is what stops timing from revealing whether an account
exists. At the configured work factor (`m=19456`, `t=2`) that is roughly 19 MB
and milliseconds of CPU per request, available to anyone who can reach the port.
A limit applied after the hash would return the same `429` while performing the
exact work it exists to prevent. The limit is therefore not only an
anti-guessing measure; it is the only thing bounding an amplification vector.

**Why only failures are recorded.** A caller with correct credentials is not the
threat being defended against. Charging them would turn a security control into
a capacity control, and would throttle exactly the legitimate case — a fleet
re-authenticating on a short `token_ttl_secs`. The consequence is that a
correct client is never limited *for succeeding*, though it is still refused
while its address is over budget from other callers' failures.

**Why login and nothing else.** Everywhere else in the API a limit is a capacity
control, and capacity numbers chosen without measurement are guesses — which is
the thing M5's benchmarks exist to remove. The mechanism is deliberately
route-agnostic so that the benchmark work can decide the rest;
`kimmy_api::Limiter` maps an arbitrary key to a budget and knows nothing about
login.

**A token bucket, with time as a parameter.** Consistent with
[ADR-007](#adr-007--physical-time-as-a-parameter): `check_at`/`record_at` take
the clock as an argument and the wrappers supply it. Refill across a window, a
bucket that recovers, a burst that cannot be banked, and a clock that steps
backwards all become ordinary unit tests instead of tests that sleep. The
backwards-clock case is not hypothetical — a naive `now - last` on `u64`
underflows to an enormous elapsed time and refills the bucket completely, which
is a limiter an attacker resets by waiting for an NTP correction.

**Rejected: `tower_governor`.** Battle-tested GCRA and much less code to own,
but it reads `Instant::now()` internally, so its behaviour cannot be driven from
a test without real sleeps. That would have made the limiter one of the few
components here whose timing rules are untested, in a codebase whose stated rule
is that a test never seen to fail is of unmeasured value. Hand-rolling it is
~200 lines and keeps the dependency count where the pure-Rust discipline of
[ADR-001](#adr-001--redb-as-the-storage-engine) and
[ADR-016](#adr-016--pure-rust-cryptography) put it.

**The key space is attacker-controlled, so it is capped.** A source address is
whatever packets arrive from and a username is whatever was typed, so an
unbounded map would make the defence against one denial of service into another.
`max_tracked_keys` bounds it; buckets that have fully refilled are evicted first
because they carry no information, and only then the fullest remaining one,
because it has the least evidence of abuse against it.

**A forwarded header is not trusted by default.** `trusted_proxy_header` is
opt-in. Trusting `X-Forwarded-For` where nothing rewrites it would let any
caller mint a fresh budget per request, which is worse than having no limiter,
because the metrics and logs would suggest one was working. When it is set, the
**last** value is used, not the first: a proxy appends the peer it saw, so the
rightmost entry is the only one the client did not supply. Verified against a
running server — prepending an attacker-chosen address to a drained peer's entry
still returned `429`.

**Per-username limiting exists and defaults to off.** It is the only defence
against a guess spread across many source addresses, and it introduces a
lockout: anyone reachable can spend a named user's budget and keep the real
holder out for the window. Trading a guessing risk for a denial-of-service risk
is a deployment-specific judgement, not something a default should make on an
operator's behalf. Both behaviours are tested; switching it on is one config
value.

---

## ADR-039 — TLS terminates natively, on the provider already in the build

**Decision.** The HTTP, WebSocket and MCP listener terminates TLS itself, via
`axum-server` over `rustls`, with **`ring`** as the crypto provider. Enabled by
naming a certificate and a key; there is no separate toggle.

**Why native at all, when a proxy could do it.** A proxy remains a perfectly
good deployment and nothing here forces the change. But "terminate at a proxy"
was the *only* answer, which made a single node with no proxy in front of it
unable to protect a password in transit — including the bootstrap login that
sets one. A database that cannot be run safely on its own is a database with a
prerequisite it never declared.

**Why `ring`, not the default.** `axum-server`'s `tls-rustls` feature selects
`aws-lc-rs`, which needs CMake and a full C build. `ring` is already compiled
into every build via `reqwest` (see the correction on
[ADR-016](#adr-016--pure-rust-cryptography)), so choosing it adds **no new
toolchain requirement**, while the default would add a second native crypto
stack for the same primitives. Hence `tls-rustls-no-provider` plus an explicit
`ring` provider installed at startup — explicit so the choice is visible in the
code rather than resolved by feature unification, which is exactly how a build
acquires a dependency nobody chose.

**Why no `enabled` flag.** TLS is on when both `cert_file` and `key_file` are
set. A separate toggle would create a state — enabled with no certificate —
whose only possible meaning is a startup failure. Naming exactly one of the two
is refused for the same reason: it is unambiguously a mistake, and the useful
moment to say so is at startup rather than at the first handshake.

**Certificates are read before the socket is bound.** A missing or unreadable
file stops the node with a message naming it. The alternative is a node that
starts, reports healthy, and fails only for whoever connects first — the failure
lands on traffic instead of on the person who can fix it. Same principle as
refusing a bad configuration at startup.

**Plaintext on a public bind warns, and does not refuse.** Terminating at a
proxy or a service mesh is legitimate, so refusing to start would break real
deployments. But the risk is invisible from the server's side — nothing about a
successful request reveals that the token authorising it crossed the wire in the
clear — so it is said out loud once at startup.

**What the serving stack had to preserve.** `axum::serve` has no TLS, so the TLS
path runs on `axum-server`, and two existing properties depend on the serving
stack rather than on the router:

- **Connection info.** The login limiter keys on the peer address, and losing it
  fails *silently* — requests still succeed, and the only symptom is every
  caller sharing one bucket. Both paths use
  `into_make_service_with_connect_info`, and a test asserts a real handler sees a
  real address through each.
- **Graceful shutdown.** Measured at 52 ms on a TLS node against ~20 ms
  plaintext; both are drain-then-exit rather than a hard kill.

**A bug this found before it shipped.** The first version called
`set_nonblocking(false)` on the listener handed to `axum-server`, reasoning that
a `std` listener wants blocking mode. Tokio panics when a blocking socket is
registered with the runtime — and it would have panicked at the *first TLS
connection*, not at startup, so a smoke test that only checked the process was
up would have passed. Caught by the end-to-end test on the first run.

**WebSocket survives ALPN negotiating h2**, which was not obvious and was checked
rather than assumed: axum's upgrade is HTTP/1.1-only (no RFC 8441), so a
connection that had genuinely switched to HTTP/2 could not carry a change
stream. Hyper's auto builder sniffs the connection preface and serves HTTP/1.1
when it does not see the h2 one, so a client offering `h2,http/1.1` still opens a
stream. Verified against a running node with a real write flowing through it.

**Out of scope, deliberately:** client certificates (mTLS), certificate reload
without a restart, and any HTTP→HTTPS redirect — one listener, one port, no
plaintext half. Node-to-node TLS is a separate piece with its own trust
question; `cluster_secret` still authenticates peers without encrypting them.

---

## ADR-040 — Replication TLS is bound to `cluster_secret`, not to certificates

**Decision.** Node-to-node replication runs over TLS. Each node generates a
self-signed certificate at startup and **neither side verifies the other's**.
Instead the existing mutual HMAC handshake additionally signs the TLS session's
exported keying material ([RFC 5705]):

```text
proof = HMAC(cluster_secret, len(nonce) || nonce || len(exporter) || exporter)
```

Always on. There is no switch.

**The problem.** `cluster_secret` already authenticated peers — a three-message
challenge-response where neither side transmits the secret ([ADR-035]). What was
missing was confidentiality: frames carrying oplog entries were plaintext, so
anyone on the path could read replicated documents.

**Why not operator certificates.** The conventional answer is a CA and per-node
certificates with mutual verification. It composes with existing PKI, and it
makes certificate distribution and rotation a standing burden on every cluster
— including a two-node one on a private network — while adding a new way to
lock a cluster out of itself. For a database whose clustering story is "set one
shared secret and name a seed", requiring a PKI to encrypt is a large step
backwards in operability.

**Why unverified TLS alone is not enough.** It stops a passive eavesdropper and
nothing else. An active attacker terminates two TLS sessions and relays between
them, reading everything. The HMAC handshake does not help on its own, because a
relay can forward the challenge and its answer untouched.

**What channel binding adds.** The exporter is derived from secrets specific to
one TLS session. A man-in-the-middle holds two, so the value it sees on one side
never equals the value on the other; the proof it relays is computed over the
wrong bytes, and recomputing it requires `cluster_secret`, which it does not
have. The result is confidentiality *and* man-in-the-middle resistance, with the
secret remaining the only thing an operator manages.

**This is asserted, not argued.** `a_man_in_the_middle_cannot_relay_the_handshake`
stands up a relay that really does terminate TLS on both sides and really can
read the frames, and requires the handshake to fail. Removing the binding from
the proof makes that test fail while its control — two nodes converging with
nobody in the middle — still passes, so the failure is specifically the relay
rather than replication being broken.

**Signature verification is still performed.** Only "is this certificate one I
trust" is waived. The TLS handshake signature proves the peer holds the key for
the certificate it presented, which is what makes the session, and therefore the
exporter, belong to a single endpoint rather than being splice-able.

**Length-prefixed inputs.** `nonce || binding` alone would let a nonce of `AB`
with binding `C` hash identically to `A` with `BC`. The same reasoning as the
separator in `CollectionId::derive` ([ADR-031]).

**No switch, and the upgrade cost that implies.** `cluster_secret` must already
match cluster-wide; a second setting that must also match is another way to
misconfigure a cluster, and one whose failure mode is silent plaintext. The cost
is that a cluster cannot be upgraded to this version node by node — a node
speaking TLS and one speaking plaintext cannot talk. Pre-1.0, with no
compatibility promise, that is the right trade; it is called out in
[Operations](operations.md) because it is a real operational consequence.

**Certificates are ephemeral and per process.** They prove nothing, so
persisting one would create a key to manage and leak for no benefit.

**What this does not do.** It does not authenticate a node's identity beyond
"holds the cluster secret". Every holder is equally trusted, which is what the
secret already meant.

[RFC 5705]: https://www.rfc-editor.org/rfc/rfc5705
[ADR-035]: #adr-035--replication-pulls-over-tcp-peers-authenticate-mutually
[ADR-031]: #adr-031--collection-ids-are-derived-from-the-name-not-allocated

---

## ADR-041 — Backup is online and served; restore is offline and refuses to overwrite

**Decision.** `GET /v1/admin/backup` streams a consistent backup from a running
node. `kimmyd restore --from <file>` writes one into a data directory that does
not yet contain a database. The backup carries the node's identity.

**Why the backup is an endpoint rather than a command.** redb allows one process
to hold a database, so a separate `kimmyd backup` process could not open a live
one. The only way to take a backup *without stopping the node* is for the node
to take it. Copying `kimmy.redb` from underneath a running node copies a torn
file — pages are being rewritten during the copy, and the result is not a state
the database was ever in.

**Consistency comes from a read transaction.** The whole walk happens inside
one, so redb's MVCC gives every table the same instant, writers are neither
blocked nor affected, and a backup taken under load is a snapshot rather than a
mixture. A test writes concurrently while a backup runs and asserts everything
committed beforehand is present.

**The response is buffered, not streamed as it is produced.** Streaming would
hold the read transaction open for as long as the client took to read, pinning
MVCC pages to a slow socket. Memory is the cheaper cost and is bounded by the
database rather than by the caller.

**Backups include index entries**, though they are derivable from documents.
Recomputing them on restore would make a restore's correctness depend on
replaying index maintenance exactly — the part most likely to differ between
versions. Copying them makes a restore a transcription rather than a
re-derivation.

**`admin` over `*` is required.** A backup is every document on the node, so a
lesser grant would let a database-scoped administrator read past their own.
There is deliberately **no grant-filtered backup**: a partial backup that looks
whole is a restore that silently loses data.

**Restore refuses an existing file.** An in-place restore turns a mistyped path
into data loss. An operator who wants to overwrite can remove the file, having
thought about it.

**The identity travels with the backup, and there is no flag to change it.** The
node id is the tiebreak half of every write's stamp ([ADR-006]), so restoring
under a fresh identity makes the node a stranger to its own history — every
last-writer-wins comparison against its old writes changes meaning. So restore
keeps it.

The sharp edge is that restoring one backup onto two nodes puts one identity on
both, and the cluster then cannot tell them apart, which breaks the tiebreak
convergence depends on. **Restore is for replacing a node, not cloning one.** A
`--new-identity` flag would be one keystroke between recovering and corrupting a
cluster's identity space, so cloning is not offered here; the supported way to
add a node is to start an empty one and let anti-entropy fill it ([ADR-036]).
The CLI says so on every restore.

**Explicit format, with a version byte**, for the reason [ADR-003] gives for the
on-disk records. A backup a future version cannot read is a backup that does not
exist, and a format defined by a serde derive changes when a dependency does. An
unknown table tag is refused rather than skipped: skipping would restore a
database silently missing whatever it held.

[ADR-006]: #adr-006--hlc-with-node-id-tiebreak-whole-document-lww
[ADR-036]: #adr-036--a-peer-past-the-retention-horizon-gets-state-not-history
[ADR-003]: #adr-003--hand-rolled-binary-codec-for-hot-records

---

## ADR-042 — The audit log hangs off the authorization point, not the routes

**Decision.** Authorization decisions are recorded inside
`Auth::require` — the one function every check funnels through — at the
`kimmy::audit` tracing target. `audit.mode` selects `off`, `denials` (default),
`writes` or `all`.

**Why there and not at each route.** A log each handler has to remember to write
is a log with holes in it, and the holes are invisible: nothing about a missing
audit line says it is missing. The check already lives in one place
([ADR-013]); the record belongs beside it, so a new route inherits auditing
by inheriting the check rather than by anyone remembering.

**Why the mode is process-global.** It is a property of a deployment, not of a
request. Threading it through would put a configuration parameter into the
signature of code that has no other reason to know configuration exists. It is
set once, before anything can be authorized, and read atomically.

**Why `denials` is the default.** `all` writes one line per authorized
operation, which on a read-heavy node is one line per request — a real cost, and
one an operator should opt into. `off` would mean nobody gets the event they
actually want. A denial is rare and is what someone is watching for.

**`search` and `watch` count as reads at `writes`.** One ranks documents, the
other observes them; neither changes anything, and an auditor asking "what
changed" does not want them.

**Authentication is not audited here.** A failed login is not an authorization
decision — there is no principal yet — and it is already logged and counted.
Mixing them would make "denied" mean two things in one stream.

**A bad mode fails at startup.** A typo would otherwise produce a server that
records nothing, which looks exactly like a server nobody has attacked.

---

## ADR-043 — Metrics count statuses, not call sites

**Decision.** `/metrics` gains uptime, request and response counters, storage
size, and counters for authorization denials, authentication failures and rate
limiting. The three specific counters are derived in one middleware from the
response status rather than incremented where the refusal happens.

**Why derived.** Each of those statuses has exactly one source: 401 from token
or credential rejection, 403 from `ApiError::forbidden` (RBAC and nothing else),
429 from the rate limiter. Counting them in one layer means a new route is
counted by existing, and a counter that lives beside a check is a counter
someone forgets to bump.

**Plain atomics rather than a metrics framework.** Nine numbers and a fixed set
of series; a registry would add a dependency and an abstraction for no gain.

**Counters render at zero rather than appearing on first use.** A series that
materialises only after its first event makes a dashboard show "no data" where
it should show "nothing has gone wrong yet".

**Still no per-collection series.** `/metrics` is unauthenticated, and a series
per collection puts the schema on it — the same reason the endpoint has always
reported counts rather than names.

**Deliberately absent.** Latency histograms need buckets chosen from
measurements that exist for the storage layer but not for end-to-end requests,
and a histogram with guessed buckets reports confidently about the wrong ranges.
Oplog lag needs a peer's version vector, which the API layer does not hold and
which the replication loop would have to push here. Both are worth doing and
neither is worth guessing.

[ADR-013]: #adr-013--one-authorization-decision-point

---

## ADR-044 — Point-in-time restore rewinds from post-images, and refuses what it cannot reconstruct

**Decision.** `kimmyd restore --from <backup> --until <ms>` restores a backup and
then rewinds document state to that instant using the oplog the backup carries.
It refuses — having written nothing — when the target predates the oplog
horizon, when a schema change happened after it, or when any document's value at
that instant is no longer recoverable.

**What the oplog can and cannot answer.** It stores **post-images**
([ADR-008]): what a document *became*, never what it was. A delete stores
nothing at all — `DocRecord::tombstone` discards the body and the `Delete` entry
has no payload. So the only history that exists is the sequence of values
documents took, and a rewind can put a document back only to a value the
retained oplog still holds.

That gives an exact rule for each document changed after the target `T`:

| Condition | State at `T` |
|---|---|
| It has an entry at or before `T` | That entry's post-image (or a tombstone, if it was a delete) |
| Its earliest entry after `T` is an `Insert` | It did not exist yet — remove it |
| Otherwise | **Unrecoverable**: it existed, and its value has been collected |

**The third case is refused, not guessed.** Leaving such a document at its
*later* value would produce a database that looks restored and is not — a wrong
answer no caller could detect, which is the worst outcome available here. The
affected documents are named.

**`oplog_retention_secs` is therefore the point-in-time window**, and that is
worth knowing when choosing it. A mistaken update to a document written within
the window is recoverable; one to a document untouched since before the horizon
is not, because its previous value exists nowhere.

**Dropped collections cannot be undone**, and any schema change after the target
is refused for the same reason: `drop_collection` purges the documents, purged
documents are not in the oplog either, and a rewind that recreated an empty
collection would be answering a question it was not asked.

**Nothing is written until everything is known.** The whole plan is computed and
every refusal raised before a single document is touched, so a refused rewind
leaves the database exactly as it was — asserted by a test.

**The undone future leaves the oplog, and the version vector comes down with
it.** An entry describing a change that no longer exists would be shipped to a
peer, which would ship it straight back and undo the rewind. And a version
vector left high would have the node claim history it no longer holds, so no
peer would ever send that range again — it would be permanently missing writes
while looking caught up.

That lowering is the **one** legitimate exception to the rule that the version
vector is authoritative and never rebuilt downwards. The rule exists for
snapshot resync, where a snapshot grants coverage the oplog never held. Here the
history is gone because this operation deliberately removed it, offline. It has
its own function, `reset_version_vector_to_oplog`, rather than relaxing the
existing one.

**A rewound database must not rejoin a cluster that still holds the undone
writes.** Anti-entropy would put them back. Rewind produces a database to run
standalone or to seed a new cluster; the CLI says so.

**`restore` skips the serving configuration.** It writes a file and exits — it
never authenticates anybody — so requiring a root password would mean an
operator recovering from an incident has to invent one first. Found by running
it.

[ADR-008]: #adr-008--the-oplog-is-written-unconditionally

---

## ADR-045 — Webhook delivery is owned by a derived node, over replicated progress

**Decision.** Each subscription is delivered by exactly one node, chosen by
rendezvous hashing over the live SWIM member set. Every node records what it has
delivered as a `VersionVector` in its **own** progress document, and any node
reads the union. At-least-once, ordered per subscription.

**The two obvious designs both fail.** If the *originating* node delivers, there
is exactly one request — until that node dies before dispatching, and then
nobody delivers at all, because its peers hold the data but consider it not
theirs. A client silently never receives an event. If *every* node delivers,
nothing is lost and a five-node cluster fires five identical requests per write.

Both fix *which node delivers* in advance, and that framing is what forces the
choice. The way out is to make delivery progress replicated state.

**Progress is written per (subscription, node).** A node only ever writes its
own record, so there are no write conflicts and nothing for last-writer-wins to
discard — the union is the cluster's answer. This is the same shape as
`oplog_versions`, and it uses the same type, so "what have I not delivered?" is
answered by `VersionVector::behind`, exactly as anti-entropy asks it of a peer.

**Ownership is derived, not elected.** `owner = rendezvous_hash(subscription,
live_members)` is a pure function computed identically and independently by
every node. There is no vote, no term, no consensus and no cluster-wide
coordinator — this is not leader election, and the project's leaderless premise
is intact. A transient membership disagreement produces a *duplicate delivery*,
not a split brain.

**Rendezvous, not modulo.** `hash(subscription) % members.len()` remaps almost
every subscription whenever the member count changes, so one node leaving would
shuffle the whole cluster. Rendezvous moves only what the departed node owned,
which is what makes failover cheap — and there is a test asserting nothing else
moves.

**FNV-1a, not `DefaultHasher`.** The standard hasher is explicitly not stable
between Rust versions, and an ownership function that changed under a compiler
upgrade would reshuffle every subscription on a rolling restart. The mapping is
pinned by a test, cross-checked against an independent implementation.

**So a dying node does not cost an event.** It leaves the live set, every
survivor recomputes, one becomes the owner, and it resumes from the union of
progress. The only way an event is never delivered is if the write never
replicated off the node that accepted it — in which case the data is gone from
the database too, and webhooks are not what failed.

**At-least-once, stated rather than implied.** Progress advances only after an
endpoint accepts, so a crash mid-flight redelivers. Exactly-once is not
achievable over a network, so every delivery carries the originating `Stamp` as
`X-Kimmy-Event-Id` — globally unique, identical on every node, stable across
redeliveries — and deduplicating is a set-membership test.

**Addresses, not node ids, feed the hash.** `Members` publishes `SocketAddr`,
and hashing that keeps ownership a pure function of what SWIM already provides.
Re-addressing a node reshuffles its subscriptions, which is the same disruption
as that node leaving and another joining. Chosen over building an
address-to-node-id mapping purely to hash it.

**Deliveries are signed** with `HMAC-SHA256(secret, timestamp || "." || body)`.
The timestamp is *inside* the signature: signing the body alone would leave it
free to change, so a captured delivery could be replayed later with a fresh one
and still verify.

**Redirects are refused and the egress policy is re-checked before every
delivery**, not only at registration. A hostname is not a destination — a name
that resolves publicly today can resolve to `169.254.169.254` tomorrow, and a
permitted host answering `302` would otherwise walk the request straight through
the policy.

**A pass plans serially, delivers concurrently under a bound, and applies
serially.** Only the network call overlaps; every engine read and write stays on
one thread, so two subscriptions can never race each other's progress record.
The bound (`webhooks.max_concurrent_deliveries`, default 8) is what stops a
webhook on a hot collection consuming every outbound connection the node has.

The concurrency is not a throughput tweak. The dispatcher was serial, which
meant one endpoint that had stopped answering held the whole pass for the
ten-second delivery timeout and delayed every subscription behind it — the exact
cross-subscription interference the per-subscription backoff exists to prevent,
one layer up. A webhook nobody controls decided when the ones they did control
fired.

**The bound was asked for before it could bind.** M6 planned this cap "so a
webhook on a hot collection cannot saturate a node's outbound connections",
which the serial dispatcher made impossible — one `.await` per subscription in a
`for` loop is a hard limit of one request in flight. The cap became meaningful
only once concurrency was introduced, so the two arrived together rather than
the cap being the fix it was described as. Recorded in
[Deviations](deviations.md), because a mitigation built for a risk that cannot
occur is worth noticing before the next one.

**An event is never dropped for being large.** Batches are trimmed to
`webhooks.max_payload_bytes`; a single event whose document alone exceeds it is
delivered with `fullDocument` omitted and `fullDocumentOmitted` set, so the
receiver still learns the change happened and can read the document itself.
Skipping it would leave a gap the receiver could never detect — which is exactly
what invalidation exists to avoid, so doing it silently for a large document
would contradict the rest of the design.

**The resume point is written forward on a heartbeat, even when nothing is
delivered.** Retention collects by age, and a position that only moved on a
successful delivery would sit still while the horizon walked toward it: a
webhook on a quiet collection — or on a busy one that goes quiet overnight — was
invalidated for falling behind events it was never going to be sent. Every
healthy webhook died one retention window after its last delivery.

Deciding an entry is not yours is work, and the position has to move over it.
Advancing is safe by construction: the scan is contiguous from the resume point
and nothing in it matched the subscription's filter, so nothing deliverable is
stepped over. Once a minute rather than once per tick, because recording
progress is itself a write — it appends the very entry the next pass reads, and
doing it every two seconds would have an idle node writing to the oplog, and
replicating it, forever.

**Removing a subscription removes its progress records.** Otherwise
`__webhook_progress` keeps one orphan per node that ever delivered it,
replicating and being backed up, with nothing left that would ever read them.

---

## Superseded / reconsidered

| Original plan | Now | Why |
|---|---|---|
| Slow consumers get `invalidate` | Lag recovers from the oplog | ADR-010 |
| `keyenc` lives in `kimmy-storage` | Lives in `kimmy-core` | `kimmy-query` needs the comparison semantics; putting both in core avoids query→storage coupling |
| Node identity in a file beside the data | Inside the database file | Copying or restoring carries identity with it; one source of truth |
| Flat-then-HNSW vector index | HNSW from the start | User preference, taken during planning |
| HNSW via `hnswlib-rs` | `hnsw_rs` | `hnswlib-rs` requires **nightly** Rust — its `corenn-kernels` dependency uses `#![feature(f16)]`. Named in the plan without checking |
| Graph tombstones for deletes | Skip missing records at read time | ADR-022 — the graph is a candidate source, so a stale node costs nothing |
| `fastembed` local ONNX as the default provider | `byo` is the default | ADR-021 |

---

## ADR-046 — The two metrics ADR-043 refused to guess, each built on its stated terms

**Decision.** `/metrics` gains `kimmy_request_duration_seconds` (a histogram)
and `kimmy_replication_lag_seconds` (a gauge). ADR-043 deferred both with a
reason each; each is now built by answering that reason rather than waiving it.

**The histogram's buckets were measured, not chosen.** End-to-end against a
release build on the development machine (single client, loopback, 10k
documents seeded, 200 samples per shape): point reads by id run p50 ≈ 250 µs
and p99 under 1 ms; filtered finds with a limit 1.4–2.6 ms; single-document
inserts p50 ≈ 6 ms — one durable commit each, agreeing with the M5 write
benchmark; a 10k-document aggregation 10–43 ms. Twelve buckets bracket those
clusters with headroom on both ends (100 µs to 10 s); the wide top bucket
exists so a stall reads as a shape change rather than vanishing into `+Inf`.
The conditions are stated because the numbers are only claims under them —
and stating them mattered: the first draft of this paragraph was written from
expectation, and the measurement corrected it.

**Found while measuring.** `find {_id: …}` is a collection scan — ~7 ms over
those same 10k documents — because the planner consults secondary indexes
only, never the primary key. `GET /docs/{id}` is the point-read path.
Recorded in [Deviations](deviations.md) rather than fixed here.

**Health probes and scrapes are excluded from the histogram** — they run
every few seconds forever and would crowd the buckets real traffic lands in —
but still counted as requests, so a scrape stays visible as traffic.

**Lag is pushed from the replication loop**, the only place a peer's version
vector exists, through a callback on `ReplicationConfig` — the shape ADR-043
predicted. It is computed *after* a sync round, against the vector the peer
opened with: zero in the caught-up steady state, non-zero exactly when the
backlog exceeded one batch. Measured from entry timestamps — the age span of
unapplied work, not of a cursor, which is the lesson the webhook backlog
gauge taught (ADR-043's successor metrics repeat it deliberately).

**Two refusals inside the lag number.** An unreachable cluster reports
nothing rather than zero — the last value stands, because overwriting it
would report an outage as perfect health. And an origin this node has never
seen contributes nothing: only the peer's *newest* stamp is at hand, so the
honest gap would need the oldest, and `newest − zero` is the age of the
epoch — a joining node would open with a fifty-year lie and every alert
would fire.

---

## ADR-047 — The provider audit: Voyage rides OpenAI, Cohere and Gemini get dialects

**Decision.** The three providers the M8 plan named were checked against their
documented API shapes. **Voyage** is OpenAI-compatible, so it is the existing
`openai` provider with `endpoint: "https://api.voyageai.com"` — no new code,
one test that pins it stays covered. **Cohere** and **Gemini** each differ
enough that `custom_http` cannot reach them, so each gains a dialect.

**What custom_http could not reach, precisely:**

- **Cohere** sends `texts`, not `input`; requires `input_type` (omitting it on
  a v3+ model embeds documents under the wrong role and quietly degrades
  recall); and nests the response under `embeddings.float` on v2. The dialect
  sends `search_document` always — the server only embeds documents; a query
  is embedded client-side and must use `search_query`, a Cohere asymmetry
  callers meet outside this server. The response parser accepts both the v2
  nested shape and the v1 flat one, so an account on either version works.
- **Gemini** nests the text under `content.parts`, returns vectors under
  `values`, and authenticates with an `x-goog-api-key` **header** rather than a
  bearer token — which is why `HttpProvider` grew an `Auth` enum. The model
  rides the URL bare (`:batchEmbedContents`) and the body prefixed
  (`models/…`).

**Verified against documented shapes, with fixtures — not live endpoints.**
The suite has never called a live embedding provider: it needs a paid key and
would publish text to a third party, and OpenAI and Ollama have been
fixture-tested since M2 for exactly that reason. The new dialects meet the same
bar — the fixture tests pin the request each builds and the response each
parses, and are where a reviewer checks the shape against current API docs. A
provider that changes its shape is a fixture update, not a silent break; this
is the honest verification level available, stated so the number is a claim
under known conditions rather than a guess.

**Why per-vendor dialects rather than a configurable JSON-path adapter.** The
design already had per-vendor dialects (OpenAi, Ollama), so two more is the
established pattern, not a new axis. A generic path-mapping adapter would be
more surface to get wrong and harder to verify against a fixture, and it would
still not solve Gemini's header auth or URL-embedded model.

---

## ADR-048 — Bulk insert is its own route, and one commit makes it atomic

**Decision.** `POST /v1/db/{db}/coll/{coll}/bulk` takes a bare JSON array and
inserts it in **one redb transaction**, all of it or none of it, capped at 1000
documents. `POST /docs` is untouched. The response is
`{"inserted": n, "insertedIds": [...]}`, in submission order.

**The commit is the whole feature, and the measurement says so.** Release
build on the development machine, `write_path` bench, Criterion medians over
30 samples, documents of ~200 bytes with no secondary indexes:

| Batch | Total | Per document | Against a lone insert |
|---|---|---|---|
| 1 (own commit) | 3.43 ms | 3.43 ms | — |
| `bulk` 1 | 3.41 ms | 3.41 ms | 1.0× |
| `bulk` 10 | 4.68 ms | 0.468 ms | 7.3× |
| `bulk` 100 | 7.66 ms | 0.077 ms | 45× |
| `bulk` 1000 | 19.48 ms | 0.019 ms | **176×** |

That is ~291 documents/sec becoming ~51,300. Fitting the 100 and 1000 points
puts the marginal document at ~13 µs against a fixed commit of several
milliseconds: the durable commit is not merely the dominant cost, it is very
nearly the *only* cost. `bulk` at batch size 1 lands on the single-insert
number, so the batch path adds nothing when there is nothing to amortize.

This is what M8 task 3 predicted. Throughput was flat from one writer to eight
(~300 docs/sec) because redb has a single writer, so parallelism had nothing
to give and per-commit overhead was the only cost left worth removing.

**Atomicity is a consequence, not a goal — but it is promised.** One
transaction makes all-or-nothing free, and the documentation says so, which
means it cannot later be chunked into several commits without breaking a
stated guarantee. That is accepted: chunking would only matter above a batch
size the 2 MB body limit already forbids, and the alternative is worse. The
guarantee is deliberately stronger than `update` and `delete`, which still
apply document by document and can stop partway ([Deviations](deviations.md)).

**Rejected: best-effort with per-document results.** Returning
`{"ok": true/false}` per document requires isolating each failure in its own
commit — which is exactly the cost the feature exists to remove. It would have
bought a nicer error report by giving up the entire 176×. Instead the batch
fails whole and the error names the offending position, which is the only
thing a caller can act on when nothing was written.

**Rejected: an array body on `POST /docs`.** It needs no new route, but it
makes the response shape *and* the atomicity guarantee depend on the request
body's type — a caller could not tell from the URL which contract they were
getting, and the MCP tool's argument would go polymorphic with it. The
codebase already puts multi-document verbs on their own paths (`/find`,
`/count`, `/aggregate`, `/update`, `/delete`), so a sibling `/bulk` is the
established pattern.

**`/bulk`, not `/docs/bulk`.** `/docs/{id}` already owns that space. Static
segments win in the router, so the two would not collide — but the document
whose `_id` is literally `"bulk"` would become unreachable, and a public path
that quietly shadows a legal key is a trap laid for one unlucky user.

**The cap is 1000, and the body limit usually binds first.** A batch is held
in memory whole and then published as one event per document into a bounded
broadcast channel, so it needs a ceiling. 1000 sits well past where the
per-document curve flattens, and for any document over ~2 KB axum's 2 MB
request body limit stops the caller sooner. That limit is left where it is:
raising it is a memory-pressure change, and nobody has measured it.

**A batch is checked against itself.** Two documents sharing an `_id`, or
colliding on a unique index, fail the batch — the occupancy check and the
unique probe both read inside the transaction, and a redb read sees its own
transaction's uncommitted writes. That property was never exercised before
this branch; it now has a test on each of the two paths, because the whole
correctness argument for reusing the single-document checks rests on it.

---

## ADR-049 — Certificate reload on both SIGHUP and an mtime poll, because neither alone covers both deployments

**Decision.** A renewed certificate takes effect without a restart. One
`reload` function sits behind two triggers: **SIGHUP**, and a **60-second
timer** that reloads when either file's mtime has moved. A failed reload leaves
the certificate currently in use serving, logs a WARN naming the file and the
parse error, and increments
`kimmy_tls_reloads_total{outcome="ok"|"failed"}`.

**The mechanism was already there, which is why this ADR is about the trigger
and almost nothing else.** `axum-server` holds its
`RustlsConfig` as a shared, swappable handle that the running acceptor reads
per handshake, and exposes `reload_from_pem_file`. So a reload is a store into
that handle: no rebind, no restart, no dropped connections. Connections already
in flight finish under the certificate they negotiated with; the next handshake
gets the new one. Nothing had to be built to make that true — it had to be
*triggered*.

**ADR-039's constraint is satisfied by the API, not by code written here.**
That ADR reads certificates before binding the socket so a bad one stops the
node rather than failing for whoever connects first — which raises the obvious
hazard for a reload: a bad *new* certificate must not take down a node that is
serving perfectly well. Reading `reload_from_pem_file` rather than assuming:
it parses the pair into a new `ServerConfig` and only then stores it, so a
failure returns `Err` with the live configuration untouched. The startup rule
and the reload rule are therefore different on purpose, and both are right — a
bad certificate at startup is fatal because there is nothing to fall back to,
and a bad certificate at reload is survivable because there is.

**Why both triggers, when either alone is smaller.** They cover different
deployments, and this project ships for both:

- **SIGHUP** is the Unix convention and what an operator on systemd or bare
  metal reaches for. It costs almost nothing here — `shutdown_signal` already
  installs a `tokio::signal::unix` handler, so this is the same machinery
  again. But there is no convenient way to signal PID 1 of a Kubernetes pod.
- **The poll** is what works unattended where certificates are rotated *by*
  something rather than by someone — cert-manager rewriting a mounted Secret,
  which this project plausibly meets given `k8s:` discovery already exists.
  But a poll alone leaves an operator who has just replaced a file with no way
  to say "now"; they wait out the interval.

Neither is redundant with the other, and both reduce to one `reload` call, so
the second trigger is a `select!` arm rather than a second implementation.

**The half-rotated pair is handled rather than avoided.** Replacing a
certificate and its key is two writes, and between them the pair does not
match. That window is not closed — it is absorbed: a mismatched pair fails to
parse, the old certificate keeps serving, and the next tick retries. The worst
case is up to 60 seconds more on the previous certificate, which is correct
behaviour rather than an outage. Kubernetes Secret mounts swap a symlinked
directory atomically and mostly do not produce the window at all; two `cp`
commands on bare metal do.

**The interval is a constant and the poll only runs with TLS configured.**
Rotation is not urgent — a renewal lands weeks before expiry — so 60 seconds is
far inside any window that matters, and SIGHUP already covers "now". A
configurable interval would be public surface maintained for a knob nobody has
asked for. With TLS off, `TlsConfig::pair()` is `None` and there is nothing to
watch, so no task is spawned.

**A counter, and deliberately not an expiry gauge — yet.** The trap this
feature introduces is a reload that silently fails: the node keeps serving the
old certificate perfectly, right up until it expires and every client drops at
once. `kimmy_tls_reloads_total{outcome="failed"}` makes an attempted-and-failed
reload visible, which is the failure this branch can create. It does *not*
catch a certificate nobody ever tried to rotate — that wants
`kimmy_tls_cert_expiry_seconds`, parsed from `notAfter`, which is the metric
worth alerting on. It is left out here because it needs `x509-parser` as a new
runtime dependency, and adding one to a branch about *triggers* is how a branch
stops being about one thing. Recorded in [Deviations](deviations.md) as the
follow-up rather than dropped.

---

## ADR-050 — SRV discovery takes a resolver crate, stripped to the one thing it is for

**Decision.** `dns-srv:` resolves. `hickory-resolver` provides it, with
`default-features = false` and exactly two features: `system-config` and
`tokio`.

**The standard library could never have done this.** `tokio::net::lookup_host`
resolves *names*, which is enough for `dns:` and `k8s:` because those pair
every address with a port from configuration. SRV's entire purpose is that the
record carries the port, so reading one means reading a record type the
standard resolver does not expose. That is why this sat unimplemented from M4:
not difficulty, but a dependency nobody wanted to take blindly.

**The feature list is the whole decision, and it is load-bearing.** Every
transport this crate speaks beyond plain DNS — DoT, DoH, QUIC, h3 — and DNSSEC
validation each ship in a `-ring` and an `-aws-lc-rs` flavour. Selecting any
aws-lc-rs variant would add CMake and a second implementation of primitives
`ring` already provides, which is precisely the rule the M2 correction to
ADR-016 left standing. None of them is needed to read an SRV record over UDP.
`./scripts/check-native-deps.sh` still reports `cc` alone, and the default
build still contains no `aws-lc-rs` and no `openssl` — checked rather than
assumed, because the last time this property was asserted in prose it had
already been false for two milestones.

`system-config` earns its place separately: it reads `/etc/resolv.conf`, which
is what makes a container inherit its cluster's resolver instead of needing one
configured. Without it the feature would work everywhere except the place it is
for.

**Resolution is two steps, because SRV is two facts.** An SRV record names a
*host and a port*, not an address. So: read the SRV records, then resolve each
target and pair every address it yields with the port from the record that
named it. Verified on two live nodes on **7911 and 7922** — neither the 7900
default — which is the only arrangement that can tell a working implementation
apart from one that resolved the names and quietly used the default port.

**An empty answer is not a failure, and finding that out mattered.** The
resolver reports "no records found" as an `Err`, and the discovery loop warns
on every error it receives, once per tick. A name that exists with no SRV
records yet is exactly what a cluster looks like before its first node
registers — so the straightforward mapping would have produced a warning every
few seconds, forever, while nothing was wrong. `NoRecordsFound` is therefore
translated to an empty set. A test pins it, because the failure is a log nobody
reads rather than a broken cluster.

**A target that will not resolve is skipped, not fatal.** One pod mid-restart
must not cost a node every other peer it was told about, so a failed address
lookup drops that one target at `debug` and keeps the rest. The set is also
deduplicated: two records may name one host, and a host with both A and AAAA
records yields two addresses for one peer.

---

## ADR-051 — Webhook ownership hashes node ids, and the identity carries one

**Decision.** `kimmy_cluster::Member` — the identity SWIM already gossips —
gains a `NodeId`. `Members` becomes a map from address to node id, and
`ownership::owner` hashes **node ids** instead of `SocketAddr`.

**The cost of the old scheme was worse than the thing it was compared to.**
ADR-045 accepted address hashing on the grounds that re-addressing a node was
"the same disruption as that node leaving and a new one joining". Measured over
100,000 subscriptions with three members, one of which changes address:

| | subscriptions moved |
|---|---|
| A node genuinely leaving (3 → 2) | 25.0% |
| **One node re-addressed** (3 → 3) | **50.8%** |

Re-addressing was *twice* as disruptive as the node dying, because it is a
departure and an arrival at the same time: the old address's share scatters,
and the new address then claims a fresh share from everyone. For a node that
never went anywhere. In Kubernetes, where a rescheduled pod routinely comes
back on a different IP, that is not a rare event.

**The node id is the right key because it is already durable.** It lives inside
the database file, so it survives restarts and travels with a restore — the
same property that makes it the tiebreak half of every write's stamp. Nothing
new had to be invented to name a node; the ownership function was simply using
the wrong noun.

**Gossiping it is free.** foca disseminates identities already, so the id rides
along inside `Member` and needs no second channel, no separate broadcast, and
no address-to-node map to keep in step with membership. Verified end to end:
a node learns a peer's id *second-hand*, through a third node it never
announced to.

**An announce carries a placeholder, and foca is built for that.** Discovery
yields an address; which node answers is not known until it does. So
`Member::announcing` uses an all-zero id, and `win_addr_conflict` lets any real
identity displace a placeholder. This is not a workaround — foca accepts an
`Announce` whose `dst` matches on **address alone**, precisely because the
announcer cannot know the target's full identity. The design was anticipated.

**Two nodes claiming one address now resolve.** Both start at incarnation 0, so
the previous rule (`self.incarnation > adversary.incarnation`) made neither
displace the other and the address stalled on whichever was seen first. With a
node id present, ties fall through to comparing it: arbitrary, but *agreed* by
every node, which is the only property that matters.

**This is a hard cutover, and the M8 plan expected otherwise.** The plan said a
mixed-version cluster "produces duplicates, which at-least-once tolerates".
That is wrong, and checking rather than assuming is what found it. foca encodes
identities with postcard, which is not self-describing, so adding a field
changes the wire format:

- a **new** node decoding an old identity fails outright
  (`DeserializeUnexpectedEnd`)
- an **old** node decoding a new one silently "succeeds", reading the first two
  fields and ignoring the trailing id

So a mixed-version cluster does not double-deliver — **membership does not
form**. Nodes must be upgraded together, exactly as ADR-040 required for
replication TLS. Recorded in [Operations](operations.md) beside that one rather
than left to be discovered.

**A simplification falls out.** The dispatcher needed an address to call `me`,
and with clustering off there was none — so a literal `127.0.0.1:7900` stood in
for one. A node id exists whether or not clustering does, so the placeholder is
gone and `Cluster` no longer carries an advertised address at all.

---

## ADR-052 — Token revocation is a per-user version, checked against a cache the oplog keeps honest

**Decision.** The user record gains `token_version`, and the token carries the
value it was issued under. A request is refused when the two disagree, when the
user is `disabled`, or when the user is **gone**. Changing a password or
changing grants bumps the version, which invalidates every outstanding token
for that user at once.

**A version rather than a deny-list, because a deny-list can fail open.** A
list of revoked token ids must have arrived on the node handling the request to
have any effect: replication lag, a lost entry or a node that has not caught up
all produce a token that is honoured because the *revocation* is missing.
Nothing about that failure is visible. A version fails the other way — the
check is a positive match against the user's current state, so a node that is
behind rejects tokens rather than accepting them. The cost is granularity:
this revokes all of a user's tokens or none, and cannot end one session while
leaving another alive.

**Deleting a user needs no bump, and that is the point.** There is no record
left to hold a version, so the absent case is the revocation: an unknown user
is refused. `disabled` works the same way, and already existed on the record —
it was checked at login and nowhere afterwards, which is why disabling an
account left its live tokens working. Both close the recorded debt without a
counter being involved.

**Verification stays a pure function; the lookup lives one layer up.**
`TokenIssuer::verify` still decodes and checks a signature and an expiry, with
no engine and no I/O — it is the piece that must stay cheap and testable. The
version check happens in the `Auth` extractor, which already holds state. That
split keeps the signature check honest about what it does and does not prove.

**The cache is the interesting part, and it is the oplog again.** Reading the
user record on every authenticated request would put a storage read on a path
that currently costs nothing — measured point reads run p50 ≈ 250 µs
([ADR-046](decisions.md)), against authentication that does no I/O at all. So
nodes hold a small in-memory map of user → version, and an ordinary oplog
consumer evicts an entry whenever `__users` is written. That is the same shape
as the embedding worker and the webhook dispatcher: a new background consumer
is "subscribe to the log", not new machinery. It also gets cluster-wide
propagation for free, because a *replicated* write to `__users` publishes on
the receiving node exactly as a local one does.

**Two things evict, and the second was added because the first fails
quietly.** The consumer alone would have made revocation depend on remembering
to spawn a task: the integration tests, which build the router without one,
happily kept honouring revoked tokens until the routes were changed. So a route
that edits a user now **also evicts synchronously**. A single node is then
correct with no background task at all, and a forgotten consumer delays
*cluster-wide* revocation rather than silently disabling revocation altogether.
The two cover different cases — local action versus replicated arrival — so
neither is redundant.

**A miss reads through rather than rejecting.** The map is a cache with the
store as the authority, not a replica of it. That avoids the failure the
alternative invites: a map primed only by the log is empty at startup and
would refuse every request until it filled. Populating on miss means a cold
node is slow for one request per user rather than wrong for all of them.

**Lag clears the cache rather than being tracked.** The broadcast channel is
bounded, so a consumer can be told it missed entries. For a durable consumer
that would mean resuming from a position; for a cache the correct response is
simply to drop everything and re-read. Cheap, and it cannot silently serve a
stale version.

**Changing grants bumps the version, which is the quiet win.** Grants are
embedded in the token, so until now a permission change — *including narrowing
one* — did nothing until the token expired, up to an hour later. The token
comment said as much and called short lifetimes the mitigation. Bumping makes
edits take effect at once. The cost, accepted: there is no refresh flow, so
every permission change forces that user to log in again.

**Rejected: reading the user record per request.** Always correct and needs no
consumer, but it is a disk read on the hot path of every authenticated request,
which is the property this design has been protecting since M1.

**Rejected: a cached read with a short TTL.** It bounds the cost without a
consumer to write, but it makes revocation take effect *after a delay* — a
second bolted onto a feature whose whole purpose is immediacy, and one that
would have to be explained in the security documentation as a window.

---

## ADR-053 — SWIM datagrams are authenticated with the cluster secret

**Decision.** Every membership datagram carries `HMAC-SHA256(cluster_secret,
payload)` as a 32-byte prefix. A datagram whose tag does not verify is dropped
before foca ever sees it, and counted.

**Found by driving a five-node cluster, not by reading the code.** A node
configured with the *wrong* `cluster_secret` joined a real cluster's member
set. Replication rejected it correctly — `peer failed authentication` — and it
could read nothing. But `membership.rs` never referenced the secret at all:
only the TCP replication handshake did, so SWIM was open to anything that could
reach the UDP port.

**The cost was not a leak; it was silent webhook loss.** Ownership is
rendezvous-hashed over the live member set ([ADR-045](decisions.md),
[ADR-051](decisions.md)), so an unauthenticated node becomes an ownership
candidate. It wins roughly `1/(N+1)` of subscriptions and delivers none of
them, because it has no data to deliver. Measured on a three-node cluster with
twelve subscriptions: **eight delivered, four delivered nothing.** Every real
node believed it had correctly stood down. That is the same failure shape as
the bug the cluster harness found in M8 task 1 — an owner that can never be
the one delivering — arrived at from the opposite direction.

**The realistic trigger is a misconfiguration, not an attacker.** Rotating
`cluster_secret` across a cluster one node at a time puts a node in exactly
this state for the length of the rollout. Nothing in any log or metric said a
quarter of the webhooks had stopped.

**Why a MAC on every datagram rather than a handshake.** SWIM is connectionless
by design — that is what makes failure detection cheap — so there is no session
to authenticate once. Tagging each datagram keeps the protocol's shape and adds
32 bytes to packets foca already caps at 1400, against a 64 KB receive buffer.
The same `hmac`/`subtle` pair the replication handshake uses, so no new
dependency and one verification idiom in the crate.

**What this does and does not defend.** It proves a datagram was produced by
something holding the cluster secret. It does **not** prevent replay of a
captured datagram: that would need per-peer sequence state or a synchronised
clock, and buys little here — a replayed message is a peer's own gossip, which
foca's incarnation numbers already order and discard when stale. Origin
authentication was the property that was missing and the property the failure
needed. Stated rather than implied, so nobody reads more into it later.

**Verification is constant-time**, for the reason `proof_is_valid` already
gives: a byte-by-byte comparison leaks how much of a forged tag was right.

**Rejected: encrypting the datagrams too.** Membership traffic reveals a
topology, and an operator who cares can put the cluster on a private network.
Encryption needs a key schedule, nonces and replay handling — a materially
larger design — and it does not fix the reported problem. Recorded in
[Deviations](deviations.md) as a known limit rather than done badly here.

**Rejected: dropping unauthenticated peers only at ownership time.** Filtering
the member set where webhooks read it would fix the symptom found and leave the
gauge, failure detection and any future consumer of `Members` still trusting an
unauthenticated peer. The member set should not contain strangers in the first
place.

**This is a stop-the-cluster upgrade, the third in M8's lineage.** A tagged
datagram is not a valid untagged one and vice versa, so old and new nodes
cannot gossip. Same rollout as [ADR-040](decisions.md) and
[ADR-051](decisions.md), recorded beside them in [Operations](operations.md).

---

## ADR-054 — Two vectors: what a node can serve, and what it has seen

**Decision.** A node keeps a second, durable version vector — **witnessed** —
recording the newest stamp it has *processed* per origin, whether or not that
processing appended anything. The oplog-derived vector keeps its meaning
(**what I can serve**) and is still what a peer receives from `AskVersions`.
`behind` and `lag_behind_ms` switch to the witnessed vector.

**One root cause, two symptoms, both found by driving a cluster.** The vector
advances only in `append_oplog`, and two situations leave a permanent hole:

- **A document that loses last-writer-wins.** `apply_remote` aborts the
  transaction and returns `false`, so nothing is appended and the origin's
  entry never moves past that stamp. *(Replicated DDL is **not** in this list:
  `apply_ddl` deliberately appends the originating entry afterwards, precisely
  to advance the vector. An early draft of this ADR said otherwise; reading the
  code corrected it.)*
- **An entry the sender holds but never ships.** A `UniqueViolation` is logged
  locally — it has to be an oplog entry to reach change streams — so it is in
  the sender's advertised vector, but `entries_for_peer` excludes it by design
  ([ADR-029](decisions.md)) because every node observes the same collision
  independently. A receiver therefore *cannot* cover that stamp by receiving
  it, no matter how many times it asks.

**The universal trigger is the bootstrap user.** Every node creates its own
`__kimmy.__users` collection and inserts `root` locally, with its own stamp. So
in every cluster, each peer's `root` insert arrives, loses last-writer-wins
against the local one, and leaves a hole — which is why a bare cluster with no
user data at all still re-syncs forever.

**Symptom one: every cluster re-syncs forever.** `behind` returns the earliest
point this node appears to lack, so a hole means every round re-requests
everything from that point, re-applies it, appends nothing, and leaves the hole
exactly where it was. Measured on an **idle, fully converged** three-node
cluster: **60 merge rounds in 20 seconds** with no user data at all, and 40 a
second with one collection, indefinitely. The re-sent range grows with
everything written after the hole, so the waste grows with the database rather
than staying constant. After the fix the same clusters report **zero**.

**Symptom two: the lag gauge sticks.** After concurrent updates to one
document, `kimmy_replication_lag_seconds` pinned at 1204 on all five nodes of a
converged, idle cluster and stayed there — clearing only when a *winning* write
from each origin finally advanced the vector past the discarded stamps. This is
the symptom that was visible, and the smaller half of the problem. The gauge is
documented as "0 is the caught-up steady state" and operators are told to alert
on it ([ADR-046](decisions.md), [Operations](operations.md)), so a permanently
non-zero reading is both a false alarm and a real one nobody could distinguish.

Symptom two only appears once an origin has *some* recorded stamp, because
`lag_behind_ms` deliberately ignores an origin it has never seen — the
"fifty-year lie" rule. That is why the busy loop ran in every cluster while the
gauge read zero.

**Why two vectors rather than making the one vector mean "seen".** What a node
can *serve* and what it has *seen* are genuinely different facts, and the sync
protocol needs both:

- The vector a peer receives must stay **servable**. If a node advertised
  coverage it could not serve, a peer would stop asking for entries nobody
  would then send it.
- The vector used to decide **what to ask for** must be *seen*, or the node
  re-asks for what it has already processed — which is the bug.

Pairing them this way leaves the wire format and `AskVersions` untouched: a
node still answers with what it holds, and only its own local comparison
changes.

**Witnessing happens in one place.** `apply_batch` records each entry's stamp
after processing it, on every branch — applied, superseded, DDL, skipped. The
per-entry logic moved into `apply_one` so no branch *can* forget: a `continue`
that skipped the bookkeeping is how this class of hole appears. An entry that
fails is not witnessed, because the batch aborts and it has not been processed.

**And a short batch witnesses the whole of the peer's vector.** Per-entry
witnessing cannot close the second hole, because the receiver never receives
those entries. But a batch shorter than the limit means the peer sent
everything it is willing to send from that point, so the receiver has now seen
all of what the peer advertised — including what it will never ship. A *full*
batch was truncated and gets no such credit.

**Witnessed is always at least servable.** `append_oplog` raises both, so the
invariant holds by construction and a node cannot claim to have served
something it has not seen.

**Existing databases start with witnessed = servable.** A lower bound, which is
safe: the first sync round re-fetches once, witnesses what it processes, and
goes quiet. No migration step and no schema bump — the table is created on open
like the others.

**Rejected: appending replicated DDL to the oplog.** It would advance the
vector as a side effect and let a node serve another's schema changes
transitively, which is arguably better. But it contradicts a standing invariant
for a reason that still holds, it changes what change-stream subscribers see,
and it grows every node's log by the whole cluster's DDL. A larger design than
the bug requires.

**Rejected: a per-peer high-water mark instead of a vector.** It would quiet
the loop, since the node would remember it had already asked. It would not fix
the gauge, which is a statement about origins rather than peers, and it adds
state that grows with cluster churn.

---

## ADR-055 — The client protocol is HTTP/JSON and WebSocket, said out loud

**Decision.** KimmyDB's client protocol is the HTTP/JSON and WebSocket API it
already serves, promoted from an implementation detail to a **specified,
versioned contract** with first-party clients for Rust, Python and Go. The
MongoDB wire protocol, gRPC and GraphQL are all rejected as client-facing
protocols.

This closes the client story deferred on 2026-08-11 ([Deviations](deviations.md))
and extends [ADR-011](#adr-011--httpjson--websocket-not-the-mongodb-wire-protocol),
which chose HTTP/JSON as a *server* decision and never said what a client could
rely on.

> **Corrected 2026-08-13.** The envelope below was written as
> `{status, tag, message}`. The wire carries
> `{"error": "<code>", "message": "…"}` — the status is the HTTP status and the
> field is `error`, not `tag`. Three documents and this ADR all described it
> the same wrong way, which is the point ADR-055 was making about prose nothing
> checks, made against ADR-055. `docs/openapi.yaml` is now the authority, and
> [ADR-056](#adr-056--the-protocol-specification-is-hand-written-and-a-contract-test-keeps-it-true)
> is what keeps it true.

**The protocol already existed; nothing had written it down.** Framing is HTTP,
encoding is Extended JSON v2 (`kimmy-api/src/lib.rs`), authentication is a
bearer token from `/v1/auth/login`, errors are `{status, tag, message}`,
streaming is a WebSocket carrying resume tokens that are portable across nodes,
and `kimmy-cli` is a working 464-line client built on it. Every question a wire
protocol has to answer already has an answer here. What was missing was that
**nothing specified, versioned or tested the protocol in use** — the same shape
of defect as the native-dependency claim in ADR-016, which "lived in prose that
nothing checked" and was quietly false for two milestones. The register framed
this as "there is no wire protocol", which conceded a premise that was never
true. There is one. It was undocumented.

**Rejected: the MongoDB wire protocol.** Three of its prerequisites — cursors,
`findAndModify`, computed expressions — are M9 tasks that are needed either
way, so they are not the argument. What is unique to it: an OP_MSG/OP_QUERY
codec including payload-type-1 document sequences, SASL/SCRAM, `hello` and a
wire-version claim, Mongo's numeric error codes, and `(lsid, txnNumber)`
deduplication without which a driver's automatic retry silently double-applies
a write. Beyond size, three things make it wrong rather than merely expensive:

- **SCRAM cannot be derived from an Argon2 hash.** It needs PBKDF2-HMAC-SHA-256
  material from the plaintext at set time, so every account would need a second
  stored credential obtainable only at the next password change — or `PLAIN`
  over TLS, which is not the default any driver reaches for.
- **Driver authentication is connection-scoped**, and a connection lives for
  hours. ADR-052's revocation is a token check on a request; there is no token
  to expire on a held connection.
- **Mongo's write commands partially succeed by design** — `ordered` batches
  report `writeErrors` per document — which contradicts ADR-048's standing
  invariant that a bulk insert is one transaction and a failure anywhere aborts
  all of it.

**And the deciding reason is topology.** `hello` forces a claim. Presented as a
*standalone*, drivers disable change streams — so KimmyDB's best feature
becomes unreachable through the very drivers the shim exists to serve.
Presented as a *replica set*, drivers run SDAM, pick the one node advertising
`isWritablePrimary`, and funnel every write to it. **KimmyDB is leaderless and
every node accepts writes.** The client model cannot express the architecture;
adopting it would mean misrepresenting the cluster to every driver. Vector
search, hybrid search, webhooks, schema inference and MCP have no
wire-protocol representation at all, so the HTTP API would remain forever as a
second, unequal surface.

**Rejected: gRPC.** ADR-011 already listed and rejected it; two of its costs
have sharpened since, and one argument for it has been measured away.

- **Protobuf is schema-first and this store is schemaless.** An arbitrary
  document can be `google.protobuf.Struct`, which cannot express ObjectId,
  Decimal128, Binary, DateTime or Int64-versus-Double — reintroducing precisely
  the fidelity problem Extended JSON already solves — or `bytes` carrying raw
  BSON, at which point protobuf's type system contributes nothing and gRPC is
  framing plus code generation.
- **Code generation is available from OpenAPI**, which M10 task 1 produces
  anyway, so that half is not exclusive to gRPC.
- **The efficiency argument is answered by [Benchmarks](benchmarks.md).** The
  durable commit is the cost, not the encoding — the marginal document runs
  ~13 µs against a several-millisecond commit, roughly 260:1 — and `/bulk`
  already converted that into 291 → 51,320 docs/sec. Encoding was never the
  bottleneck a binary protocol would relieve.
- **Its build cost lands on documented discipline.** `prost-build` wants a
  `protoc` binary, and `tonic`'s TLS feature selects a rustls provider — the
  aws-lc-rs trap already met twice, in ADR-016/ADR-039 and again in ADR-050.
  `./scripts/check-native-deps.sh` exists to make exactly this visible.
- **Browsers cannot speak it**, so the HTTP API stays regardless. It is purely
  additive surface.

**Rejected: GraphQL.** Never previously evaluated; recorded here so the absence
is a decision rather than an oversight.

- **Its value is the typed selection set.** Against arbitrary documents the
  body becomes a `JSON` scalar and GraphQL degenerates into "post a query
  string, receive JSON" — which `/find` already is, with less machinery.
- **Generating a schema from `schema.rs` inference is the tempting move and is
  a trap.** That module states plainly that it is "inference, not a schema.
  Nothing here is enforced." A schema derived from a sample makes perfectly
  valid documents unqueryable whenever a field falls outside it — a silent,
  data-dependent failure that gets worse as a collection grows more
  heterogeneous. It would convert a deliberate design property into an
  accidental constraint.
- **Nested selection is an unbounded cost surface**, so a GraphQL server owes
  depth limiting and complexity scoring. Rate limiting here covers login only.
- **The precedent is unanimous.** Hasura and PostGraphile are services *in
  front of* a database, not features of one. If GraphQL is ever wanted it
  belongs on top of the HTTP API, outside `kimmyd`.

**Why the count matters more than any single comparison.** Protocols multiply
against features. The surface is CRUD, aggregate, bulk, indexes, change
streams, webhooks, vector search, hybrid search, schema inference and MCP —
every additional protocol must carry all of it or leave part of it
second-class, and this is a one-maintainer project with one branch per task.
The question was never "is gRPC good"; it is "what does a second and third
protocol cost on every feature after it".

**What the decision costs, stated plainly.** No existing MongoDB tooling —
Compass, `mongosh`, existing drivers. That is the same cost ADR-011 accepted,
now paid deliberately with the alternative examined rather than assumed.
Adoption starts from zero: nobody can point an existing application at
KimmyDB until a client exists for their language. And three clients are three
maintenance streams that will drift, which is why a conformance suite is a
task in M10 rather than a nicety.

**Left open on purpose.** A narrow, read-mostly wire shim presenting as a
*standalone* server — enough for Compass and `mongosh` to inspect a database,
without claiming replica-set topology and inheriting retryable writes,
`$clusterTime` and primary election — is **not** rejected here. It is a much
smaller and different decision, it does not touch the write path, and it should
be made after M10 from experience with real clients. Sharding stays deferred on
its original terms.

---

## ADR-056 — The protocol specification is hand-written, and a contract test keeps it true

**Decision.** `docs/openapi.yaml` is an OpenAPI 3.1 document written by hand,
not generated from the route handlers. What makes it a contract rather than
prose is `crates/kimmy-api/tests/openapi.rs`, which fails when the router and
the document disagree — on which operations exist, and on what a response
actually contains.

This carries out M10 task 1 of [ADR-055](#adr-055--the-client-protocol-is-httpjson-and-websocket-said-out-loud).

**The alternative was generation** — `utoipa` or `aide`, annotating each
handler so the specification falls out of the build. Rejected for a reason
specific to this codebase rather than a general preference:

- **Thirty-eight of thirty-nine handlers return `Json<Value>`.** There are no
  response types to derive from. Generation would supply paths, path
  parameters and the thirteen typed request bodies; every response schema
  would still be hand-written, inside a macro attribute, next to the handler
  instead of in a document anyone can read. The half it automates is the half
  a text scan already checks, and the half it cannot automate is the half that
  drifts.
- **A generated document cannot disagree with the code, which is exactly what
  makes it weaker here.** It would have recorded that `PUT /docs/{id}` returns
  `{"matched": true}` and been *correct*, and the inconsistency below would
  have survived into three client libraries as a documented feature. The
  value of writing the specification by hand is that writing down what a route
  ought to return is what makes it visible that it does not.
- **It adds a dependency to the shipped crate** for something the server never
  reads. Both test dependencies here are dev-only; the daemon does not carry
  its own specification.

**Two conditions make this safe rather than optimistic**, because a
hand-written document that nothing checks is precisely the ADR-016 failure
this project has already paid for once:

1. **Inventory, both directions.** Every registered route is described, and
   every described operation is registered.
2. **Behaviour, against a running server.** Every documented operation is
   driven over a real socket and its response validated against the declared
   schema, and the test ends by asserting that *every* documented operation
   was exercised. A specification entry nothing executes cannot be added.

**What it found immediately**, which is the argument for the second condition:

- `GET /v1/users` returns an array of **names**; the first draft of the
  document said user objects, from reading the handler rather than the store.
- **`PUT /docs/{id}` returned `matched` and `modified` as booleans** while
  `/update` and `/find_and_modify` return them as counts — one field name,
  two types, on one protocol. `WriteOutcome` is three bools and the route
  serialized them straight through. No document had ever stated the type, so
  nothing disagreed; `docs/handoff.md` described it as `{"matched": 0}`, which
  was wrong in a way no test could contradict. The route had **no integration
  test at all**. Normalized to counts by decision, before a client existed to
  break — `upserted` stays a boolean because it genuinely is one.
- The inventory check that had lived in `routes.rs` since M8 matched
  `.route("` at the start of a line, so it **silently skipped the three
  registrations rustfmt breaks across lines** — including `/docs/{id}`. It had
  been passing while never checking the busiest route on the API.

**`/mcp` is deliberately not in the document.** It is a different protocol,
specified by MCP itself, mounted conditionally on `mcp.enabled` — describing it
here would put a second and weaker description of it in the repository.
[MCP](mcp.md) is its reference.

**The cost, stated plainly.** The document is maintained by hand, so every
later task that changes the wire must edit it, and the coverage assertion means
every new route needs a scenario in the test before it can be merged. That is
the intended cost: it is the mechanism, not friction around it.

---

## ADR-057 — The error taxonomy is closed, and retryability is three-valued

**Decision.** The error code set is a Rust enum, `kimmy_api::error::ErrorCode`,
and every code carries a **retry class**: `no`, `wait`, or `elsewhere`. The
class travels in the error envelope beside the code, and
`components.schemas.ErrorCode` in `docs/openapi.yaml` documents the whole set,
checked against the enum by the contract test from
[ADR-056](#adr-056--the-protocol-specification-is-hand-written-and-a-contract-test-keeps-it-true).

This is M10 task 2 of [ADR-055](#adr-055--the-client-protocol-is-httpjson-and-websocket-said-out-loud).

**Three-valued rather than a boolean, and the reason is the architecture.**
KimmyDB is leaderless: every node accepts writes, and every node holds a full
copy. So "ask a different node" is a genuine answer to a failure, and it is the
*correct* answer for a failure that belongs to the node rather than to the
request — a storage error on this disk, a missing API key in this node's
environment, an unusable snapshot in this node's directory. A boolean
`retryable` collapses that: a client told `internal` is retryable retries the
one machine that just failed it, which is the worst available choice. A client
told it is not retryable gives up while N−1 nodes could have answered.

The classes divide as follows, and the division is a claim about *what action
could possibly work*, not about how likely it is:

- **`no`** — twelve codes. The request must change, or the state it asks about
  must. `duplicate_key` does not become un-duplicate; `resume_token_expired`
  cannot be un-collected, and a client that retries the same token loops
  forever.
- **`wait`** — `rate_limited`, which says how long in `Retry-After`, and
  `provider_error`, which does not. Both are the same node later: every node
  calls the same embedding provider, so moving accomplishes nothing.
- **`elsewhere`** — `internal`, `misconfigured`, `snapshot`. Node-local
  conditions that replication makes recoverable.

`not_implemented` takes the conservative answer, `no`, though it has two
sources that disagree: `CoreError::Unsupported` is a capability that exists
nowhere, while a node built without `local-embeddings` would be recoverable on
a peer that has it. Optimizing for the second means sending every client around
the whole cluster for an answer that will not change, to serve a cluster
assembled inconsistently. One code cannot carry two classes; the common case
wins and the nuance is documented rather than encoded.

**The class is on the wire, not only in the document.** A client that acts on
`retry` handles a code released after it was written; a client that acts on a
table it compiled at release time does not. That is precisely what has to be
true for "adding an error code" to be an additive change under task 3's
compatibility policy — so the field is what makes the promise keepable.

**Closed by the compiler.** `ErrorCode` is an enum, and both the wire string
and the retry class come from exhaustive matches, so a new variant does not
build until both are answered. The second is the one that matters: it is a
decision about client behaviour that would otherwise be made by whoever was
adding an unrelated feature, silently, by picking a string.

**The alternative was a string constant plus a test that scans for
`ApiError::new` literals.** Rejected because it is a text scan over source, and
task 1 had just found a text scan that had been green for two milestones while
never looking at the busiest route on the API. A code assembled with `format!`
or produced through a helper would be invisible to it.

**The envelope has to reach every refusal, which turned out not to be true.**
Axum rejects a malformed or wrong-shaped body with bare text and no code.
`From<JsonRejection> for ApiError` has existed since M5 to fix that, but a
handler only reaches it by taking `Result<Json<T>, JsonRejection>`, and exactly
one of nineteen did — so eighteen routes answered `422 text/plain`, outside the
taxonomy entirely. The mapping now lives in an extractor, `json::JsonBody<T>`,
which cannot be used without it; the same applies to the WebSocket upgrade
rejection on `/watch`, which was the one refusal on the API that carried no
code at all. A taxonomy that sixteen routes bypass is not public surface, and
neither defect was visible from the code — both were found by driving a node,
because every test that exercised a wrong-shaped body used the one route that
had it right.

**The set had already drifted, which is the argument for all of this.**
`no_vectors` — a 409 returned when searching a collection whose vectors were
never ingested — existed in `vectors.rs` and appeared in neither
`docs/http-api.md` nor the first draft of `docs/openapi.yaml`. Both lists were
assembled by reading `error.rs`, and the codes accrete across five modules. The
same pass found 422 documented in the HTTP reference and specified nowhere: a
body that is valid JSON of the wrong shape is a different failure from a body
that is not JSON, and seventeen operations can return it.

**Cost.** The envelope gained a field, which is additive but is still a wire
change; `ApiError.code` changed type, which touched six construction sites and
four assertions and nothing else, because the sixty-one other sites go through
named constructors. And every future error must be classified — deliberately,
since that is the mechanism rather than an overhead beside it.

---

## ADR-058 — `/v1` does not break, and a node says what it can do

**Decision.** The path carries the major version and **`/v1` never breaks**.
Additive changes ship in it without ceremony; anything that would break a
correct `/v1` client mints `/v2`, served alongside `/v1` for at least one minor
release line and no less than six months. There is no version header, no
per-request pinning, and no compatibility shim layer. `GET /v1/version` reports
the protocol version, the build, the node that answered, and a **capability
list**. [Compatibility](compatibility.md) is the policy in full; this records
why it has that shape.

M10 task 3 of [ADR-055](#adr-055--the-client-protocol-is-httpjson-and-websocket-said-out-loud).

**Rejected: date-versioned requests, Stripe style.** A client pins
`Kimmy-Version: 2026-08-13` and the server keeps a transformation shim per
released version. It is the strongest compatibility story available and it lets
the wire evolve freely — and it costs a shim layer that grows without bound and
has to be *exercised* for every version it claims to support, or it is a
promise nothing checks, which is the failure mode this milestone exists to end.
Stripe can afford that; a one-maintainer project taking it on before shipping a
single client would be buying a permanent tax against a hypothetical.

**Rejected: additive-only forever, with no `/v2` ever.** Simplest to state and
easiest to keep honest, but it is a bet that no breaking change will ever be
worth making. When one is, the escape hatch would be an unplanned emergency
rather than a documented path. The current decision keeps the same discipline
in practice — `/v2` is meant to be rare — while writing down what happens if it
is not.

**The capability list is the load-bearing half, and it is not a version
number.** A client is handed one address and, once task 5 lands, will
round-robin across a cluster whose nodes are upgraded one at a time. So the
node answering the next request can be older than the one that answered the
last, and the question a client actually has is "does *this* node have the
feature I am about to use". A version number answers that only if the client
also carries a table mapping versions to features — which is exactly the table
`/v1/version` exists to replace, and exactly the table that goes stale in every
client independently.

`Capability` is an enum for the same reason `ErrorCode` is (ADR-057): the set
is public surface, and a hand-kept list drifts from what it describes. Twelve
of the thirteen are constant for a given build, which is not a reason to omit
them — a client asking may be talking to a node from before the feature
existed. The thirteenth, `local-embeddings`, varies *today*, between two builds
of the same version, which is what keeps the list a question rather than a
decoration.

**Unauthenticated, deliberately.** A client must be able to negotiate before it
holds a token — whether a refresh route exists is precisely what it wants to
know while logging in. `/readyz` already discloses the node id without
credentials and the build version is on every release artifact, so this adds no
fact an observer could not have. The rule it must not break is the one in
[Security](security.md): an unauthenticated endpoint must not leak the
*schema*. This names no database, collection or user.

**What is mechanism and what is prose.** The distinction is recorded because
this project has been wrong before about claims nothing checked. The contract
test enforces that every versioned route sits under `/v1/`, that the prefix
agrees with both the server's reported protocol and the specification's
`info.version`, that the served capability set is exactly the documented one
and that each capability is explained, and that **no response schema forbids
unknown properties** — which is what makes "a new response field is additive"
true rather than intended, since a validating client would otherwise break on
the next field added, silently, and only for them. The six-month window and
"changing what a route means is breaking" are prose: one is a promise about
calendar time, the other cannot be detected by anything that reads shapes.

**The contrast with the cluster wire is the point.** SWIM identities are
postcard-encoded and not self-describing, so a field change there breaks
membership outright — three stop-the-cluster upgrades are already documented
(ADR-040, ADR-051, ADR-053). Two wires, two opposite properties, each right for
its side: the internal one is optimized for size and changed under an outage
window that the operator schedules; the client one is optimized for never
needing one.

---

## ADR-059 — Refresh is sliding re-issue of the access token, not a second credential

**Decision.** `POST /v1/auth/refresh` takes a valid token in the ordinary
`Authorization` header and returns a fresh one. There is no refresh token, no
stored session, and no grace for an expired token. Login and refresh both
report `expiresIn`, so a client schedules its own renewal without decoding
anything.

M10 task 4 of [ADR-055](#adr-055--the-client-protocol-is-httpjson-and-websocket-said-out-loud).

**The security half was the real work, and it is a property of the route's
shape rather than of code inside it.** Refresh takes the `Auth` extractor, so
the presented token goes through exactly the check every other route applies:
signature, expiry, and the storage read that makes deletion, disabling and a
token-version bump take effect immediately ([ADR-052](#adr-052--token-revocation-is-a-per-user-version-checked-against-a-cache-the-oplog-keeps-honest)).
A token refused there never reaches the handler. **Refresh cannot launder a
revoked session**, and it cannot do so by construction rather than by
remembering to check — which matters, because the checking version of this is
one forgotten line away from being an indefinite session-laundering endpoint.

The new token is built from a **fresh read of the user record**, not from the
old token's claims. Today that cannot differ — any change to grants bumps the
version, so the extractor would already have refused — but a route that
carries authority forward must read the authority, not copy it.

**Rejected: a separate refresh token.** It buys the offline case: an
application idle overnight resumes without stored credentials. It costs two
credentials to store and two lifetimes to reason about, and its apparent
advantage does not survive contact with this design — the only revocation
available is bumping `token_version`, which invalidates *every* token that user
holds, so a refresh token is not separately revocable and the granularity it
seems to offer is not there.

**Rejected: stored, rotating refresh tokens with reuse detection.** The
strongest story on paper, and it fits this architecture badly. Rotation is a
compare-and-set on a replicated record, which a leaderless store does not
offer: a client that rotates against one node and then reaches another
mid-replication can be told its live credential is unknown, and two concurrent
rotations resolve by last-writer-wins — discarding a token a client is holding.
Every failure mode is a client logged out for a reason it cannot see.

**Rejected: a grace window for refreshing a recently expired token.** It would
soften the idle case without a second credential, and it would make `exp` mean
two different things depending on which route reads it. One of those two
readers gets it wrong eventually, and the reader that matters is the security
one.

**What this deliberately does not fix.** The old token keeps working until it
expires; a stateless token cannot be recalled. Ending a session early is what
the version bump is for. And a client idle longer than `token_ttl_secs` logs in
again — which is a thing a library may ask of an application, where re-sending
credentials every hour is not. That distinction is the whole point of the task.

**Not rate-limited.** The login limiter bounds Argon2 work
([ADR-038](#adr-038--login-is-rate-limited-before-the-password-is-checked));
refresh verifies a signature and reads one cached record, so a limit there
would defend nothing and would throttle the healthy case this route exists to
serve.

## ADR-060 — Client topology comes from a replicated registry, liveness from SWIM

**Decision.** `GET /v1/topology` lists the cluster's nodes. **Addresses** come
from a replicated registry — each node writes one record naming itself and the
endpoint it advertises into `__kimmy.__nodes`. **Liveness** comes from SWIM
membership. Entries nobody can vouch for are reported as `unknown` rather than
hidden, the answering node is always listed and always first, and the route
requires a token.

M10 task 5 of [ADR-055](#adr-055--the-client-protocol-is-httpjson-and-websocket-said-out-loud).

**Reading addresses out of SWIM is not available, and that is the whole
decision.** `Member` is `{addr, incarnation, node}` where `addr` is the
*gossip* address — there is no client-facing address anywhere in membership.
Putting one there means adding a field to an identity encoded with postcard,
which is not self-describing: a new node rejects an old identity outright, so
it is a stop-the-cluster upgrade and a note in
[Operations](operations.md). That would be the fourth in three milestones
([ADR-040](#adr-040--replication-tls-is-bound-to-cluster_secret-not-to-certificates), [ADR-051](#adr-051--webhook-ownership-hashes-node-ids-and-the-identity-carries-one),
[ADR-053](#adr-053--swim-datagrams-are-authenticated-with-the-cluster-secret)), and it would put
client-facing configuration inside the cluster's internal wire, where every
future change to it costs an outage window.

Inferring the client address from the gossip address — same host, different
port — is the tempting shortcut and is a guess: the two listeners can be on
different interfaces, one may terminate TLS, and in a container the address
clients reach frequently belongs to a service or an ingress rather than to the
process at all.

**So the registry is an ordinary replicated collection**, for the same reasons
the webhook registry is one ([ADR-045](#adr-045--webhook-delivery-is-owned-by-a-derived-node-over-replicated-progress)):
it replicates with no second transport, it is in a backup, it comes back on a
restore, and reading it is reading a collection. A node learns about peers it
was never seeded with, because the record arrives the way every other write
does.

**It is an address book, not a heartbeat.** A node writes its record at startup
and only when the content would differ. A periodic rewrite would append to the
oplog every tick, on every node, forever, for a fact that changes at restart —
the failure mode the webhook dispatcher's progress heartbeat exists to bound.
Freshness belongs to liveness, and SWIM is a better source for it than a
timestamp.

**`status` is `live` or `unknown`, never `down`.** A node whose gossip is
partitioned while its HTTP is perfectly reachable is a real state in a
leaderless cluster, and hiding it would remove an option exactly when a client
most wants one. `unknown` says what is true: this node has not heard from it.
Trying it anyway costs one round trip, and `retry: elsewhere` already describes
the outcome.

**Two inherited traps, both named because both have cost an outage before.**
`Members` holds **peers only** — it can never contain this node — so the
answering node is added explicitly rather than looked for; a set derived from
membership alone would tell a client the cluster does not include the node that
just answered, which is the shape of the bug that silently undelivered every
clustered webhook. And the member set must contain **only authenticated
peers** (ADR-053): that invariant now protects more than ownership, because an
unauthenticated peer in the set would be advertised to clients as a node to
send credentials to.

**Authenticated, unlike `/v1/version`.** A version is a fact about software; a
topology is a map of where a deployment's data lives. A client that wants to
fail over already holds a token, so nothing needs it earlier.

**`server.advertise` is configuration and cannot be inferred.** A node bound to
a wildcard has no single address, so one with nothing configured advertises
nothing and says so at startup rather than publishing a guess to every client
in the cluster. With a concrete bind it defaults to that address, with the
scheme following whether the node terminates TLS.

**What is not solved.** A record outlives the node that wrote it: a
decommissioned node stays listed as `unknown` forever. Age-based collection
would be wrong — records are not rewritten, so age measures uptime — so removal
is a deliberate act, and the registry being an ordinary collection is what makes
it possible without new API surface. Recorded as carried debt rather than left
to be discovered.

## ADR-061 — A reachability probe counts unreachable points, not missed searches

**Decision.** The build-time check that a finished HNSW graph can find its own
data splits a failed probe into two outcomes and only acts on one. A sampled
point that does not return itself at ordinary search effort is re-probed with a
much larger `ef` and a `k` above one. Points recovered by the second look are
**search budget**, not data loss, and are ignored. Only points still missing
when effort stops being the limit count toward the rebuild threshold.

Supersedes the single miss counter added with the check itself.

**The counter it replaces could not hold at more than one collection size.** It
added the two outcomes together, and they scale differently. An ordinary search
explores a fixed amount, so as a collection grows the same budget covers less of
it and a perfectly reachable point starts being missed for want of looking —
that count climbs with size and width. Genuine orphaning does not. Summed, the
threshold had to be set for one fixture and was: 400 vectors at 16 dimensions.

At 384 dimensions — the realistic embedding width, and the one the recall tests
already single out — the sum climbs steeply with collection size:

| vectors (at 384 dimensions) | missed at ordinary effort | still unreachable |
|---:|---:|---:|
| 500 | median 0 | median 0 |
| 1,000 | median 1 | median 1 |
| 2,000 | median 3 | median 2 |
| 4,000 | **median 8** | median 3 |

Against a threshold of three, **seven builds in ten at 4,000 vectors were
discarded, rebuilt twice, and then reported to the operator as losing data** —
three times the build cost and a false alarm, on a healthy graph. The
re-probe removes the growth; the threshold covers what is left.

**Both sides of the threshold are measured, which is the part that was missing
before.** The distribution of healthy builds says nothing about the distance to
a bad one, and a threshold is a claim about that distance. Over 600 builds at
the size where the catastrophic failure is known to occur, checking every point
rather than a sample for ground truth:

| | true orphaned | sampled score |
|---|---|---|
| 599 healthy builds | 0.8%–3.0% | 0–5, median 1 |
| the one catastrophic build | 14.8% | **22** |

Eight sits above every healthy observation across 725 builds spanning two
widths and five collection sizes, and far below the only bad one.

**A correction worth keeping.** The replaced comment explained a healthy miss as
a vector among near-duplicates being edged out by a neighbour — approximation
working as designed. That was assumed and it is false. Re-probed with the budget
removed, those points are still not found: they are unreachable, not edged out.
**Every graph this builds orphans a little**, between 0.8% and 3.0% of the
collection, and the check has always been separating routine orphaning from
catastrophic orphaning rather than noise from signal. Naming that correctly is
what makes the threshold a judgement about a measured gap instead of a guard
against a mechanism that does not exist.

**Alternatives.** Scaling the threshold with collection size keeps the summed
counter and fits another constant to measurements — but the growth depends on
width as well as count, so it is two fitted constants, and it needs the bad-side
distribution measured at every point on the surface to prove the margin holds.
A threshold as a fraction of the sample is scale-free in the right way but the
margin was already under 2× at 4,000 vectors and shrinking. Neither addresses
the actual defect, which is that two different events were being added together.

**What is not solved.** Routine orphaning of up to 3% of a collection is
accepted and not reported. It is a property of the graph parameters rather than
of a build, so rebuilding cannot fix it, and the honest place to change it is
`MAX_CONNECTIONS` or `EF_CONSTRUCTION` with recall measurements to hand.
Recorded so that the number is known rather than discovered.

---

## ADR-062 — Pre-1.0 SemVer: minors may break, patches never do, and there is one version

**Decision.** While the workspace is `0.x`, a **`0.MINOR` bump** carries
features and is the only release that may break anything — operator surface,
configuration, CLI flags, defaults, the cluster wire — and a **`0.x.PATCH`
bump** carries fixes only and must never require reading release notes. The
workspace version in the root `Cargo.toml` is the **single source of truth**:
`kimmyd` and `kimmy` are always released together at that number, releases are
driven by pushing a `v{MAJOR.MINOR.PATCH}` tag, and a test pins that neither
binary can drift from the workspace. The **protocol version stays independent**
— `/v1` in the path is a promise about the wire (ADR-058) and says nothing
about builds; nothing here weakens it.

[Compatibility](compatibility.md) carries the policy where users read it; this
records why it has this shape.

**Rejected: per-crate versions.** Cargo supports them and eleven crates could
each carry their own. But nothing here is published to crates.io (deferred past
0.x, deliberately — a registry release is a compatibility promise about
*library APIs*, and the only public surfaces today are the HTTP protocol and
two binaries), so per-crate numbers would version things nobody consumes at a
cost paid on every release: eleven bumps to reason about instead of one, and a
release that has to explain which crates moved. One number, moved in lockstep,
is the version story a two-binary project actually has.

**Rejected: pretending 0.x is 1.x.** Strict SemVer under 1.0 makes every
breaking change a *major* bump, which this project is not ready to spend —
the cluster wire alone has needed three stop-the-cluster changes in eleven
milestones (ADR-040, ADR-051, ADR-053). The `0.MINOR`-may-break convention is
the one the Rust ecosystem already reads correctly, and writing it down is
what turns a convention into a promise: a patch is always safe, a minor is a
release-notes event.

**Why the build version and the protocol version must not merge.** They answer
different questions on different clocks. "Does this node have the feature I am
about to use" is answered by capabilities, per node, during a rolling upgrade;
"which exact build is behaving oddly" is answered by `version` plus the commit
hash now baked into every build. Tying `/v1` to `1.0.0` would either freeze
the build number or break the wire promise every time the build number moved.

**Cost.** Two numbers to explain instead of one, and a pre-1.0 minor that
*may* break obliges the release notes to say clearly when it *does*. That
obligation is the point: the alternative was breakage nobody had promised to
announce.

---

## ADR-063 — cargo-dist builds the release; the container image is the server's channel

**Decision.** Releases are built by **`dist` (cargo-dist)**, configured in
`dist-workspace.toml`, which generates `.github/workflows/release.yml` — a
file nobody edits by hand. Pushing a `v*` tag builds `kimmyd` and `kimmy` for
four targets — macOS arm64 and x86_64, and Linux arm64 and x86_64 as **static
musl binaries on native runners** — attaches tarballs and SHA256 checksums to
a GitHub Release whose notes come from `CHANGELOG.md`, and publishes a
Homebrew formula for **`kimmy` only** to `titusai-io/homebrew-tap`. The
server's distribution channel is the **multi-arch container image** at
`ghcr.io/titusai-io/kimmydb`, built by `.github/workflows/publish-ghcr.yml`
as a custom dist publish job. CI stays the merge gate; the release workflow
trusts what merged.

**The spike that settled it.** The worry was an 11-crate workspace fighting a
tool built for single-binary projects. It did not: `dist init` found exactly
the two shipped binaries and nothing else, `installers = []` on `kimmyd`
scopes Homebrew to the CLI, `formula = "kimmy"` names the formula after the
binary rather than the crate, and mapping `aarch64-unknown-linux-musl` to
`ubuntu-24.04-arm` in `[dist.github-custom-runners]` gave native arm builds
with `musl-tools` installed automatically. The one thing dist does not do —
the container image — bolts on as a first-class `./publish-ghcr` publish job
with its own escalated permissions. Hand-rolling the same matrix would mean
owning tarball naming, checksum generation, release-note extraction and the
formula template forever, to reproduce what a config file already says.

**Rejected: a hand-rolled matrix workflow.** It was the fallback, and it
remains the escape hatch if dist is ever abandoned upstream — the cost of
leaving is one generated workflow and one config file, since the tarball
layout and checksum format are conventional. The reason it lost is the
Homebrew half: formula generation and tap publication are exactly the kind of
templated, twice-a-year-touched code that rots in a repository and stays
maintained in a tool.

**musl, verified rather than assumed.** The claim that redb and ring build
against musl on arm64 was tested empirically before being configured: the full
workspace compiles in an aarch64 Alpine container and both binaries link
statically and run. A static binary runs on any distribution and in a
`scratch` container, and it sidesteps the glibc-version matrix that gnu
builds inherit from whatever runner built them.

**QEMU rejected for the image build.** The Dockerfile compiles the workspace
in release mode; under QEMU emulation that is the better part of an hour per
architecture, natively it is minutes. GitHub's arm64 runners are free for
public repositories, so each architecture builds on its own hardware and a
final job merges the two digests into one manifest with the `X.Y.Z`, `X.Y`,
`latest` and commit tags. No bare `X` tag pre-1.0: a floating `0` would
promise a stability `0.x` does not have (ADR-062).

**Homebrew ships the CLI only.** A server under `brew services` is a second
init system to support and a data directory in a surprising place; the
container is the supported way to run `kimmyd`, and the release tarballs
exist for everything else. `brew install titusai-io/tap/kimmy` installs the
prebuilt macOS binaries with checksums the formula carries.

**Cost, and what is not solved.** A generated workflow means trusting a
generator: `release.yml` is 350 lines nobody here wrote, pinned to
`cargo-dist-version = "0.32.0"` and changed only by `dist generate` (the plan
job fails any PR where the two drift). The publish half cannot be proven
without publishing — dist runs a plan on every PR and builds artifacts on a
`pull_request` opt-in, but the tap push and the GHCR manifest need a real tag,
so the first release is preceded by a prerelease shakeout. And dist skips
publish jobs on prerelease tags by default, which is the safe default and
means the shakeout proves the build half only.

---

## ADR-064 — Two verifiers, routed by the issuer a token claims

**Decision.** A node may federate with **one** external OpenID Connect
provider. `TokenIssuer` (HS256, cluster secret, local users) is untouched; a
second verifier, `OidcVerifier`, checks RS256/ES256 signatures against the
provider's JWKS with issuer, audience and expiry validation. The `Auth`
extractor reads the **unverified** `iss` claim and sends the token to the
verifier it names — external issuer to the OIDC path, anything else to the
local one. Nothing downstream changes: `Principal::can`, RBAC, MCP, the audit
log and the search-without-read grant all treat a federated principal as a
principal. `kimmy-auth` still does no I/O; the JWKS is fetched by `kimmyd` and
injected.

**Why reading an unverified claim is safe.** It decides which verifier gets to
say yes, never whether the answer is yes. Both verifiers pin their own
algorithm list and their own key, so a forged `iss` routes a token to a
verifier that refuses it, and an omitted `iss` routes it to the local one,
which refuses it just as firmly. Critically, a token is offered to **exactly
one** verifier, which is what leaves no algorithm-confusion surface between
them — the classic attack needs one verifier willing to read the algorithm out
of the header, and neither is.

**Rejected: one verifier that tries both keys.** It is the shape that
introduces the bug. A verifier holding a shared secret *and* a public key,
choosing by `alg`, is one header edit away from verifying an RSA public key as
an HMAC secret — and the provider's public key is, by construction, public.
Two verifiers that never see each other's tokens cannot make that mistake.

**Rejected: replacing local users entirely.** An identity provider is a
dependency, and a database that cannot be administered while its IdP is down
is a database that cannot be recovered during the incident that took the IdP
down. Local accounts stay, and `admin` stays local-only (ADR-067).

**One issuer, this round.** A second issuer is a second trust root, and the
question it raises — which of them may assert that a subject is an analyst —
has an answer that depends on a deployment rather than on a default. Adding a
list later is additive; guessing now is not.

**Sixty seconds of leeway, on this path only.** The local path allows none:
nodes in one cluster are expected to agree about the time and are operated by
whoever operates the database. An external provider is somebody else's clock,
and a few seconds of NTP drift refusing freshly minted tokens looks exactly
like an outage in the IdP.

**Cost.** A second thing to configure, a second thing to be wrong about, and a
background task that can fail quietly — a node that cannot reach its provider
keeps verifying perfectly against the keys it already holds until the provider
rotates, and then refuses every federated caller at once. That failure is why
`kimmy_jwks_refresh_total{outcome="failed"}` exists and why `check-config`
does a live fetch. Boot deliberately does **not**: a briefly unreachable
provider must not stop a database from restarting.

---

## ADR-065 — Revocation is asymmetric, because a federated identity has no record here

**Decision.** Token-version revocation (ADR-052) applies to local users only.
`Sessions::check` early-returns for a federated principal, exactly as it does
for the `--insecure-no-auth` one. A federated session ends when the provider
says so: a short token lifetime, and the provider declining to mint the next
one. `/v1/auth/refresh` refuses a federated principal outright, and
`Principal::federated` is carried into the audit record beside
`unauthenticated`.

**Why the skip is required, not a shortcut.** The check reads the user's
record from `__users`, and **the absence of a record is how it refuses a
deleted account**. A federated subject has no record, so without the skip every
federated request would be refused as revoked. The subtler failure is the
other direction: if a local user happened to share the name the provider
asserts, that local account's version and `disabled` flag would silently decide
whether the federated caller may connect.

**Why refresh refuses rather than reissuing.** Minting a local HS256 token from
a federated principal would launder the identity — it would shed the origin
flag, outlive the provider's say in it, and make the audit log claim a local
user did the work. The client library's answer is `Builder::token_provider`:
the application refreshes with its own provider, and this database never holds
a refresh token.

**Rejected: mirroring federated users into `__users`.** It would restore
symmetry and a familiar revocation story, at the price of a second source of
truth for identity, a synchronisation problem nobody asked for, and a
provisioning question (what happens on first login?) with no good default. The
provider is authoritative about who exists; this database is authoritative
about what they may do.

**Cost, stated plainly.** There is no way to end one federated session from
here. Cutting someone off means doing it at the provider, and it takes effect
when their current token expires — which is the argument for short token
lifetimes, and is documented in [security.md](security.md) rather than left to
be discovered.

---

## ADR-066 — Role mappings live in the config file, not in the database

**Decision.** The mapping from an IdP claim value to a set of grants is written
inline in `kimmy.example.toml` under `[[auth.oidc.role_mappings]]`, read at
startup, and changed by editing the file and restarting. It is not a
collection, there is no endpoint that edits it, and it does not replicate.

**Why.** Verification stays a pure function of the token plus configuration —
the same property that makes the local path free (ADR-013). A mapping stored in
the database would put a lookup on the request path, would need a cache and an
invalidation story, and would make "what may a federated caller do" a question
whose answer can differ between two nodes mid-replication. It would also make
the mapping editable by anyone holding `admin`, which is precisely the
privilege ADR-067 keeps away from the IdP.

**Rejected: mapping to named roles that live in the database.** Tempting,
because it is how the local user store already works. It loses the property
above and adds an ordering problem: a role a mapping names but the database
does not hold yet is either an error at startup or a silent zero-grant
identity, and neither is good.

**A role that maps to nothing is a principal with no grants**, not a refusal.
It authenticated; it is simply not authorized here. Refusing at the door turns
"your administrator has not given you access to this database" into "your
login is broken", and the existing model already answers `false` to every
question a grant-less principal asks.

**Cost.** Changing who may do what is a restart and a config deploy, and in a
cluster it is a rolling one — during which two nodes can hold different
mappings. That window is real, and it is the same window every other
configuration change already has.

---

## ADR-067 — `admin` is not federatable

**Decision.** A role mapping whose grants name the `admin` action is **refused
at startup**, alongside the unknown-action refusal. Administration of KimmyDB —
creating and dropping collections, managing indexes, managing users, taking a
backup — is reachable only through a local account. The rule lives in
`OidcSettings::validate`, which both `Config::validate` and
`OidcVerifier::new` call, so `check-config` refuses exactly what the node
refuses.

**Why.** It is a break-glass boundary. Federation makes an external system a
dependency of authentication, and every other privilege is worth that trade —
a compromised or misconfigured provider that can mint a reader is bad, and a
compromised provider that can mint a superuser over the database is
unrecoverable from inside the database. Keeping `admin` local means the answer
to "the IdP has been taken over" is still "log in as root and turn federation
off", rather than "restore from backup".

**It is also a smaller mistake to make.** A group in a directory is a thing
somebody adds people to for reasons that have nothing to do with this database.
`kimmydb-analyst` mapping to read on `sales.orders*` is a decision an operator
made; the same directory group silently carrying `admin` because someone
copied a grant is a decision nobody made.

**Rejected: allowing it behind a flag.** A flag whose only purpose is to remove
a security boundary is a flag that gets set during an outage and never unset.
The narrower version — allowing `admin` scoped to one database — was rejected
too: it is still create and drop over that database, and the boundary that can
be explained in one sentence is the one that survives.

**Cost.** An organisation that wants every privilege in its directory cannot
have that, and administering a federated deployment means keeping at least one
local account and its password. That is the intended cost: the break-glass
account is the point, and one that exists only in the IdP is not break-glass.


---

## ADR-068 — Telemetry attribute privacy: names are off by default

**Decision.** `telemetry.include_names` defaults to **false**, and with it
false a span carries no database name, no collection name and no request path.
Span names come from `http.route` — the axum route *template*,
`/v1/db/{db}/coll/{coll}/docs` — and from `db.operation.name` (`find`,
`insert`, `aggregate`), neither of which can carry a name because neither is
built from one. `url.path`, `db.namespace` and `db.collection.name` are the
three attributes that can, and all three are behind the flag. The
`kimmy::audit` target is excluded from the OTLP layer **entirely**, at any
setting of the flag.

**Why.** The endpoint an operator points at is not the boundary the data
crosses. A collector is a fan-out: it forwards to a vendor, it is scraped by a
platform team, its retention is somebody else's policy, and traces are read by
people who were never granted anything in this database. `/metrics` has been
counts-and-never-names since M2 for exactly that reason, and the argument does
not weaken because the destination was configured rather than exposed.

**Private by construction, not by redaction.** The point of naming spans from
the route template is that there is nothing to redact: the template is a
compile-time string, so a span name cannot leak a name even if the gate is
wrong. Redaction has to be right every time; construction has to be right once.
The template is also what keeps span names low-cardinality — a trace backend
groups by name, and a name built from a URI is one group per document id.

**Spans are exported; log events are not.** `tracing-opentelemetry` turns
every event that happens inside a span into a **span event**, carrying that
event's own fields with it — and nothing in this codebase writes log lines with
telemetry in mind. `kimmy_storage` logs `db` and `collection` on every DDL
line, the dispatcher logs a webhook `url`, `kimmy-auth` logs `user`. This was
not reasoned out; it was found by reading what a live collector received, with
an earlier version of this decision in place: a `create_collection` span
arrived carrying `collection: "orders"` as an event field while every span
attribute was correctly empty and `include_names` was off.

Gating those field by field is not a fix — it is an audit of every `info!` in
the workspace, redone whenever anyone adds one, and the failure mode is a leak
nobody notices. Spans are the surface designed for this: bounded, reviewed,
named from route templates. Events are the surface nobody designed for it. So
the OTLP layer takes spans only, and logs stay logs.

**The audit exclusion is separate and unconditional.** Audit records are
events, so the span rule already covers them — but they are named in the filter
anyway, because the promise is about *identities* rather than about how one
layer happens to be filtered, and if auditing ever grew a span it must still be
refused. An audit record carries the *principal's* name alongside the
collection's, a second category of thing entirely, and `include_names` was
never meant to gate identities. Attaching the filter to the OTLP layer alone
also leaves an operator's `RUST_LOG=kimmy::audit=info` routing working exactly
as it did.

**Rejected: treating an operator-chosen collector as a different trust boundary
and exporting names unconditionally.** It is a defensible sentence and a bad
default. The person who sets `telemetry.endpoint` during an incident is not
the person who decided what the trace backend's retention is or who can read
it, and the mistake is unrecoverable: names already shipped cannot be
un-shipped. A flag that has to be turned *on* is one decision made once by
someone who thought about it; a flag that has to be turned *off* is a decision
nobody makes until after it has mattered.

**Cost.** With names off, a trace shows *that* a `find` was slow and not *what*
it was over, so an operator debugging one collection has to turn the flag on
and restart. That is the intended shape — the diagnosis that needs names is a
deliberate act — but it is a real cost during an incident, and it is why the
flag exists at all rather than the names being absent for good.

**And a second cost, from the events rule.** A trace shows the shape of a
request and not the log lines inside it, so correlating the two means matching
on time and on the node rather than clicking through from a span. Trace-to-log
correlation by trace id is the thing this gives up, and it is worth giving up:
it would be bought by exporting every field of every log line, which is the
leak above.

---

## ADR-069 — OTLP over HTTP, never gRPC

**Decision.** The OpenTelemetry exporters speak OTLP over HTTP —
`http/protobuf` or `http/json` — and the gRPC transport is not compiled in.
`opentelemetry-otlp` is taken with `default-features = false` and no
`grpc-tonic`, so `tonic` is not in the tree. `telemetry.protocol` accepts the
two HTTP names and refuses everything else with a message that says gRPC is
deliberate rather than missing.

**Why.** ADR-016's surviving rule is that the build pays for one native crypto
stack and must not acquire a second; the check that enforces it is
`scripts/check-native-deps.sh`. The HTTP exporter rides `reqwest` and
`rustls`/`ring`, both already in the tree for the remote embedding providers
and for TLS termination, so the allowlist is unchanged and the musl and arm64
cross-compiles are exactly as hard as they were. That was verified rather than
assumed: the native-dependency set is byte-identical to the one on `main`.

**And it costs nothing to reach.** The OpenTelemetry Collector's `:4318` HTTP
receiver is on in its default configuration, so "HTTP only" is not a
restriction an operator has to work around in the common case — it is the port
the thing already listens on.

**The client is the blocking one, and that is not an accident.**
`logging::init` runs before the tokio runtime is built, because configuration
is resolved and refused before a runtime exists so that a bad file is a
one-line error rather than a panic in a worker thread. An async exporter built
there would have no reactor. `reqwest-blocking-client` owns its own threads and
does not care, and exports are off the request path either way.

**Rejected: a TLS backend for the exporter.** `reqwest` 0.13 — which
`opentelemetry-http` uses, a different major version from the one this
workspace carries — offers TLS only together with `rustls-platform-verifier`,
which brings `security-framework-sys` on macOS. That is a new native crate for
a case a sidecar collector does not have, so `https://` endpoints are
**refused at startup** with a message naming the alternative, rather than
accepted and failing at every export into a log nobody reads.

**Cost.** A deployment whose collector exposes only the gRPC receiver on
`:4317`, or only TLS, needs a Collector in front of it — which is the component
whose entire job is to be in front of things. A collector across an untrusted
network needs one locally to forward over TLS.

---

## ADR-070 — Counters are bridged to OTLP, not duplicated

**Decision.** The OTLP meter provider registers **observable** instruments
whose callbacks read the same `AtomicU64`s that `/metrics` renders, through a
new `Metrics::snapshot`. `Metrics::render` is untouched, and the `/metrics`
body is byte-for-byte what it was — pinned by a golden test over the whole
render string, and by a second test asserting the exact ordered list of series
names in the HTTP response.

**Why.** The alternative is two sets of counters incremented at the same call
sites, which works right up until one of them is not. A `record_request` that
bumps the atomic and forgets the instrument is a Prometheus dashboard and a
trace backend disagreeing about how many requests a node served, with nothing
in either to say which is right — and the divergence is silent, because both
numbers look plausible. One source of truth per counter removes the question.

**Observable rather than synchronous, because of when the state exists.** The
counters live in the API state, which needs a database; the subscriber is
installed before the runtime, let alone the engine. An observable instrument is
registered later, from `node::run`, and reads at export time, so the ordering
falls out rather than being arranged.

**The golden test is the load-bearing half.** `/metrics` is scraped by a live
cluster, and the failure mode of changing it is a panel that goes blank and an
alert that stops firing — neither of which says anything when it happens.
`render` is fully deterministic in a test (`uptime_secs` is 0 on a fresh
instance), so there is no reason to check it loosely: the whole string is
compared against an inline literal. The route's body prepends engine gauges and
`kimmy_storage_bytes` is a file size, so that one is pinned structurally
instead — the ordered series names, which is what a scrape config names.

**Cost.** The OTLP instrument names are not the Prometheus ones. A counter
exported as `kimmy_requests_total` comes back out of a collector's Prometheus
exporter as `kimmy_requests_total_total`, so the OTLP names drop the suffix and
use dots (`kimmy.requests`). Anyone correlating the two surfaces has to know
that, and it is written down here because nothing about either name reveals it.


## ADR-071 — The audience is the resource identifier, and there is no second key for it

**Decision.** KimmyDB names itself as an OAuth 2.0 protected resource, and the
name is `auth.oidc.audience`. Written as an `https` URL it is the RFC 8707
resource identifier: the node publishes RFC 9728 metadata at
`/.well-known/oauth-protected-resource`, every 401 and 403 carries an RFC 6750
`WWW-Authenticate` challenge pointing at it, and `kimmy login` sends the
identifier as a `resource` parameter on both OAuth flows. Written as anything
else — `kimmydb`, Entra ID's `api://<guid>`, a `urn:` — the audience is opaque:
no metadata, no `resource` parameter, and behaviour identical to what shipped in
0.2.0.

**Alternatives.** A separate `resource_identifier` key beside `audience`, with
a startup refusal when the two disagreed. A `publish_metadata` boolean. Making
the resource identifier mandatory whenever OIDC is configured.

**Why.** The gap this closes is not cosmetic. The CLI could not send a
`resource` parameter at all, so the only audience it could obtain was whatever
the provider defaulted to — for a conformant server, its own issuer URL. That
made `audience = "<issuer>"` the only configuration that worked, which is one
audience shared by every resource the provider serves, which is precisely what
an audience restriction exists to prevent. The correct configuration existed on
paper and was unreachable from the tool.

The rejected alternative is the instructive one. Two keys that must always be
equal are one key, and the startup refusal invented to police them was the tell:
it would have been enforcing a rule the design created. Deriving the identifier
from the audience also makes the change **non-breaking** — every existing
configuration keeps its exact behaviour without being edited.

**Only `http://` is refused, and that is a deliberately narrow rule.** The
obvious reading of RFC 8707 §2 — "an absolute URI, so validate every audience
with a scheme" — would refuse `api://<guid>`, which is Entra ID's own default
audience for a registered application, and Entra is a named target provider.
Those values are absolute URIs, are not dereferenceable, and can never be
resource identifiers; they are perfectly good audiences. So a scheme alone does
not make an audience a resource identifier — `https` does — and the only refusals
are `http://` (an https identifier with the scheme mistyped, where a client
could be pointed at a substitute authorization server and send its credentials
there) and a fragment (forbidden by §2, and `aud` is matched byte for byte, so
it could only ever fail to match).

**No `scopes_supported` in the metadata.** Authorization here is roles carried
in the token. Advertising a scope vocabulary would describe an access-control
model this database does not implement, and a client that asked for those
scopes would receive them and still be refused.

**The challenge is a middleware, not part of `ApiError`.** A 401 arrives from
two unrelated places — the `Auth` extractor building one directly, and
`From<AuthError>` converting a verifier's refusal — and the conversion has no
access to state, so it cannot know the metadata URL. More importantly, the RFC
6750 §3 distinction between a request that offered **no** credentials (a bare
challenge, no `error` code) and one that offered a bad one (`invalid_token`)
depends on the *request*, which an error value has never seen. One layer that
already wraps every route answers both. `/v1/auth/login` is excluded: it is
where a token comes from, not a bearer-protected resource, and challenging
there would tell a client to come back with the thing it is asking for.

**Cost.** The identifier is no longer a free string — RFC 9728 §3 puts the
metadata at `<identifier>/.well-known/oauth-protected-resource`, so it has to be
the public base URL clients reach the node at, and it has to match the
provider's own registration byte for byte in two more places. An operator who
gets it wrong sees `invalid_target` from the provider or a refused token here.
Both are the system working, and neither is self-evident from the error alone,
which is why `kimmyd check-config` and the startup log both say which mode the
node is in.

The router also registers the well-known path as a **literal** rather than
building it from the shared constant, because the documentation contract in
`tests/openapi.rs` scans the source for route literals and a computed path is a
route that silently escapes it. A test holds the literal and the constant
together.

## ADR-072 — The verifier binds the metadata to the issuer, and `typ` is opt-in

**Decision.** Three hardening changes to the federated path, and one of them
deliberately ships turned off.

1. **`nbf` is validated.** A federated token whose "not before" is in the
   future is refused, under the same 60-second leeway `exp` already gets.
2. **A discovery document must name the issuer it was fetched for**, and the
   `jwks_uri` it names must be `https` — checked at *both* places that read a
   discovery document, the node's JWKS refresher and the CLI's login flows.
   Plain `http` to a loopback address is exempt.
3. **`typ: at+jwt` is checked only when `auth.oidc.require_at_jwt` is set**,
   which defaults to `false`.

**Alternatives.** Validating `nbf` without leeway. Requiring `at+jwt` by
default, as RFC 9068 §4 reads on its face. Checking the discovery issuer only
on the server, on the grounds that only the server picks signing keys.
Requiring `https` with no loopback exemption.

**Why.**

*`nbf`.* jsonwebtoken 11 defaults `validate_nbf` to `false`, so setting
`validate_exp` — which reads like "check the times" — left the front edge
unchecked, and a token stamped as valid from next week was accepted today. RFC
7519 §4.1.5 makes rejecting it a MUST. The claim stays optional, because it is
optional in the RFC and most providers omit it; making it required would have
refused ordinary tokens. The leeway is shared with `exp` for the reason
ADR-064 gives for having any leeway at all: the provider's clock is somebody
else's, and a fleet whose NTP drifts by seconds must not read as an outage.

*Binding the metadata.* OpenID Connect Discovery §4.3 and RFC 8414 §3.3 both
require the check, and it is the step that ties a document to the identity the
operator meant. Without it, anything that can answer for the well-known path —
a followed redirect, a stale cache, a hijacked record — chooses the `jwks_uri`,
and therefore the signing keys every federated token is verified against. The
per-token `iss` match does not cover this: an attacker who supplies the key set
satisfies that check too, because they are minting the tokens.

**Both call sites, not just the server.** The CLI reads a discovery document
for different reasons than the node does — it learns where to POST a **client
secret** and where to collect an **access token** — and a substituted document
nominates somewhere else for both. Neither check substitutes for the other. The
rule is implemented twice rather than shared, because `kimmy-cli` deliberately
links no kimmy crate that could carry it, for the reason recorded on
`PROTECTED_RESOURCE_METADATA_PATH`.

**Loopback is exempt from the `https` requirement.** The same exemption
RFC 8252 §7.3 makes for native applications and browsers make for secure
contexts, resting on the same fact: there is no network path to be on between a
process and itself. Without it a locally-run provider would be undevelopable
against and a stub untestable — which is a reliable way to have a security check
deleted later by somebody who only ever sees it in the way. The host is parsed
rather than prefix-matched, so `http://127.0.0.1.attacker.example` and
`http://127.0.0.1@attacker.example` are both refused.

*`typ` is opt-in, and this is the uncomfortable one.* RFC 9068 §4 requires an
access token to carry `typ: at+jwt`, and the check exists so an **ID token**
from the same issuer cannot be presented as an access token. It cannot default
to strict: **Entra ID stamps `typ: JWT` on its v2 access tokens**, and Entra is
a named target provider throughout this design, so a strict default would
refuse every token from a canonical deployment. This is the same shape as
ADR-071's narrowing — a specification applied literally would break a provider
the feature exists to support.

Shipping it off is defensible because **ADR-071 already closed the confusion it
guards against** for anyone who took that route: an ID token's `aud` is the
client id, which cannot also be the node's `https://…` resource identifier, so
an audience written as a URL refuses an ID token on the audience alone. The
switch is therefore defence in depth for those operators and the real check for
anyone whose provider mints an opaque audience.

**Cost.** `require_at_jwt` is a setting whose correct value depends on the
provider, which is a question an operator should not have to hold — the
alternative was refusing Entra outright, and the field documents which case is
which. The discovery binding turns a class of provider misconfiguration
(metadata naming an issuer that differs by a trailing slash) from silent into a
startup-visible failure; that is the intent, but it will be met as a break by
anyone who was relying on the mismatch. Both refusals name what was found and
what was expected, because a byte-level difference is otherwise invisible.

## ADR-073 — A role is one stored object, and role grants union with direct ones

**Decision.** Roles become first-class stored objects, in a `__kimmy.__roles`
system collection beside `__users`. A user record carries role *names*; a
`[[auth.oidc.role_mappings]]` entry may name a role instead of, or as well as,
carrying grants inline. Effective permission is always the **union** of a
principal's direct grants and its roles' grants.

Editing or deleting a role bumps `token_version` for every local user holding
it. `Role`, declared since the first RBAC pass and never constructed outside a
round-trip test, is now the thing this stores.

**Alternatives.** Leave grants copied onto every user record (the state before
this). Store roles but make them *replace* direct grants rather than add to
them. Resolve a named role once, when the verifier is built, instead of per
request.

**Why.** The system had ended up with two authorization models: local users
carried grants directly, an ACL, while federated users got them from an IdP
claim, RBAC. "analyst" meant one thing in a config file and a hand-assembled
copy of that thing on each user record, with nothing keeping the two in
agreement. That is survivable for a small org and fails an enterprise in three
specific ways — access review ("who can write to `sales`?" is a full scan of
every user instead of one lookup), joiner-mover-leaver (edit N records instead
of one object), and convergence with an IdP that already speaks roles.

**A union, not a replacement.** Kubernetes RBAC is purely additive and Postgres
unions privileges across role membership, so it is the unsurprising rule — but
the decisive argument is that it is the only one needing no migration: a user
holding no roles gets exactly what it got before, so every existing record
means what it always meant.

**No schema bump, and this is worth stating because the obvious reading is
wrong.** `UserStore` is not a redb table; it is BSON documents in an ordinary
system collection, created on demand. A `__roles` collection therefore needs no
migration, `User.roles` behind `serde(default)` decodes every pre-existing
record as holding none, and `SCHEMA_VERSION` stays where it was.

**Four consequences that are easy to miss.**

1. **The bump is not optional.** A local user's grants are resolved at login and
   embedded in its token, so without invalidating holders a *narrowing* edit
   would take effect only as each token expired — silently contradicting the
   revocation promise `set_grants` has made since ADR-052.
2. **The two paths need opposite treatment.** A federated principal has no user
   record and no token version (ADR-065), and its grants re-resolve from the
   store on every request, so a role edit already applies to it immediately and
   it needs no bump. It is easy to read the bump as universal; it is not.
3. **But federated role *membership* is still stale until the token expires.**
   This database makes no introspection call, so the `roles` claim is frozen in
   the access token. If the provider revokes someone's membership, this node
   honours the old claim for the rest of that token's life. Role *grants*
   re-resolve per request; role *membership* does not. These two facts are
   stated next to each other deliberately, because they are easy to conflate —
   and the auth service made the opposite trade for its own admin surface,
   re-reading the database on every request. Short token lifetimes are the
   mitigation.
4. **A deleted role leaves a dangling name on holders' records**, resolving to
   nothing. The alternative is rewriting every user record on a delete, and a
   name that grants nothing is the safe direction to fail in.

**Resolution is per request, not cached at construction.** Pre-resolving the
mapping table when the verifier is built is the obvious optimisation and it
silently freezes every federated principal's permissions at startup, so editing
a role would change nothing until a restart — the opposite of what naming a
stored role is for. Resolution therefore cannot live in `OidcVerifier`, which
does no I/O by design; it happens in the `Auth` extractor, the first place that
holds both the names and the engine.

**The audit record carries the roles *held*, not the role that decided.** Grants
are a union and more than one role can supply the same permission, so "the
deciding role" is not well defined — which grant `can` happened to match first
is an implementation detail, not a fact worth putting in an audit record. The
names matter most for a federated caller, where there is no user record to read
the association back from later.

**A note on vocabulary.** A provider typically enforces a small fixed set of
role names at client registration — the one this deployment federates with
allows exactly `admin`, `developer` and `user` — so a KimmyDB-specific
`claim_value` may simply be unregistrable. Named roles are a **mapping target**,
not a mirror of the provider's vocabulary, and all the interesting granularity
lives here. That is an argument for this decision rather than against it.

**Cost.** Invalidating holders is a scan of every user record, because there is
no index from role to holder. That is the honest cost of the storage shape, and
a role edit is an administrative action rather than a request-path one. Roles
also do not move the ceiling ADR-076 set: the collection is still the finest
unit of protection, and this changes who holds a permission and how it is
administered, not how finely it cuts.

## ADR-074 — `admin` is federatable, but only when asked for

**Decision.** `auth.oidc.allow_federated_admin`, default `false`. With it off —
the behaviour that shipped — a federated principal can never hold `admin`,
enforced in two places because there are two ways to ask for it: an inline
mapping naming `admin` is refused at startup, and a stored role that resolves to
`admin` has that action dropped where the role is resolved. With it on, both are
permitted and the node says so in its startup summary.

**Alternatives.** Keep ADR-067's absolute refusal. Allow it unconditionally.
Refuse the whole request when a federated principal's role carries `admin`,
rather than dropping the action.

**Why not keep the absolute refusal.** ADR-067 is well reasoned for a small org
and it makes the enterprise deployment *impossible*, not merely awkward: large
organisations run joiner-mover-leaver, and auditors specifically flag privileged
local accounts living outside the IdP — which the rule requires. MinIO, Vault,
Grafana and Elasticsearch all allow mapping a group to admin; the canonical
break-glass pattern is to allow the mapping and separately keep an emergency
local account, not to forbid it.

**Why the default does not change.** The boundary was tested rather than
reasoned about: a device-flow token from the live provider came back asserting
`roles: ["user", "admin"]`, and this node granted only what the `user` mapping
said — `GET /v1/users` 403, an out-of-scope write 403. That is the answer the
default must keep giving, and this flag is the only thing that changes it.

**Why the check moved to resolution time.** A startup check is sufficient for an
inline mapping, which cannot change while the process runs. It is *not*
sufficient for a stored role, which can be edited to include `admin` at any
time — a startup-only check would enforce a rule that stops being true minutes
later. So the named-role half is enforced where the role is resolved, on every
request.

**Why drop the action rather than refuse the request.** A role is shared with
the local users who hold it, and some of them legitimately have `admin`.
Failing the whole request would take away a federated caller's unrelated,
legitimate grants in order to withhold a permission it was never going to be
given anyway. The action is dropped, a grant left empty by that is discarded,
and the node warns once per process rather than once per request.

**Cost.** An operator who turns this on has moved a real security boundary, and
a compromised or misconfigured provider can then produce a superuser over this
database. The mitigation is that it cannot happen quietly: it is a config file
change, and the node names it at every start.

## ADR-075 — The CLI caches an access token only when asked, and never a refresh token

**Decision.** `kimmy login --cache-token` (or `KIMMY_TOKEN_CACHE`) stores the
access token it just obtained in a `0600` file under `$XDG_CACHE_HOME/kimmy`,
keyed by issuer, client id and resource, and reuses it until it is within a
minute of expiring. **Off by default.** A refresh token is never requested by
either flow and never stored. A token whose lifetime the provider did not state
is not cached at all.

**Alternatives.** Caching by default, as `gh`, `aws`, `az` and `kubectl` all
do. Keeping the absolute no-disk rule and closing the question. Caching the
refresh token too, so a lapsed session renews silently.

**Why.** The no-disk rule was a real design position — an environment variable
answers for a token's permissions, its lifetime and its cleanup by not existing
afterwards — and it was also stricter than every comparable tool, which made an
interactive user re-run the whole device flow whenever their shell export
lapsed. Opt-in keeps both: nobody who does not ask for it inherits a file to
look after, and the default behaviour is byte-for-byte what shipped before.

**The refresh token is the line, and it is not arbitrary.** An access token is
short-lived and, since ADR-071, audience-restricted to one node; a refresh
token outlives the session and mints more. Caching the first is a convenience
whose worst case expires on its own. Caching the second would be storing the
credential actually worth stealing, and it is why neither flow asks for one.

**A token with no `expires_in` is not cached.** RFC 6749 §5.1 only RECOMMENDS
the field. The cache exists to reuse a token *known* to still be valid, and
without a lifetime there is nothing to know — a guessed one would serve a dead
token as a 401 in some later, unrelated command.

**The key is all three of issuer, client and resource**, because each changes
what the token is: a different issuer is a different trust root, a different
client a different identity, and a different resource a different audience. A
token with the wrong audience is refused by the node, which would read as a
broken cache rather than as the wrong key having been used.

**Every read failure is a miss, not an error.** A corrupt file, an absent one
and one from a future version all mean the same thing to the caller —
authenticate again. A cache that can break a login is worse than no cache. The
write path is the same shape: it warns and carries on, and it writes through a
temporary file and a rename so an interrupted write cannot destroy tokens
already stored.

**Cost.** There is now a file holding a bearer token for anyone who opts in,
and the tool owns its permissions. `0600` is applied explicitly after creation
rather than relying on the umask, and the directory is `0700`; both are
asserted by tests, because a permissive umask would otherwise leave the token
group-readable and nothing would say so. Expired entries are swept on the next
write, so an issuer that stops being used does not leave a token on disk
indefinitely. Two functions take an explicit path so the tests never set
`XDG_CACHE_HOME` — an environment variable is process-global and cargo runs
tests in parallel, the same shape as the `include_names` race that cost a round
to diagnose.

## ADR-076 — Authorization stops at RBAC, and `/metrics` keeps its listener

**Decision.** Two boundaries, stated so they are not re-argued every round.

1. **Authorization stops at role-based access control.** The collection is the
   finest unit of protection: no document-level filtering, no field masking, no
   attribute-based rules, and no embedded policy engine — not OPA, not Cedar,
   not an expression language. First-class named roles, when they arrive, do
   **not** move this ceiling.
2. **`/metrics` stays unauthenticated on the main listener.** A separate
   metrics port was evaluated and is not built.

**Alternatives.** Document- or field-level security, as Elastic sells it. An
embedded policy engine evaluated at the authorization point. A second bind
address for `/metrics`, with its own TLS and bind-refusal rules. A startup
warning for any grant whose `db` pattern ends in `*`.

**Why RBAC is the stopping point.** Roles are the vocabulary every compliance
framework is already written in — access review, joiner-mover-leaver,
segregation of duties are all phrased in them — so roles are the thing an
enterprise buyer is actually asking about, and ABAC is the unusual request. A
policy engine inside the database would also create a *second* place where
access is decided, which is precisely what the single authorization decision
point exists to prevent, and it would put a language nobody can audit at a
glance in front of every read. A deployment that genuinely needs ABAC is better
served by it living in an application in front of this one, where the request
context it needs actually exists.

**Saying "roles do not close this" is the load-bearing half.** The arrival of
named roles is easy to read as having solved multi-tenancy, and it does not:
roles govern who holds a permission and how it is administered, not how finely
the permission cuts. The known-limits table says so in the same words, so the
two cannot drift.

**Why `/metrics` keeps its listener.** The usual argument for a second port is
that the metrics surface leaks operational detail. **This one does not carry
that detail** — it exposes counts and never names, with a golden test over the
whole render, so no database, collection, user or query text appears. The
benefit is therefore mostly notional, while the cost is concrete: a second bind
address, a second TLS decision, its own non-loopback refusal rules, and a fresh
way to misconfigure a node so that Prometheus silently scrapes nothing.
Restricting who may reach a port is a question a firewall, a network policy or
an existing reverse proxy already answers.

**With a stated trigger for revisiting it**, so this is a decision rather than a
refusal: if `/metrics` ever gains a label carrying a name, the trade inverts and
the second listener stops being ceremony. Adding such a label and adding the
listener are one piece of work.

**No warning for a `db` pattern ending in `*`.** The trailing `*` applies to
database names and matches a prefix, so `sales*` also covers `salesforce` — a
wider blast radius than most people picture. It is still a legitimate grant,
`{ "db": "*" }` for an administrator is the commonest one in existence, and a
warning emitted on every start for something correct is a warning nobody reads
by the second week. Documented with the habit that avoids the surprise — use a
separator you control, `sales_*` — and left to the audit log, which names the
database each decision was made against.

**Cost.** These are commitments, and a prospect who needs field-level security
is told plainly that this is not the database for that job. That is the cheaper
answer: the expensive one is implying it might arrive and being asked about it
every quarter.


## ADR-077 — Embedding work is owned per collection by rendezvous hash

**Decision.** Backfill scans and deferred re-checks run only on the node the
collection's `"{db}/{collection}"` key assigns through the same rendezvous
function the webhook dispatcher and the TTL sweeper use
(`kimmy_api::ownership`). The streaming path keeps its existing origin-node
rule — whoever wrote a document embeds it immediately — because that path was
never duplicated. A `[vector] worker_enabled` setting and its one-way
`--disable-vector-worker` flag let an operator take a node out of embedding
entirely, and the worker publishes `kimmy_embed_*` counters so the choice is
observable.

**Alternatives.** Deriving ownership inside `kimmy-vector` by depending on the
cluster crates — rejected because it inverts a layering boundary to save one
injected closure. Extending [`FOREIGN_GRACE`] until replication always wins —
rejected because the grace is a guess about lag, and the load test of
2026-08-24 measured hours of it: every expiry found stale vectors and all three
members embedded the same backlog. Doing nothing — the duplicate calls were
correct in their *results* (`vectors_are_stale` makes them no-ops on write) and
wrong only in cost, but the cost scaled with cluster size and with exactly the
metered-provider deployments this database targets.

**Why.** The load test put 7,219 documents into one vector-enabled collection
and watched all three members embed the same corpus against one shared
provider: roughly three times the inference, three times the shadow-collection
writes replicated twice each, and hours of provider saturation that ended with
the provider itself thrashing. The origin-node rule already deduplicated the
streaming path; what remained duplicated was everything *around* it — backfill,
which had no origin to defer to, and deferrals whose grace expired before
replication delivered the owner's vectors. Rendezvous ownership closes both
with machinery the codebase already trusts and tests: a pure function over the
live member set, where a node's departure moves only its own share, a
disagreement produces a duplicate rather than a gap, and a single node owns
everything without a special case. Dropping a non-owner's deferral is safe for
the same reason every member deferred it in the first place: the owner holds
its own copy of the same re-check.

## ADR-078 — Role mappings reach the environment as one JSON document

**Decision.** `auth.oidc.role_mappings` can be set through
`KIMMY_OIDC_ROLE_MAPPINGS` as a single JSON array of mapping objects, parsed
once at startup. When the variable is set it **replaces** the config file's
list entirely; when it is absent the file stands alone. There are still no
per-mapping flags, and `allow_federated_admin` remains file-only. Every
startup refusal — a mapping naming neither `role` nor `grants`, an inline
`admin` grant, an unknown action — applies to the env-supplied list unchanged,
because both forms land in the same field before `validate` runs.

**Alternatives.** Repeated flags (`--role-mapping claim_value=… role=…`) —
rejected for the reason ADR-066 kept mappings out of the CLI in the first
place: grants are structures, and a command line is where structures go to be
mistyped. Merging the variable with the file's list — rejected because a merge
needs an answer for "both sources name the same `claim_value`", and every
possible answer (first wins, last wins, refuse) surprises somebody; replace
matches how every other override here behaves: what was passed is what runs.
A separate key-value file mounted into the container — rejected as a third
place to look, and one that reintroduces exactly the file-plumbing problem
orchestrator deployments turned to env vars to escape.

**Why.** Federation shipped trusting one external issuer, but the deployment
shape almost everyone actually runs — compose, swarm, kubernetes — configures
the node through an environment block, and every other `[auth.oidc]` setting
has a `KIMMY_OIDC_*` variable. Mappings did not, so those deployments could
federate but could never say what a federated identity was *worth*: every such
node started with zero mappings and every federated caller held zero grants,
which looked exactly like a permissions problem and was really an
expressiveness gap. This was found live: a legitimately authenticated cluster
owner saw empty listings and bare 403s against his own database because the
cluster he deployed through env vars had no way to configure a mapping.

**Cost.** Two spellings of one setting, with replace-not-merge semantics that
must be documented loudly — an operator who sets both sources gets only the
variable's answer, which is discoverable but still surprising the first time.
The JSON form also moves a syntax error from TOML-parse time (where the file's
line number is reported) to a message naming the variable and showing the
expected shape, which the parser does deliberately.

## ADR-079 — The system database never matches a wildcard

**Decision.** `Principal::can` special-cases `__kimmy`. Wildcard database
patterns (`*`, trailing-`*` forms) no longer reach it at all. Two doors open
it instead: holding the **`admin` action on any grant** — administration
reaches through every boundary, and managing users and roles is what admin is
for — or a grant **naming `__kimmy` exactly**, honored down to its collection
pattern and actions. Exact means exact: `__k*` is still a wildcard and does
not match. Collection listings inside the system database keep the house
hidden-not-forbidden behavior: a caller with no door answers an empty list,
never a name.

**Alternatives.** Documenting wildcard access as intended — rejected because
it makes every `*/*` data-plane role a password-hash reader by default, which
is the kind of grant most deployments write first. Redacting sensitive fields
from `__users` reads instead of restricting them — rejected because the
collection also carries token versions and role names, because field-level
redaction multiplies surfaces to audit, and because "you may read this but not
these columns" inverts the grain RBAC runs at. Requiring a dedicated
`system` action for any system-database access — deferred; if a deployment
needs to hand out system visibility without full admin, an explicit
`{db:"__kimmy"}` grant already expresses it, and a new action can be added
without breaking anything if that proves too blunt.

**Why.** Found live, as the best ones are: the cluster owner's own federated
role — plain read/write over `*/*`, exactly what nearly every deployment writes
first — listed `__kimmy` in `kimmy databases`, and reading `__users` meant
reading argon2id password hashes and token versions. Nothing about federation
made it worse; *every* wildcard-granted principal had this reach since roles
shipped. The system database holds no user documents, only the machinery —
there is no legitimate data-plane reason for it to follow the data plane's
wildcards, and there always was an administrative path that bypasses them.

**Cost.** Anyone who was deliberately reading `__kimmy` through a wildcard
must now either hold admin somewhere or write the exact grant — a visible
behavior change, called out in the changelog's Changed section, and the safer
direction to err in: the change hides information from people who were never
meant to have it rather than revealing more. Root and every admin-flavored
deployment are untouched, proven by the existing suite passing unmodified.

## ADR-080 — The token cache is on by default, and every command reads it

**Decision.** `kimmy login` caches the access token it mints — unconditionally,
the way `kimmy token` already did — and every data command falls back to that
cache when no token was said with `--token`, `KIMMY_TOKEN`, or the settings
file. The `--cache-token` flag is removed. The device flow offers to open the
verification URL in the default browser (Enter to accept; skipped entirely when
stdin or stdout is not a terminal, so piped and scripted invocations are
untouched). A refresh token remains never-requested, never-stored.

**Why.** Found live, as the good ones are: the maintainer ran `init` → `login` →
`whoami` and got a 401 telling him to run `login`. Login had printed a valid,
correctly-audienceed token to his screen and kept nothing; whoami read only
`--token` / `KIMMY_TOKEN` / the settings file, none of which existed. Two
stores, zero consumers — the documented "log in once, then use the database"
workflow had never actually worked without an `export KIMMY_TOKEN=$(…)` step
nobody documents. A cache nobody reads is not privacy; it is ceremony.

**Cost.** Storing a bearer token at `0600` under the user's cache directory is
a responsibility the tool now always carries (ADR-075's original concern).
Against it: printing the token already put an unguarded copy in terminal
scrollback, so the disk copy is not the new exposure; what is stored did not
grow (access token alone, one hour, audience-restricted); and revocation stays
what it was — server-side `token_version`, honored because the cached copy dies
at expiry with no refresh token to extend it.

## ADR-081 — A recreated collection floors its previous incarnations at the drop

**Decision.** When a collection is created over a drop tombstone of the same
id — a recreate — the meta records that drop's stamp as an **incarnation
floor**. Replicated documents resolving to the collection are counted
superseded when their stamp is **at or below** the floor; collections created
with no tombstone behind them carry no floor and behave exactly as before
(serde-defaulted `None`, so legacy metas need no migration).

**Why.** Collection ids are derived from `(db, name)` (deliberately — every
node computes the same id, see `CollectionId::derive`), so drop-and-recreate
produces the *same* id, and a replicated document from before the drop still
resolves into the replacement. The existing tombstone guard compares
*strictly*: an entry whose stamp ties with the drop escapes it. That tie is
not exotic — a partitioned peer's final pre-drop write and the drop share one
millisecond whenever both engines tick one wall clock, which is every
same-host test run and any tight deployment. Observed as an intermittent CI
failure (`documents_written_before_a_drop_do_not_return_to_a_recreated_
collection`, left 1 / right 0), unreproducible across sixteen local runs.

**Why the floor is the drop, not the creation stamp.** The first attempt
floored at `meta.created`. It broke four replication tests, because a
*replicated* creation records the receiver's clock at apply time — which sits
after the entire catch-up backlog — so flooring there suppresses exactly the
documents a joining node most needs. The drop stamp has no such ambiguity: the
tombstone already travels with its originating stamp for precisely this class
of reason, and the floor simply echoes it into the incarnation that followed.
Entries stamped between drop and recreate cannot exist locally and are
correctly suppressed from partitioned peers; entries stamped after recreate
apply normally. Cross-host wall-clock skew can still place a peer's write
after the drop in stamp order — that is inherent to last-writer-wins stamps
and out of scope here.

**Cost.** One optional field on the meta. Same-ms writes made *after* a
recreate but sorting at or below the drop stamp are suppressed rather than
applied — a one-millisecond ambiguity that last-writer-wins cannot resolve and
that failing closed protects the recreated collection's isolation, which is
the property the whole guard exists for.

## ADR-082 — A full batch proves coverage of its window, for every origin the peer advertised

**Decision.** When a sync round receives a **full** batch — one truncated at
`MAX_BATCH` — the receiver raises its witnessed vector, for **every origin
the peer advertised**, to the lower of the peer's coverage of that origin and
the last delivered stamp. A short batch keeps its existing meaning: the peer's
whole tail, so the witnessed vector is raised to the peer's vector outright.
The decision is one pure function in storage, `coverage_after_batch`, called
from the one place the transport merges a served batch, `apply_peer_batch`.

**The defect.** `VersionVector::behind` reduces "what am I missing" to one
threshold: this node's *own* position at whichever trailing origin it holds
least of — by design, so a catch-up is a single range read of an oplog keyed
by time. The cost was known to be over-fetching. What was not seen: an
advertised stamp the receiver can *never* be sent — a unique-violation entry
is locally stamped, in the sender's vector, and filtered from every batch
(ADR-029) — pins the threshold at that origin's floor. The cure for exactly
that, absorbing the peer's vector, ran only on a short batch, and the comment
beside it described the failure it guarded against ("would ask again every
round forever") while guarding the cure away from the case that needs it.
Once the sender holds a full batch of *other* origins' entries after the
pinned floor — inevitable on any member that writes rarely and replicates
much — every round re-serves the same window, every entry in it superseded,
and the round is never short again.

**Observed** on a three-member test cluster, 2026-08-26, at 0.7.0 with
one member's storage and cluster targets at debug: 94 of 95 batches pulled
from one peer over 25 minutes read `applied=0, superseded=1021`; the peer
summary reported `ddl=3` from the same peer, and 1021 + 3 is `MAX_BATCH`. Two
log lines, one loop. `kimmy_replication_lag_seconds` read 68 545 — the age of
the cluster — because `lag_behind_ms` measures the pinned origin's gap from
its floor. Replication itself was healthy throughout: a fresh write reached
every member in under ten seconds, because it arrived through the *other*
peer, whose vector this node was not behind.

**Why raising to the window end is safe.** The sender serves contiguously in
stamp order from the point asked for (`read_oplog_from`, then the violation
filter). Nothing inside the delivered window was skipped except entries
deliberately withheld, and claiming those is correct: every node observes a
violation independently (ADR-029), so there is nothing to wait for. Bounding
each origin by the peer's own coverage means the receiver never claims history
the peer does not hold — another peer may. Bounding by the last delivered
stamp means nothing past the window is claimed. Entries that *tie* with the
last stamp from an origin with a higher node id sort after it and were not
served; they are re-served next round, because the range read is inclusive at
the stamp asked for. The one thing raised beyond what was delivered is the
pinned origin's floor, and that is the point.

**Why not fix `behind`.** Per-origin ranges would make a catch-up a scan and
filter over a time-keyed oplog, on every round, for every peer — the cost
ADR-054 and the `behind` doc comment deliberately declined. The threshold is
lossy but load-bearing; this decision keeps it and stops the loss from
compounding.

**Alternatives.** Ship violation entries and have receivers drop them: moves
the exclusion to the wrong side and doubles the wire cost of every collision.
Advertise the servable vector minus violations: a second vector to keep
consistent, and it does not cover the general shape — any advertised stamp
behind a full window pins the same way, whatever withheld it.

**Cost.** One `VersionVector` built per full batch, at most one entry per
origin the peer advertised. The witnessed-vector write it feeds already
happened per batch; this widens what it carries. A three-engine storage test
reproduces the loop deterministically — `applied 0, superseded 7, ddl 1` on
every round with the raise removed — and converges with it.

## ADR-083 — `update` and `delete` by filter run inside the write transaction

**Decision.** A filtered write matches and writes in **one** write
transaction. `Engine::modify_where` collects the matches under the writer,
applies the caller's operators to the image the transaction holds, writes
each result with its oplog entry, commits once, and publishes afterwards. It
is the same per-document body `find_and_modify` uses — one function,
`modify_in_txn`, behind both — so a rule added to one filtered write holds
for every filtered write. `multi: false` is `stop_after = 1` on the same
path. The executor plans the access path the way `find` does — primary key,
then index, then scan — and hands the engine a `Candidates`, which gains a
`Keys` variant for the primary-key case.

**The defect.** `exec::update` collected its targets in a read transaction,
applied the operators in memory, and stored each result through
`Engine::replace` in its own write transaction. The operators therefore ran
on an image another writer could already have moved past: two concurrent
`$inc`s on one document both read 5 and both stored 6. A lost update, on a
single node, in the plainest write there is — against the documented
per-document atomicity. Measured before the fix: four writers × 500
increments through the API left the counter at 500. `delete` had the same
read-then-write split, and a `multi` update that failed part-way had
already committed the documents before the failure.

**Why reuse `find_and_modify`'s body rather than lock around the old path.**
The body already existed, was already correct, and was already tested for
exactly the property that was missing. A lock around read-then-write would
have kept two code paths that must agree and left the second one to drift.
The `insert` / `insert_many` / `insert_in_txn` pattern is the precedent.

**What it changes for a `multi: true` request.** It is now one transaction:
all of it lands or none of it does, and the request costs one fsync rather
than one per document. The price is that the writer is held for the whole
request, so the same 10,000-match ceiling `find_and_modify` has applies —
refused, not truncated, because a filtered write over a prefix of the matches
would silently leave the rest. Lifting the ceiling by committing in bounded
chunks is a separate decision; this one closes the correctness gap.

**`find_and_modify` on `_id` no longer scans.** Only the index planner ran
for it, so a filter on the primary key scanned the collection under the
writer. Sharing the executor's planner with `update` fixed that in passing.

**Alternatives.** Re-read each target inside its own write transaction and
re-apply the operators there: correct per document, but `multi` stays one
commit per document and the two paths still diverge. Optimistic retry on a
stamp mismatch: needs the stamp exposed first, and retries a race the single
writer can simply prevent.

**Cost.** One transaction held across the match. Indexed and primary-key
filters add microseconds; an unindexed one adds the scan, exactly as
`find_and_modify` already did, bounded by the same cap. A `multi` update
that used to partly succeed on a mid-way failure now fails whole — the
behaviour the durability table always claimed.

## ADR-084 — Conditional writes by stamp, node-local, with one error code

**Decision.** Every write reports the stamp it produced; every
single-document write accepts `if_stamp`, and lands only if the document is
still at that version. The check runs inside the write transaction, in the
one per-document body every filtered write already shares (`modify_in_txn`,
ADR-083) and in `replace_if` / `delete_where` for the by-id routes, so there
is no window between check and write and no second code path to drift. A
mismatch — a different version, or no live document where one was expected
— is **`409 stale`, `retry: no`**, on every route alike, and writes nothing:
no document, no oplog entry, no event. `find` returns versions on request
(`stamps: true`, a parallel array), a read by id carries its version as
`ETag`, and the whole thing is advertised as the `conditional-writes`
capability.

**What it is for.** Check-then-act on one document: read, decide, write —
with the write refused if anyone else got there first. The lost-update fix
(ADR-083) made `update` atomic *within* a request; this is the tool for the
race *between* two requests, and it is the honest one on an AP store, because
it coordinates nothing. It is node-local by construction: two nodes can each
accept a conditional write against the same version during a partition, and
last-writer-wins decides when they meet. The docs say so in every place the
feature is described.

**Why a stamp, and why opaque.** The document's version already exists — the
stamp of the write that produced it, which last-writer-wins runs on — so no
new counter, no per-document field, and nothing to migrate. It is encoded as
an opaque token (the HLC's order-preserving bytes, then the node id, in
base64url) rather than exposed as a structure, for the same reason cursors
and resume tokens are: a client compares for equality and hands it back, and
the encoding can change without a client noticing.

**Why bodies and query strings rather than `If-Match`.** `If-Match` is the
HTTP-native spelling for the by-id routes, and its failure is `412`. But
`update` and `find_and_modify` carry their condition in a JSON body, and one
feature answering `412` on two routes and `409` on two others is a client
branching on the route rather than the code. One code, one status, one retry
class, across all four — the envelope contract of ADR-057 — won. `ETag` is
still set on a read by id, because it costs nothing and is what a curl user
looks for; nothing is conditional on it being sent back as a header.

**Why a missing document is stale, even with `upsert`.** The caller said "at
this version". A document that is gone is not at that version, and creating a
fresh one under an `upsert` would silently turn a lost race into a resurrected
document with the caller's stale image. Refusing is the only reading a caller
can act on correctly.

**Why not `stamps` inside each document.** A `_stamp` field in `find` results
would come back on the next `PUT` as data, and would collide with a user's
own field of that name. A parallel array keeps the document exactly as
stored.

**Alternatives.** A per-document integer version: a second counter beside
the stamp, with nothing the stamp does not already give. `If-Match` / `412`
on the by-id routes only: two spellings of one condition. Server-side retry
on `stale`: the server cannot re-run the caller's decision, and a client that
wants blind retry has `update` operators for that.

**Cost.** One `Stamp` comparison inside the transaction, for callers that
pass `if_stamp`; nothing for callers that do not. Response bodies gain a
`stamp` field (additive under `/v1`). Three clients gain conditional variants
and a typed `stale`; the conformance suite gains a scenario that holds all
three to the same answer.
## ADR-085 — Tombstones outlive the oplog, and a stale rejoiner is named, not refused

**Decision.** Two operational guards around partition resurrection. First, a
node refuses to start with `storage.tombstone_retention_secs` shorter than
`storage.oplog_retention_secs`. Second, the replication loop measures how far
each peer trails *this* node — the same per-origin lag it already computes,
with the roles swapped — and when that exceeds tombstone retention it logs
one `WARN` on the transition and reports the peer to the API, which shows
`staleSince` and `behindSecs` on the peer's `GET /v1/topology` entry until
the peer is back within the window. The merge itself is not refused.

**Why the refusal.** A tombstone exists to out-argue a late write. The oplog
entry that carries a delete can be served to a peer for `oplog_retention`;
the tombstone that makes applying it *mean* something lives for
`tombstone_retention`. Set the second shorter than the first and there is a
window in which a peer is told about a delete it has nothing to lose
against: the replay finds no tombstone, the peer's own older image of the
document wins, and a partition shorter than the oplog window has resurrected
data. It is the only retention configuration with that property, the
defaults (24 h and 24 h) never had it, and nothing legitimate needs it.

**Why name rather than refuse the rejoiner.** Refusing a merge is a policy
decision with a real cost: a cluster that quarantines a member on its own
authority is one an operator has to reason about mid-incident, and the
observed case on the test cluster (2026-08-26) was not a partitioned member
but every member at once, each behind the others' horizon after ten hours
dark — a refusal would have left the cluster refusing itself. The warning
and the topology field give the operator the fact and the recommended
action (stop the peer, reset its data directory, let anti-entropy refill
it) at the moment they matter; had they existed, the outage would have been
named in the first sync round after the horizon passed rather than found by
reading storage sizes diverge.

**Why the measure is per origin and ignores unseen origins.** "Behind by
more than retention" is only meaningful for history the peer *has*: a
brand-new member has seen nothing and holds nothing old enough to
resurrect, so it must not read as stale on its first round. The existing
`lag_behind_ms` already skips origins the trailing side has never observed;
reusing it reversed keeps one definition of lag.

**Alternatives.** Refuse the merge when stale: see above. Raise the peer's
floor silently (treat it as fresh): hides exactly the event the operator
needs to see. A metric only: a gauge cannot name the peer, and the topology
route is where a peer already has a row.

**Cost.** One vector comparison per successful round, already computed for
lag; a `BTreeMap` of stale peers in API state; two additive fields on a
topology entry, present only while the condition holds. The refusal is a
behaviour change for a configuration nobody should have had.

## ADR-086 — A `multi` write commits in bounded chunks

**Decision.** `update` and `delete` with `multi: true` commit in chunks of
`storage.multi_chunk_docs` documents (default 1,000; 1 to 10,000). Each chunk
is one write transaction, matched and written inside it exactly as ADR-083
made a single-document write; the writer is released between chunks; the
next chunk resumes strictly after the last document key the previous one
wrote, on every candidate path. A failure in a later chunk leaves the earlier
ones committed and answers with an error. The response gains `commits`. The
10,000-match refusal that ADR-083 gave `multi` is gone; `find_and_modify`
keeps it, because it has to sort the whole match set before choosing.

**Why not keep one transaction per request.** It held the single writer for
the whole match and the whole write, which is why it had to be capped — and
the cap was a refusal on a filtered write, which is a new failure mode for
a request that used to run to completion. Chunking is what the TTL expiry
pass already does (1,000 documents per pass, precisely so a backlog does not
hold the writer), and it gives the same two properties here: bounded writer
hold, and a bounded loss — at most the chunk in flight.

**Why resume by key rather than re-scan.** Every candidate path delivers
documents in key order — the documents table by its key, a primary-key list
and an index union once sorted — so "strictly after the last key written" is
a range bound, not a filter. A chunk costs its own size, a document is never
visited twice, and a document that stops matching between chunks (another
writer changed it) is simply not matched by the next chunk, which is the
same answer a concurrent writer gets between any two requests.

**Why the failure answer is an error and not a partial count.** A response
carrying both an error and "but 2,000 landed" is a shape no client handles;
the envelope's contract (ADR-057) is one code per answer. What landed is
exactly what the oplog and the collection show, and `commits` on the success
path already tells a caller the request was chunked. A caller that must know
after a failure reads the collection.

**Alternatives.** Keep the cap and document it: the cap was the problem.
One transaction with the scan outside it: reintroduces the read-then-write
gap ADR-083 closed. A configurable "all or nothing up to N": that is
`multi_chunk_docs = 10000`, which is allowed.

**Cost.** One more field on two responses; one configuration key; a chunk
boundary is a place a concurrent writer can interleave, which the documents
call out. `examined` and `matched` sum across chunks.
## ADR-087 — Standing unique violations are a query, derived from the oplog

**Decision.** `GET /v1/db/{db}/coll/{coll}/violations` reports the unique
violations that still stand on a collection: a count per index, or with
`?index=<name>` the colliding groups, each with its `_id`s, the `_id` whose
merge revealed the collision, and the documents themselves. It is derived on
request from the retained oplog's `UniqueViolation` entries (ADR-029),
deduplicated by index and id set, keeping only those whose named documents
all still exist. Authorised as `read`. Nothing new is stored and nothing is
written by the route.

**Why derived rather than kept.** A table of open violations would be a
second record of a fact the oplog already holds, with its own lifecycle —
cleared when? by whom? — and a second thing replication would have to carry.
The oplog entry is already there on every node that merged the collision, it
already names the documents, and "still standing" is one read per named id.
A pass over the retained oplog costs what retention bounds, on a route
nobody calls in a loop.

**Why "all documents still exist" is the definition of standing.** The
violation is that two `_id`s share a key. Once either document is gone the
key is unique again, so a delete always resolves it and the report clears. A
rewrite that changes the colliding value *also* resolves it — but this route
does not notice, because it does not re-evaluate keys. That is the honest
limit of deriving from the event: the report can over-state after a rewrite,
never under-state after a delete. The recipe in `indexes.md` says so, and a
client that rewrote can confirm with `find`. Re-evaluating keys on the route
would mean re-running index maintenance in order to read, which is where a
cheap derived query stops being cheap.

**Why `read` rather than `admin`.** The documents are readable, and the
`uniqueViolation` change-stream event that announced the collision is
readable by anyone with `watch`. Hiding the resolution view behind `admin`
would make the one principal that has to fix the data the one that cannot
see what is broken. `/metrics` keeps its name-free count for the same reason
it always has.

**Alternatives.** Refuse the merge instead: ADR-029 already declined that —
refusing a replicated write is how two nodes stop agreeing. Emit only the
event: it is the durable record, but a client that missed it has nothing to
ask. A metric with index labels: leaks schema on an unauthenticated port.

**Cost.** One oplog pass and one read per named document, per request; a
report that can over-state after a rewrite, documented; bounded by retention
— a collision older than the oplog window is no longer listed, and the
change-stream event is the record that outlives it.

## ADR-088 — Two durability classes, and the one there is not

**Decision.** `storage.durability` selects how a commit reaches the disk.
`durable`, the default, is unchanged: every commit fsyncs before it returns.
`coalesced` writes a commit with redb's `Durability::None` and then waits at
a barrier for the next shared fsync — one durable commit, carrying a marker
so it is never empty, per `commit_coalesce_ms` window (default 5 ms) — so
that N concurrent writers pay one fsync rather than N. Both classes are
durable when the write's response returns. The class is reported by
`GET /v1/version` as `durability`, and `/metrics` gains `kimmy_fsyncs` and
`kimmy_commits_grouped_total`. There is no third class.

**The barrier.** No background thread and no handle to the engine: the
committers run it. The first to arrive after a flush becomes the leader,
sleeps one window so others can join, performs the flush, and wakes everyone
whose commit it covered; arrivals during the window only wait. Every
committer waits for a flush that *started after* its own commit, which is
the whole of the durability argument, and the leader's sleep is what turns
concurrency into grouping. A single-writer loop gains nothing from it — each
commit waits a window for company that never comes — and the documentation
says so; it pays off under concurrency, which is where the single-writer
line in the benchmarks was flat.

**Why not `fast`.** The spike showed redb would allow it cleanly: commit
with `None`, fsync on a timer, respond at once. It was declined (the maintainer,
2026-08-26) because it changes what a 200 means — an acknowledged write
becomes losable for up to an interval — and because the loss is not even
local: replication and change streams read the oplog before the disk has
it, so a peer or a subscriber can hold an entry this node then forgets,
which anti-entropy would later refill from the peer as if the node had
never written it. A knob that quietly weakens the one promise the durability
table makes is the kind that gets turned on and forgotten. `coalesced`
takes the throughput without touching the promise.

**Why a field on `/v1/version` rather than a capability.** The capability
set is closed by an enum and checked against the specification; a value
like `durability:coalesced` is configuration, not a feature a client
depends on, and a client has nothing to branch on either way. A field
answers the operator's question — "which class is this node running?" —
without pretending it is a protocol feature.

**Alternatives.** Group commit by merging transactions: redb's single writer
cannot merge two open transactions, and the leader pattern gives the same
amortisation without changing what a transaction is. A background flusher
thread: needs a handle to the database from outside the engine and a
lifecycle of its own; the leader-elected barrier has neither. Per-request
durability choice: a request that asks for less than the node's class is a
request that changed the promise for everyone sharing its fsync.

**Cost.** One mutex-guarded ticket per commit under `coalesced`, one window
of latency per write, one marker key in `meta` rewritten per flush. Two
configuration keys, one field on `/v1/version`, two metric series. Under
`durable` nothing changes but a counter.

## ADR-089 — The CLI is for people: `client_credentials` leaves `kimmy`

**Decision.** `kimmy login --client-credentials` and `kimmy token
--client-credentials` are removed, with the `KIMMY_OIDC_CLIENT_SECRET`
variable and the `client_secret` settings key they existed to consume. The
CLI runs exactly two flows: a password login for a named local account, and
the RFC 8628 device flow for everyone else. A script, a cron job or a
healthcheck sets `KIMMY_TOKEN` (or the settings file's `token`) to a bearer
token minted elsewhere — for example, a personal access token from the
console, audienced at the node — and every command works as it always did. A
`client_secret` line left in an existing `.kimmy` is warned about and
ignored rather than rejected; `kimmy init` drops it when it rewrites the
file.

**Why.** The CLI is, by intent, a tool for humans, and the grant was there on
the claim that it was the only non-interactive way to a token. It was not:
`--token` / `KIMMY_TOKEN` has been a global argument on every subcommand
since the tool existed, and `$(kimmy login --client-credentials)` only ever
produced a value for it. Where a bearer token came from is not the CLI's
concern. No other tool in the field carries the grant unless its platform has
no other machine credential — `gh` has none at all, `stripe` and `doctl` take
an API key from the environment — and KimmyDB has two: a local account, and
a token from the provider. Against the deployment it ships in, the flag was
also inert: `kimmy-cli` is registered as a *public* client, and a public
client has no secret to present, so the grant could never succeed under the
CLI's own id. Worst, the affordance misled: twice in one session it pulled a
careful reader toward minting a machine token for a person's session. An
option that steers competent readers wrong is evidence about the option,
not about the readers.

**Rejected: keep the flag and fix the documentation.** A documentation fix
keeps the pull; it only adds a sign next to it. Nothing depends on the flag —
no repository invokes it, and the one service that does use the grant (a
sibling project's agents) has its own implementation and never shelled out to
`kimmy`. Removal breaks nothing that exists, and pre-1.0 a `0.MINOR` may
carry a breaking change that produces the better design.

**Rejected: reject a stale `client_secret` key.** The settings file treats
an unknown key as an error, deliberately, because a typo that silently did
nothing is worse than a loud failure. A retired key is not a typo: refusing
every command over a line that used to be valid is the worse failure. So
that one key is named, warned about on stderr and skipped, and every other
unknown key still fails loudly.

**Cost.** A personal access token is user-scoped and expires — 90 days by
default, 365 at most — so it is not a true non-human identity. If KimmyDB
ever wants one, that is a *service* holding client credentials and calling
the token endpoint itself, which is a different piece of work and still not
a CLI flag. The Basic-auth encoding, the auth-method discovery and the
empty-scope rule that the grant needed all leave with it; the device flow
never used them.

## Next

- [Roadmap](roadmap.md) — decisions still to be made
- [Testing](testing.md) — how these choices are defended
